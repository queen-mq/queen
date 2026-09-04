//! parquet — the Parquet writer (PLAN_S3_SINK.md §6.4, read by §7).
//!
//! One window's records in, one Parquet object's bytes out, through the
//! **low-level** writer API ([`SerializedFileWriter`] + typed column writers).
//! There is no `arrow` in the dependency tree and none is wanted: the envelope
//! is five fixed columns, and an Arrow round trip would buy nothing but the
//! whole `arrow-*` build (plan F12).
//!
//! # The envelope
//!
//! ```text
//! message queen_record {
//!   required binary partition (STRING);
//!   required int64 offset;
//!   required binary transaction_id (STRING);
//!   required int64 ts (TIMESTAMP(MICROS,true));
//!   optional binary payload (STRING);
//! }
//! ```
//!
//! There is **no `queue` column**: the queue name is the `queue=<esc>` key of
//! the object's path (plan §6.3), which is where Hive and Spark put a partition
//! key when they write `partitionBy`. A column that repeats the key is a table
//! a Hive metastore refuses and a dataset PyArrow will not merge (its partition
//! discovery infers the key as `dictionary<string>`, the file column is
//! `string`). What a file copied OUT of the layout keeps instead is the footer
//! pair `queen.queue=<queue>`, so it stays self-describing.
//!
//! `payload` is the record's JSON **text, spliced verbatim** — never re-parsed,
//! never re-serialised (`types::Record`) — and is NULL exactly when the wire
//! carried `"payload":null`. `ts` is the segment's `created_at` in microseconds
//! since the epoch, UTC-adjusted, so a reader's `WHERE ts BETWEEN …` prunes row
//! groups.
//!
//! # Determinism (the whole basis of exactly-once, plan §4.2)
//!
//! The same records in the same order must produce the same bytes in every
//! process, forever, so every knob that can move a byte is pinned here rather
//! than inherited from the parquet crate's defaults:
//!
//! | pin | value | why |
//! |---|---|---|
//! | `created_by` | `"queen-s3"` | the crate default embeds `parquet-rs version <v>`; the *writer* identity belongs in the manifest's `writer` field, not in the object |
//! | key/value metadata | exactly `queen.envelope=1` and `queen.queue=<queue>` | two pairs, both constant for the object: the queue name is fixed per writer, so the footer is still a pure function of the window; no hostname, no time, nothing that varies |
//! | writer version | `PARQUET_1_0` | DataPageV1: the format every reader in plan §7 takes. The two INT64 columns still opt into `DELTA_BINARY_PACKED` explicitly |
//! | compression | ZSTD level 3, or Snappy | fixed level: a level is part of the bytes |
//! | statistics | `Chunk` | min/max per row group — the pruning plan §6.4 asks for; page statistics (and with them the column index) would only add footer weight |
//! | row groups | closed every `row_group_records` | the caller's fixed number, never a byte- or time-derived one |
//! | data page size / dictionary page size | 1 MiB each | crate defaults *today*; pinned so a crate default change is visible as a test failure, not as silently different bytes |
//! | data page row count | 20 000 | as above |
//! | write batch size | 1024 | it decides where pages split, so it decides bytes |
//! | statistics / column index truncation | 64 bytes | as above |
//!
//! What is deliberately **not** pinned is the parquet crate version itself: a
//! library upgrade may move the bytes, which is why the manifest records the
//! writer string ([`ParquetFactory::describe`]) and `Cargo.lock` is bumped
//! deliberately (plan §6.4).

use std::io::Write;
use std::sync::Arc;

use bytes::Bytes;
use parquet::basic::{Compression as PqCompression, Encoding, ZstdLevel};
use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
use parquet::errors::ParquetError;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{
    EnabledStatistics, WriterProperties, WriterPropertiesPtr, WriterVersion,
};
use parquet::file::writer::{
    SerializedColumnWriter, SerializedFileWriter, SerializedRowGroupWriter,
};
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::{ColumnPath, TypePtr};

use super::{RecordWriter, WriteError, WriterFactory};
use crate::types::{Compression, Format, ParquetCodec, Record};

// ---------------------------------------------------------------------------
// The pins
// ---------------------------------------------------------------------------

/// The envelope of plan §6.4, and the only schema this writer ever writes.
const SCHEMA: &str = "message queen_record {
  required binary partition (STRING);
  required int64 offset;
  required binary transaction_id (STRING);
  required int64 ts (TIMESTAMP(MICROS,true));
  optional binary payload (STRING);
}
";

/// The columns, in schema order. Indices are used as `cols[..]` order below.
const COL_PARTITION: &str = "partition";
const COL_OFFSET: &str = "offset";
const COL_TRANSACTION_ID: &str = "transaction_id";
const COL_TS: &str = "ts";
const COL_PAYLOAD: &str = "payload";

/// `created_by` in the footer: a constant, never a version and never a time.
const CREATED_BY: &str = "queen-s3";

/// The schema-generation marker for readers: constant, in the properties.
const ENVELOPE_KEY: &str = "queen.envelope";
const ENVELOPE_VALUE: &str = "1";

/// The queue name, appended to the footer at [`RecordWriter::finish`] because
/// it is known only from the pushed records. One pair, constant per object: the
/// queue is otherwise only in the `queue=` path key (plan §6.3), and a file
/// copied out of the layout would lose it.
const QUEUE_KEY: &str = "queen.queue";

/// The parquet crate's major version, for [`ParquetFactory::describe`]. Read
/// from `Cargo.lock` by a test rather than at build time.
const PARQUET_MAJOR: &str = "59";

const WRITER_VERSION: WriterVersion = WriterVersion::PARQUET_1_0;
const ZSTD_LEVEL: i32 = 3;
const DATA_PAGE_SIZE: usize = 1024 * 1024;
const DICTIONARY_PAGE_SIZE: usize = 1024 * 1024;
const DATA_PAGE_ROW_COUNT: usize = 20_000;
const WRITE_BATCH_SIZE: usize = 1024;
const TRUNCATE_LENGTH: usize = 64;

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Opens Parquet writers for one fixed (codec, row-group size).
pub struct ParquetFactory {
    codec: ParquetCodec,
    row_group_records: usize,
    schema: TypePtr,
    props: WriterPropertiesPtr,
}

impl ParquetFactory {
    /// `row_group_records` is clamped to at least 1: a row group per record is
    /// pathological but writable, a row group per *zero* records is not.
    pub fn new(codec: ParquetCodec, row_group_records: usize) -> ParquetFactory {
        ParquetFactory {
            codec,
            row_group_records: row_group_records.max(1),
            schema: Arc::new(
                parse_message_type(SCHEMA).expect("the envelope schema is a compile-time constant"),
            ),
            props: Arc::new(properties(codec)),
        }
    }
}

/// The pinned writer properties for a codec. Everything that can move a byte is
/// set explicitly; see the module docs for why each one is here.
fn properties(codec: ParquetCodec) -> WriterProperties {
    let compression = match codec {
        ParquetCodec::Zstd => {
            PqCompression::ZSTD(ZstdLevel::try_new(ZSTD_LEVEL).expect("zstd level 3 is in range"))
        }
        ParquetCodec::Snappy => PqCompression::SNAPPY,
    };
    WriterProperties::builder()
        .set_writer_version(WRITER_VERSION)
        .set_created_by(CREATED_BY.to_string())
        .set_key_value_metadata(Some(vec![KeyValue::new(
            ENVELOPE_KEY.to_string(),
            ENVELOPE_VALUE.to_string(),
        )]))
        .set_compression(compression)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_statistics_truncate_length(Some(TRUNCATE_LENGTH))
        .set_column_index_truncate_length(Some(TRUNCATE_LENGTH))
        .set_data_page_size_limit(DATA_PAGE_SIZE)
        .set_dictionary_page_size_limit(DICTIONARY_PAGE_SIZE)
        .set_data_page_row_count_limit(DATA_PAGE_ROW_COUNT)
        .set_write_batch_size(WRITE_BATCH_SIZE)
        // partition: dictionary. A handful of hot entities per window make the
        // dictionary the whole column, and `partition`'s min/max then prune by
        // entity (plan §6.4).
        .set_column_dictionary_enabled(ColumnPath::from(COL_PARTITION), true)
        // offset: dense, ascending within a partition — delta beats a
        // dictionary of a million distinct integers. An explicit encoding is
        // only a *fallback* while the dictionary is on, hence both calls.
        .set_column_dictionary_enabled(ColumnPath::from(COL_OFFSET), false)
        .set_column_encoding(ColumnPath::from(COL_OFFSET), Encoding::DELTA_BINARY_PACKED)
        // ts: one value per segment, non-decreasing within a partition, so the
        // deltas are mostly zero.
        .set_column_dictionary_enabled(ColumnPath::from(COL_TS), false)
        .set_column_encoding(ColumnPath::from(COL_TS), Encoding::DELTA_BINARY_PACKED)
        // transaction_id and payload: high-cardinality text. Plain, and let the
        // block codec do the work.
        .set_column_dictionary_enabled(ColumnPath::from(COL_TRANSACTION_ID), false)
        .set_column_encoding(ColumnPath::from(COL_TRANSACTION_ID), Encoding::PLAIN)
        .set_column_dictionary_enabled(ColumnPath::from(COL_PAYLOAD), false)
        .set_column_encoding(ColumnPath::from(COL_PAYLOAD), Encoding::PLAIN)
        .build()
}

impl WriterFactory for ParquetFactory {
    fn open(&self) -> Box<dyn RecordWriter> {
        Box::new(ParquetWriter::new(
            self.schema.clone(),
            self.props.clone(),
            self.row_group_records,
        ))
    }

    fn extension(&self) -> &'static str {
        "parquet"
    }

    fn content_type(&self) -> &'static str {
        "application/vnd.apache.parquet"
    }

    fn describe(&self) -> String {
        format!(
            "queen-s3/{} parquet/{} {}",
            super::CRATE_VERSION,
            PARQUET_MAJOR,
            codec_name(self.codec)
        )
    }

    fn format(&self) -> Format {
        Format::Parquet
    }

    /// [`Compression`] has no Snappy variant and this crate does not get to add
    /// one (`types.rs` is the shared vocabulary): a Snappy Parquet object
    /// reports `None` here, and the codec it was actually written with is
    /// carried by [`WriterFactory::describe`] into the intent and the manifest.
    /// The distinction is invisible to readers anyway — the codec is recorded
    /// per column chunk inside the file.
    fn compression(&self) -> Compression {
        match self.codec {
            ParquetCodec::Zstd => Compression::Zstd,
            ParquetCodec::Snappy => Compression::None,
        }
    }
}

fn codec_name(codec: ParquetCodec) -> &'static str {
    match codec {
        ParquetCodec::Zstd => "zstd",
        ParquetCodec::Snappy => "snappy",
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// The five column buffers of one row group, index-aligned by row.
#[derive(Default)]
struct Columns {
    partition: Vec<ByteArray>,
    offset: Vec<i64>,
    transaction_id: Vec<ByteArray>,
    ts: Vec<i64>,
    /// Only the non-null payloads, in row order.
    payload: Vec<ByteArray>,
    /// One level per row: 1 = present, 0 = NULL.
    payload_def: Vec<i16>,
}

impl Columns {
    fn rows(&self) -> usize {
        self.offset.len()
    }

    fn clear(&mut self) {
        self.partition.clear();
        self.offset.clear();
        self.transaction_id.clear();
        self.ts.clear();
        self.payload.clear();
        self.payload_def.clear();
    }
}

struct ParquetWriter {
    /// `None` once [`RecordWriter::finish`] has taken the footer out.
    file: Option<SerializedFileWriter<Vec<u8>>>,
    row_group_records: usize,
    cols: Columns,
    /// The current partition name, interned as a refcounted buffer: every row
    /// repeats it, and a `Bytes` clone is a refcount bump instead of an
    /// allocation.
    partition_cache: Option<(Arc<str>, Bytes)>,
    /// The queue name of the first record pushed, appended to the footer as
    /// `queen.queue` by [`RecordWriter::finish`]. `None` for an object with no
    /// records: there is nothing to name it after, and no rows to describe.
    queue: Option<String>,
    records: u64,
    weight: usize,
    /// The `(partition, offset)` of the previous record, for the sortedness
    /// assertion. Debug builds only — in release it does not exist.
    #[cfg(debug_assertions)]
    last: Option<(Arc<str>, i64)>,
}

impl ParquetWriter {
    fn new(schema: TypePtr, props: WriterPropertiesPtr, row_group_records: usize) -> ParquetWriter {
        // `SerializedFileWriter::new` only writes the 4-byte magic into the
        // Vec, so the only way it fails is a Vec that cannot grow.
        let file = SerializedFileWriter::new(Vec::new(), schema, props)
            .expect("writing PAR1 into a Vec cannot fail");
        ParquetWriter {
            file: Some(file),
            row_group_records,
            cols: Columns::default(),
            partition_cache: None,
            queue: None,
            records: 0,
            weight: 0,
            #[cfg(debug_assertions)]
            last: None,
        }
    }

    fn flush_row_group(&mut self) -> Result<(), WriteError> {
        let cols = &mut self.cols;
        if cols.rows() == 0 {
            return Ok(());
        }
        let file = self.file.as_mut().ok_or_else(finished)?;
        let mut rg = file.next_row_group().map_err(werr)?;

        {
            let mut c = column(&mut rg)?;
            c.typed::<ByteArrayType>()
                .write_batch(&cols.partition, None, None)
                .map_err(werr)?;
            c.close().map_err(werr)?;
        }
        {
            let mut c = column(&mut rg)?;
            c.typed::<Int64Type>()
                .write_batch(&cols.offset, None, None)
                .map_err(werr)?;
            c.close().map_err(werr)?;
        }
        {
            let mut c = column(&mut rg)?;
            c.typed::<ByteArrayType>()
                .write_batch(&cols.transaction_id, None, None)
                .map_err(werr)?;
            c.close().map_err(werr)?;
        }
        {
            let mut c = column(&mut rg)?;
            c.typed::<Int64Type>()
                .write_batch(&cols.ts, None, None)
                .map_err(werr)?;
            c.close().map_err(werr)?;
        }
        {
            let mut c = column(&mut rg)?;
            c.typed::<ByteArrayType>()
                .write_batch(&cols.payload, Some(&cols.payload_def), None)
                .map_err(werr)?;
            c.close().map_err(werr)?;
        }

        rg.close().map_err(werr)?;
        cols.clear();
        Ok(())
    }
}

impl RecordWriter for ParquetWriter {
    fn push(&mut self, queue: &str, rec: &Record) -> Result<(), WriteError> {
        if self.file.is_none() {
            return Err(finished());
        }
        #[cfg(debug_assertions)]
        {
            if let Some((p, o)) = self.last.as_ref() {
                debug_assert!(
                    (rec.partition.as_ref(), rec.offset) > (p.as_ref(), *o),
                    "parquet: records must arrive sorted by (partition, offset), \
                     got ({:?}, {}) after ({:?}, {})",
                    rec.partition,
                    rec.offset,
                    p,
                    o
                );
            }
            self.last = Some((rec.partition.clone(), rec.offset));
        }

        if self.queue.is_none() {
            self.queue = Some(queue.to_string());
        }
        let partition_bytes = intern(&mut self.partition_cache, &rec.partition);

        self.cols.partition.push(ByteArray::from(partition_bytes));
        self.cols.offset.push(rec.offset);
        self.cols
            .transaction_id
            .push(ByteArray::from(rec.transaction_id.as_str()));
        self.cols.ts.push(rec.ts.0);
        match rec.payload.as_ref() {
            Some(p) => {
                self.cols.payload.push(ByteArray::from(p.get()));
                self.cols.payload_def.push(1);
            }
            None => self.cols.payload_def.push(0),
        }

        self.records += 1;
        self.weight += rec.weight();

        if self.cols.rows() >= self.row_group_records {
            self.flush_row_group()?;
        }
        Ok(())
    }

    fn records(&self) -> u64 {
        self.records
    }

    /// The sum of [`Record::weight`] over **every** record pushed, not just the
    /// rows still buffered: the window's close rule asks "how big is this object
    /// getting", and the answer must not drop back to zero every row group.
    /// Uncompressed, and an estimate — the object on the wire is far smaller.
    fn bytes_so_far(&self) -> usize {
        self.weight
    }

    fn finish(&mut self) -> Result<Vec<u8>, WriteError> {
        self.flush_row_group()?;
        let mut file = self.file.take().ok_or_else(finished)?;
        // One pair, after the properties' `queen.envelope=1` and before the
        // footer is written: the queue name, so a file lifted out of the
        // `queue=` layout still says which queue it holds.
        if let Some(queue) = self.queue.take() {
            file.append_key_value_metadata(KeyValue::new(QUEUE_KEY.to_string(), queue));
        }
        // `into_inner` writes the footer and hands the buffer back.
        file.into_inner().map_err(werr)
    }
}

/// Intern one repeated string: returns a refcounted clone when `key` is the
/// cached one, otherwise copies it once and caches it. Records arrive grouped
/// by partition, so the miss rate is one per partition per object.
fn intern(cache: &mut Option<(Arc<str>, Bytes)>, key: &str) -> Bytes {
    if let Some((name, bytes)) = cache.as_ref() {
        if name.as_ref() == key {
            return bytes.clone();
        }
    }
    let bytes = Bytes::copy_from_slice(key.as_bytes());
    *cache = Some((Arc::from(key), bytes.clone()));
    bytes
}

fn column<'a, W: Write + Send>(
    rg: &'a mut SerializedRowGroupWriter<'_, W>,
) -> Result<SerializedColumnWriter<'a>, WriteError> {
    rg.next_column()
        .map_err(werr)?
        .ok_or_else(|| WriteError("parquet: the envelope declares fewer columns than five".into()))
}

fn werr(e: ParquetError) -> WriteError {
    WriteError(format!("parquet: {e}"))
}

fn finished() -> WriteError {
    WriteError("parquet: writer already finished".into())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Micros;
    use serde_json::value::RawValue;

    fn rec(partition: &str, offset: i64, payload: Option<&str>) -> Record {
        Record {
            partition: partition.into(),
            offset,
            transaction_id: format!("txn-{partition}-{offset}"),
            ts: Micros(1_756_980_000_000_000 + offset),
            payload: payload.map(|p| RawValue::from_string(p.to_string()).unwrap()),
        }
    }

    #[test]
    fn schema_constant_parses_into_five_columns() {
        let t = parse_message_type(SCHEMA).unwrap();
        assert_eq!(t.name(), "queen_record");
        let names: Vec<&str> = t.get_fields().iter().map(|f| f.name()).collect();
        assert_eq!(
            names,
            vec![
                COL_PARTITION,
                COL_OFFSET,
                COL_TRANSACTION_ID,
                COL_TS,
                COL_PAYLOAD
            ]
        );
        assert!(!names.contains(&"queue"), "the queue is the path key");
    }

    #[test]
    fn properties_are_pinned() {
        let p = properties(ParquetCodec::Zstd);
        assert_eq!(p.created_by(), "queen-s3");
        assert_eq!(p.writer_version(), WriterVersion::PARQUET_1_0);
        assert_eq!(
            p.compression(&ColumnPath::from(COL_PAYLOAD)),
            PqCompression::ZSTD(ZstdLevel::try_new(3).unwrap())
        );
        assert_eq!(p.data_page_size_limit(), DATA_PAGE_SIZE);
        assert_eq!(p.dictionary_page_size_limit(), DICTIONARY_PAGE_SIZE);
        assert_eq!(p.data_page_row_count_limit(), DATA_PAGE_ROW_COUNT);
        assert_eq!(p.write_batch_size(), WRITE_BATCH_SIZE);
        assert_eq!(p.statistics_truncate_length(), Some(TRUNCATE_LENGTH));
        assert_eq!(p.column_index_truncate_length(), Some(TRUNCATE_LENGTH));
        assert_eq!(
            p.statistics_enabled(&ColumnPath::from(COL_OFFSET)),
            EnabledStatistics::Chunk
        );
        assert!(p.dictionary_enabled(&ColumnPath::from(COL_PARTITION)));
        assert!(!p.dictionary_enabled(&ColumnPath::from(COL_OFFSET)));
        assert_eq!(
            p.encoding(&ColumnPath::from(COL_OFFSET)),
            Some(Encoding::DELTA_BINARY_PACKED)
        );
        assert_eq!(
            p.encoding(&ColumnPath::from(COL_TS)),
            Some(Encoding::DELTA_BINARY_PACKED)
        );
        assert_eq!(
            p.encoding(&ColumnPath::from(COL_TRANSACTION_ID)),
            Some(Encoding::PLAIN)
        );
        // The properties carry the constant pair only; `queen.queue` is
        // appended per object at `finish`, where the queue name is known.
        let kv = p.key_value_metadata().expect("one constant pair");
        assert_eq!(kv.len(), 1);
        assert_eq!(kv[0].key, ENVELOPE_KEY);
        assert_eq!(kv[0].value.as_deref(), Some(ENVELOPE_VALUE));
        assert_eq!(
            properties(ParquetCodec::Snappy).compression(&ColumnPath::from(COL_PAYLOAD)),
            PqCompression::SNAPPY
        );
    }

    #[test]
    fn factory_surface() {
        let f = ParquetFactory::new(ParquetCodec::Zstd, 1000);
        assert_eq!(f.extension(), "parquet");
        assert_eq!(f.content_type(), "application/vnd.apache.parquet");
        assert_eq!(f.format(), Format::Parquet);
        assert_eq!(f.compression(), Compression::Zstd);
        assert_eq!(
            f.describe(),
            format!("queen-s3/{} parquet/59 zstd", super::super::CRATE_VERSION)
        );
        let s = ParquetFactory::new(ParquetCodec::Snappy, 1000);
        assert_eq!(s.compression(), Compression::None);
        assert_eq!(
            s.describe(),
            format!("queen-s3/{} parquet/59 snappy", super::super::CRATE_VERSION)
        );
    }

    #[test]
    fn zero_row_group_size_is_clamped_not_divided_by_zero() {
        let f = ParquetFactory::new(ParquetCodec::Snappy, 0);
        assert_eq!(f.row_group_records, 1);
        let mut w = f.open();
        w.push("q", &rec("p", 0, Some("1"))).unwrap();
        w.push("q", &rec("p", 1, Some("2"))).unwrap();
        assert!(w.finish().unwrap().len() > 4);
    }

    #[test]
    fn empty_object_is_still_a_parquet_file() {
        let mut w = ParquetFactory::new(ParquetCodec::Zstd, 10).open();
        assert_eq!(w.records(), 0);
        assert_eq!(w.bytes_so_far(), 0);
        let bytes = w.finish().unwrap();
        assert_eq!(&bytes[..4], b"PAR1");
        assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
    }

    #[test]
    fn a_writer_is_single_use() {
        let mut w = ParquetFactory::new(ParquetCodec::Zstd, 10).open();
        w.push("q", &rec("p", 0, None)).unwrap();
        assert!(w.finish().is_ok());
        assert_eq!(w.finish(), Err(finished()));
        assert_eq!(w.push("q", &rec("p", 1, None)), Err(finished()));
    }

    #[test]
    fn counters_track_records_and_weight() {
        let mut w = ParquetFactory::new(ParquetCodec::Zstd, 2).open();
        let a = rec("p", 0, Some("{\"a\":1}"));
        let b = rec("p", 1, None);
        w.push("orders", &a).unwrap();
        w.push("orders", &b).unwrap();
        // The row group flushed at record 2; the byte estimate must not have
        // been reset by it.
        assert_eq!(w.records(), 2);
        assert_eq!(w.bytes_so_far(), a.weight() + b.weight());
        w.finish().unwrap();
    }

    #[test]
    fn intern_reuses_the_same_allocation() {
        let mut cache = None;
        let a = intern(&mut cache, "cust-0420");
        let b = intern(&mut cache, "cust-0420");
        assert_eq!(a.as_ptr(), b.as_ptr());
        let c = intern(&mut cache, "cust-0421");
        assert_ne!(a.as_ptr(), c.as_ptr());
        assert_eq!(&c[..], b"cust-0421");
    }
}
