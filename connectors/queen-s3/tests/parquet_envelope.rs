//! The Parquet envelope of PLAN_S3_SINK.md §6.4, pinned from the outside: the
//! bytes are read back with the parquet crate's own reader (no arrow anywhere),
//! exactly as a reader in §7 would see them.
//!
//! Five things are load bearing and each has a test here:
//!
//! 1. **Determinism** — the same records twice give the same bytes, for both
//!    codecs. Exactly-once into the lake is a retried PUT of identical bytes
//!    (plan §4.2), so this is the contract, not a nicety.
//! 2. **Round trip** — every column reads back as it was pushed, `ts` at
//!    microsecond precision, a `None` payload as SQL NULL.
//! 3. **Structure** — row groups close at the configured record count and carry
//!    min/max statistics on `partition`, `offset` and `ts`.
//! 4. **The footer** — `created_by`, the writer version and the two key/value
//!    pairs (`queen.envelope=1` and `queen.queue=<queue>`), none of which may
//!    ever vary for a given queue.
//! 5. **The schema** — a golden string, so an accidental column change fails
//!    here instead of in someone's warehouse.

use std::sync::Arc;

use bytes::Bytes;
use parquet::basic::{Compression as PqCompression, Encoding, Type as PhysicalType};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use parquet::record::RowAccessor;
use parquet::schema::printer::print_schema;
use serde_json::value::RawValue;

use queen_s3::types::{Micros, ParquetCodec, Record};
use queen_s3::writer::parquet::ParquetFactory;
use queen_s3::writer::WriterFactory;

const QUEUE: &str = "orders";

/// The schema every queen-s3 Parquet object must have, verbatim as
/// `parquet::schema::printer` renders it. Changing a column means changing this
/// string, which means reading plan §6.4 first and telling the readers of §7
/// after.
const GOLDEN_SCHEMA: &str = "message queen_record {
  REQUIRED BYTE_ARRAY partition (STRING);
  REQUIRED INT64 offset;
  REQUIRED BYTE_ARRAY transaction_id (STRING);
  REQUIRED INT64 ts (TIMESTAMP(MICROS,true));
  OPTIONAL BYTE_ARRAY payload (STRING);
}
";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Five partitions, deliberately including a unicode name and names that do not
/// sort the way their insertion order does.
const PARTITIONS: [&str; 5] = ["cust-0001", "cust-0420", "cust-9999", "ünïcode-∂", "z"];

/// `n` records over [`PARTITIONS`], sorted by `(partition, offset)` as the
/// engine delivers them (plan §6.4):
///
/// * offsets restart per partition, from a different base and with gaps, so the
///   `offset` column is not one global run;
/// * `ts` is shared by runs of seven records (a segment's `created_at` is shared
///   by every record of that segment) and is non-decreasing within a partition;
/// * every 11th record has a NULL payload;
/// * every 5th payload carries unicode, one of them a lone `"` escape.
fn sample(n: usize) -> Vec<Record> {
    let mut recs: Vec<Record> = (0..n)
        .map(|i| {
            let pi = i % PARTITIONS.len();
            let partition: Arc<str> = PARTITIONS[pi].into();
            let seq = (i / PARTITIONS.len()) as i64;
            let offset = 1_000 * (pi as i64 + 1) + seq * 3;
            let ts = Micros(
                1_756_980_000_000_000 + (pi as i64) * 1_000_000 + (seq / 7) * 250_000 + 918_204,
            );
            let payload = if i % 11 == 0 {
                None
            } else if i % 5 == 0 {
                Some(format!(
                    "{{\"type\":\"païd ✅\",\"note\":\"quote\\\" and \\\\ and ünïcode ∂é\",\"n\":{seq}}}"
                ))
            } else {
                Some(format!(
                    "{{\"type\":\"paid\",\"amount\":{},\"cust\":\"{partition}\"}}",
                    1290 + seq
                ))
            };
            Record {
                partition,
                offset,
                transaction_id: format!("ord-{pi}-{offset:09}"),
                ts,
                payload: payload.map(|p| RawValue::from_string(p).unwrap()),
            }
        })
        .collect();
    recs.sort_by(|a, b| {
        a.partition
            .as_ref()
            .cmp(b.partition.as_ref())
            .then(a.offset.cmp(&b.offset))
    });
    recs
}

/// splitmix64 — a deterministic stand-in for the entropy a real payload
/// carries. A fixture built only from a counter compresses like a counter, and
/// the size numbers this file reports would be a fiction.
fn mix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// `n` records whose payloads are ~200 bytes of order JSON: a fixed template
/// (which every compressor eats) around genuinely varying ids, amounts, skus
/// and addresses (which it does not). One thousand distinct partitions, because
/// the merged layout of plan §6.3 puts a whole window's entities in one object.
fn fat_sample(n: usize) -> Vec<Record> {
    let mut state = 0x5EED_5EED_5EED_5EEDu64;
    let cities = ["Milano", "Roma", "Torino", "Napoli", "Bologna", "Firenze"];
    let channels = ["web", "app-ios", "app-android", "pos", "call-center"];
    let mut recs: Vec<Record> = (0..n)
        .map(|i| {
            let pi = i % 1_000;
            let partition: Arc<str> = format!("cust-{pi:04}").into();
            let seq = (i / 1_000) as i64;
            let r = mix(&mut state);
            let s = mix(&mut state);
            let payload = format!(
                "{{\"type\":\"order.paid\",\"id\":\"ord-{r:016x}\",\"cust\":\"{partition}\",\
                 \"amount\":{}.{:02},\"items\":[{{\"sku\":\"SKU-{:06}\",\"qty\":{}}},\
                 {{\"sku\":\"SKU-{:06}\",\"qty\":{}}}],\"channel\":\"{}\",\
                 \"ship\":{{\"city\":\"{}\",\"zip\":\"{:05}\"}}}}",
                r % 5_000,
                r % 100,
                r % 999_999,
                1 + r % 5,
                s % 999_999,
                1 + s % 3,
                channels[(r % 5) as usize],
                cities[(s % 6) as usize],
                s % 99_999,
            );
            Record {
                partition,
                offset: 1_000 + seq,
                transaction_id: format!("ord-{:016x}", mix(&mut state)),
                ts: Micros(1_756_980_000_000_000 + (i as i64 / 7) * 250_000),
                payload: Some(RawValue::from_string(payload).unwrap()),
            }
        })
        .collect();
    recs.sort_by(|a, b| {
        a.partition
            .as_ref()
            .cmp(b.partition.as_ref())
            .then(a.offset.cmp(&b.offset))
    });
    recs
}

fn write(codec: ParquetCodec, row_group_records: usize, recs: &[Record]) -> Vec<u8> {
    let factory = ParquetFactory::new(codec, row_group_records);
    let mut w = factory.open();
    for r in recs {
        w.push(QUEUE, r).unwrap();
    }
    assert_eq!(w.records(), recs.len() as u64);
    w.finish().unwrap()
}

fn meta(bytes: &[u8]) -> ParquetMetaData {
    let reader = SerializedFileReader::new(Bytes::copy_from_slice(bytes)).unwrap();
    reader.metadata().clone()
}

fn int64_stats(s: &Statistics) -> (i64, i64) {
    match s {
        Statistics::Int64(v) => (*v.min_opt().unwrap(), *v.max_opt().unwrap()),
        other => panic!("expected INT64 statistics, got {other:?}"),
    }
}

fn byte_array_stats(s: &Statistics) -> (String, String) {
    match s {
        Statistics::ByteArray(v) => (
            v.min_opt().unwrap().as_utf8().unwrap().to_string(),
            v.max_opt().unwrap().as_utf8().unwrap().to_string(),
        ),
        other => panic!("expected BYTE_ARRAY statistics, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 1. Determinism
// ---------------------------------------------------------------------------

#[test]
fn the_same_records_produce_byte_identical_objects() {
    let recs = sample(10_000);
    for codec in [ParquetCodec::Zstd, ParquetCodec::Snappy] {
        let a = write(codec, 2_500, &recs);
        let b = write(codec, 2_500, &recs);
        assert_eq!(a.len(), b.len(), "{codec:?}: length differs");
        assert!(
            a == b,
            "{codec:?}: a retried upload must be the same object"
        );
    }
}

#[test]
fn two_factories_of_the_same_configuration_agree() {
    // A restarted process rebuilds the factory from the config; the object must
    // not depend on which factory instance opened the writer.
    let recs = sample(3_000);
    let a = ParquetFactory::new(ParquetCodec::Zstd, 1_000);
    let b = ParquetFactory::new(ParquetCodec::Zstd, 1_000);
    let write_with = |f: &ParquetFactory| {
        let mut w = f.open();
        for r in &recs {
            w.push(QUEUE, r).unwrap();
        }
        w.finish().unwrap()
    };
    assert!(write_with(&a) == write_with(&b));
}

#[test]
fn the_row_group_size_changes_the_bytes_but_not_the_content() {
    let recs = sample(4_000);
    let small = write(ParquetCodec::Zstd, 500, &recs);
    let large = write(ParquetCodec::Zstd, 4_000, &recs);
    assert_ne!(
        small, large,
        "8 row groups cannot encode to the same bytes as 1"
    );
    assert_eq!(meta(&small).num_row_groups(), 8);
    assert_eq!(meta(&large).num_row_groups(), 1);
    assert_eq!(read_back(&small), read_back(&large));
    assert_eq!(read_back(&small).len(), recs.len());
}

#[test]
fn the_two_codecs_differ_only_in_the_compressed_bytes() {
    let recs = sample(2_000);
    let zstd = write(ParquetCodec::Zstd, 1_000, &recs);
    let snappy = write(ParquetCodec::Snappy, 1_000, &recs);
    assert_ne!(zstd, snappy);
    assert_eq!(read_back(&zstd), read_back(&snappy));
    for (bytes, want) in [
        (&zstd, PqCompression::ZSTD(Default::default())),
        (&snappy, PqCompression::SNAPPY),
    ] {
        let m = meta(bytes);
        for col in m.row_group(0).columns() {
            // The level is not recorded in the file, only the codec.
            assert_eq!(
                std::mem::discriminant(&col.compression()),
                std::mem::discriminant(&want),
                "{:?}",
                col.column_path()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 2. Round trip
// ---------------------------------------------------------------------------

/// Every row, through the crate's own row iterator — the reader path of plan §7
/// minus DuckDB.
fn read_back(bytes: &[u8]) -> Vec<(String, i64, String, i64, Option<String>)> {
    let reader = SerializedFileReader::new(Bytes::copy_from_slice(bytes)).unwrap();
    reader
        .get_row_iter(None)
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            (
                row.get_string(0).unwrap().clone(),
                row.get_long(1).unwrap(),
                row.get_string(2).unwrap().clone(),
                row.get_timestamp_micros(3).unwrap(),
                if row.is_null(4).unwrap() {
                    None
                } else {
                    Some(row.get_string(4).unwrap().clone())
                },
            )
        })
        .collect()
}

#[test]
fn every_column_round_trips_including_nulls_and_unicode() {
    let recs = sample(1_111);
    let bytes = write(ParquetCodec::Zstd, 250, &recs);
    let rows = read_back(&bytes);
    assert_eq!(rows.len(), recs.len());

    let mut nulls = 0usize;
    let mut unicode = 0usize;
    for (row, rec) in rows.iter().zip(&recs) {
        assert_eq!(row.0, rec.partition.as_ref());
        assert_eq!(row.1, rec.offset);
        assert_eq!(row.2, rec.transaction_id);
        assert_eq!(row.3, rec.ts.0, "ts must survive at microsecond precision");
        match (&row.4, &rec.payload) {
            (None, None) => nulls += 1,
            (Some(got), Some(want)) => {
                assert_eq!(got, want.get(), "the payload is spliced verbatim");
                if !got.is_ascii() {
                    unicode += 1;
                }
            }
            (got, want) => panic!("payload nullness diverged: {got:?} vs {:?}", want.is_some()),
        }
    }
    assert!(nulls > 0, "the fixture must exercise NULL payloads");
    assert!(unicode > 0, "the fixture must exercise unicode payloads");
    assert!(
        rows.iter().any(|r| !r.0.is_ascii()),
        "and a unicode partition"
    );
}

#[test]
fn a_single_record_and_an_all_null_object_are_readable() {
    let one = vec![Record {
        partition: "p".into(),
        offset: 7,
        transaction_id: "t".into(),
        ts: Micros(1_756_980_000_000_001),
        payload: None,
    }];
    let bytes = write(ParquetCodec::Snappy, 100_000, &one);
    let rows = read_back(&bytes);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].3, 1_756_980_000_000_001);
    assert_eq!(rows[0].4, None);
    let m = meta(&bytes);
    assert_eq!(m.num_row_groups(), 1);
    assert_eq!(m.file_metadata().num_rows(), 1);
    let payload = m.row_group(0).column(4);
    assert_eq!(payload.statistics().unwrap().null_count_opt(), Some(1));
}

// ---------------------------------------------------------------------------
// 3. Structure: row groups, statistics, encodings
// ---------------------------------------------------------------------------

#[test]
fn row_groups_close_at_the_configured_record_count() {
    let recs = sample(2_500);
    let m = meta(&write(ParquetCodec::Zstd, 1_000, &recs));
    assert_eq!(m.num_row_groups(), 3);
    let rows: Vec<i64> = (0..m.num_row_groups())
        .map(|i| m.row_group(i).num_rows())
        .collect();
    assert_eq!(rows, vec![1_000, 1_000, 500]);
    assert_eq!(m.file_metadata().num_rows(), 2_500);

    // An exact multiple must not leave an empty trailing row group behind.
    let m = meta(&write(ParquetCodec::Zstd, 500, &sample(1_000)));
    assert_eq!(m.num_row_groups(), 2);
    assert!((0..m.num_row_groups()).all(|i| m.row_group(i).num_rows() == 500));
}

#[test]
fn statistics_prune_by_partition_offset_and_ts() {
    let recs = sample(2_000);
    let bytes = write(ParquetCodec::Zstd, 500, &recs);
    let m = meta(&bytes);
    assert_eq!(m.num_row_groups(), 4);

    for i in 0..m.num_row_groups() {
        let rg = m.row_group(i);
        let from = i * 500;
        let slice = &recs[from..from + 500];

        let (min, max) = byte_array_stats(rg.column(0).statistics().expect("partition stats"));
        assert_eq!(
            min,
            slice.iter().map(|r| r.partition.as_ref()).min().unwrap()
        );
        assert_eq!(
            max,
            slice.iter().map(|r| r.partition.as_ref()).max().unwrap()
        );

        let (min, max) = int64_stats(rg.column(1).statistics().expect("offset stats"));
        assert_eq!(min, slice.iter().map(|r| r.offset).min().unwrap());
        assert_eq!(max, slice.iter().map(|r| r.offset).max().unwrap());

        let (min, max) = int64_stats(rg.column(3).statistics().expect("ts stats"));
        assert_eq!(min, slice.iter().map(|r| r.ts.0).min().unwrap());
        assert_eq!(max, slice.iter().map(|r| r.ts.0).max().unwrap());

        assert_eq!(rg.column(1).statistics().unwrap().null_count_opt(), Some(0));
    }
}

#[test]
fn the_column_types_and_encodings_are_the_pinned_ones() {
    let bytes = write(ParquetCodec::Zstd, 1_000, &sample(2_000));
    let m = meta(&bytes);
    let rg = m.row_group(0);

    let want_type = [
        PhysicalType::BYTE_ARRAY,
        PhysicalType::INT64,
        PhysicalType::BYTE_ARRAY,
        PhysicalType::INT64,
        PhysicalType::BYTE_ARRAY,
    ];
    for (i, want) in want_type.iter().enumerate() {
        assert_eq!(rg.column(i).column_type(), *want, "column {i}");
    }

    let encodings = |i: usize| rg.column(i).encodings().collect::<Vec<_>>();
    // partition is dictionary encoded: the dictionary page itself is PLAIN (v1
    // spells it PLAIN_DICTIONARY) and the data pages index into it.
    assert!(
        encodings(0).contains(&Encoding::PLAIN_DICTIONARY)
            || encodings(0).contains(&Encoding::RLE_DICTIONARY),
        "column 0 (partition) must be dictionary encoded, got {:?}",
        encodings(0)
    );
    for i in [1usize, 3] {
        assert!(
            encodings(i).contains(&Encoding::DELTA_BINARY_PACKED),
            "column {i} must be delta encoded, got {:?}",
            encodings(i)
        );
        assert!(!encodings(i).contains(&Encoding::PLAIN_DICTIONARY));
    }
    for i in [2usize, 4] {
        assert!(
            encodings(i).contains(&Encoding::PLAIN),
            "column {i} must be plain, got {:?}",
            encodings(i)
        );
        assert!(!encodings(i).contains(&Encoding::PLAIN_DICTIONARY));
    }
}

// ---------------------------------------------------------------------------
// 4. The footer
// ---------------------------------------------------------------------------

#[test]
fn the_footer_carries_no_varying_field() {
    for codec in [ParquetCodec::Zstd, ParquetCodec::Snappy] {
        let m = meta(&write(codec, 1_000, &sample(100)));
        let f = m.file_metadata();
        assert_eq!(
            f.created_by(),
            Some("queen-s3"),
            "created_by is a constant: no crate version, no host, no clock"
        );
        assert_eq!(f.version(), 1, "writer version PARQUET_1_0, pinned");
        let kv = f.key_value_metadata().expect("the envelope marker");
        assert_eq!(kv.len(), 2, "exactly two constant pairs, got {kv:?}");
        assert_eq!(kv[0].key, "queen.envelope");
        assert_eq!(kv[0].value.as_deref(), Some("1"));
        // The queue is not a column any more (it is the `queue=` path key), so
        // the footer is what keeps a file copied out of the layout
        // self-describing.
        assert_eq!(kv[1].key, "queen.queue");
        assert_eq!(kv[1].value.as_deref(), Some(QUEUE));
    }
}

/// Two queues, two footers, everything else identical: `queen.queue` is the
/// only thing in the object that varies with the queue, and it is not in a row.
#[test]
fn the_footer_names_the_queue_and_nothing_else_does() {
    let recs = sample(200);
    let write_as = |queue: &str| {
        let f = ParquetFactory::new(ParquetCodec::Zstd, 1_000);
        let mut w = f.open();
        for r in &recs {
            w.push(queue, r).unwrap();
        }
        w.finish().unwrap()
    };
    let orders = write_as("orders");
    let audit = write_as("audit");
    assert_ne!(orders, audit, "the footer pair differs");
    assert_eq!(
        orders.len(),
        audit.len() + "orders".len() - "audit".len(),
        "and it is the ONLY difference: one string, once, in the footer"
    );
    for (bytes, queue) in [(&orders, "orders"), (&audit, "audit")] {
        let m = meta(bytes);
        let kv = m.file_metadata().key_value_metadata().unwrap();
        assert_eq!(kv[1].value.as_deref(), Some(queue));
    }
    assert_eq!(read_back(&orders), read_back(&audit), "the rows are equal");
}

#[test]
fn the_object_is_framed_as_a_parquet_file() {
    let bytes = write(ParquetCodec::Zstd, 1_000, &sample(10));
    assert_eq!(&bytes[..4], b"PAR1");
    assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
}

// ---------------------------------------------------------------------------
// 5. The schema
// ---------------------------------------------------------------------------

#[test]
fn the_schema_is_the_golden_envelope() {
    let bytes = write(ParquetCodec::Zstd, 1_000, &sample(10));
    let m = meta(&bytes);
    let mut printed = Vec::new();
    print_schema(&mut printed, m.file_metadata().schema());
    assert_eq!(String::from_utf8(printed).unwrap(), GOLDEN_SCHEMA);
}

// ---------------------------------------------------------------------------
// The factory's own surface
// ---------------------------------------------------------------------------

#[test]
fn describe_names_the_parquet_major_that_cargo_lock_pins() {
    let lock = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.lock")).unwrap();
    let locked = lock
        .split("\n[[package]]\n")
        .find(|p| p.starts_with("name = \"parquet\"\n"))
        .expect("parquet must be in Cargo.lock")
        .lines()
        .find_map(|l| l.strip_prefix("version = \""))
        .map(|v| v.trim_end_matches('"').to_string())
        .expect("a version line");
    let major = locked.split('.').next().unwrap();
    assert_eq!(
        major, "59",
        "Cargo.lock moved to parquet {locked}: `describe()` hard-codes the major, \
         so bump it there (and expect the bytes to move — plan §6.4)"
    );
    let d = ParquetFactory::new(ParquetCodec::Zstd, 1).describe();
    assert_eq!(
        d,
        format!("queen-s3/{} parquet/59 zstd", env!("CARGO_PKG_VERSION"))
    );
    assert!(d.contains(&format!("parquet/{major}")));
}

// ---------------------------------------------------------------------------
// The sorted-input assertion
// ---------------------------------------------------------------------------

/// The engine guarantees `(partition, offset)` order and the writer asserts it
/// in debug builds — a wrong order would silently poison the row-group
/// statistics that plan §6.4 sells as the pruning story.
#[cfg(debug_assertions)]
#[test]
fn out_of_order_records_trip_the_debug_assertion() {
    let ok = |a: &Record, b: &Record| {
        let f = ParquetFactory::new(ParquetCodec::Snappy, 1_000);
        let mut w = f.open();
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            w.push(QUEUE, a).unwrap();
            w.push(QUEUE, b).unwrap();
        }));
        std::panic::set_hook(hook);
        r.is_ok()
    };
    let at = |p: &str, o: i64| Record {
        partition: p.into(),
        offset: o,
        transaction_id: "t".into(),
        ts: Micros(1_756_980_000_000_000),
        payload: None,
    };

    assert!(ok(&at("a", 1), &at("a", 2)), "ascending offsets are fine");
    assert!(
        ok(&at("a", 9), &at("b", 1)),
        "a new partition may restart offsets"
    );
    assert!(
        !ok(&at("a", 2), &at("a", 1)),
        "a descending offset must trip"
    );
    assert!(!ok(&at("a", 1), &at("a", 1)), "a repeated offset must trip");
    assert!(
        !ok(&at("b", 1), &at("a", 9)),
        "a descending partition must trip"
    );
}

// ---------------------------------------------------------------------------
// Size sanity — the number the deploy page quotes
// ---------------------------------------------------------------------------

#[test]
fn size_sanity_at_a_hundred_thousand_records() {
    let recs = fat_sample(100_000);
    let raw: usize = recs
        .iter()
        .map(|r| r.payload.as_ref().unwrap().get().len())
        .sum();
    let mean = raw / recs.len();
    assert!(
        (190..=220).contains(&mean),
        "the fixture claims ~200-byte payloads, got {mean}"
    );

    let mut report = Vec::new();
    for codec in [ParquetCodec::Zstd, ParquetCodec::Snappy] {
        let bytes = write(codec, 100_000, &recs);
        let m = meta(&bytes);
        assert_eq!(m.file_metadata().num_rows(), 100_000);
        report.push((codec, bytes.len()));
        // A compressed object of ~200-byte JSON must beat the raw payload by a
        // wide margin; if it ever does not, a codec or an encoding fell off.
        assert!(
            bytes.len() < raw / 2,
            "{codec:?}: {} bytes for {raw} of payload is not compression",
            bytes.len()
        );
    }
    println!("--- queen-s3 parquet size sanity: 100 000 records, {raw} bytes of payload ({mean} B mean) ---");
    for (codec, size) in &report {
        println!(
            "{codec:?}: {size} bytes, {:.2} B/record, {:.1}x smaller than the raw payload",
            *size as f64 / 100_000.0,
            raw as f64 / *size as f64
        );
    }
}
