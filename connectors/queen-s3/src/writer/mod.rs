//! The record writers: one window's records in, one object's bytes out.
//!
//! The contract every writer must keep, and the tests pin (plan §6.4):
//!
//! * **Deterministic**: the same records in the same order produce the same
//!   bytes, on every run, in every process. No wall clock, no random ids, no
//!   environment-dependent metadata inside the object. A retried upload of a
//!   window is byte-identical to the first (plan §4.2), which is the whole basis
//!   of exactly-once here.
//! * **Records arrive sorted by `(partition, offset)`**; the engine guarantees
//!   it, the writer may assert it.
//! * **The payload is spliced, never re-serialised**: JSONL copies the
//!   `RawValue` text; Parquet stores it as a UTF8 column.
//! * **The queue name is not in the record**: it is the `queue=<esc>` key of
//!   the object's path (plan §6.3), the way Hive and Spark write a partition
//!   key, and a column repeating it is a table a metastore refuses. `push`
//!   still takes it because the Parquet writer puts it in the file footer
//!   (`queen.queue`), so an object copied out of the layout still names its
//!   queue.

pub mod jsonl;
pub mod parquet;

use crate::types::{Compression, Format, ParquetCodec, Record};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteError(pub String);

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for WriteError {}

/// One object in the making. `finish` consumes the accumulated records and
/// returns the complete object; a writer is single-use.
pub trait RecordWriter: Send {
    /// `queue` is NOT written into the row — it is the path key. Parquet
    /// records it once, in the file's key/value metadata; JSONL ignores it.
    fn push(&mut self, queue: &str, rec: &Record) -> Result<(), WriteError>;
    fn records(&self) -> u64;
    /// Bytes buffered so far — a size estimate for the window's close rule,
    /// not necessarily the final object size.
    fn bytes_so_far(&self) -> usize;
    fn finish(&mut self) -> Result<Vec<u8>, WriteError>;
}

/// Opens writers for a configured (format, compression) and knows the
/// object-key extension and content type.
pub trait WriterFactory: Send + Sync {
    fn open(&self) -> Box<dyn RecordWriter>;
    /// `jsonl.zst`, `jsonl.gz`, `jsonl`, `parquet`.
    fn extension(&self) -> &'static str;
    fn content_type(&self) -> &'static str;
    /// `queen-s3/<version> jsonl+zstd` / `queen-s3/<version> parquet/59 zstd` —
    /// recorded in the intent and the manifest.
    fn describe(&self) -> String;
    fn format(&self) -> Format;
    fn compression(&self) -> Compression;
}

/// The writer configuration the factory is built from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriterConfig {
    pub format: Format,
    pub compression: Compression,
    pub parquet_codec: ParquetCodec,
    /// zstd level for JSONL objects; fixed per configuration, part of
    /// determinism.
    pub zstd_level: i32,
    /// Records per Parquet row group; fixed, part of determinism.
    pub parquet_row_group_records: usize,
}

impl Default for WriterConfig {
    fn default() -> WriterConfig {
        WriterConfig {
            format: Format::Jsonl,
            compression: Compression::Zstd,
            parquet_codec: ParquetCodec::Zstd,
            zstd_level: 3,
            parquet_row_group_records: 100_000,
        }
    }
}

pub const CRATE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Build the factory for a configuration.
pub fn factory(cfg: &WriterConfig) -> Box<dyn WriterFactory> {
    match cfg.format {
        Format::Jsonl => Box::new(jsonl::JsonlFactory::new(cfg.compression, cfg.zstd_level)),
        Format::Parquet => Box::new(parquet::ParquetFactory::new(
            cfg.parquet_codec,
            cfg.parquet_row_group_records,
        )),
    }
}
