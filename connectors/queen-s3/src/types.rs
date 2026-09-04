//! The shared vocabulary of the connector. Every module speaks in these types and
//! none of them owns I/O.
//!
//! Two rules are encoded here rather than in prose:
//!
//! * **There is one time type, [`Micros`], and every value of it comes from the
//!   broker** — a segment's `ts`, the discovery `safeTime`, a partition's
//!   `lastWriteAt`. They are PostgreSQL's `clock_timestamp()` (003_log_push.sql:
//!   138-142), rendered at microsecond precision (032_log_fetch.sql:250-251).
//!   Nothing in this crate compares a `Micros` to `SystemTime`; a window boundary
//!   derived from the sink's own clock would be wrong by construction (plan §12).
//! * **A record's payload is JSON and travels as [`serde_json::value::RawValue`]**:
//!   push takes `&RawValue` (server/src/handlers/data.rs:138), so it is JSON by
//!   construction, and the writers splice it; nothing parses it into a tree.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

// ---------------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------------

/// Microseconds since the Unix epoch, UTC, on PostgreSQL's clock.
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct Micros(pub i64);

impl Micros {
    /// The window start of `start=earliest`: before every record.
    pub const MIN: Micros = Micros(i64::MIN);
    pub const SECOND: Micros = Micros(1_000_000);
    pub const MINUTE: Micros = Micros(60 * 1_000_000);
    pub const HOUR: Micros = Micros(3_600 * 1_000_000);
    pub const DAY: Micros = Micros(86_400 * 1_000_000);

    pub fn from_millis(ms: i64) -> Micros {
        Micros(ms.saturating_mul(1_000))
    }

    pub fn saturating_add(self, other: Micros) -> Micros {
        Micros(self.0.saturating_add(other.0))
    }

    pub fn saturating_sub(self, other: Micros) -> Micros {
        Micros(self.0.saturating_sub(other.0))
    }

    /// Round DOWN to a multiple of `unit` (e.g. [`Micros::HOUR`]). Correct for
    /// negative values too (floors toward −∞), which `MIN` needs.
    pub fn floor_to(self, unit: Micros) -> Micros {
        debug_assert!(unit.0 > 0);
        if self == Micros::MIN {
            return self;
        }
        Micros(self.0.div_euclid(unit.0) * unit.0)
    }

    /// The first multiple of `unit` strictly ABOVE `self` — the next alignment
    /// boundary a window may not cross.
    pub fn next_boundary(self, unit: Micros) -> Micros {
        debug_assert!(unit.0 > 0);
        if self == Micros::MIN {
            // A window starting at −∞ is bounded by the first boundary at or
            // above the first real record, which the engine picks from data; the
            // formal answer here is "no constraint yet".
            return Micros::MIN;
        }
        Micros((self.0.div_euclid(unit.0) + 1) * unit.0)
    }

    /// Parse the broker's rendering: `YYYY-MM-DDTHH:MM:SS[.f{1,6}]Z`. Anything
    /// else is an error; in particular no offsets other than `Z`, because the
    /// broker never emits one (the to_char UTC pin, 032_log_fetch.sql:234-251).
    pub fn parse_iso(s: &str) -> Result<Micros, String> {
        let b = s.as_bytes();
        let bad = || format!("not a broker timestamp: {s:?}");
        if b.len() < 20
            || b[4] != b'-'
            || b[7] != b'-'
            || b[10] != b'T'
            || b[13] != b':'
            || b[16] != b':'
        {
            return Err(bad());
        }
        let num = |from: usize, to: usize| -> Result<i64, String> {
            let mut v: i64 = 0;
            for &c in &b[from..to] {
                if !c.is_ascii_digit() {
                    return Err(bad());
                }
                v = v * 10 + (c - b'0') as i64;
            }
            Ok(v)
        };
        let y = num(0, 4)?;
        let mo = num(5, 7)?;
        let d = num(8, 10)?;
        let h = num(11, 13)?;
        let mi = num(14, 16)?;
        let se = num(17, 19)?;
        let mut i = 19;
        let mut frac: i64 = 0;
        if b[i] == b'.' {
            i += 1;
            let start = i;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            let digits = i - start;
            if digits == 0 || digits > 6 {
                return Err(bad());
            }
            frac = num(start, i)?;
            for _ in digits..6 {
                frac *= 10;
            }
        }
        if i + 1 != b.len() || b[i] != b'Z' {
            return Err(bad());
        }
        if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || se > 60 {
            return Err(bad());
        }
        let days = days_from_civil(y, mo, d);
        let secs = days * 86_400 + h * 3_600 + mi * 60 + se;
        Ok(Micros(secs * 1_000_000 + frac))
    }

    /// Render exactly as the broker does: six fractional digits, trailing `Z`.
    pub fn to_iso(self) -> String {
        if self == Micros::MIN {
            return "-inf".to_string();
        }
        let secs = self.0.div_euclid(1_000_000);
        let frac = self.0.rem_euclid(1_000_000);
        let days = secs.div_euclid(86_400);
        let sod = secs.rem_euclid(86_400);
        let (y, m, d) = civil_from_days(days);
        format!(
            "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{frac:06}Z",
            sod / 3_600,
            (sod % 3_600) / 60,
            sod % 60
        )
    }
}

impl fmt::Display for Micros {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_iso())
    }
}

/// Days since 1970-01-01 for a proleptic Gregorian civil date (Hinnant).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Civil date for days since 1970-01-01 (Hinnant).
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

// ---------------------------------------------------------------------------
// Records and bounds
// ---------------------------------------------------------------------------

/// One record as the broker renders it (server/src/handlers/fetch.rs:606-629)
/// plus its coordinates. There are deliberately no headers: a stored frame has
/// no header map and the broker refuses to fake one (fetch.rs:610-613).
#[derive(Clone, Debug)]
pub struct Record {
    pub partition: Arc<str>,
    /// Absolute offset within the partition; with `ts`, co-monotone per
    /// partition (003_log_push.sql:219-223).
    pub offset: i64,
    /// The message's addressable identity — the client's transaction id, which
    /// `GET /api/v1/messages/:pid/:txn` is keyed by. May repeat across the log.
    pub transaction_id: String,
    /// The SEGMENT's `created_at`: every record of one segment shares it.
    pub ts: Micros,
    /// `None` when the wire carried `"payload":null`. Otherwise valid JSON,
    /// decrypted by the broker when at-rest encryption is on (fetch.rs:620).
    pub payload: Option<Box<RawValue>>,
}

impl PartialEq for Record {
    fn eq(&self, o: &Record) -> bool {
        self.partition == o.partition
            && self.offset == o.offset
            && self.transaction_id == o.transaction_id
            && self.ts == o.ts
            && self.payload.as_ref().map(|p| p.get()) == o.payload.as_ref().map(|p| p.get())
    }
}

impl Eq for Record {}

impl Record {
    /// Bytes this record contributes to a buffer budget: the payload plus a fixed
    /// allowance for the envelope. Never zero, so a run of `null` payloads still
    /// moves a size trigger (the same rule the broker's `minBytes` follows).
    pub fn weight(&self) -> usize {
        64 + self.transaction_id.len()
            + self.partition.len()
            + self.payload.as_ref().map(|p| p.get().len()).unwrap_or(4)
    }
}

/// What the broker knows about one partition, from discovery
/// (`partitions/changed`) or from a fetch's bounds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionBounds {
    pub name: Arc<str>,
    /// The allocator: the last offset assigned, `-1` for a never-written lane.
    /// The high watermark is `last_offset + 1`.
    pub last_offset: i64,
    /// The retention watermark: the first offset still stored.
    pub log_start: i64,
    /// Quantized to one real change per second (001_log_schema.sql:39-45).
    /// `None` when the source was a fetch, which does not carry it.
    pub last_write_at: Option<Micros>,
}

/// One entry of a `POST /api/v1/fetch` request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchRequestEntry {
    pub queue: String,
    pub partition: Arc<str>,
    pub offset: i64,
    /// Per-entry ceiling over COMPRESSED segment bytes; the broker clamps to
    /// 8 MiB (fetch.rs:89-90). `None` = the broker default (1 MiB).
    pub max_bytes: Option<i64>,
}

/// Per-entry error markers, spelled as the broker spells them
/// (032_log_fetch.sql:25-35, 46-53).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FetchError {
    /// `offset < logStart` (retention passed the caller by) or `offset > high`.
    OffsetOutOfRange,
    /// No queue of that name for this tenant.
    UnknownTopicOrPartition,
    Other(String),
}

impl FetchError {
    pub fn from_wire(s: &str) -> FetchError {
        match s {
            "OFFSET_OUT_OF_RANGE" => FetchError::OffsetOutOfRange,
            "UNKNOWN_TOPIC_OR_PARTITION" => FetchError::UnknownTopicOrPartition,
            other => FetchError::Other(other.to_string()),
        }
    }
}

/// One entry of a fetch response, index-aligned with the request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchedEntry {
    pub queue: String,
    pub partition: Arc<str>,
    /// In offset order, contiguous from the requested offset (possibly empty).
    pub records: Vec<Record>,
    /// The next offset the log will assign (`last + 1`), reported even when
    /// the entry carried nothing.
    pub high_watermark: i64,
    pub log_start_offset: i64,
    pub error: Option<FetchError>,
}

// ---------------------------------------------------------------------------
// Discovery (`POST /api/v1/partitions/changed`)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangedRequestEntry {
    pub queue: String,
    /// `None` = full enumeration by name; `Some(t)` = partitions with
    /// `lastWriteAt >= t`, by `(lastWriteAt, name)`.
    pub since: Option<Micros>,
    /// The opaque cursor echoed from a previous [`ChangedEntry::next`].
    pub after: Option<String>,
    /// Clamped by the broker to 1..=1000.
    pub limit: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangedEntry {
    pub queue: String,
    pub partitions: Vec<PartitionBounds>,
    /// `Some` when there is another page.
    pub next: Option<String>,
    /// `UNKNOWN_TOPIC_OR_PARTITION` for a queue this tenant does not have.
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangedResponse {
    /// No segment with `ts < safe_time` can still become visible (plan §5.2).
    pub safe_time: Micros,
    /// The broker could not see every session and answered its floor instead.
    pub safe_time_degraded: bool,
    pub entries: Vec<ChangedEntry>,
}

// ---------------------------------------------------------------------------
// Formats and layout
// ---------------------------------------------------------------------------

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Jsonl,
    Parquet,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    Zstd,
    Gzip,
    None,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParquetCodec {
    Zstd,
    Snappy,
}

/// `merged` (default): one object per window per queue, the partition a column
/// inside it. `per-partition`: one object per (window, partition),
/// Connect-shaped keys. The QUEUE is the `queue=` path key in both, never a
/// column (`writer`).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Layout {
    Merged,
    PerPartition,
}

/// Windows never straddle an alignment boundary, so `dt=`/`hour=` are exact.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Align {
    Hour,
    Day,
    None,
}

impl Align {
    pub fn unit(self) -> Option<Micros> {
        match self {
            Align::Hour => Some(Micros::HOUR),
            Align::Day => Some(Micros::DAY),
            Align::None => None,
        }
    }
}

/// Where a queue with no committed pointer starts.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Start {
    Latest,
    Earliest,
}

// ---------------------------------------------------------------------------
// The KV documents (plan §4.3) — namespace `queen-s3`, keys
// `s3:<sink>:<esc queue>:{intent,committed,lease}`. Small, far under the
// 64 KiB value ceiling; expiry "forever" for intent and committed.
// ---------------------------------------------------------------------------

/// Written BEFORE the upload. Fixes `T_k` so a retry rebuilds the same bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Intent {
    pub k: u64,
    pub t_start: Micros,
    pub t_end: Micros,
    pub format: Format,
    pub compression: Compression,
    pub layout: Layout,
    /// `queen-s3/<version> <writer>/<version>` — which code wrote it.
    pub writer: String,
}

/// Written AFTER the upload and the manifest. The commit pointer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Committed {
    pub k: u64,
    pub t_end: Micros,
    /// The manifest key of window `k`.
    pub manifest: String,
    pub records: u64,
    pub bytes: u64,
    /// Wall clock of the sink at commit, milliseconds — informational only.
    pub committed_at_ms: i64,
}

/// Queue ownership across instances (plan §6.6), TTL'd.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Lease {
    pub instance: String,
    pub incarnation: String,
    pub since_ms: i64,
}

// ---------------------------------------------------------------------------
// Bucket sidecars
// ---------------------------------------------------------------------------

/// One data object of a window: the whole window (`merged`) or one partition
/// of it (`per-partition`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestObject {
    pub key: String,
    pub bytes: u64,
    pub records: u64,
    pub sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_offset: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_offset: Option<i64>,
}

/// An offset range retention deleted before the sink read it (plan §4.6).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LostRange {
    pub partition: String,
    /// First missing offset (inclusive).
    pub from: i64,
    /// Last missing offset (inclusive).
    pub to: i64,
}

/// `_queen/<esc queue>/windows/<k>.json` — one per committed window. The only
/// place a wall-clock value is written (`committed_at`): the data object itself
/// carries none, so a retry is byte-identical (plan §4.2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    pub sink: String,
    pub queue: String,
    pub k: u64,
    pub t_start: Micros,
    pub t_end: Micros,
    pub format: Format,
    pub compression: Compression,
    pub layout: Layout,
    pub objects: Vec<ManifestObject>,
    pub records: u64,
    pub bytes: u64,
    /// Distinct partitions with at least one record in the window.
    pub partitions: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_ts: Option<Micros>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ts: Option<Micros>,
    #[serde(default)]
    pub lost: Vec<LostRange>,
    pub writer: String,
    pub committed_at: String,
}

/// `_queen/<esc queue>/checkpoint/<k>.json.zst` — the position cache as of
/// window `k` committed: for each partition the next offset not yet shipped.
/// A cache: a stale or missing entry costs re-reads, never correctness
/// (plan §4.5).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Checkpoint {
    pub k: u64,
    pub t_end: Micros,
    /// `(partition name, next offset)`, sorted by name so the object is
    /// deterministic.
    pub positions: Vec<(String, i64)>,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why a call did not produce an answer. Coarse on purpose (the facade's
/// `queen::Error` precedent): every variant maps to "back off and retry the same
/// step" except [`SinkError::Precondition`] and [`SinkError::Config`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SinkError {
    /// The request never completed: DNS, connect, TLS, timeout, reset.
    Transport(String),
    /// The service answered with a non-success status.
    Status {
        code: u16,
        body: String,
        /// `Retry-After`, milliseconds — the proxy sets it on every 429
        /// (proxy/src/errors.rs) and S3 on some 503s.
        retry_after_ms: Option<i64>,
    },
    /// A 2xx whose body this client cannot read.
    Body(String),
    /// A conditional KV write lost its precondition: another instance owns the
    /// queue, or the pointer moved under us. Never retried blindly.
    Precondition {
        failed_index: usize,
        reason: String,
        version: i64,
        value: serde_json::Value,
    },
    /// A checksum did not match what was sent (ETag / Content-MD5).
    Integrity(String),
    Config(String),
}

impl fmt::Display for SinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SinkError::Transport(s) => write!(f, "transport: {s}"),
            SinkError::Status { code, body, .. } => {
                let b: String = body.chars().take(200).collect();
                write!(f, "status {code}: {b}")
            }
            SinkError::Body(s) => write!(f, "body: {s}"),
            SinkError::Precondition {
                failed_index,
                reason,
                version,
                ..
            } => write!(
                f,
                "precondition lost at op {failed_index}: {reason} (winner version {version})"
            ),
            SinkError::Integrity(s) => write!(f, "integrity: {s}"),
            SinkError::Config(s) => write!(f, "config: {s}"),
        }
    }
}

impl std::error::Error for SinkError {}

impl SinkError {
    /// Whether the same step may simply be tried again after a backoff.
    pub fn is_retriable(&self) -> bool {
        match self {
            SinkError::Transport(_) | SinkError::Body(_) | SinkError::Integrity(_) => true,
            SinkError::Status { code, .. } => *code == 408 || *code == 429 || *code >= 500,
            SinkError::Precondition { .. } | SinkError::Config(_) => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_round_trip_at_micro_precision() {
        for s in [
            "1970-01-01T00:00:00.000000Z",
            "2026-09-04T10:03:41.918204Z",
            "1999-12-31T23:59:59.999999Z",
            "2000-02-29T12:00:00.000001Z",
            "2400-02-29T00:00:00.000000Z",
            "1969-12-31T23:59:59.999999Z",
            "0001-01-01T00:00:00.000000Z",
        ] {
            let m = Micros::parse_iso(s).unwrap();
            assert_eq!(m.to_iso(), s, "round trip of {s}");
        }
    }

    #[test]
    fn known_epoch_values() {
        assert_eq!(
            Micros::parse_iso("1970-01-01T00:00:00Z").unwrap(),
            Micros(0)
        );
        assert_eq!(
            Micros::parse_iso("2026-09-04T00:00:00Z").unwrap(),
            Micros(1_788_480_000 * 1_000_000)
        );
        assert_eq!(Micros(-1).to_iso(), "1969-12-31T23:59:59.999999Z");
    }

    #[test]
    fn short_fractions_are_scaled() {
        assert_eq!(
            Micros::parse_iso("1970-01-01T00:00:00.5Z").unwrap(),
            Micros(500_000)
        );
        assert_eq!(
            Micros::parse_iso("1970-01-01T00:00:00.123Z").unwrap(),
            Micros(123_000)
        );
    }

    #[test]
    fn rejects_what_the_broker_never_emits() {
        for s in [
            "2026-09-04T10:03:41.918204+00:00",
            "2026-09-04 10:03:41Z",
            "2026-09-04T10:03:41.1234567Z",
            "2026-13-04T10:03:41Z",
            "2026-09-04T10:03:41",
            "",
            "garbage",
        ] {
            assert!(Micros::parse_iso(s).is_err(), "{s} must not parse");
        }
    }

    #[test]
    fn floor_and_next_boundary() {
        let t = Micros::parse_iso("2026-09-04T10:03:41.918204Z").unwrap();
        assert_eq!(
            t.floor_to(Micros::HOUR).to_iso(),
            "2026-09-04T10:00:00.000000Z"
        );
        assert_eq!(
            t.next_boundary(Micros::HOUR).to_iso(),
            "2026-09-04T11:00:00.000000Z"
        );
        assert_eq!(
            t.floor_to(Micros::DAY).to_iso(),
            "2026-09-04T00:00:00.000000Z"
        );
        let exact = Micros::parse_iso("2026-09-04T11:00:00Z").unwrap();
        assert_eq!(exact.floor_to(Micros::HOUR), exact);
        assert_eq!(
            exact.next_boundary(Micros::HOUR).to_iso(),
            "2026-09-04T12:00:00.000000Z"
        );
        assert_eq!(
            Micros(-1).floor_to(Micros::HOUR).to_iso(),
            "1969-12-31T23:00:00.000000Z"
        );
        assert_eq!(Micros::MIN.floor_to(Micros::HOUR), Micros::MIN);
    }

    #[test]
    fn record_equality_compares_payload_text() {
        let p = |s: &str| Some(RawValue::from_string(s.to_string()).unwrap());
        let a = Record {
            partition: "p".into(),
            offset: 1,
            transaction_id: "t".into(),
            ts: Micros(1),
            payload: p("{\"a\":1}"),
        };
        let mut b = a.clone();
        assert_eq!(a, b);
        b.payload = p("{\"a\":2}");
        assert_ne!(a, b);
        b.payload = None;
        assert_ne!(a, b);
        assert!(b.weight() > 0);
    }

    #[test]
    fn documents_serialize_camel_case() {
        let i = Intent {
            k: 7,
            t_start: Micros(0),
            t_end: Micros(10),
            format: Format::Jsonl,
            compression: Compression::Zstd,
            layout: Layout::Merged,
            writer: "queen-s3/1.5.0".into(),
        };
        let s = serde_json::to_string(&i).unwrap();
        assert!(s.contains("\"tStart\":0"), "{s}");
        assert!(s.contains("\"format\":\"jsonl\""), "{s}");
        assert!(s.contains("\"layout\":\"merged\""), "{s}");
        let back: Intent = serde_json::from_str(&s).unwrap();
        assert_eq!(back, i);
        let l: Layout = serde_json::from_str("\"per-partition\"").unwrap();
        assert_eq!(l, Layout::PerPartition);
    }

    #[test]
    fn retriability() {
        assert!(SinkError::Transport("x".into()).is_retriable());
        assert!(SinkError::Status {
            code: 503,
            body: String::new(),
            retry_after_ms: None
        }
        .is_retriable());
        assert!(!SinkError::Status {
            code: 403,
            body: String::new(),
            retry_after_ms: None
        }
        .is_retriable());
        assert!(!SinkError::Config("x".into()).is_retriable());
    }
}
