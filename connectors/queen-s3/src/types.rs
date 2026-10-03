//! The shared vocabulary of the connector. Every module speaks in these types and
//! none of them owns I/O.
//!
//! Two rules are encoded here rather than in prose:
//!
//! * **There is one time type, [`Micros`], and every value of it comes from the
//!   broker** — a record's `ts`, the discovery `safeTime`, a partition's
//!   `lastWriteAt`. They are stamps of the replicated log: an append is stamped
//!   `max(the planning cycle's clock, the partition's last stamp + 1)`, and every
//!   cycle's clock is above every stamp already committed or in flight, so stamps
//!   strictly increase in log order across the whole broker
//!   (server/src/rsm/planner/push.rs). Nothing in this crate compares a `Micros`
//!   to `SystemTime`; a window boundary derived from the sink's own clock would
//!   be wrong by construction (plan §12).
//! * **A record's payload is JSON and travels as [`serde_json::value::RawValue`]**:
//!   every push path stores the JSON document it was given, and the broker's
//!   fetch renders a payload that is not JSON as a JSON string
//!   (server/src/rsm/facade/real/phase2/reads.rs `payload_json`), so the writers
//!   splice it; nothing parses it into a tree.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

// ---------------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------------

/// Microseconds since the Unix epoch, UTC, on the broker's log clock: the stamp
/// an append got in the replicated log, never a wall clock of this process.
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
    /// broker never emits one (`iso_us`, server/src/rsm/planner/timers.rs,
    /// always six fractional digits and a literal `Z`).
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

/// One record as the broker's fetch reads it
/// (server/src/rsm/facade/real/phase2/reads.rs `fetch_once`) plus its
/// coordinates. There are deliberately no headers: a stored frame has no header
/// map and the fetch does not fake one.
///
/// `(partition, offset)` identifies a record within one incarnation of its
/// partition; a partition deleted and created again under the same name
/// restarts at offset 0, so the identity across incarnations — and across the
/// lake — is `(partition, offset, ts)`.
#[derive(Clone, Debug)]
pub struct Record {
    pub partition: Arc<str>,
    /// Absolute offset within the partition; with `ts`, co-monotone per
    /// partition: a partition's appends are stamped in offset order, each above
    /// the one before.
    pub offset: i64,
    /// The message's addressable identity — the client's transaction id, which
    /// `GET /api/v1/messages/:pid/:txn` is keyed by. May repeat across the log.
    pub transaction_id: String,
    /// The stamp of the append that wrote the record: every record of one
    /// append shares it.
    pub ts: Micros,
    /// `None` when the stored payload is empty (the route renders it
    /// `"payload":null`). Otherwise valid JSON, decrypted by the broker when
    /// at-rest encryption is on.
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
/// (`partitions/changed`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionBounds {
    pub name: Arc<str>,
    /// The partition's id — its uuid, as a string. Stable for the partition's
    /// life; a partition retention deleted (idle and empty for
    /// `PARTITION_CLEANUP_DAYS`) or a queue deleted and created again under the
    /// same name comes back with a NEW id and offsets starting at 0 again. It is
    /// what tells two incarnations of one name apart. `None` when the broker
    /// does not report it: the engine then cannot see an incarnation change.
    pub id: Option<String>,
    /// The last offset assigned, `-1` for a partition with no record. The high
    /// watermark is `last_offset + 1`.
    pub last_offset: i64,
    /// The retention watermark: the first offset still stored.
    pub log_start: i64,
    /// The stamp of the partition's last record, exact to the microsecond (its
    /// creation stamp if it never had one).
    pub last_write_at: Option<Micros>,
}

/// One entry of a `POST /api/v1/fetch` request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchRequestEntry {
    pub queue: String,
    pub partition: Arc<str>,
    pub offset: i64,
    /// Per-entry ceiling over the payload bytes read (each record counts at
    /// least one), clamped by the broker to `1..=8 MiB`; the first record of an
    /// entry is always delivered, whatever its size, and an entry stops at
    /// 10 000 records. `None` = the broker default (1 MiB).
    pub max_bytes: Option<i64>,
}

/// Per-entry error markers, spelled as the broker spells them.
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
    /// In offset order from the requested offset (possibly empty). Offsets are
    /// dense, but the broker steps silently over an offset it cannot find —
    /// retention trimmed the head between its partition read and its log read —
    /// so an answer can start above the requested offset or jump inside: those
    /// offsets are gone, and the engine records them as lost.
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
    /// `None` = every partition of the queue; `Some(t)` = the partitions whose
    /// `lastWriteAt >= t`. Both are listed in partition creation order.
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
    /// `UNKNOWN_TOPIC_OR_PARTITION` for a queue this tenant does not have,
    /// `BAD_CURSOR` for an `after` the broker did not issue.
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangedResponse {
    /// The greatest stamp the SERVING NODE has applied: no record with an older
    /// stamp can still become visible on that node (plan §5.2). Per node — a
    /// follower's trails the leader's — so it is only ever paired with reads
    /// the same node serves.
    pub safe_time: Micros,
    /// Parsed for wire compatibility and always `false` on the raft broker,
    /// whose `safeTime` is exact rather than a floor.
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

/// The codec an object is written with, as its intent and manifest record it:
/// the EFFECTIVE codec of the object's bytes, whichever knob chose it. A JSONL
/// object is `zstd`, `gzip` or `none` (`QUEEN_S3_COMPRESSION`); a Parquet
/// object is `zstd` or `snappy` (`QUEEN_S3_PARQUET_CODEC`, its pages' codec),
/// so `snappy` names a Parquet object and nothing else.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    Zstd,
    Gzip,
    None,
    /// Parquet pages compressed with Snappy. No JSONL object is ever written
    /// with it: `QUEEN_S3_COMPRESSION` does not take it, and a JSONL writer
    /// asked for it refuses to finish an object.
    Snappy,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParquetCodec {
    Zstd,
    Snappy,
}

/// `merged` (default): one object per window per queue, the partition a column
/// inside it. `per-partition`: one object per (window, partition),
/// Connect-shaped keys. The TENANT and the QUEUE are the `tenant=` and `queue=`
/// path keys in both, never a
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
// `s3:<sink>:<esc queue>:{intent,committed,lease}`. Small, far under the KV
// value ceiling; expiry "forever" for intent and committed.
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
///
/// It has a second reader: the broker's retention hold. A queue configured
/// with `retentionSinkHold=<sink>` keeps its records until `tEnd − 60 s` of
/// this document (server/src/rsm/maintenance.rs `sink_floor`), which reads
/// `tEnd` as an ISO-8601 UTC string. So `tEnd` is written in exactly the form
/// the broker renders timestamps ([`Micros::to_iso`]); a number there reads as
/// "never committed" and the hold falls back to its cap. 1.5.0 wrote the number,
/// and reading still accepts it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Committed {
    pub k: u64,
    #[serde(with = "iso_micros")]
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

/// An offset range that went away before the sink read it (plan §4.6):
/// retention passed the sink by, or the partition was deleted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LostRange {
    pub partition: String,
    /// First missing offset (inclusive).
    pub from: i64,
    /// Last missing offset (inclusive).
    pub to: i64,
}

impl LostRange {
    /// The records in the range; an inverted range counts none.
    pub fn count(&self) -> u64 {
        self.to.saturating_sub(self.from).saturating_add(1).max(0) as u64
    }
}

/// `_queen/tenant=<esc>/queue=<esc>/windows/<k>.json` — one per committed
/// window. The only place a wall-clock value is written (`committed_at`): the
/// data object itself carries none, so a retry is byte-identical (plan §4.2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    pub sink: String,
    /// The broker tenant the window belongs to — the `tenant=` key of every
    /// object it names.
    #[serde(default)]
    pub tenant: String,
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

/// `_queen/tenant=<esc>/queue=<esc>/checkpoint/<k>.json.zst` — the position cache as of
/// window `k` committed: for each partition the next offset not yet shipped.
/// A cache: a stale or missing entry costs re-reads, never correctness
/// (plan §4.5).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Checkpoint {
    pub k: u64,
    pub t_end: Micros,
    /// One [`Position`] per partition, sorted by name so the object is
    /// deterministic.
    pub positions: Vec<Position>,
}

/// One entry of a [`Checkpoint`]: the offset the next window reads one
/// partition from, and the incarnation that offset belongs to.
///
/// On the wire it is a JSON array — `["name", offset]` or
/// `["name", offset, "id"]` — so a million of them stay a compact object. The
/// two-element form is what 1.5.0 wrote; it still decodes, as an unknown
/// incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Position {
    pub partition: String,
    pub offset: i64,
    /// The partition id ([`PartitionBounds::id`]) the offset was read under.
    /// An offset means nothing in another incarnation of the same name, so a
    /// position is applied only when this matches the partition discovery
    /// names, or when either side is unknown (`None`).
    pub id: Option<String>,
}

impl Position {
    /// A position of an unknown incarnation.
    pub fn new(partition: impl Into<String>, offset: i64) -> Position {
        Position {
            partition: partition.into(),
            offset,
            id: None,
        }
    }

    /// A position read under the partition id `id`.
    pub fn with_id(partition: impl Into<String>, offset: i64, id: impl Into<String>) -> Position {
        Position {
            partition: partition.into(),
            offset,
            id: Some(id.into()),
        }
    }
}

impl Serialize for Position {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(Some(if self.id.is_some() { 3 } else { 2 }))?;
        seq.serialize_element(&self.partition)?;
        seq.serialize_element(&self.offset)?;
        if let Some(id) = &self.id {
            seq.serialize_element(id)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Position {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Position, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Position;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("[partition, offset] or [partition, offset, id]")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Position, A::Error> {
                let partition: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let offset: i64 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let id: Option<String> = seq.next_element::<Option<String>>()?.flatten();
                // Anything after the id belongs to a later writer: read past it
                // rather than refuse a position this one can still use.
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                Ok(Position {
                    partition,
                    offset,
                    id,
                })
            }
        }
        d.deserialize_seq(V)
    }
}

/// [`Committed::t_end`] on the wire: written as the broker's ISO-8601 rendering
/// ([`Micros::to_iso`]), read from that or from the integer microseconds 1.5.0
/// wrote.
mod iso_micros {
    use super::Micros;

    pub fn serialize<S: serde::Serializer>(t: &Micros, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&t.to_iso())
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Micros, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = Micros;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("an ISO-8601 UTC timestamp or integer microseconds")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Micros, E> {
                Micros::parse_iso(v).map_err(E::custom)
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Micros, E> {
                Ok(Micros(v))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Micros, E> {
                i64::try_from(v)
                    .map(Micros)
                    .map_err(|_| E::custom("microseconds out of range"))
            }
        }
        d.deserialize_any(V)
    }
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
    /// The service answered with a non-success status: the bucket, or a broker
    /// route the in-process adapter answered through
    /// ([`crate::queen::parse_kv_answer`]).
    Status {
        code: u16,
        body: String,
        /// `Retry-After`, milliseconds — S3 sets it on some 503s, and an
        /// adapter may pass the broker's own.
        retry_after_ms: Option<i64>,
    },
    /// A 2xx whose body the crate cannot read.
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

    /// The codec names an intent and a manifest carry, both ways. `snappy`
    /// (a Parquet object's page codec) is the one 2.0.0 added; the other three
    /// are the spellings documents already in a bucket hold.
    #[test]
    fn compression_spells_the_effective_codec() {
        for (c, name) in [
            (Compression::Zstd, "\"zstd\""),
            (Compression::Gzip, "\"gzip\""),
            (Compression::None, "\"none\""),
            (Compression::Snappy, "\"snappy\""),
        ] {
            assert_eq!(serde_json::to_string(&c).unwrap(), name);
            assert_eq!(serde_json::from_str::<Compression>(name).unwrap(), c);
        }
    }

    /// The commit pointer's `tEnd` is read by the broker's retention hold
    /// (server/src/rsm/maintenance.rs `sink_floor`) as an ISO-8601 UTC string
    /// — its parser takes `YYYY-MM-DDTHH:MM:SS`, a fraction it truncates to
    /// milliseconds, and `Z`. This pins the exact string, so a change of the
    /// document's shape fails here rather than silently turning the hold into
    /// its cap.
    #[test]
    fn the_committed_t_end_is_the_iso_string_the_retention_hold_parses() {
        let c = Committed {
            k: 42,
            t_end: Micros::parse_iso("2026-09-04T11:00:00.123456Z").unwrap(),
            manifest: "queen/_queen/tenant=t1/queue=orders/windows/0000000042.json".into(),
            records: 3,
            bytes: 99,
            committed_at_ms: 1_788_519_600_000,
        };
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["tEnd"], "2026-09-04T11:00:00.123456Z");
        assert_eq!(v["k"], 42);
        assert_eq!(serde_json::from_value::<Committed>(v).unwrap(), c);

        // 1.5.0 wrote integer microseconds; such a pointer still restores.
        let old = serde_json::json!({
            "k": 7, "tEnd": 1_788_516_300_000_000i64, "manifest": "m",
            "records": 1, "bytes": 1, "committedAtMs": 0
        });
        let back: Committed = serde_json::from_value(old).unwrap();
        assert_eq!(back.t_end, Micros(1_788_516_300_000_000));
        assert!(serde_json::from_value::<Committed>(serde_json::json!({
            "k": 7, "tEnd": "soon", "manifest": "m", "records": 1, "bytes": 1, "committedAtMs": 0
        }))
        .is_err());
    }

    #[test]
    fn a_position_is_a_two_or_three_element_array() {
        let with = Position::with_id("cust-1", 17, "0b1c4c6e-6d4f-4b7a-9f00-1d2e3f405162");
        let s = serde_json::to_string(&with).unwrap();
        assert_eq!(s, r#"["cust-1",17,"0b1c4c6e-6d4f-4b7a-9f00-1d2e3f405162"]"#);
        assert_eq!(serde_json::from_str::<Position>(&s).unwrap(), with);

        let without = Position::new("cust-1", 17);
        assert_eq!(serde_json::to_string(&without).unwrap(), r#"["cust-1",17]"#);
        // The 1.5.0 form decodes as an unknown incarnation, and so does a null.
        assert_eq!(
            serde_json::from_str::<Position>(r#"["cust-1",17]"#).unwrap(),
            without
        );
        assert_eq!(
            serde_json::from_str::<Position>(r#"["cust-1",17,null]"#).unwrap(),
            without
        );
        // A later writer's extra element is read past, not refused.
        assert_eq!(
            serde_json::from_str::<Position>(r#"["cust-1",17,"x",{"more":1}]"#).unwrap(),
            Position::with_id("cust-1", 17, "x")
        );
        assert!(serde_json::from_str::<Position>(r#"["cust-1"]"#).is_err());
        assert!(serde_json::from_str::<Position>(r#"{"partition":"x"}"#).is_err());
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
