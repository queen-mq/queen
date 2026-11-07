//! `POST /api/v1/feed`: every push to a tenant's queues, in the order the log
//! committed them, read by position from this node's own applied state.
//!
//! A position is the log entry (`seq`) that wrote a record. One number orders
//! every queue of the tenant, and the pushes of one entry (a transaction that
//! pushed to several queues) share it, so a reader that keeps one position
//! sees each step whole or not at all. The read walks the queue logs on disk
//! ([`QLog::feed_plan`], [`feed_scan`]) and never past the applied index: the
//! queue logs are also the raft log, and their tail can hold entries this
//! node has not applied yet, which a new leader may still cut.
//!
//! A shard (`shard: {count, index}`) reads the sealed files of a log through
//! their indexes: of the records of the partitions whose name hashes to its
//! shard (kept per file in a small cache, [`shard_slots`]), it reads the ones
//! it wants where the index says they are, and never touches the others. The
//! active file of each log, a file whose index is not checked yet, and a log
//! that holds a queue the shard leaves whole are scanned, as an unsharded
//! read scans everything.
//!
//! The leader of a cluster never serves it. A node that leads the tenant's
//! raft group answers 421 with the followers to ask instead, so the only work
//! a reader costs the leader is that answer. `QUEEN_FEED_ON_LEADER=true` lifts
//! it (a test cluster); a single node serves it, having no follower.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};

use super::json_out::{push_payload, push_str};
use super::{read_error, ApiOut, RaftFacade, ReqCtx, RsmError};
use crate::frames::{unpack_frames_ref, uuid_bytes_to_string};
use crate::obs::panic_policy::LockExt;
use crate::rsm::planner::timers::iso_us;
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::qlog::{
    decode_message, feed_pids, feed_scan, feed_slots, record, FeedFile, FeedIndex, FeedSlot, QLog,
    SlotReader,
};
use crate::rsm::replicator::Replicator;
use crate::rsm::store::heed_store::HeedStore;
use crate::rsm::store::rows::PartitionRow;
use crate::rsm::store::{Store, TypedReads};

/// Payload bytes one answer carries when the call names no `maxBytes`.
const DEFAULT_MAX_BYTES: usize = 1 << 20;
const MAX_MAX_BYTES: usize = 16 << 20;
/// Messages one answer carries when the call names no `maxRecords`.
const DEFAULT_MAX_RECORDS: usize = 1_000;
const MAX_MAX_RECORDS: usize = 10_000;
const MAX_WAIT_MS: u64 = 30_000;
/// Queue names or patterns one call may give.
const MAX_PATTERNS: usize = 256;
/// Queues one call may read: past it, the queues after the first this many
/// (in name order) are left for a narrower call.
const MAX_QUEUES: usize = 10_000;
const MAX_PARTITIONS: usize = 1_000;
const MAX_CONDITIONS: usize = 16;
const MAX_IN_VALUES: usize = 256;
const MAX_FIELD_DEPTH: usize = 16;
/// The least of a call's byte budget one queue log may fill before the read
/// stops there: a call over many logs still reads each a useful amount.
const MIN_LOG_BUDGET: usize = 64 << 10;
/// What one sharded read may scan at most, in raw record bytes and records:
/// a shard keeps about 1/count of what it scans, so it scans more (up to these)
/// to answer as full a page as an unsharded read.
const MAX_SCAN_BYTES: usize = 32 << 20;
const MAX_SCAN_RECORDS: usize = 100_000;
/// The index entries of its shard one sharded read looks at per answer in a
/// log's sealed files, about. An entry of a partition the read does not want
/// (another tenant's queue in a shared log, a partition `partitions` leaves
/// out) costs a lookup and no read; past this many, the record of the next
/// such entry is read to learn its position, and the read stops there.
const MAX_INDEX_SLOTS: usize = 100_000;
/// The least of [`MAX_INDEX_SLOTS`] one log gets, however many a read walks.
const MIN_LOG_SLOTS: usize = 4_096;
/// The bytes of index entries the shard-slot cache ([`slot_cache`]) holds.
const SLOT_CACHE_BYTES: usize = 64 << 20;

/// Whether `QUEEN_FEED_ON_LEADER` lets the leader serve the feed too.
fn feed_on_leader_allowed() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("QUEEN_FEED_ON_LEADER")
            .ok()
            .is_some_and(|v| matches!(v.trim(), "1" | "true" | "yes" | "on"))
    })
}

// ---------------------------------------------------------------- the request

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FeedBody {
    #[serde(default)]
    from: Option<Value>,
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    queues: Option<Vec<String>>,
    #[serde(default)]
    partitions: Option<Vec<String>>,
    #[serde(default)]
    kinds: Option<Vec<String>>,
    #[serde(default, rename = "where")]
    conditions: Option<Vec<CondBody>>,
    #[serde(default)]
    max_bytes: Option<u64>,
    #[serde(default)]
    max_records: Option<u64>,
    #[serde(default)]
    max_wait_ms: Option<u64>,
    #[serde(default)]
    shard: Option<ShardBody>,
}

/// `shard: { count, index, queues? }`: only the partitions whose name hashes
/// to `index` of `count` (FNV-1a, 32 bit, over the name's UTF-8 bytes). With
/// `queues` (names or patterns), only those queues are split; the others pass
/// whole, as a table every shard needs.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ShardBody {
    count: u64,
    index: u64,
    #[serde(default)]
    queues: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CondBody {
    field: String,
    op: String,
    #[serde(default)]
    value: Value,
}

/// Where a read starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Start {
    /// This position, included.
    At(u64),
    /// Past everything this node has applied: only what comes next.
    Now,
    /// The oldest record the queue logs still hold.
    Earliest,
}

#[derive(Clone, Debug)]
enum Field {
    Queue,
    Partition,
    TransactionId,
    Offset,
    Position,
    /// The payload, or a path into it (`data.customer.country`).
    Data(Vec<String>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    Exists,
    Prefix,
    NotPrefix,
}

#[derive(Clone, Debug)]
struct Cond {
    field: Field,
    op: Op,
    value: Value,
}

/// The slice of the partitions one shard reads.
#[derive(Clone, Debug)]
struct Shard {
    count: u32,
    index: u32,
    /// The queues split; `None`: every queue the read selects.
    patterns: Option<Vec<String>>,
}

/// A checked request.
#[derive(Clone, Debug)]
struct FeedAsk {
    start: Start,
    since_us: Option<i64>,
    patterns: Option<Vec<String>>,
    partitions: Option<HashSet<String>>,
    conds: Vec<Cond>,
    max_bytes: usize,
    max_records: usize,
    shard: Option<Shard>,
}

/// The most shards one query may split into.
const MAX_SHARDS: u64 = 4096;

/// FNV-1a, 32 bit: the hash a shard is chosen by. Clients compute the same one
/// (over the UTF-8 bytes), so a client can tell which shard holds a partition.
fn fnv1a32(bytes: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for b in bytes {
        h ^= u32::from(*b);
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

/// The shard of `count` a partition belongs to.
fn shard_of(partition: &str, count: u32) -> u32 {
    fnv1a32(partition.as_bytes()) % count.max(1)
}

fn parse_start(v: Option<Value>) -> Result<Start, String> {
    match v {
        None | Some(Value::Null) => Ok(Start::Now),
        Some(Value::Number(n)) => n
            .as_u64()
            .map(Start::At)
            .ok_or_else(|| format!("from must be a position (a whole number from 1), got {n}")),
        Some(Value::String(s)) => match s.trim() {
            "now" => Ok(Start::Now),
            "earliest" => Ok(Start::Earliest),
            t => t.parse::<u64>().map(Start::At).map_err(|_| {
                format!("from must be a position, \"now\" or \"earliest\", got {s:?}")
            }),
        },
        Some(other) => Err(format!(
            "from must be a position, \"now\" or \"earliest\", got {other}"
        )),
    }
}

fn parse_field(raw: &str) -> Result<Field, String> {
    match raw {
        "queue" => return Ok(Field::Queue),
        "partition" => return Ok(Field::Partition),
        "transactionId" => return Ok(Field::TransactionId),
        "offset" => return Ok(Field::Offset),
        "position" => return Ok(Field::Position),
        "data" => return Ok(Field::Data(Vec::new())),
        _ => {}
    }
    let Some(path) = raw.strip_prefix("data.") else {
        return Err(format!(
            "where: unknown field {raw:?} (queue, partition, transactionId, offset, position, data or data.<path>)"
        ));
    };
    let parts: Vec<String> = path.split('.').map(str::to_string).collect();
    if parts.iter().any(String::is_empty) || parts.len() > MAX_FIELD_DEPTH {
        return Err(format!("where: bad field path {raw:?}"));
    }
    Ok(Field::Data(parts))
}

fn parse_op(raw: &str) -> Result<Op, String> {
    Ok(match raw {
        "=" | "==" => Op::Eq,
        "!=" => Op::Ne,
        "<" => Op::Lt,
        "<=" => Op::Le,
        ">" => Op::Gt,
        ">=" => Op::Ge,
        "in" => Op::In,
        "exists" => Op::Exists,
        "prefix" => Op::Prefix,
        "!prefix" => Op::NotPrefix,
        _ => {
            return Err(format!(
                "where: unknown op {raw:?} (=, !=, <, <=, >, >=, in, exists, prefix, !prefix)"
            ))
        }
    })
}

impl FeedAsk {
    /// The body checked, and the call's long-poll wait in milliseconds.
    fn from_body(b: FeedBody) -> Result<(FeedAsk, u64), String> {
        let start = parse_start(b.from)?;
        let since_us = match b.since {
            None => None,
            Some(s) => Some(
                crate::rsm::dashboard::model::parse_ts_us(&s)
                    .ok_or_else(|| format!("since is not a timestamp: {s:?}"))?,
            ),
        };
        if let Some(kinds) = &b.kinds {
            if let Some(k) = kinds.iter().find(|k| k.as_str() != "push") {
                return Err(format!(
                    "kinds: {k:?} is not served yet; this broker's feed carries \"push\" only"
                ));
            }
        }
        let patterns = match b.queues {
            None => None,
            Some(q) if q.is_empty() => None,
            Some(q) => {
                if q.len() > MAX_PATTERNS {
                    return Err(format!("queues: at most {MAX_PATTERNS} names or patterns"));
                }
                if q.iter().any(String::is_empty) {
                    return Err("queues: a name or pattern must not be empty".into());
                }
                Some(q)
            }
        };
        let partitions = match b.partitions {
            None => None,
            Some(p) if p.is_empty() => None,
            Some(p) => {
                if p.len() > MAX_PARTITIONS {
                    return Err(format!("partitions: at most {MAX_PARTITIONS} names"));
                }
                Some(p.into_iter().collect())
            }
        };
        let raw = b.conditions.unwrap_or_default();
        if raw.len() > MAX_CONDITIONS {
            return Err(format!("where: at most {MAX_CONDITIONS} conditions"));
        }
        let mut conds = Vec::with_capacity(raw.len());
        for c in raw {
            let field = parse_field(&c.field)?;
            let op = parse_op(&c.op)?;
            match op {
                Op::In => match &c.value {
                    Value::Array(a) if a.len() <= MAX_IN_VALUES => {}
                    Value::Array(_) => {
                        return Err(format!("where: in takes at most {MAX_IN_VALUES} values"))
                    }
                    _ => return Err(format!("where: in on {:?} needs an array", c.field)),
                },
                Op::Prefix | Op::NotPrefix if !c.value.is_string() => {
                    return Err(format!("where: {} on {:?} needs a string", c.op, c.field))
                }
                Op::Lt | Op::Le | Op::Gt | Op::Ge
                    if !(c.value.is_number() || c.value.is_string()) =>
                {
                    return Err(format!(
                        "where: {} on {:?} needs a number or a string",
                        c.op, c.field
                    ))
                }
                _ => {}
            }
            conds.push(Cond {
                field,
                op,
                value: c.value,
            });
        }
        let max_bytes = b
            .max_bytes
            .map_or(DEFAULT_MAX_BYTES, |n| (n as usize).clamp(1, MAX_MAX_BYTES));
        let max_records = b.max_records.map_or(DEFAULT_MAX_RECORDS, |n| {
            (n as usize).clamp(1, MAX_MAX_RECORDS)
        });
        let wait = b.max_wait_ms.unwrap_or(0).min(MAX_WAIT_MS);
        let shard = match b.shard {
            None => None,
            Some(s) => {
                if s.count == 0 || s.count > MAX_SHARDS {
                    return Err(format!(
                        "shard: count is a whole number from 1 to {MAX_SHARDS}, got {}",
                        s.count
                    ));
                }
                if s.index >= s.count {
                    return Err(format!(
                        "shard: index is from 0 to count - 1 ({}), got {}",
                        s.count - 1,
                        s.index
                    ));
                }
                let patterns = match s.queues {
                    None => None,
                    Some(q) if q.is_empty() => None,
                    Some(q) => {
                        if q.len() > MAX_PATTERNS {
                            return Err(format!(
                                "shard: queues takes at most {MAX_PATTERNS} names or patterns"
                            ));
                        }
                        if q.iter().any(String::is_empty) {
                            return Err("shard: a queue name or pattern must not be empty".into());
                        }
                        Some(q)
                    }
                };
                Some(Shard {
                    count: s.count as u32,
                    index: s.index as u32,
                    patterns,
                })
            }
        };
        Ok((
            FeedAsk {
                start,
                since_us,
                patterns,
                partitions,
                conds,
                max_bytes,
                max_records,
                shard,
            },
            wait,
        ))
    }

    /// Whether a message of `queue`/`partition` is in this read's shard: always,
    /// without a shard or for a queue the shard does not split.
    fn in_shard(&self, queue: &str, partition: &str) -> bool {
        match &self.shard {
            Some(s) if self.splits(queue) => shard_of(partition, s.count) == s.index,
            _ => true,
        }
    }

    /// Whether this read's shard splits `queue` (reads only its share of the
    /// queue's partitions): `false` without a shard.
    fn splits(&self, queue: &str) -> bool {
        match &self.shard {
            None => false,
            Some(s) => match &s.patterns {
                None => true,
                Some(p) => p.iter().any(|pat| glob_match(pat, queue)),
            },
        }
    }

    /// Whether this read wants the messages of a partition with row `row`:
    /// the tenant's, of a selected queue (`wanted`), named by `partitions` if
    /// it names any, and in the shard.
    fn wants(&self, tenant: &str, wanted: &HashSet<&str>, row: &PartitionRow) -> bool {
        row.tenant == tenant
            && wanted.contains(row.queue.as_str())
            && self
                .partitions
                .as_ref()
                .is_none_or(|p| p.contains(&row.partition))
            && self.in_shard(&row.queue, &row.partition)
    }

    fn selects_queue(&self, name: &str) -> bool {
        match &self.patterns {
            None => true,
            Some(p) => p.iter().any(|pat| glob_match(pat, name)),
        }
    }

    fn wants_payload(&self) -> bool {
        self.conds.iter().any(|c| matches!(c.field, Field::Data(_)))
    }

    fn matches(&self, e: &Entry, doc: Option<&Value>) -> bool {
        self.conds.iter().all(|c| cond_holds(c, e, doc))
    }
}

/// `*` any run of characters, `?` one, everything else itself.
fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut i, mut j) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while j < t.len() {
        if i < p.len() && (p[i] == '?' || p[i] == t[j]) {
            i += 1;
            j += 1;
        } else if i < p.len() && p[i] == '*' {
            star = Some((i, j));
            i += 1;
        } else if let Some((si, sj)) = star {
            i = si + 1;
            j = sj + 1;
            star = Some((si, sj + 1));
        } else {
            return false;
        }
    }
    while i < p.len() && p[i] == '*' {
        i += 1;
    }
    i == p.len()
}

fn json_eq(a: &Value, b: &Value) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) if a.is_number() && b.is_number() => x == y,
        _ => a == b,
    }
}

fn json_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Number(_), Value::Number(_)) => a.as_f64()?.partial_cmp(&b.as_f64()?),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Whether condition `c` holds for entry `e`. A field the entry does not have
/// (a path its payload lacks, a payload that is not JSON) holds for no
/// operator, `!=` included, as SQL's comparisons with NULL.
fn cond_holds(c: &Cond, e: &Entry, doc: Option<&Value>) -> bool {
    let owned;
    let v: Option<&Value> = match &c.field {
        Field::Queue => {
            owned = Value::String(e.queue.clone());
            Some(&owned)
        }
        Field::Partition => {
            owned = Value::String(e.partition.clone());
            Some(&owned)
        }
        Field::TransactionId => {
            owned = Value::String(e.txn.clone());
            Some(&owned)
        }
        Field::Offset => {
            owned = Value::from(e.offset);
            Some(&owned)
        }
        Field::Position => {
            owned = Value::from(e.position);
            Some(&owned)
        }
        Field::Data(path) => doc.and_then(|d| {
            path.iter().try_fold(d, |cur, key| match cur {
                Value::Object(m) => m.get(key),
                Value::Array(a) => key.parse::<usize>().ok().and_then(|i| a.get(i)),
                _ => None,
            })
        }),
    };
    let Some(v) = v else {
        return false;
    };
    match c.op {
        Op::Exists => true,
        Op::Eq => json_eq(v, &c.value),
        Op::Ne => !json_eq(v, &c.value),
        Op::In => c
            .value
            .as_array()
            .is_some_and(|a| a.iter().any(|x| json_eq(v, x))),
        Op::Prefix => match (v.as_str(), c.value.as_str()) {
            (Some(s), Some(p)) => s.starts_with(p),
            _ => false,
        },
        // A string that does not start with it; like `!=`, a value that is
        // not a string (or no value) holds for neither.
        Op::NotPrefix => match (v.as_str(), c.value.as_str()) {
            (Some(s), Some(p)) => !s.starts_with(p),
            _ => false,
        },
        Op::Lt => json_cmp(v, &c.value).is_some_and(|o| o.is_lt()),
        Op::Le => json_cmp(v, &c.value).is_some_and(|o| o.is_le()),
        Op::Gt => json_cmp(v, &c.value).is_some_and(|o| o.is_gt()),
        Op::Ge => json_cmp(v, &c.value).is_some_and(|o| o.is_ge()),
    }
}

// ---------------------------------------------------------------- the read

/// One message of the answer.
struct Entry {
    position: u64,
    ts_us: i64,
    queue: String,
    partition: String,
    partition_id: String,
    id: String,
    offset: u64,
    txn: String,
    payload: Vec<u8>,
}

/// A message record a scan found, kept as stored until the partition rows
/// say whether it is wanted.
struct Cand {
    seq: u64,
    pid: u64,
    created_at_us: i64,
    raw: Vec<u8>,
}

/// One queue log's part of a read: what its walk has taken, how much it may
/// take, and where it stops. A file is either scanned (every message record,
/// the read's or not) or read through its index (the read's own records
/// only), and each way has its budget: a sharded scan keeps about one record
/// in `count`, so it may take `count` times as much as an index read.
#[derive(Default)]
struct LogWalk {
    /// Raw bytes and records the scans took, and their budget.
    scanned: usize,
    scanned_records: usize,
    scan_bytes: usize,
    scan_records: usize,
    /// Raw bytes and records the index reads took, and their budget.
    kept: usize,
    kept_records: usize,
    keep_bytes: usize,
    keep_records: usize,
    /// Index entries looked at; past `examine_until`, the walk stops at the
    /// entry of the next record it reads. `examine_step` moves the mark on
    /// when that record is below the read's first position.
    examined: usize,
    examine_until: usize,
    examine_step: usize,
    /// The entry the budget ran out in: read to its end, then stop.
    stop_seq: Option<u64>,
    /// A record past `stop_seq` was met: this log is done.
    past: bool,
}

impl LogWalk {
    /// A record the walk met, in seq order (`indexed`: read through the
    /// index): taken, unless it is past the entry the walk stops at. Answers
    /// whether to go on.
    fn take(
        &mut self,
        cands: &mut Vec<Cand>,
        h: &record::Header,
        raw: &[u8],
        indexed: bool,
    ) -> bool {
        if self.stop_seq.is_some_and(|s| h.seq > s) {
            self.past = true;
            return false;
        }
        cands.push(Cand {
            seq: h.seq,
            pid: h.pid,
            created_at_us: h.created_at_us,
            raw: raw.to_vec(),
        });
        let full = if indexed {
            self.kept += raw.len();
            self.kept_records += 1;
            self.kept >= self.keep_bytes || self.kept_records >= self.keep_records
        } else {
            self.scanned += raw.len();
            self.scanned_records += 1;
            self.scanned >= self.scan_bytes || self.scanned_records >= self.scan_records
        };
        if self.stop_seq.is_none() && full {
            self.stop_seq = Some(h.seq);
        }
        true
    }
}

/// Files read through their index, in this process: a test's proof that a
/// sharded read took that way.
#[cfg(test)]
static INDEXED_FILES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Tests: the index entries one log may look at per answer instead of
/// [`MAX_INDEX_SLOTS`] (0: that), so a small log reaches the stop a large one
/// does. Process-wide, and safe so: an answer never depends on it, only where
/// a read's pages end.
#[cfg(test)]
static TEST_INDEX_SLOTS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// One sealed file's records of the partitions whose name hashes to one shard
/// (of every tenant and queue the file holds), in file order, and those
/// partitions, each once.
struct ShardSlots {
    slots: Vec<FeedSlot>,
    pids: Vec<u64>,
}

impl ShardSlots {
    /// What it costs the cache.
    fn bytes(&self) -> usize {
        std::mem::size_of::<ShardSlots>()
            + self.slots.len() * std::mem::size_of::<FeedSlot>()
            + self.pids.len() * std::mem::size_of::<u64>()
    }
}

/// The shard slots sharded reads made lately, by `(index uid, shard count,
/// shard)`. A rewritten or rebuilt file comes with a new index and so a new
/// uid: the slots of its old version are never served for it. At most
/// [`SLOT_CACHE_BYTES`]; the entry used longest ago goes first.
#[derive(Default)]
struct SlotCache {
    map: HashMap<(u64, u32, u32), (Arc<ShardSlots>, u64)>,
    bytes: usize,
    clock: u64,
}

impl SlotCache {
    fn get(&mut self, key: (u64, u32, u32)) -> Option<Arc<ShardSlots>> {
        self.clock += 1;
        let clock = self.clock;
        let (slots, used) = self.map.get_mut(&key)?;
        *used = clock;
        Some(slots.clone())
    }

    fn put(&mut self, key: (u64, u32, u32), slots: Arc<ShardSlots>) {
        let add = slots.bytes();
        if add > SLOT_CACHE_BYTES {
            return;
        }
        self.clock += 1;
        if let Some((old, _)) = self.map.insert(key, (slots, self.clock)) {
            self.bytes -= old.bytes();
        }
        self.bytes += add;
        while self.bytes > SLOT_CACHE_BYTES {
            let Some(oldest) = self
                .map
                .iter()
                .min_by_key(|(_, (_, used))| *used)
                .map(|(k, _)| *k)
            else {
                break;
            };
            if let Some((gone, _)) = self.map.remove(&oldest) {
                self.bytes -= gone.bytes();
            }
        }
    }
}

fn slot_cache() -> &'static Mutex<SlotCache> {
    static CACHE: OnceLock<Mutex<SlotCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(SlotCache::default()))
}

/// Read into `rows` the partition rows of `pids` this read has not read yet,
/// in one store read (`None`: the partition is gone).
fn load_rows(
    store: &HeedStore,
    pids: impl Iterator<Item = u64>,
    rows: &mut HashMap<u64, Option<PartitionRow>>,
) -> Result<(), RsmError> {
    let missing: BTreeSet<u64> = pids.filter(|p| !rows.contains_key(p)).collect();
    if missing.is_empty() {
        return Ok(());
    }
    let found = store
        .read(|r| {
            let mut out = Vec::with_capacity(missing.len());
            for pid in &missing {
                out.push((*pid, r.partition(*pid)?));
            }
            Ok(out)
        })
        .map_err(read_error)?;
    rows.extend(found);
    Ok(())
}

/// The records of `shard`'s partitions in the sealed file `ix` indexes:
/// cached, or made from the index, with the row of every partition the file
/// holds read into `rows` (the hash of a partition's name picks its shard).
/// Every record of the file is applied ([`QLog::feed_plan`]), so a partition
/// with no row was deleted, and is in no shard for good: ids are never reused.
fn shard_slots(
    store: &HeedStore,
    ix: &FeedIndex,
    shard: &Shard,
    rows: &mut HashMap<u64, Option<PartitionRow>>,
) -> Result<Arc<ShardSlots>, RsmError> {
    let key = (ix.view.uid(), shard.count, shard.index);
    if let Some(s) = slot_cache().lock_unpoisoned().get(key) {
        return Ok(s);
    }
    let pids = feed_pids(&ix.view);
    load_rows(store, pids.iter().copied(), rows)?;
    let in_shard = |pid: u64| {
        rows.get(&pid)
            .and_then(Option::as_ref)
            .is_some_and(|row| shard_of(&row.partition, shard.count) == shard.index)
    };
    let made = Arc::new(ShardSlots {
        slots: feed_slots(&ix.view, &mut |pid| in_shard(pid)),
        pids: pids.into_iter().filter(|p| in_shard(*p)).collect(),
    });
    slot_cache().lock_unpoisoned().put(key, made.clone());
    Ok(made)
}

/// Whether the read wants each partition of `pids` ([`FeedAsk::wants`]),
/// decided once per partition per read.
fn fill_members(
    store: &HeedStore,
    (tenant, ask, wanted): (&str, &FeedAsk, &HashSet<&str>),
    pids: &[u64],
    rows: &mut HashMap<u64, Option<PartitionRow>>,
    members: &mut HashMap<u64, bool>,
) -> Result<(), RsmError> {
    load_rows(
        store,
        pids.iter().copied().filter(|p| !members.contains_key(p)),
        rows,
    )?;
    for pid in pids {
        members.entry(*pid).or_insert_with(|| {
            rows.get(pid)
                .and_then(Option::as_ref)
                .is_some_and(|row| ask.wants(tenant, wanted, row))
        });
    }
    Ok(())
}

/// Walk sealed file `file` through its index `ix`. Of `slots` (the file's
/// records of the shard's partitions, in file order), the records of the
/// partitions the read wants are read where the index says they are, and
/// each is checked as [`feed_scan`] checks a record (seq bounds, kind,
/// `since`) before the walk takes it; the others are passed over unread. A
/// record that is not what its index entry says (damage) ends the file for
/// this read, as the first bad record ends a scan.
#[allow(clippy::too_many_arguments)]
fn walk_indexed(
    file: &FeedFile,
    ix: &FeedIndex,
    slots: &[FeedSlot],
    members: &HashMap<u64, bool>,
    (from, upto, since_us): (u64, u64, Option<i64>),
    walk: &mut LogWalk,
    cands: &mut Vec<Cand>,
    stamps: &mut Vec<(u64, i64)>,
) -> std::io::Result<()> {
    let start = ix.start(file)?;
    // Stopping already: a file that starts past the stop entry is past it, as
    // the first record a scan of it would read.
    if let Some(stop) = walk.stop_seq {
        match ix.record_at(start)? {
            None => return Ok(()),
            Some((h, _)) if h.seq > stop => {
                walk.past = true;
                return Ok(());
            }
            Some(_) => {}
        }
    }
    let mut reader = SlotReader::new(ix);
    for i in slots.partition_point(|s| s.offset < start)..slots.len() {
        let slot = slots[i];
        walk.examined += 1;
        let member = members.get(&slot.pid).copied().unwrap_or(false);
        // Long past the read's own records: this one is read only to learn
        // the entry it is in, which the walk then stops at.
        let probe = !member && walk.stop_seq.is_none() && walk.examined >= walk.examine_until;
        if !(member || probe) || (member && since_us.is_some_and(|s| slot.created_at_us < s)) {
            continue;
        }
        let Some((h, raw)) = reader.read(slots, i)? else {
            return Ok(());
        };
        if h.seq > upto {
            return Ok(());
        }
        if probe {
            if h.seq >= from {
                walk.stop_seq = Some(h.seq);
                stamps.push((h.seq, h.created_at_us));
            } else {
                walk.examine_until = walk.examined + walk.examine_step;
            }
            continue;
        }
        let kind = h.kind();
        let message = kind == 0 || kind == record::TXN_CROSS_QUEUE;
        if !message || h.seq < from || since_us.is_some_and(|s| h.created_at_us < s) {
            continue;
        }
        if !walk.take(cands, &h, raw, true) {
            return Ok(());
        }
    }
    Ok(())
}

/// One answer: its JSON, and what the long poll decides on.
struct FeedPage {
    json: String,
    empty: bool,
    caught_up: bool,
    next: u64,
    /// The queues the read selected, for the long poll's watch.
    selected: Vec<String>,
}

fn render(entries: &[Entry], next: u64, applied: u64, caught_up: bool, resolved_us: i64) -> String {
    let mut out = String::with_capacity(
        96 + entries
            .iter()
            .map(|e| e.payload.len() + e.queue.len() + e.partition.len() + e.txn.len() + 220)
            .sum::<usize>(),
    );
    out.push_str("{\"applied\":");
    out.push_str(&applied.to_string());
    out.push_str(",\"caughtUp\":");
    out.push_str(if caught_up { "true" } else { "false" });
    out.push_str(",\"entries\":[");
    for (i, e) in entries.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"id\":");
        push_str(&mut out, &e.id);
        out.push_str(",\"kind\":\"push\",\"offset\":");
        out.push_str(&e.offset.to_string());
        out.push_str(",\"partition\":");
        push_str(&mut out, &e.partition);
        out.push_str(",\"partitionId\":");
        push_str(&mut out, &e.partition_id);
        out.push_str(",\"payload\":");
        push_payload(&mut out, &e.payload);
        out.push_str(",\"position\":");
        out.push_str(&e.position.to_string());
        out.push_str(",\"queue\":");
        push_str(&mut out, &e.queue);
        out.push_str(",\"transactionId\":");
        push_str(&mut out, &e.txn);
        out.push_str(",\"ts\":");
        push_str(&mut out, &iso_us(e.ts_us));
        out.push('}');
    }
    out.push_str("],\"next\":");
    out.push_str(&next.to_string());
    out.push_str(",\"resolvedTs\":");
    push_str(&mut out, &iso_us(resolved_us));
    out.push('}');
    out
}

fn io_error(e: std::io::Error) -> RsmError {
    RsmError::Internal(format!("feed read: {e}"))
}

/// One read of the feed, on a blocking thread: what this node has applied,
/// the queue logs of the selected queues walked up to it, merged by
/// position, filtered and cut at an entry boundary.
fn feed_read(
    store: &HeedStore,
    qlog: &QLogReader,
    encryption: &crate::encryption::Encryption,
    tenant: &str,
    ask: &FeedAsk,
) -> Result<FeedPage, RsmError> {
    // 1. The clock, then the applied index, then the queue rows. Every record
    // of an entry at or below the applied index is in the queue logs and
    // readable (apply writes its meta rows after its payload bytes, and the
    // applied index last). The clock is read BEFORE the index: meta is RAM,
    // read live (read-uncommitted), and apply writes an entry's `now_us`
    // before its index, so a clock read after the index can be the `now_us`
    // of an entry above it, whose records are stamped at that clock or above
    // it and are not in this answer (Jepsen W13, 2026-11-07: resolvedTs equal
    // to the stamp at `next`). Read first, it is the `now_us` of an entry at
    // most one above the index read next, and every record of every entry
    // above that index is stamped at that `now_us` or above it: one
    // microsecond below it is safe. It never goes back on one node, and lags
    // the newest stamp by at most one entry. `max_created_at_us` is left out:
    // it can be a stamp of that entry above the index.
    let (applied, safe_us, selected) = store
        .read(|r| {
            let now = r.last_now_us()?;
            let applied = r.applied_index()?;
            let safe = now.saturating_sub(1).max(0);
            let mut names: Vec<String> = Vec::new();
            r.scan_queues(tenant, usize::MAX, &mut |name, _cfg| {
                if ask.selects_queue(name) {
                    names.push(name.to_string());
                }
                names.len() < MAX_QUEUES
            })?;
            Ok((applied, safe, names))
        })
        .map_err(read_error)?;
    let from = match ask.start {
        Start::At(p) => p.max(1),
        Start::Now => applied + 1,
        Start::Earliest => 1,
    };
    if from > applied || selected.is_empty() {
        let next = from.max(applied + 1);
        return Ok(FeedPage {
            json: render(&[], next, applied, true, safe_us),
            empty: true,
            caught_up: true,
            next,
            selected,
        });
    }

    let wanted: HashSet<&str> = selected.iter().map(String::as_str).collect();

    // 2. The logs holding the selected queues (a shared log once), each
    // planned under its lock and walked after it, with retention paused so no
    // planned file is unlinked mid-walk. A log whose every selected queue the
    // read's shard splits has its sealed files read through their indexes:
    // only the shard's own records are read. Any other log, and the active
    // file of every log, is scanned.
    let mut logs: BTreeMap<u64, (Arc<RwLock<QLog>>, bool)> = BTreeMap::new();
    for q in &selected {
        let split = ask.splits(q);
        for (id, log) in qlog.queue_logs(QLogReader::queue_id_of(tenant, q)) {
            logs.entry(id).or_insert((log, true)).1 &= split;
        }
    }
    let _pause = qlog.pause_reclaim();
    let n_logs = logs.len().max(1);
    // A sharded scan keeps about one record in `count`, so it may take that
    // many times as much, up to the scan caps.
    let scale = ask.shard.as_ref().map_or(1, |s| s.count as usize).max(1);
    let per_log = ((ask.max_bytes / n_logs).max(MIN_LOG_BUDGET))
        .saturating_mul(scale)
        .min((MAX_SCAN_BYTES / n_logs).max(MIN_LOG_BUDGET));
    let scan_records = ask
        .max_records
        .saturating_mul(scale)
        .min(MAX_SCAN_RECORDS.max(ask.max_records));
    // An index read takes the read's own records only: an unsharded budget.
    let keep_bytes = (ask.max_bytes / n_logs).max(MIN_LOG_BUDGET);
    let examine_step = (MAX_INDEX_SLOTS / n_logs).max(MIN_LOG_SLOTS);
    #[cfg(test)]
    let examine_step = match TEST_INDEX_SLOTS.load(std::sync::atomic::Ordering::Relaxed) {
        0 => examine_step,
        n => n,
    };
    let mut cands: Vec<Cand> = Vec::new();
    // (seq, stamp) of the records read only to learn where a walk stops.
    let mut stamps: Vec<(u64, i64)> = Vec::new();
    // The partition rows this read has read (`None`: the partition is gone),
    // and whether it wants each partition an index walk met.
    let mut rows: HashMap<u64, Option<PartitionRow>> = HashMap::new();
    let mut members: HashMap<u64, bool> = HashMap::new();
    // Every selected record at or below `complete` has been read.
    let mut complete = applied;
    for (log, split) in logs.values() {
        let plan = {
            let g = log.read().expect("qlog poisoned");
            g.feed_plan(from, applied, ask.since_us, *split)
        };
        let mut walk = LogWalk {
            scan_bytes: per_log,
            scan_records,
            keep_bytes,
            keep_records: ask.max_records,
            examine_until: examine_step,
            examine_step,
            ..LogWalk::default()
        };
        for file in &plan {
            // The file's handle, taken with its index under the log's lock
            // when the walk gets to it.
            let ix = match (&file.view, &ask.shard) {
                (Some(_), Some(_)) => log.read().expect("qlog poisoned").feed_index(file),
                _ => None,
            };
            match (&ix, &ask.shard) {
                (Some(ix), Some(shard)) => {
                    let slots = shard_slots(store, ix, shard, &mut rows)?;
                    fill_members(
                        store,
                        (tenant, ask, &wanted),
                        &slots.pids,
                        &mut rows,
                        &mut members,
                    )?;
                    walk_indexed(
                        file,
                        ix,
                        &slots.slots,
                        &members,
                        (from, applied, ask.since_us),
                        &mut walk,
                        &mut cands,
                        &mut stamps,
                    )
                    .map_err(io_error)?;
                    #[cfg(test)]
                    INDEXED_FILES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                _ => feed_scan(file, from, applied, ask.since_us, &mut |h, raw| {
                    Ok(walk.take(&mut cands, h, raw, false))
                })
                .map_err(io_error)?,
            }
            if walk.past {
                break;
            }
        }
        if let Some(s) = walk.stop_seq {
            complete = complete.min(s);
        }
    }
    cands.retain(|c| c.seq <= complete);
    // (seq, stamp) of every record read, for the clock of a partial answer.
    stamps.extend(cands.iter().map(|c| (c.seq, c.created_at_us)));

    // 3. The partitions of the records read: the rows not read yet, in one
    // store read.
    load_rows(store, cands.iter().map(|c| c.pid), &mut rows)?;

    // 4. The messages, filtered, in log order.
    let mut entries: Vec<Entry> = Vec::new();
    for c in &cands {
        // A partition deleted since, another tenant's queue in a shared log,
        // a queue or partition the call did not select, another shard's
        // partition: left before its payload is decoded.
        let Some(Some(row)) = rows.get(&c.pid) else {
            continue;
        };
        if !ask.wants(tenant, &wanted, row) {
            continue;
        }
        let rec = decode_message(&c.raw).map_err(io_error)?;
        let Some(frames) = unpack_frames_ref(&rec.payload) else {
            continue;
        };
        let partition_id = uuid_bytes_to_string(&row.uuid);
        for (i, f) in frames.into_iter().enumerate() {
            let offset = rec.base_offset + i as u64;
            // Below the retention watermark: gone, whatever the file still holds.
            if offset < row.log_start {
                continue;
            }
            let payload = if f.encrypted {
                encryption
                    .decrypt_payload_bytes(f.payload)
                    .unwrap_or_else(|| f.payload.to_vec())
            } else {
                f.payload.to_vec()
            };
            let entry = Entry {
                position: c.seq,
                ts_us: rec.created_at_us,
                queue: row.queue.clone(),
                partition: row.partition.clone(),
                partition_id: partition_id.clone(),
                id: uuid_bytes_to_string(&f.message_id),
                offset,
                txn: f.txn.to_string(),
                payload,
            };
            let doc: Option<Value> = if ask.wants_payload() {
                serde_json::from_slice(&entry.payload).ok()
            } else {
                None
            };
            if ask.matches(&entry, doc.as_ref()) {
                entries.push(entry);
            }
        }
    }
    entries.sort_by(|a, b| {
        (a.position, &a.queue, &a.partition, a.offset).cmp(&(
            b.position,
            &b.queue,
            &b.partition,
            b.offset,
        ))
    });

    // 5. The call's caps, cut where a new position starts: a position is in
    // an answer whole or not at all, and the first one whatever it weighs.
    let mut bytes = 0usize;
    for i in 0..entries.len() {
        let weight = entries[i].payload.len().max(1);
        let new_position = i > 0 && entries[i].position != entries[i - 1].position;
        if new_position && (i >= ask.max_records || bytes + weight > ask.max_bytes) {
            complete = entries[i - 1].position;
            entries.truncate(i);
            break;
        }
        bytes += weight;
    }

    let caught_up = complete >= applied;
    let resolved_us = if caught_up {
        safe_us
    } else {
        // Every later entry is stamped strictly above every stamp before it,
        // so every record stamped at or below the newest one read is in. An
        // index read knows the stamps of the records it read only, so this
        // may be below the newest stamp the log holds there: never above. It
        // holds the stamp of the entry the read stopped at, which is above
        // every stamp an earlier answer gave. It is also held to `safe_us`,
        // the claim a caught-up answer makes next: a no-op entry (a new
        // leader's first) moves the applied index and not the clock, so after
        // the newest entry's own stamp a caught-up answer could only claim one
        // microsecond below it, and resolvedTs went back (Jepsen W13,
        // 2026-11-07, a kill run).
        stamps
            .iter()
            .filter(|(seq, _)| *seq <= complete)
            .map(|(_, ts)| *ts)
            .max()
            .map_or(safe_us, |ts| ts.min(safe_us))
    };
    let next = complete + 1;
    Ok(FeedPage {
        json: render(&entries, next, applied, caught_up, resolved_us),
        empty: entries.is_empty(),
        caught_up,
        next,
        selected,
    })
}

impl RaftFacade {
    /// Sealed files sharded feed reads have read through their index, in this
    /// process (tests).
    #[cfg(test)]
    pub(crate) fn feed_indexed_files_for_test() -> u64 {
        INDEXED_FILES.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Set [`TEST_INDEX_SLOTS`] (tests; 0 restores the default).
    #[cfg(test)]
    pub(crate) fn set_feed_index_slots_for_test(n: usize) {
        TEST_INDEX_SLOTS.store(n, std::sync::atomic::Ordering::Relaxed);
    }

    /// The followers a feed read goes to instead of this node: every other
    /// member of the cluster, when this node leads the tenant's raft group and
    /// the leader does not serve the feed (`QUEEN_FEED_ON_LEADER`). Empty when
    /// this node serves the read: a single node, a follower, or no other
    /// member known.
    fn feed_followers_here(&self) -> Vec<crate::rsm::replicator::raft::MemberView> {
        if feed_on_leader_allowed() {
            return Vec::new();
        }
        // No cluster view: a single node, which has no follower to send to.
        let Some(view) = self.repl.cluster_view() else {
            return Vec::new();
        };
        if !matches!(
            self.repl.role(),
            crate::rsm::replicator::Role::Leader { .. }
        ) {
            return Vec::new();
        }
        view.members
            .into_iter()
            .filter(|m| m.node_id != view.node_id)
            .collect()
    }

    /// The HTTP addresses of [`Self::feed_followers_here`], for the HTTP layer
    /// that relays a feed read a caller asked to be relayed
    /// (`handlers/raft.rs`, `relay_feed_from_leader`): the followers that
    /// answered a heartbeat most recently first, so a dead one is tried last.
    pub(crate) fn feed_relay_followers(&self) -> Vec<String> {
        let mut members: Vec<_> = self
            .feed_followers_here()
            .into_iter()
            .filter(|m| !m.http_addr.is_empty())
            .collect();
        members.sort_by_key(|m| m.heartbeat_age_ms.unwrap_or(u64::MAX));
        members.into_iter().map(|m| m.http_addr).collect()
    }

    /// The leader's answer to a feed read, when this node leads the tenant's
    /// raft group of a cluster: 421 and the followers to ask. `None` when it
    /// may serve the read.
    fn feed_refusal(&self) -> Option<ApiOut> {
        let followers: Vec<Value> = self
            .feed_followers_here()
            .iter()
            .map(|m| json!({"nodeId": m.node_id, "httpAddr": m.http_addr, "voter": m.voter}))
            .collect();
        if followers.is_empty() {
            return None;
        }
        Some(ApiOut::json(
            421,
            json!({
                "error": "this node leads the tenant's raft group: the feed is read from followers so that its readers add no load to the leader; ask one of `followers`",
                "code": "feed_on_leader",
                "followers": followers,
            })
            .to_string(),
        ))
    }

    async fn feed_once(
        &self,
        tenant: &str,
        ask: &FeedAsk,
        qlog: &QLogReader,
    ) -> Result<FeedPage, RsmError> {
        let store = self.store.clone();
        let encryption = self.encryption.clone();
        let qlog = qlog.clone();
        let tenant = tenant.to_string();
        let ask = ask.clone();
        tokio::task::spawn_blocking(move || feed_read(&store, &qlog, &encryption, &tenant, &ask))
            .await
            .map_err(|e| RsmError::Internal(format!("feed read task: {e}")))?
    }

    /// `POST /api/v1/feed`.
    pub(super) async fn api_feed(&self, ctx: ReqCtx, body: &[u8]) -> Result<ApiOut, RsmError> {
        let bad = |msg: String| ApiOut::json(400, json!({ "error": msg }).to_string());
        let parsed: FeedBody = match serde_json::from_slice(body) {
            Ok(b) => b,
            Err(e) => return Ok(bad(format!("bad body: {e}"))),
        };
        let (mut ask, max_wait) = match FeedAsk::from_body(parsed) {
            Ok(x) => x,
            Err(msg) => return Ok(bad(msg)),
        };
        if let Some(out) = self.feed_refusal() {
            return Ok(out);
        }
        let Some(qlog) = self.qlog_reader.clone() else {
            return Ok(ApiOut::json(
                501,
                json!({
                    "error": "the feed reads the queue logs, and this node keeps its messages in segment files",
                    "code": "feed_unavailable",
                })
                .to_string(),
            ));
        };
        let deadline = Instant::now() + Duration::from_millis(max_wait);
        let mut armed = false;
        let mut watch: Option<crate::notify::PartitionWatch> = None;
        loop {
            let page = self.feed_once(&ctx.tenant, &ask, &qlog).await?;
            let now = Instant::now();
            if !page.empty || !page.caught_up || max_wait == 0 || now >= deadline {
                return Ok(ApiOut::json(200, page.json));
            }
            // Caught up with nothing to send: wait for a push to a selected
            // queue on this node, from where this read ended.
            ask.start = Start::At(page.next);
            if !armed {
                // Armed, then read once more: a push applied between the read
                // and the arming is read now rather than missed. No watch on
                // nothing: one would only make apply collect every entry's
                // partitions for nobody.
                armed = true;
                let keys = watch_keys(&ctx.tenant, &ask, &page.selected);
                if !keys.is_empty() {
                    watch = Some(self.notifier.watch_topics(&keys));
                }
                continue;
            }
            // Capped at a second: a queue created after the watch was armed
            // under a name only a pattern gives has no watch, and the re-read
            // is the floor.
            let wait = deadline
                .saturating_duration_since(now)
                .min(Duration::from_secs(1));
            match &watch {
                Some(w) => {
                    w.wait(wait).await;
                }
                None => tokio::time::sleep(wait).await,
            }
        }
    }
}

/// The queues a caught-up read waits on: the ones it selected, and every
/// queue the call names exactly (no `*` or `?`), which may not exist yet. The
/// first push to such a queue creates it, and wakes the read at once instead
/// of at its next one-second re-read.
fn watch_keys(tenant: &str, ask: &FeedAsk, selected: &[String]) -> Vec<String> {
    let named = ask
        .patterns
        .iter()
        .flatten()
        .filter(|p| !p.contains(['*', '?']));
    let queues: BTreeSet<&str> = selected.iter().chain(named).map(String::as_str).collect();
    queues
        .into_iter()
        .map(|q| crate::handlers::tenant_queue_key(tenant, q))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_matches_like_a_shell() {
        assert!(glob_match("orders", "orders"));
        assert!(!glob_match("orders", "orders-it"));
        assert!(glob_match("orders*", "orders-it"));
        assert!(glob_match("orders*", "orders"));
        assert!(glob_match("*-it", "orders-it"));
        assert!(glob_match("o?ders", "orders"));
        assert!(glob_match("*", "anything"));
        assert!(!glob_match("pay*", "orders"));
        assert!(glob_match("a*b*c", "a-x-b-y-c"));
        assert!(!glob_match("a*b*c", "a-x-c-y-b"));
    }

    fn entry(payload: &str) -> Entry {
        Entry {
            position: 7,
            ts_us: 0,
            queue: "orders".into(),
            partition: "customer-42".into(),
            partition_id: String::new(),
            id: String::new(),
            offset: 3,
            txn: "order-1".into(),
            payload: payload.as_bytes().to_vec(),
        }
    }

    fn ask(conds: Value) -> FeedAsk {
        let body: FeedBody = serde_json::from_value(json!({ "where": conds })).unwrap();
        FeedAsk::from_body(body).unwrap().0
    }

    fn holds(a: &FeedAsk, e: &Entry) -> bool {
        let doc: Option<Value> = serde_json::from_slice(&e.payload).ok();
        a.matches(e, doc.as_ref())
    }

    #[test]
    fn where_reads_payload_paths_and_message_fields() {
        let e = entry(r#"{"country":"IT","total":40.5,"tags":["vip"],"customer":{"tier":2}}"#);
        assert!(holds(
            &ask(json!([{"field":"data.country","op":"=","value":"IT"}])),
            &e
        ));
        assert!(!holds(
            &ask(json!([{"field":"data.country","op":"=","value":"FR"}])),
            &e
        ));
        assert!(holds(
            &ask(json!([{"field":"data.total","op":">","value":40}])),
            &e
        ));
        assert!(holds(
            &ask(json!([{"field":"data.total","op":"<=","value":40.5}])),
            &e
        ));
        assert!(holds(
            &ask(json!([{"field":"data.customer.tier","op":"=","value":2.0}])),
            &e
        ));
        assert!(holds(
            &ask(json!([{"field":"data.tags.0","op":"=","value":"vip"}])),
            &e
        ));
        assert!(holds(
            &ask(json!([{"field":"data.country","op":"in","value":["FR","IT"]}])),
            &e
        ));
        assert!(holds(
            &ask(json!([{"field":"partition","op":"prefix","value":"customer-"}])),
            &e
        ));
        assert!(!holds(
            &ask(json!([{"field":"partition","op":"!prefix","value":"customer-"}])),
            &e
        ));
        assert!(holds(
            &ask(json!([{"field":"transactionId","op":"!prefix","value":"fd-"}])),
            &e
        ));
        assert!(holds(
            &ask(json!([{"field":"queue","op":"=","value":"orders"}])),
            &e
        ));
        assert!(holds(
            &ask(json!([{"field":"offset","op":">=","value":3}])),
            &e
        ));
        assert!(holds(
            &ask(json!([{"field":"data.customer","op":"exists"}])),
            &e
        ));
        // A path the payload lacks holds for no operator, `!=` included.
        assert!(!holds(
            &ask(json!([{"field":"data.missing","op":"!=","value":"x"}])),
            &e
        ));
        assert!(!holds(
            &ask(json!([{"field":"data.missing","op":"exists"}])),
            &e
        ));
        // Conditions are ANDed.
        assert!(!holds(
            &ask(json!([
                {"field":"data.country","op":"=","value":"IT"},
                {"field":"data.total","op":"<","value":10}
            ])),
            &e
        ));
        // A payload that is not JSON has no data fields.
        let raw = entry("not json");
        assert!(!holds(&ask(json!([{"field":"data","op":"exists"}])), &raw));
        assert!(holds(
            &ask(json!([{"field":"transactionId","op":"=","value":"order-1"}])),
            &raw
        ));
    }

    #[test]
    fn a_bad_body_is_named() {
        let err = |v: Value| {
            let body: FeedBody = serde_json::from_value(v).unwrap();
            FeedAsk::from_body(body).unwrap_err()
        };
        assert!(
            err(json!({"where":[{"field":"nope","op":"=","value":1}]})).contains("unknown field")
        );
        assert!(
            err(json!({"where":[{"field":"data.x","op":"~","value":1}]})).contains("unknown op")
        );
        assert!(
            err(json!({"where":[{"field":"data.x","op":"in","value":1}]}))
                .contains("needs an array")
        );
        assert!(err(json!({"kinds":["ack"]})).contains("push"));
        assert!(err(json!({"from":-1})).contains("position"));
        assert!(err(json!({"since":"yesterday"})).contains("timestamp"));
        // An unknown key is refused rather than ignored (a typo would widen
        // the read).
        assert!(serde_json::from_value::<FeedBody>(json!({"queue":"orders"})).is_err());
    }

    #[test]
    fn fnv1a_is_the_standard_one() {
        // The published FNV-1a 32-bit test vectors: clients hash the same way.
        assert_eq!(fnv1a32(b""), 0x811c_9dc5);
        assert_eq!(fnv1a32(b"a"), 0xe40c_292c);
        assert_eq!(fnv1a32(b"foobar"), 0xbf9c_f968);
        // UTF-8 bytes, not characters.
        assert_eq!(fnv1a32("é".as_bytes()), fnv1a32(&[0xc3, 0xa9]));
    }

    fn shard_ask(v: Value) -> Result<FeedAsk, String> {
        let body: FeedBody = serde_json::from_value(v).map_err(|e| e.to_string())?;
        FeedAsk::from_body(body).map(|(a, _)| a)
    }

    #[test]
    fn a_shard_reads_its_partitions_only_and_every_partition_has_one_shard() {
        let count = 4u64;
        let asks: Vec<FeedAsk> = (0..count)
            .map(|i| shard_ask(json!({"shard": {"count": count, "index": i}})).unwrap())
            .collect();
        for n in 0..200 {
            let p = format!("customer-{n}");
            let owners = asks.iter().filter(|a| a.in_shard("orders", &p)).count();
            assert_eq!(owners, 1, "{p} belongs to exactly one shard");
            let i = shard_of(&p, count as u32) as usize;
            assert!(asks[i].in_shard("orders", &p));
        }
        // The partitions spread over every shard.
        let mut seen = HashSet::new();
        for n in 0..200 {
            seen.insert(shard_of(&format!("customer-{n}"), count as u32));
        }
        assert_eq!(seen.len(), count as usize);
        // No shard: everything.
        assert!(shard_ask(json!({})).unwrap().in_shard("orders", "anything"));
    }

    #[test]
    fn a_shard_with_queues_splits_those_and_passes_the_others_whole() {
        let a =
            shard_ask(json!({"shard": {"count": 2, "index": 0, "queues": ["orders*"]}})).unwrap();
        let b =
            shard_ask(json!({"shard": {"count": 2, "index": 1, "queues": ["orders*"]}})).unwrap();
        for n in 0..50 {
            let p = format!("p-{n}");
            assert_ne!(a.in_shard("orders-it", &p), b.in_shard("orders-it", &p));
            // A table queue is read whole by every shard.
            assert!(a.in_shard("settings", &p) && b.in_shard("settings", &p));
        }
    }

    #[test]
    fn a_bad_shard_is_named() {
        assert!(shard_ask(json!({"shard": {"count": 0, "index": 0}}))
            .unwrap_err()
            .contains("count"));
        assert!(shard_ask(json!({"shard": {"count": 4, "index": 4}}))
            .unwrap_err()
            .contains("index"));
        assert!(shard_ask(json!({"shard": {"count": 5000, "index": 0}}))
            .unwrap_err()
            .contains("count"));
        assert!(
            shard_ask(json!({"shard": {"count": 2, "index": 0, "queues": [""]}}))
                .unwrap_err()
                .contains("empty")
        );
        assert!(shard_ask(json!({"shard": {"count": 2, "index": 0, "by": "x"}})).is_err());
    }

    #[test]
    fn a_read_waits_on_the_queues_it_names_even_before_they_exist() {
        let key = |q: &str| crate::handlers::tenant_queue_key("t", q);
        let a = shard_ask(json!({"queues": ["fresh", "orders*", "o?ders"]})).unwrap();
        assert_eq!(
            watch_keys("t", &a, &["orders-it".to_string()]),
            vec![key("fresh"), key("orders-it")]
        );
        // Patterns only, matching nothing yet: nothing to wait on by name.
        let b = shard_ask(json!({"queues": ["pay*"]})).unwrap();
        assert!(watch_keys("t", &b, &[]).is_empty());
        // Selected and named: once.
        let c = shard_ask(json!({"queues": ["orders"]})).unwrap();
        assert_eq!(
            watch_keys("t", &c, &["orders".to_string()]),
            vec![key("orders")]
        );
    }

    #[test]
    fn from_reads_positions_and_words() {
        assert_eq!(parse_start(None).unwrap(), Start::Now);
        assert_eq!(parse_start(Some(json!(12))).unwrap(), Start::At(12));
        assert_eq!(parse_start(Some(json!("12"))).unwrap(), Start::At(12));
        assert_eq!(parse_start(Some(json!("now"))).unwrap(), Start::Now);
        assert_eq!(
            parse_start(Some(json!("earliest"))).unwrap(),
            Start::Earliest
        );
        assert!(parse_start(Some(json!(true))).is_err());
    }
}
