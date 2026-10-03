//! The Queen side of the sink: the [`QueenApi`] trait the broker implements
//! in-process, the key/value wire this crate speaks through it, the decoders of
//! the three routes' answers, and a faithful in-memory double for the tests of
//! every module above it.
//!
//! The sink runs INSIDE the broker, on every node of the cluster, and reads the
//! node's own applied state: there is no socket, no bearer and no URL between
//! the two. The broker hands the crate an `Arc<dyn QueenApi>` whose four calls
//! are the in-process twins of four routes, with the routes' semantics:
//!
//!   * **fetch** — `POST /api/v1/fetch`: one read per entry, answered one entry
//!     per request entry, in request order, carrying `records`, `highWatermark`,
//!     `logStartOffset` and an optional per-entry `error`
//!     (server/src/rsm/facade/real/phase2/reads.rs `fetch_once`). A record is
//!     `{offset, transactionId, payload, ts}` and nothing else, and `ts` is the
//!     stamp of the append that wrote it, at microsecond precision.
//!   * **partitions_changed** — `POST /api/v1/partitions/changed`, the discovery
//!     endpoint of plan §5.1 (reads.rs `changed_read`): the serving node's
//!     `safeTime` for the whole call and, per queue, the partitions whose
//!     `lastWriteAt` is at or after `since`, each with its `id`, in partition
//!     creation order, paged through an OPAQUE cursor.
//!   * **kv** — `POST /api/v1/kv` `{"operations":[…]}` → `{"results":[…]}`, one
//!     result per operation, each stamped with its own `index`
//!     (server/src/handlers/kv.rs, server/src/rsm/planner/kv.rs). The two
//!     commit-pointer documents and the queue lease live here. The broker's
//!     adapter can answer it as the route does — the wire is [`KvOp::to_json`]
//!     and [`parse_kv_answer`] — or build [`KvResult`]s itself.
//!   * **list_queues** — the queue NAMES of the tenant, which is all
//!     `QUEEN_S3_QUEUES=*` needs.
//!
//! ONE RULE THAT IS NOT OBVIOUS FROM THE TYPES: a lost KV precondition arrives
//! as HTTP **200** with `{"ok":false,"reason":"kv_precondition",…}`, never as a
//! status code, and deliberately so (handlers/kv.rs `precondition_200`: it is
//! the EXPECTED outcome of every legitimate redelivery, and it must pollute
//! neither the error metrics nor the retry policies). It is mapped to
//! [`SinkError::Precondition`], the one error in this crate that must never be
//! retried blindly: for the sink it means another node owns the queue, or the
//! pointer moved under this one.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;
use serde_json::Value;

use crate::types::{
    ChangedEntry, ChangedRequestEntry, ChangedResponse, FetchError, FetchRequestEntry,
    FetchedEntry, Micros, PartitionBounds, Record, SinkError,
};

/// A boxed future, so [`QueenApi`] stays dyn-compatible. `async fn` in a trait
/// is not: it desugars to an opaque associated type no trait object can name,
/// and the driver holds `Arc<dyn QueenApi>` precisely so the broker can hand it
/// its in-process implementation and the tests [`FakeQueen`].
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub type Result<T> = std::result::Result<T, SinkError>;

/// The KV namespace, for every key this connector writes. A constant and not a
/// field: the broker validates namespaces against `^[a-z0-9][a-z0-9._-]{0,63}$`,
/// it is never a place to put anything a client chose, and one sink writing two
/// namespaces would be two commit stores for one sink. The broker's retention
/// hold reads the commit pointer in this namespace by name
/// (server/src/rsm/maintenance.rs `sink_floor`).
pub const KV_NAMESPACE: &str = "queen-s3";

/// Entries per fetch. The broker refuses more than 1024 in one call, so the
/// ceiling is checked by the caller rather than discovered as a refusal.
pub const MAX_FETCH_ENTRIES: usize = 1024;
/// Queues per discovery call the sink makes (plan §5.1). The broker takes
/// more; the driver asks about one queue at a time anyway.
pub const MAX_CHANGED_ENTRIES: usize = 64;
/// Partitions per entry per call, clamped rather than refused — the broker
/// clamps identically.
pub const MAX_CHANGED_LIMIT: u32 = 1_000;
/// Rows per `getPrefix` page: the broker clamps `limit` to `1..=1000`.
pub const MAX_KV_PREFIX_LIMIT: i64 = 1_000;

// ---------------------------------------------------------------------------
// KV operations
// ---------------------------------------------------------------------------

/// One key/value operation, in the shape the KV route takes.
///
/// `ns` is NOT a field: it is [`KV_NAMESPACE`] on every operation this crate
/// builds, and [`KvOp::to_json`] writes it. The conditional half — `expect` and
/// `required` — is the whole of plan §6.6's fence, and it is the broker's own
/// (server/src/rsm/planner/kv.rs: the planner judges every `expect` against the
/// version the previous writer left, and a lost `required` precondition aborts
/// the whole call): nothing on the broker exists for the sink.
#[derive(Debug, Clone, PartialEq)]
pub enum KvOp {
    /// Read one key. Answers `found` separately from `value`, because `null` is
    /// a legal stored value and `{found:true,value:null}` is not
    /// `{found:false}`.
    Get { key: String },
    /// Read a known key list: `rows` for the ones that are there, `missing` for
    /// the ones that are not. Absence is a datum, not a hole computed by
    /// difference.
    GetMany { keys: Vec<String> },
    /// Read a key range by prefix, paged with an EXCLUSIVE `after` cursor in
    /// byte order. `limit` is clamped by the broker to `1..=1000`.
    GetPrefix {
        prefix: String,
        limit: i64,
        after: Option<String>,
    },
    /// An upsert, optionally conditional.
    ///
    /// `ttl_seconds: None` is **forever**, and it is spelled `"forever": true`
    /// on the wire: the broker demands EXACTLY ONE of `ttlSeconds` and
    /// `forever` on every write, so that nothing lands in the store without
    /// somebody having decided when it leaves. The commit pointer's answer is
    /// "never" — an expired pointer is a silent full replay (plan §12) — and the
    /// lease's answer is `QUEEN_S3_LEASE_TTL_MS`, because that row IS the
    /// liveness claim.
    ///
    /// `expect: Some(0)` is "must not exist"; `expect: Some(n>0)` is a PURE
    /// UPDATE that creates nothing when the key is absent.
    Put {
        key: String,
        value: Value,
        ttl_seconds: Option<u64>,
        expect: Option<i64>,
        /// Turn a lost precondition from a verdict into an abort of the WHOLE
        /// batch — the fence.
        required: bool,
    },
    /// `putIfAbsent`: a `put` with `expect: 0`, which wins against an
    /// expired-but-unswept row (liveness is `expires > now` for every reader
    /// and writer) — which is what lets an instance that restarts before the
    /// sweep reclaim the lease rather than lose to its own corpse.
    PutIfAbsent {
        key: String,
        value: Value,
        ttl_seconds: Option<u64>,
        required: bool,
    },
    /// Remove a key. `expect: Some(n)` is a fenced delete.
    Delete {
        key: String,
        expect: Option<i64>,
        required: bool,
    },
}

impl KvOp {
    pub fn get(key: impl Into<String>) -> KvOp {
        KvOp::Get { key: key.into() }
    }

    pub fn get_many(keys: Vec<String>) -> KvOp {
        KvOp::GetMany { keys }
    }

    pub fn get_prefix(prefix: impl Into<String>, limit: i64, after: Option<String>) -> KvOp {
        KvOp::GetPrefix {
            prefix: prefix.into(),
            limit,
            after,
        }
    }

    /// An unconditional write that never expires — the commit pointer's shape.
    pub fn put(key: impl Into<String>, value: Value) -> KvOp {
        KvOp::Put {
            key: key.into(),
            value,
            ttl_seconds: None,
            expect: None,
            required: false,
        }
    }

    /// A conditional write that never expires and answers a VERDICT rather than
    /// aborting: `applied:false` with the winner's version and value.
    pub fn put_expecting(key: impl Into<String>, value: Value, expect: i64) -> KvOp {
        KvOp::Put {
            key: key.into(),
            value,
            ttl_seconds: None,
            expect: Some(expect),
            required: false,
        }
    }

    /// A FENCED write that never expires: conditional AND `required`, so losing
    /// it rolls the whole batch back rather than answering `applied:false`
    /// beside writes that landed anyway. Plan §6.6: this is what makes it
    /// impossible for two instances to commit different window `k`s for one
    /// queue.
    pub fn fence(key: impl Into<String>, value: Value, expect: i64) -> KvOp {
        KvOp::Put {
            key: key.into(),
            value,
            ttl_seconds: None,
            expect: Some(expect),
            required: true,
        }
    }

    /// A write that expires — the lease heartbeat.
    pub fn put_ttl(key: impl Into<String>, value: Value, ttl_seconds: u64) -> KvOp {
        KvOp::Put {
            key: key.into(),
            value,
            ttl_seconds: Some(ttl_seconds),
            expect: None,
            required: false,
        }
    }

    /// A fenced write that expires: the lease refresh that must lose to whoever
    /// took the lease away.
    pub fn fence_ttl(key: impl Into<String>, value: Value, expect: i64, ttl_seconds: u64) -> KvOp {
        KvOp::Put {
            key: key.into(),
            value,
            ttl_seconds: Some(ttl_seconds),
            expect: Some(expect),
            required: true,
        }
    }

    /// The lease claim: take it if nobody holds it, and be TOLD who does if
    /// somebody already has. Not `required`, deliberately — losing it is not an
    /// error, it is the answer "somebody owns this queue, at this version, and
    /// here is their row".
    pub fn put_if_absent_ttl(key: impl Into<String>, value: Value, ttl_seconds: u64) -> KvOp {
        KvOp::PutIfAbsent {
            key: key.into(),
            value,
            ttl_seconds: Some(ttl_seconds),
            required: false,
        }
    }

    pub fn delete(key: impl Into<String>, expect: Option<i64>) -> KvOp {
        KvOp::Delete {
            key: key.into(),
            expect,
            required: false,
        }
    }

    /// The key this operation addresses, or `None` for the two multi-key reads.
    pub fn key(&self) -> Option<&str> {
        match self {
            KvOp::Get { key }
            | KvOp::Put { key, .. }
            | KvOp::PutIfAbsent { key, .. }
            | KvOp::Delete { key, .. } => Some(key),
            KvOp::GetMany { .. } | KvOp::GetPrefix { .. } => None,
        }
    }

    /// Whether a lost precondition on this operation rolls the whole batch back.
    pub fn is_required(&self) -> bool {
        match self {
            KvOp::Put { required, .. }
            | KvOp::PutIfAbsent { required, .. }
            | KvOp::Delete { required, .. } => *required,
            KvOp::Get { .. } | KvOp::GetMany { .. } | KvOp::GetPrefix { .. } => false,
        }
    }

    /// The operation exactly as the KV route takes it — what an in-process
    /// adapter hands the broker's KV call, one element of `operations`.
    ///
    /// Built as a `Value` rather than derived, for one reason worth the extra
    /// lines: `ns`, and the mutual exclusion of `ttlSeconds` and `forever`, are
    /// invariants of this crate rather than of the caller — and here they are
    /// visible in one place instead of spread over six `skip_serializing_if`
    /// attributes.
    pub fn to_json(&self) -> Value {
        let mut m = serde_json::Map::new();
        m.insert("ns".into(), Value::String(KV_NAMESPACE.into()));
        match self {
            KvOp::Get { key } => {
                m.insert("op".into(), "get".into());
                m.insert("key".into(), Value::String(key.clone()));
            }
            KvOp::GetMany { keys } => {
                m.insert("op".into(), "getMany".into());
                m.insert(
                    "keys".into(),
                    Value::Array(keys.iter().cloned().map(Value::String).collect()),
                );
            }
            KvOp::GetPrefix {
                prefix,
                limit,
                after,
            } => {
                m.insert("op".into(), "getPrefix".into());
                m.insert("prefix".into(), Value::String(prefix.clone()));
                m.insert("limit".into(), Value::from(*limit));
                if let Some(a) = after {
                    m.insert("after".into(), Value::String(a.clone()));
                }
            }
            KvOp::Put {
                key,
                value,
                ttl_seconds,
                expect,
                required,
            } => {
                m.insert("op".into(), "put".into());
                m.insert("key".into(), Value::String(key.clone()));
                m.insert("value".into(), value.clone());
                expiry(&mut m, *ttl_seconds);
                if let Some(e) = expect {
                    m.insert("expect".into(), Value::from(*e));
                }
                if *required {
                    m.insert("required".into(), Value::Bool(true));
                }
            }
            KvOp::PutIfAbsent {
                key,
                value,
                ttl_seconds,
                required,
            } => {
                m.insert("op".into(), "putIfAbsent".into());
                m.insert("key".into(), Value::String(key.clone()));
                m.insert("value".into(), value.clone());
                expiry(&mut m, *ttl_seconds);
                if *required {
                    m.insert("required".into(), Value::Bool(true));
                }
            }
            KvOp::Delete {
                key,
                expect,
                required,
            } => {
                m.insert("op".into(), "delete".into());
                m.insert("key".into(), Value::String(key.clone()));
                if let Some(e) = expect {
                    m.insert("expect".into(), Value::from(*e));
                }
                if *required {
                    m.insert("required".into(), Value::Bool(true));
                }
            }
        }
        Value::Object(m)
    }
}

/// EXACTLY ONE of `ttlSeconds` and `forever`, never both and never neither: the
/// broker refuses a write that declares both or none. `"forever": false` is
/// zero declarations, not one, so a TTL write must not carry the flag at all.
fn expiry(m: &mut serde_json::Map<String, Value>, ttl_seconds: Option<u64>) {
    match ttl_seconds {
        None => {
            m.insert("forever".into(), Value::Bool(true));
        }
        Some(secs) => {
            m.insert("ttlSeconds".into(), Value::from(secs));
        }
    }
}

/// `null` read as the type's default — for the fields the broker renders as
/// `null` when it has nothing to say (a precondition whose detail it could not
/// render).
fn null_as_default<'de, D, T>(d: D) -> std::result::Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

/// One row of a read.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
pub struct KvRow {
    #[serde(default)]
    pub key: String,
    /// `null` is a legal stored value, so this is `Value::Null` both for a key
    /// holding null and for an answer that carried none.
    #[serde(default)]
    pub value: Value,
    /// Opaque and unique, never re-issued — so there is no ABA. Compared for
    /// EQUALITY only, never ordered and never arithmetic. `0` is "not there",
    /// which is also how an expired row reads.
    #[serde(default, deserialize_with = "null_as_default")]
    pub version: i64,
}

/// What one operation answered.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct KvResult {
    #[serde(default, deserialize_with = "null_as_default")]
    pub index: usize,
    /// The operation as the broker labelled it. Informational: nothing in the
    /// crate branches on it.
    #[serde(default)]
    pub op: String,
    /// `get` only, and separate from `value` on purpose.
    #[serde(default)]
    pub found: Option<bool>,
    #[serde(default)]
    pub key: Option<String>,
    /// Writes only. A write with no precondition is always `Some(true)`.
    #[serde(default)]
    pub applied: Option<bool>,
    /// The version the key holds AFTER the operation when it applied, and the
    /// WINNER's when it did not — so a loser never needs a second round trip.
    #[serde(default, deserialize_with = "null_as_default")]
    pub version: i64,
    /// Writes only, and only when `applied` is false: `version`, `absent` or
    /// `exists`. Which one decides whether a fence is retried or handed over.
    #[serde(default)]
    pub reason: Option<String>,
    /// The value under the key; the WINNER's when the operation did not apply.
    #[serde(default)]
    pub value: Value,
    #[serde(default)]
    pub rows: Vec<KvRow>,
    /// `getMany` only: the keys that are not there.
    #[serde(default)]
    pub missing: Vec<String>,
    /// The read did not return everything it matched — either the page limit or
    /// the 4 MiB call budget cut it. Keys lost to the budget are in NEITHER
    /// `rows` nor `missing`, which is why this flag can never be ignored.
    #[serde(default)]
    pub truncated: bool,
    /// `getPrefix` only: the cursor to continue from, set when `truncated`.
    #[serde(default)]
    pub next_after: Option<String>,
}

impl KvResult {
    /// The value of a `get` that found something.
    pub fn value_if_found(&self) -> Option<&Value> {
        match self.found {
            Some(true) => Some(&self.value),
            _ => None,
        }
    }

    /// Whether a conditional write landed. `None` (a read) is not "applied".
    pub fn did_apply(&self) -> bool {
        self.applied == Some(true)
    }
}

// ---------------------------------------------------------------------------
// The trait
// ---------------------------------------------------------------------------

/// The calls the sink makes to Queen — implemented by the broker, in-process,
/// against the node the sink runs on.
///
/// Every method takes OWNED arguments: the driver builds a fetch batch per
/// window from a buffer it then drops, and a borrowed slice would tie the
/// future's lifetime to a value the caller wants to reuse while the call is in
/// flight.
///
/// The log reads — `fetch` and `partitions_changed` — must be served from
/// THIS node's applied state, and `partitions_changed` must answer this node's
/// `safeTime`: the window engine pairs the two, and a `safeTime` from one node
/// with reads from another is the one mix it cannot survive. `kv` goes through
/// the broker's KV path like any client's: a read-only call waits for the
/// cluster's read index (linearizable), and a call with a write reads at its
/// own entry, so the pointers and the lease a node reads are the latest.
pub trait QueenApi: Send + Sync {
    /// The twin of `POST /api/v1/fetch` — one read per entry, answered one
    /// result per entry, in request order. The sink always asks with
    /// `max_wait_ms = 0`: it never parks.
    fn fetch(
        &self,
        entries: Vec<FetchRequestEntry>,
        max_wait_ms: u64,
        min_bytes: i64,
    ) -> BoxFuture<'_, Result<Vec<FetchedEntry>>>;

    /// The twin of `POST /api/v1/partitions/changed` — the discovery sweep of
    /// plan §5.1, plus the `safeTime` every window close is bounded by.
    fn partitions_changed(
        &self,
        entries: Vec<ChangedRequestEntry>,
    ) -> BoxFuture<'_, Result<ChangedResponse>>;

    /// The twin of `POST /api/v1/kv` — one answer per operation, aligned by
    /// the `index` each answer carries. A lost `required` precondition is
    /// `Err(SinkError::Precondition)` ([`parse_kv_answer`] does that mapping
    /// for an adapter that answers with the route's body).
    fn kv(&self, ops: Vec<KvOp>) -> BoxFuture<'_, Result<Vec<KvResult>>>;

    /// The tenant's queue NAMES, in the listing's own order — what
    /// `QUEEN_S3_QUEUES=*` resolves through.
    fn list_queues(&self) -> BoxFuture<'_, Result<Vec<String>>>;
}

// ---------------------------------------------------------------------------
// Wire bodies
// ---------------------------------------------------------------------------
//
// The routes' JSON, for an adapter that answers the trait through the routes'
// shapes rather than through the broker's typed twins. The sink itself never
// builds a request: these are here so such an adapter and the routes cannot
// drift apart without a test noticing.

#[derive(Deserialize)]
struct FetchResponseBody {
    #[serde(default)]
    entries: Vec<FetchResultEntry>,
}

#[derive(Deserialize)]
struct FetchResultEntry {
    #[serde(default)]
    queue: String,
    #[serde(default)]
    partition: String,
    #[serde(default)]
    records: Vec<WireRecord>,
    #[serde(rename = "highWatermark", default)]
    high_watermark: i64,
    #[serde(rename = "logStartOffset", default)]
    log_start_offset: i64,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct WireRecord {
    #[serde(default)]
    offset: i64,
    #[serde(rename = "transactionId", default)]
    transaction_id: String,
    /// Kept as raw text and NEVER parsed into a tree: that parse was the
    /// loader's ceiling at 1M msg/s and it is the one thing this connector may
    /// not spend CPU on (plan §6.5).
    #[serde(default)]
    payload: Option<Box<RawValue>>,
    #[serde(default)]
    ts: String,
}

#[derive(Deserialize)]
struct ChangedResponseBody {
    #[serde(rename = "safeTime", default)]
    safe_time: String,
    #[serde(rename = "safeTimeDegraded", default)]
    safe_time_degraded: bool,
    #[serde(default)]
    entries: Vec<ChangedResultEntry>,
}

#[derive(Deserialize)]
struct ChangedResultEntry {
    #[serde(default)]
    queue: String,
    #[serde(default)]
    partitions: Vec<WirePartition>,
    #[serde(default)]
    next: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct WirePartition {
    #[serde(default)]
    name: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(rename = "lastOffset", default)]
    last_offset: i64,
    #[serde(rename = "logStart", default)]
    log_start: i64,
    #[serde(rename = "lastWriteAt", default)]
    last_write_at: Option<String>,
}

/// The KV envelope. `ok:false` is the lost-precondition verdict, and it arrives
/// with HTTP 200 (handlers/kv.rs `precondition_200`). `failedIndex`, `version`
/// and `value` are `null` when the broker could not render the detail; the
/// index then reads as 0, i.e. as the fence, which is the cautious reading:
/// the driver stops the queue either way.
#[derive(Deserialize)]
struct KvResponseBody {
    #[serde(default = "yes")]
    ok: bool,
    #[serde(default)]
    reason: String,
    #[serde(rename = "failedIndex", default, deserialize_with = "null_as_default")]
    failed_index: usize,
    #[serde(rename = "kvReason", default)]
    kv_reason: Option<String>,
    #[serde(default, deserialize_with = "null_as_default")]
    version: i64,
    #[serde(default)]
    value: Value,
    #[serde(default)]
    results: Vec<KvResult>,
}

fn yes() -> bool {
    true
}

/// The body of `POST /api/v1/fetch` for these entries.
pub fn fetch_body(entries: &[FetchRequestEntry], max_wait_ms: u64, min_bytes: i64) -> Value {
    Value::Object(
        [
            (
                "entries".to_string(),
                Value::Array(
                    entries
                        .iter()
                        .map(|e| {
                            let mut m = serde_json::Map::new();
                            m.insert("queue".into(), Value::String(e.queue.clone()));
                            m.insert("partition".into(), Value::String(e.partition.to_string()));
                            m.insert("offset".into(), Value::from(e.offset));
                            if let Some(mb) = e.max_bytes {
                                m.insert("maxBytes".into(), Value::from(mb));
                            }
                            Value::Object(m)
                        })
                        .collect(),
                ),
            ),
            ("maxWaitMs".to_string(), Value::from(max_wait_ms)),
            ("minBytes".to_string(), Value::from(min_bytes)),
        ]
        .into_iter()
        .collect(),
    )
}

/// The body of `POST /api/v1/partitions/changed` for these entries.
///
/// `since` is rendered as the broker's own ISO-micros form, or `null` for a
/// listing of every partition. [`Micros::MIN`] is `null` too: `-∞` IS
/// "everything there is", and rendering it would put the string `-inf` on a
/// wire that parses timestamps (and refuses a malformed one with a 400).
pub fn changed_body(entries: &[ChangedRequestEntry]) -> Value {
    Value::Object(
        [(
            "entries".to_string(),
            Value::Array(
                entries
                    .iter()
                    .map(|e| {
                        let mut m = serde_json::Map::new();
                        m.insert("queue".into(), Value::String(e.queue.clone()));
                        m.insert(
                            "since".into(),
                            match e.since.filter(|s| *s != Micros::MIN) {
                                Some(t) => Value::String(t.to_iso()),
                                None => Value::Null,
                            },
                        );
                        m.insert(
                            "after".into(),
                            match &e.after {
                                Some(a) => Value::String(a.clone()),
                                None => Value::Null,
                            },
                        );
                        m.insert(
                            "limit".into(),
                            Value::from(e.limit.clamp(1, MAX_CHANGED_LIMIT)),
                        );
                        Value::Object(m)
                    })
                    .collect(),
            ),
        )]
        .into_iter()
        .collect(),
    )
}

/// The body of `POST /api/v1/kv`. `{"operations":[…]}` and not the bare array
/// the route also accepts: it is the shape the transaction wire uses, so one
/// shape is learned once.
pub fn kv_body(ops: &[KvOp]) -> Value {
    Value::Object(
        [(
            "operations".to_string(),
            Value::Array(ops.iter().map(|o| o.to_json()).collect()),
        )]
        .into_iter()
        .collect(),
    )
}

// ---- response decoders ----------------------------------------------------

/// Match a fetch answer back to the entries that asked for it.
///
/// The broker answers in request order, so this is a second belt on braced
/// trousers — and it is worth wearing: an entry read as another partition's
/// would put one lane's records under another lane's name in the lake, for
/// ever.
pub fn decode_fetch(body: &str, asked: &[FetchRequestEntry]) -> Result<Vec<FetchedEntry>> {
    let parsed: FetchResponseBody =
        serde_json::from_str(body).map_err(|e| SinkError::Body(e.to_string()))?;
    if parsed.entries.len() != asked.len() {
        return Err(SinkError::Body(format!(
            "fetch answered {} entries for {} asked",
            parsed.entries.len(),
            asked.len()
        )));
    }
    let mut out = Vec::with_capacity(asked.len());
    for (i, (got, want)) in parsed.entries.into_iter().zip(asked).enumerate() {
        if got.queue != want.queue || got.partition != *want.partition {
            return Err(SinkError::Body(format!(
                "fetch entry {i} came back as {}/{} but was asked for {}/{}",
                got.queue, got.partition, want.queue, want.partition
            )));
        }
        let mut records = Vec::with_capacity(got.records.len());
        for r in got.records {
            let ts = Micros::parse_iso(&r.ts).map_err(SinkError::Body)?;
            records.push(Record {
                partition: want.partition.clone(),
                offset: r.offset,
                transaction_id: r.transaction_id,
                ts,
                // `"payload":null` and an absent payload are one thing here:
                // both mean the stored payload was empty.
                payload: r.payload.filter(|p| p.get().trim() != "null"),
            });
        }
        out.push(FetchedEntry {
            queue: got.queue,
            partition: want.partition.clone(),
            records,
            high_watermark: got.high_watermark,
            log_start_offset: got.log_start_offset,
            error: got.error.as_deref().map(FetchError::from_wire),
        });
    }
    Ok(out)
}

/// Decode a discovery answer, checking the per-entry queue names the same way.
pub fn decode_changed(body: &str, asked: &[ChangedRequestEntry]) -> Result<ChangedResponse> {
    let parsed: ChangedResponseBody =
        serde_json::from_str(body).map_err(|e| SinkError::Body(e.to_string()))?;
    let safe_time = Micros::parse_iso(&parsed.safe_time)
        .map_err(|e| SinkError::Body(format!("safeTime: {e}")))?;
    if parsed.entries.len() != asked.len() {
        return Err(SinkError::Body(format!(
            "partitions/changed answered {} entries for {} asked",
            parsed.entries.len(),
            asked.len()
        )));
    }
    let mut entries = Vec::with_capacity(asked.len());
    for (i, (got, want)) in parsed.entries.into_iter().zip(asked).enumerate() {
        if got.queue != want.queue {
            return Err(SinkError::Body(format!(
                "partitions/changed entry {i} came back as {} but was asked for {}",
                got.queue, want.queue
            )));
        }
        let mut partitions = Vec::with_capacity(got.partitions.len());
        for p in got.partitions {
            let last_write_at = match p.last_write_at {
                Some(s) => Some(
                    Micros::parse_iso(&s)
                        .map_err(|e| SinkError::Body(format!("lastWriteAt: {e}")))?,
                ),
                None => None,
            };
            partitions.push(PartitionBounds {
                name: p.name.into(),
                id: p.id.filter(|id| !id.is_empty()),
                last_offset: p.last_offset,
                log_start: p.log_start,
                last_write_at,
            });
        }
        entries.push(ChangedEntry {
            queue: got.queue,
            partitions,
            next: got.next,
            error: got.error,
        });
    }
    Ok(ChangedResponse {
        safe_time,
        safe_time_degraded: parsed.safe_time_degraded,
        entries,
    })
}

/// Decode a KV answer body, mapping the 200-with-`ok:false` verdict onto
/// [`SinkError::Precondition`] and aligning the results by their own `index`.
pub fn decode_kv(body: &str, ops: usize) -> Result<Vec<KvResult>> {
    let parsed: KvResponseBody =
        serde_json::from_str(body).map_err(|e| SinkError::Body(e.to_string()))?;
    if !parsed.ok {
        return Err(match parsed.reason.as_str() {
            "kv_precondition" => SinkError::Precondition {
                failed_index: parsed.failed_index,
                reason: parsed.kv_reason.unwrap_or_default(),
                version: parsed.version,
                value: parsed.value,
            },
            other => SinkError::Body(format!("kv answered ok=false, reason={other}")),
        });
    }
    let mut out: Vec<Option<KvResult>> = (0..ops).map(|_| None).collect();
    for r in parsed.results {
        let slot = out
            .get_mut(r.index)
            .ok_or_else(|| SinkError::Body(format!("kv result {} is out of range", r.index)))?;
        if slot.is_some() {
            return Err(SinkError::Body(format!(
                "kv result {} appears twice",
                r.index
            )));
        }
        *slot = Some(r);
    }
    out.into_iter()
        .enumerate()
        .map(|(i, r)| {
            r.ok_or_else(|| SinkError::Body(format!("kv answered nothing for operation {i}")))
        })
        .collect()
}

/// Turn an answer of the KV route — its HTTP status and its body, exactly as
/// `POST /api/v1/kv` produced them for `ops` operations — into one result per
/// operation, or the error the driver acts on.
///
/// * a non-2xx status is [`SinkError::Status`]: retried behind the backoff
///   when it is 408, 429 or 5xx, terminal for the queue otherwise (a 400 is a
///   refusal of the batch, which no retry changes);
/// * a 200 carrying `{"ok":false,"reason":"kv_precondition",…}` is
///   [`SinkError::Precondition`] — another node owns the queue, or the pointer
///   moved under this one, and it is never retried blindly;
/// * a 200 that does not decode, or that does not answer every operation
///   exactly once, is [`SinkError::Body`].
///
/// An empty batch is answered without looking at the body, so an adapter may
/// skip the call for one.
pub fn parse_kv_answer(status: u16, body: &str, ops: usize) -> Result<Vec<KvResult>> {
    if ops == 0 {
        return Ok(Vec::new());
    }
    if !(200..300).contains(&status) {
        return Err(SinkError::Status {
            code: status,
            body: body.to_string(),
            retry_after_ms: None,
        });
    }
    decode_kv(body, ops)
}

// ---------------------------------------------------------------------------
// The test double
// ---------------------------------------------------------------------------

/// One stored append: a stamp and the records pushed under it.
///
/// The append is the unit that carries `ts` — every record of one push to one
/// partition shares it — so the double stores appends rather than records,
/// which is what makes `ts` behave the way the window engine depends on:
/// co-monotone with offset inside a partition, and repeated across a run of
/// records.
#[derive(Clone, Debug)]
struct Segment {
    ts: Micros,
    base_offset: i64,
    records: Vec<(String, Option<Box<RawValue>>)>,
}

#[derive(Clone, Debug)]
struct Lane {
    /// The partition's id — a uuid-shaped string, new for every incarnation.
    id: String,
    /// Creation order across the whole double: the order discovery lists
    /// partitions in, and what its cursor names.
    seq: u64,
    /// The retention watermark: the first offset still stored.
    log_start: i64,
    /// The next offset the allocator hands out — i.e. the high watermark.
    next_offset: i64,
    segments: Vec<Segment>,
    /// The stamp of the partition's last record, exact, as the broker keeps it.
    last_write_at: Option<Micros>,
    /// Set by [`FakeQueen::trim_during_next_fetch`]: the next fetch of this
    /// lane meets retention trimming the head below this offset between its
    /// partition read and its log read.
    race_trim: Option<i64>,
}

/// A `put` normalised out of [`KvOp::Put`] or [`KvOp::PutIfAbsent`]: the key,
/// the value, the TTL (`None` = forever), the precondition, whether losing it
/// aborts the whole batch, and the label the answer carries. Named, because the
/// two variants are one code path on the broker (`putIfAbsent` is a `put` with
/// `expect: 0`) and must be one here too — see [`FakeState::apply_kv`].
type NormalizedPut<'a> = (
    &'a String,
    &'a Value,
    Option<u64>,
    Option<i64>,
    bool,
    &'static str,
);

#[derive(Clone, Debug)]
struct KvEntry {
    value: Value,
    version: i64,
    /// `None` = forever.
    expires_at_ms: Option<i64>,
}

#[derive(Default)]
struct FakeState {
    /// Queue → partition → lane. A queue with no lanes is a configured queue
    /// nobody has pushed to, which is a real and distinct state.
    queues: BTreeMap<String, BTreeMap<String, Lane>>,
    next_seq: u64,
    kv: BTreeMap<String, KvEntry>,
    /// Opaque and from a counter, never `version + 1` and never re-issued —
    /// the broker's rule.
    next_version: i64,
    safe_time_pin: Option<Micros>,
    safe_time_degraded: bool,
    max_ts: Option<Micros>,
    now_ms_pin: Option<i64>,
    /// `(wall ms, tokio instant)` at [`FakeQueen::follow_tokio_clock`]: TTLs
    /// then run on the tokio clock, which a paused test advances.
    tokio_clock: Option<(i64, tokio::time::Instant)>,
    fail_next: usize,
    fail_kv_next: usize,
    /// How long a KV batch waits between being sent and being applied.
    kv_latency: std::time::Duration,
    records_per_call: usize,
    fetch_calls: u64,
    changed_calls: u64,
    kv_calls: u64,
    kv_batches: Vec<Vec<KvOp>>,
}

/// A faithful in-memory Queen: the log, the discovery index, `safeTime` and the
/// key/value store, with the semantics of the raft broker's routes
/// (server/src/rsm/facade/real/phase2/reads.rs for fetch and discovery,
/// server/src/rsm/planner/kv.rs for KV) rather than a convenient approximation
/// of them — plus a few knobs that make the broker's rare cases happen on
/// demand.
///
/// It is load-bearing: every test of the window engine, the driver, the lease
/// and the seek runs against it, so a semantic it gets wrong is a semantic the
/// whole crate is proved against wrongly. That is why its own test file exists.
pub struct FakeQueen {
    state: Mutex<FakeState>,
}

impl Default for FakeQueen {
    fn default() -> FakeQueen {
        FakeQueen::new()
    }
}

/// Bytes one record is assumed to cost when a `maxBytes` ceiling is turned into
/// a record count. The broker's ceiling is over payload bytes; what matters to
/// every caller is that a ceiling truncates an answer — never below one record
/// — and the caller must come back for the rest, and this reproduces exactly
/// that.
pub const FAKE_BYTES_PER_RECORD: i64 = 1_024;

/// The broker's own per-entry record ceiling.
pub const FAKE_MAX_RECORDS_PER_ENTRY: usize = 10_000;

impl FakeQueen {
    pub fn new() -> FakeQueen {
        FakeQueen {
            state: Mutex::new(FakeState {
                records_per_call: 100_000,
                ..FakeState::default()
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    // ---- seeding ---------------------------------------------------------

    /// Configure a queue with no partitions — `/configure` and nothing else.
    /// Fetching a lane of it answers bounds `0/0` and NO error, which is the
    /// distinction the broker's fetch draws and a Kafka-shaped caller depends
    /// on.
    pub fn create_queue(&self, queue: &str) {
        self.lock().queues.entry(queue.to_string()).or_default();
    }

    /// Push one append: `payloads` is a list of JSON texts (`"null"` for an
    /// empty payload), all sharing `ts`. Returns the base offset.
    ///
    /// Transaction ids are minted `txn-<partition>-<offset>`, which is stable
    /// across runs so a test can assert on them; [`FakeQueen::push_records`]
    /// takes explicit ones.
    pub fn push(&self, queue: &str, partition: &str, ts: Micros, payloads: &[&str]) -> i64 {
        let mut records = Vec::with_capacity(payloads.len());
        for (i, p) in payloads.iter().enumerate() {
            let raw = RawValue::from_string((*p).to_string()).unwrap_or_else(|e| {
                panic!("FakeQueen::push got payload {i} that is not JSON: {e}")
            });
            let payload = if raw.get().trim() == "null" {
                None
            } else {
                Some(raw)
            };
            records.push((String::new(), payload));
        }
        self.push_records(queue, partition, ts, records)
    }

    /// [`FakeQueen::push`] with explicit transaction ids. An empty id is filled
    /// in with the minted form, so the two entry points agree.
    pub fn push_records(
        &self,
        queue: &str,
        partition: &str,
        ts: Micros,
        records: Vec<(String, Option<Box<RawValue>>)>,
    ) -> i64 {
        let mut st = self.lock();
        let seq = st.next_seq;
        let lanes = st.queues.entry(queue.to_string()).or_default();
        let created = !lanes.contains_key(partition);
        let lane = lanes.entry(partition.to_string()).or_insert_with(|| Lane {
            id: fake_partition_id(seq),
            seq,
            log_start: 0,
            next_offset: 0,
            segments: Vec::new(),
            last_write_at: None,
            race_trim: None,
        });
        let base = lane.next_offset;
        let stamped: Vec<(String, Option<Box<RawValue>>)> = records
            .into_iter()
            .enumerate()
            .map(|(i, (txn, payload))| {
                let txn = if txn.is_empty() {
                    format!("txn-{partition}-{}", base + i as i64)
                } else {
                    txn
                };
                (txn, payload)
            })
            .collect();
        lane.next_offset += stamped.len() as i64;
        lane.segments.push(Segment {
            ts,
            base_offset: base,
            records: stamped,
        });
        // The stamp of the last record. The broker's only ever rises (stamps
        // strictly increase in log order); a test may push out of order, and
        // the double keeps the larger.
        if lane.last_write_at.is_none_or(|cur| ts > cur) {
            lane.last_write_at = Some(ts);
        }
        if created {
            st.next_seq += 1;
        }
        st.max_ts = Some(match st.max_ts {
            Some(m) if m >= ts => m,
            _ => ts,
        });
        base
    }

    /// Simulate retention: everything below `offset` is gone, and a fetch from
    /// below it answers `OFFSET_OUT_OF_RANGE` — the one failure of plan §4.6.
    pub fn retention_delete_below(&self, queue: &str, partition: &str, offset: i64) {
        self.lock().trim(queue, partition, offset);
    }

    /// The next fetch of this lane meets retention trimming its head below
    /// `offset` between its partition read and its log read: it answers the
    /// bounds it read first, and records that start above the requested offset
    /// — the offset jump the broker's fetch produces by stepping over offsets
    /// it cannot find. The trim is real once that fetch has answered.
    pub fn trim_during_next_fetch(&self, queue: &str, partition: &str, offset: i64) {
        let mut st = self.lock();
        if let Some(lane) = st.queues.get_mut(queue).and_then(|q| q.get_mut(partition)) {
            lane.race_trim = Some(offset);
        }
    }

    /// Delete a partition, the way retention deletes one that has been idle
    /// and empty for `PARTITION_CLEANUP_DAYS`. The next push to the same name
    /// creates a new incarnation: a new id, offsets from 0, and a place at the
    /// END of the creation order.
    pub fn delete_partition(&self, queue: &str, partition: &str) {
        let mut st = self.lock();
        if let Some(lanes) = st.queues.get_mut(queue) {
            lanes.remove(partition);
        }
    }

    /// Delete a whole queue: discovery and fetch answer
    /// `UNKNOWN_TOPIC_OR_PARTITION` until a push creates it again, with new
    /// partitions.
    pub fn delete_queue(&self, queue: &str) {
        self.lock().queues.remove(queue);
    }

    /// Pin `safeTime`. Unpinned, it is one microsecond past the newest pushed
    /// record — "everything that exists is safe", as on a node that has applied
    /// one more log entry since (on the raft broker, the sink's own lease
    /// refresh is such an entry) — the state a test that is not about
    /// `safeTime` wants.
    pub fn set_safe_time(&self, t: Micros) {
        self.lock().safe_time_pin = Some(t);
    }

    /// Go back to the derived `safeTime`.
    pub fn clear_safe_time(&self) {
        self.lock().safe_time_pin = None;
    }

    /// The flag 1.x brokers set when they answered a floor; the raft broker
    /// always answers `false`.
    pub fn set_safe_time_degraded(&self, degraded: bool) {
        self.lock().safe_time_degraded = degraded;
    }

    /// Pin the clock TTLs are measured against. Unpinned, it is the wall clock.
    pub fn set_now_ms(&self, now_ms: i64) {
        self.lock().now_ms_pin = Some(now_ms);
    }

    /// Measure TTLs on the tokio clock from now on: a test with a paused clock
    /// then expires a lease — or a node's presence row — by letting virtual
    /// time pass, the way a node that died stops renewing.
    pub fn follow_tokio_clock(&self) {
        let mut st = self.lock();
        let now = st.now_ms();
        st.now_ms_pin = None;
        st.tokio_clock = Some((now, tokio::time::Instant::now()));
    }

    /// Move a pinned clock forward — how a lease is expired in a test.
    pub fn advance_ms(&self, delta: i64) {
        let mut st = self.lock();
        let now = st.now_ms();
        st.now_ms_pin = Some(now + delta);
    }

    /// A record ceiling over a whole fetch CALL, spent in entry order. Not a
    /// broker rule — the broker bounds each entry on its own — but the way to
    /// make an entry come back short or empty on demand, which a caller must
    /// survive whatever causes it.
    pub fn set_records_per_call(&self, n: usize) {
        self.lock().records_per_call = n;
    }

    /// Fail the next `n` calls to `fetch`, `partitions_changed` and
    /// `list_queues` with a transport error. KV is separate, because the two
    /// failure modes have different consequences: a failed read is a retry, a
    /// failed commit is a window that may or may not have landed.
    pub fn fail_next(&self, n: usize) {
        self.lock().fail_next = n;
    }

    /// Fail the next `n` calls to `kv`.
    pub fn fail_kv_next(&self, n: usize) {
        self.lock().fail_kv_next = n;
    }

    /// Apply every KV batch `latency` after it is sent (on the tokio clock), as
    /// the broker applies a proposal once raft has committed it. Two batches
    /// sent within that time are both in flight, each with the preconditions
    /// its sender read — the race a caller that writes one row from two tasks
    /// has to survive.
    pub fn set_kv_latency(&self, latency: std::time::Duration) {
        self.lock().kv_latency = latency;
    }

    // ---- inspection ------------------------------------------------------

    pub fn safe_time(&self) -> Micros {
        self.lock().safe_time()
    }

    pub fn fetch_calls(&self) -> u64 {
        self.lock().fetch_calls
    }

    pub fn changed_calls(&self) -> u64 {
        self.lock().changed_calls
    }

    pub fn kv_calls(&self) -> u64 {
        self.lock().kv_calls
    }

    /// Every KV batch this double was asked to apply, in order — so a test can
    /// assert that a commit carried its fence at index 0.
    pub fn kv_batches(&self) -> Vec<Vec<KvOp>> {
        self.lock().kv_batches.clone()
    }

    /// One live key's value, or `None` when it is absent or expired.
    pub fn kv_get(&self, key: &str) -> Option<Value> {
        let st = self.lock();
        let now = st.now_ms();
        st.kv
            .get(key)
            .filter(|e| e.expires_at_ms.is_none_or(|exp| exp > now))
            .map(|e| e.value.clone())
    }

    /// One live key's version; `0` for absent or expired, exactly as the store
    /// reports it.
    pub fn kv_version(&self, key: &str) -> i64 {
        let st = self.lock();
        let now = st.now_ms();
        st.kv
            .get(key)
            .filter(|e| e.expires_at_ms.is_none_or(|exp| exp > now))
            .map(|e| e.version)
            .unwrap_or(0)
    }

    /// Seed a key directly, without going through the wire.
    pub fn kv_seed(&self, key: &str, value: Value) {
        let mut st = self.lock();
        let version = st.bump_version();
        st.kv.insert(
            key.to_string(),
            KvEntry {
                value,
                version,
                expires_at_ms: None,
            },
        );
    }

    /// Every live key, sorted — the shape a `getPrefix` walks.
    pub fn kv_keys(&self) -> Vec<String> {
        let st = self.lock();
        let now = st.now_ms();
        st.kv
            .iter()
            .filter(|(_, e)| e.expires_at_ms.is_none_or(|exp| exp > now))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// The bounds a fetch would report for one lane, without making a call.
    pub fn bounds(&self, queue: &str, partition: &str) -> Option<(i64, i64)> {
        let st = self.lock();
        st.queues.get(queue).map(|q| match q.get(partition) {
            Some(lane) => (lane.log_start, lane.next_offset),
            None => (0, 0),
        })
    }

    /// The id of the partition's current incarnation, `None` when it does not
    /// exist.
    pub fn partition_id(&self, queue: &str, partition: &str) -> Option<String> {
        let st = self.lock();
        st.queues
            .get(queue)
            .and_then(|q| q.get(partition))
            .map(|lane| lane.id.clone())
    }
}

/// A uuid-shaped id for the partition created `seq`-th. Unique per incarnation,
/// which is all the sink may assume about an id.
fn fake_partition_id(seq: u64) -> String {
    format!("00000000-0000-4000-8000-{seq:012x}")
}

impl FakeState {
    fn now_ms(&self) -> i64 {
        if let Some(pin) = self.now_ms_pin {
            return pin;
        }
        match self.tokio_clock {
            Some((base, at)) => base + at.elapsed().as_millis() as i64,
            None => crate::obs::now_epoch_ms(),
        }
    }

    fn safe_time(&self) -> Micros {
        match self.safe_time_pin {
            Some(t) => t,
            None => self
                .max_ts
                .map(|t| t.saturating_add(Micros(1)))
                .unwrap_or(Micros(0)),
        }
    }

    fn bump_version(&mut self) -> i64 {
        self.next_version += 1;
        self.next_version
    }

    fn live(&self, key: &str, now: i64) -> Option<&KvEntry> {
        self.kv
            .get(key)
            .filter(|e| e.expires_at_ms.is_none_or(|exp| exp > now))
    }

    fn trim(&mut self, queue: &str, partition: &str, offset: i64) {
        let Some(lane) = self
            .queues
            .get_mut(queue)
            .and_then(|q| q.get_mut(partition))
        else {
            return;
        };
        lane.log_start = lane.log_start.max(offset.min(lane.next_offset));
        let floor = lane.log_start;
        for seg in &mut lane.segments {
            let drop = (floor - seg.base_offset).max(0) as usize;
            if drop >= seg.records.len() {
                seg.records.clear();
            } else if drop > 0 {
                seg.records.drain(0..drop);
                seg.base_offset += drop as i64;
            }
        }
        lane.segments.retain(|s| !s.records.is_empty());
    }

    /// One fetch entry, with the broker's arms (reads.rs `fetch_once`), and
    /// the trim to apply once the call has answered, when the lane was set up
    /// to race one.
    fn fetch_one(
        &self,
        entry: &FetchRequestEntry,
        budget: &mut usize,
    ) -> (FetchedEntry, Option<i64>) {
        let empty = |error: Option<FetchError>, high: i64, log_start: i64| FetchedEntry {
            queue: entry.queue.clone(),
            partition: entry.partition.clone(),
            records: Vec::new(),
            high_watermark: high,
            log_start_offset: log_start,
            error,
        };
        let Some(lanes) = self.queues.get(&entry.queue) else {
            return (empty(Some(FetchError::UnknownTopicOrPartition), 0, 0), None);
        };
        // A partition nobody has written is EMPTY, not missing: bounds 0/0 and
        // no error. It still takes the offset arms below — offset 0 is valid
        // and empty, anything above it is out of range, exactly as it will be
        // after the first push.
        let Some(lane) = lanes.get(&*entry.partition) else {
            if entry.offset != 0 {
                return (empty(Some(FetchError::OffsetOutOfRange), 0, 0), None);
            }
            return (empty(None, 0, 0), None);
        };
        let (log_start, high) = (lane.log_start, lane.next_offset);
        if entry.offset < log_start || entry.offset > high {
            return (
                empty(Some(FetchError::OffsetOutOfRange), high, log_start),
                None,
            );
        }
        let per_entry = match entry.max_bytes {
            Some(mb) => {
                ((mb / FAKE_BYTES_PER_RECORD).max(1) as usize).min(FAKE_MAX_RECORDS_PER_ENTRY)
            }
            None => FAKE_MAX_RECORDS_PER_ENTRY,
        };
        // The racing trim: the bounds above were read before it, the log below
        // after it.
        let readable_from = entry.offset.max(lane.race_trim.unwrap_or(i64::MIN));
        let mut records = Vec::new();
        'segments: for seg in &lane.segments {
            for (i, (txn, payload)) in seg.records.iter().enumerate() {
                let offset = seg.base_offset + i as i64;
                if offset < readable_from {
                    continue;
                }
                if records.len() >= per_entry || *budget == 0 {
                    break 'segments;
                }
                *budget -= 1;
                records.push(Record {
                    partition: entry.partition.clone(),
                    offset,
                    transaction_id: txn.clone(),
                    ts: seg.ts,
                    payload: payload.clone(),
                });
            }
        }
        (
            FetchedEntry {
                queue: entry.queue.clone(),
                partition: entry.partition.clone(),
                records,
                high_watermark: high,
                log_start_offset: log_start,
                error: None,
            },
            lane.race_trim,
        )
    }

    /// One discovery entry, as the broker pages it (reads.rs `changed_page`):
    /// partitions in creation order, `since` keeping those whose `lastWriteAt`
    /// is at or after it, an opaque cursor naming the page's last partition,
    /// and `BAD_CURSOR` for anything the double did not issue.
    fn changed_one(&self, entry: &ChangedRequestEntry) -> ChangedEntry {
        let refuse = |error: &str| ChangedEntry {
            queue: entry.queue.clone(),
            partitions: Vec::new(),
            next: None,
            error: Some(error.to_string()),
        };
        let Some(lanes) = self.queues.get(&entry.queue) else {
            return refuse("UNKNOWN_TOPIC_OR_PARTITION");
        };
        let limit = entry.limit.clamp(1, MAX_CHANGED_LIMIT) as usize;
        let since = entry.since.filter(|s| *s != Micros::MIN);
        let after: Option<u64> = match entry.after.as_deref() {
            None | Some("") => None,
            Some(cursor) => match cursor.strip_prefix("p|").and_then(|s| s.parse().ok()) {
                Some(seq) => Some(seq),
                None => return refuse("BAD_CURSOR"),
            },
        };

        let mut rows: Vec<(&String, &Lane)> = lanes
            .iter()
            .filter(|(_, lane)| after.is_none_or(|a| lane.seq > a))
            .filter(|(_, lane)| {
                since.is_none_or(|t| lane.last_write_at.unwrap_or(Micros::MIN) >= t)
            })
            .collect();
        rows.sort_by_key(|(_, lane)| lane.seq);
        rows.truncate(limit);
        // Issued exactly when the page is full, as the broker does: a full last
        // page costs one more, empty, call.
        let next = match (rows.len() >= limit, rows.last()) {
            (true, Some((_, lane))) => Some(format!("p|{}", lane.seq)),
            _ => None,
        };
        ChangedEntry {
            queue: entry.queue.clone(),
            partitions: rows
                .into_iter()
                .map(|(name, lane)| PartitionBounds {
                    name: name.as_str().into(),
                    id: Some(lane.id.clone()),
                    last_offset: lane.next_offset - 1,
                    log_start: lane.log_start,
                    last_write_at: lane.last_write_at,
                })
                .collect(),
            next,
            error: None,
        }
    }

    /// Apply one KV batch, all-or-nothing when a `required` precondition loses.
    ///
    /// The op ORDER is the broker's, not the caller's: `getMany` and
    /// `getPrefix` are evaluated after every write and every single `get`. A
    /// batch that puts a key and then reads it back by prefix therefore sees
    /// the write — which is worth reproducing, because a caller that relies on
    /// it against the real broker would be right.
    fn apply_kv(&mut self, ops: &[KvOp]) -> Result<Vec<KvResult>> {
        let now = self.now_ms();
        let snapshot = self.kv.clone();
        let mut out: Vec<KvResult> = (0..ops.len()).map(|_| KvResult::default()).collect();
        let mut order: Vec<usize> = (0..ops.len()).collect();
        order.sort_by_key(|i| {
            let phase = match ops[*i] {
                KvOp::GetMany { .. } | KvOp::GetPrefix { .. } => 1,
                _ => 0,
            };
            (phase, *i)
        });

        for i in order {
            let op = &ops[i];
            let mut res = KvResult {
                index: i,
                ..KvResult::default()
            };
            // `putIfAbsent` IS a `put` with `expect: 0` — one code path, one
            // verdict — and the answer is labelled with the name it was asked
            // under. Normalising here is what gives this double the same one
            // code path.
            let as_put: Option<NormalizedPut<'_>> = match op {
                KvOp::Put {
                    key,
                    value,
                    ttl_seconds,
                    expect,
                    required,
                } => Some((key, value, *ttl_seconds, *expect, *required, "put")),
                KvOp::PutIfAbsent {
                    key,
                    value,
                    ttl_seconds,
                    required,
                } => Some((key, value, *ttl_seconds, Some(0), *required, "putIfAbsent")),
                _ => None,
            };
            if let Some((key, value, ttl_seconds, expect, required, label)) = as_put {
                res.op = label.into();
                res.key = Some(key.clone());
                let current = self.live(key, now).cloned();
                let (applied, reason) = match (expect, &current) {
                    (None, _) => (true, None),
                    (Some(0), None) => (true, None),
                    (Some(0), Some(_)) => (false, Some("exists".to_string())),
                    (Some(_), None) => (false, Some("absent".to_string())),
                    (Some(n), Some(e)) if e.version == n => (true, None),
                    (Some(_), Some(_)) => (false, Some("version".to_string())),
                };
                if applied {
                    let version = self.bump_version();
                    let expires_at_ms = ttl_seconds.map(|s| now + (s as i64).saturating_mul(1_000));
                    self.kv.insert(
                        key.clone(),
                        KvEntry {
                            value: value.clone(),
                            version,
                            expires_at_ms,
                        },
                    );
                    res.applied = Some(true);
                    res.version = version;
                    res.value = value.clone();
                } else {
                    res.applied = Some(false);
                    res.reason = reason;
                    // The loser is handed the WINNER's version and value, so it
                    // never needs a second round trip.
                    res.version = current.as_ref().map(|e| e.version).unwrap_or(0);
                    res.value = current.map(|e| e.value).unwrap_or(Value::Null);
                    if required {
                        self.kv = snapshot;
                        return Err(SinkError::Precondition {
                            failed_index: i,
                            reason: res.reason.unwrap_or_default(),
                            version: res.version,
                            value: res.value,
                        });
                    }
                }
                out[i] = res;
                continue;
            }
            match op {
                KvOp::Get { key } => {
                    res.op = "get".into();
                    res.key = Some(key.clone());
                    match self.live(key, now) {
                        Some(e) => {
                            res.found = Some(true);
                            res.value = e.value.clone();
                            res.version = e.version;
                        }
                        None => res.found = Some(false),
                    }
                }
                KvOp::GetMany { keys } => {
                    res.op = "getMany".into();
                    for k in keys {
                        match self.live(k, now) {
                            Some(e) => res.rows.push(KvRow {
                                key: k.clone(),
                                value: e.value.clone(),
                                version: e.version,
                            }),
                            None => res.missing.push(k.clone()),
                        }
                    }
                }
                KvOp::GetPrefix {
                    prefix,
                    limit,
                    after,
                } => {
                    res.op = "getPrefix".into();
                    let limit = (*limit).clamp(1, MAX_KV_PREFIX_LIMIT) as usize;
                    let mut matched: Vec<KvRow> = self
                        .kv
                        .iter()
                        .filter(|(k, e)| {
                            k.starts_with(prefix.as_str())
                                && e.expires_at_ms.is_none_or(|exp| exp > now)
                                && after.as_ref().is_none_or(|a| *k > a)
                        })
                        .map(|(k, e)| KvRow {
                            key: k.clone(),
                            value: e.value.clone(),
                            version: e.version,
                        })
                        .collect();
                    matched.sort_by(|a, b| a.key.cmp(&b.key));
                    res.truncated = matched.len() > limit;
                    matched.truncate(limit);
                    res.next_after = match res.truncated {
                        true => matched.last().map(|r| r.key.clone()),
                        false => None,
                    };
                    res.rows = matched;
                }
                // Both are handled above, as one desugared code path; an arm
                // here only keeps the match total.
                KvOp::Put { .. } | KvOp::PutIfAbsent { .. } => {}
                KvOp::Delete {
                    key,
                    expect,
                    required,
                } => {
                    res.op = "delete".into();
                    res.key = Some(key.clone());
                    let current = self.live(key, now).cloned();
                    let (applied, reason) = match (expect, &current) {
                        (None, _) => (true, None),
                        (Some(0), None) => (true, None),
                        (Some(0), Some(_)) => (false, Some("exists".to_string())),
                        (Some(_), None) => (false, Some("absent".to_string())),
                        (Some(n), Some(e)) if e.version == *n => (true, None),
                        (Some(_), Some(_)) => (false, Some("version".to_string())),
                    };
                    if applied {
                        self.kv.remove(key);
                        res.applied = Some(true);
                        res.version = 0;
                    } else {
                        res.applied = Some(false);
                        res.reason = reason;
                        res.version = current.as_ref().map(|e| e.version).unwrap_or(0);
                        res.value = current.map(|e| e.value).unwrap_or(Value::Null);
                        if *required {
                            self.kv = snapshot;
                            return Err(SinkError::Precondition {
                                failed_index: i,
                                reason: res.reason.unwrap_or_default(),
                                version: res.version,
                                value: res.value,
                            });
                        }
                    }
                }
            }
            out[i] = res;
        }
        Ok(out)
    }
}

impl QueenApi for FakeQueen {
    fn fetch(
        &self,
        entries: Vec<FetchRequestEntry>,
        _max_wait_ms: u64,
        _min_bytes: i64,
    ) -> BoxFuture<'_, Result<Vec<FetchedEntry>>> {
        Box::pin(async move {
            let mut st = self.lock();
            st.fetch_calls += 1;
            if st.fail_next > 0 {
                st.fail_next -= 1;
                return Err(SinkError::Transport("injected fetch failure".into()));
            }
            if entries.len() > MAX_FETCH_ENTRIES {
                return Err(SinkError::Config(format!(
                    "a fetch of {} entries exceeds the broker's {MAX_FETCH_ENTRIES}",
                    entries.len()
                )));
            }
            let mut budget = st.records_per_call;
            let mut trims: Vec<(String, String, i64)> = Vec::new();
            let out: Vec<FetchedEntry> = entries
                .iter()
                .map(|e| {
                    let (answer, trim) = st.fetch_one(e, &mut budget);
                    if let Some(below) = trim {
                        trims.push((e.queue.clone(), e.partition.to_string(), below));
                    }
                    answer
                })
                .collect();
            for (queue, partition, below) in trims {
                st.trim(&queue, &partition, below);
                if let Some(lane) = st
                    .queues
                    .get_mut(&queue)
                    .and_then(|q| q.get_mut(&partition))
                {
                    lane.race_trim = None;
                }
            }
            Ok(out)
        })
    }

    fn partitions_changed(
        &self,
        entries: Vec<ChangedRequestEntry>,
    ) -> BoxFuture<'_, Result<ChangedResponse>> {
        Box::pin(async move {
            let mut st = self.lock();
            st.changed_calls += 1;
            if st.fail_next > 0 {
                st.fail_next -= 1;
                return Err(SinkError::Transport("injected discovery failure".into()));
            }
            if entries.len() > MAX_CHANGED_ENTRIES {
                return Err(SinkError::Config(format!(
                    "a discovery call of {} queues exceeds the sink's {MAX_CHANGED_ENTRIES}",
                    entries.len()
                )));
            }
            Ok(ChangedResponse {
                safe_time: st.safe_time(),
                safe_time_degraded: st.safe_time_degraded,
                entries: entries.iter().map(|e| st.changed_one(e)).collect(),
            })
        })
    }

    fn kv(&self, ops: Vec<KvOp>) -> BoxFuture<'_, Result<Vec<KvResult>>> {
        Box::pin(async move {
            let latency = self.lock().kv_latency;
            if !latency.is_zero() {
                tokio::time::sleep(latency).await;
            }
            let mut st = self.lock();
            st.kv_calls += 1;
            st.kv_batches.push(ops.clone());
            if st.fail_kv_next > 0 {
                st.fail_kv_next -= 1;
                return Err(SinkError::Transport("injected kv failure".into()));
            }
            st.apply_kv(&ops)
        })
    }

    fn list_queues(&self) -> BoxFuture<'_, Result<Vec<String>>> {
        Box::pin(async move {
            let mut st = self.lock();
            if st.fail_next > 0 {
                st.fail_next -= 1;
                return Err(SinkError::Transport("injected list failure".into()));
            }
            Ok(st.queues.keys().cloned().collect())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_forever_write_says_forever_and_a_ttl_write_says_only_ttl() {
        let forever = KvOp::put("s3:default:orders:committed", serde_json::json!({"k": 1}));
        let j = forever.to_json();
        assert_eq!(j["ns"], KV_NAMESPACE);
        assert_eq!(j["op"], "put");
        assert_eq!(j["forever"], true);
        assert!(j.get("ttlSeconds").is_none(), "{j}");
        assert!(j.get("expect").is_none(), "{j}");
        assert!(j.get("required").is_none(), "{j}");

        let ttl = KvOp::put_ttl("s3:default:orders:lease", Value::Null, 30);
        let j = ttl.to_json();
        assert_eq!(j["ttlSeconds"], 30);
        assert!(
            j.get("forever").is_none(),
            "a TTL write must not declare both: {j}"
        );
    }

    #[test]
    fn the_fence_carries_expect_and_required() {
        let j = KvOp::fence("s3:default:orders:lease", Value::Null, 7).to_json();
        assert_eq!(j["expect"], 7);
        assert_eq!(j["required"], true);
        assert_eq!(j["forever"], true);
        assert!(KvOp::fence("k", Value::Null, 7).is_required());
        assert!(!KvOp::put("k", Value::Null).is_required());
    }

    #[test]
    fn put_if_absent_goes_out_under_its_own_name() {
        let j = KvOp::put_if_absent_ttl("s3:default:orders:lease", Value::Null, 30).to_json();
        assert_eq!(j["op"], "putIfAbsent");
        assert_eq!(j["ttlSeconds"], 30);
        // `expect` is NOT sent: the broker refuses a putIfAbsent whose expect is
        // anything but 0, and it supplies the 0 itself.
        assert!(j.get("expect").is_none(), "{j}");
    }

    #[test]
    fn get_prefix_carries_its_cursor_only_when_it_has_one() {
        let j = KvOp::get_prefix("s3:default:", 100, None).to_json();
        assert_eq!(j["limit"], 100);
        assert!(j.get("after").is_none(), "{j}");
        let j = KvOp::get_prefix("s3:default:", 100, Some("s3:default:a".into())).to_json();
        assert_eq!(j["after"], "s3:default:a");
    }

    #[test]
    fn the_kv_body_is_an_operations_object() {
        let body = kv_body(&[KvOp::get("a"), KvOp::delete("b", Some(3))]);
        assert_eq!(body["operations"][0]["op"], "get");
        assert_eq!(body["operations"][1]["op"], "delete");
        assert_eq!(body["operations"][1]["expect"], 3);
    }

    #[test]
    fn the_fetch_body_omits_max_bytes_when_there_is_none() {
        let entries = vec![
            FetchRequestEntry {
                queue: "orders".into(),
                partition: "cust-1".into(),
                offset: 7,
                max_bytes: None,
            },
            FetchRequestEntry {
                queue: "orders".into(),
                partition: "cust-2".into(),
                offset: 0,
                max_bytes: Some(4096),
            },
        ];
        let body = fetch_body(&entries, 500, 1);
        assert_eq!(body["maxWaitMs"], 500);
        assert_eq!(body["minBytes"], 1);
        assert_eq!(body["entries"][0]["partition"], "cust-1");
        assert_eq!(body["entries"][0]["offset"], 7);
        assert!(body["entries"][0].get("maxBytes").is_none());
        assert_eq!(body["entries"][1]["maxBytes"], 4096);
    }

    #[test]
    fn the_changed_body_renders_since_as_iso_or_null() {
        let entries = vec![
            ChangedRequestEntry {
                queue: "orders".into(),
                since: None,
                after: None,
                limit: 1000,
            },
            ChangedRequestEntry {
                queue: "clicks".into(),
                since: Some(Micros::parse_iso("2026-09-04T10:00:00Z").unwrap()),
                after: Some("t|1|x".into()),
                limit: 5_000,
            },
            ChangedRequestEntry {
                queue: "backfill".into(),
                since: Some(Micros::MIN),
                after: None,
                limit: 0,
            },
        ];
        let body = changed_body(&entries);
        assert_eq!(body["entries"][0]["since"], Value::Null);
        assert_eq!(body["entries"][0]["after"], Value::Null);
        assert_eq!(body["entries"][1]["since"], "2026-09-04T10:00:00.000000Z");
        assert_eq!(body["entries"][1]["after"], "t|1|x");
        assert_eq!(body["entries"][1]["limit"], 1000, "clamped to the ceiling");
        assert_eq!(
            body["entries"][2]["since"],
            Value::Null,
            "-inf IS enumeration"
        );
        assert_eq!(body["entries"][2]["limit"], 1, "clamped to the floor");
    }

    #[test]
    fn a_lost_precondition_is_a_two_hundred() {
        let body = r#"{"ok":false,"reason":"kv_precondition","failedIndex":0,
                       "kvReason":"version","version":42,"value":{"instance":"b"}}"#;
        match decode_kv(body, 2).unwrap_err() {
            SinkError::Precondition {
                failed_index,
                reason,
                version,
                value,
            } => {
                assert_eq!(failed_index, 0);
                assert_eq!(reason, "version");
                assert_eq!(version, 42);
                assert_eq!(value["instance"], "b");
            }
            other => panic!("expected a precondition, got {other:?}"),
        }
    }

    #[test]
    fn an_unknown_ok_false_reason_is_named_rather_than_guessed_at() {
        let err = decode_kv(r#"{"ok":false,"reason":"something_new"}"#, 1).unwrap_err();
        assert!(
            matches!(err, SinkError::Body(ref s) if s.contains("something_new")),
            "{err:?}"
        );
    }

    /// What the KV route answers, through the parser the broker's adapter
    /// hands the crate: the shapes are handlers/kv.rs's own.
    #[test]
    fn a_kv_route_answer_becomes_results_or_the_error_the_driver_acts_on() {
        // Success: one result per operation, by index.
        let ok = r#"{"results":[{"index":0,"op":"put","applied":true,"key":"s3:default:orders:lease",
                                 "value":{"instance":"node-1"},"version":11},
                                {"index":1,"op":"put","applied":true,"key":"s3:default:orders:committed",
                                 "value":{"k":42},"version":12}]}"#;
        let out = parse_kv_answer(200, ok, 2).unwrap();
        assert!(out[0].did_apply());
        assert_eq!(out[1].version, 12);

        // A lost `required` precondition: HTTP 200, and an error that is never
        // retried blindly.
        let lost = r#"{"ok":false,"reason":"kv_precondition","failedIndex":0,
                       "kvReason":"version","version":99,"value":{"instance":"node-2"}}"#;
        let err = parse_kv_answer(200, lost, 2).unwrap_err();
        assert!(!err.is_retriable());
        match err {
            SinkError::Precondition {
                failed_index,
                reason,
                version,
                value,
            } => {
                assert_eq!((failed_index, reason.as_str(), version), (0, "version", 99));
                assert_eq!(value["instance"], "node-2");
            }
            other => panic!("expected a precondition, got {other:?}"),
        }

        // The bare verdict, when the broker could not render the detail: the
        // index reads as the fence's, and it is still a precondition.
        let bare = r#"{"ok":false,"reason":"kv_precondition","failedIndex":null,
                       "kvReason":null,"version":null,"value":null}"#;
        assert!(matches!(
            parse_kv_answer(200, bare, 2).unwrap_err(),
            SinkError::Precondition {
                failed_index: 0,
                version: 0,
                ..
            }
        ));

        // A refusal and an outage: a status error, retried only when the
        // status says the same call can work later.
        let refused = parse_kv_answer(
            400,
            r#"{"error":"kv_bad_request","reason":"kv_bad_expect"}"#,
            1,
        )
        .unwrap_err();
        assert!(matches!(refused, SinkError::Status { code: 400, .. }));
        assert!(!refused.is_retriable());
        let busy = parse_kv_answer(503, r#"{"error":"kv_unavailable"}"#, 1).unwrap_err();
        assert!(busy.is_retriable());
        let throttled = parse_kv_answer(429, "{}", 1).unwrap_err();
        assert!(throttled.is_retriable());

        // A 200 that does not answer every operation is not a success.
        assert!(matches!(
            parse_kv_answer(200, r#"{"results":[{"index":0,"applied":true}]}"#, 2).unwrap_err(),
            SinkError::Body(_)
        ));
        assert!(matches!(
            parse_kv_answer(200, "not json", 1).unwrap_err(),
            SinkError::Body(_)
        ));
        // And an empty batch needs no answer at all.
        assert!(parse_kv_answer(500, "", 0).unwrap().is_empty());
    }

    #[test]
    fn a_read_answer_decodes_with_its_rows_and_its_absences() {
        let body = r#"{"results":[{"index":0,"op":"getMany","rows":[
            {"key":"s3:default:orders:committed","value":{"k":3},"version":7,
             "expiresAt":null,"updatedAt":"2026-09-04T10:00:00.000000Z"}],
            "missing":["s3:default:orders:intent"],"truncated":false}]}"#;
        let out = parse_kv_answer(200, body, 1).unwrap();
        assert_eq!(out[0].rows.len(), 1);
        assert_eq!(out[0].rows[0].version, 7);
        assert_eq!(out[0].missing, vec!["s3:default:orders:intent"]);
        assert!(!out[0].truncated);
    }

    #[test]
    fn kv_results_are_aligned_by_their_own_index() {
        let body = r#"{"results":[{"index":1,"op":"put","applied":true,"version":9},
                                  {"index":0,"op":"get","found":false}]}"#;
        let out = decode_kv(body, 2).unwrap();
        assert_eq!(out[0].op, "get");
        assert_eq!(out[0].found, Some(false));
        assert_eq!(out[1].op, "put");
        assert!(out[1].did_apply());
        assert_eq!(out[1].version, 9);
        // A gap is a refusal, not a silent None.
        assert!(decode_kv(r#"{"results":[{"index":0}]}"#, 2).is_err());
        assert!(decode_kv(r#"{"results":[{"index":5}]}"#, 2).is_err());
    }

    #[test]
    fn a_fetch_answer_for_another_lane_is_refused() {
        let asked = vec![FetchRequestEntry {
            queue: "orders".into(),
            partition: "a".into(),
            offset: 0,
            max_bytes: None,
        }];
        let wrong = r#"{"entries":[{"queue":"orders","partition":"b","records":[],
                        "highWatermark":0,"logStartOffset":0}]}"#;
        assert!(decode_fetch(wrong, &asked).is_err());
        let short = r#"{"entries":[]}"#;
        assert!(decode_fetch(short, &asked).is_err());
    }

    #[test]
    fn a_fetched_record_keeps_its_payload_as_text_and_null_becomes_none() {
        let asked = vec![FetchRequestEntry {
            queue: "orders".into(),
            partition: "a".into(),
            offset: 0,
            max_bytes: None,
        }];
        let body = r#"{"entries":[{"queue":"orders","partition":"a","records":[
            {"offset":0,"transactionId":"t0","payload":{"amount":1290},"ts":"2026-09-04T10:03:41.918204Z"},
            {"offset":1,"transactionId":"t1","payload":null,"ts":"2026-09-04T10:03:41.918204Z"}],
            "highWatermark":2,"logStartOffset":0}]}"#;
        let out = decode_fetch(body, &asked).unwrap();
        assert_eq!(out[0].records.len(), 2);
        assert_eq!(
            out[0].records[0].payload.as_ref().unwrap().get(),
            "{\"amount\":1290}"
        );
        assert!(out[0].records[1].payload.is_none());
        assert_eq!(
            out[0].records[0].ts,
            Micros::parse_iso("2026-09-04T10:03:41.918204Z").unwrap()
        );
        assert_eq!(out[0].high_watermark, 2);
    }

    #[test]
    fn per_entry_errors_are_decoded_as_the_markers_they_are() {
        let asked = vec![FetchRequestEntry {
            queue: "orders".into(),
            partition: "a".into(),
            offset: 0,
            max_bytes: None,
        }];
        let body = r#"{"entries":[{"queue":"orders","partition":"a","records":[],
            "highWatermark":9,"logStartOffset":4,"error":"OFFSET_OUT_OF_RANGE"}]}"#;
        let out = decode_fetch(body, &asked).unwrap();
        assert_eq!(out[0].error, Some(FetchError::OffsetOutOfRange));
        assert_eq!(out[0].log_start_offset, 4);
    }

    #[test]
    fn a_discovery_answer_decodes_its_bounds_and_its_cursor() {
        let asked = vec![ChangedRequestEntry {
            queue: "orders".into(),
            since: None,
            after: None,
            limit: 1000,
        }];
        // The broker's own rendering (reads.rs `api_partitions_changed`).
        let body = r#"{"safeTime":"2026-09-04T10:04:57.412331Z","safeTimeDegraded":false,
            "entries":[{"queue":"orders","partitions":[
                {"name":"cust-0420","id":"6f1c2d3e-4b5a-4c6d-8e7f-001122334455",
                 "lastOffset":1811,"logStart":1400,
                 "lastWriteAt":"2026-09-04T10:04:00.123456Z"},
                {"name":"cust-0007","lastOffset":-1,"logStart":0,
                 "lastWriteAt":"2026-09-04T10:04:01.000000Z"}],"next":"p|8812"}]}"#;
        let out = decode_changed(body, &asked).unwrap();
        assert_eq!(
            out.safe_time,
            Micros::parse_iso("2026-09-04T10:04:57.412331Z").unwrap()
        );
        assert!(!out.safe_time_degraded);
        let p = &out.entries[0].partitions[0];
        assert_eq!(&*p.name, "cust-0420");
        assert_eq!(
            p.id.as_deref(),
            Some("6f1c2d3e-4b5a-4c6d-8e7f-001122334455")
        );
        assert_eq!(p.last_offset, 1811);
        assert_eq!(p.log_start, 1400);
        assert_eq!(
            p.last_write_at,
            Some(Micros::parse_iso("2026-09-04T10:04:00.123456Z").unwrap()),
            "lastWriteAt is exact to the microsecond"
        );
        assert_eq!(
            out.entries[0].partitions[1].id, None,
            "an entry without an id is an unknown incarnation, not an error"
        );
        assert_eq!(out.entries[0].next.as_deref(), Some("p|8812"));
        assert!(out.entries[0].error.is_none());
    }

    #[test]
    fn an_unknown_queue_entry_carries_no_partitions() {
        let asked = vec![ChangedRequestEntry {
            queue: "nope".into(),
            since: None,
            after: None,
            limit: 10,
        }];
        let body = r#"{"safeTime":"2026-09-04T10:00:00Z","entries":[
            {"queue":"nope","error":"UNKNOWN_TOPIC_OR_PARTITION"}]}"#;
        let out = decode_changed(body, &asked).unwrap();
        assert_eq!(
            out.entries[0].error.as_deref(),
            Some("UNKNOWN_TOPIC_OR_PARTITION")
        );
        assert!(out.entries[0].partitions.is_empty());
    }

    #[test]
    fn a_safe_time_that_is_not_a_broker_timestamp_is_a_body_error() {
        let asked = vec![];
        assert!(decode_changed(r#"{"safeTime":"soon","entries":[]}"#, &asked).is_err());
    }
}
