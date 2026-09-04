//! The Queen side of the sink: an HTTP client for the four routes it needs, and
//! a faithful in-memory double for the tests of every module above it.
//!
//! `queen-s3` is a CLIENT of the broker (plan §3), so the obvious move is to
//! depend on `clients/client-rust`. It does not fit, for the same structural
//! reason the Kafka facade gave and one more of its own: that client has no
//! `/api/v1/fetch` (F2 — no SDK does), and the sink's whole read path is fetch.
//! What is left after adding fetch is a `reqwest` client with a bearer, which is
//! this file.
//!
//! Routes used, and their shapes, read off the broker rather than guessed:
//!
//!   * `POST /api/v1/fetch` `{"entries":[{"queue","partition","offset",
//!     "maxBytes"},…],"maxWaitMs","minBytes"}` → one entry per request entry, in
//!     request order, carrying `records`, `highWatermark`, `logStartOffset` and
//!     an optional per-entry `error` (server/src/handlers/fetch.rs,
//!     crates/queen-protocol/src/fetch.rs). A record renders as
//!     `{offset, transactionId, payload, ts}` and nothing else — no headers
//!     (fetch.rs:610-613) — and `ts` is the SEGMENT's `created_at` at microsecond
//!     precision (032_log_fetch.sql:250-251).
//!   * `POST /api/v1/partitions/changed` — the discovery endpoint of plan §5.1
//!     (server/sql/procedures/033_log_partitions_changed.sql). It answers a
//!     `safeTime` for the whole call and, per queue, the partitions that moved,
//!     paged through an OPAQUE cursor.
//!   * `POST /api/v1/kv` `{"operations":[…]}` → `{"results":[…]}`, one result per
//!     operation, each stamped with its own `index` (server/src/handlers/kv.rs,
//!     server/sql/procedures/024_kv.sql). The two commit-pointer documents and
//!     the queue lease live here.
//!   * `GET /api/v1/resources/queues` → `{"queues":[{"name":…},…]}`, read for
//!     the NAMES only, which is all `QUEEN_S3_QUEUES=*` needs.
//!
//! ONE RULE THAT IS NOT OBVIOUS FROM THE TYPES: a lost KV precondition arrives
//! as HTTP **200** with `{"ok":false,"reason":"kv_precondition",…}`, never as a
//! status code, and deliberately so (kv.rs:317-353: it is the EXPECTED outcome
//! of every legitimate redelivery, and "it must pollute neither the error
//! metrics nor the retry policies"). It is mapped to
//! [`SinkError::Precondition`], the one error in this crate that must never be
//! retried blindly: for the sink it means another instance owns the queue.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::Value;

use crate::types::{
    ChangedEntry, ChangedRequestEntry, ChangedResponse, FetchError, FetchRequestEntry,
    FetchedEntry, Micros, PartitionBounds, Record, SinkError,
};

/// A boxed future, so [`QueenApi`] stays dyn-compatible. `async fn` in a trait
/// is not: it desugars to an opaque associated type no trait object can name,
/// and the driver holds `Arc<dyn QueenApi>` precisely so the tests can hand it
/// [`FakeQueen`] instead of a socket.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub type Result<T> = std::result::Result<T, SinkError>;

/// The KV namespace, for every key this connector writes. A constant and not a
/// field: the namespace is validated by the stored procedure against
/// `^[a-z0-9][a-z0-9._-]{0,63}$` (024_kv.sql), it is never a place to put
/// anything a client chose, and one process writing two namespaces would be two
/// commit stores for one sink.
pub const KV_NAMESPACE: &str = "queen-s3";

/// Entries per `POST /api/v1/fetch`. The broker clamps at 1024 and fails the
/// batch above it (fetch.rs:78-122), so the ceiling is checked here rather than
/// discovered as a 400 on a call that already cost a round trip.
pub const MAX_FETCH_ENTRIES: usize = 1024;
/// The broker's own park ceiling (fetch.rs).
pub const MAX_FETCH_WAIT_MS: u64 = 30_000;
/// Queues per `POST /api/v1/partitions/changed` (plan §5.1).
pub const MAX_CHANGED_ENTRIES: usize = 64;
/// Partitions per entry per call, clamped rather than refused — the broker
/// clamps identically (033_log_partitions_changed.sql).
pub const MAX_CHANGED_LIMIT: u32 = 1_000;
/// Rows per `getPrefix` page (024_kv.sql `C_PREFIX_MAX`).
pub const MAX_KV_PREFIX_LIMIT: i64 = 1_000;

// ---------------------------------------------------------------------------
// KV operations
// ---------------------------------------------------------------------------

/// One key/value operation, in the shape `kv_apply_v1` takes.
///
/// `ns` is NOT a field: it is [`KV_NAMESPACE`] on every operation this crate
/// builds, and [`KvOp::to_json`] writes it. The conditional half — `expect` and
/// `required` — is the whole of plan §6.6's fence, and it is the broker's own
/// (024_kv.sql:830-853, :1498-1502): nothing on the server changes for it.
#[derive(Debug, Clone, PartialEq)]
pub enum KvOp {
    /// Read one key. Answers `found` separately from `value`, because
    /// `'null'::jsonb` is a legal stored value and `{found:true,value:null}` is
    /// not `{found:false}`.
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
    /// on the wire: the stored procedure demands EXACTLY ONE of `ttlSeconds`
    /// and `forever` on every write (024_kv.sql:565-575), so that nothing lands
    /// in the store without somebody having decided when it leaves. The commit
    /// pointer's answer is "never" — an expired pointer is a silent full replay
    /// (plan §12) — and the lease's answer is `QUEEN_S3_LEASE_TTL_MS`, because
    /// that row IS the liveness claim.
    ///
    /// `expect: Some(0)` is "must not exist"; `expect: Some(n>0)` is a PURE
    /// UPDATE that creates nothing when the key is absent (024:1053-1090).
    Put {
        key: String,
        value: Value,
        ttl_seconds: Option<u64>,
        expect: Option<i64>,
        /// Turn a lost precondition from a verdict into an abort of the WHOLE
        /// batch (024:1498-1502) — the fence.
        required: bool,
    },
    /// `putIfAbsent`. The stored procedure desugars it to `put` with `expect:0`
    /// at the entry of its loop, and it wins against an expired-but-unpruned
    /// row (024:1010-1015) — which is what lets an instance that restarts
    /// inside the sweeper's lag reclaim its own lease rather than lose to its
    /// own corpse.
    PutIfAbsent {
        key: String,
        value: Value,
        ttl_seconds: Option<u64>,
        required: bool,
    },
    /// Remove a key. `expect: Some(n)` is a fenced delete (024:1092-1120).
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
    /// here is their row" (024:1467-1471).
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

    /// The operation exactly as it goes on the wire.
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

/// EXACTLY ONE of `ttlSeconds` and `forever`, never both and never neither
/// (024_kv.sql:565-575). `"forever": false` is zero expiry declarations to the
/// stored procedure, not one, so a TTL write must not carry the flag at all.
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

/// One row of a read.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
pub struct KvRow {
    #[serde(default)]
    pub key: String,
    /// `'null'::jsonb` is a legal stored value, so this is `Value::Null` both
    /// for a key holding null and for an answer that carried none.
    #[serde(default)]
    pub value: Value,
    /// Opaque, unique, from a sequence — never `version + 1`, so there is no
    /// ABA (024:133-140). Compared for EQUALITY only, never ordered and never
    /// arithmetic. `0` is "not there", which is also how an expired row reads.
    #[serde(default)]
    pub version: i64,
}

/// What one operation answered.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct KvResult {
    #[serde(default)]
    pub index: usize,
    /// As the stored procedure LABELLED it, which is authoritative over what
    /// was asked: it is where `putIfAbsent` becomes `put`.
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
    #[serde(default)]
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

/// The calls the sink makes to Queen.
///
/// Every method takes OWNED arguments, which is not the facade's shape and is
/// deliberate: the driver builds a fetch batch per window from a buffer it then
/// drops, and a borrowed slice would tie the future's lifetime to a value the
/// caller wants to reuse while the call is in flight.
pub trait QueenApi: Send + Sync {
    /// `POST /api/v1/fetch` — one read per entry, answered one result per
    /// entry, in request order and CHECKED against it.
    fn fetch(
        &self,
        entries: Vec<FetchRequestEntry>,
        max_wait_ms: u64,
        min_bytes: i64,
    ) -> BoxFuture<'_, Result<Vec<FetchedEntry>>>;

    /// `POST /api/v1/partitions/changed` — the discovery sweep of plan §5.1,
    /// plus the `safeTime` every window close is bounded by.
    fn partitions_changed(
        &self,
        entries: Vec<ChangedRequestEntry>,
    ) -> BoxFuture<'_, Result<ChangedResponse>>;

    /// `POST /api/v1/kv` — one answer per operation, aligned by the `index`
    /// each answer carries.
    fn kv(&self, ops: Vec<KvOp>) -> BoxFuture<'_, Result<Vec<KvResult>>>;

    /// `GET /api/v1/resources/queues`, NAMES only, in the list's own order —
    /// what `QUEEN_S3_QUEUES=*` resolves through.
    fn list_queues(&self) -> BoxFuture<'_, Result<Vec<String>>>;
}

// ---------------------------------------------------------------------------
// The real client
// ---------------------------------------------------------------------------

/// Total budget for one non-fetch call.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The HTTP budget for one fetch, which is the only call that does not take the
/// client's default.
///
/// A fetch is a LONG POLL: the broker is EXPECTED to hold the request open for
/// up to `maxWaitMs` before answering. Under the client-wide timeout a sink
/// asking for a 30 second park would have its request cancelled and be told the
/// transport failed — an error where the correct answer was "nothing yet", on
/// every poll, for ever.
fn fetch_timeout(max_wait_ms: u64) -> Duration {
    let park = Duration::from_millis(max_wait_ms.min(MAX_FETCH_WAIT_MS));
    std::cmp::max(REQUEST_TIMEOUT, park + Duration::from_secs(10))
}

/// The real client. One `reqwest::Client` for the process: it owns the
/// connection pool, and every queue task shares it.
pub struct HttpQueen {
    /// `QUEEN_URL`, without a trailing slash (see [`normalize_base_url`]).
    base: String,
    /// The bearer, bound once. Unlike the Kafka facade this process acts as ONE
    /// credential for its whole life — there is no per-connection identity to
    /// forward — so the token belongs to the client.
    token: Option<String>,
    /// The `Host` header every call sends, when it is not the one the URL
    /// implies: the proxy routes on the first DNS label of `Host`, so a sink
    /// behind a shared ingress has to say which cell it means.
    host: Option<String>,
    http: reqwest::Client,
}

impl HttpQueen {
    pub fn new(base_url: &str, token: Option<String>) -> std::result::Result<HttpQueen, String> {
        let base = normalize_base_url(base_url)?;
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|e| format!("cannot build the HTTP client for QUEEN_URL={base}: {e}"))?;
        Ok(HttpQueen {
            base,
            token,
            host: None,
            http,
        })
    }

    /// Build the client this configuration describes.
    pub fn from_config(cfg: &crate::config::Config) -> std::result::Result<HttpQueen, String> {
        HttpQueen::new(&cfg.queen_url, cfg.queen_token.clone())
    }

    /// The same Queen, reached with `host` as the HTTP `Host` header of every
    /// call. Clones the `reqwest::Client`, which shares the connection pool,
    /// the DNS cache and the TLS session cache with the original.
    pub fn with_host(&self, host: &str) -> HttpQueen {
        HttpQueen {
            base: self.base.clone(),
            token: self.token.clone(),
            host: Some(host.to_string()),
            http: self.http.clone(),
        }
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let req = self
            .http
            .request(method, format!("{}{path}", self.base))
            .header(reqwest::header::CONTENT_TYPE, "application/json");
        // Set explicitly, which is what stops hyper filling it in from the
        // URL's authority — it only adds a `Host` that is not already there.
        let req = match &self.host {
            Some(h) => req.header(reqwest::header::HOST, h.as_str()),
            None => req,
        };
        match &self.token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }

    async fn send(op: &'static str, req: reqwest::RequestBuilder) -> Result<String> {
        let started = std::time::Instant::now();
        let resp = req
            .send()
            .await
            .map_err(|e| SinkError::Transport(e.to_string()))?;
        let status = resp.status();
        let retry_after_ms = retry_after_ms(resp.headers());
        let body = resp
            .text()
            .await
            .map_err(|e| SinkError::Transport(e.to_string()))?;
        tracing::debug!(
            target: "queen-s3",
            op,
            status = status.as_u16(),
            bytes = body.len(),
            ms = started.elapsed().as_millis() as u64,
            "queen call"
        );
        if !status.is_success() {
            return Err(SinkError::Status {
                code: status.as_u16(),
                body,
                retry_after_ms,
            });
        }
        Ok(body)
    }
}

/// The `Retry-After` of one answer, in milliseconds.
///
/// Only the `delta-seconds` form is read, because that is the only form the
/// proxy writes (proxy/src/errors.rs `err_429` formats an integer). RFC 9110
/// also allows an HTTP-date, and a date is deliberately answered `None` rather
/// than parsed: this value becomes a SLEEP, and a misread date is a sink parked
/// for hours.
fn retry_after_ms(headers: &reqwest::header::HeaderMap) -> Option<i64> {
    let seconds: i64 = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    seconds.checked_mul(1_000).filter(|ms| *ms >= 0)
}

/// `QUEEN_URL` without its trailing slash, checked for a scheme this client
/// speaks. Copied from protocols/queen-kafka/src/queen.rs so the two binaries
/// refuse the same strings with the same words.
pub fn normalize_base_url(raw: &str) -> std::result::Result<String, String> {
    let trimmed = raw.trim().trim_end_matches('/');
    let url: reqwest::Url = trimmed
        .parse()
        .map_err(|e| format!("QUEEN_URL={raw} is not a URL: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!(
            "QUEEN_URL={raw} has scheme `{}` — the sink speaks HTTP to Queen, so it must be \
             http:// or https://",
            url.scheme()
        ));
    }
    if url.host_str().is_none() {
        return Err(format!("QUEEN_URL={raw} has no host"));
    }
    Ok(trimmed.to_string())
}

// ---- wire bodies ----------------------------------------------------------

#[derive(Deserialize)]
struct QueueListBody {
    #[serde(default)]
    queues: Vec<QueueName>,
}

/// Only `name` is read. The broker adds to this response for the dashboard
/// (retainedBytes, messages, per-queue counters) and none of that may turn into
/// a parse failure here.
#[derive(Deserialize)]
struct QueueName {
    #[serde(default)]
    name: String,
}

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
    #[serde(rename = "lastOffset", default)]
    last_offset: i64,
    #[serde(rename = "logStart", default)]
    log_start: i64,
    #[serde(rename = "lastWriteAt", default)]
    last_write_at: Option<String>,
}

/// The KV envelope. `ok:false` is the lost-precondition verdict, and it arrives
/// with HTTP 200 (kv.rs:338-353).
#[derive(Deserialize)]
struct KvResponseBody {
    #[serde(default = "yes")]
    ok: bool,
    #[serde(default)]
    reason: String,
    #[serde(rename = "failedIndex", default)]
    failed_index: usize,
    #[serde(rename = "kvReason", default)]
    kv_reason: Option<String>,
    #[serde(default)]
    version: i64,
    #[serde(default)]
    value: Value,
    #[serde(default)]
    results: Vec<KvResult>,
}

fn yes() -> bool {
    true
}

// ---- request builders (public, so a test can assert the exact body) --------

/// The body `POST /api/v1/fetch` is sent.
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

/// The body `POST /api/v1/partitions/changed` is sent.
///
/// `since` is rendered as the broker's own ISO-micros form, or `null` for a
/// full enumeration by name. [`Micros::MIN`] is `null` too: `-∞` IS "everything
/// there is", which is the enumeration mode, and rendering it would put the
/// string `-inf` on a wire that parses timestamps.
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

/// The body `POST /api/v1/kv` is sent. `{"operations":[…]}` and not the bare
/// array the route also accepts: it is the shape the transaction wire uses, so
/// one shape is learned once.
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

/// Match a fetch response back to the entries that asked for it.
///
/// The broker answers in request order and the SQL fails the whole call rather
/// than return a misaligned array, so this is a second belt on braced trousers —
/// and it is worth wearing: an entry read as another partition's would put one
/// lane's records under another lane's name in the lake, for ever.
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
                // both mean the stored payload was empty (fetch.rs:620).
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

/// Decode a KV answer, mapping the 200-with-`ok:false` verdict onto
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

impl QueenApi for HttpQueen {
    fn fetch(
        &self,
        entries: Vec<FetchRequestEntry>,
        max_wait_ms: u64,
        min_bytes: i64,
    ) -> BoxFuture<'_, Result<Vec<FetchedEntry>>> {
        Box::pin(async move {
            if entries.is_empty() {
                return Ok(Vec::new());
            }
            if entries.len() > MAX_FETCH_ENTRIES {
                return Err(SinkError::Config(format!(
                    "a fetch of {} entries exceeds the broker's {MAX_FETCH_ENTRIES}",
                    entries.len()
                )));
            }
            let payload = fetch_body(&entries, max_wait_ms, min_bytes).to_string();
            let body = Self::send(
                "fetch",
                self.request(reqwest::Method::POST, "/api/v1/fetch")
                    .body(payload)
                    // The ONE call that overrides the client's budget.
                    .timeout(fetch_timeout(max_wait_ms)),
            )
            .await?;
            decode_fetch(&body, &entries)
        })
    }

    fn partitions_changed(
        &self,
        entries: Vec<ChangedRequestEntry>,
    ) -> BoxFuture<'_, Result<ChangedResponse>> {
        Box::pin(async move {
            if entries.len() > MAX_CHANGED_ENTRIES {
                return Err(SinkError::Config(format!(
                    "a discovery call of {} queues exceeds the broker's {MAX_CHANGED_ENTRIES}",
                    entries.len()
                )));
            }
            let payload = changed_body(&entries).to_string();
            let body = Self::send(
                "partitions_changed",
                self.request(reqwest::Method::POST, "/api/v1/partitions/changed")
                    .body(payload),
            )
            .await?;
            decode_changed(&body, &entries)
        })
    }

    fn kv(&self, ops: Vec<KvOp>) -> BoxFuture<'_, Result<Vec<KvResult>>> {
        Box::pin(async move {
            if ops.is_empty() {
                return Ok(Vec::new());
            }
            let payload = kv_body(&ops).to_string();
            let body = Self::send(
                "kv",
                self.request(reqwest::Method::POST, "/api/v1/kv")
                    .body(payload),
            )
            .await?;
            decode_kv(&body, ops.len())
        })
    }

    fn list_queues(&self) -> BoxFuture<'_, Result<Vec<String>>> {
        Box::pin(async move {
            let body = Self::send(
                "list_queues",
                self.request(reqwest::Method::GET, "/api/v1/resources/queues"),
            )
            .await?;
            let parsed: QueueListBody =
                serde_json::from_str(&body).map_err(|e| SinkError::Body(e.to_string()))?;
            Ok(parsed
                .queues
                .into_iter()
                .map(|q| q.name)
                .filter(|n| !n.is_empty())
                .collect())
        })
    }
}

// ---------------------------------------------------------------------------
// The test double
// ---------------------------------------------------------------------------

/// One stored segment: a timestamp and the records pushed under it.
///
/// The segment is the unit that carries `ts` — every record of one push shares
/// it (F1) — so the double stores segments rather than records, which is what
/// makes `ts` behave the way the window engine depends on: co-monotone with
/// offset inside a partition, and repeated across a run of records.
#[derive(Clone, Debug)]
struct Segment {
    ts: Micros,
    base_offset: i64,
    records: Vec<(String, Option<Box<RawValue>>)>,
}

#[derive(Clone, Debug, Default)]
struct Lane {
    /// The retention watermark: the first offset still stored.
    log_start: i64,
    /// The next offset the allocator hands out — i.e. the high watermark.
    next_offset: i64,
    segments: Vec<Segment>,
    /// Quantized the way the broker quantizes it (001_log_schema:39-45): the
    /// FIRST push stamps the exact timestamp, every later push floors to the
    /// second, and the value only ever moves UP. That asymmetry is what makes
    /// `(last_write_at, name)` a sound paging key, so the double has to have it
    /// or the pagination tests prove nothing.
    last_write_at: Option<Micros>,
}

/// A `put` normalised out of [`KvOp::Put`] or [`KvOp::PutIfAbsent`]: the key,
/// the value, the TTL (`None` = forever), the precondition, and whether losing
/// it aborts the whole batch. Named, because the two variants are one code path
/// in the stored procedure and must be one here too — see [`FakeState::apply_kv`].
type NormalizedPut<'a> = (&'a String, &'a Value, Option<u64>, Option<i64>, bool);

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
    /// nobody has pushed to, which is a real and distinct state (032:36-49).
    queues: BTreeMap<String, BTreeMap<String, Lane>>,
    kv: BTreeMap<String, KvEntry>,
    /// Opaque and from a counter, never `version + 1` — the no-ABA rule of
    /// 024_kv.sql:133-140.
    next_version: i64,
    safe_time_pin: Option<Micros>,
    safe_time_degraded: bool,
    max_ts: Option<Micros>,
    now_ms_pin: Option<i64>,
    fail_next: usize,
    fail_kv_next: usize,
    records_per_call: usize,
    fetch_calls: u64,
    changed_calls: u64,
    kv_calls: u64,
    kv_batches: Vec<Vec<KvOp>>,
}

/// A faithful in-memory Queen: the log, the discovery index, `safeTime` and the
/// key/value store, with the semantics of 032_log_fetch.sql,
/// 033_log_partitions_changed.sql and 024_kv.sql rather than a convenient
/// approximation of them.
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
/// a record count. The real broker's ceiling is over COMPRESSED segment bytes,
/// which an in-memory double has no way to reproduce; what matters to every
/// caller is that a ceiling truncates a response and the caller must come back
/// for the rest, and this reproduces exactly that.
pub const FAKE_BYTES_PER_RECORD: i64 = 1_024;

/// The broker's own per-entry record ceiling (fetch.rs:78-122).
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
        self.state.lock().expect("FakeQueen lock")
    }

    // ---- seeding ---------------------------------------------------------

    /// Configure a queue with no partitions — `/configure` and nothing else.
    /// Fetching a lane of it answers bounds `0/0` and NO error, which is the
    /// distinction 032_log_fetch draws and a Kafka-shaped caller depends on.
    pub fn create_queue(&self, queue: &str) {
        self.lock().queues.entry(queue.to_string()).or_default();
    }

    /// Push one segment: `payloads` is a list of JSON texts (`"null"` for an
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
        let lane = st
            .queues
            .entry(queue.to_string())
            .or_default()
            .entry(partition.to_string())
            .or_default();
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
        // The quantization rule of 001_log_schema:39-45, and it only moves up.
        let candidate = match lane.last_write_at {
            None => ts,
            Some(_) => ts.floor_to(Micros::SECOND),
        };
        if lane.last_write_at.is_none_or(|cur| candidate > cur) {
            lane.last_write_at = Some(candidate);
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
        let mut st = self.lock();
        let Some(lane) = st.queues.get_mut(queue).and_then(|q| q.get_mut(partition)) else {
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

    /// Pin `safeTime`. Unpinned, it is one microsecond past the newest pushed
    /// segment, i.e. "everything that exists is safe" — the state a test that
    /// is not about `safeTime` wants.
    pub fn set_safe_time(&self, t: Micros) {
        self.lock().safe_time_pin = Some(t);
    }

    /// Go back to the derived `safeTime`.
    pub fn clear_safe_time(&self) {
        self.lock().safe_time_pin = None;
    }

    pub fn set_safe_time_degraded(&self, degraded: bool) {
        self.lock().safe_time_degraded = degraded;
    }

    /// Pin the clock TTLs are measured against. Unpinned, it is the wall clock.
    pub fn set_now_ms(&self, now_ms: i64) {
        self.lock().now_ms_pin = Some(now_ms);
    }

    /// Move a pinned clock forward — how a lease is expired in a test.
    pub fn advance_ms(&self, delta: i64) {
        let mut st = self.lock();
        let now = st.now_ms();
        st.now_ms_pin = Some(now + delta);
    }

    /// The overall record ceiling of one fetch CALL, spent in entry order.
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
}

impl FakeState {
    fn now_ms(&self) -> i64 {
        self.now_ms_pin.unwrap_or_else(crate::obs::now_epoch_ms)
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

    /// One fetch entry, with 032_log_fetch.sql's own arms.
    fn fetch_one(&self, entry: &FetchRequestEntry, budget: &mut usize) -> FetchedEntry {
        let empty = |error: Option<FetchError>, high: i64, log_start: i64| FetchedEntry {
            queue: entry.queue.clone(),
            partition: entry.partition.clone(),
            records: Vec::new(),
            high_watermark: high,
            log_start_offset: log_start,
            error,
        };
        let Some(lanes) = self.queues.get(&entry.queue) else {
            return empty(Some(FetchError::UnknownTopicOrPartition), 0, 0);
        };
        // A lane that has never been written is EMPTY, not missing: bounds 0/0
        // and no error (032:36-49). It still takes the offset arms below —
        // offset 0 is valid and empty, anything above it is out of range,
        // exactly as it will be after the first push.
        let empty_lane = Lane::default();
        let lane = lanes.get(&*entry.partition).unwrap_or(&empty_lane);
        let (log_start, high) = (lane.log_start, lane.next_offset);
        if entry.offset < log_start || entry.offset > high {
            return empty(Some(FetchError::OffsetOutOfRange), high, log_start);
        }
        let per_entry = match entry.max_bytes {
            Some(mb) => {
                ((mb / FAKE_BYTES_PER_RECORD).max(1) as usize).min(FAKE_MAX_RECORDS_PER_ENTRY)
            }
            None => FAKE_MAX_RECORDS_PER_ENTRY,
        };
        let mut records = Vec::new();
        'segments: for seg in &lane.segments {
            for (i, (txn, payload)) in seg.records.iter().enumerate() {
                let offset = seg.base_offset + i as i64;
                if offset < entry.offset {
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
        FetchedEntry {
            queue: entry.queue.clone(),
            partition: entry.partition.clone(),
            records,
            high_watermark: high,
            log_start_offset: log_start,
            error: None,
        }
    }

    /// One discovery entry, with 033_log_partitions_changed.sql's two modes,
    /// its opaque mode-tagged cursor and its `BAD_CURSOR` arm.
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

        // Only lanes that exist are enumerated: an unwritten lane has no row,
        // so there is no second arm here to leak a foreign tenant through.
        let mut rows: Vec<(&String, &Lane)> = lanes
            .iter()
            .filter(|(_, lane)| lane.last_write_at.is_some())
            .collect();

        let after = entry.after.as_deref();
        match since {
            None => {
                if let Some(cursor) = after {
                    let Some(name) = cursor.strip_prefix("n|") else {
                        return refuse("BAD_CURSOR");
                    };
                    rows.retain(|(n, _)| n.as_str() > name);
                }
                rows.sort_by(|a, b| a.0.cmp(b.0));
            }
            Some(t) => {
                if let Some(cursor) = after {
                    let Some(rest) = cursor.strip_prefix("t|") else {
                        return refuse("BAD_CURSOR");
                    };
                    let Some((us, name)) = rest.split_once('|') else {
                        return refuse("BAD_CURSOR");
                    };
                    let Ok(us) = us.parse::<i64>() else {
                        return refuse("BAD_CURSOR");
                    };
                    let key = (Micros(us), name.to_string());
                    rows.retain(|(n, lane)| {
                        (lane.last_write_at.unwrap_or(Micros::MIN), (*n).clone()) > key
                    });
                }
                rows.retain(|(_, lane)| lane.last_write_at.unwrap_or(Micros::MIN) >= t);
                rows.sort_by(|a, b| (a.1.last_write_at, a.0).cmp(&(b.1.last_write_at, b.0)));
            }
        }

        let truncated = rows.len() > limit;
        rows.truncate(limit);
        let next = match (truncated, rows.last()) {
            (true, Some((name, lane))) => Some(match since {
                None => format!("n|{name}"),
                Some(_) => format!("t|{}|{name}", lane.last_write_at.unwrap_or(Micros::MIN).0),
            }),
            _ => None,
        };
        ChangedEntry {
            queue: entry.queue.clone(),
            partitions: rows
                .into_iter()
                .map(|(name, lane)| PartitionBounds {
                    name: name.as_str().into(),
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
    /// The op ORDER is the stored procedure's, not the caller's: `getMany` and
    /// `getPrefix` run in a second phase after every write and every single
    /// `get` (024_kv.sql:939). A batch that puts a key and then reads it back by
    /// prefix therefore sees the write — which is worth reproducing, because a
    /// caller that relies on it against the real broker would be right.
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
            // `putIfAbsent` DESUGARS to `put` with `expect:0` at the entry of
            // the stored procedure's loop, and the answer is LABELLED `put` —
            // one code path, one label (024:960-966). Normalising here is what
            // gives this double the same one code path.
            let as_put: Option<NormalizedPut<'_>> = match op {
                KvOp::Put {
                    key,
                    value,
                    ttl_seconds,
                    expect,
                    required,
                } => Some((key, value, *ttl_seconds, *expect, *required)),
                KvOp::PutIfAbsent {
                    key,
                    value,
                    ttl_seconds,
                    required,
                } => Some((key, value, *ttl_seconds, Some(0), *required)),
                _ => None,
            };
            if let Some((key, value, ttl_seconds, expect, required)) = as_put {
                res.op = "put".into();
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
                    // never needs a second round trip (024:1467-1471).
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
                KvOp::Put { .. } | KvOp::PutIfAbsent { .. } => {
                    unreachable!("both are handled above, as one desugared code path")
                }
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
            Ok(entries
                .iter()
                .map(|e| st.fetch_one(e, &mut budget))
                .collect())
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
                    "a discovery call of {} queues exceeds the broker's {MAX_CHANGED_ENTRIES}",
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
        // `expect` is NOT sent: the stored procedure refuses a putIfAbsent whose
        // expect is anything but 0 (024:842-848), and it supplies the 0 itself.
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
    fn fetch_timeout_covers_the_park_and_never_shrinks() {
        assert_eq!(fetch_timeout(0), REQUEST_TIMEOUT);
        assert_eq!(fetch_timeout(30_000), Duration::from_secs(40));
        assert_eq!(fetch_timeout(999_999), Duration::from_secs(40));
    }

    #[test]
    fn base_urls_are_normalized_the_way_the_facade_normalizes_them() {
        assert_eq!(
            normalize_base_url("http://localhost:6632/").unwrap(),
            "http://localhost:6632"
        );
        assert!(normalize_base_url("ftp://x")
            .unwrap_err()
            .contains("scheme"));
        assert!(normalize_base_url("not a url").is_err());
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
        let body = r#"{"safeTime":"2026-09-04T10:04:57.412331Z","safeTimeDegraded":false,
            "entries":[{"queue":"orders","partitions":[
                {"name":"cust-0420","lastOffset":1811,"logStart":1400,
                 "lastWriteAt":"2026-09-04T10:04:00.000000Z"}],"next":"n|cust-0420"}]}"#;
        let out = decode_changed(body, &asked).unwrap();
        assert_eq!(
            out.safe_time,
            Micros::parse_iso("2026-09-04T10:04:57.412331Z").unwrap()
        );
        assert!(!out.safe_time_degraded);
        assert_eq!(out.entries[0].partitions[0].last_offset, 1811);
        assert_eq!(out.entries[0].partitions[0].log_start, 1400);
        assert_eq!(out.entries[0].next.as_deref(), Some("n|cust-0420"));
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
