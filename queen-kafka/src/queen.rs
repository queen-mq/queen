//! The Queen side of the facade: an HTTP client for the broker's admin surface,
//! plus the short-lived cache the Kafka metadata path reads through.
//!
//! `queen-kafka` is a *client* of Queen (PLAN_QUEEN_KAFKA.md: "speaks plain HTTP
//! to broker/proxy as a normal client"), so the obvious move is to depend on
//! `clients/client-rust`. It does not fit, for one reason that is structural
//! rather than cosmetic: that client binds its bearer token once, into the
//! `reqwest` default headers built in `Queen::new` (clients/client-rust/src/
//! http.rs), and there is no per-request override. M5 forwards a *per-connection*
//! credential — SASL/PLAIN maps a username/password to the tenant's token — so a
//! token that belongs to the client object would mean one client object per
//! Kafka connection: a fresh connection pool, TLS session cache and load
//! balancer per consumer. The token is therefore an argument of every call here,
//! and the caller (M1: `QUEEN_TOKEN`, M5: the connection's own) decides it. The
//! admin surface would not have carried its weight either — it returns
//! `serde_json::Value` for these routes, which is the same parsing this module
//! does, minus the queue/kv/timer/streams/buffer modules that come with it.
//!
//! Routes used, and their shapes, read off server/src/main.rs and
//! server/src/handlers/queues.rs (never guessed):
//!
//!   * `GET /api/v1/resources/queues` → `queen.get_queues_v2` enriched with the
//!     live per-queue counts, i.e. `{"queues":[{"name":…,"partitions":N,…}],…}`
//!     (server/sql/procedures/018_stats.sql). `partitions` is the number of
//!     `queen.log_partitions` rows the queue has — see [`Queue::partitions`].
//!   * `POST /api/v1/configure` `{"queue":"<name>"}` → `queen.configure_queue_v1`
//!     (server/sql/procedures/012_configure.sql), which upserts the
//!     `queen.queues` row and answers `{"configured":true,…}`. An empty options
//!     bag is deliberate: it takes the SP's own defaults, and those leave
//!     retention OFF (`retentionEnabled` false, `retentionSeconds` 0), so a
//!     topic auto-created for a Kafka producer cannot quietly start expiring the
//!     records it is handed.
//!   * `POST /api/v1/push` `{"items":[{"queue","partition","payload"},…]}` →
//!     one JSON object per item, in item order, carrying `status` and — since
//!     C1 (PLAN_QUEEN_KAFKA.md) — the assigned absolute `offset`
//!     (server/src/handlers/data.rs, `render_push_results`). This is the write
//!     path for Produce; see [`PushItem`].
//!   * `POST /api/v1/fetch` `{"entries":[{"queue","partition","offset",
//!     "maxBytes"},…],"maxWaitMs","minBytes"}` → one entry per request entry,
//!     in request order, carrying the records plus `highWatermark` and
//!     `logStartOffset` (C2, server/src/handlers/fetch.rs). This is the read
//!     path for Fetch AND the bounds probe for ListOffsets; see [`FetchEntry`].
//!   * `POST /api/v1/kv` `{"operations":[…]}` → `{"results":[…]}`, one result
//!     per operation, each stamped with its own `index`
//!     (server/src/handlers/kv.rs, server/sql/procedures/024_kv.sql). This is
//!     where committed group offsets live until the native cursor of C3 exists
//!     (PLAN_QUEEN_KAFKA.md M4); see [`KvOp`] and [`crate::offsets`].

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::identity::TenantKey;
use crate::secret::CredentialKey;

/// A boxed future, so [`QueenApi`] stays dyn-compatible. `async fn` in a trait
/// is not: it desugars to an opaque associated type, which no trait object can
/// name. The facade holds `Arc<dyn QueenApi>` precisely so the tests can hand it
/// a double instead of a socket.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub type Result<T> = std::result::Result<T, Error>;

/// Why a call to Queen did not produce an answer. Kept coarse on purpose: every
/// variant maps to the same retriable Kafka error at the metadata boundary, and
/// the distinction exists for the log line, not for control flow.
///
/// `Clone` because [`Catalog`] caches the failure as well as the success and
/// hands the same one to every caller inside the window — see [`Entry`].
#[derive(Debug, Clone)]
pub enum Error {
    /// The request never completed: DNS, connect, TLS, timeout, reset.
    Transport(String),
    /// Queen answered, with a status that is not a success.
    Status {
        code: u16,
        body: String,
        /// The `Retry-After` the answer carried, in milliseconds.
        ///
        /// The proxy sets it on every 429 it writes (proxy/src/errors.rs,
        /// `err_429`) and on nothing else, which is what makes a rate cap or a
        /// freeze say WHEN rather than merely "not now". It is carried here
        /// rather than mapped on the spot because its destination is a Kafka
        /// field — `throttle_time_ms`, see [`crate::throttle`] — and this
        /// module knows nothing about Kafka.
        retry_after_ms: Option<i64>,
    },
    /// Queen answered 2xx with a body this client cannot read.
    Body(String),
}

impl Error {
    /// A non-2xx answer that named no `Retry-After`.
    pub fn status(code: u16, body: impl Into<String>) -> Error {
        Error::Status {
            code,
            body: body.into(),
            retry_after_ms: None,
        }
    }

    /// The `Retry-After` this failure carried, in milliseconds.
    pub fn retry_after_ms(&self) -> Option<i64> {
        match self {
            Error::Status { retry_after_ms, .. } => *retry_after_ms,
            _ => None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Transport(e) => write!(f, "transport: {e}"),
            Error::Status {
                code,
                body,
                retry_after_ms,
            } => match retry_after_ms {
                Some(ms) => write!(f, "HTTP {code} (retry after {ms}ms): {}", Snippet(body)),
                None => write!(f, "HTTP {code}: {}", Snippet(body)),
            },
            Error::Body(e) => write!(f, "body: {e}"),
        }
    }
}

impl std::error::Error for Error {}

/// A response body in a log line, clamped. Queen error bodies are small JSON,
/// but a misrouted request can land on anything (an ingress error page, a
/// dashboard bundle), and that must not become the log.
struct Snippet<'a>(&'a str);

impl std::fmt::Display for Snippet<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const MAX: usize = 200;
        let s = self.0.trim();
        match s.char_indices().nth(MAX) {
            Some((cut, _)) => write!(f, "{}…", &s[..cut]),
            None => write!(f, "{s}"),
        }
    }
}

/// One queue as the admin list reports it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Queue {
    pub name: String,
    /// Live count of `queen.log_partitions` rows for this queue.
    ///
    /// It is NOT a declared width the way a Kafka partition count is: Queen has
    /// no such declaration. `/configure` creates the queue row and nothing else
    /// ("the log engine creates partitions lazily on the first push",
    /// 012_configure.sql), so a queue that has never been pushed to reports 0
    /// and a queue pushed to on lanes 0 and 7 reports 2. Turning this into the
    /// number Kafka needs is [`crate::handlers::metadata`]'s job, not this
    /// module's.
    #[serde(default)]
    pub partitions: i64,
}

/// One message to write, as `POST /api/v1/push` takes it.
///
/// `transactionId` is deliberately not sent. It is the broker's dedup key, and
/// the facade has nothing to put in it that would be safe: a Kafka producer's
/// own identity is its `producer_id`/`sequence` pair, which is exactly what M2
/// refuses to accept (`handlers::produce`), and inventing a key from the record
/// bytes would silently drop the legitimate retransmissions Kafka producers make
/// by design. Omitted, the broker mints one per message, which is at-least-once
/// — the same guarantee Kafka gives a non-idempotent producer.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PushItem {
    pub queue: String,
    /// The Queen partition NAME. The facade writes the Kafka partition index as
    /// a decimal string, which is what makes "Kafka partition n = Queen
    /// partition n" true on both sides (PLAN_QUEEN_KAFKA.md).
    pub partition: String,
    pub payload: serde_json::Value,
}

/// What the broker did with one pushed item.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Pushed {
    /// `queued`, `duplicate`, `error`, `buffered` or `failed` — the per-item
    /// status `render_push_results` writes. Kept as it came so a refusal can
    /// name itself in the log rather than becoming a bare "no offset".
    #[serde(default)]
    pub status: String,
    /// The assigned absolute offset within the partition (C1). Absent whenever
    /// the broker allocated none: a spooled item in maintenance mode, an item
    /// whose bundle failed. Never guessed.
    #[serde(default)]
    pub offset: Option<i64>,
}

// ----------------------------------------------------------------------- read
//
// `POST /api/v1/fetch` (C2). The three ceilings below are the broker's own
// (server/src/handlers/fetch.rs: `MAX_ENTRIES`, `MAX_WAIT_MS`,
// `MAX_BYTES_PER_ENTRY`) and are named here because the facade has to respect
// them BEFORE the call rather than discover them in the answer. The entry count
// is the one that matters most: the broker CLAMPS the other two silently, and
// REJECTS an over-long entry list with a 400 for the whole batch — which for a
// consumer assigned more lanes than that would be its entire poll failing
// rather than a shorter answer. See `handlers::fetch`, which chunks against it.

/// Max entries in one `POST /api/v1/fetch`.
pub const MAX_FETCH_ENTRIES: usize = 1024;
/// Max long-poll parking the broker will honour, milliseconds.
pub const MAX_FETCH_WAIT_MS: i64 = 30_000;
/// Max per-entry `maxBytes`, over the COMPRESSED segment bytes the read is
/// budgeted in — not over the records that come back. See [`FetchEntry`].
pub const MAX_FETCH_BYTES_PER_ENTRY: i64 = 8 * 1024 * 1024;

/// One partition to read, as `POST /api/v1/fetch` takes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FetchEntry {
    pub queue: String,
    /// The Queen partition NAME, the same decimal-index spelling [`PushItem`]
    /// writes — which is what makes a record readable at the offset the push
    /// answered.
    pub partition: String,
    /// Absolute offset to read FROM, inclusive. Must not be negative: the
    /// broker rejects the whole batch rather than reading a Kafka sentinel as
    /// an offset (server/src/handlers/fetch.rs).
    pub offset: i64,
    /// Ceiling on the COMPRESSED segment bytes this entry may read. It is not
    /// the size of the answer — Queen segments are zstd'd, so a budget spent in
    /// compressed bytes buys an unknown, usually larger, number of record
    /// bytes. Kafka's own `max_bytes` is documented as a soft limit for exactly
    /// this class of reason (KIP-74), which is what makes the mapping in
    /// `handlers::fetch` legitimate rather than approximate-and-hoping.
    #[serde(rename = "maxBytes")]
    pub max_bytes: i64,
}

/// One record of one entry's answer.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct FetchedRecord {
    /// Absolute offset within the partition.
    pub offset: i64,
    /// The stored payload, spliced back verbatim. `null` for a frame with an
    /// empty payload, which is what a native producer's empty push looks like.
    #[serde(default)]
    pub payload: serde_json::Value,
    /// When the SEGMENT carrying this record was written, as the broker renders
    /// it: `YYYY-MM-DDTHH:MM:SS.ffffffZ`. Per segment, not per record — it is
    /// the log's own timestamp, and it is only ever a FALLBACK for a payload
    /// that carries no producer timestamp of its own ([`crate::records`]).
    #[serde(default)]
    pub ts: Option<String>,
}

impl FetchedRecord {
    /// [`FetchedRecord::ts`] as epoch milliseconds, or `None` when there is no
    /// timestamp or it is not the shape the broker renders.
    pub fn timestamp_ms(&self) -> Option<i64> {
        self.ts.as_deref().and_then(epoch_millis)
    }
}

/// What one entry of a fetch answered.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Fetched {
    #[serde(default)]
    pub records: Vec<FetchedRecord>,
    /// The next offset the log will assign: `last + 1`. Reported whether or not
    /// a record came back, and reported alongside an `error` too — which is
    /// what makes an errored fetch a usable bounds probe.
    #[serde(rename = "highWatermark", default)]
    pub high_watermark: i64,
    /// The retention watermark: the first offset still stored.
    #[serde(rename = "logStartOffset", default)]
    pub log_start_offset: i64,
    /// `UNKNOWN_TOPIC_OR_PARTITION` or `OFFSET_OUT_OF_RANGE`, spelled the way
    /// Kafka spells them (server/sql/procedures/032_log_fetch.sql). Kept as a
    /// string rather than parsed into an enum here: a marker this build does
    /// not know must reach the handler as itself so it can be logged, not
    /// silently become one it does know.
    #[serde(default)]
    pub error: Option<String>,
}

/// The two per-entry error markers C2 answers with.
pub const FETCH_ERR_UNKNOWN: &str = "UNKNOWN_TOPIC_OR_PARTITION";
pub const FETCH_ERR_OUT_OF_RANGE: &str = "OFFSET_OUT_OF_RANGE";

/// Parse the broker's segment timestamp into epoch milliseconds.
///
/// The format is fixed and narrow — `to_char(created_at,
/// 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')` in 032_log_fetch.sql — so this parses that
/// and nothing else rather than pulling in a date-time crate for one field.
/// Anything that is not exactly that shape answers `None`, which becomes
/// "unknown timestamp" on the Kafka side; a wrong instant would be worse than
/// no instant, because a consumer's time-based tooling would believe it.
///
/// The trailing `Z` is taken at its word. `to_char` renders a `TIMESTAMPTZ` in
/// the SESSION time zone and the broker sets none, so an operator running
/// Postgres on a non-UTC `timezone` GUC would have the broker stamp local time
/// under a `Z`. That is C2's to fix if it ever bites; here it can only move a
/// FALLBACK timestamp (a Kafka-produced record carries its own in the
/// envelope), never an offset.
fn epoch_millis(ts: &str) -> Option<i64> {
    let ts = ts.strip_suffix('Z').unwrap_or(ts);
    let (date, time) = ts.split_once('T')?;
    let mut d = date.splitn(3, '-');
    let (y, mo, da) = (d.next()?, d.next()?, d.next()?);
    if y.len() != 4 || mo.len() != 2 || da.len() != 2 {
        return None;
    }
    let mut t = time.splitn(3, ':');
    let (h, mi, rest) = (t.next()?, t.next()?, t.next()?);
    let (s, frac) = match rest.split_once('.') {
        Some((s, frac)) => (s, frac),
        None => (rest, ""),
    };
    if h.len() != 2 || mi.len() != 2 || s.len() != 2 || !frac.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    // Sub-second digits are truncated, not rounded: Kafka timestamps are
    // milliseconds and the broker renders microseconds, so the three digits
    // past the millisecond are precision the destination cannot carry.
    let millis: i64 = format!("{frac:0<3}")[..3].parse().ok()?;
    let num = |s: &str| s.parse::<i64>().ok();
    let (y, mo, da) = (num(y)?, num(mo)?, num(da)?);
    let (h, mi, s) = (num(h)?, num(mi)?, num(s)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&da) {
        return None;
    }
    if !(0..=23).contains(&h) || !(0..=59).contains(&mi) || !(0..=60).contains(&s) {
        return None;
    }
    let days = days_from_civil(y, mo, da);
    Some(((days * 86_400 + h * 3_600 + mi * 60 + s) * 1_000) + millis)
}

/// Days between 1970-01-01 and a proleptic-Gregorian civil date (Hinnant's
/// `days_from_civil`). Branch-free calendar arithmetic, exact for every year
/// this will ever see.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

// ------------------------------------------------------------------ key/value
//
// `POST /api/v1/kv`, the durable half of M4: a consumer group's committed
// offsets. The ceilings below are the broker's own defaults
// (server/src/handlers/kv.rs and the constants at the head of
// server/sql/procedures/024_kv.sql) and are mirrored here for the same reason
// the C2 ones above are — the facade has to stay under them BEFORE the call,
// because an over-long batch is a 400 for the whole batch and a whole batch of
// offsets is one consumer's entire commit.

/// Max operations in one `POST /api/v1/kv` (`QUEEN_KV_MAX_OPS_PER_CALL`).
pub const MAX_KV_OPS_PER_CALL: usize = 256;
/// Max keys summed over one call's operations (`QUEEN_KV_MAX_KEYS_PER_CALL`).
/// A `getMany` counts its key array; a `getPrefix` counts its clamped limit.
pub const MAX_KV_KEYS_PER_CALL: usize = 1024;
/// Ceiling a `getPrefix` limit is CLAMPED to, never rejected against.
pub const MAX_KV_PREFIX_LIMIT: i64 = 1000;
/// Max serialized bytes of one value (`QUEEN_KV_MAX_VALUE_BYTES`).
pub const MAX_KV_VALUE_BYTES: usize = 65_536;
/// Total value bytes ONE call may read back before the stored procedure starts
/// truncating (`QUEEN_KV_MAX_READ_BYTES`, 4 MiB). It is not an error and not a
/// clamp a caller can see coming: rows past it are simply not returned, and
/// `truncated` is how the answer says so. See [`crate::offsets`], which sizes
/// its chunks against this number rather than against the key count alone.
pub const MAX_KV_READ_BYTES: usize = 4 * 1024 * 1024;

/// One key/value operation, in the exact shape `kv_apply_v1` takes.
///
/// Only the three the offset path needs are modelled. `ns` is a separate field
/// from `key` and is validated against `^[a-z0-9][a-z0-9._-]{0,63}$` by the
/// stored procedure, so it is never a place to put anything a client chose.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "op")]
pub enum KvOp {
    /// Unconditional upsert. `forever` is not a default and not an option: the
    /// stored procedure demands EXACTLY ONE of `ttlSeconds` and `forever` on
    /// every write, deliberately, so that nothing lands in the store without
    /// someone having decided when it leaves. A committed offset's answer is
    /// "never" — a consumer group that finds its offsets expired resumes from
    /// `auto.offset.reset`, which is either a replay of the whole topic or a
    /// silent skip to its end. Use [`KvOp::put`] rather than writing the field.
    #[serde(rename = "put")]
    Put {
        ns: String,
        key: String,
        value: serde_json::Value,
        forever: bool,
    },
    /// Read a known key list. Answers `rows` for the ones that are there and
    /// `missing` for the ones that are not — absence is a datum, not a hole the
    /// caller computes by difference.
    #[serde(rename = "getMany")]
    GetMany { ns: String, keys: Vec<String> },
    /// Read a key range by prefix, paged with an exclusive `after` cursor in
    /// byte order. The one operation `POST /api/v1/kv` has and the transaction
    /// wire does not.
    #[serde(rename = "getPrefix")]
    GetPrefix {
        ns: String,
        prefix: String,
        limit: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        after: Option<String>,
    },
}

impl KvOp {
    /// A write that never expires. See [`KvOp::Put`].
    pub fn put(ns: &str, key: &str, value: serde_json::Value) -> KvOp {
        KvOp::Put {
            ns: ns.to_string(),
            key: key.to_string(),
            value,
            forever: true,
        }
    }

    /// What this operation costs against [`MAX_KV_KEYS_PER_CALL`], counted the
    /// way the broker counts it (server/src/handlers/kv.rs).
    pub fn keys(&self) -> usize {
        match self {
            KvOp::Put { .. } => 1,
            KvOp::GetMany { keys, .. } => keys.len(),
            KvOp::GetPrefix { limit, .. } => (*limit).clamp(1, MAX_KV_PREFIX_LIMIT) as usize,
        }
    }
}

/// One row of a read.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct KvRow {
    pub key: String,
    /// `'null'::jsonb` is a legal stored value, so this is `Value::Null` both
    /// for a key holding null and for an answer that carried no value at all
    /// (`keysOnly`, which this facade never asks for).
    #[serde(default)]
    pub value: serde_json::Value,
}

/// What one operation answered. A faithful subset: the fields the offset path
/// reads, plus the `index` that says which operation this is.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct KvAnswer {
    #[serde(default)]
    pub index: usize,
    /// `put`, `getMany`, `getPrefix` — as the stored procedure labelled it,
    /// which is authoritative over what was asked (it is where `putIfAbsent`
    /// becomes `put`).
    #[serde(default)]
    pub op: String,
    /// Writes only. A `put` with no precondition is always `Some(true)`.
    #[serde(default)]
    pub applied: Option<bool>,
    #[serde(default)]
    pub rows: Vec<KvRow>,
    /// `getMany` only: the keys that are not there.
    #[serde(default)]
    pub missing: Vec<String>,
    /// The read did not return everything it matched — either the page limit or
    /// the 4 MiB call budget cut it. Keys lost to the budget are in NEITHER
    /// `rows` nor `missing`, which is why this flag can never be ignored: for
    /// an offset read, treating one as absent is a consumer reset.
    #[serde(default)]
    pub truncated: bool,
    /// `getPrefix` only: the cursor to continue from, set when `truncated`.
    #[serde(rename = "nextAfter", default)]
    pub next_after: Option<String>,
}

/// The calls the facade makes to Queen. A trait, and not just the concrete
/// client below, so the policies built on it — auto-create, the produce
/// mapping, the fetch batching, the offset store — can be tested against a
/// double that records what it was asked rather than against a broker.
pub trait QueenApi: Send + Sync + 'static {
    /// `GET /api/v1/resources/queues`, in the queue list's own order.
    fn list_queues<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Vec<Queue>>>;

    /// `POST /api/v1/configure` — create the queue if it is not there, leave it
    /// exactly as it is if it is (the SP is an upsert of the config columns, and
    /// an empty options bag rewrites them to the SP defaults, so this is only
    /// ever called for a name the catalog does not know).
    fn create_queue<'a>(
        &'a self,
        name: &'a str,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<()>>;

    /// `POST /api/v1/push` — one write for every item, answered one result per
    /// item, aligned to `items` by index.
    fn push<'a>(
        &'a self,
        items: &'a [PushItem],
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Pushed>>>;

    /// `POST /api/v1/fetch` — one read for every entry, answered one result per
    /// entry, in request order. `max_wait_ms` and `min_bytes` are the long poll
    /// and must already be within [`MAX_FETCH_WAIT_MS`]; `entries` must already
    /// be within [`MAX_FETCH_ENTRIES`], because exceeding it fails the batch.
    fn fetch<'a>(
        &'a self,
        entries: &'a [FetchEntry],
        max_wait_ms: i64,
        min_bytes: i64,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Fetched>>>;

    /// `POST /api/v1/kv` — one answer per operation, aligned to `ops` by the
    /// `index` each answer carries. `ops` must already be within
    /// [`MAX_KV_OPS_PER_CALL`] and [`MAX_KV_KEYS_PER_CALL`], because exceeding
    /// either fails the whole batch.
    fn kv<'a>(
        &'a self,
        ops: &'a [KvOp],
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<KvAnswer>>>;

    /// `GET /auth/me` — the identity Queen attributes to this credential, read
    /// as [`crate::identity::tenant_of`] reads it: `Some(tenant)` when the
    /// answer names the cluster the credential acts on, `None` when it answers
    /// and names none.
    ///
    /// The tenant is what BOTH long-lived per-credential maps are keyed by (the
    /// catalog below and the group registry), so that one tenant's two
    /// credentials are one scope rather than two sharing one set of committed
    /// offsets — see [`crate::identity`] for the whole of that argument, and
    /// for what each surface answers today.
    ///
    /// Defaulted to "names none" for the same reason [`QueenApi::with_host`] is
    /// defaulted: it is the honest answer for an implementation that has no
    /// identity surface, and it keeps a double that is testing something else
    /// from having to script one.
    fn identity<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Option<String>>> {
        let _ = token;
        Box::pin(async { Ok(None) })
    }

    /// The same Queen, reached with `host` as the HTTP `Host` header of every
    /// call — the M5 shared-host fit (`QUEEN_KAFKA_FORWARD_SNI_HOST`).
    ///
    /// `None` means "this implementation has no HTTP to stamp", and the caller
    /// keeps the client it already has. That is the honest answer for the test
    /// double, and it is why this is a defaulted method rather than a required
    /// one.
    ///
    /// The header is what the proxy routes on: the first DNS label of `Host`
    /// names the cluster, unless the name is in `QUEEN_PROXY_SHARED_HOSTS`, in
    /// which case the credential does (proxy/src/acting.rs, decision z). One
    /// facade in front of many tenants therefore has to send the name each
    /// connection asked for, and TLS already carries it: the SNI.
    fn with_host(&self, host: &str) -> Option<Arc<dyn QueenApi>> {
        let _ = host;
        None
    }
}

// --------------------------------------------------------------------- client

/// Total budget for one admin call. The Kafka connection that triggered it is
/// muted until it answers (conn.rs: one request in flight), so an unbounded wait
/// here is a hung consumer with no error to show for it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// The real client. One `reqwest::Client` for the process: it owns the
/// connection pool, and the facade's whole traffic to Queen is a handful of
/// admin calls per metadata refresh.
pub struct HttpQueen {
    /// `QUEEN_URL`, without a trailing slash (see [`normalize_base_url`]).
    base: String,
    /// The `Host` header every call sends, when it is not the one the URL
    /// implies. See [`HttpQueen::with_host`].
    host: Option<String>,
    http: reqwest::Client,
}

impl HttpQueen {
    pub fn new(base_url: &str) -> std::result::Result<HttpQueen, String> {
        let base = normalize_base_url(base_url)?;
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|e| format!("cannot build the HTTP client for QUEEN_URL={base}: {e}"))?;
        Ok(HttpQueen {
            base,
            host: None,
            http,
        })
    }

    fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        token: Option<&str>,
    ) -> reqwest::RequestBuilder {
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
        // Bearer, matching what the broker's auth layer extracts
        // (server/src/auth.rs::extract_bearer). Per call, never on the client:
        // see the module header.
        match token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }

    async fn send(req: reqwest::RequestBuilder) -> Result<String> {
        let resp = req
            .send()
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        let status = resp.status();
        let retry_after_ms = retry_after_ms(resp.headers());
        let body = resp
            .text()
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        if !status.is_success() {
            return Err(Error::Status {
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
/// than parsed: this value becomes a Kafka `throttle_time_ms` that a client
/// SLEEPS for, and a misread date is a consumer parked for hours. `None` is not
/// a loss — [`crate::throttle`] has a default for exactly this.
fn retry_after_ms(headers: &reqwest::header::HeaderMap) -> Option<i64> {
    let seconds: i64 = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    // A negative or absurd value is a broker saying something this cannot use.
    // `checked_mul` because seconds came off the wire.
    seconds.checked_mul(1_000).filter(|ms| *ms >= 0)
}

/// The list body. Only the fields this facade reads are named; the broker adds
/// to this response for the dashboard (retainedBytes, messages, the top-level
/// kv/timer byte counters) and none of that may turn into a parse failure here.
#[derive(Debug, Deserialize)]
struct QueueListBody {
    #[serde(default)]
    queues: Vec<Queue>,
}

impl QueenApi for HttpQueen {
    fn list_queues<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Vec<Queue>>> {
        Box::pin(async move {
            // Not `?stats=cached`: that serves `queen.stats.child_count` as of
            // the last stats refresh, which for a queue created seconds ago (the
            // auto-create path, every time) is a partition count from before the
            // queue existed. The enriched form costs a pass over the tenant's
            // partitions, which is what the TTL below is for.
            let body =
                Self::send(self.request(reqwest::Method::GET, "/api/v1/resources/queues", token))
                    .await?;
            let parsed: QueueListBody =
                serde_json::from_str(&body).map_err(|e| Error::Body(e.to_string()))?;
            Ok(parsed.queues)
        })
    }

    fn create_queue<'a>(
        &'a self,
        name: &'a str,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let payload = serde_json::json!({ "queue": name }).to_string();
            let body = Self::send(
                self.request(reqwest::Method::POST, "/api/v1/configure", token)
                    .body(payload),
            )
            .await?;
            // handle_configure surfaces a stored-procedure failure as a non-2xx,
            // but the SP can also echo `{"error":…}` inside a 200 — the handler
            // only re-maps it when it parses (server/src/handlers/queues.rs,
            // "RUSTFIX item 25"). Check for it rather than reporting a queue we
            // did not create.
            let parsed: serde_json::Value =
                serde_json::from_str(&body).map_err(|e| Error::Body(e.to_string()))?;
            if let Some(e) = parsed.get("error").filter(|e| !e.is_null()) {
                return Err(Error::Body(format!("configure answered {e}")));
            }
            if parsed.get("configured").and_then(|c| c.as_bool()) != Some(true) {
                return Err(Error::Body(format!(
                    "configure did not confirm: {}",
                    Snippet(&body)
                )));
            }
            Ok(())
        })
    }

    fn push<'a>(
        &'a self,
        items: &'a [PushItem],
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Pushed>>> {
        Box::pin(async move {
            let payload = serde_json::to_string(&PushBody { items })
                .map_err(|e| Error::Body(format!("cannot serialize the push body: {e}")))?;
            let body = Self::send(
                self.request(reqwest::Method::POST, "/api/v1/push", token)
                    .body(payload),
            )
            .await?;
            align_push_results(&body, items.len())
        })
    }

    fn fetch<'a>(
        &'a self,
        entries: &'a [FetchEntry],
        max_wait_ms: i64,
        min_bytes: i64,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Fetched>>> {
        Box::pin(async move {
            let payload = serde_json::to_string(&FetchBody {
                entries,
                max_wait_ms,
                min_bytes,
            })
            .map_err(|e| Error::Body(format!("cannot serialize the fetch body: {e}")))?;
            let body = Self::send(
                self.request(reqwest::Method::POST, "/api/v1/fetch", token)
                    .body(payload)
                    // The ONE call that overrides the client's budget. See
                    // `fetch_timeout`.
                    .timeout(fetch_timeout(max_wait_ms)),
            )
            .await?;
            align_fetch_results(&body, entries)
        })
    }

    fn kv<'a>(
        &'a self,
        ops: &'a [KvOp],
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<KvAnswer>>> {
        Box::pin(async move {
            // `{"operations":[…]}` and not the bare array the route also
            // accepts: it is the shape the transaction wire uses, so one shape
            // is learned once (server/src/handlers/kv.rs).
            let payload = serde_json::to_string(&KvBody { operations: ops })
                .map_err(|e| Error::Body(format!("cannot serialize the kv body: {e}")))?;
            let body = Self::send(
                self.request(reqwest::Method::POST, "/api/v1/kv", token)
                    .body(payload),
            )
            .await?;
            align_kv_results(&body, ops.len())
        })
    }

    /// `GET /auth/me`, with this call's own bearer.
    ///
    /// The path is the same on both surfaces and so is the payload shape
    /// (server/src/handlers/standalone.rs is written to be "field-for-field the
    /// proxy's shape"), which is why one call covers broker-direct and Cloud.
    /// A refusal is returned as the [`Error::Status`] it is rather than
    /// flattened to `None` here: the CALLER distinguishes "answered, names
    /// none" from "would not answer", because only one of those is worth
    /// asking again.
    fn identity<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Option<String>>> {
        Box::pin(async move {
            let body = Self::send(self.request(
                reqwest::Method::GET,
                crate::identity::IDENTITY_PATH,
                token,
            ))
            .await?;
            Ok(crate::identity::tenant_of(&body))
        })
    }

    /// A second handle on the SAME `reqwest::Client`, differing only in the
    /// `Host` header it writes. Cloning the client is what makes this cheap
    /// enough to do per SNI name: a `reqwest::Client` clone shares the
    /// connection pool, the DNS cache and the TLS session cache with the
    /// original, so a hundred hostnames are still one pool.
    fn with_host(&self, host: &str) -> Option<Arc<dyn QueenApi>> {
        Some(Arc::new(HttpQueen {
            base: self.base.clone(),
            host: Some(host.to_string()),
            http: self.http.clone(),
        }))
    }
}

/// The KV request body. Borrowed, like [`PushBody`] and [`FetchBody`].
#[derive(Serialize)]
struct KvBody<'a> {
    operations: &'a [KvOp],
}

#[derive(Deserialize)]
struct KvResponseBody {
    #[serde(default)]
    results: Vec<KvAnswer>,
}

/// Match a KV response back to the operations that produced it.
///
/// By the explicit `index` each result carries, for the same reason
/// [`align_push_results`] does it that way: the stored procedure applies write
/// operations in key order rather than input order and reports them by input
/// ordinal, so position is a property of the answer rather than of the call —
/// and an offset read attributed to the wrong partition is a consumer told to
/// resume somewhere it never was.
fn align_kv_results(body: &str, ops: usize) -> Result<Vec<KvAnswer>> {
    let parsed: KvResponseBody =
        serde_json::from_str(body).map_err(|e| Error::Body(e.to_string()))?;
    let mut out: Vec<Option<KvAnswer>> = (0..ops).map(|_| None).collect();
    for r in parsed.results {
        let slot = out
            .get_mut(r.index)
            .ok_or_else(|| Error::Body(format!("kv result {} is out of range", r.index)))?;
        if slot.is_some() {
            return Err(Error::Body(format!("kv result {} appears twice", r.index)));
        }
        *slot = Some(r);
    }
    out.into_iter()
        .enumerate()
        .map(|(i, r)| {
            r.ok_or_else(|| Error::Body(format!("kv answered nothing for operation {i}")))
        })
        .collect()
}

/// The HTTP budget for one fetch, which is the only call that does not take the
/// client's default.
///
/// It has to be its own, and the reason is not tuning: a fetch is a LONG POLL,
/// so the broker is EXPECTED to hold the request open for up to
/// [`MAX_FETCH_WAIT_MS`] before answering. Under the client-wide
/// [`REQUEST_TIMEOUT`] a consumer asking for a 30-second park would have its
/// request cancelled at 10 seconds and be told the transport failed — an error
/// where the correct answer was "nothing yet", on every poll, for ever. The
/// budget is therefore the park PLUS the same slack every other call gets, so
/// the time allowed for the request itself is unchanged and only the parking is
/// added.
fn fetch_timeout(max_wait_ms: i64) -> Duration {
    REQUEST_TIMEOUT + Duration::from_millis(max_wait_ms.clamp(0, MAX_FETCH_WAIT_MS) as u64)
}

/// The fetch request body. Borrowed, like [`PushBody`].
#[derive(Serialize)]
struct FetchBody<'a> {
    entries: &'a [FetchEntry],
    #[serde(rename = "maxWaitMs")]
    max_wait_ms: i64,
    #[serde(rename = "minBytes")]
    min_bytes: i64,
}

/// One entry of the fetch response, with the two fields that say WHICH entry it
/// is (server/src/handlers/fetch.rs, `render_fetch`).
#[derive(Deserialize)]
struct FetchResultEntry {
    #[serde(default)]
    queue: String,
    #[serde(default)]
    partition: String,
    #[serde(flatten)]
    fetched: Fetched,
}

#[derive(Deserialize)]
struct FetchResponseBody {
    #[serde(default)]
    entries: Vec<FetchResultEntry>,
}

/// Match a fetch response back to the entries that asked for it.
///
/// The broker answers in request order and the SQL fails the whole call rather
/// than return a misaligned array, so this is a second belt on a braced trouser
/// — and it is worth wearing, for the same reason [`align_push_results`] is: an
/// entry read as another partition's hands a consumer records and a high
/// watermark it will commit against the wrong lane. The response NAMES each
/// entry's queue and partition, so the check is exact rather than positional.
fn align_fetch_results(body: &str, entries: &[FetchEntry]) -> Result<Vec<Fetched>> {
    let parsed: FetchResponseBody =
        serde_json::from_str(body).map_err(|e| Error::Body(e.to_string()))?;
    if parsed.entries.len() != entries.len() {
        return Err(Error::Body(format!(
            "fetch answered {} entries for {} asked",
            parsed.entries.len(),
            entries.len()
        )));
    }
    let mut out = Vec::with_capacity(entries.len());
    for (i, (got, want)) in parsed.entries.into_iter().zip(entries).enumerate() {
        if got.queue != want.queue || got.partition != want.partition {
            return Err(Error::Body(format!(
                "fetch entry {i} came back as {}/{} but was asked for {}/{}",
                got.queue, got.partition, want.queue, want.partition
            )));
        }
        out.push(got.fetched);
    }
    Ok(out)
}

/// The push request body. Borrowed, so a batch of ten thousand records is
/// serialized straight out of the items the produce handler already built.
#[derive(Serialize)]
struct PushBody<'a> {
    items: &'a [PushItem],
}

/// One result as `render_push_results` writes it, with the `index` that says
/// which item it belongs to.
#[derive(Deserialize)]
struct PushResultItem {
    index: usize,
    #[serde(flatten)]
    pushed: Pushed,
}

/// Match a push response back to the items that produced it.
///
/// The broker renders one object per item in item order and stamps each with
/// its `index`, but the caller has to line the two up by SOMETHING, and the
/// alignment is what the whole offset mapping rests on: a produce answer built
/// from the wrong item reports one partition's offset for another's records.
/// So it is done by the explicit `index`, and a response that does not cover
/// every item exactly once is an error rather than a shifted answer.
fn align_push_results(body: &str, items: usize) -> Result<Vec<Pushed>> {
    let parsed: Vec<PushResultItem> =
        serde_json::from_str(body).map_err(|e| Error::Body(e.to_string()))?;
    let mut out: Vec<Option<Pushed>> = vec![None; items];
    for r in parsed {
        let slot = out
            .get_mut(r.index)
            .ok_or_else(|| Error::Body(format!("push result {} is out of range", r.index)))?;
        if slot.is_some() {
            return Err(Error::Body(format!(
                "push result {} appears twice",
                r.index
            )));
        }
        *slot = Some(r.pushed);
    }
    out.into_iter()
        .enumerate()
        .map(|(i, r)| r.ok_or_else(|| Error::Body(format!("push answered nothing for item {i}"))))
        .collect()
}

/// Reject a `QUEEN_URL` at boot instead of on the first metadata refresh, and
/// strip the trailing slash so paths concatenate to exactly one.
pub fn normalize_base_url(raw: &str) -> std::result::Result<String, String> {
    let trimmed = raw.trim().trim_end_matches('/');
    let url: reqwest::Url = trimmed
        .parse()
        .map_err(|e| format!("QUEEN_URL={raw} is not a URL: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!(
            "QUEEN_URL={raw} has scheme `{}` — the facade speaks HTTP to Queen, so it must be \
             http:// or https://",
            url.scheme()
        ));
    }
    if url.host_str().is_none() {
        return Err(format!("QUEEN_URL={raw} has no host"));
    }
    Ok(trimmed.to_string())
}

// -------------------------------------------------------------------- catalog

/// How long a queue list is served without asking Queen again.
///
/// Every Kafka client refreshes metadata on its own timer (`metadata.max.age.ms`
/// defaults to 5 minutes) and *immediately* on any retriable error, which is how
/// a hiccup turns into every consumer in a fleet asking at the same instant. The
/// window only has to be long enough to collapse that burst into one admin call;
/// past a few seconds it starts hiding real topic creations from clients that
/// just made them. Not a knob: PLAN_QUEEN_KAFKA.md's config surface is the
/// operator's, and this number has no operator-visible effect worth tuning.
const LIST_TTL: Duration = Duration::from_secs(3);

/// The queue list, cached briefly, with single-flight refresh.
///
/// ## Nothing waits behind the admin call
///
/// The refresh is single-flight, and the naive way to get that — hold the map
/// lock across the call — is what makes the whole PROCESS wait on it. Every
/// Produce reads this list (`handlers::produce` needs the partition width before
/// it can stage anything), each Kafka connection is muted until its response is
/// written (conn.rs), and `GET /api/v1/resources/queues` is the enriched form,
/// a pass over the tenant's partitions, under a ten-second budget. Under that
/// arrangement one expiry every [`LIST_TTL`] parks every produce on every
/// connection behind one admin call, on a tenant with enough partitions for
/// seconds at a time — a periodic stall with nothing to do with the writes.
///
/// So the map lock is never held across a call. The refresh is serialised on a
/// SEPARATE per-credential lock, and a caller that finds it taken does not queue
/// behind it: it serves the list it already has, which is a few seconds old at
/// worst and is the same answer [`Catalog::list`] already gives when a refresh
/// FAILS. Only a caller with nothing in hand at all waits — and then it waits
/// for the call already in flight rather than starting a second one, which is
/// the storm collapse this cache exists for, in both directions.
pub struct Catalog {
    api: Arc<dyn QueenApi>,
    ttl: Duration,
    /// Which tenant a credential speaks for, asked once per credential. The
    /// key space of both maps below.
    identities: crate::identity::Identities,
    /// Keyed by TENANT. M1 has exactly one credential (`QUEEN_TOKEN`), but M5
    /// hands every connection its own, and a catalog shared across them would
    /// show one tenant another's topics.
    ///
    /// The key is the tenant Queen names for the credential and, when it names
    /// none, the HASHED credential itself ([`crate::identity::TenantKey`]) —
    /// never the token. This map is process-wide and outlives every connection
    /// that writes to it, so keyed by the raw string it was a permanent record
    /// of every password the facade had been shown, including the refused ones,
    /// which arrive from anyone who can open a socket.
    ///
    /// Filing by tenant rather than by credential is what makes one tenant's
    /// two keys ONE queue list rather than two — the same merge the group
    /// registry does, for the same reason.
    ///
    /// Held for a map read or a map write and never across an await on Queen.
    entries: tokio::sync::Mutex<HashMap<TenantKey, Entry>>,
    /// One refresh lock per tenant — the same key space as `entries`. THIS is
    /// what is held across the call to Queen, and holding it blocks only the
    /// callers that have no list to serve.
    refreshes: tokio::sync::Mutex<HashMap<TenantKey, Arc<tokio::sync::Mutex<()>>>>,
}

/// How many scopes one catalog keeps an answer for.
///
/// The key space is at worst "credentials this facade has been shown" — a
/// resolved tenant merges its own credentials into one entry, but an
/// unresolved one is still its own key — and on a listener with SASL that is a
/// number a peer chooses: every connection may present a different password,
/// and a wrong one costs the attacker one frame. Both maps are therefore
/// bounded and the coldest entry goes when a new one arrives.
/// A thousand is far past any deployment's real credential count — one per
/// tenant, plus whatever a rotation has in flight — and an evicted entry costs
/// its owner one admin call, not an error.
const MAX_CREDENTIALS: usize = 1_024;

/// What the last call to Queen for one credential produced, and when it
/// finished. Both outcomes are kept, and that is the point.
///
/// The success is kept even after it goes stale, so a blip serves a slightly old
/// world instead of an empty one ([`Catalog::list`]). The FAILURE is kept
/// because the single-flight mutex only collapses a storm on the success path:
/// with Queen down and nothing cached, the caller that loses the race used to
/// find no entry, take the lock and repeat the whole 10-second call — sixteen
/// cold connections against a dead Queen were ~160 seconds of dead air, each
/// connection muted for the whole of its turn (conn.rs), rather than one failed
/// refresh. A remembered failure is what makes the storm one call in that
/// direction too.
struct Entry {
    /// The last list that ARRIVED, if one ever has, and when.
    queues: Option<Arc<Vec<Queue>>>,
    listed_at: Option<Instant>,
    /// Set when the LAST call failed; cleared by the next success. Carries the
    /// CREDENTIAL that made the failed call, and is only ever replayed to that
    /// same credential.
    ///
    /// The attribution is what makes a shared entry safe. An entry is now one
    /// TENANT's, so several credentials write to it; a remembered failure is
    /// the credential's, not the tenant's. A key with narrower scopes than its
    /// sibling (a produce-only key answered 403 by the proxy's route
    /// classifier) or one that has just been revoked would otherwise stand in
    /// for every other key of its tenant for a whole TTL — including at
    /// authentication, where [`Catalog::refresh`] reads it and a stranger's
    /// 403 would refuse a valid connection.
    failure: Option<(CredentialKey, Error)>,
    /// When the last call finished, whichever way it went. This is what the TTL
    /// is measured against.
    probed_at: Instant,
    /// When this entry was last READ, which is a different question and is the
    /// one eviction asks ([`make_room`]). A credential in steady use is served
    /// from the cache without a call to Queen, so its `probed_at` sits still
    /// while it is doing exactly what it is here for.
    used_at: Instant,
}

impl Catalog {
    pub fn new(api: Arc<dyn QueenApi>) -> Catalog {
        Catalog::with_ttl(api, LIST_TTL)
    }

    /// Same, with an explicit TTL. For tests, which cannot wait three seconds to
    /// prove that the cache expires.
    pub fn with_ttl(api: Arc<dyn QueenApi>, ttl: Duration) -> Catalog {
        Catalog {
            identities: crate::identity::Identities::new(Arc::clone(&api)),
            api,
            ttl,
            entries: tokio::sync::Mutex::new(HashMap::new()),
            refreshes: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// The scope `token` speaks for, as it is ALREADY known — no call to Queen.
    ///
    /// The connection path's one question ([`crate::Facade::authenticated_as`]):
    /// it runs immediately after [`Catalog::refresh`] has authenticated the
    /// credential and therefore resolved it, and it cannot await. A credential
    /// that was never resolved answers with itself, which is the same key this
    /// catalog would file it under.
    pub fn tenant_key(&self, token: Option<&str>) -> TenantKey {
        self.identities.known(token)
    }

    /// The queue list for `token`, from cache when it is fresh.
    ///
    /// On a refresh failure with a previous list in hand, that list is served
    /// stale and the error is logged. A Kafka client reacts to "topic unknown"
    /// by dropping the topic from its subscription or by re-resolving the
    /// leader, and doing that because the admin API blipped for one second is a
    /// far worse answer than a topic list three seconds old. With nothing
    /// cached, the error is returned and the caller decides the Kafka error code.
    ///
    /// The same reasoning is what makes a refresh already in flight serve the
    /// stale list rather than queue behind it — see the type's header.
    pub async fn list(&self, token: Option<&str>) -> Result<Arc<Vec<Queue>>> {
        // The KNOWN identity and not a resolution: a list is the hot path (every
        // Metadata, every Produce, every Fetch reads it) and it must not be able
        // to make a second HTTP call. Resolution happens once, at
        // authentication — see [`Catalog::refresh`].
        //
        // On a SASL listener that ordering is total: a connection authenticates
        // before it may ask for anything, so every list here is filed under the
        // resolved key. On a listener with NO SASL a list can come first and be
        // filed under the credential, which the first `refresh` then resolves
        // past — leaving one stale entry that costs its owner nothing and is
        // evicted like any other.
        let key = self.identities.known(token);
        let cred = CredentialKey::of(token);
        if let Some(fresh) = self.cached(&key, cred).await {
            return fresh;
        }
        let refresh = self.refresh_lock(&key).await;
        // Ours to run, or someone else's. `try_lock` and not `lock` because
        // that difference IS the fix: a caller that finds the refresh taken
        // must not queue behind it.
        let held = match refresh.try_lock() {
            Ok(held) => held,
            Err(_) => {
                // What we have is at most one TTL plus one call old, and serving
                // it now is the difference between a consumer's poll costing
                // microseconds and costing whatever the admin API costs today.
                if let Some(stale) = self.in_hand(&key).await {
                    return Ok(stale);
                }
                // Nothing to serve. Wait for the call in flight rather than
                // starting a second one, and take its answer.
                refresh.lock().await
            }
        };
        // Re-checked under the lock: the answer to the call we just missed may
        // have landed between the two.
        let answer = match self.cached(&key, cred).await {
            Some(fresh) => fresh,
            None => self.fetch(&key, cred, token, true).await,
        };
        drop(held);
        answer
    }

    /// The queue list as of now, ignoring the TTL — and reporting a failure
    /// rather than falling back to the stale copy.
    ///
    /// The one caller is the auto-create path, and the reason is that
    /// `configure_queue_v1` is an UPSERT: called for a queue that already
    /// exists, it rewrites every config column to the stored procedure's
    /// defaults, so an auto-create decided from a three-second-old list could
    /// silently reset a native queue's leaseTime, retention and dedup window.
    /// There is no create-if-absent on the broker to ask for instead, so the
    /// window is closed as far as it can be: down from the TTL to the length of
    /// one admin call.
    pub async fn refresh(&self, token: Option<&str>) -> Result<Arc<Vec<Queue>>> {
        // THE resolution point. This is the call the SASL check makes
        // (`handlers::sasl_authenticate`), so it is where "which tenant is this
        // credential" is asked — once per credential, before anything is filed
        // under a key that would later have to change. Every other path reads
        // the answer synchronously.
        let key = self.identities.resolve(token).await;
        let cred = CredentialKey::of(token);
        let refresh = self.refresh_lock(&key).await;
        // Waits out a call in flight instead of overlapping one, which is what
        // it did when the map lock was the refresh lock. `list` does not wait
        // here; this path does, because a stale answer is exactly what it may
        // not have.
        let _held = refresh.lock().await;
        // The TTL is deliberately ignored for a SUCCESS — that is the whole
        // reason this method exists. A fresh FAILURE is different: re-running a
        // call that just failed is not a fresher answer, it is the next
        // connection's ten seconds of silence.
        if let Some(e) = self.remembered_failure(&key, cred).await {
            return Err(e);
        }
        self.fetch(&key, cred, token, false).await
    }

    /// The answer for `key` if the last call to Queen finished within the TTL:
    /// the list when there is one, and otherwise the failure that is standing in
    /// for it. `None` means "ask Queen".
    async fn cached(
        &self,
        key: &TenantKey,
        cred: CredentialKey,
    ) -> Option<Result<Arc<Vec<Queue>>>> {
        let mut entries = self.entries.lock().await;
        let entry = entries.get_mut(key)?;
        if entry.probed_at.elapsed() >= self.ttl {
            return None;
        }
        entry.used_at = Instant::now();
        // A list in hand is the answer whether the last call succeeded or
        // failed — the failed one would have served it stale anyway. It is the
        // TENANT's list, so every credential of it gets the same one.
        if let Some(queues) = &entry.queues {
            return Some(Ok(Arc::clone(queues)));
        }
        // Nothing in hand and this credential's last call just failed:
        // repeating it is the storm this cache exists to collapse. Another
        // credential's failure is not this one's — see [`Entry::failure`].
        remembered(entry, cred).map(Err)
    }

    /// The last list that arrived for `key`, at any age.
    async fn in_hand(&self, key: &TenantKey) -> Option<Arc<Vec<Queue>>> {
        let entries = self.entries.lock().await;
        entries.get(key)?.queues.as_ref().map(Arc::clone)
    }

    /// The failure `cred`'s last call produced, while it is still inside the
    /// TTL.
    async fn remembered_failure(&self, key: &TenantKey, cred: CredentialKey) -> Option<Error> {
        let entries = self.entries.lock().await;
        let entry = entries.get(key)?;
        (entry.probed_at.elapsed() < self.ttl)
            .then(|| remembered(entry, cred))
            .flatten()
    }

    /// The refresh lock for one credential, created on first use.
    ///
    /// The map is swept rather than capped: an entry nobody is holding is a
    /// lock nobody is waiting on, so dropping it is free — and an entry that IS
    /// held stays, because two callers holding two different mutexes for one
    /// credential would be two calls to Queen where the point is to have one.
    async fn refresh_lock(&self, key: &TenantKey) -> Arc<tokio::sync::Mutex<()>> {
        let mut refreshes = self.refreshes.lock().await;
        if refreshes.len() >= MAX_CREDENTIALS && !refreshes.contains_key(key) {
            refreshes.retain(|_, lock| Arc::strong_count(lock) > 1);
        }
        Arc::clone(refreshes.entry(key.clone()).or_default())
    }

    /// Ask Queen and store the answer. `fall_back_to_stale` decides what a
    /// failure means: for [`Catalog::list`] a slightly old world, for
    /// [`Catalog::refresh`] an error.
    ///
    /// The caller holds the credential's refresh lock; the map lock is taken
    /// twice here and never across the call.
    async fn fetch(
        &self,
        key: &TenantKey,
        cred: CredentialKey,
        token: Option<&str>,
        fall_back_to_stale: bool,
    ) -> Result<Arc<Vec<Queue>>> {
        // Read before the call so a failure does not forget the last good list.
        let previous = self
            .entries
            .lock()
            .await
            .get(key)
            .and_then(|e| e.queues.as_ref().map(|q| (Arc::clone(q), e.listed_at)));

        match self.api.list_queues(token).await {
            Ok(queues) => {
                let queues = Arc::new(queues);
                let now = Instant::now();
                let mut entries = self.entries.lock().await;
                make_room(&mut entries, key);
                entries.insert(
                    key.clone(),
                    Entry {
                        queues: Some(Arc::clone(&queues)),
                        listed_at: Some(now),
                        failure: None,
                        probed_at: now,
                        used_at: now,
                    },
                );
                drop(entries);
                Ok(queues)
            }
            Err(e) => {
                // The failure is remembered whether or not it is served, so the
                // callers behind this one do not each pay for it — see `Entry`.
                let mut entries = self.entries.lock().await;
                make_room(&mut entries, key);
                entries.insert(
                    key.clone(),
                    Entry {
                        queues: previous.as_ref().map(|(q, _)| Arc::clone(q)),
                        listed_at: previous.as_ref().and_then(|(_, at)| *at),
                        failure: Some((cred, e.clone())),
                        probed_at: Instant::now(),
                        used_at: Instant::now(),
                    },
                );
                drop(entries);
                match previous.filter(|_| fall_back_to_stale) {
                    Some((stale, listed_at)) => {
                        tracing::warn!(
                            target: "kafka",
                            error = %e,
                            age_ms = listed_at
                                .map(|at| at.elapsed().as_millis() as u64)
                                .unwrap_or(0),
                            "queue list refresh failed; serving the last one"
                        );
                        Ok(stale)
                    }
                    None => Err(e),
                }
            }
        }
    }

    /// Create a queue and drop the cached list, so the next [`Catalog::list`]
    /// sees it. The caller does not wait for that refresh: it already knows the
    /// name it just created.
    pub async fn create(&self, name: &str, token: Option<&str>) -> Result<()> {
        self.api.create_queue(name, token).await?;
        // The tenant's entry, so the sibling credentials of the key that
        // created the topic see it too rather than each waiting out a TTL.
        let key = self.identities.known(token);
        self.entries.lock().await.remove(&key);
        Ok(())
    }
}

/// The failure `cred`'s own last call left on `entry`, if that is whose it was.
///
/// A `None` here is not "no failure": it can also be "somebody else's". Both
/// mean the same thing to every caller — this credential has to make the call
/// itself — which is why one function answers both. See [`Entry::failure`].
fn remembered(entry: &Entry, cred: CredentialKey) -> Option<Error> {
    entry
        .failure
        .as_ref()
        .filter(|(who, _)| *who == cred)
        .map(|(_, e)| e.clone())
}

/// Drop the coldest entry if `key` would take the map past its bound.
///
/// "Coldest" is least recently READ and not least recently refreshed: a
/// credential doing its job is answered from the cache without a call to Queen,
/// and evicting the entries that are working while keeping the ones that are
/// not is the wrong way round.
fn make_room(entries: &mut HashMap<TenantKey, Entry>, key: &TenantKey) {
    if entries.len() < MAX_CREDENTIALS || entries.contains_key(key) {
        return;
    }
    if let Some(coldest) = entries
        .iter()
        .min_by_key(|(_, e)| e.used_at)
        .map(|(k, _)| k.clone())
    {
        entries.remove(&coldest);
    }
}

/// The test double, shared with `handlers::metadata`'s tests: the auto-create
/// policy is decided there and executed here, and both halves are worth testing
/// against something that records what it was asked rather than a broker.
#[cfg(test)]
pub mod testing {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// A [`QueenApi`] that answers from a script and counts what it was asked.
    pub struct FakeQueen {
        pub queues: Mutex<Vec<Queue>>,
        pub lists: AtomicUsize,
        pub creates: Mutex<Vec<String>>,
        pub tokens: Mutex<Vec<Option<String>>>,
        /// When set, every call fails with it.
        pub fail: Mutex<Option<String>>,
        /// When set, the next `list_queues` fails with exactly this error —
        /// the credential check of [`crate::handlers::sasl_authenticate`] needs
        /// failures `fail` cannot express.
        pub list_error: Mutex<Option<Error>>,
        /// When set, `list_queues` answers 401 to every credential but this
        /// one. The double's whole model of "a token Queen accepts".
        pub only_token: Mutex<Option<String>>,
        /// The `Host` each call was made through, if it went through a
        /// host-scoped view ([`FakeQueen::with_host`]).
        pub hosts: Mutex<Vec<String>>,
        /// Every push, as it was sent.
        pub pushes: Mutex<Vec<Vec<PushItem>>>,
        /// When set, the push fails with exactly this error — the status-code
        /// mapping needs failures `fail` cannot express.
        pub push_error: Mutex<Option<Error>>,
        /// When set, a push answers this instead of allocating offsets, so the
        /// broker answers a produce handler cannot produce on its own (a
        /// missing offset, an errored item) are reachable in a test.
        pub push_reply: Mutex<Option<Vec<Pushed>>>,
        /// Every fetch, as it was sent: the entries plus the long-poll pair.
        pub fetches: Mutex<Vec<(Vec<FetchEntry>, i64, i64)>>,
        /// When set, the next fetch answers this verbatim.
        pub fetch_reply: Mutex<Option<Vec<Fetched>>>,
        /// When set, the next fetch fails with exactly this error.
        pub fetch_error: Mutex<Option<Error>>,
        /// The log itself, per (queue, partition): the payloads in offset order
        /// and the retention watermark below them. This is what makes the
        /// double behave like a log rather than a script — a push appends here
        /// and a fetch reads it back, so the produce and fetch handlers can be
        /// tested against each other.
        logs: Mutex<HashMap<(String, String), Lane>>,
        /// Every KV call, as it was sent.
        pub kv_calls: Mutex<Vec<Vec<KvOp>>>,
        /// When set, the next KV call fails with exactly this error.
        pub kv_error: Mutex<Option<Error>>,
        /// The KV store itself, per (namespace, key) — the same "behave like the
        /// thing, not like a script" as `logs`, so a commit and the fetch that
        /// reads it back can be tested against each other.
        ///
        /// A `BTreeMap`, and that is load-bearing rather than tidy: the stored
        /// procedure's key column is `COLLATE "C"`, so a prefix read comes back
        /// in BYTE order and pages with a cursor in that order. A `HashMap` here
        /// would let a paging bug pass.
        kv: Mutex<std::collections::BTreeMap<(String, String), serde_json::Value>>,
        /// Ceiling on the rows ONE read answers before it truncates, standing in
        /// for the stored procedure's 4 MiB byte budget ([`MAX_KV_READ_BYTES`]),
        /// which no test can reach with realistic values. `None` is "never
        /// truncate".
        kv_read_rows: Mutex<Option<usize>>,
        /// What `GET /auth/me` answers, per credential. A token with no entry
        /// here is answered `Ok(None)` — "answered, named no tenant", which is
        /// what every deployment's identity surface tells a bearer today, so
        /// the default keeps every other test on the pre-identity behaviour.
        identities: Mutex<HashMap<Option<String>, Result<Option<String>>>>,
        /// The credential every identity call was made with, in call order.
        identity_calls: Mutex<Vec<Option<String>>>,
    }

    /// One partition of the fake log.
    #[derive(Default)]
    struct Lane {
        /// The offset of `payloads[0]`; everything below it was retained away.
        start: i64,
        payloads: Vec<serde_json::Value>,
    }

    impl Lane {
        fn high(&self) -> i64 {
            self.start + self.payloads.len() as i64
        }
    }

    impl FakeQueen {
        pub fn with(queues: &[(&str, i64)]) -> Arc<FakeQueen> {
            Arc::new(FakeQueen {
                queues: Mutex::new(
                    queues
                        .iter()
                        .map(|(n, p)| Queue {
                            name: n.to_string(),
                            partitions: *p,
                        })
                        .collect(),
                ),
                lists: AtomicUsize::new(0),
                creates: Mutex::new(Vec::new()),
                tokens: Mutex::new(Vec::new()),
                fail: Mutex::new(None),
                list_error: Mutex::new(None),
                only_token: Mutex::new(None),
                hosts: Mutex::new(Vec::new()),
                pushes: Mutex::new(Vec::new()),
                push_error: Mutex::new(None),
                push_reply: Mutex::new(None),
                fetches: Mutex::new(Vec::new()),
                fetch_reply: Mutex::new(None),
                fetch_error: Mutex::new(None),
                logs: Mutex::new(HashMap::new()),
                kv_calls: Mutex::new(Vec::new()),
                kv_error: Mutex::new(None),
                kv: Mutex::new(std::collections::BTreeMap::new()),
                kv_read_rows: Mutex::new(None),
                identities: Mutex::new(HashMap::new()),
                identity_calls: Mutex::new(Vec::new()),
            })
        }

        /// `/auth/me` names `tenant` as the cluster `token` acts on — the
        /// answer a surface that identifies bearers would give.
        pub fn answer_identity(&self, token: Option<&str>, tenant: &str) {
            self.identities
                .lock()
                .unwrap()
                .insert(token.map(str::to_string), Ok(Some(tenant.to_string())));
        }

        /// `/auth/me` fails for `token` — a refusal, or an unreachable Queen.
        pub fn fail_identity(&self, token: Option<&str>, e: Error) {
            self.identities
                .lock()
                .unwrap()
                .insert(token.map(str::to_string), Err(e));
        }

        /// Every credential `/auth/me` was asked about, in call order.
        pub fn identity_calls(&self) -> Vec<Option<String>> {
            self.identity_calls.lock().unwrap().clone()
        }

        /// Put `payloads` into a lane at `start`, as if a native producer had.
        /// The interop half of the envelope contract needs a log the facade did
        /// not write, and retention below `start` is what makes an
        /// OFFSET_OUT_OF_RANGE reachable.
        pub fn seed(
            &self,
            queue: &str,
            partition: &str,
            start: i64,
            payloads: &[serde_json::Value],
        ) {
            self.logs.lock().unwrap().insert(
                (queue.to_string(), partition.to_string()),
                Lane {
                    start,
                    payloads: payloads.to_vec(),
                },
            );
        }

        /// The next fetch answers `entries` verbatim.
        pub fn reply_fetch(&self, entries: Vec<Fetched>) {
            *self.fetch_reply.lock().unwrap() = Some(entries);
        }

        /// The next fetch fails with `e`.
        pub fn fail_fetch(&self, e: Error) {
            *self.fetch_error.lock().unwrap() = Some(e);
        }

        /// Every fetch's entries, flattened.
        pub fn fetched(&self) -> Vec<FetchEntry> {
            self.fetches
                .lock()
                .unwrap()
                .iter()
                .flat_map(|(entries, _, _)| entries.clone())
                .collect()
        }

        pub fn list_count(&self) -> usize {
            self.lists.load(Ordering::SeqCst)
        }

        pub fn created(&self) -> Vec<String> {
            self.creates.lock().unwrap().clone()
        }

        pub fn fail_with(&self, why: &str) {
            *self.fail.lock().unwrap() = Some(why.to_string());
        }

        /// Everything one push carried, flattened.
        pub fn pushed(&self) -> Vec<PushItem> {
            self.pushes
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .cloned()
                .collect()
        }

        /// The next push fails with `e` instead of writing.
        pub fn fail_push(&self, e: Error) {
            *self.push_error.lock().unwrap() = Some(e);
        }

        /// The next push answers `results` verbatim.
        pub fn reply_push(&self, results: Vec<Pushed>) {
            *self.push_reply.lock().unwrap() = Some(results);
        }

        /// Put a key into the store, as if something else had written it.
        pub fn kv_seed(&self, ns: &str, key: &str, value: serde_json::Value) {
            self.kv
                .lock()
                .unwrap()
                .insert((ns.to_string(), key.to_string()), value);
        }

        /// What the store holds for one key.
        pub fn kv_get(&self, ns: &str, key: &str) -> Option<serde_json::Value> {
            self.kv
                .lock()
                .unwrap()
                .get(&(ns.to_string(), key.to_string()))
                .cloned()
        }

        /// The store, in byte order.
        pub fn kv_keys(&self) -> Vec<String> {
            self.kv
                .lock()
                .unwrap()
                .keys()
                .map(|(_, k)| k.clone())
                .collect()
        }

        /// Every KV operation, flattened.
        pub fn kv_ops(&self) -> Vec<KvOp> {
            self.kv_calls
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .cloned()
                .collect()
        }

        /// The next KV call fails with `e`.
        pub fn fail_kv(&self, e: Error) {
            *self.kv_error.lock().unwrap() = Some(e);
        }

        /// Truncate every read at `rows`. See [`FakeQueen::kv_read_rows`].
        pub fn kv_truncate_reads_at(&self, rows: usize) {
            *self.kv_read_rows.lock().unwrap() = Some(rows);
        }

        /// The next `list_queues` fails with `e`.
        pub fn fail_list(&self, e: Error) {
            *self.list_error.lock().unwrap() = Some(e);
        }

        /// `token` is the only credential `list_queues` accepts; everything
        /// else is a 401, exactly as the broker's auth layer answers one.
        pub fn accept_only(&self, token: &str) {
            *self.only_token.lock().unwrap() = Some(token.to_string());
        }

        /// Every `Host` a call was made through, in call order.
        pub fn hosts(&self) -> Vec<String> {
            self.hosts.lock().unwrap().clone()
        }

        fn note_host(&self, host: &str) {
            self.hosts.lock().unwrap().push(host.to_string());
        }
    }

    /// The double behind an optional `Host` header: it records the name and
    /// forwards everything to the [`FakeQueen`] behind it.
    ///
    /// The real client's `with_host` clones a `reqwest::Client` and changes one
    /// header, which is invisible to a test that has no HTTP. This makes it
    /// visible: what a lane is FOR is that a connection's calls carry the name
    /// it asked for, and this is where that can be asserted without a socket.
    /// [`FakeQueen`] itself answers `with_host` with `None` — the honest
    /// "nothing to stamp" — so a test that wants lanes starts from
    /// [`Routed::over`].
    pub struct Routed {
        host: Option<String>,
        inner: Arc<FakeQueen>,
    }

    impl Routed {
        /// The root view: no `Host` of its own, and able to make ones that do.
        pub fn over(inner: Arc<FakeQueen>) -> Arc<Routed> {
            Arc::new(Routed { host: None, inner })
        }

        fn note(&self) {
            if let Some(host) = &self.host {
                self.inner.note_host(host);
            }
        }
    }

    impl QueenApi for Routed {
        fn list_queues<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Vec<Queue>>> {
            self.note();
            self.inner.list_queues(token)
        }

        fn create_queue<'a>(
            &'a self,
            name: &'a str,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<()>> {
            self.note();
            self.inner.create_queue(name, token)
        }

        fn push<'a>(
            &'a self,
            items: &'a [PushItem],
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Vec<Pushed>>> {
            self.note();
            self.inner.push(items, token)
        }

        fn fetch<'a>(
            &'a self,
            entries: &'a [FetchEntry],
            max_wait_ms: i64,
            min_bytes: i64,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Vec<Fetched>>> {
            self.note();
            self.inner.fetch(entries, max_wait_ms, min_bytes, token)
        }

        fn kv<'a>(
            &'a self,
            ops: &'a [KvOp],
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Vec<KvAnswer>>> {
            self.note();
            self.inner.kv(ops, token)
        }

        fn identity<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Option<String>>> {
            self.note();
            self.inner.identity(token)
        }

        fn with_host(&self, host: &str) -> Option<Arc<dyn QueenApi>> {
            Some(Arc::new(Routed {
                host: Some(host.to_string()),
                inner: Arc::clone(&self.inner),
            }))
        }
    }

    impl QueenApi for FakeQueen {
        fn list_queues<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Vec<Queue>>> {
            Box::pin(async move {
                self.lists.fetch_add(1, Ordering::SeqCst);
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                if let Some(e) = self.list_error.lock().unwrap().take() {
                    return Err(e);
                }
                // The broker's own answer to a credential it does not know
                // (server/src/auth.rs `auth_middleware`), which is what the
                // SASL check reads.
                if let Some(want) = self.only_token.lock().unwrap().as_deref() {
                    if token != Some(want) {
                        return Err(Error::status(401, "Authentication required"));
                    }
                }
                match self.fail.lock().unwrap().clone() {
                    Some(e) => Err(Error::Transport(e)),
                    None => Ok(self.queues.lock().unwrap().clone()),
                }
            })
        }

        /// `GET /auth/me`, from the script. An unscripted credential is
        /// answered "names no tenant", which is what both surfaces tell a
        /// bearer today — so a test that is about something else keeps the
        /// per-credential scoping it was written against.
        ///
        /// Deliberately NOT gated on `fail`: that stands for a broken data
        /// path, and the identity call has to be able to succeed while the
        /// queue list fails (and the other way round) or the two halves of
        /// authentication cannot be tested apart.
        fn identity<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Option<String>>> {
            Box::pin(async move {
                let asked = token.map(str::to_string);
                self.identity_calls.lock().unwrap().push(asked.clone());
                self.identities
                    .lock()
                    .unwrap()
                    .get(&asked)
                    .cloned()
                    .unwrap_or(Ok(None))
            })
        }

        fn create_queue<'a>(
            &'a self,
            name: &'a str,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                if let Some(e) = self.fail.lock().unwrap().clone() {
                    return Err(Error::Transport(e));
                }
                self.creates.lock().unwrap().push(name.to_string());
                self.queues.lock().unwrap().push(Queue {
                    name: name.to_string(),
                    partitions: 0,
                });
                Ok(())
            })
        }

        fn push<'a>(
            &'a self,
            items: &'a [PushItem],
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Vec<Pushed>>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                self.pushes.lock().unwrap().push(items.to_vec());
                if let Some(e) = self.push_error.lock().unwrap().take() {
                    return Err(e);
                }
                if let Some(e) = self.fail.lock().unwrap().clone() {
                    return Err(Error::Transport(e));
                }
                if let Some(scripted) = self.push_reply.lock().unwrap().take() {
                    return Ok(scripted);
                }
                let mut logs = self.logs.lock().unwrap();
                Ok(items
                    .iter()
                    .map(|it| {
                        let lane = logs
                            .entry((it.queue.clone(), it.partition.clone()))
                            .or_default();
                        let offset = lane.high();
                        lane.payloads.push(it.payload.clone());
                        Pushed {
                            status: "queued".to_string(),
                            offset: Some(offset),
                        }
                    })
                    .collect())
            })
        }

        /// Reads the fake log with C2's own offset rules
        /// (server/sql/procedures/032_log_fetch.sql): a queue this fake does
        /// not have is UNKNOWN, a lane nothing was pushed to is an EMPTY log
        /// rather than a missing one, `offset == high` is valid and empty, and
        /// anything outside `[logStart, high]` is OFFSET_OUT_OF_RANGE with the
        /// bounds still reported.
        fn fetch<'a>(
            &'a self,
            entries: &'a [FetchEntry],
            max_wait_ms: i64,
            min_bytes: i64,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Vec<Fetched>>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                self.fetches
                    .lock()
                    .unwrap()
                    .push((entries.to_vec(), max_wait_ms, min_bytes));
                if let Some(e) = self.fetch_error.lock().unwrap().take() {
                    return Err(e);
                }
                if let Some(e) = self.fail.lock().unwrap().clone() {
                    return Err(Error::Transport(e));
                }
                if let Some(scripted) = self.fetch_reply.lock().unwrap().take() {
                    return Ok(scripted);
                }
                let queues = self.queues.lock().unwrap();
                let logs = self.logs.lock().unwrap();
                Ok(entries
                    .iter()
                    .map(|e| {
                        if !queues.iter().any(|q| q.name == e.queue) {
                            return Fetched {
                                records: Vec::new(),
                                high_watermark: 0,
                                log_start_offset: 0,
                                error: Some(FETCH_ERR_UNKNOWN.to_string()),
                            };
                        }
                        let empty = Lane::default();
                        let lane = logs
                            .get(&(e.queue.clone(), e.partition.clone()))
                            .unwrap_or(&empty);
                        let (start, high) = (lane.start, lane.high());
                        if e.offset < start || e.offset > high {
                            return Fetched {
                                records: Vec::new(),
                                high_watermark: high,
                                log_start_offset: start,
                                error: Some(FETCH_ERR_OUT_OF_RANGE.to_string()),
                            };
                        }
                        let records = lane
                            .payloads
                            .iter()
                            .enumerate()
                            .skip((e.offset - start) as usize)
                            .map(|(i, payload)| FetchedRecord {
                                offset: start + i as i64,
                                payload: payload.clone(),
                                ts: Some("2026-08-27T10:00:00.000000Z".to_string()),
                            })
                            .collect();
                        Fetched {
                            records,
                            high_watermark: high,
                            log_start_offset: start,
                            error: None,
                        }
                    })
                    .collect())
            })
        }

        /// Applies the ops with `kv_apply_v1`'s own rules
        /// (server/sql/procedures/024_kv.sql): a `put` is an unconditional
        /// upsert that always applies, a `getMany` reports `rows` AND
        /// `missing`, and a `getPrefix` pages an exclusive `after` cursor in
        /// BYTE order, setting `truncated` with a `nextAfter` when the page
        /// limit — or the read budget, standing in as a row count here — cuts
        /// it short.
        fn kv<'a>(
            &'a self,
            ops: &'a [KvOp],
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Vec<KvAnswer>>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                self.kv_calls.lock().unwrap().push(ops.to_vec());
                if let Some(e) = self.kv_error.lock().unwrap().take() {
                    return Err(e);
                }
                if let Some(e) = self.fail.lock().unwrap().clone() {
                    return Err(Error::Transport(e));
                }
                // The two ceilings the broker refuses the WHOLE batch over. A
                // double that accepted them would let a caller ship a batch the
                // broker answers 400 to (server/src/handlers/kv.rs).
                if ops.len() > MAX_KV_OPS_PER_CALL {
                    return Err(Error::status(400, format!("{} ops in one call", ops.len())));
                }
                let keys: usize = ops.iter().map(KvOp::keys).sum();
                if keys > MAX_KV_KEYS_PER_CALL {
                    return Err(Error::status(400, format!("{keys} keys in one call")));
                }
                let cut = *self.kv_read_rows.lock().unwrap();
                let mut kv = self.kv.lock().unwrap();
                Ok(ops
                    .iter()
                    .enumerate()
                    .map(|(index, op)| match op {
                        KvOp::Put { ns, key, value, .. } => {
                            kv.insert((ns.clone(), key.clone()), value.clone());
                            KvAnswer {
                                applied: Some(true),
                                ..empty_answer(index, "put")
                            }
                        }
                        KvOp::GetMany { ns, keys } => {
                            let mut rows = Vec::new();
                            let mut missing = Vec::new();
                            let mut truncated = false;
                            // Sorted, because the stored procedure returns rows
                            // ordered by key and spends its byte budget in that
                            // order — which is what decides WHICH keys a
                            // truncated read drops.
                            let mut sorted: Vec<&String> = keys.iter().collect();
                            sorted.sort();
                            for key in sorted {
                                match kv.get(&(ns.clone(), key.clone())) {
                                    Some(value) => {
                                        if cut.is_some_and(|c| rows.len() >= c) {
                                            truncated = true;
                                            continue;
                                        }
                                        rows.push(KvRow {
                                            key: key.clone(),
                                            value: value.clone(),
                                        });
                                    }
                                    None => missing.push(key.clone()),
                                }
                            }
                            KvAnswer {
                                rows,
                                missing,
                                truncated,
                                ..empty_answer(index, "getMany")
                            }
                        }
                        KvOp::GetPrefix {
                            ns,
                            prefix,
                            limit,
                            after,
                        } => {
                            let limit = (*limit).clamp(1, MAX_KV_PREFIX_LIMIT) as usize;
                            let limit = cut.map_or(limit, |c| limit.min(c));
                            let mut rows = Vec::new();
                            let mut truncated = false;
                            for ((row_ns, key), value) in kv.iter() {
                                if row_ns != ns || !key.starts_with(prefix) {
                                    continue;
                                }
                                // Exclusive, and in byte order: the same cursor
                                // the SP implements with `key > after`.
                                if after.as_ref().is_some_and(|a| key <= a) {
                                    continue;
                                }
                                if rows.len() == limit {
                                    truncated = true;
                                    break;
                                }
                                rows.push(KvRow {
                                    key: key.clone(),
                                    value: value.clone(),
                                });
                            }
                            let next_after = truncated
                                .then(|| rows.last().map(|r| r.key.clone()))
                                .flatten();
                            KvAnswer {
                                rows,
                                truncated,
                                next_after,
                                ..empty_answer(index, "getPrefix")
                            }
                        }
                    })
                    .collect())
            })
        }
    }

    /// A result with nothing in it but its identity.
    fn empty_answer(index: usize, op: &str) -> KvAnswer {
        KvAnswer {
            index,
            op: op.to_string(),
            applied: None,
            rows: Vec::new(),
            missing: Vec::new(),
            truncated: false,
            next_after: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::FakeQueen;
    use super::*;
    use std::sync::atomic::Ordering;

    // ------------------------------------------------------------ parsing

    /// The exact body `GET /api/v1/resources/queues` serves, trimmed of nothing:
    /// get_queues_v2's per-queue object plus the live `partitions`/`segments`
    /// enrichment and the top-level kv/timer byte counters
    /// (server/src/handlers/queues.rs). Unknown fields must be ignored, not
    /// rejected, or a dashboard-driven addition to the response breaks the
    /// facade.
    const REAL_LIST_BODY: &str = r#"{
      "queues": [
        {"id":"0d5a1e9c-1f7f-4f2f-9a02-6b6b1f0b1a11","name":"orders","namespace":"","task":"",
         "createdAt":"2026-08-27T10:00:00.000Z","partitions":12,"retainedBytes":4096,
         "segments":{"segments":3,"messages":900},
         "messages":{"total":900,"pending":10,"processing":2}},
        {"id":"1d5a1e9c-1f7f-4f2f-9a02-6b6b1f0b1a12","name":"clicks","namespace":"ns","task":"t",
         "createdAt":"2026-08-27T10:00:01.000Z","partitions":0,"retainedBytes":0,
         "messages":{"total":0,"pending":0,"processing":0}}
      ],
      "kvBytes": 0, "timerBytes": 0
    }"#;

    #[test]
    fn the_queue_list_body_parses_to_names_and_partition_counts() {
        let parsed: QueueListBody = serde_json::from_str(REAL_LIST_BODY).unwrap();
        assert_eq!(
            parsed.queues,
            vec![
                Queue {
                    name: "orders".into(),
                    partitions: 12
                },
                Queue {
                    name: "clicks".into(),
                    partitions: 0
                },
            ]
        );
    }

    #[test]
    fn an_empty_broker_parses_to_an_empty_list() {
        let parsed: QueueListBody = serde_json::from_str(r#"{"queues":[]}"#).unwrap();
        assert!(parsed.queues.is_empty());
    }

    // ---------------------------------------------------------------- fetch

    /// The exact body `POST /api/v1/fetch` serves (server/src/handlers/fetch.rs,
    /// `render_fetch`): one entry per request entry, naming its own queue and
    /// partition, with `transactionId` and the bounds beside the records.
    const REAL_FETCH_BODY: &str = r#"{"entries":[
      {"queue":"orders","partition":"3","records":[
        {"offset":41,"transactionId":"t1","payload":{"k":null,"v":"b25l"},"ts":"2026-08-27T10:00:00.123456Z"},
        {"offset":42,"transactionId":"t2","payload":null,"ts":"2026-08-27T10:00:00.123456Z"}
       ],"highWatermark":43,"logStartOffset":7},
      {"queue":"clicks","partition":"0","records":[],"highWatermark":0,"logStartOffset":0,
       "error":"UNKNOWN_TOPIC_OR_PARTITION"}
    ]}"#;

    fn fetch_entries() -> Vec<FetchEntry> {
        vec![
            FetchEntry {
                queue: "orders".into(),
                partition: "3".into(),
                offset: 41,
                max_bytes: 1024,
            },
            FetchEntry {
                queue: "clicks".into(),
                partition: "0".into(),
                offset: 0,
                max_bytes: 1024,
            },
        ]
    }

    #[test]
    fn the_fetch_body_parses_to_records_bounds_and_errors() {
        let got = align_fetch_results(REAL_FETCH_BODY, &fetch_entries()).unwrap();
        assert_eq!(got.len(), 2);

        assert_eq!(got[0].high_watermark, 43);
        assert_eq!(got[0].log_start_offset, 7);
        assert_eq!(got[0].error, None);
        assert_eq!(got[0].records.len(), 2);
        assert_eq!(got[0].records[0].offset, 41);
        assert_eq!(got[0].records[0].payload["v"], "b25l");
        // An empty frame renders as a null payload, and that has to parse
        // rather than fail the entry.
        assert_eq!(got[0].records[1].payload, serde_json::Value::Null);
        assert_eq!(
            got[0].records[0].timestamp_ms(),
            Some(1_787_824_800_123),
            "the segment timestamp did not become epoch millis"
        );

        assert_eq!(got[1].error.as_deref(), Some(FETCH_ERR_UNKNOWN));
        assert!(got[1].records.is_empty());
    }

    /// An entry answered for a different lane is an error, not records read
    /// against the wrong partition.
    #[test]
    fn a_misaligned_fetch_answer_is_an_error() {
        for body in [
            // One entry for two asked.
            r#"{"entries":[{"queue":"orders","partition":"3","records":[],"highWatermark":0,"logStartOffset":0}]}"#,
            // The right count, the wrong lane.
            r#"{"entries":[
                 {"queue":"orders","partition":"4","records":[],"highWatermark":0,"logStartOffset":0},
                 {"queue":"clicks","partition":"0","records":[],"highWatermark":0,"logStartOffset":0}]}"#,
            // The right lanes, swapped.
            r#"{"entries":[
                 {"queue":"clicks","partition":"0","records":[],"highWatermark":0,"logStartOffset":0},
                 {"queue":"orders","partition":"3","records":[],"highWatermark":0,"logStartOffset":0}]}"#,
            // Not the shape at all.
            r#"{"error":"fetch failed"}"#,
        ] {
            assert!(
                align_fetch_results(body, &fetch_entries()).is_err(),
                "{body} was accepted"
            );
        }
    }

    /// The long poll has to outlive the client's own budget, or a consumer
    /// that asked to wait gets a transport error where the answer was "nothing
    /// yet".
    #[test]
    fn a_parked_fetch_gets_the_park_on_top_of_the_normal_budget() {
        assert_eq!(fetch_timeout(0), REQUEST_TIMEOUT);
        assert_eq!(
            fetch_timeout(500),
            REQUEST_TIMEOUT + Duration::from_millis(500)
        );
        assert!(
            fetch_timeout(MAX_FETCH_WAIT_MS) > Duration::from_millis(MAX_FETCH_WAIT_MS as u64),
            "a full park would be cut short by its own timeout"
        );
        // A value above what the broker honours cannot buy more budget, and a
        // negative one cannot buy less.
        assert_eq!(
            fetch_timeout(i64::MAX),
            fetch_timeout(MAX_FETCH_WAIT_MS),
            "an over-long park bought extra budget"
        );
        assert_eq!(fetch_timeout(-5), REQUEST_TIMEOUT);
    }

    #[test]
    fn the_segment_timestamp_parses_or_refuses() {
        // The epoch itself, and the shape the broker renders.
        assert_eq!(epoch_millis("1970-01-01T00:00:00.000000Z"), Some(0));
        assert_eq!(epoch_millis("1970-01-01T00:00:00.999999Z"), Some(999));
        assert_eq!(
            epoch_millis("2026-08-27T10:00:00.123456Z"),
            Some(1_787_824_800_123)
        );
        // A leap day, which is where naive day arithmetic breaks.
        assert_eq!(
            epoch_millis("2024-02-29T00:00:00.000000Z"),
            Some(1_709_164_800_000)
        );
        // 2000 is a leap year and 1900 is not: the century rule, exercised.
        assert_eq!(
            epoch_millis("2000-03-01T00:00:00.000000Z"),
            Some(951_868_800_000)
        );
        assert_eq!(epoch_millis("1969-12-31T23:59:59.000000Z"), Some(-1_000));
        // Sub-second precision is optional; the `Z` is too.
        assert_eq!(
            epoch_millis("2026-08-27T10:00:00Z"),
            Some(1_787_824_800_000)
        );
        assert_eq!(
            epoch_millis("2026-08-27T10:00:00.5Z"),
            Some(1_787_824_800_500)
        );

        for bad in [
            "",
            "2026-08-27",
            "2026-08-27 10:00:00Z",
            "26-08-27T10:00:00Z",
            "2026-8-27T10:00:00Z",
            "2026-08-27T10:00Z",
            "2026-13-01T00:00:00Z",
            "2026-08-27T24:00:00Z",
            "2026-08-27T10:00:00.abcZ",
            "not a timestamp",
        ] {
            assert_eq!(epoch_millis(bad), None, "{bad} was parsed");
        }
    }

    /// The fake log obeys C2's offset rules, which is what every handler test
    /// downstream is written against.
    #[tokio::test]
    async fn the_double_reads_back_what_was_pushed() {
        let api = FakeQueen::with(&[("orders", 1)]);
        api.push(
            &[
                PushItem {
                    queue: "orders".into(),
                    partition: "0".into(),
                    payload: serde_json::json!({"k": null, "v": "b25l"}),
                },
                PushItem {
                    queue: "orders".into(),
                    partition: "0".into(),
                    payload: serde_json::json!({"k": null, "v": "dHdv"}),
                },
            ],
            None,
        )
        .await
        .unwrap();

        let entry = |queue: &str, partition: &str, offset: i64| FetchEntry {
            queue: queue.into(),
            partition: partition.into(),
            offset,
            max_bytes: 1024,
        };

        let got = api
            .fetch(&[entry("orders", "0", 0)], 0, 1, None)
            .await
            .unwrap();
        assert_eq!(got[0].records.len(), 2);
        assert_eq!(got[0].records[0].offset, 0);
        assert_eq!(got[0].high_watermark, 2);

        // Caught up: valid, and empty.
        let caught_up = api
            .fetch(&[entry("orders", "0", 2)], 0, 1, None)
            .await
            .unwrap();
        assert!(caught_up[0].records.is_empty());
        assert_eq!(caught_up[0].error, None);

        // Past the end, an unwritten lane, and a queue that is not there.
        let bounds = api
            .fetch(
                &[
                    entry("orders", "0", 3),
                    entry("orders", "9", 0),
                    entry("nope", "0", 0),
                ],
                0,
                1,
                None,
            )
            .await
            .unwrap();
        assert_eq!(bounds[0].error.as_deref(), Some(FETCH_ERR_OUT_OF_RANGE));
        assert_eq!(bounds[0].high_watermark, 2);
        assert_eq!(bounds[1].error, None, "an unwritten lane is an empty log");
        assert_eq!(bounds[1].high_watermark, 0);
        assert_eq!(bounds[2].error.as_deref(), Some(FETCH_ERR_UNKNOWN));
    }

    // ------------------------------------------------------------------- kv

    /// The exact body `POST /api/v1/kv` serves for a put, a getMany and a
    /// getPrefix (server/src/handlers/kv.rs `batch_response`, over the elements
    /// `kv_apply_v1` builds). Note the results are NOT in operation order — the
    /// stored procedure applies writes in key order and reports by ordinal —
    /// which is exactly what `index` is for.
    const REAL_KV_BODY: &str = r#"{"results":[
      {"index":1,"op":"getMany","rows":[
         {"key":"qk:group:g:orders:0","value":{"offset":41,"metadata":"","ts":1787824800123},
          "version":91005,"expiresAt":null,"updatedAt":"2026-08-27T10:00:00.123456Z"}],
       "missing":["qk:group:g:orders:1"],"truncated":false},
      {"index":0,"op":"put","applied":true,"key":"qk:group:g:orders:0",
       "value":{"offset":41},"version":91005},
      {"index":2,"op":"getPrefix","rows":[
         {"key":"qk:group:g:clicks:7","value":{"offset":3,"metadata":"m","ts":1},
          "version":91006,"expiresAt":null,"updatedAt":"2026-08-27T10:00:00.123456Z"}],
       "truncated":true,"nextAfter":"qk:group:g:clicks:7"}
    ]}"#;

    #[test]
    fn the_kv_body_parses_and_realigns_to_the_operations() {
        let got = align_kv_results(REAL_KV_BODY, 3).unwrap();

        assert_eq!(got[0].op, "put");
        assert_eq!(got[0].applied, Some(true));

        assert_eq!(got[1].op, "getMany");
        assert_eq!(got[1].rows.len(), 1);
        assert_eq!(got[1].rows[0].key, "qk:group:g:orders:0");
        assert_eq!(got[1].rows[0].value["offset"], 41);
        assert_eq!(got[1].missing, ["qk:group:g:orders:1"]);
        assert!(!got[1].truncated);

        assert_eq!(got[2].op, "getPrefix");
        assert!(got[2].truncated);
        assert_eq!(got[2].next_after.as_deref(), Some("qk:group:g:clicks:7"));
    }

    #[test]
    fn a_kv_answer_that_does_not_cover_the_operations_is_an_error() {
        for (body, ops) in [
            // One answer for two operations.
            (r#"{"results":[{"index":0,"op":"put","applied":true}]}"#, 2),
            // The same operation twice, and none for the other.
            (
                r#"{"results":[{"index":0,"op":"put","applied":true},{"index":0,"op":"put","applied":true}]}"#,
                2,
            ),
            // An index nothing asked for.
            (r#"{"results":[{"index":7,"op":"put","applied":true}]}"#, 1),
            // An error envelope rather than results.
            (r#"{"error":"kv_bad_request","reason":"kv_bad_ttl"}"#, 1),
        ] {
            assert!(align_kv_results(body, ops).is_err(), "{body} was accepted");
        }
    }

    /// A put must declare an expiry and this one's is "never" — a committed
    /// offset that expires is a consumer group that silently resets.
    #[test]
    fn a_put_declares_that_it_never_expires() {
        let op = KvOp::put(
            "queen-kafka",
            "qk:group:g:orders:0",
            serde_json::json!({"offset": 7}),
        );
        let body = serde_json::to_string(&KvBody { operations: &[op] }).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        let put = &parsed["operations"][0];
        assert_eq!(put["op"], "put");
        assert_eq!(put["forever"], true);
        assert_eq!(put["ns"], "queen-kafka");
        assert!(put.get("ttlSeconds").is_none());
    }

    /// The key budget is counted the way the broker counts it: a getMany costs
    /// its key list and a getPrefix costs its CLAMPED limit, so a batch that
    /// passes here cannot be refused wholesale there.
    #[test]
    fn operations_cost_what_the_broker_charges_them() {
        assert_eq!(KvOp::put("ns", "k", serde_json::Value::Null).keys(), 1);
        assert_eq!(
            KvOp::GetMany {
                ns: "ns".into(),
                keys: vec!["a".into(), "b".into(), "c".into()]
            }
            .keys(),
            3
        );
        let prefix = |limit| KvOp::GetPrefix {
            ns: "ns".into(),
            prefix: "p".into(),
            limit,
            after: None,
        };
        assert_eq!(prefix(128).keys(), 128);
        assert_eq!(prefix(10_000).keys(), MAX_KV_PREFIX_LIMIT as usize);
        assert_eq!(prefix(0).keys(), 1);
    }

    /// The double is a store, not a script: a put is readable by key and by
    /// prefix, absence is reported as absence, and a prefix page carries the
    /// cursor to continue from.
    #[tokio::test]
    async fn the_double_reads_back_what_was_put() {
        let api = FakeQueen::with(&[]);
        let value = |n: i64| serde_json::json!({ "offset": n });
        api.kv(
            &[
                KvOp::put("queen-kafka", "qk:group:g:orders:1", value(1)),
                KvOp::put("queen-kafka", "qk:group:g:orders:0", value(0)),
            ],
            None,
        )
        .await
        .unwrap();

        let got = api
            .kv(
                &[KvOp::GetMany {
                    ns: "queen-kafka".into(),
                    keys: vec!["qk:group:g:orders:0".into(), "qk:group:g:orders:9".into()],
                }],
                None,
            )
            .await
            .unwrap();
        assert_eq!(got[0].rows.len(), 1);
        assert_eq!(got[0].rows[0].value["offset"], 0);
        assert_eq!(got[0].missing, ["qk:group:g:orders:9"]);

        // One row per page, so the cursor is exercised rather than assumed.
        let page = |after: Option<&str>| KvOp::GetPrefix {
            ns: "queen-kafka".into(),
            prefix: "qk:group:g:".into(),
            limit: 1,
            after: after.map(str::to_string),
        };
        let first = api.kv(&[page(None)], None).await.unwrap();
        assert_eq!(first[0].rows[0].key, "qk:group:g:orders:0");
        assert!(first[0].truncated);
        let second = api
            .kv(&[page(first[0].next_after.as_deref())], None)
            .await
            .unwrap();
        assert_eq!(second[0].rows[0].key, "qk:group:g:orders:1");
        assert!(!second[0].truncated, "the last page is not truncated");
    }

    /// ...and it refuses the two batch shapes the broker refuses WHOLESALE, so
    /// a caller that forgot to chunk fails in the tests rather than in the rig.
    #[tokio::test]
    async fn the_double_refuses_a_batch_over_the_brokers_ceilings() {
        let api = FakeQueen::with(&[]);
        let ops: Vec<KvOp> = (0..MAX_KV_OPS_PER_CALL + 1)
            .map(|i| KvOp::put("queen-kafka", &format!("k{i}"), serde_json::Value::Null))
            .collect();
        assert!(api.kv(&ops, None).await.is_err());

        let keys: Vec<String> = (0..MAX_KV_KEYS_PER_CALL + 1)
            .map(|i| format!("k{i}"))
            .collect();
        assert!(api
            .kv(
                &[KvOp::GetMany {
                    ns: "queen-kafka".into(),
                    keys
                }],
                None
            )
            .await
            .is_err());
    }

    // -------------------------------------------------------------- base url

    #[test]
    fn the_base_url_is_checked_at_boot_and_loses_its_trailing_slash() {
        assert_eq!(
            normalize_base_url("http://queen-mq-v1:6632/").unwrap(),
            "http://queen-mq-v1:6632"
        );
        assert_eq!(
            normalize_base_url("  https://cloud.queenmq.com  ").unwrap(),
            "https://cloud.queenmq.com"
        );
        for bad in ["queen-mq-v1:6632", "", "ftp://host", "postgres://h/db", "/"] {
            assert!(normalize_base_url(bad).is_err(), "{bad} was accepted");
        }
    }

    // --------------------------------------------------------------- catalog

    #[tokio::test]
    async fn the_list_is_served_from_cache_until_it_expires() {
        let api = FakeQueen::with(&[("orders", 4)]);
        let catalog = Catalog::with_ttl(api.clone(), Duration::from_millis(40));

        assert_eq!(catalog.list(None).await.unwrap().len(), 1);
        assert_eq!(catalog.list(None).await.unwrap().len(), 1);
        assert_eq!(api.lists.load(Ordering::SeqCst), 1, "second call refetched");

        tokio::time::sleep(Duration::from_millis(60)).await;
        catalog.list(None).await.unwrap();
        assert_eq!(api.lists.load(Ordering::SeqCst), 2);
    }

    /// A metadata storm is one admin call: the refresh is single-flight, and the
    /// callers that queue behind it find the entry fresh.
    #[tokio::test]
    async fn concurrent_refreshes_collapse_into_one_call() {
        let api = FakeQueen::with(&[("orders", 4)]);
        let catalog = Arc::new(Catalog::new(api.clone()));
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let c = Arc::clone(&catalog);
            tasks.push(tokio::spawn(
                async move { c.list(None).await.unwrap().len() },
            ));
        }
        for t in tasks {
            assert_eq!(t.await.unwrap(), 1);
        }
        assert_eq!(api.lists.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn creating_a_queue_invalidates_the_cache() {
        let api = FakeQueen::with(&[("orders", 4)]);
        let catalog = Catalog::new(api.clone());

        catalog.list(None).await.unwrap();
        catalog.create("clicks", None).await.unwrap();
        let after = catalog.list(None).await.unwrap();

        assert_eq!(api.creates.lock().unwrap().as_slice(), ["clicks"]);
        assert_eq!(api.lists.load(Ordering::SeqCst), 2, "cache was not dropped");
        assert!(after.iter().any(|q| q.name == "clicks"));
    }

    /// A failed refresh serves the previous list rather than an empty world.
    #[tokio::test]
    async fn a_failed_refresh_falls_back_to_the_last_good_list() {
        let api = FakeQueen::with(&[("orders", 4)]);
        let catalog = Catalog::with_ttl(api.clone(), Duration::from_millis(10));
        catalog.list(None).await.unwrap();

        *api.fail.lock().unwrap() = Some("connection refused".into());
        tokio::time::sleep(Duration::from_millis(20)).await;
        let stale = catalog.list(None).await.unwrap();
        assert_eq!(stale[0].name, "orders");
    }

    /// ...but a first call with nothing cached reports the failure, because
    /// there is no answer to give.
    #[tokio::test]
    async fn a_cold_failure_is_an_error() {
        let api = FakeQueen::with(&[]);
        *api.fail.lock().unwrap() = Some("connection refused".into());
        let catalog = Catalog::new(api);
        assert!(catalog.list(None).await.is_err());
    }

    /// The single-flight property has to hold in BOTH directions. With Queen
    /// down and nothing cached, the callers queued on the mutex used to find no
    /// entry and each repeat the whole 10-second call — sixteen cold
    /// connections were ~160 seconds of dead air, every one of them muted for
    /// its turn (conn.rs). The failure is remembered, so it is one call.
    #[tokio::test]
    async fn a_cold_failure_is_not_paid_for_by_every_caller() {
        let api = FakeQueen::with(&[]);
        *api.fail.lock().unwrap() = Some("connection refused".into());
        let catalog = Arc::new(Catalog::new(api.clone()));

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let c = Arc::clone(&catalog);
            tasks.push(tokio::spawn(async move { c.list(None).await.is_err() }));
        }
        for t in tasks {
            assert!(t.await.unwrap(), "every caller still learns it failed");
        }
        assert_eq!(api.lists.load(Ordering::SeqCst), 1);

        // The TTL-bypassing refresh honours the same marker: it exists to skip
        // a STALE SUCCESS, not to re-run a call that just failed.
        assert!(catalog.refresh(None).await.is_err());
        assert_eq!(api.lists.load(Ordering::SeqCst), 1);
    }

    /// The remembered failure expires with the TTL — it is a collapse window,
    /// not a circuit breaker, so recovery needs no special path.
    #[tokio::test]
    async fn a_remembered_failure_expires_and_the_next_call_recovers() {
        let api = FakeQueen::with(&[("orders", 4)]);
        *api.fail.lock().unwrap() = Some("connection refused".into());
        let catalog = Catalog::with_ttl(api.clone(), Duration::from_millis(20));
        assert!(catalog.list(None).await.is_err());
        assert!(catalog.list(None).await.is_err());
        assert_eq!(api.lists.load(Ordering::SeqCst), 1);

        *api.fail.lock().unwrap() = None;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(catalog.list(None).await.unwrap().len(), 1);
        assert_eq!(api.lists.load(Ordering::SeqCst), 2);
    }

    /// A failure never erases the last good list: the stale copy is still what
    /// `list` serves, and it survives repeated failures.
    #[tokio::test]
    async fn a_failure_does_not_forget_the_last_good_list() {
        let api = FakeQueen::with(&[("orders", 4)]);
        let catalog = Catalog::with_ttl(api.clone(), Duration::from_millis(10));
        catalog.list(None).await.unwrap();

        *api.fail.lock().unwrap() = Some("connection refused".into());
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(15)).await;
            assert_eq!(catalog.list(None).await.unwrap()[0].name, "orders");
        }
    }

    /// A `QueenApi` whose listing parks until it is let through, one call at a
    /// time. The stall this guards against is about what OTHER callers do while
    /// a call is in flight, and that needs a call that stays in flight.
    struct Gated {
        queues: Vec<Queue>,
        through: tokio::sync::Semaphore,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl Gated {
        fn new(queues: &[(&str, i64)]) -> Arc<Gated> {
            Arc::new(Gated {
                queues: queues
                    .iter()
                    .map(|(n, p)| Queue {
                        name: n.to_string(),
                        partitions: *p,
                    })
                    .collect(),
                through: tokio::sync::Semaphore::new(0),
                calls: std::sync::atomic::AtomicUsize::new(0),
            })
        }

        /// Let one parked (or one future) listing complete.
        fn let_one_through(&self) {
            self.through.add_permits(1);
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl QueenApi for Gated {
        fn list_queues<'a>(&'a self, _token: Option<&'a str>) -> BoxFuture<'a, Result<Vec<Queue>>> {
            Box::pin(async move {
                self.through
                    .acquire()
                    .await
                    .expect("the gate is never closed")
                    .forget();
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(self.queues.clone())
            })
        }

        fn create_queue<'a>(&'a self, _: &'a str, _: Option<&'a str>) -> BoxFuture<'a, Result<()>> {
            unreachable!("the stall test only lists")
        }

        fn push<'a>(
            &'a self,
            _: &'a [PushItem],
            _: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Vec<Pushed>>> {
            unreachable!("the stall test only lists")
        }

        fn fetch<'a>(
            &'a self,
            _: &'a [FetchEntry],
            _: i64,
            _: i64,
            _: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Vec<Fetched>>> {
            unreachable!("the stall test only lists")
        }

        fn kv<'a>(
            &'a self,
            _: &'a [KvOp],
            _: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Vec<KvAnswer>>> {
            unreachable!("the stall test only lists")
        }
    }

    /// THE stall: every produce reads this list, and a refresh used to be run
    /// with the map lock held, so one expiry parked every caller in the process
    /// behind one ten-second admin call. A caller that has a list must be
    /// answered from it while the refresh runs.
    #[tokio::test]
    async fn a_refresh_in_flight_does_not_park_a_caller_that_has_a_list() {
        let api = Gated::new(&[("orders", 4)]);
        let catalog = Arc::new(Catalog::with_ttl(api.clone(), Duration::from_millis(20)));

        api.let_one_through();
        assert_eq!(catalog.list(None).await.unwrap().len(), 1);

        // The entry goes stale, and the next call parks in the admin API.
        tokio::time::sleep(Duration::from_millis(30)).await;
        let refreshing = tokio::spawn({
            let c = Arc::clone(&catalog);
            async move { c.list(None).await.map(|q| q.len()) }
        });
        tokio::task::yield_now().await;

        // Everyone else is answered from the list already in hand. Without a
        // timeout this assertion would hang rather than fail, which is what the
        // defect looked like from a producer.
        for _ in 0..8 {
            let served = tokio::time::timeout(Duration::from_millis(200), catalog.list(None))
                .await
                .expect("a caller with a list in hand parked behind the refresh")
                .unwrap();
            assert_eq!(served.len(), 1);
        }
        assert_eq!(api.calls(), 1, "the stale readers started their own calls");

        api.let_one_through();
        assert_eq!(refreshing.await.unwrap().unwrap(), 1);
        assert_eq!(api.calls(), 2);
    }

    /// The other half: with nothing in hand there is nothing to serve, so a
    /// caller waits for the call in flight — and does not start a second one.
    #[tokio::test]
    async fn a_cold_caller_joins_the_call_in_flight_instead_of_repeating_it() {
        let api = Gated::new(&[("orders", 4)]);
        let catalog = Arc::new(Catalog::new(api.clone()));

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let c = Arc::clone(&catalog);
            tasks.push(tokio::spawn(
                async move { c.list(None).await.unwrap().len() },
            ));
        }
        tokio::task::yield_now().await;
        assert_eq!(api.calls(), 0, "nothing completes before the gate opens");

        api.let_one_through();
        for t in tasks {
            assert_eq!(t.await.unwrap(), 1);
        }
        assert_eq!(api.calls(), 1);
    }

    /// ...and two credentials of ONE tenant are one entry: the queue list is
    /// the tenant's, so a key rotation does not double the admin traffic and
    /// the two keys cannot disagree about which topics exist.
    #[tokio::test]
    async fn one_tenants_two_credentials_are_one_entry() {
        let api = FakeQueen::with(&[("orders", 4)]);
        api.answer_identity(Some("key-a"), "cluster-1");
        api.answer_identity(Some("key-b"), "cluster-1");
        api.answer_identity(Some("key-c"), "cluster-2");
        let catalog = Catalog::new(api.clone());

        // `refresh` is the authentication call, and is where the tenant is
        // resolved — the connection path in one line.
        for token in ["key-a", "key-b", "key-c"] {
            catalog.refresh(Some(token)).await.unwrap();
        }
        assert_eq!(
            catalog.entries.lock().await.len(),
            2,
            "one tenant's two credentials are two queue-list caches"
        );
        // Each credential still reaches Queen as ITSELF: the entry is shared,
        // the bearer never is.
        assert_eq!(
            api.tokens.lock().unwrap().as_slice(),
            [
                Some("key-a".to_string()),
                Some("key-b".to_string()),
                Some("key-c".to_string())
            ]
        );
        // ...and a credential nobody could identify is its own entry rather
        // than joining a shared bucket.
        catalog.refresh(Some("key-d")).await.unwrap();
        assert_eq!(catalog.entries.lock().await.len(), 3);
    }

    /// A shared entry must not let one credential answer for another AT
    /// AUTHENTICATION. A remembered failure is the credential's — a key with
    /// narrower scopes, or one revoked a second ago — and replaying it to a
    /// sibling key would refuse a valid connection for a whole TTL.
    #[tokio::test]
    async fn one_credentials_failure_is_not_its_tenants() {
        let api = FakeQueen::with(&[("orders", 4)]);
        api.answer_identity(Some("key-a"), "cluster-1");
        api.answer_identity(Some("key-b"), "cluster-1");
        // The broker accepts one of the tenant's two keys and refuses the
        // other, exactly as its auth layer answers one it does not know.
        api.accept_only("key-b");
        let catalog = Catalog::new(api.clone());

        assert!(catalog.refresh(Some("key-a")).await.is_err());
        // The refused credential is still collapsed into one call per window:
        // a fleet retrying a stale password is one call, not one per
        // connection.
        assert!(catalog.refresh(Some("key-a")).await.is_err());
        assert_eq!(api.list_count(), 1, "the refused credential called twice");

        // ...and its sibling is admitted, on its own call.
        assert!(
            catalog.refresh(Some("key-b")).await.is_ok(),
            "a sibling credential's refusal refused this one"
        );
        assert_eq!(api.list_count(), 2);
    }

    /// Two credentials are two catalogs — the M5 seam, wired now so it cannot be
    /// forgotten when SASL lands.
    #[tokio::test]
    async fn each_token_gets_its_own_entry() {
        let api = FakeQueen::with(&[("orders", 4)]);
        let catalog = Catalog::new(api.clone());
        catalog.list(Some("tenant-a")).await.unwrap();
        catalog.list(Some("tenant-b")).await.unwrap();
        catalog.list(Some("tenant-a")).await.unwrap();
        assert_eq!(api.lists.load(Ordering::SeqCst), 2);
        assert_eq!(
            api.tokens.lock().unwrap().as_slice(),
            [Some("tenant-a".to_string()), Some("tenant-b".to_string())]
        );
    }

    /// ...and the key space that makes those two entries two is a space a peer
    /// chooses from: with SASL on, every connection may present a different
    /// password and a wrong one costs the sender a single frame. Both maps are
    /// bounded, so a credential nobody is using is what goes.
    #[tokio::test]
    async fn the_catalog_keeps_a_bounded_number_of_credentials() {
        let api = FakeQueen::with(&[("orders", 4)]);
        // Refused credentials fill the map exactly as accepted ones do — that
        // is the shape an attacker has, and it is why this bound exists.
        api.accept_only("the-one-real-token");
        let catalog = Catalog::new(api.clone());

        catalog.list(Some("the-one-real-token")).await.unwrap();
        for i in 0..MAX_CREDENTIALS * 2 {
            // Every few tries the real credential is used again, which is what
            // keeps its entry from being the coldest.
            if i % 8 == 0 {
                catalog.list(Some("the-one-real-token")).await.ok();
            }
            catalog.list(Some(&format!("guess-{i}"))).await.ok();
        }

        assert_eq!(catalog.entries.lock().await.len(), MAX_CREDENTIALS);
        assert!(
            catalog.refreshes.lock().await.len() <= MAX_CREDENTIALS,
            "the refresh locks grew past the bound the entries are held to"
        );
        // The credential that is being used is still cached: a flood of wrong
        // passwords must not cost the tenant its catalog.
        let real = Some("the-one-real-token");
        assert!(
            catalog
                .cached(&catalog.tenant_key(real), CredentialKey::of(real))
                .await
                .is_some(),
            "a flood of refused credentials evicted the one in use"
        );
    }

    /// The maps are keyed by a hash and hold no credential, which is the point
    /// of [`CredentialKey`]: they are process-wide, they outlive every
    /// connection, and half of what reaches them is somebody's mistyped
    /// password.
    #[tokio::test]
    async fn the_catalog_holds_no_credential() {
        const SECRET: &str = "s3cr3t-tenant-token";
        let api = FakeQueen::with(&[("orders", 4)]);
        api.accept_only(SECRET);
        let catalog = Catalog::new(api.clone());
        catalog.list(Some(SECRET)).await.unwrap();
        catalog.list(Some("wrong-but-secret")).await.ok();

        let keys: Vec<String> = catalog
            .entries
            .lock()
            .await
            .keys()
            .map(|k| format!("{k:?}"))
            .collect();
        assert_eq!(keys.len(), 2, "the two credentials are two entries");
        for printed in keys {
            assert!(!printed.contains("s3cr3t"), "{printed}");
            assert!(!printed.contains("wrong-but-secret"), "{printed}");
        }
    }
}
