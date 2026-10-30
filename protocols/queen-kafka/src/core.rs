//! The facade's only door into Queen: the broker it runs inside.
//!
//! [`Broker`] is implemented by the server (server/src/kafka_inproc.rs). The
//! record path (push, fetch) is typed; everything else is one of the broker's
//! own JSON routes handed to its router in-process ([`Broker::call`]), the same
//! operations a native client uses. [`Queen`] wraps those routes.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The broker's admission of a request ([`Broker::admit`]): its bytes go back
/// to the budget when this drops.
pub type Permit = Box<dyn std::any::Any + Send + Sync>;

/// What one Queen transaction may set at once (`POST /api/v1/transaction`).
pub const MAX_POSITION_WRITES: usize = 16_384;
/// What one positions read may name (`POST /api/v1/consumer-groups/positions`).
pub const MAX_POSITION_READS: usize = 65_536;
/// What one time lookup may name (`POST /api/v1/fetch/offsets`).
pub const MAX_TIME_LOOKUPS: usize = 1_024;
/// What one typed fetch may name ([`Broker::fetch`]).
pub const MAX_FETCH_ENTRIES: usize = 1_024;

/// One record to push: queue, partition NAME, one JSON document, and the
/// transaction id that makes a retry of it a duplicate (`None`: the broker
/// mints one).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawPush {
    pub queue: String,
    pub partition: String,
    pub payload: Vec<u8>,
    pub txn: Option<String>,
}

/// The broker's answer for one pushed record.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Pushed {
    /// `queued`, `duplicate`, `error`.
    #[serde(default)]
    pub status: String,
    /// The record's offset in its partition; absent on an error.
    #[serde(default)]
    pub offset: Option<i64>,
}

/// One partition to read from an offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchEntry {
    pub queue: String,
    pub partition: String,
    pub offset: i64,
    pub max_bytes: i64,
}

/// One stored record as read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRecord {
    pub offset: i64,
    pub payload: Bytes,
    /// The log's own timestamp, epoch ms: the fallback for a payload with none.
    pub timestamp_ms: Option<i64>,
}

/// One partition of a fetch answer.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RawFetched {
    pub records: Vec<RawRecord>,
    pub high_watermark: i64,
    pub log_start_offset: i64,
    /// `UNKNOWN_TOPIC_OR_PARTITION` or `OFFSET_OUT_OF_RANGE`.
    pub error: Option<String>,
}

/// One route call, as an HTTP client would make it, minus the socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalRequest {
    pub method: &'static str,
    pub path: String,
    pub host: Option<String>,
    pub bearer: Option<String>,
    pub body: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalResponse {
    pub status: u16,
    pub retry_after: Option<String>,
    pub body: String,
}

/// A typed call's value, or the HTTP answer the route would have given instead.
#[derive(Debug)]
pub enum LocalAnswer<T> {
    Done(T),
    Refused(LocalResponse),
}

/// An ordered push: `in_log` answers `true` once every command of the push
/// was proposed by this node as leader, so anything sent after it lands after
/// it; `false` when the push ended without that.
pub struct OrderedLocal {
    pub in_log: BoxFuture<'static, bool>,
    pub done: BoxFuture<'static, Result<LocalAnswer<Vec<Pushed>>, String>>,
}

/// The records one Fetch answer carried from one queue, and how long they had
/// waited (now minus the log's own timestamp of each).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FetchedFrom {
    pub queue: String,
    pub records: u64,
    pub lag_sum_ms: u64,
    pub lag_max_ms: u64,
}

/// One raft member as Kafka sees it: a broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

/// The raft cluster now, as this node knows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterView {
    pub node_id: i32,
    /// The raft leader, when known. It leads every partition and coordinates
    /// every group.
    pub leader: Option<i32>,
    /// Every voter, this node included.
    pub nodes: Vec<Node>,
}

impl ClusterView {
    pub fn leads(&self) -> bool {
        self.leader == Some(self.node_id)
    }

    pub fn leader_node(&self) -> Option<&Node> {
        let id = self.leader?;
        self.nodes.iter().find(|n| n.id == id)
    }
}

/// What the broker gives the facade.
pub trait Broker: Send + Sync + 'static {
    /// One of the broker's JSON routes, through its router.
    fn call(&self, req: LocalRequest) -> BoxFuture<'static, Result<LocalResponse, String>>;

    /// Push records, ordered (see [`OrderedLocal`]). `admitted` is the
    /// request's admission taken by [`Broker::admit`] before it was decoded:
    /// the push holds it until it answers and takes none of its own.
    fn push(
        &self,
        bearer: Option<String>,
        items: Vec<RawPush>,
        admitted: Option<Permit>,
    ) -> OrderedLocal;

    /// Admission for a request of `bytes` (its frame), BEFORE it is decoded:
    /// the HTTP edge's rule, which sizes a push by its `Content-Length` before
    /// it reads the body. `Err(retry_after_s)` when the broker stayed saturated
    /// for its whole hold. The default admits everything.
    fn admit(&self, _bytes: usize) -> BoxFuture<'static, Result<Option<Permit>, u64>> {
        Box::pin(async { Ok(None) })
    }

    /// Read partitions from offsets, long poll included.
    fn fetch(
        &self,
        bearer: Option<String>,
        entries: Vec<FetchEntry>,
        max_wait_ms: i64,
        min_bytes: i64,
        max_bytes: Option<i64>,
    ) -> BoxFuture<'static, Result<LocalAnswer<Vec<RawFetched>>, String>>;

    /// The raft cluster now.
    fn cluster(&self) -> ClusterView;

    /// What one Fetch answer carried, per queue, for the dashboard: Kafka
    /// reads count there as pops do.
    fn note_fetched(&self, _bearer: Option<&str>, _from: &[FetchedFrom]) {}
}

// ------------------------------------------------------------------ routes

/// A route call that did not answer 2xx, or answered something unreadable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError {
    /// No answer at all: the broker task died.
    Transport(String),
    Status {
        code: u16,
        body: String,
    },
    Body(String),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Transport(e) => write!(f, "broker call failed: {e}"),
            CallError::Status { code, body } => {
                write!(f, "broker answered {code}: {}", clip(body, 300))
            }
            CallError::Body(e) => write!(f, "unreadable broker answer: {e}"),
        }
    }
}

fn clip(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

/// One queue of the queue list.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct QueueRow {
    pub name: String,
    /// How many partitions exist (any name).
    #[serde(default)]
    pub partitions: i64,
}

/// One KV operation (`POST /api/v1/kv`, or a rider of a transaction).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "op")]
pub enum KvOp {
    /// A write; with `expect` it is conditional on the row's version (`0`:
    /// absent), and `required` makes a lost condition roll the whole
    /// transaction back instead of skipping this op.
    #[serde(rename = "put")]
    Put {
        ns: String,
        key: String,
        value: Value,
        /// Exactly one of `forever` and `ttlSeconds` is sent.
        #[serde(skip_serializing_if = "std::ops::Not::not")]
        forever: bool,
        #[serde(rename = "ttlSeconds", skip_serializing_if = "Option::is_none")]
        ttl_seconds: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        expect: Option<i64>,
        #[serde(skip_serializing_if = "std::ops::Not::not")]
        required: bool,
    },
    #[serde(rename = "getMany")]
    GetMany { ns: String, keys: Vec<String> },
    #[serde(rename = "getPrefix")]
    GetPrefix {
        ns: String,
        prefix: String,
        limit: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        after: Option<String>,
    },
    #[serde(rename = "delete")]
    Delete { ns: String, key: String },
}

impl KvOp {
    /// An unconditional write that never expires.
    pub fn put(ns: &str, key: &str, value: Value) -> KvOp {
        KvOp::Put {
            ns: ns.to_string(),
            key: key.to_string(),
            value,
            forever: true,
            ttl_seconds: None,
            expect: None,
            required: false,
        }
    }

    /// A write at a version (`0`: only when absent), skipped when it moved.
    pub fn put_at(ns: &str, key: &str, value: Value, expect: i64) -> KvOp {
        KvOp::Put {
            ns: ns.to_string(),
            key: key.to_string(),
            value,
            forever: true,
            ttl_seconds: None,
            expect: Some(expect),
            required: false,
        }
    }

    /// A write at a version that rolls its transaction back when it moved.
    pub fn fence(ns: &str, key: &str, value: Value, expect: i64) -> KvOp {
        KvOp::Put {
            ns: ns.to_string(),
            key: key.to_string(),
            value,
            forever: true,
            ttl_seconds: None,
            expect: Some(expect),
            required: true,
        }
    }

    /// The same write, expiring `seconds` after it lands instead of never:
    /// a row nobody writes again goes by itself.
    pub fn expiring(mut self, seconds: i64) -> KvOp {
        if let KvOp::Put {
            forever,
            ttl_seconds,
            ..
        } = &mut self
        {
            *forever = false;
            *ttl_seconds = Some(seconds.max(1));
        }
        self
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct KvRow {
    pub key: String,
    #[serde(default)]
    pub value: Value,
    #[serde(default)]
    pub version: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct KvAnswer {
    #[serde(default)]
    pub index: usize,
    /// `false` when a conditional write was skipped.
    #[serde(default)]
    pub applied: Option<bool>,
    /// The row's version after a write.
    #[serde(default)]
    pub version: i64,
    #[serde(default)]
    pub rows: Vec<KvRow>,
    #[serde(rename = "nextAfter", default)]
    pub next_after: Option<String>,
}

/// Where `group` reads `partition` of `queue` next (a Kafka committed offset).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PositionOp {
    pub queue: String,
    pub partition: String,
    #[serde(rename = "consumerGroup")]
    pub group: String,
    /// The next offset the group reads; `None` forgets the position.
    pub offset: Option<i64>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub metadata: String,
}

/// One answer of [`Queen::positions`].
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Position {
    pub queue: String,
    pub partition: String,
    #[serde(default)]
    pub offset: Option<i64>,
    #[serde(default)]
    pub metadata: String,
}

/// One time lookup (`POST /api/v1/fetch/offsets`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TimeLookup {
    pub queue: String,
    pub partition: String,
    #[serde(rename = "timestamp")]
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TimeFound {
    pub offset: Option<i64>,
    pub timestamp_ms: Option<i64>,
    pub error: Option<String>,
}

/// The broker's routes the facade uses, typed.
#[derive(Clone)]
pub struct Queen {
    pub broker: Arc<dyn Broker>,
}

impl Queen {
    pub fn new(broker: Arc<dyn Broker>) -> Queen {
        Queen { broker }
    }

    async fn call(
        &self,
        method: &'static str,
        path: String,
        bearer: Option<&str>,
        body: Option<Value>,
    ) -> Result<Value, CallError> {
        let text = self
            .call_text(method, path, bearer, body.map(|b| b.to_string()))
            .await?;
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text).map_err(|e| CallError::Body(e.to_string()))
    }

    /// A route call whose 2xx body is read straight into `T`, never through a
    /// `Value` tree; `body` is already JSON text (a typed struct serialized).
    async fn call_typed<T: DeserializeOwned>(
        &self,
        method: &'static str,
        path: String,
        bearer: Option<&str>,
        body: Option<String>,
    ) -> Result<T, CallError> {
        let text = self.call_text(method, path, bearer, body).await?;
        let text = if text.trim().is_empty() { "null" } else { &text };
        serde_json::from_str(text).map_err(|e| CallError::Body(e.to_string()))
    }

    /// A route call: the body of a 2xx answer, or the refusal.
    async fn call_text(
        &self,
        method: &'static str,
        path: String,
        bearer: Option<&str>,
        body: Option<String>,
    ) -> Result<String, CallError> {
        let resp = self
            .broker
            .call(LocalRequest {
                method,
                path,
                host: None,
                bearer: bearer.map(str::to_string),
                body,
            })
            .await
            .map_err(CallError::Transport)?;
        if !(200..300).contains(&resp.status) {
            return Err(CallError::Status {
                code: resp.status,
                body: resp.body,
            });
        }
        Ok(resp.body)
    }

    /// Whether the broker takes `token` (SASL): a call every token must be
    /// allowed to make, its answer read for the status alone.
    pub async fn check_token(&self, token: &str) -> Result<(), CallError> {
        self.call_text(
            "GET",
            "/api/v1/resources/queues".into(),
            Some(token),
            None,
        )
        .await
        .map(|_| ())
    }

    /// Every queue of the caller's tenant.
    pub async fn queues(&self, bearer: Option<&str>) -> Result<Vec<QueueRow>, CallError> {
        #[derive(Deserialize)]
        struct Body {
            #[serde(default)]
            queues: Vec<QueueRow>,
        }
        let b: Body = self
            .call_typed(
                "GET",
                "/api/v1/resources/queues?stats=lanes".into(),
                bearer,
                None,
            )
            .await?;
        Ok(b.queues)
    }

    /// Create a queue for a Kafka topic: Queen's own defaults, but the
    /// facade's dedup window when it has one (0 keeps Queen's).
    /// Create a topic's queue with the facade's dedup window and, when the
    /// topic's configs ask for one, a retention in whole seconds
    /// (`retention.ms`, see `handlers::admin::retention_of`).
    pub async fn create_queue(
        &self,
        bearer: Option<&str>,
        name: &str,
        dedup_window_seconds: i64,
        retention_seconds: Option<i64>,
    ) -> Result<(), CallError> {
        let mut options = serde_json::Map::new();
        if dedup_window_seconds > 0 {
            options.insert("dedupWindowSeconds".into(), json!(dedup_window_seconds));
        }
        if let Some(s) = retention_seconds {
            options.insert("retentionEnabled".into(), json!(true));
            options.insert("retentionSeconds".into(), json!(s));
        }
        self.configure(bearer, name, Value::Object(options)).await
    }

    /// Set some options of a queue (creating it when it is missing); options
    /// not named keep their value.
    pub async fn configure(
        &self,
        bearer: Option<&str>,
        name: &str,
        options: Value,
    ) -> Result<(), CallError> {
        let mut body = match options {
            Value::Object(o) => o,
            _ => serde_json::Map::new(),
        };
        body.insert("queue".into(), Value::String(name.to_string()));
        let v = self
            .call(
                "POST",
                "/api/v1/configure".into(),
                bearer,
                Some(Value::Object(body)),
            )
            .await?;
        if v.get("configured").and_then(Value::as_bool) == Some(true) {
            Ok(())
        } else {
            Err(CallError::Body(format!("configure did not confirm: {v}")))
        }
    }

    /// A queue's options (`leaseTime`, `dedupWindowSeconds`, …); `None` when
    /// there is no such queue.
    pub async fn queue_options(
        &self,
        bearer: Option<&str>,
        name: &str,
    ) -> Result<Option<Value>, CallError> {
        match self
            .call(
                "GET",
                format!("/api/v1/resources/queues/{}", encode_segment(name)),
                bearer,
                None,
            )
            .await
        {
            Ok(v) => Ok(v.get("options").cloned()),
            Err(CallError::Status { code: 404, .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Delete a queue; `false` when there was none.
    pub async fn delete_queue(&self, bearer: Option<&str>, name: &str) -> Result<bool, CallError> {
        match self
            .call(
                "DELETE",
                format!("/api/v1/resources/queues/{}", encode_segment(name)),
                bearer,
                None,
            )
            .await
        {
            Ok(v) => Ok(v.get("deleted").and_then(Value::as_bool).unwrap_or(true)),
            Err(CallError::Status { code: 404, .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub async fn kv(&self, bearer: Option<&str>, ops: &[KvOp]) -> Result<Vec<KvAnswer>, CallError> {
        #[derive(Deserialize)]
        struct Body {
            #[serde(default = "yes")]
            ok: bool,
            #[serde(default)]
            reason: String,
            #[serde(default)]
            results: Vec<KvAnswer>,
        }
        #[derive(Serialize)]
        struct Request<'a> {
            operations: &'a [KvOp],
        }
        let request = serde_json::to_string(&Request { operations: ops })
            .map_err(|e| CallError::Body(e.to_string()))?;
        let b: Body = self
            .call_typed("POST", "/api/v1/kv".into(), bearer, Some(request))
            .await?;
        if !b.ok {
            return Err(CallError::Body(format!(
                "kv answered ok=false: {}",
                b.reason
            )));
        }
        let mut out: Vec<KvAnswer> = b.results;
        out.sort_by_key(|a| a.index);
        Ok(out)
    }

    /// A group's positions: on the given partitions, or every one it has.
    pub async fn positions(
        &self,
        bearer: Option<&str>,
        group: &str,
        entries: Option<&[(String, String)]>,
    ) -> Result<Vec<Position>, CallError> {
        #[derive(Deserialize)]
        struct Body {
            #[serde(default)]
            entries: Vec<Position>,
        }
        #[derive(Serialize)]
        struct Entry<'a> {
            queue: &'a str,
            partition: &'a str,
        }
        #[derive(Serialize)]
        struct Request<'a> {
            #[serde(rename = "consumerGroup")]
            group: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            entries: Option<Vec<Entry<'a>>>,
        }
        if entries.is_some_and(|e| e.len() > MAX_POSITION_READS) {
            return Err(CallError::Status {
                code: 400,
                body: format!("more than {MAX_POSITION_READS} positions in one read"),
            });
        }
        let request = Request {
            group,
            entries: entries.map(|e| {
                e.iter()
                    .map(|(queue, partition)| Entry { queue, partition })
                    .collect()
            }),
        };
        let request =
            serde_json::to_string(&request).map_err(|e| CallError::Body(e.to_string()))?;
        let b: Body = self
            .call_typed(
                "POST",
                "/api/v1/consumer-groups/positions".into(),
                bearer,
                Some(request),
            )
            .await?;
        Ok(b.entries)
    }

    /// Write positions: one transaction, one raft entry.
    pub async fn commit(&self, bearer: Option<&str>, ops: &[PositionOp]) -> Result<(), CallError> {
        if ops.len() > MAX_POSITION_WRITES {
            return Err(CallError::Status {
                code: 400,
                body: format!("more than {MAX_POSITION_WRITES} positions in one commit"),
            });
        }
        let request = serde_json::to_string(&TxnRequest {
            operations: &[],
            kv: &[],
            positions: ops,
        })
        .map_err(|e| CallError::Body(e.to_string()))?;
        let text = self
            .call_text("POST", "/api/v1/transaction".into(), bearer, Some(request))
            .await?;
        if text.trim().is_empty() {
            return Ok(());
        }
        let v: TxnAnswer =
            serde_json::from_str(&text).map_err(|e| CallError::Body(e.to_string()))?;
        if v.success == Some(false) {
            return Err(CallError::Body(format!(
                "the commit rolled back: {}",
                clip(&text, 300)
            )));
        }
        Ok(())
    }

    /// One Queen transaction: the records pushed, the KV riders written and
    /// the positions set, all or nothing. `Err` is an unknown outcome.
    pub async fn transaction(
        &self,
        bearer: Option<&str>,
        records: &[RawPush],
        kv: &[KvOp],
        positions: &[PositionOp],
    ) -> Result<crate::txn::Committed, CallError> {
        use crate::txn::Committed;
        // Typed and borrowed: each payload is written as the JSON it already
        // is, never parsed into a tree and written again.
        let mut items = Vec::with_capacity(records.len());
        for r in records {
            let payload: &serde_json::value::RawValue = std::str::from_utf8(&r.payload)
                .ok()
                .and_then(|p| serde_json::from_str(p).ok())
                .ok_or_else(|| {
                    CallError::Body("a staged record is not one JSON document".into())
                })?;
            items.push(PushItem {
                queue: &r.queue,
                partition: &r.partition,
                payload,
                transaction_id: r.txn.as_deref(),
            });
        }
        let pushes = !items.is_empty();
        let push = [PushOperation {
            kind: "push",
            items,
        }];
        let operations: &[PushOperation<'_>] = if pushes { &push } else { &[] };
        let body = TxnRequest {
            operations,
            kv,
            positions,
        };
        let body = serde_json::to_string(&body).map_err(|e| CallError::Body(e.to_string()))?;
        let resp = self
            .broker
            .call(LocalRequest {
                method: "POST",
                path: "/api/v1/transaction".into(),
                host: None,
                bearer: bearer.map(str::to_string),
                body: Some(body),
            })
            .await
            .map_err(CallError::Transport)?;
        if (400..500).contains(&resp.status) && resp.status != 408 && resp.status != 429 {
            return Ok(Committed::Refused(format!(
                "{}: {}",
                resp.status,
                clip(&resp.body, 300)
            )));
        }
        if !(200..300).contains(&resp.status) {
            return Err(CallError::Status {
                code: resp.status,
                body: resp.body,
            });
        }
        let v: TxnAnswer =
            serde_json::from_str(&resp.body).map_err(|e| CallError::Body(e.to_string()))?;
        if v.success == Some(false) {
            let reason = v.reason.as_deref().unwrap_or_default();
            return Ok(if reason == "kv_precondition" {
                Committed::Fenced
            } else {
                Committed::Refused(format!("{reason}: {}", clip(&resp.body, 300)))
            });
        }
        let fence_version = v
            .results
            .iter()
            .filter(|r| r.kind.as_deref() == Some("kv"))
            .find_map(|r| r.version);
        Ok(Committed::Done { fence_version })
    }

    /// Forget a group's positions; how many partitions it had.
    pub async fn forget_group(&self, bearer: Option<&str>, group: &str) -> Result<u64, CallError> {
        let v = self
            .call(
                "DELETE",
                format!(
                    "/api/v1/consumer-groups/{}?deleteMetadata=true",
                    encode_segment(group)
                ),
                bearer,
                None,
            )
            .await?;
        Ok(v.get("deletedPartitions")
            .and_then(Value::as_u64)
            .unwrap_or(0))
    }

    /// The first offset at or after each timestamp.
    pub async fn times(
        &self,
        bearer: Option<&str>,
        lookups: &[TimeLookup],
    ) -> Result<Vec<TimeFound>, CallError> {
        #[derive(Deserialize)]
        struct Wire {
            #[serde(default)]
            offset: Option<i64>,
            #[serde(default)]
            ts: Option<String>,
            #[serde(default)]
            error: Option<String>,
        }
        #[derive(Deserialize)]
        struct Body {
            #[serde(default)]
            entries: Vec<Wire>,
        }
        #[derive(Serialize)]
        struct Request<'a> {
            entries: &'a [TimeLookup],
        }
        if lookups.len() > MAX_TIME_LOOKUPS {
            return Err(CallError::Status {
                code: 400,
                body: format!("more than {MAX_TIME_LOOKUPS} time lookups in one call"),
            });
        }
        let request = serde_json::to_string(&Request { entries: lookups })
            .map_err(|e| CallError::Body(e.to_string()))?;
        let b: Body = self
            .call_typed(
                "POST",
                "/api/v1/fetch/offsets".into(),
                bearer,
                Some(request),
            )
            .await?;
        if b.entries.len() != lookups.len() {
            return Err(CallError::Body(format!(
                "{} time lookups answered for {} asked",
                b.entries.len(),
                lookups.len()
            )));
        }
        Ok(b.entries
            .into_iter()
            .map(|w| TimeFound {
                offset: w.offset,
                timestamp_ms: w.ts.as_deref().and_then(epoch_millis),
                error: w.error,
            })
            .collect())
    }
}

fn yes() -> bool {
    true
}

/// `POST /api/v1/transaction`, typed: nothing of it is built as a `Value`.
#[derive(Serialize)]
struct TxnRequest<'a> {
    operations: &'a [PushOperation<'a>],
    kv: &'a [KvOp],
    positions: &'a [PositionOp],
}

#[derive(Serialize)]
struct PushOperation<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    items: Vec<PushItem<'a>>,
}

#[derive(Serialize)]
struct PushItem<'a> {
    queue: &'a str,
    partition: &'a str,
    payload: &'a serde_json::value::RawValue,
    #[serde(rename = "transactionId", skip_serializing_if = "Option::is_none")]
    transaction_id: Option<&'a str>,
}

/// The part of a transaction's answer the facade reads.
#[derive(Deserialize)]
struct TxnAnswer {
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    results: Vec<TxnResult>,
}

#[derive(Deserialize)]
struct TxnResult {
    #[serde(rename = "type", default)]
    kind: Option<String>,
    #[serde(default)]
    version: Option<i64>,
}

/// Percent-encode everything but RFC 3986's unreserved set.
pub fn encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// `2026-10-20T08:00:00.123Z` → epoch ms.
pub fn epoch_millis(ts: &str) -> Option<i64> {
    let ts = ts.strip_suffix('Z').unwrap_or(ts);
    let (date, time) = ts.split_once('T')?;
    let mut d = date.splitn(3, '-');
    let (y, mo, da) = (d.next()?, d.next()?, d.next()?);
    let mut t = time.splitn(3, ':');
    let (h, mi, rest) = (t.next()?, t.next()?, t.next()?);
    let (s, frac) = rest.split_once('.').unwrap_or((rest, ""));
    let frac: String = frac.chars().take_while(|c| c.is_ascii_digit()).collect();
    let millis: i64 = format!("{frac:0<3}")[..3].parse().ok()?;
    let num = |s: &str| s.parse::<i64>().ok();
    let (y, mo, da) = (num(y)?, num(mo)?, num(da)?);
    let (h, mi, s) = (num(h)?, num(mi)?, num(s)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&da) {
        return None;
    }
    let days = days_from_civil(y, mo, da);
    Some(((days * 86_400 + h * 3_600 + mi * 60 + s) * 1_000) + millis)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_parse_to_epoch_millis() {
        assert_eq!(epoch_millis("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(epoch_millis("1970-01-01T00:00:01.5Z"), Some(1500));
        assert_eq!(
            epoch_millis("2026-10-20T00:00:00.000Z"),
            Some(1_792_454_400_000)
        );
        assert_eq!(epoch_millis("nonsense"), None);
    }

    #[test]
    fn segments_are_percent_encoded() {
        assert_eq!(encode_segment("a b/c"), "a%20b%2Fc");
        assert_eq!(encode_segment("orders-1.x_y~"), "orders-1.x_y~");
    }
}
