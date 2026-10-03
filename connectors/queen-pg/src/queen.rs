//! The Queen side of the connectors: the [`QueenApi`] trait the broker
//! implements in-process, and the wire shapes of the five calls.
//!
//! The connectors run INSIDE the broker, on every node. The broker hands each
//! one an `Arc<dyn QueenApi>` bound to the connector's tenant; its five calls
//! are the in-process twins of five routes, with the routes' semantics:
//!
//!   * **transaction** — `POST /api/v1/transaction`: pushes plus a top-level
//!     `kv` rider, ONE raft entry, all-or-nothing (server/src/rsm/planner/txn.rs).
//!     A lost `required` KV precondition or a duplicate push rolls the whole
//!     bundle back and answers HTTP 200 with `success:false` and `reason`
//!     `kv_precondition` / `duplicate`. That 200 is the EXPECTED outcome of a
//!     retried bundle, never an error status.
//!   * **kv** — `POST /api/v1/kv`: one result per operation; a lost `required`
//!     precondition answers 200 `{"ok":false,"reason":"kv_precondition",…}`.
//!   * **pop** — `GET /api/v1/pop/queue/:queue` (wildcard) with a consumer
//!     group, `autoAck=false`: the messages of the partitions this call
//!     claimed, each with its lease.
//!   * **ack** — `POST /api/v1/ack/batch`: one result per item, input order.
//!   * **extend_lease** — `POST /api/v1/lease/:leaseId/extend`.
//!
//! Every method takes OWNED arguments and returns a boxed future, so the trait
//! stays dyn-compatible (`async fn` in a trait is not).

use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;
use serde_json::Value;

/// A boxed future, so [`QueenApi`] stays dyn-compatible.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The KV namespace of every key the connectors write (the broker validates
/// namespaces against `^[a-z0-9][a-z0-9._-]{0,63}$`).
pub const KV_NAMESPACE: &str = "queen-pg";

/// Rows per `getPrefix` page: the broker clamps `limit` to `1..=1000`.
pub const MAX_KV_PREFIX_LIMIT: i64 = 1_000;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// The broker answered an error status, or the in-process call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueenError {
    /// The status, body and `Retry-After` (ms) the route would have answered.
    Status {
        code: u16,
        body: String,
        retry_after_ms: Option<u64>,
    },
    /// The in-process call itself failed (the broker task ended).
    Transport(String),
}

impl QueenError {
    /// Back off and send the SAME request again.
    pub fn is_retryable(&self) -> bool {
        match self {
            QueenError::Status { code, .. } => matches!(code, 408 | 429 | 500 | 502 | 503 | 504),
            QueenError::Transport(_) => true,
        }
    }

    /// The outcome is unknown: the call may or may not have committed. A
    /// caller with a non-idempotent call must READ the state before deciding
    /// (the source reads its pointer).
    /// A 503 that was refused before it reached the log (no leader) is
    /// counted in doubt too: reading the state costs one KV read, guessing
    /// wrong costs a duplicate or a gap.
    pub fn is_in_doubt(&self) -> bool {
        match self {
            QueenError::Status { code, .. } => matches!(code, 408 | 500 | 502 | 503 | 504),
            QueenError::Transport(_) => true,
        }
    }

    /// How long the broker asked to wait, if it did.
    pub fn retry_after_ms(&self) -> Option<u64> {
        match self {
            QueenError::Status { retry_after_ms, .. } => *retry_after_ms,
            QueenError::Transport(_) => None,
        }
    }
}

impl std::fmt::Display for QueenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueenError::Status { code, body, .. } => {
                let b: String = body.chars().take(300).collect();
                write!(f, "HTTP {code}: {b}")
            }
            QueenError::Transport(m) => write!(f, "transport: {m}"),
        }
    }
}

// ---------------------------------------------------------------------------
// KV operations (the route's wire, namespace KV_NAMESPACE on every op)
// ---------------------------------------------------------------------------

/// One key/value operation. Copied from connectors/queen-s3/src/queen.rs: the
/// conditional half (`expect`, `required`) is the broker's own fence
/// (server/src/rsm/planner/kv.rs).
#[derive(Debug, Clone, PartialEq)]
pub enum KvOp {
    Get {
        key: String,
    },
    GetMany {
        keys: Vec<String>,
    },
    GetPrefix {
        prefix: String,
        limit: i64,
        after: Option<String>,
    },
    /// `ttl_seconds: None` is FOREVER (`"forever": true` on the wire).
    /// `expect: Some(0)` is "must not exist"; `Some(n>0)` a pure update.
    Put {
        key: String,
        value: Value,
        ttl_seconds: Option<u64>,
        expect: Option<i64>,
        /// A lost precondition aborts the WHOLE call (or transaction).
        required: bool,
    },
    PutIfAbsent {
        key: String,
        value: Value,
        ttl_seconds: Option<u64>,
        required: bool,
    },
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

    pub fn get_prefix(prefix: impl Into<String>, limit: i64, after: Option<String>) -> KvOp {
        KvOp::GetPrefix {
            prefix: prefix.into(),
            limit,
            after,
        }
    }

    /// Unconditional, forever.
    pub fn put(key: impl Into<String>, value: Value) -> KvOp {
        KvOp::Put {
            key: key.into(),
            value,
            ttl_seconds: None,
            expect: None,
            required: false,
        }
    }

    /// FENCED, forever: conditional AND required.
    pub fn fence(key: impl Into<String>, value: Value, expect: i64) -> KvOp {
        KvOp::Put {
            key: key.into(),
            value,
            ttl_seconds: None,
            expect: Some(expect),
            required: true,
        }
    }

    /// FENCED with a TTL: the lease refresh.
    pub fn fence_ttl(key: impl Into<String>, value: Value, expect: i64, ttl_seconds: u64) -> KvOp {
        KvOp::Put {
            key: key.into(),
            value,
            ttl_seconds: Some(ttl_seconds),
            expect: Some(expect),
            required: true,
        }
    }

    /// The lease claim: not required — losing it is the answer "somebody owns
    /// this", with the winner's version and value.
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

    /// The operation exactly as the KV route (and a transaction's `kv` rider)
    /// takes it.
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

/// EXACTLY ONE of `ttlSeconds` and `forever` (the broker refuses both or none).
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
    #[serde(default)]
    pub value: Value,
    /// Opaque, unique, never re-issued: compare for equality only.
    #[serde(default, deserialize_with = "null_as_default")]
    pub version: i64,
}

/// What one operation answered.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct KvResult {
    #[serde(default, deserialize_with = "null_as_default")]
    pub index: usize,
    #[serde(default)]
    pub op: String,
    /// `get` only.
    #[serde(default)]
    pub found: Option<bool>,
    #[serde(default)]
    pub key: Option<String>,
    /// Writes only.
    #[serde(default)]
    pub applied: Option<bool>,
    /// AFTER the operation when it applied; the WINNER's when it did not.
    #[serde(default, deserialize_with = "null_as_default")]
    pub version: i64,
    /// Writes that did not apply: `version`, `absent` or `exists`.
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub value: Value,
    #[serde(default)]
    pub rows: Vec<KvRow>,
    #[serde(default)]
    pub missing: Vec<String>,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub next_after: Option<String>,
}

impl KvResult {
    pub fn value_if_found(&self) -> Option<&Value> {
        match self.found {
            Some(true) => Some(&self.value),
            _ => None,
        }
    }

    pub fn did_apply(&self) -> bool {
        self.applied == Some(true)
    }
}

/// The KV call's answer. `ok == false` is the lost-`required`-precondition
/// verdict (nothing was written): `failed_index` names the op, `version` and
/// `value` the winner's row.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KvAnswer {
    #[serde(default = "yes")]
    pub ok: bool,
    #[serde(default)]
    pub reason: String,
    #[serde(default, deserialize_with = "null_as_default")]
    pub failed_index: usize,
    #[serde(default)]
    pub kv_reason: Option<String>,
    #[serde(default, deserialize_with = "null_as_default")]
    pub version: i64,
    #[serde(default)]
    pub value: Value,
    #[serde(default)]
    pub results: Vec<KvResult>,
}

fn yes() -> bool {
    true
}

/// The body of `POST /api/v1/kv` for these operations.
pub fn kv_body(ops: &[KvOp]) -> Value {
    serde_json::json!({ "operations": ops.iter().map(KvOp::to_json).collect::<Vec<_>>() })
}

/// Parse the KV route's 200 body.
pub fn parse_kv_answer(body: &str) -> Result<KvAnswer, QueenError> {
    serde_json::from_str::<KvAnswer>(body).map_err(|e| QueenError::Status {
        code: 200,
        body: format!(
            "unreadable KV answer ({e}): {}",
            body.chars().take(300).collect::<String>()
        ),
        retry_after_ms: None,
    })
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

/// The answer of `POST /api/v1/transaction` (HTTP 200 either way).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TxnAnswer {
    #[serde(default)]
    pub success: bool,
    /// On a rollback: `kv_precondition`, `duplicate`, `bad_request`, ….
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    /// One element per flat operation: pushes first (`type: "push"`), then the
    /// KV rider (`type: "kv"`, with `opIndex` = its index in the rider and the
    /// KV result fields: `applied`, `version`, …).
    #[serde(default)]
    pub results: Vec<Value>,
    /// `kv_precondition` only: the failed op's flat index, its reason and the
    /// winner's version/value.
    #[serde(default)]
    pub failed_index: Option<usize>,
    #[serde(default)]
    pub kv_reason: Option<String>,
    #[serde(default)]
    pub version: Option<i64>,
    #[serde(default)]
    pub value: Option<Value>,
}

impl TxnAnswer {
    /// The KV rider's result for rider op `op_index`, when the bundle committed.
    pub fn kv_result(&self, op_index: usize) -> Option<KvResult> {
        self.results
            .iter()
            .filter(|r| r.get("type").and_then(Value::as_str) == Some("kv"))
            .find(|r| r.get("opIndex").and_then(Value::as_u64) == Some(op_index as u64))
            .and_then(|r| serde_json::from_value::<KvResult>(r.clone()).ok())
    }

    pub fn is_precondition(&self) -> bool {
        !self.success && self.reason.as_deref() == Some("kv_precondition")
    }

    pub fn is_duplicate(&self) -> bool {
        !self.success && self.reason.as_deref() == Some("duplicate")
    }
}

/// Parse the transaction route's body.
pub fn parse_txn_answer(body: &str) -> Result<TxnAnswer, QueenError> {
    serde_json::from_str::<TxnAnswer>(body).map_err(|e| QueenError::Status {
        code: 200,
        body: format!(
            "unreadable transaction answer ({e}): {}",
            body.chars().take(300).collect::<String>()
        ),
        retry_after_ms: None,
    })
}

// ---------------------------------------------------------------------------
// Pop / ack
// ---------------------------------------------------------------------------

/// A wildcard pop for a consumer group, never auto-acked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PopRequest {
    pub queue: String,
    pub group: String,
    /// Messages per call (the broker's `batch`).
    pub batch: u32,
    /// Long-poll up to this long when nothing is ready (0 = answer at once).
    pub wait_ms: u64,
    /// The lease the claimed partitions get.
    pub lease_seconds: u32,
    /// `all` or `new`: how a group registers on its first pop.
    pub subscription_mode: String,
    /// How many partitions one call may claim; `None` lets the broker pick
    /// (autopilot).
    pub max_partitions: Option<u32>,
}

/// One delivered message.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Popped {
    #[serde(default)]
    pub transaction_id: String,
    /// The NUMERIC internal partition id, as text. Unique per partition
    /// incarnation: a partition recreated after it died gets a new id.
    #[serde(default)]
    pub partition_id: String,
    #[serde(default)]
    pub partition: String,
    #[serde(default)]
    pub lease_id: String,
    #[serde(default)]
    pub offset: i64,
    /// ISO-8601 UTC, as the broker renders it.
    #[serde(default)]
    pub created_at: String,
    /// The payload as stored: raw JSON text, `null` for an empty payload.
    pub data: Box<RawValue>,
    #[serde(default)]
    pub delivery_attempt: u32,
}

impl Popped {
    /// `partition_id` as a number (the sink's progress key).
    pub fn partition_number(&self) -> Option<i64> {
        self.partition_id.parse::<i64>().ok()
    }
}

/// A pop's answer: the messages, in claim order (each partition's in offset
/// order).
#[derive(Debug, Clone, Default)]
pub struct PopAnswer {
    pub messages: Vec<Popped>,
}

#[derive(Deserialize)]
struct PopBody {
    #[serde(default)]
    messages: Vec<Popped>,
}

/// Parse the pop route's body (`{"success":true,…,"messages":[…]}`); an empty
/// body (204) is no messages.
pub fn parse_pop_answer(body: &str) -> Result<PopAnswer, QueenError> {
    if body.trim().is_empty() {
        return Ok(PopAnswer::default());
    }
    serde_json::from_str::<PopBody>(body)
        .map(|b| PopAnswer {
            messages: b.messages,
        })
        .map_err(|e| QueenError::Status {
            code: 200,
            body: format!(
                "unreadable pop answer ({e}): {}",
                body.chars().take(300).collect::<String>()
            ),
            retry_after_ms: None,
        })
}

/// How a message is settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckStatus {
    /// Done.
    Ok,
    /// Dead-letter it now.
    Dlq,
    /// Failed (the queue's retry budget decides).
    Failed,
}

impl AckStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            AckStatus::Ok => "completed",
            AckStatus::Dlq => "dlq",
            AckStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckItem {
    pub transaction_id: String,
    pub partition_id: String,
    pub lease_id: String,
    pub status: AckStatus,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckRequest {
    pub group: String,
    pub items: Vec<AckItem>,
}

/// One item's verdict, input order.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AckItemResult {
    #[serde(default)]
    pub index: usize,
    #[serde(default)]
    pub transaction_id: String,
    #[serde(default)]
    pub success: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub lease_released: bool,
    #[serde(default)]
    pub dlq: bool,
    #[serde(default)]
    pub noop: bool,
}

#[derive(Debug, Clone, Default)]
pub struct AckAnswer {
    pub results: Vec<AckItemResult>,
}

/// The body of `POST /api/v1/ack/batch`.
pub fn ack_body(req: &AckRequest) -> Value {
    serde_json::json!({
        "consumerGroup": req.group,
        "acknowledgments": req.items.iter().map(|i| {
            let mut o = serde_json::json!({
                "transactionId": i.transaction_id,
                "partitionId": i.partition_id,
                "leaseId": i.lease_id,
                "status": i.status.as_str(),
            });
            if let Some(e) = &i.error {
                o["error"] = Value::String(e.clone());
            }
            o
        }).collect::<Vec<_>>(),
    })
}

/// Parse the ack route's body: a JSON array, or an object with `results`.
pub fn parse_ack_answer(body: &str) -> Result<AckAnswer, QueenError> {
    let v: Value = serde_json::from_str(body).map_err(|e| QueenError::Status {
        code: 200,
        body: format!("unreadable ack answer ({e})"),
        retry_after_ms: None,
    })?;
    let arr = match &v {
        Value::Array(a) => a.clone(),
        Value::Object(o) => o
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        _ => Vec::new(),
    };
    Ok(AckAnswer {
        results: arr
            .into_iter()
            .filter_map(|r| serde_json::from_value::<AckItemResult>(r).ok())
            .collect(),
    })
}

// ---------------------------------------------------------------------------
// The trait
// ---------------------------------------------------------------------------

/// The five calls the connectors make to Queen, for ONE tenant, implemented by
/// the broker in-process (server/src/pg_inproc.rs `LocalQueen`) and by
/// [`crate::fake::FakeQueen`] in tests.
pub trait QueenApi: Send + Sync {
    /// `POST /api/v1/transaction` with `body` (JSON text). A rollback verdict
    /// is `Ok(TxnAnswer { success: false, .. })`, never `Err`.
    fn transaction(&self, body: String) -> BoxFuture<'_, Result<TxnAnswer, QueenError>>;

    /// `POST /api/v1/kv`. A lost required precondition is
    /// `Ok(KvAnswer { ok: false, .. })`.
    fn kv(&self, ops: Vec<KvOp>) -> BoxFuture<'_, Result<KvAnswer, QueenError>>;

    /// A wildcard pop for `req.group`, `autoAck=false`.
    fn pop(&self, req: PopRequest) -> BoxFuture<'_, Result<PopAnswer, QueenError>>;

    /// `POST /api/v1/ack/batch`.
    fn ack(&self, req: AckRequest) -> BoxFuture<'_, Result<AckAnswer, QueenError>>;

    /// `POST /api/v1/lease/:leaseId/extend` by `seconds`.
    fn extend_lease(&self, lease_id: String, seconds: u32)
        -> BoxFuture<'_, Result<(), QueenError>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fenced_put_says_forever_expect_and_required() {
        let v = KvOp::fence("src:a:pointer", serde_json::json!({"lsn":"0/1"}), 7).to_json();
        assert_eq!(v["ns"], "queen-pg");
        assert_eq!(v["op"], "put");
        assert_eq!(v["forever"], true);
        assert!(v.get("ttlSeconds").is_none());
        assert_eq!(v["expect"], 7);
        assert_eq!(v["required"], true);
    }

    #[test]
    fn a_transaction_answer_finds_its_kv_result_by_op_index() {
        let a = parse_txn_answer(
            r#"{"transactionId":"t","success":true,"results":[
                {"index":0,"type":"push","success":true,"transactionId":"x"},
                {"index":1,"opIndex":0,"type":"kv","op":"put","applied":true,"version":42}]}"#,
        )
        .unwrap();
        assert!(a.success);
        assert_eq!(a.kv_result(0).unwrap().version, 42);
        assert!(a.kv_result(1).is_none());
    }

    #[test]
    fn a_rollback_verdict_parses() {
        let a = parse_txn_answer(
            r#"{"transactionId":"t","success":false,"reason":"kv_precondition","error":"QKV","results":[],"ok":false,"failedIndex":3,"kvReason":"version","version":9,"value":{"lsn":"0/2"}}"#,
        )
        .unwrap();
        assert!(a.is_precondition());
        assert_eq!(a.version, Some(9));
    }

    #[test]
    fn pop_and_ack_bodies_round_trip() {
        let p = parse_pop_answer(
            r#"{"success":true,"queue":"q","messages":[{"id":"m","transactionId":"t1","data":{"a":12345678901234567890},"partitionId":"17","partition":"p","leaseId":"L","offset":4,"createdAt":"2026-10-02T10:00:00.000Z","deliveryAttempt":1}],"partitionsClaimed":1}"#,
        )
        .unwrap();
        assert_eq!(p.messages.len(), 1);
        assert_eq!(p.messages[0].partition_number(), Some(17));
        assert_eq!(p.messages[0].data.get(), r#"{"a":12345678901234567890}"#);
        let a = parse_ack_answer(
            r#"[{"index":0,"transactionId":"t1","success":true,"error":null,"leaseReleased":true,"dlq":false,"noop":false}]"#,
        )
        .unwrap();
        assert!(a.results[0].success);
        let body = ack_body(&AckRequest {
            group: "g".into(),
            items: vec![AckItem {
                transaction_id: "t1".into(),
                partition_id: "17".into(),
                lease_id: "L".into(),
                status: AckStatus::Dlq,
                error: Some("bad".into()),
            }],
        });
        assert_eq!(body["acknowledgments"][0]["status"], "dlq");
    }
}
