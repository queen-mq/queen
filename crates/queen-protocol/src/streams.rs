//! `/streams/v1/*` — the streaming engine's wire format.
//!
//! Three endpoints, and the interesting one is the third:
//!
//! * `POST /streams/v1/queries` — idempotent query registration. Answers 409
//!   when the chain's `config_hash` differs from what was registered before,
//!   which is the guard against redeploying a changed pipeline onto state
//!   computed by the old one.
//! * `POST /streams/v1/state/get` — read the operator state rows for one
//!   `(query_id, partition_id)`.
//! * `POST /streams/v1/cycle` — commit state mutations, sink pushes and the
//!   source ack **in one PostgreSQL transaction**. This is what makes the
//!   engine exactly-once against its own state: either the window advanced,
//!   the output was written and the input was acked, or none of it happened.

use serde::{Deserialize, Serialize};

// --------------------------------------------------------------- register

/// Body of `POST /streams/v1/queries`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisterRequest {
    /// The durable identity of the query. Two processes using the same name
    /// share state and cursor — which is how a stream scales horizontally.
    pub name: String,

    pub source_queue: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink_queue: Option<String>,

    /// Fingerprint of the operator chain's *shape*. Not of the user's
    /// closures — those cannot be serialized stably and change for cosmetic
    /// reasons; what matters is that a window size or a reordered chain does
    /// not silently reinterpret existing state.
    pub config_hash: String,

    /// Wipe the query's state and accept the new `config_hash`.
    #[serde(default)]
    pub reset: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RegisterResponse {
    #[serde(default = "default_true")]
    pub success: bool,

    /// The canonical id every later call uses. Distinct from the `name` the
    /// caller chose.
    #[serde(default)]
    pub query_id: String,

    #[serde(default)]
    pub name: String,

    /// The query did not exist before this call.
    #[serde(default)]
    pub fresh: bool,

    /// The registration wiped existing state.
    #[serde(default)]
    pub reset: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn default_true() -> bool {
    true
}

// ------------------------------------------------------------------ state

/// Body of `POST /streams/v1/state/get`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateGetRequest {
    pub query_id: String,
    pub partition_id: String,

    /// Empty means "every row for this partition". The broker defaults the
    /// field to `[]` when absent, so it is always sent explicitly.
    #[serde(default)]
    pub keys: Vec<String>,

    /// Restrict to keys starting with this — how the idle flush scopes its
    /// scan to a single window operator's rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_prefix: Option<String>,

    /// Restrict to rows whose window has closed at or before this epoch-ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ripe_at_or_before: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateRow {
    pub key: String,
    #[serde(default)]
    pub value: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StateGetResponse {
    #[serde(default = "default_true")]
    pub success: bool,

    #[serde(default)]
    pub rows: Vec<StateRow>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ------------------------------------------------------------------ cycle

/// A mutation to the query's state, applied inside the cycle transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StateOp {
    Upsert {
        key: String,
        value: serde_json::Value,
    },
    Delete {
        key: String,
    },
}

impl StateOp {
    pub fn key(&self) -> &str {
        match self {
            Self::Upsert { key, .. } | Self::Delete { key } => key,
        }
    }
}

/// A message the cycle emits to a sink queue.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SinkPushItem {
    pub queue: String,

    /// Defaults to `Default` broker-side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,

    pub payload: serde_json::Value,

    /// The broker mints a uuidv7 when absent, and defaults the transaction id
    /// to it.
    #[serde(rename = "messageId", default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,

    #[serde(
        rename = "transactionId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub transaction_id: Option<String>,
}

/// The source ack a cycle carries.
///
/// `count` is how many source messages this cycle covers, which the broker
/// needs to decide whether the whole leased batch was consumed. A gate that
/// stopped early reports the shorter prefix.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CycleAck {
    #[serde(rename = "transactionId")]
    pub transaction_id: String,

    #[serde(rename = "leaseId")]
    pub lease_id: String,

    /// `completed` or a nack spelling; normalized broker-side by the same
    /// table as a plain ack.
    pub status: String,

    pub count: i64,
}

/// Body of `POST /streams/v1/cycle`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CycleRequest {
    pub query_id: String,
    pub partition_id: String,
    pub consumer_group: String,

    #[serde(default)]
    pub state_ops: Vec<StateOp>,

    #[serde(default)]
    pub push_items: Vec<SinkPushItem>,

    /// `None` on an idle-flush cycle, which advances no cursor.
    #[serde(default)]
    pub ack: Option<CycleAck>,

    /// False keeps the source lease so an un-acked tail is redelivered *to
    /// this worker's successor in order* rather than becoming claimable by
    /// anyone. The gate operator's whole ordering guarantee rests on it.
    pub release_lease: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CycleAckResult {
    #[serde(default)]
    pub count: i64,
    #[serde(default)]
    pub lease_released: bool,
    #[serde(default)]
    pub dlq: bool,
}

/// Response of a cycle. **Always HTTP 200** on a completed procedure call, so
/// `success` is the only signal — a rolled-back cycle looks like a successful
/// request.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CycleResponse {
    #[serde(default = "default_true")]
    pub success: bool,

    #[serde(default)]
    pub query_id: String,

    #[serde(default)]
    pub partition_id: String,

    /// The *source* queue's name, resolved broker-side.
    #[serde(rename = "queueName", default)]
    pub queue_name: String,

    #[serde(default)]
    pub state_ops_applied: i64,

    #[serde(default)]
    pub push_results: serde_json::Value,

    #[serde(default)]
    pub ack_result: Option<CycleAckResult>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Reserved state key holding a partition's event-time watermark.
///
/// User keys must never start with `__`: the reducer skips anything whose
/// operator tag begins with it, so a user key in that space would be silently
/// ignored rather than accumulated.
pub const WATERMARK_STATE_KEY: &str = "__wm__";

/// Field separator inside a state key. ASCII unit separator, chosen because it
/// cannot appear in a window key (an ISO timestamp) and is vanishingly
/// unlikely in a user key.
pub const STATE_KEY_SEP: char = '\u{1f}';

/// Build a windowed reducer's state key: `{tag}␟{windowKey}␟{userKey}`.
///
/// The operator tag is what lets two window operators in one query share the
/// state table without colliding, and what lets the idle flush scan one
/// operator's rows by prefix.
pub fn state_key_for(operator_tag: &str, window_key: &str, user_key: &str) -> String {
    format!("{operator_tag}{STATE_KEY_SEP}{window_key}{STATE_KEY_SEP}{user_key}")
}

/// The three parts of a state key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateKeyParts {
    pub operator_tag: String,
    pub window_key: String,
    pub user_key: String,
}

/// Decompose a state key. Two-part keys are the pre-tag layout and parse with
/// an empty operator tag, matching the other SDKs.
pub fn parse_state_key(state_key: &str) -> Option<StateKeyParts> {
    let parts: Vec<&str> = state_key.split(STATE_KEY_SEP).collect();
    match parts.len() {
        3 => Some(StateKeyParts {
            operator_tag: parts[0].to_string(),
            window_key: parts[1].to_string(),
            user_key: parts[2].to_string(),
        }),
        2 => Some(StateKeyParts {
            operator_tag: String::new(),
            window_key: parts[0].to_string(),
            user_key: parts[1].to_string(),
        }),
        _ => None,
    }
}

/// A session window's open-state key: `{tag}␟open␟{userKey}`.
pub fn session_state_key(operator_tag: &str, user_key: &str) -> String {
    format!("{operator_tag}{STATE_KEY_SEP}open{STATE_KEY_SEP}{user_key}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_keys_round_trip() {
        let k = state_key_for("tumb:60", "2026-08-04T10:00:00.000Z", "user-1");
        assert_eq!(k, "tumb:60\u{1f}2026-08-04T10:00:00.000Z\u{1f}user-1");
        let p = parse_state_key(&k).unwrap();
        assert_eq!(p.operator_tag, "tumb:60");
        assert_eq!(p.window_key, "2026-08-04T10:00:00.000Z");
        assert_eq!(p.user_key, "user-1");
    }

    #[test]
    fn two_part_keys_parse_with_an_empty_tag() {
        let p = parse_state_key("2026-08-04T10:00:00.000Z\u{1f}user-1").unwrap();
        assert_eq!(p.operator_tag, "");
        assert_eq!(p.user_key, "user-1");
        // and anything else is not a state key
        assert!(parse_state_key("nonsense").is_none());
        assert!(parse_state_key("a\u{1f}b\u{1f}c\u{1f}d").is_none());
    }

    #[test]
    fn session_keys_carry_the_open_marker() {
        assert_eq!(
            session_state_key("sess:30", "user-1"),
            "sess:30\u{1f}open\u{1f}user-1"
        );
    }

    #[test]
    fn state_ops_are_tagged_by_type() {
        let ops = vec![
            StateOp::Upsert {
                key: "k1".into(),
                value: serde_json::json!({ "acc": 5 }),
            },
            StateOp::Delete { key: "k2".into() },
        ];
        let s = serde_json::to_string(&ops).unwrap();
        assert_eq!(
            s,
            r#"[{"type":"upsert","key":"k1","value":{"acc":5}},{"type":"delete","key":"k2"}]"#
        );
        assert_eq!(ops[0].key(), "k1");
        assert_eq!(ops[1].key(), "k2");
    }

    #[test]
    fn a_flush_cycle_carries_a_null_ack() {
        let req = CycleRequest {
            query_id: "q".into(),
            partition_id: "p".into(),
            consumer_group: "g".into(),
            state_ops: vec![],
            push_items: vec![],
            ack: None,
            release_lease: true,
        };
        let s = serde_json::to_string(&req).unwrap();
        assert!(s.contains(r#""ack":null"#), "{s}");
        assert!(s.contains(r#""release_lease":true"#), "{s}");
    }

    #[test]
    fn a_gate_cycle_retains_the_lease() {
        let req = CycleRequest {
            query_id: "q".into(),
            partition_id: "p".into(),
            consumer_group: "g".into(),
            state_ops: vec![],
            push_items: vec![],
            ack: Some(CycleAck {
                transaction_id: "t".into(),
                lease_id: "L".into(),
                status: "completed".into(),
                count: 3,
            }),
            release_lease: false,
        };
        let s = serde_json::to_string(&req).unwrap();
        assert!(s.contains(r#""release_lease":false"#), "{s}");
        assert!(s.contains(r#""count":3"#), "{s}");
    }

    #[test]
    fn a_rolled_back_cycle_is_still_a_200() {
        let resp: CycleResponse = serde_json::from_str(
            r#"{"success":false,"error":"deadlock detected","query_id":"q","partition_id":"p"}"#,
        )
        .unwrap();
        assert!(!resp.success);
        assert_eq!(resp.error.as_deref(), Some("deadlock detected"));
    }

    #[test]
    fn register_response_reports_reset_and_freshness() {
        let resp: RegisterResponse = serde_json::from_str(
            r#"{"success":true,"query_id":"0190-aaaa","name":"my-query","fresh":true,"reset":false}"#,
        )
        .unwrap();
        assert!(resp.success);
        assert!(resp.fresh);
        assert!(!resp.reset);
        assert_eq!(resp.query_id, "0190-aaaa");
    }

    #[test]
    fn sink_items_omit_what_the_broker_will_mint() {
        let item = SinkPushItem {
            queue: "out".into(),
            partition: Some("eu".into()),
            payload: serde_json::json!({ "n": 1 }),
            message_id: None,
            transaction_id: None,
        };
        assert_eq!(
            serde_json::to_string(&item).unwrap(),
            r#"{"queue":"out","partition":"eu","payload":{"n":1}}"#
        );
    }
}
