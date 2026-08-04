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

use serde::{Deserialize, Deserializer, Serialize};

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

/// Read a string field that the broker may send as JSON `null`.
fn null_as_empty<'de, D>(d: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(d)?.unwrap_or_default())
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

    /// The *source* queue's name, resolved broker-side. Empty when the broker
    /// could not resolve it.
    ///
    /// The SP emits this key as SQL NULL whenever the partition lookup found
    /// nothing (`server/sql/procedures/007_log_streams.sql:171` initializes
    /// `v_source_queue_name` to NULL and only :193 ever sets it), and
    /// `jsonb_build_object` keeps nulls — so both the success shape at :447-455
    /// and, far more often, the EXCEPTION shape at :457-467 can carry
    /// `"queueName": null`. `#[serde(default)]` alone does not cover that: it
    /// fills an *absent* key, not a present null, so a rolled-back cycle failed
    /// to deserialize at all and the runner reported a decode error instead of
    /// the `error` string sitting right next to it. The broker itself expects
    /// the null (`server/src/handlers/streams.rs:446`, "bumped even if
    /// queueName came back NULL").
    #[serde(rename = "queueName", default, deserialize_with = "null_as_empty")]
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

    // ------------------------------------------------------------------
    // `handle_streams_cycle` is the one handler that never deserializes its
    // body into a struct — it reads `serde_json::Value` field by field
    // (`server/src/handlers/streams.rs:165-225` and :311-318). A key it cannot
    // find is not an error there, it is a default: an unfound `release_lease`
    // becomes true and silently releases the lease a gate operator was holding
    // to keep its ordering; an unfound `leaseId` becomes an empty worker and
    // the SP rejects the ack, rolling the cycle back. The readers below walk
    // exactly those keys.
    // ------------------------------------------------------------------

    #[derive(Debug, PartialEq)]
    struct SeenCycle {
        query_id: String,
        partition_id: String,
        consumer_group: String,
        release_lease: bool,
        state_ops: serde_json::Value,
        /// (queue, partition, payload, messageId, transactionId)
        push_items: Vec<(String, String, serde_json::Value, String, String)>,
        /// (worker, ok, count) — None on an idle-flush cycle.
        ack: Option<(String, bool, i64)>,
    }

    fn read_cycle_like_the_broker(body: &str) -> SeenCycle {
        let root: serde_json::Value = serde_json::from_str(body).expect("body must be JSON");
        let mut push_items = Vec::new();
        if let Some(items) = root.get("push_items").and_then(|x| x.as_array()) {
            for pi in items {
                let queue = pi
                    .get("queue")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                if queue.is_empty() {
                    continue; // no sink queue -> nothing to push
                }
                push_items.push((
                    queue,
                    // NB the non-empty filter, which the transaction path does
                    // NOT have: here an explicit "" does fall back to Default.
                    pi.get("partition")
                        .and_then(|x| x.as_str())
                        .filter(|s| !s.is_empty())
                        .unwrap_or(crate::DEFAULT_PARTITION)
                        .to_string(),
                    pi.get("payload")
                        .cloned()
                        .or_else(|| pi.get("data").cloned())
                        .unwrap_or_else(|| serde_json::Value::Object(Default::default())),
                    pi.get("messageId")
                        .and_then(|x| x.as_str())
                        .filter(|s| !s.is_empty())
                        .unwrap_or("<broker mints a uuidv7>")
                        .to_string(),
                    pi.get("transactionId")
                        .and_then(|x| x.as_str())
                        .filter(|s| !s.is_empty())
                        .unwrap_or("<defaults to the messageId>")
                        .to_string(),
                ));
            }
        }
        let ack = match root.get("ack") {
            Some(a) if !a.is_null() => Some((
                a.get("leaseId")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string(),
                // status_is_ok's table, same as a plain ack's.
                matches!(
                    a.get("status").and_then(|x| x.as_str()),
                    None | Some("completed") | Some("success") | Some("acked") | Some("ok")
                ),
                a.get("count").and_then(|x| x.as_i64()).unwrap_or(0),
            )),
            _ => None,
        };
        SeenCycle {
            query_id: root
                .get("query_id")
                .and_then(|x| x.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("<400: query_id is required>")
                .to_string(),
            partition_id: root
                .get("partition_id")
                .and_then(|x| x.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("<400: partition_id is required>")
                .to_string(),
            consumer_group: root
                .get("consumer_group")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            release_lease: root
                .get("release_lease")
                .and_then(|x| x.as_bool())
                .unwrap_or(true),
            state_ops: root
                .get("state_ops")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([])),
            push_items,
            ack,
        }
    }

    #[test]
    fn a_cycle_body_is_read_key_for_key_by_the_broker() {
        // The body is snake_case at the top level and camelCase INSIDE the ack
        // — `leaseId`, not `lease_id`. That inconsistency is the trap: a
        // tidy-looking rename of the ack's keys costs the worker id, the SP
        // sees an empty worker, and every gate cycle rolls back with an invalid
        // lease while the request still looks perfectly well-formed.
        let req = CycleRequest {
            query_id: "0198f0aa-0000-7000-8000-0000000000q1".into(),
            partition_id: "0198f0aa-0000-7000-8000-0000000000e1".into(),
            consumer_group: "windower".into(),
            state_ops: vec![StateOp::Upsert {
                key: state_key_for("tumb:60", "2026-08-04T10:00:00.000Z", "user-1"),
                value: serde_json::json!({"acc": 5}),
            }],
            push_items: vec![SinkPushItem {
                queue: "windows".into(),
                partition: Some("eu".into()),
                payload: serde_json::json!({"sum": 5}),
                message_id: None,
                transaction_id: None,
            }],
            ack: Some(CycleAck {
                transaction_id: "order-1".into(),
                lease_id: "0198f0aa-0000-7000-8000-00000000a001".into(),
                status: "completed".into(),
                count: 3,
            }),
            release_lease: false,
        };
        let seen = read_cycle_like_the_broker(&serde_json::to_string(&req).unwrap());

        assert_eq!(seen.query_id, req.query_id);
        assert_eq!(seen.partition_id, req.partition_id);
        assert_eq!(seen.consumer_group, "windower");
        assert!(
            !seen.release_lease,
            "a gate cycle must keep its lease; the broker's default here is TRUE, \
             so an unread key silently releases it"
        );
        assert_eq!(
            seen.state_ops,
            serde_json::json!([{
                "type": "upsert",
                "key": state_key_for("tumb:60", "2026-08-04T10:00:00.000Z", "user-1"),
                "value": {"acc": 5},
            }])
        );
        assert_eq!(
            seen.ack,
            Some(("0198f0aa-0000-7000-8000-00000000a001".to_string(), true, 3)),
            "the ack's leaseId/status/count are read under camelCase keys"
        );

        let (queue, partition, payload, mid, txn) = &seen.push_items[0];
        assert_eq!((queue.as_str(), partition.as_str()), ("windows", "eu"));
        assert_eq!(payload, &serde_json::json!({"sum": 5}));
        assert!(
            mid.starts_with('<') && txn.starts_with('<'),
            "omitting messageId/transactionId must leave the broker to mint them"
        );
    }

    #[test]
    fn an_idle_flush_cycle_reads_as_no_ack_at_all() {
        // The flush cycle serializes `"ack": null`, and the broker branches on
        // `Some(a) if !a.is_null()`. If the field were ever skipped instead of
        // nulled the branch would still take the same leg, but a `{}` or a
        // zero-count ack would not: it would advance a cursor over messages
        // that were never delivered.
        let req = CycleRequest {
            query_id: "q".into(),
            partition_id: "p".into(),
            consumer_group: "g".into(),
            state_ops: vec![],
            push_items: vec![],
            ack: None,
            release_lease: true,
        };
        let seen = read_cycle_like_the_broker(&serde_json::to_string(&req).unwrap());
        assert_eq!(seen.ack, None);
        assert!(seen.push_items.is_empty());
    }

    #[test]
    fn a_nacking_cycle_is_read_as_not_ok() {
        // `status_is_ok` uses the plain ack's alias table, so anything outside
        // it is a nack. A cycle that reports "failed" must not be read as a
        // completion, or the source cursor advances past messages the pipeline
        // rejected.
        let mut req = CycleRequest {
            query_id: "q".into(),
            partition_id: "p".into(),
            consumer_group: "g".into(),
            state_ops: vec![],
            push_items: vec![],
            ack: Some(CycleAck {
                transaction_id: "t".into(),
                lease_id: "L".into(),
                status: "failed".into(),
                count: 2,
            }),
            release_lease: true,
        };
        let seen = read_cycle_like_the_broker(&serde_json::to_string(&req).unwrap());
        assert_eq!(seen.ack, Some(("L".to_string(), false, 2)));

        for ok_spelling in ["completed", "success", "acked", "ok"] {
            req.ack.as_mut().unwrap().status = ok_spelling.to_string();
            let seen = read_cycle_like_the_broker(&serde_json::to_string(&req).unwrap());
            assert!(
                seen.ack.unwrap().1,
                "{ok_spelling} must read as a completion"
            );
        }
    }

    #[test]
    fn a_sink_push_with_no_queue_is_dropped_rather_than_rejected() {
        // The broker `continue`s past an item with an empty queue
        // (streams.rs:196-199), so a mis-built sink emits nothing and the cycle
        // still reports success. Nothing upstream surfaces it, which is worth
        // knowing before chasing a silently empty sink.
        let req = CycleRequest {
            query_id: "q".into(),
            partition_id: "p".into(),
            consumer_group: "g".into(),
            state_ops: vec![],
            push_items: vec![
                SinkPushItem {
                    queue: String::new(),
                    partition: None,
                    payload: serde_json::json!({"lost": true}),
                    message_id: None,
                    transaction_id: None,
                },
                SinkPushItem {
                    queue: "out".into(),
                    partition: None,
                    payload: serde_json::json!({"kept": true}),
                    message_id: None,
                    transaction_id: None,
                },
            ],
            ack: None,
            release_lease: true,
        };
        let seen = read_cycle_like_the_broker(&serde_json::to_string(&req).unwrap());
        assert_eq!(
            seen.push_items.len(),
            1,
            "the queue-less item vanished silently"
        );
        assert_eq!(seen.push_items[0].1, crate::DEFAULT_PARTITION);
    }

    /// A committed cycle result, transcribed from the `jsonb_build_object` in
    /// `server/sql/procedures/007_log_streams.sql:447-455`, with the
    /// `ack_result` from :439-444 and a `push_results` element from :323-327.
    /// The handler returns this inner object verbatim
    /// (`server/src/handlers/streams.rs:355`, `json(status, result.to_string())`).
    const COMMITTED_CYCLE_FROM_THE_SP: &str = concat!(
        r#"{"success":true,"query_id":"0198f0aa-0000-7000-8000-0000000000q1","#,
        r#""partition_id":"0198f0aa-0000-7000-8000-0000000000e1","queueName":"events","#,
        r#""state_ops_applied":2,"#,
        r#""push_results":[{"queue":"windows","partition":"eu","ok":true,"count":1}],"#,
        r#""ack_result":{"success":true,"count":3,"lease_released":true,"dlq":false}}"#,
    );

    #[test]
    fn a_committed_cycle_result_parses_with_its_ack_result() {
        let got: CycleResponse = serde_json::from_str(COMMITTED_CYCLE_FROM_THE_SP)
            .expect("the object the SP builds for a committed cycle must deserialize");
        assert!(got.success);
        assert_eq!(got.queue_name, "events");
        assert_eq!(got.state_ops_applied, 2);
        let ack = got
            .ack_result
            .expect("a cycle that acked must report an ack_result");
        assert_eq!(ack.count, 3);
        assert!(
            ack.lease_released,
            "lease_released is what the broker's promote-on-ack branch keys on"
        );
        assert!(!ack.dlq);
        // The SP puts a `success` key inside ack_result too (:440) that this
        // struct does not model; it must be dropped, not rejected.
        assert_eq!(got.push_results[0]["queue"], serde_json::json!("windows"));
    }

    #[test]
    fn a_gate_partial_ack_reports_the_lease_retained() {
        // A gate cycle that acked only a prefix keeps the lease, and the SP
        // says so with lease_released:false. That flag is the difference
        // between the tail being redelivered to this worker's successor in
        // order and it becoming claimable by anyone.
        let wire = concat!(
            r#"{"success":true,"query_id":"q","partition_id":"p","queueName":"events","#,
            r#""state_ops_applied":0,"push_results":[],"#,
            r#""ack_result":{"success":true,"count":2,"lease_released":false,"dlq":false}}"#,
        );
        let got: CycleResponse = serde_json::from_str(wire).unwrap();
        assert!(!got.ack_result.unwrap().lease_released);
    }

    #[test]
    fn an_idle_flush_result_carries_a_null_ack_result() {
        // `v_ack_result` starts as the jsonb scalar 'null' (007:169) and stays
        // that way when the cycle carried no ack, so the key IS present and
        // explicitly null rather than absent.
        let wire = concat!(
            r#"{"success":true,"query_id":"q","partition_id":"p","queueName":"events","#,
            r#""state_ops_applied":5,"push_results":[],"ack_result":null}"#,
        );
        let got: CycleResponse = serde_json::from_str(wire).unwrap();
        assert!(got.ack_result.is_none());
        assert_eq!(got.state_ops_applied, 5);
    }

    #[test]
    fn a_rolled_back_cycle_still_reports_its_reason_when_the_queue_is_unknown() {
        // The SP's EXCEPTION arm (007:457-467) is the one a caller most needs
        // to read, and it is also the one where `queueName` is most often SQL
        // NULL: `v_source_queue_name` is reset to NULL at :171 and only set by
        // the partition lookup at :193, which is exactly what has not happened
        // when the failure is a bad query_id or partition_id.
        //
        // `#[serde(default)]` does not cover a present null, so this body used
        // to fail to deserialize outright — and the runner, which exists to
        // surface `error` on a rollback, reported a decode failure instead of
        // the reason sitting one key away.
        let wire = concat!(
            r#"{"success":false,"query_id":"","partition_id":"","queueName":null,"#,
            r#""state_ops_applied":0,"push_results":[],"ack_result":null,"#,
            r#""error":"query_id is required"}"#,
        );
        let got: CycleResponse = serde_json::from_str(wire)
            .expect("a null queueName must not cost the caller the rollback reason");
        assert!(!got.success);
        assert_eq!(got.error.as_deref(), Some("query_id is required"));
        assert_eq!(got.queue_name, "", "an unresolvable queue reads as empty");
    }

    #[test]
    fn a_cycle_result_from_a_newer_broker_still_parses() {
        let wire = concat!(
            r#"{"success":true,"query_id":"q","partition_id":"p","queueName":"events","#,
            r#""state_ops_applied":0,"push_results":[],"ack_result":null,"watermark":1234}"#,
        );
        let got: CycleResponse =
            serde_json::from_str(wire).expect("an unmodelled key must not fail the decode");
        assert!(got.success);
    }

    // ---------------------------------------------------------- register

    #[test]
    fn a_register_request_uses_the_keys_the_handler_validates() {
        // The handler rejects the request before the SP when `name`,
        // `source_queue` or `config_hash` is missing or empty
        // (`server/src/handlers/streams.rs:49-57`), and the SP destructures the
        // same names plus `sink_queue`/`reset`
        // (`server/sql/procedures/008_streams_register_query_v1.sql:82-86`).
        // These are snake_case on a wire that is camelCase everywhere else.
        let body = serde_json::to_string(&RegisterRequest {
            name: "hourly-totals".into(),
            source_queue: "events".into(),
            sink_queue: Some("windows".into()),
            config_hash: "abc123".into(),
            reset: true,
        })
        .unwrap();
        let root: serde_json::Value = serde_json::from_str(&body).unwrap();
        for required in ["name", "source_queue", "config_hash"] {
            assert!(
                root.get(required)
                    .and_then(|x| x.as_str())
                    .is_some_and(|s| !s.is_empty()),
                "the handler answers 400 without a non-empty `{required}`: {body}"
            );
        }
        assert_eq!(
            root.get("sink_queue").unwrap(),
            &serde_json::json!("windows")
        );
        assert_eq!(root.get("reset").unwrap(), &serde_json::json!(true));
    }

    #[test]
    fn a_sink_less_query_omits_the_key_the_sql_nullifs() {
        // The SP does `NULLIF(v_req->>'sink_queue', '')`, so an omitted key and
        // an empty string are the same thing — but sending `"sink_queue": null`
        // is what a naive Option serialization would do, and this type omits it
        // instead.
        let body = serde_json::to_string(&RegisterRequest {
            name: "n".into(),
            source_queue: "events".into(),
            sink_queue: None,
            config_hash: "h".into(),
            reset: false,
        })
        .unwrap();
        assert!(!body.contains("sink_queue"), "{body}");
        // `reset` is not skipped: the SP COALESCEs it, but sending it flat
        // keeps the "did I ask for a wipe" question answerable from the body.
        assert!(body.contains(r#""reset":false"#), "{body}");
    }

    #[test]
    fn a_successful_registration_parses_and_ignores_the_echoed_config_hash() {
        // Transcribed from
        // `server/sql/procedures/008_streams_register_query_v1.sql:147-154`.
        // The SP echoes `config_hash`, which this type does not model — proof
        // that the unknown-key path is exercised by the real wire and not only
        // by hypothetical future fields.
        let wire = concat!(
            r#"{"success":true,"query_id":"0198f0aa-0000-7000-8000-0000000000q1","#,
            r#""name":"hourly-totals","config_hash":"abc123","fresh":true,"reset":false}"#,
        );
        let got: RegisterResponse =
            serde_json::from_str(wire).expect("the SP's success shape must deserialize");
        assert!(got.success && got.fresh && !got.reset);
        assert_eq!(got.query_id, "0198f0aa-0000-7000-8000-0000000000q1");
        assert_eq!(
            got.name, "hourly-totals",
            "`name` is the caller's chosen name; `query_id` is the broker's id, \
             and later calls must use the latter"
        );
    }

    #[test]
    fn a_config_hash_mismatch_parses_from_the_409_body() {
        // `008:136-142` — the shape behind the 409 the handler maps at
        // `server/src/handlers/streams.rs:74`. It carries NEITHER `fresh` nor
        // `reset`, so both defaults have to hold; reading a missing `reset` as
        // anything but false would tell a caller its state had been wiped when
        // the registration was in fact refused.
        let wire = concat!(
            r#"{"success":false,"query_id":"0198f0aa-0000-7000-8000-0000000000q1","#,
            r#""name":"hourly-totals","#,
            r#""error":"config_hash mismatch: operator chain changed for queryId "#,
            r#"'hourly-totals'. Pass reset:true to wipe existing state, or use a new queryId."}"#,
        );
        let got: RegisterResponse = serde_json::from_str(wire).unwrap();
        assert!(!got.success);
        assert!(!got.fresh && !got.reset, "absent must not read as true");
        assert!(got.error.unwrap().contains("reset:true"));
    }

    // --------------------------------------------------------- state get

    #[test]
    fn a_state_get_request_uses_the_keys_the_sql_destructures() {
        // `server/sql/procedures/009_streams_state_get_v1.sql:65-80` reads
        // exactly these names. `keys` is sent even when empty because the SP's
        // array coercion (:73-75) treats a non-array as `[]` and the handler
        // fills an absent key in anyway (:96) — sending it explicitly is what
        // keeps the three paths from disagreeing.
        let body = serde_json::to_string(&StateGetRequest {
            query_id: "q".into(),
            partition_id: "p".into(),
            keys: vec![],
            key_prefix: Some("tumb:60\u{1f}".into()),
            ripe_at_or_before: Some(1_754_300_000_000),
        })
        .unwrap();
        let root: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(root.get("keys").unwrap(), &serde_json::json!([]));
        assert!(root.get("key_prefix").is_some());
        assert!(
            root.get("ripe_at_or_before").is_some_and(|v| v.is_number()),
            "the SP only honours ripe_at_or_before when jsonb_typeof is 'number' \
             (009:77-81), so a stringified timestamp is silently ignored: {body}"
        );
    }

    #[test]
    fn an_unfiltered_state_get_omits_the_two_optional_filters() {
        let body = serde_json::to_string(&StateGetRequest {
            query_id: "q".into(),
            partition_id: "p".into(),
            keys: vec!["k1".into()],
            key_prefix: None,
            ripe_at_or_before: None,
        })
        .unwrap();
        assert!(!body.contains("key_prefix"), "{body}");
        assert!(!body.contains("ripe_at_or_before"), "{body}");
    }

    #[test]
    fn state_rows_parse_as_the_sql_projects_them() {
        // `009:97-101` builds each row as {key, value, updated_at}; the
        // envelope at :126-132 wraps them in {success, rows}. The windowed key
        // goes through `state_key_for` rather than being typed out: its
        // separator is an unprintable ASCII unit separator, and a literal one
        // in this file would be indistinguishable from a space on inspection.
        let windowed = state_key_for("tumb:60", "2026-08-04T10:00:00.000Z", "user-1");
        let wire = serde_json::json!({
            "success": true,
            "rows": [
                {
                    "key": windowed,
                    "value": { "acc": 5, "windowEnd": 1_754_301_600_000i64 },
                    "updated_at": "2026-08-04T10:00:05.123Z",
                },
                { "key": WATERMARK_STATE_KEY, "value": 1_754_301_599_000i64, "updated_at": null },
            ],
        })
        .to_string();
        let got: StateGetResponse =
            serde_json::from_str(&wire).expect("the SP's rows shape must deserialize");
        assert!(got.success);
        assert_eq!(got.rows.len(), 2);

        let parts = parse_state_key(&got.rows[0].key).expect("a windowed key must decompose");
        assert_eq!(parts.operator_tag, "tumb:60");
        assert_eq!(parts.user_key, "user-1");
        assert_eq!(got.rows[0].value["acc"], serde_json::json!(5));

        // The watermark row is the reserved key, and its value is a bare
        // number rather than an object — `value` has to stay untyped.
        assert_eq!(got.rows[1].key, WATERMARK_STATE_KEY);
        assert!(got.rows[1].value.is_number());
        assert!(
            got.rows[1].updated_at.is_none(),
            "a null updated_at must not fail the row"
        );
    }

    #[test]
    fn a_rejected_state_get_carries_an_empty_rows_array() {
        // `009:85-92`. The handler maps success:false to a 400
        // (`server/src/handlers/streams.rs:113`), so this body arrives with a
        // 4xx and still has to decode for the reason to be readable.
        let wire = concat!(
            r#"{"success":false,"error":"query_id and partition_id are required","#,
            r#""rows":[]}"#,
        );
        let got: StateGetResponse = serde_json::from_str(wire).unwrap();
        assert!(!got.success);
        assert!(got.rows.is_empty());
        assert_eq!(
            got.error.as_deref(),
            Some("query_id and partition_id are required")
        );
    }

    #[test]
    fn state_responses_from_a_newer_broker_still_parse() {
        let got: StateGetResponse = serde_json::from_str(
            r#"{"success":true,"rows":[{"key":"k","value":1,"updated_at":null,"version":7}],"elapsed_ms":2}"#,
        )
        .expect("an unmodelled key must not fail the decode");
        assert_eq!(got.rows.len(), 1);
    }

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
