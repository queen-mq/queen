//! `POST /api/v1/transaction` — push and ack atomically.
//!
//! This is the endpoint that makes a handoff safe: acking the message you just
//! finished and pushing the next stage's happen in one PostgreSQL transaction,
//! so a crash between them is impossible.
//!
//! A rolled-back transaction still answers **HTTP 200** with
//! `success: false` — the status code is not the signal.

use serde::{Deserialize, Serialize};

use crate::ack::AckStatus;

/// One message pushed inside a transaction.
///
/// Unlike [`crate::PushItem`], this one *does* carry `traceId`: the
/// transaction path reads it and `TxnFrame` stores it, so the trace id survives
/// to delivery here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TxnPushItem {
    pub queue: String,

    /// Defaults to [`crate::DEFAULT_PARTITION`] broker-side when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,

    /// The broker accepts either `payload` or `data` here and prefers
    /// `payload`; this type always writes `payload`.
    pub payload: serde_json::Value,

    #[serde(
        rename = "transactionId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub transaction_id: Option<String>,

    /// Must be a UUID. A non-UUID string is dropped by the broker rather than
    /// rejected.
    #[serde(rename = "traceId", default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

impl TxnPushItem {
    pub fn new(queue: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            queue: queue.into(),
            partition: None,
            payload,
            transaction_id: None,
            trace_id: None,
        }
    }

    pub fn partition(mut self, partition: impl Into<String>) -> Self {
        self.partition = Some(partition.into());
        self
    }

    pub fn transaction_id(mut self, transaction_id: impl Into<String>) -> Self {
        self.transaction_id = Some(transaction_id.into());
        self
    }

    pub fn trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }
}

/// An ack operation inside a transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TxnAckOperation {
    #[serde(rename = "transactionId")]
    pub transaction_id: String,

    #[serde(rename = "partitionId")]
    pub partition_id: String,

    pub status: AckStatus,

    #[serde(
        rename = "consumerGroup",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub consumer_group: Option<String>,

    #[serde(rename = "leaseId", default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,

    /// Why the message failed, carried through to the DLQ row.
    ///
    /// `handle_transaction` reads `error` off the ack operation and hands it to
    /// the SP as the dead-letter reason. Without this field a transactional
    /// `failed`/`dlq` ack could only ever produce a DLQ entry with no reason,
    /// while the plain ack route carries one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// One operation in a transaction, tagged by `type` on the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TxnOperation {
    Push { items: Vec<TxnPushItem> },
    Ack(TxnAckOperation),
}

/// Body of `POST /api/v1/transaction`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransactionRequest {
    pub operations: Vec<TxnOperation>,

    /// Leases that must still be valid for the transaction to commit. Passing
    /// the leases of the messages being acked is what makes a handoff safe
    /// against a lease that expired while the handler was running: the whole
    /// transaction rolls back rather than pushing stage two for a message
    /// somebody else has already re-claimed.
    #[serde(rename = "requiredLeases", default)]
    pub required_leases: Vec<String>,
}

impl TransactionRequest {
    pub fn new(operations: Vec<TxnOperation>) -> Self {
        Self {
            operations,
            required_leases: Vec::new(),
        }
    }

    /// Deduplicate the required leases, preserving first-seen order. A
    /// multi-partition pop gives every message the same lease id, so a naive
    /// collect repeats it once per message.
    pub fn with_required_leases(mut self, leases: impl IntoIterator<Item = String>) -> Self {
        let mut seen = std::collections::HashSet::new();
        self.required_leases = leases
            .into_iter()
            .filter(|l| seen.insert(l.clone()))
            .collect();
        self
    }
}

/// One entry of a successful transaction's `results` array.
///
/// Push and ack entries carry different fields; both are flattened into this
/// one struct because the broker builds them as free-form JSON objects rather
/// than from a typed enum.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TxnResultItem {
    pub index: usize,

    /// `"push"` or `"ack"`.
    #[serde(rename = "type")]
    pub op_type: String,

    pub success: bool,

    #[serde(rename = "transactionId")]
    pub transaction_id: String,

    /// Push results only.
    #[serde(rename = "messageId", default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,

    /// Push results only.
    #[serde(rename = "queueName", default, skip_serializing_if = "Option::is_none")]
    pub queue_name: Option<String>,

    /// Push results only, and present *only when true* — the broker omits the
    /// key entirely for a non-duplicate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duplicate: Option<bool>,

    /// Ack results only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Ack results only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dlq: Option<bool>,
}

impl TxnResultItem {
    pub fn is_duplicate(&self) -> bool {
        self.duplicate.unwrap_or(false)
    }

    pub fn is_dlq(&self) -> bool {
        self.dlq.unwrap_or(false)
    }
}

/// Response of `POST /api/v1/transaction`, both on commit and on rollback.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransactionResponse {
    #[serde(rename = "transactionId")]
    pub transaction_id: String,

    pub success: bool,

    #[serde(default)]
    pub results: Vec<TxnResultItem>,

    /// Set on rollback. Carries the database's own message, so it is prefixed
    /// with the broker's SQL error tags — `QDUP ...` for a duplicate push,
    /// `QTXN ...` for an ack that referenced an unknown message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------
    // A replica of the broker's request reader.
    //
    // `handle_transaction` never deserializes the body into a struct: it walks
    // `serde_json::Value` key by key (`server/src/handlers/data.rs:3229-3327`,
    // plus `txn_add_push` at :3139-3159 for each pushed item). Asserting that
    // `TransactionRequest` serializes to its own field names proves nothing
    // against that — a rename on this side and a rename in the test agree with
    // each other and disagree with the broker, which then silently reads a
    // default. The reader below walks the SAME keys with the SAME defaults, so
    // a rename on either side shows up as a value that went missing.
    // ------------------------------------------------------------------

    #[derive(Debug, PartialEq)]
    struct SeenPush {
        queue: String,
        partition: String,
        payload: serde_json::Value,
        transaction_id: Option<String>,
        trace_id: Option<String>,
    }

    #[derive(Debug, PartialEq)]
    struct SeenAck {
        transaction_id: String,
        partition_id: String,
        consumer_group: String,
        status: String,
        error: Option<String>,
        lease_id: Option<String>,
    }

    #[derive(Debug, Default, PartialEq)]
    struct BrokerView {
        pushes: Vec<SeenPush>,
        acks: Vec<SeenAck>,
        lease_hints: Vec<String>,
        unknown_ops: usize,
        /// The flat index the broker assigns each operation. Result entries are
        /// slotted by it, so it is what lines a result up with its request.
        flat: usize,
    }

    fn read_push_like_the_broker(item: &serde_json::Value) -> SeenPush {
        SeenPush {
            queue: item
                .get("queue")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            // NB no non-empty filter here, unlike the streams cycle: an
            // explicit `"partition": ""` reaches the broker as an empty lane
            // name rather than falling back to Default.
            partition: item
                .get("partition")
                .and_then(|x| x.as_str())
                .unwrap_or(crate::DEFAULT_PARTITION)
                .to_string(),
            payload: item
                .get("payload")
                .cloned()
                .or_else(|| item.get("data").cloned())
                .unwrap_or_else(|| serde_json::Value::Object(Default::default())),
            transaction_id: item
                .get("transactionId")
                .and_then(|x| x.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string),
            trace_id: item
                .get("traceId")
                .and_then(|x| x.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string),
        }
    }

    fn read_like_the_broker(body: &str) -> BrokerView {
        let root: serde_json::Value = serde_json::from_str(body).expect("body must be JSON");
        let mut view = BrokerView::default();

        if let Some(rl) = root.get("requiredLeases").and_then(|x| x.as_array()) {
            for l in rl {
                if let Some(s) = l.as_str().filter(|s| !s.is_empty()) {
                    view.lease_hints.push(s.to_string());
                }
            }
        }

        let operations = root
            .get("operations")
            .and_then(|o| o.as_array())
            .expect("no `operations` array: the broker answers 400 and reads nothing else");

        for op in operations {
            match op.get("type").and_then(|x| x.as_str()).unwrap_or("") {
                "push" => match op.get("items").and_then(|x| x.as_array()) {
                    Some(items) => {
                        for item in items {
                            view.pushes.push(read_push_like_the_broker(item));
                            view.flat += 1;
                        }
                    }
                    // The broker also accepts a flat push op with the item
                    // fields hoisted to the operation itself.
                    None => {
                        view.pushes.push(read_push_like_the_broker(op));
                        view.flat += 1;
                    }
                },
                "ack" => {
                    let lease = op
                        .get("leaseId")
                        .and_then(|x| x.as_str())
                        .filter(|s| !s.is_empty())
                        .map(str::to_string);
                    if let Some(l) = &lease {
                        view.lease_hints.push(l.clone());
                    }
                    view.acks.push(SeenAck {
                        transaction_id: op
                            .get("transactionId")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string(),
                        partition_id: op
                            .get("partitionId")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string(),
                        consumer_group: op
                            .get("consumerGroup")
                            .and_then(|x| x.as_str())
                            .filter(|s| !s.is_empty())
                            .unwrap_or(crate::QUEUE_MODE_GROUP)
                            .to_string(),
                        status: op
                            .get("status")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string(),
                        error: op
                            .get("error")
                            .and_then(|x| x.as_str())
                            .filter(|s| !s.is_empty())
                            .map(str::to_string),
                        lease_id: lease,
                    });
                    view.flat += 1;
                }
                _ => {
                    view.unknown_ops += 1;
                    view.flat += 1;
                }
            }
        }
        view
    }

    #[test]
    fn a_handoff_transaction_is_read_key_for_key_by_the_broker() {
        // The canonical handoff: ack the message just finished and push the
        // next stage, atomically. Every value below has to survive the trip
        // through the broker's untyped reader, because a key it cannot find is
        // not an error — it is a default. A mis-named `partitionId` acks
        // partition "", a mis-named `requiredLeases` drops the lease guard that
        // makes the handoff safe, and both commit happily.
        let req = TransactionRequest::new(vec![
            TxnOperation::Ack(TxnAckOperation {
                transaction_id: "order-1".into(),
                partition_id: "0198f0aa-0000-7000-8000-0000000000e1".into(),
                status: AckStatus::Completed,
                consumer_group: Some("fulfilment".into()),
                lease_id: Some("0198f0aa-0000-7000-8000-00000000a001".into()),
                error: None,
            }),
            TxnOperation::Push {
                items: vec![
                    TxnPushItem::new("shipping", serde_json::json!({"order": 1}))
                        .partition("eu")
                        .transaction_id("ship-1")
                        .trace_id("0198f0aa-0000-7000-8000-0000000000c3"),
                ],
            },
        ])
        .with_required_leases(["0198f0aa-0000-7000-8000-00000000a001".to_string()]);

        let body = serde_json::to_string(&req).unwrap();
        let seen = read_like_the_broker(&body);

        assert_eq!(seen.acks.len(), 1, "the ack operation was not recognized");
        assert_eq!(
            seen.acks[0],
            SeenAck {
                transaction_id: "order-1".into(),
                partition_id: "0198f0aa-0000-7000-8000-0000000000e1".into(),
                consumer_group: "fulfilment".into(),
                status: "completed".into(),
                error: None,
                lease_id: Some("0198f0aa-0000-7000-8000-00000000a001".into()),
            },
            "the broker read different values than the ack was built with"
        );

        assert_eq!(
            seen.pushes.len(),
            1,
            "the push operation was not recognized"
        );
        assert_eq!(
            seen.pushes[0],
            SeenPush {
                queue: "shipping".into(),
                partition: "eu".into(),
                payload: serde_json::json!({"order": 1}),
                transaction_id: Some("ship-1".into()),
                // Unlike the plain push route, this one keeps the trace id.
                trace_id: Some("0198f0aa-0000-7000-8000-0000000000c3".into()),
            }
        );

        // The lease appears twice on the wire (top level and on the ack) and
        // the broker collects both into one hint list; that is what lets it
        // resolve the worker unambiguously.
        assert_eq!(seen.lease_hints.len(), 2);
        assert!(seen
            .lease_hints
            .iter()
            .all(|l| l == "0198f0aa-0000-7000-8000-00000000a001"));
        assert_eq!(seen.unknown_ops, 0);
        assert_eq!(seen.flat, 2, "one result slot per operation, in order");
    }

    #[test]
    fn omitted_optionals_land_on_the_defaults_the_broker_applies() {
        // Everything this type may omit, omitted at once. The broker fills each
        // gap silently, so the only way to know which default a client actually
        // gets is to read them off its reader.
        let req = TransactionRequest::new(vec![
            TxnOperation::Push {
                items: vec![TxnPushItem::new("q", serde_json::json!(null))],
            },
            TxnOperation::Ack(TxnAckOperation {
                transaction_id: "t".into(),
                partition_id: "p".into(),
                status: AckStatus::Retry,
                consumer_group: None,
                lease_id: None,
                error: None,
            }),
        ]);
        let seen = read_like_the_broker(&serde_json::to_string(&req).unwrap());

        assert_eq!(
            seen.pushes[0].partition,
            crate::DEFAULT_PARTITION,
            "an omitted partition must land on the same lane the constant names"
        );
        assert!(seen.pushes[0].transaction_id.is_none());
        assert_eq!(
            seen.acks[0].consumer_group,
            crate::QUEUE_MODE_GROUP,
            "a group-less ack is queue-mode, not group \"\""
        );
        assert_eq!(
            seen.acks[0].status, "retry",
            "the nack spelling must survive"
        );
        assert!(
            seen.lease_hints.is_empty(),
            "no lease anywhere means the broker skips the lease check entirely, \
             which is what lets a lapsed-lease ack still commit"
        );
    }

    #[test]
    fn a_push_operation_always_sends_the_items_array() {
        // The broker branches on the PRESENCE of `items` (data.rs:3265-3273):
        // with it, each element is one operation; without it, the operation
        // itself is the item. If this enum ever serialized the flat form, one
        // push op would consume one result slot instead of N and every result
        // after it would be attributed to the wrong request index.
        let body = serde_json::to_string(&TransactionRequest::new(vec![TxnOperation::Push {
            items: vec![
                TxnPushItem::new("a", serde_json::json!(1)),
                TxnPushItem::new("b", serde_json::json!(2)),
            ],
        }]))
        .unwrap();
        assert!(body.contains(r#""type":"push","items":["#), "{body}");
        let seen = read_like_the_broker(&body);
        assert_eq!(seen.pushes.len(), 2);
        assert_eq!(seen.flat, 2, "two items are two operations, not one");
    }

    #[test]
    fn an_unknown_operation_type_would_be_rejected_wholesale() {
        // Not reachable through `TxnOperation`, which is the point: the broker
        // answers 400 and rolls the WHOLE transaction back when it meets a type
        // it does not know (data.rs:3322-3335), so the tagged enum is the guard
        // that keeps a future variant from silently voiding a handoff.
        let seen = read_like_the_broker(
            r#"{"operations":[{"type":"push","items":[{"queue":"q","payload":1}]},{"type":"noop"}],"requiredLeases":[]}"#,
        );
        assert_eq!(seen.unknown_ops, 1);
    }

    /// A committed transaction body, transcribed from the response builder in
    /// `server/src/handlers/data.rs`: the envelope at :3700-3705, the push
    /// entries at :3674-3688 and the ack entries at :3691-3700.
    ///
    /// Note how little the two entry kinds share — a push carries `messageId`
    /// and `queueName` and no `error`/`dlq`, an ack carries `error`/`dlq` and
    /// neither of the other two. Both land in the same `TxnResultItem`, so
    /// every field one of them omits has to be optional.
    const COMMITTED_TXN_FROM_THE_HANDLER: &str = concat!(
        r#"{"transactionId":"0198f0aa-0000-7000-8000-00000000ffff","success":true,"results":["#,
        r#"{"index":0,"type":"ack","success":true,"transactionId":"order-1","#,
        r#""error":null,"dlq":false},"#,
        r#"{"index":1,"type":"push","success":true,"transactionId":"ship-1","#,
        r#""messageId":"0198f0aa-0000-7000-8000-000000000001","queueName":"shipping"},"#,
        r#"{"index":2,"type":"push","success":true,"transactionId":"ship-1","#,
        r#""messageId":"0198f0aa-0000-7000-8000-000000000001","queueName":"shipping","#,
        r#""duplicate":true}"#,
        r#"]}"#,
    );

    #[test]
    fn a_committed_transaction_parses_every_entry_kind() {
        let got: TransactionResponse = serde_json::from_str(COMMITTED_TXN_FROM_THE_HANDLER)
            .expect("the committed body the handler builds must deserialize");
        assert!(got.success);
        assert_eq!(got.results.len(), 3);

        // Results are slotted by request index, so entry i answers operation i.
        for (i, r) in got.results.iter().enumerate() {
            assert_eq!(r.index, i, "result {i} is out of order");
        }

        let ack = &got.results[0];
        assert_eq!(ack.op_type, "ack");
        assert!(
            ack.error.is_none(),
            "the handler writes an explicit `\"error\": null` on every ack entry; \
             it must not surface as Some(\"null\")"
        );
        assert!(!ack.is_dlq());
        assert!(
            ack.message_id.is_none() && ack.queue_name.is_none(),
            "an ack entry carries neither key"
        );

        let push = &got.results[1];
        assert_eq!(push.queue_name.as_deref(), Some("shipping"));
        assert!(
            push.dlq.is_none() && push.error.is_none(),
            "a push entry carries neither key, and an absent dlq must not read as false-by-value"
        );
        assert!(!push.is_duplicate());

        // The deduplicated push echoes the FIRST message's id, which is what
        // makes a retried transaction idempotent rather than a second copy.
        let dup = &got.results[2];
        assert!(dup.is_duplicate());
        assert_eq!(dup.message_id, push.message_id);
    }

    #[test]
    fn a_dead_lettering_ack_reports_it_on_its_own_entry() {
        // `dlq` is stamped per ack entry from the set of dead-lettered indices
        // (data.rs:3691-3699), so a transaction can commit with success:true
        // while one of its acks poisoned a message. A caller that only checks
        // the top-level `success` never learns.
        let got: TransactionResponse = serde_json::from_str(
            r#"{"transactionId":"T","success":true,"results":[{"index":0,"type":"ack","success":true,"transactionId":"t0","error":null,"dlq":true}]}"#,
        )
        .unwrap();
        assert!(got.success);
        assert!(got.results[0].is_dlq());
    }

    #[test]
    fn a_rolled_back_transaction_is_a_200_with_an_empty_results_array() {
        // `txn_fail_body` (data.rs:3204-3212) — the shape for every rollback,
        // including the pre-flight bogus-ack rejection at :3479-3483 whose
        // message is quoted verbatim here. HTTP 200 throughout, so `success` is
        // the only signal and an empty `results` is normal, not a decode
        // problem.
        let wire = concat!(
            r#"{"transactionId":"0198f0aa-0000-7000-8000-00000000ffff","success":false,"#,
            r#""error":"QTXN ack references unknown transactionId; transaction rolled back","#,
            r#""results":[]}"#,
        );
        let got: TransactionResponse = serde_json::from_str(wire).unwrap();
        assert!(!got.success);
        assert!(got.results.is_empty());
        assert!(
            got.error.as_deref().unwrap().starts_with("QTXN "),
            "the SQL error tag is how a caller tells a rejected ack from a duplicate push"
        );
    }

    #[test]
    fn a_response_from_a_newer_broker_still_parses() {
        // Unknown keys at both levels must be dropped rather than rejected: a
        // client that cannot decode the reply has no way to know whether the
        // transaction committed, and the pushes are already durable.
        let wire = concat!(
            r#"{"transactionId":"T","success":true,"elapsedMs":4,"results":["#,
            r#"{"index":0,"type":"push","success":true,"transactionId":"t1","#,
            r#""messageId":"m1","queueName":"q","partitionId":"p1"}]}"#,
        );
        let got: TransactionResponse =
            serde_json::from_str(wire).expect("an unmodelled key must not fail the decode");
        assert_eq!(got.results[0].message_id.as_deref(), Some("m1"));
    }

    #[test]
    fn operations_are_tagged_by_type() {
        let req = TransactionRequest::new(vec![
            TxnOperation::Push {
                items: vec![TxnPushItem::new("stage2", serde_json::json!({"n": 1}))],
            },
            TxnOperation::Ack(TxnAckOperation {
                transaction_id: "t1".into(),
                partition_id: "p1".into(),
                status: AckStatus::Completed,
                consumer_group: Some("g".into()),
                lease_id: Some("L1".into()),
                error: None,
            }),
        ])
        .with_required_leases(["L1".to_string()]);

        let s = serde_json::to_string(&req).unwrap();
        assert!(s.contains(r#""type":"push""#), "{s}");
        assert!(s.contains(r#""type":"ack""#), "{s}");
        assert!(s.contains(r#""requiredLeases":["L1"]"#), "{s}");
        // A completed ack carries no reason, and must not send an empty one.
        assert!(!s.contains(r#""error""#), "{s}");
    }

    #[test]
    fn a_failed_ack_carries_its_reason_under_the_key_the_broker_reads() {
        // `handle_transaction` does `op.get("error")` and hands the string to
        // the SP as the dead-letter reason. Any other key — `reason`,
        // `errorMessage`, a rename — dead-letters with no reason at all, and
        // the broker cannot tell the difference between that and a failure
        // nobody explained.
        let req = TransactionRequest::new(vec![TxnOperation::Ack(TxnAckOperation {
            transaction_id: "t1".into(),
            partition_id: "p1".into(),
            status: AckStatus::Failed,
            consumer_group: Some("g".into()),
            lease_id: None,
            error: Some("handler blew up".into()),
        })]);

        let s = serde_json::to_string(&req).unwrap();
        assert!(s.contains(r#""error":"handler blew up""#), "{s}");

        let back: TransactionRequest = serde_json::from_str(&s).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn required_leases_are_deduplicated_in_order() {
        // A multi-partition pop hands every message the same lease id.
        let req = TransactionRequest::new(vec![]).with_required_leases([
            "L1".to_string(),
            "L2".to_string(),
            "L1".to_string(),
        ]);
        assert_eq!(req.required_leases, vec!["L1", "L2"]);
    }

    #[test]
    fn txn_push_item_carries_trace_id_unlike_plain_push() {
        let item = TxnPushItem::new("q", serde_json::json!(1))
            .trace_id("6f1a3d0e-0000-7000-8000-000000000000");
        let s = serde_json::to_string(&item).unwrap();
        assert!(
            s.contains(r#""traceId":"6f1a3d0e-0000-7000-8000-000000000000""#),
            "{s}"
        );
    }

    #[test]
    fn parses_a_committed_response() {
        let wire = r#"{"transactionId":"T","success":true,"results":[
            {"index":0,"type":"push","success":true,"transactionId":"t1","messageId":"m1","queueName":"stage2"},
            {"index":1,"type":"ack","success":true,"transactionId":"t0","error":null,"dlq":false}]}"#;
        let got: TransactionResponse = serde_json::from_str(wire).unwrap();
        assert!(got.success);
        assert_eq!(got.results[0].message_id.as_deref(), Some("m1"));
        assert!(!got.results[0].is_duplicate());
        assert!(!got.results[1].is_dlq());
        assert_eq!(got.results[1].op_type, "ack");
    }

    #[test]
    fn duplicate_key_is_present_only_when_true() {
        let wire = r#"{"index":0,"type":"push","success":true,"transactionId":"t1","messageId":"m1","queueName":"q","duplicate":true}"#;
        let got: TxnResultItem = serde_json::from_str(wire).unwrap();
        assert!(got.is_duplicate());
        // Round-trip keeps it absent when it was absent.
        let plain: TxnResultItem = serde_json::from_str(
            r#"{"index":0,"type":"push","success":true,"transactionId":"t1","messageId":"m1","queueName":"q"}"#,
        )
        .unwrap();
        assert!(!serde_json::to_string(&plain).unwrap().contains("duplicate"));
    }

    #[test]
    fn parses_a_rollback_response() {
        let wire = r#"{"transactionId":"T","success":false,"error":"QDUP duplicate transactionId","results":[]}"#;
        let got: TransactionResponse = serde_json::from_str(wire).unwrap();
        assert!(!got.success);
        assert!(got.error.unwrap().starts_with("QDUP"));
        assert!(got.results.is_empty());
    }
}
