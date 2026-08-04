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
        self.required_leases = leases.into_iter().filter(|l| seen.insert(l.clone())).collect();
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
            }),
        ])
        .with_required_leases(["L1".to_string()]);

        let s = serde_json::to_string(&req).unwrap();
        assert!(s.contains(r#""type":"push""#), "{s}");
        assert!(s.contains(r#""type":"ack""#), "{s}");
        assert!(s.contains(r#""requiredLeases":["L1"]"#), "{s}");
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
        assert!(s.contains(r#""traceId":"6f1a3d0e-0000-7000-8000-000000000000""#), "{s}");
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
