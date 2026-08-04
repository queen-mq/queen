//! `POST /api/v1/push` — enqueue one or more messages.

use serde::{Deserialize, Serialize};

/// One message to enqueue.
///
/// # `traceId` is deliberately absent
///
/// The JS, Go and Python clients all attach a `traceId` to push items, but the
/// broker's push path has nowhere to put it: `PushItem<'a>` in
/// `server/src/handlers/data.rs` declares no such field, and neither `PreFrame`
/// nor `fusion::OwnedFrame` — the two structs a pushed message actually travels
/// through — carry a trace id. Serde drops the unknown key silently, so a
/// message pushed with a `traceId` is stored with none and pops back with
/// `"traceId": null`.
///
/// It is *not* dropped on the transaction path: `txn_add_push` reads
/// `traceId` and `TxnFrame` carries it, so [`crate::TxnPushItem`] does expose
/// the field.
///
/// Rather than ship an option that does nothing, this type omits it. The
/// broker's conformance tests assert the current behaviour, so if the push path
/// ever learns to carry a trace id the test fails and the field gets added
/// here deliberately.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PushItem {
    pub queue: String,

    /// Omit for [`crate::DEFAULT_PARTITION`]. The broker applies the same
    /// default, so omitting and sending `"Default"` are equivalent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,

    pub payload: serde_json::Value,

    /// Idempotency key inside the queue's dedup window. When omitted the broker
    /// mints one (the message id), which makes the push non-idempotent — a
    /// retry after a timeout enqueues a second copy. Clients that care about
    /// exactly-once ingest generate this themselves so the id is knowable
    /// before the request is sent.
    #[serde(
        rename = "transactionId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub transaction_id: Option<String>,
}

impl PushItem {
    /// A push of `payload` to `queue`'s default partition, with no explicit
    /// transaction id.
    pub fn new(queue: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            queue: queue.into(),
            partition: None,
            payload,
            transaction_id: None,
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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PushRequest {
    pub items: Vec<PushItem>,
}

impl PushRequest {
    pub fn new(items: Vec<PushItem>) -> Self {
        Self { items }
    }
}

/// Per-item outcome of a push.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PushStatus {
    /// Stored.
    Queued,
    /// A message with this `transactionId` already existed inside the queue's
    /// dedup window. `message_id` is the *pre-existing* message's id, not a new
    /// one — which is what makes a retried push safely idempotent.
    Duplicate,
    /// The database transaction that would have stored this message failed.
    Error,
    /// Push maintenance mode is on: the message went to the broker's on-disk
    /// spool and will be replayed when maintenance is disabled. It is accepted,
    /// but it is not yet in the queue.
    Buffered,
    /// Maintenance mode was on and the spool write itself failed. The message
    /// is lost; this is the only push status that means "not accepted".
    Failed,
}

impl PushStatus {
    /// Whether the broker took responsibility for the message. `Buffered`
    /// counts: it is durable in the spool and replays on maintenance exit.
    pub fn accepted(self) -> bool {
        matches!(self, Self::Queued | Self::Duplicate | Self::Buffered)
    }
}

/// One entry of the `200`/`201` push response array, in request order.
///
/// Note the mixed casing — `message_id` and `transaction_id` are snake_case
/// while `queueName` is camelCase. This is what `render_push_results` emits;
/// see the crate docs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PushResult {
    pub index: usize,

    #[serde(rename = "message_id")]
    pub message_id: String,

    #[serde(rename = "transaction_id")]
    pub transaction_id: String,

    #[serde(rename = "queueName")]
    pub queue_name: String,

    pub status: PushStatus,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_item_omits_absent_optionals() {
        let item = PushItem::new("orders", serde_json::json!({"id": 1}));
        let s = serde_json::to_string(&item).unwrap();
        assert_eq!(s, r#"{"queue":"orders","payload":{"id":1}}"#);
    }

    #[test]
    fn push_item_emits_camel_case_transaction_id() {
        let item = PushItem::new("orders", serde_json::json!(null))
            .partition("eu")
            .transaction_id("txn-1");
        let s = serde_json::to_string(&item).unwrap();
        assert!(s.contains(r#""transactionId":"txn-1""#), "{s}");
        assert!(s.contains(r#""partition":"eu""#), "{s}");
    }

    #[test]
    fn push_item_ignores_a_trace_id_on_the_wire() {
        // Faithful to the broker: an incoming traceId is an unknown field and
        // is dropped rather than rejected.
        let item: PushItem =
            serde_json::from_str(r#"{"queue":"q","payload":1,"traceId":"whatever"}"#).unwrap();
        assert_eq!(item.queue, "q");
    }

    #[test]
    fn push_result_parses_the_mixed_case_wire() {
        let wire = r#"[{"index":0,"message_id":"m1","transaction_id":"t1","queueName":"orders","status":"queued"}]"#;
        let got: Vec<PushResult> = serde_json::from_str(wire).unwrap();
        assert_eq!(got[0].message_id, "m1");
        assert_eq!(got[0].queue_name, "orders");
        assert_eq!(got[0].status, PushStatus::Queued);
        // and round-trips byte-identically
        assert_eq!(serde_json::to_string(&got).unwrap(), wire);
    }

    #[test]
    fn every_push_status_parses() {
        for (s, want) in [
            ("queued", PushStatus::Queued),
            ("duplicate", PushStatus::Duplicate),
            ("error", PushStatus::Error),
            ("buffered", PushStatus::Buffered),
            ("failed", PushStatus::Failed),
        ] {
            let got: PushStatus = serde_json::from_str(&format!("\"{s}\"")).unwrap();
            assert_eq!(got, want);
        }
    }

    #[test]
    fn accepted_excludes_only_error_and_failed() {
        assert!(PushStatus::Queued.accepted());
        assert!(PushStatus::Duplicate.accepted());
        assert!(PushStatus::Buffered.accepted());
        assert!(!PushStatus::Error.accepted());
        assert!(!PushStatus::Failed.accepted());
    }
}
