//! Atomic push + ack.

use std::sync::Arc;

use serde::Serialize;

use queen_protocol::{
    AckStatus, Message, TransactionRequest, TransactionResponse, TxnAckOperation, TxnOperation,
    TxnPushItem,
};

use crate::error::{Error, Result};
use crate::http::Opts;
use crate::inner::Inner;
use crate::uuid;

/// Builds one transaction.
///
/// The point of this endpoint is the handoff: ack the message you just
/// processed and push the next stage's in a single PostgreSQL transaction, so
/// there is no window where one happened and the other did not.
///
/// Acking through here also collects the messages' lease ids into
/// `requiredLeases`, so a lease that expired while the handler was running
/// rolls the whole thing back instead of pushing stage two for a message
/// somebody else has already re-claimed.
pub struct TransactionBuilder {
    inner: Arc<Inner>,
    operations: Vec<TxnOperation>,
    leases: Vec<String>,
}

impl TransactionBuilder {
    pub(crate) fn new(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            operations: Vec::new(),
            leases: Vec::new(),
        }
    }

    /// Ack a message as completed.
    pub fn ack(self, message: &Message) -> Self {
        self.ack_with(message, AckStatus::Completed)
    }

    /// Ack a message with an explicit outcome.
    pub fn ack_with(self, message: &Message, status: AckStatus) -> Self {
        self.ack_op(message, status, None)
    }

    /// Fail a message with a reason, inside the transaction.
    ///
    /// The reason reaches the DLQ row the same way [`crate::Queen::nack`]'s
    /// does — the broker reads `error` off the ack operation. Without it a
    /// transactional failure dead-letters with an empty reason.
    pub fn nack(self, message: &Message, reason: impl Into<String>) -> Self {
        self.ack_op(message, AckStatus::Failed, Some(reason.into()))
    }

    /// Ack with both an explicit outcome and a reason, for `Dlq` as well as
    /// `Failed`.
    pub fn ack_with_reason(
        self,
        message: &Message,
        status: AckStatus,
        reason: impl Into<String>,
    ) -> Self {
        self.ack_op(message, status, Some(reason.into()))
    }

    fn ack_op(mut self, message: &Message, status: AckStatus, error: Option<String>) -> Self {
        self.operations.push(TxnOperation::Ack(TxnAckOperation {
            transaction_id: message.transaction_id.clone(),
            partition_id: message.partition_id.clone(),
            status,
            consumer_group: non_empty(&message.consumer_group),
            lease_id: non_empty(&message.lease_id),
            error,
        }));
        if !message.lease_id.is_empty() {
            self.leases.push(message.lease_id.clone());
        }
        self
    }

    /// Ack several messages at once.
    pub fn ack_all<'a, I>(mut self, messages: I) -> Self
    where
        I: IntoIterator<Item = &'a Message>,
    {
        for m in messages {
            self = self.ack_with(m, AckStatus::Completed);
        }
        self
    }

    /// Push a payload to `queue`'s default partition.
    pub fn push<T: Serialize>(self, queue: impl Into<String>, payload: T) -> Result<Self> {
        self.push_item(TxnPushItem {
            queue: queue.into(),
            partition: None,
            payload: serde_json::to_value(payload)?,
            transaction_id: Some(uuid::uuidv7()),
            trace_id: None,
        })
    }

    /// Push a payload to a named partition.
    pub fn push_to<T: Serialize>(
        self,
        queue: impl Into<String>,
        partition: impl Into<String>,
        payload: T,
    ) -> Result<Self> {
        self.push_item(TxnPushItem {
            queue: queue.into(),
            partition: Some(partition.into()),
            payload: serde_json::to_value(payload)?,
            transaction_id: Some(uuid::uuidv7()),
            trace_id: None,
        })
    }

    /// Push a fully-formed item. Unlike a plain push, a `traceId` set here is
    /// stored and comes back at delivery.
    pub fn push_item(mut self, item: TxnPushItem) -> Result<Self> {
        if let Some(t) = &item.trace_id {
            if !uuid::is_valid_uuid(t) {
                return Err(Error::Invalid(format!(
                    "traceId must be a UUID (the broker drops anything else silently) — got '{t}'"
                )));
            }
        }
        // Consecutive pushes to the same (queue, partition) join one operation,
        // matching how the broker groups frames anyway.
        match self.operations.last_mut() {
            Some(TxnOperation::Push { items })
                if items
                    .last()
                    .is_some_and(|l| l.queue == item.queue && l.partition == item.partition) =>
            {
                items.push(item);
            }
            _ => self
                .operations
                .push(TxnOperation::Push { items: vec![item] }),
        }
        Ok(self)
    }

    /// How many operations are staged.
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Send it.
    ///
    /// A rolled-back transaction comes back as HTTP 200 with `success: false`;
    /// this surfaces it as an error, since a caller who ignored it would
    /// believe a handoff happened that did not.
    pub async fn commit(self) -> Result<TransactionResponse> {
        if self.operations.is_empty() {
            return Err(Error::Invalid(
                "a transaction needs at least one operation".into(),
            ));
        }

        let req = TransactionRequest::new(self.operations).with_required_leases(self.leases);
        let resp: Option<TransactionResponse> = self
            .inner
            .http
            .post_json("/api/v1/transaction", &req, &Opts::default())
            .await?;
        let resp =
            resp.ok_or_else(|| Error::Decode("transaction returned an empty body".into()))?;

        if !resp.success {
            return Err(Error::Invalid(format!(
                "transaction rolled back: {}",
                resp.error.as_deref().unwrap_or("no reason given")
            )));
        }
        Ok(resp)
    }
}

fn non_empty(s: &str) -> Option<String> {
    (!s.is_empty()).then(|| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::Queen;

    fn queen() -> Queen {
        Queen::connect(Config::new("http://127.0.0.1:1")).unwrap()
    }

    fn msg(txn: &str, lease: &str, group: &str) -> Message {
        Message {
            id: "m".into(),
            transaction_id: txn.into(),
            trace_id: None,
            data: serde_json::json!(null),
            producer_sub: None,
            created_at: "2026-08-04T10:00:00Z".into(),
            partition_id: "p1".into(),
            partition: "Default".into(),
            lease_id: lease.into(),
            consumer_group: group.into(),
        }
    }

    #[test]
    fn ack_collects_lease_ids_for_the_rollback_guard() {
        let t = queen()
            .transaction()
            .ack(&msg("t1", "L1", "g"))
            .ack(&msg("t2", "L1", "g"));
        // One lease, mentioned twice — a multi-partition pop shares it.
        let req = TransactionRequest::new(t.operations).with_required_leases(t.leases);
        assert_eq!(req.required_leases, vec!["L1"]);
    }

    #[test]
    fn an_auto_ack_message_contributes_no_lease() {
        let t = queen().transaction().ack(&msg("t1", "", "g"));
        assert!(t.leases.is_empty());
        match &t.operations[0] {
            TxnOperation::Ack(a) => assert!(a.lease_id.is_none()),
            _ => panic!("expected an ack"),
        }
    }

    #[test]
    fn queue_mode_group_is_carried_through() {
        let t = queen()
            .transaction()
            .ack(&msg("t1", "L1", "__QUEUE_MODE__"));
        match &t.operations[0] {
            // Echoing the broker's own default is equivalent to omitting it,
            // and safer than relying on the caller to remember the group.
            TxnOperation::Ack(a) => {
                assert_eq!(a.consumer_group.as_deref(), Some("__QUEUE_MODE__"))
            }
            _ => panic!("expected an ack"),
        }
    }

    #[test]
    fn consecutive_pushes_to_one_lane_merge() {
        let t = queen()
            .transaction()
            .push("stage2", serde_json::json!(1))
            .unwrap()
            .push("stage2", serde_json::json!(2))
            .unwrap();
        assert_eq!(t.len(), 1);
        match &t.operations[0] {
            TxnOperation::Push { items } => assert_eq!(items.len(), 2),
            _ => panic!("expected a push"),
        }
    }

    #[test]
    fn pushes_to_different_lanes_stay_separate() {
        let t = queen()
            .transaction()
            .push("stage2", serde_json::json!(1))
            .unwrap()
            .push_to("stage2", "eu", serde_json::json!(2))
            .unwrap()
            .push("stage3", serde_json::json!(3))
            .unwrap();
        assert_eq!(t.len(), 3);
    }

    #[test]
    fn a_non_uuid_trace_id_is_rejected_instead_of_dropped() {
        let e = queen()
            .transaction()
            .push_item(TxnPushItem::new("q", serde_json::json!(1)).trace_id("not-a-uuid"));
        assert!(
            e.is_err(),
            "a silently-dropped traceId is worse than an error"
        );
    }

    #[test]
    fn a_valid_trace_id_is_accepted() {
        assert!(queen()
            .transaction()
            .push_item(
                TxnPushItem::new("q", serde_json::json!(1))
                    .trace_id("0190aaaa-0000-7000-8000-000000000001")
            )
            .is_ok());
    }

    #[tokio::test]
    async fn an_empty_transaction_is_rejected_before_the_network() {
        let e = queen().transaction().commit().await.unwrap_err();
        assert!(e.to_string().contains("at least one operation"));
    }
}
