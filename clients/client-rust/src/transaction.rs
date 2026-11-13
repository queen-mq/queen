//! Atomic push + ack.

use std::sync::Arc;

use serde::Serialize;

use queen_protocol::kv::KvOperation;
use queen_protocol::timers::TimerOperation;
use queen_protocol::{
    AckStatus, Expiry, Message, TransactionRequest, TransactionResponse, TxnAckOperation,
    TxnOperation, TxnPushItem, DEFAULT_PARTITION,
};

use crate::error::{Error, Result};
use crate::http::Opts;
use crate::inner::Inner;
use crate::routing::{self, Headers, LinkFrom, LinkSource};
use crate::uuid;

/// Builds one transaction.
///
/// The point of this endpoint is the handoff: ack the message you just
/// processed and push the next stage's in a single broker command, so
/// there is no window where one happened and the other did not.
///
/// Acking through here also collects the messages' lease ids into
/// `requiredLeases`, so a lease that expired while the handler was running
/// rolls the whole thing back instead of pushing stage two for a message
/// somebody else has already re-claimed.
///
/// # KV and timers ride along
///
/// [`TransactionBuilder::kv`] and [`TransactionBuilder::timer`] put state
/// writes and scheduled deliveries **in the same transaction** as
/// the ack and the push. That is what the KV surface is for: an idempotency
/// marker written outside the transaction that acks the message it guards
/// protects nothing, because the two can come apart.
///
/// The gate reads best as one sentence: *claim the work, do it, hand it on —
/// all of it or none of it.*
///
/// ```no_run
/// # use queen_mq::{Expiry, Message, Queen};
/// # async fn example(queen: &Queen, msg: &Message) -> queen_mq::Result<()> {
/// let resp = queen
///     .transaction()
///     .ack(msg)
///     .push("invoices", serde_json::json!({ "order": 9137 }))?
///     // If this marker is already there, somebody handled this delivery and
///     // the whole bundle rolls back — the invoice is not pushed twice.
///     .kv_put_if_absent("orders", "idem:9137", serde_json::json!(true), Expiry::seconds(86_400))?
///     .commit()
///     .await?;
///
/// if let Some(lost) = resp.lost_precondition() {
///     // Expected, not exceptional: this delivery was a redelivery.
///     let _ = lost.value;
///     return Ok(());
/// }
/// # Ok(())
/// # }
/// ```
pub struct TransactionBuilder {
    inner: Arc<Inner>,
    operations: Vec<TxnOperation>,
    leases: Vec<String>,
    kv: Vec<KvOperation>,
    /// Lock handles whose permit this bundle commits under. Kept as handles,
    /// not as operations: the token is read when `commit` sends, because a
    /// lock's own renewal changes it.
    guards: Vec<crate::locks::Lock>,
    timers: Vec<TimerOperation>,
    /// The `link` operations, each with its place among `operations`: it goes
    /// before `operations[at]`, so after everything staged before it.
    links: Vec<(usize, LinkOp)>,
}

/// One `link` operation (broker catalogue version 8): the message `from`
/// names, into `queue`'s `partition`, with the link's own headers.
#[derive(Debug, Clone, PartialEq, Serialize)]
struct LinkOp {
    #[serde(rename = "type")]
    kind: &'static str,
    queue: String,
    partition: String,
    from: LinkFrom,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<Headers>,
}

/// The JSON body of a bundle that links: [`TransactionRequest`]'s, key for
/// key, with each `link` operation at its place among the others. A link has
/// no place in the binary body, so a bundle with one always goes as this.
#[derive(Serialize)]
struct Linked<'a> {
    operations: Vec<LinkedOp<'a>>,
    #[serde(rename = "requiredLeases")]
    required_leases: &'a [String],
    #[serde(skip_serializing_if = "is_empty")]
    kv: &'a [KvOperation],
    #[serde(skip_serializing_if = "is_empty")]
    timers: &'a [TimerOperation],
}

#[derive(Serialize)]
#[serde(untagged)]
enum LinkedOp<'a> {
    Op(&'a TxnOperation),
    Link(&'a LinkOp),
}

fn is_empty<T>(items: &&[T]) -> bool {
    items.is_empty()
}

impl<'a> Linked<'a> {
    fn new(req: &'a TransactionRequest, links: &'a [(usize, LinkOp)]) -> Self {
        let mut operations = Vec::with_capacity(req.operations.len() + links.len());
        let mut links = links.iter().peekable();
        for (i, op) in req.operations.iter().enumerate() {
            while let Some((_, link)) = links.next_if(|(at, _)| *at <= i) {
                operations.push(LinkedOp::Link(link));
            }
            operations.push(LinkedOp::Op(op));
        }
        operations.extend(links.map(|(_, link)| LinkedOp::Link(link)));
        Self {
            operations,
            required_leases: &req.required_leases,
            kv: &req.kv,
            timers: &req.timers,
        }
    }
}

impl TransactionBuilder {
    pub(crate) fn new(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            operations: Vec::new(),
            leases: Vec::new(),
            kv: Vec::new(),
            guards: Vec::new(),
            timers: Vec::new(),
            links: Vec::new(),
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
            headers: None,
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
            headers: None,
        })
    }

    /// Push a fully-formed item. Unlike a plain push, a `traceId` set here is
    /// stored and comes back at delivery.
    ///
    /// Headers ride the item ([`TxnPushItem::headers`]), checked as
    /// [`crate::QueueBuilder::headers`] checks them.
    pub fn push_item(mut self, item: TxnPushItem) -> Result<Self> {
        if let Some(t) = &item.trace_id {
            if !uuid::is_valid_uuid(t) {
                return Err(Error::Invalid(format!(
                    "traceId must be a UUID (the broker drops anything else silently) — got '{t}'"
                )));
            }
        }
        routing::check_headers(item.headers.as_ref())?;
        // Consecutive pushes to the same (queue, partition) join one operation,
        // matching how the broker groups frames anyway — unless a link was
        // staged in between, which must stay between them.
        let after_link = self
            .links
            .last()
            .is_some_and(|(at, _)| *at == self.operations.len());
        match self.operations.last_mut() {
            Some(TxnOperation::Push { items })
                if !after_link
                    && items.last().is_some_and(|l| {
                        l.queue == item.queue && l.partition == item.partition
                    }) =>
            {
                items.push(item);
            }
            _ => self
                .operations
                .push(TxnOperation::Push { items: vec![item] }),
        }
        Ok(self)
    }

    /// Link messages into `queue`'s `partition`: the target gets each one with
    /// the same id and transaction id, and its payload is not sent or stored
    /// again (broker catalogue version 8). One `link` operation per message.
    ///
    /// A message is named as a pop returned it (a [`Message`]: its
    /// `partitionId`, `offset`, `transactionId` and consumer group), as a feed
    /// entry names it ([`crate::feed::FeedEntry`]), or by hand
    /// ([`LinkFrom`]). The link commits with the rest of the bundle or not at
    /// all: a message its source no longer keeps rolls the bundle back
    /// (`source_gone`), as do a transaction id that does not match
    /// (`source_mismatch`) and a reading queue as the target
    /// (`reading_queue`). A target partition that already holds the message
    /// makes its link a no-op that answers `duplicate: true`, so forwarding is
    /// safe to retry.
    ///
    /// ```no_run
    /// # async fn example(queen: queen_mq::Queen, msg: queen_mq::Message) -> queen_mq::Result<()> {
    /// queen
    ///     .transaction()
    ///     .ack(&msg)
    ///     .link_with_headers("manual-review", &msg.partition, [&msg], [("review-reason", "amount")])?
    ///     .commit()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn link<'a, I, S>(
        self,
        queue: impl Into<String>,
        partition: impl Into<String>,
        messages: I,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = &'a S>,
        S: LinkSource + ?Sized + 'a,
    {
        self.link_op(queue.into(), partition.into(), messages, None)
    }

    /// [`Self::link`] with the link's own headers. A reader of the target sees
    /// the source message's headers with these written over them.
    pub fn link_with_headers<'a, I, S, H, K, V>(
        self,
        queue: impl Into<String>,
        partition: impl Into<String>,
        messages: I,
        headers: H,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = &'a S>,
        S: LinkSource + ?Sized + 'a,
        H: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut own = None;
        routing::extend_headers(&mut own, headers);
        self.link_op(queue.into(), partition.into(), messages, own)
    }

    fn link_op<'a, I, S>(
        mut self,
        queue: String,
        partition: String,
        messages: I,
        headers: Option<Headers>,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = &'a S>,
        S: LinkSource + ?Sized + 'a,
    {
        if queue.is_empty() {
            return Err(Error::Invalid("link: name the queue to link into".into()));
        }
        routing::check_headers(headers.as_ref())?;
        let headers = headers.filter(|h| !h.is_empty());
        let partition = if partition.is_empty() {
            DEFAULT_PARTITION.to_string()
        } else {
            partition
        };
        let at = self.operations.len();
        for m in messages {
            self.links.push((
                at,
                LinkOp {
                    kind: "link",
                    queue: queue.clone(),
                    partition: partition.clone(),
                    from: m.link_from()?,
                    headers: headers.clone(),
                },
            ));
        }
        Ok(self)
    }

    // ------------------------------------------------------- kv riders

    /// Stage a KV operation, built with [`crate::kv`] or by hand.
    ///
    /// `getPrefix` is refused by the broker here — its cost is not bounded by
    /// the caller, and this transaction holds the outermost lock space of the
    /// product. `get` and `getMany` are allowed, because the caller fixes what
    /// they cost.
    pub fn kv(mut self, op: KvOperation) -> Self {
        self.kv.push(op);
        self
    }

    /// Stage a `putIfAbsent` that **rolls the bundle back** when the key is
    /// already there.
    ///
    /// This is the gate, and it is the reason the riders exist. Its failure is
    /// not an error: [`TransactionResponse::lost_precondition`] reports it on a
    /// successful `commit()` call, because a redelivery meeting its own marker
    /// is the system working.
    pub fn kv_put_if_absent(
        self,
        ns: &str,
        key: &str,
        value: serde_json::Value,
        expiry: Expiry,
    ) -> Result<Self> {
        let op = KvOperation::put_if_absent(ns, key, value, expiry)
            .map_err(Error::Invalid)?
            .required();
        Ok(self.kv(op))
    }

    /// Stage an unconditional write of state alongside the ack.
    pub fn kv_put(
        self,
        ns: &str,
        key: &str,
        value: serde_json::Value,
        expiry: Expiry,
    ) -> Result<Self> {
        let op = KvOperation::put(ns, key, value, expiry).map_err(Error::Invalid)?;
        Ok(self.kv(op))
    }

    /// Stage a delete alongside the ack.
    pub fn kv_delete(self, ns: &str, key: &str) -> Self {
        self.kv(KvOperation::delete(ns, key))
    }

    /// Stage a `check` that **rolls the bundle back** unless the key is still
    /// at `version` (`0`: unless the key is absent).
    ///
    /// The gate on a key the bundle does not write. Like the marker's, its
    /// failure is a verdict and not an error:
    /// [`TransactionResponse::lost_precondition`] reports it.
    pub fn kv_check(self, ns: &str, key: &str, version: i64) -> Self {
        self.kv(KvOperation::check(ns, key, version).required())
    }

    /// Commit this bundle only while `lock` is held.
    ///
    /// The guard is a `check` of the permit's row at the lock's token,
    /// `required`: the broker judges it in the same log entry as the acks,
    /// pushes, keys and timers beside it. A holder that was paused past its
    /// lifetime and replaced commits NOTHING — which the lock by itself
    /// cannot promise, since nobody stops an expired holder from running.
    ///
    /// The token is read when [`Self::commit`] sends, and the lock's own
    /// background renewal moves it. A guard that lost to that renewal is sent
    /// again with the new token; one that lost to another holder is the
    /// verdict ([`TransactionResponse::lost_precondition`]), and the handle
    /// then reports the lock lost.
    ///
    /// `commit` fails with [`Error::Invalid`] when the handle holds nothing: a
    /// step that asked for a guard must not go out without one.
    pub fn guard(mut self, lock: &crate::locks::Lock) -> Self {
        self.guards.push(lock.clone());
        self
    }

    // ---------------------------------------------------- timer riders

    /// Stage a timer operation, built with [`crate::timers`] or by hand.
    pub fn timer(mut self, op: TimerOperation) -> Self {
        self.timers.push(op);
        self
    }

    /// Schedule a delivery in the same transaction as the ack.
    pub fn schedule(
        self,
        queue: &str,
        timer_key: &str,
        delay: std::time::Duration,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        let payload = payload.into();
        self.timer(TimerOperation::schedule(
            queue,
            timer_key,
            delay.as_millis().min(i64::MAX as u128) as i64,
            uuid::uuidv7(),
            &payload,
        ))
    }

    /// Cancel a timer in the same transaction as the ack.
    ///
    /// A cancel sent this way inherits the transaction's authorization: on a
    /// cluster over quota the whole bundle is refused. When a cancel must land
    /// regardless, [`crate::timers::Timers::cancel`] has a route that is never
    /// blocked.
    pub fn cancel_timer(self, queue: &str, timer_key: &str) -> Self {
        self.timer(TimerOperation::cancel(queue, timer_key))
    }

    /// How many operations are staged (each link is one). Riders are not
    /// operations and are not counted here — they occupy their own arrays,
    /// and their results sit after the operations' in the response.
    pub fn len(&self) -> usize {
        self.operations.len() + self.links.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
            && self.links.is_empty()
            && self.kv.is_empty()
            && self.guards.is_empty()
            && self.timers.is_empty()
    }

    /// Send it.
    ///
    /// A rolled-back transaction comes back as HTTP 200 with `success: false`,
    /// and this surfaces it as an error — a caller who ignored it would believe
    /// a handoff happened that did not.
    ///
    /// # With one exception, and it is the reason the riders exist
    ///
    /// A rollback whose reason is `kv_precondition` **returns** instead of
    /// raising. It is not a failure: it is the answer to "has this work already
    /// been done?", it is the expected outcome of every legitimate redelivery,
    /// and raising would put the most frequent verdict of the feature inside
    /// the caller's error handling and retry policy. Read it with
    /// [`TransactionResponse::lost_precondition`], which is `None` on a commit
    /// and on every other kind of rollback:
    ///
    /// ```no_run
    /// # async fn example(tx: queen_mq::TransactionBuilder) -> queen_mq::Result<()> {
    /// let resp = tx.commit().await?;
    /// match resp.lost_precondition() {
    ///     Some(lost) => { /* somebody already did it; `lost.value` is theirs */ }
    ///     None => { /* committed */ }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// So `commit().await?` alone is no longer proof that the bundle landed.
    /// `resp.success` is.
    pub async fn commit(self) -> Result<TransactionResponse> {
        match self.commit_outcome().await? {
            TxOutcome::Answered(resp) => {
                if !resp.success && resp.lost_precondition().is_none() {
                    return Err(Error::Invalid(format!(
                        "transaction rolled back: {}",
                        resp.error.as_deref().unwrap_or("no reason given")
                    )));
                }
                Ok(resp)
            }
            TxOutcome::LockNotHeld(name) => Err(Error::Invalid(format!(
                "lock '{name}' is not held; there is nothing to guard with"
            ))),
        }
    }

    /// Send it, and hand back every verdict as it came: a commit, or a
    /// rollback with its `reason` (`duplicate`, `kv_precondition`, ...), or
    /// a guard lock that holds nothing (the JS client's `LOCK_NOT_HELD`).
    /// What the feed runner tells apart; [`Self::commit`] is built on it.
    pub(crate) async fn commit_outcome(self) -> Result<TxOutcome> {
        if self.is_empty() {
            return Err(Error::Invalid(
                "a transaction needs at least one operation".into(),
            ));
        }

        // Where the `kv` rider starts in the flat `results` space: one slot
        // per pushed item, per ack and per link come first.
        let ops_flat: usize = self
            .operations
            .iter()
            .map(|op| match op {
                TxnOperation::Push { items } => items.len(),
                TxnOperation::Ack(_) => 1,
            })
            .sum::<usize>()
            + self.links.len();
        let base = TransactionRequest::new(self.operations)
            .with_required_leases(self.leases)
            .with_kv(self.kv)
            .with_timers(self.timers);
        let guards = self.guards;
        let links = self.links;

        // A bundle is sent again in ONE case: its guard lost to the lock's
        // own background renewal, which moved the token between the moment
        // the body was built and the moment the broker judged it. The row
        // still names this owner, so the lock is held; nothing committed (a
        // lost required precondition rolls the whole bundle back), so sending
        // it again with the new token is the same step, not a second one.
        let mut attempt = 0;
        let resp = loop {
            let mut req = base.clone();
            if !guards.is_empty() {
                // The guards go first in `kv`: guard `i` is op `i` of the rider.
                let mut kv = Vec::with_capacity(guards.len() + req.kv.len());
                for lock in &guards {
                    lock.settled().await;
                    match lock.guard() {
                        Some(op) => kv.push(op),
                        None => return Ok(TxOutcome::LockNotHeld(lock.name().to_string())),
                    }
                }
                kv.append(&mut req.kv);
                req.kv = kv;
            }
            let resp: Option<TransactionResponse> = if links.is_empty() {
                self.inner.http.transaction(&req, &Opts::default()).await?
            } else {
                self.inner
                    .http
                    .post_json(
                        "/api/v1/transaction",
                        &Linked::new(&req, &links),
                        &Opts::default(),
                    )
                    .await?
            };
            let resp =
                resp.ok_or_else(|| Error::Decode("transaction returned an empty body".into()))?;
            let lost_guard = resp.lost_precondition().and_then(|lost| {
                let i = lost.failed_index?.checked_sub(ops_flat)?;
                guards.get(i).map(|lock| (lock, lost))
            });
            let Some((lock, lost)) = lost_guard else {
                break resp;
            };
            let own_renewal = lost.reason == Some(queen_protocol::KvReason::Version)
                && lost
                    .value
                    .as_ref()
                    .and_then(|v| v.get("owner"))
                    .and_then(|o| o.as_str())
                    == Some(lock.owner());
            if !own_renewal {
                // Expired, released, or another holder's: the lock is gone.
                lock.mark_lost();
                break resp;
            }
            lock.settled().await;
            if attempt >= 3 || !lock.held() {
                break resp;
            }
            attempt += 1;
        };
        Ok(TxOutcome::Answered(resp))
    }
}

/// What [`TransactionBuilder::commit_outcome`] got.
#[derive(Debug)]
pub(crate) enum TxOutcome {
    /// The broker's answer: `success`, or a rollback with its `reason`.
    Answered(TransactionResponse),
    /// A guard's lock holds nothing: the bundle was not sent.
    LockNotHeld(String),
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
            headers: None,
            created_at: "2026-08-04T10:00:00Z".into(),
            partition_id: "p1".into(),
            partition: "Default".into(),
            lease_id: lease.into(),
            consumer_group: group.into(),
            offset: Some(41),
        }
    }

    /// The body `commit` sends for a bundle with links.
    fn linked_body(t: &TransactionBuilder) -> serde_json::Value {
        let req = TransactionRequest::new(t.operations.clone())
            .with_required_leases(t.leases.clone())
            .with_kv(t.kv.clone())
            .with_timers(t.timers.clone());
        serde_json::to_value(Linked::new(&req, &t.links)).unwrap()
    }

    #[test]
    fn a_link_is_one_operation_per_message_at_its_place() {
        let entry_from = crate::LinkFrom::by_name("orders", "order-1", 7, "t-feed");
        let t = queen()
            .transaction()
            .ack(&msg("t1", "L1", "billing"))
            .link("review", "order-1", [&msg("t1", "L1", "billing")])
            .unwrap()
            .push_to("audit", "order-1", serde_json::json!({"n": 1}))
            .unwrap()
            .link_with_headers("review", "", [&entry_from], [("review-reason", "amount")])
            .unwrap();
        assert_eq!(t.len(), 4);
        let body = linked_body(&t);
        let ops = body["operations"].as_array().unwrap();
        let kinds: Vec<&str> = ops.iter().map(|o| o["type"].as_str().unwrap()).collect();
        assert_eq!(kinds, vec!["ack", "link", "push", "link"]);
        assert_eq!(
            ops[1],
            serde_json::json!({"type": "link", "queue": "review", "partition": "order-1",
                "from": {"partitionId": "p1", "offset": 41, "transactionId": "t1",
                         "consumerGroup": "billing"}})
        );
        assert_eq!(
            ops[3],
            serde_json::json!({"type": "link", "queue": "review", "partition": "Default",
                "from": {"queue": "orders", "partition": "order-1", "offset": 7,
                         "transactionId": "t-feed"},
                "headers": {"review-reason": "amount"}})
        );
        assert_eq!(body["requiredLeases"], serde_json::json!(["L1"]));
        assert!(body.get("kv").is_none() && body.get("timers").is_none());
    }

    #[test]
    fn a_bundle_without_links_is_written_as_the_request_type_writes_it() {
        let t = queen()
            .transaction()
            .ack(&msg("t1", "L1", "g"))
            .push_to("out", "p", serde_json::json!(1))
            .unwrap()
            .kv_delete("ns", "k");
        let req = TransactionRequest::new(t.operations.clone())
            .with_required_leases(t.leases.clone())
            .with_kv(t.kv.clone());
        assert_eq!(
            serde_json::to_string(&Linked::new(&req, &[])).unwrap(),
            serde_json::to_string(&req).unwrap()
        );
    }

    #[test]
    fn a_push_after_a_link_does_not_join_the_push_before_it() {
        let t = queen()
            .transaction()
            .push_to("out", "p", serde_json::json!(1))
            .unwrap()
            .link("out", "p", [&msg("t1", "L1", "g")])
            .unwrap()
            .push_to("out", "p", serde_json::json!(2))
            .unwrap();
        let kinds: Vec<String> = linked_body(&t)["operations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| o["type"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(kinds, vec!["push", "link", "push"]);
    }

    #[test]
    fn a_link_needs_an_offset_a_queue_and_names_the_broker_takes() {
        let mut no_offset = msg("t1", "L1", "g");
        no_offset.offset = None;
        let err = queen()
            .transaction()
            .link("review", "p", [&no_offset])
            .err()
            .expect("no offset, no link")
            .to_string();
        assert!(err.contains("carries no offset"), "{err}");
        assert!(queen()
            .transaction()
            .link("", "p", [&msg("t1", "L1", "g")])
            .is_err());
        assert!(queen()
            .transaction()
            .link_with_headers("review", "p", [&msg("t1", "L1", "g")], [("Bad Name", "x")])
            .is_err());
        // An empty list stages nothing.
        let t = queen()
            .transaction()
            .link("review", "p", Vec::<&Message>::new())
            .unwrap();
        assert!(t.is_empty());
    }

    #[test]
    fn a_push_items_headers_are_checked() {
        let bad = TxnPushItem::new("q", serde_json::json!(1)).headers([("queen-x", "1")]);
        assert!(queen().transaction().push_item(bad).is_err());
        let good = TxnPushItem::new("q", serde_json::json!(1)).headers([("type", "a")]);
        assert!(queen().transaction().push_item(good).is_ok());
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
