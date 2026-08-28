//! Scheduled deliveries: a message that becomes real later.
//!
//! A timer is not a job. It is a **push that has not happened yet**: at
//! `deliverAt` the broker deletes the row and pushes the frame in one
//! transaction, into the queue and partition named at schedule time, where it
//! is an ordinary message with an ordinary consumer.
//!
//! # What the contract says, and where it hurts
//!
//! * `deliverAt` is **"no earlier than"**, never "exactly at".
//! * A delay of zero, or a delay already in the past, is **legal** and fires on
//!   the first sweep cycle.
//! * **There is no tombstone.** A delivered timer leaves no row, so a cancel
//!   that arrives afterwards answers [`TimerStatus::Absent`], which means *no
//!   longer pending* and **may mean already delivered**. The authority is the
//!   log: the response carries the `txn`, so look for that transaction id in
//!   the destination queue.
//! * A cancel or a reschedule that meets a timer a broker has already claimed
//!   answers [`TimerStatus::TooLate`]. That broker has packed the payload and
//!   is about to commit it; the window is bounded by the sweeper lease. The
//!   remedy is a new key, or waiting for the delivery and acting on the
//!   message.
//!
//! Which is why the saga shape is this one, and not the obvious one: the
//! consumer of a compensation queue **checks the saga's KV state before
//! compensating**. Without that check, a timer that fired five milliseconds
//! before the cancel undoes a booking that has already gone out — and the
//! cancel answered `absent` with an HTTP 200.
//!
//! ```no_run
//! use std::time::Duration;
//! use queen_mq::Queen;
//!
//! # async fn example() -> queen_mq::Result<()> {
//! let queen = Queen::connect_to("http://localhost:6789")?;
//!
//! // If the saga has not closed in fifteen minutes, compensate.
//! queen
//!     .timers()
//!     .schedule("compensations", "saga:9137", Duration::from_secs(900))
//!     .payload_json(&serde_json::json!({ "sagaId": 9137 }))?
//!     .send()
//!     .await?;
//!
//! // ...and when it closes:
//! let cancelled = queen.timers().cancel("compensations", "saga:9137").await?;
//! if !cancelled.ok {
//!     // absent or too_late — it may already be on its way.
//! }
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;
use std::time::Duration;

use queen_protocol::timers::{
    TimerOperation, TimerPage, TimerPeek, TimerRequest, TimerResponse, TimerResult,
};
use serde::Serialize;

use crate::error::{Error, Result};
use crate::http::Opts;
use crate::inner::Inner;
use crate::queue::urlencode;
use crate::uuid;

/// The timer surface. Obtained from [`crate::Queen::timers`].
#[derive(Clone)]
pub struct Timers {
    inner: Arc<Inner>,
}

impl Timers {
    pub(crate) fn new(inner: Arc<Inner>) -> Self {
        Self { inner }
    }

    /// Schedule a delivery.
    ///
    /// `timer_key` is the timer's identity inside the queue: scheduling again
    /// under the same key **overwrites** the pending one, which is what makes a
    /// retry after a crash safe rather than a second delivery.
    ///
    /// `Duration::ZERO` is legal and means "on the next cycle".
    pub fn schedule(&self, queue: &str, timer_key: &str, delay: Duration) -> ScheduleBuilder {
        ScheduleBuilder {
            timers: self.clone(),
            queue: queue.to_string(),
            timer_key: timer_key.to_string(),
            delay_ms: delay.as_millis().min(i64::MAX as u128) as i64,
            partition: None,
            txn: None,
            payload: None,
            reschedule: false,
        }
    }

    /// Cancel a pending timer.
    ///
    /// This goes through `DELETE /api/v1/timers/:queue/*timerKey`, which has a
    /// route and an authorization class of its own **precisely so that nothing
    /// can block it**: the fire never switches itself off, so a caller that
    /// could not cancel would keep producing messages it cannot stop. Cancels
    /// sent through the batch route inherit the schedule's class and can be
    /// refused with it.
    pub async fn cancel(&self, queue: &str, timer_key: &str) -> Result<TimerResult> {
        self.cancel_inner(queue, timer_key, None).await
    }

    /// Cancel, telling the broker which `txn` you expect.
    ///
    /// It comes back on `absent`, so "was it already delivered?" can be
    /// answered by looking for that transaction id in the destination queue —
    /// with no second API call.
    pub async fn cancel_expecting(
        &self,
        queue: &str,
        timer_key: &str,
        txn: &str,
    ) -> Result<TimerResult> {
        self.cancel_inner(queue, timer_key, Some(txn)).await
    }

    async fn cancel_inner(
        &self,
        queue: &str,
        timer_key: &str,
        txn: Option<&str>,
    ) -> Result<TimerResult> {
        let mut path = format!(
            "/api/v1/timers/{}/{}",
            urlencode(queue),
            urlencode(timer_key)
        );
        // The only query parameter this route reads, and it is the caller's own
        // identifier: nothing is disclosed by its presence.
        if let Some(t) = txn.filter(|t| !t.is_empty()) {
            path.push_str(&format!("?txn={}", urlencode(t)));
        }
        let out: Option<TimerResult> = self.inner.http.delete_json(&path, &Opts::default()).await?;
        out.ok_or_else(|| Error::Decode("timer cancel returned an empty body".into()))
    }

    /// Read one pending timer, with its payload.
    ///
    /// The payload comes back **as stored**: if the queue encrypts at rest,
    /// this is the envelope and `encrypted` says so. Peek is an inspection
    /// surface and does not quietly decrypt what the delivery will carry.
    pub async fn peek(&self, queue: &str, timer_key: &str) -> Result<TimerPeek> {
        let path = format!(
            "/api/v1/timers/{}/{}",
            urlencode(queue),
            urlencode(timer_key)
        );
        let out: Option<TimerPeek> = self.inner.http.get_json(&path, &Opts::default()).await?;
        out.ok_or_else(|| Error::Decode("timer peek returned an empty body".into()))
    }

    /// List a queue's pending timers, a page at a time.
    ///
    /// The queue is mandatory and there is no tenant-wide list: that would be a
    /// scan an end user of yours could trigger.
    pub fn list(&self, queue: &str) -> ListQuery {
        ListQuery {
            timers: self.clone(),
            queue: queue.to_string(),
            after: None,
            limit: None,
        }
    }

    /// Send operations built by hand, in one round trip.
    ///
    /// Note the asymmetry this route has and [`Timers::cancel`] does not: on a
    /// cluster over quota, a batch carrying even one schedule is refused
    /// **whole**, cancels included. That is deliberate — half-applying a batch
    /// is worse — but it means a cancel that must land belongs on the DELETE
    /// route.
    pub async fn batch(&self, ops: Vec<TimerOperation>) -> Result<Vec<TimerResult>> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }
        let expected = ops.len();
        let req = TimerRequest::new(ops);
        let resp: Option<TimerResponse> = self
            .inner
            .http
            .post_json("/api/v1/timers", &req, &Opts::default())
            .await?;
        let resp =
            resp.ok_or_else(|| Error::Decode("timer batch returned an empty body".into()))?;
        if resp.results.len() != expected {
            return Err(Error::Decode(format!(
                "timers returned {} results for {expected} operations",
                resp.results.len()
            )));
        }
        Ok(resp.results)
    }
}

/// A schedule under construction.
pub struct ScheduleBuilder {
    timers: Timers,
    queue: String,
    timer_key: String,
    delay_ms: i64,
    partition: Option<String>,
    txn: Option<String>,
    payload: Option<Vec<u8>>,
    reschedule: bool,
}

impl ScheduleBuilder {
    /// The lane the delivered message lands on. Defaults to `Default`.
    ///
    /// Two timers on the same lane that come due together enter the log in
    /// **expiry** order, not in the order they were scheduled.
    pub fn partition(mut self, partition: &str) -> Self {
        self.partition = Some(partition.to_string());
        self
    }

    /// The transaction id the delivered message will carry. Minted as a uuidv7
    /// when not given.
    ///
    /// Set it when the delivery has to be deduplicated against something the
    /// caller already knows — it is also what a later cancel echoes back.
    pub fn txn(mut self, txn: &str) -> Self {
        self.txn = Some(txn.to_string());
        self
    }

    /// The message body, as bytes.
    pub fn payload(mut self, payload: impl Into<Vec<u8>>) -> Self {
        self.payload = Some(payload.into());
        self
    }

    /// The message body, as JSON — which is what a consumer of this queue will
    /// find in `message.data`.
    pub fn payload_json<T: Serialize>(mut self, payload: &T) -> Result<Self> {
        self.payload = Some(serde_json::to_vec(payload)?);
        Ok(self)
    }

    /// Name this a `reschedule`.
    ///
    /// The broker treats it identically to a schedule — same upsert, and
    /// `attempts` goes back to zero either way, because a rescheduled timer is
    /// a new timer under an old name and must not inherit the retry budget the
    /// old payload spent. The name is for whoever reads the call site.
    pub fn reschedule(mut self) -> Self {
        self.reschedule = true;
        self
    }

    /// The operation as it would be sent inside a transaction.
    pub fn operation(self) -> Result<TimerOperation> {
        let payload = self.payload.ok_or_else(|| {
            Error::Invalid(
                "a timer needs a payload: it becomes a message, and a message has a body".into(),
            )
        })?;
        let txn = self.txn.unwrap_or_else(uuid::uuidv7);
        let mut op =
            TimerOperation::schedule(&self.queue, &self.timer_key, self.delay_ms, txn, &payload);
        if let Some(p) = self.partition {
            op = op.partition(p);
        }
        if self.reschedule {
            op = op.reschedule();
        }
        Ok(op)
    }

    pub async fn send(self) -> Result<TimerResult> {
        let timers = self.timers.clone();
        let mut out = timers.batch(vec![self.operation()?]).await?;
        Ok(out.remove(0))
    }
}

/// A page of pending timers under construction.
pub struct ListQuery {
    timers: Timers,
    queue: String,
    after: Option<String>,
    limit: Option<i32>,
}

impl ListQuery {
    /// Resume after this timer key. Exclusive, and stable: the ordering is byte
    /// order, identical between machines and unchanged by a locale upgrade.
    pub fn after(mut self, after: &str) -> Self {
        self.after = Some(after.to_string());
        self
    }

    /// Clamped server-side rather than rejected, with `truncated` telling the
    /// truth.
    pub fn limit(mut self, limit: i32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub async fn send(self) -> Result<TimerPage> {
        let mut params: Vec<(&'static str, String)> = Vec::new();
        if let Some(a) = &self.after {
            params.push(("after", a.clone()));
        }
        if let Some(l) = self.limit {
            params.push(("limit", l.to_string()));
        }
        let mut path = format!("/api/v1/timers/{}", urlencode(&self.queue));
        if !params.is_empty() {
            path.push('?');
            path.push_str(&crate::queue::encode_pairs(&params));
        }
        let out: Option<TimerPage> = self
            .timers
            .inner
            .http
            .get_json(&path, &Opts::default())
            .await?;
        Ok(out.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::Queen;
    use queen_protocol::timers::TimerOpKind;

    fn timers() -> Timers {
        Queen::connect(Config::new("http://127.0.0.1:1"))
            .unwrap()
            .timers()
    }

    #[test]
    fn a_schedule_without_a_payload_is_refused_before_the_network() {
        let e = timers()
            .schedule("q", "k", Duration::from_secs(60))
            .operation()
            .unwrap_err();
        assert!(e.to_string().contains("payload"), "{e}");
    }

    #[test]
    fn a_delay_becomes_milliseconds() {
        let op = timers()
            .schedule("q", "k", Duration::from_millis(250))
            .payload(b"x".to_vec())
            .operation()
            .unwrap();
        assert_eq!(op.delay_ms, Some(250));

        let zero = timers()
            .schedule("q", "k", Duration::ZERO)
            .payload(b"x".to_vec())
            .operation()
            .unwrap();
        assert_eq!(
            zero.delay_ms,
            Some(0),
            "zero is legal and fires on the first cycle"
        );
    }

    #[test]
    fn a_txn_is_minted_when_the_caller_does_not_supply_one() {
        let op = timers()
            .schedule("q", "k", Duration::from_secs(1))
            .payload(b"x".to_vec())
            .operation()
            .unwrap();
        let txn = op.txn.unwrap();
        assert!(
            uuid::is_valid_uuid(&txn),
            "the broker requires a txn; an empty one is a 400: {txn}"
        );
    }

    #[test]
    fn reschedule_differs_only_in_the_name() {
        let op = timers()
            .schedule("q", "k", Duration::from_secs(1))
            .txn("t")
            .payload(b"x".to_vec())
            .reschedule()
            .operation()
            .unwrap();
        assert_eq!(op.op, TimerOpKind::Reschedule);
        assert_eq!(op.txn.as_deref(), Some("t"));
    }

    #[test]
    fn a_json_payload_is_the_body_a_consumer_will_read() {
        let op = timers()
            .schedule("q", "k", Duration::from_secs(1))
            .payload_json(&serde_json::json!({"sagaId": 9137}))
            .unwrap()
            .operation()
            .unwrap();
        let decoded =
            queen_protocol::timers::base64_decode(op.payload.as_deref().unwrap()).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&decoded).unwrap()["sagaId"],
            9137
        );
    }

    #[tokio::test]
    async fn an_empty_batch_never_reaches_the_network() {
        assert!(timers().batch(Vec::new()).await.unwrap().is_empty());
    }
}
