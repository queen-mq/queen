//! Ephemeral queues: RAM-class rings behind `/api/v1/ephemeral/*`
//! (EPHEMERAL_QUEUES.md §1, §3.1, §4).
//!
//! Eight verbs: [`Ephemeral::configure`], [`Ephemeral::reset`],
//! [`Ephemeral::delete`], [`Ephemeral::push`], [`Ephemeral::pop`],
//! [`Ephemeral::ack`], [`Ephemeral::queues`] and [`Ephemeral::depth`].
//!
//! # What this class is about, before any signature
//!
//! Contents survive **nothing** (§1.2). Not a restart, not a crash, not a
//! deploy, not the ownership move that a membership change causes. Treat a
//! failover like a Redis restart. Declared *configuration* is durable — it lives
//! in PostgreSQL and comes back after a restart, as configured and EMPTY. There
//! is no replay, no history, no subscription mode and no DLQ, because none of
//! those concepts has a referent when there is no history to have.
//!
//! # Delivery is not "at most once"
//!
//! The class picks what can be LOST; the ack mode picks the guarantee (§1.3).
//! [`EphemeralPopBuilder::auto_ack`] advances the cursor at delivery and is
//! at-most-once. The default — explicit ack — is at-least-once for as long as
//! the owning broker incarnation lives: an unacked message redelivers when its
//! lease expires, with `attempts` incremented, until the queue's `retryLimit`,
//! after which it is dropped and counted. Consumers still need idempotency,
//! exactly as on durable queues.
//!
//! # Consumption semantics come from the group
//!
//! Exactly as on the durable engine (§1.5). There is no queue-level mode:
//!
//! ```no_run
//! # use queen_mq::Queen;
//! # async fn example(queen: Queen) -> queen_mq::Result<()> {
//! let eph = queen.ephemeral();
//!
//! // competing consumers: one cursor shared by everyone in the group
//! let work = eph.pop("inbox").group("workers").batch(10).send().await?;
//!
//! // fan-out: this subscriber's own cursor over the same ring
//! let tail = eph.pop("inbox").group("tail-a").send().await?;
//!
//! // groupless queue mode, as on durable
//! let any = eph.pop("inbox").send().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Two differences from the durable surface, both deliberate
//!
//! * **A pop returns a [`EphemeralBatch`], not a bare vector.** The batch
//!   carries the queue and the group the pop used, so [`Ephemeral::ack`] can
//!   read them off it. That keeps this crate's rule — *acks read the consumer
//!   group from the delivery, not from a separate argument, so a forgotten group
//!   cannot ack the wrong cursor* — which the wire alone would not: an
//!   ephemeral delivery carries `{id, partition, payload, attempts}` and no
//!   group.
//!
//! * **A 404 is one error, with one exception.** No SDK in this repo negotiates
//!   a version, and against a broker or proxy older than 1.1 the whole family
//!   answers 404 — the broker because the routes do not exist, the proxy because
//!   an unknown API path is `route_blocked`. Every verb here rewrites that into
//!   one message, and [`is_unsupported`] is the predicate to branch on.
//!
//!   The exception is [`Ephemeral::depth`], the only verb that has to say "no
//!   such queue": every other one either creates the queue by naming it (push,
//!   pop) or describes a miss inside a 200 (`reset` answers `dropped:0`,
//!   `delete` answers `deleted:false`). Its 404 carries
//!   [`queen_protocol::EPHEMERAL_QUEUE_NOT_FOUND_CODE`] and answers
//!   [`is_queue_not_found`] instead. That is why the two predicates read the
//!   CODE and not just the status they share.

use std::sync::Arc;
use std::time::Duration;

use queen_protocol::{
    EphemeralAck, EphemeralAckRequest, EphemeralAckResponse, EphemeralAckResult,
    EphemeralConfigureRequest, EphemeralDeleteResponse, EphemeralDelivered, EphemeralMessage,
    EphemeralOptions, EphemeralPopParams, EphemeralPopResponse, EphemeralPushRequest,
    EphemeralPushResponse, EphemeralResetRequest, EphemeralResetResponse, EphemeralStatus,
};
use serde::Serialize;
use serde_json::Value;

use crate::buffer::{self, BufferOptions, Destination, EPHEMERAL_PUSH_PATH};
use crate::config::RetryKind;
use crate::error::{Error, Result};
use crate::http::Opts;
use crate::inner::Inner;
use crate::queue::urlencode;

/// The message every SDK fixes for the old-broker case (§4). Keep it identical
/// across clients: an operator grepping two clients' logs should see one string.
pub const EPHEMERAL_UNSUPPORTED: &str =
    "broker/proxy does not support ephemeral queues (requires >= 1.1)";

/// Whether an error from one of these verbs is the family-wide "upgrade the
/// broker or the proxy in front of it" refusal.
///
/// A 404 whose body carries no ephemeral code of its own: an old broker never
/// registered the routes, an old proxy fails closed on unknown API paths and
/// answers `route_blocked`. Neither is a statement about a queue.
///
/// The original `code` is preserved on the error while the message is rewritten
/// to [`EPHEMERAL_UNSUPPORTED`]. Branch on this predicate or on the code, never
/// on the prose.
pub fn is_unsupported(err: &Error) -> bool {
    err.status() == Some(404) && !is_queue_not_found(err)
}

/// Whether an error is [`Ephemeral::depth`]'s "no such queue".
///
/// The one 404 on this family that is about a queue rather than about the
/// routes — and on this class it also means "empty and idle long enough to have
/// been collected", because an implicit queue is a live ring and nothing else.
pub fn is_queue_not_found(err: &Error) -> bool {
    err.status() == Some(404)
        && err.code().map(|c| c.as_str()) == Some(queen_protocol::EPHEMERAL_QUEUE_NOT_FOUND_CODE)
}

/// Rewrite the routes-are-missing 404 into the one message, keeping status and
/// code intact. Everything else — including depth's own 404 — passes through.
fn map_unsupported(err: Error) -> Error {
    if !is_unsupported(&err) {
        return err;
    }
    let code = err.code().cloned();
    let retry_after_seconds = match &err {
        Error::Http {
            retry_after_seconds,
            ..
        } => *retry_after_seconds,
        _ => None,
    };
    Error::Http {
        status: 404,
        message: EPHEMERAL_UNSUPPORTED.to_string(),
        code,
        retry_after_seconds,
    }
}

/// The ephemeral surface. Obtained from [`crate::Queen::ephemeral`].
#[derive(Clone)]
pub struct Ephemeral {
    inner: Arc<Inner>,
}

/// What a push produced.
///
/// `buffered` is the fork in this struct: a buffered push resolves once the
/// messages are IN the buffer, not once they are at the broker, so `pushed` is
/// meaningless there and `count` is what the buffer accepted instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EphemeralPushed {
    /// The broker's own count, from `{pushed}`. Zero on a buffered push.
    pub pushed: u64,
    /// These messages went into a client-side buffer, not to the broker.
    pub buffered: bool,
    /// How many messages this call accounted for.
    pub count: usize,
}

/// One pop's worth of messages, plus the identity the pop used.
///
/// The queue and the group travel with the delivery so that acking cannot
/// address the wrong cursor — see the module docs.
#[derive(Debug, Clone, PartialEq)]
pub struct EphemeralBatch {
    pub queue: String,
    /// The group the pop used. `None` is the groupless queue mode.
    pub group: Option<String>,
    /// Empty rather than absent when the poll found nothing.
    pub messages: Vec<EphemeralDelivered>,
}

impl EphemeralBatch {
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// The ids in this batch, in delivery order.
    pub fn ids(&self) -> Vec<&str> {
        self.messages.iter().map(|m| m.id.as_str()).collect()
    }
}

impl Ephemeral {
    pub(crate) fn new(inner: Arc<Inner>) -> Self {
        Self { inner }
    }

    // ------------------------------------------------------------ declaration

    /// Declare a queue and its bounds, persisting the OPTIONS in PostgreSQL
    /// (§1.1): the configuration survives a restart, the contents never do, and
    /// the queue comes back declared and empty.
    ///
    /// Optional in every sense — a push or a pop that names an unknown queue
    /// creates it implicitly with the tenant defaults. Declare when you want
    /// non-default bounds, or when you want the queue to exist in the dashboard
    /// before its first message.
    ///
    /// [`EphemeralOptions`] is the closed list of §3.1, and a struct is this
    /// language's way of refusing an option the client does not know: the JS
    /// client has to check the key set by hand, because an object literal would
    /// happily carry a misspelt `ttlSecond` that is then dropped on the floor —
    /// and a silently ignored bound is a ring that grows until a global budget
    /// answers 503.
    pub async fn configure(&self, queue: &str, options: EphemeralOptions) -> Result<Value> {
        let req = EphemeralConfigureRequest { queue, options };
        let out: Option<Value> = self
            .inner
            .http
            .post_json("/api/v1/ephemeral/configure", &req, &Opts::default())
            .await
            .map_err(map_unsupported)?;
        Ok(out.unwrap_or(Value::Null))
    }

    /// Drop every message, void every lease, rewind every group cursor. Returns
    /// how many messages were dropped.
    ///
    /// A verb that would be indefensible on a durable queue and is merely honest
    /// here: it destroys nothing the class ever promised to keep (§1.2). The
    /// declared configuration stays.
    pub async fn reset(&self, queue: &str) -> Result<u64> {
        let req = EphemeralResetRequest { queue };
        let out: Option<EphemeralResetResponse> = self
            .inner
            .http
            .post_json("/api/v1/ephemeral/reset", &req, &Opts::default())
            .await
            .map_err(map_unsupported)?;
        Ok(out.map(|r| r.dropped).unwrap_or(0))
    }

    /// Delete the queue: contents, cursors, and the declared configuration in
    /// PostgreSQL.
    ///
    /// READ `deleted`, NOT the `Ok`. A queue that was not there is a 200 with
    /// `deleted: false`, and the scar behind that is the durable queue delete:
    /// it answers the same way, and every client that ignored the field read a
    /// miss as a success.
    pub async fn delete(&self, queue: &str) -> Result<EphemeralDeleteResponse> {
        let path = format!("/api/v1/ephemeral/queue/{}", urlencode(queue));
        let out: Option<EphemeralDeleteResponse> = self
            .inner
            .http
            .delete_json(&path, &Opts::default())
            .await
            .map_err(map_unsupported)?;
        Ok(out.unwrap_or_else(|| EphemeralDeleteResponse {
            queue: queue.to_string(),
            ..Default::default()
        }))
    }

    // ------------------------------------------------------------------- push

    /// Push one payload. All-or-nothing per request.
    pub fn push<T: Serialize>(&self, queue: &str, payload: T) -> EphemeralPushBuilder {
        self.push_many(queue, std::iter::once(payload))
    }

    /// Push several payloads to the same queue and partition.
    ///
    /// A payload that will not serialize is carried on the builder and surfaces
    /// from [`EphemeralPushBuilder::send`], rather than making every call site
    /// unwrap a `Result` before it can name a partition.
    pub fn push_many<T, I>(&self, queue: &str, payloads: I) -> EphemeralPushBuilder
    where
        T: Serialize,
        I: IntoIterator<Item = T>,
    {
        let mut messages = Vec::new();
        let mut error = None;
        for p in payloads {
            match serde_json::to_value(p) {
                Ok(v) => messages.push(EphemeralMessage::new(v)),
                Err(e) => {
                    error = Some(Error::Decode(format!(
                        "an ephemeral payload cannot be encoded as JSON: {e}"
                    )));
                    break;
                }
            }
        }
        EphemeralPushBuilder {
            inner: Arc::clone(&self.inner),
            queue: queue.to_string(),
            partition: None,
            messages,
            buffer: None,
            error,
        }
    }

    /// Send everything buffered for one ephemeral queue and partition, now.
    ///
    /// [`crate::Queen::close`] and [`crate::Queen::flush_all_buffers`] already
    /// drain ephemeral buffers with everything else — they live in one manager
    /// under a namespaced address. This is for the times one queue has to land
    /// before the rest.
    pub async fn flush_buffer(&self, queue: &str, partition: Option<&str>) -> Result<()> {
        self.inner
            .buffers
            .flush(&buffer::ephemeral_address(queue, partition))
            .await?;
        Ok(())
    }

    // -------------------------------------------------------------------- pop

    /// Start a pop. Nothing leaves until [`EphemeralPopBuilder::send`].
    pub fn pop(&self, queue: &str) -> EphemeralPopBuilder {
        EphemeralPopBuilder {
            inner: Arc::clone(&self.inner),
            queue: queue.to_string(),
            partition: None,
            batch: None,
            wait: false,
            poll_timeout: Duration::from_millis(
                queen_protocol::EPHEMERAL_DEFAULT_WAIT_TIMEOUT_MILLIS,
            ),
            group: None,
            auto_ack: false,
        }
    }

    // -------------------------------------------------------------------- ack

    /// Mark every message of a batch completed.
    ///
    /// The queue and the group come from the batch, so this cannot ack the
    /// wrong cursor.
    pub async fn ack(&self, batch: &EphemeralBatch) -> Result<Vec<EphemeralAckResult>> {
        self.ack_with(batch, EphemeralStatus::Completed, None).await
    }

    /// Reject every message of a batch. They come back with `attempts + 1`
    /// until the queue's `retryLimit`, after which they are dropped and counted
    /// — there is no DLQ on this class (§9).
    pub async fn nack(
        &self,
        batch: &EphemeralBatch,
        reason: impl Into<String>,
    ) -> Result<Vec<EphemeralAckResult>> {
        self.ack_with(batch, EphemeralStatus::Failed, Some(reason.into()))
            .await
    }

    /// Ack a whole batch with an explicit outcome.
    pub async fn ack_with(
        &self,
        batch: &EphemeralBatch,
        status: EphemeralStatus,
        error: Option<String>,
    ) -> Result<Vec<EphemeralAckResult>> {
        if batch.messages.is_empty() {
            return Ok(Vec::new());
        }
        let acks: Vec<EphemeralAck> = batch
            .messages
            .iter()
            .map(|m| {
                let mut a = EphemeralAck::new(m.id.clone()).status(status);
                if let Some(e) = &error {
                    a = a.error(e.clone());
                }
                a
            })
            .collect();
        let mut builder = self.ack_ids(&batch.queue, acks);
        if let Some(g) = &batch.group {
            builder = builder.group(g);
        }
        builder.send().await
    }

    /// Ack hand-built entries — a mixed batch, or ids kept across a restart.
    ///
    /// This is the escape hatch under [`Ephemeral::ack`], and the group has to
    /// be supplied because there is no delivery to read it from. Pass the same
    /// one the pop used: cursors are per group.
    pub fn ack_ids(&self, queue: &str, acks: Vec<EphemeralAck>) -> EphemeralAckBuilder {
        EphemeralAckBuilder {
            inner: Arc::clone(&self.inner),
            queue: queue.to_string(),
            group: None,
            acks,
        }
    }

    // ----------------------------------------------------------------- status

    /// Every ephemeral queue this tenant currently has, declared and implicit.
    ///
    /// Free to poll: the gauges are read out of the broker's own memory, with no
    /// database behind them — unlike the durable meter, whose 1s poll is
    /// load-bearing on PostgreSQL.
    ///
    /// Untyped on purpose. The renderer for this route does not exist in the
    /// broker yet, so its key names are not decided, and putting a guess in
    /// `queen-protocol` — the crate whose job is to be the written-down truth —
    /// would be worse than reading the body as it arrives.
    pub async fn queues(&self) -> Result<Value> {
        let out: Option<Value> = self
            .inner
            .http
            .get_json("/api/v1/ephemeral/queues", &Opts::default())
            .await
            .map_err(map_unsupported)?;
        Ok(out.unwrap_or(Value::Null))
    }

    /// Gauges for one queue: ring length, bytes, and the per-group cursors.
    /// Same untyped-body rule as [`Ephemeral::queues`].
    ///
    /// The ONE verb of this family that answers a 404 about a queue rather than
    /// about the routes — see [`is_queue_not_found`].
    pub async fn depth(&self, queue: &str) -> Result<Value> {
        let path = format!("/api/v1/ephemeral/queues/{}/depth", urlencode(queue));
        let out: Option<Value> = self
            .inner
            .http
            .get_json(&path, &Opts::default())
            .await
            .map_err(map_unsupported)?;
        Ok(out.unwrap_or(Value::Null))
    }
}

/// A push under construction.
pub struct EphemeralPushBuilder {
    inner: Arc<Inner>,
    queue: String,
    partition: Option<String>,
    messages: Vec<EphemeralMessage>,
    buffer: Option<BufferOptions>,
    error: Option<Error>,
}

impl EphemeralPushBuilder {
    /// Pick the ring. FIFO is per `(queue, partition)` within one ownership
    /// incarnation (§1.4).
    ///
    /// Left unset the field never reaches the wire and the BROKER picks: which
    /// partition an ephemeral push without one lands on is its rule to make.
    pub fn partition(mut self, partition: impl Into<String>) -> Self {
        self.partition = Some(partition.into());
        self
    }

    /// Batch client-side through the same machinery the durable push uses
    /// (§4.1): blocking backpressure at `max_size`, a failed batch put back at
    /// the FRONT and retried until it lands, [`crate::Queen::close`] draining
    /// what is left under a deadline.
    ///
    /// Buffering is a client-side latency/efficiency trade, not a durability
    /// change: a buffered message that has not flushed dies with the process.
    /// That is already inside this class's contract, which is exactly why
    /// buffering is a reasonable default here and a considered decision on a
    /// durable queue.
    pub fn buffered(mut self, options: BufferOptions) -> Self {
        self.buffer = Some(options);
        self
    }

    /// Send it.
    pub async fn send(self) -> Result<EphemeralPushed> {
        if let Some(e) = self.error {
            return Err(e);
        }
        if self.queue.is_empty() {
            return Err(Error::Invalid(
                "an ephemeral push needs a queue name".into(),
            ));
        }
        let count = self.messages.len();
        if count == 0 {
            return Ok(EphemeralPushed::default());
        }

        if let Some(opts) = self.buffer {
            let address = buffer::ephemeral_address(&self.queue, self.partition.as_deref());
            let dest = Destination::Ephemeral {
                queue: self.queue.clone(),
                partition: self.partition.clone(),
            };
            for m in self.messages {
                // The buffer's element type is `PushItem` on both families. The
                // ephemeral drain reads only `payload` off it; queue and
                // partition ride along so a re-queued batch stays
                // self-describing, and `transaction_id` stays absent because
                // this wire has nowhere to put one.
                let item = queen_protocol::PushItem {
                    queue: self.queue.clone(),
                    partition: self.partition.clone(),
                    payload: m.payload,
                    transaction_id: None,
                };
                // Awaited one at a time: `add_to` is where `max_size` blocks, so
                // a buffered push that resolved without awaiting would report
                // success for messages the buffer never accepted.
                self.inner
                    .buffers
                    .add_to(address.clone(), dest.clone(), item, opts)
                    .await?;
            }
            return Ok(EphemeralPushed {
                pushed: 0,
                buffered: true,
                count,
            });
        }

        let mut req = EphemeralPushRequest::new(self.queue.clone(), self.messages);
        req.partition = self.partition.clone();
        let affinity = queen_protocol::grouping_key(
            Some(&self.queue),
            self.partition.as_deref(),
            None,
            None,
            None,
        );
        let out: Option<EphemeralPushResponse> = self
            .inner
            .http
            .post_json(EPHEMERAL_PUSH_PATH, &req, &Opts::affinity(affinity))
            .await
            .map_err(map_unsupported)?;
        Ok(EphemeralPushed {
            pushed: out.map(|r| r.pushed).unwrap_or(0),
            buffered: false,
            count,
        })
    }
}

/// A pop under construction.
pub struct EphemeralPopBuilder {
    inner: Arc<Inner>,
    queue: String,
    partition: Option<String>,
    batch: Option<i32>,
    wait: bool,
    poll_timeout: Duration,
    group: Option<String>,
    auto_ack: bool,
}

impl EphemeralPopBuilder {
    pub fn partition(mut self, partition: impl Into<String>) -> Self {
        self.partition = Some(partition.into());
        self
    }

    /// Ceiling on messages returned. The broker's default is 1.
    pub fn batch(mut self, n: i32) -> Self {
        self.batch = Some(n);
        self
    }

    /// The whole of the consumption semantics (§1.5): the same group competes,
    /// its own group fans out, no group is the groupless queue mode.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Long-poll instead of answering empty.
    ///
    /// A real long poll, parked on a RAM gate with no database behind it and no
    /// polling interval anywhere (§3.4) — the structural reason an ephemeral
    /// inbox answers in transport time.
    pub fn wait(mut self, enabled: bool) -> Self {
        self.wait = enabled;
        self
    }

    /// How long the BROKER waits. Sent only with [`EphemeralPopBuilder::wait`],
    /// and the HTTP deadline is set five seconds past it so the broker's own
    /// timer fires first and a quiet queue reads as an empty poll rather than a
    /// timeout.
    pub fn poll_timeout(mut self, d: Duration) -> Self {
        self.poll_timeout = d;
        self
    }

    /// Commit at delivery. At-most-once, and no lease bookkeeping at all —
    /// there is nothing to ack afterwards.
    pub fn auto_ack(mut self, enabled: bool) -> Self {
        self.auto_ack = enabled;
        self
    }

    /// Send it.
    pub async fn send(self) -> Result<EphemeralBatch> {
        if self.queue.is_empty() {
            return Err(Error::Invalid("an ephemeral pop needs a queue name".into()));
        }

        let params = EphemeralPopParams {
            queue: self.queue.clone(),
            partition: self.partition.clone(),
            batch: self.batch,
            // `Some(true)` or nothing: `to_pairs` drops `Some(false)` anyway,
            // but making the absence explicit keeps the intent at the call site.
            wait: self.wait.then_some(true),
            timeout_millis: Some(self.poll_timeout.as_millis() as u64),
            group: self.group.clone(),
            auto_ack: self.auto_ack.then_some(true),
        };
        let url = format!(
            "/api/v1/ephemeral/pop?{}",
            crate::queue::encode_pairs(&params.to_pairs())
        );

        // Affinity so repeated pops of one queue land on one backend when the
        // client holds several URLs. The broker forwards to the rendezvous
        // owner either way, so this saves a hop, it does not create
        // correctness — and it is the same key the durable pop uses, so a
        // client speaking both families does not spread itself across the cell.
        let mut opts = Opts::affinity(queen_protocol::grouping_key(
            Some(&self.queue),
            self.partition.as_deref(),
            None,
            None,
            self.group.as_deref(),
        ));
        if self.wait {
            opts = opts
                .timeout(self.poll_timeout + Duration::from_secs(5))
                // A long poll that meets a 429 should back off and keep waiting
                // rather than give up after a handful of tries.
                .kind(RetryKind::Pop);
        }

        let resp: Option<EphemeralPopResponse> = self
            .inner
            .http
            .get_json(&url, &opts)
            .await
            .map_err(map_unsupported)?;

        // An empty pop is an empty ARRAY on this family, never a 204 — but a
        // bodiless answer still resolves to an empty batch rather than to an
        // error, because "no messages" and "the queue name came back" are the
        // same outcome to a caller.
        Ok(match resp {
            Some(r) => EphemeralBatch {
                queue: if r.queue.is_empty() {
                    self.queue
                } else {
                    r.queue
                },
                group: self.group,
                messages: r.messages,
            },
            None => EphemeralBatch {
                queue: self.queue,
                group: self.group,
                messages: Vec::new(),
            },
        })
    }
}

/// An ack under construction.
pub struct EphemeralAckBuilder {
    inner: Arc<Inner>,
    queue: String,
    group: Option<String>,
    acks: Vec<EphemeralAck>,
}

impl EphemeralAckBuilder {
    /// The group the pop used — cursors are per group.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Send it. Results are one per ack, in request order.
    ///
    /// [`queen_protocol::EphemeralOutcome::Stale`] is not an error and never
    /// arrives as one: it is the answer to an ack whose message belonged to a
    /// previous incarnation of the ring, which is how this class fences a
    /// restart or an ownership move without a lease protocol.
    pub async fn send(self) -> Result<Vec<EphemeralAckResult>> {
        if self.queue.is_empty() {
            return Err(Error::Invalid("an ephemeral ack needs a queue name".into()));
        }
        if self.acks.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(bad) = self.acks.iter().position(|a| a.id.is_empty()) {
            return Err(Error::Invalid(format!(
                "the ack at index {bad} carries no message id"
            )));
        }

        let expected = self.acks.len();
        let mut req = EphemeralAckRequest::new(self.queue, self.acks);
        req.group = self.group;

        let out: Option<EphemeralAckResponse> = self
            .inner
            .http
            .post_json("/api/v1/ephemeral/ack", &req, &Opts::default())
            .await
            .map_err(map_unsupported)?;
        let results = out.map(|r| r.results).unwrap_or_default();
        // The same guard the durable ack path carries: a short array silently
        // attributed to the wrong id is worse than a refused response.
        if results.len() != expected {
            return Err(Error::Decode(format!(
                "ephemeral ack returned {} results for {expected} acknowledgments",
                results.len()
            )));
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::Queen;

    fn eph() -> Ephemeral {
        Queen::connect(Config::new("http://127.0.0.1:1"))
            .unwrap()
            .ephemeral()
    }

    #[tokio::test]
    async fn an_empty_push_never_reaches_the_network() {
        let out = eph()
            .push_many("presence", Vec::<i32>::new())
            .send()
            .await
            .unwrap();
        assert_eq!(out.count, 0);
        assert!(!out.buffered);
    }

    #[tokio::test]
    async fn an_empty_ack_never_reaches_the_network() {
        assert!(eph()
            .ack_ids("inbox", Vec::new())
            .send()
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn an_ack_without_an_id_is_refused_locally() {
        let err = eph()
            .ack_ids("inbox", vec![EphemeralAck::new("")])
            .send()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("index 0"), "{err}");
    }

    #[tokio::test]
    async fn acking_an_empty_batch_is_a_no_op() {
        let batch = EphemeralBatch {
            queue: "inbox".into(),
            group: Some("workers".into()),
            messages: Vec::new(),
        };
        assert!(eph().ack(&batch).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_verb_without_a_queue_name_fails_at_the_call() {
        assert!(eph().push("", 1).send().await.is_err());
        assert!(eph().pop("").send().await.is_err());
    }

    fn http(status: u16, code: Option<&str>) -> Error {
        Error::Http {
            status,
            message: "x".into(),
            code: code.map(|c| queen_protocol::ErrorCode::Other(c.into())),
            retry_after_seconds: None,
        }
    }

    #[test]
    fn the_unsupported_predicate_reads_the_code_and_not_only_the_status() {
        assert!(is_unsupported(&http(404, None)));
        assert!(is_unsupported(&http(404, Some("route_blocked"))));
        assert!(!is_unsupported(&http(429, None)));
        assert!(!is_unsupported(&Error::Invalid("x".into())));

        // The one 404 that is about a queue, not about the routes.
        let missing = http(404, Some(queen_protocol::EPHEMERAL_QUEUE_NOT_FOUND_CODE));
        assert!(is_queue_not_found(&missing));
        assert!(
            !is_unsupported(&missing),
            "a missing queue must not read as an out-of-date broker"
        );
    }

    #[test]
    fn a_missing_queue_keeps_its_own_message() {
        let missing = http(404, Some(queen_protocol::EPHEMERAL_QUEUE_NOT_FOUND_CODE));
        let mapped = map_unsupported(missing);
        assert!(is_queue_not_found(&mapped));
        assert!(
            !mapped.to_string().contains(EPHEMERAL_UNSUPPORTED),
            "the broker's own explanation must survive: {mapped}"
        );
    }

    #[test]
    fn mapping_a_404_keeps_the_code_and_replaces_the_prose() {
        let original = Error::Http {
            status: 404,
            message: "route not available".into(),
            code: Some(queen_protocol::ErrorCode::Other("route_blocked".into())),
            retry_after_seconds: None,
        };
        let mapped = map_unsupported(original);
        assert_eq!(
            mapped.to_string(),
            format!("HTTP 404 (route_blocked): {EPHEMERAL_UNSUPPORTED}")
        );
        assert_eq!(mapped.code().map(|c| c.as_str()), Some("route_blocked"));
    }

    #[test]
    fn mapping_leaves_every_other_error_alone() {
        let e = Error::Http {
            status: 429,
            message: "the ephemeral queue is at its maxBytes".into(),
            code: Some(queen_protocol::ErrorCode::Other("queue_full".into())),
            retry_after_seconds: Some(1.0),
        };
        let mapped = map_unsupported(e);
        assert_eq!(mapped.status(), Some(429));
        assert!(mapped.to_string().contains("maxBytes"));
    }

    #[test]
    fn a_batch_reports_its_own_shape() {
        let batch = EphemeralBatch {
            queue: "inbox".into(),
            group: None,
            messages: vec![EphemeralDelivered {
                id: "e:1:Default:1".into(),
                partition: "Default".into(),
                payload: serde_json::json!(1),
                attempts: 1,
            }],
        };
        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.ids(), vec!["e:1:Default:1"]);
    }
}
