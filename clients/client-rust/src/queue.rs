//! The queue builder: configure, push, pop, consume, DLQ.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use serde::Serialize;

use queen_protocol::{
    ConfigureRequest, DlqParams, DlqResponse, Message, PopParams, PopResponse, PushItem,
    PushRequest, PushResult, QueueOptions, SubscriptionMode, DEFAULT_PARTITION,
};

use crate::buffer::{self, BufferOptions};
use crate::config::RetryKind;
use crate::error::{Error, Result};
use crate::http::Opts;
use crate::inner::Inner;
use crate::uuid;

/// Fluent entry point for everything scoped to a queue.
///
/// Created by [`crate::Queen::queue`]. Configuration methods take `self` and
/// return it, so a call chain reads as one statement.
#[derive(Clone)]
pub struct QueueBuilder {
    pub(crate) inner: Arc<Inner>,
    pub(crate) queue: Option<String>,
    pub(crate) partition: String,
    pub(crate) namespace: Option<String>,
    pub(crate) task: Option<String>,
    pub(crate) group: Option<String>,

    pub(crate) concurrency: usize,
    pub(crate) batch: i32,
    pub(crate) max_partitions: i32,
    pub(crate) limit: Option<u64>,
    pub(crate) idle: Option<Duration>,
    pub(crate) auto_ack: bool,
    pub(crate) wait: bool,
    pub(crate) poll_timeout: Duration,
    pub(crate) renew_lease: Option<Duration>,
    pub(crate) lease_seconds: Option<i32>,
    pub(crate) subscription_mode: Option<SubscriptionMode>,
    pub(crate) subscription_from: Option<String>,
    pub(crate) conflation: bool,
    pub(crate) cancel: Option<crate::consumer::Cancel>,

    pub(crate) buffer: Option<BufferOptions>,
}

impl QueueBuilder {
    pub(crate) fn new(inner: Arc<Inner>, queue: Option<String>) -> Self {
        Self {
            inner,
            queue,
            partition: DEFAULT_PARTITION.to_string(),
            namespace: None,
            task: None,
            group: None,
            concurrency: 1,
            batch: 1,
            max_partitions: 1,
            limit: None,
            idle: None,
            // Client-side auto-ack during consume; NOT the server-side
            // `autoAck` query parameter, which pop() controls separately.
            auto_ack: true,
            wait: true,
            poll_timeout: Duration::from_secs(30),
            renew_lease: None,
            lease_seconds: None,
            subscription_mode: None,
            subscription_from: None,
            conflation: false,
            cancel: None,
            buffer: None,
        }
    }

    /// Hand the consumer a shutdown signal. Without one, `consume` runs until
    /// its limit or idle timeout, or forever if neither is set.
    pub fn cancel(mut self, token: crate::consumer::Cancel) -> Self {
        self.cancel = Some(token);
        self
    }

    /// The queue name, if this builder has one.
    pub fn name(&self) -> Option<&str> {
        self.queue.as_deref()
    }

    // ------------------------------------------------------------ addressing

    /// The ordered lane within the queue. Defaults to `Default`.
    pub fn partition(mut self, partition: impl Into<String>) -> Self {
        self.partition = partition.into();
        self
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn task(mut self, task: impl Into<String>) -> Self {
        self.task = Some(task.into());
        self
    }

    /// Consume as part of a consumer group, which keeps its own cursor per
    /// partition. Without one, consumption is in queue mode.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    // ------------------------------------------------------------- consuming

    /// Parallel workers, each with its own poll loop. Ordering is preserved
    /// *within* a partition regardless, because a partition is claimed by one
    /// worker at a time.
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    /// Messages per poll.
    pub fn batch(mut self, n: i32) -> Self {
        self.batch = n.max(1);
        self
    }

    /// Claim up to `n` partitions per poll, sharing the batch budget and one
    /// lease. The way to drain many sparse lanes without a round-trip each.
    pub fn partitions(mut self, n: i32) -> Self {
        self.max_partitions = n.max(1);
        self
    }

    /// Stop after this many messages.
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    /// Stop after this long without a message.
    pub fn idle(mut self, d: Duration) -> Self {
        self.idle = Some(d);
        self
    }

    /// Ack on handler success and nack on handler error, automatically.
    /// On by default.
    pub fn auto_ack(mut self, enabled: bool) -> Self {
        self.auto_ack = enabled;
        self
    }

    /// Long-poll rather than returning empty. On by default.
    pub fn wait(mut self, enabled: bool) -> Self {
        self.wait = enabled;
        self
    }

    /// Long-poll window. The HTTP timeout is this plus five seconds of slack,
    /// so the broker's timer always fires first and the client sees an empty
    /// poll rather than a transport timeout.
    pub fn poll_timeout(mut self, d: Duration) -> Self {
        self.poll_timeout = d;
        self
    }

    /// Extend the lease every `interval` while a handler runs. For handlers
    /// that can outlive the queue's `leaseTime` — without it the claim expires
    /// mid-flight and the message is redelivered to somebody else.
    pub fn renew_lease(mut self, interval: Duration) -> Self {
        self.renew_lease = Some(interval);
        self
    }

    /// Per-request lease length, overriding the queue's `leaseTime`.
    pub fn lease_seconds(mut self, seconds: i32) -> Self {
        self.lease_seconds = Some(seconds);
        self
    }

    /// Where a *new* group cursor starts. Ignored for a cursor that already
    /// exists — an existing group is never re-seeded, so this cannot be used to
    /// replay. Use [`crate::Admin::seek_consumer_group`] for that.
    pub fn subscription_mode(mut self, mode: SubscriptionMode) -> Self {
        self.subscription_mode = Some(mode);
        self
    }

    /// `now`, or an ISO-8601 timestamp.
    pub fn subscription_from(mut self, from: impl Into<String>) -> Self {
        self.subscription_from = Some(from.into());
        self
    }

    /// Last-value delivery: a pop of a partition delivers only the **newest**
    /// message in it, and the ack retires everything behind that message in one
    /// step. For command-style queues where one partition is one logical task
    /// key and only the latest "recompute entity X" matters — under a backlog
    /// the handler runs once per partition instead of once per message.
    ///
    /// Nothing on disk is touched: retention still governs storage, and a second
    /// group reading the same queue without this flag still sees every message.
    ///
    /// # It is a property of the group, not of the call
    ///
    /// The value is persisted the first time the group registers on the queue,
    /// and from then on the **stored** value is what the broker applies to every
    /// consumer of that group. A later consumer declaring the opposite does not
    /// flip it; it keeps working under the stored policy and this client logs one
    /// warning per (queue, group). Requires a [`QueueBuilder::group`], and the
    /// broker refuses it together with [`QueueBuilder::pop_auto_ack`] — auto-ack
    /// commits at delivery with no lease, which would turn the guarantee below
    /// into at-most-once on the one feature that exists to provide it.
    ///
    /// # The guarantee
    ///
    /// After the last push to a partition, at least one run of that partition's
    /// handler *starts* after that push committed. The broker never commits past
    /// an offset it did not observe when it took the claim, so a message that
    /// arrives mid-handler leaves the partition pending and is picked up by the
    /// next pop.
    ///
    /// # Requires broker >= 1.1.0
    ///
    /// An older broker drops the unknown parameter silently and answers with the
    /// whole backlog. Rather than let that pass as "conflation is on", every pop
    /// checks the broker's echo and fails with an error naming the version — see
    /// [`QueueBuilder::pop`].
    pub fn conflation(mut self, enabled: bool) -> Self {
        self.conflation = enabled;
        self
    }

    // -------------------------------------------------------------- pushing

    /// Batch pushes client-side instead of sending each one.
    pub fn buffer(mut self, opts: BufferOptions) -> Self {
        self.buffer = Some(opts);
        self
    }

    fn require_queue(&self) -> Result<&str> {
        self.queue
            .as_deref()
            .ok_or_else(|| Error::Invalid("a queue name is required for this operation".into()))
    }

    /// Push one payload.
    ///
    /// A transaction id is minted client-side so a retry after a timeout is
    /// idempotent within the queue's dedup window. Use [`QueueBuilder::push_items`]
    /// to supply your own.
    pub async fn push<T: Serialize>(&self, payload: T) -> Result<Vec<PushResult>> {
        self.push_many(std::iter::once(payload)).await
    }

    /// Push several payloads to the same queue and partition.
    pub async fn push_many<T, I>(&self, payloads: I) -> Result<Vec<PushResult>>
    where
        T: Serialize,
        I: IntoIterator<Item = T>,
    {
        let queue = self.require_queue()?.to_string();
        let items: Result<Vec<PushItem>> = payloads
            .into_iter()
            .map(|p| {
                Ok(PushItem {
                    queue: queue.clone(),
                    partition: Some(self.partition.clone()),
                    payload: serde_json::to_value(p)?,
                    transaction_id: Some(uuid::uuidv7()),
                })
            })
            .collect();
        self.push_items(items?).await
    }

    /// Push fully-formed items, for when the transaction id matters.
    ///
    /// Note there is no `traceId`: the broker's push path cannot store one.
    /// Push inside a transaction if you need a trace id to survive.
    pub async fn push_items(&self, items: Vec<PushItem>) -> Result<Vec<PushResult>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        if let Some(opts) = self.buffer {
            let queue = self.require_queue()?;
            for item in items {
                let addr = buffer::address(queue, &self.partition);
                // Awaited: `add` is where the buffer's max_size backpressure is
                // applied, so a buffered push resolves only once the message is
                // actually IN the buffer. It returns an error instead of
                // parking forever if the client is closing, and a caller that
                // cannot afford to wait wraps this in `tokio::time::timeout`.
                self.inner.buffers.add(addr, item, opts).await?;
            }
            // Buffered pushes have no per-item verdict yet — it arrives with the
            // flush. An empty result here means "accepted for batching", not
            // "nothing happened".
            return Ok(Vec::new());
        }

        let req = PushRequest::new(items);
        let results: Option<Vec<PushResult>> = self
            .inner
            .http
            .post_json("/api/v1/push", &req, &Opts::affinity(self.affinity_key()))
            .await?;
        Ok(results.unwrap_or_default())
    }

    /// Send anything this queue and partition have buffered.
    pub async fn flush_buffer(&self) -> Result<Vec<PushResult>> {
        let queue = self.require_queue()?;
        let addr = buffer::address(queue, &self.partition);
        self.inner.buffers.flush(&addr).await
    }

    // --------------------------------------------------------------- popping

    pub(crate) fn affinity_key(&self) -> Option<String> {
        queen_protocol::grouping_key(
            self.queue.as_deref(),
            // A pop that does not name a partition may be served any of them,
            // so `*` is the right key — matching the broker's own grouping.
            if self.partition == DEFAULT_PARTITION {
                None
            } else {
                Some(&self.partition)
            },
            self.namespace.as_deref(),
            self.task.as_deref(),
            self.group.as_deref(),
        )
    }

    pub(crate) fn pop_path(&self) -> Result<String> {
        if let Some(q) = &self.queue {
            if self.partition != DEFAULT_PARTITION {
                return Ok(format!(
                    "/api/v1/pop/queue/{}/partition/{}",
                    urlencode(q),
                    urlencode(&self.partition)
                ));
            }
            return Ok(format!("/api/v1/pop/queue/{}", urlencode(q)));
        }
        if self.namespace.is_some() || self.task.is_some() {
            return Ok("/api/v1/pop".to_string());
        }
        Err(Error::Invalid(
            "pop needs a queue, or a namespace/task to discover one".into(),
        ))
    }

    pub(crate) fn pop_params(&self, auto_ack: bool) -> PopParams {
        PopParams {
            batch: Some(self.batch),
            partitions: Some(self.max_partitions),
            auto_ack: Some(auto_ack),
            wait: Some(self.wait),
            timeout_millis: Some(self.poll_timeout.as_millis() as u64),
            lease_seconds: self.lease_seconds,
            consumer_group: self.group.clone(),
            subscription_mode: self.subscription_mode,
            subscription_from: self.subscription_from.clone(),
            // `Some(true)` or nothing. `PopParams::to_pairs` drops `Some(false)`
            // anyway, but making the absence explicit here keeps the intent at
            // the call site: not asking is not the same as asking for `false`,
            // which the broker reads as a disagreement with the stored policy.
            conflation: self.conflation.then_some(true),
            namespace: self.namespace.clone(),
            task: self.task.clone(),
        }
    }

    /// The pair a conflation policy is stored under, and therefore the unit a
    /// declaration conflict is warned about — once per process, §4. Not the
    /// partition: every lane of a queue shares one stored policy, so warning per
    /// lane would multiply one operator mistake by the partition count.
    fn conflation_scope(&self) -> String {
        let addressed = match &self.queue {
            Some(q) => q.clone(),
            // A discovery pop has no queue in its path; what it addresses is the
            // namespace/task pair.
            None => format!(
                "{}/{}",
                self.namespace.as_deref().unwrap_or("*"),
                self.task.as_deref().unwrap_or("*")
            ),
        };
        format!(
            "{addressed}\u{1}{}",
            self.group
                .as_deref()
                .unwrap_or(queen_protocol::QUEUE_MODE_GROUP)
        )
    }

    /// The degrade-loudly contract, PLAN_CONFLATION §4.
    ///
    /// No SDK negotiates versions with the broker, and query parameters an older
    /// broker does not know are dropped without a word — so a 1.1.0 client
    /// against a 1.0.6 broker would ask for last-value delivery, be handed the
    /// entire backlog, and process every message of it believing it had skipped
    /// them. The response echo is the only evidence, and a conflating broker
    /// emits it on empty pops too (answering 200 with a body where it would
    /// otherwise answer a bodiless 204), which is what lets this fire on the
    /// FIRST round trip, before a single message is handled.
    ///
    /// `None` is that bodiless 204: no body, no echo, old broker.
    ///
    /// # What is *not* a version skew
    ///
    /// A group registered before anyone wanted conflation stores `false`, and
    /// §3.3 gives the stored value the last word — so this consumer is answered
    /// "no": `conflationConflict` with no `conflation` echo. The missing echo
    /// looks exactly like an old broker's, and treating it as one would blame the
    /// broker's version for an operator's group policy and stop the half of a
    /// rolling deploy that is already correct. The conflict key is the
    /// discriminator: no broker older than 1.1.0 can emit it. So the error is
    /// raised only when the response says **nothing at all** about conflation.
    fn check_conflation(&self, resp: Option<&PopResponse>) -> Result<()> {
        if !self.conflation {
            return Ok(());
        }
        let applied = resp.is_some_and(|r| r.conflation_applied());
        let conflict = resp.is_some_and(|r| r.has_conflation_conflict());

        if !applied && !conflict {
            return Err(Error::Invalid(CONFLATION_UNSUPPORTED.to_string()));
        }
        // §3.3: the stored policy wins and this consumer keeps working — a
        // conflict is a rolling-deploy artefact, not an outage. But it is also
        // an operator mistake that is invisible otherwise, so it gets said out
        // loud exactly once: a mismatched fleet polls several times a second,
        // and a line per poll is a line that gets filtered out.
        if conflict && first_conflation_conflict(&self.conflation_scope()) {
            tracing::warn!(
                queue = self.queue.as_deref().unwrap_or("<discovery>"),
                group = self
                    .group
                    .as_deref()
                    .unwrap_or(queen_protocol::QUEUE_MODE_GROUP),
                applied,
                "this consumer declared a conflation policy the group does not have; the \
                 group's stored policy is what the broker applied"
            );
        }
        Ok(())
    }

    /// Claim messages once.
    ///
    /// Unlike the JS and Python clients, a failed pop returns `Err` rather than
    /// an empty vector: silently turning a 403 or an exhausted retry budget
    /// into "no messages" hides an outage as an idle queue. An *empty* claim,
    /// and a claim refused because pop maintenance is on, both return `Ok`
    /// with no messages.
    ///
    /// `auto_ack` here is the broker-side flag: the cursor commits at delivery
    /// and no lease is taken, so a crash mid-handler loses the batch. Defaults
    /// to off, matching every other SDK's `pop()`.
    ///
    /// # With [`QueueBuilder::conflation`] on
    ///
    /// The pop returns the newest message of each claimed partition, and errors
    /// if the broker did not say it applied conflation — including on an empty
    /// poll, which is what makes the check fire before any message is handled
    /// rather than after a backlog has been drained. A broker older than 1.1.0
    /// drops the parameter silently, so its absence in the response is the only
    /// evidence there is.
    pub async fn pop(&self) -> Result<Vec<Message>> {
        self.pop_with_auto_ack(false).await
    }

    /// Claim messages and have the broker commit the cursor immediately.
    pub async fn pop_auto_ack(&self) -> Result<Vec<Message>> {
        self.pop_with_auto_ack(true).await
    }

    async fn pop_with_auto_ack(&self, auto_ack: bool) -> Result<Vec<Message>> {
        let path = self.pop_path()?;
        let params = self.pop_params(auto_ack);
        let url = format!("{path}?{}", encode_pairs(&params.to_pairs()));

        let mut opts = Opts::affinity(self.affinity_key());
        if self.wait {
            // Five seconds of slack so the broker's long-poll timer fires
            // before the client's, and a quiet queue reads as an empty poll
            // rather than a timeout.
            opts = opts
                .timeout(self.poll_timeout + Duration::from_secs(5))
                .kind(RetryKind::Pop);
        } else {
            opts = opts.timeout(self.poll_timeout);
        }

        let resp: Option<PopResponse> = self.inner.http.get_json(&url, &opts).await?;
        let Some(resp) = resp else {
            // A bodiless 204 — the shape a pre-1.1.0 broker gives an empty pop.
            // A conflating broker never sends it, so for a consumer that asked
            // for conflation this is the version check firing on the first
            // round trip rather than an ordinary quiet poll.
            self.check_conflation(None)?;
            return Ok(Vec::new());
        };
        if resp.is_paused() {
            // Pop maintenance is not a version skew: the request never reached
            // the claim path, so there is no echo to expect and nothing to
            // conclude from its absence.
            tracing::debug!("broker is in pop maintenance; treating as an empty poll");
            return Ok(Vec::new());
        }
        if !resp.success {
            return Err(Error::Http {
                status: 200,
                message: resp
                    .error
                    .unwrap_or_else(|| "pop failed without a reason".into()),
                code: None,
                retry_after_seconds: None,
            });
        }
        // Before the messages are handed over, never after: the whole point is
        // that no message from a backlog the caller asked to skip is ever
        // processed. §4.
        self.check_conflation(Some(&resp))?;
        Ok(resp.messages)
    }

    // ------------------------------------------------------------- lifecycle

    /// Create or reconfigure the queue.
    pub async fn configure(&self, options: QueueOptions) -> Result<serde_json::Value> {
        let queue = self.require_queue()?;
        let mut req = ConfigureRequest::new(queue).options(options);
        req.namespace = self.namespace.clone();
        req.task = self.task.clone();

        let out: Option<serde_json::Value> = self
            .inner
            .http
            .post_json("/api/v1/configure", &req, &Opts::default())
            .await?;
        let out = out.unwrap_or(serde_json::Value::Null);
        // configure_queue_v1 reports failure inside a 200 body.
        if let Some(err) = out.get("error").and_then(|e| e.as_str()) {
            return Err(Error::Invalid(format!("configure failed: {err}")));
        }
        Ok(out)
    }

    /// Create the queue with default options.
    pub async fn create(&self) -> Result<serde_json::Value> {
        self.configure(QueueOptions::default()).await
    }

    /// Drop the queue and everything in it.
    pub async fn delete(&self) -> Result<serde_json::Value> {
        let queue = self.require_queue()?;
        let path = format!("/api/v1/resources/queues/{}", urlencode(queue));
        let out: Option<serde_json::Value> =
            self.inner.http.delete_json(&path, &Opts::default()).await?;
        Ok(out.unwrap_or(serde_json::Value::Null))
    }

    /// Read the dead-letter queue.
    ///
    /// Only `queue`, `consumerGroup`, `limit` and `offset` filter anything —
    /// the broker ignores every other query key, so this builder does not offer
    /// options that would quietly do nothing.
    pub async fn dlq(&self, limit: Option<i32>, offset: Option<i32>) -> Result<DlqResponse> {
        let queue = self.require_queue()?;
        let params = DlqParams {
            queue: Some(queue.to_string()),
            consumer_group: self.group.clone(),
            limit,
            offset,
        };
        let path = format!("/api/v1/dlq?{}", encode_pairs(&params.to_pairs()));
        let out: Option<DlqResponse> = self.inner.http.get_json(&path, &Opts::default()).await?;
        Ok(out.unwrap_or_default())
    }
}

/// PLAN_CONFLATION §4, word for word. It has to name the broker version: the
/// symptom a consumer sees is "conflation does nothing", and the cause is never
/// in their code.
pub(crate) const CONFLATION_UNSUPPORTED: &str =
    "conflation was requested but this broker did not apply it — requires broker >= 1.1.0";

/// True the first time this (queue, group) scope reports a declaration conflict,
/// false for ever after. Process-global on purpose: the point is one line per
/// mistake per process, not one per builder — and a consumer typically rebuilds
/// its `QueueBuilder` on every poll.
fn first_conflation_conflict(scope: &str) -> bool {
    static SEEN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    SEEN.get_or_init(|| Mutex::new(HashSet::new()))
        // A poisoned lock here means some other thread panicked while holding a
        // set of strings. Losing the warning history is not worth turning a
        // warning path into a panic on the consume loop.
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(scope.to_string())
}

pub(crate) fn encode_pairs(pairs: &[(&'static str, String)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={}", urlencode(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Percent-encode everything outside the unreserved set. Deliberately strict:
/// queue and partition names are user data and routinely contain `/`, `:` and
/// spaces, all of which would otherwise change the path.
pub(crate) fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::Queen;

    fn q(name: &str) -> QueueBuilder {
        Queen::connect(Config::new("http://127.0.0.1:1"))
            .unwrap()
            .queue(name)
    }

    #[test]
    fn urlencode_escapes_path_breaking_characters() {
        assert_eq!(urlencode("plain"), "plain");
        assert_eq!(urlencode("with space"), "with%20space");
        assert_eq!(urlencode("a/b"), "a%2Fb");
        assert_eq!(urlencode("a:b"), "a%3Ab");
        assert_eq!(urlencode("ünïcode"), "%C3%BCn%C3%AFcode");
        assert_eq!(urlencode("keep-._~"), "keep-._~");
    }

    #[test]
    fn pop_path_follows_the_addressing_mode() {
        assert_eq!(q("orders").pop_path().unwrap(), "/api/v1/pop/queue/orders");
        assert_eq!(
            q("orders").partition("eu").pop_path().unwrap(),
            "/api/v1/pop/queue/orders/partition/eu"
        );
        // Default is the absence of a partition, not a partition named Default.
        assert_eq!(
            q("orders").partition("Default").pop_path().unwrap(),
            "/api/v1/pop/queue/orders"
        );
    }

    #[test]
    fn pop_path_encodes_names() {
        let b = q("or/ders").partition("eu west");
        assert_eq!(
            b.pop_path().unwrap(),
            "/api/v1/pop/queue/or%2Fders/partition/eu%20west"
        );
    }

    #[test]
    fn discovery_pop_uses_the_bare_route() {
        let inner = Queen::connect(Config::new("http://127.0.0.1:1")).unwrap();
        let b = inner.queue_opt(None).namespace("billing").task("invoice");
        assert_eq!(b.pop_path().unwrap(), "/api/v1/pop");
    }

    #[test]
    fn a_pop_with_no_addressing_is_rejected() {
        let inner = Queen::connect(Config::new("http://127.0.0.1:1")).unwrap();
        assert!(inner.queue_opt(None).pop_path().is_err());
    }

    #[test]
    fn affinity_key_matches_the_brokers_grouping() {
        assert_eq!(
            q("orders").group("workers").affinity_key().unwrap(),
            "orders:*:workers"
        );
        assert_eq!(
            q("orders").partition("eu").affinity_key().unwrap(),
            "orders:eu:__QUEUE_MODE__"
        );
    }

    #[test]
    fn pop_params_carry_the_defaults_the_sdks_send() {
        let p = q("orders").batch(10).pop_params(false);
        let pairs = p.to_pairs();
        assert!(pairs.contains(&("batch", "10".to_string())));
        assert!(pairs.contains(&("wait", "true".to_string())));
        assert!(pairs.contains(&("timeout", "30000".to_string())));
        // autoAck=false is never sent, and partitions=1 is the absence of the key
        assert!(!pairs.iter().any(|(k, _)| *k == "autoAck"));
        assert!(!pairs.iter().any(|(k, _)| *k == "partitions"));
    }

    #[test]
    fn pop_params_send_multi_partition_and_lease_override() {
        let p = q("orders")
            .partitions(8)
            .lease_seconds(120)
            .pop_params(true);
        let pairs = p.to_pairs();
        assert!(pairs.contains(&("partitions", "8".to_string())));
        assert!(pairs.contains(&("leaseSeconds", "120".to_string())));
        assert!(pairs.contains(&("autoAck", "true".to_string())));
    }

    #[test]
    fn encode_pairs_percent_encodes_values() {
        let pairs = vec![
            ("consumerGroup", "a group".to_string()),
            ("batch", "1".into()),
        ];
        assert_eq!(encode_pairs(&pairs), "consumerGroup=a%20group&batch=1");
    }

    #[tokio::test]
    async fn operations_needing_a_queue_name_say_so() {
        let inner = Queen::connect(Config::new("http://127.0.0.1:1")).unwrap();
        let b = inner.queue_opt(None);
        assert!(b.push(serde_json::json!(1)).await.is_err());
        assert!(b.delete().await.is_err());
        assert!(b.dlq(None, None).await.is_err());
    }

    #[tokio::test]
    async fn pushing_nothing_is_a_no_op() {
        assert!(q("orders").push_items(vec![]).await.unwrap().is_empty());
    }

    // `wait` is sent even when false. The broker long-polls by default, so
    // omitting the key would silently turn a non-blocking poll into a 30-second
    // one — the exact opposite of what the caller asked for.
    #[test]
    fn a_non_blocking_pop_says_so_explicitly() {
        let pairs = q("orders")
            .wait(false)
            .poll_timeout(Duration::from_millis(1500))
            .pop_params(false)
            .to_pairs();
        assert!(
            pairs.contains(&("wait", "false".to_string())),
            "wait=false was dropped from the query: {pairs:?}"
        );
        assert!(
            pairs.contains(&("timeout", "1500".to_string())),
            "the poll timeout travels as `timeout`, in milliseconds: {pairs:?}"
        );
    }

    #[test]
    fn pop_params_carry_the_group_and_the_subscription_seed() {
        let pairs = q("orders")
            .group("workers")
            .subscription_mode(SubscriptionMode::New)
            .subscription_from("2026-08-04T10:00:00Z")
            .pop_params(false)
            .to_pairs();
        assert!(
            pairs.contains(&("consumerGroup", "workers".to_string())),
            "{pairs:?}"
        );
        assert!(
            pairs.contains(&("subscriptionMode", "new".to_string())),
            "{pairs:?}"
        );
        assert!(
            pairs.contains(&("subscriptionFrom", "2026-08-04T10:00:00Z".to_string())),
            "{pairs:?}"
        );
    }

    // A discovery pop has no queue in its path, so namespace and task are the
    // only thing telling the broker what to serve. Dropping them from the query
    // would turn the call into "any queue at all".
    #[test]
    fn a_discovery_pop_carries_its_addressing_in_the_query() {
        let queen = Queen::connect(Config::new("http://127.0.0.1:1")).unwrap();
        let b = queen.queue_opt(None).namespace("billing").task("invoice");
        assert_eq!(b.pop_path().unwrap(), "/api/v1/pop");

        let pairs = b.pop_params(false).to_pairs();
        assert!(
            pairs.contains(&("namespace", "billing".to_string())),
            "{pairs:?}"
        );
        assert!(
            pairs.contains(&("task", "invoice".to_string())),
            "{pairs:?}"
        );
    }

    // Zero means "the smallest useful amount", not "none": batch=0 would let
    // the broker apply its own default of 200, partitions=0 is not a claim at
    // all, and concurrency=0 would spawn no workers and make consume return
    // instantly having done nothing.
    #[test]
    fn zero_and_negative_knobs_clamp_to_one() {
        let b = q("orders").batch(0).partitions(-5).concurrency(0);
        assert_eq!(b.batch, 1);
        assert_eq!(b.max_partitions, 1);
        assert_eq!(b.concurrency, 1);

        let pairs = b.pop_params(false).to_pairs();
        assert!(pairs.contains(&("batch", "1".to_string())), "{pairs:?}");
        assert!(
            !pairs.iter().any(|(k, _)| *k == "partitions"),
            "partitions=1 is the absence of the key, not partitions=1: {pairs:?}"
        );
    }

    #[test]
    fn the_affinity_key_covers_discovery_and_gives_up_when_there_is_nothing_to_pin() {
        let queen = Queen::connect(Config::new("http://127.0.0.1:1")).unwrap();
        assert_eq!(
            queen
                .queue_opt(None)
                .namespace("billing")
                .task("invoice")
                .group("g")
                .affinity_key()
                .unwrap(),
            "billing:invoice:g"
        );
        assert_eq!(
            queen
                .queue_opt(None)
                .task("invoice")
                .affinity_key()
                .unwrap(),
            "*:invoice:__QUEUE_MODE__"
        );
        // Nothing to hash on, so the request spreads rather than pinning to a
        // backend chosen from a key that describes nothing.
        assert!(queen.queue_opt(None).group("g").affinity_key().is_none());
        assert_eq!(
            q("orders").affinity_key().unwrap(),
            "orders:*:__QUEUE_MODE__"
        );
    }

    // A buffered push answers before the broker has seen anything, so its empty
    // result means "accepted for batching" and not "nothing was pushed". Anyone
    // treating `Ok(vec![])` as a failed push would be wrong exactly here.
    #[tokio::test]
    async fn a_buffered_push_is_accepted_without_a_verdict() {
        let queen = Queen::connect(Config::new("http://127.0.0.1:1")).unwrap();
        let opts = BufferOptions {
            message_count: 100,
            time: Duration::from_secs(60),
            ..Default::default()
        };
        let out = queen
            .queue("orders")
            .partition("eu")
            .buffer(opts)
            .push_many([serde_json::json!(1), serde_json::json!(2)])
            .await
            .expect("a buffered push does not touch the network, so it cannot fail here");
        assert!(
            out.is_empty(),
            "a buffered push has no per-item verdict yet"
        );

        let stats = queen.buffer_stats();
        assert_eq!(stats.total_buffered_messages, 2);
        assert_eq!(
            stats.active_buffers, 1,
            "one buffer per (queue, partition), not one per push"
        );
    }

    // ----------------------------------------------- conflation (§1.1, §4)

    fn pop_response(body: &str) -> PopResponse {
        serde_json::from_str(body).expect("fixture must decode as a pop response")
    }

    // Rust is the one SDK where `pop()` and `consume()` share a parameter
    // builder — every other one has two copies and a standing comment about the
    // bug that produced them. `pop_params` is called with `auto_ack` true from
    // `pop_auto_ack` and false from both `pop()` and the consume loop, so both
    // arms are asserted rather than assumed.
    #[test]
    fn conflation_reaches_the_query_from_both_pop_param_paths() {
        for auto_ack in [false, true] {
            let pairs = q("orders")
                .group("workers")
                .conflation(true)
                .pop_params(auto_ack)
                .to_pairs();
            assert!(
                pairs.contains(&("conflation", "true".to_string())),
                "auto_ack={auto_ack}: {pairs:?}"
            );
        }
    }

    // Default off, and an explicit `false` is the absence of the key — never
    // `conflation=false`. The broker reads a sent `false` against a group whose
    // stored policy is `true` as a DISAGREEMENT (§3.3) and charges a conflict
    // counter for it, so a consumer that never opted in must stay silent.
    #[test]
    fn conflation_is_off_by_default_and_off_is_silence() {
        for b in [
            q("orders").group("workers"),
            q("orders").group("workers").conflation(false),
        ] {
            let pairs = b.pop_params(false).to_pairs();
            assert!(!pairs.iter().any(|(k, _)| *k == "conflation"), "{pairs:?}");
        }
    }

    // §4, the degrade-loudly contract. No SDK negotiates versions, and a broker
    // older than 1.1.0 drops the unknown query parameter without a word — so the
    // response echo is the only evidence conflation happened. Missing echo =
    // error, on the first response, before a message is processed.
    #[test]
    fn a_response_without_the_echo_is_an_error_when_conflation_was_asked_for() {
        let asked = q("orders").group("workers").conflation(true);

        let old_broker = pop_response(r#"{"messages":[],"partitionsClaimed":0}"#);
        let err = asked
            .check_conflation(Some(&old_broker))
            .expect_err("a pop with no echo came from a broker that ignored the flag");
        assert_eq!(err.to_string(), CONFLATION_UNSUPPORTED);

        // A bodiless 204 is the same verdict: a conflating broker answers an
        // empty pop with 200 and a body precisely so this fires on the first
        // round trip rather than whenever traffic next arrives.
        assert!(asked.check_conflation(None).is_err());

        let applied = pop_response(r#"{"messages":[],"conflation":true}"#);
        assert!(asked.check_conflation(Some(&applied)).is_ok());
    }

    // The trap inside the degrade check. A group registered earlier WITHOUT
    // conflation stores `false`, and §3.3 says the stored value wins — so a
    // consumer that asks for it is answered "no": `conflationConflict:true` and
    // no `conflation` echo, which is byte-for-byte what an old broker's answer
    // also lacks. Reading that as a version skew would blame the broker for an
    // operator's group policy and take down the half of a rolling deploy that is
    // already correct — exactly what §7.3's E2E-4 forbids ("both consumers keep
    // working"). The conflict key is the discriminator: no broker older than
    // 1.1.0 can emit it.
    #[test]
    fn a_group_whose_stored_policy_is_off_is_a_conflict_not_an_old_broker() {
        let refused = pop_response(r#"{"messages":[],"conflationConflict":true}"#);
        let asked = q("conflict-queue-c").group("g1").conflation(true);

        assert!(
            asked.check_conflation(Some(&refused)).is_ok(),
            "the group stores conflation=false; that is a declaration conflict, not a \
             broker too old to understand the request"
        );
        assert!(
            !first_conflation_conflict(&asked.conflation_scope()),
            "the refusal must still be said out loud, once"
        );
    }

    // The mirror case is a non-event: a consumer that never asked for conflation
    // must not be broken by a group whose stored policy happens to be off — or on.
    #[test]
    fn a_consumer_that_did_not_ask_for_conflation_is_never_failed_by_the_echo() {
        let plain = q("orders").group("workers");
        assert!(plain.check_conflation(None).is_ok());
        assert!(plain
            .check_conflation(Some(&pop_response(r#"{"messages":[]}"#)))
            .is_ok());
        assert!(plain
            .check_conflation(Some(&pop_response(r#"{"messages":[],"conflation":true}"#)))
            .is_ok());
    }

    // §3.3/§4: the stored policy wins, the consumer keeps working, and it says so
    // ONCE per (queue, group) per process. A mismatched fleet polls several times
    // a second; a warning per poll is a log flood that gets the line filtered out,
    // which is the same as not warning at all.
    #[test]
    fn a_declaration_conflict_warns_once_per_queue_and_group() {
        let conflicted =
            pop_response(r#"{"messages":[],"conflation":true,"conflationConflict":true}"#);

        // Names unique to this test: the registry is process-global on purpose,
        // and a shared name would couple this test to whatever ran before it.
        let a = q("conflict-queue-a").group("g1").conflation(true);
        assert!(
            a.check_conflation(Some(&conflicted)).is_ok(),
            "a conflict is not an error"
        );

        assert!(
            !first_conflation_conflict(&a.conflation_scope()),
            "the pop already claimed this scope; a second warning would be a duplicate"
        );

        // A different group on the same queue is a different declaration and
        // gets its own warning...
        let b = q("conflict-queue-a").group("g2").conflation(true);
        assert!(first_conflation_conflict(&b.conflation_scope()));
        // ...as does the same group on a different queue: the policy is stored
        // per (queue, group), so that is the unit that can disagree.
        let c = q("conflict-queue-b").group("g1").conflation(true);
        assert!(first_conflation_conflict(&c.conflation_scope()));
    }

    // The scope is the pair the policy is stored under — not the partition, which
    // would warn once per lane, and not the queue alone, which would silence a
    // second group's genuinely different disagreement.
    #[test]
    fn the_conflict_scope_is_the_queue_and_the_group_only() {
        let base = q("orders").group("workers");
        assert_eq!(
            base.clone().partition("eu").conflation_scope(),
            base.conflation_scope(),
            "two lanes of one queue share a stored policy, so they share a warning"
        );
        assert_ne!(
            q("orders").group("audit").conflation_scope(),
            base.conflation_scope()
        );

        // A group-less pop cannot conflate at all (the broker refuses it with
        // 400), but the scope still has to be well-defined rather than collide
        // with a real group named "".
        assert!(q("orders").conflation_scope().contains("__QUEUE_MODE__"));

        // A discovery pop has no queue in its path; its identity is the
        // namespace/task pair it addresses.
        let queen = Queen::connect(Config::new("http://127.0.0.1:1")).unwrap();
        let scope = queen
            .queue_opt(None)
            .namespace("billing")
            .task("invoice")
            .group("workers")
            .conflation_scope();
        assert!(
            scope.contains("billing") && scope.contains("invoice"),
            "{scope}"
        );
    }

    #[tokio::test]
    async fn the_other_queue_scoped_operations_also_require_a_name() {
        let b = Queen::connect(Config::new("http://127.0.0.1:1"))
            .unwrap()
            .queue_opt(None);
        assert!(b.flush_buffer().await.is_err());
        assert!(b.create().await.is_err());
        assert!(b.configure(QueueOptions::default()).await.is_err());
        assert!(b.name().is_none());
    }
}
