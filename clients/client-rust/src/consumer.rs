//! The consume loop.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use queen_protocol::{AckStatus, Message};

use crate::error::{Error, Result};
use crate::queue::QueueBuilder;

/// Cooperative shutdown signal for a running consumer.
///
/// Cloneable and cheap; hand one copy to the consumer and keep another to stop
/// it. Workers check it between polls and between messages, so a stop takes
/// effect at most one in-flight handler later — never mid-message, which would
/// leave a claim in limbo.
#[derive(Clone, Default)]
pub struct Cancel(Arc<CancelInner>);

impl std::fmt::Debug for Cancel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cancel")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

#[derive(Default)]
struct CancelInner {
    flag: AtomicBool,
    notify: tokio::sync::Notify,
}

impl Cancel {
    pub fn new() -> Self {
        Self::default()
    }

    /// Ask the consumer to wind down.
    pub fn cancel(&self) {
        self.0.flag.store(true, Ordering::SeqCst);
        self.0.notify.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.flag.load(Ordering::SeqCst)
    }

    /// Resolves once cancelled.
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        self.0.notify.notified().await;
    }
}

/// What a consumer did before it stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ConsumeSummary {
    pub processed: u64,
    pub acked: u64,
    pub nacked: u64,
    pub stopped_by: StopReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StopReason {
    /// Hit the configured message limit.
    Limit,
    /// Went quiet for longer than the idle timeout.
    Idle,
    /// A [`Cancel`] was triggered.
    Cancelled,
    /// Ran until the caller's future was dropped.
    #[default]
    Ended,
}

struct Shared {
    processed: AtomicU64,
    acked: AtomicU64,
    nacked: AtomicU64,
    stop: AtomicBool,
    reason: std::sync::Mutex<StopReason>,
}

impl QueueBuilder {
    /// Consume one message at a time.
    ///
    /// The handler's error type only needs to be printable; a returned error
    /// nacks the message (when `auto_ack` is on, the default) and the reason is
    /// recorded on the DLQ row if that nack exhausts the retry budget.
    ///
    /// # Ordering after a nack
    ///
    /// A nack releases the lease and clamps the group's cursor at the failed
    /// message, so everything after it in the same claimed batch *will* be
    /// redelivered. This loop therefore abandons the rest of the batch after a
    /// nack rather than processing messages whose acks the broker would reject
    /// anyway.
    pub async fn consume<F, Fut, E>(&self, handler: F) -> Result<ConsumeSummary>
    where
        F: Fn(Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), E>> + Send,
        E: std::fmt::Display + Send,
    {
        let handler = Arc::new(handler);
        self.run(move |msgs, ctx| {
            let handler = Arc::clone(&handler);
            async move {
                for msg in msgs {
                    if ctx.should_stop() {
                        break;
                    }
                    let outcome = handler(msg.clone()).await;
                    let ok = ctx.settle(&msg, outcome).await;
                    ctx.bump_processed();
                    if !ok {
                        // See the ordering note above.
                        tracing::warn!(
                            transaction_id = %msg.transaction_id,
                            "nacked; abandoning the rest of this batch (it will be redelivered)"
                        );
                        break;
                    }
                    if ctx.limit_reached() {
                        break;
                    }
                }
            }
        })
        .await
    }

    /// Consume a whole claimed batch per call.
    ///
    /// One ack (or nack) covers the batch, which is a round-trip cheaper than
    /// per-message settling when the handler is naturally vectorised.
    pub async fn consume_batch<F, Fut, E>(&self, handler: F) -> Result<ConsumeSummary>
    where
        F: Fn(Vec<Message>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), E>> + Send,
        E: std::fmt::Display + Send,
    {
        let handler = Arc::new(handler);
        self.run(move |msgs, ctx| {
            let handler = Arc::clone(&handler);
            async move {
                let n = msgs.len() as u64;
                let outcome = handler(msgs.clone()).await;
                ctx.settle_batch(&msgs, outcome).await;
                ctx.bump_processed_by(n);
            }
        })
        .await
    }

    async fn run<F, Fut>(&self, process: F) -> Result<ConsumeSummary>
    where
        F: Fn(Vec<Message>, WorkerCtx) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = ()> + Send,
    {
        if self.queue.is_none() && self.namespace.is_none() && self.task.is_none() {
            return Err(Error::Invalid(
                "consume needs a queue, or a namespace/task to discover one".into(),
            ));
        }

        let shared = Arc::new(Shared {
            processed: AtomicU64::new(0),
            acked: AtomicU64::new(0),
            nacked: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            reason: std::sync::Mutex::new(StopReason::Ended),
        });

        let mut workers = Vec::with_capacity(self.concurrency);
        for id in 0..self.concurrency {
            let builder = self.clone();
            let shared = Arc::clone(&shared);
            let process = process.clone();
            let cancel = self.cancel.clone();
            workers.push(tokio::spawn(async move {
                worker(id, builder, shared, cancel, process).await
            }));
        }

        let mut first_error = None;
        for w in workers {
            match w.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) if first_error.is_none() => first_error = Some(e),
                Ok(Err(_)) => {}
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(Error::Network(format!("consumer task panicked: {e}")));
                    }
                }
            }
        }

        if let Some(e) = first_error {
            return Err(e);
        }

        let stopped_by = *shared.reason.lock().unwrap();
        Ok(ConsumeSummary {
            processed: shared.processed.load(Ordering::Relaxed),
            acked: shared.acked.load(Ordering::Relaxed),
            nacked: shared.nacked.load(Ordering::Relaxed),
            stopped_by,
        })
    }
}

async fn worker<F, Fut>(
    id: usize,
    builder: QueueBuilder,
    shared: Arc<Shared>,
    cancel: Option<Cancel>,
    process: F,
) -> Result<()>
where
    F: Fn(Vec<Message>, WorkerCtx) -> Fut + Send,
    Fut: Future<Output = ()> + Send,
{
    let mut last_message = Instant::now();

    loop {
        if shared.stop.load(Ordering::SeqCst) {
            break;
        }
        if cancel.as_ref().is_some_and(|c| c.is_cancelled()) {
            stop_with(&shared, StopReason::Cancelled);
            break;
        }
        if let Some(limit) = builder.limit {
            if shared.processed.load(Ordering::SeqCst) >= limit {
                stop_with(&shared, StopReason::Limit);
                break;
            }
        }
        if let Some(idle) = builder.idle {
            if last_message.elapsed() >= idle {
                stop_with(&shared, StopReason::Idle);
                break;
            }
        }

        // Never take a broker-side auto-ack here: consume settles messages
        // itself, and a server-side ack at delivery would lose the batch on a
        // handler crash.
        let popped = match builder.pop().await {
            Ok(m) => m,
            Err(e) => {
                if e.is_terminal_refusal() {
                    // A suspended cluster or a gated feature never resolves;
                    // hot-looping on it helps nobody.
                    tracing::error!(worker = id, error = %e, "consumer stopping: terminal refusal");
                    stop_with(&shared, StopReason::Ended);
                    return Err(e);
                }
                if e.is_retryable() {
                    tracing::warn!(worker = id, error = %e, "poll failed; backing off");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                stop_with(&shared, StopReason::Ended);
                return Err(e);
            }
        };

        if popped.is_empty() {
            if !builder.wait {
                // Without long-polling an empty queue would spin the CPU.
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            continue;
        }

        last_message = Instant::now();

        // Keep the claim alive for as long as the handler runs.
        let renewal = builder.renew_lease.and_then(|interval| {
            let lease = popped.first()?.lease_id.clone();
            if lease.is_empty() {
                return None;
            }
            let inner = Arc::clone(&builder.inner);
            Some(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    let path = format!("/api/v1/lease/{}/extend", crate::queue::urlencode(&lease));
                    let out: Result<Option<serde_json::Value>> = inner
                        .http
                        .post_empty(&path, &crate::http::Opts::default())
                        .await;
                    if let Err(e) = out {
                        tracing::error!(error = %e, "lease renewal failed");
                    }
                }
            }))
        });

        let ctx = WorkerCtx {
            builder: builder.clone(),
            shared: Arc::clone(&shared),
            cancel: cancel.clone(),
        };
        process(popped, ctx).await;

        if let Some(h) = renewal {
            h.abort();
        }
    }

    Ok(())
}

fn stop_with(shared: &Shared, reason: StopReason) {
    if !shared.stop.swap(true, Ordering::SeqCst) {
        *shared.reason.lock().unwrap() = reason;
    }
}

/// Handed to the per-batch processor so it can settle messages and observe the
/// stop conditions without reaching back into the builder.
#[derive(Clone)]
pub struct WorkerCtx {
    builder: QueueBuilder,
    shared: Arc<Shared>,
    cancel: Option<Cancel>,
}

impl WorkerCtx {
    fn should_stop(&self) -> bool {
        self.shared.stop.load(Ordering::SeqCst)
            || self.cancel.as_ref().is_some_and(|c| c.is_cancelled())
    }

    fn limit_reached(&self) -> bool {
        self.builder
            .limit
            .is_some_and(|l| self.shared.processed.load(Ordering::SeqCst) >= l)
    }

    fn bump_processed(&self) {
        self.shared.processed.fetch_add(1, Ordering::SeqCst);
    }

    fn bump_processed_by(&self, n: u64) {
        self.shared.processed.fetch_add(n, Ordering::SeqCst);
    }

    /// Ack or nack one message. Returns whether it was acked.
    async fn settle<E: std::fmt::Display>(
        &self,
        msg: &Message,
        outcome: std::result::Result<(), E>,
    ) -> bool {
        if !self.builder.auto_ack {
            return outcome.is_ok();
        }
        match outcome {
            Ok(()) => {
                if let Err(e) = self.builder.inner.ack_one(msg, AckStatus::Completed, None).await {
                    tracing::error!(transaction_id = %msg.transaction_id, error = %e, "ack failed");
                }
                self.shared.acked.fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(err) => {
                let reason = err.to_string();
                if let Err(e) = self
                    .builder
                    .inner
                    .ack_one(msg, AckStatus::Failed, Some(reason.clone()))
                    .await
                {
                    tracing::error!(transaction_id = %msg.transaction_id, error = %e, "nack failed");
                }
                tracing::warn!(transaction_id = %msg.transaction_id, reason, "message nacked");
                self.shared.nacked.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    async fn settle_batch<E: std::fmt::Display>(
        &self,
        msgs: &[Message],
        outcome: std::result::Result<(), E>,
    ) {
        if !self.builder.auto_ack || msgs.is_empty() {
            return;
        }
        let (status, reason) = match outcome {
            Ok(()) => (AckStatus::Completed, None),
            Err(e) => (AckStatus::Failed, Some(e.to_string())),
        };
        if let Err(e) = self.builder.inner.ack_batch(msgs, status, reason).await {
            tracing::error!(count = msgs.len(), error = %e, "batch ack failed");
        }
        let n = msgs.len() as u64;
        if status == AckStatus::Completed {
            self.shared.acked.fetch_add(n, Ordering::Relaxed);
        } else {
            self.shared.nacked.fetch_add(n, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancel_is_observable_from_a_clone() {
        let a = Cancel::new();
        let b = a.clone();
        assert!(!b.is_cancelled());
        a.cancel();
        assert!(b.is_cancelled());
        // and resolves immediately once already cancelled
        b.cancelled().await;
    }

    #[tokio::test]
    async fn cancelled_resolves_when_triggered_later() {
        let a = Cancel::new();
        let b = a.clone();
        let waiter = tokio::spawn(async move { b.cancelled().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        a.cancel();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("cancelled() never resolved")
            .unwrap();
    }

    #[test]
    fn the_first_stop_reason_wins() {
        let shared = Shared {
            processed: AtomicU64::new(0),
            acked: AtomicU64::new(0),
            nacked: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            reason: std::sync::Mutex::new(StopReason::Ended),
        };
        stop_with(&shared, StopReason::Limit);
        stop_with(&shared, StopReason::Idle);
        assert_eq!(*shared.reason.lock().unwrap(), StopReason::Limit);
    }
}
