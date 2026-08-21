//! Client-side push batching.
//!
//! Accumulates messages per `(queue, partition)` and sends them in one request
//! once either threshold trips. This trades latency for throughput on the
//! producer side and is entirely optional — an unbuffered push goes out
//! immediately.
//!
//! Two properties beyond the batching are load-bearing, and neither existed
//! before 2026-08-20:
//!
//! - [`BufferOptions::max_size`] is a BLOCKING bound. Once that many messages
//!   are waiting (or in flight), [`BufferManager::add`] does not return until
//!   the flusher drains below it, so a producer that outruns the flush pipeline
//!   is paced down to the drain rate instead of growing the heap. Measured on
//!   the Go client, whose buffer had the same shape: filling at 1.46M msg/s
//!   against a 1.0M msg/s flush pipeline accumulated 20.9M messages (11.7 GB of
//!   RSS) in 45 seconds and lost every one of them at process exit, with ZERO
//!   client-side errors reported anywhere. The bounded version sustained
//!   881,148 msg/s with exact send/receive parity (39,655,787 = 39,655,787) and
//!   71 MB of RSS.
//!
//! - A batch whose request fails is put BACK at the front of the buffer, in
//!   order, and retried after [`BufferOptions::retry_delay`]. It is never
//!   dropped. Before this, the chunk was drained out before the POST and lost
//!   on `?`, so up to `message_count` messages vanished per failed request.
//!
//! Together they turn a broker outage into blocked producers with bounded
//! memory instead of silent loss.
//!
//! Buffered messages still live only in this process's memory: a crash loses
//! them, so buffering belongs on telemetry-shaped traffic, not on anything that
//! must not be lost. [`crate::Queen::close`] flushes before returning, which
//! covers an orderly shutdown.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore, TryAcquireError};

use queen_protocol::{PushItem, PushResult};

use crate::error::{Error, Result};
use crate::http::{HttpClient, Opts};

/// When a buffer flushes, and how much it will hold while it does.
///
/// Adding a field to a struct with public fields is a source break for literals
/// that name every field; build these with `..Default::default()`:
///
/// ```
/// # use queen_mq::BufferOptions;
/// # use std::time::Duration;
/// let opts = BufferOptions {
///     message_count: 500,
///     time: Duration::from_millis(200),
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferOptions {
    /// Flush once this many messages are waiting.
    pub message_count: usize,
    /// Flush this long after the first message arrives, however few there are.
    pub time: Duration,
    /// Backpressure bound: once this many messages are waiting or in flight,
    /// [`BufferManager::add`] waits for the flusher instead of growing the
    /// buffer. `0` means `4 * message_count`, and values below `message_count`
    /// are floored up to it — "unbounded" is deliberately not expressible,
    /// because unbounded is the defect this exists to close (see the module
    /// docs for the measurement).
    ///
    /// The bound covers the batch currently in flight, so unlike the Go
    /// reference — which bounds only what is in the buffer and lets one batch
    /// sit on top — occupancy here never exceeds `max_size`.
    pub max_size: usize,
    /// How long the flusher waits before retrying a batch whose request
    /// failed. The batch goes back to the front of the buffer and is retried
    /// until it lands or the manager is stopped; it is never dropped.
    /// [`Duration::ZERO`] means 250ms.
    pub retry_delay: Duration,
}

impl Default for BufferOptions {
    fn default() -> Self {
        Self {
            message_count: 100,
            time: Duration::from_millis(1000),
            // 4 x message_count, spelled out because Default is const-shaped.
            max_size: 400,
            retry_delay: Duration::from_millis(250),
        }
    }
}

impl BufferOptions {
    /// Fill in the derived defaults and enforce the bound's invariants. Applied
    /// once, when the buffer for an address is created.
    fn normalized(mut self) -> Self {
        let d = Self::default();
        if self.message_count == 0 {
            self.message_count = d.message_count;
        }
        if self.time.is_zero() {
            self.time = d.time;
        }
        if self.max_size == 0 {
            self.max_size = 4 * self.message_count;
        }
        if self.max_size < self.message_count {
            // A bound below the flush threshold would park the producer before
            // the buffer could assemble the batch that unparks it.
            self.max_size = self.message_count;
        }
        if self.retry_delay.is_zero() {
            self.retry_delay = d.retry_delay;
        }
        self
    }
}

/// A snapshot of what is waiting to be sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BufferStats {
    pub active_buffers: usize,
    pub total_buffered_messages: usize,
    /// Age of the oldest message still waiting.
    pub oldest_buffer_age: Duration,
    pub flushes_performed: u64,
}

/// The push body, borrowing the batch instead of owning it: the drain keeps
/// ownership of the chunk so it can put it back when the request fails — or
/// when the flush future is dropped mid-request. Same wire shape as
/// `queen_protocol::PushRequest`.
#[derive(Serialize)]
struct PushBatch<'a> {
    items: &'a [PushItem],
}

/// The ephemeral push body (EPHEMERAL_QUEUES.md §3.1), borrowing the same way.
///
/// The two families disagree about where the identity lives, and that
/// disagreement is the entire reason [`Destination`] exists: the durable push
/// repeats `{queue, partition}` on EVERY item, so its envelope is just
/// `{items}`; the ephemeral push hoists them to the envelope, so the elements
/// carry nothing but their payload — no `transactionId`, because there is no
/// dedup index to hold one.
#[derive(Serialize)]
struct EphemeralBatch<'a> {
    queue: &'a str,
    /// Omitted, never defaulted client-side: which ring a push without a
    /// partition lands on is the broker's rule to make.
    #[serde(skip_serializing_if = "Option::is_none")]
    partition: Option<&'a str>,
    messages: Vec<EphemeralItem<'a>>,
}

#[derive(Serialize)]
struct EphemeralItem<'a> {
    payload: &'a serde_json::Value,
}

/// WHERE a buffered batch goes, and in what shape.
///
/// The machinery this module implements — blocking backpressure at `max_size`,
/// one drain per address, a failed batch put back at the FRONT and retried
/// until it lands — is about ordering, occupancy and loss. None of that is
/// durable-specific, and none of it is worth writing twice, so the drain takes
/// a destination instead of a hardcoded POST.
///
/// [`Destination::Durable`] IS today's request, byte for byte, and it is what
/// every buffer created through [`BufferManager::add`] gets — which is every
/// caller that existed before ephemeral queues did. `tests/ephemeral_wire.rs`
/// carries a pin that fails if that ever stops being true.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Destination {
    /// `POST /api/v1/push`.
    Durable,
    /// `POST /api/v1/ephemeral/push`, bound to one `(queue, partition)`.
    Ephemeral {
        queue: String,
        partition: Option<String>,
    },
}

#[cfg(test)]
type FakeSink = Arc<
    dyn Fn(
            Vec<PushItem>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Vec<PushResult>>> + Send>,
        > + Send
        + Sync,
>;

/// Where a drained batch goes. In production this is the HTTP client; the unit
/// tests swap in a closure, because the contract worth testing here — ordering,
/// occupancy, retries, what happens to a batch nobody accepted — is about the
/// buffer, not about the broker.
enum Sink {
    Http(Arc<HttpClient>),
    #[cfg(test)]
    Fake(FakeSink),
}

impl Sink {
    async fn send(&self, dest: &Destination, items: &[PushItem]) -> Result<Vec<PushResult>> {
        match self {
            Self::Http(http) => match dest {
                Destination::Durable => {
                    let body = PushBatch { items };
                    let results: Option<Vec<PushResult>> = http
                        .post_json("/api/v1/push", &body, &Opts::default())
                        .await?;
                    Ok(results.unwrap_or_default())
                }
                Destination::Ephemeral { queue, partition } => {
                    let body = EphemeralBatch {
                        queue,
                        partition: partition.as_deref(),
                        messages: items
                            .iter()
                            .map(|i| EphemeralItem {
                                payload: &i.payload,
                            })
                            .collect(),
                    };
                    // `{pushed}` and no per-item array: this wire has no
                    // message id to report, because it has no dedup index to
                    // mint one from. The count is read so a malformed answer
                    // still fails the flush (and re-queues the batch) rather
                    // than being taken for success.
                    let _: Option<queen_protocol::EphemeralPushResponse> = http
                        .post_json(EPHEMERAL_PUSH_PATH, &body, &Opts::default())
                        .await?;
                    Ok(Vec::new())
                }
            },
            #[cfg(test)]
            Self::Fake(f) => f(items.to_vec()).await,
        }
    }
}

/// The ephemeral push route. Named here because the drain and the unbuffered
/// push in `ephemeral.rs` must not be able to disagree about it.
pub(crate) const EPHEMERAL_PUSH_PATH: &str = "/api/v1/ephemeral/push";

struct Buffer {
    items: Vec<PushItem>,
    opts: BufferOptions,
    first_at: Option<Instant>,
    /// Set while a timer task is armed for this address, so repeated adds do
    /// not stack up one timer per message.
    timer_armed: bool,
    /// Set while a drain task is spawned or running for this address. Without
    /// it, every add past the threshold spawned ANOTHER flush task — under
    /// overload that is one spawned task per message, all of them contending
    /// for the same buffer.
    flushing: bool,
    /// One permit per message that is in the buffer OR in flight. Producers
    /// take a permit before appending and the flusher returns it only once the
    /// message is definitively sent, which is what makes `max_size` a real
    /// bound rather than a hint: a batch that fails keeps its permits, so a
    /// re-queue cannot let producers refill against room that never freed.
    ///
    /// A semaphore rather than a `Notify`: `acquire` queues waiters FIFO,
    /// stores no wakeup to lose, is cancel-safe on drop (a dropped `add`
    /// simply leaves the queue), and `close()` releases every waiter at once,
    /// which is exactly what stop() needs.
    capacity: Arc<Semaphore>,
    /// Adds parked on `capacity`, or woken and not yet re-locked. The buffer
    /// entry must not be removed from the map while this is non-zero: a parked
    /// add holds THIS entry's semaphore, and a replacement entry would have a
    /// different one.
    parked: usize,
    /// Serializes actual draining for this address and lets a second flusher
    /// WAIT for the one in flight instead of interleaving batches with it —
    /// two senders on one partition would reorder the lane.
    drain_lock: Arc<AsyncMutex<()>>,
    /// Where this address drains to. Fixed when the entry is created, and a
    /// pure function of the address: the `eph:` prefix that namespaces an
    /// ephemeral address is exactly what keeps the two families' buffers apart,
    /// so one address can never want two destinations.
    dest: Destination,
}

impl Buffer {
    fn with_dest(opts: BufferOptions, dest: Destination) -> Self {
        let opts = opts.normalized();
        Self {
            items: Vec::new(),
            first_at: None,
            timer_armed: false,
            flushing: false,
            capacity: Arc::new(Semaphore::new(opts.max_size)),
            parked: 0,
            drain_lock: Arc::new(AsyncMutex::new(())),
            dest,
            opts,
        }
    }
}

pub(crate) struct BufferManager {
    sink: Sink,
    buffers: Mutex<HashMap<String, Buffer>>,
    flush_count: AtomicU64,
    stopped: AtomicBool,
}

impl BufferManager {
    pub fn new(http: Arc<HttpClient>) -> Arc<Self> {
        Arc::new(Self {
            sink: Sink::Http(http),
            buffers: Mutex::new(HashMap::new()),
            flush_count: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        })
    }

    /// Queue a message, waiting for room if the buffer is at its bound.
    ///
    /// BACKPRESSURE: this is where a producer that outruns the flush pipeline
    /// gets paced. The wait ends when the flusher drains below `max_size`, or
    /// when the manager is stopped (an error — the message was never
    /// buffered), or when the caller drops this future, which is how
    /// cancellation is spelled in async Rust: `tokio::time::timeout(d,
    /// buffers.add(..))` gives up cleanly and leaves nothing behind.
    pub async fn add(
        self: &Arc<Self>,
        address: String,
        item: PushItem,
        opts: BufferOptions,
    ) -> Result<()> {
        self.add_to(address, Destination::Durable, item, opts).await
    }

    /// [`BufferManager::add`], for a buffer that drains somewhere other than the
    /// durable push. The destination is applied when the entry is created and
    /// ignored afterwards — see [`Buffer::dest`].
    pub async fn add_to(
        self: &Arc<Self>,
        address: String,
        dest: Destination,
        item: PushItem,
        opts: BufferOptions,
    ) -> Result<()> {
        enum Slot {
            Ready(OwnedSemaphorePermit),
            Full(Arc<Semaphore>),
        }

        let slot = {
            let mut buffers = self.buffers.lock().unwrap();
            if self.stopped.load(Ordering::Acquire) {
                return Err(stopped_error(&address));
            }
            let buf = buffers
                .entry(address.clone())
                .or_insert_with(|| Buffer::with_dest(opts, dest.clone()));
            let capacity = Arc::clone(&buf.capacity);
            match Arc::clone(&capacity).try_acquire_owned() {
                Ok(permit) => Slot::Ready(permit),
                Err(TryAcquireError::NoPermits) => {
                    buf.parked += 1;
                    Slot::Full(capacity)
                }
                Err(TryAcquireError::Closed) => return Err(stopped_error(&address)),
            }
        };

        let (permit, parked_guard) = match slot {
            Slot::Ready(permit) => (permit, None),
            Slot::Full(capacity) => {
                // Parked means producers outran the flusher, so make sure one
                // is actually running before waiting on it — the time-based
                // flush may be a full `time` away, and nothing else starts it.
                self.spawn_drain(address.clone());
                // Decrements `parked` however this scope ends, cancellation
                // included. Released only once the message is actually in the
                // buffer, so the entry cannot be retired out from under it.
                let guard = ParkedGuard {
                    manager: self,
                    address: address.clone(),
                };
                let permit = capacity
                    .acquire_owned()
                    .await
                    .map_err(|_| stopped_error(&address))?;
                (permit, Some(guard))
            }
        };
        // Held by the message from here on: the flusher returns this permit
        // with `add_permits` once the message has actually been sent, so
        // dropping it at the end of this scope would uncap the buffer.
        permit.forget();

        let (should_flush, arm_timer, delay) = {
            let mut buffers = self.buffers.lock().unwrap();
            if let Some(guard) = parked_guard {
                guard.release(&mut buffers);
            }
            let buf = buffers
                .entry(address.clone())
                .or_insert_with(|| Buffer::with_dest(opts, dest.clone()));
            if buf.items.is_empty() {
                buf.first_at = Some(Instant::now());
            }
            buf.items.push(item);

            let should_flush = buf.items.len() >= buf.opts.message_count;
            let arm_timer = !should_flush && !buf.timer_armed;
            if arm_timer {
                buf.timer_armed = true;
            }
            (should_flush, arm_timer, buf.opts.time)
        };

        if should_flush {
            self.spawn_drain(address);
        } else if arm_timer {
            let this = Arc::clone(self);
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                {
                    let mut buffers = this.buffers.lock().unwrap();
                    if let Some(b) = buffers.get_mut(&address) {
                        b.timer_armed = false;
                    }
                }
                this.spawn_drain(address);
            });
        }

        Ok(())
    }

    /// Start the retrying drain for an address, unless one is already going.
    ///
    /// The `flushing` flag is claimed here, under the buffers lock, rather than
    /// inside the spawned task: claiming it later would leave a window in which
    /// every add past the threshold spawns its own task.
    fn spawn_drain(self: &Arc<Self>, address: String) {
        {
            let mut buffers = self.buffers.lock().unwrap();
            let Some(buf) = buffers.get_mut(&address) else {
                return;
            };
            if buf.flushing || buf.items.is_empty() {
                return;
            }
            buf.flushing = true;
        }

        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.drain_retrying(&address).await;
        });
    }

    /// Flush an address, retrying a failed batch after `retry_delay` until it
    /// lands or the manager is stopped. This is the background flusher: it is
    /// the one path that must never give up, because giving up here is the
    /// silent loss the fix exists to remove.
    async fn drain_retrying(&self, address: &str) {
        let _flushing = FlushingGuard {
            manager: self,
            address,
        };

        loop {
            match self.flush(address).await {
                Ok(_) => return,
                Err(e) => {
                    let Some(delay) = self.retry_delay_of(address) else {
                        return; // buffer is gone: nothing left to retry
                    };
                    if self.stopped.load(Ordering::Acquire) {
                        return;
                    }
                    tracing::warn!(
                        address = %address,
                        error = %e,
                        retry_in_ms = delay.as_millis() as u64,
                        "buffer flush failed; batch re-queued, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    if self.stopped.load(Ordering::Acquire) {
                        return;
                    }
                }
            }
        }
    }

    /// Send everything waiting for one address, in `message_count`-sized
    /// requests.
    ///
    /// One attempt per batch: a batch that fails goes back to the front of the
    /// buffer (nothing is lost) and the error is returned. Retrying is the
    /// background flusher's job — a caller-facing flush that retried forever
    /// would turn a broker outage into a shutdown that never ends.
    ///
    /// If another flush is already draining this address, this waits for it
    /// rather than interleaving batches with it.
    pub async fn flush(&self, address: &str) -> Result<Vec<PushResult>> {
        let drain_lock = {
            let buffers = self.buffers.lock().unwrap();
            buffers.get(address).map(|b| Arc::clone(&b.drain_lock))
        };
        let Some(drain_lock) = drain_lock else {
            return Ok(Vec::new());
        };
        let _drain = drain_lock.lock_owned().await;

        let mut all = Vec::new();
        loop {
            let (chunk, dest) = {
                let mut buffers = self.buffers.lock().unwrap();
                let Some(buf) = buffers.get_mut(address) else {
                    break;
                };
                if buf.items.is_empty() {
                    if buf.parked == 0 {
                        buffers.remove(address);
                    }
                    break;
                }
                let n = buf.opts.message_count.min(buf.items.len()).max(1);
                let chunk: Vec<PushItem> = buf.items.drain(..n).collect();
                if buf.items.is_empty() {
                    buf.first_at = None;
                }
                (chunk, buf.dest.clone())
            };

            // Owns the chunk until it is acknowledged. If the request fails —
            // or this future is dropped mid-request, which a caller's timeout
            // does — the chunk goes back at the FRONT of the buffer, in order.
            let mut in_flight = InFlight {
                manager: self,
                address,
                items: Some(chunk),
                dest: dest.clone(),
            };

            let result = {
                let items = in_flight.items.as_deref().expect("just set");
                self.sink.send(&dest, items).await
            };

            match result {
                Ok(results) => {
                    let sent = in_flight.sent();
                    self.flush_count.fetch_add(1, Ordering::Relaxed);
                    // Only now is the room genuinely free: returning the
                    // permits is what wakes producers parked on the bound.
                    self.release_capacity(address, sent);
                    all.extend(results);
                }
                Err(e) => {
                    // in_flight's Drop re-queues the batch.
                    return Err(e);
                }
            }
        }
        Ok(all)
    }

    /// Send everything waiting, for every address.
    pub async fn flush_all(&self) -> Result<Vec<PushResult>> {
        let addresses: Vec<String> = {
            let buffers = self.buffers.lock().unwrap();
            buffers.keys().cloned().collect()
        };
        let mut all = Vec::new();
        let mut first_error = None;
        for a in addresses {
            match self.flush(&a).await {
                Ok(r) => all.extend(r),
                // Keep going: one unreachable queue must not strand the others'
                // messages during a shutdown flush.
                Err(e) if first_error.is_none() => first_error = Some(e),
                Err(_) => {}
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(all),
        }
    }

    /// [`flush_all`](Self::flush_all), retrying failures until everything lands
    /// or the manager is stopped. Meant to be run under a caller's timeout —
    /// [`crate::Queen::close`] does exactly that, so a shutdown rides out a
    /// broker restart but still ends.
    pub async fn flush_all_retrying(&self) -> Result<Vec<PushResult>> {
        loop {
            match self.flush_all().await {
                Ok(r) => return Ok(r),
                Err(e) => {
                    if self.stopped.load(Ordering::Acquire) {
                        return Err(e);
                    }
                    tokio::time::sleep(self.shortest_retry_delay()).await;
                }
            }
        }
    }

    /// Stop accepting messages, wake every parked add and end the retry loops.
    /// Returns how many messages were still buffered — anything counted here
    /// never reached the broker, and saying so out loud is the whole point of
    /// the exercise.
    pub fn stop(&self) -> usize {
        self.stopped.store(true, Ordering::Release);
        let buffers = self.buffers.lock().unwrap();
        let mut unsent = 0;
        for buf in buffers.values() {
            unsent += buf.items.len();
            // Releases every parked add with an error: their message was never
            // buffered, and a shutdown must not leave a producer waiting on a
            // flusher that is not coming back.
            buf.capacity.close();
        }
        unsent
    }

    pub fn stats(&self) -> BufferStats {
        let buffers = self.buffers.lock().unwrap();
        let now = Instant::now();
        let mut total = 0;
        let mut oldest = Duration::ZERO;
        for b in buffers.values() {
            total += b.items.len();
            if let Some(t) = b.first_at {
                oldest = oldest.max(now.duration_since(t));
            }
        }
        BufferStats {
            active_buffers: buffers.len(),
            total_buffered_messages: total,
            oldest_buffer_age: oldest,
            flushes_performed: self.flush_count.load(Ordering::Relaxed),
        }
    }

    fn release_capacity(&self, address: &str, permits: usize) {
        if permits == 0 {
            return;
        }
        let buffers = self.buffers.lock().unwrap();
        if let Some(buf) = buffers.get(address) {
            buf.capacity.add_permits(permits);
        }
    }

    fn retry_delay_of(&self, address: &str) -> Option<Duration> {
        let buffers = self.buffers.lock().unwrap();
        buffers.get(address).map(|b| b.opts.retry_delay)
    }

    fn shortest_retry_delay(&self) -> Duration {
        let buffers = self.buffers.lock().unwrap();
        buffers
            .values()
            .map(|b| b.opts.retry_delay)
            .min()
            .unwrap_or_else(|| BufferOptions::default().retry_delay)
    }
}

/// Puts an in-flight batch back at the front of its buffer unless it was
/// acknowledged. A plain `?` in the send loop is what lost the chunk before;
/// making the re-queue a `Drop` means it also survives the flush future being
/// dropped mid-request.
struct InFlight<'a> {
    manager: &'a BufferManager,
    address: &'a str,
    items: Option<Vec<PushItem>>,
    /// Carried so the rebuild path below can restore the entry with the
    /// destination it had, rather than silently re-creating an ephemeral
    /// address as a durable one.
    dest: Destination,
}

impl InFlight<'_> {
    /// Disarm: the batch reached the broker. Returns how many messages that was.
    fn sent(&mut self) -> usize {
        self.items.take().map(|i| i.len()).unwrap_or(0)
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        let Some(items) = self.items.take() else {
            return;
        };
        if items.is_empty() {
            return;
        }
        let mut buffers = self.manager.buffers.lock().unwrap();
        let Some(buf) = buffers.get_mut(self.address) else {
            // The entry was retired under us (only an empty buffer is ever
            // retired). Rebuild it around the batch rather than dropping the
            // batch — dropping is the very defect this guard exists for.
            let opts = BufferOptions {
                max_size: items.len().max(BufferOptions::default().max_size),
                ..Default::default()
            };
            let mut restored = Buffer::with_dest(opts, self.dest.clone());
            // These messages held capacity on the entry that went away; take it
            // again on the fresh one so the bound still describes reality.
            restored
                .capacity
                .try_acquire_many(items.len() as u32)
                .expect("max_size was sized to fit this batch")
                .forget();
            restored.first_at = Some(Instant::now());
            restored.items = items;
            buffers.insert(self.address.to_string(), restored);
            return;
        };
        // Front, not back: these were queued before everything still waiting,
        // and a retry must not reorder the partition's lane.
        let mut items = items;
        items.append(&mut buf.items);
        buf.items = items;
        if buf.first_at.is_none() {
            buf.first_at = Some(Instant::now());
        }
    }
}

/// Keeps `Buffer::parked` accurate across every exit path, cancellation
/// included: a leaked count would keep an empty buffer entry alive forever,
/// and an early one would let a flush retire the entry between the wake and
/// the append — leaving the woken add holding a permit on a semaphore nobody
/// looks at any more.
struct ParkedGuard<'a> {
    manager: &'a BufferManager,
    address: String,
}

impl ParkedGuard<'_> {
    /// Decrement inside a lock the caller already holds, and defuse `Drop` —
    /// which would take that same non-reentrant lock again.
    fn release(self, buffers: &mut HashMap<String, Buffer>) {
        if let Some(buf) = buffers.get_mut(&self.address) {
            buf.parked = buf.parked.saturating_sub(1);
        }
        std::mem::forget(self);
    }
}

impl Drop for ParkedGuard<'_> {
    fn drop(&mut self) {
        let mut buffers = self.manager.buffers.lock().unwrap();
        if let Some(buf) = buffers.get_mut(&self.address) {
            buf.parked = buf.parked.saturating_sub(1);
        }
    }
}

/// Clears `Buffer::flushing` however the drain task ends, so a panic or an
/// early return cannot leave an address permanently marked as flushing —
/// which would mean no further flush is ever spawned for it.
struct FlushingGuard<'a> {
    manager: &'a BufferManager,
    address: &'a str,
}

impl Drop for FlushingGuard<'_> {
    fn drop(&mut self) {
        let mut buffers = self.manager.buffers.lock().unwrap();
        if let Some(buf) = buffers.get_mut(self.address) {
            buf.flushing = false;
        }
    }
}

fn stopped_error(address: &str) -> Error {
    Error::Invalid(format!(
        "push buffer {address} is stopped: message not buffered"
    ))
}

/// The key a buffer is held under. Batching is per `(queue, partition)` because
/// that is the granularity the broker fuses on — mixing partitions in one
/// buffer would not make the write any cheaper.
pub(crate) fn address(queue: &str, partition: &str) -> String {
    format!("{queue}/{partition}")
}

/// The ephemeral counterpart, kept next to its durable sibling so the two can
/// be compared at a glance: `eph:queue/partition`, or `eph:queue` when the
/// caller named no partition — which is a DIFFERENT destination from any named
/// one, because the broker picks, and a buffer must not merge the two.
///
/// The prefix is [`queen_protocol::EPHEMERAL_KEY_PREFIX`], the same namespacing
/// the broker applies to its own queue keys, for the same reason: an ephemeral
/// `orders` and a durable `orders` are unrelated objects, and a shared address
/// would post one family's messages to the other family's route.
pub(crate) fn ephemeral_address(queue: &str, partition: Option<&str>) -> String {
    match partition {
        Some(p) => format!("{}{queue}/{p}", queen_protocol::EPHEMERAL_KEY_PREFIX),
        None => format!("{}{queue}", queen_protocol::EPHEMERAL_KEY_PREFIX),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::future::Future;
    use std::sync::atomic::AtomicUsize;

    fn manager() -> Arc<BufferManager> {
        let http = Arc::new(HttpClient::new(Config::new("http://127.0.0.1:1")).unwrap());
        BufferManager::new(http)
    }

    /// Same unreachable port, but giving up after one attempt: the default three
    /// attempts with exponential backoff make every failing flush cost three
    /// seconds of wall clock, and these tests drive a lot of failing flushes.
    fn impatient_manager() -> Arc<BufferManager> {
        let config = Config::new("http://127.0.0.1:1").retry_attempts(1);
        let http =
            Arc::new(HttpClient::new(config).expect("one http:// URL is a valid configuration"));
        BufferManager::new(http)
    }

    /// A manager whose sink is a closure instead of a broker. Everything this
    /// module promises — ordering, occupancy, retries, nothing dropped — is
    /// observable here without a server.
    fn fake_manager<F, Fut>(f: F) -> Arc<BufferManager>
    where
        F: Fn(Vec<PushItem>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<PushResult>>> + Send + 'static,
    {
        Arc::new(BufferManager {
            sink: Sink::Fake(Arc::new(move |items| Box::pin(f(items)))),
            buffers: Mutex::new(HashMap::new()),
            flush_count: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        })
    }

    /// The sink every parity test uses: records what actually went out, and
    /// fails whichever attempts the test asks it to.
    #[derive(Default)]
    struct Recorder {
        sent: Mutex<Vec<u64>>,
        attempts: AtomicUsize,
        batches: Mutex<Vec<Vec<u64>>>,
    }

    impl Recorder {
        fn n(item: &PushItem) -> u64 {
            item.payload["n"].as_u64().expect("test items carry n")
        }

        fn attempt(&self, items: &[PushItem], accept: bool) -> Result<Vec<PushResult>> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            let ns: Vec<u64> = items.iter().map(Self::n).collect();
            self.batches.lock().unwrap().push(ns.clone());
            if !accept {
                return Err(Error::Network("sink refused this attempt".into()));
            }
            self.sent.lock().unwrap().extend(ns);
            Ok(Vec::new())
        }

        fn sent(&self) -> Vec<u64> {
            self.sent.lock().unwrap().clone()
        }
    }

    fn item(n: u64) -> PushItem {
        PushItem::new("orders", serde_json::json!({ "n": n }))
    }

    fn opts(message_count: usize) -> BufferOptions {
        BufferOptions {
            message_count,
            time: Duration::from_secs(60),
            ..Default::default()
        }
    }

    /// Put items in a buffer without going through `add`, which flushes on the
    /// way past `message_count` and so cannot build a buffer big enough to make
    /// `flush` send more than one chunk.
    fn seed(m: &Arc<BufferManager>, address: &str, count: u64, opts: BufferOptions) {
        let mut buffers = m.buffers.lock().unwrap();
        let mut buf = Buffer::with_dest(opts, Destination::Durable);
        buf.items = (0..count).map(item).collect();
        buf.first_at = Some(Instant::now());
        // Seeded messages hold capacity like added ones do, or the bound would
        // be wrong for anything the test adds afterwards.
        buf.capacity
            .try_acquire_many(count as u32)
            .expect("seed must fit inside max_size")
            .forget();
        buffers.insert(address.to_string(), buf);
    }

    /// Whether a timer task is currently armed for this address. Panics if the
    /// buffer entry is gone, which is itself worth failing on: a flush only
    /// removes the entry when it succeeded.
    fn timer_armed(m: &Arc<BufferManager>, address: &str) -> bool {
        m.buffers
            .lock()
            .unwrap()
            .get(address)
            .map(|b| b.timer_armed)
            .expect("no buffer at that address")
    }

    /// Poll until the buffered count reaches `want`, then return it. A fixed
    /// sleep would be either flaky on a loaded machine or needlessly slow on an
    /// idle one.
    async fn buffered_settles_at(m: &Arc<BufferManager>, want: usize) -> usize {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let n = m.stats().total_buffered_messages;
            if n == want || Instant::now() >= deadline {
                return n;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn until(mut done: impl FnMut() -> bool, what: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if done() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("timed out waiting for: {what}");
    }

    #[test]
    fn address_is_per_queue_and_partition() {
        assert_eq!(address("orders", "Default"), "orders/Default");
        assert_eq!(address("orders", "eu"), "orders/eu");
    }

    #[test]
    fn defaults_match_the_other_sdks() {
        let d = BufferOptions::default();
        assert_eq!(d.message_count, 100);
        assert_eq!(d.time, Duration::from_millis(1000));
        assert_eq!(d.max_size, 400);
        assert_eq!(d.retry_delay, Duration::from_millis(250));
    }

    // 0 means the DEFAULT bound, never "unbounded": unbounded is the defect
    // this knob closes, so opting out of backpressure is not expressible.
    #[test]
    fn a_zero_max_size_resolves_to_a_bound() {
        let o = BufferOptions {
            message_count: 10,
            max_size: 0,
            ..Default::default()
        }
        .normalized();
        assert_eq!(o.max_size, 40, "0 must mean 4 x message_count, not infinity");

        let floored = BufferOptions {
            message_count: 100,
            max_size: 10,
            ..Default::default()
        }
        .normalized();
        assert_eq!(
            floored.max_size, 100,
            "a bound below the flush threshold would park a producer before the buffer could \
             assemble the batch that unparks it"
        );

        let zeroed = BufferOptions {
            message_count: 0,
            time: Duration::ZERO,
            max_size: 0,
            retry_delay: Duration::ZERO,
        }
        .normalized();
        assert_eq!(zeroed.message_count, 100);
        assert_eq!(zeroed.time, Duration::from_millis(1000));
        assert_eq!(zeroed.max_size, 400);
        assert_eq!(zeroed.retry_delay, Duration::from_millis(250));
    }

    #[tokio::test]
    async fn accumulates_below_the_threshold() {
        let m = manager();
        let o = opts(10);
        for i in 0..4 {
            m.add(address("orders", "Default"), item(i), o).await.unwrap();
        }
        let s = m.stats();
        assert_eq!(s.active_buffers, 1);
        assert_eq!(s.total_buffered_messages, 4);
        assert_eq!(s.flushes_performed, 0);
    }

    #[tokio::test]
    async fn separate_partitions_buffer_separately() {
        let m = manager();
        let o = opts(10);
        m.add(address("orders", "eu"), item(1), o).await.unwrap();
        m.add(address("orders", "us"), item(2), o).await.unwrap();
        m.add(address("other", "eu"), item(3), o).await.unwrap();
        assert_eq!(m.stats().active_buffers, 3);
    }

    #[tokio::test]
    async fn oldest_age_tracks_the_first_message() {
        let m = manager();
        let o = opts(100);
        m.add(address("orders", "Default"), item(1), o).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        m.add(address("orders", "Default"), item(2), o).await.unwrap();
        let s = m.stats();
        assert_eq!(s.total_buffered_messages, 2);
        assert!(
            s.oldest_buffer_age >= Duration::from_millis(25),
            "age {:?} should track the FIRST message, not the last",
            s.oldest_buffer_age
        );
    }

    #[tokio::test]
    async fn flushing_an_unknown_address_is_a_no_op() {
        let m = manager();
        let out = m.flush("nothing/here").await.unwrap();
        assert!(out.is_empty());
        assert_eq!(m.stats().flushes_performed, 0);
    }

    #[tokio::test]
    async fn a_failed_flush_reports_the_error() {
        // Port 1 refuses; the flush must surface that rather than silently
        // dropping the batch.
        let m = manager();
        let o = opts(100);
        m.add(address("orders", "Default"), item(1), o).await.unwrap();
        let err = m.flush(&address("orders", "Default")).await;
        assert!(
            err.is_err(),
            "unreachable broker must not look like success"
        );
    }

    // The time-based flush is the branch every other test in this file avoids
    // by using a 60s timer — while the shipped default is 1s, so it is the
    // branch real producers live on. Broken, a low-volume producer's messages
    // would sit in memory until the process exits.
    #[tokio::test]
    async fn the_timer_flushes_a_buffer_that_never_reaches_its_count() {
        let m = fake_manager(|_| async { Ok(Vec::new()) });
        let o = BufferOptions {
            message_count: 1_000,
            time: Duration::from_millis(80),
            ..Default::default()
        };
        let addr = address("orders", "Default");
        m.add(addr.clone(), item(1), o).await.unwrap();
        m.add(addr.clone(), item(2), o).await.unwrap();

        assert_eq!(
            m.stats().total_buffered_messages,
            2,
            "two items are nowhere near the 1000-message threshold, so nothing should have gone \
             out yet"
        );
        assert!(timer_armed(&m, &addr), "the first add armed no timer");

        assert_eq!(
            buffered_settles_at(&m, 0).await,
            0,
            "the 80ms timer never fired: the buffer is still holding its messages"
        );
    }

    // `timer_armed` is what keeps one timer per *batch* instead of one per
    // message. If it were never cleared the buffer would flush once and then go
    // silent forever; if it were never set, or set per add, the batching this
    // module exists for would collapse into a request per message.
    #[tokio::test]
    async fn the_timer_re_arms_for_the_batch_after_it() {
        let m = fake_manager(|_| async { Ok(Vec::new()) });
        let o = BufferOptions {
            message_count: 1_000,
            time: Duration::from_millis(80),
            ..Default::default()
        };
        let addr = address("orders", "Default");

        m.add(addr.clone(), item(1), o).await.unwrap();
        assert!(timer_armed(&m, &addr));
        m.add(addr.clone(), item(2), o).await.unwrap();
        assert!(
            timer_armed(&m, &addr),
            "an add while a timer is already armed must leave the flag alone, not schedule a \
             second flush"
        );

        assert_eq!(buffered_settles_at(&m, 0).await, 0, "first flush never ran");

        m.add(addr.clone(), item(3), o).await.unwrap();
        assert_eq!(
            m.stats().total_buffered_messages,
            1,
            "the third item did not land in the buffer"
        );
        assert!(timer_armed(&m, &addr), "the timer did not re-arm");
        assert_eq!(
            buffered_settles_at(&m, 0).await,
            0,
            "the re-armed timer never fired: only the first batch would ever be sent"
        );
    }

    // The buffer keeps the options of the add that created it. Worth pinning
    // because the call site is `push_items`, where the options come from a
    // per-call builder and so look like they should retune an existing buffer.
    #[tokio::test]
    async fn the_add_that_creates_a_buffer_fixes_its_thresholds() {
        let m = impatient_manager();
        let addr = address("orders", "Default");
        m.add(addr.clone(), item(0), opts(1_000)).await.unwrap();
        for i in 1..3 {
            m.add(addr.clone(), item(i), opts(2)).await.unwrap();
        }

        // Give any flush task that was wrongly spawned a chance to run: on a
        // current-thread runtime nothing spawned gets polled until we await.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            m.stats().total_buffered_messages,
            3,
            "a later add's message_count=2 retuned the buffer and flushed it"
        );
    }

    // The defect this replaced: `flush` took its chunk out of the buffer before
    // the request and a failure did not put it back, so a flush that never
    // reached the broker lost those messages outright.
    #[tokio::test]
    async fn a_failed_flush_keeps_the_chunk_it_could_not_send() {
        let m = impatient_manager();
        let addr = address("orders", "Default");
        for i in 0..3 {
            m.add(addr.clone(), item(i), opts(100)).await.unwrap();
        }

        assert!(
            m.flush(&addr).await.is_err(),
            "an unreachable broker must not look like success"
        );

        let s = m.stats();
        assert_eq!(
            s.total_buffered_messages, 3,
            "the chunk must go back into the buffer: dropping it is the loss this fixed"
        );
        assert_eq!(
            s.flushes_performed, 0,
            "a flush that never reached the broker must not be counted as one"
        );
        assert_eq!(s.active_buffers, 1);
    }

    // Ordering is the product's headline promise, so a re-queued batch has to
    // land back at the FRONT — ahead of anything added while it was failing.
    #[tokio::test]
    async fn a_re_queued_batch_keeps_its_place_at_the_front() {
        let recorder = Arc::new(Recorder::default());
        let sink = Arc::clone(&recorder);
        // Fail the first attempt only.
        let m = fake_manager(move |items| {
            let sink = Arc::clone(&sink);
            async move {
                let first = sink.attempts.load(Ordering::SeqCst) == 0;
                sink.attempt(&items, !first)
            }
        });

        let addr = address("orders", "Default");
        let o = BufferOptions {
            message_count: 2,
            time: Duration::from_secs(60),
            max_size: 20,
            retry_delay: Duration::from_millis(5),
        };
        for i in 0..6 {
            m.add(addr.clone(), item(i), o).await.unwrap();
        }

        until(
            || recorder.sent().len() == 6,
            "every message to reach the sink",
        )
        .await;
        assert_eq!(
            recorder.sent(),
            vec![0, 1, 2, 3, 4, 5],
            "the retried batch must go back ahead of the messages queued behind it"
        );
        assert_eq!(
            recorder.batches.lock().unwrap()[0],
            vec![0, 1],
            "the failed attempt was the first batch"
        );
        assert_eq!(
            recorder.batches.lock().unwrap()[1],
            vec![0, 1],
            "the retry must resend the SAME batch, not the next one"
        );
    }

    // The whole point, stated as count parity: every message an add accepted
    // comes out of the sink exactly once, in order, however many attempts
    // failed on the way. This is the shape of the 39,655,787 = 39,655,787 run
    // that validated the Go fix, at a size a unit test can afford.
    #[tokio::test]
    async fn nothing_is_dropped_across_intermittent_failures() {
        let recorder = Arc::new(Recorder::default());
        let sink = Arc::clone(&recorder);
        let m = fake_manager(move |items| {
            let sink = Arc::clone(&sink);
            async move {
                let attempt = sink.attempts.load(Ordering::SeqCst);
                sink.attempt(&items, attempt % 3 != 1)
            }
        });

        let addr = address("orders", "Default");
        let o = BufferOptions {
            message_count: 7,
            time: Duration::from_millis(50),
            max_size: 21,
            retry_delay: Duration::from_millis(1),
        };
        for i in 0..300 {
            m.add(addr.clone(), item(i), o).await.unwrap();
        }
        // The tail below message_count leaves on the timer or this flush.
        while m.stats().total_buffered_messages > 0 {
            let _ = m.flush(&addr).await;
        }
        until(|| recorder.sent().len() == 300, "the full run to drain").await;

        let sent = recorder.sent();
        assert_eq!(sent.len(), 300, "messages were dropped");
        assert_eq!(
            sent,
            (0..300).collect::<Vec<u64>>(),
            "order was not preserved across the retries"
        );
        assert!(
            recorder.attempts.load(Ordering::SeqCst) > 300 / 7,
            "precondition: the sink actually refused some attempts"
        );
    }

    // BACKPRESSURE. An add past the bound must WAIT, not append: this is the
    // 20.9M-messages-in-45-seconds defect, in miniature.
    #[tokio::test]
    async fn add_blocks_at_the_bound_and_resumes_when_the_flusher_drains() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let sink_gate = Arc::clone(&gate);
        let recorder = Arc::new(Recorder::default());
        let sink = Arc::clone(&recorder);
        // Counted on ARRIVAL, before the gate: the point of the test is that a
        // batch is sitting in the sink unanswered, which is invisible in the
        // recorder (it only records completed attempts).
        let arrived = Arc::new(AtomicUsize::new(0));
        let sink_arrived = Arc::clone(&arrived);
        // Every request waits for the test to open the gate.
        let m = fake_manager(move |items| {
            let gate = Arc::clone(&sink_gate);
            let sink = Arc::clone(&sink);
            let arrived = Arc::clone(&sink_arrived);
            async move {
                arrived.fetch_add(1, Ordering::SeqCst);
                let permit = gate.acquire().await.expect("gate stays open");
                permit.forget();
                sink.attempt(&items, true)
            }
        });

        let addr = address("orders", "Default");
        let o = BufferOptions {
            message_count: 2,
            time: Duration::from_secs(60),
            max_size: 4,
            retry_delay: Duration::from_millis(5),
        };
        for i in 0..4 {
            m.add(addr.clone(), item(i), o).await.unwrap();
        }
        until(
            || arrived.load(Ordering::SeqCst) == 1,
            "the first batch to reach the (stalled) sink",
        )
        .await;

        // Four messages are accounted for: two in flight, two waiting. The
        // fifth has nowhere to go.
        let mut blocked = Box::pin(m.add(addr.clone(), item(4), o));
        let parked = tokio::time::timeout(Duration::from_millis(80), &mut blocked).await;
        assert!(
            parked.is_err(),
            "add returned while the buffer was at its bound: that is the unbounded defect"
        );
        assert_eq!(
            m.stats().total_buffered_messages,
            2,
            "the buffer grew past its bound while an add was parked"
        );

        // Draining frees permits, which is what wakes the parked add.
        gate.add_permits(4);
        blocked.await.expect("the parked add must succeed once room frees");
        until(|| recorder.sent().len() >= 4, "the stalled batches to land").await;
        assert_eq!(recorder.sent()[..4], [0, 1, 2, 3]);
    }

    // Cancellation: a caller that gives up gets an error, never a silent
    // success, and leaves nothing behind in the buffer.
    #[tokio::test]
    async fn a_blocked_add_that_times_out_reports_it() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let sink_gate = Arc::clone(&gate);
        let m = fake_manager(move |_| {
            let gate = Arc::clone(&sink_gate);
            async move {
                let _ = gate.acquire().await;
                Ok(Vec::new())
            }
        });

        let addr = address("orders", "Default");
        // max_size 4 = the two the stalled sink is holding plus two waiting;
        // the bound covers messages in flight, so a fifth has nowhere to go.
        let o = BufferOptions {
            message_count: 2,
            time: Duration::from_secs(60),
            max_size: 4,
            retry_delay: Duration::from_millis(5),
        };
        for i in 0..4 {
            m.add(addr.clone(), item(i), o).await.unwrap();
        }

        let outcome =
            tokio::time::timeout(Duration::from_millis(60), m.add(addr.clone(), item(9), o)).await;
        assert!(outcome.is_err(), "a blocked add must not report success");
        assert_eq!(
            m.stats().total_buffered_messages,
            2,
            "an abandoned add must not leave its message in the buffer"
        );
        assert_eq!(
            m.buffers.lock().unwrap().get(&addr).map(|b| b.parked),
            Some(0),
            "the parked count must come back down when the add is cancelled"
        );
    }

    // stop() is what shutdown calls. A producer parked on the bound has to be
    // released — and told that its message was never buffered.
    #[tokio::test]
    async fn stop_wakes_parked_adds_with_an_error() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let sink_gate = Arc::clone(&gate);
        let m = fake_manager(move |_| {
            let gate = Arc::clone(&sink_gate);
            async move {
                let _ = gate.acquire().await;
                Ok(Vec::new())
            }
        });

        let addr = address("orders", "Default");
        let o = BufferOptions {
            message_count: 2,
            time: Duration::from_secs(60),
            max_size: 4,
            retry_delay: Duration::from_millis(5),
        };
        for i in 0..4 {
            m.add(addr.clone(), item(i), o).await.unwrap();
        }

        let waiting = tokio::spawn({
            let m = Arc::clone(&m);
            let addr = addr.clone();
            async move { m.add(addr, item(9), o).await }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!waiting.is_finished(), "precondition: the add is parked");

        let unsent = m.stop();
        let outcome = tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .expect("stop() must not leave a producer parked forever")
            .expect("the add task panicked");
        assert!(
            outcome.is_err(),
            "a parked add woken by stop() must report that its message was not buffered"
        );
        assert_eq!(unsent, 2, "stop() must say how much never reached the broker");
        assert!(
            m.add(addr, item(10), o).await.is_err(),
            "a stopped manager must refuse new messages instead of buffering them forever"
        );
    }

    // One drain per address, no matter how many adds trip the threshold. Before
    // the guard, every add past `message_count` spawned another flush task —
    // under overload, one spawned task per message.
    #[tokio::test]
    async fn adds_past_the_threshold_do_not_stack_up_flush_tasks() {
        let concurrent = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (c, p) = (Arc::clone(&concurrent), Arc::clone(&peak));
        let m = fake_manager(move |_| {
            let c = Arc::clone(&c);
            let p = Arc::clone(&p);
            async move {
                let now = c.fetch_add(1, Ordering::SeqCst) + 1;
                p.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                c.fetch_sub(1, Ordering::SeqCst);
                Ok(Vec::new())
            }
        });

        let addr = address("orders", "Default");
        let o = BufferOptions {
            message_count: 2,
            time: Duration::from_secs(60),
            max_size: 100,
            retry_delay: Duration::from_millis(5),
        };
        for i in 0..40 {
            m.add(addr.clone(), item(i), o).await.unwrap();
        }
        until(|| m.stats().total_buffered_messages == 0, "the drain").await;

        assert_eq!(
            peak.load(Ordering::SeqCst),
            1,
            "two senders on one partition interleave batches and reorder the lane"
        );
    }

    // The loss is bounded by the chunk size in both directions: everything
    // behind the failing chunk stays put, and the chunk itself comes back.
    #[tokio::test]
    async fn a_failed_flush_keeps_the_chunks_it_had_not_sent_yet() {
        let m = impatient_manager();
        let addr = address("orders", "Default");
        seed(
            &m,
            &addr,
            250,
            BufferOptions {
                message_count: 100,
                time: Duration::from_secs(60),
                max_size: 400,
                retry_delay: Duration::from_millis(250),
            },
        );

        assert!(m.flush(&addr).await.is_err());
        assert_eq!(
            m.stats().total_buffered_messages,
            250,
            "flush must send in message_count-sized chunks and put a failed one back: nothing is \
             lost by an unreachable broker"
        );
    }

    // `flush_all` is what `Queen::close` calls on the way out. One unreachable
    // address must not strand the other addresses' messages, and the failure
    // still has to reach the caller.
    #[tokio::test]
    async fn flush_all_drains_every_address_before_reporting_the_failure() {
        let m = impatient_manager();
        let o = opts(100);
        m.add(address("orders", "eu"), item(1), o).await.unwrap();
        m.add(address("orders", "us"), item(2), o).await.unwrap();
        m.add(address("other", "eu"), item(3), o).await.unwrap();

        assert!(
            m.flush_all().await.is_err(),
            "a shutdown flush that reached nobody must not report success"
        );
        assert_eq!(
            m.stats().total_buffered_messages,
            3,
            "flush_all must try every address and keep what it could not send"
        );
        assert_eq!(m.stats().active_buffers, 3);
    }
}
