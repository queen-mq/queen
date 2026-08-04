//! Client-side push batching.
//!
//! Accumulates messages per `(queue, partition)` and sends them in one request
//! once either threshold trips. This trades latency for throughput on the
//! producer side and is entirely optional — an unbuffered push goes out
//! immediately.
//!
//! Buffered messages live in this process's memory. A crash before the flush
//! loses them, so buffering belongs on telemetry-shaped traffic, not on
//! anything that must not be lost. [`crate::Queen::close`] flushes before
//! returning, which covers an orderly shutdown.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use queen_protocol::{PushItem, PushRequest, PushResult};

use crate::error::Result;
use crate::http::{HttpClient, Opts};

/// When a buffer flushes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferOptions {
    /// Flush once this many messages are waiting.
    pub message_count: usize,
    /// Flush this long after the first message arrives, however few there are.
    pub time: Duration,
}

impl Default for BufferOptions {
    fn default() -> Self {
        Self {
            message_count: 100,
            time: Duration::from_millis(1000),
        }
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

struct Buffer {
    items: Vec<PushItem>,
    opts: BufferOptions,
    first_at: Option<Instant>,
    /// Set while a timer task is armed for this address, so repeated adds do
    /// not stack up one timer per message.
    timer_armed: bool,
}

pub(crate) struct BufferManager {
    http: Arc<HttpClient>,
    buffers: Mutex<HashMap<String, Buffer>>,
    flush_count: AtomicU64,
}

impl BufferManager {
    pub fn new(http: Arc<HttpClient>) -> Arc<Self> {
        Arc::new(Self {
            http,
            buffers: Mutex::new(HashMap::new()),
            flush_count: AtomicU64::new(0),
        })
    }

    /// Queue a message. Returns true when this add tripped the size threshold
    /// and a flush was started.
    pub fn add(self: &Arc<Self>, address: String, item: PushItem, opts: BufferOptions) {
        let (should_flush, arm_timer) = {
            let mut buffers = self.buffers.lock().unwrap();
            let buf = buffers.entry(address.clone()).or_insert_with(|| Buffer {
                items: Vec::new(),
                opts,
                first_at: None,
                timer_armed: false,
            });
            if buf.items.is_empty() {
                buf.first_at = Some(Instant::now());
            }
            buf.items.push(item);

            let should_flush = buf.items.len() >= buf.opts.message_count;
            let arm_timer = !should_flush && !buf.timer_armed;
            if arm_timer {
                buf.timer_armed = true;
            }
            (should_flush, arm_timer)
        };

        if should_flush {
            let this = Arc::clone(self);
            let addr = address;
            tokio::spawn(async move {
                if let Err(e) = this.flush(&addr).await {
                    tracing::error!(address = %addr, error = %e, "buffer flush failed");
                }
            });
        } else if arm_timer {
            let this = Arc::clone(self);
            let addr = address;
            let delay = opts.time;
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                {
                    let mut buffers = this.buffers.lock().unwrap();
                    if let Some(b) = buffers.get_mut(&addr) {
                        b.timer_armed = false;
                    }
                }
                if let Err(e) = this.flush(&addr).await {
                    tracing::error!(address = %addr, error = %e, "buffer flush failed");
                }
            });
        }
    }

    /// Send everything waiting for one address, in `message_count`-sized
    /// requests.
    pub async fn flush(&self, address: &str) -> Result<Vec<PushResult>> {
        let mut all = Vec::new();
        loop {
            // Drain under the lock, send outside it: holding a std Mutex across
            // an await would both deadlock the runtime and serialize producers.
            let chunk = {
                let mut buffers = self.buffers.lock().unwrap();
                let Some(buf) = buffers.get_mut(address) else {
                    break;
                };
                if buf.items.is_empty() {
                    buffers.remove(address);
                    break;
                }
                let n = buf.opts.message_count.min(buf.items.len()).max(1);
                let chunk: Vec<PushItem> = buf.items.drain(..n).collect();
                if buf.items.is_empty() {
                    buf.first_at = None;
                }
                chunk
            };

            if chunk.is_empty() {
                break;
            }

            let req = PushRequest::new(chunk);
            let results: Option<Vec<PushResult>> = self
                .http
                .post_json("/api/v1/push", &req, &Opts::default())
                .await?;
            self.flush_count.fetch_add(1, Ordering::Relaxed);
            all.extend(results.unwrap_or_default());
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
}

/// The key a buffer is held under. Batching is per `(queue, partition)` because
/// that is the granularity the broker fuses on — mixing partitions in one
/// buffer would not make the write any cheaper.
pub(crate) fn address(queue: &str, partition: &str) -> String {
    format!("{queue}/{partition}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

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

    fn item(n: u64) -> PushItem {
        PushItem::new("orders", serde_json::json!({ "n": n }))
    }

    /// Put items in a buffer without going through `add`, which flushes on the
    /// way past `message_count` and so cannot build a buffer big enough to make
    /// `flush` send more than one chunk.
    fn seed(m: &Arc<BufferManager>, address: &str, count: u64, opts: BufferOptions) {
        let mut buffers = m.buffers.lock().unwrap();
        buffers.insert(
            address.to_string(),
            Buffer {
                items: (0..count).map(item).collect(),
                opts,
                first_at: Some(Instant::now()),
                timer_armed: false,
            },
        );
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
    }

    #[tokio::test]
    async fn accumulates_below_the_threshold() {
        let m = manager();
        let opts = BufferOptions {
            message_count: 10,
            time: Duration::from_secs(60),
        };
        for i in 0..4 {
            m.add(address("orders", "Default"), item(i), opts);
        }
        let s = m.stats();
        assert_eq!(s.active_buffers, 1);
        assert_eq!(s.total_buffered_messages, 4);
        assert_eq!(s.flushes_performed, 0);
    }

    #[tokio::test]
    async fn separate_partitions_buffer_separately() {
        let m = manager();
        let opts = BufferOptions {
            message_count: 10,
            time: Duration::from_secs(60),
        };
        m.add(address("orders", "eu"), item(1), opts);
        m.add(address("orders", "us"), item(2), opts);
        m.add(address("other", "eu"), item(3), opts);
        assert_eq!(m.stats().active_buffers, 3);
    }

    #[tokio::test]
    async fn oldest_age_tracks_the_first_message() {
        let m = manager();
        let opts = BufferOptions {
            message_count: 100,
            time: Duration::from_secs(60),
        };
        m.add(address("orders", "Default"), item(1), opts);
        tokio::time::sleep(Duration::from_millis(30)).await;
        m.add(address("orders", "Default"), item(2), opts);
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
        let opts = BufferOptions {
            message_count: 100,
            time: Duration::from_secs(60),
        };
        m.add(address("orders", "Default"), item(1), opts);
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
        let m = impatient_manager();
        let opts = BufferOptions {
            message_count: 1_000,
            time: Duration::from_millis(80),
        };
        let addr = address("orders", "Default");
        m.add(addr.clone(), item(1), opts);
        m.add(addr.clone(), item(2), opts);

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
        let m = impatient_manager();
        let opts = BufferOptions {
            message_count: 1_000,
            time: Duration::from_millis(80),
        };
        let addr = address("orders", "Default");

        m.add(addr.clone(), item(1), opts);
        assert!(timer_armed(&m, &addr));
        m.add(addr.clone(), item(2), opts);
        assert!(
            timer_armed(&m, &addr),
            "an add while a timer is already armed must leave the flag alone, not schedule a \
             second flush"
        );

        assert_eq!(buffered_settles_at(&m, 0).await, 0, "first flush never ran");
        assert!(
            !timer_armed(&m, &addr),
            "the timer task must clear the flag before flushing, or the next batch waits forever"
        );

        m.add(addr.clone(), item(3), opts);
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
        m.add(
            addr.clone(),
            item(0),
            BufferOptions {
                message_count: 1_000,
                time: Duration::from_secs(60),
            },
        );
        for i in 1..3 {
            m.add(
                addr.clone(),
                item(i),
                BufferOptions {
                    message_count: 2,
                    time: Duration::from_secs(60),
                },
            );
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

    // `flush` takes its chunk out of the buffer *before* the request and a
    // failure does not put it back, so a flush that never reaches the broker
    // loses those messages. Measured rather than assumed, because it is the
    // difference between "buffering is a latency knob" and "buffering can drop
    // data on a blip" — the module doc only warns about the crash case.
    #[tokio::test]
    async fn a_failed_flush_drops_the_chunk_it_had_already_taken() {
        let m = impatient_manager();
        let opts = BufferOptions {
            message_count: 100,
            time: Duration::from_secs(60),
        };
        let addr = address("orders", "Default");
        for i in 0..3 {
            m.add(addr.clone(), item(i), opts);
        }

        assert!(
            m.flush(&addr).await.is_err(),
            "an unreachable broker must not look like success"
        );

        let s = m.stats();
        assert_eq!(
            s.total_buffered_messages, 0,
            "the chunk was not returned to the buffer, so those three messages are gone"
        );
        assert_eq!(
            s.flushes_performed, 0,
            "a flush that never reached the broker must not be counted as one"
        );
        assert_eq!(
            s.active_buffers, 1,
            "the emptied buffer entry survives the failure (only a successful flush removes it)"
        );
        assert_eq!(
            s.oldest_buffer_age,
            Duration::ZERO,
            "an empty buffer has no oldest message"
        );
    }

    // The loss above is bounded by the chunk size, not by the buffer size: the
    // send loop drains `message_count` at a time, so everything behind the
    // failing chunk is still there to retry.
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
            },
        );

        assert!(m.flush(&addr).await.is_err());
        assert_eq!(
            m.stats().total_buffered_messages,
            150,
            "flush must send in message_count-sized chunks: only the 100 in flight are lost"
        );
    }

    // `flush_all` is what `Queen::close` calls on the way out. One unreachable
    // address must not strand the other addresses' messages, and the failure
    // still has to reach the caller.
    #[tokio::test]
    async fn flush_all_drains_every_address_before_reporting_the_failure() {
        let m = impatient_manager();
        let opts = BufferOptions {
            message_count: 100,
            time: Duration::from_secs(60),
        };
        m.add(address("orders", "eu"), item(1), opts);
        m.add(address("orders", "us"), item(2), opts);
        m.add(address("other", "eu"), item(3), opts);

        assert!(
            m.flush_all().await.is_err(),
            "a shutdown flush that reached nobody must not report success"
        );
        assert_eq!(
            m.stats().total_buffered_messages,
            0,
            "flush_all stopped at the first failing address instead of trying the rest"
        );
    }
}
