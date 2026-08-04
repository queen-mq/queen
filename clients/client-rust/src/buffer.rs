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

    fn item(n: u64) -> PushItem {
        PushItem::new("orders", serde_json::json!({ "n": n }))
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
        assert!(err.is_err(), "unreachable broker must not look like success");
    }
}
