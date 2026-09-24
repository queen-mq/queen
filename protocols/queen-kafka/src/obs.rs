//! The one sanctioned way to log from a per-connection path.
//!
//! A Kafka listener's noisiest lines are the ones an attacker or a
//! misconfigured fleet controls: a wrong password retried by two hundred
//! consumers, a health probe that opens a TCP connection and closes it, a
//! client pinned to a version this build does not speak. Logged one line per
//! event, each of those is an amplifier — the cheapest request on the listener
//! becomes the most expensive line in the log pipeline.
//!
//! So the rule the broker and the proxy already follow (server/src/obs.rs:
//! "rate AND sizes, aggregated over a window") applies here too, through the
//! same primitive, deliberately kept identical to the broker's so that one
//! idiom covers all three binaries.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A wall-clock window gate: at most one emit per `interval_ms` per instance,
/// process-wide, chosen by a CAS so exactly one thread wins. Returns the number
/// of events suppressed since the last emit, so the line that is printed says
/// how many it stands for.
///
/// ```ignore
/// static AUTH_FAIL: Sampler = Sampler::new(10_000);
/// if let Some(suppressed) = AUTH_FAIL.tick_now() {
///     warn!(target: "kafka", suppressed, "sasl authentication failed");
/// }
/// ```
pub struct Sampler {
    last_ms: AtomicI64,
    interval_ms: i64,
    suppressed: AtomicU64,
}

impl Sampler {
    pub const fn new(interval_ms: i64) -> Sampler {
        Sampler {
            last_ms: AtomicI64::new(0),
            interval_ms,
            suppressed: AtomicU64::new(0),
        }
    }

    /// `Some(suppressed_since_last)` when it is this caller's turn to emit;
    /// `None` otherwise, having counted this call as suppressed.
    pub fn tick(&self, now_ms: i64) -> Option<u64> {
        let prev = self.last_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(prev) < self.interval_ms {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        if self
            .last_ms
            .compare_exchange(prev, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        Some(self.suppressed.swap(0, Ordering::Relaxed))
    }

    /// [`Sampler::tick`] against the process clock.
    pub fn tick_now(&self) -> Option<u64> {
        self.tick(now_epoch_ms())
    }
}

/// A latency histogram that costs four relaxed atomics per sample: count, sum,
/// max and one power-of-two bucket of microseconds. Percentiles read off it are
/// bucket-accurate — within a factor of two, which is the resolution the
/// questions it answers need ("is a commit 3 ms or 300 ms, and where does the
/// time go"). Process-wide statics, reported in `GET /status` by the broker
/// that runs the facade in-process ([`crate::introspect`]).
pub struct Timing {
    count: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
    /// Bucket `i` counts samples in `[2^i, 2^(i+1))` µs (bucket 0 also takes 0).
    buckets: [AtomicU64; 40],
}

impl Timing {
    #[allow(clippy::declare_interior_mutable_const)]
    const ZERO: AtomicU64 = AtomicU64::new(0);

    pub const fn new() -> Timing {
        Timing {
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
            buckets: [Timing::ZERO; 40],
        }
    }

    pub fn record(&self, d: std::time::Duration) {
        self.record_us(d.as_micros().min(u128::from(u64::MAX)) as u64);
    }

    pub fn record_us(&self, us: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.max_us.fetch_max(us, Ordering::Relaxed);
        let i = (u64::BITS - us.max(1).leading_zeros() - 1) as usize;
        self.buckets[i.min(39)].fetch_add(1, Ordering::Relaxed);
    }

    /// `{count, meanMs, p50Ms, p99Ms, maxMs, sumUs, buckets}`; a percentile is
    /// its bucket's upper bound. The raw sum and buckets let two snapshots be
    /// subtracted into the figures of the interval between them.
    pub fn snapshot(&self) -> serde_json::Value {
        let count = self.count.load(Ordering::Relaxed);
        let buckets: Vec<u64> = self
            .buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect();
        let pct = |q: f64| -> f64 {
            let want = ((count as f64) * q).ceil().max(1.0) as u64;
            let mut seen = 0;
            for (i, n) in buckets.iter().enumerate() {
                seen += n;
                if seen >= want {
                    return (1u64 << (i + 1)) as f64 / 1000.0;
                }
            }
            0.0
        };
        let ms = |us: u64| us as f64 / 1000.0;
        serde_json::json!({
            "count": count,
            "meanMs": if count == 0 { 0.0 } else { ms(self.sum_us.load(Ordering::Relaxed)) / count as f64 },
            "p50Ms": if count == 0 { 0.0 } else { pct(0.5) },
            "p99Ms": if count == 0 { 0.0 } else { pct(0.99) },
            "maxMs": ms(self.max_us.load(Ordering::Relaxed)),
            "sumUs": self.sum_us.load(Ordering::Relaxed),
            "buckets": buckets,
        })
    }
}

impl Default for Timing {
    fn default() -> Timing {
        Timing::new()
    }
}

/// Milliseconds since the epoch, or 0 on a clock before it.
fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_emit_per_window_and_the_rest_are_counted() {
        let s = Sampler::new(1_000);
        // The first event always emits, standing for nothing but itself.
        assert_eq!(s.tick(10_000), Some(0));
        assert_eq!(s.tick(10_100), None);
        assert_eq!(s.tick(10_999), None);
        // The next window's line carries the two it stands for.
        assert_eq!(s.tick(11_000), Some(2));
        assert_eq!(s.tick(12_000), Some(0));
    }

    /// A clock that goes backwards (an NTP step) must not silence the sampler
    /// for as long as the jump.
    #[test]
    fn a_backwards_clock_does_not_wedge_it() {
        let s = Sampler::new(1_000);
        assert_eq!(s.tick(10_000), Some(0));
        assert_eq!(s.tick(9_000), None, "before the window, still suppressed");
        assert_eq!(s.tick(11_000), Some(1));
    }
}
