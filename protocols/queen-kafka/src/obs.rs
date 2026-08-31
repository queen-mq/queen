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
