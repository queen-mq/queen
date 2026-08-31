//! The one sanctioned way to log from a per-request path.
//!
//! An SQS listener's noisiest lines are the ones a client or an attacker
//! controls: a signature that does not verify, retried by every worker in a
//! fleet on the same misconfigured clock; a health probe; a long poll that
//! answers empty twenty times a second because that is what `sqs-consumer`
//! does when a queue is idle. Logged one line per event, each of those is an
//! amplifier — the cheapest request on the listener becomes the most expensive
//! line in the log pipeline.
//!
//! So the rule the broker, the proxy and queen-kafka already follow
//! (server/src/obs.rs: "rate AND sizes, aggregated over a window") applies here
//! too, through the same primitive, deliberately kept identical to theirs so
//! that one idiom covers all four binaries.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A wall-clock window gate: at most one emit per `interval_ms` per instance,
/// process-wide, chosen by a CAS so exactly one thread wins. Returns the number
/// of events suppressed since the last emit, so the line that is printed says
/// how many it stands for.
///
/// ```ignore
/// static SIGV4_FAIL: Sampler = Sampler::new(10_000);
/// if let Some(suppressed) = SIGV4_FAIL.tick_now() {
///     warn!(target: "sqs", suppressed, "sigv4 verification failed");
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
pub fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// `2026-08-31T01:02:03.456Z` — the `Timestamp` an SNS notification carries.
///
/// THREE fractional digits, which is SNS's own shape and not the broker's: the
/// broker writes microseconds (`to_char(…, '…US"Z"')`, which
/// `queen::testing::iso_from_epoch_ms` renders), and a notification that carried
/// six would be a field no SNS consumer has ever parsed. The two formats have
/// different consumers, so they are different functions over one calendar
/// ([`civil_from_days`]).
pub fn iso8601_ms(ms: i64) -> String {
    let (days, rem) = (ms.div_euclid(86_400_000), ms.rem_euclid(86_400_000));
    let (y, mo, d) = civil_from_days(days);
    let (h, mi, s, milli) = (
        rem / 3_600_000,
        (rem / 60_000) % 60,
        (rem / 1_000) % 60,
        rem % 1_000,
    );
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{milli:03}Z")
}

/// Hinnant's `civil_from_days`, the inverse of the `days_from_civil` the receive
/// path parses timestamps with. Branch-free calendar arithmetic, exact for every
/// year this will see.
///
/// `pub(crate)` and here rather than beside either renderer, because there are
/// two renderers — SNS's millisecond shape above and the broker's microsecond
/// shape in the test double — and two copies of a calendar is how one of them
/// gets a leap year wrong.
pub(crate) fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
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

    /// The notification timestamp, against the parser that is already in the
    /// tree: what this writes, the receive path reads back to the same
    /// millisecond. A calendar tested only against itself is a calendar nobody
    /// tested.
    #[test]
    fn a_notification_timestamp_round_trips_through_the_receive_paths_parser() {
        for ms in [
            0,                 // the epoch
            1_787_011_200_000, // 2026-08-31
            1_709_164_800_123, // a leap day, with a fraction
            4_102_444_800_999, // 2100-01-01, which is not a leap year
        ] {
            let iso = iso8601_ms(ms);
            assert_eq!(
                crate::actions::messages::epoch_ms_of(&iso),
                Some(ms),
                "{iso}"
            );
        }
        assert_eq!(iso8601_ms(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso8601_ms(1_709_164_800_000), "2024-02-29T00:00:00.000Z");
        // THREE fractional digits, always — a consumer that slices the string
        // reads the same width on every message.
        assert_eq!(&iso8601_ms(1_787_011_200_007)[19..], ".007Z");
    }
}
