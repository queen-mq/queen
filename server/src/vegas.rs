use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// Adaptive concurrency limiter (TCP-Vegas style, à la Netflix concurrency-limits).
// A permit gates each in-flight DB op; on completion the observed RTT updates the
// limit: estimate the queued work q = limit * (1 - rtt_base/rtt); grow the limit
// when q <= alpha (we're not queueing), shrink when q >= beta (PG is backing up).
// The limit is materialised as a dynamically-resized tokio Semaphore.

// rtt_base window: 60 one-second slots (doc 18 §10). Each slot holds the minimum
// RTT observed during that wall-clock second; rtt_base = min over the populated,
// non-expired slots. Unlike the old slow-rising EWMA minimum, slots EXPIRE as
// time advances, so when the cheap ops disappear (e.g. the workload shifts from
// tiny acks to heavy pushes) the base rises within a minute instead of anchoring
// the limit to a no-longer-representative floor.
const RTT_WINDOW_SLOTS: usize = 60;

pub struct Vegas {
    sem: Arc<Semaphore>,
    permits: AtomicU64,      // permits currently issued to the semaphore
    inner: Mutex<Calc>,
    min: u64,
    max: u64,
    alpha: f64,
    beta: f64,
    // Wall-clock anchor for slot indexing; all slot times are seconds since this.
    anchor: Instant,
    // Test hook: simulated seconds added to the anchor-relative clock so unit
    // tests can rotate the window without sleeping.
    #[cfg(test)]
    test_advance_sec: AtomicU64,
}

#[derive(Clone, Copy)]
struct SlotMin {
    sec: u64,    // absolute second (since anchor) this slot was last written; u64::MAX = never
    min_ns: u64, // minimum RTT observed during that second
}

struct Calc {
    limit: f64,
    slots: [SlotMin; RTT_WINDOW_SLOTS], // per-second RTT minima ring (rtt_base window)
    in_flight: i64,
}

impl Vegas {
    pub fn new(initial: u64, min: u64, max: u64, alpha: f64, beta: f64) -> Arc<Vegas> {
        let init = initial.clamp(min, max);
        Arc::new(Vegas {
            sem: Arc::new(Semaphore::new(init as usize)),
            permits: AtomicU64::new(init),
            inner: Mutex::new(Calc {
                limit: init as f64,
                slots: [SlotMin { sec: u64::MAX, min_ns: u64::MAX }; RTT_WINDOW_SLOTS],
                in_flight: 0,
            }),
            min,
            max,
            alpha,
            beta,
            anchor: Instant::now(),
            #[cfg(test)]
            test_advance_sec: AtomicU64::new(0),
        })
    }

    // Seconds since construction (plus the simulated offset under test).
    fn now_sec(&self) -> u64 {
        #[allow(unused_mut)]
        let mut s = self.anchor.elapsed().as_secs();
        #[cfg(test)]
        {
            s += self.test_advance_sec.load(Ordering::Relaxed);
        }
        s
    }

    // Minimum over the ring's populated slots that are still inside the window.
    // Slots older than RTT_WINDOW_SLOTS seconds are expired (they would be
    // overwritten on their next touch anyway; the age check covers idle gaps
    // where a stale slot was never revisited). Returns 1 when nothing is
    // populated so callers never divide by zero.
    fn windowed_base(slots: &[SlotMin; RTT_WINDOW_SLOTS], now_sec: u64) -> u64 {
        let mut m = u64::MAX;
        for s in slots {
            if s.sec != u64::MAX
                && now_sec.saturating_sub(s.sec) < RTT_WINDOW_SLOTS as u64
                && s.min_ns < m
            {
                m = s.min_ns;
            }
        }
        if m == u64::MAX {
            1
        } else {
            m
        }
    }

    // Await a slot. Returned permit must be held for the whole op.
    pub async fn acquire(self: &Arc<Self>) -> OwnedSemaphorePermit {
        let p = self.sem.clone().acquire_owned().await.expect("sem closed");
        self.inner.lock().unwrap().in_flight += 1;
        p
    }

    // Take a slot WITHOUT waiting. Returns None when no permit is immediately
    // available (the lane is contended). Fusion's fire-on-idle path uses this:
    // an available permit means the push lane is idle enough to flush a low-rate
    // push right now instead of arming the hold timer. Accounting matches
    // `acquire` (in_flight++ on success; `record` decrements), so a permit taken
    // here has the identical lifecycle to one awaited.
    pub fn try_acquire(self: &Arc<Self>) -> Option<OwnedSemaphorePermit> {
        match self.sem.clone().try_acquire_owned() {
            Ok(p) => {
                self.inner.lock().unwrap().in_flight += 1;
                Some(p)
            }
            Err(_) => None,
        }
    }

    pub fn record(&self, rtt: Duration) {
        let rtt_ns = (rtt.as_nanos() as u64).max(1);
        let now_sec = self.now_sec();
        let mut c = self.inner.lock().unwrap();
        c.in_flight -= 1;
        // Update the current one-second slot's minimum. A slot whose stamp isn't
        // this second is stale (any prior 60s-old content expired) → overwrite.
        let idx = (now_sec as usize) % RTT_WINDOW_SLOTS;
        let slot = &mut c.slots[idx];
        if slot.sec != now_sec {
            *slot = SlotMin { sec: now_sec, min_ns: rtt_ns };
        } else if rtt_ns < slot.min_ns {
            slot.min_ns = rtt_ns;
        }
        let base = Self::windowed_base(&c.slots, now_sec) as f64;
        let rtt_ns = (rtt_ns as f64).max(1.0);
        let base = base.max(1.0);
        let limit = c.limit;
        let queue = limit * (1.0 - base / rtt_ns);
        // Only adjust when we are actually driving the limit (avoids ramping on
        // an idle system). log-scaled step keeps it stable at high limits.
        let step = (limit.ln()).max(1.0);
        let new_limit = if (c.in_flight as f64) >= limit - 1.0 {
            if queue <= self.alpha {
                limit + step
            } else if queue >= self.beta {
                limit - step
            } else {
                limit
            }
        } else {
            limit
        };
        let new_limit = new_limit.clamp(self.min as f64, self.max as f64);
        c.limit = new_limit;
        let target = new_limit as u64;
        drop(c);
        self.set_permits(target);
    }

    fn set_permits(&self, target: u64) {
        let cur = self.permits.load(Ordering::Relaxed);
        if target > cur {
            self.sem.add_permits((target - cur) as usize);
            self.permits.store(target, Ordering::Relaxed);
        } else if target < cur {
            let removed = self.sem.forget_permits((cur - target) as usize);
            if removed > 0 {
                self.permits.fetch_sub(removed as u64, Ordering::Relaxed);
            }
        }
    }

    pub fn limit(&self) -> u64 {
        self.permits.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Vegas {
        fn test_advance(&self, secs: u64) {
            self.test_advance_sec.fetch_add(secs, Ordering::Relaxed);
        }
        fn test_base_ns(&self) -> u64 {
            let now = self.now_sec();
            Vegas::windowed_base(&self.inner.lock().unwrap().slots, now)
        }
    }

    fn observe(v: &Arc<Vegas>, rtt: Duration) {
        let p = v.try_acquire().expect("permit available");
        v.record(rtt);
        drop(p);
    }

    #[test]
    fn rtt_base_rises_when_cheap_ops_disappear() {
        let v = Vegas::new(16, 4, 64, 3.0, 6.0);

        // A 1ms observation sets the floor.
        observe(&v, Duration::from_millis(1));
        assert_eq!(v.test_base_ns(), 1_000_000);

        // 30 simulated seconds later the 1ms slot is still inside the window:
        // 5ms observations must NOT raise the base yet.
        v.test_advance(30);
        observe(&v, Duration::from_millis(5));
        assert_eq!(v.test_base_ns(), 1_000_000);

        // Only 5ms RTTs from here on. Once the window has fully rotated past
        // the 1ms slot (>60 simulated seconds), the base rises to 5ms — the old
        // EWMA estimator would have stayed pinned near 1ms for hours.
        for _ in 0..61 {
            v.test_advance(1);
            observe(&v, Duration::from_millis(5));
        }
        assert_eq!(v.test_base_ns(), 5_000_000);
    }

    #[test]
    fn rtt_base_is_min_within_window() {
        let v = Vegas::new(16, 4, 64, 3.0, 6.0);
        observe(&v, Duration::from_millis(5));
        observe(&v, Duration::from_millis(2));
        observe(&v, Duration::from_millis(9));
        assert_eq!(v.test_base_ns(), 2_000_000);
        // A cheaper op in a later second lowers the base immediately.
        v.test_advance(3);
        observe(&v, Duration::from_millis(1));
        assert_eq!(v.test_base_ns(), 1_000_000);
    }
}
