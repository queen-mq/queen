use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// Adaptive concurrency limiter (TCP-Vegas style, à la Netflix concurrency-limits).
// A permit gates each in-flight DB op; on completion the observed RTT updates the
// limit: estimate the queued work q = limit * (1 - rtt_base/rtt); grow the limit
// when q <= alpha (we're not queueing), shrink when q >= beta (PG is backing up).
// The limit is materialised as a dynamically-resized tokio Semaphore.
pub struct Vegas {
    sem: Arc<Semaphore>,
    permits: AtomicU64,      // permits currently issued to the semaphore
    inner: Mutex<Calc>,
    min: u64,
    max: u64,
    alpha: f64,
    beta: f64,
}

struct Calc {
    limit: f64,
    rtt_base_ns: f64, // estimated no-load RTT (slow-rising minimum)
    in_flight: i64,
}

impl Vegas {
    pub fn new(initial: u64, min: u64, max: u64, alpha: f64, beta: f64) -> Arc<Vegas> {
        let init = initial.clamp(min, max);
        Arc::new(Vegas {
            sem: Arc::new(Semaphore::new(init as usize)),
            permits: AtomicU64::new(init),
            inner: Mutex::new(Calc { limit: init as f64, rtt_base_ns: 0.0, in_flight: 0 }),
            min,
            max,
            alpha,
            beta,
        })
    }

    // Await a slot. Returned permit must be held for the whole op.
    pub async fn acquire(self: &Arc<Self>) -> OwnedSemaphorePermit {
        let p = self.sem.clone().acquire_owned().await.expect("sem closed");
        self.inner.lock().unwrap().in_flight += 1;
        p
    }

    pub fn record(&self, rtt: Duration) {
        let rtt_ns = rtt.as_nanos() as f64;
        let mut c = self.inner.lock().unwrap();
        c.in_flight -= 1;
        // no-load RTT: fast to fall, very slow to rise (adapts to true baseline).
        if c.rtt_base_ns == 0.0 || rtt_ns < c.rtt_base_ns {
            c.rtt_base_ns = rtt_ns;
        } else {
            c.rtt_base_ns += (rtt_ns - c.rtt_base_ns) * 0.0005;
        }
        let rtt_ns = rtt_ns.max(1.0);
        let base = c.rtt_base_ns.max(1.0);
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
