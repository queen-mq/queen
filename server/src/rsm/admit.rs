//! Push admission: a byte budget for the storage-growing commands on their way
//! through the serial planner.
//!
//! Without it, an offered rate above the planner's ceiling piled request bodies
//! up in RAM until the kernel killed the broker (measured 2026-09-22/23 at
//! 240k–300k msg/s: 10 GB and rising, then an OOM kill).
//!
//! **Where.** An HTTP push or transaction takes permits at the edge, sized by
//! its `Content-Length`, BEFORE its body is read (`admit_edge` in
//! `handlers::raft`). A push that waits for room holds only its connection: its
//! bytes stay in the socket and TCP slows the sender down — what Kafka does by
//! not reading a muted channel. The facade (`push_impl`, `submit`) gates the
//! callers that do not come through that edge (in-process ones, transactions,
//! KV puts, timer schedules) and skips the requests the edge already admitted
//! ([`pre_admitted`]). Permits are held until the reply arrives. Drain work
//! (acks, pops) never takes permits.
//!
//! **Hold, then 429.** When the budget is spent a new command WAITS for room,
//! so a normal producer just slows down. Only after the hold is it refused with
//! `429` + `Retry-After`, which protects the broker from senders that never
//! slow down. Both are jittered: the hold by ±25% and `Retry-After` over 1–5 s.
//! Measured 2026-09-23 at 300k offered with a fixed 5 s hold and a fixed
//! `Retry-After: 5`: requests that queued together were refused together and
//! came back together (the SDKs retry a 429 by themselves), and every wave
//! reconnected thousands of sockets at once and stalled pops and acks for
//! 2–4 s.
//!
//! `QUEEN_RAFT_ADMIT_MAX_MB` (default 64; 0 = off) and
//! `QUEEN_RAFT_ADMIT_HOLD_MS` (default 15000, under the SDKs' 30 s request
//! timeout).

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rand::Rng;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// In raw request bytes: a parsed push costs several times its wire size, so
/// 64 MB of wire is roughly half a GB in flight (~2,000 pushes of 100 x 300 B,
/// far more concurrency than 300k msg/s needs).
const DEFAULT_MAX_MB: u64 = 64;
const DEFAULT_HOLD_MS: u64 = 15_000;

/// The size charged to a request that has no `Content-Length` (chunked).
pub const UNKNOWN_LEN_BYTES: usize = 64 * 1024;

/// Commands admitted after waiting for room / refused after the hold.
static WAITED: AtomicU64 = AtomicU64::new(0);
static REFUSED: AtomicU64 = AtomicU64::new(0);
/// Budget, bytes currently held, and commands waiting for room, for the gauges.
static CAP_BYTES: AtomicU64 = AtomicU64::new(0);
static HELD_BYTES: AtomicU64 = AtomicU64::new(0);
static WAITING: AtomicU64 = AtomicU64::new(0);

/// The budget was spent for the whole hold: refuse, retry after this long.
#[derive(Debug, Clone, Copy)]
pub struct Overloaded {
    pub retry_after_s: u64,
}

pub struct AdmitGate {
    sem: Arc<Semaphore>,
    cap: u32,
    hold: Duration,
}

/// A command's admission: its bytes return to the budget when this drops.
pub struct Admitted {
    _permit: OwnedSemaphorePermit,
    bytes: u64,
}

impl Drop for Admitted {
    fn drop(&mut self) {
        HELD_BYTES.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

/// Decrements the waiting gauge however the wait ends (admitted, refused, or
/// the request dropped).
struct Waiting;

impl Drop for Waiting {
    fn drop(&mut self) {
        WAITING.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The process's gate, shared by the HTTP edge and every facade, from the
/// environment on first use; `None` when `QUEEN_RAFT_ADMIT_MAX_MB=0`.
pub fn global() -> Option<&'static AdmitGate> {
    static GATE: OnceLock<Option<AdmitGate>> = OnceLock::new();
    GATE.get_or_init(AdmitGate::from_env).as_ref()
}

tokio::task_local! {
    static PRE_ADMITTED: ();
}

/// Run `f` as a request the HTTP edge already admitted: the facade's gate lets
/// it through without taking permits a second time.
pub async fn pre_admitted_scope<F: Future>(f: F) -> F::Output {
    PRE_ADMITTED.scope((), f).await
}

/// Inside [`pre_admitted_scope`].
pub fn pre_admitted() -> bool {
    PRE_ADMITTED.try_with(|_| ()).is_ok()
}

impl AdmitGate {
    /// The gate from the environment; `None` when `QUEEN_RAFT_ADMIT_MAX_MB=0`.
    pub fn from_env() -> Option<AdmitGate> {
        let mb = env_u64("QUEEN_RAFT_ADMIT_MAX_MB").unwrap_or(DEFAULT_MAX_MB);
        let hold = env_u64("QUEEN_RAFT_ADMIT_HOLD_MS").unwrap_or(DEFAULT_HOLD_MS);
        (mb > 0).then(|| AdmitGate::new(mb.saturating_mul(1 << 20), Duration::from_millis(hold)))
    }

    pub fn new(cap_bytes: u64, hold: Duration) -> AdmitGate {
        // Permits are bytes; tokio caps a semaphore below u32::MAX >> 3.
        let cap = cap_bytes.clamp(1, (u32::MAX >> 4) as u64) as u32;
        CAP_BYTES.store(cap as u64, Ordering::Relaxed);
        AdmitGate {
            sem: Arc::new(Semaphore::new(cap as usize)),
            cap,
            hold,
        }
    }

    /// Take `bytes` of budget, waiting up to the (jittered) hold for room. A
    /// command larger than the whole budget takes all of it (it runs alone).
    pub async fn admit(&self, bytes: usize) -> Result<Admitted, Overloaded> {
        let n = (bytes as u64).clamp(1, self.cap as u64) as u32;
        let permit = match self.sem.clone().try_acquire_many_owned(n) {
            Ok(p) => p,
            Err(_) => {
                WAITED.fetch_add(1, Ordering::Relaxed);
                WAITING.fetch_add(1, Ordering::Relaxed);
                let _waiting = Waiting;
                let hold = self.hold.mul_f64(rand::thread_rng().gen_range(0.75..1.25));
                match tokio::time::timeout(hold, self.sem.clone().acquire_many_owned(n)).await {
                    Ok(Ok(p)) => p,
                    _ => {
                        REFUSED.fetch_add(1, Ordering::Relaxed);
                        return Err(Overloaded {
                            retry_after_s: rand::thread_rng().gen_range(1..=5),
                        });
                    }
                }
            }
        };
        HELD_BYTES.fetch_add(n as u64, Ordering::Relaxed);
        Ok(Admitted {
            _permit: permit,
            bytes: n as u64,
        })
    }
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok().and_then(|v| v.trim().parse().ok())
}

/// Prometheus lines for the admission budget.
pub fn render(out: &mut String) {
    use std::fmt::Write;
    let _ = writeln!(
        out,
        "# HELP queen_raft_admit_bytes Admission budget and bytes held by storage-growing commands in the pipeline\n# TYPE queen_raft_admit_bytes gauge"
    );
    let _ = writeln!(
        out,
        "queen_raft_admit_bytes{{kind=\"cap\"}} {}",
        CAP_BYTES.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "queen_raft_admit_bytes{{kind=\"held\"}} {}",
        HELD_BYTES.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "# HELP queen_raft_admit_waiting Commands waiting for admission room now\n# TYPE queen_raft_admit_waiting gauge"
    );
    let _ = writeln!(
        out,
        "queen_raft_admit_waiting {}",
        WAITING.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "# HELP queen_raft_admit_total Commands that waited for admission room, and that were refused with 429\n# TYPE queen_raft_admit_total counter"
    );
    let _ = writeln!(
        out,
        "queen_raft_admit_total{{outcome=\"waited\"}} {}",
        WAITED.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "queen_raft_admit_total{{outcome=\"refused\"}} {}",
        REFUSED.load(Ordering::Relaxed)
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn under_budget_is_immediate_over_budget_waits_then_proceeds() {
        let g = AdmitGate::new(1000, Duration::from_millis(500));
        let a = g.admit(600).await.expect("room");
        // 600 + 600 > 1000: waits until `a` is released.
        let g2 = std::sync::Arc::new(g);
        let g3 = g2.clone();
        let t = tokio::spawn(async move { g3.admit(600).await.is_ok() });
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(a);
        assert!(t.await.unwrap(), "admitted once room was released");
    }

    #[tokio::test]
    async fn refused_after_the_jittered_hold_with_a_jittered_retry_after() {
        let g = AdmitGate::new(1000, Duration::from_millis(100));
        let _a = g.admit(1000).await.expect("room");
        let mut after = std::collections::HashSet::new();
        for _ in 0..40 {
            let t0 = std::time::Instant::now();
            let r = g.admit(10).await;
            let o = r.err().expect("no room within the hold must refuse");
            assert!(t0.elapsed() >= Duration::from_millis(75), "it held first");
            assert!((1..=5).contains(&o.retry_after_s));
            after.insert(o.retry_after_s);
        }
        assert!(after.len() > 1, "Retry-After is spread, not one value");
    }

    #[tokio::test]
    async fn a_command_larger_than_the_budget_runs_alone() {
        let g = AdmitGate::new(1000, Duration::from_millis(100));
        let big = g.admit(5000).await.expect("clamped to the whole budget");
        assert!(g.admit(1).await.is_err(), "nothing else fits while it runs");
        drop(big);
        assert!(g.admit(1).await.is_ok());
    }

    #[tokio::test]
    async fn pre_admitted_only_inside_the_scope() {
        assert!(!pre_admitted());
        assert!(pre_admitted_scope(async { pre_admitted() }).await);
        assert!(!pre_admitted());
    }
}
