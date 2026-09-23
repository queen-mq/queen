//! Push admission: a byte budget for the storage-growing commands (pushes,
//! push-carrying transactions, KV puts, timer schedules) that are on their way
//! through the serial planner.
//!
//! Without it, an offered rate above the planner's ceiling piled request bodies
//! up in RAM until the kernel killed the broker (measured 2026-09-22/23 at
//! 240k–300k msg/s: 10.9 GB and an OOM kill). With it, a command takes permits
//! for its estimated bytes before it is queued and gives them back when its
//! reply arrives (or its deadline passes). When the budget is spent a new
//! command WAITS for room — the way Kafka delays a producer instead of failing
//! it, so a normal producer just slows down — and only after the hold limit is
//! it refused with `429` + `Retry-After`, which protects the broker from
//! senders that never slow down. Drain work (acks, pops) never takes permits.
//!
//! `QUEEN_RAFT_ADMIT_MAX_MB` (default 256; 0 = off) and
//! `QUEEN_RAFT_ADMIT_HOLD_MS` (default 5000).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const DEFAULT_MAX_MB: u64 = 256;
const DEFAULT_HOLD_MS: u64 = 5_000;

/// Commands admitted after waiting for room / refused after the hold.
static WAITED: AtomicU64 = AtomicU64::new(0);
static REFUSED: AtomicU64 = AtomicU64::new(0);
/// Budget and bytes currently held, for the gauges.
static CAP_BYTES: AtomicU64 = AtomicU64::new(0);
static HELD_BYTES: AtomicU64 = AtomicU64::new(0);

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

    /// Take `bytes` of budget, waiting up to the hold for room. A command
    /// larger than the whole budget takes all of it (it runs alone).
    pub async fn admit(&self, bytes: usize) -> Result<Admitted, Overloaded> {
        let n = (bytes as u64).clamp(1, self.cap as u64) as u32;
        let permit = match self.sem.clone().try_acquire_many_owned(n) {
            Ok(p) => p,
            Err(_) => {
                WAITED.fetch_add(1, Ordering::Relaxed);
                match tokio::time::timeout(self.hold, self.sem.clone().acquire_many_owned(n)).await
                {
                    Ok(Ok(p)) => p,
                    _ => {
                        REFUSED.fetch_add(1, Ordering::Relaxed);
                        return Err(Overloaded {
                            retry_after_s: self.hold.as_secs().max(1),
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
    async fn refused_after_the_hold() {
        let g = AdmitGate::new(1000, Duration::from_millis(100));
        let _a = g.admit(1000).await.expect("room");
        let t0 = std::time::Instant::now();
        let r = g.admit(10).await;
        assert!(r.is_err(), "no room within the hold must refuse");
        assert!(t0.elapsed() >= Duration::from_millis(100), "it held first");
    }

    #[tokio::test]
    async fn a_command_larger_than_the_budget_runs_alone() {
        let g = AdmitGate::new(1000, Duration::from_millis(100));
        let big = g.admit(5000).await.expect("clamped to the whole budget");
        assert!(g.admit(1).await.is_err(), "nothing else fits while it runs");
        drop(big);
        assert!(g.admit(1).await.is_ok());
    }
}
