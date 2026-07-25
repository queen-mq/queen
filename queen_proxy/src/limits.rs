//! Rate limiting + parked-consumer gauges. OWNER: Agent D.
//! Design: rev 2.3 T4a ported — dual token bucket per cluster (capacity =
//! burst column, refill = sustained column), sharded lock map keyed by
//! cluster_id; per-cluster AtomicI64 parked gauge with RAII release; shadow
//! mode when !cfg.enforce (decide, log, meter — but always Allow).
//!
//! Skeleton: everything allows; the Decision/Guard contracts are final.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use uuid::Uuid;

use crate::state::ClusterCtx;

pub enum Decision {
    Allow,
    /// Deny with Retry-After seconds. Callers map to err_429.
    Deny { retry_after_s: u64, code: &'static str },
}

pub struct ParkedGuard {
    gauge: Option<Arc<AtomicI64>>,
}

impl Drop for ParkedGuard {
    fn drop(&mut self) {
        if let Some(g) = self.gauge.take() {
            g.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

pub struct Limits {
    enforce: bool,
    /// cell-wide parked cap protecting the broker (5k comfortable on a free cell)
    cell_parked: Arc<AtomicI64>,
    cell_parked_max: i64,
}

impl Limits {
    pub fn new(cfg: &crate::config::Config) -> Limits {
        Limits {
            enforce: cfg.enforce,
            cell_parked: Arc::new(AtomicI64::new(0)),
            cell_parked_max: crate::config::env_u64("QUEEN_PROXY_CELL_MAX_PARKED", 5000) as i64,
        }
    }

    pub fn enforcing(&self) -> bool {
        self.enforce
    }

    /// One request token. Call once per proxied request, post-authn.
    pub fn check_req(&self, _ctx: &ClusterCtx) -> Decision {
        // Agent D: dual GCRA (burst+sustained) on ctx.limits.{max_req_per_sec,req_burst}
        Decision::Allow
    }

    /// n message tokens (push items / transaction push-ops). May also be
    /// debited post-completion for deliveries (bucket may go negative).
    pub fn check_msgs(&self, _ctx: &ClusterCtx, _n: u64) -> Decision {
        Decision::Allow
    }

    pub fn debit_deliveries(&self, _ctx: &ClusterCtx, _n: u64) {
        // post-completion debit, never denies (rev 2.3: parked pops exempt at entry)
    }

    /// Acquire a parked-consumer slot (wait=true pop). Err -> 429.
    pub fn parked_slot(&self, ctx: &ClusterCtx) -> Result<ParkedGuard, Decision> {
        let cur = self.cell_parked.fetch_add(1, Ordering::Relaxed);
        if cur >= self.cell_parked_max && self.enforce {
            self.cell_parked.fetch_sub(1, Ordering::Relaxed);
            return Err(Decision::Deny { retry_after_s: 5, code: crate::errors::CODE_RATE_LIMITED });
        }
        let _ = ctx; // Agent D: per-cluster gauge vs ctx.limits.max_parked_pops
        Ok(ParkedGuard { gauge: Some(self.cell_parked.clone()) })
    }

    /// Storage quota state, updated by the registry reconciler.
    pub fn set_push_blocked(&self, _cluster_id: Uuid, _blocked: bool) {}
}
