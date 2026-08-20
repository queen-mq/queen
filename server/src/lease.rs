//! Durable per-task maintenance scheduler over `queen.maintenance_leases`
//! (029_maintenance_leases.sql; PLAN_STATS_REFRESH.md §2 / T2.1).
//!
//! The cluster-singleton loops (stats reconciler, retained-bytes lane,
//! retention) used to be "scheduled" by per-replica timers plus a
//! `pg_try_advisory_*_lock` — mutual exclusion, not cadence, so the cluster ran
//! every task at interval/replicas and every helm value carried an "x3 FOR THE
//! REPLICA COUNT" note. Here the schedule is one durable row per task:
//!
//!   * the DB clock arbitrates (`next_due_at <= now()`) — pod clocks and pod
//!     lifecycles are irrelevant, and the configured period IS the cluster
//!     cadence, whatever `replicas:` says;
//!   * a lease bounds failover when a holder dies mid-cycle (the advisory-lock
//!     design's failure window was the TCP keepalive, ~2h11m);
//!   * a fencing token makes a resurrected holder's release a no-op.
//!
//! Loops poll cheaply (one single-row UPDATE per `poll_interval`, ~0.02 ms for
//! a loser) and run their existing cycle — INCLUDING the old advisory lock,
//! kept as belt for the mixed-fleet window where old-image pods still schedule
//! by timer — only when they win a claim.
//!
//! THE TWO TRAPS (encoded here, do not rearrange): `next_due_at` advances at
//! RELEASE only, exactly once, fixed-rate with a catch-up clamp — advancing at
//! claim time too silently halves the cadence; and the claim must always carry
//! the lease — a bare `last_start_at` stamp permits two concurrent runners.
//!
//! Claim/release deliberately take NO maintenance-lane admission slot: they are
//! bounded single-row writes at poll cadence, and burning a lane slot per probe
//! is exactly the loser-cost problem this table removes (stats.rs used to open
//! an empty BEGIN/COMMIT on a lane slot just to learn it lost the lock).

use std::time::Duration;

use deadpool_postgres::Pool;

use crate::config::Config;

/// Holder identity written into the lease row: server id (the pod hostname
/// under k8s, QUEEN_SERVER_ID otherwise) plus pid, so two brokers on one host
/// stay distinguishable.
pub fn holder_id(cfg: &Config) -> String {
    format!("{}#{}", cfg.sync.server_id, std::process::id())
}

/// How often a loop probes its row. A tenth of the period keeps the takeover
/// latency after a lease expiry small against the period itself, and doubles as
/// the anti-spin floor between back-to-back cycles when the task is overdue
/// (the sleep-floor policy PLAN_STATS_REFRESH.md T0.3 asked for): after a run,
/// the loop sleeps at least this long before probing again.
pub fn poll_interval(period: Duration) -> Duration {
    (period / 10).clamp(Duration::from_secs(1), Duration::from_secs(60))
}

/// Lease duration for a task of the given period: 2x the period, floored at
/// 60 s (a dev-tier 5 s retention period must still survive a slow cycle
/// without being stolen) and capped at 30 min. The lease must comfortably
/// exceed a genuine cycle; a stolen lease is not a double-run (the belt
/// advisory lock still excludes overlap) but it is schedule churn worth a WARN.
pub fn lease_ms(period_ms: u64) -> u64 {
    (period_ms.saturating_mul(2)).clamp(60_000, 1_800_000)
}

const ENSURE_SQL: &str = "\
    INSERT INTO queen.maintenance_leases AS t (task, period_ms) VALUES ($1, $2) \
    ON CONFLICT (task) DO UPDATE \
       SET period_ms   = EXCLUDED.period_ms, \
           next_due_at = LEAST(t.next_due_at, \
                               now() + EXCLUDED.period_ms * interval '1 millisecond')";

const CLAIM_SQL: &str = "\
    UPDATE queen.maintenance_leases t \
       SET lease_until   = now() + $2::bigint * interval '1 millisecond', \
           holder        = $3, \
           fence         = t.fence + 1, \
           last_start_at = now() \
     WHERE t.task = $1 \
       AND t.enabled \
       AND t.next_due_at <= now() \
       AND (t.lease_until IS NULL OR t.lease_until <= now()) \
    RETURNING t.fence";

/// Fixed-rate advance with catch-up clamp: an overdue task re-fires after one
/// poll, never in a zero-sleep spin, and never tries to make up more than
/// one missed period.
const RELEASE_ADVANCE_SQL: &str = "\
    UPDATE queen.maintenance_leases t \
       SET next_due_at      = GREATEST(t.next_due_at + t.period_ms * interval '1 millisecond', now()), \
           lease_until      = NULL, \
           holder           = NULL, \
           last_end_at      = now(), \
           last_duration_ms = $3, \
           runs             = t.runs + 1 \
     WHERE t.task = $1 AND t.fence = $2";

/// A failed cycle releases the lease WITHOUT advancing the schedule: the task
/// stays due, so any replica retries after one poll interval. `next_due_at`
/// left in the past is also the honest "we are behind" signal in the table.
const RELEASE_RETRY_SQL: &str = "\
    UPDATE queen.maintenance_leases t \
       SET lease_until = NULL, holder = NULL \
     WHERE t.task = $1 AND t.fence = $2";

/// Upsert the task row with the configured period. Called once per loop at
/// startup (after schema apply). Never touches `enabled` (operator-owned) or
/// the fence/lease; shrinking the period clamps `next_due_at` so the new
/// period takes effect within one period instead of waiting out the old one.
pub async fn ensure_row(
    pool: &Pool,
    task: &str,
    period_ms: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = pool.get().await?;
    client.execute(ENSURE_SQL, &[&task, &(period_ms as i64)]).await?;
    Ok(())
}

/// Try to claim the task. `Ok(Some(fence))` = this replica runs the cycle now
/// and MUST call [`release`] with that fence afterwards; `Ok(None)` = not due,
/// disabled, or leased by someone else — sleep one poll and retry.
pub async fn claim(
    pool: &Pool,
    task: &str,
    lease_ms: u64,
    holder: &str,
) -> Result<Option<i64>, Box<dyn std::error::Error + Send + Sync>> {
    let client = pool.get().await?;
    let row = client
        .query_opt(CLAIM_SQL, &[&task, &(lease_ms as i64), &holder])
        .await?;
    Ok(row.map(|r| r.get(0)))
}

/// How a finished cycle releases its claim.
pub enum Release {
    /// The period was served (the cycle ran, or the belt advisory lock showed
    /// someone else — an old-image pod or the manual refresh — was serving it):
    /// advance `next_due_at` by exactly one period.
    Advance { elapsed_ms: i32 },
    /// The cycle errored: free the lease, leave the task due, retry after a poll.
    Retry,
}

/// Release the claim. Errors are logged (sampled by the caller's own error
/// path being separate) and swallowed: an unreleased lease self-heals at
/// `lease_until`, costing at most one lease of schedule delay — turning a
/// completed cycle into an error over bookkeeping would be worse. A stale
/// fence (the lease expired mid-cycle and someone else claimed) is WARNed:
/// it means a cycle outlived `lease_ms`, which is the signal to raise it.
pub async fn release(pool: &Pool, task: &str, fence: i64, how: Release) {
    let res = async {
        let client = pool.get().await?;
        let n = match how {
            Release::Advance { elapsed_ms } => {
                client
                    .execute(RELEASE_ADVANCE_SQL, &[&task, &fence, &elapsed_ms])
                    .await?
            }
            Release::Retry => client.execute(RELEASE_RETRY_SQL, &[&task, &fence]).await?,
        };
        Ok::<u64, Box<dyn std::error::Error + Send + Sync>>(n)
    }
    .await;
    match res {
        Ok(1) => {}
        Ok(_) => tracing::warn!(
            target: "lease",
            task,
            fence,
            "stale fence on release: the lease expired mid-cycle and another replica claimed; \
             raise the lease if cycles legitimately run this long"
        ),
        Err(e) => tracing::warn!(
            target: "lease",
            task,
            fence,
            error = %e,
            "lease release failed; the lease self-heals at lease_until"
        ),
    }
}
