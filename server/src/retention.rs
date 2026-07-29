//! Background retention + eviction service for the log engine (18-log-engine.md §8).
//!
//! Keeps the cadence of the old seg-engine service (every `RETENTION_INTERVAL`
//! ms — now defaulting to 5000 — on ONE dedicated pooled connection, gated by
//! advisory lock 737001), but the shape of the work is inverted: instead of one
//! `seg_retention_sweep_v1()` call = one giant transaction over EVERY partition,
//! each cycle loops the bounded STEP functions of 045_log_maintenance.sql:
//!
//!   * `queen.log_retention_step_v1(pid, all_cutoff, completed_cutoff, max_rows)`
//!     — rules 1+2 (retention_seconds / completed_retention_seconds), at most
//!     `max_rows` segment rows per call, looped until `{"done":true}`.
//!   * `queen.log_txns_purge_step_v1(pid, cutoff, max_rows)` — the log_txns
//!     hash-sidecar purge, cutoff = now() - GREATEST(dedup_window_seconds,
//!     completed_retention_seconds, 900).
//!   * `queen.log_evict_max_wait_step_v1(pid, cutoff, max_rows)` — the
//!     max_wait_time_seconds age eviction that used to live Rust-side in
//!     db::seg_evict_max_wait; retention.rs now owns it via this SP.
//!
//! CONCURRENCY SAFETY (the whole point of R1 — doc 17's "retention-sweep
//! effect" / doc 18 §8, §12.5 "no sweep stalls"): there is NO wrapping
//! transaction. Every step call autocommits, and each step takes exactly ONE
//! `queen.log_partitions` row lock (`FOR UPDATE`, the same serializer the push
//! allocator uses) and holds it only for its own bounded batch — so a huge
//! backlog is drained in many short transactions instead of one sweep-length
//! lock hold, and pushes interleave between batches. Cutoffs are computed
//! CALLER-side from one `now()` (one consistent clock per cycle); a NULL cutoff
//! disables that rule. Partitions are iterated in ascending id order — the one
//! global total order shared with the multi-partition lockers (045 header) —
//! though with single-row locks per step the steps cannot deadlock regardless.
//!
//! The advisory lock is now SESSION-scoped (`pg_try_advisory_lock`) because
//! there is no cycle transaction to scope it to; it is explicitly released with
//! `pg_advisory_unlock` on every exit path. If the connection dies mid-cycle the
//! backend session dies with it and PostgreSQL releases the lock anyway (and
//! deadpool recycles the broken connection), so the lock cannot leak. Behind a
//! load balancer only one replica sweeps per cycle; a replica that can't take
//! the lock skips the cycle, so replicas never double-delete.
//!
//! A failing cycle is logged and swallowed so the loop survives — a transient DB
//! error must not kill background maintenance.

use std::time::{Duration, Instant};

use deadpool_postgres::Pool;

use crate::config::Config;
use crate::db;

/// Advisory lock shared with the C++ retention/eviction services
/// (server/src/services/retention_service.cpp:73), now session-level (see module
/// docs). A replica that can't acquire it skips the cycle.
const CLEANUP_LOCK_ID: i64 = 737_001;

/// One consistent `now()` per cycle: the work-list query computes every cutoff
/// as `timestamptz::text` server-side (all columns of one statement share one
/// `now()`), and the step calls cast the text back. A NULL column = that rule is
/// disabled for the queue (the 026-era `retention_seconds > 0` gating, decided
/// here, not in SQL — 045 header).
///
/// Queue identity is (tenant_id, name) on BOTH sides of the queues→log_queues
/// join (schema.sql, 041_log_schema.sql), so the join predicate MUST carry
/// tenant_id: matching on name alone is a cross product as soon as two tenants
/// hold the same queue name, and one tenant's cutoffs would then be emitted
/// against another tenant's partitions — which the step calls below execute as
/// deletes. Single-tenant deployments are unaffected (the join is 1:1 either
/// way).
const WORK_LIST_SQL: &str = "\
    SELECT p.id::text, \
           lq.id::text, \
           CASE WHEN qq.retention_enabled AND COALESCE(qq.retention_seconds, 0) > 0 \
                THEN (now() - make_interval(secs => qq.retention_seconds))::text END, \
           CASE WHEN qq.retention_enabled AND COALESCE(qq.completed_retention_seconds, 0) > 0 \
                THEN (now() - make_interval(secs => qq.completed_retention_seconds))::text END, \
           (now() - make_interval(secs => GREATEST(lq.dedup_window_seconds, \
                                                   COALESCE(qq.completed_retention_seconds, 0), \
                                                   900)))::text, \
           CASE WHEN COALESCE(qq.max_wait_time_seconds, 0) > 0 \
                THEN (now() - make_interval(secs => qq.max_wait_time_seconds))::text END \
    FROM queen.queues qq \
    JOIN queen.log_queues lq ON lq.name = qq.name AND lq.tenant_id = qq.tenant_id \
    JOIN queen.log_partitions p ON p.queue_id = lq.id \
    WHERE qq.storage = 'segments' \
    ORDER BY p.id";

/// Launch the retention/eviction background loop. Non-blocking: spawns a detached
/// tokio task and returns immediately. Call once at boot, before `axum::serve`.
pub fn spawn(pool: Pool, cfg: &Config) {
    let interval = Duration::from_millis(cfg.retention_interval_ms);
    let knobs = Knobs {
        metrics_retention_days: cfg.metrics_retention_days,
        batch_size: cfg.retention_batch_size,
    };
    tracing::info!(
        target: "retention",
        interval_ms = cfg.retention_interval_ms,
        advisory_lock = CLEANUP_LOCK_ID,
        batch_size = knobs.batch_size,
        metrics_retention_days = knobs.metrics_retention_days,
        "service started"
    );
    tokio::spawn(async move { run_loop(pool, interval, knobs).await });
}

/// Cfg values the maintenance cycle needs. A small Copy struct so the values
/// move cleanly into the detached task. `batch_size` = p_max_rows per step call
/// (RETENTION_BATCH_SIZE) and also bounds each metrics-purge DELETE.
#[derive(Clone, Copy)]
struct Knobs {
    metrics_retention_days: i32,
    batch_size: usize,
}

impl Knobs {
    /// `p_max_rows` for the step SPs (their INT arg).
    fn max_rows(&self) -> i32 {
        self.batch_size.min(i32::MAX as usize) as i32
    }
}

async fn run_loop(pool: Pool, interval: Duration, knobs: Knobs) {
    loop {
        let start = Instant::now();
        match run_cycle(&pool, knobs).await {
            Ok(Outcome::Skipped) => {
                // Another replica holds the cleanup lock this cycle — nothing to do.
            }
            Ok(Outcome::Ran {
                queues,
                segments_deleted,
                txns_purged,
                max_wait,
                metrics,
            }) => {
                let elapsed_ms = start.elapsed().as_millis() as u64;
                // LOGGING_PLAN.md Phase 2: only speak when the cycle actually
                // deleted something — an idle cluster used to emit this at INFO
                // every RETENTION_INTERVAL (5s) with all-zero counters. The metrics
                // purge (90-day window) rarely fires, so it stays out of the gate;
                // a working cycle names the counts + elapsed, an idle one is DEBUG.
                if segments_deleted > 0 || txns_purged > 0 || max_wait > 0 {
                    tracing::info!(
                        target: "retention",
                        queues,
                        segments_deleted,
                        txns_purged,
                        max_wait_evicted = max_wait,
                        metrics_purge = %metrics.trim(),
                        elapsed_ms,
                        "swept"
                    );
                } else {
                    tracing::debug!(target: "retention", queues, elapsed_ms, "idle cycle");
                }
            }
            Err(e) => {
                // A sustained DB outage must not emit one ERROR every 5s.
                if let Some(suppressed) = CYCLE_ERR.tick_now() {
                    tracing::error!(target: "retention", error = %e, suppressed, "cycle error");
                }
            }
        }
        // Fixed cadence measured from cycle start (matches the C++ services:
        // sleep = interval - elapsed, clamped at 0).
        let sleep = interval.checked_sub(start.elapsed()).unwrap_or(Duration::ZERO);
        tokio::time::sleep(sleep).await;
    }
}

enum Outcome {
    /// Advisory lock was held by another replica; cycle skipped.
    Skipped,
    /// Cycle ran; carries the numeric work counts (so the loop can suppress
    /// idle no-op cycles) + a short metrics-purge summary.
    Ran {
        queues: usize,
        segments_deleted: i64,
        txns_purged: i64,
        max_wait: i64,
        metrics: String,
    },
}

/// Rate-limit the cycle-error ERROR so a sustained DB outage doesn't emit one
/// line per RETENTION_INTERVAL (LOGGING_PLAN.md WARN/ERROR doctrine).
static CYCLE_ERR: crate::obs::Sampler = crate::obs::Sampler::new(30_000);
static PURGE_ERR: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// One maintenance cycle on a dedicated connection: try the SESSION advisory
/// lock, run the phases (each an autocommitting SP call — no wrapping
/// transaction), then release the lock on EVERY exit path. Autocommit statements
/// never leave the connection in an aborted-transaction state, so the connection
/// goes back to the pool clean even after a mid-cycle error.
async fn run_cycle(pool: &Pool, knobs: Knobs) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    let client = pool.get().await?;
    let got: bool = client
        .query_one("SELECT pg_try_advisory_lock($1)", &[&CLEANUP_LOCK_ID])
        .await?
        .get(0);
    if !got {
        return Ok(Outcome::Skipped);
    }
    let res = cycle_body(&client, knobs).await;
    // Session lock: MUST unlock before the connection returns to the pool, or a
    // healthy pooled session would keep leader-gating every other cycle forever.
    // If this fails the connection itself is almost certainly broken — the
    // backend session then dies and PostgreSQL releases the lock with it (see
    // module docs), so log loudly but don't turn a successful cycle into an error.
    match client.query_one("SELECT pg_advisory_unlock($1)", &[&CLEANUP_LOCK_ID]).await {
        Ok(row) => {
            let released: bool = row.get(0);
            if !released {
                tracing::warn!(
                    target: "retention",
                    advisory_lock = CLEANUP_LOCK_ID,
                    "pg_advisory_unlock reported not-held (lock-protocol invariant broken)"
                );
            }
        }
        Err(e) => tracing::warn!(
            target: "retention",
            error = %e,
            "advisory unlock failed; lock releases with the session if the connection is recycled"
        ),
    }
    res
}

async fn cycle_body(
    client: &deadpool_postgres::Client,
    knobs: Knobs,
) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    // Work list, ONCE per cycle: every partition of every log ('segments')
    // queue, with the per-queue cutoffs precomputed from one now() (see
    // WORK_LIST_SQL). Which phases apply per row is encoded by NULL-ness.
    let stmt = client.prepare_cached(WORK_LIST_SQL).await?;
    let rows = client.query(&stmt, &[]).await?;
    let work: Vec<WorkItem> = rows
        .iter()
        .map(|r| WorkItem {
            pid: r.get(0),
            queue_id: r.get(1),
            all_cutoff: r.get(2),
            completed_cutoff: r.get(3),
            txns_cutoff: r.get(4),
            max_wait_cutoff: r.get(5),
        })
        .collect();
    let max_rows = knobs.max_rows();

    // Phase 1: retention rules 1+2 (queues gated 026-style: retention_enabled
    // AND a positive window — encoded as at least one non-NULL cutoff). The
    // swept-queue count is keyed by log_queues.id, not name: names repeat across
    // tenants, so a name-keyed set would collapse distinct queues into one.
    let mut segments_deleted: i64 = 0;
    let mut retention_queues: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let stmt = client
        .prepare_cached(
            "SELECT (queen.log_retention_step_v1($1::text::uuid, $2::text::timestamptz, \
             $3::text::timestamptz, $4::int))::text",
        )
        .await?;
    for w in &work {
        if w.all_cutoff.is_none() && w.completed_cutoff.is_none() {
            continue;
        }
        retention_queues.insert(w.queue_id.as_str());
        loop {
            let row = client
                .query_one(&stmt, &[&w.pid, &w.all_cutoff, &w.completed_cutoff, &max_rows])
                .await?;
            let (deleted, done) = step_result(row.get(0));
            segments_deleted += deleted;
            // The step contract (045) is done:true whenever nothing was deleted,
            // so `!done` implies progress; the deleted==0 arm is a defensive
            // stop against a contract break looping us forever.
            if done || deleted == 0 {
                break;
            }
        }
    }

    // Phase 2: log_txns hash-sidecar purge — EVERY log partition (the 900s floor
    // in the cutoff makes the window always applicable; 041 header).
    let mut txns_purged: i64 = 0;
    let stmt = client
        .prepare_cached(
            "SELECT (queen.log_txns_purge_step_v1($1::text::uuid, $2::text::timestamptz, \
             $3::int))::text",
        )
        .await?;
    for w in &work {
        loop {
            let row = client
                .query_one(&stmt, &[&w.pid, &w.txns_cutoff, &max_rows])
                .await?;
            let (deleted, done) = step_result(row.get(0));
            txns_purged += deleted;
            if done || deleted == 0 {
                break;
            }
        }
    }

    // Phase 3: max_wait_time_seconds eviction — applies regardless of
    // retention_enabled (a queue configured with ONLY maxWaitTimeSeconds still
    // gets swept), matching the old db::seg_evict_max_wait / C++ EvictionService.
    // Sums segment rows deleted, the same number the old Rust helper returned.
    let mut max_wait: i64 = 0;
    let stmt = client
        .prepare_cached(
            "SELECT (queen.log_evict_max_wait_step_v1($1::text::uuid, $2::text::timestamptz, \
             $3::int))::text",
        )
        .await?;
    for w in &work {
        let Some(cutoff) = &w.max_wait_cutoff else {
            continue;
        };
        loop {
            let row = client.query_one(&stmt, &[&w.pid, &cutoff, &max_rows]).await?;
            let (deleted, done) = step_result(row.get(0));
            max_wait += deleted;
            if done || deleted == 0 {
                break;
            }
        }
    }

    // Phase 4: purge stale metrics (RUSTFIX item 20; C++ RetentionService::
    // cleanup_old_metrics, retention_service.cpp:432-476) on the configured
    // window. Plain autocommit calls on the same client — the old SAVEPOINT
    // bracketing is gone because there is no shared transaction left to protect:
    // a failed purge statement can no longer poison the retention deletes, which
    // committed statement-by-statement above. Errors are logged, not fatal.
    let metrics = match purge_metrics(client, knobs).await {
        Ok(s) => s,
        Err(e) => {
            if let Some(suppressed) = PURGE_ERR.tick_now() {
                tracing::error!(target: "retention", error = %e, suppressed, "metrics purge error");
            }
            "error".to_string()
        }
    };

    Ok(Outcome::Ran {
        queues: retention_queues.len(),
        segments_deleted,
        txns_purged,
        max_wait,
        metrics,
    })
}

/// One partition's row in the cycle work list. Cutoffs are prerendered
/// `timestamptz::text` values (None = rule disabled for this queue).
struct WorkItem {
    pid: String,
    queue_id: String,
    all_cutoff: Option<String>,
    completed_cutoff: Option<String>,
    txns_cutoff: String,
    max_wait_cutoff: Option<String>,
}

/// Parse a step SP's `{"deleted":N,...,"done":bool}` JSONB::text return.
/// Unparseable output degrades to (0, done=true) so a broken contract stops the
/// loop instead of spinning it.
fn step_result(s: String) -> (i64, bool) {
    let v: serde_json::Value = serde_json::from_str(&s).unwrap_or(serde_json::Value::Null);
    let deleted = v.get("deleted").and_then(|d| d.as_i64()).unwrap_or(0);
    let done = v.get("done").and_then(|d| d.as_bool()).unwrap_or(true);
    (deleted, done)
}

/// Trim worker/lag/parked metrics (SP) and queen.system_metrics (batched DELETE)
/// older than `metrics_retention_days`. Returns a short summary for the log line.
/// Runs as plain autocommit statements — no transaction, no savepoints (see
/// cycle_body phase 4).
async fn purge_metrics(
    client: &deadpool_postgres::Client,
    knobs: Knobs,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let worker = db::cleanup_worker_metrics(client, knobs.metrics_retention_days).await?;
    let system = db::cleanup_system_metrics(
        client,
        knobs.metrics_retention_days,
        knobs.batch_size,
    )
    .await?;
    Ok(format!("worker={} system_rows={}", worker.trim(), system))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shape guard only — the cycle itself needs a live pool, so behavioural
    /// coverage lives in the two-tenant isolation smoke. What this pins down is
    /// the one property that silently turns the work list into a cross-tenant
    /// delete: joining queues to log_queues on name without tenant_id.
    #[test]
    fn work_list_join_is_tenant_scoped() {
        assert!(WORK_LIST_SQL.contains("lq.name = qq.name AND lq.tenant_id = qq.tenant_id"));
        // The partition leg must stay keyed on the (already tenant-resolved)
        // log_queues row, never re-resolved by name.
        assert!(WORK_LIST_SQL.contains("queen.log_partitions p ON p.queue_id = lq.id"));
    }
}
