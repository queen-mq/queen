//! Background retention + eviction service for the segments engine.
//!
//! Ports the cadence of the C++ RetentionService + EvictionService
//! (server/src/services/{retention_service,eviction_service}.cpp) onto a tokio
//! task: every `RETENTION_INTERVAL` ms, on ONE dedicated pooled connection,
//! inside a transaction guarded by the same transaction-level advisory lock
//! (737001) the C++ services use, run the segment-maintenance functions.
//!
//! The `pg_try_advisory_xact_lock` is non-blocking: behind a load balancer only
//! one replica sweeps per cycle (a replica that can't take the lock skips the
//! cycle, so replicas never double-delete). The lock is transaction-scoped, so
//! COMMIT/ROLLBACK releases it.
//!
//! Each cycle runs, in that one transaction:
//!   * `queen.seg_retention_sweep_v1()` — age-based `retention_seconds`,
//!     consumed-below-all-cursors `completed_retention_seconds`, and the
//!     `seg_dedup` window purge (all config read from `queen.queues`).
//!   * `db::seg_evict_max_wait()` — `max_wait_time_seconds` age eviction. The
//!     C++ EvictionService rule that has no segments SP (see db.rs); kept here so
//!     a `{ retentionEnabled, maxWaitTimeSeconds }` queue is still swept.
//!
//! A failing cycle is logged and swallowed so the loop survives — a transient DB
//! error or a serialization failure must not kill background maintenance.

use std::time::{Duration, Instant};

use deadpool_postgres::Pool;

use crate::config::Config;
use crate::db;

/// Transaction-level advisory lock shared with the C++ retention/eviction
/// services (server/src/services/retention_service.cpp:73). A replica that can't
/// acquire it skips the cycle.
const CLEANUP_LOCK_ID: i64 = 737_001;

/// Launch the retention/eviction background loop. Non-blocking: spawns a detached
/// tokio task and returns immediately. Call once at boot, before `axum::serve`.
pub fn spawn(pool: Pool, cfg: &Config) {
    let interval = Duration::from_millis(cfg.retention_interval_ms);
    let knobs = Knobs {
        metrics_retention_days: cfg.metrics_retention_days,
        batch_size: cfg.retention_batch_size,
    };
    println!(
        "retention: service started (interval={}ms, advisory_lock={}, metrics_retention_days={})",
        cfg.retention_interval_ms, CLEANUP_LOCK_ID, knobs.metrics_retention_days
    );
    tokio::spawn(async move { run_loop(pool, interval, knobs).await });
}

/// Cfg values the maintenance cycle needs (RUSTFIX item 20). A small Copy struct
/// so the values move cleanly into the detached task.
#[derive(Clone, Copy)]
struct Knobs {
    metrics_retention_days: i32,
    batch_size: usize,
}

async fn run_loop(pool: Pool, interval: Duration, knobs: Knobs) {
    loop {
        let start = Instant::now();
        match run_cycle(&pool, knobs).await {
            Ok(Outcome::Skipped) => {
                // Another replica holds the cleanup lock this cycle — nothing to do.
            }
            Ok(Outcome::Ran { sweep, max_wait, metrics }) => {
                println!(
                    "retention: cycle sweep={} max_wait_segments_deleted={} metrics_purge={}",
                    sweep.trim(),
                    max_wait,
                    metrics.trim()
                );
            }
            Err(e) => eprintln!("retention: cycle error: {e}"),
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
    /// Cycle ran; carries the sweep SP result JSON + max_wait delete count + a
    /// short metrics-purge summary. (RUSTFIX item 3: max_queue_size eviction
    /// removed — seg_evict_v1 is no longer called.)
    Ran {
        sweep: String,
        max_wait: i64,
        metrics: String,
    },
}

/// One maintenance cycle on a dedicated connection: BEGIN, try the advisory lock,
/// run the maintenance, then COMMIT (success) / ROLLBACK (error). Either ends the
/// transaction, releasing the xact-scoped lock and returning a clean connection
/// to the pool.
async fn run_cycle(pool: &Pool, knobs: Knobs) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    let client = pool.get().await?;
    client.batch_execute("BEGIN").await?;
    let res = cycle_body(&client, knobs).await;
    match &res {
        Ok(_) => {
            let _ = client.batch_execute("COMMIT").await;
        }
        Err(_) => {
            let _ = client.batch_execute("ROLLBACK").await;
        }
    }
    res
}

async fn cycle_body(
    client: &deadpool_postgres::Client,
    knobs: Knobs,
) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    let got: bool = client
        .query_one("SELECT pg_try_advisory_xact_lock($1)", &[&CLEANUP_LOCK_ID])
        .await?
        .get(0);
    if !got {
        return Ok(Outcome::Skipped);
    }

    let sweep: String = client
        .query_one("SELECT (queen.seg_retention_sweep_v1())::text", &[])
        .await?
        .get(0);
    // RUSTFIX item 3: seg_evict_v1 (max_queue_size) is no longer called — it was a
    // Rust-only regression that deleted whole segments. max_wait_time eviction is
    // KEPT (db::seg_evict_max_wait) because the C++ EvictionService enforced it.
    let max_wait = db::seg_evict_max_wait(client).await?;

    // RUSTFIX item 20: purge stale metrics INSIDE the retention loop (C++
    // RetentionService::cleanup_old_metrics, retention_service.cpp:432-476), using
    // the configured window (default 90 days). worker_metrics/lag/parked via the
    // existing SP; queen.system_metrics (written every window by syscollect.rs and
    // NEVER purged before this) via a batched DELETE. Errors here are logged but
    // don't abort the cycle — maintenance must survive a transient metrics-purge
    // failure the same way the sweep does.
    let metrics = match purge_metrics(client, knobs).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("retention: metrics purge error: {e}");
            "error".to_string()
        }
    };

    Ok(Outcome::Ran { sweep, max_wait, metrics })
}

/// Trim worker/lag/parked metrics (SP) and queen.system_metrics (batched DELETE)
/// older than `metrics_retention_days`. Returns a short summary for the log line.
///
/// RUSTFIX item 20: the purge runs inside the cycle's locked transaction, but is
/// bracketed in a SAVEPOINT. In Postgres a failed statement aborts the whole
/// transaction, turning the subsequent COMMIT into a silent ROLLBACK — which would
/// discard the sweep + max_wait eviction that already succeeded in the same
/// transaction. Rolling a failed purge back to the savepoint leaves the
/// transaction clean, so the outer COMMIT still persists the sweep. The advisory
/// lock is taken before the savepoint, so ROLLBACK TO SAVEPOINT does not release it
/// (leader-gating is preserved).
async fn purge_metrics(
    client: &deadpool_postgres::Client,
    knobs: Knobs,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    client.batch_execute("SAVEPOINT metrics_purge").await?;
    match purge_metrics_inner(client, knobs).await {
        Ok(summary) => {
            // Best-effort (mirrors run_cycle's COMMIT/ROLLBACK): a RELEASE failure
            // can't lose the purge's already-applied changes.
            let _ = client.batch_execute("RELEASE SAVEPOINT metrics_purge").await;
            Ok(summary)
        }
        Err(e) => {
            // Un-abort the transaction so the outer COMMIT still persists the sweep.
            let _ = client.batch_execute("ROLLBACK TO SAVEPOINT metrics_purge").await;
            Err(e)
        }
    }
}

async fn purge_metrics_inner(
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
