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
//!   * `queen.seg_evict_v1()` — `max_queue_size` enforcement.
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
    println!(
        "retention: service started (interval={}ms, advisory_lock={})",
        cfg.retention_interval_ms, CLEANUP_LOCK_ID
    );
    tokio::spawn(async move { run_loop(pool, interval).await });
}

async fn run_loop(pool: Pool, interval: Duration) {
    loop {
        let start = Instant::now();
        match run_cycle(&pool).await {
            Ok(Outcome::Skipped) => {
                // Another replica holds the cleanup lock this cycle — nothing to do.
            }
            Ok(Outcome::Ran { sweep, evict, max_wait }) => {
                println!(
                    "retention: cycle sweep={} evict={} max_wait_segments_deleted={}",
                    sweep.trim(),
                    evict.trim(),
                    max_wait
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
    /// Cycle ran; carries the sweep/evict SP result JSON + max_wait delete count.
    Ran {
        sweep: String,
        evict: String,
        max_wait: i64,
    },
}

/// One maintenance cycle on a dedicated connection: BEGIN, try the advisory lock,
/// run the maintenance, then COMMIT (success) / ROLLBACK (error). Either ends the
/// transaction, releasing the xact-scoped lock and returning a clean connection
/// to the pool.
async fn run_cycle(pool: &Pool) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    let client = pool.get().await?;
    client.batch_execute("BEGIN").await?;
    let res = cycle_body(&client).await;
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
    let evict: String = client
        .query_one("SELECT (queen.seg_evict_v1())::text", &[])
        .await?
        .get(0);
    let max_wait = db::seg_evict_max_wait(client).await?;

    Ok(Outcome::Ran { sweep, evict, max_wait })
}
