//! Background stats reconciler for the segments engine.
//!
//! Ports the cadence of the C++ StatsService (server/src/services/stats_service.cpp)
//! onto a tokio task: every `STATS_INTERVAL_MS` ms, on ONE pooled connection under a
//! transaction-level advisory lock, run `queen.seg_refresh_all_stats_v1()` — which
//! recomputes `queen.stats` (queue/namespace/task/system rows) DIRECTLY from the
//! segments tables (`seg_*`), since the v1 reconciler reads `queen.messages` (empty
//! under segments). Without this loop `queen.stats` stays empty and every
//! stats-backed reader (get_system_overview_v3 / get_status_v3 / get_status_queues_v2
//! / get_queue_detail_v2) returns zeros — the "pages show no data" regression.
//!
//! Leader election via `pg_try_advisory_xact_lock` (non-blocking): behind a load
//! balancer only one replica refreshes per cycle; a replica that can't take the lock
//! skips the cycle. Distinct lock id from retention so the two never serialize.
//!
//! A failing cycle is logged and swallowed so the loop survives transient DB errors.

use std::time::{Duration, Instant};

use deadpool_postgres::Pool;

use crate::config::Config;
use crate::db;

/// Transaction-level advisory lock for the stats reconciler. Distinct from the
/// retention lock (737_001) so stats and retention never block each other.
const STATS_LOCK_ID: i64 = 737_002;

/// Metrics-retention window: worker/lag/parked rows older than this are trimmed.
const METRICS_RETENTION_DAYS: i32 = 7;

/// Launch the stats reconciler loop. Non-blocking: spawns a detached tokio task.
/// Call once at boot, before `axum::serve`.
pub fn spawn(pool: Pool, cfg: &Config) {
    let interval = Duration::from_millis(cfg.stats_interval_ms);
    println!(
        "stats: reconciler started (interval={}ms, advisory_lock={})",
        cfg.stats_interval_ms, STATS_LOCK_ID
    );
    tokio::spawn(async move { run_loop(pool, interval).await });
}

async fn run_loop(pool: Pool, interval: Duration) {
    // Cleanup runs at most once per ~this many cycles (≈ every 10 min at 10s cadence).
    let cleanup_every = (Duration::from_secs(600).as_millis() / interval.as_millis().max(1)).max(1);
    let mut cycle: u128 = 0;
    loop {
        let start = Instant::now();
        match run_cycle(&pool, cycle % cleanup_every == 0).await {
            Ok(Outcome::Skipped) => {}
            Ok(Outcome::Ran { summary }) => {
                // Compact: the SP already reports queuesUpdated/segPartitions.
                eprintln!("stats: refresh {}", summary.trim());
            }
            Err(e) => eprintln!("stats: cycle error: {e}"),
        }
        cycle = cycle.wrapping_add(1);
        let sleep = interval.checked_sub(start.elapsed()).unwrap_or(Duration::ZERO);
        tokio::time::sleep(sleep).await;
    }
}

enum Outcome {
    Skipped,
    Ran { summary: String },
}

async fn run_cycle(
    pool: &Pool,
    do_cleanup: bool,
) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    let client = pool.get().await?;
    client.batch_execute("BEGIN").await?;
    let res = cycle_body(&client, do_cleanup).await;
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
    do_cleanup: bool,
) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    let got: bool = client
        .query_one("SELECT pg_try_advisory_xact_lock($1)", &[&STATS_LOCK_ID])
        .await?
        .get(0);
    if !got {
        return Ok(Outcome::Skipped);
    }

    let summary = db::seg_refresh_all_stats(client).await?;
    if do_cleanup {
        if let Err(e) = db::cleanup_worker_metrics(client, METRICS_RETENTION_DAYS).await {
            eprintln!("stats: metrics cleanup error: {e}");
        }
    }
    Ok(Outcome::Ran { summary })
}
