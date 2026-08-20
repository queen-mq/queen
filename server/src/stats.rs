//! Background stats reconciler for the log engine.
//!
//! Runs `queen.log_refresh_all_stats_v1()` (011_log_stats) on ONE pooled
//! connection, in a transaction — which recomputes `queen.stats`
//! (queue/namespace/task/system rows) DIRECTLY from the log tables (`log_*`)
//! with O(partitions) watermark arithmetic, since the v1 reconciler reads
//! `queen.messages` (empty under the log engine). Without this loop
//! `queen.stats` stays empty and every stats-backed reader (get_system_overview_v3 /
//! get_status_v3 / get_status_queues_v2 / get_queue_detail_v2) returns zeros — the
//! "pages show no data" regression. The summary the SP returns keeps the seg-era
//! keys ('engine':'segments', 'segPartitions' — telemetry labels aligned with the
//! queen.queues.storage compat value, 011_log_stats) and is printed verbatim below.
//!
//! SCHEDULING (PLAN_STATS_REFRESH.md T2.1): cadence comes from the durable
//! claim row `queen.maintenance_leases['stats_refresh']` (crate::lease), NOT
//! from a per-replica timer — `STATS_INTERVAL_MS` is therefore the TRUE cluster
//! cadence, independent of the replica count. Each replica polls the row
//! cheaply and runs a cycle only when it wins the claim; failover after a dead
//! holder is bounded by the lease, not by TCP keepalive. The old
//! `pg_try_advisory_xact_lock` stays INSIDE the cycle as belt: during a mixed
//! fleet an old-image pod still schedules by its own timer, and the lock is
//! what keeps the two schemes from ever running the SP concurrently. (Before
//! the claim row, that lock WAS the scheduler — mutual exclusion, not cadence,
//! so three replicas ran the refresh at interval/3.)
//!
//! A failing cycle is logged and swallowed so the loop survives transient DB
//! errors; the claim is then released WITHOUT advancing the schedule, so the
//! task stays due and any replica retries after one poll.

use std::time::{Duration, Instant};

use deadpool_postgres::Pool;

use crate::config::Config;
use crate::db;

/// Transaction-level advisory lock for the stats reconciler. Distinct from the
/// retention lock (737_001) so stats and retention never block each other.
const STATS_LOCK_ID: i64 = 737_002;

/// Transaction-level advisory lock for the retained-bytes slow lane. Its own id
/// (not 737_001/737_002): the byte scan must never serialize with — nor steal a
/// cycle from — the counters refresh or retention. The lock id also carries the
/// writer-exclusion guarantee through a mixed fleet: it lives in the binary, so
/// it survives the last-boot-wins SQL re-apply that an ORDER BY in a procedure
/// body does not (PLAN_STATS_REFRESH.md §8.4.6).
const BYTES_LOCK_ID: i64 = 737_003;

/// Rate-limit the cycle-error ERROR (LOGGING_PLAN.md doctrine): a persistently
/// failing DB must not emit one line per STATS_INTERVAL_MS.
static CYCLE_ERR: crate::obs::Sampler = crate::obs::Sampler::new(30_000);

/// Same doctrine for the retained-bytes lane. Kept separate so a failing byte
/// scan and a failing counters refresh are distinguishable in the logs — a
/// frozen retained_bytes gauge sits behind the proxy's hard-403 storage gate
/// and has no other symptom.
static BYTES_ERR: crate::obs::Sampler = crate::obs::Sampler::new(30_000);

// NOTE (RUSTFIX item 20): the metrics purge used to live here on a hardcoded
// 7-day window. It moved to retention.rs (C++ parity — the RetentionService owned
// it, at 90 days) so it also purges queen.system_metrics and honors
// METRICS_RETENTION_DAYS / RETENTION_BATCH_SIZE. This loop now only reconciles
// queen.stats.

/// Lease-table task name for the counters refresh (029_maintenance_leases).
const TASK: &str = "stats_refresh";

/// Launch the stats reconciler loop. Non-blocking: spawns a detached tokio task.
/// Call once at boot, before `axum::serve`.
pub fn spawn(pool: Pool, cfg: &Config) {
    let period = Duration::from_millis(cfg.stats_interval_ms);
    let holder = crate::lease::holder_id(cfg);
    tracing::info!(
        target: "stats",
        period_ms = cfg.stats_interval_ms,
        lease_task = TASK,
        advisory_lock = STATS_LOCK_ID,
        "reconciler started"
    );
    tokio::spawn(async move { run_loop(pool, period, holder).await });
}

async fn run_loop(pool: Pool, period: Duration, holder: String) {
    let period_ms = period.as_millis() as u64;
    let poll = crate::lease::poll_interval(period);
    let lease_ms = crate::lease::lease_ms(period_ms);
    // The schedule row must exist before the first claim; the DB may still be
    // coming up at boot, so retry on the poll cadence rather than dying.
    while let Err(e) = crate::lease::ensure_row(&pool, TASK, period_ms).await {
        if let Some(suppressed) = CYCLE_ERR.tick_now() {
            tracing::error!(target: "stats", error = %e, suppressed, "lease row upsert failed");
        }
        tokio::time::sleep(poll).await;
    }
    loop {
        let fence = match crate::lease::claim(&pool, TASK, lease_ms, &holder).await {
            Ok(Some(fence)) => fence,
            Ok(None) => {
                // Not due, disabled, or another replica holds the lease.
                tokio::time::sleep(poll).await;
                continue;
            }
            Err(e) => {
                if let Some(suppressed) = CYCLE_ERR.tick_now() {
                    tracing::error!(target: "stats", error = %e, suppressed, "lease claim error");
                }
                tokio::time::sleep(poll).await;
                continue;
            }
        };
        let start = Instant::now();
        match run_cycle(&pool, period_ms).await {
            Ok(Outcome::Skipped) => {
                // Belt advisory lock busy: an old-image pod's own timer or the
                // unlocked manual refresh (POST /api/v1/stats/refresh) is
                // serving this period right now. Count the period served —
                // re-claiming immediately would double the work, not halve the
                // staleness.
                tracing::info!(
                    target: "stats",
                    "claimed but advisory lock busy; period served by another writer"
                );
                let elapsed_ms = start.elapsed().as_millis() as i32;
                crate::lease::release(&pool, TASK, fence, crate::lease::Release::Advance { elapsed_ms })
                    .await;
            }
            Ok(Outcome::Ran { summary }) => {
                let elapsed_ms = start.elapsed().as_millis() as u64;
                let s = summary.trim();
                // LOGGING_PLAN.md: add elapsed_ms (the refresh is O(partitions) —
                // the CPU signal the old line dropped) and demote idle reconciles
                // (nothing updated) to DEBUG so a quiet leader stops emitting INFO.
                if s.contains("\"queuesUpdated\": 0,") || s.contains("\"queuesUpdated\":0,") {
                    tracing::debug!(target: "stats", elapsed_ms, summary = %s, "refresh (idle)");
                } else {
                    tracing::info!(target: "stats", elapsed_ms, summary = %s, "refresh");
                }
                crate::lease::release(
                    &pool,
                    TASK,
                    fence,
                    crate::lease::Release::Advance { elapsed_ms: elapsed_ms as i32 },
                )
                .await;
            }
            Err(e) => {
                if let Some(suppressed) = CYCLE_ERR.tick_now() {
                    tracing::error!(target: "stats", error = %e, suppressed, "cycle error");
                }
                crate::lease::release(&pool, TASK, fence, crate::lease::Release::Retry).await;
            }
        }
        // The poll doubles as the anti-spin floor when the task is overdue
        // (lease::poll_interval — the T0.3 sleep-floor policy).
        tokio::time::sleep(poll).await;
    }
}

enum Outcome {
    Skipped,
    Ran { summary: String },
}

async fn run_cycle(
    pool: &Pool,
    period_ms: u64,
) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    // Maintenance-lane admission: the stats refresh commits WAL on the same
    // device as everything else — it rides the shared budget, low priority.
    let mut slot = crate::admission::lane_slot(crate::admission::Lane::Maint).await;
    let t0 = std::time::Instant::now();
    let client = pool.get().await?;
    // T0.2 (PLAN_STATS_REFRESH.md): bound the transaction on both sides. SET
    // LOCAL dies at COMMIT/ROLLBACK, so neither value can leak into the pooled
    // connection. Before this, a black-holed pod held the advisory lock plus an
    // open XID (vacuum horizon pinned on the fillfactor-tuned tables) for the
    // TCP keepalive window, ~2h11m, with stats frozen cluster-wide.
    client
        .batch_execute(&format!(
            "BEGIN; \
             SET LOCAL statement_timeout = {}; \
             SET LOCAL idle_in_transaction_session_timeout = 60000",
            period_ms.clamp(30_000, 900_000)
        ))
        .await?;
    let res = cycle_body(&client).await;
    match &res {
        Ok(_) => {
            let _ = client.batch_execute("COMMIT").await;
            if let Some(sl) = slot.as_mut() {
                sl.commit_done(t0.elapsed());
            }
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
        .query_one("SELECT pg_try_advisory_xact_lock($1)", &[&STATS_LOCK_ID])
        .await?
        .get(0);
    if !got {
        return Ok(Outcome::Skipped);
    }

    let summary = db::seg_refresh_all_stats(client).await?;
    Ok(Outcome::Ran { summary })
}

// ============================================================================
// Retained-bytes slow lane (PLAN_STATS_REFRESH.md T1.0).
//
// queen.stats.retained_bytes used to be recomputed inside EVERY counters
// refresh above — an unqualified heap scan of queen.log_segments (1 GB+ in
// prod) per cycle, the largest single term of the #1 query on the instance —
// to feed exactly one consequential reader: the proxy storage quota, which is
// hysteretic by contract. This loop is that scan on its own, much slower
// cadence (RETAINED_BYTES_INTERVAL_MS); 011's refresh self-assigns the column
// and never writes it. Same scheduling shape as the counters loop: the
// 'retained_bytes' claim row is the cadence (the configured period IS the
// cluster cadence), advisory lock 737_003 stays inside the cycle as belt.
// ============================================================================

/// Lease-table task name for the bytes lane (029_maintenance_leases).
const TASK_BYTES: &str = "retained_bytes";

/// Launch the retained-bytes lane. Non-blocking: spawns a detached tokio task.
/// Call once at boot, next to `spawn`.
pub fn spawn_retained_bytes(pool: Pool, cfg: &Config) {
    let period = Duration::from_millis(cfg.retained_bytes_interval_ms);
    let holder = crate::lease::holder_id(cfg);
    tracing::info!(
        target: "stats",
        period_ms = cfg.retained_bytes_interval_ms,
        lease_task = TASK_BYTES,
        advisory_lock = BYTES_LOCK_ID,
        "retained-bytes lane started"
    );
    tokio::spawn(async move { run_bytes_loop(pool, period, holder).await });
}

async fn run_bytes_loop(pool: Pool, period: Duration, holder: String) {
    let period_ms = period.as_millis() as u64;
    let poll = crate::lease::poll_interval(period);
    let lease_ms = crate::lease::lease_ms(period_ms);
    while let Err(e) = crate::lease::ensure_row(&pool, TASK_BYTES, period_ms).await {
        if let Some(suppressed) = BYTES_ERR.tick_now() {
            tracing::error!(target: "stats", error = %e, suppressed, "bytes lease row upsert failed");
        }
        tokio::time::sleep(poll).await;
    }
    loop {
        let fence = match crate::lease::claim(&pool, TASK_BYTES, lease_ms, &holder).await {
            Ok(Some(fence)) => fence,
            Ok(None) => {
                tokio::time::sleep(poll).await;
                continue;
            }
            Err(e) => {
                if let Some(suppressed) = BYTES_ERR.tick_now() {
                    tracing::error!(target: "stats", error = %e, suppressed, "bytes lease claim error");
                }
                tokio::time::sleep(poll).await;
                continue;
            }
        };
        let start = Instant::now();
        match run_bytes_cycle(&pool, period_ms).await {
            Ok(Outcome::Skipped) => {
                // Belt advisory lock busy (old-image pod mid-scan): the period
                // is being served; advance rather than churn the claim.
                tracing::info!(
                    target: "stats",
                    "bytes lane claimed but advisory lock busy; period served by another writer"
                );
                let elapsed_ms = start.elapsed().as_millis() as i32;
                crate::lease::release(
                    &pool,
                    TASK_BYTES,
                    fence,
                    crate::lease::Release::Advance { elapsed_ms },
                )
                .await;
            }
            Ok(Outcome::Ran { summary }) => {
                let elapsed_ms = start.elapsed().as_millis() as u64;
                let s = summary.trim();
                // Overrun is the one symptom a frozen quota gauge produces
                // before the proxy starts serving stale 403 decisions: WARN,
                // don't just INFO (PLAN_STATS_REFRESH.md §8.4.10).
                if start.elapsed() >= period {
                    tracing::warn!(
                        target: "stats",
                        elapsed_ms,
                        period_ms,
                        summary = %s,
                        "retained-bytes cycle overran its period"
                    );
                } else {
                    tracing::info!(target: "stats", elapsed_ms, summary = %s, "retained-bytes refresh");
                }
                crate::lease::release(
                    &pool,
                    TASK_BYTES,
                    fence,
                    crate::lease::Release::Advance { elapsed_ms: elapsed_ms as i32 },
                )
                .await;
            }
            Err(e) => {
                if let Some(suppressed) = BYTES_ERR.tick_now() {
                    tracing::error!(target: "stats", error = %e, suppressed, "retained-bytes cycle error");
                }
                crate::lease::release(&pool, TASK_BYTES, fence, crate::lease::Release::Retry).await;
            }
        }
        tokio::time::sleep(poll).await;
    }
}

async fn run_bytes_cycle(
    pool: &Pool,
    period_ms: u64,
) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    // Maintenance-lane admission, same reasoning as the counters cycle.
    let mut slot = crate::admission::lane_slot(crate::admission::Lane::Maint).await;
    let t0 = std::time::Instant::now();
    let client = pool.get().await?;
    // Same T0.2 bounds as the counters cycle; the ceiling scales with the
    // period because this transaction's one statement is the full heap scan.
    client
        .batch_execute(&format!(
            "BEGIN; \
             SET LOCAL statement_timeout = {}; \
             SET LOCAL idle_in_transaction_session_timeout = 60000",
            period_ms.clamp(30_000, 900_000)
        ))
        .await?;
    let res = bytes_cycle_body(&client).await;
    match &res {
        Ok(_) => {
            let _ = client.batch_execute("COMMIT").await;
            if let Some(sl) = slot.as_mut() {
                sl.commit_done(t0.elapsed());
            }
        }
        Err(_) => {
            let _ = client.batch_execute("ROLLBACK").await;
        }
    }
    res
}

async fn bytes_cycle_body(
    client: &deadpool_postgres::Client,
) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    let got: bool = client
        .query_one("SELECT pg_try_advisory_xact_lock($1)", &[&BYTES_LOCK_ID])
        .await?
        .get(0);
    if !got {
        return Ok(Outcome::Skipped);
    }

    let summary = db::seg_refresh_retained_bytes(client).await?;
    Ok(Outcome::Ran { summary })
}
