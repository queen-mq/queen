//! Background system + worker metrics collector.
//!
//! Ports the C++ MetricsCollector (system gauges -> queen.system_metrics) and the
//! per-worker throughput flush (queen.worker_metrics, which the AFTER INSERT
//! trigger rolls into queen.worker_metrics_summary). Without this loop those
//! tables stay empty and the System view + the dashboard throughput/lifetime
//! totals read zero.
//!
//! Every `METRICS_FLUSH_MS` (default 60s) it, on one pooled connection:
//!   * diffs the in-process op counters (metrics.rs) since the last flush and
//!     INSERTs one queen.worker_metrics row (per-minute deltas) for this replica;
//!   * samples host/process gauges (CPU + RSS via getrusage, DB pool via
//!     deadpool status) and INSERTs one queen.system_metrics row whose `metrics`
//!     JSONB matches the shape queen.get_system_metrics_v1 re-aggregates.
//!
//! Single-replica by design: every replica records its OWN worker/system rows
//! (keyed by hostname+worker_id), so unlike the stats reconciler this loop is NOT
//! advisory-locked — the dashboard aggregates across replicas.

use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;

use crate::config::Config;
use crate::db;
use crate::metrics::{Counters, Metrics, QueueSnap};

pub fn spawn(pool: Pool, metrics: Arc<Metrics>, cfg: &Config) {
    let interval = Duration::from_millis(cfg.metrics_flush_ms);
    let hostname = cfg.sync.server_id.clone();
    let port: i32 = cfg.port.parse().unwrap_or(6632);
    println!(
        "syscollect: metrics collector started (flush={}ms, replica={})",
        cfg.metrics_flush_ms, hostname
    );
    tokio::spawn(async move { run_loop(pool, metrics, interval, hostname, port).await });
}

async fn run_loop(pool: Pool, metrics: Arc<Metrics>, interval: Duration, hostname: String, port: i32) {
    let started = Instant::now();
    // Baseline snapshot: the first flush reflects only traffic since boot.
    let mut last: Counters = metrics.snapshot();
    // RUSTFIX item 24: per-queue baseline (diffed into queue_lag_metrics buckets).
    let mut last_pq = metrics.per_queue.snapshot();
    // Baseline cumulative CPU: the CPU gauge is the DELTA over each interval,
    // expressed as (percent × 100) — the units System.vue divides by 100 to plot %.
    let (mut last_user_us, mut last_sys_us, _) = rusage();
    // Stable pid for the (hostname, worker_id, pid, bucket) uniqueness key.
    let pid: i32 = std::process::id() as i32;

    loop {
        tokio::time::sleep(interval).await;
        let now = metrics.snapshot();
        let d = delta(&last, &now);
        last = now;

        let client = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("syscollect: pool error: {e}");
                continue;
            }
        };

        // --- worker throughput (per-minute deltas) ------------------------
        if let Err(e) = db::insert_worker_metrics(
            &client,
            &hostname,
            0, // worker_id: this broker is single-worker (async runtime, not fork-per-worker)
            pid,
            d.push_requests as i64,
            d.push_messages as i64,
            d.pop_requests as i64,
            d.pop_messages as i64,
            d.ack_requests as i64,
            d.ack_messages as i64,
            d.ack_success as i64,
            d.ack_failed as i64,
            d.transactions as i64,
            d.dlq_moved as i64,
            d.db_errors as i64,
        )
        .await
        {
            eprintln!("syscollect: worker_metrics insert error: {e}");
        }

        // --- system gauges ------------------------------------------------
        let uptime = started.elapsed().as_secs();
        let (pool_size, pool_idle, pool_active) = pool_gauges(&pool);
        // CPU: delta CPU-µs over the interval -> percent×100 (utilisation of one
        // core = 100%). secs = the wall interval; du/(secs*1e6) is the core-fraction,
        // ×100 -> %, ×100 again -> the centi-percent the frontend expects.
        let (cur_user_us, cur_sys_us, rss) = rusage();
        let secs = interval.as_secs_f64().max(1.0);
        let cpu_user = cur_user_us.saturating_sub(last_user_us) as f64 / (secs * 100.0);
        let cpu_sys = cur_sys_us.saturating_sub(last_sys_us) as f64 / (secs * 100.0);
        last_user_us = cur_user_us;
        last_sys_us = cur_sys_us;
        let metrics_json =
            build_system_metrics_json(uptime, cpu_user, cpu_sys, rss, pool_size, pool_idle, pool_active);
        if let Err(e) = db::insert_system_metrics(
            &client,
            &hostname,
            port,
            "worker-0",
            (interval.as_secs().max(1)) as i32,
            &metrics_json,
        )
        .await
        {
            eprintln!("syscollect: system_metrics insert error: {e}");
        }

        // --- per-queue throughput -> queue_lag_metrics --------------------
        // RUSTFIX item 24: diff each queue's cumulative counters into a per-minute
        // bucket (UPSERT-accumulate across replicas). Only queues with activity this
        // interval are written. Lag/ack/parked columns keep their table defaults —
        // the segments broker does not track them per queue.
        let now_pq = metrics.per_queue.snapshot();
        for (queue, cur) in &now_pq {
            let prev = last_pq.get(queue).copied().unwrap_or_default();
            let d = QueueSnap {
                push_requests: cur.push_requests.saturating_sub(prev.push_requests),
                push_messages: cur.push_messages.saturating_sub(prev.push_messages),
                pop_count: cur.pop_count.saturating_sub(prev.pop_count),
                pop_empty: cur.pop_empty.saturating_sub(prev.pop_empty),
                transactions: cur.transactions.saturating_sub(prev.transactions),
            };
            if d.push_requests == 0
                && d.push_messages == 0
                && d.pop_count == 0
                && d.pop_empty == 0
                && d.transactions == 0
            {
                continue;
            }
            if let Err(e) = db::upsert_queue_lag_metrics(
                &client,
                queue,
                d.pop_count as i64,
                d.push_requests as i64,
                d.push_messages as i64,
                d.pop_empty as i64,
                d.transactions as i64,
            )
            .await
            {
                eprintln!("syscollect: queue_lag_metrics upsert error ({queue}): {e}");
            }
        }
        last_pq = now_pq;
    }
}

fn delta(prev: &Counters, now: &Counters) -> Counters {
    // Counters are monotone; saturating_sub guards a counter reset (never expected
    // in-process, but keeps a delta non-negative if it ever happens).
    Counters {
        push_requests: now.push_requests.saturating_sub(prev.push_requests),
        push_messages: now.push_messages.saturating_sub(prev.push_messages),
        pop_requests: now.pop_requests.saturating_sub(prev.pop_requests),
        pop_messages: now.pop_messages.saturating_sub(prev.pop_messages),
        ack_requests: now.ack_requests.saturating_sub(prev.ack_requests),
        ack_messages: now.ack_messages.saturating_sub(prev.ack_messages),
        ack_success: now.ack_success.saturating_sub(prev.ack_success),
        ack_failed: now.ack_failed.saturating_sub(prev.ack_failed),
        transactions: now.transactions.saturating_sub(prev.transactions),
        dlq_moved: now.dlq_moved.saturating_sub(prev.dlq_moved),
        db_errors: now.db_errors.saturating_sub(prev.db_errors),
    }
}

// deadpool pool utilisation as (configured size, idle, active). `available` can be
// negative under contention, so clamp both derived gauges at 0.
fn pool_gauges(pool: &Pool) -> (i64, i64, i64) {
    let s = pool.status();
    let size = s.max_size as i64;
    let idle = (s.available.max(0)) as i64;
    let active = (s.size as i64 - s.available.max(0) as i64).max(0);
    (size, idle, active)
}

// getrusage(2): cumulative CPU (µs) + peak RSS. ru_maxrss is bytes on macOS,
// kilobytes on Linux — normalise to bytes.
fn rusage() -> (u64, u64, u64) {
    unsafe {
        let mut u: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut u) != 0 {
            return (0, 0, 0);
        }
        let user_us = u.ru_utime.tv_sec as u64 * 1_000_000 + u.ru_utime.tv_usec as u64;
        let sys_us = u.ru_stime.tv_sec as u64 * 1_000_000 + u.ru_stime.tv_usec as u64;
        let maxrss = u.ru_maxrss as u64;
        let rss_bytes = if cfg!(target_os = "macos") { maxrss } else { maxrss.saturating_mul(1024) };
        (user_us, sys_us, rss_bytes)
    }
}

// Build the metrics JSONB in the exact nested shape queen.get_system_metrics_v1
// re-aggregates: every numeric leaf is {avg,min,max,last} (all equal for an
// instantaneous sample). Gauges this broker doesn't have (threadpool queues,
// cluster sidecar/transport) are present-but-zero so the SP never sees NULLs and
// the System view renders every panel.
#[allow(clippy::too_many_arguments)]
fn build_system_metrics_json(
    uptime: u64,
    cpu_user: f64,
    cpu_sys: f64,
    rss: u64,
    pool_size: i64,
    pool_idle: i64,
    pool_active: i64,
) -> String {
    // {avg,min,max,last} with all fields equal to `v`.
    fn m(v: f64) -> serde_json::Value {
        serde_json::json!({ "avg": v, "min": v, "max": v, "last": v })
    }
    let zero = || m(0.0);
    let body = serde_json::json!({
        // user_us/system_us carry percent×100 (System.vue plots the value / 100).
        "cpu": { "user_us": m(cpu_user), "system_us": m(cpu_sys) },
        "memory": { "rss_bytes": m(rss as f64), "virtual_bytes": m(0.0) },
        "database": {
            "pool_size": m(pool_size as f64),
            "pool_idle": m(pool_idle as f64),
            "pool_active": m(pool_active as f64),
        },
        "threadpool": {
            "db": { "pool_size": zero(), "queue_size": zero() },
            "system": { "pool_size": zero(), "queue_size": zero() },
        },
        "registries": { "response": zero() },
        "uptime_seconds": uptime,
        "shared_state": {
            "enabled": false,
            "sidecar_ops": {
                "push": { "count": zero(), "latency_us": zero(), "items": zero() },
                "pop":  { "count": zero(), "latency_us": zero(), "items": zero() },
                "ack":  { "count": zero(), "latency_us": zero(), "items": zero() },
            },
            "queue_backoff": {
                "queues_with_backoff": zero(),
                "total_backed_off_groups": zero(),
                "avg_interval_ms": zero(),
            },
            "queue_backoff_summary": [],
            "queue_config_cache": { "size": zero(), "hits": zero(), "misses": zero() },
            "consumer_presence": {
                "queues_tracked": zero(), "servers_tracked": zero(), "total_registrations": zero(),
            },
            "server_health": { "alive": m(1.0), "dead": m(0.0) },
            "transport": { "sent": zero(), "received": zero(), "dropped": zero() },
        }
    });
    body.to_string()
}
