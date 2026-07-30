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
    tracing::info!(
        target: "metrics",
        flush_ms = cfg.metrics_flush_ms,
        replica = %hostname,
        "collector started"
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
    // Baseline for the scheduler-lag ("event loop") probe accumulators.
    let mut last_evl_sum: u64 = 0;
    let mut last_evl_cnt: u64 = 0;
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
                static POOL_ERR: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
                if let Some(suppressed) = POOL_ERR.tick_now() {
                    tracing::warn!(target: "metrics", error = %e, suppressed, "pool error");
                }
                continue;
            }
        };

        // --- worker throughput (per-minute deltas) ------------------------
        // Scheduler ("event loop") lag over this interval: diff the cumulative
        // sum/count from the 100ms probe (metrics::spawn_samplers), swap-drain the
        // interval max. Feeds worker_metrics.avg/max_event_loop_lag_ms — the
        // dashboard's "Event loop" row.
        let evl_sum = metrics.evl_sum_us.load(std::sync::atomic::Ordering::Relaxed);
        let evl_cnt = metrics.evl_count.load(std::sync::atomic::Ordering::Relaxed);
        let d_evl_sum = evl_sum.saturating_sub(last_evl_sum);
        let d_evl_cnt = evl_cnt.saturating_sub(last_evl_cnt);
        last_evl_sum = evl_sum;
        last_evl_cnt = evl_cnt;
        let avg_evl_ms: i32 = if d_evl_cnt > 0 {
            ((d_evl_sum / d_evl_cnt) as f64 / 1000.0).round() as i32
        } else {
            0
        };
        let max_evl_ms: i32 =
            (metrics.evl_max_us.swap(0, std::sync::atomic::Ordering::Relaxed) / 1000) as i32;

        // Worker-level pop lag for this interval: fold the per-queue lag deltas
        // (worker_metrics.avg/max_lag_ms + lag_count feed the System view's
        // worker-health and the dashboard Time-lag aggregation).
        let now_pq = metrics.per_queue.snapshot();
        let lag_max = metrics.per_queue.take_lag_max();
        let (mut w_lag_sum, mut w_lag_n) = (0u64, 0u64);
        for (queue, cur) in &now_pq {
            let prev = last_pq.get(queue).copied().unwrap_or_default();
            w_lag_sum += cur.lag_sum_ms.saturating_sub(prev.lag_sum_ms);
            w_lag_n += cur.lag_count.saturating_sub(prev.lag_count);
        }
        let w_lag_avg: i64 = if w_lag_n > 0 { (w_lag_sum / w_lag_n) as i64 } else { 0 };
        let w_lag_max: i64 = lag_max.values().copied().max().unwrap_or(0) as i64;

        // Pool gauges are sampled BEFORE the worker_metrics insert so the row
        // carries the connection-pool state of the interval it describes.
        let (pool_size, pool_idle, pool_active) = pool_gauges(&pool);
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
            avg_evl_ms,
            max_evl_ms,
            w_lag_avg,
            w_lag_max,
            w_lag_n as i64,
            pool_active as i32,
            pool_idle as i32,
        )
        .await
        {
            static WORKER_ERR: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
            if let Some(suppressed) = WORKER_ERR.tick_now() {
                tracing::warn!(target: "metrics", error = %e, suppressed, "worker_metrics insert error");
            }
        }

        // --- system gauges ------------------------------------------------
        let uptime = started.elapsed().as_secs();
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
            static SYSTEM_ERR: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
            if let Some(suppressed) = SYSTEM_ERR.tick_now() {
                tracing::warn!(target: "metrics", error = %e, suppressed, "system_metrics insert error");
            }
        }

        // --- per-queue throughput -> queue_lag_metrics --------------------
        // RUSTFIX item 24: diff each queue's cumulative counters into a per-minute
        // bucket (UPSERT-accumulate across replicas). Lag merges weighted, parked
        // is this replica's minute-average of the 1 Hz parked samples. Only queues
        // with any activity (including parked-only idle consumers) are written.
        // (now_pq / lag_max were captured above for the worker-level fold.)
        let mut parked = drain_parked_avg(&metrics, interval);
        for (queue, cur) in &now_pq {
            // Track B (§5): now_pq/parked are keyed by tenant_queue_key(tenant,
            // queue). Split back to (tenant, name) so each metric row is written
            // under its own tenant. Flag OFF ⇒ tenant is always the default.
            let (q_tenant, q_name) = crate::handlers::split_tenant_queue(queue);
            let prev = last_pq.get(queue).copied().unwrap_or_default();
            let d = QueueSnap {
                push_requests: cur.push_requests.saturating_sub(prev.push_requests),
                push_messages: cur.push_messages.saturating_sub(prev.push_messages),
                pop_count: cur.pop_count.saturating_sub(prev.pop_count),
                pop_empty: cur.pop_empty.saturating_sub(prev.pop_empty),
                transactions: cur.transactions.saturating_sub(prev.transactions),
                ack_requests: cur.ack_requests.saturating_sub(prev.ack_requests),
                ack_success: cur.ack_success.saturating_sub(prev.ack_success),
                ack_failed: cur.ack_failed.saturating_sub(prev.ack_failed),
                lag_sum_ms: cur.lag_sum_ms.saturating_sub(prev.lag_sum_ms),
                lag_count: cur.lag_count.saturating_sub(prev.lag_count),
            };
            let parked_avg = parked.remove(queue.as_str()).unwrap_or(0);
            if d.push_requests == 0
                && d.push_messages == 0
                && d.pop_count == 0
                && d.pop_empty == 0
                && d.transactions == 0
                && d.ack_requests == 0
                && parked_avg == 0
            {
                continue;
            }
            let avg_lag = if d.lag_count > 0 { (d.lag_sum_ms / d.lag_count) as i64 } else { 0 };
            if let Err(e) = db::upsert_queue_lag_metrics(
                &client,
                q_tenant,
                q_name,
                d.pop_count as i64,
                d.push_requests as i64,
                d.push_messages as i64,
                d.pop_empty as i64,
                d.transactions as i64,
                d.ack_requests as i64,
                d.ack_success as i64,
                d.ack_failed as i64,
                avg_lag,
                lag_max.get(queue.as_str()).copied().unwrap_or(0) as i64,
                d.lag_count as i64,
                parked_avg,
            )
            .await
            {
                static LAG_ERR: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
                if let Some(suppressed) = LAG_ERR.tick_now() {
                    tracing::warn!(target: "metrics", queue = %q_name, error = %e, suppressed, "queue_lag_metrics upsert error");
                }
            }
            if parked_avg > 0 {
                if let Err(e) =
                    db::upsert_queue_parked_replica(&client, q_tenant, q_name, &hostname, 0, parked_avg).await
                {
                    static PARKED_ERR: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
                    if let Some(suppressed) = PARKED_ERR.tick_now() {
                        tracing::warn!(target: "metrics", queue = %q_name, error = %e, suppressed, "queue_parked_replica upsert error");
                    }
                }
            }
        }
        // Queues that ONLY had parked long-polls this interval (no per_queue
        // counter entry yet — e.g. consumers idling on a never-pushed queue).
        for (queue, parked_avg) in parked {
            if parked_avg == 0 {
                continue;
            }
            let (q_tenant, q_name) = crate::handlers::split_tenant_queue(&queue);
            if let Err(e) = db::upsert_queue_lag_metrics(
                &client, q_tenant, q_name, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, parked_avg,
            )
            .await
            {
                static LAG_ERR_PARKED: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
                if let Some(suppressed) = LAG_ERR_PARKED.tick_now() {
                    tracing::warn!(target: "metrics", queue = %q_name, error = %e, suppressed, "queue_lag_metrics upsert error");
                }
            }
            if let Err(e) =
                db::upsert_queue_parked_replica(&client, q_tenant, q_name, &hostname, 0, parked_avg).await
            {
                static PARKED_ERR_ONLY: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
                if let Some(suppressed) = PARKED_ERR_ONLY.tick_now() {
                    tracing::warn!(target: "metrics", queue = %q_name, error = %e, suppressed, "queue_parked_replica upsert error");
                }
            }
        }
        last_pq = now_pq;
    }
}

// Minute-average of the 1 Hz parked samples for this interval: sum of samples
// divided by the interval's seconds (a queue parked for the whole minute with
// one consumer averages 1; parked 30s averages 0 after integer rounding-down
// only when < half — round to nearest instead).
fn drain_parked_avg(
    metrics: &Metrics,
    interval: Duration,
) -> std::collections::HashMap<String, i32> {
    let secs = interval.as_secs().max(1);
    metrics
        .parked
        .drain()
        .into_iter()
        .map(|(q, (sum, _n))| (q, ((sum as f64 / secs as f64).round()) as i32))
        .collect()
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

// getrusage(2): cumulative CPU (µs) + RSS in bytes.
//
// The RSS reported here is the CURRENT resident set, not ru_maxrss. ru_maxrss is
// the process high-water mark and never decreases, so charting it as "Memory
// Usage" turns any transient spike into a permanent plateau that reads as a
// leak. ru_maxrss is kept only as the last-resort fallback when the live figure
// is unavailable, and it is at least an upper bound then.
fn rusage() -> (u64, u64, u64) {
    unsafe {
        let mut u: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut u) != 0 {
            return (0, 0, 0);
        }
        let user_us = u.ru_utime.tv_sec as u64 * 1_000_000 + u.ru_utime.tv_usec as u64;
        let sys_us = u.ru_stime.tv_sec as u64 * 1_000_000 + u.ru_stime.tv_usec as u64;
        let maxrss = u.ru_maxrss as u64;
        let peak_bytes =
            if cfg!(target_os = "macos") { maxrss } else { maxrss.saturating_mul(1024) };
        (user_us, sys_us, current_rss_bytes().unwrap_or(peak_bytes))
    }
}

// Live resident-set size in bytes, or None when the platform hook is unavailable.
#[cfg(target_os = "linux")]
fn current_rss_bytes() -> Option<u64> {
    // /proc/self/statm field 2 = resident pages.
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return None;
    }
    Some(pages.saturating_mul(page_size as u64))
}

#[cfg(target_os = "macos")]
fn current_rss_bytes() -> Option<u64> {
    // proc_pidinfo(PROC_PIDTASKINFO) -> pti_resident_size (bytes).
    unsafe {
        let mut ti: libc::proc_taskinfo = std::mem::zeroed();
        let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
        let n = libc::proc_pidinfo(
            libc::getpid(),
            libc::PROC_PIDTASKINFO,
            0,
            &mut ti as *mut _ as *mut libc::c_void,
            size,
        );
        if n == size {
            Some(ti.pti_resident_size)
        } else {
            None
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn current_rss_bytes() -> Option<u64> {
    None
}

// Build the metrics JSONB in the exact nested shape queen.get_system_metrics_v1
// re-aggregates: every numeric leaf is {avg,min,max,last} (all equal for an
// instantaneous sample). A gauge this broker genuinely does not have is OMITTED
// rather than written as zero — the reader then yields NULL, which a chart draws
// as a gap instead of a confident flat zero line. The cluster sidecar/transport
// block stays present because `shared_state.enabled:false` already says, in the
// payload itself, that its numbers are not measurements.
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
        // No "threadpool" family: this broker is async (tokio) and has no worker
        // thread pool or job queue to measure. Emitting present-but-zero gauges
        // made the System view plot a permanently flat line as if it were a live
        // measurement; ABSENT makes the reader fall through to NULL, which is
        // the truth ("not measured here"), not zero.
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
