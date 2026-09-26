//! The raft-mode metrics collector, with the node's
//! [`DashStore`](super::store::DashStore) as its sink (PLAN_RAFT.md §10.2:
//! "metrics collector → local.db").
//!
//! Every `METRICS_FLUSH_MS` (default 60 s) it diffs the process counters the
//! raft facade feeds ([`crate::metrics::global`]) into one [`WorkerRow`], one
//! [`SystemRow`] (CPU, RSS, and the raft family), and a [`QueueRow`] /
//! [`ParkedRow`] per queue with activity.
//!
//! One thread per process, started by the first facade that opens.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::model::{trunc_us, ParkedRow, QueueRow, SystemRow, WorkerRow, US_PER_MIN, US_PER_SEC};
use super::store::{DashStore, Flush};

/// The raft family of a [`SystemRow`]: what this node's log and store look
/// like at flush time (`inflight`, `applied_lag`, `log_bytes`, `log_files`,
/// `map_used_pct`), sampled by the facade that started the collector.
pub type RaftGauges = Arc<dyn Fn() -> Option<[f64; 5]> + Send + Sync>;

static STARTED: AtomicBool = AtomicBool::new(false);

/// Start the collector once per process. `node_id` names the rows.
pub fn spawn_once(store: Arc<DashStore>, node_id: u64, gauges: RaftGauges) {
    if STARTED.swap(true, Ordering::AcqRel) {
        return;
    }
    let interval = Duration::from_millis(
        std::env::var("METRICS_FLUSH_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|v| *v >= 1000)
            .unwrap_or(60_000),
    );
    let spawned = std::thread::Builder::new()
        .name("queen-dash-collector".into())
        .spawn(move || run(store, node_id, gauges, interval));
    if let Err(e) = spawned {
        STARTED.store(false, Ordering::Release);
        tracing::warn!(target: "metrics", error = %e, "raft metrics collector did not start");
    }
}

fn wall_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

fn run(store: Arc<DashStore>, node_id: u64, gauges: RaftGauges, interval: Duration) {
    // The facade installs the process metrics at boot; wait for them.
    let metrics = loop {
        if let Some(m) = crate::metrics::global() {
            break m.clone();
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    let hostname = super::node_label(node_id);
    let port = super::http_port();
    let pid = std::process::id() as i32;
    let started = Instant::now();
    tracing::info!(
        target: "metrics",
        flush_ms = interval.as_millis() as u64,
        node = %hostname,
        "raft metrics collector started"
    );

    let mut last = metrics.snapshot();
    let mut last_pq = metrics.per_queue.snapshot();
    let (mut last_user_us, mut last_sys_us, _) = crate::syscollect::rusage();
    let mut last_evl_sum = metrics.evl_sum_us.load(Ordering::Relaxed);
    let mut last_evl_cnt = metrics.evl_count.load(Ordering::Relaxed);

    loop {
        std::thread::sleep(interval);
        let now = wall_us();
        let at_us = trunc_us(now, US_PER_SEC);
        let bucket_us = trunc_us(now, US_PER_MIN);

        // --- worker row (per-flush deltas) -------------------------------
        let snap = metrics.snapshot();
        let d = crate::syscollect::delta(&last, &snap);
        last = snap;
        let evl_sum = metrics.evl_sum_us.load(Ordering::Relaxed);
        let evl_cnt = metrics.evl_count.load(Ordering::Relaxed);
        let (d_sum, d_cnt) = (
            evl_sum.saturating_sub(last_evl_sum),
            evl_cnt.saturating_sub(last_evl_cnt),
        );
        last_evl_sum = evl_sum;
        last_evl_cnt = evl_cnt;
        let avg_evl_ms = if d_cnt > 0 {
            ((d_sum / d_cnt) as f64 / 1000.0).round() as i32
        } else {
            0
        };
        let max_evl_ms = (metrics.evl_max_us.swap(0, Ordering::Relaxed) / 1000) as i32;

        let now_pq = metrics.per_queue.snapshot();
        let lag_max = metrics.per_queue.take_lag_max();
        let (mut w_lag_sum, mut w_lag_n) = (0u64, 0u64);
        for (queue, cur) in &now_pq {
            let prev = last_pq.get(queue).copied().unwrap_or_default();
            w_lag_sum += cur.lag_sum_ms.saturating_sub(prev.lag_sum_ms);
            w_lag_n += cur.lag_count.saturating_sub(prev.lag_count);
        }
        let worker = WorkerRow {
            at_us,
            hostname: hostname.clone(),
            worker_id: 0,
            pid,
            push_requests: d.push_requests as i64,
            push_messages: d.push_messages as i64,
            pop_requests: d.pop_requests as i64,
            pop_messages: d.pop_messages as i64,
            ack_requests: d.ack_requests as i64,
            ack_messages: d.ack_messages as i64,
            ack_success: d.ack_success as i64,
            ack_failed: d.ack_failed as i64,
            transactions: d.transactions as i64,
            dlq: d.dlq_moved as i64,
            db_errors: d.db_errors as i64,
            avg_event_loop_lag_ms: avg_evl_ms,
            max_event_loop_lag_ms: max_evl_ms,
            avg_lag_ms: if w_lag_n > 0 {
                (w_lag_sum / w_lag_n) as i64
            } else {
                0
            },
            max_lag_ms: lag_max.values().copied().max().unwrap_or(0) as i64,
            lag_count: w_lag_n as i64,
        };

        // --- system row ---------------------------------------------------
        let (user_us, sys_us, rss) = crate::syscollect::rusage();
        let secs = interval.as_secs_f64().max(1.0);
        // percent × 100, the unit System.vue divides by 100.
        let cpu_user = user_us.saturating_sub(last_user_us) as f64 / (secs * 100.0);
        let cpu_sys = sys_us.saturating_sub(last_sys_us) as f64 / (secs * 100.0);
        last_user_us = user_us;
        last_sys_us = sys_us;
        let system = SystemRow {
            at_us,
            hostname: hostname.clone(),
            port,
            worker_id: "worker-0".into(),
            sample_count: interval.as_secs().max(1) as i32,
            metrics_json: system_json(
                started.elapsed().as_secs(),
                cpu_user,
                cpu_sys,
                rss,
                gauges(),
            )
            .to_string(),
        };

        // --- per-queue rows (only queues with activity) --------------------
        let mut parked = crate::syscollect::drain_parked_avg(&metrics, interval);
        let mut queue_rows = Vec::new();
        let mut conflated_now: Vec<(String, String, i64)> = Vec::new();
        let mut parked_rows = Vec::new();
        for (key, cur) in &now_pq {
            let prev = last_pq.get(key).copied().unwrap_or_default();
            let parked_avg = parked.remove(key.as_str()).unwrap_or(0);
            let (tenant, queue) = crate::handlers::split_tenant_queue(key);
            let row = QueueRow {
                bucket_us,
                tenant: tenant.to_string(),
                queue: queue.to_string(),
                pop_messages: cur.pop_count.saturating_sub(prev.pop_count) as i64,
                push_requests: cur.push_requests.saturating_sub(prev.push_requests) as i64,
                push_messages: cur.push_messages.saturating_sub(prev.push_messages) as i64,
                pop_empty: cur.pop_empty.saturating_sub(prev.pop_empty) as i64,
                transactions: cur.transactions.saturating_sub(prev.transactions) as i64,
                ack_requests: cur.ack_requests.saturating_sub(prev.ack_requests) as i64,
                ack_success: cur.ack_success.saturating_sub(prev.ack_success) as i64,
                ack_failed: cur.ack_failed.saturating_sub(prev.ack_failed) as i64,
                avg_lag_ms: {
                    let n = cur.lag_count.saturating_sub(prev.lag_count);
                    if n > 0 {
                        (cur.lag_sum_ms.saturating_sub(prev.lag_sum_ms) / n) as i64
                    } else {
                        0
                    }
                },
                max_lag_ms: lag_max.get(key.as_str()).copied().unwrap_or(0) as i64,
                lag_count: cur.lag_count.saturating_sub(prev.lag_count) as i64,
                parked_count: parked_avg,
                conflated: cur.conflated.saturating_sub(prev.conflated) as i64,
            };
            let active = row.push_requests != 0
                || row.push_messages != 0
                || row.pop_messages != 0
                || row.pop_empty != 0
                || row.transactions != 0
                || row.ack_requests != 0
                || row.conflated != 0
                || parked_avg != 0;
            if !active {
                continue;
            }
            if parked_avg > 0 {
                parked_rows.push(ParkedRow {
                    bucket_us,
                    tenant: row.tenant.clone(),
                    queue: row.queue.clone(),
                    hostname: hostname.clone(),
                    worker_id: 0,
                    parked_count: parked_avg,
                });
            }
            if row.conflated > 0 {
                conflated_now.push((row.tenant.clone(), row.queue.clone(), row.conflated));
            }
            queue_rows.push(row);
        }
        *LAST_CONFLATED.lock().unwrap_or_else(|p| p.into_inner()) = conflated_now;
        // Queues that only had parked long-polls this interval.
        for (key, parked_avg) in parked {
            if parked_avg == 0 {
                continue;
            }
            let (tenant, queue) = crate::handlers::split_tenant_queue(&key);
            queue_rows.push(QueueRow {
                bucket_us,
                tenant: tenant.to_string(),
                queue: queue.to_string(),
                parked_count: parked_avg,
                ..Default::default()
            });
            parked_rows.push(ParkedRow {
                bucket_us,
                tenant: tenant.to_string(),
                queue: queue.to_string(),
                hostname: hostname.clone(),
                worker_id: 0,
                parked_count: parked_avg,
            });
        }
        last_pq = now_pq;

        // Close the partition-churn minutes apply recorded (RAM until then).
        crate::rsm::local_metrics::flush_all_churn(now);

        store.append(Flush {
            worker: vec![worker],
            system: vec![system],
            queue: queue_rows,
            parked: parked_rows,
        });
    }
}

/// The `metrics` JSON of a [`SystemRow`] (every leaf `{avg,min,max,last}`),
/// plus `raft`.
fn system_json(uptime: u64, cpu_user: f64, cpu_sys: f64, rss: u64, raft: Option<[f64; 5]>) -> Value {
    fn m(v: f64) -> Value {
        json!({ "avg": v, "min": v, "max": v, "last": v })
    }
    let mut body = json!({
        "cpu": { "user_us": m(cpu_user), "system_us": m(cpu_sys) },
        "memory": { "rss_bytes": m(rss as f64), "virtual_bytes": m(0.0) },
        "registries": { "response": m(0.0) },
        "uptime_seconds": uptime,
    });
    if let (Some([inflight, applied_lag, log_bytes, log_files, map_used_pct]), Some(o)) =
        (raft, body.as_object_mut())
    {
        o.insert(
            "raft".into(),
            json!({
                "inflight": m(inflight),
                "applied_lag": m(applied_lag),
                "log_bytes": m(log_bytes),
                "log_files": m(log_files),
                "map_used_pct": m(map_used_pct),
            }),
        );
    }
    body
}

/// The last bucket's conflated count per (tenant, queue): what
/// `queen_queue_conflated_per_minute` exports on `/metrics/prometheus`.
static LAST_CONFLATED: std::sync::Mutex<Vec<(String, String, i64)>> =
    std::sync::Mutex::new(Vec::new());

pub(crate) fn last_conflated() -> Vec<(String, String, i64)> {
    LAST_CONFLATED
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
}
