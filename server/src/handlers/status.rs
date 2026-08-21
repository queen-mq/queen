#![allow(unused_imports)]
use super::*;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use deadpool_postgres::Pool;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::db;
use crate::frames::{
    pack_frames, unpack_frames, uuid_bytes_to_string, uuid_string_to_bytes, zstd_compress,
    zstd_decompress, FrameIn,
};
use crate::fusion::{json_escape_into, AddMsg, Fusion, ItemResult, OwnedFrame, PushState};
use crate::metrics::Metrics;
use crate::util::uuidv7_bytes;

pub async fn handle_status() -> Response {
    json(StatusCode::OK, "{\"status\":\"ok\",\"engine\":\"segments-rust\"}".to_string())
}

// RUSTFIX item 21: GET /metrics returns the C++ JSON shape (metrics.cpp:11-46),
// NOT Prometheus text — Prometheus lives at /metrics/prometheus only. The C++
// original populated only the `database` block from the pool; the rest were
// literal 0. We populate uptime/requests/messages/memory as a safe superset.
pub async fn handle_metrics(State(st): State<Arc<AppState>>) -> Response {
    let snap = st.metrics.snapshot();
    let ps = st.pool.status();
    let out = serde_json::json!({
        "uptime": st.metrics.uptime_seconds(),
        "requests": {
            "total": snap.push_requests + snap.pop_requests + snap.ack_requests,
            "rate": 0,
        },
        "messages": {
            "total": snap.push_messages + snap.pop_messages + snap.ack_messages,
            "rate": 0,
        },
        "database": {
            "poolSize": ps.max_size as i64,
            "idleConnections": (ps.available as i64).max(0),
            "waitingRequests": 0,
        },
        "memory": {
            "rss": st.metrics.resident_bytes(),
            "heapTotal": 0, "heapUsed": 0, "external": 0, "arrayBuffers": 0,
        },
        "cpu": { "user": 0, "system": 0 },
    });
    json(StatusCode::OK, out.to_string())
}

// ============================================================ management surface
// Read/observe endpoints for the segments broker (messages/dlq/traces/status/
// health/prometheus). These ADD to the data hot-path handlers above; they never
// touch push/pop/ack/transaction/configure.

// ------------------------------------------------------------ GET /api/v1/status
pub async fn handle_api_status(
    State(st): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let filters = filters_from_query(&params, &["from", "to", "queue", "namespace", "task"]);
    let filters_json = serde_json::Value::Object(filters).to_string();
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_status(&client, &filters_json).await {
        Ok(txt) => sp_result_to_response(txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("status failed: ", &e),
        ),
    }
}

// ----------------------------------------------------- GET /api/v1/status/queues
pub async fn handle_status_queues(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    // `queue` is forwarded too — the SP filters on it, so a client asking for one
    // queue no longer has to download the tenant's whole list and filter locally.
    let mut filters = filters_from_query(&params, &["from", "to", "queue", "namespace", "task"]);
    // Track B (§5): queen.get_status_queues_v2 reads `_tenant` from the filter JSON
    // and scopes the per-queue status listing to it (default tenant when off).
    filters.insert("_tenant".to_string(), serde_json::json!(tenant.as_str()));
    let filters_json = serde_json::Value::Object(filters).to_string();
    let limit = qint(&params, "limit", 100);
    let offset = qint(&params, "offset", 0);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_status_queues(&client, &filters_json, limit, offset).await {
        Ok(txt) => sp_result_to_response(txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("status queues failed: ", &e),
        ),
    }
}

// -------------------------------------------------------------------- GET /health
// 200 when a trivial DB round-trip succeeds, 503 otherwise.
pub async fn handle_health(State(st): State<Arc<AppState>>) -> Response {
    let healthy = match st.pool.get().await {
        Ok(c) => db::ping(&c).await.is_ok(),
        Err(_) => false,
    };
    if healthy {
        json(
            StatusCode::OK,
            format!(
                "{{\"status\":\"healthy\",\"database\":\"connected\",\"engine\":\"segments-rust\",\"version\":\"{}\"}}",
                crate::VERSION
            ),
        )
    } else {
        json(
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "{{\"status\":\"unhealthy\",\"database\":\"disconnected\",\"engine\":\"segments-rust\",\"version\":\"{}\"}}",
                crate::VERSION
            ),
        )
    }
}

// Escape a Prometheus label value (backslash, double-quote, newline).
fn prom_label_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

// Append a subset of the get_prometheus_metrics_v1 JSON blob as exposition lines.
fn format_db_prometheus(txt: &str, out: &mut String) {
    let v: serde_json::Value = match serde_json::from_str(txt) {
        Ok(v) => v,
        Err(_) => return,
    };
    // RUSTFIX item 24: DB-backed cluster-LIFETIME totals (queen.worker_metrics_summary,
    // populated by the 019_worker_metrics trigger from syscollect.rs) under the CANONICAL
    // queen_cluster_* names — the same names a v0.16.0 dashboard's max(queen_cluster_*)
    // panels use. (The in-process live counters were renamed queen_process_* in
    // metrics.rs, vacating this namespace.) Every family gets HELP/TYPE.
    if let Some(t) = v.get("system_totals").and_then(|x| x.as_object()) {
        let map = [
            ("pushRequests", "queen_cluster_push_requests_total"),
            ("popRequests", "queen_cluster_pop_requests_total"),
            ("ackRequests", "queen_cluster_ack_requests_total"),
            ("transactions", "queen_cluster_transactions_total"),
            ("pushMessages", "queen_cluster_push_messages_total"),
            ("popMessages", "queen_cluster_pop_messages_total"),
            ("ackMessages", "queen_cluster_ack_messages_total"),
            ("dbErrors", "queen_cluster_db_errors_total"),
            ("dlqCount", "queen_cluster_dlq_total"),
        ];
        for (k, metric) in map {
            if let Some(n) = t.get(k).and_then(|x| x.as_i64()) {
                out.push_str(&format!("# HELP {metric} DB-backed cluster lifetime total\n# TYPE {metric} counter\n"));
                out.push_str(metric);
                out.push_str("{scope=\"cluster\"} ");
                out.push_str(&n.to_string());
                out.push('\n');
            }
        }
        // ack success/failed as a single family split by result (prometheus.cpp:410-418).
        out.push_str("# HELP queen_cluster_ack_total Acks by outcome (DB-backed)\n# TYPE queen_cluster_ack_total counter\n");
        for (k, res) in [("ackSuccess", "success"), ("ackFailed", "failed")] {
            if let Some(n) = t.get(k).and_then(|x| x.as_i64()) {
                out.push_str(&format!("queen_cluster_ack_total{{scope=\"cluster\",result=\"{res}\"}} {n}\n"));
            }
        }
    }
    // Per-queue minute-rate gauges (RUSTFIX item 24, prometheus.cpp:442-487).
    //
    // PLAN_CONFLATION §6.3 adds queen_queue_conflated_per_minute here: log
    // positions retired by conflation WITHOUT a handler invocation, read from
    // queen.queue_lag_metrics.conflated_count. It sits next to pop_messages on
    // purpose — the pair is the whole story of a conflating queue, what was
    // handled and what was skipped because something newer had already arrived.
    // 0 on every queue whose groups do not conflate.
    //
    // KEEP THE COMMENTS OUT OF THE ARRAY BELOW. webdoc/scripts/gen-metrics.mjs
    // attaches the templated `# HELP {fam}` line to the family names in the
    // nearest preceding `[`, searching only 900 characters back; a comment
    // inside the literal pushes the bracket out of that window and every family
    // here lands in the reference with empty Help and Type cells (a hard error
    // since the generator stopped warning and started failing).
    if let Some(arr) = v.get("per_queue_lag").and_then(|x| x.as_array()) {
        let fams = [
            ("queen_queue_pop_messages_per_minute", "pop_count"),
            ("queen_queue_push_requests_per_minute", "push_request_count"),
            ("queen_queue_push_messages_per_minute", "push_message_count"),
            ("queen_queue_pop_empty_per_minute", "pop_empty_count"),
            ("queen_queue_transactions_per_minute", "transaction_count"),
            ("queen_queue_conflated_per_minute", "conflated_count"),
            ("queen_queue_parked_consumers", "parked_count"),
            ("queen_queue_metrics_age_seconds", "bucket_age_seconds"),
        ];
        for (fam, _) in fams {
            out.push_str(&format!("# HELP {fam} Per-queue minute-rate\n# TYPE {fam} gauge\n"));
        }
        out.push_str("# HELP queen_queue_pop_lag_milliseconds Per-queue pop lag\n# TYPE queen_queue_pop_lag_milliseconds gauge\n");
        out.push_str("# HELP queen_queue_ack_per_minute Per-queue acks by result\n# TYPE queen_queue_ack_per_minute gauge\n");
        for e in arr {
            let q = prom_label_escape(e.get("queue").and_then(|x| x.as_str()).unwrap_or(""));
            let num = |k: &str| e.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0);
            for (fam, key) in fams {
                out.push_str(&format!("{fam}{{queue=\"{q}\"}} {}\n", num(key)));
            }
            out.push_str(&format!("queen_queue_pop_lag_milliseconds{{queue=\"{q}\",stat=\"avg\"}} {}\n", num("avg_lag_ms")));
            out.push_str(&format!("queen_queue_pop_lag_milliseconds{{queue=\"{q}\",stat=\"max\"}} {}\n", num("max_lag_ms")));
            out.push_str(&format!("queen_queue_ack_per_minute{{queue=\"{q}\",result=\"success\"}} {}\n", num("ack_success_count")));
            out.push_str(&format!("queen_queue_ack_per_minute{{queue=\"{q}\",result=\"failed\"}} {}\n", num("ack_failed_count")));
        }
    }
    // DLQ depth (cluster total + per-queue).
    if let Some(d) = v.get("dlq") {
        out.push_str("# HELP queen_dlq_depth Dead-letter queue depth\n# TYPE queen_dlq_depth gauge\n");
        out.push_str("# HELP queen_dlq_depth_by_queue Dead-letter depth per queue\n# TYPE queen_dlq_depth_by_queue gauge\n");
        if let Some(n) = d.get("total").and_then(|x| x.as_i64()) {
            out.push_str("queen_dlq_depth{scope=\"cluster\"} ");
            out.push_str(&n.to_string());
            out.push('\n');
        }
        if let Some(arr) = d.get("per_queue").and_then(|x| x.as_array()) {
            for e in arr {
                let q = e.get("queue").and_then(|x| x.as_str()).unwrap_or("");
                let c = e.get("count").and_then(|x| x.as_i64()).unwrap_or(0);
                out.push_str(&format!(
                    "queen_dlq_depth_by_queue{{queue=\"{}\"}} {}\n",
                    prom_label_escape(q),
                    c
                ));
            }
        }
    }
    // Queue depth: DELETED, deliberately — PLAN_CONFLATION §6.3's "caution found
    // in passing", resolved here because §6.3 adds a family next to these two and
    // the docs generator cannot tell live code from dead.
    //
    // `queen_queue_depth_total` and `queen_queue_depth_pending` read a
    // `queue_depth` key that queen.get_prometheus_metrics_v1 (023_prometheus.sql)
    // has never built — grep it: the SP returns system_totals / per_queue_lag /
    // per_worker / dlq and nothing else. So the block never ran, not one sample
    // was ever exposed, and not even the `# HELP` lines were emitted (they sit
    // inside the same `if let Some(arr)`). What DID exist was the documentation:
    // gen-metrics.mjs scrapes `# HELP` strings out of this file, so both families
    // were published in the manual as if they worked.
    //
    // Removing them changes nothing an operator can observe — a family with no
    // samples is not in the exposition — and it stops the generator from
    // documenting a metric the broker cannot produce. Per-queue depth is not
    // being dropped as an idea: it is per-GROUP work (a conflating group's
    // pending is not the queue's), which is exactly what
    // GET /api/v1/resources/queues/:q/depth answers (§2.5/§5.3). Making
    // queen.stats conflation-aware is out of scope on purpose (§2.5, §9).
}

// -------------------------------------------------------- GET /metrics/prometheus
// In-process gauges (same as /metrics) + a subset of the DB-derived metrics from
// get_prometheus_metrics_v1. DB block is best-effort — the in-process gauges are
// always emitted even if the DB read fails.
pub async fn handle_prometheus(State(st): State<Arc<AppState>>) -> Response {
    let mut body = st.metrics.prometheus();
    let adm = st.admission.snapshot();
    body.push_str("# HELP queen_admission_budget Write-transaction admission budget\n# TYPE queen_admission_budget gauge\n");
    body.push_str(&format!("queen_admission_budget {}\n", adm.budget));
    body.push_str("# HELP queen_admission_inflight Admitted write transactions per lane\n# TYPE queen_admission_inflight gauge\n");
    body.push_str("# HELP queen_admission_waiting Waiters per lane\n# TYPE queen_admission_waiting gauge\n");
    for (i, lane) in ["push", "pop", "ack", "maint"].iter().enumerate() {
        body.push_str(&format!(
            "queen_admission_inflight{{lane=\"{}\"}} {}\nqueen_admission_waiting{{lane=\"{}\"}} {}\n",
            lane, adm.inflight[i], lane, adm.waiting[i]
        ));
    }
    body.push_str("# HELP queen_admission_trains_per_s Commit trains per second (flush cycles)\n# TYPE queen_admission_trains_per_s gauge\n");
    body.push_str(&format!("queen_admission_trains_per_s {:.2}\n", adm.trains_per_s));
    body.push_str("# HELP queen_admission_txn_per_train Mean transactions per commit train\n# TYPE queen_admission_txn_per_train gauge\n");
    body.push_str(&format!("queen_admission_txn_per_train {:.2}\n", adm.txn_per_train_avg));
    body.push_str("# HELP queen_admission_cycle_ms Median flush-cycle duration\n# TYPE queen_admission_cycle_ms gauge\n");
    body.push_str(&format!("queen_admission_cycle_ms {:.3}\n", adm.cycle_ms.unwrap_or(0.0)));

    // RUSTFIX item 24: restore the DB pool gauges (prometheus.cpp:157-174), the
    // maintenance-mode gauge (prometheus.cpp:253-266), and the file-buffer gauges
    // (now live, item 1). Per-worker/threadpool/registry/sidecar families are
    // intentionally obsolete for the single-process async broker.
    let ps = st.pool.status();
    let active = (ps.size as i64 - ps.available as i64).max(0);
    body.push_str("# HELP queen_db_pool_size Configured DB pool size\n# TYPE queen_db_pool_size gauge\n");
    body.push_str(&format!("queen_db_pool_size {}\n", ps.max_size));
    body.push_str("# HELP queen_db_pool_idle Idle pooled connections\n# TYPE queen_db_pool_idle gauge\n");
    body.push_str(&format!("queen_db_pool_idle {}\n", (ps.available as i64).max(0)));
    body.push_str("# HELP queen_db_pool_active Active pooled connections\n# TYPE queen_db_pool_active gauge\n");
    body.push_str(&format!("queen_db_pool_active {active}\n"));
    body.push_str("# HELP queen_maintenance_mode_enabled Push maintenance mode flag\n# TYPE queen_maintenance_mode_enabled gauge\n");
    body.push_str(&format!(
        "queen_maintenance_mode_enabled {}\n",
        st.maintenance.load(std::sync::atomic::Ordering::Relaxed) as i32
    ));
    body.push_str("# HELP queen_file_buffer_pending Spooled push events awaiting drain\n# TYPE queen_file_buffer_pending gauge\n");
    body.push_str(&format!("queen_file_buffer_pending {}\n", st.file_buffer.pending_count()));
    body.push_str("# HELP queen_file_buffer_failed Spool write failures\n# TYPE queen_file_buffer_failed gauge\n");
    body.push_str(&format!("queen_file_buffer_failed {}\n", st.file_buffer.failed_count()));
    body.push_str("# HELP queen_file_buffer_db_healthy File-buffer DB-reachability hint\n# TYPE queen_file_buffer_db_healthy gauge\n");
    body.push_str(&format!("queen_file_buffer_db_healthy {}\n", st.file_buffer.db_healthy() as i32));

    if let Ok(c) = st.pool.get().await {
        if let Ok(txt) = db::get_prometheus_metrics(&c).await {
            format_db_prometheus(&txt, &mut body);
        }
    }
    // RUSTFIX item 24: charset + Cache-Control: no-cache (prometheus.cpp:663-664).
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
        .into_response()
}

// ========================================================= consumer groups
// Management surface for consumer groups. list/lagging/details are read-only
// over queen.get_consumer_groups_v4 and its sibling readers (010_log_admin);
// delete/subscription/seek mutate the log cursor state (queen.log_consumers,
// via 010_log_admin) plus the shared coordination tables (014_consumer_groups).
// These ADD to the handlers above; they never touch
// push/pop/ack/transaction/configure.

// POST /api/v1/stats/refresh — force the stats reconciler NOW instead of waiting
// out STATS_INTERVAL_MS. ADMIN operation.
//
// This used to call queen.refresh_all_stats_v1, the ROWS reconciler, described in
// the comment here as "no-op for segments, wired for parity". It was neither: that
// SP recomputes queen.stats from queen.partitions/queen.messages for EVERY queue
// with no storage filter, and a log-engine queue has no rows there — so forcing a
// refresh upserted zeros over the log-derived counters, and every reader of those
// stat rows (/status, /status/queues, the dashboard overview) reported an empty
// broker until the next stats cycle repaired it. Measured on a queue holding 3
// pending messages: child_count 2 -> 0, total 3 -> 0, pending 3 -> 0.
//
// It now runs the same reconciler the stats loop does, which is what "force the
// refresh" was always supposed to mean.
pub async fn handle_stats_refresh(State(st): State<Arc<AppState>>) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::seg_refresh_all_stats(&client).await {
        Ok(txt) => sp_result_to_response(txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("refresh failed: ", &e),
        ),
    }
}

