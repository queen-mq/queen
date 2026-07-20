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
use crate::vegas::Vegas;

pub async fn handle_status() -> Response {
    json(StatusCode::OK, "{\"status\":\"ok\",\"engine\":\"segments-rust\"}".to_string())
}

pub async fn handle_metrics(State(st): State<Arc<AppState>>) -> Response {
    let mut body = st.metrics.prometheus();
    body.push_str(&format!(
        "queen_seg_push_vegas_limit {}\nqueen_seg_pop_vegas_limit {}\n",
        st.push_vegas.limit(),
        st.pop_vegas.limit()
    ));
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
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
        Ok(txt) => json(StatusCode::OK, txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("status failed: ", &e),
        ),
    }
}

// ----------------------------------------------------- GET /api/v1/status/queues
pub async fn handle_status_queues(
    State(st): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let filters = filters_from_query(&params, &["from", "to", "namespace", "task"]);
    let filters_json = serde_json::Value::Object(filters).to_string();
    let limit = qint(&params, "limit", 100);
    let offset = qint(&params, "offset", 0);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_status_queues(&client, &filters_json, limit, offset).await {
        Ok(txt) => json(StatusCode::OK, txt),
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
                "{{\"status\":\"ok\",\"database\":\"connected\",\"engine\":\"segments-rust\",\"version\":\"{}\"}}",
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
    // DB-persisted lifetime totals (queen.worker_metrics_summary). Emitted under
    // a distinct queen_db_* prefix so they do NOT collide with the in-process
    // queen_cluster_* live counters this handler already emits (the rust broker
    // does not write worker_metrics_summary, so these read 0 today).
    if let Some(t) = v.get("system_totals").and_then(|x| x.as_object()) {
        let map = [
            ("pushRequests", "queen_db_push_requests_total"),
            ("popRequests", "queen_db_pop_requests_total"),
            ("ackRequests", "queen_db_ack_requests_total"),
            ("transactions", "queen_db_transactions_total"),
            ("pushMessages", "queen_db_push_messages_total"),
            ("popMessages", "queen_db_pop_messages_total"),
            ("ackMessages", "queen_db_ack_messages_total"),
            ("ackSuccess", "queen_db_ack_success_total"),
            ("ackFailed", "queen_db_ack_failed_total"),
            ("dbErrors", "queen_db_errors_total"),
            ("dlqCount", "queen_db_dlq_total"),
        ];
        for (k, metric) in map {
            if let Some(n) = t.get(k).and_then(|x| x.as_i64()) {
                out.push_str(metric);
                out.push_str("{scope=\"cluster\"} ");
                out.push_str(&n.to_string());
                out.push('\n');
            }
        }
    }
    // DLQ depth (cluster total + per-queue).
    if let Some(d) = v.get("dlq") {
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
    // Queue depth (per-queue message counts, both engines).
    if let Some(arr) = v.get("queue_depth").and_then(|x| x.as_array()) {
        for e in arr {
            let q = e.get("queue").and_then(|x| x.as_str()).unwrap_or("");
            let storage = e.get("storage").and_then(|x| x.as_str()).unwrap_or("");
            let total = e.get("total_messages").and_then(|x| x.as_i64()).unwrap_or(0);
            let pending = e.get("pending_messages").and_then(|x| x.as_i64()).unwrap_or(0);
            let ql = prom_label_escape(q);
            let sl = prom_label_escape(storage);
            out.push_str(&format!(
                "queen_queue_depth_total{{queue=\"{}\",storage=\"{}\"}} {}\n",
                ql, sl, total
            ));
            out.push_str(&format!(
                "queen_queue_depth_pending{{queue=\"{}\",storage=\"{}\"}} {}\n",
                ql, sl, pending
            ));
        }
    }
}

// -------------------------------------------------------- GET /metrics/prometheus
// In-process gauges (same as /metrics) + a subset of the DB-derived metrics from
// get_prometheus_metrics_v1. DB block is best-effort — the in-process gauges are
// always emitted even if the DB read fails.
pub async fn handle_prometheus(State(st): State<Arc<AppState>>) -> Response {
    let mut body = st.metrics.prometheus();
    body.push_str(&format!(
        "queen_seg_push_vegas_limit {}\nqueen_seg_pop_vegas_limit {}\n",
        st.push_vegas.limit(),
        st.pop_vegas.limit()
    ));
    if let Ok(c) = st.pool.get().await {
        if let Ok(txt) = db::get_prometheus_metrics(&c).await {
            format_db_prometheus(&txt, &mut body);
        }
    }
    text_plain(StatusCode::OK, body)
}

// ========================================================= consumer groups
// Management surface for consumer groups on the segments engine. list/lagging/
// details are read-only over queen.get_consumer_groups_v4 (dual-engine, 027) and
// the 008 lag/detail readers; delete/subscription/seek mutate the segment cursor
// state (queen.partition_consumers) plus the shared coordination tables. These ADD to
// the handlers above; they never touch push/pop/ack/transaction/configure.

// POST /api/v1/stats/refresh — force the stats reconciler (no-op for segments,
// wired for parity). ADMIN operation.
pub async fn handle_stats_refresh(State(st): State<Arc<AppState>>) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::refresh_all_stats(&client).await {
        Ok(txt) => json(StatusCode::OK, txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("refresh failed: ", &e),
        ),
    }
}

