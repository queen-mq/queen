//! Analytics / metrics HTTP handlers (System, Analytics, QueueOperations,
//! QueueDetail dashboard pages). Each dispatches a stored procedure and serves its
//! JSON verbatim — the C++ routes had no response envelope, so neither do these.
//! The backing data is produced by the stats reconciler (server/src/stats.rs) and
//! the metrics collector (server/src/syscollect.rs).

use super::*;
use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;

// Serve a stored-procedure result: raw JSON on success, {"error":..} on failure.
fn serve(prefix: &str, r: Result<String, tokio_postgres::Error>) -> Response {
    match r {
        Ok(txt) => sp_result_to_response(txt),
        Err(e) => json(StatusCode::INTERNAL_SERVER_ERROR, json_err(prefix, &e)),
    }
}

// Acquire a pooled client or return a 500 JSON error.
macro_rules! client_or_500 {
    ($st:expr) => {
        match $st.pool.get().await {
            Ok(c) => c,
            Err(_) => {
                return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string())
            }
        }
    };
}

fn filters_json(params: &HashMap<String, String>, keys: &[&str]) -> String {
    serde_json::Value::Object(filters_from_query(params, keys)).to_string()
}

// Track B (§5): like filters_json, but stamps `_tenant` so the SP scopes to it.
// queen.get_analytics_v1 reads COALESCE((p_filters->>'_tenant')::uuid, default).
fn filters_json_tenant(params: &HashMap<String, String>, keys: &[&str], tenant: &str) -> String {
    let mut m = filters_from_query(params, keys);
    m.insert("_tenant".to_string(), serde_json::json!(tenant));
    serde_json::Value::Object(m).to_string()
}

// ------------------------------------------- GET /api/v1/status/queues/:queue
pub async fn handle_queue_detail(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(queue): Path<String>,
) -> Response {
    let client = client_or_500!(st);
    // RUSTFIX item 25: route the SP result through sp_result_to_response (the same
    // helper every sibling handler in this file uses) so an embedded {"error":..}
    // body maps to 404 ("not found") / 500 instead of being served at HTTP 200. A
    // valid queue detail has no top-level "error" key, so success is still 200.
    // Track B (§5): scoped to the request tenant's queue.
    serve("queue detail failed: ", db::get_queue_detail(&client, &queue, tenant.as_str()).await)
}

// ---------------------------------------------- GET /api/v1/status/analytics
pub async fn handle_status_analytics(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let f = filters_json_tenant(&params, &["from", "to", "interval", "queue", "namespace", "task"], tenant.as_str());
    let client = client_or_500!(st);
    serve("analytics failed: ", db::get_analytics(&client, &f).await)
}

// --------------------------------------- GET /api/v1/analytics/system-metrics
pub async fn handle_system_metrics(
    State(st): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let f = filters_json(&params, &["from", "to", "hostname", "workerId"]);
    let client = client_or_500!(st);
    serve("system metrics failed: ", db::get_system_metrics(&client, &f).await)
}

// --------------------------------------- GET /api/v1/analytics/worker-metrics
pub async fn handle_worker_metrics(
    State(st): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let f = filters_json(&params, &["from", "to", "queue", "hostname", "workerId"]);
    let client = client_or_500!(st);
    serve("worker metrics failed: ", db::get_worker_metrics_ts(&client, &f).await)
}

// -------------------------------------------- GET /api/v1/analytics/queue-lag
// Positional (from,to,queue) args; returns a BARE JSON array.
// Track B (§5): scoped to the request tenant.
pub async fn handle_queue_lag(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let from = params.get("from").filter(|s| !s.is_empty()).map(|s| s.as_str());
    let to = params.get("to").filter(|s| !s.is_empty()).map(|s| s.as_str());
    let queue = params.get("queue").filter(|s| !s.is_empty()).map(|s| s.as_str());
    let client = client_or_500!(st);
    serve("queue lag failed: ", db::get_queue_lag(&client, from, to, queue, tenant.as_str()).await)
}

// -------------------------------------------- GET /api/v1/analytics/queue-ops
// Track B (§5): tenant injected into the filters JSON (get_queue_ops_v1 reads `_tenant`).
pub async fn handle_queue_ops(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let f = filters_json_tenant(&params, &["from", "to", "queue"], tenant.as_str());
    let client = client_or_500!(st);
    serve("queue ops failed: ", db::get_queue_ops(&client, &f).await)
}

// -------------------------------- GET /api/v1/analytics/queue-parked-replicas
// Track B (§5): tenant injected into the filters JSON.
pub async fn handle_queue_parked_replicas(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let f = filters_json_tenant(&params, &["from", "to", "queue"], tenant.as_str());
    let client = client_or_500!(st);
    serve("parked replicas failed: ", db::get_queue_parked_replicas(&client, &f).await)
}

// -------------------------------------------- GET /api/v1/analytics/retention
// Track B (§5): tenant injected into the filters JSON. queen.retention_history is
// written by the log engine's retention/eviction steps (006_log_maintenance);
// 022_retention_analytics resolves the queue through queen.log_partitions.
pub async fn handle_retention(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let f = filters_json_tenant(&params, &["from", "to", "queue"], tenant.as_str());
    let client = client_or_500!(st);
    serve("retention failed: ", db::get_retention_ts(&client, &f).await)
}

// --------------------------------------- GET /api/v1/analytics/postgres-stats
pub async fn handle_postgres_stats(State(st): State<Arc<AppState>>) -> Response {
    let client = client_or_500!(st);
    serve("postgres stats failed: ", db::get_postgres_stats(&client).await)
}

// --------------------------------------------- GET /api/v1/status/buffers
// Live file-buffer status (RUSTFIX item 1) — the shape the dashboard's
// backpressure panel expects. `dbHealthy` reflects the buffer's own DB-reachability
// hint (flipped by push/drain), falling back to a fresh ping when nothing has been
// buffered yet.
pub async fn handle_status_buffers(State(st): State<Arc<AppState>>) -> Response {
    let db_healthy = st.file_buffer.db_healthy()
        && matches!(st.pool.get().await, Ok(c) if db::ping(&c).await.is_ok());
    let body = serde_json::json!({
        "pending": st.file_buffer.pending_count(),
        "failed": st.file_buffer.failed_count(),
        "dbHealthy": db_healthy,
        "worker": 0,
    });
    json(StatusCode::OK, body.to_string())
}
