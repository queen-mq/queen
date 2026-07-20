//! Internal inter-instance HTTP endpoints (port of server/src/routes/internal.cpp).
//!
//! These are the HTTP fallback / observability surface for the UDP sync layer:
//!
//!   POST /internal/api/notify              — HTTP fallback for MESSAGE_AVAILABLE.
//!                                            An external system (or a peer without
//!                                            UDP reachability) posts {queue,partition?}
//!                                            and we wake local parked pops AND fan the
//!                                            signal out to peers over UDP — exactly like
//!                                            a local push (matches the C++ route, which
//!                                            calls notify_message_available).
//!   GET  /internal/api/shared-state/stats  — UDP transport + node stats.
//!   GET  /internal/api/inter-instance/stats — legacy alias for the same stats.
//!
//! All three are ADMIN-gated: `route_access_level` maps every `/internal/` path to
//! `AccessLevel::Admin`, so with JWT enabled they require an admin token, and with
//! JWT disabled (the default test posture) they pass through like every other route.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::handlers::AppState;

fn json(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// POST /internal/api/notify {queue, partition?}
pub async fn handle_notify(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let v: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    let queue = v.get("queue").and_then(|x| x.as_str()).unwrap_or("");
    if queue.is_empty() {
        return json(
            StatusCode::BAD_REQUEST,
            "{\"error\":\"queue is required\"}".to_string(),
        );
    }
    let partition = v.get("partition").and_then(|x| x.as_str()).unwrap_or("");
    // Same signal a local push emits: wake local parked pops + broadcast to peers.
    st.notifier.notify_pushed(queue, partition);
    json(StatusCode::OK, "{\"status\":\"ok\"}".to_string())
}

/// Shared shape for both stats routes.
fn stats_body(st: &Arc<AppState>) -> String {
    use std::sync::atomic::Ordering;
    let maint = st.maintenance.load(Ordering::Relaxed);
    let pop_maint = st.pop_maintenance.load(Ordering::Relaxed);
    match st.notifier.transport() {
        Some(t) => {
            let mut obj = t.stats();
            if let Some(map) = obj.as_object_mut() {
                map.insert("enabled".into(), serde_json::Value::Bool(true));
                map.insert("running".into(), serde_json::Value::Bool(true));
                map.insert("maintenance_mode".into(), serde_json::Value::Bool(maint));
                map.insert("pop_maintenance_mode".into(), serde_json::Value::Bool(pop_maint));
            }
            obj.to_string()
        }
        None => serde_json::json!({
            "enabled": false,
            "reason": "no_peers",
            "maintenance_mode": maint,
            "pop_maintenance_mode": pop_maint,
        })
        .to_string(),
    }
}

/// GET /internal/api/shared-state/stats
pub async fn handle_shared_state_stats(State(st): State<Arc<AppState>>) -> Response {
    json(StatusCode::OK, stats_body(&st))
}

/// GET /internal/api/inter-instance/stats (legacy alias)
pub async fn handle_inter_instance_stats(State(st): State<Arc<AppState>>) -> Response {
    json(StatusCode::OK, stats_body(&st))
}
