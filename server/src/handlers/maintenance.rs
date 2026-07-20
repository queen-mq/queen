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

// ============================================================ system maintenance
// Parity with the C++ maintenance routes (server/src/routes/maintenance.cpp),
// backed by an in-process AtomicBool + queen.system_state (keys 'maintenance_mode'
// / 'pop_maintenance_mode', value {"enabled":bool} — the SAME rows the C++
// SharedStateManager reads/writes, so a mixed deployment stays consistent). The
// in-process flag is the source of truth for hot-path checks; the DB write is a
// best-effort mirror for restart/cluster propagation.
//
// The segments broker has no file buffer, so `maintenanceMode` is REPORTED
// (bufferedMessages always 0) but does not divert pushes — messages keep flowing
// to the DB, which is what maintenance.js needs (all produced messages eventually
// received). `popMaintenanceMode` pauses pops (see handle_pop).
#[derive(Deserialize)]
struct MaintenanceBody {
    enabled: Option<bool>,
}

// GET /api/v1/system/maintenance — current flags + buffer status (buffer is a
// no-op for the segments engine, so it always reports empty + healthy).
pub async fn handle_get_maintenance(State(st): State<Arc<AppState>>) -> Response {
    let out = serde_json::json!({
        "maintenanceMode": st.maintenance.load(Ordering::Relaxed),
        "popMaintenanceMode": st.pop_maintenance.load(Ordering::Relaxed),
        "bufferedMessages": 0,
        "bufferHealthy": true,
        "bufferStats": {},
    });
    json(StatusCode::OK, out.to_string())
}

// POST /api/v1/system/maintenance {enabled:bool} — toggle push maintenance.
pub async fn handle_set_maintenance(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let b: MaintenanceBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    let enabled = match b.enabled {
        Some(v) => v,
        None => {
            return json(
                StatusCode::BAD_REQUEST,
                "{\"error\":\"enabled (boolean) is required\"}".to_string(),
            )
        }
    };
    st.maintenance.store(enabled, Ordering::Relaxed);
    if let Ok(c) = st.pool.get().await {
        let _ = db::set_system_flag(&c, "maintenance_mode", enabled).await;
    }
    // Propagate the flip to peer replicas (no-op with no UDP transport).
    st.notifier.broadcast_maintenance(enabled);
    let out = serde_json::json!({
        "maintenanceMode": enabled,
        "bufferedMessages": 0,
        "bufferHealthy": true,
        "message": if enabled {
            "Maintenance mode ENABLED."
        } else {
            "Maintenance mode DISABLED."
        },
    });
    json(StatusCode::OK, out.to_string())
}

// GET /api/v1/system/maintenance/pop — pop maintenance status.
pub async fn handle_get_pop_maintenance(State(st): State<Arc<AppState>>) -> Response {
    let pop = st.pop_maintenance.load(Ordering::Relaxed);
    let out = serde_json::json!({
        "popMaintenanceMode": pop,
        "message": if pop {
            "Pop maintenance mode is ON. All POP operations return empty arrays."
        } else {
            "Pop maintenance mode is OFF. Normal operation."
        },
    });
    json(StatusCode::OK, out.to_string())
}

// POST /api/v1/system/maintenance/pop {enabled:bool} — toggle pop maintenance.
pub async fn handle_set_pop_maintenance(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let b: MaintenanceBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    let enabled = match b.enabled {
        Some(v) => v,
        None => {
            return json(
                StatusCode::BAD_REQUEST,
                "{\"error\":\"enabled (boolean) is required\"}".to_string(),
            )
        }
    };
    st.pop_maintenance.store(enabled, Ordering::Relaxed);
    if let Ok(c) = st.pool.get().await {
        let _ = db::set_system_flag(&c, "pop_maintenance_mode", enabled).await;
    }
    // Propagate the flip to peer replicas (no-op with no UDP transport).
    st.notifier.broadcast_pop_maintenance(enabled);
    let out = serde_json::json!({
        "popMaintenanceMode": enabled,
        "message": if enabled {
            "Pop maintenance mode ENABLED. All POP operations will return empty arrays."
        } else {
            "Pop maintenance mode DISABLED. Normal operation resumed."
        },
    });
    json(StatusCode::OK, out.to_string())
}

// GET /api/v1/system/shared-state — UDPSYNC cache stats. This broker has no
// cluster gossip transport, so report a single-node summary carrying the live
// flags (parity shape with the C++ get_stats()).
pub async fn handle_shared_state(State(st): State<Arc<AppState>>) -> Response {
    let out = serde_json::json!({
        "enabled": false,
        "reason": "single_node_segments_broker",
        "maintenance_mode": st.maintenance.load(Ordering::Relaxed),
        "pop_maintenance_mode": st.pop_maintenance.load(Ordering::Relaxed),
    });
    json(StatusCode::OK, out.to_string())
}

// ================================================================= streams
// Three handlers for the fat-JS-client stream engine (client-v2/streams). The
// broker only serves these 3 endpoints + the normal pop path; all window/
// watermark/gate/operator logic runs client-side. Each streaming SP takes a
// JSONB ARRAY of requests ([{idx,..}]) and returns [{idx, result}]; we wrap the
// single client body in a one-element array (idx:0) and unwrap [0].result before
// returning — the SDK reads the inner result object directly (res.success /
// res.query_id / res.rows / res.push_results ...). This mirrors the C++ streams
// routes (server/src/routes/streams/*.cpp).

