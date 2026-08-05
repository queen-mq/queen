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

// ============================================================ system maintenance
// Parity with the C++ maintenance routes (server/src/routes/maintenance.cpp),
// backed by an in-process AtomicBool + queen.system_state (keys 'maintenance_mode'
// / 'pop_maintenance_mode', value {"enabled":bool} — the SAME rows the C++
// SharedStateManager reads/writes, so a mixed deployment stays consistent). The
// in-process flag is the source of truth for hot-path checks; the DB write is a
// best-effort mirror for restart/cluster propagation.
//
// When `maintenanceMode` is on, pushes are diverted to the file buffer (RUSTFIX
// item 17) and reported status:"buffered" — nothing reaches queen.seg_segments
// until maintenance is disabled and the background drain replays the spool.
// `popMaintenanceMode` pauses pops (see handle_pop).
#[derive(Deserialize)]
struct MaintenanceBody {
    enabled: Option<bool>,
}

// GET /api/v1/system/maintenance — current flags + live file-buffer status
// (RUSTFIX items 1 & 17).
pub async fn handle_get_maintenance(State(st): State<Arc<AppState>>) -> Response {
    // RUSTFIX item 16: read the flags FRESH from queen.system_state (C++
    // get_maintenance_mode_fresh), so a change made by another node is reflected
    // immediately, and update the in-process atomics. Fall back to the atomics if
    // the pool/DB is unavailable so the endpoint never 500s.
    let (maint, pop_maint) = match st.pool.get().await {
        Ok(c) => {
            let m = db::get_system_flag(&c, "maintenance_mode")
                .await
                .unwrap_or_else(|_| st.maintenance.load(Ordering::Relaxed));
            let pm = db::get_system_flag(&c, "pop_maintenance_mode")
                .await
                .unwrap_or_else(|_| st.pop_maintenance.load(Ordering::Relaxed));
            st.maintenance.store(m, Ordering::Relaxed);
            st.pop_maintenance.store(pm, Ordering::Relaxed);
            (m, pm)
        }
        Err(_) => (
            st.maintenance.load(Ordering::Relaxed),
            st.pop_maintenance.load(Ordering::Relaxed),
        ),
    };
    let out = serde_json::json!({
        "maintenanceMode": maint,
        "popMaintenanceMode": pop_maint,
        "bufferedMessages": st.file_buffer.pending_count(),
        "bufferHealthy": st.file_buffer.db_healthy(),
        "bufferStats": st.file_buffer.buffer_stats(),
    });
    json(StatusCode::OK, out.to_string())
}

// POST /api/v1/system/maintenance {enabled:bool} — toggle push maintenance.
pub async fn handle_set_maintenance(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let b: MaintenanceBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, json_err("bad body: ", e)),
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
    // Drive the buffer drain lifecycle (parity with async_queue_manager.cpp
    // set_maintenance_mode:1108-1125): on ENABLE pause the drain so spooled pushes
    // accumulate; on DISABLE force-finalize the active spool file and resume so it
    // drains to the DB.
    if enabled {
        st.file_buffer.pause_background_drain();
    } else {
        st.file_buffer.force_finalize_all();
        st.file_buffer.resume_background_drain();
    }
    // Propagate the flip to peer replicas (no-op with no mesh transport).
    st.notifier.broadcast_maintenance(enabled);
    let out = serde_json::json!({
        "maintenanceMode": enabled,
        "bufferedMessages": st.file_buffer.pending_count(),
        "bufferHealthy": st.file_buffer.db_healthy(),
        // Exact C++ text (routes/maintenance.cpp) — some tooling greps for it.
        "message": if enabled {
            "Maintenance mode ENABLED. All PUSHes routing to file buffer."
        } else {
            "Maintenance mode DISABLED. Background processor will drain buffer to DB."
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
        Err(e) => return json(StatusCode::BAD_REQUEST, json_err("bad body: ", e)),
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
    // Propagate the flip to peer replicas (no-op with no mesh transport).
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

// GET /api/v1/system/shared-state — mesh sync cache stats. This broker has no
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

