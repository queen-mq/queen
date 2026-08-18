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

// ==================================================== kv/timers kill switches
// PLAN_KV_TIMERS.md §12.1 rungs 7 and 8, on the SAME shape as the two flags
// above: an in-process atomic that is authoritative on the hot path, plus a row
// in queen.system_state as a best-effort mirror for propagation and restart, and
// a FRESH read on the GET so a change made on another node shows up here at once.
//
// WHY THESE EXIST AT ALL, now that they are the ONLY level. There used to be a
// boot flag under each of them (`QUEEN_KV_ENABLED`, `QUEEN_TIMERS_ENABLED`),
// which answered 404 and turned the surface off for the life of the process.
// Both are gone: kv and timers are part of the engine, not features a cell opts
// into. These switches are not their replacement and never were — they are the
// incident instrument. Changing a boot flag was a rollout; this is a POST that
// takes effect now, on a cell somebody is holding at three in the morning, and
// is expected to be flipped back. A paused surface answers 503 with
// `Retry-After` ("an operator paused it; come back"), never 404.
//
// WHY THE TWO TIMER SWITCHES ARE SEPARATE, and it is the sentence to read before
// touching either: the halves have OPPOSITE COSTS. Pausing the schedule promises
// nothing new and is instantly reversible. Pausing the FIRE accumulates work that
// was already promised — nothing is lost, `deliverAt` is "no earlier than" and
// the backlog drains from the oldest — but until it is turned back on, a customer
// sees messages that do not arrive, which reads as loss and not as latency. No
// automatic rung of the degradation ladder may ever touch the fire; only this
// endpoint can.
//
// NO PER-QUEUE FLAG (§12.1): the KV has no queue, and a per-queue timer flag
// would be a column on queen.queues, which `/configure` RESETS (confirmed defect
// 2026-08-05) — the flag would switch itself back on at the first
// reconfiguration, silently.

/// GET /api/v1/system/kv-timers — the four rungs as an operator sees them, plus
/// the freshness of the measurement the quota gate is enforcing against.
pub async fn handle_get_kv_timers(State(st): State<Arc<AppState>>) -> Response {
    use crate::switches::Switches;
    // Fresh from the DB, same as the maintenance GET, falling back to the
    // in-process atomics so the endpoint never 500s on a busy pool.
    if let Ok(c) = st.pool.get().await {
        for key in [
            Switches::KEY_KV,
            Switches::KEY_TIMERS_SCHEDULE,
            Switches::KEY_TIMERS_FIRE,
        ] {
            if let Ok(v) = db::get_system_flag_opt(&c, key).await {
                st.switches.adopt(key, v);
            }
        }
    }
    let refreshed_ms = st.quota.refreshed_ms();
    let out = serde_json::json!({
        // `kvEnabledByConfig` / `timersEnabledByConfig` USED TO BE HERE and are gone
        // with the boot flags they reported. Nothing replaces them: the answer would
        // be `true` on every cell that can serve this request, which is a field whose
        // only function would be to keep a dashboard asking a question that no longer
        // has two answers.
        "kvEnabled": st.switches.kv_on(),
        "timersScheduleEnabled": st.switches.timers_schedule_on(),
        "timersFireEnabled": st.switches.fire_allowed(),
        // The quota gate's own health. `quotaAgeMs` is the number to look at when
        // limits behave oddly: it is how stale the measurement being enforced
        // against is, and a value that keeps growing means the refresh is failing
        // (the enforcer still works — the local delta keeps accumulating — but
        // nothing is being RELEASED).
        "quotaTenants": st.quota.known(),
        "quotaAgeMs": if refreshed_ms == 0 {
            serde_json::Value::Null
        } else {
            serde_json::Value::from((crate::util::now_epoch_ms() - refreshed_ms).max(0))
        },
        "quotaHot": st.quota.hot(),
    });
    json(StatusCode::OK, out.to_string())
}

#[derive(Deserialize)]
struct KvTimersBody {
    kv: Option<bool>,
    #[serde(rename = "timersSchedule")]
    timers_schedule: Option<bool>,
    #[serde(rename = "timersFire")]
    timers_fire: Option<bool>,
}

/// POST /api/v1/system/kv-timers {kv?, timersSchedule?, timersFire?}
///
/// Each field is INDEPENDENT and an omitted one is left alone — an operator
/// pausing the schedule at three in the morning must not have to restate the
/// other two, and a body that reset the unmentioned ones to a default would turn
/// "pause new timers" into "and also stop delivering the ones already promised".
pub async fn handle_set_kv_timers(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    use crate::switches::Switches;
    let b: KvTimersBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, json_err("bad body: ", e)),
    };
    if b.kv.is_none() && b.timers_schedule.is_none() && b.timers_fire.is_none() {
        return json(
            StatusCode::BAD_REQUEST,
            "{\"error\":\"at least one of kv, timersSchedule, timersFire (boolean) is required\"}"
                .to_string(),
        );
    }
    // In-process FIRST and the mirror second: the atomic is what the hot path
    // reads, and an operator pulling a lever during an incident must not have the
    // effect wait on a database that may be the thing going wrong.
    if let Some(v) = b.kv {
        st.switches.set_kv(v);
    }
    if let Some(v) = b.timers_schedule {
        st.switches.set_timers_schedule(v);
    }
    if let Some(v) = b.timers_fire {
        st.switches.set_timers_fire(v);
    }
    let mut mirrored = true;
    match st.pool.get().await {
        Ok(c) => {
            for (key, v) in [
                (Switches::KEY_KV, b.kv),
                (Switches::KEY_TIMERS_SCHEDULE, b.timers_schedule),
                (Switches::KEY_TIMERS_FIRE, b.timers_fire),
            ] {
                if let Some(v) = v {
                    if db::set_system_flag(&c, key, v).await.is_err() {
                        mirrored = false;
                    }
                }
            }
        }
        Err(_) => mirrored = false,
    }
    let out = serde_json::json!({
        "kvEnabled": st.switches.kv_on(),
        "timersScheduleEnabled": st.switches.timers_schedule_on(),
        "timersFireEnabled": st.switches.fire_allowed(),
        // Said out loud rather than swallowed: the flip IS in force on this
        // broker, but with the mirror unwritten it will not survive a restart and
        // will not reach the other replicas, which reconcile from the row.
        "mirrored": mirrored,
        "message": if st.switches.fire_allowed() {
            "kv/timers runtime switches updated"
        } else {
            "TIMER FIRE IS PAUSED. Nothing is lost — deliverAt is 'no earlier than' and the \
             backlog drains oldest-first when it is turned back on — but promised messages are \
             NOT being delivered. Watch queen_timers_due_backlog."
        },
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

