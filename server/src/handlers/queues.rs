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

// ------------------------------------------------------------------ configure
// POST /api/v1/configure — create/update a queue. The JS/Go builders send
//   {queue, namespace?, task?, options:{...15 opts...}}
// but raw callers may put the options top-level. We normalize to a single options
// object, run queen.configure_queue_v1 (which persists all 15 columns on
// queen.queues and echoes them back), then — because this is the segments engine —
// pin storage='segments' and materialize a queen.seg_queues row so the pop path
// reads the configured leaseTime. The configure_queue_v1 JSON is returned verbatim;
// the JS `configureQueue` test asserts res.configured===true and round-trips every
// options[key], so we MUST NOT reshape it.
pub async fn handle_configure(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let root: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };

    // The C++ configure route only rejects a missing/non-string `queue`; an EMPTY
    // string is a valid queue name (configure_queue_v1 creates a row named ''). The
    // JS client's `.queue('').create()` (load.js::testLoadConsumerGroup) relies on
    // this, so accept "" here rather than filtering it out.
    let queue = match root.get("queue").and_then(|x| x.as_str()) {
        Some(q) => q.to_string(),
        None => {
            return json(StatusCode::BAD_REQUEST, "{\"error\":\"queue is required\"}".to_string())
        }
    };

    // Options: prefer a nested `options` object; otherwise treat the top-level
    // body (minus the routing keys) as the options bag.
    let mut opts: serde_json::Map<String, serde_json::Value> =
        match root.get("options").and_then(|o| o.as_object()) {
            Some(o) => o.clone(),
            None => {
                let mut m = root.as_object().cloned().unwrap_or_default();
                m.remove("queue");
                m.remove("options");
                m
            }
        };
    // Fold top-level namespace/task into options (configure_queue_v1 reads them
    // from the options bag). Only when present as a non-empty string.
    for key in ["namespace", "task"] {
        if !opts.contains_key(key) {
            if let Some(s) = root.get(key).and_then(|x| x.as_str()).filter(|s| !s.is_empty()) {
                opts.insert(key.to_string(), serde_json::Value::String(s.to_string()));
            }
        }
    }
    // dedupWindowSeconds is segments-only (persisted to seg_queues, not queen.queues).
    // Read it from the options if provided; else keep the seg_queues default (3600).
    let dedup_window: i32 = opts
        .get("dedupWindowSeconds")
        .and_then(|x| x.as_i64())
        .map(|v| v.max(0) as i32)
        .unwrap_or(3600);

    let opts_json = serde_json::Value::Object(opts).to_string();

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let cfg_txt = match db::configure_queue(&client, &queue, &opts_json).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("configure failed: ", &e),
            )
        }
    };

    // RUSTFIX item 25: if the SP echo carries an {"error":...}, surface it as
    // 500/404 and short-circuit BEFORE the side-effecting seg_queues writes.
    if cfg_txt.contains("\"error\"") {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&cfg_txt) {
            if v.get("error").filter(|e| !e.is_null()).is_some() {
                return sp_result_to_response(cfg_txt);
            }
        }
    }

    // Pull the resolved (defaulted) leaseTime/retentionSeconds from the SP echo so
    // the seg_queues row matches queen.queues exactly.
    let cfg_val: serde_json::Value =
        serde_json::from_str(&cfg_txt).unwrap_or(serde_json::Value::Null);
    let lease_time = cfg_val
        .pointer("/options/leaseTime")
        .and_then(|x| x.as_i64())
        .unwrap_or(300) as i32;
    let retention_seconds = cfg_val
        .pointer("/options/retentionSeconds")
        .and_then(|x| x.as_i64())
        .unwrap_or(0) as i32;

    // Segments engine: be explicit about storage, and ensure the seg_queues row.
    if let Err(e) = db::mark_queue_segments(&client, &queue).await {
        return json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("configure(storage) failed: ", &e),
        );
    }
    if let Err(e) =
        db::upsert_seg_queue(&client, &queue, lease_time, retention_seconds, dedup_window).await
    {
        return json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("configure(seg_queue) failed: ", &e),
        );
    }

    // Invalidate the cached lease so a leaseTime change is reflected on next pop.
    st.lease_cache.lock().unwrap().remove(&queue);
    // Invalidate the same queue's config cache on peer replicas.
    st.notifier.broadcast_queue_config_set(&queue);

    json(StatusCode::OK, cfg_txt)
}

// -------------------------------------------------------------- delete queue
// DELETE /api/v1/resources/queues/:queue — drop the queue. Removes the rows-side
// coordination data (queen.delete_queue_v1) AND the segment data (seg_queues
// cascade). Response is the SP JSON {deleted:true,...} at HTTP 200 (a 204 would
// make the JS client return null and fail res.deleted===true).
pub async fn handle_delete_queue(
    State(st): State<Arc<AppState>>,
    Path(queue): Path<String>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let del_txt = match db::delete_queue(&client, &queue).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("delete failed: ", &e),
            )
        }
    };
    // Best-effort segment-data drop (independent of the rows-side delete).
    if let Err(e) = db::delete_seg_queue(&client, &queue).await {
        return json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("delete(seg) failed: ", &e),
        );
    }

    st.lease_cache.lock().unwrap().remove(&queue);
    // Invalidate the deleted queue's config cache on peer replicas.
    st.notifier.broadcast_queue_config_delete(&queue);
    json(StatusCode::OK, del_txt)
}

// ----------------------------------------------------------------- get queue
// GET /api/v1/resources/queues/:queue — basic queue detail via get_queue_v2,
// enriched with a segments message count (get_queue_v2's stats come from
// queen.stats, which the segments engine does not populate). 404 when the queue
// is gone.
pub async fn handle_get_queue(
    State(st): State<Arc<AppState>>,
    Path(queue): Path<String>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let txt = match db::get_queue(&client, &queue).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("get failed: ", &e),
            )
        }
    };
    let mut v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
    if v.get("error").is_some() || v.is_null() {
        return json(StatusCode::NOT_FOUND, "{\"error\":\"Queue not found\"}".to_string());
    }

    // Enrich with segment counts (best-effort; leave the base detail intact on error).
    if let Ok((segs, msgs)) = db::seg_queue_message_stats(&client, &queue).await {
        if let Some(obj) = v.as_object_mut() {
            obj.insert(
                "segments".to_string(),
                serde_json::json!({"segments": segs, "messages": msgs}),
            );
        }
    }

    json(StatusCode::OK, v.to_string())
}

// ------------------------------------------------------- resources LIST API
// GET /api/v1/resources/queues — queue list via get_queues_v2, enriched with
// segment counts. get_queues_v2 reads its partitions/messages from queen.stats,
// which the segments engine leaves empty, so those come back 0; we overlay the
// live seg_partitions/seg_segments counts (mirrors handle_get_queue's segments
// enrichment). Namespace/task query filters are accepted but not applied — the
// full list is a valid superset for the CLI list view.
pub async fn handle_list_queues(State(st): State<Arc<AppState>>) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let txt = match db::get_queues(&client).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("list failed: ", &e),
            )
        }
    };
    let mut v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);

    // Enrich each queue with segment-derived counts (best-effort; leave the base
    // list intact on error).
    if let Ok(stats) = db::seg_queue_stats_all(&client).await {
        let map: HashMap<String, (i64, i64, i64)> = stats
            .into_iter()
            .map(|(name, parts, segs, msgs)| (name, (parts, segs, msgs)))
            .collect();
        if let Some(arr) = v.get_mut("queues").and_then(|q| q.as_array_mut()) {
            for item in arr.iter_mut() {
                let name = match item.get("name").and_then(|x| x.as_str()) {
                    Some(s) => s.to_string(),
                    None => continue,
                };
                if let (Some(&(parts, segs, msgs)), Some(obj)) =
                    (map.get(&name), item.as_object_mut())
                {
                    obj.insert(
                        "segments".to_string(),
                        serde_json::json!({"segments": segs, "messages": msgs}),
                    );
                    obj.insert("partitions".to_string(), serde_json::json!(parts));
                    // The SP's messages{} block is all-zero for seg queues; overlay
                    // total/pending with the segment message count.
                    let m = obj.entry("messages".to_string()).or_insert_with(
                        || serde_json::json!({"total": 0, "pending": 0, "processing": 0}),
                    );
                    if let Some(mo) = m.as_object_mut() {
                        mo.insert("total".to_string(), serde_json::json!(msgs));
                        mo.insert("pending".to_string(), serde_json::json!(msgs));
                    }
                }
            }
        }
    }

    json(StatusCode::OK, v.to_string())
}

// GET /api/v1/resources/overview — system overview via get_system_overview_v3.
pub async fn handle_system_overview(State(st): State<Arc<AppState>>) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_system_overview(&client).await {
        Ok(t) => sp_result_to_response(t),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("overview failed: ", &e),
        ),
    }
}

// GET /api/v1/resources/namespaces — namespace list via get_namespaces_v2.
pub async fn handle_list_namespaces(State(st): State<Arc<AppState>>) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_namespaces(&client).await {
        Ok(t) => sp_result_to_response(t),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("namespaces failed: ", &e),
        ),
    }
}

// GET /api/v1/resources/tasks — task list via get_tasks_v2.
pub async fn handle_list_tasks(State(st): State<Arc<AppState>>) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_tasks(&client).await {
        Ok(t) => sp_result_to_response(t),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("tasks failed: ", &e),
        ),
    }
}

