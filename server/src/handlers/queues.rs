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

// ------------------------------------------------------------------ configure
// POST /api/v1/configure — create/update a queue. The JS/Go builders send
//   {queue, namespace?, task?, options:{...15 opts...}}
// but raw callers may put the options top-level. We normalize to a single options
// object and run queen.configure_queue_v1, which owns ALL config writes on
// queen.queues (queue identity is the queues id now — there is no second queue
// table to mirror, and dedupWindowSeconds/leaseTime persist from the options
// blob like every other option). The configure_queue_v1 JSON is returned
// verbatim; the JS `configureQueue` test asserts res.configured===true and
// round-trips every options[key], so we MUST NOT reshape it.
pub async fn handle_configure(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    body: Bytes,
) -> Response {
    let root: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, json_err("bad body: ", e)),
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
    // dedupWindowSeconds travels IN the options blob: configure_queue_v1
    // persists it to queen.queues.dedup_window_seconds (DDL default 3600).
    let opts_json = serde_json::Value::Object(opts).to_string();

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let cfg_txt = match db::configure_queue(&client, &queue, tenant.as_str(), &opts_json).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("configure failed: ", &e),
            )
        }
    };

    // RUSTFIX item 25: if the SP echo carries an {"error":...}, surface it as
    // 500/404 and short-circuit BEFORE the cache invalidations below.
    if cfg_txt.contains("\"error\"") {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&cfg_txt) {
            if v.get("error").filter(|e| !e.is_null()).is_some() {
                return sp_result_to_response(cfg_txt);
            }
        }
    }

    // Invalidate the cached lease so a leaseTime change is reflected on next pop.
    // Track B (§5): the cache is keyed by (tenant, name) — invalidate this tenant's.
    let qkey = crate::handlers::tenant_queue_key(tenant.as_str(), &queue);
    st.lease_cache.lock().unwrap().remove(&qkey);
    // Invalidate the same queue's config cache on peer replicas — the frame carries
    // the tenant, so a peer invalidates exactly this tenant's entry (§5).
    st.notifier.broadcast_queue_config_set(&qkey);

    json(StatusCode::OK, cfg_txt)
}

// -------------------------------------------------------------- delete queue
// DELETE /api/v1/resources/queues/:queue — drop the queue. ONE SP call:
// queen.delete_queue_v1 owns the whole delete (the FK-less log_txns/log_dlq
// purge + the cascading queen.queues delete — queue identity is the queues id
// now, so log_partitions and everything under it cascade from that one row).
// Response is the SP JSON at HTTP 200 (a 204 would make the JS client
// return null and fail res.deleted===true), with `deleted` reflecting whether a
// queue was actually removed — see the existed:false guard below.
pub async fn handle_delete_queue(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(queue): Path<String>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let del_txt = match db::delete_queue(&client, &queue, tenant.as_str()).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("delete failed: ", &e),
            )
        }
    };

    let qkey = crate::handlers::tenant_queue_key(tenant.as_str(), &queue);
    st.lease_cache.lock().unwrap().remove(&qkey);
    // Invalidate the deleted queue's config cache on peer replicas.
    st.notifier.broadcast_queue_config_delete(&qkey);

    // The SP always reports deleted:true and hides the real outcome in
    // `existed`, so "delete a queue that isn't yours / doesn't exist" reads as
    // success to any client that trusts `deleted`. Make the body self-consistent
    // (deleted mirrors existed). The status stays 200: DELETE is idempotent here
    // and the SDKs use delete-before-create as a cleanup idiom, so a 404 would
    // turn a no-op into a thrown error for them.
    let mut v: serde_json::Value =
        serde_json::from_str(&del_txt).unwrap_or(serde_json::Value::Null);
    if v.get("existed").and_then(|x| x.as_bool()) == Some(false) {
        if let Some(o) = v.as_object_mut() {
            o.insert("deleted".to_string(), serde_json::json!(false));
            o.insert(
                "message".to_string(),
                serde_json::json!("Queue not found, nothing was deleted"),
            );
            return json(StatusCode::OK, v.to_string());
        }
    }
    json(StatusCode::OK, del_txt)
}

// ----------------------------------------------------------------- get queue
// GET /api/v1/resources/queues/:queue — basic queue detail via get_queue_v2,
// enriched with a segments message count (get_queue_v2's stats come from
// queen.stats, which the segments engine does not populate). 404 when the queue
// is gone.
pub async fn handle_get_queue(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(queue): Path<String>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let txt = match db::get_queue(&client, &queue, tenant.as_str()).await {
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
    if let Ok((segs, msgs)) = db::seg_queue_message_stats(&client, &queue, tenant.as_str()).await {
        if let Some(obj) = v.as_object_mut() {
            obj.insert(
                "segments".to_string(),
                serde_json::json!({"segments": segs, "messages": msgs}),
            );
        }
    }

    json(StatusCode::OK, v.to_string())
}

// GET /api/v1/resources/queues/:queue/depth?group=... — minimal per-partition
// backlog read (queen.log_queue_depth_v1, 011_log_stats). Built for relay/
// scheduler pollers that read exactly one number per partition: the watermark
// arithmetic only, no segments scan, no timestamps, no DLQ join — against the
// console-grade GET /resources/queues/:queue this is one index-only read.
// `group` absent = queue-level pending under the same worst-cursor precedence
// the dashboard publishes; `group=<name>` = that group's own backlog (the ETA
// ingredient). 404 shape matches handle_get_queue.
#[derive(Deserialize)]
pub struct QueueDepthParams {
    group: Option<String>,
}

pub async fn handle_queue_depth(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(queue): Path<String>,
    Query(p): Query<QueueDepthParams>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::queue_depth(&client, &queue, p.group.as_deref(), tenant.as_str()).await {
        Ok(Some(txt)) => json(StatusCode::OK, txt),
        Ok(None) => json(StatusCode::NOT_FOUND, "{\"error\":\"Queue not found\"}".to_string()),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("depth failed: ", &e),
        ),
    }
}

// ------------------------------------------------------- resources LIST API
// GET /api/v1/resources/queues — queue list via get_queues_v2, enriched with
// segment counts. get_queues_v2 reads its partitions/messages from queen.stats,
// which the segments engine leaves empty, so those come back 0; we overlay the
// live seg_partitions/seg_segments counts (mirrors handle_get_queue's segments
// enrichment). Namespace/task query filters are accepted but not applied — the
// full list is a valid superset for the CLI list view.
pub async fn handle_list_queues(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let txt = match db::get_queues(&client, tenant.as_str()).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("list failed: ", &e),
            )
        }
    };
    let mut v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);

    // RUSTFIX item 25: surface an embedded {"error":..} SP body as 404/500 instead
    // of serving it at 200 (mirrors handle_configure's guard and the resources
    // siblings). Happy-path bodies ({"queues":[...]}) have no top-level error key.
    if v.get("error").filter(|e| !e.is_null()).is_some() {
        return sp_result_to_response(txt);
    }

    // Enrich each queue with segment-derived counts (best-effort; leave the base
    // list intact on error).
    if let Ok(stats) = db::seg_queue_stats_all(&client, tenant.as_str()).await {
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
                    // `msgs` is the RETAINED frame count (log_queue_stats_all_v1),
                    // which is `total`, NOT the unconsumed backlog. `pending` and
                    // `processing` must keep the SP's watermark values (queen.stats,
                    // refreshed for seg queues by log_refresh_all_stats_v1) — the
                    // same numbers the overview sums — or the queue list contradicts
                    // every other pending reading in the UI.
                    let m = obj.entry("messages".to_string()).or_insert_with(
                        || serde_json::json!({"total": 0, "pending": 0, "processing": 0}),
                    );
                    if let Some(mo) = m.as_object_mut() {
                        mo.insert("total".to_string(), serde_json::json!(msgs));
                    }
                }
            }
        }
    }

    // PLAN_KV_TIMERS §9.8 P2: four TOP-LEVEL fields on the response the proxy's
    // reconciler already polls, so that the storage quota — which is a live hard
    // gate, not a no-op — can see the bytes of the two new tables. They have no
    // queue, so there is no per-queue entry they could ride on, and without this
    // they are the only place in the product where a tenant occupies disk that
    // no quota can measure.
    //
    // Read from the sweeper's cached measurement via the db.rs wrapper (a
    // primary-key lookup), NEVER counted here: a cloud reconciler polls this
    // route every ten seconds per cell.
    //
    // On a failure the fields are OMITTED rather than sent as zero, and that
    // distinction is the contract: the proxy reads absent fields as zero AND
    // warns, so a cell that cannot answer produces a loud zero instead of a
    // silent under-count. Sending a zero we do not believe would be the silent
    // one.
    match db::kv_usage_snapshot(&client, tenant.as_str()).await {
        Ok(usage) => {
            let (kr, kb, tr, tb) = usage.unwrap_or((0, 0, 0, 0));
            if let Some(obj) = v.as_object_mut() {
                obj.insert("kvRows".to_string(), serde_json::json!(kr));
                obj.insert("kvBytes".to_string(), serde_json::json!(kb));
                obj.insert("timerRows".to_string(), serde_json::json!(tr));
                obj.insert("timerBytes".to_string(), serde_json::json!(tb));
            }
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "queue listing: kv/timer usage unavailable, omitting the quota fields"
            );
        }
    }

    json(StatusCode::OK, v.to_string())
}

// GET /api/v1/resources/overview — system overview via get_system_overview_v3.
// Track B (§5): scoped to the request tenant (default tenant ⇒ global, as before).
pub async fn handle_system_overview(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_system_overview(&client, tenant.as_str()).await {
        Ok(t) => sp_result_to_response(t),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("overview failed: ", &e),
        ),
    }
}

// GET /api/v1/resources/namespaces — namespace list via get_namespaces_v2.
// Track B (§5): scoped to the request tenant.
pub async fn handle_list_namespaces(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_namespaces(&client, tenant.as_str()).await {
        Ok(t) => sp_result_to_response(t),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("namespaces failed: ", &e),
        ),
    }
}

// GET /api/v1/resources/tasks — task list via get_tasks_v2.
// Track B (§5): scoped to the request tenant.
pub async fn handle_list_tasks(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_tasks(&client, tenant.as_str()).await {
        Ok(t) => sp_result_to_response(t),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("tasks failed: ", &e),
        ),
    }
}

