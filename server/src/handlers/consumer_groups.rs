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

// GET /api/v1/consumer-groups — every group across both engines. Returns the
// get_consumer_groups_v4 JSON array verbatim (the Admin client reads it as-is).
pub async fn handle_consumer_groups(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_consumer_groups(&client, tenant.as_str()).await {
        Ok(txt) => sp_result_to_response(txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("consumer groups failed: ", &e),
        ),
    }
}

// GET /api/v1/consumer-groups/lagging?minLagSeconds= — partitions lagging beyond
// the threshold. Registered BEFORE the /:group route so the static `lagging`
// segment wins over the param match.
pub async fn handle_lagging_consumers(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    // RUSTFIX item 22: default 3600s, matching C++ consumer_groups.cpp:99.
    let min_lag = qint(&params, "minLagSeconds", 3600);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_lagging_partitions(&client, min_lag, tenant.as_str()).await {
        Ok(txt) => sp_result_to_response(txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("lagging failed: ", &e),
        ),
    }
}

// GET /api/v1/consumer-groups/:group — per-queue/partition detail for one group.
pub async fn handle_consumer_group_details(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(group): Path<String>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_consumer_group_details(&client, &group, tenant.as_str()).await {
        Ok(txt) => sp_result_to_response(txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("details failed: ", &e),
        ),
    }
}

// DELETE /api/v1/consumer-groups/:group?deleteMetadata= — drop the group. Removes
// its log cursors (queen.log_consumers, all partitions) + consumer_watermarks
// AND its shared coordination state (consumer_watermarks,
// consumer_groups_metadata when deleteMetadata). deletedPartitions sums both
// engines. HTTP 200 with the merged SP JSON (a 204 would make the JS client
// return null).
pub async fn handle_delete_consumer_group(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(group): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let delete_metadata = qbool(&params, "deleteMetadata", true);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let seg = match db::delete_consumer_group_seg(&client, &group, delete_metadata, tenant.as_str()).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("delete(seg) failed: ", &e),
            )
        }
    };
    // Best-effort rows-side cleanup (empty for a pure-segments deployment).
    let rows = db::delete_consumer_group_rows(&client, &group, delete_metadata, tenant.as_str())
        .await
        .unwrap_or_else(|_| "{}".to_string());

    let seg_v: serde_json::Value = serde_json::from_str(&seg).unwrap_or(serde_json::Value::Null);
    let rows_v: serde_json::Value = serde_json::from_str(&rows).unwrap_or(serde_json::Value::Null);
    let seg_n = seg_v.get("deletedPartitions").and_then(|x| x.as_i64()).unwrap_or(0);
    let rows_n = rows_v.get("deletedPartitions").and_then(|x| x.as_i64()).unwrap_or(0);

    // Hot-list invalidation (2026-07-24) — symmetry with the DB cursor/watermark
    // delete above. The delete removed the group's committed cursors, so it must
    // reconsume from the start; but with QUEEN_HOTLIST on, the in-memory ring is
    // the discovery source and a pre-delete ring (stale IDLE/wheel entries + a
    // recent reseed clock) would suppress the reconsume until the ≤30s periodic
    // floor. Drop the group's ring on every queue so first contact reseeds cold.
    // Track B (§5): tenant-scoped, matching the SQL delete above and the
    // `seeded_groups` purge below — on a shared cell `workers` is a universal group
    // name and one tenant's delete must not cold-start every other tenant's ring.
    st.hotlist.forget_group_all_queues(tenant.as_str(), &group);
    // The group-first-contact seed marker (consumer_groups_metadata) was removed
    // when delete_metadata, but the monotonic positive `seeded_groups` cache still
    // says "seeded" and would route the next pop down the ring path, skipping the
    // first-contact BULK SEED that safely re-creates the cursors. Drop the group
    // from every queue's cached set so the next pop re-checks the (now-absent)
    // marker and re-seeds via the first-contact wildcard path.
    if delete_metadata {
        // Track B (§5): seeded_groups is keyed by (tenant, queue); drop the group
        // only from THIS tenant's queue sets (prefix "<tenant>\x1f…").
        let prefix = crate::handlers::tenant_queue_key(tenant.as_str(), "");
        let mut sg = st.seeded_groups.lock().unwrap();
        for (k, set) in sg.iter_mut() {
            if k.starts_with(&prefix) {
                set.remove(&group);
            }
        }
    }

    let out = serde_json::json!({
        "success": true,
        "consumerGroup": group,
        "deletedPartitions": seg_n + rows_n,
        "metadataDeleted": delete_metadata,
    });
    json(StatusCode::OK, out.to_string())
}

// DELETE /api/v1/consumer-groups/:group/queues/:queue?deleteMetadata= — drop the
// group FOR ONE QUEUE only. Removes the group's segment cursors for every
// partition of THAT queue (queen.log_consumers) + its consumer_watermarks row for
// (queue, group), AND the rows-side per-queue coordination state via
// queen.delete_consumer_group_for_queue_v1 (partition_consumers, consumer_watermarks,
// and consumer_groups_metadata when deleteMetadata). Clearing the empty-scan
// watermark is what lets the group re-consume the queue from the start (an
// advanced watermark would otherwise fence off every partition). deletedPartitions
// sums both engines; HTTP 200 with the merged JSON (a 204 would make the JS client
// return null).
pub async fn handle_delete_consumer_group_for_queue(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path((group, queue)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let delete_metadata = qbool(&params, "deleteMetadata", true);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let seg_n = match db::delete_consumer_group_for_queue_seg(&client, &group, &queue, tenant.as_str()).await {
        Ok(n) => n as i64,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("delete(seg) failed: ", &e),
            )
        }
    };
    // Best-effort rows-side cleanup (empty for a pure-segments deployment).
    let rows = db::delete_consumer_group_for_queue_rows(&client, &group, &queue, delete_metadata, tenant.as_str())
        .await
        .unwrap_or_else(|_| "{}".to_string());
    let rows_v: serde_json::Value = serde_json::from_str(&rows).unwrap_or(serde_json::Value::Null);
    let rows_n = rows_v.get("deletedPartitions").and_then(|x| x.as_i64()).unwrap_or(0);

    // Hot-list invalidation (2026-07-24), scoped to this queue — see the all-queues
    // sibling for the rationale. Drop the group's ring for `queue` so a stale
    // pre-delete ring cannot mask the from-the-start reconsume, and (when the
    // per-(queue, group) seed marker was removed) drop the stale positive
    // `seeded_groups` entry so the next pop re-seeds via first contact.
    st.hotlist
        .forget_group(&crate::handlers::tenant_queue_key(tenant.as_str(), &queue), &group);
    if delete_metadata {
        // Track B (§5): seeded_groups is keyed by (tenant, queue).
        let key = crate::handlers::tenant_queue_key(tenant.as_str(), &queue);
        if let Some(set) = st.seeded_groups.lock().unwrap().get_mut(&key) {
            set.remove(&group);
        }
    }

    let out = serde_json::json!({
        "success": true,
        "consumerGroup": group,
        "queueName": queue,
        "deletedPartitions": seg_n + rows_n,
        "metadataDeleted": delete_metadata,
    });
    json(StatusCode::OK, out.to_string())
}

// POST /api/v1/consumer-groups/:group/subscription {subscriptionTimestamp} —
// update the group's subscription cutoff (consumer_groups_metadata; engine-
// agnostic). Returns the SP JSON.
#[derive(Deserialize)]
struct SubscriptionBody {
    #[serde(rename = "subscriptionTimestamp")]
    subscription_timestamp: Option<String>,
}

pub async fn handle_update_subscription(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(group): Path<String>,
    body: Bytes,
) -> Response {
    let b: SubscriptionBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, json_err("bad body: ", e)),
    };
    let ts = match b.subscription_timestamp.filter(|s| !s.is_empty()) {
        Some(t) => t,
        None => {
            return json(
                StatusCode::BAD_REQUEST,
                "{\"error\":\"subscriptionTimestamp is required\"}".to_string(),
            )
        }
    };
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::update_consumer_group_subscription(&client, &group, &ts, tenant.as_str()).await {
        Ok(txt) => sp_result_to_response(txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("subscription failed: ", &e),
        ),
    }
}

// Seek body: {toEnd:true} or {timestamp:"<iso>"}. Resolve to (to_end, timestamp).
#[derive(Deserialize)]
struct SeekBody {
    #[serde(rename = "toEnd")]
    to_end: Option<bool>,
    timestamp: Option<String>,
}

fn parse_seek(body: &Bytes) -> Result<(bool, Option<String>), &'static str> {
    let b: SeekBody = serde_json::from_slice(body).map_err(|_| "bad body")?;
    if b.to_end == Some(true) {
        return Ok((true, None));
    }
    match b.timestamp.filter(|s| !s.is_empty()) {
        Some(ts) => Ok((false, Some(ts))),
        None => Err("Must specify toEnd=true or a timestamp"),
    }
}

// A seek SP returns {"success":false,...} for a no-op request (partition not
// found). Only re-seed the ring on a real cursor move; a parse miss defaults to
// true (re-seeding is evidence-based and harmless either way).
fn seek_succeeded(txt: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(txt)
        .ok()
        .and_then(|v| v.get("success").and_then(|s| s.as_bool()))
        .unwrap_or(true)
}

// Hot-list re-discovery after a successful seek (2026-07-24). A BACKWARD seek moves
// the group's committed cursor behind messages the ring already cleared as
// consumed; with QUEEN_HOTLIST on the ring is the discovery source, so without an
// explicit re-seed the reconsume only resumes at the ≤30s periodic reseed floor
// (the legacy path was immediate). Re-seed the (queue, group) ring from committed
// PG state NOW and wake parked pops. Evidence-based by construction:
// log_hotlist_reseed_v1 returns only partitions with last_offset > committed, so a
// seek-to-end (cursor moved FORWARD) re-adds nothing — no false positive — while a
// backward/timestamp seek re-adds exactly the re-pending partitions. Over-marking
// would be a harmless ~0.2ms empty probe; under-marking is the bug we are closing.
//
// FULL and BROADCASTING, not the ordinary reseed (2026-08-11, with the windowed
// floor): a seek is the one operation that makes OLD partitions pending without
// writing them, so the windowed scan is blind to it by construction — it must be the
// full walk. And the walk only repairs the ring of the broker that served the seek;
// the peers used to heal within one ≤30s floor because every peer walked everything
// that often, which the slower full cadence no longer guarantees. Handing the rows to
// the mesh dirty set restores that immediacy over the existing frame.
async fn reseed_after_seek(
    st: &Arc<AppState>,
    client: &deadpool_postgres::Client,
    group: &str,
    queue: &str,
    tenant: &str,
    txt: &str,
) {
    if st.hotlist.enabled() && seek_succeeded(txt) {
        let now_ms = crate::util::now_epoch_ms();
        let qkey = crate::handlers::tenant_queue_key(tenant, queue);
        super::data::hotlist_reseed_full_broadcast(&st.hotlist, client, &qkey, group, now_ms).await;
    }
}

// POST /api/v1/consumer-groups/:group/queues/:queue/seek — move the group's
// segment cursor for every partition of the queue.
pub async fn handle_seek_consumer_group(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path((group, queue)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let (to_end, ts) = match parse_seek(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, json_err("", e)),
    };
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::seg_seek_consumer_group(&client, &group, &queue, to_end, ts.as_deref(), tenant.as_str()).await {
        Ok(txt) => {
            reseed_after_seek(&st, &client, &group, &queue, tenant.as_str(), &txt).await;
            sp_result_to_response(txt)
        }
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("seek failed: ", &e),
        ),
    }
}

// POST /api/v1/consumer-groups/:group/queues/:queue/partitions/:partition/seek —
// move the group's segment cursor for ONE partition.
pub async fn handle_seek_partition(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path((group, queue, partition)): Path<(String, String, String)>,
    body: Bytes,
) -> Response {
    // The webapp's per-partition "Skip to end" button (and the C++ per-partition
    // seek) send NO body; default an empty body to toEnd=true instead of 400ing. A
    // present body still goes through parse_seek (explicit toEnd / timestamp).
    let (to_end, ts) = if body.is_empty() {
        (true, None)
    } else {
        match parse_seek(&body) {
            Ok(v) => v,
            Err(e) => return json(StatusCode::BAD_REQUEST, json_err("", e)),
        }
    };
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::seg_seek_partition(&client, &group, &queue, &partition, to_end, ts.as_deref(), tenant.as_str()).await {
        Ok(txt) => {
            reseed_after_seek(&st, &client, &group, &queue, tenant.as_str(), &txt).await;
            sp_result_to_response(txt)
        }
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("seek failed: ", &e),
        ),
    }
}

