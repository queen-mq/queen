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

// GET /api/v1/consumer-groups — every group across both engines. Returns the
// get_consumer_groups_v4 JSON array verbatim (the Admin client reads it as-is).
pub async fn handle_consumer_groups(State(st): State<Arc<AppState>>) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_consumer_groups(&client).await {
        Ok(txt) => json(StatusCode::OK, txt),
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
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let min_lag = qint(&params, "minLagSeconds", 60);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_lagging_partitions(&client, min_lag).await {
        Ok(txt) => json(StatusCode::OK, txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("lagging failed: ", &e),
        ),
    }
}

// GET /api/v1/consumer-groups/:group — per-queue/partition detail for one group.
pub async fn handle_consumer_group_details(
    State(st): State<Arc<AppState>>,
    Path(group): Path<String>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_consumer_group_details(&client, &group).await {
        Ok(txt) => json(StatusCode::OK, txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("details failed: ", &e),
        ),
    }
}

// DELETE /api/v1/consumer-groups/:group?deleteMetadata= — drop the group. Removes
// its segment cursors (partition_consumers, all partitions) + consumer_watermarks
// AND its rows-side coordination state (partition_consumers, consumer_watermarks,
// consumer_groups_metadata when deleteMetadata). deletedPartitions sums both
// engines. HTTP 200 with the merged SP JSON (a 204 would make the JS client
// return null).
pub async fn handle_delete_consumer_group(
    State(st): State<Arc<AppState>>,
    Path(group): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let delete_metadata = qbool(&params, "deleteMetadata", true);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let seg = match db::delete_consumer_group_seg(&client, &group, delete_metadata).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("delete(seg) failed: ", &e),
            )
        }
    };
    // Best-effort rows-side cleanup (empty for a pure-segments deployment).
    let rows = db::delete_consumer_group_rows(&client, &group, delete_metadata)
        .await
        .unwrap_or_else(|_| "{}".to_string());

    let seg_v: serde_json::Value = serde_json::from_str(&seg).unwrap_or(serde_json::Value::Null);
    let rows_v: serde_json::Value = serde_json::from_str(&rows).unwrap_or(serde_json::Value::Null);
    let seg_n = seg_v.get("deletedPartitions").and_then(|x| x.as_i64()).unwrap_or(0);
    let rows_n = rows_v.get("deletedPartitions").and_then(|x| x.as_i64()).unwrap_or(0);

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
// partition of THAT queue (partition_consumers) + its consumer_watermarks row for
// (queue, group), AND the rows-side per-queue coordination state via
// queen.delete_consumer_group_for_queue_v1 (partition_consumers, consumer_watermarks,
// and consumer_groups_metadata when deleteMetadata). Clearing the empty-scan
// watermark is what lets the group re-consume the queue from the start (an
// advanced watermark would otherwise fence off every partition). deletedPartitions
// sums both engines; HTTP 200 with the merged JSON (a 204 would make the JS client
// return null).
pub async fn handle_delete_consumer_group_for_queue(
    State(st): State<Arc<AppState>>,
    Path((group, queue)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let delete_metadata = qbool(&params, "deleteMetadata", true);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let seg_n = match db::delete_consumer_group_for_queue_seg(&client, &group, &queue).await {
        Ok(n) => n as i64,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("delete(seg) failed: ", &e),
            )
        }
    };
    // Best-effort rows-side cleanup (empty for a pure-segments deployment).
    let rows = db::delete_consumer_group_for_queue_rows(&client, &group, &queue, delete_metadata)
        .await
        .unwrap_or_else(|_| "{}".to_string());
    let rows_v: serde_json::Value = serde_json::from_str(&rows).unwrap_or(serde_json::Value::Null);
    let rows_n = rows_v.get("deletedPartitions").and_then(|x| x.as_i64()).unwrap_or(0);

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
    Path(group): Path<String>,
    body: Bytes,
) -> Response {
    let b: SubscriptionBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
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
    match db::update_consumer_group_subscription(&client, &group, &ts).await {
        Ok(txt) => json(StatusCode::OK, txt),
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

// POST /api/v1/consumer-groups/:group/queues/:queue/seek — move the group's
// segment cursor for every partition of the queue.
pub async fn handle_seek_consumer_group(
    State(st): State<Arc<AppState>>,
    Path((group, queue)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let (to_end, ts) = match parse_seek(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"{e}\"}}")),
    };
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::seg_seek_consumer_group(&client, &group, &queue, to_end, ts.as_deref()).await {
        Ok(txt) => json(StatusCode::OK, txt),
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
    Path((group, queue, partition)): Path<(String, String, String)>,
    body: Bytes,
) -> Response {
    let (to_end, ts) = match parse_seek(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"{e}\"}}")),
    };
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::seg_seek_partition(&client, &group, &queue, &partition, to_end, ts.as_deref()).await {
        Ok(txt) => json(StatusCode::OK, txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("seek failed: ", &e),
        ),
    }
}

