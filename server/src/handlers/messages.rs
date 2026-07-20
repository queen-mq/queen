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

// -------------------------------------------------- GET /api/v1/messages/:pid/:txn
// Per-message access (plan "Per-message access" decision): resolve (partitionId,
// transactionId) -> (seq, frame_idx) via seg_dedup, fetch the segment blob,
// zstd-decompress + unpack frames, and return the addressed frame decoded. 404
// when the dedup row is gone (older than the dedup window) or the segment/frame
// no longer exists.
pub async fn handle_get_message(
    State(st): State<Arc<AppState>>,
    Path((partition_id, transaction_id)): Path<(String, String)>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let (seq, frame_idx, _mid) =
        match db::seg_resolve_position(&client, &partition_id, &transaction_id).await {
            Ok(Some(p)) => p,
            Ok(None) => {
                return json(StatusCode::NOT_FOUND, "{\"error\":\"Message not found\"}".to_string())
            }
            Err(e) => {
                return json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json_err("resolve failed: ", &e),
                )
            }
        };

    let (created_at, partition_name, blob) = match db::seg_fetch_segment(&client, &partition_id, seq).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return json(StatusCode::NOT_FOUND, "{\"error\":\"Message not found\"}".to_string())
        }
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("segment fetch failed: ", &e),
            )
        }
    };

    let raw = zstd_decompress(&blob);
    let frames = match unpack_frames(&raw) {
        Some(f) => f,
        None => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "{\"error\":\"frame decode failed\"}".to_string(),
            )
        }
    };
    let f = match frames.get(frame_idx.max(0) as usize) {
        Some(f) => f,
        None => return json(StatusCode::NOT_FOUND, "{\"error\":\"Message not found\"}".to_string()),
    };

    // Payload is stored as raw JSON bytes (or the {encrypted,iv,authTag}
    // envelope object when the frame's encrypted flag is set — crypto decode is a
    // later slice, so we surface the envelope + isEncrypted here).
    let payload: serde_json::Value = if f.payload.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&f.payload).unwrap_or(serde_json::Value::Null)
    };

    let out = serde_json::json!({
        "id": f.message_id,
        "transactionId": f.txn,
        "data": payload,
        "payload": payload,
        "traceId": f.trace_id,
        "producerSub": f.producer_sub,
        "createdAt": created_at,
        "partitionId": partition_id,
        "partition": partition_name,
        "isEncrypted": f.encrypted,
    });
    json(StatusCode::OK, out.to_string())
}

// -------------------------------------------- DELETE /api/v1/messages/:pid/:txn
// Delete a message by address. In the segments engine live payloads live in
// immutable segments; the deletable rows are the DLQ snapshots in queen.seg_dlq.
// This backs the DLQ manual-requeue workflow (dlq list -> re-push -> delete the
// DLQ row). Always 200 with {success,...}; success:false when nothing matched.
pub async fn handle_delete_message(
    State(st): State<Arc<AppState>>,
    Path((partition_id, transaction_id)): Path<(String, String)>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::delete_message(&client, &partition_id, &transaction_id).await {
        Ok(deleted) => {
            let out = serde_json::json!({
                "success": deleted,
                "partitionId": partition_id,
                "transactionId": transaction_id,
                "message": if deleted { "Message deleted successfully" } else { "Message not found" },
            });
            json(StatusCode::OK, out.to_string())
        }
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("delete failed: ", &e),
        ),
    }
}

// Enrich a list_messages_v1 result: segment-queue entries come back with
// payloadAvailable:false + segment:{seq,frameIdx}; fetch each referenced segment
// once, decode, and fill data/payload/transactionId/traceId/producerSub for the
// addressed frame. Segments are cached per (partitionId, seq) so a page that
// spans one segment decodes it exactly once.
async fn enrich_segment_payloads(client: &deadpool_postgres::Client, v: &mut serde_json::Value) {
    let msgs = match v.get_mut("messages").and_then(|m| m.as_array_mut()) {
        Some(m) => m,
        None => return,
    };
    let mut cache: HashMap<(String, i64), Option<Vec<crate::frames::FrameOut>>> = HashMap::new();
    for msg in msgs.iter_mut() {
        let obj = match msg.as_object_mut() {
            Some(o) => o,
            None => continue,
        };
        let needs = obj.get("payloadAvailable").and_then(|x| x.as_bool()) == Some(false)
            && obj.get("segment").is_some();
        if !needs {
            continue;
        }
        let pid = match obj.get("partitionId").and_then(|x| x.as_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };
        let seg = obj.get("segment").cloned().unwrap_or(serde_json::Value::Null);
        let seq = match seg.get("seq").and_then(|x| x.as_i64()) {
            Some(s) => s,
            None => continue,
        };
        let fidx = seg.get("frameIdx").and_then(|x| x.as_i64()).unwrap_or(0).max(0) as usize;

        let key = (pid.clone(), seq);
        if !cache.contains_key(&key) {
            let decoded = match db::seg_fetch_segment(client, &pid, seq).await {
                Ok(Some((_c, _p, blob))) => {
                    let raw = zstd_decompress(&blob);
                    unpack_frames(&raw)
                }
                _ => None,
            };
            cache.insert(key.clone(), decoded);
        }
        if let Some(Some(frames)) = cache.get(&key) {
            if let Some(f) = frames.get(fidx) {
                let payload: serde_json::Value = if f.payload.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&f.payload).unwrap_or(serde_json::Value::Null)
                };
                obj.insert("data".to_string(), payload.clone());
                obj.insert("payload".to_string(), payload);
                obj.insert("transactionId".to_string(), serde_json::Value::String(f.txn.clone()));
                obj.insert(
                    "traceId".to_string(),
                    f.trace_id.clone().map(serde_json::Value::String).unwrap_or(serde_json::Value::Null),
                );
                obj.insert(
                    "producerSub".to_string(),
                    f.producer_sub.clone().map(serde_json::Value::String).unwrap_or(serde_json::Value::Null),
                );
                obj.insert("isEncrypted".to_string(), serde_json::Value::Bool(f.encrypted));
                obj.insert("payloadAvailable".to_string(), serde_json::Value::Bool(true));
            }
        }
    }
}

// ---------------------------------------------------------- GET /api/v1/messages
pub async fn handle_list_messages(
    State(st): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let mut filters =
        filters_from_query(&params, &["queue", "partition", "namespace", "ns", "task", "status", "from", "to"]);
    // Accept `ns` as an alias for `namespace` (the C++ route uses `ns`).
    if let Some(ns) = filters.remove("ns") {
        filters.entry("namespace".to_string()).or_insert(ns);
    }
    filters.insert("limit".to_string(), serde_json::json!(qint(&params, "limit", 200)));
    filters.insert("offset".to_string(), serde_json::json!(qint(&params, "offset", 0)));
    let filters_json = serde_json::Value::Object(filters).to_string();

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let txt = match db::list_messages(&client, &filters_json).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("list failed: ", &e),
            )
        }
    };
    let mut v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
    enrich_segment_payloads(&client, &mut v).await;
    if let Some(obj) = v.as_object_mut() {
        let total = obj.get("messages").and_then(|m| m.as_array()).map(|a| a.len()).unwrap_or(0);
        obj.insert("total".to_string(), serde_json::json!(total));
    }
    json(StatusCode::OK, v.to_string())
}

// --------------------------------------------------------------- GET /api/v1/dlq
// queen.seg_dlq stores payload SNAPSHOTS, so no decode is needed. Adds a `total`
// (the DLQBuilder reads result.total) alongside the SP's {messages, pagination}.
pub async fn handle_dlq(
    State(st): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let mut filters = filters_from_query(&params, &["queue", "consumerGroup"]);
    filters.insert("limit".to_string(), serde_json::json!(qint(&params, "limit", 100)));
    filters.insert("offset".to_string(), serde_json::json!(qint(&params, "offset", 0)));
    let filters_json = serde_json::Value::Object(filters).to_string();

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let txt = match db::get_dlq_messages(&client, &filters_json).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("dlq failed: ", &e),
            )
        }
    };
    let mut v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
    if let Some(obj) = v.as_object_mut() {
        let total = obj.get("messages").and_then(|m| m.as_array()).map(|a| a.len()).unwrap_or(0);
        obj.insert("total".to_string(), serde_json::json!(total));
    }
    json(StatusCode::OK, v.to_string())
}

