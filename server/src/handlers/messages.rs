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

    // Resolve (seq, frame_idx) via the dedup window; RUSTFIX item 23: if the dedup
    // entry has been purged, fall back to a bounded newest-first scan of the
    // partition's segments so a message older than the dedup window still resolves
    // instead of 404-ing.
    let resolved: Option<(i64, i32)> =
        match db::seg_resolve_position(&client, &partition_id, &transaction_id).await {
            Ok(Some((seq, fidx, _))) => Some((seq, fidx)),
            Ok(None) => {
                let mut found = None;
                if let Ok(cands) = db::seg_scan_segments(&client, &partition_id, 5000).await {
                    'scan: for (s, blob) in cands {
                        if let Some(frames) = unpack_frames(&zstd_decompress(&blob)) {
                            for (fi, fr) in frames.iter().enumerate() {
                                if fr.txn == transaction_id {
                                    found = Some((s, fi as i32));
                                    break 'scan;
                                }
                            }
                        }
                    }
                }
                found
            }
            Err(e) => {
                return json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json_err("resolve failed: ", &e),
                )
            }
        };
    let (seq, frame_idx) = match resolved {
        Some(p) => p,
        None => {
            return json(StatusCode::NOT_FOUND, "{\"error\":\"Message not found\"}".to_string())
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

    // RUSTFIX item 8: decrypt the {encrypted,iv,authTag} envelope when the
    // encryption key is configured (envelope-sniff, per messages.cpp — regardless of
    // the stored flag, so migrated v0.16.0 messages decrypt too). `isEncrypted`
    // still reports the stored flag.
    let payload: serde_json::Value = if f.payload.is_empty() {
        serde_json::Value::Null
    } else if let Some(pt) = st.encryption.decrypt_payload_bytes(&f.payload) {
        serde_json::from_slice(&pt).unwrap_or(serde_json::Value::Null)
    } else {
        serde_json::from_slice(&f.payload).unwrap_or(serde_json::Value::Null)
    };

    // RUSTFIX item 23: the full ~20-field detail shape (010_messages.sql parity):
    // queue/namespace/task, queueConfig, mode, consumerGroups, status, errorMessage,
    // retryCount, leaseExpiresAt. Missing detail (partition gone) degrades to nulls.
    let detail: serde_json::Value =
        match db::seg_message_detail(&client, &partition_id, seq, frame_idx).await {
            Ok(Some(txt)) => serde_json::from_str(&txt).unwrap_or_else(|_| serde_json::json!({})),
            _ => serde_json::json!({}),
        };
    let boolf = |k: &str| detail.get(k).and_then(|x| x.as_bool()).unwrap_or(false);
    let bus_groups = detail.get("busGroups").and_then(|x| x.as_i64()).unwrap_or(0);
    let is_dlq = boolf("isDlq");
    // status: dead_letter | completed | processing | pending (010:203-224 semantics).
    let status = if is_dlq {
        "dead_letter"
    } else if (bus_groups > 0 && boolf("busAllPassed")) || (bus_groups == 0 && boolf("qmodePassed")) {
        "completed"
    } else if boolf("anyLeaseLive") {
        "processing"
    } else {
        "pending"
    };
    let get = |k: &str| detail.get(k).cloned().unwrap_or(serde_json::Value::Null);
    let queue = get("queue");
    let partition_field = get("partition");
    let queue_path = match (queue.as_str(), partition_field.as_str()) {
        (Some(q), Some(p)) => serde_json::Value::String(format!("{q}/{p}")),
        _ => serde_json::Value::Null,
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
        // --- RUSTFIX item 23 additions ---
        "queue": queue,
        "queuePath": queue_path,
        "namespace": get("namespace"),
        "task": get("task"),
        "status": status,
        "errorMessage": get("errorMessage"),
        // The segments engine has no per-message retry counter: seg_dlq stores no
        // retry_count, and retries are tracked per-(partition,group) on
        // partition_consumers.batch_retry_count. Surfacing a true per-message count
        // would need a seg_dlq schema addition populated at dead-letter time.
        "retryCount": 0,
        "leaseExpiresAt": get("leaseExpiresAt"),
        "queueConfig": get("queueConfig"),
        "mode": serde_json::json!({
            "hasQueueMode": boolf("hasQueueMode"),
            "busGroupsCount": bus_groups,
            "type": if bus_groups > 0 { "bus" } else { "queue" },
        }),
        "consumerGroups": get("consumerGroups"),
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
async fn enrich_segment_payloads(
    client: &deadpool_postgres::Client,
    enc: &crate::encryption::Encryption,
    v: &mut serde_json::Value,
) {
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
                // RUSTFIX item 8: decrypt the envelope when a key is configured
                // (sniff by shape, regardless of the stored flag).
                let payload: serde_json::Value = if f.payload.is_empty() {
                    serde_json::Value::Null
                } else if let Some(pt) = enc.decrypt_payload_bytes(&f.payload) {
                    serde_json::from_slice(&pt).unwrap_or(serde_json::Value::Null)
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
    enrich_segment_payloads(&client, &st.encryption, &mut v).await;
    // RUSTFIX item 25: surface an embedded SP {"error":...} as 500/404.
    if v.get("error").filter(|e| !e.is_null()).is_some() {
        return sp_result_to_response(v.to_string());
    }
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
    // RUSTFIX item 25: surface an embedded SP {"error":...} as 500/404.
    if v.get("error").filter(|e| !e.is_null()).is_some() {
        return sp_result_to_response(v.to_string());
    }
    if let Some(obj) = v.as_object_mut() {
        let total = obj.get("messages").and_then(|m| m.as_array()).map(|a| a.len()).unwrap_or(0);
        obj.insert("total".to_string(), serde_json::json!(total));
    }
    json(StatusCode::OK, v.to_string())
}

