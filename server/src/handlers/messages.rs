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
use crate::util::{txn_hash128, uuidv7_bytes};

// -------------------------------------------------- GET /api/v1/messages/:pid/:txn
// Per-message access (plan "Per-message access" decision): resolve (partitionId,
// transactionId) -> absolute offset via the broker-computed 16B xxh3_128 txn hash
// probed against queen.log_txns (§3: SQL never hashes), fetch the covering
// segment blob, zstd-decompress + unpack frames, and return frame
// (offset - base_offset) decoded. 404 when the log_txns rows are gone (older
// than the txns purge window) or the segment/frame no longer exists.
pub async fn handle_get_message(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path((partition_id, transaction_id)): Path<(String, String)>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    // Track B (§5) OWNERSHIP GATE: this endpoint is addressed by raw partition uuid
    // and the resolver SPs carry no tenant. Verify the pid belongs to the request
    // tenant BEFORE reading any payload; a foreign pid returns the SAME 404 as a
    // genuinely-missing message (no cross-tenant existence leak). No-op when off.
    if !st.tenant_owns_partition(&client, &partition_id, tenant.as_str()).await {
        return json(StatusCode::NOT_FOUND, "{\"error\":\"Message not found\"}".to_string());
    }

    // Resolve (base_offset, frame_idx): hash the txn (xxh3_128 BE, §3) and probe
    // queen.log_txns, then find the covering segment (frameIdx = offset - base,
    // §11). RUSTFIX item 23: if the log_txns rows have been purged (older than
    // the txns window), fall back to a bounded newest-first scan of the
    // partition's segment blobs so the message still resolves instead of 404-ing.
    let hash = txn_hash128(&transaction_id);
    let resolved: Option<(i64, i32)> =
        match db::log_resolve_position(&client, &partition_id, &hash).await {
            Ok(Some(off)) => match db::log_segment_covering(&client, &partition_id, off).await {
                Ok(Some((base, _end, _blob))) => Some((base, (off - base) as i32)),
                // Resolved but the covering segment is gone (retention won the
                // race): the frame is unrecoverable -> not found.
                Ok(None) => None,
                Err(e) => {
                    return json(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        json_err("resolve failed: ", &e),
                    )
                }
            },
            Ok(None) => {
                let mut found = None;
                if let Ok(cands) = db::seg_scan_segments(&client, &partition_id, 5000).await {
                    'scan: for (base, blob) in cands {
                        if let Some(frames) = unpack_frames(&zstd_decompress(&blob)) {
                            for (fi, fr) in frames.iter().enumerate() {
                                if fr.txn == transaction_id {
                                    found = Some((base, fi as i32));
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
    // `seq` carries the covering segment's base_offset (the log_segments PK);
    // the frame's absolute offset is seq + frame_idx.
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

    // RUSTFIX item 23: the full ~20-field detail shape (parity with the retired
    // rows-era get_message_v1, once in the messages SQL file):
    // queue/namespace/task, queueConfig, mode, consumerGroups, status, errorMessage,
    // retryCount, leaseExpiresAt. Missing detail (partition gone) degrades to nulls.
    // seg_message_detail's `seq` argument carries the ABSOLUTE offset in the log
    // engine (scalar cursor: consumed = offset <= committed; the DLQ probe is
    // offset-addressed too); its frame_idx argument is vestigial and ignored.
    let detail: serde_json::Value =
        match db::seg_message_detail(&client, &partition_id, seq + frame_idx as i64, 0).await {
            Ok(Some(txt)) => serde_json::from_str(&txt).unwrap_or_else(|_| serde_json::json!({})),
            _ => serde_json::json!({}),
        };
    let boolf = |k: &str| detail.get(k).and_then(|x| x.as_bool()).unwrap_or(false);
    let bus_groups = detail.get("busGroups").and_then(|x| x.as_i64()).unwrap_or(0);
    let is_dlq = boolf("isDlq");
    // status: dead_letter | completed | processing | pending (the status
    // derivation of the retired rows-era get_message_v1).
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
        // For a dead-lettered message this is the (partition,group) retry counter
        // snapshotted onto queen.log_dlq.retry_count at dead-letter time (the old
        // dead_letter_queue.retry_count analogue). Live messages report 0: the
        // log engine tracks retries per-(partition,group), not per-message.
        "retryCount": detail.get("dlqRetryCount").and_then(|x| x.as_i64()).unwrap_or(0),
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
// Delete a message by address. In the log engine live payloads live in
// immutable segments; the deletable rows are the DLQ snapshots in queen.log_dlq.
// This backs the DLQ manual-requeue workflow (dlq list -> re-push -> delete the
// DLQ row). A live (pending/processing/completed) message cannot be deleted at
// all, so "nothing matched" is a 404, not a 200 carrying success:false — a
// caller that ignores the body must not read a no-op as a deletion.
pub async fn handle_delete_message(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path((partition_id, transaction_id)): Path<(String, String)>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    let not_found = || {
        json(
            StatusCode::NOT_FOUND,
            serde_json::json!({
                "success": false,
                "partitionId": partition_id,
                "transactionId": transaction_id,
                "error": "Message not found",
                "message": "No dead-letter row for this address. Live messages live in immutable segments and cannot be deleted",
            })
            .to_string(),
        )
    };
    // Track B (§5) OWNERSHIP GATE (pid-addressed delete): a foreign pid must not
    // delete another tenant's DLQ row — treat it as "not found" (no-op when off).
    if !st.tenant_owns_partition(&client, &partition_id, tenant.as_str()).await {
        return not_found();
    }
    match db::delete_message(&client, &partition_id, &transaction_id).await {
        Ok(true) => {
            let out = serde_json::json!({
                "success": true,
                "partitionId": partition_id,
                "transactionId": transaction_id,
                "message": "Message deleted successfully",
            });
            json(StatusCode::OK, out.to_string())
        }
        Ok(false) => not_found(),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("delete failed: ", &e),
        ),
    }
}

// ------------------------------------- POST /api/v1/messages/:pid/:txn/retry
// Replay a dead-lettered message: re-push the queen.log_dlq payload snapshot to
// its own queue/partition, then drop the DLQ row. This is the DLQ's replay
// action; it exists ONLY for dead-lettered addresses — a live message has
// nothing to replay and 404s. NOT reached from the console: app/'s DeadLetter
// view offers Purge only. Its callers are the admin SDKs and `queenctl dlq
// retry`.
//
// The replayed frame gets a FRESH transaction id (the push path mints one from
// the new message id): reusing the original would be seen by the dedup window
// as a duplicate and silently dropped. The DLQ row is deleted only AFTER the
// push is accepted, so a failure leaves the message in the DLQ rather than
// losing it.
pub async fn handle_retry_message(
    State(st): State<Arc<AppState>>,
    Extension(authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path((partition_id, transaction_id)): Path<(String, String)>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    let not_found = || {
        json(
            StatusCode::NOT_FOUND,
            serde_json::json!({
                "success": false,
                "partitionId": partition_id,
                "transactionId": transaction_id,
                "error": "Message not found",
                "message": "No dead-letter row for this address. Only dead-lettered messages can be replayed",
            })
            .to_string(),
        )
    };
    // Track B (§5) OWNERSHIP GATE: a foreign pid must not replay (or reveal)
    // another tenant's DLQ row — same 404 as a genuinely-missing address.
    if !st.tenant_owns_partition(&client, &partition_id, tenant.as_str()).await {
        return not_found();
    }

    let (queue, partition, payload_txt) =
        match db::dlq_row_for_replay(&client, &partition_id, &transaction_id).await {
            Ok(Some(v)) => v,
            Ok(None) => return not_found(),
            Err(e) => {
                return json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json_err("dlq lookup failed: ", &e),
                )
            }
        };

    // The snapshot is stored verbatim, so on an encryption-enabled queue it is
    // the {encrypted,iv,authTag} envelope. Re-pushing that as the payload would
    // double-encrypt it; replay the PLAINTEXT and let the push path re-encrypt.
    let payload_txt = match st.encryption.decrypt_payload_bytes(payload_txt.as_bytes()) {
        Some(pt) => String::from_utf8_lossy(&pt).into_owned(),
        None => payload_txt,
    };

    // Make the freshness contract explicit instead of relying on handle_push's
    // missing-transactionId fallback. Besides documenting the security-sensitive
    // dedup boundary at the call site, this lets us reject an impossible UUID
    // collision before a replay can be mistaken for the quarantined frame.
    let replay_transaction_id = fresh_replay_transaction_id(&transaction_id);
    let push_body = serde_json::json!({
        "items": [{
            "queue": queue,
            "partition": partition,
            "payload": serde_json::from_str::<serde_json::Value>(&payload_txt)
                .unwrap_or(serde_json::Value::Null),
            "transactionId": replay_transaction_id,
        }]
    })
    .to_string();

    let resp = super::data::handle_push(
        State(st.clone()),
        Extension(authed),
        Extension(tenant),
        Bytes::from(push_body),
    )
    .await;
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap_or_default();
    if !status.is_success() {
        return json(
            status,
            serde_json::json!({
                "success": false,
                "error": "Replay push failed — the message is still in the dead-letter queue",
                "pushStatus": status.as_u16(),
                "pushResult": serde_json::from_slice::<serde_json::Value>(&body)
                    .unwrap_or(serde_json::Value::Null),
            })
            .to_string(),
        );
    }
    let first = match accepted_replay_push_result(&body, &transaction_id) {
        Some(result) => result,
        None => {
            let pushed = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({
                    "success": false,
                    "error": "Replay push was rejected — the message is still in the dead-letter queue",
                    "pushResult": pushed,
                })
                .to_string(),
            );
        }
    };

    // Push accepted: drop the DLQ row. A failure here is reported (the message
    // now exists twice: replayed AND still dead-lettered) rather than swallowed.
    match db::delete_message(&client, &partition_id, &transaction_id).await {
        Ok(removed) => json(
            StatusCode::OK,
            serde_json::json!({
                "success": true,
                "queue": queue,
                "partition": partition,
                "replayedAs": first,
                "dlqRowRemoved": removed,
            })
            .to_string(),
        ),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({
                "success": false,
                "replayed": true,
                "dlqRowRemoved": false,
                "error": format!("dlq cleanup failed: {e}"),
            })
            .to_string(),
        ),
    }
}

// A retry submits exactly one snapshot. Deleting its DLQ row is therefore safe
// only when the push response contains exactly one result and that result is a
// durable acceptance. Be deliberately fail-closed: `duplicate` is not enough
// here because the retry uses a fresh transaction id, while every failure,
// unknown status, extra result or malformed body must leave the snapshot in the
// DLQ for another operator attempt.
fn fresh_replay_transaction_id(original_transaction_id: &str) -> String {
    loop {
        let candidate = uuid_bytes_to_string(&uuidv7_bytes());
        if candidate != original_transaction_id {
            return candidate;
        }
    }
}

fn accepted_replay_push_result(
    body: &[u8],
    original_transaction_id: &str,
) -> Option<serde_json::Value> {
    let mut results: Vec<serde_json::Value> = serde_json::from_slice(body).ok()?;
    if results.len() != 1 {
        return None;
    }
    let result = results.pop()?;
    let parsed: queen_protocol::PushResult = serde_json::from_value(result.clone()).ok()?;
    (parsed.index == 0
        && !parsed.message_id.is_empty()
        && !parsed.transaction_id.is_empty()
        && parsed.transaction_id != original_transaction_id
        && !parsed.queue_name.is_empty()
        && matches!(
            parsed.status,
            queen_protocol::PushStatus::Queued | queen_protocol::PushStatus::Buffered
        ))
    .then_some(result)
}

// Enrich a list_messages_v1 result: log-queue entries come back with
// payloadAvailable:false + segment:{seq,frameIdx} — seq carries the covering
// segment's base_offset and frameIdx carries (offset - base_offset), per
// 010_log_admin's §11 key contract. Fetch each referenced segment once
// (log_segments PK), decode, and fill
// data/payload/id/transactionId/traceId/producerSub for the addressed frame —
// 010_log_admin emits id/transactionId as NULL for log entries because
// mids and txn text live only inside the blob. Segments are cached per
// (partitionId, seq) so a page that spans one segment decodes it exactly once.
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
                // 010_log_admin's log entries carry id (and transactionId) as null — the
                // frame is the only carrier of the mid; fill both from it.
                obj.insert("id".to_string(), serde_json::Value::String(f.message_id.clone()));
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
    Extension(tenant): Extension<crate::tenant::Tenant>,
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
    // Track B (§5): queen.list_messages_v1 reads `_tenant` from the filters JSON and
    // scopes the listing to that tenant's queues (default tenant when off).
    filters.insert("_tenant".to_string(), serde_json::json!(tenant.as_str()));
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
// queen.log_dlq stores payload SNAPSHOTS, so no decode is needed. Adds a `total`
// (the DLQBuilder reads result.total) alongside the SP's {messages, pagination}.
pub async fn handle_dlq(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let mut filters = filters_from_query(&params, &["queue", "consumerGroup"]);
    filters.insert("limit".to_string(), serde_json::json!(qint(&params, "limit", 100)));
    filters.insert("offset".to_string(), serde_json::json!(qint(&params, "offset", 0)));
    // Track B (§5): queen.get_dlq_messages_v1 reads `_tenant` from the filters JSON.
    filters.insert("_tenant".to_string(), serde_json::json!(tenant.as_str()));
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
    // The snapshot is stored VERBATIM at quarantine time (dlq_file_head), so on
    // an encryption-enabled queue it is the {encrypted,iv,authTag} envelope.
    // Decrypt on read — same sniff the live read paths use — or the DLQ shows
    // ciphertext and is useless for the debugging it exists for.
    decrypt_dlq_payloads(&st.encryption, &mut v);
    if let Some(obj) = v.as_object_mut() {
        let total = obj.get("messages").and_then(|m| m.as_array()).map(|a| a.len()).unwrap_or(0);
        obj.insert("total".to_string(), serde_json::json!(total));
    }
    json(StatusCode::OK, v.to_string())
}

// ------------------------------------------------------------ DELETE /api/v1/dlq
// Purge DLQ snapshots by exact queue name, optionally narrowed to an exact
// consumer group. Queue is required so an omitted query parameter can never
// become a tenant-wide delete. The SQL function repeats the tenant boundary;
// queue names alone are not globally unique.
pub async fn handle_purge_dlq(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let queue = match params.get("queue").filter(|value| !value.is_empty()) {
        Some(queue) => queue,
        None => {
            return json(
                StatusCode::BAD_REQUEST,
                serde_json::json!({
                    "success": false,
                    "error": "queue is required",
                    "message": "Bulk DLQ purge requires an exact queue name",
                })
                .to_string(),
            )
        }
    };
    let consumer_group = params
        .get("consumerGroup")
        .filter(|value| !value.is_empty())
        .map(String::as_str);

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "{\"error\":\"pool\"}".to_string(),
            )
        }
    };
    match db::purge_dlq(&client, tenant.as_str(), queue, consumer_group).await {
        Ok(deleted) => json(
            StatusCode::OK,
            serde_json::json!({
                "success": true,
                "deleted": deleted,
                "queue": queue,
                "consumerGroup": consumer_group,
            })
            .to_string(),
        ),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("bulk dlq purge failed: ", &e),
        ),
    }
}

// Walk a get_dlq_messages_v1 result and replace every encrypted `data` envelope
// with its plaintext, flagging the row with isEncrypted so the client can tell
// a decrypted payload from one that was never encrypted. A payload that does
// not sniff as an envelope (or that fails to decrypt — wrong/rotated key) is
// left exactly as stored: showing the envelope beats inventing a payload.
fn decrypt_dlq_payloads(enc: &crate::encryption::Encryption, v: &mut serde_json::Value) {
    if !enc.is_enabled() {
        return;
    }
    let msgs = match v.get_mut("messages").and_then(|m| m.as_array_mut()) {
        Some(m) => m,
        None => return,
    };
    for msg in msgs.iter_mut() {
        let obj = match msg.as_object_mut() {
            Some(o) => o,
            None => continue,
        };
        let raw = match obj.get("data") {
            Some(d) if d.is_object() => d.to_string(),
            _ => continue,
        };
        if let Some(pt) = enc.decrypt_payload_bytes(raw.as_bytes()) {
            let plain: serde_json::Value =
                serde_json::from_slice(&pt).unwrap_or(serde_json::Value::Null);
            obj.insert("data".to_string(), plain);
            obj.insert("isEncrypted".to_string(), serde_json::Value::Bool(true));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{accepted_replay_push_result, fresh_replay_transaction_id};

    #[test]
    fn dlq_replay_mints_a_transaction_id_different_from_the_original() {
        let original = "01a04f39-7c33-7000-985d-707a8e01e44f";
        let replay = fresh_replay_transaction_id(original);

        assert_ne!(replay, original);
        assert_eq!(replay.len(), 36, "replay id must retain UUID wire shape");
    }

    #[test]
    fn dlq_replay_accepts_exactly_one_durable_push_result() {
        for status in ["queued", "buffered"] {
            let body = format!(
                r#"[{{"index":0,"message_id":"m1","transaction_id":"t1","queueName":"q","status":"{status}"}}]"#
            );
            let accepted = accepted_replay_push_result(body.as_bytes(), "original-txn")
                .expect("queued and buffered are durable replay outcomes");
            assert_eq!(accepted["status"], status);
        }
    }

    #[test]
    fn dlq_replay_rejects_non_accepted_and_malformed_push_results() {
        for body in [
            r#"[{"index":0,"message_id":"m1","transaction_id":"t1","queueName":"q","status":"failed"}]"#,
            r#"[{"index":0,"message_id":"m1","transaction_id":"t1","queueName":"q","status":"duplicate"}]"#,
            r#"[{"index":0,"message_id":"m1","transaction_id":"t1","queueName":"q","status":"error"}]"#,
            r#"[{"index":0,"message_id":"m1","transaction_id":"t1","queueName":"q","status":"unknown"}]"#,
            r#"[{"index":0,"message_id":"m1","transaction_id":"t1","queueName":"q","status":"QUEUED"}]"#,
            r#"[{"index":0,"message_id":"m1","transaction_id":"t1","queueName":"q","status":null}]"#,
            r#"[{"index":0,"message_id":"m1","transaction_id":"original-txn","queueName":"q","status":"queued"}]"#,
            r#"[{"index":1,"message_id":"m1","transaction_id":"t1","queueName":"q","status":"queued"}]"#,
            r#"[{"index":0,"message_id":"","transaction_id":"t1","queueName":"q","status":"queued"}]"#,
            r#"[{"index":0,"message_id":"m1","transaction_id":"","queueName":"q","status":"queued"}]"#,
            r#"[{"index":0,"message_id":"m1","transaction_id":"t1","queueName":"","status":"queued"}]"#,
            r#"[{"status":"queued"}]"#,
            r#"[{}]"#,
            r#"[]"#,
            r#"[{"index":0,"message_id":"m1","transaction_id":"t1","queueName":"q","status":"queued"},{"index":1,"message_id":"m2","transaction_id":"t2","queueName":"q","status":"buffered"}]"#,
            r#"{"status":"queued"}"#,
            r#"["queued"]"#,
            r#"not-json"#,
        ] {
            assert!(
                accepted_replay_push_result(body.as_bytes(), "original-txn").is_none(),
                "replay result must be rejected: {body}"
            );
        }
    }
}
