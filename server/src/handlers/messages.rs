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
    pack_frames, pack_segment, unpack_frames, uuid_bytes_to_string, uuid_string_to_bytes,
    zstd_compress, zstd_decompress, FrameIn,
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

// ============================== DLQ replay, on the move primitive ===========
//
// Both replay routes are the same operation with two ways of naming the row:
// `POST /api/v1/dlq/:id/replay` takes the row id the DLQ listing already
// returns, and `POST /api/v1/messages/:pid/:txn/retry` resolves its address to
// the newest row and then moves THAT id. Everything below the naming is shared,
// because the four defects the old replay had were all in the shared part: it
// minted a fresh transaction id per attempt, read the row without a lock,
// deleted it in a second statement that could fail on its own, and addressed a
// (partition, txn) pair that can carry one row per consumer group while
// deleting every one of them. The dashboard dropped its replay button for
// exactly those reasons (8d357fa4); a button is a double click away, so it only
// comes back on a primitive that cannot do any of the four.
//
// The primitive is `queen.log_dlq_move_v1` (016_messages): claim under the row
// lock, push, delete, ONE transaction. The broker's half is to pack the frame —
// outside that transaction, the same recipe the fusion flush and the timer fire
// use — and to announce the landing afterwards.
//
// One deliberate difference from the push path: PUSH MAINTENANCE REFUSES A
// MOVE, it does not divert it. A maintenance-mode push is answered `buffered`
// and replayed from the disk spool later, which a move cannot be — the spool
// carries frames, not the deletion of the dead-letter row, so a spooled move
// would be exactly the "pushed but still dead-lettered" state this replaces.
// Nor may it write: the switch's whole guarantee is that nothing reaches
// queen.log_segments while it is on, and this route would reach it through
// log_push_one_v1 (a new queue and partition included). So both routes answer
// 503 while the switch is on (`move_maintenance_response`), which loses
// nothing: the dead-letter row stays exactly where it was and the same replay
// works once maintenance is off. A move therefore either commits against the
// database or fails, and a failure leaves the row.

/// Where a move lands. Defaults to the source row's own queue and partition;
/// the replay route lets a caller override either half, which is what makes
/// this a move rather than a replay-in-place (same tenant by construction — the
/// row was read under it and the SP resolves the destination under it too).
struct MoveDest {
    queue: String,
    partition: String,
}

/// What a completed move tells the caller. `offset` is where the message is in
/// the destination partition: the allocated offset for `moved`, and the
/// PRE-EXISTING occurrence's offset for `duplicate`.
struct Moved {
    result: &'static str,
    queue: String,
    partition: String,
    offset: Option<i64>,
    message_id: String,
    transaction_id: String,
    consumer_group: String,
    /// The transaction id the DEAD-LETTERED message carried. Echoed because the
    /// replayed frame deliberately does not reuse it (that is defect 1), so
    /// without it the caller holds two ids and no link between them. Nullable in
    /// the column, and therefore here.
    original_transaction_id: Option<String>,
}

/// Why a move produced no message.
#[derive(Debug)]
enum MoveFailure {
    /// The row is not there: already replayed, purged, another tenant's, a
    /// concurrent caller took the row lock first and moved it, or the id is not
    /// a uuid at all. All of them are one answer on purpose — see the SP's
    /// header for why the tenant case is not a distinct one.
    Gone,
    /// The DATABASE refused the statement: a `tokio_postgres` error carrying a
    /// SQLSTATE, which is the broker having watched the SP raise. Every guard
    /// in `log_dlq_move_v1` raises before the DELETE, so this one really is
    /// "nothing happened" — the whole transaction rolled back and the
    /// dead-letter row is where it was.
    Broken(String),
    /// The move's outcome is NOT KNOWN here. Two ways in, and neither can be
    /// reported as "nothing happened": a transport error with no SQLSTATE (the
    /// connection can be lost AFTER a single-statement transaction committed),
    /// and a verdict the broker cannot read (a verdict came back at all, so the
    /// statement ran and its DELETE ran with it). Answered with
    /// `dlqRowRemoved: null` rather than `false`.
    Unknown(String),
}

/// The transaction id a moved frame carries, and the reason a second move of
/// the same row is a `duplicate` instead of a second copy: it is DERIVED from
/// the source row's id, not minted per attempt. (The row is deleted in the same
/// transaction, so in practice the second call sees `gone` first — the
/// deterministic id is the belt to that transaction's braces, and it is also
/// what makes a replayed message traceable back to the dead-letter record it
/// came from.)
fn move_transaction_id(dlq_id: &str) -> String {
    format!("dlq:{dlq_id}")
}

/// Parse the SP's verdict into (result, offset). A verdict the broker does not
/// recognise is `Unknown`, never silently treated as success: the statement
/// RETURNED, so it committed — and on a `moved` it committed the DELETE too —
/// which is exactly the case where "I do not know what happened" must not be
/// rendered as either "replayed" or "nothing happened".
fn parse_move_verdict(txt: &str) -> Result<(&'static str, Option<i64>), MoveFailure> {
    let v: serde_json::Value = serde_json::from_str(txt)
        .map_err(|e| MoveFailure::Unknown(format!("dlq move returned no JSON: {e}")))?;
    let off = v.get("offset").and_then(|x| x.as_i64());
    match v.get("result").and_then(|x| x.as_str()) {
        Some("moved") => Ok(("moved", off)),
        Some("duplicate") => Ok(("duplicate", off)),
        Some("gone") => Err(MoveFailure::Gone),
        _ => Err(MoveFailure::Unknown(format!(
            "dlq move returned an unknown verdict: {txt}"
        ))),
    }
}

/// The two ways a `tokio_postgres` failure can be read, and they are not the
/// same fact.
///
/// A SQLSTATE means the DATABASE answered: it parsed the statement, ran it, and
/// raised. `log_dlq_move_v1` raises only from its guards and from
/// `log_push_one_v1`, all of which are before the DELETE, so the transaction
/// rolled back whole and `dlqRowRemoved: false` is a fact. No SQLSTATE means
/// the connection itself failed, and a single-statement transaction can commit
/// and still lose its answer — so the outcome is unknown, which is a different
/// sentence for the operator (`move_unknown_response`).
fn move_failure_from_db(prefix: &str, e: &tokio_postgres::Error) -> MoveFailure {
    match e.as_db_error() {
        Some(_) => MoveFailure::Broken(format!("{prefix}{e}")),
        None => MoveFailure::Unknown(format!("{prefix}{e}")),
    }
}

/// 22P02, `invalid_text_representation`. Read at the two ROW LOOKUPS and nowhere
/// else — the id-addressed one and the (partitionId, transactionId) one — where
/// the only caller text that is cast is the address out of the path, so the code
/// can only mean "that is not a uuid" and therefore "that is not a row". (The
/// tenant those statements also cast arrives from the auth layer already
/// validated; a broker whose own tenant were malformed would fail every read,
/// not this one.) Deliberately not folded into `move_failure_from_db`: the move
/// statement casts other things too (the SP reads its own offsets), and
/// answering `gone` to one of those would tell an operator a row is gone while
/// it is still there.
fn is_not_a_uuid(e: &tokio_postgres::Error) -> bool {
    e.as_db_error()
        .is_some_and(|db| db.code().code() == "22P02")
}

/// Move one dead-letter row into the log. The whole broker-side half:
/// decrypt the snapshot, pack one frame, call the SP, announce.
async fn move_dlq_row(
    st: &Arc<AppState>,
    client: &deadpool_postgres::Client,
    tenant: &str,
    row: &db::DlqRow,
    dest: &MoveDest,
) -> Result<Moved, MoveFailure> {
    // The snapshot is stored VERBATIM, so on an encryption-enabled queue it is
    // the {encrypted,iv,authTag} envelope. Re-packing that as a payload would
    // double-encrypt it; move the PLAINTEXT and let the pack below decide
    // encryption again — for the DESTINATION, which may be a different queue
    // with a different answer.
    let plaintext: Vec<u8> = match st.encryption.decrypt_payload_bytes(row.payload.as_bytes()) {
        Some(pt) => pt,
        None => row.payload.clone().into_bytes(),
    };

    let mid = uuidv7_bytes();
    let mid_str = uuid_bytes_to_string(&mid);
    let txn = move_transaction_id(&row.id);

    // The same decision the push path makes, through the same memoized helper:
    // encryption is a property of the queue the frame LANDS in.
    let enc_on = st.encryption.is_enabled() && st.encryption_enabled_for(&dest.queue, tenant).await;
    let (payload, encrypted) = if enc_on {
        match st.encryption.encrypt(&plaintext) {
            Some(env) => (env, true),
            None => {
                // Parity with the push path: warn and store plaintext rather
                // than fail. Unsampled because a move is an operator action,
                // not an ingest rate.
                tracing::warn!(target: "dlq", queue = %dest.queue, "encryption failed; moved plaintext");
                (plaintext, false)
            }
        }
    } else {
        (plaintext, false)
    };

    // ONE frame, packed with `frames::pack_segment` — the same recipe the timer
    // fire (sweeper::pack_one) and the fusion flush use, which matters because
    // the bytes land in the same queen.log_segments.blob column and are read
    // back by the same pop. No trace id: the dead-letter record carries none,
    // and the replaying operator's request is not the message's trace. No
    // producer sub either: the DLQ snapshot does not keep the original
    // producer's identity (010_log_admin emits `producerSub: null` for every
    // row), and stamping the operator who pressed replay would attribute the
    // message to somebody who did not produce it.
    let seg = pack_segment(
        &[FrameIn {
            message_id: mid,
            txn: &txn,
            trace_id: None,
            producer_sub: None,
            payload: &payload,
            encrypted,
        }],
        st.zstd_level,
    );

    let txt = db::log_dlq_move(
        client,
        tenant,
        &row.id,
        &dest.queue,
        &dest.partition,
        &seg.hashes,
        &seg.blob,
    )
    .await
    .map_err(|e| move_failure_from_db("dlq move failed: ", &e))?;

    let (result, offset) = parse_move_verdict(&txt)?;

    // The counters `handle_push` records for a pushed frame (handlers/data.rs),
    // recorded here for the same reason: a moved frame IS in queen.log_segments
    // and will be popped from there, so a per-queue push rate that skipped it
    // would show the destination queue consuming messages nobody pushed — on
    // the very Workload page this dashboard renders. Only on `moved`:
    // `duplicate` wrote nothing, and counting it would inflate the rate with
    // frames that do not exist. The PROCESS-wide `metrics.push.record_request`
    // is deliberately left alone — that one counts push REQUESTS, and the
    // fusion sizes its cadence from it (the same reason `db::log_dlq_move`
    // skips `admission::note_commit`); a button press is not ingest.
    if result == "moved" {
        st.metrics.per_queue.add_push(tenant, &dest.queue, 1);
    }

    // MANDATORY, and the reason it is spelled here rather than left to the next
    // reseed: the frame is in a partition no pop is watching until somebody
    // says so. `handlers::announce_landed` is the one function every landing
    // path goes through, and the fire and the spool replay forgetting it is
    // exactly the 1.0.3-through-1.5.1 bug the 1.5.1 timer fix closed. A
    // `duplicate` announces too: nothing was written, so the mark is a harmless
    // false positive (the pop finds nothing and the entry clears) and the
    // alternative is a branch that can rot into the same omission.
    announce_landed(
        &st.hotlist,
        &st.notifier,
        &[(
            tenant_queue_key(tenant, &dest.queue),
            dest.partition.clone(),
            1,
        )],
    );

    Ok(Moved {
        result,
        queue: dest.queue.clone(),
        partition: dest.partition.clone(),
        offset,
        message_id: mid_str,
        transaction_id: txn,
        consumer_group: row.consumer_group.clone(),
        original_transaction_id: row.transaction_id.clone(),
    })
}

/// The 200 body both routes answer with. `replayedAs` keeps the key list the
/// five SDK wrappers and `queenctl dlq retry` already read off the old retry
/// route (a `queen_protocol::PushResult`: index, message_id, transaction_id,
/// queueName, status), so re-implementing the route on the move primitive is
/// not a wire change for them; `offset` joins it because the move knows the
/// position and a caller that wants to read the message back needs it.
///
/// On `duplicate` the frame was NOT written, so `replayedAs.message_id` is the
/// all-zero uuid — `fusion.rs::resolve_dup_mids`' "original unknown" sentinel,
/// used there for exactly this: a duplicate whose pre-existing message id the
/// broker cannot resolve. `queen_protocol::PushStatus::Duplicate` documents
/// that field as the PRE-EXISTING message's id, so the id this handler minted
/// for a frame the SP refused to store must never travel under it; the copy
/// already in the log at `offset` carries its own id inside a segment blob this
/// route does not read. The field stays a string rather than becoming null so
/// every SDK that parses `replayedAs` as a push result keeps parsing it.
///
/// `dlqRowRemoved` follows the verdict, because the SP does: `moved` deleted
/// the source row in the same transaction, `duplicate` wrote nothing and
/// therefore removed nothing (016_messages' duplicate branch says why).
fn move_response(m: &Moved, dlq_id: &str) -> Response {
    json(StatusCode::OK, move_body(m, dlq_id).to_string())
}

/// The body itself, as a value: `move_response` only wraps it. Split out so the
/// two verdicts' shapes — which the five SDK wrappers parse and the dashboard
/// renders — are pinned by a unit test instead of by a rig run.
fn move_body(m: &Moved, dlq_id: &str) -> serde_json::Value {
    let moved = m.result == "moved";
    serde_json::json!({
        "success": true,
        "result": m.result,
        "queue": m.queue,
        "partition": m.partition,
        "consumerGroup": m.consumer_group,
        "dlqId": dlq_id,
        "originalTransactionId": m.original_transaction_id,
        "replayedAs": {
            "index": 0,
            "message_id": if moved {
                m.message_id.clone()
            } else {
                uuid_bytes_to_string(&[0u8; 16])
            },
            "transaction_id": m.transaction_id,
            "queueName": m.queue,
            "status": if moved { "queued" } else { "duplicate" },
            "offset": m.offset,
        },
        "dlqRowRemoved": moved,
    })
}

/// The 500 body for a move the DATABASE refused. `dlqRowRemoved: false` is a
/// fact, not a hedge: the SP raised, every one of its guards raises before the
/// DELETE, and one transaction rolls back whole — so the dead-letter row is
/// untouched and the caller may try again without duplicating anything.
fn move_failed_response(detail: &str) -> Response {
    json(
        StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::json!({
            "success": false,
            "error": detail,
            "dlqRowRemoved": false,
            "message": "Nothing was replayed — the dead-letter row is untouched",
        })
        .to_string(),
    )
}

/// The 500 body for a move whose outcome the broker did not learn: a transport
/// failure with no SQLSTATE, or a verdict it cannot read. Both mean the
/// statement may have committed — and a committed move deleted the row — so
/// `dlqRowRemoved` is `null` and the sentence says what to do about it rather
/// than asserting a state. The same honesty the client already applies to a
/// request that got no answer at all.
fn move_unknown_response(detail: &str) -> Response {
    json(
        StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::json!({
            "success": false,
            "error": detail,
            "dlqRowRemoved": serde_json::Value::Null,
            "message": "The outcome of this replay is unknown — re-read the dead-letter list: if the row is gone, the move happened",
        })
        .to_string(),
    )
}

/// The 503 both routes answer while PUSH MAINTENANCE is on.
///
/// A maintenance-mode push is diverted to the on-disk spool
/// (`handlers/data.rs`, "nothing reaches queen.log_segments"), and a move
/// cannot be: the spool carries frames, not the deletion of the dead-letter
/// row, so a spooled move would be exactly the "pushed but still
/// dead-lettered" state this primitive exists to make impossible. Writing
/// anyway would break the switch's one guarantee — an operator turns it on
/// precisely so nothing new reaches the log — so the move is REFUSED instead,
/// which costs nothing: the row is still there and the same replay works the
/// moment the switch is off.
fn move_maintenance_response() -> Response {
    json(
        StatusCode::SERVICE_UNAVAILABLE,
        serde_json::json!({
            "success": false,
            "result": "maintenance",
            "error": "push maintenance is on",
            "dlqRowRemoved": false,
            "message": "Push maintenance is on, so nothing may be written to the log. The dead-letter row is untouched; replay it once maintenance is off",
        })
        .to_string(),
    )
}

// Optional body of the replay route: `{}` (or nothing at all) replays to the
// row's own queue and partition; either half can be overridden to move the
// message somewhere else. A present field must name something once TRIMMED —
// an empty destination name is refused here AND in the SP, because it would
// otherwise provision a queue nobody can name, and a padded one (`"  orders "`)
// is the same defect one space away: `log_push_one_v1` provisions exactly the
// text it is given, so the value that is validated has to be the value that is
// written.
#[derive(Deserialize)]
struct ReplayBody {
    queue: Option<String>,
    partition: Option<String>,
}

fn parse_replay_overrides(
    body: &Bytes,
) -> Result<(Option<String>, Option<String>), &'static str> {
    // The dashboard's plain "Replay" sends no body at all, and a `{}` from a
    // curl is the same request: neither is a 400.
    if body.iter().all(|b| b.is_ascii_whitespace()) {
        return Ok((None, None));
    }
    let b: ReplayBody = serde_json::from_slice(body)
        .map_err(|_| "bad body: expected {} or {\"queue\":\"...\",\"partition\":\"...\"}")?;
    let queue = b.queue.map(|q| q.trim().to_string());
    let partition = b.partition.map(|p| p.trim().to_string());
    if queue.as_deref().is_some_and(str::is_empty) {
        return Err("queue override must be a non-empty name");
    }
    if partition.as_deref().is_some_and(str::is_empty) {
        return Err("partition override must be a non-empty name");
    }
    Ok((queue, partition))
}

// ------------------------------------------- POST /api/v1/dlq/:id/replay
// Replay (or move) ONE dead-letter row, addressed by the row id `GET
// /api/v1/dlq` returns. Body `{}` — or none — replays to the row's own
// queue/partition; `{"queue":...,"partition":...}` overrides either half, and an
// overridden destination that does not exist yet is provisioned by
// `queen.log_push_one_v1` exactly as a first-contact producer push provisions
// one (003_log_push's missing-branch INSERTs, which the SP reaches with
// p_pid/p_window NULL).
//
// Tenancy: the row is read under the request tenant and the SP repeats the
// predicate under the lock, so another tenant's row is `gone` — the same 404 as
// a row that never existed, for the same reason the pid-addressed routes never
// answer 403 (a distinct status confirms existence across the boundary).
pub async fn handle_dlq_replay(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(dlq_id): Path<String>,
    body: Bytes,
) -> Response {
    let (queue_override, partition_override) = match parse_replay_overrides(&body) {
        Ok(v) => v,
        // The `{success:false, error, message}` shape the rest of this file uses
        // for a refused request (handle_purge_dlq's missing queue), so a client
        // reads every non-2xx here the same way.
        Err(e) => {
            return json(
                StatusCode::BAD_REQUEST,
                serde_json::json!({
                    "success": false,
                    "error": e,
                    "message": "The replay body is optional; when present it may name a queue and/or a partition to move the message to",
                })
                .to_string(),
            )
        }
    };

    let gone = || {
        json(
            StatusCode::NOT_FOUND,
            serde_json::json!({
                "success": false,
                "result": "gone",
                "dlqId": dlq_id,
                "error": "Message not found",
                "message": "No dead-letter row with this id — it was already replayed or purged",
            })
            .to_string(),
        )
    };

    // Refused, not written, while the push switch is on — see the section
    // header. Before the pool is taken: a refusal that reads no row is also the
    // cheapest one.
    if st.maintenance.load(Ordering::Relaxed) {
        return move_maintenance_response();
    }

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    // A malformed id is not a row, and it gets the same `gone` as a row that is
    // no longer there — never a 500 from the `::text::uuid` cast, which is what
    // the dashboard would render as "the replay failed".
    //
    // The DECIDER IS POSTGRES, deliberately, and not a parser here: its uuid
    // input accepts more spellings than a 32-nibble-with-optional-dashes rule
    // does (braces, and a hyphen after any group of four), so a guard written
    // in Rust would have to either mirror that exactly or answer `gone` to ids
    // this database would have resolved. `move_failure_from_db` maps the cast's
    // own 22P02 to `Gone`, which is one definition of "not a row id" instead of
    // two that can drift apart.
    let row = match db::dlq_row_for_move(&client, tenant.as_str(), &dlq_id).await {
        Ok(Some(r)) => r,
        Ok(None) => return gone(),
        Err(e) if is_not_a_uuid(&e) => return gone(),
        // Any other lookup failure: nothing was written either way — a SELECT
        // that failed changed nothing — so this is the categorical answer, not
        // the unknown one.
        Err(e) => return move_failed_response(&format!("dlq lookup failed: {e}")),
    };

    // An omitted half keeps the source's: "move this to queue X" must not also
    // silently re-partition the message, and vice versa.
    let dest = MoveDest {
        queue: queue_override.unwrap_or_else(|| row.queue.clone()),
        partition: partition_override.unwrap_or_else(|| row.partition.clone()),
    };

    match move_dlq_row(&st, &client, tenant.as_str(), &row, &dest).await {
        Ok(m) => move_response(&m, &row.id),
        Err(MoveFailure::Gone) => gone(),
        Err(MoveFailure::Broken(detail)) => move_failed_response(&detail),
        Err(MoveFailure::Unknown(detail)) => move_unknown_response(&detail),
    }
}

// ------------------------------------- POST /api/v1/messages/:pid/:txn/retry
// The same move, addressed the way this route always has been. It exists ONLY
// for dead-lettered addresses — a live message has nothing to replay and 404s —
// and its callers are the five admin SDKs, `queenctl dlq retry` and now the
// dashboard's new replay action (which prefers the id-addressed route above).
//
// (partition_id, transaction_id) can match one row PER CONSUMER GROUP. The
// address resolves to the NEWEST of them (`failed_at DESC LIMIT 1`, the choice
// this route has always made) and the move removes exactly that row: the other
// groups' dead-letter records survive a replay, where the old implementation
// deleted all of them while re-pushing one snapshot.
pub async fn handle_retry_message(
    State(st): State<Arc<AppState>>,
    // The auth layer stamps this on every request and the embedded facade passes
    // it too. It is unused here on purpose: the move stamps no producer identity
    // on the frame (see `move_dlq_row`), and the extractor stays so the handler
    // signature — which `embedded::Broker::retry_message` calls directly — does
    // not change under the facade.
    Extension(_authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path((partition_id, transaction_id)): Path<(String, String)>,
) -> Response {
    // Refused, not written, while the push switch is on — the section header
    // says why a move may neither be spooled nor write past it. Before the pool
    // is taken, exactly as on the id-addressed route.
    if st.maintenance.load(Ordering::Relaxed) {
        return move_maintenance_response();
    }
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

    let row = match db::dlq_newest_row_for_address(
        &client,
        tenant.as_str(),
        &partition_id,
        &transaction_id,
    )
    .await
    {
        Ok(Some(r)) => r,
        Ok(None) => return not_found(),
        // A malformed `:partitionId` is not an address, and it gets the same 404
        // as an address that holds no dead-letter row — never a 500 from the
        // `::text::uuid` cast in the lookup. The same reasoning as on the
        // id-addressed route, and the same decider: Postgres, whose uuid input
        // accepts more spellings than any rule written here would, so a Rust
        // pre-parse would 404 ids this database would have resolved. Note that
        // WITH tenancy on this path already answered 404 — the ownership gate
        // above asks Postgres first and reads its failure as "not owned" — so
        // this arm is also what keeps the two deployments answering alike
        // instead of the answer depending on QUEEN_TENANCY_HEADER.
        Err(e) if is_not_a_uuid(&e) => return not_found(),
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("dlq lookup failed: ", &e),
            )
        }
    };

    // This route replays IN PLACE. The destination override is the id-addressed
    // route's affordance; keeping it out of here preserves the shape every SDK
    // wrapper posts with an empty body.
    let dest = MoveDest {
        queue: row.queue.clone(),
        partition: row.partition.clone(),
    };

    match move_dlq_row(&st, &client, tenant.as_str(), &row, &dest).await {
        Ok(m) => move_response(&m, &row.id),
        Err(MoveFailure::Gone) => not_found(),
        Err(MoveFailure::Broken(detail)) => move_failed_response(&detail),
        Err(MoveFailure::Unknown(detail)) => move_unknown_response(&detail),
    }
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
    use super::{
        move_body, move_transaction_id, parse_move_verdict, parse_replay_overrides, MoveFailure,
        Moved,
    };
    use axum::body::Bytes;

    // The scheme IS the fix for defect 1 (a fresh id per attempt made every
    // double click a second copy), so it is pinned here rather than left to the
    // rig: the rig proves the SP's behaviour given this id, this proves the id.
    #[test]
    fn a_moved_frame_carries_the_dlq_row_id_as_its_transaction_id() {
        let row = "0198f3b1-4c2a-7c31-9d0e-6f2b8a1c4d55";
        assert_eq!(move_transaction_id(row), format!("dlq:{row}"));
        assert_eq!(
            move_transaction_id(row),
            move_transaction_id(row),
            "two replays of one row must address the same message, or the dedup \
             window cannot recognise the second"
        );
    }

    #[test]
    fn an_absent_or_empty_body_replays_in_place() {
        for body in ["", "{}", "  \n\t ", "{ }"] {
            let parsed = parse_replay_overrides(&Bytes::from(body))
                .unwrap_or_else(|e| panic!("{body:?} must be accepted, got {e}"));
            assert_eq!(parsed, (None, None), "{body:?} must not override anything");
        }
    }

    #[test]
    fn overrides_are_taken_one_half_at_a_time() {
        let q = parse_replay_overrides(&Bytes::from(r#"{"queue":"orders.retry"}"#)).unwrap();
        assert_eq!(q, (Some("orders.retry".to_string()), None));

        // The value that is VALIDATED has to be the value that is written:
        // log_push_one_v1 provisions the name it is given, so a padded one
        // would create a queue nobody addressing `orders.retry` can reach.
        let padded = parse_replay_overrides(&Bytes::from(
            "{\"queue\":\"  orders.retry \",\"partition\":\"\\teu-2\\n\"}",
        ))
        .unwrap();
        assert_eq!(
            padded,
            (Some("orders.retry".to_string()), Some("eu-2".to_string())),
            "a destination name must reach the SP trimmed"
        );

        let p = parse_replay_overrides(&Bytes::from(r#"{"partition":"eu-2"}"#)).unwrap();
        assert_eq!(p, (None, Some("eu-2".to_string())));

        let both =
            parse_replay_overrides(&Bytes::from(r#"{"queue":"orders.retry","partition":"eu-2"}"#))
                .unwrap();
        assert_eq!(
            both,
            (Some("orders.retry".to_string()), Some("eu-2".to_string()))
        );
    }

    #[test]
    fn a_blank_or_malformed_override_is_refused() {
        for body in [
            r#"{"queue":""}"#,
            r#"{"queue":"   "}"#,
            r#"{"partition":""}"#,
            r#"{"queue":"orders","partition":"\t"}"#,
            r#"{"queue":42}"#,
            r#"["orders"]"#,
            "null",
            "not-json",
        ] {
            assert!(
                parse_replay_overrides(&Bytes::from(body)).is_err(),
                "must be a 400: {body}"
            );
        }
    }

    #[test]
    fn the_two_success_verdicts_carry_a_position() {
        let moved = parse_move_verdict(
            r#"{"result":"moved","queue":"orders","partition":"eu-1","offset":41}"#,
        )
        .expect("moved is a success verdict");
        assert_eq!(moved, ("moved", Some(41)));

        let dup = parse_move_verdict(
            r#"{"result":"duplicate","queue":"orders","partition":"eu-1","offset":7}"#,
        )
        .expect("duplicate is a success verdict");
        assert_eq!(
            dup,
            ("duplicate", Some(7)),
            "a duplicate reports the PRE-EXISTING occurrence's offset"
        );
    }

    #[test]
    fn gone_is_not_an_error_and_everything_unknown_is() {
        assert!(matches!(
            parse_move_verdict(r#"{"result":"gone"}"#),
            Err(MoveFailure::Gone)
        ));
        // A verdict the broker cannot read must never render as "replayed" —
        // nor as "nothing happened". The statement RETURNED, so it committed,
        // and on a `moved` its DELETE committed with it: `Unknown` is the arm
        // that answers `dlqRowRemoved: null` instead of asserting a state.
        for txt in [
            r#"{"result":"queued"}"#,
            r#"{"result":null}"#,
            r#"{"status":"moved"}"#,
            "{}",
            "[]",
            "",
        ] {
            assert!(
                matches!(parse_move_verdict(txt), Err(MoveFailure::Unknown(_))),
                "must be a 500 of unknown outcome: {txt}"
            );
        }
    }

    fn moved(result: &'static str) -> Moved {
        Moved {
            result,
            queue: "orders".to_string(),
            partition: "eu-1".to_string(),
            offset: Some(7),
            message_id: "0198f3b1-4c2a-7c31-9d0e-6f2b8a1c4d55".to_string(),
            transaction_id: "dlq:0198f3b1-0000-7c31-9d0e-6f2b8a1c4d55".to_string(),
            consumer_group: "movers".to_string(),
            original_transaction_id: Some("order-4471".to_string()),
        }
    }

    /// The two 200 bodies, pinned: they are what the five SDK wrappers parse as
    /// a `PushResult` and what the dashboard renders a verdict from.
    #[test]
    fn a_move_reports_the_row_gone_and_a_duplicate_reports_it_kept() {
        let body = move_body(&moved("moved"), "0198f3b1-0000-7c31-9d0e-6f2b8a1c4d55");
        assert_eq!(body["dlqRowRemoved"], serde_json::json!(true));
        assert_eq!(body["replayedAs"]["status"], serde_json::json!("queued"));
        assert_eq!(
            body["replayedAs"]["message_id"],
            serde_json::json!("0198f3b1-4c2a-7c31-9d0e-6f2b8a1c4d55"),
            "a written frame carries the id it was written under"
        );

        let dup = move_body(&moved("duplicate"), "0198f3b1-0000-7c31-9d0e-6f2b8a1c4d55");
        // 016_messages' duplicate branch writes nothing and therefore deletes
        // nothing — the row is still dead-lettered and the list must keep it.
        assert_eq!(dup["dlqRowRemoved"], serde_json::json!(false));
        assert_eq!(dup["replayedAs"]["status"], serde_json::json!("duplicate"));
        // `PushStatus::Duplicate` documents message_id as the PRE-EXISTING
        // message's id. This route cannot read it (it is inside a segment
        // blob), so it answers the zero-uuid sentinel fusion.rs uses for the
        // same unknown — never the id it minted for a frame that was refused.
        assert_eq!(
            dup["replayedAs"]["message_id"],
            serde_json::json!("00000000-0000-0000-0000-000000000000")
        );
        assert_eq!(
            dup["replayedAs"]["offset"],
            serde_json::json!(7),
            "the duplicate's offset is where the pre-existing occurrence lives"
        );
    }
}
