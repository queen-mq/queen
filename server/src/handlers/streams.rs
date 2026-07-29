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
use crate::vegas::Vegas;

// Unwrap the streaming SP's [{idx, result}] array to the single inner result
// object. Falls back to the raw parsed value if the shape is unexpected.
fn unwrap_stream_result(txt: &str) -> serde_json::Value {
    let v: serde_json::Value = serde_json::from_str(txt).unwrap_or(serde_json::Value::Null);
    v.get(0)
        .and_then(|e| e.get("result"))
        .cloned()
        .unwrap_or(v)
}

// POST /streams/v1/queries — idempotent query registration
// (queen.streams_register_query_v1, unchanged SP). Body:
//   {name, source_queue, sink_queue?, config_hash, reset?}
// On success:false (config_hash mismatch without reset) → 409 so the SDK's
// registerQuery surfaces the reset:true hint; else 200 with the inner result.
pub async fn handle_streams_register(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let mut root: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    // Required-field validation mirrors the C++ route (400 before the SP).
    let field = |k: &str| root.get(k).and_then(|x| x.as_str()).filter(|s| !s.is_empty()).is_some();
    if !field("name") {
        return json(StatusCode::BAD_REQUEST, "{\"error\":\"name is required\"}".to_string());
    }
    if !field("source_queue") {
        return json(StatusCode::BAD_REQUEST, "{\"error\":\"source_queue is required\"}".to_string());
    }
    if !field("config_hash") {
        return json(StatusCode::BAD_REQUEST, "{\"error\":\"config_hash is required\"}".to_string());
    }
    // Stamp idx:0 and wrap in a one-element requests array.
    if let Some(obj) = root.as_object_mut() {
        obj.insert("idx".to_string(), serde_json::json!(0));
    }
    let requests = serde_json::Value::Array(vec![root]).to_string();

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::streams_register(&client, &requests).await {
        Ok(txt) => {
            let result = unwrap_stream_result(&txt);
            let ok = result.get("success").and_then(|x| x.as_bool()).unwrap_or(true);
            // Any success:false out of register here is a config_hash mismatch
            // (missing-field cases are rejected above) → 409, matching the C++ route.
            let status = if ok { StatusCode::OK } else { StatusCode::CONFLICT };
            json(status, result.to_string())
        }
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("register failed: ", &e),
        ),
    }
}

// POST /streams/v1/state/get — read state rows for one (query_id, partition_id)
// (queen.streams_state_get_v1, unchanged SP). Read-only; keys defaults to [].
// Body: {query_id, partition_id, keys?, key_prefix?, ripe_at_or_before?}. Returns
// the inner result object {success, rows}.
pub async fn handle_streams_state_get(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let mut root: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    if let Some(obj) = root.as_object_mut() {
        obj.insert("idx".to_string(), serde_json::json!(0));
        // Default keys:[] so the SP's COALESCE(r->'keys',...) always has a value.
        obj.entry("keys".to_string()).or_insert_with(|| serde_json::json!([]));
    } else {
        return json(StatusCode::BAD_REQUEST, "{\"error\":\"request body must be a JSON object\"}".to_string());
    }
    let requests = serde_json::Value::Array(vec![root]).to_string();

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::streams_state_get(&client, &requests).await {
        Ok(txt) => {
            // RUSTFIX item 25: mirror state_get.cpp:96-101 — 400 when the inner
            // result has success:false, 500 on an embedded error, else 200.
            let result = unwrap_stream_result(&txt);
            let code = if result.get("error").filter(|e| !e.is_null()).is_some() {
                StatusCode::INTERNAL_SERVER_ERROR
            } else if result.get("success").and_then(|s| s.as_bool()) == Some(false) {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::OK
            };
            json(code, result.to_string())
        }
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("state get failed: ", &e),
        ),
    }
}

// POST /streams/v1/cycle — atomic streaming cycle commit on the log engine
// (queen.log_streams_cycle_v1, 046). This is the packing handler: it converts the
// SDK's high-level push_items into broker-prepacked `sink_segments` (txn hashes +
// base64 zstd blob, exactly like handle_transaction packs `pushes`) and maps the
// SDK ack {transactionId, leaseId, status, count} to the SP's ack {ok, count}
// plus the top-level `worker` (= the source leaseId) and `release_lease`.
//
// SDK body (cycle.js commitCycle):
//   {query_id, partition_id, consumer_group, state_ops:[...], push_items:[...],
//    ack:{transactionId, leaseId, status, count}|null, release_lease}
// Each push_item (SinkOperator.buildPushItems): {queue, partition, payload}.
//
// SP element (046):
//   {idx:0, query_id, partition_id, consumer_group, worker, release_lease,
//    state_ops, sink_segments:[{queue, partition, hashesB64, blobB64, count}],
//    ack:{ok, count}|null}
// hashesB64 = base64 of the concatenated 16-byte xxh3_128 txn hashes, frame
// order (046's replacement for the seg-era metas:[{i,mid,txn}]) — the log
// engine's only per-frame SQL metadata (§3: the broker hashes, SQL stores
// bytea); mids and txn text live only inside the blob.
//
// Returns the inner result {success, query_id, partition_id, queueName,
// state_ops_applied, push_results, ack_result}. Always HTTP 200 on a completed
// SP call (the SDK inspects success/error to decide whether to retry), matching
// the C++ cycle route.
//
// On a committed cycle it ALSO runs the post-commit discoverability bookkeeping
// (hot-list mark on each sink partition + promote-on-ack for the released source
// lease) — see the block at the bottom. The SP commits everything internally, so
// no push/ack fast path runs it for us; skipping it strands sink emits until the
// 30s reseed floor. This is the log-engine form of 021's inline
// update_partition_lookup_v1.
pub async fn handle_streams_cycle(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let root: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };

    let query_id = match root.get("query_id").and_then(|x| x.as_str()).filter(|s| !s.is_empty()) {
        Some(q) => q.to_string(),
        None => return json(StatusCode::BAD_REQUEST, "{\"error\":\"query_id is required\"}".to_string()),
    };
    let partition_id = match root.get("partition_id").and_then(|x| x.as_str()).filter(|s| !s.is_empty()) {
        Some(p) => p.to_string(),
        None => return json(StatusCode::BAD_REQUEST, "{\"error\":\"partition_id is required\"}".to_string()),
    };
    let consumer_group = root
        .get("consumer_group")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    // Default true preserves the atomic full-batch cycle; a gate operator passes
    // false to retain the source lease on the un-acked tail.
    let release_lease = root.get("release_lease").and_then(|x| x.as_bool()).unwrap_or(true);
    // state_ops pass through verbatim to the SP (upsert/delete on queen_streams.state).
    let state_ops = root.get("state_ops").cloned().unwrap_or_else(|| serde_json::json!([]));

    // ---- pack push_items -> sink_segments, grouped by (queue, partition) -------
    struct SinkFrame {
        mid: [u8; 16],
        txn: String,
        payload: Vec<u8>,
        // RUSTFIX item 8: set when payload was replaced with an encryption envelope.
        encrypted: bool,
    }
    let mut groups: Vec<(String, String, Vec<SinkFrame>)> = Vec::new();
    let mut group_of: HashMap<(String, String), usize> = HashMap::new();
    if let Some(items) = root.get("push_items").and_then(|x| x.as_array()) {
        for pi in items {
            let queue = pi.get("queue").and_then(|x| x.as_str()).unwrap_or("").to_string();
            if queue.is_empty() {
                continue; // no sink queue -> nothing to push
            }
            let partition = pi
                .get("partition")
                .and_then(|x| x.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("Default")
                .to_string();
            // Stamp a UUIDv7 messageId (monotonic ordering across cycles) unless the
            // item already carries one; txn defaults to that messageId string.
            let mid = pi
                .get("messageId")
                .and_then(|x| x.as_str())
                .filter(|s| !s.is_empty())
                .and_then(uuid_string_to_bytes)
                .unwrap_or_else(uuidv7_bytes);
            let mid_str = uuid_bytes_to_string(&mid);
            let txn = pi
                .get("transactionId")
                .and_then(|x| x.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .unwrap_or_else(|| mid_str.clone());
            let payload = pi
                .get("payload")
                .cloned()
                .or_else(|| pi.get("data").cloned())
                .unwrap_or_else(|| serde_json::Value::Object(Default::default()));

            let key = (queue.clone(), partition.clone());
            let gi = *group_of.entry(key).or_insert_with(|| {
                groups.push((queue.clone(), partition.clone(), Vec::new()));
                groups.len() - 1
            });
            groups[gi].2.push(SinkFrame {
                mid,
                txn,
                payload: serde_json::to_vec(&payload).unwrap_or_default(),
                encrypted: false,
            });
        }
    }

    // RUSTFIX item 8: encrypt sink-push payloads for encryption-enabled queues
    // (warn + plaintext on failure — never fail the cycle).
    // Track B (§5): streams stay plan-gated / dedicated-only in v1 (their name
    // surfaces are scoped in a later pass), so the encryption flag is resolved on
    // the default tenant here — the only tenant a dedicated streams cell serves.
    if st.encryption.is_enabled() {
        for (queue, _partition, frames) in groups.iter_mut() {
            if frames.is_empty()
                || !st.encryption_enabled_for(queue, crate::config::DEFAULT_TENANT).await
            {
                continue;
            }
            for f in frames.iter_mut() {
                match st.encryption.encrypt(&f.payload) {
                    Some(env) => {
                        f.payload = env;
                        f.encrypted = true;
                    }
                    None => {
                        static ENC_FAIL_STREAMS: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
                        if let Some(suppressed) = ENC_FAIL_STREAMS.tick_now() {
                            tracing::warn!(target: "streams", queue = %queue, suppressed, "encryption failed; stored plaintext");
                        }
                    }
                }
            }
        }
    }

    let mut sink_segments: Vec<serde_json::Value> = Vec::with_capacity(groups.len());
    for (queue, partition, frames) in &groups {
        if frames.is_empty() {
            continue;
        }
        let fins: Vec<FrameIn> = frames
            .iter()
            .map(|f| FrameIn {
                message_id: f.mid,
                txn: &f.txn,
                trace_id: None,
                producer_sub: None,
                payload: &f.payload,
                encrypted: f.encrypted,
            })
            .collect();
        // 16B xxh3_128 per frame, concatenated in frame order then base64'd —
        // a misaligned length fails the element loudly via log_push_one_v1's
        // stride guard rather than silently mis-addressing acks (046).
        let mut hashes: Vec<u8> = Vec::with_capacity(frames.len() * 16);
        for f in frames {
            hashes.extend_from_slice(&txn_hash128(&f.txn));
        }
        let blob = zstd_compress(&pack_frames(&fins), st.zstd_level);
        let blob_b64 = base64::engine::general_purpose::STANDARD.encode(&blob);
        sink_segments.push(serde_json::json!({
            "queue": queue,
            "partition": partition,
            "hashesB64": base64::engine::general_purpose::STANDARD.encode(&hashes),
            "blobB64": blob_b64,
            "count": frames.len(),
        }));
    }

    // ---- map the SDK ack -> SP ack {ok,count} + top-level worker (= leaseId) ----
    let mut ack_ok = false;
    // The REQUESTED ack item count, kept for the post-commit ack metric: on a nack
    // 046 forces its own `ack_result.count` to 0 (it advances no cursor), so the
    // failed-item count has to come from the request or a nacked batch of N would
    // report neither an ok nor a failed ack. See the metric block below.
    let mut ack_req_count: u64 = 0;
    let (worker, ack_val) = match root.get("ack") {
        Some(a) if !a.is_null() => {
            let ok = status_is_ok(a.get("status").and_then(|x| x.as_str()));
            let count = a.get("count").and_then(|x| x.as_i64()).unwrap_or(0);
            let worker = a.get("leaseId").and_then(|x| x.as_str()).unwrap_or("").to_string();
            ack_ok = ok;
            ack_req_count = count.max(0) as u64;
            (worker, serde_json::json!({"ok": ok, "count": count}))
        }
        // idle-flush cycle: no source ack, skip the lease block SP-side.
        _ => (String::new(), serde_json::Value::Null),
    };

    // 19-wildcard-hotlist §2/§7: the (queue, partition, frame count) triples this
    // cycle emits to, captured BEFORE the SP call so the post-commit mark below
    // can make them discoverable. Same role as handle_push's `hotlist_marks`.
    let sink_marks: Vec<(String, String, u32)> = groups
        .iter()
        .filter(|(_, _, frames)| !frames.is_empty())
        .map(|(q, p, frames)| (q.clone(), p.clone(), frames.len() as u32))
        .collect();

    let element = serde_json::json!({
        "idx": 0,
        "query_id": query_id,
        "partition_id": partition_id,
        "consumer_group": consumer_group,
        "worker": worker,
        "release_lease": release_lease,
        "state_ops": state_ops,
        "sink_segments": sink_segments,
        "ack": ack_val,
    });
    let requests = serde_json::Value::Array(vec![element]).to_string();

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::streams_cycle(&client, &requests).await {
        Ok(txt) => {
            let result = unwrap_stream_result(&txt);
            // 19-wildcard-hotlist §2/§7 (streams cycle): like the transaction wire —
            // and UNLIKE handle_push — this cycle committed INSIDE the procedure
            // (log_streams_cycle_v1's inline sink push + cursor advance), so nothing
            // on the push/ack fast paths ran the post-commit bookkeeping that makes
            // the write discoverable. Without it, with QUEEN_HOTLIST on (the default)
            // a sink emit is invisible to consumers until the periodic reseed floor
            // (QUEEN_HOTLIST_RESEED_MS, 30s) sweeps it up, and the source partition
            // stays parked in the wheel until its lease expires even though the ack
            // just released it — a window emit that lands in PG in milliseconds is
            // not deliverable for tens of seconds. This is 021's inline
            // update_partition_lookup_v1 (rows engine) in its log-engine form: the
            // broker owns discoverability now, so it is a Rust-side call, not SQL.
            //
            // Gated on the element's success: the SP wraps each element in a
            // subtransaction, so success:false means nothing committed and there is
            // nothing to advertise.
            if result.get("success").and_then(|x| x.as_bool()).unwrap_or(false) {
                // Per-queue + broker-wide metric attribution for the traffic this
                // cycle committed. Same root cause as the discoverability block
                // below: the SP commits everything internally, so NEITHER
                // handle_push's per-queue rollup NOR process_acks' ack counters ran
                // for us. Without this the streaming engine's sink emits and source
                // acks are invisible to queue_lag_metrics (the per-queue Push/s and
                // Ack/s charts) and to worker_metrics (the broker-wide rollup
                // syscollect.rs flushes) — the queue visibly fills and drains while
                // both charts read flat zero.
                //
                // Gated on the element's success for the same reason as the hot-list
                // block: the SP wraps each element in a subtransaction, so
                // success:false means nothing committed and there is nothing to
                // count. NOT gated on st.hotlist.enabled() — metrics are orthogonal
                // to discoverability, so this sits ABOVE that if/else.
                //
                // Tenant is DEFAULT_TENANT, exactly as in the encryption lookup
                // above and for the same reason: streams are plan-gated /
                // dedicated-cell only in v1 (§5), so the default tenant is the only
                // one a streams cell serves.
                let mtenant = crate::config::DEFAULT_TENANT;

                // Sink pushes. One push_request per DISTINCT sink queue plus its
                // frame count as push_messages — handle_push's per_q rollup, which
                // likewise aggregates across partitions. sink_marks already excludes
                // empty groups, so a non-empty vec always carries >= 1 frame.
                if !sink_marks.is_empty() {
                    let mut per_q: HashMap<&str, u64> = HashMap::new();
                    for (q, _p, n) in &sink_marks {
                        *per_q.entry(q.as_str()).or_insert(0) += *n as u64;
                    }
                    let total: u64 = per_q.values().sum();
                    for (q, msgs) in per_q {
                        st.metrics.per_queue.add_push(mtenant, q, msgs);
                    }
                    // Broker-wide push counters (worker_metrics push_request /
                    // push_message). One cycle = one request, like handle_push. Only
                    // when the cycle actually emitted: record_request(0) books an
                    // EMPTY push, and a state-only or ack-only cycle is not one.
                    // record_batch is deliberately NOT called — its rtt histogram is
                    // the fusion flush latency (fusion.rs), a path this cycle skips.
                    st.metrics.push.record_request(total as usize);
                }

                // Source ack, attributed to the SOURCE queue — which is precisely
                // what 046 resolves and returns `queueName` for (its comment names
                // record_ack_with_queue, the C++ counterpart of add_ack). ack_result
                // is JSON null on an idle-flush cycle that carried no ack.
                if let Some(ar) = result.get("ack_result").filter(|a| !a.is_null()) {
                    // Outcome split matches the direct ack path: only accepted
                    // COMPLETIONS are ack_success, a nack is ack_failed (the
                    // worker_metrics "Failed acks (retries, errors)" column). The ok
                    // count comes from the SP (authoritative — it is what the cursor
                    // actually advanced by, including the gate partial-ack case);
                    // the failed count from the request, per ack_req_count above.
                    let committed = ar
                        .get("count")
                        .and_then(|x| x.as_i64())
                        .unwrap_or(0)
                        .max(0) as u64;
                    let (ok, failed) = if ack_ok {
                        (committed, 0u64)
                    } else {
                        (0u64, ack_req_count)
                    };
                    if let Some(sq) = result
                        .get("queueName")
                        .and_then(|x| x.as_str())
                        .filter(|s| !s.is_empty())
                    {
                        st.metrics.per_queue.add_ack(mtenant, sq, ok, failed);
                    }
                    // Broker-wide ack counters (worker_metrics ack_request +
                    // ack_{success,failed}_count), bumped even if queueName came back
                    // NULL so the rollup never under-counts what the per-queue view
                    // could not attribute. No dlq_moved: 046 hard-codes
                    // ack_result.dlq=false — a cycle has no dead-letter path.
                    st.metrics.ack.record_request((ok + failed) as usize);
                    st.metrics.ack_success.fetch_add(ok, Ordering::Relaxed);
                    st.metrics.ack_failed.fetch_add(failed, Ordering::Relaxed);
                }

                let now_ms = crate::util::now_epoch_ms();
                if st.hotlist.enabled() {
                    // mark_local (not the push path's mark_local_quiet): this is a
                    // low-frequency path with no separate notify, so it must do the
                    // local wake itself. Also queues the coalesced mesh dirty hint,
                    // so peers discover sink emits too.
                    // Track B (§5): streams stay dedicated-only in v1, so the ring key
                    // is built on the default tenant — the same tenant the encryption
                    // lookup above resolves against.
                    for (q, p, n) in &sink_marks {
                        let qkey = crate::handlers::tenant_queue_key(
                            crate::config::DEFAULT_TENANT,
                            q,
                        );
                        st.hotlist.mark_local(&qkey, p, *n, now_ms);
                    }
                    // §7 promote-on-ack: the cycle's ack released the source lease
                    // (the SP reports which). covered=true only when the ack both
                    // succeeded AND released — i.e. it completed the WHOLE leased
                    // batch, which is eligible for clear-on-ack when nothing arrived
                    // during the lease. A nack (lease released, cursor untouched)
                    // promotes with covered=false: that batch is redeliverable
                    // content. A gate partial ack RETAINS the lease and so reports
                    // lease_released=false — no promote, correctly.
                    let lease_released = result
                        .get("ack_result")
                        .and_then(|a| a.get("lease_released"))
                        .and_then(|x| x.as_bool())
                        .unwrap_or(false);
                    if lease_released {
                        if let Some(sq) = result
                            .get("queueName")
                            .and_then(|x| x.as_str())
                            .filter(|s| !s.is_empty())
                        {
                            let group = if consumer_group.is_empty() {
                                "__QUEUE_MODE__"
                            } else {
                                consumer_group.as_str()
                            };
                            st.hotlist.promote_ack(
                                &crate::handlers::tenant_queue_key(
                                    crate::config::DEFAULT_TENANT,
                                    sq,
                                ),
                                group,
                                &partition_id,
                                now_ms,
                                ack_ok && release_lease,
                            );
                        }
                    }
                } else if !sink_marks.is_empty() {
                    // Flag off ⇒ pops fall back to the SQL candidate scan, which sees
                    // the committed rows directly; all that is missing is the wake, so
                    // mirror handle_push's else-branch (local parked pops + peers).
                    let keys: Vec<(String, String)> = sink_marks
                        .iter()
                        .map(|(q, p, _)| {
                            (
                                crate::handlers::tenant_queue_key(
                                    crate::config::DEFAULT_TENANT,
                                    q,
                                ),
                                p.clone(),
                            )
                        })
                        .collect();
                    st.notifier.notify_pushed_batch(&keys);
                }
            }
            json(StatusCode::OK, result.to_string())
        }
        // The SP internalizes per-element failures as success:false; an Err here
        // is an infra/protocol error. 500 lets the SDK's HTTP client retry (the
        // whole SP call is one transaction, so a retry is safe).
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("cycle failed: ", &e),
        ),
    }
}
