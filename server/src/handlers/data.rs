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

// ------------------------------------------------------------------ push
#[derive(Deserialize)]
struct PushItem<'a> {
    queue: &'a str,
    partition: Option<&'a str>,
    #[serde(borrow)]
    payload: &'a RawValue,
    #[serde(rename = "transactionId")]
    transaction_id: Option<&'a str>,
}

#[derive(Deserialize)]
struct PushBody<'a> {
    #[serde(borrow)]
    items: Vec<PushItem<'a>>,
}

// Frames staged per (queue, partition) group. `item` indexes into `results`.
struct PreFrame {
    mid: [u8; 16],
    txn: String,
    payload: Vec<u8>,
    item: usize,
}

// Resolve layer-1 duplicate followers: a follower adopts the leader's FINAL
// message_id (a leader that turned out to be a cross-flush duplicate now carries
// the pre-existing id) and inherits an "error" status if the leader errored.
fn resolve_push_followers(results: &mut [ItemResult]) {
    for i in 0..results.len() {
        if let Some(l) = results[i].dup_of {
            let leader_mid = results[l].message_id.clone();
            let leader_status = results[l].status;
            results[i].message_id = leader_mid;
            if leader_status == "error" {
                results[i].status = "error";
            }
        }
    }
}

fn render_push_results(results: &[ItemResult]) -> String {
    let mut out = String::with_capacity(results.len() * 96);
    out.push('[');
    for (i, item) in results.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"index\":");
        out.push_str(&i.to_string());
        out.push_str(",\"message_id\":\"");
        out.push_str(&item.message_id);
        out.push_str("\",\"transaction_id\":\"");
        json_escape_into(&mut out, &item.txn);
        out.push_str("\",\"queueName\":\"");
        json_escape_into(&mut out, &item.queue);
        out.push_str("\",\"status\":\"");
        out.push_str(item.status);
        out.push_str("\"}");
    }
    out.push(']');
    out
}

pub async fn handle_push(
    State(st): State<Arc<AppState>>,
    Extension(authed): Extension<crate::auth::AuthedSub>,
    body: Bytes,
) -> Response {
    let parsed: PushBody = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    let n = parsed.items.len();
    if n == 0 {
        return json(StatusCode::CREATED, "[]".to_string());
    }
    let mut results: Vec<ItemResult> = Vec::with_capacity(n);
    let mut groups: HashMap<(String, String), Vec<PreFrame>> = HashMap::new();
    // Layer 1 — intra-request first-wins dedup: (queue, partition, txn) -> the
    // leader item's index. A repeat within THIS request becomes a duplicate
    // follower of the leader and produces no frame; at render it copies the
    // leader's final message_id.
    let mut seen: HashMap<(String, String, String), usize> = HashMap::new();
    for (i, it) in parsed.items.iter().enumerate() {
        let mid = uuidv7_bytes();
        let mid_str = uuid_bytes_to_string(&mid);
        let txn = it
            .transaction_id
            .map(|s| s.to_string())
            .unwrap_or_else(|| mid_str.clone());
        let queue = it.queue.to_string();
        let partition = it.partition.unwrap_or("Default").to_string();

        if let Some(&leader) = seen.get(&(queue.clone(), partition.clone(), txn.clone())) {
            // Intra-request duplicate: follower of `leader`. message_id is
            // provisional (the leader's minted id) and finalized at render time.
            let provisional = results[leader].message_id.clone();
            results.push(ItemResult {
                message_id: provisional,
                txn,
                queue,
                status: "duplicate",
                dup_of: Some(leader),
            });
            continue;
        }
        seen.insert((queue.clone(), partition.clone(), txn.clone()), i);
        results.push(ItemResult {
            message_id: mid_str,
            txn: txn.clone(),
            queue: queue.clone(),
            status: "queued",
            dup_of: None,
        });
        groups.entry((queue, partition)).or_default().push(PreFrame {
            mid,
            txn,
            payload: it.payload.get().as_bytes().to_vec(),
            item: i,
        });
    }

    // producer_sub is stamped ONLY from the validated JWT `sub`. The request body
    // is never a source (PushItem doesn't parse `producerSub`, so a spoofed value
    // is silently dropped). The sub (when present) is carried THROUGH fusion on
    // each OwnedFrame — the flush's pack_frames stamps it into the frame
    // (FLAG_PSUB) — so auth-enabled pushes coalesce across requests exactly like
    // the anonymous path. Auth disabled, or a token with no sub, leaves it None.
    let producer_sub = authed.0.filter(|s| !s.is_empty());

    let pending = groups.len();
    // Capture the pushed (queue, partition) set before the submit loop consumes
    // `groups`, so we can wake parked pops / notify peers once the write lands.
    let notify_keys: Vec<(String, String)> = groups.keys().cloned().collect();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let state = Arc::new(PushState {
        results: Mutex::new(results),
        pending: AtomicUsize::new(pending),
        done: Mutex::new(Some(tx)),
    });
    for ((queue, partition), pfs) in groups {
        let frames: Vec<OwnedFrame> = pfs
            .into_iter()
            .map(|p| OwnedFrame {
                message_id: p.mid,
                txn: p.txn,
                payload: p.payload,
                producer_sub: producer_sub.clone(),
                state: state.clone(),
                item: p.item,
            })
            .collect();
        st.fusion.submit(AddMsg { queue, partition, frames });
    }
    let _ = rx.await;
    st.metrics.push.record_request(n);
    // The segment is committed — wake any parked long-poll pops on these queues
    // (local) and notify peer replicas (UDP) so cross-replica consume is immediate.
    for (queue, partition) in &notify_keys {
        st.notifier.notify_pushed(queue, partition);
    }

    let mut guard = state.results.lock().unwrap();
    resolve_push_followers(guard.as_mut_slice());
    let body = render_push_results(guard.as_slice());
    json(StatusCode::CREATED, body)
}

// ------------------------------------------------------------------- pop
#[derive(Deserialize)]
pub struct PopParams {
    batch: Option<i32>,
    partitions: Option<i32>,
    #[serde(rename = "autoAck")]
    auto_ack: Option<bool>,
    wait: Option<bool>,
    timeout: Option<u64>,
    #[serde(rename = "consumerGroup")]
    consumer_group: Option<String>,
    // Subscription seeding for a NEW (partition, group) cursor on first contact:
    // subscriptionMode 'new' | 'all' (default), subscriptionFrom 'now' | ISO
    // timestamp | '' (default). Threaded to the seg pop SPs (p_sub_mode /
    // p_sub_from); existing cursors are never re-seeded.
    #[serde(rename = "subscriptionMode")]
    subscription_mode: Option<String>,
    #[serde(rename = "subscriptionFrom")]
    subscription_from: Option<String>,
}

#[derive(Deserialize)]
struct PopResult {
    #[serde(default)]
    partitions: Vec<PopPart>,
    #[serde(default)]
    error: Option<String>,
}
#[derive(Deserialize)]
struct PopPart {
    partition: String,
    #[serde(rename = "partitionId")]
    partition_id: String,
    #[serde(default)]
    segments: Vec<PopSeg>,
}
#[derive(Deserialize)]
struct PopSeg {
    #[serde(rename = "startOff")]
    start_off: i32,
    take: i32,
    #[serde(rename = "createdAt")]
    created_at: String,
    blob: String,
}

// Single-partition pop result (queen.seg_pop_segments_wire_v1): segments +
// partitionId, no partition name (the caller knows it from the path).
#[derive(Deserialize)]
struct PopSpecificResult {
    #[serde(default)]
    segments: Vec<PopSeg>,
    #[serde(rename = "partitionId", default)]
    partition_id: String,
    #[serde(default)]
    error: Option<String>,
}

pub async fn handle_pop(
    State(st): State<Arc<AppState>>,
    Path(queue): Path<String>,
    Query(p): Query<PopParams>,
) -> Response {
    // Pop maintenance: consumers get an empty, paused result (204) — matches the
    // C++ pop-maintenance behavior, but shaped so the client's empty-response
    // handling (no messages) simply retries.
    if st.pop_maintenance.load(Ordering::Relaxed) {
        return json(StatusCode::NO_CONTENT, "{\"messages\":[],\"paused\":true}".to_string());
    }
    let batch = p.batch.unwrap_or(200);
    let max_parts = p.partitions.unwrap_or(1);
    let auto_ack = p.auto_ack.unwrap_or(false);
    let wait = p.wait.unwrap_or(false);
    let timeout_ms = p.timeout.unwrap_or(st.pop_default_timeout_ms);
    let group = p.consumer_group.unwrap_or_else(|| "__QUEUE_MODE__".to_string());
    let sub_mode = p.subscription_mode.unwrap_or_else(|| "all".to_string());
    let sub_from = p.subscription_from.unwrap_or_default();
    let worker = uuid_bytes_to_string(&uuidv7_bytes());
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let lease_seconds = st.lease_time_for(&queue).await;

    loop {
        let permit = st.pop_vegas.acquire().await;
        let client = match st.pool.get().await {
            Ok(c) => c,
            Err(_) => {
                drop(permit);
                return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string());
            }
        };
        let t0 = Instant::now();
        let res = tokio::time::timeout(
            st.stmt_timeout,
            db::pop_wildcard(
                &client, &queue, &group, batch, lease_seconds, &worker, auto_ack, max_parts,
                &sub_mode, &sub_from,
            ),
        )
        .await;
        let rtt = t0.elapsed();
        st.pop_vegas.record(rtt);
        drop(permit);

        let txt = match res {
            Ok(Ok(t)) => t,
            _ => {
                return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pop failed\"}".to_string())
            }
        };

        // On a leased (non-autoAck) pop, the worker id IS the lease id the client
        // echoes back in ack/renew. autoAck pops advance the cursor server-side and
        // carry no lease, so they report an empty leaseId.
        let lease_id: &str = if auto_ack { "" } else { &worker };
        let (body, count) = build_pop_response(&txt, &queue, &group, lease_id);
        if count == 0 && wait && Instant::now() < deadline {
            // Park on the queue's wake gate instead of a blind sleep: a push (local
            // or peer MESSAGE_AVAILABLE) returns us at once; else we re-poll after
            // the poll interval, exactly as before.
            let waitd = deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(st.pop_wait_poll_ms));
            st.notifier.wait_queue(&queue, waitd).await;
            continue;
        }
        st.metrics.pop.record_request(count);
        st.metrics.pop.record_batch(count, true, rtt);
        return json(if count == 0 { StatusCode::NO_CONTENT } else { StatusCode::OK }, body);
    }
}

// GET /api/v1/pop/queue/:queue/partition/:partition — pop from ONE named
// partition. Same query params + long-poll + lease/leaseId semantics as the
// wildcard path; only the SP call and response adapter differ (single-partition
// shape). `partitions` is ignored here (a specific pop is one partition).
pub async fn handle_pop_partition(
    State(st): State<Arc<AppState>>,
    Path((queue, partition)): Path<(String, String)>,
    Query(p): Query<PopParams>,
) -> Response {
    if st.pop_maintenance.load(Ordering::Relaxed) {
        return json(StatusCode::NO_CONTENT, "{\"messages\":[],\"paused\":true}".to_string());
    }
    let batch = p.batch.unwrap_or(200);
    let auto_ack = p.auto_ack.unwrap_or(false);
    let wait = p.wait.unwrap_or(false);
    let timeout_ms = p.timeout.unwrap_or(st.pop_default_timeout_ms);
    let group = p.consumer_group.unwrap_or_else(|| "__QUEUE_MODE__".to_string());
    let sub_mode = p.subscription_mode.unwrap_or_else(|| "all".to_string());
    let sub_from = p.subscription_from.unwrap_or_default();
    let worker = uuid_bytes_to_string(&uuidv7_bytes());
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let lease_seconds = st.lease_time_for(&queue).await;

    loop {
        let permit = st.pop_vegas.acquire().await;
        let client = match st.pool.get().await {
            Ok(c) => c,
            Err(_) => {
                drop(permit);
                return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string());
            }
        };
        let t0 = Instant::now();
        let res = tokio::time::timeout(
            st.stmt_timeout,
            db::pop_specific(
                &client, &queue, &partition, &group, batch, lease_seconds, &worker,
                auto_ack, &sub_mode, &sub_from,
            ),
        )
        .await;
        let rtt = t0.elapsed();
        st.pop_vegas.record(rtt);
        drop(permit);

        let txt = match res {
            Ok(Ok(t)) => t,
            _ => {
                return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pop failed\"}".to_string())
            }
        };

        let lease_id: &str = if auto_ack { "" } else { &worker };
        let (body, count) = build_pop_specific_response(&txt, &queue, &partition, &group, lease_id);
        if count == 0 && wait && Instant::now() < deadline {
            // A push to any partition of this queue wakes us; we re-poll our
            // partition. Falls back to the poll interval on a missed wake.
            let waitd = deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(st.pop_wait_poll_ms));
            st.notifier.wait_queue(&queue, waitd).await;
            continue;
        }
        st.metrics.pop.record_request(count);
        st.metrics.pop.record_batch(count, true, rtt);
        return json(if count == 0 { StatusCode::NO_CONTENT } else { StatusCode::OK }, body);
    }
}

// Discovery pop params — same knobs as PopParams plus the namespace/task scope.
// This is the bare `GET /api/v1/pop` the clients issue for
// `client.queue().namespace_name(ns).consume(...)` (no queue in the path).
#[derive(Deserialize)]
pub struct PopDiscoverParams {
    batch: Option<i32>,
    partitions: Option<i32>,
    #[serde(rename = "autoAck")]
    auto_ack: Option<bool>,
    wait: Option<bool>,
    timeout: Option<u64>,
    #[serde(rename = "consumerGroup")]
    consumer_group: Option<String>,
    namespace: Option<String>,
    task: Option<String>,
    #[serde(rename = "subscriptionMode")]
    subscription_mode: Option<String>,
    #[serde(rename = "subscriptionFrom")]
    subscription_from: Option<String>,
}

// GET /api/v1/pop?namespace=&task=&consumerGroup=... — namespace/task discovery
// pop. Resolves every segment queue whose queen.queues row matches the requested
// namespace/task and wildcard-pops across their partitions in one call
// (queen.seg_pop_discover_wire_v1), returning the SAME response shape as
// handle_pop. Same long-poll + lease/leaseId semantics; ack/attempt work
// identically because the SP reuses the per-partition seg_pop_segments_v1 path.
// At least one of namespace/task must be provided (the clients never send a bare
// pop without one — QueueBuilder.pop throws first — so a neither-provided call is
// a 400 rather than an unbounded scan of every queue).
pub async fn handle_pop_discover(
    State(st): State<Arc<AppState>>,
    Query(p): Query<PopDiscoverParams>,
) -> Response {
    if st.pop_maintenance.load(Ordering::Relaxed) {
        return json(StatusCode::NO_CONTENT, "{\"messages\":[],\"paused\":true}".to_string());
    }
    let namespace = p.namespace.unwrap_or_default();
    let task = p.task.unwrap_or_default();
    if namespace.is_empty() && task.is_empty() {
        return json(
            StatusCode::BAD_REQUEST,
            "{\"success\":false,\"error\":\"namespace or task is required\",\"messages\":[]}".to_string(),
        );
    }
    let batch = p.batch.unwrap_or(200);
    let max_parts = p.partitions.unwrap_or(1);
    let auto_ack = p.auto_ack.unwrap_or(false);
    let wait = p.wait.unwrap_or(false);
    let timeout_ms = p.timeout.unwrap_or(st.pop_default_timeout_ms);
    let group = p.consumer_group.unwrap_or_else(|| "__QUEUE_MODE__".to_string());
    let sub_mode = p.subscription_mode.unwrap_or_else(|| "all".to_string());
    let sub_from = p.subscription_from.unwrap_or_default();
    let worker = uuid_bytes_to_string(&uuidv7_bytes());
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    // No single queue to read a lease from: the SP leases each partition with its
    // own queue's configured lease_time; this is only the fallback for a matching
    // queue that has no seg_queues.lease_time.
    let lease_seconds = DEFAULT_LEASE_SECONDS;

    loop {
        let permit = st.pop_vegas.acquire().await;
        let client = match st.pool.get().await {
            Ok(c) => c,
            Err(_) => {
                drop(permit);
                return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string());
            }
        };
        let t0 = Instant::now();
        let res = tokio::time::timeout(
            st.stmt_timeout,
            db::pop_discover(
                &client, &namespace, &task, &group, batch, lease_seconds, &worker,
                auto_ack, max_parts, &sub_mode, &sub_from,
            ),
        )
        .await;
        let rtt = t0.elapsed();
        st.pop_vegas.record(rtt);
        drop(permit);

        let txt = match res {
            Ok(Ok(t)) => t,
            _ => {
                return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pop failed\"}".to_string())
            }
        };

        let lease_id: &str = if auto_ack { "" } else { &worker };
        // Discovery spans queues, so there is no single top-level queue name; the
        // per-message JSON carries partitionId/leaseId/consumerGroup (all the ack
        // needs), and the top-level "queue" field is left empty.
        let (body, count) = build_pop_response(&txt, "", &group, lease_id);
        if count == 0 && wait && Instant::now() < deadline {
            // Discovery pops span queues → park on the shared gate, woken by any push.
            let waitd = deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(st.pop_wait_poll_ms));
            st.notifier.wait_any(waitd).await;
            continue;
        }
        st.metrics.pop.record_request(count);
        st.metrics.pop.record_batch(count, true, rtt);
        return json(if count == 0 { StatusCode::NO_CONTENT } else { StatusCode::OK }, body);
    }
}

fn pop_error_body(e: &str) -> (String, usize) {
    let mut out = String::from("{\"success\":false,\"error\":\"");
    json_escape_into(&mut out, e);
    out.push_str("\",\"messages\":[]}");
    (out, 0)
}

// Wildcard pop response: SP result is {"partitions":[{partition,partitionId,segments}]}.
fn build_pop_response(txt: &str, queue: &str, group: &str, lease_id: &str) -> (String, usize) {
    let parsed: PopResult = match serde_json::from_str(txt) {
        Ok(p) => p,
        Err(_) => return pop_error_body("parse"),
    };
    if let Some(e) = parsed.error {
        return pop_error_body(&e);
    }
    render_pop_parts(&parsed.partitions, queue, group, lease_id)
}

// Specific-partition pop response: SP result is single-partition shaped
// ({"segments":[...],"partitionId":..}) with no partition NAME — the broker
// supplies the name from the request path. Adapts it to the same per-partition
// structure the wildcard renderer consumes so every message emits the identical
// per-message JSON and top-level fields.
fn build_pop_specific_response(
    txt: &str,
    queue: &str,
    partition: &str,
    group: &str,
    lease_id: &str,
) -> (String, usize) {
    let parsed: PopSpecificResult = match serde_json::from_str(txt) {
        Ok(p) => p,
        Err(_) => return pop_error_body("parse"),
    };
    if let Some(e) = parsed.error {
        return pop_error_body(&e);
    }
    let part = PopPart {
        partition: partition.to_string(),
        partition_id: parsed.partition_id,
        segments: parsed.segments,
    };
    render_pop_parts(std::slice::from_ref(&part), queue, group, lease_id)
}

// Shared renderer: decode + slice each partition's segment frames into the
// wire per-message JSON, then wrap with the common top-level fields.
fn render_pop_parts(parts: &[PopPart], queue: &str, group: &str, lease_id: &str) -> (String, usize) {
    let mut msgs = String::new();
    let mut count = 0usize;
    let mut first_name = String::new();
    let mut first_id = String::new();
    let mut first_set = false;
    for part in parts {
        if !first_set {
            first_name = part.partition.clone();
            first_id = part.partition_id.clone();
            first_set = true;
        }
        for seg in &part.segments {
            // Postgres encode(...,'base64') wraps lines at 76 cols — strip
            // whitespace before decoding (STANDARD rejects non-alphabet bytes).
            let b64: Vec<u8> = seg
                .blob
                .bytes()
                .filter(|b| !b.is_ascii_whitespace())
                .collect();
            let blob = match base64::engine::general_purpose::STANDARD.decode(&b64) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let raw = zstd_decompress(&blob);
            let frames = match unpack_frames(&raw) {
                Some(f) => f,
                None => continue,
            };
            let start = seg.start_off.max(0) as usize;
            let take = seg.take.max(0) as usize;
            let end = (start + take).min(frames.len());
            for f in frames.iter().take(end).skip(start) {
                if !msgs.is_empty() {
                    msgs.push(',');
                }
                msgs.push_str("{\"id\":\"");
                json_escape_into(&mut msgs, &f.message_id);
                msgs.push_str("\",\"transactionId\":\"");
                json_escape_into(&mut msgs, &f.txn);
                msgs.push_str("\",\"traceId\":");
                match &f.trace_id {
                    Some(t) => {
                        msgs.push('"');
                        json_escape_into(&mut msgs, t);
                        msgs.push('"');
                    }
                    None => msgs.push_str("null"),
                }
                msgs.push_str(",\"data\":");
                if f.payload.is_empty() {
                    msgs.push_str("null");
                } else {
                    // raw splice: payload is already valid JSON
                    msgs.push_str(&String::from_utf8_lossy(&f.payload));
                }
                msgs.push_str(",\"producerSub\":");
                match &f.producer_sub {
                    Some(ps) => {
                        msgs.push('"');
                        json_escape_into(&mut msgs, ps);
                        msgs.push('"');
                    }
                    None => msgs.push_str("null"),
                }
                msgs.push_str(",\"createdAt\":\"");
                json_escape_into(&mut msgs, &seg.created_at);
                msgs.push_str("\",\"partitionId\":\"");
                json_escape_into(&mut msgs, &part.partition_id);
                msgs.push_str("\",\"partition\":\"");
                json_escape_into(&mut msgs, &part.partition);
                msgs.push_str("\",\"leaseId\":\"");
                json_escape_into(&mut msgs, lease_id);
                msgs.push_str("\",\"consumerGroup\":\"");
                json_escape_into(&mut msgs, group);
                msgs.push_str("\"}");
                count += 1;
            }
        }
    }
    let mut out = String::with_capacity(msgs.len() + 256);
    out.push_str("{\"success\":true,\"queue\":\"");
    json_escape_into(&mut out, queue);
    out.push_str("\",\"partition\":\"");
    json_escape_into(&mut out, &first_name);
    out.push_str("\",\"partitionId\":\"");
    json_escape_into(&mut out, &first_id);
    out.push_str("\",\"leaseId\":\"");
    json_escape_into(&mut out, lease_id);
    out.push_str("\",\"consumerGroup\":\"");
    json_escape_into(&mut out, group);
    out.push_str("\",\"messages\":[");
    out.push_str(&msgs);
    out.push_str("],\"partitionsClaimed\":");
    out.push_str(&parts.len().to_string());
    out.push('}');
    (out, count)
}

// ------------------------------------------------------------------- ack
// Wire contract (matches the C++ broker + JS client):
//   POST /api/v1/ack        {transactionId, partitionId, status, consumerGroup?, leaseId?}
//   POST /api/v1/ack/batch  {consumerGroup?, acknowledgments:[{transactionId, partitionId, status, leaseId?}]}
// Response is a TOP-LEVEL ARRAY, one element per ack, in request order:
//   [{index, transactionId, success, error, leaseReleased, dlq}]
#[derive(Deserialize)]
struct AckSingle {
    #[serde(rename = "transactionId")]
    transaction_id: Option<String>,
    #[serde(rename = "partitionId")]
    partition_id: Option<String>,
    status: Option<String>,
    #[serde(rename = "consumerGroup")]
    consumer_group: Option<String>,
    #[serde(rename = "leaseId")]
    lease_id: Option<String>,
    // Failure reason for a nack (status:'failed'); recorded on the DLQ row when
    // this nack exhausts the retry budget on a DLQ-enabled queue.
    error: Option<String>,
}

#[derive(Deserialize)]
struct AckBatchItem {
    #[serde(rename = "transactionId")]
    transaction_id: Option<String>,
    #[serde(rename = "partitionId")]
    partition_id: Option<String>,
    status: Option<String>,
    #[serde(rename = "leaseId")]
    lease_id: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct AckBatch {
    #[serde(default)]
    acknowledgments: Vec<AckBatchItem>,
    #[serde(rename = "consumerGroup")]
    consumer_group: Option<String>,
}

// One normalized ack: original request index + resolution inputs.
struct Ack {
    txn: String,
    partition_id: String,
    worker: String,
    ok: bool,
    // Nack failure reason, threaded into the DLQ snapshot when retries exhaust.
    error: Option<String>,
}

pub async fn handle_ack(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let a: AckSingle = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    let group = a.consumer_group.clone().unwrap_or_else(|| "__QUEUE_MODE__".to_string());
    let acks = vec![Ack {
        txn: a.transaction_id.unwrap_or_default(),
        partition_id: a.partition_id.unwrap_or_default(),
        worker: a.lease_id.unwrap_or_default(),
        ok: status_is_ok(a.status.as_deref()),
        error: a.error.filter(|s| !s.is_empty()),
    }];
    let body = process_acks(&st, &group, acks).await;
    json(StatusCode::OK, body)
}

pub async fn handle_ack_batch(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let b: AckBatch = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    let group = b.consumer_group.clone().unwrap_or_else(|| "__QUEUE_MODE__".to_string());
    let acks: Vec<Ack> = b
        .acknowledgments
        .into_iter()
        .map(|it| Ack {
            txn: it.transaction_id.unwrap_or_default(),
            partition_id: it.partition_id.unwrap_or_default(),
            worker: it.lease_id.unwrap_or_default(),
            ok: status_is_ok(it.status.as_deref()),
            error: it.error.filter(|s| !s.is_empty()),
        })
        .collect();
    let body = process_acks(&st, &group, acks).await;
    json(StatusCode::OK, body)
}

// Resolve acks per (partition, worker) via seg_ack_by_txn_v1, then emit the
// per-item result array in the original order.
async fn process_acks(st: &Arc<AppState>, group: &str, acks: Vec<Ack>) -> String {
    let n = acks.len();
    let mut success = vec![false; n];
    let mut errors: Vec<Option<String>> = vec![None; n];
    let mut lease_released = vec![false; n];
    let mut dlq_flags = vec![false; n];

    // Group item indices by (partition_id, worker): one seg_ack_by_txn_v1 call each.
    let mut groups: HashMap<(String, String), Vec<usize>> = HashMap::new();
    for (i, a) in acks.iter().enumerate() {
        groups
            .entry((a.partition_id.clone(), a.worker.clone()))
            .or_default()
            .push(i);
    }

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => {
            for e in errors.iter_mut() {
                *e = Some("pool".to_string());
            }
            return render_ack_results(&acks, &success, &errors, &lease_released, &dlq_flags);
        }
    };

    for ((pid, worker), idxs) in groups {
        // Build the [{txn, ok}] array for this partition's acks.
        let mut aj = String::from("[");
        for (k, &i) in idxs.iter().enumerate() {
            if k > 0 {
                aj.push(',');
            }
            aj.push_str("{\"txn\":\"");
            json_escape_into(&mut aj, &acks[i].txn);
            aj.push_str("\",\"ok\":");
            aj.push_str(if acks[i].ok { "true" } else { "false" });
            aj.push('}');
        }
        aj.push(']');

        match db::ack_by_txn(&client, &pid, group, &worker, &aj).await {
            Ok(txt) => {
                // {"ok":bool,"error":?,...}
                let v: serde_json::Value =
                    serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
                let sp_ok = v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false);
                if sp_ok {
                    // DLQ hand-off: the SP signalled that the head un-acked frame
                    // is poison (a nack whose delivery attempt exceeded the queue's
                    // retry_limit on a DLQ-enabled queue). It kept the lease held
                    // so we can decode the leased segment, snapshot the poison
                    // frame's payload, and file the seg_dlq row (seg_dlq_head then
                    // advances the cursor + releases the lease).
                    if v.get("dlq").and_then(|x| x.as_bool()).unwrap_or(false) {
                        let seq = v.get("seq").and_then(|x| x.as_i64()).unwrap_or(0);
                        let frame_idx =
                            v.get("frameIdx").and_then(|x| x.as_i64()).unwrap_or(0) as i32;
                        match dlq_file_head(&client, &pid, group, &worker, seq, frame_idx, &acks, &idxs)
                            .await
                        {
                            Ok(true) => {
                                for &i in &idxs {
                                    success[i] = true;
                                    lease_released[i] = true;
                                    dlq_flags[i] = true;
                                }
                            }
                            Ok(false) => {
                                for &i in &idxs {
                                    errors[i] = Some("dlq rejected".to_string());
                                }
                            }
                            Err(e) => {
                                let msg = e.to_string();
                                for &i in &idxs {
                                    errors[i] = Some(msg.clone());
                                }
                            }
                        }
                    } else {
                        for &i in &idxs {
                            success[i] = true;
                            lease_released[i] = true;
                        }
                    }
                } else {
                    let err = v
                        .get("error")
                        .and_then(|x| x.as_str())
                        .unwrap_or("ack rejected")
                        .to_string();
                    for &i in &idxs {
                        errors[i] = Some(err.clone());
                    }
                }
            }
            Err(e) => {
                let msg = e.to_string();
                for &i in &idxs {
                    errors[i] = Some(msg.clone());
                }
            }
        }
    }

    render_ack_results(&acks, &success, &errors, &lease_released, &dlq_flags)
}

// Decode the leased segment, extract the poison HEAD frame (payload snapshot,
// txn, message_id), pick the failure reason from the matching nack, and file the
// seg_dlq row via seg_dlq_head (which advances the cursor + releases the lease).
// Ok(true) => filed, Ok(false) => couldn't extract / SP rejected.
async fn dlq_file_head(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    group: &str,
    worker: &str,
    seq: i64,
    frame_idx: i32,
    acks: &[Ack],
    idxs: &[usize],
) -> Result<bool, tokio_postgres::Error> {
    let (payload, txn, message_id) = match db::seg_fetch_segment(client, partition_id, seq).await? {
        Some((_created, _pname, blob)) => {
            let raw = zstd_decompress(&blob);
            match unpack_frames(&raw) {
                Some(frames) => match frames.get(frame_idx.max(0) as usize) {
                    Some(f) => {
                        let payload = if f.payload.is_empty() {
                            "null".to_string()
                        } else {
                            String::from_utf8_lossy(&f.payload).into_owned()
                        };
                        (payload, f.txn.clone(), f.message_id.clone())
                    }
                    None => return Ok(false),
                },
                None => return Ok(false),
            }
        }
        None => return Ok(false),
    };

    // Failure reason: the nacked ack whose txn matches the poison frame; else the
    // first nacked error in the group; else a default (v1's "Retries exhausted").
    let error = idxs
        .iter()
        .map(|&i| &acks[i])
        .filter(|a| !a.ok)
        .find(|a| a.txn == txn)
        .and_then(|a| a.error.clone())
        .or_else(|| {
            idxs.iter()
                .map(|&i| &acks[i])
                .filter(|a| !a.ok)
                .find_map(|a| a.error.clone())
        })
        .unwrap_or_else(|| "Retries exhausted".to_string());

    let res = db::seg_dlq_head(
        client, partition_id, group, worker, seq, frame_idx, &message_id, &txn, &payload, &error,
    )
    .await?;
    let v: serde_json::Value = serde_json::from_str(&res).unwrap_or(serde_json::Value::Null);
    Ok(v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false))
}

fn render_ack_results(
    acks: &[Ack],
    success: &[bool],
    errors: &[Option<String>],
    lease_released: &[bool],
    dlq_flags: &[bool],
) -> String {
    let mut out = String::from("[");
    for (i, a) in acks.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"index\":");
        out.push_str(&i.to_string());
        out.push_str(",\"transactionId\":\"");
        json_escape_into(&mut out, &a.txn);
        out.push_str("\",\"success\":");
        out.push_str(if success[i] { "true" } else { "false" });
        out.push_str(",\"error\":");
        match &errors[i] {
            Some(e) => {
                out.push('"');
                json_escape_into(&mut out, e);
                out.push('"');
            }
            None => out.push_str("null"),
        }
        out.push_str(",\"leaseReleased\":");
        out.push_str(if lease_released[i] { "true" } else { "false" });
        out.push_str(",\"dlq\":");
        out.push_str(if dlq_flags[i] { "true" } else { "false" });
        out.push('}');
    }
    out.push(']');
    out
}

// ---------------------------------------------------------------- lease/extend
// POST /api/v1/lease/:leaseId/extend  body {"seconds":60} (default 60).
// Renews every partition_consumers lease held by :leaseId (= the worker id minted at
// pop) via queen.seg_renew_lease_v1. Always HTTP 200 (best-effort renewal, like
// the rows engine). The response carries every key the clients read:
//   JS:  result.leaseId ? result.newExpiresAt : result.lease_expires_at
//   Go:  result["newExpiresAt"] (RFC3339 string)
#[derive(Deserialize)]
struct RenewBody {
    seconds: Option<i32>,
}

pub async fn handle_lease_extend(
    State(st): State<Arc<AppState>>,
    Path(lease_id): Path<String>,
    body: Bytes,
) -> Response {
    let seconds = if body.is_empty() {
        60
    } else {
        serde_json::from_slice::<RenewBody>(&body)
            .ok()
            .and_then(|b| b.seconds)
            .unwrap_or(60)
    };

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    match db::renew_lease(&client, &lease_id, seconds).await {
        Ok(txt) => {
            // {"renewed":n,"expiresAt":iso|null}
            let v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
            let renewed = v.get("renewed").and_then(|x| x.as_i64()).unwrap_or(0);
            let expires = v.get("expiresAt").and_then(|x| x.as_str());

            let mut out = String::from("{\"leaseId\":\"");
            json_escape_into(&mut out, &lease_id);
            out.push_str("\",\"success\":");
            out.push_str(if renewed > 0 { "true" } else { "false" });
            out.push_str(",\"renewed\":");
            out.push_str(&renewed.to_string());
            // Same value under the three keys different clients look for.
            for key in ["newExpiresAt", "expiresAt", "lease_expires_at"] {
                out.push_str(",\"");
                out.push_str(key);
                out.push_str("\":");
                match expires {
                    Some(e) => {
                        out.push('"');
                        json_escape_into(&mut out, e);
                        out.push('"');
                    }
                    None => out.push_str("null"),
                }
            }
            out.push('}');
            json(StatusCode::OK, out)
        }
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("renew failed: ", &e),
        ),
    }
}

// ------------------------------------------------------------------ transaction
// POST /api/v1/transaction — atomic multi-op push+ack through one call to
// queen.seg_transaction_wire_v1. Body:
//   {"operations":[
//      {"type":"push","items":[{queue,partition?,payload,transactionId?,traceId?}]}
//      | {"type":"push",queue,payload,...}            (flat form)
//      | {"type":"ack",transactionId,partitionId,status,consumerGroup?,leaseId?}
//    ],
//    "requiredLeases":["<leaseId>",...]}
// Response mirrors the v1 transaction wire shape:
//   ok:    {transactionId, success:true, results:[per-op]}
//   fail:  {transactionId, success:false, error, results:[]}  (HTTP 200; the
//          SP RAISEs on rollback, surfacing as a DB error here).
struct TxnPushEcho {
    index: usize,
    txn: String,
    mid: String,
    queue: String,
    duplicate: bool,
}
struct TxnAckEcho {
    index: usize,
    txn: String,
}
struct TxnFrame {
    mid: [u8; 16],
    txn: String,
    trace: Option<[u8; 16]>,
    payload: Vec<u8>,
}
struct TxnPushGroup {
    queue: String,
    partition: String,
    frames: Vec<TxnFrame>,
    // txn -> first message_id (intra-batch first-wins dedup, matching the C++
    // broker: a repeated txn in one (queue,partition) group would otherwise
    // trip seg_push_segment_v1's per-segment dedup and raise QDUP).
    seen: HashMap<String, String>,
}
struct TxnAckGroup {
    partition_id: String,
    group: String,
    worker: String,
    items: Vec<(String, bool)>,
}

fn txn_add_push(
    item: &serde_json::Value,
    index: usize,
    groups: &mut Vec<TxnPushGroup>,
    group_of: &mut HashMap<(String, String), usize>,
    echoes: &mut Vec<TxnPushEcho>,
) {
    let queue = item.get("queue").and_then(|x| x.as_str()).unwrap_or("").to_string();
    let partition = item
        .get("partition")
        .and_then(|x| x.as_str())
        .unwrap_or("Default")
        .to_string();
    let payload = item
        .get("payload")
        .cloned()
        .or_else(|| item.get("data").cloned())
        .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
    let txn_opt = item
        .get("transactionId")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let trace = item
        .get("traceId")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
        .and_then(uuid_string_to_bytes);

    let mid = uuidv7_bytes();
    let mid_str = uuid_bytes_to_string(&mid);
    let txn = txn_opt.unwrap_or_else(|| mid_str.clone());

    let key = (queue.clone(), partition.clone());
    let gi = *group_of.entry(key).or_insert_with(|| {
        groups.push(TxnPushGroup {
            queue: queue.clone(),
            partition: partition.clone(),
            frames: Vec::new(),
            seen: HashMap::new(),
        });
        groups.len() - 1
    });
    let grp = &mut groups[gi];

    if let Some(first_mid) = grp.seen.get(&txn) {
        echoes.push(TxnPushEcho {
            index,
            txn,
            mid: first_mid.clone(),
            queue,
            duplicate: true,
        });
        return;
    }
    grp.seen.insert(txn.clone(), mid_str.clone());
    echoes.push(TxnPushEcho {
        index,
        txn: txn.clone(),
        mid: mid_str,
        queue,
        duplicate: false,
    });
    grp.frames.push(TxnFrame {
        mid,
        txn,
        trace,
        payload: serde_json::to_vec(&payload).unwrap_or_default(),
    });
}

fn txn_fail_body(txn_id: &str, err: &str, status: StatusCode) -> Response {
    let out = serde_json::json!({
        "transactionId": txn_id,
        "success": false,
        "error": err,
        "results": [],
    });
    json(status, out.to_string())
}

pub async fn handle_transaction(
    State(st): State<Arc<AppState>>,
    Extension(authed): Extension<crate::auth::AuthedSub>,
    body: Bytes,
) -> Response {
    let root: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    let txn_id = uuid_bytes_to_string(&uuidv7_bytes());
    // Authenticated producer identity (JWT sub), stamped onto every pushed frame
    // when auth is enabled. None when auth is disabled or the token had no sub.
    let producer_sub = authed.0.filter(|s| !s.is_empty());

    let operations = match root.get("operations").and_then(|o| o.as_array()) {
        Some(o) => o,
        None => {
            return txn_fail_body(&txn_id, "transaction requires an operations array", StatusCode::BAD_REQUEST)
        }
    };

    // Combined lease hints: top-level requiredLeases (where the JS/Go builders
    // put the leaseId) + any per-op ack leaseId (raw HTTP callers).
    let mut lease_hints: Vec<String> = Vec::new();
    if let Some(rl) = root.get("requiredLeases").and_then(|x| x.as_array()) {
        for l in rl {
            if let Some(s) = l.as_str() {
                if !s.is_empty() {
                    lease_hints.push(s.to_string());
                }
            }
        }
    }

    let mut flat = 0usize;
    let mut echoes: Vec<TxnPushEcho> = Vec::new();
    let mut ack_echoes: Vec<TxnAckEcho> = Vec::new();
    let mut groups: Vec<TxnPushGroup> = Vec::new();
    let mut group_of: HashMap<(String, String), usize> = HashMap::new();
    let mut ack_groups: Vec<TxnAckGroup> = Vec::new();
    let mut ack_group_of: HashMap<(String, String), usize> = HashMap::new();
    let mut any_unknown = false;

    for op in operations {
        let ty = op.get("type").and_then(|x| x.as_str()).unwrap_or("");
        match ty {
            "push" => {
                if let Some(items) = op.get("items").and_then(|x| x.as_array()) {
                    for item in items {
                        txn_add_push(item, flat, &mut groups, &mut group_of, &mut echoes);
                        flat += 1;
                    }
                } else {
                    txn_add_push(op, flat, &mut groups, &mut group_of, &mut echoes);
                    flat += 1;
                }
            }
            "ack" => {
                let txn = op.get("transactionId").and_then(|x| x.as_str()).unwrap_or("").to_string();
                let partition_id =
                    op.get("partitionId").and_then(|x| x.as_str()).unwrap_or("").to_string();
                let group = op
                    .get("consumerGroup")
                    .and_then(|x| x.as_str())
                    .filter(|s| !s.is_empty())
                    .unwrap_or("__QUEUE_MODE__")
                    .to_string();
                let status = op.get("status").and_then(|x| x.as_str());
                let ok = status_is_ok(status);
                let lease = op
                    .get("leaseId")
                    .and_then(|x| x.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string());
                if let Some(l) = &lease {
                    lease_hints.push(l.clone());
                }

                ack_echoes.push(TxnAckEcho { index: flat, txn: txn.clone() });
                let key = (partition_id.clone(), group.clone());
                let gi = *ack_group_of.entry(key).or_insert_with(|| {
                    ack_groups.push(TxnAckGroup {
                        partition_id: partition_id.clone(),
                        group: group.clone(),
                        worker: String::new(),
                        items: Vec::new(),
                    });
                    ack_groups.len() - 1
                });
                let ag = &mut ack_groups[gi];
                if ag.worker.is_empty() {
                    if let Some(l) = lease {
                        ag.worker = l;
                    }
                }
                ag.items.push((txn, ok));
                flat += 1;
            }
            _ => {
                any_unknown = true;
                flat += 1;
            }
        }
    }

    if any_unknown {
        return txn_fail_body(
            &txn_id,
            "segments transaction supports only push and ack operations",
            StatusCode::BAD_REQUEST,
        );
    }

    // Fallback worker resolution: ack groups with no per-op leaseId inherit the
    // single unambiguous lease hint (the common case — one pop batch, one lease
    // in requiredLeases). Ambiguous hints leave the worker empty and the SP
    // rejects (invalid lease), rolling the transaction back.
    let unique_hint: Option<String> = {
        let mut only: Option<&String> = None;
        let mut ambiguous = false;
        for h in &lease_hints {
            match only {
                None => only = Some(h),
                Some(o) => {
                    if o != h {
                        ambiguous = true;
                        break;
                    }
                }
            }
        }
        if ambiguous {
            None
        } else {
            only.cloned()
        }
    };
    // Acquire the DB client up front: ack worker resolution and the bogus-ack
    // pre-check below both need it, and it is reused for the SP call.
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    // Resolve each ack group's worker/lease. The JS/Go builders carry the leaseId
    // only in the top-level requiredLeases (never per ack op), so a transaction
    // that acks messages leased from more than one partition cannot be resolved
    // from the request alone — requiredLeases has no (lease -> partition) mapping,
    // and the single-hint fallback goes ambiguous the moment two leases appear
    // (the transactionWithPartitions / transactionMultipleQueues failures). The
    // authoritative source is queen.partition_consumers: exactly one live lease exists
    // per (partition, group), so read worker_id straight from it. Precedence:
    //   1. a per-op leaseId (raw HTTP callers), already set during parse;
    //   2. the current partition_consumers.worker_id for (partition, group);
    //   3. the single unambiguous requiredLeases hint (last resort).
    for ag in &mut ack_groups {
        if !ag.worker.is_empty() || ag.partition_id.is_empty() {
            continue;
        }
        if let Ok(Some(r)) = client
            .query_opt(
                "SELECT worker_id FROM queen.partition_consumers \
                 WHERE partition_id = $1::text::uuid AND consumer_group = $2",
                &[&ag.partition_id, &ag.group],
            )
            .await
        {
            if let Some(w) = r.get::<_, Option<String>>(0) {
                ag.worker = w;
            }
        }
        if ag.worker.is_empty() {
            if let Some(u) = &unique_hint {
                ag.worker = u.clone();
            }
        }
    }

    // Bogus-ack pre-check (atomic rollback). seg_ack_by_txn_v1 resolves acked
    // txns through queen.seg_dedup and SILENTLY IGNORES any txn with no dedup
    // entry ("without a surviving dedup entry => not acked"). So a transaction
    // that acks a non-existent transactionId — the transactionRollback test acks
    // {transactionId:'non-existent-id'} alongside a real ack on the same
    // partition — would otherwise have its pushes committed and report
    // success:true, because the merged ack call still returns ok. The SP cannot
    // surface that within one call, so reject it HERE, before running the SP: if
    // any acked txn has no surviving dedup row for its partition, we return a
    // v1-shaped failure and never touch the DB, so the pushes roll back too.
    for ag in &ack_groups {
        if ag.partition_id.is_empty() || ag.items.is_empty() {
            continue;
        }
        let txns: Vec<&str> = ag.items.iter().map(|(t, _)| t.as_str()).collect();
        match client
            .query_opt(
                "SELECT 1 FROM unnest($2::text[]) AS a(txn) \
                 WHERE NOT EXISTS ( \
                   SELECT 1 FROM queen.seg_dedup d \
                   WHERE d.partition_id = $1::text::uuid \
                     AND d.txn_hash = hashtextextended(a.txn, 0)) \
                 LIMIT 1",
                &[&ag.partition_id, &txns],
            )
            .await
        {
            Ok(Some(_)) => {
                return txn_fail_body(
                    &txn_id,
                    "QTXN ack references unknown transactionId; transaction rolled back",
                    StatusCode::OK,
                )
            }
            Err(e) => return txn_fail_body(&txn_id, &e.to_string(), StatusCode::OK),
            Ok(None) => {}
        }
    }

    // ------------------------------------------------ build the SP payload
    let mut pushes_json: Vec<serde_json::Value> = Vec::new();
    for g in &groups {
        if g.frames.is_empty() {
            continue;
        }
        let fins: Vec<FrameIn> = g
            .frames
            .iter()
            .map(|f| FrameIn {
                message_id: f.mid,
                txn: &f.txn,
                trace_id: f.trace,
                producer_sub: producer_sub.as_deref(),
                payload: &f.payload,
                encrypted: false,
            })
            .collect();
        let metas: Vec<serde_json::Value> = g
            .frames
            .iter()
            .enumerate()
            .map(|(k, f)| {
                serde_json::json!({"i": k, "mid": uuid_bytes_to_string(&f.mid), "txn": f.txn})
            })
            .collect();
        let blob = zstd_compress(&pack_frames(&fins), st.zstd_level);
        let blob_b64 = base64::engine::general_purpose::STANDARD.encode(&blob);
        pushes_json.push(serde_json::json!({
            "queue": g.queue,
            "partition": g.partition,
            "metas": metas,
            "blobB64": blob_b64,
            "count": g.frames.len(),
        }));
    }

    let acks_json: Vec<serde_json::Value> = ack_groups
        .iter()
        .map(|ag| {
            let txns: Vec<serde_json::Value> = ag
                .items
                .iter()
                .map(|(t, ok)| serde_json::json!({"txn": t, "ok": ok}))
                .collect();
            serde_json::json!({
                "partitionId": ag.partition_id,
                "group": ag.group,
                "worker": ag.worker,
                "txns": txns,
            })
        })
        .collect();

    let payload = serde_json::json!({"pushes": pushes_json, "acks": acks_json}).to_string();

    match db::transaction(&client, &payload).await {
        Ok(txt) => {
            let v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
            if v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false) {
                let mut results: Vec<serde_json::Value> = vec![serde_json::Value::Null; flat];
                for e in &echoes {
                    let mut obj = serde_json::json!({
                        "index": e.index,
                        "type": "push",
                        "success": true,
                        "transactionId": e.txn,
                        "messageId": e.mid,
                        "queueName": e.queue,
                    });
                    if e.duplicate {
                        obj["duplicate"] = serde_json::Value::Bool(true);
                    }
                    if e.index < results.len() {
                        results[e.index] = obj;
                    }
                }
                for a in &ack_echoes {
                    if a.index < results.len() {
                        results[a.index] = serde_json::json!({
                            "index": a.index,
                            "type": "ack",
                            "success": true,
                            "transactionId": a.txn,
                            "error": serde_json::Value::Null,
                            "dlq": false,
                        });
                    }
                }
                let out = serde_json::json!({
                    "transactionId": txn_id,
                    "success": true,
                    "results": results,
                });
                json(StatusCode::OK, out.to_string())
            } else {
                let err = v
                    .get("error")
                    .and_then(|x| x.as_str())
                    .map(str::to_string)
                    .unwrap_or_else(|| "transaction failed".to_string());
                txn_fail_body(&txn_id, &err, StatusCode::OK)
            }
        }
        // The SP RAISEs on rollback (duplicate push / rejected ack): surface the
        // DB message (e.g. "QDUP ...", "QTXN ...") as a v1-shaped failure,
        // HTTP 200 (matches the C++ broker).
        Err(e) => {
            let msg = e
                .as_db_error()
                .map(|d| d.message().to_string())
                .unwrap_or_else(|| e.to_string());
            txn_fail_body(&txn_id, &msg, StatusCode::OK)
        }
    }
}

