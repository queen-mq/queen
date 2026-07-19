use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
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

pub struct AppState {
    pub pool: Pool,
    pub fusion: Arc<Fusion>,
    pub push_vegas: Arc<Vegas>,
    pub pop_vegas: Arc<Vegas>,
    pub metrics: Arc<Metrics>,
    pub stmt_timeout: Duration,
    pub pop_default_timeout_ms: u64,
    pub pop_wait_poll_ms: u64,
    // zstd level for broker-packed segments on the transaction push path (the
    // fusion path carries its own copy).
    pub zstd_level: i32,
    // Per-queue configured lease time (seconds), read from queen.seg_queues on
    // first use. No invalidation for now (queue-config invalidation is a later
    // slice); a reconfigure of leaseTime is not reflected until restart.
    pub lease_cache: Mutex<HashMap<String, i32>>,
    // System maintenance flags, mirrored to queen.system_state (the SAME
    // {"enabled":..} rows the C++ SharedStateManager uses). `maintenance` is
    // reported only — the segments broker has no file buffer, so it does not
    // divert pushes and bufferedMessages is always 0. `pop_maintenance` pauses
    // pops (handle_pop / handle_pop_partition early-return {messages:[],paused:true}).
    pub maintenance: AtomicBool,
    pub pop_maintenance: AtomicBool,
}

const DEFAULT_LEASE_SECONDS: i32 = 300;

impl AppState {
    // Resolve the queue's lease time, caching the lookup. Falls back to
    // DEFAULT_LEASE_SECONDS when the queue has no seg_queues row yet or the DB
    // is unreachable. The std Mutex guard is always dropped before the .await.
    async fn lease_time_for(&self, queue: &str) -> i32 {
        if let Some(v) = self.lease_cache.lock().unwrap().get(queue).copied() {
            return v;
        }
        let v = match self.pool.get().await {
            Ok(c) => db::queue_lease_time(&c, queue)
                .await
                .ok()
                .flatten()
                .unwrap_or(DEFAULT_LEASE_SECONDS),
            Err(_) => DEFAULT_LEASE_SECONDS,
        };
        self.lease_cache.lock().unwrap().insert(queue.to_string(), v);
        v
    }
}

fn json(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

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

pub async fn handle_push(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let parsed: PushBody = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    let n = parsed.items.len();
    if n == 0 {
        return json(StatusCode::CREATED, "[]".to_string());
    }
    // Frames staged per (queue, partition) group, minus their owning request
    // (attached once the shared PushState exists). item = index into `results`.
    struct PreFrame {
        mid: [u8; 16],
        txn: String,
        payload: Vec<u8>,
        item: usize,
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
    let pending = groups.len();
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
                state: state.clone(),
                item: p.item,
            })
            .collect();
        st.fusion.submit(AddMsg { queue, partition, frames });
    }
    let _ = rx.await;
    st.metrics.push.record_request(n);

    let mut r = state.results.lock().unwrap();
    // Resolve layer-1 followers: adopt the leader's FINAL message_id (a leader
    // that turned out to be a cross-flush duplicate now carries the pre-existing
    // id). A follower stays "duplicate" unless the leader failed outright.
    for i in 0..r.len() {
        if let Some(l) = r[i].dup_of {
            let leader_mid = r[l].message_id.clone();
            let leader_status = r[l].status;
            r[i].message_id = leader_mid;
            if leader_status == "error" {
                r[i].status = "error";
            }
        }
    }
    let mut out = String::with_capacity(n * 96);
    out.push('[');
    for (i, item) in r.iter().enumerate() {
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
    json(StatusCode::CREATED, out)
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
            tokio::time::sleep(Duration::from_millis(st.pop_wait_poll_ms)).await;
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
            tokio::time::sleep(Duration::from_millis(st.pop_wait_poll_ms)).await;
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

fn status_is_ok(s: Option<&str>) -> bool {
    // The JS/Go/etc clients send "completed" for success and "failed" for a nack.
    // Absent status defaults to success (a bare completion).
    match s {
        Some(v) => matches!(v, "completed" | "success" | "acked" | "ok"),
        None => true,
    }
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
// Renews every seg_consumers lease held by :leaseId (= the worker id minted at
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
            format!("{{\"error\":\"renew failed: {}\"}}", e).replace('"', "'"),
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

pub async fn handle_transaction(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let root: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    let txn_id = uuid_bytes_to_string(&uuidv7_bytes());

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
    // authoritative source is queen.seg_consumers: exactly one live lease exists
    // per (partition, group), so read worker_id straight from it. Precedence:
    //   1. a per-op leaseId (raw HTTP callers), already set during parse;
    //   2. the current seg_consumers.worker_id for (partition, group);
    //   3. the single unambiguous requiredLeases hint (last resort).
    for ag in &mut ack_groups {
        if !ag.worker.is_empty() || ag.partition_id.is_empty() {
            continue;
        }
        if let Ok(Some(r)) = client
            .query_opt(
                "SELECT worker_id FROM queen.seg_consumers \
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
                producer_sub: None,
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
                format!("{{\"error\":\"configure failed: {}\"}}", e).replace('"', "'"),
            )
        }
    };

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
            format!("{{\"error\":\"configure(storage) failed: {}\"}}", e).replace('"', "'"),
        );
    }
    if let Err(e) =
        db::upsert_seg_queue(&client, &queue, lease_time, retention_seconds, dedup_window).await
    {
        return json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{{\"error\":\"configure(seg_queue) failed: {}\"}}", e).replace('"', "'"),
        );
    }

    // Invalidate the cached lease so a leaseTime change is reflected on next pop.
    st.lease_cache.lock().unwrap().remove(&queue);

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
                format!("{{\"error\":\"delete failed: {}\"}}", e).replace('"', "'"),
            )
        }
    };
    // Best-effort segment-data drop (independent of the rows-side delete).
    if let Err(e) = db::delete_seg_queue(&client, &queue).await {
        return json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{{\"error\":\"delete(seg) failed: {}\"}}", e).replace('"', "'"),
        );
    }

    st.lease_cache.lock().unwrap().remove(&queue);
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
                format!("{{\"error\":\"get failed: {}\"}}", e).replace('"', "'"),
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

pub async fn handle_status() -> Response {
    json(StatusCode::OK, "{\"status\":\"ok\",\"engine\":\"segments-rust\"}".to_string())
}

pub async fn handle_metrics(State(st): State<Arc<AppState>>) -> Response {
    let mut body = st.metrics.prometheus();
    body.push_str(&format!(
        "queen_seg_push_vegas_limit {}\nqueen_seg_pop_vegas_limit {}\n",
        st.push_vegas.limit(),
        st.pop_vegas.limit()
    ));
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

// ============================================================ management surface
// Read/observe endpoints for the segments broker (messages/dlq/traces/status/
// health/prometheus). These ADD to the data hot-path handlers above; they never
// touch push/pop/ack/transaction/configure.

fn text_plain(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response()
}

// Collect the given query keys into a JSON filter object, keeping only
// non-empty string values.
fn filters_from_query(
    params: &HashMap<String, String>,
    keys: &[&str],
) -> serde_json::Map<String, serde_json::Value> {
    let mut m = serde_json::Map::new();
    for &k in keys {
        if let Some(v) = params.get(k) {
            if !v.is_empty() {
                m.insert(k.to_string(), serde_json::Value::String(v.clone()));
            }
        }
    }
    m
}

fn qint(params: &HashMap<String, String>, key: &str, def: i32) -> i32 {
    params.get(key).and_then(|v| v.parse::<i32>().ok()).unwrap_or(def)
}

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
                    format!("{{\"error\":\"resolve failed: {}\"}}", e).replace('"', "'"),
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
                format!("{{\"error\":\"segment fetch failed: {}\"}}", e).replace('"', "'"),
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
                format!("{{\"error\":\"list failed: {}\"}}", e).replace('"', "'"),
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
                format!("{{\"error\":\"dlq failed: {}\"}}", e).replace('"', "'"),
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

// ------------------------------------------------------------ POST /api/v1/traces
// Engine-agnostic (queen.message_traces). Mirrors the C++ traces route: require
// transactionId/partitionId/data, default consumerGroup/eventType, pass through
// traceNames. record_trace_v1 is segment-aware via 030 (resolves message_id from
// seg_dedup, records with NULL message_id past the dedup window).
pub async fn handle_record_trace(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let body_v: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };

    let txn = body_v.get("transactionId").and_then(|x| x.as_str()).filter(|s| !s.is_empty());
    let pid = body_v.get("partitionId").and_then(|x| x.as_str()).filter(|s| !s.is_empty());
    let (txn, pid) = match (txn, pid) {
        (Some(t), Some(p)) => (t, p),
        _ => {
            return json(
                StatusCode::BAD_REQUEST,
                "{\"error\":\"transactionId and partitionId are required\"}".to_string(),
            )
        }
    };
    let data = match body_v.get("data") {
        Some(d) => d.clone(),
        None => return json(StatusCode::BAD_REQUEST, "{\"error\":\"data is required\"}".to_string()),
    };

    let mut sp = serde_json::Map::new();
    sp.insert("transactionId".to_string(), serde_json::Value::String(txn.to_string()));
    sp.insert("partitionId".to_string(), serde_json::Value::String(pid.to_string()));
    sp.insert(
        "consumerGroup".to_string(),
        serde_json::Value::String(
            body_v.get("consumerGroup").and_then(|x| x.as_str()).unwrap_or("__QUEUE_MODE__").to_string(),
        ),
    );
    sp.insert(
        "eventType".to_string(),
        serde_json::Value::String(
            body_v.get("eventType").and_then(|x| x.as_str()).unwrap_or("info").to_string(),
        ),
    );
    sp.insert("data".to_string(), data);
    sp.insert("workerId".to_string(), serde_json::Value::String("seg-rust".to_string()));
    if let Some(tn) = body_v.get("traceNames") {
        if tn.is_array() {
            sp.insert("traceNames".to_string(), tn.clone());
        } else if let Some(s) = tn.as_str() {
            sp.insert("traceNames".to_string(), serde_json::json!([s]));
        }
    }
    let sp_json = serde_json::Value::Object(sp).to_string();

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::record_trace(&client, &sp_json).await {
        Ok(txt) => {
            let v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
            let ok = v.get("success").and_then(|x| x.as_bool()).unwrap_or(true)
                && v.get("error").map(|e| e.is_null()).unwrap_or(true);
            json(if ok { StatusCode::CREATED } else { StatusCode::INTERNAL_SERVER_ERROR }, txt)
        }
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{{\"error\":\"trace failed: {}\"}}", e).replace('"', "'"),
        ),
    }
}

// -------------------------------- GET /api/v1/traces/:partitionId/:transactionId
pub async fn handle_message_traces(
    State(st): State<Arc<AppState>>,
    Path((partition_id, transaction_id)): Path<(String, String)>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_message_traces(&client, &partition_id, &transaction_id).await {
        Ok(txt) => json(StatusCode::OK, txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{{\"error\":\"traces failed: {}\"}}", e).replace('"', "'"),
        ),
    }
}

// ------------------------------------------ GET /api/v1/traces/by-name/:traceName
pub async fn handle_traces_by_name(
    State(st): State<Arc<AppState>>,
    Path(trace_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let limit = qint(&params, "limit", 100);
    let offset = qint(&params, "offset", 0);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_traces_by_name(&client, &trace_name, limit, offset).await {
        Ok(txt) => json(StatusCode::OK, txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{{\"error\":\"traces failed: {}\"}}", e).replace('"', "'"),
        ),
    }
}

// ------------------------------------------------------- GET /api/v1/traces/names
pub async fn handle_trace_names(
    State(st): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let limit = qint(&params, "limit", 50);
    let offset = qint(&params, "offset", 0);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_trace_names(&client, limit, offset).await {
        Ok(txt) => json(StatusCode::OK, txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{{\"error\":\"trace names failed: {}\"}}", e).replace('"', "'"),
        ),
    }
}

// ------------------------------------------------------------ GET /api/v1/status
pub async fn handle_api_status(
    State(st): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let filters = filters_from_query(&params, &["from", "to", "queue", "namespace", "task"]);
    let filters_json = serde_json::Value::Object(filters).to_string();
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_status(&client, &filters_json).await {
        Ok(txt) => json(StatusCode::OK, txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{{\"error\":\"status failed: {}\"}}", e).replace('"', "'"),
        ),
    }
}

// ----------------------------------------------------- GET /api/v1/status/queues
pub async fn handle_status_queues(
    State(st): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let filters = filters_from_query(&params, &["from", "to", "namespace", "task"]);
    let filters_json = serde_json::Value::Object(filters).to_string();
    let limit = qint(&params, "limit", 100);
    let offset = qint(&params, "offset", 0);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_status_queues(&client, &filters_json, limit, offset).await {
        Ok(txt) => json(StatusCode::OK, txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{{\"error\":\"status queues failed: {}\"}}", e).replace('"', "'"),
        ),
    }
}

// -------------------------------------------------------------------- GET /health
// 200 when a trivial DB round-trip succeeds, 503 otherwise.
pub async fn handle_health(State(st): State<Arc<AppState>>) -> Response {
    let healthy = match st.pool.get().await {
        Ok(c) => db::ping(&c).await.is_ok(),
        Err(_) => false,
    };
    if healthy {
        json(
            StatusCode::OK,
            "{\"status\":\"ok\",\"database\":\"connected\",\"engine\":\"segments-rust\"}".to_string(),
        )
    } else {
        json(
            StatusCode::SERVICE_UNAVAILABLE,
            "{\"status\":\"unhealthy\",\"database\":\"disconnected\",\"engine\":\"segments-rust\"}".to_string(),
        )
    }
}

// Escape a Prometheus label value (backslash, double-quote, newline).
fn prom_label_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

// Append a subset of the get_prometheus_metrics_v1 JSON blob as exposition lines.
fn format_db_prometheus(txt: &str, out: &mut String) {
    let v: serde_json::Value = match serde_json::from_str(txt) {
        Ok(v) => v,
        Err(_) => return,
    };
    // DB-persisted lifetime totals (queen.worker_metrics_summary). Emitted under
    // a distinct queen_db_* prefix so they do NOT collide with the in-process
    // queen_cluster_* live counters this handler already emits (the rust broker
    // does not write worker_metrics_summary, so these read 0 today).
    if let Some(t) = v.get("system_totals").and_then(|x| x.as_object()) {
        let map = [
            ("pushRequests", "queen_db_push_requests_total"),
            ("popRequests", "queen_db_pop_requests_total"),
            ("ackRequests", "queen_db_ack_requests_total"),
            ("transactions", "queen_db_transactions_total"),
            ("pushMessages", "queen_db_push_messages_total"),
            ("popMessages", "queen_db_pop_messages_total"),
            ("ackMessages", "queen_db_ack_messages_total"),
            ("ackSuccess", "queen_db_ack_success_total"),
            ("ackFailed", "queen_db_ack_failed_total"),
            ("dbErrors", "queen_db_errors_total"),
            ("dlqCount", "queen_db_dlq_total"),
        ];
        for (k, metric) in map {
            if let Some(n) = t.get(k).and_then(|x| x.as_i64()) {
                out.push_str(metric);
                out.push_str("{scope=\"cluster\"} ");
                out.push_str(&n.to_string());
                out.push('\n');
            }
        }
    }
    // DLQ depth (cluster total + per-queue).
    if let Some(d) = v.get("dlq") {
        if let Some(n) = d.get("total").and_then(|x| x.as_i64()) {
            out.push_str("queen_dlq_depth{scope=\"cluster\"} ");
            out.push_str(&n.to_string());
            out.push('\n');
        }
        if let Some(arr) = d.get("per_queue").and_then(|x| x.as_array()) {
            for e in arr {
                let q = e.get("queue").and_then(|x| x.as_str()).unwrap_or("");
                let c = e.get("count").and_then(|x| x.as_i64()).unwrap_or(0);
                out.push_str(&format!(
                    "queen_dlq_depth_by_queue{{queue=\"{}\"}} {}\n",
                    prom_label_escape(q),
                    c
                ));
            }
        }
    }
    // Queue depth (per-queue message counts, both engines).
    if let Some(arr) = v.get("queue_depth").and_then(|x| x.as_array()) {
        for e in arr {
            let q = e.get("queue").and_then(|x| x.as_str()).unwrap_or("");
            let storage = e.get("storage").and_then(|x| x.as_str()).unwrap_or("");
            let total = e.get("total_messages").and_then(|x| x.as_i64()).unwrap_or(0);
            let pending = e.get("pending_messages").and_then(|x| x.as_i64()).unwrap_or(0);
            let ql = prom_label_escape(q);
            let sl = prom_label_escape(storage);
            out.push_str(&format!(
                "queen_queue_depth_total{{queue=\"{}\",storage=\"{}\"}} {}\n",
                ql, sl, total
            ));
            out.push_str(&format!(
                "queen_queue_depth_pending{{queue=\"{}\",storage=\"{}\"}} {}\n",
                ql, sl, pending
            ));
        }
    }
}

// -------------------------------------------------------- GET /metrics/prometheus
// In-process gauges (same as /metrics) + a subset of the DB-derived metrics from
// get_prometheus_metrics_v1. DB block is best-effort — the in-process gauges are
// always emitted even if the DB read fails.
pub async fn handle_prometheus(State(st): State<Arc<AppState>>) -> Response {
    let mut body = st.metrics.prometheus();
    body.push_str(&format!(
        "queen_seg_push_vegas_limit {}\nqueen_seg_pop_vegas_limit {}\n",
        st.push_vegas.limit(),
        st.pop_vegas.limit()
    ));
    if let Ok(c) = st.pool.get().await {
        if let Ok(txt) = db::get_prometheus_metrics(&c).await {
            format_db_prometheus(&txt, &mut body);
        }
    }
    text_plain(StatusCode::OK, body)
}

// ========================================================= consumer groups
// Management surface for consumer groups on the segments engine. list/lagging/
// details are read-only over queen.get_consumer_groups_v4 (dual-engine, 027) and
// the 008 lag/detail readers; delete/subscription/seek mutate the segment cursor
// state (queen.seg_consumers) plus the shared coordination tables. These ADD to
// the handlers above; they never touch push/pop/ack/transaction/configure.

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
            format!("{{\"error\":\"consumer groups failed: {}\"}}", e).replace('"', "'"),
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
            format!("{{\"error\":\"lagging failed: {}\"}}", e).replace('"', "'"),
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
            format!("{{\"error\":\"details failed: {}\"}}", e).replace('"', "'"),
        ),
    }
}

fn qbool(params: &HashMap<String, String>, key: &str, def: bool) -> bool {
    match params.get(key).map(|s| s.as_str()) {
        Some("false" | "0" | "no") => false,
        Some("true" | "1" | "yes") => true,
        _ => def,
    }
}

// DELETE /api/v1/consumer-groups/:group?deleteMetadata= — drop the group. Removes
// its segment cursors (seg_consumers, all partitions) + seg_consumer_watermarks
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
                format!("{{\"error\":\"delete(seg) failed: {}\"}}", e).replace('"', "'"),
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
            format!("{{\"error\":\"subscription failed: {}\"}}", e).replace('"', "'"),
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
            format!("{{\"error\":\"seek failed: {}\"}}", e).replace('"', "'"),
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
            format!("{{\"error\":\"seek failed: {}\"}}", e).replace('"', "'"),
        ),
    }
}

// POST /api/v1/stats/refresh — force the stats reconciler (no-op for segments,
// wired for parity). ADMIN operation.
pub async fn handle_stats_refresh(State(st): State<Arc<AppState>>) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::refresh_all_stats(&client).await {
        Ok(txt) => json(StatusCode::OK, txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{{\"error\":\"refresh failed: {}\"}}", e).replace('"', "'"),
        ),
    }
}

// ============================================================ system maintenance
// Parity with the C++ maintenance routes (server/src/routes/maintenance.cpp),
// backed by an in-process AtomicBool + queen.system_state (keys 'maintenance_mode'
// / 'pop_maintenance_mode', value {"enabled":bool} — the SAME rows the C++
// SharedStateManager reads/writes, so a mixed deployment stays consistent). The
// in-process flag is the source of truth for hot-path checks; the DB write is a
// best-effort mirror for restart/cluster propagation.
//
// The segments broker has no file buffer, so `maintenanceMode` is REPORTED
// (bufferedMessages always 0) but does not divert pushes — messages keep flowing
// to the DB, which is what maintenance.js needs (all produced messages eventually
// received). `popMaintenanceMode` pauses pops (see handle_pop).
#[derive(Deserialize)]
struct MaintenanceBody {
    enabled: Option<bool>,
}

// GET /api/v1/system/maintenance — current flags + buffer status (buffer is a
// no-op for the segments engine, so it always reports empty + healthy).
pub async fn handle_get_maintenance(State(st): State<Arc<AppState>>) -> Response {
    let out = serde_json::json!({
        "maintenanceMode": st.maintenance.load(Ordering::Relaxed),
        "popMaintenanceMode": st.pop_maintenance.load(Ordering::Relaxed),
        "bufferedMessages": 0,
        "bufferHealthy": true,
        "bufferStats": {},
    });
    json(StatusCode::OK, out.to_string())
}

// POST /api/v1/system/maintenance {enabled:bool} — toggle push maintenance.
pub async fn handle_set_maintenance(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let b: MaintenanceBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    let enabled = match b.enabled {
        Some(v) => v,
        None => {
            return json(
                StatusCode::BAD_REQUEST,
                "{\"error\":\"enabled (boolean) is required\"}".to_string(),
            )
        }
    };
    st.maintenance.store(enabled, Ordering::Relaxed);
    if let Ok(c) = st.pool.get().await {
        let _ = db::set_system_flag(&c, "maintenance_mode", enabled).await;
    }
    let out = serde_json::json!({
        "maintenanceMode": enabled,
        "bufferedMessages": 0,
        "bufferHealthy": true,
        "message": if enabled {
            "Maintenance mode ENABLED."
        } else {
            "Maintenance mode DISABLED."
        },
    });
    json(StatusCode::OK, out.to_string())
}

// GET /api/v1/system/maintenance/pop — pop maintenance status.
pub async fn handle_get_pop_maintenance(State(st): State<Arc<AppState>>) -> Response {
    let pop = st.pop_maintenance.load(Ordering::Relaxed);
    let out = serde_json::json!({
        "popMaintenanceMode": pop,
        "message": if pop {
            "Pop maintenance mode is ON. All POP operations return empty arrays."
        } else {
            "Pop maintenance mode is OFF. Normal operation."
        },
    });
    json(StatusCode::OK, out.to_string())
}

// POST /api/v1/system/maintenance/pop {enabled:bool} — toggle pop maintenance.
pub async fn handle_set_pop_maintenance(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let b: MaintenanceBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    let enabled = match b.enabled {
        Some(v) => v,
        None => {
            return json(
                StatusCode::BAD_REQUEST,
                "{\"error\":\"enabled (boolean) is required\"}".to_string(),
            )
        }
    };
    st.pop_maintenance.store(enabled, Ordering::Relaxed);
    if let Ok(c) = st.pool.get().await {
        let _ = db::set_system_flag(&c, "pop_maintenance_mode", enabled).await;
    }
    let out = serde_json::json!({
        "popMaintenanceMode": enabled,
        "message": if enabled {
            "Pop maintenance mode ENABLED. All POP operations will return empty arrays."
        } else {
            "Pop maintenance mode DISABLED. Normal operation resumed."
        },
    });
    json(StatusCode::OK, out.to_string())
}

// GET /api/v1/system/shared-state — UDPSYNC cache stats. This broker has no
// cluster gossip transport, so report a single-node summary carrying the live
// flags (parity shape with the C++ get_stats()).
pub async fn handle_shared_state(State(st): State<Arc<AppState>>) -> Response {
    let out = serde_json::json!({
        "enabled": false,
        "reason": "single_node_segments_broker",
        "maintenance_mode": st.maintenance.load(Ordering::Relaxed),
        "pop_maintenance_mode": st.pop_maintenance.load(Ordering::Relaxed),
    });
    json(StatusCode::OK, out.to_string())
}

// ================================================================= streams
// Three handlers for the fat-JS-client stream engine (client-v2/streams). The
// broker only serves these 3 endpoints + the normal pop path; all window/
// watermark/gate/operator logic runs client-side. Each streaming SP takes a
// JSONB ARRAY of requests ([{idx,..}]) and returns [{idx, result}]; we wrap the
// single client body in a one-element array (idx:0) and unwrap [0].result before
// returning — the SDK reads the inner result object directly (res.success /
// res.query_id / res.rows / res.push_results ...). This mirrors the C++ streams
// routes (server/src/routes/streams/*.cpp).

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
            format!("{{\"error\":\"register failed: {}\"}}", e).replace('"', "'"),
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
        Ok(txt) => json(StatusCode::OK, unwrap_stream_result(&txt).to_string()),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{{\"error\":\"state get failed: {}\"}}", e).replace('"', "'"),
        ),
    }
}

// POST /streams/v1/cycle — atomic streaming cycle commit on the segments engine
// (queen.seg_streams_cycle_v1, 029). This is the packing handler: it converts the
// SDK's high-level push_items into broker-prepacked `sink_segments` (metas +
// base64 zstd blob, exactly like handle_transaction packs `pushes`) and maps the
// SDK ack {transactionId, leaseId, status, count} to the SP's ack {ok, count}
// plus the top-level `worker` (= the source leaseId) and `release_lease`.
//
// SDK body (cycle.js commitCycle):
//   {query_id, partition_id, consumer_group, state_ops:[...], push_items:[...],
//    ack:{transactionId, leaseId, status, count}|null, release_lease}
// Each push_item (SinkOperator.buildPushItems): {queue, partition, payload}.
//
// SP element (029):
//   {idx:0, query_id, partition_id, consumer_group, worker, release_lease,
//    state_ops, sink_segments:[{queue, partition, metas:[{i,mid,txn}], blobB64,
//    count}], ack:{ok, count}|null}
//
// Returns the inner result {success, query_id, partition_id, queueName,
// state_ops_applied, push_results, ack_result}. Always HTTP 200 on a completed
// SP call (the SDK inspects success/error to decide whether to retry), matching
// the C++ cycle route.
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
            });
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
                encrypted: false,
            })
            .collect();
        let metas: Vec<serde_json::Value> = frames
            .iter()
            .enumerate()
            .map(|(k, f)| serde_json::json!({"i": k, "mid": uuid_bytes_to_string(&f.mid), "txn": f.txn}))
            .collect();
        let blob = zstd_compress(&pack_frames(&fins), st.zstd_level);
        let blob_b64 = base64::engine::general_purpose::STANDARD.encode(&blob);
        sink_segments.push(serde_json::json!({
            "queue": queue,
            "partition": partition,
            "metas": metas,
            "blobB64": blob_b64,
            "count": frames.len(),
        }));
    }

    // ---- map the SDK ack -> SP ack {ok,count} + top-level worker (= leaseId) ----
    let (worker, ack_val) = match root.get("ack") {
        Some(a) if !a.is_null() => {
            let ok = status_is_ok(a.get("status").and_then(|x| x.as_str()));
            let count = a.get("count").and_then(|x| x.as_i64()).unwrap_or(0);
            let worker = a.get("leaseId").and_then(|x| x.as_str()).unwrap_or("").to_string();
            (worker, serde_json::json!({"ok": ok, "count": count}))
        }
        // idle-flush cycle: no source ack, skip the lease block SP-side.
        _ => (String::new(), serde_json::Value::Null),
    };

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
        Ok(txt) => json(StatusCode::OK, unwrap_stream_result(&txt).to_string()),
        // The SP internalizes per-element failures as success:false; an Err here
        // is an infra/protocol error. 500 lets the SDK's HTTP client retry (the
        // whole SP call is one transaction, so a retry is safe).
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{{\"error\":\"cycle failed: {}\"}}", e).replace('"', "'"),
        ),
    }
}
