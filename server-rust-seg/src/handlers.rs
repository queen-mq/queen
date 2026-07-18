use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
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
use crate::frames::{unpack_frames, uuid_bytes_to_string, zstd_decompress};
use crate::fusion::{json_escape_into, AddMsg, Contributor, Fusion, ItemResult, OwnedFrame, PushState};
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
    let mut results: Vec<ItemResult> = Vec::with_capacity(n);
    let mut groups: HashMap<(String, String), (Vec<OwnedFrame>, Vec<usize>)> = HashMap::new();
    for (i, it) in parsed.items.iter().enumerate() {
        let mid = uuidv7_bytes();
        let mid_str = uuid_bytes_to_string(&mid);
        let txn = it
            .transaction_id
            .map(|s| s.to_string())
            .unwrap_or_else(|| mid_str.clone());
        let queue = it.queue.to_string();
        let partition = it.partition.unwrap_or("Default").to_string();
        results.push(ItemResult {
            message_id: mid_str,
            txn: txn.clone(),
            queue: queue.clone(),
            status: "queued",
        });
        let g = groups.entry((queue, partition)).or_default();
        g.0.push(OwnedFrame {
            message_id: mid,
            txn,
            payload: it.payload.get().as_bytes().to_vec(),
        });
        g.1.push(i);
    }
    let pending = groups.len();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let state = Arc::new(PushState {
        results: Mutex::new(results),
        pending: AtomicUsize::new(pending),
        done: Mutex::new(Some(tx)),
    });
    for ((queue, partition), (frames, idxs)) in groups {
        st.fusion.submit(AddMsg {
            queue,
            partition,
            frames,
            contrib: Contributor {
                state: state.clone(),
                item_indices: idxs,
            },
        });
    }
    let _ = rx.await;
    st.metrics.push.record_request(n);

    let r = state.results.lock().unwrap();
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

pub async fn handle_pop(
    State(st): State<Arc<AppState>>,
    Path(queue): Path<String>,
    Query(p): Query<PopParams>,
) -> Response {
    let batch = p.batch.unwrap_or(200);
    let max_parts = p.partitions.unwrap_or(1);
    let auto_ack = p.auto_ack.unwrap_or(false);
    let wait = p.wait.unwrap_or(false);
    let timeout_ms = p.timeout.unwrap_or(st.pop_default_timeout_ms);
    let group = p.consumer_group.unwrap_or_else(|| "__QUEUE_MODE__".to_string());
    let worker = uuid_bytes_to_string(&uuidv7_bytes());
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);

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
            db::pop_wildcard(&client, &queue, &group, batch, 60, &worker, auto_ack, max_parts),
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

        let (body, count) = build_pop_response(&txt, &queue, &group);
        if count == 0 && wait && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(st.pop_wait_poll_ms)).await;
            continue;
        }
        st.metrics.pop.record_request(count);
        st.metrics.pop.record_batch(count, true, rtt);
        return json(if count == 0 { StatusCode::NO_CONTENT } else { StatusCode::OK }, body);
    }
}

fn build_pop_response(txt: &str, queue: &str, group: &str) -> (String, usize) {
    let parsed: PopResult = match serde_json::from_str(txt) {
        Ok(p) => p,
        Err(_) => return ("{\"success\":false,\"error\":\"parse\",\"messages\":[]}".to_string(), 0),
    };
    if let Some(e) = parsed.error {
        let mut out = String::from("{\"success\":false,\"error\":\"");
        json_escape_into(&mut out, &e);
        out.push_str("\",\"messages\":[]}");
        return (out, 0);
    }
    let mut msgs = String::new();
    let mut count = 0usize;
    let mut first_name = String::new();
    let mut first_id = String::new();
    let mut first_set = false;
    for part in &parsed.partitions {
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
                msgs.push_str("\",\"leaseId\":\"\",\"consumerGroup\":\"");
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
    out.push_str("\",\"leaseId\":\"\",\"consumerGroup\":\"");
    json_escape_into(&mut out, group);
    out.push_str("\",\"messages\":[");
    out.push_str(&msgs);
    out.push_str("],\"partitionsClaimed\":");
    out.push_str(&parsed.partitions.len().to_string());
    out.push('}');
    (out, count)
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
