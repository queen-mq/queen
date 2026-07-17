use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::Response;
use deadpool_postgres::Pool;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::auth::Auth;
use crate::crypto::Crypto;
use crate::engine::{Engine, LANE_ACK, LANE_POP, LANE_PUSH};
use crate::metrics::Metrics;
use crate::notify::Notifier;
use crate::util::{count_occurrences, extract_result_object, uuidv7};

pub struct PopCfg {
    pub initial_ms: u64,
    pub threshold: u32,
    pub multiplier: u64,
    pub max_ms: u64,
    pub default_timeout_ms: u64,
    pub stmt_timeout: Duration,
}

pub struct AppState {
    pub engine: Engine,
    pub metrics: Arc<Metrics>,
    pub notifier: Notifier,
    pub pool: Pool,
    pub pop: PopCfg,
    pub auth: Option<Auth>,
    pub crypto: Option<Crypto>,
    pub features: std::sync::Arc<crate::features::Features>,
}

fn json(status: StatusCode, body: Vec<u8>) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(body))
        .unwrap()
}

fn err(status: StatusCode, msg: &str) -> Response {
    let b = format!("{{\"error\":{}}}", serde_json::to_string(msg).unwrap());
    json(status, b.into_bytes())
}

fn array_response(status: StatusCode, elems: &[Vec<u8>]) -> Response {
    let mut buf = Vec::with_capacity(64);
    buf.push(b'[');
    for (i, e) in elems.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        buf.extend_from_slice(e);
    }
    buf.push(b']');
    json(status, buf)
}

fn append_json_str(buf: &mut Vec<u8>, s: &str) {
    // serde_json produces a correctly-escaped quoted string.
    let q = serde_json::to_string(s).unwrap();
    buf.extend_from_slice(q.as_bytes());
}

// ---- push ----
#[derive(Deserialize)]
struct PushBody<'a> {
    #[serde(borrow, default)]
    items: Vec<PushItem<'a>>,
}
#[derive(Deserialize)]
struct PushItem<'a> {
    #[serde(default)]
    queue: String,
    #[serde(default)]
    partition: String,
    #[serde(borrow, default)]
    payload: Option<&'a RawValue>,
    #[serde(default, rename = "transactionId")]
    transaction_id: String,
    #[serde(default, rename = "traceId")]
    trace_id: String,
}

pub async fn handle_push(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    if let Some(a) = &st.auth {
        if !a.check() {
            return err(StatusCode::UNAUTHORIZED, "unauthorized");
        }
    }
    let pb: PushBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return err(StatusCode::BAD_REQUEST, &format!("invalid JSON: {e}")),
    };
    if pb.items.is_empty() {
        return json(StatusCode::CREATED, b"[]".to_vec());
    }
    let mut items: Vec<Vec<u8>> = Vec::with_capacity(pb.items.len());
    let mut part_set: HashMap<String, ()> = HashMap::new();
    let mut queues: Vec<String> = Vec::new();
    for it in &pb.items {
        let partition = if it.partition.is_empty() { "Default" } else { &it.partition };
        part_set.insert(format!("{}\u{1f}{}", it.queue, partition), ());
        if !queues.contains(&it.queue) {
            queues.push(it.queue.clone());
        }
        let mut buf = Vec::with_capacity(128);
        buf.extend_from_slice(b"{\"queue\":");
        append_json_str(&mut buf, &it.queue);
        buf.extend_from_slice(b",\"partition\":");
        append_json_str(&mut buf, partition);
        buf.extend_from_slice(b",\"payload\":");
        let mut encrypted = false;
        if let Some(c) = &st.crypto {
            let pt: &[u8] = it.payload.map(|p| p.get().as_bytes()).unwrap_or(b"{}");
            buf.extend_from_slice(&c.encrypt_payload(pt));
            encrypted = true;
        } else {
            match it.payload {
                Some(p) => buf.extend_from_slice(p.get().as_bytes()),
                None => buf.extend_from_slice(b"{}"),
            }
        }
        if encrypted {
            buf.extend_from_slice(b",\"is_encrypted\":true,\"messageId\":");
        } else {
            buf.extend_from_slice(b",\"is_encrypted\":false,\"messageId\":");
        }
        append_json_str(&mut buf, &uuidv7());
        if !it.transaction_id.is_empty() {
            buf.extend_from_slice(b",\"transactionId\":");
            append_json_str(&mut buf, &it.transaction_id);
        }
        if !it.trace_id.is_empty() {
            buf.extend_from_slice(b",\"traceId\":");
            append_json_str(&mut buf, &it.trace_id);
        }
        buf.push(b'}');
        items.push(buf);
    }
    let parts: Vec<String> = part_set.into_keys().collect();
    let n = items.len();
    match st.engine.submit(LANE_PUSH, items, parts).await {
        Ok(elems) => {
            st.metrics.push.record_request(n);
            if st.features.enabled {
                for q in &queues {
                    st.features.ensure_queue_config(q).await;
                }
                st.features.attribute_push(&elems);
            }
            for q in queues {
                st.notifier.notify(&q);
            }
            array_response(StatusCode::CREATED, &elems)
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

// ---- pop ----
pub async fn handle_pop_wildcard(
    State(st): State<Arc<AppState>>,
    Path(queue): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    do_pop(st, queue, String::new(), q).await
}

pub async fn handle_pop_partition(
    State(st): State<Arc<AppState>>,
    Path((queue, partition)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    do_pop(st, queue, partition, q).await
}

async fn do_pop(st: Arc<AppState>, queue: String, partition: String, q: HashMap<String, String>) -> Response {
    if let Some(a) = &st.auth {
        if !a.check() {
            return err(StatusCode::UNAUTHORIZED, "unauthorized");
        }
    }
    let cg = q.get("consumerGroup").filter(|s| !s.is_empty()).cloned().unwrap_or_else(|| "__QUEUE_MODE__".into());
    let batch: i64 = q.get("batch").and_then(|s| s.parse().ok()).unwrap_or(1);
    let mut max_partitions: i64 = q.get("partitions").and_then(|s| s.parse().ok()).unwrap_or(1);
    if max_partitions < 1 {
        max_partitions = 1;
    }
    let auto_ack = q.get("autoAck").map(|s| s == "true").unwrap_or(false);
    let sub_mode = q.get("subscriptionMode").cloned().unwrap_or_default();
    let sub_from = q.get("subscriptionFrom").cloned().unwrap_or_default();
    let wait = q.get("wait").map(|s| s == "true").unwrap_or(false);
    let timeout_ms: u64 = q.get("timeout").and_then(|s| s.parse().ok()).unwrap_or(st.pop.default_timeout_ms);

    let build_req = || -> Vec<u8> {
        let mut b = Vec::with_capacity(256);
        b.extend_from_slice(b"{\"queue_name\":");
        append_json_str(&mut b, &queue);
        b.extend_from_slice(b",\"partition_name\":");
        append_json_str(&mut b, &partition);
        b.extend_from_slice(b",\"consumer_group\":");
        append_json_str(&mut b, &cg);
        b.extend_from_slice(format!(",\"batch_size\":{}", batch).as_bytes());
        b.extend_from_slice(b",\"lease_seconds\":0,\"worker_id\":");
        append_json_str(&mut b, &uuidv7());
        b.extend_from_slice(b",\"sub_mode\":");
        append_json_str(&mut b, &sub_mode);
        b.extend_from_slice(b",\"sub_from\":");
        append_json_str(&mut b, &sub_from);
        b.extend_from_slice(format!(",\"auto_ack\":{},\"max_partitions\":{}}}", auto_ack, max_partitions).as_bytes());
        b
    };

    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let mut interval = st.pop.initial_ms;
    let mut empties: u32 = 0;

    loop {
        // Register wake interest BEFORE the attempt (lost-wakeup safe).
        let notify = st.notifier.get(&queue);
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let elems = match st.engine.submit(LANE_POP, vec![build_req()], Vec::new()).await {
            Ok(e) => e,
            Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e),
        };
        let result: Option<Vec<u8>> = elems
            .first()
            .and_then(|e| extract_result_object(e).map(|r| r.to_vec()));
        let n = result.as_ref().map(|r| count_occurrences(r, b"\"transactionId\"")).unwrap_or(0);

        if n > 0 || !wait {
            st.metrics.pop.record_request(n);
            return match result {
                Some(r) if n > 0 => {
                    let out = match &st.crypto {
                        Some(c) => decrypt_pop_result(c, r),
                        None => r,
                    };
                    if st.features.enabled {
                        st.features.attribute_pop(&queue, &out);
                    }
                    json(StatusCode::OK, out)
                }
                _ => json(StatusCode::NO_CONTENT, b"{\"messages\":[]}".to_vec()),
            };
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            st.metrics.pop.record_request(0);
            return json(StatusCode::NO_CONTENT, b"{\"messages\":[]}".to_vec());
        }
        let mut sleep = Duration::from_millis(interval);
        if sleep > remaining {
            sleep = remaining;
        }
        tokio::select! {
            _ = notified.as_mut() => { interval = st.pop.initial_ms; empties = 0; }
            _ = tokio::time::sleep(sleep) => {
                empties += 1;
                if empties >= st.pop.threshold {
                    interval = (interval * st.pop.multiplier).min(st.pop.max_ms);
                }
            }
        }
    }
}

// ---- ack ----
fn decrypt_pop_result(crypto: &Crypto, result: Vec<u8>) -> Vec<u8> {
    let mut v: serde_json::Value = match serde_json::from_slice(&result) {
        Ok(x) => x,
        Err(_) => return result,
    };
    if let Some(msgs) = v.get_mut("messages").and_then(|m| m.as_array_mut()) {
        for m in msgs.iter_mut() {
            let dec = m.get("data").and_then(|d| {
                let e = d.get("encrypted")?.as_str()?;
                let i = d.get("iv")?.as_str()?;
                let t = d.get("authTag")?.as_str()?;
                crypto.decrypt(e, i, t)
            });
            if let Some(pt) = dec {
                if let Ok(dv) = serde_json::from_slice::<serde_json::Value>(&pt) {
                    m["data"] = dv;
                }
            }
        }
    }
    serde_json::to_vec(&v).unwrap_or(result)
}

pub async fn handle_ack(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    if let Some(a) = &st.auth {
        if !a.check() {
            return err(StatusCode::UNAUTHORIZED, "unauthorized");
        }
    }
    let m: HashMap<String, serde_json::Value> = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return err(StatusCode::BAD_REQUEST, &format!("invalid JSON: {e}")),
    };
    let cg = m.get("consumerGroup").and_then(|v| v.as_str()).filter(|s| !s.is_empty()).unwrap_or("__QUEUE_MODE__");
    let obj = build_ack_object(&m, cg);
    match st.engine.submit(LANE_ACK, vec![obj], Vec::new()).await {
        Ok(elems) => {
            st.metrics.ack.record_request(1);
            if let Some(e) = elems.first() {
                json(StatusCode::OK, e.clone())
            } else {
                json(StatusCode::OK, b"{\"success\":false}".to_vec())
            }
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

pub async fn handle_ack_batch(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    if let Some(a) = &st.auth {
        if !a.check() {
            return err(StatusCode::UNAUTHORIZED, "unauthorized");
        }
    }
    #[derive(Deserialize)]
    struct AB {
        #[serde(default)]
        acknowledgments: Vec<HashMap<String, serde_json::Value>>,
        #[serde(rename = "consumerGroup", default)]
        consumer_group: String,
    }
    let ab: AB = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return err(StatusCode::BAD_REQUEST, &format!("invalid JSON: {e}")),
    };
    if ab.acknowledgments.is_empty() {
        return err(StatusCode::BAD_REQUEST, "acknowledgments array is required");
    }
    let cg = if ab.consumer_group.is_empty() { "__QUEUE_MODE__" } else { &ab.consumer_group };
    let items: Vec<Vec<u8>> = ab.acknowledgments.iter().map(|a| build_ack_object(a, cg)).collect();
    let n = items.len();
    match st.engine.submit(LANE_ACK, items, Vec::new()).await {
        Ok(elems) => {
            st.metrics.ack.record_request(n);
            array_response(StatusCode::OK, &elems)
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

fn build_ack_object(a: &HashMap<String, serde_json::Value>, cg: &str) -> Vec<u8> {
    let mut b = Vec::with_capacity(128);
    b.push(b'{');
    let mut first = true;
    for k in ["transactionId", "partitionId", "leaseId", "status", "error"] {
        if let Some(v) = a.get(k) {
            if !first {
                b.push(b',');
            }
            first = false;
            append_json_str(&mut b, k);
            b.push(b':');
            b.extend_from_slice(serde_json::to_string(v).unwrap().as_bytes());
        }
    }
    if !first {
        b.push(b',');
    }
    append_json_str(&mut b, "consumerGroup");
    b.push(b':');
    append_json_str(&mut b, cg);
    b.push(b'}');
    b
}

// ---- configure / status / metrics ----
pub async fn handle_configure(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let m: HashMap<String, serde_json::Value> = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return err(StatusCode::BAD_REQUEST, &format!("invalid JSON: {e}")),
    };
    let queue = match m.get("queue").and_then(|v| v.as_str()) {
        Some(q) => q.to_string(),
        None => return err(StatusCode::BAD_REQUEST, "queue is required"),
    };
    let mut opts = m.get("options").cloned().unwrap_or_else(|| serde_json::json!({}));
    if let Some(o) = opts.as_object_mut() {
        if let Some(ns) = m.get("namespace") {
            o.insert("namespace".into(), ns.clone());
        }
        if let Some(t) = m.get("task") {
            o.insert("task".into(), t.clone());
        }
    }
    let opts_str = opts.to_string();
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    let stmt = match client.prepare_cached("SELECT queen.configure_queue_v1($1, $2::text::jsonb)::text").await {
        Ok(s) => s,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    match tokio::time::timeout(st.pop.stmt_timeout, client.query_one(&stmt, &[&queue, &opts_str])).await {
        Ok(Ok(row)) => {
            let v: String = row.get(0);
            json(StatusCode::OK, v.into_bytes())
        }
        Ok(Err(e)) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        Err(_) => err(StatusCode::INTERNAL_SERVER_ERROR, "configure timeout"),
    }
}

pub async fn handle_status() -> Response {
    json(StatusCode::OK, b"{\"status\":\"ok\",\"engine\":\"rust-hotpath-spike\"}".to_vec())
}

pub async fn handle_metrics(State(st): State<Arc<AppState>>) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")
        .body(axum::body::Body::from(st.metrics.prometheus()))
        .unwrap()
}
