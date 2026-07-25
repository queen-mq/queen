//! The data-plane pipeline. OWNER: Agent A.
//!
//! Pipeline per request (spec §4/§14 — the order is load-bearing):
//!   1. resolve ClusterCtx from Host (miss -> 421)
//!   2. cluster status gate (Suspended -> 403 suspended; PushBlocked + Produce -> 403 push_blocked)
//!   3. authenticate (auth::authenticate) + authorize (auth::authorize vs classify)
//!   4. limits: check_req; Produce -> buffer body (cap = min(plan, cfg)), count
//!      items + per-item payload caps, registry.admit each (queue,partition),
//!      check_msgs(n); Consume wait=true -> parked_slot RAII guard held across
//!      the upstream await
//!   5. forward: rebuild URI on ctx.cell_base_url, strip hop-by-hop headers,
//!      inject Authorization (ctx.cell_token), X-Queen-Tenant (cfg.send_tenant_header),
//!      X-Queen-Request-Id; long-poll timeout = min(client timeout|30s, cfg max) + margin
//!   6. meter post-response (M1–M6): push -> parse per-item statuses (exclude
//!      error, dedupe duplicate, buffered counts), pop -> delivered count +
//!      debit_deliveries, bytes in/out always
//!   7. shadow mode: when !limits.enforcing(), Deny decisions are logged (target
//!      `limits`, field `would_block`) but the request proceeds
//!
//! Steps 4 and 6 are implemented here. Size caps (per-item payload, batch items,
//! and the body-buffer ceiling) are HARD limits — always enforced, since we must
//! bound how much we buffer; only the rate/quota decisions (check_req, check_msgs,
//! registry admit, parked_slot) honour shadow mode. See the crate report for the
//! semantics flagged for Alice/Agent-D (body-total-vs-per-item cap; 5xx billing).

use std::collections::HashSet;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::response::Parts;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode, Uri};
use axum::response::Response;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::errors;
use crate::limits::Decision;
use crate::meter::Sample;
use crate::registry::Admit;
use crate::routes::{classify, is_wait_pop, poll_timeout_ms, RouteClass};
use crate::state::{ClusterCtx, ClusterStatus, OpClass, St};

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "proxy-connection",
    "keep-alive",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// Safety ceiling on buffered *response* bodies we parse for metering (push 201,
/// pop 200). Matches the broker's default `QUEEN_MAX_BODY_BYTES` (64 MiB): a pop
/// batch of large messages must still relay, so this is deliberately NOT clamped
/// by `cfg.max_body_bytes` (which caps *request* bodies, default 16 MiB). See the
/// report: CONTRACTS §gateway::handle says `min(cfg.max_body_bytes, 64MiB)` —
/// flagged as a conflict with STEP 6's "limite 64MiB safety".
const RESP_BUFFER_CAP: usize = 64 * 1024 * 1024;

pub async fn handle(State(st): State<St>, req: Request) -> Response {
    // ----- 1. resolve cluster from Host -----
    let host = req
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let Some(ctx) = st.cache.resolve_host(&host).await else {
        return errors::err_421("no cluster for this host");
    };

    // ----- 2. cluster status gate -----
    match ctx.status {
        ClusterStatus::Suspended | ClusterStatus::Deleting => {
            return errors::err_403(errors::CODE_SUSPENDED, "cluster suspended");
        }
        _ => {}
    }

    // ----- 3. classify + feature gate + authn/authz -----
    let class = classify(req.method(), req.uri().path());
    if class == RouteClass::Blocked {
        return errors::err_404(errors::CODE_ROUTE_BLOCKED, "not available");
    }
    if let RouteClass::Gated(f) = class {
        let on = match f {
            crate::routes::Feature::Streams => ctx.features.streams,
            crate::routes::Feature::Traces => ctx.features.traces,
        };
        if !on {
            return errors::err_403(errors::CODE_FEATURE_GATED, "not in your plan");
        }
    }
    if (ctx.status == ClusterStatus::PushBlocked || st.limits.is_push_blocked(ctx.cluster_id))
        && class == RouteClass::Produce
    {
        return errors::err_403(
            errors::CODE_PUSH_BLOCKED,
            "pushes blocked (storage quota or billing)",
        );
    }

    let principal = match crate::auth::authenticate(&st, req.headers(), ctx.cluster_id).await {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    if let Err(resp) = crate::auth::authorize(&principal, class) {
        return resp;
    }

    // One request id threads the whole pipeline (logs + upstream + response).
    let rid = crate::obs::request_id();

    // Capture what we need from the request line before decomposing it.
    let path_only = req.uri().path().to_string();
    let query = req.uri().query().map(|s| s.to_string());
    let path_q = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    let wait_pop = is_wait_pop(&path_only, query.as_deref());
    let timeout_ms = if wait_pop {
        let want = poll_timeout_ms(query.as_deref()).unwrap_or(30_000);
        want.min(st.cfg.longpoll_max_ms) + st.cfg.longpoll_margin_ms
    } else {
        st.cfg.upstream_request_timeout_ms
    };

    // ----- 4a. per-request bucket (every proxied request, post-authn) -----
    match st.limits.check_req(&ctx) {
        Decision::Allow => {}
        Decision::Deny { retry_after_s, code } => {
            if st.limits.enforcing() {
                return errors::err_429(code, retry_after_s, "request rate limit exceeded");
            }
            // shadow deny: canonical would_block log emitted inside check_req
        }
    }

    let (mut parts, body) = req.into_parts();

    // ----- 4b. Produce: buffer body, count items, per-item + batch caps,
    //           registry admission, msg bucket. Bytes forwarded verbatim. -----
    let mut produce_n: u64 = 0;
    let mut bytes_in: u64 = 0;
    let forward_body: Body = if class == RouteClass::Produce {
        // Body-total cap is the instance cap only; the per-item plan cap is
        // enforced per item inside enforce_produce (a batch of many small
        // items must not be 413'd by the single-item ceiling).
        let per_item_cap = ctx.limits.max_payload_bytes.map(|p| p.max(0) as usize);
        let body_cap = st.cfg.max_body_bytes;
        let buffered = match axum::body::to_bytes(body, body_cap).await {
            Ok(b) => b,
            Err(_) => return errors::err_413("request body exceeds cap"),
        };
        bytes_in = buffered.len() as u64;
        match enforce_produce(&st, &ctx, &path_only, &buffered, per_item_cap, &rid).await {
            Ok(n) => produce_n = n,
            Err(resp) => return resp,
        }
        Body::from(buffered)
    } else {
        body
    };

    // ----- 4c. Consume long-poll: parked-slot RAII guard, held across the
    //           upstream await (drops on cancel/disconnect by itself). -----
    let parked_guard = if wait_pop {
        match st.limits.parked_slot(&ctx) {
            Ok(g) => Some(g),
            // parked_slot only returns Err when enforcing: a shadow over-cap
            // returns Ok(guard) so the gauges keep tracking real parked pops
            // (see limits.rs) — dropping to None here would undercount.
            Err(Decision::Deny { retry_after_s, code }) => {
                return errors::err_429(code, retry_after_s, "too many parked consumers");
            }
            Err(Decision::Allow) => None,
        }
    } else {
        None
    };

    // ----- 5. forward -----
    let op = op_for(&path_only, class);
    let target: Uri = match format!("{}{}", ctx.cell_base_url.trim_end_matches('/'), path_q).parse()
    {
        Ok(u) => u,
        Err(_) => return errors::err_502("bad upstream uri"),
    };
    for h in HOP_BY_HOP {
        parts.headers.remove(*h);
    }
    parts.headers.remove(header::HOST);
    parts.headers.remove(header::AUTHORIZATION);
    if let Some(tok) = &ctx.cell_token {
        if let Ok(v) = HeaderValue::from_str(&format!("Bearer {tok}")) {
            parts.headers.insert(header::AUTHORIZATION, v);
        }
    }
    if st.cfg.send_tenant_header {
        if let Ok(v) = HeaderValue::from_str(&ctx.broker_tenant.to_string()) {
            parts.headers.insert(crate::config::TENANT_HEADER, v);
        }
    }
    if let Ok(v) = HeaderValue::from_str(&rid) {
        parts.headers.insert(crate::config::REQUEST_ID_HEADER, v);
    }
    parts.uri = target;

    let upstream_req = Request::from_parts(parts, forward_body);
    let fut = st.upstream.request(upstream_req);
    let resp = match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), fut).await {
        Err(_) => {
            // Our own 504: record the request but never bill it (M5).
            st.meter.record(Sample {
                cluster_id: ctx.cluster_id,
                op,
                reqs: 1,
                msgs: 0,
                bytes_in,
                bytes_out: 0,
            });
            tracing::warn!(target: "meter", cluster = %ctx.slug, rid, "upstream timeout (not billed)");
            return errors::err_504("upstream timeout");
        }
        Ok(Err(e)) => {
            st.meter.record(Sample {
                cluster_id: ctx.cluster_id,
                op,
                reqs: 1,
                msgs: 0,
                bytes_in,
                bytes_out: 0,
            });
            tracing::warn!(cluster = %ctx.slug, rid, error = %e, "upstream error (not billed)");
            return errors::err_502("upstream unreachable");
        }
        Ok(Ok(r)) => r,
    };
    // No longer parked once the upstream has responded.
    drop(parked_guard);

    // ----- 6. metering (M1–M6) -----
    let status = resp.status();
    let resp_cl = content_length(resp.headers());

    // M5: upstream 5xx — record reqs=1 msgs=0, log separately, never bill. Stream
    // the body straight through (no buffering).
    if status.is_server_error() {
        st.meter.record(Sample {
            cluster_id: ctx.cluster_id,
            op,
            reqs: 1,
            msgs: 0,
            bytes_in,
            bytes_out: resp_cl,
        });
        tracing::warn!(
            target: "meter", cluster = %ctx.slug, status = status.as_u16(),
            op = op.as_str(), rid, "upstream 5xx (not billed)"
        );
        let (rparts, rbody) = resp.into_parts();
        return finalize(rparts, Body::new(rbody), &rid);
    }

    let is_pop = is_pop_path(&path_only);

    // Push 201: buffer response, charge accepted per-item statuses.
    if op == OpClass::Push && status == StatusCode::CREATED {
        let (rparts, rbody) = resp.into_parts();
        let buffered = match axum::body::to_bytes(Body::new(rbody), RESP_BUFFER_CAP).await {
            Ok(b) => b,
            Err(_) => {
                tracing::warn!(target: "meter", cluster = %ctx.slug, rid, "push response too large to buffer");
                return errors::err_502("push response too large");
            }
        };
        let msgs = count_push_statuses(&buffered).unwrap_or_else(|| {
            tracing::warn!(target: "meter", cluster = %ctx.slug, rid, "push response parse failed; msgs=0");
            0
        });
        st.meter.record(Sample {
            cluster_id: ctx.cluster_id,
            op: OpClass::Push,
            reqs: 1,
            msgs,
            bytes_in,
            bytes_out: buffered.len() as u64,
        });
        return finalize(rparts, Body::from(buffered), &rid);
    }

    // Transaction 2xx: charge the ingress-counted push ops (msgs = n). The broker
    // returns 200 even on rollback (success:false); see report — flagged.
    if op == OpClass::Txn {
        let msgs = if status.is_success() { produce_n } else { 0 };
        st.meter.record(Sample {
            cluster_id: ctx.cluster_id,
            op: OpClass::Txn,
            reqs: 1,
            msgs,
            bytes_in,
            bytes_out: resp_cl,
        });
        let (rparts, rbody) = resp.into_parts();
        return finalize(rparts, Body::new(rbody), &rid);
    }

    // Pop: 200 with body -> count deliveries + debit; 204 -> reqs only, no buffer.
    if is_pop {
        if status == StatusCode::OK {
            let (rparts, rbody) = resp.into_parts();
            let buffered = match axum::body::to_bytes(Body::new(rbody), RESP_BUFFER_CAP).await {
                Ok(b) => b,
                Err(_) => {
                    tracing::warn!(target: "meter", cluster = %ctx.slug, rid, "pop response too large to buffer");
                    return errors::err_502("pop response too large");
                }
            };
            let delivered = count_pop_messages(&buffered).unwrap_or(0);
            st.limits.debit_deliveries(&ctx, delivered);
            st.meter.record(Sample {
                cluster_id: ctx.cluster_id,
                op: OpClass::Delivery,
                reqs: 1,
                msgs: delivered,
                bytes_in: 0,
                bytes_out: buffered.len() as u64,
            });
            return finalize(rparts, Body::from(buffered), &rid);
        }
        // 204 (empty / paused) or any other non-200: nothing to parse.
        st.meter.record(Sample {
            cluster_id: ctx.cluster_id,
            op: OpClass::Delivery,
            reqs: 1,
            msgs: 0,
            bytes_in: 0,
            bytes_out: resp_cl,
        });
        let (rparts, rbody) = resp.into_parts();
        return finalize(rparts, Body::new(rbody), &rid);
    }

    // Configure / queue-admin, reads, acks/leases, gated: reqs-only, bytes_out
    // from Content-Length, NEVER buffered (streaming straight through).
    st.meter.record(Sample {
        cluster_id: ctx.cluster_id,
        op,
        reqs: 1,
        msgs: 0,
        bytes_in,
        bytes_out: resp_cl,
    });
    let (rparts, rbody) = resp.into_parts();
    finalize(rparts, Body::new(rbody), &rid)
}

/// STEP 4 for Produce routes: count push items, enforce per-item + batch size
/// caps (hard), registry admission and the message bucket (shadow-aware).
/// Returns the counted push-item count `n`, or a ready error Response.
async fn enforce_produce(
    st: &St,
    ctx: &ClusterCtx,
    path: &str,
    bytes: &Bytes,
    per_item_cap: Option<usize>,
    rid: &str,
) -> Result<u64, Response> {
    let items = match parse_produce_items(path, bytes) {
        Ok(v) => v,
        Err(()) => {
            // Malformed body: forward verbatim, let the broker return its own 400.
            // We do not half-enforce an unparseable batch (spec).
            tracing::warn!(
                target: "limits", cluster = %ctx.slug, rid,
                "produce body unparseable; forwarding without msg enforcement"
            );
            return Ok(0);
        }
    };
    let n = items.len() as u64;

    // Per-item payload cap — hard size limit, always enforced.
    if let Some(cap) = per_item_cap {
        for (i, it) in items.iter().enumerate() {
            if it.payload_len > cap {
                return Err(errors::err_413(&format!(
                    "item {i}: payload {} bytes exceeds max_payload_bytes ({cap})",
                    it.payload_len
                )));
            }
        }
    }

    // Batch item cap — hard size limit, always enforced.
    let max_batch = ctx
        .limits
        .max_batch_items
        .map(|v| v.max(0) as u64)
        .unwrap_or(st.cfg.default_max_batch_items);
    if n > max_batch {
        return Err(errors::err_413(&format!(
            "batch of {n} items exceeds max_batch_items ({max_batch})"
        )));
    }

    // Registry admission for each DISTINCT (queue, partition) in the batch.
    let mut seen: HashSet<(&str, &str)> = HashSet::new();
    for it in &items {
        if !seen.insert((it.queue.as_str(), it.partition.as_str())) {
            continue;
        }
        match st.registry.admit(ctx, &it.queue, &it.partition).await {
            Admit::Allowed => {}
            Admit::OverQueues { max } => {
                if st.limits.enforcing() {
                    return Err(errors::err_403(
                        errors::CODE_QUOTA_EXCEEDED,
                        &format!("queue limit reached ({max})"),
                    ));
                }
                tracing::warn!(
                    target: "limits", would_block = "queues", cluster = %ctx.slug,
                    max, queue = %it.queue, rid, "shadow deny"
                );
            }
            Admit::OverPartitions { max } => {
                if st.limits.enforcing() {
                    return Err(errors::err_403(
                        errors::CODE_QUOTA_EXCEEDED,
                        &format!("partition limit reached ({max})"),
                    ));
                }
                tracing::warn!(
                    target: "limits", would_block = "partitions", cluster = %ctx.slug,
                    max, queue = %it.queue, partition = %it.partition, rid, "shadow deny"
                );
            }
        }
    }

    // Message bucket — shadow-aware, same shape as check_req.
    match st.limits.check_msgs(ctx, n) {
        Decision::Allow => {}
        Decision::Deny { retry_after_s, code } => {
            if st.limits.enforcing() {
                return Err(errors::err_429(code, retry_after_s, "message rate limit exceeded"));
            }
            // shadow deny: canonical would_block log emitted inside check_msgs
        }
    }

    Ok(n)
}

/// A single push item, resolved to owned data (no borrow of the request bytes
/// escapes the parse — this is what STEP 4 iterates over).
struct ItemInfo {
    queue: String,
    partition: String,
    /// Byte length of the payload's raw JSON text (`RawValue::get().len()`).
    payload_len: usize,
}

const DEFAULT_PARTITION: &str = "Default";

// Minimal borrow-parses of the produce bodies. serde ignores unknown fields by
// default, so transactionId / traceId / requiredLeases etc. are skipped for free.
#[derive(Deserialize)]
struct PushLite<'a> {
    // required: a body with no `items` array is not a valid push -> parse fails
    // -> forward verbatim and let the broker return its own 400.
    #[serde(borrow)]
    items: Vec<PushItem<'a>>,
}
#[derive(Deserialize)]
struct PushItem<'a> {
    queue: String,
    #[serde(default)]
    partition: Option<String>,
    #[serde(borrow)]
    payload: &'a RawValue,
}
#[derive(Deserialize)]
struct TxnLite<'a> {
    // required, same rationale as PushLite::items.
    #[serde(borrow)]
    operations: Vec<OpLite<'a>>,
}
#[derive(Deserialize)]
struct OpLite<'a> {
    #[serde(default, rename = "type")]
    ty: Option<String>,
    #[serde(default)]
    queue: Option<String>,
    #[serde(default)]
    partition: Option<String>,
    #[serde(borrow, default)]
    payload: Option<&'a RawValue>,
    #[serde(borrow, default)]
    items: Option<Vec<PushItem<'a>>>,
}

/// Parse a produce body into its flattened push items. `/api/v1/push` uses the
/// `{items:[...]}` shape (queue+payload required — a malformed item fails the
/// whole parse, so we forward without enforcement). `/api/v1/transaction` uses
/// `{operations:[...]}` — push ops (flat `{queue,payload}` OR nested `{items:[]}`)
/// are counted; ack ops (no queue/payload, or `type:"ack"`) are skipped, since
/// pid ownership is checked broker-side.
fn parse_produce_items(path: &str, bytes: &[u8]) -> Result<Vec<ItemInfo>, ()> {
    if path == "/api/v1/push" {
        let parsed: PushLite = serde_json::from_slice(bytes).map_err(|_| ())?;
        let mut out = Vec::with_capacity(parsed.items.len());
        for it in parsed.items {
            out.push(ItemInfo {
                queue: it.queue,
                partition: it.partition.unwrap_or_else(|| DEFAULT_PARTITION.to_string()),
                payload_len: it.payload.get().len(),
            });
        }
        Ok(out)
    } else {
        // transaction
        let parsed: TxnLite = serde_json::from_slice(bytes).map_err(|_| ())?;
        let mut out = Vec::new();
        for op in parsed.operations {
            if op.ty.as_deref() == Some("ack") {
                continue;
            }
            if let Some(items) = op.items {
                // nested push form
                for it in items {
                    out.push(ItemInfo {
                        queue: it.queue,
                        partition: it.partition.unwrap_or_else(|| DEFAULT_PARTITION.to_string()),
                        payload_len: it.payload.get().len(),
                    });
                }
            } else if let (Some(q), Some(pl)) = (op.queue, op.payload) {
                // flat push form
                out.push(ItemInfo {
                    queue: q,
                    partition: op.partition.unwrap_or_else(|| DEFAULT_PARTITION.to_string()),
                    payload_len: pl.get().len(),
                });
            }
            // else: ack-shaped or malformed op -> not a counted push
        }
        Ok(out)
    }
}

/// Count accepted messages in a push 201 response. The body is a top-level array
/// of per-item `{...,"status":...}`; `queued` and `buffered` count, `duplicate`
/// / `error` / `failed` do not (M1–M3). None on parse failure.
fn count_push_statuses(bytes: &[u8]) -> Option<u64> {
    #[derive(Deserialize)]
    struct StatusLite {
        status: String,
    }
    let arr: Vec<StatusLite> = serde_json::from_slice(bytes).ok()?;
    Some(
        arr.iter()
            .filter(|s| s.status == "queued" || s.status == "buffered")
            .count() as u64,
    )
}

/// Count delivered messages in a pop 200 response: `{...,"messages":[...]}`.
/// `IgnoredAny` counts elements without materialising them. None on parse fail.
fn count_pop_messages(bytes: &[u8]) -> Option<u64> {
    #[derive(Deserialize)]
    struct PopLite {
        #[serde(default)]
        messages: Vec<serde::de::IgnoredAny>,
    }
    let p: PopLite = serde_json::from_slice(bytes).ok()?;
    Some(p.messages.len() as u64)
}

fn content_length(headers: &HeaderMap) -> u64 {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn is_pop_path(path: &str) -> bool {
    path.starts_with("/api/v1/pop/queue/")
}

/// Map a (path, class) to the metering op class. Consume that is not a pop
/// (ack / lease) and Gated surfaces meter as reqs-only `Read` — see report.
fn op_for(path: &str, class: RouteClass) -> OpClass {
    match class {
        RouteClass::Produce if path == "/api/v1/push" => OpClass::Push,
        RouteClass::Produce => OpClass::Txn,
        RouteClass::Consume if is_pop_path(path) => OpClass::Delivery,
        RouteClass::Consume => OpClass::Read,
        RouteClass::QueueAdmin => OpClass::Configure,
        RouteClass::Read => OpClass::Read,
        RouteClass::Gated(_) => OpClass::Read,
        RouteClass::Blocked => OpClass::Read,
    }
}

/// Rebuild a client-facing response: strip hop-by-hop headers, stamp the request
/// id. `Content-Length` is left intact — buffered bodies are byte-identical.
fn finalize(mut parts: Parts, body: Body, rid: &str) -> Response {
    for h in HOP_BY_HOP {
        parts.headers.remove(*h);
    }
    if let Ok(v) = HeaderValue::from_str(rid) {
        parts.headers.insert(crate::config::REQUEST_ID_HEADER, v);
    }
    Response::from_parts(parts, body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_items_count_partition_default_and_payload_len() {
        let body = br#"{"items":[
            {"queue":"orders","payload":{"a":1}},
            {"queue":"orders","partition":"p1","payload":"hello"},
            {"queue":"events","payload":[1,2,3],"transactionId":"t1"}
        ]}"#;
        let items = parse_produce_items("/api/v1/push", body).expect("parse");
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].queue, "orders");
        assert_eq!(items[0].partition, "Default"); // defaulted
        assert_eq!(items[1].partition, "p1");
        assert_eq!(items[2].queue, "events");
        // payload_len == raw JSON text length of the value as sent
        assert_eq!(items[0].payload_len, r#"{"a":1}"#.len());
        assert_eq!(items[1].payload_len, r#""hello""#.len());
        assert_eq!(items[2].payload_len, "[1,2,3]".len());
    }

    #[test]
    fn push_empty_items_is_zero() {
        let items = parse_produce_items("/api/v1/push", br#"{"items":[]}"#).expect("parse");
        assert_eq!(items.len(), 0);
    }

    #[test]
    fn push_malformed_body_is_err() {
        // item missing required `queue` -> whole parse fails -> forward-anyway path
        assert!(parse_produce_items("/api/v1/push", br#"{"items":[{"payload":"x"}]}"#).is_err());
        // not JSON at all
        assert!(parse_produce_items("/api/v1/push", b"not json").is_err());
        // wrong top-level shape
        assert!(parse_produce_items("/api/v1/push", br#"{"nope":1}"#).is_err());
    }

    #[test]
    fn push_status_counting() {
        let body = br#"[
            {"index":0,"status":"queued","queueName":"a"},
            {"index":1,"status":"duplicate","queueName":"a"},
            {"index":2,"status":"error","queueName":"a"},
            {"index":3,"status":"buffered","queueName":"b"},
            {"index":4,"status":"failed","queueName":"b"}
        ]"#;
        // queued + buffered = 2; duplicate/error/failed excluded
        assert_eq!(count_push_statuses(body), Some(2));
        assert_eq!(count_push_statuses(b"[]"), Some(0));
        // an error object (not an array) -> None (parse fail, msgs=0 at call site)
        assert_eq!(count_push_statuses(br#"{"error":"bad body"}"#), None);
    }

    #[test]
    fn pop_message_counting() {
        let body = br#"{"success":true,"queue":"q","partition":"p","partitionId":"pid","leaseId":"l","consumerGroup":"g","messages":[{"a":1},{"b":2},{"c":3}]}"#;
        assert_eq!(count_pop_messages(body), Some(3));
        assert_eq!(count_pop_messages(br#"{"success":true,"messages":[]}"#), Some(0));
        // paused 204-ish body still parses to zero
        assert_eq!(count_pop_messages(br#"{"messages":[],"paused":true}"#), Some(0));
        // missing messages field defaults to empty
        assert_eq!(count_pop_messages(br#"{"success":true}"#), Some(0));
    }

    #[test]
    fn transaction_oplite_counts_push_ops_only() {
        // flat push + ack (ignored) + nested-items push
        let body = br#"{"operations":[
            {"type":"push","queue":"orders","payload":{"x":1}},
            {"type":"ack","transactionId":"t","partitionId":"pid","status":"completed"},
            {"type":"push","items":[
                {"queue":"orders","partition":"p2","payload":"a"},
                {"queue":"events","payload":"b"}
            ]}
        ],"requiredLeases":["l1"]}"#;
        let items = parse_produce_items("/api/v1/transaction", body).expect("parse");
        assert_eq!(items.len(), 3); // 1 flat + 2 nested; ack skipped
        assert_eq!(items[0].queue, "orders");
        assert_eq!(items[0].partition, "Default");
        assert_eq!(items[1].partition, "p2");
        assert_eq!(items[2].queue, "events");
        assert_eq!(items[0].payload_len, r#"{"x":1}"#.len());
    }

    #[test]
    fn transaction_ack_only_counts_zero() {
        let body = br#"{"operations":[
            {"type":"ack","transactionId":"t","partitionId":"pid","status":"completed"}
        ]}"#;
        let items = parse_produce_items("/api/v1/transaction", body).expect("parse");
        assert_eq!(items.len(), 0);
    }

    #[test]
    fn transaction_untyped_ack_is_skipped() {
        // no `type`, no queue/payload (an ack in all but name) -> not counted
        let body = br#"{"operations":[
            {"transactionId":"t","partitionId":"pid","status":"completed"},
            {"queue":"q","payload":1}
        ]}"#;
        let items = parse_produce_items("/api/v1/transaction", body).expect("parse");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].queue, "q");
    }

    #[test]
    fn op_class_mapping() {
        assert_eq!(op_for("/api/v1/push", RouteClass::Produce), OpClass::Push);
        assert_eq!(
            op_for("/api/v1/transaction", RouteClass::Produce),
            OpClass::Txn
        );
        assert_eq!(
            op_for("/api/v1/pop/queue/orders", RouteClass::Consume),
            OpClass::Delivery
        );
        assert_eq!(op_for("/api/v1/ack", RouteClass::Consume), OpClass::Read);
        assert_eq!(
            op_for("/api/v1/lease/abc/extend", RouteClass::Consume),
            OpClass::Read
        );
        assert_eq!(
            op_for("/api/v1/configure", RouteClass::QueueAdmin),
            OpClass::Configure
        );
        assert_eq!(
            op_for("/api/v1/resources/queues", RouteClass::Read),
            OpClass::Read
        );
    }

    #[test]
    fn content_length_parse() {
        let mut h = HeaderMap::new();
        assert_eq!(content_length(&h), 0);
        h.insert(header::CONTENT_LENGTH, HeaderValue::from_static("1234"));
        assert_eq!(content_length(&h), 1234);
    }
}
