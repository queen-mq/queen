//! The data-plane pipeline. OWNER: Agent A.
//!
//! Pipeline per request (spec §4/§14 — the order is load-bearing):
//!   1. resolve ClusterCtx from Host (miss -> 421)
//!   2. cluster status gate (Suspended -> 403 suspended; Produce while blocked
//!      -> 403 storage_quota_exceeded (live quota flag) or push_blocked (DB
//!      lifecycle status) — two causes, two codes)
//!   3. authenticate (auth::authenticate) + authorize (auth::authorize vs classify)
//!   4. limits: check_req; Produce -> buffer body (cap = min(plan, cfg)), count
//!      items + per-item payload caps, registry.admit each (queue,partition),
//!      check_msgs(n); POST /configure -> buffer body, retention ceiling +
//!      registry.admit the created (queue, Default); Consume wait=true ->
//!      parked_slot RAII guard held across the upstream await
//!   5. forward: rebuild URI on ctx.cell_base_url, strip hop-by-hop headers,
//!      inject Authorization (ctx.cell_token), X-Queen-Tenant (cfg.send_tenant_header),
//!      X-Queen-Request-Id; long-poll timeout = min(client timeout|30s, cfg max) + margin
//!   6. meter post-response (M1–M6): push -> parse per-item statuses (exclude
//!      error, dedupe duplicate, buffered counts), pop -> delivered count +
//!      debit_deliveries, bytes in/out always; the same push parse feeds the
//!      sampled §6.10 maintenance signal
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
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri};
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
    if class == RouteClass::Produce {
        // Three causes, three codes — Track C clients switch on the code and
        // treat `storage_quota_exceeded` as terminal. Each live flag is written
        // by exactly one thing (storage: the pump in main.rs off registry
        // over_storage; monthly: the rollup task off plans.monthly_msgs_quota),
        // so when one is set the cause is unambiguous; ctx.status is the DB
        // lifecycle one (tenant `grace` or cluster `push_blocked` —
        // cache.rs::merge_status) and never says *why* it was set, so it keeps
        // the generic code. Live flags first: they are the more specific claim.
        //
        // Both live flags are HARD gates — checked and enforced regardless of
        // `limits.enforcing()`, unlike every rate decision below. That is
        // deliberate (a quota that only warns protects neither the cell's disk
        // nor the bill) and it is the surprising part: a misconfigured
        // monthly_msgs_quota blocks production pushes even on a cell deployed
        // in shadow mode.
        match st.limits.push_block_reason(ctx.cluster_id) {
            Some(crate::limits::PushBlock::Storage) => {
                return errors::err_403(
                    errors::CODE_STORAGE_QUOTA,
                    "storage quota exceeded; pushes blocked",
                );
            }
            Some(crate::limits::PushBlock::MonthlyQuota) => {
                return errors::err_403(
                    errors::CODE_QUOTA_EXCEEDED,
                    "monthly message quota (monthly_msgs_quota) exhausted; pushes blocked until the next calendar month",
                );
            }
            None => {}
        }
        if ctx.status == ClusterStatus::PushBlocked {
            return errors::err_403(errors::CODE_PUSH_BLOCKED, "pushes blocked (billing hold)");
        }
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
    } else if is_configure(&parts.method, &path_only) {
        // 4b'. QueueAdmin: /configure is the EXPLICIT creation path (push only
        // auto-creates), so the registry caps apply here identically (§4/§14),
        // plus the per-plan retention ceiling (§6.1). Same hard body-buffer
        // ceiling as produce — we must bound what we buffer.
        let buffered = match axum::body::to_bytes(body, st.cfg.max_body_bytes).await {
            Ok(b) => b,
            Err(_) => return errors::err_413("request body exceeds cap"),
        };
        // This route is now buffered, so its true ingress size is known: meter
        // it like every other buffered route (STEP 6, "bytes in/out always")
        // instead of the 0 that not-buffering used to imply.
        bytes_in = buffered.len() as u64;
        if let Err(resp) = enforce_configure(&st, &ctx, &buffered, &rid).await {
            return resp;
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
        let counts = count_push_statuses(&buffered).unwrap_or_else(|| {
            tracing::warn!(target: "meter", cluster = %ctx.slug, rid, "push response parse failed; msgs=0");
            PushCounts::default()
        });
        // §6.10 maintenance signal, off the same parse (never a second pass).
        // Short-circuits before the clock read when nothing was buffered.
        if predominantly_buffered(&counts) && maint_log_due(std::time::Instant::now()) {
            tracing::info!(
                target: "gateway", cluster = %ctx.slug, buffered = counts.buffered,
                items = counts.total, rid,
                "cell is spooling pushes to disk (maintenance mode or DB outage)"
            );
        }
        st.meter.record(Sample {
            cluster_id: ctx.cluster_id,
            op: OpClass::Push,
            reqs: 1,
            msgs: counts.accepted,
            bytes_in,
            bytes_out: buffered.len() as u64,
        });
        return finalize(rparts, Body::from(buffered), &rid);
    }

    // Transaction: charge the ingress-counted push ops, but only for a
    // transaction that actually committed. The broker answers HTTP 200 on
    // rollback too (`{transactionId, success:false, error, results:[]}` —
    // server/src/handlers/data.rs::txn_fail_body, the SP RAISEs and the whole
    // transaction is undone), so the status line alone cannot tell the two
    // apart and this used to bill a tenant for messages that were never
    // stored. The body can: `success` is a top-level bool on both paths.
    if op == OpClass::Txn {
        if !status.is_success() {
            st.meter.record(Sample {
                cluster_id: ctx.cluster_id,
                op: OpClass::Txn,
                reqs: 1,
                msgs: 0,
                bytes_in,
                bytes_out: resp_cl,
            });
            let (rparts, rbody) = resp.into_parts();
            return finalize(rparts, Body::new(rbody), &rid);
        }
        let (rparts, rbody) = resp.into_parts();
        let buffered = match axum::body::to_bytes(Body::new(rbody), RESP_BUFFER_CAP).await {
            Ok(b) => b,
            Err(_) => {
                tracing::warn!(target: "meter", cluster = %ctx.slug, rid, "transaction response too large to buffer");
                return errors::err_502("transaction response too large");
            }
        };
        let msgs = match txn_outcome(&buffered) {
            // Intra-batch first-wins dedup (txn_add_push's `seen` map) echoes
            // `duplicate:true` and stores nothing new — excluded exactly like
            // a `duplicate` status on the push path (M2).
            TxnOutcome::Committed { duplicates } => produce_n.saturating_sub(duplicates),
            TxnOutcome::RolledBack => 0,
            TxnOutcome::Unknown => {
                // Same stance as an unparseable push 201: charge nothing we
                // cannot confirm. Under-billing on a shape we don't recognise
                // beats billing a rollback.
                tracing::warn!(target: "meter", cluster = %ctx.slug, rid, "transaction response parse failed; msgs=0");
                0
            }
        };
        st.meter.record(Sample {
            cluster_id: ctx.cluster_id,
            op: OpClass::Txn,
            reqs: 1,
            msgs,
            bytes_in,
            bytes_out: buffered.len() as u64,
        });
        return finalize(rparts, Body::from(buffered), &rid);
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

/// STEP 4 for `POST /api/v1/configure`, the explicit queue-creation path.
/// Push auto-creates queues/partitions and is capped by the registry; configure
/// creates them deliberately and must be capped identically (§4/§14), on top of
/// the per-plan retention ceiling (§6.1). Shadow-aware like `enforce_produce`,
/// and the body is forwarded verbatim either way.
async fn enforce_configure(
    st: &St,
    ctx: &ClusterCtx,
    bytes: &Bytes,
    rid: &str,
) -> Result<(), Response> {
    let cfg = match parse_configure(bytes) {
        Ok(c) => c,
        Err(()) => {
            // Malformed body: forward verbatim, let the broker return its own
            // 400 — same rule as an unparseable produce batch, no half-enforcement.
            tracing::warn!(
                target: "limits", cluster = %ctx.slug, rid,
                "configure body unparseable; forwarding without admin enforcement"
            );
            return Ok(());
        }
    };

    // Retention ceiling. Checked BEFORE admission so a refused configure leaves
    // nothing registered.
    //
    // REFUSE, not clamp: §14's queue-admin row says "retention clamp", but the
    // proxy is parse-only and never rewrites a request body (§5, "parse-only,
    // no rewrite, ever"), and every other limit here refuses — 403
    // quota_exceeded, 413 payload_too_large, 429 rate_limited. Silently
    // shortening a customer's retention would make their data disappear later
    // with no signal at the moment they asked for it; an error they can see
    // beats data they cannot get back.
    if let Some(ceiling) = ctx.limits.max_retention_seconds {
        if let Some((key, requested)) = cfg.retention_over(ceiling) {
            if st.limits.enforcing() {
                tracing::warn!(
                    target: "limits", cluster = %ctx.slug, kind = "retention",
                    queue = %cfg.queue, requested, ceiling, blocked = true, rid,
                    "retention ceiling exceeded"
                );
                return Err(errors::err_403(
                    errors::CODE_QUOTA_EXCEEDED,
                    &format!(
                        "{key} of {requested}s exceeds the plan's max_retention_seconds ({ceiling}s)"
                    ),
                ));
            }
            tracing::warn!(
                target: "limits", cluster = %ctx.slug, kind = "retention",
                queue = %cfg.queue, requested, ceiling, would_block = true, rid,
                "retention ceiling exceeded (shadow)"
            );
        }
    }

    // Registry admission. configure_queue_v1 (server/sql/procedures/012_configure.sql)
    // creates the queue plus exactly one partition, `Default` — there is no
    // partition-count option in the body — so one admit call covers what this
    // request can create, through the same API the produce path calls per
    // distinct (queue, partition).
    match st.registry.admit(ctx, &cfg.queue, DEFAULT_PARTITION).await {
        Admit::Allowed => {}
        Admit::OverQueues { max } => {
            log_configure_deny(st.limits.enforcing(), "queues", ctx, &cfg.queue, max, rid);
            if st.limits.enforcing() {
                return Err(errors::err_403(
                    errors::CODE_QUOTA_EXCEEDED,
                    &format!("queue limit reached ({max})"),
                ));
            }
        }
        Admit::OverPartitions { max } => {
            log_configure_deny(st.limits.enforcing(), "partitions", ctx, &cfg.queue, max, rid);
            if st.limits.enforcing() {
                return Err(errors::err_403(
                    errors::CODE_QUOTA_EXCEEDED,
                    &format!("partition limit reached ({max})"),
                ));
            }
        }
    }

    Ok(())
}

/// Uniform deny log for the configure path, same field shape as
/// limits.rs::log_deny (`kind` plus `blocked`/`would_block` booleans — the
/// canonical pair the limits dashboards filter on). `enforce_produce`'s
/// registry arms predate that convention and put the kind in `would_block`
/// itself; new code uses the canonical form.
fn log_configure_deny(
    enforcing: bool,
    kind: &'static str,
    ctx: &ClusterCtx,
    queue: &str,
    max: i64,
    rid: &str,
) {
    if enforcing {
        tracing::warn!(
            target: "limits", cluster = %ctx.slug, kind, queue, max, blocked = true, rid,
            "configure over cap"
        );
    } else {
        tracing::warn!(
            target: "limits", cluster = %ctx.slug, kind, queue, max, would_block = true, rid,
            "configure over cap (shadow)"
        );
    }
}

/// The one QueueAdmin route with a body worth parsing. `classify` maps this
/// path to QueueAdmin for every method, so the method check lives here.
fn is_configure(method: &Method, path: &str) -> bool {
    method == Method::POST && path == "/api/v1/configure"
}

/// Options that decide how long a queue keeps data: both are read by
/// configure_queue_v1 (012_configure.sql) into queen.queues and become the
/// retention sweep's rule-1 / rule-2 cutoffs (server/src/retention.rs).
/// Deliberately NOT here: `maxWaitTimeSeconds`, an eviction deadline that only
/// ever SHORTENS a message's life (045_log_maintenance.sql,
/// log_evict_max_wait_step_v1), so it cannot exceed a retention ceiling; and
/// `ttl`, which the segments engine only echoes back.
const RETENTION_KEYS: &[&str] = &["retentionSeconds", "completedRetentionSeconds"];

/// The parts of a `/configure` body the proxy enforces on.
struct ConfigureInfo {
    queue: String,
    /// (option name, seconds) for each retention option set to a POSITIVE
    /// value — see `parse_configure` for why non-positive is skipped.
    retention: Vec<(&'static str, i64)>,
}

impl ConfigureInfo {
    /// The first retention option above `ceiling`, if any.
    fn retention_over(&self, ceiling: i64) -> Option<(&'static str, i64)> {
        self.retention.iter().copied().find(|(_, secs)| *secs > ceiling)
    }
}

/// Parse a configure body, mirroring the broker's own normalization
/// (server/src/handlers/queues.rs::handle_configure): `queue` must be a string
/// — an EMPTY name is legal and does create a queue — and the options bag is
/// the nested `options` object when it *is* an object, otherwise the top-level
/// body minus the routing keys (neither retention key is a routing key, so
/// reading them straight off the root is the same bag). Err on anything the
/// broker would 400 on, so the caller forwards without enforcing.
fn parse_configure(bytes: &[u8]) -> Result<ConfigureInfo, ()> {
    let root: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| ())?;
    let obj = root.as_object().ok_or(())?;
    let queue = obj.get("queue").and_then(|q| q.as_str()).ok_or(())?.to_string();
    let opts = obj.get("options").and_then(|o| o.as_object()).unwrap_or(obj);

    let mut retention = Vec::new();
    for key in RETENTION_KEYS {
        let Some(v) = opts.get(*key) else { continue };
        // A JSON string counts too: the SP reads the option with `->>` (text)
        // and casts to integer, so "3600" and 3600 configure the same
        // retention — the ceiling has to see both or it is trivially bypassed.
        let secs = v
            .as_i64()
            .or_else(|| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()));
        // Non-positive (or absent) means that rule is DISABLED, i.e. data is
        // kept forever — retention.rs gates on `retention_enabled AND
        // retention_seconds > 0`. Unbounded is not "under the ceiling", but it
        // is also exactly what every client sends by default (the JS
        // QUEUE_DEFAULTS post retentionSeconds: 0), so refusing it would 403 an
        // out-of-the-box `.queue(x).create()`. Unbounded growth is the storage
        // quota's job (max_retained_bytes -> push blocked, §6.1); this ceiling
        // caps what a tenant explicitly asks to keep.
        if let Some(s) = secs.filter(|s| *s > 0) {
            retention.push((*key, s));
        }
    }
    Ok(ConfigureInfo { queue, retention })
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

/// Per-item tallies from a push 201 response.
#[derive(Default, Debug, PartialEq, Eq)]
struct PushCounts {
    /// Billable items (M1–M3): `queued` + `buffered`.
    accepted: u64,
    /// `buffered` alone — tracked separately because it is the cell's
    /// maintenance/spool signal (§6.10), not because it bills differently.
    buffered: u64,
    /// Items in the response, whatever their status.
    total: u64,
}

/// Count a push 201 response. The body is a top-level array of per-item
/// `{...,"status":...}`; `queued` and `buffered` are accepted, `duplicate` /
/// `error` / `failed` are not (M1–M3). None on parse failure.
fn count_push_statuses(bytes: &[u8]) -> Option<PushCounts> {
    #[derive(Deserialize)]
    struct StatusLite {
        status: String,
    }
    let arr: Vec<StatusLite> = serde_json::from_slice(bytes).ok()?;
    let mut counts = PushCounts { total: arr.len() as u64, ..PushCounts::default() };
    for s in &arr {
        match s.status.as_str() {
            "queued" => counts.accepted += 1,
            "buffered" => {
                counts.accepted += 1;
                counts.buffered += 1;
            }
            _ => {}
        }
    }
    Some(counts)
}

/// What a transaction 2xx response says about the push ops counted on the way
/// in. The two committed/rolled-back shapes are documented at
/// server/src/handlers/data.rs::handle_transaction.
#[derive(Debug, PartialEq, Eq)]
enum TxnOutcome {
    /// `success:true` — the SP committed. `duplicates` push ops were
    /// first-wins-deduped inside the batch and created no message.
    Committed { duplicates: u64 },
    /// `success:false` — the SP raised and the whole transaction rolled back,
    /// so NOTHING was stored, whatever the request contained.
    RolledBack,
    /// Not a shape this proxy can read (broker skew, truncation, non-JSON).
    Unknown,
}

/// Read a transaction response. `success` is required: a body without it is
/// `Unknown` rather than assumed-committed, so an unrecognised shape can never
/// bill a rollback.
fn txn_outcome(bytes: &[u8]) -> TxnOutcome {
    #[derive(Deserialize)]
    struct TxnResultLite {
        #[serde(default, rename = "type")]
        ty: Option<String>,
        #[serde(default)]
        duplicate: bool,
    }
    #[derive(Deserialize)]
    struct TxnRespLite {
        success: bool,
        // Nulls are legal here (results is pre-sized per flat op index), so
        // each entry is optional rather than failing the whole parse.
        #[serde(default)]
        results: Vec<Option<TxnResultLite>>,
    }
    let Ok(resp) = serde_json::from_slice::<TxnRespLite>(bytes) else {
        return TxnOutcome::Unknown;
    };
    if !resp.success {
        return TxnOutcome::RolledBack;
    }
    let duplicates = resp
        .results
        .iter()
        .flatten()
        .filter(|r| r.duplicate && r.ty.as_deref() == Some("push"))
        .count() as u64;
    TxnOutcome::Committed { duplicates }
}

/// §6.10: a cell in maintenance (or with its DB down) spools pushes to disk and
/// answers `buffered` instead of `queued` (server/src/handlers/data.rs). A
/// single buffered item can also be one item's failed transaction, so only a
/// clear majority is read as "this cell is not writing to PG right now".
fn predominantly_buffered(counts: &PushCounts) -> bool {
    counts.total > 0 && counts.buffered * 2 > counts.total
}

/// Sampling interval for the maintenance signal — one line, not one per push.
const MAINT_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Next instant a maintenance line may be emitted. Process-wide rather than
/// per-cluster on purpose: one proxy fronts one cell (§2) and maintenance is a
/// property of the cell, so the first cluster to notice reports for all of them.
static MAINT_LOG_NEXT: std::sync::Mutex<Option<std::time::Instant>> = std::sync::Mutex::new(None);

/// Is a maintenance line due? Non-blocking try_lock, so a concurrent responder
/// skips its line instead of waiting on the hot path (same shape as
/// limits.rs::maybe_gc).
fn maint_log_due(now: std::time::Instant) -> bool {
    let Ok(mut next) = MAINT_LOG_NEXT.try_lock() else { return false };
    match *next {
        Some(at) if now < at => false,
        _ => {
            *next = Some(now + MAINT_LOG_INTERVAL);
            true
        }
    }
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
        // queued + buffered = 2 accepted; duplicate/error/failed excluded
        let counts = count_push_statuses(body).expect("parse");
        assert_eq!(counts.accepted, 2);
        assert_eq!(counts.buffered, 1);
        assert_eq!(counts.total, 5);
        assert_eq!(count_push_statuses(b"[]"), Some(PushCounts::default()));
        // an error object (not an array) -> None (parse fail, msgs=0 at call site)
        assert_eq!(count_push_statuses(br#"{"error":"bad body"}"#), None);
    }

    #[test]
    fn maintenance_signal_needs_a_buffered_majority() {
        let all_buffered = br#"[{"status":"buffered"},{"status":"buffered"}]"#;
        assert!(predominantly_buffered(&count_push_statuses(all_buffered).expect("parse")));

        // one item's transaction failed and spooled: not a cell-wide signal
        let one_of_three = br#"[{"status":"queued"},{"status":"queued"},{"status":"buffered"}]"#;
        assert!(!predominantly_buffered(&count_push_statuses(one_of_three).expect("parse")));

        // exactly half is not a majority
        let half = br#"[{"status":"queued"},{"status":"buffered"}]"#;
        assert!(!predominantly_buffered(&count_push_statuses(half).expect("parse")));

        // nothing to report: empty array, and the parse-failure default
        assert!(!predominantly_buffered(&count_push_statuses(b"[]").expect("parse")));
        assert!(!predominantly_buffered(&PushCounts::default()));
    }

    #[test]
    fn maint_log_sampling_gate_admits_one_line_per_interval() {
        // The gate is a process-wide static; this is the only test touching it.
        let t0 = std::time::Instant::now();
        assert!(maint_log_due(t0), "first line is always due");
        assert!(!maint_log_due(t0), "a second push in the same instant is sampled out");
        assert!(
            !maint_log_due(t0 + MAINT_LOG_INTERVAL - std::time::Duration::from_millis(1)),
            "still inside the interval"
        );
        assert!(maint_log_due(t0 + MAINT_LOG_INTERVAL), "due again once the interval elapses");
    }

    // ---- transaction billing: a rollback is HTTP 200 and must not be charged ----

    #[test]
    fn transaction_rollback_is_not_charged() {
        // txn_fail_body's exact shape (data.rs): 200, success:false, empty
        // results. Every push op in the request was undone.
        let rolled_back = br#"{"transactionId":"01890000-0000-7000-8000-000000000000",
            "success":false,"error":"QDUP duplicate transaction","results":[]}"#;
        assert_eq!(txn_outcome(rolled_back), TxnOutcome::RolledBack);
    }

    #[test]
    fn transaction_commit_charges_pushes_minus_intra_batch_duplicates() {
        let committed = br#"{"transactionId":"t","success":true,"results":[
            {"index":0,"type":"push","success":true,"transactionId":"a","messageId":"m1","queueName":"q"},
            {"index":1,"type":"push","success":true,"transactionId":"a","messageId":"m1","queueName":"q","duplicate":true},
            {"index":2,"type":"ack","success":true,"transactionId":"a","error":null,"dlq":false}
        ]}"#;
        // One real push, one first-wins duplicate (M2: never charged), one ack.
        assert_eq!(txn_outcome(committed), TxnOutcome::Committed { duplicates: 1 });

        // Ack-only transaction: committed, nothing to subtract.
        let ack_only = br#"{"transactionId":"t","success":true,"results":[
            {"index":0,"type":"ack","success":true,"transactionId":"a","error":null,"dlq":true}
        ]}"#;
        assert_eq!(txn_outcome(ack_only), TxnOutcome::Committed { duplicates: 0 });
    }

    #[test]
    fn transaction_results_may_contain_nulls() {
        // results is pre-sized per flat op index and left Null where no echo
        // landed — that must not fail the parse (which would bill 0).
        let with_nulls = br#"{"transactionId":"t","success":true,"results":[
            null,{"index":1,"type":"push","success":true,"duplicate":true}
        ]}"#;
        assert_eq!(txn_outcome(with_nulls), TxnOutcome::Committed { duplicates: 1 });
    }

    #[test]
    fn transaction_unreadable_body_is_unknown_not_committed() {
        // No `success` key, not JSON, or the wrong top-level shape: never
        // assumed committed — Unknown charges 0 at the call site.
        assert_eq!(txn_outcome(br#"{"transactionId":"t","results":[]}"#), TxnOutcome::Unknown);
        assert_eq!(txn_outcome(b"not json"), TxnOutcome::Unknown);
        assert_eq!(txn_outcome(br#"[{"success":true}]"#), TxnOutcome::Unknown);
        assert_eq!(txn_outcome(b""), TxnOutcome::Unknown);
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

    // ---- /configure enforcement (registry admission + retention ceiling) ----

    #[test]
    fn configure_route_is_post_only() {
        assert!(is_configure(&Method::POST, "/api/v1/configure"));
        // classify() maps the path for every method; only POST carries a body
        assert!(!is_configure(&Method::GET, "/api/v1/configure"));
        assert!(!is_configure(&Method::POST, "/api/v1/configure/extra"));
        assert!(!is_configure(&Method::POST, "/api/v1/push"));
    }

    #[test]
    fn configure_queue_name_from_client_shape() {
        // what QueueBuilder.create() posts: {queue, namespace, task, options}
        let body = br#"{"queue":"orders","namespace":"ns","task":"t",
            "options":{"leaseTime":300,"retryLimit":3,"retentionSeconds":0}}"#;
        let cfg = parse_configure(body).expect("parse");
        assert_eq!(cfg.queue, "orders");
        // retentionSeconds:0 = retention disabled = not a ceiling candidate
        assert_eq!(cfg.retention_over(60), None);
    }

    #[test]
    fn configure_empty_queue_name_is_valid() {
        // the broker creates a queue named "" (handle_configure accepts it), so
        // the proxy must enforce on it rather than treat the body as malformed
        let cfg = parse_configure(br#"{"queue":"","options":{}}"#).expect("parse");
        assert_eq!(cfg.queue, "");
    }

    #[test]
    fn configure_malformed_body_is_err() {
        // no queue key -> broker 400s -> we forward without enforcing
        assert!(parse_configure(br#"{"options":{"leaseTime":300}}"#).is_err());
        // queue present but not a string
        assert!(parse_configure(br#"{"queue":42}"#).is_err());
        // not JSON at all, and a non-object top level
        assert!(parse_configure(b"not json").is_err());
        assert!(parse_configure(br#"[{"queue":"orders"}]"#).is_err());
    }

    #[test]
    fn configure_retention_read_from_either_options_shape() {
        // nested options bag (client shape)
        let nested = parse_configure(br#"{"queue":"q","options":{"retentionSeconds":86400}}"#)
            .expect("parse");
        assert_eq!(nested.retention_over(3600), Some(("retentionSeconds", 86400)));
        // top-level bag (raw caller shape — the broker's fallback)
        let flat = parse_configure(br#"{"queue":"q","retentionSeconds":86400}"#).expect("parse");
        assert_eq!(flat.retention_over(3600), Some(("retentionSeconds", 86400)));
        // `options` present but not an object -> broker falls back to top level
        let odd = parse_configure(br#"{"queue":"q","options":7,"retentionSeconds":86400}"#)
            .expect("parse");
        assert_eq!(odd.retention_over(3600), Some(("retentionSeconds", 86400)));
    }

    #[test]
    fn configure_retention_ceiling_decision() {
        let body = br#"{"queue":"q","options":{"retentionSeconds":3600,
            "completedRetentionSeconds":7200,"maxWaitTimeSeconds":999999,"ttl":999999}}"#;
        let cfg = parse_configure(body).expect("parse");
        // under/at the ceiling: allowed (the check is strictly greater-than)
        assert_eq!(cfg.retention_over(7200), None);
        // completedRetentionSeconds alone over the ceiling is still a refusal,
        // and the option name comes back for the error message
        assert_eq!(cfg.retention_over(3600), Some(("completedRetentionSeconds", 7200)));
        // both over -> reports the first one found, in RETENTION_KEYS order
        assert_eq!(cfg.retention_over(60), Some(("retentionSeconds", 3600)));
    }

    #[test]
    fn configure_retention_accepts_the_numeric_string_form() {
        // the SP reads options with `->>` and casts, so "86400" configures the
        // same retention as 86400 — the ceiling must not be bypassable that way
        let cfg = parse_configure(br#"{"queue":"q","options":{"retentionSeconds":"86400"}}"#)
            .expect("parse");
        assert_eq!(cfg.retention_over(3600), Some(("retentionSeconds", 86400)));
        // a non-numeric string is not a retention request (the SP's cast would
        // error broker-side); nothing to enforce, forward and let it 500 there
        let junk = parse_configure(br#"{"queue":"q","options":{"retentionSeconds":"forever"}}"#)
            .expect("parse");
        assert_eq!(junk.retention_over(1), None);
    }

    #[test]
    fn configure_disabled_retention_is_not_a_ceiling_violation() {
        // 0 / negative / absent all mean the retention rule is OFF (kept
        // forever). Deliberately allowed here — see parse_configure.
        for body in [
            &br#"{"queue":"q","options":{"retentionSeconds":0}}"#[..],
            &br#"{"queue":"q","options":{"retentionSeconds":-1}}"#[..],
            &br#"{"queue":"q","options":{}}"#[..],
        ] {
            let cfg = parse_configure(body).expect("parse");
            assert_eq!(cfg.retention_over(1), None, "body: {}", String::from_utf8_lossy(body));
        }
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
