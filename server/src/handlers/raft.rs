//! `handlers/raft.rs` — the raft-mode wiring of the storage seam (PLAN_RAFT.md
//! WP-1.7a, expanded by Phase 2).
//!
//! Three things live here:
//!
//! 1. **Receiver dispatch** (`dispatch_*`): the message-path handlers
//!    (`handle_push`, the three `handle_pop*`, `handle_ack*`,
//!    `handle_lease_extend`) branch here at their very top when
//!    `st.storage.is_raft()`. Each does the pool-free receiver pre-work of §9.1
//!    (the request id, the per-request deadline, and the R-108 name-length
//!    guard) and hands a typed command to `st.rsm` (rsm/facade.rs). The receiver
//!    threads validated producer identity into the facade; the facade owns
//!    frame packing, encryption, dedup repacking, and long-poll parking.
//!
//! 2. **`/health`, `/metrics/prometheus`, `/stats/refresh`**: they read
//!    node-local state only. `/health` keeps `status`/`version`/`engine` and
//!    adds the `raft` block (§14.1); `/stats/refresh` is the no-op 200 §9.6
//!    requires (every SDK's Admin API calls it); `/metrics/prometheus` drops the
//!    DB-backed blob and keeps the in-process families.
//!
//! 3. **The composition roots** `build_raft_state` (the `AppState`) and
//!    `build_raft_router` (the router:
//!    hot routes to the typed handlers and the rest of `/api` and `/streams` to
//!    the generic Phase-2 facade adapter). `build_raft_state` is shared
//!    by `main.rs` (the binary), `embedded/boot.rs` and the seam test, so the
//!    one raft `AppState` shape cannot drift between them.

use std::sync::Arc;

use axum::http::{header, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};

use super::{json, AppState};
use crate::config::Config;
use crate::rsm::facade::{
    self, AckReq, Deadline, DepthReq, DlqHeadReq, PopDiscoverReq, PopOptions, PopPinnedReq, PopReq,
    PushReq, RenewReq, ReqCtx, RsmError,
};

// ---------------------------------------------------------------------------
// RsmError → HTTP.
// ---------------------------------------------------------------------------

/// Render a typed [`RsmError`] as the HTTP response the SDKs and the proxy read.
/// Every arm carries a stable `code`; the retryable arms carry `Retry-After`.
/// Never a bare 500 and never a panic — the un-ported surface is a clean 503.
pub(crate) fn err_response(e: RsmError) -> Response {
    let (status, retry_after): (StatusCode, Option<u64>) = match &e {
        RsmError::Unsupported => (StatusCode::SERVICE_UNAVAILABLE, Some(1)),
        RsmError::Retry { .. } => (StatusCode::SERVICE_UNAVAILABLE, Some(1)),
        RsmError::NoLeader => (StatusCode::SERVICE_UNAVAILABLE, Some(1)),
        RsmError::NameTooLong { .. } => (StatusCode::PAYLOAD_TOO_LARGE, None),
        RsmError::StorageFull => (StatusCode::INSUFFICIENT_STORAGE, None),
        RsmError::Overloaded { retry_after_s } => {
            (StatusCode::TOO_MANY_REQUESTS, Some(*retry_after_s))
        }
        RsmError::Timeout => (StatusCode::SERVICE_UNAVAILABLE, Some(1)),
        RsmError::InDoubt => (StatusCode::SERVICE_UNAVAILABLE, Some(1)),
        RsmError::Standby => (StatusCode::SERVICE_UNAVAILABLE, Some(1)),
        RsmError::Rejected { .. } => (StatusCode::BAD_REQUEST, None),
        RsmError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, None),
    };
    // Build a valid JSON body {"error":"<escaped>","code":"<code>"} (+ the
    // leader hint when we have one). Reuse `json_escape_into` for the message.
    // A `Rejected` carries the planner's own `code`; every other variant uses
    // its stable static one.
    let mut body = String::from("{\"error\":\"");
    crate::util::json_escape_into(&mut body, &e.to_string());
    body.push_str("\",\"code\":\"");
    match &e {
        RsmError::Rejected { code, .. } => crate::util::json_escape_into(&mut body, code),
        _ => body.push_str(e.code()),
    }
    body.push('"');
    if let RsmError::Retry {
        leader_hint: Some(h),
    } = &e
    {
        body.push_str(",\"leader\":\"");
        crate::util::json_escape_into(&mut body, h);
        body.push('"');
    }
    body.push('}');

    match retry_after {
        Some(secs) => (
            status,
            [
                (header::CONTENT_TYPE, "application/json".to_string()),
                (header::RETRY_AFTER, secs.to_string()),
            ],
            body,
        )
            .into_response(),
        None => json(status, body),
    }
}

/// The response for a route that raft phase 1 does not serve. Named so the
/// fallback and any explicit un-ported route read the same.
#[allow(dead_code)]
pub(crate) fn unsupported() -> Response {
    err_response(RsmError::Unsupported)
}

/// The per-command deadline, derived from the client's timeout budget (§9.1
/// step 5). A generous ceiling for the seam; WP-1.7c derives it from the request.
fn deadline_for(timeout_ms: u64) -> Deadline {
    let ms = timeout_ms.clamp(1, 60_000);
    Deadline::after(std::time::Duration::from_millis(ms))
}

// ---------------------------------------------------------------------------
// Receiver dispatch (called by the storage-aware guards in data.rs).
// ---------------------------------------------------------------------------

/// `POST /api/v1/push`. Bounds every item's `(queue, partition)` name (R-108),
/// then hands the raw body to the facade. WP-1.7c adds the fusion pack/hash,
/// encryption and message-id pre-work of §9.1.
pub(crate) async fn dispatch_push(
    st: &AppState,
    tenant: &str,
    producer_sub: Option<String>,
    body: axum::body::Bytes,
) -> Response {
    // The name-length guard (R-108) runs inside the facade's own parse of the
    // body, per item, before anything is submitted: a second parse here only
    // to read the names cost a full scan of every payload (measured
    // 2026-09-23: serde_json `ignore_value`/`skip_to_escape` among the top
    // HTTP-side symbols at 400k msg/s).
    let ctx = ReqCtx::new(tenant, deadline_for(st.pop_default_timeout_ms))
        .with_producer_sub(producer_sub);
    // PERF-J: the whole push handler, push-only. Compared to goload's per-request
    // latency this isolates the server-side cost from the loader / axum-accept /
    // auth+tenant middleware wrapper (the ~200 ms C1000 gap PERF-J localizes).
    let _t_total = crate::rsm::timing::stamp();
    let resp = match st.rsm.push(ctx, PushReq { raw: body.to_vec() }).await {
        Ok(out) => json(StatusCode::CREATED, out.body),
        Err(e) => err_response(e),
    };
    if let Some(t) = _t_total {
        crate::rsm::timing::metrics()
            .push_h_total
            .record_dur(t.elapsed());
    }
    resp
}

/// A pop's answer: an empty one is a bodiless 204, unless it has something to
/// say about
/// conflation (the group's effective policy conflates, or the request conflicted
/// with it) — an SDK that asked for conflation reads a 204 as a broker that
/// cannot conflate. A conflict is counted and logged.
fn pop_answer(
    st: &AppState,
    tenant: &str,
    out: crate::rsm::facade::PopOut,
    conflict_scope: Option<(Option<&str>, &str, &str, Option<bool>)>,
) -> Response {
    if out.conflation_conflict {
        if let Some((queue, scope, group, requested)) = conflict_scope {
            crate::handlers::data::note_conflation_conflict(
                st,
                tenant,
                queue,
                scope,
                group,
                out.conflation,
                requested,
            );
        }
    }
    if out.empty && !out.conflation && !out.conflation_conflict {
        StatusCode::NO_CONTENT.into_response()
    } else {
        json(StatusCode::OK, out.body)
    }
}

/// `GET /api/v1/pop/queue/:queue` (wildcard). Fields are extracted by the guard
/// in data.rs (where `PopParams`'s private fields are readable) and passed in.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn dispatch_pop(
    st: &AppState,
    tenant: &str,
    queue: String,
    group: Option<String>,
    batch: i32,
    auto_ack: bool,
    wait: bool,
    timeout_ms: u64,
    options: PopOptions,
) -> Response {
    if let Err(e) = facade::check_message_key_names(tenant, &queue, group.as_deref(), None) {
        return err_response(e);
    }
    let ctx = ReqCtx::new(tenant, deadline_for(timeout_ms));
    // A conflict needs the names for its log line; only a request that named a
    // policy can conflict, so nothing is copied otherwise.
    let requested = options.conflate_requested;
    let scope = requested.map(|_| (queue.clone(), group.clone().unwrap_or_default()));
    let req = PopReq {
        queue,
        group,
        batch: batch.max(0) as u32,
        auto_ack,
        wait,
        timeout_ms,
        options,
    };
    // NOTE(WP-1.7c): on an empty claim with `wait`, park on `st.rsm.notifier()`
    // and re-poll (§9.5). The stub never returns Ok, so there is nothing to park
    // on yet.
    // PERF-J: the whole pop handler, pop-only — confirms the empty polling pops
    // are cheap and dominate the mixed `arrival_to_proposed` p50.
    let _t_total = crate::rsm::timing::stamp();
    let resp = match st.rsm.pop_wildcard(ctx, req).await {
        Ok(out) => pop_answer(
            st,
            tenant,
            out,
            scope
                .as_ref()
                .map(|(q, g)| (Some(q.as_str()), q.as_str(), g.as_str(), requested)),
        ),
        Err(e) => err_response(e),
    };
    if let Some(t) = _t_total {
        crate::rsm::timing::metrics()
            .pop_h_total
            .record_dur(t.elapsed());
    }
    resp
}

/// `GET /api/v1/pop/queue/:queue/partition/:partition` (pinned).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn dispatch_pop_partition(
    st: &AppState,
    tenant: &str,
    queue: String,
    partition: String,
    group: Option<String>,
    batch: i32,
    auto_ack: bool,
    wait: bool,
    timeout_ms: u64,
    options: PopOptions,
) -> Response {
    if let Err(e) =
        facade::check_message_key_names(tenant, &queue, group.as_deref(), Some(&partition))
    {
        return err_response(e);
    }
    let ctx = ReqCtx::new(tenant, deadline_for(timeout_ms));
    let requested = options.conflate_requested;
    let scope = requested.map(|_| (queue.clone(), group.clone().unwrap_or_default()));
    let req = PopPinnedReq {
        queue,
        partition,
        group,
        batch: batch.max(0) as u32,
        auto_ack,
        wait,
        timeout_ms,
        options,
    };
    match st.rsm.pop_pinned(ctx, req).await {
        Ok(out) => pop_answer(
            st,
            tenant,
            out,
            scope
                .as_ref()
                .map(|(q, g)| (Some(q.as_str()), q.as_str(), g.as_str(), requested)),
        ),
        Err(e) => err_response(e),
    }
}

/// `GET /api/v1/pop` (namespace/task discovery).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn dispatch_pop_discover(
    st: &AppState,
    tenant: &str,
    namespace: String,
    task: String,
    group: Option<String>,
    batch: i32,
    auto_ack: bool,
    wait: bool,
    timeout_ms: u64,
    options: PopOptions,
) -> Response {
    // The discovery selectors (namespace/task) are not store keys, but the
    // consumer group is; bound it (queue empty — discovery has none in the path).
    if let Err(e) = facade::check_message_key_names(tenant, "", group.as_deref(), None) {
        return err_response(e);
    }
    let ctx = ReqCtx::new(tenant, deadline_for(timeout_ms));
    // Discovery spans queues: the conflict is attributed to the namespace/task
    // pair, with no per-queue counter.
    let requested = options.conflate_requested;
    let scope = requested.map(|_| {
        let star = |s: &str| {
            if s.is_empty() {
                "*".to_string()
            } else {
                s.to_string()
            }
        };
        (
            format!("{}/{}", star(&namespace), star(&task)),
            group.clone().unwrap_or_default(),
        )
    });
    let req = PopDiscoverReq {
        namespace,
        task,
        group,
        batch: batch.max(0) as u32,
        auto_ack,
        wait,
        timeout_ms,
        options,
    };
    match st.rsm.pop_discover(ctx, req).await {
        Ok(out) => pop_answer(
            st,
            tenant,
            out,
            scope
                .as_ref()
                .map(|(s, g)| (None, s.as_str(), g.as_str(), requested)),
        ),
        Err(e) => err_response(e),
    }
}

/// `POST /api/v1/ack` and `POST /api/v1/ack/batch`. The consumer group is the
/// user-supplied name in the composite cursor key, so bound it (R-108); the
/// partition id is a bounded derived value.
pub(crate) async fn dispatch_ack(st: &AppState, tenant: &str, body: axum::body::Bytes) -> Response {
    let group = ack_group(&body);
    if let Err(e) = facade::check_message_key_names(tenant, "", Some(&group), None) {
        return err_response(e);
    }
    let ctx = ReqCtx::new(tenant, deadline_for(st.stmt_timeout.as_millis() as u64));
    let req = AckReq {
        queue: None,
        group,
        raw: body.to_vec(),
    };
    match st.rsm.ack(ctx, req).await {
        Ok(out) => json(StatusCode::OK, out.body),
        Err(e) => err_response(e),
    }
}

/// `POST /api/v1/transaction` (Phase B): the whole bundle to the facade, which
/// renders the SQL wire transaction's body (HTTP 200 on commit AND on rollback).
pub(crate) async fn dispatch_transaction(
    st: &AppState,
    tenant: &str,
    producer_sub: Option<String>,
    body: axum::body::Bytes,
) -> Response {
    let ctx = ReqCtx::new(tenant, deadline_for(st.stmt_timeout.as_millis() as u64))
        .with_producer_sub(producer_sub);
    let req = crate::rsm::facade::TxnReq { raw: body.to_vec() };
    match st.rsm.transaction(ctx, req).await {
        Ok(out) => json(
            StatusCode::from_u16(out.status).unwrap_or(StatusCode::OK),
            out.body,
        ),
        Err(e) => err_response(e),
    }
}

/// Extract the consumer group from an ack body (single or batch) without the
/// private `AckSingle`/`AckBatch` types: read the `consumerGroup` key
/// generically, defaulting to the queue-mode group the handlers use.
fn ack_group(body: &axum::body::Bytes) -> String {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| {
            v.get("consumerGroup")
                .and_then(|x| x.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "__QUEUE_MODE__".to_string())
}

/// `POST /api/v1/lease/:leaseId/extend`.
pub(crate) async fn dispatch_lease_extend(
    st: &AppState,
    tenant: &str,
    lease_id: String,
    seconds: i64,
) -> Response {
    let ctx = ReqCtx::new(tenant, deadline_for(st.stmt_timeout.as_millis() as u64));
    match st.rsm.renew(ctx, RenewReq { lease_id, seconds }).await {
        Ok(out) => json(StatusCode::OK, out.body),
        Err(e) => err_response(e),
    }
}

/// A DLQ-head read on the ack path (kept here so the ack surface's facade calls
/// live together; not yet routed from a handler in WP-1.7a).
#[allow(dead_code)]
pub(crate) async fn dispatch_dlq_head(
    st: &AppState,
    tenant: &str,
    queue: String,
    group: String,
) -> Response {
    if let Err(e) = facade::check_message_key_names(tenant, &queue, Some(&group), None) {
        return err_response(e);
    }
    let ctx = ReqCtx::new(tenant, deadline_for(st.stmt_timeout.as_millis() as u64));
    match st.rsm.dlq_head(ctx, DlqHeadReq { queue, group }).await {
        Ok(out) => match out.body {
            Some(b) => json(StatusCode::OK, b),
            None => json(StatusCode::OK, "{\"message\":null}".to_string()),
        },
        Err(e) => err_response(e),
    }
}

/// Queue depth in raft mode (kept beside the other facade calls; the depth route
/// is a §9.6 local stale read wired in a later WP).
#[allow(dead_code)]
pub(crate) async fn dispatch_depth(
    st: &AppState,
    tenant: &str,
    queue: String,
    group: Option<String>,
) -> Response {
    let ctx = ReqCtx::new(tenant, deadline_for(st.stmt_timeout.as_millis() as u64));
    match st.rsm.depth(ctx, DepthReq { queue, group }).await {
        Ok(out) => json(StatusCode::OK, format!("{{\"pending\":{}}}", out.pending)),
        Err(e) => err_response(e),
    }
}

// ---------------------------------------------------------------------------
// Raft-mode observability handlers (no pool).
// ---------------------------------------------------------------------------

/// `GET /health` in raft mode (§14.1): keeps `status`/`version`/`engine`, adds
/// the `raft` block, and answers `200 healthy` while a leader is known, the
/// apply lag is under the ready threshold and that leader heard from a
/// majority of the voters recently, else `503 settling`. Reads node-local
/// state only.
pub(crate) async fn handle_health(
    axum::extract::State(st): axum::extract::State<Arc<AppState>>,
) -> Response {
    let h = st.rsm.health();
    let (status, label) = if h.ready(st.raft_ready_lag_ms, st.raft_ready_quorum_ms) {
        (StatusCode::OK, "healthy")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "settling")
    };
    let body = format!(
        "{{\"status\":\"{}\",\"engine\":\"raft\",\"version\":\"{}\",\"raft\":{}}}",
        label,
        crate::VERSION,
        h.to_json()
    );
    json(status, body)
}

/// `POST /api/v1/stats/refresh` in raft mode: the §9.6 no-op that answers 200.
/// Counters are maintained at apply (D16), so there is nothing to refresh; every
/// SDK's Admin API calls this, so it must not 404 or 503.
pub(crate) async fn handle_stats_refresh(
    axum::extract::State(_st): axum::extract::State<Arc<AppState>>,
) -> Response {
    json(
        StatusCode::OK,
        "{\"success\":true,\"refreshed\":false,\"engine\":\"raft\"}".to_string(),
    )
}

/// `GET /metrics/prometheus`: the in-process families plus the state machine's.
pub(crate) async fn handle_prometheus(
    axum::extract::State(st): axum::extract::State<Arc<AppState>>,
) -> Response {
    let mut body = st.metrics.prometheus();
    // PERF-1 (O18): the node-local rsm timing histograms and counters. Empty
    // (all-zero) until the pipeline has done work, and skipped entirely when
    // QUEEN_RAFT_METRICS is off.
    crate::rsm::timing::render_prometheus(&mut body);
    body.push_str(&st.rsm.prometheus());
    // The in-process S3 sink's `queen_s3_*` families (s3_inproc.rs), when it
    // runs: per node, like every family above.
    #[cfg(feature = "s3")]
    if let Some(sink) = crate::s3_inproc::prometheus_text() {
        body.push_str(&sink);
    }
    // The in-process Postgres connectors' `queen_pg_*` families (pg_inproc.rs),
    // where they run: per node, like every family above.
    #[cfg(feature = "pg")]
    if let Some(pg) = crate::pg_inproc::prometheus_text() {
        body.push_str(&pg);
    }
    let conflated = crate::rsm::dashboard::collector::last_conflated();
    if !conflated.is_empty() {
        body.push_str(
            "# HELP queen_queue_conflated_per_minute Messages conflated away per queue in the last metrics bucket (METRICS_FLUSH_MS)\n# TYPE queen_queue_conflated_per_minute gauge\n",
        );
        for (tenant, queue, n) in conflated {
            let queue = crate::metrics::escape_label(&queue);
            if tenant == crate::config::DEFAULT_TENANT {
                body.push_str(&format!(
                    "queen_queue_conflated_per_minute{{queue=\"{queue}\"}} {n}\n"
                ));
            } else {
                let tenant = crate::metrics::escape_label(&tenant);
                body.push_str(&format!(
                    "queen_queue_conflated_per_minute{{tenant=\"{tenant}\",queue=\"{queue}\"}} {n}\n"
                ));
            }
        }
    }
    // The two families channel-go's queue alerts read, under the names and
    // labels the Postgres engine exported: pop lag per queue as this process
    // measured it at delivery over the last metrics bucket, and the dead-letter
    // depth per queue from the leader.
    let lag = crate::rsm::dashboard::collector::last_queue_lag();
    if !lag.is_empty() {
        body.push_str(
            "# HELP queen_queue_pop_lag_milliseconds Per-queue pop lag (delivery time minus creation time) over the last metrics bucket (METRICS_FLUSH_MS), on this process\n# TYPE queen_queue_pop_lag_milliseconds gauge\n",
        );
        for (tenant, queue, avg, max) in lag {
            let labels = queue_labels(&tenant, &queue);
            body.push_str(&format!(
                "queen_queue_pop_lag_milliseconds{{{labels},stat=\"avg\"}} {avg}\nqueen_queue_pop_lag_milliseconds{{{labels},stat=\"max\"}} {max}\n"
            ));
        }
    }
    let dlq = st.rsm.dlq_depth_by_queue();
    if !dlq.is_empty() {
        body.push_str(
            "# HELP queen_dlq_depth_by_queue Dead letters per queue (exported by the raft leader only)\n# TYPE queen_dlq_depth_by_queue gauge\n",
        );
        for (tenant, queue, n) in dlq {
            let labels = queue_labels(&tenant, &queue);
            body.push_str(&format!("queen_dlq_depth_by_queue{{{labels}}} {n}\n"));
        }
    }
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

/// `queue="…"`, with `tenant="…"` in front for any tenant but the default one
/// (the shape `queen_queue_conflated_per_minute` has).
fn queue_labels(tenant: &str, queue: &str) -> String {
    let queue = crate::metrics::escape_label(queue);
    if tenant == crate::config::DEFAULT_TENANT {
        format!("queue=\"{queue}\"")
    } else {
        format!(
            "tenant=\"{}\",queue=\"{queue}\"",
            crate::metrics::escape_label(tenant)
        )
    }
}

/// `GET /metrics`. Process metrics are local; state-machine role/readiness
/// lives in `raft` (§14.6).
pub(crate) async fn handle_metrics(
    axum::extract::State(st): axum::extract::State<Arc<AppState>>,
) -> Response {
    let snap = st.metrics.snapshot();
    let health = st.rsm.health();
    let out = serde_json::json!({
        "uptime": st.metrics.uptime_seconds(),
        "requests": {
            "total": snap.push_requests + snap.pop_requests + snap.ack_requests,
            "rate": 0,
        },
        "messages": {
            "total": snap.push_messages + snap.pop_messages + snap.ack_messages,
            "rate": 0,
        },
        "memory": {
            "rss": st.metrics.resident_bytes(),
            "heapTotal": 0, "heapUsed": 0, "external": 0, "arrayBuffers": 0,
        },
        "cpu": { "user": 0, "system": 0 },
        "engine": "raft",
        "raft": {
            "role": health.role,
            "leader": health.leader_known,
            "term": health.term,
            "applied": health.applied,
            "commit": health.commit,
            "lag": health.lag_ms,
            "quorumAckMs": health.quorum_ack_ms,
            "storageReady": health.storage_ready,
        }
    });
    json(StatusCode::OK, out.to_string())
}

// ---------------------------------------------------------------------------
// Raft membership, for an operator (`/api/v1/system/raft/membership`).
// ---------------------------------------------------------------------------
//
// The routes sit under `/api/v1/system/`, the operator family: `auth` gives
// them the Admin level, the proxy never lets a tenant through to them, and a
// request that names a tenant (`x-queen-tenant`) is refused here — the
// membership is the cell's, not a tenant's. Every node answers them: a
// follower forwards a change to the leader over the Raft RPC port
// (`replicator/raft/admin.rs`), and with no leader known answers the usual
// `503 retry` with the leader hint when there is one.
//
// | route | body | answer |
// |---|---|---|
// | `GET /api/v1/system/raft/membership` | — | `{"engine","membership":{...}}` |
// | `POST .../learners` | `{"id","raft","http"}` | `{"ok":true,"membership"}` |
// | `POST .../promote` | `{"ids":[..],"force"?}` | same |
// | `PUT .../voters` | `{"voters":[..],"force"?}` | same |
// | `DELETE .../members/:id` | — | same |
//
// `?timeoutMs=` bounds a change (default 30 s): past it the answer is `504
// timeout` and the change may still complete. A refused change is `409` (or
// `400` for a malformed one) with a stable `code`: `in_flight`, `no_quorum`,
// `last_voter`, `learner_behind`, `already_voter`, `address_mismatch`,
// `membership_changed`, `leader_changed`, `not_a_learner`, `not_a_member`,
// `bad_request`; `503 leader_unreachable` when the leader did not answer.

/// A membership change's deadline: `?timeoutMs=` (1 ms to 120 s, default 30 s).
#[cfg(feature = "server")]
fn membership_deadline(q: &std::collections::HashMap<String, String>) -> Deadline {
    let ms = q
        .get("timeoutMs")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(30_000)
        .clamp(1, 120_000);
    Deadline::after(std::time::Duration::from_millis(ms))
}

/// The membership is the cell's: a request scoped to a tenant is refused.
#[cfg(feature = "server")]
fn membership_operator_only(tenant: &crate::tenant::Tenant) -> Option<Response> {
    (tenant.as_str() != crate::config::DEFAULT_TENANT).then(|| {
        json(
            StatusCode::FORBIDDEN,
            serde_json::json!({
                "ok": false,
                "code": "forbidden",
                "error": "the raft membership is cell-wide: only an operator request (no \
                          x-queen-tenant) may read or change it"
            })
            .to_string(),
        )
    })
}

#[cfg(feature = "server")]
fn api_out(out: crate::rsm::facade::ApiOut) -> Response {
    let status = StatusCode::from_u16(out.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, [(header::CONTENT_TYPE, out.content_type)], out.body).into_response()
}

#[cfg(feature = "server")]
fn membership_bad_request(msg: impl Into<String>) -> Response {
    json(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"ok": false, "code": "bad_request", "error": msg.into()}).to_string(),
    )
}

#[cfg(feature = "server")]
async fn membership_change(
    st: &AppState,
    tenant: &crate::tenant::Tenant,
    q: &std::collections::HashMap<String, String>,
    change: crate::rsm::replicator::MembershipChange,
) -> Response {
    if let Some(r) = membership_operator_only(tenant) {
        return r;
    }
    let ctx = ReqCtx::new(tenant.as_str(), membership_deadline(q));
    match st.rsm.raft_change_membership(ctx, change).await {
        Ok(out) => api_out(out),
        Err(e) => err_response(e),
    }
}

/// The JSON body of a membership change (an empty body is `{}`).
#[cfg(feature = "server")]
#[allow(clippy::result_large_err)]
fn membership_body(body: &axum::body::Bytes) -> Result<serde_json::Value, Response> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(serde_json::json!({}));
    }
    serde_json::from_slice(body).map_err(|e| membership_bad_request(format!("body: {e}")))
}

/// `force` from the body or `?force=true`.
#[cfg(feature = "server")]
fn membership_force(v: &serde_json::Value, q: &std::collections::HashMap<String, String>) -> bool {
    v.get("force")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
        || q.get("force").is_some_and(|f| f == "true" || f == "1")
}

/// A list of node ids from `field` (or a single `one`).
#[cfg(feature = "server")]
fn membership_ids(v: &serde_json::Value, field: &str, one: &str) -> Option<Vec<u64>> {
    if let Some(a) = v.get(field).and_then(serde_json::Value::as_array) {
        return a.iter().map(serde_json::Value::as_u64).collect();
    }
    v.get(one)
        .and_then(serde_json::Value::as_u64)
        .map(|id| vec![id])
}

/// `GET /api/v1/system/raft/membership`.
#[cfg(feature = "server")]
pub(crate) async fn handle_membership_get(
    axum::extract::State(st): axum::extract::State<Arc<AppState>>,
    axum::extract::Extension(tenant): axum::extract::Extension<crate::tenant::Tenant>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    if let Some(r) = membership_operator_only(&tenant) {
        return r;
    }
    let ctx = ReqCtx::new(tenant.as_str(), membership_deadline(&q));
    match st.rsm.raft_membership(ctx).await {
        Ok(out) => api_out(out),
        Err(e) => err_response(e),
    }
}

/// `POST /api/v1/system/raft/membership/learners` `{"id", "raft", "http"}`.
#[cfg(feature = "server")]
pub(crate) async fn handle_membership_add_learner(
    axum::extract::State(st): axum::extract::State<Arc<AppState>>,
    axum::extract::Extension(tenant): axum::extract::Extension<crate::tenant::Tenant>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Response {
    let v = match membership_body(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let id = v
        .get("id")
        .or_else(|| v.get("nodeId"))
        .and_then(serde_json::Value::as_u64);
    let raft = v.get("raft").and_then(serde_json::Value::as_str);
    let http = v.get("http").and_then(serde_json::Value::as_str);
    let (Some(node), Some(raft), Some(http)) = (id, raft, http) else {
        return membership_bad_request(
            "expected {\"id\": <node id>, \"raft\": \"host:port\", \"http\": \"host:port\"}",
        );
    };
    let change = crate::rsm::replicator::MembershipChange::AddLearner {
        node,
        raft: raft.to_string(),
        http: http.to_string(),
    };
    membership_change(&st, &tenant, &q, change).await
}

/// `POST /api/v1/system/raft/membership/promote` `{"ids": [..], "force"?}`.
#[cfg(feature = "server")]
pub(crate) async fn handle_membership_promote(
    axum::extract::State(st): axum::extract::State<Arc<AppState>>,
    axum::extract::Extension(tenant): axum::extract::Extension<crate::tenant::Tenant>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Response {
    let v = match membership_body(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let Some(nodes) = membership_ids(&v, "ids", "id").filter(|n| !n.is_empty()) else {
        return membership_bad_request("expected {\"ids\": [<learner id>, ...], \"force\"?: bool}");
    };
    let force = membership_force(&v, &q);
    let change = crate::rsm::replicator::MembershipChange::Promote { nodes, force };
    membership_change(&st, &tenant, &q, change).await
}

/// `PUT /api/v1/system/raft/membership/voters` `{"voters": [..], "force"?}`.
#[cfg(feature = "server")]
pub(crate) async fn handle_membership_set_voters(
    axum::extract::State(st): axum::extract::State<Arc<AppState>>,
    axum::extract::Extension(tenant): axum::extract::Extension<crate::tenant::Tenant>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Response {
    let v = match membership_body(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let Some(voters) = membership_ids(&v, "voters", "voter") else {
        return membership_bad_request("expected {\"voters\": [<node id>, ...], \"force\"?: bool}");
    };
    let force = membership_force(&v, &q);
    let change = crate::rsm::replicator::MembershipChange::SetVoters { voters, force };
    membership_change(&st, &tenant, &q, change).await
}

/// `DELETE /api/v1/system/raft/membership/members/:id`.
#[cfg(feature = "server")]
pub(crate) async fn handle_membership_remove(
    axum::extract::State(st): axum::extract::State<Arc<AppState>>,
    axum::extract::Extension(tenant): axum::extract::Extension<crate::tenant::Tenant>,
    axum::extract::Path(id): axum::extract::Path<String>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Ok(node) = id.parse::<u64>() else {
        return membership_bad_request(format!("`{id}` is not a node id"));
    };
    let change = crate::rsm::replicator::MembershipChange::Remove { node };
    membership_change(&st, &tenant, &q, change).await
}

// ---------------------------------------------------------------------------
// Composition roots.
// ---------------------------------------------------------------------------

/// Build the broker's [`AppState`] around the state machine facade.
///
/// Shared by `main.rs`, `embedded/boot.rs` and the seam test so the state
/// cannot drift between them.
pub(crate) fn build_raft_state(cfg: &Config) -> Result<Arc<AppState>, String> {
    build_raft_state_with(cfg, None)
}

/// [`build_raft_state`] with the facade supplied by the caller instead of the
/// builder hook — the handler tests' seam: they drive the REAL handlers over a
/// real `RaftFacade` without registering the process-global builder (which
/// would change what the WP-1.7a seam tests get).
pub(crate) fn build_raft_state_with(
    cfg: &Config,
    rsm_override: Option<Arc<dyn facade::Rsm>>,
) -> Result<Arc<AppState>, String> {
    let metrics = Arc::new(crate::metrics::Metrics::new());
    // The facade counts through the global handle, and the dashboard
    // collector (rsm/dashboard/collector.rs) flushes it.
    crate::metrics::install_global(metrics.clone());
    let notifier = crate::notify::Notifier::new(cfg.tenancy_header);

    // The facade: the real state machine when WP-1.7c has registered its
    // builder, the NotReady stub otherwise — or the caller's own.
    let rsm = match rsm_override {
        Some(r) => r,
        None => facade::build(&facade::RsmBuildCtx {
            data_dir: cfg.raft_dir.clone(),
            notifier: notifier.clone(),
            disk_high_pct: cfg.raft_disk_high_pct,
            disk_low_pct: cfg.raft_disk_low_pct,
        }),
    };
    let bootstrap = rsm.bootstrap();
    if let Some(error) = bootstrap.startup_error.as_deref() {
        return Err(format!("raft bootstrap state: {error}"));
    }

    let ephemeral = crate::ephemeral::Ephemeral::new(
        crate::ephemeral::Knobs {
            global_max_bytes: cfg.ephemeral_max_bytes,
            queue_max_bytes: cfg.ephemeral_queue_max_bytes,
            queue_max_length: cfg.ephemeral_queue_max_length,
            lease_ms: cfg.ephemeral_lease_s * 1000,
            retry_limit: cfg.ephemeral_retry_limit,
            implicit_idle_ms: cfg.ephemeral_implicit_idle_s * 1000,
            require_grant: cfg.ephemeral_require_grant,
            rate: cfg.ephemeral_rate,
            burst: cfg.ephemeral_burst,
            max_tenants: cfg.kv_max_tenants,
        },
        metrics.clone(),
    );
    ephemeral.apply_grants(
        bootstrap
            .ephemeral_grants
            .iter()
            .map(|(tenant, grant)| crate::ephemeral::Grant {
                tenant: tenant.clone(),
                enabled: grant.enabled,
                max_bytes: grant.max_bytes,
                max_queues: grant.max_queues.map(i64::from),
                max_msgs_per_sec: grant.max_msgs_per_sec.map(i64::from),
            })
            .collect(),
    );
    for (tenant, queue, options) in &bootstrap.ephemeral_configs {
        ephemeral.set_config(
            tenant,
            queue,
            crate::ephemeral::parse_stored_options(options),
            true,
        );
    }
    let switches = crate::switches::Switches::new();
    switches.set_kv(bootstrap.kv_enabled);
    switches.set_timers_schedule(bootstrap.timers_schedule_enabled);
    switches.set_timers_fire(bootstrap.timers_fire_enabled);
    switches.set_ephemeral(bootstrap.ephemeral_enabled);

    let quota = crate::quota::from_config(cfg);
    quota.refresh(
        bootstrap
            .kv_grants
            .iter()
            .map(
                |(tenant, grant, kv_rows, kv_bytes, timer_rows)| crate::quota::TenantRow {
                    tenant: tenant.clone(),
                    limits: Some(quota_limits(grant)),
                    measure: crate::quota::Measure {
                        kv_rows: *kv_rows,
                        kv_bytes: *kv_bytes,
                        timer_rows: *timer_rows,
                        computed_at_ms: crate::util::now_epoch_ms(),
                    },
                },
            )
            .collect(),
    );

    Ok(Arc::new(AppState {
        metrics,
        stmt_timeout: cfg.stmt_timeout,
        pop_default_timeout_ms: cfg.pop_default_timeout_ms,
        default_subscription_mode: cfg.default_subscription_mode.clone(),
        notifier,
        ephemeral,
        peers: Arc::new(crate::peerclient::PeerClient::new()),
        tenancy_enabled: cfg.tenancy_header,
        quota,
        switches,
        auth_enabled: cfg.auth.enabled,
        server_id: cfg.server_id.clone(),
        rsm,
        raft_ready_lag_ms: cfg.raft_ready_lag_ms,
        raft_ready_quorum_ms: cfg.raft_ready_quorum_ms,
    }))
}

/// The raft-mode router (server target only). Ported routes go to the real
/// handlers — the message-path ones branch to the facade via their
/// storage-aware guard — and the remaining `/api` or `/streams` routes go
/// through the generic Phase-2 adapter. Auth and tenancy layers wrap them all.
#[cfg(feature = "server")]
pub(crate) fn build_raft_router(
    state: Arc<AppState>,
    authenticator: Arc<crate::auth::Authenticator>,
    tenancy_header: bool,
) -> axum::Router {
    use axum::routing::{get, post};

    let routes = axum::Router::new()
        // ------------------------------------------------ message path → facade
        .route(
            "/api/v1/push",
            post(super::handle_push).layer(axum::middleware::from_fn(admit_edge)),
        )
        .route("/api/v1/pop", get(super::handle_pop_discover))
        .route("/api/v1/pop/queue/:queue", get(super::handle_pop))
        .route(
            "/api/v1/pop/queue/:queue/partition/:partition",
            get(super::handle_pop_partition),
        )
        .route("/api/v1/ack", post(super::handle_ack))
        .route("/api/v1/ack/batch", post(super::handle_ack_batch))
        // Phase B: the wire transaction → one facade command, all-or-nothing.
        .route(
            "/api/v1/transaction",
            post(super::handle_transaction).layer(axum::middleware::from_fn(admit_edge)),
        )
        .route(
            "/api/v1/lease/:leaseId/extend",
            post(super::handle_lease_extend),
        )
        // --------------------------------------------- kv → facade (WP-2.2)
        // Each handler dispatches to the facade after the shared edge ceilings
        // and ladder. The static console paths are under `/api/v1/resources`, and
        // no literal segment may ever sit under `/api/v1/kv/:ns/`.
        .route("/api/v1/kv", post(super::handle_kv_batch))
        .route(
            "/api/v1/kv/:ns/*key",
            get(super::handle_kv_get)
                .put(super::handle_kv_put)
                .delete(super::handle_kv_delete),
        )
        .route(
            "/api/v1/resources/kv/namespaces",
            get(super::handle_kv_namespaces),
        )
        .route("/api/v1/resources/kv/list", post(super::handle_kv_list))
        // ------------------------------------------------- locks → kv → facade
        // A lock and a semaphore, as leases stored in KV: the one route, whose
        // handler turns each operation into the KV calls above (`locks.rs`).
        .route("/api/v1/locks", post(super::handle_locks_batch))
        // ------------------------------------ timers (WP-2.3) → facade, 025's wire
        // The four timer routes; the handlers dispatch to the state machine.
        .route("/api/v1/timers", post(super::handle_timers_batch))
        .route("/api/v1/timers/:queue", get(super::handle_timers_list))
        .route(
            "/api/v1/timers/:queue/*timerKey",
            get(super::handle_timer_peek).delete(super::handle_timer_cancel),
        )
        // ---------------------------------------------- observability (no pool)
        .route("/health", get(handle_health))
        .route("/metrics", get(handle_metrics))
        .route("/metrics/prometheus", get(handle_prometheus))
        .route("/status", get(super::handle_status))
        .route("/api/v1/stats/refresh", post(handle_stats_refresh))
        // ------------------------ raft membership (operator: Admin, no tenant)
        .route("/api/v1/system/raft/membership", get(handle_membership_get))
        .route(
            "/api/v1/system/raft/membership/learners",
            post(handle_membership_add_learner),
        )
        .route(
            "/api/v1/system/raft/membership/promote",
            post(handle_membership_promote),
        )
        .route(
            "/api/v1/system/raft/membership/voters",
            axum::routing::put(handle_membership_set_voters),
        )
        .route(
            "/api/v1/system/raft/membership/members/:id",
            axum::routing::delete(handle_membership_remove),
        )
        // --------------------------------------------------- broker identity
        .route("/auth/me", get(super::handle_auth_me))
        .route("/auth/login", get(super::handle_auth_login))
        .route("/auth/logout", post(super::handle_auth_logout))
        // --------------------------------------- ephemeral RAM verbs (D24, §14.8)
        .route("/api/v1/ephemeral/push", post(super::handle_ephemeral_push))
        .route("/api/v1/ephemeral/pop", get(super::handle_ephemeral_pop))
        .route("/api/v1/ephemeral/ack", post(super::handle_ephemeral_ack))
        .route(
            "/api/v1/ephemeral/reset",
            post(super::handle_ephemeral_reset),
        )
        .route(
            "/api/v1/ephemeral/queues",
            get(super::handle_ephemeral_queues),
        )
        .route(
            "/api/v1/ephemeral/queues/:queue/depth",
            get(super::handle_ephemeral_depth),
        )
        .route(
            "/api/v1/ephemeral/queue/:queue",
            axum::routing::delete(super::handle_ephemeral_delete_queue),
        )
        // Broker to broker (§3.7 across nodes): a ring hand-over, a drain
        // notice and the readiness probe. Behind auth and tenancy like every
        // route — auth takes the cluster token for these three in place of a
        // JWT (`auth::is_peer_call`) — and the handlers also require the
        // forward mark (and the cluster token when one is set).
        .route(
            "/api/v1/ephemeral/_adopt",
            post(super::handle_ephemeral_adopt),
        )
        .route(
            "/api/v1/ephemeral/_leaving",
            post(super::handle_ephemeral_leaving),
        )
        .route(
            "/api/v1/ephemeral/_ready",
            get(super::handle_ephemeral_ready),
        );
    // ------------------- Postgres connectors (PLAN_PG_CONNECTORS.md §3.1)
    // The documents every node's manager runs (pg_inproc.rs), stored in the
    // broker-internal tenant. A document is small: the body limit here, not
    // the message path's 64 MiB, so a PUT cannot park megabytes in memory.
    #[cfg(feature = "pg")]
    let routes = routes
        .route("/api/v1/connectors", get(super::handle_connectors_list))
        .route(
            "/api/v1/connectors/:name",
            get(super::handle_connector_get)
                .put(super::handle_connector_put)
                .delete(super::handle_connector_delete)
                .layer(axum::extract::DefaultBodyLimit::max(
                    super::connectors::MAX_BODY_BYTES,
                )),
        )
        .route(
            "/api/v1/connectors/:name/resync",
            post(super::handle_connector_resync),
        );
    routes
        // Phase-2 /api and /streams are served by the generic RSM facade;
        // everything else falls through to the SPA/static handler.
        .fallback(raft_fallback)
        .layer(axum::extract::DefaultBodyLimit::max(
            std::env::var("QUEEN_MAX_BODY_BYTES")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(64 * 1024 * 1024),
        ))
        // Inside tenancy: it reads the tenant the layer below resolved.
        .layer(axum::middleware::from_fn(refuse_reserved_tenant))
        .layer(axum::middleware::from_fn_with_state(
            crate::tenant::TenancyConfig {
                enabled: tenancy_header,
            },
            crate::tenant::tenant_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            authenticator,
            crate::auth::auth_middleware,
        ))
        // Outermost: a follower hands the request to the leader untouched,
        // before auth or tenancy spend anything on it.
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            forward_to_leader,
        ))
        .with_state(state)
}

/// Push admission at the HTTP edge ([`crate::rsm::admit`]): the permits are
/// sized by `Content-Length` and taken BEFORE the body is read, so a push that
/// waits for room holds only its connection — its bytes stay in the socket and
/// TCP slows the sender. Inside auth and tenancy (an unauthenticated request
/// never takes budget); the facade sees the request as already admitted.
///
/// On the leader these are this node's clients' share of the planner's budget
/// ([`crate::rsm::admit::Source::Local`], served in turn with each
/// follower's). On a follower they only guard its memory: the leader admits
/// the prepared commands, and its overload comes back within a short hold as
/// this request's `429` with a `Retry-After`, so a follower's permits are
/// never held for seconds while the leader is saturated.
#[cfg(feature = "server")]
async fn admit_edge(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let Some(gate) = crate::rsm::admit::global() else {
        return next.run(req).await;
    };
    let bytes = req
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(crate::rsm::admit::UNKNOWN_LEN_BYTES);
    match gate.admit(bytes).await {
        Ok(_admitted) => crate::rsm::admit::pre_admitted_scope(next.run(req)).await,
        Err(o) => err_response(RsmError::Overloaded {
            retry_after_s: o.retry_after_s,
        }),
    }
}

/// No request is served as a reserved tenant ([`crate::config::is_reserved_tenant`]):
/// the proxy's state and the broker's own documents are written and read
/// in-process by their owners, and nothing that arrives over HTTP is one of them.
/// Runs after the tenant middleware, on whatever tenant it resolved — a client
/// header on a router with tenancy on, a relay another broker proved — so every
/// route is covered, the KV and the message path included, on every router.
#[cfg(feature = "server")]
async fn refuse_reserved_tenant(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let reserved = req
        .extensions()
        .get::<crate::tenant::Tenant>()
        .is_some_and(|t| crate::config::is_reserved_tenant(t.as_str()));
    if reserved {
        return (
            StatusCode::FORBIDDEN,
            [(header::CONTENT_TYPE, "application/json")],
            "{\"error\":\"this tenant is reserved: no route acts on it\",\"code\":\"reserved_tenant\"}",
        )
            .into_response();
    }
    next.run(req).await
}

/// Marks a request a follower already forwarded: it is served where it lands.
#[cfg(feature = "server")]
pub(crate) const FORWARDED_HEADER: &str = "x-queen-forwarded";

/// How long a forwarded request may take at the leader (a long-poll pop is
/// capped at 60 s there).
#[cfg(feature = "server")]
const FORWARD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// A follower of a raft cluster forwards every `/api/` and `/streams/` request
/// to the leader, so a client may talk to any node. A request that was already
/// forwarded once is served here whatever the role (the facade then answers a
/// retry naming the leader), so no loop can form while nodes disagree on who
/// leads. With no leader known the answer is a 503 the client retries.
///
/// `/api/v1/raft/` is the exception: those routes describe THE NODE ASKED
/// (its id, its role, the members view it holds), so a follower answers them
/// itself — forwarded, a follower's `nodeId` would be the leader's.
#[cfg(feature = "server")]
async fn forward_to_leader(
    axum::extract::State(st): axum::extract::State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = req.uri().path();
    // The Raft view describes THIS node and gathers its peers itself: it must
    // answer where it lands, above all while no leader is known.
    let forwardable =
        (path.starts_with("/api/") && !is_node_local(path)) || path.starts_with("/streams/");
    if !forwardable || req.headers().contains_key(FORWARDED_HEADER) {
        return next.run(req).await;
    }
    match st.rsm.route() {
        facade::Route::Local => next.run(req).await,
        facade::Route::NoLeader => err_response(RsmError::NoLeader),
        facade::Route::Leader(addr) => forward(&addr, req).await,
    }
}

/// The `/api/` routes a follower answers itself: the ones about the node.
#[cfg(feature = "server")]
fn is_node_local(path: &str) -> bool {
    path.starts_with("/api/v1/raft/")
        // The ephemeral rings are node RAM and place themselves (§3.7): only
        // the two verbs that write the replicated declaration go to the leader.
        || (path.starts_with("/api/v1/ephemeral/")
            && path != "/api/v1/ephemeral/configure"
            && !path.starts_with("/api/v1/ephemeral/queue/"))
}

#[cfg(feature = "server")]
fn is_hop_by_hop(name: &header::HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "host"
    )
}

/// Relay `req` to the leader at `addr` and its answer back, both streamed.
#[cfg(feature = "server")]
async fn forward(addr: &str, req: axum::extract::Request) -> Response {
    use std::sync::OnceLock;
    type Client = hyper_util::client::legacy::Client<
        hyper_util::client::legacy::connect::HttpConnector,
        axum::body::Body,
    >;
    static CLIENT: OnceLock<Client> = OnceLock::new();
    let client = CLIENT.get_or_init(|| {
        let mut c = hyper_util::client::legacy::connect::HttpConnector::new();
        c.set_nodelay(true);
        c.set_connect_timeout(Some(std::time::Duration::from_secs(2)));
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .pool_max_idle_per_host(256)
            .build::<_, axum::body::Body>(c)
    });
    let unavailable = |why: String| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [
                (header::CONTENT_TYPE, "application/json".to_string()),
                (header::RETRY_AFTER, "1".to_string()),
            ],
            serde_json::json!({ "error": why, "code": "retry" }).to_string(),
        )
            .into_response()
    };
    let (mut parts, body) = req.into_parts();
    let pq = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "/".into());
    parts.uri = match format!("http://{addr}{pq}").parse() {
        Ok(u) => u,
        Err(e) => return unavailable(format!("leader address {addr}: {e}")),
    };
    parts.version = axum::http::Version::HTTP_11;
    let names: Vec<header::HeaderName> = parts
        .headers
        .keys()
        .filter(|n| is_hop_by_hop(n))
        .cloned()
        .collect();
    for n in names {
        parts.headers.remove(&n);
    }
    parts
        .headers
        .insert(FORWARDED_HEADER, axum::http::HeaderValue::from_static("1"));
    let upstream = axum::http::Request::from_parts(parts, body);
    match tokio::time::timeout(FORWARD_TIMEOUT, client.request(upstream)).await {
        Ok(Ok(resp)) => {
            let (mut parts, body) = resp.into_parts();
            let names: Vec<header::HeaderName> = parts
                .headers
                .keys()
                .filter(|n| is_hop_by_hop(n))
                .cloned()
                .collect();
            for n in names {
                parts.headers.remove(&n);
            }
            Response::from_parts(parts, axum::body::Body::new(body))
        }
        Ok(Err(e)) => unavailable(format!("the leader at {addr} is unreachable: {e}")),
        Err(_) => unavailable(format!("the leader at {addr} did not answer in time")),
    }
}

/// The fallback for the raft router: `/api/` and `/streams/` are delegated to
/// the generic Phase-2 facade, which returns the endpoint's normal response or
/// a JSON 404. Anything else is the dashboard SPA / static assets.
#[cfg(feature = "server")]
async fn raft_fallback(
    axum::extract::State(st): axum::extract::State<Arc<AppState>>,
    axum::extract::Extension(tenant): axum::extract::Extension<crate::tenant::Tenant>,
    method: Method,
    uri: Uri,
    body: axum::body::Bytes,
) -> Response {
    let path = uri.path();
    if path.starts_with("/api/") || path.starts_with("/streams/") {
        return dispatch_api(
            &st,
            tenant.as_str(),
            method.as_str(),
            path,
            uri.query(),
            body,
        )
        .await;
    }
    super::handle_static(method, uri).await
}

/// Shared Phase-2 HTTP/embedded adapter. The raft fallback and the handlers the
/// embedded API calls both use this path.
pub(crate) async fn dispatch_api(
    st: &Arc<AppState>,
    tenant: &str,
    method: &str,
    path: &str,
    query: Option<&str>,
    body: axum::body::Bytes,
) -> Response {
    let ctx = ReqCtx::new(tenant, deadline_for(st.pop_default_timeout_ms));
    let req = crate::rsm::facade::ApiReq {
        method: method.to_string(),
        path: path.to_string(),
        query: query.map(str::to_string),
        body: body.to_vec(),
    };
    match st.rsm.api(ctx, req).await {
        Ok(out) => {
            let status =
                StatusCode::from_u16(out.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            if status.is_success() {
                #[cfg(feature = "server")]
                apply_local_control(st, method, path, tenant, &body, &out.body);
            }
            (status, [(header::CONTENT_TYPE, out.content_type)], out.body).into_response()
        }
        Err(e) => err_response(e),
    }
}

pub(crate) fn query_string(params: &std::collections::HashMap<String, String>) -> String {
    let mut pairs: Vec<_> = params.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{}={}", percent_encode(key), percent_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Mirrors a successful control call into this node's in-memory state. `answer`
/// is the state machine's success body: a purge or a quota grant names the
/// tenant it acted on there, and the mirror touches exactly that tenant — never
/// one it re-reads out of the request by rules of its own.
#[cfg(feature = "server")]
fn apply_local_control(
    st: &AppState,
    method: &str,
    path: &str,
    tenant: &str,
    body: &[u8],
    answer: &str,
) {
    let parsed = || serde_json::from_slice::<serde_json::Value>(body).ok();
    match (method, path) {
        ("POST", "/api/v1/system/kv-timers") => {
            if let Some(v) = parsed() {
                if let Some(x) = v.get("kv").and_then(|x| x.as_bool()) {
                    st.switches.set_kv(x);
                }
                if let Some(x) = v.get("timersSchedule").and_then(|x| x.as_bool()) {
                    st.switches.set_timers_schedule(x);
                }
                if let Some(x) = v.get("timersFire").and_then(|x| x.as_bool()) {
                    st.switches.set_timers_fire(x);
                }
            }
        }
        ("POST", "/api/v1/system/ephemeral") => {
            if let Some(v) = parsed().and_then(|v| v.get("enabled").and_then(|x| x.as_bool())) {
                st.switches.set_ephemeral(v);
            }
        }
        ("POST", "/api/v1/ephemeral/configure") => {
            if let Some(v) = parsed() {
                if let Some(queue) = v.get("queue").and_then(|x| x.as_str()) {
                    let options = v
                        .get("options")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({}));
                    st.ephemeral.set_config(
                        tenant,
                        queue,
                        crate::ephemeral::parse_stored_options(&options),
                        true,
                    );
                }
            }
        }
        ("POST", "/api/v1/resources/quota")
        | ("POST", "/api/v1/system/quota")
        | ("POST", "/api/v1/system/quotas") => {
            if let (Some(v), Some(target)) = (parsed(), answered_tenant(answer)) {
                let grant = quota_grant_from_json(&v);
                match v.get("kind").and_then(|x| x.as_str()) {
                    Some("kv") => st.quota.upsert_limits(&target, quota_limits(&grant)),
                    Some("ephemeral") => st.ephemeral.upsert_grant(crate::ephemeral::Grant {
                        tenant: target,
                        enabled: grant.enabled,
                        max_bytes: grant.max_bytes,
                        max_queues: grant.max_queues.map(i64::from),
                        max_msgs_per_sec: grant.max_msgs_per_sec.map(i64::from),
                    }),
                    _ => {}
                }
            }
        }
        ("DELETE", "/api/v1/resources/tenant") => {
            if let Some(target) = answered_tenant(answer) {
                st.quota.remove_tenant(&target);
                st.ephemeral.remove_tenant(&target);
            }
        }
        ("DELETE", p) if p.starts_with("/api/v1/ephemeral/queue/") => {
            let queue = &p["/api/v1/ephemeral/queue/".len()..];
            st.ephemeral.remove(tenant, queue);
        }
        _ => {}
    }
}

/// The `tenant` a successful purge or quota grant answered with.
#[cfg(feature = "server")]
fn answered_tenant(answer: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(answer)
        .ok()?
        .get("tenant")?
        .as_str()
        .map(str::to_string)
}

pub(crate) fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            use std::fmt::Write;
            let _ = write!(&mut out, "%{byte:02X}");
        }
    }
    out
}

fn quota_limits(grant: &crate::rsm::effect::QuotaGrant) -> crate::quota::Limits {
    crate::quota::Limits {
        enabled: grant.enabled,
        max_rows: grant.max_rows,
        max_bytes: grant.max_bytes,
        max_timers: grant.max_timers,
        max_timer_horizon_s: grant.max_timer_horizon_s,
        max_reads_per_sec: grant.max_reads_per_sec.and_then(|n| u32::try_from(n).ok()),
        max_writes_per_sec: grant.max_writes_per_sec.and_then(|n| u32::try_from(n).ok()),
    }
}

fn quota_grant_from_json(v: &serde_json::Value) -> crate::rsm::effect::QuotaGrant {
    let i64v = |camel: &str, snake: &str| {
        v.get(camel)
            .or_else(|| v.get(snake))
            .and_then(serde_json::Value::as_i64)
    };
    crate::rsm::effect::QuotaGrant {
        enabled: v.get("enabled").and_then(|x| x.as_bool()).unwrap_or(true),
        max_rows: i64v("maxRows", "max_rows"),
        max_bytes: i64v("maxBytes", "max_bytes"),
        max_timers: i64v("maxTimers", "max_timers"),
        max_timer_horizon_s: i64v("maxTimerHorizonSeconds", "max_timer_horizon_s"),
        max_reads_per_sec: i64v("maxReadsPerSecond", "max_reads_per_sec")
            .and_then(|n| i32::try_from(n).ok()),
        max_writes_per_sec: i64v("maxWritesPerSecond", "max_writes_per_sec")
            .and_then(|n| i32::try_from(n).ok()),
        max_queues: i64v("maxQueues", "max_queues").and_then(|n| i32::try_from(n).ok()),
        max_msgs_per_sec: i64v("maxMessagesPerSecond", "max_msgs_per_sec")
            .and_then(|n| i32::try_from(n).ok()),
        max_queries: i64v("maxQueries", "max_queries"),
        updated_at_us: 0,
    }
}

// ---------------------------------------------------------------------------
// WP-1.7a seam test: boot the broker,
// and confirm the message path routes to the (NotReady) facade — 503
// raft_phase1_unsupported from push and pop, 413 for an over-long name — while
// /health answers 200 with the raft block. Exercises the same handler functions
// the router dispatches to (the storage-aware guards in data.rs → this module →
// the facade), against a raft `AppState` whose lazy pool is never dialled.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    /// The routes about THE NODE ASKED are answered by it: a follower that
    /// forwarded `/api/v1/raft/liveness` would report the leader's id as its own
    /// — and a Kafka facade on that follower would file itself under the
    /// leader's raft node. Everything else under `/api/` still goes to the
    /// leader.
    #[cfg(feature = "server")]
    #[test]
    fn a_follower_answers_the_raft_routes_itself() {
        assert!(super::is_node_local("/api/v1/raft/liveness"));
        assert!(super::is_node_local("/api/v1/raft/members"));
        assert!(super::is_node_local("/api/v1/raft/status"));
        for forwarded in [
            "/api/v1/push",
            "/api/v1/kv",
            "/api/v1/resources/queues",
            "/api/v1/raftish",
        ] {
            assert!(!super::is_node_local(forwarded), "{forwarded}");
        }
    }

    use axum::body::Bytes;
    use axum::extract::{Path, Query, State};
    use axum::http::StatusCode;
    use axum::Extension;

    use super::AppState;
    use crate::auth::AuthedSub;
    use crate::tenant::Tenant;

    /// A config with a throwaway data dir. `config::load` is the only
    /// caller of itself in the lib unit-test binary, so setting the env here is
    /// race-free.
    fn raft_config() -> crate::config::Config {
        let dir = std::env::temp_dir().join(format!("queen-raft-seam-{}", std::process::id()));
        std::env::set_var("QUEEN_RAFT_DIR", dir.display().to_string());
        crate::config::load()
    }

    async fn body_of(resp: axum::response::Response) -> (StatusCode, String) {
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("read body");
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    fn raft_state() -> Arc<AppState> {
        let cfg = raft_config();
        super::build_raft_state(&cfg).expect("build raft state")
    }

    #[tokio::test]
    async fn push_answers_503_raft_phase1_unsupported() {
        let st = raft_state();
        let body = Bytes::from_static(b"{\"items\":[{\"queue\":\"q\",\"payload\":{}}]}");
        let resp = super::super::handle_push(
            State(st),
            Extension(AuthedSub(None)),
            Extension(Tenant::default_tenant()),
            body,
        )
        .await;
        let (status, text) = body_of(resp).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "push status: {text}"
        );
        assert!(
            text.contains("raft_phase1_unsupported"),
            "push body should carry the code: {text}"
        );
    }

    #[tokio::test]
    async fn pop_answers_503_raft_phase1_unsupported() {
        let st = raft_state();
        // All-None PopParams (a plain pop): its fields are private to `data`, but
        // its derived Deserialize builds it from an empty object.
        let p: super::super::PopParams = serde_json::from_str("{}").expect("empty PopParams");
        let resp = super::super::handle_pop(
            State(st),
            Extension(Tenant::default_tenant()),
            Path("orders".to_string()),
            Query(p),
        )
        .await;
        let (status, text) = body_of(resp).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "pop status: {text}"
        );
        assert!(
            text.contains("raft_phase1_unsupported"),
            "pop body should carry the code: {text}"
        );
    }

    #[tokio::test]
    async fn over_long_queue_name_answers_413() {
        let st = raft_state();
        let long = "q".repeat(600); // > NAME_BUDGET_BYTES (511 - 64)
        assert!(long.len() > crate::rsm::facade::NAME_BUDGET_BYTES);
        let p: super::super::PopParams = serde_json::from_str("{}").expect("empty PopParams");
        let resp = super::super::handle_pop(
            State(st),
            Extension(Tenant::default_tenant()),
            Path(long),
            Query(p),
        )
        .await;
        let (status, text) = body_of(resp).await;
        assert_eq!(
            status,
            StatusCode::PAYLOAD_TOO_LARGE,
            "over-long queue: {text}"
        );
        assert!(
            text.contains("name_too_long"),
            "413 body should carry the code: {text}"
        );
    }

    #[tokio::test]
    async fn health_answers_200_with_raft_block() {
        let st = raft_state();
        let resp = super::handle_health(State(st)).await;
        let (status, text) = body_of(resp).await;
        assert_eq!(status, StatusCode::OK, "health status: {text}");
        assert!(
            text.contains("\"status\":\"healthy\""),
            "health body: {text}"
        );
        assert!(
            text.contains("\"raft\":{"),
            "health body should carry the raft block: {text}"
        );
        assert!(
            text.contains("\"storageReady\":false"),
            "phase-1 storage is not ready: {text}"
        );
    }

    /// WP-2.3: the four timer routes, through the REAL handlers, over a REAL
    /// `RaftFacade` — the same funnel a client goes through (body
    /// shape, server-owned fields, horizon, the ladder), the state machine
    /// below it. Schedule → peek → list/count → it fires → the pop sees the
    /// message; cancel → `absent` with the caller's txn echoed; a bad call is
    /// the SP's `400 timers_bad_request`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn timers_routes_serve_raft_mode_end_to_end() {
        use std::collections::HashMap;
        use std::time::Duration;

        use base64::Engine;

        let cfg = raft_config();
        let dir = std::env::temp_dir().join(format!(
            "queen-raft-timers-handlers-{}-{}",
            std::process::id(),
            crate::util::uuidv7_bytes()[15]
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
        let facade = crate::rsm::facade::real::RaftFacade::open_with(
            &crate::rsm::facade::RsmBuildCtx {
                data_dir: dir.display().to_string(),
                notifier: crate::notify::Notifier::new(false),
                disk_high_pct: 85.0,
                disk_low_pct: 80.0,
            },
            crate::rsm::batcher::BatcherConfig {
                timer_tick_ms: 10,
                ..crate::rsm::batcher::BatcherConfig::from_env()
            },
        )
        .expect("open facade");
        let rsm: Arc<dyn crate::rsm::facade::Rsm> = Arc::new(facade);
        let st = super::build_raft_state_with(&cfg, Some(rsm)).expect("raft state");
        let tenant = || Extension(Tenant::default_tenant());
        let payload = base64::engine::general_purpose::STANDARD.encode(br#"{"hello":"raft"}"#);

        // ---- POST /api/v1/timers: one schedule far out, one due soon --------
        let body = format!(
            r#"{{"operations":[
                {{"op":"schedule","queue":"hq","timerKey":"later","delayMs":600000,"txn":"tx-later","payload":"{payload}"}},
                {{"op":"schedule","queue":"hq","timerKey":"soon","delayMs":300,"txn":"tx-soon","payload":"{payload}"}}
            ]}}"#
        );
        let resp = super::super::handle_timers_batch(
            State(st.clone()),
            Extension(AuthedSub(Some("svc-h".into()))),
            tenant(),
            Bytes::from(body),
        )
        .await;
        let (status, text) = body_of(resp).await;
        assert_eq!(status, StatusCode::OK, "schedule: {text}");
        let v: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(v["results"][0]["status"], "scheduled", "{text}");
        assert_eq!(v["results"][1]["status"], "scheduled", "{text}");
        let soon_mid = v["results"][1]["messageId"].as_str().unwrap().to_string();

        // ---- GET peek / list / count ----------------------------------------
        let resp = super::super::handle_timer_peek(
            State(st.clone()),
            tenant(),
            Path(("hq".to_string(), "later".to_string())),
        )
        .await;
        let (status, text) = body_of(resp).await;
        assert_eq!(status, StatusCode::OK);
        let p: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(p["found"], true, "{text}");
        assert_eq!(p["producerSub"], "svc-h", "stamped from the JWT sub");
        assert_eq!(p["payload"], payload);

        let q: super::super::timers::TimerReadParams =
            serde_json::from_str(r#"{"mode":"count","prefix":"so"}"#).unwrap();
        let resp = super::super::handle_timers_list(
            State(st.clone()),
            tenant(),
            Path("hq".to_string()),
            Query(q),
        )
        .await;
        let (status, text) = body_of(resp).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, r#"{"count":1}"#));

        // ---- it fires on its own; the pop route sees the message ------------
        let mut fired = false;
        for _ in 0..500 {
            let resp = super::super::handle_timer_peek(
                State(st.clone()),
                tenant(),
                Path(("hq".to_string(), "soon".to_string())),
            )
            .await;
            let (_, text) = body_of(resp).await;
            if text.contains("\"found\":false") {
                fired = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(fired, "the due timer never fired");
        let p: super::super::PopParams = serde_json::from_str("{}").expect("PopParams");
        let resp = super::super::handle_pop(
            State(st.clone()),
            tenant(),
            Path("hq".to_string()),
            Query(p),
        )
        .await;
        let (status, text) = body_of(resp).await;
        assert_eq!(status, StatusCode::OK, "pop: {text}");
        let pop: serde_json::Value = serde_json::from_str(&text).unwrap();
        let msgs = pop["messages"].as_array().expect("messages");
        assert_eq!(msgs.len(), 1, "only the due one fired: {text}");
        assert_eq!(msgs[0]["id"], soon_mid.as_str());
        assert_eq!(msgs[0]["transactionId"], "tx-soon");
        assert_eq!(msgs[0]["data"]["hello"], "raft");

        // ---- DELETE: cancel the pending one, then `absent` ------------------
        let resp = super::super::handle_timer_cancel(
            State(st.clone()),
            tenant(),
            Path(("hq".to_string(), "later".to_string())),
            Query(HashMap::new()),
        )
        .await;
        let (status, text) = body_of(resp).await;
        assert_eq!(status, StatusCode::OK);
        let c: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(c["status"], "cancelled", "{text}");
        let mut qp = HashMap::new();
        qp.insert("txn".to_string(), "tx-later".to_string());
        let resp = super::super::handle_timer_cancel(
            State(st.clone()),
            tenant(),
            Path(("hq".to_string(), "later".to_string())),
            Query(qp),
        )
        .await;
        let (_, text) = body_of(resp).await;
        let c: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            (c["ok"].clone(), c["status"].clone()),
            (false.into(), "absent".into())
        );
        assert_eq!(
            c["txn"], "tx-later",
            "the expected txn is echoed (025 §4.4)"
        );

        // ---- a bad call: the SP's 400, nothing scheduled --------------------
        let bad = format!(
            r#"[{{"op":"schedule","queue":"hq","timerKey":"x","delayMs":5,"payload":"{payload}"}}]"#
        );
        let resp = super::super::handle_timers_batch(
            State(st.clone()),
            Extension(AuthedSub(None)),
            tenant(),
            Bytes::from(bad),
        )
        .await;
        let (status, text) = body_of(resp).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
        assert!(text.contains("timers_bad_request"), "{text}");
        assert!(text.contains("txn is required"), "{text}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// PLAN_RAFT.md WP-2.2: the KV routes, through the REAL handlers, answered
    /// by the REAL state machine. The builder hook is not registered in the
    /// unit-test binary (the seam tests above keep the stub), so the facade is
    /// swapped into a raft `AppState` by hand. Every answer here is the wire
    /// answer a client gets for the same call.
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn kv_routes_are_served_by_the_state_machine_in_raft_mode() {
        use std::collections::HashMap;

        use crate::rsm::facade::real::RaftFacade;
        use crate::rsm::facade::RsmBuildCtx;

        std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
        let dir = std::env::temp_dir().join(format!("queen-raft-kv-routes-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let facade = Arc::new(
            RaftFacade::open(&RsmBuildCtx {
                data_dir: dir.display().to_string(),
                notifier: crate::notify::Notifier::new(false),
                disk_high_pct: 85.0,
                disk_low_pct: 80.0,
            })
            .expect("open the facade"),
        );
        let mut s = Arc::try_unwrap(raft_state())
            .ok()
            .expect("a fresh raft state has one owner");
        s.rsm = facade.clone();
        let st = Arc::new(s);
        let t = || Extension(Tenant::default_tenant());
        let q = || Query(HashMap::<String, String>::new());
        let path = |k: &str| Path(("orders".to_string(), k.to_string()));

        // PUT /api/v1/kv/:ns/*key — a key with slashes.
        let (status, text) = body_of(
            super::super::handle_kv_put(
                State(st.clone()),
                t(),
                path("order/9f1/items"),
                q(),
                Bytes::from_static(br#"{"value":{"n":1},"ttlSeconds":60}"#),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{text}");
        let put: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(put["applied"], true, "{text}");
        let v = put["version"].as_u64().expect("version");

        // GET: the element, and the ETag of its version.
        let resp =
            super::super::handle_kv_get(State(st.clone()), t(), path("order/9f1/items"), q()).await;
        let etag = resp
            .headers()
            .get(axum::http::header::ETAG)
            .map(|h| h.to_str().unwrap().to_string());
        let (status, text) = body_of(resp).await;
        assert_eq!(status, StatusCode::OK, "{text}");
        assert_eq!(etag.as_deref(), Some(format!("\"{v}\"").as_str()));
        let got: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(got["found"], true);
        assert_eq!(got["value"], serde_json::json!({"n":1}));

        // A miss is 200 with found:false and no ETag.
        let resp = super::super::handle_kv_get(State(st.clone()), t(), path("nope"), q()).await;
        assert!(resp.headers().get(axum::http::header::ETAG).is_none());
        let (status, text) = body_of(resp).await;
        assert_eq!(
            (status, text.contains("\"found\":false")),
            (StatusCode::OK, true),
            "{text}"
        );

        // POST /api/v1/kv: the batch envelope, a CAS loser answered 200.
        let body = format!(
            r#"{{"operations":[
                {{"op":"put","ns":"orders","key":"order/9f1/items","value":2,"ttlSeconds":60,"expect":{}}},
                {{"op":"incr","ns":"ctr","key":"hits","delta":1,"forever":true}},
                {{"op":"getPrefix","ns":"orders","prefix":"order/"}}
            ]}}"#,
            v + 1000
        );
        let (status, text) =
            body_of(super::super::handle_kv_batch(State(st.clone()), t(), Bytes::from(body)).await)
                .await;
        assert_eq!(status, StatusCode::OK, "{text}");
        let b: serde_json::Value = serde_json::from_str(&text).unwrap();
        let r = b["results"].as_array().expect("results");
        assert_eq!(r[0]["applied"], false);
        assert_eq!(r[0]["reason"], "version");
        assert_eq!(r[0]["value"], serde_json::json!({"n":1}));
        assert_eq!(r[1]["value"], 1);
        assert_eq!(r[2]["rows"][0]["key"], "order/9f1/items");

        // 024's shape refusal: 400 with the stable reason.
        let (status, text) = body_of(
            super::super::handle_kv_batch(
                State(st.clone()),
                t(),
                Bytes::from_static(br#"[{"op":"put","ns":"orders","key":"k","value":1}]"#),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
        assert!(text.contains("\"error\":\"kv_bad_request\""), "{text}");
        assert!(text.contains("kv_expiry_not_specified"), "{text}");

        // A lost `required` precondition: 200, ok:false, the DETAIL's fields.
        let (status, text) = body_of(
            super::super::handle_kv_batch(
                State(st.clone()),
                t(),
                Bytes::from_static(
                    br#"[{"op":"putIfAbsent","ns":"orders","key":"order/9f1/items","value":0,"forever":true,"required":true}]"#,
                ),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{text}");
        let p: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(p["ok"], false);
        assert_eq!(p["reason"], "kv_precondition");
        assert_eq!(p["failedIndex"], 0);
        assert_eq!(p["kvReason"], "exists");
        assert_eq!(p["version"].as_u64(), Some(v));

        // The console reads.
        let (status, text) =
            body_of(super::super::handle_kv_namespaces(State(st.clone()), t(), q()).await).await;
        assert_eq!(status, StatusCode::OK, "{text}");
        let ns: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            ns,
            serde_json::json!({"namespaces":[
                {"namespace":"ctr","keys":1},{"namespace":"orders","keys":1}]})
        );
        let (status, text) = body_of(
            super::super::handle_kv_list(
                State(st.clone()),
                t(),
                q(),
                Bytes::from_static(br#"{"namespace":"orders","includeExpired":true}"#),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{text}");
        let l: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(l["rows"][0]["key"], "order/9f1/items");
        assert_eq!(l["rows"][0]["expired"], false);
        assert!(l["rows"][0]["expiresAt"].as_str().unwrap().ends_with('Z'));

        // DELETE, then the key is gone.
        let (status, text) = body_of(
            super::super::handle_kv_delete(
                State(st.clone()),
                t(),
                path("order/9f1/items"),
                q(),
                Bytes::new(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{text}");
        assert!(text.contains("\"applied\":true"), "{text}");
        let (_, text) = body_of(
            super::super::handle_kv_get(State(st.clone()), t(), path("order/9f1/items"), q()).await,
        )
        .await;
        assert!(text.contains("\"found\":false"), "{text}");

        drop(st);
        if let Ok(f) = Arc::try_unwrap(facade) {
            f.shutdown().await;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `POST /api/v1/locks`, through the REAL handler and the REAL state
    /// machine: the envelope, the status rule (a held lock is a 200), the
    /// refusals by name, the series, and the claim the whole feature rests on
    /// — a permit is a KV row, so the KV routes show it and an operator
    /// breaks a stuck lock with a KV delete.
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn the_locks_route_is_served_through_kv_in_raft_mode() {
        use std::collections::HashMap;

        use crate::metrics::{KvResult, LockOp};
        use crate::rsm::facade::real::RaftFacade;
        use crate::rsm::facade::RsmBuildCtx;

        std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
        let dir =
            std::env::temp_dir().join(format!("queen-raft-locks-route-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let facade = Arc::new(
            RaftFacade::open(&RsmBuildCtx {
                data_dir: dir.display().to_string(),
                notifier: crate::notify::Notifier::new(false),
                disk_high_pct: 85.0,
                disk_low_pct: 80.0,
            })
            .expect("open the facade"),
        );
        let mut s = Arc::try_unwrap(raft_state())
            .ok()
            .expect("a fresh raft state has one owner");
        s.rsm = facade.clone();
        let st = Arc::new(s);
        let t = || Extension(Tenant::default_tenant());
        let q = || Query(HashMap::<String, String>::new());
        let locks = |body: &str| {
            super::super::handle_locks_batch(State(st.clone()), t(), Bytes::from(body.to_string()))
        };
        let results = |text: &str| -> Vec<serde_json::Value> {
            let b: serde_json::Value = serde_json::from_str(text).expect("json");
            b["results"].as_array().expect("results").clone()
        };

        // Acquire, in the envelope and as a bare array.
        let (status, text) = body_of(
            locks(r#"{"operations":[{"op":"acquire","name":"daily-report","ttlSeconds":30,"owner":"a"}]}"#)
                .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{text}");
        let a = results(&text).remove(0);
        assert_eq!(a["acquired"], true, "{text}");
        let token = a["token"].as_u64().expect("a token");
        assert_eq!(a["guard"]["key"], "daily-report#0");

        // Held by somebody else: 200, with the verdict in the body.
        let (status, text) = body_of(
            locks(r#"[{"op":"acquire","name":"daily-report","ttlSeconds":30,"owner":"b"}]"#).await,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "a held lock is not an error: {text}"
        );
        let b = results(&text).remove(0);
        assert_eq!(b["acquired"], false, "{text}");
        assert_eq!(b["reason"], "held");
        assert_eq!(b["holders"][0]["owner"], "a");

        // The series: one acquire applied, one rejected, no error.
        let m = &st.metrics.kvt;
        assert_eq!(m.lock_ops_total(LockOp::Acquire, KvResult::Applied), 1);
        assert_eq!(m.lock_ops_total(LockOp::Acquire, KvResult::Rejected), 1);
        assert_eq!(m.lock_ops_total(LockOp::Acquire, KvResult::Error), 0);

        // Refused by name, before anything is sent.
        for (body, reason) in [
            ("{", "locks_bad_body"),
            (r#"{"ops":[]}"#, "locks_bad_body"),
            (r#"[{"op":"acquire","name":"x"}]"#, "locks_ttl_required"),
            (
                r#"[{"op":"acquire","name":"a#b","ttlSeconds":1}]"#,
                "locks_bad_name",
            ),
            (r#"[{"op":"release","name":"x"}]"#, "locks_token_required"),
        ] {
            let (status, text) = body_of(locks(body).await).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {text}");
            assert!(text.contains("\"error\":\"locks_bad_request\""), "{text}");
            assert!(text.contains(reason), "{body}: {text}");
        }
        // An empty call is an empty answer.
        let (status, text) = body_of(locks("[]").await).await;
        assert_eq!(
            (status, text.as_str()),
            (StatusCode::OK, r#"{"results":[]}"#)
        );

        // The permit is a KV row: the console list of the lock namespace
        // shows it, at the version that is the token.
        let (status, text) = body_of(
            super::super::handle_kv_list(
                State(st.clone()),
                t(),
                q(),
                Bytes::from_static(br#"{"namespace":"queen-locks"}"#),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{text}");
        let l: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(l["rows"].as_array().map(Vec::len), Some(1), "{text}");
        assert_eq!(l["rows"][0]["key"], "daily-report#0");
        assert_eq!(l["rows"][0]["value"], serde_json::json!({"owner":"a"}));
        assert_eq!(l["rows"][0]["version"].as_u64(), Some(token));

        // An operator breaks the lock with a KV delete; b takes it, and a's
        // release is told the permit is b's.
        let (status, text) = body_of(
            super::super::handle_kv_delete(
                State(st.clone()),
                t(),
                Path(("queen-locks".to_string(), "daily-report#0".to_string())),
                q(),
                Bytes::new(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{text}");
        assert!(text.contains("\"applied\":true"), "{text}");
        let (_, text) = body_of(
            locks(r#"[{"op":"acquire","name":"daily-report","ttlSeconds":30,"owner":"b"}]"#).await,
        )
        .await;
        let b = results(&text).remove(0);
        assert_eq!(b["acquired"], true, "{text}");
        assert!(b["token"].as_u64().unwrap() > token);
        let (_, text) = body_of(
            locks(&format!(
                r#"[{{"op":"release","name":"daily-report","token":{token}}},{{"op":"get","name":"gpu"}}]"#
            ))
            .await,
        )
        .await;
        let r = results(&text);
        assert_eq!(r[0]["released"], false, "{text}");
        assert_eq!(r[0]["holders"][0]["owner"], "b");
        assert_eq!(r[1]["held"], false, "{text}");

        // The operator's KV switch turns the locks off with the keys: a
        // permit is a KV row, and the ladder is KV's.
        st.switches.set_kv(false);
        let (status, text) = body_of(
            locks(r#"[{"op":"acquire","name":"other","ttlSeconds":30,"owner":"a"}]"#).await,
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{text}");
        let (status, _) = body_of(locks(r#"[{"op":"get","name":"other"}]"#).await).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        st.switches.set_kv(true);
        let (status, text) = body_of(locks(r#"[{"op":"get","name":"other"}]"#).await).await;
        assert_eq!(status, StatusCode::OK, "{text}");

        drop(st);
        if let Ok(f) = Arc::try_unwrap(facade) {
            f.shutdown().await;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(feature = "server")]
    #[tokio::test]
    async fn phase2_fallback_serves_api_and_unknown_routes_are_404() {
        use crate::rsm::facade::real::RaftFacade;
        use crate::rsm::facade::{Rsm, RsmBuildCtx};

        std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
        let dir = std::env::temp_dir().join(format!(
            "queen-raft-phase2-fallback-{}-{}",
            std::process::id(),
            crate::util::uuidv7_bytes()[15]
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let facade = Arc::new(
            RaftFacade::open(&RsmBuildCtx {
                data_dir: dir.display().to_string(),
                notifier: crate::notify::Notifier::new(false),
                disk_high_pct: 85.0,
                disk_low_pct: 80.0,
            })
            .expect("open the facade"),
        );
        let rsm: Arc<dyn Rsm> = facade.clone();
        let st = super::build_raft_state_with(&raft_config(), Some(rsm)).expect("raft state");
        let resp = super::raft_fallback(
            State(st.clone()),
            Extension(Tenant::default_tenant()),
            axum::http::Method::GET,
            "/api/v1/does-not-exist".parse().unwrap(),
            Bytes::new(),
        )
        .await;
        let (status, text) = body_of(resp).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{text}");
        assert!(text.contains("not found"), "fallback body: {text}");

        drop(st);
        if let Ok(f) = Arc::try_unwrap(facade) {
            f.shutdown().await;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A queue whose name has to be encoded in a path (`laravel:default`, a
    /// space, a slash) is one queue on every route: the push files it under
    /// the name in its body, and the depth read (the Laravel supervisor's
    /// scaling input), the resource view, the sizes, the status read, the pop
    /// and the delete all reach it through the encoded segment, as
    /// `rawurlencode` and `encodeURIComponent` write it. Through the whole
    /// router, the way a client reaches it. The embedded API's delete hands
    /// the handler the plain name and reaches the same queue.
    #[cfg(all(feature = "server", feature = "kafka"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn an_encoded_queue_name_reaches_its_queue_on_every_path_route() {
        use axum::http::{Method, Request};
        use serde_json::{json, Value};
        use tower::ServiceExt;

        use crate::rsm::facade::real::RaftFacade;
        use crate::rsm::facade::{Rsm, RsmBuildCtx};

        async fn call(
            router: &axum::Router,
            method: Method,
            path: &str,
            body: Option<Value>,
        ) -> (StatusCode, Value) {
            let req = Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    body.map(|b| b.to_string()).unwrap_or_default(),
                ))
                .expect("request");
            let (status, text) = body_of(router.clone().oneshot(req).await.expect("answer")).await;
            (status, serde_json::from_str(&text).unwrap_or(Value::Null))
        }
        // A write is answered before the store commit a fresh read sees (the
        // commit cadence): read until the queue is there, briefly.
        async fn get(router: &axum::Router, path: &str) -> (StatusCode, Value) {
            for _ in 0..400 {
                let (s, v) = call(router, Method::GET, path, None).await;
                if s != StatusCode::NOT_FOUND {
                    return (s, v);
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            call(router, Method::GET, path, None).await
        }

        std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
        let dir = std::env::temp_dir().join(format!(
            "queen-raft-encoded-names-{}-{}",
            std::process::id(),
            crate::util::uuidv7_bytes()[15]
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let facade = Arc::new(
            RaftFacade::open(&RsmBuildCtx {
                data_dir: dir.display().to_string(),
                notifier: crate::notify::Notifier::new(false),
                disk_high_pct: 85.0,
                disk_low_pct: 80.0,
            })
            .expect("open the facade"),
        );
        let rsm: Arc<dyn Rsm> = facade.clone();
        let st = super::build_raft_state_with(&raft_config(), Some(rsm)).expect("raft state");
        let auth = crate::auth::Authenticator::new(crate::config::AuthConfig {
            enabled: false,
            algorithm: "HS256".into(),
            secret: String::new(),
            public_key: String::new(),
            jwks_url: String::new(),
            jwks_refresh_interval_seconds: 3600,
            jwks_request_timeout_ms: 5000,
            issuer: String::new(),
            audience: String::new(),
            clock_skew_seconds: 30,
            skip_paths: Vec::new(),
            roles_claim: "role".into(),
            roles_array_claim: "roles".into(),
            role_admin: "admin".into(),
            role_read_write: "read-write".into(),
            role_read_only: "read-only".into(),
            role_write_only: "write-only".into(),
        });
        let router = super::build_raft_router(st.clone(), auth, false);
        let push = |name: &str| {
            let items: Vec<Value> = (0..3)
                .map(|n| json!({"queue": name, "payload": {"n": n}}))
                .collect();
            json!({ "items": items })
        };

        for (name, seg) in [
            ("laravel:default", "laravel%3Adefault"),
            ("jobs/eu west", "jobs%2Feu%20west"),
        ] {
            let (s, v) = call(&router, Method::POST, "/api/v1/push", Some(push(name))).await;
            assert_eq!(s, StatusCode::CREATED, "{name}: push {v}");

            let (s, v) = get(&router, &format!("/api/v1/resources/queues/{seg}/depth")).await;
            assert_eq!(s, StatusCode::OK, "{name}: depth {v}");
            assert_eq!(v["queue"], name, "{v}");
            assert_eq!(v["pending"], 3, "{name}: depth {v}");

            let (s, v) = get(&router, &format!("/api/v1/resources/queues/{seg}")).await;
            assert_eq!(s, StatusCode::OK, "{name}: resource view {v}");
            assert_eq!(v["name"], name, "{v}");
            assert_eq!(v["totals"]["total"], 3, "{name}: resource view {v}");

            let (s, v) = get(&router, &format!("/api/v1/resources/queues/{seg}/sizes")).await;
            assert_eq!(s, StatusCode::OK, "{name}: sizes {v}");
            assert_eq!(v["queue"], name, "{v}");

            let (s, v) = get(&router, &format!("/api/v1/status/queues/{seg}")).await;
            assert_eq!(s, StatusCode::OK, "{name}: status {v}");
            assert_eq!(v["queue"]["name"], name, "{v}");

            let pop = format!("/api/v1/pop/queue/{seg}?batch=10&wait=false&autoAck=true");
            let (s, v) = call(&router, Method::GET, &pop, None).await;
            assert_eq!(s, StatusCode::OK, "{name}: pop {v}");
            assert_eq!(v["messages"].as_array().map(Vec::len), Some(3), "{v}");

            let path = format!("/api/v1/resources/queues/{seg}");
            let (s, v) = call(&router, Method::DELETE, &path, None).await;
            assert_eq!(s, StatusCode::OK, "{name}: delete {v}");
            assert_eq!(v["deleted"], true, "{name}: delete {v}");
            let depth = format!("/api/v1/resources/queues/{seg}/depth");
            let (s, v) = call(&router, Method::GET, &depth, None).await;
            assert_eq!(s, StatusCode::NOT_FOUND, "{name}: depth after delete {v}");
        }

        // The embedded API's delete: the handler is handed the plain name.
        let (s, v) = call(
            &router,
            Method::POST,
            "/api/v1/push",
            Some(push("jobs/eu west")),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED, "push {v}");
        get(&router, "/api/v1/resources/queues/jobs%2Feu%20west").await;
        let resp = super::super::handle_delete_queue(
            State(st.clone()),
            Extension(Tenant::default_tenant()),
            Path("jobs/eu west".to_string()),
        )
        .await;
        let (status, text) = body_of(resp).await;
        assert_eq!(status, StatusCode::OK, "{text}");
        assert!(text.contains("\"deleted\":true"), "embedded delete: {text}");

        drop((router, st));
        if let Ok(f) = Arc::try_unwrap(facade) {
            f.shutdown().await;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The reserved tenants (the embedded proxy's `...fffe`, which holds its
    /// tenants, users, API-key hashes, plans and usage; the broker-internal
    /// `...fffd`) are reachable only in-process by their owners, never over
    /// HTTP. Two defences compose through the whole router, proven here with
    /// auth OFF (the weakest posture, the internal broker port) so nothing but
    /// the tenant rules can be doing the refusing:
    ///   * a request SCOPED to a reserved tenant (`x-queen-tenant`) is a 403
    ///     before any handler — it would otherwise read and write the proxy's
    ///     own state by name;
    ///   * a tenant purge or quota grant that NAMES a reserved tenant as its
    ///     target is a 403 at the facade — the target is in the query or body,
    ///     not the header, so the scope check above never sees it.
    /// A normal tenant is still purged and still takes a quota, so the guard is
    /// the reserved ids and nothing wider.
    #[cfg(feature = "server")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn the_reserved_tenants_are_unreachable_over_http() {
        use axum::http::{Method, Request};
        use serde_json::{json, Value};
        use tower::ServiceExt;

        use crate::rsm::facade::real::RaftFacade;
        use crate::rsm::facade::{Rsm, RsmBuildCtx};

        const PROXY: &str = crate::config::PROXY_SYSTEM_TENANT; // ...fffe
        const INTERNAL: &str = crate::config::SYSTEM_TENANT; // ...fffd
        const ACME: &str = "aabbccdd-1122-3344-5566-778899aabbcc";

        async fn call(
            router: &axum::Router,
            method: Method,
            path: &str,
            tenant_header: Option<&str>,
            body: Option<Value>,
        ) -> (StatusCode, Value) {
            let mut req = Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json");
            if let Some(t) = tenant_header {
                req = req.header(crate::config::TENANT_HEADER, t);
            }
            let req = req
                .body(axum::body::Body::from(
                    body.map(|b| b.to_string()).unwrap_or_default(),
                ))
                .expect("request");
            let (status, text) = body_of(router.clone().oneshot(req).await.expect("answer")).await;
            (status, serde_json::from_str(&text).unwrap_or(Value::Null))
        }

        std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
        let dir = std::env::temp_dir().join(format!(
            "queen-raft-reserved-tenant-{}-{}",
            std::process::id(),
            crate::util::uuidv7_bytes()[15]
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let facade = Arc::new(
            RaftFacade::open(&RsmBuildCtx {
                data_dir: dir.display().to_string(),
                notifier: crate::notify::Notifier::new(false),
                disk_high_pct: 85.0,
                disk_low_pct: 80.0,
            })
            .expect("open the facade"),
        );
        let rsm: Arc<dyn Rsm> = facade.clone();
        let st = super::build_raft_state_with(&raft_config(), Some(rsm)).expect("raft state");
        let mut auth_cfg = crate::config::load().auth;
        auth_cfg.enabled = false;
        let auth = crate::auth::Authenticator::new(auth_cfg);
        // Tenancy ON so the header resolves to a tenant: the real posture of the
        // router that sits behind the proxy, where a leaked `x-queen-tenant`
        // would otherwise be honoured.
        let router = super::build_raft_router(st.clone(), auth, true);

        // (1) A request SCOPED to a reserved tenant: refused before any handler,
        // on a read as much as a write.
        for reserved in [PROXY, INTERNAL] {
            for (method, path, body) in [
                (Method::GET, "/api/v1/resources/queues".to_string(), None),
                (
                    Method::POST,
                    "/api/v1/kv".to_string(),
                    Some(json!({"ops":[{"op":"get","namespace":"px.keys","key":"#x"}]})),
                ),
            ] {
                let (s, v) = call(&router, method.clone(), &path, Some(reserved), body).await;
                assert_eq!(s, StatusCode::FORBIDDEN, "{method} {path} as {reserved}: {v}");
                assert_eq!(v["code"], "reserved_tenant", "{method} {path} as {reserved}: {v}");
            }
        }

        // (2) A purge or a quota grant that NAMES a reserved tenant as its
        // target (query or body), the request itself scoped to the default
        // tenant: refused at the facade.
        for reserved in [PROXY, INTERNAL] {
            let (s, v) = call(
                &router,
                Method::DELETE,
                &format!("/api/v1/resources/tenant?tenant={reserved}"),
                None,
                None,
            )
            .await;
            assert_eq!(s, StatusCode::FORBIDDEN, "purge target {reserved}: {v}");
            assert_eq!(v["code"], "reserved_tenant", "purge target {reserved}: {v}");

            let (s, v) = call(
                &router,
                Method::POST,
                "/api/v1/resources/quota",
                None,
                Some(json!({"tenant": reserved, "kind": "kv", "maxRows": 1})),
            )
            .await;
            assert_eq!(s, StatusCode::FORBIDDEN, "quota target {reserved}: {v}");
            assert_eq!(v["code"], "reserved_tenant", "quota target {reserved}: {v}");
        }

        // The same target in the body of the purge (not only the query): still
        // refused, so neither spelling of the target slips past.
        let (s, v) = call(
            &router,
            Method::DELETE,
            "/api/v1/resources/tenant",
            None,
            Some(json!({"tenant": PROXY})),
        )
        .await;
        assert_eq!(s, StatusCode::FORBIDDEN, "purge body target: {v}");
        assert_eq!(v["code"], "reserved_tenant", "purge body target: {v}");

        // (3) A NORMAL tenant is untouched: the purge runs (nothing there, so a
        // clean done) and the quota is accepted. The guard is the reserved ids,
        // not every tenant that is not the default.
        let (s, v) = call(
            &router,
            Method::DELETE,
            &format!("/api/v1/resources/tenant?tenant={ACME}"),
            None,
            None,
        )
        .await;
        assert_eq!(s, StatusCode::OK, "purge normal tenant: {v}");
        assert_eq!(v["success"], true, "purge normal tenant: {v}");

        let (s, v) = call(
            &router,
            Method::POST,
            "/api/v1/resources/quota",
            None,
            Some(json!({"tenant": ACME, "kind": "kv", "maxRows": 10})),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "quota normal tenant: {v}");
        assert_eq!(v["success"], true, "quota normal tenant: {v}");

        drop((router, st));
        if let Ok(f) = Arc::try_unwrap(facade) {
            f.shutdown().await;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
