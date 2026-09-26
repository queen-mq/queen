//! The message-path handlers: push, the three pop routes, ack, lease renew and
//! the wire transaction. Each one validates the request at the edge and hands a
//! typed command to the replicated state machine (`handlers::raft::dispatch_*`).
#![allow(unused_imports)]
use super::*;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use serde::Deserialize;

// ------------------------------------------------------------------ push

/// Wire limit on `transactionId`, dictated by the u16 length prefix the segment
/// frame codec writes for it (`frames::pack_frames`).
pub(crate) const MAX_TXN_BYTES: usize = u16::MAX as usize;

pub async fn handle_push(
    State(st): State<Arc<AppState>>,
    Extension(authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    body: Bytes,
) -> Response {
    // Producer identity is server-owned: only the validated JWT subject is
    // forwarded to either storage backend, never a body-supplied field.
    let producer_sub = authed.0.filter(|s| !s.is_empty());
    crate::handlers::raft::dispatch_push(&st, tenant.as_str(), producer_sub, body).await
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
    // RUSTFIX item 18: per-request lease override (?leaseSeconds=N). Wins over the
    // queue's configured leaseTime; 0/absent = use the queue value (else 60).
    #[serde(rename = "leaseSeconds")]
    lease_seconds: Option<i32>,
    #[serde(rename = "consumerGroup")]
    consumer_group: Option<String>,
    // Subscription seeding for a NEW (partition, group) cursor on first contact:
    // subscriptionMode 'new' | 'all', subscriptionFrom 'now' | ISO. When omitted,
    // the broker applies DEFAULT_SUBSCRIPTION_MODE (default 'new' — see
    // config::normalize_subscription_mode). Group-less "queue mode" pops are
    // hard-pinned to 'all' by the SQL and ignore both.
    // timestamp | '' (default). Threaded to the log pop SPs (p_sub_mode /
    // p_sub_from); existing cursors are never re-seeded.
    #[serde(rename = "subscriptionMode")]
    subscription_mode: Option<String>,
    #[serde(rename = "subscriptionFrom")]
    subscription_from: Option<String>,
    // PLAN_CONFLATION §3.1 — last-value delivery for this consumer GROUP on this
    // queue: a pop of a partition delivers exactly the newest visible message and
    // leases (committed, tail]. Declared here (query string, never a body field —
    // the subscriptionMode shape), persisted on the group's first registration,
    // and from then on the STORED value wins for every consumer of that group
    // (§1.1/§3.3). Absent ⇒ off ⇒ byte-identical behaviour.
    #[serde(rename = "conflation")]
    conflation: Option<bool>,
    // POP AUTOPILOT (server/src/pop_autopilot.rs) — "choose the knobs I did not
    // send". Emitted ONLY when true, the `conflation` shape and for the same
    // reason: a consumer that does not opt in sends the request it sent before
    // this option existed, byte for byte.
    //
    // It has to be a NEW parameter and could not be "field absent ⇒ broker
    // decides": absent `partitions` already MEANS 1 and absent `batch` already
    // MEANS 200, and today's SDKs omit both at their defaults (the Go SDK only
    // sends `partitions` when > 1), so the absent-field encoding would silently
    // change the behaviour of every consumer in the field that never touched the
    // knob. Per DIMENSION: `autopilot=true&partitions=1` is a manual width of 1
    // with an automatic batch, and the controller never touches a dimension the
    // client sent. Absent ⇒ off ⇒ byte-identical behaviour.
    #[serde(rename = "autopilot")]
    autopilot: Option<bool>,
}

/// §3.3 items 2 and 4 — group-setting-wins, LOUDLY: the counters and a
/// rate-limited line, never a per-request log line (a mismatched fleet would turn
/// that into a flood — the `POOL_SAT` idiom in obs.rs). The third channel, the
/// response echo, is `Conflation::conflict` and is rendered by the caller.
///
/// `queue` is `None` on the discovery route, which spans queues and so has no
/// single per-queue counter to attribute to; `scope` is the label for the log
/// line either way (a queue name, or the `namespace/task` pair). The raft pop
/// answers call it too (`handlers::raft::pop_answer`).
pub(crate) fn note_conflation_conflict(
    st: &AppState,
    tenant: &str,
    queue: Option<&str>,
    scope: &str,
    group: &str,
    stored: bool,
    requested: Option<bool>,
) {
    if let Some(q) = queue {
        st.metrics.per_queue.add_conflation_conflict(tenant, q);
    }
    st.metrics
        .conflation_conflicts
        .fetch_add(1, Ordering::Relaxed);
    static CFL_CONFLICT: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
    if let Some(suppressed) = CFL_CONFLICT.tick(crate::util::now_epoch_ms()) {
        tracing::warn!(
            target: "conflation",
            queue = scope,
            group,
            stored,
            requested = requested.unwrap_or(false),
            suppressed,
            "consumer declared a conflation policy the group does not have — \
             the STORED group setting wins"
        );
    }
}

/// §3.3 — the two refused combinations, rejected at the handler with a 400 that
/// names the reason. This is the one place conflation REJECTS rather than warns,
/// because both are consumer bugs whose silent form is unfixable in production.
/// Returns the refusal response when the request is illegal.
fn conflation_refusal(
    requested: Option<bool>,
    has_group: bool,
    auto_ack: bool,
) -> Option<Response> {
    if requested != Some(true) {
        return None;
    }
    if !has_group {
        return Some(json(
            StatusCode::BAD_REQUEST,
            "{\"success\":false,\"error\":\"conflation requires consumerGroup: queue mode is a \
             shared cursor with no group identity to hang a delivery policy on\",\"messages\":[]}"
                .to_string(),
        ));
    }
    if auto_ack {
        return Some(json(
            StatusCode::BAD_REQUEST,
            "{\"success\":false,\"error\":\"conflation cannot be combined with autoAck: auto-ack \
             commits at delivery with no lease, so a failed handler loses the tail and the \
             at-least-once guarantee conflation exists to provide degrades to \
             at-most-once\",\"messages\":[]}"
                .to_string(),
        ));
    }
    None
}

pub async fn handle_pop(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(queue): Path<String>,
    Query(p): Query<PopParams>,
) -> Response {
    let batch = p.batch.unwrap_or(200);
    let auto_ack = p.auto_ack.unwrap_or(false);
    if let Some(r) = conflation_refusal(p.conflation, p.consumer_group.is_some(), auto_ack) {
        return r;
    }
    let from = p.subscription_from.as_deref();
    let conflate = p.conflation == Some(true) && p.consumer_group.is_some();
    // POP AUTOPILOT (rsm/facade/autopilot.rs): opt-in per dimension; a
    // conflating pop is exempt (its `partitions` is the message budget, not a
    // width).
    let autopilot = p.autopilot == Some(true) && !conflate;
    crate::handlers::raft::dispatch_pop(
        &st,
        tenant.as_str(),
        queue,
        p.consumer_group.clone(),
        batch,
        auto_ack,
        p.wait.unwrap_or(false),
        p.timeout.unwrap_or(st.pop_default_timeout_ms),
        crate::rsm::facade::PopOptions {
            auto_parts: autopilot && p.partitions.is_none(),
            auto_batch: autopilot && p.batch.is_none(),
            max_parts: if conflate {
                p.partitions.unwrap_or(batch).clamp(1, 64) as u32
            } else {
                p.partitions.unwrap_or(1).clamp(1, 64) as u32
            },
            lease_seconds: p.lease_seconds.unwrap_or(0),
            subscription_mode: p
                .subscription_mode
                .as_deref()
                .map(crate::config::normalize_subscription_mode)
                .unwrap_or_else(|| st.default_subscription_mode.clone()),
            subscription_from_us: from
                .filter(|v| !v.eq_ignore_ascii_case("now"))
                .and_then(crate::util::parse_iso_ms)
                .map(|ms| ms.saturating_mul(1_000)),
            subscription_from_now: from.is_some_and(|v| v.eq_ignore_ascii_case("now")),
            conflate,
            conflate_requested: p.conflation,
        },
    )
    .await
}

// GET /api/v1/pop/queue/:queue/partition/:partition — pop from ONE named
// partition. Same query params + long-poll + lease/leaseId semantics as the
// wildcard path; only the SP call and response adapter differ (single-partition
// shape). `partitions` is ignored here (a specific pop is one partition).
pub async fn handle_pop_partition(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path((queue, partition)): Path<(String, String)>,
    Query(p): Query<PopParams>,
) -> Response {
    let batch = p.batch.unwrap_or(200);
    let auto_ack = p.auto_ack.unwrap_or(false);
    if let Some(r) = conflation_refusal(p.conflation, p.consumer_group.is_some(), auto_ack) {
        return r;
    }
    let from = p.subscription_from.as_deref();
    crate::handlers::raft::dispatch_pop_partition(
        &st,
        tenant.as_str(),
        queue,
        partition,
        p.consumer_group.clone(),
        batch,
        auto_ack,
        p.wait.unwrap_or(false),
        p.timeout.unwrap_or(st.pop_default_timeout_ms),
        crate::rsm::facade::PopOptions {
            auto_parts: false,
            auto_batch: false,
            max_parts: 1,
            lease_seconds: p.lease_seconds.unwrap_or(0),
            subscription_mode: p
                .subscription_mode
                .as_deref()
                .map(crate::config::normalize_subscription_mode)
                .unwrap_or_else(|| st.default_subscription_mode.clone()),
            subscription_from_us: from
                .filter(|v| !v.eq_ignore_ascii_case("now"))
                .and_then(crate::util::parse_iso_ms)
                .map(|ms| ms.saturating_mul(1_000)),
            subscription_from_now: from.is_some_and(|v| v.eq_ignore_ascii_case("now")),
            conflate: p.conflation == Some(true) && p.consumer_group.is_some(),
            conflate_requested: p.conflation,
        },
    )
    .await
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
    // RUSTFIX item 18: per-request lease override; 0/absent lets each discovered
    // partition use its own queue's queues.lease_time (else 60).
    #[serde(rename = "leaseSeconds")]
    lease_seconds: Option<i32>,
    #[serde(rename = "consumerGroup")]
    consumer_group: Option<String>,
    namespace: Option<String>,
    task: Option<String>,
    #[serde(rename = "subscriptionMode")]
    subscription_mode: Option<String>,
    #[serde(rename = "subscriptionFrom")]
    subscription_from: Option<String>,
    // PLAN_CONFLATION §3.1 — same query parameter as the queue-scoped routes.
    #[serde(rename = "conflation")]
    conflation: Option<bool>,
}

// GET /api/v1/pop?namespace=&task=&consumerGroup=... — namespace/task discovery
// pop. Resolves every queue whose namespace/task matches the request and pops
// across their partitions in one call, returning the SAME response shape as
// handle_pop. Same lease/leaseId semantics; ack/attempt work identically.
// At least one of namespace/task must be provided (the clients never send a bare
// pop without one — QueueBuilder.pop throws first — so a neither-provided call is
// a 400 rather than an unbounded scan of every queue).
pub async fn handle_pop_discover(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Query(p): Query<PopDiscoverParams>,
) -> Response {
    if p.namespace.as_deref().unwrap_or_default().is_empty()
        && p.task.as_deref().unwrap_or_default().is_empty()
    {
        return json(
            StatusCode::BAD_REQUEST,
            "{\"success\":false,\"error\":\"namespace or task is required\",\"messages\":[]}"
                .to_string(),
        );
    }
    let batch = p.batch.unwrap_or(200);
    let auto_ack = p.auto_ack.unwrap_or(false);
    if let Some(r) = conflation_refusal(p.conflation, p.consumer_group.is_some(), auto_ack) {
        return r;
    }
    let from = p.subscription_from.as_deref();
    let conflate = p.conflation == Some(true) && p.consumer_group.is_some();
    crate::handlers::raft::dispatch_pop_discover(
        &st,
        tenant.as_str(),
        p.namespace.clone().unwrap_or_default(),
        p.task.clone().unwrap_or_default(),
        p.consumer_group.clone(),
        batch,
        auto_ack,
        p.wait.unwrap_or(false),
        p.timeout.unwrap_or(st.pop_default_timeout_ms),
        crate::rsm::facade::PopOptions {
            auto_parts: false,
            auto_batch: false,
            max_parts: if conflate {
                p.partitions.unwrap_or(batch).clamp(1, 64) as u32
            } else {
                p.partitions.unwrap_or(1).clamp(1, 64) as u32
            },
            lease_seconds: p.lease_seconds.unwrap_or(0),
            subscription_mode: p
                .subscription_mode
                .as_deref()
                .map(crate::config::normalize_subscription_mode)
                .unwrap_or_else(|| st.default_subscription_mode.clone()),
            subscription_from_us: from
                .filter(|v| !v.eq_ignore_ascii_case("now"))
                .and_then(crate::util::parse_iso_ms)
                .map(|ms| ms.saturating_mul(1_000)),
            subscription_from_now: from.is_some_and(|v| v.eq_ignore_ascii_case("now")),
            conflate,
            conflate_requested: p.conflation,
        },
    )
    .await
}

// Append raw bytes that are expected to be valid UTF-8 (payloads stored from
// client JSON). std's from_utf8 validation is markedly cheaper than the lossy
// chunk iterator; invalid bytes fall back to lossy replacement.
// pub(crate) so the fetch renderer (handlers/fetch.rs) splices payloads through
// the SAME function the pop renderer does — one lossy-UTF8 policy, not two.
pub(crate) fn push_utf8(out: &mut String, bytes: &[u8]) {
    match std::str::from_utf8(bytes) {
        Ok(s) => out.push_str(s),
        Err(_) => out.push_str(&String::from_utf8_lossy(bytes)),
    }
}

// ------------------------------------------------------------------- ack
pub async fn handle_ack(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    body: Bytes,
) -> Response {
    crate::handlers::raft::dispatch_ack(&st, tenant.as_str(), body).await
}

pub async fn handle_ack_batch(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    body: Bytes,
) -> Response {
    crate::handlers::raft::dispatch_ack(&st, tenant.as_str(), body).await
}

// ----------------------------------------------------------- lease renew
// ---------------------------------------------------------------- lease/extend
// POST /api/v1/lease/:leaseId/extend  body {"seconds":60} (default 60).
// Renews every lease held by :leaseId (= the worker id minted at pop). Always
// HTTP 200 (best-effort renewal). The response carries every key the clients
// read:
//   JS:  result.leaseId ? result.newExpiresAt : result.lease_expires_at
//   Go:  result["newExpiresAt"] (RFC3339 string)
#[derive(Deserialize)]
struct RenewBody {
    seconds: Option<i32>,
}

pub async fn handle_lease_extend(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
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

    crate::handlers::raft::dispatch_lease_extend(&st, tenant.as_str(), lease_id, seconds as i64)
        .await
}

// ------------------------------------------------------------ transaction
pub async fn handle_transaction(
    State(st): State<Arc<AppState>>,
    Extension(authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    body: Bytes,
) -> Response {
    // As on the standalone push path, identity comes only from validated auth.
    let producer_sub = authed.0.filter(|s| !s.is_empty());
    crate::handlers::raft::dispatch_transaction(&st, tenant.as_str(), producer_sub, body).await
}
