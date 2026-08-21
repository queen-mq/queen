//! THE THREE HOT VERBS of the ephemeral class — EPHEMERAL_QUEUES.md §3.1/§3.3.
//!
//! push, pop and ack. The management verbs (`configure`, `reset`, `delete`, the
//! status reads) are a later phase and are deliberately absent rather than
//! stubbed: a route that exists and answers "not yet" is a route an SDK will
//! ship against.
//!
//! ---------------------------------------------------------------------------
//! THE PROPERTY THIS FILE EXISTS TO KEEP
//!
//! Nothing here touches the pool, builds a statement, or names a table. That is
//! not hygiene, it is the entire performance premise of the class: a req/reply
//! inbox costs one hash lookup and a `VecDeque` push, and the moment one of
//! these three functions acquires a connection the class is just the durable
//! engine with a worse durability story. `tests/kv_handler_isolation.rs` greps
//! every source under `src/handlers/` for the feature's table names and fails
//! the suite on a hit — including on prose, which is why the two tables are not
//! named anywhere in this file.
//!
//! ---------------------------------------------------------------------------
//! WHY THE LONG POLL COSTS NOTHING HERE (§3.4)
//!
//! The durable pop parks on a deadline and RE-QUERIES on a backoff, because the
//! authority is a database that may have been written by another broker. Here
//! the authority is this process's heap, and a push into it wakes the gate
//! directly — so the wait is purely event-driven: no `pop_backoff_interval`, no
//! re-poll, no probe. A missed wake costs the remaining timeout and never
//! correctness, exactly as on the durable path.
//!
//! ---------------------------------------------------------------------------
//! THE TENANT (`kv.rs`'s rule, restated because it is the one that bites)
//!
//! The tenant comes from `Extension<Tenant>`, i.e. from the middleware that
//! reads the trusted header, and NEVER from a request body. Every engine map key
//! is built by `Ephemeral::qkey`, which is `tenant_queue_key(tenant, "eph:" +
//! name)`; there is no other way into the engine's maps.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Extension, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use serde::Deserialize;
use serde_json::value::RawValue;

use super::{json, qbool, qint, AppState};
use crate::ephemeral::{self, AckOutcome, AckStatus, Refusal};
use crate::tenant::Tenant;

// ---------------------------------------------------------------------------
// Edge shape limits.
//
// These are BODY guards, not policy: they exist so a malformed or hostile
// request is refused before it can allocate, and every one of them is a
// constant rather than a knob because none of them is a decision an operator
// would ever want to make differently. The resource limits that ARE decisions
// (bytes, length, ttl, lease) live on the queue's config and in `config.rs`.
// ---------------------------------------------------------------------------

/// Longest queue / partition / group name accepted. The durable engine has no
/// explicit cap because its names land in `VARCHAR(255)` columns that impose
/// one; nothing here reaches a database, so the cap has to be written down.
const MAX_NAME_BYTES: usize = 512;

/// Messages per push call and ids per ack call. All-or-nothing per request
/// (§3.1) makes an unbounded array a single allocation the caller chooses.
const MAX_ITEMS_PER_CALL: usize = 10_000;

/// Ceiling on a pop's `batch`, and on its `timeout`.
const MAX_BATCH: i64 = 10_000;
const MAX_TIMEOUT_MS: i64 = 300_000;

// ---------------------------------------------------------------------------
// The `{error, code}` envelope (§3.1).
//
// `code` is a stable identifier from a closed taxonomy and is the only field a
// client may branch on; `error` is the human half and may be reworded at any
// time. The house rule everywhere else in this codebase — string matching on a
// message is forbidden — is what makes that split load-bearing rather than
// decorative.
// ---------------------------------------------------------------------------

fn err(status: StatusCode, code: &str, message: &str) -> Response {
    let mut body = String::with_capacity(64 + message.len());
    body.push_str("{\"error\":\"");
    crate::fusion::json_escape_into(&mut body, message);
    body.push_str("\",\"code\":\"");
    body.push_str(code);
    body.push_str("\"}");
    json(status, body)
}

fn bad_request(message: &str) -> Response {
    err(StatusCode::BAD_REQUEST, "ephemeral_bad_request", message)
}

/// The refusals of §1.6, rendered in ONE place so two call sites cannot answer
/// differently for the same condition (`quota.rs`'s `Verdict::http` discipline).
fn refusal(r: Refusal) -> Response {
    match r {
        // The queue's own policy said no. 429 and not 507/503: this is
        // BACKPRESSURE, and the shape every 1.0.6 SDK's bounded buffer already
        // knows how to drain against.
        Refusal::QueueFull => err(
            StatusCode::TOO_MANY_REQUESTS,
            "queue_full",
            "the ephemeral queue is at its maxBytes/maxLength and its policy is reject",
        ),
        // A CELL condition, not the tenant's doing: 503, which is what tells a
        // client to come back rather than to fix its request.
        Refusal::NoRoom => err(
            StatusCode::SERVICE_UNAVAILABLE,
            "ephemeral_unavailable",
            "this broker is at QUEEN_EPHEMERAL_MAX_BYTES",
        ),
        // Unreachable until the grant ladder lands (see `gated`); rendered
        // anyway so the arm is not written for the first time under pressure.
        Refusal::TenantQuota => err(
            StatusCode::FORBIDDEN,
            "ephemeral_quota_exceeded",
            "this tenant is at its ephemeral byte allowance",
        ),
    }
}

/// THE HOOK POINT for phase T1c — the three-rung ladder of §1.6/M7.
///
/// When it lands this becomes one call to `switches::decide` with new
/// `Surface::{EphPush, EphPop, EphAck}` variants, rendered through
/// `Answer::http` exactly as `kv.rs::gated` does: rung 1 the operator's runtime
/// kill switch (503 `ephemeral_disabled`), rung 2 the grant (403
/// `feature_gated`), rung 3 the per-tenant occupancy and rate (403
/// `ephemeral_quota_exceeded` / 429 `rate_limited`).
///
/// It is a FUNCTION and not three inline checks precisely so that the order of
/// the rungs is decided once: the answer must name the OUTERMOST reason, or a
/// prober learns that a tenant exists from a quota message that leaked past a
/// switch an operator pulled.
///
/// Returning `None` unconditionally is deliberate for this phase. The cell-wide
/// byte ceiling (rung 3's global half) IS enforced — inside the engine, where
/// the bytes are — so nothing in T1a is unbounded; what is missing is only the
/// per-TENANT half, which needs the grant table this phase does not read.
#[inline]
fn gated(_st: &AppState, _tenant: &str) -> Option<Response> {
    None
}

// ---------------------------------------------------------------------------
// Name validation.
//
// Mirrors the durable push path's ONE charset rule (`handlers/data.rs`:
// control characters are rejected in queue, partition and transactionId,
// because those fields are joined on `\x1f` to build composite keys). The same
// join happens here — `tenant_queue_key` — so the same rule applies, and for
// the same reason: a `\x1f` inside a name would alias two distinct queues onto
// one engine key.
// ---------------------------------------------------------------------------

fn check_name(what: &str, v: &str) -> Result<(), Response> {
    if v.is_empty() {
        return Err(bad_request(&format!("{what} must not be empty")));
    }
    if v.len() > MAX_NAME_BYTES {
        return Err(bad_request(&format!(
            "{what} is longer than {MAX_NAME_BYTES} bytes"
        )));
    }
    if v.as_bytes().iter().any(|&b| b < 0x20) {
        return Err(bad_request(&format!(
            "control characters are not allowed in {what}"
        )));
    }
    Ok(())
}

// ===========================================================================
// push
// ===========================================================================

#[derive(Deserialize)]
struct PushMsg<'a> {
    #[serde(borrow)]
    payload: &'a RawValue,
}

#[derive(Deserialize)]
struct PushBody<'a> {
    queue: String,
    partition: Option<String>,
    #[serde(borrow)]
    messages: Vec<PushMsg<'a>>,
}

/// `POST /api/v1/ephemeral/push` — one queue per request, all-or-nothing,
/// 201 `{pushed}`.
///
/// ONE QUEUE PER REQUEST, unlike the durable push whose items each name their
/// own queue. That is not a simplification: the durable form exists so a bundle
/// spanning queues can share ONE transaction, and there is no transaction here
/// to share. A flat body is what lets the SDK's buffered sink keep one drain
/// loop per (queue, partition) address with no regrouping pass (§4.1).
pub async fn handle_ephemeral_push(
    State(st): State<Arc<AppState>>,
    Extension(_authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<Tenant>,
    body: Bytes,
) -> Response {
    if let Some(r) = gated(&st, tenant.as_str()) {
        return r;
    }
    let parsed: PushBody = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return bad_request(&format!("bad body: {e}")),
    };
    if let Err(r) = check_name("queue", &parsed.queue) {
        return r;
    }
    let partition = parsed.partition.as_deref().unwrap_or(ephemeral::DEFAULT_PARTITION);
    if let Err(r) = check_name("partition", partition) {
        return r;
    }
    if parsed.messages.is_empty() {
        return json(StatusCode::CREATED, "{\"pushed\":0}".to_string());
    }
    if parsed.messages.len() > MAX_ITEMS_PER_CALL {
        return bad_request(&format!("at most {MAX_ITEMS_PER_CALL} messages per push"));
    }

    // COPY OUT of the request buffer, here and nowhere else. Holding a
    // `Bytes` slice would keep the whole request allocation alive for as long
    // as the message lives — with thousands of live inboxes and a 64 MiB body
    // limit that is the difference between a few MB of rings and the cell.
    let payloads: Vec<Box<[u8]>> = parsed
        .messages
        .iter()
        .map(|m| m.payload.get().as_bytes().to_vec().into_boxed_slice())
        .collect();
    let n = payloads.len();

    let now = crate::util::now_epoch_ms();
    match st.ephemeral.push(tenant.as_str(), &parsed.queue, partition, payloads, now) {
        Ok(pushed) => {
            // §3.4 — the hotlist-OFF direct wake. Ephemeral never enters the
            // hot list or its ~5 ms coalescing tick: the list exists to make a
            // wildcard SQL candidate scan cheap, and there is no scan here.
            let qkey = ephemeral::Ephemeral::qkey(tenant.as_str(), &parsed.queue);
            st.notifier.notify_pushed_batch(&[(qkey, partition.to_string())]);
            json(StatusCode::CREATED, format!("{{\"pushed\":{pushed}}}"))
        }
        Err(r) => {
            debug_assert!(n > 0);
            refusal(r)
        }
    }
}

// ===========================================================================
// pop
// ===========================================================================

/// `GET /api/v1/ephemeral/pop` — `?queue&partition&batch&wait&timeout&group&autoAck`.
///
/// 200 `{queue, messages:[{id, partition, payload, attempts}]}`, with an EMPTY
/// array on timeout rather than a 204: the durable pop's 204 exists because its
/// empty body carried no information and an announced content-length on an
/// elided body poisoned strict HTTP/1.1 clients. Here the body carries the
/// queue name, so there is something to send and one shape for every outcome.
pub async fn handle_ephemeral_pop(
    State(st): State<Arc<AppState>>,
    Extension(_authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<Tenant>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if let Some(r) = gated(&st, tenant.as_str()) {
        return r;
    }
    let Some(queue) = q.get("queue").map(String::as_str) else {
        return bad_request("queue is required");
    };
    if let Err(r) = check_name("queue", queue) {
        return r;
    }
    let partition = q.get("partition").map(String::as_str).filter(|s| !s.is_empty());
    if let Some(p) = partition {
        if let Err(r) = check_name("partition", p) {
            return r;
        }
    }
    let group = q.get("group").map(String::as_str).filter(|s| !s.is_empty());
    if let Some(g) = group {
        if let Err(r) = check_name("group", g) {
            return r;
        }
    }
    let batch = qint(&q, "batch", 1).clamp(1, MAX_BATCH as i32) as usize;
    let wait = qbool(&q, "wait", false);
    let auto_ack = qbool(&q, "autoAck", false);
    let timeout_ms = q
        .get("timeout")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(st.pop_default_timeout_ms as i64)
        .clamp(0, MAX_TIMEOUT_MS);

    // Built once: the notifier's gate key and the parked gauge's queue half are
    // the same namespaced name, so an ephemeral queue and a durable queue of
    // the same name never share either (§10 Q8).
    let eph_name = format!("{}{}", ephemeral::EPH_PREFIX, queue);
    let qkey = crate::handlers::tenant_queue_key(tenant.as_str(), &eph_name);
    let window = st.ephemeral.window(tenant.as_str(), queue);

    let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(0) as u64);
    let mut held: Vec<ephemeral::Delivered> = Vec::new();
    let mut first_at: Option<Instant> = None;
    // Held OUTSIDE the loop so the gauge covers the whole park, including the
    // window-fattening legs — a gauge that only counted the first wait would
    // under-report exactly the pops that wait longest.
    let mut parked_guard: Option<crate::metrics::ParkedGuard> = None;

    loop {
        let now = crate::util::now_epoch_ms();
        let got = st.ephemeral.pop(
            tenant.as_str(),
            queue,
            partition,
            group,
            batch - held.len(),
            auto_ack,
            now,
        );
        if !got.is_empty() {
            if first_at.is_none() {
                first_at = Some(Instant::now());
            }
            held.extend(got);
        }

        let waited_ms = first_at.map_or(0, |t| t.elapsed().as_millis() as i64);
        if ephemeral::window_ready(held.len(), batch, waited_ms, window) {
            break;
        }
        if !wait {
            break;
        }
        // §1.7 — the window is bounded by the pop's OWN timeout, never the
        // other way round: a 5 s window on a 100 ms pop must not hold the
        // response for 5 s.
        let effective = match first_at {
            Some(t) if window.enabled() && window.ms > 0 => {
                deadline.min(t + Duration::from_millis(window.ms))
            }
            _ => deadline,
        };
        let now_i = Instant::now();
        if now_i >= effective {
            break;
        }
        if parked_guard.is_none() {
            parked_guard = Some(st.metrics.parked.enter(tenant.as_str(), &eph_name));
        }
        // PURE EVENT WAIT. No probe, no backoff, no re-query — there is no
        // second authority to consult, so a wake is the only thing that can
        // change the answer. This is the structural reason the pop floor of
        // this class is transport time.
        st.notifier.wait_queue(&qkey, effective - now_i).await;
    }
    drop(parked_guard);

    render_pop(queue, &held)
}

/// Hand-rolled, because `payload` is raw JSON that must be re-emitted VERBATIM.
/// Round-tripping it through `serde_json::Value` would re-order object keys and
/// normalize numbers — a broker that silently rewrites a payload is a broker
/// nobody can checksum against.
fn render_pop(queue: &str, msgs: &[ephemeral::Delivered]) -> Response {
    let mut out = String::with_capacity(128 + msgs.iter().map(|m| m.payload.len() + 96).sum::<usize>());
    out.push_str("{\"queue\":\"");
    crate::fusion::json_escape_into(&mut out, queue);
    out.push_str("\",\"messages\":[");
    for (i, m) in msgs.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"id\":\"");
        // The id is broker-minted from an epoch, a partition name and a seq;
        // only the partition half can carry anything worth escaping.
        crate::fusion::json_escape_into(&mut out, &m.id);
        out.push_str("\",\"partition\":\"");
        crate::fusion::json_escape_into(&mut out, &m.partition);
        out.push_str("\",\"attempts\":");
        out.push_str(&m.attempts.to_string());
        out.push_str(",\"payload\":");
        // Valid JSON by construction: it was parsed as a `RawValue` at push.
        out.push_str(&String::from_utf8_lossy(&m.payload));
        out.push('}');
    }
    out.push_str("]}");
    json(StatusCode::OK, out)
}

// ===========================================================================
// ack
// ===========================================================================

#[derive(Deserialize)]
struct AckOne {
    id: String,
    status: Option<String>,
    /// Accepted and IGNORED, on purpose. The durable wire carries an error
    /// string because it lands in `log_dlq` and in the trace store; this class
    /// has neither (§9), so storing it would mean inventing a place to put it.
    /// Refusing the field instead would break every SDK that shares one ack
    /// builder across the two engines, which is the shape §4 asks for.
    #[allow(dead_code)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct AckBody {
    queue: String,
    group: Option<String>,
    acks: Vec<AckOne>,
}

/// `POST /api/v1/ephemeral/ack` — 200 `{results:[{id, outcome}]}`.
///
/// PER-ID OUTCOMES and no failure status, because on this class the interesting
/// answers are not errors: `stale` means the id was minted by an incarnation
/// that is gone (a restart — the loss contract, §1.2, not a bug) and `unknown`
/// means the lease is no longer ours to release (already acked, already expired
/// and redelivered). A client that reconnects after a broker restart flushes its
/// outstanding acks and gets a row of `stale`, which is information, where a
/// 4xx per id would be a retry storm.
pub async fn handle_ephemeral_ack(
    State(st): State<Arc<AppState>>,
    Extension(_authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<Tenant>,
    body: Bytes,
) -> Response {
    if let Some(r) = gated(&st, tenant.as_str()) {
        return r;
    }
    let parsed: AckBody = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return bad_request(&format!("bad body: {e}")),
    };
    if let Err(r) = check_name("queue", &parsed.queue) {
        return r;
    }
    if let Some(g) = parsed.group.as_deref().filter(|s| !s.is_empty()) {
        if let Err(r) = check_name("group", g) {
            return r;
        }
    }
    if parsed.acks.len() > MAX_ITEMS_PER_CALL {
        return bad_request(&format!("at most {MAX_ITEMS_PER_CALL} acks per call"));
    }
    let items: Vec<(String, AckStatus)> = parsed
        .acks
        .into_iter()
        .map(|a| (a.id, AckStatus::parse(a.status.as_deref())))
        .collect();

    let now = crate::util::now_epoch_ms();
    let results = st.ephemeral.ack(
        tenant.as_str(),
        &parsed.queue,
        parsed.group.as_deref(),
        &items,
        now,
    );
    // A `failed`/`retry` ack put messages back on a group's redelivery queue,
    // and a consumer of that group may be parked right now. Waking on the ack
    // is what makes a nack's redelivery immediate instead of one lease-expiry
    // late — the durable engine cannot do this because its redelivery is a
    // committed cursor move it does not observe locally.
    if results.iter().any(|(_, o)| *o == AckOutcome::Redelivered) {
        let qkey = ephemeral::Ephemeral::qkey(tenant.as_str(), &parsed.queue);
        st.notifier.notify_pushed_batch(&[(qkey, String::new())]);
    }
    render_ack(&results)
}

fn render_ack(results: &[(String, AckOutcome)]) -> Response {
    let mut out = String::with_capacity(32 + results.len() * 64);
    out.push_str("{\"results\":[");
    for (i, (id, outcome)) in results.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"id\":\"");
        crate::fusion::json_escape_into(&mut out, id);
        out.push_str("\",\"outcome\":\"");
        out.push_str(outcome.as_str());
        out.push_str("\"}");
    }
    out.push_str("]}");
    json(StatusCode::OK, out)
}
