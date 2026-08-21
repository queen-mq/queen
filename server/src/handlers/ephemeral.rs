//! THE EPHEMERAL HTTP SURFACE — EPHEMERAL_QUEUES.md §3.1/§3.3.
//!
//! Two halves, and the line between them is the point of the whole feature:
//!
//!   * the THREE HOT VERBS — push, pop, ack — which touch this process's heap
//!     and nothing else;
//!   * the FIVE MANAGEMENT VERBS — configure, reset, delete and the two status
//!     reads — of which exactly three (configure, delete, and nothing on the
//!     status path) take a pooled connection, and only ever to read or write a
//!     queue's DECLARATION. Never a message.
//!
//! ---------------------------------------------------------------------------
//! THE PROPERTY THIS FILE EXISTS TO KEEP
//!
//! No hot verb touches the pool, builds a statement, or names a table. That is
//! not hygiene, it is the entire performance premise of the class: a req/reply
//! inbox costs one hash lookup and a `VecDeque` push, and the moment push, pop
//! or ack acquires a connection the class is just the durable engine with a
//! worse durability story. `tests/kv_handler_isolation.rs` greps every source
//! under `src/handlers/` for the feature's table names and fails the suite on a
//! hit — including on prose, which is why the two tables are not named anywhere
//! in this file, not even in the management half: the SQL lives behind the
//! `db::eph_*` wrappers, which are the only code allowed to bind `p_tenant`.
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
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use serde::Deserialize;
use serde_json::value::RawValue;

use super::{json, qbool, qint, AppState};
use crate::db;
use crate::ephemeral::{self, AckOutcome, AckStatus, Refusal};
use crate::switches::{decide_ephemeral, Origin, Surface};
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
        // Rung 2's occupancy half (§1.6): the tenant's own byte or object
        // allowance, from its grant row. 403 and never 429, for the reason
        // `quota.rs` states once for the whole product: a `Retry-After` on a
        // capacity quota is a lie, because no delay resolves it.
        Refusal::TenantQuota => err(
            StatusCode::FORBIDDEN,
            "ephemeral_quota_exceeded",
            "this tenant is at its ephemeral byte or queue allowance",
        ),
    }
}

/// THE ONE CHOKE POINT — the three-rung ladder of §1.6/M7.
///
/// Rung 1 the operator's runtime kill switch (503 `ephemeral_disabled`), rung 2
/// the grant (403 `feature_gated`), rung 3 the per-tenant message rate (429
/// `rate_limited` + `Retry-After`). The per-tenant BYTE and OBJECT allowances are
/// the fourth thing this ladder is responsible for and they are deliberately not
/// tested here: they are charged atomically inside the engine, where the bytes
/// are, and come back as `Refusal::TenantQuota` → 403 `ephemeral_quota_exceeded`
/// through `refusal` above. A majorant out here that could disagree with the
/// enforcer is the one failure mode a quota must not have.
///
/// It is a FUNCTION and not three inline checks precisely so that the order of
/// the rungs is decided once: the answer must name the OUTERMOST reason, or a
/// prober learns that a tenant exists from a quota message that leaked past a
/// switch an operator pulled (§13.5, error hygiene).
///
/// EVERY handler in this file opens with it, including the two status reads: a
/// paused surface that still answered depth would be telling an operator the
/// feature is running.
fn gated(st: &AppState, tenant: &str, surface: Surface, add_msgs: i64) -> Option<Response> {
    let a = decide_ephemeral(&st.switches, &st.ephemeral, tenant, surface, add_msgs);
    let h = a.http(Origin::Route, surface)?;
    let status = StatusCode::from_u16(h.status).unwrap_or(StatusCode::FORBIDDEN);
    // The human half is chosen from the CODE and not from the rung, so the two
    // cannot drift: a client branches on `code` (§3.1) and a human reads `error`.
    let message = match h.code {
        "ephemeral_disabled" => {
            "an operator has paused the ephemeral surface on this broker; nothing stored \
             durably is affected"
        }
        "feature_gated" => "this tenant is not granted the ephemeral queue class",
        "rate_limited" => "this tenant is over its ephemeral messages-per-second allowance",
        _ => "this broker cannot serve ephemeral queues right now",
    };
    let body = err(status, h.code, message);
    Some(match h.retry_after {
        Some(secs) => with_retry_after(body, secs),
        None => body,
    })
}

/// Stamp `Retry-After` on an already-rendered refusal.
///
/// Written as a mutation of the response instead of a second renderer, because
/// the `{error, code}` envelope must have exactly one producer in this file —
/// two would be two places for the taxonomy to drift. Only ever applied to the
/// statuses where a delay is honest (429 and 503); a 403 carrying one would be
/// telling the client that waiting helps.
fn with_retry_after(mut resp: Response, secs: u32) -> Response {
    if let Ok(v) = axum::http::HeaderValue::from_str(&secs.to_string()) {
        resp.headers_mut().insert(axum::http::header::RETRY_AFTER, v);
    }
    resp
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
    // PARSED BEFORE GATED, and that order is the `kv.rs::apply_ops` shape rather
    // than an oversight: rung 3 of the ladder is a MESSAGES-per-second rate, and
    // the number of messages is a property of the body. Gating first would mean
    // charging one token per CALL, which a class that encourages batching would
    // defeat with a single request. Parsing is pure CPU on a body the edge has
    // already capped, and nothing is admitted before the gate speaks.
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
    if let Some(r) = gated(&st, tenant.as_str(), Surface::EphPush, parsed.messages.len() as i64) {
        return r;
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
    if let Some(r) = gated(&st, tenant.as_str(), Surface::EphPop, 0) {
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
    if let Some(r) = gated(&st, tenant.as_str(), Surface::EphAck, 0) {
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

// ===========================================================================
// THE MANAGEMENT HALF — configure / reset / delete / status (§3.1)
// ===========================================================================
//
// CROSS-BROKER PROPAGATION IS NOT HERE, and its absence is deliberate rather
// than forgotten. `configure`, `reset` and `delete` act on THIS broker's rings
// and on the shared config rows; on a multi-broker cell the other brokers keep
// their own rings until the config refresh reaches them (§10 Q4 accepts a ghost
// ring for at most one refresh interval, and implicit rings die of idle GC
// anyway). The mesh phase adds one `T_EPH_ADMIN` broadcast at each of the three
// sites below and changes nothing else — see EPHEMERAL_QUEUES.md §3.5.
//
// MESH HOOK (§3.5): broadcast {op, tenant, queue} here when the frame lands.

/// The CLOSED option list of §3.1. An option not in this array is a 400, not a
/// silently ignored field.
///
/// WHY REJECTING IS WORTH A BREAKING-CHANGE RISK. Every knob here has a default,
/// so an ignored typo (`ttlSecond`, `maxByte`) produces a queue that looks
/// configured, behaves like the default and drops data for a reason its owner
/// cannot see — on a class whose whole contract is that dropping is legal. The
/// cost is that a client written against a LATER broker gets a 400 from an older
/// one instead of a partial application; that is the correct direction, and the
/// boot load is deliberately the opposite (it ignores unknown keys) because it is
/// reading back rows a future version may have written.
const OPTION_KEYS: [&str; 7] = [
    "maxBytes",
    "maxLength",
    "policy",
    "ttlSeconds",
    "leaseSeconds",
    "retryLimit",
    "windowBuffer",
];

#[derive(Deserialize)]
struct ConfigureBody {
    queue: String,
    /// Absent = declare with the broker defaults, which is a legitimate thing to
    /// want: the reason to declare a queue may be purely that it must survive a
    /// restart and appear in the listing (§1.1 tier 2), not that any knob differs.
    options: Option<serde_json::Value>,
}

/// Validate the option blob against the closed list and turn it into engine
/// options. `Err` is the rendered 400.
fn parse_options(v: &serde_json::Value) -> Result<(ephemeral::QueueOptions, String), Response> {
    let mut o = ephemeral::QueueOptions::default();
    let Some(map) = v.as_object() else {
        return Err(bad_request("options must be an object"));
    };
    for k in map.keys() {
        if !OPTION_KEYS.contains(&k.as_str()) {
            return Err(bad_request(&format!(
                "unknown option `{k}`; the ephemeral options are {}",
                OPTION_KEYS.join(", ")
            )));
        }
    }
    // Every numeric option is read as an i64 and rejected — not truncated — when
    // it is not one. A float `ttlSeconds: 1.5` silently becoming 1 is the same
    // class of invisible surprise as an ignored key.
    let num = |k: &str| -> Result<Option<i64>, Response> {
        match map.get(k) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(x) => match x.as_i64() {
                Some(n) if n >= 0 => Ok(Some(n)),
                _ => Err(bad_request(&format!("{k} must be a non-negative integer"))),
            },
        }
    };
    o.max_bytes = num("maxBytes")?.filter(|n| *n > 0);
    o.max_length = num("maxLength")?.filter(|n| *n > 0);
    // 0 is MEANINGFUL on these two and must survive the filter above: it is how
    // an operator turns an age limit back off on a declared queue.
    o.ttl_ms = num("ttlSeconds")?.map(|s| s.saturating_mul(1000));
    o.lease_ms = num("leaseSeconds")?.filter(|n| *n > 0).map(|s| s.saturating_mul(1000));
    o.retry_limit = num("retryLimit")?.map(|n| n.clamp(0, u32::MAX as i64) as u32);
    if let Some(p) = map.get("policy") {
        let Some(s) = p.as_str().and_then(ephemeral::Policy::parse) else {
            return Err(bad_request("policy must be \"reject\" or \"dropOldest\""));
        };
        o.policy = Some(s);
    }
    if let Some(w) = map.get("windowBuffer") {
        let Some(wo) = w.as_object() else {
            return Err(bad_request("windowBuffer must be an object {ms?, count?}"));
        };
        for k in wo.keys() {
            if k != "ms" && k != "count" {
                return Err(bad_request(&format!("unknown windowBuffer field `{k}`")));
            }
        }
        let g = |k: &str| wo.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
        o.window = Some(ephemeral::Window { ms: g("ms"), count: g("count") as usize });
    }
    Ok((o, v.to_string()))
}

/// `POST /api/v1/ephemeral/configure` — 201 with the stored declaration.
///
/// TWO WRITES, DATABASE FIRST. The row is what survives a restart (§1.2), so a
/// broker that applied the config to RAM and then failed to persist it would
/// serve a configuration that silently reverts at the next deploy — the worst of
/// the two failure orders. Persisting first means the opposite failure (row
/// written, broker dies before applying) self-heals on the next boot load.
pub async fn handle_ephemeral_configure(
    State(st): State<Arc<AppState>>,
    Extension(_authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<Tenant>,
    body: Bytes,
) -> Response {
    if let Some(r) = gated(&st, tenant.as_str(), Surface::EphAdmin, 0) {
        return r;
    }
    let parsed: ConfigureBody = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return bad_request(&format!("bad body: {e}")),
    };
    if let Err(r) = check_name("queue", &parsed.queue) {
        return r;
    }
    let raw = parsed.options.unwrap_or(serde_json::Value::Object(Default::default()));
    let (opts, blob) = match parse_options(&raw) {
        Ok(v) => v,
        Err(r) => return r,
    };

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => {
            return err(
                StatusCode::SERVICE_UNAVAILABLE,
                "ephemeral_unavailable",
                "no database connection is available to persist the declaration",
            )
        }
    };
    let stored = match db::eph_config_set(&client, tenant.as_str(), &parsed.queue, &blob).await {
        Ok(t) => t,
        Err(e) => {
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ephemeral_configure_failed",
                &format!("could not store the declaration: {e}"),
            )
        }
    };
    st.ephemeral.set_config(tenant.as_str(), &parsed.queue, opts, true);
    // The SP's own row, echoed verbatim: the broker must not re-render what it
    // just stored, or the echo and the table can disagree about what was saved.
    // The CLAMPED values the engine actually applied are what the status reads
    // publish — the two are different questions and are answered separately.
    json(StatusCode::CREATED, stored)
}

#[derive(Deserialize)]
struct QueueOnlyBody {
    queue: String,
}

/// `POST /api/v1/ephemeral/reset` — 200 `{queue, dropped}`.
///
/// Drops every message, voids every lease and rewinds every group cursor. It is
/// legal only because of §1.2 and it is the one destructive verb on this class
/// that is destructive ON PURPOSE. A queue that is not here answers `dropped:0`
/// rather than 404: an implicit inbox that has been idle-collected is
/// indistinguishable from one that was never used, and both are correctly
/// described as "there is nothing left to drop".
pub async fn handle_ephemeral_reset(
    State(st): State<Arc<AppState>>,
    Extension(_authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<Tenant>,
    body: Bytes,
) -> Response {
    if let Some(r) = gated(&st, tenant.as_str(), Surface::EphAdmin, 0) {
        return r;
    }
    let parsed: QueueOnlyBody = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return bad_request(&format!("bad body: {e}")),
    };
    if let Err(r) = check_name("queue", &parsed.queue) {
        return r;
    }
    let dropped = st.ephemeral.reset(tenant.as_str(), &parsed.queue).unwrap_or(0);
    let mut out = String::with_capacity(64 + parsed.queue.len());
    out.push_str("{\"queue\":\"");
    crate::fusion::json_escape_into(&mut out, &parsed.queue);
    out.push_str("\",\"dropped\":");
    out.push_str(&dropped.to_string());
    out.push('}');
    json(StatusCode::OK, out)
}

/// `DELETE /api/v1/ephemeral/queue/:queue` — 200 `{queue, deleted, declared}`.
///
/// Both halves: the RAM rings go, and the declaration row goes with them. 200
/// with `deleted:false` on a miss and never a 404 — the same house rule the
/// durable queue delete follows, and the same reason: the status describes the
/// outcome of the CALL, not the verdict of the predicate.
///
/// RAM FIRST here, unlike `configure`. The dangerous residue of a half-done
/// delete is a live ring nobody can see, not a config row: the row is inert
/// (it only decides what a future boot vivifies) while the ring holds memory.
pub async fn handle_ephemeral_delete_queue(
    State(st): State<Arc<AppState>>,
    Extension(_authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<Tenant>,
    Path(queue): Path<String>,
) -> Response {
    if let Some(r) = gated(&st, tenant.as_str(), Surface::EphAdmin, 0) {
        return r;
    }
    if let Err(r) = check_name("queue", &queue) {
        return r;
    }
    let removed = st.ephemeral.remove(tenant.as_str(), &queue);
    let declared;
    match st.pool.get().await {
        Ok(c) => match db::eph_config_delete(&c, tenant.as_str(), &queue).await {
            Ok(txt) => {
                declared = serde_json::from_str::<serde_json::Value>(&txt)
                    .ok()
                    .and_then(|v| v.get("deleted").and_then(|x| x.as_bool()))
                    .unwrap_or(false);
            }
            Err(e) => {
                return err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "ephemeral_delete_failed",
                    &format!("the rings were dropped but the declaration was not: {e}"),
                )
            }
        },
        Err(_) => {
            return err(
                StatusCode::SERVICE_UNAVAILABLE,
                "ephemeral_unavailable",
                "the rings were dropped but no database connection was available to \
                 remove the declaration",
            )
        }
    }
    let mut out = String::with_capacity(80 + queue.len());
    out.push_str("{\"queue\":\"");
    crate::fusion::json_escape_into(&mut out, &queue);
    out.push_str("\",\"deleted\":");
    out.push_str(if removed || declared { "true" } else { "false" });
    out.push_str(",\"declared\":");
    out.push_str(if declared { "true" } else { "false" });
    out.push('}');
    json(StatusCode::OK, out)
}

/// `GET /api/v1/ephemeral/queues` — tenant-scoped list, declared and implicit.
///
/// ZERO DATABASE, on purpose and as a documented property (§5.3): every number
/// here is an in-process gauge, so unlike the durable meter — whose 1 s poll is
/// load-bearing on Postgres — a dashboard may poll this at 1-2 s and it costs
/// the cell nothing but a map walk.
pub async fn handle_ephemeral_queues(
    State(st): State<Arc<AppState>>,
    Extension(_authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<Tenant>,
) -> Response {
    if let Some(r) = gated(&st, tenant.as_str(), Surface::EphAdmin, 0) {
        return r;
    }
    let rows = st.ephemeral.list(tenant.as_str());
    let mut out = String::with_capacity(128 + rows.len() * 256);
    out.push_str("{\"queues\":[");
    for (i, q) in rows.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"queue\":\"");
        crate::fusion::json_escape_into(&mut out, &q.name);
        // The TIER of §1.1, and the one column that says what survives a
        // restart: a declared queue comes back configured and EMPTY, an implicit
        // one does not come back at all.
        out.push_str("\",\"tier\":\"");
        out.push_str(if q.declared { "declared" } else { "implicit" });
        out.push_str("\",\"depth\":");
        out.push_str(&q.depth.to_string());
        out.push_str(",\"bytes\":");
        out.push_str(&q.bytes.to_string());
        out.push_str(",\"partitions\":");
        out.push_str(&q.partitions.to_string());
        out.push_str(",\"groups\":");
        out.push_str(&q.groups.to_string());
        // Three numbers and not one: bounds says the queue is too small (or its
        // producer too fast), ttl that the consumer is too slow, retry that the
        // handlers are failing. A single `drops` would hide which.
        out.push_str(",\"drops\":{\"bounds\":");
        out.push_str(&q.dropped_bounds.to_string());
        out.push_str(",\"ttl\":");
        out.push_str(&q.dropped_ttl.to_string());
        out.push_str(",\"retry\":");
        out.push_str(&q.dropped_retry.to_string());
        // The EFFECTIVE configuration — what the engine clamped the options to,
        // not what was asked for (§2: the engine clamps, the stored blob does
        // not, and this is where the difference becomes visible).
        out.push_str("},\"options\":{\"maxBytes\":");
        out.push_str(&q.config.max_bytes.to_string());
        out.push_str(",\"maxLength\":");
        out.push_str(&q.config.max_length.to_string());
        out.push_str(",\"policy\":\"");
        out.push_str(q.config.policy.as_str());
        out.push_str("\",\"ttlSeconds\":");
        out.push_str(&(q.config.ttl_ms / 1000).to_string());
        out.push_str(",\"leaseSeconds\":");
        out.push_str(&(q.config.lease_ms / 1000).to_string());
        out.push_str(",\"retryLimit\":");
        out.push_str(&q.config.retry_limit.to_string());
        out.push_str(",\"windowBuffer\":{\"ms\":");
        out.push_str(&q.config.window.ms.to_string());
        out.push_str(",\"count\":");
        out.push_str(&q.config.window.count.to_string());
        out.push_str("}}}");
    }
    out.push_str("],\"count\":");
    out.push_str(&rows.len().to_string());
    // Cell-wide, not tenant-wide: it is the number the 503 of §1.6 rung 3 is
    // measured against, and an operator reading a tenant's list during an
    // incident needs to know how close the CELL is.
    out.push_str(",\"cellBytes\":");
    out.push_str(&st.ephemeral.global_bytes().to_string());
    out.push('}');
    json(StatusCode::OK, out)
}

/// `GET /api/v1/ephemeral/queues/:queue/depth` — the durable depth read's shape.
///
/// Same field names as `GET /api/v1/resources/queues/:queue/depth`
/// (`queue`, `group`, `pending`, `partitionsPending`, `partitions[]`) so a relay
/// or a scheduler that already polls one can poll the other with the same
/// parser, and the same 404 on an unknown queue. What is ADDED is what only this
/// class has: `bytes` (the budget of §1.6 is memory, not rows), `tier`, and the
/// per-group `skipped` count — for a fan-out consumer that number is the
/// difference between "slow" and "lost data", which on this class is legal and
/// therefore has to be legible.
///
/// What is deliberately ABSENT is `conflation` / `effectivePending`: there is no
/// conflation on this engine, and a field that was always `false` would be a
/// promise the class does not make.
pub async fn handle_ephemeral_depth(
    State(st): State<Arc<AppState>>,
    Extension(_authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<Tenant>,
    Path(queue): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if let Some(r) = gated(&st, tenant.as_str(), Surface::EphAdmin, 0) {
        return r;
    }
    if let Err(r) = check_name("queue", &queue) {
        return r;
    }
    let group = q.get("group").map(String::as_str).filter(|s| !s.is_empty());
    if let Some(g) = group {
        if let Err(r) = check_name("group", g) {
            return r;
        }
    }
    let Some(d) = st.ephemeral.depth_detail(tenant.as_str(), &queue, group) else {
        return err(
            StatusCode::NOT_FOUND,
            "ephemeral_queue_not_found",
            "no ephemeral queue by that name exists on this broker",
        );
    };
    let mut out = String::with_capacity(160 + d.partitions.len() * 96 + d.groups.len() * 96);
    out.push_str("{\"queue\":\"");
    crate::fusion::json_escape_into(&mut out, &d.queue);
    out.push_str("\",\"group\":");
    match group {
        Some(g) => {
            out.push('"');
            crate::fusion::json_escape_into(&mut out, g);
            out.push('"');
        }
        None => out.push_str("null"),
    }
    out.push_str(",\"tier\":\"");
    out.push_str(if d.declared { "declared" } else { "implicit" });
    out.push_str("\",\"pending\":");
    out.push_str(&d.pending.to_string());
    out.push_str(",\"partitionsPending\":");
    out.push_str(&d.partitions_pending.to_string());
    out.push_str(",\"bytes\":");
    out.push_str(&d.bytes.to_string());
    out.push_str(",\"partitions\":[");
    for (i, p) in d.partitions.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"partition\":\"");
        crate::fusion::json_escape_into(&mut out, &p.partition);
        out.push_str("\",\"pending\":");
        out.push_str(&p.pending.to_string());
        out.push_str(",\"bytes\":");
        out.push_str(&p.bytes.to_string());
        out.push('}');
    }
    out.push_str("],\"groups\":[");
    for (i, g) in d.groups.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"group\":\"");
        crate::fusion::json_escape_into(&mut out, &g.group);
        out.push_str("\",\"pending\":");
        out.push_str(&g.pending.to_string());
        out.push_str(",\"skipped\":");
        out.push_str(&g.skipped.to_string());
        out.push('}');
    }
    out.push_str("]}");
    json(StatusCode::OK, out)
}
