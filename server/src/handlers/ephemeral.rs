//! THE EPHEMERAL HTTP SURFACE — EPHEMERAL_QUEUES.md §3.1/§3.3.
//!
//! Two halves, and the line between them is the point of the whole feature:
//!
//!   * the THREE HOT VERBS — push, pop, ack — which touch this process's heap
//!     and nothing else;
//!   * the FIVE MANAGEMENT VERBS — configure, reset, delete and the two status
//!     reads — of which exactly two (configure and delete) go through the state
//!     machine, and only ever to write a queue's DECLARATION. Never a message.
//!
//! No hot verb reaches the state machine: a req/reply inbox costs one hash
//! lookup and a `VecDeque` push. A push into the heap wakes a parked pop's gate
//! directly, so the long poll is purely event-driven; a missed wake costs the
//! remaining timeout and never correctness.
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
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use axum::response::Response;
use serde::Deserialize;
use serde_json::value::RawValue;

use super::{json, qbool, qbool_or_alias, qint, AppState};
use crate::ephemeral::{self, AckOutcome, AckStatus, Refusal, Route};
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
    crate::util::json_escape_into(&mut body, message);
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
        resp.headers_mut()
            .insert(axum::http::header::RETRY_AFTER, v);
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

// ===========================================================================
// FORWARDING TO THE RENDEZVOUS OWNER — §3.6/§3.7
// ===========================================================================
//
// Placement is single-owner (§1.5): one ring per (queue, partition), on the
// broker the rendezvous hash names. A request that lands anywhere else is
// RELAYED rather than served, because the alternative — serving it locally —
// would silently create a second ring for the same queue on a second broker,
// and two rings mean two cursors, duplicate delivery for a competing group and
// a depth read that is true nowhere.
//
// The proxy does not need to know any of this: a cell's `base_url` is one k8s
// Service with no stickiness (§5.1), so a request lands on an arbitrary broker
// and this is precisely what makes that correct.
//
// THE LOOP IS UNREPRESENTABLE, NOT MERELY UNLIKELY. A relayed request carries
// `x-queen-eph-fwd: 1`; a broker that sees it and finds it is not the owner
// answers 503 `owner_moved` instead of relaying again. Membership can disagree
// for a few seconds (§1.4) and without that header two brokers with opposite
// views would bounce a request between them until a timeout.

/// Deadline for a relayed request that does not park: push, ack, and a pop with
/// `wait=false`. Generous rather than tight — the peer is one hop away on the
/// cell network, so anything approaching this is a peer in trouble, and the
/// honest answer for that is a 503 the client can retry, not a truncated wait.
const FORWARD_DEADLINE_MS: u64 = 30_000;

/// Slack over a forwarded WAITING pop's own timeout (§3.6/§10 Q5).
///
/// The owner holds the pop open for the client's FULL timeout — that is the
/// resolved decision, because a fast-empty answer would turn every remote inbox
/// into a poll loop, which is the one thing this class must not reintroduce. An
/// internal deadline equal to that timeout would therefore race the peer's own
/// answer and turn a legitimately empty long-poll into a spurious 503; the slack
/// is what makes the timeout mean "the peer is gone", not "the peer is about to
/// reply".
const FORWARD_SLACK_MS: u64 = 5_000;

/// Was this request already relayed by another broker?
fn is_forwarded(h: &HeaderMap) -> bool {
    crate::peerclient::forwarded(h)
}

/// §3.6 — the one answer a broker gives for a request it was handed but does not
/// own. 503 and not a redirect: the client is not the one who got the placement
/// wrong, and the peer that relayed it is the one that can fix it (by re-hashing
/// once). `code` is what the relaying broker branches on.
fn owner_moved() -> Response {
    err(
        StatusCode::SERVICE_UNAVAILABLE,
        "owner_moved",
        "this ephemeral partition has moved to another broker; the request was not served",
    )
}

fn forward_failed(detail: &str) -> Response {
    err(
        StatusCode::SERVICE_UNAVAILABLE,
        "ephemeral_forward_failed",
        &format!("could not reach the broker that owns this ephemeral partition: {detail}"),
    )
}

/// The headers a relayed request carries, and deliberately only these.
///
///   * the FORWARD MARK, so the owner never relays it again;
///   * the TENANT, because the receiving broker's middleware reads it from the
///     header and nothing else — the value comes from THIS broker's
///     `Extension<Tenant>`, i.e. from a header its own trusted proxy set, never
///     from a body (`kv.rs`'s rule). The owner believes it with the cluster
///     token even where its own router has tenancy off (`tenant.rs`): PORT
///     next to a proxy on its own port, the layout stage and prod run;
///   * `Authorization`, unchanged, when the inbound request had one. Re-presenting
///     the caller's own credential means the peer applies the same policy to the
///     same principal — the relay grants nothing the original request did not
///     already carry, which is what keeps this off the list of ways to bypass
///     `auth.rs`.
///
/// Everything else is dropped on purpose: copying an arbitrary header set from
/// one broker's request into another's is how a hop-by-hop header becomes a bug.
fn relay_headers(tenant: &str, inbound: &HeaderMap) -> Vec<(HeaderName, HeaderValue)> {
    // The forward mark, and the cluster token next to it when one is set:
    // the owner believes the mark only with the token (`peerclient::forwarded`).
    let mut out: Vec<(HeaderName, HeaderValue)> = crate::peerclient::internal_headers();
    if let (Ok(k), Ok(v)) = (
        HeaderName::from_bytes(crate::config::TENANT_HEADER.as_bytes()),
        HeaderValue::from_str(tenant),
    ) {
        out.push((k, v));
    }
    if let Some(a) = inbound.get(axum::http::header::AUTHORIZATION) {
        out.push((axum::http::header::AUTHORIZATION, a.clone()));
    }
    out
}

/// One relay attempt. `Ok(None)` means the peer answered `owner_moved` — a
/// placement disagreement, which is the caller's business, not a failure.
#[allow(clippy::too_many_arguments)]
async fn relay_once(
    st: &AppState,
    tenant: &str,
    inbound: &HeaderMap,
    method: Method,
    http_addr: &str,
    path_and_query: &str,
    body: Bytes,
    deadline_ms: u64,
) -> Result<Option<Response>, String> {
    let url = format!("{}{}", http_addr.trim_end_matches('/'), path_and_query);
    // Counted per HOP and not per request: a re-hash that relays twice really did
    // pay two hops, and §7.6 is measuring hops.
    st.metrics
        .eph_forwarded
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let r = st
        .peers
        .call(
            method,
            &url,
            &relay_headers(tenant, inbound),
            body,
            std::time::Duration::from_millis(deadline_ms),
        )
        .await?;
    if r.status == StatusCode::SERVICE_UNAVAILABLE && body_code(&r.body) == Some("owner_moved") {
        return Ok(None);
    }
    let mut resp = json(r.status, String::from_utf8_lossy(&r.body).into_owned());
    // The one header worth carrying back: a 429 from the owner's rate bucket
    // without its `Retry-After` would be a refusal the client cannot pace
    // against, which is the exact thing `quota.rs` renders it for.
    if let Some(ra) = r.retry_after {
        resp.headers_mut()
            .insert(axum::http::header::RETRY_AFTER, ra);
    }
    Ok(Some(resp))
}

/// Read the `code` out of an `{error, code}` envelope. Branching on the CODE and
/// never on the message is the same rule this file states for its own clients.
fn body_code(body: &[u8]) -> Option<&'static str> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    match v.get("code").and_then(|x| x.as_str()) {
        Some("owner_moved") => Some("owner_moved"),
        _ => None,
    }
}

/// What the request addresses, for the single re-hash of §3.6.
#[derive(Clone, Copy)]
enum Target<'a> {
    /// A named partition: the hash answers exactly.
    Partition(&'a str),
    /// A partition-less pop: `route_queue`'s v1 rule decides.
    Queue,
}

/// §3.6 — relay to the owner, with the ONE re-hash the design allows.
///
/// `None` means "serve it here after all": the re-hash came back local, which is
/// what happens when the owner died between the first hash and the answer and
/// this broker won the partition. Every other outcome is a `Response`.
///
/// WHY EXACTLY ONE RE-HASH. A relay that got `owner_moved` proves the two
/// brokers disagreed, and membership is converging by construction (a HELLO or a
/// dead-threshold expiry). One retry catches the common case — a peer that just
/// joined or just left — and a second would be a client-visible latency budget
/// spent on a race that is already over. Beyond it the honest answer is 503:
/// come back, the cell is mid-move, and the contents of this queue are legally
/// gone anyway (§1.2).
#[allow(clippy::too_many_arguments)]
async fn forward_to_owner(
    st: &AppState,
    tenant: &str,
    queue: &str,
    target: Target<'_>,
    first_owner: String,
    first_addr: String,
    inbound: &HeaderMap,
    method: Method,
    path_and_query: &str,
    body: Bytes,
    deadline_ms: u64,
) -> Option<Response> {
    match relay_once(
        st,
        tenant,
        inbound,
        method.clone(),
        &first_addr,
        path_and_query,
        body.clone(),
        deadline_ms,
    )
    .await
    {
        Ok(Some(resp)) => return Some(resp),
        Ok(None) => { /* owner_moved — fall through to the single re-hash */ }
        Err(e) => return Some(forward_failed(&e)),
    }
    let again = match target {
        Target::Partition(p) => st.ephemeral.route(tenant, queue, p),
        Target::Queue => st.ephemeral.route_queue(tenant, queue),
    };
    match again {
        Route::Local => None,
        Route::Remote {
            server_id,
            http_addr,
        } => {
            // The same answer as before means nothing has converged yet; a second
            // call to the broker that just refused would get the same refusal.
            if server_id == first_owner {
                return Some(owner_moved());
            }
            match relay_once(
                st,
                tenant,
                inbound,
                method,
                &http_addr,
                path_and_query,
                body,
                deadline_ms,
            )
            .await
            {
                Ok(Some(resp)) => Some(resp),
                Ok(None) => Some(owner_moved()),
                Err(e) => Some(forward_failed(&e)),
            }
        }
    }
}

// The error is the HTTP answer itself, built only on the cold path.
#[allow(clippy::result_large_err)]
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
    headers: HeaderMap,
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
    let partition = parsed
        .partition
        .as_deref()
        .unwrap_or(ephemeral::DEFAULT_PARTITION);
    if let Err(r) = check_name("partition", partition) {
        return r;
    }
    if parsed.messages.is_empty() {
        return json(StatusCode::CREATED, "{\"pushed\":0}".to_string());
    }
    if parsed.messages.len() > MAX_ITEMS_PER_CALL {
        return bad_request(&format!("at most {MAX_ITEMS_PER_CALL} messages per push"));
    }
    let forwarded = is_forwarded(&headers);
    // RUNG 3 IS CHARGED ONCE PER REQUEST, AT THE BROKER THE CLIENT REACHED.
    //
    // A relayed push passes 0 messages here, which by `gate_push`'s own rule
    // skips the token bucket while still running rungs 1 and 2. The rate bounds
    // ARRIVAL into the cell, and a request that arrived at broker A already paid
    // for its arrival; charging it again at the owner would make a tenant's
    // effective ceiling depend on how its queue names happened to hash. Rung 1
    // (this broker's kill switch) and rung 2 (is this tenant granted here at all)
    // are properties of the RECEIVING broker and still apply to every hop.
    let charge = if forwarded {
        0
    } else {
        parsed.messages.len() as i64
    };
    if let Some(r) = gated(&st, tenant.as_str(), Surface::EphPush, charge) {
        return r;
    }

    // §3.7 — placement, before any state is touched. A `Remote` verdict also
    // DROPS whatever this broker still holds for that partition (the
    // membership-change wipe), so an ownership move frees its memory on the first
    // request that observes it rather than waiting for the periodic reap.
    if let Route::Remote {
        server_id,
        http_addr,
    } = st
        .ephemeral
        .route(tenant.as_str(), &parsed.queue, partition)
    {
        // A forwarded request is NEVER re-forwarded (§3.6).
        if forwarded {
            return owner_moved();
        }
        // What this node still held for the partition goes FIRST, so the
        // owner serves it before this push (a hand-over, where v1 wiped).
        ship(
            &st,
            st.ephemeral
                .take_handover(tenant.as_str(), &parsed.queue, Some(partition)),
        )
        .await;
        if let Some(r) = forward_to_owner(
            &st,
            tenant.as_str(),
            &parsed.queue,
            Target::Partition(partition),
            server_id,
            http_addr,
            &headers,
            Method::POST,
            "/api/v1/ephemeral/push",
            body.clone(),
            FORWARD_DEADLINE_MS,
        )
        .await
        {
            return r;
        }
        // `None` ⇒ the re-hash came back local ⇒ fall through and serve it here.
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
    match st
        .ephemeral
        .push(tenant.as_str(), &parsed.queue, partition, payloads, now)
    {
        Ok(pushed) => {
            // §3.4 — the hotlist-OFF direct wake. Ephemeral never enters the
            // hot list or its ~5 ms coalescing tick: the list exists to make a
            // wildcard candidate scan cheap, and there is no scan here.
            let qkey = ephemeral::Ephemeral::qkey(tenant.as_str(), &parsed.queue);
            st.notifier
                .notify_pushed_batch(&[(qkey, partition.to_string())]);
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

/// `GET /api/v1/ephemeral/pop` —
/// `?queue&partition&batch&wait&timeout&group&commitOnDelivery`, with `autoAck`
/// accepted as the deprecated alias of `commitOnDelivery` (either one true
/// commits at delivery, with no lease).
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
    headers: HeaderMap,
    // The raw URI, purely so a relayed pop carries the caller's query string
    // BYTE FOR BYTE (§3.6). Re-encoding it from the parsed map would risk
    // changing a value the owner then parses differently — and the one thing a
    // relay must not do is alter the request it is relaying.
    uri: Uri,
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
    let partition = q
        .get("partition")
        .map(String::as_str)
        .filter(|s| !s.is_empty());
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
    // At-most-once: the cursor moves as the messages leave, with no lease.
    // `autoAck` is the deprecated alias the released SDKs still send.
    let commit_on_delivery = qbool_or_alias(&q, "commitOnDelivery", "autoAck");
    let timeout_ms = q
        .get("timeout")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(st.pop_default_timeout_ms as i64)
        .clamp(0, MAX_TIMEOUT_MS);

    // §3.7 — placement. A NAMED partition routes exactly; a partition-less pop
    // takes the v1 rule of `route_queue` (serve this broker's share of the queue
    // if it owns any of it, otherwise route on the default partition). Both wipe
    // the rings this broker no longer owns as they go.
    let route = match partition {
        Some(p) => st.ephemeral.route(tenant.as_str(), queue, p),
        None => st.ephemeral.route_queue(tenant.as_str(), queue),
    };
    if let Route::Remote {
        server_id,
        http_addr,
    } = route
    {
        if is_forwarded(&headers) {
            return owner_moved();
        }
        ship(
            &st,
            st.ephemeral
                .take_handover(tenant.as_str(), queue, partition),
        )
        .await;
        // §10 Q5 — the owner holds a `wait=true` pop open for the client's FULL
        // timeout, so the relay's own deadline has to outlive it.
        let deadline_ms = if wait {
            (timeout_ms.max(0) as u64).saturating_add(FORWARD_SLACK_MS)
        } else {
            FORWARD_DEADLINE_MS
        };
        let pq = match uri.query() {
            Some(qs) => format!("/api/v1/ephemeral/pop?{qs}"),
            None => "/api/v1/ephemeral/pop".to_string(),
        };
        if let Some(r) = forward_to_owner(
            &st,
            tenant.as_str(),
            queue,
            match partition {
                Some(p) => Target::Partition(p),
                None => Target::Queue,
            },
            server_id,
            http_addr,
            &headers,
            Method::GET,
            &pq,
            Bytes::new(),
            deadline_ms,
        )
        .await
        {
            return r;
        }
    }

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
            commit_on_delivery,
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
    let mut out =
        String::with_capacity(128 + msgs.iter().map(|m| m.payload.len() + 96).sum::<usize>());
    out.push_str("{\"queue\":\"");
    crate::util::json_escape_into(&mut out, queue);
    out.push_str("\",\"messages\":[");
    for (i, m) in msgs.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"id\":\"");
        // The id is broker-minted from an epoch, a partition name and a seq;
        // only the partition half can carry anything worth escaping.
        crate::util::json_escape_into(&mut out, &m.id);
        out.push_str("\",\"partition\":\"");
        crate::util::json_escape_into(&mut out, &m.partition);
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
    /// string because it lands in the DLQ and in the trace store; this class
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
    headers: HeaderMap,
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

    // §3.7 — an ack must reach the ring that holds the LEASE, which is the
    // owner's. The wire carries the partition inside each id (§3.1), so the
    // routing key is the first id's partition; a batch that spans partitions of
    // one queue is routed by its first, which is correct whenever those
    // partitions share an owner and is the same v1 simplification the
    // partition-less pop makes. An id this broker cannot parse routes on the
    // default partition — the honest place to ask, and the answer for a
    // garbage id is `unknown` wherever it lands.
    let ack_partition = parsed
        .acks
        .first()
        .and_then(|a| ephemeral::parse_id(&a.id).map(|(_, p, _)| p.to_string()))
        .unwrap_or_else(|| ephemeral::DEFAULT_PARTITION.to_string());
    if let Route::Remote {
        server_id,
        http_addr,
    } = st
        .ephemeral
        .route(tenant.as_str(), &parsed.queue, &ack_partition)
    {
        if is_forwarded(&headers) {
            return owner_moved();
        }
        ship(
            &st,
            st.ephemeral
                .take_handover(tenant.as_str(), &parsed.queue, Some(&ack_partition)),
        )
        .await;
        if let Some(r) = forward_to_owner(
            &st,
            tenant.as_str(),
            &parsed.queue,
            Target::Partition(&ack_partition),
            server_id,
            http_addr,
            &headers,
            Method::POST,
            "/api/v1/ephemeral/ack",
            body.clone(),
            FORWARD_DEADLINE_MS,
        )
        .await
        {
            return r;
        }
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
        crate::util::json_escape_into(&mut out, id);
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
// CROSS-BROKER PROPAGATION IS A BROADCAST, NOT A FORWARD (§3.5).
//
// The three hot verbs are relayed to ONE broker — the partition's rendezvous
// owner — because there is exactly one ring to act on. These three are the
// opposite: they act on state every broker holds a copy of (a declared config)
// or might hold a ring for (after an ownership move), so each of them fans a
// `T_EPH_ADMIN` frame out to every peer and each peer applies it locally.
// Forwarding one of them to "the owner" would leave the other brokers' copies
// untouched, which for `delete` is precisely the ghost this phase removes.
//
// FIRE AND FORGET, backstopped rather than acknowledged. A dropped frame leaves
// a ghost for at most one config-refresh interval: `refresh_once` re-reads the
// declared rows and `drop_undeclared` forgets any declared queue whose row is
// gone (§10 Q4). That backstop is why none of the three waits for a peer, and
// why a peer being down costs nothing here.
//
// The broadcast is sent AFTER the local effect in all three, so a broker never
// tells its peers about a change it has not made itself.

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

/// `POST /api/v1/ephemeral/configure` — 201 with the stored declaration. The
/// declaration is replicated state; the rings pick it up on the node that
/// served the call and on every node at boot.
pub async fn handle_ephemeral_configure(
    State(st): State<Arc<AppState>>,
    Extension(_authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<Tenant>,
    body: Bytes,
) -> Response {
    if let Some(r) = gated(&st, tenant.as_str(), Surface::EphAdmin, 0) {
        return r;
    }
    // The declaration is replicated state: the state machine stores it and the
    // node that served the call applies it to its rings (`apply_local_control`).
    crate::handlers::raft::dispatch_api(
        &st,
        tenant.as_str(),
        "POST",
        "/api/v1/ephemeral/configure",
        None,
        body,
    )
    .await
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
    headers: HeaderMap,
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
    let dropped = st
        .ephemeral
        .reset(tenant.as_str(), &parsed.queue)
        .unwrap_or(0);
    // §3.5 — the queue's partitions live on every node, so every node resets
    // its own. Fire and forget, as v1's mesh frame was; a relayed reset is
    // never relayed again.
    if !is_forwarded(&headers) {
        fan_out(
            &st,
            tenant.as_str(),
            &headers,
            Method::POST,
            "/api/v1/ephemeral/reset".to_string(),
            body.clone(),
        );
    }
    // §3.5 — every broker drops its own rings for this queue. `dropped` is
    // therefore THIS broker's count and not the cell's, which is the honest
    // number to report: a fire-and-forget broadcast cannot know what the peers
    // dropped, and inventing a total would be a sum of unacknowledged frames.
    let mut out = String::with_capacity(64 + parsed.queue.len());
    out.push_str("{\"queue\":\"");
    crate::util::json_escape_into(&mut out, &parsed.queue);
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
pub async fn handle_ephemeral_delete_queue(
    State(st): State<Arc<AppState>>,
    Extension(_authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<Tenant>,
    headers: HeaderMap,
    Path(queue): Path<String>,
) -> Response {
    if let Some(r) = gated(&st, tenant.as_str(), Surface::EphAdmin, 0) {
        return r;
    }
    if let Err(r) = check_name("queue", &queue) {
        return r;
    }
    // A peer relaying its delete: drop this node's rings, nothing else (the
    // declaration is one replicated row, removed once by the node served).
    if is_forwarded(&headers) {
        let deleted = st.ephemeral.remove(tenant.as_str(), &queue);
        let mut out = String::with_capacity(48 + queue.len());
        out.push_str("{\"queue\":\"");
        crate::util::json_escape_into(&mut out, &queue);
        out.push_str("\",\"deleted\":");
        out.push_str(if deleted { "true" } else { "false" });
        out.push('}');
        return json(StatusCode::OK, out);
    }
    let resp = crate::handlers::raft::dispatch_api(
        &st,
        tenant.as_str(),
        "DELETE",
        &format!("/api/v1/ephemeral/queue/{queue}"),
        None,
        Bytes::new(),
    )
    .await;
    if resp.status().is_success() {
        fan_out(
            &st,
            tenant.as_str(),
            &headers,
            Method::DELETE,
            format!("/api/v1/ephemeral/queue/{}", percent_path(&queue)),
            Bytes::new(),
        );
    }
    resp
}

/// `GET /api/v1/ephemeral/queues` — tenant-scoped list, declared and implicit.
///
/// ZERO DATABASE, on purpose and as a documented property (§5.3): every number
/// here is an in-process gauge, so a dashboard may poll this at 1-2 s and it
/// costs the cell nothing but a map walk.
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
        crate::util::json_escape_into(&mut out, &q.name);
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
    crate::util::json_escape_into(&mut out, &d.queue);
    out.push_str("\",\"group\":");
    match group {
        Some(g) => {
            out.push('"');
            crate::util::json_escape_into(&mut out, g);
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
        crate::util::json_escape_into(&mut out, &p.partition);
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
        crate::util::json_escape_into(&mut out, &g.group);
        out.push_str("\",\"pending\":");
        out.push_str(&g.pending.to_string());
        out.push_str(",\"skipped\":");
        out.push_str(&g.skipped.to_string());
        out.push('}');
    }
    out.push_str("]}");
    json(StatusCode::OK, out)
}

// ===========================================================================
// §3.7 across raft nodes — placement, hand-over, drain
// ===========================================================================
//
// Every node computes the owner of each (queue, partition) from the raft
// members' view (`Ephemeral::set_placement`); a node that is not the owner
// relays push/pop/ack to it (above). When ownership moves — a node joins,
// leaves, or is judged down — the node that held a ring HANDS IT OVER to the new
// owner instead of dropping it, and a node stopping on SIGTERM hands over
// everything first. Only a crash still loses the crashed node's share (§1.2).
//
// A peer takes partitions once raft hears it AND its listener answers
// (`_ready`): a restarted node answers the leader's heartbeats before its HTTP
// listener is up, and a hand-over sent into that gap was dropped (two prod
// rolls, 2026-10-02). A hand-over that does not land is tried again with
// backoff, each time to the partition's owner of the moment (this node
// included), until `HANDOVER_RETRY_FOR`. The three calls between brokers —
// the hand-over, the leaving notice, the readiness probe — carry no client and
// prove themselves with the cluster token, never a JWT (`auth.rs`).

/// How long a node that announced it is leaving stays out of the placement.
/// Long enough for raft to judge it down if it really went; a node that comes
/// back sooner rejoins when the notice expires, or as soon as its new process
/// answers the readiness probe when the notice named the one that left.
const LEAVING_EXCLUSION_MS: i64 = 30_000;

/// Deadline for one hand-over request.
const HANDOVER_DEADLINE: Duration = Duration::from_secs(10);

/// How long a hand-over keeps trying before its contents are dropped: a peer
/// whose listener is a few seconds behind its raft, or a placement that moves
/// the partition elsewhere (back here included) while it waits.
const HANDOVER_RETRY_FOR: Duration = Duration::from_secs(30);

/// The pause before the second attempt, doubled after each one up to
/// [`HANDOVER_BACKOFF_MAX`].
const HANDOVER_BACKOFF_MIN: Duration = Duration::from_millis(100);
const HANDOVER_BACKOFF_MAX: Duration = Duration::from_secs(2);

/// How long a stopping node gives its rings to land (SIGTERM).
const DRAIN_FOR: Duration = Duration::from_secs(15);

/// Deadline for one readiness probe.
const PROBE_DEADLINE: Duration = Duration::from_secs(1);

/// A hand-over request's size target: rings are batched per (peer, tenant)
/// up to about this many bytes.
const HANDOVER_BATCH_BYTES: usize = 8 * 1024 * 1024;

/// Percent-encode one path segment (a queue name may hold `/`, `?`, spaces).
fn percent_path(seg: &str) -> String {
    let mut out = String::with_capacity(seg.len());
    for b in seg.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Relay an admin verb to every peer, fire and forget (v1's `T_EPH_ADMIN`).
fn fan_out(
    st: &Arc<AppState>,
    tenant: &str,
    inbound: &HeaderMap,
    method: Method,
    path: String,
    body: Bytes,
) {
    let peers = st.ephemeral.peers();
    if peers.is_empty() {
        return;
    }
    let headers = relay_headers(tenant, inbound);
    let st = st.clone();
    tokio::spawn(async move {
        for (_, addr) in peers {
            let url = format!("{}{}", addr.trim_end_matches('/'), path);
            match st
                .peers
                .call(
                    method.clone(),
                    &url,
                    &headers,
                    body.clone(),
                    Duration::from_millis(FORWARD_DEADLINE_MS),
                )
                .await
            {
                Ok(r) if r.status.is_success() => {}
                // A peer that answered and refused (a 401 from a proxy in its
                // way, a 403 on an unproven tenant) kept its rings as they were.
                Ok(r) => tracing::warn!(
                    target: "ephemeral",
                    %url,
                    status = r.status.as_u16(),
                    body = %String::from_utf8_lossy(&r.body),
                    "admin fan-out refused"
                ),
                Err(e) => {
                    tracing::warn!(target: "ephemeral", %url, error = %e, "admin fan-out failed")
                }
            }
        }
    });
}

/// One ring as the `_adopt` body carries it.
fn ring_json(out: &mut String, r: &ephemeral::RingExport) {
    out.push_str("{\"queue\":\"");
    crate::util::json_escape_into(out, &r.queue);
    out.push_str("\",\"partition\":\"");
    crate::util::json_escape_into(out, &r.partition);
    out.push_str("\",\"messages\":[");
    let mut first = true;
    for (t, p) in &r.msgs {
        // Payloads are the raw JSON a push carried; anything else never got in.
        let Ok(raw) = std::str::from_utf8(p) else {
            continue;
        };
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str("{\"t\":");
        out.push_str(&t.to_string());
        out.push_str(",\"p\":");
        out.push_str(raw);
        out.push('}');
    }
    out.push_str("],\"groups\":[");
    for (i, g) in r.groups.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"g\":\"");
        crate::util::json_escape_into(out, &g.name);
        out.push_str("\",\"next\":");
        out.push_str(&g.next.to_string());
        out.push_str(",\"redeliver\":[");
        for (j, o) in g.redeliver.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            out.push_str(&o.to_string());
        }
        out.push_str("],\"attempts\":[");
        for (j, (o, n)) in g.attempts.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            out.push_str(&format!("[{o},{n}]"));
        }
        out.push_str("],\"skipped\":");
        out.push_str(&g.skipped.to_string());
        out.push('}');
    }
    out.push_str("]}");
}

/// Ship hand-overs to their new owners now: one attempt each, batched per
/// (peer, tenant). What does not land goes back in the queue, which the
/// placement loop ships again with retries ([`ship_until`]); what an owner
/// refuses is dropped and counted. Returns `(rings delivered, rings lost)`.
///
/// The relayed push, pop and ack call this before relaying, so the owner
/// serves what this node still held ahead of the request: they cannot wait
/// out a retry, and need not — the relay itself fails while the owner does
/// not answer.
pub(crate) async fn ship(st: &AppState, out: Vec<ephemeral::Handover>) -> (usize, usize) {
    let (s, left) = ship_rounds(st, out, None).await;
    st.ephemeral.requeue(left);
    (s.delivered, s.lost)
}

/// What [`ship_until`] did with the rings it was given.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Shipped {
    /// Rings their new owner took.
    pub delivered: usize,
    /// Rings dropped: refused by the owner, or not landed by the deadline.
    pub lost: usize,
    /// Rings adopted here again: their partition came back to this node while
    /// they waited (the peer they were going to dropped out).
    pub kept: usize,
    /// Messages in the delivered rings, as the owners counted them.
    pub messages: usize,
}

/// Ship hand-overs, trying again with backoff until `until`, each attempt to
/// the partition's owner of the moment: the peer it was detached for, another
/// one if the placement moved it, or this node, which adopts it back. What has
/// not landed by `until` is dropped and counted — the contents of a class that
/// survives nothing (§1.2).
pub(crate) async fn ship_until(
    st: &AppState,
    out: Vec<ephemeral::Handover>,
    until: Instant,
) -> Shipped {
    let (mut s, left) = ship_rounds(st, out, Some(until)).await;
    if !left.is_empty() {
        tracing::warn!(
            target: "ephemeral",
            rings = left.len(),
            "hand-over did not land before its deadline"
        );
    }
    for h in &left {
        st.ephemeral.handover_lost(h);
    }
    s.lost += left.len();
    s
}

/// The rounds of [`ship`] and [`ship_until`]: one, then — given a deadline —
/// one per backoff while something is left to try and `until` has not
/// passed. Returns what is left.
async fn ship_rounds(
    st: &AppState,
    out: Vec<ephemeral::Handover>,
    until: Option<Instant>,
) -> (Shipped, Vec<ephemeral::Handover>) {
    let mut s = Shipped::default();
    if out.is_empty() {
        return (s, out);
    }
    let taken = out.len();
    st.ephemeral.shipping_started(taken);
    let mut pending = out;
    let mut backoff = HANDOVER_BACKOFF_MIN;
    let mut said = false;
    loop {
        let mut by: std::collections::BTreeMap<(String, String), Vec<ephemeral::Handover>> =
            std::collections::BTreeMap::new();
        let now_ms = crate::util::now_epoch_ms();
        let mut woken: Vec<(String, String)> = Vec::new();
        for mut h in pending.drain(..) {
            let r = &h.ring;
            match st
                .ephemeral
                .destination(&r.tenant, &r.queue, &r.partition, now_ms)
            {
                ephemeral::Dest::Peer(owner, addr) => {
                    h.owner = owner;
                    h.addr = addr;
                }
                // The partition is this node's again: adopt it here.
                ephemeral::Dest::Here => {
                    let key = (
                        ephemeral::Ephemeral::qkey(&r.tenant, &r.queue),
                        r.partition.clone(),
                    );
                    if st.ephemeral.take_back(h, now_ms) {
                        s.kept += 1;
                        woken.push(key);
                    } else {
                        s.lost += 1;
                    }
                    continue;
                }
                // Leaving, with every peer leaving too: nobody will take it.
                ephemeral::Dest::Nobody => {
                    st.ephemeral.handover_lost(&h);
                    s.lost += 1;
                    continue;
                }
            }
            by.entry((h.addr.clone(), h.ring.tenant.clone()))
                .or_default()
                .push(h);
        }
        if !woken.is_empty() {
            st.notifier.notify_pushed_batch(&woken);
        }
        // Every (peer, tenant) at once: a peer that does not answer holds
        // nobody else's rings up. Within a deadline, no attempt outlives it
        // by more than a second.
        let attempt = match until {
            None => HANDOVER_DEADLINE,
            Some(u) => HANDOVER_DEADLINE.min(
                u.saturating_duration_since(Instant::now())
                    .max(Duration::from_secs(1)),
            ),
        };
        let sent = futures_util::future::join_all(
            by.into_iter()
                .map(|((addr, tenant), hs)| send_rings(st, addr, tenant, hs, attempt)),
        )
        .await;
        let mut why = None;
        for r in sent {
            s.delivered += r.delivered;
            s.lost += r.lost;
            s.messages += r.messages;
            pending.extend(r.retry);
            why = why.or(r.why);
        }
        let now = Instant::now();
        let Some(until) = until.filter(|u| now < *u) else {
            break;
        };
        if pending.is_empty() {
            break;
        }
        if !said {
            said = true;
            tracing::info!(
                target: "ephemeral",
                rings = pending.len(),
                why = why.as_deref().unwrap_or(""),
                for_s = until.saturating_duration_since(now).as_secs(),
                "hand-over not landed yet; trying again"
            );
        }
        tokio::time::sleep(backoff.min(until - now)).await;
        backoff = (backoff * 2).min(HANDOVER_BACKOFF_MAX);
    }
    st.ephemeral.shipping_done(taken);
    (s, pending)
}

/// What one peer did with one tenant's rings in one round.
#[derive(Default)]
struct Sent {
    delivered: usize,
    lost: usize,
    messages: usize,
    /// Not landed, worth another attempt: no answer, or a 5xx / 429.
    retry: Vec<ephemeral::Handover>,
    /// Why the first of them did not land, for the log.
    why: Option<String>,
}

/// POST one tenant's rings to `addr`'s `_adopt`, in batches of about
/// [`HANDOVER_BATCH_BYTES`].
async fn send_rings(
    st: &AppState,
    addr: String,
    tenant: String,
    hs: Vec<ephemeral::Handover>,
    attempt: Duration,
) -> Sent {
    let url = format!("{}/api/v1/ephemeral/_adopt", addr.trim_end_matches('/'));
    let mut headers = crate::peerclient::internal_headers();
    if let (Ok(k), Ok(v)) = (
        HeaderName::from_bytes(crate::config::TENANT_HEADER.as_bytes()),
        HeaderValue::from_str(&tenant),
    ) {
        headers.push((k, v));
    }
    let mut out = Sent::default();
    let mut hs = hs.into_iter().peekable();
    while hs.peek().is_some() {
        let mut batch = Vec::new();
        let mut body = String::from("{\"rings\":[");
        while let Some(h) = hs.next_if(|_| batch.is_empty() || body.len() < HANDOVER_BATCH_BYTES) {
            if !batch.is_empty() {
                body.push(',');
            }
            ring_json(&mut body, &h.ring);
            batch.push(h);
        }
        body.push_str("]}");
        let r = st
            .peers
            .call(Method::POST, &url, &headers, Bytes::from(body), attempt)
            .await;
        match r {
            Ok(resp) if resp.status.is_success() => {
                let (adopted, refused) = adopt_answer(&resp.body);
                let refused = refused.min(batch.len());
                if refused > 0 {
                    // Which ones the answer does not say: the count is what
                    // the class's contract owes (§1.2), counted, never silent.
                    st.metrics
                        .eph_wipes
                        .fetch_add(refused as u64, std::sync::atomic::Ordering::Relaxed);
                    tracing::warn!(
                        target: "ephemeral",
                        %url,
                        rings = refused,
                        "the new owner refused rings (its bounds or quotas); their contents are dropped"
                    );
                }
                out.delivered += batch.len() - refused;
                out.lost += refused;
                out.messages += adopted;
            }
            Ok(resp)
                if resp.status.is_server_error()
                    || resp.status == StatusCode::TOO_MANY_REQUESTS =>
            {
                // A peer that is stopping said so: out of the way now, so the
                // next attempt goes to the partition's next owner instead of
                // waiting for its leaving notice or for raft.
                if is_leaving_answer(&resp.body) {
                    if let Some(h) = batch.first() {
                        st.ephemeral.exclude_peer(
                            &h.owner,
                            crate::util::now_epoch_ms() + LEAVING_EXCLUSION_MS,
                            None,
                        );
                    }
                }
                tracing::debug!(
                    target: "ephemeral",
                    %url,
                    status = resp.status.as_u16(),
                    "hand-over not taken yet; trying again"
                );
                if out.why.is_none() {
                    out.why = Some(format!("{url} answered {}", resp.status.as_u16()));
                }
                out.retry.extend(batch);
            }
            Ok(resp) => {
                let hint = if matches!(
                    resp.status,
                    StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
                ) {
                    "set the same QUEEN_RAFT_TOKEN on every node: the calls between brokers prove themselves with it"
                } else {
                    ""
                };
                tracing::warn!(
                    target: "ephemeral",
                    %url,
                    status = resp.status.as_u16(),
                    body = %String::from_utf8_lossy(&resp.body),
                    hint,
                    "hand-over refused"
                );
                for h in &batch {
                    st.ephemeral.handover_lost(h);
                }
                out.lost += batch.len();
            }
            Err(e) => {
                tracing::debug!(
                    target: "ephemeral",
                    %url,
                    error = %e,
                    "hand-over did not reach the peer; trying again"
                );
                if out.why.is_none() {
                    out.why = Some(format!("{url}: {e}"));
                }
                out.retry.extend(batch);
            }
        }
    }
    out
}

/// `{adopted, refusedRings}` out of an `_adopt` answer; zeros when it says
/// neither (an older broker answers the same shape).
fn adopt_answer(body: &[u8]) -> (usize, usize) {
    let v: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
    let n = |k: &str| v.get(k).and_then(|x| x.as_u64()).unwrap_or(0) as usize;
    (n("adopted"), n("refusedRings"))
}

/// Is this the `{error, code: "leaving"}` a stopping peer answers ([`leaving`])?
fn is_leaving_answer(body: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| {
            v.get("code")
                .and_then(|c| c.as_str())
                .map(|c| c == "leaving")
        })
        .unwrap_or(false)
}

#[derive(Deserialize)]
struct AdoptMsg<'a> {
    t: i64,
    #[serde(borrow)]
    p: &'a RawValue,
}

#[derive(Deserialize)]
struct AdoptGroup {
    g: String,
    next: u64,
    #[serde(default)]
    redeliver: Vec<u64>,
    #[serde(default)]
    attempts: Vec<(u64, u32)>,
    #[serde(default)]
    skipped: u64,
}

#[derive(Deserialize)]
struct AdoptRing<'a> {
    queue: String,
    partition: String,
    #[serde(borrow)]
    messages: Vec<AdoptMsg<'a>>,
    #[serde(default)]
    groups: Vec<AdoptGroup>,
}

#[derive(Deserialize)]
struct AdoptBody<'a> {
    #[serde(borrow)]
    rings: Vec<AdoptRing<'a>>,
}

fn internal_only() -> Response {
    err(
        StatusCode::FORBIDDEN,
        "internal_only",
        "this route is for the brokers of this cluster",
    )
}

/// What a node that is stopping answers a peer that would give it partitions:
/// 503, so the peer tries again — with the partition's next owner.
fn leaving() -> Response {
    err(
        StatusCode::SERVICE_UNAVAILABLE,
        "leaving",
        "this broker is stopping and takes no ephemeral partitions",
    )
}

/// `POST /api/v1/ephemeral/_adopt` — broker to broker: take over rings whose
/// ownership moved here. The tenant is the request's (the trusted header the
/// sending broker set), never a body field: a hand-over cannot cross tenants.
/// A node that is stopping takes nothing: what it adopted now would leave with
/// it.
pub async fn handle_ephemeral_adopt(
    State(st): State<Arc<AppState>>,
    Extension(_authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<Tenant>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !is_forwarded(&headers) {
        return internal_only();
    }
    if st.ephemeral.is_leaving() {
        return leaving();
    }
    let parsed: AdoptBody = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return bad_request(&format!("bad body: {e}")),
    };
    let now = crate::util::now_epoch_ms();
    let (mut adopted, mut refused) = (0usize, 0usize);
    let mut woken: Vec<(String, String)> = Vec::new();
    for r in parsed.rings {
        if check_name("queue", &r.queue).is_err() || check_name("partition", &r.partition).is_err()
        {
            refused += 1;
            continue;
        }
        let ring = ephemeral::RingExport {
            tenant: tenant.as_str().to_string(),
            queue: r.queue.clone(),
            partition: r.partition.clone(),
            msgs: r
                .messages
                .iter()
                .map(|m| (m.t, m.p.get().as_bytes().to_vec().into_boxed_slice()))
                .collect(),
            groups: r
                .groups
                .into_iter()
                .map(|g| ephemeral::GroupExport {
                    name: g.g,
                    next: g.next,
                    redeliver: g.redeliver,
                    attempts: g.attempts,
                    skipped: g.skipped,
                })
                .collect(),
        };
        match st.ephemeral.adopt(ring, now) {
            Ok(n) => {
                adopted += n;
                woken.push((
                    ephemeral::Ephemeral::qkey(tenant.as_str(), &r.queue),
                    r.partition,
                ));
            }
            Err(_) => refused += 1,
        }
    }
    if !woken.is_empty() {
        st.notifier.notify_pushed_batch(&woken);
    }
    json(
        StatusCode::OK,
        format!("{{\"adopted\":{adopted},\"refusedRings\":{refused}}}"),
    )
}

#[derive(Deserialize)]
struct LeavingBody {
    node: String,
    /// The incarnation that is leaving, in hex (absent from a node older than
    /// this field): its successor answers the readiness probe with another.
    #[serde(default)]
    epoch: Option<String>,
}

/// `POST /api/v1/ephemeral/_leaving` — broker to broker: the sender is
/// stopping (SIGTERM) and has handed its rings over; place nothing on it.
pub async fn handle_ephemeral_leaving(
    State(st): State<Arc<AppState>>,
    Extension(_authed): Extension<crate::auth::AuthedSub>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !is_forwarded(&headers) {
        return internal_only();
    }
    let parsed: LeavingBody = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return bad_request(&format!("bad body: {e}")),
    };
    st.ephemeral.exclude_peer(
        &parsed.node,
        crate::util::now_epoch_ms() + LEAVING_EXCLUSION_MS,
        parsed
            .epoch
            .as_deref()
            .and_then(|e| u64::from_str_radix(e, 16).ok()),
    );
    json(StatusCode::OK, "{\"ok\":true}".to_string())
}

/// `GET /api/v1/ephemeral/_ready` — broker to broker: does this node take
/// partitions? 200 `{node, epoch}` once its listener is up — which is all an
/// answer can prove, and the one thing raft cannot: a restarted node answers
/// raft before it listens here. 503 `leaving` while it stops. A peer places
/// nothing on a node before this has answered it (§3.7).
pub async fn handle_ephemeral_ready(
    State(st): State<Arc<AppState>>,
    Extension(_authed): Extension<crate::auth::AuthedSub>,
    headers: HeaderMap,
) -> Response {
    if !is_forwarded(&headers) {
        return internal_only();
    }
    if st.ephemeral.is_leaving() {
        return leaving();
    }
    let node = st
        .rsm
        .members()
        .map(|c| c.node_id.to_string())
        .unwrap_or_default();
    json(
        StatusCode::OK,
        format!(
            "{{\"node\":\"{node}\",\"epoch\":\"{:x}\"}}",
            st.ephemeral.epoch()
        ),
    )
}

/// Ask the peer `id` at `addr` whether it takes partitions (`_ready`).
///
/// Ready on a 200, with the incarnation it names. Ready, too, on a 4xx: the
/// listener answered, which is what the probe is for — a node older than the
/// route answers 404 (a rolling upgrade), and one that cannot verify this
/// node's calls answers 401 or 403 (its hand-overs then fail, as they would
/// anyway, and say why). Not ready on a 5xx (leaving, or a proxy in the way
/// with nothing behind it) or no answer at all.
pub(crate) async fn probe(st: &AppState, id: &str, addr: &str) -> ephemeral::Probe {
    let url = format!("{}/api/v1/ephemeral/_ready", addr.trim_end_matches('/'));
    let r = st
        .peers
        .call(
            Method::GET,
            &url,
            &crate::peerclient::internal_headers(),
            Bytes::new(),
            PROBE_DEADLINE,
        )
        .await;
    match r {
        Ok(resp) if resp.status.is_success() => {
            let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap_or_default();
            let node = v.get("node").and_then(|x| x.as_str()).unwrap_or("");
            if !node.is_empty() && node != id {
                // Two members at one address: placing partitions on it would
                // give one process two shares.
                static WRONG_NODE: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
                if let Some(suppressed) = WRONG_NODE.tick_now() {
                    tracing::warn!(
                        target: "ephemeral",
                        peer = %id,
                        %addr,
                        answered = %node,
                        suppressed,
                        "another node answers at this peer's address; it takes no partitions (check QUEEN_RAFT_PEERS)"
                    );
                }
                return ephemeral::Probe::NotReady;
            }
            let epoch = v
                .get("epoch")
                .and_then(|x| x.as_str())
                .and_then(|e| u64::from_str_radix(e, 16).ok());
            ephemeral::Probe::Ready(epoch)
        }
        Ok(resp) if resp.status.is_client_error() => {
            if matches!(
                resp.status,
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
            ) {
                static REFUSED: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
                if let Some(suppressed) = REFUSED.tick_now() {
                    tracing::warn!(
                        target: "ephemeral",
                        peer = %id,
                        %addr,
                        status = resp.status.as_u16(),
                        suppressed,
                        "a peer refuses this node's calls: ring hand-overs to it will fail; set the same QUEEN_RAFT_TOKEN on every node"
                    );
                }
            }
            ephemeral::Probe::Ready(None)
        }
        _ => ephemeral::Probe::NotReady,
    }
}

/// How long a peer raft finds live may keep its listener silent before this
/// node says so: longer than a restart's gap between the two.
const SILENT_LISTENER_WARN: Duration = Duration::from_secs(30);

/// Since when each peer raft finds live has not answered the probe (and is
/// not leaving): the placement loop's, cleared when raft judges it down.
type Silent = Arc<std::sync::Mutex<HashMap<String, Instant>>>;

/// Probe one peer in the background, at most one probe per peer in flight;
/// a peer found ready is placed at once (the loop is kicked). One that stays
/// silent is named in a warning: it takes no partitions, and nothing else
/// would say why (an unreachable http half of `QUEEN_RAFT_PEERS`, say).
fn spawn_probe(st: &Arc<AppState>, silent: &Silent, id: String, addr: String) {
    if !st.ephemeral.start_probe(&id) {
        return;
    }
    let (st, silent) = (st.clone(), silent.clone());
    tokio::spawn(async move {
        let answer = probe(&st, &id, &addr).await;
        if st.ephemeral.probe_done(&id, &addr, answer) {
            tracing::info!(target: "ephemeral", peer = %id, %addr, "peer listening: it takes partitions");
            st.ephemeral.kick_handle().notify_one();
        }
        let leaving = st
            .ephemeral
            .excluded_peers(crate::util::now_epoch_ms())
            .contains(&id);
        let mut silent = silent.lock().unwrap();
        if answer != ephemeral::Probe::NotReady || leaving {
            silent.remove(&id);
            return;
        }
        let since = *silent.entry(id.clone()).or_insert_with(Instant::now);
        if since.elapsed() >= SILENT_LISTENER_WARN {
            static SILENT: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
            if let Some(suppressed) = SILENT.tick_now() {
                tracing::warn!(
                    target: "ephemeral",
                    peer = %id,
                    %addr,
                    silent_s = since.elapsed().as_secs(),
                    suppressed,
                    "a peer raft finds live does not answer on its listener: it takes no ephemeral \
                     partitions until it does (check the http half of QUEEN_RAFT_PEERS)"
                );
            }
        }
    });
}

/// Bring this node's RAM in line with the replicated control rows: the switch,
/// the grants, the declarations. `apply_local_control` does it at once on the
/// node that served the write; this does it within a second on every other.
fn reconcile_control(st: &AppState) {
    let Some(ctl) = st.rsm.ephemeral_control() else {
        return;
    };
    st.switches.set_ephemeral(ctl.enabled);
    st.ephemeral.apply_grants(
        ctl.grants
            .into_iter()
            .map(|(tenant, grant)| ephemeral::Grant {
                tenant,
                enabled: grant.enabled,
                max_bytes: grant.max_bytes,
                max_queues: grant.max_queues.map(i64::from),
                max_msgs_per_sec: grant.max_msgs_per_sec.map(i64::from),
            })
            .collect(),
    );
    st.ephemeral.reconcile_declared(
        ctl.configs
            .iter()
            .map(|(tenant, queue, options)| {
                (
                    tenant.clone(),
                    queue.clone(),
                    ephemeral::parse_stored_options(options),
                )
            })
            .collect(),
    );
}

/// `QUEEN_EPHEMERAL_MEMBER_TTL_MS` (default 4000): a member the raft leader
/// has not heard from for this long owns no ephemeral partition.
fn member_ttl_ms() -> u64 {
    std::env::var("QUEEN_EPHEMERAL_MEMBER_TTL_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v >= 200)
        .unwrap_or(4_000)
}

/// Do this node's calls to its peers carry the cluster token? Without it a
/// peer with JWT auth on cannot tell a hand-over from a client (`auth.rs`).
fn sends_cluster_token() -> bool {
    crate::peerclient::internal_headers()
        .iter()
        .any(|(k, _)| k.as_str() == crate::peerclient::TOKEN_HEADER)
}

/// The placement loop (server only): every 250 ms — sooner when kicked — judge
/// the raft members' view, keep out the peers that are leaving or not
/// listening yet (and probe those), install the placement, hand over the rings
/// that moved, and once a second converge the control rows.
pub fn spawn_ephemeral_placement(st: Arc<AppState>) {
    if st.rsm.members().is_none() {
        return;
    }
    if st.auth_enabled && !sends_cluster_token() {
        tracing::warn!(
            target: "ephemeral",
            "JWT_ENABLED without QUEEN_RAFT_TOKEN: a peer cannot verify this node's ring \
             hand-overs and leaving notices, so a stopping node's ephemeral contents are \
             dropped; set the same QUEEN_RAFT_TOKEN on every node"
        );
    }
    tokio::spawn(async move {
        let mut judge = ephemeral::MemberJudge::new(member_ttl_ms());
        let silent: Silent = Arc::default();
        let mut last_reconcile = Instant::now();
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                _ = st.ephemeral.kick_handle().notified() => {}
            }
            if !st.ephemeral.is_leaving() {
                if let Some(cm) = st.rsm.members() {
                    let judged = judge.judge(&cm);
                    for id in judge.down_ids() {
                        st.ephemeral.clear_exclusion(&id);
                        st.ephemeral.forget_ready(&id);
                        silent.lock().unwrap().remove(&id);
                    }
                    if let Some(p) = judged {
                        let (p, probe) = match p {
                            Some(p) => st.ephemeral.admit(p, crate::util::now_epoch_ms()),
                            None => (None, Vec::new()),
                        };
                        for (id, addr) in probe {
                            spawn_probe(&st, &silent, id, addr);
                        }
                        if st.ephemeral.set_placement(p) {
                            let moved = st.ephemeral.reap_foreign();
                            tracing::info!(
                                target: "ephemeral",
                                peers = ?st.ephemeral.peers(),
                                handed_over = moved,
                                "placement changed"
                            );
                        }
                    }
                }
            }
            // Shipped beside the loop, not in it: a hand-over waiting on a
            // peer must not hold up the placement that may move it elsewhere.
            let out = st.ephemeral.take_outbox();
            if !out.is_empty() {
                let st = st.clone();
                tokio::spawn(async move {
                    let rings = out.len();
                    let s = ship_until(&st, out, Instant::now() + HANDOVER_RETRY_FOR).await;
                    tracing::info!(
                        target: "ephemeral",
                        rings,
                        delivered = s.delivered,
                        lost = s.lost,
                        kept = s.kept,
                        messages = s.messages,
                        "hand-over"
                    );
                });
            }
            if last_reconcile.elapsed() >= Duration::from_secs(1) {
                last_reconcile = Instant::now();
                reconcile_control(&st);
            }
        }
    });
}

/// What [`ephemeral_drain`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Drained {
    /// The peers it told it is leaving, and how many took the notice.
    pub peers: usize,
    pub told: usize,
    /// Rings it detached at the signal.
    pub rings: u64,
    /// What became of the rings it shipped (those, and any requeued).
    pub shipped: Shipped,
    /// Rings the placement loop still had on their way when the bound ran out.
    pub still_shipping: usize,
}

/// SIGTERM, before the raft hand-off: tell the peers this node is leaving,
/// stop owning anything, and hand every ring to its next owner, retrying what
/// does not land. Bounded by [`DRAIN_FOR`]: a stop is never held hostage by a
/// peer that does not answer. It also waits, within the same bound, for the
/// hand-overs the placement loop had in flight.
pub async fn ephemeral_drain(st: &Arc<AppState>) -> Drained {
    let peers = st.ephemeral.peers();
    if peers.is_empty() {
        return Drained::default();
    }
    let until = Instant::now() + DRAIN_FOR;
    let me = st
        .rsm
        .members()
        .map(|c| c.node_id.to_string())
        .unwrap_or_default();
    st.ephemeral.set_leaving();
    let headers = crate::peerclient::internal_headers();
    let body = format!(
        "{{\"node\":\"{me}\",\"epoch\":\"{:x}\"}}",
        st.ephemeral.epoch()
    );
    let notices = peers.iter().map(|(_, addr)| {
        let url = format!("{}/api/v1/ephemeral/_leaving", addr.trim_end_matches('/'));
        let headers = headers.clone();
        let body = Bytes::from(body.clone());
        let st = st.clone();
        async move {
            match st
                .peers
                .call(Method::POST, &url, &headers, body, Duration::from_secs(2))
                .await
            {
                Ok(r) if r.status.is_success() => true,
                Ok(r) => {
                    tracing::warn!(
                        target: "shutdown",
                        %url,
                        status = r.status.as_u16(),
                        body = %String::from_utf8_lossy(&r.body),
                        "a peer refused the leaving notice"
                    );
                    false
                }
                Err(e) => {
                    tracing::warn!(target: "shutdown", %url, error = %e, "a peer did not take the leaving notice");
                    false
                }
            }
        }
    });
    let told = futures_util::future::join_all(notices)
        .await
        .into_iter()
        .filter(|ok| *ok)
        .count();
    let rings = st.ephemeral.reap_foreign();
    let mut s = Shipped::default();
    loop {
        let out = st.ephemeral.take_outbox();
        if !out.is_empty() {
            if Instant::now() >= until {
                // Past the drain's bound: what is still queued leaves with
                // this process.
                for h in &out {
                    st.ephemeral.handover_lost(h);
                }
                s.lost += out.len();
                break;
            }
            let r = ship_until(st, out, until).await;
            s.delivered += r.delivered;
            s.lost += r.lost;
            s.kept += r.kept;
            s.messages += r.messages;
            continue;
        }
        if st.ephemeral.shipping() == 0 || Instant::now() >= until {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let d = Drained {
        peers: peers.len(),
        told,
        rings,
        shipped: s,
        still_shipping: st.ephemeral.shipping(),
    };
    tracing::info!(
        target: "shutdown",
        peers = d.peers,
        told = d.told,
        rings = d.rings,
        delivered = s.delivered,
        lost = s.lost,
        messages = s.messages,
        still_shipping = d.still_shipping,
        "ephemeral rings handed over"
    );
    d
}

// Two brokers over real HTTP: the hand-over with JWT auth on, and to a peer
// still starting (`src/tests_unit/README.md`).
#[cfg(all(test, feature = "server"))]
#[path = "../tests_unit/ephemeral_handover.rs"]
mod ephemeral_handover_tests;
