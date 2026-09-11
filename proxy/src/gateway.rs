//! The data-plane pipeline. OWNER: Agent A.
//!
//! Pipeline per request (spec §4/§14 — the order is load-bearing):
//!   1. resolve ClusterCtx from Host (miss -> 421). On a SHARED host
//!      (QUEEN_PROXY_SHARED_HOSTS, decision z) there is no cluster to resolve
//!      yet: steps 2 and the cluster-dependent half of 3 are deferred to just
//!      after authentication, which is where the credential names the cluster.
//!      The order among the gates is unchanged — only where authentication
//!      sits relative to them. A missing or invalid credential there is 401,
//!      never 421.
//!
//!      That deferral covers the WHOLE LISTENER as soon as any shared host is
//!      configured, Host-named clusters included, and the 421 for a Host that
//!      names nothing waits for a credential too. Otherwise the pre-auth
//!      answers differ per Host — 401 for a slug that exists, 421 for one that
//!      does not, 403 `cluster_suspended` for one being deleted — and a shared
//!      host sits behind a wildcard record, so an anonymous caller could read
//!      the whole tenant list and each tenant's lifecycle state off the front
//!      door. With no shared hosts configured (every self-hosted proxy that
//!      never sets the variable) the order below is byte-for-byte the old one.
//!   2. cluster status gate (Suspended -> 403 suspended; Produce while blocked
//!      -> 403 storage_quota_exceeded (live quota flag) or push_blocked (DB
//!      lifecycle status) — two causes, two codes)
//!   3. authenticate (auth::authenticate) + authorize (auth::authorize vs classify)
//!   4. limits: check_req; Produce -> buffer body (cap = min(plan, cfg)), count
//!      items + per-item payload caps, registry.admit each (queue,partition),
//!      check_msgs(n); POST /configure -> buffer body, retention ceiling +
//!      registry.admit the created (queue, Default); POST /ephemeral/push ->
//!      buffer body, check_msgs(messages.len()) and nothing else (no registry,
//!      no retention: the RAM class creates no rows); POST /streams/v1/cycle ->
//!      ALWAYS buffer body, whole-cycle refusal when the cluster is blocked and
//!      push_items is non-empty (§9.6), then the sink items answer the PRODUCE
//!      caps in enforce_produce's own order — per-item payload cap,
//!      registry.admit each distinct (queue,partition) (the cycle SP creates
//!      those rows lazily, like push auto-create), check_msgs(sink items);
//!      Consume wait=true, and
//!      GET /ephemeral/pop wait=true -> parked_slot RAII guard held across the
//!      upstream await
//!   5. forward: rebuild URI on ctx.cell_base_url, strip hop-by-hop headers,
//!      inject Authorization (ctx.cell_token), X-Queen-Tenant (cfg.send_tenant_header),
//!      X-Queen-Request-Id; long-poll timeout = min(client timeout|30s, cfg max) + margin
//!   6. meter post-response (M1–M6): push -> parse per-item statuses (exclude
//!      error, dedupe duplicate, buffered counts), pop -> delivered count +
//!      debit_deliveries, bytes in/out always; the same push parse feeds the
//!      sampled §6.10 maintenance signal. The ephemeral push/pop pair meters as
//!      Push/Delivery like its durable twin (EPHEMERAL_QUEUES.md Q6), off a
//!      one-number 201 body instead of a status array. The streams cycle keeps
//!      its reqs-only base sample and adds a SECOND Sample (op Push, reqs 0)
//!      for the sink items its push_results confirm as `queued`
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

/// Step 2, as a function so the shared-host path can run the SAME gate in the
/// same place once the credential has named a cluster.
fn status_gate(ctx: &ClusterCtx) -> Option<Response> {
    match ctx.status {
        ClusterStatus::Suspended | ClusterStatus::Deleting => {
            Some(errors::err_403(errors::CODE_SUSPENDED, "cluster suspended"))
        }
        _ => None,
    }
}

/// The half of step 3 that needs a cluster: the plan's feature gate and the
/// push blocks. Extracted for the same reason as `status_gate` — one
/// implementation, run in both orders, so a shared host cannot end up with a
/// weaker gate than a per-cluster hostname.
fn plan_gates(
    st: &St,
    ctx: &ClusterCtx,
    class: RouteClass,
    method: &Method,
    path: &str,
) -> Option<Response> {
    if let RouteClass::Gated(f, op) = class {
        if !feature_enabled(f, &ctx.features) {
            return Some(errors::err_403(errors::CODE_FEATURE_GATED, "not in your plan"));
        }
        // PLAN_KV_TIMERS.md §9.5: the growing half of a gated family answers
        // the push blocks exactly like Produce, because it is the half that
        // consumes the disk the storage quota bounds. `Read` and `Open` never
        // do — a tenant that cannot read or delete cannot get back under its
        // own quota, and a tenant that cannot cancel cannot stop a fire.
        // `Mixed` is decided against the body, further down, because it is the
        // only class where both halves arrive in one array (§9.6).
        if op == crate::routes::GatedOp::Grow {
            if let Some(resp) = push_block_response(st, ctx) {
                return Some(resp);
            }
        }
    }
    if class == RouteClass::Produce {
        if let Some(resp) = push_block_response(st, ctx) {
            return Some(resp);
        }
    }
    // PLAN_DASHBOARD_ACTIONS.md §2.0: a DLQ replay is a push wearing a
    // `QueueAdmin` class. It writes a frame into a live partition — the same
    // rows, the same retained bytes `/api/v1/push` writes — so the storage and
    // monthly blocks have to see it, or a cluster blocked from pushing could
    // keep growing one dead letter at a time, past the exact gate that was set
    // to stop it. The CLASS stays `QueueAdmin` because that is the authority
    // the route needs (it deletes the dead-letter row, irreversibly); whether
    // the push half may proceed is a different question, and this is where
    // every other quota answers it. The streams cycle makes the same split from
    // the other direction: `Gated(_, Mixed)`, blocked by the body rather than
    // by the class.
    //
    // Deliberately NOT symmetrical with `Produce` in one respect: a replay's
    // bytes never reach `note_accepted_bytes`, because the proxy cannot see
    // them. The payload is the broker's own dead-letter snapshot and the
    // request body carries nothing but the optional target overrides, so the
    // in-flight storage estimate learns about a replay only at the next
    // retained-bytes computation. One message per call, at a button's pace: the
    // window that leaves open is the one `publish_retained` closes on its own.
    if is_dlq_replay(method, path) {
        if let Some(resp) = push_block_response(st, ctx) {
            return Some(resp);
        }
    }
    None
}

pub async fn handle(State(st): State<St>, req: Request) -> Response {
    // ----- 1. resolve the cluster this request acts on -----
    // Host by default; `x-queen-act-cluster` when a human session names one;
    // and on a SHARED host (QUEEN_PROXY_SHARED_HOSTS) nothing at all until the
    // credential is read — acting.rs owns that whole policy. Everything
    // downstream — the status gate, the plan limits, the injected
    // X-Queen-Tenant, the meter attribution — then follows the cluster picked
    // here, whichever of the three named it.
    let mut unknown_host = false;
    let fixed = match crate::acting::resolve_route(&st, req.headers()).await {
        Ok(crate::acting::Route::Fixed(ctx)) => Some(ctx),
        // Shared host: steps 2 and the cluster-dependent half of 3 are deferred
        // below, past authentication, because until the credential is read
        // there is no cluster to gate. The ctx-FREE refusals (blocked route,
        // operator flag) still answer before authentication, exactly as they do
        // on every other host.
        Ok(crate::acting::Route::FromCredential) => None,
        // Only ever produced on a listener that HAS shared hosts: the Host
        // named no cluster, and the 421 that says so is withheld until a
        // credential proves live (acting.rs, decision z applied to the whole
        // front door). Answered after the ctx-free refusals below so a blocked
        // route stays a 404 here exactly as it is everywhere else.
        Ok(crate::acting::Route::UnknownHost) => {
            unknown_host = true;
            None
        }
        Err(resp) => return resp,
    };

    // ----- 2. cluster status gate -----
    // On a shielded listener this waits for step 3's authentication, together
    // with the plan gates: `status_gate` distinguishes an existing suspended
    // cluster from an existing active one, so running it pre-auth beside a
    // shared host would hand an anonymous caller each tenant's lifecycle state
    // (measured: a `deleting` tenant answered 403 cluster_suspended to a
    // request carrying no credential at all). Nothing moves on a listener with
    // no shared hosts — `shielded` is false there and this is the same
    // pre-auth gate it has always been.
    let shielded = st.cfg.has_shared_hosts();
    if !shielded {
        if let Some(ctx) = &fixed {
            if let Some(resp) = status_gate(ctx) {
                return resp;
            }
        }
    }

    // ----- 3. classify + feature gate + authn/authz -----
    let class = classify(req.method(), req.uri().path());
    if class == RouteClass::Blocked {
        return errors::err_404(errors::CODE_ROUTE_BLOCKED, "not available");
    }
    if class == RouteClass::Operator && !st.cfg.operator_enabled {
        // The per-cell gate, answered BEFORE authentication so a cell with the
        // capability off behaves exactly as it did when these routes were hard
        // Blocked: same 404, same code, no lookup, no way in. Turning the flag
        // on is the only thing that makes the class reachable at all.
        return errors::err_404(errors::CODE_ROUTE_BLOCKED, "not available");
    }

    // ----- 3b. the Kafka facade's own bookkeeping -----
    // A `POST /api/v1/kv` batch that addresses NOTHING but the facade's
    // reserved key space (`queen-kafka` / `qk:`) is reclassified `Consume` —
    // the authority of the fetch it serves. See `kafka_kv.rs` for the rule and
    // the fail-closed direction; `classify()` is unchanged, because this is a
    // property of the BODY and that function has none.
    //
    // Here, and not later, because the gates whose answer it changes run below:
    // `plan_gates` (the `kv` plan flag) on the next line, `auth::authorize` at
    // step 3, and the `mixed_block` verdict at step 4. The body does not
    // otherwise exist until step 4's `into_parts`, so the decision that feeds
    // an AUTHORIZATION and FEATURE question cannot be deferred to the point
    // where reading it is free.
    //
    // TWO COSTS, both accepted and neither hidden:
    //
    //  - every `POST /api/v1/kv` now buffers and parses once, against §9.6's
    //    "on an unblocked cluster the body is never buffered". A KV batch is
    //    small by construction (the facade bounds itself to 256 ops per call)
    //    and `max_body_bytes` is the same ceiling `enforce_produce` and
    //    `enforce_configure` already buffer under, so nothing new is unbounded.
    //  - on a NON-shielded listener the gates below run before authentication,
    //    so this buffer is taken before the caller has proven anything. That is
    //    one route, under one existing cap. The alternative — deferring the kv
    //    plan gate past authentication for everyone — would move when a plain
    //    KV tenant gets its 403, which is a change to a surface this track was
    //    not asked to move.
    //
    // Metering is deliberately NOT touched: `bytes_in` stays 0 for a batch that
    // travels on, exactly as it did when this route was never buffered, so no
    // tenant's bill changes because of a classification.
    let (class, req) = if crate::kafka_kv::is_kv_batch(class) {
        let (parts, body) = req.into_parts();
        let buffered = match axum::body::to_bytes(body, st.cfg.max_body_bytes).await {
            Ok(b) => b,
            Err(_) => return errors::err_413("request body exceeds cap"),
        };
        // Two carve-outs, chained, and the order does not matter: each demands
        // that EVERY op of the batch sit in its OWN namespace, so a body can
        // satisfy at most one of them and a body that satisfies neither comes
        // back unchanged. The second is the S3 sink's (`s3_kv.rs`, PLAN_S3_SINK
        // §8/D4): `queen-s3` / `s3:`, whose window commit is the one WRITE a
        // read path cannot proceed without — refusing it under the storage
        // block re-uploads the same window for ever while the lag grows.
        let class = crate::kafka_kv::effective_class(class, &buffered);
        let class = crate::s3_kv::effective_class(class, &buffered);
        (class, Request::from_parts(parts, Body::from(buffered)))
    } else {
        (class, req)
    };

    if !shielded {
        if let Some(ctx) = &fixed {
            if let Some(resp) = plan_gates(&st, ctx, class, req.method(), req.uri().path()) {
                return resp;
            }
        }
    }
    if unknown_host {
        // 421 for a live credential, that credential's own 401 otherwise.
        return crate::acting::unknown_host_refusal(&st, req.headers()).await;
    }

    let (ctx, principal) = match fixed {
        Some(ctx) => {
            let p = match crate::acting::authenticate_for(&st, req.headers(), &ctx).await {
                Ok(p) => p,
                Err(resp) => return resp,
            };
            // Deferred from step 2 on a shielded listener, in the same order
            // and through the same two functions, so a Host-named cluster
            // there gets exactly the gates it gets everywhere else — only
            // after the caller has earned an answer about it.
            if shielded {
                if let Some(resp) = status_gate(&ctx) {
                    return resp;
                }
                if let Some(resp) = plan_gates(&st, &ctx, class, req.method(), req.uri().path()) {
                    return resp;
                }
            }
            (ctx, p)
        }
        None => {
            let (ctx, p) = match crate::acting::resolve_from_credential(&st, req.headers()).await {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            // The same two gates, in the same order, now that the credential
            // has named the cluster. A suspended tenant on a shared host is
            // still a 403 `cluster_suspended`, and a Produce into a
            // storage-blocked one is still a 403 `storage_quota_exceeded`.
            if let Some(resp) = status_gate(&ctx) {
                return resp;
            }
            if let Some(resp) = plan_gates(&st, &ctx, class, req.method(), req.uri().path()) {
                return resp;
            }
            (ctx, p)
        }
    };
    if let Err(resp) = crate::auth::authorize(&principal, class) {
        // An operator route refused is answered as if it did not exist — the
        // same 404 the flag-off cell gives — rather than a 403 that tells a
        // tenant admin there is a capability to go looking for.
        if class == RouteClass::Operator {
            return errors::err_404(errors::CODE_ROUTE_BLOCKED, "not available");
        }
        return resp;
    }
    if class == RouteClass::Operator {
        if let crate::state::Principal::User { user_id, .. } = &principal {
            // Sampled: a dashboard polls these every few seconds, so one line
            // per minute carrying the suppressed count, not one per request.
            crate::acting::note_operator_access(*user_id, &ctx.slug, req.uri().path());
        }
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

    // §9.6: a kv/timers `Mixed` batch is only inspected when something is
    // actually blocking. On an unblocked cluster — the overwhelming majority —
    // the body is never buffered and never parsed, exactly like today. The
    // streams cycle is the one `Mixed` route that reads its body either way
    // (its sink items bill); this verdict is still what decides whether the
    // cycle is REFUSED, which is why it is computed for the whole class.
    let mixed_block = match class {
        RouteClass::Gated(_, crate::routes::GatedOp::Mixed) => push_block_response(&st, &ctx),
        _ => None,
    };

    let (mut parts, body) = req.into_parts();

    // ----- 4b. Produce: buffer body, count items, per-item + batch caps,
    //           registry admission, msg bucket. Bytes forwarded verbatim. -----
    let mut produce_n: u64 = 0;
    let mut bytes_in: u64 = 0;
    // The sink half of a streams cycle, read once on the way in (4b'''' below)
    // and matched against `push_results` on the way out (step 6). It has to
    // cross the whole pipeline because the two ends know different halves of
    // the same fact: the response says which (queue, partition) GROUPS were
    // written, and only the request knows how many messages each one carried.
    let mut cycle_sinks = CycleSinks::default();
    let is_cycle = is_streams_cycle(&parts.method, &path_only);
    // Read once here, used once at step 6. The request line is about to be
    // consumed and the replay's message can only be counted off the RESPONSE
    // (D4), so the predicate has to cross the pipeline the way `is_cycle` does.
    let is_replay = is_dlq_replay(&parts.method, &path_only);
    // ...and the narrower half of it, the only replay route whose body can name
    // a destination to create. Step 4b buffers and admits on this one alone.
    let names_a_dest = is_replay_with_dest(&parts.method, &path_only);
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
    } else if names_a_dest {
        // 4b'''''. The OTHER QueueAdmin creation path, and the reason it is
        // here rather than only in `plan_gates`: a replay whose body names
        // `{queue, partition}` MOVES the dead letter somewhere else, and the
        // SP provisions a destination this cluster does not carry yet exactly
        // as a first-contact producer push does (`log_push_one_v1`, reached
        // with `p_pid` NULL). Without this arm it is the one route in the proxy
        // that creates a queue or a partition and answers no admission at all:
        // a tenant at its `max_queues` is 403 `quota_exceeded` on push and on
        // /configure, and creates queue 21 one dead letter at a time here.
        //
        // Same shape as 4b' above — bounded buffer, forwarded verbatim, the
        // same `admit_pairs` and therefore the same 403 and the same canonical
        // log line — because the two routes create the same rows and must not
        // drift into two different caps for the same act.
        //
        // WHAT THIS CANNOT SEE, stated rather than implied: the proxy knows
        // only the halves the REQUEST names, because an omitted half keeps the
        // source row's and that row lives in the broker. So a HALF-named
        // destination is not admissible here at all, and it is refused (400,
        // `admit_replay_dest`) rather than forwarded: forwarding it is the
        // bypass itself — one unadmitted partition per dead letter, repeatable.
        // A body that names NEITHER half is a different case and travels on: it
        // moves nothing, and the destination it resolves to is the source row's
        // own pair, which the broker had to find (`db::dlq_row_for_move` joins
        // log_partitions and queues) before it could read the row at all.
        //
        // The address-keyed retry route is deliberately NOT here: it replays in
        // place and never reads its body, so there is no destination to admit
        // and a body posted to it names nothing (`is_replay_with_dest`).
        let buffered = match axum::body::to_bytes(body, st.cfg.max_body_bytes).await {
            Ok(b) => b,
            Err(_) => return errors::err_413("request body exceeds cap"),
        };
        // Buffered, so the ingress size is known and metered like every other
        // buffered route (STEP 6) rather than the 0 not-buffering implied.
        bytes_in = buffered.len() as u64;
        if let Err(resp) = admit_replay_dest(&st, &ctx, &buffered, &rid).await {
            return resp;
        }
        Body::from(buffered)
    } else if is_cycle {
        // 4b''''. The streams CYCLE — out of prime order deliberately: it is
        // `Mixed` like a kv/timers batch (4b'' below), but it has to be matched
        // BEFORE that arm, for two reasons, and both are load-bearing:
        //
        //  - that arm's sniffer (`mixed_batch_grows`) looks for an `operations`
        //    array and reads a body without one as "does not grow". A cycle
        //    body is an object with no such array, so the generic arm would
        //    fail OPEN on every cycle, sink emits included.
        //  - it buffers only when the cluster is blocked. The cycle must ALWAYS
        //    buffer: its sink items are messages, and messages have to be
        //    counted for the msg bucket here and for the meter at step 6,
        //    blocked or not. That is the `/api/v1/ephemeral/push` precedent
        //    directly above — a body whose items bill, so a body always read.
        let buffered = match axum::body::to_bytes(body, st.cfg.max_body_bytes).await {
            Ok(b) => b,
            Err(_) => return errors::err_413("request body exceeds cap"),
        };
        bytes_in = buffered.len() as u64;
        // Blocked FIRST, the produce caps after — the order Produce already
        // gets, where `plan_gates` answers the push blocks at step 3 and the
        // caps run at step 4. A request we refuse never reaches the broker, so
        // it must neither debit the message bucket that shapes real traffic nor
        // register a sink pair the broker will never be asked to create.
        if let Some(blocked) = mixed_block {
            if cycle_batch_grows(&buffered) {
                tracing::info!(
                    target: "limits", cluster = %ctx.slug, rid, path = %path_only,
                    "gated batch refused whole: cluster blocked and the batch grows state"
                );
                return blocked;
            }
            // ...and an ack-/state-only cycle travels on. THE §9.6 point for
            // this family: a blocked tenant has to keep draining its source, or
            // the quota that was meant to stop its writes also freezes the
            // backlog it needs to work through to get back under the quota.
        }
        cycle_sinks = count_cycle_push_items(&buffered);
        // From here the sink half is enforced as PRODUCE, in `enforce_produce`'s
        // own internal order: per-item size cap (hard), registry admission
        // (shadow-aware), then the message bucket. A sink emit creates the same
        // rows and stores the same bytes as a push, so it answers the same caps.
        //
        // §9.6 TENSION, acknowledged: a refusal here takes the ack down with the
        // sink items, and this arm is the one that argued an ack must survive a
        // storage block. The two are not the same case. A storage block is a
        // state of the CLUSTER that the request did nothing to cause and can do
        // nothing about except drain — so refusing its ack would lock the tenant
        // out of the only exit. These denials are caused by THIS request's own
        // sink set: an item over the payload cap, or a queue/partition the plan
        // does not allow. The fix is in the caller's hands, it is the same
        // answer the same items would get from `/api/v1/push`, and a runner that
        // crash-loops on a 413 it can read is the honest surfacing. The
        // alternative — drop the offending sinks, forward the ack — answers 200
        // to a half-applied request and loses messages silently, which is the
        // outcome the whole-batch rule exists to prevent.
        if let Err(resp) =
            cycle_payload_cap(&cycle_sinks, ctx.limits.max_payload_bytes.map(|p| p.max(0) as usize))
        {
            return resp;
        }
        // Registry admission for each DISTINCT sink pair. The cycle SP creates
        // the queue and partition rows lazily in its sink pre-pass
        // (007_log_streams §2), exactly as push's auto-create does, so the plan's
        // max_queues / max_partitions_per_queue gate it here through the same
        // function and with the same 403 `quota_exceeded`. `groups` is already
        // distinct, which is the whole shape the admission wants.
        if let Err(resp) = admit_pairs(
            &st.registry,
            st.limits.enforcing(),
            &ctx,
            cycle_sinks.groups.iter().map(sink_pair),
            &rid,
        )
        .await
        {
            return resp;
        }
        if cycle_sinks.total > 0 {
            match st.limits.check_msgs(&ctx, cycle_sinks.total) {
                Decision::Allow => {}
                Decision::Deny { retry_after_s, code } => {
                    if st.limits.enforcing() {
                        return errors::err_429(code, retry_after_s, "message rate limit exceeded");
                    }
                    // shadow deny: canonical would_block log emitted inside check_msgs
                }
            }
        }
        // A sink emit grows retained bytes exactly like a push, so it feeds the
        // same in-flight storage account, at the same point in the order (after
        // every cap, before the forward).
        st.limits.note_accepted_bytes(ctx.cluster_id, cycle_sinks.payload_bytes);
        Body::from(buffered)
    } else if let Some(blocked) = mixed_block {
        // 4b''. A kv/timers batch on a blocked cluster (PLAN_KV_TIMERS.md
        // §9.6) — and only those two families: the third `Mixed` route, the
        // streams cycle, is taken by the arm above before this sniffer can
        // fail open on a body shape it was never written for.
        //
        // Both halves of the family arrive in ONE array, so the class
        // alone cannot decide: a `cancel`-only or read/delete-only batch must
        // get through — it is how the tenant stops a fire and how it gets back
        // under its own quota — while a batch that schedules or writes is
        // refused whole, carrying the same code the equivalent push would.
        //
        // Refusing the WHOLE batch is the point. Dropping the growing ops and
        // forwarding the rest would answer 200 for a request that was half
        // applied, which no client can act on.
        let buffered = match axum::body::to_bytes(body, st.cfg.max_body_bytes).await {
            Ok(b) => b,
            Err(_) => return errors::err_413("request body exceeds cap"),
        };
        bytes_in = buffered.len() as u64;
        if mixed_batch_grows(&buffered) {
            tracing::info!(
                target: "limits", cluster = %ctx.slug, rid, path = %path_only,
                "gated batch refused whole: cluster blocked and the batch grows state"
            );
            return blocked;
        }
        Body::from(buffered)
    } else if path_only == EPH_PUSH_PATH {
        // 4b'''. EPHEMERAL_QUEUES.md §5.1: the RAM push is not `Produce` — it
        // creates no queue row and no partition, so registry admission and the
        // retention ceiling have nothing to say about it — but its items ARE
        // messages, and the message-rate bucket is the one limit that must see
        // them (Q6). Same hard body-buffer ceiling as produce, and the same
        // tolerance: a body we cannot read counts 0 and travels on to the
        // broker's own 400 rather than being half-enforced here.
        let buffered = match axum::body::to_bytes(body, st.cfg.max_body_bytes).await {
            Ok(b) => b,
            Err(_) => return errors::err_413("request body exceeds cap"),
        };
        bytes_in = buffered.len() as u64;
        match st.limits.check_msgs(&ctx, count_ephemeral_push_items(&buffered)) {
            Decision::Allow => {}
            Decision::Deny { retry_after_s, code } => {
                if st.limits.enforcing() {
                    return errors::err_429(code, retry_after_s, "message rate limit exceeded");
                }
                // shadow deny: canonical would_block log emitted inside check_msgs
            }
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
    // PLAN_KV_TIMERS.md §9.9. The removal is UNCONDITIONAL and comes BEFORE the
    // `if`: with `send_tenant_header` off in front of a broker that has tenancy
    // on, the tenant would otherwise be whatever the client typed. That is
    // pre-existing, but KV is the first feature where it means "read and write
    // someone else's state knowing only its name" instead of "address a queue
    // whose acks are ownership-checked anyway". A KV key has no opaque id and
    // therefore no equivalent gate: the gate IS the WHERE clause, and the WHERE
    // takes the tenant from this header.
    parts.headers.remove(crate::config::TENANT_HEADER);
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
        // Two 201 shapes reach here: the durable per-item status array, and the
        // RAM family's one number (§3.1). Same treatment on a parse failure —
        // charge nothing we cannot confirm.
        let parsed = if path_only == EPH_PUSH_PATH {
            count_ephemeral_pushed(&buffered)
        } else {
            count_push_statuses(&buffered)
        };
        let counts = parsed.unwrap_or_else(|| {
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

    // Streams cycle: the request's sink items, billed only where the response
    // confirms the broker wrote them. Entered only when this cycle actually
    // carried sink groups — an ack-/state-only cycle (the shape a stream that
    // emits nothing sends all day) has nothing to match and falls through to
    // the reqs-only arm below without buffering a response at all.
    if is_cycle && status.is_success() && !cycle_sinks.groups.is_empty() {
        let (rparts, rbody) = resp.into_parts();
        let buffered = match axum::body::to_bytes(Body::new(rbody), RESP_BUFFER_CAP).await {
            Ok(b) => b,
            Err(_) => {
                // The same 502 the push/txn/pop arms give a response they
                // cannot buffer. Not a shape that reaches the cap in practice:
                // a cycle response carries one `push_results` element per sink
                // GROUP plus one ack result, never one per message.
                tracing::warn!(target: "meter", cluster = %ctx.slug, rid, "cycle response too large to buffer");
                return errors::err_502("cycle response too large");
            }
        };
        let accepted = count_cycle_accepted(&buffered, &cycle_sinks.groups).unwrap_or_else(|| {
            tracing::warn!(target: "meter", cluster = %ctx.slug, rid, "cycle response parse failed; msgs=0");
            0
        });
        // TWO samples, not one. The base sample is the route's own class
        // (`op_for` -> reqs-only `Read`, like every other gated surface): the
        // HTTP request is booked exactly once, there.
        st.meter.record(Sample {
            cluster_id: ctx.cluster_id,
            op,
            reqs: 1,
            msgs: 0,
            bytes_in,
            bytes_out: buffered.len() as u64,
        });
        // The second carries ONLY the messages — `reqs: 0` because this is the
        // same HTTP request the line above already counted, and double-counting
        // it would inflate the request side of every bill that has a cycle in
        // it. Bytes are on the base sample for the same reason. `OpClass::Push`
        // because that is what these items ARE: a sink emit grows retained
        // bytes exactly like `/api/v1/push`, so it belongs in the same
        // `usage_days` row a push lands in and in the monthly rollup the
        // `monthly_msgs_quota` block is read from (`cluster_month_msgs`).
        // Nothing else in the cycle bills as a message: the source pop was
        // already metered as `Delivery` on its own request, and the ack
        // advances a cursor — the reqs-only posture `/api/v1/ack` has.
        if accepted > 0 {
            st.meter.record(Sample {
                cluster_id: ctx.cluster_id,
                op: OpClass::Push,
                reqs: 0,
                msgs: accepted,
                bytes_in: 0,
                bytes_out: 0,
            });
        }
        return finalize(rparts, Body::from(buffered), &rid);
    }

    // DLQ replay: one message, billed only where the answer confirms it was
    // written (D4). The cycle's two-sample shape, with the counting problem
    // turned inside out — there the request knows the count and the response
    // the verdict; here the count is ALWAYS one and only the verdict is in
    // doubt, because the payload is the broker's own dead-letter snapshot and
    // never crosses this proxy in either direction.
    if is_replay && status.is_success() {
        let (rparts, rbody) = resp.into_parts();
        let buffered = match axum::body::to_bytes(Body::new(rbody), RESP_BUFFER_CAP).await {
            Ok(b) => b,
            Err(e) => {
                // A body we could not read, which on this route is either the
                // 64 MiB cap (unreachable in practice: a replay answers one
                // small object naming the queue, the partition and the ids it
                // wrote, never a message body) or the upstream dropping the
                // connection mid-body. The MOVE IS ALREADY DONE either way —
                // `log_dlq_move_v1` wrote the frame and deleted the dead-letter
                // row in one transaction, and a second attempt answers `gone` —
                // so the request is booked (`reqs: 1`) rather than vanishing
                // from usage, the message is not (we never learned the
                // verdict), and the 502 says what is actually known instead of
                // asserting a cause this branch has not established.
                for s in replay_samples(ctx.cluster_id, op, bytes_in, 0, false) {
                    st.meter.record(s);
                }
                tracing::warn!(
                    target: "meter", cluster = %ctx.slug, rid, error = %e,
                    "replay response could not be buffered; msgs=0 (the move may have committed)"
                );
                return errors::err_502(
                    "replay response could not be read; the move may have committed, so re-read the dead-letter row",
                );
            }
        };
        let stored = replay_stored_a_message(&buffered).unwrap_or_else(|| {
            tracing::warn!(target: "meter", cluster = %ctx.slug, rid, "replay response parse failed; msgs=0");
            false
        });
        for s in replay_samples(ctx.cluster_id, op, bytes_in, buffered.len() as u64, stored) {
            st.meter.record(s);
        }
        return finalize(rparts, Body::from(buffered), &rid);
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
    admit_pairs(
        &st.registry,
        st.limits.enforcing(),
        ctx,
        items.iter().map(produce_pair),
        rid,
    )
    .await?;

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

    // Every cap has passed and the batch is about to be forwarded, so these
    // bytes are ours to account for: they land on the broker's disk long before
    // its next retained-bytes computation notices them, and the storage gate
    // has to see them in the meantime (limits.rs, `StorageAccount`). LAST in
    // this function on purpose: a batch refused above is never forwarded, so
    // counting it would refuse later pushes over bytes that were never stored.
    st.limits.note_accepted_bytes(ctx.cluster_id, payload_bytes(&items));

    Ok(n)
}

/// Total payload bytes of a produce batch, on the same raw-JSON-text basis as
/// the per-item cap (`ItemInfo::payload_len`). Saturating because the sum is
/// fed to a quota estimate, where wrapping would read as room to spare.
fn payload_bytes(items: &[ItemInfo]) -> u64 {
    items.iter().fold(0u64, |acc, it| acc.saturating_add(it.payload_len as u64))
}

/// The two projections into `admit_pairs`' pair set, as named functions rather
/// than closures at the call sites. Not a style choice: a closure is inferred at
/// ONE borrow lifetime, so mapping one into an `async fn`'s iterator argument
/// makes the resulting future fail axum's higher-ranked `Send` bound with
/// "implementation of `FnOnce` is not general enough" — at the router, pages
/// away from the cause. A fn item is generic over its input lifetime and simply
/// works, at no runtime cost and with a name that says what is being projected.
fn produce_pair(it: &ItemInfo) -> (&str, &str) {
    (it.queue.as_str(), it.partition.as_str())
}
fn sink_pair(g: &((String, String), u64)) -> (&str, &str) {
    (g.0 .0.as_str(), g.0 .1.as_str())
}

/// Registry admission for a set of (queue, partition) pairs: the plan caps that
/// bound how many queues and partitions a tenant may bring into existence.
///
/// Shared by every request that can CREATE one. Two do: the produce batch
/// (`/api/v1/push` and `/api/v1/transaction`, via `enforce_produce`), whose
/// auto-create is the classic case, and the streams cycle, whose sink pre-pass
/// creates the queue and partition rows lazily in exactly the same way
/// (007_log_streams §2, "lazily create queue/partition rows … like 003_log_push").
/// One implementation on purpose: the two routes create the same rows, so they
/// must not be able to drift into two different caps for the same act.
/// `/configure` stays on its own single-pair call — it creates one known pair
/// and its denial is logged in the canonical `log_configure_deny` shape.
///
/// Deduped HERE rather than at the call sites: a produce batch arrives as a flat
/// item list with repeats and the cycle arrives pre-grouped, and re-hashing an
/// already-distinct handful of pairs costs nothing next to one `admit` call.
///
/// Shadow-aware, and it LOGS EITHER WAY (2026-09-04). The enforcing arms used
/// to return their 403 without a line, which is how a cluster refused every new
/// queue for hours with nothing in the proxy's logs to show for it; they now
/// emit the canonical `kind` + `blocked = true` shape `log_configure_deny` and
/// limits.rs::log_deny use. The shadow lines keep the kind spelled into
/// `would_block` as well as in `kind`. This IS the produce path's line and the
/// limits dashboards filter on it, so the old field stays put and the canonical
/// one is added beside it rather than replacing it.
///
/// Takes the registry and the enforcing flag rather than `St`: the decision
/// needs exactly those two things, and a function that does not need an
/// `AppState` can be unit-tested with a `Registry::new(None)`.
async fn admit_pairs<'a>(
    registry: &crate::registry::Registry,
    enforcing: bool,
    ctx: &ClusterCtx,
    pairs: impl IntoIterator<Item = (&'a str, &'a str)>,
    rid: &str,
) -> Result<(), Response> {
    let mut seen: HashSet<(&str, &str)> = HashSet::new();
    for (queue, partition) in pairs {
        if !seen.insert((queue, partition)) {
            continue;
        }
        match registry.admit(ctx, queue, partition).await {
            Admit::Allowed => {}
            Admit::OverQueues { max } => {
                if enforcing {
                    // Until 2026-09-04 this arm returned in SILENCE. On the
                    // trial cell that meant every push to a new queue name came
                    // back 403 `queue limit reached (20)` with not one line in
                    // the proxy's logs to say which cluster, which queue, or
                    // that a limit had fired at all, so the refusals could only
                    // be found from the client side.
                    tracing::warn!(
                        target: "limits", kind = "queues", cluster = %ctx.slug,
                        max, queue, blocked = true, rid, "queue limit reached"
                    );
                    return Err(errors::err_403(
                        errors::CODE_QUOTA_EXCEEDED,
                        &format!("queue limit reached ({max})"),
                    ));
                }
                tracing::warn!(
                    target: "limits", kind = "queues", would_block = "queues", cluster = %ctx.slug,
                    max, queue, rid, "shadow deny"
                );
            }
            Admit::OverPartitions { max } => {
                if enforcing {
                    tracing::warn!(
                        target: "limits", kind = "partitions", cluster = %ctx.slug,
                        max, queue, partition, blocked = true, rid, "partition limit reached"
                    );
                    return Err(errors::err_403(
                        errors::CODE_QUOTA_EXCEEDED,
                        &format!("partition limit reached ({max})"),
                    ));
                }
                tracing::warn!(
                    target: "limits", kind = "partitions", would_block = "partitions", cluster = %ctx.slug,
                    max, queue, partition, rid, "shadow deny"
                );
            }
        }
    }
    Ok(())
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

/// The first QueueAdmin route with a body worth parsing (the row-id replay
/// below is the other). `classify` maps this path to QueueAdmin for every
/// method, so the method check lives here.
fn is_configure(method: &Method, path: &str) -> bool {
    method == Method::POST && path == "/api/v1/configure"
}

/// STEP 4b for `POST /api/v1/dlq/:id/replay`: registry admission for the
/// destination its body NAMES (`is_replay_with_dest` is the gate on the arm;
/// the address-keyed replay reads no body and reaches this function never).
///
/// A replay with `{queue, partition}` is a MOVE, and the move provisions what
/// it does not find — `queen.log_dlq_move_v1` calls `log_push_one_v1` with
/// `p_pid` NULL, the branch whose own header says it "resolves — and PROVISIONS
/// — queue and partition under p_tenant, exactly as a first-contact producer
/// push does". So it answers the same caps, through the same `admit_pairs` and
/// with the same 403 and log line: `enforce_configure`'s argument, one route
/// over.
///
/// A destination is admitted as a PAIR or not at all:
///
///   both halves  the pair, so both caps (`admit_pairs`)
///   one half     400. An omitted half keeps the source row's, and that name is
///                in the broker's dead-letter row, not in this request — so
///                neither cap can be answered for the pair that will actually
///                be provisioned. A `{queue}`-only move into a queue the tenant
///                already has creates a partition inside it whose name this
///                proxy never sees, and a `{partition}`-only move creates one
///                inside a queue it never sees: both are `max_partitions_per_queue`
///                one dead letter at a time, repeatable, which is the hole this
///                function exists to close. The caller loses nothing by naming
///                both halves — an omitted half is the source row's, and the
///                source row is on the caller's own screen — so the refusal
///                names the missing one and says to send it.
///   neither      nothing to admit, and nothing gets created: the destination
///                is the source row's own pair, and the broker resolved that
///                pair (queue JOIN partition, `db::dlq_row_for_move`) before it
///                could read the row at all. A `{}` replay cannot provision.
///
/// Unparseable bodies are forwarded verbatim for the broker's own 400, the rule
/// `enforce_configure` and `enforce_produce` already follow: no half-enforcement
/// on a body this proxy could not read. The difference from a half-named one is
/// the outcome upstream — a body the broker will 400 creates nothing, a
/// half-named one succeeds and creates rows.
async fn admit_replay_dest(
    st: &St,
    ctx: &ClusterCtx,
    bytes: &Bytes,
    rid: &str,
) -> Result<(), Response> {
    let (queue, partition) = match parse_replay_dest(bytes) {
        Ok(d) => d,
        Err(()) => {
            tracing::warn!(
                target: "limits", cluster = %ctx.slug, rid,
                "replay body unparseable; forwarding without admin enforcement"
            );
            return Ok(());
        }
    };
    match (queue.as_deref(), partition.as_deref()) {
        (Some(q), Some(p)) => {
            admit_pairs(&st.registry, st.limits.enforcing(), ctx, [(q, p)], rid).await
        }
        (Some(_), None) => half_named_dest(st, ctx, "partition", rid),
        (None, Some(_)) => half_named_dest(st, ctx, "queue", rid),
        (None, None) => Ok(()),
    }
}

/// The refusal for a replay body that names ONE half of its destination:
/// `missing` is the half it left out, which is the half the caller has to add.
///
/// 400 rather than 403: nothing is over its cap here — the request simply does
/// not say what it will create, and the fix is in the caller's hands. The
/// message is worded so the dashboard can file it under the Advanced input it
/// is about (`useDlqReplay.js` reads "queue override" / "partition override"
/// out of the text), and `invalid_request` is the 400 code the rest of this
/// proxy already uses (console.rs, operator.rs, oauth.rs).
///
/// Shadow-aware like every other decision in this file: a cell configured to
/// measure instead of enforce refuses nothing, and logs the same canonical
/// `kind` + `would_block` line the cap denials log — ONE MESSAGE, written
/// twice with `(shadow)` on the second, which is the shape
/// `log_configure_deny` and `limits.rs::log_deny` already use. The shadow arm
/// used to say the generic "shadow deny" instead, so the two halves of one
/// refusal did not share a name: an operator grepping for the line they had
/// seen enforcing found nothing in a shadow cell, and the line that WAS there
/// sat in the pile every shadow deny in the proxy lands in.
fn half_named_dest(st: &St, ctx: &ClusterCtx, missing: &str, rid: &str) -> Result<(), Response> {
    if !st.limits.enforcing() {
        tracing::warn!(
            target: "limits", kind = "replay_dest", would_block = "replay_dest",
            cluster = %ctx.slug, missing, rid, "replay destination named in half (shadow)"
        );
        return Ok(());
    }
    tracing::warn!(
        target: "limits", kind = "replay_dest", cluster = %ctx.slug, missing,
        blocked = true, rid, "replay destination named in half"
    );
    Err(errors::json_error(
        StatusCode::BAD_REQUEST,
        "invalid_request",
        &format!(
            "{missing} override missing: a replay that names a destination must name both \
             `queue` and `partition`, so the pair can be admitted against this plan's caps. \
             An omitted half is the dead-letter row's own value: send it explicitly"
        ),
    ))
}

/// The optional body of a replay, in the shape the handler's
/// `parse_replay_overrides` accepts: absent, empty or `{}` names no
/// destination, and either half may be present on its own (what that means for
/// admission is `admit_replay_dest`'s decision, not this parser's). A present
/// name comes back TRIMMED, the way the handler trims it, and a
/// present-but-blank one is read as absent — the broker refuses that with a
/// 400, and this function's job is to name what will be CREATED, not to validate.
#[derive(Deserialize)]
struct ReplayDest {
    queue: Option<String>,
    partition: Option<String>,
}

fn parse_replay_dest(bytes: &[u8]) -> Result<(Option<String>, Option<String>), ()> {
    // The dashboard's plain "Replay" sends no body at all; a `{}` from a curl
    // is the same request. Neither names a destination.
    if bytes.iter().all(|b| b.is_ascii_whitespace()) {
        return Ok((None, None));
    }
    let d: ReplayDest = serde_json::from_slice(bytes).map_err(|_| ())?;
    // TRIMMED, because the handler trims: `parse_replay_overrides` is
    // `b.queue.map(|q| q.trim().to_string())` (server/src/handlers/messages.rs),
    // so the name that reaches `log_dlq_move_v1` — and the queue the SP
    // PROVISIONS — is the trimmed one. `str::trim` on both sides, so the two
    // agree on what whitespace is rather than on an ASCII rule here and a
    // Unicode one there.
    //
    // Admitting the padded spelling instead would make the registry's count
    // and the broker's rows two different sets, in both directions and
    // permanently: `" orders"` is remembered (and persisted to
    // queen_proxy.queues by `enqueue_persist`) as a queue that will never
    // exist, spending a plan slot on a name nobody can address, while
    // `orders` — the queue the move actually creates — is admitted by nobody
    // and later counted a SECOND time by the first push that spells it
    // plainly. One queue at the broker, two against the cap.
    //
    // The body still travels VERBATIM (§5, parse-only, no rewrite): the broker
    // trims it again on arrival, which is exactly why trimming here is safe —
    // this function reports what will be created, and the trim is what makes
    // that report true.
    let named = |v: Option<String>| v.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    Ok((named(d.queue), named(d.partition)))
}

/// The two DLQ replay routes (PLAN_DASHBOARD_ACTIONS.md §2.0/§2.3): the
/// address-keyed `POST /api/v1/messages/:pid/:txid/retry` the SDKs and
/// `queenctl dlq retry` already call, and the row-id-keyed
/// `POST /api/v1/dlq/:id/replay` the move primitive adds.
///
/// The other QueueAdmin route this file has an opinion about, and for the
/// opposite reason to `is_configure`: that one needs a body, this one needs a
/// GATE. `classify` answers `QueueAdmin` for both paths, which is the right
/// authorization verdict and a silent one about quota — `plan_gates` runs the
/// push blocks for `Produce` and `Gated(_, Grow)` only, so a class that is
/// neither sails past them. A replay writes a frame into a live partition, so
/// the blocks must see it; the class must stay `QueueAdmin`, so they cannot see
/// it through the class. This predicate is what makes the two separable.
///
/// The method is checked here rather than inherited from `classify`'s
/// method-exact arms, for the reason `is_configure` and `is_streams_cycle`
/// state in their own words: a gate that trusts another function's method rule
/// breaks quietly the day that rule moves, and this one stands in front of a
/// quota.
///
/// The SHAPE is the same one `classify` matches, and for the same reason: a
/// transaction id is arbitrary caller text, so `/api/v1/messages/:pid/retry` is
/// the address of a message whose transaction id is `retry` rather than a
/// replay of anything. Matching a suffix would gate — and, at step 4b, buffer
/// and admit — a route that does not exist.
fn is_dlq_replay(method: &Method, path: &str) -> bool {
    if method != Method::POST {
        return false;
    }
    let retry = path
        .strip_prefix("/api/v1/messages/")
        .and_then(|rest| rest.strip_suffix("/retry"))
        .is_some_and(|mid| {
            let mut seg = mid.split('/');
            matches!(
                (seg.next(), seg.next(), seg.next()),
                (Some(pid), Some(txid), None) if !pid.is_empty() && !txid.is_empty()
            )
        });
    retry || is_replay_with_dest(method, path)
}

/// The replay route that can NAME a destination: the row-id-keyed one.
///
/// Its sibling replays IN PLACE and reads no body at all
/// (`handlers/messages.rs`: "This route replays IN PLACE. The destination
/// override is the id-addressed route's affordance"), so there is nothing there
/// to admit — and admitting one anyway would be worse than doing nothing:
/// `Registry::admit` REMEMBERS the name it allows, so a `{"queue":"…"}` posted
/// to the retry route would spend a plan slot on a queue the broker is never
/// going to create, until the reconciler prunes it back to the broker's own
/// inventory. The day that route learns the override, this predicate is what
/// has to grow, and the admission follows it.
fn is_replay_with_dest(method: &Method, path: &str) -> bool {
    method == Method::POST
        && path
            .strip_prefix("/api/v1/dlq/")
            .and_then(|rest| rest.strip_suffix("/replay"))
            .is_some_and(|id| !id.is_empty() && !id.contains('/'))
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
        // A non-positive value means that rule is DISABLED, i.e. data is kept
        // forever — retention.rs gates on `retention_enabled AND
        // retention_seconds > 0`. Unbounded is not "under the ceiling", but it
        // is also exactly what every client sends by default (the JS
        // QUEUE_DEFAULTS post retentionSeconds: 0), so refusing it would 403 an
        // out-of-the-box `.queue(x).create()`. Unbounded growth is the storage
        // quota's job (max_retained_bytes -> push blocked, §6.1); this ceiling
        // caps what a tenant explicitly asks to keep.
        //
        // An ABSENT key is a different thing and nothing to check: since 1.6.0
        // /configure MERGES unless the body says `"mode":"replace"`, so a body
        // that does not mention a retention option is not asking for anything —
        // the queue keeps a window this gate already admitted. (The one gap that
        // leaves is a tenant DOWNGRADED to a lower ceiling: their old window
        // survives an unrelated edit, and it is the plan change, not this
        // request, that has to bring it back into line.)
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

/// Items in an ephemeral push body: `{queue, partition?, messages:[...]}`
/// (EPHEMERAL_QUEUES.md §3.1). Only the array length is wanted — the per-item
/// payload caps and the registry belong to the durable path — so the elements
/// are counted without being materialised.
///
/// UNPARSEABLE COUNTS 0, deliberately: the same stance `enforce_produce` takes
/// on a body it cannot read. The broker rejects the malformed body too, so
/// nothing is stored either way, and the caller gets a named 400 instead of a
/// 429 about a rate it never consumed.
fn count_ephemeral_push_items(bytes: &[u8]) -> u64 {
    #[derive(Deserialize)]
    struct EphPushLite {
        #[serde(default)]
        messages: Vec<serde::de::IgnoredAny>,
    }
    serde_json::from_slice::<EphPushLite>(bytes)
        .map(|p| p.messages.len() as u64)
        .unwrap_or(0)
}

/// Count an ephemeral push 201: `{"pushed":N}` (§3.1). The family answers one
/// number rather than a per-item status array because the request is
/// all-or-nothing — there is no `duplicate` (no dedup) and no `buffered` (no
/// spool: a RAM queue has nowhere to spill to), so `accepted` is the whole
/// tally and the §6.10 maintenance signal can never fire off it. `None` on a
/// shape we cannot read, handled at the call site exactly like a durable one.
fn count_ephemeral_pushed(bytes: &[u8]) -> Option<PushCounts> {
    #[derive(Deserialize)]
    struct EphPushedLite {
        pushed: u64,
    }
    let p: EphPushedLite = serde_json::from_slice(bytes).ok()?;
    Some(PushCounts { accepted: p.pushed, buffered: 0, total: p.pushed })
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

/// The push-block verdict, shared by `Produce` and by the growing half of a
/// gated family (PLAN_KV_TIMERS.md §9.5). `None` = nothing is blocking.
///
/// Three causes, three codes — Track C clients switch on the code and treat
/// `storage_quota_exceeded` as terminal. Each live flag is written by exactly
/// one thing (storage: the pump in main.rs off registry over_storage; monthly:
/// the rollup task off plans.monthly_msgs_quota), so when one is set the cause
/// is unambiguous; ctx.status is the DB lifecycle one (tenant `grace` or
/// cluster `push_blocked` — cache.rs::merge_status) and never says *why* it was
/// set, so it keeps the generic code. Live flags first: they are the more
/// specific claim.
///
/// Both live flags are HARD gates — checked and enforced regardless of
/// `limits.enforcing()`, unlike every rate decision in `handle`. That is
/// deliberate (a quota that only warns protects neither the cell's disk nor the
/// bill) and it is the surprising part: a misconfigured monthly_msgs_quota
/// blocks production pushes even on a cell deployed in shadow mode.
fn push_block_response(st: &St, ctx: &ClusterCtx) -> Option<Response> {
    // `_for`, not the cluster-id form: the storage verdict now has two sources,
    // the pump's flag and the in-flight estimate that catches a cluster going
    // over BETWEEN the broker's retained-bytes computations (limits.rs,
    // `StorageAccount`), and the second needs this cluster's own cap.
    match st.limits.push_block_reason_for(ctx) {
        Some(crate::limits::PushBlock::Storage) => {
            return Some(errors::err_403(
                errors::CODE_STORAGE_QUOTA,
                "storage quota exceeded; pushes blocked",
            ));
        }
        Some(crate::limits::PushBlock::MonthlyQuota) => {
            return Some(errors::err_403(
                errors::CODE_QUOTA_EXCEEDED,
                "monthly message quota (monthly_msgs_quota) exhausted; pushes blocked until the next calendar month",
            ));
        }
        None => {}
    }
    if ctx.status == ClusterStatus::PushBlocked {
        return Some(errors::err_403(errors::CODE_PUSH_BLOCKED, "pushes blocked (billing hold)"));
    }
    None
}

/// Does this `GatedOp::Mixed` batch contain an op that GROWS stored state?
///
/// PLAN_KV_TIMERS.md §9.6. `POST /api/v1/timers` carries `cancel` in the same
/// array as `schedule`, and `POST /api/v1/kv` carries `get`/`delete` in the
/// same array as `put`/`incr`. The batch is refused only when it really adds
/// something, and then it is refused WHOLE — never half-applied, never
/// silently trimmed — so the caller gets one unambiguous answer about one
/// request.
///
/// Body shape, both endpoints: a bare array of ops, or `{"operations":[...]}`.
/// The `op` field names the operation.
///
/// UNPARSEABLE IS NOT GROWING. A body this function cannot read is forwarded
/// and the broker answers `400` with a named reason, which is a better error
/// than a `403` about a quota. The blocked tenant gains nothing by it: the
/// broker rejects the malformed body too, so no row is created either way.
fn mixed_batch_grows(body: &[u8]) -> bool {
    let root: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let ops = match &root {
        serde_json::Value::Array(a) => a,
        serde_json::Value::Object(o) => match o.get("operations") {
            Some(serde_json::Value::Array(a)) => a,
            _ => return false,
        },
        _ => return false,
    };
    // A CLOSED list of the ops that only free or read, so an op this proxy has
    // never heard of counts as growing. The taxonomy is owned by the stored
    // procedures; when it gains a member, the fail-safe direction is that a
    // tenant over quota waits rather than that an unknown op slips the gate.
    ops.iter().any(|o| {
        !matches!(
            o.get("op").and_then(|v| v.as_str()),
            Some("get" | "getPrefix" | "delete" | "cancel")
        )
    })
}

// ---------------------------------------------------------------------------
// The streams CYCLE — routes.rs' other `Mixed` family
//
// One request carries both halves at once: the SOURCE ack, which only advances
// a cursor, and the SINK `push_items`, which are produce — they grow retained
// bytes exactly like `/api/v1/push`. The pop that fed the cycle already crossed
// this proxy through `/api/v1/pop` and was metered there as `Delivery`, and the
// ack is deliberately not message-metered (the posture `/api/v1/ack` has always
// had: a cursor advance is a request, not a message). The sink items are
// therefore the ONLY messages of the family this proxy would otherwise never
// count — the gap these three functions close.
//
// Wire contract, both directions: server/src/handlers/streams.rs
// (handle_streams_cycle) packs the request, server/sql/procedures/007_log_streams.sql
// (log_streams_cycle_v1) writes it and builds the response.
// ---------------------------------------------------------------------------

/// The one cycle route. Named once and used twice — the ingress arm that counts
/// the sink items, and the response arm that bills them.
const STREAMS_CYCLE_PATH: &str = "/streams/v1/cycle";

/// Is this THE cycle request? `classify` gives the path its own `Mixed` class
/// only for POST; the same path with any other method falls into the family's
/// `Open` bucket, so the method check lives here — the `is_configure`
/// precedent, not the `EPH_PUSH_PATH` one (that path IS method-exact upstream).
fn is_streams_cycle(method: &Method, path: &str) -> bool {
    method == Method::POST && path == STREAMS_CYCLE_PATH
}

/// Does this cycle body carry the produce half of the family?
///
/// The cycle's twin of `mixed_batch_grows`, and it needs its own parser rather
/// than that one: a cycle body is a JSON OBJECT with no `operations` array,
/// which the kv/timers sniffer reads as "does not grow" — it would fail OPEN on
/// every cycle, sink emits included.
///
/// Unlike kv/timers there is no op taxonomy to fail closed on. A cycle grows
/// stored state exactly when it emits sink messages, so a non-empty
/// `push_items` IS the predicate. Everything else in the body only advances or
/// deletes — the source ack, the state upserts/deletes — and refusing those
/// would strand a blocked tenant's backlog behind its own quota, which is the
/// §9.6 trap `Mixed` exists to avoid.
///
/// UNPARSEABLE IS NOT GROWING, the same tolerance the kv sniffer takes: a body
/// we cannot read travels to the broker's own 400 (`handle_streams_cycle`
/// rejects it before the SP) rather than being half-enforced here as a 403
/// about a quota. The blocked tenant gains nothing by it — nothing is written
/// either way.
///
/// The predicate is the ARRAY, not the number of items the broker would keep:
/// a `push_items` whose entries all have an empty queue is refused even though
/// the broker would have skipped every one of them. Deliberate, and the same
/// safe direction `mixed_batch_grows` takes on an op it has never heard of —
/// the tenant can re-send the cycle without the degenerate array.
fn cycle_batch_grows(body: &[u8]) -> bool {
    #[derive(Deserialize)]
    struct CycleGrowLite<'a> {
        // Absent, `null`, or not an array all mean "no sink emits" — the same
        // three shapes `handle_streams_cycle`'s `as_array()` reads as nothing
        // to push. Borrowed raw elements: their payloads are never materialised.
        #[serde(default, borrow)]
        push_items: Option<Vec<&'a RawValue>>,
    }
    // A non-object body (array, scalar, garbage) fails this parse, which is
    // also how "parses as a JSON object" is enforced.
    match serde_json::from_slice::<CycleGrowLite>(body) {
        Ok(c) => c.push_items.is_some_and(|items| !items.is_empty()),
        Err(_) => false,
    }
}

/// The sink half of a cycle request: what the message bucket must see, and the
/// per-group counts the response cannot supply.
#[derive(Debug, Default, PartialEq, Eq)]
struct CycleSinks {
    /// Every item the broker will pack into a sink segment.
    total: u64,
    /// One entry per DISTINCT (queue, partition), first-seen order, with the
    /// number of items in it. The response echoes ONE `push_results` element
    /// per group and never says how many messages the group carried, so this is
    /// the only place that number exists — it is threaded from step 4 to step 6.
    groups: Vec<((String, String), u64)>,
    /// Payload bytes over every COUNTED item, for the in-flight storage
    /// account. `heaviest` answers the per-item cap and cannot answer this: the
    /// quota is about the total a cycle adds to the disk, not its worst element.
    payload_bytes: u64,
    /// `(index in push_items as sent, payload bytes)` of the HEAVIEST counted
    /// item — everything the per-item payload cap needs, without a vector of
    /// lengths. `enforce_produce` refuses the FIRST item over the cap; this
    /// refuses the worst one, because the cap is not known down here (this is a
    /// parse, not a limits decision) and carrying the maximum is the one number
    /// that answers the question for any cap. The index is the position in the
    /// array AS SENT, skipped items included, so it points at the element the
    /// client can actually find.
    heaviest: Option<(usize, usize)>,
}

/// Group a cycle body's `push_items` EXACTLY as the broker's own packer does
/// (`handle_streams_cycle`): an item whose `queue` is missing, not a string, or
/// empty is SKIPPED (there is no sink to push to), and a `partition` that is
/// missing, not a string, or empty defaults to `Default`. Both defaults are
/// load-bearing: `count_cycle_accepted` matches the response back by
/// (queue, partition), so a divergence here either drops a group's messages
/// from the bill or bills a group the broker never wrote.
///
/// Two-level parse on purpose. The outer pass captures each item as a raw
/// slice, so the payloads — the bulk of the body — are never materialised; the
/// inner pass reads only the two routing fields of one item, and an item it
/// cannot read is SKIPPED rather than failing the whole count, because that is
/// what the broker does with it (a non-object element has no `queue`, so it
/// pushes nothing, and the items around it still travel). Both fields land in a
/// `Value` rather than a `String` for the same reason: the broker reads them
/// with `as_str()`, so a numeric `queue` is one skipped item there, not a
/// rejected body — and a name written with JSON escapes unescapes to the same
/// text on both sides.
///
/// The payload measure is the RAW JSON TEXT of the item's `payload` — the same
/// basis `enforce_produce` uses (`ItemInfo::payload_len`, `RawValue::get().len()`),
/// deliberately, so the identical bytes meet the identical cap whether a tenant
/// sends them through `/api/v1/push` or emits them from a stream. `data` is the
/// broker's accepted alias for the field and is measured the same; an item with
/// neither is stored as `{}` and measures 0, which no cap can refuse. Measured
/// only for items that COUNT: a skipped item never becomes a message, so
/// refusing a body over its payload would refuse bytes the broker discards.
///
/// UNPARSEABLE COUNTS 0, the stance `count_ephemeral_push_items` takes on a
/// body it cannot read: the broker answers its own 400 and nothing is stored,
/// so a 429 about a rate the tenant never consumed would be the wrong answer.
fn count_cycle_push_items(body: &[u8]) -> CycleSinks {
    #[derive(Deserialize)]
    struct CycleLite<'a> {
        #[serde(default, borrow)]
        push_items: Vec<&'a RawValue>,
    }
    #[derive(Deserialize)]
    struct SinkItemLite<'a> {
        #[serde(default)]
        queue: serde_json::Value,
        #[serde(default)]
        partition: serde_json::Value,
        // Borrowed from the item slice, which borrows from `body`: the payload
        // is measured, never copied.
        #[serde(borrow, default)]
        payload: Option<&'a RawValue>,
        #[serde(borrow, default)]
        data: Option<&'a RawValue>,
    }
    let Ok(parsed) = serde_json::from_slice::<CycleLite>(body) else {
        return CycleSinks::default();
    };
    let mut out = CycleSinks::default();
    for (idx, raw) in parsed.push_items.into_iter().enumerate() {
        let Ok(item) = serde_json::from_str::<SinkItemLite>(raw.get()) else {
            continue;
        };
        let Some(queue) = item.queue.as_str().filter(|s| !s.is_empty()) else {
            continue;
        };
        let partition =
            item.partition.as_str().filter(|s| !s.is_empty()).unwrap_or(DEFAULT_PARTITION);
        out.total += 1;
        // `payload`, else the broker's `data` alias, else the `{}` it stores.
        let payload_len = item.payload.or(item.data).map(|p| p.get().len()).unwrap_or(0);
        out.payload_bytes = out.payload_bytes.saturating_add(payload_len as u64);
        match out.heaviest {
            Some((_, len)) if len >= payload_len => {}
            _ => out.heaviest = Some((idx, payload_len)),
        }
        // Linear scan rather than a map: one cycle emits to a handful of sinks
        // (the broker builds the same groups the same way), so a HashMap would
        // cost two key clones per ITEM to save a walk over one or two entries.
        match out.groups.iter_mut().find(|(k, _)| k.0 == queue && k.1 == partition) {
            Some((_, n)) => *n += 1,
            None => out.groups.push(((queue.to_string(), partition.to_string()), 1)),
        }
    }
    out
}

/// The per-item payload cap for a cycle's sink items — `enforce_produce`'s
/// first hard check, on the route that emits the same messages by another door.
/// `Err` is the ready 413.
///
/// HARD, like every size cap in this file: enforced whether or not the cell is
/// enforcing rate limits, because the caps bound what the proxy and the cell
/// have to hold, not what the tenant is billed for.
///
/// The message names `push_items[i]` rather than push's `item {i}`: it is the
/// field the client actually sent, and the index is the position in that array
/// as sent (see `CycleSinks::heaviest`, which also explains why the item
/// reported is the heaviest rather than the first over the cap).
fn cycle_payload_cap(sinks: &CycleSinks, cap: Option<usize>) -> Result<(), Response> {
    let (Some(cap), Some((idx, len))) = (cap, sinks.heaviest) else {
        return Ok(());
    };
    if len > cap {
        return Err(errors::err_413(&format!(
            "push_items[{idx}]: payload {len} bytes exceeds max_payload_bytes ({cap})"
        )));
    }
    Ok(())
}

/// Sink messages the broker CONFIRMS it wrote, read off a cycle 2xx response
/// and the request groups that produced it. `None` on a shape we cannot read.
///
/// The response carries one `push_results` element per (queue, partition) group
/// and no per-message count — that number exists only in the request, which is
/// why `groups` is threaded through from step 4. A group whose element says
/// `queued` bills every item the request put in it; `duplicate` bills nothing,
/// because `log_push_one_v1` probes BEFORE it allocates and a duplicate verdict
/// returns having written nothing at all (003_log_push: no allocator bump, no
/// inserts, whole segment) — the same exclusion `count_push_statuses` applies
/// on the push path. A group with no element at all is not confirmed and is not
/// billed.
///
/// `success:false` is ZERO, not "count whatever echoed": the SP runs each
/// element inside a savepoint and the inline sink pushes roll back with it
/// (007_log_streams — that rollback is what makes the cycle exactly-once), so a
/// failed cycle stored nothing however far it got. Same reasoning `txn_outcome`
/// applies to a rolled-back transaction that answered HTTP 200.
///
/// Counting is driven from the REQUEST side, so a group can be billed at most
/// once whatever the response repeats.
fn count_cycle_accepted(bytes: &[u8], groups: &[((String, String), u64)]) -> Option<u64> {
    #[derive(Deserialize)]
    struct SinkResultLite {
        #[serde(default)]
        queue: String,
        #[serde(default)]
        partition: String,
        #[serde(default)]
        status: String,
    }
    #[derive(Deserialize)]
    struct CycleRespLite {
        // Required: a body without it is `None`, never assumed successful.
        success: bool,
        #[serde(default)]
        push_results: Vec<SinkResultLite>,
    }
    let resp: CycleRespLite = serde_json::from_slice(bytes).ok()?;
    if !resp.success {
        return Some(0);
    }
    let mut accepted: u64 = 0;
    for ((queue, partition), n) in groups {
        let written = resp
            .push_results
            .iter()
            .any(|r| r.status == "queued" && r.queue == *queue && r.partition == *partition);
        if written {
            accepted += n;
        }
    }
    Some(accepted)
}

fn content_length(headers: &HeaderMap) -> u64 {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// Is the plan flag for this gated family on? One arm per `Feature`, so the
/// compiler names the file to edit when a family is added. A `Features` a plan
/// row never mentioned is all-false (`cache::parse_features`), which is what
/// makes a cell that has never heard of a feature deny it.
fn feature_enabled(f: crate::routes::Feature, features: &crate::state::Features) -> bool {
    match f {
        crate::routes::Feature::Streams => features.streams,
        crate::routes::Feature::Traces => features.traces,
        crate::routes::Feature::Kv => features.kv,
        crate::routes::Feature::Timers => features.timers,
        crate::routes::Feature::Ephemeral => features.ephemeral,
    }
}

/// The two RAM-family data-plane paths (EPHEMERAL_QUEUES.md §3.1). `classify`
/// is method-exact on both, so matching the path here already implies the
/// method — unlike `/api/v1/configure`, which classifies for every method and
/// therefore needs `is_configure` to check one.
const EPH_PUSH_PATH: &str = "/api/v1/ephemeral/push";
const EPH_POP_PATH: &str = "/api/v1/ephemeral/pop";

/// Both pop families: the response shape is the same `{...,"messages":[...]}`,
/// so the delivery count and the `debit_deliveries` that follows it are read
/// the same way. The RAM pop is `Gated`, never `Consume`, so widening this
/// does not move any durable route between classes.
fn is_pop_path(path: &str) -> bool {
    path.starts_with("/api/v1/pop/queue/") || path == EPH_POP_PATH
}

/// Did this replay response say a message was written?
///
/// PLAN_DASHBOARD_ACTIONS.md D4: a replay is billed as a push of ONE message,
/// because that is what it is — one frame, in one partition, growing the
/// retained bytes the storage block already treats it as growing. The count is
/// not a function of (path, class), so it cannot live in `op_for`; it has to be
/// read off the answer, exactly like the cycle's sink verdicts.
///
/// The two shapes this has to read, and they agree where it matters. BOTH
/// routes answer `{success:true, result:"moved"|"duplicate", ...}` on every
/// broker that carries the move primitive — they share one body builder
/// (`handlers/messages.rs::move_response`) — and the verdict is the whole
/// answer: `moved` wrote a frame, `duplicate` found the destination's dedup
/// window already holding the deterministic id and wrote nothing.
///
/// The absent-`result` arm is BACKWARD COMPATIBILITY, not the current wire, and
/// it has to stay while a proxy can stand in front of an older broker — which
/// is the M0 ship order itself (W0 ships as 1.5.4 before the move primitive
/// lands). A broker of 1.5.3 or earlier answers the address-keyed retry route
/// `success:true` with no verdict field, and only once its push has been
/// accepted, so an absent field is a write. Delete the arm as dead code and
/// every replay served by such a broker stops being billed, silently.
///
/// Everything else is NOT BILLED. An unreadable body, a `success:false` (the
/// "push rejected, still dead-lettered" answer), a `result` string this proxy
/// has not been told about: charge nothing we cannot confirm, the stance the
/// push and cycle arms take on a parse failure. Under-billing a shape we do not
/// recognise beats billing a message that was never stored. `gone` never
/// reaches here at all — it is a 404.
fn replay_stored_a_message(body: &[u8]) -> Option<bool> {
    let root: serde_json::Value = serde_json::from_slice(body).ok()?;
    if !root.get("success")?.as_bool()? {
        return Some(false);
    }
    Some(matches!(
        root.get("result").and_then(|v| v.as_str()),
        None | Some("moved")
    ))
}

/// What one replay answer books on the meter (D4), as a value rather than as
/// two `record` calls inside `handle`.
///
/// A pure function on purpose: the arm it came out of could be deleted whole
/// and every proxy test still passed, because its decision was only ever
/// expressed as side effects on a live `Meter`. The decision is the billing —
/// under-count and the revenue is gone, over-count and a monthly-quota block
/// trips on messages nobody wrote — so it is stated here, where a test can read
/// it back.
///
/// Always exactly one `reqs`, on the route's own class (`op_for` -> `Configure`
/// for a queue-admin replay): one HTTP request, booked once, in the bucket
/// every other DLQ action lands in. The message rides a SECOND sample carrying
/// only itself — `reqs: 0` because the first sample already counted the
/// request, and no bytes for the same reason — under `OpClass::Push`, because a
/// replayed frame is stored, retained and rolled up exactly like a pushed one,
/// so it belongs in the `usage_days` row a push lands in and in the monthly
/// total the `monthly_msgs_quota` block reads. `stored: false` books the
/// request alone: a `duplicate`, a refusal, a verdict this proxy has not been
/// told about, or an answer it never managed to read.
fn replay_samples(
    cluster_id: uuid::Uuid,
    op: OpClass,
    bytes_in: u64,
    bytes_out: u64,
    stored: bool,
) -> Vec<Sample> {
    let mut out = vec![Sample { cluster_id, op, reqs: 1, msgs: 0, bytes_in, bytes_out }];
    if stored {
        out.push(Sample {
            cluster_id,
            op: OpClass::Push,
            reqs: 0,
            msgs: 1,
            bytes_in: 0,
            bytes_out: 0,
        });
    }
    out
}

/// Map a (path, class) to the metering op class. Consume that is not a pop
/// (ack / lease) and Gated surfaces meter as reqs-only `Read` — see report.
///
/// TWO surfaces bill messages WITHOUT changing class here, and for the same
/// reason: the count is not a function of (path, class). The streams cycle
/// stays `Read` for its request while `handle`'s cycle arm records a SECOND
/// `Push` sample for the sink items, which needs the request's per-group item
/// counts AND the response's per-group verdicts. A DLQ replay stays
/// `Configure` — the queue-admin authority it is classified for — while
/// `handle`'s replay arm records one `Push` message when the answer says a
/// frame was written (D4, `replay_stored_a_message`).
fn op_for(path: &str, class: RouteClass) -> OpClass {
    match class {
        RouteClass::Produce if path == "/api/v1/push" => OpClass::Push,
        RouteClass::Produce => OpClass::Txn,
        RouteClass::Consume if is_pop_path(path) => OpClass::Delivery,
        RouteClass::Consume => OpClass::Read,
        RouteClass::QueueAdmin => OpClass::Configure,
        RouteClass::Read => OpClass::Read,
        // EPHEMERAL_QUEUES.md Q6, decided before the family shipped: the RAM
        // data plane meters as what it is — push as `Push`, pop as `Delivery`
        // — instead of falling into the reqs-only `Read` gap below. Its
        // messages cross this proxy exactly like durable ones and cost the
        // cell exactly as much bandwidth; only where they are stored differs,
        // and that is the storage quota's business, not the meter's. The other
        // verbs (ack, configure, reset, delete, the two status reads) keep the
        // `Read` default: they carry no messages.
        RouteClass::Gated(crate::routes::Feature::Ephemeral, _) if path == EPH_PUSH_PATH => {
            OpClass::Push
        }
        RouteClass::Gated(crate::routes::Feature::Ephemeral, _) if path == EPH_POP_PATH => {
            OpClass::Delivery
        }
        // Gated surfaces meter as reqs-only `Read`. That INCLUDES
        // `POST /api/v1/timers`, and PLAN_KV_TIMERS.md §9.7 says it should not
        // stay that way: a timer is a message that will never cross this proxy
        // when it fires, so the promise has to be billed at schedule time, one
        // message per `schedule` op. That is F8 P4 and is NOT implemented here
        // — see the report. Until it is, timers are free.
        //
        // It also includes `POST /streams/v1/cycle`, whose reqs belong here and
        // whose sink MESSAGES are billed by the extra `Push` sample described
        // above the function. The cycle is no longer free; its class is.
        RouteClass::Gated(_, _) => OpClass::Read,
        // Operator surfaces are reqs-only reads like any other GET. They still
        // meter — against the cluster the operator is acting on — because the
        // request really did cost the cell something; the volume is a
        // dashboard's poll rate, not a tenant's workload.
        RouteClass::Operator => OpClass::Read,
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

    // ---- PLAN_KV_TIMERS.md §9.6: the mixed batch, and why the cancel lives ----

    #[test]
    fn cancel_only_timer_batch_does_not_grow() {
        // THE case the whole split exists for. A tenant over quota still has
        // timers in flight; nothing stops a fire on its own; this batch is the
        // only way to stop them. It must not be refused.
        let body = br#"[{"op":"cancel","queue":"orders","timerKey":"a"},
                        {"op":"cancel","queue":"orders","timerKey":"b"}]"#;
        assert!(!mixed_batch_grows(body));
        // ...and the `{"operations":[...]}` spelling of the same thing
        let wrapped = br#"{"operations":[{"op":"cancel","queue":"q","timerKey":"a"}]}"#;
        assert!(!mixed_batch_grows(wrapped));
    }

    #[test]
    fn a_schedule_anywhere_in_the_batch_grows() {
        // One schedule hiding behind 255 cancels still grows: the batch is
        // refused whole rather than applied in part.
        let body = br#"[{"op":"cancel","queue":"q","timerKey":"a"},
                        {"op":"schedule","queue":"q","timerKey":"b","delayMs":1000},
                        {"op":"cancel","queue":"q","timerKey":"c"}]"#;
        assert!(mixed_batch_grows(body));
        assert!(mixed_batch_grows(br#"[{"op":"reschedule","queue":"q","timerKey":"b"}]"#));
    }

    #[test]
    fn kv_reads_and_deletes_do_not_grow_but_writes_do() {
        // §9.5: reads and DELETEs are always permitted, over quota included,
        // or a full tenant can never empty itself.
        assert!(!mixed_batch_grows(br#"[{"op":"get","ns":"a","key":"k"}]"#));
        assert!(!mixed_batch_grows(br#"[{"op":"getPrefix","ns":"a","prefix":"k"}]"#));
        assert!(!mixed_batch_grows(br#"[{"op":"delete","ns":"a","key":"k"}]"#));

        assert!(mixed_batch_grows(br#"[{"op":"put","ns":"a","key":"k","value":1}]"#));
        assert!(mixed_batch_grows(br#"[{"op":"putIfAbsent","ns":"a","key":"k"}]"#));
        assert!(mixed_batch_grows(br#"[{"op":"incr","ns":"a","key":"k","by":1}]"#));
    }

    #[test]
    fn unknown_and_missing_ops_count_as_growing() {
        // The op taxonomy is owned by the stored procedures. An op this proxy
        // has never heard of must not be the way past a quota.
        assert!(mixed_batch_grows(br#"[{"op":"somethingNew","ns":"a"}]"#));
        assert!(mixed_batch_grows(br#"[{"ns":"a","key":"k"}]"#), "no op field");
        assert!(mixed_batch_grows(br#"[{"op":123}]"#), "op is not a string");
    }

    #[test]
    fn an_unreadable_body_is_left_to_the_broker() {
        // Not "grows", because the answer a caller deserves for a malformed
        // body is the broker's named 400, not a 403 about a quota — and the
        // broker rejects it too, so no row is created either way.
        assert!(!mixed_batch_grows(b"not json"));
        assert!(!mixed_batch_grows(br#"{"nope":1}"#));
        assert!(!mixed_batch_grows(br#"42"#));
        assert!(!mixed_batch_grows(b"[]"), "an empty batch creates nothing");
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

    // ---- the streams cycle: its sink items are the family's only messages ----

    #[test]
    fn cycle_grows_exactly_when_it_emits_sink_items() {
        // A cycle that emits into a sink queue is produce, and on a blocked
        // cluster it is refused WHOLE.
        let emits = br#"{"query_id":"0189-q","partition_id":"0189-p","consumer_group":"g",
            "state_ops":[],"push_items":[{"queue":"enriched","payload":{"a":1}}],
            "ack":{"transactionId":"t","leaseId":"l","status":"completed","count":10}}"#;
        assert!(cycle_batch_grows(emits));

        // THE case the class exists for: this cycle only advances a cursor and
        // deletes state, so a blocked tenant keeps draining its source. Refuse
        // it and the quota freezes the very backlog the tenant has to work
        // through to get back under the quota.
        let ack_only = br#"{"query_id":"0189-q","partition_id":"0189-p",
            "state_ops":[{"op":"delete","key":"k"}],"release_lease":true,
            "ack":{"transactionId":"t","leaseId":"l","status":"completed","count":10}}"#;
        assert!(!cycle_batch_grows(ack_only));
        // the three spellings of "no sink emits" the broker's own `as_array()`
        // reads as nothing to push
        assert!(!cycle_batch_grows(br#"{"query_id":"q","push_items":[]}"#));
        assert!(!cycle_batch_grows(br#"{"query_id":"q","push_items":null}"#));
        assert!(!cycle_batch_grows(br#"{"query_id":"q","push_items":7}"#));
    }

    #[test]
    fn an_unreadable_cycle_body_is_left_to_the_broker() {
        // Same tolerance as the kv sniffer: a body we cannot read gets the
        // broker's named 400, not a 403 about a quota, and neither one writes.
        assert!(!cycle_batch_grows(b"not json"));
        assert!(!cycle_batch_grows(b""));
        // a non-object body is not a cycle at all (handle_streams_cycle 400s on
        // the missing query_id before the SP is called)
        assert!(!cycle_batch_grows(br#"[{"queue":"q"}]"#));
        assert!(!cycle_batch_grows(b"42"));

        // ...and THIS is why the cycle needs its own sniffer and its own arm
        // ahead of the generic `Mixed` one: a cycle body has no `operations`
        // array, so the kv/timers sniffer reads a sink emit as "does not grow"
        // and would let it straight past a storage block.
        let emits = br#"{"query_id":"q","push_items":[{"queue":"enriched","payload":1}]}"#;
        assert!(!mixed_batch_grows(emits));
        assert!(cycle_batch_grows(emits));
    }

    #[test]
    fn cycle_sink_items_group_like_the_broker_packs_them() {
        let body = br#"{"query_id":"q","partition_id":"p","push_items":[
            {"queue":"enriched","payload":{"a":1}},
            {"queue":"enriched","partition":"shard-1","payload":{"a":2}},
            {"queue":"enriched","partition":"","payload":{"a":3}},
            {"queue":"audit","payload":{"a":4}},
            {"queue":"enriched","partition":"shard-1","payload":{"a":5}}
        ]}"#;
        let sinks = count_cycle_push_items(body);
        // the message bucket sees every item...
        assert_eq!(sinks.total, 5);
        // ...and the meter sees them grouped exactly as the broker packs its
        // sink_segments: a missing partition AND an empty one both default to
        // `Default`, which is the key the response is matched back on.
        assert_eq!(
            sinks.groups,
            vec![
                (("enriched".to_string(), "Default".to_string()), 2),
                (("enriched".to_string(), "shard-1".to_string()), 2),
                (("audit".to_string(), "Default".to_string()), 1),
            ]
        );
    }

    #[test]
    fn cycle_items_without_a_queue_are_skipped_like_the_broker_skips_them() {
        // "no sink queue -> nothing to push" (handle_streams_cycle), so the
        // item is neither bucketed nor billed. A non-string queue is the same
        // verdict there (`as_str()` -> None) and must not take its neighbours
        // down with it.
        let body = br#"{"push_items":[
            {"payload":{"a":1}},
            {"queue":"","payload":{"a":2}},
            {"queue":42,"payload":{"a":3}},
            {"queue":"kept","payload":{"a":4}}
        ]}"#;
        let sinks = count_cycle_push_items(body);
        assert_eq!(sinks.total, 1);
        assert_eq!(sinks.groups, vec![(("kept".to_string(), "Default".to_string()), 1)]);

        // an element that is not an object at all: skipped, neighbours kept —
        // a whole-body parse would have counted 0 and under-billed the rest
        assert_eq!(count_cycle_push_items(br#"{"push_items":[7,{"queue":"kept","payload":1}]}"#).total, 1);

        // nothing to count: unreadable, no sink half, or an empty one
        assert_eq!(count_cycle_push_items(b"not json"), CycleSinks::default());
        assert_eq!(count_cycle_push_items(br#"{"query_id":"q"}"#), CycleSinks::default());
        assert_eq!(count_cycle_push_items(br#"{"push_items":[]}"#), CycleSinks::default());
    }

    /// The response says WHICH groups were written; the request says how many
    /// messages each one carried. Billing is the join of the two.
    #[test]
    fn cycle_bills_only_the_groups_the_response_confirms() {
        let groups = vec![
            (("enriched".to_string(), "Default".to_string()), 3),
            (("enriched".to_string(), "shard-1".to_string()), 2),
            (("audit".to_string(), "Default".to_string()), 5),
        ];
        // queued -> the group's whole request count; duplicate -> nothing (the
        // probe returns before allocating: that segment wrote no rows at all);
        // a group the response never mentions -> not confirmed, not billed.
        let resp = br#"{"success":true,"query_id":"q","partition_id":"p","queueName":"src",
            "state_ops_applied":2,"push_results":[
              {"queue":"enriched","partition":"Default","status":"queued","baseOffset":41},
              {"queue":"enriched","partition":"shard-1","status":"duplicate","dups":[{"i":0,"off":7}]}
            ],"ack_result":{"success":true,"count":10,"lease_released":true,"dlq":false}}"#;
        assert_eq!(count_cycle_accepted(resp, &groups), Some(3));

        // everything confirmed: the full 3 + 2 + 5
        let all_queued = br#"{"success":true,"push_results":[
              {"queue":"enriched","partition":"Default","status":"queued"},
              {"queue":"enriched","partition":"shard-1","status":"queued"},
              {"queue":"audit","partition":"Default","status":"queued"}
            ],"ack_result":null}"#;
        assert_eq!(count_cycle_accepted(all_queued, &groups), Some(10));

        // the key is matched EXACTLY, defaults included: a response that spelled
        // the defaulted partition differently confirms nothing, rather than
        // being billed against the wrong group
        let wrong_key = br#"{"success":true,"push_results":[
              {"queue":"enriched","partition":"","status":"queued"}]}"#;
        assert_eq!(count_cycle_accepted(wrong_key, &groups), Some(0));
        // a group we never sent contributes nothing either
        let stranger = br#"{"success":true,"push_results":[
              {"queue":"other","partition":"Default","status":"queued"}]}"#;
        assert_eq!(count_cycle_accepted(stranger, &groups), Some(0));

        // an ack-only cycle has no groups, so nothing can be billed (the call
        // site does not even buffer this response)
        assert_eq!(count_cycle_accepted(all_queued, &[]), Some(0));
    }

    #[test]
    fn a_failed_cycle_bills_nothing_and_an_unreadable_one_charges_nothing() {
        let groups = vec![(("enriched".to_string(), "Default".to_string()), 4)];
        // success:false is HTTP 200 too. The SP runs the element inside a
        // savepoint and the inline sink push rolls back with it, so whatever
        // push_results already echoed, nothing was stored.
        let failed = br#"{"success":false,"query_id":"q","partition_id":"p",
            "state_ops_applied":0,"push_results":[
              {"queue":"enriched","partition":"Default","status":"queued"}],
            "ack_result":null,"error":"lease expired"}"#;
        assert_eq!(count_cycle_accepted(failed, &groups), Some(0));

        // Never assumed successful: no `success` key, not JSON, or the wrong
        // top-level shape all come back None -> warn + msgs=0 at the call site.
        assert_eq!(count_cycle_accepted(br#"{"push_results":[]}"#, &groups), None);
        assert_eq!(count_cycle_accepted(b"not json", &groups), None);
        assert_eq!(count_cycle_accepted(br#"[{"success":true}]"#, &groups), None);
        assert_eq!(count_cycle_accepted(b"", &groups), None);
        // a readable success with no push_results at all is a confirmed zero,
        // not a parse failure
        assert_eq!(count_cycle_accepted(br#"{"success":true}"#, &groups), Some(0));
    }

    // ---- the cycle's sink half answers the PRODUCE caps ----

    fn cycle_ctx(max_queues: Option<i64>, max_partitions_per_queue: Option<i64>) -> ClusterCtx {
        ClusterCtx {
            cluster_id: uuid::Uuid::from_u128(42),
            tenant_id: uuid::Uuid::from_u128(43),
            broker_tenant: uuid::Uuid::from_u128(44),
            slug: "cycle-test".to_string(),
            cell_base_url: "http://127.0.0.1:1".to_string(),
            cell_token: None,
            status: ClusterStatus::Active,
            limits: crate::state::EffectiveLimits {
                max_queues,
                max_partitions_per_queue,
                ..Default::default()
            },
            features: crate::state::Features::default(),
        }
    }

    /// The `{error, code}` envelope of a refusal, so a test can pin the code a
    /// Track C client switches on rather than just the status line.
    async fn err_body(resp: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.expect("body");
        serde_json::from_slice(&bytes).expect("json envelope")
    }

    const SINK_BODY: &[u8] = br#"{"query_id":"q","partition_id":"p","push_items":[
        {"queue":"enriched","payload":{"a":1}},
        {"queue":"audit","partition":"shard-1","payload":{"a":2}},
        {"queue":"enriched","payload":{"a":3}},
        {"queue":"audit","partition":"shard-1","payload":{"a":4}},
        {"queue":"enriched","partition":"shard-2","payload":{"a":5}}
    ],"ack":{"transactionId":"t","leaseId":"l","status":"completed","count":9}}"#;

    /// The sink pre-pass of the cycle SP creates queue and partition rows
    /// exactly as push's auto-create does, so the same plan caps have to see
    /// them — one admission per DISTINCT pair, which is the shape the meter's
    /// grouping already produces.
    #[tokio::test]
    async fn cycle_sink_pairs_are_admitted_once_each() {
        let sinks = count_cycle_push_items(SINK_BODY);
        assert_eq!(sinks.total, 5);
        let pairs: Vec<(&str, &str)> = sinks.groups.iter().map(sink_pair).collect();
        assert_eq!(
            pairs,
            vec![("enriched", "Default"), ("audit", "shard-1"), ("enriched", "shard-2")],
            "five items, three admissions"
        );

        // Two queues and two partitions each: everything fits, and a second
        // identical cycle takes the in-process fast path without creating more.
        let reg = crate::registry::Registry::new(None);
        let ctx = cycle_ctx(Some(2), Some(2));
        for _ in 0..2 {
            assert!(admit_pairs(&reg, true, &ctx, sinks.groups.iter().map(sink_pair), "rid")
                .await
                .is_ok());
        }
    }

    /// A cap the sink set does not fit refuses the WHOLE cycle, with the answer
    /// the same items would have got from `/api/v1/push`: 403 `quota_exceeded`.
    #[tokio::test]
    async fn a_sink_over_a_registry_cap_refuses_the_whole_cycle() {
        let sinks = count_cycle_push_items(SINK_BODY);

        // One partition per queue: `enriched`'s second partition is over.
        let reg = crate::registry::Registry::new(None);
        let ctx = cycle_ctx(Some(9), Some(1));
        let refused = admit_pairs(&reg, true, &ctx, sinks.groups.iter().map(sink_pair), "rid")
            .await
            .expect_err("over the partition cap");
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        let body = err_body(refused).await;
        assert_eq!(body["code"], errors::CODE_QUOTA_EXCEEDED);
        assert_eq!(body["error"], "partition limit reached (1)");

        // One queue: the second distinct queue is over.
        let reg = crate::registry::Registry::new(None);
        let ctx = cycle_ctx(Some(1), Some(9));
        let refused = admit_pairs(&reg, true, &ctx, sinks.groups.iter().map(sink_pair), "rid")
            .await
            .expect_err("over the queue cap");
        let body = err_body(refused).await;
        assert_eq!(body["code"], errors::CODE_QUOTA_EXCEEDED);
        assert_eq!(body["error"], "queue limit reached (1)");

        // Shadow mode: the same cycle over the same cap is logged, not refused
        // — the rate/quota posture the whole file shares.
        let reg = crate::registry::Registry::new(None);
        let ctx = cycle_ctx(Some(1), Some(1));
        assert!(admit_pairs(&reg, false, &ctx, sinks.groups.iter().map(sink_pair), "rid")
            .await
            .is_ok());
    }

    /// The dedup lives in `admit_pairs` and serves BOTH callers: extracting it
    /// must not turn a 1000-item push into one queue into 1000 admissions.
    #[tokio::test]
    async fn repeated_produce_pairs_are_still_one_admission() {
        let items: Vec<ItemInfo> = (0..5)
            .map(|_| ItemInfo {
                queue: "orders".to_string(),
                partition: DEFAULT_PARTITION.to_string(),
                payload_len: 1,
            })
            .collect();
        let reg = crate::registry::Registry::new(None);
        // room for exactly one queue and one partition: five copies of one pair
        let ctx = cycle_ctx(Some(1), Some(1));
        assert!(admit_pairs(&reg, true, &ctx, items.iter().map(produce_pair), "rid").await.is_ok());
    }

    /// The per-item payload cap, on the same basis `enforce_produce` measures:
    /// the raw JSON text of the item's payload.
    #[tokio::test]
    async fn a_sink_item_over_the_payload_cap_refuses_the_cycle() {
        let sinks = count_cycle_push_items(
            br#"{"push_items":[
                {"queue":"a","payload":"tiny"},
                {"queue":"b","payload":{"k":"vvvvvvvvvvvvvvvvvvvv"}}
            ]}"#,
        );
        assert_eq!(sinks.heaviest, Some((1, r#"{"k":"vvvvvvvvvvvvvvvvvvvv"}"#.len())));
        assert!(cycle_payload_cap(&sinks, Some(64)).is_ok(), "under the cap");
        assert!(cycle_payload_cap(&sinks, None).is_ok(), "no cap configured");

        let refused = cycle_payload_cap(&sinks, Some(8)).expect_err("over the cap");
        assert_eq!(refused.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = err_body(refused).await;
        assert_eq!(body["code"], errors::CODE_PAYLOAD_TOO_LARGE);
        assert_eq!(body["error"], "push_items[1]: payload 28 bytes exceeds max_payload_bytes (8)");
    }

    #[test]
    fn the_cycle_payload_measure_mirrors_the_broker_field_by_field() {
        // `data` is the broker's accepted alias for `payload` and measures the
        // same; an item with neither is stored as `{}` and can never be over.
        assert_eq!(
            count_cycle_push_items(br#"{"push_items":[{"queue":"a","data":[1,2,3]}]}"#).heaviest,
            Some((0, "[1,2,3]".len()))
        );
        let bare = count_cycle_push_items(br#"{"push_items":[{"queue":"a"}]}"#);
        assert_eq!(bare.heaviest, Some((0, 0)));
        assert!(cycle_payload_cap(&bare, Some(0)).is_ok());

        // The index is the position AS SENT: a skipped item still occupies one,
        // so the number points at the element the client can find.
        let with_skip = count_cycle_push_items(
            br#"{"push_items":[{"payload":"skipped, no queue"},{"queue":"a","payload":"yyyy"}]}"#,
        );
        assert_eq!(with_skip.heaviest, Some((1, r#""yyyy""#.len())));
        // and a skipped item's payload is never measured — the broker discards
        // those bytes, so a cap must not refuse the body over them
        let heavy_skip = count_cycle_push_items(
            br#"{"push_items":[{"payload":"aaaaaaaaaaaaaaaaaaaaaaaaaaaa"},{"queue":"a","payload":1}]}"#,
        );
        assert_eq!(heavy_skip.heaviest, Some((1, 1)));
    }

    /// The §9.6 line, now that refusals exist on this route: a cycle carrying
    /// only an ack and state ops answers NONE of the produce caps, so a tenant
    /// at every one of them can still drain its source.
    #[tokio::test]
    async fn an_ack_only_cycle_answers_none_of_the_produce_caps() {
        let sinks = count_cycle_push_items(
            br#"{"query_id":"q","partition_id":"p","state_ops":[{"op":"delete","key":"k"}],
                "ack":{"transactionId":"t","leaseId":"l","status":"completed","count":10}}"#,
        );
        assert_eq!(sinks.total, 0);
        assert!(sinks.groups.is_empty());
        assert_eq!(sinks.heaviest, None, "nothing to measure");
        assert!(cycle_payload_cap(&sinks, Some(1)).is_ok());

        // caps of zero: no pair to admit, so nothing to refuse
        let reg = crate::registry::Registry::new(None);
        let ctx = cycle_ctx(Some(0), Some(0));
        assert!(admit_pairs(&reg, true, &ctx, sinks.groups.iter().map(sink_pair), "rid")
            .await
            .is_ok());
    }

    /// The cycle bills messages WITHOUT changing its op class: the request
    /// stays on the gated reqs-only `Read` sample and the sink items ride a
    /// second `Push` one. If this ever flips to `Push` here, every cycle would
    /// also be counted as a push REQUEST on top of the messages.
    #[test]
    fn cycle_keeps_the_gated_read_class_for_its_request() {
        use crate::routes::{Feature, GatedOp};
        assert_eq!(
            op_for(STREAMS_CYCLE_PATH, RouteClass::Gated(Feature::Streams, GatedOp::Mixed)),
            OpClass::Read
        );
        // POST-only: `classify` hands every other method on the path the
        // family's `Open` class, so the cycle arm must not take them.
        assert!(is_streams_cycle(&Method::POST, STREAMS_CYCLE_PATH));
        assert!(!is_streams_cycle(&Method::GET, STREAMS_CYCLE_PATH));
        assert!(!is_streams_cycle(&Method::POST, "/streams/v1/queries"));
        assert!(!is_streams_cycle(&Method::POST, "/streams/v1/cycle/extra"));
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
        // PLAN_QUEEN_KAFKA.md C2. The batched fetch is `Consume` for its
        // authorization (routes.rs) but is NOT an `is_pop_path`, so it books
        // reqs-only like ack and lease. Deliberate: `Delivery` would debit the
        // delivery bucket off a count `count_pop_messages` cannot read from the
        // fetch response shape, and a wrong debit 429s the next request.
        assert_eq!(op_for("/api/v1/fetch", RouteClass::Consume), OpClass::Read);
        assert!(!is_pop_path("/api/v1/fetch"));
        // PLAN_S3_SINK.md §5.1/§8. Partition discovery is `Consume` for its
        // authorization and is not a pop path either, so it books reqs-only —
        // the same arm, and deliberately so: its response carries no messages
        // at all, so `Delivery` would debit the bucket off a count that does
        // not exist.
        assert_eq!(
            op_for("/api/v1/partitions/changed", RouteClass::Consume),
            OpClass::Read
        );
        assert!(!is_pop_path("/api/v1/partitions/changed"));
        assert_eq!(
            op_for("/api/v1/configure", RouteClass::QueueAdmin),
            OpClass::Configure
        );
        assert_eq!(
            op_for("/api/v1/resources/queues", RouteClass::Read),
            OpClass::Read
        );
    }

    // ---- EPHEMERAL_QUEUES.md: the RAM family through the data plane ----

    /// Q6, resolved before the family shipped: its two data-plane verbs meter
    /// as messages, not as the reqs-only `Read` every other gated surface
    /// falls into. A regression here is invisible in production until an
    /// invoice is wrong, so it is pinned.
    #[test]
    fn ephemeral_push_and_pop_meter_as_messages() {
        use crate::routes::{Feature, GatedOp};
        assert_eq!(
            op_for(EPH_PUSH_PATH, RouteClass::Gated(Feature::Ephemeral, GatedOp::Grow)),
            OpClass::Push
        );
        assert_eq!(
            op_for(EPH_POP_PATH, RouteClass::Gated(Feature::Ephemeral, GatedOp::Open)),
            OpClass::Delivery
        );
        // the rest of the family carries no messages and keeps the default
        for p in [
            "/api/v1/ephemeral/ack",
            "/api/v1/ephemeral/configure",
            "/api/v1/ephemeral/reset",
            "/api/v1/ephemeral/queue/orders",
        ] {
            assert_eq!(
                op_for(p, RouteClass::Gated(Feature::Ephemeral, GatedOp::Open)),
                OpClass::Read,
                "{p}"
            );
        }
        assert_eq!(
            op_for("/api/v1/ephemeral/queues", RouteClass::Gated(Feature::Ephemeral, GatedOp::Read)),
            OpClass::Read
        );
        // and the neighbouring families are untouched by the new arms
        assert_eq!(
            op_for("/api/v1/kv/ns/k", RouteClass::Gated(Feature::Kv, GatedOp::Grow)),
            OpClass::Read
        );
    }

    /// The pop-metering branch is selected by path, so the RAM pop has to be
    /// on it: that is what counts deliveries out of the response and debits
    /// them. Durable routes must not change class in the process.
    #[test]
    fn ephemeral_pop_is_a_pop_path() {
        assert!(is_pop_path(EPH_POP_PATH));
        assert!(is_pop_path("/api/v1/pop/queue/orders"));
        assert!(!is_pop_path(EPH_PUSH_PATH));
        assert!(!is_pop_path("/api/v1/pop"));
        // its response shape is the durable one, so one counter serves both
        assert_eq!(
            count_pop_messages(br#"{"queue":"inbox","messages":[{"id":"e:1a:p:1"},{"id":"e:1a:p:2"}]}"#),
            Some(2)
        );
        assert_eq!(count_pop_messages(br#"{"queue":"inbox","messages":[]}"#), Some(0));
    }

    /// The ingress count that reaches `limits.check_msgs`. Cheap (elements are
    /// counted, never materialised) and tolerant (an unreadable body is 0, not
    /// a refusal), exactly as `enforce_produce` treats a durable batch.
    #[test]
    fn ephemeral_push_items_are_counted_from_the_body() {
        let body = br#"{"queue":"inbox","partition":"c-42","messages":[
            {"payload":{"a":1}},{"payload":"hello"},{"payload":[1,2,3]}
        ]}"#;
        assert_eq!(count_ephemeral_push_items(body), 3);
        assert_eq!(count_ephemeral_push_items(br#"{"queue":"inbox","messages":[]}"#), 0);
        // no partition is the common shape (the broker defaults it)
        assert_eq!(count_ephemeral_push_items(br#"{"queue":"i","messages":[{"payload":1}]}"#), 1);
        // unreadable / wrong shape -> 0, and the broker answers its own 400
        assert_eq!(count_ephemeral_push_items(b"not json"), 0);
        assert_eq!(count_ephemeral_push_items(br#"{"queue":"inbox"}"#), 0);
        assert_eq!(count_ephemeral_push_items(br#"[{"payload":1}]"#), 0);
    }

    /// The 201 the family answers is one number, not a status array (§3.1).
    #[test]
    fn ephemeral_push_201_is_counted_from_pushed() {
        let counts = count_ephemeral_pushed(br#"{"pushed":3}"#).expect("parse");
        assert_eq!(counts.accepted, 3);
        assert_eq!(counts.total, 3);
        // nothing spools in RAM, so the maintenance signal can never fire here
        assert_eq!(counts.buffered, 0);
        assert!(!predominantly_buffered(&counts));
        // a shape we cannot read charges nothing, like a durable push 201
        assert_eq!(count_ephemeral_pushed(br#"{"error":"queue_full"}"#), None);
        assert_eq!(count_ephemeral_pushed(b"not json"), None);
        // and the durable counter is not fooled by it either way round
        assert_eq!(count_push_statuses(br#"{"pushed":3}"#), None);
    }

    /// The plan gate the 403 `feature_gated` is read from. A `Features` a plan
    /// row never mentioned is all-false, so a cell that has never heard of the
    /// RAM class denies it — the §8 "cloud off until the plan says so" posture.
    #[test]
    fn ephemeral_is_denied_until_the_plan_grants_it() {
        use crate::routes::Feature;
        let none = crate::state::Features::default();
        assert!(!feature_enabled(Feature::Ephemeral, &none));

        let granted = crate::state::Features { ephemeral: true, ..Default::default() };
        assert!(feature_enabled(Feature::Ephemeral, &granted));
        // one flag, one family: granting the RAM class grants nothing else
        assert!(!feature_enabled(Feature::Kv, &granted));
        assert!(!feature_enabled(Feature::Timers, &granted));
        assert!(!feature_enabled(Feature::Streams, &granted));
        assert!(!feature_enabled(Feature::Traces, &granted));
        // ...and no other flag turns it on
        let kv_only = crate::state::Features { kv: true, ..Default::default() };
        assert!(!feature_enabled(Feature::Ephemeral, &kv_only));
    }

    // ---- the Kafka facade's kv override (kafka_kv.rs), at the five gates ----
    //
    // The sniffer's own behaviour is pinned in `kafka_kv.rs`. These tests are
    // about what the RECLASSIFICATION does once it has happened, which is a
    // property of this file: the gates are `plan_gates` (feature + push block),
    // `auth::authorize`, the `mixed_block` verdict, and `op_for`. Each is
    // exercised through the same function `handle` calls, with the class the
    // same `effective_class` would have produced.

    /// A committed offset is the facade's internal bookkeeping, not the KV
    /// PRODUCT, so a tenant whose plan never mentions `kv` must still be able
    /// to run a Kafka consumer. Before the override the feature gate answered
    /// 403 `feature_gated` and the client could not commit at all.
    #[test]
    fn a_qk_batch_reaches_a_tenant_whose_plan_has_no_kv() {
        use crate::routes::{classify, Feature};
        let kv_batch = classify(&Method::POST, "/api/v1/kv");
        let qk = br#"{"operations":[{"op":"put","ns":"queen-kafka","key":"qk:group:g:orders:0","value":{"offset":7},"forever":true}]}"#;
        let class = crate::kafka_kv::effective_class(kv_batch, qk);
        assert_eq!(class, RouteClass::Consume);

        // The feature gate only ever fires on a `Gated` class, and this is no
        // longer one — so a plan with `kv` false is not consulted.
        let no_kv = crate::state::Features::default();
        assert!(!feature_enabled(Feature::Kv, &no_kv));
        assert!(
            !matches!(class, RouteClass::Gated(_, _)),
            "a Gated class would still meet the plan flag"
        );
        // and a consume-scoped key — the credential a Kafka consumer holds —
        // may now call it, where `Gated(_,_)` demanded produce-or-consume and
        // the plan flag on top
        let consumer = crate::state::Principal::ApiKey {
            key_id: uuid::Uuid::nil(),
            scopes: crate::state::Scopes {
                consume: true,
                read: true,
                produce: false,
                admin: false,
            },
        };
        assert!(crate::auth::authorize(&consumer, class).is_ok());
    }

    /// THE read-strands-consumers trap, on the offsets. A tenant over its
    /// storage quota is push-blocked on purpose; refusing its OffsetFetch and
    /// its OffsetCommit as well would pin every consumer at an offset it can
    /// never move past, while the backlog it would drain keeps growing — the
    /// opposite of what the block is for. `mixed_block` is computed for the
    /// CLASS, and `Consume` is not a class it is computed for.
    #[test]
    fn a_blocked_tenant_can_still_read_and_commit_offsets() {
        use crate::routes::classify;
        let kv_batch = classify(&Method::POST, "/api/v1/kv");
        // an OffsetFetch (getMany) and an OffsetCommit (put), the two halves
        for body in [
            br#"{"operations":[{"op":"getMany","ns":"queen-kafka","keys":["qk:group:g:orders:0"]}]}"#.as_slice(),
            br#"{"operations":[{"op":"put","ns":"queen-kafka","key":"qk:group:g:orders:0","value":{"offset":7}}]}"#,
        ] {
            let class = crate::kafka_kv::effective_class(kv_batch, body);
            assert_eq!(class, RouteClass::Consume);
            // `handle` computes `mixed_block` with exactly this match; a class
            // outside it is never offered the push block at all, blocked
            // cluster or not.
            let would_block = matches!(class, RouteClass::Gated(_, crate::routes::GatedOp::Mixed));
            assert!(!would_block, "{}", String::from_utf8_lossy(body));
        }
    }

    /// The regression guard. A non-`qk:` batch is the KV product and keeps
    /// `Gated(Kv, Mixed)` in every one of the five gates — same feature flag,
    /// same authorize arm, same `mixed_block` computation, same metering.
    #[test]
    fn a_plain_kv_batch_is_still_gated_exactly_as_before() {
        use crate::routes::{classify, Feature, GatedOp};
        let kv_batch = classify(&Method::POST, "/api/v1/kv");
        for body in [
            br#"{"operations":[{"op":"put","ns":"app","key":"cache:x","value":1}]}"#.as_slice(),
            br#"{"operations":[{"op":"getMany","ns":"app","keys":["cache:x"]}]}"#,
            // ...including one that only LOOKS like the facade's
            br#"{"operations":[{"op":"put","ns":"app","key":"qk:group:g:orders:0","value":1}]}"#,
            // ...and a mixed batch, which must fail closed onto the old class
            br#"{"operations":[{"op":"put","ns":"queen-kafka","key":"qk:group:g:orders:0","value":1},{"op":"put","ns":"app","key":"cache:x","value":1}]}"#,
        ] {
            let class = crate::kafka_kv::effective_class(kv_batch, body);
            let why = String::from_utf8_lossy(body);
            // 1. same class
            assert_eq!(class, RouteClass::Gated(Feature::Kv, GatedOp::Mixed), "{why}");
            // 2. the plan flag is consulted, and a plan without kv denies
            assert!(!feature_enabled(Feature::Kv, &crate::state::Features::default()), "{why}");
            // 3. authorize keeps the produce-or-consume arm: a read-only key is
            //    still refused, where `Consume` would have been asked for
            //    `scopes.consume` alone
            let read_only = crate::state::Principal::ApiKey {
                key_id: uuid::Uuid::nil(),
                scopes: crate::state::Scopes {
                    read: true,
                    produce: false,
                    consume: false,
                    admin: false,
                },
            };
            assert!(crate::auth::authorize(&read_only, class).is_err(), "{why}");
            // 4. `mixed_block` is still computed for it, so a blocked cluster
            //    still refuses the growing half
            assert!(
                matches!(class, RouteClass::Gated(_, GatedOp::Mixed)),
                "{why}"
            );
            // 5. metering unchanged
            assert_eq!(op_for("/api/v1/kv", class), OpClass::Read, "{why}");
        }
    }

    /// No bill moves because of a classification. `Gated(_,_)` meters reqs-only
    /// `Read`, and a `Consume` that is not a pop path meters reqs-only `Read`
    /// too — the same bucket, the same zero messages.
    #[test]
    fn a_qk_batch_meters_the_same_as_before() {
        use crate::routes::classify;
        let before = classify(&Method::POST, "/api/v1/kv");
        let after = crate::kafka_kv::effective_class(
            before,
            br#"{"operations":[{"op":"getPrefix","ns":"queen-kafka","prefix":"qk:groups:","limit":500}]}"#,
        );
        assert_ne!(before, after, "the class really did change");
        assert_eq!(op_for("/api/v1/kv", before), OpClass::Read);
        assert_eq!(op_for("/api/v1/kv", after), OpClass::Read);
        // the batch route is not a pop path, so the reclassified `Consume`
        // cannot fall into the delivery bucket `debit_deliveries` drives
        assert!(!is_pop_path("/api/v1/kv"));
        // ...and it takes no parked slot either: the long-poll gauge reads the
        // query, and this route has none of it
        assert!(!is_wait_pop("/api/v1/kv", Some("wait=true&timeout=30000")));
    }

    /// The override is scoped to the one class that asks its body. Nothing
    /// else in the file may be moved by a body that happens to look like the
    /// facade's — a push, a timer batch, a streams cycle.
    #[test]
    fn the_override_touches_no_other_route() {
        use crate::routes::classify;
        let qk = br#"{"operations":[{"op":"put","ns":"queen-kafka","key":"qk:group:g:orders:0","value":{"offset":1}}]}"#;
        for (m, p) in [
            (Method::POST, "/api/v1/push"),
            (Method::POST, "/api/v1/transaction"),
            (Method::POST, "/api/v1/timers"),
            (Method::POST, "/streams/v1/cycle"),
            (Method::GET, "/api/v1/kv/queen-kafka/qk:group:g:orders:0"),
            (Method::PUT, "/api/v1/kv/queen-kafka/qk:group:g:orders:0"),
            (Method::DELETE, "/api/v1/kv/queen-kafka/qk:group:g:orders:0"),
            (Method::POST, "/api/v1/fetch"),
            (Method::POST, "/api/v1/partitions/changed"),
        ] {
            let before = classify(&m, p);
            assert_eq!(
                crate::kafka_kv::effective_class(before, qk),
                before,
                "{m} {p}"
            );
        }
    }

    // ---- the S3 sink's kv override (s3_kv.rs), at the same gates ----
    //
    // The sniffer's own behaviour is pinned in `s3_kv.rs`. These are about what
    // the RECLASSIFICATION does once it has happened, which is this file's
    // property, and they are the sink's half of the two tests above.

    /// THE trap this carve-out exists for, and it is sharper than the Kafka
    /// one. A tenant over its storage quota is push-blocked on purpose; if the
    /// same block refuses the sink's WINDOW COMMIT, the sink re-uploads and
    /// re-commits the identical window for ever while its lag grows without
    /// bound — and a commit pointer of a few hundred bytes is not what the
    /// storage block exists to stop. `mixed_block` is computed for the CLASS,
    /// and `Consume` is not a class it is computed for.
    #[test]
    fn a_blocked_tenant_can_still_commit_a_window() {
        use crate::routes::classify;
        let kv_batch = classify(&Method::POST, "/api/v1/kv");
        for body in [
            // the commit: a CAS put with a required precondition
            br#"{"operations":[{"op":"put","ns":"queen-s3","key":"s3:commit:sink-a:orders","value":{"tEnd":"2026-09-04T10:00:00Z"},"expect":41,"required":true,"forever":true}]}"#.as_slice(),
            // the recovery read that precedes it
            br#"{"operations":[{"op":"getMany","ns":"queen-s3","keys":["s3:commit:sink-a:orders"]}]}"#,
        ] {
            let class = crate::s3_kv::effective_class(kv_batch, body);
            assert_eq!(class, RouteClass::Consume);
            let would_block = matches!(class, RouteClass::Gated(_, crate::routes::GatedOp::Mixed));
            assert!(!would_block, "{}", String::from_utf8_lossy(body));
        }
    }

    /// The sink is a CONSUMER, so a consume-scoped credential — the one it
    /// holds for the fetch and the discovery call — is enough for its
    /// bookkeeping too, and a plan that never mentions `kv` does not gate it.
    #[test]
    fn a_sink_batch_reaches_a_tenant_whose_plan_has_no_kv() {
        use crate::routes::{classify, Feature};
        let kv_batch = classify(&Method::POST, "/api/v1/kv");
        let body = br#"{"operations":[{"op":"putIfAbsent","ns":"queen-s3","key":"s3:instance:sink-a","value":{},"ttlSeconds":60}]}"#;
        let class = crate::s3_kv::effective_class(kv_batch, body);
        assert_eq!(class, RouteClass::Consume);
        assert!(!feature_enabled(
            Feature::Kv,
            &crate::state::Features::default()
        ));
        assert!(
            !matches!(class, RouteClass::Gated(_, _)),
            "a Gated class would still meet the plan flag"
        );
        let consumer = crate::state::Principal::ApiKey {
            key_id: uuid::Uuid::nil(),
            scopes: crate::state::Scopes {
                consume: true,
                read: true,
                produce: false,
                admin: false,
            },
        };
        assert!(crate::auth::authorize(&consumer, class).is_ok());
    }

    /// No bill moves because of a classification, same as the Kafka override:
    /// `Gated(_,_)` meters reqs-only `Read`, and a `Consume` that is not a pop
    /// path meters reqs-only `Read` too.
    #[test]
    fn a_sink_batch_meters_the_same_as_before() {
        use crate::routes::classify;
        let before = classify(&Method::POST, "/api/v1/kv");
        let after = crate::s3_kv::effective_class(
            before,
            br#"{"operations":[{"op":"getPrefix","ns":"queen-s3","prefix":"s3:instance:","limit":500}]}"#,
        );
        assert_ne!(before, after, "the class really did change");
        assert_eq!(op_for("/api/v1/kv", before), OpClass::Read);
        assert_eq!(op_for("/api/v1/kv", after), OpClass::Read);
        assert!(!is_pop_path("/api/v1/kv"));
        assert!(!is_wait_pop("/api/v1/kv", Some("wait=true&timeout=30000")));
    }

    /// A non-`s3:` batch is the KV product and keeps `Gated(Kv, Mixed)` in
    /// every gate — and the two carve-outs stay disjoint, so chaining them in
    /// `handle` cannot grant a class neither would grant alone.
    #[test]
    fn a_plain_kv_batch_survives_both_carve_outs_unchanged() {
        use crate::routes::{classify, Feature, GatedOp};
        let kv_batch = classify(&Method::POST, "/api/v1/kv");
        for body in [
            br#"{"operations":[{"op":"put","ns":"app","key":"cache:x","value":1}]}"#.as_slice(),
            // ...one that only LOOKS like the sink's
            br#"{"operations":[{"op":"put","ns":"app","key":"s3:commit:a","value":1}]}"#,
            // ...and one that mixes the two reserved spaces, which must fail
            // closed onto the old class whichever predicate sees it first
            br#"{"operations":[{"op":"put","ns":"queen-s3","key":"s3:commit:a","value":1},{"op":"put","ns":"queen-kafka","key":"qk:group:g","value":1}]}"#,
        ] {
            let why = String::from_utf8_lossy(body);
            // the chain `handle` runs, in the order it runs it
            let class = crate::s3_kv::effective_class(
                crate::kafka_kv::effective_class(kv_batch, body),
                body,
            );
            assert_eq!(
                class,
                RouteClass::Gated(Feature::Kv, GatedOp::Mixed),
                "{why}"
            );
            assert!(
                !feature_enabled(Feature::Kv, &crate::state::Features::default()),
                "{why}"
            );
            let read_only = crate::state::Principal::ApiKey {
                key_id: uuid::Uuid::nil(),
                scopes: crate::state::Scopes {
                    read: true,
                    produce: false,
                    consume: false,
                    admin: false,
                },
            };
            assert!(crate::auth::authorize(&read_only, class).is_err(), "{why}");
            assert!(matches!(class, RouteClass::Gated(_, GatedOp::Mixed)), "{why}");
            assert_eq!(op_for("/api/v1/kv", class), OpClass::Read, "{why}");
        }
    }

    /// The sink override is scoped to the one class that asks its body, the
    /// discovery route it serves included.
    #[test]
    fn the_sink_override_touches_no_other_route() {
        use crate::routes::classify;
        let s3 = br#"{"operations":[{"op":"put","ns":"queen-s3","key":"s3:commit:a","value":1}]}"#;
        for (m, p) in [
            (Method::POST, "/api/v1/push"),
            (Method::POST, "/api/v1/transaction"),
            (Method::POST, "/api/v1/timers"),
            (Method::POST, "/streams/v1/cycle"),
            (Method::GET, "/api/v1/kv/queen-s3/s3:commit:a"),
            (Method::PUT, "/api/v1/kv/queen-s3/s3:commit:a"),
            (Method::DELETE, "/api/v1/kv/queen-s3/s3:commit:a"),
            (Method::POST, "/api/v1/fetch"),
            (Method::POST, "/api/v1/partitions/changed"),
        ] {
            let before = classify(&m, p);
            assert_eq!(crate::s3_kv::effective_class(before, s3), before, "{m} {p}");
        }
    }

    // ---- PLAN_DASHBOARD_ACTIONS.md §1.4/§2.0: the DLQ replay gates --------
    //
    // The arms themselves are pinned in `routes.rs`. These are about what the
    // classification DOES once it has happened, which is this file's property:
    // `auth::authorize` (who may call it), `plan_gates` (whether a blocked
    // cluster may) and the meter (what it costs). Each is exercised through the
    // same function `handle` calls.

    const RETRY_PATH: &str = "/api/v1/messages/P/T/retry";
    const REPLAY_PATH: &str = "/api/v1/dlq/3f8a9c1e-0000-4000-8000-000000000000/replay";

    fn user(role: crate::state::Role) -> crate::state::Principal {
        crate::state::Principal::User { user_id: uuid::Uuid::nil(), role, operator: false }
    }

    fn key(scopes: crate::state::Scopes) -> crate::state::Principal {
        crate::state::Principal::ApiKey { key_id: uuid::Uuid::nil(), scopes }
    }

    /// A whole `St` with no pxdb behind it — acting.rs's `st_with` twin, copied
    /// rather than shared because that one lives inside its own test module.
    /// Nothing here reaches the network: `plan_gates` asks `st.limits` and
    /// nothing else.
    fn gate_st() -> St {
        gate_st_with(false)
    }

    /// The same, with the limits ENFORCING rather than shadowing: a registry
    /// refusal is a 403 only when `st.limits.enforcing()`, and the admission
    /// half of the replay gate has to be pinned on both sides of that flag.
    fn gate_st_enforcing() -> St {
        gate_st_with(true)
    }

    fn gate_st_with(enforce: bool) -> St {
        let mut cfg = crate::config::test_config(&[]);
        cfg.enforce = enforce;
        let cache = crate::cache::ClusterCache::new(&cfg, None);
        let limits = crate::limits::Limits::new(&cfg);
        let meter = std::sync::Arc::new(crate::meter::Meter::new(&cfg));
        let registry = crate::registry::Registry::new(None);
        let keys = crate::auth::Keys::from_config(&cfg);
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.set_nodelay(true);
        let upstream =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build::<_, axum::body::Body>(connector);
        std::sync::Arc::new(crate::state::AppState {
            cfg,
            db: None,
            upstream,
            cache,
            limits,
            meter,
            registry,
            keys,
        })
    }

    /// THE finding (§1.4). Through the proxy a Viewer session — or a key with
    /// nothing but the `read` scope — could replay a dead letter, because the
    /// route fell through the `/api/v1/messages` reads prefix and `Read` is
    /// true for every user role and for `scopes.read`. `QueueAdmin` is the
    /// authority the irreversible half needs: Admin, or `scopes.admin`.
    #[test]
    fn a_viewer_cannot_replay_a_dead_letter() {
        let viewer = user(crate::state::Role::Viewer);
        let admin = user(crate::state::Role::Admin);
        let read_only = key(crate::state::Scopes {
            read: true,
            produce: false,
            consume: false,
            admin: false,
        });
        let admin_key = key(crate::state::Scopes {
            read: false,
            produce: false,
            consume: false,
            admin: true,
        });
        for p in [RETRY_PATH, REPLAY_PATH] {
            let class = classify(&Method::POST, p);
            assert_eq!(class, RouteClass::QueueAdmin, "{p}");
            let refused = crate::auth::authorize(&viewer, class).expect_err("a viewer may not replay");
            assert_eq!(refused.status(), StatusCode::FORBIDDEN, "{p}");
            assert!(crate::auth::authorize(&read_only, class).is_err(), "read-scoped key {p}");
            // ...and the credentials that own the act still hold it: the fix
            // must not close the route on the role the dashboard button is for.
            assert!(crate::auth::authorize(&admin, class).is_ok(), "admin {p}");
            assert!(crate::auth::authorize(&admin_key, class).is_ok(), "admin key {p}");
        }
        // The reads on the same prefix are untouched — a Viewer still sees the
        // dead letters it may no longer replay, which is the whole point of the
        // class split.
        assert!(crate::auth::authorize(&viewer, classify(&Method::GET, "/api/v1/dlq")).is_ok());
        assert!(crate::auth::authorize(&viewer, classify(&Method::GET, "/api/v1/messages/P/T")).is_ok());
    }

    /// The second half of the finding, and the reason `is_dlq_replay` exists: a
    /// replay writes a frame, so a cluster that is blocked from pushing must be
    /// refused one. `plan_gates` reads the CLASS for the push blocks, and
    /// `QueueAdmin` is not a class it reads — without the predicate the replay
    /// sails past a gate that was set to stop exactly this.
    #[tokio::test]
    async fn a_storage_blocked_tenant_cannot_replay_but_can_still_delete() {
        let st = gate_st();
        let mut ctx = cycle_ctx(None, None);
        ctx.limits.max_retained_bytes = Some(1_000);
        // The in-flight estimate the produce path keeps, over the cap. HARD,
        // like the pump-set flag it backstops: `test_config` is shadow mode and
        // a storage block does not care.
        st.limits.note_accepted_bytes(ctx.cluster_id, 2_000);

        for p in [RETRY_PATH, REPLAY_PATH] {
            let refused = plan_gates(&st, &ctx, classify(&Method::POST, p), &Method::POST, p)
                .expect("a replay into a storage-blocked cluster is refused");
            assert_eq!(refused.status(), StatusCode::FORBIDDEN, "{p}");
            let body = err_body(refused).await;
            // The same code the equivalent push gets: one block, one answer, and
            // a Track C client that already treats it as terminal.
            assert_eq!(body["code"], errors::CODE_STORAGE_QUOTA, "{p}");
        }
        assert!(
            plan_gates(&st, &ctx, RouteClass::Produce, &Method::POST, "/api/v1/push").is_some(),
            "the push the replay is being held to"
        );

        // ...and PLAN_KV_TIMERS.md §9.5's rule holds: everything a blocked
        // tenant needs to get back under its cap still passes. Purging dead
        // letters, deleting a message, dropping a queue, shortening retention
        // and every read are how the block is ESCAPED; refusing them would lock
        // the tenant inside it.
        for (m, p) in [
            (Method::DELETE, "/api/v1/dlq"),
            (Method::DELETE, "/api/v1/messages/P/T"),
            (Method::DELETE, "/api/v1/resources/queues/orders"),
            (Method::POST, "/api/v1/configure"),
            (Method::GET, "/api/v1/dlq"),
            (Method::GET, "/api/v1/messages"),
        ] {
            let class = classify(&m, p);
            assert!(plan_gates(&st, &ctx, class, &m, p).is_none(), "{m} {p}");
        }

        // The billing hold is the third cause and answers its own code, so an
        // operator reading a 403 can tell a full disk from an unpaid invoice.
        let st = gate_st();
        let mut held = cycle_ctx(None, None);
        held.status = ClusterStatus::PushBlocked;
        for p in [RETRY_PATH, REPLAY_PATH] {
            let refused = plan_gates(&st, &held, classify(&Method::POST, p), &Method::POST, p)
                .expect("a replay into a cluster on billing hold is refused");
            let body = err_body(refused).await;
            assert_eq!(body["code"], errors::CODE_PUSH_BLOCKED, "{p}");
        }
    }

    /// The healthy path, pinned so a future gate cannot quietly take the button
    /// away: an Admin acting on a cluster with nothing blocking replays.
    #[test]
    fn an_admin_on_a_healthy_tenant_replays() {
        let st = gate_st();
        let ctx = cycle_ctx(None, None);
        let admin = user(crate::state::Role::Admin);
        for p in [RETRY_PATH, REPLAY_PATH] {
            let class = classify(&Method::POST, p);
            assert!(crate::auth::authorize(&admin, class).is_ok(), "{p}");
            assert!(
                plan_gates(&st, &ctx, class, &Method::POST, p).is_none(),
                "nothing is blocking, so nothing may refuse {p}"
            );
        }
    }

    /// D4: a replay is billed as a push of ONE message. Its class does not move
    /// — the REQUEST is a queue-admin act and is booked in `Configure` — and
    /// the message rides the second sample `handle` records off the verdict.
    #[test]
    fn a_replay_bills_one_message_without_changing_its_class() {
        assert_eq!(op_for(RETRY_PATH, RouteClass::QueueAdmin), OpClass::Configure);
        assert_eq!(op_for(REPLAY_PATH, RouteClass::QueueAdmin), OpClass::Configure);

        // The move primitive's two verdicts (§2.3): `moved` wrote a frame,
        // `duplicate` found the destination's dedup window already holding the
        // deterministic id and wrote nothing.
        assert_eq!(
            replay_stored_a_message(br#"{"success":true,"result":"moved","queue":"orders"}"#),
            Some(true)
        );
        assert_eq!(
            replay_stored_a_message(br#"{"success":true,"result":"duplicate","queue":"orders"}"#),
            Some(false)
        );
        // A broker older than the move primitive (1.5.3 and back, which this
        // proxy ships in front of: W0 is 1.5.4 and M0 lands alone). Its
        // address-keyed retry answers `success:true` with no verdict field, and
        // only once its push has been accepted, so an absent field is a write.
        // Both routes on a current broker answer through `move_response` and
        // always carry `result` — this arm is the compatibility one, and
        // deleting it would un-bill every replay served by an older broker.
        assert_eq!(
            replay_stored_a_message(
                br#"{"success":true,"queue":"orders","partition":"Default",
                     "replayedAs":{"transaction_id":"dlq:1"},"dlqRowRemoved":true}"#
            ),
            Some(true)
        );
        // Charge nothing we cannot confirm: the "push rejected, still
        // dead-lettered" answer, a verdict word this proxy has not been told
        // about, and every unreadable shape.
        assert_eq!(
            replay_stored_a_message(br#"{"success":false,"error":"push rejected"}"#),
            Some(false)
        );
        assert_eq!(
            replay_stored_a_message(br#"{"success":true,"result":"parked"}"#),
            Some(false)
        );
        assert_eq!(replay_stored_a_message(b"not json"), None);
        assert_eq!(replay_stored_a_message(br#"{"queue":"orders"}"#), None);
        assert_eq!(replay_stored_a_message(b""), None);

        // The predicate that arms all of it is method- and SHAPE-exact, so the
        // neighbours on both prefixes keep their own answers — including the
        // message whose transaction id is literally `retry` and the dead-letter
        // row whose id is `replay`, which a suffix test would gate, buffer and
        // admit as routes that do not exist.
        assert!(is_dlq_replay(&Method::POST, RETRY_PATH));
        assert!(is_dlq_replay(&Method::POST, REPLAY_PATH));
        assert!(!is_dlq_replay(&Method::GET, RETRY_PATH));
        assert!(!is_dlq_replay(&Method::DELETE, "/api/v1/messages/P/T"));
        assert!(!is_dlq_replay(&Method::DELETE, "/api/v1/dlq"));
        assert!(!is_dlq_replay(&Method::POST, "/api/v1/push"));
        assert!(!is_dlq_replay(&Method::POST, "/api/v1/messages/P/retry"));
        assert!(!is_dlq_replay(&Method::POST, "/api/v1/messages/P/T/U/retry"));
        assert!(!is_dlq_replay(&Method::POST, "/api/v1/messages/P/T/retry/"));
        assert!(!is_dlq_replay(&Method::POST, "/api/v1/dlq/replay"));
        // `is_dlq_replay` and `classify` answer the same set, which is the
        // property that keeps the gate and the class from drifting apart: a
        // path either is this route (QueueAdmin, gated, admitted, billed) or it
        // is not this route in both of them.
        for p in [
            RETRY_PATH,
            REPLAY_PATH,
            "/api/v1/messages/P/retry",
            "/api/v1/messages/P/T/U/retry",
            "/api/v1/messages/P/T/retry/",
            "/api/v1/dlq/replay",
            "/api/v1/push",
        ] {
            assert_eq!(
                is_dlq_replay(&Method::POST, p),
                classify(&Method::POST, p) == RouteClass::QueueAdmin
                    && (p.ends_with("/retry") || p.ends_with("/replay")),
                "{p}"
            );
        }
        // ...and a replay is not a pop, so it neither debits the delivery
        // bucket nor holds a parked slot.
        assert!(!is_pop_path(RETRY_PATH));
        assert!(!is_wait_pop(RETRY_PATH, Some("wait=true&timeout=30000")));
    }

    /// D4 as a VALUE, which is the half the arm in `handle` could not be tested
    /// for: the two `record` calls were side effects on a live meter, so the
    /// whole block could be deleted with every test still green. The billing is
    /// the decision — under-count and the revenue is gone, over-count and a
    /// monthly quota trips on messages nobody wrote — so it is asserted here.
    #[test]
    fn a_stored_replay_books_one_request_and_exactly_one_message() {
        let cluster = uuid::Uuid::from_u128(42);

        let stored = replay_samples(cluster, OpClass::Configure, 24, 310, true);
        assert_eq!(stored.len(), 2, "the request and the message it wrote");
        // The request, on the route's own class: the queue-admin bucket every
        // other DLQ action lands in, carrying both byte counts.
        assert_eq!(stored[0].op, OpClass::Configure);
        assert_eq!((stored[0].reqs, stored[0].msgs), (1, 0));
        assert_eq!((stored[0].bytes_in, stored[0].bytes_out), (24, 310));
        // The message, on `Push`: a replayed frame is stored, retained and
        // rolled up exactly like a pushed one, so it lands in the `usage_days`
        // row a push lands in and in the monthly total the quota block reads.
        assert_eq!(stored[1].op, OpClass::Push);
        assert_eq!((stored[1].reqs, stored[1].msgs), (0, 1));
        assert_eq!((stored[1].bytes_in, stored[1].bytes_out), (0, 0));

        // A `duplicate`, a refusal, a verdict we do not know, a body we could
        // not read: the request is still one request, and nothing was written.
        let unstored = replay_samples(cluster, OpClass::Configure, 24, 310, false);
        assert_eq!(unstored.len(), 1);
        assert_eq!((unstored[0].reqs, unstored[0].msgs), (1, 0));

        // The invariant both shapes share, and the one an over-count breaks:
        // ONE HTTP request is booked once, whatever the verdict.
        for samples in [&stored, &unstored] {
            assert_eq!(samples.iter().map(|s| s.reqs).sum::<u64>(), 1);
            assert!(samples.iter().map(|s| s.msgs).sum::<u64>() <= 1);
            assert!(samples.iter().all(|s| s.cluster_id == cluster));
        }
    }

    // ---- D4 through `handle` ------------------------------------------------
    //
    // The test above pins `replay_samples` as a VALUE, which leaves the half
    // that value exists for untested: the arm in `handle` that calls it is two
    // `record` side effects on a live meter, and deleting the whole block left
    // every test in this file green while every replay on the cell silently
    // stopped billing its message. So the test below drives the real pipeline —
    // classify, gate, authorize, admit, forward, meter — against a stub broker.
    //
    // Reading the samples back is the awkward part and the reason this is the
    // only test here shaped like an integration test: a `Meter` has exactly one
    // exit that does not need a pxdb, its disk spool, which is where a flush
    // against an unreachable database puts the rows. So the meter is given a
    // pool nothing listens behind, and the failure is the readout.

    /// A stub broker on a loopback port, answering every request with one
    /// `move_response`-shaped verdict chosen by the dead-letter row id in the
    /// path — which is the only thing the two requests below differ by.
    async fn stub_broker() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let app = axum::Router::new().fallback(|req: Request| async move {
            let verdict =
                if req.uri().path().contains("/moved-row/") { "moved" } else { "duplicate" };
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"success":true,"result":"{verdict}","queue":"orders","partition":"Default"}}"#
                )))
                .expect("stub response")
        });
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), task)
    }

    /// An `St` wired end to end at a stub broker: dev-static names the cluster
    /// (so `resolve_host` answers without a pxdb) and dev-insecure hands the
    /// request a full-scope principal (so the test is about steps 4-6, not
    /// about minting a credential). The flush interval is pushed out of the
    /// way — the rows this test reads are the ones `drain()` writes.
    fn replay_st(cell_url: &str, spool_dir: &str) -> St {
        let mut cfg = crate::config::test_config(&[]);
        cfg.dev_insecure = true;
        cfg.dev_static = Some(crate::config::DevStaticCluster {
            cell_url: cell_url.to_string(),
            cell_token: None,
            broker_tenant: crate::config::DEFAULT_TENANT_UUID.to_string(),
        });
        cfg.spool_dir = spool_dir.to_string();
        cfg.meter_flush_ms = 600_000;
        let cache = crate::cache::ClusterCache::new(&cfg, None);
        let limits = crate::limits::Limits::new(&cfg);
        let meter = std::sync::Arc::new(crate::meter::Meter::new(&cfg));
        let registry = crate::registry::Registry::new(None);
        let keys = crate::auth::Keys::from_config(&cfg);
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.set_nodelay(true);
        let upstream =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build::<_, axum::body::Body>(connector);
        std::sync::Arc::new(crate::state::AppState {
            cfg,
            db: None,
            upstream,
            cache,
            limits,
            meter,
            registry,
            keys,
        })
    }

    /// A pool nothing listens behind (registry.rs's `unreachable_pool` twin,
    /// copied for the same reason `gate_st` is): every flush through it fails,
    /// and a failed flush spools its rows to disk, which is this test's readout.
    fn unreachable_pool() -> deadpool_postgres::Pool {
        let mut pg = tokio_postgres::Config::new();
        pg.host("127.0.0.1")
            .port(1)
            .user("x")
            .dbname("x")
            .connect_timeout(std::time::Duration::from_secs(2));
        let mgr = deadpool_postgres::Manager::from_config(
            pg,
            tokio_postgres::NoTls,
            deadpool_postgres::ManagerConfig {
                recycling_method: deadpool_postgres::RecyclingMethod::Fast,
            },
        );
        deadpool_postgres::Pool::builder(mgr)
            .max_size(1)
            .runtime(deadpool_postgres::Runtime::Tokio1)
            .wait_timeout(Some(std::time::Duration::from_secs(2)))
            .create_timeout(Some(std::time::Duration::from_secs(2)))
            .build()
            .expect("pool")
    }

    fn spooled_rows(dir: &str) -> Vec<crate::meter::UsageRow> {
        let mut rows = Vec::new();
        for entry in std::fs::read_dir(dir).expect("spool dir").flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("buf") {
                continue;
            }
            let text = std::fs::read_to_string(&path).expect("spool file");
            for line in text.lines().filter(|l| !l.trim().is_empty()) {
                rows.push(serde_json::from_str(line).expect("usage row"));
            }
        }
        rows
    }

    /// D4, through `handle`: the request is booked once per call and the
    /// MESSAGE only where the broker's answer says a frame was written. The two
    /// calls differ by nothing but the dead-letter row id — same route, same
    /// body, same everything — so the only thing that can move the second
    /// sample is the verdict, which is the decision this arm exists to make.
    #[tokio::test]
    async fn the_replay_arm_bills_a_moved_message_and_not_a_duplicate() {
        let dir = std::env::temp_dir().join(format!("queen-proxy-replay-{}", uuid::Uuid::new_v4()));
        let dir = dir.to_str().expect("temp path").to_string();
        let (cell, _broker) = stub_broker().await;
        let st = replay_st(&cell, &dir);
        // Sets the pool `drain()` writes through. Unreachable on purpose.
        st.meter.spawn_flush(Some(unreachable_pool()));

        for id in ["moved-row", "duplicate-row"] {
            let resp = handle(
                State(st.clone()),
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/api/v1/dlq/{id}/replay"))
                    .header(header::HOST, "cell.test")
                    // `{}` names no destination, so nothing is admitted and the
                    // request reaches the broker exactly as the dashboard's
                    // plain "Replay" button sends it.
                    .body(Body::from("{}"))
                    .expect("request"),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::OK, "{id}");
            // The body is returned to the caller as well as read for the
            // verdict: the arm BUFFERS the response, and a buffer that forgets
            // to hand the bytes back would be a broken replay button.
            let bytes = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.expect("body");
            let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
            assert_eq!(body["success"], true, "{id}");
        }

        st.meter.drain().await;
        let rows = spooled_rows(&dir);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(!rows.is_empty(), "the drain spooled nothing: the readout itself is broken");
        let totals = |op: &str| -> (u64, u64) {
            rows.iter()
                .filter(|r| r.op == op)
                .fold((0, 0), |(reqs, msgs), r| (reqs + r.reqs, msgs + r.msgs))
        };
        // Two requests, on the route's own queue-admin class, carrying no
        // messages: that half is the base sample every gated route records.
        assert_eq!(totals("configure"), (2, 0));
        // ...and the second sample, which is the whole of D4: ONE message,
        // booked as a push, with `reqs: 0` so the HTTP request is not counted
        // twice — and only for the reply that said a frame was written. Delete
        // the arm in `handle` and this is `(0, 0)`.
        assert_eq!(totals("push"), (0, 1));
    }

    /// The `{queue, partition}` override is a CREATION path — the SP provisions
    /// a destination this cluster does not carry yet, exactly as a first-contact
    /// push does — so it answers the plan caps through the same admission, with
    /// the same code, as `/api/v1/push` and `/api/v1/configure`. Without this
    /// the replay was the one route in the proxy that creates rows and is asked
    /// nothing: a tenant at its queue cap, 403 on push and on configure, made
    /// queue 21 one dead letter at a time.
    #[tokio::test]
    async fn a_replay_that_names_a_new_destination_answers_the_registry_caps() {
        // One queue, one partition each.
        let st = gate_st_enforcing();
        let ctx = cycle_ctx(Some(1), Some(1));

        // The first destination fits and is remembered.
        assert!(admit_replay_dest(&st, &ctx, &Bytes::from_static(br#"{"queue":"orders","partition":"p"}"#), "rid")
            .await
            .is_ok());
        // A second partition in it does not.
        let refused = admit_replay_dest(
            &st,
            &ctx,
            &Bytes::from_static(br#"{"queue":"orders","partition":"p2"}"#),
            "rid",
        )
        .await
        .expect_err("over the partition cap");
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        let body = err_body(refused).await;
        assert_eq!(body["code"], errors::CODE_QUOTA_EXCEEDED);
        assert_eq!(body["error"], "partition limit reached (1)");

        // ...and a second QUEUE does not either.
        let refused = admit_replay_dest(
            &st,
            &ctx,
            &Bytes::from_static(br#"{"queue":"orders-2","partition":"p"}"#),
            "rid",
        )
        .await
        .expect_err("over the queue cap");
        let answer = err_body(refused).await;
        assert_eq!(answer["code"], errors::CODE_QUOTA_EXCEEDED);
        assert_eq!(answer["error"], "queue limit reached (1)");

        // Only ONE of the two routes can name a destination, so only one is
        // buffered and admitted. The address-keyed retry replays in place and
        // never reads its body; admitting a queue it will not create would
        // spend a plan slot on a name that never appears at the broker.
        assert!(is_replay_with_dest(&Method::POST, REPLAY_PATH));
        assert!(!is_replay_with_dest(&Method::POST, RETRY_PATH));
        assert!(!is_replay_with_dest(&Method::GET, REPLAY_PATH));
        assert!(is_dlq_replay(&Method::POST, RETRY_PATH), "still gated and billed");

        // The bodies that name no destination, and the ones this proxy cannot
        // read: nothing to admit, forwarded verbatim for the broker's own
        // answer. Same rule as an unparseable produce batch or configure body —
        // no half-enforcement on a body we did not understand. A body whose
        // only name is BLANK names nothing either (the broker 400s it), so it
        // is in this list and not in the half-named one below.
        for body in [
            br#"{}"#.as_slice(),
            b"".as_slice(),
            b"   ".as_slice(),
            br#"{"queue":""}"#.as_slice(),
            b"not json".as_slice(),
        ] {
            assert!(
                admit_replay_dest(&st, &ctx, &Bytes::copy_from_slice(body), "rid").await.is_ok(),
                "{}",
                String::from_utf8_lossy(body)
            );
        }

        // Shadow mode logs and admits, the posture every other cap in this file
        // takes: the same over-cap destination travels on.
        let shadow = gate_st();
        let ctx = cycle_ctx(Some(1), Some(1));
        assert!(admit_replay_dest(
            &shadow,
            &ctx,
            &Bytes::from_static(br#"{"queue":"a","partition":"p"}"#),
            "rid"
        )
        .await
        .is_ok());
        assert!(admit_replay_dest(
            &shadow,
            &ctx,
            &Bytes::from_static(br#"{"queue":"b","partition":"p"}"#),
            "rid"
        )
        .await
        .is_ok());
    }

    /// The name ADMITTED is the name the broker will CREATE — padding removed.
    /// `parse_replay_overrides` trims both halves before `log_dlq_move_v1`
    /// provisions anything (server/src/handlers/messages.rs), so a proxy that
    /// admitted the padded spelling would hold a plan slot for `" orders"`, a
    /// queue that never exists anywhere, and leave the `orders` the move really
    /// creates to be counted a SECOND time by the first push that spells it
    /// plainly: one queue at the broker, two against the cap, for ever.
    #[tokio::test]
    async fn a_padded_destination_is_admitted_as_the_trimmed_name() {
        // Room for exactly one queue with one partition in it: a cap is what
        // makes "the same name" and "a second name" tell each other apart.
        let st = gate_st_enforcing();
        let ctx = cycle_ctx(Some(1), Some(1));

        let padded = br#"{"queue":"  orders  ","partition":"  p  "}"#;
        assert!(admit_replay_dest(&st, &ctx, &Bytes::copy_from_slice(padded), "rid").await.is_ok());
        // The same pair, spelled the way the broker will store it. It is
        // already known, so it takes neither a second queue slot nor a second
        // partition slot — which is only true if the padded body registered the
        // trimmed pair.
        assert!(
            admit_replay_dest(
                &st,
                &ctx,
                &Bytes::from_static(br#"{"queue":"orders","partition":"p"}"#),
                "rid"
            )
            .await
            .is_ok(),
            "the padded destination must have been remembered under its trimmed name"
        );
        // ...and the cap is still a cap: a genuinely different queue is refused,
        // so the assertion above is about trimming and not about a cap that
        // stopped firing.
        let refused = admit_replay_dest(
            &st,
            &ctx,
            &Bytes::from_static(br#"{"queue":"orders-2","partition":"p"}"#),
            "rid",
        )
        .await
        .expect_err("over the queue cap");
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
    }

    /// A destination named in HALF is refused, because no cap can answer it.
    ///
    /// This is the arm `admit_replay_dest` used to END with, `_ => Ok(())`, and
    /// it was the hole the admission was added to close, one step further in:
    /// `{"partition":"p-new"}` admitted nothing and travelled on, and
    /// `queen.log_dlq_move_v1` then PROVISIONED that partition inside the
    /// source row's queue (016_messages: the destination "may not exist yet and
    /// is resolved — and PROVISIONED — inside log_push_one_v1 itself"). So a
    /// tenant already refused `403 quota_exceeded` on `/api/v1/push` and
    /// `/api/v1/configure` grew partition N+1 one dead letter at a time, and
    /// partitions are the dimension broker maintenance CPU scales on.
    /// `{"queue": ...}`-only was the same hole with the queue cap answered: the
    /// partition it creates in that queue is the source row's own name, which
    /// never reaches this proxy.
    ///
    /// The caller loses no expressiveness — an omitted half is the source row's
    /// value, and the source row is the one the caller is looking at — so the
    /// refusal names the half to add rather than guessing at it.
    #[tokio::test]
    async fn a_half_named_replay_destination_is_refused() {
        // Caps with room to spare: this is not a cap verdict, it is the shape
        // that makes a cap verdict impossible.
        let st = gate_st_enforcing();
        let ctx = cycle_ctx(Some(10), Some(10));

        for (body, missing, other) in [
            (br#"{"partition":"p-new"}"#.as_slice(), "queue", "partition"),
            (br#"{"queue":"orders.retry"}"#.as_slice(), "partition", "queue"),
        ] {
            let refused = admit_replay_dest(&st, &ctx, &Bytes::copy_from_slice(body), "rid")
                .await
                .expect_err("half-named destination");
            assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
            let answer = err_body(refused).await;
            assert_eq!(answer["code"], "invalid_request");
            let text = answer["error"].as_str().expect("error text");
            // Names the MISSING half, and only that one: the dashboard renders
            // the sentence under the Advanced input the operator has to fill in
            // (app/src/composables/useDlqReplay.js reads "<half> override" out
            // of this text), so naming both halves would file it under the
            // wrong one.
            assert!(text.starts_with(&format!("{missing} override missing")), "{text}");
            assert!(!text.contains(&format!("{other} override")), "{text}");
        }

        // A refused name is not a remembered name: the queue in a half-named
        // body is never created upstream, so it must not spend a plan slot here
        // either (`Registry::admit*` REMEMBERS what it allows).
        let tight = gate_st_enforcing();
        let one = cycle_ctx(Some(1), Some(1));
        assert!(
            admit_replay_dest(&tight, &one, &Bytes::from_static(br#"{"queue":"ghost"}"#), "rid")
                .await
                .is_err()
        );
        assert!(
            admit_replay_dest(
                &tight,
                &one,
                &Bytes::from_static(br#"{"queue":"orders","partition":"p"}"#),
                "rid"
            )
            .await
            .is_ok(),
            "the refused name must not have taken the single queue slot"
        );

        // Shadow mode refuses nothing — a cell configured to measure instead of
        // enforce has no cap to bypass, and the posture of every other decision
        // in this file is that the flag decides whether anything is refused.
        let shadow = gate_st();
        for body in [
            br#"{"partition":"p-new"}"#.as_slice(),
            br#"{"queue":"orders.retry"}"#.as_slice(),
        ] {
            assert!(
                admit_replay_dest(&shadow, &ctx, &Bytes::copy_from_slice(body), "rid")
                    .await
                    .is_ok(),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }

    /// What the body parser hands the admission, and the shapes that name
    /// nothing. The handler's own `parse_replay_overrides` is the twin
    /// (server/src/handlers/messages.rs): a present half must be a non-empty
    /// name, an omitted one keeps the source row's.
    #[test]
    fn the_replay_body_names_zero_one_or_two_halves() {
        let dest = |b: &[u8]| parse_replay_dest(b);
        assert_eq!(dest(b""), Ok((None, None)));
        assert_eq!(dest(b"  \n"), Ok((None, None)));
        assert_eq!(dest(br#"{}"#), Ok((None, None)));
        assert_eq!(
            dest(br#"{"queue":"orders.retry"}"#),
            Ok((Some("orders.retry".to_string()), None))
        );
        assert_eq!(dest(br#"{"partition":"eu-2"}"#), Ok((None, Some("eu-2".to_string()))));
        assert_eq!(
            dest(br#"{"queue":"orders.retry","partition":"eu-2"}"#),
            Ok((Some("orders.retry".to_string()), Some("eu-2".to_string())))
        );
        // A blank name is not a name: the broker 400s it, and admitting one
        // would burn a plan slot on a queue nobody can address.
        assert_eq!(dest(br#"{"queue":"   ","partition":""}"#), Ok((None, None)));
        // PADDED is the same name, because the handler trims before the SP
        // sees it (`parse_replay_overrides`). What is admitted here has to be
        // what gets created there, or the registry counts a queue the broker
        // never makes and misses the one it does.
        assert_eq!(
            dest(br#"{"queue":"  orders.retry ","partition":"\t eu-2 \n"}"#),
            Ok((Some("orders.retry".to_string()), Some("eu-2".to_string())))
        );
        // ...and a padded half is still a HALF: the trim must not turn
        // `{"queue":" orders "}` into a body that names both, nor into one that
        // names neither. `admit_replay_dest` still sees one name and refuses.
        assert_eq!(dest(br#"{"queue":" orders "}"#), Ok((Some("orders".to_string()), None)));
        // Fields this proxy does not know are the broker's business.
        assert_eq!(
            dest(br#"{"queue":"orders","reason":"manual"}"#),
            Ok((Some("orders".to_string()), None))
        );
        // Unreadable: forwarded, never half-enforced.
        assert_eq!(dest(b"not json"), Err(()));
        assert_eq!(dest(br#"["orders"]"#), Err(()));
        assert_eq!(dest(br#"{"queue":7}"#), Err(()));
        // A two-element ARRAY is the same two halves, positionally: serde reads
        // a struct from a sequence as well as from a map, and the handler's
        // `ReplayBody` is derived the same way, so the broker provisions
        // exactly what this admits. Pinned because it is surprising, and
        // because the two parsers agreeing is the property that matters.
        assert_eq!(
            dest(br#"["orders","eu-2"]"#),
            Ok((Some("orders".to_string()), Some("eu-2".to_string())))
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
