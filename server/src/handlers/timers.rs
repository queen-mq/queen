//! Timer HTTP surface — PLAN_KV_TIMERS.md §8.1 (routes), §4 (the declared timer
//! contract), §9.6 (why cancel has a route of its own), §9.5 (what an overrun
//! answers), §13.4 (payload encryption happens at SCHEDULE, not at fire).
//!
//! THE STATUS-CODE RULE IS THE SAME AS KV (§8.1): the status describes the
//! outcome of the CALL. `too_late` and `absent` are VERDICTS and answer HTTP 200
//! with `ok:false`. A cancel that arrives on a claimed timer has not failed —
//! the broker holding the claim has already decompressed and packed that payload
//! and is about to commit it, and granting the cancel would leave "did it go
//! out?" with no answer. The window is bounded by the lease.
//!
//! AND THE ONE SENTENCE THAT MUST TRAVEL WITH `absent`, because it is where a
//! user gets hurt (§4.4): there is no tombstone. `absent` means "no longer
//! pending" and MAY MEAN ALREADY DELIVERED. The authority is the log — the
//! response therefore echoes the `txn` the caller supplied, so that check needs
//! no second API call. `absent` carries `ok:false` for the same reason queue
//! delete's `deleted:false` with a 200 read as success to every client that
//! trusted the field.
//!
//! CANCEL IS NOT SCHEDULE (§9.6). `DELETE /api/v1/timers/:queue/*timerKey` is a
//! separate route so the proxy can classify it separately and NEVER block it. A
//! design that classified all timer traffic as a Produce variant would 403 the
//! cancels of a tenant that is over quota — while the fire never stops
//! automatically (§12.1), so that tenant would keep producing messages it cannot
//! stop, until the horizon or an operator. The block would produce the opposite
//! of its purpose. Schedule and cancel are the same stored procedure but NOT the
//! same authorization decision.
//!
//! ISOLATION (§13.1): nothing here names a table or a stored procedure — the
//! tenant is bound by the wrappers in `db.rs` and comes from the middleware, not
//! from a body. `tests/kv_handler_isolation.rs` enforces it mechanically.

// See kv.rs: nothing here has a caller until main.rs registers the timer routes.
// REMOVE once they are wired.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use axum::body::Bytes;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use base64::Engine;
use serde_json::{Map, Value};

use super::{json, AppState};
use crate::db;
use crate::tenant::Tenant;

// ---------------------------------------------------------------------------
// Feature gate and edge ceilings.
//
// The gate reads the RESOLVED CONFIG through the metrics latch, for the reason
// spelled out in kv.rs: an env read here would answer `false` for an embedded
// broker that turned the feature on through its builder. main.rs decides whether
// these routes exist at all (§16 step 1 — flag off means "not even registered",
// hence 404 and not 403); this is the second lock on that door.
// ---------------------------------------------------------------------------

#[inline]
pub(crate) fn timers_enabled(st: &AppState) -> bool {
    st.metrics.kvt.timers_on()
}

/// Same seam, and the same caveat, as the block in kv.rs: `Config` already
/// carries `timers_max_payload_bytes`, `timers_max_horizon_s` and
/// `timers_max_ops_per_call`, and every default here is identical to its own, so
/// the HTTP broker cannot disagree with itself. The embedded broker can, until
/// these live on `AppState`.
fn env_usize(key: &'static str, def: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(def)
}

/// §9.7 assumes a cap of 256 ops per call, and the metering counts PER OP for
/// exactly that reason. The stored procedure has no ceiling of its own on the
/// op count — this edge is the only one, which makes it load-bearing rather
/// than defensive.
fn max_ops_per_call() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| env_usize("QUEEN_TIMERS_MAX_OPS_PER_CALL", 256))
}

/// `QUEEN_TIMERS_MAX_PAYLOAD_BYTES = min(1 MiB, plan.max_payload_bytes)` —
/// DERIVED, never independent (§9.2). A timer becomes a message: if its ceiling
/// were not the message's, the timer would be a service entrance for exceeding
/// the plan's `max_payload_bytes`. The plan half of that minimum lives in the
/// proxy; this is the absolute half.
fn max_payload_bytes() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| env_usize("QUEEN_TIMERS_MAX_PAYLOAD_BYTES", 1024 * 1024))
}

/// FINITE by default (§9.2), 90 days — not "zero means unlimited". With an
/// infinite horizon the row quota stops being cyclic and becomes permanent: the
/// tenant fills `max_timers` and never frees it. With a finite one the table's
/// worst case is computable, `rows <= schedule_rate * horizon`.
fn max_horizon_ms() -> i64 {
    static V: OnceLock<i64> = OnceLock::new();
    *V.get_or_init(|| env_usize("QUEEN_TIMERS_MAX_HORIZON_S", 7_776_000) as i64 * 1000)
}

// ---------------------------------------------------------------------------
// Response shapes. Same envelope discipline as the KV surface: `error` is a code
// from the closed §9.5 taxonomy and is the only field a client may branch on.
// ---------------------------------------------------------------------------

fn body_obj(pairs: Vec<(&str, Value)>) -> String {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert(k.to_string(), v);
    }
    Value::Object(m).to_string()
}

fn err(code: &str, reason: Option<&str>, detail: Option<&str>) -> String {
    let mut pairs = vec![("error", Value::String(code.to_string()))];
    if let Some(r) = reason {
        pairs.push(("reason", Value::String(r.to_string())));
    }
    if let Some(d) = detail {
        pairs.push(("detail", Value::String(d.to_string())));
    }
    body_obj(pairs)
}

fn json_retry(status: StatusCode, body: String, secs: u32) -> Response {
    use axum::http::header;
    use axum::response::IntoResponse;
    (
        status,
        [
            (header::CONTENT_TYPE, "application/json".to_string()),
            (header::RETRY_AFTER, secs.to_string()),
        ],
        body,
    )
        .into_response()
}

fn disabled_404() -> Response {
    json(
        StatusCode::NOT_FOUND,
        err(
            "not_found",
            Some("timers_not_enabled"),
            Some("the timer surface is not enabled on this cell"),
        ),
    )
}

fn bad_request(reason: &str, detail: &str) -> Response {
    json(
        StatusCode::BAD_REQUEST,
        err("timers_bad_request", Some(reason), Some(detail)),
    )
}

fn unavailable(reason: &str) -> Response {
    json_retry(
        StatusCode::SERVICE_UNAVAILABLE,
        err("timers_unavailable", Some(reason), None),
        1,
    )
}

/// Count a refused SCHEDULE, and only on the schedule route (§14.2:
/// `queen_timers_schedule_rejected_total{reason}`). The status code IS the
/// taxonomy of §9.5 — 429 retry later and it will work, 403 retry all you like
/// and it will not, 503 not your fault, it is the cell — so mapping from it
/// keeps one classification instead of two that can disagree. A peek or a list
/// that is refused is not a schedule and is not counted here.
fn note_schedule_reject(st: &AppState, resp: &Response) {
    use crate::metrics::ScheduleReject::*;
    let why = match resp.status() {
        StatusCode::BAD_REQUEST => Shape,
        StatusCode::PAYLOAD_TOO_LARGE => PayloadTooLarge,
        StatusCode::FORBIDDEN => Horizon,
        StatusCode::TOO_MANY_REQUESTS => RateLimited,
        StatusCode::SERVICE_UNAVAILABLE => Unavailable,
        StatusCode::NOT_FOUND => Disabled,
        _ => return,
    };
    st.metrics.kvt.timers_schedule_rejected(why);
}

fn db_error_response(st: &AppState, e: &tokio_postgres::Error) -> Response {
    let dbe = match e.as_db_error() {
        None => {
            st.metrics.record_db_error();
            return unavailable("connection");
        }
        Some(d) => d,
    };
    let code = dbe.code().code().to_string();
    let msg = dbe.message().to_string();
    // The timer SP puts the actionable text in the MESSAGE (with the op's index)
    // and the teaching in the HINT; there is no name-bearing DETAIL to withhold
    // the way the KV precondition has (§13.5).
    let hint = dbe.hint().map(str::to_string);
    let class = &code[..2.min(code.len())];

    match code.as_str() {
        "22023" => json(
            StatusCode::BAD_REQUEST,
            err("timers_bad_request", Some(&msg), hint.as_deref()),
        ),
        "22001" => json(
            StatusCode::PAYLOAD_TOO_LARGE,
            err("payload_too_large", Some(&msg), hint.as_deref()),
        ),
        _ if code == "40001" || code == "40P01" || matches!(class, "08" | "53" | "57" | "58") => {
            st.metrics.record_db_error();
            unavailable(&msg)
        }
        _ if class == "42" => {
            st.metrics.record_db_error();
            static MISCONF: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
            if let Some(suppressed) = MISCONF.tick_now() {
                tracing::error!(
                    target: "timers",
                    sqlstate = %code,
                    suppressed,
                    "timer stored procedure missing or malformed; is the schema applied?"
                );
            }
            json(
                StatusCode::INTERNAL_SERVER_ERROR,
                err("timers_misconfigured", Some(&msg), None),
            )
        }
        _ => {
            st.metrics.record_db_error();
            json(
                StatusCode::INTERNAL_SERVER_ERROR,
                err("timers_error", Some(&msg), None),
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Op preparation — the part of §8.2 point 5 that belongs to this route.
// ---------------------------------------------------------------------------

/// Fields the SERVER owns. Present in an op, they are a rejection and never a
/// silent drop (§4.2): a tenant posting `{"producerSub":"billing-service"}` would
/// otherwise get, one second later, a frame in the log whose provenance is
/// attested by the broker and forged by the client — and `producer_sub` is the
/// one non-repudiable field of a frame. The stored procedure rejects the same
/// list; this edge exists because it also has to reject the UNDERSCORED spelling
/// the broker itself uses.
///
/// `_messageId` is the load-bearing one. The SP takes it as
/// `COALESCE((op->>'_messageId')::uuid, gen_random_uuid())` — it is how the
/// broker promises the id at schedule time (§20's "messageId promised at
/// schedule") — and it is NOT in the SP's own server-owned list, because the SP
/// cannot tell the broker's injection from a client's. This route can, so this
/// is where a forged message id has to die.
fn reject_server_owned(op: &Map<String, Value>, index: usize) -> Option<Response> {
    for k in op.keys() {
        if k.starts_with('_') {
            return Some(bad_request(
                "timers_server_owned_field",
                &format!(
                    "op at index {index}: `{k}` is server-owned; underscore-prefixed fields are \
                     injected by the broker and cannot be supplied"
                ),
            ));
        }
    }
    None
}

/// Prepare one op: validate the shape this route owns, mint the message id, and
/// encrypt the payload when the destination queue asks for it.
///
/// ENCRYPTION HAPPENS HERE, AT SCHEDULE, NOT AT FIRE (§13.4). Per-frame
/// encryption normally happens in the push handler before packing, with a
/// process key; a timer's push happens inside the sweeper, so if it were not
/// encrypted here the payload would sit in cleartext at rest for days. The two
/// consequences must be declared rather than discovered: a queue whose
/// encryption is turned ON after a timer was scheduled delivers that frame in
/// CLEARTEXT, and if the key rotates between schedule and fire the frame becomes
/// undecryptable. That is the price of encrypting early, and it is preferable to
/// the alternative.
///
/// ORDERING, for whoever writes the fire path: the broker encrypts the bytes it
/// received, so encryption is OUTERMOST. A payload the client compressed and
/// flagged `payloadZstd` must be decrypted first and decompressed second.
async fn prepare_schedule(
    st: &Arc<AppState>,
    tenant: &str,
    mut op: Map<String, Value>,
    index: usize,
) -> Result<Value, Response> {
    let queue = match op.get("queue").and_then(|v| v.as_str()) {
        Some(q) if !q.is_empty() => q.to_string(),
        _ => {
            return Err(bad_request(
                "timers_queue_required",
                &format!("op at index {index}: queue is required"),
            ))
        }
    };

    // §4.2 and §20.6: only RELATIVE durations on this wire, and they are in
    // MILLISECONDS. The declared rule of the product is "durations that can be
    // sub-second are in milliseconds, the ones that cannot are in seconds" — a
    // 250 ms retry backoff is a real and central use of timers, a sub-second TTL
    // is not a real use for anybody. An absolute instant is not expressible: one
    // clock, Postgres's, and no inter-broker skew can enter anywhere.
    let delay = match op.get("delayMs") {
        Some(v) if v.is_number() => v.as_f64().unwrap_or(0.0),
        _ => {
            return Err(bad_request(
                "timers_delay_required",
                &format!(
                    "op at index {index}: delayMs (a number of milliseconds) is required; a \
                     delayMs in the past is LEGAL and fires on the first cycle"
                ),
            ))
        }
    };
    // A past delay is legal (§4.2). A delay beyond the horizon is 403 and not
    // 400: it is a plan/configuration verdict, and §9.5 gives it its own code.
    if delay > max_horizon_ms() as f64 {
        return Err(json(
            StatusCode::FORBIDDEN,
            err(
                "timer_horizon_exceeded",
                Some("timers_horizon"),
                Some(&format!(
                    "op at index {index}: delayMs {} is beyond the {} ms horizon of this cell",
                    delay as i64,
                    max_horizon_ms()
                )),
            ),
        ));
    }

    let payload_b64 = match op.get("payload").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => {
            return Err(bad_request(
                "timers_payload_required",
                &format!("op at index {index}: payload (base64) is required"),
            ))
        }
    };
    let raw = match base64::engine::general_purpose::STANDARD.decode(payload_b64.as_bytes()) {
        Ok(b) => b,
        Err(e) => {
            return Err(bad_request(
                "timers_payload_not_base64",
                &format!("op at index {index}: payload is not valid base64: {e}"),
            ))
        }
    };
    if raw.len() > max_payload_bytes() {
        return Err(json(
            StatusCode::PAYLOAD_TOO_LARGE,
            err(
                "payload_too_large",
                Some("timers_payload_too_large"),
                Some(&format!(
                    "op at index {index}: payload is {} bytes, the ceiling is {}",
                    raw.len(),
                    max_payload_bytes()
                )),
            ),
        ));
    }

    // Encrypt when this queue is configured for at-rest encryption. A client
    // that ALSO claims `encrypted:true` while the broker is about to encrypt is
    // an ambiguity, not a convenience: double encryption or a lie to the
    // consumer, depending on who is right. Refuse instead of guessing.
    let broker_encrypts =
        st.encryption.is_enabled() && st.encryption_enabled_for(&queue, tenant).await;
    let client_claims = op.get("encrypted").and_then(|v| v.as_bool()) == Some(true);
    if broker_encrypts && client_claims {
        return Err(bad_request(
            "timers_encrypted_conflict",
            &format!(
                "op at index {index}: queue `{queue}` encrypts at rest, so `encrypted` is set by \
                 the broker and must not be supplied"
            ),
        ));
    }
    if broker_encrypts {
        match st.encryption.encrypt(&raw) {
            Some(env) => {
                op.insert(
                    "payload".to_string(),
                    Value::String(base64::engine::general_purpose::STANDARD.encode(&env)),
                );
                op.insert("encrypted".to_string(), Value::Bool(true));
            }
            None => {
                // Same policy as the push handler: warn (sampled — a broken
                // cipher must not flood stderr at ingest rate) and store
                // plaintext. Never fail the schedule.
                static ENC_FAIL: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
                if let Some(suppressed) = ENC_FAIL.tick_now() {
                    tracing::warn!(
                        target: "timers",
                        queue = %queue,
                        suppressed,
                        "encryption failed; timer payload stored as plaintext"
                    );
                }
            }
        }
    }

    // The message id is minted here and promised to the caller in the response:
    // a client that knows the id at schedule time can correlate the delivered
    // frame without a second API. The SP mints its own only as a fallback.
    let mid = crate::frames::uuid_bytes_to_string(&crate::util::uuidv7_bytes());
    op.insert("_messageId".to_string(), Value::String(mid));

    Ok(Value::Object(op))
}

// ---------------------------------------------------------------------------
// The one path to the database.
// ---------------------------------------------------------------------------

async fn apply_ops(
    st: &Arc<AppState>,
    tenant: &str,
    producer_sub: Option<&str>,
    ops: Vec<Value>,
) -> Result<Vec<Value>, Response> {
    if ops.is_empty() {
        return Ok(Vec::new());
    }
    if ops.len() > max_ops_per_call() {
        return Err(bad_request(
            "timers_too_many_ops",
            &format!(
                "{} ops in one call, the ceiling is {}",
                ops.len(),
                max_ops_per_call()
            ),
        ));
    }
    let ops_json = Value::Array(ops).to_string();

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => {
            st.metrics.record_db_error();
            return Err(unavailable("timers_pool_exhausted"));
        }
    };
    // Captured before the query: a schedule that outlives the broker-side
    // timeout must be cancelled server-side, not abandoned. An abandoned
    // statement keeps row locks on the timer table, and those locks are what a
    // user's cancel waits behind (§12).
    let cancel = client.cancel_token();
    let res = tokio::time::timeout(
        st.stmt_timeout,
        db::timers_apply(&client, &ops_json, tenant, producer_sub),
    )
    .await;

    match super::kv::resolve_db(res, client, cancel, "timers_apply", &st.metrics) {
        Ok(txt) => match serde_json::from_str::<Value>(&txt) {
            Ok(Value::Array(a)) => Ok(a),
            _ => {
                st.metrics.record_db_error();
                Err(json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    err("timers_error", Some("timers_result_unreadable"), None),
                ))
            }
        },
        Err(Some(e)) => Err(db_error_response(st, &e)),
        Err(None) => Err(unavailable("timers_timeout")),
    }
}

fn batch_response(results: Vec<Value>) -> Response {
    json(
        StatusCode::OK,
        Value::Object(Map::from_iter([(
            "results".to_string(),
            Value::Array(results),
        )]))
        .to_string(),
    )
}

fn single_response(results: Vec<Value>) -> Response {
    match results.into_iter().next() {
        Some(v) => json(StatusCode::OK, v.to_string()),
        None => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            err("timers_error", Some("timers_result_missing"), None),
        ),
    }
}

// ---------------------------------------------------------------------------
// POST /api/v1/timers — schedule and reschedule, batch.
//
// `cancel` is accepted in this array too (it is the same stored procedure and
// the same transaction), but a cancel sent here inherits this route's
// authorization class. The route that is guaranteed never to be blocked is
// DELETE /api/v1/timers/:queue/*timerKey (§9.6), and that is what an SDK must
// use when it cancels.
// ---------------------------------------------------------------------------

pub async fn handle_timers_batch(
    State(st): State<Arc<AppState>>,
    Extension(authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<Tenant>,
    body: Bytes,
) -> Response {
    let resp = timers_batch_inner(&st, authed, &tenant, body).await;
    // One funnel for the reject counter: every non-200 leaving THIS route is a
    // schedule that did not happen, and the status already carries which kind.
    if resp.status() != StatusCode::OK {
        note_schedule_reject(&st, &resp);
    }
    resp
}

async fn timers_batch_inner(
    st: &Arc<AppState>,
    authed: crate::auth::AuthedSub,
    tenant: &Tenant,
    body: Bytes,
) -> Response {
    if !timers_enabled(st) {
        return disabled_404();
    }
    let root: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return bad_request("timers_bad_body", &e.to_string()),
    };
    let raw_ops = match &root {
        Value::Array(a) => a.clone(),
        Value::Object(o) => match o.get("operations") {
            Some(Value::Array(a)) => a.clone(),
            _ => {
                return bad_request(
                    "timers_bad_body",
                    "body must be an array of operations, or {\"operations\":[...]}",
                )
            }
        },
        _ => {
            return bad_request(
                "timers_bad_body",
                "body must be an array of operations, or {\"operations\":[...]}",
            )
        }
    };
    if raw_ops.len() > max_ops_per_call() {
        return bad_request(
            "timers_too_many_ops",
            &format!(
                "{} ops in one call, the ceiling is {}",
                raw_ops.len(),
                max_ops_per_call()
            ),
        );
    }

    let mut ops: Vec<Value> = Vec::with_capacity(raw_ops.len());
    for (i, raw) in raw_ops.into_iter().enumerate() {
        let obj = match raw {
            Value::Object(o) => o,
            _ => return bad_request("timers_bad_op", &format!("op at index {i} is not an object")),
        };
        if let Some(r) = reject_server_owned(&obj, i) {
            return r;
        }
        let kind = obj.get("op").and_then(|v| v.as_str()).unwrap_or_default();
        match kind {
            "schedule" | "reschedule" => match prepare_schedule(st, tenant.as_str(), obj, i).await
            {
                Ok(prepared) => ops.push(prepared),
                Err(resp) => return resp,
            },
            // Cancel and anything unknown go through untouched: the stored
            // procedure owns the closed taxonomy, and duplicating it here would
            // give the product two places to disagree about what an operation is.
            _ => ops.push(Value::Object(obj)),
        }
    }

    // producer_sub is stamped ONLY from the validated JWT `sub`, never from the
    // body (§4.2) — the same rule the push handler follows.
    let producer_sub = authed.0.filter(|s| !s.is_empty());

    match apply_ops(st, tenant.as_str(), producer_sub.as_deref(), ops).await {
        Ok(results) => {
            // SEAM (§7.4): the local, in-process sweeper wake goes HERE, AFTER
            // the commit and never before — a wake for a transaction that then
            // rolls back costs a wasted cycle and, worse, teaches the loop that
            // work exists which does not. It is one `st.timer_wake.hint(ms)`
            // once AppState carries the waker, and the anti-storm property is
            // free: the hint applies only when the new minimum is EARLIER, so a
            // million timers scheduled for next week produce exactly one wake.
            // Without it nothing breaks: QUEEN_SWEEPER_MAX_SLEEP_MS (1 s) is the
            // recovery window, and deliverAt is "not before", never "exactly at".
            batch_response(results)
        }
        Err(resp) => resp,
    }
}

// ---------------------------------------------------------------------------
// DELETE /api/v1/timers/:queue/*timerKey — cancel. §9.6: its own route and its
// own class, never blockable.
// ---------------------------------------------------------------------------

pub async fn handle_timer_cancel(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<Tenant>,
    Path((queue, timer_key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if !timers_enabled(&st) {
        return disabled_404();
    }
    let mut op = Map::new();
    op.insert("op".to_string(), Value::String("cancel".to_string()));
    op.insert("queue".to_string(), Value::String(queue));
    op.insert("timerKey".to_string(), Value::String(timer_key));
    // The caller may echo the txn it expects, and the SP hands it back on
    // `absent` so the "was it already delivered?" check needs no second API
    // (§4.4). It is the only query parameter this route reads, and it is the
    // caller's own identifier — nothing is revealed by its presence.
    if let Some(txn) = q.get("txn").filter(|s| !s.is_empty()) {
        op.insert("txn".to_string(), Value::String(txn.clone()));
    }

    // A cancel carries no producer identity: it produces nothing.
    match apply_ops(&st, tenant.as_str(), None, vec![Value::Object(op)]).await {
        Ok(results) => single_response(results),
        Err(resp) => resp,
    }
}

// ---------------------------------------------------------------------------
// GET /api/v1/timers/:queue/*timerKey — peek, one key, with the payload.
// ---------------------------------------------------------------------------

pub async fn handle_timer_peek(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<Tenant>,
    Path((queue, timer_key)): Path<(String, String)>,
) -> Response {
    if !timers_enabled(&st) {
        return disabled_404();
    }
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => {
            st.metrics.record_db_error();
            return unavailable("timers_pool_exhausted");
        }
    };
    let cancel = client.cancel_token();
    let res = tokio::time::timeout(
        st.stmt_timeout,
        db::timers_peek(&client, tenant.as_str(), &queue, &timer_key),
    )
    .await;
    match super::kv::resolve_db(res, client, cancel, "timers_peek", &st.metrics) {
        // A miss is `{"found":false}` with HTTP 200, not a 404: the one
        // status-code rule (§8.1). And the payload comes back exactly as it is
        // stored, with `encrypted` telling the truth about it — peek is an
        // inspection surface and must not quietly decrypt what the fire will
        // deliver as an envelope.
        Ok(txt) => json(StatusCode::OK, txt),
        Err(Some(e)) => db_error_response(&st, &e),
        Err(None) => unavailable("timers_timeout"),
    }
}

// ---------------------------------------------------------------------------
// GET /api/v1/timers/:queue — list, keyset.
//
// THE QUEUE IS MANDATORY (§4.1), which is why it is a path segment and not a
// filter: a tenant-wide list would be a scan that an end user of the customer
// could trigger, on the first endpoint of this product whose call rate is
// decided by somebody else's web traffic.
// ---------------------------------------------------------------------------

pub async fn handle_timers_list(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<Tenant>,
    Path(queue): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if !timers_enabled(&st) {
        return disabled_404();
    }
    // `after` is an EXCLUSIVE keyset cursor, not an offset, and it is stable
    // because timer_key carries COLLATE "C". `limit` is CLAMPED by the SP and
    // never rejected, with `truncated` telling the truth: a 400 on a too-large
    // limit is an error the user cannot fix without reading the server's
    // configuration.
    let after = q.get("after").filter(|s| !s.is_empty()).cloned();
    let limit = q
        .get("limit")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(100);

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => {
            st.metrics.record_db_error();
            return unavailable("timers_pool_exhausted");
        }
    };
    let cancel = client.cancel_token();
    let res = tokio::time::timeout(
        st.stmt_timeout,
        db::timers_list(&client, tenant.as_str(), &queue, after.as_deref(), limit),
    )
    .await;
    match super::kv::resolve_db(res, client, cancel, "timers_list", &st.metrics) {
        Ok(txt) => json(StatusCode::OK, txt),
        Err(Some(e)) => db_error_response(&st, &e),
        Err(None) => unavailable("timers_timeout"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rule that closes the forged-provenance hole: the SP cannot tell the
    /// broker's `_messageId` from a client's, so this route is where a supplied
    /// one has to die.
    #[test]
    fn underscore_prefixed_fields_are_rejected() {
        let mut op = Map::new();
        op.insert("_messageId".to_string(), Value::String("x".to_string()));
        assert!(reject_server_owned(&op, 0).is_some());

        let mut tenant_field = Map::new();
        tenant_field.insert("_tenant".to_string(), Value::String("x".to_string()));
        assert!(reject_server_owned(&tenant_field, 0).is_some());

        let mut ok = Map::new();
        ok.insert("queue".to_string(), Value::String("q".to_string()));
        ok.insert("delayMs".to_string(), Value::from(250));
        assert!(reject_server_owned(&ok, 0).is_none());
    }

    /// The horizon is finite by default and is a 403 (a plan verdict), never a
    /// 400 — §9.5 keeps the two apart on purpose.
    #[test]
    fn the_default_horizon_is_ninety_days() {
        assert_eq!(max_horizon_ms(), 7_776_000_000);
    }

    /// The four routes of §8.1, built exactly as main.rs must build them (see
    /// the twin test in kv.rs for why registration cannot be exported as a
    /// sub-router). The cancel route being its own DELETE path is §9.6 in the
    /// router: it is what lets the proxy classify cancel apart from schedule and
    /// never block it.
    #[test]
    fn the_four_routes_accept_these_handlers() {
        use axum::routing::{delete, get, post};
        let _: axum::Router<Arc<AppState>> = axum::Router::new()
            .route("/api/v1/timers", post(handle_timers_batch))
            .route("/api/v1/timers/:queue", get(handle_timers_list))
            .route("/api/v1/timers/:queue/*timerKey", get(handle_timer_peek))
            .route("/api/v1/timers/:queue/*timerKey", delete(handle_timer_cancel));
    }
}
