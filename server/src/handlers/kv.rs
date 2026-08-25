//! KV HTTP surface — PLAN_KV_TIMERS.md §8.1 (routes), §8.4 (the three defences),
//! §5.5 (where getPrefix may live), §9.5 (what every overrun answers), §13.1/§13.5
//! (isolation and error hygiene).
//!
//! ONE RULE ABOUT STATUS CODES, AND IT IS THE REASON THIS FILE READS ODD (§8.1):
//! the status describes the outcome of the CALL, never the verdict of the
//! business predicate. An absent key, a lost putIfAbsent race, a delete that hit
//! nothing — all 200, with an explicit field in the body. `applied:false` is the
//! single most frequent outcome of this product, and a 4xx would put it inside
//! the retry policy, the error metrics and the dashboards of seven clients plus
//! the proxy. The declared cost: `curl` does not behave "RESTfully" on a missing
//! key and anyone scripting this must read the body. The in-house precedent is
//! queue delete, which keeps 200 on `deleted:false` for exactly this reason.
//!
//! THE ISOLATION RULE (§13.1). Nothing in this file names a table or a stored
//! procedure. `tenant_id` is not a filter applied to an id the caller handed us —
//! it IS part of the primary key, and the only code allowed to bind it is the
//! wrapper layer in `db.rs`. `tests/kv_handler_isolation.rs` makes that
//! mechanical: it greps every source under `src/handlers/` for the two table
//! names and fails the suite on a hit. The grep is deliberately dumb — it reads
//! comments too, and the KV procedure's own name has the table name as a prefix,
//! so not even prose here may spell either of them. The tenant comes from
//! `Extension<Tenant>`, i.e. from the middleware that reads the trusted header,
//! and never from a request body.
//!
//! WHAT THIS FILE DELIBERATELY DOES NOT DO:
//!   * NO arbiter lane, for reads OR writes (§8.4 point 2, which corrects the
//!     earlier design). Thirty tenants each inside their own 100 writes/s would
//!     burn 3000 `Lane::Push` slots per second on a stack whose measured
//!     commit-bound ceiling on a free tier is ~480 msg/s: nobody violated
//!     anything and the message path starves, because the weigher cannot tell
//!     the two kinds of work apart once they share a lane. Backpressure here is
//!     the pool, not the arbiter. Do not add `admission::acquire` to this file.
//!   * NO caching of KV values, with any TTL (§8.5). Use case number one is the
//!     idempotency marker: a stale read says "not there" and the caller performs
//!     the external effect twice. Single-flight is the only safe amplifier and it
//!     is a seam below, not a cache.
//!   * NO prefix in a query string, ever (§5.5). `getPrefix` exists only inside
//!     the POST batch body. `?prefix=quota:acme:` would pass through the access
//!     logs of the broker, the proxy, the meter sample, the per-request-id
//!     tracing and any ingress in front — a mitigation living in one component
//!     out of four is not a mitigation. The GET route therefore rejects a
//!     non-empty query string outright rather than ignoring it silently.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use axum::body::Bytes;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{Map, Value};

use super::{json, AppState};
use crate::db;
use crate::switches::{decide, Origin, Surface};
use crate::tenant::Tenant;

// ---------------------------------------------------------------------------
// Edge ceilings.
//
// THERE IS NO FEATURE GATE IN THIS FILE any more, and no handler below opens
// with one. `QUEEN_KV_ENABLED` is gone: the routes are registered
// unconditionally in main.rs, so no call can arrive at a cell that "does not
// have KV", and nothing here answers 404 for that reason. See the header of
// `switches.rs` for why a boot gate and a runtime kill switch are different
// instruments and only the second survived.
//
// The rungs that remain — the operator's runtime kill switch, the grant and the
// occupancy — live in `apply_ops`, in ONE call to `switches::decide`, because
// they need the tenant and because two call sites would be two places to get
// the order of the rungs wrong.
// ---------------------------------------------------------------------------

/// Map a ladder refusal onto the reject counter of §14.2
/// (`queen_kv_read_rejected_total{reason}` = rate_limited|quota|pool|disabled).
///
/// CLASSIFIED FROM THE ANSWER, NOT FROM THE STATUS, and that had to change with
/// the boot flag. While the flag existed, `disabled` was fed by its 404 and the
/// only 503 the ladder could produce was pool-shaped, so reading the status was
/// good enough. Now the operator's kill switch is the ONLY thing that answers
/// 503 from here — and a status-based map would file it under `pool`, i.e. tell
/// whoever pulled the switch that their cell is out of connections. `disabled`
/// would meanwhile have no producer left at all, which is how a label quietly
/// becomes a lie in both directions at once.
///
/// NO new series and NO `tenant` label: §14.1 allows the tenant label only on
/// the occupancy GAUGES, one series per tenant written by the rollup, and
/// forbids it on per-operation counters where it would be tenant × op × outcome,
/// i.e. a cardinality the user chooses — the same disease this endpoint is being
/// defended against. Naming the tenant is the job of the top-N log line.
fn note_reject(st: &AppState, answer: crate::switches::Answer) {
    use crate::metrics::KvReject;
    use crate::quota::Verdict;
    use crate::switches::Answer;
    st.metrics.kvt.kv_read_rejected(match answer {
        // An operator pulled the lever. This is the surface being off, which is
        // exactly and only what `disabled` has ever meant.
        Answer::Paused => KvReject::Disabled,
        Answer::Refused(Verdict::RateLimited(_)) => KvReject::RateLimited,
        // A cell condition, not the tenant's doing (§9.4 point 2) — same bucket
        // as the standalone shed below, which is the other way to run out of the
        // cell's own room.
        Answer::Refused(Verdict::NoRoom) => KvReject::Pool,
        _ => KvReject::Quota,
    });
}

/// Render a ladder answer, or `None` when the call may proceed.
fn gated(st: &AppState, tenant: &str, surface: Surface, rows: i64, bytes: i64) -> Option<Response> {
    let a = decide(&st.switches, &st.quota, tenant, surface, rows, bytes);
    let h = a.http(Origin::Route, surface)?;
    note_reject(st, a);
    let status = StatusCode::from_u16(h.status).unwrap_or(StatusCode::FORBIDDEN);
    // `reason` carries the same stable identifier the code does when the taxonomy
    // has nothing finer to say: clients branch on `error`, never on prose (§13.5).
    let body = err(h.code, Some(h.code), None);
    Some(match h.retry_after {
        Some(secs) => json_retry(status, body, secs),
        None => json(status, body),
    })
}

/// What a batch could ADD, as an upper bound (§9.3: the delta is a majorant).
///
/// Every op that can CREATE a key counts one row; `delete` counts none, which is
/// what makes "deletes are always allowed" structural rather than a special case
/// (§9.5). `incr` is included because its first call on an absent key creates
/// one. The byte figure is the serialized value, the same number the edge
/// ceiling measures — deliberately not the canonical JSONB length the stored
/// procedure would compute, since the gate runs BEFORE a connection is taken and
/// over-counting is the safe direction.
pub(super) fn write_footprint(ops: &[Value]) -> (i64, i64) {
    let mut rows = 0i64;
    let mut bytes = 0i64;
    for o in ops {
        match o.get("op").and_then(|v| v.as_str()) {
            Some("put" | "putIfAbsent" | "incr") => {
                rows += 1;
                bytes += o.get("value").map(|v| v.to_string().len() as i64).unwrap_or(0);
            }
            _ => {}
        }
    }
    (rows, bytes)
}

/// Read a positive integer env knob once. These four ceilings are the HTTP-edge
/// half of §9.2: the SP carries the same numbers as constants and is the floor
/// nothing can get under, while these guard the body BEFORE a connection is
/// taken. The value ceiling is the documented case where the two halves measure
/// different things — raw body bytes here, canonical JSONB text in the SP,
/// normally shorter — and that surprise belongs in the documentation, not in a
/// bug report from the first user with a value near the ceiling.
///
/// SEAM: `Config` ALREADY carries every one of these (`kv_max_value_bytes`,
/// `kv_max_ops_per_call`, `kv_max_keys_per_call`, `timers_max_*`) — they are read
/// here from the environment only because `AppState` does not carry them yet, and
/// `AppState` is not this phase's file. For the HTTP broker the two agree by
/// construction: same variable, same default, resolved once. For the EMBEDDED
/// broker they can diverge, because its configuration comes from a builder and
/// not from the process environment — which is the same reason the enable flag
/// above is read from the resolved config instead. Four fields on `AppState`
/// close it, and these four functions disappear.
fn env_usize(key: &'static str, def: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(def)
}

fn max_value_bytes() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| env_usize("QUEEN_KV_MAX_VALUE_BYTES", 65_536))
}

fn max_ops_per_call() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| env_usize("QUEEN_KV_MAX_OPS_PER_CALL", 256))
}

/// 1024 and not the stored procedure's 4096: the edge is allowed to be STRICTER
/// than the floor, and this default is `config.rs`'s, which is the number an
/// operator sees documented. Every default in this block is kept identical to
/// `Config`'s on purpose — the two read the same environment variables, so for
/// the HTTP broker they cannot disagree, and the moment they could is the moment
/// they must move onto `AppState` (see the seam note above).
fn max_keys_per_call() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| env_usize("QUEEN_KV_MAX_KEYS_PER_CALL", 1024))
}

// ---------------------------------------------------------------------------
// The three defences of §8.4, and which of them live here.
//
// 1. DEDICATED POOL, `QUEEN_KV_POOL_SIZE = clamp(pool_size / 10, 4, 32)`, derived
//    from the pool and not hardcoded. The pool IS the semaphore, and the property
//    no other defence gives: at ~1 ms reads its capacity is ~16 000/s, far above
//    any rate limit, so it is not the limiter in normal conditions — it becomes
//    the limiter exactly when the DB slows down, and at 100 ms per read capacity
//    collapses to 160/s and the rest gets 503 instead of stealing connections
//    from the message path.
// 2. NO lane (see the module header).
// 3. PER-TENANT TOKEN BUCKET evaluated BEFORE pool.get(), 429 + Retry-After.
// 4. `resolve_query_timeout` mandatory: a slow getPrefix with no server-side
//    cancel leaves the backend spinning and quarantines the connection, and on a
//    pool of 16 three quarantines are 19% of capacity.
//
// (1) and (3) own state that lives on `AppState`, which this phase does not own.
// `kv_pool` below is the ONE line to change when the dedicated pool lands, and
// `rate_check` is the ONE call site for the bucket. Point (4) is implemented
// here, in full, on every call.
// ---------------------------------------------------------------------------

#[inline]
fn kv_pool(st: &AppState) -> &deadpool_postgres::Pool {
    // SEAM (§8.4 point 1): return `&st.kv_pool` once AppState carries it. Until
    // then the shared pool is used, which is correct but NOT yet a bulkhead —
    // that is the difference between "the KV endpoint is slow" and "the KV
    // endpoint made the message path slow".
    &st.pool
}

/// RUNG 5 of the degradation ladder (§12.1): "scritture KV standalone rifiutate
/// … le scritture KV DENTRO il wire continuano: la transazione e' il valore del
/// prodotto, la POST e' la comodita'."
///
/// The trigger is sustained refusal by the pool, which is the cell telling us the
/// database is slow — not the tenant telling us anything, which is why the answer
/// is 503 and not 429 or 403. It is a streak and not a single miss: one refusal
/// under a burst is normal, `kv_standalone_shed_after` in a row is a condition.
///
/// This rung exists ONLY on this file's surface. The in-wire KV path in
/// `data.rs` never consults it, and that asymmetry is the whole content of the
/// rung: shedding the convenience keeps the transaction working.
fn standalone_shed(st: &AppState) -> bool {
    use std::sync::atomic::Ordering;
    let shed = st.kv_pressure.load(Ordering::Relaxed) >= st.kv_standalone_shed_after;
    static SHED: crate::obs::OnChange<bool> = crate::obs::OnChange::new();
    crate::obs::sweeper_stage(
        &SHED,
        "kv_standalone_writes",
        shed,
        "the KV pool refused consecutive standalone writes",
    );
    shed
}

fn note_pool(st: &AppState, ok: bool) {
    use std::sync::atomic::Ordering;
    if ok {
        st.kv_pressure.store(0, Ordering::Relaxed);
    } else {
        // Saturating, so a long outage cannot wrap the counter back under the
        // threshold and silently un-shed the rung.
        let _ = st
            .kv_pressure
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(1))
            });
    }
}

// ---------------------------------------------------------------------------
// Response shapes.
// ---------------------------------------------------------------------------

fn body_obj(pairs: Vec<(&str, Value)>) -> String {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert(k.to_string(), v);
    }
    Value::Object(m).to_string()
}

/// Error envelope. `error` is a CODE from the closed §9.5 taxonomy and is the
/// only field a client may branch on: string matching on a message is forbidden
/// everywhere in this codebase. `reason` carries the SP's opaque MESSAGE (also a
/// stable identifier, e.g. `kv_bad_ttl`), `detail` the human half.
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

fn bad_request(reason: &str, detail: &str) -> Response {
    json(StatusCode::BAD_REQUEST, err("kv_bad_request", Some(reason), Some(detail)))
}

fn unavailable(reason: &str) -> Response {
    json_retry(
        StatusCode::SERVICE_UNAVAILABLE,
        err("kv_unavailable", Some(reason), None),
        1,
    )
}

// ---------------------------------------------------------------------------
// DB error → HTTP, per §9.5 and §7.6's SQLSTATE taxonomy.
//
// The three interesting rows:
//
//   23514 check_violation — `required:true` lost its precondition. §8.3: this is
//   the EXPECTED outcome of every legitimate redelivery, so it is HTTP 200 with
//   an explicit body, never a 4xx/5xx. The transaction really did abort in SQL
//   and the RAISE really was necessary, but it must pollute neither the error
//   metrics nor the retry policies. Everything the client needs is read from
//   `detail()`, which is JSON, and NEVER from the message: the message is
//   deliberately opaque because handlers echo DB text and namespace/key names
//   would land in shared logs and error aggregators (§13.5).
//
//   42xxx — configuration, not data: a new broker against an old database
//   (`QUEEN_APPLY_SCHEMA=0`) resolves the SP at runtime and fails with 42883.
//   Permanent, so a retry is pointless and the status must not invite one: 500.
//
//   40001 / 40P01 / classes 08, 53, 57, 58, and NO SQLSTATE at all — transient.
//   503 + Retry-After, which §9.5 defines as "not your fault, it is the cell".
// ---------------------------------------------------------------------------

fn precondition_200(detail: Option<&str>) -> Response {
    // The DETAIL is capped at 4 KiB in the SP, so a pathological value can
    // truncate it into invalid JSON. Degrade to the bare verdict rather than
    // turning a legitimate lost race into a 500.
    let parsed: Option<Value> = detail.and_then(|d| serde_json::from_str(d).ok());
    let mut pairs = vec![
        ("ok", Value::Bool(false)),
        ("reason", Value::String("kv_precondition".to_string())),
    ];
    if let Some(v) = parsed {
        pairs.push(("failedIndex", v.get("index").cloned().unwrap_or(Value::Null)));
        pairs.push(("kvReason", v.get("reason").cloned().unwrap_or(Value::Null)));
        pairs.push(("version", v.get("version").cloned().unwrap_or(Value::Null)));
        pairs.push(("value", v.get("value").cloned().unwrap_or(Value::Null)));
    }
    json(StatusCode::OK, body_obj(pairs))
}

/// Resolve a timed DB call while PRESERVING the SQLSTATE-bearing error.
///
/// `db::resolve_query_timeout` collapses "the statement failed" into `None`,
/// which is right for the pop paths (an empty result is a legal answer there)
/// and wrong here: this surface's whole error taxonomy — 400 on shape, 413 on
/// size, 200 on a lost precondition, 503 on transient — is a function of the
/// SQLSTATE, and a lost SQLSTATE would turn every one of them into a 500.
///
/// The TIMEOUT arm is delegated to `db::resolve_query_timeout` unchanged rather
/// than reimplemented: it owns the server-side cancel (which needs the TLS
/// connector matching how the pool was built) and the deadpool quarantine, and a
/// second copy of that logic in a handler is precisely how the two drift. §8.4
/// point 4 makes this mandatory on these routes: a slow read whose backend keeps
/// running holds its locks, and on a pool of 16 three quarantined connections
/// are 19% of capacity.
///
/// `Err(None)` is the timeout, `Err(Some(e))` the database's own verdict.
pub(super) fn resolve_db<T>(
    res: Result<Result<T, tokio_postgres::Error>, tokio::time::error::Elapsed>,
    client: deadpool_postgres::Client,
    cancel: tokio_postgres::CancelToken,
    what: &'static str,
    metrics: &crate::metrics::Metrics,
) -> Result<T, Option<tokio_postgres::Error>> {
    match res {
        Ok(Ok(v)) => {
            drop(client);
            Ok(v)
        }
        // NOT counted as a db error here: most of these are verdicts (a lost
        // race, a malformed op) and counting them would inflate "DB errors" on
        // the single most frequent outcome of the product. The arms in
        // `db_error_response` that ARE failures record it themselves — the same
        // discrimination the transaction path makes for QDUP/QTXN.
        Ok(Err(e)) => {
            drop(client);
            Err(Some(e))
        }
        Err(elapsed) => {
            let _: Option<T> = db::resolve_query_timeout(Err(elapsed), client, cancel, what, metrics);
            Err(None)
        }
    }
}

fn db_error_response(st: &AppState, e: &tokio_postgres::Error) -> Response {
    let dbe = match e.as_db_error() {
        // No SQLSTATE means the connection itself failed: transient by the
        // house classifier, and the tenant did nothing wrong.
        None => {
            st.metrics.record_db_error();
            return unavailable("connection");
        }
        Some(d) => d,
    };
    let code = dbe.code().code().to_string();
    let msg = dbe.message().to_string();
    let detail = dbe.detail().map(str::to_string);
    let class = &code[..2.min(code.len())];

    match code.as_str() {
        // A lost precondition is a verdict, not a failure: no db-error metric.
        "23514" => precondition_200(detail.as_deref()),
        // Shape: charset, missing TTL, unknown op, empty prefix, a tenant field
        // inside an op, getPrefix in the wire, one key named twice.
        //
        // The DETAIL is returned TO THE CALLER but never written to a broker log
        // (§13.5): it describes what the caller itself just sent, so handing it
        // back is not disclosure, while logging it would put namespaces into
        // shared logs and error aggregators. Verified in 024: no DETAIL on this
        // path interpolates a KEY — the worst case is a malformed namespace,
        // echoed only to whoever wrote it.
        "22023" => json(
            StatusCode::BAD_REQUEST,
            err("kv_bad_request", Some(&msg), detail.as_deref()),
        ),
        // Value or key over the ceiling. 413 is the §9.5 row; the SP's own
        // measurement is the canonical JSONB text, not the raw body.
        "22001" => json(
            StatusCode::PAYLOAD_TOO_LARGE,
            err("payload_too_large", Some(&msg), detail.as_deref()),
        ),
        _ if code == "40001" || code == "40P01" || matches!(class, "08" | "53" | "57" | "58") => {
            st.metrics.record_db_error();
            unavailable(&msg)
        }
        // Class 42 is configuration (§7.6): an operator can repair it, and it is
        // the shape of "new broker, old schema". Loud, not retryable.
        _ if class == "42" => {
            st.metrics.record_db_error();
            static MISCONF: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
            if let Some(suppressed) = MISCONF.tick_now() {
                tracing::error!(
                    target: "kv",
                    sqlstate = %code,
                    suppressed,
                    "kv stored procedure missing or malformed; is the schema applied?"
                );
            }
            json(
                StatusCode::INTERNAL_SERVER_ERROR,
                err("kv_misconfigured", Some(&msg), None),
            )
        }
        _ => {
            st.metrics.record_db_error();
            json(
                StatusCode::INTERNAL_SERVER_ERROR,
                err("kv_error", Some(&msg), None),
            )
        }
    }
}

// ---------------------------------------------------------------------------
// The one path to the database.
// ---------------------------------------------------------------------------

/// Apply a validated op array. `p_in_wire` is FALSE here by construction: this
/// is the HTTP surface, the one place `getPrefix` and `incr` are allowed. The
/// transaction wire passes TRUE from its own call site (§5.5, §6.3) — that flag
/// is a parameter of the SP and not a second procedure, so the two surfaces can
/// never drift apart.
async fn apply_ops(st: &Arc<AppState>, tenant: &str, ops: Vec<Value>, write: bool) -> Result<Vec<Value>, Response> {
    if ops.is_empty() {
        return Ok(Vec::new());
    }
    if ops.len() > max_ops_per_call() {
        return Err(bad_request(
            "kv_too_many_ops",
            &format!("{} ops in one call, the ceiling is {}", ops.len(), max_ops_per_call()),
        ));
    }
    // §6.1 point 4: an op count alone bounds nothing — 63 getMany of 256 keys
    // read 16 128 rows. The key sum is a budget of its own, at the edge as well
    // as in the SP, and it is counted the SAME way in both: a getPrefix counts
    // as its CLAMPED limit, which is the most it can return. Counting it as one
    // would let a batch of prefix reads through the edge only to be refused by
    // the SP — after spending the connection this guard exists to protect.
    const PREFIX_DEFAULT: usize = 100;
    const PREFIX_CAP: usize = 1000;
    let keys: usize = ops
        .iter()
        .map(|o| match o.get("op").and_then(|v| v.as_str()) {
            Some("getMany") => o
                .get("keys")
                .and_then(|k| k.as_array())
                .map(|a| a.len())
                .unwrap_or(0),
            Some("getPrefix") => o
                .get("limit")
                .and_then(|v| v.as_u64())
                .map(|n| (n as usize).clamp(1, PREFIX_CAP))
                .unwrap_or(PREFIX_DEFAULT),
            _ => 1,
        })
        .sum();
    if keys > max_keys_per_call() {
        return Err(bad_request(
            "kv_too_many_keys",
            &format!("{} keys in one call, the ceiling is {}", keys, max_keys_per_call()),
        ));
    }
    // Raw-body half of the value ceiling (§9.2). Measured on the serialized
    // value, before a connection is taken.
    for (i, o) in ops.iter().enumerate() {
        if let Some(v) = o.get("value") {
            let n = v.to_string().len();
            if n > max_value_bytes() {
                return Err(json(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    err(
                        "payload_too_large",
                        Some("kv_value_too_large"),
                        Some(&format!(
                            "op at index {i}: value is {n} bytes, the ceiling is {}",
                            max_value_bytes()
                        )),
                    ),
                ));
            }
        }
    }

    // Rung 5 (§12.1), before everything else this function does with a
    // connection: a cell whose pool is refusing sheds the CONVENIENCE surface and
    // keeps the transaction working. Reads are not shed — they are the cheap half
    // and the one an idempotency marker cannot do without.
    if write && standalone_shed(st) {
        st.metrics.kvt.kv_read_rejected(crate::metrics::KvReject::Pool);
        return Err(json_retry(
            StatusCode::SERVICE_UNAVAILABLE,
            err(
                "kv_unavailable",
                Some("kv_standalone_paused"),
                Some(
                    "standalone KV writes are shed while the cell is under pressure; KV \
                     operations inside POST /api/v1/transaction continue",
                ),
            ),
            1,
        ));
    }

    // THE LADDER (§9.5, §12.1), in one call, and evaluated BEFORE `pool.get()`
    // because the entire point of a rate limit is not to spend a connection on a
    // request that is over it (§8.4 point 3).
    //
    // `rate_limited` on a tenant that was previously at zero is the EARLIEST of
    // the six pre-incident signals (§14.3.1) — not a fault, but the advance
    // warning of the one new failure mode this feature introduces: a customer
    // who has just put KV reads on their own end users' request path.
    let (add_rows, add_bytes) = if write {
        write_footprint(&ops)
    } else {
        (0, 0)
    };
    let surface = if write { Surface::KvWrite } else { Surface::KvRead };
    if let Some(resp) = gated(st, tenant, surface, add_rows, add_bytes) {
        return Err(resp);
    }

    // The op kinds are kept for the metrics: on failure the SP tells us nothing
    // per-op, and a batch whose ops all read as `get` would misattribute the
    // whole error series.
    let kinds: Vec<crate::metrics::KvOp> = ops.iter().filter_map(|o| kv_op_label(o.get("op").and_then(|v| v.as_str()))).collect();
    let bytes_in: u64 = ops
        .iter()
        .filter_map(|o| o.get("value"))
        .map(|v| v.to_string().len() as u64)
        .sum();

    let ops_json = Value::Array(ops).to_string();

    let client = match kv_pool(st).get().await {
        Ok(c) => c,
        // The pool IS the bulkhead (§8.4 point 1): exhausted means the DB is
        // slow, which is the cell's problem and not the tenant's. 503, not 500,
        // and with Retry-After — §12.1 degradation stage 2.
        Err(_) => {
            st.metrics.record_db_error();
            st.metrics.kvt.kv_read_rejected(crate::metrics::KvReject::Pool);
            note_pool(st, false);
            // The call never reached the database, so the charge it took must go
            // back. Without this, a slow database inflates every delta until the
            // tenant answers 403 — turning a cell fault ("not your fault", 503)
            // into a plan verdict ("yours", 403), which §12 forbids.
            st.quota.refund(tenant, add_rows, add_bytes, 0);
            return Err(unavailable("kv_pool_exhausted"));
        }
    };
    note_pool(st, true);
    // Captured BEFORE the query: on a broker-side timeout the still-running
    // statement is cancelled server-side and this connection is quarantined
    // rather than abandoned (§8.4 point 4).
    let cancel = client.cancel_token();
    let t0 = std::time::Instant::now();
    let res = tokio::time::timeout(
        st.stmt_timeout,
        db::kv_apply(&client, &ops_json, tenant, false),
    )
    .await;
    // One duration per CALL, shared out over the ops of the batch: a batch is one
    // round trip and one commit, so charging every op the whole latency would
    // make the p99 a function of batch size rather than of the database.
    let ms = t0.elapsed().as_secs_f64() * 1000.0 / (kinds.len().max(1) as f64);

    match resolve_db(res, client, cancel, "kv_apply", &st.metrics) {
        Ok(txt) => match serde_json::from_str::<Value>(&txt) {
            // §6.4: the results are index-aligned to the input array, and the SP
            // raises rather than returning a short one — so a shape other than an
            // array is a broken contract, never a user error.
            Ok(Value::Array(a)) => {
                record_results(st, &a, ms, bytes_in);
                Ok(a)
            }
            _ => {
                st.metrics.record_db_error();
                record_all(st, &kinds, crate::metrics::KvResult::Error, ms);
                Err(json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    err("kv_error", Some("kv_result_unreadable"), None),
                ))
            }
        },
        Err(Some(e)) => {
            // A lost precondition is a VERDICT, not a fault (§8.3): counting it
            // as an error would make the most frequent outcome of the product's
            // number-one use case read as a failure on every dashboard.
            let verdict = e
                .as_db_error()
                .map(|d| d.code().code() == "23514")
                .unwrap_or(false);
            let outcome = if verdict {
                crate::metrics::KvResult::Rejected
            } else {
                crate::metrics::KvResult::Error
            };
            record_all(st, &kinds, outcome, ms);
            // The transaction aborted, so nothing was written — including on the
            // 23514 verdict path, where the RAISE is what rolled it back.
            st.quota.refund(tenant, add_rows, add_bytes, 0);
            Err(db_error_response(st, &e))
        }
        Err(None) => {
            record_all(st, &kinds, crate::metrics::KvResult::Error, ms);
            // NOT refunded, deliberately. A broker-side timeout does not say
            // whether the statement committed; the cancel is best-effort. Keeping
            // the charge over-counts for at most one refresh period, and
            // over-counting blocks early while under-counting blocks late — only
            // the second is unsafe (§9.3).
            Err(unavailable("kv_timeout"))
        }
    }
}

/// `putIfAbsent` is NOT a label of its own: it desugars to `put` with `expect:0`
/// at the entry of the SP, so it is one code path and therefore one series.
fn kv_op_label(op: Option<&str>) -> Option<crate::metrics::KvOp> {
    use crate::metrics::KvOp;
    match op? {
        "get" => Some(KvOp::Get),
        "getMany" => Some(KvOp::GetMany),
        "getPrefix" => Some(KvOp::GetPrefix),
        "put" | "putIfAbsent" => Some(KvOp::Put),
        "delete" => Some(KvOp::Delete),
        "incr" => Some(KvOp::Incr),
        _ => None,
    }
}

fn record_all(st: &AppState, kinds: &[crate::metrics::KvOp], r: crate::metrics::KvResult, ms: f64) {
    for k in kinds {
        st.metrics.kvt.kv_op(*k, r, ms);
    }
}

/// Attribute each returned element to its own op and outcome. The element's own
/// `op` field is authoritative rather than the input's: the SP is where
/// `putIfAbsent` becomes `put`, and reading the answer instead of the question
/// keeps the two from drifting.
fn record_results(st: &AppState, results: &[Value], ms: f64, bytes_in: u64) {
    use crate::metrics::KvResult;
    let mut out: u64 = 0;
    for r in results {
        let Some(op) = kv_op_label(r.get("op").and_then(|v| v.as_str())) else {
            continue;
        };
        // Writes report `applied`; reads have no predicate to lose, so a read
        // that reached the database is `applied` whether or not it found a row —
        // `found:false` is a datum, not a rejection.
        let outcome = match r.get("applied").and_then(|v| v.as_bool()) {
            Some(true) => KvResult::Applied,
            Some(false) => KvResult::Rejected,
            None => KvResult::Applied,
        };
        st.metrics.kvt.kv_op(op, outcome, ms);
        if let Some(v) = r.get("value") {
            out += v.to_string().len() as u64;
        }
        if let Some(rows) = r.get("rows").and_then(|v| v.as_array()) {
            for row in rows {
                if let Some(v) = row.get("value") {
                    out += v.to_string().len() as u64;
                }
            }
        }
    }
    st.metrics.kvt.kv_bytes(bytes_in, out);
}

/// The batch envelope. `{"results":[…]}` and not a bare array: the results are
/// index-aligned to the input (§6.4) and an object leaves room for the
/// call-level fields that quota and metering will want, without a second shape
/// change on seven clients.
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

/// Single-op routes answer with the ELEMENT, not a one-element array: the path
/// routes are sugar for the three cases people write by hand, and a batch
/// envelope would make the sugar taste of the batch.
fn single_response(results: Vec<Value>) -> Response {
    match results.into_iter().next() {
        Some(v) => json(StatusCode::OK, v.to_string()),
        None => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            err("kv_error", Some("kv_result_missing"), None),
        ),
    }
}

// ---------------------------------------------------------------------------
// POST /api/v1/kv — the complete surface (§8.1).
//
// The only route that accepts `getPrefix` and `incr`. Body is either a bare
// array of ops or `{"operations":[...]}`, the same key the transaction wire
// uses, so one shape is learned once.
// ---------------------------------------------------------------------------

pub async fn handle_kv_batch(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<Tenant>,
    body: Bytes,
) -> Response {
    let root: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return bad_request("kv_bad_body", &e.to_string()),
    };
    let ops = match &root {
        Value::Array(a) => a.clone(),
        Value::Object(o) => match o.get("operations") {
            Some(Value::Array(a)) => a.clone(),
            _ => {
                return bad_request(
                    "kv_bad_body",
                    "body must be an array of operations, or {\"operations\":[...]}",
                )
            }
        },
        _ => {
            return bad_request(
                "kv_bad_body",
                "body must be an array of operations, or {\"operations\":[...]}",
            )
        }
    };
    // A batch is a write as soon as one op writes: the bucket must charge the
    // expensive rate, or a single read in front of 255 writes buys the write
    // rate at the read price.
    let write = ops.iter().any(|o| {
        matches!(
            o.get("op").and_then(|v| v.as_str()),
            Some("put" | "putIfAbsent" | "delete" | "incr")
        )
    });
    match apply_ops(&st, tenant.as_str(), ops, write).await {
        Ok(results) => batch_response(results),
        Err(resp) => resp,
    }
}

// ---------------------------------------------------------------------------
// The three path routes.
//
// `*key` is a catch-all so `order/9f1/items` writes naturally. NO literal
// segment may ever be added under `/api/v1/kv/:ns/`, or it would make any key
// named like it unreachable — which is also why `incr` exists only in the POST
// batch (§8.1).
// ---------------------------------------------------------------------------

/// Guard the privacy rule structurally (§5.5, §13.5). This route reads no query
/// parameters at all, so ignoring them would be harmless for behaviour and
/// harmful in practice: `?prefix=quota:acme:` sent here would still be recorded
/// by every access log, proxy sample and tracing span on the way in. Rejecting
/// makes the rule enforceable instead of documentary.
fn reject_query(q: &HashMap<String, String>) -> Option<Response> {
    if q.is_empty() {
        return None;
    }
    Some(bad_request(
        "kv_no_query_string",
        "the KV path routes take no query parameters; prefix reads live only in \
         the POST /api/v1/kv body, because a prefix in a URL is recorded by every \
         access log between the client and the database",
    ))
}

/// Path-route bodies name the key in the PATH. A body that also carries `op`,
/// `ns` or `key` would be silently ignored, which is the class of silent
/// override this product refuses everywhere else (§4.2).
fn reject_path_shadowing(o: &Map<String, Value>) -> Option<Response> {
    for f in ["op", "ns", "namespace", "key"] {
        if o.contains_key(f) {
            return Some(bad_request(
                "kv_field_in_path",
                &format!("`{f}` is taken from the URL on this route and must not be in the body"),
            ));
        }
    }
    None
}

pub async fn handle_kv_get(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<Tenant>,
    Path((ns, key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if let Some(r) = reject_query(&q) {
        return r;
    }
    let op = Value::Object(Map::from_iter([
        ("op".to_string(), Value::String("get".to_string())),
        ("ns".to_string(), Value::String(ns)),
        ("key".to_string(), Value::String(key)),
    ]));
    match apply_ops(&st, tenant.as_str(), vec![op], false).await {
        Ok(results) => get_response(results),
        Err(resp) => resp,
    }
}

pub async fn handle_kv_put(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<Tenant>,
    Path((ns, key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    if let Some(r) = reject_query(&q) {
        return r;
    }
    let root: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return bad_request("kv_bad_body", &e.to_string()),
    };
    let obj = match root.as_object() {
        Some(o) => o.clone(),
        None => return bad_request("kv_bad_body", "body must be a JSON object"),
    };
    if let Some(r) = reject_path_shadowing(&obj) {
        return r;
    }
    let mut op = Map::new();
    op.insert("op".to_string(), Value::String("put".to_string()));
    op.insert("ns".to_string(), Value::String(ns));
    op.insert("key".to_string(), Value::String(key));
    // Only the fields §8.1 names for this route are forwarded. Expiry is NOT
    // defaulted here: exactly one of ttlSeconds and forever is mandatory and the
    // SP is the single place that says so, so all seven clients and the embedded
    // broker inherit the rule without a line of their own (§5.1). A `put` that
    // silently inherited the previous TTL is the fastest way to make a marker
    // immortal, so an absent expiry must reach the SP as absent.
    for f in ["value", "ttlSeconds", "forever", "expect", "required"] {
        if let Some(v) = obj.get(f) {
            op.insert(f.to_string(), v.clone());
        }
    }
    match apply_ops(&st, tenant.as_str(), vec![Value::Object(op)], true).await {
        Ok(results) => single_response(results),
        Err(resp) => resp,
    }
}

pub async fn handle_kv_delete(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<Tenant>,
    Path((ns, key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    if let Some(r) = reject_query(&q) {
        return r;
    }
    // DELETE with no body is the common case and must not be a 400; `{"expect"}`
    // is optional (§8.1).
    let obj = if body.is_empty() {
        Map::new()
    } else {
        match serde_json::from_slice::<Value>(&body) {
            Ok(Value::Object(o)) => o,
            Ok(Value::Null) => Map::new(),
            Ok(_) => return bad_request("kv_bad_body", "body must be a JSON object"),
            Err(e) => return bad_request("kv_bad_body", &e.to_string()),
        }
    };
    if let Some(r) = reject_path_shadowing(&obj) {
        return r;
    }
    let mut op = Map::new();
    op.insert("op".to_string(), Value::String("delete".to_string()));
    op.insert("ns".to_string(), Value::String(ns));
    op.insert("key".to_string(), Value::String(key));
    for f in ["expect", "required"] {
        if let Some(v) = obj.get(f) {
            op.insert(f.to_string(), v.clone());
        }
    }
    match apply_ops(&st, tenant.as_str(), vec![Value::Object(op)], true).await {
        Ok(results) => single_response(results),
        Err(resp) => resp,
    }
}

/// The GET answer: the element, plus `ETag: "<version>"` when the key was found.
///
/// A miss is 200 with `{"found":false}` — the one status-code rule (§8.1) — and
/// carries no ETag, because there is no version to name.
///
/// ETag YES, If-Match NO (§8.1). The response header is free and informative; a
/// conditional write goes through `expect` in the body and nowhere else, so
/// there is exactly one way to express a precondition. And it must be said in
/// the documentation that the ETag saves BANDWIDTH, not the round trip to the
/// database — there is no cache in front of this and there will not be (§8.5).
fn get_response(results: Vec<Value>) -> Response {
    let elem = match results.into_iter().next() {
        Some(v) => v,
        None => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                err("kv_error", Some("kv_result_missing"), None),
            )
        }
    };
    let etag = elem
        .get("version")
        .and_then(|x| x.as_i64())
        .filter(|_| elem.get("found").and_then(|f| f.as_bool()) == Some(true))
        .map(|ver| format!("\"{ver}\""));
    match etag {
        Some(tag) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/json".to_string()),
                (header::ETAG, tag),
            ],
            elem.to_string(),
        )
            .into_response(),
        None => json(StatusCode::OK, elem.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The two rules of this file that are cheap to break and silent when broken.

    #[test]
    fn path_routes_refuse_body_fields_that_the_url_already_names() {
        let mut m = Map::new();
        m.insert("key".to_string(), Value::String("other".to_string()));
        assert!(reject_path_shadowing(&m).is_some());
        let mut ok = Map::new();
        ok.insert("value".to_string(), Value::Bool(true));
        assert!(reject_path_shadowing(&ok).is_none());
    }

    #[test]
    fn path_routes_refuse_any_query_string() {
        let mut q = HashMap::new();
        q.insert("prefix".to_string(), "quota:acme:".to_string());
        assert!(reject_query(&q).is_some());
        assert!(reject_query(&HashMap::new()).is_none());
    }

    // A truncated DETAIL must degrade to the bare verdict, never to a 500: the
    // lost precondition is the expected outcome of a legitimate redelivery.
    #[test]
    fn a_truncated_precondition_detail_still_answers_200() {
        let r = precondition_200(Some("{\"index\":0,\"value\":{\"a\":"));
        assert_eq!(r.status(), StatusCode::OK);
    }

    /// The four routes of §8.1, built exactly as main.rs must build them.
    ///
    /// This is a TYPE CHECK plus a router check, and it is here rather than in
    /// main.rs for one reason: the routes cannot be exported as a sub-router to
    /// be merged, because `webdoc/scripts/gen-routes.mjs` derives the public
    /// route reference by regexing main.rs's builder chain for `.route("…",
    /// verb(handler))`. Anything merged from elsewhere disappears from the
    /// documentation silently. So main.rs owns the registration and this owns
    /// the proof that these handlers can be registered — a signature that does
    /// not satisfy axum's `Handler` fails HERE, in this file's own suite, and
    /// not in a stranger's build.
    ///
    /// The static path is declared before the parametric one, which is the rule
    /// main.rs repeats four times, and the catch-all `*key` is what lets
    /// `order/9f1/items` be written naturally. Building the router also proves
    /// the two do not conflict.
    #[test]
    fn the_four_routes_accept_these_handlers() {
        use axum::routing::{get, post};
        let _: axum::Router<Arc<AppState>> = axum::Router::new()
            .route("/api/v1/kv", post(handle_kv_batch))
            .route(
                "/api/v1/kv/:ns/*key",
                get(handle_kv_get).put(handle_kv_put).delete(handle_kv_delete),
            );
    }

    /// WHAT THE CATCH-ALL ACTUALLY HANDS THE HANDLER.
    ///
    /// The whole key namespace of this product rides on this one answer: if
    /// `*key` arrived with a leading slash, every key written through the path
    /// routes would be stored under a different name than the same key written
    /// through the POST batch, and nothing would ever error — the two surfaces
    /// would simply address different rows. It cannot be settled by reading the
    /// handler, so it is settled by a real request through a real router.
    ///
    /// It also pins the two decisions that would otherwise be discovered in
    /// production: a key MAY contain slashes (`order/9f1/items` writes
    /// naturally, which is why the segment is a catch-all at all), and percent
    /// escapes are decoded once by the extractor, so a key with a literal `/`
    /// in it is expressible as `%2F`.
    // axum::serve lives behind the `server` feature (axum/tokio); the lean
    // embedded build (--no-default-features) has no serve stack, so this
    // routing proof only compiles with it. Caught by CI's server-msrv-and-lean
    // on 2026-08-25 — the first lean compile after the test was added.
    #[cfg(feature = "server")]
    #[tokio::test]
    async fn the_catch_all_key_arrives_without_a_leading_slash() {
        use axum::routing::get;

        async fn probe(Path((ns, key)): Path<(String, String)>) -> String {
            format!("{ns}|{key}")
        }
        let app = axum::Router::new()
            .route("/api/v1/kv/:ns/*key", get(probe))
            .route("/api/v1/timers/:queue/*timerKey", get(probe));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        assert_eq!(get_body(addr, "/api/v1/kv/orders/order/9f1/items").await, "orders|order/9f1/items");
        assert_eq!(get_body(addr, "/api/v1/kv/orders/simple").await, "orders|simple");
        // Percent-decoding happens in the extractor, once: a key that really
        // contains a slash is expressible and does not become two segments.
        assert_eq!(get_body(addr, "/api/v1/kv/orders/a%2Fb").await, "orders|a/b");
        assert_eq!(get_body(addr, "/api/v1/timers/q/tenant/42").await, "q|tenant/42");

        server.abort();
    }

    /// Minimal HTTP/1.1 client: the crate has no dev-dependencies and pulling
    /// one in for four assertions is not worth a Cargo.toml change in a file
    /// this phase does not own.
    #[cfg(test)]
    async fn get_body(addr: std::net::SocketAddr, path: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.write_all(format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut buf = String::new();
        s.read_to_string(&mut buf).await.unwrap();
        buf.split_once("\r\n\r\n").map(|(_, b)| b.to_string()).unwrap_or_default()
    }
}
