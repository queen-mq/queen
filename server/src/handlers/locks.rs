//! `POST /api/v1/locks` — the locks surface (`crate::locks`): acquire, renew,
//! release and get, for a lock and for a semaphore.
//!
//! THE STATUS RULE IS KV'S (`handlers/kv.rs`): the status describes the
//! outcome of the CALL, never the verdict of an operation. A lock somebody
//! else holds, a token that is no longer the row's, a release of what was not
//! held — all 200, with an explicit field (`acquired`, `renewed`, `released`).
//! `acquired: false` is the most frequent answer a waiter ever gets, and a 4xx
//! would put it inside the retry policy and the error metrics of every client.
//!
//! THE LADDER IS KV'S TOO, on purpose and in the same one call: a permit is a
//! KV row, so the operator's KV switch, the tenant's grant, its write and read
//! rates and its occupancy all apply here as they do to the `putIfAbsent` this
//! route sends. A lock is not a way around a quota a key would meet.
//!
//! A FAILED CALL (a 503) MAY HAVE APPLIED SOME OF ITS OPERATIONS: it is
//! several KV calls, not one. Send it again. With their `owner` an acquire
//! and a renew answer the permit the first attempt took, and a release of a
//! permit already released answers `released: false`, which is what the
//! caller wanted.
//!
//! Nothing in this file touches the store. It builds no row and judges no
//! precondition: `crate::locks` turns the call into KV operations, and the
//! state machine answers them as it answers anybody's.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Response;
use serde_json::Value;

use super::kv::{batch_response, err, gated, json_retry, raft_failure};
use super::{json, AppState};
use crate::locks::{self, LockOp};
use crate::switches::Surface;
use crate::tenant::Tenant;

fn bad_request(reason: &str, detail: &str) -> Response {
    json(
        StatusCode::BAD_REQUEST,
        err("locks_bad_request", Some(reason), Some(detail)),
    )
}

fn metric_of(op: &LockOp) -> crate::metrics::LockOp {
    use crate::metrics::LockOp as M;
    match op {
        LockOp::Acquire { .. } => M::Acquire,
        LockOp::Renew { .. } => M::Renew,
        LockOp::Release { .. } => M::Release,
        LockOp::Get { .. } => M::Get,
    }
}

/// Whether the operation did what it asked. A `get` always did.
fn granted(result: &Value) -> bool {
    ["acquired", "renewed", "released"]
        .iter()
        .find_map(|flag| result.get(*flag).and_then(Value::as_bool))
        .unwrap_or(true)
}

/// What the answers show was NOT added, of what the ladder charged before
/// the call ([`locks::footprint`]): an acquire that was refused, or that
/// answered a permit its owner already had, wrote no row.
fn not_added(ops: &[LockOp], results: &[Value]) -> (i64, i64) {
    let mut back = (0i64, 0i64);
    for (op, r) in ops.iter().zip(results) {
        let new_row = r["acquired"] == Value::Bool(true) && r.get("already").is_none();
        if matches!(op, LockOp::Acquire { .. }) && !new_row {
            let (rows, bytes) = locks::footprint(std::slice::from_ref(op));
            back = (back.0 + rows, back.1 + bytes);
        }
    }
    back
}

/// A KV refusal of this route's own calls, in this route's envelope where it
/// is about the caller's request — a name too long for the store's key, once
/// the tenant and the namespace are in front of it — and KV's own answer
/// everywhere else (a timeout, no leader, a full disk).
fn failure(f: crate::rsm::facade::KvFailure) -> Response {
    use crate::rsm::facade::KvFailure;
    match f {
        KvFailure::Invalid {
            status: 413,
            reason,
            detail,
        } => json(
            StatusCode::PAYLOAD_TOO_LARGE,
            err("payload_too_large", Some(&reason), Some(&detail)),
        ),
        KvFailure::Invalid { reason, detail, .. } => bad_request(&reason, &detail),
        // A lock operation sends no `required` op, so no precondition aborts
        // one: a lost race is a verdict of its own answer. Should it ever,
        // the caller is told to come back, never that it holds something.
        KvFailure::Precondition { .. } => json_retry(
            StatusCode::SERVICE_UNAVAILABLE,
            err("kv_unavailable", Some("locks_precondition"), None),
            1,
        ),
        other => raft_failure(other),
    }
}

/// `POST /api/v1/locks`. The body is an array of operations or
/// `{"operations":[...]}`, as on the KV and timers routes; the answer is
/// `{"results":[...]}`, one per operation, in order.
pub async fn handle_locks_batch(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<Tenant>,
    body: Bytes,
) -> Response {
    const SHAPE: &str = "body must be an array of operations, or {\"operations\":[...]}";
    let root: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return bad_request("locks_bad_body", &e.to_string()),
    };
    let raw = match &root {
        Value::Array(a) => a,
        Value::Object(o) => match o.get("operations") {
            Some(Value::Array(a)) => a,
            _ => return bad_request("locks_bad_body", SHAPE),
        },
        _ => return bad_request("locks_bad_body", SHAPE),
    };
    let ops = match locks::parse_ops(raw) {
        Ok(ops) => ops,
        Err(e) => return bad_request(e.reason, &e.detail),
    };
    if ops.is_empty() {
        return batch_response(Vec::new());
    }

    // The ladder, before anything is sent (kv.rs §8.4 point 3). A call is a
    // write as soon as one operation is: a `get` in front of 63 acquires must
    // not buy the write rate at the read price.
    let write = ops.iter().any(LockOp::is_write);
    let (rows, bytes) = if write {
        locks::footprint(&ops)
    } else {
        (0, 0)
    };
    let surface = if write {
        Surface::KvWrite
    } else {
        Surface::KvRead
    };
    if let Some(resp) = gated(&st, tenant.as_str(), surface, rows, bytes) {
        return resp;
    }

    use crate::metrics::KvResult;
    use crate::rsm::facade::Deadline;
    let t0 = std::time::Instant::now();
    let res = locks::apply(
        st.rsm.as_ref(),
        tenant.as_str(),
        Deadline::after(st.stmt_timeout),
        &ops,
    )
    .await;
    // One duration per call, shared out over its operations (as KV does).
    let ms = t0.elapsed().as_secs_f64() * 1000.0 / ops.len() as f64;
    match res {
        Ok(results) => {
            for (op, r) in ops.iter().zip(&results) {
                let outcome = if granted(r) {
                    KvResult::Applied
                } else {
                    KvResult::Rejected
                };
                st.metrics.kvt.lock_op(metric_of(op), outcome, ms);
            }
            let (back_rows, back_bytes) = not_added(&ops, &results);
            st.quota.refund(tenant.as_str(), back_rows, back_bytes, 0);
            batch_response(results)
        }
        Err(failed) => {
            for op in &ops {
                st.metrics.kvt.lock_op(metric_of(op), KvResult::Error, ms);
            }
            // The charge comes back only when nothing can have been written:
            // over-counting blocks early where under-counting blocks late.
            if !failed.wrote {
                st.quota.refund(tenant.as_str(), rows, bytes, 0);
            }
            failure(failed.failure)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ops(v: Value) -> Vec<LockOp> {
        locks::parse_ops(v.as_array().unwrap()).unwrap()
    }

    #[test]
    fn a_verdict_is_read_off_its_own_flag_and_a_get_always_did() {
        assert!(granted(&json!({"op":"acquire","acquired":true})));
        assert!(!granted(
            &json!({"op":"acquire","acquired":false,"reason":"held"})
        ));
        assert!(!granted(&json!({"op":"renew","renewed":false})));
        assert!(granted(&json!({"op":"release","released":true})));
        assert!(granted(&json!({"op":"get","held":false})));
    }

    /// The occupancy the ladder charged comes back for every acquire that
    /// wrote no row, so a waiter polling a held lock never eats the quota.
    #[test]
    fn the_charge_of_an_acquire_that_added_no_row_comes_back() {
        let call = ops(json!([
            {"op":"acquire","name":"a","ttlSeconds":1,"owner":"w"},
            {"op":"acquire","name":"b","ttlSeconds":1,"owner":"w"},
            {"op":"acquire","name":"c","ttlSeconds":1,"owner":"w"},
            {"op":"renew","name":"d","token":1,"ttlSeconds":1},
        ]));
        let results = vec![
            json!({"acquired":true}),
            json!({"acquired":false,"reason":"held"}),
            json!({"acquired":true,"already":true}),
            json!({"renewed":true}),
        ];
        let one = r#"{"owner":"w"}"#.len() as i64;
        assert_eq!(locks::footprint(&call), (3, 3 * one));
        assert_eq!(not_added(&call, &results), (2, 2 * one));
    }

    /// The route, built as `handlers/raft.rs` builds it (the route reference
    /// is derived from that builder chain, so the registration lives there):
    /// this is the proof the signature satisfies axum's `Handler`.
    #[test]
    fn the_route_accepts_this_handler() {
        use axum::routing::post;
        let _: axum::Router<Arc<AppState>> =
            axum::Router::new().route("/api/v1/locks", post(handle_locks_batch));
    }
}
