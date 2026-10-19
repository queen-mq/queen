//! The ephemeral long poll's lost wake-up (found 2026-10-11; on the released
//! 2.5.0, 2026-10-19, about one reply in 950 reached its requester only at
//! the pop's timeout when every request had its own inbox).
//!
//! A waiting pop looked at its rings and only then took its queue's gate and
//! armed its wake. A push landing between the two found no gate, or a gate
//! nobody was armed on (`notify_waiters` keeps no permit), and the pop slept
//! to its timeout with the message already there. What it pins: a push
//! landing in exactly that gap, run by [`TEST_BETWEEN_LOOK_AND_PARK`], is
//! answered at once, on an inbox no pop has parked on (no gate yet) and on one
//! a pop has parked on before (the gate exists).
//!
//! The module is a child of `handlers/ephemeral.rs` (`#[path]`, resolved from
//! `src/handlers/`), so `use super::*` reaches its private items.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Extension, Query, State};
use axum::http::{HeaderMap, Uri};

use super::*;
use crate::auth::AuthedSub;
use crate::config::DEFAULT_TENANT;
use crate::handlers::AppState;
use crate::tenant::Tenant;

/// The pop's timeout: a pop that slept through the push answers after it.
const POP_TIMEOUT_MS: u64 = 3_000;

/// One broker's state, over the state machine's stub: the ephemeral rings
/// are all this file needs, and they are node RAM.
fn node() -> Arc<AppState> {
    let mut cfg = crate::config::load();
    cfg.ephemeral_require_grant = false;
    cfg.tenancy_header = false;
    let stub: Arc<dyn crate::rsm::facade::Rsm> = Arc::new(crate::rsm::facade::NotReady::new());
    crate::handlers::raft::build_raft_state_with(&cfg, Some(stub)).expect("state")
}

/// What the push handler does once the owner takes a message: into the
/// rings, then the wake.
fn push(st: &AppState, queue: &str) {
    st.ephemeral
        .push(
            DEFAULT_TENANT,
            queue,
            "Default",
            vec![b"{\"n\":1}".to_vec().into_boxed_slice()],
            crate::util::now_epoch_ms(),
        )
        .expect("push");
    st.notifier.notify_pushed_batch(&[(
        ephemeral::Ephemeral::qkey(DEFAULT_TENANT, queue),
        "Default".to_string(),
    )]);
}

/// A waiting pop of `queue`, committing at delivery: the messages it
/// answered with, and how long it took.
async fn pop(st: &Arc<AppState>, queue: &str) -> (usize, Duration) {
    let uri: Uri = format!(
        "/api/v1/ephemeral/pop?queue={queue}&wait=true&timeout={POP_TIMEOUT_MS}&commitOnDelivery=true"
    )
    .parse()
    .expect("uri");
    let q: Query<HashMap<String, String>> = Query::try_from_uri(&uri).expect("query");
    let t0 = Instant::now();
    let resp = handle_ephemeral_pop(
        State(st.clone()),
        Extension(AuthedSub(None)),
        Extension(Tenant::default_tenant()),
        HeaderMap::new(),
        uri,
        q,
    )
    .await;
    let took = t0.elapsed();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    let v: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    (v["messages"].as_array().map_or(0, Vec::len), took)
}

#[tokio::test]
async fn a_push_between_the_look_and_the_park_wakes_the_pop() {
    let st = node();
    // An inbox a pop has parked on before: its gate exists.
    let (n, _) = tokio::join!(pop(&st, "inbox-used"), async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        push(&st, "inbox-used");
    })
    .0;
    assert_eq!(n, 1, "the warm-up pop gets its message");

    for queue in ["inbox-new", "inbox-used"] {
        let st2 = st.clone();
        TEST_BETWEEN_LOOK_AND_PARK.with(|h| {
            *h.borrow_mut() = Some(Box::new(move || push(&st2, queue)));
        });
        let (n, took) = pop(&st, queue).await;
        assert_eq!(n, 1, "{queue}: the message pushed in the gap");
        assert!(
            took < Duration::from_millis(POP_TIMEOUT_MS / 3),
            "{queue}: answered after {took:?}, the pop slept through the push to its timeout"
        );
    }
}
