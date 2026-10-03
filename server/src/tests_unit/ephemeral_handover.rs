//! The ephemeral hand-over between raft nodes over real HTTP — §3.7 across
//! raft nodes, the half `ephemeral_engine.rs` cannot reach: the auth layer, the
//! peer client, the retries.
//!
//! Two brokers on localhost, each its own state and router with JWT auth ON,
//! sharing the cluster token (`peerclient::test_token`). What it pins:
//!
//! - a SIGTERM drain hands every ring over — delivered = rings, lost = 0 —
//!   and the leaving notice lands: the calls between brokers prove themselves
//!   with the cluster token, never a client's JWT (they got 401 before,
//!   2026-10-02); the forward mark without the token proves nothing;
//! - a hand-over to a peer whose listener is not up yet (raft hears a restarted
//!   node first) waits for it and lands, where it was dropped on the first
//!   refused connection (two prod rolls, 2026-10-02);
//! - a hand-over whose peer drops out while it waits comes back to this node;
//! - the readiness probe a peer must pass before it takes partitions.
//!
//! The module is a child of `handlers/ephemeral.rs` (`#[path]`, resolved from
//! `src/handlers/`), so `use super::*` reaches its private items.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::http::{HeaderName, HeaderValue, Method, StatusCode};

use super::*;
use crate::config::DEFAULT_TENANT;
use crate::ephemeral::{Placement, Probe};
use crate::handlers::AppState;
use crate::peerclient::{FWD_HEADER, TOKEN_HEADER};

const SECRET: &str = "ephemeral-handover-test-secret";

/// The broker's JWT auth, on: HS256 over [`SECRET`], nothing skipped.
fn jwt_on() -> crate::config::AuthConfig {
    let mut a = crate::config::load().auth;
    a.enabled = true;
    a.algorithm = "HS256".into();
    a.secret = SECRET.into();
    a.public_key.clear();
    a.jwks_url.clear();
    a.issuer.clear();
    a.audience.clear();
    a.skip_paths = Vec::new();
    a
}

/// One broker's state, over the state machine's stub: the ephemeral rings
/// are all this file needs, and they are node RAM.
fn node() -> Arc<AppState> {
    let mut cfg = crate::config::load();
    cfg.ephemeral_require_grant = false;
    cfg.tenancy_header = false;
    let stub: Arc<dyn crate::rsm::facade::Rsm> = Arc::new(crate::rsm::facade::NotReady::new());
    crate::handlers::raft::build_raft_state_with(&cfg, Some(stub)).expect("state")
}

/// Serve `st`'s router — JWT on — on `listener`; its base URL.
fn serve_on(st: &Arc<AppState>, listener: tokio::net::TcpListener) -> String {
    let addr = listener.local_addr().expect("addr");
    let router = crate::handlers::raft::build_raft_router(
        st.clone(),
        crate::auth::Authenticator::new(jwt_on()),
        false,
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    format!("http://{addr}")
}

async fn serve(st: &Arc<AppState>) -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    serve_on(st, l)
}

/// A port nothing listens on yet.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

/// A placement that gives every partition to one peer at `addr`, as a node
/// that is leaving sees its only peer.
fn to_peer(me: &str, peer: &str, addr: &str) -> Placement {
    Placement {
        me: me.to_string(),
        nodes: vec![peer.to_string()],
        addrs: [(peer.to_string(), addr.to_string())].into(),
    }
}

fn msgs(n: usize, tag: &str) -> Vec<Box<[u8]>> {
    (0..n)
        .map(|i| {
            format!("{{\"m\":\"{tag}-{i}\"}}")
                .into_bytes()
                .into_boxed_slice()
        })
        .collect()
}

/// Every message the queues `q0..qn` hold here.
fn held(st: &AppState, queues: usize) -> i64 {
    (0..queues)
        .map(|q| {
            st.ephemeral
                .depth(DEFAULT_TENANT, &format!("q{q}"))
                .map_or(0, |d| d.0)
        })
        .sum()
}

/// A client's own credential: a read-write JWT, valid for an hour.
fn client_jwt() -> String {
    let exp = crate::util::now_epoch_ms() / 1000 + 3600;
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &serde_json::json!({"sub": "a-client", "role": "read-write", "exp": exp}),
        &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .expect("jwt")
}

fn header(k: &str, v: &str) -> (HeaderName, HeaderValue) {
    (
        HeaderName::from_bytes(k.as_bytes()).expect("name"),
        HeaderValue::from_str(v).expect("value"),
    )
}

async fn post(
    st: &AppState,
    url: &str,
    headers: &[(HeaderName, HeaderValue)],
    body: &str,
) -> StatusCode {
    st.peers
        .call(
            Method::POST,
            url,
            headers,
            Bytes::from(body.to_string()),
            Duration::from_secs(5),
        )
        .await
        .expect("answer")
        .status
}

/// With JWT auth on, SIGTERM hands every ring to the peer: the leaving notice
/// and the hand-over pass the peer's auth on the cluster token, every ring is
/// delivered and none lost, and a group resumes where it was. Without the
/// token the same calls are refused: a client's JWT, or the mark alone, is not
/// a broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_jwt_on_a_sigterm_hand_over_delivers_every_message() {
    let token = crate::peerclient::test_token();
    let (a, b) = (node(), node());
    let base_b = serve(&b).await;
    let now = crate::util::now_epoch_ms();
    for q in 0..5 {
        for p in 0..4 {
            a.ephemeral
                .push(
                    DEFAULT_TENANT,
                    &format!("q{q}"),
                    &format!("p{p}"),
                    msgs(10, &format!("q{q}p{p}")),
                    now,
                )
                .expect("push");
        }
    }
    // A group part-way through one ring: its position travels with the ring.
    assert_eq!(
        a.ephemeral
            .pop(DEFAULT_TENANT, "q0", Some("p0"), Some("g"), 3, true, now)
            .len(),
        3
    );
    let before = held(&a, 5);
    assert!(before >= 197);
    a.ephemeral.set_placement(Some(to_peer("1", "2", &base_b)));

    // The calls between brokers, as a client would have to make them.
    let adopt = format!("{base_b}/api/v1/ephemeral/_adopt");
    let ring = "{\"rings\":[]}";
    assert_eq!(
        post(&a, &adopt, &[header(FWD_HEADER, "1")], ring).await,
        StatusCode::UNAUTHORIZED,
        "the forward mark alone is not a credential"
    );
    assert_eq!(
        post(
            &a,
            &adopt,
            &[
                header(FWD_HEADER, "1"),
                header(TOKEN_HEADER, "not-the-token")
            ],
            ring
        )
        .await,
        StatusCode::UNAUTHORIZED,
        "nor is a wrong token"
    );
    let bearer = format!("Bearer {}", client_jwt());
    assert_eq!(
        post(
            &a,
            &adopt,
            &[header(FWD_HEADER, "1"), header("authorization", &bearer)],
            ring
        )
        .await,
        StatusCode::FORBIDDEN,
        "a client's JWT passes auth, and the route is still the brokers'"
    );
    assert_eq!(
        post(
            &a,
            &format!("{base_b}/api/v1/ephemeral/push"),
            &[header(FWD_HEADER, "1"), header(TOKEN_HEADER, token)],
            "{\"queue\":\"x\",\"messages\":[{\"payload\":1}]}"
        )
        .await,
        StatusCode::UNAUTHORIZED,
        "the token opens the three calls between brokers, not a client's route"
    );

    let d = ephemeral_drain(&a).await;
    assert_eq!((d.peers, d.told), (1, 1), "B took the leaving notice");
    assert_eq!(d.rings, 20);
    assert_eq!(
        (d.shipped.delivered, d.shipped.lost, d.shipped.kept),
        (20, 0, 0),
        "delivered = every ring, lost = 0"
    );
    assert_eq!(d.shipped.messages as i64, before);
    assert_eq!(d.still_shipping, 0);
    assert_eq!(held(&a, 5), 0, "nothing left behind");
    assert_eq!(a.ephemeral.global_bytes(), 0);
    assert_eq!(held(&b, 5), before, "every message is on B");

    let g = b
        .ephemeral
        .pop(DEFAULT_TENANT, "q0", Some("p0"), Some("g"), 100, true, now);
    assert_eq!(g.len(), 7);
    assert_eq!(
        &*g[0].payload, b"{\"m\":\"q0p0-3\"}",
        "g resumes at its fourth"
    );
    let other = b
        .ephemeral
        .pop(DEFAULT_TENANT, "q4", Some("p3"), Some("h"), 100, true, now);
    assert_eq!(other.len(), 10);

    // B knows A is leaving: A is out of B's placement until another A answers.
    assert_eq!(b.ephemeral.excluded_peers(now).len(), 1);
    // And a leaving A takes nothing back: B's hand-overs go elsewhere.
    let base_a = serve(&a).await;
    assert_eq!(
        post(
            &b,
            &format!("{base_a}/api/v1/ephemeral/_adopt"),
            &crate::peerclient::internal_headers(),
            ring
        )
        .await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(probe(&b, "1", &base_a).await, Probe::NotReady);
}

/// A restarted node answers raft before its HTTP listener is up. A hand-over
/// sent into that gap is tried again until the listener answers, and lands —
/// nothing dropped. A single attempt (a relayed request's) puts what did not
/// land back in the queue rather than dropping it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hand_over_to_a_peer_still_starting_waits_and_delivers() {
    crate::peerclient::test_token();
    let (a, b) = (node(), node());
    let port = free_port();
    let base_b = format!("http://127.0.0.1:{port}");
    let now = crate::util::now_epoch_ms();
    for q in 0..3 {
        for p in 0..2 {
            a.ephemeral
                .push(
                    DEFAULT_TENANT,
                    &format!("q{q}"),
                    &format!("p{p}"),
                    msgs(5, &format!("q{q}p{p}")),
                    now,
                )
                .expect("push");
        }
    }
    a.ephemeral.set_placement(Some(to_peer("1", "2", &base_b)));
    assert_eq!(a.ephemeral.reap_foreign(), 6);

    // Nobody listens yet: the probe says so, and one attempt drops nothing.
    assert_eq!(probe(&a, "2", &base_b).await, Probe::NotReady);
    assert_eq!(ship(&a, a.ephemeral.take_outbox()).await, (0, 0));
    let out = a.ephemeral.take_outbox();
    assert_eq!(out.len(), 6, "back in the queue, not dropped");

    // B starts listening 1.5 s into the hand-over.
    let b2 = b.clone();
    let starting = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        let l = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("bind B's port");
        serve_on(&b2, l)
    });
    let t0 = Instant::now();
    let s = ship_until(&a, out, Instant::now() + Duration::from_secs(20)).await;
    assert!(
        t0.elapsed() >= Duration::from_millis(1_400),
        "it waited for B's listener ({:?})",
        t0.elapsed()
    );
    assert_eq!(
        (s.delivered, s.lost, s.kept, s.messages),
        (6, 0, 0, 30),
        "delivered = every ring, lost = 0"
    );
    assert_eq!(starting.await.expect("B"), base_b);
    assert_eq!(held(&b, 3), 30);
    assert_eq!(held(&a, 3), 0);
    assert_eq!(a.ephemeral.shipping(), 0);

    // Now B answers the probe with its incarnation: it takes partitions.
    assert_eq!(
        probe(&a, "2", &base_b).await,
        Probe::Ready(Some(b.ephemeral.epoch()))
    );
}

/// A hand-over waiting on a peer that drops out of the placement comes back to
/// this node when the partition is its own again: adopted here, not dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hand_over_whose_peer_drops_out_comes_back_here() {
    crate::peerclient::test_token();
    let a = node();
    let base_dead = format!("http://127.0.0.1:{}", free_port());
    let now = crate::util::now_epoch_ms();
    a.ephemeral
        .push(DEFAULT_TENANT, "q0", "p0", msgs(4, "q0p0"), now)
        .expect("push");
    a.ephemeral
        .push(DEFAULT_TENANT, "q1", "p0", msgs(6, "q1p0"), now)
        .expect("push");
    a.ephemeral
        .set_placement(Some(to_peer("1", "2", &base_dead)));
    assert_eq!(a.ephemeral.reap_foreign(), 2);
    let out = a.ephemeral.take_outbox();
    assert_eq!(held(&a, 2), 0);

    let a2 = a.clone();
    let shipping = tokio::spawn(async move {
        ship_until(&a2, out, Instant::now() + Duration::from_secs(20)).await
    });
    // The peer is judged down while the hand-over retries.
    tokio::time::sleep(Duration::from_millis(600)).await;
    a.ephemeral.set_placement(None);
    let s = shipping.await.expect("ship");
    assert_eq!((s.delivered, s.lost, s.kept), (0, 0, 2));
    assert_eq!(held(&a, 2), 10, "every message is back here");
    let got = a
        .ephemeral
        .pop(DEFAULT_TENANT, "q0", Some("p0"), None, 10, true, now);
    assert_eq!(got.len(), 4);
}

/// A whole cluster stopping (`docker compose down`): every peer answers the
/// hand-over 503 `leaving`. The drain takes that peer out at once and, with
/// nobody left to take the rings, drops them — what any stop of every node
/// costs this class — instead of retrying for its whole bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn when_every_peer_is_leaving_the_drain_does_not_wait_out_its_bound() {
    crate::peerclient::test_token();
    let (a, b) = (node(), node());
    let base_b = serve(&b).await;
    let now = crate::util::now_epoch_ms();
    a.ephemeral
        .push(DEFAULT_TENANT, "q0", "p0", msgs(3, "q0p0"), now)
        .expect("push");
    a.ephemeral.set_placement(Some(to_peer("1", "2", &base_b)));
    b.ephemeral.set_leaving();

    let t0 = Instant::now();
    let d = ephemeral_drain(&a).await;
    assert!(
        t0.elapsed() < Duration::from_secs(3),
        "the drain gave up at once ({:?})",
        t0.elapsed()
    );
    assert_eq!((d.told, d.rings), (1, 1));
    assert_eq!(
        (d.shipped.delivered, d.shipped.lost, d.shipped.kept),
        (0, 1, 0)
    );
    assert!(a.ephemeral.excluded_peers(now).contains("2"));
}
