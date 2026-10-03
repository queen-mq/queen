//! The single binary (PLAN_SINGLE_BINARY.md W3/W4): the proxy runs inside the
//! broker, one process per node.
//!
//! - **State:** the proxy's tables live in this broker's replicated KV under
//!   the proxy's system tenant ([`queen_proxy::store::schema::PROXY_TENANT`]),
//!   through [`RsmKv`] — the same KV pipeline every client uses.
//! - **Data plane:** the proxy authenticates the caller (API key, session),
//!   applies the plan's limits and meters the request, then hands it to the
//!   broker router IN-PROCESS with the cluster's broker tenant in
//!   `x-queen-tenant`. That inner router is built with tenancy on and the
//!   broker's own JWT off, and is bound to no socket: the proxy is the only
//!   way in.
//! - **Public port:** serves the proxy router (console, OAuth, operator,
//!   dashboard, data plane). `/health` passes straight to the broker for
//!   probes; `/metrics*` only with the control-plane token.
//! - **Internal port (optional, [`separate_port`]):** with `QUEEN_PROXY_PORT`
//!   set to a port other than `PORT`, the proxy serves `QUEEN_PROXY_PORT` and
//!   `PORT` serves the broker router itself, exactly as a broker without the
//!   proxy does (its own JWT and tenancy settings): for clients inside the
//!   network, scrapers and probes. That port must not face the internet.
//!
//! On with `QUEEN_PROXY_EMBEDDED=true`; configured by the `QUEEN_PROXY_*`
//! environment.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use serde_json::Value;

use queen_proxy::store::kv::{BoxFut, KvBackend, KvError};
use queen_proxy::store::schema::PROXY_TENANT;

use crate::rsm::facade::{Deadline, KvFailure, KvReq, ReqCtx, Rsm};

/// `QUEEN_PROXY_EMBEDDED`: run the proxy in this process (raft mode).
pub fn enabled() -> bool {
    matches!(
        std::env::var("QUEEN_PROXY_EMBEDDED")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// `QUEEN_PROXY_PORT`, when the proxy gets a port of its own: set and not
/// `broker_port` (the broker's `PORT`). `None`: the proxy fronts `PORT` and
/// the broker router has no socket.
pub fn separate_port(broker_port: &str) -> Option<String> {
    separate_port_of(
        std::env::var("QUEEN_PROXY_PORT").ok().as_deref(),
        broker_port,
    )
}

fn separate_port_of(proxy_port: Option<&str>, broker_port: &str) -> Option<String> {
    let port = proxy_port?.trim();
    (!port.is_empty() && port != broker_port.trim()).then(|| port.to_string())
}

/// The proxy's store: this broker's KV, for the proxy's system tenant.
pub struct RsmKv {
    rsm: Arc<dyn Rsm>,
}

impl RsmKv {
    pub fn new(rsm: Arc<dyn Rsm>) -> RsmKv {
        RsmKv { rsm }
    }
}

/// How long one proxy KV batch may take (a write waits for its commit).
const KV_DEADLINE: Duration = Duration::from_secs(10);

impl KvBackend for RsmKv {
    fn kv(&self, ops: Vec<Value>) -> BoxFut<'_, Result<Vec<Value>, KvError>> {
        Box::pin(async move {
            let ctx = ReqCtx::new(PROXY_TENANT, Deadline::after(KV_DEADLINE));
            match self.rsm.kv(ctx, KvReq { ops }).await {
                Ok(out) => Ok(out.results),
                Err(KvFailure::Invalid {
                    status,
                    reason,
                    detail,
                }) => Err(KvError::Invalid {
                    status,
                    reason,
                    detail,
                }),
                Err(KvFailure::Precondition { detail }) => Err(KvError::Precondition {
                    detail: serde_json::from_str(&detail).unwrap_or(Value::String(detail)),
                }),
                Err(KvFailure::Rsm(e)) => Err(KvError::Unavailable(e.to_string())),
            }
        })
    }
}

/// The S3 sink's side of the control plane (`/api/cp/clusters/:slug/s3`):
/// offered only where this node runs the sink (`QUEEN_S3_EMBEDDED=true` in a
/// binary built with it), so a cell that does not answers `s3_unavailable`
/// rather than storing a tenant sink nothing would ever run.
fn s3_hooks() -> Option<Arc<dyn queen_proxy::s3::S3Sinks>> {
    #[cfg(feature = "s3")]
    if crate::config::S3SinkConfig::from_env().enabled {
        return Some(Arc::new(crate::s3_inproc::ControlPlaneHooks::new()));
    }
    None
}

/// The public router of a single-binary node: the proxy in front of
/// `broker` (the inner broker router: tenancy on, broker JWT off, no socket).
pub fn public_router(
    rsm: Arc<dyn Rsm>,
    broker: Router,
) -> Result<(queen_proxy::state::St, Router), String> {
    let (st, proxy) = queen_proxy::app::build_embedded(queen_proxy::app::Embedded {
        kv: Arc::new(RsmKv::new(rsm)),
        broker: broker.clone(),
        node: crate::rsm::dashboard::node_label(
            std::env::var("QUEEN_RAFT_NODE_ID")
                .ok()
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(1),
        ),
        s3: s3_hooks(),
    })?;
    let inner = queen_proxy::upstream::Upstream::InProcess(broker);
    let passthrough = move |req: Request<Body>| {
        let inner = inner.clone();
        async move {
            match inner.call(req).await {
                Ok(r) => r,
                Err(e) => (axum::http::StatusCode::BAD_GATEWAY, e).into_response(),
            }
        }
    };
    // The broker's metrics describe every tenant's queues: operator-only, on
    // the control-plane token, and a 404 for anyone else.
    let metrics = {
        let passthrough = passthrough.clone();
        move |req: Request<Body>| {
            let passthrough = passthrough.clone();
            async move {
                match queen_proxy::cp::operator_token_ok(req.headers()) {
                    Some(true) => passthrough(req).await,
                    _ => queen_proxy::errors::err_404("not_found", "not found"),
                }
            }
        }
    };
    // Behind the same edge as the proxy's own routes (security headers,
    // request limits, the connection's client IP).
    let edge = queen_proxy::harden::Edge::from_env().map_err(|e| format!("edge settings: {e}"))?;
    let ops = edge.data_plane(
        Router::new()
            .route("/health", any(passthrough))
            .route("/metrics", any(metrics.clone()))
            .route("/metrics/prometheus", any(metrics)),
    );
    let router = ops.merge(proxy);
    Ok((st, router))
}

/// The paths a broker of this cluster calls on another one (`handlers/
/// ephemeral.rs`): the relayed push/pop/ack, the reset and delete fan-out, the
/// ring hand-over and the leaving notice.
const PEER_PATHS: &str = "/api/v1/ephemeral/";

/// The public router when the proxy shares PORT ([`separate_port`] `None`):
/// the proxy in front of everything, except a relay from another broker of
/// this cluster, which goes to `broker` (the inner router: tenancy on, no
/// JWT) as it would on a node whose PORT serves the broker router.
///
/// Peers address each other by PORT (the http half of `QUEEN_RAFT_PEERS`), and
/// the proxy has no business with their calls: it would want a bearer
/// credential (`401`), and past it would strip the forward mark and replace
/// the tenant with its own. The relay must PROVE itself
/// (`peerclient::verified_relay`: the forward mark and the cluster token),
/// so a cluster without `QUEEN_RAFT_TOKEN` keeps everything behind the proxy,
/// and a client's mark alone is the proxy's request like any other.
pub fn with_peer_relays(public: Router, broker: Router) -> Router {
    let public = queen_proxy::upstream::Upstream::InProcess(public);
    let broker = queen_proxy::upstream::Upstream::InProcess(broker);
    Router::new().fallback(move |req: Request<Body>| {
        let peer = req.uri().path().starts_with(PEER_PATHS)
            && crate::peerclient::verified_relay(req.headers());
        let to = if peer { broker.clone() } else { public.clone() };
        async move {
            match to.call(req).await {
                Ok(r) => r,
                Err(e) => (axum::http::StatusCode::BAD_GATEWAY, e).into_response(),
            }
        }
    })
}

/// Serve the single binary's public router: TLS when `QUEEN_TLS_CERT` /
/// `QUEEN_TLS_KEY` are set, the edge's connection limits
/// (`QUEEN_EDGE_*`), the peer address every per-IP rule keys on. Drains and
/// returns once `shutdown` resolves.
pub async fn serve(
    listener: tokio::net::TcpListener,
    app: Router,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<(), String> {
    let tls = queen_proxy::harden::tls_config_from_env("QUEEN")?;
    let opts = queen_proxy::harden::ServeOptions::from_env()?;
    tracing::info!(target: "boot", tls = tls.is_some(), "public listener (hardened)");
    queen_proxy::harden::serve(listener, app, tls, opts, shutdown).await;
    Ok(())
}

/// A 503 for a public request while the embedded proxy is not ready.
#[allow(dead_code)]
pub fn not_ready() -> Response {
    (
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "{\"error\":\"proxy starting\"}",
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::separate_port_of;

    #[test]
    fn the_proxy_gets_its_own_port_only_when_one_other_than_port_is_set() {
        assert_eq!(separate_port_of(None, "6632"), None);
        assert_eq!(separate_port_of(Some(""), "6632"), None);
        assert_eq!(separate_port_of(Some(" 6632 "), "6632"), None);
        assert_eq!(
            separate_port_of(Some("6711"), "6632"),
            Some("6711".to_string())
        );
        assert_eq!(
            separate_port_of(Some(" 6711\n"), "6632"),
            Some("6711".to_string())
        );
    }

    // -----------------------------------------------------------------------
    // Ephemeral relays between two nodes, through both proxy layouts.
    //
    // Node A takes tenant ACME's requests the way its proxy hands them on:
    // in-process, to the broker router with tenancy on and `x-queen-tenant`
    // set. The partition is owned by node B, so A relays over HTTP to B's
    // PORT, which serves what a single binary serves there:
    //   * `Layout::OwnPort` — the proxy has its own port (QUEEN_PROXY_PORT,
    //     stage and prod): PORT is the broker router with tenancy OFF;
    //   * `Layout::SharedPort` — the proxy fronts PORT: the public router,
    //     here a stand-in that answers every request the way the proxy
    //     answers one without credentials, behind `with_peer_relays`.
    // -----------------------------------------------------------------------

    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::{Body, Bytes};
    use axum::http::{Method, Request, StatusCode};
    use axum::Router;
    use serde_json::Value;

    use crate::config::{DEFAULT_TENANT, TENANT_HEADER};
    use crate::ephemeral::{hrw_pick, rendezvous_key, Placement};
    use crate::handlers::AppState;
    use crate::peerclient::{FWD_HEADER, TOKEN_HEADER};

    const ACME: &str = "aabbccdd-1122-3344-5566-778899aabbcc";

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Layout {
        OwnPort,
        SharedPort,
    }

    fn node() -> Arc<AppState> {
        let mut cfg = crate::config::load();
        cfg.tenancy_header = false;
        cfg.ephemeral_require_grant = false;
        crate::handlers::raft::build_raft_state(&cfg).expect("raft state")
    }

    fn broker_router(st: &Arc<AppState>, tenancy: bool) -> Router {
        let mut auth = crate::config::load().auth;
        auth.enabled = false;
        crate::handlers::raft::build_raft_router(
            st.clone(),
            crate::auth::Authenticator::new(auth),
            tenancy,
        )
    }

    /// What the proxy answers a request without a credential.
    fn stand_in_proxy() -> Router {
        Router::new().fallback(|| async {
            (
                StatusCode::UNAUTHORIZED,
                "{\"error\":\"missing bearer credential\"}",
            )
        })
    }

    /// One request into `router` in-process. `headers` are set as given.
    async fn call(
        router: &Router,
        method: Method,
        uri: &str,
        headers: &[(&str, &str)],
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut req = Request::builder().method(method).uri(uri);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let body = match body {
            Some(v) => {
                req = req.header("content-type", "application/json");
                Body::from(v.to_string())
            }
            None => Body::empty(),
        };
        let resp = queen_proxy::upstream::Upstream::InProcess(router.clone())
            .call(req.body(body).unwrap())
            .await
            .expect("in-process call");
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("body");
        let v = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
        (status, v)
    }

    /// One request to `base` over HTTP, as a client of that port would send it.
    async fn http(
        base: &str,
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut hs: Vec<(axum::http::HeaderName, axum::http::HeaderValue)> = Vec::new();
        for (k, v) in headers {
            hs.push((
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                axum::http::HeaderValue::from_str(v).unwrap(),
            ));
        }
        let body = body.map(|v| Bytes::from(v.to_string())).unwrap_or_default();
        let r = crate::peerclient::PeerClient::new()
            .call(
                method,
                &format!("{base}{path}"),
                &hs,
                body,
                Duration::from_secs(10),
            )
            .await
            .expect("http call");
        let v = serde_json::from_slice(&r.body)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&r.body).into_owned()));
        (r.status, v)
    }

    async fn serve(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        format!("http://{addr}")
    }

    /// A partition of `queue` that `owner` holds for every one of `tenants`
    /// among `nodes` (the rendezvous key has the tenant in it).
    fn partition_owned_by(tenants: &[&str], queue: &str, nodes: &[String], owner: &str) -> String {
        (0..10_000)
            .map(|i| format!("p{i}"))
            .find(|p| {
                tenants
                    .iter()
                    .all(|t| hrw_pick(&rendezvous_key(t, queue, p), nodes) == Some(owner))
            })
            .expect("some partition hashes to the owner")
    }

    /// Messages held for (`tenant`, `queue`) on a node: `None` = no such queue.
    fn depth(st: &AppState, tenant: &str, queue: &str) -> Option<i64> {
        st.ephemeral.depth(tenant, queue).map(|(n, _)| n)
    }

    async fn eventually(what: &str, mut ok: impl FnMut() -> bool) {
        for _ in 0..200 {
            if ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("{what}: not within 2 s");
    }

    async fn tenant_stays_on_the_owner(layout: Layout) {
        let token = crate::peerclient::test_token();
        let (a, b) = (node(), node());
        let inner_a = broker_router(&a, true);
        let inner_b = broker_router(&b, true);
        let port_b = match layout {
            Layout::OwnPort => broker_router(&b, false),
            Layout::SharedPort => super::with_peer_relays(stand_in_proxy(), inner_b.clone()),
        };
        let base_b = serve(port_b).await;
        let nodes: Vec<String> = vec!["1".into(), "2".into()];
        let placement = |me: &str, peer: &str, addr: &str| {
            Some(Placement {
                me: me.into(),
                nodes: nodes.clone(),
                addrs: [(peer.to_string(), addr.to_string())].into_iter().collect(),
            })
        };
        a.ephemeral.set_placement(placement("1", "2", &base_b));
        // B never relays here; an address nothing listens on.
        b.ephemeral
            .set_placement(placement("2", "1", "http://127.0.0.1:9"));

        let q = format!("inbox-{layout:?}");
        let p = partition_owned_by(&[ACME, DEFAULT_TENANT], &q, &nodes, "2");
        let acme = [(TENANT_HEADER, ACME)];
        let pop_uri = format!("/api/v1/ephemeral/pop?queue={q}&partition={p}&batch=10");

        // ---- push: relayed to B, stored under ACME there and nowhere else ----
        let push = serde_json::json!({
            "queue": q, "partition": p,
            "messages": [{"payload": {"secret": "acme-1"}}],
        });
        let (s, v) = call(
            &inner_a,
            Method::POST,
            "/api/v1/ephemeral/push",
            &acme,
            Some(push.clone()),
        )
        .await;
        assert_eq!(
            (s, &v["pushed"]),
            (StatusCode::CREATED, &Value::from(1)),
            "{layout:?} push: {v}"
        );
        assert_eq!(
            depth(&b, ACME, &q),
            Some(1),
            "{layout:?}: the owner holds ACME's message"
        );
        assert_eq!(
            depth(&b, DEFAULT_TENANT, &q),
            None,
            "{layout:?}: not under the default tenant"
        );
        assert_eq!(
            depth(&a, ACME, &q),
            None,
            "{layout:?}: not on the relaying node"
        );

        // ---- a default-tenant client of B's PORT sees nothing of ACME's ----
        let (s, v) = http(&base_b, Method::GET, &pop_uri, &[], None).await;
        match layout {
            Layout::OwnPort => {
                assert_eq!(s, StatusCode::OK, "{v}");
                assert_eq!(v["messages"], serde_json::json!([]), "{layout:?}: {v}");
            }
            Layout::SharedPort => assert_eq!(s, StatusCode::UNAUTHORIZED, "{v}"),
        }
        // ...nor does one that sets the mark and names ACME without the token.
        for spoof in [
            vec![(FWD_HEADER, "1"), (TENANT_HEADER, ACME)],
            vec![
                (FWD_HEADER, "1"),
                (TOKEN_HEADER, "guess"),
                (TENANT_HEADER, ACME),
            ],
        ] {
            let (s, v) = http(&base_b, Method::GET, &pop_uri, &spoof, None).await;
            let want = match layout {
                Layout::OwnPort => StatusCode::FORBIDDEN,
                Layout::SharedPort => StatusCode::UNAUTHORIZED,
            };
            assert_eq!(s, want, "{layout:?} spoofed relay: {v}");
        }
        assert_eq!(depth(&b, ACME, &q), Some(1), "{layout:?}: still there");

        // ---- pop, nack, pop again, ack: all relayed, all ACME's ----
        let pop = || call(&inner_a, Method::GET, &pop_uri, &acme, None);
        let (s, v) = pop().await;
        assert_eq!(s, StatusCode::OK, "{layout:?} pop: {v}");
        assert_eq!(
            v["messages"][0]["payload"]["secret"], "acme-1",
            "{layout:?}: {v}"
        );
        let id = v["messages"][0]["id"].as_str().unwrap().to_string();
        let ack = |id: &str, status: &str| serde_json::json!({"queue": q, "acks": [{"id": id, "status": status}]});
        let (s, v) = call(
            &inner_a,
            Method::POST,
            "/api/v1/ephemeral/ack",
            &acme,
            Some(ack(&id, "retry")),
        )
        .await;
        assert_eq!(
            (s, &v["results"][0]["outcome"]),
            (StatusCode::OK, &Value::from("redelivered")),
            "{layout:?} nack: {v}"
        );
        let (s, v) = pop().await;
        assert_eq!(s, StatusCode::OK, "{layout:?} pop again: {v}");
        assert_eq!(
            v["messages"][0]["payload"]["secret"], "acme-1",
            "{layout:?}: {v}"
        );
        let id = v["messages"][0]["id"].as_str().unwrap().to_string();
        let (s, v) = call(
            &inner_a,
            Method::POST,
            "/api/v1/ephemeral/ack",
            &acme,
            Some(ack(&id, "completed")),
        )
        .await;
        assert_eq!(
            (s, &v["results"][0]["outcome"]),
            (StatusCode::OK, &Value::from("acked")),
            "{layout:?} ack: {v}"
        );
        assert_eq!(
            depth(&b, ACME, &q),
            Some(0),
            "{layout:?}: acked on the owner"
        );

        // ---- reset fans out to B: ACME's queue, never the default tenant's ----
        b.ephemeral
            .push(
                DEFAULT_TENANT,
                &q,
                &p,
                vec![b"{\"secret\":\"default-1\"}".to_vec().into_boxed_slice()],
                crate::util::now_epoch_ms(),
            )
            .expect("default tenant push");
        let (s, v) = call(
            &inner_a,
            Method::POST,
            "/api/v1/ephemeral/push",
            &acme,
            Some(push.clone()),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED, "{layout:?} push: {v}");
        assert_eq!(depth(&b, ACME, &q), Some(1));
        let (s, v) = call(
            &inner_a,
            Method::POST,
            "/api/v1/ephemeral/reset",
            &acme,
            Some(serde_json::json!({"queue": q})),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{layout:?} reset: {v}");
        eventually("the reset reaches B", || depth(&b, ACME, &q) == Some(0)).await;
        assert_eq!(
            depth(&b, DEFAULT_TENANT, &q),
            Some(1),
            "{layout:?}: the default tenant's queue survives ACME's reset"
        );

        // ---- delete, as A's fan-out sends it: ACME's rings only ----
        let mut relay: Vec<(&str, &str)> = vec![
            (FWD_HEADER, "1"),
            (TOKEN_HEADER, token),
            (TENANT_HEADER, ACME),
        ];
        let (s, v) = http(
            &base_b,
            Method::DELETE,
            &format!("/api/v1/ephemeral/queue/{q}"),
            &relay,
            None,
        )
        .await;
        assert_eq!(
            (s, &v["deleted"]),
            (StatusCode::OK, &Value::from(true)),
            "{layout:?} relayed delete: {v}"
        );
        assert_eq!(
            depth(&b, ACME, &q),
            None,
            "{layout:?}: ACME's queue is gone"
        );
        assert_eq!(
            depth(&b, DEFAULT_TENANT, &q),
            Some(1),
            "{layout:?}: the default tenant's is not"
        );

        // ---- hand-over: a ring A held moves to B under ACME ----
        let q2 = format!("handover-{layout:?}");
        let p2 = partition_owned_by(&[ACME], &q2, &nodes, "2");
        a.ephemeral.set_placement(None);
        let push2 = serde_json::json!({"queue": q2, "partition": p2, "messages": [{"payload": {"secret": "acme-2"}}]});
        let (s, v) = call(
            &inner_a,
            Method::POST,
            "/api/v1/ephemeral/push",
            &acme,
            Some(push2),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED, "{layout:?} local push: {v}");
        assert_eq!(depth(&a, ACME, &q2), Some(1));
        a.ephemeral.set_placement(placement("1", "2", &base_b));
        assert_eq!(a.ephemeral.reap_foreign(), 1);
        let (delivered, lost) = crate::handlers::ship(&a, a.ephemeral.take_outbox()).await;
        assert_eq!((delivered, lost), (1, 0), "{layout:?}: hand-over");
        assert_eq!(
            depth(&b, ACME, &q2),
            Some(1),
            "{layout:?}: B adopted ACME's ring"
        );
        assert_eq!(
            depth(&b, DEFAULT_TENANT, &q2),
            None,
            "{layout:?}: not as the default tenant's"
        );
        let (s, v) = call(
            &inner_a,
            Method::GET,
            &format!("/api/v1/ephemeral/pop?queue={q2}&partition={p2}"),
            &acme,
            None,
        )
        .await;
        assert_eq!(
            v["messages"][0]["payload"]["secret"], "acme-2",
            "{layout:?} {s}: {v}"
        );

        // ---- the leaving notice reaches B too ----
        relay.retain(|(k, _)| *k != TENANT_HEADER);
        let (s, v) = http(
            &base_b,
            Method::POST,
            "/api/v1/ephemeral/_leaving",
            &relay,
            Some(serde_json::json!({"node": "1"})),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{layout:?} leaving: {v}");
        assert!(b
            .ephemeral
            .excluded_peers(crate::util::now_epoch_ms())
            .contains("1"));

        // ---- the peer door opens on the ephemeral paths only ----
        if layout == Layout::SharedPort {
            let (s, v) = http(
                &base_b,
                Method::GET,
                "/api/v1/resources/queues",
                &relay,
                None,
            )
            .await;
            assert_eq!(
                s,
                StatusCode::UNAUTHORIZED,
                "a verified relay to another path is the proxy's: {v}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ephemeral_relays_keep_the_tenant_with_the_proxy_on_its_own_port() {
        tenant_stays_on_the_owner(Layout::OwnPort).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ephemeral_relays_reach_the_broker_with_the_proxy_on_port() {
        tenant_stays_on_the_owner(Layout::SharedPort).await;
    }
}
