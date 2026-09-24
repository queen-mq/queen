//! The single binary (PLAN_SINGLE_BINARY.md W3/W4): the proxy runs inside the
//! broker, one process per node, no Postgres.
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
//!   dashboard, data plane). `/health` and `/metrics*` pass straight to the
//!   broker, unauthenticated as on a bare broker, for probes and scrapers.
//!
//! On with `QUEEN_PROXY_EMBEDDED=true`; configured by the same
//! `QUEEN_PROXY_*` environment as the standalone proxy.

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

/// The public router of a single-binary node: the proxy in front of
/// `broker` (the inner broker router: tenancy on, broker JWT off, no socket).
pub fn public_router(rsm: Arc<dyn Rsm>, broker: Router) -> (queen_proxy::state::St, Router) {
    let (st, proxy) = queen_proxy::app::build_embedded(queen_proxy::app::Embedded {
        kv: Arc::new(RsmKv::new(rsm)),
        broker: broker.clone(),
    });
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
    let router = Router::new()
        .route("/health", any(passthrough.clone()))
        .route("/metrics", any(passthrough.clone()))
        .route("/metrics/prometheus", any(passthrough))
        .merge(proxy);
    (st, router)
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
