//! Where the data plane sends a request once auth, limits and metering let
//! it through (PLAN_SINGLE_BINARY.md W3: "gateway → in-process").
//!
//! - [`Upstream::Http`]: the standalone proxy relays to the cluster's cell
//!   broker over pooled HTTP (`ClusterCtx::cell_base_url`).
//! - [`Upstream::InProcess`]: the single binary hands the request to the
//!   broker router it runs inside — no socket, no second process. The
//!   broker's own tenant middleware reads the `x-queen-tenant` header the
//!   gateway sets (the router is built with tenancy on and is reachable only
//!   through this call).

use axum::body::Body;
use axum::http::{Request, Response};

pub type HttpClient =
    hyper_util::client::legacy::Client<hyper_util::client::legacy::connect::HttpConnector, Body>;

#[derive(Clone)]
pub enum Upstream {
    Http(HttpClient),
    InProcess(axum::Router),
}

impl Upstream {
    /// The pooled HTTP client the standalone proxy uses (TCP_NODELAY, 64 idle
    /// per host — the settings `app.rs` always used).
    pub fn http() -> Upstream {
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.set_nodelay(true);
        Upstream::Http(
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .pool_max_idle_per_host(64)
                .build::<_, Body>(connector),
        )
    }

    /// Whether requests stay inside this process.
    pub fn in_process(&self) -> bool {
        matches!(self, Upstream::InProcess(_))
    }

    /// Send `req` (its URI already absolute for `Http`, a path for
    /// `InProcess`) and return the answer.
    pub async fn call(&self, req: Request<Body>) -> Result<Response<Body>, String> {
        match self {
            Upstream::Http(c) => c
                .request(req)
                .await
                .map(|r| r.map(Body::new))
                .map_err(|e| e.to_string()),
            Upstream::InProcess(router) => {
                use tower::ServiceExt;
                router
                    .clone()
                    .oneshot(req)
                    .await
                    .map_err(|e| match e {})
            }
        }
    }
}
