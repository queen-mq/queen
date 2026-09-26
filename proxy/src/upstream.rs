//! Where the data plane sends a request once auth, limits and metering let
//! it through (PLAN_SINGLE_BINARY.md W3: "gateway → in-process").
//!
//! - [`Upstream::Http`]: relay to the cluster's cell broker over pooled HTTP
//!   (`ClusterCtx::cell_base_url`) — a broker in another process (tests run
//!   a stub broker this way).
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
