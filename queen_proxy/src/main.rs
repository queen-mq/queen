//! queen-proxy — multi-tenant data-plane gateway for QueenMQ cells.
//! Spec: PLAN_QUEEN_PROXY_CLOUD.md (repo root). Module ownership: CONTRACTS.md.

mod auth;
mod cache;
mod config;
mod db;
mod errors;
mod gateway;
mod httpget;
mod limits;
mod meter;
mod oauth;
mod obs;
mod pgtls;
mod registry;
mod routes;
mod spool;
mod state;

use std::sync::Arc;

use axum::routing::get;
use axum::Router;

use crate::state::{AppState, St};

#[tokio::main]
async fn main() {
    obs::init();
    let cfg = config::Config::load();

    let db = match &cfg.pxdb {
        Some(pxcfg) => match db::create_pool(pxcfg).await {
            Ok(pool) => {
                if let Err(e) = db::apply_migrations(&pool).await {
                    tracing::error!("migrations failed: {e}");
                    std::process::exit(1);
                }
                Some(pool)
            }
            Err(e) => {
                tracing::error!("pxdb unavailable: {e}");
                std::process::exit(1);
            }
        },
        None => {
            if cfg.dev_static.is_none() {
                tracing::error!(
                    "no PXDB_HOST and no QUEEN_PROXY_DEV_CELL_URL — nothing to serve"
                );
                std::process::exit(1);
            }
            tracing::warn!("running in dev-static mode (no pxdb)");
            None
        }
    };

    let upstream = hyper_util::client::legacy::Client::builder(
        hyper_util::rt::TokioExecutor::new(),
    )
    .build_http::<axum::body::Body>();

    let cache = cache::ClusterCache::new(&cfg, db.clone());
    let limits = limits::Limits::new(&cfg);
    let meter = meter::Meter::new(&cfg);
    let registry = registry::Registry::new(db.clone());
    let keys = auth::Keys::from_config(&cfg);

    let st: St = Arc::new(AppState {
        cfg,
        db,
        upstream,
        cache,
        limits,
        meter,
        registry,
        keys,
    });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/.well-known/jwks.json", get(jwks))
        .nest("/auth", oauth::router())
        .fallback(gateway::handle)
        .with_state(st.clone());

    let addr = format!("0.0.0.0:{}", st.cfg.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
    tracing::info!(
        addr,
        enforce = st.cfg.enforce,
        dev_static = st.cfg.dev_static.is_some(),
        "queen-proxy up"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(obs::shutdown_signal())
        .await
        .expect("serve");
}

async fn healthz() -> &'static str {
    "ok"
}

async fn jwks(axum::extract::State(st): axum::extract::State<St>) -> axum::response::Response {
    let mut resp = axum::response::IntoResponse::into_response(st.keys.jwks_json());
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    resp
}
