//! Human identity endpoints: local login, Google OAuth (port of
//! proxy/src/google-auth.js), GitHub OAuth (OAuth2 without OIDC — /user +
//! /user/emails, verified primary email only), sessions as JWT cookies,
//! auth-host mode. OWNER: Agent G (wave 2).
//!
//! Skeleton: routes exist and answer honestly that they're not configured.

use axum::routing::get;
use axum::Router;

use crate::state::St;

pub fn router() -> Router<St> {
    Router::new()
        .route("/login", get(login_page))
        .route("/google", get(not_configured))
        .route("/google/callback", get(not_configured))
        .route("/github", get(not_configured))
        .route("/github/callback", get(not_configured))
}

async fn login_page() -> axum::response::Html<&'static str> {
    axum::response::Html(
        "<!doctype html><title>queen-proxy</title><h1>queen-proxy</h1>\
         <p>Login is not configured on this instance (Track A pending).</p>",
    )
}

async fn not_configured() -> axum::response::Response {
    crate::errors::err_404("not_configured", "identity provider not configured")
}
