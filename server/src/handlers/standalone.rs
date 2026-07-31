//! The `/auth/*` surface the broker answers when it serves the SPA itself
//! (broker-direct, no queen_proxy in front).
//!
//! The dashboard is identity-first: it boots from `GET /auth/me` and derives
//! every permission from that one payload (app/src/stores/identity.js) — it
//! never sniffs who is serving it. Under the proxy, /auth/me is the proxy's
//! cookie-authenticated session endpoint (proxy/src/oauth.rs). Served
//! broker-direct there is no session to describe, so the BROKER answers:
//!
//!   * auth disabled (JWT_ENABLED=false, the default): a fixed "standalone"
//!     identity — one synthetic cluster, admin role, operator live, and
//!     `"standalone": true` so the shell hides the session UI (logout,
//!     cluster selector). Everything else the SPA calls (/api/v1/*, /health,
//!     /metrics/prometheus) is native broker surface, so the whole dashboard
//!     works with no proxy. The API was already open in this configuration —
//!     the dashboard adds visibility, not privilege.
//!
//!   * auth enabled: /auth/me answers 401 `auth_required`, the SPA's login
//!     redirect lands on /auth/login, and that serves a static page saying the
//!     dashboard needs the proxy when the broker requires tokens. No redirect
//!     loop: the page is terminal, the broker has no session to mint.

#![allow(unused_imports)]
use super::*;
use axum::response::Redirect;
use std::sync::Arc;

/// The act-as header name the SPA attaches to every /api/v1 request once a
/// cluster is picked (identity.js DEFAULT_ACT_HEADER, proxy/src/config.rs
/// ACT_CLUSTER_HEADER). The broker ignores the header entirely; it is echoed
/// here only so the identity payload is shape-identical to the proxy's.
const ACT_CLUSTER_HEADER: &str = "x-queen-act-cluster";

/// GET /auth/me — the standalone identity, or 401 when this broker requires
/// tokens. Field-for-field the proxy's shape (proxy/src/oauth.rs `me`): the SPA
/// must not be able to tell which binary answered.
pub async fn handle_auth_me(State(state): State<Arc<AppState>>) -> Response {
    if state.auth_enabled {
        return json(
            StatusCode::UNAUTHORIZED,
            "{\"error\":\"auth_required\",\"code\":\"auth_required\"}".to_string(),
        );
    }
    let body = serde_json::json!({
        "user_id": "standalone",
        "email": null,
        "tenant_slug": "local",
        // Whoever runs the broker IS the operator of this cell, so the
        // cell-level pages (System) are theirs by construction.
        "is_operator": true,
        "operator_enabled": true,
        "operator_live": true,
        "acting_cluster": { "id": "local", "slug": "local" },
        "clusters": [{
            "id": "local",
            "slug": "local",
            "role": "admin",
            "tenant_slug": "local",
            // The tenant every request resolves to with tenancy off — the
            // synthetic cluster names the tenant the data actually lives under.
            "tenant_id": crate::config::DEFAULT_TENANT,
            "status": "active",
            "cell_slug": state.server_id,
        }],
        "act_cluster_header": ACT_CLUSTER_HEADER,
        "role": "admin",
        "cluster": null,
        "standalone": true,
    });
    json(StatusCode::OK, body.to_string())
}

/// GET /auth/login. With auth off there is nothing to sign in to — the
/// dashboard already works — so land on it. With auth on, serve the terminal
/// explanation page: this broker validates bearer tokens but holds no user
/// accounts or browser sessions, so a login form here could never be honest.
pub async fn handle_auth_login(State(state): State<Arc<AppState>>) -> Response {
    if !state.auth_enabled {
        return Redirect::to("/").into_response();
    }
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        AUTH_REQUIRED_HTML,
    )
        .into_response()
}

/// POST /auth/logout — a session no-op: standalone has no session to end. The
/// SPA hides its sign-out control when /auth/me says standalone; this answers
/// a hand-typed call instead of letting it fall to the JSON 404.
pub async fn handle_auth_logout() -> Response {
    json(StatusCode::OK, "{\"ok\":true}".to_string())
}

/// Served for GET /auth/login when JWT auth is enabled. Fully self-contained
/// (inline CSS, no assets): with JWT on, the SPA bundle's root-level files are
/// classified read-write by the access table, so an <img> here would 401 —
/// text only, by construction.
const AUTH_REQUIRED_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Queen — authentication required</title>
<style>
  body { margin: 0; min-height: 100vh; display: grid; place-items: center;
         background: #0c0d10; color: #e8e6e3;
         font: 15px/1.6 system-ui, -apple-system, "Segoe UI", sans-serif; }
  main { max-width: 34rem; padding: 2.5rem 1.5rem; text-align: center; }
  h1   { font-size: 1.15rem; font-weight: 600; margin: 0 0 .75rem; }
  p    { margin: 0 0 .75rem; color: #b9b6b1; }
  code { background: #1a1c21; border-radius: 4px; padding: .1em .35em;
         font-size: .92em; color: #e8e6e3; }
</style>
</head>
<body>
<main>
  <h1>This broker requires authentication</h1>
  <p>JWT auth is enabled (<code>JWT_ENABLED=true</code>): the API accepts bearer
     tokens, but the broker holds no user accounts and no browser sessions, so
     there is nothing to sign in to here.</p>
  <p>To use the dashboard, serve it through <code>queen_proxy</code>, which owns
     login and sessions — or run the broker with auth disabled for the open
     standalone dashboard.</p>
</main>
</body>
</html>
"#;
