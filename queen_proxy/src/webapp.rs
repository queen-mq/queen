//! The Queen dashboard, served BY THE PROXY at `/`, behind mandatory auth.
//!
//! Embed: `../server/webapp/dist` — the SAME built artifact the broker embeds
//! (server/src/handlers/static_files.rs), pointed at across the workspace
//! rather than copied, so one `npm run build` in `app/` keeps both surfaces on
//! the same bytes and the repo carries one copy. `#[folder]` is resolved
//! against CARGO_MANIFEST_DIR, exactly like console.rs's `console/dist`.
//!
//! The broker keeps serving its own copy on purpose: a `kubectl port-forward`
//! straight to a broker is the operator escape hatch when the proxy or its
//! control plane is the thing that is broken. Nothing here touches server/.
//!
//! What DID change is where an unknown path goes. It used to fall through
//! `gateway::handle` to the broker, which answered with the broker's embedded
//! dashboard — an unauthenticated, tenant-unaware copy of the app. Repointing
//! DNS at the proxy would therefore have served the OLD webapp, half-broken.
//! `route_fallback` below splits the two surfaces instead: broker-bound paths
//! proxy as before, everything else is this SPA.
//!
//! AUTH IS MANDATORY HERE. Not one byte of the app — not index.html, not an
//! asset, not a client-side route — reaches a caller without a live human
//! session; they get a 302 to the login page carrying `next`. That is what the
//! Node proxy in `proxy/` does today and it must not regress. API paths are
//! unaffected and keep answering JSON 401/403, never a redirect, so the SPA's
//! own XHR can react instead of parsing a login page.

use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

use crate::auth::{self, Credential};
use crate::state::St;

/// Vite build output of `app/` (base `/`), the artifact `server/` embeds.
#[derive(RustEmbed)]
#[folder = "../server/webapp/dist"]
struct WebappAssets;

// ---------------------------------------------------------------------------
// surface split
// ---------------------------------------------------------------------------

/// Is this path the broker's, rather than the webapp's?
///
/// The list mirrors the broker's own router (server/src/main.rs): everything
/// under `/api/`, `/streams/`, `/internal/`, plus its four bare endpoints. It
/// is deliberately a PREFIX/exact list and not "anything with a dot in it":
/// the webapp owns client-side routes like `/queues/orders`, and a new broker
/// route landing under one of these prefixes must keep proxying, not silently
/// start returning index.html.
///
/// Note this only decides WHERE a request goes. Whether it is allowed is still
/// `routes::classify` + `auth::authorize` on the gateway side.
pub fn is_upstream_path(path: &str) -> bool {
    path.starts_with("/api/")
        || path.starts_with("/streams/")
        || path.starts_with("/internal/")
        || path == "/health"
        || path == "/status"
        || path == "/metrics"
        || path == "/metrics/prometheus"
}

/// Router fallback: broker-bound paths to the data-plane pipeline, everything
/// else to the auth-gated SPA.
pub async fn route_fallback(State(st): State<St>, req: Request) -> Response {
    if is_upstream_path(req.uri().path()) {
        return crate::gateway::handle(State(st), req).await;
    }
    serve(&st, req.headers(), req.uri()).await
}

// ---------------------------------------------------------------------------
// the gate
// ---------------------------------------------------------------------------

async fn serve(st: &St, headers: &HeaderMap, uri: &Uri) -> Response {
    if !has_live_session(st, headers).await {
        return redirect_to_login(uri);
    }
    serve_asset(uri.path())
}

/// A live HUMAN session, and nothing else. An `Authorization: Bearer qk_...`
/// is a data-plane credential: it can push and pop, and it must not be able to
/// pull down a dashboard bundle, so it is treated exactly like no session.
async fn has_live_session(st: &St, headers: &HeaderMap) -> bool {
    let Credential::Session(token) =
        auth::read_credential_for_document(&st.cfg.cookie_name, headers)
    else {
        return false;
    };
    // Full verification, deny-list included: `verify_session` is the same call
    // the data plane makes, so a logged-out cookie cannot still open the app.
    auth::verify_session(st, &token).await.is_ok()
}

/// 302 to the login page, carrying the path the caller was after so the login
/// lands them back on it. `next` is re-validated by oauth.rs on the way out
/// too (`safe_next`) — this is our own URI, but the rule is cheap and the
/// open-redirect class of bug is not.
fn redirect_to_login(uri: &Uri) -> Response {
    let want = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    let next = if crate::oauth::is_safe(want) { want } else { "/" };
    let location = format!("/auth/login?next={}", crate::oauth::pct(next));
    let mut resp = StatusCode::FOUND.into_response();
    if let Ok(v) = HeaderValue::from_str(&location) {
        resp.headers_mut().insert(header::LOCATION, v);
    }
    // A redirect that a cache could replay to the next visitor would be a
    // login loop at best; never store it.
    resp.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    resp
}

// ---------------------------------------------------------------------------
// static serving
// ---------------------------------------------------------------------------

/// Path relative to `webapp/dist`. `/` (and any client-side route with no
/// matching built file) maps to `index.html`, which is how a hard refresh on
/// `/queues/orders` still boots the SPA.
fn rel_path(path: &str) -> &str {
    let rel = path.trim_start_matches('/');
    if rel.is_empty() {
        "index.html"
    } else {
        rel
    }
}

fn serve_asset(path: &str) -> Response {
    let rel = rel_path(path);
    if let Some(resp) = embedded(rel) {
        return resp;
    }
    match embedded("index.html") {
        Some(resp) => resp,
        None => (
            StatusCode::NOT_FOUND,
            "webapp not built (server/webapp/dist is empty — build app/ first)",
        )
            .into_response(),
    }
}

fn embedded(path: &str) -> Option<Response> {
    let file = WebappAssets::get(path)?;
    let mut resp = ([(header::CONTENT_TYPE, content_type(path))], file.data.into_owned()).into_response();
    resp.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(cache_control(path)));
    Some(resp)
}

/// `private` on everything: the whole surface is served only to an
/// authenticated caller, so no shared cache should ever hold a copy. Hashed
/// bundles are immutable by construction (Vite puts the content hash in the
/// filename); the HTML shell must be revalidated or a deploy leaves browsers
/// booting last week's asset names.
fn cache_control(path: &str) -> &'static str {
    if path.starts_with("assets/") {
        "private, max-age=31536000, immutable"
    } else {
        "private, no-cache"
    }
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "application/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" | "map" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "ttf" => "font/ttf",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_paths_still_proxy() {
        for p in [
            "/api/v1/push",
            "/api/v1/resources/queues",
            "/api/console/overview", // nested route wins before the fallback
            "/streams/v1/queries",
            "/internal/api/notify",
            "/health",
            "/status",
            "/metrics",
            "/metrics/prometheus",
        ] {
            assert!(is_upstream_path(p), "{p}");
        }
    }

    #[test]
    fn webapp_paths_are_not_proxied() {
        for p in [
            "/",
            "/index.html",
            "/assets/index-D4y6H0nC.js",
            "/favicon.svg",
            // the SPA's own client-side routes, which must reach index.html
            "/queues",
            "/queues/orders",
            "/analytics",
            "/system",
            // near-misses of the upstream prefixes
            "/apidocs",
            "/statuses",
            "/metricsx",
            "/healthz",
        ] {
            assert!(!is_upstream_path(p), "{p}");
        }
    }

    #[test]
    fn rel_path_maps_root_and_deep_links_to_index() {
        assert_eq!(rel_path("/"), "index.html");
        assert_eq!(rel_path(""), "index.html");
        assert_eq!(rel_path("/assets/index-abc.js"), "assets/index-abc.js");
        // no built file matches, so serve_asset falls back to index.html
        assert_eq!(rel_path("/queues/orders"), "queues/orders");
    }

    #[test]
    fn cache_control_is_private_everywhere() {
        assert!(cache_control("index.html").starts_with("private,"));
        assert!(cache_control("assets/index-abc.js").starts_with("private,"));
        assert_eq!(cache_control("assets/index-abc.js"), "private, max-age=31536000, immutable");
        assert_eq!(cache_control("index.html"), "private, no-cache");
    }

    #[test]
    fn the_built_webapp_is_actually_embedded() {
        // Guards the cross-crate `#[folder = "../server/webapp/dist"]`: if the
        // path ever stops resolving, this fails loudly instead of the proxy
        // silently serving "webapp not built" in production.
        assert!(WebappAssets::get("index.html").is_some(), "index.html missing from the embed");
        assert!(
            WebappAssets::iter().any(|f| f.starts_with("assets/")),
            "no hashed assets in the embed"
        );
    }
}
