#![allow(unused_imports)]
use super::*;
use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

// The built Vue dashboard, embedded into the binary at compile time from
// server/webapp/dist (populated by `app` -> `npm run build` + copied into the
// crate; the Dockerfile copies it into the build context). No QUEEN_STATIC_DIR,
// no on-disk assets at runtime — a single self-contained binary serves the SPA.
#[derive(RustEmbed)]
#[folder = "webapp/dist"]
struct Assets;

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

fn serve(path: &str) -> Option<Response> {
    Assets::get(path).map(|f| {
        (
            [(header::CONTENT_TYPE, content_type(path))],
            f.data.into_owned(),
        )
            .into_response()
    })
}

/// Router FALLBACK: serve the embedded SPA dashboard. A real asset under the
/// root is served with a guessed content-type; any other path falls back to
/// index.html so the client-side (Vue) router takes over. This is ONLY the
/// fallback, so it never shadows an /api/v1 route. When the dashboard wasn't
/// bundled at build time it 404s. Mirrors the static surface the retired C++
/// server exposed.
///
/// API paths and non-GET methods are answered with a JSON 404 instead of the
/// SPA document: the fallback matches ANY method, so a call to an endpoint that
/// does not exist used to come back 200 text/html — which every JSON client
/// (and the webapp's axios) reads as SUCCESS. A phantom endpoint must fail
/// loudly, not look like it worked.
///
/// /auth/* gets the same treatment: the SPA's identity bootstrap fetches
/// /auth/me and parses JSON, so an /auth path this broker does not implement
/// (the real ones are registered routes — handlers/standalone.rs) answering
/// with the SPA document is the same phantom-endpoint lie.
pub async fn handle_static(method: axum::http::Method, uri: Uri) -> Response {
    let path = uri.path();
    let readable = method == axum::http::Method::GET || method == axum::http::Method::HEAD;
    if path.starts_with("/api/") || path == "/api" || path.starts_with("/auth/") || path == "/auth" || !readable {
        return json(
            StatusCode::NOT_FOUND,
            "{\"error\":\"Not Found\",\"code\":\"no_such_route\"}".to_string(),
        );
    }
    let rel = path.trim_start_matches('/');
    if !rel.is_empty() {
        if let Some(resp) = serve(rel) {
            return resp;
        }
    }
    match serve("index.html") {
        Some(resp) => resp,
        None => (StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}
