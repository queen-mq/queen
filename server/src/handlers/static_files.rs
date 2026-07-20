#![allow(unused_imports)]
use super::*;
use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};

// Static dashboard root, resolved once from QUEEN_STATIC_DIR (default
// "webapp/dist" — the path the Dockerfile copies the built Vue app to).
static STATIC_DIR: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn static_root() -> &'static str {
    STATIC_DIR.get_or_init(|| {
        std::env::var("QUEEN_STATIC_DIR").unwrap_or_else(|_| "webapp/dist".to_string())
    })
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

/// Router FALLBACK: serve the SPA dashboard from QUEEN_STATIC_DIR. A real file
/// under the root is served with a guessed content-type; any other path falls
/// back to index.html so the client-side (Vue) router can take over. This is
/// ONLY the fallback, so it never shadows an /api/v1 route; when the directory
/// is absent (dev / CI) it just 404s. Mirrors the static surface the retired C++
/// server exposed. Path traversal is rejected (no ".." / backslash / NUL, and
/// percent-encoded escapes never decode to a real parent path on disk).
pub async fn handle_static(uri: Uri) -> Response {
    let root = static_root();
    let rel = uri.path().trim_start_matches('/');
    let index = format!("{root}/index.html");

    let safe = !rel.contains("..") && !rel.contains('\\') && !rel.contains('\0');
    if safe && !rel.is_empty() {
        let candidate = format!("{root}/{rel}");
        if let Ok(bytes) = tokio::fs::read(&candidate).await {
            return ([(header::CONTENT_TYPE, content_type(&candidate))], bytes).into_response();
        }
    }

    match tokio::fs::read(&index).await {
        Ok(bytes) => (
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            bytes,
        )
            .into_response(),
        Err(_) => (StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}
