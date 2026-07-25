//! The proxy's client-facing error contract (PLAN_QUEEN_PROXY_CLOUD.md §4/§6):
//! JSON envelope `{"error": <human>, "code": <machine>}`, `Retry-After` on 429.
//! Codes are load-bearing — Track C clients switch on them. Do not invent new
//! codes casually; add them here and document in the README.

use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

pub const CODE_UNAUTHORIZED: &str = "unauthorized";
pub const CODE_FORBIDDEN: &str = "forbidden";
pub const CODE_RATE_LIMITED: &str = "rate_limited";
pub const CODE_QUOTA_EXCEEDED: &str = "quota_exceeded";
pub const CODE_STORAGE_QUOTA: &str = "storage_quota_exceeded";
pub const CODE_SUSPENDED: &str = "cluster_suspended";
pub const CODE_PUSH_BLOCKED: &str = "push_blocked";
pub const CODE_FEATURE_GATED: &str = "feature_gated";
pub const CODE_ROUTE_BLOCKED: &str = "route_blocked";
pub const CODE_CLUSTER_UNKNOWN: &str = "cluster_unknown";
pub const CODE_SCOPE_UNKNOWN: &str = "scope_unknown";
pub const CODE_PAYLOAD_TOO_LARGE: &str = "payload_too_large";
pub const CODE_BAD_GATEWAY: &str = "bad_gateway";
pub const CODE_UPSTREAM_TIMEOUT: &str = "upstream_timeout";

pub fn json_error(status: StatusCode, code: &str, msg: &str) -> Response {
    let body = serde_json::json!({ "error": msg, "code": code }).to_string();
    let mut resp = (status, body).into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    resp
}

pub fn err_429(code: &str, retry_after_s: u64, msg: &str) -> Response {
    let mut resp = json_error(StatusCode::TOO_MANY_REQUESTS, code, msg);
    if let Ok(v) = HeaderValue::from_str(&retry_after_s.to_string()) {
        resp.headers_mut().insert(header::RETRY_AFTER, v);
    }
    resp
}

pub fn err_401(msg: &str) -> Response {
    json_error(StatusCode::UNAUTHORIZED, CODE_UNAUTHORIZED, msg)
}

pub fn err_403(code: &str, msg: &str) -> Response {
    json_error(StatusCode::FORBIDDEN, code, msg)
}

pub fn err_404(code: &str, msg: &str) -> Response {
    json_error(StatusCode::NOT_FOUND, code, msg)
}

pub fn err_421(msg: &str) -> Response {
    json_error(StatusCode::MISDIRECTED_REQUEST, CODE_CLUSTER_UNKNOWN, msg)
}

pub fn err_413(msg: &str) -> Response {
    json_error(StatusCode::PAYLOAD_TOO_LARGE, CODE_PAYLOAD_TOO_LARGE, msg)
}

pub fn err_502(msg: &str) -> Response {
    json_error(StatusCode::BAD_GATEWAY, CODE_BAD_GATEWAY, msg)
}

pub fn err_504(msg: &str) -> Response {
    json_error(StatusCode::GATEWAY_TIMEOUT, CODE_UPSTREAM_TIMEOUT, msg)
}
