//! Scoped requests at the HTTP edge (`queen_protocol::scope`): a client the
//! proxy authenticated with a scoped API key or a client token, whose rights
//! are a list of grants instead of the whole tenant.
//!
//! The scope reaches the router two ways: as a typed request extension, from
//! the proxy in this process, or as the [`SCOPE_HEADER`], on a request a
//! follower relayed to the leader whole (`QUEEN_RAFT_CLIENT_OFFLOAD=false`:
//! `handlers::raft::forward` keeps the headers and drops the extensions). The
//! header is believed wherever it comes from: a scope only ever narrows, so a
//! client that sends one restricts itself. One that does not decode refuses
//! the request; it is never ignored.
//!
//! [`scope_middleware`] resolves it once per request and refuses every route a
//! scoped client has no business on. The routes it lets through check the
//! names themselves, where they resolve them (`rsm::facade::check_scope`).

use std::sync::Arc;

use axum::extract::Request;
use axum::http::{header, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use queen_protocol::scope::{Ask, Scope, SCOPE_HEADER};

/// A request's scope, as the extension the handlers read.
pub type ClientScope = Arc<Scope>;

/// Global layer, inside tenancy: resolve the request's scope and refuse a
/// scoped request on a route outside [`scoped_route`]. A request with no scope
/// costs two map lookups.
pub async fn scope_middleware(mut req: Request, next: Next) -> Response {
    let scoped = if req.extensions().get::<ClientScope>().is_some() {
        true
    } else if let Some(v) = req.headers().get(SCOPE_HEADER) {
        match Scope::decode(v.as_bytes()) {
            Ok(s) => {
                req.extensions_mut().insert::<ClientScope>(Arc::new(s));
                true
            }
            Err(e) => return refused("scope_invalid", &e),
        }
    } else {
        false
    };
    if scoped && !scoped_route(req.method(), req.uri().path()) {
        return refused(
            "scope_denied",
            "this credential may push, pop, ack, renew a lease, and use transactions, KV and \
             timers, each on the names its grants cover: not this route",
        );
    }
    next.run(req).await
}

/// `403` with the broker's error body.
pub fn refused(code: &str, message: &str) -> Response {
    let mut body = String::from("{\"error\":\"");
    crate::util::json_escape_into(&mut body, message);
    body.push_str("\",\"code\":\"");
    crate::util::json_escape_into(&mut body, code);
    body.push_str("\"}");
    (
        StatusCode::FORBIDDEN,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

/// The refusal of `ask` when `scope` does not cover it; `None` when it does,
/// or when the request has no scope.
pub fn denied(scope: Option<&Scope>, ask: Ask<'_>) -> Option<Response> {
    match scope {
        Some(s) if !s.allows(&ask) => Some(crate::handlers::raft::err_response(
            crate::rsm::facade::scope_denied(&ask),
        )),
        _ => None,
    }
}

/// The routes a scoped request may reach: `queen_protocol::scope::scoped_route`,
/// the one list the proxy checks too, exactly as `handlers::raft::
/// build_raft_router` spells them. Each checks the names it touches. A
/// near-miss the router would not match reaches the fallback, which refuses a
/// scoped request too.
pub fn scoped_route(method: &Method, path: &str) -> bool {
    queen_protocol::scope::scoped_route(method.as_str(), path)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The list itself is pinned in `queen_protocol::scope`; this pins that the
    /// router's methods reach it spelled as that list spells them.
    #[test]
    fn the_router_asks_the_shared_list_with_its_own_methods() {
        assert!(scoped_route(&Method::POST, "/api/v1/push"));
        assert!(scoped_route(
            &Method::HEAD,
            "/api/v1/pop/queue/q/partition/p"
        ));
        assert!(scoped_route(&Method::DELETE, "/api/v1/timers/q/k"));
        assert!(!scoped_route(&Method::GET, "/api/v1/resources/queues"));
        assert!(!scoped_route(&Method::PATCH, "/api/v1/kv/ns/k"));
    }
}
