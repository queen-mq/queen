//! BROKER → BROKER HTTP — EPHEMERAL_QUEUES.md §3.6.
//!
//! One pooled client, used by exactly one caller: the ephemeral handler, when a
//! (queue, partition) belongs to another broker of the same cell (§3.7) and the
//! request has to be relayed to its owner.
//!
//! ---------------------------------------------------------------------------
//! NO REQWEST, BY DESIGN (M2)
//!
//! The repository has one HTTP client pattern and this is a copy of it: the
//! proxy's `hyper_util::client::legacy::Client` (`proxy/src/main.rs`), with the
//! same two settings and for the same two reasons.
//!
//!   * `set_nodelay(true)` — every body on this path is small (a push item, a
//!     pop batch, an ack list) and Nagle would sit on it for up to ~40 ms
//!     waiting to coalesce, on a keep-alive pool with one request in flight.
//!     That would be a latency tax on the exact hop §7.6 exists to measure.
//!   * `pool_max_idle_per_host` — the peer set of a cell is tiny and stable, so
//!     the pool is reused rather than churned; a modest ceiling keeps the idle
//!     set bounded without ever closing a live connection.
//!
//! Adding a second HTTP stack (reqwest brings its own TLS, its own resolver and
//! its own runtime assumptions) to a binary that already links hyper through
//! axum would be paying twice for one capability.
//!
//! ---------------------------------------------------------------------------
//! WHAT THIS MODULE DELIBERATELY DOES NOT DO
//!
//! No retries, no circuit breaker, no health tracking. A forward is one attempt
//! against one address, and its failure is a 503 the caller renders — because on
//! this class the alternative to a fast honest failure is not durability, it is a
//! longer wait for the same answer. The ONE retry the design does allow (an
//! `owner_moved` re-hash, §3.6) is a placement decision and lives with the
//! placement code, not here.
//!
//! Plaintext `http` only. Mesh traffic is intra-cell — the same trust boundary
//! the TCP mesh's HMAC handshake assumes — and the peer address is advertised by
//! that authenticated handshake, never discovered from a request.

use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request};
use http_body_util::BodyExt;

/// Cap on a relayed response body. Generous enough for the largest legal pop
/// (10 000 messages against the edge's body limit) and bounded so a hostile or
/// broken peer cannot make this broker buffer without limit on ITS behalf.
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// Marks a request as ALREADY FORWARDED (§3.6). A broker that sees it and finds
/// it is not the owner answers `owner_moved` instead of forwarding again, which
/// is what makes a forwarding loop unrepresentable rather than merely unlikely
/// during the seconds of membership disagreement §1.4 permits.
pub const FWD_HEADER: &str = "x-queen-eph-fwd";

/// The cluster's shared secret on the HTTP port (`QUEEN_RAFT_TOKEN`, the value
/// every Raft RPC already carries). A request presenting it comes from another
/// broker of this cluster: a relayed ephemeral request, a ring hand-over, a
/// drain notice. The proxy strips it from client requests.
pub const TOKEN_HEADER: &str = "x-queen-raft-token";

/// `QUEEN_RAFT_TOKEN`, read once ([`cluster_token`]).
static TOKEN: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();

/// `QUEEN_RAFT_TOKEN`, read once. `None` when unset or blank: a cluster without
/// a token (a dev cluster) has no credential for broker-to-broker calls.
fn cluster_token() -> Option<&'static str> {
    TOKEN
        .get_or_init(|| {
            std::env::var("QUEEN_RAFT_TOKEN")
                .ok()
                .filter(|t| !t.trim().is_empty())
        })
        .as_deref()
}

/// Does `h` carry `want`? `false` when there is nothing to match. Compared in
/// constant time: it is a credential.
fn token_ok(want: Option<&str>, h: &HeaderMap) -> bool {
    let (Some(want), Some(got)) = (want, h.get(TOKEN_HEADER)) else {
        return false;
    };
    let (a, b) = (want.as_bytes(), got.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Does this request carry the cluster token? Always `false` when no token is
/// configured.
pub fn token_matches(h: &HeaderMap) -> bool {
    token_ok(cluster_token(), h)
}

/// Was this request relayed by another broker, and may it be believed?
///
/// The mark alone used to be enough, and a client could set it: a relayed push
/// skips the per-tenant rate charge. With a token configured the mark counts
/// only next to the token; without one (a dev cluster) the mark alone, as before.
pub fn forwarded(h: &HeaderMap) -> bool {
    if !h.contains_key(FWD_HEADER) {
        return false;
    }
    match cluster_token() {
        Some(_) => token_matches(h),
        None => true,
    }
}

/// A relay PROVEN to come from another broker of this cluster: the forward
/// mark and the cluster token. Unlike [`forwarded`] it is never true without a
/// configured token, because of what it unlocks: the tenant the relay names
/// (`tenant.rs`), believed whatever `QUEEN_TENANCY_HEADER` says, and the
/// broker router behind a proxy that shares PORT (`proxy_embed.rs`). A
/// client that sets the mark alone gets neither.
pub fn verified_relay(h: &HeaderMap) -> bool {
    relay_proven(cluster_token(), h)
}

fn relay_proven(want: Option<&str>, h: &HeaderMap) -> bool {
    h.contains_key(FWD_HEADER) && token_ok(want, h)
}

/// The token every test that needs one shares: the first caller fixes the
/// process's token ([`TOKEN`] is read once), so they must all agree on it.
/// Panics when something already read an unset `QUEEN_RAFT_TOKEN`.
#[cfg(test)]
pub(crate) fn test_token() -> &'static str {
    const TEST_TOKEN: &str = "test-cluster-token";
    TOKEN.get_or_init(|| Some(TEST_TOKEN.to_string()));
    let t = cluster_token().expect("the cluster token was read unset before a test fixed it");
    assert_eq!(
        t, TEST_TOKEN,
        "the cluster token was read from the environment first"
    );
    t
}

/// The headers that make a request this broker sends to a peer recognisable
/// as internal: the forward mark, and the token when one is configured.
pub fn internal_headers() -> Vec<(HeaderName, HeaderValue)> {
    let mut out = Vec::with_capacity(2);
    out.push((
        HeaderName::from_static(FWD_HEADER),
        HeaderValue::from_static("1"),
    ));
    if let Some(t) = cluster_token() {
        if let Ok(v) = HeaderValue::from_str(t) {
            out.push((HeaderName::from_static(TOKEN_HEADER), v));
        }
    }
    out
}

/// What a peer answered. Status and bytes only: the ephemeral wire is JSON with
/// no meaningful response headers, and copying an arbitrary header set from one
/// broker's response into another's is how a hop-by-hop header becomes a bug.
pub struct PeerResponse {
    pub status: axum::http::StatusCode,
    pub body: Bytes,
    /// The ONE header worth carrying back to the caller: a 429 from the owner's
    /// rate bucket without its `Retry-After` is a refusal the client cannot pace
    /// against. Everything else is deliberately dropped (see the struct's note).
    pub retry_after: Option<HeaderValue>,
}

pub struct PeerClient {
    inner: hyper_util::client::legacy::Client<
        hyper_util::client::legacy::connect::HttpConnector,
        Body,
    >,
}

impl PeerClient {
    pub fn new() -> PeerClient {
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.set_nodelay(true);
        let inner =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .pool_max_idle_per_host(64)
                .build::<_, Body>(connector);
        PeerClient { inner }
    }

    /// One relayed request. `Err` is a human string for the log line and the
    /// 503's message — never a status, because a transport failure has no status
    /// and inventing one here would let the caller confuse "the peer refused"
    /// with "the peer could not be reached".
    ///
    /// `deadline` bounds the whole call INCLUDING the body read. It is the
    /// caller's timeout plus slack (§3.6): a forwarded `wait=true` pop is held
    /// open at the owner for the client's full timeout, so an internal deadline
    /// equal to that timeout would race the peer's own answer and turn a
    /// legitimately empty long-poll into a spurious 503.
    pub async fn call(
        &self,
        method: Method,
        url: &str,
        headers: &[(HeaderName, HeaderValue)],
        body: Bytes,
        deadline: Duration,
    ) -> Result<PeerResponse, String> {
        let mut req = Request::builder().method(method).uri(url);
        {
            let h = req
                .headers_mut()
                .ok_or_else(|| "bad peer url".to_string())?;
            let mut base = HeaderMap::new();
            base.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            for (k, v) in headers {
                base.insert(k.clone(), v.clone());
            }
            *h = base;
        }
        let req = req.body(Body::from(body)).map_err(|e| e.to_string())?;

        let resp = tokio::time::timeout(deadline, self.inner.request(req))
            .await
            .map_err(|_| "peer did not answer within the forwarding deadline".to_string())?
            .map_err(|e| e.to_string())?;
        let status = resp.status();
        let retry_after = resp.headers().get(axum::http::header::RETRY_AFTER).cloned();
        let collected = tokio::time::timeout(deadline, resp.into_body().collect())
            .await
            .map_err(|_| "peer response body stalled".to_string())?
            .map_err(|e| e.to_string())?;
        let bytes = collected.to_bytes();
        if bytes.len() > MAX_RESPONSE_BYTES {
            return Err("peer response exceeds the relay cap".to_string());
        }
        Ok(PeerResponse {
            status,
            body: bytes,
            retry_after,
        })
    }
}

impl Default for PeerClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(*k, HeaderValue::from_str(v).unwrap());
        }
        h
    }

    /// The mark proves nothing without the token, and nothing at all on a
    /// cluster with no token: what a verified relay unlocks (the tenant it
    /// names, the broker router behind the proxy) is never a client's to take.
    #[test]
    fn a_relay_is_verified_only_by_the_mark_and_the_configured_token() {
        let both = headers(&[(FWD_HEADER, "1"), (TOKEN_HEADER, "s3cret")]);
        assert!(relay_proven(Some("s3cret"), &both));
        // No token configured (a dev cluster): the mark alone is never proof.
        assert!(!relay_proven(None, &both));
        assert!(!relay_proven(None, &headers(&[(FWD_HEADER, "1")])));
        // The wrong token, a token of another length, no token.
        assert!(!relay_proven(
            Some("s3cret"),
            &headers(&[(FWD_HEADER, "1"), (TOKEN_HEADER, "s3creT")])
        ));
        assert!(!relay_proven(
            Some("s3cret"),
            &headers(&[(FWD_HEADER, "1"), (TOKEN_HEADER, "s3cret-and-more")])
        ));
        assert!(!relay_proven(
            Some("s3cret"),
            &headers(&[(FWD_HEADER, "1")])
        ));
        // The token without the mark is a call, not a relay.
        assert!(!relay_proven(
            Some("s3cret"),
            &headers(&[(TOKEN_HEADER, "s3cret")])
        ));
    }

    /// What this broker sends a peer is what the peer's check accepts.
    #[test]
    fn internal_headers_make_a_verified_relay() {
        let token = test_token();
        let mut h = HeaderMap::new();
        for (k, v) in internal_headers() {
            h.insert(k, v);
        }
        assert_eq!(h.get(TOKEN_HEADER).unwrap(), token);
        assert!(verified_relay(&h));
        assert!(forwarded(&h));
        h.remove(TOKEN_HEADER);
        assert!(!verified_relay(&h));
        assert!(
            !forwarded(&h),
            "with a token configured the mark alone is not believed"
        );
    }
}
