//! Track B (native tenant scoping, PLAN_QUEEN_PROXY_CLOUD.md §5) — the per-request
//! tenant resolution boundary.
//!
//! The broker gains ONE opaque concept: a `tenant_id` scoping key on queue
//! identity, taken from the trusted `x-queen-tenant` header the colocated proxy
//! sets. This middleware resolves it ONCE per request and stamps a `Tenant` into
//! the request extensions; every handler that resolves a queue by name reads it
//! and threads it to the db:: layer, which binds it to the SQL name-resolution
//! functions.
//!
//! Semantics (identical-when-off is load-bearing — the OSS 117/117 suite must stay
//! green):
//!   * flag OFF → every request uses `config::DEFAULT_TENANT` (byte-identical
//!     to pre-Track-B; the DDL column defaults to the same constant) — except
//!     a relay another broker of the cluster PROVES with the cluster token,
//!     whose header is resolved as with the flag on (`tenant_middleware`).
//!   * flag ON, header absent/empty → `config::DEFAULT_TENANT`.
//!   * flag ON, header present + valid UUID → that tenant (opaque; NOT validated
//!     against anything — trust is the cell network, per §2/§5).
//!   * flag ON, header present + malformed → 400 (never reaches PG as a cast error).

use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::config::{DEFAULT_TENANT, TENANT_HEADER};

/// Per-request resolved tenant scoping key: a canonical lowercase UUID string.
/// Carried as text because the broker binds tenants to SQL via `$n::text::uuid`
/// (the same pattern db.rs already uses for every uuid argument — no `uuid` crate).
#[derive(Clone, Debug)]
pub struct Tenant(pub String);

impl Tenant {
    #[inline]
    pub fn default_tenant() -> Self {
        Tenant(DEFAULT_TENANT.to_string())
    }
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for Tenant {
    fn default() -> Self {
        Tenant::default_tenant()
    }
}

/// Small, cheap-to-clone state carried by the tenant middleware: just the flag.
#[derive(Clone, Copy)]
pub struct TenancyConfig {
    pub enabled: bool,
}

/// Validate + canonicalize an 8-4-4-4-12 hex UUID. Returns the lowercase form, or
/// None when malformed. The broker does NOT check the tenant against any registry
/// (it is opaque), but a malformed value must be rejected here so it never reaches
/// a `::uuid` cast (which would surface as a 500, not the intended 400).
pub fn parse_tenant_uuid(s: &str) -> Option<String> {
    let b = s.as_bytes();
    if b.len() != 36 {
        return None;
    }
    for (i, c) in b.iter().enumerate() {
        match i {
            8 | 13 | 18 | 23 => {
                if *c != b'-' {
                    return None;
                }
            }
            _ => {
                if !c.is_ascii_hexdigit() {
                    return None;
                }
            }
        }
    }
    Some(s.to_ascii_lowercase())
}

fn bad_tenant() -> Response {
    (
        StatusCode::BAD_REQUEST,
        [(header::CONTENT_TYPE, "application/json")],
        "{\"error\":\"invalid x-queen-tenant header (must be a UUID)\"}",
    )
        .into_response()
}

fn unverified_relay() -> Response {
    (
        StatusCode::FORBIDDEN,
        [(header::CONTENT_TYPE, "application/json")],
        "{\"error\":\"a relayed request names a tenant this broker cannot verify: set the same \
         QUEEN_RAFT_TOKEN on every node\",\"code\":\"tenant_unverified\"}",
    )
        .into_response()
}

/// The tenant the header names: absent or empty ⇒ the default tenant, a UUID ⇒
/// that tenant, anything else (non-ASCII included) ⇒ 400, never a silent default.
// The error is the HTTP answer itself, built only on the cold path.
#[allow(clippy::result_large_err)]
fn header_tenant(headers: &axum::http::HeaderMap) -> Result<Tenant, Response> {
    let Some(v) = headers.get(TENANT_HEADER) else {
        return Ok(Tenant::default_tenant());
    };
    let s = v.to_str().map_err(|_| bad_tenant())?.trim();
    if s.is_empty() {
        return Ok(Tenant::default_tenant());
    }
    parse_tenant_uuid(s).map(Tenant).ok_or_else(bad_tenant)
}

/// Global axum layer that runs on every request. Off ⇒ the default tenant, with
/// one exception below. On ⇒ resolves the header to a `Tenant`, 400-ing a
/// malformed value.
///
/// THE EXCEPTION: a relay from another broker of this cluster
/// (`peerclient::verified_relay`, the forward mark AND the cluster token). It
/// names the tenant the sending broker resolved at ITS edge, and a node's
/// routers do not share the flag: behind the single binary's proxy the router
/// has tenancy on while PORT serves one with `QUEEN_TENANCY_HEADER` off. A relay
/// that dropped to the default tenant here stored tenant A's ephemeral message
/// where any default-tenant client of the owner could pop it. A relay that
/// carries the mark and another tenant but cannot prove itself is refused,
/// never re-scoped to the default tenant.
pub async fn tenant_middleware(
    State(cfg): State<TenancyConfig>,
    mut req: Request,
    next: Next,
) -> Response {
    match resolve(cfg.enabled, req.headers()) {
        Ok(tenant) => {
            req.extensions_mut().insert(tenant);
            next.run(req).await
        }
        Err(r) => r,
    }
}

/// [`tenant_middleware`]'s decision, `enabled` being the router's flag.
#[allow(clippy::result_large_err)]
fn resolve(enabled: bool, headers: &axum::http::HeaderMap) -> Result<Tenant, Response> {
    if enabled {
        return header_tenant(headers);
    }
    if !headers.contains_key(crate::peerclient::FWD_HEADER) {
        return Ok(Tenant::default_tenant());
    }
    if crate::peerclient::verified_relay(headers) {
        return header_tenant(headers);
    }
    if names_another_tenant(headers) {
        return Err(unverified_relay());
    }
    Ok(Tenant::default_tenant())
}

/// Does the header name a tenant other than the default one? A value that is
/// not one (malformed, non-ASCII) counts as another: it is not the default.
fn names_another_tenant(headers: &axum::http::HeaderMap) -> bool {
    let Some(v) = headers.get(TENANT_HEADER) else {
        return false;
    };
    match v.to_str().map(str::trim) {
        Ok("") => false,
        Ok(s) => parse_tenant_uuid(s).as_deref() != Some(DEFAULT_TENANT),
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_canonical_uuid_and_lowercases() {
        assert_eq!(
            parse_tenant_uuid("00000000-0000-0000-0000-000000000001").as_deref(),
            Some("00000000-0000-0000-0000-000000000001")
        );
        assert_eq!(
            parse_tenant_uuid("AABBCCDD-1122-3344-5566-778899AABBCC").as_deref(),
            Some("aabbccdd-1122-3344-5566-778899aabbcc")
        );
    }

    #[test]
    fn rejects_malformed() {
        assert!(parse_tenant_uuid("").is_none());
        assert!(parse_tenant_uuid("not-a-uuid").is_none());
        assert!(parse_tenant_uuid("00000000000000000000000000000001").is_none()); // no dashes
        assert!(parse_tenant_uuid("00000000-0000-0000-0000-00000000000g").is_none()); // non-hex
        assert!(parse_tenant_uuid("00000000-0000-0000-0000-0000000000012").is_none());
        // too long
    }

    use axum::http::{HeaderMap, HeaderValue};

    const ACME: &str = "aabbccdd-1122-3344-5566-778899aabbcc";

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(*k, HeaderValue::from_str(v).unwrap());
        }
        h
    }

    fn tenant(enabled: bool, h: &HeaderMap) -> Result<String, StatusCode> {
        resolve(enabled, h).map(|t| t.0).map_err(|r| r.status())
    }

    /// QUEEN_TENANCY_HEADER off, the router PORT serves next to a proxy on its
    /// own port: a client's header is ignored as before, a relay proven by the
    /// cluster token keeps the tenant its sending broker resolved, and a relay
    /// that cannot prove itself is refused rather than stored as the default
    /// tenant's.
    #[test]
    fn with_tenancy_off_only_a_verified_relay_names_its_tenant() {
        use crate::peerclient::{FWD_HEADER, TOKEN_HEADER};
        let token = crate::peerclient::test_token();

        // A client: the default tenant, whatever it sends.
        assert_eq!(tenant(false, &HeaderMap::new()).unwrap(), DEFAULT_TENANT);
        assert_eq!(
            tenant(false, &headers(&[(TENANT_HEADER, ACME)])).unwrap(),
            DEFAULT_TENANT
        );
        assert_eq!(
            tenant(false, &headers(&[(TENANT_HEADER, "not-a-uuid")])).unwrap(),
            DEFAULT_TENANT
        );

        // A relay another broker proves: its tenant, parsed like the flag on.
        let relay =
            |t: &str| headers(&[(FWD_HEADER, "1"), (TOKEN_HEADER, token), (TENANT_HEADER, t)]);
        assert_eq!(tenant(false, &relay(ACME)).unwrap(), ACME);
        assert_eq!(
            tenant(false, &relay(&ACME.to_ascii_uppercase())).unwrap(),
            ACME
        );
        assert_eq!(
            tenant(false, &relay(DEFAULT_TENANT)).unwrap(),
            DEFAULT_TENANT
        );
        assert_eq!(tenant(false, &relay("")).unwrap(), DEFAULT_TENANT);
        assert_eq!(
            tenant(false, &relay("not-a-uuid")).unwrap_err(),
            StatusCode::BAD_REQUEST
        );

        // The mark without the token, or with the wrong one, naming another
        // tenant: refused. Naming the default tenant (or none): the default.
        for spoof in [
            headers(&[(FWD_HEADER, "1"), (TENANT_HEADER, ACME)]),
            headers(&[
                (FWD_HEADER, "1"),
                (TOKEN_HEADER, "guess"),
                (TENANT_HEADER, ACME),
            ]),
            headers(&[(FWD_HEADER, "1"), (TENANT_HEADER, "not-a-uuid")]),
        ] {
            assert_eq!(tenant(false, &spoof).unwrap_err(), StatusCode::FORBIDDEN);
        }
        assert_eq!(
            tenant(
                false,
                &headers(&[(FWD_HEADER, "1"), (TENANT_HEADER, DEFAULT_TENANT)])
            )
            .unwrap(),
            DEFAULT_TENANT
        );
        assert_eq!(
            tenant(false, &headers(&[(FWD_HEADER, "1")])).unwrap(),
            DEFAULT_TENANT
        );
    }

    /// QUEEN_TENANCY_HEADER on (the router behind the proxy): unchanged, the
    /// header is the tenant for every caller.
    #[test]
    fn with_tenancy_on_the_header_is_the_tenant() {
        assert_eq!(tenant(true, &HeaderMap::new()).unwrap(), DEFAULT_TENANT);
        assert_eq!(
            tenant(true, &headers(&[(TENANT_HEADER, ACME)])).unwrap(),
            ACME
        );
        assert_eq!(
            tenant(true, &headers(&[(TENANT_HEADER, "not-a-uuid")])).unwrap_err(),
            StatusCode::BAD_REQUEST
        );
    }
}
