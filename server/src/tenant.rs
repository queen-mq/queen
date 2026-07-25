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
//!   * flag OFF  → every request uses `config::DEFAULT_TENANT` (byte-identical to
//!                 pre-Track-B; the DDL column defaults to the same constant).
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

/// Global axum layer that runs on every request. Off ⇒ a transparent pass-through
/// that stamps the default tenant (zero header reads). On ⇒ resolves the header to
/// a `Tenant`, 400-ing a malformed value.
pub async fn tenant_middleware(
    State(cfg): State<TenancyConfig>,
    mut req: Request,
    next: Next,
) -> Response {
    if !cfg.enabled {
        req.extensions_mut().insert(Tenant::default_tenant());
        return next.run(req).await;
    }
    let tenant = match req.headers().get(TENANT_HEADER) {
        None => Tenant::default_tenant(),
        Some(v) => {
            // A non-ASCII header value is malformed → 400 (never a silent default).
            let s = match v.to_str() {
                Ok(s) => s.trim(),
                Err(_) => return bad_tenant(),
            };
            if s.is_empty() {
                Tenant::default_tenant()
            } else {
                match parse_tenant_uuid(s) {
                    Some(u) => Tenant(u),
                    None => return bad_tenant(),
                }
            }
        }
    };
    req.extensions_mut().insert(tenant);
    next.run(req).await
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
        assert!(parse_tenant_uuid("00000000-0000-0000-0000-0000000000012").is_none()); // too long
    }
}
