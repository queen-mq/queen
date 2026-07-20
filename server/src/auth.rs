//! JWT authentication for the segments broker — a real per-request validator
//! that mirrors the C++ auth (server/src/auth/{auth_middleware,jwt_validator}.cpp)
//! and its per-route access-level map (get_route_access_level).
//!
//! HS256 is verified by hand with `hmac` + `sha2` + `base64` (the same
//! dependency-light approach as server-rust/src/auth.rs), so the broker keeps
//! building offline. RS256/EdDSA are intentionally not implemented here — the
//! `auth` client tests only exercise HS256; a token that declares any other
//! algorithm is rejected 401.
//!
//! Access levels (PUBLIC < READ_ONLY < WRITE_ONLY < READ_WRITE < ADMIN) are NOT
//! a strict ladder: WRITE_ONLY is non-hierarchical (a produce-only role passes
//! `push` but is rejected on any read/consume route — issue #31), so the checks
//! are role-set membership tests, not numeric comparisons.

use crate::config::AuthConfig;
use axum::extract::{Request, State};
use axum::http::{header, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64URL, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

/// Authenticated producer identity, stamped into request extensions by the
/// middleware. `None` means: auth disabled, a skip/PUBLIC route, or a token
/// without a `sub`. Handlers read this to stamp `producerSub` — it is the ONLY
/// source of that value (a client-supplied `producerSub` is never honored).
#[derive(Clone, Debug)]
pub struct AuthedSub(pub Option<String>);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AccessLevel {
    Public,
    ReadOnly,
    WriteOnly,
    ReadWrite,
    Admin,
}

/// Decoded + validated JWT claims we care about.
struct Claims {
    sub: String,
    role: String,
    roles: Vec<String>,
    exp: Option<i64>,
    iat: Option<i64>,
    nbf: Option<i64>,
    iss: Option<String>,
    aud: Vec<String>,
}

impl Claims {
    fn has_role(&self, name: &str) -> bool {
        (!self.role.is_empty() && self.role == name) || self.roles.iter().any(|r| r == name)
    }
}

pub struct Authenticator {
    cfg: AuthConfig,
}

impl Authenticator {
    pub fn new(cfg: AuthConfig) -> Arc<Self> {
        Arc::new(Self { cfg })
    }

    // -- role -> capability (mirror JwtClaims::is_* in jwt_validator.cpp) --
    fn is_admin(&self, c: &Claims) -> bool {
        c.has_role(&self.cfg.role_admin)
    }
    fn is_read_write(&self, c: &Claims) -> bool {
        self.is_admin(c) || c.has_role(&self.cfg.role_read_write)
    }
    fn is_read_only(&self, c: &Claims) -> bool {
        // Any reader (read-only, read-write, admin). Excludes write-only.
        self.is_read_write(c) || c.has_role(&self.cfg.role_read_only)
    }
    fn is_write_only(&self, c: &Claims) -> bool {
        // Any writer (write-only, read-write, admin). Excludes read-only.
        self.is_read_write(c) || c.has_role(&self.cfg.role_write_only)
    }

    fn allows(&self, c: &Claims, level: AccessLevel) -> bool {
        match level {
            AccessLevel::Public => true,
            AccessLevel::ReadOnly => self.is_read_only(c),
            AccessLevel::WriteOnly => self.is_write_only(c),
            AccessLevel::ReadWrite => self.is_read_write(c),
            AccessLevel::Admin => self.is_admin(c),
        }
    }

    /// Full HS256 validation: signature, algorithm, exp/nbf/iat (±clock skew),
    /// and optional issuer/audience. Returns the decoded claims or an
    /// (HTTP status, message) pair for the middleware to surface.
    fn validate(&self, token: &str) -> Result<Claims, (StatusCode, &'static str)> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err((StatusCode::UNAUTHORIZED, "Malformed token"));
        }

        // Header: only HS256 is supported by this broker.
        let header_raw = B64URL
            .decode(parts[0])
            .map_err(|_| (StatusCode::UNAUTHORIZED, "Bad token header"))?;
        let header: serde_json::Value = serde_json::from_slice(&header_raw)
            .map_err(|_| (StatusCode::UNAUTHORIZED, "Bad token header"))?;
        let alg = header.get("alg").and_then(|a| a.as_str()).unwrap_or("");
        if alg != "HS256" {
            return Err((StatusCode::UNAUTHORIZED, "Unsupported token algorithm"));
        }
        if self.cfg.algorithm != "HS256" && self.cfg.algorithm != "auto" {
            return Err((StatusCode::UNAUTHORIZED, "Unsupported algorithm configuration"));
        }
        if self.cfg.secret.is_empty() {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, "HS256 secret not configured"));
        }

        // Signature (constant-time via HMAC verify).
        let signing_input = format!("{}.{}", parts[0], parts[1]);
        let sig = B64URL
            .decode(parts[2])
            .map_err(|_| (StatusCode::UNAUTHORIZED, "Bad token signature"))?;
        let mut mac = HmacSha256::new_from_slice(self.cfg.secret.as_bytes())
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "HMAC init failed"))?;
        mac.update(signing_input.as_bytes());
        if mac.verify_slice(&sig).is_err() {
            return Err((StatusCode::UNAUTHORIZED, "Invalid token signature"));
        }

        // Claims.
        let payload_raw = B64URL
            .decode(parts[1])
            .map_err(|_| (StatusCode::UNAUTHORIZED, "Bad token payload"))?;
        let v: serde_json::Value = serde_json::from_slice(&payload_raw)
            .map_err(|_| (StatusCode::UNAUTHORIZED, "Bad token payload"))?;
        let claims = self.parse_claims(&v);

        // Temporal checks with clock skew.
        let now = now_secs();
        let skew = self.cfg.clock_skew_seconds;
        if let Some(exp) = claims.exp {
            if now > exp + skew {
                return Err((StatusCode::UNAUTHORIZED, "Token has expired"));
            }
        }
        if let Some(nbf) = claims.nbf {
            if now + skew < nbf {
                return Err((StatusCode::UNAUTHORIZED, "Token not yet valid"));
            }
        }
        if let Some(iat) = claims.iat {
            if iat > now + skew {
                return Err((StatusCode::UNAUTHORIZED, "Token issued in the future"));
            }
        }

        // Optional issuer / audience.
        if !self.cfg.issuer.is_empty() && claims.iss.as_deref() != Some(self.cfg.issuer.as_str()) {
            return Err((StatusCode::UNAUTHORIZED, "Invalid token issuer"));
        }
        if !self.cfg.audience.is_empty() && !claims.aud.iter().any(|a| a == &self.cfg.audience) {
            return Err((StatusCode::UNAUTHORIZED, "Invalid token audience"));
        }

        Ok(claims)
    }

    fn parse_claims(&self, v: &serde_json::Value) -> Claims {
        let get_str = |k: &str| v.get(k).and_then(|x| x.as_str()).map(|s| s.to_string());
        let role = v
            .get(self.cfg.roles_claim.as_str())
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let mut roles = Vec::new();
        if let Some(arr) = v
            .get(self.cfg.roles_array_claim.as_str())
            .and_then(|x| x.as_array())
        {
            for it in arr {
                if let Some(r) = it.as_str() {
                    roles.push(r.to_string());
                }
            }
        }
        let mut aud = Vec::new();
        match v.get("aud") {
            Some(serde_json::Value::String(a)) => aud.push(a.clone()),
            Some(serde_json::Value::Array(arr)) => {
                for it in arr {
                    if let Some(a) = it.as_str() {
                        aud.push(a.to_string());
                    }
                }
            }
            _ => {}
        }
        Claims {
            sub: get_str("sub").unwrap_or_default(),
            role,
            roles,
            exp: v.get("exp").and_then(|x| x.as_i64()),
            iat: v.get("iat").and_then(|x| x.as_i64()),
            nbf: v.get("nbf").and_then(|x| x.as_i64()),
            iss: get_str("iss"),
            aud,
        }
    }
}

/// Per-route required access level — a faithful port of the C++
/// `get_route_access_level`, extended with the segments broker's `/streams/*`
/// surface (queries/cycle = READ_WRITE, state/get = READ_ONLY).
pub fn route_access_level(method: &Method, path: &str) -> AccessLevel {
    use AccessLevel::*;
    let m = method.as_str();

    // -------- PUBLIC (health / metrics / static / plain status) --------
    if path == "/health" || path == "/metrics" || path == "/metrics/prometheus" || path == "/status"
    {
        return Public;
    }
    if path == "/" || path.starts_with("/assets/") || path.starts_with("/favicon") {
        return Public;
    }

    // -------- ADMIN --------
    if path.starts_with("/api/v1/system/") || path.starts_with("/internal/") {
        return Admin;
    }
    if m == "DELETE" && path.starts_with("/api/v1/consumer-groups/") {
        return Admin;
    }
    if m == "DELETE" && path.starts_with("/api/v1/resources/queues/") {
        return Admin;
    }
    if path == "/api/v1/stats/refresh" {
        return Admin;
    }

    // -------- READ_ONLY (GET status / info endpoints) --------
    if m == "GET" {
        if path.starts_with("/api/v1/status") || path.starts_with("/api/v1/analytics") {
            return ReadOnly;
        }
        if path.starts_with("/api/v1/resources/") {
            return ReadOnly;
        }
        if path.starts_with("/api/v1/messages") {
            return ReadOnly;
        }
        if path.starts_with("/api/v1/consumer-groups") {
            return ReadOnly;
        }
        if path.starts_with("/api/v1/dlq") {
            return ReadOnly;
        }
        if path.starts_with("/api/v1/traces") {
            return ReadOnly;
        }
    }

    // -------- Streams --------
    if path == "/streams/v1/state/get" {
        return ReadOnly;
    }
    if path.starts_with("/streams/") {
        return ReadWrite;
    }

    // -------- WRITE_ONLY (pure produce; non-hierarchical, see issue #31) --------
    if path == "/api/v1/push" {
        return WriteOnly;
    }

    // -------- READ_WRITE (pop / ack / transaction / lease / configure / seek
    //          / subscription / traces POST / message DELETE / unknown) --------
    ReadWrite
}

fn extract_bearer(req: &Request) -> Option<String> {
    let raw = req.headers().get(header::AUTHORIZATION)?.to_str().ok()?;
    if raw.len() > 7 && raw[..7].eq_ignore_ascii_case("bearer ") {
        Some(raw[7..].to_string())
    } else {
        // Some clients send the bare token with no "Bearer " prefix.
        Some(raw.to_string())
    }
}

fn error_response(code: StatusCode, msg: &str) -> Response {
    (
        code,
        [(header::CONTENT_TYPE, "application/json")],
        format!("{{\"error\":\"{msg}\"}}"),
    )
        .into_response()
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Global axum layer. When auth is disabled it is a transparent pass-through
/// that stamps `AuthedSub(None)`. When enabled it enforces the route's required
/// level, short-circuiting 401 (missing/invalid token) or 403 (valid token,
/// insufficient level), and on success stamps the authenticated `sub`.
pub async fn auth_middleware(
    State(auth): State<Arc<Authenticator>>,
    mut req: Request,
    next: Next,
) -> Response {
    if !auth.cfg.enabled {
        req.extensions_mut().insert(AuthedSub(None));
        return next.run(req).await;
    }

    let path = req.uri().path().to_string();
    let method = req.method().clone();

    if auth.cfg.should_skip(&path) {
        req.extensions_mut().insert(AuthedSub(None));
        return next.run(req).await;
    }

    let level = route_access_level(&method, &path);
    if level == AccessLevel::Public {
        req.extensions_mut().insert(AuthedSub(None));
        return next.run(req).await;
    }

    let token = match extract_bearer(&req) {
        Some(t) => t,
        None => return error_response(StatusCode::UNAUTHORIZED, "Authentication required"),
    };

    let claims = match auth.validate(&token) {
        Ok(c) => c,
        Err((code, msg)) => return error_response(code, msg),
    };

    if !auth.allows(&claims, level) {
        return error_response(StatusCode::FORBIDDEN, "Insufficient permissions");
    }

    let sub = if claims.sub.is_empty() {
        None
    } else {
        Some(claims.sub.clone())
    };
    req.extensions_mut().insert(AuthedSub(sub));
    next.run(req).await
}
