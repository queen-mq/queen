//! JWT authentication for the segments broker — a real per-request validator
//! that mirrors the C++ auth (server/src/auth/{auth_middleware,jwt_validator}.cpp)
//! and its per-route access-level map (get_route_access_level).
//!
//! RUSTFIX item 7: signatures are verified with the `jsonwebtoken` crate, which
//! supports HS256/384/512, RS256/384/512 and EdDSA. `JWT_ALGORITHM=auto` dispatches
//! on the token header. RS256/EdDSA keys come from a static `JWT_PUBLIC_KEY` PEM or
//! from a JWKS endpoint (cached by `kid`, refreshed on unknown-kid and on an
//! interval). The temporal (exp/nbf/iat ±clock skew) and issuer/audience checks
//! stay in this file (the crate's own checks are disabled) so those semantics
//! remain byte-for-byte at C++ parity.
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
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

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
    /// JWKS decoding keys, keyed by `kid` (empty string key = a single-key set's
    /// fallback). Refreshed by `fetch_jwks`.
    jwks: RwLock<HashMap<String, DecodingKey>>,
    /// Unix seconds of the last JWKS fetch — throttles refresh-on-miss.
    last_refresh: AtomicI64,
}

impl Authenticator {
    pub fn new(cfg: AuthConfig) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            jwks: RwLock::new(HashMap::new()),
            last_refresh: AtomicI64::new(0),
        })
    }

    /// Whether this deployment should pre-fetch / refresh JWKS on startup.
    pub fn uses_jwks(&self) -> bool {
        self.cfg.enabled
            && !self.cfg.jwks_url.is_empty()
            && matches!(self.cfg.algorithm.as_str(), "RS256" | "RS384" | "RS512" | "EdDSA" | "auto")
    }

    pub fn jwks_refresh_interval(&self) -> Duration {
        Duration::from_secs(self.cfg.jwks_refresh_interval_seconds.max(1) as u64)
    }

    /// GET the JWKS endpoint and rebuild the key cache. Returns the key count.
    pub async fn fetch_jwks(&self) -> Result<usize, String> {
        if self.cfg.jwks_url.is_empty() {
            return Err("JWT_JWKS_URL not set".into());
        }
        let timeout = Duration::from_millis(self.cfg.jwks_request_timeout_ms.max(1) as u64);
        let json = crate::httpget::get_json(&self.cfg.jwks_url, timeout).await?;
        let keys = json
            .get("keys")
            .and_then(|k| k.as_array())
            .ok_or("JWKS has no `keys` array")?;
        let mut map = HashMap::new();
        for k in keys {
            if let Some((kid, dk)) = jwk_to_decoding_key(k) {
                map.insert(kid, dk);
            }
        }
        let n = map.len();
        *self.jwks.write().await = map;
        self.last_refresh.store(now_secs(), Ordering::Relaxed);
        Ok(n)
    }

    /// Look up a cached JWKS key by `kid`; refresh once (throttled) on a miss so a
    /// rotated key is picked up without a restart (jwt_validator.cpp:279).
    async fn key_for_kid(&self, kid: &str) -> Option<DecodingKey> {
        if let Some(k) = self.lookup(kid).await {
            return Some(k);
        }
        // Throttle refresh-on-miss to avoid a fetch stampede under load.
        if now_secs() - self.last_refresh.load(Ordering::Relaxed) >= 5 {
            let _ = self.fetch_jwks().await;
        }
        self.lookup(kid).await
    }

    async fn lookup(&self, kid: &str) -> Option<DecodingKey> {
        let map = self.jwks.read().await;
        if kid.is_empty() {
            // Empty kid: single-key fallback (get_public_key_for_kid:551).
            map.values().next().cloned()
        } else {
            map.get(kid).cloned()
        }
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

    /// Full validation: signature (HS256/384/512, RS256/384/512, EdDSA per the
    /// configured algorithm + token header), exp/nbf/iat (±clock skew), and
    /// optional issuer/audience. Returns the decoded claims or an (HTTP status,
    /// message) pair for the middleware to surface.
    async fn validate(&self, token: &str) -> Result<Claims, (StatusCode, &'static str)> {
        let header = jsonwebtoken::decode_header(token)
            .map_err(|_| (StatusCode::UNAUTHORIZED, "Bad token header"))?;
        let alg = header.alg;

        // Algorithm dispatch (jwt_validator.cpp:88-116): the token's alg must be
        // permitted by the configured JWT_ALGORITHM.
        self.check_alg_allowed(alg)?;

        // Select the verification key for this alg.
        let key = self.decoding_key_for(alg, header.kid.as_deref()).await?;

        // Verify ONLY the signature + alg with jsonwebtoken; the crate's own
        // temporal/audience checks are disabled so the parity code below (with the
        // configured clock skew, and treating exp as OPTIONAL like C++ jwt-cpp) is
        // the single source of truth. required_spec_claims is cleared so a token
        // with no `exp` is accepted (jsonwebtoken otherwise requires it).
        let mut val = Validation::new(alg);
        val.algorithms = vec![alg];
        val.required_spec_claims = std::collections::HashSet::new();
        val.validate_exp = false;
        val.validate_nbf = false;
        val.validate_aud = false;
        let data = jsonwebtoken::decode::<serde_json::Value>(token, &key, &val)
            .map_err(|_| (StatusCode::UNAUTHORIZED, "Invalid token signature"))?;
        let claims = self.parse_claims(&data.claims);

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

    /// Is the token's `alg` permitted by the configured JWT_ALGORITHM?
    /// (jwt_validator.cpp:88-116: auto accepts HS256/RS*/EdDSA; explicit configs
    /// require a matching family.)
    fn check_alg_allowed(&self, alg: Algorithm) -> Result<(), (StatusCode, &'static str)> {
        use Algorithm::*;
        let ok = match self.cfg.algorithm.as_str() {
            "auto" => matches!(alg, HS256 | HS384 | HS512 | RS256 | RS384 | RS512 | EdDSA),
            "HS256" => matches!(alg, HS256),
            "HS384" => matches!(alg, HS384),
            "HS512" => matches!(alg, HS512),
            "RS256" => matches!(alg, RS256 | RS384 | RS512),
            "RS384" => matches!(alg, RS384),
            "RS512" => matches!(alg, RS512),
            "EdDSA" => matches!(alg, EdDSA),
            _ => false,
        };
        if ok {
            Ok(())
        } else {
            Err((StatusCode::UNAUTHORIZED, "Unsupported token algorithm"))
        }
    }

    /// Choose the verification key for `alg`: HMAC secret for HS*, else the static
    /// JWT_PUBLIC_KEY PEM if set, else the JWKS key for `kid` (jwt_validator.cpp
    /// :269-287,408-426).
    async fn decoding_key_for(
        &self,
        alg: Algorithm,
        kid: Option<&str>,
    ) -> Result<DecodingKey, (StatusCode, &'static str)> {
        use Algorithm::*;
        match alg {
            HS256 | HS384 | HS512 => {
                if self.cfg.secret.is_empty() {
                    return Err((StatusCode::INTERNAL_SERVER_ERROR, "HS secret not configured"));
                }
                Ok(DecodingKey::from_secret(self.cfg.secret.as_bytes()))
            }
            RS256 | RS384 | RS512 => {
                if !self.cfg.public_key.is_empty() {
                    DecodingKey::from_rsa_pem(self.cfg.public_key.as_bytes())
                        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "invalid RSA public key PEM"))
                } else {
                    self.key_for_kid(kid.unwrap_or("")).await
                        .ok_or((StatusCode::UNAUTHORIZED, "Unknown key ID"))
                }
            }
            EdDSA => {
                if !self.cfg.public_key.is_empty() {
                    DecodingKey::from_ed_pem(self.cfg.public_key.as_bytes())
                        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "invalid Ed public key PEM"))
                } else {
                    self.key_for_kid(kid.unwrap_or("")).await
                        .ok_or((StatusCode::UNAUTHORIZED, "Unknown key ID"))
                }
            }
            _ => Err((StatusCode::UNAUTHORIZED, "Unsupported token algorithm")),
        }
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

/// Convert one JWK object to a (kid, DecodingKey). RSA uses the n/e components;
/// OKP/Ed25519 uses the raw 32-byte `x` public key (ring expects it raw). Returns
/// None for unsupported key types / malformed keys.
fn jwk_to_decoding_key(k: &serde_json::Value) -> Option<(String, DecodingKey)> {
    let kty = k.get("kty")?.as_str()?;
    let kid = k.get("kid").and_then(|x| x.as_str()).unwrap_or("").to_string();
    match kty {
        "RSA" => {
            let n = k.get("n")?.as_str()?;
            let e = k.get("e")?.as_str()?;
            let dk = DecodingKey::from_rsa_components(n, e).ok()?;
            Some((kid, dk))
        }
        "OKP" => {
            if k.get("crv").and_then(|x| x.as_str()) != Some("Ed25519") {
                return None;
            }
            let x = k.get("x")?.as_str()?;
            let raw = B64URL.decode(x).ok()?;
            if raw.len() != 32 {
                return None;
            }
            // from_ed_der takes the raw 32-byte Ed25519 public key (ring's form).
            let dk = DecodingKey::from_ed_der(&raw);
            Some((kid, dk))
        }
        _ => None,
    }
}

/// Per-route required access level — a faithful port of the C++
/// `get_route_access_level`, extended with the segments broker's `/streams/*`
/// surface (queries/cycle = READ_WRITE, state/get = READ_ONLY).
pub fn route_access_level(method: &Method, path: &str) -> AccessLevel {
    use AccessLevel::*;
    let m = method.as_str();

    // -------- PUBLIC (health / metrics / static) --------
    // RUSTFIX item 7: /status is NOT public (C++ had no /status branch); it is
    // demoted to READ_ONLY below so broker info is not served unauthenticated.
    if path == "/health" || path == "/metrics" || path == "/metrics/prometheus" {
        return Public;
    }
    if path == "/" || path.starts_with("/assets/") || path.starts_with("/favicon") {
        return Public;
    }
    // Broker-direct dashboard identity surface (handlers/standalone.rs). Public
    // by design: /auth/me answers 401 ITSELF when auth is enabled, and
    // /auth/login is the page that explains auth — it must render to an
    // unauthenticated browser or it explains nothing. /auth/logout is a
    // session no-op (the broker holds no sessions).
    if path == "/auth/me" || path == "/auth/login" || path == "/auth/logout" {
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
        // RUSTFIX item 7: the bare /status GET route (main.rs) is READ_ONLY, not
        // public and not the READ_WRITE default.
        if path == "/status" {
            return ReadOnly;
        }
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
        // PLAN_KV_TIMERS.md §8.1. WITHOUT these two lines the KV and timer GETs
        // fall through to the ReadWrite default at the bottom of this function,
        // and a read-only token gets 403 on `GET /api/v1/kv/:ns/*key` — i.e. the
        // broker demands WRITE access to read a key. They must live INSIDE this
        // `if m == "GET"` block: the hurried fix is a `starts_with` placed just
        // outside it, which hands write access to every read-only token.
        if path.starts_with("/api/v1/kv") {
            return ReadOnly;
        }
        if path.starts_with("/api/v1/timers") {
            return ReadOnly;
        }
        // EPHEMERAL_QUEUES.md §3.9. The two STATUS reads only — `/queues` and
        // `/queues/:queue/depth` — and they are inside this block for the exact
        // reason the KV lines above are: a `starts_with` placed just outside it
        // is the hurried version of this fix and hands WRITE access to every
        // read-only token. `GET /api/v1/ephemeral/pop` is deliberately NOT here:
        // a pop mutates (it moves a cursor and takes a lease), so it carries the
        // same level the durable pop does, one arm further down.
        if path.starts_with("/api/v1/ephemeral/queues") {
            return ReadOnly;
        }
    }

    // PLAN_QUEEN_KAFKA.md C2 — `POST /api/v1/fetch` is a pure READ (no lease,
    // no cursor movement, no claim), so it takes the same level as the other
    // message reads and NOT the ReadWrite fallthrough, which would demand write
    // access to read a log. It cannot ride the `m == "GET"` block above only
    // because its request is a batch body. Spelled with the method for the same
    // reason `/api/v1/ephemeral/push` is: the route is POST-only today and a
    // future verb on it must not inherit this level by accident.
    if m == "POST" && path == "/api/v1/fetch" {
        return ReadOnly;
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
    // Scheduling a timer IS a produce operation — the timer becomes a message —
    // so a produce-only token must be able to do it, and without this line it
    // would take 403 from the ReadWrite default (§8.1). The CANCEL is
    // deliberately NOT here: `DELETE /api/v1/timers/:queue/*timerKey` is
    // ReadWrite by the fallthrough below, which is right for a route that
    // removes state, and it is the route §9.6 guarantees is never blocked.
    if m == "POST" && path == "/api/v1/timers" {
        return WriteOnly;
    }
    // An ephemeral push is a produce operation and nothing else, so it takes the
    // same level as `/api/v1/push` — a produce-only token must be able to fill an
    // inbox it can never read. Spelled with the method as well as the path: the
    // route is POST-only today, and a future GET on it must not inherit
    // write-only by accident.
    if m == "POST" && path == "/api/v1/ephemeral/push" {
        return WriteOnly;
    }

    // -------- KV and timer writes: READ_WRITE, stated rather than inherited ---
    // These already land on the fallthrough, and the whole point of writing them
    // is that the fallthrough is not a decision. `webdoc/scripts/gen-routes.mjs`
    // mirrors this function behind a fingerprint and publishes the result, so an
    // access level nobody chose becomes an access level the site advertises.
    if path.starts_with("/api/v1/kv") || path.starts_with("/api/v1/timers") {
        return ReadWrite;
    }
    // EPHEMERAL_QUEUES.md §3.9 — everything else on the family: pop, ack,
    // configure, reset and the queue delete. They already land on the
    // fallthrough, and the whole point of writing them is that the fallthrough
    // is not a decision: `webdoc/scripts/gen-routes.mjs` mirrors this function
    // behind a fingerprint and PUBLISHES the result, so a level nobody chose
    // becomes a level the site advertises. The DELETE is ReadWrite and not Admin
    // — unlike a durable queue delete, which destroys committed history — because
    // here it destroys only what was already promised to be lost (§1.2).
    if path.starts_with("/api/v1/ephemeral") {
        return ReadWrite;
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

    let claims = match auth.validate(&token).await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SUPPORTED_JWT_ALGORITHMS;

    /// Every `alg` a token header can carry, i.e. every input `check_alg_allowed`
    /// can be asked about. Kept honest by `every_alg_is_exhaustive` below.
    const EVERY_ALG: &[Algorithm] = &[
        Algorithm::HS256,
        Algorithm::HS384,
        Algorithm::HS512,
        Algorithm::ES256,
        Algorithm::ES384,
        Algorithm::RS256,
        Algorithm::RS384,
        Algorithm::RS512,
        Algorithm::PS256,
        Algorithm::PS384,
        Algorithm::PS512,
        Algorithm::EdDSA,
    ];

    /// Compile-time guard, not a runtime one: `jsonwebtoken::Algorithm` is not
    /// `#[non_exhaustive]`, so adding a variant upstream breaks THIS match, which
    /// is the reminder to add it to `EVERY_ALG` (a stale `EVERY_ALG` would make the
    /// parity test below silently stop covering the new algorithm).
    #[allow(dead_code)]
    fn every_alg_is_exhaustive(a: Algorithm) {
        match a {
            Algorithm::HS256
            | Algorithm::HS384
            | Algorithm::HS512
            | Algorithm::ES256
            | Algorithm::ES384
            | Algorithm::RS256
            | Algorithm::RS384
            | Algorithm::RS512
            | Algorithm::PS256
            | Algorithm::PS384
            | Algorithm::PS512
            | Algorithm::EdDSA => {}
        }
    }

    /// An enabled auth config carrying EVERY kind of key material, so
    /// `validate()` answers only the question these tests ask — "is this
    /// algorithm NAME supported?" — and never the independent question of
    /// whether the key material suits it.
    fn cfg_with_alg(algorithm: &str) -> AuthConfig {
        AuthConfig {
            enabled: true,
            algorithm: algorithm.to_string(),
            secret: "secret".into(),
            public_key: "pem".into(),
            jwks_url: "https://example.invalid/jwks".into(),
            jwks_refresh_interval_seconds: 3600,
            jwks_request_timeout_ms: 5000,
            issuer: String::new(),
            audience: String::new(),
            clock_skew_seconds: 30,
            skip_paths: Vec::new(),
            roles_claim: "role".into(),
            roles_array_claim: "roles".into(),
            role_admin: "admin".into(),
            role_read_write: "read-write".into(),
            role_read_only: "read-only".into(),
            role_write_only: "write-only".into(),
        }
    }

    /// Would the REQUEST-time verifier ever let a token through with this value
    /// configured? `check_alg_allowed` answers per token `alg`, so the question
    /// "is this configured value usable at all" is asked by sweeping every alg:
    /// an unknown `JWT_ALGORITHM` falls to the `_ => false` arm and matches none.
    fn verifier_accepts(algorithm: &str) -> bool {
        let a = Authenticator::new(cfg_with_alg(algorithm));
        EVERY_ALG.iter().any(|&alg| a.check_alg_allowed(alg).is_ok())
    }

    fn boot_accepts(algorithm: &str) -> bool {
        cfg_with_alg(algorithm).validate().is_ok()
    }

    /// The regression this test exists for: `config.rs` refused HS384/HS512 at
    /// boot while `auth.rs` implemented them, so the broker died on a value its
    /// own verifier could serve — and the only way to accept an HS512 token was
    /// `auto`, which also accepts the other six.
    ///
    /// The property is stronger than a list: for EVERY candidate spelling, the
    /// two sides must give the same verdict. Adding an algorithm to either side
    /// alone fails here, in whichever direction the drift happens.
    #[test]
    fn boot_and_the_verifier_accept_the_same_algorithms() {
        let candidates = [
            // The supported set.
            "HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "EdDSA", "auto",
            // Deliberately unsupported: implemented by `jsonwebtoken` but not
            // wired here (ES*/PS*), plus the near-misses and the empty value, so
            // the two sides are pinned on their REFUSALS too, not just their
            // acceptances.
            "ES256", "ES384", "PS256", "PS384", "PS512", "none", "None", "HS128",
            "hs256", "RS128", "eddsa", "", "   ",
        ];
        for c in candidates {
            assert_eq!(
                boot_accepts(c),
                verifier_accepts(c),
                "JWT_ALGORITHM={c:?}: boot validation and the verifier disagree \
                 (config.rs::AuthConfig::validate vs auth.rs::check_alg_allowed)"
            );
        }
    }

    /// The set itself, pinned. The property above only proves the two sides
    /// AGREE; this proves they agree on the intended set rather than on some
    /// other one both drifted to together.
    #[test]
    fn the_supported_set_is_exactly_these_eight() {
        assert_eq!(
            SUPPORTED_JWT_ALGORITHMS,
            &["HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "EdDSA", "auto"],
        );
        for alg in SUPPORTED_JWT_ALGORITHMS {
            assert!(boot_accepts(alg), "{alg} is in the set but boot refuses it");
            assert!(verifier_accepts(alg), "{alg} is in the set but the verifier refuses it");
        }
    }

    /// The third copy of the list was the error MESSAGE, which named the set to
    /// the operator staring at a dead boot. It is built from the const now, so
    /// this pins the wording the const produces.
    #[test]
    fn the_boot_error_names_every_supported_algorithm() {
        let err = cfg_with_alg("ES256").validate().unwrap_err();
        for alg in SUPPORTED_JWT_ALGORITHMS {
            assert!(err.contains(*alg), "{alg} missing from the boot error: {err}");
        }
        assert!(
            err.contains("use HS256, HS384, HS512, RS256, RS384, RS512, EdDSA, or auto"),
            "unexpected phrasing: {err}"
        );
    }

    /// Widening the allow-list must not have widened the key-material check:
    /// the HS family still boots on a secret alone, and the asymmetric families
    /// still demand a public key or a JWKS URL.
    #[test]
    fn key_material_is_still_required_per_family() {
        let hs_only_secret = |alg: &str| {
            let mut c = cfg_with_alg(alg);
            c.public_key = String::new();
            c.jwks_url = String::new();
            c.validate()
        };
        for alg in ["HS256", "HS384", "HS512", "auto"] {
            assert!(hs_only_secret(alg).is_ok(), "{alg} should boot on JWT_SECRET alone");
        }
        for alg in ["HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "EdDSA", "auto"] {
            let mut c = cfg_with_alg(alg);
            c.secret = String::new();
            c.public_key = String::new();
            c.jwks_url = String::new();
            assert!(c.validate().is_err(), "{alg} with NO key material must not boot");
        }
        for alg in ["RS256", "RS384", "RS512", "EdDSA"] {
            let mut c = cfg_with_alg(alg);
            c.public_key = String::new();
            c.jwks_url = String::new();
            assert!(c.validate().is_err(), "{alg} must not boot on a secret alone");
        }
        // Disabled auth validates whatever it is given: the middleware is a
        // pass-through, so an unsupported value is inert rather than fatal.
        let mut off = cfg_with_alg("ES256");
        off.enabled = false;
        assert!(off.validate().is_ok());
    }

    /// PLAN_QUEEN_KAFKA.md C2. `POST /api/v1/fetch` reads a log and writes
    /// nothing, so it must sit with the other message reads and NOT on the
    /// ReadWrite fallthrough — a read-only token that takes 403 on a read is
    /// the failure this arm exists to prevent. Its neighbours are pinned in
    /// the same test because the value of stating the level is that the
    /// STATEMENT is what is checked, not the fallthrough.
    #[test]
    fn a_fetch_is_a_read_and_a_push_is_still_a_write() {
        let post = Method::POST;
        assert_eq!(
            route_access_level(&post, "/api/v1/fetch"),
            AccessLevel::ReadOnly
        );
        // The method is part of the decision: a future verb on the same path
        // must not inherit this level by accident.
        assert_eq!(
            route_access_level(&Method::GET, "/api/v1/fetch"),
            AccessLevel::ReadWrite
        );
        // Unchanged neighbours: produce stays write-only, consume stays
        // read-write (a pop takes a lease and moves a cursor).
        assert_eq!(
            route_access_level(&post, "/api/v1/push"),
            AccessLevel::WriteOnly
        );
        assert_eq!(
            route_access_level(&Method::GET, "/api/v1/pop/queue/orders"),
            AccessLevel::ReadWrite
        );
    }
}
