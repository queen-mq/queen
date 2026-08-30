//! Authentication + authorization. OWNER: Agent C (credential verification,
//! key lookup wiring, JWT mint/verify, JWKS). The `authorize` matrix below is
//! FINAL (spec §14) — extend only via report.
//!
//! Two credential kinds (PLAN §3 "Credentials"):
//!   * cluster **API keys** — `qk_<env>_<43 base64url>`, sha256-hashed at rest,
//!     resolved to (cluster, scopes) by the cache. Daemons put these in config.
//!   * user **JWTs** — minted by the proxy itself (EdDSA in cloud with the
//!     private key only on the auth-host; HS256 for dev), verified by every
//!     proxy and the console. Quotas never live in the token (DB only).
//!
//! Crypto backend is `jsonwebtoken` 9 (HS256 + EdDSA) plus `ring` for deriving
//! the Ed25519 public key from the private PKCS#8 PEM. No new non-ring deps.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::http::HeaderMap;
use axum::response::Response;
use base64::engine::general_purpose::{STANDARD as B64_STD, URL_SAFE_NO_PAD as B64_URL};
use base64::Engine;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use rand::rngs::OsRng;
use rand::RngCore;
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::errors;
use crate::routes::RouteClass;
use crate::state::{Principal, Role, Scopes, St};

// ---------------------------------------------------------------------------
// API keys
// ---------------------------------------------------------------------------

/// Opaque cluster API-key format: `qk_<env>_<43 base64url chars>` (32 random
/// bytes). Stored only as sha256 hex (`key_hash_hex`); the plaintext is shown
/// to the operator exactly once at creation (CLI/console). The `authenticate`
/// fast-path routes on this prefix.
pub const API_KEY_PREFIX: &str = "qk_";

/// Generate a fresh cluster API key for environment `env` (e.g. "live", "test").
/// Uses the OS CSPRNG. Caller stores `key_hash_hex(&key)`; the plaintext return
/// value is the only time the secret exists outside the client's config.
///
/// Staged deliverable: the in-crate caller (CLI / console `issue_api_key`) lands
/// in a later wave, so this is currently uncalled — hence `allow(dead_code)`.
#[allow(dead_code)]
pub fn generate_api_key(env: &str) -> String {
    // 32 bytes of CSPRNG material -> exactly 43 base64url chars.
    let mut raw = [0u8; 32];
    OsRng.fill_bytes(&mut raw);
    format!("{}{}_{}", API_KEY_PREFIX, env, B64_URL.encode(raw))
}

/// sha256 hex of an API key string — the stored representation in api_keys.key_hash.
pub fn key_hash_hex(key: &str) -> String {
    hex::encode(Sha256::digest(key.as_bytes()))
}

// ---------------------------------------------------------------------------
// credential extraction
// ---------------------------------------------------------------------------

/// What a request presents as its credential, before any verification.
///
/// `Session` covers BOTH forms a human session arrives in: the `Authorization:
/// Bearer` the console SPA and the SDKs send, and the httpOnly session cookie
/// a browser sends on its own. Accepting the cookie here is what lets the
/// webapp call `/api/v1/*` with plain same-origin XHR and no token plumbing;
/// it is safe against cross-site writes because the cookie is `SameSite=Lax`
/// (oauth.rs::session_cookie), which browsers do not attach to cross-site
/// fetch/XHR at all.
#[derive(Debug, PartialEq, Eq)]
pub enum Credential {
    ApiKey(String),
    Session(String),
    None,
}

/// Read the credential out of an API request. Authorization wins over the
/// cookie when both are present (an explicit token is a deliberate act; the
/// cookie merely rides along). Pure — `cookie_name` + `secure` instead of
/// `&St` — so the precedence rules are unit-testable without an AppState.
///
/// `secure` is `oauth::cookie_is_secure` for this request; it only picks which
/// of the two cell-cookie names is looked for (see `cell_cookie_name_of`).
///
/// A cookie is NOT accepted on a cross-document navigation here; see
/// `is_navigation`.
pub fn read_credential(cookie_name: &str, secure: bool, headers: &HeaderMap) -> Credential {
    read_credential_inner(cookie_name, secure, headers, false)
}

/// The same, for the DOCUMENT surface (the webapp shell in webapp.rs), where a
/// navigation is the normal and only way the page is ever loaded. Serving the
/// SPA is a read of static bytes: there is no state to forge, so the rule that
/// protects the API would only break the app.
pub fn read_credential_for_document(
    cookie_name: &str,
    secure: bool,
    headers: &HeaderMap,
) -> Credential {
    read_credential_inner(cookie_name, secure, headers, true)
}

fn read_credential_inner(
    cookie_name: &str,
    secure: bool,
    headers: &HeaderMap,
    cookie_on_navigation: bool,
) -> Credential {
    if let Some(raw) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        let token = raw.strip_prefix("Bearer ").unwrap_or(raw).trim();
        if !token.is_empty() {
            return if token.starts_with(API_KEY_PREFIX) {
                Credential::ApiKey(token.to_string())
            } else {
                Credential::Session(token.to_string())
            };
        }
    }
    if !cookie_on_navigation && is_navigation(headers) {
        return Credential::None;
    }
    match read_session_cookie(cookie_name, secure, headers) {
        // A cookie is only ever a HUMAN session. A `qk_` value sitting in one
        // is nonsense (nothing mints that) and is dropped rather than promoted
        // to a data-plane credential — a cookie is attached by the browser,
        // not chosen per request, so it must never widen what a caller can do.
        Some(t) if !t.is_empty() && !t.starts_with(API_KEY_PREFIX) => Credential::Session(t),
        _ => Credential::None,
    }
}

/// The name of the CELL-SCOPED session cookie: the one a verify-only host sets
/// for itself when it adopts a console handoff (`oauth::establish_handoff`).
///
/// # ⚠ Why it is not simply the fleet cookie's name
///
/// The handoff token names ONE cell in `aud`, and every sibling enforces its
/// own (`audience_ok`). Setting it under the fleet cookie's name *and* the
/// fleet's `Domain` — which is what this used to do — REPLACES the browser's
/// `.queenmq.cloud` cookie with a credential only this cell accepts, and every
/// sibling console then answers 401 until the user logs in at the portal
/// again. The adopted token has to go somewhere the fleet cookie is not.
///
/// Host-only alone does not get there. A jar keys cookies by name+domain+path,
/// so a host-only `<name>` and a `Domain=.queenmq.cloud` `<name>` coexist and
/// the browser sends BOTH — under one name, in an order this code does not
/// choose. RFC 6265 §5.4 sorts equal paths by creation time, oldest first, so
/// the one that arrives first is the FLEET cookie (set at login, before any
/// handoff), and a reader taking the first match would ignore the session the
/// user just established. A distinct name is what makes the precedence below a
/// decision rather than an accident of what the jar happens to emit.
///
/// # ⚠ Why the `__Host-` prefix, and why only when `Secure`
///
/// Same reasoning as queen-control's control cookie. Host-only is not the same
/// as un-shadowable: any sibling answering on the fleet domain can set a
/// `Domain=.queenmq.cloud` cookie of this name, and the browser would present
/// it here alongside ours. `__Host-` closes that at the browser — a
/// `Set-Cookie` carrying the prefix is rejected outright unless it has
/// `Secure`, no `Domain` and `Path=/`, which is exactly what this cookie is —
/// so no other host can put a cookie of this name in the jar.
///
/// The prefix is dropped when `secure` is false because that same rule
/// requires `Secure`: over plain http a `__Host-` cookie is not stored at all,
/// and local dev could not hold a session. That deployment has no fleet domain
/// and so no sibling to be shadowed from, which is the condition
/// `oauth::cookie_is_secure` already keys on.
pub fn cell_cookie_name_of(fleet_name: &str, secure: bool) -> String {
    if secure {
        format!("__Host-{fleet_name}_cell")
    } else {
        format!("{fleet_name}_cell")
    }
}

/// The session token this request presents, most specific cookie first.
///
/// The CELL cookie wins over the fleet one on the host that set it. It is the
/// deliberate act — the user clicked through a console handoff *to this cell* —
/// and it is at least as fresh as the fleet cookie by construction: the handoff
/// token is minted from the caller's own control-plane session and clamped to
/// what is left of it, and this cookie's `Max-Age` is the token's own remaining
/// life, so it can never sit in the jar outliving its token.
///
/// The one case it can be stale is a session revoked at the portal while this
/// cookie is still inside `exp`; that host then answers 401 until the user
/// clicks the console link again, which re-establishes it. That is the same
/// click that opened the console, and it is a better failure than the reverse
/// order, where a browser holding any fleet cookie would silently ignore every
/// handoff it was ever sent.
pub(crate) fn read_session_cookie(
    fleet_name: &str,
    secure: bool,
    headers: &HeaderMap,
) -> Option<String> {
    read_cookie(&cell_cookie_name_of(fleet_name, secure), headers)
        .filter(|v| !v.is_empty())
        .or_else(|| read_cookie(fleet_name, headers))
}

/// Is this a cross-document NAVIGATION (a link, a form, a redirect, the
/// address bar) rather than a script-initiated fetch?
///
/// It matters because accepting the session cookie on the API is what lets the
/// webapp use plain same-origin XHR, and the cookie is `SameSite=Lax`:
/// browsers withhold it from cross-site subresource and fetch/XHR requests but
/// DO attach it to a top-level navigation. Without this check a hostile page
/// could navigate a logged-in browser to `GET /api/v1/pop/queue/orders` and
/// lease a tenant's messages on the cookie alone — the one CSRF shape Lax does
/// not already cover.
///
/// Everything the SPA issues is `Sec-Fetch-Mode: cors|same-origin|no-cors`,
/// never `navigate`. Non-browser clients (the SDKs, curl) send no `Sec-Fetch-*`
/// at all and are unaffected, as is any request carrying an Authorization
/// header — that is chosen per request and cannot be attached by a third party.
fn is_navigation(headers: &HeaderMap) -> bool {
    headers
        .get("sec-fetch-mode")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("navigate"))
        .unwrap_or(false)
}

/// Minimal cookie parse: exact name match, first wins.
fn read_cookie(name: &str, headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
    for kv in raw.split(';') {
        if let Some((k, v)) = kv.split_once('=') {
            if k.trim() == name {
                return Some(v.trim().to_string());
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// authenticate / authorize
// ---------------------------------------------------------------------------

/// A verified human session: the token's claims plus whether the operator
/// capability is LIVE for this user on this cell (`cfg.operator_enabled` AND
/// `users.is_operator`). Handed to `role_on_cluster`, which is the one place
/// that turns a session into a per-cluster role.
pub struct Session {
    pub claims: VerifiedClaims,
    pub operator: bool,
}

/// Verify a session token end to end: signature + issuer + exp (pure crypto),
/// then the revocation deny-list, then the operator capability. Shared by the
/// Host path (`authenticate`) and the act-as-cluster path (acting.rs), so a
/// session is never accepted by one and rejected by the other.
pub async fn verify_session(st: &St, token: &str) -> Result<Session, Response> {
    let claims = match st.keys.verify_jwt_claims(token) {
        Ok(c) => c,
        Err(reason) => {
            // Distinct reason in the log, generic 401 to the client.
            tracing::debug!(target: "auth", reason = %reason.log_reason(), "jwt rejected");
            return Err(errors::err_401("invalid credential"));
        }
    };

    // Deny-list check (revoked_tokens): 60s in-process cache; skipped when there
    // is no pxdb (dev). Transient DB failure fails OPEN (see is_revoked).
    if st.keys.is_revoked(&st.db, &claims.jti).await {
        tracing::debug!(target: "auth", jti = %claims.jti, "jwt revoked");
        return Err(errors::err_401("invalid credential"));
    }

    // The per-cell gate is checked BEFORE the row lookup, not after: on a cell
    // where the capability is off, no request ever asks pxdb about it.
    let operator = st.cfg.operator_enabled && st.keys.is_operator(&st.db, claims.user_id).await;
    Ok(Session { claims, operator })
}

/// The session's effective role on one cluster.
///
/// A live operator is Admin on EVERY cluster, membership row or not — that is
/// what "super-admin" means here, and resolving it in one place keeps the Host
/// path and the act-as path from disagreeing. Everyone else needs a real
/// `cluster_roles` row; a missing one is fail-closed.
pub async fn role_on_cluster(
    st: &St,
    session: &Session,
    cluster_id: Uuid,
) -> Result<Role, Response> {
    if session.operator {
        return Ok(Role::Admin);
    }
    match st.keys.cluster_role(&st.db, session.claims.user_id, cluster_id).await {
        Some(role) => Ok(role),
        None => {
            // Distinguish "user exists but has no role here" (a real 403)
            // from "the session's user no longer exists" — a stale JWT
            // after user deletion (or a dev pxdb reset): that session is
            // dead, and 401 lets the SPA bounce to login instead of
            // leaving a 403 dead-end.
            if !st.keys.user_exists(&st.db, session.claims.user_id).await {
                return Err(errors::err_401("session no longer valid"));
            }
            Err(errors::err_403(errors::CODE_FORBIDDEN, "no role on this cluster"))
        }
    }
}

/// Authenticate a data-plane request against the cluster the caller resolved
/// from Host. Returns the Principal, or the ready error Response. Also
/// cross-checks key->cluster binding (key of another cluster on this Host ->
/// 403) and, for sessions, cluster claim / cluster_roles membership.
pub async fn authenticate(
    st: &St,
    headers: &HeaderMap,
    cluster_id: uuid::Uuid,
) -> Result<Principal, Response> {
    if st.cfg.dev_insecure {
        return Ok(Principal::ApiKey { key_id: uuid::Uuid::nil(), scopes: Scopes::all() });
    }
    let secure = crate::oauth::cookie_is_secure(st, headers);
    let token = match read_credential(&st.cfg.cookie_name, secure, headers) {
        // --- API key path (opaque, hashed lookup) ---
        Credential::ApiKey(key) => {
            let hash = key_hash_hex(&key);
            return match st.cache.by_key_hash(&hash).await {
                Some((ctx, key_id, scopes)) if ctx.cluster_id == cluster_id => {
                    Ok(Principal::ApiKey { key_id, scopes })
                }
                Some(_) => Err(errors::err_403(errors::CODE_FORBIDDEN, "key/cluster mismatch")),
                None => Err(errors::err_401("unknown or revoked api key")),
            };
        }
        Credential::Session(t) => t,
        Credential::None => return Err(errors::err_401("missing bearer credential")),
    };

    // --- session path (EdDSA / HS256) ---
    let session = verify_session(st, &token).await?;

    // Cluster binding. A cluster-scoped token is trusted verbatim; an unscoped
    // token has its per-cluster role resolved live from cluster_roles.
    let role = match bind_cluster(session.claims.cluster, session.claims.role, cluster_id) {
        ClusterBinding::Trust(role) => role,
        ClusterBinding::Mismatch => {
            return Err(errors::err_403(errors::CODE_FORBIDDEN, "token not valid for this cluster"));
        }
        ClusterBinding::Lookup => role_on_cluster(st, &session, cluster_id).await?,
    };

    Ok(Principal::User { user_id: session.claims.user_id, role, operator: session.operator })
}

/// Outcome of matching a token's optional `cluster` claim against the request's
/// resolved cluster. Pure + unit-tested; the DB lookup happens in `authenticate`.
#[derive(Debug, PartialEq, Eq)]
enum ClusterBinding {
    /// Claim is bound to this cluster — trust its role.
    Trust(Role),
    /// No cluster claim — resolve the role from cluster_roles.
    Lookup,
    /// Claim is bound to a *different* cluster — reject (403).
    Mismatch,
}

fn bind_cluster(claim_cluster: Option<Uuid>, claim_role: Role, request_cluster: Uuid) -> ClusterBinding {
    match claim_cluster {
        Some(c) if c == request_cluster => ClusterBinding::Trust(claim_role),
        Some(_) => ClusterBinding::Mismatch,
        None => ClusterBinding::Lookup,
    }
}

/// FINAL authorization matrix (spec §14).
pub fn authorize(p: &Principal, class: RouteClass) -> Result<(), Response> {
    let ok = match (&p, class) {
        (_, RouteClass::Blocked) => false,
        // Cell-wide surfaces: a LIVE operator only. `operator` already folds in
        // the per-cell flag (see Principal::User), so an api key — which can
        // never be one — and every tenant role fall to the same false.
        (Principal::User { operator: true, .. }, RouteClass::Operator) => true,
        (_, RouteClass::Operator) => false,
        (Principal::ApiKey { scopes, .. }, RouteClass::Produce) => scopes.produce,
        (Principal::ApiKey { scopes, .. }, RouteClass::Consume) => scopes.consume,
        (Principal::ApiKey { scopes, .. }, RouteClass::QueueAdmin) => scopes.admin,
        (Principal::ApiKey { scopes, .. }, RouteClass::Read) => {
            scopes.read || scopes.admin
        }
        // A gated READ is a read: a read-only key must be able to do
        // `GET /api/v1/kv/:ns/*key`, which PLAN_KV_TIMERS.md §8.1 puts at the
        // broker's `ReadOnly` access level. Only the kv/timers families ever
        // produce this arm — streams and traces classify as `Open` — so this
        // widens nothing that exists today.
        (Principal::ApiKey { scopes, .. }, RouteClass::Gated(_, crate::routes::GatedOp::Read)) => {
            scopes.read || scopes.admin || scopes.produce || scopes.consume
        }
        (Principal::ApiKey { scopes, .. }, RouteClass::Gated(_, _)) => {
            scopes.produce || scopes.consume
        }
        (Principal::User { role, .. }, RouteClass::Produce) => {
            matches!(role, Role::Admin | Role::Producer)
        }
        (Principal::User { role, .. }, RouteClass::Consume) => {
            matches!(role, Role::Admin | Role::Consumer)
        }
        (Principal::User { role, .. }, RouteClass::QueueAdmin) => matches!(role, Role::Admin),
        (Principal::User { .. }, RouteClass::Read) => true,
        (Principal::User { .. }, RouteClass::Gated(_, crate::routes::GatedOp::Read)) => true,
        (Principal::User { role, .. }, RouteClass::Gated(_, _)) => {
            !matches!(role, Role::Viewer)
        }
    };
    if ok {
        Ok(())
    } else {
        Err(errors::err_403(errors::CODE_FORBIDDEN, "operation not permitted for this credential"))
    }
}

fn role_from_str(s: &str) -> Option<Role> {
    match s {
        "admin" => Some(Role::Admin),
        "producer" => Some(Role::Producer),
        "consumer" => Some(Role::Consumer),
        "viewer" => Some(Role::Viewer),
        _ => None,
    }
}

#[cfg(test)]
fn role_as_str(r: Role) -> &'static str {
    match r {
        Role::Admin => "admin",
        Role::Producer => "producer",
        Role::Consumer => "consumer",
        Role::Viewer => "viewer",
    }
}

// ---------------------------------------------------------------------------
// JWT claims + verification result
// ---------------------------------------------------------------------------

/// Wire shape of a proxy-minted user token. `exp`/`iss` are validated by
/// `jsonwebtoken`; `sub`/`jti`/`role`/`cluster`/`aud` we validate by hand so the
/// broker-parity semantics (UUID subs, mandatory jti, closed role set) live
/// here rather than being implied by serde.
#[derive(Debug, Serialize, Deserialize)]
struct UserClaims {
    sub: String,
    iss: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    iat: Option<i64>,
    exp: i64,
    jti: String,
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cluster: Option<String>,
    /// OPTIONAL, and optional in both directions: this proxy does not mint it,
    /// and a token without it verifies exactly as it did before the claim
    /// existed. See `audience_ok` for the rule and why it is one-sided.
    #[serde(skip_serializing_if = "Option::is_none")]
    aud: Option<Audience>,
}

/// The two shapes RFC 7519 §4.1.3 allows `aud` to take: one string, or a list
/// of them. Both are accepted because the minter is a different codebase (the
/// control plane) and half the JWT libraries in existence emit the array form
/// for a single audience. Parsing only the string would turn that choice into a
/// token this proxy cannot even deserialize, which is a 401 on every session of
/// a correctly-configured cell.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum Audience {
    One(String),
    Many(Vec<String>),
}

impl Audience {
    /// The audiences actually named, blanks dropped. `""` and `[]` are legal
    /// JSON but name nobody, so they are treated as an absent claim rather than
    /// as an audience that can never match.
    fn values(&self) -> impl Iterator<Item = &str> {
        let all: &[String] = match self {
            Audience::One(s) => std::slice::from_ref(s),
            Audience::Many(v) => v,
        };
        all.iter().map(|s| s.trim()).filter(|s| !s.is_empty())
    }
}

/// The audience rule, pure so the whole matrix is pinned by tests.
///
/// A verify-only cell holds the fleet's public key, so every sibling's token
/// verifies here on signature and issuer alone: one token minted for cell A
/// replays against cell B, C and D. `aud` is what binds a token to the cell it
/// was issued for.
///
/// The rule is ONE-SIDED on purpose. A token that names an audience must name
/// ours; a token that names none is accepted. Every token minted before this
/// claim existed is in that second group, so this ships and takes effect on a
/// fleet the moment the control plane starts stamping `aud`, with no window in
/// which already-issued sessions are rejected and no ordering requirement
/// between deploying the cells and deploying the minter. The cost is stated
/// plainly: until the minter sets it, an unstamped token still replays, and the
/// day the fleet is fully stamped is the day this becomes a real boundary.
///
/// `want == None` (no audience configured) accepts everything, which is what
/// every self-hosted proxy that sets neither variable keeps getting.
fn audience_ok(want: Option<&str>, aud: Option<&Audience>) -> bool {
    let Some(want) = want else { return true };
    let mut named = aud.into_iter().flat_map(Audience::values).peekable();
    if named.peek().is_none() {
        return true;
    }
    // Case-insensitive: the value is a hostname on both sides (config.rs
    // lowercases what it resolves), and a minter that stamps `Cell-A` must not
    // be refused by a cell configured as `cell-a`.
    named.any(|a| a.eq_ignore_ascii_case(want))
}

/// Post-verification, shape-checked view handed back to `authenticate`.
#[derive(Debug)]
pub struct VerifiedClaims {
    pub user_id: Uuid,
    pub role: Role,
    pub jti: String,
    pub cluster: Option<Uuid>,
    /// Unix seconds. Carried out of verification because revoking a session
    /// needs the token's OWN expiry: `queen_proxy.revoke_session` stores it so
    /// the sweep can drop the deny-list row once the token is dead anyway.
    pub exp: i64,
}

/// Why a token was rejected — a distinct, non-client-facing reason for logs.
#[derive(Debug)]
pub(crate) enum JwtReject {
    NotConfigured,
    Expired,
    BadIssuer,
    /// The token named an audience, and it was somebody else's cell.
    BadAudience,
    BadSignature,
    Malformed,
    BadClaim(&'static str),
    MissingClaim(&'static str),
}

impl JwtReject {
    fn log_reason(&self) -> String {
        match self {
            JwtReject::NotConfigured => "jwt signer not configured".to_string(),
            JwtReject::Expired => "token expired".to_string(),
            JwtReject::BadIssuer => "issuer mismatch".to_string(),
            JwtReject::BadAudience => "audience mismatch (token minted for another cell)".to_string(),
            JwtReject::BadSignature => "signature invalid".to_string(),
            JwtReject::Malformed => "malformed token".to_string(),
            JwtReject::BadClaim(c) => format!("bad claim: {c}"),
            JwtReject::MissingClaim(c) => format!("missing claim: {c}"),
        }
    }
}

fn map_jwt_err(e: &jsonwebtoken::errors::Error) -> JwtReject {
    use jsonwebtoken::errors::ErrorKind;
    match e.kind() {
        ErrorKind::ExpiredSignature => JwtReject::Expired,
        ErrorKind::InvalidIssuer => JwtReject::BadIssuer,
        ErrorKind::InvalidSignature => JwtReject::BadSignature,
        ErrorKind::MissingRequiredClaim(c) if c == "exp" => JwtReject::MissingClaim("exp"),
        ErrorKind::MissingRequiredClaim(_) => JwtReject::MissingClaim("claim"),
        _ => JwtReject::Malformed,
    }
}

// ---------------------------------------------------------------------------
// Keys — mint / verify / JWKS
// ---------------------------------------------------------------------------

/// The active signing/verification material. EdDSA takes priority over HS when
/// both are configured (PLAN §3: asym in cloud, HS for dev). The `enc`/`kid`
/// fields are read only by the mint path (`sign`), which is staged for Track A —
/// hence `allow(dead_code)` until a mint caller is wired.
#[allow(dead_code)]
enum Signer {
    /// Ed25519. `enc` is None on a verify-only host (public PEM but no private
    /// key); `dec` is None only on a misconfig where the public key could not be
    /// derived. `x_b64` is the raw 32-byte public key, base64url — the JWKS `x`.
    Ed {
        enc: Option<EncodingKey>,
        dec: Option<DecodingKey>,
        kid: String,
        x_b64: String,
    },
    /// HS256 symmetric secret (dev). kid is the fixed string "hs".
    Hs {
        enc: EncodingKey,
        dec: DecodingKey,
        kid: String,
    },
    /// No JWT material configured — mint and verify both fail.
    None,
}

/// JWT mint/verify material + small deny-list / membership caches. OWNER:
/// Agent C. Held once in AppState behind an Arc; the caches use interior
/// mutability and are never locked across an await.
pub struct Keys {
    signer: Signer,
    issuer: String,
    /// The `aud` a token may name, or `None` to enforce none. Resolved once, by
    /// `config::resolve_jwt_audience`.
    audience: Option<String>,
    /// jti -> (revoked?, checked_at). 60s TTL; bounds revocation propagation.
    revoked_cache: Mutex<HashMap<String, (bool, Instant)>>,
    /// (user, cluster) -> (role, checked_at). 30s TTL; positive memberships only.
    role_cache: Mutex<HashMap<(Uuid, Uuid), (Role, Instant)>>,
    /// user -> (is_operator, checked_at). 30s TTL, BOTH answers cached: unlike
    /// role_cache this is read on every request of an operator-enabled cell,
    /// and the common answer is `false`. The TTL is therefore also the bound
    /// on how long a `set_operator(..., false)` takes to bite on a running
    /// proxy — set_operator writes no cluster_id, so there is no NOTIFY to
    /// shorten it.
    operator_cache: Mutex<HashMap<Uuid, (bool, Instant)>>,
    /// Deny-list policy when the lookup is UNAVAILABLE — see `is_revoked`.
    /// false (default) = fail open, true = fail closed.
    revocation_strict: bool,
    /// Rate limiter for the "deny-list unavailable" line, so a pxdb outage
    /// costs one log line per window instead of one per request.
    revoked_warn: Mutex<WarnSampler>,
}

const REVOKED_TTL: Duration = Duration::from_secs(60);
const ROLE_TTL: Duration = Duration::from_secs(30);
/// Soft cap after which a cache is pruned of stale entries on the next insert.
const AUTH_CACHE_CAP: usize = 100_000;
/// At most one deny-list-unavailable warn per this window.
const REVOKED_WARN_EVERY: Duration = Duration::from_secs(30);

/// One log line per window, carrying how many events it stands for. Pure state
/// machine (no clock of its own) so both the window and the suppression count
/// are unit-testable.
#[derive(Default)]
struct WarnSampler {
    last: Option<Instant>,
    suppressed: u64,
}

impl WarnSampler {
    /// `Some(suppressed_since_the_last_line)` when the caller should log now.
    fn tick(&mut self, now: Instant, every: Duration) -> Option<u64> {
        match self.last {
            Some(last) if now.duration_since(last) < every => {
                self.suppressed += 1;
                None
            }
            _ => {
                let suppressed = std::mem::take(&mut self.suppressed);
                self.last = Some(now);
                Some(suppressed)
            }
        }
    }
}

impl Keys {
    pub fn from_config(cfg: &crate::config::Config) -> Keys {
        // An OPTIONAL separately-supplied Ed25519 PUBLIC-key PEM. It lets a
        // verify-only host (data-plane proxy / console, per §10) hold only public
        // material, and it is a robustness override when public-from-private
        // derivation is undesirable. When absent, the public key is derived from
        // the private PEM with ring.
        //
        // Read through config.rs rather than off the environment here: main.rs's
        // boot gate classifies the deployment from the same knob, and two readers
        // with two "is it set?" rules would print one mode and run another.
        let pub_override = crate::config::jwt_ed25519_pub_pem();
        Keys::build(
            cfg.jwt_ed25519_pem.clone(),
            pub_override,
            cfg.jwt_hs_secret.clone(),
            cfg.jwt_issuer.clone(),
            cfg.jwt_audience.clone(),
        )
    }

    /// Construct from raw material. Split out from `from_config` so unit tests
    /// can inject keys without building a whole `Config`.
    fn build(
        ed_priv_pem: Option<String>,
        ed_pub_pem_override: Option<String>,
        hs_secret: Option<String>,
        issuer: String,
        audience: Option<String>,
    ) -> Keys {
        let signer = match ed_priv_pem.as_ref().filter(|s| !s.trim().is_empty()) {
            Some(priv_pem) => match EncodingKey::from_ed_pem(priv_pem.as_bytes()) {
                Ok(enc) => {
                    // Public key: explicit override PEM, else derive from private.
                    let raw_pub = ed_pub_pem_override
                        .as_deref()
                        .and_then(ed_pub_pem_to_raw)
                        .or_else(|| ed_raw_pub_from_private_pem(priv_pem));
                    match raw_pub {
                        Some(raw) => {
                            let kid = hex::encode(&Sha256::digest(&raw)[..16]);
                            Signer::Ed {
                                enc: Some(enc),
                                dec: Some(DecodingKey::from_ed_der(&raw)),
                                kid,
                                x_b64: B64_URL.encode(&raw),
                            }
                        }
                        None => {
                            tracing::error!(
                                target: "auth",
                                "Ed25519 private key loaded but public key could not be derived; \
                                 set QUEEN_PROXY_JWT_ED25519_PUB_PEM. Minting works, verification DISABLED."
                            );
                            Signer::Ed { enc: Some(enc), dec: None, kid: String::new(), x_b64: String::new() }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(target: "auth", err = %e, "invalid QUEEN_PROXY_JWT_ED25519_PEM; falling back");
                    hs_or_none(hs_secret)
                }
            },
            // No private key. HS256 still wins when a secret is set — that keeps
            // every dev/bench cell on exactly today's behaviour — but with no
            // secret either, a separately-supplied PUBLIC PEM makes this a
            // VERIFY-ONLY host: it accepts sessions the auth host minted and
            // mints nothing. That is the `enc: None` shape `Signer::Ed`
            // documents, and until now nothing constructed it: a public-key-only
            // host fell through to `Signer::None` and rejected every session it
            // was configured to accept.
            None => match hs_or_none(hs_secret) {
                Signer::None => verify_only_ed(ed_pub_pem_override.as_deref()),
                hs => hs,
            },
        };
        Keys {
            signer,
            issuer,
            audience,
            revoked_cache: Mutex::new(HashMap::new()),
            role_cache: Mutex::new(HashMap::new()),
            operator_cache: Mutex::new(HashMap::new()),
            revocation_strict: crate::config::revocation_strict(),
            revoked_warn: Mutex::new(WarnSampler::default()),
        }
    }

    /// Mint a short-lived user JWT. EdDSA when configured (kid = sha256[..16] of
    /// the public key), else HS256 (kid "hs"). `jti` is a fresh uuid v4, `iss`
    /// comes from config. Errors when no signer is configured or `role` is not a
    /// recognized role string.
    ///
    /// Staged: the mint path is consumed by Track A (`oauth.rs` sessions) and the
    /// console; no in-crate caller in this wave, hence `allow(dead_code)`.
    #[allow(dead_code)]
    pub fn mint_user_jwt(
        &self,
        user_id: Uuid,
        role: &str,
        cluster: Option<Uuid>,
        ttl_s: u64,
    ) -> Result<String, String> {
        if role_from_str(role).is_none() {
            return Err(format!("unknown role: {role}"));
        }
        let now = now_secs();
        let claims = UserClaims {
            sub: user_id.to_string(),
            iss: self.issuer.clone(),
            iat: Some(now),
            exp: now + ttl_s as i64,
            jti: Uuid::new_v4().to_string(),
            role: role.to_string(),
            cluster: cluster.map(|c| c.to_string()),
            // Not stamped here. This proxy mints for itself, and a token it
            // both mints and verifies gains nothing from an audience; the
            // claim exists for the CONTROL PLANE's fleet tokens, which are
            // minted elsewhere and must not be replayable across cells.
            aud: None,
        };
        self.sign(&claims)
    }

    /// Sign a claim set with the configured signer. Shared by `mint_user_jwt`
    /// (staged) and the unit tests that forge edge-case claims.
    #[allow(dead_code)]
    fn sign(&self, claims: &UserClaims) -> Result<String, String> {
        match &self.signer {
            Signer::Ed { enc: Some(enc), kid, .. } => {
                let mut h = Header::new(Algorithm::EdDSA);
                h.kid = Some(kid.clone());
                encode(&h, claims, enc).map_err(|e| e.to_string())
            }
            Signer::Ed { enc: None, .. } => Err("ed25519 verify-only host cannot mint".to_string()),
            Signer::Hs { enc, kid, .. } => {
                let mut h = Header::new(Algorithm::HS256);
                h.kid = Some(kid.clone());
                encode(&h, claims, enc).map_err(|e| e.to_string())
            }
            Signer::None => Err("no jwt signer configured".to_string()),
        }
    }

    /// Verify signature + temporal/issuer claims and shape-check the payload.
    /// Pure crypto: no DB. The algorithm is pinned to the configured signer so a
    /// token cannot downgrade (e.g. present HS to an EdDSA verifier).
    pub(crate) fn verify_jwt_claims(&self, token: &str) -> Result<VerifiedClaims, JwtReject> {
        let (alg, dec) = match &self.signer {
            Signer::Ed { dec: Some(d), .. } => (Algorithm::EdDSA, d),
            Signer::Ed { dec: None, .. } => return Err(JwtReject::NotConfigured),
            Signer::Hs { dec, .. } => (Algorithm::HS256, dec),
            Signer::None => return Err(JwtReject::NotConfigured),
        };

        let mut v = Validation::new(alg);
        v.algorithms = vec![alg]; // pin — no alg confusion / downgrade
        v.validate_exp = true; // exp is mandatory (default required_spec_claims = {exp})
        v.validate_nbf = false;
        // `aud` is checked by hand below, not here: jsonwebtoken's own check
        // REQUIRES the claim once an expected audience is set, and the whole
        // point of this one is that a token without `aud` stays valid.
        v.validate_aud = false;
        v.set_issuer(&[self.issuer.as_str()]);

        let data = decode::<UserClaims>(token, dec, &v).map_err(|e| map_jwt_err(&e))?;
        let c = data.claims;

        if !audience_ok(self.audience.as_deref(), c.aud.as_ref()) {
            return Err(JwtReject::BadAudience);
        }
        if c.jti.trim().is_empty() {
            return Err(JwtReject::MissingClaim("jti"));
        }
        let user_id = Uuid::parse_str(c.sub.trim()).map_err(|_| JwtReject::BadClaim("sub"))?;
        let role = role_from_str(&c.role).ok_or(JwtReject::BadClaim("role"))?;
        let cluster = match c.cluster.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            Some(s) => Some(Uuid::parse_str(s).map_err(|_| JwtReject::BadClaim("cluster"))?),
            None => None,
        };

        Ok(VerifiedClaims { user_id, role, jti: c.jti, cluster, exp: c.exp })
    }

    /// Is this jti on the deny-list? Cached 60s. `db == None` (dev) => not
    /// revoked.
    ///
    /// When the lookup is UNAVAILABLE (pool or query error — not a clean "no
    /// such row") the answer is a deliberate policy, not an accident:
    ///   * default (`QUEEN_PROXY_REVOCATION_STRICT` unset/false) — fail OPEN.
    ///     A pxdb blip must not 401 every session; the token still had to pass
    ///     signature + exp, and the data plane already degrades without pxdb.
    ///     The cost is bounded and explicit: for as long as pxdb is unreachable
    ///     the deny-list is not enforced on this proxy.
    ///   * strict (`QUEEN_PROXY_REVOCATION_STRICT=true`) — fail CLOSED, for
    ///     deployments that prefer rejecting sessions to honouring one that may
    ///     have been revoked.
    /// Either way the outcome is logged (sampled — an outage is one line per
    /// `REVOKED_WARN_EVERY`, not one per request) and never cached: only an
    /// answer the DB actually gave is worth remembering for 60s.
    pub async fn is_revoked(&self, db: &Option<deadpool_postgres::Pool>, jti: &str) -> bool {
        let Some(pool) = db else { return false };

        if let Some((rev, at)) = self.revoked_cache.lock().unwrap().get(jti) {
            if at.elapsed() < REVOKED_TTL {
                return *rev;
            }
        }

        let looked_up = match pool.get().await {
            Ok(client) => match client
                .query_opt(
                    // jti column is TEXT by design (001_init.sql): the deny-list
                    // must accept non-UUID jtis from foreign token mints too.
                    "SELECT 1 FROM queen_proxy.revoked_tokens WHERE jti = $1",
                    &[&jti],
                )
                .await
            {
                Ok(row) => Some(row.is_some()),
                Err(e) => {
                    self.warn_deny_list_unavailable(&e.to_string(), "revoked_tokens query failed");
                    None
                }
            },
            Err(e) => {
                self.warn_deny_list_unavailable(&e.to_string(), "pxdb unavailable");
                None
            }
        };

        let Some(revoked) = looked_up else {
            return self.revocation_strict;
        };

        let mut cache = self.revoked_cache.lock().unwrap();
        if cache.len() >= AUTH_CACHE_CAP {
            cache.retain(|_, (_, at)| at.elapsed() < REVOKED_TTL);
        }
        cache.insert(jti.to_string(), (revoked, Instant::now()));
        revoked
    }

    /// The sampled counterpart of the policy above: the line names which way
    /// the proxy is failing and the knob that flips it, so the behaviour is
    /// visible in the log of a deployment that never read this file.
    fn warn_deny_list_unavailable(&self, err: &str, what: &str) {
        let sample = self.revoked_warn.lock().unwrap().tick(Instant::now(), REVOKED_WARN_EVERY);
        let Some(suppressed) = sample else { return };
        if self.revocation_strict {
            tracing::warn!(
                target: "auth", err = %err, cause = what, suppressed,
                "deny-list unavailable; failing CLOSED (QUEEN_PROXY_REVOCATION_STRICT=true): \
                 user sessions rejected until pxdb recovers"
            );
        } else {
            tracing::warn!(
                target: "auth", err = %err, cause = what, suppressed,
                "deny-list unavailable; failing OPEN: revocations are NOT enforced until pxdb \
                 recovers (set QUEEN_PROXY_REVOCATION_STRICT=true to reject instead)"
            );
        }
    }

    /// Pin a jti as revoked in this process's deny-list cache. Called right
    /// after a successful `queen_proxy.revoke_session` so the proxy that served
    /// the logout stops honouring the token immediately, instead of waiting out
    /// a cached negative answer (other cells converge within their own
    /// `REVOKED_TTL`). Best-effort local shortcut — the DB row is the truth.
    pub fn note_revoked(&self, jti: &str) {
        let mut cache = self.revoked_cache.lock().unwrap();
        if cache.len() >= AUTH_CACHE_CAP {
            cache.retain(|_, (_, at)| at.elapsed() < REVOKED_TTL);
        }
        cache.insert(jti.to_string(), (true, Instant::now()));
    }

    /// Resolve a user's role on a cluster from `cluster_roles`. Cached 30s
    /// (positive memberships only). `db == None` or no row => None (fail-closed:
    /// membership cannot be proven => caller returns 403). UUIDs are bound as
    /// text and cast in SQL to avoid a tokio-postgres uuid feature dependency.
    /// Does the session's user still exist? Only consulted on the rare
    /// role-miss path, so deliberately uncached: a deleted user must lose
    /// access immediately, and dev pxdb resets must not leave stale-but-
    /// validly-signed sessions stuck on a 403 dead-end. On transient DB
    /// errors we assume the user exists (prefer the softer 403 over killing
    /// a possibly-good session).
    pub async fn user_exists(&self, db: &Option<deadpool_postgres::Pool>, user_id: Uuid) -> bool {
        let Some(pool) = db.as_ref() else { return false };
        let Ok(client) = pool.get().await else { return true };
        match client
            .query_opt(
                "SELECT 1 FROM queen_proxy.users WHERE id = $1::text::uuid",
                &[&user_id.to_string()],
            )
            .await
        {
            Ok(row) => row.is_some(),
            Err(e) => {
                tracing::warn!(target: "auth", err = %e, "user_exists query failed; assuming exists");
                true
            }
        }
    }

    pub async fn cluster_role(
        &self,
        db: &Option<deadpool_postgres::Pool>,
        user_id: Uuid,
        cluster_id: Uuid,
    ) -> Option<Role> {
        let key = (user_id, cluster_id);
        if let Some((r, at)) = self.role_cache.lock().unwrap().get(&key) {
            if at.elapsed() < ROLE_TTL {
                return Some(*r);
            }
        }

        let pool = db.as_ref()?;
        let role = match pool.get().await {
            Ok(client) => {
                let uid = user_id.to_string();
                let cid = cluster_id.to_string();
                match client
                    .query_opt(
                        "SELECT role FROM queen_proxy.cluster_roles \
                         WHERE user_id = $1::text::uuid AND cluster_id = $2::text::uuid",
                        &[&uid, &cid],
                    )
                    .await
                {
                    Ok(Some(row)) => role_from_str(&row.get::<_, String>(0)),
                    Ok(None) => None,
                    Err(e) => {
                        tracing::warn!(target: "auth", err = %e, "cluster_roles query failed");
                        return None;
                    }
                }
            }
            Err(e) => {
                tracing::warn!(target: "auth", err = %e, "pxdb unavailable for cluster_roles");
                return None;
            }
        };

        if let Some(r) = role {
            let mut cache = self.role_cache.lock().unwrap();
            if cache.len() >= AUTH_CACHE_CAP {
                cache.retain(|_, (_, at)| at.elapsed() < ROLE_TTL);
            }
            cache.insert(key, (r, Instant::now()));
        }
        role
    }

    /// Does this user hold the operator bit (`queen_proxy.users.is_operator`)?
    /// Cached `ROLE_TTL`, both answers.
    ///
    /// Fail-CLOSED on every unknown: no pxdb, an unavailable pool, a query
    /// error, or a column a pre-006 pxdb does not have yet all answer `false`.
    /// This is the opposite stance from `is_revoked`'s default fail-open, and
    /// deliberately so — a degraded control plane must never GRANT cell-wide
    /// access it cannot confirm, whereas failing open there only means a
    /// revocation is late.
    pub async fn is_operator(&self, db: &Option<deadpool_postgres::Pool>, user_id: Uuid) -> bool {
        if let Some((flag, at)) = self.operator_cache.lock().unwrap().get(&user_id) {
            if at.elapsed() < ROLE_TTL {
                return *flag;
            }
        }
        let Some(pool) = db.as_ref() else { return false };
        let Ok(client) = pool.get().await else {
            tracing::warn!(target: "auth", "pxdb unavailable for is_operator; denying");
            return false;
        };
        let flag = match client
            .query_opt(
                "SELECT is_operator FROM queen_proxy.users WHERE id = $1::text::uuid",
                &[&user_id.to_string()],
            )
            .await
        {
            Ok(Some(row)) => row.get::<_, bool>(0),
            Ok(None) => false,
            Err(e) => {
                tracing::warn!(target: "auth", err = %e, "is_operator query failed; denying");
                // Never cached: an answer pxdb did not give is not worth
                // remembering, and recovery must be immediate.
                return false;
            }
        };

        let mut cache = self.operator_cache.lock().unwrap();
        if cache.len() >= AUTH_CACHE_CAP {
            cache.retain(|_, (_, at)| at.elapsed() < ROLE_TTL);
        }
        cache.insert(user_id, (flag, Instant::now()));
        flag
    }

    /// The JWKS document served at `/.well-known/jwks.json`. EdDSA => a real
    /// one-key OKP/Ed25519 set; HS or unconfigured => `{"keys":[]}` (nothing
    /// public to publish).
    /// Can this proxy ISSUE a session token? False on a verify-only host and on
    /// one with no material at all. Read once, at boot (main.rs), so a cell that
    /// serves a login it cannot satisfy refuses to start instead of 500ing every
    /// login for as long as nobody tries one.
    pub fn can_mint(&self) -> bool {
        matches!(&self.signer, Signer::Ed { enc: Some(_), .. } | Signer::Hs { .. })
    }

    /// Can this proxy ACCEPT a session token? False when no verification key
    /// could be built — every session then answers 401 with the data plane green.
    pub fn can_verify(&self) -> bool {
        matches!(&self.signer, Signer::Ed { dec: Some(_), .. } | Signer::Hs { .. })
    }

    pub fn jwks_json(&self) -> String {
        match &self.signer {
            Signer::Ed { dec: Some(_), kid, x_b64, .. } => serde_json::json!({
                "keys": [{
                    "kty": "OKP",
                    "crv": "Ed25519",
                    "x": x_b64,
                    "kid": kid,
                    "alg": "EdDSA",
                    "use": "sig",
                }]
            })
            .to_string(),
            _ => "{\"keys\":[]}".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Deny-list sweep
// ---------------------------------------------------------------------------

/// Detached loop dropping `revoked_tokens` rows whose own `exp` has passed
/// (`queen_proxy.sweep_revoked_tokens`, migration 004). Without it the
/// deny-list only ever grows, since a logout inserts a row per session.
///
/// Maintenance, so it is deliberately unexcited about failure: a pxdb outage
/// warns and the next tick tries again — nothing here can affect a verify's
/// outcome (an expired row is already inert). Skipped entirely without a pxdb
/// (dev-static) and disabled by `QUEEN_PROXY_REVOCATION_SWEEP_MS=0`.
pub fn spawn_revocation_sweep(st: St) {
    let Some(pool) = st.db.clone() else {
        tracing::info!(target: "auth", "revocation sweep: no pxdb configured, skipping (dev-static mode)");
        return;
    };
    let interval = crate::config::revocation_sweep_interval();
    if interval.is_zero() {
        tracing::info!(target: "auth", "revocation sweep disabled (QUEEN_PROXY_REVOCATION_SWEEP_MS=0)");
        return;
    }
    tokio::spawn(async move {
        // First tick fires immediately: a restart is a good moment to clear
        // whatever expired while the process was down.
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            sweep_revoked_once(&pool).await;
        }
    });
}

async fn sweep_revoked_once(pool: &deadpool_postgres::Pool) {
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "auth", err = %e, "revocation sweep: pxdb unavailable");
            return;
        }
    };
    match client.query_one("SELECT queen_proxy.sweep_revoked_tokens()", &[]).await {
        Ok(row) => {
            let deleted: i32 = row.get(0);
            tracing::debug!(target: "auth", deleted, "revoked_tokens swept");
        }
        Err(e) => tracing::warn!(target: "auth", err = %e, "revocation sweep failed"),
    }
}

fn hs_or_none(hs_secret: Option<String>) -> Signer {
    match hs_secret.filter(|s| !s.is_empty()) {
        Some(secret) => Signer::Hs {
            enc: EncodingKey::from_secret(secret.as_bytes()),
            dec: DecodingKey::from_secret(secret.as_bytes()),
            kid: "hs".to_string(),
        },
        None => Signer::None,
    }
}

/// Ed25519 signer built from a PUBLIC key alone: verifies, never mints.
///
/// A PEM that will not parse yields `Signer::None` rather than a guess, and
/// main.rs's boot gate turns that into a refusal — a verify-only host that
/// cannot verify 401s every session while looking perfectly healthy, which is
/// the same failure class as a mint host that cannot mint.
fn verify_only_ed(pub_pem: Option<&str>) -> Signer {
    match pub_pem.and_then(ed_pub_pem_to_raw) {
        Some(raw) => {
            let kid = hex::encode(&Sha256::digest(&raw)[..16]);
            Signer::Ed {
                enc: None,
                dec: Some(DecodingKey::from_ed_der(&raw)),
                kid,
                x_b64: B64_URL.encode(&raw),
            }
        }
        None => Signer::None,
    }
}

// ---------------------------------------------------------------------------
// PEM / key helpers
// ---------------------------------------------------------------------------

/// Strip PEM armor and base64-decode to DER. Tolerates CRLF and stray blank
/// lines; returns None on malformed base64.
fn pem_to_der(pem: &str) -> Option<Vec<u8>> {
    let b64: String = pem
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("-----"))
        .collect();
    B64_STD.decode(b64.as_bytes()).ok()
}

/// Raw 32-byte Ed25519 public key from a private PKCS#8 PEM, via ring. Uses the
/// lenient `from_pkcs8_maybe_unchecked` so both v1 (OpenSSL, seed-only) and v2
/// (ring, seed+public) documents are accepted — matching what jsonwebtoken's
/// own EdDSA signing accepts.
fn ed_raw_pub_from_private_pem(priv_pem: &str) -> Option<Vec<u8>> {
    let der = pem_to_der(priv_pem)?;
    let kp = Ed25519KeyPair::from_pkcs8_maybe_unchecked(&der).ok()?;
    Some(kp.public_key().as_ref().to_vec())
}

/// Raw 32-byte public key from an Ed25519 SubjectPublicKeyInfo (`PUBLIC KEY`)
/// PEM. The SPKI DER is a fixed 12-byte prefix + the 32-byte key (44 total).
fn ed_pub_pem_to_raw(pub_pem: &str) -> Option<Vec<u8>> {
    let der = pem_to_der(pub_pem)?;
    match der.len() {
        44 => Some(der[12..].to_vec()),
        32 => Some(der), // already raw
        _ => None,
    }
}

#[allow(dead_code)] // read by the staged mint path + unit tests
fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Test fixtures shared with other modules' suites
// ---------------------------------------------------------------------------

/// Key material other modules' unit tests need but cannot build, because the
/// PEM plumbing, `UserClaims` and `sign` are all private to this module.
///
/// It exists for `oauth.rs`'s dashboard-handoff suite, which has to exercise
/// the real EdDSA verify against a real verify-only host: stubbing that out
/// would test the branch and not the crypto, and the crypto is the thing.
#[cfg(test)]
pub(crate) mod testkit {
    use super::*;
    use ring::rand::SystemRandom;

    pub(crate) fn der_to_pem(der: &[u8], label: &str) -> String {
        let b64 = B64_STD.encode(der);
        let mut body = String::new();
        for chunk in b64.as_bytes().chunks(64) {
            body.push_str(std::str::from_utf8(chunk).unwrap());
            body.push('\n');
        }
        format!("-----BEGIN {label}-----\n{body}-----END {label}-----\n")
    }

    /// The two hosts a fleet handoff runs between, sharing one keypair: a
    /// mint-capable auth host, and the VERIFY-ONLY cell that holds only its
    /// public key. `audience` is the cell's expected `aud`.
    ///
    /// Two calls give two unrelated keypairs, which is how a suite gets the
    /// "signed by somebody else's auth host" case.
    pub(crate) fn ed_handoff_pair(issuer: &str, audience: Option<&str>) -> (Keys, Keys) {
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let kp = Ed25519KeyPair::from_pkcs8_maybe_unchecked(pkcs8.as_ref()).unwrap();
        // SubjectPublicKeyInfo for Ed25519 is a fixed 12-byte prefix + the raw
        // 32-byte key, the 44-byte shape `ed_pub_pem_to_raw` reads back.
        let mut spki: Vec<u8> =
            vec![0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00];
        spki.extend_from_slice(kp.public_key().as_ref());
        let mint = Keys::build(
            Some(der_to_pem(pkcs8.as_ref(), "PRIVATE KEY")),
            None,
            None,
            issuer.to_string(),
            audience.map(str::to_string),
        );
        let verify_only = Keys::build(
            None,
            Some(der_to_pem(&spki, "PUBLIC KEY")),
            None,
            issuer.to_string(),
            audience.map(str::to_string),
        );
        (mint, verify_only)
    }

    /// Sign a session token with an explicit `exp`, for the expiry edges
    /// `mint_user_jwt`'s `ttl_s: u64` cannot express. `aud` is stamped as the
    /// control plane would stamp it, which no proxy mint ever does.
    pub(crate) fn mint_at(
        keys: &Keys,
        user_id: Uuid,
        exp: i64,
        aud: Option<&str>,
    ) -> Result<String, String> {
        keys.sign(&UserClaims {
            sub: user_id.to_string(),
            iss: keys.issuer.clone(),
            iat: Some(exp - 900),
            exp,
            jti: Uuid::new_v4().to_string(),
            role: "viewer".to_string(),
            cluster: None,
            aud: aud.map(|a| Audience::One(a.to_string())),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;
    use ring::signature::{Ed25519KeyPair, KeyPair};

    use super::testkit::der_to_pem;

    fn ed_keys(issuer: &str) -> Keys {
        ed_keys_aud(issuer, None)
    }

    /// `ed_keys` with an expected audience, for the `aud` matrix below.
    fn ed_keys_aud(issuer: &str, audience: Option<&str>) -> Keys {
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let pem = der_to_pem(pkcs8.as_ref(), "PRIVATE KEY");
        Keys::build(Some(pem), None, None, issuer.to_string(), audience.map(str::to_string))
    }

    fn hs_keys(issuer: &str) -> Keys {
        Keys::build(None, None, Some("dev-secret-value".to_string()), issuer.to_string(), None)
    }

    #[test]
    fn api_key_generate_hash_format() {
        let k = generate_api_key("live");
        assert!(k.starts_with("qk_live_"), "prefix: {k}");
        assert_eq!(k.len(), "qk_live_".len() + 43, "43 base64url body chars");
        let body = &k["qk_live_".len()..];
        assert!(
            body.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "base64url alphabet only: {body}"
        );
        let h = key_hash_hex(&k);
        assert_eq!(h.len(), 64);
        assert!(h.bytes().all(|b| b.is_ascii_hexdigit()));
        // fresh randomness each call
        assert_ne!(generate_api_key("live"), generate_api_key("live"));
        // env is interpolated
        assert!(generate_api_key("test").starts_with("qk_test_"));
    }

    #[test]
    fn eddsa_mint_verify_roundtrip() {
        let keys = ed_keys("queen-proxy");
        let uid = Uuid::new_v4();
        let token = keys.mint_user_jwt(uid, "producer", None, 3600).unwrap();

        // header carries EdDSA + the JWKS kid
        let hdr = jsonwebtoken::decode_header(&token).unwrap();
        assert_eq!(hdr.alg, Algorithm::EdDSA);

        let vc = keys.verify_jwt_claims(&token).unwrap();
        assert_eq!(vc.user_id, uid);
        assert_eq!(vc.role, Role::Producer);
        assert!(vc.cluster.is_none());
        assert!(!vc.jti.is_empty());
    }

    #[test]
    fn hs256_mint_verify_roundtrip_with_cluster() {
        let keys = hs_keys("queen-proxy");
        let uid = Uuid::new_v4();
        let cl = Uuid::new_v4();
        let token = keys.mint_user_jwt(uid, "admin", Some(cl), 3600).unwrap();

        let hdr = jsonwebtoken::decode_header(&token).unwrap();
        assert_eq!(hdr.alg, Algorithm::HS256);
        assert_eq!(hdr.kid.as_deref(), Some("hs"));

        let vc = keys.verify_jwt_claims(&token).unwrap();
        assert_eq!(vc.user_id, uid);
        assert_eq!(vc.role, Role::Admin);
        assert_eq!(vc.cluster, Some(cl));
    }

    #[test]
    fn expired_token_rejected() {
        let keys = ed_keys("queen-proxy");
        let now = now_secs();
        // > default 60s leeway in the past
        let claims = UserClaims {
            sub: Uuid::new_v4().to_string(),
            iss: "queen-proxy".to_string(),
            iat: Some(now - 7200),
            exp: now - 3600,
            jti: Uuid::new_v4().to_string(),
            role: "viewer".to_string(),
            cluster: None,
            aud: None,
        };
        let token = keys.sign(&claims).unwrap();
        let err = keys.verify_jwt_claims(&token).unwrap_err();
        assert!(matches!(err, JwtReject::Expired), "got {err:?}");
    }

    #[test]
    fn wrong_issuer_rejected() {
        let keys = ed_keys("queen-proxy");
        let now = now_secs();
        let claims = UserClaims {
            sub: Uuid::new_v4().to_string(),
            iss: "evil-issuer".to_string(),
            iat: Some(now),
            exp: now + 3600,
            jti: Uuid::new_v4().to_string(),
            role: "viewer".to_string(),
            cluster: None,
            aud: None,
        };
        let token = keys.sign(&claims).unwrap();
        let err = keys.verify_jwt_claims(&token).unwrap_err();
        assert!(matches!(err, JwtReject::BadIssuer), "got {err:?}");
    }

    // ---- optional audience: binding a fleet token to one cell --------------

    /// A token carrying exactly this `aud`. `mint_user_jwt` never stamps one
    /// (the minter this claim exists for is the control plane, not a proxy), so
    /// the fleet-token shape has to be built by hand here.
    fn token_with_aud(keys: &Keys, aud: Option<Audience>) -> String {
        let now = now_secs();
        keys.sign(&UserClaims {
            sub: Uuid::new_v4().to_string(),
            iss: "queen-proxy".to_string(),
            iat: Some(now),
            exp: now + 3600,
            jti: Uuid::new_v4().to_string(),
            role: "viewer".to_string(),
            cluster: None,
            aud,
        })
        .unwrap()
    }

    /// Backward compatibility, and the reason the rule is one-sided: every
    /// token any minter has ever produced carries no `aud`, and configuring an
    /// audience must not reject a single one of them. Without this the change
    /// is a flag day.
    #[test]
    fn a_token_with_no_audience_is_accepted_by_a_cell_that_has_one() {
        let keys = ed_keys_aud("queen-proxy", Some("cell-a.queenmq.cloud"));
        let token = keys.mint_user_jwt(Uuid::new_v4(), "admin", None, 3600).unwrap();
        assert!(keys.verify_jwt_claims(&token).is_ok(), "an unstamped token must keep working");
        // ... including the explicitly-empty shapes, which name nobody.
        for empty in [Audience::One(String::new()), Audience::One("  ".into()), Audience::Many(vec![])] {
            let t = token_with_aud(&keys, Some(empty));
            assert!(keys.verify_jwt_claims(&t).is_ok(), "an empty aud names no cell");
        }
    }

    #[test]
    fn a_token_naming_this_cell_is_accepted() {
        let keys = ed_keys_aud("queen-proxy", Some("cell-a.queenmq.cloud"));
        for aud in [
            Audience::One("cell-a.queenmq.cloud".into()),
            // The array form, which plenty of JWT libraries emit for a single
            // audience, and one naming several cells including this one.
            Audience::Many(vec!["cell-a.queenmq.cloud".into()]),
            Audience::Many(vec!["cell-b.queenmq.cloud".into(), "cell-a.queenmq.cloud".into()]),
            // A hostname is case-insensitive; a minter that stamps it mixed-case
            // must not lock the cell out of every session it was configured for.
            Audience::One("Cell-A.QueenMQ.Cloud".into()),
        ] {
            let token = token_with_aud(&keys, Some(aud));
            assert!(keys.verify_jwt_claims(&token).is_ok());
        }
    }

    #[test]
    fn a_token_naming_another_cell_is_refused() {
        let keys = ed_keys_aud("queen-proxy", Some("cell-a.queenmq.cloud"));
        for aud in [
            Audience::One("cell-b.queenmq.cloud".into()),
            Audience::Many(vec!["cell-b.queenmq.cloud".into(), "cell-c.queenmq.cloud".into()]),
            // A suffix is not a match: the audience is a whole name, exactly as
            // QUEEN_PROXY_SHARED_HOSTS is.
            Audience::One("queenmq.cloud".into()),
            Audience::One("evil-cell-a.queenmq.cloud".into()),
        ] {
            let token = token_with_aud(&keys, Some(aud));
            let err = keys.verify_jwt_claims(&token).unwrap_err();
            assert!(matches!(err, JwtReject::BadAudience), "got {err:?}");
        }
    }

    /// The property the claim exists for, end to end. Two cells verify with the
    /// SAME fleet public key, so today each accepts anything minted for the
    /// other. Once the token names its cell, it stops crossing.
    #[test]
    fn a_stamped_token_does_not_replay_against_a_sibling_cell() {
        let (priv_pem, pub_pem, _) = ed_pair();
        let control_plane = Keys::build(Some(priv_pem), None, None, "queen-proxy".to_string(), None);
        let cell = |aud: &str| {
            Keys::build(None, Some(pub_pem.clone()), None, "queen-proxy".to_string(), Some(aud.into()))
        };
        let cell_a = cell("cell-a.queenmq.cloud");
        let cell_b = cell("cell-b.queenmq.cloud");

        let for_a = token_with_aud(&control_plane, Some(Audience::One("cell-a.queenmq.cloud".into())));
        assert!(cell_a.verify_jwt_claims(&for_a).is_ok(), "the cell it was minted for accepts it");
        assert!(
            matches!(cell_b.verify_jwt_claims(&for_a).unwrap_err(), JwtReject::BadAudience),
            "the sibling must refuse a token that names cell A"
        );

        // The same token against a cell with no audience configured: accepted,
        // which is exactly the replay this closes and the behaviour every
        // deployment keeps until it configures one.
        let unconfigured = Keys::build(None, Some(pub_pem), None, "queen-proxy".to_string(), None);
        assert!(unconfigured.verify_jwt_claims(&for_a).is_ok());
    }

    /// The rule itself, without crypto in the way.
    #[test]
    fn audience_matrix() {
        // Nothing configured: every token passes, stamped or not.
        assert!(audience_ok(None, None));
        assert!(audience_ok(None, Some(&Audience::One("anything".into()))));
        // Configured: absent or blank aud passes, ours passes, theirs does not.
        let want = Some("cell-a");
        assert!(audience_ok(want, None));
        assert!(audience_ok(want, Some(&Audience::One(String::new()))));
        assert!(audience_ok(want, Some(&Audience::Many(vec![]))));
        assert!(audience_ok(want, Some(&Audience::One("cell-a".into()))));
        assert!(audience_ok(want, Some(&Audience::One(" cell-a ".into()))));
        assert!(audience_ok(want, Some(&Audience::Many(vec!["cell-b".into(), "cell-a".into()]))));
        assert!(!audience_ok(want, Some(&Audience::One("cell-b".into()))));
        assert!(!audience_ok(want, Some(&Audience::Many(vec!["cell-b".into(), "cell-c".into()]))));
        // A blank entry beside a real one does not turn the claim into "absent".
        assert!(!audience_ok(want, Some(&Audience::Many(vec!["".into(), "cell-b".into()]))));
    }

    /// `aud` is read off the wire, so both JSON shapes have to survive serde.
    /// A string-only parse would fail the whole decode on the array form, which
    /// reads as a malformed token rather than as the deserialization choice it
    /// really is.
    #[test]
    fn both_audience_json_shapes_deserialize() {
        let one: UserClaims = serde_json::from_str(
            r#"{"sub":"s","iss":"i","exp":1,"jti":"j","role":"viewer","aud":"cell-a"}"#,
        )
        .unwrap();
        assert!(matches!(one.aud, Some(Audience::One(ref s)) if s == "cell-a"));
        let many: UserClaims = serde_json::from_str(
            r#"{"sub":"s","iss":"i","exp":1,"jti":"j","role":"viewer","aud":["cell-a","cell-b"]}"#,
        )
        .unwrap();
        assert!(matches!(many.aud, Some(Audience::Many(ref v)) if v.len() == 2));
        let none: UserClaims =
            serde_json::from_str(r#"{"sub":"s","iss":"i","exp":1,"jti":"j","role":"viewer"}"#)
                .unwrap();
        assert!(none.aud.is_none());
        // And the mint path never writes the key at all.
        let minted = serde_json::to_string(&none).unwrap();
        assert!(!minted.contains("aud"), "{minted}");
    }

    #[test]
    fn foreign_signature_rejected() {
        let a = ed_keys("queen-proxy");
        let b = ed_keys("queen-proxy");
        let token = a.mint_user_jwt(Uuid::new_v4(), "viewer", None, 3600).unwrap();
        let err = b.verify_jwt_claims(&token).unwrap_err();
        assert!(matches!(err, JwtReject::BadSignature), "got {err:?}");
    }

    #[test]
    fn cluster_binding_matrix() {
        let req = Uuid::new_v4();
        let other = Uuid::new_v4();
        // claim bound to this cluster -> trust claim role
        assert_eq!(bind_cluster(Some(req), Role::Admin, req), ClusterBinding::Trust(Role::Admin));
        // claim bound elsewhere -> mismatch (403)
        assert_eq!(bind_cluster(Some(other), Role::Admin, req), ClusterBinding::Mismatch);
        // no claim -> DB lookup
        assert_eq!(bind_cluster(None, Role::Admin, req), ClusterBinding::Lookup);
    }

    #[test]
    fn cluster_claim_survives_verify() {
        // A token minted for a specific cluster verifies with that cluster, so
        // authenticate's mismatch check has real data to compare.
        let keys = ed_keys("queen-proxy");
        let cl = Uuid::new_v4();
        let token = keys.mint_user_jwt(Uuid::new_v4(), "consumer", Some(cl), 600).unwrap();
        let vc = keys.verify_jwt_claims(&token).unwrap();
        assert_eq!(vc.cluster, Some(cl));
        // a different request cluster -> Mismatch
        assert_eq!(bind_cluster(vc.cluster, vc.role, Uuid::new_v4()), ClusterBinding::Mismatch);
    }

    #[test]
    fn jwks_eddsa_real_and_kid_matches_header() {
        let keys = ed_keys("queen-proxy");
        let j: serde_json::Value = serde_json::from_str(&keys.jwks_json()).unwrap();
        let k = &j["keys"][0];
        assert_eq!(k["kty"], "OKP");
        assert_eq!(k["crv"], "Ed25519");
        assert_eq!(k["alg"], "EdDSA");
        assert_eq!(k["use"], "sig");

        let kid = k["kid"].as_str().unwrap();
        assert_eq!(kid.len(), 32, "sha256[..16] hex = 32 chars");

        // x decodes to a raw 32-byte key
        let x = k["x"].as_str().unwrap();
        let raw = B64_URL.decode(x).unwrap();
        assert_eq!(raw.len(), 32);

        // the minted header kid equals the JWKS kid (verifier can select the key)
        let token = keys.mint_user_jwt(Uuid::new_v4(), "viewer", None, 60).unwrap();
        let hdr = jsonwebtoken::decode_header(&token).unwrap();
        assert_eq!(hdr.kid.as_deref(), Some(kid));
    }

    #[test]
    fn jwks_hs_is_empty() {
        let keys = hs_keys("queen-proxy");
        let j: serde_json::Value = serde_json::from_str(&keys.jwks_json()).unwrap();
        assert_eq!(j["keys"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn separate_public_pem_override_verifies_and_publishes() {
        // Auth-host holds the private key; the public key is supplied separately
        // (the verify-only material path). Both must agree.
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let kp = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let raw_pub = kp.public_key().as_ref().to_vec();

        let priv_pem = der_to_pem(pkcs8.as_ref(), "PRIVATE KEY");
        // hand-build the Ed25519 SPKI DER: fixed 12-byte prefix + raw key
        let mut spki = vec![0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00];
        spki.extend_from_slice(&raw_pub);
        let pub_pem = der_to_pem(&spki, "PUBLIC KEY");

        let keys = Keys::build(Some(priv_pem), Some(pub_pem), None, "queen-proxy".to_string(), None);
        let token = keys.mint_user_jwt(Uuid::new_v4(), "consumer", None, 3600).unwrap();
        assert!(keys.verify_jwt_claims(&token).is_ok());

        // JWKS x equals the real public key
        let j: serde_json::Value = serde_json::from_str(&keys.jwks_json()).unwrap();
        let x = j["keys"][0]["x"].as_str().unwrap();
        assert_eq!(B64_URL.decode(x).unwrap(), raw_pub);
    }

    #[test]
    fn ring_extraction_matches_override() {
        // The ring-derived public (build with override=None) must equal the
        // SPKI-parsed public for the same key — proving the two code paths agree.
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let priv_pem = der_to_pem(pkcs8.as_ref(), "PRIVATE KEY");
        let raw_from_ring = ed_raw_pub_from_private_pem(&priv_pem).unwrap();

        let kp = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        assert_eq!(raw_from_ring, kp.public_key().as_ref());
    }

    #[test]
    fn unknown_role_cannot_be_minted() {
        let keys = ed_keys("queen-proxy");
        assert!(keys.mint_user_jwt(Uuid::new_v4(), "superuser", None, 60).is_err());
    }

    #[test]
    fn role_str_roundtrip() {
        for r in [Role::Admin, Role::Producer, Role::Consumer, Role::Viewer] {
            assert_eq!(role_from_str(role_as_str(r)), Some(r));
        }
        assert!(role_from_str("nope").is_none());
    }

    /// A pool that can never connect: `pool.get()` fails on a refused TCP
    /// connect, which is exactly the "deny-list lookup unavailable" branch of
    /// `is_revoked` — no live Postgres needed to pin the policy.
    fn unreachable_pool() -> deadpool_postgres::Pool {
        let mut pg = tokio_postgres::Config::new();
        // :1 is never listening; connect_timeout bounds the test if some
        // environment blackholes it instead of refusing.
        pg.host("127.0.0.1")
            .port(1)
            .user("nobody")
            .dbname("nope")
            .connect_timeout(Duration::from_secs(2));
        let mgr = deadpool_postgres::Manager::from_config(
            pg,
            tokio_postgres::NoTls,
            deadpool_postgres::ManagerConfig {
                recycling_method: deadpool_postgres::RecyclingMethod::Fast,
            },
        );
        deadpool_postgres::Pool::builder(mgr).max_size(1).build().unwrap()
    }

    #[tokio::test]
    async fn revocation_fails_open_by_default() {
        let keys = hs_keys("queen-proxy");
        assert!(!keys.revocation_strict, "default policy is fail-open");
        let db = Some(unreachable_pool());
        assert!(
            !keys.is_revoked(&db, "some-jti").await,
            "an unreachable pxdb must not revoke every session by default"
        );
        assert!(
            keys.revoked_cache.lock().unwrap().is_empty(),
            "a non-answer must not be cached for 60s"
        );
    }

    #[tokio::test]
    async fn revocation_fails_closed_when_strict() {
        let mut keys = hs_keys("queen-proxy");
        keys.revocation_strict = true;
        let db = Some(unreachable_pool());
        assert!(
            keys.is_revoked(&db, "some-jti").await,
            "strict mode must treat an unavailable deny-list as revoked"
        );
        assert!(
            keys.revoked_cache.lock().unwrap().is_empty(),
            "a non-answer must not be cached, so recovery is immediate"
        );
    }

    #[tokio::test]
    async fn no_pxdb_means_not_revoked_in_either_policy() {
        for strict in [false, true] {
            let mut keys = hs_keys("queen-proxy");
            keys.revocation_strict = strict;
            assert!(
                !keys.is_revoked(&None, "some-jti").await,
                "dev-static mode has no deny-list at all (strict={strict})"
            );
        }
    }

    #[test]
    fn note_revoked_is_honoured_by_the_cache() {
        let keys = hs_keys("queen-proxy");
        keys.note_revoked("jti-1");
        // Cached positive answers are returned without touching the pool, so an
        // unreachable pxdb cannot resurrect a token this proxy just revoked.
        let db = Some(unreachable_pool());
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        assert!(rt.block_on(keys.is_revoked(&db, "jti-1")));
        assert!(!rt.block_on(keys.is_revoked(&db, "jti-2")), "unrelated jti unaffected (fail-open)");
    }

    #[test]
    fn warn_sampler_emits_once_per_window_with_the_suppressed_count() {
        let mut s = WarnSampler::default();
        let t0 = Instant::now();
        let every = Duration::from_secs(30);
        assert_eq!(s.tick(t0, every), Some(0), "first event always logs");
        assert_eq!(s.tick(t0 + Duration::from_secs(1), every), None);
        assert_eq!(s.tick(t0 + Duration::from_secs(29), every), None);
        // window elapsed: log again, reporting the two it stood in for
        assert_eq!(s.tick(t0 + Duration::from_secs(31), every), Some(2));
        // counter resets after each emitted line
        assert_eq!(s.tick(t0 + Duration::from_secs(32), every), None);
        assert_eq!(s.tick(t0 + Duration::from_secs(62), every), Some(1));
    }

    #[test]
    fn verified_claims_carry_exp_for_revocation() {
        // logout deny-lists (jti, exp): both must survive verification, or the
        // revoked_tokens row cannot be swept when the token dies on its own.
        let keys = ed_keys("queen-proxy");
        let before = now_secs();
        let token = keys.mint_user_jwt(Uuid::new_v4(), "viewer", None, 3600).unwrap();
        let vc = keys.verify_jwt_claims(&token).unwrap();
        assert!(vc.exp >= before + 3600 && vc.exp <= now_secs() + 3600);
        assert!(!vc.jti.is_empty());
    }

    // ---- operator gate (F3): the authorize half of the capability ----------

    fn user(role: Role, operator: bool) -> Principal {
        Principal::User { user_id: Uuid::new_v4(), role, operator }
    }

    #[test]
    fn only_a_live_operator_opens_the_operator_class() {
        assert!(authorize(&user(Role::Admin, true), RouteClass::Operator).is_ok());
        // The bit, not the role: an admin without it is refused, an operator
        // is admitted whatever role the cluster gave them.
        assert!(authorize(&user(Role::Admin, false), RouteClass::Operator).is_err());
        for r in [Role::Producer, Role::Consumer, Role::Viewer] {
            assert!(authorize(&user(r, false), RouteClass::Operator).is_err());
            assert!(authorize(&user(r, true), RouteClass::Operator).is_ok());
        }
        // An api key can never be an operator — not even one with every scope.
        let key = Principal::ApiKey { key_id: Uuid::new_v4(), scopes: Scopes::all() };
        assert!(authorize(&key, RouteClass::Operator).is_err());
    }

    #[test]
    fn operator_bit_opens_nothing_that_is_hard_blocked() {
        // `Blocked` is checked before any principal arm and stays absolute:
        // migration/internal/stats-refresh/discovery-pop are not dashboard
        // data, and the capability must not become a general skeleton key.
        assert!(authorize(&user(Role::Admin, true), RouteClass::Blocked).is_err());
        let key = Principal::ApiKey { key_id: Uuid::new_v4(), scopes: Scopes::all() };
        assert!(authorize(&key, RouteClass::Blocked).is_err());
    }

    #[test]
    fn operator_does_not_change_the_ordinary_matrix() {
        // The capability adds a class; it does not relax the existing rows.
        // (An operator's role is Admin anyway — this pins that the flag alone
        // is not what grants produce/consume.)
        assert!(authorize(&user(Role::Viewer, true), RouteClass::Produce).is_err());
        assert!(authorize(&user(Role::Viewer, true), RouteClass::QueueAdmin).is_err());
        assert!(authorize(&user(Role::Viewer, false), RouteClass::Read).is_ok());
    }

    // ---- PLAN_KV_TIMERS.md §9.8 P1: the gated read/write split ----

    #[test]
    fn a_gated_read_is_a_read_for_authorization() {
        use crate::routes::{Feature, GatedOp};
        let read_only =
            Principal::ApiKey { key_id: Uuid::new_v4(), scopes: Scopes { read: true, ..Default::default() } };
        // GET /api/v1/kv/:ns/*key is `ReadOnly` in the broker's own access
        // table (§8.1); a read-scoped key must not be 403'd at the proxy for
        // an operation the broker would allow.
        assert!(authorize(&read_only, RouteClass::Gated(Feature::Kv, GatedOp::Read)).is_ok());
        assert!(authorize(&user(Role::Viewer, false), RouteClass::Gated(Feature::Kv, GatedOp::Read)).is_ok());
    }

    #[test]
    fn a_gated_write_still_needs_a_writing_credential() {
        use crate::routes::{Feature, GatedOp};
        let read_only =
            Principal::ApiKey { key_id: Uuid::new_v4(), scopes: Scopes { read: true, ..Default::default() } };
        // Reading a key does not license writing one, cancelling one, or
        // scheduling one — including on the never-quota-blocked `Open` half,
        // which is about quotas, not about credentials.
        for op in [GatedOp::Grow, GatedOp::Open, GatedOp::Mixed] {
            assert!(
                authorize(&read_only, RouteClass::Gated(Feature::Timers, op)).is_err(),
                "{op:?} must not be reachable with a read-only key"
            );
            assert!(authorize(&user(Role::Viewer, false), RouteClass::Gated(Feature::Timers, op)).is_err());
        }
        let producer = Principal::ApiKey {
            key_id: Uuid::new_v4(),
            scopes: Scopes { produce: true, ..Default::default() },
        };
        assert!(authorize(&producer, RouteClass::Gated(Feature::Timers, GatedOp::Mixed)).is_ok());
    }

    /// EPHEMERAL_QUEUES.md §5.1: the RAM family's writes need a writing
    /// credential (produce ‖ consume keys, users that are not Viewer) and its
    /// two status reads are reads — the same matrix the existing gated
    /// families already produce, inherited rather than re-invented.
    #[test]
    fn ephemeral_authorization_follows_the_gated_matrix() {
        use crate::routes::{Feature, GatedOp};
        let read_only =
            Principal::ApiKey { key_id: Uuid::new_v4(), scopes: Scopes { read: true, ..Default::default() } };
        // depth/queues listings
        assert!(authorize(&read_only, RouteClass::Gated(Feature::Ephemeral, GatedOp::Read)).is_ok());
        assert!(
            authorize(&user(Role::Viewer, false), RouteClass::Gated(Feature::Ephemeral, GatedOp::Read))
                .is_ok()
        );
        // push (Grow) and ack/configure/reset/delete (Open) are writes
        for op in [GatedOp::Grow, GatedOp::Open] {
            assert!(
                authorize(&read_only, RouteClass::Gated(Feature::Ephemeral, op)).is_err(),
                "{op:?} must not be reachable with a read-only key"
            );
            assert!(
                authorize(&user(Role::Viewer, false), RouteClass::Gated(Feature::Ephemeral, op))
                    .is_err()
            );
        }
        // a consume-scoped key pops and acks; a produce-scoped one pushes
        let consumer = Principal::ApiKey {
            key_id: Uuid::new_v4(),
            scopes: Scopes { consume: true, ..Default::default() },
        };
        assert!(authorize(&consumer, RouteClass::Gated(Feature::Ephemeral, GatedOp::Open)).is_ok());
        let producer = Principal::ApiKey {
            key_id: Uuid::new_v4(),
            scopes: Scopes { produce: true, ..Default::default() },
        };
        assert!(authorize(&producer, RouteClass::Gated(Feature::Ephemeral, GatedOp::Grow)).is_ok());
    }

    /// The pre-existing gated surfaces must come out of this change with the
    /// exact matrix they had. For traces and the streams register/state routes
    /// that is `Open`, untouched.
    ///
    /// The streams CYCLE is the one that moved: routes.rs classifies it
    /// `Gated(Streams, Mixed)` since the tenant-compat pass, because its sink
    /// emits are produce and a storage/monthly block must be able to refuse a
    /// growing body. That is a QUOTA decision, and quotas are not credentials —
    /// so the AUTHORIZATION of the cycle must be bit-for-bit what `Open` gave
    /// it. It is, because both land on the same `Gated(_, non-Read)` arm, which
    /// asks for produce ‖ consume (and, for users, "not Viewer"). Asserted as an
    /// equivalence rather than as two plausible-looking outcomes: the claim
    /// being made is "unchanged", so that is the claim under test.
    #[test]
    fn streams_and_traces_authorization_is_unchanged() {
        use crate::routes::{Feature, GatedOp};
        let read_only =
            Principal::ApiKey { key_id: Uuid::new_v4(), scopes: Scopes { read: true, ..Default::default() } };
        assert!(authorize(&read_only, RouteClass::Gated(Feature::Streams, GatedOp::Open)).is_err());
        assert!(authorize(&user(Role::Viewer, false), RouteClass::Gated(Feature::Traces, GatedOp::Open)).is_err());
        assert!(authorize(&user(Role::Consumer, false), RouteClass::Gated(Feature::Streams, GatedOp::Open)).is_ok());

        // The cycle, principal by principal, before (`Open`) and after
        // (`Mixed`).
        let produce_key = Principal::ApiKey {
            key_id: Uuid::new_v4(),
            scopes: Scopes { produce: true, ..Default::default() },
        };
        let consume_key = Principal::ApiKey {
            key_id: Uuid::new_v4(),
            scopes: Scopes { consume: true, ..Default::default() },
        };
        for p in [
            &read_only,
            &produce_key,
            &consume_key,
            &Principal::ApiKey { key_id: Uuid::new_v4(), scopes: Scopes::all() },
            &user(Role::Admin, false),
            &user(Role::Producer, false),
            &user(Role::Consumer, false),
            &user(Role::Viewer, false),
        ] {
            assert_eq!(
                authorize(p, RouteClass::Gated(Feature::Streams, GatedOp::Open)).is_ok(),
                authorize(p, RouteClass::Gated(Feature::Streams, GatedOp::Mixed)).is_ok(),
                "reclassifying the cycle must not move any credential: {p:?}"
            );
        }
        // ...and the outcomes themselves, spelled out on the class the cycle
        // actually carries now. A streaming job runs on one key, so BOTH halves
        // of its wire — the source ack and the sink emits — have to be
        // reachable with a produce-scoped key and with a consume-scoped one.
        assert!(authorize(&produce_key, RouteClass::Gated(Feature::Streams, GatedOp::Mixed)).is_ok());
        assert!(authorize(&consume_key, RouteClass::Gated(Feature::Streams, GatedOp::Mixed)).is_ok());
        assert!(authorize(&read_only, RouteClass::Gated(Feature::Streams, GatedOp::Mixed)).is_err());
        assert!(authorize(&user(Role::Viewer, false), RouteClass::Gated(Feature::Streams, GatedOp::Mixed)).is_err());
    }

    // ---- credential extraction ---------------------------------------------

    fn headers_with(cookie: Option<&str>, auth: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(c) = cookie {
            h.insert(axum::http::header::COOKIE, axum::http::HeaderValue::from_str(c).unwrap());
        }
        if let Some(a) = auth {
            h.insert(axum::http::header::AUTHORIZATION, axum::http::HeaderValue::from_str(a).unwrap());
        }
        h
    }

    #[test]
    fn credential_prefers_authorization_over_cookie() {
        let h = headers_with(Some("queen_session=cookietoken"), Some("Bearer explicit"));
        assert_eq!(
            read_credential("queen_session", true, &h),
            Credential::Session("explicit".to_string())
        );
    }

    #[test]
    fn credential_falls_back_to_the_session_cookie() {
        let h = headers_with(Some("other=1; queen_session=abc123; another=2"), None);
        assert_eq!(
            read_credential("queen_session", true, &h),
            Credential::Session("abc123".to_string())
        );
        // exact name match only
        let h2 = headers_with(Some("queen_session_v2=wrong; queen_session=right"), None);
        assert_eq!(
            read_credential("queen_session", true, &h2),
            Credential::Session("right".to_string())
        );
    }

    // ---- fleet cookie vs cell cookie ---------------------------------------

    /// ⚠ The precedence the console handoff depends on, and the reason the two
    /// cookies do not share a name.
    ///
    /// A browser on `<cell>.queenmq.cloud` presents the fleet cookie (set at the
    /// portal, `Domain=.queenmq.cloud`) AND, once a handoff was established
    /// here, this host's own cell cookie. The session the user just established
    /// has to win — and it has to win whichever way round the jar emits them.
    /// RFC 6265 §5.4 sorts equal paths oldest-first, which puts the FLEET
    /// cookie first in the real case, so a rule that took the header's first
    /// match would take the wrong one every time.
    #[test]
    fn the_cell_cookie_wins_over_the_fleet_cookie_on_the_host_that_set_it() {
        for raw in [
            "queen_session=fleet; __Host-queen_session_cell=cell",
            "__Host-queen_session_cell=cell; queen_session=fleet",
        ] {
            let h = headers_with(Some(raw), None);
            assert_eq!(
                read_credential("queen_session", true, &h),
                Credential::Session("cell".to_string()),
                "cell cookie must win, order notwithstanding: {raw}"
            );
        }
    }

    /// And on every cell the user has NOT opened a console on — which is every
    /// sibling, and the whole point of the fix — the fleet cookie is the
    /// session, exactly as before.
    #[test]
    fn the_fleet_cookie_is_the_session_where_no_handoff_was_established() {
        let h = headers_with(Some("other=1; queen_session=fleet"), None);
        assert_eq!(
            read_credential("queen_session", true, &h),
            Credential::Session("fleet".to_string())
        );
        // An empty cell cookie is not a session either: a cleared cookie the
        // browser still sends must fall through, not shadow a live fleet one.
        let h = headers_with(Some("__Host-queen_session_cell=; queen_session=fleet"), None);
        assert_eq!(
            read_credential("queen_session", true, &h),
            Credential::Session("fleet".to_string())
        );
    }

    /// The name follows `Secure`, because `__Host-` is only storable with it.
    /// Over plain http (local dev, no fleet domain, nothing to be shadowed by)
    /// the plain name is what both halves use.
    #[test]
    fn the_cell_cookie_carries_the_host_prefix_only_when_secure() {
        assert_eq!(cell_cookie_name_of("queen_session", true), "__Host-queen_session_cell");
        assert_eq!(cell_cookie_name_of("queen_session", false), "queen_session_cell");

        let h = headers_with(Some("queen_session_cell=cell; queen_session=fleet"), None);
        assert_eq!(
            read_credential("queen_session", false, &h),
            Credential::Session("cell".to_string())
        );
        // ...and a Secure host does not read the unprefixed name, which any
        // sibling on the fleet domain could have set.
        assert_eq!(
            read_credential("queen_session", true, &h),
            Credential::Session("fleet".to_string())
        );
    }

    #[test]
    fn credential_recognises_api_keys_by_prefix() {
        let h = headers_with(None, Some("Bearer qk_live_abc"));
        assert_eq!(read_credential("queen_session", true, &h), Credential::ApiKey("qk_live_abc".to_string()));
        // a bare (non-Bearer) Authorization value is accepted verbatim, as the
        // old bearer parse did
        let h2 = headers_with(None, Some("qk_live_abc"));
        assert_eq!(read_credential("queen_session", true, &h2), Credential::ApiKey("qk_live_abc".to_string()));
    }

    #[test]
    fn an_api_key_in_a_cookie_is_not_a_credential() {
        // A cookie is attached by the browser, not chosen per request: it must
        // never be able to widen a caller into a data-plane key.
        let h = headers_with(Some("queen_session=qk_live_abc"), None);
        assert_eq!(read_credential("queen_session", true, &h), Credential::None);
    }

    #[test]
    fn a_cookie_is_refused_on_a_cross_document_navigation() {
        // The one CSRF shape SameSite=Lax lets through: a hostile page
        // top-level-navigating a logged-in browser to a mutating GET
        // (/api/v1/pop leases messages). Script-issued calls never carry
        // Sec-Fetch-Mode: navigate.
        let mut h = headers_with(Some("queen_session=abc123"), None);
        h.insert("sec-fetch-mode", axum::http::HeaderValue::from_static("navigate"));
        assert_eq!(read_credential("queen_session", true, &h), Credential::None);
        // ... but the app shell itself IS loaded by navigation, and serving
        // static bytes forges nothing.
        assert_eq!(
            read_credential_for_document("queen_session", true, &h),
            Credential::Session("abc123".to_string())
        );
    }

    #[test]
    fn navigation_does_not_affect_explicit_credentials_or_normal_fetches() {
        // An Authorization header is chosen per request; a third-party page
        // cannot attach one, so the navigation rule never applies to it.
        let mut h = headers_with(None, Some("Bearer qk_live_abc"));
        h.insert("sec-fetch-mode", axum::http::HeaderValue::from_static("navigate"));
        assert_eq!(read_credential("queen_session", true, &h), Credential::ApiKey("qk_live_abc".to_string()));

        // The SPA's own XHR, and any non-browser client (no Sec-Fetch-* at all).
        for mode in ["cors", "same-origin", "no-cors"] {
            let mut h = headers_with(Some("queen_session=abc123"), None);
            h.insert("sec-fetch-mode", axum::http::HeaderValue::from_str(mode).unwrap());
            assert_eq!(
                read_credential("queen_session", true, &h),
                Credential::Session("abc123".to_string()),
                "mode {mode}"
            );
        }
        let bare = headers_with(Some("queen_session=abc123"), None);
        assert_eq!(read_credential("queen_session", true, &bare), Credential::Session("abc123".to_string()));
    }

    #[test]
    fn credential_none_when_nothing_usable_is_present() {
        assert_eq!(read_credential("queen_session", true, &headers_with(None, None)), Credential::None);
        // empty Authorization falls through to the (absent) cookie
        assert_eq!(
            read_credential("queen_session", true, &headers_with(None, Some("Bearer   "))),
            Credential::None
        );
        assert_eq!(
            read_credential("queen_session", true, &headers_with(Some("queen_session="), None)),
            Credential::None
        );
    }

    #[tokio::test]
    async fn is_operator_fails_closed_without_a_control_plane() {
        let keys = hs_keys("queen-proxy");
        let uid = Uuid::new_v4();
        // no pxdb at all (dev-static), and an unreachable one: both deny.
        assert!(!keys.is_operator(&None, uid).await);
        assert!(!keys.is_operator(&Some(unreachable_pool()), uid).await);
        assert!(
            keys.operator_cache.lock().unwrap().is_empty(),
            "a non-answer must not be cached as a grant or a denial"
        );
    }

    #[test]
    fn no_signer_configured_cannot_mint_or_verify() {
        let keys = Keys::build(None, None, None, "queen-proxy".to_string(), None);
        assert!(keys.mint_user_jwt(Uuid::new_v4(), "admin", None, 60).is_err());
        assert!(matches!(keys.verify_jwt_claims("x.y.z"), Err(JwtReject::NotConfigured)));
        // What main.rs's boot gate reads, and the shape it refuses in mint mode.
        assert!(!keys.can_mint());
        assert!(!keys.can_verify());
    }

    // ---- capability, as the boot gate sees it (spec §9b) --------------------

    /// A private key + the matching PEM-armored SPKI public key for it.
    fn ed_pair() -> (String, String, Vec<u8>) {
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let kp = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let raw_pub = kp.public_key().as_ref().to_vec();
        let mut spki = vec![0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00];
        spki.extend_from_slice(&raw_pub);
        (der_to_pem(pkcs8.as_ref(), "PRIVATE KEY"), der_to_pem(&spki, "PUBLIC KEY"), raw_pub)
    }

    #[test]
    fn a_configured_signer_can_both_mint_and_verify() {
        for keys in [ed_keys("queen-proxy"), hs_keys("queen-proxy")] {
            assert!(keys.can_mint());
            assert!(keys.can_verify());
        }
    }

    /// The verify-only host: public key, no private key, no HS secret. It must
    /// accept what the auth host minted and mint nothing itself — the shape
    /// `Signer::Ed { enc: None }` has always documented and that nothing built,
    /// so a public-key-only cell rejected every session it was configured for.
    #[test]
    fn a_public_key_alone_verifies_and_refuses_to_mint() {
        let (priv_pem, pub_pem, raw_pub) = ed_pair();
        let auth_host = Keys::build(Some(priv_pem), None, None, "queen-proxy".to_string(), None);
        let token = auth_host.mint_user_jwt(Uuid::new_v4(), "admin", None, 3600).unwrap();

        let cell = Keys::build(None, Some(pub_pem), None, "queen-proxy".to_string(), None);
        assert!(cell.can_verify(), "a verify-only host must verify");
        assert!(!cell.can_mint(), "a verify-only host holds no private key");
        assert!(cell.verify_jwt_claims(&token).is_ok(), "the auth host's token must be accepted");
        assert!(cell.mint_user_jwt(Uuid::new_v4(), "admin", None, 60).is_err());

        // It publishes the verification key, which is the whole point of JWKS.
        let j: serde_json::Value = serde_json::from_str(&cell.jwks_json()).unwrap();
        assert_eq!(B64_URL.decode(j["keys"][0]["x"].as_str().unwrap()).unwrap(), raw_pub);
        // Same kid as the minting host: a verifier can select the key.
        assert_eq!(
            j["keys"][0]["kid"],
            serde_json::from_str::<serde_json::Value>(&auth_host.jwks_json()).unwrap()["keys"][0]
                ["kid"]
        );
    }

    #[test]
    fn an_unparseable_public_pem_alone_configures_nothing() {
        // Boot-gate input: `can_verify == false` in verify-only mode is what
        // turns a typo'd CA-style paste into a refusal instead of a cell that
        // 401s every session with a green data plane.
        let keys = Keys::build(None, Some("-----BEGIN PUBLIC KEY-----\nnope\n-----END PUBLIC KEY-----\n".to_string()), None, "queen-proxy".to_string(), None);
        assert!(!keys.can_verify());
        assert!(!keys.can_mint());
    }

    #[test]
    fn an_hs_secret_still_wins_over_a_public_pem() {
        // Unchanged behaviour, pinned: every dev and bench cell in this repo
        // sets QUEEN_PROXY_JWT_SECRET, and the verify-only arm must not be able
        // to take the signer out from under one that also carries a public key.
        let (priv_pem, pub_pem, _) = ed_pair();
        let keys =
            Keys::build(None, Some(pub_pem), Some("dev-secret-value".into()), "queen-proxy".into(), None);
        assert!(keys.can_mint(), "HS256 mints");
        let own = keys.mint_user_jwt(Uuid::new_v4(), "viewer", None, 60).unwrap();
        assert!(keys.verify_jwt_claims(&own).is_ok());
        // And the algorithm is still pinned to HS: the Ed token does not verify.
        let ed = Keys::build(Some(priv_pem), None, None, "queen-proxy".to_string(), None)
            .mint_user_jwt(Uuid::new_v4(), "viewer", None, 60)
            .unwrap();
        assert!(keys.verify_jwt_claims(&ed).is_err());
    }
}
