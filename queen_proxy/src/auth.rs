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
// authenticate / authorize
// ---------------------------------------------------------------------------

/// Authenticate a data-plane request. Returns the Principal, or the ready
/// error Response. The cluster was already resolved from Host by the caller;
/// Agent C also cross-checks key->cluster binding (key of another cluster on
/// this Host -> 403) and, for JWTs, cluster claim / cluster_roles membership.
pub async fn authenticate(
    st: &St,
    headers: &HeaderMap,
    cluster_id: uuid::Uuid,
) -> Result<Principal, Response> {
    if st.cfg.dev_insecure {
        return Ok(Principal::ApiKey { key_id: uuid::Uuid::nil(), scopes: Scopes::all() });
    }
    let bearer = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.strip_prefix("Bearer ").unwrap_or(v).trim().to_string());
    let Some(token) = bearer else {
        return Err(errors::err_401("missing bearer credential"));
    };

    // --- API key path (opaque, hashed lookup) ---
    if token.starts_with(API_KEY_PREFIX) {
        let hash = key_hash_hex(&token);
        return match st.cache.by_key_hash(&hash).await {
            Some((ctx, key_id, scopes)) if ctx.cluster_id == cluster_id => {
                Ok(Principal::ApiKey { key_id, scopes })
            }
            Some(_) => Err(errors::err_403(errors::CODE_FORBIDDEN, "key/cluster mismatch")),
            None => Err(errors::err_401("unknown or revoked api key")),
        };
    }

    // --- JWT path (EdDSA / HS256) ---
    let claims = match st.keys.verify_jwt_claims(&token) {
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

    // Cluster binding. A cluster-scoped token is trusted verbatim; an unscoped
    // token has its per-cluster role resolved live from cluster_roles.
    let role = match bind_cluster(claims.cluster, claims.role, cluster_id) {
        ClusterBinding::Trust(role) => role,
        ClusterBinding::Mismatch => {
            return Err(errors::err_403(errors::CODE_FORBIDDEN, "token not valid for this cluster"));
        }
        ClusterBinding::Lookup => match st.keys.cluster_role(&st.db, claims.user_id, cluster_id).await {
            Some(role) => role,
            None => {
                // Distinguish "user exists but has no role here" (a real 403)
                // from "the session's user no longer exists" — a stale JWT
                // after user deletion (or a dev pxdb reset): that session is
                // dead, and 401 lets the SPA bounce to login instead of
                // leaving a 403 dead-end.
                if !st.keys.user_exists(&st.db, claims.user_id).await {
                    return Err(errors::err_401("session no longer valid"));
                }
                return Err(errors::err_403(errors::CODE_FORBIDDEN, "no role on this cluster"));
            }
        },
    };

    Ok(Principal::User { user_id: claims.user_id, role })
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
        (Principal::ApiKey { scopes, .. }, RouteClass::Produce) => scopes.produce,
        (Principal::ApiKey { scopes, .. }, RouteClass::Consume) => scopes.consume,
        (Principal::ApiKey { scopes, .. }, RouteClass::QueueAdmin) => scopes.admin,
        (Principal::ApiKey { scopes, .. }, RouteClass::Read) => {
            scopes.read || scopes.admin
        }
        (Principal::ApiKey { scopes, .. }, RouteClass::Gated(_)) => {
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
        (Principal::User { role, .. }, RouteClass::Gated(_)) => {
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
/// `jsonwebtoken`; `sub`/`jti`/`role`/`cluster` we validate by hand so the
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
}

/// Post-verification, shape-checked view handed back to `authenticate`.
#[derive(Debug)]
pub struct VerifiedClaims {
    pub user_id: Uuid,
    pub role: Role,
    pub jti: String,
    pub cluster: Option<Uuid>,
}

/// Why a token was rejected — a distinct, non-client-facing reason for logs.
#[derive(Debug)]
pub(crate) enum JwtReject {
    NotConfigured,
    Expired,
    BadIssuer,
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
    /// jti -> (revoked?, checked_at). 60s TTL; bounds revocation propagation.
    revoked_cache: Mutex<HashMap<String, (bool, Instant)>>,
    /// (user, cluster) -> (role, checked_at). 30s TTL; positive memberships only.
    role_cache: Mutex<HashMap<(Uuid, Uuid), (Role, Instant)>>,
}

const REVOKED_TTL: Duration = Duration::from_secs(60);
const ROLE_TTL: Duration = Duration::from_secs(30);
/// Soft cap after which a cache is pruned of stale entries on the next insert.
const AUTH_CACHE_CAP: usize = 100_000;

impl Keys {
    pub fn from_config(cfg: &crate::config::Config) -> Keys {
        // New knob, read directly here (not via config.rs, which Agent C does not
        // own): an OPTIONAL separately-supplied Ed25519 PUBLIC-key PEM. It lets a
        // verify-only host (data-plane proxy / console, per §10) hold only public
        // material, and it is a robustness override when public-from-private
        // derivation is undesirable. When absent, the public key is derived from
        // the private PEM with ring.
        let pub_override = std::env::var("QUEEN_PROXY_JWT_ED25519_PUB_PEM")
            .ok()
            .filter(|s| !s.trim().is_empty());
        Keys::build(
            cfg.jwt_ed25519_pem.clone(),
            pub_override,
            cfg.jwt_hs_secret.clone(),
            cfg.jwt_issuer.clone(),
        )
    }

    /// Construct from raw material. Split out from `from_config` so unit tests
    /// can inject keys without building a whole `Config`.
    fn build(
        ed_priv_pem: Option<String>,
        ed_pub_pem_override: Option<String>,
        hs_secret: Option<String>,
        issuer: String,
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
            None => hs_or_none(hs_secret),
        };
        Keys {
            signer,
            issuer,
            revoked_cache: Mutex::new(HashMap::new()),
            role_cache: Mutex::new(HashMap::new()),
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
        v.validate_aud = false; // our tokens carry no aud
        v.set_issuer(&[self.issuer.as_str()]);

        let data = decode::<UserClaims>(token, dec, &v).map_err(|e| map_jwt_err(&e))?;
        let c = data.claims;

        if c.jti.trim().is_empty() {
            return Err(JwtReject::MissingClaim("jti"));
        }
        let user_id = Uuid::parse_str(c.sub.trim()).map_err(|_| JwtReject::BadClaim("sub"))?;
        let role = role_from_str(&c.role).ok_or(JwtReject::BadClaim("role"))?;
        let cluster = match c.cluster.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            Some(s) => Some(Uuid::parse_str(s).map_err(|_| JwtReject::BadClaim("cluster"))?),
            None => None,
        };

        Ok(VerifiedClaims { user_id, role, jti: c.jti, cluster })
    }

    /// Is this jti on the deny-list? Cached 60s. `db == None` (dev) => not
    /// revoked. A transient pxdb failure fails OPEN (logs a warning, does not
    /// cache) — availability over a brief revocation-propagation gap; the token
    /// still had to pass signature + exp, and the whole data plane already
    /// degrades if pxdb is down.
    pub async fn is_revoked(&self, db: &Option<deadpool_postgres::Pool>, jti: &str) -> bool {
        let Some(pool) = db else { return false };

        if let Some((rev, at)) = self.revoked_cache.lock().unwrap().get(jti) {
            if at.elapsed() < REVOKED_TTL {
                return *rev;
            }
        }

        let revoked = match pool.get().await {
            // jti is a UUID (minted as uuid-v4); bound as text + cast so the
            // column can be `uuid` (Agent B schema) without a tokio-postgres
            // uuid feature dependency here.
            Ok(client) => match client
                .query_opt(
                    // jti column is TEXT by design (001_init.sql): the deny-list
                    // must accept non-UUID jtis from foreign token mints too.
                    "SELECT 1 FROM queen_proxy.revoked_tokens WHERE jti = $1",
                    &[&jti],
                )
                .await
            {
                Ok(row) => row.is_some(),
                Err(e) => {
                    tracing::warn!(target: "auth", err = %e, "revoked_tokens query failed; fail-open");
                    return false;
                }
            },
            Err(e) => {
                tracing::warn!(target: "auth", err = %e, "pxdb unavailable for revoked check; fail-open");
                return false;
            }
        };

        let mut cache = self.revoked_cache.lock().unwrap();
        if cache.len() >= AUTH_CACHE_CAP {
            cache.retain(|_, (_, at)| at.elapsed() < REVOKED_TTL);
        }
        cache.insert(jti.to_string(), (revoked, Instant::now()));
        revoked
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

    /// The JWKS document served at `/.well-known/jwks.json`. EdDSA => a real
    /// one-key OKP/Ed25519 set; HS or unconfigured => `{"keys":[]}` (nothing
    /// public to publish).
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;
    use ring::signature::{Ed25519KeyPair, KeyPair};

    fn der_to_pem(der: &[u8], label: &str) -> String {
        let b64 = B64_STD.encode(der);
        let mut body = String::new();
        for chunk in b64.as_bytes().chunks(64) {
            body.push_str(std::str::from_utf8(chunk).unwrap());
            body.push('\n');
        }
        format!("-----BEGIN {label}-----\n{body}-----END {label}-----\n")
    }

    fn ed_keys(issuer: &str) -> Keys {
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let pem = der_to_pem(pkcs8.as_ref(), "PRIVATE KEY");
        Keys::build(Some(pem), None, None, issuer.to_string())
    }

    fn hs_keys(issuer: &str) -> Keys {
        Keys::build(None, None, Some("dev-secret-value".to_string()), issuer.to_string())
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
        };
        let token = keys.sign(&claims).unwrap();
        let err = keys.verify_jwt_claims(&token).unwrap_err();
        assert!(matches!(err, JwtReject::BadIssuer), "got {err:?}");
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

        let keys = Keys::build(Some(priv_pem), Some(pub_pem), None, "queen-proxy".to_string());
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

    #[test]
    fn no_signer_configured_cannot_mint_or_verify() {
        let keys = Keys::build(None, None, None, "queen-proxy".to_string());
        assert!(keys.mint_user_jwt(Uuid::new_v4(), "admin", None, 60).is_err());
        assert!(matches!(keys.verify_jwt_claims("x.y.z"), Err(JwtReject::NotConfigured)));
    }
}
