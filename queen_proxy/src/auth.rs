//! Authentication + authorization. OWNER: Agent C (credential verification,
//! key lookup wiring, JWT mint/verify, JWKS). The `authorize` matrix below is
//! FINAL (spec §14) — extend only via report.
//!
//! Skeleton: dev_insecure grants full scopes; real Bearer handling is Agent C.

use axum::http::HeaderMap;
use axum::response::Response;
use sha2::{Digest, Sha256};

use crate::errors;
use crate::routes::RouteClass;
use crate::state::{Principal, Role, Scopes, St};

/// sha256 hex of an API key string — the stored representation in api_keys.key_hash.
pub fn key_hash_hex(key: &str) -> String {
    hex::encode(Sha256::digest(key.as_bytes()))
}

/// Authenticate a data-plane request. Returns the Principal, or the ready
/// error Response. The cluster was already resolved from Host by the caller;
/// Agent C must also cross-check key->cluster binding (key of another cluster
/// on this Host -> 421-equivalent 403).
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
    if token.starts_with("qk_") {
        let hash = key_hash_hex(&token);
        match st.cache.by_key_hash(&hash).await {
            Some((ctx, key_id, scopes)) if ctx.cluster_id == cluster_id => {
                return Ok(Principal::ApiKey { key_id, scopes });
            }
            Some(_) => return Err(errors::err_403(errors::CODE_FORBIDDEN, "key/cluster mismatch")),
            None => return Err(errors::err_401("unknown or revoked api key")),
        }
    }
    // Agent C: JWT verify path (HS dev / asym via JWKS), revoked_tokens check,
    // role extraction from cluster_roles.
    Err(errors::err_401("jwt verification not yet available"))
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

/// JWT mint/verify material. OWNER: Agent C — asym mint (Ed25519 PEM, cloud)
/// with private key only on the auth-host instance; HS256 fallback for dev;
/// JWKS JSON served at /.well-known/jwks.json.
pub struct Keys {
    _hs_secret: Option<String>,
}

impl Keys {
    pub fn from_config(cfg: &crate::config::Config) -> Keys {
        Keys { _hs_secret: cfg.jwt_hs_secret.clone() }
    }

    pub fn jwks_json(&self) -> String {
        // Agent C: real key set when asym material is configured.
        "{\"keys\":[]}".to_string()
    }
}
