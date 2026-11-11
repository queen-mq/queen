//! Scoped clients (`queen_protocol::scope`): an API key with grants, used
//! directly, and the client tokens minted from it — who such a caller is,
//! what it may touch, the per-key and per-entity rate limits, the block list.
//!
//! Who decides what:
//! - the proxy authenticates the credential and builds its SCOPE: the key's
//!   grants with `{sub}` filled in, then the token's own grants, one layer
//!   each. It opens only the routes that check names to such a caller
//!   (`queen_protocol::scope::scoped_route`) and hands the scope to the broker
//!   with the request: the [`SCOPE_HEADER`], and the same scope as a typed
//!   extension when the broker is this process;
//! - the broker checks every name where it resolves it (server/src/scope.rs):
//!   only it can, since an ack names its partition by number and a binary
//!   push names its partitions inside groups this proxy does not open.
//!
//! A client token is [`TOKEN_PREFIX`] and a JWT signed by this proxy's
//! session signer, `typ: "client"`. It names its key by id, its cluster, its
//! entity (`sub`), its expiry, and optionally grants that narrow the key's.
//! It is never stored. Its key is read on every request (cached, invalidated
//! within a second), so revoking or narrowing a key reaches every token it
//! minted; a blocked entity is refused on every node within a second.

use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::http::{HeaderValue, StatusCode};
use axum::response::Response;
use queen_protocol::scope::{layer, Grant, Scope, ScopeGrant};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::errors;
use crate::state::{ClusterCtx, Principal, St};
use crate::store::schema::{KeyLimitsDoc, KeyPolicyDoc};
use crate::store::{data, Store};

pub use queen_protocol::scope::SCOPE_HEADER;

/// What every client token starts with. It routes the credential without a
/// decode, and lets a secret scanner tell a Queen client token from any JWT.
pub const TOKEN_PREFIX: &str = "qt_";

/// The lifetime of a token whose request names none.
pub const DEFAULT_TOKEN_TTL_S: u64 = 3600;

/// The longest token a key mints when its policy sets no ceiling: a day.
pub const DEFAULT_TOKEN_TTL_MAX_S: u64 = 86_400;

/// The most grants a token may carry of its own.
pub const MAX_TOKEN_GRANTS: usize = 16;

/// The `typ` claim of a client token. A session token has none, so neither
/// decodes as the other.
const TOKEN_TYP: &str = "client";

// ---------------------------------------------------------------------------
// the policy and the principal
// ---------------------------------------------------------------------------

/// A scoped key's policy row, as the proxy uses it.
#[derive(Debug)]
pub struct KeyPolicy {
    pub grants: Vec<Grant>,
    pub limits: KeyLimitsDoc,
    /// The longest lifetime a token minted from the key may have, seconds.
    pub token_ttl_max_s: u64,
    /// The key used directly, with no token: no `sub`, so only its grants
    /// that name no entity. Built once per policy read.
    direct: Arc<ClientPrincipal>,
}

impl KeyPolicy {
    pub fn new(key_id: Uuid, doc: KeyPolicyDoc) -> KeyPolicy {
        let direct = ClientPrincipal::build(
            key_id,
            None,
            None,
            vec![layer(&doc.grants, None)],
            doc.limits.clone(),
        );
        KeyPolicy {
            grants: doc.grants,
            limits: doc.limits,
            token_ttl_max_s: doc
                .token_ttl_max_s
                .unwrap_or(DEFAULT_TOKEN_TTL_MAX_S)
                .min(data::TOKEN_TTL_CEILING_S),
            direct,
        }
    }

    /// The principal of the key used directly.
    pub fn direct(&self) -> Arc<ClientPrincipal> {
        self.direct.clone()
    }
}

/// A scoped caller, after authentication: what it is, and the scope that
/// travels to the broker with each of its requests.
#[derive(Debug)]
pub struct ClientPrincipal {
    pub key_id: Uuid,
    /// The token's entity; `None` for the key used directly.
    pub sub: Option<String>,
    /// The token's id, for the log lines.
    pub jti: Option<String>,
    pub scope: Arc<Scope>,
    /// `scope`, encoded once for the header.
    pub header: HeaderValue,
    pub limits: KeyLimitsDoc,
}

impl ClientPrincipal {
    fn build(
        key_id: Uuid,
        sub: Option<&str>,
        jti: Option<&str>,
        layers: Vec<Vec<ScopeGrant>>,
        limits: KeyLimitsDoc,
    ) -> Arc<ClientPrincipal> {
        let scope = Scope::new(layers);
        // JSON escapes every control character, and a header value may carry
        // the bytes of a UTF-8 name (obs-text): this never fails in practice,
        // and a scope that could not travel would be one that allows nothing.
        let header = HeaderValue::from_bytes(scope.encode().as_bytes())
            .unwrap_or_else(|_| HeaderValue::from_static(r#"{"v":1,"layers":[[]]}"#));
        Arc::new(ClientPrincipal {
            key_id,
            sub: sub.map(str::to_string),
            jti: jti.map(str::to_string),
            scope: Arc::new(scope),
            header,
            limits,
        })
    }

    /// `key 6f1…` or `key 6f1… sub dev-8f3a`, for log lines.
    pub fn who(&self) -> String {
        match &self.sub {
            Some(s) => format!("key {} sub {s}", self.key_id),
            None => format!("key {}", self.key_id),
        }
    }
}

/// The claims of a client token. Every field is required but `allow`: a
/// session token, which has neither `typ` nor `key`, never decodes as one.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientClaims {
    pub iss: String,
    pub typ: String,
    pub sub: String,
    pub key: Uuid,
    pub cluster: Uuid,
    pub iat: i64,
    pub exp: i64,
    pub jti: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow: Option<Vec<Grant>>,
}

impl ClientClaims {
    /// The shape a client token must have beyond its signature: its `typ`, a
    /// well-formed `sub`, well-formed grants of its own.
    pub fn check(&self) -> Result<(), &'static str> {
        if self.typ != TOKEN_TYP {
            return Err("typ");
        }
        if !queen_protocol::scope::valid_sub(&self.sub) {
            return Err("sub");
        }
        if let Some(a) = &self.allow {
            if a.len() > MAX_TOKEN_GRANTS || queen_protocol::scope::check_grants(a).is_err() {
                return Err("allow");
            }
        }
        if self.jti.trim().is_empty() {
            return Err("jti");
        }
        Ok(())
    }

    /// A new token's claims.
    pub fn new(
        issuer: &str,
        sub: &str,
        key: Uuid,
        cluster: Uuid,
        ttl_s: u64,
        allow: Option<Vec<Grant>>,
    ) -> ClientClaims {
        let now = now_secs();
        ClientClaims {
            iss: issuer.to_string(),
            typ: TOKEN_TYP.to_string(),
            sub: sub.to_string(),
            key,
            cluster,
            iat: now,
            exp: now + ttl_s as i64,
            jti: Uuid::new_v4().to_string(),
            allow,
        }
    }
}

// ---------------------------------------------------------------------------
// the token cache
// ---------------------------------------------------------------------------

/// A token verified once: its claims, and the principal last built from them
/// with the key policy it was built from. A key read again (invalidated,
/// narrowed) is a new `Arc`, and the next request rebuilds.
struct TokenEntry {
    claims: ClientClaims,
    built: Mutex<Option<(Arc<KeyPolicy>, Arc<ClientPrincipal>)>>,
}

impl TokenEntry {
    fn principal(&self, policy: &Arc<KeyPolicy>) -> Arc<ClientPrincipal> {
        let mut built = self.built.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((p, principal)) = built.as_ref() {
            if Arc::ptr_eq(p, policy) {
                return principal.clone();
            }
        }
        let c = &self.claims;
        let mut layers = vec![layer(&policy.grants, Some(&c.sub))];
        if let Some(allow) = &c.allow {
            layers.push(layer(allow, Some(&c.sub)));
        }
        let principal = ClientPrincipal::build(
            c.key,
            Some(&c.sub),
            Some(&c.jti),
            layers,
            policy.limits.clone(),
        );
        *built = Some((policy.clone(), principal.clone()));
        principal
    }
}

const TOKEN_SHARDS: usize = 16;
/// Verified tokens held per shard before the expired ones are swept; a shard
/// still full after the sweep is emptied (each entry costs one verify to
/// rebuild).
const TOKEN_SHARD_CAP: usize = 8_192;

/// Tokens by the sha256 of their text: a signature is verified once per
/// token per process (an Ed25519 verify is tens of microseconds; a cache hit
/// is a hash and a map lookup). A token that fails verification is not kept.
pub struct TokenCache {
    shards: Vec<Mutex<HashMap<[u8; 32], Arc<TokenEntry>>>>,
}

impl Default for TokenCache {
    fn default() -> Self {
        TokenCache {
            shards: (0..TOKEN_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
        }
    }
}

impl TokenCache {
    fn shard(&self, h: &[u8; 32]) -> &Mutex<HashMap<[u8; 32], Arc<TokenEntry>>> {
        &self.shards[h[0] as usize % TOKEN_SHARDS]
    }

    /// The verified claims of `token` (the JWT, prefix stripped), from the
    /// cache or from `verify`.
    fn get_or_verify<E>(
        &self,
        token: &str,
        verify: impl FnOnce(&str) -> Result<ClientClaims, E>,
    ) -> Result<Arc<TokenEntry>, E> {
        let h: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let now = now_secs();
        {
            let shard = self.shard(&h).lock().unwrap_or_else(|e| e.into_inner());
            if let Some(e) = shard.get(&h) {
                if e.claims.exp > now {
                    return Ok(e.clone());
                }
            }
        }
        let claims = verify(token)?;
        let entry = Arc::new(TokenEntry {
            claims,
            built: Mutex::new(None),
        });
        let mut shard = self.shard(&h).lock().unwrap_or_else(|e| e.into_inner());
        if shard.len() >= TOKEN_SHARD_CAP {
            shard.retain(|_, e| e.claims.exp > now);
            if shard.len() >= TOKEN_SHARD_CAP {
                shard.clear();
            }
        }
        shard.insert(h, entry.clone());
        Ok(entry)
    }
}

// ---------------------------------------------------------------------------
// the block list
// ---------------------------------------------------------------------------

/// How long a cluster's block list is believed without the invalidation feed
/// saying anything: the fallback when the feed is down.
const BLOCKS_TTL: Duration = Duration::from_secs(30);

/// A cluster's blocked entities: key id → subs.
type BlockSet = HashMap<Uuid, HashSet<String>>;

/// Every cluster's blocked entities, held in memory, read whole from the
/// store on first use and again after the invalidation feed names the
/// cluster (a block or an unblock marks it). A store that does not answer
/// keeps the last list it gave.
#[derive(Default)]
pub struct BlockList {
    sets: RwLock<HashMap<Uuid, (Arc<BlockSet>, Instant)>>,
    load: tokio::sync::Mutex<()>,
}

impl BlockList {
    fn fresh(&self, cluster: Uuid) -> Option<Arc<BlockSet>> {
        let sets = self.sets.read().unwrap_or_else(|e| e.into_inner());
        sets.get(&cluster)
            .filter(|(_, at)| at.elapsed() < BLOCKS_TTL)
            .map(|(s, _)| s.clone())
    }

    pub async fn is_blocked(&self, store: &Store, cluster: Uuid, key: Uuid, sub: &str) -> bool {
        let hit = |set: &BlockSet| set.get(&key).is_some_and(|subs| subs.contains(sub));
        if let Some(set) = self.fresh(cluster) {
            return hit(&set);
        }
        let _one = self.load.lock().await;
        if let Some(set) = self.fresh(cluster) {
            return hit(&set);
        }
        match data::list_blocks(store, cluster).await {
            Ok(rows) => {
                let mut set: BlockSet = HashMap::new();
                for b in rows {
                    set.entry(b.key_id).or_default().insert(b.sub);
                }
                let set = Arc::new(set);
                let blocked = hit(&set);
                self.sets
                    .write()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(cluster, (set, Instant::now()));
                blocked
            }
            Err(e) => {
                let stale = self
                    .sets
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&cluster)
                    .map(|(s, _)| s.clone());
                match stale {
                    Some(set) => hit(&set),
                    None => {
                        tracing::warn!(
                            target: "clientauth", cluster = %cluster, error = %e,
                            "block list unavailable and never read on this node: not enforced \
                             until the store answers"
                        );
                        false
                    }
                }
            }
        }
    }

    /// Forget a cluster's list: the next request reads it again.
    pub fn invalidate(&self, cluster: Uuid) {
        self.sets
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&cluster);
    }
}

// ---------------------------------------------------------------------------
// per-key and per-entity limits
// ---------------------------------------------------------------------------

struct Bucket {
    tokens: f64,
    at: Instant,
    rate: f64,
    burst: f64,
}

impl Bucket {
    fn refill(&mut self, now: Instant) {
        let dt = now.saturating_duration_since(self.at).as_secs_f64();
        self.tokens = (self.tokens + dt * self.rate).min(self.burst);
        self.at = now;
    }
}

const LIMIT_SHARDS: usize = 16;
/// Buckets per shard before the idle ones (full again) are swept.
const LIMIT_SHARD_CAP: usize = 16_384;

/// A scoped key's own limits (`KeyLimitsDoc`): one bucket per key, one per
/// (key, entity). Per node, like every other limit of this proxy. An idle
/// bucket is full, so dropping it loses nothing: that is how the map stays
/// bounded with a million entities.
pub struct ClientLimiter {
    shards: Vec<Mutex<HashMap<u64, Bucket>>>,
    hasher: std::collections::hash_map::RandomState,
}

impl Default for ClientLimiter {
    fn default() -> Self {
        ClientLimiter {
            shards: (0..LIMIT_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            hasher: std::collections::hash_map::RandomState::new(),
        }
    }
}

impl ClientLimiter {
    /// `Err(retry_after_s)` when the caller's entity or key is over its rate.
    /// The entity's bucket first: a device over its own rate costs its key
    /// nothing.
    pub fn check(&self, p: &ClientPrincipal) -> Result<(), u64> {
        let l = &p.limits;
        if let (Some(rps), Some(sub)) = (l.sub_rps, &p.sub) {
            let h = self.hasher.hash_one((1u8, p.key_id, sub.as_str()));
            self.take(h, rps, l.sub_burst.unwrap_or(rps))?;
        }
        if let Some(rps) = l.key_rps {
            let h = self.hasher.hash_one((0u8, p.key_id));
            self.take(h, rps, l.key_burst.unwrap_or(rps))?;
        }
        Ok(())
    }

    fn take(&self, h: u64, rps: u32, burst: u32) -> Result<(), u64> {
        let now = Instant::now();
        let rate = f64::from(rps.max(1));
        let burst = f64::from(burst.max(1));
        let mut shard = self.shards[(h % LIMIT_SHARDS as u64) as usize]
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if shard.len() >= LIMIT_SHARD_CAP && !shard.contains_key(&h) {
            shard.retain(|_, b| {
                b.refill(now);
                b.tokens < b.burst
            });
        }
        let b = shard.entry(h).or_insert(Bucket {
            tokens: burst,
            at: now,
            rate,
            burst,
        });
        // A key whose limits were edited takes its new numbers at once.
        b.rate = rate;
        b.burst = burst;
        b.refill(now);
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            Ok(())
        } else {
            Err(((1.0 - b.tokens) / rate).ceil().max(1.0) as u64)
        }
    }
}

// ---------------------------------------------------------------------------
// log lines
// ---------------------------------------------------------------------------

/// Lines per second this node writes about scoped clients before it starts
/// counting instead: a client stuck in a refused loop, or a backend minting
/// for a fleet at once, must not flood the log.
const LOG_LINES_PER_SEC: u32 = 20;

/// One line per refused call and per minted token, up to
/// [`LOG_LINES_PER_SEC`]; past it, one line per second saying how many were
/// not written.
#[derive(Default)]
pub struct ClientLog {
    window: Mutex<(Option<Instant>, u32, u64)>,
}

impl ClientLog {
    /// `Some(suppressed in the window before)` when a line may be written.
    fn due(&self) -> Option<u64> {
        let now = Instant::now();
        let mut w = self.window.lock().unwrap_or_else(|e| e.into_inner());
        let (start, lines, suppressed) = &mut *w;
        if start.is_none_or(|s| now.duration_since(s) >= Duration::from_secs(1)) {
            let before = std::mem::take(suppressed);
            *start = Some(now);
            *lines = 1;
            return Some(before);
        }
        if *lines < LOG_LINES_PER_SEC {
            *lines += 1;
            return Some(0);
        }
        *suppressed += 1;
        None
    }

    /// A scoped client's call refused, by this proxy or by the broker.
    pub fn refused(&self, cluster: &str, who: &str, method: &str, path: &str, why: &str) {
        if let Some(suppressed) = self.due() {
            tracing::info!(
                target: "clientauth", cluster, client = who, method, path, why, suppressed,
                "scoped client call refused"
            );
        }
    }

    /// A client token minted.
    pub fn minted(&self, cluster: &str, key: Uuid, sub: &str, jti: &str, exp: i64) {
        if let Some(suppressed) = self.due() {
            tracing::info!(
                target: "clientauth", cluster, key = %key, sub, jti, exp, suppressed,
                "client token minted"
            );
        }
    }
}

/// The client state every node keeps: verified tokens, the block lists, the
/// buckets, the log sampler.
#[derive(Default)]
pub struct Clients {
    pub tokens: TokenCache,
    pub blocks: BlockList,
    pub limiter: ClientLimiter,
    pub log: ClientLog,
}

// ---------------------------------------------------------------------------
// authentication
// ---------------------------------------------------------------------------

/// The refusal of a token that is past its `exp`: its own code, so a client
/// knows to fetch a new token rather than give up.
pub fn err_expired() -> Response {
    errors::json_error(
        StatusCode::UNAUTHORIZED,
        "token_expired",
        "client token expired",
    )
}

/// The refusal of a blocked entity.
pub fn err_blocked() -> Response {
    errors::err_403("client_blocked", "this client is blocked")
}

/// Authenticate a client token (its text after [`TOKEN_PREFIX`]): its
/// signature (once per token), its expiry, its cluster (`want`, when the
/// request already named one), its key (live, scoped, of that cluster), its
/// entity (not blocked). Answers the key's cluster and the principal.
pub async fn authenticate_token(
    st: &St,
    jwt: &str,
    want: Option<Uuid>,
) -> Result<(Arc<ClusterCtx>, Principal), Response> {
    let entry = st
        .clients
        .tokens
        .get_or_verify(jwt, |t| st.keys.verify_client_token(t))
        .map_err(|reason| {
            tracing::debug!(target: "auth", reason = %reason.log_reason(), "client token rejected");
            match reason {
                crate::auth::JwtReject::Expired => err_expired(),
                _ => errors::err_401("invalid client token"),
            }
        })?;
    let c = &entry.claims;
    // Exact, where the signature check allows a minute of clock skew: a token
    // stops at its `exp` on every node, cached or not.
    if c.exp <= now_secs() {
        return Err(err_expired());
    }
    if want.is_some_and(|w| w != c.cluster) {
        return Err(errors::err_403(
            errors::CODE_FORBIDDEN,
            "token not valid for this cluster",
        ));
    }
    let Some(key) = st.cache.by_key_id(c.key).await else {
        return Err(errors::err_401(
            "the key this token belongs to is revoked or unknown",
        ));
    };
    let Some(policy) = key
        .policy
        .as_ref()
        .filter(|_| key.ctx.cluster_id == c.cluster)
    else {
        return Err(errors::err_401(
            "the key this token belongs to is not a scoped key of this cluster",
        ));
    };
    if st
        .clients
        .blocks
        .is_blocked(&st.store, c.cluster, c.key, &c.sub)
        .await
    {
        st.clients.log.refused(
            &key.ctx.slug,
            &format!("key {} sub {}", c.key, c.sub),
            "-",
            "-",
            "client blocked",
        );
        return Err(err_blocked());
    }
    let principal = entry.principal(policy);
    Ok((key.ctx.clone(), Principal::Client(principal)))
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grants(v: serde_json::Value) -> Vec<Grant> {
        serde_json::from_value(v).unwrap()
    }

    fn principal(limits: KeyLimitsDoc, sub: Option<&str>) -> ClientPrincipal {
        let p = ClientPrincipal::build(Uuid::from_u128(7), sub, None, vec![Vec::new()], limits);
        Arc::try_unwrap(p).unwrap()
    }

    #[test]
    fn a_policy_used_directly_keeps_only_the_grants_that_name_no_entity() {
        let doc = KeyPolicyDoc {
            key_id: Uuid::from_u128(1),
            grants: grants(serde_json::json!([
                {"queue": "telemetry", "partition": "{sub}", "ops": ["produce"]},
                {"queue": "audit", "ops": ["produce"]}
            ])),
            limits: KeyLimitsDoc::default(),
            token_ttl_max_s: None,
            updated_at_us: 0,
        };
        let p = KeyPolicy::new(doc.key_id, doc);
        assert_eq!(p.token_ttl_max_s, DEFAULT_TOKEN_TTL_MAX_S);
        let d = p.direct();
        assert_eq!(d.scope.layers[0].len(), 1);
        let decoded = Scope::decode(d.header.as_bytes()).unwrap();
        assert_eq!(&decoded, d.scope.as_ref(), "the header is the scope");
    }

    #[test]
    fn a_token_principal_adds_its_own_grants_as_a_second_layer_and_is_rebuilt_per_policy() {
        let doc = |g: serde_json::Value| KeyPolicyDoc {
            key_id: Uuid::from_u128(1),
            grants: grants(g),
            limits: KeyLimitsDoc::default(),
            token_ttl_max_s: Some(600),
            updated_at_us: 0,
        };
        let wide = Arc::new(KeyPolicy::new(
            Uuid::from_u128(1),
            doc(serde_json::json!([{"queue": "t", "partition": "{sub}/*", "ops": ["produce"]}])),
        ));
        let entry = TokenEntry {
            claims: ClientClaims::new(
                "iss",
                "dev1",
                Uuid::from_u128(1),
                Uuid::from_u128(2),
                60,
                Some(grants(
                    serde_json::json!([{"queue": "t", "partition": "{sub}/gps", "ops": ["produce"]}]),
                )),
            ),
            built: Mutex::new(None),
        };
        let p = entry.principal(&wide);
        assert_eq!(p.scope.layers.len(), 2);
        let ask = |part| queen_protocol::scope::Ask::Produce {
            queue: "t",
            partition: part,
        };
        assert!(p.scope.allows(&ask("dev1/gps")));
        assert!(!p.scope.allows(&ask("dev1/temp")), "the token narrows");
        assert!(
            Arc::ptr_eq(&p, &entry.principal(&wide)),
            "built once per policy"
        );
        let narrowed = Arc::new(KeyPolicy::new(
            Uuid::from_u128(1),
            doc(serde_json::json!([{"queue": "t", "partition": "{sub}/temp", "ops": ["produce"]}])),
        ));
        let q = entry.principal(&narrowed);
        assert!(!Arc::ptr_eq(&p, &q), "a policy read again rebuilds");
        assert!(
            !q.scope.allows(&ask("dev1/gps")),
            "the key's narrowing reaches the token"
        );
    }

    #[test]
    fn a_token_that_is_not_a_client_token_does_not_check() {
        let mut c = ClientClaims::new("i", "dev1", Uuid::nil(), Uuid::nil(), 60, None);
        assert!(c.check().is_ok());
        c.typ = "session".into();
        assert_eq!(c.check(), Err("typ"));
        let mut c = ClientClaims::new("i", "dev/1", Uuid::nil(), Uuid::nil(), 60, None);
        assert_eq!(c.check(), Err("sub"));
        c.sub = "dev1".into();
        c.allow = Some(grants(
            serde_json::json!([{"queue": "q", "partition": "{sub}*", "ops": ["produce"]}]),
        ));
        assert_eq!(c.check(), Err("allow"));
        // A session's claims are not a client token's.
        let session = serde_json::json!({
            "sub": Uuid::nil(), "iss": "i", "exp": 1, "jti": "j", "role": "admin"
        });
        assert!(serde_json::from_value::<ClientClaims>(session).is_err());
    }

    #[test]
    fn the_token_cache_verifies_once_and_drops_expired_tokens() {
        let cache = TokenCache::default();
        let mut calls = 0;
        let claims = ClientClaims::new("i", "dev1", Uuid::nil(), Uuid::nil(), 60, None);
        for _ in 0..3 {
            let e = cache.get_or_verify("tok", |_| -> Result<_, ()> {
                calls += 1;
                Ok(claims.clone())
            });
            assert!(e.is_ok());
        }
        assert_eq!(calls, 1, "verified once");
        assert!(cache.get_or_verify("bad", |_| Err(())).is_err());
        assert!(
            cache.get_or_verify("bad", |_| Err(())).is_err(),
            "a failure is not kept"
        );
        let mut expired = claims.clone();
        expired.exp = now_secs() - 1;
        let mut calls = 0;
        for _ in 0..2 {
            let _ = cache.get_or_verify("old", |_| -> Result<_, ()> {
                calls += 1;
                Ok(expired.clone())
            });
        }
        assert_eq!(
            calls, 2,
            "an expired entry is verified again, and refused by its caller"
        );
    }

    #[test]
    fn the_limiter_holds_an_entity_and_its_key_to_their_rates() {
        let l = ClientLimiter::default();
        let limits = KeyLimitsDoc {
            key_rps: Some(1000),
            key_burst: None,
            sub_rps: Some(2),
            sub_burst: Some(2),
        };
        let dev1 = principal(limits.clone(), Some("dev1"));
        let dev2 = principal(limits.clone(), Some("dev2"));
        assert!(l.check(&dev1).is_ok());
        assert!(l.check(&dev1).is_ok());
        let retry = l.check(&dev1).expect_err("the third within a second");
        assert!(retry >= 1);
        assert!(l.check(&dev2).is_ok(), "another entity has its own bucket");
        let key_only = KeyLimitsDoc {
            key_rps: Some(3),
            key_burst: Some(3),
            sub_rps: None,
            sub_burst: None,
        };
        let a = principal(key_only.clone(), Some("a"));
        let b = principal(key_only, Some("b"));
        assert!(l.check(&a).is_ok());
        assert!(l.check(&b).is_ok());
        assert!(l.check(&a).is_ok());
        assert!(
            l.check(&b).is_err(),
            "the key's bucket is shared by its entities"
        );
        let free = principal(KeyLimitsDoc::default(), Some("free"));
        for _ in 0..10_000 {
            assert!(l.check(&free).is_ok(), "no limit set: no limit");
        }
    }

    #[test]
    fn the_log_writes_a_line_per_event_up_to_its_rate() {
        let log = ClientLog::default();
        let written = (0..100).filter(|_| log.due().is_some()).count();
        assert_eq!(written, LOG_LINES_PER_SEC as usize);
    }
}
