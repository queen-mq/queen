//! Which TENANT a credential speaks for, and the cache that asks Queen once.
//!
//! ## The seam this closes
//!
//! Two of this facade's long-lived maps are filed per credential: the
//! queue-list cache ([`crate::queen::Catalog`]) and the consumer-group registry
//! ([`crate::coordinator`]). Queen files the thing the second of those is ABOUT
//! — the committed offsets a group resumes from — per TENANT. The broker takes
//! the tenant from the trusted `x-queen-tenant` header the proxy stamps from
//! the cluster the credential named (proxy/src/gateway.rs, server/src/tenant.rs)
//! and the KV keys this facade composes carry no credential at all
//! ([`crate::offsets::key`]).
//!
//! Keyed by the credential, the two scopes agree for as long as a tenant has
//! ONE credential and disagree the moment it has two — a key rotation, a
//! per-service key. Two consumers naming one group id then become the sole
//! member of two groups, each assigned everything by its own leader, both
//! writing the same offset keys with a generation the other cannot see: every
//! record read twice, and progress overwritten in both directions.
//!
//! So the scope is asked for rather than assumed: one call to `GET /auth/me`
//! per credential, at authentication time, and the answer keys both maps.
//!
//! ## Why the ACTING CLUSTER and not the tenant slug
//!
//! `/auth/me` names several things that read like a tenant, and only one of
//! them is the right grain. A Queen Cloud tenant may own SEVERAL clusters, and
//! every cluster has its own `broker_tenant_uuid` (proxy/src/cache.rs) — which
//! is the tenant the broker actually scopes KV by. Keying by `tenant_slug` or
//! by a cluster row's `tenant_id` would therefore be COARSER than the offset
//! namespace: two consumers of one group id on two clusters of one tenant would
//! be merged into a single coordinator while committing into two separate
//! offset namespaces, which is a new bug of exactly the shape as the old one.
//!
//! `acting_cluster` is the cluster THIS request resolves to, decided by the
//! same code the data plane uses (proxy/src/acting.rs `peek_ctx`), and one
//! cluster is one broker tenant is one offset namespace. It is never coarser
//! than what it keys, which is the property that makes it safe.
//!
//! ## What the two surfaces answer TODAY
//!
//! Stated plainly, because it decides what this module actually does in each
//! deployment:
//!
//!   * **broker with `JWT_ENABLED=false`** — `/auth/me` is a Public route
//!     (server/src/auth.rs `route_access_level`) and answers the standalone
//!     identity, `acting_cluster: {"id":"local"}`, to any caller
//!     (server/src/handlers/standalone.rs). Every credential on that broker
//!     resolves to `local`, and that is not a merge of things that should be
//!     apart: such a broker has one tenant (`config::DEFAULT_TENANT`) and no
//!     auth at all, so two credentials against it are already the same
//!     principal reading the same queues and the same offsets. This is the OSS
//!     single-tenant case, and it is the one that resolves today.
//!   * **broker with `JWT_ENABLED=true`** — `/auth/me` answers 401
//!     `auth_required` to everyone, bearer or not: it describes a dashboard
//!     SESSION, and the broker holds none. No identity; the credential stands
//!     for itself.
//!   * **the proxy** — `/auth/me` reads the session COOKIE and nothing else
//!     (proxy/src/oauth.rs `me`: "Cookie only"), so a bearer is answered 401
//!     `no session`. No identity; the credential stands for itself.
//!
//! Which means the honest summary: **in Cloud, the deployment the seam was
//! named for, this resolves nothing today** and the facade keeps its
//! pre-existing per-credential behaviour, with the warning in
//! [`crate::coordinator`] still the diagnostic for the configuration that
//! produces duplicate consumption. The machinery is here, it is exercised by
//! the OSS shape above, and it lights up the day the proxy's identity surface
//! answers a bearer — no other part of the facade has to change for that.
//!
//! ## The fallback is per credential, never a shared bucket
//!
//! Every failure — unreachable, refused, an answer that names no cluster —
//! degrades to [`TenantKey::Credential`], the same hashed credential the maps
//! were keyed by before this module existed. It never degrades to a common
//! "unknown" bucket: that would put every unresolved credential on the listener
//! into one group namespace and one queue-list cache, which is the cross-tenant
//! collision this facade already refuses to have (`coordinator::GroupKey`).
//!
//! ## An answer, once resolved, does not change
//!
//! A credential's key is settled by the FIRST answer and then stands for the
//! life of the cache entry — including when the answer was "no identity here".
//! Re-asking later and getting a different key is not a refinement: a group
//! whose members joined under one key and whose next member arrives under
//! another is the split this module exists to prevent, produced by the fix
//! rather than by the configuration. The one exception is a resolution that
//! never got an ANSWER (unreachable, 5xx): there is nothing to be stable about
//! yet, so it is retried after [`RETRY_UNREACHABLE`].

use std::collections::HashMap;
use std::sync::atomic;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::queen::{self, QueenApi};
use crate::secret::CredentialKey;

/// The identity route, on the broker and on the proxy alike.
pub const IDENTITY_PATH: &str = "/auth/me";

/// The scope one credential's groups and queue list are filed under.
///
/// `Clone` and not `Copy`: the resolved half owns a small string. It is an
/// `Arc<str>` so that cloning it — which happens on every registry lookup — is
/// a refcount rather than an allocation.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum TenantKey {
    /// Queen named the cluster this credential acts on. Two credentials that
    /// name the same one are ONE tenant and share everything filed under it.
    Tenant(Arc<str>),
    /// Queen named none, so the credential stands for itself — the behaviour
    /// this facade had before identities were asked for at all.
    Credential(CredentialKey),
}

/// Says what the key IS and never what the credential is. The tenant half is an
/// identifier Queen chose and returned (a cluster uuid, or `local`), not a
/// secret, and it is bounded and character-checked by [`usable_id`] before it
/// ever reaches this type — so a log line carrying it can neither leak a token
/// nor forge a line.
impl std::fmt::Debug for TenantKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TenantKey::Tenant(id) => write!(f, "TenantKey(tenant={id})"),
            TenantKey::Credential(c) => write!(f, "TenantKey({c:?})"),
        }
    }
}

impl TenantKey {
    /// The unresolved key for `token`: the credential, hashed.
    pub fn of_credential(token: Option<&str>) -> TenantKey {
        TenantKey::Credential(CredentialKey::of(token))
    }

    /// The key of a connection that presented no credential at all — a
    /// listener with no SASL, where every connection reaches Queen as the
    /// process does.
    pub fn anonymous() -> TenantKey {
        TenantKey::of_credential(None)
    }

    /// Whether this is that key. For the one caller that treats an
    /// unauthenticated listener differently from an authenticated one.
    pub fn is_anonymous(&self) -> bool {
        matches!(self, TenantKey::Credential(c) if c.is_anonymous())
    }

    /// Whether Queen named the tenant, as opposed to this being the fallback.
    /// The distinction is what makes the coordinator's warning precise: two
    /// NAMED tenants sharing a group id are two tenants, which is fine; a
    /// fallback sharing one with anything is a question nobody can answer.
    pub fn is_named(&self) -> bool {
        matches!(self, TenantKey::Tenant(_))
    }

    /// The tenant Queen named, for a log line. `None` for the fallback.
    pub fn named(&self) -> Option<&str> {
        match self {
            TenantKey::Tenant(id) => Some(id),
            TenantKey::Credential(_) => None,
        }
    }
}

/// Ceiling on a tenant identifier, in characters. A cluster uuid is 36 and the
/// standalone broker's is `local`; anything longer is not one, and every copy
/// of it would live in a process-wide map key and in log lines.
const MAX_TENANT_ID_CHARS: usize = 128;

/// The identifier in a `/auth/me` answer, if it is one this facade may hold.
///
/// The value arrives over HTTP and ends up in log lines and in map keys, so it
/// is validated rather than trusted: bounded, and restricted to the characters
/// a cluster uuid, a slug or the standalone `local` are spelled with. That
/// makes a forged log line (a CR, an LF) impossible by construction rather than
/// by the formatter's promise, and it is why the [`TenantKey`] `Debug` above
/// can print it.
fn usable_id(raw: &str) -> Option<Arc<str>> {
    let id = raw.trim();
    if id.is_empty() || id.chars().count() > MAX_TENANT_ID_CHARS {
        return None;
    }
    id.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':'))
        .then(|| Arc::from(id))
}

/// The tenant a `/auth/me` body names, or `None` when it names none.
///
/// `acting_cluster.id` and nothing else — see the module header for why that
/// field and not `tenant_slug`. A body of any other shape (an ingress error
/// page, a future field rename, a 200 from something that is not Queen) names
/// none, which is the fallback and not an error: this is a refinement of the
/// key, so "cannot tell" has a correct answer.
pub fn tenant_of(body: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(body).ok()?;
    parsed
        .get("acting_cluster")?
        .get("id")?
        .as_str()
        .map(str::to_string)
}

/// How long a resolution that got no ANSWER is left before it is asked again.
/// Short, because nothing has been settled yet; see the module header.
const RETRY_UNREACHABLE: Duration = Duration::from_secs(30);

/// How many credentials one cache keeps an identity for. The same bound and
/// the same reasoning as [`crate::queen::Catalog`]'s: the key space is
/// "credentials this facade has been shown", which on a SASL listener is a
/// number a peer chooses.
const MAX_CREDENTIALS: usize = 1_024;

/// One line per window when Queen cannot be asked. A listener whose Queen is
/// unreachable produces this per connection, so it is sampled like every other
/// per-connection line ([`crate::obs`]).
static UNREACHABLE: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// Whether the "this Queen does not identify bearers" line has been said. Once
/// per process, not per window: it describes the deployment, and it is the
/// answer an operator gets for every credential from then on.
static SAID_UNIDENTIFIED: atomic::AtomicBool = atomic::AtomicBool::new(false);

/// What one credential resolved to, and whether that is final.
struct Known {
    key: TenantKey,
    /// `Some(at)` when the key is a fallback that was never actually answered
    /// and may be asked again from `at`. `None` is settled: either Queen named
    /// a tenant, or it answered that it names none.
    retry_at: Option<Instant>,
    /// When this entry was last read. What eviction orders by, for the same
    /// reason the catalog's does: a credential in steady use is answered from
    /// the cache and its resolution time sits still while it works.
    used_at: Instant,
}

/// The identities, asked once per credential and kept.
///
/// One per [`crate::queen::Catalog`], which means one per SNI LANE rather than
/// one per process, and that is right rather than incidental: the lane is the
/// `Host` a connection's calls carry, and on a per-cluster hostname the Host is
/// what decides which cluster `/auth/me` reports (proxy/src/acting.rs). One
/// credential dialling two hostnames is therefore allowed to resolve to two
/// tenants, because on those two hostnames it addresses two clusters — two
/// offset namespaces, and correctly two coordinators.
pub struct Identities {
    api: Arc<dyn QueenApi>,
    retry: Duration,
    /// A `std::sync::Mutex` and never held across an await, which is what lets
    /// [`Identities::known`] be synchronous — and it has to be, because the one
    /// caller that needs a key without being able to wait for one is
    /// [`crate::Facade::authenticated_as`], on the connection path.
    entries: Mutex<HashMap<CredentialKey, Known>>,
    /// One resolve lock per credential. Held across the call to Queen, so a
    /// fleet authenticating at once makes ONE `/auth/me` call per credential
    /// rather than one per connection — and, more importantly, every one of
    /// those connections gets the SAME key rather than racing to a different
    /// one.
    resolving: tokio::sync::Mutex<HashMap<CredentialKey, Arc<tokio::sync::Mutex<()>>>>,
}

impl Identities {
    pub fn new(api: Arc<dyn QueenApi>) -> Identities {
        Identities::with_retry(api, RETRY_UNREACHABLE)
    }

    /// Same, with an explicit retry window. For the tests, which cannot wait
    /// thirty seconds to prove that an unreachable Queen is asked again.
    pub fn with_retry(api: Arc<dyn QueenApi>, retry: Duration) -> Identities {
        Identities {
            api,
            retry,
            entries: Mutex::new(HashMap::new()),
            resolving: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// The key for `token`, asking Queen if this credential has never been
    /// asked about. Never fails: every failure is a [`TenantKey::Credential`].
    pub async fn resolve(&self, token: Option<&str>) -> TenantKey {
        let cred = CredentialKey::of(token);
        if let Some(key) = self.settled(cred) {
            return key;
        }
        let gate = self.gate(cred).await;
        // Waited on rather than skipped: a caller here has no key at all, and
        // the answer in flight is the one it must share.
        let _held = gate.lock().await;
        // The call we queued behind may have settled it.
        if let Some(key) = self.settled(cred) {
            return key;
        }
        self.ask(cred, token).await
    }

    /// The key for `token` as it is ALREADY known, without asking Queen.
    ///
    /// A credential that was never resolved answers with itself, which is both
    /// the honest answer and the pre-existing behaviour. The callers are the
    /// hot paths — every Metadata, every Produce, every Fetch reads the queue
    /// list — plus the connection path, which cannot await.
    pub fn known(&self, token: Option<&str>) -> TenantKey {
        let cred = CredentialKey::of(token);
        self.settled(cred).unwrap_or(TenantKey::Credential(cred))
    }

    /// The key for `cred` if it is settled, or if its retry window is still
    /// open. Touches the entry, because this is what "used" means here.
    fn settled(&self, cred: CredentialKey) -> Option<TenantKey> {
        let mut entries = self
            .entries
            .lock()
            .expect("the identity map lock is never held across a panic");
        let entry = entries.get_mut(&cred)?;
        if entry.retry_at.is_some_and(|at| Instant::now() >= at) {
            return None;
        }
        entry.used_at = Instant::now();
        Some(entry.key.clone())
    }

    /// The resolve lock for one credential, created on first use and swept the
    /// way the catalog sweeps its own: an entry nobody holds is a lock nobody
    /// waits on, so dropping it is free, while a held one stays because two
    /// mutexes for one credential are two calls where the point is one.
    async fn gate(&self, cred: CredentialKey) -> Arc<tokio::sync::Mutex<()>> {
        let mut resolving = self.resolving.lock().await;
        if resolving.len() >= MAX_CREDENTIALS && !resolving.contains_key(&cred) {
            resolving.retain(|_, lock| Arc::strong_count(lock) > 1);
        }
        Arc::clone(resolving.entry(cred).or_default())
    }

    /// Ask Queen, classify the answer, and remember it. The caller holds this
    /// credential's resolve lock; the map lock is taken once, after the call.
    async fn ask(&self, cred: CredentialKey, token: Option<&str>) -> TenantKey {
        let (key, retry_at) = match self.api.identity(token).await {
            // Queen named a cluster. Everything filed under it is shared by
            // every credential that names the same one.
            Ok(Some(raw)) => match usable_id(&raw) {
                Some(id) => {
                    tracing::debug!(
                        target: "kafka",
                        tenant = %id,
                        "this credential's tenant was resolved from {IDENTITY_PATH}"
                    );
                    (TenantKey::Tenant(id), None)
                }
                // An identifier this facade will not hold. Not a failure to
                // retry: the same answer would come back.
                None => {
                    tracing::debug!(
                        target: "kafka",
                        "{IDENTITY_PATH} named an identifier this facade will not use as a key; \
                         this credential stands for itself"
                    );
                    (TenantKey::Credential(cred), None)
                }
            },
            // Queen answered and named no cluster: the standalone-with-auth
            // shape, and anything else that is not an identity payload. Settled
            // — see the module header on why a key does not change later.
            Ok(None) => (TenantKey::Credential(cred), None),
            // The identity surface refused, which every deployment's does today
            // for a bearer (module header). Settled, and said ONCE for the life
            // of the process: it is a static property of the Queen this facade
            // is pointed at, not an event — a line per credential, or per
            // window, would be a healthy deployment's noisiest log.
            Err(queen::Error::Status {
                code: code @ (401 | 403 | 404 | 405),
                ..
            }) => {
                if !SAID_UNIDENTIFIED.swap(true, atomic::Ordering::Relaxed) {
                    tracing::info!(
                        target: "kafka",
                        status = code,
                        "this Queen does not identify bearer credentials at {IDENTITY_PATH}, so \
                         each credential's consumer groups are scoped to the credential itself. \
                         That is correct for one credential per tenant; two credentials of ONE \
                         tenant are two groups sharing one set of committed offsets, which the \
                         coordinator warns about if it happens"
                    );
                }
                (TenantKey::Credential(cred), None)
            }
            // No answer at all. Nothing is settled, so it is asked again later.
            Err(e) => {
                if let Some(suppressed) = UNREACHABLE.tick_now() {
                    tracing::warn!(
                        target: "kafka",
                        error = %e,
                        suppressed,
                        retry_ms = self.retry.as_millis() as u64,
                        "{IDENTITY_PATH} could not be reached; this credential's consumer groups \
                         and queue list are filed under the credential itself until it can be. \
                         Two credentials of ONE tenant are then two groups sharing one set of \
                         committed offsets — see the coordinator's own warning"
                    );
                }
                (
                    TenantKey::Credential(cred),
                    Some(Instant::now() + self.retry),
                )
            }
        };
        let now = Instant::now();
        let mut entries = self
            .entries
            .lock()
            .expect("the identity map lock is never held across a panic");
        make_room(&mut entries, cred);
        entries.insert(
            cred,
            Known {
                key: key.clone(),
                retry_at,
                used_at: now,
            },
        );
        key
    }

    /// How many credentials have an identity. For the tests that prove the
    /// bound.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

/// Drop the coldest entry if `cred` would take the map past its bound. Least
/// recently READ, for the reason [`crate::queen::Catalog`]'s twin explains.
fn make_room(entries: &mut HashMap<CredentialKey, Known>, cred: CredentialKey) {
    if entries.len() < MAX_CREDENTIALS || entries.contains_key(&cred) {
        return;
    }
    if let Some(coldest) = entries
        .iter()
        .min_by_key(|(_, e)| e.used_at)
        .map(|(k, _)| *k)
    {
        entries.remove(&coldest);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queen::testing::FakeQueen;

    /// The double, as the cache takes it. One place for the coercion, so a
    /// test reads as what it is asking rather than as a cast.
    fn over(api: &Arc<FakeQueen>) -> Arc<dyn QueenApi> {
        Arc::clone(api) as Arc<dyn QueenApi>
    }

    /// The broker's standalone answer, field for field
    /// (server/src/handlers/standalone.rs).
    const STANDALONE: &str = r#"{
        "user_id": "standalone", "email": null, "tenant_slug": "local",
        "is_operator": true, "operator_enabled": true, "operator_live": true,
        "acting_cluster": {"id": "local", "slug": "local"},
        "clusters": [{"id": "local", "slug": "local", "role": "admin",
                      "tenant_slug": "local",
                      "tenant_id": "00000000-0000-0000-0000-000000000001",
                      "status": "active", "cell_slug": "dev"}],
        "act_cluster_header": "x-queen-act-cluster", "role": "admin",
        "cluster": null, "standalone": true
    }"#;

    /// The proxy's, for a session that has settled on a cluster
    /// (proxy/src/oauth.rs `me`).
    const PROXY: &str = r#"{
        "user_id": "6f1e…", "email": "a@example.test", "tenant_slug": "acme",
        "is_operator": false, "operator_enabled": true, "operator_live": false,
        "acting_cluster": {"id": "3a2b1c4d-0000-4000-8000-000000000001", "slug": "prod"},
        "clusters": [{"id": "3a2b1c4d-0000-4000-8000-000000000001", "slug": "prod",
                      "role": "admin", "tenant_slug": "acme",
                      "tenant_id": "9f9f9f9f-0000-4000-8000-00000000000a",
                      "status": "active", "cell_slug": "eu-1"}],
        "act_cluster_header": "x-queen-act-cluster", "role": "user",
        "cluster": null
    }"#;

    #[test]
    fn the_tenant_is_the_acting_cluster_of_either_surface() {
        assert_eq!(tenant_of(STANDALONE).as_deref(), Some("local"));
        assert_eq!(
            tenant_of(PROXY).as_deref(),
            Some("3a2b1c4d-0000-4000-8000-000000000001")
        );
    }

    /// The tenant a body names is the CLUSTER's id and never the tenant's own,
    /// because one Cloud tenant may own several clusters and each has its own
    /// broker tenant — the thing offsets are actually scoped by. Merging by
    /// `tenant_id` would key a group more coarsely than the offsets it commits.
    #[test]
    fn the_tenant_is_not_the_tenant_id_of_the_cluster_row() {
        let named = tenant_of(PROXY).unwrap();
        assert_ne!(named, "9f9f9f9f-0000-4000-8000-00000000000a");
        assert_ne!(named, "acme");
    }

    /// A session that has not settled on a cluster names no tenant — the
    /// shared-host first load — and so does anything that is not this payload.
    #[test]
    fn an_answer_that_names_no_cluster_names_no_tenant() {
        for body in [
            r#"{"acting_cluster": null, "clusters": []}"#,
            r#"{"tenant_slug": "acme"}"#,
            r#"{"acting_cluster": {"slug": "prod"}}"#,
            r#"{"acting_cluster": {"id": 7}}"#,
            "<!doctype html><title>502</title>",
            "",
        ] {
            assert_eq!(tenant_of(body), None, "{body}");
        }
    }

    /// The value goes in a map key and in log lines, so it is bounded and
    /// character-checked rather than trusted.
    #[test]
    fn an_identifier_that_could_forge_a_log_line_is_not_a_key() {
        assert!(usable_id("local").is_some());
        assert!(usable_id("3a2b1c4d-0000-4000-8000-000000000001").is_some());
        assert!(usable_id("  prod.eu-1  ").is_some(), "trimmed, not refused");
        for hostile in [
            "acme\r\nlevel=INFO msg=\"nothing happened\"",
            "acme\ttenant=other",
            "acme tenant",
            "",
            "   ",
            &"c".repeat(MAX_TENANT_ID_CHARS + 1),
        ] {
            assert!(usable_id(hostile).is_none(), "{hostile:?} was accepted");
        }
    }

    /// THE property the type exists for: a key printed anywhere cannot print
    /// the credential it stands for.
    #[test]
    fn printing_a_key_never_prints_the_credential() {
        const SECRET: &str = "s3cr3t-tenant-token";
        let printed = format!("{:?}", TenantKey::of_credential(Some(SECRET)));
        assert!(!printed.contains(SECRET), "{printed}");
        assert!(!printed.contains("s3cr3t"), "{printed}");
        assert_eq!(
            format!("{:?}", TenantKey::Tenant(Arc::from("local"))),
            "TenantKey(tenant=local)"
        );
    }

    /// Two credentials that Queen says are one tenant are ONE key; two that it
    /// says are two tenants are two; and a credential it says nothing about
    /// stands for itself rather than joining a shared "unknown" bucket.
    #[tokio::test]
    async fn one_tenant_is_one_key_and_a_failure_is_never_a_shared_one() {
        let api = FakeQueen::with(&[]);
        api.answer_identity(Some("key-a"), "cluster-1");
        api.answer_identity(Some("key-b"), "cluster-1");
        api.answer_identity(Some("key-c"), "cluster-2");
        let ids = Identities::new(over(&api));

        let a = ids.resolve(Some("key-a")).await;
        let b = ids.resolve(Some("key-b")).await;
        let c = ids.resolve(Some("key-c")).await;
        assert_eq!(a, b, "one tenant's two keys are two scopes");
        assert_ne!(a, c);
        assert!(a.is_named());

        // Two credentials nobody could resolve are two keys, not one.
        let x = ids.resolve(Some("unknown-x")).await;
        let y = ids.resolve(Some("unknown-y")).await;
        assert_ne!(x, y, "unresolved credentials were pooled");
        assert!(!x.is_named());
        assert_eq!(x, TenantKey::of_credential(Some("unknown-x")));
    }

    /// One call per credential, and the answer is what every later caller gets
    /// without asking again.
    #[tokio::test]
    async fn a_credential_is_asked_about_once() {
        let api = FakeQueen::with(&[]);
        api.answer_identity(Some("key-a"), "cluster-1");
        let ids = Identities::new(over(&api));

        for _ in 0..5 {
            assert!(ids.resolve(Some("key-a")).await.is_named());
        }
        assert_eq!(api.identity_calls(), [Some("key-a".to_string())]);
        // ...and the synchronous read sees the same answer, still without a
        // call: that is the one the connection path takes.
        assert_eq!(
            ids.known(Some("key-a")),
            TenantKey::Tenant(Arc::from("cluster-1"))
        );
        assert_eq!(api.identity_calls().len(), 1);
    }

    /// A credential nobody has resolved is not invented into the map by being
    /// read: `known` answers with the credential and leaves nothing behind, so
    /// the hot paths cannot fill a process-wide map.
    #[tokio::test]
    async fn reading_an_unresolved_credential_stores_nothing() {
        let api = FakeQueen::with(&[]);
        let ids = Identities::new(over(&api));
        assert_eq!(
            ids.known(Some("never-seen")),
            TenantKey::of_credential(Some("never-seen"))
        );
        assert_eq!(ids.len(), 0);
        assert!(api.identity_calls().is_empty());
    }

    /// A refusal is the answer every surface gives a bearer today, so it is
    /// settled rather than retried: no call per connection for ever.
    #[tokio::test]
    async fn a_refusal_is_settled_and_not_asked_again() {
        for refusal in [401, 403, 404, 405] {
            let api = FakeQueen::with(&[]);
            api.fail_identity(Some("key-a"), queen::Error::status(refusal, "no session"));
            let ids = Identities::new(over(&api));

            assert_eq!(
                ids.resolve(Some("key-a")).await,
                TenantKey::of_credential(Some("key-a")),
                "HTTP {refusal}"
            );
            ids.resolve(Some("key-a")).await;
            assert_eq!(api.identity_calls().len(), 1, "HTTP {refusal}");
        }
    }

    /// An unreachable Queen settles nothing, so it IS asked again — and until
    /// it answers, the credential stands for itself.
    ///
    /// A real (short) window rather than a paused clock, because the map is
    /// read from synchronous code and therefore keeps `std::time::Instant`,
    /// which tokio's clock does not move. Same shape as the catalog's own TTL
    /// test.
    #[tokio::test]
    async fn an_unreachable_identity_surface_is_asked_again() {
        let api = FakeQueen::with(&[]);
        api.fail_identity(
            Some("key-a"),
            queen::Error::Transport("connection refused".into()),
        );
        let ids = Identities::with_retry(over(&api), Duration::from_millis(40));

        assert_eq!(
            ids.resolve(Some("key-a")).await,
            TenantKey::of_credential(Some("key-a"))
        );
        // Inside the window the failure stands and nothing is asked.
        ids.resolve(Some("key-a")).await;
        assert_eq!(api.identity_calls().len(), 1);

        // Past it, and now Queen answers.
        api.answer_identity(Some("key-a"), "cluster-1");
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            ids.resolve(Some("key-a")).await,
            TenantKey::Tenant(Arc::from("cluster-1"))
        );
        assert_eq!(api.identity_calls().len(), 2);
    }

    /// ...and once it HAS answered, the key never changes under a live group:
    /// a second answer is not asked for, whatever Queen would say now.
    #[tokio::test]
    async fn a_resolved_key_is_never_re_resolved() {
        let api = FakeQueen::with(&[]);
        api.answer_identity(Some("key-a"), "cluster-1");
        let ids = Identities::with_retry(over(&api), Duration::from_millis(10));
        assert_eq!(
            ids.resolve(Some("key-a")).await,
            TenantKey::Tenant(Arc::from("cluster-1"))
        );

        api.answer_identity(Some("key-a"), "cluster-2");
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(
            ids.resolve(Some("key-a")).await,
            TenantKey::Tenant(Arc::from("cluster-1")),
            "the scope of a live group moved under it"
        );
        assert_eq!(api.identity_calls().len(), 1);
    }

    /// The key space is "credentials this facade has been shown", which a peer
    /// chooses on a SASL listener, so the map is bounded — and what goes is the
    /// coldest, so a credential doing its job keeps its identity.
    #[tokio::test]
    async fn the_identity_map_is_bounded_and_evicts_the_coldest() {
        let api = FakeQueen::with(&[]);
        api.answer_identity(Some("real"), "cluster-1");
        let ids = Identities::new(over(&api));
        ids.resolve(Some("real")).await;

        for i in 0..MAX_CREDENTIALS * 2 {
            if i % 8 == 0 {
                // The real tenant keeps working throughout.
                assert!(ids.known(Some("real")).is_named());
            }
            ids.resolve(Some(&format!("junk-{i}"))).await;
        }
        assert_eq!(ids.len(), MAX_CREDENTIALS);
        assert!(
            ids.known(Some("real")).is_named(),
            "a flood of credentials took a tenant's identity away from it"
        );
    }

    /// A fleet authenticating at once asks once, and every connection of it
    /// gets the SAME key — the property that keeps one tenant's consumers in
    /// one group when they all start together.
    #[tokio::test]
    async fn a_burst_on_one_credential_asks_once() {
        let api = FakeQueen::with(&[]);
        api.answer_identity(Some("key-a"), "cluster-1");
        let ids = Arc::new(Identities::new(over(&api)));

        let mut joins = Vec::new();
        for _ in 0..16 {
            let ids = Arc::clone(&ids);
            joins.push(tokio::spawn(
                async move { ids.resolve(Some("key-a")).await },
            ));
        }
        for join in joins {
            assert_eq!(
                join.await.unwrap(),
                TenantKey::Tenant(Arc::from("cluster-1"))
            );
        }
        assert_eq!(api.identity_calls().len(), 1);
    }
}
