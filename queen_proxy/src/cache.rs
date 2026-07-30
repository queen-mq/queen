//! ClusterCache: host -> ClusterCtx and api-key-hash -> (ClusterCtx, scopes),
//! DB-backed with TTL + LISTEN/NOTIFY invalidation. OWNER: Agent B.
//!
//! dev-static (QUEEN_PROXY_DEV_CELL_URL) always wins when configured -- the
//! DB-backed path below is only ever consulted when it isn't. See
//! migrations/001_init.sql for the table shapes and the limit_overrides
//! merge convention (mirrored exactly by `merge_limits` below).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::config::{Config, PxdbConfig};
use crate::state::{ClusterCtx, ClusterStatus, EffectiveLimits, Features, Scopes};

const HOST_TTL: Duration = Duration::from_secs(30);
const KEY_POSITIVE_TTL: Duration = Duration::from_secs(30);
/// Short negative TTL so a brute-forced/garbage key hash still forces a
/// re-check every 5s (anti-brute-force: the DB, not memory, is the source of
/// truth for "does this hash exist"), while capping how hard a hot loop of
/// bad keys can hit pxdb.
const KEY_NEGATIVE_TTL: Duration = Duration::from_secs(5);

/// The pg_notify channel every queen_proxy.* mutating SQL function targets
/// (migrations/002_functions.sql via record_operation()).
pub const INVAL_CHANNEL: &str = "queen_proxy_inval";

const LISTENER_MAX_BACKOFF: Duration = Duration::from_secs(30);
/// A session that stayed up at least this long counts as "was healthy" for
/// backoff-reset purposes -- see `listen_forever`.
const LISTENER_HEALTHY_SESSION_MIN: Duration = Duration::from_secs(10);

/// Sampling interval for the stale-serve warning -- one line per window while
/// pxdb is down, not one per request (every request takes that path during an
/// outage). Same shape as gateway.rs::maint_log_due.
const STALE_LOG_INTERVAL: Duration = Duration::from_secs(10);
static STALE_LOG_NEXT: std::sync::Mutex<Option<Instant>> = std::sync::Mutex::new(None);

/// Is a stale-serve line due? Non-blocking try_lock so a concurrent resolver
/// skips its line rather than waiting on the request path.
fn stale_log_due(now: Instant) -> bool {
    let Ok(mut next) = STALE_LOG_NEXT.try_lock() else { return false };
    match *next {
        Some(at) if now < at => false,
        _ => {
            *next = Some(now + STALE_LOG_INTERVAL);
            true
        }
    }
}

/// What one pxdb lookup told us. `Absent` (the query ran and matched no row)
/// and `Unavailable` (pxdb never produced an answer) are deliberately
/// distinct: the fail-open below is only sound as long as "the DB said no"
/// can never be confused with "the DB did not answer".
enum Lookup<T> {
    Found(T),
    Absent,
    Unavailable,
}

/// Why the fresh-cache fast path was missed, as far as the fail-open rule
/// cares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Miss {
    /// pxdb ran the query and matched nothing: the row genuinely isn't there.
    NoSuchRow,
    /// pxdb never answered -- pool checkout, query, or row decode failed.
    NoAnswer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fallback {
    /// Serve the expired entry: last known-good, still inside the grace
    /// window, and pxdb couldn't contradict it.
    ServeStale,
    /// pxdb answered "no such row" -- deny, and forget any expired entry so a
    /// later outage can't resurrect a deleted cluster/revoked key through the
    /// grace window.
    FailClosed,
    /// Nothing safe to serve (no cached entry, or its grace window is gone).
    Deny,
}

/// The PLAN §2 degradation rule, kept free of I/O so it can be tested:
/// fail-open for known good, fail-closed for unknowns and for anything pxdb
/// positively denied.
fn fallback(miss: Miss, stale_expires_at: Option<Instant>, now: Instant, grace: Duration) -> Fallback {
    if miss == Miss::NoSuchRow {
        return Fallback::FailClosed;
    }
    // checked_add: an absurd QUEEN_PROXY_STALE_GRACE_MS overflows the Instant,
    // and a config typo must neither panic on the request path nor silently
    // become an unbounded fail-open.
    match stale_expires_at.and_then(|expires_at| expires_at.checked_add(grace)) {
        Some(deadline) if now <= deadline => Fallback::ServeStale,
        _ => Fallback::Deny,
    }
}

struct HostEntry {
    ctx: Arc<ClusterCtx>,
    expires_at: Instant,
}

#[derive(Clone)]
struct KeyEntry {
    /// None = negative cache (hash not found / revoked at last check).
    value: Option<(Arc<ClusterCtx>, Uuid, Scopes)>,
    expires_at: Instant,
}

type HostMap = RwLock<HashMap<String, HostEntry>>;
type KeyMap = RwLock<HashMap<String, KeyEntry>>;

pub struct ClusterCache {
    dev_static: Option<ClusterCtx>,
    /// Dev/demo fallback (QUEEN_PROXY_DEFAULT_CLUSTER): slug tried when the
    /// Host header resolves to no cluster — browsers on localhost send
    /// `Host: localhost:6711`, which is no cluster's slug. Never set in cloud.
    default_cluster: Option<String>,
    db: Option<deadpool_postgres::Pool>,
    /// Cloned at construction so `spawn_listener` can open its own dedicated
    /// LISTEN connection later. The pool can't be reused for this: deadpool
    /// drives each pooled connection on its own background task and
    /// discards `AsyncMessage::Notification` (see tokio-postgres's
    /// LISTEN/NOTIFY docs -- a dedicated `Connection` object, polled by
    /// hand, is the documented pattern).
    pxdb_cfg: Option<PxdbConfig>,
    /// How far past its TTL an entry may still be served when pxdb fails to
    /// answer (config::stale_grace).
    stale_grace: Duration,
    host_cache: Arc<HostMap>,
    key_cache: Arc<KeyMap>,
}

impl ClusterCache {
    pub fn new(cfg: &Config, db: Option<deadpool_postgres::Pool>) -> ClusterCache {
        let dev_static = cfg.dev_static.as_ref().map(|d| ClusterCtx {
            cluster_id: Uuid::nil(),
            tenant_id: Uuid::nil(),
            broker_tenant: Uuid::parse_str(&d.broker_tenant)
                .unwrap_or_else(|_| Uuid::parse_str(crate::config::DEFAULT_TENANT_UUID).unwrap()),
            slug: "dev".to_string(),
            cell_base_url: d.cell_url.clone(),
            cell_token: d.cell_token.clone(),
            status: ClusterStatus::Active,
            limits: EffectiveLimits::default(),
            features: Features { streams: true, traces: true },
        });
        ClusterCache {
            dev_static,
            default_cluster: cfg.default_cluster.clone(),
            db,
            pxdb_cfg: cfg.pxdb.clone(),
            stale_grace: crate::config::stale_grace(),
            host_cache: Arc::new(RwLock::new(HashMap::new())),
            key_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Resolve the cluster for an inbound Host header (host[:port]).
    pub async fn resolve_host(&self, host: &str) -> Option<Arc<ClusterCtx>> {
        // dev-static always wins when configured -- behavior unchanged from
        // the skeleton.
        if let Some(ctx) = &self.dev_static {
            return Some(Arc::new(ctx.clone()));
        }
        if let Some(slug) = slug_from_host(host) {
            if let Some(ctx) = self.resolve_slug(slug).await {
                return Some(ctx);
            }
        }
        // fallback for dev/demo hosts (localhost etc.); None in cloud
        let d = self.default_cluster.clone()?;
        self.resolve_slug(d).await
    }

    /// Resolve a cluster named EXPLICITLY (act-as-cluster, acting.rs): a slug
    /// or a cluster uuid. Deliberately does NOT honour dev-static or
    /// `default_cluster` — those exist to guess a cluster when Host cannot
    /// name one, and a caller that named one must get that one or nothing.
    pub async fn resolve_ref(&self, reference: &str) -> Option<Arc<ClusterCtx>> {
        let reference = reference.trim();
        if reference.is_empty() {
            return None;
        }
        match Uuid::parse_str(reference) {
            // Cache key is namespaced so `id:<uuid>` can never collide with a
            // slug entry; invalidation retains by cluster_id, so both shapes
            // are dropped together on NOTIFY.
            Ok(id) => {
                let key = format!("id:{id}");
                self.resolve_keyed(key, RESOLVE_BY_ID_SQL, id.to_string()).await
            }
            Err(_) => self.resolve_slug(reference.to_ascii_lowercase()).await,
        }
    }

    async fn resolve_slug(&self, slug: String) -> Option<Arc<ClusterCtx>> {
        self.resolve_keyed(slug.clone(), RESOLVE_HOST_SQL, slug).await
    }

    /// The shared TTL + fail-open body behind both lookups: `cache_key` names
    /// the entry, `sql`/`param` name the row.
    async fn resolve_keyed(
        &self,
        cache_key: String,
        sql: &'static str,
        param: String,
    ) -> Option<Arc<ClusterCtx>> {
        let slug = cache_key;
        let pool = self.db.as_ref()?;

        // An expired entry is kept in hand, not dropped: should pxdb turn out
        // to be unreachable it is the last known-good answer for this slug,
        // and PLAN §2 wants that served rather than letting a control-plane
        // outage 421 every cluster in the cell.
        let stale = {
            let cache = self.host_cache.read().unwrap();
            match cache.get(&slug) {
                Some(entry) if entry.expires_at > Instant::now() => return Some(entry.ctx.clone()),
                Some(entry) => Some((entry.ctx.clone(), entry.expires_at)),
                None => None,
            }
        };

        let miss = match lookup_host(pool, sql, &param).await {
            Lookup::Found(ctx) => {
                let mut cache = self.host_cache.write().unwrap();
                cache.insert(slug, HostEntry { ctx: ctx.clone(), expires_at: Instant::now() + HOST_TTL });
                drop(cache);
                return Some(ctx);
            }
            Lookup::Absent => Miss::NoSuchRow,
            Lookup::Unavailable => Miss::NoAnswer,
        };

        match fallback(miss, stale.as_ref().map(|(_, at)| *at), Instant::now(), self.stale_grace) {
            Fallback::ServeStale => stale.map(|(ctx, _)| {
                if stale_log_due(Instant::now()) {
                    tracing::warn!(
                        slug = %slug, grace_s = self.stale_grace.as_secs(),
                        "pxdb unreachable: serving cluster from expired cache (fail-open, PLAN §2)"
                    );
                }
                ctx
            }),
            Fallback::FailClosed => {
                // Only when there is something to forget: a flood of garbage
                // Host headers (the common 421 case) must not take the write
                // lock, and never inserted an entry to begin with.
                if stale.is_some() {
                    self.host_cache.write().unwrap().remove(&slug);
                }
                None
            }
            Fallback::Deny => None,
        }
    }

    /// Look up an API key by sha256 hash (hex). Returns the cluster and scopes.
    pub async fn by_key_hash(&self, hash_hex: &str) -> Option<(Arc<ClusterCtx>, Uuid, Scopes)> {
        let pool = self.db.as_ref()?;

        // Same fail-open rule as resolve_slug, and needed for it to be worth
        // anything: a cluster resolved from a stale host entry is still 401
        // for every API-key request if the key lookup can't degrade too.
        let stale = {
            let cache = self.key_cache.read().unwrap();
            match cache.get(hash_hex) {
                Some(entry) if entry.expires_at > Instant::now() => return entry.value.clone(),
                Some(entry) => Some(entry.clone()),
                None => None,
            }
        };

        let miss = match lookup_key(pool, hash_hex).await {
            Lookup::Found(result) => {
                let mut cache = self.key_cache.write().unwrap();
                cache.insert(
                    hash_hex.to_string(),
                    KeyEntry { value: Some(result.clone()), expires_at: Instant::now() + KEY_POSITIVE_TTL },
                );
                drop(cache);
                return Some(result);
            }
            Lookup::Absent => Miss::NoSuchRow,
            Lookup::Unavailable => Miss::NoAnswer,
        };

        match fallback(miss, stale.as_ref().map(|e| e.expires_at), Instant::now(), self.stale_grace) {
            Fallback::ServeStale => {
                let served = stale.and_then(|e| e.value);
                // Only a positive entry is a fail-open worth reporting; a
                // stale negative just keeps denying.
                if served.is_some() && stale_log_due(Instant::now()) {
                    tracing::warn!(
                        grace_s = self.stale_grace.as_secs(),
                        "pxdb unreachable: serving api key from expired cache (fail-open, PLAN §2)"
                    );
                }
                served
            }
            Fallback::FailClosed => {
                // pxdb answered: this hash is unknown or revoked. The negative
                // entry both denies and replaces any expired positive, so the
                // grace window can never resurrect a revoked key.
                let mut cache = self.key_cache.write().unwrap();
                cache.insert(
                    hash_hex.to_string(),
                    KeyEntry { value: None, expires_at: Instant::now() + KEY_NEGATIVE_TTL },
                );
                drop(cache);
                None
            }
            Fallback::Deny => None,
        }
    }

    /// Invalidate a cluster (NOTIFY payload or admin action).
    pub fn invalidate(&self, cluster_id: Uuid) {
        invalidate_caches(&self.host_cache, &self.key_cache, cluster_id);
    }

    /// Spawn the LISTEN task. Called from main.rs at startup, next to
    /// `registry.spawn_reconciler()`. No-op in dev-static mode (matches the
    /// doc comment on the skeleton) and also when there's simply no pxdb
    /// configured. Note this takes `&self`, not `self: &Arc<Self>` -- the
    /// skeleton declared the latter, but `AppState` stores `cache` as a
    /// plain field (not `Arc<ClusterCache>`), so nothing could ever have
    /// called it that way.
    pub fn spawn_listener(&self) {
        let Some(pxcfg) = self.pxdb_cfg.clone() else {
            tracing::info!("queen_proxy_inval listener: no pxdb configured, skipping (dev-static mode)");
            return;
        };
        let host_cache = self.host_cache.clone();
        let key_cache = self.key_cache.clone();
        tokio::spawn(async move {
            listen_forever(pxcfg, host_cache, key_cache).await;
        });
    }
}

// --------------------------------------------------------- pxdb lookups
//
// Both return `Unavailable` for a malformed row on purpose: the row EXISTS,
// so pxdb has not said "no such cluster/key" -- we merely failed to decode
// its answer (schema drift, a column that went NULL). Classifying that as
// `Absent` would let one bad row deny a live cluster.

async fn lookup_host(
    pool: &deadpool_postgres::Pool,
    sql: &str,
    param: &str,
) -> Lookup<Arc<ClusterCtx>> {
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "resolve_host: pxdb pool.get failed");
            return Lookup::Unavailable;
        }
    };
    let row_opt = match client.query_opt(sql, &[&param]).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, cluster = %param, "resolve_host: query failed");
            return Lookup::Unavailable;
        }
    };
    let Some(row) = row_opt else { return Lookup::Absent };
    match ctx_from_row(&row) {
        Ok(c) => Lookup::Found(Arc::new(c)),
        Err(e) => {
            tracing::error!(error = %e, cluster = %param, "resolve_host: malformed row");
            Lookup::Unavailable
        }
    }
}

async fn lookup_key(
    pool: &deadpool_postgres::Pool,
    hash_hex: &str,
) -> Lookup<(Arc<ClusterCtx>, Uuid, Scopes)> {
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "by_key_hash: pxdb pool.get failed");
            return Lookup::Unavailable;
        }
    };
    let row_opt = match client.query_opt(BY_KEY_HASH_SQL, &[&hash_hex]).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "by_key_hash: query failed");
            return Lookup::Unavailable;
        }
    };
    let Some(row) = row_opt else { return Lookup::Absent };
    let result = match build_key_result(&row) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "by_key_hash: malformed row");
            return Lookup::Unavailable;
        }
    };

    // Best-effort last_used_at bump -- never fail the auth call over it. Only
    // runs on a cache MISS (once per positive-TTL window per key), not per
    // request.
    let key_id_str = result.1.to_string();
    if let Err(e) = client
        .execute(
            "UPDATE queen_proxy.api_keys SET last_used_at = now() WHERE id = $1::text::uuid",
            &[&key_id_str],
        )
        .await
    {
        tracing::debug!(error = %e, "by_key_hash: last_used_at update failed (non-fatal)");
    }
    Lookup::Found(result)
}

fn invalidate_caches(host_cache: &HostMap, key_cache: &KeyMap, cluster_id: Uuid) {
    host_cache.write().unwrap().retain(|_, e| e.ctx.cluster_id != cluster_id);
    key_cache.write().unwrap().retain(|_, e| match &e.value {
        Some((ctx, _, _)) => ctx.cluster_id != cluster_id,
        // Negative entries (unknown/garbage hash) aren't tied to any real
        // cluster; a cluster's invalidation has nothing to say about them.
        None => true,
    });
}

// ---------------------------------------------------------------- listener

/// Drives the dedicated LISTEN connection forever, reconnecting with
/// exponential backoff (capped) on any error or graceful close. Backoff
/// resets to its floor once a session has stayed up "long enough" to count
/// as healthy, so a blip doesn't leave the listener limping at the max
/// backoff long after pxdb recovers.
async fn listen_forever(pxcfg: PxdbConfig, host_cache: Arc<HostMap>, key_cache: Arc<KeyMap>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let started = Instant::now();
        if let Err(e) = listen_once(&pxcfg, &host_cache, &key_cache).await {
            tracing::warn!(error = %e, backoff_s = backoff.as_secs(), "queen_proxy_inval listener: connection lost, retrying");
        }
        backoff = if started.elapsed() >= LISTENER_HEALTHY_SESSION_MIN {
            Duration::from_secs(1)
        } else {
            (backoff * 2).min(LISTENER_MAX_BACKOFF)
        };
        tokio::time::sleep(backoff).await;
    }
}

/// One connect-LISTEN-poll session. Returns (with an error) when the
/// connection drops; the caller reconnects. Deliberately NOT built on the
/// deadpool pool (see the field comment on `pxdb_cfg`): this opens its own
/// tokio_postgres connection and polls the `Connection` object directly
/// (`std::future::poll_fn` bridging `Connection::poll_message`, the
/// documented tokio-postgres LISTEN/NOTIFY pattern) rather than spawning it
/// as a bare driver task, which is what would discard notifications.
async fn listen_once(pxcfg: &PxdbConfig, host_cache: &Arc<HostMap>, key_cache: &Arc<KeyMap>) -> Result<(), String> {
    let mut pg = tokio_postgres::Config::new();
    pg.host(&pxcfg.host)
        .port(pxcfg.port)
        .user(&pxcfg.user)
        .password(&pxcfg.password)
        .dbname(&pxcfg.dbname)
        .application_name("queen-proxy-listen");
    let listen_stmt = format!("LISTEN {INVAL_CHANNEL}");

    if pxcfg.use_ssl {
        let connector = crate::pgtls::make_connector(pxcfg.ssl_reject_unauthorized);
        let (client, connection) = pg.connect(connector).await.map_err(|e| format!("connect: {e}"))?;
        run_listen_session(client, connection, &listen_stmt, host_cache, key_cache, true).await
    } else {
        let (client, connection) =
            pg.connect(tokio_postgres::NoTls).await.map_err(|e| format!("connect: {e}"))?;
        run_listen_session(client, connection, &listen_stmt, host_cache, key_cache, false).await
    }
}

/// Drive the LISTEN statement and the notification stream from ONE loop.
///
/// This ordering is load-bearing. `Client` only queues a request; the
/// `Connection` is what drives the socket, so awaiting `batch_execute` BEFORE
/// entering the poll loop parks forever — the LISTEN never reaches the server,
/// no notification ever arrives, and the failure is silent because the
/// "connected" log line sits on the far side of that await. Every cache
/// invalidation then silently degrades to TTL expiry (30s), which is how a
/// revoked API key kept working. Poll both, or neither works.
async fn run_listen_session<S, T>(
    client: tokio_postgres::Client,
    mut connection: tokio_postgres::Connection<S, T>,
    listen_stmt: &str,
    host_cache: &Arc<HostMap>,
    key_cache: &Arc<KeyMap>,
    tls: bool,
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    T: tokio_postgres::tls::TlsStream + Unpin,
{
    let listen = client.batch_execute(listen_stmt);
    tokio::pin!(listen);
    let mut listening = false;

    loop {
        tokio::select! {
            res = &mut listen, if !listening => {
                res.map_err(|e| format!("LISTEN: {e}"))?;
                listening = true;
                tracing::info!(channel = INVAL_CHANNEL, tls, "queen_proxy_inval listener connected");
            }
            msg = std::future::poll_fn(|cx| connection.poll_message(cx)) => {
                match msg {
                    Some(Ok(tokio_postgres::AsyncMessage::Notification(n))) => {
                        handle_notification(&n, host_cache, key_cache);
                    }
                    Some(Ok(_)) => {} // notices etc., nothing to do
                    Some(Err(e)) => return Err(format!("connection error: {e}")),
                    None => return Err("connection closed".to_string()),
                }
            }
        }
    }
}

fn handle_notification(n: &tokio_postgres::Notification, host_cache: &Arc<HostMap>, key_cache: &Arc<KeyMap>) {
    if n.channel() != INVAL_CHANNEL {
        return;
    }
    match Uuid::parse_str(n.payload()) {
        Ok(cluster_id) => {
            tracing::debug!(cluster = %cluster_id, "cache invalidated via NOTIFY");
            invalidate_caches(host_cache, key_cache, cluster_id);
        }
        Err(e) => {
            tracing::warn!(payload = n.payload(), error = %e, "queen_proxy_inval: unparseable NOTIFY payload");
        }
    }
}

// -------------------------------------------------------------- row -> ctx

/// The projection `ctx_from_row` reads, shared by every cluster lookup keyed
/// on the clusters table. A macro (not a const) so `concat!` can glue a WHERE
/// onto it at compile time and the two queries cannot drift apart.
macro_rules! cluster_select {
    () => {
        "
    SELECT c.id::text                  AS cluster_id,
           c.tenant_id::text           AS tenant_id,
           c.broker_tenant_uuid::text  AS broker_tenant,
           c.slug                      AS slug,
           ce.base_url                 AS base_url,
           ce.cell_secret              AS cell_secret,
           t.status                    AS tenant_status,
           c.status                    AS cluster_status,
           p.max_req_per_sec, p.req_burst, p.max_msgs_per_sec, p.msgs_burst,
           p.max_queues, p.max_partitions_per_queue, p.max_parked_pops,
           p.max_payload_bytes, p.max_batch_items, p.max_retained_bytes, p.max_retention_seconds,
           (p.features)::text          AS features_json,
           (c.limit_overrides)::text   AS overrides_json
    FROM queen_proxy.clusters c
    JOIN queen_proxy.tenants t ON t.id = c.tenant_id
    JOIN queen_proxy.cells   ce ON ce.id = c.cell_id
    JOIN queen_proxy.plans   p  ON p.id = c.plan_id
    "
    };
}

const RESOLVE_HOST_SQL: &str = concat!(cluster_select!(), "WHERE c.slug = $1");

/// Act-as-cluster by uuid. `$1::text::uuid` for the same reason every other
/// query in this crate does it: no uuid feature on tokio-postgres.
const RESOLVE_BY_ID_SQL: &str = concat!(cluster_select!(), "WHERE c.id = $1::text::uuid");

const BY_KEY_HASH_SQL: &str = "
    SELECT ak.id::text                 AS key_id,
           ak.scopes                   AS scopes,
           c.id::text                  AS cluster_id,
           c.tenant_id::text           AS tenant_id,
           c.broker_tenant_uuid::text  AS broker_tenant,
           c.slug                      AS slug,
           ce.base_url                 AS base_url,
           ce.cell_secret              AS cell_secret,
           t.status                    AS tenant_status,
           c.status                    AS cluster_status,
           p.max_req_per_sec, p.req_burst, p.max_msgs_per_sec, p.msgs_burst,
           p.max_queues, p.max_partitions_per_queue, p.max_parked_pops,
           p.max_payload_bytes, p.max_batch_items, p.max_retained_bytes, p.max_retention_seconds,
           (p.features)::text          AS features_json,
           (c.limit_overrides)::text   AS overrides_json
    FROM queen_proxy.api_keys ak
    JOIN queen_proxy.clusters c ON c.id = ak.cluster_id
    JOIN queen_proxy.tenants  t ON t.id = c.tenant_id
    JOIN queen_proxy.cells    ce ON ce.id = c.cell_id
    JOIN queen_proxy.plans    p  ON p.id = c.plan_id
    WHERE ak.key_hash = $1 AND ak.revoked_at IS NULL";

fn build_key_result(row: &tokio_postgres::Row) -> Result<(Arc<ClusterCtx>, Uuid, Scopes), String> {
    let key_id = parse_uuid(row, "key_id")?;
    let scopes_vec: Vec<String> = row.try_get("scopes").map_err(|e| format!("scopes: {e}"))?;
    let scopes = Scopes {
        produce: scopes_vec.iter().any(|s| s == "produce"),
        consume: scopes_vec.iter().any(|s| s == "consume"),
        admin: scopes_vec.iter().any(|s| s == "admin"),
        read: scopes_vec.iter().any(|s| s == "read"),
    };
    let ctx = ctx_from_row(row)?;
    Ok((Arc::new(ctx), key_id, scopes))
}

/// Shared by resolve_host and by_key_hash: both SELECTs alias to the same
/// column names (cluster_id, tenant_id, broker_tenant, slug, base_url,
/// cell_secret, tenant_status, cluster_status, the plan limit columns,
/// features_json, overrides_json) precisely so this one builder works for
/// either.
fn ctx_from_row(row: &tokio_postgres::Row) -> Result<ClusterCtx, String> {
    let cluster_id = parse_uuid(row, "cluster_id")?;
    let tenant_id = parse_uuid(row, "tenant_id")?;
    let broker_tenant = parse_uuid(row, "broker_tenant")?;
    let slug: String = row.try_get("slug").map_err(|e| format!("slug: {e}"))?;
    let cell_base_url: String = row.try_get("base_url").map_err(|e| format!("base_url: {e}"))?;
    let cell_token: Option<String> = row.try_get("cell_secret").map_err(|e| format!("cell_secret: {e}"))?;
    let tenant_status: String = row.try_get("tenant_status").map_err(|e| format!("tenant_status: {e}"))?;
    let cluster_status: String = row.try_get("cluster_status").map_err(|e| format!("cluster_status: {e}"))?;
    let status = merge_status(&tenant_status, &cluster_status);

    let base = EffectiveLimits {
        max_req_per_sec: get_i32_as_i64(row, "max_req_per_sec")?,
        req_burst: get_i32_as_i64(row, "req_burst")?,
        max_msgs_per_sec: get_i32_as_i64(row, "max_msgs_per_sec")?,
        msgs_burst: get_i32_as_i64(row, "msgs_burst")?,
        max_queues: get_i32_as_i64(row, "max_queues")?,
        max_partitions_per_queue: get_i32_as_i64(row, "max_partitions_per_queue")?,
        max_parked_pops: get_i32_as_i64(row, "max_parked_pops")?,
        max_payload_bytes: get_i32_as_i64(row, "max_payload_bytes")?,
        max_batch_items: get_i32_as_i64(row, "max_batch_items")?,
        max_retained_bytes: row.try_get("max_retained_bytes").map_err(|e| format!("max_retained_bytes: {e}"))?,
        max_retention_seconds: get_i32_as_i64(row, "max_retention_seconds")?,
    };
    let overrides_json: String = row.try_get("overrides_json").map_err(|e| format!("overrides_json: {e}"))?;
    let overrides: serde_json::Value = serde_json::from_str(&overrides_json).unwrap_or(serde_json::Value::Null);
    let limits = merge_limits(base, &overrides);

    let features_json: String = row.try_get("features_json").map_err(|e| format!("features_json: {e}"))?;
    let features = parse_features(&features_json);

    Ok(ClusterCtx {
        cluster_id,
        tenant_id,
        broker_tenant,
        slug,
        cell_base_url,
        cell_token,
        status,
        limits,
        features,
    })
}

fn parse_uuid(row: &tokio_postgres::Row, col: &str) -> Result<Uuid, String> {
    let s: String = row.try_get(col).map_err(|e| format!("{col}: {e}"))?;
    Uuid::parse_str(&s).map_err(|e| format!("{col}: bad uuid {s:?}: {e}"))
}

fn get_i32_as_i64(row: &tokio_postgres::Row, col: &str) -> Result<Option<i64>, String> {
    let v: Option<i32> = row.try_get(col).map_err(|e| format!("{col}: {e}"))?;
    Ok(v.map(i64::from))
}

/// tenant.status + cluster.status -> effective ClusterStatus, worst wins
/// (PLAN §6.2 / open decision §13.b: tenant `grace` == "payment-failed" maps
/// to the same severity as cluster `push_blocked` -- pushes blocked,
/// consumes allowed -- rather than a full suspend).
fn merge_status(tenant_status: &str, cluster_status: &str) -> ClusterStatus {
    fn tenant_severity(s: &str) -> u8 {
        match s {
            "active" => 1,
            "grace" => 2,
            "suspended" => 3,
            "deleting" => 4,
            // Unreachable given the DB CHECK constraint, but if a future
            // status value isn't recognized here yet, fail closed (at least
            // as restrictive as suspended), never open.
            _ => 3,
        }
    }
    fn cluster_severity(s: &str) -> u8 {
        match s {
            "active" => 1,
            "push_blocked" => 2,
            "suspended" => 3,
            "deleting" => 4,
            _ => 3,
        }
    }
    match tenant_severity(tenant_status).max(cluster_severity(cluster_status)) {
        4 => ClusterStatus::Deleting,
        3 => ClusterStatus::Suspended,
        2 => ClusterStatus::PushBlocked,
        _ => ClusterStatus::Active,
    }
}

/// Merge a cluster's `limit_overrides` JSONB onto its plan's base limits.
/// Convention (also documented on clusters.limit_overrides in
/// 001_init.sql): key absent -> inherit the plan value; key present as JSON
/// null -> force unlimited; key present as a number -> that value wins.
fn merge_limits(base: EffectiveLimits, overrides: &serde_json::Value) -> EffectiveLimits {
    EffectiveLimits {
        max_req_per_sec: override_or(overrides, "max_req_per_sec", base.max_req_per_sec),
        req_burst: override_or(overrides, "req_burst", base.req_burst),
        max_msgs_per_sec: override_or(overrides, "max_msgs_per_sec", base.max_msgs_per_sec),
        msgs_burst: override_or(overrides, "msgs_burst", base.msgs_burst),
        max_queues: override_or(overrides, "max_queues", base.max_queues),
        max_partitions_per_queue: override_or(overrides, "max_partitions_per_queue", base.max_partitions_per_queue),
        max_parked_pops: override_or(overrides, "max_parked_pops", base.max_parked_pops),
        max_payload_bytes: override_or(overrides, "max_payload_bytes", base.max_payload_bytes),
        max_batch_items: override_or(overrides, "max_batch_items", base.max_batch_items),
        max_retained_bytes: override_or(overrides, "max_retained_bytes", base.max_retained_bytes),
        max_retention_seconds: override_or(overrides, "max_retention_seconds", base.max_retention_seconds),
    }
}

/// One field of the merge above: absent key -> `base`; JSON null -> `None`
/// (explicit unlimited); JSON number -> that value (falls back to `base` if
/// the value present isn't actually a number, rather than silently zeroing
/// the limit). `pub(crate)` so registry.rs's storage-quota reconciler can
/// resolve a single effective limit (max_retained_bytes) without
/// duplicating this three-way rule.
pub(crate) fn override_or(overrides: &serde_json::Value, key: &str, base: Option<i64>) -> Option<i64> {
    match overrides.get(key) {
        None => base,
        Some(serde_json::Value::Null) => None,
        Some(v) => v.as_i64().or(base),
    }
}

fn parse_features(json: &str) -> Features {
    let v: serde_json::Value = match serde_json::from_str(json) {
        Ok(v) => v,
        Err(_) => return Features::default(),
    };
    Features {
        streams: v.get("streams").and_then(|b| b.as_bool()).unwrap_or(false),
        traces: v.get("traces").and_then(|b| b.as_bool()).unwrap_or(false),
    }
}

/// First DNS label of a Host header, with the port stripped. Lowercased
/// (DNS is case-insensitive; clusters.slug is stored lowercase). Only
/// strips a trailing `:port` when what follows the colon is all-digits, so
/// a colon that isn't a port separator doesn't get silently swallowed.
fn slug_from_host(host: &str) -> Option<String> {
    let host = host.trim();
    if host.is_empty() {
        return None;
    }
    let without_port = match host.rsplit_once(':') {
        Some((h, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => h,
        _ => host,
    };
    let label = without_port.split('.').next()?;
    if label.is_empty() {
        None
    } else {
        Some(label.to_ascii_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slug_from_host_strips_port_and_lowercases() {
        assert_eq!(slug_from_host("Acme.eu1.queenmq.cloud:6711").as_deref(), Some("acme"));
        assert_eq!(slug_from_host("acme.eu1.queenmq.cloud").as_deref(), Some("acme"));
        assert_eq!(slug_from_host("acme").as_deref(), Some("acme"));
        assert_eq!(slug_from_host(""), None);
        assert_eq!(slug_from_host(":6711"), None); // empty label once the port is stripped
    }

    #[test]
    fn slug_from_host_leaves_non_port_colon_suffix_alone() {
        // Not a real Host-header shape (DNS names can't contain ':'), and
        // not one this function tries to be clever about: since
        // "notaport.example.com" isn't all-digits, nothing gets stripped,
        // so the returned "label" keeps the colon (`.split('.')` doesn't
        // treat ':' as a separator). That's fine -- it must merely not
        // panic and not silently guess a stripped slug for malformed
        // input. Downstream is safe either way: "weird:notaport" can never
        // match a real clusters.slug (its CHECK constraint forbids ':'), so
        // resolve_host just misses -> 421, exactly as it should for
        // garbage input.
        assert_eq!(slug_from_host("weird:notaport.example.com").as_deref(), Some("weird:notaport"));
    }

    #[test]
    fn merge_limits_absent_key_inherits_plan() {
        let base = EffectiveLimits { max_queues: Some(20), ..Default::default() };
        let merged = merge_limits(base, &serde_json::json!({}));
        assert_eq!(merged.max_queues, Some(20));
    }

    #[test]
    fn merge_limits_null_forces_unlimited() {
        let base = EffectiveLimits { max_queues: Some(20), ..Default::default() };
        let merged = merge_limits(base, &serde_json::json!({ "max_queues": null }));
        assert_eq!(merged.max_queues, None);
    }

    #[test]
    fn merge_limits_number_overrides() {
        let base = EffectiveLimits { max_queues: Some(20), ..Default::default() };
        let merged = merge_limits(base, &serde_json::json!({ "max_queues": 500 }));
        assert_eq!(merged.max_queues, Some(500));
    }

    #[test]
    fn merge_limits_leaves_other_fields_untouched() {
        let base = EffectiveLimits {
            max_queues: Some(20),
            max_partitions_per_queue: Some(8),
            ..Default::default()
        };
        let merged = merge_limits(base, &serde_json::json!({ "max_queues": 500 }));
        assert_eq!(merged.max_queues, Some(500));
        assert_eq!(merged.max_partitions_per_queue, Some(8));
    }

    #[test]
    fn merge_status_worst_wins() {
        assert_eq!(merge_status("active", "active"), ClusterStatus::Active);
        assert_eq!(merge_status("grace", "active"), ClusterStatus::PushBlocked);
        assert_eq!(merge_status("active", "push_blocked"), ClusterStatus::PushBlocked);
        assert_eq!(merge_status("suspended", "active"), ClusterStatus::Suspended);
        assert_eq!(merge_status("active", "deleting"), ClusterStatus::Deleting);
        assert_eq!(merge_status("deleting", "active"), ClusterStatus::Deleting);
        assert_eq!(merge_status("grace", "suspended"), ClusterStatus::Suspended);
    }

    #[test]
    fn merge_status_unknown_fails_closed() {
        assert_eq!(merge_status("something_new", "active"), ClusterStatus::Suspended);
    }

    // ---- fail-open on pxdb outage (PLAN §2) ----

    const GRACE: Duration = Duration::from_secs(600);

    #[test]
    fn fallback_serves_stale_only_while_inside_the_grace_window() {
        let now = Instant::now();
        // TTL blew 1s ago: pxdb didn't answer, entry is last known-good.
        let expired = now - Duration::from_secs(1);
        assert_eq!(fallback(Miss::NoAnswer, Some(expired), now, GRACE), Fallback::ServeStale);
        // Exactly at the edge of the window still counts as good.
        assert_eq!(fallback(Miss::NoAnswer, Some(now - GRACE), now, GRACE), Fallback::ServeStale);
        // One tick past it: the entry is too old to vouch for anything.
        assert_eq!(
            fallback(Miss::NoAnswer, Some(now - GRACE - Duration::from_millis(1)), now, GRACE),
            Fallback::Deny
        );
    }

    #[test]
    fn fallback_never_serves_stale_when_pxdb_answered_no() {
        let now = Instant::now();
        // The whole correctness of the change: a clean "no such row" fails
        // closed even with a perfectly fresh-looking cached entry behind it.
        assert_eq!(
            fallback(Miss::NoSuchRow, Some(now + Duration::from_secs(30)), now, GRACE),
            Fallback::FailClosed
        );
        assert_eq!(fallback(Miss::NoSuchRow, None, now, GRACE), Fallback::FailClosed);
    }

    #[test]
    fn fallback_denies_unknown_with_nothing_cached() {
        // pxdb down + never seen this slug/key -> still a 421/401, never a
        // guess (PLAN §2: "deny unknowns").
        let now = Instant::now();
        assert_eq!(fallback(Miss::NoAnswer, None, now, GRACE), Fallback::Deny);
    }

    #[test]
    fn fallback_zero_grace_disables_stale_serving() {
        // QUEEN_PROXY_STALE_GRACE_MS=0 is the opt-out: an expired entry is
        // then only servable in the same instant it expired.
        let now = Instant::now();
        assert_eq!(
            fallback(Miss::NoAnswer, Some(now - Duration::from_millis(1)), now, Duration::ZERO),
            Fallback::Deny
        );
    }

    #[test]
    fn stale_log_is_sampled() {
        let t0 = Instant::now();
        assert!(stale_log_due(t0), "first line is always due");
        assert!(!stale_log_due(t0), "a second stale serve in the same instant is sampled out");
        assert!(stale_log_due(t0 + STALE_LOG_INTERVAL), "due again once the interval elapses");
    }

    #[test]
    fn parse_features_defaults_missing_to_false() {
        let f = parse_features("{}");
        assert!(!f.streams);
        assert!(!f.traces);
        let f = parse_features(r#"{"streams":true}"#);
        assert!(f.streams);
        assert!(!f.traces);
    }
}
