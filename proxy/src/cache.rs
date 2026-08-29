//! ClusterCache: host -> ClusterCtx and api-key-hash -> (ClusterCtx, scopes),
//! DB-backed with TTL + LISTEN/NOTIFY invalidation. OWNER: Agent B.
//!
//! dev-static (QUEEN_PROXY_DEV_CELL_URL) always wins when configured -- the
//! DB-backed path below is only ever consulted when it isn't. See
//! migrations/001_init.sql for the table shapes and the limit_overrides
//! merge convention (mirrored exactly by `merge_limits` below).

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
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
/// Ceiling on NEGATIVE api-key entries (unknown/revoked hashes). Their cache
/// key is attacker-chosen, so this is the difference between a memo and an
/// unbounded allocation an anonymous caller drives; see the `Lookup::Absent`
/// arm of `apply_key_lookup`. At ~230 B per entry this caps them near 2.5 MB,
/// while still absorbing any realistic burst of retries from one broken client
/// (a 5 s TTL means the cap is only reached by ~10 000 DISTINCT bad keys inside
/// one window).
///
/// It doubles as the trigger for the prune pass, which is why it is compared
/// against the WHOLE map's length: a cell with more live api keys than this
/// would prune on every unknown key instead of every TTL, which costs a walk
/// and never costs correctness. Positive entries are never dropped by it.
const KEY_NEGATIVE_MAX: usize = 10_000;
/// After a refresh pxdb failed to answer, how long an expired entry keeps
/// being served before another refresh is attempted for it. Bounds the cost
/// of an outage to one (failing) lookup per entry per second, not per request.
const REFRESH_BACKOFF: Duration = Duration::from_secs(1);
/// Default cadence of the batched `api_keys.last_used_at` write
/// (QUEEN_PROXY_KEY_TOUCH_MS).
const KEY_TOUCH_FLUSH_MS: u64 = 10_000;

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
static STALE_LOG: LogGate = LogGate::new();

/// One line per STALE_LOG_INTERVAL. Non-blocking try_lock so a concurrent
/// resolver skips its line rather than waiting on the request path. A struct
/// (not a bare static) so tests can own a gate instead of sharing the
/// process-wide one with every other test that happens to log.
struct LogGate(Mutex<Option<Instant>>);

impl LogGate {
    const fn new() -> LogGate {
        LogGate(Mutex::new(None))
    }

    fn due(&self, now: Instant) -> bool {
        let Ok(mut next) = self.0.try_lock() else { return false };
        match *next {
            Some(at) if now < at => false,
            _ => {
                *next = Some(now + STALE_LOG_INTERVAL);
                true
            }
        }
    }
}

/// Is a stale-serve line due on the process-wide gate?
fn stale_log_due(now: Instant) -> bool {
    STALE_LOG.due(now)
}

/// What one pxdb lookup told us. `Absent` (the query ran and matched no row)
/// and `Unavailable` (pxdb never produced an answer) are deliberately
/// distinct: the fail-open below is only sound as long as "the DB said no"
/// can never be confused with "the DB did not answer".
#[derive(Clone)]
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
    /// Earliest instant a background refresh may be started for this entry
    /// once it has expired. `expires_at` on insert; pushed out by
    /// REFRESH_BACKOFF each time pxdb fails to answer.
    refresh_after: Instant,
}

type KeyResult = (Arc<ClusterCtx>, Uuid, Scopes);

#[derive(Clone)]
struct KeyEntry {
    /// None = negative cache (hash not found / revoked at last check).
    value: Option<KeyResult>,
    expires_at: Instant,
    refresh_after: Instant,
}

type HostMap = RwLock<HashMap<String, HostEntry>>;
type KeyMap = RwLock<HashMap<String, KeyEntry>>;
/// Keys a successful lookup has seen since the last `last_used_at` flush.
type Touched = Mutex<HashSet<Uuid>>;

// ------------------------------------------------------------ single-flight

/// One in-flight pxdb lookup per cache key, shared by every request that
/// needs it. Before this, every request that arrived while an entry was
/// expired ran its own lookup: at a 30s TTL under load that was a herd of
/// dozens of identical SELECTs per expiry -- and for keys dozens of
/// `last_used_at` UPDATEs serialised on one row, each waiting for the
/// previous one's fsync, which is how the 2026-08-22 soak wrote
/// `last_used_at` 103 726 times for 35 keys.
///
/// The lookup runs in its own task and applies its outcome to the cache
/// itself, so a caller that disconnects mid-way neither cancels it nor
/// strands the others; waiters only learn the outcome.
struct Flight<T> {
    done: Notify,
    /// Set once the task is over, result or not, BEFORE `done` fires. A
    /// waiter that starts polling after the wakeup (its `Notified` only
    /// exists from its first poll) reads this instead of waiting for a
    /// notification that already happened.
    finished: AtomicBool,
    result: Mutex<Option<T>>,
}

struct Flights<T> {
    map: Mutex<HashMap<String, Arc<Flight<T>>>>,
}

/// Removes the flight and wakes its waiters when the lookup task finishes --
/// or unwinds. A panic inside a lookup must not leave the key permanently
/// "in flight" with every later request parked on it.
struct FlightCleanup<T> {
    flights: Arc<Flights<T>>,
    key: String,
    flight: Arc<Flight<T>>,
}

impl<T> Drop for FlightCleanup<T> {
    fn drop(&mut self) {
        self.flights.map.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.key);
        self.flight.finished.store(true, Ordering::SeqCst);
        self.flight.done.notify_waiters();
    }
}

impl<T: Clone + Send + 'static> Flights<T> {
    fn new() -> Arc<Self> {
        Arc::new(Flights { map: Mutex::new(HashMap::new()) })
    }

    /// The flight for `key`: the one in progress, or a new task running
    /// `work`. Spawned synchronously, so a caller that only wants the lookup
    /// to happen (a fire-and-forget refresh) drops the handle and is done.
    fn start<F>(self: &Arc<Self>, key: &str, work: F) -> Arc<Flight<T>>
    where
        F: Future<Output = T> + Send + 'static,
    {
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(f) = map.get(key) {
            return f.clone();
        }
        let f = Arc::new(Flight {
            done: Notify::new(),
            finished: AtomicBool::new(false),
            result: Mutex::new(None),
        });
        map.insert(key.to_string(), f.clone());
        let cleanup = FlightCleanup { flights: self.clone(), key: key.to_string(), flight: f.clone() };
        tokio::spawn(async move {
            let out = work.await;
            *cleanup.flight.result.lock().unwrap_or_else(|e| e.into_inner()) = Some(out);
            drop(cleanup); // publishes: removes the entry, wakes the waiters
        });
        f
    }

    /// Join the lookup in flight for `key`, starting `work` if there is none.
    /// Resolves to `None` only if the task died without a result (a panic),
    /// which callers treat as "pxdb did not answer".
    fn join<F>(self: &Arc<Self>, key: &str, work: F) -> impl Future<Output = Option<T>>
    where
        F: Future<Output = T> + Send + 'static,
    {
        Self::wait(self.start(key, work))
    }

    async fn wait(flight: Arc<Flight<T>>) -> Option<T> {
        // Register for the wakeup BEFORE checking `finished`: notify_waiters
        // only reaches futures that already exist, and the task publishes,
        // flags and notifies back to back. A task already over is read from
        // the flag, result or not.
        let notified = flight.done.notified();
        if !flight.finished.load(Ordering::SeqCst) {
            notified.await;
        }
        flight.result.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

// ----------------------------------------------------------------- the cache

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
    host_flights: Arc<Flights<Lookup<Arc<ClusterCtx>>>>,
    key_flights: Arc<Flights<Lookup<KeyResult>>>,
    touched: Arc<Touched>,
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
            // dev-static has no plans table to read, so every feature is on —
            // it is a single-developer loopback mode, and the cloud path never
            // reaches this branch.
            features: Features {
                streams: true,
                traces: true,
                kv: true,
                timers: true,
                ephemeral: true,
            },
        });
        ClusterCache {
            dev_static,
            default_cluster: cfg.default_cluster.clone(),
            db,
            pxdb_cfg: cfg.pxdb.clone(),
            stale_grace: crate::config::stale_grace(),
            host_cache: Arc::new(RwLock::new(HashMap::new())),
            key_cache: Arc::new(RwLock::new(HashMap::new())),
            host_flights: Flights::new(),
            key_flights: Flights::new(),
            touched: Arc::new(Mutex::new(HashSet::new())),
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

    /// The shared body behind both lookups: `cache_key` names the entry,
    /// `sql`/`param` name the row. Three outcomes, in order:
    ///   * a fresh entry is served;
    ///   * an expired entry inside the grace window is served AS IS, and one
    ///     background refresh is started for it (stale-while-revalidate). Its
    ///     answer lands in the cache for the next request, so the TTL costs no
    ///     request its latency and no request a herd. A cluster pxdb has since
    ///     deleted is answered 421 one request later than before -- still
    ///     within the TTL, and the control-plane actions that must not wait
    ///     even that long arrive through NOTIFY invalidation;
    ///   * nothing servable: one lookup, shared by every request that needs
    ///     it, then the PLAN §2 rule (`fallback`) on its outcome.
    async fn resolve_keyed(
        &self,
        cache_key: String,
        sql: &'static str,
        param: String,
    ) -> Option<Arc<ClusterCtx>> {
        let pool = self.db.as_ref()?;
        let now = Instant::now();

        let stale = {
            let cache = self.host_cache.read().unwrap();
            match cache.get(&cache_key) {
                Some(e) if e.expires_at > now => return Some(e.ctx.clone()),
                Some(e) => Some((e.ctx.clone(), e.expires_at, e.refresh_after)),
                None => None,
            }
        };

        if let Some((ctx, expires_at, refresh_after)) = &stale {
            if fallback(Miss::NoAnswer, Some(*expires_at), now, self.stale_grace) == Fallback::ServeStale {
                if now >= *refresh_after {
                    // Fire-and-forget: one task per key, a second start is a no-op.
                    let work = host_lookup_work(pool, &self.host_cache, cache_key.clone(), sql, param);
                    self.host_flights.start(&cache_key, work);
                }
                return Some(ctx.clone());
            }
        }

        let looked = self
            .host_flights
            .join(&cache_key, host_lookup_work(pool, &self.host_cache, cache_key.clone(), sql, param))
            .await;
        let miss = match looked {
            Some(Lookup::Found(ctx)) => return Some(ctx),
            Some(Lookup::Absent) => Miss::NoSuchRow,
            Some(Lookup::Unavailable) | None => Miss::NoAnswer,
        };
        match fallback(miss, stale.as_ref().map(|(_, at, _)| *at), now, self.stale_grace) {
            // Served above in practice; kept so the decision stays the one
            // `fallback`'s tests pin.
            Fallback::ServeStale => stale.map(|(ctx, _, _)| ctx),
            Fallback::FailClosed | Fallback::Deny => None,
        }
    }

    /// Look up an API key by sha256 hash (hex). Returns the cluster and scopes.
    /// Same three-way shape as `resolve_keyed`; a stale negative keeps
    /// denying while its refresh runs, a stale positive is the fail-open of
    /// PLAN §2 one refresh away from being confirmed or withdrawn.
    pub async fn by_key_hash(&self, hash_hex: &str) -> Option<KeyResult> {
        let pool = self.db.as_ref()?;
        let now = Instant::now();

        let stale = {
            let cache = self.key_cache.read().unwrap();
            match cache.get(hash_hex) {
                Some(e) if e.expires_at > now => return e.value.clone(),
                Some(e) => Some(e.clone()),
                None => None,
            }
        };

        if let Some(e) = &stale {
            if fallback(Miss::NoAnswer, Some(e.expires_at), now, self.stale_grace) == Fallback::ServeStale {
                if now >= e.refresh_after {
                    let work = key_lookup_work(pool, &self.key_cache, &self.touched, hash_hex);
                    self.key_flights.start(hash_hex, work);
                }
                return e.value.clone();
            }
        }

        let looked = self
            .key_flights
            .join(hash_hex, key_lookup_work(pool, &self.key_cache, &self.touched, hash_hex))
            .await;
        let miss = match looked {
            Some(Lookup::Found(result)) => return Some(result),
            Some(Lookup::Absent) => Miss::NoSuchRow,
            Some(Lookup::Unavailable) | None => Miss::NoAnswer,
        };
        match fallback(miss, stale.as_ref().map(|e| e.expires_at), now, self.stale_grace) {
            Fallback::ServeStale => stale.and_then(|e| e.value),
            Fallback::FailClosed | Fallback::Deny => None,
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

    /// Batched `api_keys.last_used_at` writer: a key is "touched" when a
    /// lookup finds it (once per TTL per key, thanks to single-flight), and
    /// the set is written as ONE statement every QUEEN_PROXY_KEY_TOUCH_MS,
    /// off every request path. Best-effort like the inline UPDATE it
    /// replaces: a failed flush is dropped and the next refresh re-touches.
    /// No-op without a pxdb (dev-static).
    pub fn spawn_touch_flush(&self) {
        let Some(pool) = self.db.clone() else { return };
        let touched = self.touched.clone();
        tokio::spawn(async move {
            let every = Duration::from_millis(
                crate::config::env_u64("QUEEN_PROXY_KEY_TOUCH_MS", KEY_TOUCH_FLUSH_MS).max(1_000),
            );
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                flush_touched(&pool, &touched).await;
            }
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

    // `last_used_at` is NOT bumped here: `apply_key_lookup` records the key
    // and `spawn_touch_flush` writes the set in one statement per interval.
    Lookup::Found(result)
}

// ------------------------------------------------------ lookup tasks

/// The body of one host flight: look the row up, apply the outcome to the
/// cache, hand the outcome to whoever is waiting. Runs in its own task.
fn host_lookup_work(
    pool: &deadpool_postgres::Pool,
    cache: &Arc<HostMap>,
    cache_key: String,
    sql: &'static str,
    param: String,
) -> impl Future<Output = Lookup<Arc<ClusterCtx>>> + Send + 'static {
    let pool = pool.clone();
    let cache = cache.clone();
    async move {
        let looked = lookup_host(&pool, sql, &param).await;
        apply_host_lookup(&cache, &cache_key, &looked);
        looked
    }
}

/// Same, for one key flight.
fn key_lookup_work(
    pool: &deadpool_postgres::Pool,
    cache: &Arc<KeyMap>,
    touched: &Arc<Touched>,
    hash_hex: &str,
) -> impl Future<Output = Lookup<KeyResult>> + Send + 'static {
    let pool = pool.clone();
    let cache = cache.clone();
    let touched = touched.clone();
    let hash = hash_hex.to_string();
    async move {
        let looked = lookup_key(&pool, &hash).await;
        apply_key_lookup(&cache, &touched, &hash, &looked);
        looked
    }
}

// ------------------------------------------------------ outcome -> cache

/// Apply a host lookup's outcome. Runs inside the single-flight task, so it
/// happens exactly once per lookup whoever was waiting.
fn apply_host_lookup(cache: &HostMap, key: &str, looked: &Lookup<Arc<ClusterCtx>>) {
    match looked {
        Lookup::Found(ctx) => {
            let now = Instant::now();
            cache.write().unwrap().insert(
                key.to_string(),
                HostEntry { ctx: ctx.clone(), expires_at: now + HOST_TTL, refresh_after: now + HOST_TTL },
            );
        }
        // pxdb answered "no such row": forget any expired entry so a later
        // outage can't resurrect a deleted cluster through the grace window.
        // Only when there is something to forget -- a flood of garbage Host
        // headers (the common 421 case) must not take the write lock.
        Lookup::Absent => {
            if cache.read().unwrap().contains_key(key) {
                cache.write().unwrap().remove(key);
            }
        }
        // No answer: the entry (if any) stays as the last known-good, and is
        // not refreshed again before REFRESH_BACKOFF.
        Lookup::Unavailable => {
            if !cache.read().unwrap().contains_key(key) {
                return;
            }
            let now = Instant::now();
            let mut w = cache.write().unwrap();
            let Some(e) = w.get_mut(key) else { return };
            e.refresh_after = (now + REFRESH_BACKOFF).max(e.refresh_after);
            drop(w);
            if stale_log_due(now) {
                tracing::warn!(
                    slug = %key,
                    "pxdb unreachable: serving cluster from expired cache (fail-open, PLAN §2)"
                );
            }
        }
    }
}

/// Apply a key lookup's outcome; same contract as `apply_host_lookup`.
fn apply_key_lookup(cache: &KeyMap, touched: &Touched, hash_hex: &str, looked: &Lookup<KeyResult>) {
    match looked {
        Lookup::Found(result) => {
            let now = Instant::now();
            cache.write().unwrap().insert(
                hash_hex.to_string(),
                KeyEntry {
                    value: Some(result.clone()),
                    expires_at: now + KEY_POSITIVE_TTL,
                    refresh_after: now + KEY_POSITIVE_TTL,
                },
            );
            touched.lock().unwrap().insert(result.1);
        }
        // pxdb answered: this hash is unknown or revoked. The negative entry
        // both denies and replaces any expired positive, so the grace window
        // can never resurrect a revoked key.
        //
        // BOUNDED, unlike the positive side: the key is a sha256 of whatever
        // the caller presented, so an unauthenticated caller chooses it, and
        // nothing else ever removes these — `invalidate_caches` deliberately
        // retains them (a cluster's invalidation has nothing to say about a
        // garbage hash) and there is no TTL sweeper. Measured before this cap:
        // 20 000 distinct garbage bearer tokens grew the process by ~4.6 MB
        // (~230 B/key) and a sustained single-connection flood reached 99 MB in
        // about two minutes, monotonically. The shared host is what makes it
        // reachable with no Host gate in front (there, the key lookup IS the
        // routing decision), and `limits.check_req` runs after authorize, so
        // nothing else bounds it.
        //
        // `apply_host_lookup` answers the same problem by not caching a miss at
        // all. Here the miss is worth caching — without it every request
        // carrying a garbage key is a pxdb round trip — so it is capped
        // instead: expired negatives are dropped first (they are only worth one
        // avoided lookup each), and if the cap still holds, this one is simply
        // not cached. The request is denied either way; only the memo is lost.
        Lookup::Absent => {
            let now = Instant::now();
            let mut w = cache.write().unwrap();
            let entry =
                KeyEntry { value: None, expires_at: now + KEY_NEGATIVE_TTL, refresh_after: now + KEY_NEGATIVE_TTL };
            // Replacing an entry (including an expired POSITIVE one, which is
            // the revoked-key case) never grows the map, so it is never capped.
            if w.contains_key(hash_hex) {
                w.insert(hash_hex.to_string(), entry);
                return;
            }
            // The O(1) length check is the trigger; the pass below is the only
            // O(n) work, and it both prunes and counts, so a request never
            // walks the map twice. Under a flood it runs once per ~TTL worth of
            // keys (the entries it drops are the expired ones), not once per
            // request.
            if w.len() >= KEY_NEGATIVE_MAX {
                let mut negatives = 0usize;
                // Positives past their TTL are KEPT: the stale-while-revalidate
                // grace window (`fallback`) is what serves them through a pxdb
                // outage, and dropping them here would turn a flood into a
                // fail-CLOSED for live keys. Only expired negatives go, and
                // only they are counted against the cap.
                w.retain(|_, e| {
                    if e.value.is_some() {
                        return true;
                    }
                    if e.expires_at <= now {
                        return false;
                    }
                    negatives += 1;
                    true
                });
                if negatives >= KEY_NEGATIVE_MAX {
                    tracing::debug!(
                        negatives,
                        "negative api-key cache at its cap; denying without caching"
                    );
                    return;
                }
            }
            w.insert(hash_hex.to_string(), entry);
        }
        Lookup::Unavailable => {
            if !cache.read().unwrap().contains_key(hash_hex) {
                return;
            }
            let now = Instant::now();
            let mut w = cache.write().unwrap();
            let Some(e) = w.get_mut(hash_hex) else { return };
            e.refresh_after = (now + REFRESH_BACKOFF).max(e.refresh_after);
            let positive = e.value.is_some();
            drop(w);
            // Only a positive entry is a fail-open worth reporting; a stale
            // negative just keeps denying.
            if positive && stale_log_due(now) {
                tracing::warn!("pxdb unreachable: serving api key from expired cache (fail-open, PLAN §2)");
            }
        }
    }
}

/// One statement for every key touched since the last tick.
async fn flush_touched(pool: &deadpool_postgres::Pool, touched: &Touched) {
    let ids: Vec<String> = {
        let mut set = touched.lock().unwrap();
        if set.is_empty() {
            return;
        }
        set.drain().map(|id| id.to_string()).collect()
    };
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!(error = %e, keys = ids.len(), "last_used_at flush: pxdb unavailable (non-fatal)");
            return;
        }
    };
    if let Err(e) = client
        .execute(
            "UPDATE queen_proxy.api_keys SET last_used_at = now() WHERE id = ANY($1::text[]::uuid[])",
            &[&ids],
        )
        .await
    {
        tracing::debug!(error = %e, keys = ids.len(), "last_used_at flush failed (non-fatal)");
    }
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
        .application_name("queen-proxy-listen")
        .connect_timeout(pxcfg.timeout());
    let listen_stmt = format!("LISTEN {INVAL_CHANNEL}");

    if pxcfg.use_ssl {
        // Same trust as the pool, from the same validated material — a LISTEN
        // connection that trusted a different root set than the pool would be
        // the worst kind of divergence: invalidations arriving over a link
        // nobody audited.
        let connector = crate::pgtls::make_connector(
            pxcfg.ssl_reject_unauthorized,
            pxcfg.ssl_root_cert.as_deref(),
        )
        .map_err(|e| format!("PXDB_SSL_ROOT_CERT: {e}"))?;
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
        // PLAN_KV_TIMERS.md §9.8 P1. Same "missing = false" rule as its
        // neighbours, and here it is load-bearing rather than tidy: the cell
        // where these are wanted is the one whose plan row is updated to say
        // so, and nowhere else. No migration, no default-on.
        kv: v.get("kv").and_then(|b| b.as_bool()).unwrap_or(false),
        timers: v.get("timers").and_then(|b| b.as_bool()).unwrap_or(false),
        // EPHEMERAL_QUEUES.md §5.1: same rule again, and the reason the family
        // ships "OSS on, cloud off" without a second mechanism — the plan row
        // that does not name it is the plan that does not have it.
        ephemeral: v.get("ephemeral").and_then(|b| b.as_bool()).unwrap_or(false),
    }
}

/// First DNS label of a Host header, with the port and the DNS root label
/// stripped. Lowercased (DNS is case-insensitive; clusters.slug is stored
/// lowercase). The strip is `config::canonical_host` — the SAME one
/// `Config::is_shared_host` uses, so a host cannot be shared for one of them
/// and a slug for the other.
fn slug_from_host(host: &str) -> Option<String> {
    let host = host.trim();
    if host.is_empty() {
        return None;
    }
    let without_port = crate::config::canonical_host(host);
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
        // Fully-qualified: the same name, so the same slug. (The first label
        // was never affected by the root dot; this pins that the shared
        // `canonical_host` did not change it either.)
        assert_eq!(slug_from_host("acme.eu1.queenmq.cloud.").as_deref(), Some("acme"));
        assert_eq!(slug_from_host("acme.:6711").as_deref(), Some("acme"));
        assert_eq!(slug_from_host("."), None);
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
        let gate = LogGate::new();
        let t0 = Instant::now();
        assert!(gate.due(t0), "first line is always due");
        assert!(!gate.due(t0), "a second stale serve in the same instant is sampled out");
        assert!(gate.due(t0 + STALE_LOG_INTERVAL), "due again once the interval elapses");
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

    /// PLAN_KV_TIMERS.md §9.8 P1: a plan that has never heard of kv/timers —
    /// which is every plan row on every cell today — denies both.
    #[test]
    fn kv_and_timers_default_off_and_are_independent() {
        let f = parse_features(r#"{"streams":true,"traces":true}"#);
        assert!(!f.kv, "a plan that does not mention kv does not have kv");
        assert!(!f.timers);

        let f = parse_features(r#"{"timers":true}"#);
        assert!(f.timers);
        assert!(!f.kv, "the two features ship and sell separately (§16)");

        // Junk in the JSONB is not a yes.
        let f = parse_features(r#"{"kv":"true","timers":1}"#);
        assert!(!f.kv);
        assert!(!f.timers);
        assert!(!parse_features("not json").kv);
    }

    /// EPHEMERAL_QUEUES.md §5.1/§8: the RAM family is default-off in the
    /// cloud, and this is where that is decided — the 403 `feature_gated` a
    /// tenant gets from gateway.rs is this `false` travelling downstream.
    #[test]
    fn ephemeral_defaults_off_and_is_independent() {
        for json in ["{}", r#"{"streams":true,"traces":true,"kv":true,"timers":true}"#, "not json"] {
            assert!(!parse_features(json).ephemeral, "features: {json}");
        }
        let f = parse_features(r#"{"ephemeral":true}"#);
        assert!(f.ephemeral);
        assert!(!f.kv, "the RAM class is sold on its own, not with kv");
        // Junk in the JSONB is not a yes here either.
        assert!(!parse_features(r#"{"ephemeral":"true"}"#).ephemeral);
        assert!(!parse_features(r#"{"ephemeral":1}"#).ephemeral);
    }

    // ---- single-flight + outcome application ----

    fn ctx(id: u128) -> Arc<ClusterCtx> {
        Arc::new(ClusterCtx {
            cluster_id: Uuid::from_u128(id),
            tenant_id: Uuid::from_u128(id),
            broker_tenant: Uuid::from_u128(id),
            slug: format!("c{id}"),
            cell_base_url: "http://127.0.0.1:1".to_string(),
            cell_token: None,
            status: ClusterStatus::Active,
            limits: EffectiveLimits::default(),
            features: Features::default(),
        })
    }

    #[tokio::test]
    async fn flights_run_one_lookup_per_key_and_share_the_result() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let flights: Arc<Flights<u32>> = Flights::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let mut waiters = Vec::new();
        for _ in 0..32 {
            let runs = runs.clone();
            waiters.push(flights.join("k", async move {
                runs.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                7
            }));
        }
        for w in waiters {
            assert_eq!(w.await, Some(7));
        }
        assert_eq!(runs.load(Ordering::SeqCst), 1, "32 concurrent callers, one lookup");
        assert!(flights.map.lock().unwrap().is_empty(), "released once published");
        assert_eq!(flights.join("other", async { 9 }).await, Some(9), "keys do not share");
    }

    #[tokio::test]
    async fn a_fire_and_forget_refresh_still_runs() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let flights: Arc<Flights<u32>> = Flights::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let r = runs.clone();
        drop(flights.start("k", async move {
            r.fetch_add(1, Ordering::SeqCst);
            1
        }));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 1, "the task was spawned before the future was dropped");
        assert!(flights.map.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_lookup_that_panics_releases_the_key_and_answers_none() {
        let flights: Arc<Flights<u32>> = Flights::new();
        let w1 = flights.join("k", async { panic!("lookup task died") });
        let w2 = flights.join("k", async { 1 }); // joins the doomed flight
        assert_eq!(w1.await, None);
        assert_eq!(w2.await, None);
        assert!(flights.map.lock().unwrap().is_empty(), "a dead flight must not pin the key");
        assert_eq!(flights.join("k", async { 2 }).await, Some(2), "the key is usable again");
    }

    #[test]
    fn host_outcomes_found_unavailable_absent() {
        let cache: Arc<HostMap> = Arc::new(RwLock::new(HashMap::new()));
        apply_host_lookup(&cache, "acme", &Lookup::Found(ctx(1)));
        assert!(cache.read().unwrap()["acme"].expires_at > Instant::now());

        // Expire it by hand, as it would be when a refresh runs for it.
        let past = Instant::now() - Duration::from_secs(1);
        {
            let mut w = cache.write().unwrap();
            let e = w.get_mut("acme").unwrap();
            e.expires_at = past;
            e.refresh_after = past;
        }
        apply_host_lookup(&cache, "acme", &Lookup::Unavailable);
        assert!(cache.read().unwrap().contains_key("acme"), "kept: it is the fail-open material");
        assert!(cache.read().unwrap()["acme"].refresh_after > Instant::now(), "no second refresh inside the backoff");

        apply_host_lookup(&cache, "acme", &Lookup::Absent);
        assert!(!cache.read().unwrap().contains_key("acme"), "pxdb said no: nothing left to resurrect");
        apply_host_lookup(&cache, "garbage", &Lookup::Absent);
        assert!(!cache.read().unwrap().contains_key("garbage"), "unknown hosts are never cached");
    }

    #[test]
    fn key_outcomes_touch_positives_and_cache_negatives() {
        let cache: Arc<KeyMap> = Arc::new(RwLock::new(HashMap::new()));
        let touched: Arc<Touched> = Arc::new(Mutex::new(HashSet::new()));
        let key_id = Uuid::from_u128(7);
        apply_key_lookup(&cache, &touched, "h1", &Lookup::Found((ctx(1), key_id, Scopes::all())));
        assert!(cache.read().unwrap()["h1"].value.is_some());
        assert!(touched.lock().unwrap().contains(&key_id), "last_used_at is written by the batch");

        // Revoked since: the negative replaces the positive, so no grace
        // window can serve it again.
        apply_key_lookup(&cache, &touched, "h1", &Lookup::Absent);
        let e = cache.read().unwrap()["h1"].clone();
        assert!(e.value.is_none());
        assert!(e.expires_at <= Instant::now() + KEY_NEGATIVE_TTL);

        // Unknown hash: a negative too, re-checked at the anti-brute-force cadence.
        apply_key_lookup(&cache, &touched, "h2", &Lookup::Absent);
        assert!(cache.read().unwrap()["h2"].value.is_none());

        // No answer for a stale negative: it stays, backed off, and nothing is touched.
        apply_key_lookup(&cache, &touched, "h2", &Lookup::Unavailable);
        assert!(cache.read().unwrap()["h2"].value.is_none());
        assert_eq!(touched.lock().unwrap().len(), 1);
    }

    #[test]
    fn a_flood_of_unknown_keys_cannot_grow_the_cache_without_bound() {
        // The cache key is a sha256 of what the CALLER presented, so its
        // cardinality is chosen by an unauthenticated client — on a shared host
        // with no Host gate in front of the lookup. Measured before the cap:
        // ~230 B per distinct garbage token, growing monotonically for as long
        // as the flood lasted.
        let cache: Arc<KeyMap> = Arc::new(RwLock::new(HashMap::new()));
        let touched: Arc<Touched> = Arc::new(Mutex::new(HashSet::new()));

        // A live key, cached before the flood starts.
        let key_id = Uuid::from_u128(7);
        apply_key_lookup(&cache, &touched, "live", &Lookup::Found((ctx(1), key_id, Scopes::all())));

        for i in 0..(KEY_NEGATIVE_MAX * 2) {
            apply_key_lookup(&cache, &touched, &format!("{i:064x}"), &Lookup::Absent);
        }
        let map = cache.read().unwrap();
        let negatives = map.values().filter(|e| e.value.is_none()).count();
        assert!(negatives <= KEY_NEGATIVE_MAX, "negatives unbounded: {negatives}");
        // The flood must not be able to evict a live key: that would turn a
        // memory problem into a fail-closed one for real traffic.
        assert!(map["live"].value.is_some(), "the flood evicted a live key");
    }

    #[test]
    fn re_denying_a_hash_already_cached_is_never_capped() {
        // One broken client retrying the same wrong key forever occupies one
        // entry, so it must keep being memoised however full the map is —
        // otherwise the cap turns exactly that case into a pxdb round trip per
        // request.
        let cache: Arc<KeyMap> = Arc::new(RwLock::new(HashMap::new()));
        let touched: Arc<Touched> = Arc::new(Mutex::new(HashSet::new()));
        for i in 0..KEY_NEGATIVE_MAX {
            apply_key_lookup(&cache, &touched, &format!("{i:064x}"), &Lookup::Absent);
        }
        let hash = format!("{:064x}", 3);
        apply_key_lookup(&cache, &touched, &hash, &Lookup::Absent);
        assert!(cache.read().unwrap().contains_key(&hash));
        // And a REVOKED key still replaces its positive at the cap, or the
        // grace window would keep serving it.
        let key_id = Uuid::from_u128(9);
        apply_key_lookup(&cache, &touched, "revoked", &Lookup::Found((ctx(2), key_id, Scopes::all())));
        apply_key_lookup(&cache, &touched, "revoked", &Lookup::Absent);
        assert!(cache.read().unwrap()["revoked"].value.is_none(), "a revoked key must not survive");
    }
}
