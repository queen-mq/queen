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
    db: Option<deadpool_postgres::Pool>,
    /// Cloned at construction so `spawn_listener` can open its own dedicated
    /// LISTEN connection later. The pool can't be reused for this: deadpool
    /// drives each pooled connection on its own background task and
    /// discards `AsyncMessage::Notification` (see tokio-postgres's
    /// LISTEN/NOTIFY docs -- a dedicated `Connection` object, polled by
    /// hand, is the documented pattern).
    pxdb_cfg: Option<PxdbConfig>,
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
            db,
            pxdb_cfg: cfg.pxdb.clone(),
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
        let pool = self.db.as_ref()?;
        let slug = slug_from_host(host)?;

        {
            let cache = self.host_cache.read().unwrap();
            if let Some(entry) = cache.get(&slug) {
                if entry.expires_at > Instant::now() {
                    return Some(entry.ctx.clone());
                }
            }
        }

        let client = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "resolve_host: pxdb pool.get failed");
                return None;
            }
        };
        let row_opt = match client.query_opt(RESOLVE_HOST_SQL, &[&slug]).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, slug = %slug, "resolve_host: query failed");
                return None;
            }
        };
        let row = row_opt?;
        let ctx = match ctx_from_row(&row) {
            Ok(c) => Arc::new(c),
            Err(e) => {
                tracing::error!(error = %e, slug = %slug, "resolve_host: malformed row");
                return None;
            }
        };

        let mut cache = self.host_cache.write().unwrap();
        cache.insert(slug, HostEntry { ctx: ctx.clone(), expires_at: Instant::now() + HOST_TTL });
        drop(cache);

        Some(ctx)
    }

    /// Look up an API key by sha256 hash (hex). Returns the cluster and scopes.
    pub async fn by_key_hash(&self, hash_hex: &str) -> Option<(Arc<ClusterCtx>, Uuid, Scopes)> {
        let pool = self.db.as_ref()?;

        {
            let cache = self.key_cache.read().unwrap();
            if let Some(entry) = cache.get(hash_hex) {
                if entry.expires_at > Instant::now() {
                    return entry.value.clone();
                }
            }
        }

        let client = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "by_key_hash: pxdb pool.get failed");
                return None;
            }
        };
        let row_opt = match client.query_opt(BY_KEY_HASH_SQL, &[&hash_hex]).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "by_key_hash: query failed");
                return None;
            }
        };

        let result = match row_opt {
            None => None,
            Some(row) => match build_key_result(&row) {
                Ok(r) => Some(r),
                Err(e) => {
                    tracing::error!(error = %e, "by_key_hash: malformed row");
                    None
                }
            },
        };

        if let Some((_, key_id, _)) = &result {
            // Best-effort last_used_at bump -- never fail the auth call over
            // it. Only runs on a cache MISS (once per positive-TTL window
            // per key), not per request.
            let key_id_str = key_id.to_string();
            if let Err(e) = client
                .execute(
                    "UPDATE queen_proxy.api_keys SET last_used_at = now() WHERE id = $1::text::uuid",
                    &[&key_id_str],
                )
                .await
            {
                tracing::debug!(error = %e, "by_key_hash: last_used_at update failed (non-fatal)");
            }
        }

        let ttl = if result.is_some() { KEY_POSITIVE_TTL } else { KEY_NEGATIVE_TTL };
        let mut cache = self.key_cache.write().unwrap();
        cache.insert(
            hash_hex.to_string(),
            KeyEntry { value: result.clone(), expires_at: Instant::now() + ttl },
        );
        drop(cache);

        result
    }

    /// Invalidate a cluster (NOTIFY payload or admin action).
    pub fn invalidate(&self, cluster_id: Uuid) {
        invalidate_caches(&self.host_cache, &self.key_cache, cluster_id);
    }

    /// Spawn the LISTEN task. No-op in dev-static mode (matches the doc
    /// comment on the skeleton) and also when there's simply no pxdb
    /// configured. Note this takes `&self`, not `self: &Arc<Self>` -- the
    /// skeleton declared the latter, but `AppState` stores `cache` as a
    /// plain field (not `Arc<ClusterCache>`), so nothing could ever have
    /// called it that way. See the report for the main.rs wiring this
    /// still needs (nothing calls spawn_listener/spawn_reconciler today).
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
        let (client, mut connection) = pg.connect(connector).await.map_err(|e| format!("connect: {e}"))?;
        client.batch_execute(&listen_stmt).await.map_err(|e| format!("LISTEN: {e}"))?;
        tracing::info!(channel = INVAL_CHANNEL, tls = true, "queen_proxy_inval listener connected");
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(tokio_postgres::AsyncMessage::Notification(n))) => {
                    handle_notification(&n, host_cache, key_cache);
                }
                Some(Ok(_)) => {} // notices etc., nothing to do
                Some(Err(e)) => return Err(format!("connection error: {e}")),
                None => return Err("connection closed".to_string()),
            }
        }
    } else {
        let (client, mut connection) =
            pg.connect(tokio_postgres::NoTls).await.map_err(|e| format!("connect: {e}"))?;
        client.batch_execute(&listen_stmt).await.map_err(|e| format!("LISTEN: {e}"))?;
        tracing::info!(channel = INVAL_CHANNEL, tls = false, "queen_proxy_inval listener connected");
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(tokio_postgres::AsyncMessage::Notification(n))) => {
                    handle_notification(&n, host_cache, key_cache);
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(format!("connection error: {e}")),
                None => return Err("connection closed".to_string()),
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

const RESOLVE_HOST_SQL: &str = "
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
    WHERE c.slug = $1";

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
