//! Queue/partition registry: plan-cap admission for implicit creation (push
//! auto-creates!) and /configure, plus the periodic reconciler against the
//! broker's (Track B: tenant-scoped) inventory. OWNER: Agent B.
//!
//! `admit()`'s in-process state tracks two things per cluster, deliberately
//! separate:
//!   * `pairs` -- exact (queue, partition) tuples THIS PROCESS has admitted.
//!     This is the literal fast path ("known pair -> Allowed, no DB") and
//!     the only place individual partition identity lives at all.
//!   * `db_partition_floor` -- the last-known partitions_count per queue,
//!     seeded from queen_proxy.queues on lazy load and kept in step by the
//!     reconciler. This exists because queen_proxy.queues (001_init.sql)
//!     only ever stores a COUNT, never partition names (and neither does
//!     the broker's own resources/queues response) -- so after a proxy
//!     restart `pairs` starts empty and there is no way to repopulate exact
//!     historical partition names from the DB. `db_partition_floor` is the
//!     restart-safety net: the effective partition count used for the cap
//!     check is `max(observed in `pairs`, db_partition_floor)`, so a
//!     restart can't be used to bypass max_partitions_per_queue before the
//!     reconciler's next pass (or fresh traffic) catches back up.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use uuid::Uuid;

use crate::state::ClusterCtx;

const RECONCILE_INTERVAL: Duration = Duration::from_secs(60);
const RECONCILE_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
/// Cap on a single cell's resources/queues response body -- generous for a
/// cell with many thousands of queues while still bounded against a
/// misbehaving broker.
const MAX_RECONCILE_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum Admit {
    Allowed,
    OverQueues { max: i64 },
    OverPartitions { max: i64 },
}

#[derive(Default, Debug)]
struct ClusterRegistry {
    /// Exact (queue, partition) pairs admitted this process lifetime.
    pairs: HashSet<(String, String)>,
    /// queue name -> last-known partitions_count from the DB (lazy-load or
    /// reconciler). A floor, not a ceiling -- see module doc.
    db_partition_floor: HashMap<String, i64>,
    /// Queue names known to exist (DB lazy-load, reconciler, or this
    /// process's own admits) -- backs the max_queues check.
    queue_names: HashSet<String>,
}

pub struct Registry {
    db: Option<deadpool_postgres::Pool>,
    known: Arc<RwLock<HashMap<Uuid, ClusterRegistry>>>,
    /// Clusters whose `known` entry has been bootstrapped from the DB at
    /// least once -- separate from `known.contains_key` because an entry
    /// gets created on a cache MISS too (see `admit`), before any DB load.
    loaded: Arc<RwLock<HashSet<Uuid>>>,
    /// Clusters currently believed to be over their storage quota, per the
    /// reconciler's last successful byte count. See `over_storage` and the
    /// report for the (currently unwired) consumer.
    over_storage: Arc<RwLock<HashSet<Uuid>>>,
}

impl Registry {
    pub fn new(db: Option<deadpool_postgres::Pool>) -> Registry {
        Registry {
            db,
            known: Arc::new(RwLock::new(HashMap::new())),
            loaded: Arc::new(RwLock::new(HashSet::new())),
            over_storage: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Called with every (queue, partition) named in a produce/configure request.
    /// Fast path: known pair -> Allowed without touching the DB.
    pub async fn admit(&self, ctx: &ClusterCtx, queue: &str, partition: &str) -> Admit {
        self.ensure_loaded(ctx.cluster_id).await;

        let key = (queue.to_string(), partition.to_string());

        // Fast path: exact pair already known for this cluster.
        {
            let map = self.known.read().unwrap();
            if let Some(cr) = map.get(&ctx.cluster_id) {
                if cr.pairs.contains(&key) {
                    return Admit::Allowed;
                }
            }
        }

        // Miss: evaluate caps and admit under one write-lock critical
        // section (dropped before any .await below) so "count, then
        // insert" can't race with itself within this process. Cross-process
        // races don't apply today (one proxy per cell, PLAN §2); a race
        // against the periodic reconciler can cause brief overshoot, healed
        // on the next pass -- accepted per spec.
        let persisted = {
            let mut map = self.known.write().unwrap();
            let cr = map.entry(ctx.cluster_id).or_default();

            // Someone else may have admitted this exact pair while we
            // waited for the write lock.
            if cr.pairs.contains(&key) {
                return Admit::Allowed;
            }

            let is_new_queue = !cr.queue_names.contains(queue);
            if let Some(max_q) = ctx.limits.max_queues {
                let projected_queues = cr.queue_names.len() as i64 + if is_new_queue { 1 } else { 0 };
                if projected_queues > max_q {
                    return Admit::OverQueues { max: max_q };
                }
            }

            let observed = cr.pairs.iter().filter(|(q, _)| q == queue).count() as i64;
            let floor = cr.db_partition_floor.get(queue).copied().unwrap_or(0);
            let projected_partitions = observed.max(floor) + 1;
            if let Some(max_p) = ctx.limits.max_partitions_per_queue {
                if projected_partitions > max_p {
                    return Admit::OverPartitions { max: max_p };
                }
            }

            cr.pairs.insert(key);
            cr.queue_names.insert(queue.to_string());
            (queue.to_string(), projected_partitions)
        };

        self.upsert_queue_row(ctx.cluster_id, &persisted.0, persisted.1).await;
        Admit::Allowed
    }

    /// Bootstrap a cluster's known-queue-names + partition floor from
    /// queen_proxy.queues, once. No-op (and harmless) with no DB (dev-static).
    async fn ensure_loaded(&self, cluster_id: Uuid) {
        if self.loaded.read().unwrap().contains(&cluster_id) {
            return;
        }
        let mut cr = ClusterRegistry::default();
        if let Some(pool) = &self.db {
            match pool.get().await {
                Ok(client) => {
                    let cluster_id_str = cluster_id.to_string();
                    let stmt = "SELECT name, partitions_count FROM queen_proxy.queues \
                                WHERE cluster_id = $1::text::uuid AND deleted_at IS NULL";
                    match client.query(stmt, &[&cluster_id_str]).await {
                        Ok(rows) => {
                            for r in rows {
                                let name: String = r.get(0);
                                let count: i32 = r.get(1);
                                cr.queue_names.insert(name.clone());
                                cr.db_partition_floor.insert(name, count as i64);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(cluster = %cluster_id, error = %e, "registry: lazy-load query failed");
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(cluster = %cluster_id, error = %e, "registry: lazy-load pool.get failed");
                }
            }
        }
        {
            let mut map = self.known.write().unwrap();
            map.entry(cluster_id).or_insert(cr);
        }
        self.loaded.write().unwrap().insert(cluster_id);
    }

    async fn upsert_queue_row(&self, cluster_id: Uuid, queue: &str, partitions_count: i64) {
        let Some(pool) = &self.db else { return };
        let client = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(cluster = %cluster_id, error = %e, "registry: admit upsert pool.get failed");
                return;
            }
        };
        let cluster_id_str = cluster_id.to_string();
        let stmt = "INSERT INTO queen_proxy.queues(cluster_id, name, partitions_count) \
                    VALUES ($1::text::uuid, $2, $3) \
                    ON CONFLICT (cluster_id, name) WHERE deleted_at IS NULL \
                    DO UPDATE SET partitions_count = GREATEST(queen_proxy.queues.partitions_count, EXCLUDED.partitions_count)";
        if let Err(e) = client.execute(stmt, &[&cluster_id_str, &queue, &partitions_count]).await {
            tracing::warn!(cluster = %cluster_id, queue, error = %e, "registry: admit upsert failed");
        }
    }

    /// Clusters currently believed over their plan's max_retained_bytes, per
    /// the reconciler's last successful byte count for that cluster.
    ///
    /// WIRING GAP (see report): nothing calls this yet. The intended
    /// consumer is a periodic task that reads this list and calls
    /// `st.limits.set_push_blocked(id, true)` for members (and false for
    /// clusters that dropped out of it) -- Registry can't do that itself,
    /// it has no reference to `Limits` (nor should it grow one without
    /// orchestrator sign-off, since AppState's shape is frozen). Also note:
    /// as of this writing this set is ALWAYS EMPTY in practice, because the
    /// broker's resources/queues response has no bytes field yet to sum --
    /// see reconcile_cluster's doc comment.
    pub fn over_storage(&self) -> Vec<Uuid> {
        self.over_storage.read().unwrap().iter().copied().collect()
    }

    pub fn spawn_reconciler(&self) {
        let Some(pool) = self.db.clone() else {
            tracing::info!("registry reconciler: no pxdb configured, skipping (dev-static mode)");
            return;
        };
        let known = self.known.clone();
        let over_storage = self.over_storage.clone();
        tokio::spawn(async move {
            // env-tunable so e2e smokes don't wait a full minute per cycle
            let interval = std::time::Duration::from_millis(crate::config::env_u64(
                "QUEEN_PROXY_RECONCILE_MS",
                RECONCILE_INTERVAL.as_millis() as u64,
            ));
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                reconcile_once(&pool, &known, &over_storage).await;
            }
        });
    }

    #[allow(dead_code)]
    pub fn forget(&self, cluster_id: Uuid) {
        self.known.write().unwrap().remove(&cluster_id);
        self.loaded.write().unwrap().remove(&cluster_id);
    }
}

// ------------------------------------------------------------- reconciler

struct ReconcileTarget {
    cluster_id: Uuid,
    broker_tenant: String,
    base_url: String,
    cell_secret: Option<String>,
    max_retained_bytes: Option<i64>,
}

async fn reconcile_once(
    pool: &deadpool_postgres::Pool,
    known: &Arc<RwLock<HashMap<Uuid, ClusterRegistry>>>,
    over_storage: &Arc<RwLock<HashSet<Uuid>>>,
) {
    let targets = match load_targets(pool).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "registry reconciler: failed to list clusters, skipping cycle");
            return;
        }
    };
    for target in targets {
        reconcile_cluster(pool, known, over_storage, &target).await;
    }
}

async fn load_targets(pool: &deadpool_postgres::Pool) -> Result<Vec<ReconcileTarget>, String> {
    let client = pool.get().await.map_err(|e| format!("pool.get: {e}"))?;
    // Reconcile everything not being torn down -- push_blocked clusters
    // especially still need this loop (it's how their byte count, and thus
    // push_blocked itself once §wiring lands, gets re-evaluated).
    let stmt = "SELECT c.id::text, c.broker_tenant_uuid::text, ce.base_url, ce.cell_secret, \
                       p.max_retained_bytes, (c.limit_overrides)::text \
                FROM queen_proxy.clusters c \
                JOIN queen_proxy.cells  ce ON ce.id = c.cell_id \
                JOIN queen_proxy.plans  p  ON p.id = c.plan_id \
                WHERE c.status <> 'deleting'";
    let rows = client.query(stmt, &[]).await.map_err(|e| format!("query: {e}"))?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let id_str: String = row.get(0);
        let Ok(cluster_id) = Uuid::parse_str(&id_str) else {
            tracing::warn!(id = %id_str, "registry reconciler: unparseable cluster id, skipping");
            continue;
        };
        let broker_tenant: String = row.get(1);
        let base_url: String = row.get(2);
        let cell_secret: Option<String> = row.get(3);
        let max_retained_plan: Option<i64> = row.get(4);
        let overrides_json: String = row.get(5);
        let overrides: serde_json::Value = serde_json::from_str(&overrides_json).unwrap_or(serde_json::Value::Null);
        let max_retained_bytes = crate::cache::override_or(&overrides, "max_retained_bytes", max_retained_plan);
        out.push(ReconcileTarget { cluster_id, broker_tenant, base_url, cell_secret, max_retained_bytes });
    }
    Ok(out)
}

async fn reconcile_cluster(
    pool: &deadpool_postgres::Pool,
    known: &Arc<RwLock<HashMap<Uuid, ClusterRegistry>>>,
    over_storage: &Arc<RwLock<HashSet<Uuid>>>,
    target: &ReconcileTarget,
) {
    let url = format!("{}/api/v1/resources/queues", target.base_url.trim_end_matches('/'));
    let auth_header = target.cell_secret.as_ref().map(|secret| format!("Bearer {secret}"));
    let mut headers: Vec<(&str, &str)> = vec![("x-queen-tenant", target.broker_tenant.as_str())];
    if let Some(auth) = &auth_header {
        headers.push(("Authorization", auth.as_str()));
    }

    let body = match get_json_with_headers(&url, &headers, RECONCILE_HTTP_TIMEOUT).await {
        Ok(v) => v,
        Err(e) => {
            // Resilience requirement: cell down -> log and skip, never
            // panic or poison the cycle for other clusters.
            tracing::warn!(cluster = %target.cluster_id, cell = %target.base_url, error = %e, "registry reconciler: cell unreachable, skipping");
            return;
        }
    };
    let Some(queues) = body.get("queues").and_then(|q| q.as_array()) else {
        tracing::warn!(cluster = %target.cluster_id, "registry reconciler: unexpected response shape (no \"queues\" array)");
        return;
    };

    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(cluster = %target.cluster_id, error = %e, "registry reconciler: pool.get failed, skipping DB sync this cycle");
            return;
        }
    };

    let mut seen_names: Vec<String> = Vec::with_capacity(queues.len());
    let mut total_bytes: i64 = 0;
    let mut bytes_found = false;

    for q in queues {
        let Some(name) = q.get("name").and_then(|n| n.as_str()) else { continue };
        let name = name.to_string();
        seen_names.push(name.clone());
        let partitions = q.get("partitions").and_then(|p| p.as_i64()).unwrap_or(0);

        // Best-effort retained-bytes: as of this writing (2026-07-25) the
        // broker's get_queues_v2 (+ segment-stats enrichment) response has
        // NO bytes/size field at all -- checked server/src/handlers/queues.rs
        // handle_list_queues and the SQL behind it (queen.get_queues_v2,
        // queen.log_queue_stats_all_v1). These key names are speculative so
        // storage-quota tracking activates automatically the day the broker
        // adds one of them, without another proxy code change; today none
        // will match and bytes_found stays false for every cluster. See the
        // report's storage-quota gap.
        for key in ["retainedBytes", "bytes", "sizeBytes", "diskBytes"] {
            if let Some(b) = q.get(key).and_then(|v| v.as_i64()) {
                total_bytes += b;
                bytes_found = true;
                break;
            }
        }

        let cluster_id_str = target.cluster_id.to_string();
        let stmt = "INSERT INTO queen_proxy.queues(cluster_id, name, partitions_count) \
                    VALUES ($1::text::uuid, $2, $3) \
                    ON CONFLICT (cluster_id, name) WHERE deleted_at IS NULL \
                    DO UPDATE SET partitions_count = EXCLUDED.partitions_count";
        if let Err(e) = client.execute(stmt, &[&cluster_id_str, &name, &partitions]).await {
            tracing::warn!(cluster = %target.cluster_id, queue = %name, error = %e, "registry reconciler: queue upsert failed");
        }

        let mut map = known.write().unwrap();
        let cr = map.entry(target.cluster_id).or_default();
        cr.queue_names.insert(name.clone());
        cr.db_partition_floor.insert(name, partitions);
    }

    // Queues that disappeared from the broker's own listing: soft-delete.
    // queen_proxy.queues is a CACHE (ownership is broker-side, PLAN §5), so
    // a false sweep here (e.g. a transient empty response) just makes the
    // proxy briefly re-learn a queue on its next push -- it never touches
    // broker data. Known rough edge, see report: a genuinely empty `queues`
    // array (new cluster, or a flaky-but-200 response) marks every existing
    // row deleted; self-heals on the next real reconcile or next push.
    let cluster_id_str = target.cluster_id.to_string();
    let sweep_stmt = "UPDATE queen_proxy.queues SET deleted_at = now() \
                       WHERE cluster_id = $1::text::uuid AND deleted_at IS NULL AND NOT (name = ANY($2))";
    if let Err(e) = client.execute(sweep_stmt, &[&cluster_id_str, &seen_names]).await {
        tracing::warn!(cluster = %target.cluster_id, error = %e, "registry reconciler: deleted-queue sweep failed");
    }

    if bytes_found {
        if let Some(max) = target.max_retained_bytes {
            let mut over = over_storage.write().unwrap();
            if total_bytes > max {
                over.insert(target.cluster_id);
            } else {
                over.remove(&target.cluster_id);
            }
        }
    }
}

// --------------------------------------------------- minimal headered GET

/// Minimal plaintext HTTP/1.1 GET with custom headers, for the broker
/// resources/queues call (needs x-queen-tenant + Authorization).
/// `httpget::get_json` (src/httpget.rs) has no header parameter -- it was
/// built for the JWKS fetch, which never needed one -- and that file isn't
/// this agent's to edit; see the report for a suggested unification. Cell
/// traffic is always plaintext HTTP inside the cell network (PLAN §7: "no
/// client TLS at all" for the broker upstream), so this never needs TLS,
/// simplifying it relative to httpget.rs.
async fn get_json_with_headers(
    url: &str,
    headers: &[(&str, &str)],
    timeout: Duration,
) -> Result<serde_json::Value, String> {
    let fetched: Result<Vec<u8>, String> = tokio::time::timeout(timeout, fetch_once(url, headers))
        .await
        .map_err(|_| format!("timeout after {}ms", timeout.as_millis()))?;
    let body = fetched?;
    serde_json::from_slice(&body).map_err(|e| format!("json parse: {e}"))
}

async fn fetch_once(url: &str, headers: &[(&str, &str)]) -> Result<Vec<u8>, String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| "reconciler: cell base_url must be http:// (cell-internal traffic only)".to_string())?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h, p.parse::<u16>().map_err(|_| "invalid port".to_string())?),
        None => (authority, 80u16),
    };
    if host.is_empty() {
        return Err("empty host".to_string());
    }

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect((host, port))
        .await
        .map_err(|e| format!("connect {host}:{port}: {e}"))?;

    let mut req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: queen-proxy\r\nAccept: application/json\r\nConnection: close\r\n"
    );
    for (k, v) in headers {
        req.push_str(k);
        req.push_str(": ");
        req.push_str(v);
        req.push_str("\r\n");
    }
    req.push_str("\r\n");

    stream.write_all(req.as_bytes()).await.map_err(|e| format!("write: {e}"))?;
    stream.flush().await.ok();

    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = stream.read(&mut chunk).await.map_err(|e| format!("read: {e}"))?;
        if n == 0 {
            break; // EOF (Connection: close)
        }
        if buf.len() + n > MAX_RECONCILE_RESPONSE_BYTES {
            return Err(format!("response exceeds {MAX_RECONCILE_RESPONSE_BYTES}-byte cap"));
        }
        buf.extend_from_slice(&chunk[..n]);
    }

    let sep = find_subslice(&buf, b"\r\n\r\n").ok_or("no header terminator")?;
    let head = String::from_utf8_lossy(&buf[..sep]).to_string();
    let body = buf[sep + 4..].to_vec();

    let status_line = head.lines().next().unwrap_or("");
    let code: u16 = status_line.split_whitespace().nth(1).and_then(|c| c.parse().ok()).unwrap_or(0);
    if !(200..300).contains(&code) {
        return Err(format!("http status {code}"));
    }
    // Assumes Content-Length (never chunked): the broker is our own axum
    // service returning a fully-materialized JSON body, which axum always
    // sends with Content-Length. Unlike httpget.rs (arbitrary third-party
    // JWKS endpoints), there's no untrusted-transfer-encoding concern here.
    Ok(body)
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{ClusterStatus, EffectiveLimits, Features};

    fn test_ctx(max_queues: Option<i64>, max_partitions_per_queue: Option<i64>) -> ClusterCtx {
        ClusterCtx {
            cluster_id: Uuid::from_u128(1),
            tenant_id: Uuid::from_u128(2),
            broker_tenant: Uuid::from_u128(3),
            slug: "test".to_string(),
            cell_base_url: "http://127.0.0.1:1".to_string(),
            cell_token: None,
            status: ClusterStatus::Active,
            limits: EffectiveLimits { max_queues, max_partitions_per_queue, ..Default::default() },
            features: Features::default(),
        }
    }

    #[tokio::test]
    async fn admit_allows_then_fast_paths_without_db() {
        let reg = Registry::new(None);
        let ctx = test_ctx(Some(10), Some(10));
        assert_eq!(reg.admit(&ctx, "orders", "p0").await, Admit::Allowed);
        // Same pair again: db is None, so this can only be the in-process
        // fast path (any DB-touching path would panic/None-deref).
        assert_eq!(reg.admit(&ctx, "orders", "p0").await, Admit::Allowed);
    }

    #[tokio::test]
    async fn admit_enforces_max_partitions_per_queue() {
        let reg = Registry::new(None);
        let ctx = test_ctx(None, Some(2));
        assert_eq!(reg.admit(&ctx, "orders", "p0").await, Admit::Allowed);
        assert_eq!(reg.admit(&ctx, "orders", "p1").await, Admit::Allowed);
        assert_eq!(reg.admit(&ctx, "orders", "p2").await, Admit::OverPartitions { max: 2 });
    }

    #[tokio::test]
    async fn admit_enforces_max_queues_but_not_on_repeat_partitions() {
        let reg = Registry::new(None);
        let ctx = test_ctx(Some(1), None);
        assert_eq!(reg.admit(&ctx, "orders", "p0").await, Admit::Allowed);
        // A second partition of the SAME queue must not count against
        // max_queues.
        assert_eq!(reg.admit(&ctx, "orders", "p1").await, Admit::Allowed);
        assert_eq!(reg.admit(&ctx, "shipments", "p0").await, Admit::OverQueues { max: 1 });
    }

    #[test]
    fn forget_clears_state() {
        let reg = Registry::new(None);
        let cluster_id = Uuid::from_u128(42);
        reg.known.write().unwrap().insert(cluster_id, ClusterRegistry::default());
        reg.loaded.write().unwrap().insert(cluster_id);
        reg.forget(cluster_id);
        assert!(!reg.known.read().unwrap().contains_key(&cluster_id));
        assert!(!reg.loaded.read().unwrap().contains(&cluster_id));
    }
}
