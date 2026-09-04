//! Queue/partition registry: plan-cap admission for implicit creation (push
//! auto-creates!) and /configure, plus the periodic reconciler against the
//! broker's (Track B: tenant-scoped) inventory. OWNER: Agent B.
//!
//! `admit()`'s in-process state tracks two things per cluster, deliberately
//! separate:
//!   * `partitions` -- per queue, the exact partition names THIS PROCESS has
//!     admitted. This is the literal fast path ("known pair -> Allowed, no
//!     DB"), the per-queue count the cap check reads in O(1), and the only
//!     place individual partition identity lives at all.
//!   * `db_partition_floor` -- the last-known partitions_count per queue,
//!     seeded from queen_proxy.queues on lazy load and kept in step by the
//!     reconciler. This exists because queen_proxy.queues (001_init.sql)
//!     only ever stores a COUNT, never partition names (and neither does
//!     the broker's own resources/queues response) -- so after a proxy
//!     restart `partitions` starts empty and there is no way to repopulate exact
//!     historical partition names from the DB. `db_partition_floor` is the
//!     restart-safety net: the effective partition count used for the cap
//!     check is `max(observed in `partitions`, db_partition_floor)`, so a
//!     restart can't be used to bypass max_partitions_per_queue before the
//!     reconciler's next pass (or fresh traffic) catches back up.
//!
//! Nothing on the request path waits for the database. A miss is decided in
//! memory and the queue row that records it is coalesced per (cluster, queue)
//! for `spawn_persister`, which writes one UNNEST upsert per tick. The 2026-08-22
//! soak ran the old shape -- one synchronous upsert per new (queue, partition),
//! awaited before the push was forwarded -- to 743 149 writes during a
//! partition-creation ramp, on the Postgres the data path was saturating.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use uuid::Uuid;

use crate::state::ClusterCtx;

const RECONCILE_INTERVAL: Duration = Duration::from_secs(60);
/// Cadence of the coalesced queue-row write (QUEEN_PROXY_REGISTRY_PERSIST_MS).
const PERSIST_INTERVAL: Duration = Duration::from_secs(1);
const RECONCILE_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
/// Cap on a single cell's resources/queues response body -- generous for a
/// cell with many thousands of queues while still bounded against a
/// misbehaving broker.
const MAX_RECONCILE_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Storage-quota hysteresis (PLAN §6.1): a cluster blocks at `> max` but is
/// only released once retained bytes fall back to this percentage of the cap.
/// Without the asymmetry a tenant parked on the boundary flips blocked <->
/// unblocked on every reconcile pass and sees 403 and 201 alternating on
/// identical pushes. 90% is one reconcile pass' worth of headroom at any
/// realistic ingest rate, so a release means the tenant actually deleted or
/// aged out data, not that a rounding wobble crossed the line.
const STORAGE_RELEASE_PERCENT: i64 = 90;

/// What this cycle learned about a cluster's queues, which is what decides
/// whether the deleted-queue sweep may run.
///
/// Until 2026-09-04 the sweep was gated on an empty inventory REPEATING, on the
/// reasoning that "a genuinely empty cluster looks exactly like a broker that
/// answered before its stats were readable". It does not: `seen_names` is built
/// from `queues[].name`, and the broker builds that list straight from
/// `queen.queues` (server/sql/procedures/018_stats.sql: `FROM queen.queues q
/// WHERE q.tenant_id = p_tenant`). Stats readiness moves `partitions` and
/// `retainedBytes`; it cannot invent or withhold a name. So a 200 carrying an
/// empty array IS the answer "this tenant has no queues" -- which is exactly the
/// state a customer who has just deleted everything is in, and the state whose
/// convergence the deferral was delaying. Everything genuinely ambiguous -- an
/// unreachable cell, a timeout, a body without a `queues` array -- returns
/// before the sweep is ever considered, and is `Unreachable` here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Inventory {
    /// The broker answered 200 and we parsed its `queues` array. Its contents
    /// are the truth about what exists, empty or not.
    Confirmed { empty: bool },
    /// No usable answer this cycle. Says nothing about what exists, so it must
    /// not be allowed to delete anything.
    Unreachable,
}

/// queen_proxy.queues.partitions_count is INTEGER (001_init.sql), while
/// partition counts are carried as i64 throughout this module. Binding the i64
/// straight into the statement makes tokio-postgres reject EVERY upsert with
/// "error serializing parameter 2", which is how the table stayed permanently
/// empty — silently, since both write paths only warn. Narrow at the bind sites
/// and keep the in-process arithmetic in i64.
fn clamp_partitions(n: i64) -> i32 {
    n.clamp(0, i32::MAX as i64) as i32
}

#[derive(Debug, PartialEq, Eq)]
pub enum Admit {
    Allowed,
    OverQueues { max: i64 },
    OverPartitions { max: i64 },
}

#[derive(Default, Debug)]
struct ClusterRegistry {
    /// queue -> partition names admitted this process lifetime. Exact-pair
    /// membership and the per-queue count are both O(1) reads of this map.
    partitions: HashMap<String, HashSet<String>>,
    /// queue name -> last-known partitions_count from the DB (lazy-load or
    /// reconciler). A floor, not a ceiling -- see module doc.
    db_partition_floor: HashMap<String, i64>,
    /// Queue names known to exist (DB lazy-load, reconciler, or this
    /// process's own admits) -- backs the max_queues check. Pruned to the
    /// broker's inventory on every confirmed reconcile pass, and dropped
    /// wholesale by `invalidate` when pxdb says the cluster changed: a name in
    /// here that no longer exists is a plan slot the tenant cannot use.
    queue_names: HashSet<String>,
    /// Has this cluster's cell already been reported as not sending the
    /// kv/timer usage fields? One line per cluster per proxy lifetime — see
    /// `kv_timer_bytes`.
    warned_missing_kv_usage: bool,
}

pub struct Registry {
    db: Option<deadpool_postgres::Pool>,
    known: Arc<RwLock<HashMap<Uuid, ClusterRegistry>>>,
    /// Clusters whose `known` entry has been bootstrapped from the DB at
    /// least once -- separate from `known.contains_key` because an entry
    /// gets created on a cache MISS too (see `admit`), before any DB load.
    loaded: Arc<RwLock<HashSet<Uuid>>>,
    /// Clusters currently believed to be over their storage quota, per the
    /// reconciler's last successful byte count. Read by the storage-quota
    /// pump in main.rs -- see `over_storage`.
    over_storage: Arc<RwLock<HashSet<Uuid>>>,
    /// The last retained-bytes TOTAL measured for each cluster, alongside the
    /// over/under verdict derived from it. The verdict alone cannot drive the
    /// in-flight accounting in limits.rs: that needs the number, so it can tell
    /// a fresh computation from a republication of the same one.
    retained: Arc<RwLock<HashMap<Uuid, i64>>>,
    /// Queue rows admitted since the last persist tick: (cluster, queue) ->
    /// highest projected partition count. Bounded by the number of queues.
    pending: Arc<Pending>,
}

impl Registry {
    pub fn new(db: Option<deadpool_postgres::Pool>) -> Registry {
        Registry {
            db,
            known: Arc::new(RwLock::new(HashMap::new())),
            loaded: Arc::new(RwLock::new(HashSet::new())),
            over_storage: Arc::new(RwLock::new(HashSet::new())),
            retained: Arc::new(RwLock::new(HashMap::new())),
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Called with every (queue, partition) named in a produce/configure request.
    /// Fast path: known pair -> Allowed without touching the DB. A miss is
    /// decided in memory too; the queue row that records it is written off
    /// the request path (`enqueue_persist`).
    pub async fn admit(&self, ctx: &ClusterCtx, queue: &str, partition: &str) -> Admit {
        self.ensure_loaded(ctx.cluster_id).await;

        // Fast path: exact pair already known for this cluster.
        {
            let map = self.known.read().unwrap();
            if let Some(cr) = map.get(&ctx.cluster_id) {
                if cr.partitions.get(queue).is_some_and(|p| p.contains(partition)) {
                    return Admit::Allowed;
                }
            }
        }

        // Miss: evaluate caps and admit under one write-lock critical
        // section so "count, then insert" can't race with itself within this
        // process. Everything in here is O(1): the section is the serial
        // point of every new partition in the cell, and it used to scan the
        // cluster's whole pair set per miss (616 us at 100k partitions).
        // Cross-process races don't apply today (one proxy per cell, PLAN §2);
        // a race against the periodic reconciler can cause brief overshoot,
        // healed on the next pass -- accepted per spec.
        let projected = {
            let mut map = self.known.write().unwrap();
            let cr = map.entry(ctx.cluster_id).or_default();

            // Someone else may have admitted this exact pair while we
            // waited for the write lock.
            if cr.partitions.get(queue).is_some_and(|p| p.contains(partition)) {
                return Admit::Allowed;
            }

            let is_new_queue = !cr.queue_names.contains(queue);
            if let Some(max_q) = ctx.limits.max_queues {
                let projected_queues = cr.queue_names.len() as i64 + if is_new_queue { 1 } else { 0 };
                if projected_queues > max_q {
                    return Admit::OverQueues { max: max_q };
                }
            }

            let observed = cr.partitions.get(queue).map_or(0, |p| p.len()) as i64;
            let floor = cr.db_partition_floor.get(queue).copied().unwrap_or(0);
            let projected_partitions = observed.max(floor) + 1;
            if let Some(max_p) = ctx.limits.max_partitions_per_queue {
                if projected_partitions > max_p {
                    return Admit::OverPartitions { max: max_p };
                }
            }

            cr.partitions.entry(queue.to_string()).or_default().insert(partition.to_string());
            cr.queue_names.insert(queue.to_string());
            projected_partitions
        };

        self.enqueue_persist(ctx.cluster_id, queue, projected);
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

    /// Record a queue's projected partition count for the persister, keeping
    /// the maximum per (cluster, queue). A burst creating 5 000 partitions of
    /// one queue is one row in the next flush, not 5 000 upserts on the
    /// request path. No-op without a pxdb (dev-static).
    fn enqueue_persist(&self, cluster_id: Uuid, queue: &str, partitions_count: i64) {
        if self.db.is_none() {
            return;
        }
        let mut pending = self.pending.lock().unwrap();
        let slot = pending.entry((cluster_id, queue.to_string())).or_insert(0);
        *slot = (*slot).max(partitions_count);
    }

    /// Spawn the queue-row persister: every QUEEN_PROXY_REGISTRY_PERSIST_MS it
    /// writes whatever `admit` coalesced since the last tick as ONE statement.
    /// Called from main.rs next to `spawn_reconciler`. No-op without a pxdb.
    pub fn spawn_persister(&self) {
        let Some(pool) = self.db.clone() else {
            tracing::info!("registry persister: no pxdb configured, skipping (dev-static mode)");
            return;
        };
        let pending = self.pending.clone();
        tokio::spawn(async move {
            let interval = Duration::from_millis(
                crate::config::env_u64("QUEEN_PROXY_REGISTRY_PERSIST_MS", PERSIST_INTERVAL.as_millis() as u64)
                    .max(100),
            );
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                flush_pending(&pool, &pending).await;
            }
        });
    }

    /// Shutdown counterpart of the periodic flush (main.rs bounds it).
    pub async fn drain(&self) {
        if let Some(pool) = &self.db {
            flush_pending(pool, &self.pending).await;
        }
    }

    /// Clusters currently believed over their plan's max_retained_bytes, per
    /// the reconciler's last successful byte count for that cluster.
    ///
    /// Consumed by the storage-quota pump in main.rs, which diffs this list
    /// every 10s and calls `st.limits.set_push_blocked(id, ..)` on the
    /// transitions; gateway.rs then answers 403 `storage_quota_exceeded` on
    /// Produce for a blocked cluster (consumes stay allowed). Registry does
    /// not call `Limits` itself -- it holds no reference to it, and AppState's
    /// shape is frozen. Membership is asymmetric on purpose (`decide_over_storage`):
    /// entering the set needs usage over the cap, leaving it needs usage back
    /// under the release band.
    pub fn over_storage(&self) -> Vec<Uuid> {
        self.over_storage.read().unwrap().iter().copied().collect()
    }

    /// The reconciler's last measured retained-bytes total per cluster.
    ///
    /// The storage pump in main.rs feeds these to `Limits::publish_retained`,
    /// which is what lets the push gate add the bytes accepted since a total
    /// was computed instead of trusting a figure that may be a whole broker
    /// recompute period old.
    pub fn retained_totals(&self) -> Vec<(Uuid, i64)> {
        self.retained.read().unwrap().iter().map(|(id, bytes)| (*id, *bytes)).collect()
    }

    /// Drop a cluster's in-memory queue registry so the next `admit` rebuilds
    /// it from pxdb, where the `deleted_at IS NULL` filter lives.
    ///
    /// The partition-cap floor survives, because `ensure_loaded` re-reads it
    /// from `queen_proxy.queues`; the exact partition NAMES this process
    /// admitted do not, which is the same position a restart leaves the
    /// registry in and which the floor exists to cover (module doc).
    pub fn invalidate(&self, cluster_id: Uuid) {
        invalidate_cluster(&self.known, &self.loaded, cluster_id);
    }

    /// `invalidate` as a standalone callable, for the pxdb NOTIFY listener in
    /// cache.rs to hold.
    ///
    /// Handing over clones of the two maps rather than an `Arc<Registry>` (or,
    /// worse, the `AppState` that owns it) keeps this a leaf: the listener task
    /// outlives nothing and owns nothing that owns it, so there is no reference
    /// cycle to reason about and `AppState`'s shape is untouched.
    pub fn invalidator(&self) -> impl Fn(Uuid) + Send + Sync + 'static {
        let known = self.known.clone();
        let loaded = self.loaded.clone();
        move |cluster_id| invalidate_cluster(&known, &loaded, cluster_id)
    }

    pub fn spawn_reconciler(&self) {
        let Some(pool) = self.db.clone() else {
            tracing::info!("registry reconciler: no pxdb configured, skipping (dev-static mode)");
            return;
        };
        let known = self.known.clone();
        let over_storage = self.over_storage.clone();
        let retained = self.retained.clone();
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
                reconcile_once(&pool, &known, &over_storage, &retained).await;
            }
        });
    }

    #[allow(dead_code)]
    pub fn forget(&self, cluster_id: Uuid) {
        self.invalidate(cluster_id);
    }
}

/// The one place a cluster's in-memory registry is dropped, shared by
/// `Registry::invalidate` and the closure `invalidator` hands the listener.
/// `loaded` is cleared LAST: a concurrent `admit` that reads `loaded` between
/// the two writes re-runs `ensure_loaded`, which is a wasted query at worst,
/// whereas the opposite order can leave the stale entry marked as loaded.
fn invalidate_cluster(
    known: &Arc<RwLock<HashMap<Uuid, ClusterRegistry>>>,
    loaded: &Arc<RwLock<HashSet<Uuid>>>,
    cluster_id: Uuid,
) {
    known.write().unwrap().remove(&cluster_id);
    loaded.write().unwrap().remove(&cluster_id);
}

// -------------------------------------------------------------- persister

type Pending = Mutex<HashMap<(Uuid, String), i64>>;

/// One multi-row upsert per flush. Arrays bind as text/int4 and cast in SQL
/// (no uuid feature on tokio-postgres, like every other query in this crate);
/// `GREATEST` keeps the row a floor that only the reconciler, which knows the
/// broker's true count, ever lowers. Each (cluster, name) appears at most once
/// per batch (it is the map key), which `ON CONFLICT DO UPDATE` requires.
const PERSIST_SQL: &str = "INSERT INTO queen_proxy.queues(cluster_id, name, partitions_count) \
    SELECT c::uuid, n, p FROM UNNEST($1::text[], $2::text[], $3::int4[]) AS t(c, n, p) \
    ON CONFLICT (cluster_id, name) WHERE deleted_at IS NULL \
    DO UPDATE SET partitions_count = GREATEST(queen_proxy.queues.partitions_count, EXCLUDED.partitions_count)";

async fn flush_pending(pool: &deadpool_postgres::Pool, pending: &Pending) {
    let batch: HashMap<(Uuid, String), i64> = {
        let mut p = pending.lock().unwrap();
        if p.is_empty() {
            return;
        }
        std::mem::take(&mut *p)
    };
    let mut ids = Vec::with_capacity(batch.len());
    let mut names = Vec::with_capacity(batch.len());
    let mut counts = Vec::with_capacity(batch.len());
    for ((cluster_id, name), count) in &batch {
        ids.push(cluster_id.to_string());
        names.push(name.clone());
        counts.push(clamp_partitions(*count));
    }
    let written = match pool.get().await {
        Ok(client) => client
            .execute(PERSIST_SQL, &[&ids, &names, &counts])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        Err(e) => Err(e.to_string()),
    };
    match written {
        Ok(()) => tracing::debug!(rows = batch.len(), "registry: queue rows persisted"),
        Err(e) => {
            tracing::warn!(rows = batch.len(), error = %e, "registry: queue persist failed; retrying next tick");
            // Put the batch back under whatever arrived meanwhile. Bounded by
            // the number of queues, so an outage holds one count per queue.
            let mut p = pending.lock().unwrap();
            for (key, count) in batch {
                let slot = p.entry(key).or_insert(0);
                *slot = (*slot).max(count);
            }
        }
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
    retained: &Arc<RwLock<HashMap<Uuid, i64>>>,
) {
    let targets = match load_targets(pool).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "registry reconciler: failed to list clusters, skipping cycle");
            return;
        }
    };
    for target in targets {
        reconcile_cluster(pool, known, over_storage, retained, &target).await;
    }
}

async fn load_targets(pool: &deadpool_postgres::Pool) -> Result<Vec<ReconcileTarget>, String> {
    let client = pool.get().await.map_err(|e| format!("pool.get: {e}"))?;
    // Reconcile everything not being torn down -- push_blocked clusters
    // especially still need this loop: it's the only thing that re-evaluates
    // their byte count, so skipping them would make the block permanent.
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
    retained: &Arc<RwLock<HashMap<Uuid, i64>>>,
    target: &ReconcileTarget,
) {
    // `?stats=cached`: the reconciler reads `name`, `partitions`, `retainedBytes`
    // and the top-level kv/timer bytes, all of which the broker's cached
    // queen.stats view already carries (refreshed at its stats cadence), so the
    // per-poll live enrichment — until 2026-08-23 a pass over EVERY segment in
    // the cell, per cluster, per interval — is declined. A broker older than the
    // parameter ignores it and answers as before.
    let url = format!("{}/api/v1/resources/queues?stats=cached", target.base_url.trim_end_matches('/'));
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
            skip_without_inventory(target, &format!("cell unreachable: {e}"));
            return;
        }
    };
    let Some(queues) = body.get("queues").and_then(|q| q.as_array()) else {
        skip_without_inventory(target, "unexpected response shape (no \"queues\" array)");
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

        // Retained bytes for this queue, as emitted per (tenant, queue) by
        // the broker since Track B2 (server/sql/procedures/013_stats.sql,
        // `'retainedBytes', COALESCE(s.retained_bytes, 0)`), refreshed at the
        // broker's stats cadence. A cell that predates B2 sends no such field:
        // bytes_found then stays false and the quota is simply not evaluated
        // this cycle, which leaves any existing decision alone rather than
        // releasing or blocking on a phantom zero.
        if let Some(b) = q.get("retainedBytes").and_then(|v| v.as_i64()) {
            total_bytes += b;
            bytes_found = true;
        }

        // Write only what changed. `db_partition_floor` is the last count this
        // process read from or wrote to the row, so an equal value means the
        // row already says so -- and a no-op UPDATE is not free in Postgres:
        // a new tuple version, WAL, and a dead tuple for autovacuum, once per
        // queue per cycle, on the database the data path shares.
        let unchanged = {
            let map = known.read().unwrap();
            map.get(&target.cluster_id)
                .and_then(|cr| cr.db_partition_floor.get(&name))
                .is_some_and(|&known_count| known_count == partitions)
        };
        if !unchanged {
            let cluster_id_str = target.cluster_id.to_string();
            let count = clamp_partitions(partitions);
            let stmt = "INSERT INTO queen_proxy.queues(cluster_id, name, partitions_count) \
                        VALUES ($1::text::uuid, $2, $3) \
                        ON CONFLICT (cluster_id, name) WHERE deleted_at IS NULL \
                        DO UPDATE SET partitions_count = EXCLUDED.partitions_count";
            if let Err(e) = client.execute(stmt, &[&cluster_id_str, &name, &count]).await {
                tracing::warn!(cluster = %target.cluster_id, queue = %name, error = %e, "registry reconciler: queue upsert failed");
                // Not remembered as written: the next cycle must try again.
                continue;
            }
        }

        let mut map = known.write().unwrap();
        let cr = map.entry(target.cluster_id).or_default();
        cr.queue_names.insert(name.clone());
        cr.db_partition_floor.insert(name, partitions);
    }

    // PLAN_KV_TIMERS.md §9.8 P2. `queen.kv` and `queen.log_timers` have no
    // queue, so their bytes cannot ride on any per-queue entry above — and
    // without them they are the one place in the product where a tenant
    // occupies disk that no quota can see. The four fields are TOP-LEVEL on
    // this same response and come from the broker's cached measurement (the
    // sweeper's five-minute rollup), never from a count run for this poll.
    {
        let mut map = known.write().unwrap();
        let cr = map.entry(target.cluster_id).or_default();
        match kv_timer_bytes(&body) {
            Some(extra) => {
                total_bytes += extra;
                // A cell that answers with the fields IS a measurement, even
                // when every queue is gone. Without this a cluster whose only
                // occupancy is KV would never be evaluated at all: `bytes_found`
                // is set by the per-queue loop, which such a cluster never
                // enters. It also fixes a pre-existing corner by construction —
                // a blocked cluster that deletes ALL its queues used to leave
                // `bytes_found` false forever and so could never release.
                bytes_found = true;
            }
            None => {
                if !cr.warned_missing_kv_usage {
                    cr.warned_missing_kv_usage = true;
                    // ZERO, LOUDLY — never "measurement not found". `bytes_found`
                    // stays true on account of the queues, so abstaining here
                    // would leave an old cell producing a silent UNDER-COUNT of
                    // the quota forever. A noisy zero is the honest failure.
                    tracing::warn!(
                        cluster = %target.cluster_id, cell = %target.base_url,
                        "registry reconciler: cell sends no kv/timer usage fields; \
                         counting them as ZERO toward the storage quota (PLAN_KV_TIMERS §9.8 P2)"
                    );
                }
            }
        }
    }

    // Queues that disappeared from the broker's own listing: soft-delete.
    // queen_proxy.queues is a CACHE (ownership is broker-side, PLAN §5), so a
    // false sweep never touches broker data. It isn't free either: the swept
    // rows are what re-seeds `db_partition_floor` after a proxy restart
    // (module doc), so losing them loses the partition-cap floor. Which answers
    // are trustworthy enough to delete on is `Inventory`'s whole subject.
    let inventory = Inventory::Confirmed { empty: seen_names.is_empty() };
    if sweep_allowed(inventory) {
        let cluster_id_str = target.cluster_id.to_string();
        let sweep_stmt = "UPDATE queen_proxy.queues SET deleted_at = now() \
                           WHERE cluster_id = $1::text::uuid AND deleted_at IS NULL AND NOT (name = ANY($2))";
        if let Err(e) = client.execute(sweep_stmt, &[&cluster_id_str, &seen_names]).await {
            tracing::warn!(cluster = %target.cluster_id, error = %e, "registry reconciler: deleted-queue sweep failed");
        } else {
            // The swept rows are gone: forget their floor too, so a queue
            // that comes back is written again rather than skipped as
            // unchanged.
            //
            // ...and forget the NAMES, which is what `admit` counts against
            // max_queues. Until 2026-09-04 this line was missing and nothing
            // else ever removed from `queue_names`, so the count was of every
            // queue the process had ever seen. Measured on the trial cell: a
            // cluster with 61 tombstoned rows and 0 live ones refused every
            // push to a new queue name with `queue limit reached (20)` until
            // the proxy was restarted, because a restart is the only thing that
            // re-ran the lazy load and its `deleted_at IS NULL` filter. Pruned
            // in the SAME branch as the DB write so the two never disagree
            // about what exists; a failed sweep retries next cycle.
            let seen: HashSet<&str> = seen_names.iter().map(String::as_str).collect();
            let mut map = known.write().unwrap();
            if let Some(cr) = map.get_mut(&target.cluster_id) {
                cr.db_partition_floor.retain(|name, _| seen.contains(name.as_str()));
                let before = cr.queue_names.len();
                cr.queue_names.retain(|name| seen.contains(name.as_str()));
                // The exact partition names of a swept queue go with it: they
                // describe a queue the broker no longer has, and leaving them
                // would keep the pair fast-path answering Allowed for it.
                cr.partitions.retain(|name, _| seen.contains(name.as_str()));
                let freed = before - cr.queue_names.len();
                if freed > 0 {
                    tracing::info!(
                        target: "limits", cluster = %target.cluster_id, kind = "queues",
                        freed, live = cr.queue_names.len(),
                        "registry: released queue slots for queues the broker no longer has"
                    );
                }
            }
        }
    } else {
        tracing::warn!(cluster = %target.cluster_id, cell = %target.base_url, "registry reconciler: unconfirmed queue inventory, deferring sweep");
    }

    if bytes_found {
        // Published whether or not the cluster has a cap: the number is a
        // measurement, and `limits` is the one that knows whether this cluster
        // has anything to measure it against. Published BEFORE the verdict so
        // the two can never disagree about which computation they describe.
        retained.write().unwrap().insert(target.cluster_id, total_bytes);
        if let Some(max) = target.max_retained_bytes {
            let mut over = over_storage.write().unwrap();
            let blocked = over.contains(&target.cluster_id);
            if decide_over_storage(blocked, total_bytes, max) {
                over.insert(target.cluster_id);
            } else {
                over.remove(&target.cluster_id);
            }
        }
    }
}

/// The KV + timer bytes a cell reports for this tenant, or `None` when the cell
/// does not report them at all (PLAN_KV_TIMERS.md §9.8 P2).
///
/// `None` means "this cell is older than the feature", and the caller turns it
/// into a zero plus one warning per cluster. It is NOT the same as a cell that
/// reports zero, which is a real measurement of an empty pair of tables.
///
/// Only the two BYTE fields are summed. `kvRows` / `timerRows` are the row-count
/// quotas of §9.2, which are enforced broker-side against a local delta (§9.3)
/// and are F8 P3 here — reading them into the storage total would double-count a
/// tenant against a cap denominated in bytes.
///
/// Negative values are ignored rather than subtracted: a byte count is not
/// allowed to make a cluster's total go DOWN, or a single malformed field turns
/// into a quota bypass.
fn kv_timer_bytes(body: &serde_json::Value) -> Option<i64> {
    let kv = body.get("kvBytes").and_then(|v| v.as_i64());
    let tm = body.get("timerBytes").and_then(|v| v.as_i64());
    match (kv, tm) {
        (None, None) => None,
        (a, b) => Some(a.unwrap_or(0).max(0).saturating_add(b.unwrap_or(0).max(0))),
    }
}

/// Storage-quota decision with hysteresis: block at `> max`, release only
/// once retained bytes are back at or under STORAGE_RELEASE_PERCENT of the
/// cap. A cluster already over on the very first pass (`blocked == false`,
/// nothing remembered across a restart) blocks immediately -- the band only
/// ever delays the *release* side.
fn decide_over_storage(blocked: bool, total_bytes: i64, max: i64) -> bool {
    if !blocked {
        return total_bytes > max;
    }
    // i128 so the percentage is exact for any i64 cap, with no overflow and
    // no float rounding near the boundary this exists to stabilise.
    let release_at = (max as i128 * STORAGE_RELEASE_PERCENT as i128 / 100) as i64;
    total_bytes > release_at
}

/// Whether this cycle's inventory may drive the deleted-queue sweep. A
/// confirmed listing sweeps whether or not it is empty; anything else defers.
fn sweep_allowed(inventory: Inventory) -> bool {
    matches!(inventory, Inventory::Confirmed { .. })
}

/// This cluster produced no usable listing this cycle, so nothing about it can
/// be reconciled and, in particular, nothing may be swept. One shape for both
/// causes (cell unreachable or unreadable, body without a `queues` array), and
/// the deferral is stated rather than left implicit in an early `return`. The
/// rows this protects are the partition-cap floor a restart reads back.
fn skip_without_inventory(target: &ReconcileTarget, cause: &str) {
    tracing::warn!(
        cluster = %target.cluster_id,
        cell = %target.base_url,
        cause,
        sweep = sweep_allowed(Inventory::Unreachable),
        "registry reconciler: no usable queue inventory, skipping cluster"
    );
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

    /// A pool nothing listens behind (127.0.0.1:1), bounded so a test can
    /// never hang on it: the registry must behave with a pxdb that is
    /// configured but unreachable.
    fn unreachable_pool() -> deadpool_postgres::Pool {
        let mut pg = tokio_postgres::Config::new();
        pg.host("127.0.0.1").port(1).user("x").dbname("x").connect_timeout(Duration::from_secs(2));
        let mgr = deadpool_postgres::Manager::from_config(
            pg,
            tokio_postgres::NoTls,
            deadpool_postgres::ManagerConfig { recycling_method: deadpool_postgres::RecyclingMethod::Fast },
        );
        deadpool_postgres::Pool::builder(mgr)
            .max_size(1)
            .runtime(deadpool_postgres::Runtime::Tokio1)
            .wait_timeout(Some(Duration::from_secs(2)))
            .create_timeout(Some(Duration::from_secs(2)))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn new_partitions_coalesce_into_one_pending_row_per_queue() {
        let reg = Registry::new(Some(unreachable_pool()));
        let ctx = test_ctx(None, None);
        for i in 0..50 {
            assert_eq!(reg.admit(&ctx, "orders", &format!("p{i}")).await, Admit::Allowed);
        }
        assert_eq!(reg.admit(&ctx, "shipments", "p0").await, Admit::Allowed);
        let pending = reg.pending.lock().unwrap().clone();
        assert_eq!(pending.len(), 2, "one row per (cluster, queue), not one per partition");
        assert_eq!(pending[&(ctx.cluster_id, "orders".to_string())], 50, "the highest projected count wins");
        assert_eq!(pending[&(ctx.cluster_id, "shipments".to_string())], 1);
    }

    #[tokio::test]
    async fn a_failed_flush_keeps_the_batch_for_the_next_tick() {
        let reg = Registry::new(Some(unreachable_pool()));
        let ctx = test_ctx(None, None);
        assert_eq!(reg.admit(&ctx, "orders", "p0").await, Admit::Allowed);
        reg.drain().await; // pxdb unreachable: the row must survive the failure
        let pending = reg.pending.lock().unwrap().clone();
        assert_eq!(pending.get(&(ctx.cluster_id, "orders".to_string())), Some(&1));
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

    // ---- storage-quota hysteresis (PLAN §6.1) ----

    #[test]
    fn storage_blocks_over_cap_on_the_first_pass() {
        // Nothing is remembered across a restart, so "already over" must
        // block immediately rather than waiting for a transition.
        assert!(decide_over_storage(false, 1_001, 1_000));
        assert!(!decide_over_storage(false, 1_000, 1_000), "at the cap is not over it");
    }

    #[test]
    fn storage_holds_the_block_inside_the_release_band() {
        // The flapping case: usage sits just under the cap. Before the band,
        // this released and the next tick blocked again -- 403/201/403/201.
        assert!(decide_over_storage(true, 1_000, 1_000));
        assert!(decide_over_storage(true, 999, 1_000));
        assert!(decide_over_storage(true, 901, 1_000), "still inside the 90% band");
    }

    #[test]
    fn storage_releases_only_below_the_band() {
        assert!(!decide_over_storage(true, 900, 1_000), "at 90% of the cap: released");
        assert!(!decide_over_storage(true, 500, 1_000));
    }

    #[test]
    fn storage_band_is_exact_for_large_caps() {
        // 8 EiB-scale caps must not lose precision (no f64, no overflow).
        let max = i64::MAX;
        let release_at = (max as i128 * STORAGE_RELEASE_PERCENT as i128 / 100) as i64;
        assert!(decide_over_storage(true, release_at + 1, max));
        assert!(!decide_over_storage(true, release_at, max));
    }

    #[test]
    fn storage_zero_cap_blocks_and_never_releases_while_holding_bytes() {
        // max = 0 (a cluster with no storage allowance): release band is 0
        // too, so any retained byte keeps it blocked, and an empty cluster
        // clears.
        assert!(decide_over_storage(false, 1, 0));
        assert!(decide_over_storage(true, 1, 0));
        assert!(!decide_over_storage(true, 0, 0));
    }

    // ---- PLAN_KV_TIMERS.md §9.8 P2: the bytes with no queue ----

    fn body(json: &str) -> serde_json::Value {
        serde_json::from_str(json).expect("test body")
    }

    #[test]
    fn kv_and_timer_bytes_are_summed_into_the_storage_total() {
        assert_eq!(
            kv_timer_bytes(&body(r#"{"queues":[],"kvBytes":700,"timerBytes":300}"#)),
            Some(1_000)
        );
        // A real cell with the feature switched off reports a real zero.
        assert_eq!(
            kv_timer_bytes(&body(r#"{"queues":[],"kvRows":0,"kvBytes":0,"timerRows":0,"timerBytes":0}"#)),
            Some(0)
        );
    }

    /// The distinction the whole design of this field rests on: a cell that
    /// cannot answer is NOT a cell that answers zero. Both count as zero
    /// bytes, but only the first one warns — and `None` is what tells the
    /// caller to warn.
    #[test]
    fn a_cell_that_predates_the_feature_is_none_not_zero() {
        assert_eq!(kv_timer_bytes(&body(r#"{"queues":[{"name":"orders"}]}"#)), None);
        // Half an answer is still an answer: the missing half is zero.
        assert_eq!(kv_timer_bytes(&body(r#"{"kvBytes":42}"#)), Some(42));
        assert_eq!(kv_timer_bytes(&body(r#"{"timerBytes":42}"#)), Some(42));
        // A non-numeric field is not a number.
        assert_eq!(kv_timer_bytes(&body(r#"{"kvBytes":"42","timerBytes":null}"#)), None);
    }

    /// A byte count must never make the total smaller — that would be a quota
    /// bypass hidden in one malformed field.
    #[test]
    fn negative_usage_never_credits_the_tenant() {
        assert_eq!(kv_timer_bytes(&body(r#"{"kvBytes":-5000,"timerBytes":10}"#)), Some(10));
        assert_eq!(
            kv_timer_bytes(&body(&format!(r#"{{"kvBytes":{max},"timerBytes":{max}}}"#, max = i64::MAX))),
            Some(i64::MAX),
            "saturating, not wrapping: two huge counts must not become a small one"
        );
    }

    /// The reason this feeds the SAME `total_bytes` rather than a quota of its
    /// own: the gate it has to reach already exists and already has hysteresis.
    #[test]
    fn kv_bytes_alone_can_block_a_cluster_with_no_queues() {
        let extra = kv_timer_bytes(&body(r#"{"queues":[],"kvBytes":1500,"timerBytes":0}"#))
            .expect("measured");
        assert!(decide_over_storage(false, extra, 1_000));
    }

    /// A byte-for-byte capture of what the broker actually answers on
    /// `GET /api/v1/resources/queues` after the P2 change (queen 1.0.2, both
    /// feature flags OFF, one tenant measured). Field NAMES are the contract
    /// between two repositories' worth of code and nothing else checks them:
    /// a rename on either side is a silent under-count, which is precisely the
    /// failure §9.8 P2 exists to prevent.
    #[test]
    fn the_real_broker_body_parses() {
        let captured =
            r#"{"kvBytes":987654321,"kvRows":12345,"queues":[],"timerBytes":4096,"timerRows":42}"#;
        assert_eq!(kv_timer_bytes(&body(captured)), Some(987_654_321 + 4_096));

        // Same route, a tenant the rollup has never seen: a real zero, not an
        // absence — the fields are there.
        let never_measured = r#"{"kvBytes":0,"kvRows":0,"queues":[],"timerBytes":0,"timerRows":0}"#;
        assert_eq!(kv_timer_bytes(&body(never_measured)), Some(0));
    }

    // ---- inventory sweep guard (2026-09-04) ----

    /// The case the old two-cycle deferral was blocking: a customer deletes
    /// every queue, the broker answers 200 with an empty array, and that IS the
    /// answer. Deferring it left the cluster's tombstones (and, before the
    /// `queue_names` prune, its plan slots) waiting on a second cycle.
    #[test]
    fn a_confirmed_empty_inventory_sweeps_immediately() {
        assert!(sweep_allowed(Inventory::Confirmed { empty: true }));
    }

    #[test]
    fn a_confirmed_listing_sweeps() {
        assert!(sweep_allowed(Inventory::Confirmed { empty: false }));
    }

    /// The distinction that matters: "the broker says nothing exists" is not
    /// "the broker said nothing". An unreachable cell, a timeout or an
    /// unreadable body must never soft-delete a row -- those rows are the
    /// partition-cap floor a restart reads back.
    #[test]
    fn an_unreachable_broker_still_defers() {
        assert!(!sweep_allowed(Inventory::Unreachable));
    }

    // ---- max_queues counts live queues only (2026-09-03 defect B) ----

    /// The trial-cell bug in miniature: fill the plan's queue slots, then have
    /// the reconciler observe that the broker no longer has those queues. The
    /// slots must come back without a restart.
    #[tokio::test]
    async fn pruning_swept_names_frees_queue_slots_without_a_restart() {
        let reg = Registry::new(None);
        let ctx = test_ctx(Some(2), None);
        assert_eq!(reg.admit(&ctx, "orders", "p0").await, Admit::Allowed);
        assert_eq!(reg.admit(&ctx, "shipments", "p0").await, Admit::Allowed);
        assert_eq!(reg.admit(&ctx, "invoices", "p0").await, Admit::OverQueues { max: 2 });

        // What the reconciler's sweep branch does to the in-memory registry
        // once the broker's inventory comes back empty.
        {
            let mut map = reg.known.write().unwrap();
            let cr = map.get_mut(&ctx.cluster_id).expect("registry entry");
            cr.queue_names.clear();
            cr.partitions.clear();
            cr.db_partition_floor.clear();
        }

        assert_eq!(
            reg.admit(&ctx, "invoices", "p0").await,
            Admit::Allowed,
            "a queue the broker no longer has must not hold a plan slot"
        );
    }

    /// The other half of the fix: a soft-delete performed anywhere else (the
    /// console, an operator's hand) reaches the proxy as a pxdb NOTIFY, and the
    /// invalidator has to make the next admit rebuild from the live rows.
    #[tokio::test]
    async fn invalidate_frees_queue_slots_without_a_restart() {
        let reg = Registry::new(None);
        let ctx = test_ctx(Some(1), None);
        assert_eq!(reg.admit(&ctx, "orders", "p0").await, Admit::Allowed);
        assert_eq!(reg.admit(&ctx, "shipments", "p0").await, Admit::OverQueues { max: 1 });

        reg.invalidate(ctx.cluster_id);
        assert_eq!(
            reg.admit(&ctx, "shipments", "p0").await,
            Admit::Allowed,
            "after invalidation the count is rebuilt, not inherited"
        );
    }

    #[test]
    fn the_invalidator_closure_clears_the_same_state() {
        let reg = Registry::new(None);
        let cluster_id = Uuid::from_u128(42);
        reg.known.write().unwrap().insert(cluster_id, ClusterRegistry::default());
        reg.loaded.write().unwrap().insert(cluster_id);

        let invalidate = reg.invalidator();
        invalidate(cluster_id);

        assert!(!reg.known.read().unwrap().contains_key(&cluster_id));
        assert!(!reg.loaded.read().unwrap().contains(&cluster_id));
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

    #[test]
    fn retained_totals_round_trip_for_the_storage_pump() {
        let reg = Registry::new(None);
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        reg.retained.write().unwrap().insert(a, 4_096);
        reg.retained.write().unwrap().insert(b, 0);
        let mut got = reg.retained_totals();
        got.sort();
        assert_eq!(got, vec![(a, 4_096), (b, 0)]);
    }
}
