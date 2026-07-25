//! Queue/partition registry: plan-cap admission for implicit creation (push
//! auto-creates!) and /configure, plus the periodic reconciler against the
//! broker's (Track B: tenant-scoped) inventory. OWNER: Agent B.

use uuid::Uuid;

use crate::state::ClusterCtx;

pub enum Admit {
    Allowed,
    OverQueues { max: i64 },
    OverPartitions { max: i64 },
}

pub struct Registry {
    #[allow(dead_code)]
    db: Option<deadpool_postgres::Pool>,
}

impl Registry {
    pub fn new(db: Option<deadpool_postgres::Pool>) -> Registry {
        Registry { db }
    }

    /// Called with every (queue, partition) named in a produce/configure request.
    /// Fast path: known pair -> Allowed without touching the DB.
    pub async fn admit(&self, _ctx: &ClusterCtx, _queue: &str, _partition: &str) -> Admit {
        // Agent B: per-cluster known-set cache; on miss check counts vs
        // ctx.limits.max_queues / max_partitions_per_queue, upsert queen_proxy.queues,
        // slight overshoot on races is acceptable (reconciler heals).
        Admit::Allowed
    }

    pub fn spawn_reconciler(self: &std::sync::Arc<Self>) {
        // Agent B: periodic GET {cell}/api/v1/resources/queues with the cluster's
        // tenant header; sync queen_proxy.queues (partitions_count, deleted_at),
        // recompute retained bytes for storage quota (limits::storage_state).
    }

    #[allow(dead_code)]
    pub fn forget(&self, _cluster_id: Uuid) {}
}
