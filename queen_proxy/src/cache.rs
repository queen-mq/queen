//! ClusterCache: host -> ClusterCtx and api-key-hash -> (ClusterCtx, scopes),
//! DB-backed with TTL + LISTEN/NOTIFY invalidation. OWNER: Agent B.
//!
//! Skeleton behavior: dev-static mode only (QUEEN_PROXY_DEV_CELL_URL) — any
//! host resolves to the single static cluster. DB-backed resolution is Agent
//! B's job (queries + pg LISTEN on channel `queen_proxy_inval`).

use std::sync::Arc;

use uuid::Uuid;

use crate::config::Config;
use crate::state::{ClusterCtx, ClusterStatus, EffectiveLimits, Features, Scopes};

pub struct ClusterCache {
    dev_static: Option<ClusterCtx>,
    #[allow(dead_code)]
    db: Option<deadpool_postgres::Pool>,
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
        ClusterCache { dev_static, db }
    }

    /// Resolve the cluster for an inbound Host header (host[:port]).
    pub async fn resolve_host(&self, _host: &str) -> Option<Arc<ClusterCtx>> {
        // Agent B: slug = first DNS label; look up clusters+cells+plans (cached, TTL,
        // NOTIFY-invalidated). Skeleton: static cluster for any host.
        self.dev_static.clone().map(Arc::new)
    }

    /// Look up an API key by sha256 hash (hex). Returns the cluster and scopes.
    pub async fn by_key_hash(&self, _hash_hex: &str) -> Option<(Arc<ClusterCtx>, Uuid, Scopes)> {
        // Agent B: SELECT ... FROM api_keys JOIN clusters ... WHERE key_hash=$1
        // AND revoked_at IS NULL; cache positive + negative results briefly.
        None
    }

    /// Invalidate a cluster (NOTIFY payload or admin action).
    pub fn invalidate(&self, _cluster_id: Uuid) {}

    /// Spawn the LISTEN task. No-op in dev-static mode.
    pub fn spawn_listener(self: &Arc<Self>) {}
}
