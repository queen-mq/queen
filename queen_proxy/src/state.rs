//! Shared types + AppState. Owned by the orchestrator — agents must not edit
//! this file; if your module needs a new field or type, put the exact request
//! in your final report.

use std::sync::Arc;

use uuid::Uuid;

use crate::config::Config;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterStatus {
    Active,
    /// storage quota exceeded or payment grace: pushes 403, consumes allowed
    PushBlocked,
    /// all data-plane 403
    Suspended,
    Deleting,
}

/// Per-cluster effective limits (plan merged with overrides). None = unlimited.
#[derive(Clone, Debug, Default)]
pub struct EffectiveLimits {
    pub max_req_per_sec: Option<i64>,
    pub req_burst: Option<i64>,
    pub max_msgs_per_sec: Option<i64>,
    pub msgs_burst: Option<i64>,
    pub max_queues: Option<i64>,
    pub max_partitions_per_queue: Option<i64>,
    pub max_parked_pops: Option<i64>,
    pub max_payload_bytes: Option<i64>,
    pub max_batch_items: Option<i64>,
    pub max_retained_bytes: Option<i64>,
    pub max_retention_seconds: Option<i64>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Features {
    pub streams: bool,
    pub traces: bool,
}

/// Everything the data plane needs to know about a cluster, cached hot.
#[derive(Clone, Debug)]
pub struct ClusterCtx {
    pub cluster_id: Uuid,
    pub tenant_id: Uuid,
    /// Value of the X-Queen-Tenant header sent upstream (Track B scoping key).
    pub broker_tenant: Uuid,
    pub slug: String,
    pub cell_base_url: String,
    /// Bearer token the proxy presents to the cell broker (JWT_ENABLED on cells).
    pub cell_token: Option<String>,
    pub status: ClusterStatus,
    pub limits: EffectiveLimits,
    pub features: Features,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Scopes {
    pub produce: bool,
    pub consume: bool,
    pub admin: bool,
    pub read: bool,
}

impl Scopes {
    pub fn all() -> Scopes {
        Scopes { produce: true, consume: true, admin: true, read: true }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Admin,
    Producer,
    Consumer,
    Viewer,
}

#[derive(Clone, Debug)]
pub enum Principal {
    ApiKey { key_id: Uuid, scopes: Scopes },
    User { user_id: Uuid, role: Role },
}

/// Operation classes metered into usage_minutes.op_class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OpClass {
    Push,
    Delivery,
    Txn,
    Configure,
    Read,
}

impl OpClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            OpClass::Push => "push",
            OpClass::Delivery => "delivery",
            OpClass::Txn => "txn",
            OpClass::Configure => "configure",
            OpClass::Read => "read",
        }
    }
}

pub struct AppState {
    pub cfg: Config,
    /// pxdb pool; None in dev-static mode (no persistence, no auth DB).
    pub db: Option<deadpool_postgres::Pool>,
    /// Pooled plaintext HTTP/1.1 client toward cell brokers.
    pub upstream:
        hyper_util::client::legacy::Client<hyper_util::client::legacy::connect::HttpConnector, axum::body::Body>,
    pub cache: crate::cache::ClusterCache,
    pub limits: crate::limits::Limits,
    pub meter: crate::meter::Meter,
    pub registry: crate::registry::Registry,
    pub keys: crate::auth::Keys,
}

pub type St = Arc<AppState>;
