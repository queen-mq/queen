//! Where the proxy keeps its state (PLAN_SINGLE_BINARY.md W3/W4/W5).
//!
//! The standalone proxy keeps it in its own Postgres (migrations 001–011).
//! Inside the broker — the single binary — it keeps it in the broker's
//! replicated KV, under a reserved system tenant: the one pipeline every
//! other Queen feature uses, no proxy-specific raft effect or keyspace
//! (feedback: "facades translate to Queen commands").
//!
//! Every read and write goes through a domain repository here, and every
//! repository function answers from either backend:
//!
//! | module | tables | owner |
//! |---|---|---|
//! | [`data`] | tenants, cells, plans, clusters, cluster_roles, api_keys, revoked_tokens, queues | W3 data plane |
//! | [`web`] | users, identities, operations, outbox | W4 web plane |
//! | [`usage`] | usage_minutes, usage_days | W3 metering |
//! | [`import`] | all of them, Postgres → KV once | W5 |
//!
//! [`schema`] fixes the KV layout (namespaces, keys, index keys) every one of
//! them — and the import — must agree on; [`kv`] is the backend seam and the
//! typed helpers; [`memkv`] is an in-memory backend with the broker's KV
//! semantics, for tests.

use std::sync::Arc;

pub mod data;
pub mod import;
pub mod kv;
pub mod memkv;
pub mod schema;
pub mod usage;
pub mod web;

pub use kv::{KvBackend, KvError};

/// The proxy's state backend.
#[derive(Clone)]
pub enum Store {
    /// The standalone proxy: its own Postgres.
    Pg(deadpool_postgres::Pool),
    /// The single binary: the broker's replicated KV (system tenant).
    Kv(Arc<dyn KvBackend>),
    /// Dev-static mode: no persistence at all.
    None,
}

impl Store {
    /// The Postgres pool, when this is the standalone proxy.
    pub fn pg(&self) -> Option<&deadpool_postgres::Pool> {
        match self {
            Store::Pg(p) => Some(p),
            _ => None,
        }
    }

    /// The KV backend, when this runs inside the broker.
    pub fn kv(&self) -> Option<&Arc<dyn KvBackend>> {
        match self {
            Store::Kv(k) => Some(k),
            _ => None,
        }
    }

    /// Whether any persistence is configured.
    pub fn is_some(&self) -> bool {
        !matches!(self, Store::None)
    }
}
