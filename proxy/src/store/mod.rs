//! Where the proxy keeps its state (PLAN_SINGLE_BINARY.md W3/W4): the
//! broker's replicated KV, under a reserved system tenant — the one pipeline
//! every other Queen feature uses, no proxy-specific raft effect or keyspace
//! (feedback: "facades translate to Queen commands").
//!
//! Every read and write goes through a domain repository here:
//!
//! | module | documents | owner |
//! |---|---|---|
//! | [`data`] | tenants, cells, plans, clusters, cluster_roles, api_keys, revoked_tokens, queues | W3 data plane |
//! | [`web`] | users, identities, operations, outbox | W4 web plane |
//! | [`usage`] | usage_minutes, usage_days | W3 metering |
//!
//! [`schema`] fixes the KV layout (namespaces, keys, index keys) every one of
//! them must agree on; [`kv`] is the backend seam and the typed helpers;
//! [`memkv`] is an in-memory backend with the broker's KV semantics, for
//! tests.

use std::sync::Arc;

pub mod data;
pub mod kv;
pub mod memkv;
pub mod schema;
pub mod seed;
pub mod usage;
pub mod web;

pub use kv::{KvBackend, KvError};

/// The proxy's state backend.
#[derive(Clone)]
pub enum Store {
    /// The broker's replicated KV (system tenant).
    Kv(Arc<dyn KvBackend>),
    /// No persistence: reads answer "nothing", writes refuse. Tests only —
    /// the broker always hands the proxy its KV.
    None,
}

impl Store {
    /// The KV backend.
    pub fn kv(&self) -> Option<&Arc<dyn KvBackend>> {
        match self {
            Store::Kv(k) => Some(k),
            Store::None => None,
        }
    }

    /// Whether any persistence is configured.
    pub fn is_some(&self) -> bool {
        !matches!(self, Store::None)
    }
}
