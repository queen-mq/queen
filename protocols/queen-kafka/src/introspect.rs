//! What a running facade says about itself to the process hosting it.
//!
//! The in-process facade (server/src/kafka_inproc.rs) reports its phase in the
//! broker's `GET /status`; this is the rest of that block: the live set the
//! facade advertises and how it judges it, and the idempotent-producer
//! tracker's size and evictions — the two numbers an operator needs to see
//! whether the cluster is whole and whether producers are being forced to
//! bump their epochs.
//!
//! Held WEAKLY, so a facade that stops (and one the in-process supervisor
//! restarts) is never kept alive by the report; the newest registration wins.

use std::sync::{Mutex, Weak};

use serde_json::json;

use crate::cluster::ClusterState;
use crate::idempotent::Producers;
use crate::Facade;

struct Registered {
    cluster: Option<Weak<ClusterState>>,
    producers: Weak<Producers>,
    max_partitions: u32,
    default_partitions: u32,
}

static CURRENT: Mutex<Option<Registered>> = Mutex::new(None);

/// Report on `facade` from now on. Called once per serve, at boot.
pub fn register(facade: &Facade) {
    let registered = Registered {
        cluster: facade.cluster.state().map(std::sync::Arc::downgrade),
        producers: std::sync::Arc::downgrade(&facade.producers),
        max_partitions: facade.max_partitions,
        default_partitions: facade.default_partitions,
    };
    if let Ok(mut current) = CURRENT.lock() {
        *current = Some(registered);
    }
}

/// The facade's own report, or `None` when no facade has registered (or the
/// one that did has stopped).
pub fn snapshot() -> Option<serde_json::Value> {
    let current = CURRENT.lock().ok()?;
    let r = current.as_ref()?;
    let producers = r.producers.upgrade()?;
    let cluster = r.cluster.as_ref().and_then(Weak::upgrade).map(|state| {
        let view = state.view();
        json!({
            "nodeId": state.me.id,
            "liveness": if state.raft_liveness() { "raft" } else { "registry" },
            "raftNode": state.raft_node(),
            "live": view.as_ref().map(|v| v.nodes.iter().map(|n| n.id).collect::<Vec<_>>()),
            "down": view.as_ref().map(|v| v.down.iter().map(|n| n.id).collect::<Vec<_>>()),
            "viewAgeMs": view.as_ref().map(|v| v.read_at.elapsed().as_millis() as u64),
            "coordinating": state.coordinating(),
        })
    });
    Some(json!({
        "maxPartitions": r.max_partitions,
        "defaultPartitions": r.default_partitions,
        "producers": {
            "tracked": producers.tracked(),
            "capacity": producers.capacity(),
            "evictions": producers.evictions(),
        },
        "cluster": cluster,
        "consume": crate::stats::snapshot(),
    }))
}
