//! The replicator a node runs, chosen at boot by `QUEEN_RAFT_REPLICATOR`:
//!
//! - `local` (the default): [`LocalReplicator`], a single node with no
//!   consensus protocol;
//! - `openraft` (or `raft`): [`RaftReplicator`], openraft's protocol over the
//!   same queue logs and the same apply thread.
//!
//! Both implement [`Replicator`]; the batcher and the facade cannot tell them
//! apart. A data directory belongs to the replicator that created it: each one
//! refuses a directory the other wrote.

use std::io;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{watch, Notify as ApplyWake};

use super::local::{LocalReplicator, OpenConfig, Waker};
use super::raft::{ClusterConfig, RaftReplicator};
use super::{
    AppliedAt, Membership, MembershipChange, NodeId, ProposeError, ReplError, ReplMetrics,
    Replicator, Role,
};
use crate::rsm::apply::{self, ApplyStats};
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::segments;
use crate::rsm::store::Store;

/// Which replicator a node runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicatorKind {
    Local,
    Raft,
}

impl ReplicatorKind {
    /// `QUEEN_RAFT_REPLICATOR`: `local` (default) or `openraft` / `raft`.
    pub fn from_env() -> Result<ReplicatorKind, String> {
        match std::env::var("QUEEN_RAFT_REPLICATOR") {
            Err(_) => Ok(ReplicatorKind::Local),
            Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
                "" | "local" => Ok(ReplicatorKind::Local),
                "openraft" | "raft" => Ok(ReplicatorKind::Raft),
                other => Err(format!(
                    "QUEEN_RAFT_REPLICATOR={other}: expected `local` or `openraft`"
                )),
            },
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            ReplicatorKind::Local => "local",
            ReplicatorKind::Raft => "openraft",
        }
    }
}

/// The node's replicator.
pub enum NodeReplicator<S: Store + 'static> {
    Local(LocalReplicator<S>),
    Raft(RaftReplicator<S>),
}

impl<S: Store + 'static> NodeReplicator<S> {
    /// Open the chosen replicator over `store`. A boot call (blocking I/O).
    pub fn open(
        kind: ReplicatorKind,
        store: Arc<S>,
        cfg: OpenConfig,
        waker: Arc<dyn Waker>,
        clock: Arc<dyn apply::Clock>,
    ) -> io::Result<NodeReplicator<S>> {
        NodeReplicator::open_with(kind, store, cfg, None, waker, clock, false)
    }

    /// [`NodeReplicator::open`] for a node of `cluster` (openraft only).
    /// `exit_on_restart`: see [`RaftReplicator::open_with`].
    pub fn open_with(
        kind: ReplicatorKind,
        store: Arc<S>,
        cfg: OpenConfig,
        cluster: Option<ClusterConfig>,
        waker: Arc<dyn Waker>,
        clock: Arc<dyn apply::Clock>,
        exit_on_restart: bool,
    ) -> io::Result<NodeReplicator<S>> {
        let data_dir = cfg
            .seg_root
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_default();
        match kind {
            ReplicatorKind::Local => {
                if cluster.is_some() {
                    return Err(io::Error::other(
                        "QUEEN_RAFT_PEERS needs QUEEN_RAFT_REPLICATOR=openraft",
                    ));
                }
                if data_dir.join("raft").join("state.json").exists() {
                    return Err(io::Error::other(format!(
                        "{} was written by the openraft replicator: run it with \
                         QUEEN_RAFT_REPLICATOR=openraft",
                        data_dir.display()
                    )));
                }
                LocalReplicator::open(store, cfg, waker, clock).map(NodeReplicator::Local)
            }
            ReplicatorKind::Raft => RaftReplicator::open_with(
                store,
                cfg,
                cluster,
                waker,
                clock,
                super::raft::RaftOpts::from_env(exit_on_restart),
            )
            .map(NodeReplicator::Raft),
        }
    }

    pub fn kind(&self) -> ReplicatorKind {
        match self {
            NodeReplicator::Local(_) => ReplicatorKind::Local,
            NodeReplicator::Raft(_) => ReplicatorKind::Raft,
        }
    }

    pub fn reader(&self) -> segments::Reader {
        match self {
            NodeReplicator::Local(r) => r.reader(),
            NodeReplicator::Raft(r) => r.reader(),
        }
    }

    /// Where a follower forwards client requests: the leader's HTTP address,
    /// when another node leads. `None` on the leader, on a single node, and
    /// while no leader is known.
    pub fn leader_http(&self) -> Option<String> {
        match self {
            NodeReplicator::Local(_) => None,
            NodeReplicator::Raft(r) => r.leader_http(),
        }
    }

    pub fn qlog_reader(&self) -> Option<QLogReader> {
        match self {
            NodeReplicator::Local(r) => r.qlog_reader(),
            NodeReplicator::Raft(r) => r.qlog_reader(),
        }
    }

    /// Stop the node; returns the apply thread's stats and the store handle.
    pub fn shutdown(self) -> io::Result<(ApplyStats, Arc<S>)> {
        match self {
            NodeReplicator::Local(r) => r.shutdown(),
            NodeReplicator::Raft(r) => r.shutdown(),
        }
    }
}

#[async_trait]
impl<S: Store + 'static> Replicator for NodeReplicator<S> {
    async fn propose(&self, entry: Bytes, deadline: Instant) -> Result<AppliedAt, ProposeError> {
        match self {
            NodeReplicator::Local(r) => r.propose(entry, deadline).await,
            NodeReplicator::Raft(r) => r.propose(entry, deadline).await,
        }
    }

    fn wants_bytes(&self) -> bool {
        match self {
            NodeReplicator::Local(r) => r.wants_bytes(),
            NodeReplicator::Raft(r) => r.wants_bytes(),
        }
    }

    async fn propose_entry(
        &self,
        entry: Bytes,
        planned: Arc<crate::rsm::entry::Entry>,
        deadline: Instant,
    ) -> Result<AppliedAt, ProposeError> {
        // Both backends submit in their first poll; this `match` adds no
        // suspension point before it.
        match self {
            NodeReplicator::Local(r) => r.propose_entry(entry, planned, deadline).await,
            NodeReplicator::Raft(r) => r.propose_entry(entry, planned, deadline).await,
        }
    }

    fn role(&self) -> Role {
        match self {
            NodeReplicator::Local(r) => r.role(),
            NodeReplicator::Raft(r) => r.role(),
        }
    }

    fn watch_role(&self) -> watch::Receiver<Role> {
        match self {
            NodeReplicator::Local(r) => r.watch_role(),
            NodeReplicator::Raft(r) => r.watch_role(),
        }
    }

    fn applied_notify(&self) -> Option<Arc<ApplyWake>> {
        match self {
            NodeReplicator::Local(r) => r.applied_notify(),
            NodeReplicator::Raft(r) => r.applied_notify(),
        }
    }

    async fn read_barrier(&self, deadline: Instant) -> Result<u64, ProposeError> {
        match self {
            NodeReplicator::Local(r) => r.read_barrier(deadline).await,
            NodeReplicator::Raft(r) => r.read_barrier(deadline).await,
        }
    }

    fn applied_index(&self) -> u64 {
        match self {
            NodeReplicator::Local(r) => r.applied_index(),
            NodeReplicator::Raft(r) => r.applied_index(),
        }
    }

    async fn transfer_leadership(
        &self,
        to: Option<NodeId>,
        deadline: Instant,
    ) -> Result<(), ReplError> {
        match self {
            NodeReplicator::Local(r) => r.transfer_leadership(to, deadline).await,
            NodeReplicator::Raft(r) => r.transfer_leadership(to, deadline).await,
        }
    }

    async fn membership(&self) -> Membership {
        match self {
            NodeReplicator::Local(r) => r.membership().await,
            NodeReplicator::Raft(r) => r.membership().await,
        }
    }

    async fn change_membership(
        &self,
        change: MembershipChange,
        deadline: Instant,
    ) -> Result<(), ReplError> {
        match self {
            NodeReplicator::Local(r) => r.change_membership(change, deadline).await,
            NodeReplicator::Raft(r) => r.change_membership(change, deadline).await,
        }
    }

    fn metrics(&self) -> ReplMetrics {
        match self {
            NodeReplicator::Local(r) => r.metrics(),
            NodeReplicator::Raft(r) => r.metrics(),
        }
    }
}
