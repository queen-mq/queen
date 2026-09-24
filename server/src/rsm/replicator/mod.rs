//! The consensus seam (PLAN_RAFT.md §12.1): the [`Replicator`] trait the rest
//! of the RSM talks to, the [`StateMachine`] trait the apply side presents, and
//! the errors between them. Two backends implement it: [`local::LocalReplicator`]
//! (a single node, no consensus protocol) and [`raft::RaftReplicator`]
//! (openraft's protocol over the same queue logs and apply thread);
//! [`node::NodeReplicator`] picks one at boot (`QUEEN_RAFT_REPLICATOR`). The
//! rest of the node — the batcher (§7.1), the forwarder (§9.2), the reads
//! (§9.4) — cannot tell one from the other.
//!
//! # What phase 1 wires, and one honest deviation
//!
//! The traits below are §12.1 verbatim. [`LocalReplicator`] implements
//! [`Replicator`]; it does NOT drive the apply side through the [`StateMachine`]
//! trait. WP-1.4's apply thread (`rsm::apply`) is already a `std` thread that
//! consumes committed entries from a channel and reports back through
//! `apply::Notify` (applied / wake / durable), and that channel-and-notify seam
//! is what resolves a `propose` after LOCAL apply (I4) and what tells the log
//! when it may drop files behind a durable point (§11.4). [`StateMachine`] is
//! carried here as the documented seam the openraft adapter (WP-3.3) will
//! implement — its `SnapshotHandle` / `StagedSnapshot` are WP-4.6's — so the
//! shape is fixed now and phase 3 does not reshape the trait the batcher was
//! written against. See the WP-1.6a row of RAFT_STATUS.md.
//!
//! # The propose contract (§7.1, I3, I4)
//!
//! [`Replicator::propose`] resolves only after the entry is committed AND
//! applied on this node. Its errors ([`ProposeError`]) carry the §7.1
//! meanings the planner acts on:
//!
//! - [`ProposeError::NotLeader`] — refused before the log append; the overlay
//!   is dropped and the waiters retry against the hinted leader.
//! - [`ProposeError::OutcomeUnknown`] — leadership was lost AFTER the append;
//!   the entry may still commit under the new leader, so the retry (same
//!   request id) finds it or plans anew (§5.4, I6). The planner stops.
//! - [`ProposeError::Timeout`] — still leader, no answer inside the deadline
//!   (D13's `propose` deadline). The waiters get `Retry` but the entry STAYS
//!   IN FLIGHT (I3): it may still commit, so the planner does not plan the
//!   next cycle until the entry applies or the role changes. `LocalReplicator`
//!   keeps that promise by leaving the entry in its log and its pipeline; only
//!   a stalled disk produces a `Timeout` on a single node.
//! - [`ProposeError::Refused`] / [`ProposeError::Fatal`] — a malformed
//!   proposal, or a log/apply failure that stops this node (§12.1 `Fatal`).

use std::io;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;

pub mod fake;
pub mod local;
pub mod log;
pub mod node;

/// The openraft replicator: openraft's protocol over the queue logs and the
/// apply thread, behind the same [`Replicator`] trait (see its module header).
pub mod raft;

// Callers reach the concrete types by their module path, the way the rest of
// `rsm` does (`rsm::apply::Applier`, `rsm::store::HeedStore`): `local`, `fake`
// and `log` are public, so `rsm::replicator::local::LocalReplicator`,
// `fake::FakeReplicator` and `log::LogStore` are the addresses WP-1.6b's
// batcher and WP-1.7's seam use. No blanket re-export here.

/// A Raft node id (§12.3 TypeConfig: `NodeId = u64`).
pub type NodeId = u64;

/// The role this node plays. A single-node [`LocalReplicator`] is always
/// [`Role::Leader`] with term 1 until it stops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Leader { term: u64 },
    Follower { leader: Option<NodeId> },
    Learner,
    Candidate,
    Stopped,
}

impl Role {
    pub fn is_leader(&self) -> bool {
        matches!(self, Role::Leader { .. })
    }

    pub fn leader_hint(&self) -> Option<NodeId> {
        match self {
            Role::Leader { .. } => None,
            Role::Follower { leader } => *leader,
            _ => None,
        }
    }
}

/// Where a committed-and-applied entry landed. The forward server returns each
/// command's outcome with this commit index (§7.1 step 4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppliedAt {
    pub index: u64,
    pub term: u64,
}

/// Why a [`Replicator::propose`] did not return a committed-and-applied entry
/// (§12.1 verbatim; the §7.1 meanings are in the module header).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposeError {
    NotLeader { hint: Option<NodeId> },
    OutcomeUnknown,
    Timeout,
    Refused(String),
    Fatal(String),
}

impl std::fmt::Display for ProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProposeError::NotLeader { hint } => write!(f, "not the leader (hint: {hint:?})"),
            ProposeError::OutcomeUnknown => {
                write!(f, "leadership lost after append; outcome unknown")
            }
            ProposeError::Timeout => write!(f, "propose deadline elapsed; entry kept in flight"),
            ProposeError::Refused(s) => write!(f, "propose refused: {s}"),
            ProposeError::Fatal(s) => write!(f, "propose fatal: {s}"),
        }
    }
}

impl std::error::Error for ProposeError {}

/// Why a membership or leadership operation failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplError {
    NotLeader {
        hint: Option<NodeId>,
    },
    /// The backend cannot do this here (a single-node [`LocalReplicator`] has
    /// no peer to transfer to or add).
    Unsupported(String),
    Timeout,
    Fatal(String),
}

impl std::fmt::Display for ReplError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplError::NotLeader { hint } => write!(f, "not the leader (hint: {hint:?})"),
            ReplError::Unsupported(s) => write!(f, "unsupported: {s}"),
            ReplError::Timeout => write!(f, "deadline elapsed"),
            ReplError::Fatal(s) => write!(f, "fatal: {s}"),
        }
    }
}

impl std::error::Error for ReplError {}

/// The voter and learner sets (§6.1 `meta.membership`). Minimal for phase 1;
/// the openraft adapter maps this to its own membership config.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Membership {
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
}

/// One member of the cluster as the raft LEADER last saw it
/// ([`MembersView`]).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MemberSeen {
    pub id: NodeId,
    pub voter: bool,
    /// The member's client address (`QUEEN_RAFT_PEERS`), empty when unknown.
    pub http: String,
    /// The member's Raft RPC address, empty when unknown.
    pub raft: String,
    /// Milliseconds since the leader last had an RPC to this member
    /// acknowledged — a heartbeat or an append — when the view was taken. 0 for
    /// the leader itself. A member not heard from since this leader was
    /// elected counts from the election. `None` only when not even that is
    /// known.
    pub last_ack_ms: Option<u64>,
    /// The last log index the leader knows the member holds (RSM numbering).
    pub matched: Option<u64>,
}

/// Who is in the cluster and when the raft leader last heard from each
/// member: the leader's own observations, which is what every node judges a
/// member's liveness by (`GET /api/v1/raft/members`). A follower holds a copy
/// it fetched from the leader over the Raft RPC port — never through the
/// client pipeline — so the view keeps flowing when the data path is saturated.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MembersView {
    /// The leader that took the view.
    pub leader: NodeId,
    pub term: u64,
    pub members: Vec<MemberSeen>,
}

/// What one node knows about the cluster's members.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterMembers {
    /// This node.
    pub node_id: NodeId,
    /// The leader as this node knows it.
    pub leader: Option<NodeId>,
    pub term: u64,
    /// The leader's observations: taken here on the leader, a fetched copy on a
    /// follower. `None` before a follower has fetched one.
    pub view: Option<MembersView>,
    /// How old `view` is: 0 on the leader, the copy's age on a follower.
    pub view_age: Option<std::time::Duration>,
}

impl ClusterMembers {
    /// A cluster of one: this node, the leader, heard from now.
    pub fn single(node_id: NodeId, term: u64) -> ClusterMembers {
        ClusterMembers {
            node_id,
            leader: Some(node_id),
            term,
            view: Some(MembersView {
                leader: node_id,
                term,
                members: vec![MemberSeen {
                    id: node_id,
                    voter: true,
                    http: String::new(),
                    raft: String::new(),
                    last_ack_ms: Some(0),
                    matched: None,
                }],
            }),
            view_age: Some(std::time::Duration::ZERO),
        }
    }
}

impl Membership {
    /// The single-voter membership of raft1 / embedded (D2).
    pub fn single(node: NodeId) -> Membership {
        Membership {
            voters: vec![node],
            learners: Vec::new(),
        }
    }
}

/// A membership change (§12.3, §12.6). Phase 1's single node cannot apply any
/// of these; the openraft adapter (phase 3/4) does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MembershipChange {
    AddLearner { node: NodeId, addr: String },
    Promote { node: NodeId },
    Remove { node: NodeId },
}

/// What `/health`, `/metrics/prometheus` and the dashboard read (§12.3
/// "Metrics"). Node-local, a snapshot in time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplMetrics {
    pub term: u64,
    pub leader: Option<NodeId>,
    pub is_leader: bool,
    /// The last index the log holds durably (fsynced). On a single node this
    /// is also the committed index.
    pub last_log_index: u64,
    pub committed_index: u64,
    /// The last index this node has APPLIED to its store (what readiness and
    /// stale reads gate on, §11.5 step 6).
    pub applied_index: u64,
    /// The last index a durable point covered (§11.4).
    pub durable_index: u64,
    /// `queen_raft_inflight` (I3): appended but not yet applied on this node.
    pub inflight: u64,
    /// Proposals accepted since open.
    pub proposals: u64,
    pub log_files: u64,
    pub log_bytes: u64,
}

// ---------------------------------------------------------------------------
// The seam (§12.1, verbatim)
// ---------------------------------------------------------------------------

/// The consensus backend the rest of the RSM talks to (§12.1). One entry is a
/// batch of commands' effects (§5.1); the backend commits it, applies it on
/// this node, and resolves [`Replicator::propose`].
#[async_trait]
pub trait Replicator: Send + Sync + 'static {
    /// Leader only. Resolves after the entry is committed AND applied on this
    /// node (I4). `entry` is one encoded [`Entry`](crate::rsm::entry).
    ///
    /// # Submission ordering (I5, WP-1.11 F-1)
    ///
    /// SEMANTIC CONTRACT. When several `propose` futures are outstanding at
    /// once, the backend MUST assign their log indexes in the order the driver
    /// FIRST-POLLED them — not the order they later suspend, wake or commit.
    /// This is stated on the ordering the caller depends on, deliberately NOT on
    /// any tokio future-shape detail of how a backend achieves it; a backend is
    /// free to satisfy it with an explicit per-propose sequence number the log
    /// sorts by, by serializing its submissions, or any other means.
    ///
    /// Why it exists: the batcher (§7.1) stamps `now_us` monotone in plan order
    /// and first-polls each `propose` from its one driver task in that same
    /// order ([`crate::rsm::batcher`]). With this contract the log index then
    /// follows the `now_us` stamp, so apply never sees `now_us` go backwards
    /// with the index (I5). A backend that assigned indexes in commit or enqueue
    /// order instead would let concurrent proposes reorder the log and poison
    /// the node at the ratified pipeline depth (D4) — the WP-1.11 F-1 bug.
    ///
    /// [`local::LocalReplicator`] satisfies the contract by handing the entry to
    /// its writer channel in the future's first-poll synchronous prefix, before
    /// its first `.await`; there first-poll order IS the index order. That is one
    /// valid realization of the guarantee above, not the guarantee itself.
    ///
    /// [`raft::RaftReplicator`] meets the contract explicitly: it cannot rely
    /// on `client_write` enqueueing before its first suspension, so its
    /// `propose_entry` pushes the entry onto an unbounded channel in the
    /// first-poll synchronous prefix and ONE submitter task hands the entries
    /// to openraft in channel order. The batcher also checks every answer
    /// against the index it predicted and stops on a mismatch.
    async fn propose(&self, entry: Bytes, deadline: Instant) -> Result<AppliedAt, ProposeError>;

    /// Whether [`Replicator::propose_entry`] needs the encoded bytes. `false`
    /// lets the batcher skip encoding the entry on its serial task (it runs
    /// `Entry::validate` instead): the local replicator on the queue-log path
    /// takes the planned entry itself and never writes the encoded form.
    fn wants_bytes(&self) -> bool {
        true
    }

    /// [`Replicator::propose`] with the planned entry in hand, so a backend
    /// that keeps the entry need not decode `entry` again. The default ignores
    /// `planned`. Same submission-ordering contract as `propose`.
    async fn propose_entry(
        &self,
        entry: Bytes,
        planned: std::sync::Arc<crate::rsm::entry::Entry>,
        deadline: Instant,
    ) -> Result<AppliedAt, ProposeError> {
        let _ = planned;
        self.propose(entry, deadline).await
    }

    fn role(&self) -> Role;

    fn watch_role(&self) -> tokio::sync::watch::Receiver<Role>;

    /// A notification pulsed whenever this node's applied index advances
    /// (PERF-G, `QUEEN_RAFT_DRIVER_NOTIFY`). A driver in driver-notify mode
    /// wakes on it and resolves every in-flight entry whose index the applied
    /// index has now passed — the freed pipeline slot is reused on ONE
    /// cross-thread wake (the apply thread → the driver), instead of the
    /// two-hop `oneshot` → forwarding task → `mpsc` path the per-propose await
    /// takes. It is a pure NODE-LOCAL wake, never state, and it never replaces
    /// the propose future: that future still runs (it drives commit on a
    /// backend that resolves it, and it still delivers `Timeout`/`Fatal`),
    /// so a resolution the notify reaches first only answers the entry's
    /// waiters sooner — commit + local apply (D7, I4) still gate it, because
    /// the notify fires only AFTER `applied_index` advanced past the entry.
    ///
    /// The default is `None`: a backend that does not expose the signal makes
    /// the driver keep its per-propose await path, so this is never a
    /// correctness gap, only the absence of the latency shortcut.
    /// [`local::LocalReplicator`] and [`fake::FakeReplicator`] override it.
    fn applied_notify(&self) -> Option<std::sync::Arc<tokio::sync::Notify>> {
        None
    }

    /// A linearizable read index (§9.4): apply must reach it before a read is
    /// answered. On a single node it is the current applied index.
    async fn read_barrier(&self, deadline: Instant) -> Result<u64, ProposeError>;

    fn applied_index(&self) -> u64;

    async fn transfer_leadership(
        &self,
        to: Option<NodeId>,
        deadline: Instant,
    ) -> Result<(), ReplError>;

    async fn membership(&self) -> Membership;

    async fn change_membership(
        &self,
        change: MembershipChange,
        deadline: Instant,
    ) -> Result<(), ReplError>;

    fn metrics(&self) -> ReplMetrics;
}

/// The state machine (§12.1). Implemented by the apply side. In phase 1 the
/// apply thread of WP-1.4 fills this role through its own channel-and-notify
/// seam (see the module header); this trait is the shape WP-3.3's openraft
/// adapter implements, so the batcher is written against a stable seam.
pub trait StateMachine: Send + 'static {
    /// Apply one committed entry, in index order.
    fn apply(&mut self, index: u64, term: u64, entry: &[u8]) -> ApplyResult;
    /// `(applied index, applied term, membership)`.
    fn applied(&self) -> (u64, u64, Membership);
    /// Take a durable point and return the index it covered (§11.4).
    fn durable_point(&mut self) -> io::Result<u64>;
    /// Build a snapshot for transfer (§11.6). WP-4.6 defines the handle.
    fn build_snapshot(&mut self) -> io::Result<SnapshotHandle>;
    /// Install a received snapshot (§11.6, I17). WP-4.6 defines the staging.
    fn install_snapshot(&mut self, staged: StagedSnapshot) -> io::Result<()>;
}

/// The result of applying one entry. Phase 1's apply thread reports the richer
/// per-command outcomes through `apply::Notify::applied`; this is the shape the
/// synchronous [`StateMachine::apply`] seam returns.
pub type ApplyResult = io::Result<()>;

/// A built snapshot ready to transfer (§11.6). A placeholder WP-4.6 fills:
/// phase 1 has no snapshots (single voter repairs from its own log, §11.5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotHandle {
    pub index: u64,
    pub term: u64,
    pub dir: std::path::PathBuf,
}

/// A received snapshot staged for install (§11.6, I17). A placeholder WP-4.6
/// fills.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedSnapshot {
    pub index: u64,
    pub term: u64,
    pub dir: std::path::PathBuf,
}
