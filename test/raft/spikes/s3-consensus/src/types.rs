//! The openraft type configuration for the spike.
//!
//! The application data is opaque bytes, exactly as PLAN_RAFT.md §12.1 states
//! the seam: `propose(entry: Bytes)`. The first 8 bytes of a payload are the
//! client's write id; the state machine records them so a scenario can prove
//! that no acknowledged write went missing.

use serde::Deserialize;
use serde::Serialize;

pub type NodeId = u64;
pub type Node = openraft::NodeInfo;

/// One client write: an opaque entry, plus the id the checker follows.
#[derive(Clone, Serialize, Deserialize)]
pub struct AppRequest {
    /// Client-assigned id of this write; unique within a scenario run.
    pub id: u64,
    /// Opaque entry bytes (the RSM's serialized effects, in the real system).
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

impl std::fmt::Debug for AppRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AppRequest{{id:{}, {} bytes}}",
            self.id,
            self.payload.len()
        )
    }
}

/// `AppData` requires `Display`; never print the payload itself.
impl std::fmt::Display for AppRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "write(id={}, {} bytes)", self.id, self.payload.len())
    }
}

/// What the state machine answers for one applied entry.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppResponse {
    /// Number of entries applied on this node, including this one.
    pub applied_count: u64,
}

openraft::declare_raft_types!(
    /// Type config of the S3 spike: opaque byte entries, u64 node ids,
    /// `NodeInfo` (raft_addr) as the node record.
    ///
    /// `LeaderId` is pinned to the advanced form so a log id carries its term
    /// in a readable field (the spike names snapshot directories `<index>-<term>`).
    pub TypeConfig:
        D = AppRequest,
        R = AppResponse,
        NodeId = NodeId,
        Node = Node,
        Term = u64,
        LeaderId = openraft::impls::leader_id_adv::LeaderId<Self::Term, Self::NodeId>,
);

pub type Raft = openraft::Raft<TypeConfig, crate::sm::SmStore>;
pub type LogId = openraft::alias::LogIdOf<TypeConfig>;
pub type SnapshotMeta = openraft::alias::SnapshotMetaOf<TypeConfig>;
pub type StoredMembership = openraft::alias::StoredMembershipOf<TypeConfig>;
pub type Vote = openraft::alias::VoteOf<TypeConfig>;
