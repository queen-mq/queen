//! The wire: length-prefixed MessagePack frames on one TCP port.
//!
//! `u32 length (BE) | msgpack body`. No HTTP, as PLAN_RAFT.md D12 prescribes
//! for the real transport. The spike leaves out the HMAC handshake (that is
//! spike S4's subject) and carries client/admin calls on the same port.

use std::io;

use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use crate::sm::Manifest;
use crate::types::NodeId;
use crate::types::SnapshotMeta;
use crate::types::TypeConfig;
use crate::types::Vote;

/// Refuse anything larger; a 64 MiB frame is already far past what this spike
/// sends (chunks are 1 MiB, entry batches a few MiB).
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
pub enum Req {
    // ---- Raft protocol ----
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<TypeConfig>),
    PreVote(VoteRequest<TypeConfig>),
    TransferLeader(TransferLeaderRequest<TypeConfig>),

    // ---- snapshot session (one per connection) ----
    /// Opens a session; the answer says which files the receiver still needs.
    SnapBegin {
        vote: Vote,
        meta: SnapshotMeta,
        manifest: Manifest,
    },
    SnapFileStart {
        name: String,
    },
    SnapChunk {
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    SnapFileEnd {
        xxh3: u64,
    },
    /// Every manifest file is present: verify and install.
    SnapEnd,

    // ---- client and admin ----
    Init {
        members: Vec<(NodeId, String)>,
    },
    Write {
        id: u64,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
    },
    /// Linearizable read (§9.4). mode 0 = one `ensure_linearizable` per read,
    /// 1 = fixed window batcher, 2 = coalesce into the in-flight barrier.
    LinRead {
        mode: u8,
    },
    Status,
    AddLearner {
        id: NodeId,
        addr: String,
    },
    ChangeMembership {
        voters: Vec<NodeId>,
    },
    TriggerTransfer {
        to: NodeId,
    },
    TriggerSnapshot,
    PurgeLog {
        upto: u64,
    },
    /// Ids the state machine has applied (the checker's oracle).
    AppliedIds,
    /// Read every entry the reopened log claims to hold and report whether
    /// they are readable and contiguous: a log that reports a last index it
    /// cannot back with bytes is exactly what dropped writes would produce.
    VerifyLog,
    /// Emulate a ONE-WAY network fault: keep processing this peer's Raft RPCs
    /// (so its heartbeats still renew our follower lease) but never answer
    /// them. That is the link shape behind openraft GH#2080: a leader whose
    /// quorum-ack lease expires while its heartbeats keep the followers'
    /// leases alive.
    DropResponsesTo {
        peers: Vec<NodeId>,
    },
    Shutdown,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StatusResp {
    pub id: NodeId,
    pub is_leader: bool,
    pub current_leader: Option<NodeId>,
    pub term: u64,
    pub last_log_index: Option<u64>,
    pub last_applied: Option<u64>,
    pub applied_count: u64,
    pub digest: u64,
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
    pub snapshot_index: Option<u64>,
    pub purged: Option<u64>,
    pub live_dir: String,
    /// Leader only: what the leader believes each follower has matched
    /// (`RaftMetrics::replication`). A follower that is killed after the leader
    /// counted its ack must reopen its log at or above this index.
    #[serde(default)]
    pub replication: Option<Vec<(NodeId, Option<u64>)>>,
    /// Term of the persisted vote: it must never go backwards across a crash.
    #[serde(default)]
    pub vote_term: u64,
    #[serde(default)]
    pub committed: Option<u64>,
    /// What this process found on disk at open, before any replication
    /// (see `server::ReopenInfo`).
    #[serde(default)]
    pub reopen_last_log: Option<u64>,
    #[serde(default)]
    pub reopen_purged: Option<u64>,
    #[serde(default)]
    pub reopen_committed: Option<u64>,
    #[serde(default)]
    pub reopen_vote_term: Option<u64>,
    #[serde(default)]
    pub reopen_sm_applied: Option<u64>,
    #[serde(default)]
    pub reopen_sm_applied_count: u64,
    #[serde(default)]
    pub reopen_sm_digest: u64,
    /// ms `Raft::wait_for_recovery` took, when the node was started with it.
    #[serde(default)]
    pub recovery_ms: Option<i64>,
    /// Leader only: how long ago a quorum last acknowledged this leader
    /// (`RaftMetrics::last_quorum_acked`). §14's alert is "leader whose
    /// quorum-ack lease has expired".
    #[serde(default)]
    pub quorum_acked_ms_ago: Option<i64>,
}

/// What a read-back of the whole reopened log found.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogVerdict {
    pub purged: Option<u64>,
    pub last: Option<u64>,
    /// Entries actually read between `purged+1` and `last`.
    pub read: u64,
    /// Entries expected in that range.
    pub expected: u64,
    /// First index that could not be read, if any.
    pub hole_at: Option<u64>,
    pub error: Option<String>,
}

/// How a write ended, in the vocabulary of PLAN_RAFT.md §12.1 `ProposeError`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WriteErr {
    /// openraft answered `ForwardToLeader` before the entry was appended.
    NotLeader {
        hint: Option<NodeId>,
        reason: String,
    },
    /// The entry may still commit: `LogEntryDiscarded`, or a timeout.
    OutcomeUnknown {
        detail: String,
    },
    Fatal {
        detail: String,
    },
}

#[derive(Serialize, Deserialize)]
pub enum Resp {
    Append(Result<AppendEntriesResponse<TypeConfig>, String>),
    Vote(Result<VoteResponse<TypeConfig>, String>),
    Snapshot(Result<SnapshotResponse<TypeConfig>, String>),
    Unit(Result<(), String>),
    /// Answer to `SnapBegin`: the manifest files the receiver does not have.
    SnapNeed {
        missing: Vec<String>,
    },
    /// Answer to `Write`: the log index it was committed and applied at.
    Write(Result<(u64, u64), WriteErr>),
    /// Answer to `LinRead`: (read log id index, local applied index).
    LinRead(Result<(u64, u64), String>),
    Status(StatusResp),
    AppliedIds(Vec<u64>),
    VerifyLog(Result<LogVerdict, String>),
}

pub fn encode<T: Serialize>(v: &T) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    rmp_serde::encode::write_named(&mut buf, v).map_err(io::Error::other)?;
    Ok(buf)
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
    rmp_serde::decode::from_slice(bytes).map_err(io::Error::other)
}

pub async fn write_frame<W>(w: &mut W, body: &[u8]) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    if body.len() > MAX_FRAME {
        return Err(io::Error::other(format!(
            "frame of {} bytes is too large",
            body.len()
        )));
    }
    w.write_all(&(body.len() as u32).to_be_bytes()).await?;
    w.write_all(body).await?;
    w.flush().await
}

pub async fn read_frame<R>(r: &mut R) -> io::Result<Vec<u8>>
where
    R: AsyncReadExt + Unpin,
{
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::other(format!(
            "peer announced a {len} byte frame"
        )));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await?;
    Ok(body)
}

pub async fn send<W, T>(w: &mut W, v: &T) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let body = encode(v)?;
    write_frame(w, &body).await
}

pub async fn recv<R, T>(r: &mut R) -> io::Result<T>
where
    R: AsyncReadExt + Unpin,
    T: DeserializeOwned,
{
    let body = read_frame(r).await?;
    decode(&body)
}
