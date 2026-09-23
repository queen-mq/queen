//! The network openraft is given on a single-voter node: none.
//!
//! A cluster of one voter never sends an RPC (it is its own quorum), so every
//! call here is a bug and is answered `Unreachable`. The multi-node transport
//! replaces this factory; nothing else in `replicator/raft` changes.

use std::future::Future;
use std::io;

use openraft::errors::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::v2::RaftNetworkV2;
use openraft::network::{RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::OptionalSend;

use super::state_machine::{Checkpoint, Snapshot};
use super::types::{Node, NodeId, TypeConfig, Vote};

fn no_peer() -> RPCError<TypeConfig> {
    RPCError::Unreachable(Unreachable::new(&io::Error::other(
        "no raft network: this node runs as a single voter",
    )))
}

#[derive(Clone, Default)]
pub(crate) struct NoNetwork;

impl RaftNetworkFactory<TypeConfig> for NoNetwork {
    type Network = NoPeer;

    async fn new_client(&mut self, _target: NodeId, _node: &Node) -> NoPeer {
        NoPeer
    }
}

pub(crate) struct NoPeer;

impl RaftNetworkV2<TypeConfig> for NoPeer {
    type SnapshotData = Checkpoint;

    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        Err(no_peer())
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        Err(no_peer())
    }

    async fn full_snapshot(
        &mut self,
        _vote: Vote,
        _snapshot: Snapshot,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        Err(StreamingError::from(no_peer()))
    }
}
