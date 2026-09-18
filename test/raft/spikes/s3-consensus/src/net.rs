//! `RaftNetworkFactory` / `RaftNetworkV2` over the framed TCP transport.
//!
//! One connection per target for the protocol RPCs (append, vote, pre-vote,
//! transfer_leader), and a separate connection for every snapshot transfer so
//! a 10 GiB stream cannot queue behind — or in front of — a heartbeat
//! (PLAN_RAFT.md §12.5).

use std::future::Future;
use std::io;
use std::time::Duration;

use openraft::errors::NetworkError;
use openraft::errors::RPCError;
use openraft::errors::ReplicationClosed;
use openraft::errors::StreamingError;
use openraft::errors::Unreachable;
use openraft::network::v2::RaftNetworkV2;
use openraft::network::RPCOption;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::TransferLeaderResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::OptionalSend;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::sm::SnapshotHandle;
use crate::types::Node;
use crate::types::NodeId;
use crate::types::TypeConfig;
use crate::types::Vote;
use crate::wire;
use crate::wire::Req;
use crate::wire::Resp;

/// Bytes per `SnapChunk`.
pub const CHUNK: usize = 1024 * 1024;

/// Floor for the append/snapshot RPC deadline.
///
/// openraft builds the replication `RPCOption` from `heartbeat_interval`
/// (replication/mod.rs: `rpc_timeout = heartbeat_interval`), so with D14's
/// 100 ms heartbeat every append round gets a 100 ms hard / 75 ms soft budget.
/// A follower that fsyncs its log and its state machine needs more than that,
/// and a transport that enforces 75 ms tears the replication stream down and
/// rebuilds it on every slow append. Votes keep the library's budget, which is
/// what makes elections fast.
pub const APPEND_TIMEOUT_FLOOR: Duration = Duration::from_secs(5);

#[derive(Clone, Default)]
pub struct NetworkFactory;

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Client;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        Client {
            target,
            addr: node.raft_addr.clone(),
            conn: None,
        }
    }
}

pub struct Client {
    #[allow(dead_code)]
    target: NodeId,
    addr: String,
    conn: Option<TcpStream>,
}

impl Client {
    async fn connect(&mut self) -> io::Result<&mut TcpStream> {
        if self.conn.is_none() {
            let s = TcpStream::connect(&self.addr).await?;
            s.set_nodelay(true)?;
            self.conn = Some(s);
        }
        Ok(self.conn.as_mut().unwrap())
    }

    /// One request, one response, with the soft TTL as the deadline. Any error
    /// drops the connection so the next call reconnects.
    async fn call(&mut self, req: Req, ttl: Duration) -> Result<Resp, RPCError<TypeConfig>> {
        let res = tokio::time::timeout(ttl, async {
            let s = self.connect().await?;
            wire::send(s, &req).await?;
            wire::recv::<_, Resp>(s).await
        })
        .await;

        match res {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) => {
                self.conn = None;
                Err(RPCError::Unreachable(Unreachable::new(&e)))
            }
            Err(_elapsed) => {
                self.conn = None;
                Err(RPCError::Unreachable(Unreachable::new(&io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("no answer from {} within {:?}", self.addr, ttl),
                ))))
            }
        }
    }
}

fn protocol_err<T>(what: &str) -> Result<T, RPCError<TypeConfig>> {
    Err(RPCError::Network(NetworkError::from_string(format!(
        "unexpected answer to {what}"
    ))))
}

impl RaftNetworkV2<TypeConfig> for Client {
    type SnapshotData = SnapshotHandle;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let ttl = option.soft_ttl().max(APPEND_TIMEOUT_FLOOR);
        match self.call(Req::Append(rpc), ttl).await? {
            Resp::Append(Ok(r)) => Ok(r),
            Resp::Append(Err(e)) => Err(RPCError::Network(NetworkError::from_string(e))),
            _ => protocol_err("append_entries"),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        match self.call(Req::Vote(rpc), option.soft_ttl()).await? {
            Resp::Vote(Ok(r)) => Ok(r),
            Resp::Vote(Err(e)) => Err(RPCError::Network(NetworkError::from_string(e))),
            _ => protocol_err("vote"),
        }
    }

    /// Pre-vote must be a real RPC: the default implementation grants
    /// unconditionally, which would make an isolated node elect itself
    /// (D14 wants pre-vote on, and on means asking the peers).
    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        match self.call(Req::PreVote(rpc), option.soft_ttl()).await? {
            Resp::Vote(Ok(r)) => Ok(r),
            Resp::Vote(Err(e)) => Err(RPCError::Network(NetworkError::from_string(e))),
            _ => protocol_err("pre_vote"),
        }
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        match self
            .call(Req::TransferLeader(req), option.soft_ttl())
            .await?
        {
            Resp::Unit(Ok(())) => Ok(Ok(())),
            Resp::Unit(Err(e)) => Err(RPCError::Network(NetworkError::from_string(e))),
            _ => protocol_err("transfer_leader"),
        }
    }

    /// Stream a manifest snapshot on its own connection, file by file, with a
    /// per-file xxh3. The receiver answers `SnapBegin` with the files it is
    /// still missing, so a killed transfer resumes per file (§11.6).
    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: openraft::alias::SnapshotOf<TypeConfig, SnapshotHandle>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let addr = self.addr.clone();
        let handle = snapshot.snapshot;
        let meta = snapshot.meta;
        tokio::pin!(cancel);

        let transfer = async move {
            let mut s = TcpStream::connect(&addr).await?;
            s.set_nodelay(true)?;

            wire::send(
                &mut s,
                &Req::SnapBegin {
                    vote,
                    meta,
                    manifest: handle.manifest.clone(),
                },
            )
            .await?;
            let missing = match wire::recv::<_, Resp>(&mut s).await? {
                Resp::SnapNeed { missing } => missing,
                _ => return Err(io::Error::other("unexpected answer to SnapBegin")),
            };
            tracing::info!(
                "snapshot to {addr}: {} of {} files needed",
                missing.len(),
                handle.manifest.files.len()
            );

            for f in handle
                .manifest
                .files
                .iter()
                .filter(|f| missing.contains(&f.name))
            {
                wire::send(
                    &mut s,
                    &Req::SnapFileStart {
                        name: f.name.clone(),
                    },
                )
                .await?;
                let mut file = tokio::fs::File::open(handle.dir.join(&f.name)).await?;
                let mut left = f.size;
                let mut buf = vec![0u8; CHUNK];
                while left > 0 {
                    let want = std::cmp::min(left as usize, CHUNK);
                    tokio::io::AsyncReadExt::read_exact(&mut file, &mut buf[..want]).await?;
                    wire::send(
                        &mut s,
                        &Req::SnapChunk {
                            data: buf[..want].to_vec(),
                        },
                    )
                    .await?;
                    left -= want as u64;
                }
                wire::send(&mut s, &Req::SnapFileEnd { xxh3: f.xxh3 }).await?;
                match wire::recv::<_, Resp>(&mut s).await? {
                    Resp::Unit(Ok(())) => {}
                    Resp::Unit(Err(e)) => return Err(io::Error::other(e)),
                    _ => return Err(io::Error::other("unexpected answer to SnapFileEnd")),
                }
            }

            wire::send(&mut s, &Req::SnapEnd).await?;
            let resp = match wire::recv::<_, Resp>(&mut s).await? {
                Resp::Snapshot(Ok(r)) => r,
                Resp::Snapshot(Err(e)) => return Err(io::Error::other(e)),
                _ => return Err(io::Error::other("unexpected answer to SnapEnd")),
            };
            let _ = s.shutdown().await;
            Ok::<_, io::Error>(resp)
        };

        let _ = option;
        tokio::select! {
            closed = &mut cancel => Err(StreamingError::Closed(closed)),
            res = transfer => match res {
                Ok(r) => Ok(r),
                Err(e) => Err(StreamingError::from(RPCError::<TypeConfig>::Unreachable(Unreachable::new(&e)))),
            },
        }
    }
}
