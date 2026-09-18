//! Client side of the framed protocol, used by the scenario driver.

use std::io;
use std::time::Duration;

use tokio::net::TcpStream;

use crate::types::NodeId;
use crate::wire;
use crate::wire::Req;
use crate::wire::Resp;
use crate::wire::StatusResp;
use crate::wire::WriteErr;

pub struct Conn {
    #[allow(dead_code)]
    pub addr: String,
    stream: TcpStream,
}

impl Conn {
    pub async fn connect(addr: &str) -> io::Result<Conn> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Conn {
            addr: addr.to_string(),
            stream,
        })
    }

    pub async fn call(&mut self, req: Req) -> io::Result<Resp> {
        wire::send(&mut self.stream, &req).await?;
        wire::recv(&mut self.stream).await
    }

    pub async fn status(&mut self) -> io::Result<StatusResp> {
        match self.call(Req::Status).await? {
            Resp::Status(s) => Ok(s),
            _ => Err(io::Error::other("unexpected answer to Status")),
        }
    }

    pub async fn write(
        &mut self,
        id: u64,
        payload: Vec<u8>,
    ) -> io::Result<Result<(u64, u64), WriteErr>> {
        match self.call(Req::Write { id, payload }).await? {
            Resp::Write(r) => Ok(r),
            _ => Err(io::Error::other("unexpected answer to Write")),
        }
    }

    pub async fn lin_read(&mut self, mode: u8) -> io::Result<Result<(u64, u64), String>> {
        match self.call(Req::LinRead { mode }).await? {
            Resp::LinRead(r) => Ok(r),
            _ => Err(io::Error::other("unexpected answer to LinRead")),
        }
    }

    pub async fn unit(&mut self, req: Req) -> io::Result<Result<(), String>> {
        match self.call(req).await? {
            Resp::Unit(r) => Ok(r),
            _ => Err(io::Error::other("unexpected answer, wanted Unit")),
        }
    }

    pub async fn verify_log(&mut self) -> io::Result<Result<crate::wire::LogVerdict, String>> {
        match self.call(Req::VerifyLog).await? {
            Resp::VerifyLog(v) => Ok(v),
            _ => Err(io::Error::other("unexpected answer to VerifyLog")),
        }
    }

    pub async fn applied_ids(&mut self) -> io::Result<Vec<u64>> {
        match self.call(Req::AppliedIds).await? {
            Resp::AppliedIds(v) => Ok(v),
            _ => Err(io::Error::other("unexpected answer to AppliedIds")),
        }
    }
}

/// Connect, retrying until the deadline (a node that has just been spawned
/// may not be listening yet).
pub async fn connect_retry(addr: &str, within: Duration) -> io::Result<Conn> {
    let deadline = std::time::Instant::now() + within;
    let mut last = None;
    while std::time::Instant::now() < deadline {
        match Conn::connect(addr).await {
            Ok(c) => return Ok(c),
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    Err(last.unwrap_or_else(|| io::Error::other("no attempt")))
}

/// The node that says of itself that it is the leader.
///
/// `current_leader` in the metrics is NOT evidence: after a restart every
/// follower still reports the leader of its persisted vote, while with
/// `enable_leader_restore = false` that node is not a leader any more. Only
/// `is_leader` on the node itself is proof.
pub async fn find_leader(addrs: &[(NodeId, String)], within: Duration) -> Option<(NodeId, String)> {
    let deadline = std::time::Instant::now() + within;
    loop {
        for (id, addr) in addrs {
            if let Ok(mut c) = Conn::connect(addr).await {
                if let Ok(s) = c.status().await {
                    if s.is_leader {
                        return Some((*id, addr.clone()));
                    }
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// What the nodes *believe* about leadership, without asking the leader
/// itself: used to show the difference after a restart.
pub async fn believed_leader(addrs: &[(NodeId, String)]) -> Vec<(NodeId, Option<NodeId>, bool)> {
    let mut out = Vec::new();
    for (id, addr) in addrs {
        if let Ok(mut c) = Conn::connect(addr).await {
            if let Ok(s) = c.status().await {
                out.push((*id, s.current_leader, s.is_leader));
            }
        }
    }
    out
}
