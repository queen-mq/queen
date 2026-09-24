//! Clients served from every node (`QUEEN_RAFT_CLIENT_OFFLOAD`, default on in
//! a cluster).
//!
//! A follower does its clients' work itself: it parses and validates the
//! request, prepares the command exactly as the leader would, and renders the
//! answer from its OWN state. Only the prepared command crosses to the leader,
//! whose batcher plans it with every other command; the reply comes back with
//! the index its entry landed at, and the follower waits until it has applied
//! that index itself before it answers — so a pop reads its payloads from the
//! follower's own queue logs, and a client reads its own writes wherever it
//! connects.
//!
//! The request and the reply travel as postcard (payload bytes as raw bytes),
//! over the Raft RPC listener (`/raft/v1/submit`), with the cluster token.
//! A retried command keeps its request id, so a reply lost in transit and a
//! retry never plan it twice (I6).

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::RsmError;
use crate::rsm::batcher::{Command, CommandTx, Reply, Submission};
use crate::rsm::entry::Outcome;
use crate::rsm::planner::Refusal;
use crate::rsm::replicator::AppliedAt;

/// What a follower sends the leader.
#[derive(Serialize, Deserialize)]
struct SubmitReq {
    /// How long the leader may take, from when it receives the request.
    budget_ms: u64,
    command: Command,
}

/// What the leader answers.
#[derive(Serialize, Deserialize)]
enum WireReply {
    Done {
        #[serde(with = "serde_bytes")]
        outcome: Vec<u8>,
        at: Option<(u64, u64)>,
        /// The leader's applied index when it answered: a follower answering
        /// from committed state alone (`at` = `None`, a request-id hit) waits
        /// until it has applied this far, so it reads what the leader read.
        seen: u64,
    },
    Refused(Refusal),
    Retry {
        hint: Option<u64>,
    },
    /// The leader could not take the command (its own admission budget, a
    /// timeout, a closed pipeline).
    Error {
        code: String,
        message: String,
        retry_after_s: Option<u64>,
    },
}

pub(super) fn encode_request(command: &Command, budget: Duration) -> Result<Vec<u8>, RsmError> {
    #[derive(Serialize)]
    struct Ref<'a> {
        budget_ms: u64,
        command: &'a Command,
    }
    postcard::to_stdvec(&Ref {
        budget_ms: budget.as_millis() as u64,
        command,
    })
    .map_err(|e| RsmError::Internal(format!("encode a prepared command: {e}")))
}

fn encode_reply(r: Result<Reply, RsmError>, seen: u64) -> Vec<u8> {
    let w = match r {
        Ok(Reply::Done { outcome, at }) => WireReply::Done {
            outcome: outcome.encode(),
            at: at.map(|a| (a.index, a.term)),
            seen,
        },
        Ok(Reply::Refused(r)) => WireReply::Refused(r),
        Ok(Reply::Retry { hint }) => WireReply::Retry { hint },
        Err(e) => WireReply::Error {
            code: e.code().to_string(),
            message: e.to_string(),
            retry_after_s: match e {
                RsmError::Overloaded { retry_after_s } => Some(retry_after_s),
                _ => None,
            },
        },
    };
    postcard::to_stdvec(&w).unwrap_or_default()
}

/// The reply a follower got back, as its own batcher would have answered, and
/// the index it must have applied before it answers from it.
pub(super) fn decode_reply(b: &[u8]) -> Result<(Reply, u64), RsmError> {
    let w: WireReply = postcard::from_bytes(b)
        .map_err(|e| RsmError::Internal(format!("the leader's reply does not decode: {e}")))?;
    match w {
        WireReply::Done { outcome, at, seen } => Ok((
            Reply::Done {
                outcome: Outcome::decode(&outcome)
                    .map_err(|e| RsmError::Internal(format!("the leader's outcome: {e:?}")))?,
                at: at.map(|(index, term)| AppliedAt { index, term }),
            },
            at.map_or(seen, |(index, _)| index),
        )),
        WireReply::Refused(r) => Ok((Reply::Refused(r), 0)),
        WireReply::Retry { hint } => Ok((Reply::Retry { hint }, 0)),
        WireReply::Error {
            code,
            message,
            retry_after_s,
        } => Err(match code.as_str() {
            "overloaded" => RsmError::Overloaded {
                retry_after_s: retry_after_s.unwrap_or(1),
            },
            "storage_full" => RsmError::StorageFull,
            "timeout" => RsmError::Timeout,
            "no_leader" => RsmError::NoLeader,
            "retry" => RsmError::Retry { leader_hint: None },
            _ => RsmError::Internal(format!("leader: {message}")),
        }),
    }
}

/// The leader side: plan a follower's prepared command through this node's
/// batcher, under the push admission budget, and answer the encoded reply.
/// `cmd_tx` is weak, so a follower's request never keeps a stopped facade's
/// batcher alive.
pub(super) async fn serve(
    cmd_tx: tokio::sync::mpsc::WeakSender<Submission>,
    admit: Option<&'static crate::rsm::admit::AdmitGate>,
    applied: Arc<dyn Fn() -> u64 + Send + Sync>,
    body: bytes::Bytes,
) -> Result<bytes::Bytes, String> {
    let req: SubmitReq =
        postcard::from_bytes(&body).map_err(|e| format!("prepared command: {e}"))?;
    let deadline = Instant::now() + Duration::from_millis(req.budget_ms.clamp(1, 120_000));
    let reply = submit_local(cmd_tx, admit, req.command, deadline).await;
    Ok(bytes::Bytes::from(encode_reply(reply, applied())))
}

async fn submit_local(
    cmd_tx: tokio::sync::mpsc::WeakSender<Submission>,
    admit: Option<&'static crate::rsm::admit::AdmitGate>,
    command: Command,
    deadline: Instant,
) -> Result<Reply, RsmError> {
    let _admitted = match (admit, command.grows_storage()) {
        (Some(gate), true) => Some(gate.admit(command.size_hint()).await.map_err(|o| {
            RsmError::Overloaded {
                retry_after_s: o.retry_after_s,
            }
        })?),
        _ => None,
    };
    let Some(tx) = cmd_tx.upgrade() else {
        return Err(RsmError::Internal("planner channel closed".into()));
    };
    let (sub, rx) = Submission::new(command);
    let remaining = deadline.saturating_duration_since(Instant::now());
    match tokio::time::timeout(remaining, tx.send(sub)).await {
        Ok(Ok(())) => {}
        Ok(Err(_closed)) => return Err(RsmError::Internal("planner channel closed".into())),
        Err(_elapsed) => return Err(RsmError::Timeout),
    }
    drop(tx);
    let remaining = deadline.saturating_duration_since(Instant::now());
    match tokio::time::timeout(remaining, rx).await {
        Ok(Ok(reply)) => Ok(reply),
        Ok(Err(_dropped)) => Err(RsmError::Internal("planner dropped the reply".into())),
        Err(_elapsed) => Err(RsmError::Timeout),
    }
}

/// The handler a cluster node installs on its replicator.
pub(super) fn handler(
    cmd_tx: &CommandTx,
    admit: Option<&'static crate::rsm::admit::AdmitGate>,
    applied: Arc<dyn Fn() -> u64 + Send + Sync>,
) -> crate::rsm::replicator::raft::RemoteHandler {
    let weak = cmd_tx.downgrade();
    Arc::new(move |body| {
        let weak = weak.clone();
        Box::pin(serve(weak, admit, applied.clone(), body))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rsm::planner::positions::PositionOp;
    use crate::rsm::planner::txn::TxnCommand;
    use crate::rsm::planner::SubIntent;

    /// A follower's prepared transaction reaches the leader with its positions
    /// rider intact: the offload path is how a commit made on a follower is
    /// planned at all.
    #[test]
    fn a_prepared_transaction_keeps_its_positions() {
        let txn = TxnCommand {
            request_id: [7; 16],
            tenant: "t".into(),
            pushes: Vec::new(),
            acks: Vec::new(),
            positional_acks: Vec::new(),
            kv: Vec::new(),
            timers: Vec::new(),
            extra_effects: Vec::new(),
            allow_duplicate: false,
            positions: vec![PositionOp {
                queue: "orders".into(),
                partition: "3".into(),
                group: "billing".into(),
                offset: Some(42),
                metadata: "batch-42".into(),
                sub: SubIntent {
                    mode: "new".into(),
                    from_us: None,
                    now: false,
                },
            }],
        };
        let bytes = encode_request(&Command::Transaction(txn.clone()), Duration::from_secs(1))
            .expect("encode");
        let back: SubmitReq = postcard::from_bytes(&bytes).expect("decode");
        match back.command {
            Command::Transaction(t) => assert_eq!(t, txn),
            other => panic!("{other:?}"),
        }
    }
}
