//! Clients served from every node (`QUEEN_RAFT_CLIENT_OFFLOAD`, default on in
//! a cluster).
//!
//! A follower does its clients' work itself: it parses and validates the
//! request, prepares the command exactly as the leader would, and renders the
//! answer from its OWN state. Only the prepared command crosses to the leader,
//! whose batcher plans it with every other command; the reply comes back with
//! the index its entry landed at, and the follower waits until it has applied
//! that index itself before it answers — so a pop reads its payloads from the
//! follower's own queue logs. A push or an ack whose answer renders from its
//! outcome alone waits less: until the follower knows the entry committed with
//! the reply's term, one append after the leader's commit, however far behind
//! its apply runs (`QUEEN_RAFT_FOLLOWER_ANSWER_AT_DONE`, `RaftFacade::submit_offloaded`).
//! A read of this node's state after such an answer takes the read barrier,
//! as a read after another node's answer always had to.
//!
//! The request and the reply travel as postcard (payload bytes as raw bytes),
//! over the Raft RPC listener, with the cluster token: many to a write over the
//! follower's streams (`/raft/v1/forward`, [`crate::rsm::replicator::raft`]'s
//! `forward`), or one `/raft/v1/submit` call each (`QUEEN_RAFT_FWD_BATCH=0`, or
//! a leader that predates the streams). A retried command keeps its request
//! id, so a reply lost in transit and a retry never plan it twice (I6).

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
/// batcher, under the push admission budget (its share of it: `from` names
/// the follower, [`crate::rsm::admit`]), and answer the encoded reply.
/// `cmd_tx` is weak, so a follower's request never keeps a stopped facade's
/// batcher alive.
pub(super) async fn serve(
    cmd_tx: tokio::sync::mpsc::WeakSender<Submission>,
    admit: Option<&'static crate::rsm::admit::AdmitGate>,
    applied: Arc<dyn Fn() -> u64 + Send + Sync>,
    from: crate::rsm::admit::Source,
    body: bytes::Bytes,
) -> Result<bytes::Bytes, String> {
    let req: SubmitReq =
        postcard::from_bytes(&body).map_err(|e| format!("prepared command: {e}"))?;
    let deadline = Instant::now() + Duration::from_millis(req.budget_ms.clamp(1, 120_000));
    let reply = submit_local(cmd_tx, admit, from, req.command, deadline).await;
    Ok(bytes::Bytes::from(encode_reply(reply, applied())))
}

async fn submit_local(
    cmd_tx: tokio::sync::mpsc::WeakSender<Submission>,
    admit: Option<&'static crate::rsm::admit::AdmitGate>,
    from: crate::rsm::admit::Source,
    command: Command,
    deadline: Instant,
) -> Result<Reply, RsmError> {
    // The follower's own edge admitted the request as its memory guard; the
    // leader decides how the planner's budget is shared, and answers an
    // explicit overload (the follower's 429) well within the caller's time.
    let _admitted = match (admit, command.grows_storage()) {
        (Some(gate), true) => Some(
            gate.admit_forwarded(
                from,
                command.size_hint(),
                deadline.saturating_duration_since(Instant::now()),
            )
            .await
            .map_err(|o| RsmError::Overloaded {
                retry_after_s: o.retry_after_s,
            })?,
        ),
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
///
/// The Raft RPC server runs on the `queen-raft` runtime, whose few threads
/// also carry openraft's core, the replication to every follower and the
/// votes: a follower's command decoded, admitted and awaited there queued the
/// commits behind it. Called from that runtime, the handler hands the work to
/// the runtime the facade opened on (the client-facing one) and only awaits
/// the answer there; called from anywhere else (the batched intake, which
/// already runs off it) it runs in place.
pub(super) fn handler(
    cmd_tx: &CommandTx,
    admit: Option<&'static crate::rsm::admit::AdmitGate>,
    applied: Arc<dyn Fn() -> u64 + Send + Sync>,
) -> crate::rsm::replicator::raft::RemoteHandler {
    type Answer =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<bytes::Bytes, String>> + Send>>;
    let weak = cmd_tx.downgrade();
    let clients = tokio::runtime::Handle::try_current().ok();
    Arc::new(move |body: bytes::Bytes| -> Answer {
        let from = crate::rsm::admit::forwarded_from();
        let fut = serve(weak.clone(), admit, applied.clone(), from, body);
        match &clients {
            Some(rt) if on_raft_runtime() => {
                let task = rt.spawn(fut);
                Box::pin(async move {
                    task.await
                        .map_err(|e| format!("the forwarded command's task: {e}"))?
                })
            }
            _ => Box::pin(fut),
        }
    })
}

/// Whether the calling thread is one of the `queen-raft` runtime's
/// (`replicator::raft`, `QUEEN_RAFT_RT_THREADS`).
fn on_raft_runtime() -> bool {
    std::thread::current().name() == Some("queen-raft")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rsm::planner::positions::PositionOp;
    use crate::rsm::planner::txn::TxnCommand;
    use crate::rsm::planner::SubIntent;

    /// Called from a `queen-raft` thread (the per-command route of the Raft RPC
    /// server), the handler works the command on the runtime the facade
    /// installed it from: it reaches the batcher while the raft runtime's only
    /// thread is busy, and the raft runtime only awaits the encoded reply.
    #[test]
    fn a_command_from_the_raft_runtime_is_worked_on_the_clients_runtime() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let rt = |name: &str| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name(name)
                .enable_all()
                .build()
                .expect("runtime")
        };
        let (clients, raft) = (rt("clients"), rt("queen-raft"));
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Submission>(8);
        let h = clients.block_on(async { handler(&tx, None, Arc::new(|| 7)) });
        let reached = Arc::new(AtomicBool::new(false));
        let seen = reached.clone();
        clients.spawn(async move {
            while let Some(sub) = rx.recv().await {
                seen.store(true, Ordering::SeqCst);
                let _ = sub.reply.send(Reply::Retry { hint: Some(3) });
            }
        });
        let cmd = Command::Nack(crate::rsm::planner::NackCommand {
            request_id: [9; 16],
            pid: 1,
            tenant: String::new(),
            queue: String::new(),
            group: "g".into(),
            worker: "w".into(),
        });
        let body = bytes::Bytes::from(encode_request(&cmd, Duration::from_secs(5)).unwrap());
        let answer = raft
            .block_on(raft.spawn(async move {
                assert!(on_raft_runtime(), "called on the raft runtime");
                let answer = h(body);
                // The raft runtime's one thread stays busy: worked here, the
                // command would not even start before it is awaited.
                let t0 = std::time::Instant::now();
                while !reached.load(Ordering::SeqCst) && t0.elapsed() < Duration::from_secs(5) {
                    std::thread::sleep(Duration::from_millis(1));
                }
                assert!(
                    reached.load(Ordering::SeqCst),
                    "the command reached the batcher while the raft thread was busy"
                );
                answer.await
            }))
            .expect("the raft runtime's task");
        let (reply, _) = decode_reply(&answer.expect("answered")).expect("decodes");
        assert!(matches!(reply, Reply::Retry { hint: Some(3) }), "{reply:?}");
        assert!(!on_raft_runtime(), "a test thread is not a raft one");
        drop(tx);
    }

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
