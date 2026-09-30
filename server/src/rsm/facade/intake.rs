//! The leader's intake: where one prepared command — this node's own
//! client's ([`super::real::RaftFacade`]'s `submit`) or a follower's forwarded
//! one ([`super::remote`]) — meets the state machine on the node that leads.
//!
//! Consumption never goes through the log (2026-09-30). Pops, acks, nacks,
//! renews, DLQ heads and positional acks are the consumption engine's
//! ([`crate::rsm::consume`]): it answers them from the leader's memory and
//! checkpoints the cursors and leases they changed on its own clock
//! ([`spawn_ticker`]). A transaction's consumption half — its acks,
//! positional acks and positions — is the engine's too: the engine validates
//! and reserves it HERE, before the planner, and the command that reaches the
//! batcher carries the rows it writes (`extra_effects`) and the per-target
//! results (`engine_acks`) with those legs stripped (but for positions on
//! partitions that do not exist yet: the planner creates those in the same
//! entry); the batcher tells the engine when that entry lands
//! (`Engine::txn_resolve`). Everything else is the batcher's.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use super::RsmError;
use crate::rsm::batcher::{Command, CommandTx, Reply, Submission};
use crate::rsm::consume::{Engine, Served};
use crate::rsm::effect::Effect;
use crate::rsm::planner::txn::TxnCommand;
use crate::rsm::planner::{EffectsCommand, Refusal};

/// Whether a transaction carries a consumption half for the engine.
fn has_consumption_half(t: &TxnCommand) -> bool {
    !t.acks.is_empty() || !t.positional_acks.is_empty() || !t.positions.is_empty()
}

/// Whether an effects command writes consumer-group positions (an admin
/// seek, a consumer-group delete): rows the engine owns.
fn writes_positions(c: &EffectsCommand) -> bool {
    c.effects.iter().any(|e| {
        matches!(
            e,
            Effect::CursorSet { .. } | Effect::CursorDelete { .. } | Effect::GroupDelete { .. }
        )
    })
}

/// How much earlier than its caller's deadline a pop stops claiming (on top
/// of the engine's own reply margin): a claim is answered once the checkpoint
/// holding its lease commits, and a follower's answer still has to cross
/// back, apply on that node, and be rendered. Every pop, the leader's own
/// clients' too: with the margin on followers only, a leader-attached
/// long-poll claimed in its last 50 ms and missed its deadline waiting for
/// the checkpoint (2026-09-30, 10k queues: 478-966 timeouts per leader-attached
/// loader against 71-245 per follower-attached one).
///
/// Capped at a quarter of the time the pop has left. Whole, it swallowed short
/// long-polls: with the planner's 50 ms a follower pop with `timeout=300` never
/// claimed at all (the Rust streams runner polls every 300 ms: 4 of 39 tests
/// passed against a follower, 39 against the leader). A claim nobody receives
/// in time is handed back (the facade's `release_unanswered`, the engine's
/// `release_claims`).
pub(super) const POP_ANSWER_MARGIN: Duration = Duration::from_millis(250);

/// A pop's deadline on the ENGINE's clock: the time its caller has left,
/// measured here on the leader (a forwarded command carries its budget), less
/// the answer margin. The deadline a facade stamped came off ITS wall clock,
/// which a clock jump or a skewed follower can put anywhere.
fn pop_deadline_us(engine: &Engine, deadline: Instant) -> i64 {
    let left = deadline.saturating_duration_since(Instant::now());
    let usable = left.saturating_sub(POP_ANSWER_MARGIN.min(left / 4));
    engine
        .now_us()
        .saturating_add(usable.as_micros().min(i64::MAX as u128) as i64)
}

/// Serve `command` on this node, the leader, before `deadline`: the engine's
/// consumption commands from memory, a transaction's consumption half
/// prepared by the engine, and the rest (and the prepared transaction)
/// through the batcher, whose sender `batcher` hands out (`None`: the
/// batcher is gone). A node that does not lead answers `Retry` (the engine
/// and the batcher both know), and its caller takes the command to the
/// leader. No sender is held while the engine holds an answer (a long-poll
/// may wait its whole budget), so a stopping facade's batcher is never kept
/// alive by one.
pub(super) async fn submit_here(
    engine: &Engine,
    batcher: impl FnOnce() -> Option<CommandTx>,
    command: Command,
    deadline: Instant,
) -> Result<Reply, RsmError> {
    let command = match command {
        Command::Transaction(t) if has_consumption_half(&t) => match prepare(engine, t) {
            Ok(c) => c,
            // This node stopped leading: the caller takes the transaction to
            // whoever leads now, as it does a batcher's `Retry`.
            Err(refusal) if refusal.retryable && refusal.code == "not_leader" => {
                return Ok(Reply::Retry { hint: None })
            }
            Err(refusal) => return Ok(Reply::Refused(refusal)),
        },
        other => other,
    };
    let mut command = command;
    if let Command::PopWildcard(c) | Command::PopPinned(c) | Command::PopDiscover(c) = &mut command
    {
        c.deadline_us = pop_deadline_us(engine, deadline);
    }
    if command.is_consumption() || matches!(&command, Command::Effects(c) if writes_positions(c)) {
        match engine.serve(&command, engine.now_us()) {
            Served::Now(reply) => return Ok(reply),
            Served::Later(rx) => return answer_later(rx, deadline).await,
            Served::NotMine => {}
        }
    }
    let Some(tx) = batcher() else {
        return Err(RsmError::Internal("planner channel closed".into()));
    };
    to_batcher(tx, command, deadline).await
}

/// The engine's half of a transaction: validated and reserved against its
/// live state, carried by the command as rows (`extra_effects`) and results
/// (`engine_acks`), with the legs the planner no longer plans stripped — but
/// for the positions on partitions that do not exist yet, which the planner
/// creates (their pids are its to allocate) in the same entry. The planner
/// (planner/txn.rs) refuses any other consumption leg and records
/// `engine_acks` as its outcome's acks.
fn prepare(engine: &Engine, mut t: TxnCommand) -> Result<Command, Refusal> {
    if let Some(part) = engine.txn_prepare(&t, engine.now_us())? {
        t.acks.clear();
        t.positional_acks.clear();
        t.positions = part.planner_positions;
        t.extra_effects.extend(part.effects);
        t.engine_acks = part.acks;
    }
    Ok(Command::Transaction(t))
}

/// An answer the engine gives later (a held long-poll, or a claim or an ack
/// waiting for its checkpoint to commit), awaited until `deadline`. An
/// answer the engine let go of without sending (it stopped leading) is a
/// `Retry`: the caller takes the command to whoever leads now.
async fn answer_later(rx: oneshot::Receiver<Reply>, deadline: Instant) -> Result<Reply, RsmError> {
    match tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), rx).await {
        Ok(Ok(reply)) => Ok(reply),
        Ok(Err(_dropped)) => Ok(Reply::Retry { hint: None }),
        Err(_elapsed) => Err(RsmError::Timeout),
    }
}

/// Submit one command to the batcher and await its [`Reply`] until
/// `deadline`. The sender is dropped once the command is in the channel. A
/// closed channel or an elapsed deadline is a failure the caller maps (I15).
pub(super) async fn to_batcher(
    cmd_tx: CommandTx,
    command: Command,
    deadline: Instant,
) -> Result<Reply, RsmError> {
    let (sub, rx) = Submission::new(command);
    // The bounded channel absorbs back-pressure; a full channel waits, up to
    // the deadline.
    let remaining = deadline.saturating_duration_since(Instant::now());
    match tokio::time::timeout(remaining, cmd_tx.send(sub)).await {
        Ok(Ok(())) => {}
        Ok(Err(_closed)) => return Err(RsmError::Internal("planner channel closed".into())),
        Err(_elapsed) => return Err(RsmError::Timeout),
    }
    drop(cmd_tx);
    let remaining = deadline.saturating_duration_since(Instant::now());
    match tokio::time::timeout(remaining, rx).await {
        Ok(Ok(reply)) => Ok(reply),
        Ok(Err(_dropped)) => Err(RsmError::Internal("planner dropped the reply".into())),
        Err(_elapsed) => Err(RsmError::Timeout),
    }
}

/// `QUEEN_CONSUME_CHECKPOINT_MS` (default 5, at least 1): how often the
/// engine's checkpoint is taken and logged. Group commit: every claim and ack
/// the engine answered since the last one rides the next.
pub(crate) fn checkpoint_period() -> Duration {
    static MS: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
        std::env::var("QUEEN_CONSUME_CHECKPOINT_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(5)
            .clamp(1, 60_000)
    });
    Duration::from_millis(*MS)
}

/// How often the engine's clock runs ([`Engine::tick`]): held long-polls'
/// deadlines, lease expiries, delayed readiness, the leader lease.
const TICK_EVERY: Duration = Duration::from_millis(10);

/// The engine's clock, on every node (only a leader's engine has anything to
/// do): every [`checkpoint_period`] the checkpoint is taken and its commands
/// go to the batcher as they are ([`Command::Effects`], never through the
/// engine again); their replies are awaited off the ticker, so the next
/// checkpoint can be taken while one is in flight, and the engine learns
/// whether each committed ([`Engine::checkpoint_resolved`]). Every
/// [`TICK_EVERY`] the engine's clock runs. The sender is held weakly, so the
/// ticker never keeps a stopping facade's batcher alive; it ends with it.
pub(super) fn spawn_ticker(engine: Arc<Engine>, cmd_tx: &CommandTx) {
    let weak = cmd_tx.downgrade();
    tokio::spawn(async move {
        let mut every = tokio::time::interval(checkpoint_period());
        every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut ticked = Instant::now();
        loop {
            every.tick().await;
            let Some(tx) = weak.upgrade() else {
                break;
            };
            let now = engine.now_us();
            if ticked.elapsed() >= TICK_EVERY {
                ticked = Instant::now();
                engine.tick(now);
            }
            let Some(cp) = engine.take_checkpoint(now) else {
                continue;
            };
            let ticket = cp.ticket;
            let mut replies = Vec::with_capacity(cp.commands.len());
            let mut sent = true;
            for fx in cp.commands {
                let (sub, rx) = Submission::new(Command::Effects(fx));
                if tx.send(sub).await.is_err() {
                    sent = false;
                    break;
                }
                replies.push(rx);
            }
            drop(tx);
            if !sent {
                engine.checkpoint_resolved(ticket, false);
                break;
            }
            let engine = engine.clone();
            tokio::spawn(async move {
                let mut committed = true;
                for rx in replies {
                    committed &= matches!(rx.await, Ok(Reply::Done { .. }));
                }
                engine.checkpoint_resolved(ticket, committed);
            });
        }
    });
}
