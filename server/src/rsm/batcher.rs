//! The cycle driver (PLAN_RAFT.md §7.1, WP-1.6b): the one place on the leader
//! that turns queued client commands into log entries and answers their
//! receivers. It owns the bounded pipeline (D4: `QUEEN_RAFT_PIPELINE` = 4
//! entries in flight), the [`Overlay`] across that pipeline, and the request-id
//! expiry step of §10.1.
//!
//! # The cycle (§7.1)
//!
//! One cycle is: drain the command channel up to the caps and the planning
//! budget (O17); rebuild the overlay from committed bases plus the entries
//! still in flight, in index order ([`Overlay::ingest_entry`]); mark the cycle
//! start so the entry gets the `pid_base`/`kv_version_base` apply will assert
//! against `meta` (I18); for each command look the request id up (§5.4, I6) and,
//! on a miss, plan it, folding its effects into the overlay so the next command
//! in the cycle sees them; build ONE [`Entry`], encode it (a fallible step:
//! [`encode_entry`] re-runs [`Entry::validate`], so an entry that cannot encode
//! fails its waiters with a refusal and is never proposed); propose through the
//! [`Replicator`] seam; and answer each command's receiver from the outcome once
//! the entry is committed AND applied on this node (I4, D7).
//!
//! # What the overlay covers, and the drop gate
//!
//! The planner reads COMMITTED state through a store read transaction. Apply
//! commits the store on a cadence (`QUEEN_RAFT_STORE_COMMIT_MS`, ≤ 4 ms), so
//! an entry that has already been executed — even one whose `propose` has
//! resolved — is not visible to a fresh read until the next store commit. The
//! overlay closes exactly that gap: the batcher folds every in-flight entry
//! whose index is ABOVE the committed `applied_index` the planning read
//! reports, and drops one from its list only once that committed index has
//! passed it. This is engine-agnostic: on the real apply thread the committed
//! index advances and the overlay stays shallow; against a store that never
//! applies (the [`FakeReplicator`](super::replicator::fake) tests) it holds
//! everything, which is correct, because committed state never absorbs it.
//!
//! # The pipeline and the §7.1 error semantics (I3)
//!
//! Up to [`BatcherConfig::pipeline`] entries are proposed before the earliest
//! is applied. An entry leaves the pipeline only when it is applied locally (its
//! `propose` resolved `Ok`) or leadership is lost — a [`ProposeError::Timeout`]
//! does NOT remove it (I3):
//!
//! - [`ProposeError::NotLeader`] / [`ProposeError::OutcomeUnknown`]: drop the
//!   whole overlay together, fail every waiter with `Retry` (plus the hint), and
//!   stop planning until this node is leader again and has applied the first
//!   entry of its term (I13). The retry (same request id) finds the outcome if
//!   the entry committed, or plans anew (§5.4).
//! - [`ProposeError::Timeout`] while still leader: answer the entry's waiters
//!   `Retry` but keep the entry IN FLIGHT and plan NOTHING until it applies
//!   locally or the role changes. Dropping the overlay here would plan the next
//!   cycle without an entry that can still commit: duplicate offsets, double
//!   claims, two CAS winners.
//! - [`ProposeError::Refused`] / [`ProposeError::Fatal`]: a malformed proposal
//!   or a log/apply failure. The node is broken; fail every waiter `Retry` and
//!   stop the driver.
//!
//! # Threads (I15)
//!
//! `run` is one tokio task. The planning step — a store read transaction and
//! the pure planner — runs on the blocking pool ([`tokio::task::spawn_blocking`]),
//! so no store call ever blocks a tokio worker; `propose` is awaited as a
//! spawned task per entry, so several are in flight at once without the driver
//! blocking on any. No `std::sync::Mutex` is held across an `.await`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::MissedTickBehavior;

use crate::rsm::effect::Effect;
use crate::rsm::entry::{encode_entry, Entry, Outcome, RequestId};
use crate::rsm::planner::Refusal;
use crate::rsm::planner::{
    AckCommand, AckPositionalCommand, CommandKind, DlqHeadCommand, Lookup, NackCommand, Overlay,
    Plan, PlanConfig, Planned, Planner, PopCommand, PushCommand, RenewCommand,
};
use crate::rsm::replicator::{AppliedAt, NodeId, ProposeError, Replicator, Role};
use crate::rsm::state::{Committed, Derived};
use crate::rsm::store::{Reads, Store, TypedReads};

/// How often the driver polls the applied index while it holds the pipeline on
/// a [`ProposeError::Timeout`] (I3). Node-local timing, never state.
const HOLD_POLL_MS: u64 = 2;

// ---------------------------------------------------------------------------
// Configuration (Appendix H)
// ---------------------------------------------------------------------------

/// The batcher's knobs (§5.1, §7.1, §10.1, D4, D6, D13). Resolved once at boot
/// (WP-1.7); nothing here is read from the environment on the hot path.
#[derive(Clone, Debug)]
pub struct BatcherConfig {
    /// `QUEEN_RAFT_PIPELINE` (D4, I3): at most this many entries in flight.
    pub pipeline: usize,
    /// `QUEEN_RAFT_BATCH_MAX_CMDS` (§5.1).
    pub batch_max_cmds: usize,
    /// `QUEEN_RAFT_BATCH_MAX_BYTES` (§5.1): the estimated size at which the
    /// drain is cut. The exact bound is the codec's, enforced by
    /// [`encode_entry`].
    pub batch_max_bytes: usize,
    /// `QUEEN_RAFT_PROPOSE_MS` (D13): the deadline handed to every `propose`.
    pub propose_ms: u64,
    /// `QUEEN_RAFT_REQUEST_ID_WINDOW_S` (D6): how long outcomes live, and the
    /// age past which the expiry step retires them.
    pub request_id_window_s: u64,
    /// The cadence of the §10.1 request-id expiry step.
    pub request_expire_every_ms: u64,
    /// The channel depth the facade feeds (§9.1). Back-pressure lands on the
    /// receiver, which holds or refuses per D13.
    pub command_queue_depth: usize,
    /// The planner's own budget and slow-command threshold (O17, O18).
    pub plan: PlanConfig,
}

impl Default for BatcherConfig {
    fn default() -> BatcherConfig {
        BatcherConfig {
            pipeline: 4,
            batch_max_cmds: crate::rsm::entry::BATCH_MAX_CMDS_DEFAULT,
            batch_max_bytes: crate::rsm::entry::BATCH_MAX_BYTES_DEFAULT,
            propose_ms: 5000,
            request_id_window_s: 600,
            request_expire_every_ms: 10_000,
            command_queue_depth: 1024,
            plan: PlanConfig::default(),
        }
    }
}

impl BatcherConfig {
    /// Resolve from the environment. Called ONCE at boot by the storage seam
    /// (WP-1.7).
    pub fn from_env() -> BatcherConfig {
        fn num(name: &str, cur: u64) -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(cur)
        }
        let d = BatcherConfig::default();
        BatcherConfig {
            pipeline: num("QUEEN_RAFT_PIPELINE", d.pipeline as u64) as usize,
            batch_max_cmds: num("QUEEN_RAFT_BATCH_MAX_CMDS", d.batch_max_cmds as u64) as usize,
            batch_max_bytes: num("QUEEN_RAFT_BATCH_MAX_BYTES", d.batch_max_bytes as u64) as usize,
            propose_ms: num("QUEEN_RAFT_PROPOSE_MS", d.propose_ms),
            request_id_window_s: num("QUEEN_RAFT_REQUEST_ID_WINDOW_S", d.request_id_window_s),
            request_expire_every_ms: d.request_expire_every_ms,
            command_queue_depth: num(
                "QUEEN_RAFT_COMMAND_QUEUE_DEPTH",
                d.command_queue_depth as u64,
            ) as usize,
            plan: PlanConfig {
                entry_max_bytes: num("QUEEN_RAFT_ENTRY_MAX_BYTES", d.plan.entry_max_bytes as u64)
                    as usize,
                plan_budget_ms: num("QUEEN_RAFT_PLAN_MAX_MS", d.plan.plan_budget_ms),
                slow_command_ms: num("QUEEN_RAFT_SLOW_COMMAND_MS", d.plan.slow_command_ms),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Commands and replies (the seam the facade feeds, §9.1)
// ---------------------------------------------------------------------------

/// A client command the batcher plans — the phase-1 subset of §9.1. WP-1.7's
/// receiver maps HTTP requests and forwarded frames onto these; the batcher is
/// their only consumer, and the planner has already been written against the
/// per-kind structs (`PushCommand`, `PopCommand`, …).
#[derive(Clone, Debug)]
pub enum Command {
    Push(PushCommand),
    PopPinned(PopCommand),
    PopWildcard(PopCommand),
    PopDiscover(PopCommand),
    Ack(AckCommand),
    AckPositional(AckPositionalCommand),
    Nack(NackCommand),
    Renew(RenewCommand),
    DlqHead(DlqHeadCommand),
}

impl Command {
    /// The request id the receiver minted once (D6), reused across forwarding
    /// retries.
    pub fn request_id(&self) -> RequestId {
        match self {
            Command::Push(c) => c.request_id,
            Command::PopPinned(c) | Command::PopWildcard(c) | Command::PopDiscover(c) => {
                c.request_id
            }
            Command::Ack(c) => c.request_id,
            Command::AckPositional(c) => c.request_id,
            Command::Nack(c) => c.request_id,
            Command::Renew(c) => c.request_id,
            Command::DlqHead(c) => c.request_id,
        }
    }

    /// The kind, for the O18 per-kind planner metrics and the slow-command log.
    pub fn kind(&self) -> CommandKind {
        match self {
            Command::Push(_) => CommandKind::Push,
            Command::PopPinned(_) => CommandKind::PopPinned,
            Command::PopWildcard(_) => CommandKind::PopWildcard,
            Command::PopDiscover(_) => CommandKind::PopDiscover,
            Command::Ack(_) => CommandKind::Ack,
            Command::AckPositional(_) => CommandKind::AckPositional,
            Command::Nack(_) => CommandKind::Nack,
            Command::Renew(_) => CommandKind::Renew,
            Command::DlqHead(_) => CommandKind::DlqHead,
        }
    }

    /// A cheap upper estimate of the bytes this command will add to an entry,
    /// for cutting the drain at `batch_max_bytes` BEFORE planning. The exact
    /// bound is the codec's ([`encode_entry`]); this only decides how many
    /// commands share one cycle.
    fn size_hint(&self) -> usize {
        match self {
            Command::Push(c) => c.items.iter().map(|i| i.frame.len() + 16).sum::<usize>() + 64,
            Command::Ack(c) => {
                c.targets
                    .iter()
                    .map(|t| t.items.len() * 48 + 64)
                    .sum::<usize>()
                    + 32
            }
            Command::DlqHead(c) => c.snapshot.payload.len() + 128,
            _ => 128,
        }
    }

    /// Dispatch to the matching `plan_*`. The planner folds a `Logged` plan's
    /// effects into the overlay before returning, so the next command in the
    /// cycle sees them (§7.2).
    fn plan<R: Reads + ?Sized>(&self, p: &Planner<'_, R>, ov: &mut Overlay) -> Planned {
        match self {
            Command::Push(c) => p.plan_push(ov, c),
            Command::PopPinned(c) => p.plan_pop_pinned(ov, c),
            Command::PopWildcard(c) => p.plan_pop_wildcard(ov, c),
            Command::PopDiscover(c) => p.plan_pop_discover(ov, c),
            Command::Ack(c) => p.plan_ack(ov, c),
            Command::AckPositional(c) => p.plan_ack_positional(ov, c),
            Command::Nack(c) => p.plan_nack(ov, c),
            Command::Renew(c) => p.plan_renew(ov, c),
            Command::DlqHead(c) => p.plan_dlq_head(ov, c),
        }
    }
}

/// The answer the batcher returns to a command's receiver (§5.4, §7.5). The
/// receiver renders the wire reply from it; a pop reads its payload bytes from
/// this node's own files once its applied index has reached `at.index` (D7).
#[derive(Clone, Debug)]
pub enum Reply {
    /// Committed and applied on this node. `at` is the commit index it landed
    /// at, or `None` for an answer served from committed state alone (a
    /// request-id hit, I6).
    Done {
        outcome: Outcome,
        at: Option<AppliedAt>,
    },
    /// A whole-command refusal (§5.4, I14): the receiver turns a retryable one
    /// into a 5xx the SDK retries with the same request id, a non-retryable one
    /// into a 4xx it does not.
    Refused(Refusal),
    /// The entry could not commit here (§7.1). Retry against the hinted leader.
    Retry { hint: Option<NodeId> },
}

/// One submission on the batcher's channel: a command and where its answer
/// goes. The receiver awaits `reply`.
pub struct Submission {
    pub command: Command,
    pub reply: oneshot::Sender<Reply>,
}

impl Submission {
    pub fn new(command: Command) -> (Submission, oneshot::Receiver<Reply>) {
        let (tx, rx) = oneshot::channel();
        (Submission { command, reply: tx }, rx)
    }
}

/// The sender the facade feeds (§9.1). Bounded, so back-pressure reaches the
/// receiver rather than growing without bound here (I8).
pub type CommandTx = mpsc::Sender<Submission>;

// ---------------------------------------------------------------------------
// In-flight bookkeeping
// ---------------------------------------------------------------------------

/// A waiter attached to an in-flight entry.
enum Waiter {
    /// Answer from the entry's own command outcome for this request id — a
    /// logged command, a same-cycle duplicate of one, or a retry whose id is in
    /// this in-flight entry (§5.4).
    Command {
        request_id: RequestId,
        reply: oneshot::Sender<Reply>,
    },
    /// Answer with this fixed outcome — an empty result whose overlay read
    /// depended on this entry (§7.2), so it is answered only after the entry
    /// applies, and `Retry` if it fails.
    Fixed {
        outcome: Outcome,
        reply: oneshot::Sender<Reply>,
    },
}

/// One entry the batcher has proposed and is tracking.
struct InFlightEntry {
    /// The batcher's own monotone id, to correlate a `propose` result.
    seq: u64,
    /// The log index this entry occupies. Predicted at propose time from the
    /// replicator's log tip (single leader, no interleaving entries in phase 1)
    /// and confirmed by `AppliedAt` on `Ok`; the drop gate compares it against
    /// the committed `applied_index`.
    index: u64,
    entry: Arc<Entry>,
    waiters: Vec<Waiter>,
    /// `Some` once `propose` resolved `Ok` (applied locally) or the hold was
    /// released after a timeout: the entry has left the pipeline (I3).
    resolved: Option<AppliedAt>,
    /// A `Timeout` kept this entry in flight (I3): its waiters were already
    /// answered `Retry`, and it stays folded into the overlay until it applies.
    timed_out: bool,
}

impl InFlightEntry {
    /// Answer every waiter from this entry's committed outcome (`Ok`).
    fn resolve_ok(&mut self, at: AppliedAt) {
        self.resolved = Some(at);
        for w in self.waiters.drain(..) {
            let (tx, msg) = match w {
                Waiter::Command { request_id, reply } => {
                    let outcome = self
                        .entry
                        .commands
                        .iter()
                        .find(|c| c.request_id == request_id)
                        .map(|c| c.outcome.clone())
                        // The id is in this entry by construction (a waiter is
                        // only ever attached to the entry that carries it).
                        .unwrap_or(Outcome::Empty);
                    (
                        reply,
                        Reply::Done {
                            outcome,
                            at: Some(at),
                        },
                    )
                }
                Waiter::Fixed { outcome, reply } => (
                    reply,
                    Reply::Done {
                        outcome,
                        at: Some(at),
                    },
                ),
            };
            let _ = tx.send(msg);
        }
    }

    /// Fail every waiter (`Retry`), dropping this entry's overlay contribution.
    fn fail(&mut self, hint: Option<NodeId>) {
        for w in self.waiters.drain(..) {
            let tx = match w {
                Waiter::Command { reply, .. } => reply,
                Waiter::Fixed { reply, .. } => reply,
            };
            let _ = tx.send(Reply::Retry { hint });
        }
    }
}

// ---------------------------------------------------------------------------
// The blocking planning step
// ---------------------------------------------------------------------------

/// The disposition of one drained command after planning.
enum Slot {
    /// Answer at once: a committed request-id hit or a whole-command refusal.
    Immediate(Reply),
    /// Logged into the new entry under this id; answered when it applies.
    Logged(RequestId),
    /// A same-cycle duplicate of an already-logged id: attach a second waiter
    /// to the new entry under that id (the entry carries the id once).
    SameCycle(RequestId),
    /// The id is in an entry still in flight (§5.4): wait on that entry.
    InFlightHit {
        request_id: RequestId,
        outcome: Outcome,
    },
    /// An empty result (§5.4): answered at once if nothing was in flight, else
    /// after the entries it read applied.
    Empty(Outcome),
    /// The planning budget (O17) was spent before this command was reached;
    /// return it for the next cycle.
    Deferred(Box<Command>),
}

/// What one blocking cycle produced.
struct PlanOutput {
    store_applied: u64,
    entry: Option<Entry>,
    slots: Vec<Slot>,
    /// The request-id expiry step (§10.1) was included in `entry`.
    expired: bool,
}

/// Plan one cycle inside ONE store read transaction (I15: on the blocking
/// pool). `folded` are the in-flight entries, in index order, with the index
/// each occupies; `batch` are the drained commands, in order.
fn plan_cycle_blocking<S: Store>(
    store: &S,
    folded: Vec<(u64, Arc<Entry>)>,
    batch: Vec<Command>,
    cfg: PlanConfig,
    wall_us: i64,
    expire_window_us: Option<i64>,
) -> crate::rsm::store::Result<PlanOutput> {
    store.read(|r| {
        let store_applied = r.applied_index()?;
        let base_pid = r.next_pid()?;
        let base_kv = r.kv_version_next()?;

        // The committed clock floor (D5). Used to rebuild the rings and as the
        // base the overlay lifts above.
        let empty = Derived::default();
        let base_now = Committed::new(r, &empty).plan_now(wall_us)?;
        let derived = Derived::rebuild(r, base_now)?;
        let committed = Committed::new(r, &derived);

        // Fold every in-flight entry the committed read does not yet reflect
        // (§7.2). Its own `Overlay::plan_now` lifts the stamp above every folded
        // `now_us` and `created_at`, so the effects a folded append carries stay
        // monotone (I5).
        let mut ov = Overlay::new(base_pid, base_kv);
        for (index, e) in &folded {
            if *index > store_applied {
                ov.ingest_entry(e);
            }
        }
        let now_us = ov
            .plan_now(&committed, wall_us)
            // A store error under the clock read: fail the whole cycle
            // retryably (I14).
            .map_err(|_| crate::rsm::store::StoreError::Io("clock read".into()))?;
        ov.mark_cycle_start();
        let planner = Planner::new(committed, now_us, cfg.clone());

        let mut entry = Entry::new(now_us, ov.cycle_pid_base(), ov.cycle_kv_base());
        let mut slots: Vec<Slot> = Vec::with_capacity(batch.len());
        // Ids already logged into THIS entry, so a same-cycle retry becomes a
        // second waiter rather than a second command (§5.4; `Entry::validate`
        // forbids two commands sharing an id).
        let mut seen: HashMap<RequestId, ()> = HashMap::new();
        let start = Instant::now();
        let budget = Duration::from_millis(cfg.plan_budget_ms);
        let mut cut = false;

        for cmd in batch {
            if cut {
                slots.push(Slot::Deferred(Box::new(cmd)));
                continue;
            }
            let id = cmd.request_id();
            if seen.contains_key(&id) {
                slots.push(Slot::SameCycle(id));
                cut = start.elapsed() > budget;
                continue;
            }
            let slot = match planner.lookup_request_id(&ov, &id) {
                Err(refusal) => Slot::Immediate(Reply::Refused(refusal)),
                Ok(Lookup::Committed(outcome)) => {
                    Slot::Immediate(Reply::Done { outcome, at: None })
                }
                Ok(Lookup::InFlight(outcome)) => Slot::InFlightHit {
                    request_id: id,
                    outcome,
                },
                Ok(Lookup::Miss) => match cmd.plan(&planner, &mut ov) {
                    Ok(Plan::Logged { effects, outcome }) => {
                        match entry.add_command(id, outcome, effects) {
                            Ok(()) => {
                                seen.insert(id, ());
                                Slot::Logged(id)
                            }
                            Err(e) => Slot::Immediate(Reply::Refused(Refusal::retry(
                                "internal",
                                format!("entry build: {e:?}"),
                            ))),
                        }
                    }
                    Ok(Plan::Empty(outcome)) => Slot::Empty(outcome),
                    Ok(Plan::Refused(refusal)) => Slot::Immediate(Reply::Refused(refusal)),
                    Err(refusal) => Slot::Immediate(Reply::Refused(refusal)),
                },
            };
            slots.push(slot);
            cut = start.elapsed() > budget;
        }

        // §10.1 request-id expiry: a leader-loop step is one command with its
        // own minted id (see `Entry::validate`), answered by nobody.
        let mut expired = false;
        if let Some(window_us) = expire_window_us {
            let cutoff = now_us.saturating_sub(window_us);
            let id = crate::util::uuidv7_bytes();
            if entry
                .add_command(
                    id,
                    Outcome::Empty,
                    vec![Effect::RequestIdsExpire { cutoff_us: cutoff }],
                )
                .is_ok()
            {
                expired = true;
            }
        }

        let entry = if entry.commands.is_empty() {
            None
        } else {
            Some(entry)
        };
        Ok(PlanOutput {
            store_applied,
            entry,
            slots,
            expired,
        })
    })
}

// ---------------------------------------------------------------------------
// The batcher
// ---------------------------------------------------------------------------

/// The leader-side cycle driver. Build one with [`Batcher::new`] and start it
/// with [`Batcher::spawn`], which returns the channel the facade feeds and a
/// join handle for the driver task.
pub struct Batcher<S: Store, R: Replicator> {
    store: Arc<S>,
    repl: Arc<R>,
    cfg: BatcherConfig,
}

impl<S: Store + 'static, R: Replicator> Batcher<S, R> {
    pub fn new(store: Arc<S>, repl: Arc<R>, cfg: BatcherConfig) -> Batcher<S, R> {
        Batcher { store, repl, cfg }
    }

    /// Start the driver on the current runtime. Returns the command sender and
    /// the task handle; dropping every sender drains the driver and exits it.
    pub fn spawn(self) -> (CommandTx, tokio::task::JoinHandle<()>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(self.cfg.command_queue_depth);
        let handle = tokio::spawn(self.run(cmd_rx));
        (cmd_tx, handle)
    }

    async fn run(self, cmd_rx: mpsc::Receiver<Submission>) {
        let (result_tx, result_rx) = mpsc::unbounded_channel();
        let mut expire =
            tokio::time::interval(Duration::from_millis(self.cfg.request_expire_every_ms));
        expire.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The first tick fires immediately; swallow it so the driver does not
        // propose an expiry step before it has done any work.
        expire.tick().await;

        // Capture what the watch and metrics say BEFORE the fields move into
        // `RunState`.
        let role_rx = self.repl.watch_role();
        let role = *role_rx.borrow();
        let next_index = self.repl.metrics().last_log_index + 1;
        let mut st = RunState {
            store: self.store,
            repl: self.repl,
            cfg: self.cfg,
            role_rx,
            cmd_rx,
            result_tx,
            result_rx,
            queue: VecDeque::new(),
            inflight: VecDeque::new(),
            next_seq: 1,
            next_index,
            holding_until: None,
            paused: !role.is_leader(),
            closing: false,
            expire_due: false,
            stopped: false,
        };

        loop {
            while st.can_plan() {
                st.plan_cycle().await;
            }
            if st.should_exit() {
                break;
            }

            tokio::select! {
                biased;
                _ = st.role_rx.changed() => {
                    let role = *st.role_rx.borrow();
                    st.on_role(role);
                }
                Some((seq, res)) = st.result_rx.recv() => {
                    st.on_result(seq, res);
                }
                _ = expire.tick() => {
                    st.expire_due = true;
                }
                _ = tokio::time::sleep(Duration::from_millis(HOLD_POLL_MS)),
                    if st.holding_until.is_some() =>
                {
                    st.check_hold();
                }
                maybe = st.cmd_rx.recv() => {
                    match maybe {
                        Some(sub) => st.queue.push_back(sub),
                        None => st.closing = true,
                    }
                }
            }

            if st.stopped {
                break;
            }
        }

        // On a Fatal exit, nothing else will answer the stragglers.
        st.fail_all(None);
    }
}

// The driver's live state. A struct so the `select!` arms borrow disjoint
// channel fields while the handlers take `&mut self` afterwards.
struct RunState<S: Store, R: Replicator> {
    store: Arc<S>,
    repl: Arc<R>,
    cfg: BatcherConfig,
    role_rx: watch::Receiver<Role>,
    cmd_rx: mpsc::Receiver<Submission>,
    result_tx: mpsc::UnboundedSender<(u64, Result<AppliedAt, ProposeError>)>,
    result_rx: mpsc::UnboundedReceiver<(u64, Result<AppliedAt, ProposeError>)>,
    queue: VecDeque<Submission>,
    inflight: VecDeque<InFlightEntry>,
    next_seq: u64,
    next_index: u64,
    /// `Some(index)` while the pipeline is held on a timeout (I3): plan nothing
    /// until the committed apply reaches this index or the role changes.
    holding_until: Option<u64>,
    /// This node is not the leader (a `NotLeader`/`OutcomeUnknown`, or a role
    /// watch that reported a follower): plan nothing until it is leader again.
    paused: bool,
    closing: bool,
    expire_due: bool,
    stopped: bool,
}

impl<S: Store + 'static, R: Replicator> RunState<S, R> {
    /// Entries proposed but not yet applied locally (the I3 pipeline count):
    /// a `Timeout` does NOT decrement it until the entry applies.
    fn unresolved(&self) -> usize {
        self.inflight
            .iter()
            .filter(|e| e.resolved.is_none())
            .count()
    }

    fn can_plan(&self) -> bool {
        !self.stopped
            && !self.paused
            && self.holding_until.is_none()
            && self.unresolved() < self.cfg.pipeline
            && (!self.queue.is_empty() || (self.expire_due && !self.closing))
    }

    fn should_exit(&self) -> bool {
        self.stopped
            || (self.closing
                && self.queue.is_empty()
                && self.unresolved() == 0
                && self.holding_until.is_none())
    }

    /// Drain up to the caps (§5.1) into one batch, leaving the rest queued.
    fn drain_batch(&mut self) -> Vec<Submission> {
        let mut batch = Vec::new();
        let mut bytes = 0usize;
        while let Some(front) = self.queue.front() {
            if batch.len() >= self.cfg.batch_max_cmds {
                break;
            }
            let hint = front.command.size_hint();
            if !batch.is_empty() && bytes + hint > self.cfg.batch_max_bytes {
                break;
            }
            bytes += hint;
            batch.push(self.queue.pop_front().unwrap());
        }
        batch
    }

    /// One cycle: drain, plan on the blocking pool, propose, route answers.
    async fn plan_cycle(&mut self) {
        let batch = self.drain_batch();
        let expire = self.expire_due && !self.closing;
        if batch.is_empty() && !expire {
            return;
        }
        // §13.5 `batcher.drained`: commands are out of the channel and in the
        // cycle; nothing is planned. A crash here loses only unanswered work.
        if !batch.is_empty() {
            crate::rsm::faults::hit("batcher.drained");
        }

        // Split the submissions: commands go to the blocking planner, reply
        // senders stay here in the same order.
        let (commands, replies): (Vec<Command>, Vec<oneshot::Sender<Reply>>) =
            batch.into_iter().map(|s| (s.command, s.reply)).unzip();

        let folded: Vec<(u64, Arc<Entry>)> = self
            .inflight
            .iter()
            .map(|e| (e.index, e.entry.clone()))
            .collect();

        let wall_us = now_micros();
        let expire_window_us = expire.then(|| self.cfg.request_id_window_s as i64 * 1_000_000);
        let store = self.store.clone();
        let cfg = self.cfg.plan.clone();

        let planned = tokio::task::spawn_blocking(move || {
            plan_cycle_blocking(&*store, folded, commands, cfg, wall_us, expire_window_us)
        })
        .await;

        let out = match planned {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => {
                // The store could not answer: refuse the whole batch retryably
                // (I14) and try again next tick. The expiry step, if it was
                // due, stays due.
                for reply in replies {
                    let _ =
                        reply.send(Reply::Refused(Refusal::retry("unavailable", e.to_string())));
                }
                return;
            }
            Err(join) => {
                for reply in replies {
                    let _ = reply.send(Reply::Refused(Refusal::retry(
                        "unavailable",
                        format!("planner task: {join}"),
                    )));
                }
                return;
            }
        };

        if out.expired {
            self.expire_due = false;
        }

        // §13.5 `planner.planned`: effects and outcomes exist in the overlay;
        // nothing is proposed. A crash here has committed nothing (I1): the
        // overlay is RAM, the store was only read.
        if out.entry.is_some() {
            crate::rsm::faults::hit("planner.planned");
        }

        // Drop the in-flight entries the committed read now reflects (§7.2).
        self.drop_landed(out.store_applied);

        // Encode the entry BEFORE routing any answer, so an entry that cannot
        // encode fails only the commands that went into it, and nothing is
        // half-answered. `encode_entry` re-runs `Entry::validate` (§5.1).
        let encoded = match &out.entry {
            Some(entry) => match encode_entry(entry) {
                Ok(bytes) => Some(Bytes::from(bytes)),
                Err(e) => {
                    self.route_encode_failure(out.slots, replies, &e);
                    return;
                }
            },
            None => None,
        };

        let (index, seq, has_entry) = if encoded.is_some() {
            let index = self.next_index;
            let seq = self.next_seq;
            self.next_index += 1;
            self.next_seq += 1;
            (index, seq, true)
        } else {
            (0, 0, false)
        };

        // The barrier entry for an empty answer that read the overlay (§7.2):
        // the new entry if one was built, else the newest UNRESOLVED in-flight
        // entry; if neither exists the read was of committed state alone and is
        // answered at once.
        let barrier_seq: Option<u64> = if has_entry {
            Some(seq)
        } else {
            self.inflight
                .iter()
                .rev()
                .find(|e| e.resolved.is_none())
                .map(|e| e.seq)
        };

        let mut waiters: Vec<Waiter> = Vec::new();
        let mut deferred: Vec<Submission> = Vec::new();

        for (slot, reply) in out.slots.into_iter().zip(replies.into_iter()) {
            match slot {
                Slot::Immediate(r) => {
                    let _ = reply.send(r);
                }
                Slot::Deferred(command) => deferred.push(Submission {
                    command: *command,
                    reply,
                }),
                Slot::Logged(request_id) | Slot::SameCycle(request_id) => {
                    waiters.push(Waiter::Command { request_id, reply });
                }
                Slot::InFlightHit {
                    request_id,
                    outcome,
                } => self.attach_inflight_hit(request_id, outcome, reply),
                Slot::Empty(outcome) => match barrier_seq {
                    None => {
                        let _ = reply.send(Reply::Done { outcome, at: None });
                    }
                    Some(bseq) if bseq == seq && has_entry => {
                        waiters.push(Waiter::Fixed { outcome, reply });
                    }
                    Some(bseq) => {
                        if let Some(e) = self.inflight.iter_mut().find(|e| e.seq == bseq) {
                            e.waiters.push(Waiter::Fixed { outcome, reply });
                        } else {
                            let _ = reply.send(Reply::Done { outcome, at: None });
                        }
                    }
                },
            }
        }

        // Return budget-cut commands to the front of the queue, in order.
        for sub in deferred.into_iter().rev() {
            self.queue.push_front(sub);
        }

        if let (Some(bytes), Some(entry)) = (encoded, out.entry) {
            self.propose(seq, index, Arc::new(entry), waiters, bytes);
        } else {
            debug_assert!(waiters.is_empty(), "no entry but waiters were attached");
        }
    }

    /// An entry that could not encode ([`encode_entry`], §5.1): a should-never
    /// happen bug once the planner and the batch caps hold. Fail every waiter
    /// this cycle with a non-retryable refusal, requeue the budget-cut tail,
    /// and propose nothing.
    fn route_encode_failure(
        &mut self,
        slots: Vec<Slot>,
        replies: Vec<oneshot::Sender<Reply>>,
        err: &crate::rsm::effect::CodecError,
    ) {
        tracing::error!(target: "rsm", error = ?err, "batcher entry failed to encode; refusing its commands");
        for (slot, reply) in slots.into_iter().zip(replies.into_iter()) {
            match slot {
                Slot::Deferred(command) => self.queue.push_front(Submission {
                    command: *command,
                    reply,
                }),
                Slot::Immediate(r) => {
                    let _ = reply.send(r);
                }
                _ => {
                    let _ = reply.send(Reply::Refused(Refusal::client(
                        "entry_encode_failed",
                        format!("{err:?}"),
                    )));
                }
            }
        }
    }

    /// Spawn the `propose` for one entry and record it in flight.
    fn propose(
        &mut self,
        seq: u64,
        index: u64,
        entry: Arc<Entry>,
        waiters: Vec<Waiter>,
        bytes: Bytes,
    ) {
        self.inflight.push_back(InFlightEntry {
            seq,
            index,
            entry,
            waiters,
            resolved: None,
            timed_out: false,
        });
        let repl = self.repl.clone();
        let result_tx = self.result_tx.clone();
        let deadline = Instant::now() + Duration::from_millis(self.cfg.propose_ms);
        tokio::spawn(async move {
            // §13.5 `propose.sent`: the entry is about to reach the replicator;
            // the client is still waiting and the outcome is UNKNOWN (D6, I6).
            // A crash here has put nothing in the log, so the write is
            // unanswered and at-most-once: the retry with the same request id
            // finds nothing committed and plans anew (§5.4).
            crate::rsm::faults::hit("propose.sent");
            let res = repl.propose(bytes, deadline).await;
            let _ = result_tx.send((seq, res));
        });
    }

    /// Attach a retry whose id is in an entry still in flight (§5.4): answer at
    /// once if that entry has already applied, else wait on it.
    fn attach_inflight_hit(
        &mut self,
        request_id: RequestId,
        outcome: Outcome,
        reply: oneshot::Sender<Reply>,
    ) {
        if let Some(e) = self
            .inflight
            .iter_mut()
            .find(|e| e.entry.commands.iter().any(|c| c.request_id == request_id))
        {
            match e.resolved {
                Some(at) => {
                    let _ = reply.send(Reply::Done {
                        outcome,
                        at: Some(at),
                    });
                }
                None => e.waiters.push(Waiter::Command { request_id, reply }),
            }
        } else {
            // It was dropped between planning and here (it applied and the
            // committed read passed it): answer from the looked-up outcome.
            let _ = reply.send(Reply::Done { outcome, at: None });
        }
    }

    /// Drop in-flight entries the committed apply has passed (§7.2). Their
    /// waiters were already answered on `Ok` (or `Retry` on a timeout).
    fn drop_landed(&mut self, store_applied: u64) {
        while let Some(front) = self.inflight.front() {
            if front.index <= store_applied && front.resolved.is_some() {
                self.inflight.pop_front();
            } else {
                break;
            }
        }
    }

    /// A `propose` resolved.
    fn on_result(&mut self, seq: u64, res: Result<AppliedAt, ProposeError>) {
        match res {
            Ok(at) => {
                if let Some(e) = self.inflight.iter_mut().find(|e| e.seq == seq) {
                    // The predicted index is confirmed; trust the library's.
                    e.index = at.index;
                    e.resolve_ok(at);
                }
            }
            Err(ProposeError::Timeout) => {
                // I3: keep the entry in flight, answer its waiters Retry, and
                // hold the pipeline until it applies or the role changes.
                if let Some(e) = self.inflight.iter_mut().find(|e| e.seq == seq) {
                    e.timed_out = true;
                    e.fail(None);
                    let idx = e.index;
                    self.holding_until = Some(self.holding_until.map_or(idx, |h| h.max(idx)));
                }
            }
            Err(ProposeError::NotLeader { hint }) => self.lose_leadership(hint),
            Err(ProposeError::OutcomeUnknown) => self.lose_leadership(None),
            Err(ProposeError::Refused(why)) | Err(ProposeError::Fatal(why)) => {
                tracing::error!(target: "rsm", why, "batcher propose failed fatally; driver stops");
                self.stopped = true;
            }
        }
    }

    /// §7.1: drop the whole overlay, fail every waiter `Retry`, and stop
    /// planning until this node is leader again (I13).
    fn lose_leadership(&mut self, hint: Option<NodeId>) {
        for mut e in self.inflight.drain(..) {
            e.fail(hint);
        }
        self.holding_until = None;
        self.paused = true;
    }

    /// The role watch changed.
    fn on_role(&mut self, role: Role) {
        match role {
            Role::Leader { .. } => {
                if self.paused {
                    // I13: a new leader plans only after applying the first
                    // entry of its own term. On a single node with the
                    // LocalReplicator the term never changes, so this is the
                    // reset of the planning base after a regain; the openraft
                    // adapter (phase 3) waits on the term's first entry.
                    self.paused = false;
                    self.next_index = self.repl.metrics().last_log_index + 1;
                }
            }
            Role::Stopped => {
                self.stopped = true;
            }
            _ => {
                // Follower / Learner / Candidate: drop the overlay, fail
                // waiters, and wait for leadership.
                self.lose_leadership(role.leader_hint());
            }
        }
    }

    /// Poll the applied index while holding on a timeout (I3).
    fn check_hold(&mut self) {
        let Some(until) = self.holding_until else {
            return;
        };
        if self.repl.applied_index() >= until {
            // The held entries applied locally; they leave the pipeline. Mark
            // every timed-out entry at or below the applied index resolved so
            // it no longer counts, and resume planning.
            let applied = self.repl.applied_index();
            let term = match *self.role_rx.borrow() {
                Role::Leader { term } => term,
                _ => 0,
            };
            for e in self.inflight.iter_mut() {
                if e.timed_out && e.resolved.is_none() && e.index <= applied {
                    e.resolved = Some(AppliedAt {
                        index: e.index,
                        term,
                    });
                }
            }
            self.holding_until = None;
        }
    }

    /// Fail every remaining waiter (`Retry`) — the driver is exiting.
    fn fail_all(&mut self, hint: Option<NodeId>) {
        for mut e in self.inflight.drain(..) {
            e.fail(hint);
        }
        while let Some(sub) = self.queue.pop_front() {
            let _ = sub.reply.send(Reply::Retry { hint });
        }
    }
}

/// µs since the Unix epoch, the wall clock the planner stamps `now` from (D5).
/// The planner is exempt from the I2 clock ban; apply is not.
fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}
