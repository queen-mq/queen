//! The cycle driver (PLAN_RAFT.md §7.1, WP-1.6b): the one place on the leader
//! that turns queued client commands into log entries and answers their
//! receivers. It owns the bounded pipeline (D4: `QUEEN_RAFT_PIPELINE`, default
//! 8 entries in flight), the [`Overlay`] across that pipeline, and the request-id
//! expiry step of §10.1.
//!
//! # The cycle (§7.1)
//!
//! One cycle is: drain the command channel up to the caps and the planning
//! budget (O17); bring the overlay to committed bases plus the entries still
//! in flight, in index order — the one the planner thread KEPT from the last
//! cycle, minus the entries that have landed since (KEEP_OVERLAY,
//! [`crate::rsm::planner::kept`]), or one rebuilt from scratch
//! ([`Overlay::ingest_entry`]) with the knob off and on every fallback; mark
//! the cycle start so the entry gets the `pid_base`/`kv_version_base` apply
//! will assert against `meta` (I18); for each command look the request id up
//! (§5.4, I6) and,
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
//!   the entry committed, or plans anew (§5.4). While ANOTHER node is known to
//!   lead, every queued command and every one that arrives is answered `Retry`
//!   at once, never parked: it would wait for this node to lead again (a
//!   leadership transfer cost each such caller its whole deadline).
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
//! the pure planner — runs on ONE dedicated thread (`queen-planner`), so no
//! store call ever blocks a tokio worker; that thread also owns what planning
//! keeps between cycles (KEEP_OVERLAY), with no lock. Each entry's `propose` is
//! SUBMITTED to the replicator inline, on this one driver task and in plan
//! order (so the log index follows the `now_us` stamp order — WP-1.11 F-1,
//! I5), and only its wait for local apply is spawned, so up to `pipeline`
//! entries are in flight at once without the driver blocking on any. No
//! `std::sync::Mutex` is held across an `.await`.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch, Notify};
use tokio::time::MissedTickBehavior;

use crate::rsm::dedup::DedupFront;
use crate::rsm::effect::Effect;
use crate::rsm::entry::{encode_entry, Entry, Outcome, RequestId};
use crate::rsm::planner::kept::KeptOverlay;
use crate::rsm::planner::timers::{TimerFireConfig, TimersCommand};
pub use crate::rsm::planner::txn::{TxnCommand, TxnOutcome};
use crate::rsm::planner::Refusal;
use crate::rsm::planner::{
    AckCommand, AckPositionalCommand, CommandKind, DlqHeadCommand, EffectsCommand, KvCommand,
    Lookup, NackCommand, Overlay, Plan, PlanConfig, Planned, Planner, PopCommand, PushCommand,
    RenewCommand,
};
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::replicator::{AppliedAt, NodeId, ProposeError, Replicator, Role};
use crate::rsm::segments::Reader;
use crate::rsm::state::Committed;
use crate::rsm::store::{Reads, Store, TypedReads};

#[path = "batcher_lanes.rs"]
mod lanes;

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
    /// `QUEEN_RAFT_PIPELINE` (D4, I3): at most this many entries in flight
    /// (default 8). A slot frees only when its entry applies here, ~30-50 ms
    /// after its proposal on three nodes at 1M msg/s, so 4 capped the leader at
    /// ~100 cycles/s: with each cycle at the 4 MiB drain cap, just 1M msg/s,
    /// and the queue never drained after the first retention wave (2026-09-29).
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
    /// age past which the expiry step retires them. Default 60 s: the ids are
    /// minted by the receiver and serve its own forwarding and leader-change
    /// retries, which end within seconds. At 600 s the outcomes of ~5,900
    /// commands/s (200k msg/s) held ~4 GB of RAM (measured 2026-09-23).
    pub request_id_window_s: u64,
    /// The cadence of the §10.1 request-id expiry step. A step that finds a
    /// full [`crate::rsm::apply::REQUEST_EXPIRE_LIMIT`] past the cutoff runs
    /// again in the next cycle rather than at the next tick.
    pub request_expire_every_ms: u64,
    /// The channel depth the facade feeds (§9.1). Back-pressure lands on the
    /// receiver, which holds or refuses per D13.
    pub command_queue_depth: usize,
    /// The planner's own budget and slow-command threshold (O17, O18).
    pub plan: PlanConfig,
    /// `QUEEN_RAFT_DRIVER_NOTIFY` (PERF-G, default on): resolve an in-flight
    /// entry off the replicator's applied-index notify (one cross-thread wake)
    /// instead of waiting for its per-propose forwarding task to deliver the
    /// result through the `mpsc`. It only makes the happy-path answer arrive
    /// sooner — the propose future still runs, so `Timeout`/`Fatal` are
    /// unchanged and D7/I4 still gate the answer on commit + local apply. Off:
    /// the pre-PERF-G path (the forwarding task alone). Inert on a backend that
    /// returns no `applied_notify`.
    pub driver_notify: bool,
    /// `QUEEN_RAFT_PUSH_PRIORITY` (PERF-J, default on): drain a batch so pushes
    /// are not head-of-line-blocked behind the expensive pop/other commands.
    /// The batch is round-robined push-first (a push, then one other, then a
    /// push, …) so a burst of ~0.5 ms pop plans cannot push a cheap ~4 µs push
    /// past the `plan_budget_ms` cut into the next cycle, while non-push
    /// commands still get every other early slot (no lane starves). Per-partition
    /// push order is preserved (the partition is stable, and the push lane is
    /// consumed in order). Off: the pre-PERF-J FIFO drain.
    pub push_priority: bool,
    /// `QUEEN_RAFT_DRAIN_GREEDY` (PERF-L): pull the whole channel into one
    /// batch before planning, so one entry carries many commands.
    pub drain_greedy: bool,
    /// `QUEEN_RAFT_DRAIN_LANE` (default on): every non-push command (acks, pops,
    /// renewals, nacks, transactions, timers, KV) waits in a priority lane that
    /// each cycle drains BEFORE any push, and a budget-cut command returns to the
    /// front of its own lane. Measured 2026-09-23 at 200k msg/s: with one FIFO
    /// queue, budget-cut pushes went back to its front and each 4 MB drain filled
    /// with them, so pops waited behind them past their long-poll deadline and
    /// consumption fell to zero while pushes kept landing. Draining first keeps
    /// the work that empties the system ahead of the work that fills it.
    pub drain_lane: bool,
    /// `QUEEN_RAFT_KV_SWEEP_MS` (WP-2.2, 026's cadence role): how often the
    /// leader plans one bounded KV expiry step. Reads never wait for it — an
    /// expired key reads as absent at once (§5.7) — so this only bounds how
    /// long a dead row keeps its RAM.
    pub kv_sweep_every_ms: u64,
    /// `QUEEN_RAFT_KV_SWEEP_LIMIT`: the most rows one step deletes.
    pub kv_sweep_limit: usize,
    /// `QUEEN_RAFT_TIMER_TICK_MS` (WP-2.3): the cadence of the leader's timer
    /// fire step (025's sweeper, as a leader loop). `0` = timers never fire.
    /// The step also runs in any cycle that planned a timers command, so a
    /// timer scheduled already due fires in the entry that schedules it.
    /// The programmatic default is OFF so the batcher's own tests plan exactly
    /// as before; [`BatcherConfig::from_env`] (every real node) turns it on.
    /// It never stops for maintenance: 025's fire does not either.
    pub timer_tick_ms: u64,
    /// The fire step's bounds and backoff.
    pub timer_fire: TimerFireConfig,
    /// Leader-only retention, trace-expiry and delete-resume cadence. `0`
    /// disables it (the programmatic test default); real nodes enable it from
    /// `RETENTION_INTERVAL`.
    pub maintenance_every_ms: u64,
    pub maintenance: crate::rsm::maintenance::Config,
    /// `QUEEN_RAFT_KEEP_OVERLAY` (default on): the planner thread keeps the
    /// overlay BETWEEN cycles and updates it per landed entry
    /// ([`crate::rsm::planner::kept`]). Off: every cycle rebuilds it from
    /// scratch, the path before the knob.
    pub keep_overlay: bool,
    /// `QUEEN_RAFT_KEEP_OVERLAY_VERIFY` (default off): also rebuild the overlay
    /// the old way every cycle and compare. A difference is logged and counted
    /// (`queen_raft_keep_overlay_total{outcome="mismatch"}`) and the rebuild is
    /// what plans. A diagnostic: it costs the rebuild it exists to save.
    pub keep_overlay_verify: bool,
    /// `QUEEN_LANES` (default 8): how many lane threads plan a cycle's
    /// commands in parallel ([`lanes`]). 1 is the single planner, exactly as
    /// before lanes.
    pub lanes: u64,
    /// `QUEEN_RAFT_CLOCK` (default `monotonic`): within a term the RSM clock
    /// runs on the leader's monotonic clock from where the term began
    /// ([`RunState::plan_wall`]); `wall` stamps the wall clock every cycle, as
    /// before.
    pub monotonic_clock: bool,
    /// `QUEEN_RAFT_RETENTION_SCAN` (default on in production): the retention
    /// walk runs on a background thread ([`crate::rsm::retention_scan`]) and
    /// the planner only judges what it proposes. `None` keeps the walk on the
    /// planning thread, as do lanes (`QUEEN_LANES` > 1).
    pub retention_scan: Option<crate::rsm::retention_scan::ScanConfig>,
}

impl Default for BatcherConfig {
    fn default() -> BatcherConfig {
        BatcherConfig {
            pipeline: 8,
            batch_max_cmds: crate::rsm::entry::BATCH_MAX_CMDS_DEFAULT,
            batch_max_bytes: crate::rsm::entry::BATCH_MAX_BYTES_DEFAULT,
            propose_ms: 5000,
            request_id_window_s: 60,
            request_expire_every_ms: 10_000,
            // The channel is only the hand-off: admission bounds what waits
            // (and the drain is by bytes and commands). At 1024 the facade's
            // senders blocked once a cycle's worth was queued, capping intake
            // at ~1024 per plan window with one command per message.
            command_queue_depth: 16_384,
            plan: PlanConfig::default(),
            driver_notify: true,
            push_priority: true,
            drain_greedy: true,
            drain_lane: true,
            kv_sweep_every_ms: 1_000,
            kv_sweep_limit: crate::rsm::planner::kv::SWEEP_LIMIT_DEFAULT,
            timer_tick_ms: 0,
            timer_fire: TimerFireConfig::default(),
            maintenance_every_ms: 0,
            maintenance: crate::rsm::maintenance::Config::default(),
            keep_overlay: true,
            keep_overlay_verify: false,
            lanes: 8,
            monotonic_clock: true,
            retention_scan: None,
        }
    }
}

/// The production timer tick (`QUEEN_RAFT_TIMER_TICK_MS` unset): the bound on
/// how late a due timer fires on an idle node, and the cost of one cheap
/// planning cycle (one fire-order seek) when nothing is due.
pub const TIMER_TICK_MS_DEFAULT: u64 = 50;

/// `QUEEN_QLOG_RECLAIM` (default on): whether the maintenance tick starts the
/// node-local queue-log reclaim. Off keeps every sealed file (disk grows); it
/// exists to measure what the reclaim costs the pipeline.
fn qlog_reclaim_from_env() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("QUEEN_QLOG_RECLAIM")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "false" | "off" | "no"
        )
    })
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
        // PERF-E: the dedup index authority the planner reads. The apply-side
        // `record` global is set separately at the production boot seam
        // (`real_builder`), NOT here — the facade's own unit tests build
        // `RaftFacade` directly and must NOT flip a process-wide `record` mode
        // that concurrent apply/store tests depend on. In production both come
        // from the same `QUEEN_RAFT_DEDUP_INDEX`, so they agree; in a facade
        // test the planner runs `txns` while `record` stays `rows` (writes both
        // keyspaces), which the txns reader handles correctly.
        // A boolean knob: only "0"/"false"/"off"/"no" turns it off (an unset or
        // malformed value keeps the default-on driver-notify path).
        fn flag(name: &str, cur: bool) -> bool {
            std::env::var(name)
                .ok()
                .map(|v| {
                    !matches!(
                        v.trim().to_ascii_lowercase().as_str(),
                        "0" | "false" | "off" | "no"
                    )
                })
                .unwrap_or(cur)
        }
        let index_mode = crate::rsm::dedup::IndexMode::from_env();
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
                index_mode,
            },
            driver_notify: flag("QUEEN_RAFT_DRIVER_NOTIFY", d.driver_notify),
            push_priority: flag("QUEEN_RAFT_PUSH_PRIORITY", d.push_priority),
            drain_greedy: flag("QUEEN_RAFT_DRAIN_GREEDY", d.drain_greedy),
            drain_lane: flag("QUEEN_RAFT_DRAIN_LANE", d.drain_lane),
            kv_sweep_every_ms: num("QUEEN_RAFT_KV_SWEEP_MS", d.kv_sweep_every_ms),
            kv_sweep_limit: num("QUEEN_RAFT_KV_SWEEP_LIMIT", d.kv_sweep_limit as u64) as usize,
            // `0` is honoured here (firing off), unlike the other numeric knobs.
            timer_tick_ms: std::env::var("QUEEN_RAFT_TIMER_TICK_MS")
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(TIMER_TICK_MS_DEFAULT),
            timer_fire: TimerFireConfig::from_env(),
            maintenance_every_ms: std::env::var("RETENTION_INTERVAL")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(5_000)
                .max(1),
            maintenance: crate::rsm::maintenance::Config {
                row_limit: num("RETENTION_BATCH_SIZE", d.maintenance.row_limit as u64) as usize,
                trace_retention_s: num(
                    "QUEEN_RAFT_TRACE_RETENTION_S",
                    d.maintenance.trace_retention_s as u64,
                ) as i64,
                partition_cleanup_enabled: flag(
                    "QUEEN_PARTITION_CLEANUP_ENABLED",
                    d.maintenance.partition_cleanup_enabled,
                ),
                partition_cleanup_days: num(
                    "PARTITION_CLEANUP_DAYS",
                    d.maintenance.partition_cleanup_days as u64,
                ) as i64,
                visit_cap: num("QUEEN_RAFT_RETENTION_VISIT", d.maintenance.visit_cap as u64)
                    as usize,
                walk: Default::default(),
                visit_total: num(
                    "QUEEN_RAFT_RETENTION_VISIT_TOTAL",
                    d.maintenance.visit_total as u64,
                ) as usize,
                walk_queue: Default::default(),
                txn_window_min_s: num(
                    "QUEEN_RAFT_TXN_WINDOW_MIN_S",
                    d.maintenance.txn_window_min_s as u64,
                ) as i64,
                partition_walk: true,
            },
            keep_overlay: flag("QUEEN_RAFT_KEEP_OVERLAY", d.keep_overlay),
            lanes: std::env::var("QUEEN_LANES")
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(d.lanes)
                .clamp(1, 64),
            monotonic_clock: std::env::var("QUEEN_RAFT_CLOCK")
                .map(|v| !v.trim().eq_ignore_ascii_case("wall"))
                .unwrap_or(d.monotonic_clock),
            retention_scan: crate::rsm::retention_scan::ScanConfig::from_env(),
            // Off unless explicitly turned on: only "1"/"true"/"on"/"yes".
            keep_overlay_verify: std::env::var("QUEEN_RAFT_KEEP_OVERLAY_VERIFY")
                .map(|v| {
                    matches!(
                        v.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "on" | "yes"
                    )
                })
                .unwrap_or(false),
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
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
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
    /// Phase B: push + ack all-or-nothing in ONE entry.
    Transaction(TxnCommand),
    /// A KV call carrying at least one write (024, WP-2.2).
    Kv(KvCommand),
    /// Timer schedules and cancels (025 `log_timers_apply_v1`, WP-2.3). The
    /// FIRE is not a command: it is the driver's own leader-loop step.
    Timers(TimersCommand),
    /// Deterministic Phase-2 metadata/control effects.
    Effects(EffectsCommand),
    /// One HTTP push that names several partitions: ONE command, ONE request
    /// id, ONE outcome (every item's verdict, in input order), instead of one
    /// command per (queue, partition) group. The lanes plan its groups in
    /// parallel, each in its partition's lane, and the merge logs it as one
    /// command. Not atomic across partitions (neither was a push request):
    /// each group is a plain push. Last in the enum so the forward codec keeps
    /// the indices of every older variant.
    MultiPush(MultiPushCommand),
}

/// A push request's per-(queue, partition) groups as one command
/// ([`Command::MultiPush`]). Each group is a whole [`PushCommand`] (its own
/// derived request id is not recorded: the parent's is).
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct MultiPushCommand {
    pub request_id: RequestId,
    /// In input order: the outcome's verdicts are these groups' items, group
    /// after group.
    pub pushes: Vec<PushCommand>,
}

impl MultiPushCommand {
    /// Plan every group against one overlay, in order, as ONE command: the
    /// single planner's reference for what the lanes produce in parallel.
    /// A group refused with nothing planned for the request so far refuses the
    /// whole command (a retryable refusal stays retryable); otherwise a
    /// refused group answers its items with a per-item refusal, as a push
    /// request does for an item it cannot take.
    pub(crate) fn plan<R: Reads + ?Sized>(&self, p: &Planner<'_, R>, ov: &mut Overlay) -> Planned {
        let mut effects: Vec<Effect> = Vec::new();
        let mut items: Vec<crate::rsm::entry::PushVerdict> = Vec::new();
        for push in &self.pushes {
            match p.plan_push(ov, push) {
                Ok(Plan::Logged {
                    effects: e,
                    outcome,
                }) => {
                    effects.extend(e);
                    items.extend(push_items(outcome, push.items.len()));
                }
                Ok(Plan::Empty(outcome)) => items.extend(push_items(outcome, push.items.len())),
                Ok(Plan::Refused(refusal)) | Err(refusal) => {
                    if effects.is_empty() && items.is_empty() && self.pushes.len() == 1 {
                        return Err(refusal);
                    }
                    items.extend(refused_items(&refusal, push.items.len()));
                }
            }
        }
        let outcome = Outcome::Push(crate::rsm::entry::PushOutcome { items });
        if effects.is_empty() {
            Ok(Plan::Empty(outcome))
        } else {
            Ok(Plan::Logged { effects, outcome })
        }
    }
}

/// A push group's verdicts out of its outcome (a push outcome always carries
/// one per item; anything else is answered as a refusal of each item).
pub(crate) fn push_items(outcome: Outcome, n: usize) -> Vec<crate::rsm::entry::PushVerdict> {
    match outcome {
        Outcome::Push(o) if o.items.len() == n => o.items,
        _ => refused_items(
            &Refusal::retry("internal", "a push planned a non-push outcome"),
            n,
        ),
    }
}

/// `n` per-item refusals carrying `refusal`'s code and message.
pub(crate) fn refused_items(refusal: &Refusal, n: usize) -> Vec<crate::rsm::entry::PushVerdict> {
    (0..n)
        .map(|_| crate::rsm::entry::PushVerdict::Refused {
            code: refusal.code.to_string(),
            message: refusal.message.to_string(),
        })
        .collect()
}

impl Command {
    /// Whether this is a consumption command: the consumption engine's
    /// ([`crate::rsm::consume`]) to serve on the leader, never the planner's.
    pub(crate) fn is_consumption(&self) -> bool {
        matches!(
            self,
            Command::PopWildcard(_)
                | Command::PopPinned(_)
                | Command::PopDiscover(_)
                | Command::Ack(_)
                | Command::AckPositional(_)
                | Command::Nack(_)
                | Command::Renew(_)
                | Command::DlqHead(_)
        )
    }

    /// Whether §11.8 must refuse this command while a node is above the disk
    /// high-water mark. Deletes, acknowledgements and admin cleanup continue.
    pub(crate) fn grows_storage(&self) -> bool {
        use crate::rsm::planner::timers::TimerOp;
        use crate::rsm::planner::KvOp;
        let kv_grows = |ops: &[KvOp]| {
            ops.iter()
                .any(|op| matches!(op, KvOp::Put { .. } | KvOp::Incr { .. }))
        };
        let timers_grow = |ops: &[TimerOp]| ops.iter().any(|op| matches!(op, TimerOp::Schedule(_)));
        let effects_grow = |effects: &[Effect]| {
            effects
                .iter()
                .any(|e| matches!(e, Effect::TraceAppend { .. }))
        };
        match self {
            Command::Push(_) | Command::MultiPush(_) => true,
            Command::Transaction(c) => {
                !c.pushes.is_empty()
                    || kv_grows(&c.kv)
                    || timers_grow(&c.timers)
                    || effects_grow(&c.extra_effects)
            }
            Command::Kv(c) => kv_grows(&c.ops),
            Command::Timers(c) => timers_grow(&c.ops),
            Command::Effects(c) => effects_grow(&c.effects),
            _ => false,
        }
    }

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
            Command::Transaction(c) => c.request_id,
            Command::Kv(c) => c.request_id,
            Command::Timers(c) => c.request_id,
            Command::Effects(c) => c.request_id,
            Command::MultiPush(c) => c.request_id,
        }
    }

    /// The kind, for the O18 per-kind planner metrics and the slow-command log.
    pub fn kind(&self) -> CommandKind {
        match self {
            Command::Push(_) | Command::MultiPush(_) => CommandKind::Push,
            Command::PopPinned(_) => CommandKind::PopPinned,
            Command::PopWildcard(_) => CommandKind::PopWildcard,
            Command::PopDiscover(_) => CommandKind::PopDiscover,
            Command::Ack(_) => CommandKind::Ack,
            Command::AckPositional(_) => CommandKind::AckPositional,
            Command::Nack(_) => CommandKind::Nack,
            Command::Renew(_) => CommandKind::Renew,
            Command::DlqHead(_) => CommandKind::DlqHead,
            Command::Transaction(_) => CommandKind::Transaction,
            Command::Kv(_) => CommandKind::Kv,
            Command::Timers(_) => CommandKind::Timers,
            Command::Effects(_) => CommandKind::Effects,
        }
    }

    /// A cheap upper estimate of the bytes this command will add to an entry,
    /// for cutting the drain at `batch_max_bytes` BEFORE planning. The exact
    /// bound is the codec's ([`encode_entry`]); this only decides how many
    /// commands share one cycle.
    pub(crate) fn size_hint(&self) -> usize {
        match self {
            Command::Push(c) => c.items.iter().map(|i| i.frame.len() + 16).sum::<usize>() + 64,
            Command::MultiPush(c) => c
                .pushes
                .iter()
                .map(|p| p.items.iter().map(|i| i.frame.len() + 16).sum::<usize>() + 64)
                .sum::<usize>(),
            Command::Ack(c) => {
                c.targets
                    .iter()
                    .map(|t| t.items.len() * 48 + 64)
                    .sum::<usize>()
                    + 32
            }
            Command::DlqHead(c) => c.snapshot.payload.len() + 128,
            Command::Transaction(c) => {
                c.pushes
                    .iter()
                    .map(|p| p.items.iter().map(|i| i.frame.len() + 16).sum::<usize>() + 64)
                    .sum::<usize>()
                    + c.acks
                        .iter()
                        .map(|t| t.items.len() * 48 + 64)
                        .sum::<usize>()
                    + c.kv.len() * 128
                    + c.timers.iter().map(|t| t.size_hint()).sum::<usize>()
                    + c.extra_effects
                        .iter()
                        .map(|e| e.encode_body().len() + 8)
                        .sum::<usize>()
                    + c.positions
                        .iter()
                        .map(|p| p.group.len() + p.metadata.len() + 112)
                        .sum::<usize>()
                    + 64
            }
            Command::Kv(c) => {
                c.ops
                    .iter()
                    .map(|op| match op {
                        crate::rsm::planner::KvOp::Put { value, key, .. } => {
                            value.len() + key.len() + 96
                        }
                        _ => 96,
                    })
                    .sum::<usize>()
                    + 64
            }
            Command::Timers(c) => c.ops.iter().map(|o| o.size_hint()).sum::<usize>() + 64,
            Command::Effects(c) => {
                c.effects
                    .iter()
                    .map(|e| e.encode_body().len() + 8)
                    .sum::<usize>()
                    + 64
            }
            _ => 128,
        }
    }

    /// How many messages this command carries, for the `drain_messages`
    /// histogram (PERF-1): a push's items, an ack's items, a timers call's
    /// ops, one for the rest.
    fn message_count(&self) -> u64 {
        match self {
            Command::Push(c) => c.items.len() as u64,
            Command::MultiPush(c) => c.pushes.iter().map(|p| p.items.len() as u64).sum(),
            Command::Ack(c) => c.targets.iter().map(|t| t.items.len() as u64).sum(),
            Command::Transaction(c) => {
                c.pushes.iter().map(|p| p.items.len() as u64).sum::<u64>()
                    + c.acks.iter().map(|t| t.items.len() as u64).sum::<u64>()
                    + c.extra_effects.len() as u64
                    + c.positions.len() as u64
            }
            Command::Timers(c) => c.ops.len().max(1) as u64,
            Command::Effects(c) => c.effects.len().max(1) as u64,
            _ => 1,
        }
    }

    /// The `(tenant, queue)` for the O18 slow-command log. Renew is
    /// worker-scoped and has no queue.
    fn label(&self) -> (&str, &str) {
        match self {
            Command::Push(c) => (&c.tenant, &c.queue),
            Command::MultiPush(c) => c
                .pushes
                .first()
                .map(|p| (p.tenant.as_str(), p.queue.as_str()))
                .unwrap_or(("", "")),
            Command::PopPinned(c) | Command::PopWildcard(c) | Command::PopDiscover(c) => {
                (&c.tenant, &c.queue)
            }
            Command::Ack(c) => c
                .targets
                .first()
                .map(|t| (t.tenant.as_str(), t.queue.as_str()))
                .unwrap_or(("", "")),
            Command::AckPositional(c) => (&c.tenant, &c.queue),
            Command::Nack(c) => (&c.tenant, &c.queue),
            Command::Renew(_) => ("", ""),
            Command::DlqHead(c) => (&c.tenant, &c.queue),
            Command::Transaction(c) => c
                .pushes
                .first()
                .map(|p| (p.tenant.as_str(), p.queue.as_str()))
                .unwrap_or((c.tenant.as_str(), "")),
            // No queue; never a namespace or a key either — names stay out of
            // shared logs (024 §13.5).
            Command::Kv(c) => (&c.tenant, ""),
            Command::Timers(c) => (&c.tenant, c.ops.first().map_or("", |o| o.queue())),
            Command::Effects(c) => (&c.tenant, ""),
        }
    }

    /// Dispatch to the matching `plan_*`. The planner folds a `Logged` plan's
    /// effects into the overlay before returning, so the next command in the
    /// cycle sees them (§7.2). Consumption is the engine's
    /// ([`crate::rsm::consume`]) and never reaches a planner: one that does is
    /// refused.
    fn plan<R: Reads + ?Sized>(&self, p: &Planner<'_, R>, ov: &mut Overlay) -> Planned {
        match self {
            Command::Push(c) => p.plan_push(ov, c),
            Command::PopPinned(_)
            | Command::PopWildcard(_)
            | Command::PopDiscover(_)
            | Command::Ack(_)
            | Command::AckPositional(_)
            | Command::Nack(_)
            | Command::Renew(_)
            | Command::DlqHead(_) => Err(Refusal::retry(
                "internal",
                "consumption commands are served by the engine",
            )),
            Command::Transaction(c) => p.plan_transaction(ov, c),
            Command::Kv(c) => p.plan_kv(ov, c),
            Command::Timers(c) => p.plan_timers(ov, c),
            Command::Effects(c) => p.plan_effects(ov, c),
            Command::MultiPush(c) => c.plan(p, ov),
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
    /// When the facade FIRST handed this to the channel (PERF-1, O18): the
    /// arrival stamp `arrival_to_proposed` measures from. PERF-J: this is the
    /// TRUE facade arrival and is NEVER restamped — a budget-cut command
    /// re-queued for the next cycle keeps its original value, so
    /// `arrival_to_proposed` measures the whole facade→proposed wait across
    /// every defer cycle. (Rounds 1-3 restamped it on defer, so the histogram
    /// measured only the final re-plan leg and hid the real ~270 ms C1000 wait
    /// — the artifact that invented the nonexistent "per-partition propose
    /// lock". The per-cycle slot wait is measured separately by `enqueued_at`.)
    ///
    /// `None` when the instrumentation is off (`QUEEN_RAFT_METRICS=0`): this is
    /// the highest-frequency timing read (one per COMMAND, at facade ingress),
    /// so it is taken through [`crate::rsm::timing::stamp`] like every other
    /// hot-path clock read — the knob removes the `Instant::now()` itself, not
    /// just the histogram write, so the VM ablation prices the whole lever (the
    /// PERF-1 refutation: a bare `Instant::now()` here left the arrival clock on
    /// even with the knob off). Its only consumer, the `arrivals` collection,
    /// already runs only when metrics are on, so every stamp there is `Some`.
    pub received_at: Option<Instant>,
    /// PERF-J: when this submission was LAST enqueued for a cycle — set at
    /// ingress and RESTAMPED every time a budget-cut command is re-queued. This
    /// is the per-cycle "slot wait" leg the `queue_wait` histogram (PERF-G's
    /// first leg) measures, kept distinct from `received_at` so fixing the
    /// arrival clock does not change what `queue_wait` reports. Follows the same
    /// metrics knob.
    pub enqueued_at: Option<Instant>,
}

impl Submission {
    pub fn new(command: Command) -> (Submission, oneshot::Receiver<Reply>) {
        let (tx, rx) = oneshot::channel();
        let at = crate::rsm::timing::stamp();
        (
            Submission {
                command,
                reply: tx,
                received_at: at,
                enqueued_at: at,
            },
            rx,
        )
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
    /// The term this driver planned the entry in. The applied index passing
    /// `index` proves the entry applied only when the entry applied there is
    /// of this term ([`Replicator::applied_term_at`]): once this node has lost
    /// the term, the new leader's entry sits at that index.
    term: u64,
    entry: Arc<Entry>,
    waiters: Vec<Waiter>,
    /// `Some` once `propose` resolved `Ok` (applied locally) or the hold was
    /// released after a timeout: the entry has left the pipeline (I3).
    resolved: Option<AppliedAt>,
    /// A `Timeout` kept this entry in flight (I3): its waiters were already
    /// answered `Retry`, and it stays folded into the overlay until it applies.
    timed_out: bool,
    /// Known committed ([`Replicator::committed_watch`]) before it applied
    /// here: it no longer holds a pipeline slot, and the waiters whose answer
    /// needs nothing but the outcome were answered ([`InFlightEntry::answer_committed`]).
    /// It stays in flight (folded into the overlay) until it applies.
    committed: bool,
    /// When `propose` was submitted (PERF-1): the `propose_roundtrip`
    /// histogram measures from here to the moment the result arrives. `None`
    /// when the instrumentation is off (`QUEEN_RAFT_METRICS=0`), so the knob
    /// prices the clock read at propose too, not just the histogram write.
    proposed_at: Option<Instant>,
}

impl InFlightEntry {
    /// Answer every waiter from this entry's committed outcome (`Ok`).
    fn resolve_ok(&mut self, at: AppliedAt) {
        self.resolved = Some(at);
        let waiters = std::mem::take(&mut self.waiters);
        for w in waiters {
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

    /// The entry is committed (not yet applied here): answer every waiter
    /// whose reply is its outcome alone — a push, a multi-push, an ack, a
    /// nack, and an empty read that barriered on this entry — and every
    /// command `at_commit` names (the consumption engine's checkpoints: durable
    /// once committed, and the claims and acks they carry wait on them). A pop
    /// keeps waiting for local apply (its answer reads this node's rows and
    /// payload files), as do KV calls (rendered by apply, at their position),
    /// transactions, renews and dead-letter reads.
    fn answer_committed(&mut self, at: AppliedAt, at_commit: &dyn Fn(&RequestId) -> bool) {
        let waiters = std::mem::take(&mut self.waiters);
        for w in waiters {
            match w {
                Waiter::Command { request_id, reply } => {
                    let outcome = self
                        .entry
                        .commands
                        .iter()
                        .find(|c| c.request_id == request_id)
                        .map(|c| &c.outcome);
                    match outcome {
                        Some(o) if answerable_at_commit(o) || at_commit(&request_id) => {
                            let _ = reply.send(Reply::Done {
                                outcome: o.clone(),
                                at: Some(at),
                            });
                        }
                        _ => self.waiters.push(Waiter::Command { request_id, reply }),
                    }
                }
                Waiter::Fixed { outcome, reply } => {
                    if answerable_at_commit(&outcome) {
                        let _ = reply.send(Reply::Done {
                            outcome,
                            at: Some(at),
                        });
                    } else {
                        self.waiters.push(Waiter::Fixed { outcome, reply });
                    }
                }
            }
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

impl Slot {
    /// Does this drained command become a waiter on the entry proposed THIS
    /// cycle — i.e. is its answer gated on that entry committing? Only these get
    /// an `arrival_to_proposed` sample (PERF-1): a `Logged` command and a
    /// `SameCycle` retry both wait on the new entry, and an `Empty` read that
    /// barriered on it (its `barrier_seq` is this entry's `seq`) is answered when
    /// it commits. Everything else drained this cycle was NOT proposed here — an
    /// `Immediate` answer, a `Deferred` re-queue, an `InFlightHit` on an OLDER
    /// entry, or an `Empty` read barriered on an older entry — so it must not be
    /// priced against this entry's propose (the metric counted every drained
    /// submission before). The caller has confirmed an entry was proposed, so an
    /// `Empty` with no barrier cannot reach here.
    fn waits_on_new_entry(&self, seq: u64, barrier_seq: Option<u64>) -> bool {
        match self {
            Slot::Logged(_) | Slot::SameCycle(_) => true,
            Slot::Empty(_) => barrier_seq == Some(seq),
            Slot::Immediate(_) | Slot::InFlightHit { .. } | Slot::Deferred(_) => false,
        }
    }
}

/// The arrival stamps of exactly the drained commands whose round-trip is the
/// entry proposed this cycle (see [`Slot::waits_on_new_entry`]). `slots` and
/// `arrivals` are in the same (plan) order; a shorter `arrivals` (the knob is
/// off, so it is empty) yields nothing.
fn proposed_arrivals(
    slots: &[Slot],
    arrivals: &[Instant],
    seq: u64,
    barrier_seq: Option<u64>,
) -> Vec<Instant> {
    slots
        .iter()
        .zip(arrivals)
        .filter_map(|(slot, at)| slot.waits_on_new_entry(seq, barrier_seq).then_some(*at))
        .collect()
}

/// What one blocking cycle produced.
pub(crate) struct PlanOutput {
    pub(crate) store_applied: u64,
    /// Shared with the kept overlay (KEEP_OVERLAY), which holds the entries it
    /// has folded until they land.
    pub(crate) entry: Option<Arc<Entry>>,
    slots: Vec<Slot>,
    /// The request-id expiry step (§10.1) was included in `entry`.
    expired: bool,
    /// ...and the committed store held a full step's worth past the cutoff:
    /// more are due now.
    expire_more: bool,
    /// The KV expiry sweep (WP-2.2) ran this cycle — whether or not it found
    /// anything due, so the driver clears its flag either way.
    kv_swept: bool,
    /// The timer fire step ran this cycle (WP-2.3), whatever it found.
    fired: bool,
    /// ...and hit one of its bounds: more timers are due now.
    fire_more: bool,
    maintained: bool,
    maintenance_more: bool,
}

/// The leader-loop step that fires due timers (WP-2.3): plan it against the
/// committed view plus the overlay — AFTER the cycle's commands, so a cancel or
/// a reschedule drained in the same cycle wins over the fire — and add its
/// effects to the entry as ONE command with its own minted id, answered by
/// nobody, exactly like the request-id expiry step. The message append and the
/// timer's removal (or its backoff) are therefore one entry: they commit, apply
/// and replay together, which is the whole of exactly-once in effect.
///
/// Returns whether the step hit a bound (more is due now).
fn plan_fire_step<R: Reads + ?Sized>(
    planner: &Planner<'_, R>,
    ov: &mut Overlay,
    entry: &mut Entry,
    cfg: &TimerFireConfig,
) -> bool {
    match planner.plan_timer_fire(ov, cfg) {
        Ok((effects, report)) => {
            if !effects.is_empty() {
                let id = crate::util::uuidv7_bytes();
                if let Err(e) = entry.add_command(id, Outcome::Empty, effects) {
                    // Unreachable (the effects are non-empty); the timers stay
                    // due and the next tick plans them again.
                    tracing::error!(target: "rsm", error = ?e, "timer fire step did not fit the entry");
                    return false;
                }
                tracing::debug!(
                    target: "rsm",
                    fired = report.fired,
                    duplicates = report.duplicates,
                    backed_off = report.backed_off,
                    dead_lettered = report.dead_lettered,
                    more = report.more,
                    "timer fire step",
                );
            }
            report.more
        }
        Err(r) => {
            // The candidate read failed before anything was folded (I14):
            // nothing fires this cycle; the next tick retries.
            tracing::warn!(
                target: "rsm",
                code = %r.code,
                message = %r.message,
                "timer fire step refused; retried on the next tick",
            );
            false
        }
    }
}

/// KEEP_OVERLAY: drop the kept state every this many cycles and rebuild it —
/// a bound on how long any drift nothing detected could live (a few seconds
/// at full load; one rebuild each time, the cost every cycle paid before).
pub(crate) const KEEP_RESET_EVERY: u64 = 1 << 14;
/// The most verify mismatches [`KeepStats`] remembers.
const KEEP_MISMATCH_KEEP: usize = 16;

/// How one cycle treats the state the planner thread keeps (KEEP_OVERLAY).
#[derive(Clone, Copy, Debug)]
pub(crate) struct KeepCfg {
    /// [`BatcherConfig::keep_overlay`]. Off: the overlay is rebuilt from
    /// scratch every cycle and nothing is kept.
    pub(crate) enabled: bool,
    /// The driver's epoch. It moves whenever the in-flight list stops being
    /// "the entries this thread planned, minus the ones that landed" — a lost
    /// leadership drains it, an entry that was planned but never proposed, a
    /// cycle that failed — and a state kept under another epoch is dropped.
    pub(crate) epoch: u64,
    /// [`BatcherConfig::keep_overlay_verify`]: compare with the old path's
    /// rebuild every cycle; plan from the rebuild when they differ.
    pub(crate) verify: bool,
    /// Drop the kept state every this many cycles (`0` = never).
    pub(crate) reset_every: u64,
}

impl KeepCfg {
    /// The old path: rebuild everything every cycle.
    pub(crate) fn off() -> KeepCfg {
        KeepCfg {
            enabled: false,
            epoch: 0,
            verify: false,
            reset_every: 0,
        }
    }
}

/// What the kept path did, for the tests and the metrics.
#[derive(Clone, Debug, Default)]
pub(crate) struct KeepStats {
    /// Cycles that advanced a kept overlay.
    pub(crate) kept: u64,
    /// Cycles that built the overlay from scratch with nothing kept (the first
    /// cycle, a new epoch, a periodic reset, the knob off).
    pub(crate) rebuilt: u64,
    /// Kept overlays that could not be advanced (rebuilt that cycle).
    pub(crate) fallbacks: u64,
    /// Cycles whose own folds did not match their entry: not kept.
    pub(crate) poisoned: u64,
    /// Verify mismatches, the first [`KEEP_MISMATCH_KEEP`].
    pub(crate) mismatches: Vec<String>,
}

impl KeepStats {
    fn mismatch(&mut self, what: String) {
        tracing::error!(target: "rsm", what, "KEEP_OVERLAY verify: the kept state differs from a rebuild");
        crate::rsm::timing::metrics()
            .keep_mismatch
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.mismatches.len() < KEEP_MISMATCH_KEEP {
            self.mismatches.push(what);
        }
    }
}

/// What the `queen-planner` thread keeps between cycles (KEEP_OVERLAY): the
/// overlay with the in-flight entries it folded. Owned by that one thread, so
/// it takes no lock.
#[derive(Default)]
pub(crate) struct PlannerState {
    overlay: Option<KeptOverlay>,
    epoch: u64,
    cycles: u64,
    pub(crate) stats: KeepStats,
    /// The lanes (`QUEEN_LANES` > 1), made on the first cycle that uses them.
    lanes: Option<Box<lanes::LanesState>>,
}

/// Plan one cycle inside ONE store read transaction (I15: on the planner
/// thread). `folded` are the in-flight entries, in index order, with the index
/// each occupies; `batch` are the drained commands, in order; `state` is what
/// the planner thread keeps between cycles (KEEP_OVERLAY).
// The cycle inputs are the scalars/handles the driver has already gathered;
// bundling them into a struct buys nothing at the single call site.
#[allow(clippy::too_many_arguments)]
pub(crate) fn plan_cycle_blocking<S: Store>(
    store: &S,
    front: &DedupFront,
    state: &mut PlannerState,
    keep: KeepCfg,
    reader: Option<Reader>,
    qlog_reader: Option<QLogReader>,
    folded: Vec<(u64, Arc<Entry>)>,
    batch: Vec<Command>,
    cfg: PlanConfig,
    wall_us: i64,
    expire_window_us: Option<i64>,
    kv_sweep_limit: Option<usize>,
    fire: Option<TimerFireConfig>,
    maintenance: Option<crate::rsm::maintenance::Config>,
    scan: Option<Arc<crate::rsm::retention_scan::ScanShared>>,
) -> crate::rsm::store::Result<PlanOutput> {
    // KEEP_OVERLAY: a state kept under another epoch (or with the knob off) is
    // gone, and every `reset_every` cycles it is rebuilt whatever it says.
    if !keep.enabled || state.epoch != keep.epoch {
        state.overlay = None;
        state.epoch = keep.epoch;
    }
    state.cycles += 1;
    let cycle_no = state.cycles;
    if keep.reset_every > 0 && cycle_no.is_multiple_of(keep.reset_every) {
        state.overlay = None;
    }
    // Taken OUT for the cycle: only a cycle that reaches its end puts it
    // back, so an error part-way (a store read refused, a panic) leaves
    // nothing half-advanced for the next one — it rebuilds.
    let prior = state.overlay.take();

    store.read(|r| {
        let store_applied = r.applied_index()?;
        let base_pid = r.next_pid()?;
        let base_kv = r.kv_version_next()?;

        // The overlay (§7.2): every in-flight entry the committed read does not
        // yet reflect, folded. KEEP_OVERLAY advances the kept one past the
        // entries that landed; the old path — the only one with the knob off,
        // and every fallback — rebuilds it from the in-flight list. Either way
        // its `Overlay::plan_now` lifts the stamp above every folded `now_us`
        // and `created_at`, so the effects a folded append carries stay
        // monotone (I5).
        let tm = crate::rsm::timing::metrics();
        let metrics_on = crate::rsm::timing::enabled();
        let bump = |c: &std::sync::atomic::AtomicU64| {
            if metrics_on {
                c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        };
        let mut kept = match prior.map(|k| k.advance(&folded, store_applied, base_pid, base_kv)) {
            Some(Ok(k)) => {
                state.stats.kept += 1;
                bump(&tm.keep_advanced);
                k
            }
            Some(Err(why)) => {
                state.stats.fallbacks += 1;
                bump(&tm.keep_fallback);
                tracing::debug!(target: "rsm", why, "KEEP_OVERLAY: the kept overlay is rebuilt");
                KeptOverlay::rebuild(&folded, store_applied, base_pid, base_kv)
            }
            None => {
                state.stats.rebuilt += 1;
                bump(&tm.keep_rebuilt);
                KeptOverlay::rebuild(&folded, store_applied, base_pid, base_kv)
            }
        };
        if keep.enabled && keep.verify {
            let reference = KeptOverlay::rebuild(&folded, store_applied, base_pid, base_kv);
            if let Some(what) = kept.diff(&reference) {
                state
                    .stats
                    .mismatch(format!("cycle {cycle_no}: overlay: {what}"));
                kept = reference;
            }
        }

        let committed = Committed::new(r);

        // The cycle's own folds carry its tag; each step below checks it folded
        // exactly the effects it added to the entry, so the kept overlay is the
        // fold of the entry and nothing else (`poisoned` otherwise: not kept).
        let cycle_tag = kept.begin_cycle();
        let mut poisoned = false;
        let ov = kept.overlay_mut();
        let now_us = ov
            .plan_now(&committed, wall_us)
            // A store error under the clock read: fail the whole cycle
            // retryably (I14).
            .map_err(|_| crate::rsm::store::StoreError::Io("clock read".into()))?;
        ov.mark_cycle_start();
        let cycle_folds0 = ov.folds();
        let mut planner = Planner::new(committed, now_us, cfg.clone(), front, reader.clone());
        // Phase A2: when the qlog knob is on, the planner reads the committed
        // `DEDUP_INDEX=segment` dedup authority from the per-queue log instead of
        // the `.seg` files (same committed bound, same O(claimed) walk). `None`
        // leaves the segment path untouched.
        planner.set_qlog_reader(qlog_reader.clone());

        let mut entry =
            Entry::with_capacity(now_us, ov.cycle_pid_base(), ov.cycle_kv_base(), batch.len());
        let mut slots: Vec<Slot> = Vec::with_capacity(batch.len());
        // Ids already logged into THIS entry, so a same-cycle retry becomes a
        // second waiter rather than a second command (§5.4; `Entry::validate`
        // forbids two commands sharing an id).
        let mut seen: HashMap<RequestId, ()> = HashMap::new();
        let batch_len = batch.len() as u64;
        let start = Instant::now();
        let loop_cpu0 = crate::rsm::timing::thread_cpu_ns();
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
            // O18: per-command planning time, per-kind counter and the slow log.
            // The clock read is gated on the knob (`stamp` is `None` when metrics
            // are off), so the ablation prices the whole per-command O18 leg —
            // the two clock reads and the record — not just the histogram write.
            let cmd_started = crate::rsm::timing::stamp();
            let (folds0, effects0) = (ov.folds(), entry.effects.len());
            let slot = match planner.lookup_request_id(ov, &id) {
                Err(refusal) => Slot::Immediate(Reply::Refused(refusal)),
                Ok(Lookup::Committed(outcome)) => {
                    Slot::Immediate(Reply::Done { outcome, at: None })
                }
                Ok(Lookup::InFlight(outcome)) => Slot::InFlightHit {
                    request_id: id,
                    outcome,
                },
                Ok(Lookup::Miss) => match cmd.plan(&planner, ov) {
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
            // A command folds exactly what it logs: a refused or empty one
            // folds nothing (a refused transaction restores the overlay). One
            // that folded and was not logged left effects in the overlay that
            // no entry carries; the old path dropped them with the cycle's
            // overlay, a kept one would carry them on — so it is not kept.
            poisoned |= ov.folds() - folds0 != (entry.effects.len() - effects0) as u64;
            slots.push(slot);
            if let Some(cmd_started) = cmd_started {
                let dur = cmd_started.elapsed();
                let kind = cmd.kind();
                let slow = cfg.slow_command_ms > 0 && dur.as_millis() as u64 >= cfg.slow_command_ms;
                let tm = crate::rsm::timing::metrics();
                tm.kinds.record(kind, dur, slow);
                if slow {
                    tm.slow_commands
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let (tenant, queue) = cmd.label();
                    tracing::warn!(
                        target: "rsm",
                        kind = kind.name(),
                        tenant,
                        queue,
                        duration_ms = dur.as_millis() as u64,
                        batch = batch_len,
                        "O18 slow command",
                    );
                }
            }
            cut = start.elapsed() > budget;
        }
        if crate::rsm::timing::enabled() {
            let tm = crate::rsm::timing::metrics();
            tm.plan.record_dur(start.elapsed());
            tm.plan_cpu
                .record(crate::rsm::timing::thread_cpu_ns().saturating_sub(loop_cpu0));
            tm.plan_deferred.record(
                slots
                    .iter()
                    .filter(|s| matches!(s, Slot::Deferred(_)))
                    .count() as u64,
            );
        }

        // WP-2.3 timer fire: after the commands (a cancel or a reschedule
        // drained this cycle wins), before the KV sweep and the expiry step.
        // The two leader steps touch disjoint state (timers and the messages
        // they push; KV rows), so their relative order is immaterial.
        let fired = fire.is_some();
        let (folds0, effects0) = (ov.folds(), entry.effects.len());
        let fire_more = match fire.as_ref() {
            Some(fcfg) => plan_fire_step(&planner, ov, &mut entry, fcfg),
            None => false,
        };
        poisoned |= ov.folds() - folds0 != (entry.effects.len() - effects0) as u64;

        // WP-2.7/2.8: bounded leader-only retention, trace expiry and
        // crash-resumable admin garbage. It is a normal command in this entry,
        // so every watermark/delete has the same commit and replay guarantees
        // as a client write.
        let maintained = maintenance.is_some();
        let mut maintenance_more = false;
        if let Some(mcfg) = maintenance.as_ref() {
            let mut planned = crate::rsm::maintenance::plan(r, now_us, mcfg)?;
            maintenance_more = planned.more;
            // Retention judges a partition dead from committed state alone. One
            // a push wrote this cycle or in an entry still in flight is not
            // dead: deleting it would drop the message that push was answered
            // for. A later tick judges it again.
            planned.effects.retain(|e| match e {
                Effect::PartitionDelete { pid } => !ov.touches_partition(*pid),
                _ => true,
            });
            if !planned.effects.is_empty() {
                let id = crate::util::uuidv7_bytes();
                if entry
                    .add_command(id, Outcome::Empty, planned.effects.clone())
                    .is_ok()
                {
                    ov.apply_effects(&planned.effects);
                }
            }
        }

        // Background retention (`QUEEN_RAFT_RETENTION_SCAN`): what the scanner
        // proposed, judged again against committed state and everything in
        // flight — its snapshot may be old — and logged like the walk's own.
        if let Some(scan) = scan.as_ref() {
            let proposals = scan.take(crate::rsm::retention_scan::TAKE_PER_CYCLE);
            if !proposals.is_empty() {
                let (effects, _dropped) =
                    crate::rsm::retention_scan::judge(r, ov, now_us, scan, &proposals)?;
                if !effects.is_empty() {
                    let id = crate::util::uuidv7_bytes();
                    if entry
                        .add_command(id, Outcome::Empty, effects.clone())
                        .is_ok()
                    {
                        ov.apply_effects(&effects);
                    }
                }
            }
        }

        // WP-2.2 KV expiry sweep (026's `kv_expire_step_v1` as a leader loop):
        // one bounded step, planned AFTER this cycle's commands so it sees
        // their writes in the overlay (a key a command just rewrote is not
        // swept), and one command with its own minted id, answered by nobody.
        // A store refusal skips the step until the next tick.
        let mut kv_swept = false;
        if let Some(limit) = kv_sweep_limit {
            kv_swept = true;
            if let Ok(effects) = planner.plan_kv_sweep(ov, limit) {
                if !effects.is_empty() {
                    let id = crate::util::uuidv7_bytes();
                    if entry
                        .add_command(id, Outcome::Empty, effects.clone())
                        .is_ok()
                    {
                        ov.apply_effects(&effects);
                    }
                }
            }
        }

        // §10.1 request-id expiry: a leader-loop step is one command with its
        // own minted id (see `Entry::validate`), answered by nobody. Its one
        // effect is folded too — the fold reads nothing from it; it only keeps
        // the count of folded effects equal to the entry's.
        let mut expired = false;
        let mut expire_more = false;
        if let Some(window_us) = expire_window_us {
            let cutoff = now_us.saturating_sub(window_us);
            let id = crate::util::uuidv7_bytes();
            let effects = vec![Effect::RequestIdsExpire { cutoff_us: cutoff }];
            if entry
                .add_command(id, Outcome::Empty, effects.clone())
                .is_ok()
            {
                ov.apply_effects(&effects);
                expired = true;
                // One step retires at most REQUEST_EXPIRE_LIMIT outcomes. When
                // the committed store holds that many past the cutoff, the next
                // step runs in the next cycle, not at the next tick: one step
                // per 10 s tick (~410 outcomes/s) never caught up with ~5,900
                // commands/s, and the table grew without bound (measured
                // 2026-09-23). Only the leader's cadence changes; what a step
                // retires does not (I2). Steps still in flight are not seen
                // here, so up to a pipeline's worth may find nothing.
                let limit = crate::rsm::apply::REQUEST_EXPIRE_LIMIT;
                let mut past = 0usize;
                r.scan_request_expiry(limit, &mut |at, _| {
                    if at >= cutoff {
                        return false;
                    }
                    past += 1;
                    true
                })?;
                expire_more = past >= limit;
            }
        }
        // The cycle as a whole: its overlay holds the fold of its entry, and
        // nothing else (the per-step checks above make this the sum of them).
        poisoned |= ov.folds() - cycle_folds0 != entry.effects.len() as u64;
        drop(planner);

        let entry = if entry.commands.is_empty() {
            None
        } else {
            Some(Arc::new(entry))
        };

        // KEEP_OVERLAY: keep the overlay — now carrying this cycle's entry as in
        // flight — for the next cycle. An overlay that folded anything its
        // entry does not carry, or whose build met a request id twice, is
        // dropped: the next cycle rebuilds.
        if keep.enabled {
            let mut keep_it = !poisoned && kept.exact();
            if keep_it {
                if let Some(e) = &entry {
                    keep_it = kept.push_entry(e.clone(), cycle_tag).is_ok();
                }
            }
            if keep_it {
                state.overlay = Some(kept);
            } else {
                state.stats.poisoned += 1;
                bump(&tm.keep_poisoned);
            }
        }
        Ok(PlanOutput {
            store_applied,
            entry,
            slots,
            expired,
            expire_more,
            kv_swept,
            fired,
            fire_more,
            maintained,
            maintenance_more,
        })
    })
}

// ---------------------------------------------------------------------------
// The batcher
// ---------------------------------------------------------------------------

/// The leader-side cycle driver. Build one with [`Batcher::new`] and start it
/// with [`Batcher::spawn`], which returns the channel the facade feeds and a
/// join handle for the driver task.
/// A membership change's request to the driver
/// (`rsm::replicator::raft::admin::change_on_leader`). openraft appends the
/// change's config entries itself while this node leads, and every entry in
/// flight was proposed at a PREDICTED index that those entries would shift —
/// the overlay fold, the drop gate and the applied-index wake all trust it. So
/// the driver stops planning, answers `drained` once nothing is in flight,
/// and plans again from the log's end once `resume` resolves (or is dropped).
pub struct QuiesceReq {
    pub drained: oneshot::Sender<()>,
    pub resume: oneshot::Receiver<()>,
}

/// Resumes the driver when dropped ([`quiesce_hook`]).
pub struct QuiesceGuard {
    _resume: oneshot::Sender<()>,
}

/// The hook a membership change pauses the driver behind `tx` with: it
/// resolves once nothing is in flight, to a guard that resumes the driver when
/// dropped; `None` when the driver is gone.
pub fn quiesce_hook(
    tx: mpsc::UnboundedSender<QuiesceReq>,
) -> crate::rsm::replicator::raft::QuiesceHook {
    Arc::new(move || {
        let tx = tx.clone();
        Box::pin(async move {
            let (drained, drained_rx) = oneshot::channel();
            let (resume_tx, resume) = oneshot::channel();
            tx.send(QuiesceReq { drained, resume }).ok()?;
            drained_rx.await.ok()?;
            Some(Box::new(QuiesceGuard { _resume: resume_tx }) as Box<dyn Send>)
        })
    })
}

/// A driver pausing for a [`QuiesceReq`].
struct Quiescing {
    drained: Option<oneshot::Sender<()>>,
    resume: oneshot::Receiver<()>,
}

pub struct Batcher<S: Store, R: Replicator> {
    store: Arc<S>,
    repl: Arc<R>,
    cfg: BatcherConfig,
    /// The persistent dedup front (PERF-B), shared with the blocking planner
    /// each cycle. One per broker; reset on leadership regain.
    front: Arc<DedupFront>,
    /// PERF-E `DEDUP_INDEX=segment`: the segment read side the planner serves the
    /// committed dedup authority from. `None` until the facade hands it in with
    /// [`Batcher::with_reader`]; only `segment` mode reads it.
    reader: Option<Reader>,
    /// Phase A2 (`QUEEN_RAFT_QLOG`): the per-queue-log read side. When present
    /// AND `DEDUP_INDEX=segment`, the planner reads the committed dedup authority
    /// from the qlog instead of the `.seg` files. `None` when the knob is off.
    qlog_reader: Option<QLogReader>,
    /// Membership changes pausing the driver ([`QuiesceReq`]).
    quiesce_rx: Option<mpsc::UnboundedReceiver<QuiesceReq>>,
    /// The facade's consumption engine ([`crate::rsm::consume`]): told when
    /// this driver starts and stops planning as leader, and when a
    /// transaction's entry lands; it serves any consumption command that
    /// still reaches this driver. `None`: no engine (driver tests).
    engine: Option<Arc<crate::rsm::consume::Engine>>,
}

/// `QUEEN_RAFT_ANSWER_AT_COMMIT` (default on): free pipeline slots and answer
/// pushes and acks when their entry commits, not when it applies here
/// ([`Replicator::committed_watch`]).
static ANSWER_AT_COMMIT: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
    !matches!(
        std::env::var("QUEEN_RAFT_ANSWER_AT_COMMIT")
            .as_deref()
            .map(str::trim),
        Ok("0") | Ok("false") | Ok("off") | Ok("no")
    )
});

/// `QUEEN_RAFT_COMMIT_LAG_MAX` (default 64): how many entries past the pipeline
/// may be committed and not yet applied here before planning waits for apply.
/// Each is folded into every planning overlay until it applies, and holds its
/// entry in RAM.
static COMMIT_LAG_MAX: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::env::var("QUEEN_RAFT_COMMIT_LAG_MAX")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(64)
});

/// `QUEEN_RAFT_PIPELINED_PLANNING` (default on): a cycle launches the next
/// one as soon as its own entry is known good, before routing its answers and
/// proposing it ([`RunState::prefetch`]).
static PIPELINED_PLANNING: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
    !matches!(
        std::env::var("QUEEN_RAFT_PIPELINED_PLANNING")
            .as_deref()
            .map(str::trim),
        Ok("0") | Ok("false") | Ok("off") | Ok("no")
    )
});

/// A cycle handed to the planner thread and not finished yet: everything
/// the second half needs ([`RunState::finish`]).
struct Launched {
    rx: tokio::sync::oneshot::Receiver<crate::rsm::store::Result<PlanOutput>>,
    replies: Vec<oneshot::Sender<Reply>>,
    received: Vec<Option<Instant>>,
    arrivals: Vec<Instant>,
    /// The request id of each drained transaction, by batch position (the
    /// engine releases the reservation of one this cycle does not log).
    txn_ids: Vec<Option<RequestId>>,
    epoch0: u64,
    hold0: bool,
    expire: bool,
    kv_sweep: bool,
    fire: bool,
    maintenance: bool,
    scanned: bool,
    trace: bool,
    tr: (usize, usize, usize, u64, usize),
    timing_on: bool,
    drain_started: Option<Instant>,
}

/// Whether a reply is its outcome alone, rendered with no read of this
/// node's state: an ack, or a push none of whose items is a duplicate. A
/// duplicate's answer carries the ORIGINAL message id, read from the original
/// frame in this node's queue log, which a commit reached by the followers'
/// acks may precede here; a pop reads rows and payloads. Those wait for apply.
fn answerable_at_commit(o: &Outcome) -> bool {
    match o {
        Outcome::Ack(_) => true,
        Outcome::Push(p) => !p
            .items
            .iter()
            .any(|v| matches!(v, crate::rsm::entry::PushVerdict::Duplicate { .. })),
        _ => false,
    }
}

/// Resolves when the watch changes; never when there is none (a disabled
/// `select!` arm must still type-check).
async fn watch_changed(rx: &mut Option<watch::Receiver<(u64, u64)>>) {
    match rx.as_mut() {
        Some(rx) => {
            if rx.changed().await.is_err() {
                std::future::pending::<()>().await
            }
        }
        None => std::future::pending::<()>().await,
    }
}

impl<S: Store + 'static, R: Replicator> Batcher<S, R> {
    pub fn new(store: Arc<S>, repl: Arc<R>, cfg: BatcherConfig) -> Batcher<S, R> {
        let front = Arc::new(DedupFront::from_env());
        DedupFront::spawn_sweeper(&front);
        Batcher {
            store,
            repl,
            cfg,
            front,
            reader: None,
            qlog_reader: None,
            quiesce_rx: None,
            engine: None,
        }
    }

    /// Let membership changes pause this driver around openraft's own config
    /// entries ([`QuiesceReq`]).
    pub fn with_quiesce(mut self, rx: mpsc::UnboundedReceiver<QuiesceReq>) -> Batcher<S, R> {
        self.quiesce_rx = Some(rx);
        self
    }

    /// Hand the driver the facade's consumption engine (see [`Batcher::engine`]).
    pub fn with_engine(mut self, engine: Arc<crate::rsm::consume::Engine>) -> Batcher<S, R> {
        self.engine = Some(engine);
        self
    }

    /// Hand the batcher the segment [`Reader`] the planner uses to serve the
    /// committed dedup authority under `DEDUP_INDEX=segment` (PERF-E). The
    /// facade owns the `Segments`/apply and clones a `Reader` for the planner
    /// side; the default modes never read it.
    pub fn with_reader(mut self, reader: Reader) -> Batcher<S, R> {
        self.reader = Some(reader);
        self
    }

    /// Hand the batcher the per-queue-log [`QLogReader`] (Phase A2). When present
    /// and `DEDUP_INDEX=segment`, the planner reads the committed dedup authority
    /// from the qlog instead of the segments; `None` (the knob off) keeps the
    /// segment path.
    pub fn with_qlog_reader(mut self, qlog_reader: Option<QLogReader>) -> Batcher<S, R> {
        self.qlog_reader = qlog_reader;
        self
    }

    /// Start the driver on the current runtime. Returns the command sender and
    /// the task handle; dropping every sender drains the driver and exits it.
    pub fn spawn(self) -> (CommandTx, tokio::task::JoinHandle<()>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(self.cfg.command_queue_depth);
        // Leading already (a single node, a restart that won its election
        // before this driver starts): the engine serves this term from now on,
        // not from the driver's first poll — a command sent the moment the
        // facade opened would be answered `Retry` in between.
        let role = *self.repl.watch_role().borrow();
        let told = match (role, &self.engine) {
            (Role::Leader { term }, Some(e)) => {
                e.on_leader(term, self.repl.metrics().last_log_index);
                Some(term)
            }
            _ => None,
        };
        // W1: the driver is core. It is a task on the caller's runtime, not a
        // named thread, so the `core` wrapper (not the thread name) is what
        // makes a panic in it abort instead of silently wedging every write.
        let handle = tokio::spawn(crate::obs::panic_policy::core(self.run(cmd_rx, told)));
        (cmd_tx, handle)
    }

    /// The driver. `told`: the term the engine was told it leads at spawn.
    async fn run(self, cmd_rx: mpsc::Receiver<Submission>, told: Option<u64>) {
        let (result_tx, result_rx) = mpsc::unbounded_channel();
        let mut expire =
            tokio::time::interval(Duration::from_millis(self.cfg.request_expire_every_ms));
        expire.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The first tick fires immediately; swallow it so the driver does not
        // propose an expiry step before it has done any work.
        expire.tick().await;
        // WP-2.2: the KV expiry sweep's cadence, on the same pattern.
        let mut kv_sweep =
            tokio::time::interval(Duration::from_millis(self.cfg.kv_sweep_every_ms.max(1)));
        kv_sweep.set_missed_tick_behavior(MissedTickBehavior::Delay);
        kv_sweep.tick().await;
        // WP-2.3: the timer fire cadence. Off (`0`) keeps the arm disabled. Its
        // first tick is NOT swallowed: timers already due at boot (a restart
        // after downtime) fire in the first cycle.
        let timers_on = self.cfg.timer_tick_ms > 0;
        let mut timer_tick =
            tokio::time::interval(Duration::from_millis(self.cfg.timer_tick_ms.max(1)));
        timer_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let maintenance_on = self.cfg.maintenance_every_ms > 0;
        let mut maintenance_tick =
            tokio::time::interval(Duration::from_millis(self.cfg.maintenance_every_ms.max(1)));
        maintenance_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // Capture what the watch and metrics say BEFORE the fields move into
        // `RunState`.
        let role_rx = self.repl.watch_role();
        let role = *role_rx.borrow();
        let next_index = self.repl.metrics().last_log_index + 1;
        // PERF-G: the applied-index wake, when the backend and the knob both
        // offer it. `None` disables the driver-notify select arm and keeps the
        // per-propose await path.
        let applied_notify = if self.cfg.driver_notify {
            self.repl.applied_notify()
        } else {
            None
        };
        // Background retention: the partition walk leaves the planning thread.
        // With lanes, each watermark proposal is judged by its partition's
        // lane, in parallel, and only partition deletes go to control: the walk
        // left in control covered 819 partitions/s (a round took 20 min at 1M
        // partitions, 3.4 h at 10M), and judging every proposal in control cost
        // ~2 ms of each serial cycle (2026-09-29).
        let mut cfg = self.cfg;
        let scan = match cfg.retention_scan {
            Some(scan_cfg) if maintenance_on => {
                let shared = crate::rsm::retention_scan::ScanShared::new(&cfg.maintenance);
                match crate::rsm::retention_scan::spawn(
                    self.store.clone(),
                    cfg.maintenance.clone(),
                    scan_cfg,
                    shared.clone(),
                ) {
                    Ok(()) => {
                        cfg.maintenance.partition_walk = false;
                        Some(shared)
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "rsm",
                            error = %e,
                            "rsm retention scanner did not start; the walk stays on the planner",
                        );
                        None
                    }
                }
            }
            _ => None,
        };
        let scan_on = scan.is_some();
        let mut scan_tick = tokio::time::interval(Duration::from_millis(200));
        scan_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let committed_rx = if *ANSWER_AT_COMMIT {
            self.repl.committed_watch()
        } else {
            None
        };
        let mut st = RunState {
            planner_thread: PlannerThread::spawn(),
            scan,
            scan_due: false,
            store: self.store,
            repl: self.repl,
            cfg,
            front: self.front,
            reader: self.reader,
            qlog_reader: self.qlog_reader,
            role_rx,
            cmd_rx,
            result_tx,
            result_rx,
            applied_notify,
            committed_rx,
            queue: VecDeque::new(),
            lane: VecDeque::new(),
            inflight: VecDeque::new(),
            next_seq: 1,
            next_index,
            holding_until: None,
            paused: !role.is_leader(),
            planning_term: match role {
                Role::Leader { term } => Some(term),
                _ => None,
            },
            closing: false,
            expire_due: false,
            kv_sweep_due: false,
            timers_due: false,
            maintenance_due: maintenance_on,
            qlog_gc_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            stopped: false,
            wake_reason: "init",
            wake_seq: 0,
            last_cycle_at: None,
            plan_epoch: 0,
            quiesce_rx: self.quiesce_rx,
            engine: self.engine,
            prefetch: None,
            quiescing: None,
            realign_at: None,
            clock_anchor: None,
        };
        if role.is_leader() {
            st.begin_term_clock();
            // Leading from the start: the engine serves this term (it was
            // told at spawn, unless the term changed since).
            if let (Role::Leader { term }, Some(e)) = (role, &st.engine) {
                if told != Some(term) {
                    e.on_leader(term, st.repl.metrics().last_log_index);
                }
            }
        } else if let (Some(_), Some(e)) = (told, &st.engine) {
            // Leadership went between the spawn and this first look.
            e.on_step_down();
        }

        loop {
            // Take every command that has already arrived BEFORE planning. The
            // biased select below reaches the command channel last, so without
            // this a cycle plans on a partial queue while the channel fills:
            // faster cycles → smaller entries → more result wakes → even less
            // intake (measured: 1024-deep channel full, submit waits 6-110 ms).
            while let Ok(more) = st.cmd_rx.try_recv() {
                st.enqueue(more);
            }
            st.sync_scan();
            // A cycle launched early by the last one is always finished, even
            // when nothing else is left to plan (it may have drained the queue).
            while st.prefetch.is_some() || st.can_plan() {
                st.plan_cycle().await;
                // Take what arrived while that cycle planned before the next
                // one drains (the channel is otherwise read only at the top of
                // this loop, and back-to-back cycles would leave it full).
                while let Ok(more) = st.cmd_rx.try_recv() {
                    st.enqueue(more);
                }
            }
            if st.should_exit() {
                break;
            }

            tokio::select! {
                biased;
                _ = st.role_rx.changed() => {
                    st.note_wake("role");
                    let role = *st.role_rx.borrow();
                    st.on_role(role);
                }
                Some((seq, res)) = st.result_rx.recv() => {
                    st.note_wake("result");
                    st.on_result(seq, res);
                }
                Some(req) = recv_quiesce(&mut st.quiesce_rx) => {
                    st.note_wake("quiesce");
                    st.on_quiesce(req);
                }
                _ = quiesce_resumed(&mut st.quiescing), if st.quiescing.is_some() => {
                    st.note_wake("resume");
                    st.on_resume();
                }
                // PERF-G: the applied index advanced (QUEEN_RAFT_DRIVER_NOTIFY).
                // Resolve every in-flight entry the apply has now passed, so the
                // freed pipeline slot is reused on this one wake rather than
                // waiting for each entry's forwarding task.
                _ = wait_notify(&st.applied_notify), if st.applied_notify.is_some() => {
                    st.note_wake("applied_notify");
                    st.on_applied_wake();
                }
                // The commit index advanced: free the slots of this leader's
                // entries it covers and answer what needs no local apply.
                _ = watch_changed(&mut st.committed_rx), if st.committed_rx.is_some() => {
                    st.note_wake("committed");
                    st.on_committed();
                }
                _ = expire.tick() => {
                    st.note_wake("expire");
                    st.expire_due = true;
                }
                _ = kv_sweep.tick() => {
                    st.note_wake("kv_sweep");
                    st.kv_sweep_due = true;
                }
                _ = timer_tick.tick(), if timers_on => {
                    st.note_wake("timers");
                    st.timers_due = true;
                }
                _ = maintenance_tick.tick(), if maintenance_on => {
                    st.note_wake("maintenance");
                    st.maintenance_due = true;
                    st.start_qlog_gc();
                }
                _ = scan_tick.tick(), if scan_on => {
                    if st.scan.as_ref().is_some_and(|s| s.queued() > 0) {
                        st.note_wake("retention_scan");
                        st.scan_due = true;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(HOLD_POLL_MS)),
                    if st.holding_until.is_some() || st.realign_at.is_some() || st.quiescing.is_some() =>
                {
                    st.note_wake("hold");
                    st.check_hold();
                }
                maybe = st.cmd_rx.recv() => {
                    st.note_wake("arrival");
                    match maybe {
                        Some(sub) => {
                            st.enqueue(sub);
                            // PERF-L level-1 fusion: pull everything already
                            // queued in the channel into this cycle's batch so
                            // one log entry (hence one fsync) carries many
                            // commands, instead of ~one per select wake. Order
                            // preserved; drain_batch still applies the caps.
                            if st.cfg.drain_greedy {
                                while let Ok(more) = st.cmd_rx.try_recv() {
                                    st.enqueue(more);
                                }
                            }
                        }
                        None => st.closing = true,
                    }
                }
            }

            st.check_quiesced();
            if st.stopped {
                break;
            }
        }

        if let Some(scan) = &st.scan {
            scan.stop();
        }
        // On a Fatal exit, nothing else will answer the stragglers.
        st.fail_all(None);
    }
}

// The driver's live state. A struct so the `select!` arms borrow disjoint
// channel fields while the handlers take `&mut self` afterwards.
/// ONE dedicated OS thread (`queen-planner`) that runs every planning cycle, in
/// place of a hop onto the shared blocking pool (where the planner ran on a
/// different pool thread almost every cycle — 53 threads in a 20 s profile —
/// and shared the pool with every facade store read). A fixed thread gives it
/// warm caches, its own scheduler statistics, and a home for state kept
/// between cycles: the [`PlannerState`] lives on its stack and every job gets
/// it by `&mut` (KEEP_OVERLAY). Jobs run strictly one at a time, in submission
/// order.
struct PlannerThread {
    tx: std::sync::mpsc::Sender<PlannerJob>,
}

type PlannerJob = Box<dyn FnOnce(&mut PlannerState) + Send>;

/// `QUEEN_RAFT_PLAN_NICE` (default -10; 0 = leave it): the nice value the
/// planner and lane threads ask for. A cycle forks eight lane jobs and waits
/// for the slowest; on a leader running ~10 of its 16 cores (HTTP workers,
/// replication, compression) the last lane thread started ~2 ms after its
/// job was handed over, a fifth of the cycle (2026-09-29). Without the
/// privilege (most containers) the call fails and nothing changes.
static PLAN_NICE: std::sync::LazyLock<i32> = std::sync::LazyLock::new(|| {
    std::env::var("QUEEN_RAFT_PLAN_NICE")
        .ok()
        .and_then(|v| v.trim().parse::<i32>().ok())
        .unwrap_or(-10)
        .clamp(-20, 19)
});

/// Give the calling thread [`PLAN_NICE`] (Linux; best effort).
pub(crate) fn raise_planning_thread_priority() {
    #[cfg(target_os = "linux")]
    {
        let nice = *PLAN_NICE;
        if nice == 0 {
            return;
        }
        // SAFETY: gettid takes no arguments; setpriority reads only its
        // integer arguments. A refusal (EPERM/EACCES) leaves the thread as it was.
        unsafe {
            let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
            if libc::setpriority(libc::PRIO_PROCESS, tid, nice) != 0 {
                tracing::debug!(target: "rsm", nice, "planning thread priority unchanged (no privilege)");
            }
        }
    }
}

impl PlannerThread {
    fn spawn() -> PlannerThread {
        let (tx, rx) = std::sync::mpsc::channel::<PlannerJob>();
        std::thread::Builder::new()
            .name("queen-planner".into())
            .spawn(move || {
                raise_planning_thread_priority();
                let mut state = PlannerState::default();
                while let Ok(job) = rx.recv() {
                    job(&mut state);
                }
            })
            .expect("spawn the queen-planner thread");
        PlannerThread { tx }
    }

    /// Run `f` on the planner thread, with its kept state; the result comes
    /// back on the returned channel (closed without a value if the thread is
    /// gone).
    fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut PlannerState) -> T + Send + 'static,
    ) -> tokio::sync::oneshot::Receiver<T> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.tx.send(Box::new(move |state: &mut PlannerState| {
            let _ = tx.send(f(state));
        }));
        rx
    }
}

struct RunState<S: Store, R: Replicator> {
    planner_thread: PlannerThread,
    /// The background retention scanner's handle ([`crate::rsm::retention_scan`]),
    /// when it runs; `scan_due` asks a cycle to plan what it queued.
    scan: Option<Arc<crate::rsm::retention_scan::ScanShared>>,
    scan_due: bool,
    store: Arc<S>,
    repl: Arc<R>,
    cfg: BatcherConfig,
    front: Arc<DedupFront>,
    /// PERF-E: the segment reader for `DEDUP_INDEX=segment`, cloned into each
    /// blocking plan cycle. `None` outside `segment` mode.
    reader: Option<Reader>,
    /// Phase A2: the per-queue-log reader, cloned into each blocking plan cycle
    /// when `QUEEN_RAFT_QLOG` is on. The planner reads the committed dedup
    /// authority from it (under `DEDUP_INDEX=segment`) instead of the segments.
    qlog_reader: Option<QLogReader>,
    role_rx: watch::Receiver<Role>,
    cmd_rx: mpsc::Receiver<Submission>,
    result_tx: mpsc::UnboundedSender<(u64, Result<AppliedAt, ProposeError>)>,
    result_rx: mpsc::UnboundedReceiver<(u64, Result<AppliedAt, ProposeError>)>,
    /// PERF-G: the replicator's applied-index wake (`QUEEN_RAFT_DRIVER_NOTIFY`),
    /// or `None` when the knob is off or the backend does not expose it.
    applied_notify: Option<Arc<Notify>>,
    /// The replicator's committed log id ([`Replicator::committed_watch`]),
    /// `None` when `QUEEN_RAFT_ANSWER_AT_COMMIT=0` or the backend reports
    /// commits only through apply.
    committed_rx: Option<watch::Receiver<(u64, u64)>>,
    queue: VecDeque<Submission>,
    /// The priority lane (`drain_lane`): non-push commands, drained first.
    lane: VecDeque<Submission>,
    inflight: VecDeque<InFlightEntry>,
    next_seq: u64,
    next_index: u64,
    /// `Some(index)` while the pipeline is held on a timeout (I3): plan nothing
    /// until the committed apply reaches this index or the role changes.
    holding_until: Option<u64>,
    /// This node is not the leader (a `NotLeader`/`OutcomeUnknown`, or a role
    /// watch that reported a follower): plan nothing until it is leader again.
    paused: bool,
    /// The term the planning base belongs to. A `Leader` role of another term
    /// is a regain even when no non-leader role was observed in between (the
    /// role watch keeps only the latest value): another leader may have
    /// committed entries this node never planned.
    planning_term: Option<u64>,
    closing: bool,
    expire_due: bool,
    /// WP-2.2: the KV expiry sweep is due (set by its tick, cleared once a
    /// cycle has run it).
    kv_sweep_due: bool,
    /// WP-2.3: the timer tick fired (or the last fire step hit a bound), so
    /// the next cycle runs the fire step even with no command to plan.
    timers_due: bool,
    /// WP-2.7: retention / trace expiry / garbage continuation is due.
    maintenance_due: bool,
    /// The node-local qlog sweep runs independently of serialized command
    /// planning. The flag coalesces ticks while one bounded step is active.
    qlog_gc_running: Arc<std::sync::atomic::AtomicBool>,
    stopped: bool,
    /// PERF-K trace: what the last `select!` wake was (`arrival` / `result` /
    /// `applied_notify` / `expire` / `kv_sweep` / `timers` / `hold` / `role`), for the
    /// `CYCLETRACE` "reason" field. Cheap `&'static str`, always maintained.
    wake_reason: &'static str,
    /// PERF-K trace: a monotone counter bumped on each `select!` wake, so the
    /// analysis can group the burst of cycles that ran after one wake.
    wake_seq: u64,
    /// PERF-K trace: when the previous cycle ran, for the inter-cycle gap.
    last_cycle_at: Option<Instant>,
    /// KEEP_OVERLAY ([`KeepCfg::epoch`]): moved by everything that makes the
    /// in-flight list stop being "what the planner thread planned, minus what
    /// landed" — a lost leadership (the pipeline is drained), a planned entry
    /// that is never proposed (it failed to encode), a failed cycle. The
    /// planner thread drops a state kept under an older epoch.
    plan_epoch: u64,
    /// Membership changes pausing the driver ([`QuiesceReq`]), and the one
    /// pausing it now.
    quiesce_rx: Option<mpsc::UnboundedReceiver<QuiesceReq>>,
    /// See [`Batcher::engine`].
    engine: Option<Arc<crate::rsm::consume::Engine>>,
    /// The next cycle, launched by the one being finished before it proposed
    /// (`QUEEN_RAFT_PIPELINED_PLANNING`); finished by the next `plan_cycle`.
    prefetch: Option<Launched>,
    quiescing: Option<Quiescing>,
    /// An entry landed at another index than planned: planning resumes, from
    /// the log's end, once apply reaches this index ([`RunState::check_hold`]).
    realign_at: Option<u64>,
    /// The RSM clock when this node's leadership began planning, and the
    /// monotonic instant it was read at ([`RunState::plan_wall`]).
    clock_anchor: Option<(i64, Instant)>,
}

/// The next [`QuiesceReq`], or never when no channel was handed in.
async fn recv_quiesce(rx: &mut Option<mpsc::UnboundedReceiver<QuiesceReq>>) -> Option<QuiesceReq> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// The pausing change resumed the driver (or went away).
async fn quiesce_resumed(q: &mut Option<Quiescing>) {
    match q {
        Some(q) => {
            let _ = (&mut q.resume).await;
        }
        None => std::future::pending().await,
    }
}

impl<S: Store + 'static, R: Replicator> RunState<S, R> {
    /// KEEP_OVERLAY: whatever the planner thread kept is no longer about the
    /// in-flight list the next cycle will hand it.
    fn invalidate_kept(&mut self) {
        self.plan_epoch = self.plan_epoch.wrapping_add(1);
    }

    fn start_qlog_gc(&self) {
        use std::sync::atomic::Ordering;

        // Node-local: every node of a cluster reclaims its own queue logs, the
        // followers included (the retention floor keeps what the log needs).
        if self.closing || !qlog_reclaim_from_env() {
            return;
        }
        let Some(qlogs) = self.qlog_reader.clone() else {
            return;
        };
        if self
            .qlog_gc_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let store = self.store.clone();
        let running = self.qlog_gc_running.clone();
        tokio::task::spawn_blocking(move || {
            // W1: core. The pass compacts and unlinks under a qlog WRITE lock
            // that the log writer, syncers and planner `.expect()`: it is
            // crash-safe (replay redoes it) but not unwind-safe (a panic between
            // the rename and the index swap leaves the in-memory log pointing
            // into the new file). So a panic here aborts, on this shared
            // blocking-pool thread, through a scope rather than a thread mark.
            // The flag still drops on any exit, or GC would never run again.
            struct Reset(Arc<std::sync::atomic::AtomicBool>);
            impl Drop for Reset {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::Release);
                }
            }
            let _reset = Reset(running);
            let t0 = std::time::Instant::now();
            let result = crate::obs::panic_policy::core_scope(|| {
                store.read(|r| crate::rsm::maintenance::reclaim_qlogs(r, &qlogs))
            });
            let took = t0.elapsed();
            if took >= Duration::from_millis(20) {
                tracing::info!(
                    target: "rsm",
                    ms = took.as_millis() as u64,
                    "rsm qlog retention pass"
                );
            }
            match result {
                Ok(n) if n > 0 => tracing::info!(
                    target: "rsm",
                    files = n,
                    "rsm qlog retention reclaimed sealed files"
                ),
                Ok(_) => {}
                Err(e) => tracing::warn!(
                    target: "rsm",
                    error = %e,
                    "rsm qlog retention will retry"
                ),
            }
        });
    }

    /// Entries proposed but not yet applied locally: a `Timeout` does NOT
    /// decrement it until the entry applies.
    fn unresolved(&self) -> usize {
        self.inflight
            .iter()
            .filter(|e| e.resolved.is_none())
            .count()
    }

    /// The pipeline count (I3): entries proposed and neither applied here nor
    /// known committed. A slot frees at COMMIT when the backend reports it
    /// ([`Replicator::committed_watch`]): the planner already folds every
    /// entry up to the store's applied index into its overlay, committed or
    /// not, so what a committed entry still costs here is only its place in
    /// that overlay — bounded apart by [`COMMIT_LAG_MAX`]. With apply clocking
    /// every slot (a batch of entries applied, then the next handed over) the
    /// pipeline ran at 8 entries per commit-plus-apply round (2026-09-30).
    fn uncommitted(&self) -> usize {
        self.inflight
            .iter()
            .filter(|e| e.resolved.is_none() && !e.committed && !e.timed_out)
            .count()
            + self
                .inflight
                .iter()
                .filter(|e| e.resolved.is_none() && e.timed_out)
                .count()
    }

    /// While another node is known to lead (this one follows it, or is a
    /// learner), the hint every command reaching this batcher is answered
    /// with: nothing will be planned here until this node leads again, which
    /// may be never. `None` while this node leads, or while no leader is known
    /// (an election: what is queued waits for its outcome).
    fn bounce_hint(&self) -> Option<Option<NodeId>> {
        if !self.paused {
            return None;
        }
        match *self.role_rx.borrow() {
            Role::Follower {
                leader: Some(leader),
            } => Some(Some(leader)),
            Role::Learner => Some(None),
            _ => None,
        }
    }

    /// Answer every queued command `Retry` (with the leader's hint): another
    /// node leads, and each caller retries there — the offloading facade takes
    /// it to the new leader under the same request id.
    fn bounce_queued(&mut self, hint: Option<NodeId>) {
        while let Some(sub) = self.lane.pop_front().or_else(|| self.queue.pop_front()) {
            let _ = sub.reply.send(Reply::Retry { hint });
        }
    }

    /// Queue a submission at the back of its lane: pushes in `queue`, every
    /// other command in the priority `lane` (when `drain_lane` is on). While
    /// another node leads, answer it `Retry` instead ([`Self::bounce_hint`]):
    /// a command parked here would wait for this node to lead again — its
    /// caller's whole deadline, or for ever — though the leader could plan it
    /// at once (a follower still forwarding to a node that just handed
    /// leadership away, a local command that passed the role check a moment
    /// before the step-down).
    fn enqueue(&mut self, sub: Submission) {
        if let Some(hint) = self.bounce_hint() {
            let _ = sub.reply.send(Reply::Retry { hint });
            return;
        }
        // Consumption is the engine's, served before the driver
        // (`facade::intake`). One that still reaches it (a path that submits
        // here directly) is served by the engine too, never planned: this is
        // the backstop.
        if sub.command.is_consumption() {
            if let Some(engine) = &self.engine {
                match engine.serve(&sub.command, now_micros()) {
                    crate::rsm::consume::Served::Now(reply) => {
                        let _ = sub.reply.send(reply);
                        return;
                    }
                    crate::rsm::consume::Served::Later(rx) => {
                        let reply = sub.reply;
                        tokio::spawn(async move {
                            let answer = rx.await.unwrap_or(Reply::Retry { hint: None });
                            let _ = reply.send(answer);
                        });
                        return;
                    }
                    crate::rsm::consume::Served::NotMine => {}
                }
            }
        }
        if self.cfg.drain_lane && !matches!(sub.command, Command::Push(_) | Command::MultiPush(_)) {
            self.lane.push_back(sub);
        } else {
            self.queue.push_back(sub);
        }
    }

    /// Put a submission back at the FRONT of its lane (a budget-cut
    /// deferral): it is the next of its kind to be planned.
    fn requeue_front(&mut self, sub: Submission) {
        if self.cfg.drain_lane && !matches!(sub.command, Command::Push(_) | Command::MultiPush(_)) {
            self.lane.push_front(sub);
        } else {
            self.queue.push_front(sub);
        }
    }

    /// Commands waiting in both lanes.
    fn queued(&self) -> usize {
        self.queue.len() + self.lane.len()
    }

    fn can_plan(&self) -> bool {
        !self.stopped
            && !self.paused
            && self.holding_until.is_none()
            && self.quiescing.is_none()
            && self.uncommitted() < self.cfg.pipeline
            && self.unresolved() < self.cfg.pipeline + *COMMIT_LAG_MAX
            && (self.queued() > 0
                || ((self.expire_due
                    || self.kv_sweep_due
                    || self.timers_due
                    || self.maintenance_due
                    || self.scan_due)
                    && !self.closing))
    }

    fn should_exit(&self) -> bool {
        self.stopped
            || (self.closing
                && self.queued() == 0
                && self.unresolved() == 0
                && self.holding_until.is_none())
    }

    /// Drain up to the caps (§5.1) into one batch, leaving the rest queued.
    /// PERF-J: with `push_priority` on, the drained batch is round-robined
    /// push-first so a cheap push is never budget-cut behind the batch's
    /// expensive pops.
    fn drain_batch(&mut self) -> Vec<Submission> {
        let mut batch = Vec::new();
        let mut bytes = 0usize;
        let (max_cmds, max_bytes) = (self.cfg.batch_max_cmds, self.cfg.batch_max_bytes);
        // The priority lane first (empty when `drain_lane` is off), then pushes.
        for q in [&mut self.lane, &mut self.queue] {
            while let Some(front) = q.front() {
                if batch.len() >= max_cmds {
                    break;
                }
                let hint = front.command.size_hint();
                if !batch.is_empty() && bytes + hint > max_bytes {
                    break;
                }
                bytes += hint;
                batch.push(q.pop_front().unwrap());
            }
        }
        if self.cfg.push_priority && !self.cfg.drain_lane && batch.len() > 1 {
            batch = interleave_push_first(batch);
        }
        batch
    }

    /// One cycle: drain, plan on the planner thread, propose, route answers.
    /// A cycle launched early by the one before it ([`RunState::prefetch`])
    /// is finished here instead of launching a new one.
    async fn plan_cycle(&mut self) {
        let launched = match self.prefetch.take() {
            Some(l) => Some(l),
            None => self.launch(None, true),
        };
        if let Some(l) = launched {
            self.finish(l).await;
        }
    }

    /// Whether the cycle being finished may launch the next one before its own
    /// entry is proposed ([`RunState::prefetch`]): what [`Self::can_plan`] asks,
    /// with that entry counted in flight, and commands waiting.
    fn can_prefetch(&self) -> bool {
        *PIPELINED_PLANNING
            && self.prefetch.is_none()
            && !self.stopped
            && !self.paused
            && !self.closing
            && self.holding_until.is_none()
            && self.quiescing.is_none()
            && self.uncommitted() + 1 < self.cfg.pipeline
            && self.unresolved() + 1 < self.cfg.pipeline + *COMMIT_LAG_MAX
            && self.queued() > 0
    }

    /// The first half of a cycle: drain the queue and hand the batch to the
    /// planner thread. `extra` is an entry about to be proposed at that index
    /// (the cycle launching this one): it is planned on as if in flight. With
    /// `steps` off the cycle plans commands only — the leader steps (expiry,
    /// sweep, maintenance, scanner) run in cycles that launch on their own,
    /// because the maintenance walk judges committed state alone and would
    /// plan the previous cycle's watermarks again.
    fn launch(&mut self, extra: Option<(u64, Arc<Entry>)>, steps: bool) -> Option<Launched> {
        let batch = self.drain_batch();
        let expire = steps && self.expire_due && !self.closing;
        let kv_sweep = steps && self.kv_sweep_due && !self.closing;
        // WP-2.3: the fire step runs on its tick, and in any cycle that plans a
        // timers command (a timer scheduled already due fires in its own entry).
        let fire = self.cfg.timer_tick_ms > 0
            && !self.closing
            && ((steps && self.timers_due)
                || batch
                    .iter()
                    .any(|s| matches!(s.command, Command::Timers(_))));
        let maintenance = steps && self.maintenance_due && !self.closing;
        let scanned = steps && self.scan_due && !self.closing;
        if batch.is_empty() && !expire && !kv_sweep && !fire && !maintenance && !scanned {
            return None;
        }
        // PERF-K trace: capture the before-state and the inter-cycle gap now
        // (before draining/proposing changes the counts). Gated on
        // QUEEN_RAFT_CYCLE_TRACE; the whole block is a single cached-bool branch
        // when off.
        let trace = crate::rsm::timing::cycle_trace_enabled();
        let (tr_drained, tr_unresolved0, tr_inflight0, tr_gap_us, tr_qbefore) = if trace {
            let now = Instant::now();
            let gap = self
                .last_cycle_at
                .map(|t| now.saturating_duration_since(t).as_micros() as u64)
                .unwrap_or(0);
            self.last_cycle_at = Some(now);
            (
                batch.len(),
                self.unresolved(),
                self.inflight.len(),
                gap,
                self.queued(),
            )
        } else {
            (0, 0, 0, 0, 0)
        };
        // §13.5 `batcher.drained`: commands are out of the channel and in the
        // cycle; nothing is planned. A crash here loses only unanswered work.
        if !batch.is_empty() {
            crate::rsm::faults::hit("batcher.drained");
        }

        // PERF-1 drain sizes and the arrival stamps (the unzip below drops
        // `received_at`, so capture it first). All of it — the per-cycle
        // `message_count` sum AND the `arrivals` allocation — is behind the knob,
        // so `QUEEN_RAFT_METRICS=0` pays for none of it (`arrivals` stays empty,
        // and the arrival→proposed selection below is skipped in step with it).
        // Each `received_at` is itself the gated `timing::stamp()` taken at
        // ingress, so with the knob off it is `None` and no arrival clock was
        // ever read; with it on every submission in the batch stamped `Some`
        // (the knob is a process-global constant), so `filter_map` keeps them all
        // and `arrivals` stays index-aligned with `slots`.
        let timing_on = crate::rsm::timing::enabled();
        // PERF-G: `drain_to_propose` measures from here (the moment the cycle
        // has the batch in hand) to the entry's inline submit below, isolating
        // the plan + `spawn_blocking` hop + encode from the queue wait.
        let drain_started = timing_on.then(Instant::now);
        let arrivals: Vec<Instant> = if timing_on {
            let drain_cmds = batch.len() as u64;
            if drain_cmds > 0 {
                let drain_msgs: u64 = batch.iter().map(|s| s.command.message_count()).sum();
                let tm = crate::rsm::timing::metrics();
                tm.drain_commands.record(drain_cmds);
                tm.drain_messages.record(drain_msgs);
                // PERF-G leg 1: how long each drained command sat in the queue
                // before this cycle (the pipeline-slot wait). PERF-J: measured
                // from `enqueued_at` (restamped on every defer), NOT
                // `received_at` (the arrival, now preserved across defers), so
                // this stays the PER-CYCLE slot wait while `arrival_to_proposed`
                // becomes the true whole wait.
                if let Some(now) = drain_started {
                    let qw = &crate::rsm::timing::metrics().queue_wait;
                    for s in &batch {
                        if let Some(at) = s.enqueued_at {
                            qw.record_dur(now.saturating_duration_since(at));
                        }
                    }
                }
            }
            batch.iter().filter_map(|s| s.received_at).collect()
        } else {
            Vec::new()
        };

        // PERF-J: carry each drained command's ORIGINAL arrival stamp past the
        // unzip so a budget-cut command re-queued below keeps it (never
        // restamped). Index-aligned with `slots`/`replies`; empty (and unused)
        // when the metrics knob is off, since `received_at` is then `None`.
        let received: Vec<Option<Instant>> = batch.iter().map(|s| s.received_at).collect();

        // The request id of each drained transaction, by batch position:
        // taken before the commands move.
        let txn_ids: Vec<Option<RequestId>> = batch
            .iter()
            .map(|s| match &s.command {
                Command::Transaction(c) => Some(c.request_id),
                _ => None,
            })
            .collect();

        // Split the submissions: commands go to the blocking planner, reply
        // senders stay here in the same order.
        let (commands, replies): (Vec<Command>, Vec<oneshot::Sender<Reply>>) =
            batch.into_iter().map(|s| (s.command, s.reply)).unzip();

        let folded: Vec<(u64, Arc<Entry>)> = self
            .inflight
            .iter()
            .map(|e| (e.index, e.entry.clone()))
            .chain(extra)
            .collect();

        let wall_us = self.plan_wall();
        self.front.set_clock(wall_us);
        let expire_window_us = expire.then(|| self.cfg.request_id_window_s as i64 * 1_000_000);
        let kv_sweep_limit = kv_sweep.then_some(self.cfg.kv_sweep_limit);
        let store = self.store.clone();
        let cfg = self.cfg.plan.clone();
        let front = self.front.clone();
        let reader = self.reader.clone();
        let qlog_reader = self.qlog_reader.clone();
        let fire_cfg = fire.then(|| self.cfg.timer_fire.clone());
        let maintenance_cfg = maintenance.then(|| self.cfg.maintenance.clone());
        let scan = self.scan.clone();
        let keep = KeepCfg {
            enabled: self.cfg.keep_overlay,
            epoch: self.plan_epoch,
            verify: self.cfg.keep_overlay_verify,
            reset_every: KEEP_RESET_EVERY,
        };

        let lanes_n = self.cfg.lanes;
        let epoch0 = self.plan_epoch;
        let hold0 = self.holding_until.is_some();
        let rx = self.planner_thread.run(move |state| {
            let w0 = Instant::now();
            let c0 = crate::rsm::timing::thread_cpu_ns();
            let r = if lanes_n > 1 {
                let ls = state
                    .lanes
                    .get_or_insert_with(|| Box::new(lanes::LanesState::new(lanes_n)));
                lanes::plan_cycle_lanes(
                    &store,
                    &front,
                    ls,
                    keep,
                    reader,
                    qlog_reader,
                    folded,
                    commands,
                    cfg,
                    wall_us,
                    expire_window_us,
                    kv_sweep_limit,
                    fire_cfg,
                    maintenance_cfg,
                    scan,
                )
            } else {
                plan_cycle_blocking(
                    &*store,
                    &front,
                    state,
                    keep,
                    reader,
                    qlog_reader,
                    folded,
                    commands,
                    cfg,
                    wall_us,
                    expire_window_us,
                    kv_sweep_limit,
                    fire_cfg,
                    maintenance_cfg,
                    scan,
                )
            };
            if crate::rsm::timing::enabled() {
                let tm = crate::rsm::timing::metrics();
                // The single planner records its command loop as `plan`; with
                // lanes the whole cycle is the planning (router, lanes,
                // control, merge), and it went unrecorded: the histogram read
                // zero on every lanes node.
                if lanes_n > 1 {
                    tm.plan.record_dur(w0.elapsed());
                }
                tm.plan_whole_wall.record_dur(w0.elapsed());
                tm.plan_whole_cpu
                    .record(crate::rsm::timing::thread_cpu_ns().saturating_sub(c0));
            }
            r
        });
        Some(Launched {
            rx,
            replies,
            received,
            arrivals,
            txn_ids,
            epoch0,
            hold0,
            expire,
            kv_sweep,
            fire,
            maintenance,
            scanned,
            trace,
            tr: (
                tr_drained,
                tr_unresolved0,
                tr_inflight0,
                tr_gap_us,
                tr_qbefore,
            ),
            timing_on,
            drain_started,
        })
    }

    /// The second half of a cycle: wait for the plan, then encode, route the
    /// answers and propose. Once the entry is known good it launches the next
    /// cycle before proposing it ([`Self::can_prefetch`]), so the planner
    /// thread plans while this task routes and proposes.
    async fn finish(&mut self, l: Launched) {
        let Launched {
            rx,
            replies,
            received,
            arrivals,
            txn_ids,
            epoch0,
            hold0,
            expire: _,
            kv_sweep,
            fire,
            maintenance,
            scanned: _,
            trace,
            tr,
            timing_on,
            drain_started,
        } = l;
        let (tr_drained, tr_unresolved0, tr_inflight0, tr_gap_us, tr_qbefore) = tr;
        // While the planner thread plans, keep answering the entries that
        // landed: a slot freed now is reused by the next cycle at once, rather
        // than after this one (each cycle used to wait out the planning of the
        // ones before it — measured: 12 of a 19.5 ms round trip at 370k/s).
        tokio::pin!(rx);
        let planned = loop {
            tokio::select! {
                biased;
                r = &mut rx => {
                    break r.map_err(|_| "the queen-planner thread is gone".to_string());
                }
                Some((seq, res)) = self.result_rx.recv() => {
                    self.note_wake("result");
                    self.on_result(seq, res);
                }
                _ = wait_notify(&self.applied_notify), if self.applied_notify.is_some() => {
                    self.note_wake("applied_notify");
                    self.on_applied_wake();
                }
                _ = watch_changed(&mut self.committed_rx), if self.committed_rx.is_some() => {
                    self.note_wake("committed");
                    self.on_committed();
                }
                // Keep taking arrivals while the planner thread plans: they
                // wait in the queue for the next cycle instead of blocking
                // their senders on a full channel.
                Some(sub) = self.cmd_rx.recv() => {
                    self.enqueue(sub);
                    while let Ok(more) = self.cmd_rx.try_recv() {
                        self.enqueue(more);
                    }
                }
            }
        };

        // The KV sweep and the timer fire are best-effort and self-rescheduling:
        // a cycle that could not plan leaves them to their next tick rather
        // than spinning on a store that cannot answer.
        if !matches!(planned, Ok(Ok(_))) {
            if kv_sweep {
                self.kv_sweep_due = false;
            }
            if fire {
                self.timers_due = false;
            }
            if maintenance {
                self.maintenance_due = false;
            }
            self.scan_due = false;
        }
        // A result handled during planning dropped the pipeline (a lost
        // leadership moves the epoch) or started a hold (a timed-out propose):
        // this cycle was planned on entries that may never commit, so it is
        // not proposed. Its commands are refused retryably; the SDK retries
        // with the same request ids. When leadership was lost (this driver is
        // paused) the answer is `Retry` with the leader's hint, as for every
        // command a node that does not lead cannot plan: an offloading facade
        // then takes the command to the new leader itself instead of failing
        // its caller.
        if matches!(planned, Ok(Ok(_)))
            && (self.plan_epoch != epoch0 || (!hold0 && self.holding_until.is_some()))
        {
            self.invalidate_kept();
            for id in &txn_ids {
                self.txn_not_logged(*id);
            }
            let lost = self.paused.then(|| self.role_rx.borrow().leader_hint());
            for reply in replies {
                let _ = match lost {
                    Some(hint) => reply.send(Reply::Retry { hint }),
                    None => reply.send(Reply::Refused(Refusal::retry(
                        "unavailable",
                        "the pipeline changed while this cycle planned",
                    ))),
                };
            }
            return;
        }
        let out = match planned {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => {
                // The store could not answer: refuse the whole batch retryably
                // (I14) and try again next tick. The expiry step, if it was
                // due, stays due. (The planner thread kept nothing from a cycle
                // that failed; the epoch says so too.)
                self.invalidate_kept();
                for id in &txn_ids {
                    self.txn_not_logged(*id);
                }
                for reply in replies {
                    let _ =
                        reply.send(Reply::Refused(Refusal::retry("unavailable", e.to_string())));
                }
                return;
            }
            Err(join) => {
                self.invalidate_kept();
                for id in &txn_ids {
                    self.txn_not_logged(*id);
                }
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
            // A step that found a full limit's worth runs again at once.
            self.expire_due = out.expire_more;
        }
        if out.kv_swept {
            self.kv_sweep_due = false;
        }
        if out.fired {
            // A step that hit a bound runs again at once; otherwise the next
            // tick does.
            self.timers_due = out.fire_more;
        }
        if out.maintained {
            self.maintenance_due = out.maintenance_more;
        }
        // Proposals still queued keep the cycles coming (the pipeline bound
        // still paces them); the scan tick re-arms this otherwise.
        self.scan_due = self.scan.as_ref().is_some_and(|s| s.queued() > 0);

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
            // The backend takes the planned entry (queue-log path): validate it
            // instead of encoding every payload byte on this serial task.
            Some(entry) if !self.repl.wants_bytes() => match entry.validate() {
                Ok(()) => Some(Bytes::new()),
                Err(e) => {
                    self.route_encode_failure(out.slots, replies, received, &txn_ids, &e);
                    return;
                }
            },
            Some(entry) => match encode_entry(entry) {
                Ok(bytes) => Some(Bytes::from(bytes)),
                Err(e) => {
                    self.route_encode_failure(out.slots, replies, received, &txn_ids, &e);
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
        // Launch the next cycle now, with this entry counted in flight at its
        // index: the planner thread is free, and routing and proposing this one
        // no longer idles it. Not after a cycle that deferred commands: those
        // go back to the front of the queue and must be planned before later
        // arrivals.
        if has_entry
            && !out.slots.iter().any(|s| matches!(s, Slot::Deferred(_)))
            && self.can_prefetch()
        {
            if let Some(entry) = out.entry.clone() {
                self.prefetch = self.launch(Some((index, entry)), false);
            }
        }
        // PERF-K trace: the command count that went into this cycle's entry.
        let tr_entry_cmds = if trace {
            out.entry.as_ref().map(|e| e.commands.len()).unwrap_or(0)
        } else {
            0
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

        // PERF-1: capture the arrivals of the commands that will wait on THIS
        // entry BEFORE the routing loop consumes `out.slots`; they are recorded
        // against the propose instant below. Empty unless an entry is being
        // proposed and the knob is on (`arrivals` is otherwise empty too).
        let proposed_arr = if timing_on && has_entry {
            proposed_arrivals(&out.slots, &arrivals, seq, barrier_seq)
        } else {
            Vec::new()
        };

        let mut waiters: Vec<Waiter> = Vec::new();
        let mut deferred: Vec<Submission> = Vec::new();

        for (((slot, reply), arrived), txn_id) in out
            .slots
            .into_iter()
            .zip(replies.into_iter())
            .zip(received.into_iter())
            .zip(txn_ids.into_iter())
        {
            // A transaction this cycle did not log — refused, planned empty (a
            // lost KV precondition), or answered from an earlier commit of its
            // id: the reservation of its consumption half, if any, is released.
            if matches!(slot, Slot::Immediate(_) | Slot::Empty(_)) {
                self.txn_not_logged(txn_id);
            }
            match slot {
                Slot::Immediate(r) => {
                    let _ = reply.send(r);
                }
                // PERF-J: preserve the ORIGINAL arrival (`arrived`), restamp only
                // the per-cycle `enqueued_at`, so a command deferred several
                // cycles reports its true whole wait in `arrival_to_proposed`.
                Slot::Deferred(command) => deferred.push(Submission {
                    command: *command,
                    reply,
                    received_at: arrived,
                    enqueued_at: crate::rsm::timing::stamp(),
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
            self.requeue_front(sub);
        }

        if let (Some(bytes), Some(entry)) = (encoded, out.entry) {
            // PERF-1: arrival → proposed, for the commands actually proposed in
            // THIS entry (`proposed_arrivals`) — not every drained submission,
            // which over-counted `Immediate`/`Deferred`/`InFlightHit`/older-
            // barrier reads that never went into this entry. The selection tracks
            // this entry's waiter set exactly.
            debug_assert!(
                !timing_on || proposed_arr.len() == waiters.len(),
                "arrival_to_proposed selection ({}) must match this entry's waiters ({})",
                proposed_arr.len(),
                waiters.len(),
            );
            if !proposed_arr.is_empty() {
                let now = Instant::now();
                let hist = &crate::rsm::timing::metrics().arrival_to_proposed;
                for a in &proposed_arr {
                    hist.record_dur(now.saturating_duration_since(*a));
                }
            }
            // PERF-G leg 2: this cycle's drain → its entry proposed.
            if let Some(started) = drain_started {
                crate::rsm::timing::metrics()
                    .drain_to_propose
                    .record_dur(started.elapsed());
            }
            self.propose(seq, index, entry, waiters, bytes);
        } else {
            debug_assert!(waiters.is_empty(), "no entry but waiters were attached");
        }

        // PERF-K trace: one line per cycle. The before-state was captured at
        // entry; the after-state is read now (post-propose). `reason` is the
        // wake that led here; `wake_seq` groups the burst of cycles a single
        // wake produced (the drain shape's lockstep bursts show as several
        // cycles sharing one `wake_seq`).
        if trace {
            crate::rsm::timing::cycle_trace_line(format!(
                "CYCLETRACE t={} wake={} reason={} gap_us={} drained={} entry={} \
                 cmds={} qbefore={} qafter={} unresolved={}->{} inflight={}->{}",
                crate::rsm::timing::trace_now_us(),
                self.wake_seq,
                self.wake_reason,
                tr_gap_us,
                tr_drained,
                if has_entry { 1 } else { 0 },
                tr_entry_cmds,
                tr_qbefore,
                self.queued(),
                tr_unresolved0,
                self.unresolved(),
                tr_inflight0,
                self.inflight.len(),
            ));
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
        received: Vec<Option<Instant>>,
        txn_ids: &[Option<RequestId>],
        err: &crate::rsm::effect::CodecError,
    ) {
        tracing::error!(target: "rsm", error = ?err, "batcher entry failed to encode; refusing its commands");
        // The planner thread kept this entry as in flight; it never will be.
        self.invalidate_kept();
        for (pos, ((slot, reply), arrived)) in slots
            .into_iter()
            .zip(replies.into_iter())
            .zip(received.into_iter())
            .enumerate()
        {
            // Nothing of this cycle is logged: a refused transaction's
            // reservation is released (a requeued one keeps it: it is planned
            // again as it is).
            if !matches!(slot, Slot::InFlightHit { .. } | Slot::Deferred(_)) {
                self.txn_not_logged(txn_ids.get(pos).copied().flatten());
            }
            match slot {
                // PERF-J: keep the original arrival on the requeued tail.
                Slot::Deferred(command) => self.requeue_front(Submission {
                    command: *command,
                    reply,
                    received_at: arrived,
                    enqueued_at: crate::rsm::timing::stamp(),
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

    /// Submit the `propose` for one entry, IN PLAN ORDER, and record it in
    /// flight.
    ///
    /// The submission — the part of [`Replicator::propose`] that hands the entry
    /// to the log and fixes the index it will occupy — runs INLINE here, on the
    /// single driver task, in the same order the cycle stamped `now_us`. Only
    /// the wait for local apply is spawned, so up to `pipeline` entries are
    /// still in flight at once (D4). This is the fix for WP-1.11 F-1: spawning
    /// the whole `propose` per entry let the [`LocalReplicator`] writer assign
    /// the log index in the order the spawned tasks happened to reach its
    /// channel — which tokio does not order by spawn order — so two entries
    /// stamped `now_a < now_b` in plan order could land as index `n = b`,
    /// `n+1 = a`; apply then saw `now_us` go backwards with the index and
    /// refused (I5, `apply::gates` → `TimeWentBackwards`), poisoning the node.
    /// Driving the submission from the one driver task, in plan order, makes the
    /// log index follow the stamp order, so I5 holds under the pipeline.
    ///
    /// This relies on the [`Replicator::propose`] submission-ordering contract:
    /// the backend assigns log indexes in the order the driver first-polls the
    /// proposes. Driving each first poll INLINE here, in plan order, therefore
    /// pins the index order to plan order. ([`LocalReplicator`] meets that
    /// contract by performing the submission in the future's first-poll
    /// synchronous prefix, before its first suspension.) The no-op waker here
    /// only advances the future to that first suspension; the spawned task
    /// re-polls with a real waker, which oneshot/timer re-register, so no wake
    /// is lost.
    fn propose(
        &mut self,
        seq: u64,
        index: u64,
        entry: Arc<Entry>,
        waiters: Vec<Waiter>,
        bytes: Bytes,
    ) {
        let planned = entry.clone();
        let term = self.planning_term.unwrap_or(0);
        self.inflight.push_back(InFlightEntry {
            seq,
            index,
            term,
            entry,
            waiters,
            resolved: None,
            timed_out: false,
            committed: false,
            proposed_at: crate::rsm::timing::stamp(),
        });
        // §13.5 `propose.sent`: the entry is about to reach the replicator; the
        // client is still waiting and the outcome is UNKNOWN (D6, I6). A crash
        // here has put nothing in the log, so the write is unanswered and
        // at-most-once: the retry with the same request id finds nothing
        // committed and plans anew (§5.4).
        crate::rsm::faults::hit("propose.sent");

        let repl = self.repl.clone();
        let deadline = Instant::now() + Duration::from_millis(self.cfg.propose_ms);
        // Own the replicator inside the future so it is `'static`; `propose`
        // borrows `&self` only for the length of the call.
        let mut fut = Box::pin(async move { repl.propose_entry(bytes, planned, deadline).await });

        // Drive the future to its first suspension INLINE, so the log submission
        // happens now, in plan order, on this one task. The no-op waker never
        // needs to fire: the spawned task below re-polls with a real one.
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(res) => {
                // Resolved on the first poll: a poisoned or refused backend, or
                // a deadline already past. Route it like any other result; the
                // entry was still submitted (I3), so `on_result` handles it.
                let _ = self.result_tx.send((seq, res));
            }
            Poll::Pending => {
                // Submitted and now awaiting local apply: finish off the driver
                // task so the next entry can be planned and submitted (D4).
                let result_tx = self.result_tx.clone();
                // W1: core — a completion that dies unsent leaves the entry
                // unresolved and the pipeline stuck behind it.
                tokio::spawn(crate::obs::panic_policy::core(async move {
                    let res = fut.await;
                    let _ = result_tx.send((seq, res));
                }));
            }
        }
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
                    // The overlay fold, the drop gate and the applied-index
                    // wake all use the index this driver predicted for the
                    // entry. A backend that put it anywhere else (openraft
                    // appending an entry of its own while this node leads — a
                    // membership change pauses the driver first, QuiesceReq) makes
                    // them wrong: never a fix-up of the pipeline, but a restart
                    // of it from the log's end once apply has caught up, the
                    // reset a leadership regain does (it used to stop the driver
                    // for good: every write answered 500 until leadership moved).
                    if e.index != at.index {
                        tracing::error!(
                            target: "rsm",
                            predicted = e.index,
                            actual = at.index,
                            "an entry landed at another index than planned; the pipeline restarts from the log's end",
                        );
                        self.lose_leadership(None);
                        self.realign_at = Some(self.repl.metrics().last_log_index);
                        return;
                    }
                    // PERF-G: the applied-index wake may already have resolved
                    // this entry (driver-notify). The forwarding task's late Ok
                    // is then a no-op.
                    if e.resolved.is_some() {
                        return;
                    }
                    if let Some(at0) = e.proposed_at {
                        crate::rsm::timing::metrics()
                            .propose_roundtrip
                            .record_dur(at0.elapsed());
                    }
                    // The engine first: an answered caller's next command
                    // finds the transaction's rows live.
                    txns_landed(&self.engine, &e.entry);
                    e.resolve_ok(at);
                }
            }
            Err(ProposeError::Timeout) => {
                // I3: keep the entry in flight, answer its waiters Retry, and
                // hold the pipeline until it applies or the role changes.
                if let Some(e) = self.inflight.iter_mut().find(|e| e.seq == seq) {
                    // Already resolved by the applied-index wake: an entry that
                    // committed cannot also have genuinely timed out, so ignore
                    // a stale Timeout (PERF-G).
                    if e.resolved.is_some() {
                        return;
                    }
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
        // The consumption engine stops serving with the pipeline: whatever
        // it held answers `Retry`, and what it reserved or leased is loaded
        // again from committed state if this node plans again.
        if let Some(e) = &self.engine {
            e.on_step_down();
        }
        for mut e in self.inflight.drain(..) {
            e.fail(hint);
        }
        self.holding_until = None;
        self.paused = true;
        // A pausing membership change fails with this leadership anyway.
        self.quiescing = None;
        // The pipeline is gone, and with it every entry the kept overlay holds.
        self.invalidate_kept();
    }

    /// The role watch changed.
    fn on_role(&mut self, role: Role) {
        match role {
            Role::Leader { term } => {
                if !self.paused && self.planning_term != Some(term) {
                    // Leadership was lost and regained between two looks at
                    // the role: everything in flight belongs to the old term.
                    self.lose_leadership(None);
                }
                if self.paused {
                    self.realign_at = None;
                    self.planning_term = Some(term);
                    self.begin_term_clock();
                    // I13: a new leader plans only after applying the first
                    // entry of its own term. On a single node with the
                    // LocalReplicator the term never changes, so this is the
                    // reset of the planning base after a regain; the openraft
                    // adapter (phase 3) waits on the term's first entry.
                    self.paused = false;
                    self.next_index = self.repl.metrics().last_log_index + 1;
                    // Leadership regain: a prior leader may have committed
                    // appends this front never planned, so drop it — every
                    // partition re-seeds from the committed snapshot this leader
                    // is guaranteed to hold complete. A single-node
                    // LocalReplicator never regains, so this is inert in phase 1.
                    self.front.reset();
                    // The consumption engine serves from now on, in this term:
                    // it starts from the committed cursor rows (their leases
                    // live on) after its failover pause.
                    if let Some(e) = &self.engine {
                        e.on_leader(term, self.next_index - 1);
                    }
                    // KEEP_OVERLAY: and plan from state rebuilt under the new term.
                    self.invalidate_kept();
                }
            }
            Role::Stopped => {
                self.stopped = true;
                self.invalidate_kept();
                if let Some(e) = &self.engine {
                    e.on_step_down();
                }
            }
            _ => {
                // Follower / Learner / Candidate: drop the overlay, fail
                // waiters, and wait for leadership.
                self.lose_leadership(role.leader_hint());
                // Another node leads: what is queued here would wait for this
                // node to lead again. Answer it now; its caller retries on the
                // leader (an election in progress keeps the queue: this node
                // may win it).
                if let Some(hint) = self.bounce_hint() {
                    self.bounce_queued(hint);
                }
            }
        }
    }

    /// An entry landed off its predicted index ([`RunState::on_result`]): plan
    /// again, from the log's end, once apply has caught up with everything
    /// logged — still leading, in the same term (a role change takes over
    /// otherwise).
    fn check_realign(&mut self) {
        let Some(last) = self.realign_at else {
            return;
        };
        if self.repl.applied_index() < last {
            return;
        }
        self.realign_at = None;
        let role = *self.role_rx.borrow();
        if let Role::Leader { term } = role {
            if self.paused && self.planning_term == Some(term) {
                self.paused = false;
                self.next_index = self.repl.metrics().last_log_index + 1;
                self.front.reset();
                self.invalidate_kept();
                if let Some(e) = &self.engine {
                    e.on_leader(term, self.next_index - 1);
                }
            }
        }
    }

    /// The RSM clock now: this node's wall clock, never behind the last
    /// applied entry's (D5).
    fn rsm_now(&self) -> i64 {
        let last = self.store.read(|r| r.last_now_us()).unwrap_or(0);
        now_micros().max(last)
    }

    /// A term begins planning: the RSM clock is read once, here, from the
    /// wall ([`RunState::rsm_now`]: never behind the last applied entry) —
    /// the clock's anchor.
    fn begin_term_clock(&mut self) {
        let now = self.rsm_now();
        self.clock_anchor = Some((now, Instant::now()));
    }

    /// The wall clock a cycle plans from ([`paced_wall`]).
    fn plan_wall(&mut self) -> i64 {
        let wall = now_micros();
        if !self.cfg.monotonic_clock {
            return wall;
        }
        paced_wall(self.clock_anchor, wall, Instant::now())
    }

    /// A membership change asks to pause ([`QuiesceReq`]).
    fn on_quiesce(&mut self, req: QuiesceReq) {
        self.quiescing = Some(Quiescing {
            drained: Some(req.drained),
            resume: req.resume,
        });
        self.check_quiesced();
    }

    /// While pausing: tell the change once nothing is in flight.
    fn check_quiesced(&mut self) {
        let applied = self.repl.applied_index();
        let Some(q) = self.quiescing.as_mut() else {
            return;
        };
        if q.drained.is_none() {
            return;
        }
        self.drop_landed(applied);
        if self.inflight.is_empty() && self.holding_until.is_none() {
            if let Some(q) = self.quiescing.as_mut() {
                if let Some(tx) = q.drained.take() {
                    let _ = tx.send(());
                }
            }
        }
    }

    /// The change is done (or gave up): openraft's config entries sit between
    /// the last planned entry and the next, so plan from the log's end, with
    /// the reset a leadership regain does.
    fn on_resume(&mut self) {
        self.quiescing = None;
        if !self.paused {
            self.next_index = self.repl.metrics().last_log_index + 1;
            self.front.reset();
            self.invalidate_kept();
        }
    }

    /// Poll the applied index while holding on a timeout (I3).
    fn check_hold(&mut self) {
        self.check_realign();
        let Some(until) = self.holding_until else {
            return;
        };
        if self.repl.applied_index() >= until {
            // The held entries applied locally; they leave the pipeline. Mark
            // every timed-out entry at or below the applied index resolved so
            // it no longer counts, and resume planning.
            let applied = self.repl.applied_index();
            let mut superseded = false;
            for e in self.inflight.iter_mut() {
                if e.timed_out && e.resolved.is_none() && e.index <= applied {
                    if self.repl.applied_term_at(e.index) != Some(e.term) {
                        // Another term's entry applied at its index: it never
                        // committed, and neither did anything planned after it.
                        superseded = true;
                        break;
                    }
                    e.resolved = Some(AppliedAt {
                        index: e.index,
                        term: e.term,
                    });
                    txns_landed(&self.engine, &e.entry);
                }
            }
            if superseded {
                let hint = self.role_rx.borrow().leader_hint();
                self.lose_leadership(hint);
                return;
            }
            self.holding_until = None;
        }
    }

    /// PERF-K trace: record which `select!` arm woke the driver, and bump the
    /// per-wake sequence so the analysis can group the cycles that ran after it.
    /// Background retention walks only while this node plans as leader.
    fn sync_scan(&self) {
        if let Some(scan) = &self.scan {
            scan.set_leading(!self.paused && !self.stopped && !self.closing);
        }
    }

    fn note_wake(&mut self, reason: &'static str) {
        self.wake_reason = reason;
        self.wake_seq = self.wake_seq.wrapping_add(1);
    }

    /// PERF-G: the applied index advanced (`QUEEN_RAFT_DRIVER_NOTIFY`). Resolve
    /// every in-flight entry the apply has now passed, so the freed pipeline
    /// slot is reused on this one wake instead of each entry's forwarding task.
    fn on_applied_wake(&mut self) {
        let applied = self.repl.applied_index();
        self.resolve_applied(applied);
    }

    /// The replicator's committed log id moved to `(c, t)`: every in-flight
    /// entry at or below `c` that this node proposed in term `t` is committed
    /// (see [`Replicator::committed_watch`] for why only term `t`'s), so it
    /// leaves the pipeline and its outcome-only waiters are answered. The rest
    /// of its lifecycle — the overlay, the waiters that need local rows, a
    /// transaction's landing in the engine, the term check — stays at apply.
    fn on_committed(&mut self) {
        let Some(rx) = self.committed_rx.as_mut() else {
            return;
        };
        let (c, t) = *rx.borrow_and_update();
        let engine = self.engine.clone();
        let at_commit = |id: &RequestId| engine.as_ref().is_some_and(|en| en.answers_at_commit(id));
        for e in self.inflight.iter_mut() {
            if e.index > c {
                break;
            }
            if e.resolved.is_some() || e.committed || e.timed_out || e.term != t {
                continue;
            }
            e.committed = true;
            e.answer_committed(
                AppliedAt {
                    index: e.index,
                    term: e.term,
                },
                &at_commit,
            );
        }
    }

    /// Resolve, in index order, every in-flight entry whose index the applied
    /// index has passed. D7/I4 hold: `applied_index` only advances past an
    /// entry AFTER it committed and applied locally, so answering here is
    /// answering after local apply. Idempotent with the per-propose result
    /// path (both guard on `resolved`): whichever reaches an entry first
    /// resolves it, the other is a no-op.
    fn resolve_applied(&mut self, applied: u64) {
        let mut superseded = false;
        for e in self.inflight.iter_mut() {
            // In-flight entries are pushed in index order and never reordered,
            // so the first one above the applied index bounds the rest.
            if e.index > applied {
                break;
            }
            if e.resolved.is_some() {
                continue;
            }
            // The index passing proves nothing by itself: after a term change
            // it holds the new leader's entry, and this one never committed
            // (Jepsen W3c pause: a stale leader answered its claims `Done`
            // off the new leader's noop). Unknown: the propose result decides.
            let term = e.term;
            match self.repl.applied_term_at(e.index) {
                Some(t) if t == term => {}
                Some(_) => {
                    superseded = true;
                    break;
                }
                None => break,
            }
            // The engine first: an answered caller's next command finds the
            // transaction's rows live.
            txns_landed(&self.engine, &e.entry);
            if e.timed_out {
                // Its waiters were already answered `Retry` (I3); mark it
                // resolved so it leaves the pipeline (mirrors `check_hold`).
                e.resolved = Some(AppliedAt {
                    index: e.index,
                    term,
                });
            } else {
                if let Some(at0) = e.proposed_at {
                    crate::rsm::timing::metrics()
                        .propose_roundtrip
                        .record_dur(at0.elapsed());
                }
                e.resolve_ok(AppliedAt {
                    index: e.index,
                    term,
                });
            }
        }
        if superseded {
            // Every waiter `Retry`: the facade takes each command, under its
            // request id, to the node that leads now.
            let hint = self.role_rx.borrow().leader_hint();
            self.lose_leadership(hint);
            return;
        }
        // Release a pipeline hold whose held entries have now applied (mirrors
        // `check_hold`, so a timeout followed by an applied-index wake still
        // resumes planning).
        if let Some(until) = self.holding_until {
            if applied >= until {
                self.holding_until = None;
            }
        }
    }

    /// A transaction this driver did not log (refused, planned empty, its
    /// cycle dropped): the engine releases the reservation of its consumption
    /// half — unless an earlier attempt of the same id is still in flight,
    /// whose landing decides ([`txns_landed`]).
    fn txn_not_logged(&self, id: Option<RequestId>) {
        let (Some(engine), Some(id)) = (&self.engine, id) else {
            return;
        };
        if self
            .inflight
            .iter()
            .any(|e| e.entry.commands.iter().any(|c| c.request_id == id))
        {
            return;
        }
        engine.txn_resolve(id, false);
    }

    /// Fail every remaining waiter (`Retry`) — the driver is exiting.
    fn fail_all(&mut self, hint: Option<NodeId>) {
        for mut e in self.inflight.drain(..) {
            e.fail(hint);
        }
        while let Some(sub) = self.lane.pop_front().or_else(|| self.queue.pop_front()) {
            let _ = sub.reply.send(Reply::Retry { hint });
        }
    }
}

/// Every transaction of `entry`, which applied on this leader: the engine
/// makes the rows its consumption half reserved the live state
/// ([`crate::rsm::consume::Engine::txn_resolve`]; ids it never reserved are
/// ignored).
fn txns_landed(engine: &Option<Arc<crate::rsm::consume::Engine>>, entry: &Entry) {
    let Some(engine) = engine else {
        return;
    };
    for c in &entry.commands {
        if matches!(&c.outcome, Outcome::Placeholder(p) if p.tag() == crate::rsm::planner::txn::TXN_OUTCOME_TAG)
        {
            engine.txn_resolve(c.request_id, true);
        }
    }
}

/// Await the applied-index wake, when one is present (PERF-G). The guard on the
/// select arm ensures `n` is `Some` before this is polled; the `unwrap` runs
/// only under that guard. When absent the arm is disabled, so this never awaits
/// `None`.
async fn wait_notify(n: &Option<Arc<Notify>>) {
    match n {
        Some(n) => n.notified().await,
        // Unreachable under the select guard; park forever so a stray poll is
        // inert rather than a busy spin.
        None => std::future::pending::<()>().await,
    }
}

/// The clock a leader's cycle plans from: the RSM clock where its term
/// began, moved on by the MONOTONIC time since (`wall` before any term).
///
/// The planner stamps each entry above this and the RSM clock's high-water
/// mark (D5: never back). Following the wall clock every cycle, a step of it —
/// NTP, a paused VM, Jepsen's clock nemesis — moved the RSM clock with it:
///  - forward, and every lease ended early by the size of the step (P9 W2: the
///    leader's clock leapt ~300 s and a second worker got a message 10 ms
///    after the first);
///  - back, and the stamp stood still at its high-water mark, every lease,
///    delay and TTL with it, until the wall caught up; while it stood ahead of
///    every node's wall clock, their pops' deadlines (wall + timeout) had all
///    passed (P9 W5, W2: a 150-170 s excursion answered every pop empty for
///    minutes).
///
/// On the monotonic clock neither happens: a lease lasts its real duration,
/// and the RSM clock stays with real time through a leader's excursions. The
/// wall is read once per term ([`RunState::begin_term_clock`]); a step of it
/// reaches the RSM clock at the next term, as a jump the lease grace covers up
/// to `QUEEN_RAFT_MAX_CLOCK_SKEW_MS`. Between steps, the wall and the
/// monotonic clock advance at the same NTP-disciplined rate.
fn paced_wall(anchor: Option<(i64, Instant)>, wall: i64, now: Instant) -> i64 {
    match anchor {
        Some((at_us, at)) => {
            at_us.saturating_add(now.saturating_duration_since(at).as_micros() as i64)
        }
        None => wall,
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

/// PERF-J (`QUEEN_RAFT_PUSH_PRIORITY`): reorder a drained batch push-first so a
/// cheap push (~4 µs plan) is not budget-cut behind the batch's expensive pops
/// (~0.5 ms each). Round-robin, starting with a push: `[push, other, push,
/// other, …]` then the longer lane's tail. Both lanes keep their relative order
/// (`Vec::into_iter().partition` is stable and each lane is consumed in order),
/// so per-partition push order is preserved AND no lane can be starved — every
/// non-push still lands on an early (pre-cut) slot. Callers gate on the knob and
/// on `batch.len() > 1`, so a single-command batch is never reordered.
fn interleave_push_first(batch: Vec<Submission>) -> Vec<Submission> {
    let n = batch.len();
    let (pushes, others): (Vec<Submission>, Vec<Submission>) = batch
        .into_iter()
        .partition(|s| matches!(s.command, Command::Push(_) | Command::MultiPush(_)));
    // All one kind: nothing to interleave, and the original order is already
    // right (the partition preserved it).
    if pushes.is_empty() || others.is_empty() {
        let mut out = pushes;
        out.extend(others);
        return out;
    }
    let mut out = Vec::with_capacity(n);
    let mut pi = pushes.into_iter();
    let mut oi = others.into_iter();
    loop {
        match (pi.next(), oi.next()) {
            (Some(p), Some(o)) => {
                out.push(p);
                out.push(o);
            }
            (Some(p), None) => {
                out.push(p);
                out.extend(pi);
                break;
            }
            (None, Some(o)) => {
                out.push(o);
                out.extend(oi);
                break;
            }
            (None, None) => break,
        }
    }
    out
}

#[cfg(test)]
mod perf1_arrival_tests {
    //! PERF-1 refutation: `arrival_to_proposed` must price only the commands
    //! actually proposed in the new entry, not every drained submission. This
    //! pins [`Slot::waits_on_new_entry`] / [`proposed_arrivals`], the selection
    //! the leader cycle uses, over a mixed batch. It reads no global metric
    //! registry (that singleton is shared across the whole parallel test binary,
    //! so an exact count on it is not observable), it exercises the selection
    //! directly. Reverting the selection to "every drained submission" makes the
    //! `assert_eq!`s below read 5 instead of 3 / 2, so the test fails.
    use super::{proposed_arrivals, Command, Outcome, Reply, Slot, Submission};
    use crate::rsm::planner::RenewCommand;
    use std::time::Instant;

    #[test]
    fn received_at_follows_the_metrics_knob() {
        // PERF-1 refutation: the arrival stamp `Submission.received_at` is the
        // highest-frequency timing read (one per COMMAND, at facade ingress), so
        // it must be gated by the SAME knob every other timing read is — taken
        // through `timing::stamp()`, `None` when `QUEEN_RAFT_METRICS=0`. Before
        // the fix it was a bare `Instant::now()` in `Submission::new`, an
        // ungated clock read the off path still paid, so the VM ablation could
        // not price it. The stamp is present exactly when the instrumentation is
        // on. (Reverting the fix to a bare `Instant` breaks this at compile.)
        let cmd = Command::Renew(RenewCommand {
            request_id: [7u8; 16],
            tenant: None,
            worker: "w".into(),
            seconds: 30,
        });
        let (sub, _rx) = Submission::new(cmd);
        assert_eq!(
            sub.received_at.is_some(),
            crate::rsm::timing::enabled(),
            "the arrival stamp must follow the QUEEN_RAFT_METRICS knob, \
             like every other hot-path clock read",
        );
    }

    fn mixed_batch() -> Vec<Slot> {
        // One logged command, one same-cycle retry of it (both wait on the new
        // entry), one duplicate answered at once, one hit on an OLDER in-flight
        // entry, and one empty read (its disposition depends on the barrier).
        vec![
            Slot::Logged([1u8; 16]),
            Slot::SameCycle([1u8; 16]),
            Slot::Immediate(Reply::Retry { hint: None }),
            Slot::InFlightHit {
                request_id: [2u8; 16],
                outcome: Outcome::Empty,
            },
            Slot::Empty(Outcome::Empty),
        ]
    }

    #[test]
    fn arrival_to_proposed_prices_only_the_proposed_commands() {
        let slots = mixed_batch();
        let now = Instant::now();
        let arrivals = vec![now; slots.len()];
        let seq = 9u64;

        // The empty read barriered on THIS entry (barrier_seq == seq): Logged,
        // SameCycle and Empty are the three that wait on it — not the Immediate
        // answer nor the older-entry InFlightHit.
        let on_this = proposed_arrivals(&slots, &arrivals, seq, Some(seq));
        assert_eq!(
            on_this.len(),
            3,
            "only the commands proposed in THIS entry are priced (not all 5 drained)",
        );

        // The empty read barriered on an OLDER in-flight entry: it is answered by
        // that entry's commit, so it is NOT this entry's arrival→proposed sample.
        let on_older = proposed_arrivals(&slots, &arrivals, seq, Some(seq - 1));
        assert_eq!(
            on_older.len(),
            2,
            "an empty read on an older barrier is not priced against this entry",
        );
    }

    #[test]
    fn waits_on_new_entry_classifies_every_slot() {
        let seq = 4u64;
        assert!(Slot::Logged([0u8; 16]).waits_on_new_entry(seq, Some(seq)));
        assert!(Slot::SameCycle([0u8; 16]).waits_on_new_entry(seq, Some(seq)));
        assert!(Slot::Empty(Outcome::Empty).waits_on_new_entry(seq, Some(seq)));
        // Not proposed in this entry:
        assert!(!Slot::Empty(Outcome::Empty).waits_on_new_entry(seq, Some(seq - 1)));
        assert!(!Slot::Immediate(Reply::Retry { hint: None }).waits_on_new_entry(seq, Some(seq)));
        assert!(!Slot::InFlightHit {
            request_id: [0u8; 16],
            outcome: Outcome::Empty,
        }
        .waits_on_new_entry(seq, Some(seq)));
    }
}

#[cfg(test)]
mod perf_j_tests {
    //! PERF-J: the push-priority drain reorder ([`interleave_push_first`]) and the
    //! split arrival/enqueue stamps on [`Submission`]. Both are new this round, so
    //! these fail to build against the pre-fix tree.
    use super::{interleave_push_first, Command, Submission};
    use crate::rsm::effect::QueueConfig;
    use crate::rsm::entry::RequestId;
    use crate::rsm::planner::{PushCommand, RenewCommand};

    fn rid(n: u8) -> RequestId {
        let mut id = [0u8; 16];
        id[0] = n;
        id
    }

    fn qc() -> QueueConfig {
        QueueConfig {
            id: [0u8; 16],
            namespace: None,
            task: None,
            priority: 0,
            lease_time: 30,
            retry_limit: 3,
            retry_delay: 0,
            ttl: 0,
            dead_letter_queue: true,
            dlq_after_max_retries: true,
            delayed_processing: 0,
            window_buffer: 0,
            retention_seconds: 0,
            completed_retention_seconds: 0,
            retention_enabled: false,
            encryption_enabled: false,
            max_wait_time_seconds: 0,
            max_queue_size: 0,
            min_pop_wait_time: 0,
            dedup_window_seconds: 0,
            retention_sink_hold: String::new(),
            retention_sink_hold_max_seconds: 0,
            created_at_us: 0,
        }
    }

    /// A push submission tagged by `id`, to partition `part`.
    fn push_sub(id: u8, part: &str) -> Submission {
        let cmd = Command::Push(PushCommand {
            request_id: rid(id),
            tenant: "t".into(),
            queue: "q".into(),
            partition: part.into(),
            items: Vec::new(),
            create_cfg: qc(),
        });
        Submission::new(cmd).0
    }

    /// A non-push (renew) submission tagged by `id`.
    fn other_sub(id: u8) -> Submission {
        let cmd = Command::Renew(RenewCommand {
            request_id: rid(id),
            tenant: None,
            worker: "w".into(),
            seconds: 30,
        });
        Submission::new(cmd).0
    }

    fn ids(batch: &[Submission]) -> Vec<u8> {
        batch.iter().map(|s| s.command.request_id()[0]).collect()
    }

    fn is_push(s: &Submission) -> bool {
        matches!(s.command, Command::Push(_))
    }

    #[test]
    fn push_priority_round_robins_push_first_and_keeps_push_order() {
        // A mixed batch as FIFO arrival order: other, push(p0), other, push(p0),
        // push(p1), other. Pre-fix (FIFO) would plan the leading `other`(10)
        // BEFORE the first push, so a budget cut on the expensive pops could defer
        // the pushes. PERF-J round-robins push-first.
        let batch = vec![
            other_sub(10),
            push_sub(1, "p0"),
            other_sub(11),
            push_sub(2, "p0"),
            push_sub(3, "p1"),
            other_sub(12),
        ];
        let out = interleave_push_first(batch);
        // pushes [1,2,3] (stable) interleaved with others [10,11,12] (stable),
        // starting with a push: [1,10,2,11,3,12].
        assert_eq!(ids(&out), vec![1, 10, 2, 11, 3, 12]);
        // The first planned command is a push (never head-of-line-blocked).
        assert!(is_push(&out[0]));
        // Per-partition push order preserved: 1 (p0) before 2 (p0) before 3 (p1).
        let push_ids: Vec<u8> = out
            .iter()
            .filter(|s| is_push(s))
            .map(|s| s.command.request_id()[0])
            .collect();
        assert_eq!(push_ids, vec![1, 2, 3], "push order must be preserved");
        // No lane starves: every `other` is present, none dropped.
        let other_ids: Vec<u8> = out
            .iter()
            .filter(|s| !is_push(s))
            .map(|s| s.command.request_id()[0])
            .collect();
        assert_eq!(other_ids, vec![10, 11, 12]);
    }

    #[test]
    fn push_priority_lifts_the_only_push_ahead_of_many_others() {
        // One push behind three others: the push must not wait behind them.
        let batch = vec![
            other_sub(10),
            other_sub(11),
            other_sub(12),
            push_sub(1, "p0"),
        ];
        let out = interleave_push_first(batch);
        assert_eq!(ids(&out), vec![1, 10, 11, 12]);
    }

    #[test]
    fn push_priority_never_starves_the_only_other() {
        // Many pushes, one other: the other still lands on an early (pre-cut)
        // slot rather than behind every push.
        let batch = vec![
            push_sub(1, "p0"),
            push_sub(2, "p0"),
            push_sub(3, "p0"),
            other_sub(10),
        ];
        let out = interleave_push_first(batch);
        assert_eq!(ids(&out), vec![1, 10, 2, 3]);
    }

    #[test]
    fn push_priority_is_a_noop_on_a_single_kind() {
        let all_push = vec![push_sub(1, "p0"), push_sub(2, "p1"), push_sub(3, "p0")];
        assert_eq!(ids(&interleave_push_first(all_push)), vec![1, 2, 3]);
        let all_other = vec![other_sub(10), other_sub(11)];
        assert_eq!(ids(&interleave_push_first(all_other)), vec![10, 11]);
    }

    #[test]
    fn submission_new_sets_both_the_arrival_and_the_enqueue_stamp() {
        // PERF-J: `received_at` (arrival, never restamped) and `enqueued_at`
        // (per-cycle) are both set at ingress, both following the metrics knob.
        let (sub, _rx) = Submission::new(Command::Renew(RenewCommand {
            request_id: rid(7),
            tenant: None,
            worker: "w".into(),
            seconds: 30,
        }));
        let on = crate::rsm::timing::enabled();
        assert_eq!(sub.received_at.is_some(), on);
        assert_eq!(sub.enqueued_at.is_some(), on);
        // They are the SAME instant at ingress (one stamp read).
        assert_eq!(sub.received_at, sub.enqueued_at);
    }
}

#[cfg(test)]
mod clock_tests {
    use std::time::{Duration, Instant};

    use super::paced_wall;

    const S: i64 = 1_000_000;

    #[test]
    fn before_any_term_it_is_the_wall() {
        assert_eq!(paced_wall(None, 42 * S, Instant::now()), 42 * S);
    }

    /// P9 W2 / W5 under the clock nemesis: the leader's wall clock leapt
    /// forward and back; the RSM clock went with it, ending leases early and
    /// then standing still. It moves on real time only.
    #[test]
    fn within_a_term_it_moves_on_real_time_whatever_the_wall_does() {
        let t0 = Instant::now();
        let anchor = Some((1_000 * S, t0));
        let t1 = t0 + Duration::from_secs(10);
        assert_eq!(
            paced_wall(anchor, 1_010 * S, t1),
            1_010 * S,
            "in step with the wall"
        );
        assert_eq!(
            paced_wall(anchor, 1_310 * S, t1),
            1_010 * S,
            "a 300 s leap: ignored"
        );
        assert_eq!(
            paced_wall(anchor, 840 * S, t1),
            1_010 * S,
            "a 170 s drop: ignored"
        );
        let t2 = t0 + Duration::from_secs(13);
        assert_eq!(
            paced_wall(anchor, 0, t2),
            1_013 * S,
            "3 s of real time is 3 s"
        );
    }
}
