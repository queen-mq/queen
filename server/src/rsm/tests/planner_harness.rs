//! The tiny apply loop the planner tests run against (WP-1.5).
//!
//! A cell is one raft1 node: a real store and real segment files (WP-1.2/1.3),
//! the real applier (WP-1.4), and the real planner. Each [`Cell::run`] is one
//! planning cycle — plan a batch of commands over the committed view plus an
//! overlay, build the ONE entry their [`Plan::Logged`] effects make, apply it
//! and take a durable point — so the committed state a later cycle reads is
//! exactly the state apply produced from the effects the planner emitted. That
//! round trip is what proves the planner and apply agree (I1 holds by
//! construction: the planner writes nothing, apply writes everything).
//!
//! The applier is re-opened per apply on purpose: `Applier::open` runs §11.5
//! recovery over the committed state the planner reads to plan the next cycle.
//! It also sidesteps a self-referential borrow of the store.
//!
//! No consumption is planned (the consumption engine serves it): a test moves
//! a group's cursor the way the engine's checkpoints do, with an effect
//! command ([`cursor_set`]).

#![allow(dead_code)]

use std::sync::Arc;

use crate::rsm::apply::{Applier, Committed as ApplyCommitted, NoNotify, StateDigest};
use crate::rsm::dedup::DedupFront;
use crate::rsm::effect::{CursorRow, Effect, GarbageScope, Pid, QueueConfig};
use crate::rsm::entry::{Entry, Outcome, RequestId};
use crate::rsm::planner::timers::{FireReport, TimerFireConfig, TimersCommand};
use crate::rsm::planner::txn::TxnCommand;
use crate::rsm::planner::{
    EffectsCommand, KvCommand, Overlay, Plan, PlanConfig, Planned, Planner, PushCommand, PushItem,
};
use crate::rsm::state::Committed;
use crate::rsm::store::keys::Counter;
use crate::rsm::store::rows::{DlqRow, PartitionRow};
use crate::rsm::store::{Reads, Store, TypedReads};

use super::apply::{cfg, seg_opts, Node};

pub const TENANT: &str = "t1";
pub const BASE_US: i64 = 1_800_000_000_000_000;

/// A request id from a small counter, distinct per command.
pub fn rid(n: u64) -> RequestId {
    let mut id = [0u8; 16];
    id[0..8].copy_from_slice(&n.to_be_bytes());
    id[8] = 0x5A;
    id
}

/// A queue config the fixtures start from. Fields the test cares about are set
/// through the builder methods below.
pub fn qcfg() -> QueueConfig {
    QueueConfig {
        id: {
            let mut u = [0u8; 16];
            u[0] = 0xC0;
            u[15] = 7;
            u
        },
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
        created_at_us: BASE_US,
    }
}

/// One command the cell can plan, tagged with which entry point to use.
#[derive(Clone, Debug)]
pub enum Cmd {
    Push(PushCommand),
    /// Deterministic effects: a catalog change, or cursor rows as the
    /// consumption engine's checkpoints write them.
    Effects(EffectsCommand),
    /// A KV call (WP-2.2).
    Kv(KvCommand),
    /// One leader KV expiry step, planned exactly as the batcher plans it (a
    /// command of its own, `Outcome::Empty`, logged only when it deletes).
    KvSweep {
        id: u64,
        limit: usize,
    },
    /// WP-2.3: a timers call (schedule / cancel).
    Timers(TimersCommand),
    /// A transaction: pushes, KV, timers, positions on new partitions, and
    /// the consumption engine's riders.
    Txn(TxnCommand),
}

impl Cmd {
    fn request_id(&self) -> RequestId {
        match self {
            Cmd::Push(c) => c.request_id,
            Cmd::Effects(c) => c.request_id,
            Cmd::Kv(c) => c.request_id,
            Cmd::KvSweep { id, .. } => rid(*id),
            Cmd::Timers(c) => c.request_id,
            Cmd::Txn(c) => c.request_id,
        }
    }

    fn plan<R: Reads + ?Sized>(&self, p: &Planner<'_, R>, ov: &mut Overlay) -> Planned {
        match self {
            Cmd::Push(c) => p.plan_push(ov, c),
            Cmd::Effects(c) => p.plan_effects(ov, c),
            Cmd::Kv(c) => p.plan_kv(ov, c),
            Cmd::KvSweep { limit, .. } => {
                let effects = p.plan_kv_sweep(ov, *limit)?;
                if effects.is_empty() {
                    Ok(Plan::Empty(Outcome::Empty))
                } else {
                    ov.apply_effects(&effects);
                    Ok(Plan::Logged {
                        effects,
                        outcome: Outcome::Empty,
                    })
                }
            }
            Cmd::Timers(c) => p.plan_timers(ov, c),
            Cmd::Txn(c) => p.plan_transaction(ov, c),
        }
    }
}

// --------------------------------------------------------------------------
// Command builders
// --------------------------------------------------------------------------

fn frame(txn: &str) -> Vec<u8> {
    format!("{{\"t\":\"{txn}\"}}").into_bytes()
}

pub fn item(txn: &str) -> PushItem {
    PushItem {
        hash: crate::util::txn_hash128(txn),
        frame: frame(txn),
    }
}

pub fn push(id: u64, queue: &str, partition: &str, txns: &[&str]) -> Cmd {
    push_cfg(id, queue, partition, txns, qcfg())
}

pub fn push_cfg(id: u64, queue: &str, partition: &str, txns: &[&str], cfg: QueueConfig) -> Cmd {
    Cmd::Push(PushCommand {
        request_id: rid(id),
        tenant: TENANT.to_string(),
        queue: queue.to_string(),
        partition: partition.to_string(),
        items: txns.iter().map(|t| item(t)).collect(),
        create_cfg: cfg,
    })
}

/// A group's cursor row as the consumption engine checkpoints it: the last
/// acked offset `committed`, no lease.
pub fn cursor_row(committed: i64) -> CursorRow {
    CursorRow {
        committed,
        batch_end: None,
        worker: None,
        lease_expires_at_us: None,
        lease_acquired_at_us: None,
        batch_retry_count: 0,
        attempt_offset: None,
        attempt_count: 0,
        total_consumed: 0,
        lease_conflated: false,
        delivered: Vec::new(),
        created_at_us: BASE_US,
        metadata: String::new(),
        released: None,
    }
}

/// Deterministic effects as one command.
pub fn effects(id: u64, effects: Vec<Effect>) -> Cmd {
    Cmd::Effects(EffectsCommand {
        request_id: rid(id),
        tenant: TENANT.to_string(),
        effects,
    })
}

/// A checkpoint of one cursor: `group` has acked `pid` up to `committed`.
pub fn cursor_set(id: u64, pid: Pid, group: &str, committed: i64) -> Cmd {
    effects(
        id,
        vec![Effect::CursorSet {
            pid,
            group: group.to_string(),
            row: cursor_row(committed),
        }],
    )
}

/// An empty transaction (fill in the legs a test needs).
pub fn txn(id: u64) -> TxnCommand {
    TxnCommand {
        request_id: rid(id),
        tenant: TENANT.to_string(),
        pushes: Vec::new(),
        acks: Vec::new(),
        positional_acks: Vec::new(),
        kv: Vec::new(),
        timers: Vec::new(),
        extra_effects: Vec::new(),
        allow_duplicate: false,
        positions: Vec::new(),
        engine_acks: Default::default(),
    }
}

/// A queue delete as the receiver writes it: the queue row, and every one of
/// its partitions (`pids`) to garbage with a first chunk.
pub fn delete_queue(id: u64, queue: &str, pids: &[Pid]) -> Cmd {
    effects(
        id,
        vec![
            Effect::QueueDelete {
                tenant: TENANT.to_string(),
                queue: queue.to_string(),
            },
            Effect::GarbageAdd {
                pids: pids.to_vec(),
                scope: GarbageScope::Queue,
                deleted_at_us: BASE_US,
            },
            Effect::DeleteChunk {
                pids: pids.to_vec(),
                scope: GarbageScope::Queue,
                resume: Vec::new(),
                limit: 1_000,
            },
        ],
    )
}

// --------------------------------------------------------------------------
// The cell
// --------------------------------------------------------------------------

pub struct Cell {
    pub node: Node,
    index: u64,
    term: u64,
    wall: i64,
    /// Persistent across cycles, exactly like the batcher owns it. Disabled by
    /// default so existing tests plan precisely as the baseline; `enable_front`
    /// turns it on for the PERF-B tests.
    front: DedupFront,
}

/// What one [`Cell::run`] produced: the per-command results in input order, and
/// whether an entry was proposed.
pub struct Cycle {
    pub results: Vec<Planned>,
    pub now_us: i64,
    pub logged: bool,
}

impl Cycle {
    /// The outcome of command `i`, expecting it was decided (Logged or Empty).
    pub fn outcome(&self, i: usize) -> Outcome {
        match &self.results[i] {
            Ok(Plan::Logged { outcome, .. }) => outcome.clone(),
            Ok(Plan::Empty(o)) => o.clone(),
            other => panic!("command {i} was not decided: {other:?}"),
        }
    }

    pub fn plan(&self, i: usize) -> &Planned {
        &self.results[i]
    }
}

impl Cell {
    pub fn new(tag: &str) -> Cell {
        Cell {
            node: Node::new(tag),
            index: 0,
            term: 1,
            wall: BASE_US,
            front: DedupFront::disabled(),
        }
    }

    /// Turn the dedup front on for this cell (PERF-B tests). `cap_mb` bounds the
    /// filter footprint.
    pub fn enable_front(&mut self, cap_mb: usize) -> &mut Cell {
        self.front = DedupFront::new(true, cap_mb << 20);
        self
    }

    /// The cell's persistent dedup front, for asserting on its stats.
    pub fn front(&self) -> &DedupFront {
        &self.front
    }

    /// Advance the wall clock the planner stamps from.
    pub fn advance(&mut self, us: i64) -> &mut Cell {
        self.wall += us;
        self
    }

    pub fn now(&self) -> i64 {
        self.wall
    }

    /// Plan and apply one cycle of commands.
    pub fn run(&mut self, cmds: &[Cmd]) -> Cycle {
        self.run_fire(cmds, None).0
    }

    /// Plan and apply one cycle: the commands, then — with `fire` — the
    /// timer fire step exactly as the batcher runs it (after the commands, as
    /// ONE command with its own id). Returns the cycle, the fire report, and
    /// the fire command's effects (empty when nothing fired).
    pub fn run_fire(
        &mut self,
        cmds: &[Cmd],
        fire: Option<&TimerFireConfig>,
    ) -> (Cycle, FireReport, Vec<crate::rsm::effect::Effect>) {
        let (results, entry, now, report, fired) = self.plan_cycle_fire(cmds, fire);
        let logged = entry.is_some();
        if let Some(entry) = entry {
            self.index += 1;
            let index = self.index;
            let term = self.term;
            let (mut a, _rec) = Applier::open(
                self.node.store(),
                &self.node.seg_dir(),
                seg_opts(),
                cfg(),
                Arc::new(NoNotify),
            )
            .expect("open applier");
            a.apply(&ApplyCommitted {
                index,
                term,
                entry: entry.into(),
            })
            .expect("apply");
            a.durable_point().expect("durable point");
        }
        (
            Cycle {
                results,
                now_us: now,
                logged,
            },
            report,
            fired,
        )
    }

    /// Plan a cycle (commands + fire step) WITHOUT applying it: the entry it
    /// would propose, for the tests that fold it into a later overlay
    /// themselves (an entry still in flight).
    pub fn plan_entry(&self, cmds: &[Cmd], fire: Option<&TimerFireConfig>) -> Option<Entry> {
        self.plan_cycle_fire(cmds, fire).1
    }

    /// Plan the fire step over committed state plus an overlay that folded
    /// `in_flight` (the entries a batcher still has in its pipeline).
    pub fn plan_fire_over(
        &self,
        in_flight: &[Entry],
        fire: &TimerFireConfig,
    ) -> (FireReport, Vec<crate::rsm::effect::Effect>) {
        let wall = self.wall;
        self.node
            .store()
            .read(|r| {
                let committed = Committed::new(r);
                let mut ov = Overlay::new(r.next_pid()?, r.kv_version_next()?);
                for e in in_flight {
                    ov.ingest_entry(e);
                }
                let now = ov.plan_now(&committed, wall).expect("plan now");
                ov.mark_cycle_start();
                let planner =
                    Planner::new(committed, now, PlanConfig::default(), &self.front, None);
                let (effects, report) = planner.plan_timer_fire(&mut ov, fire).expect("fire");
                Ok((report, effects))
            })
            .expect("plan fire read")
    }

    /// Run one command, returning its plan.
    pub fn one(&mut self, cmd: Cmd) -> Planned {
        let mut c = self.run(&[cmd]);
        c.results.remove(0)
    }

    /// Plan a batch WITHOUT applying it — for measuring the planning step in
    /// isolation (the dedup front's whole effect is on planning, never apply).
    /// The persistent front is still consulted and updated, exactly as in a
    /// real cycle.
    pub fn plan_only(&self, cmds: &[Cmd]) -> Vec<Planned> {
        self.plan_cycle(cmds).0
    }

    fn plan_cycle(&self, cmds: &[Cmd]) -> (Vec<Planned>, Option<Entry>, i64) {
        let (results, entry, now, _, _) = self.plan_cycle_fire(cmds, None);
        (results, entry, now)
    }

    #[allow(clippy::type_complexity)]
    fn plan_cycle_fire(
        &self,
        cmds: &[Cmd],
        fire: Option<&TimerFireConfig>,
    ) -> (
        Vec<Planned>,
        Option<Entry>,
        i64,
        FireReport,
        Vec<crate::rsm::effect::Effect>,
    ) {
        let wall = self.wall;
        self.node
            .store()
            .read(|r| {
                let committed = Committed::new(r);
                let now = committed.plan_now(wall)?;
                let mut ov = Overlay::new(r.next_pid()?, r.kv_version_next()?);
                ov.mark_cycle_start();
                let planner =
                    Planner::new(committed, now, PlanConfig::default(), &self.front, None);

                // §7.1 step 3: the request-id lookup FIRST (D6, I6). A committed
                // or in-flight hit is answered from the recorded outcome and
                // plans nothing; a miss is planned.
                let mut results: Vec<Planned> = Vec::with_capacity(cmds.len());
                for cmd in cmds {
                    use crate::rsm::planner::Lookup;
                    match planner.lookup_request_id(&ov, &cmd.request_id()) {
                        Ok(Lookup::Committed(o)) | Ok(Lookup::InFlight(o)) => {
                            results.push(Ok(Plan::Empty(o)));
                        }
                        Ok(Lookup::Miss) => results.push(cmd.plan(&planner, &mut ov)),
                        Err(r) => results.push(Err(r)),
                    }
                }

                let mut entry = Entry::new(now, ov.cycle_pid_base(), ov.cycle_kv_base());
                let mut any = false;
                for (cmd, res) in cmds.iter().zip(&results) {
                    if let Ok(Plan::Logged { effects, outcome }) = res {
                        entry
                            .add_command(cmd.request_id(), outcome.clone(), effects.clone())
                            .expect("add command");
                        any = true;
                    }
                }
                // WP-2.3: the fire step, after the commands, as the batcher
                // runs it — one command with its own id, answered by nobody.
                let mut report = FireReport::default();
                let mut fired = Vec::new();
                if let Some(fcfg) = fire {
                    let (effects, rep) = planner.plan_timer_fire(&mut ov, fcfg).expect("fire step");
                    report = rep;
                    if !effects.is_empty() {
                        fired = effects.clone();
                        entry
                            .add_command(crate::util::uuidv7_bytes(), Outcome::Empty, effects)
                            .expect("add fire command");
                        any = true;
                    }
                }
                Ok((
                    results,
                    if any { Some(entry) } else { None },
                    now,
                    report,
                    fired,
                ))
            })
            .expect("plan cycle read")
    }

    // ---- committed-state readers for assertions --------------------------

    pub fn pid_of(&self, queue: &str, partition: &str) -> Option<Pid> {
        self.node
            .store()
            .read(|r| r.pid_of(TENANT, queue, partition))
            .expect("read")
    }

    pub fn cursor(&self, pid: Pid, group: &str) -> Option<CursorRow> {
        self.node
            .store()
            .read(|r| r.cursor(pid, group))
            .expect("read")
    }

    pub fn partition(&self, pid: Pid) -> Option<PartitionRow> {
        self.node.store().read(|r| r.partition(pid)).expect("read")
    }

    pub fn queue(&self, queue: &str) -> Option<QueueConfig> {
        self.node
            .store()
            .read(|r| r.queue(TENANT, queue))
            .expect("read")
    }

    pub fn group_exists(&self, queue: &str, group: &str) -> bool {
        self.node
            .store()
            .read(|r| Ok(r.group(TENANT, queue, group)?.is_some()))
            .expect("read")
    }

    pub fn partition_counter(&self, pid: Pid, c: Counter) -> i64 {
        self.node
            .store()
            .read(|r| r.partition_counter(pid, c))
            .expect("read")
    }

    /// Every dead letter, decoded (the fixtures use one queue at a time).
    pub fn dlq_rows(&self) -> Vec<DlqRow> {
        use crate::rsm::store::Keyspace;
        self.node
            .store()
            .read(|r| {
                let mut out = Vec::new();
                r.scan_raw(Keyspace::Dlq, &[], &[], usize::MAX, &mut |_k, v| {
                    if let Ok(row) = crate::rsm::store::rows::dlq_decode(v) {
                        out.push(row);
                    }
                    true
                })?;
                Ok(out)
            })
            .expect("read")
    }

    pub fn digest(&self) -> StateDigest {
        self.node.digest()
    }
}
