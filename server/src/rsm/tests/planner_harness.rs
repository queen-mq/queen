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
//! recovery and rebuilds the derived rings from committed `pending`, which is
//! the same state the planner rebuilds to plan the next cycle — so the two can
//! never drift on a stale in-RAM ring. It also sidesteps a self-referential
//! borrow of the store.

#![allow(dead_code)]

use std::sync::Arc;

use crate::rsm::apply::{Applier, Committed as ApplyCommitted, NoNotify, StateDigest};
use crate::rsm::dedup::DedupFront;
use crate::rsm::effect::{CursorRow, Pid, QueueConfig};
use crate::rsm::entry::{Entry, Outcome, RequestId};
use crate::rsm::planner::timers::{FireReport, TimerFireConfig, TimersCommand};
use crate::rsm::planner::{
    AckCommand, AckItem, AckPositionalCommand, AckStatus, AckTarget, DlqHeadCommand, DlqSnapshot,
    NackCommand, Overlay, Plan, PlanConfig, Planned, Planner, PopCommand, PushCommand, PushItem,
    RenewCommand, SubIntent,
};
use crate::rsm::state::{Committed, Derived};
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
    PopPinned(PopCommand),
    PopWildcard(PopCommand),
    PopDiscover(PopCommand),
    Ack(AckCommand),
    AckPos(AckPositionalCommand),
    Nack(NackCommand),
    Renew(RenewCommand),
    DlqHead(DlqHeadCommand),
    /// WP-2.3: a timers call (schedule / cancel).
    Timers(TimersCommand),
}

impl Cmd {
    fn request_id(&self) -> RequestId {
        match self {
            Cmd::Push(c) => c.request_id,
            Cmd::PopPinned(c) | Cmd::PopWildcard(c) | Cmd::PopDiscover(c) => c.request_id,
            Cmd::Ack(c) => c.request_id,
            Cmd::AckPos(c) => c.request_id,
            Cmd::Nack(c) => c.request_id,
            Cmd::Renew(c) => c.request_id,
            Cmd::DlqHead(c) => c.request_id,
            Cmd::Timers(c) => c.request_id,
        }
    }

    fn plan<R: Reads + ?Sized>(&self, p: &Planner<'_, R>, ov: &mut Overlay) -> Planned {
        match self {
            Cmd::Push(c) => p.plan_push(ov, c),
            Cmd::PopPinned(c) => p.plan_pop_pinned(ov, c),
            Cmd::PopWildcard(c) => p.plan_pop_wildcard(ov, c),
            Cmd::PopDiscover(c) => p.plan_pop_discover(ov, c),
            Cmd::Ack(c) => p.plan_ack(ov, c),
            Cmd::AckPos(c) => p.plan_ack_positional(ov, c),
            Cmd::Nack(c) => p.plan_nack(ov, c),
            Cmd::Renew(c) => p.plan_renew(ov, c),
            Cmd::DlqHead(c) => p.plan_dlq_head(ov, c),
            Cmd::Timers(c) => p.plan_timers(ov, c),
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

fn pop_base(id: u64, queue: &str, group: &str, worker: &str) -> PopCommand {
    PopCommand {
        request_id: rid(id),
        tenant: TENANT.to_string(),
        queue: queue.to_string(),
        partition: None,
        group: group.to_string(),
        worker: worker.to_string(),
        budget: 100,
        max_parts: 10,
        lease_seconds: 60,
        auto_ack: false,
        conflate: false,
        sub: SubIntent {
            mode: "all".to_string(),
            from_us: None,
            now: false,
        },
        skip_window_debounce: false,
        namespace: String::new(),
        task: String::new(),
        create_cfg: Some(qcfg()),
        deadline_us: 0,
    }
}

pub fn pop_pinned(id: u64, queue: &str, partition: &str, group: &str, worker: &str) -> Cmd {
    let mut c = pop_base(id, queue, group, worker);
    c.partition = Some(partition.to_string());
    Cmd::PopPinned(c)
}

pub fn pop_wildcard(id: u64, queue: &str, group: &str, worker: &str) -> Cmd {
    Cmd::PopWildcard(pop_base(id, queue, group, worker))
}

/// A wildcard pop the builder hands to the test to tweak (budget, sub, auto_ack…).
pub fn pop_wildcard_with(
    id: u64,
    queue: &str,
    group: &str,
    worker: &str,
    f: impl FnOnce(&mut PopCommand),
) -> Cmd {
    let mut c = pop_base(id, queue, group, worker);
    f(&mut c);
    Cmd::PopWildcard(c)
}

pub fn pop_pinned_with(
    id: u64,
    queue: &str,
    partition: &str,
    group: &str,
    worker: &str,
    f: impl FnOnce(&mut PopCommand),
) -> Cmd {
    let mut c = pop_base(id, queue, group, worker);
    c.partition = Some(partition.to_string());
    f(&mut c);
    Cmd::PopPinned(c)
}

pub fn ack(
    id: u64,
    pid: Pid,
    queue: &str,
    group: &str,
    worker: &str,
    items: &[(&str, AckStatus)],
) -> Cmd {
    Cmd::Ack(AckCommand {
        request_id: rid(id),
        targets: vec![AckTarget {
            pid,
            tenant: TENANT.to_string(),
            queue: queue.to_string(),
            group: group.to_string(),
            worker: worker.to_string(),
            items: items
                .iter()
                .map(|(t, s)| AckItem {
                    hash: crate::util::txn_hash128(t),
                    status: *s,
                    error: None,
                    snapshot: matches!(s, AckStatus::Failed | AckStatus::Dlq).then(|| {
                        DlqSnapshot {
                            message_id: None,
                            txn: t.to_string(),
                            payload: frame(t),
                        }
                    }),
                })
                .collect(),
        }],
    })
}

pub fn ack_pos(
    id: u64,
    pid: Pid,
    queue: &str,
    group: &str,
    worker: &str,
    upto: Option<i64>,
    ok: bool,
    acked_count: i32,
) -> Cmd {
    Cmd::AckPos(AckPositionalCommand {
        request_id: rid(id),
        pid,
        tenant: TENANT.to_string(),
        queue: queue.to_string(),
        group: group.to_string(),
        worker: worker.to_string(),
        upto,
        ok,
        acked_count,
    })
}

pub fn nack(id: u64, pid: Pid, queue: &str, group: &str, worker: &str) -> Cmd {
    Cmd::Nack(NackCommand {
        request_id: rid(id),
        pid,
        tenant: TENANT.to_string(),
        queue: queue.to_string(),
        group: group.to_string(),
        worker: worker.to_string(),
    })
}

pub fn renew(id: u64, worker: &str, seconds: i32) -> Cmd {
    Cmd::Renew(RenewCommand {
        request_id: rid(id),
        worker: worker.to_string(),
        seconds,
    })
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
    /// `QUEEN_RAFT_CLAIM_FROM_RING` override for this cell (PERF-I): `None` uses
    /// the environment default (on), `Some(v)` forces the bounded/ baseline claim
    /// path so the differential A/B runs both in one process.
    claim_from_ring: Option<bool>,
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
            claim_from_ring: None,
        }
    }

    /// Force the PERF-I claim path for this cell (the differential A/B).
    pub fn claim_from_ring(&mut self, v: bool) -> &mut Cell {
        self.claim_from_ring = Some(v);
        self
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
            a.apply(&ApplyCommitted { index, term, entry })
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
                let d0 = Derived::default();
                let base = Committed::new(r, &d0).plan_now(wall)?;
                let d = Derived::rebuild(r, base)?;
                let committed = Committed::new(r, &d);
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
                let d0 = Derived::default();
                let now = Committed::new(r, &d0).plan_now(wall)?;
                let d = Derived::rebuild(r, now)?;
                let committed = Committed::new(r, &d);
                let mut ov = Overlay::new(r.next_pid()?, r.kv_version_next()?);
                ov.mark_cycle_start();
                let mut planner =
                    Planner::new(committed, now, PlanConfig::default(), &self.front, None);
                if let Some(v) = self.claim_from_ring {
                    planner.set_claim_from_ring(v);
                }

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
            .read(|r| Ok(r.pid_of(TENANT, queue, partition)?))
            .expect("read")
    }

    pub fn cursor(&self, pid: Pid, group: &str) -> Option<CursorRow> {
        self.node
            .store()
            .read(|r| Ok(r.cursor(pid, group)?))
            .expect("read")
    }

    pub fn partition(&self, pid: Pid) -> Option<PartitionRow> {
        self.node
            .store()
            .read(|r| Ok(r.partition(pid)?))
            .expect("read")
    }

    pub fn queue(&self, queue: &str) -> Option<QueueConfig> {
        self.node
            .store()
            .read(|r| Ok(r.queue(TENANT, queue)?))
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
            .read(|r| Ok(r.partition_counter(pid, c)?))
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
