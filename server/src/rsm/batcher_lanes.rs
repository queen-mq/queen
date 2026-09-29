//! LANES (`QUEEN_LANES` > 1): a cycle's commands planned in parallel.
//!
//! One Raft log, one entry per cycle, exactly as with a single planner; what
//! changes is who plans. A LANE is a thread that owns the partitions with
//! `pid % n == lane` and plans every command that touches only them — pushes
//! to an existing partition, pinned and wildcard pops, acks, nacks, DLQ heads,
//! single-lane transactions — against its OWN kept overlay (its effects, plus
//! the catalog effects every lane must see). The lanes of a cycle run at the
//! same time; then the CONTROL step plans everything else on the planner
//! thread, with a full view (every in-flight entry plus this cycle's lane
//! effects): the catalog (queue and group creation, configuration, deletes),
//! new partitions, KV, timers, cross-lane commands, the leader steps. Lane
//! effects of different lanes touch disjoint state, and control comes after
//! them in the entry, so the entry applies exactly as if one thread had
//! planned it in that order.
//!
//! A wildcard pop names no partition. It goes to the lane that holds every
//! partition of its queue when one does ([`LanesState::whole_lane`]), and what
//! that lane finds empty is empty. Otherwise the router sends it where a
//! partition is claimable ([`LanesState::wildcard_dest`]): every lane moves its
//! kept rings with its own planned claims, acks and appends (not only when an
//! entry lands), and the router reads what each ring can give from them; it
//! sends a pop to the lane whose claimable partition has waited longest and
//! reserves what the pop may take, so the pops of a cycle do not race for one
//! partition. When no lane has anything the whole queue is empty: a long-poll
//! pop is answered at once (it parks until a partition becomes claimable) and a
//! pop that does not wait goes to control, which sees every partition. A lane
//! the router sent a pop to and that finds nothing after all hands it to
//! control too; a ring some lane does not keep yet is guessed, once.
//!
//! A push to a partition that does not exist yet is planned by the CREATION
//! step, after the lanes and before control, on a fresh overlay over the
//! cycle's bases (its partition takes the cycle's next id); a push to one an
//! entry in flight created goes to that partition's lane, whose slice holds the
//! create. A renew goes to the one lane holding its worker's leases.
//!
//! Invariants that make this equal to the single planner:
//!
//! - **L1** a lane plans only commands whose every partition is its own and
//!   already committed, on a queue with no catalog change or delete in flight;
//!   anything else goes to control ([`LanesState::route`]). A lane result that
//!   would create catalog state anyway is discarded and re-planned by control.
//! - **L2** one clock per cycle: the router stamps `now` for every lane and
//!   control from the committed floor and every in-flight entry (I5).
//! - **L3** pids stay on the one global counter: only the creation step and
//!   control create partitions, both from the cycle's base in entry order
//!   (creations right after the lanes' commands, then control's), so the
//!   entry's creates are dense from its `pid_base` (I18 unchanged).
//! - **L4** a lane's overlay holds only its own partitions' effects (and the
//!   catalog): what it folds is recorded as its slice of the entry, and the
//!   slices of in-flight entries are what its kept overlay advances over.
//! - **L5** request ids are looked up once, by the router, before routing:
//!   committed first, then every in-flight entry.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use super::{
    plan_fire_step, KeepCfg, PlanOutput, Slot, KEEP_RESET_EVERY, RING_EVICT_EVERY, RING_IDLE_CYCLES,
};
use crate::rsm::dedup::DedupFront;
use crate::rsm::effect::{Effect, Pid};
use crate::rsm::entry::{Entry, Outcome, PopOutcome, RequestId};
use crate::rsm::planner::kept::KeptOverlay;
use crate::rsm::planner::timers::TimerFireConfig;
use crate::rsm::planner::{
    Lookup, Overlay, Plan, PlanConfig, PlannedPending, Planner, Refusal, RenewCommand,
};
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::segments::Reader;
use crate::rsm::state::{Committed, Derived, PlanRings, RingKey, RingView};
use crate::rsm::store::{Reads, Store, TypedReads};

use super::{Command, Reply};

/// A persistent lane thread running planning jobs.
struct LaneWorker {
    tx: std::sync::mpsc::Sender<Box<dyn FnOnce() + Send>>,
}

impl LaneWorker {
    fn spawn(lane: usize) -> LaneWorker {
        let (tx, rx) = std::sync::mpsc::channel::<Box<dyn FnOnce() + Send>>();
        std::thread::Builder::new()
            .name(format!("queen-lane-{lane}"))
            .spawn(move || {
                for job in rx {
                    job();
                }
            })
            .expect("spawn a lane thread");
        LaneWorker { tx }
    }
}

/// What one lane keeps between cycles.
#[derive(Default)]
struct LaneState {
    kept: Option<KeptOverlay>,
    rings: Option<PlanRings>,
    /// The tag of the cycle in progress (set by `begin_cycle`).
    tag: u64,
}

/// The `(pid, group)` rows a lane's effects can move in its rings: an append
/// arms every group of the partition's queue, a cursor write its own group.
#[derive(Default)]
struct Touched {
    appended: HashSet<Pid>,
    cursors: HashSet<(Pid, String)>,
}

impl Touched {
    fn note(&mut self, effects: &[Effect]) {
        for e in effects {
            match e {
                Effect::Append { pid, .. } => {
                    self.appended.insert(*pid);
                }
                Effect::CursorSet { pid, group, .. } | Effect::CursorDelete { pid, group } => {
                    self.cursors.insert((*pid, group.clone()));
                }
                _ => {}
            }
        }
    }
}

/// One entry in flight, as the lanes know it.
struct Record {
    full: Arc<Entry>,
    /// Each lane's slice: exactly what that lane folded for this entry.
    subs: Vec<Arc<Entry>>,
    now_us: i64,
    pid_hi: u64,
    kv_hi: u64,
    created_hi: i64,
    /// Queues whose catalog rows this entry changes, tenants it purges, and
    /// pids it deletes: commands touching them go to control until it lands.
    queues: Vec<(String, String)>,
    tenants: Vec<String>,
    pids: Vec<Pid>,
    /// Queues this entry creates partitions for: until it lands no lane holds
    /// the whole of one ([`LanesState::whole_lane`]).
    creates: Vec<(String, String)>,
    /// Workers this entry grants or moves a lease for: a renew of one of them
    /// goes to control until it lands (its lease may sit in any lane).
    workers: HashSet<String>,
    /// The partitions this entry creates, by name: until it lands a push to one
    /// goes to its lane, whose slice of the entry holds the create.
    created: Vec<(String, String, String, Pid)>,
}

/// The lanes' state on the planner thread (see the module header).
pub(crate) struct LanesState {
    n: u64,
    workers: Vec<LaneWorker>,
    lanes: Vec<Arc<Mutex<LaneState>>>,
    records: VecDeque<Record>,
    /// Every in-flight command's outcome by request id (L5).
    ids: HashMap<RequestId, Outcome>,
    epoch: u64,
    cycles: u64,
    /// Round-robin cursor per wildcard ring, for the pops of a ring some lane
    /// does not keep yet (its first cycle).
    rr: HashMap<RingKey, u64>,
    /// The router's state for the cycle it is routing (reset every cycle).
    cycle: RouterCycle,
}

/// tenant → queue → name → `V`: nested so a lookup borrows `&str` instead of
/// building an owned key per command.
type ByName<V> = HashMap<String, HashMap<String, HashMap<String, V>>>;

/// One lane's side of a ring in the cycle being routed.
struct LaneAvail {
    view: RingView,
    /// How many of `view.rows` this cycle's pops already took.
    taken: usize,
}

/// What the router reads once per cycle rather than once per command.
#[derive(Default)]
struct RouterCycle {
    /// The cycle's clock (L2).
    now_us: i64,
    /// What each lane can still give a wildcard pop of a ring this cycle: its
    /// kept ring as its last job left it (the rows its entries in flight move
    /// at their planned state, the leases due by `now_us` claimable), less what
    /// the router has already sent it. `None` while some lane does not keep
    /// the ring: a pop of it is then guessed.
    avail: HashMap<RingKey, Option<Vec<LaneAvail>>>,
    /// Per queue: the lane holding every partition of it, if one does.
    whole: HashMap<(String, String), Option<u64>>,
    /// Per ring: whether its queue and group are committed and its queue quiet
    /// (a wildcard pop may go to a lane at all).
    wild: HashMap<RingKey, bool>,
    /// Workers a pop of this batch leases for: their renews go to control.
    pop_workers: HashSet<String>,
    /// tenant → queue → committed.
    queues: HashMap<String, HashMap<String, bool>>,
    /// tenant → queue → group → committed.
    groups: ByName<bool>,
    /// tenant → queue → partition name → its committed pid, and whether it is
    /// garbage.
    pids: ByName<Option<(Pid, bool)>>,
    /// pid → committed and not garbage.
    live: HashMap<Pid, bool>,
}

/// How many commands the lanes and control planned (a log line every 10 s).
struct LaneStats {
    lane: std::sync::atomic::AtomicU64,
    control: std::sync::atomic::AtomicU64,
    cycles: std::sync::atomic::AtomicU64,
    /// Microseconds summed over the cycles: router, fork-to-join, control,
    /// merge, and the lanes' own wall and CPU inside their jobs.
    router_us: std::sync::atomic::AtomicU64,
    lanes_us: std::sync::atomic::AtomicU64,
    control_us: std::sync::atomic::AtomicU64,
    merge_us: std::sync::atomic::AtomicU64,
    job_wall_us: std::sync::atomic::AtomicU64,
    job_cpu_us: std::sync::atomic::AtomicU64,
    jobs: std::sync::atomic::AtomicU64,
    /// Pushes the creation step planned, and its microseconds.
    create: std::sync::atomic::AtomicU64,
    create_us: std::sync::atomic::AtomicU64,
    last: Mutex<Option<std::time::Instant>>,
}

static LANE_STATS: LaneStats = LaneStats {
    lane: std::sync::atomic::AtomicU64::new(0),
    control: std::sync::atomic::AtomicU64::new(0),
    cycles: std::sync::atomic::AtomicU64::new(0),
    router_us: std::sync::atomic::AtomicU64::new(0),
    lanes_us: std::sync::atomic::AtomicU64::new(0),
    control_us: std::sync::atomic::AtomicU64::new(0),
    merge_us: std::sync::atomic::AtomicU64::new(0),
    job_wall_us: std::sync::atomic::AtomicU64::new(0),
    job_cpu_us: std::sync::atomic::AtomicU64::new(0),
    jobs: std::sync::atomic::AtomicU64::new(0),
    create: std::sync::atomic::AtomicU64::new(0),
    create_us: std::sync::atomic::AtomicU64::new(0),
    last: Mutex::new(None),
};

fn add(c: &std::sync::atomic::AtomicU64, v: u64) {
    c.fetch_add(v, std::sync::atomic::Ordering::Relaxed);
}

impl LaneStats {
    fn maybe_log(&self) {
        let mut last = self.last.lock().expect("lane stats");
        let now = std::time::Instant::now();
        if last.is_some_and(|t| now.duration_since(t) < std::time::Duration::from_secs(10)) {
            return;
        }
        *last = Some(now);
        use std::sync::atomic::Ordering::Relaxed;
        let lane = self.lane.swap(0, Relaxed);
        let control = self.control.swap(0, Relaxed);
        let cycles = self.cycles.swap(0, Relaxed).max(1);
        let jobs = self.jobs.swap(0, Relaxed).max(1);
        let per = |c: &std::sync::atomic::AtomicU64, n: u64| c.swap(0, Relaxed) / n;
        let (router, lanes, ctl, merge) = (
            per(&self.router_us, cycles),
            per(&self.lanes_us, cycles),
            per(&self.control_us, cycles),
            per(&self.merge_us, cycles),
        );
        let (job_wall, job_cpu) = (per(&self.job_wall_us, jobs), per(&self.job_cpu_us, jobs));
        let create = self.create.swap(0, Relaxed);
        let create_us = per(&self.create_us, cycles);
        if lane + control + create > 0 {
            tracing::info!(
                target: "rsm",
                lane,
                control,
                router_us = router,
                lanes_us = lanes,
                control_us = ctl,
                merge_us = merge,
                job_wall_us = job_wall,
                job_cpu_us = job_cpu,
                create,
                create_us,
                "lanes: commands planned since the last line (per-cycle means)",
            );
        }
    }
}

/// Where a command is planned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Dest {
    Lane(u64),
    /// A wildcard pop's lane that holds every partition of the queue: what it
    /// finds empty is empty.
    Whole(u64),
    Control,
    /// A long-poll wildcard pop no lane has anything for: empty now, without
    /// planning (the facade parks it; the next claimable partition wakes it).
    Empty,
    /// A push whose partition does not exist yet: the creation step plans it.
    Create,
}

/// The most committed partitions the router reads to find the one lane of a
/// queue ([`LanesState::whole_lane`]); a queue with more counts as spanning
/// lanes.
const WHOLE_SCAN_CAP: usize = 16;

/// How one command fared in a lane.
enum LaneSlot {
    /// Logged into the lane's part of the entry ([`LaneOut::part`]).
    Logged(RequestId),
    SameCycle(RequestId),
    Empty(Outcome),
    Refused(Refusal),
    Deferred(Box<Command>),
    /// Planned catalog state (L1 violated), or a wildcard pop that found
    /// nothing ready in this lane (another lane may have): control plans it
    /// again, with every partition in view.
    Misrouted(Box<Command>),
}

struct LaneOut {
    slots: Vec<(usize, LaneSlot)>,
    /// The lane folded something its logged commands do not account for.
    poisoned: bool,
    /// Its logged commands with their effects, as they go into the entry:
    /// built on the lane's thread, so the merge only moves them.
    part: Entry,
    /// The same commands with payload-free effects ([`strip`]): the lane's
    /// slice of the entry, also built here.
    sub: Entry,
}

/// A clone of `e` without its payload: a lane's slice is only ever unfolded
/// (keys, offsets, hashes), never applied or written.
fn strip(e: &Effect) -> Effect {
    let mut c = e.clone();
    if let Effect::Append { blob, .. } = &mut c {
        *blob = Vec::new();
    }
    c
}

/// Which lanes must fold `e`: its partition's lane, every lane (catalog), or
/// none (control state only).
fn lanes_of(e: &Effect, n: u64) -> LanesOf {
    match e {
        Effect::PartitionCreate { pid, .. }
        | Effect::PartitionDelete { pid }
        | Effect::Append { pid, .. }
        | Effect::CursorSet { pid, .. }
        | Effect::CursorDelete { pid, .. }
        | Effect::DlqInsert { pid, .. }
        | Effect::Watermark { pid, .. }
        | Effect::StreamsStatePut { pid, .. }
        | Effect::StreamsStateDelete { pid, .. } => LanesOf::One(pid % n),
        Effect::QueueUpsert { .. }
        | Effect::QueueDelete { .. }
        | Effect::GroupUpsert { .. }
        | Effect::GroupDelete { .. }
        | Effect::GarbageAdd { .. }
        | Effect::DeleteChunk { .. }
        | Effect::TenantPurge { .. }
        | Effect::EphemeralConfigSet { .. }
        | Effect::EphemeralConfigDelete { .. } => LanesOf::All,
        _ => LanesOf::None,
    }
}

enum LanesOf {
    One(u64),
    All,
    None,
}

/// Split an entry into the lanes' slices by [`lanes_of`] (a reset rebuilds the
/// lanes from the entries in flight this way).
fn split(full: &Entry, n: u64) -> Vec<Arc<Entry>> {
    let mut subs: Vec<Entry> = (0..n)
        .map(|_| Entry::new(full.now_us, full.pid_base, full.kv_version_base))
        .collect();
    for c in &full.commands {
        let mut per: Vec<Vec<Effect>> = vec![Vec::new(); n as usize];
        for e in full.effects_of(c) {
            match lanes_of(e, n) {
                LanesOf::One(l) => per[l as usize].push(strip(e)),
                LanesOf::All => {
                    for p in per.iter_mut() {
                        p.push(strip(e));
                    }
                }
                LanesOf::None => {}
            }
        }
        for (l, effects) in per.into_iter().enumerate() {
            if !effects.is_empty() {
                let _ = subs[l].add_command(c.request_id, c.outcome.clone(), effects);
            }
        }
    }
    subs.into_iter().map(Arc::new).collect()
}

fn record_of(full: Arc<Entry>, subs: Vec<Arc<Entry>>) -> Record {
    let (mut pid_hi, mut kv_hi, mut created_hi) = (0u64, 0u64, 0i64);
    let mut queues = Vec::new();
    let mut tenants = Vec::new();
    let mut pids = Vec::new();
    let mut creates: Vec<(String, String)> = Vec::new();
    let mut workers: HashSet<String> = HashSet::new();
    let mut created: Vec<(String, String, String, Pid)> = Vec::new();
    for e in &full.effects {
        if let Effect::PartitionCreate {
            pid,
            tenant,
            queue,
            partition,
            ..
        } = e
        {
            created.push((tenant.clone(), queue.clone(), partition.clone(), *pid));
        }
        match e {
            Effect::CursorSet { row, .. } => {
                if let Some(w) = &row.worker {
                    if !workers.contains(w) {
                        workers.insert(w.clone());
                    }
                }
            }
            Effect::PartitionCreate {
                pid,
                created_at_us,
                tenant,
                queue,
                ..
            } => {
                pid_hi = pid_hi.max(pid.saturating_add(1));
                created_hi = created_hi.max(*created_at_us);
                if creates
                    .last()
                    .is_none_or(|(t, q)| t != tenant || q != queue)
                {
                    creates.push((tenant.clone(), queue.clone()));
                }
            }
            Effect::Append { created_at_us, .. } => created_hi = created_hi.max(*created_at_us),
            Effect::KvPut { version, .. } => kv_hi = kv_hi.max(version.saturating_add(1)),
            Effect::QueueUpsert { tenant, queue, .. }
            | Effect::QueueDelete { tenant, queue }
            | Effect::GroupUpsert { tenant, queue, .. }
            | Effect::GroupDelete { tenant, queue, .. }
            | Effect::EphemeralConfigSet { tenant, queue, .. }
            | Effect::EphemeralConfigDelete { tenant, queue } => {
                queues.push((tenant.clone(), queue.clone()))
            }
            Effect::TenantPurge { tenant } => tenants.push(tenant.clone()),
            Effect::PartitionDelete { pid } => pids.push(*pid),
            Effect::GarbageAdd { pids: p, .. } | Effect::DeleteChunk { pids: p, .. } => {
                pids.extend(p.iter().copied())
            }
            _ => {}
        }
    }
    creates.sort_unstable();
    creates.dedup();
    Record {
        now_us: full.now_us,
        full,
        subs,
        pid_hi,
        kv_hi,
        created_hi,
        queues,
        tenants,
        pids,
        creates,
        workers,
        created,
    }
}

/// The in-flight catalog work commands must not race (L1).
struct Busy {
    /// tenant → queues with a catalog change in flight.
    queues: HashMap<String, HashSet<String>>,
    tenants: HashSet<String>,
    pids: HashSet<Pid>,
    /// tenant → queues with a partition being created.
    creating: HashMap<String, HashSet<String>>,
    /// Workers with a lease write in flight ([`Record::workers`]).
    workers: HashSet<String>,
    /// tenant → queue → partition name → pid, for the partitions created in
    /// flight ([`Record::created`]).
    created: ByName<Pid>,
}

impl Busy {
    fn queue(&self, tenant: &str, queue: &str) -> bool {
        self.tenants.contains(tenant)
            || self.queues.get(tenant).is_some_and(|qs| qs.contains(queue))
    }

    fn creating(&self, tenant: &str, queue: &str) -> bool {
        self.creating
            .get(tenant)
            .is_some_and(|qs| qs.contains(queue))
    }

    /// The pid an entry in flight created for `(tenant, queue, name)`.
    fn created(&self, tenant: &str, queue: &str, name: &str) -> Option<Pid> {
        self.created.get(tenant)?.get(queue)?.get(name).copied()
    }
}

impl LanesState {
    pub(crate) fn new(n: u64) -> LanesState {
        let n = n.max(2);
        LanesState {
            n,
            workers: (0..n as usize).map(LaneWorker::spawn).collect(),
            lanes: (0..n)
                .map(|_| Arc::new(Mutex::new(LaneState::default())))
                .collect(),
            records: VecDeque::new(),
            ids: HashMap::new(),
            epoch: u64::MAX,
            cycles: 0,
            rr: HashMap::new(),
            cycle: RouterCycle::default(),
        }
    }

    fn reset(&mut self) {
        self.records.clear();
        self.ids.clear();
        for l in &self.lanes {
            let mut s = l.lock().expect("lane state");
            s.kept = None;
            s.rings = None;
        }
    }

    /// Bring the records in line with the in-flight list: drop the landed
    /// prefix; anything else unexpected (an entry this state never saw) resets
    /// the lanes, which then rebuild from slices split off the full entries.
    fn sync(&mut self, folded: &[(u64, Arc<Entry>)]) {
        let first = folded.first().map(|(_, e)| e.clone());
        while let Some(r) = self.records.front() {
            match &first {
                Some(f) if Arc::ptr_eq(&r.full, f) => break,
                _ => {
                    let r = self.records.pop_front().expect("front");
                    for c in &r.full.commands {
                        self.ids.remove(&c.request_id);
                    }
                }
            }
        }
        let aligned = self.records.len() == folded.len()
            && self
                .records
                .iter()
                .zip(folded.iter())
                .all(|(r, (_, e))| Arc::ptr_eq(&r.full, e));
        if !aligned {
            self.reset();
            for (_, e) in folded {
                let subs = split(e, self.n);
                for c in &e.commands {
                    self.ids
                        .entry(c.request_id)
                        .or_insert_with(|| c.outcome.clone());
                }
                self.records.push_back(record_of(e.clone(), subs));
            }
        }
    }

    fn busy(&self) -> Busy {
        let mut b = Busy {
            queues: HashMap::new(),
            tenants: HashSet::new(),
            pids: HashSet::new(),
            creating: HashMap::new(),
            workers: HashSet::new(),
            created: HashMap::new(),
        };
        for r in &self.records {
            for (t, q) in &r.queues {
                b.queues.entry(t.clone()).or_default().insert(q.clone());
            }
            b.tenants.extend(r.tenants.iter().cloned());
            b.pids.extend(r.pids.iter().copied());
            for (t, q) in &r.creates {
                b.creating.entry(t.clone()).or_default().insert(q.clone());
            }
            b.workers.extend(r.workers.iter().cloned());
            for (t, q, name, pid) in &r.created {
                b.created
                    .entry(t.clone())
                    .or_default()
                    .entry(q.clone())
                    .or_default()
                    .insert(name.clone(), *pid);
            }
        }
        b
    }

    /// L1: where `cmd` is planned. Every committed read goes through the
    /// cycle's caches ([`RouterCycle`]): a batch of pushes to a few partitions
    /// reads each of them once, not once per push.
    fn route<R: Reads + ?Sized>(
        &mut self,
        r: &R,
        cmd: &Command,
        busy: &Busy,
    ) -> crate::rsm::store::Result<Dest> {
        let one = |lanes: &[Option<u64>]| -> Dest {
            match lanes.first() {
                Some(Some(l)) if lanes.iter().all(|x| *x == Some(*l)) => Dest::Lane(*l),
                _ => Dest::Control,
            }
        };
        Ok(match cmd {
            Command::Push(c) => self.push_dest(r, &c.tenant, &c.queue, &c.partition, busy)?,
            Command::PopPinned(c) => {
                self.cycle.pop_workers.insert(c.worker.clone());
                match c.partition.as_deref() {
                    Some(name)
                        if !c.conflate && self.group_known(r, &c.tenant, &c.queue, &c.group)? =>
                    {
                        match self.part_lane(r, &c.tenant, &c.queue, name, busy)? {
                            Some(l) => Dest::Lane(l),
                            None => Dest::Control,
                        }
                    }
                    _ => Dest::Control,
                }
            }
            Command::PopWildcard(c) => {
                self.cycle.pop_workers.insert(c.worker.clone());
                let key = (c.tenant.clone(), c.queue.clone(), c.group.clone());
                if c.conflate || !self.wildcard_may_lane(r, &key, busy)? {
                    Dest::Control
                } else if let Some(l) = self.whole_lane_once(r, &c.tenant, &c.queue, busy)? {
                    Dest::Whole(l)
                } else {
                    self.wildcard_dest(&key, c.max_parts, c.wait)
                }
            }
            Command::Ack(c) => {
                let mut lanes = Vec::with_capacity(c.targets.len());
                for t in &c.targets {
                    lanes.push(self.pid_lane(r, &t.tenant, &t.queue, t.pid, busy)?);
                }
                one(&lanes)
            }
            Command::AckPositional(c) => {
                match self.pid_lane(r, &c.tenant, &c.queue, c.pid, busy)? {
                    Some(l) => Dest::Lane(l),
                    None => Dest::Control,
                }
            }
            Command::Nack(c) => match self.pid_lane(r, &c.tenant, &c.queue, c.pid, busy)? {
                Some(l) => Dest::Lane(l),
                None => Dest::Control,
            },
            Command::DlqHead(c) => match self.pid_lane(r, &c.tenant, &c.queue, c.pid, busy)? {
                Some(l) => Dest::Lane(l),
                None => Dest::Control,
            },
            Command::Transaction(c) => {
                // Positions can create partitions and register groups, which
                // only control does (L1, L3).
                if !c.kv.is_empty()
                    || !c.timers.is_empty()
                    || !c.extra_effects.is_empty()
                    || !c.positions.is_empty()
                {
                    Dest::Control
                } else {
                    let mut lanes = Vec::new();
                    for p in &c.pushes {
                        lanes.push(self.part_lane(r, &p.tenant, &p.queue, &p.partition, busy)?);
                    }
                    for t in &c.acks {
                        lanes.push(self.pid_lane(r, &t.tenant, &t.queue, t.pid, busy)?);
                    }
                    for a in &c.positional_acks {
                        lanes.push(self.pid_lane(r, &a.tenant, &a.queue, a.pid, busy)?);
                    }
                    one(&lanes)
                }
            }
            Command::Renew(c) => self.renew_dest(r, c, busy)?,
            Command::PopDiscover(_) | Command::Kv(_) | Command::Timers(_) | Command::Effects(_) => {
                Dest::Control
            }
        })
    }

    /// Where a push goes: the lane of its partition when that is committed and
    /// live on a quiet queue; the lane that created it when an entry in flight
    /// did (its slice holds the create); the creation step when the partition
    /// does not exist yet; control for anything else (a queue to create, a
    /// catalog change in flight, a partition on its way out).
    fn push_dest<R: Reads + ?Sized>(
        &mut self,
        r: &R,
        tenant: &str,
        queue: &str,
        name: &str,
        busy: &Busy,
    ) -> crate::rsm::store::Result<Dest> {
        if busy.queue(tenant, queue) || !self.queue_known(r, tenant, queue)? {
            return Ok(Dest::Control);
        }
        Ok(match self.committed_pid(r, tenant, queue, name)? {
            Some((pid, false)) if !busy.pids.contains(&pid) => Dest::Lane(pid % self.n),
            Some(_) => Dest::Control,
            None => match busy.created(tenant, queue, name) {
                Some(pid) if !busy.pids.contains(&pid) => Dest::Lane(pid % self.n),
                Some(_) => Dest::Control,
                None => Dest::Create,
            },
        })
    }

    /// The lane of a committed, live partition of a quiet queue, by name.
    fn part_lane<R: Reads + ?Sized>(
        &mut self,
        r: &R,
        tenant: &str,
        queue: &str,
        name: &str,
        busy: &Busy,
    ) -> crate::rsm::store::Result<Option<u64>> {
        if busy.queue(tenant, queue) || !self.queue_known(r, tenant, queue)? {
            return Ok(None);
        }
        Ok(match self.committed_pid(r, tenant, queue, name)? {
            Some((pid, false)) if !busy.pids.contains(&pid) => Some(pid % self.n),
            _ => None,
        })
    }

    /// The lane of a committed, live partition of a quiet queue, by pid.
    fn pid_lane<R: Reads + ?Sized>(
        &mut self,
        r: &R,
        tenant: &str,
        queue: &str,
        pid: Pid,
        busy: &Busy,
    ) -> crate::rsm::store::Result<Option<u64>> {
        if busy.queue(tenant, queue) || busy.pids.contains(&pid) {
            return Ok(None);
        }
        let live = match self.cycle.live.get(&pid) {
            Some(v) => *v,
            None => {
                let v = r.partition(pid)?.is_some() && r.garbage(pid)?.is_none();
                self.cycle.live.insert(pid, v);
                v
            }
        };
        Ok(live.then_some(pid % self.n))
    }

    /// Whether `(tenant, queue)` is committed (read once per cycle).
    fn queue_known<R: Reads + ?Sized>(
        &mut self,
        r: &R,
        tenant: &str,
        queue: &str,
    ) -> crate::rsm::store::Result<bool> {
        if let Some(v) = self.cycle.queues.get(tenant).and_then(|qs| qs.get(queue)) {
            return Ok(*v);
        }
        let v = r.queue(tenant, queue)?.is_some();
        self.cycle
            .queues
            .entry(tenant.to_string())
            .or_default()
            .insert(queue.to_string(), v);
        Ok(v)
    }

    /// Whether `(tenant, queue, group)` is committed (read once per cycle).
    fn group_known<R: Reads + ?Sized>(
        &mut self,
        r: &R,
        tenant: &str,
        queue: &str,
        group: &str,
    ) -> crate::rsm::store::Result<bool> {
        if let Some(v) = self
            .cycle
            .groups
            .get(tenant)
            .and_then(|qs| qs.get(queue))
            .and_then(|gs| gs.get(group))
        {
            return Ok(*v);
        }
        let v = r.group(tenant, queue, group)?.is_some();
        self.cycle
            .groups
            .entry(tenant.to_string())
            .or_default()
            .entry(queue.to_string())
            .or_default()
            .insert(group.to_string(), v);
        Ok(v)
    }

    /// The committed pid of `(tenant, queue, name)` and whether it is garbage
    /// (read once per cycle).
    fn committed_pid<R: Reads + ?Sized>(
        &mut self,
        r: &R,
        tenant: &str,
        queue: &str,
        name: &str,
    ) -> crate::rsm::store::Result<Option<(Pid, bool)>> {
        if let Some(v) = self
            .cycle
            .pids
            .get(tenant)
            .and_then(|qs| qs.get(queue))
            .and_then(|ps| ps.get(name))
        {
            return Ok(*v);
        }
        let v = match r.pid_of(tenant, queue, name)? {
            Some(pid) => Some((pid, r.garbage(pid)?.is_some())),
            None => None,
        };
        self.cycle
            .pids
            .entry(tenant.to_string())
            .or_default()
            .entry(queue.to_string())
            .or_default()
            .insert(name.to_string(), v);
        Ok(v)
    }

    /// The one lane that holds every partition of `(tenant, queue)` (committed,
    /// none being created), when there is one: a wildcard pop planned there
    /// sees the whole queue. `None` when the partitions span lanes, when there
    /// are none, or past [`WHOLE_SCAN_CAP`] of them.
    fn whole_lane<R: Reads + ?Sized>(
        &self,
        r: &R,
        tenant: &str,
        queue: &str,
        busy: &Busy,
    ) -> crate::rsm::store::Result<Option<u64>> {
        if busy.creating(tenant, queue) {
            return Ok(None);
        }
        let n = self.n;
        let (mut lane, mut one, mut seen) = (None, true, 0usize);
        r.scan_queue_partitions(tenant, queue, None, WHOLE_SCAN_CAP + 1, &mut |pid| {
            seen += 1;
            one = *lane.get_or_insert(pid % n) == pid % n;
            one
        })?;
        Ok(lane.filter(|_| one && seen <= WHOLE_SCAN_CAP))
    }

    /// [`LanesState::whole_lane`], read once per queue per cycle.
    fn whole_lane_once<R: Reads + ?Sized>(
        &mut self,
        r: &R,
        tenant: &str,
        queue: &str,
        busy: &Busy,
    ) -> crate::rsm::store::Result<Option<u64>> {
        let k = (tenant.to_string(), queue.to_string());
        if let Some(l) = self.cycle.whole.get(&k) {
            return Ok(*l);
        }
        let l = self.whole_lane(r, tenant, queue, busy)?;
        self.cycle.whole.insert(k, l);
        Ok(l)
    }

    /// Whether a wildcard pop of `key` may be planned in a lane at all: its
    /// queue is quiet (L1) and both the queue and the group are committed (a
    /// first contact registers the group, a catalog write only control plans).
    /// Read once per ring per cycle.
    fn wildcard_may_lane<R: Reads + ?Sized>(
        &mut self,
        r: &R,
        key: &RingKey,
        busy: &Busy,
    ) -> crate::rsm::store::Result<bool> {
        if let Some(ok) = self.cycle.wild.get(key) {
            return Ok(*ok);
        }
        let (t, q, g) = key;
        let ok = !busy.queue(t, q) && self.queue_known(r, t, q)? && self.group_known(r, t, q, g)?;
        self.cycle.wild.insert(key.clone(), ok);
        Ok(ok)
    }

    /// Where a wildcard pop of `key` goes when its queue spans lanes.
    ///
    /// Every lane leaves its job with the rows of each ring it keeps at their
    /// PLANNED state (its own claims, acks and appends in flight included):
    /// what it can give. The router sends the pop to the lane whose claimable
    /// partition has waited longest — across lanes the order each lane claims
    /// in, so a partition that is served again and again (acked, ready again at
    /// once) never keeps another lane's waiting; among equals, to the lane with
    /// the most left — and takes off what the pop may claim (`max_parts`), so
    /// two pops of a cycle do not race for one partition. When no lane has anything the pop is empty — the whole queue
    /// is — without being planned: a long-poll pop is answered at once and
    /// parks until a partition becomes claimable, a pop that does not wait goes
    /// to control, which re-reads every partition before answering empty. A
    /// ring some lane does not keep yet (its first cycle) is guessed
    /// round-robin; the lanes keep it from then on.
    fn wildcard_dest(&mut self, key: &RingKey, max_parts: i32, wait: bool) -> Dest {
        let now = self.cycle.now_us;
        let lanes = &self.lanes;
        let avail = self.cycle.avail.entry(key.clone()).or_insert_with(|| {
            let mut v = Vec::with_capacity(lanes.len());
            for s in lanes {
                let s = s.lock().expect("lane state");
                let view = s.rings.as_ref()?.view(key, now)?;
                v.push(LaneAvail { view, taken: 0 });
            }
            Some(v)
        });
        match avail {
            Some(v) => {
                // Past the instants a lane carries its partitions count as the
                // youngest, and among equals the lane with the most left wins:
                // a cycle's many pops still spread over the lanes.
                let mut best: Option<(i64, usize, usize)> = None;
                for (l, a) in v.iter().enumerate() {
                    let left = a.view.left;
                    if left == 0 {
                        continue;
                    }
                    let at = a.view.rows.get(a.taken).copied().unwrap_or(i64::MAX);
                    if best.is_none_or(|(b, bl, _)| at < b || (at == b && left > bl)) {
                        best = Some((at, left, l));
                    }
                }
                match best {
                    Some((_, _, l)) => {
                        let want = if max_parts <= 0 {
                            usize::MAX
                        } else {
                            max_parts as usize
                        };
                        let a = &mut v[l];
                        let take = want.min(a.view.left);
                        a.view.left -= take;
                        a.taken = a.taken.saturating_add(take);
                        Dest::Lane(l as u64)
                    }
                    None if wait => Dest::Empty,
                    None => Dest::Control,
                }
            }
            None => {
                let c = self.rr.entry(key.clone()).or_insert(0);
                let l = *c % self.n;
                *c = c.wrapping_add(1);
                Dest::Lane(l)
            }
        }
    }

    /// A renew goes to the one lane that holds every live committed lease of
    /// its worker. What the router cannot see from committed state goes to
    /// control, which sees it all: a lease write of the worker in flight or in
    /// this batch, leases in more than one lane or none, a catalog change in
    /// flight.
    fn renew_dest<R: Reads + ?Sized>(
        &mut self,
        r: &R,
        c: &RenewCommand,
        busy: &Busy,
    ) -> crate::rsm::store::Result<Dest> {
        if busy.workers.contains(&c.worker)
            || self.cycle.pop_workers.contains(&c.worker)
            || !busy.queues.is_empty()
            || !busy.tenants.is_empty()
        {
            return Ok(Dest::Control);
        }
        let (n, now) = (self.n, self.cycle.now_us);
        let mut lane: Option<u64> = None;
        let mut one = true;
        r.scan_worker_leases(&c.worker, usize::MAX, &mut |pid, _group, exp| {
            if exp <= now {
                return true;
            }
            if busy.pids.contains(&pid) || *lane.get_or_insert(pid % n) != pid % n {
                one = false;
                return false;
            }
            true
        })?;
        Ok(match lane {
            Some(l) if one => Dest::Lane(l),
            _ => Dest::Control,
        })
    }
}

/// Plan one lane's commands (on its thread): its kept overlay advanced over its
/// slices of the in-flight entries, its rings filtered to its partitions, the
/// router's clock. Each command carries whether this lane holds its whole
/// queue ([`Dest::Whole`]).
#[allow(clippy::too_many_arguments)]
fn plan_lane<S: Store>(
    store: &S,
    front: &DedupFront,
    st: &mut LaneState,
    lane: u64,
    n: u64,
    keep: &KeepCfg,
    reader: Option<Reader>,
    qlog_reader: Option<QLogReader>,
    folded: &[(u64, Arc<Entry>)],
    cmds: Vec<(usize, Command, bool)>,
    cfg: PlanConfig,
    now_us: i64,
    ring_keys: &[RingKey],
    cycle_no: u64,
) -> crate::rsm::store::Result<LaneOut> {
    let prior = if keep.enabled { st.kept.take() } else { None };
    let mut rings = if keep.enabled { st.rings.take() } else { None };
    store.read(|r| {
        let store_applied = r.applied_index()?;
        let base_pid = r.next_pid()?;
        let base_kv = r.kv_version_next()?;
        let empty = Derived::default();
        let base_now = Committed::new(r, &empty).plan_now(now_us)?;
        let mut kept = match prior.map(|k| k.advance(folded, store_applied, base_pid, base_kv)) {
            Some(Ok(k)) => k,
            _ => KeptOverlay::rebuild(folded, store_applied, base_pid, base_kv),
        };
        let pr = rings.get_or_insert_with(|| {
            let mut p = PlanRings::new(store_applied, base_now);
            p.set_lane(lane, n);
            p
        });
        pr.advance(r, folded, store_applied, base_now)?;
        for k in ring_keys {
            pr.ensure(r, k, cycle_no)?;
        }
        if cycle_no.is_multiple_of(RING_EVICT_EVERY) {
            pr.evict_idle(cycle_no.saturating_sub(RING_IDLE_CYCLES));
        }
        // The rows this lane's entries still in flight move: `advance` just
        // read the landed truth into the rings, and they go back to their
        // planned state below, every cycle, until their entries land.
        let mut touched = Touched::default();
        for (index, e) in folded {
            if *index > store_applied {
                touched.note(&e.effects);
            }
        }
        let derived = Derived::default();
        let committed = Committed::with_plan_rings(r, &derived, pr);
        st.tag = kept.begin_cycle();
        let ov = kept.overlay_mut();
        ov.mark_cycle_start();
        let mut planner = Planner::new(committed, now_us, cfg.clone(), front, reader.clone());
        planner.set_qlog_reader(qlog_reader.clone());

        let mut out = LaneOut {
            slots: Vec::with_capacity(cmds.len()),
            poisoned: false,
            part: Entry::with_capacity(now_us, base_pid, base_kv, cmds.len()),
            sub: Entry::with_capacity(now_us, base_pid, base_kv, cmds.len()),
        };
        let mut seen: HashSet<RequestId> = HashSet::new();
        let start = std::time::Instant::now();
        let budget = std::time::Duration::from_millis(cfg.plan_budget_ms);
        let mut cut = false;
        for (pos, cmd, whole) in cmds {
            if cut {
                out.slots.push((pos, LaneSlot::Deferred(Box::new(cmd))));
                continue;
            }
            let id = cmd.request_id();
            if seen.contains(&id) {
                out.slots.push((pos, LaneSlot::SameCycle(id)));
                continue;
            }
            let folds0 = ov.folds();
            let slot = match cmd.plan(&planner, ov) {
                Ok(Plan::Logged { effects, outcome }) => {
                    let catalog = effects.iter().any(|e| {
                        matches!(
                            e,
                            Effect::PartitionCreate { .. }
                                | Effect::QueueUpsert { .. }
                                | Effect::GroupUpsert { .. }
                                | Effect::QueueDelete { .. }
                                | Effect::GroupDelete { .. }
                        )
                    });
                    if catalog {
                        // L1 broke under us: the folds are not kept, control
                        // plans the command again.
                        out.poisoned = true;
                        LaneSlot::Misrouted(Box::new(cmd))
                    } else {
                        if ov.folds() - folds0 != effects.len() as u64 {
                            out.poisoned = true;
                        }
                        touched.note(&effects);
                        let stripped: Vec<Effect> = effects.iter().map(strip).collect();
                        match out
                            .sub
                            .add_command(id, outcome.clone(), stripped)
                            .and_then(|()| out.part.add_command(id, outcome, effects))
                        {
                            Ok(()) => {
                                seen.insert(id);
                                LaneSlot::Logged(id)
                            }
                            Err(e) => {
                                // Folded but not logged: the lane keeps
                                // nothing of this cycle.
                                out.poisoned = true;
                                LaneSlot::Refused(Refusal::retry(
                                    "internal",
                                    format!("entry build: {e:?}"),
                                ))
                            }
                        }
                    }
                }
                Ok(Plan::Empty(outcome)) => {
                    if ov.folds() != folds0 {
                        out.poisoned = true;
                    }
                    if matches!(&cmd, Command::PopWildcard(_)) && !whole {
                        // Nothing ready among THIS lane's partitions, and other
                        // lanes hold some of the queue: the pop is empty only
                        // when the whole queue is, so control plans it with
                        // every partition. A long-poll pop too: parked on this
                        // false empty, it is woken by the next append and
                        // guessed again (on 8 lanes, one claim per ~8 wakes,
                        // measured 2026-09-25).
                        LaneSlot::Misrouted(Box::new(cmd))
                    } else {
                        LaneSlot::Empty(outcome)
                    }
                }
                Ok(Plan::Refused(refusal)) | Err(refusal) => {
                    if ov.folds() != folds0 {
                        out.poisoned = true;
                    }
                    LaneSlot::Refused(refusal)
                }
            };
            out.slots.push((pos, slot));
            cut = start.elapsed() > budget;
        }
        // Move every touched row of the kept rings to its planned state — what
        // apply will write once the entries land — so what the lane hands the
        // router counts its own claims, acks and appends in flight: a pop is
        // sent where a partition is claimable, and to no lane when none is.
        let mut moves: Vec<(String, String, String, Pid, Option<i64>)> = Vec::new();
        let mut row = |t: String, q: String, g: &str, pid: Pid, state: PlannedPending| match state {
            PlannedPending::Keep => {}
            PlannedPending::Clear => moves.push((t, q, g.to_string(), pid, None)),
            PlannedPending::At(at) => moves.push((t, q, g.to_string(), pid, Some(at))),
        };
        for pid in &touched.appended {
            let Ok(Some((t, q))) = planner.partition_queue(ov, *pid) else {
                continue;
            };
            for g in pr.ring_groups(&t, &q) {
                if let Ok(Some((t, q, state))) = planner.planned_pending(ov, *pid, &g, true) {
                    row(t, q, &g, *pid, state);
                }
            }
        }
        for (pid, g) in &touched.cursors {
            if touched.appended.contains(pid) {
                continue; // every kept group of its queue, just above
            }
            if let Ok(Some((t, q, state))) = planner.planned_pending(ov, *pid, g, false) {
                row(t, q, g, *pid, state);
            }
        }
        drop(planner);
        for (t, q, g, pid, at) in moves {
            pr.plan_set(&t, &q, &g, pid, at);
        }
        st.kept = Some(kept);
        st.rings = rings.take();
        Ok(out)
    })
}

/// A cycle with lanes: route, plan the lanes in parallel, plan control, merge
/// into one entry. Same inputs and output as [`super::plan_cycle_blocking`].
#[allow(clippy::too_many_arguments)]
pub(crate) fn plan_cycle_lanes<S: Store + 'static>(
    store: &Arc<S>,
    front: &Arc<DedupFront>,
    ls: &mut LanesState,
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
) -> crate::rsm::store::Result<PlanOutput> {
    let n = ls.n;
    if !keep.enabled || ls.epoch != keep.epoch {
        ls.reset();
        ls.epoch = keep.epoch;
    }
    ls.cycles += 1;
    let cycle_no = ls.cycles;
    if keep.reset_every > 0 && cycle_no.is_multiple_of(keep.reset_every.min(KEEP_RESET_EVERY)) {
        for l in &ls.lanes {
            let mut s = l.lock().expect("lane state");
            s.kept = None;
            s.rings = None;
        }
    }
    ls.sync(&folded);
    let busy = ls.busy();
    let batch_len = batch.len();

    // ---- the router ------------------------------------------------------
    let t_router = std::time::Instant::now();
    let mut slots: Vec<Option<Slot>> = (0..batch_len).map(|_| None).collect();
    let mut per_lane: Vec<Vec<(usize, Command, bool)>> = (0..n).map(|_| Vec::new()).collect();
    let mut control: Vec<(usize, Command)> = Vec::new();
    let mut creates: Vec<(usize, Command)> = Vec::new();
    // The rings this cycle's wildcard pops read: every lane keeps them (and
    // the router reads them there) from this cycle on.
    let mut ring_keys: Vec<RingKey> = Vec::new();
    let mut ring_seen: HashSet<RingKey> = HashSet::new();
    let (store_applied, base_pid, base_kv, now_us) = {
        let st = store.clone();
        st.read(|r| {
            let store_applied = r.applied_index()?;
            let base_pid = r.next_pid()?;
            let base_kv = r.kv_version_next()?;
            let empty = Derived::default();
            let base_now = Committed::new(r, &empty).plan_now(wall_us)?;
            // L2: one clock for the cycle, above everything in flight.
            let now_us = ls.records.iter().fold(base_now, |m, rec| {
                m.max(rec.now_us.saturating_add(1))
                    .max(rec.created_hi.saturating_add(1))
            });
            let lookup = Planner::new(
                Committed::new(r, &empty),
                now_us,
                cfg.clone(),
                front,
                reader.clone(),
            );
            let no_overlay = Overlay::new(base_pid, base_kv);
            ls.cycle = RouterCycle {
                now_us,
                ..RouterCycle::default()
            };
            // A request id seen twice in one batch (a client retry) goes where
            // its first copy went: one planner sees both, logs it once and
            // answers the second as a same-cycle hit.
            let mut routed: HashMap<RequestId, Dest> = HashMap::new();
            for (pos, cmd) in batch.into_iter().enumerate() {
                let id = cmd.request_id();
                match lookup.lookup_request_id(&no_overlay, &id) {
                    Err(refusal) => {
                        slots[pos] = Some(Slot::Immediate(Reply::Refused(refusal)));
                        continue;
                    }
                    Ok(Lookup::Committed(outcome)) => {
                        slots[pos] = Some(Slot::Immediate(Reply::Done { outcome, at: None }));
                        continue;
                    }
                    Ok(_) => {}
                }
                if let Some(outcome) = ls.ids.get(&id) {
                    slots[pos] = Some(Slot::InFlightHit {
                        request_id: id,
                        outcome: outcome.clone(),
                    });
                    continue;
                }
                let dest = match routed.get(&id) {
                    Some(d) => *d,
                    None => {
                        let d = ls.route(r, &cmd, &busy)?;
                        routed.insert(id, d);
                        d
                    }
                };
                if let Command::PopWildcard(p) = &cmd {
                    if matches!(dest, Dest::Lane(_) | Dest::Whole(_) | Dest::Empty) {
                        let k = (p.tenant.clone(), p.queue.clone(), p.group.clone());
                        if !ring_seen.contains(&k) {
                            ring_seen.insert(k.clone());
                            ring_keys.push(k);
                        }
                    }
                }
                match dest {
                    Dest::Lane(l) | Dest::Whole(l) => {
                        per_lane[l as usize].push((pos, cmd, dest == Dest::Whole(l)));
                    }
                    Dest::Control => control.push((pos, cmd)),
                    Dest::Create => creates.push((pos, cmd)),
                    Dest::Empty => {
                        slots[pos] = Some(Slot::Empty(Outcome::Pop(PopOutcome::default())));
                    }
                }
            }
            Ok((store_applied, base_pid, base_kv, now_us))
        })?
    };

    add(&LANE_STATS.router_us, t_router.elapsed().as_micros() as u64);
    // ---- the lanes, in parallel -------------------------------------------
    let t_lanes = std::time::Instant::now();
    let lane_folded: Vec<Vec<(u64, Arc<Entry>)>> = (0..n as usize)
        .map(|l| {
            folded
                .iter()
                .zip(ls.records.iter())
                .map(|((i, _), rec)| (*i, rec.subs[l].clone()))
                .collect()
        })
        .collect();
    let mut waits = Vec::new();
    let mut ran = vec![false; n as usize];
    for (l, cmds) in per_lane.into_iter().enumerate() {
        if cmds.is_empty() && ring_keys.is_empty() {
            continue;
        }
        ran[l] = true;
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let store = store.clone();
        let front = front.clone();
        let state = ls.lanes[l].clone();
        let reader = reader.clone();
        let qlog_reader = qlog_reader.clone();
        let fl = lane_folded[l].clone();
        let cfg = cfg.clone();
        let keys = ring_keys.clone();
        let job = Box::new(move || {
            let w0 = std::time::Instant::now();
            let c0 = crate::rsm::timing::thread_cpu_ns();
            let mut st = state.lock().expect("lane state");
            let res = plan_lane(
                &*store,
                &front,
                &mut st,
                l as u64,
                n,
                &keep,
                reader,
                qlog_reader,
                &fl,
                cmds,
                cfg,
                now_us,
                &keys,
                cycle_no,
            );
            drop(st);
            add(&LANE_STATS.job_wall_us, w0.elapsed().as_micros() as u64);
            add(
                &LANE_STATS.job_cpu_us,
                crate::rsm::timing::thread_cpu_ns().saturating_sub(c0) / 1000,
            );
            add(&LANE_STATS.jobs, 1);
            let _ = tx.send(res);
        });
        if ls.workers[l].tx.send(job).is_err() {
            return Err(crate::rsm::store::StoreError::Io(
                "a lane thread is gone".into(),
            ));
        }
        waits.push((l, rx));
    }
    let mut lane_outs: Vec<Option<LaneOut>> = (0..n).map(|_| None).collect();
    let mut failed: Option<crate::rsm::store::StoreError> = None;
    for (l, rx) in waits {
        match rx.recv() {
            Ok(Ok(out)) => lane_outs[l] = Some(out),
            Ok(Err(e)) => failed = Some(e),
            Err(_) => failed = Some(crate::rsm::store::StoreError::Io("a lane job died".into())),
        }
    }
    if let Some(e) = failed {
        // Nothing of this cycle is kept anywhere: every lane rebuilds.
        ls.reset();
        return Err(e);
    }

    // The lanes' results: their parts of the entry (its first part, in lane
    // order), the rest answered or handed to control.
    let mut lane_parts: Vec<Option<(Entry, Entry)>> = (0..n).map(|_| None).collect();
    let mut lane_poisoned = vec![false; n as usize];
    for (l, out) in lane_outs.into_iter().enumerate() {
        let Some(out) = out else { continue };
        lane_poisoned[l] = out.poisoned;
        if !out.part.commands.is_empty() {
            lane_parts[l] = Some((out.part, out.sub));
        }
        for (pos, s) in out.slots {
            match s {
                LaneSlot::Logged(id) => slots[pos] = Some(Slot::Logged(id)),
                LaneSlot::SameCycle(id) => slots[pos] = Some(Slot::SameCycle(id)),
                LaneSlot::Empty(o) => slots[pos] = Some(Slot::Empty(o)),
                LaneSlot::Refused(r) => slots[pos] = Some(Slot::Immediate(Reply::Refused(r))),
                LaneSlot::Deferred(c) => slots[pos] = Some(Slot::Deferred(c)),
                LaneSlot::Misrouted(c) => control.push((pos, *c)),
            }
        }
    }
    control.sort_by_key(|(pos, _)| *pos);
    // A lane that folded what it did not log keeps nothing of this cycle, entry
    // or not: it rebuilds from its slices next time.
    for (l, p) in lane_poisoned.iter().enumerate() {
        if *p {
            ls.lanes[l].lock().expect("lane state").kept = None;
        }
    }
    let lane_cmds: u64 = lane_parts
        .iter()
        .flatten()
        .map(|(part, _)| part.commands.len() as u64)
        .sum();
    let control_cmds = control.len() as u64;
    LANE_STATS
        .lane
        .fetch_add(lane_cmds, std::sync::atomic::Ordering::Relaxed);
    LANE_STATS
        .control
        .fetch_add(control_cmds, std::sync::atomic::Ordering::Relaxed);
    LANE_STATS.maybe_log();

    add(&LANE_STATS.lanes_us, t_lanes.elapsed().as_micros() as u64);
    // ---- creations ----------------------------------------------------------
    // The pushes whose partition does not exist yet (`Dest::Create`), planned
    // here in batch order on a fresh overlay over the cycle's bases: the router
    // made sure their queue is committed and quiet and their partition neither
    // committed nor created in flight, so each reads committed state and the
    // creations before it — neither the lanes' view nor control's, and nothing
    // is rebuilt. Their partitions take the cycle's first ids (I18: an entry's
    // creates are dense from `pid_base`; they come right after the lanes'
    // commands, which create none), control's come after. A push that plans
    // anything but its new partition and frames goes to control, and every
    // creation behind it with it (their ids would have followed its).
    let t_create = std::time::Instant::now();
    let cycle_pid_base = ls.records.iter().map(|r| r.pid_hi).fold(base_pid, u64::max);
    let cycle_kv_base = ls.records.iter().map(|r| r.kv_hi).fold(base_kv, u64::max);
    let mut created: Vec<(RequestId, Outcome, Vec<Effect>)> = Vec::new();
    if !creates.is_empty() {
        let mut spill: Vec<(usize, Command)> = Vec::new();
        let st = store.clone();
        st.read(|r| {
            let d = Derived::default();
            let mut planner = Planner::new(
                Committed::new(r, &d),
                now_us,
                cfg.clone(),
                front,
                reader.clone(),
            );
            planner.set_qlog_reader(qlog_reader.clone());
            let mut ov = Overlay::new(cycle_pid_base, cycle_kv_base);
            ov.mark_cycle_start();
            let mut seen: HashSet<RequestId> = HashSet::new();
            let mut ours: HashSet<Pid> = HashSet::new();
            for (pos, cmd) in std::mem::take(&mut creates) {
                if !spill.is_empty() {
                    spill.push((pos, cmd));
                    continue;
                }
                let id = cmd.request_id();
                if seen.contains(&id) {
                    slots[pos] = Some(Slot::SameCycle(id));
                    continue;
                }
                match cmd.plan(&planner, &mut ov) {
                    Ok(Plan::Logged { effects, outcome }) => {
                        for e in &effects {
                            if let Effect::PartitionCreate { pid, .. } = e {
                                ours.insert(*pid);
                            }
                        }
                        let own = effects.iter().all(|e| match e {
                            Effect::PartitionCreate { .. } => true,
                            Effect::Append { pid, .. } => ours.contains(pid),
                            _ => false,
                        });
                        if own {
                            seen.insert(id);
                            slots[pos] = Some(Slot::Logged(id));
                            created.push((id, outcome, effects));
                        } else {
                            spill.push((pos, cmd));
                        }
                    }
                    Ok(Plan::Empty(o)) => slots[pos] = Some(Slot::Empty(o)),
                    Ok(Plan::Refused(refusal)) | Err(refusal) => {
                        slots[pos] = Some(Slot::Immediate(Reply::Refused(refusal)))
                    }
                }
            }
            Ok(())
        })?;
        if !spill.is_empty() {
            control.extend(spill);
            control.sort_by_key(|(pos, _)| *pos);
        }
    }
    LANE_STATS
        .create
        .fetch_add(created.len() as u64, std::sync::atomic::Ordering::Relaxed);
    add(&LANE_STATS.create_us, t_create.elapsed().as_micros() as u64);
    // ---- control ------------------------------------------------------------
    let t_control = std::time::Instant::now();
    let needs_control = !control.is_empty()
        || fire.is_some()
        || maintenance.is_some()
        || kv_sweep_limit.is_some()
        || expire_window_us.is_some();
    let mut control_logged: Vec<(RequestId, Outcome, Vec<Effect>)> = Vec::new();
    let mut pid_base = cycle_pid_base;
    let mut kv_base = cycle_kv_base;
    let (mut expired, mut expire_more, mut kv_swept, mut fired, mut fire_more) =
        (false, false, false, false, false);
    let (mut maintained, mut maintenance_more) = (false, false);
    if needs_control {
        let st = store.clone();
        st.read(|r| {
            let mut kept = KeptOverlay::rebuild(&folded, store_applied, base_pid, base_kv);
            let _tag = kept.begin_cycle();
            let ov = kept.overlay_mut();
            ov.mark_cycle_start();
            pid_base = ov.cycle_pid_base();
            kv_base = ov.cycle_kv_base();
            // This cycle's lane effects come first in the entry: control plans
            // after them, seeing them.
            for (part, _) in lane_parts.iter().flatten() {
                ov.apply_effects(&part.effects);
            }
            // Then the creations: control's own partitions take the ids after.
            for (_, _, effects) in &created {
                ov.apply_effects(effects);
            }
            let control_keys: Option<Vec<RingKey>> = if control
                .iter()
                .any(|(_, c)| matches!(c, Command::PopDiscover(_)))
            {
                None
            } else {
                let mut keys: Vec<RingKey> = control
                    .iter()
                    .filter_map(|(_, c)| match c {
                        Command::PopWildcard(p) => {
                            Some((p.tenant.clone(), p.queue.clone(), p.group.clone()))
                        }
                        _ => None,
                    })
                    .collect();
                keys.sort_unstable();
                keys.dedup();
                Some(keys)
            };
            let derived = Derived::rebuild_rings(r, now_us, control_keys.as_deref())?;
            let committed = Committed::new(r, &derived);
            let mut planner = Planner::new(committed, now_us, cfg.clone(), front, reader.clone());
            planner.set_qlog_reader(qlog_reader.clone());
            let mut entry = Entry::new(now_us, pid_base, kv_base);
            let mut seen: HashSet<RequestId> = lane_parts
                .iter()
                .flatten()
                .flat_map(|(part, _)| part.commands.iter().map(|c| c.request_id))
                .chain(created.iter().map(|(id, _, _)| *id))
                .collect();
            let start = std::time::Instant::now();
            let budget = std::time::Duration::from_millis(cfg.plan_budget_ms);
            let mut cut = false;
            for (pos, cmd) in std::mem::take(&mut control) {
                if cut {
                    slots[pos] = Some(Slot::Deferred(Box::new(cmd)));
                    continue;
                }
                let id = cmd.request_id();
                if seen.contains(&id) {
                    slots[pos] = Some(Slot::SameCycle(id));
                    continue;
                }
                let slot = match cmd.plan(&planner, ov) {
                    Ok(Plan::Logged { effects, outcome }) => {
                        match entry.add_command(id, outcome.clone(), effects) {
                            Ok(()) => {
                                seen.insert(id);
                                Slot::Logged(id)
                            }
                            Err(e) => Slot::Immediate(Reply::Refused(Refusal::retry(
                                "internal",
                                format!("entry build: {e:?}"),
                            ))),
                        }
                    }
                    Ok(Plan::Empty(outcome)) => Slot::Empty(outcome),
                    Ok(Plan::Refused(refusal)) | Err(refusal) => {
                        Slot::Immediate(Reply::Refused(refusal))
                    }
                };
                slots[pos] = Some(slot);
                cut = start.elapsed() > budget;
            }
            // The leader steps, as the single planner orders them.
            fired = fire.is_some();
            if let Some(fcfg) = fire.as_ref() {
                fire_more = plan_fire_step(&planner, ov, &mut entry, fcfg);
            }
            maintained = maintenance.is_some();
            if let Some(mcfg) = maintenance.as_ref() {
                let mut planned = crate::rsm::maintenance::plan(r, now_us, mcfg)?;
                maintenance_more = planned.more;
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
            drop(planner);
            for c in &entry.commands {
                control_logged.push((
                    c.request_id,
                    c.outcome.clone(),
                    entry.effects_of(c).to_vec(),
                ));
            }
            Ok(())
        })?;
    }

    add(
        &LANE_STATS.control_us,
        t_control.elapsed().as_micros() as u64,
    );
    // ---- merge: ONE entry, lanes first, then control -------------------------
    let t_merge = std::time::Instant::now();
    // The lanes built their parts and slices on their own threads: the merge
    // moves them, in lane order, and stamps the slices with the entry's header.
    let mut full = Entry::with_capacity(
        now_us,
        pid_base,
        kv_base,
        lane_cmds as usize + created.len() + control_logged.len(),
    );
    let mut subs: Vec<Entry> = Vec::with_capacity(n as usize);
    for part in lane_parts {
        let mut sub = match part {
            Some((part, sub)) => {
                full.append_part(part);
                sub
            }
            None => Entry::new(now_us, pid_base, kv_base),
        };
        sub.now_us = now_us;
        sub.pid_base = pid_base;
        sub.kv_version_base = kv_base;
        subs.push(sub);
    }
    // The creations' and control's effects each lane must see (L4), in entry
    // order, folded into the lane's overlay under this cycle's tag and recorded
    // in its slice: a created partition's lane holds its create from now on.
    let mut routed: Vec<Vec<Effect>> = (0..n).map(|_| Vec::new()).collect();
    for (id, outcome, effects) in created.into_iter().chain(control_logged) {
        for e in &effects {
            match lanes_of(e, n) {
                LanesOf::One(l) => routed[l as usize].push(strip(e)),
                LanesOf::All => {
                    for rt in routed.iter_mut() {
                        rt.push(strip(e));
                    }
                }
                LanesOf::None => {}
            }
        }
        if let Err(e) = full.add_command(id, outcome, effects) {
            return Err(crate::rsm::store::StoreError::Io(format!(
                "control entry build: {e:?}"
            )));
        }
    }
    let entry = if full.commands.is_empty() {
        None
    } else {
        Some(Arc::new(full))
    };

    // The lanes keep this cycle: their slice (their commands, then control's
    // effects for them) becomes an in-flight entry of their kept overlay.
    if let Some(full) = &entry {
        let mut sub_arcs: Vec<Arc<Entry>> = Vec::with_capacity(n as usize);
        for (l, (mut sub, rt)) in subs.into_iter().zip(routed).enumerate() {
            let mut s = ls.lanes[l].lock().expect("lane state");
            if !rt.is_empty() {
                let id = crate::util::uuidv7_bytes();
                let _ = sub.add_command(id, Outcome::Empty, rt.clone());
            }
            let sub = Arc::new(sub);
            // A lane that planned this cycle is inside its cycle's tag; an idle
            // one starts its cycle here.
            let planned_tag = s.tag;
            let mut keep_it = false;
            if !lane_poisoned[l] {
                if let Some(kept) = s.kept.as_mut() {
                    let tag = if ran[l] {
                        planned_tag
                    } else {
                        kept.begin_cycle()
                    };
                    kept.overlay_mut().apply_effects(&rt);
                    keep_it = kept.exact() && kept.push_entry(sub.clone(), tag).is_ok();
                }
            }
            if !keep_it {
                s.kept = None;
            }
            sub_arcs.push(sub);
        }
        for c in &full.commands {
            ls.ids.insert(c.request_id, c.outcome.clone());
        }
        ls.records.push_back(record_of(full.clone(), sub_arcs));
    }

    add(&LANE_STATS.merge_us, t_merge.elapsed().as_micros() as u64);
    add(&LANE_STATS.cycles, 1);
    let slots: Vec<Slot> = slots
        .into_iter()
        .map(|s| {
            s.unwrap_or_else(|| {
                Slot::Immediate(Reply::Refused(Refusal::retry(
                    "internal",
                    "a command was lost between the lanes",
                )))
            })
        })
        .collect();
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
}

#[cfg(test)]
mod whole_lane_tests {
    //! Where the router sends a wildcard pop: to the lane that holds the whole
    //! queue when one does ([`Dest::Whole`], an empty answer there is final),
    //! else to a guessed lane ([`Dest::Lane`], whose empty answer control
    //! re-plans). Routing only: the batcher tests
    //! (`lanes_a_woken_long_poll_pop_*`) prove what a woken pop claims.
    use super::*;
    use crate::rsm::tests::planner_harness::{self as h, Cell};

    const N: u64 = 8;

    fn wildcard(id: u64, queue: &str) -> Command {
        match h::pop_wildcard(id, queue, "g", "w") {
            h::Cmd::PopWildcard(c) => Command::PopWildcard(c),
            _ => unreachable!(),
        }
    }

    /// Route `cmd` as the first command of a new cycle (the router reads the
    /// busy set and its per-cycle state afresh each cycle).
    fn route(ls: &mut LanesState, cell: &Cell, cmd: &Command) -> Dest {
        let busy = ls.busy();
        ls.cycle = RouterCycle::default();
        cell.node
            .store()
            .read(|r| ls.route(r, cmd, &busy))
            .expect("route")
    }

    #[test]
    fn a_wildcard_pop_goes_whole_only_to_a_lane_holding_every_partition() {
        let mut cell = Cell::new("lanes-whole");
        // "one": one partition. "two": two, with consecutive pids (two lanes).
        cell.run(&[
            h::push(1, "one", "p0", &["a"]),
            h::push(2, "two", "p0", &["b"]),
            h::push(3, "two", "p1", &["c"]),
        ]);
        cell.run(&[
            h::pop_wildcard(4, "one", "g", "w"),
            h::pop_wildcard(5, "two", "g", "w"),
        ]);
        let one = cell.pid_of("one", "p0").expect("one/p0");
        let mut ls = LanesState::new(N);

        assert_eq!(
            route(&mut ls, &cell, &wildcard(10, "one")),
            Dest::Whole(one % N)
        );
        assert!(
            matches!(route(&mut ls, &cell, &wildcard(11, "two")), Dest::Lane(_)),
            "partitions in two lanes: the lane is a guess"
        );

        // A partition of "one" being created: no lane holds all of it until
        // the entry lands.
        let entry = cell
            .plan_entry(&[h::push(12, "one", "p1", &["d"])], None)
            .expect("an entry that creates one/p1");
        let subs = split(&entry, N);
        ls.records.push_back(record_of(Arc::new(entry), subs));
        assert!(
            matches!(route(&mut ls, &cell, &wildcard(13, "one")), Dest::Lane(_)),
            "a partition in flight: the lane is a guess"
        );
    }
}

#[cfg(test)]
mod router_tests {
    //! The router's wildcard and renew decisions from what the lanes publish,
    //! and the planned rows a lane publishes (`Planner::planned_pending`).
    use super::*;
    use crate::rsm::planner::AckStatus;
    use crate::rsm::tests::planner_harness::{self as h, Cell, TENANT};

    const N: u64 = 8;

    fn wildcard(id: u64, queue: &str, max_parts: i32, wait: bool) -> Command {
        match h::pop_wildcard_with(id, queue, "g", "w", |c| {
            c.max_parts = max_parts;
            c.wait = wait;
        }) {
            h::Cmd::PopWildcard(c) => Command::PopWildcard(c),
            _ => unreachable!(),
        }
    }

    fn pinned(id: u64, queue: &str, partition: &str, worker: &str) -> Command {
        match h::pop_pinned(id, queue, partition, "g", worker) {
            h::Cmd::PopPinned(c) => Command::PopPinned(c),
            _ => unreachable!(),
        }
    }

    fn renew(id: u64, worker: &str) -> Command {
        match h::renew(id, worker, 30) {
            h::Cmd::Renew(c) => Command::Renew(c),
            _ => unreachable!(),
        }
    }

    /// A new router cycle at the cell's clock.
    fn cycle(ls: &mut LanesState, cell: &Cell) {
        ls.cycle = RouterCycle {
            now_us: cell.now(),
            ..RouterCycle::default()
        };
    }

    /// Route `cmd` inside the current cycle.
    fn route(ls: &mut LanesState, cell: &Cell, cmd: &Command) -> Dest {
        let busy = ls.busy();
        cell.node
            .store()
            .read(|r| ls.route(r, cmd, &busy))
            .expect("route")
    }

    /// The ring `(queue, "g")` every lane keeps, as `(pid, ready_at)` rows
    /// split at `split_us` (at or before it ready, after it deferred); `None`:
    /// that lane does not keep it.
    fn publish(ls: &LanesState, queue: &str, split_us: i64, per_lane: &[Option<Vec<(Pid, i64)>>]) {
        let key = (TENANT.to_string(), queue.to_string(), "g".to_string());
        for (l, rows) in per_lane.iter().enumerate() {
            let mut s = ls.lanes[l].lock().expect("lane state");
            s.rings = rows.as_ref().map(|rows| {
                let mut pr = PlanRings::new(0, split_us);
                pr.keep_rows(&key, rows);
                pr
            });
        }
    }

    /// Every lane keeps the ring, with no rows.
    fn kept_empty() -> Vec<Option<Vec<(Pid, i64)>>> {
        vec![Some(Vec::new()); N as usize]
    }

    /// Queue "spread": partitions "a" and "b" in two lanes, group "g"
    /// registered (its first pop took both backlogs).
    fn spread(cell: &mut Cell) -> (Pid, Pid) {
        cell.run(&[
            h::push(1, "spread", "a", &["a0"]),
            h::push(2, "spread", "b", &["b0"]),
        ]);
        cell.run(&[h::pop_wildcard(3, "spread", "g", "w")]);
        let pa = cell.pid_of("spread", "a").expect("a");
        let pb = cell.pid_of("spread", "b").expect("b");
        assert_ne!(pa % N, pb % N, "two lanes");
        (pa, pb)
    }

    #[test]
    fn a_wildcard_pop_goes_where_a_partition_is_claimable_and_reserves_it() {
        let mut cell = Cell::new("router-reserve");
        let (pa, _) = spread(&mut cell);
        let la = (pa % N) as usize;
        let mut ls = LanesState::new(N);
        let now = cell.now();
        let mut per_lane = kept_empty();
        per_lane[la] = Some(vec![(pa, now - 10), (pa + N, now - 5)]);
        publish(&ls, "spread", now, &per_lane);

        cycle(&mut ls, &cell);
        for id in [10, 11] {
            assert_eq!(
                route(&mut ls, &cell, &wildcard(id, "spread", 1, true)),
                Dest::Lane(la as u64),
                "the lane with the claimable partitions"
            );
        }
        assert_eq!(
            route(&mut ls, &cell, &wildcard(12, "spread", 1, true)),
            Dest::Empty,
            "both reserved: nothing left anywhere, the long-poll pop is empty at once"
        );
        assert_eq!(
            route(&mut ls, &cell, &wildcard(13, "spread", 1, false)),
            Dest::Control,
            "a pop that does not wait: control re-reads every partition"
        );

        // The next cycle reserves from what the lanes publish again; a pop with
        // no partition limit reserves everything its lane has.
        cycle(&mut ls, &cell);
        assert_eq!(
            route(&mut ls, &cell, &wildcard(14, "spread", 0, true)),
            Dest::Lane(la as u64)
        );
        assert_eq!(
            route(&mut ls, &cell, &wildcard(15, "spread", 1, true)),
            Dest::Empty
        );
    }

    #[test]
    fn a_ring_some_lane_does_not_keep_is_guessed_and_every_due_lease_counts() {
        let mut cell = Cell::new("router-guess");
        let (_, pb) = spread(&mut cell);
        let lb = (pb % N) as usize;
        let mut ls = LanesState::new(N);

        // Lane 0 does not keep the ring yet: the router cannot know, it guesses.
        let now = cell.now();
        let mut per_lane = kept_empty();
        per_lane[0] = None;
        publish(&ls, "spread", now, &per_lane);
        cycle(&mut ls, &cell);
        assert!(matches!(
            route(&mut ls, &cell, &wildcard(20, "spread", 1, true)),
            Dest::Lane(_)
        ));

        // Every lane keeps it; lane `lb` deferred three leases when its job
        // ran: two are due by the router's clock and one is not, two
        // partitions to give.
        let mut per_lane = kept_empty();
        per_lane[lb] = Some(vec![
            (pb, now - 2),
            (pb + N, now - 1),
            (pb + 2 * N, now + 60_000_000),
        ]);
        publish(&ls, "spread", now - 1_000, &per_lane);
        cycle(&mut ls, &cell);
        for id in [21, 22] {
            assert_eq!(
                route(&mut ls, &cell, &wildcard(id, "spread", 1, true)),
                Dest::Lane(lb as u64),
                "an expired lease frees its partition"
            );
        }
        assert_eq!(
            route(&mut ls, &cell, &wildcard(23, "spread", 1, true)),
            Dest::Empty,
            "a lease not due yet frees nothing"
        );
    }

    #[test]
    fn a_wildcard_pop_goes_to_the_partition_that_has_waited_longest_across_lanes() {
        let mut cell = Cell::new("router-oldest");
        let (pa, pb) = spread(&mut cell);
        let (la, lb) = ((pa % N) as usize, (pb % N) as usize);
        let pid = |l: usize| if l == la { pa } else { pb };
        let mut ls = LanesState::new(N);
        let now = cell.now();

        // One partition ready for longer than the other: it goes first,
        // whichever lane comes first.
        for (young, old) in [(la, lb), (lb, la)] {
            let mut per_lane = kept_empty();
            per_lane[young] = Some(vec![(pid(young), now - 1)]);
            per_lane[old] = Some(vec![(pid(old), now - 50)]);
            publish(&ls, "spread", now, &per_lane);
            cycle(&mut ls, &cell);
            for (id, want) in [(30, Dest::Lane(old as u64)), (31, Dest::Lane(young as u64))] {
                assert_eq!(
                    route(&mut ls, &cell, &wildcard(id, "spread", 1, true)),
                    want
                );
            }
            assert_eq!(
                route(&mut ls, &cell, &wildcard(32, "spread", 1, true)),
                Dest::Empty
            );
        }

        // One consumer, one pop a cycle, both partitions always holding work:
        // each ack makes its partition ready again at once, the youngest. The
        // pops alternate; a lower lane does not win every tie while the other
        // partition waits for good.
        let mut at = [now - 2_000, now - 1_990];
        let mut served = Vec::new();
        for id in 40..46u64 {
            let mut per_lane = kept_empty();
            per_lane[la] = Some(vec![(pa, at[0])]);
            per_lane[lb] = Some(vec![(pb, at[1])]);
            publish(&ls, "spread", now, &per_lane);
            cycle(&mut ls, &cell);
            let Dest::Lane(l) = route(&mut ls, &cell, &wildcard(id, "spread", 1, true)) else {
                panic!("a lane has work");
            };
            let l = l as usize;
            served.push(l);
            at[usize::from(l == lb)] = now - 1_000 + id as i64;
        }
        assert_eq!(served, [la, lb, la, lb, la, lb]);
    }

    #[test]
    fn many_pops_of_a_ring_in_one_cycle_go_oldest_first_then_spread_over_the_lanes() {
        let mut cell = Cell::new("router-spread");
        let (pa, pb) = spread(&mut cell);
        let (la, lb) = ((pa % N) as usize, (pb % N) as usize);
        let mut ls = LanesState::new(N);
        let now = cell.now();
        // Twenty ready partitions in each lane, lane `la`'s all older.
        let rows = |pid: Pid, from: i64| -> Vec<(Pid, i64)> {
            (0..20)
                .map(|i| (pid + (i + 1) as u64 * N, from + i))
                .collect()
        };
        let mut per_lane = kept_empty();
        per_lane[la] = Some(rows(pa, now - 100));
        per_lane[lb] = Some(rows(pb, now - 50));
        publish(&ls, "spread", now, &per_lane);
        cycle(&mut ls, &cell);
        let served: Vec<u64> = (0..30u64)
            .map(
                |i| match route(&mut ls, &cell, &wildcard(50 + i, "spread", 1, true)) {
                    Dest::Lane(l) => l,
                    other => panic!("pop {i}: {other:?}"),
                },
            )
            .collect();
        let win = crate::rsm::state::RING_VIEW_ROWS;
        assert!(
            served[..win].iter().all(|l| *l == la as u64),
            "the oldest first: {served:?}"
        );
        let to = |l: usize| served.iter().filter(|x| **x == l as u64).count();
        assert_eq!((to(la), to(lb)), (15, 15), "then balanced: {served:?}");
    }

    #[test]
    fn a_renew_goes_to_the_one_lane_holding_its_workers_leases() {
        let mut cell = Cell::new("router-renew");
        cell.run(&[h::push(1, "q", "a", &["a0"]), h::push(2, "q", "b", &["b0"])]);
        let pa = cell.pid_of("q", "a").expect("a");
        let pb = cell.pid_of("q", "b").expect("b");
        assert_ne!(pa % N, pb % N, "two lanes");
        // w1 leases a; w2 (another group) leases both a and b.
        cell.run(&[h::pop_pinned(3, "q", "a", "g", "w1")]);
        cell.run(&[h::pop_wildcard_with(4, "q", "g2", "w2", |c| {
            c.max_parts = 0
        })]);
        let mut ls = LanesState::new(N);

        cycle(&mut ls, &cell);
        assert_eq!(route(&mut ls, &cell, &renew(10, "w1")), Dest::Lane(pa % N));
        assert_eq!(
            route(&mut ls, &cell, &renew(11, "w2")),
            Dest::Control,
            "leases in two lanes"
        );
        assert_eq!(
            route(&mut ls, &cell, &renew(12, "nobody")),
            Dest::Control,
            "no lease to renew: control answers"
        );

        // w1 pops earlier in the same batch: its renew goes to control, which
        // plans after the lanes and sees the new lease.
        cycle(&mut ls, &cell);
        let _ = route(&mut ls, &cell, &pinned(13, "q", "b", "w1"));
        assert_eq!(route(&mut ls, &cell, &renew(14, "w1")), Dest::Control);

        // A lease write of w1 still in flight: control too.
        cycle(&mut ls, &cell);
        let entry = cell
            .plan_entry(&[h::pop_pinned(15, "q", "b", "g3", "w1")], None)
            .expect("an entry that leases b to w1");
        let subs = split(&entry, N);
        ls.records.push_back(record_of(Arc::new(entry), subs));
        assert_eq!(route(&mut ls, &cell, &renew(16, "w1")), Dest::Control);
    }

    fn push(id: u64, queue: &str, partition: &str) -> Command {
        match h::push(id, queue, partition, &["x"]) {
            h::Cmd::Push(c) => Command::Push(c),
            _ => unreachable!(),
        }
    }

    #[test]
    fn a_push_to_a_new_partition_goes_to_the_creation_step_and_one_created_in_flight_to_its_lane() {
        let mut cell = Cell::new("router-create");
        cell.run(&[h::push(1, "q", "a", &["a0"])]);
        let pa = cell.pid_of("q", "a").expect("a");
        let mut ls = LanesState::new(N);

        cycle(&mut ls, &cell);
        assert_eq!(
            route(&mut ls, &cell, &push(10, "q", "a")),
            Dest::Lane(pa % N)
        );
        assert_eq!(route(&mut ls, &cell, &push(11, "q", "b")), Dest::Create);
        assert_eq!(
            route(&mut ls, &cell, &push(12, "nope", "a")),
            Dest::Control,
            "the queue must be created: a catalog write"
        );

        // An entry in flight creates b: a push to it goes to b's lane, whose
        // slice of that entry holds the create.
        let entry = cell
            .plan_entry(&[h::push(13, "q", "b", &["b0"])], None)
            .expect("an entry that creates q/b");
        let pb = entry
            .effects
            .iter()
            .find_map(|e| match e {
                Effect::PartitionCreate { pid, .. } => Some(*pid),
                _ => None,
            })
            .expect("a create");
        let subs = split(&entry, N);
        ls.records.push_back(record_of(Arc::new(entry), subs));
        cycle(&mut ls, &cell);
        assert_eq!(
            route(&mut ls, &cell, &push(14, "q", "b")),
            Dest::Lane(pb % N)
        );
    }

    #[test]
    fn a_lane_ring_moves_with_its_planned_claim_and_ack_before_they_land() {
        let mut cell = Cell::new("router-planned");
        cell.run(&[h::push(1, "q", "a", &["a0"])]);
        let pa = cell.pid_of("q", "a").expect("a");
        cell.run(&[h::pop_wildcard(2, "q", "g", "w")]);
        cell.run(&[h::ack(3, pa, "q", "g", "w", &[("a0", AckStatus::Ok)])]);
        cell.run(&[h::push(4, "q", "a", &["a1"])]);
        let key = (TENANT.to_string(), "q".to_string(), "g".to_string());
        let wall = cell.now();
        cell.node
            .store()
            .read(|r| {
                let d0 = Derived::default();
                let now = Committed::new(r, &d0).plan_now(wall)?;
                let mut rings = PlanRings::new(r.applied_index()?, now);
                rings.set_lane(pa % N, N);
                rings.ensure(r, &key, 1)?;
                assert_eq!(rings.ready_len(&key), 1, "a1 waits for the group");

                // Claim a1 in the overlay only: nothing has landed.
                let d = Derived::rebuild(r, now)?;
                let mut ov = Overlay::new(r.next_pid()?, r.kv_version_next()?);
                ov.mark_cycle_start();
                let planner = Planner::new(
                    Committed::new(r, &d),
                    now,
                    PlanConfig::default(),
                    cell.front(),
                    None,
                );
                let pop = match h::pop_wildcard(5, "q", "g", "w2") {
                    h::Cmd::PopWildcard(c) => c,
                    _ => unreachable!(),
                };
                assert!(matches!(
                    planner.plan_pop_wildcard(&mut ov, &pop),
                    Ok(Plan::Logged { .. })
                ));
                let Ok(Some((t, q, state))) = planner.planned_pending(&ov, pa, "g", false) else {
                    panic!("a planned row");
                };
                let PlannedPending::At(expiry) = state else {
                    panic!("a live lease defers the row, got {state:?}");
                };
                assert!(expiry > now, "deferred to the lease expiry");
                rings.plan_set(&t, &q, "g", pa, Some(expiry));
                assert_eq!(rings.ready_len(&key), 0, "the lane no longer offers it");
                assert_eq!(
                    rings.view(&key, now).map(|v| v.left),
                    Some(0),
                    "not before the lease expires"
                );
                assert_eq!(
                    rings.view(&key, expiry),
                    Some(RingView {
                        left: 1,
                        rows: vec![expiry]
                    }),
                    "from its expiry on"
                );

                // Ack a1 in the overlay: the group is caught up, no row.
                let ack = match h::ack(6, pa, "q", "g", "w2", &[("a1", AckStatus::Ok)]) {
                    h::Cmd::Ack(c) => c,
                    _ => unreachable!(),
                };
                assert!(matches!(
                    planner.plan_ack(&mut ov, &ack),
                    Ok(Plan::Logged { .. })
                ));
                let Ok(Some((_, _, state))) = planner.planned_pending(&ov, pa, "g", false) else {
                    panic!("a planned row");
                };
                assert_eq!(state, PlannedPending::Clear);
                rings.plan_set(&t, &q, "g", pa, None);
                assert_eq!(rings.view(&key, expiry), Some(RingView::default()));
                Ok(())
            })
            .expect("read");
    }
}
