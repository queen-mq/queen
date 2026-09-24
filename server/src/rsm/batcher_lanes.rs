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
//! Invariants that make this equal to the single planner:
//!
//! - **L1** a lane plans only commands whose every partition is its own and
//!   already committed, on a queue with no catalog change or delete in flight;
//!   anything else goes to control ([`LanesState::route`]). A lane result that
//!   would create catalog state anyway is discarded and re-planned by control.
//! - **L2** one clock per cycle: the router stamps `now` for every lane and
//!   control from the committed floor and every in-flight entry (I5).
//! - **L3** pids stay on the one global counter: only control creates
//!   partitions, so the entry's `pid_base` is control's (I18 unchanged).
//! - **L4** a lane's overlay holds only its own partitions' effects (and the
//!   catalog): what it folds is recorded as its slice of the entry, and the
//!   slices of in-flight entries are what its kept overlay advances over.
//! - **L5** request ids are looked up once, by the router, before routing:
//!   committed first, then every in-flight entry.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use super::{
    plan_fire_step, KeepCfg, PlanOutput, Slot, KEEP_RESET_EVERY, RING_EVICT_EVERY,
    RING_IDLE_CYCLES,
};
use crate::rsm::dedup::DedupFront;
use crate::rsm::effect::{Effect, Pid};
use crate::rsm::entry::{Entry, Outcome, RequestId};
use crate::rsm::planner::kept::KeptOverlay;
use crate::rsm::planner::timers::TimerFireConfig;
use crate::rsm::planner::{Lookup, Overlay, Plan, PlanConfig, Planner, Refusal};
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::segments::Reader;
use crate::rsm::state::{Committed, Derived, PlanRings, RingKey};
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
    /// Ready partitions per wildcard ring after the lane's last walk: the
    /// router's hint for where to send a wildcard pop.
    ready: HashMap<RingKey, usize>,
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
    /// Round-robin cursor per wildcard ring, for the pops no hint places.
    rr: HashMap<RingKey, u64>,
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
        if lane + control > 0 {
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
                "lanes: commands planned since the last line (per-cycle means)",
            );
        }
    }
}

/// Where a command is planned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Dest {
    Lane(u64),
    Control,
}

/// How one command fared in a lane.
enum LaneSlot {
    Logged {
        id: RequestId,
        outcome: Outcome,
        effects: Vec<Effect>,
    },
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
    for e in &full.effects {
        match e {
            Effect::PartitionCreate {
                pid, created_at_us, ..
            } => {
                pid_hi = pid_hi.max(pid.saturating_add(1));
                created_hi = created_hi.max(*created_at_us);
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
    }
}

/// The in-flight catalog work commands must not race (L1).
struct Busy {
    queues: HashSet<(String, String)>,
    tenants: HashSet<String>,
    pids: HashSet<Pid>,
}

impl Busy {
    fn queue(&self, tenant: &str, queue: &str) -> bool {
        self.tenants.contains(tenant) || self.queues.contains(&(tenant.to_string(), queue.to_string()))
    }
}

impl LanesState {
    pub(crate) fn new(n: u64) -> LanesState {
        let n = n.max(2);
        LanesState {
            n,
            workers: (0..n as usize).map(LaneWorker::spawn).collect(),
            lanes: (0..n).map(|_| Arc::new(Mutex::new(LaneState::default()))).collect(),
            records: VecDeque::new(),
            ids: HashMap::new(),
            epoch: u64::MAX,
            cycles: 0,
            rr: HashMap::new(),
        }
    }

    fn reset(&mut self) {
        self.records.clear();
        self.ids.clear();
        for l in &self.lanes {
            let mut s = l.lock().expect("lane state");
            s.kept = None;
            s.rings = None;
            s.ready.clear();
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
                    self.ids.entry(c.request_id).or_insert_with(|| c.outcome.clone());
                }
                self.records.push_back(record_of(e.clone(), subs));
            }
        }
    }

    fn busy(&self) -> Busy {
        let mut b = Busy {
            queues: HashSet::new(),
            tenants: HashSet::new(),
            pids: HashSet::new(),
        };
        for r in &self.records {
            b.queues.extend(r.queues.iter().cloned());
            b.tenants.extend(r.tenants.iter().cloned());
            b.pids.extend(r.pids.iter().copied());
        }
        b
    }

    /// L1: where `cmd` is planned.
    fn route<R: Reads + ?Sized>(
        &mut self,
        r: &R,
        cmd: &Command,
        busy: &Busy,
    ) -> crate::rsm::store::Result<Dest> {
        let n = self.n;
        // The partition's lane when it is committed, live, and on a quiet queue.
        let part = |tenant: &str, queue: &str, name: &str| -> crate::rsm::store::Result<Option<u64>> {
            if busy.queue(tenant, queue) || r.queue(tenant, queue)?.is_none() {
                return Ok(None);
            }
            match r.pid_of(tenant, queue, name)? {
                Some(pid) if !busy.pids.contains(&pid) && r.garbage(pid)?.is_none() => {
                    Ok(Some(pid % n))
                }
                _ => Ok(None),
            }
        };
        let pid_lane = |tenant: &str, queue: &str, pid: Pid| -> crate::rsm::store::Result<Option<u64>> {
            if busy.queue(tenant, queue) || busy.pids.contains(&pid) {
                return Ok(None);
            }
            match r.partition(pid)? {
                Some(_) if r.garbage(pid)?.is_none() => Ok(Some(pid % n)),
                _ => Ok(None),
            }
        };
        let one = |lanes: &[Option<u64>]| -> Dest {
            match lanes.first() {
                Some(Some(l)) if lanes.iter().all(|x| *x == Some(*l)) => Dest::Lane(*l),
                _ => Dest::Control,
            }
        };
        Ok(match cmd {
            // A Kafka append from an idempotent producer rewrites its window,
            // a KV row: KV versions are handed out by control alone (I18).
            Command::Push(c) if crate::rsm::planner::kafka::writes_kv(c) => Dest::Control,
            Command::Push(c) => match part(&c.tenant, &c.queue, &c.partition)? {
                Some(l) => Dest::Lane(l),
                None => Dest::Control,
            },
            Command::PopPinned(c) => {
                if c.conflate
                    || r.group(&c.tenant, &c.queue, &c.group)?.is_none()
                    || c.partition.is_none()
                {
                    Dest::Control
                } else {
                    match part(&c.tenant, &c.queue, c.partition.as_deref().unwrap_or(""))? {
                        Some(l) => Dest::Lane(l),
                        None => Dest::Control,
                    }
                }
            }
            Command::PopWildcard(c) => {
                if c.conflate
                    || busy.queue(&c.tenant, &c.queue)
                    || r.queue(&c.tenant, &c.queue)?.is_none()
                    || r.group(&c.tenant, &c.queue, &c.group)?.is_none()
                {
                    Dest::Control
                } else {
                    Dest::Lane(self.wildcard_lane(&(
                        c.tenant.clone(),
                        c.queue.clone(),
                        c.group.clone(),
                    )))
                }
            }
            Command::Ack(c) => {
                let mut lanes = Vec::with_capacity(c.targets.len());
                for t in &c.targets {
                    lanes.push(pid_lane(&t.tenant, &t.queue, t.pid)?);
                }
                one(&lanes)
            }
            Command::AckPositional(c) => match pid_lane(&c.tenant, &c.queue, c.pid)? {
                Some(l) => Dest::Lane(l),
                None => Dest::Control,
            },
            Command::Nack(c) => match pid_lane(&c.tenant, &c.queue, c.pid)? {
                Some(l) => Dest::Lane(l),
                None => Dest::Control,
            },
            Command::DlqHead(c) => match pid_lane(&c.tenant, &c.queue, c.pid)? {
                Some(l) => Dest::Lane(l),
                None => Dest::Control,
            },
            Command::Transaction(c) => {
                if !c.kv.is_empty() || !c.timers.is_empty() || !c.extra_effects.is_empty() {
                    Dest::Control
                } else {
                    let mut lanes = Vec::new();
                    for p in &c.pushes {
                        lanes.push(part(&p.tenant, &p.queue, &p.partition)?);
                    }
                    for t in &c.acks {
                        lanes.push(pid_lane(&t.tenant, &t.queue, t.pid)?);
                    }
                    for a in &c.positional_acks {
                        lanes.push(pid_lane(&a.tenant, &a.queue, a.pid)?);
                    }
                    one(&lanes)
                }
            }
            Command::PopDiscover(_)
            | Command::Renew(_)
            | Command::Kv(_)
            | Command::Timers(_)
            | Command::Effects(_) => Dest::Control,
        })
    }

    /// The lane a wildcard pop of `key` goes to: the one whose last walk found
    /// the most ready partitions (each pick uses one up), else round-robin, so
    /// every lane's partitions get consumed.
    fn wildcard_lane(&mut self, key: &RingKey) -> u64 {
        let mut best: Option<(usize, u64)> = None;
        for (l, s) in self.lanes.iter().enumerate() {
            let s = s.lock().expect("lane state");
            if let Some(&ready) = s.ready.get(key) {
                if ready > 0 && best.is_none_or(|(b, _)| ready > b) {
                    best = Some((ready, l as u64));
                }
            }
        }
        if let Some((_, l)) = best {
            let mut s = self.lanes[l as usize].lock().expect("lane state");
            if let Some(r) = s.ready.get_mut(key) {
                *r = r.saturating_sub(1);
            }
            return l;
        }
        let c = self.rr.entry(key.clone()).or_insert(0);
        let l = *c % self.n;
        *c = c.wrapping_add(1);
        l
    }
}

/// Plan one lane's commands (on its thread): its kept overlay advanced over its
/// slices of the in-flight entries, its rings filtered to its partitions, the
/// router's clock.
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
    cmds: Vec<(usize, Command)>,
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
        };
        let mut seen: HashSet<RequestId> = HashSet::new();
        let start = std::time::Instant::now();
        let budget = std::time::Duration::from_millis(cfg.plan_budget_ms);
        let mut cut = false;
        for (pos, cmd) in cmds {
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
                                // A lane's KV version would collide with
                                // control's and fail the whole entry (I18).
                                | Effect::KvPut { .. }
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
                        seen.insert(id);
                        LaneSlot::Logged {
                            id,
                            outcome,
                            effects,
                        }
                    }
                }
                Ok(Plan::Empty(outcome)) => {
                    if ov.folds() != folds0 {
                        out.poisoned = true;
                    }
                    if matches!(&cmd, Command::PopWildcard(p) if !p.wait) {
                        // Nothing ready among THIS lane's partitions, and the
                        // pop will not wait: it is empty only when the whole
                        // queue is, so control plans it with every partition.
                        // A long-poll pop parks and is woken by the next apply
                        // that makes a partition of its queue ready, whichever
                        // lane owns it.
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
        drop(planner);
        // The router's hint for the next wildcard pops.
        st.ready.clear();
        for k in ring_keys {
            st.ready.insert(k.clone(), pr.ready_len(k));
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
    let mut per_lane: Vec<Vec<(usize, Command)>> = (0..n).map(|_| Vec::new()).collect();
    let mut control: Vec<(usize, Command)> = Vec::new();
    let mut ring_keys: Vec<RingKey> = Vec::new();
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
                match dest {
                    Dest::Lane(l) => {
                        if let Command::PopWildcard(p) = &cmd {
                            let k = (p.tenant.clone(), p.queue.clone(), p.group.clone());
                            if !ring_keys.contains(&k) {
                                ring_keys.push(k);
                            }
                        }
                        per_lane[l as usize].push((pos, cmd));
                    }
                    Dest::Control => control.push((pos, cmd)),
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
        let keep = keep.clone();
        let job = Box::new(move || {
            let w0 = std::time::Instant::now();
            let c0 = crate::rsm::timing::thread_cpu_ns();
            let mut st = state.lock().expect("lane state");
            let res = plan_lane(
                &*store, &front, &mut st, l as u64, n, &keep, reader, qlog_reader, &fl, cmds,
                cfg, now_us, &keys, cycle_no,
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
            return Err(crate::rsm::store::StoreError::Io("a lane thread is gone".into()));
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

    // The lanes' results: logged commands (the entry's first part), the rest
    // answered or handed to control.
    let mut lane_logged: Vec<Vec<(RequestId, Outcome, Vec<Effect>)>> =
        (0..n).map(|_| Vec::new()).collect();
    let mut lane_poisoned = vec![false; n as usize];
    for (l, out) in lane_outs.into_iter().enumerate() {
        let Some(out) = out else { continue };
        lane_poisoned[l] = out.poisoned;
        for (pos, s) in out.slots {
            match s {
                LaneSlot::Logged {
                    id,
                    outcome,
                    effects,
                } => {
                    slots[pos] = Some(Slot::Logged(id));
                    lane_logged[l].push((id, outcome, effects));
                }
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
    let lane_cmds: u64 = lane_logged.iter().map(|v| v.len() as u64).sum();
    let control_cmds = control.len() as u64;
    LANE_STATS.lane.fetch_add(lane_cmds, std::sync::atomic::Ordering::Relaxed);
    LANE_STATS
        .control
        .fetch_add(control_cmds, std::sync::atomic::Ordering::Relaxed);
    LANE_STATS.maybe_log();

    add(&LANE_STATS.lanes_us, t_lanes.elapsed().as_micros() as u64);
    // ---- control ------------------------------------------------------------
    let t_control = std::time::Instant::now();
    let needs_control = !control.is_empty()
        || fire.is_some()
        || maintenance.is_some()
        || kv_sweep_limit.is_some()
        || expire_window_us.is_some();
    let mut control_logged: Vec<(RequestId, Outcome, Vec<Effect>)> = Vec::new();
    let mut pid_base = ls.records.iter().map(|r| r.pid_hi).fold(base_pid, u64::max);
    let mut kv_base = ls.records.iter().map(|r| r.kv_hi).fold(base_kv, u64::max);
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
            for logged in &lane_logged {
                for (_, _, effects) in logged {
                    ov.apply_effects(effects);
                }
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
            let mut seen: HashSet<RequestId> = lane_logged
                .iter()
                .flat_map(|v| v.iter().map(|(id, _, _)| *id))
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

    add(&LANE_STATS.control_us, t_control.elapsed().as_micros() as u64);
    // ---- merge: ONE entry, lanes first, then control -------------------------
    let t_merge = std::time::Instant::now();
    let mut full = Entry::with_capacity(
        now_us,
        pid_base,
        kv_base,
        lane_logged.iter().map(|v| v.len()).sum::<usize>() + control_logged.len(),
    );
    let mut subs: Vec<Entry> = (0..n).map(|_| Entry::new(now_us, pid_base, kv_base)).collect();
    for (l, logged) in lane_logged.into_iter().enumerate() {
        for (id, outcome, effects) in logged {
            let stripped: Vec<Effect> = effects.iter().map(strip).collect();
            let _ = subs[l].add_command(id, outcome.clone(), stripped);
            if let Err(e) = full.add_command(id, outcome, effects) {
                return Err(crate::rsm::store::StoreError::Io(format!(
                    "lane entry build: {e:?}"
                )));
            }
        }
    }
    // Control's effects each lane must see (L4), in control's order, folded
    // into the lane's overlay under this cycle's tag and recorded in its slice.
    let mut routed: Vec<Vec<Effect>> = (0..n).map(|_| Vec::new()).collect();
    for (id, outcome, effects) in control_logged {
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

