//! LANES (`QUEEN_LANES` > 1): a cycle's commands planned in parallel.
//!
//! One Raft log, one entry per cycle, exactly as with a single planner; what
//! changes is who plans. A LANE is a thread that owns the partitions with
//! `pid % n == lane` and plans every command that touches only them — pushes
//! to an existing partition, the groups of a multi-push, single-lane
//! transactions, the consumption engine's checkpoints (its cursor and DLQ
//! rows of one lane) — against its OWN kept overlay (its effects, plus the
//! catalog effects every lane must see). The lanes of a cycle run at the same
//! time; then the CONTROL step plans everything else on the planner thread,
//! with a full view (every in-flight entry plus this cycle's lane effects):
//! the catalog (queue and group creation, configuration, deletes), new
//! partitions, KV, timers, cross-lane commands, the leader steps. Lane effects
//! of different lanes touch disjoint state, and control comes after them in
//! the entry, so the entry applies exactly as if one thread had planned it in
//! that order.
//!
//! No consumption is planned here: pops, acks, nacks, renews and DLQ heads are
//! the consumption engine's ([`crate::rsm::consume`]), served on the leader
//! from memory. One that reaches the router goes to control, whose planner
//! refuses it.
//!
//! A push to a partition that does not exist yet is planned by the CREATION
//! step, after the lanes and before control, on a fresh overlay over the
//! cycle's bases (its partition takes the cycle's next id); a push to one an
//! entry in flight created goes to that partition's lane, whose slice holds the
//! create.
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

use super::{plan_fire_step, KeepCfg, PlanOutput, Slot, KEEP_RESET_EVERY};
use crate::rsm::dedup::DedupFront;
use crate::rsm::effect::{Effect, Pid};
use crate::rsm::entry::{Entry, Outcome, RequestId};
use crate::rsm::fasthash::FxBuild;
use crate::rsm::planner::kept::KeptOverlay;
use crate::rsm::planner::timers::TimerFireConfig;
use crate::rsm::planner::{Lookup, Overlay, Plan, PlanConfig, Planner, PushCommand, Refusal};
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::segments::Reader;
use crate::rsm::state::Committed;
use crate::rsm::store::{Reads, Store, TypedReads};

use super::{Command, MultiPushCommand, Reply};

/// What a lane thread is sent: a planning job.
enum LaneMsg {
    Job(Box<dyn FnOnce() + Send>),
}

/// A persistent lane thread running planning jobs.
struct LaneWorker {
    tx: std::sync::mpsc::Sender<LaneMsg>,
}

impl LaneWorker {
    fn spawn(lane: usize) -> LaneWorker {
        let (tx, rx) = std::sync::mpsc::channel::<LaneMsg>();
        std::thread::Builder::new()
            .name(format!("queen-lane-{lane}"))
            .spawn(move || {
                crate::rsm::batcher::raise_planning_thread_priority();
                let spin = std::time::Duration::from_micros(*LANE_SPIN_US);
                loop {
                    // With cycles back to back (pipelined planning) the next job
                    // usually arrives within tens of microseconds: poll for it
                    // that long before parking, since a futex wake on a busy
                    // box cost 0.5-1.7 ms of every cycle's lane phase.
                    let msg = if spin.is_zero() {
                        rx.recv().ok()
                    } else {
                        let t0 = std::time::Instant::now();
                        loop {
                            match rx.try_recv() {
                                Ok(m) => break Some(m),
                                Err(std::sync::mpsc::TryRecvError::Disconnected) => break None,
                                Err(std::sync::mpsc::TryRecvError::Empty) => {
                                    if t0.elapsed() >= spin {
                                        break rx.recv().ok();
                                    }
                                    std::hint::spin_loop();
                                }
                            }
                        }
                    };
                    let Some(LaneMsg::Job(job)) = msg else { break };
                    job();
                    // What the job did after it answered (dropping what it
                    // captured) delays this lane's next job.
                    if let Some(t) = JOB_SENT.with(|c| c.take()).map(|t| t.elapsed()) {
                        add(&LANE_STATS.job_tail_us, t.as_micros() as u64);
                    }
                }
            })
            .expect("spawn a lane thread");
        LaneWorker { tx }
    }
}

/// `QUEEN_LANES_SPIN_US` (default 0 = off): how long a lane thread polls for
/// its next job before parking on the channel. For A/B on the VMs: it trades a
/// little CPU per idle lane for the wake latency of the lane phase.
static LANE_SPIN_US: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
    std::env::var("QUEEN_LANES_SPIN_US")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0)
        .min(10_000)
});

thread_local! {
    /// When this lane thread's current job sent its result.
    static JOB_SENT: std::cell::Cell<Option<std::time::Instant>> = const { std::cell::Cell::new(None) };
}

/// What one lane keeps between cycles.
#[derive(Default)]
struct LaneState {
    kept: Option<KeptOverlay>,
    /// The tag of the cycle in progress (set by `begin_cycle`).
    tag: u64,
}

/// One entry in flight, as the lanes know it.
struct Record {
    full: Arc<Entry>,
    /// Each lane's slice: exactly what that lane folded for this entry (its
    /// commands carry no outcome: the router answers every id in flight from
    /// the full entries, [`LanesState::ids`]).
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
    /// Queues this entry creates partitions for.
    creates: Vec<(String, String)>,
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
    /// Every in-flight command by request id (L5): its entry and its index
    /// there, so the outcome is cloned only for a retry that hits it.
    ids: HashMap<RequestId, (Arc<Entry>, u32), FxBuild>,
    epoch: u64,
    cycles: u64,
    /// The router's state for the cycle it is routing (reset every cycle).
    cycle: RouterCycle,
    /// Control's overlay, kept between the cycles it plans in: each takes out
    /// what landed and ingests only the entries it has not seen, instead of
    /// rebuilding every entry in flight.
    control: Option<KeptOverlay>,
    /// The router's committed-catalog lookups, kept between cycles.
    kept_facts: KeptFacts,
    /// A catalog change was in flight since the facts were kept: start over
    /// once it has landed.
    kept_stale: bool,
}

/// tenant → queue → name → `V`: nested so a lookup borrows `&str` instead of
/// building an owned key per command.
type ByName<V> = FxMap<String, FxMap<String, FxMap<String, V>>>;

/// Where one group of a multi-push goes, and its partition's pid when known.
type GroupDest = (Dest, Option<Pid>);

/// A map on the crate's fast hasher: the router's per-command lookups hash names
/// and ids millions of times a second.
type FxMap<K, V> = HashMap<K, V, FxBuild>;
type FxSet<K> = HashSet<K, FxBuild>;

/// What the router reads once per cycle rather than once per command.
#[derive(Default)]
struct RouterCycle {
    /// tenant → queue → committed.
    queues: FxMap<String, FxMap<String, bool>>,
    /// tenant → queue → partition name → its committed pid, and whether it is
    /// garbage.
    pids: ByName<Option<(Pid, bool)>>,
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
    /// Per cycle: the slowest job's wall, the longest wait from handing a job
    /// to its lane to its start, and control's overlay advance (on the
    /// planning thread, while the lanes run).
    job_max_us: std::sync::atomic::AtomicU64,
    job_start_us: std::sync::atomic::AtomicU64,
    /// Summed over jobs: from a job's answer to its thread being free again.
    job_tail_us: std::sync::atomic::AtomicU64,
    advance_us: std::sync::atomic::AtomicU64,
    /// Pushes the creation step planned, and its microseconds.
    create: std::sync::atomic::AtomicU64,
    create_us: std::sync::atomic::AtomicU64,
    /// Diagnostics: lane-state resets (and those from a misaligned in-flight
    /// list), and lane jobs skipped.
    resets: std::sync::atomic::AtomicU64,
    sync_resets: std::sync::atomic::AtomicU64,
    skips: std::sync::atomic::AtomicU64,
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
    job_max_us: std::sync::atomic::AtomicU64::new(0),
    job_start_us: std::sync::atomic::AtomicU64::new(0),
    job_tail_us: std::sync::atomic::AtomicU64::new(0),
    advance_us: std::sync::atomic::AtomicU64::new(0),
    create: std::sync::atomic::AtomicU64::new(0),
    create_us: std::sync::atomic::AtomicU64::new(0),
    resets: std::sync::atomic::AtomicU64::new(0),
    sync_resets: std::sync::atomic::AtomicU64::new(0),
    skips: std::sync::atomic::AtomicU64::new(0),
    last: Mutex::new(None),
};

fn add(c: &std::sync::atomic::AtomicU64, v: u64) {
    c.fetch_add(v, std::sync::atomic::Ordering::Relaxed);
}

impl LaneStats {
    /// Log the line every 10 s; `true` when this call did.
    fn maybe_log(&self) -> bool {
        let mut last = self.last.lock().expect("lane stats");
        let now = std::time::Instant::now();
        if last.is_some_and(|t| now.duration_since(t) < std::time::Duration::from_secs(10)) {
            return false;
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
        let job_tail = per(&self.job_tail_us, jobs);
        let (job_max, job_start, advance) = (
            per(&self.job_max_us, cycles),
            per(&self.job_start_us, cycles),
            per(&self.advance_us, cycles),
        );
        let create = self.create.swap(0, Relaxed);
        let create_us = per(&self.create_us, cycles);
        let (resets, sync_resets) = (
            self.resets.swap(0, Relaxed),
            self.sync_resets.swap(0, Relaxed),
        );
        let skips = self.skips.swap(0, Relaxed);
        if lane + control + create > 0 {
            tracing::info!(
                target: "rsm",
                lane,
                control,
                cycles,
                router_us = router,
                lanes_us = lanes,
                control_us = ctl,
                merge_us = merge,
                job_wall_us = job_wall,
                job_cpu_us = job_cpu,
                job_max_us = job_max,
                job_start_max_us = job_start,
                job_tail_us = job_tail,
                advance_us = advance,
                create,
                create_us,
                resets,
                sync_resets,
                skips,
                "lanes: commands planned since the last line (per-cycle means)",
            );
        }
        true
    }
}

/// Where a command is planned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Dest {
    Lane(u64),
    Control,
    /// A push whose partition does not exist yet: the creation step plans it.
    Create,
}

/// How one command fared in a lane.
enum LaneSlot {
    /// Logged into the lane's part of the entry ([`LaneOut::part`]).
    Logged(RequestId),
    SameCycle(RequestId),
    Empty(Outcome),
    Refused(Refusal),
    Deferred(Box<Command>),
    /// Planned catalog state (L1 violated): control plans it again, with
    /// every partition in view.
    Misrouted(Box<Command>),
}

/// One group of a [`Command::MultiPush`] routed to a lane or to the creation
/// step: the multi-push it belongs to (its index in the cycle's list), the
/// group's position in it, and the group itself.
struct Piece {
    multi: usize,
    group: usize,
    push: PushCommand,
    /// The id the router assigned the group's new partition, when its lane
    /// creates it (lane-local creation, [`LOCAL_CREATE`]).
    create_pid: Option<Pid>,
    /// The live pid the router resolved the group's partition name to.
    known_pid: Option<Pid>,
}

/// `QUEEN_LANES_LOCAL_CREATE` (default on): a push to a partition that does
/// not exist yet is planned by that partition's LANE, with the id the router
/// assigns it from the cycle's base, instead of by the serial creation step.
/// The entry's creates are then its dense ids in lane order, which
/// `Entry::validate` accepts (the SET must be dense). A creation was ~8 us of
/// serial planner time (creation step, router miss, control fold): ~120k new
/// keys/s at most, with the whole of first contact on one thread.
static LOCAL_CREATE: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
    !matches!(
        std::env::var("QUEEN_LANES_LOCAL_CREATE")
            .as_deref()
            .map(str::trim),
        Ok("0") | Ok("false") | Ok("off") | Ok("no")
    )
});

/// A lane's commands for one cycle: batch position, command, and the id the
/// router assigned the partition the command creates (lane-local creation).
type LaneCmds = Vec<(usize, Command, Option<Pid>)>;

/// Where the router sent a new partition's name this cycle: to a lane with an
/// assigned id, or to the creation step. Every later push to the same name in
/// the batch goes the same way (two creators of one name would create it
/// twice).
#[derive(Clone, Copy)]
enum NewPart {
    Local(Pid),
    Step,
}

/// Where a push to a new partition goes (see [`NewPart`]): the lane of an id
/// assigned from `base` in routing order, or the creation step when `local`
/// is off (the knob, or a push too large for a lane to be sure to plan).
fn new_part_dest(
    seen: &mut HashMap<(String, String, String), NewPart>,
    assigned: &mut u64,
    base: u64,
    push: &PushCommand,
    local: bool,
) -> NewPart {
    let key = (
        push.tenant.clone(),
        push.queue.clone(),
        push.partition.clone(),
    );
    if let Some(d) = seen.get(&key) {
        return *d;
    }
    let d = if *LOCAL_CREATE && local {
        let pid = base + *assigned;
        *assigned += 1;
        NewPart::Local(pid)
    } else {
        NewPart::Step
    };
    seen.insert(key, d);
    d
}

/// The largest push a lane creates a partition for: a create that a lane
/// could refuse (too large for an entry) would leave its assigned id unused
/// and the entry's ids not dense, so anything near the limit goes to the
/// creation step, which assigns ids only to what it plans.
fn local_create_ok(size_hint: usize, entry_max_bytes: usize) -> bool {
    size_hint.saturating_mul(2) <= entry_max_bytes
}

/// A multi-push the router split: where it sat in the batch, its id, and how
/// many groups it has.
struct MultiRoute {
    pos: usize,
    id: RequestId,
    groups: usize,
}

/// The "lane" of a multi-push group planned by the creation step.
const CREATE_LANE: u64 = u64::MAX;

/// A multi-push ready to log: its effects (every group, in group order), its
/// outcome, each lane's stripped share of it for that lane's slice, and the
/// stripped effects of its groups the creation step planned (routed to their
/// partitions' lanes like any creation).
struct MultiLogged {
    id: RequestId,
    outcome: Outcome,
    effects: Vec<Effect>,
    lane_subs: Vec<(u64, Vec<Effect>)>,
    routed: Vec<Effect>,
}

/// How a [`Piece`] fared. The push comes back with it, so a multi-push that
/// has to be planned again (by control) is rebuilt without copying a frame.
struct PieceOut {
    multi: usize,
    group: usize,
    push: PushCommand,
    result: PieceResult,
}

enum PieceResult {
    /// Planned and folded: its effects (whole, and stripped for the lane's
    /// slice) and its items' verdicts.
    Logged {
        effects: Vec<Effect>,
        stripped: Vec<Effect>,
        items: Vec<crate::rsm::entry::PushVerdict>,
    },
    /// Nothing to write (every item a duplicate): its verdicts.
    Empty(Vec<crate::rsm::entry::PushVerdict>),
    /// Refused: its items answer with the refusal.
    Refused(Refusal),
    /// It planned catalog state (L1 broke under the lane): the whole
    /// multi-push is planned again by control.
    Misrouted,
}

struct LaneOut {
    slots: Vec<(usize, LaneSlot)>,
    /// The multi-push groups this lane planned, after its own commands.
    pieces: Vec<PieceOut>,
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
    match e {
        // Every field but the payload, which is never copied.
        Effect::Append {
            pid,
            bucket,
            base_offset,
            count,
            created_at_us,
            hashes,
            blob: _,
        } => Effect::Append {
            pid: *pid,
            bucket: *bucket,
            base_offset: *base_offset,
            count: *count,
            created_at_us: *created_at_us,
            hashes: hashes.clone(),
            blob: Vec::new(),
        },
        other => other.clone(),
    }
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
                let _ = subs[l].add_command(c.request_id, Outcome::Empty, effects);
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
        created,
    }
}

impl Record {
    /// Whether this entry changes any catalog fact the router caches.
    fn catalog(&self) -> bool {
        !self.queues.is_empty()
            || !self.tenants.is_empty()
            || !self.pids.is_empty()
            || !self.creates.is_empty()
    }
}

/// `QUEEN_LANES_ROUTER_KEEP` (default on): keep the router's committed-catalog
/// lookups (queue and group rows, partition name -> pid, live pids, the lane
/// holding a whole queue) from one cycle to the next while no catalog change is
/// in flight. Re-reading them each cycle cost ~2 ms of the ~11 ms cycle at
/// 1,000 queues, serial, before any lane starts (2026-09-29).
static ROUTER_KEEP: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
    !matches!(
        std::env::var("QUEEN_LANES_ROUTER_KEEP")
            .as_deref()
            .map(str::trim),
        Ok("0") | Ok("false") | Ok("off")
    )
});

/// The most kept partitions (live pids) before the kept lookups start over.
const ROUTER_KEEP_MAX_PIDS: usize = 1 << 20;

/// Free `v` on a thread of its own rather than on the serial planner thread:
/// the records of landed entries (the planner often holds an entry's last
/// reference, so every effect and frame is freed where it drops) and a cycle's
/// spent multi-push groups. At one message per partition freeing was ~20% of
/// the planner thread (2026-09-30).
fn drop_later<T: Send + 'static>(v: T) {
    type Garbage = Box<dyn Send>;
    static TX: std::sync::LazyLock<Option<std::sync::mpsc::Sender<Garbage>>> =
        std::sync::LazyLock::new(|| {
            let (tx, rx) = std::sync::mpsc::channel::<Garbage>();
            std::thread::Builder::new()
                .name("queen-rsm-drop".into())
                .spawn(move || {
                    for g in rx {
                        drop(g);
                    }
                })
                .ok()
                .map(|_| tx)
        });
    match TX.as_ref() {
        Some(tx) => {
            if let Err(e) = tx.send(Box::new(v)) {
                drop(e.0);
            }
        }
        None => drop(v),
    }
}

/// The router's committed-catalog lookups kept between cycles ([`ROUTER_KEEP`]).
#[derive(Default)]
struct KeptFacts {
    queues: FxMap<String, FxMap<String, bool>>,
    pids: ByName<Option<(Pid, bool)>>,
}

/// The in-flight catalog work commands must not race (L1).
struct Busy {
    /// tenant → queues with a catalog change in flight.
    queues: FxMap<String, FxSet<String>>,
    tenants: FxSet<String>,
    pids: FxSet<Pid>,
    /// tenant → queue → partition name → pid, for the partitions created in
    /// flight ([`Record::created`]).
    created: ByName<Pid>,
}

impl Busy {
    fn queue(&self, tenant: &str, queue: &str) -> bool {
        self.tenants.contains(tenant)
            || self.queues.get(tenant).is_some_and(|qs| qs.contains(queue))
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
            ids: HashMap::default(),
            epoch: u64::MAX,
            cycles: 0,
            cycle: RouterCycle::default(),
            control: None,
            kept_facts: KeptFacts::default(),
            kept_stale: false,
        }
    }

    fn reset(&mut self) {
        add(&LANE_STATS.resets, 1);
        self.records.clear();
        self.ids.clear();
        self.control = None;
        self.kept_facts = KeptFacts::default();
        self.kept_stale = false;
        for l in &self.lanes {
            l.lock().expect("lane state").kept = None;
        }
    }

    /// Bring the records in line with the in-flight list: drop the landed
    /// prefix; anything else unexpected (an entry this state never saw) resets
    /// the lanes, which then rebuild from slices split off the full entries.
    fn sync(&mut self, folded: &[(u64, Arc<Entry>)]) {
        let first = folded.first().map(|(_, e)| e.clone());
        let mut landed: Vec<Record> = Vec::new();
        while let Some(r) = self.records.front() {
            match &first {
                Some(f) if Arc::ptr_eq(&r.full, f) => break,
                _ => {
                    let r = self.records.pop_front().expect("front");
                    for c in &r.full.commands {
                        self.ids.remove(&c.request_id);
                    }
                    landed.push(r);
                }
            }
        }
        if !landed.is_empty() {
            drop_later(landed);
        }
        let aligned = self.records.len() == folded.len()
            && self
                .records
                .iter()
                .zip(folded.iter())
                .all(|(r, (_, e))| Arc::ptr_eq(&r.full, e));
        if !aligned {
            add(&LANE_STATS.sync_resets, 1);
            self.reset();
            for (_, e) in folded {
                let subs = split(e, self.n);
                for (i, c) in e.commands.iter().enumerate() {
                    self.ids
                        .entry(c.request_id)
                        .or_insert_with(|| (e.clone(), i as u32));
                }
                self.push_record(record_of(e.clone(), subs));
            }
        }
    }

    /// Take `r` in flight.
    fn push_record(&mut self, r: Record) {
        self.records.push_back(r);
    }

    fn busy(&self) -> Busy {
        let mut b = Busy {
            queues: HashMap::default(),
            tenants: HashSet::default(),
            pids: HashSet::default(),
            created: HashMap::default(),
        };
        for r in &self.records {
            for (t, q) in &r.queues {
                b.queues.entry(t.clone()).or_default().insert(q.clone());
            }
            b.tenants.extend(r.tenants.iter().cloned());
            b.pids.extend(r.pids.iter().copied());
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
            Command::Transaction(c) => {
                // Anything but pushes is control's: KV and timers are global
                // state, the riders (Streams state, the engine's cursor rows)
                // are checked against every partition in view there, and the
                // consumption legs must never reach a planner (control's
                // refuses them).
                if !c.kv.is_empty()
                    || !c.timers.is_empty()
                    || !c.extra_effects.is_empty()
                    || !c.positions.is_empty()
                    || !c.acks.is_empty()
                    || !c.positional_acks.is_empty()
                {
                    Dest::Control
                } else {
                    let mut lanes = Vec::new();
                    for p in &c.pushes {
                        lanes.push(self.part_lane(r, &p.tenant, &p.queue, &p.partition, busy)?);
                    }
                    one(&lanes)
                }
            }
            // The consumption engine's checkpoints: cursor and DLQ rows of the
            // partitions of one lane, planned there, in parallel. Their one
            // read is whether each partition is still live, which the lane
            // sees whatever catalog change is in flight (a queue delete's or a
            // purge's garbage folds into every lane, a partition delete into
            // its own), so only a pid with a delete in flight goes to control.
            Command::Effects(c) if !c.effects.is_empty() => {
                let mut lane: Option<u64> = None;
                let mut one = true;
                for e in &c.effects {
                    match e {
                        Effect::CursorSet { pid, .. } | Effect::DlqInsert { pid, .. }
                            if !busy.pids.contains(pid)
                                && *lane.get_or_insert(pid % self.n) == pid % self.n => {}
                        _ => {
                            one = false;
                            break;
                        }
                    }
                }
                match lane {
                    Some(l) if one => Dest::Lane(l),
                    _ => Dest::Control,
                }
            }
            // Consumption is the engine's: one that reaches the router goes to
            // control, whose planner refuses it.
            Command::PopPinned(_)
            | Command::PopWildcard(_)
            | Command::PopDiscover(_)
            | Command::Ack(_)
            | Command::AckPositional(_)
            | Command::Nack(_)
            | Command::Renew(_)
            | Command::DlqHead(_)
            | Command::Kv(_)
            | Command::Timers(_)
            | Command::Effects(_) => Dest::Control,
            // Split by the router loop (`plan_cycle_lanes`); whole, it is control's.
            Command::MultiPush(_) => Dest::Control,
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
        Ok(self.push_dest_pid(r, tenant, queue, name, busy)?.0)
    }

    /// [`LanesState::push_dest`], with the partition's pid when it goes to a
    /// lane: the lane plans the push on it without looking the name up again.
    fn push_dest_pid<R: Reads + ?Sized>(
        &mut self,
        r: &R,
        tenant: &str,
        queue: &str,
        name: &str,
        busy: &Busy,
    ) -> crate::rsm::store::Result<(Dest, Option<Pid>)> {
        if busy.queue(tenant, queue) || !self.queue_known(r, tenant, queue)? {
            return Ok((Dest::Control, None));
        }
        Ok(match self.committed_pid(r, tenant, queue, name)? {
            Some((pid, false)) if !busy.pids.contains(&pid) => {
                (Dest::Lane(pid % self.n), Some(pid))
            }
            Some(_) => (Dest::Control, None),
            None => match busy.created(tenant, queue, name) {
                Some(pid) if !busy.pids.contains(&pid) => (Dest::Lane(pid % self.n), Some(pid)),
                Some(_) => (Dest::Control, None),
                None => (Dest::Create, None),
            },
        })
    }

    /// Where each group of a multi-push goes — its partition's lane, or the
    /// creation step — or `None` when any group must be planned by control
    /// (its queue to create, a catalog change in flight, a partition on its
    /// way out), which then plans the whole multi-push.
    fn split_multi<R: Reads + ?Sized>(
        &mut self,
        r: &R,
        mp: &MultiPushCommand,
        busy: &Busy,
    ) -> crate::rsm::store::Result<Option<Vec<GroupDest>>> {
        if mp.pushes.is_empty() {
            return Ok(None);
        }
        let mut dests = Vec::with_capacity(mp.pushes.len());
        for p in &mp.pushes {
            match self.push_dest_pid(r, &p.tenant, &p.queue, &p.partition, busy)? {
                d @ (Dest::Lane(_) | Dest::Create, _) => dests.push(d),
                _ => return Ok(None),
            }
        }
        Ok(Some(dests))
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
}

/// Plan one lane's commands (on its thread): its kept overlay advanced over its
/// slices of the in-flight entries, the router's clock.
#[allow(clippy::too_many_arguments)]
fn plan_lane<S: Store>(
    store: &S,
    front: &DedupFront,
    st: &mut LaneState,
    keep: &KeepCfg,
    reader: Option<Reader>,
    qlog_reader: Option<QLogReader>,
    folded: &[(u64, Arc<Entry>)],
    cmds: LaneCmds,
    cfg: PlanConfig,
    now_us: i64,
    retention: Vec<crate::rsm::retention_scan::Proposal>,
    scan: Option<Arc<crate::rsm::retention_scan::ScanShared>>,
    pieces: Vec<Piece>,
) -> crate::rsm::store::Result<LaneOut> {
    let prior = if keep.enabled { st.kept.take() } else { None };
    store.read(|r| {
        let store_applied = r.applied_index()?;
        let base_pid = r.next_pid()?;
        let base_kv = r.kv_version_next()?;
        let mut kept = match prior.map(|k| k.advance(folded, store_applied, base_pid, base_kv)) {
            Some(Ok(k)) => k,
            _ => KeptOverlay::rebuild(folded, store_applied, base_pid, base_kv),
        };
        let committed = Committed::new(r);
        st.tag = kept.begin_cycle();
        let ov = kept.overlay_mut();
        ov.mark_cycle_start();
        let mut planner = Planner::new(committed, now_us, cfg.clone(), front, reader.clone());
        planner.set_qlog_reader(qlog_reader.clone());

        let mut out = LaneOut {
            slots: Vec::with_capacity(cmds.len()),
            pieces: Vec::with_capacity(pieces.len()),
            poisoned: false,
            part: Entry::with_capacity(now_us, base_pid, base_kv, cmds.len()),
            sub: Entry::with_capacity(now_us, base_pid, base_kv, cmds.len()),
        };
        let mut seen: HashSet<RequestId> = HashSet::new();
        let start = std::time::Instant::now();
        let budget = std::time::Duration::from_millis(cfg.plan_budget_ms);
        let mut cut = false;
        // Per-kind planning times (`queen_raft_plan_*`), as the single planner
        // records them: without them a lanes node reported no per-kind figure.
        let timed = crate::rsm::timing::enabled();
        for (pos, cmd, create_pid) in cmds {
            // A push creating its partition under an assigned id is never cut:
            // its id must be used in this entry.
            if cut && create_pid.is_none() {
                out.slots.push((pos, LaneSlot::Deferred(Box::new(cmd))));
                continue;
            }
            let id = cmd.request_id();
            if seen.contains(&id) {
                out.slots.push((pos, LaneSlot::SameCycle(id)));
                continue;
            }
            let folds0 = ov.folds();
            let kind = cmd.kind();
            let t_cmd = timed.then(std::time::Instant::now);
            let planned = match create_pid {
                Some(pid) => ov.with_next_pid(pid, |ov| cmd.plan(&planner, ov)),
                None => cmd.plan(&planner, ov),
            };
            if let Some(t) = t_cmd {
                crate::rsm::timing::metrics()
                    .kinds
                    .record(kind, t.elapsed(), false);
            }
            let slot = match planned {
                Ok(Plan::Logged { effects, outcome }) => {
                    let catalog = effects.iter().any(|e| match e {
                        // The create the router assigned this push is the
                        // lane's to plan; any other is control's (L1, L3).
                        Effect::PartitionCreate { pid, .. } => Some(*pid) != create_pid,
                        Effect::QueueUpsert { .. }
                        | Effect::GroupUpsert { .. }
                        | Effect::QueueDelete { .. }
                        | Effect::GroupDelete { .. } => true,
                        _ => false,
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
                        let stripped: Vec<Effect> = effects.iter().map(strip).collect();
                        match out
                            .sub
                            .add_command(id, Outcome::Empty, stripped)
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
                    LaneSlot::Empty(outcome)
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
        // The retention scanner's watermark proposals for this lane's
        // partitions, judged against committed state and this lane's overlay
        // (which holds every in-flight effect on its partitions, and the
        // catalog deletes every lane folds), and logged as one command with a
        // minted id that nobody waits on — as control logs the walk's.
        if let (false, Some(scan)) = (retention.is_empty(), scan.as_ref()) {
            let (effects, _dropped) =
                crate::rsm::retention_scan::judge(r, ov, now_us, scan, &retention)?;
            if !effects.is_empty() {
                let id = crate::util::uuidv7_bytes();
                let folds0 = ov.folds();
                ov.apply_effects(&effects);
                if ov.folds() - folds0 != effects.len() as u64 {
                    out.poisoned = true;
                }
                let stripped: Vec<Effect> = effects.iter().map(strip).collect();
                if out
                    .sub
                    .add_command(id, Outcome::Empty, stripped)
                    .and_then(|()| out.part.add_command(id, Outcome::Empty, effects))
                    .is_err()
                {
                    out.poisoned = true;
                }
            }
        }
        // The multi-push groups on this lane's partitions, after everything
        // else the lane logged: the merge lays each multi-push out after every
        // lane's part, so planning them last keeps the plan order the entry's.
        // Never cut by the budget: a multi-push is logged whole or re-planned
        // whole, never split across cycles.
        for Piece {
            multi,
            group,
            push,
            create_pid,
            known_pid,
        } in pieces
        {
            let folds0 = ov.folds();
            let t_cmd = timed.then(std::time::Instant::now);
            let planned = match create_pid {
                Some(pid) => ov.with_next_pid(pid, |ov| planner.plan_push(ov, &push)),
                None => planner.plan_push_known(ov, &push, known_pid),
            };
            if let Some(t) = t_cmd {
                crate::rsm::timing::metrics().kinds.record(
                    crate::rsm::planner::CommandKind::Push,
                    t.elapsed(),
                    false,
                );
            }
            let result = match planned {
                Ok(Plan::Logged { effects, outcome }) => {
                    let catalog = effects.iter().any(|e| match e {
                        Effect::PartitionCreate { pid, .. } => Some(*pid) != create_pid,
                        Effect::QueueUpsert { .. }
                        | Effect::GroupUpsert { .. }
                        | Effect::QueueDelete { .. }
                        | Effect::GroupDelete { .. } => true,
                        _ => false,
                    });
                    if catalog {
                        out.poisoned = true;
                        PieceResult::Misrouted
                    } else {
                        if ov.folds() - folds0 != effects.len() as u64 {
                            out.poisoned = true;
                        }
                        let stripped = effects.iter().map(strip).collect();
                        let items = super::push_items(outcome, push.items.len());
                        PieceResult::Logged {
                            effects,
                            stripped,
                            items,
                        }
                    }
                }
                Ok(Plan::Empty(outcome)) => {
                    if ov.folds() != folds0 {
                        out.poisoned = true;
                    }
                    PieceResult::Empty(super::push_items(outcome, push.items.len()))
                }
                Ok(Plan::Refused(refusal)) | Err(refusal) => {
                    if ov.folds() != folds0 {
                        out.poisoned = true;
                    }
                    PieceResult::Refused(refusal)
                }
            };
            out.pieces.push(PieceOut {
                multi,
                group,
                push,
                result,
            });
        }
        drop(planner);
        st.kept = Some(kept);
        Ok(out)
    })
}

/// Whether planning `c` may read the partition state of the entries in flight
/// (B27: control folds it only for a cycle that plans such a command). KV,
/// timers and effect commands do not (an effect command checks only that its
/// partitions are live, which is catalog state), nor does a consumption
/// command, which the planner refuses unread.
fn reads_partition_state(c: &Command) -> bool {
    !matches!(
        c,
        Command::Kv(_)
            | Command::Timers(_)
            | Command::Effects(_)
            | Command::PopPinned(_)
            | Command::PopWildcard(_)
            | Command::PopDiscover(_)
            | Command::Ack(_)
            | Command::AckPositional(_)
            | Command::Nack(_)
            | Command::Renew(_)
            | Command::DlqHead(_)
    )
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
    scan: Option<Arc<crate::rsm::retention_scan::ScanShared>>,
) -> crate::rsm::store::Result<PlanOutput> {
    let n = ls.n;
    if !keep.enabled || ls.epoch != keep.epoch {
        ls.reset();
        ls.epoch = keep.epoch;
    }
    ls.cycles += 1;
    let cycle_no = ls.cycles;
    // The overlays are rebuilt from the entries in flight every so often: a
    // bound on how long any drift nothing detected could live.
    if keep.reset_every > 0 && cycle_no.is_multiple_of(keep.reset_every.min(KEEP_RESET_EVERY)) {
        for l in &ls.lanes {
            l.lock().expect("lane state").kept = None;
        }
        ls.control = None;
    }
    ls.sync(&folded);
    let busy = ls.busy();
    let batch_len = batch.len();

    // ---- the router ------------------------------------------------------
    let t_router = std::time::Instant::now();
    let mut slots: Vec<Option<Slot>> = (0..batch_len).map(|_| None).collect();
    let mut per_lane: Vec<LaneCmds> = (0..n).map(|_| Vec::new()).collect();
    // Lane-local creation: the names of new partitions and where they went,
    // and how many ids the router assigned from the cycle's base.
    let mut new_parts: HashMap<(String, String, String), NewPart> = HashMap::new();
    let mut local_creates: u64 = 0;
    let mut assign_base: u64 = 0;
    let mut control: Vec<(usize, Command)> = Vec::new();
    let mut creates: Vec<(usize, Command)> = Vec::new();
    // Multi-pushes split by partition: each group goes to its lane or to the
    // creation step, and the merge logs the whole as ONE command.
    let mut multis: Vec<MultiRoute> = Vec::new();
    let mut multi_of: HashMap<RequestId, usize> = HashMap::new();
    let mut multi_dups: Vec<(usize, usize)> = Vec::new();
    let mut lane_pieces: Vec<Vec<Piece>> = (0..n).map(|_| Vec::new()).collect();
    let mut create_pieces: Vec<Piece> = Vec::new();
    let (store_applied, base_pid, base_kv, now_us, cluster_version) = {
        let st = store.clone();
        st.read(|r| {
            let store_applied = r.applied_index()?;
            let base_pid = r.next_pid()?;
            let base_kv = r.kv_version_next()?;
            let cluster_version = r.cluster_version()?;
            let base_now = Committed::new(r).plan_now(wall_us)?;
            // L2: one clock for the cycle, above everything in flight.
            let now_us = ls.records.iter().fold(base_now, |m, rec| {
                m.max(rec.now_us.saturating_add(1))
                    .max(rec.created_hi.saturating_add(1))
            });
            let lookup = Planner::new(
                Committed::new(r),
                now_us,
                cfg.clone(),
                front,
                reader.clone(),
            );
            let no_overlay = Overlay::new(base_pid, base_kv);
            // The ids this cycle's creates take start above everything in
            // flight (as the creation step's did).
            assign_base = ls.records.iter().map(|r| r.pid_hi).fold(base_pid, u64::max);
            let entry_max = cfg.entry_max_bytes;
            // The kept lookups hold while no catalog change is in flight; one
            // that was starts them over once it has landed.
            let keep = *ROUTER_KEEP && !ls.records.iter().any(Record::catalog);
            if !keep || ls.kept_stale {
                ls.kept_facts = KeptFacts::default();
            }
            ls.kept_stale = !keep;
            let kept = std::mem::take(&mut ls.kept_facts);
            ls.cycle = RouterCycle {
                queues: kept.queues,
                pids: kept.pids,
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
                if let Some((e, i)) = ls.ids.get(&id) {
                    slots[pos] = Some(Slot::InFlightHit {
                        request_id: id,
                        outcome: e.commands[*i as usize].outcome.clone(),
                    });
                    continue;
                }
                let cmd = match cmd {
                    Command::MultiPush(mp) => {
                        if let Some(&m) = multi_of.get(&id) {
                            // A second copy in this batch (a client retry):
                            // answered with the first one's result.
                            multi_dups.push((pos, m));
                            continue;
                        }
                        let dests = if routed.contains_key(&id) {
                            None
                        } else {
                            ls.split_multi(r, &mp, &busy)?
                        };
                        match dests {
                            Some(dests) => {
                                let m = multis.len();
                                multi_of.insert(id, m);
                                let groups = mp.pushes.len();
                                for (g, (push, (d, known_pid))) in
                                    mp.pushes.into_iter().zip(dests).enumerate()
                                {
                                    let mut piece = Piece {
                                        multi: m,
                                        group: g,
                                        push,
                                        create_pid: None,
                                        known_pid,
                                    };
                                    match d {
                                        Dest::Lane(l) => lane_pieces[l as usize].push(piece),
                                        _ => {
                                            let hint = piece
                                                .push
                                                .items
                                                .iter()
                                                .map(|i| i.frame.len() + 16)
                                                .sum::<usize>()
                                                + 64;
                                            match new_part_dest(
                                                &mut new_parts,
                                                &mut local_creates,
                                                assign_base,
                                                &piece.push,
                                                local_create_ok(hint, entry_max),
                                            ) {
                                                NewPart::Local(pid) => {
                                                    piece.create_pid = Some(pid);
                                                    lane_pieces[(pid % n) as usize].push(piece);
                                                }
                                                NewPart::Step => create_pieces.push(piece),
                                            }
                                        }
                                    }
                                }
                                multis.push(MultiRoute { pos, id, groups });
                                continue;
                            }
                            None => Command::MultiPush(mp),
                        }
                    }
                    other => other,
                };
                let dest = match routed.get(&id) {
                    Some(d) => *d,
                    None => {
                        let d = ls.route(r, &cmd, &busy)?;
                        routed.insert(id, d);
                        d
                    }
                };
                match dest {
                    Dest::Lane(l) => per_lane[l as usize].push((pos, cmd, None)),
                    Dest::Control => control.push((pos, cmd)),
                    Dest::Create => {
                        let local = match &cmd {
                            Command::Push(c) => Some(new_part_dest(
                                &mut new_parts,
                                &mut local_creates,
                                assign_base,
                                c,
                                local_create_ok(cmd.size_hint(), entry_max),
                            )),
                            _ => None,
                        };
                        match local {
                            Some(NewPart::Local(pid)) => {
                                per_lane[(pid % n) as usize].push((pos, cmd, Some(pid)))
                            }
                            _ => creates.push((pos, cmd)),
                        }
                    }
                }
            }
            if keep {
                let named: usize = ls
                    .cycle
                    .pids
                    .values()
                    .flat_map(|qs| qs.values())
                    .map(|ps| ps.len())
                    .sum();
                if named <= ROUTER_KEEP_MAX_PIDS {
                    ls.kept_facts = KeptFacts {
                        queues: std::mem::take(&mut ls.cycle.queues),
                        pids: std::mem::take(&mut ls.cycle.pids),
                    };
                }
            }
            Ok((store_applied, base_pid, base_kv, now_us, cluster_version))
        })?
    };

    // The retention scanner's proposals: a watermark goes to its partition's
    // lane, which judges it in parallel with the others; a partition delete
    // (rare, and a name-keyed change) goes to control. A pid with a delete in
    // flight is left for the next round.
    let mut lane_retention: Vec<Vec<crate::rsm::retention_scan::Proposal>> =
        (0..n).map(|_| Vec::new()).collect();
    let mut control_retention: Vec<crate::rsm::retention_scan::Proposal> = Vec::new();
    if let Some(scan) = scan.as_ref() {
        for p in scan.take(crate::rsm::retention_scan::TAKE_PER_CYCLE) {
            let pid = p.pid();
            if busy.pids.contains(&pid) {
                continue;
            }
            match p {
                crate::rsm::retention_scan::Proposal::Watermark { .. } => {
                    lane_retention[(pid % n) as usize].push(p)
                }
                crate::rsm::retention_scan::Proposal::Delete { .. } => control_retention.push(p),
            }
        }
    }

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
    for (((l, cmds), retention), pieces) in per_lane
        .into_iter()
        .enumerate()
        .zip(lane_retention.into_iter())
        .zip(lane_pieces.into_iter())
    {
        if cmds.is_empty() && retention.is_empty() && pieces.is_empty() {
            add(&LANE_STATS.skips, 1);
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
        let lane_scan = scan.clone();
        let handed = std::time::Instant::now();
        let job = Box::new(move || {
            let w0 = std::time::Instant::now();
            let c0 = crate::rsm::timing::thread_cpu_ns();
            let mut st = state.lock().expect("lane state");
            let res = plan_lane(
                &*store,
                &front,
                &mut st,
                &keep,
                reader,
                qlog_reader,
                &fl,
                cmds,
                cfg,
                now_us,
                retention,
                lane_scan,
                pieces,
            );
            drop(st);
            let wall = w0.elapsed().as_micros() as u64;
            add(&LANE_STATS.job_wall_us, wall);
            add(
                &LANE_STATS.job_cpu_us,
                crate::rsm::timing::thread_cpu_ns().saturating_sub(c0) / 1000,
            );
            add(&LANE_STATS.jobs, 1);
            let started = w0.duration_since(handed).as_micros() as u64;
            JOB_SENT.with(|c| c.set(Some(std::time::Instant::now())));
            let _ = tx.send((res, wall, started));
        });
        if ls.workers[l].tx.send(LaneMsg::Job(job)).is_err() {
            return Err(crate::rsm::store::StoreError::Io(
                "a lane thread is gone".into(),
            ));
        }
        waits.push((l, rx));
    }
    // Control's overlay catches up with the entries that landed and the ones it
    // has not folded while the lanes plan: it needs only the in-flight list and
    // the committed bases, and it was the biggest serial piece of the control
    // phase (~1.4 ms of each ~8 ms cycle at 1M msg/s, 2026-09-29). Whether
    // control plans at all is known only after the lanes (they can hand it
    // commands back), so this runs every cycle it has an overlay; each entry is
    // still ingested and unfolded once.
    let t_advance = std::time::Instant::now();
    let mut prior_control = ls
        .control
        .take()
        .map(|k| k.advance_ingest(&folded, store_applied, base_pid, base_kv));
    add(
        &LANE_STATS.advance_us,
        t_advance.elapsed().as_micros() as u64,
    );
    let mut lane_outs: Vec<Option<LaneOut>> = (0..n).map(|_| None).collect();
    let mut failed: Option<crate::rsm::store::StoreError> = None;
    let (mut job_max, mut start_max) = (0u64, 0u64);
    for (l, rx) in waits {
        match rx.recv() {
            Ok((res, wall, started)) => {
                job_max = job_max.max(wall);
                start_max = start_max.max(started);
                match res {
                    Ok(out) => lane_outs[l] = Some(out),
                    Err(e) => failed = Some(e),
                }
            }
            Err(_) => failed = Some(crate::rsm::store::StoreError::Io("a lane job died".into())),
        }
    }
    add(&LANE_STATS.job_max_us, job_max);
    add(&LANE_STATS.job_start_us, start_max);
    if let Some(e) = failed {
        // Nothing of this cycle is kept anywhere: every lane rebuilds.
        ls.reset();
        return Err(e);
    }

    // The lanes' results: their parts of the entry (its first part, in lane
    // order), the rest answered or handed to control.
    let mut lane_parts: Vec<Option<(Entry, Entry)>> = (0..n).map(|_| None).collect();
    let mut lane_poisoned = vec![false; n as usize];
    // Each multi-push group's result, by (multi-push, group): the lane that
    // planned it (`CREATE_LANE` for the creation step), the group, the result.
    let mut piece_res: Vec<Vec<Option<(u64, PushCommand, PieceResult)>>> = multis
        .iter()
        .map(|m| (0..m.groups).map(|_| None).collect())
        .collect();
    for (l, out) in lane_outs.iter_mut().enumerate() {
        let Some(out) = out.as_mut() else { continue };
        for po in std::mem::take(&mut out.pieces) {
            piece_res[po.multi][po.group] = Some((l as u64, po.push, po.result));
        }
    }
    // A multi-push with a group a lane could not plan (it would have created
    // catalog state) is planned again, whole, by control; every lane that
    // folded one of its groups keeps nothing of this cycle.
    let mut multi_fallback: Vec<bool> = piece_res
        .iter()
        .map(|groups| {
            groups
                .iter()
                .flatten()
                .any(|(_, _, r)| matches!(r, PieceResult::Misrouted))
        })
        .collect();
    for (m, groups) in piece_res.iter().enumerate() {
        if multi_fallback[m] {
            for (l, _, _) in groups.iter().flatten() {
                if *l != CREATE_LANE {
                    lane_poisoned[*l as usize] = true;
                }
            }
        }
    }
    for (l, out) in lane_outs.into_iter().enumerate() {
        let Some(out) = out else { continue };
        lane_poisoned[l] |= out.poisoned;
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
    if LANE_STATS.maybe_log() {
        // The dedup front beside it: whether it still answers for every
        // partition (fallbacks, bytes against its cap) and how many probes
        // scan a whole window rather than one generation's band.
        let f = front.stats();
        tracing::info!(
            target: "rsm",
            partitions = f.partitions,
            fallback = f.fallback_partitions,
            bytes_mb = f.bytes >> 20,
            messages = f.messages,
            probes_issued = f.probes_issued,
            probes_whole = f.probes_whole,
            probes_skipped = f.probes_skipped,
            recovered = f.fallback_recovered,
            "dedup front (cumulative counters)",
        );
    }

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
    if !creates.is_empty() || !create_pieces.is_empty() {
        let mut spill: Vec<(usize, Command)> = Vec::new();
        let st = store.clone();
        st.read(|r| {
            let mut planner = Planner::new(
                Committed::new(r),
                now_us,
                cfg.clone(),
                front,
                reader.clone(),
            );
            planner.set_qlog_reader(qlog_reader.clone());
            // After the ids the router assigned to the lanes' own creates.
            let mut ov = Overlay::new(cycle_pid_base + local_creates, cycle_kv_base);
            ov.mark_cycle_start();
            let mut seen: HashSet<RequestId> = HashSet::new();
            let mut ours: HashSet<Pid> = HashSet::new();
            // The multi-pushes' new partitions first: the merge lays the
            // multi-pushes out before the creations, and an entry's creates are
            // dense from its pid base in entry order (I18). A group that plans
            // anything but its partition and frames sends its whole multi-push
            // to control, and every creation behind it (their ids would have
            // followed its).
            let mut multi_spilled = false;
            for Piece {
                multi, group, push, ..
            } in std::mem::take(&mut create_pieces)
            {
                if multi_spilled || multi_fallback[multi] {
                    multi_fallback[multi] = true;
                    piece_res[multi][group] = Some((CREATE_LANE, push, PieceResult::Misrouted));
                    continue;
                }
                let items_n = push.items.len();
                let result = match planner.plan_push(&mut ov, &push) {
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
                            PieceResult::Logged {
                                effects,
                                stripped: Vec::new(),
                                items: super::push_items(outcome, items_n),
                            }
                        } else {
                            multi_spilled = true;
                            multi_fallback[multi] = true;
                            PieceResult::Misrouted
                        }
                    }
                    Ok(Plan::Empty(outcome)) => {
                        PieceResult::Empty(super::push_items(outcome, items_n))
                    }
                    Ok(Plan::Refused(refusal)) | Err(refusal) => PieceResult::Refused(refusal),
                };
                piece_res[multi][group] = Some((CREATE_LANE, push, result));
            }
            for (pos, cmd) in std::mem::take(&mut creates) {
                if !spill.is_empty() || multi_spilled {
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
    // ---- multi-pushes -------------------------------------------------------
    // A multi-push whose every group planned is ONE command: its groups'
    // effects in group order, every item's verdict in input order. One that
    // cannot be logged whole goes to control, rebuilt from its groups, and
    // every lane that folded one of its groups keeps nothing of this cycle.
    let mut multi_logged: Vec<MultiLogged> = Vec::with_capacity(multis.len());
    let mut spent: Vec<PushCommand> = Vec::new();
    for (m, mr) in multis.iter().enumerate() {
        let groups: Vec<Option<(u64, PushCommand, PieceResult)>> =
            std::mem::take(&mut piece_res[m]);
        let lost = groups.iter().any(Option::is_none);
        if multi_fallback[m] || lost {
            for (l, _, _) in groups.iter().flatten() {
                if *l != CREATE_LANE && !lane_poisoned[*l as usize] {
                    lane_poisoned[*l as usize] = true;
                    ls.lanes[*l as usize].lock().expect("lane state").kept = None;
                }
            }
            if lost {
                slots[mr.pos] = Some(Slot::Immediate(Reply::Refused(Refusal::retry(
                    "internal",
                    "a multi-push group was lost between the lanes",
                ))));
            } else {
                let pushes: Vec<PushCommand> =
                    groups.into_iter().flatten().map(|(_, p, _)| p).collect();
                control.push((
                    mr.pos,
                    Command::MultiPush(MultiPushCommand {
                        request_id: mr.id,
                        pushes,
                    }),
                ));
            }
            multi_fallback[m] = true;
            continue;
        }
        let mut effects: Vec<Effect> = Vec::with_capacity(groups.len());
        let mut items: Vec<crate::rsm::entry::PushVerdict> = Vec::with_capacity(groups.len());
        let mut lane_subs: Vec<(u64, Vec<Effect>)> = Vec::new();
        let mut routed_fx: Vec<Effect> = Vec::new();
        spent.reserve(groups.len());
        for (l, push, res) in groups.into_iter().flatten() {
            match res {
                PieceResult::Logged {
                    effects: e,
                    stripped,
                    items: it,
                } => {
                    if l == CREATE_LANE {
                        routed_fx.extend(e.iter().map(strip));
                    } else {
                        match lane_subs.iter_mut().find(|(x, _)| *x == l) {
                            Some((_, v)) => v.extend(stripped),
                            None => lane_subs.push((l, stripped)),
                        }
                    }
                    effects.extend(e);
                    items.extend(it);
                }
                PieceResult::Empty(it) => items.extend(it),
                PieceResult::Refused(refusal) => {
                    items.extend(super::refused_items(&refusal, push.items.len()))
                }
                PieceResult::Misrouted => {}
            }
            spent.push(push);
        }
        let outcome = Outcome::Push(crate::rsm::entry::PushOutcome { items });
        if effects.is_empty() {
            slots[mr.pos] = Some(Slot::Empty(outcome));
        } else {
            slots[mr.pos] = Some(Slot::Logged(mr.id));
            multi_logged.push(MultiLogged {
                id: mr.id,
                outcome,
                effects,
                lane_subs,
                routed: routed_fx,
            });
        }
    }
    if !spent.is_empty() {
        drop_later(spent);
    }
    // A second copy of a multi-push in the batch answers as its first did; one
    // whose first went to control retries (control answers the first).
    for (pos, m) in std::mem::take(&mut multi_dups) {
        let first = multis[m].pos;
        slots[pos] = Some(if multi_fallback[m] {
            Slot::Immediate(Reply::Retry { hint: None })
        } else {
            match &slots[first] {
                Some(Slot::Logged(id)) => Slot::SameCycle(*id),
                Some(Slot::Empty(o)) => Slot::Empty(o.clone()),
                Some(Slot::Immediate(r)) => Slot::Immediate(r.clone()),
                _ => Slot::Immediate(Reply::Retry { hint: None }),
            }
        });
    }
    control.sort_by_key(|(pos, _)| *pos);
    // ---- control ------------------------------------------------------------
    let t_control = std::time::Instant::now();
    let needs_control = !control.is_empty()
        || !control_retention.is_empty()
        || fire.is_some()
        || maintenance.is_some()
        || kv_sweep_limit.is_some()
        || expire_window_us.is_some();
    if !needs_control {
        // Kept, advanced: the next cycle that plans control starts from here
        // (B27: without its partition state once nobody has needed it for a
        // while).
        if let Some(Ok(mut k)) = prior_control.take() {
            k.partitions(false, cycle_no);
            ls.control = Some(k);
        }
    }
    let mut control_logged: Vec<(RequestId, Outcome, Vec<Effect>)> = Vec::new();
    let mut pid_base = cycle_pid_base;
    let mut kv_base = cycle_kv_base;
    let (mut expired, mut expire_more, mut kv_swept, mut fired, mut fire_more) =
        (false, false, false, false, false);
    let (mut maintained, mut maintenance_more) = (false, false);
    // (overlay, the cycle's tag, its fold count before the cycle)
    let mut control_state: Option<(KeptOverlay, u64, u64)> = None;
    if needs_control {
        let st = store.clone();
        st.read(|r| {
            let mut kept = match prior_control.take() {
                Some(Ok(k)) => k,
                _ => KeptOverlay::rebuild_catalog(&folded, store_applied, base_pid, base_kv),
            };
            // B27: control folds the partition state of the entries in flight
            // (every message's dedup occurrence, every append and watermark)
            // only for a cycle that plans something reading it: a command that
            // may push ([`reads_partition_state`]), a fire that may push,
            // retention. In steady state control plans ~none of these, and the
            // folds were the serial planner thread's biggest piece.
            let reads_partitions = control.iter().any(|(_, c)| reads_partition_state(c))
                || maintenance.is_some()
                || !control_retention.is_empty()
                || (fire.is_some()
                    && crate::rsm::planner::timers::fire_may_push(r, kept.overlay(), now_us));
            kept.partitions(reads_partitions, cycle_no);
            let tag = kept.begin_cycle();
            let folds0 = kept.overlay().folds();
            let ov = kept.overlay_mut();
            ov.mark_cycle_start();
            pid_base = ov.cycle_pid_base();
            kv_base = ov.cycle_kv_base();
            // This cycle's lane effects come first in the entry: control plans
            // after them, seeing them.
            for (part, _) in lane_parts.iter().flatten() {
                ov.apply_effects(&part.effects);
            }
            // Then the multi-pushes, laid out right after the lanes' parts.
            for ml in &multi_logged {
                ov.apply_effects(&ml.effects);
            }
            // Then the creations: control's own partitions take the ids after.
            for (_, _, effects) in &created {
                ov.apply_effects(effects);
            }
            let mut planner = Planner::new(
                Committed::new(r),
                now_us,
                cfg.clone(),
                front,
                reader.clone(),
            );
            planner.set_qlog_reader(qlog_reader.clone());
            let mut entry = Entry::new(now_us, pid_base, kv_base);
            let mut seen: HashSet<RequestId> = lane_parts
                .iter()
                .flatten()
                .flat_map(|(part, _)| part.commands.iter().map(|c| c.request_id))
                .chain(multi_logged.iter().map(|ml| ml.id))
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
                let kind = cmd.kind();
                let t_cmd = crate::rsm::timing::enabled().then(std::time::Instant::now);
                let planned = cmd.plan(&planner, ov);
                if let Some(t) = t_cmd {
                    crate::rsm::timing::metrics()
                        .kinds
                        .record(kind, t.elapsed(), false);
                }
                let slot = match planned {
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
            // The scanner's partition deletes, judged against committed state
            // and everything in flight (a partition touched in flight is kept).
            if let (false, Some(scan)) = (control_retention.is_empty(), scan.as_ref()) {
                let (effects, _dropped) =
                    crate::rsm::retention_scan::judge(r, ov, now_us, scan, &control_retention)?;
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
            control_state = Some((kept, tag, folds0));
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
        lane_cmds as usize + multi_logged.len() + created.len() + control_logged.len(),
    );
    // Every effect the entry gets, reserved once: grown by doubling, a cycle of
    // ~20k effects was copied several times over on this thread.
    let effects_total: usize = lane_parts
        .iter()
        .flatten()
        .map(|(part, _)| part.effects.len())
        .sum::<usize>()
        + multi_logged.iter().map(|m| m.effects.len()).sum::<usize>()
        + created.iter().map(|(_, _, e)| e.len()).sum::<usize>()
        + control_logged
            .iter()
            .map(|(_, _, e)| e.len())
            .sum::<usize>();
    full.effects.reserve(effects_total);
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
    // The multi-pushes first (entry order): each lane's share goes into its
    // slice as one command under the multi-push's id, their new partitions to
    // those partitions' lanes like any creation.
    for ml in multi_logged {
        for (l, stripped) in ml.lane_subs {
            if subs[l as usize]
                .add_command(ml.id, Outcome::Empty, stripped)
                .is_err()
            {
                lane_poisoned[l as usize] = true;
            }
        }
        for e in ml.routed {
            match lanes_of(&e, n) {
                LanesOf::One(l) => routed[l as usize].push(e),
                LanesOf::All => {
                    for rt in routed.iter_mut() {
                        rt.push(e.clone());
                    }
                }
                LanesOf::None => {}
            }
        }
        if let Err(e) = full.add_command(ml.id, ml.outcome, ml.effects) {
            return Err(crate::rsm::store::StoreError::Io(format!(
                "multi-push entry build: {e:?}"
            )));
        }
    }
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
    // Lane-local creation hands out ids before the lanes plan: one a lane did
    // not end up creating (a multi-push that fell back to control after its
    // lane created a partition for it — rare by construction: the router sends
    // a lane only pushes to committed, quiet queues and small enough to plan)
    // would leave the entry's ids not dense, which no node may apply. Refuse
    // the cycle RETRYABLY instead (every lane rebuilds; the commands come back
    // with their request ids), rather than let the encoder refuse its commands
    // for good.
    if local_creates > 0 {
        let mut ids: Vec<u64> = full
            .effects
            .iter()
            .filter_map(|e| match e {
                Effect::PartitionCreate { pid, .. } => Some(*pid),
                _ => None,
            })
            .collect();
        ids.sort_unstable();
        let dense = ids
            .iter()
            .enumerate()
            .all(|(i, pid)| *pid == full.pid_base + i as u64);
        if !dense {
            ls.reset();
            return Err(crate::rsm::store::StoreError::Io(
                "a lane-assigned partition id was not created this cycle; planned again".into(),
            ));
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
        for (i, c) in full.commands.iter().enumerate() {
            ls.ids.insert(c.request_id, (full.clone(), i as u32));
        }
        ls.push_record(record_of(full.clone(), sub_arcs));
    }
    // Control keeps its overlay when it folded exactly this cycle's entry
    // (lanes, creations, control, in entry order): the entry is then in
    // flight in it. Anything else is rebuilt next time.
    if let Some((mut kept, tag, folds0)) = control_state {
        let effects = entry.as_ref().map_or(0, |e| e.effects.len()) as u64;
        let exact = kept.exact() && kept.overlay().folds() - folds0 == effects;
        let kept_ok = exact
            && match &entry {
                Some(full) => kept.push_entry(full.clone(), tag).is_ok(),
                None => true,
            };
        if kept_ok {
            ls.control = Some(kept);
        }
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
        cluster_version,
        link: None,
    })
}

#[cfg(test)]
mod router_tests {
    //! Where the router sends a command: a push to its partition's lane, to the
    //! creation step or to control; the consumption engine's checkpoints to the
    //! lane of their partitions; any consumption command to control.
    use super::*;
    use crate::rsm::planner::{EffectsCommand, PopCommand, SubIntent};
    use crate::rsm::tests::planner_harness::{self as h, Cell, TENANT};

    const N: u64 = 8;

    /// A new router cycle.
    fn cycle(ls: &mut LanesState) {
        ls.cycle = RouterCycle::default();
    }

    /// Route `cmd` inside the current cycle.
    fn route(ls: &mut LanesState, cell: &Cell, cmd: &Command) -> Dest {
        let busy = ls.busy();
        cell.node
            .store()
            .read(|r| ls.route(r, cmd, &busy))
            .expect("route")
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

        cycle(&mut ls);
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
        ls.push_record(record_of(Arc::new(entry), subs));
        cycle(&mut ls);
        assert_eq!(
            route(&mut ls, &cell, &push(14, "q", "b")),
            Dest::Lane(pb % N)
        );
    }

    fn checkpoint(id: u64, pids: &[Pid]) -> Command {
        let mut request_id = [0u8; 16];
        request_id[..8].copy_from_slice(&id.to_be_bytes());
        Command::Effects(EffectsCommand {
            request_id,
            tenant: TENANT.to_string(),
            effects: pids
                .iter()
                .map(|pid| Effect::CursorSet {
                    pid: *pid,
                    group: "g".into(),
                    row: h::cursor_row(0),
                })
                .collect(),
        })
    }

    fn pop(id: u64, queue: &str, partition: Option<&str>) -> PopCommand {
        PopCommand {
            request_id: h::rid(id),
            tenant: TENANT.to_string(),
            queue: queue.to_string(),
            partition: partition.map(str::to_string),
            group: "g".to_string(),
            worker: "w".to_string(),
            budget: 10,
            max_parts: 1,
            lease_seconds: 30,
            auto_ack: false,
            conflate: false,
            sub: SubIntent::default(),
            skip_window_debounce: false,
            namespace: String::new(),
            task: String::new(),
            create_cfg: None,
            deadline_us: 0,
            wait: false,
        }
    }

    #[test]
    fn a_checkpoint_goes_to_the_lane_of_its_partitions_and_consumption_to_control() {
        let mut cell = Cell::new("router-checkpoint");
        cell.run(&[h::push(1, "q", "a", &["a0"]), h::push(2, "q", "b", &["b0"])]);
        let pa = cell.pid_of("q", "a").expect("a");
        let pb = cell.pid_of("q", "b").expect("b");
        assert_ne!(pa % N, pb % N, "two lanes");
        let mut ls = LanesState::new(N);

        cycle(&mut ls);
        assert_eq!(
            route(&mut ls, &cell, &checkpoint(10, &[pa])),
            Dest::Lane(pa % N)
        );
        assert_eq!(
            route(&mut ls, &cell, &checkpoint(11, &[pa, pb])),
            Dest::Control,
            "cursor rows of two lanes: control"
        );
        for cmd in [
            Command::PopWildcard(pop(12, "q", None)),
            Command::PopPinned(pop(13, "q", Some("a"))),
        ] {
            assert_eq!(route(&mut ls, &cell, &cmd), Dest::Control);
        }

        // A queue delete in flight: every checkpoint goes to control, which
        // sees the delete (a cursor row of a partition going away is refused).
        let entry = cell
            .plan_entry(&[h::delete_queue(14, "q", &[pa, pb])], None)
            .expect("an entry that deletes q");
        let subs = split(&entry, N);
        ls.push_record(record_of(Arc::new(entry), subs));
        cycle(&mut ls);
        assert_eq!(route(&mut ls, &cell, &checkpoint(15, &[pa])), Dest::Control);
    }
}
