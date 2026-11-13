//! HOMES: a cycle's commands planned on every planning thread at once
//! (`PLAN_HOMES.md`; it grew out of the footprint scheduler of
//! `PLAN_SCHEDULER.md`).
//!
//! One Raft log, one entry per cycle, one clock per cycle, as always; what
//! changes is who plans. Every key has a HOME: the planning thread that owns
//! its state in flight. A partition's home comes from its NAME, without its
//! queue ([`crate::rsm::planner::homes`]), so `orders-in/order-9`,
//! `orders-out/order-9` and the KV key `order-9` share one: what one entity's
//! step touches is planned by one thread. The cycle thread looks at each
//! command once ([`Router`]) and sends it to the one home of its keys; the
//! homes plan at the same time (`queen-plan-{i}`, and the cycle thread, which
//! is home 0), each in the shards it owns, in place: no lock, no copy, no
//! merge of partition state.
//!
//! - **S1 one serial order.** In each run the homes plan first, each its own
//!   commands in batch order; then the commands several homes share (S8), in
//!   batch order; then the commands that had to wait for them (S6), in batch
//!   order, on the cycle thread. The entry holds the commands in that order.
//!   Homes share no key, so this is what the single planner plans for the same
//!   commands in that order, byte for byte but for the uuids it mints: the
//!   oracle checks it every cycle (`tests/keep_overlay.rs`). A home's new
//!   partition ids and KV versions are placeholders
//!   ([`crate::rsm::planner::PLACEHOLDER`]), numbered in entry order once the
//!   homes are done ([`Numbering`]): the creates are dense from `pid_base`,
//!   the versions are `kv_version_base + ordinal` (I18). A follower cannot
//!   tell which planner made an entry.
//! - **S2 a multi-push is one command.** Each of its pushes is a PIECE planned
//!   by its partition's home; the entry carries the multi-push whole.
//! - **S3 what has no home plans on its own**, on the cycle thread, against
//!   the kept overlay holding everything before it: an admin command (a
//!   delete, a configure: an `Effects` command of more than rows), a push that
//!   carries links, a command whose catalog read failed. It cuts the batch
//!   into runs. The leader steps follow the commands, as always (the
//!   caller's).
//! - **S4 a home stays in its shards.** A planning thread that touches a pid
//!   outside the shards it owns marks its overlay ([`Overlay::strayed`]); so
//!   does one that folded what it did not log, or logged an effect only S3
//!   plans. The run is then planned again on the cycle thread, as the single
//!   planner plans it: a routing bug costs speed, never correctness
//!   (`queen_raft_planner_cycles_total{mode="fallback"}`). Debug builds also
//!   record every key each home read and wrote, and fall back when two homes
//!   touched one ([`CHECK_KEYS`]): every test runs with it.
//! - **S5 request ids are looked up once**, on the cycle thread, before
//!   routing (committed, then in flight). A second command with an id the
//!   batch plans waits on the first's entry when the first is logged (the
//!   single planner's same-cycle waiter), and goes to the next cycle
//!   otherwise.
//! - **S6 what waits for the homes.** A command that writes a row every thread
//!   reads (a push that creates its queue, a group registration, a position),
//!   or whose keys have several homes and cannot be shared (S8), is planned
//!   after the homes of its run, on the cycle thread, with every key in view.
//! - **S7 a home lands its own shards.** The partition state of the entries
//!   that landed since the last cycle is taken out of each shard by the thread
//!   that owns it, as the first thing it does ([`land_later`]): one wake a
//!   cycle, and a shard's rows stay on one core.
//!
//! - **S8 a transaction with keys in several homes is planned by all of
//!   them.** It is cut into SHARES, one a home: its pushes, its KV ops and its
//!   timers by the home of their keys ([`TxnPart`]). Each home plans its
//!   shares after its own commands (its second round), as if every other
//!   share will hold, and says whether its own did. The cycle thread counts:
//!   every share held, and the entry takes those commands after the homes'
//!   own, each put back together in the single planner's order
//!   ([`TxnCommand::assemble_parts`]); one did not (a pushed message that is a
//!   duplicate, a lost `required` precondition, a refusal), and the second
//!   round is taken back out of every home ([`Overlay::unpark`]) and planned
//!   by the single planner's loop, which answers what it answers. The first
//!   round stays either way. Nothing waits: a home never reads another
//!   home's vote while it plans.
//!
//! A run with one home at work, or a node with one planning thread, is planned
//! by the single planner's loop.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use super::{push_items, refused_items, Command, Reply, Slot};
use crate::rsm::dedup::DedupFront;
use crate::rsm::effect::{Effect, QueueConfig};
use crate::rsm::entry::{
    Entry, KvOpOutcome, KvOutcome, Outcome, PushOutcome, PushVerdict, RequestId,
};
use crate::rsm::fasthash::FxBuild;
use crate::rsm::planner::homes::{home_of_shard, Homes};
use crate::rsm::planner::kept::{KeptOverlay, Landed};
use crate::rsm::planner::txn::{TxnCommand, TxnOutcome, TxnPart, TxnPartPlan};
use crate::rsm::planner::{
    footprint, is_placeholder, placeholder_parts, shards_of_home, KvOp, Lookup, Overlay, Parked,
    PartShard, Plan, PlanConfig, Planner, PushCommand, Refusal, ShardMask, Undo, NEW_SHARD,
};
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::segments::Reader;
use crate::rsm::state::Committed;
use crate::rsm::store::{Reads, Store};

// ---------------------------------------------------------------------------
// The planning threads
// ---------------------------------------------------------------------------

/// A planning job.
type Job = Box<dyn FnOnce() + Send>;

/// The scheduler's state on the planner thread: its planning threads. The
/// cycle thread plans groups too, so `threads` planners are `threads - 1`
/// pool threads and itself.
pub(crate) struct Sched {
    threads: usize,
    workers: Vec<mpsc::Sender<Job>>,
    /// What a cycle lets go of, for the free thread ([`Sched::discard`]).
    garbage: mpsc::Sender<Box<dyn Send>>,
}

impl Sched {
    pub(crate) fn new(threads: usize) -> Sched {
        let (garbage, rx) = mpsc::channel::<Box<dyn Send>>();
        std::thread::Builder::new()
            .name("queen-plan-free".into())
            .spawn(move || {
                while let Ok(g) = rx.recv() {
                    drop(g);
                }
            })
            .expect("spawn the planner's free thread");
        let workers = (0..threads.saturating_sub(1))
            .map(|i| {
                let (tx, rx) = mpsc::channel::<Job>();
                std::thread::Builder::new()
                    .name(format!("queen-plan-{i}"))
                    .spawn(move || {
                        super::raise_planning_thread_priority();
                        while let Ok(job) = rx.recv() {
                            job();
                        }
                    })
                    .expect("spawn a planning thread");
                tx
            })
            .collect();
        Sched {
            threads,
            workers,
            garbage,
        }
    }

    pub(crate) fn threads(&self) -> usize {
        self.threads
    }

    /// Drop `x` on the free thread. A cycle's commands and its groups'
    /// overlays are thousands of allocations, most made by other threads:
    /// freeing them here is time the cycle thread owes its next cycle.
    fn discard<T: Send + 'static>(&self, x: T) {
        let _ = self.garbage.send(Box::new(x));
    }
}

/// The partition state of the landed entries `gone` taken out of `ov`, the
/// kept overlay ([`KeptOverlay::advance_with`]), here and now: for a cycle
/// that compares its overlay at once.
pub(super) fn land(ov: &mut Overlay, gone: &[Landed]) -> Result<(), &'static str> {
    let t0 = Instant::now();
    let r = ov.unfold_parts_here(gone);
    note(&stats().land_us, t0);
    r
}

/// [`land`], later: each home takes the landed entries out of its own shards
/// as the first thing its planning thread does this cycle ([`plan_group`]),
/// and the cycle thread out of the shards of the homes with nothing to plan.
/// A cycle that plans on one thread takes them all out before it plans
/// ([`PendingLanding::here`]). Nothing reads the partition state in between:
/// the request-id lookups and the router read none.
pub(super) fn land_later(gone: &[Landed]) -> PendingLanding {
    PendingLanding {
        gone: Arc::new(gone.to_vec()),
    }
}

/// A landing still to do ([`land_later`]).
pub(super) struct PendingLanding {
    gone: Arc<Vec<Landed>>,
}

impl PendingLanding {
    /// Do the whole landing on this thread.
    pub(super) fn here(self, ov: &mut Overlay) -> Result<(), &'static str> {
        land(ov, &self.gone)
    }
}

/// Add the time since `t0` to a stats counter, in µs.
fn note(c: &AtomicU64, t0: Instant) {
    c.fetch_add(t0.elapsed().as_micros() as u64, Ordering::Relaxed);
}

/// The planner thread's share of the stats line: a whole cycle, its wait for
/// the next one, and the leader steps after the commands.
pub(super) fn note_cycle(t0: Instant) {
    note(&stats().cycle_us, t0);
}

pub(super) fn note_idle(us: u64) {
    stats().idle_us.fetch_add(us, Ordering::Relaxed);
}

pub(super) fn note_tail(t0: Instant) {
    note(&stats().tail_us, t0);
}

/// The homes' overlays into `ov`, the kept overlay they were layered over.
/// Their shards are back already, written in place: what is left to take are
/// their catalog rows (the names of the partitions they created, their KV rows,
/// their timers) and the partitions they created, filed under their numbers.
/// `false` when a placeholder has no number, or a home kept rows of a shard.
/// The overlays stay boxed, as the homes' results hold them: each is kilobytes
/// of maps.
#[allow(clippy::vec_box)]
fn merge(sched: &Sched, ov: &mut Overlay, mut deltas: Vec<Box<Overlay>>, num: &Numbering) -> bool {
    let mut ok = true;
    for d in &mut deltas {
        ok &= ov.absorb_catalog(d, &|n| num.real_pid(n), &|n| num.real_kv(n));
        for (s, sh) in d.take_written_shards() {
            if sh.is_empty() {
                continue;
            }
            if s == NEW_SHARD {
                ok &= ov.file_new(sh, &|n| num.real_pid(n));
            } else {
                // A home gives its shards back whole: none holds rows here.
                ok = false;
            }
        }
    }
    sched.discard(deltas);
    ok
}

// ---------------------------------------------------------------------------
// Units, and where each one is planned
// ---------------------------------------------------------------------------

/// What a planning thread plans: a whole command, or one push of a multi-push
/// (S2). Positions are the batch's.
#[derive(Clone, Copy, Debug)]
enum Unit {
    Cmd(usize),
    Piece { cmd: usize, push: usize },
}

/// Where a unit's keys live.
#[derive(Clone, Copy, Default)]
struct Place {
    /// The shards of the keys it names: its partitions (an existing one by its
    /// pid, a new one by its name), its KV keys and its timers (by name).
    shards: ShardMask,
    /// It writes a row every thread reads (it creates its queue, registers a
    /// group, sets a position), or a route names its partition from the
    /// payload: it is planned after the homes, on the cycle thread.
    late: bool,
}

/// Where a command is planned (S6).
enum Class {
    /// By a planning thread: each unit by the one home of its keys (a
    /// multi-push has a unit a push).
    Homes(Vec<(Unit, usize)>),
    /// By several planning threads, a share each (S8): `(home, its share)`,
    /// the homes ascending.
    Spread(Vec<(usize, TxnPart)>),
    /// After the homes of its run, on the cycle thread, with every key in view.
    Late,
    /// On its own, between runs (S3).
    Alone,
}

/// The router: one look at each command of a run, on the cycle thread, against
/// the catalog the run starts from (committed state plus the kept overlay).
/// Queue rows are cached for the run.
struct Router<'p, 'a, R: Reads + ?Sized> {
    planner: &'p Planner<'a, R>,
    ov: &'p Overlay,
    threads: usize,
    /// The first partition id no committed entry had when the cycle began: a
    /// pid at or above it was created by an entry still in flight.
    base_pid: u64,
    cfgs: HashMap<String, HashMap<String, Option<Arc<QueueConfig>>, FxBuild>, FxBuild>,
    /// A transaction's keyed elements, each with its home: kept between
    /// commands for its allocation.
    elems: Vec<(usize, Elem)>,
}

/// One keyed element of a transaction: where a share is cut (S8).
#[derive(Clone, Copy)]
enum Elem {
    Push(usize),
    Kv(usize),
    Timers,
}

/// The homes of a set of shards.
enum HomeOf {
    /// It names no shard.
    None,
    One(usize),
    Many,
}

impl<'p, 'a, R: Reads + ?Sized> Router<'p, 'a, R> {
    fn new(
        planner: &'p Planner<'a, R>,
        ov: &'p Overlay,
        threads: usize,
        base_pid: u64,
    ) -> Router<'p, 'a, R> {
        Router {
            planner,
            ov,
            threads,
            base_pid,
            cfgs: HashMap::default(),
            elems: Vec::new(),
        }
    }

    fn cfg(&mut self, t: &str, q: &str) -> Result<Option<Arc<QueueConfig>>, Refusal> {
        if let Some(c) = self.cfgs.get(t).and_then(|m| m.get(q)) {
            return Ok(c.clone());
        }
        let c = self.planner.queue_cfg(self.ov, t, q)?.map(Arc::new);
        self.cfgs
            .entry(t.to_string())
            .or_default()
            .insert(q.to_string(), c.clone());
        Ok(c)
    }

    /// Where `cmd`, at batch position `pos`, is planned.
    fn class(&mut self, pos: usize, cmd: &Command) -> Class {
        if let Command::Transaction(c) = cmd {
            return self.txn_class(pos, c);
        }
        let units: Vec<Unit> = match cmd {
            Command::MultiPush(m) => (0..m.pushes.len())
                .map(|push| Unit::Piece { cmd: pos, push })
                .collect(),
            _ => vec![Unit::Cmd(pos)],
        };
        if units.is_empty() {
            return Class::Alone;
        }
        let mut homes = Vec::with_capacity(units.len());
        for unit in units {
            // A catalog read that failed plans the command on its own, where
            // the same read refuses it as the single planner would.
            let Some(place) = self.place(cmd, unit) else {
                return Class::Alone;
            };
            if place.late {
                return Class::Late;
            }
            match self.home(place.shards, pos) {
                Some(h) => homes.push((unit, h)),
                // Its keys have two homes.
                None => return Class::Late,
            }
        }
        Class::Homes(homes)
    }

    fn homes_of(&self, shards: ShardMask) -> HomeOf {
        let mut m = shards;
        let mut home = HomeOf::None;
        while m != 0 {
            let s = m.trailing_zeros() as usize;
            m &= m - 1;
            let h = home_of_shard(s, self.threads);
            match home {
                HomeOf::One(x) if x != h => return HomeOf::Many,
                _ => home = HomeOf::One(h),
            }
        }
        home
    }

    /// The one home of `shards`. A unit that names no shard reads none, so any
    /// thread plans it: one by its position. `None` when two homes hold them.
    fn home(&self, shards: ShardMask, pos: usize) -> Option<usize> {
        match self.homes_of(shards) {
            HomeOf::One(h) => Some(h),
            HomeOf::None => Some(pos % self.threads),
            HomeOf::Many => None,
        }
    }

    /// A transaction (S6, S8): by the one home of its keys; by every home of
    /// its keys, a share each, when they have several and nothing it reads or
    /// writes is every thread's; after the homes otherwise. On its own when a
    /// push of it is (S3), or a rider of it.
    fn txn_class(&mut self, pos: usize, c: &TxnCommand) -> Class {
        self.elems.clear();
        let mut all = Place::default();
        // Whether each keyed element has one home: a share can then be cut.
        let mut cut = true;
        for (k, p) in c.pushes.iter().enumerate() {
            let mut pl = Place::default();
            if !matches!(self.push(&mut pl, p), Ok(true)) {
                return Class::Alone;
            }
            all.late |= pl.late;
            all.shards |= pl.shards;
            match self.homes_of(pl.shards) {
                HomeOf::One(h) => self.elems.push((h, Elem::Push(k))),
                _ => cut = false,
            }
        }
        for (i, op) in c.kv.iter().enumerate() {
            if let Some(s) = kv_shard(&c.tenant, op) {
                all.shards |= 1 << s;
                self.elems
                    .push((home_of_shard(s, self.threads), Elem::Kv(i)));
            }
        }
        if !c.timers.is_empty() {
            let mut pl = Place::default();
            timer_keys(&mut pl, &c.tenant, &c.timers);
            all.shards |= pl.shards;
            match self.homes_of(pl.shards) {
                HomeOf::One(h) => self.elems.push((h, Elem::Timers)),
                _ => cut = false,
            }
        }
        // A position on a partition that does not exist yet creates it, and
        // may register its group on the queue: a row every thread reads.
        all.late |= !c.positions.is_empty();
        let mut riders = Place::default();
        if !matches!(self.rows(&mut riders, &c.extra_effects), Ok(true)) {
            return Class::Alone;
        }
        all.late |= riders.late;
        all.shards |= riders.shards;
        if all.late {
            return Class::Late;
        }
        if let Some(h) = self.home(all.shards, pos) {
            return Class::Homes(vec![(Unit::Cmd(pos), h)]);
        }
        // Its keys have several homes. A rider row of a partition still in
        // flight is read in that partition's shard, which may be none of the
        // shares': the command waits for the homes then.
        if !cut || riders.shards != 0 {
            return Class::Late;
        }
        Class::Spread(shares(&mut self.elems))
    }

    /// Where a unit's keys live, or `None` when it plans on its own (S3).
    fn place(&mut self, cmd: &Command, unit: Unit) -> Option<Place> {
        let mut pl = Place::default();
        let ok = match (cmd, unit) {
            (Command::Push(c), Unit::Cmd(_)) => self.push(&mut pl, c),
            (Command::MultiPush(m), Unit::Piece { push, .. }) => match m.pushes.get(push) {
                Some(p) => self.push(&mut pl, p),
                None => Ok(false),
            },
            (Command::Kv(c), Unit::Cmd(_)) => {
                kv_keys(&mut pl, &c.tenant, &c.ops);
                Ok(true)
            }
            (Command::Timers(c), Unit::Cmd(_)) => {
                timer_keys(&mut pl, &c.tenant, &c.ops);
                Ok(true)
            }
            (Command::Effects(c), Unit::Cmd(_)) => self.rows(&mut pl, &c.effects),
            _ => Ok(false),
        };
        matches!(ok, Ok(true)).then_some(pl)
    }

    /// A push: its partition, and what its routes write. A push that creates
    /// its queue waits (every thread reads that row). One that carries links
    /// plans on its own: it may write its sources' holder lists.
    fn push(&mut self, pl: &mut Place, c: &PushCommand) -> Result<bool, Refusal> {
        if c.links.any(&c.items) {
            return Ok(false);
        }
        self.locate(pl, &c.tenant, &c.queue, &c.partition)?;
        match self.cfg(&c.tenant, &c.queue)? {
            None => pl.late = true,
            Some(cfg) => {
                if cfg.has_readers() {
                    self.routes(pl, &c.tenant, Some(&c.partition), &cfg, 0)?;
                }
            }
        }
        Ok(true)
    }

    /// What routing a message of partition `p` (`None`: of a partition named
    /// from the payload) out of a queue configured `cfg` writes, down the
    /// chain: the reader's partition of the same name, which shares the
    /// source's shard; a reader that names its partitions from the payload
    /// makes the push wait.
    fn routes(
        &mut self,
        pl: &mut Place,
        t: &str,
        p: Option<&str>,
        cfg: &QueueConfig,
        depth: usize,
    ) -> Result<(), Refusal> {
        if depth >= crate::rsm::route::MAX_CHAIN_DEPTH {
            return Ok(());
        }
        let Some(route) = cfg.route() else {
            return Ok(());
        };
        for reader in &route.readers {
            let Some(rcfg) = self.cfg(t, reader)? else {
                continue;
            };
            let Some(rroute) = rcfg.route() else {
                continue;
            };
            if rroute.catching_up {
                continue;
            }
            let tp = p.filter(|_| rroute.partition_by.is_empty());
            match tp {
                Some(tp) => self.locate(pl, t, reader, tp)?,
                None => pl.late = true,
            }
            if rcfg.has_readers() {
                self.routes(pl, t, tp, &rcfg, depth + 1)?;
            }
        }
        Ok(())
    }

    /// The rows a command carries ready-made: the consumption engine's
    /// checkpoints (an `Effects` command of cursors, dead letters and the
    /// groups a pop registered), a seek, a transaction's riders. `false` for
    /// anything else (a delete, a configure, a flag): that command plans on
    /// its own (S3).
    ///
    /// A cursor or a dead letter reads only that its partition exists
    /// (`Planner::check_partition_rows`), from the committed row: no shard.
    /// Only a partition an entry in flight created is read from the overlay,
    /// in its shard. A group registration writes its queue's groups, which
    /// every thread reads: it waits.
    fn rows(&mut self, pl: &mut Place, effects: &[Effect]) -> Result<bool, Refusal> {
        for e in effects {
            match e {
                Effect::CursorSet { pid, .. } | Effect::DlqInsert { pid, .. } => {
                    if *pid >= self.base_pid {
                        pl.shards |= self.ov.shard_bit(*pid);
                    }
                }
                Effect::CursorDelete { .. }
                | Effect::DlqDelete { .. }
                | Effect::StreamsStatePut { .. }
                | Effect::StreamsStateDelete { .. }
                | Effect::StreamsQueryUpsert { .. }
                | Effect::TraceAppend { .. }
                | Effect::TraceRecord { .. }
                | Effect::Noop => {}
                Effect::GroupUpsert { .. } | Effect::GroupDelete { .. } => pl.late = true,
                _ => return Ok(false),
            }
        }
        Ok(true)
    }

    /// The shard of partition `t/q/p`: its pid's when it exists, its name's
    /// when a push will create it (the thread that creates it files it there
    /// once the entry numbers it).
    fn locate(&mut self, pl: &mut Place, t: &str, q: &str, p: &str) -> Result<(), Refusal> {
        let shard = match self.planner.pid_of(self.ov, t, q, p)? {
            Some(pid) if !is_placeholder(pid) => self.ov.shard_of(pid),
            _ => Homes::shard_of_name(t, p),
        };
        pl.shards |= 1 << shard;
        Ok(())
    }
}

/// A transaction's keyed elements, each with its home, cut into one share a
/// home, the homes ascending. The first one leads ([`TxnPart::lead`]).
fn shares(elems: &mut Vec<(usize, Elem)>) -> Vec<(usize, TxnPart)> {
    // Stable: a home's elements stay in the command's order.
    elems.sort_by_key(|e| e.0);
    let mut parts: Vec<(usize, TxnPart)> = Vec::new();
    for (home, elem) in elems.drain(..) {
        if parts.last().is_none_or(|p| p.0 != home) {
            parts.push((home, TxnPart::default()));
        }
        let Some((_, part)) = parts.last_mut() else {
            continue;
        };
        match elem {
            Elem::Push(k) => part.pushes.push(k),
            Elem::Kv(i) => part.kv.push(i),
            Elem::Timers => part.timers = true,
        }
    }
    if let Some((_, first)) = parts.first_mut() {
        first.lead = true;
    }
    parts
}

/// The shard of a KV op's key: its name's, so an entity's key sits with the
/// partitions of that name. `None` for an op that names no key (`GetMany` and
/// `GetPrefix` are answered after apply, from applied state).
fn kv_shard(t: &str, op: &KvOp) -> Option<usize> {
    match op {
        KvOp::Put { key, .. }
        | KvOp::Delete { key, .. }
        | KvOp::Incr { key, .. }
        | KvOp::Get { key, .. }
        | KvOp::Check { key, .. } => Some(Homes::shard_of_key(t, key)),
        KvOp::GetMany { .. } | KvOp::GetPrefix { .. } => None,
    }
}

/// A KV call's keys, each in its shard ([`kv_shard`]).
fn kv_keys(pl: &mut Place, t: &str, ops: &[KvOp]) {
    for op in ops {
        if let Some(s) = kv_shard(t, op) {
            pl.shards |= 1 << s;
        }
    }
}

fn timer_keys(pl: &mut Place, t: &str, ops: &[crate::rsm::planner::timers::TimerOp]) {
    for op in ops {
        pl.shards |= 1 << Homes::shard_of_key(t, op.key());
    }
}

/// Whether a planning thread may log `e`. `false` for an effect only the
/// cycle thread plans (a delete, a purge, a flag): the run is then planned
/// again on one thread. Every variant is named, so a new one has to be placed
/// here.
fn home_may_log(e: &Effect) -> bool {
    match e {
        Effect::Noop
        | Effect::TraceAppend { .. }
        | Effect::TraceRecord { .. }
        | Effect::QueueUpsert { .. }
        | Effect::GroupUpsert { .. }
        | Effect::GroupDelete { .. }
        | Effect::PartitionCreate { .. }
        | Effect::Append { .. }
        | Effect::Watermark { .. }
        | Effect::CursorSet { .. }
        | Effect::CursorDelete { .. }
        | Effect::DlqInsert { .. }
        | Effect::DlqDelete { .. }
        | Effect::KvPut { .. }
        | Effect::KvDelete { .. }
        | Effect::TimerUpsert { .. }
        | Effect::TimerDelete { .. }
        | Effect::TimerBackoff { .. }
        | Effect::StreamsQueryUpsert { .. }
        | Effect::StreamsStatePut { .. }
        | Effect::StreamsStateDelete { .. } => true,
        Effect::QueueDelete { .. }
        | Effect::PartitionDelete { .. }
        | Effect::GarbageAdd { .. }
        | Effect::DeleteChunk { .. }
        | Effect::RequestIdsExpire { .. }
        | Effect::ClusterVersionSet { .. }
        | Effect::MembershipNote { .. }
        | Effect::TenantPurge { .. }
        | Effect::TraceExpire { .. }
        | Effect::TraceTrim { .. }
        | Effect::FlagSet { .. }
        | Effect::QuotaSet { .. }
        | Effect::EphemeralConfigSet { .. }
        | Effect::EphemeralConfigDelete { .. } => false,
    }
}

/// The keys of planner state an effect a home logged writes, for the check
/// that no other home read or wrote them (S4, debug builds). Rows the planner
/// keeps no state of (a cursor, a dead letter, a stream's cells) have none:
/// two homes may log them in one cycle, and the entry holds them in batch
/// order.
fn effect_writes(e: &Effect, out: &mut Vec<u64>) {
    match e {
        Effect::QueueUpsert { tenant, queue, .. }
        | Effect::GroupUpsert { tenant, queue, .. }
        | Effect::GroupDelete { tenant, queue, .. } => out.push(footprint::queue(tenant, queue)),
        Effect::PartitionCreate {
            pid,
            tenant,
            queue,
            partition,
            ..
        } => {
            out.push(footprint::part(tenant, queue, partition));
            out.push(footprint::names(*pid));
        }
        Effect::Append { pid, .. } | Effect::Watermark { pid, .. } => {
            out.push(footprint::log(*pid))
        }
        Effect::KvPut {
            tenant, ns, key, ..
        }
        | Effect::KvDelete { tenant, ns, key } => out.push(footprint::kv(tenant, ns, key)),
        Effect::TimerUpsert {
            tenant, queue, key, ..
        }
        | Effect::TimerDelete { tenant, queue, key }
        | Effect::TimerBackoff {
            tenant, queue, key, ..
        } => out.push(footprint::timer(tenant, queue, key)),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Planning a home's share of a run
// ---------------------------------------------------------------------------

/// A planning thread's share of a run (S6): the units whose keys live in its
/// shards, in batch order. Empty for a home with nothing to plan this run.
struct Spec {
    units: Vec<Unit>,
    /// Its shares of the commands several homes plan (S8), by batch position,
    /// in batch order: its second round, planned after its units.
    parts: Vec<(usize, TxnPart)>,
    /// The shards its planning thread owns for the run, planned in place.
    own: ShardMask,
}

impl Spec {
    /// Whether this home has anything to plan.
    fn at_work(&self) -> bool {
        !self.units.is_empty() || !self.parts.is_empty()
    }
}

/// A home's second round (S8): its shares of the commands several homes plan.
struct Second {
    /// What it planned of each share, by the command's batch position. Short
    /// of its shares when `yes` is false.
    plans: Vec<(usize, TxnPartPlan)>,
    /// Every one of its shares held.
    yes: bool,
    /// What takes the round back out, of its overlay and of its shards.
    parked: Option<Parked>,
}

/// How one unit fared.
enum Out {
    Logged {
        effects: Vec<Effect>,
        outcome: Outcome,
    },
    Empty(Outcome),
    Refused(Refusal),
    /// The planning budget was spent before it: the next cycle plans it.
    Deferred,
    /// A multi-push piece: its effects (none when it wrote nothing) and its
    /// items' verdicts; `refused` when the whole push was refused.
    Piece {
        effects: Vec<Effect>,
        items: Vec<PushVerdict>,
        refused: Option<Refusal>,
    },
}

/// What planning one home's share produced.
struct GroupOut {
    /// The home: the planning thread, and the group of its placeholders.
    group: u32,
    /// Its units, and how each fared.
    units: Vec<Unit>,
    outs: Vec<Out>,
    /// The keys its planning read, and the keys its effects write (S4; debug
    /// builds only, empty otherwise).
    reads: Vec<u64>,
    writes: Vec<u64>,
    /// It logged an effect only the cycle thread plans.
    serial: bool,
    /// It folded what it did not log, or touched a pid outside its shards.
    poisoned: bool,
    /// The partition ids and KV versions it drew.
    drawn: (u64, u64),
    /// Its overlay, let go of its base, for the kept overlay to absorb. Boxed:
    /// an overlay is kilobytes of maps, and the results are moved and sorted.
    ov: Option<Box<Overlay>>,
    /// Its shards, for the kept overlay to take back whatever the run's fate,
    /// and what undoes its folds in them if the run is planned again.
    owned: Vec<(usize, PartShard)>,
    undo: Option<Undo>,
    /// The landing it did in its shards before it planned ([`land_later`]):
    /// an error means the kept overlay is not the entries in flight.
    landed: Result<(), &'static str>,
    /// Its second round, when it had shares to plan (S8).
    second: Option<Second>,
}

/// A home's shards, with their numbers (S6).
type OwnedShards = Vec<(usize, PartShard)>;

/// What every planner of a parallel run shares.
struct Shared {
    cmds: Vec<Command>,
    /// By home.
    specs: Vec<Spec>,
    /// Each home's shards, moved out of the kept overlay before it froze, for
    /// its planning thread.
    owned: Vec<Mutex<Option<OwnedShards>>>,
    /// The entries that landed since the last cycle, when their partition
    /// state is still in the shards: each home takes its own out first.
    landing: Option<Arc<Vec<Landed>>>,
    /// S8: a share did not hold, so the second round is going back out: the
    /// homes still planning theirs stop. Read between two shares, never waited
    /// on; the vote the cycle thread counts is each home's own.
    no: AtomicBool,
    started: Instant,
    budget: Duration,
    slow_ms: u64,
    batch_len: u64,
}

/// Whether the planning threads record what they read and write, for the
/// check that no two homes touched one key (S4). On in debug builds, which is
/// where every test runs; a release build trusts the router and keeps only
/// the check of the shards ([`Overlay::strayed`]).
const CHECK_KEYS: bool = cfg!(debug_assertions);

fn plan_group<R: Reads + ?Sized>(
    planner: &Planner<'_, R>,
    base: &Arc<Overlay>,
    group: u32,
    sh: &Shared,
) -> GroupOut {
    let spec = &sh.specs[group as usize];
    let mut ov = Box::new(Overlay::layered(base.clone(), group));
    let shards = sh.owned[group as usize]
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
        .unwrap_or_default();
    ov.own(shards, spec.own);
    // The landing first, in the shards this thread owns: what it plans next
    // reads them. It is not part of what a fall-back undoes (the undo frame
    // records a partition as the first FOLD finds it).
    let landed = match &sh.landing {
        Some(gone) => ov.unfold_parts_in(gone, spec.own),
        None => Ok(()),
    };
    let _ = planner.take_touches();
    let mut outs = Vec::with_capacity(spec.units.len());
    let mut poisoned = false;
    let mut cut = false;
    // A piece is never deferred: its multi-push is logged whole. So the budget
    // defers only what comes after the home's last piece.
    let pieces_end = spec
        .units
        .iter()
        .rposition(|u| matches!(u, Unit::Piece { .. }))
        .map_or(0, |k| k + 1);
    for (k, unit) in spec.units.iter().enumerate() {
        let folds0 = ov.folds();
        let out = match *unit {
            Unit::Cmd(i) => {
                if cut && k >= pieces_end {
                    outs.push(Out::Deferred);
                    continue;
                }
                let cmd = &sh.cmds[i];
                let t0 = crate::rsm::timing::stamp();
                let out = match cmd.plan(planner, &mut ov) {
                    Ok(Plan::Logged { effects, outcome }) => Out::Logged { effects, outcome },
                    Ok(Plan::Empty(o)) => Out::Empty(o),
                    Ok(Plan::Refused(r)) | Err(r) => Out::Refused(r),
                };
                if let Some(t0) = t0 {
                    super::record_command(cmd, t0.elapsed(), sh.slow_ms, sh.batch_len);
                }
                cut = sh.started.elapsed() > sh.budget;
                out
            }
            Unit::Piece { cmd, push } => {
                let push = match &sh.cmds[cmd] {
                    Command::MultiPush(m) => m.pushes.get(push),
                    _ => None,
                };
                match push {
                    Some(p) => match planner.plan_push(&mut ov, p) {
                        Ok(Plan::Logged { effects, outcome }) => Out::Piece {
                            items: push_items(outcome, p.items.len()),
                            effects,
                            refused: None,
                        },
                        Ok(Plan::Empty(o)) => Out::Piece {
                            items: push_items(o, p.items.len()),
                            effects: Vec::new(),
                            refused: None,
                        },
                        Ok(Plan::Refused(r)) | Err(r) => Out::Piece {
                            items: refused_items(&r, p.items.len()),
                            effects: Vec::new(),
                            refused: Some(r),
                        },
                    },
                    // Unreachable: the router made the piece from the push.
                    None => Out::Refused(Refusal::retry("internal", "a piece without its push")),
                }
            }
        };
        let logged = match &out {
            Out::Logged { effects, .. } | Out::Piece { effects, .. } => effects.len() as u64,
            _ => 0,
        };
        poisoned |= ov.folds() - folds0 != logged;
        outs.push(out);
    }
    // S8, the second round: this home's shares of the commands several homes
    // plan, after its own. Each is planned as if every other share will hold.
    // The round stays in the overlay and in the shards, parked: the cycle
    // thread keeps it if every home's shares held, and takes it back out
    // otherwise.
    let mut second = None;
    if !spec.parts.is_empty() {
        let saved = ov.save();
        let mut plans = Vec::with_capacity(spec.parts.len());
        let mut yes = true;
        for (cmd, part) in &spec.parts {
            // Another home's share did not hold: the round is going back.
            if sh.no.load(Ordering::Relaxed) {
                yes = false;
                break;
            }
            let Command::Transaction(c) = &sh.cmds[*cmd] else {
                yes = false;
                break;
            };
            let folds0 = ov.folds();
            let t0 = part.lead.then(crate::rsm::timing::stamp).flatten();
            let Some(plan) = planner.plan_transaction_part(&mut ov, c, part) else {
                yes = false;
                break;
            };
            if let Some(t0) = t0 {
                super::record_command(&sh.cmds[*cmd], t0.elapsed(), sh.slow_ms, sh.batch_len);
            }
            poisoned |= ov.folds() - folds0 != plan.folded(c, part);
            plans.push((*cmd, plan));
        }
        if !yes {
            sh.no.store(true, Ordering::Relaxed);
        }
        second = Some(Second {
            plans,
            yes,
            parked: Some(ov.park(saved)),
        });
    }
    let reads = planner.take_touches();
    let drawn = ov.drawn();
    let (owned, undo) = ov.give_back();
    // A pid outside its shards: the router sent it a unit of another home.
    poisoned |= ov.strayed();
    ov.detach();
    let mut writes = Vec::new();
    let mut serial = false;
    let firsts = outs.iter().flat_map(|o| match o {
        Out::Logged { effects, .. } | Out::Piece { effects, .. } => effects.as_slice(),
        _ => &[],
    });
    let seconds = second
        .iter()
        .flat_map(|s| s.plans.iter().flat_map(|p| p.1.effects()));
    for e in firsts.chain(seconds) {
        serial |= !home_may_log(e);
        if CHECK_KEYS {
            effect_writes(e, &mut writes);
        }
    }
    GroupOut {
        group,
        units: spec.units.clone(),
        outs,
        reads,
        writes,
        serial,
        poisoned,
        drawn,
        ov: Some(ov),
        owned,
        undo,
        landed,
        second,
    }
}

/// S8: the homes' vote on a run's second round. Every share held: each shared
/// command's effects, in the single planner's order, and its outcome, in batch
/// order (`spread` gives each command's position and how many shares it has).
/// One did not: `None`, and the round is out of every home's overlay and
/// shards again, which are back in `kept`; the cycle thread plans those
/// commands then.
fn second_round(
    kept: &mut KeptOverlay,
    cmds: &[Option<Command>],
    spread: &[(usize, usize)],
    outs: &mut [GroupOut],
) -> Option<Vec<(usize, Vec<Effect>, Outcome)>> {
    if spread.is_empty() {
        return None;
    }
    let mut held = forced_fall_back() != Forced::Second
        && outs.iter().all(|o| o.second.as_ref().is_none_or(|s| s.yes));
    let mut all = Vec::with_capacity(spread.len());
    if held {
        let mut by_cmd: HashMap<usize, Vec<TxnPartPlan>, FxBuild> = HashMap::default();
        for o in outs.iter_mut() {
            if let Some(s) = &mut o.second {
                for (cmd, plan) in s.plans.drain(..) {
                    by_cmd.entry(cmd).or_default().push(plan);
                }
            }
        }
        for &(i, shares) in spread {
            let parts = by_cmd.remove(&i).unwrap_or_default();
            let planned = match &cmds[i] {
                Some(Command::Transaction(c)) if parts.len() == shares => {
                    c.assemble_parts(parts).ok()
                }
                _ => None,
            };
            match planned {
                Some((effects, outcome)) => all.push((i, effects, outcome)),
                None => {
                    held = false;
                    break;
                }
            }
        }
    }
    if held {
        note_spread(spread.len() as u64, 0);
        return Some(all);
    }
    for o in outs.iter_mut() {
        let (Some(s), Some(delta)) = (&mut o.second, o.ov.as_deref_mut()) else {
            continue;
        };
        if let Some(parked) = s.parked.take() {
            let frame = delta.unpark(parked);
            kept.overlay_mut().undo(&frame);
            o.drawn = delta.drawn();
        }
    }
    note_spread(0, 1);
    None
}

/// S4: whether one home wrote a key another home touched.
fn conflict(outs: &[GroupOut]) -> bool {
    let mut writers: HashMap<u64, u32, FxBuild> = HashMap::default();
    for o in outs {
        for k in &o.writes {
            if *writers.entry(*k).or_insert(o.group) != o.group {
                return true;
            }
        }
    }
    outs.iter().any(|o| {
        o.reads
            .iter()
            .any(|k| writers.get(k).is_some_and(|w| *w != o.group))
    })
}

// ---------------------------------------------------------------------------
// Numbering (S1)
// ---------------------------------------------------------------------------

/// The real number of each placeholder, by group and ordinal, handed out in
/// entry order ([`Numbering::assign`]); `ok` goes false on a placeholder that
/// has none.
struct Numbering {
    pids: Vec<Vec<u64>>,
    kvs: Vec<Vec<u64>>,
    ok: bool,
    /// The next partition id and KV version once the run is numbered: the
    /// kept overlay's counters after it (`None`: the run drew none).
    next: Option<(u64, u64)>,
}

/// Not handed out (yet).
const UNSET: u64 = u64::MAX;

fn give(table: &mut [Vec<u64>], n: u64, real: u64) -> bool {
    let (g, o) = placeholder_parts(n);
    let Some(t) = table.get_mut(g as usize) else {
        return false;
    };
    let o = o as usize;
    if t.len() <= o {
        t.resize(o + 1, UNSET);
    }
    if t[o] != UNSET {
        return false;
    }
    t[o] = real;
    true
}

fn real_of(table: &[Vec<u64>], n: u64) -> Option<u64> {
    let (g, o) = placeholder_parts(n);
    table
        .get(g as usize)?
        .get(o as usize)
        .copied()
        .filter(|v| *v != UNSET)
}

impl Numbering {
    fn new(groups: usize) -> Numbering {
        Numbering {
            pids: vec![Vec::new(); groups],
            kvs: vec![Vec::new(); groups],
            ok: true,
            next: None,
        }
    }

    /// Walk `effects` in entry order and give each placeholder create the
    /// next partition id and each placeholder versioned write the next KV
    /// version, from `next` (the entry's bases plus what it holds before
    /// them): the entry's creates are then dense from its `pid_base` and its
    /// versions are `kv_version_base + ordinal` (I18), and on one key the
    /// versions still rise in the order its group planned.
    fn assign(&mut self, effects: &[Effect], next: &mut (u64, u64)) {
        for e in effects {
            match e {
                Effect::PartitionCreate { pid, .. } => {
                    if is_placeholder(*pid) && !give(&mut self.pids, *pid, next.0) {
                        self.ok = false;
                    }
                    next.0 += 1;
                }
                Effect::KvPut { version, .. } => {
                    if is_placeholder(*version) && !give(&mut self.kvs, *version, next.1) {
                        self.ok = false;
                    }
                    next.1 += 1;
                }
                _ => {}
            }
        }
    }

    fn real_pid(&self, n: u64) -> Option<u64> {
        real_of(&self.pids, n)
    }

    /// The partition ids the run created, as the entry numbers them.
    fn created_pids(&self) -> Vec<u64> {
        self.pids
            .iter()
            .flatten()
            .copied()
            .filter(|&p| p != UNSET)
            .collect()
    }

    fn real_kv(&self, n: u64) -> Option<u64> {
        real_of(&self.kvs, n)
    }

    fn pid(&mut self, n: &mut u64) {
        if is_placeholder(*n) {
            match real_of(&self.pids, *n) {
                Some(v) => *n = v,
                None => self.ok = false,
            }
        }
    }

    fn kv(&mut self, n: &mut u64) {
        if is_placeholder(*n) {
            match real_of(&self.kvs, *n) {
                Some(v) => *n = v,
                None => self.ok = false,
            }
        }
    }

    /// Every pid and KV version an effect carries. Every variant is named.
    fn effect(&mut self, e: &mut Effect) {
        match e {
            Effect::PartitionCreate { pid, .. }
            | Effect::PartitionDelete { pid }
            | Effect::CursorSet { pid, .. }
            | Effect::CursorDelete { pid, .. }
            | Effect::DlqInsert { pid, .. }
            | Effect::Watermark { pid, .. }
            | Effect::StreamsStatePut { pid, .. }
            | Effect::StreamsStateDelete { pid, .. } => self.pid(pid),
            Effect::Append { pid, blob, .. } => {
                self.pid(pid);
                // A link to a message of a partition this cycle created.
                if crate::frames::any_link(blob) {
                    let (pids, mut ok) = (&self.pids, true);
                    let whole = crate::frames::relink(blob, &mut |p| {
                        if !is_placeholder(p) {
                            return p;
                        }
                        real_of(pids, p).unwrap_or_else(|| {
                            ok = false;
                            p
                        })
                    });
                    self.ok &= ok && whole;
                }
            }
            Effect::GarbageAdd { pids, .. } | Effect::DeleteChunk { pids, .. } => {
                for p in pids {
                    self.pid(p);
                }
            }
            Effect::KvPut { version, .. } => self.kv(version),
            Effect::Noop
            | Effect::QueueUpsert { .. }
            | Effect::QueueDelete { .. }
            | Effect::GroupUpsert { .. }
            | Effect::GroupDelete { .. }
            | Effect::DlqDelete { .. }
            | Effect::KvDelete { .. }
            | Effect::TimerUpsert { .. }
            | Effect::TimerDelete { .. }
            | Effect::TimerBackoff { .. }
            | Effect::StreamsQueryUpsert { .. }
            | Effect::TraceAppend { .. }
            | Effect::TraceExpire { .. }
            | Effect::FlagSet { .. }
            | Effect::QuotaSet { .. }
            | Effect::EphemeralConfigSet { .. }
            | Effect::EphemeralConfigDelete { .. }
            | Effect::RequestIdsExpire { .. }
            | Effect::ClusterVersionSet { .. }
            | Effect::MembershipNote { .. }
            | Effect::TenantPurge { .. }
            | Effect::TraceRecord { .. }
            | Effect::TraceTrim { .. } => {}
        }
    }

    fn items(&mut self, items: &mut [PushVerdict]) {
        for v in items {
            match v {
                PushVerdict::Created { pid, .. } | PushVerdict::Duplicate { pid, .. } => {
                    self.pid(pid)
                }
                PushVerdict::Refused { .. } => {}
            }
        }
    }

    fn kv_outcome(&mut self, k: &mut KvOutcome) {
        for r in &mut k.results {
            match r {
                KvOpOutcome::Got(Some(g)) => self.kv(&mut g.version),
                KvOpOutcome::Write(w) => self.kv(&mut w.version),
                KvOpOutcome::Got(None) | KvOpOutcome::Deferred => {}
            }
        }
        if let Some(f) = &mut k.failed {
            self.kv(&mut f.version);
        }
    }

    /// Every pid and KV version an outcome carries: a push's verdicts, a KV
    /// call's versions, and both inside a transaction's outcome.
    fn outcome(&mut self, o: &mut Outcome) {
        match o {
            Outcome::Push(p) => self.items(&mut p.items),
            Outcome::Kv(k) => self.kv_outcome(k),
            Outcome::Placeholder(p) => {
                let txn = p.tag() == crate::rsm::planner::txn::TXN_OUTCOME_TAG;
                let Some(mut t) = TxnOutcome::from_outcome(o) else {
                    // Another kind carries no ids; a transaction's that does
                    // not decode cannot be numbered.
                    self.ok &= !txn;
                    return;
                };
                for p in &mut t.pushes {
                    self.items(&mut p.items);
                }
                self.kv_outcome(&mut t.kv);
                match t.into_outcome() {
                    Ok(n) => *o = n,
                    Err(_) => self.ok = false,
                }
            }
            Outcome::Empty
            | Outcome::Pop(_)
            | Outcome::Ack(_)
            | Outcome::Renew(_)
            | Outcome::DlqHead(_) => {}
        }
    }
}

// ---------------------------------------------------------------------------
// The cycle
// ---------------------------------------------------------------------------

/// What one cycle's commands need, besides the batch.
pub(super) struct Cx<'c> {
    pub(super) now_us: i64,
    pub(super) cfg: &'c PlanConfig,
    pub(super) reader: &'c Option<Reader>,
    pub(super) qlog_reader: &'c Option<QLogReader>,
    pub(super) started: Instant,
    pub(super) budget: Duration,
    /// The committed partition counter the cycle read when it began: a pid at
    /// or above it was created by an entry still in flight.
    pub(super) base_pid: u64,
}

/// A run of the batch between two commands that plan on their own: who plans
/// what (S6).
struct Run {
    /// The commands the homes plan, in batch order.
    first: Vec<usize>,
    /// Each home's units, in batch order.
    by_home: Vec<Vec<Unit>>,
    /// The commands several homes plan, a share each (S8), in batch order:
    /// each one's position and how many shares it has.
    spread: Vec<(usize, usize)>,
    /// Each home's shares, in batch order.
    parts: Vec<Vec<(usize, TxnPart)>>,
    /// The commands planned after the homes, on the cycle thread, in batch
    /// order.
    late: Vec<usize>,
}

impl Run {
    fn new(threads: usize) -> Run {
        Run {
            first: Vec::new(),
            by_home: (0..threads).map(|_| Vec::new()).collect(),
            spread: Vec::new(),
            parts: (0..threads).map(|_| Vec::new()).collect(),
            late: Vec::new(),
        }
    }

    /// Every command of `positions` after the homes: the single planner's
    /// loop, in batch order.
    fn all_late(threads: usize, positions: Vec<usize>) -> Run {
        Run {
            late: positions,
            ..Run::new(threads)
        }
    }

    fn is_empty(&self) -> bool {
        self.first.is_empty() && self.spread.is_empty() && self.late.is_empty()
    }

    /// Command `pos` goes where `class` says. `false` for a command that plans
    /// on its own: it is in no run.
    fn place(&mut self, pos: usize, class: Class) -> bool {
        match class {
            Class::Alone => return false,
            Class::Late => self.late.push(pos),
            Class::Homes(units) => {
                self.first.push(pos);
                for (unit, home) in units {
                    self.by_home[home].push(unit);
                }
            }
            Class::Spread(shares) => {
                self.spread.push((pos, shares.len()));
                for (home, part) in shares {
                    self.parts[home].push((pos, part));
                }
            }
        }
        true
    }

    /// How many homes have something to plan.
    fn homes_at_work(&self) -> usize {
        self.by_home
            .iter()
            .zip(&self.parts)
            .filter(|(units, parts)| !units.is_empty() || !parts.is_empty())
            .count()
    }

    /// The positions of the commands several homes plan, in batch order.
    fn shared(&self) -> Vec<usize> {
        self.spread.iter().map(|s| s.0).collect()
    }

    /// The run's commands, in batch order.
    fn positions(&self) -> Vec<usize> {
        let mut all: Vec<usize> = self
            .first
            .iter()
            .copied()
            .chain(self.shared())
            .chain(self.late.iter().copied())
            .collect();
        all.sort_unstable();
        all
    }
}

/// A piece of the batch: a run, or one command planned on its own (S3).
enum Seg {
    Run(Run),
    Alone(usize),
}

/// Route the commands at `positions` (batch order) again, against `ov` as it
/// is now. `None` when one of them plans on its own now.
fn route_again<R: Reads + ?Sized>(
    planner: &Planner<'_, R>,
    ov: &Overlay,
    threads: usize,
    base_pid: u64,
    cmds: &[Option<Command>],
    positions: &[usize],
) -> Option<Run> {
    let mut router = Router::new(planner, ov, threads, base_pid);
    let mut run = Run::new(threads);
    for &i in positions {
        let Some(cmd) = cmds[i].as_ref() else {
            continue;
        };
        let class = router.class(i, cmd);
        if !run.place(i, class) {
            return None;
        }
    }
    Some(run)
}

/// Plan the cycle's commands (the scheduler's share of
/// [`super::plan_cycle_blocking`]): their slots in batch order, the entry
/// filled with what they log, the kept overlay folded with it. The leader
/// steps are the caller's, after.
///
/// The entry holds each run's commands in the order they were planned in: the
/// homes' commands in batch order, then the commands several homes shared,
/// then the commands that waited for the homes, each in batch order (S1).
#[allow(clippy::too_many_arguments)]
pub(super) fn plan_commands<S: Store + 'static, R: Reads + ?Sized>(
    sched: &Sched,
    store: &Arc<S>,
    front: &Arc<DedupFront>,
    planner: &Planner<'_, R>,
    r: &R,
    kept: &mut KeptOverlay,
    entry: &mut Entry,
    batch: Vec<Command>,
    cx: &Cx<'_>,
    unkept: &mut bool,
    landing: &mut Option<PendingLanding>,
) -> Result<(Vec<Slot>, Mode), &'static str> {
    let t0 = Instant::now();
    let n = batch.len();
    let threads = sched.threads();
    let mut slots: Vec<Option<Slot>> = (0..n).map(|_| None).collect();
    let mut cmds: Vec<Option<Command>> = batch.into_iter().map(Some).collect();

    // S5: request ids, once. `firsts` maps an id the batch plans to the
    // position planning it; a later command with it waits for the end.
    let mut firsts: HashMap<RequestId, usize, FxBuild> = HashMap::default();
    let mut dups: Vec<(usize, usize)> = Vec::new();
    let mut plan: Vec<usize> = Vec::with_capacity(n);
    for (i, c) in cmds.iter().enumerate() {
        let Some(cmd) = c else { continue };
        let id = cmd.request_id();
        match planner.lookup_request_id(kept.overlay(), &id) {
            Err(refusal) => slots[i] = Some(Slot::Immediate(Reply::Refused(refusal))),
            Ok(Lookup::Committed(outcome)) => {
                slots[i] = Some(Slot::Immediate(Reply::Done { outcome, at: None }))
            }
            Ok(Lookup::InFlight(outcome)) => {
                slots[i] = Some(Slot::InFlightHit {
                    request_id: id,
                    outcome,
                })
            }
            Ok(Lookup::Miss) => match firsts.get(&id) {
                Some(&first) => dups.push((i, first)),
                None => {
                    firsts.insert(id, i);
                    plan.push(i);
                }
            },
        }
    }

    // The batch cut into runs at the commands that plan on their own (S3),
    // and each command of a run sent to its home or kept for after the homes
    // (S6).
    let mut segs: Vec<Seg> = Vec::new();
    {
        let mut router = Router::new(planner, kept.overlay(), threads, cx.base_pid);
        let mut run = Run::new(threads);
        for &i in &plan {
            let cmd = cmds[i].as_ref().expect("planned once");
            let class = router.class(i, cmd);
            if !run.place(i, class) {
                if !run.is_empty() {
                    segs.push(Seg::Run(std::mem::replace(&mut run, Run::new(threads))));
                }
                segs.push(Seg::Alone(i));
            }
        }
        if !run.is_empty() {
            segs.push(Seg::Run(run));
        }
    }
    let route_us = t0.elapsed().as_micros() as u64;

    let mut cut = false;
    let mut mode = Mode::Serial;
    let mut after_alone = false;
    for seg in segs {
        let mut run = match seg {
            Seg::Alone(i) => {
                // Nothing plans before the landed entries are out.
                if let Some(p) = landing.take() {
                    p.here(kept.overlay_mut())?;
                }
                plan_serial(
                    planner,
                    kept.overlay_mut(),
                    entry,
                    &mut cmds,
                    &[i],
                    &mut slots,
                    cx,
                    n,
                    &mut cut,
                );
                note_order(&[i]);
                after_alone = true;
                continue;
            }
            Seg::Run(run) => run,
        };
        // A command planned alone before this run may have changed what the
        // router read when the cycle began (a queue it dropped, which a push
        // here creates again): the run is routed again, against the overlay
        // as it is now. A command that plans alone now sends the whole run to
        // the single planner's loop.
        if after_alone {
            let positions = run.positions();
            run = route_again(
                planner,
                kept.overlay(),
                threads,
                cx.base_pid,
                &cmds,
                &positions,
            )
            .unwrap_or_else(|| Run::all_late(threads, positions));
        }
        // One home with work plans on the cycle thread, against the kept
        // overlay: no thread to wake.
        let parallel = threads > 1 && !cut && run.homes_at_work() > 1;
        if !parallel {
            if let Some(p) = landing.take() {
                p.here(kept.overlay_mut())?;
            }
        }
        let shared = run.shared();
        if parallel {
            let had: Vec<bool> = cmds.iter().map(Option::is_some).collect();
            let owned: Vec<Command> = cmds
                .iter_mut()
                .map(|c| c.take().unwrap_or_else(placeholder_command))
                .collect();
            let specs: Vec<Spec> = std::mem::take(&mut run.by_home)
                .into_iter()
                .zip(std::mem::take(&mut run.parts))
                .enumerate()
                .map(|(home, (units, parts))| Spec {
                    units,
                    parts,
                    own: shards_of_home(home, threads),
                })
                .collect();
            stats()
                .shared
                .fetch_add(run.late.len() as u64, Ordering::Relaxed);
            // The homes take the landed entries out of their own shards first.
            let (back, result) = plan_parallel(
                sched,
                store,
                front,
                planner,
                r,
                kept,
                owned,
                specs,
                landing.take(),
                cx,
                n as u64,
            )?;
            for ((slot, c), had) in cmds.iter_mut().zip(back).zip(had) {
                if had {
                    *slot = Some(c);
                }
            }
            if let Some(mut outs) = result {
                let (c0, e0) = (entry.commands.len(), entry.effects.len());
                // S8: the homes' vote on the second round. Held, the entry
                // takes the shared commands after the homes' own; not held,
                // the round is out of the homes again and the commands are
                // planned here, after the entry took the first round.
                let second = second_round(kept, &cmds, &run.spread, &mut outs);
                let held = second.is_some();
                let deltas: Vec<Option<Box<Overlay>>> =
                    outs.iter_mut().map(|o| o.ov.take()).collect();
                // What takes the homes' folds back out of their shards, should
                // the entry not take their results.
                let undos: Vec<Undo> = outs.iter_mut().filter_map(|o| o.undo.take()).collect();
                let mut deferred = Vec::new();
                let t_asm = Instant::now();
                let assembled = if forced_fall_back() == Forced::Assembled {
                    None
                } else {
                    assemble(
                        planner,
                        entry,
                        &cmds,
                        &run.first,
                        outs,
                        second.unwrap_or_default(),
                        threads,
                        &mut slots,
                        &mut deferred,
                    )
                };
                note(&stats().asm_us, t_asm);
                if let Some(num) = assembled {
                    mode = mode.max(Mode::Parallel);
                    for i in deferred {
                        if let Some(cmd) = cmds[i].take() {
                            slots[i] = Some(Slot::Deferred(Box::new(cmd)));
                        }
                    }
                    // The kept overlay takes what the homes folded: what a
                    // single planner's overlay holds after the same commands.
                    // Their shards are back already, written in place; what is
                    // left to take are their catalog rows and the partitions
                    // they created.
                    let ov = kept.overlay_mut();
                    let t_merge = Instant::now();
                    if !merge(sched, ov, deltas.into_iter().flatten().collect(), &num) {
                        tracing::error!(
                            target: "rsm",
                            "planner: a home's placeholder has no number; the kept overlay is rebuilt",
                        );
                        *unkept = true;
                    }
                    stats()
                        .merge_us
                        .fetch_add(t_merge.elapsed().as_micros() as u64, Ordering::Relaxed);
                    // A version a later delete of its key replaced still
                    // counts: the counters are what the run handed out.
                    if let Some((pid, kv)) = num.next {
                        ov.raise_counters(pid, kv);
                    }
                    // A partition a home created skipped the dedup front: its
                    // id was a placeholder. Its number may be one the front
                    // still holds from a create that was planned and dropped (a
                    // transaction rolled back, a cycle not proposed), as a
                    // seeded partition without these hashes. Forgotten, its
                    // next push seeds it from committed state and the kept
                    // overlay, which holds its appends now.
                    let created = num.created_pids();
                    if !created.is_empty() {
                        planner.front().forget(&created);
                    }
                    cut = cx.started.elapsed() > cx.budget;
                    note_order(&run.first);
                    // A second round that did not hold: its commands, whole,
                    // against everything the homes' own commands folded.
                    if !held {
                        plan_serial(
                            planner,
                            kept.overlay_mut(),
                            entry,
                            &mut cmds,
                            &shared,
                            &mut slots,
                            cx,
                            n,
                            &mut cut,
                        );
                    }
                    note_order(&shared);
                    // The commands that waited for the homes, with every key
                    // in view.
                    plan_serial(
                        planner,
                        kept.overlay_mut(),
                        entry,
                        &mut cmds,
                        &run.late,
                        &mut slots,
                        cx,
                        n,
                        &mut cut,
                    );
                    note_order(&run.late);
                    continue;
                }
                // The entry did not take the homes' results: nothing of them
                // stays, in the entry or in their shards.
                entry.commands.truncate(c0);
                entry.effects.truncate(e0);
                for i in run.first.iter().chain(&shared) {
                    slots[*i] = None;
                }
                for u in &undos {
                    kept.overlay_mut().undo(u);
                }
            }
            mode = Mode::Fallback;
        }
        plan_serial(
            planner,
            kept.overlay_mut(),
            entry,
            &mut cmds,
            &run.first,
            &mut slots,
            cx,
            n,
            &mut cut,
        );
        note_order(&run.first);
        plan_serial(
            planner,
            kept.overlay_mut(),
            entry,
            &mut cmds,
            &shared,
            &mut slots,
            cx,
            n,
            &mut cut,
        );
        note_order(&shared);
        plan_serial(
            planner,
            kept.overlay_mut(),
            entry,
            &mut cmds,
            &run.late,
            &mut slots,
            cx,
            n,
            &mut cut,
        );
        note_order(&run.late);
    }

    // A cycle with nothing to plan still lands.
    if let Some(p) = landing.take() {
        p.here(kept.overlay_mut())?;
    }

    // S5: a later command with an id the batch planned.
    for (i, first) in dups {
        let Some(cmd) = cmds[i].take() else { continue };
        slots[i] = Some(match &slots[first] {
            Some(Slot::Logged(id)) => Slot::SameCycle(*id),
            _ => Slot::Deferred(Box::new(cmd)),
        });
    }
    // What is left are the planned commands: their frames are in the entry.
    sched.discard(cmds);

    let st = stats();
    st.cycles(mode).fetch_add(1, Ordering::Relaxed);
    st.route_us.fetch_add(route_us, Ordering::Relaxed);
    st.maybe_log();
    if crate::rsm::timing::enabled() {
        let tm = crate::rsm::timing::metrics();
        match mode {
            Mode::Parallel => &tm.planner_parallel,
            Mode::Serial => &tm.planner_serial,
            Mode::Fallback => &tm.planner_fallback,
        }
        .fetch_add(1, Ordering::Relaxed);
    }
    let slots = slots
        .into_iter()
        .map(|s| {
            s.unwrap_or_else(|| {
                Slot::Immediate(Reply::Refused(Refusal::retry(
                    "internal",
                    "a command the scheduler did not answer",
                )))
            })
        })
        .collect();
    Ok((slots, mode))
}

/// The single planner's loop over `positions`: each command whole, against
/// the kept overlay, logged into the entry in order. Past the planning budget
/// the rest go to the next cycle (one command at least, as the single planner
/// does).
#[allow(clippy::too_many_arguments)]
fn plan_serial<R: Reads + ?Sized>(
    planner: &Planner<'_, R>,
    ov: &mut Overlay,
    entry: &mut Entry,
    cmds: &mut [Option<Command>],
    positions: &[usize],
    slots: &mut [Option<Slot>],
    cx: &Cx<'_>,
    batch_len: usize,
    cut: &mut bool,
) {
    for &i in positions {
        let Some(cmd) = cmds[i].take() else { continue };
        if *cut {
            slots[i] = Some(Slot::Deferred(Box::new(cmd)));
            continue;
        }
        let id = cmd.request_id();
        let t1 = crate::rsm::timing::stamp();
        let slot = match cmd.plan(planner, ov) {
            Ok(Plan::Logged { effects, outcome }) => {
                match entry.add_command(id, outcome, effects) {
                    Ok(()) => Slot::Logged(id),
                    Err(e) => Slot::Immediate(Reply::Refused(Refusal::retry(
                        "internal",
                        format!("entry build: {e:?}"),
                    ))),
                }
            }
            Ok(Plan::Empty(outcome)) => Slot::Empty(outcome),
            Ok(Plan::Refused(refusal)) | Err(refusal) => Slot::Immediate(Reply::Refused(refusal)),
        };
        if let Some(t1) = t1 {
            super::record_command(&cmd, t1.elapsed(), cx.cfg.slow_command_ms, batch_len as u64);
        }
        slots[i] = Some(slot);
        *cut = cx.started.elapsed() > cx.budget;
    }
}

/// What stands in for a command a run's planners do not read (one another
/// run plans, or one the request-id lookup answered): the planners only read
/// the commands their groups name.
fn placeholder_command() -> Command {
    Command::Effects(crate::rsm::planner::EffectsCommand {
        request_id: Default::default(),
        tenant: String::new(),
        effects: Vec::new(),
    })
}

/// How a cycle was planned, for the metrics: worst wins.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum Mode {
    Serial,
    Parallel,
    Fallback,
}

/// A run's homes, each planned by its own thread (the cycle thread is home 0)
/// against the frozen kept overlay, in the shards it owns. The commands come
/// back with the homes' results, `None` when the run must be planned on one
/// thread; the kept overlay is back in `kept` either way, and then holds what
/// the homes folded in their shards only if the result is `Some`.
#[allow(clippy::too_many_arguments)]
fn plan_parallel<S: Store + 'static, R: Reads + ?Sized>(
    sched: &Sched,
    store: &Arc<S>,
    front: &Arc<DedupFront>,
    planner: &Planner<'_, R>,
    r: &R,
    kept: &mut KeptOverlay,
    cmds: Vec<Command>,
    specs: Vec<Spec>,
    landing: Option<PendingLanding>,
    cx: &Cx<'_>,
    batch_len: u64,
) -> Result<(Vec<Command>, Option<Vec<GroupOut>>), &'static str> {
    let t0 = Instant::now();
    let landing = landing.map(|p| p.gone);
    let at_work: Vec<usize> = (0..specs.len()).filter(|h| specs[*h].at_work()).collect();
    // Each home's shards leave the kept overlay before it freezes: the frozen
    // overlay holds nothing of them while their planning thread writes them.
    // A home with nothing to plan leaves its shards there, and nobody reads
    // them: another home that tried would have strayed. With a landing to do,
    // those shards leave too: the cycle thread lands them while the homes
    // plan (S7), instead of after them.
    let owned_all: ShardMask = at_work.iter().fold(0, |m, h| m | specs[*h].own);
    let owned: Vec<Mutex<Option<OwnedShards>>> = specs
        .iter()
        .map(|s| Mutex::new(s.at_work().then(|| kept.overlay_mut().take_shards(s.own))))
        .collect();
    let idle_mask: ShardMask = if landing.is_some() { !owned_all } else { 0 };
    let idle = (idle_mask != 0).then(|| kept.overlay_mut().take_shards(idle_mask));
    #[cfg(test)]
    TEST_BINS.with(|c| c.set(c.get() + at_work.len() as u64));
    stats()
        .bins
        .fetch_add(at_work.len() as u64, Ordering::Relaxed);
    let sh = Arc::new(Shared {
        cmds,
        specs,
        owned,
        landing: landing.clone(),
        no: AtomicBool::new(false),
        started: cx.started,
        budget: cx.budget,
        slow_ms: cx.cfg.slow_command_ms,
        batch_len,
    });
    let base = Arc::new(std::mem::replace(kept.overlay_mut(), Overlay::new(0, 0)));
    let (tx, rx) = mpsc::channel::<crate::rsm::store::Result<GroupOut>>();
    let mut sent = 0usize;
    let mut lost = false;
    // Home h > 0 is always planning thread h - 1: its shards stay on one core.
    for &home in at_work.iter().filter(|h| **h > 0) {
        let Some(w) = sched.workers.get(home - 1) else {
            lost = true;
            continue;
        };
        let (store, front, sh, base, tx) = (
            store.clone(),
            front.clone(),
            sh.clone(),
            base.clone(),
            tx.clone(),
        );
        let (cfg, reader, qlog) = (cx.cfg.clone(), cx.reader.clone(), cx.qlog_reader.clone());
        let (now_us, base_pid) = (cx.now_us, cx.base_pid);
        let job: Job = Box::new(move || {
            let out = store.read(|r| {
                let mut p = Planner::new(Committed::new(r), now_us, cfg, &front, reader);
                p.set_qlog_reader(qlog);
                p.set_base_pid(base_pid);
                if CHECK_KEYS {
                    p.record_touches();
                }
                Ok(plan_group(&p, &base, home as u32, &sh))
            });
            // Before answering: the cycle takes the overlay and the commands
            // back as soon as every answer is in.
            drop(base);
            drop(sh);
            let _ = tx.send(out);
        });
        if w.send(job).is_ok() {
            sent += 1;
        } else {
            lost = true;
        }
    }
    drop(tx);
    // The cycle thread is home 0, with a planner of its own.
    let mut outs = Vec::with_capacity(at_work.len());
    if at_work.first() == Some(&0) {
        let mut p = Planner::new(
            Committed::new(r),
            cx.now_us,
            cx.cfg.clone(),
            planner.front(),
            cx.reader.clone(),
        );
        p.set_qlog_reader(cx.qlog_reader.clone());
        p.set_base_pid(cx.base_pid);
        if CHECK_KEYS {
            p.record_touches();
        }
        outs.push(plan_group(&p, &base, 0, &sh));
    }
    // S7: the shards of the homes with nothing to plan, landed here while the
    // planning threads work, through an overlay that owns them as a home's
    // owns its own.
    let mut idle_back: OwnedShards = Vec::new();
    let mut idle_landed = Ok(());
    if let (Some(shards), Some(gone)) = (idle, &landing) {
        let t_land = Instant::now();
        let mut ov = Overlay::layered(base.clone(), 0);
        ov.own(shards, idle_mask);
        idle_landed = ov.unfold_parts_in(gone, idle_mask);
        idle_back = ov.give_back().0;
        note(&stats().land_us, t_land);
    }
    let mut failed = lost;
    for _ in 0..sent {
        match rx.recv() {
            Ok(Ok(out)) => outs.push(out),
            Ok(Err(_)) | Err(_) => failed = true,
        }
    }
    stats()
        .group_us
        .fetch_add(t0.elapsed().as_micros() as u64, Ordering::Relaxed);
    // Every planner has dropped its share: both come back whole.
    let ov = Arc::try_unwrap(base).unwrap_or_else(|shared| {
        tracing::warn!(target: "rsm", "planner: the overlay was still shared; copied");
        (*shared).clone()
    });
    *kept.overlay_mut() = ov;
    // Every home's shards come back, whatever the run's fate: from the planner
    // that took them, or from their slot if none did.
    let mut back: ShardMask = 0;
    for slot in &sh.owned {
        if let Some(shards) = slot.lock().unwrap_or_else(|e| e.into_inner()).take() {
            back |= put_back(kept.overlay_mut(), shards);
        }
    }
    for o in &mut outs {
        back |= put_back(kept.overlay_mut(), std::mem::take(&mut o.owned));
    }
    back |= put_back(kept.overlay_mut(), idle_back);
    assert_eq!(
        back,
        owned_all | idle_mask,
        "a planning thread's shard did not come back"
    );
    // The landing: each home did its shards, and the cycle thread those of the
    // homes with nothing to plan. A landing that does not find what it takes
    // out means the kept overlay is not the entries in flight: the cycle
    // fails, and the next one rebuilds.
    if landing.is_some() {
        if failed || outs.len() != at_work.len() {
            return Err("a planning thread was lost with a landing in its shards");
        }
        for o in &outs {
            o.landed?;
        }
        idle_landed?;
    }
    let cmds = match Arc::try_unwrap(sh) {
        Ok(sh) => sh.cmds,
        Err(shared) => {
            tracing::warn!(target: "rsm", "planner: the commands were still shared; copied");
            shared.cmds.clone()
        }
    };
    outs.sort_by_key(|o| o.group);
    if failed
        || outs.len() != at_work.len()
        || outs.iter().any(|o| o.poisoned || o.serial)
        // A home that holds what only a fold applies (a sweep): none can, the
        // cycle thread plans those (S3).
        || outs
            .iter()
            .any(|o| !o.ov.as_deref().is_some_and(Overlay::absorbable))
        || (CHECK_KEYS && conflict(&outs))
        || forced_fall_back() == Forced::Planned
    {
        // The run is planned again on one thread: what the homes folded in
        // their shards is taken back out first.
        for o in &outs {
            if let Some(u) = &o.undo {
                kept.overlay_mut().undo(u);
            }
        }
        return Ok((cmds, None));
    }
    Ok((cmds, Some(outs)))
}

/// TEST ONLY: where a parallel run is made to fall back.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Forced {
    No,
    /// Once the homes have planned: their folds are undone.
    Planned,
    /// Once the homes have planned and the entry refuses their results.
    Assembled,
    /// Once the homes have planned: their second round is taken back, as when
    /// a share did not hold (S8).
    Second,
}

#[cfg(test)]
thread_local! {
    /// TEST ONLY: the homes planned on this cycle thread, a switch that sends
    /// every parallel run back to one thread once it has planned (the path a
    /// unit sent to the wrong home takes), and the order the cycles planned
    /// their commands in.
    static TEST_BINS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static TEST_FALL_BACK: std::cell::Cell<Forced> = const { std::cell::Cell::new(Forced::No) };
    static TEST_ORDER: std::cell::RefCell<Vec<usize>> = const { std::cell::RefCell::new(Vec::new()) };
    static TEST_SPREAD: std::cell::Cell<(u64, u64)> = const { std::cell::Cell::new((0, 0)) };
}

/// TEST ONLY: the commands the cycles on this thread planned by shares and
/// kept (S8), and the second rounds they took back.
#[cfg(test)]
pub(crate) fn spread_planned() -> (u64, u64) {
    TEST_SPREAD.with(|c| c.get())
}

/// TEST ONLY: how many homes the parallel runs on this thread have planned.
#[cfg(test)]
pub(crate) fn bins_planned() -> u64 {
    TEST_BINS.with(|c| c.get())
}

/// TEST ONLY: every parallel run on this thread falls back there, or none does.
#[cfg(test)]
pub(crate) fn fall_back_at(at: Forced) {
    TEST_FALL_BACK.with(|c| c.set(at));
}

/// TEST ONLY: the batch positions the cycles on this thread planned since the
/// last call, in the order they planned them (the order the entry holds).
#[cfg(test)]
pub(crate) fn take_planned_order() -> Vec<usize> {
    TEST_ORDER.with(|o| std::mem::take(&mut *o.borrow_mut()))
}

#[cfg(test)]
fn forced_fall_back() -> Forced {
    TEST_FALL_BACK.with(|c| c.get())
}

#[cfg(not(test))]
fn forced_fall_back() -> Forced {
    Forced::No
}

/// The commands at `positions` were planned, in this order.
#[cfg(test)]
fn note_order(positions: &[usize]) {
    TEST_ORDER.with(|o| o.borrow_mut().extend_from_slice(positions));
}

#[cfg(not(test))]
fn note_order(_positions: &[usize]) {}

/// S8: `kept` commands were planned by shares, or `back` second rounds were
/// taken back.
fn note_spread(kept: u64, back: u64) {
    #[cfg(test)]
    TEST_SPREAD.with(|c| {
        let (k, b) = c.get();
        c.set((k + kept, b + back));
    });
    stats().spread.fetch_add(kept, Ordering::Relaxed);
    stats().taken_back.fetch_add(back, Ordering::Relaxed);
}

/// Put a home's shards back into the kept overlay; their mask.
fn put_back(ov: &mut Overlay, shards: Vec<(usize, PartShard)>) -> ShardMask {
    let mask = shards.iter().fold(0, |m, (s, _)| m | (1 << s));
    ov.put_shards(shards);
    mask
}

/// S1, S2, S8: a run's results into the entry, numbered, and their slots; the
/// numbering, for the kept overlay. The homes' own commands first, in batch
/// order (each multi-push whole, at its place), then `spread`, the commands
/// they shared and kept ([`second_round`]). `None` when a placeholder could
/// not be numbered or a result could not be logged as planned: the caller
/// drops what this added and plans the run on one thread.
#[allow(clippy::too_many_arguments)]
fn assemble<R: Reads + ?Sized>(
    planner: &Planner<'_, R>,
    entry: &mut Entry,
    cmds: &[Option<Command>],
    positions: &[usize],
    outs: Vec<GroupOut>,
    spread: Vec<(usize, Vec<Effect>, Outcome)>,
    groups: usize,
    slots: &mut [Option<Slot>],
    deferred: &mut Vec<usize>,
) -> Option<Numbering> {
    let drew = outs.iter().any(|o| o.drawn != (0, 0));
    let mut singles: HashMap<usize, Out, FxBuild> = HashMap::default();
    let mut pieces: HashMap<usize, Vec<(usize, Out)>, FxBuild> = HashMap::default();
    for o in outs {
        for (unit, out) in o.units.into_iter().zip(o.outs) {
            match unit {
                Unit::Cmd(i) => {
                    singles.insert(i, out);
                }
                Unit::Piece { cmd, push } => pieces.entry(cmd).or_default().push((push, out)),
            }
        }
    }
    let runs = planner
        .cluster_version()
        .map(|v| crate::rsm::effect::cluster_allows(v, crate::rsm::effect::VERSION_7))
        .unwrap_or(false);
    let (c0, e0) = (entry.commands.len(), entry.effects.len());
    let mut empties: Vec<usize> = Vec::new();
    // What a group folded is in the entry, or the kept overlay would not be
    // the fold of its entries: a result that cannot be logged as planned
    // sends the run to the single planner's loop.
    let whole = std::cell::Cell::new(true);
    let log = |entry: &mut Entry, i: usize, effects: Vec<Effect>, outcome: Outcome| {
        let id = cmds[i]
            .as_ref()
            .map(Command::request_id)
            .unwrap_or_default();
        match entry.add_command(id, outcome, effects) {
            Ok(()) => Slot::Logged(id),
            Err(e) => {
                whole.set(false);
                Slot::Immediate(Reply::Refused(Refusal::retry(
                    "internal",
                    format!("entry build: {e:?}"),
                )))
            }
        }
    };
    for &i in positions {
        if let Some(mut parts) = pieces.remove(&i) {
            // A multi-push, as `MultiPushCommand::plan` builds it.
            parts.sort_by_key(|p| p.0);
            let single = parts.len() == 1;
            let mut effects = Vec::new();
            let mut items = Vec::new();
            let mut refused_whole = None;
            for (_, out) in parts {
                match out {
                    Out::Piece {
                        effects: e,
                        items: it,
                        refused,
                    } => {
                        if single {
                            refused_whole = refused;
                        }
                        effects.extend(e);
                        items.extend(it);
                    }
                    _ => {
                        whole.set(false);
                        refused_whole =
                            Some(Refusal::retry("internal", "a piece planned as a command"))
                    }
                }
            }
            slots[i] = Some(if let Some(r) = refused_whole {
                Slot::Immediate(Reply::Refused(r))
            } else {
                let outcome = Outcome::Push(PushOutcome::planned(items, runs));
                if effects.is_empty() {
                    empties.push(i);
                    Slot::Empty(outcome)
                } else {
                    log(entry, i, effects, outcome)
                }
            });
            continue;
        }
        slots[i] = match singles.remove(&i) {
            Some(Out::Logged { effects, outcome }) => Some(log(entry, i, effects, outcome)),
            Some(Out::Empty(outcome)) => {
                empties.push(i);
                Some(Slot::Empty(outcome))
            }
            Some(Out::Refused(r)) => Some(Slot::Immediate(Reply::Refused(r))),
            Some(Out::Deferred) => {
                deferred.push(i);
                None
            }
            Some(Out::Piece { .. }) | None => {
                whole.set(false);
                Some(Slot::Immediate(Reply::Refused(Refusal::retry(
                    "internal",
                    "a command its run did not plan",
                ))))
            }
        };
    }
    for (i, effects, outcome) in spread {
        slots[i] = Some(if effects.is_empty() {
            empties.push(i);
            Slot::Empty(outcome)
        } else {
            log(entry, i, effects, outcome)
        });
    }
    if !whole.get() {
        return None;
    }
    if !drew {
        return Some(Numbering::new(0));
    }
    // S1: the run's placeholders, numbered in entry order from where the entry
    // stands, then every place one can sit.
    let mut num = Numbering::new(groups);
    let mut next = entry.effects[..e0].iter().fold(
        (entry.pid_base, entry.kv_version_base),
        |(p, k), e| match e {
            Effect::PartitionCreate { .. } => (p + 1, k),
            Effect::KvPut { .. } => (p, k + 1),
            _ => (p, k),
        },
    );
    num.assign(&entry.effects[e0..], &mut next);
    num.next = Some(next);
    for e in &mut entry.effects[e0..] {
        num.effect(e);
    }
    for c in &mut entry.commands[c0..] {
        num.outcome(&mut c.outcome);
    }
    for i in empties {
        if let Some(Slot::Empty(o)) = &mut slots[i] {
            num.outcome(o);
        }
    }
    num.ok.then_some(num)
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// What the scheduler did since its last log line.
#[derive(Default)]
struct Stats {
    parallel: AtomicU64,
    serial: AtomicU64,
    fallback: AtomicU64,
    route_us: AtomicU64,
    group_us: AtomicU64,
    merge_us: AtomicU64,
    land_us: AtomicU64,
    asm_us: AtomicU64,
    cycle_us: AtomicU64,
    idle_us: AtomicU64,
    tail_us: AtomicU64,
    /// The homes that planned, and the commands planned after them (S6).
    bins: AtomicU64,
    shared: AtomicU64,
    /// The commands planned by shares and kept, and the second rounds taken
    /// back (S8).
    spread: AtomicU64,
    taken_back: AtomicU64,
    last: Mutex<Option<Instant>>,
}

impl Stats {
    fn cycles(&self, mode: Mode) -> &AtomicU64 {
        match mode {
            Mode::Parallel => &self.parallel,
            Mode::Serial => &self.serial,
            Mode::Fallback => &self.fallback,
        }
    }

    /// One line every 10 s while cycles plan.
    fn maybe_log(&self) {
        let Ok(mut last) = self.last.lock() else {
            return;
        };
        let now = Instant::now();
        if last.is_some_and(|t| now.duration_since(t) < Duration::from_secs(10)) {
            return;
        }
        *last = Some(now);
        let parallel = self.parallel.swap(0, Ordering::Relaxed);
        let serial = self.serial.swap(0, Ordering::Relaxed);
        let fallback = self.fallback.swap(0, Ordering::Relaxed);
        let cycles = (parallel + serial + fallback).max(1);
        // Every time is per cycle, so the parts add up to `cycle_us` (and
        // `idle_us` is the planner thread's wait between two cycles).
        let per = |c: &AtomicU64| c.swap(0, Ordering::Relaxed) / cycles;
        let (cycle_us, idle_us, route_us, land_us) = (
            per(&self.cycle_us),
            per(&self.idle_us),
            per(&self.route_us),
            per(&self.land_us),
        );
        let (group_us, asm_us, merge_us, tail_us) = (
            per(&self.group_us),
            per(&self.asm_us),
            per(&self.merge_us),
            per(&self.tail_us),
        );
        let bins = self.bins.swap(0, Ordering::Relaxed);
        let shared = self.shared.swap(0, Ordering::Relaxed);
        let spread = self.spread.swap(0, Ordering::Relaxed);
        let taken_back = self.taken_back.swap(0, Ordering::Relaxed);
        if parallel + fallback > 0 {
            tracing::info!(
                target: "rsm",
                parallel,
                serial,
                fallback,
                cycle_us,
                idle_us,
                route_us,
                land_us,
                group_us,
                asm_us,
                merge_us,
                tail_us,
                bins,
                late = shared,
                spread,
                taken_back,
                "planner: cycles since the last line (times per cycle, µs; in all: homes at work, commands planned late, commands planned by shares, second rounds taken back)",
            );
        }
    }
}

fn stats() -> &'static Stats {
    static S: std::sync::OnceLock<Stats> = std::sync::OnceLock::new();
    S.get_or_init(Stats::default)
}
