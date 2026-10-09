//! The engine's in-memory state: groups, their partitions ("parts") sharded by
//! `pid % SHARDS`, the ready lists, the timers, the per-worker lease index and
//! the dirty set the checkpoints drain.
//!
//! Lock order (never the other way round): the registry, then a group's
//! `st`, then ONE shard at a time, then the leaf locks (`answers`, `tickets`,
//! the serve thread's wake queue). A shard lock is never held while another
//! shard's, a group's `st` or the registry is taken.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use smallvec::SmallVec;
use tokio::sync::oneshot;

use crate::rsm::batcher::{Command, Reply};
use crate::rsm::effect::{CursorRow, Effect, GroupMeta, Pid, QueueConfig, SubscriptionMode};
use crate::rsm::planner::PopCommand;

/// How many shards the partitions are spread over.
pub(crate) const SHARDS: usize = 16;

/// A group's id inside one engine (interned).
pub(crate) type Gid = u32;

pub(crate) fn shard_of(pid: Pid) -> usize {
    (pid % SHARDS as u64) as usize
}

/// Lock a mutex, surviving a poisoned one (a panicking holder must not take
/// the whole engine with it: its state is plain data).
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

pub(crate) fn read<T>(m: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    m.read().unwrap_or_else(|p| p.into_inner())
}

pub(crate) fn write<T>(m: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    m.write().unwrap_or_else(|p| p.into_inner())
}

/// The lease a part holds (the cursor row's lease fields, plus what the ack
/// needs from the claim: the delivered set and the hashes of the leased run).
#[derive(Clone, Debug)]
pub(crate) struct Lease {
    pub worker: Arc<str>,
    /// Inclusive end of the leased batch.
    pub batch_end: u64,
    pub expires_us: i64,
    pub acquired_us: Option<i64>,
    pub conflated: bool,
    /// O16: the distinct transaction hashes delivered, first-seen order.
    pub delivered: Vec<[u8; 16]>,
    /// The hash of every offset `frames_lo ..= batch_end` (a plain claim), for
    /// resolving acks without a store scan. Empty for a conflating lease.
    pub frames_lo: u64,
    pub frames: Vec<[u8; 16]>,
    /// Granted before this engine's term (loaded from a checkpoint): timed by
    /// another node's clock, so held for the skew grace past its expiry.
    pub foreign: bool,
}

impl Lease {
    /// The lowest offset in `[lo, hi]` whose frame carries `hash`, from the
    /// leased run (`None` when the run does not cover the range or no frame
    /// matches in it).
    pub fn find(&self, hash: &[u8; 16], lo: u64, hi: u64) -> Option<u64> {
        if self.frames.is_empty() {
            return None;
        }
        let from = lo.max(self.frames_lo);
        let to = hi.min(self.frames_lo + self.frames.len() as u64 - 1);
        (from..=to).find(|off| &self.frames[(off - self.frames_lo) as usize] == hash)
    }

    /// Whether the leased run covers `[lo, hi]` wholly (so a miss there is a
    /// real miss, not an offset the run does not hold).
    pub fn covers(&self, lo: u64, hi: u64) -> bool {
        !self.frames.is_empty()
            && lo >= self.frames_lo
            && hi < self.frames_lo + self.frames.len() as u64
    }
}

/// The cursor row of a (partition, group), as the engine owns it in memory.
/// Cloneable: a transaction computes on a shadow copy until it commits.
#[derive(Clone, Debug)]
pub(crate) struct Cur {
    /// Last acked offset (`-1` = none): the next wanted is `committed + 1`.
    pub committed: i64,
    pub lease: Option<Lease>,
    pub batch_retry_count: u32,
    pub attempt_offset: Option<u64>,
    pub attempt_count: u32,
    pub total_consumed: u64,
    pub created_at_us: i64,
    pub metadata: String,
    /// The lease the last ack released and its batch (`worker`, `lo..=hi`):
    /// a repeat of that ack is answered as the first was
    /// ([`crate::rsm::effect::CursorRow::released`]).
    pub released: Option<(Arc<str>, u64, u64)>,
}

impl Cur {
    pub fn fresh(committed: i64, now_us: i64) -> Cur {
        Cur {
            committed,
            lease: None,
            batch_retry_count: 0,
            attempt_offset: None,
            attempt_count: 0,
            total_consumed: 0,
            created_at_us: now_us,
            metadata: String::new(),
            released: None,
        }
    }

    /// The cursor as a stored row describes it. `foreign_before`: a lease
    /// acquired before this instant (this engine's term start) was timed by
    /// another node's clock.
    pub fn from_row(row: &CursorRow, foreign_before: i64) -> Cur {
        let lease = row.batch_end.map(|be| Lease {
            worker: Arc::from(row.worker.as_deref().unwrap_or("")),
            batch_end: be,
            expires_us: row.lease_expires_at_us.unwrap_or(0),
            acquired_us: row.lease_acquired_at_us,
            conflated: row.lease_conflated,
            delivered: row.delivered.clone(),
            frames_lo: 0,
            frames: Vec::new(),
            foreign: row.lease_acquired_at_us.is_none_or(|a| a < foreign_before),
        });
        Cur {
            committed: row.committed,
            lease,
            batch_retry_count: row.batch_retry_count,
            attempt_offset: row.attempt_offset,
            attempt_count: row.attempt_count,
            total_consumed: row.total_consumed,
            created_at_us: row.created_at_us,
            metadata: row.metadata.clone(),
            released: row
                .released
                .as_ref()
                .map(|r| (Arc::from(r.worker.as_str()), r.lo, r.hi)),
        }
    }

    /// The whole cursor row (a checkpoint's `CursorSet`). The delivered set is
    /// not carried: a new leader rebuilds it from the stored hashes.
    pub fn row(&self) -> CursorRow {
        let l = self.lease.as_ref();
        CursorRow {
            committed: self.committed,
            batch_end: l.map(|l| l.batch_end),
            worker: l.and_then(|l| (!l.worker.is_empty()).then(|| l.worker.to_string())),
            lease_expires_at_us: l.and_then(|l| (!l.worker.is_empty()).then_some(l.expires_us)),
            lease_acquired_at_us: l.and_then(|l| l.acquired_us),
            batch_retry_count: self.batch_retry_count,
            attempt_offset: self.attempt_offset,
            attempt_count: self.attempt_count,
            total_consumed: self.total_consumed,
            lease_conflated: l.is_some_and(|l| l.conflated),
            delivered: Vec::new(),
            created_at_us: self.created_at_us,
            metadata: self.metadata.clone(),
            released: self
                .released
                .as_ref()
                .map(|(w, lo, hi)| crate::rsm::effect::ReleasedLease {
                    worker: w.to_string(),
                    lo: *lo,
                    hi: *hi,
                }),
        }
    }

    /// The worker holding the lease (live or not), `None` without one.
    pub fn worker(&self) -> Option<&str> {
        self.lease
            .as_ref()
            .and_then(|l| (!l.worker.is_empty()).then_some(&*l.worker))
    }

    /// The lease's expiry (`None` without a lease).
    pub fn expires(&self) -> Option<i64> {
        self.lease
            .as_ref()
            .and_then(|l| (!l.worker.is_empty()).then_some(l.expires_us))
    }

    /// Whether a lease holds the part now (live, or inside the skew grace of a
    /// lease another node's clock timed).
    pub fn leased(&self, now_us: i64, grace_us: i64) -> bool {
        self.lease.as_ref().is_some_and(|l| {
            !l.worker.is_empty()
                && (l.expires_us > now_us
                    || (l.foreign
                        && grace_us > 0
                        && l.expires_us.saturating_add(grace_us) > now_us))
        })
    }

    /// When the lease stops holding the part (the expiry timer).
    pub fn lease_until(&self, grace_us: i64) -> Option<i64> {
        self.lease.as_ref().and_then(|l| {
            (!l.worker.is_empty()).then(|| {
                if l.foreign && grace_us > 0 {
                    l.expires_us.saturating_add(grace_us)
                } else {
                    l.expires_us
                }
            })
        })
    }

    /// Clear every lease field (the ack registry's delivered set goes too).
    pub fn release(&mut self) {
        self.lease = None;
    }

    /// Hand back a claim that no worker received: drop its lease and take back
    /// the delivery attempt it counted. The claim left the attempt marker on
    /// its first offset, so the next pop there would read as a redelivery; a
    /// Laravel job with tries = 1 then failed without ever running. The
    /// attempts before it (a lease that really expired) still count.
    pub fn release_undelivered(&mut self) {
        self.release();
        self.attempt_count = self.attempt_count.saturating_sub(1);
        if self.attempt_count == 0 {
            self.attempt_offset = None;
        }
    }
}

/// One (partition, group) the engine holds.
#[derive(Debug)]
pub(crate) struct Part {
    pub cur: Cur,
    /// A cursor row exists in the store (or the next checkpoint writes one).
    /// A first contact that delivered nothing writes none: its seed is stable,
    /// so the next leader recomputes the same one.
    pub has_row: bool,
    /// `Some(ts)`: a first contact whose seed is not stable yet (a
    /// subscription instant in the future): recomputed at every claim.
    pub seed_ts: Option<i64>,
    /// Not claimable before this instant (delayed processing, window buffer);
    /// `0` = no hold.
    pub ready_at: i64,
    /// In the group shard's ready list.
    pub queued: bool,
    /// Changed since the last checkpoint took it.
    pub dirty: bool,
    /// A `CursorDelete` is owed (a forgotten position): the row goes, the part
    /// stays as a first contact.
    pub delete_row: bool,
    /// Version of the state (bumped by every change a checkpoint must carry),
    /// the version a ticket in flight carries, the version known committed.
    pub ver: u64,
    pub sent_ver: u64,
    pub durable_ver: u64,
    /// The highest version a checkpoint carried that resolved without a
    /// commit this engine saw: the entry may still commit under the next
    /// leader, so an answer at or below it is in doubt ([`PendingAnswer::sent`]).
    pub doubt_ver: u64,
    /// Answers waiting for this part to be durable at a version.
    pub waiters: SmallVec<[(u64, u64); 1]>,
    /// Dead letters decided on this part, not yet in a checkpoint / in flight.
    pub dlq: Vec<Effect>,
    pub dlq_sent: Vec<Effect>,
    /// A transaction holds this part until it resolves.
    pub reserved: Option<[u8; 16]>,
    /// The due time of the timer pending for this part (`0`: none).
    pub timer_at: i64,
    /// The idle-part unloader's view ([`super::unload`]): the version it last
    /// saw, and since when it has seen no other.
    pub seen_ver: u64,
    pub idle_since_us: i64,
}

impl Part {
    pub fn new(cur: Cur, has_row: bool) -> Part {
        Part {
            cur,
            has_row,
            seed_ts: None,
            ready_at: 0,
            queued: false,
            dirty: false,
            delete_row: false,
            ver: 0,
            sent_ver: 0,
            durable_ver: 0,
            doubt_ver: 0,
            waiters: SmallVec::new(),
            dlq: Vec::new(),
            dlq_sent: Vec::new(),
            reserved: None,
            timer_at: 0,
            // Never a version: the unloader's first look starts the clock.
            seen_ver: u64::MAX,
            idle_since_us: 0,
        }
    }

    /// A row is in a checkpoint that has not resolved.
    pub fn in_flight(&self) -> bool {
        self.sent_ver > self.durable_ver
    }
}

/// What the engine knows of a partition (shared by the groups holding it).
#[derive(Debug)]
pub(crate) struct PidInfo {
    /// The visible tail (`-1` = empty).
    pub tail: i64,
    pub log_start: u64,
    pub txns_start: u64,
    /// Where the partition's `txns` rows begin (catalogue version 6): the
    /// retained messages below it are read from the queue log.
    pub rows_start: u64,
    /// When the newest append was applied here (the window buffer), µs.
    pub last_append_us: i64,
    /// The groups holding this partition. A watcher's part may be missing: a
    /// whole-queue group's part the unloader dropped ([`super::unload`]); the
    /// next append here loads it back.
    pub watchers: SmallVec<[Gid; 2]>,
}

impl PidInfo {
    pub fn floor(&self) -> i64 {
        self.log_start as i64 - 1
    }
}

/// One group's parts in one shard.
pub(crate) struct GroupShard {
    pub g: Arc<Group>,
    pub parts: HashMap<Pid, Part>,
    /// Claimable parts, oldest ready first.
    pub ready: VecDeque<Pid>,
}

impl GroupShard {
    pub fn new(g: Arc<Group>) -> GroupShard {
        GroupShard {
            g,
            parts: HashMap::new(),
            ready: VecDeque::new(),
        }
    }
}

/// A timer: at `due`, look at `(gid, pid)` again (a lease expiry, a hold).
pub(crate) type Timer = Reverse<(i64, Gid, Pid)>;

/// Schedule a look at `(gid, pid)` at `t`, unless one no later is pending
/// (it re-arms, which schedules again what is still ahead).
pub(crate) fn schedule(
    timers: &mut BinaryHeap<Timer>,
    part: &mut Part,
    t: i64,
    gid: Gid,
    pid: Pid,
) {
    if part.timer_at != 0 && part.timer_at <= t {
        return;
    }
    part.timer_at = t;
    timers.push(Reverse((t, gid, pid)));
}

/// One shard of the engine.
#[derive(Default)]
pub(crate) struct Shard {
    pub pids: HashMap<Pid, PidInfo>,
    pub groups: HashMap<Gid, GroupShard>,
    pub timers: BinaryHeap<Timer>,
    /// Every live lease of a worker in this shard.
    pub workers: HashMap<Arc<str>, HashSet<(Pid, Gid)>>,
    /// Parts to checkpoint (`Part::dirty` dedups).
    pub dirty: Vec<(Pid, Gid)>,
}

impl Shard {
    pub fn index_lease(&mut self, worker: &Arc<str>, pid: Pid, gid: Gid) {
        if worker.is_empty() {
            return;
        }
        self.workers
            .entry(worker.clone())
            .or_default()
            .insert((pid, gid));
    }

    pub fn unindex_lease(&mut self, worker: &str, pid: Pid, gid: Gid) {
        if let Some(set) = self.workers.get_mut(worker) {
            set.remove(&(pid, gid));
            if set.is_empty() {
                self.workers.remove(worker);
            }
        }
    }
}

/// The configuration a claim reads: the queue's, and the group's stored
/// policy (`None` = not registered: a plain pinned pop's group).
#[derive(Clone, Debug)]
pub(crate) struct GroupCfg {
    pub queue: QueueConfig,
    pub meta: Option<GroupMeta>,
}

impl GroupCfg {
    /// The subscription instant a first contact seeds from, when the group is
    /// registered (`None`: `all`, the floor).
    pub fn seed_instant(&self) -> Option<i64> {
        match &self.meta {
            Some(m) => match m.mode {
                SubscriptionMode::All => None,
                SubscriptionMode::New => Some(m.registered_at_us),
                SubscriptionMode::Timestamp => Some(m.subscription_timestamp_us),
            },
            None => None,
        }
    }
}

/// A parked long-poll pop.
pub(crate) struct Waiter {
    pub cmd: PopCommand,
    /// `Some(pid)` for a pinned pop.
    pub pinned: Option<Pid>,
    pub sink: oneshot::Sender<Reply>,
}

/// A command held until the group finished loading (or the leader's pause).
pub(crate) struct Held {
    pub cmd: Command,
    pub sink: oneshot::Sender<Reply>,
    /// The clock it arrived at (a re-run never goes back in time).
    pub at_us: i64,
}

/// How much of a group's queue the engine holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Load {
    /// Only the parts single-partition commands touched.
    Partial,
    /// A first-contact enumeration of the queue is running.
    Loading,
    /// Every partition of the queue (new ones attach as they are created).
    Full,
}

/// A group's mutable bookkeeping (behind [`Group::st`]).
pub(crate) struct GroupSt {
    pub load: Load,
    /// Commands waiting for the load.
    pub held: Vec<Held>,
    /// Partitions created while the load ran (attached when it ends).
    pub late: Vec<Pid>,
    pub waiters: VecDeque<Waiter>,
    /// Catalog effects owed to the log (the registration `GroupUpsert`, a pop's
    /// implicit `QueueUpsert`), their versions and answers waiting on them.
    pub cat: Vec<Effect>,
    pub cat_sent: Vec<Effect>,
    pub cat_ver: u64,
    pub cat_sent_ver: u64,
    pub cat_durable: u64,
    /// As [`Part::doubt_ver`], for the catalog effects.
    pub cat_doubt: u64,
    pub cat_waiters: Vec<(u64, u64)>,
    pub cat_dirty: bool,
    /// The group was deleted (its state dropped); a later command makes a new one.
    pub dropped: bool,
    /// A conflating pinned registrar owes the queue-wide bulk seed once the
    /// group holds its whole queue.
    pub bulk_pending: bool,
}

/// One consumer group (tenant, queue, group) the engine holds.
pub(crate) struct Group {
    pub id: Gid,
    pub tenant: String,
    pub queue: String,
    pub name: String,
    pub cfg: RwLock<Arc<GroupCfg>>,
    pub st: Mutex<GroupSt>,
    /// Parts in the ready lists (the autopilot's width input).
    pub ready_n: AtomicUsize,
    /// The shard a wildcard walk starts at (rotates).
    pub rr: AtomicUsize,
    /// Parked waiters (read without the `st` lock by the wake path).
    pub waiting: AtomicUsize,
    /// Bumped whenever a part becomes claimable (a waiter re-checks it).
    pub wake_seq: AtomicU64,
    /// Dropped (deleted, or a leadership change reset the engine): nothing
    /// may be added under it any more.
    pub dead: std::sync::atomic::AtomicBool,
}

impl Group {
    pub fn new(id: Gid, tenant: &str, queue: &str, name: &str, cfg: GroupCfg, load: Load) -> Group {
        Group {
            id,
            tenant: tenant.to_string(),
            queue: queue.to_string(),
            name: name.to_string(),
            cfg: RwLock::new(Arc::new(cfg)),
            st: Mutex::new(GroupSt {
                load,
                held: Vec::new(),
                late: Vec::new(),
                waiters: VecDeque::new(),
                cat: Vec::new(),
                cat_sent: Vec::new(),
                cat_ver: 0,
                cat_sent_ver: 0,
                cat_durable: 0,
                cat_doubt: 0,
                cat_waiters: Vec::new(),
                cat_dirty: false,
                dropped: false,
                bulk_pending: false,
            }),
            ready_n: AtomicUsize::new(0),
            rr: AtomicUsize::new(0),
            waiting: AtomicUsize::new(0),
            wake_seq: AtomicU64::new(0),
            dead: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn cfg(&self) -> Arc<GroupCfg> {
        read(&self.cfg).clone()
    }

    pub fn set_cfg(&self, cfg: GroupCfg) {
        *write(&self.cfg) = Arc::new(cfg);
    }

    pub fn is(&self, tenant: &str, queue: &str, name: &str) -> bool {
        self.tenant == tenant && self.queue == queue && self.name == name
    }

    pub fn bump(&self) {
        self.wake_seq.fetch_add(1, Ordering::AcqRel);
    }

    /// One part fewer in the ready lists.
    pub fn unready(&self) {
        #[allow(deprecated)] // fetch_update: deprecated in 1.99; try_update is 1.95+, MSRV 1.88
        let _ = self
            .ready_n
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                Some(v.saturating_sub(1))
            });
    }
}

/// The key a group is found by (no allocation per lookup).
pub(crate) fn group_hash(tenant: &str, queue: &str, name: &str) -> u64 {
    let mut h = xxhash_rust::xxh3::Xxh3::new();
    h.update(tenant.as_bytes());
    h.update(&[0x1F]);
    h.update(queue.as_bytes());
    h.update(&[0x1F]);
    h.update(name.as_bytes());
    h.digest()
}

pub(crate) fn queue_hash(tenant: &str, queue: &str) -> u64 {
    let mut h = xxhash_rust::xxh3::Xxh3::new();
    h.update(tenant.as_bytes());
    h.update(&[0x1F]);
    h.update(queue.as_bytes());
    h.digest()
}

/// Every group the engine holds.
#[derive(Default)]
pub(crate) struct Registry {
    pub by_key: HashMap<u64, SmallVec<[Arc<Group>; 1]>>,
    pub by_id: HashMap<Gid, Arc<Group>>,
    /// The groups of a (tenant, queue).
    pub by_queue: HashMap<u64, SmallVec<[Gid; 2]>>,
    pub next: Gid,
}

impl Registry {
    pub fn get(&self, tenant: &str, queue: &str, name: &str) -> Option<Arc<Group>> {
        self.by_key
            .get(&group_hash(tenant, queue, name))?
            .iter()
            .find(|g| g.is(tenant, queue, name))
            .cloned()
    }

    pub fn insert(&mut self, g: Arc<Group>) {
        self.by_key
            .entry(group_hash(&g.tenant, &g.queue, &g.name))
            .or_default()
            .push(g.clone());
        self.by_queue
            .entry(queue_hash(&g.tenant, &g.queue))
            .or_default()
            .push(g.id);
        self.by_id.insert(g.id, g);
    }

    pub fn remove(&mut self, gid: Gid) -> Option<Arc<Group>> {
        let g = self.by_id.remove(&gid)?;
        let k = group_hash(&g.tenant, &g.queue, &g.name);
        if let Some(v) = self.by_key.get_mut(&k) {
            v.retain(|x| x.id != gid);
            if v.is_empty() {
                self.by_key.remove(&k);
            }
        }
        let q = queue_hash(&g.tenant, &g.queue);
        if let Some(v) = self.by_queue.get_mut(&q) {
            v.retain(|x| *x != gid);
            if v.is_empty() {
                self.by_queue.remove(&q);
            }
        }
        Some(g)
    }

    /// The groups of one queue.
    pub fn of_queue(&self, tenant: &str, queue: &str) -> Vec<Arc<Group>> {
        self.by_queue
            .get(&queue_hash(tenant, queue))
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.by_id.get(id))
                    .filter(|g| g.tenant == tenant && g.queue == queue)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// An answer waiting for the checkpoints that make it true.
pub(crate) struct PendingAnswer {
    /// Dependencies not yet durable (+1 while the registration runs).
    pub remaining: usize,
    pub reply: Reply,
    pub sink: oneshot::Sender<Reply>,
    /// The leases a claim answer granted: released when nobody receives it.
    pub claims: Vec<(Pid, Gid, Arc<str>, u64)>,
    /// A command whose second run could answer differently from its first
    /// (an ack, a nack, a positional ack, a DLQ head, a seek): once its
    /// change may be in the log it is never run again ([`Deps::exact`]).
    ///
    /// [`Deps::exact`]: super::checkpoint::Deps::exact
    pub exact: bool,
    /// A checkpoint carrying the change this answer waits on was sent: the
    /// change may commit even if this node stops leading before it learns so.
    pub sent: bool,
}

/// The answers waiting on checkpoints.
#[derive(Default)]
pub(crate) struct Answers {
    pub map: HashMap<u64, PendingAnswer>,
    pub next: u64,
}

/// One checkpoint in flight.
#[derive(Default)]
pub(crate) struct Ticket {
    /// (pid, gid, version) of every cursor row it carries.
    pub rows: Vec<(Pid, Gid, u64)>,
    /// (gid, catalog version) of every group whose catalog effects it carries.
    pub cats: Vec<(Gid, u64)>,
    /// The request ids of its commands (answered at commit: [`Tickets::ids`]).
    pub ids: Vec<crate::rsm::entry::RequestId>,
}

#[derive(Default)]
pub(crate) struct Tickets {
    pub next: u64,
    pub inflight: HashMap<u64, Ticket>,
    /// The request ids of every checkpoint command in flight: the batcher
    /// answers these when their entry COMMITS, not when it applies here — a
    /// checkpoint is durable once committed, and every claim and ack it holds
    /// waits on it (the leader's apply ran 50-400 ms behind its commits at
    /// 1M msg/s).
    pub ids: std::collections::HashSet<crate::rsm::entry::RequestId>,
}
