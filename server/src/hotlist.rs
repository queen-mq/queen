//! Wildcard candidate hot-list (19-wildcard-hotlist.md).
//!
//! Broker-side, in-memory replacement for the per-pop wildcard candidate SCAN
//! (`log_partitions ⋈ log_consumers`, `ORDER BY random() LIMIT cap`). For each
//! (queue, group) the broker keeps the set of "probably pending" partitions and
//! serves wildcard pops from it, calling `queen.log_pop_list_v1(names[])` on a
//! handful of ring candidates instead of scanning. PG stays the sole authority
//! on claim / lease / cursor / visibility: the hot-list can only be *stale in
//! excess* (a false positive = one empty SKIP LOCKED claim ~0.2ms), never hide
//! work below the reseed floor (§1).
//!
//! This module is deliberately DB-free and transport-free (so it is fully
//! unit-testable): the handler layer drives it (take candidates → call SQL →
//! check the tri-state verdicts back in), main.rs runs its background wheel
//! tick + mesh dirty flusher, and it wakes parked pops through an attached
//! `Notifier`. Everything is a no-op when `enabled == false` (flag QUEEN_HOTLIST
//! default off ⇒ the current path is byte-identical; the handler hooks branch
//! on `enabled()` and never touch this).
//!
//! ## Structures (§3)
//! * **Interning** — per QUEUE, partition name → dense `u32` index, append-only.
//!   The strings live only here; the id↔name memo (learned from pop responses /
//!   reseed) bridges the partition-id-keyed ack hook back to a ring entry.
//! * **Ring** — per (queue, group), an intrusive doubly-linked "ready" list over
//!   flat arrays indexed by the partition index. Insert is O(1) only on the
//!   empty→pending transition; a push to an already-pending partition is an
//!   atomic epoch bump with zero ring op. Sub-sharded by `index % S` so a hot
//!   mega-queue does not convoy on one mutex; each sub-ring owns COMPACT arrays
//!   indexed by the shard-local position `index / S` (no per-shard waste).
//! * **Wheel** — deferred visibility (§6, delayed + windowBuffer) and lease
//!   revisit (§7). Modelled as a per-sub-ring min-heap keyed by `revisit_at`
//!   (a functionally-equivalent, correctness-first stand-in for the spec's
//!   hierarchical timing wheel; the hierarchy is a 500k-scale perf detail).
//!
//! ## Epoch-CAS (§4)
//! A pop snapshots `epoch` per candidate before issuing the SQL; on an `empty`
//! verdict it clears the entry ONLY if `epoch` is unchanged. A mark that raced
//! (a push committed after the snapshot) bumped the epoch ⇒ CAS fails ⇒ the
//! entry stays. Every race degrades to a false positive, never a false negative
//! beyond the reseed floor.

use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::sync::Arc;

use crate::notify::Notifier;

// Entry state (one byte in the flat `state` array).
const IDLE: u8 = 0; // not tracked (not in ready ring / wheel / in-flight)
const READY: u8 = 1; // in the ready ring, claimable now
const WHEEL: u8 = 2; // deferred / leased — promoted to READY at revisit_at
const INFLIGHT: u8 = 3; // checked out by a pop, dispatch in flight

// Sentinel for "no link" in the intrusive ring (u32::MAX).
const NIL: u32 = u32::MAX;

// §6: scheduling padding on every PG-derived deadline (the broker clock only
// schedules the retry; PG remains the visibility authority).
const PAD_MS: i64 = 300;
// §6: bounded backoff for a revisit on a deferral queue that popped empty.
const REVISIT_MIN_MS: i64 = 50;
const REVISIT_MAX_MS: i64 = 1000;

/// A candidate handed to the handler for one SQL `log_pop_list_v1` call.
#[derive(Clone, Debug)]
pub struct Candidate {
    pub name: String,
    /// epoch snapshot for the empty-path CAS (§4).
    pub epoch: u32,
}

/// The tri-state verdict (§7) the handler feeds back per candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Frames delivered — keep as a candidate (may hold more).
    Took,
    /// 0 frames, no live foreign lease — CAS-clear (or revisit on a deferral queue).
    Empty,
    /// A foreign worker holds a live lease until this epoch-ms — wheel at T+pad.
    Leased(i64),
    /// The SQL did not evaluate this candidate (budget spent) — re-append.
    Requeue,
}

/// One verdict the handler checks back in.
pub struct CheckinResult {
    pub name: String,
    pub epoch: u32,
    pub verdict: Verdict,
}

/// A queue's deferral config, cached broker-side with a TTL (§6). `delayed` and
/// `window` are seconds; both 0 = no deferral (marks go straight to the ready
/// ring, empties hard-clear).
#[derive(Clone, Copy, Default)]
struct DeferCfg {
    delayed: i32,
    window: i32,
    fetched_ms: i64,
}

impl DeferCfg {
    #[inline]
    fn is_deferral(&self) -> bool {
        self.delayed > 0 || self.window > 0
    }
}

// ---------------------------------------------------------------- interning

struct QueueIntern {
    name_to_idx: HashMap<Box<str>, u32>,
    idx_to_name: Vec<Box<str>>,
    // partition-id (uuid text) ↔ index, learned lazily for the ack bridge.
    id_to_idx: HashMap<Box<str>, u32>,
    idx_to_id: Vec<Option<Box<str>>>,
}

impl QueueIntern {
    fn new() -> Self {
        QueueIntern {
            name_to_idx: HashMap::new(),
            idx_to_name: Vec::new(),
            id_to_idx: HashMap::new(),
            idx_to_id: Vec::new(),
        }
    }
    /// Intern a name → dense index (append-only).
    fn intern(&mut self, name: &str) -> u32 {
        if let Some(&i) = self.name_to_idx.get(name) {
            return i;
        }
        let i = self.idx_to_name.len() as u32;
        let b: Box<str> = name.into();
        self.name_to_idx.insert(b.clone(), i);
        self.idx_to_name.push(b);
        self.idx_to_id.push(None);
        i
    }
    fn idx_of(&self, name: &str) -> Option<u32> {
        self.name_to_idx.get(name).copied()
    }
    fn name_of(&self, idx: u32) -> Option<&str> {
        self.idx_to_name.get(idx as usize).map(|b| &**b)
    }
    fn note_id(&mut self, name: &str, id: &str) {
        let i = self.intern(name);
        if self.idx_to_id[i as usize].as_deref() != Some(id) {
            let b: Box<str> = id.into();
            self.id_to_idx.insert(b.clone(), i);
            self.idx_to_id[i as usize] = Some(b);
        }
    }
    fn idx_of_id(&self, id: &str) -> Option<u32> {
        self.id_to_idx.get(id).copied()
    }
}

// ------------------------------------------------------------------- ring

/// One sub-ring: the flat intrusive arrays + ready list + wheel for the subset
/// of partition indices with `index % S == shard`, stored at the shard-local
/// position `index / S`.
struct SubRing {
    next: Vec<u32>,
    prev: Vec<u32>,
    epoch: Vec<u32>,
    state: Vec<u8>,
    revisit_at: Vec<i64>,
    batch_count: Vec<u32>,
    ready_head: u32, // shard-local pos, NIL if empty
    ready_tail: u32,
    // (revisit_at snapshot, local pos). Stale entries (state changed or
    // rescheduled) are skipped lazily on drain.
    wheel: BinaryHeap<Reverse<(i64, u32)>>,
    len_ready: usize,
}

impl SubRing {
    fn new() -> Self {
        SubRing {
            next: Vec::new(),
            prev: Vec::new(),
            epoch: Vec::new(),
            state: Vec::new(),
            revisit_at: Vec::new(),
            batch_count: Vec::new(),
            ready_head: NIL,
            ready_tail: NIL,
            wheel: BinaryHeap::new(),
            len_ready: 0,
        }
    }

    #[inline]
    fn ensure(&mut self, local: u32) {
        let need = local as usize + 1;
        if self.state.len() < need {
            self.next.resize(need, NIL);
            self.prev.resize(need, NIL);
            self.epoch.resize(need, 0);
            self.state.resize(need, IDLE);
            self.revisit_at.resize(need, 0);
            self.batch_count.resize(need, 0);
        }
    }

    fn ready_push_tail(&mut self, l: u32) {
        self.next[l as usize] = NIL;
        self.prev[l as usize] = self.ready_tail;
        if self.ready_tail == NIL {
            self.ready_head = l;
        } else {
            self.next[self.ready_tail as usize] = l;
        }
        self.ready_tail = l;
        self.state[l as usize] = READY;
        self.len_ready += 1;
    }

    fn ready_unlink(&mut self, l: u32) {
        let p = self.prev[l as usize];
        let n = self.next[l as usize];
        if p == NIL {
            self.ready_head = n;
        } else {
            self.next[p as usize] = n;
        }
        if n == NIL {
            self.ready_tail = p;
        } else {
            self.prev[n as usize] = p;
        }
        self.next[l as usize] = NIL;
        self.prev[l as usize] = NIL;
        self.len_ready -= 1;
    }

    fn ready_pop_head(&mut self) -> Option<u32> {
        let h = self.ready_head;
        if h == NIL {
            return None;
        }
        self.ready_unlink(h);
        Some(h)
    }

    fn wheel_schedule(&mut self, l: u32, at: i64) {
        self.revisit_at[l as usize] = at;
        self.state[l as usize] = WHEEL;
        self.wheel.push(Reverse((at, l)));
    }

    /// Move every due wheel entry (revisit_at <= now) to the ready tail.
    /// Returns how many promoted. Stale heap entries are discarded.
    fn wheel_drain_due(&mut self, now: i64) -> usize {
        let mut promoted = 0;
        while let Some(&Reverse((at, l))) = self.wheel.peek() {
            if at > now {
                break;
            }
            self.wheel.pop();
            // Skip stale: rescheduled (revisit_at moved) or no longer WHEEL.
            if self.state[l as usize] != WHEEL || self.revisit_at[l as usize] != at {
                continue;
            }
            self.revisit_at[l as usize] = 0;
            self.ready_push_tail(l);
            promoted += 1;
        }
        promoted
    }

    /// The earliest pending wheel deadline (for a caller that wants to schedule
    /// its own wake). None when the wheel is empty of live entries.
    fn wheel_peek_due(&self, now: i64) -> bool {
        if let Some(&Reverse((at, _))) = self.wheel.peek() {
            at <= now
        } else {
            false
        }
    }
}

// ------------------------------------------------------------------ group

struct GroupRing {
    subs: Vec<Mutex<SubRing>>,
    // reseed bookkeeping (§8): last full reseed, and a coarse round-robin cursor
    // for take() fairness across sub-rings.
    reseed_ms: Mutex<i64>,
    take_cursor: Mutex<usize>,
}

impl GroupRing {
    fn new(shards: usize) -> Self {
        let mut subs = Vec::with_capacity(shards);
        for _ in 0..shards {
            subs.push(Mutex::new(SubRing::new()));
        }
        GroupRing {
            subs,
            reseed_ms: Mutex::new(0),
            take_cursor: Mutex::new(0),
        }
    }
}

struct QueueState {
    intern: Mutex<QueueIntern>,
    cfg: Mutex<DeferCfg>,
    groups: Mutex<HashMap<String, Arc<GroupRing>>>,
}

impl QueueState {
    fn new() -> Self {
        QueueState {
            intern: Mutex::new(QueueIntern::new()),
            cfg: Mutex::new(DeferCfg::default()),
            groups: Mutex::new(HashMap::new()),
        }
    }
    fn group(&self, group: &str, shards: usize) -> Arc<GroupRing> {
        let mut g = self.groups.lock().unwrap();
        if let Some(r) = g.get(group) {
            return r.clone();
        }
        let r = Arc::new(GroupRing::new(shards));
        g.insert(group.to_string(), r.clone());
        r
    }
}

// ------------------------------------------------------------------ HotList

pub struct HotList {
    enabled: bool,
    shards: usize,
    // windowBuffer early-promotion threshold (§6): batch_count >= this ⇒ promote
    // the entry even before the window expires.
    window_batch: u32,
    queues: Mutex<HashMap<String, Arc<QueueState>>>,
    notifier: OnceLock<Arc<Notifier>>,
    // Coalesced local vuoto→pending transitions, drained by main's mesh flusher
    // (§5). Received (remote) marks never enqueue here (no re-broadcast).
    dirty: Mutex<HashSet<(Box<str>, Box<str>)>>,
    // Observability.
    pub reseeds: std::sync::atomic::AtomicU64,
    pub marks_local: std::sync::atomic::AtomicU64,
    pub marks_remote: std::sync::atomic::AtomicU64,
}

impl HotList {
    pub fn new(enabled: bool, shards: usize, window_batch: u32) -> Arc<HotList> {
        Arc::new(HotList {
            enabled,
            shards: shards.max(1),
            window_batch: window_batch.max(1),
            queues: Mutex::new(HashMap::new()),
            notifier: OnceLock::new(),
            dirty: Mutex::new(HashSet::new()),
            reseeds: std::sync::atomic::AtomicU64::new(0),
            marks_local: std::sync::atomic::AtomicU64::new(0),
            marks_remote: std::sync::atomic::AtomicU64::new(0),
        })
    }

    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn attach_notifier(&self, n: Arc<Notifier>) {
        let _ = self.notifier.set(n);
    }

    #[inline]
    fn wake(&self, queue: &str) {
        if let Some(n) = self.notifier.get() {
            // Empty partition hint = wake the queue gate only (no targeted hint).
            n.wake_local_hint(queue, "");
        }
    }

    fn qstate(&self, queue: &str) -> Arc<QueueState> {
        let mut q = self.queues.lock().unwrap();
        if let Some(s) = q.get(queue) {
            return s.clone();
        }
        let s = Arc::new(QueueState::new());
        q.insert(queue.to_string(), s.clone());
        s
    }

    fn qstate_existing(&self, queue: &str) -> Option<Arc<QueueState>> {
        self.queues.lock().unwrap().get(queue).cloned()
    }

    // -------------------------------------------------------- queue config

    /// Cache a queue's deferral config (from queen.queues; the handler fetches
    /// it, TTL-throttled). `now_ms` stamps the fetch for the TTL check.
    pub fn set_queue_cfg(&self, queue: &str, delayed: i32, window: i32, now_ms: i64) {
        let s = self.qstate(queue);
        let mut c = s.cfg.lock().unwrap();
        c.delayed = delayed;
        c.window = window;
        c.fetched_ms = now_ms;
    }

    /// Is the cached config fresh enough (within `ttl_ms`)? Absent ⇒ false.
    pub fn cfg_fresh(&self, queue: &str, now_ms: i64, ttl_ms: i64) -> bool {
        match self.qstate_existing(queue) {
            Some(s) => {
                let c = s.cfg.lock().unwrap();
                c.fetched_ms != 0 && now_ms - c.fetched_ms < ttl_ms
            }
            None => false,
        }
    }

    fn cfg_of(&self, queue: &str) -> DeferCfg {
        match self.qstate_existing(queue) {
            Some(s) => *s.cfg.lock().unwrap(),
            None => DeferCfg::default(),
        }
    }

    /// Should the list pop skip the SQL windowBuffer debounce for this queue?
    /// Yes only when the broker KNOWS windowBuffer>0 (cache hit) and is holding
    /// it in the wheel — otherwise the SQL debounce stays the (safe) floor.
    pub fn skip_window(&self, queue: &str) -> bool {
        self.cfg_of(queue).window > 0
    }

    // -------------------------------------------------------------- marks

    fn mark_inner(
        &self,
        queue: &str,
        partition: &str,
        count: u32,
        now_ms: i64,
        broadcast: bool,
    ) {
        if !self.enabled || partition.is_empty() {
            return;
        }
        let cfg = self.cfg_of(queue);
        let s = self.qstate(queue);
        let idx = s.intern.lock().unwrap().intern(partition);
        // A mark is incondizionato across every EXISTING group ring of the queue:
        // a push makes the partition pending for every consumer group already
        // polling it. A group nobody polls has no ring (no allocation) and will
        // discover the partition via reseed on first contact (§8).
        let groups = s.groups.lock().unwrap();
        let mut woke = false;
        for ring in groups.values() {
            let sh = (idx as usize) % self.shards;
            let local = idx / self.shards as u32;
            let mut sub = ring.subs[sh].lock().unwrap();
            sub.ensure(local);
            sub.epoch[local as usize] = sub.epoch[local as usize].wrapping_add(1);
            sub.batch_count[local as usize] =
                sub.batch_count[local as usize].saturating_add(count);
            match sub.state[local as usize] {
                IDLE => {
                    if cfg.delayed > 0 {
                        // §6: delayed ⇒ wheel at commit_ts + D (broker now ≈ PG
                        // commit; SQL enforces the exact cut, wheel only schedules).
                        let at = now_ms + cfg.delayed as i64 * 1000 + PAD_MS;
                        sub.wheel_schedule(local, at);
                    } else if cfg.window > 0 {
                        // §6: windowBuffer ⇒ hold until first_mark + W, unless the
                        // batch is already fat (early promotion).
                        if sub.batch_count[local as usize] >= self.window_batch {
                            sub.ready_push_tail(local);
                            woke = true;
                        } else {
                            let at = now_ms + cfg.window as i64 * 1000;
                            sub.wheel_schedule(local, at);
                        }
                    } else {
                        sub.ready_push_tail(local);
                        woke = true;
                    }
                }
                WHEEL => {
                    // A new push while deferred. windowBuffer batch-full ⇒ promote
                    // early; otherwise keep the existing deadline (delayed governs,
                    // window keeps its first-mark deadline).
                    if cfg.window > 0
                        && cfg.delayed == 0
                        && sub.batch_count[local as usize] >= self.window_batch
                    {
                        // stale heap entry is skipped lazily on drain.
                        sub.revisit_at[local as usize] = 0;
                        sub.ready_push_tail(local);
                        woke = true;
                    }
                }
                READY => { /* already claimable */ }
                INFLIGHT => { /* epoch bump ⇒ checkin re-appends (§4) */ }
                _ => {}
            }
        }
        drop(groups);
        if broadcast {
            self.marks_local
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // §5: enqueue a coalesced dirty(queue, partition) hint on EVERY local
            // push — NOT gated on a local group-ring transition. The pushing broker
            // usually has NO consumer for the peers' groups (so no group ring, no
            // transition), yet the peers are exactly who need the hint. The set
            // dedups, so a partition pushed 1000× in one 20ms flush window costs one
            // hint (~tens of hints/s on a hot partition, §5). Received (remote) marks
            // never enqueue (no re-broadcast).
            let mut d = self.dirty.lock().unwrap();
            // Bounded: drop the hint if the coalescing set is huge (loss is healed
            // by the reseed floor — §5/§8).
            if d.len() < 200_000 {
                d.insert((queue.into(), partition.into()));
            }
        } else {
            self.marks_remote
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if woke {
            self.wake(queue);
        }
    }

    /// Local mark (push / transaction commit / DLQ move): sets the partition
    /// pending for every group ring of the queue, wakes parked pops, and (on a
    /// vuoto→pending transition) queues a coalesced mesh dirty hint (§5).
    pub fn mark_local(&self, queue: &str, partition: &str, count: u32, now_ms: i64) {
        self.mark_inner(queue, partition, count, now_ms, true);
    }

    /// Remote mark (a peer's mesh dirty hint): same effect, no re-broadcast (§5).
    pub fn mark_remote(&self, queue: &str, partition: &str, now_ms: i64) {
        self.mark_inner(queue, partition, 1, now_ms, false);
    }

    /// Learn a partition's id↔name (from a pop response / reseed) for the ack
    /// bridge (§7 promote-on-ack is keyed by partition id).
    pub fn note_partition_id(&self, queue: &str, name: &str, id: &str) {
        if !self.enabled || name.is_empty() || id.is_empty() {
            return;
        }
        let s = self.qstate(queue);
        s.intern.lock().unwrap().note_id(name, id);
    }

    /// Ack promote (§7): a completed ack released the lease on (partition_id,
    /// group) — if the entry is still pending (pushes arrived during the lease)
    /// promote it to ready NOW + wake, so the batch is claimable immediately
    /// instead of at lease expiry. No-op when the id/queue/entry is unknown
    /// (the empty-path CAS / reseed floor still covers it).
    pub fn promote_ack(&self, queue: &str, group: &str, partition_id: &str, now_ms: i64) {
        if !self.enabled {
            return;
        }
        let s = match self.qstate_existing(queue) {
            Some(s) => s,
            None => return,
        };
        let idx = match s.intern.lock().unwrap().idx_of_id(partition_id) {
            Some(i) => i,
            None => return,
        };
        let ring = {
            let g = s.groups.lock().unwrap();
            match g.get(group) {
                Some(r) => r.clone(),
                None => return,
            }
        };
        let sh = (idx as usize) % self.shards;
        let local = idx / self.shards as u32;
        let mut sub = ring.subs[sh].lock().unwrap();
        if (local as usize) >= sub.state.len() {
            return;
        }
        let _ = now_ms;
        match sub.state[local as usize] {
            WHEEL => {
                sub.revisit_at[local as usize] = 0;
                sub.ready_push_tail(local);
                drop(sub);
                self.wake(queue);
            }
            INFLIGHT => {
                // ack during an in-flight pop of the same partition: bump epoch so
                // the checkin re-appends rather than clears.
                sub.epoch[local as usize] = sub.epoch[local as usize].wrapping_add(1);
            }
            _ => { /* READY: already claimable. IDLE: nothing pending. */ }
        }
    }

    // -------------------------------------------------------- serve path

    /// Take up to `k` candidates from the (queue, group) ready ring (round-robin
    /// across sub-rings), marking them INFLIGHT. Drains due wheel entries first.
    /// Returns the candidates (name + epoch snapshot for the CAS). An empty
    /// return means the ring is empty (the caller parks / reseeds).
    pub fn take_batch(&self, queue: &str, group: &str, k: usize, now_ms: i64) -> Vec<Candidate> {
        if !self.enabled || k == 0 {
            return Vec::new();
        }
        let s = match self.qstate_existing(queue) {
            Some(s) => s,
            None => return Vec::new(),
        };
        let ring = s.group(group, self.shards);
        // Drain due wheel entries into the ready ring across all sub-rings.
        for sub in ring.subs.iter() {
            let mut sg = sub.lock().unwrap();
            if sg.wheel_peek_due(now_ms) {
                sg.wheel_drain_due(now_ms);
            }
        }
        let intern = s.intern.lock().unwrap();
        let mut out = Vec::with_capacity(k);
        let mut cursor = *ring.take_cursor.lock().unwrap();
        let mut empty_streak = 0usize;
        while out.len() < k && empty_streak < self.shards {
            let sh = cursor % self.shards;
            cursor = cursor.wrapping_add(1);
            let mut sub = ring.subs[sh].lock().unwrap();
            match sub.ready_pop_head() {
                Some(local) => {
                    empty_streak = 0;
                    sub.state[local as usize] = INFLIGHT;
                    let ep = sub.epoch[local as usize];
                    let global = local * self.shards as u32 + sh as u32;
                    if let Some(name) = intern.name_of(global) {
                        out.push(Candidate {
                            name: name.to_string(),
                            epoch: ep,
                        });
                    } else {
                        // interning lost the name (should not happen) — drop it.
                        sub.state[local as usize] = IDLE;
                    }
                }
                None => empty_streak += 1,
            }
        }
        *ring.take_cursor.lock().unwrap() = cursor;
        out
    }

    /// Check the tri-state verdicts back in (§4/§6/§7). `auto_ack` / `lease_ms`
    /// shape the `Took` handling: an autoAck pop holds no lease, so a `Took`
    /// partition is immediately re-claimable (ready); a leased (manual-ack) pop
    /// DID acquire a lease, so re-appending it to ready would just make the next
    /// consumer probe it, get `leased`, and wheel it for the whole lease — instead
    /// a leased `Took` goes to the wheel at lease-expiry (like `Leased`), to be
    /// pulled back early by OUR OWN ack's promote (§7). `Requeue` (budget/cap
    /// skipped, un-leased) always re-appends.
    pub fn checkin(
        &self,
        queue: &str,
        group: &str,
        results: Vec<CheckinResult>,
        now_ms: i64,
        auto_ack: bool,
        lease_ms: i64,
    ) {
        if !self.enabled || results.is_empty() {
            return;
        }
        let cfg = self.cfg_of(queue);
        let s = match self.qstate_existing(queue) {
            Some(s) => s,
            None => return,
        };
        let ring = s.group(group, self.shards);
        let intern = s.intern.lock().unwrap();
        for r in &results {
            let idx = match intern.idx_of(&r.name) {
                Some(i) => i,
                None => continue,
            };
            let sh = (idx as usize) % self.shards;
            let local = idx / self.shards as u32;
            let mut sub = ring.subs[sh].lock().unwrap();
            if (local as usize) >= sub.state.len() {
                continue;
            }
            // Only act on an entry we still own (INFLIGHT). A concurrent promote/
            // mark may have already re-linked it; leave it be.
            if sub.state[local as usize] != INFLIGHT {
                continue;
            }
            match r.verdict {
                Verdict::Took if !auto_ack => {
                    // Leased by us: hold it out of the ready ring until our ack's
                    // promote (or, on a missed ack, the lease-expiry revisit, §7).
                    sub.batch_count[local as usize] = 0;
                    sub.wheel_schedule(local, now_ms + lease_ms.max(1) + PAD_MS);
                }
                Verdict::Took | Verdict::Requeue => {
                    sub.batch_count[local as usize] = 0;
                    sub.ready_push_tail(local);
                }
                Verdict::Leased(until_ms) => {
                    // Never clear a leased candidate (§7): wheel at T + pad.
                    sub.wheel_schedule(local, until_ms + PAD_MS);
                }
                Verdict::Empty => {
                    // Epoch-CAS (§4): clear only if no mark raced.
                    if sub.epoch[local as usize] == r.epoch {
                        if cfg.is_deferral() {
                            // §6: deferral ⇒ never hard-clear; revisit with a
                            // bounded backoff (a fixed floor here; phase 2 could
                            // read the exact earliest-visible from the SQL meta).
                            let back = if cfg.delayed > 0 {
                                (cfg.delayed as i64 * 1000).clamp(REVISIT_MIN_MS, REVISIT_MAX_MS)
                            } else {
                                REVISIT_MIN_MS
                            };
                            sub.wheel_schedule(local, now_ms + back);
                        } else {
                            sub.state[local as usize] = IDLE;
                            sub.batch_count[local as usize] = 0;
                        }
                    } else {
                        // A mark raced during dispatch — re-append (§4).
                        sub.ready_push_tail(local);
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------ reseed

    /// Reseed policy (§8): does this (queue, group) need a keyset reseed now
    /// (ring cold / stale beyond `interval_ms`)? Caller runs the SQL scan and
    /// feeds rows via [`reseed_row`], then calls [`reseed_done`]. `force` marks
    /// a fresh (never-reseeded) group as cold-start.
    pub fn reseed_due(&self, queue: &str, group: &str, now_ms: i64, interval_ms: i64) -> bool {
        if !self.enabled {
            return false;
        }
        let s = self.qstate(queue);
        let ring = s.group(group, self.shards);
        let last = *ring.reseed_ms.lock().unwrap();
        last == 0 || now_ms - last >= interval_ms
    }

    /// Feed one reseed row: intern the name, remember the id, and mark the
    /// partition pending WITHOUT broadcasting (a reseed is local discovery).
    pub fn reseed_row(&self, queue: &str, group: &str, id: &str, name: &str, now_ms: i64) {
        if !self.enabled {
            return;
        }
        let cfg = self.cfg_of(queue);
        let s = self.qstate(queue);
        {
            let mut it = s.intern.lock().unwrap();
            it.note_id(name, id);
        }
        let idx = s.intern.lock().unwrap().idx_of(name).unwrap();
        let ring = s.group(group, self.shards);
        let sh = (idx as usize) % self.shards;
        let local = idx / self.shards as u32;
        let mut sub = ring.subs[sh].lock().unwrap();
        sub.ensure(local);
        // Only insert if currently IDLE (don't disturb a live INFLIGHT/leased entry).
        if sub.state[local as usize] == IDLE {
            if cfg.delayed > 0 {
                sub.wheel_schedule(local, now_ms + cfg.delayed as i64 * 1000 + PAD_MS);
            } else {
                sub.ready_push_tail(local);
            }
        }
    }

    /// Mark this (queue, group) reseeded at `now_ms` (resets the interval clock)
    /// and wake parked pops (the reseed may have populated the ring).
    pub fn reseed_done(&self, queue: &str, group: &str, now_ms: i64) {
        if !self.enabled {
            return;
        }
        let s = self.qstate(queue);
        let ring = s.group(group, self.shards);
        *ring.reseed_ms.lock().unwrap() = now_ms;
        self.reseeds
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.wake(queue);
    }

    // -------------------------------------------------------- background

    /// Background wheel tick (§6): promote every due wheel entry across every
    /// ring to READY and wake the affected queues. Run every ~50-100ms by main.
    pub fn tick(&self, now_ms: i64) {
        if !self.enabled {
            return;
        }
        let queues: Vec<(String, Arc<QueueState>)> = {
            let q = self.queues.lock().unwrap();
            q.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        for (qname, s) in queues {
            let groups: Vec<Arc<GroupRing>> =
                { s.groups.lock().unwrap().values().cloned().collect() };
            let mut woke = false;
            for ring in groups {
                for sub in ring.subs.iter() {
                    let mut sg = sub.lock().unwrap();
                    if sg.wheel_peek_due(now_ms) && sg.wheel_drain_due(now_ms) > 0 {
                        woke = true;
                    }
                }
            }
            if woke {
                self.wake(&qname);
            }
        }
    }

    /// Register a (queue, group) so subsequent marks reach it. A consumer's first
    /// wildcard poll implicitly does this via [`take_batch`]; a mark for a group
    /// with no ring is a no-op (the group discovers via reseed on first poll, §8),
    /// so a group must be registered before marks flow to it. Exposed mainly for
    /// tests and any caller that wants to pre-warm a group.
    pub fn ensure_group(&self, queue: &str, group: &str) {
        if !self.enabled {
            return;
        }
        let s = self.qstate(queue);
        let _ = s.group(group, self.shards);
    }

    /// Drain up to `max` coalesced local dirty transitions for the mesh flusher
    /// (§5). Returns (queue, partition) pairs; the set is cleared.
    pub fn drain_dirty(&self, max: usize) -> Vec<(String, String)> {
        if !self.enabled {
            return Vec::new();
        }
        let mut d = self.dirty.lock().unwrap();
        if d.is_empty() {
            return Vec::new();
        }
        let take: Vec<(Box<str>, Box<str>)> = d.iter().take(max).cloned().collect();
        for k in &take {
            d.remove(k);
        }
        take.into_iter()
            .map(|(q, p)| (String::from(q), String::from(p)))
            .collect()
    }
}

// ============================================================================
// Unit tests (§10): interning, ring transitions / round-robin / no-resize,
// the epoch-CAS interleavings (§4) as deterministic tests, the wheel (delayed,
// windowBuffer early promotion, lease revisit), tri-state → revisit → promote,
// and reseed keyset seeding.
// ============================================================================
#[cfg(test)]
mod tests {
    use super::*;

    fn hl() -> Arc<HotList> {
        HotList::new(true, 4, 100)
    }
    // single-shard variant makes ring order deterministic for the ordering tests.
    fn hl1() -> Arc<HotList> {
        HotList::new(true, 1, 100)
    }

    fn names(c: &[Candidate]) -> Vec<String> {
        c.iter().map(|x| x.name.clone()).collect()
    }

    // A consumer's first poll registers the (queue, group) ring (then reseeds
    // from PG, §8); only after that do marks reach it. Tests register explicitly
    // to isolate the ring mechanics from the DB-backed reseed.
    fn reg(h: &HotList, q: &str, g: &str) {
        h.ensure_group(q, g);
    }

    #[test]
    fn disabled_is_all_noop() {
        let h = HotList::new(false, 4, 100);
        h.ensure_group("q", "g");
        h.mark_local("q", "p0", 1, 0);
        assert!(h.take_batch("q", "g", 10, 0).is_empty());
        assert!(h.drain_dirty(10).is_empty());
        assert!(!h.reseed_due("q", "g", 0, 1000));
    }

    #[test]
    fn intern_is_dense_and_stable() {
        let mut it = QueueIntern::new();
        assert_eq!(it.intern("a"), 0);
        assert_eq!(it.intern("b"), 1);
        assert_eq!(it.intern("a"), 0); // stable
        assert_eq!(it.intern("c"), 2);
        assert_eq!(it.name_of(1), Some("b"));
        it.note_id("b", "id-b");
        assert_eq!(it.idx_of_id("id-b"), Some(1));
    }

    #[test]
    fn mark_before_group_registered_is_lost_then_reseed_recovers() {
        // A mark for a group with no ring is a no-op — the group discovers via
        // reseed on first poll (§8). This is the cold-start contract.
        let h = hl1();
        h.mark_local("q", "p0", 1, 0); // no "g" ring yet → lost
        assert!(h.take_batch("q", "g", 10, 0).is_empty());
        // reseed is what recovers it
        h.reseed_row("q", "g", "id0", "p0", 0);
        h.reseed_done("q", "g", 0);
        assert_eq!(names(&h.take_batch("q", "g", 10, 0)), vec!["p0"]);
    }

    #[test]
    fn mark_idle_to_ready_then_take() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        h.mark_local("q", "p1", 1, 0);
        let c = h.take_batch("q", "g", 10, 0);
        assert_eq!(names(&c), vec!["p0", "p1"]); // FIFO order, single shard
    }

    #[test]
    fn push_to_already_pending_is_epoch_bump_no_dup() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        h.mark_local("q", "p0", 1, 0); // already pending
        let c = h.take_batch("q", "g", 10, 0);
        assert_eq!(names(&c), vec!["p0"]); // no duplicate ring entry
    }

    #[test]
    fn round_robin_reappend_on_took() {
        let h = hl1();
        reg(&h, "q", "g");
        for i in 0..3 {
            h.mark_local("q", &format!("p{i}"), 1, 0);
        }
        let c = h.take_batch("q", "g", 3, 0);
        assert_eq!(names(&c), vec!["p0", "p1", "p2"]);
        // all took → re-appended to tail in the same order
        let res: Vec<CheckinResult> = c
            .iter()
            .map(|x| CheckinResult {
                name: x.name.clone(),
                epoch: x.epoch,
                verdict: Verdict::Took,
            })
            .collect();
        h.checkin("q", "g", res, 0, true, 60_000);
        let c2 = h.take_batch("q", "g", 3, 0);
        assert_eq!(names(&c2), vec!["p0", "p1", "p2"]);
    }

    // Manual-ack (leased) Took must NOT re-appear in the ready ring — we hold the
    // lease. It sits in the wheel until lease-expiry, and our OWN ack's promote
    // pulls it back early (§7). Re-appending it (the autoAck behaviour) would make
    // the next consumer probe it, get `leased`, and wheel it for the whole lease.
    #[test]
    fn manual_ack_took_wheels_until_ack_promote() {
        let h = hl1();
        reg(&h, "q", "g");
        h.note_partition_id("q", "p0", "id0");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, 0);
        assert_eq!(names(&c), vec!["p0"]);
        // leased Took (auto_ack=false, lease 60s)
        h.checkin(
            "q",
            "g",
            vec![CheckinResult {
                name: "p0".into(),
                epoch: c[0].epoch,
                verdict: Verdict::Took,
            }],
            100,
            false,
            60_000,
        );
        // not immediately re-claimable (we hold the lease)
        assert!(h.take_batch("q", "g", 1, 200).is_empty());
        // our ack releases the lease → promote pulls it back at once
        h.promote_ack("q", "g", "id0", 300);
        assert_eq!(names(&h.take_batch("q", "g", 1, 300)), vec!["p0"]);
    }

    // §4 interleaving 1: mark BETWEEN snapshot and clear ⇒ CAS fails ⇒ stays.
    #[test]
    fn cas_mark_before_clear_keeps_entry() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, 0); // snapshots epoch, INFLIGHT
        assert_eq!(names(&c), vec!["p0"]);
        // a push commits + marks during dispatch (epoch bumps)
        h.mark_local("q", "p0", 1, 0);
        // pop came back empty → CAS must FAIL (epoch changed) → re-append
        h.checkin(
            "q",
            "g",
            vec![CheckinResult {
                name: "p0".into(),
                epoch: c[0].epoch,
                verdict: Verdict::Empty,
            }],
            0,
            true,
            60_000,
        );
        let c2 = h.take_batch("q", "g", 1, 0);
        assert_eq!(names(&c2), vec!["p0"], "raced mark must keep the entry");
    }

    // §4 interleaving 2: no race ⇒ empty CAS succeeds ⇒ cleared (IDLE).
    #[test]
    fn cas_no_race_clears_entry() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, 0);
        h.checkin(
            "q",
            "g",
            vec![CheckinResult {
                name: "p0".into(),
                epoch: c[0].epoch,
                verdict: Verdict::Empty,
            }],
            0,
            true,
            60_000,
        );
        assert!(h.take_batch("q", "g", 1, 0).is_empty(), "clean empty clears");
    }

    // §4 interleaving 3: mark delayed PAST the clear ⇒ clear passes, late mark re-adds.
    #[test]
    fn cas_mark_after_clear_readds() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, 0);
        h.checkin(
            "q",
            "g",
            vec![CheckinResult {
                name: "p0".into(),
                epoch: c[0].epoch,
                verdict: Verdict::Empty,
            }],
            0,
            true,
            60_000,
        );
        assert!(h.take_batch("q", "g", 1, 0).is_empty());
        // a late mark (the delayed push) re-adds
        h.mark_local("q", "p0", 1, 0);
        assert_eq!(names(&h.take_batch("q", "g", 1, 0)), vec!["p0"]);
    }

    #[test]
    fn leased_goes_to_wheel_not_cleared() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, 1000);
        h.checkin(
            "q",
            "g",
            vec![CheckinResult {
                name: "p0".into(),
                epoch: c[0].epoch,
                verdict: Verdict::Leased(5000), // leased until t=5000ms
            }],
            1000,
            true,
            60_000,
        );
        // not claimable before the lease expiry (+pad)
        assert!(h.take_batch("q", "g", 1, 2000).is_empty());
        // claimable after T + pad
        assert_eq!(names(&h.take_batch("q", "g", 1, 5500)), vec!["p0"]);
    }

    #[test]
    fn ack_promote_pulls_leased_entry_early() {
        let h = hl1();
        reg(&h, "q", "g");
        h.note_partition_id("q", "p0", "id0");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, 0);
        h.checkin(
            "q",
            "g",
            vec![CheckinResult {
                name: "p0".into(),
                epoch: c[0].epoch,
                verdict: Verdict::Leased(10_000),
            }],
            0,
            true,
            60_000,
        );
        assert!(h.take_batch("q", "g", 1, 100).is_empty());
        // ack releases the lease early → promote by id
        h.promote_ack("q", "g", "id0", 100);
        assert_eq!(names(&h.take_batch("q", "g", 1, 100)), vec!["p0"]);
    }

    #[test]
    fn delayed_mark_holds_until_wheel_fires() {
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 2, 0, 0); // delayed 2s
        h.mark_local("q", "p0", 1, 0);
        // held in the wheel — not claimable immediately
        assert!(h.take_batch("q", "g", 1, 500).is_empty());
        // tick before deadline → still nothing
        h.tick(1000);
        assert!(h.take_batch("q", "g", 1, 1000).is_empty());
        // after commit_ts + D + pad
        h.tick(2400);
        assert_eq!(names(&h.take_batch("q", "g", 1, 2400)), vec!["p0"]);
    }

    #[test]
    fn window_buffer_delivers_at_window_and_early_on_full_batch() {
        // window 5s, threshold 100 (hl* default)
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 0, 5, 0);
        // small batch: held until the window expires
        h.mark_local("q", "p0", 10, 0);
        assert!(h.take_batch("q", "g", 1, 1000).is_empty());
        h.tick(5100);
        assert_eq!(names(&h.take_batch("q", "g", 1, 5100)), vec!["p0"]);

        // fat batch on a fresh partition: promoted early (before the window)
        h.mark_local("q", "p1", 100, 10_000); // batch_count >= threshold
        assert_eq!(names(&h.take_batch("q", "g", 1, 10_050)), vec!["p1"]);
    }

    #[test]
    fn window_buffer_early_promote_when_batch_fattens_in_wheel() {
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 0, 5, 0);
        h.mark_local("q", "p0", 40, 0); // below threshold → wheel
        assert!(h.take_batch("q", "g", 1, 100).is_empty());
        h.mark_local("q", "p0", 70, 100); // now batch_count=110 ≥ 100 → early promote
        assert_eq!(names(&h.take_batch("q", "g", 1, 200)), vec!["p0"]);
    }

    #[test]
    fn deferral_empty_revisits_not_clears() {
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 2, 0, 0); // delayed queue
        // put p0 ready via the wheel firing
        h.mark_local("q", "p0", 1, 0);
        h.tick(2400);
        let c = h.take_batch("q", "g", 1, 2400);
        assert_eq!(names(&c), vec!["p0"]);
        // empty verdict on a deferral queue must NOT hard-clear — it revisits
        h.checkin(
            "q",
            "g",
            vec![CheckinResult {
                name: "p0".into(),
                epoch: c[0].epoch,
                verdict: Verdict::Empty,
            }],
            2400,
            true,
            60_000,
        );
        // still tracked (in the wheel), reappears after the backoff
        assert!(h.take_batch("q", "g", 1, 2400).is_empty());
        h.tick(10_000);
        assert_eq!(names(&h.take_batch("q", "g", 1, 10_000)), vec!["p0"]);
    }

    #[test]
    fn dirty_on_local_push_deduped_never_remote() {
        // §5: a LOCAL push always enqueues a dirty hint (the pushing broker often
        // has no consumer for the peers' groups), deduped per flush window; a REMOTE
        // mark never re-broadcasts.
        let h = hl1();
        // NB: no group ring here — the dirty hint must fire even when this broker
        // has no local consumer (exactly the cross-broker case).
        h.mark_local("q", "p0", 1, 0); // push → dirty
        h.mark_local("q", "p0", 1, 0); // same partition → deduped
        h.mark_remote("q", "p1", 0); // remote → never dirty
        let d = h.drain_dirty(100);
        assert_eq!(d, vec![("q".to_string(), "p0".to_string())]);
        assert!(h.drain_dirty(100).is_empty()); // drained
    }

    #[test]
    fn remote_mark_makes_candidate_discoverable() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_remote("q", "p9", 0);
        assert_eq!(names(&h.take_batch("q", "g", 5, 0)), vec!["p9"]);
    }

    #[test]
    fn reseed_seeds_ring_and_sets_clock() {
        let h = hl1();
        assert!(h.reseed_due("q", "g", 1000, 30_000)); // cold
        h.reseed_row("q", "g", "id0", "p0", 1000);
        h.reseed_row("q", "g", "id1", "p1", 1000);
        h.reseed_done("q", "g", 1000);
        assert_eq!(names(&h.take_batch("q", "g", 5, 1000)), vec!["p0", "p1"]);
        assert!(!h.reseed_due("q", "g", 1000, 30_000)); // just reseeded
        assert!(h.reseed_due("q", "g", 40_000, 30_000)); // stale again
    }

    #[test]
    fn subshard_round_robin_spreads_and_no_resize_panic() {
        // many partitions across 4 shards; take spreads across sub-rings.
        let h = hl();
        reg(&h, "q", "g");
        for i in 0..40 {
            h.mark_local("q", &format!("p{i}"), 1, 0);
        }
        let c = h.take_batch("q", "g", 40, 0);
        assert_eq!(c.len(), 40);
        // all distinct
        let set: HashSet<String> = names(&c).into_iter().collect();
        assert_eq!(set.len(), 40);
    }

    #[test]
    fn per_group_rings_are_independent() {
        let h = hl1();
        reg(&h, "q", "g1");
        reg(&h, "q", "g2");
        // a mark reaches BOTH registered group rings
        h.mark_local("q", "p0", 1, 0);
        assert_eq!(names(&h.take_batch("q", "g1", 5, 0)), vec!["p0"]);
        assert_eq!(names(&h.take_batch("q", "g2", 5, 0)), vec!["p0"]);
        // g1 draining (checkin empty) does not affect g2
        let c = h.take_batch("q", "g1", 5, 0); // empty (already taken above)
        assert!(c.is_empty());
    }
}
