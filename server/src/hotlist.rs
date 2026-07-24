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
// §8: max per-group phase offset for the periodic reseed floor. A fixed random
// offset in [0, this) per (queue,group) de-synchronizes the reseed of many groups
// that registered together (cold start), so the background floor never fires as
// one synchronized keyset-scan storm.
pub const RESEED_JITTER_MS: i64 = 15_000;

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
    /// Took only (spec §2): the claim exhausted the partition's visible backlog
    /// (served batch_end reached the claim-time allocator watermark). Gates the
    /// covered-ack clear: a non-drained Took must PROMOTE on ack — batch_count
    /// alone misses pre-existing backlog (the 2026-07-24 manualAck stall).
    pub drained: bool,
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
    drained: Vec<bool>,
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
            drained: Vec::new(),
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
            self.drained.resize(need, false);
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
    // §8: fixed per-group phase offset [0, RESEED_JITTER_MS) so the periodic reseed
    // floor of co-registered groups lands at staggered instants (never one storm).
    reseed_jitter_ms: i64,
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
            reseed_jitter_ms: (rand::random::<u32>() as i64) % RESEED_JITTER_MS,
            take_cursor: Mutex::new(0),
        }
    }
}

struct QueueState {
    intern: Mutex<QueueIntern>,
    cfg: Mutex<DeferCfg>,
    groups: Mutex<HashMap<String, Arc<GroupRing>>>,
    // C1 wake coalescing: a push-path (quiet) mark that made a partition newly ready
    // sets this flag (O(1), no lock/alloc — the QueueState is already in hand). The
    // wake tick (main.rs, ~5ms) then issues ONE notify_waiters per marked queue per
    // tick instead of one per push — the O(parked consumers) storm that was the
    // residual producer-RTT gap. Immediate wakes (transaction / mesh-remote /
    // wheel / promote-on-ack) bypass this and wake at once, unchanged.
    wake_pending: std::sync::atomic::AtomicBool,
}

impl QueueState {
    fn new() -> Self {
        QueueState {
            intern: Mutex::new(QueueIntern::new()),
            cfg: Mutex::new(DeferCfg::default()),
            groups: Mutex::new(HashMap::new()),
            wake_pending: std::sync::atomic::AtomicBool::new(false),
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
    // §5: does this broker have mesh peers configured? When false the coalesced
    // dirty(queue, partition) set is a dead end (no flusher drains it), so a local
    // mark skips it entirely — no global-lock traffic and no per-push (Box<str>,
    // Box<str>) allocation on a single-broker deployment.
    mesh_active: bool,
    queues: Mutex<HashMap<String, Arc<QueueState>>>,
    notifier: OnceLock<Arc<Notifier>>,
    trace_prefix: Option<String>,
    // Coalesced local vuoto→pending transitions, drained by main's mesh flusher
    // (§5). Received (remote) marks never enqueue here (no re-broadcast).
    dirty: Mutex<HashSet<(Box<str>, Box<str>)>>,
    // Observability.
    pub reseeds: std::sync::atomic::AtomicU64,
    pub marks_local: std::sync::atomic::AtomicU64,
    pub marks_remote: std::sync::atomic::AtomicU64,
}

impl HotList {
    pub fn new(enabled: bool, shards: usize, window_batch: u32, mesh_active: bool) -> Arc<HotList> {
        Arc::new(HotList {
            enabled,
            shards: shards.max(1),
            window_batch: window_batch.max(1),
            mesh_active,
            queues: Mutex::new(HashMap::new()),
            notifier: OnceLock::new(),
            trace_prefix: std::env::var("QUEEN_HOTLIST_TRACE").ok().filter(|v| !v.is_empty()),
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
    /// Env-gated chain trace (QUEEN_HOTLIST_TRACE=<queue-prefix>): bounded-volume
    /// mark/wake/serve breadcrumbs for latency debugging. Default: off, zero cost
    /// beyond one Option check.
    pub fn traced(&self, queue: &str) -> bool {
        match &self.trace_prefix {
            Some(p) => queue.starts_with(p.as_str()),
            None => false,
        }
    }

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
        wake: bool,
    ) {
        if !self.enabled || partition.is_empty() {
            return;
        }
        // One global `queues` lookup for the whole mark (was two: a separate
        // cfg_of + qstate each locked it — hot under the push rate).
        let s = self.qstate(queue);
        let cfg = *s.cfg.lock().unwrap();
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
            if self.trace_prefix.is_some() && self.traced(queue) {
                eprintln!("[hlt] mark q={} p={} st={} t={}", queue, partition,
                    sub.state[local as usize], crate::hotlist::trace_now_ms());
            }
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
            // never enqueue (no re-broadcast). Skipped wholesale with no peers: the
            // flusher never runs, so the set would only cost lock traffic + a per-push
            // allocation for nothing (single-broker fast path).
            if self.mesh_active {
                let mut d = self.dirty.lock().unwrap();
                // Bounded: drop the hint if the coalescing set is huge (loss is healed
                // by the reseed floor — §5/§8).
                if d.len() < 200_000 {
                    d.insert((queue.into(), partition.into()));
                }
            }
        } else {
            self.marks_remote
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        // Wake routing. `wake=true` (transaction / mesh-remote) wakes at once — low
        // frequency, unchanged. `wake=false` (the per-push quiet path) does NOT wake
        // inline; it flags the queue (O(1) atomic) for the coalescing wake tick, which
        // issues ONE notify_waiters per marked queue per ~5ms instead of one per push
        // (the O(parked consumers) storm — C1). The parked pop's own backoff re-poll
        // is the floor, so a late/missed tick only adds bounded latency, never a hang.
        if woke {
            if wake {
                self.wake(queue);
            } else {
                s.wake_pending
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// Local mark that ALSO wakes parked pops (transaction commit / DLQ move / any
    /// path that does not separately notify): sets the partition pending on every
    /// group ring, wakes on a vuoto→pending transition, and (with peers) queues a
    /// coalesced mesh dirty hint (§5).
    pub fn mark_local(&self, queue: &str, partition: &str, count: u32, now_ms: i64) {
        self.mark_inner(queue, partition, count, now_ms, true, true);
    }

    /// Local mark WITHOUT the internal wake — for the push handler, which already
    /// calls notify_pushed_batch (one wake per push, issued AFTER the ring is
    /// marked). Still queues the mesh dirty hint (with peers). Avoids the redundant
    /// second notify_waiters per push (the flag-on push-RTT regression).
    pub fn mark_local_quiet(&self, queue: &str, partition: &str, count: u32, now_ms: i64) {
        self.mark_inner(queue, partition, count, now_ms, true, false);
    }

    /// Remote mark (a peer's mesh dirty hint): same effect, wakes parked pops (the
    /// receiving broker has no other wake for this), no re-broadcast (§5).
    pub fn mark_remote(&self, queue: &str, partition: &str, now_ms: i64) {
        self.mark_inner(queue, partition, 1, now_ms, false, true);
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
    pub fn promote_ack(
        &self,
        queue: &str,
        group: &str,
        partition_id: &str,
        now_ms: i64,
        covered: bool,
    ) {
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
                // Spec §2 clear-su-ack: a COVERED ack (whole leased batch completed)
                // with NO marks since the Took parking (batch_count==0; the Took
                // reset is epoch-guarded, so an in-lease mark always survives here)
                // means the partition is caught up — clear to IDLE instead of
                // promoting. The unconditional promote was measured (2026-07-24 VM
                // trace) to induce one guaranteed-empty probe per delivery: 2x
                // attempts per message = the vegas admission queue at 10k queues
                // (~250ms of woken→served wait). A NACK/partial release
                // (covered=false) always promotes: the batch itself is
                // redeliverable content.
                if covered
                    && sub.batch_count[local as usize] == 0
                    && sub.drained[local as usize]
                {
                    sub.revisit_at[local as usize] = 0;
                    sub.state[local as usize] = IDLE; // stale heap entry skips lazily
                } else {
                    sub.revisit_at[local as usize] = 0;
                    sub.ready_push_tail(local);
                    drop(sub);
                    self.wake(queue);
                }
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
                    // drained is a claim-scoped fact: reset here so only THIS
                    // claim's Took checkin can re-assert it. Without the reset a
                    // racing Leased checkin (10 concurrent poppers, one winner)
                    // can park the entry while a STALE drained=true from an older
                    // cycle survives — the next covered ack would then clear a
                    // backlogged partition (measured 2026-07-24: the intermittent
                    // testConsumerOrderingConcurrency stall).
                    sub.drained[local as usize] = false;
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

    /// Cheap, NON-destructive readiness peek (discovery-latency fix, 2026-07-24):
    /// does the (queue, group) ring have anything a `take_batch` would return right
    /// now — a ready entry, or a wheel entry already due at `now_ms`? Locks each
    /// sub-ring briefly but touches NO partition state (no INFLIGHT mark, no wheel
    /// drain) and NEVER creates a ring. The pop serve path calls this before
    /// acquiring the shared pop_vegas permit / a pooled connection, so a quiet
    /// empty re-poll (the O(#queues) long-poll storm) short-circuits without ever
    /// contending on the serving limiter.
    pub fn has_ready(&self, queue: &str, group: &str, now_ms: i64) -> bool {
        if !self.enabled {
            return false;
        }
        let s = match self.qstate_existing(queue) {
            Some(s) => s,
            None => return false,
        };
        let ring = {
            let g = s.groups.lock().unwrap();
            match g.get(group) {
                Some(r) => r.clone(),
                None => return false,
            }
        };
        for sub in ring.subs.iter() {
            let sg = sub.lock().unwrap();
            if sg.len_ready > 0 || sg.wheel_peek_due(now_ms) {
                return true;
            }
        }
        false
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
                    // batch_count reset is EPOCH-GUARDED: a mark that raced in
                    // between our claim and this checkin bumped the epoch and its
                    // count must survive — it is the clear-su-ack signal that new
                    // content arrived during the lease (promote, don't clear).
                    if sub.epoch[local as usize] == r.epoch {
                        sub.batch_count[local as usize] = 0;
                    }
                    // Claim-time fact, stored unconditionally: the covered-ack
                    // clear may only fire on a drained claim (spec §2).
                    sub.drained[local as usize] = r.drained;
                    sub.wheel_schedule(local, now_ms + lease_ms.max(1) + PAD_MS);
                }
                Verdict::Took => {
                    // auto-ack Took (same epoch guard as the leased arm above).
                    // debounce instead of going READY — a fresh delivery means the
                    // partition just fired, so hold it for another window before the
                    // next probe. Going straight to READY would re-probe immediately
                    // and deliver each subsequent message un-batched under continuous
                    // writes (diverging from the SQL quiet-period debounce). The
                    // batch-full early promotion (mark WHEEL branch) still fires if the
                    // window fattens past the threshold, preserving min(window, full).
                    // NB: a large backlog past the window is drained one budget-chunk
                    // per window rather than promptly — acceptable for a smoothing
                    // queue; if that latency matters, thread the delivered count and
                    // keep READY when the batch came back full.
                    if sub.epoch[local as usize] == r.epoch {
                        sub.batch_count[local as usize] = 0;
                    }
                    if cfg.window > 0 && cfg.delayed == 0 {
                        sub.wheel_schedule(local, now_ms + cfg.window as i64 * 1000);
                    } else {
                        sub.ready_push_tail(local);
                    }
                }
                Verdict::Requeue => {
                    // Budget/cap-skipped, un-leased: re-append promptly (never window).
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
        // Re-add an untracked (IDLE) partition, OR reclaim a stray INFLIGHT one:
        // an INFLIGHT entry whose pop was dropped between take_batch and checkin
        // (client disconnect) is otherwise stranded forever — a mark/promote can
        // only bump its epoch, never re-link it — so the reseed floor is its last
        // resort. Re-adding an entry that is still genuinely in flight is a harmless
        // false positive: the concurrent pop's checkin finds state != INFLIGHT and
        // skips, and the redundant candidate costs at most one empty SKIP LOCKED
        // claim (§1/§7 "stale in eccesso").
        //
        // A WHEEL entry on a NON-DEFERRAL queue is ALSO reclaimed here (2026-07-24,
        // the retries-stall floor): on such a queue a wheel entry can ONLY be a
        // lease-expiry parking (a leased Took, or a Leased verdict), never a
        // visibility deferral. The reseed SQL returns this partition solely because
        // it is PENDING (last_offset > committed), so its lease was released EARLY
        // (e.g. a NACK whose promote-on-ack hook did not run) yet the entry is
        // stranded in the wheel until the ORIGINAL lease expiry — up to leaseTime,
        // 300s by default. Promote it to ready: a still-live foreign lease yields
        // one empty SKIP-LOCKED probe → Leased verdict → re-wheeled at the correct
        // expiry; a released lease redelivers now. On a DEFERRAL queue the wheel
        // deadline IS the visibility cut (delayed/window), so it is left untouched.
        // READY is already claimable.
        let s = sub.state[local as usize];
        match s {
            IDLE | INFLIGHT => {
                if cfg.delayed > 0 {
                    sub.wheel_schedule(local, now_ms + cfg.delayed as i64 * 1000 + PAD_MS);
                } else {
                    sub.ready_push_tail(local);
                }
            }
            WHEEL if !cfg.is_deferral() => {
                sub.revisit_at[local as usize] = 0;
                sub.ready_push_tail(local);
            }
            _ => { /* READY, or a deferral-queue WHEEL: keep the live deadline. */ }
        }
    }

    /// §8 periodic floor: enumerate registered (queue, group) pairs whose reseed is
    /// due — last reseed older than `interval_ms + per-group jitter`. Runs even when
    /// the ring is NON-empty (the pop-path empty-ring reseed stays a fast path but is
    /// no longer the only trigger), so a partition erroneously dropped from a busy
    /// ring (a cross-broker uncommitted-lease false-clear, a stale-config hard-clear,
    /// a missed mark) is recovered within one interval. Skips never-reseeded groups
    /// (`last == 0`): those are the pop-path cold-start bootstrap's job, and firing
    /// them here would be the cold-start scan storm the jitter exists to avoid.
    pub fn periodic_reseed_due(&self, now_ms: i64, interval_ms: i64) -> Vec<(String, String)> {
        if !self.enabled {
            return Vec::new();
        }
        let queues: Vec<(String, Arc<QueueState>)> = {
            self.queues
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        let mut out = Vec::new();
        for (qname, s) in queues {
            let groups: Vec<(String, Arc<GroupRing>)> = {
                s.groups
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            };
            for (gname, ring) in groups {
                let last = *ring.reseed_ms.lock().unwrap();
                if last != 0 && now_ms - last >= interval_ms + ring.reseed_jitter_ms {
                    out.push((qname.clone(), gname));
                }
            }
        }
        out
    }

    /// Observability (§10 / VM plan): per (queue, group) ring sizes —
    /// (queue, group, ready_len, wheel_len). `ready_len` is exact; `wheel_len` is the
    /// heap length (an upper bound — it includes not-yet-drained stale entries).
    pub fn ring_sizes(&self) -> Vec<(String, String, usize, usize)> {
        let queues: Vec<(String, Arc<QueueState>)> = {
            self.queues
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        let mut out = Vec::new();
        for (qname, s) in queues {
            let groups: Vec<(String, Arc<GroupRing>)> = {
                s.groups
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            };
            for (gname, ring) in groups {
                let mut ready = 0usize;
                let mut wheel = 0usize;
                for sub in ring.subs.iter() {
                    let sg = sub.lock().unwrap();
                    ready += sg.len_ready;
                    wheel += sg.wheel.len();
                }
                out.push((qname.clone(), gname, ready, wheel));
            }
        }
        out
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

    /// Coalesced local wake (C1): wake the queue gate of every queue that received a
    /// push-path (quiet) mark since the last tick — ONE notify_waiters per marked
    /// queue, vs one per push. Run every ~5ms by main. Returns the number of queues
    /// woken (for tests/observability). Immediate wakes (transaction / mesh-remote /
    /// wheel / promote-on-ack) do not flow through here. The parked pop's own backoff
    /// re-poll (≤ pop_wait_max_interval_ms) is the correctness floor, so this is a
    /// latency optimization only — a stalled/late tick can never strand a consumer.
    pub fn wake_tick(&self) -> usize {
        if !self.enabled {
            return 0;
        }
        let queues: Vec<(String, Arc<QueueState>)> = {
            self.queues
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        let mut woke = 0;
        for (qname, s) in queues {
            if s.wake_pending.swap(false, std::sync::atomic::Ordering::Relaxed) {
                if self.traced(&qname) {
                    eprintln!("[hlt] tickwake q={} t={}", qname, trace_now_ms());
                }
                self.wake(&qname);
                woke += 1;
            }
        }
        woke
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

    /// Invalidate the hot-list ring of a deleted consumer group FOR ONE QUEUE
    /// (the delete-consumer-group-for-queue admin op). Drops the group's per-queue
    /// ring wholesale — its ready list, wheel, reseed clock and every stale
    /// IDLE/READY/WHEEL/INFLIGHT entry — so no pre-delete state survives to mask
    /// the post-delete reconsume; the group re-registers and reseeds from committed
    /// PG state on first contact (§8), which is the whole point of the delete. The
    /// per-queue interning is shared with the queue's OTHER groups, so it is left
    /// intact (an id↔name memo is never wrong, only unused). Wakes the queue so a
    /// parked pop of this group re-polls at once (routing through first-contact).
    /// Over-forgetting is a harmless cold-start; the danger the caller guards
    /// against is a SURVIVING stale ring that hides the reconsume until the
    /// periodic floor.
    pub fn forget_group(&self, queue: &str, group: &str) {
        if !self.enabled {
            return;
        }
        if let Some(s) = self.qstate_existing(queue) {
            let removed = s.groups.lock().unwrap().remove(group).is_some();
            if removed {
                self.wake(queue);
            }
        }
    }

    /// Invalidate the hot-list ring of a deleted consumer group ACROSS EVERY QUEUE
    /// (the delete-consumer-group admin op, all queues). Same rationale as
    /// [`forget_group`], applied to every queue that currently holds a ring for the
    /// group; each such queue is woken.
    pub fn forget_group_all_queues(&self, group: &str) {
        if !self.enabled {
            return;
        }
        let queues: Vec<(String, Arc<QueueState>)> = {
            self.queues
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        for (qname, s) in queues {
            let removed = s.groups.lock().unwrap().remove(group).is_some();
            if removed {
                self.wake(&qname);
            }
        }
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

    // mesh_active=true so the dirty-hint tests exercise the coalescing set.
    fn hl() -> Arc<HotList> {
        HotList::new(true, 4, 100, true)
    }
    // single-shard variant makes ring order deterministic for the ordering tests.
    fn hl1() -> Arc<HotList> {
        HotList::new(true, 1, 100, true)
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
        let h = HotList::new(false, 4, 100, true);
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

    // Discovery-latency fix (2026-07-24): has_ready is the cheap, non-destructive
    // peek the serve path uses to short-circuit a quiet empty re-poll BEFORE it
    // ever acquires the shared pop_vegas serving permit / a pooled connection.
    #[test]
    fn has_ready_reflects_ring_nondestructively() {
        let h = hl1();
        reg(&h, "q", "g");
        // Empty ring → not ready (the quiet re-poll short-circuits here).
        assert!(!h.has_ready("q", "g", 0));
        // A push mark makes it ready.
        h.mark_local("q", "p0", 1, 0);
        assert!(h.has_ready("q", "g", 0));
        // Peeking does NOT consume it: still ready, and take_batch still returns it.
        assert!(h.has_ready("q", "g", 0));
        assert_eq!(names(&h.take_batch("q", "g", 10, 0)), vec!["p0"]);
        // Once checked out (INFLIGHT) it is no longer ready.
        assert!(!h.has_ready("q", "g", 0));
    }

    #[test]
    fn has_ready_false_for_unknown_queue_or_group() {
        let h = hl1();
        assert!(!h.has_ready("never-seen", "g", 0));
        reg(&h, "q", "g");
        // A registered queue but an unpolled group creates no ring → not ready
        // (and, importantly, has_ready must NOT create the group ring).
        assert!(!h.has_ready("q", "other-group", 0));
    }

    // A windowBuffer/delayed entry sits in the wheel; has_ready must report it only
    // once DUE, so the serve path takes the permit exactly when there is drainable
    // work — never for a not-yet-due deferral (which would re-introduce a storm).
    #[test]
    fn has_ready_true_only_when_wheel_entry_is_due() {
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 0, 5, 0); // window_buffer = 5s ⇒ mark parks in the wheel
        h.mark_local("q", "p0", 1, 0); // scheduled at now(0) + 5000ms
        assert!(!h.has_ready("q", "g", 1000), "not due yet");
        assert!(h.has_ready("q", "g", 6000), "due ⇒ ready to drain");
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
                drained: false,
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
                drained: false,
            }],
            100,
            false,
            60_000,
        );
        // not immediately re-claimable (we hold the lease)
        assert!(h.take_batch("q", "g", 1, 200).is_empty());
        // our ack releases the lease → promote pulls it back at once
        h.promote_ack("q", "g", "id0", 300, false);
        assert_eq!(names(&h.take_batch("q", "g", 1, 300)), vec!["p0"]);
    }

    // Spec §2 clear-su-ack: covered ack + nothing arrived during the lease ⇒
    // IDLE, not READY — no guaranteed-empty probe (the 2026-07-24 2x-attempts
    // vegas-queue amplifier).
    #[test]
    fn covered_ack_with_no_marks_clears_to_idle() {
        let h = hl1();
        reg(&h, "q", "g");
        h.note_partition_id("q", "p0", "id0");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, 0);
        assert_eq!(names(&c), vec!["p0"]);
        h.checkin(
            "q",
            "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Took, drained: true }],
            100,
            false,
            60_000,
        );
        h.promote_ack("q", "g", "id0", 300, true);
        assert!(
            h.take_batch("q", "g", 1, 400).is_empty(),
            "caught-up partition must not be re-probed after a covered ack"
        );
    }

    // The intermittent concurrency race (2026-07-24): with N concurrent
    // poppers on one partition, a racing Leased checkin can win over the
    // Took (the INFLIGHT-ownership gate skips the loser) and park the entry
    // while a STALE drained=true from an older cycle survives — the covered
    // ack would then clear a backlogged partition. take_batch resets drained
    // at claim time, so only the current claim's Took can re-assert it.
    #[test]
    fn stale_drained_does_not_survive_a_new_claim() {
        let h = hl1();
        reg(&h, "q", "g");
        h.note_partition_id("q", "p0", "id0");
        // Cycle 1: single message, clean drain — entry ends IDLE with
        // drained=true persisted at its Took.
        h.mark_local("q", "p0", 1, 0);
        let c1 = h.take_batch("q", "g", 1, 0);
        h.checkin("q", "g",
            vec![CheckinResult { name: "p0".into(), epoch: c1[0].epoch, verdict: Verdict::Took, drained: true }],
            10, false, 60_000);
        h.promote_ack("q", "g", "id0", 20, true); // covered+drained ⇒ IDLE
        // Cycle 2: a 100-message backlog lands; OUR claim's Took checkin loses
        // the race to a concurrent popper's Leased verdict (ownership gate has
        // already been passed by the Leased one in this simulation).
        h.mark_local("q", "p0", 100, 30);
        let c2 = h.take_batch("q", "g", 1, 30);
        h.checkin("q", "g",
            vec![CheckinResult { name: "p0".into(), epoch: c2[0].epoch, verdict: Verdict::Leased(60_030), drained: false }],
            40, false, 60_000);
        // The winner's ack arrives (covered): the stale drained=true from cycle
        // 1 must NOT cause a clear — the entry must come back claimable.
        h.promote_ack("q", "g", "id0", 50, true);
        assert_eq!(
            names(&h.take_batch("q", "g", 1, 60)),
            vec!["p0"],
            "stale drained from a previous cycle must not clear a backlogged partition"
        );
    }

    // The manualAck regression (2026-07-24): a backlog consumed in batches has
    // no in-lease marks, but the claim did NOT drain the partition — the
    // covered ack must PROMOTE, not clear, or every next batch stalls on the
    // reseed floor.
    #[test]
    fn covered_ack_undrained_backlog_promotes() {
        let h = hl1();
        reg(&h, "q", "g");
        h.note_partition_id("q", "p0", "id0");
        h.mark_local("q", "p0", 1000, 0);
        let c = h.take_batch("q", "g", 1, 0);
        h.checkin(
            "q",
            "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Took, drained: false }],
            100,
            false,
            60_000,
        );
        h.promote_ack("q", "g", "id0", 300, true);
        assert_eq!(
            names(&h.take_batch("q", "g", 1, 400)),
            vec!["p0"],
            "undrained backlog must be claimable immediately after the covered ack"
        );
    }

    #[test]
    fn covered_ack_with_in_lease_mark_promotes() {
        let h = hl1();
        reg(&h, "q", "g");
        h.note_partition_id("q", "p0", "id0");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, 0);
        h.checkin(
            "q",
            "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Took, drained: true }],
            100,
            false,
            60_000,
        );
        h.mark_local("q", "p0", 1, 150); // push lands during the lease
        h.promote_ack("q", "g", "id0", 300, true);
        assert_eq!(
            names(&h.take_batch("q", "g", 1, 400)),
            vec!["p0"],
            "in-lease content must be claimable immediately after the covered ack"
        );
    }

    // The epoch guard on the Took reset: a mark racing between claim and checkin
    // must survive the batch_count reset, else the covered ack would clear a
    // partition that has fresh content.
    #[test]
    fn in_flight_mark_survives_took_reset() {
        let h = hl1();
        reg(&h, "q", "g");
        h.note_partition_id("q", "p0", "id0");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, 0);
        h.mark_local("q", "p0", 1, 50); // races the in-flight pop
        h.checkin(
            "q",
            "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Took, drained: true }],
            100,
            false,
            60_000,
        );
        h.promote_ack("q", "g", "id0", 300, true);
        assert_eq!(
            names(&h.take_batch("q", "g", 1, 400)),
            vec!["p0"],
            "raced mark must survive the Took batch_count reset"
        );
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
                drained: false,
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
                drained: false,
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
                drained: false,
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
                drained: false,
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
                drained: false,
            }],
            0,
            true,
            60_000,
        );
        assert!(h.take_batch("q", "g", 1, 100).is_empty());
        // ack releases the lease early → promote by id
        h.promote_ack("q", "g", "id0", 100, false);
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
                drained: false,
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

    // The push-path quiet mark still populates the ring (a woken pop finds the
    // candidate) but does NOT itself wake — the push handler's notify_pushed_batch
    // is the single wake. It still queues the mesh dirty hint (with peers). This is
    // the flag-on push-RTT fix: no redundant second per-push notify_waiters storm.
    #[test]
    fn quiet_mark_populates_ring_and_dirty_without_waking() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local_quiet("q", "p0", 1, 0);
        // ring is populated — a subsequent take (as a woken pop would do) sees it.
        assert_eq!(names(&h.take_batch("q", "g", 5, 0)), vec!["p0"]);
        // the mesh dirty hint is still enqueued (peers, if any, get it).
        assert_eq!(h.drain_dirty(10), vec![("q".to_string(), "p0".to_string())]);
    }

    // C1 wake coalescing: a push-path (quiet) mark flags its queue for the wake tick
    // instead of waking inline; the tick issues ONE wake per marked queue no matter
    // how many marks landed. mark→wake within a tick; no wake without a mark; a burst
    // of 10k marks coalesces to a single wake; the flag is consumed each tick.
    #[test]
    fn wake_tick_coalesces_marks_into_one_wake() {
        let h = hl1();
        reg(&h, "q", "g");
        // No mark since the last tick ⇒ nothing to wake.
        assert_eq!(h.wake_tick(), 0, "no mark ⇒ no wake");
        // A single quiet mark (new partition ⇒ IDLE→READY) flags the queue.
        h.mark_local_quiet("q", "p0", 1, 0);
        assert_eq!(h.wake_tick(), 1, "mark ⇒ exactly one wake within a tick");
        // The flag is consumed: a second tick with no new mark wakes nobody.
        assert_eq!(h.wake_tick(), 0, "flag consumed ⇒ no repeat wake");
        // A burst of 10k marks across distinct partitions still coalesces to ONE wake.
        for i in 0..10_000 {
            h.mark_local_quiet("q", &format!("b{i}"), 1, 0);
        }
        assert_eq!(h.wake_tick(), 1, "10k marks ⇒ one coalesced wake");
        assert_eq!(h.wake_tick(), 0);
    }

    // The immediate-wake marks (transaction commit via mark_local, mesh-remote via
    // mark_remote) wake AT ONCE and must NOT also flag the coalescing tick — the tick
    // is exclusively the per-push quiet path. (Wheel promotions and promote-on-ack
    // likewise wake immediately and never touch wake_pending.)
    #[test]
    fn immediate_wake_marks_do_not_flag_the_tick() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0); // wake=true ⇒ immediate, no tick flag
        h.mark_remote("q", "p1", 0); // wake=true ⇒ immediate, no tick flag
        assert_eq!(h.wake_tick(), 0, "immediate-wake marks must not flag the tick");
    }

    // With no peers configured, a local mark skips the dirty-hint set entirely (no
    // flusher would ever drain it) — no per-push lock/alloc on a single broker.
    #[test]
    fn no_peers_skips_dirty_set() {
        let h = HotList::new(true, 1, 100, false); // mesh_active = false
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        // still marks the ring (discovery works locally)…
        assert_eq!(names(&h.take_batch("q", "g", 5, 0)), vec!["p0"]);
        // …but nothing was queued for a mesh that isn't there.
        assert!(h.drain_dirty(10).is_empty());
    }

    #[test]
    fn remote_mark_makes_candidate_discoverable() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_remote("q", "p9", 0);
        assert_eq!(names(&h.take_batch("q", "g", 5, 0)), vec!["p9"]);
    }

    // Regression (P1 — periodic floor under load). A partition IDLE-cleared while it
    // still had backlog — the HA race: a peer leased it with an UNcommitted txn, so
    // our tri-state leased-probe read the pre-lease committed row (worker NULL) →
    // verdict `empty` → the epoch-CAS cleared it (no mark raced) — must be recovered
    // by the PERIODIC reseed, which fires even though the ring is NON-empty (other
    // partitions keep it busy, so the pop-path empty-ring reseed would never run).
    #[test]
    fn periodic_reseed_recovers_idle_cleared_partition_under_load() {
        let h = hl1();
        reg(&h, "q", "g");
        // Steady state: the group has completed at least one reseed at t=100
        // (last != 0; a t=0 stamp would read as "never reseeded" and be excluded).
        let base = 100i64;
        h.reseed_done("q", "g", base);
        h.mark_local("q", "p_keep", 1, base); // a continuously-written partition
        h.mark_local("q", "p_lost", 1, base); // has backlog, about to be falsely cleared
        let c = h.take_batch("q", "g", 2, base);
        assert_eq!(names(&c), vec!["p_keep", "p_lost"]);
        // p_keep took (→ ready), p_lost false-empty (→ IDLE-cleared). No mark raced,
        // so the epoch-CAS clears p_lost.
        h.checkin(
            "q",
            "g",
            vec![
                CheckinResult { name: "p_keep".into(), epoch: c[0].epoch, verdict: Verdict::Took, drained: false },
                CheckinResult { name: "p_lost".into(), epoch: c[1].epoch, verdict: Verdict::Empty, drained: false },
            ],
            base,
            true,
            60_000,
        );
        // The ring is NON-empty (p_keep is ready) ⇒ the pop-path empty-ring reseed
        // can NEVER trigger. ring_sizes confirms it (and exercises the observability
        // snapshot the VM log line uses).
        assert_eq!(h.ring_sizes()[0].2, 1, "ring must be non-empty (p_keep ready)");
        // Not due before the interval; due after interval + the max per-group jitter,
        // regardless of the busy ring.
        assert!(h.periodic_reseed_due(base + 1_000, 30_000).is_empty());
        let due_at = base + 30_000 + RESEED_JITTER_MS + 1;
        assert_eq!(
            h.periodic_reseed_due(due_at, 30_000),
            vec![("q".to_string(), "g".to_string())],
            "periodic floor must be due even with a non-empty ring"
        );
        // The background task runs the keyset scan: reseed_row for the still-pending
        // p_lost (exactly what log_hotlist_reseed_v1 returns: last_offset>committed).
        h.reseed_row("q", "g", "id_lost", "p_lost", due_at);
        h.reseed_done("q", "g", due_at);
        let got: HashSet<String> = names(&h.take_batch("q", "g", 5, due_at)).into_iter().collect();
        assert!(
            got.contains("p_lost"),
            "periodic reseed must recover the cleared partition; got {got:?}"
        );
    }

    // Regression (P3 — windowBuffer re-arm). On a pure windowBuffer queue, an
    // auto-ack Took must RE-ARM the debounce (re-enter the wheel at now+window), not
    // go straight to READY — otherwise every subsequent message is delivered
    // un-batched under continuous writes.
    #[test]
    fn window_took_rearms_the_wheel_not_ready() {
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 0, 5, 0); // windowBuffer 5s, no delayed
        h.mark_local("q", "p0", 10, 0); // small batch → wheel
        h.tick(5100); // window fires → ready
        let c = h.take_batch("q", "g", 1, 5100);
        assert_eq!(names(&c), vec!["p0"]);
        // auto-ack Took: must re-arm the window (wheel at 5100+5000), NOT go READY.
        h.checkin(
            "q",
            "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Took, drained: false }],
            5100,
            true,
            60_000,
        );
        assert!(
            h.take_batch("q", "g", 1, 5200).is_empty(),
            "windowBuffer must re-arm, not re-probe immediately"
        );
        // Claimable again only after the next window.
        h.tick(10_300);
        assert_eq!(names(&h.take_batch("q", "g", 1, 10_300)), vec!["p0"]);
    }

    // Guard: a NON-window queue's auto-ack Took still re-appends to READY at once
    // (the P3 re-arm is scoped to windowBuffer — no behavioural change elsewhere).
    #[test]
    fn non_window_took_still_goes_ready() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, 0);
        h.checkin(
            "q",
            "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Took, drained: false }],
            0,
            true,
            60_000,
        );
        assert_eq!(names(&h.take_batch("q", "g", 1, 0)), vec!["p0"], "no window ⇒ immediate READY");
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

    // Regression (cancellation strand): a candidate taken from the ring but never
    // checked back in (the pop future was dropped mid-dispatch) is stuck INFLIGHT.
    // A mark on it only bumps the epoch (no re-link), so it is invisible to every
    // pop — until the reseed floor RECLAIMS it. Without the reseed INFLIGHT-reclaim
    // this partition would be stranded permanently.
    #[test]
    fn leaked_inflight_is_reclaimed_by_reseed() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        // take_batch marks p0 INFLIGHT; we deliberately never checkin (leak).
        let c = h.take_batch("q", "g", 1, 0);
        assert_eq!(names(&c), vec!["p0"]);
        // Stuck: not claimable, and a fresh mark cannot re-link an INFLIGHT entry.
        assert!(h.take_batch("q", "g", 1, 0).is_empty());
        h.mark_local("q", "p0", 1, 0); // epoch bump only — still not in the ring
        assert!(h.take_batch("q", "g", 1, 0).is_empty(), "mark cannot re-add INFLIGHT");
        // The reseed floor re-adds the still-pending partition (last_offset>committed).
        h.reseed_row("q", "g", "id0", "p0", 0);
        h.reseed_done("q", "g", 0);
        assert_eq!(
            names(&h.take_batch("q", "g", 1, 0)),
            vec!["p0"],
            "reseed floor must reclaim a leaked INFLIGHT partition"
        );
    }

    // A reseed that re-adds a genuinely in-flight entry (concurrent real pop) is a
    // safe false positive: the entry is re-served (redundant), and the ORIGINAL
    // pop's checkin becomes a no-op because the state is no longer the INFLIGHT it
    // checked out — no double-link / no ring corruption.
    #[test]
    fn reseed_reclaim_then_original_checkin_is_noop() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, 0); // p0 INFLIGHT, epoch snapshot
        // reseed reclaims it back to READY while the "pop" is still in flight.
        h.reseed_row("q", "g", "id0", "p0", 0);
        h.reseed_done("q", "g", 0);
        // A second pop can now take it (redundant probe — allowed).
        assert_eq!(names(&h.take_batch("q", "g", 1, 0)), vec!["p0"]);
        // The original pop finally checks in Took: state is INFLIGHT (the 2nd pop's),
        // NOT the one it checked out — checkin acts on it once, no corruption. Then
        // the ring is consistent: exactly one entry, takeable after re-append.
        h.checkin(
            "q",
            "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Took, drained: false }],
            0,
            true,
            60_000,
        );
        // Whatever the interleaving, p0 appears at most once (no duplicate links).
        let got = names(&h.take_batch("q", "g", 5, 0));
        assert!(got == vec!["p0"] || got.is_empty(), "no duplicate ring entry: {got:?}");
    }

    // Retries-stall floor (2026-07-24): a manual-ack (leased) pop parks the entry in
    // the WHEEL at lease-expiry. If the lease is released EARLY (a NACK whose
    // promote-on-ack hook did not run), the entry is stranded until the ORIGINAL
    // expiry (up to leaseTime — 300s default). The reseed floor, which reports the
    // partition PENDING, must reclaim the stale wheel entry on a NON-deferral queue.
    #[test]
    fn reseed_recovers_stale_wheel_entry_on_nondeferral_queue() {
        let h = hl1();
        reg(&h, "q", "g");
        h.note_partition_id("q", "p0", "id0");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, 0);
        assert_eq!(names(&c), vec!["p0"]);
        // Leased Took → WHEEL at now + lease (300s) — the retries lease.
        h.checkin(
            "q",
            "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Took, drained: false }],
            0,
            false,
            300_000,
        );
        // Not claimable: parked in the wheel until the 300s lease expiry.
        assert!(h.take_batch("q", "g", 1, 1_000).is_empty());
        // The reseed floor reports p0 pending (last_offset > committed) → it MUST
        // reclaim the stale wheel entry instead of waiting 300s.
        h.reseed_row("q", "g", "id0", "p0", 1_000);
        assert_eq!(
            names(&h.take_batch("q", "g", 1, 1_000)),
            vec!["p0"],
            "reseed floor must reclaim a stale lease-parked wheel entry"
        );
    }

    // The converse: on a DEFERRAL queue a wheel entry is a visibility cut
    // (delayed/window), NOT a lease parking — the reseed must NOT promote it early.
    #[test]
    fn reseed_leaves_deferral_wheel_entry_until_due() {
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 0, 5, 0); // window_buffer = 5s ⇒ deferral queue
        h.note_partition_id("q", "p0", "id0");
        h.mark_local("q", "p0", 1, 0); // parked in the wheel at 5000ms (window)
        assert!(h.take_batch("q", "g", 1, 1_000).is_empty());
        // Reseed must leave the deferral deadline intact.
        h.reseed_row("q", "g", "id0", "p0", 1_000);
        assert!(
            h.take_batch("q", "g", 1, 1_000).is_empty(),
            "deferral wheel entry must stay parked until its window is due"
        );
        // Still delivered once the window elapses.
        assert_eq!(names(&h.take_batch("q", "g", 1, 6_000)), vec!["p0"]);
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

    // Delete-consumer-group-for-queue invalidation: forget_group drops the group's
    // ring so a stale post-delete entry can never mask the reconsume. After forget,
    // the ring is cold — reseed_due reports it due again (clock reset), and marks to
    // OTHER groups of the same queue are untouched.
    #[test]
    fn forget_group_drops_ring_and_resets_reseed_clock() {
        let h = hl1();
        reg(&h, "q", "g1");
        reg(&h, "q", "g2");
        h.reseed_done("q", "g1", 1000);
        h.reseed_done("q", "g2", 1000);
        // A stale ready entry that survived (a consumed partition the delete removes).
        h.mark_local("q", "p0", 1, 1000);
        assert!(h.has_ready("q", "g1", 1000));
        assert!(!h.reseed_due("q", "g1", 1500, 30_000), "recently reseeded");

        // Delete the group for this queue: the ring is forgotten wholesale.
        h.forget_group("q", "g1");
        // g1 is now cold: no stale ready entry, and a fresh reseed is due.
        assert!(!h.has_ready("q", "g1", 1500), "stale ring must not survive");
        assert!(h.reseed_due("q", "g1", 1500, 30_000), "forgotten ring reseeds cold");
        // g2 is untouched — the delete was scoped to g1.
        assert!(h.has_ready("q", "g2", 1500), "sibling group ring untouched");
        assert!(!h.reseed_due("q", "g2", 1500, 30_000));
    }

    // Delete-consumer-group (all queues) invalidation: forget_group_all_queues drops
    // the group's ring on EVERY queue it touched, leaving other groups alone.
    #[test]
    fn forget_group_all_queues_drops_every_queue_ring() {
        let h = hl1();
        reg(&h, "qa", "g");
        reg(&h, "qb", "g");
        reg(&h, "qa", "other");
        h.mark_local("qa", "p0", 1, 0);
        h.mark_local("qb", "p0", 1, 0);
        assert!(h.has_ready("qa", "g", 0));
        assert!(h.has_ready("qb", "g", 0));
        assert!(h.has_ready("qa", "other", 0));

        h.forget_group_all_queues("g");
        // The deleted group is cold on every queue…
        assert!(!h.has_ready("qa", "g", 0));
        assert!(!h.has_ready("qb", "g", 0));
        assert!(h.reseed_due("qa", "g", 0, 30_000));
        assert!(h.reseed_due("qb", "g", 0, 30_000));
        // …but a co-resident group on one of those queues is untouched.
        assert!(h.has_ready("qa", "other", 0), "co-resident group ring untouched");
    }

    // forget_* on a disabled hot-list, an unknown queue, or an unregistered group is
    // a safe no-op (never panics, never creates a ring).
    #[test]
    fn forget_group_is_safe_when_absent_or_disabled() {
        let off = HotList::new(false, 1, 100, true);
        off.forget_group("q", "g");
        off.forget_group_all_queues("g");

        let h = hl1();
        h.forget_group("never-seen", "g"); // unknown queue
        reg(&h, "q", "g");
        h.forget_group("q", "other"); // unregistered group in a known queue
        h.forget_group_all_queues("still-unknown");
        // The registered ring is still intact and usable.
        h.mark_local("q", "p0", 1, 0);
        assert_eq!(names(&h.take_batch("q", "g", 5, 0)), vec!["p0"]);
    }
}


pub(crate) fn trace_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
