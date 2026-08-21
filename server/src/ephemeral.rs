//! THE EPHEMERAL ENGINE — EPHEMERAL_QUEUES.md §3.2.
//!
//! A per-queue storage class whose contents live in this process's heap and
//! survive nothing: process exit (clean or crash), ownership movement, deploys.
//! The durable engine is not edited to make this work — not its handlers, not
//! its stored procedures, not its fusion — and that separation is enforced by
//! construction: **nothing in this file constructs SQL, takes a pooled
//! connection, or awaits anything at all.**
//!
//! ---------------------------------------------------------------------------
//! WHY EVERY METHOD HERE IS SYNCHRONOUS
//!
//! Not an oversight and not a simplification. The engine's locks are
//! `std::sync::Mutex`, and the one rule that makes them safe in an async
//! handler — never hold the guard across an `.await` — is a rule a reviewer has
//! to remember on every future edit. Making the whole surface `fn` instead of
//! `async fn` turns it into a property the compiler keeps: there is no `.await`
//! to hold a guard across, because there is nothing to await. The handler does
//! its parking OUTSIDE these calls, on the notifier (§3.4).
//!
//! ---------------------------------------------------------------------------
//! SINGLE-BROKER ONLY, DELIBERATELY (phase T1a)
//!
//! §3.7's rendezvous ownership, §3.6's forwarding and §3.5's mesh frames are a
//! LATER phase. Here self is always the owner of every (queue, partition), which
//! is exactly the short-circuit §3.7 already specifies for a broker with no
//! peers — the free-tier cell and the embedded `queen::Broker` run this path
//! unchanged when the rest lands. The one piece of the distributed design that
//! is present from day one is `eph_epoch` (M4): it costs nothing, it is in every
//! message id, and retrofitting it later would invalidate every id in flight.
//!
//! ---------------------------------------------------------------------------
//! ONE RING, N GROUP CURSORS (§1.5)
//!
//! There is no queue-level "mode". Consumption semantics come from the pop's
//! consumer group, exactly as on the durable engine:
//!
//!   * competing consumers = the same `group`, or NO group at all — the
//!     group-less queue mode, which mirrors the durable `__QUEUE_MODE__`
//!     sentinel by literally reusing the name, so "no group" is one shared
//!     cursor here for the same reason it is one shared cursor there;
//!   * fan-out = each subscriber pops with its OWN group, and every group has
//!     its own cursor over the one ring.
//!
//! A message therefore lives in the ring exactly once however many groups read
//! it, and is reclaimed when the last cursor has passed it.
//!
//! ---------------------------------------------------------------------------
//! WHAT THE KEYS ARE (M5, and the isolation rule of `handlers/mod.rs`)
//!
//! Every map key in this file is `tenant_queue_key(tenant, "eph:<name>")`. The
//! `eph:` prefix lives in the QUEUE half and never in the tenant half, because
//! the mesh splits the composite on the FIRST `\x1f` and rejoins on receive: a
//! prefix in the tenant half would not survive the round trip. A durable queue
//! literally named `eph:x` can only ever cause a cross-WAKE, which is a hint —
//! the woken side checks its own store and re-parks — so no name guard is
//! needed on the durable side (§10 Q8: same name on both engines is allowed
//! silently, they are unrelated objects).

//! ---------------------------------------------------------------------------
//! ON THE `#[allow(dead_code)]` MARKS BELOW
//!
//! They sit on exactly the items phase T1b consumes — the `configure` seam and
//! the two status reads — and on nothing else. The alternative (a module-wide
//! allow, or deleting them and writing them again next phase) would either hide
//! real dead code or split one design across two commits; the marks are removed
//! by the phase that gives each item its caller.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::metrics::Metrics;

/// The group-less cursor's name. The durable engine's sentinel, reused verbatim
/// rather than re-invented (`handlers/data.rs`: a pop with no `consumerGroup`
/// becomes `__QUEUE_MODE__`), so a reader who knows one engine's group-less
/// semantics knows the other's. It is a reserved name on both.
pub const QUEUE_MODE: &str = "__QUEUE_MODE__";

/// The queue half's namespace prefix. See the module header for why it is here
/// and not in the tenant half.
pub const EPH_PREFIX: &str = "eph:";

/// Partition a push or a pop means when it names none — the durable engine's
/// default, spelled the same way so the two classes are not gratuitously
/// different in the one place a user reads both.
pub const DEFAULT_PARTITION: &str = "Default";

// ===========================================================================
// Knobs and per-queue configuration
// ===========================================================================

/// Process-wide defaults, resolved once at boot from `Config` (§3.8).
#[derive(Clone, Copy, Debug)]
pub struct Knobs {
    /// `QUEEN_EPHEMERAL_MAX_BYTES`. The cell's ceiling; 0 = unlimited.
    pub global_max_bytes: i64,
    /// `QUEEN_EPHEMERAL_QUEUE_MAX_BYTES` — the per-queue default.
    pub queue_max_bytes: i64,
    /// `QUEEN_EPHEMERAL_QUEUE_MAX_LENGTH` — the per-queue default.
    pub queue_max_length: i64,
    /// `QUEEN_EPHEMERAL_LEASE_S`, in milliseconds by the time it gets here.
    pub lease_ms: i64,
    /// `QUEEN_EPHEMERAL_RETRY_LIMIT`.
    pub retry_limit: u32,
    /// `QUEEN_EPHEMERAL_IMPLICIT_IDLE_S`, in milliseconds.
    pub implicit_idle_ms: i64,
}

impl Knobs {
    /// The documented defaults, in ONE place, so the `Config` block and every
    /// test read the same numbers (§10 Q2: 256 MiB cell, 16 MiB / 10 000 queue).
    #[allow(dead_code)] // T1b: `configure` and the boot load read these too
    pub fn defaults() -> Knobs {
        Knobs {
            global_max_bytes: 256 * 1024 * 1024,
            queue_max_bytes: 16 * 1024 * 1024,
            queue_max_length: 10_000,
            lease_ms: 30_000,
            retry_limit: 5,
            implicit_idle_ms: 300_000,
        }
    }
}

/// What a full queue does to the next push (§1.6 rung 1).
#[allow(dead_code)] // DropOldest is only reachable through `configure` (T1b)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    /// 429 `queue_full` — the shape the 1.0.6 SDK backpressure already handles.
    Reject,
    /// Feed semantics: drop from the head until the arrival fits.
    DropOldest,
}

/// Delivery-side batch fattening (§1.7). Purely about when a WAITING pop
/// returns; it never holds a message back from a pop that is already full.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Window {
    pub ms: u64,
    pub count: usize,
}

impl Window {
    pub const OFF: Window = Window { ms: 0, count: 0 };
    #[inline]
    pub fn enabled(&self) -> bool {
        self.ms > 0 || self.count > 0
    }
}

/// The effective configuration of one queue: the knobs, plus whatever
/// `configure` overrode. Copy, so a hot path reads it out from under the lock
/// in one move instead of holding the lock while it works.
#[allow(dead_code)] // the two bound fields are read by `set_config` (T1b)
#[derive(Clone, Copy, Debug)]
pub struct QueueConfig {
    pub max_bytes: i64,
    pub max_length: i64,
    pub policy: Policy,
    /// 0 = no age limit. NOT called "retention": durable retention cleans
    /// consumed history and never touches pending, while this drops UNCONSUMED
    /// messages. One word per contract (the plan's naming rule).
    pub ttl_ms: i64,
    pub lease_ms: i64,
    pub retry_limit: u32,
    pub window: Window,
}

impl QueueConfig {
    pub fn from_knobs(k: &Knobs) -> QueueConfig {
        QueueConfig {
            max_bytes: k.queue_max_bytes,
            max_length: k.queue_max_length,
            policy: Policy::Reject,
            ttl_ms: 0,
            lease_ms: k.lease_ms,
            retry_limit: k.retry_limit,
            window: Window::OFF,
        }
    }
}

/// The overrides `configure` may set, all optional. Absent = keep the knob.
///
/// SEAM (phase T1b): `POST /api/v1/ephemeral/configure` and the boot load of
/// the declared configs both land here — this is the ONE place an option blob
/// becomes engine state, so the clamps below cannot be bypassed by adding a
/// second caller. The verb itself is not in this phase.
#[allow(dead_code)] // T1b: the `configure` verb and the declared-config boot load
#[derive(Clone, Copy, Debug, Default)]
pub struct QueueOptions {
    pub max_bytes: Option<i64>,
    pub max_length: Option<i64>,
    pub policy: Option<Policy>,
    pub ttl_ms: Option<i64>,
    pub lease_ms: Option<i64>,
    pub retry_limit: Option<u32>,
    pub window: Option<Window>,
}

impl QueueOptions {
    /// Apply over a base, CLAMPING every value against the broker's own knobs.
    ///
    /// The clamp is why 030_ephemeral.sql stores the options blob whole and
    /// validates nothing: the engine has to clamp anyway (an operator may lower
    /// `QUEEN_EPHEMERAL_QUEUE_MAX_BYTES` under queues that were declared when it
    /// was higher), so a CHECK constraint would be a second authority that
    /// drifts the first time that happens.
    #[allow(dead_code)] // T1b, with `set_config`
    pub fn apply(&self, base: QueueConfig, k: &Knobs) -> QueueConfig {
        let mut c = base;
        if let Some(v) = self.max_bytes {
            c.max_bytes = v.clamp(1, k.queue_max_bytes.max(1));
        }
        if let Some(v) = self.max_length {
            c.max_length = v.clamp(1, k.queue_max_length.max(1));
        }
        if let Some(v) = self.policy {
            c.policy = v;
        }
        if let Some(v) = self.ttl_ms {
            c.ttl_ms = v.max(0);
        }
        if let Some(v) = self.lease_ms {
            c.lease_ms = v.clamp(1, 24 * 3_600_000);
        }
        if let Some(v) = self.retry_limit {
            c.retry_limit = v.min(1_000);
        }
        if let Some(v) = self.window {
            c.window = Window { ms: v.ms.min(60_000), count: v.count.min(10_000) };
        }
        c
    }
}

// ===========================================================================
// Budget arithmetic — charge before append, refund on failure
// ===========================================================================

/// One capped counter with the `quota.rs:34-48` discipline: charge the delta
/// BEFORE the work, refund it the moment anything downstream refuses. The
/// alternative (append then check) is what lets a cell overshoot its ceiling by
/// a whole batch under concurrency, which for a RAM budget is not a rounding
/// error but the OOM.
///
/// `cap <= 0` means unlimited, and that is the value every rung whose limit has
/// not landed yet carries — so an unwired rung is inert rather than a denial.
#[derive(Debug)]
pub struct Budget {
    used: AtomicI64,
    cap: AtomicI64,
}

impl Budget {
    pub fn new(cap: i64) -> Budget {
        Budget { used: AtomicI64::new(0), cap: AtomicI64::new(cap) }
    }

    /// Reserve `n`. False (and nothing charged) when it would cross the cap.
    /// A CAS loop rather than a fetch_add-then-check, because the loser of the
    /// race must not observe its own over-cap value as if it had been granted.
    pub fn charge(&self, n: i64) -> bool {
        if n <= 0 {
            return true;
        }
        let cap = self.cap.load(Ordering::Relaxed);
        loop {
            let cur = self.used.load(Ordering::Relaxed);
            if cap > 0 && cur + n > cap {
                return false;
            }
            if self
                .used
                .compare_exchange_weak(cur, cur + n, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Give `n` back. Saturating at zero: a double refund is a bug, but a
    /// NEGATIVE occupancy would silently grant unlimited room to everyone, so
    /// the failure mode is clamped to "the counter is a little high".
    pub fn refund(&self, n: i64) {
        if n <= 0 {
            return;
        }
        let _ = self.used.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
            Some((cur - n).max(0))
        });
    }

    #[inline]
    pub fn used(&self) -> i64 {
        self.used.load(Ordering::Relaxed)
    }

    #[allow(dead_code)] // T1c: the grant refresh reads and rewrites the cap
    #[inline]
    pub fn cap(&self) -> i64 {
        self.cap.load(Ordering::Relaxed)
    }

    /// Raise or lower the ceiling at runtime (a grant refresh). Lowering it
    /// below current occupancy is legal and blocks the next arrival rather than
    /// evicting anything: this budget bounds admission, never contents.
    #[allow(dead_code)] // T1b/T1c: `configure` and the grant refresh
    pub fn set_cap(&self, cap: i64) {
        self.cap.store(cap, Ordering::Relaxed);
    }
}

// ===========================================================================
// Message ids
// ===========================================================================

/// `e:<epoch_hex>:<partition>:<seq>` — opaque to clients, self-describing to
/// the broker (§3.1).
///
/// The epoch is what makes an ack SAFE across a restart or an ownership move:
/// contents die with the incarnation, so an id minted by a previous one names
/// nothing here, and the honest answer is `stale` rather than an error or —
/// far worse — a collision with a seq this incarnation has since reused.
pub fn mint_id(epoch: u64, partition: &str, seq: u64) -> String {
    format!("e:{epoch:x}:{partition}:{seq}")
}

/// Inverse of [`mint_id`]. `None` on anything that is not one of our ids.
///
/// Parsed from BOTH ends, and that is load-bearing: a partition name may contain
/// `:` (the durable engine forbids only control characters), so a left-to-right
/// `split(':')` would truncate `orders:eu` and mis-attribute the ack. The epoch
/// is the first field and the seq is the last; whatever is between them is the
/// partition, colons and all.
pub fn parse_id(id: &str) -> Option<(u64, &str, u64)> {
    let rest = id.strip_prefix("e:")?;
    let (epoch_hex, rest) = rest.split_once(':')?;
    let (partition, seq) = rest.rsplit_once(':')?;
    if partition.is_empty() {
        return None;
    }
    let epoch = u64::from_str_radix(epoch_hex, 16).ok()?;
    let seq = seq.parse::<u64>().ok()?;
    Some((epoch, partition, seq))
}

// ===========================================================================
// The ring
// ===========================================================================

struct Msg {
    seq: u64,
    /// Copied OUT of the request buffer at push. A 1 KB slice of a 64 MiB body
    /// must not pin the whole body allocation for the life of the message —
    /// with thousands of live inboxes that is the difference between a few MB
    /// and the cell.
    payload: Box<[u8]>,
    enqueued_ms: i64,
}

/// One consumer group's view of the ring.
struct GroupState {
    /// The next seq this group has not yet been HANDED. Advances at delivery,
    /// for both ack modes: the lease (not the cursor) is what holds an
    /// unacknowledged message, which is what lets two members of the same group
    /// take disjoint messages concurrently.
    next: u64,
    /// Seqs whose lease expired or whose ack said `failed`/`retry`. Served
    /// BEFORE the cursor, so a redelivery jumps the queue rather than waiting
    /// behind everything pushed since.
    redeliver: VecDeque<u64>,
    /// Delivery count per outstanding seq. Only entries in flight or awaiting
    /// redelivery live here, so it is bounded by this group's in-flight set.
    attempts: HashMap<u64, u32>,
    /// Messages this group never saw because bounds or ttl evicted them under
    /// it (§3.2). Counted per group and never silently swallowed: for a fan-out
    /// consumer this is the difference between "slow" and "lost data".
    skipped: u64,
}

impl GroupState {
    fn new(next: u64) -> GroupState {
        GroupState {
            next,
            redeliver: VecDeque::new(),
            attempts: HashMap::new(),
            skipped: 0,
        }
    }

    /// The lowest seq this group could still be handed. `u64::MAX` when it is
    /// caught up with nothing outstanding.
    fn low_water(&self, leased: Option<u64>) -> u64 {
        let mut lo = self.next;
        if let Some(&s) = self.redeliver.iter().min() {
            lo = lo.min(s);
        }
        if let Some(s) = leased {
            lo = lo.min(s);
        }
        lo
    }
}

struct LeaseRec {
    gix: u32,
    seq: u64,
}

/// What a ring operation freed, so the caller can refund the budgets it does
/// not own. The ring deliberately knows nothing about the queue, tenant or cell
/// counters: it holds a lock, and a refund path that reached back up through
/// the queue map from under that lock would be the one lock-order inversion in
/// the engine.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct Freed {
    pub bytes: i64,
    pub count: i64,
    pub by_ttl: u64,
    pub by_bounds: u64,
}

impl Freed {
    fn merge(&mut self, o: Freed) {
        self.bytes += o.bytes;
        self.count += o.count;
        self.by_ttl += o.by_ttl;
        self.by_bounds += o.by_bounds;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DropReason {
    Ttl,
    Bounds,
}

struct Ring {
    deque: VecDeque<Msg>,
    /// Seq of `deque.front()`. Kept explicitly so index arithmetic works on an
    /// empty ring too (`head_seq == next_seq` when empty).
    head_seq: u64,
    next_seq: u64,
    groups: Vec<GroupState>,
    gix: HashMap<String, u32>,
    /// Expiry-ordered, which is the ONLY order a sweep needs: `range(..=now)`
    /// is the whole expired set and the map is never scanned whole.
    /// The `u64` breaks deadline ties and gives every lease a stable identity.
    leases: BTreeMap<(i64, u64), LeaseRec>,
    /// (group index, seq) -> the lease key, so an ack is one hash lookup rather
    /// than a walk of the deadline map.
    lease_at: HashMap<(u32, u64), (i64, u64)>,
    next_lease_id: u64,
    bytes: i64,
}

impl Ring {
    /// `start` is the seq this incarnation of the ring begins numbering at —
    /// never 0 after the first, see `Ephemeral::next_ring_base`.
    fn new(start: u64) -> Ring {
        Ring {
            deque: VecDeque::new(),
            head_seq: start,
            next_seq: start,
            groups: Vec::new(),
            gix: HashMap::new(),
            leases: BTreeMap::new(),
            lease_at: HashMap::new(),
            next_lease_id: 0,
            bytes: 0,
        }
    }

    fn group(&mut self, name: &str) -> u32 {
        if let Some(&i) = self.gix.get(name) {
            return i;
        }
        // A brand-new group starts at the ring's HEAD, not at its tail: the
        // messages still in RAM are the only history that exists, and there is
        // no `subscriptionMode` here to choose otherwise (§1.2 — no replay, no
        // history, so the concept has no referent). Starting at the tail would
        // make a fan-out subscriber miss everything already buffered for it.
        let i = self.groups.len() as u32;
        self.groups.push(GroupState::new(self.head_seq));
        self.gix.insert(name.to_string(), i);
        i
    }

    #[inline]
    fn get(&self, seq: u64) -> Option<&Msg> {
        if seq < self.head_seq {
            return None;
        }
        self.deque.get((seq - self.head_seq) as usize)
    }

    /// Retire the head unconditionally, advancing every cursor that had not
    /// reached it and counting the skip (§3.2). Returns `None` on an empty ring,
    /// which is how the eviction loops terminate.
    fn evict_head(&mut self, reason: DropReason) -> Option<Freed> {
        let m = self.deque.pop_front()?;
        let seq = m.seq;
        let bytes = m.payload.len() as i64;
        self.bytes -= bytes;
        self.head_seq = seq + 1;
        for g in self.groups.iter_mut() {
            if g.next <= seq {
                g.next = seq + 1;
                g.skipped += 1;
            }
            g.redeliver.retain(|&s| s != seq);
            g.attempts.remove(&seq);
        }
        // A leased message that is evicted under its holder cannot be
        // redelivered and its ack can no longer resolve — dropping the lease
        // here is what keeps the deadline map bounded by LIVE messages, and it
        // is why a late ack for it answers `unknown` rather than resurrecting a
        // seq the ring has reused for nothing.
        let doomed: Vec<(i64, u64)> = self
            .leases
            .iter()
            .filter(|(_, r)| r.seq == seq)
            .map(|(k, _)| *k)
            .collect();
        for k in doomed {
            if let Some(r) = self.leases.remove(&k) {
                self.lease_at.remove(&(r.gix, r.seq));
            }
        }
        Some(Freed {
            bytes,
            count: 1,
            by_ttl: (reason == DropReason::Ttl) as u64,
            by_bounds: (reason == DropReason::Bounds) as u64,
        })
    }

    /// §1.7 — head-drop of everything older than `ttl_ms`. The ring is in seq
    /// (= arrival) order, so the expired set is always a PREFIX and this is a
    /// bounded walk from the head, never a scan.
    fn drop_expired(&mut self, now_ms: i64, ttl_ms: i64) -> Freed {
        let mut out = Freed::default();
        if ttl_ms <= 0 {
            return out;
        }
        while let Some(m) = self.deque.front() {
            if now_ms - m.enqueued_ms < ttl_ms {
                break;
            }
            match self.evict_head(DropReason::Ttl) {
                Some(f) => out.merge(f),
                None => break,
            }
        }
        out
    }

    /// The memory the ring may hand back because every group is past it. This is
    /// the NORMAL reclamation path — bounds and ttl are the abnormal ones — and
    /// it is what makes a competing-consumer queue with one group behave like a
    /// plain FIFO whose memory tracks its backlog and not its history.
    ///
    /// With NO groups at all nothing is reclaimed: a queue that has been pushed
    /// to but never popped is holding messages FOR a consumer that has not
    /// arrived yet, and its bound is `maxBytes`/`ttlSeconds`, not this.
    fn trim_consumed(&mut self) -> Freed {
        let mut out = Freed::default();
        if self.groups.is_empty() {
            return out;
        }
        let mut floor = u64::MAX;
        for (i, g) in self.groups.iter().enumerate() {
            let leased = self
                .lease_at
                .keys()
                .filter(|(gi, _)| *gi == i as u32)
                .map(|(_, s)| *s)
                .min();
            floor = floor.min(g.low_water(leased));
        }
        while let Some((seq, bytes)) =
            self.deque.front().map(|m| (m.seq, m.payload.len() as i64))
        {
            if seq >= floor {
                break;
            }
            self.deque.pop_front();
            self.bytes -= bytes;
            self.head_seq = self.head_seq.max(seq + 1);
            out.bytes += bytes;
            out.count += 1;
        }
        out
    }

    /// §3.2/M10 — the opportunistic expiry sweep every ring touch runs.
    ///
    /// Expired leases become redeliveries with `attempts + 1`; a group that has
    /// burned `retry_limit` attempts on a seq gives it up (counted as
    /// `eph_dropped_retry`) WITHOUT touching the message, because on a fan-out
    /// ring another group may still be owed it. "Dropped" is therefore always
    /// per-group; the byte is reclaimed by `trim_consumed` once the last group
    /// has given up or moved past.
    fn sweep_leases(&mut self, now_ms: i64, retry_limit: u32) -> LeaseSweep {
        let mut out = LeaseSweep::default();
        let due: Vec<(i64, u64)> = self
            .leases
            .range(..=(now_ms, u64::MAX))
            .map(|(k, _)| *k)
            .collect();
        for k in due {
            let Some(rec) = self.leases.remove(&k) else { continue };
            self.lease_at.remove(&(rec.gix, rec.seq));
            let limit = retry_limit;
            let g = &mut self.groups[rec.gix as usize];
            let n = g.attempts.entry(rec.seq).or_insert(0);
            if *n >= limit {
                g.attempts.remove(&rec.seq);
                out.exhausted += 1;
            } else {
                g.redeliver.push_back(rec.seq);
                out.redelivered += 1;
            }
        }
        // Keep each group's redelivery queue in seq order: two leases that
        // expire in the same sweep arrive here in DEADLINE order, which for
        // renewed or out-of-order acks is not arrival order, and §1.4 promises
        // FIFO per (queue, partition).
        if out.redelivered > 0 {
            for g in self.groups.iter_mut() {
                if g.redeliver.len() > 1 {
                    let mut v: Vec<u64> = g.redeliver.drain(..).collect();
                    v.sort_unstable();
                    v.dedup();
                    g.redeliver = v.into();
                }
            }
        }
        out
    }

    /// The nearest lease deadline, for the sweeper's `hint_in_ms` (§3.2).
    fn next_expiry(&self) -> Option<i64> {
        self.leases.keys().next().map(|(d, _)| *d)
    }
}

#[derive(Default, Debug, PartialEq, Eq)]
struct LeaseSweep {
    redelivered: u64,
    exhausted: u64,
}

// ===========================================================================
// Queues
// ===========================================================================

struct EqQueue {
    /// Bare name, without the `eph:` prefix — what the wire says and what a log
    /// line should print.
    #[allow(dead_code)] // read by `name_of` (T1b status verbs)
    name: String,
    config: Mutex<QueueConfig>,
    partitions: Mutex<HashMap<String, Arc<Mutex<Ring>>>>,
    /// Per-QUEUE occupancy (§1.6 rung 1), summed across partitions.
    bytes: Budget,
    length: Budget,
    /// Every group name this queue has ever been popped with, so a ring created
    /// LATER is born with all of their cursors at its head.
    ///
    /// This is what makes fan-out survive the parking race, and it is the whole
    /// reason the set exists. Without it: two subscribers park on an empty
    /// inbox, a push creates the ring and wakes both, the faster one takes the
    /// message and the reclaim frees it before the slower one has registered a
    /// cursor — so a fan-out consumer misses a message for being 50 µs late.
    /// A group's cursor cannot be created before the ring is, but its NAME can,
    /// and the name is all the seeding needs.
    known_groups: Mutex<std::collections::HashSet<String>>,
    /// False = implicit (§1.1 tier 1), and therefore garbage-collectable.
    declared: AtomicBool,
    last_touch_ms: AtomicI64,
}

/// Bound on `known_groups`. Fan-out cardinality is small by nature (one group
/// per subscriber ROLE, not per subscriber), and the name comes from the client,
/// so an unbounded set is a per-queue memory pin anyone can grow. Past the cap
/// new names simply are not remembered: they still get a cursor the moment they
/// pop a live ring, which is the pre-seeding behaviour.
const KNOWN_GROUPS_CAP: usize = 256;

impl EqQueue {
    /// The partition's ring iff it exists. Split from `ring_or_create` rather
    /// than taking a `create: bool`, because creating one now costs a seq base
    /// (below) and a flag would make every read-only caller look like it might
    /// spend one.
    fn ring(&self, partition: &str) -> Option<Arc<Mutex<Ring>>> {
        self.partitions.lock().unwrap().get(partition).cloned()
    }

    /// Get-or-create. `base` is called ONLY on the create path.
    fn ring_or_create(
        &self,
        partition: &str,
        base: impl FnOnce() -> u64,
    ) -> Arc<Mutex<Ring>> {
        if let Some(r) = self.ring(partition) {
            return r;
        }
        // Snapshot the names OUTSIDE the partitions lock: `known_groups` is
        // taken by pop on a path that already holds nothing, and taking the two
        // in a fixed order here (known_groups, then partitions) keeps the pair
        // acyclic without anyone having to remember which.
        let seed: Vec<String> = self.known_groups.lock().unwrap().iter().cloned().collect();
        let start = base();
        let mut p = self.partitions.lock().unwrap();
        p.entry(partition.to_string())
            .or_insert_with(|| {
                let mut r = Ring::new(start);
                for g in &seed {
                    r.group(g);
                }
                Arc::new(Mutex::new(r))
            })
            .clone()
    }

    /// Remember a group name for the rings this queue does not have yet.
    /// Returns nothing: a name past the cap is dropped silently, because the
    /// only visible consequence is the pre-seeding behaviour it used to have.
    fn remember_group(&self, group: &str) {
        let mut g = self.known_groups.lock().unwrap();
        if g.contains(group) || g.len() >= KNOWN_GROUPS_CAP {
            return;
        }
        g.insert(group.to_string());
    }

    fn partition_names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.partitions.lock().unwrap().keys().cloned().collect();
        // Sorted, so a partition-less pop visits partitions in an order that
        // does not depend on hash iteration: a batch assembled differently on
        // every call is untestable and, worse, starves whichever partition the
        // hasher happens to visit last under a small batch.
        v.sort_unstable();
        v
    }

    #[inline]
    fn touch(&self, now_ms: i64) {
        self.last_touch_ms.store(now_ms, Ordering::Relaxed);
    }
}

/// Per-tenant occupancy (§1.6 rung 2). The cap is `Budget::cap`, which is 0 —
/// unlimited — until the grant ladder lands (T1c): the MEASUREMENT is wired
/// from day one so the byte accounting is already correct and tested when the
/// limit is switched on, which is the half that is hard to retrofit.
struct TenantUsage {
    bytes: Budget,
    queues: AtomicI64,
}

// ===========================================================================
// The engine
// ===========================================================================

/// A refusal that has to become an HTTP status. Deliberately NOT rendered here:
/// the handler owns the `{error, code}` envelope, and an engine that returned
/// `Response` could not be unit-tested without an HTTP stack.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// §1.6 rung 1, `policy = reject`. 429 `queue_full`.
    QueueFull,
    /// §1.6 rung 3, the cell ceiling. 503 `ephemeral_unavailable`.
    NoRoom,
    /// §1.6 rung 2. Unreachable until the grant ladder lands; the variant exists
    /// so the handler's match is already exhaustive over the real taxonomy.
    TenantQuota,
}

/// One message on its way out.
pub struct Delivered {
    pub id: String,
    pub partition: String,
    pub payload: Box<[u8]>,
    pub attempts: u32,
}

/// What an ack said about one id (§1.3). `dlq` is deliberately absent: there is
/// no DLQ in this class (§9), so accepting the word would promise storage that
/// does not exist.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckStatus {
    Completed,
    Failed,
    Retry,
}

impl AckStatus {
    /// Absent status = completed, the durable wire's rule
    /// (`handlers::normalize_ack_status`). Anything unrecognized is a nack, for
    /// the same reason it is there: a typo must not silently retire a message.
    pub fn parse(s: Option<&str>) -> AckStatus {
        match s {
            None => AckStatus::Completed,
            Some("completed" | "success" | "acked" | "ok") => AckStatus::Completed,
            Some("retry") => AckStatus::Retry,
            Some(_) => AckStatus::Failed,
        }
    }
}

/// The per-id answer of §3.1. A CLOSED vocabulary of four, and every terminal
/// outcome is `Acked`:
///
///   * `Acked`       — the ack was applied and the message is retired. That
///                     includes a `failed` whose attempts were already spent:
///                     the message is gone and `eph_dropped_retry` is where
///                     that is visible, because inventing a fifth outcome would
///                     break every client that matches this enum.
///   * `Redelivered` — the ack was applied and the message comes back.
///   * `Stale`       — the id was minted by another incarnation (§3.1). Never
///                     an error: this is the fencing that survives restarts.
///   * `Unknown`     — our epoch, but no live lease: already acked, already
///                     expired and redelivered to someone else, or evicted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckOutcome {
    Acked,
    Redelivered,
    Stale,
    Unknown,
}

impl AckOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            AckOutcome::Acked => "acked",
            AckOutcome::Redelivered => "redelivered",
            AckOutcome::Stale => "stale",
            AckOutcome::Unknown => "unknown",
        }
    }
}

/// What one sweeper pass did, for its log line and its next-wake hint.
#[derive(Default, Debug)]
pub struct Sweep {
    pub redelivered: u64,
    pub exhausted: u64,
    pub dropped_ttl: u64,
    pub gc_queues: u64,
    /// Epoch ms of the nearest known lease expiry, for `sweeper::hint_in_ms`.
    pub next_expiry_ms: Option<i64>,
}

pub struct Ephemeral {
    queues: Mutex<HashMap<String, Arc<EqQueue>>>,
    global: Budget,
    tenants: Mutex<HashMap<String, TenantUsage>>,
    epoch: u64,
    /// Next seq base to hand a freshly created ring — see [`next_ring_base`].
    ring_seq_base: std::sync::atomic::AtomicU64,
    knobs: Knobs,
    metrics: Arc<Metrics>,
    /// `sweeper::hint_in_ms`, injected rather than called directly.
    ///
    /// `sweeper` is declared in `main.rs` and NOT in `lib.rs` — the embedded
    /// broker has no sweeper at all — so a direct call would not compile for the
    /// library target. Injecting it also states the truth: the hint is a latency
    /// optimization for a loop that may not exist, and the on-touch sweep is the
    /// correctness floor either way. Same shape as `Notifier::attach_transport`.
    wake_hint: OnceLock<fn(i64)>,
}

impl Ephemeral {
    pub fn new(knobs: Knobs, metrics: Arc<Metrics>) -> Arc<Ephemeral> {
        Arc::new(Ephemeral {
            queues: Mutex::new(HashMap::new()),
            global: Budget::new(knobs.global_max_bytes),
            tenants: Mutex::new(HashMap::new()),
            // M4 — the boot incarnation id. Random and not derived from
            // `server_id` or the pid: two brokers on one host, and one broker
            // restarted into the same pid, must be distinguishable, and both
            // `lease::holder_id` and the maintenance fence get that wrong for
            // this purpose.
            epoch: rand::random::<u64>(),
            ring_seq_base: std::sync::atomic::AtomicU64::new(0),
            knobs,
            metrics,
            wake_hint: OnceLock::new(),
        })
    }

    /// Wire the sweeper's waker. Only the first call wins; safe to never call.
    pub fn attach_wake_hint(&self, f: fn(i64)) {
        let _ = self.wake_hint.set(f);
    }

    #[inline]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    #[allow(dead_code)] // T1b: `configure` validates against them
    #[inline]
    pub fn knobs(&self) -> &Knobs {
        &self.knobs
    }

    #[inline]
    pub fn global_bytes(&self) -> i64 {
        self.global.used()
    }

    pub fn queue_count(&self) -> usize {
        self.queues.lock().unwrap().len()
    }

    /// The composite key of one ephemeral queue. EVERY map in this engine is
    /// keyed by this and nothing else (M5); a bare name would let one tenant
    /// read another's inbox on a shared cell.
    pub fn qkey(tenant: &str, name: &str) -> String {
        crate::handlers::tenant_queue_key(tenant, &format!("{EPH_PREFIX}{name}"))
    }

    /// The seq a NEWLY CREATED ring starts numbering at, so that no two
    /// incarnations of the same (queue, partition) inside one broker
    /// incarnation ever mint the same id.
    ///
    /// WHY THIS IS NOT ZERO, which is the obvious answer and is wrong. The
    /// epoch (§3.1) fences ids across a RESTART, but an implicit queue is also
    /// garbage-collected while the process lives (§1.1): idle, empty, gone —
    /// and the next push recreates it. With rings starting at 0 every time, an
    /// ack a client buffered before the collection would name `…:Default:0`,
    /// which the recreated ring has just handed to somebody else, and a
    /// `completed` would retire a message nobody processed. That is silent
    /// message loss, not the duplication-or-loss §1.4 declares legal.
    ///
    /// The stride is per RING INCARNATION and the counter is monotone, so the
    /// hazard needs an old ring to have consumed a whole stride — 268 million
    /// messages in one (queue, partition) incarnation — before the collision is
    /// even representable. Ids are opaque to clients (§3.1), so paying for it
    /// with large numbers costs nothing.
    fn next_ring_base(&self) -> u64 {
        const RING_SEQ_STRIDE: u64 = 1 << 28;
        self.ring_seq_base.fetch_add(RING_SEQ_STRIDE, Ordering::Relaxed)
    }

    fn hint(&self, delay_ms: i64) {
        if let Some(f) = self.wake_hint.get() {
            f(delay_ms);
        }
    }

    // -------------------------------------------------------------- queues

    fn lookup(&self, tenant: &str, name: &str, create: bool) -> Option<Arc<EqQueue>> {
        let key = Self::qkey(tenant, name);
        {
            let q = self.queues.lock().unwrap();
            if let Some(x) = q.get(&key) {
                return Some(x.clone());
            }
        }
        if !create {
            return None;
        }
        let fresh = Arc::new(EqQueue {
            name: name.to_string(),
            config: Mutex::new(QueueConfig::from_knobs(&self.knobs)),
            partitions: Mutex::new(HashMap::new()),
            bytes: Budget::new(self.knobs.queue_max_bytes),
            length: Budget::new(self.knobs.queue_max_length),
            known_groups: Mutex::new(std::collections::HashSet::new()),
            declared: AtomicBool::new(false),
            last_touch_ms: AtomicI64::new(crate::util::now_epoch_ms()),
        });
        let mut q = self.queues.lock().unwrap();
        let created = !q.contains_key(&key);
        let arc = q.entry(key).or_insert(fresh).clone();
        let n = q.len() as i64;
        drop(q);
        // Only on a real creation: two racing pushes to the same new inbox must
        // not count it twice, which for the per-tenant `max_queues` grant would
        // be a refusal the tenant cannot explain.
        if created {
            self.metrics.eph_queues.store(n, Ordering::Relaxed);
            self.with_tenant(tenant, |t| {
                t.queues.fetch_add(1, Ordering::Relaxed);
            });
        }
        Some(arc)
    }

    fn with_tenant<R>(&self, tenant: &str, f: impl FnOnce(&TenantUsage) -> R) -> R {
        let mut m = self.tenants.lock().unwrap();
        let e = m.entry(tenant.to_string()).or_insert_with(|| TenantUsage {
            // 0 = unlimited. HOOK (T1c): the grant refresh sets this from
            // queen.ephemeral_quota.max_bytes; until then the rung is inert
            // rather than a denial, so an OSS cell is never gated by a table
            // nobody writes.
            bytes: Budget::new(0),
            queues: AtomicI64::new(0),
        });
        f(e)
    }

    /// SEAM (T1b): `configure` and the boot load of declared configs. Creating
    /// the queue here is what makes a declared queue exist "as configured but
    /// empty" across a restart (§1.2).
    #[allow(dead_code)] // T1b: the `configure` verb and the boot load
    pub fn set_config(&self, tenant: &str, name: &str, opts: QueueOptions, declared: bool) {
        let Some(q) = self.lookup(tenant, name, true) else { return };
        let base = QueueConfig::from_knobs(&self.knobs);
        let cfg = opts.apply(base, &self.knobs);
        *q.config.lock().unwrap() = cfg;
        q.bytes.set_cap(cfg.max_bytes);
        q.length.set_cap(cfg.max_length);
        if declared {
            q.declared.store(true, Ordering::Relaxed);
        }
    }

    /// The queue's delivery-shaping window, for the pop handler's wait loop.
    /// Default (off) for a queue that does not exist yet — a pop on an unknown
    /// queue must park like any other, not fail.
    pub fn window(&self, tenant: &str, name: &str) -> Window {
        match self.lookup(tenant, name, false) {
            Some(q) => q.config.lock().unwrap().window,
            None => Window::OFF,
        }
    }

    // ---------------------------------------------------------------- push

    /// Append `payloads` to one (queue, partition). ALL-OR-NOTHING per call
    /// (§3.1): the budgets are charged for the whole batch up front, so a
    /// request either lands entirely or changes nothing — a partial push would
    /// make the SDK's front-of-buffer retry (M13) duplicate whatever landed.
    ///
    /// The queue is auto-vivified (§1.1 tier 1). That is not a convenience: a
    /// req/reply inbox is a per-client queue with a lifetime of seconds, and a
    /// declaration step would put a round trip in front of every one of them.
    pub fn push(
        &self,
        tenant: &str,
        name: &str,
        partition: &str,
        payloads: Vec<Box<[u8]>>,
        now_ms: i64,
    ) -> Result<usize, Refusal> {
        if payloads.is_empty() {
            return Ok(0);
        }
        let Some(q) = self.lookup(tenant, name, true) else { return Err(Refusal::NoRoom) };
        let cfg = *q.config.lock().unwrap();
        let ring = q.ring_or_create(partition, || self.next_ring_base());
        q.touch(now_ms);

        let total: i64 = payloads.iter().map(|p| p.len() as i64).sum();
        let count = payloads.len() as i64;

        // Opportunistic maintenance BEFORE the charge, never after: room a ttl
        // drop or a consumed-prefix trim is about to free must be visible to
        // this arrival, or a queue at its ceiling refuses a push it had space
        // for and the operator sees `queue_full` on a queue that is empty.
        let mut freed = Freed::default();
        {
            let mut r = ring.lock().unwrap();
            let sweep = r.sweep_leases(now_ms, cfg.retry_limit);
            if sweep.exhausted > 0 {
                self.metrics.eph_dropped_retry.fetch_add(sweep.exhausted, Ordering::Relaxed);
            }
            freed.merge(r.drop_expired(now_ms, cfg.ttl_ms));
            freed.merge(r.trim_consumed());
        }
        self.release(tenant, &q, &freed);

        // §1.6, in order: queue -> tenant -> cell. Rung 1 is the only one with a
        // POLICY, because it is the only one whose pressure the queue's owner
        // chose; the other two are conditions, and a condition cannot be
        // resolved by throwing away somebody else's data.
        if !self.charge_queue(&q, cfg.policy, &ring, total, count, tenant) {
            return Err(Refusal::QueueFull);
        }
        let tenant_ok = self.with_tenant(tenant, |t| t.bytes.charge(total));
        if !tenant_ok {
            q.bytes.refund(total);
            q.length.refund(count);
            return Err(Refusal::TenantQuota);
        }
        if !self.global.charge(total) {
            q.bytes.refund(total);
            q.length.refund(count);
            self.with_tenant(tenant, |t| t.bytes.refund(total));
            return Err(Refusal::NoRoom);
        }

        {
            let mut r = ring.lock().unwrap();
            for p in payloads {
                let seq = r.next_seq;
                r.next_seq += 1;
                if r.deque.is_empty() {
                    r.head_seq = seq;
                }
                r.bytes += p.len() as i64;
                r.deque.push_back(Msg { seq, payload: p, enqueued_ms: now_ms });
            }
        }
        self.metrics.eph_pushed.fetch_add(count as u64, Ordering::Relaxed);
        self.metrics.eph_bytes.store(self.global.used(), Ordering::Relaxed);
        // A ttl'd queue nobody polls still has to shed: the sweeper is what
        // runs the drop, and this is how it learns there is anything to run for.
        if cfg.ttl_ms > 0 {
            self.hint(cfg.ttl_ms);
        }
        Ok(count as usize)
    }

    /// Rung 1, including the `dropOldest` loop.
    ///
    /// The loop is "charge, and on refusal evict exactly one head and try
    /// again", which terminates because every eviction shrinks the ring. It
    /// deliberately does NOT compute how much to free up front: two partitions
    /// of the same queue can be charging concurrently, so any pre-computed
    /// amount is stale by the time it is used, while the retry loop is correct
    /// under any interleaving.
    ///
    /// DECLARED LIMIT of `dropOldest` on a multi-partition queue: the bound is
    /// per-QUEUE (§1.6) but eviction can only reach the partition being pushed
    /// to — reaching into a sibling ring would mean holding two ring locks and
    /// is the one lock-order inversion this engine refuses to have. When the
    /// target ring empties and the queue is still full (its bytes are held by
    /// other partitions), `dropOldest` degrades to `reject`. For the workloads
    /// this class exists for (one partition per inbox) the case does not arise.
    fn charge_queue(
        &self,
        q: &Arc<EqQueue>,
        policy: Policy,
        ring: &Arc<Mutex<Ring>>,
        total: i64,
        count: i64,
        tenant: &str,
    ) -> bool {
        loop {
            if q.bytes.charge(total) {
                if q.length.charge(count) {
                    return true;
                }
                q.bytes.refund(total);
            }
            if policy == Policy::Reject {
                return false;
            }
            let freed = {
                let mut r = ring.lock().unwrap();
                r.evict_head(DropReason::Bounds)
            };
            match freed {
                Some(f) => self.release(tenant, q, &f),
                None => return false, // nothing left to give up
            }
        }
    }

    /// Give freed bytes back to all three budgets and count the drops. ONE
    /// place, because a refund that reached two of the three levels is an
    /// invisible leak that only shows up as a cell that slowly stops accepting.
    fn release(&self, tenant: &str, q: &Arc<EqQueue>, f: &Freed) {
        if f.count == 0 && f.bytes == 0 {
            return;
        }
        q.bytes.refund(f.bytes);
        q.length.refund(f.count);
        self.with_tenant(tenant, |t| t.bytes.refund(f.bytes));
        self.global.refund(f.bytes);
        if f.by_ttl > 0 {
            self.metrics.eph_dropped_ttl.fetch_add(f.by_ttl, Ordering::Relaxed);
        }
        if f.by_bounds > 0 {
            self.metrics.eph_dropped_bounds.fetch_add(f.by_bounds, Ordering::Relaxed);
        }
        self.metrics.eph_bytes.store(self.global.used(), Ordering::Relaxed);
    }

    // ----------------------------------------------------------------- pop

    /// Take up to `batch` messages for `group`. Never blocks and never waits:
    /// the long-poll is the handler's job, on the notifier (§3.4), because a
    /// wait inside a lock-holding engine call is how a RAM queue acquires the
    /// convoy behaviour it exists to avoid.
    ///
    /// `partition = None` visits every partition of the queue in name order.
    #[allow(clippy::too_many_arguments)]
    pub fn pop(
        &self,
        tenant: &str,
        name: &str,
        partition: Option<&str>,
        group: Option<&str>,
        batch: usize,
        auto_ack: bool,
        now_ms: i64,
    ) -> Vec<Delivered> {
        let mut out = Vec::new();
        if batch == 0 {
            return out;
        }
        // Auto-vivify on pop too (§1.1): a consumer that parks on its inbox
        // before the producer's first push is the NORMAL req/reply order, and
        // a pop that created nothing would have no gate for the push to wake.
        let Some(q) = self.lookup(tenant, name, true) else { return out };
        let cfg = *q.config.lock().unwrap();
        let group = group.filter(|g| !g.is_empty()).unwrap_or(QUEUE_MODE);
        q.touch(now_ms);
        // BEFORE the rings are visited, and on the empty-result path too: a
        // subscriber parking on an inbox that has no partition yet must still
        // leave its name behind, or the push that creates the partition creates
        // it without a cursor for that subscriber (see `known_groups`).
        q.remember_group(group);

        let parts: Vec<String> = match partition {
            Some(p) => vec![p.to_string()],
            None => q.partition_names(),
        };
        let mut leased_any = false;
        for p in parts {
            if out.len() >= batch {
                break;
            }
            // A named partition is created on demand (a pop may legitimately
            // be the first thing that touches an inbox); a partition-less pop
            // only visits the rings that already exist.
            let ring = match partition {
                Some(_) => q.ring_or_create(&p, || self.next_ring_base()),
                None => match q.ring(&p) {
                    Some(r) => r,
                    None => continue,
                },
            };
            let mut freed = Freed::default();
            {
                let mut r = ring.lock().unwrap();
                let sweep = r.sweep_leases(now_ms, cfg.retry_limit);
                if sweep.exhausted > 0 {
                    self.metrics.eph_dropped_retry.fetch_add(sweep.exhausted, Ordering::Relaxed);
                }
                freed.merge(r.drop_expired(now_ms, cfg.ttl_ms));

                let gi = r.group(group);
                let want = batch - out.len();
                let taken = Self::take(&mut r, gi, want, auto_ack, cfg.lease_ms, now_ms);
                leased_any |= !auto_ack && !taken.is_empty();
                for (seq, attempts) in taken {
                    if let Some(m) = r.get(seq) {
                        out.push(Delivered {
                            id: mint_id(self.epoch, &p, seq),
                            partition: p.clone(),
                            payload: m.payload.clone(),
                            attempts,
                        });
                    }
                }
                freed.merge(r.trim_consumed());
            }
            self.release(tenant, &q, &freed);
        }
        if !out.is_empty() {
            self.metrics.eph_popped.fetch_add(out.len() as u64, Ordering::Relaxed);
        }
        // The lease-expiry backstop only has to be woken when a lease was
        // actually created; a pure autoAck workload never rings it.
        if leased_any {
            self.hint(cfg.lease_ms);
        }
        out
    }

    /// The claim itself: redeliveries first (they are older than anything the
    /// cursor points at), then forward from the cursor. Returns (seq, attempts)
    /// so the caller renders while it still holds the ring.
    fn take(
        r: &mut Ring,
        gi: u32,
        want: usize,
        auto_ack: bool,
        lease_ms: i64,
        now_ms: i64,
    ) -> Vec<(u64, u32)> {
        let mut picked: Vec<(u64, u32)> = Vec::new();
        let tail = r.next_seq;
        while picked.len() < want {
            let seq = {
                let g = &mut r.groups[gi as usize];
                match g.redeliver.pop_front() {
                    Some(s) => s,
                    None => {
                        if g.next >= tail {
                            break;
                        }
                        let s = g.next;
                        g.next += 1;
                        s
                    }
                }
            };
            // A redelivery whose message has since been evicted (bounds/ttl) is
            // silently skipped: the drop was already counted where it happened,
            // and handing back a hole would be worse than handing back nothing.
            if r.get(seq).is_none() {
                continue;
            }
            let attempts = {
                let g = &mut r.groups[gi as usize];
                let n = g.attempts.entry(seq).or_insert(0);
                *n += 1;
                *n
            };
            if auto_ack {
                // §1.3 at-most-once: the cursor already moved, there is no
                // lease to keep and no attempt count to remember.
                r.groups[gi as usize].attempts.remove(&seq);
            } else {
                let id = r.next_lease_id;
                r.next_lease_id += 1;
                let key = (now_ms + lease_ms, id);
                r.leases.insert(key, LeaseRec { gix: gi, seq });
                r.lease_at.insert((gi, seq), key);
            }
            picked.push((seq, attempts));
        }
        picked
    }

    // ----------------------------------------------------------------- ack

    /// Resolve a batch of acks for ONE (queue, group). Ids that name another
    /// incarnation answer `stale`, ids with no live lease answer `unknown`, and
    /// neither is an error — §1.3's at-least-once holds only while THIS
    /// incarnation lives, so a client that reconnects after a restart must be
    /// able to flush its outstanding acks without a 4xx storm.
    pub fn ack(
        &self,
        tenant: &str,
        name: &str,
        group: Option<&str>,
        acks: &[(String, AckStatus)],
        now_ms: i64,
    ) -> Vec<(String, AckOutcome)> {
        let mut out = Vec::with_capacity(acks.len());
        let group = group.filter(|g| !g.is_empty()).unwrap_or(QUEUE_MODE);
        let Some(q) = self.lookup(tenant, name, false) else {
            // No queue: every id is either from a dead incarnation or names
            // something that has been garbage-collected. The epoch still
            // decides which, so the client can tell "restart" from "too late".
            for (id, _) in acks {
                let outcome = match parse_id(id) {
                    Some((e, _, _)) if e != self.epoch => AckOutcome::Stale,
                    _ => AckOutcome::Unknown,
                };
                out.push((id.clone(), outcome));
            }
            return out;
        };
        let cfg = *q.config.lock().unwrap();
        q.touch(now_ms);
        let mut acked = 0u64;
        let mut exhausted = 0u64;
        for (id, status) in acks {
            let Some((epoch, part, seq)) = parse_id(id) else {
                out.push((id.clone(), AckOutcome::Unknown));
                continue;
            };
            if epoch != self.epoch {
                out.push((id.clone(), AckOutcome::Stale));
                continue;
            }
            let Some(ring) = q.ring(part) else {
                out.push((id.clone(), AckOutcome::Unknown));
                continue;
            };
            let mut r = ring.lock().unwrap();
            let Some(&gi) = r.gix.get(group) else {
                out.push((id.clone(), AckOutcome::Unknown));
                continue;
            };
            let Some(key) = r.lease_at.remove(&(gi, seq)) else {
                out.push((id.clone(), AckOutcome::Unknown));
                continue;
            };
            r.leases.remove(&key);
            let outcome = match status {
                AckStatus::Completed => {
                    r.groups[gi as usize].attempts.remove(&seq);
                    acked += 1;
                    AckOutcome::Acked
                }
                // failed / retry are the same decision here (§1.3): both
                // redeliver with attempts+1. They are kept distinct on the wire
                // because they mean different things to a human reading a trace,
                // not because the engine branches on them.
                AckStatus::Failed | AckStatus::Retry => {
                    let g = &mut r.groups[gi as usize];
                    let n = *g.attempts.get(&seq).unwrap_or(&0);
                    if n >= cfg.retry_limit {
                        g.attempts.remove(&seq);
                        exhausted += 1;
                        AckOutcome::Acked
                    } else {
                        g.redeliver.push_back(seq);
                        // Keep FIFO among pending redeliveries (§1.4).
                        let mut v: Vec<u64> = g.redeliver.drain(..).collect();
                        v.sort_unstable();
                        v.dedup();
                        g.redeliver = v.into();
                        AckOutcome::Redelivered
                    }
                }
            };
            let freed = r.trim_consumed();
            drop(r);
            self.release(tenant, &q, &freed);
            out.push((id.clone(), outcome));
        }
        if acked > 0 {
            self.metrics.eph_acked.fetch_add(acked, Ordering::Relaxed);
        }
        if exhausted > 0 {
            self.metrics.eph_dropped_retry.fetch_add(exhausted, Ordering::Relaxed);
        }
        out
    }

    // --------------------------------------------------------------- sweep

    /// The sweeper's backstop pass (§3.2, M10): expire leases nobody touched,
    /// drop ttl'd heads on queues nobody is polling, and garbage-collect
    /// implicit queues that have been empty and idle past
    /// `QUEEN_EPHEMERAL_IMPLICIT_IDLE_S`.
    ///
    /// Everything here also happens opportunistically on every ring touch. This
    /// pass exists for the rings NOBODY touches — which, for a class built on
    /// thousands of short-lived inboxes, is most of them most of the time.
    pub fn sweep(&self, now_ms: i64) -> Sweep {
        let mut out = Sweep::default();
        // A snapshot of `Arc`s, so the pass never holds the queue map while it
        // locks a ring — but it MUST be dropped before `gc_implicit`, whose
        // eviction interlock is `Arc::strong_count == 1`. Holding these clones
        // across the GC would make every queue look busy and the collector a
        // no-op forever, which is the kind of bug that only shows up as a slow
        // memory climb on a cell with a lot of short-lived inboxes.
        {
            let snapshot: Vec<(String, Arc<EqQueue>)> = self
                .queues
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            for (key, q) in &snapshot {
                let (tenant, _) = crate::handlers::split_tenant_queue(key);
                let cfg = *q.config.lock().unwrap();
                let mut freed = Freed::default();
                let mut next: Option<i64> = None;
                for p in q.partition_names() {
                    let Some(ring) = q.ring(&p) else { continue };
                    let mut r = ring.lock().unwrap();
                    let ls = r.sweep_leases(now_ms, cfg.retry_limit);
                    out.redelivered += ls.redelivered;
                    out.exhausted += ls.exhausted;
                    freed.merge(r.drop_expired(now_ms, cfg.ttl_ms));
                    freed.merge(r.trim_consumed());
                    if let Some(d) = r.next_expiry() {
                        next = Some(next.map_or(d, |c: i64| c.min(d)));
                    }
                }
                out.dropped_ttl += freed.by_ttl;
                // Refund only what was actually reclaimed — `release` also
                // writes the cell gauge, and a no-op pass over ten thousand
                // idle queues must not write it ten thousand times.
                if freed.count != 0 || freed.bytes != 0 {
                    self.release(tenant, q, &freed);
                }
                if let Some(d) = next {
                    out.next_expiry_ms = Some(out.next_expiry_ms.map_or(d, |c| c.min(d)));
                }
            }
        }
        // Counted once per PASS and not once per ring, so the counter and the
        // sweeper's log line agree on what one pass did.
        if out.exhausted > 0 {
            self.metrics.eph_dropped_retry.fetch_add(out.exhausted, Ordering::Relaxed);
        }
        out.gc_queues = self.gc_implicit(now_ms);
        if let Some(d) = out.next_expiry_ms {
            self.hint((d - now_ms).max(0));
        }
        out
    }

    /// §1.1/§3.2 — free implicit queues that are empty and unpolled.
    ///
    /// The `strong_count == 1` test under the map lock is the same eviction
    /// interlock `Notifier::evict_idle` uses, and it is not decoration: a push
    /// that has taken its `Arc` and is about to append must not have its queue
    /// removed out from under it, or the message lands in an orphaned ring and
    /// is lost from a queue that is demonstrably in use.
    fn gc_implicit(&self, now_ms: i64) -> u64 {
        let idle = self.knobs.implicit_idle_ms;
        if idle <= 0 {
            return 0;
        }
        let mut gone: Vec<(String, Arc<EqQueue>)> = Vec::new();
        {
            let mut m = self.queues.lock().unwrap();
            m.retain(|k, q| {
                if q.declared.load(Ordering::Relaxed) || Arc::strong_count(q) != 1 {
                    return true;
                }
                if q.length.used() > 0 || q.bytes.used() > 0 {
                    return true;
                }
                if now_ms - q.last_touch_ms.load(Ordering::Relaxed) < idle {
                    return true;
                }
                gone.push((k.clone(), q.clone()));
                false
            });
            let n = m.len() as i64;
            drop(m);
            self.metrics.eph_queues.store(n, Ordering::Relaxed);
        }
        for (k, q) in &gone {
            // Empty by the test above, so there are no bytes to give back; the
            // per-tenant queue COUNT is what has to come down.
            let (tenant, _) = crate::handlers::split_tenant_queue(k);
            self.with_tenant(tenant, |t| {
                let _ = t.queues.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    Some((n - 1).max(0))
                });
            });
            debug_assert_eq!(q.bytes.used(), 0);
        }
        gone.len() as u64
    }

    // ---------------------------------------------------------- inspection

    /// (messages, bytes) held by one queue. For the status verbs (T1b) and for
    /// tests; reads two atomics and takes no lock.
    #[allow(dead_code)] // T1b: GET /api/v1/ephemeral/queues/:queue/depth
    pub fn depth(&self, tenant: &str, name: &str) -> Option<(i64, i64)> {
        self.lookup(tenant, name, false).map(|q| (q.length.used(), q.bytes.used()))
    }

    /// How many messages `group` never saw because eviction ran under it.
    #[allow(dead_code)] // T1b: the dashboard's per-group drop column (§5.3)
    pub fn skipped(&self, tenant: &str, name: &str, partition: &str, group: &str) -> u64 {
        let Some(q) = self.lookup(tenant, name, false) else { return 0 };
        let Some(ring) = q.ring(partition) else { return 0 };
        let r = ring.lock().unwrap();
        match r.gix.get(group) {
            Some(&i) => r.groups[i as usize].skipped,
            None => 0,
        }
    }

    /// The queue's bare name, for a log line that must not print the composite.
    #[allow(dead_code)] // T1b: the status verbs' payload
    pub fn name_of(&self, tenant: &str, name: &str) -> Option<String> {
        self.lookup(tenant, name, false).map(|q| q.name.clone())
    }
}

// ===========================================================================
// §1.7 — the windowBuffer decision, as a pure function
// ===========================================================================

/// Should a waiting pop return NOW?
///
/// Pure (no clock, no config lookup) so the rule is testable without an HTTP
/// stack or a running broker, which is the same reason `sweeper::sleep_ms` is
/// shaped this way.
///
///   * a pop holding nothing keeps waiting — an empty early return would turn a
///     long-poll into a poll loop, which is the one thing this class must not
///     reintroduce;
///   * a FULL batch always returns: the window fattens a batch, it never caps
///     one;
///   * with the window off, the first message returns immediately (the durable
///     pop's behaviour);
///   * otherwise return once `count` messages are held or `ms` has passed since
///     the FIRST of them. The pop's own `timeout` bounds this from outside and
///     is not this function's business.
pub fn window_ready(have: usize, batch: usize, waited_ms: i64, w: Window) -> bool {
    if have == 0 {
        return false;
    }
    if have >= batch {
        return true;
    }
    if !w.enabled() {
        return true;
    }
    if w.count > 0 && have >= w.count {
        return true;
    }
    w.ms > 0 && waited_ms >= w.ms as i64
}

// ===========================================================================
// Tests (EPHEMERAL_QUEUES.md §7.1, the "pure logic" half)
// ===========================================================================

#[cfg(test)]
#[path = "tests_unit/ephemeral_engine.rs"]
mod ephemeral_engine_tests;
