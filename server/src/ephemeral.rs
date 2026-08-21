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
//! PLACEMENT: RENDEZVOUS, NO LEASES (§3.7)
//!
//! Every (queue, partition) has exactly ONE owner, chosen by a highest-random-
//! weight hash over `eph_members() ∪ self`. There is no lease row, no heartbeat
//! write and no fencing protocol: every broker computes the same answer from the
//! same membership, and when membership changes the partitions that moved take
//! their contents with them — which is already the §1.2 loss contract, so a
//! membership flap costs CONTENT, never correctness. Stale acks die on the epoch
//! (§3.1), which is the only fence this design needs.
//!
//! THE SINGLE-BROKER PATH IS UNCHANGED AND THAT IS LOAD-BEARING. `route`
//! short-circuits to `Local` before it hashes anything whenever no mesh
//! transport is attached — and one is attached only when `mesh_active()` (sync
//! on AND at least one peer). Every free-tier cell and the embedded
//! `queen::Broker` therefore execute exactly the instructions they executed
//! before this phase existed: one `OnceLock::get` returning `None`.
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
//! THE GRANT AND THE CAPS (§1.6 rung 2, M7) — WHY THEY LIVE HERE
//!
//! The KV quota reads its occupancy out of a table that a rollup writes, so its
//! enforcer lives in `quota.rs` next to that measurement. This class has no such
//! loop: the bytes are in THIS process's heap and the numbers below are exact,
//! not sampled. So the per-tenant allowance lives on `TenantUsage`, next to the
//! occupancy it bounds, and `switches::decide_ephemeral` reads it through
//! `gate_grant` / `gate_push` — same three rungs, same `Answer`, different
//! authority. What Postgres holds is only the GRANT ROW, refreshed on a slow
//! cadence by `refresh_once` at the bottom of this file.
//!
//! The `#[allow(dead_code)] // T1b` marks that used to sit on the `configure`
//! seam and the status reads are gone: this phase gave every one of them a
//! caller, which is exactly what the marks were there to make visible.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
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
    /// `QUEEN_EPHEMERAL_REQUIRE_GRANT` (§1.6 rung 2). ON ⇒ a tenant with no row
    /// in the grant table is REFUSED, not merely unlimited. Derived from the
    /// tenancy flag, which is the whole `config.rs` KV posture: OSS on by
    /// default, cloud off until the plan grants it.
    pub require_grant: bool,
    /// Cell-default per-tenant message rate, overridden by a grant row's
    /// `maxMsgsPerSec`.
    pub rate: u32,
    pub burst: u32,
    /// Size cap on the per-tenant map. It DENIES and does not evict: under an
    /// unvalidated, caller-chosen tenant id an evicting map is an unbounded map
    /// with extra steps, and every rotated id would be handed a fresh budget.
    /// A tenant the control plane knows about is exempt (`quota.rs` §9.4 point 2).
    pub max_tenants: usize,
}

impl Knobs {
    /// The documented defaults, in ONE place, so the `Config` block and every
    /// test read the same numbers (§10 Q2: 256 MiB cell, 16 MiB / 10 000 queue).
    ///
    /// TEST-ONLY IN PRODUCTION BUILDS, and deliberately not `#[cfg(test)]`: the
    /// numbers here are the documented contract, and hiding them behind a test
    /// gate is how a default drifts from the value `config.rs` reads. The allow
    /// says "no production caller", not "no caller".
    #[allow(dead_code)]
    pub fn defaults() -> Knobs {
        Knobs {
            global_max_bytes: 256 * 1024 * 1024,
            queue_max_bytes: 16 * 1024 * 1024,
            queue_max_length: 10_000,
            lease_ms: 30_000,
            retry_limit: 5,
            implicit_idle_ms: 300_000,
            // Never a grant requirement by default: the defaults are the
            // self-hosted posture, and a fail-closed default here would mean a
            // fresh OSS cell answers `feature_gated` to its own operator.
            require_grant: false,
            rate: 5_000,
            burst: 10_000,
            max_tenants: 10_000,
        }
    }
}

/// What a full queue does to the next push (§1.6 rung 1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    /// 429 `queue_full` — the shape the 1.0.6 SDK backpressure already handles.
    Reject,
    /// Feed semantics: drop from the head until the arrival fits.
    DropOldest,
}

impl Policy {
    /// The wire spelling of `configure`'s `policy` option, parsed in ONE place so
    /// the boot load of a stored blob and the HTTP verb cannot disagree about
    /// what `"dropOldest"` means. `None` = not one of ours; the caller decides
    /// whether that is a 400 (the verb) or a skipped option (the boot load).
    pub fn parse(s: &str) -> Option<Policy> {
        match s {
            "reject" => Some(Policy::Reject),
            "dropOldest" => Some(Policy::DropOldest),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Policy::Reject => "reject",
            Policy::DropOldest => "dropOldest",
        }
    }
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

    #[inline]
    pub fn cap(&self) -> i64 {
        self.cap.load(Ordering::Relaxed)
    }

    /// Raise or lower the ceiling at runtime (a grant refresh). Lowering it
    /// below current occupancy is legal and blocks the next arrival rather than
    /// evicting anything: this budget bounds admission, never contents.
    pub fn set_cap(&self, cap: i64) {
        self.cap.store(cap, Ordering::Relaxed);
    }
}

// ===========================================================================
// Message ids
// ===========================================================================

// ===========================================================================
// §3.7 — rendezvous placement
// ===========================================================================

/// Where one (queue, partition) is served.
///
/// A THREE-STATE ANSWER COLLAPSED TO TWO: "I own it", "that broker owns it".
/// There is deliberately no "nobody owns it yet" — the hash is total over a
/// non-empty candidate set and self is always in that set, so the question
/// always has an answer and no request ever has to wait for a placement
/// decision. That is the whole reason placement is a hash and not a lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Route {
    Local,
    Remote {
        server_id: String,
        /// The peer's advertised base URL, e.g. `http://queen-b:6632`.
        http_addr: String,
    },
}

/// One node's weight for one key (§3.7).
///
/// HRW ("rendezvous") and not a modulo or a consistent-hashing ring, for the
/// property that matters here: adding or removing ONE node moves only the keys
/// that node wins or loses — roughly `1/n` of them — and moves nothing else. On
/// this class a moved key is an emptied ring (§1.2), so "how many keys move" is
/// literally "how much data is dropped", and a modulo (which reshuffles almost
/// everything) would turn one broker joining into a cell-wide wipe.
///
/// The score is `xxh3_64(key, seed = xxh3_64(node))`: hashing the NODE into the
/// seed and the KEY into the body means the per-node seed can be computed once
/// per node and the key is walked once per candidate, and — unlike
/// `hash(node ++ key)` — no concatenation buffer is allocated per candidate.
pub fn hrw_score(node: &str, key: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64_with_seed(key.as_bytes(), xxhash_rust::xxh3::xxh3_64(node.as_bytes()))
}

/// The winner of `key` among `nodes`. `None` only on an empty candidate set,
/// which no caller here can produce (self is always a candidate).
///
/// The tie-break is the node NAME and never the iteration order: two brokers
/// hashing the same key must agree, and the order a `Vec` happens to be in is
/// not a fact they share. A 64-bit collision between two live node ids is
/// vanishingly unlikely and the tie-break costs one comparison — the point is
/// that the answer is a FUNCTION of the membership set, with no hidden inputs.
pub fn hrw_pick<'a>(key: &str, nodes: &'a [String]) -> Option<&'a str> {
    let mut best: Option<(&'a str, u64)> = None;
    for n in nodes {
        let s = hrw_score(n, key);
        let take = match best {
            None => true,
            Some((bn, bs)) => s > bs || (s == bs && n.as_str() > bn),
        };
        if take {
            best = Some((n.as_str(), s));
        }
    }
    best.map(|(n, _)| n)
}

/// The rendezvous key of one (queue, partition): the engine's own composite key
/// plus the partition, joined with the same `\x1f` (§3.7).
///
/// Built from `qkey` and not from the three parts, so the tenant/queue half is
/// spelled EXACTLY as the map key it names — including the `eph:` prefix. A key
/// that differed from the map key by one byte would place a partition on a
/// broker that then looked it up under a different name.
pub fn rendezvous_key(tenant: &str, name: &str, partition: &str) -> String {
    format!("{}\x1f{}", Ephemeral::qkey(tenant, name), partition)
}

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

    /// Messages this group could still be handed: everything from its cursor to
    /// the tail, plus its pending redeliveries. Pure arithmetic — the deque is
    /// contiguous over `[head_seq, next_seq)` by construction (every eviction
    /// pops the FRONT and advances `head_seq` with it), so no walk is needed.
    fn pending_of(&self, gi: u32) -> i64 {
        let g = &self.groups[gi as usize];
        let from = g.next.max(self.head_seq);
        let forward = self.next_seq.saturating_sub(from) as i64;
        // Redeliveries below the head were evicted under the group and were
        // already counted as skips; they are not pending, they are gone.
        let back = g.redeliver.iter().filter(|&&s| s >= self.head_seq).count() as i64;
        forward + back
    }

    /// The number a DEPTH READ should publish for this ring.
    ///
    /// With a named group, that group's own backlog. Without one, the WORST
    /// cursor — the same precedence `log_queue_depth_v1` applies on the durable
    /// side, named groups first and `__QUEUE_MODE__` speaking only when no named
    /// group exists — so the two engines answer the same question the same way.
    /// With no cursor at all the answer is the ring's length: nobody has
    /// consumed anything, so everything is owed.
    fn pending_for(&self, group: Option<&str>) -> i64 {
        if let Some(name) = group {
            return match self.gix.get(name) {
                Some(&gi) => self.pending_of(gi),
                // A group with no cursor here owes the whole retained range —
                // the same convention as everywhere else.
                None => self.deque.len() as i64,
            };
        }
        let mut worst: Option<i64> = None;
        for (name, &gi) in self.gix.iter() {
            if name == QUEUE_MODE {
                continue;
            }
            let p = self.pending_of(gi);
            worst = Some(worst.map_or(p, |w: i64| w.max(p)));
        }
        if let Some(w) = worst {
            return w;
        }
        match self.gix.get(QUEUE_MODE) {
            Some(&gi) => self.pending_of(gi),
            None => self.deque.len() as i64,
        }
    }

    /// §3.1 `reset`, at ring level: everything goes, the cursors land on the tail
    /// and the leases are voided. Returns what the caller must refund; the ring
    /// does not own the budgets (see `Freed`).
    fn wipe(&mut self) -> Freed {
        let freed = Freed {
            bytes: self.bytes,
            count: self.deque.len() as i64,
            by_ttl: 0,
            by_bounds: 0,
        };
        self.deque.clear();
        self.bytes = 0;
        self.head_seq = self.next_seq;
        self.leases.clear();
        self.lease_at.clear();
        for g in self.groups.iter_mut() {
            g.next = self.next_seq;
            g.redeliver.clear();
            g.attempts.clear();
        }
        freed
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
    /// PER-QUEUE drop counters (§5.3: the dashboard's `drops` column). The
    /// process-wide `Metrics` counters answer "is this cell dropping?"; these
    /// answer "WHICH queue is", which is the question an operator actually has,
    /// and a Prometheus label per queue is the cardinality §14.1 forbids. They
    /// are three and not one for the same reason the global ones are: bounds
    /// means the queue is too small, ttl that the consumer is too slow, retry
    /// that the handlers are failing.
    dropped_bounds: AtomicU64,
    dropped_ttl: AtomicU64,
    dropped_retry: AtomicU64,
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

    /// Distinct consumer groups this queue has been popped with. Read from
    /// `known_groups` and not from a ring's `gix`, because the set is the union
    /// over partitions and survives a ring that has been collected — which is
    /// what a fan-out operator wants to count.
    fn group_count(&self) -> usize {
        self.known_groups.lock().unwrap().len()
    }

    fn drops(&self) -> (u64, u64, u64) {
        (
            self.dropped_bounds.load(Ordering::Relaxed),
            self.dropped_ttl.load(Ordering::Relaxed),
            self.dropped_retry.load(Ordering::Relaxed),
        )
    }
}

/// A token bucket over PUSHED MESSAGES per second (§1.6 rung 2).
///
/// A near-copy of `quota.rs`'s, and deliberately not a shared one: that bucket
/// spends exactly one token per CALL, because a KV batch is one commit however
/// many ops it carries. Here the unit is the MESSAGE — a push of 10 000 costs
/// 10 000 — since what the rate bounds is arrival into RAM, and a per-call
/// bucket would be defeated by batching, which this class encourages.
#[derive(Debug)]
struct RateBucket {
    tokens: f64,
    last_ms: i64,
}

impl RateBucket {
    fn new(burst: u32) -> RateBucket {
        RateBucket { tokens: burst as f64, last_ms: crate::util::now_epoch_ms() }
    }

    /// Spend `n` tokens, refilling first. `Retry-After` is never zero: it reads
    /// as "immediately" and produces a spin against a cell that just said no.
    fn take(&mut self, n: f64, rate: u32, burst: u32) -> crate::quota::Verdict {
        use crate::quota::Verdict;
        let now = crate::util::now_epoch_ms();
        let dt = (now - self.last_ms).max(0) as f64 / 1000.0;
        self.last_ms = now;
        self.tokens = (self.tokens + dt * rate as f64).min(burst as f64);
        if self.tokens >= n {
            self.tokens -= n;
            return Verdict::Allow;
        }
        let need = n - self.tokens;
        let secs = if rate == 0 { 1.0 } else { need / rate as f64 };
        Verdict::RateLimited(secs.ceil().max(1.0) as u32)
    }
}

/// Per-tenant occupancy AND allowance (§1.6 rung 2), in one place because the
/// two are read together on every push and a split would be two locks.
///
/// `Budget::cap == 0` is unlimited, which is what an ungranted tenant carries on
/// an OSS cell: the rung is inert rather than a denial, so a self-hosted operator
/// is never gated by a table nobody writes. `granted` is the OTHER half and is
/// the one that bites in cloud — with `require_grant` on, its absence is a
/// refusal, not a permission.
struct TenantUsage {
    bytes: Budget,
    queues: AtomicI64,
    /// `max_queues` from the grant row. 0 = unlimited.
    queues_cap: AtomicI64,
    /// Does a grant row exist for this tenant? Written only by the refresh.
    granted: AtomicBool,
    /// The row's `enabled` column. Meaningless while `granted` is false.
    enabled: AtomicBool,
    /// The row's `max_msgs_per_sec`, 0 = use the cell default.
    rate_per_s: AtomicU32,
    rate: Mutex<RateBucket>,
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

/// One row of `GET /api/v1/ephemeral/queues` (§3.1, §5.3).
///
/// The columns are this class's own truth and are deliberately NOT the durable
/// listing's: there is no `pending` vs `retained` split (nothing is retained), no
/// DLQ (§9) and no lag-from-Postgres. `declared` is the tier of §1.1, and it is
/// the one column that tells an operator whether a queue survives a restart —
/// as configuration, never as contents.
#[derive(Clone, Debug)]
pub struct QueueStat {
    pub name: String,
    pub declared: bool,
    pub depth: i64,
    pub bytes: i64,
    pub partitions: i64,
    pub groups: i64,
    pub dropped_bounds: u64,
    pub dropped_ttl: u64,
    pub dropped_retry: u64,
    pub config: QueueConfig,
}

/// `GET /api/v1/ephemeral/queues/:queue/depth` (§3.1).
#[derive(Clone, Debug)]
pub struct Depth {
    pub queue: String,
    pub declared: bool,
    pub pending: i64,
    pub partitions_pending: i64,
    pub bytes: i64,
    pub partitions: Vec<PartitionDepth>,
    pub groups: Vec<GroupDepth>,
}

#[derive(Clone, Debug)]
pub struct PartitionDepth {
    pub partition: String,
    pub pending: i64,
    pub bytes: i64,
}

/// Per-group backlog AND the skip counter (§3.2). `skipped` is on the depth read
/// and not only in a log line because for a fan-out consumer it is the
/// difference between "slow" and "lost data", and this class's whole contract is
/// that the second is legal — so it has to be legible.
#[derive(Clone, Debug)]
pub struct GroupDepth {
    pub group: String,
    pub pending: i64,
    pub skipped: u64,
}

/// One row of the grant table (§1.6 rung 2, §2). `None` is UNLIMITED — the same
/// convention as the proxy's plan columns and as `quota.rs::Limits`.
#[derive(Clone, Debug)]
pub struct Grant {
    pub tenant: String,
    pub enabled: bool,
    pub max_bytes: Option<i64>,
    pub max_queues: Option<i64>,
    pub max_msgs_per_sec: Option<i64>,
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
    /// §3.5/§3.7 — the mesh, for membership and for the admin broadcast.
    ///
    /// A `OnceLock` and not a constructor argument for the same reason the
    /// notifier's is: the transport is built AFTER the state its inbound
    /// handlers capture, so taking it at construction would be a cycle. NEVER
    /// SET ⇒ single broker ⇒ `route` short-circuits to `Local` before it hashes
    /// anything and `broadcast_admin` is a no-op — the pre-mesh behaviour, which
    /// is what every free-tier cell and the embedded `queen::Broker` run.
    mesh: OnceLock<Arc<crate::mesh::MeshTransport>>,
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
            mesh: OnceLock::new(),
        })
    }

    /// Wire the sweeper's waker. Only the first call wins; safe to never call.
    pub fn attach_wake_hint(&self, f: fn(i64)) {
        let _ = self.wake_hint.set(f);
    }

    /// Wire the mesh (§3.5/§3.7). Only the first call wins; safe to never call —
    /// never calling it IS the single-broker configuration.
    pub fn attach_mesh(&self, t: Arc<crate::mesh::MeshTransport>) {
        let _ = self.mesh.set(t);
    }

    #[inline]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    #[inline]
    pub fn knobs(&self) -> &Knobs {
        &self.knobs
    }

    #[inline]
    pub fn global_bytes(&self) -> i64 {
        self.global.used()
    }

    /// `QUEEN_EPHEMERAL_MAX_BYTES` as it is actually in force. Published next to
    /// `global_bytes` by the operator endpoint: a byte count with no ceiling
    /// beside it is a number nobody can act on at three in the morning.
    #[inline]
    pub fn global_cap(&self) -> i64 {
        self.global.cap()
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
        // §1.6 rung 2, the OBJECT half: `max_queues` counts declared plus live
        // implicit (§1.1). Checked HERE, on the auto-vivify path, because that is
        // the only place a queue is born — a check in the `configure` verb alone
        // would bound the tier that costs one row and leave unbounded the tier
        // that costs a ring each.
        if !self
            .with_tenant(tenant, |t| {
                let cap = t.queues_cap.load(Ordering::Relaxed);
                cap <= 0 || t.queues.load(Ordering::Relaxed) < cap
            })
            .unwrap_or(false)
        {
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
            dropped_bounds: AtomicU64::new(0),
            dropped_ttl: AtomicU64::new(0),
            dropped_retry: AtomicU64::new(0),
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
            let _ = self.with_tenant(tenant, |t| {
                t.queues.fetch_add(1, Ordering::Relaxed);
            });
        }
        Some(arc)
    }

    /// Run `f` against this tenant's usage, creating the entry on first sight.
    ///
    /// `None` means THE MAP IS FULL and this tenant is not already in it — the
    /// `quota.rs` §9.4 point 2 rule, restated because it is the one that looks
    /// like a bug: the cap DENIES rather than evicting. Under a tenant id that is
    /// opaque and unvalidated (`tenant.rs`), an evicting map is an unbounded map
    /// with extra steps, and every rotated header would be handed a fresh budget.
    /// Every caller here fails CLOSED on `None`; the honest 503 for it is
    /// produced one level up, by `gate_grant`, which is what the ladder calls
    /// before any of this runs.
    fn with_tenant<R>(&self, tenant: &str, f: impl FnOnce(&TenantUsage) -> R) -> Option<R> {
        let mut m = self.tenants.lock().unwrap();
        if !m.contains_key(tenant) && m.len() >= self.knobs.max_tenants {
            return None;
        }
        let e = m.entry(tenant.to_string()).or_insert_with(|| TenantUsage {
            // 0 = unlimited, until a grant row says otherwise. The rung is inert
            // rather than a denial for an ungranted tenant on an OSS cell, so
            // nobody is gated by a table nobody writes.
            bytes: Budget::new(0),
            queues: AtomicI64::new(0),
            queues_cap: AtomicI64::new(0),
            granted: AtomicBool::new(false),
            enabled: AtomicBool::new(true),
            rate_per_s: AtomicU32::new(0),
            rate: Mutex::new(RateBucket::new(self.knobs.burst)),
        });
        Some(f(e))
    }

    // -------------------------------------------------------- the grant rungs

    /// Rung 2 alone: is this tenant allowed on the surface at all?
    ///
    /// Called through `switches::decide_ephemeral`, never directly from a
    /// handler — the order of the rungs is decided in one place or it is decided
    /// in several.
    pub fn gate_grant(&self, tenant: &str) -> crate::quota::Verdict {
        use crate::quota::Verdict;
        let seen = self.with_tenant(tenant, |t| {
            (t.granted.load(Ordering::Relaxed), t.enabled.load(Ordering::Relaxed))
        });
        // The map is full and this tenant is new: a CELL condition, not the
        // tenant's doing, so it takes the cell's status (503) and not a 403.
        let Some((granted, enabled)) = seen else { return Verdict::NoRoom };
        if self.knobs.require_grant && !granted {
            return Verdict::NotGranted;
        }
        if granted && !enabled {
            return Verdict::NotGranted;
        }
        Verdict::Allow
    }

    /// Rungs 2 and 3 for a push of `n` messages: the grant, then the per-tenant
    /// message rate. The BYTE allowance is not tested here — it is charged
    /// atomically inside `push`, where the bytes are (see `decide_ephemeral`).
    pub fn gate_push(&self, tenant: &str, n: i64) -> crate::quota::Verdict {
        use crate::quota::Verdict;
        let v = self.gate_grant(tenant);
        if v != Verdict::Allow {
            return v;
        }
        // A push of nothing is not an arrival and costs no token: the SDK's
        // buffered sink can legally flush an empty batch on close.
        if n <= 0 {
            return Verdict::Allow;
        }
        self.with_tenant(tenant, |t| {
            // A grant row's rate is its own burst — `quota.rs::write_rate`'s
            // rule, and for its reason: the cell default gets a burst because an
            // operator sized the pair together, while a plan carries ONE number
            // and inventing a multiplier for it would let a tenant exceed the
            // figure its plan actually names.
            let per_row = t.rate_per_s.load(Ordering::Relaxed);
            let (rate, burst) = if per_row > 0 {
                (per_row, per_row.max(1))
            } else {
                (self.knobs.rate, self.knobs.burst)
            };
            // Clamped to the burst: a single batch larger than the bucket could
            // ever hold would be refused for ever, with a `Retry-After` that
            // never comes true. The edge cap on items per call is the real bound
            // on that shape; this one only has to be non-starving.
            let want = (n as f64).min(burst as f64);
            t.rate.lock().unwrap().take(want, rate, burst)
        })
        .unwrap_or(Verdict::NoRoom)
    }

    /// SEAM (T1b): `configure` and the boot load of declared configs. Creating
    /// the queue here is what makes a declared queue exist "as configured but
    /// empty" across a restart (§1.2).
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

    // ------------------------------------------------- §3.7 rendezvous placement

    /// The candidate set: every live, ephemeral-capable peer, plus self.
    ///
    /// `None` means THERE IS NO MESH — not "no members" — and is the
    /// short-circuit of §3.7: with no transport attached, self owns everything
    /// and no hash is computed at all.
    /// ONE MEMBERSHIP SNAPSHOT PER DECISION. The members come back with their
    /// addresses attached so the winner's `http_addr` is a lookup in a list
    /// already in hand — asking the mesh twice would not only cost a second
    /// clone of the list on every forwarded request, it would let the two reads
    /// disagree and pick an address for a node the second read no longer has.
    fn candidates(&self) -> Option<(String, Vec<String>, Vec<crate::mesh::EphMember>)> {
        let mesh = self.mesh.get()?;
        let members = mesh.eph_members();
        if members.is_empty() {
            // A mesh with no LIVE capable peer is a single broker for placement
            // purposes, and taking the same early exit keeps the two cases one
            // code path: a cell whose peer is down must behave exactly like a
            // cell that never had one, or a rolling restart would answer
            // differently from a fresh install.
            return None;
        }
        let me = mesh.server_id().to_string();
        let mut nodes: Vec<String> = Vec::with_capacity(members.len() + 1);
        nodes.push(me.clone());
        for m in &members {
            // A peer announcing OUR server_id is a misconfiguration (two brokers
            // sharing QUEEN_SERVER_ID); dedupe rather than let one key have two
            // holders that each think they are it.
            if m.server_id != me {
                nodes.push(m.server_id.clone());
            }
        }
        Some((me, nodes, members))
    }

    /// Who owns `(queue, partition)` — and, when that is not us, DROP whatever
    /// this broker still holds for it (§3.7, the membership-change wipe).
    ///
    /// THE WIPE IS A SIDE EFFECT OF ASKING, and that is the design, not a
    /// shortcut. Ownership moves when membership changes, and a ring left behind
    /// on the old owner is memory nobody can reach: its messages are invisible to
    /// every consumer (they all forward to the new owner) and its bytes still
    /// count against three budgets. Doing it here means it happens on the next
    /// TOUCH of that partition, with no watcher thread and no scan — and the
    /// periodic `reap_foreign` below covers the partitions nobody touches again.
    pub fn route(&self, tenant: &str, name: &str, partition: &str) -> Route {
        match self.owner_of(tenant, name, partition) {
            None => Route::Local,
            Some((server_id, http_addr)) => {
                self.wipe_ring(tenant, name, partition);
                Route::Remote { server_id, http_addr }
            }
        }
    }

    /// The PURE half of `route`: who owns this partition, with no side effect.
    /// `None` = this broker (including the no-mesh short-circuit).
    ///
    /// Split out so the periodic reap can ask about a partition and then decide
    /// what to do, and so the decision itself is testable without a live ring.
    fn owner_of(&self, tenant: &str, name: &str, partition: &str) -> Option<(String, String)> {
        let c = self.candidates()?;
        Self::owner_in(&c, tenant, name, partition)
    }

    /// `owner_of` against a candidate set the caller already holds.
    ///
    /// Split out for `reap_foreign`, which asks about EVERY partition on the
    /// broker: a fresh membership snapshot per partition would make a cell with
    /// ten thousand inboxes lock and clone the peer list ten thousand times per
    /// refresh. Reusing one snapshot is also the more correct sweep — a pass that
    /// re-read membership between partitions could act on two rings under two
    /// different views of the cluster.
    fn owner_in(
        cands: &(String, Vec<String>, Vec<crate::mesh::EphMember>),
        tenant: &str,
        name: &str,
        partition: &str,
    ) -> Option<(String, String)> {
        let (me, nodes, members) = cands;
        let key = rendezvous_key(tenant, name, partition);
        let owner = hrw_pick(&key, nodes)?;
        if owner == me {
            return None;
        }
        // The address is looked up on the WINNER rather than carried through the
        // hash: the hash's input is the id set — the thing every broker agrees on
        // — and an address is a local detail of how to reach it.
        let addr = members
            .iter()
            .find(|x| x.server_id == owner)
            .map(|x| x.http_addr.clone())
            .filter(|a| !a.is_empty())?;
        Some((owner.to_string(), addr))
    }

    /// The partition-less pop's placement rule (§3.7, v1).
    ///
    /// A pop that names no partition is asking about a QUEUE, and a queue's
    /// partitions can hash to different owners — so there is no single right
    /// answer and v1 picks the simple one:
    ///
    ///   * if ANY partition of this queue is owned here, serve LOCALLY and let
    ///     the engine's existing partition-less pop visit the local rings. The
    ///     pop sees this broker's share of the queue and nothing else.
    ///   * otherwise route on the DEFAULT partition, which is the only partition
    ///     a queue that has never been addressed by partition will ever have.
    ///
    /// The case this deliberately does not solve is a multi-partition queue
    /// popped without a partition from a broker that owns some of it: the
    /// consumer sees the local share only. Naming the partition is the complete
    /// answer (it routes exactly), and fan-out over a multi-owner queue is what
    /// §9's replicated local reads exist for. Scattering one pop across owners
    /// would mean N forwarded long-polls per request, which is a distributed
    /// query, not a queue read.
    ///
    /// The walk also WIPES: every partition it visits that no longer hashes here
    /// is dropped by `route`, so a queue whose partitions all moved away cleans
    /// itself up on the first pop rather than waiting for the periodic reap.
    pub fn route_queue(&self, tenant: &str, name: &str) -> Route {
        // Alone in the ring ⇒ everything is local and there is nothing to walk.
        // The same early exit `route` takes, so the two cannot disagree.
        if self.candidates().is_none() {
            return Route::Local;
        }
        let mut any_local = false;
        if let Some(q) = self.lookup(tenant, name, false) {
            for p in q.partition_names() {
                if self.route(tenant, name, &p) == Route::Local {
                    any_local = true;
                }
            }
        }
        if any_local {
            return Route::Local;
        }
        self.route(tenant, name, DEFAULT_PARTITION)
    }

    /// Drop one ring this broker no longer owns, refunding its budgets and
    /// counting the move. `false` when there was nothing here.
    ///
    /// A push holding an `Arc` to this ring can still append to it after the
    /// removal, and that message is lost — which is precisely the §1.2/§1.4
    /// contract for an ownership move ("the few seconds of membership
    /// disagreement may blur order and duplicate or lose messages"). Taking the
    /// partitions lock for the whole of somebody else's append, on this class,
    /// would be paying a convoy to protect data that is allowed to disappear.
    fn wipe_ring(&self, tenant: &str, name: &str, partition: &str) -> bool {
        let Some(q) = self.lookup(tenant, name, false) else { return false };
        let ring = q.partitions.lock().unwrap().remove(partition);
        let Some(ring) = ring else { return false };
        let freed = ring.lock().unwrap().wipe();
        self.release(tenant, &q, &freed);
        self.metrics.eph_wipes.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// The periodic half of the membership-change wipe (§3.7): drop every ring
    /// whose partition no longer hashes here, whether or not anyone touches it.
    ///
    /// Needed because the lazy wipe in `route` only fires on a request FOR that
    /// partition, and after an ownership move the requests go somewhere else by
    /// definition — so without this pass the memory of a moved partition would be
    /// freed by nothing until the queue itself was idle-collected (and never, for
    /// a declared queue). Runs on the config refresh's cadence, so the bound on
    /// stranded memory is one refresh interval, the same bound §10 Q4 accepts for
    /// a lost broadcast.
    pub fn reap_foreign(&self) -> u64 {
        // ONE membership snapshot for the whole pass (see `owner_in`), and the
        // no-mesh short-circuit in the same line.
        let Some(cands) = self.candidates() else { return 0 };
        let snapshot: Vec<(String, Arc<EqQueue>)> = self
            .queues
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let mut n = 0u64;
        for (key, q) in &snapshot {
            let (tenant, _) = crate::handlers::split_tenant_queue(key);
            for p in q.partition_names() {
                if Self::owner_in(&cands, tenant, &q.name, &p).is_some()
                    && self.wipe_ring(tenant, &q.name, &p)
                {
                    n += 1;
                }
            }
        }
        n
    }

    /// §3.5 — broadcast one admin op to every peer, fire and forget. No-op with
    /// no mesh, which is why the three admin verbs can call it unconditionally.
    pub fn broadcast_admin(&self, op: &str, tenant: &str, queue: &str) {
        if let Some(m) = self.mesh.get() {
            m.send_eph_admin(op, tenant, queue);
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
                q.dropped_retry.fetch_add(sweep.exhausted, Ordering::Relaxed);
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
        let tenant_ok = self
            .with_tenant(tenant, |t| t.bytes.charge(total))
            .unwrap_or(false);
        if !tenant_ok {
            q.bytes.refund(total);
            q.length.refund(count);
            return Err(Refusal::TenantQuota);
        }
        if !self.global.charge(total) {
            q.bytes.refund(total);
            q.length.refund(count);
            let _ = self.with_tenant(tenant, |t| t.bytes.refund(total));
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
        let _ = self.with_tenant(tenant, |t| t.bytes.refund(f.bytes));
        self.global.refund(f.bytes);
        if f.by_ttl > 0 {
            self.metrics.eph_dropped_ttl.fetch_add(f.by_ttl, Ordering::Relaxed);
            q.dropped_ttl.fetch_add(f.by_ttl, Ordering::Relaxed);
        }
        if f.by_bounds > 0 {
            self.metrics.eph_dropped_bounds.fetch_add(f.by_bounds, Ordering::Relaxed);
            q.dropped_bounds.fetch_add(f.by_bounds, Ordering::Relaxed);
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
                    q.dropped_retry.fetch_add(sweep.exhausted, Ordering::Relaxed);
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
            q.dropped_retry.fetch_add(exhausted, Ordering::Relaxed);
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
                    if ls.exhausted > 0 {
                        q.dropped_retry.fetch_add(ls.exhausted, Ordering::Relaxed);
                    }
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
            let _ = self.with_tenant(tenant, |t| {
                let _ = t.queues.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    Some((n - 1).max(0))
                });
            });
            debug_assert_eq!(q.bytes.used(), 0);
        }
        gone.len() as u64
    }

    // ---------------------------------------------------------- inspection

    /// (messages, bytes) held by one queue, straight off the two BUDGET
    /// counters — the numbers the bounds of §1.6 are enforced against, which is
    /// a different question from the cursor arithmetic `depth_detail` publishes.
    /// Reads two atomics and takes no lock.
    ///
    /// No production caller today: the HTTP depth read needs the per-group and
    /// per-partition breakdown, so it goes through `depth_detail`. This is what
    /// the engine's own suite asserts occupancy with, and what the mesh phase
    /// will answer a peer's cheap depth probe from.
    #[allow(dead_code)]
    pub fn depth(&self, tenant: &str, name: &str) -> Option<(i64, i64)> {
        self.lookup(tenant, name, false).map(|q| (q.length.used(), q.bytes.used()))
    }

    /// Per-partition, per-group backlog for `GET .../:queue/depth` (§3.1).
    ///
    /// `None` when the queue does not exist HERE, which the handler turns into
    /// the same 404 shape as the durable depth read. It is the honest answer for
    /// this class: an implicit queue that has been idle-collected is gone, and
    /// saying "0" would present a collected inbox as an empty live one.
    pub fn depth_detail(&self, tenant: &str, name: &str, group: Option<&str>) -> Option<Depth> {
        let q = self.lookup(tenant, name, false)?;
        let group = group.filter(|g| !g.is_empty());
        let mut parts: Vec<PartitionDepth> = Vec::new();
        let mut groups: Vec<GroupDepth> = Vec::new();
        let mut seen_groups: HashMap<String, (i64, u64)> = HashMap::new();
        for p in q.partition_names() {
            let Some(ring) = q.ring(&p) else { continue };
            let r = ring.lock().unwrap();
            parts.push(PartitionDepth {
                pending: r.pending_for(group),
                bytes: r.bytes,
                partition: p,
            });
            for (name, &gi) in r.gix.iter() {
                let e = seen_groups.entry(name.clone()).or_insert((0, 0));
                e.0 += r.pending_of(gi);
                e.1 += r.groups[gi as usize].skipped;
            }
        }
        parts.sort_by(|a, b| a.partition.cmp(&b.partition));
        for (name, (pending, skipped)) in seen_groups {
            groups.push(GroupDepth { group: name, pending, skipped });
        }
        groups.sort_by(|a, b| a.group.cmp(&b.group));
        Some(Depth {
            queue: q.name.clone(),
            declared: q.declared.load(Ordering::Relaxed),
            pending: parts.iter().map(|p| p.pending).sum(),
            partitions_pending: parts.iter().filter(|p| p.pending > 0).count() as i64,
            bytes: q.bytes.used(),
            partitions: parts,
            groups,
        })
    }

    /// Every ephemeral queue of ONE tenant: the declared ones (loaded at boot or
    /// declared since) and the live implicit ones, in one list.
    ///
    /// They come from the same map because a declared config IS a queue here —
    /// the boot load calls `set_config`, which vivifies it empty (§1.2: declared
    /// configuration survives, contents do not). There is therefore no merge to
    /// get wrong, and no way for the list to show a declared queue that the
    /// engine would not actually serve.
    pub fn list(&self, tenant: &str) -> Vec<QueueStat> {
        let prefix = crate::handlers::tenant_queue_key(tenant, EPH_PREFIX);
        let snapshot: Vec<Arc<EqQueue>> = self
            .queues
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .collect();
        let mut out: Vec<QueueStat> = snapshot
            .iter()
            .map(|q| {
                let (bounds, ttl, retry) = q.drops();
                let cfg = *q.config.lock().unwrap();
                QueueStat {
                    name: q.name.clone(),
                    declared: q.declared.load(Ordering::Relaxed),
                    depth: q.length.used(),
                    bytes: q.bytes.used(),
                    partitions: q.partitions.lock().unwrap().len() as i64,
                    groups: q.group_count() as i64,
                    dropped_bounds: bounds,
                    dropped_ttl: ttl,
                    dropped_retry: retry,
                    config: cfg,
                }
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// §3.1 `reset` — drop every message, void every lease, rewind every cursor.
    /// Returns how many messages were dropped, or `None` when the queue is not
    /// here. Legal only because of §1.2, and it is the one operation on this
    /// class that is destructive ON PURPOSE rather than by contract.
    ///
    /// The cursors are rewound TO THE TAIL and not to zero: the seqs are gone,
    /// so a cursor pointing below the tail would make every group replay a range
    /// that no longer exists. A group's own `attempts` bookkeeping goes with it,
    /// because the messages those attempts counted are not coming back.
    pub fn reset(&self, tenant: &str, name: &str) -> Option<i64> {
        let q = self.lookup(tenant, name, false)?;
        let mut freed = Freed::default();
        for p in q.partition_names() {
            let Some(ring) = q.ring(&p) else { continue };
            let mut r = ring.lock().unwrap();
            freed.merge(r.wipe());
        }
        let dropped = freed.count;
        self.release(tenant, &q, &freed);
        q.touch(crate::util::now_epoch_ms());
        Some(dropped)
    }

    /// §3.1 `delete` — reset, then forget the queue entirely.
    ///
    /// `false` when there was nothing here, and that is NOT an error (the house
    /// rule the durable queue delete follows: the status describes the outcome of
    /// the CALL, never the verdict of the predicate). The durable half — the
    /// declared config row — is dropped by the handler through `db.rs`; this is
    /// only the RAM half.
    pub fn remove(&self, tenant: &str, name: &str) -> bool {
        let Some(dropped) = self.reset(tenant, name) else { return false };
        let _ = dropped;
        let key = Self::qkey(tenant, name);
        let gone = {
            let mut m = self.queues.lock().unwrap();
            let gone = m.remove(&key).is_some();
            let n = m.len() as i64;
            drop(m);
            self.metrics.eph_queues.store(n, Ordering::Relaxed);
            gone
        };
        if gone {
            let _ = self.with_tenant(tenant, |t| {
                let _ = t.queues.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    Some((n - 1).max(0))
                });
            });
        }
        gone
    }

    /// §10 Q4 — the ghost-queue backstop for a LOST `delete` broadcast.
    ///
    /// Given the complete set of queue names this tenant still has declared rows
    /// for, forget every DECLARED queue of that tenant which is not in it. That
    /// is the bound the plan promises for a dropped frame: a peer that missed a
    /// `delete` keeps the queue for at most one refresh interval, not for ever.
    ///
    /// ONLY THE DECLARED TIER. An implicit queue has no row BY DEFINITION (§1.1),
    /// so "absent from the row set" says nothing about it — dropping implicit
    /// queues here would delete every live req/reply inbox on the cell twice a
    /// minute. Their lifecycle is idle GC and nothing else.
    ///
    /// The caller must pass a set read in ONE successful list: a partial or
    /// failed read must never reach this function, because "no rows" and "I could
    /// not read the rows" would then be the same instruction.
    ///
    /// Race, stated rather than papered over: a `configure` that commits its row
    /// between this tenant's list read and this call has its RAM half dropped
    /// here and is re-vivified by the next refresh. It costs a declared-but-empty
    /// queue its config for one interval — never a message, because a queue that
    /// was declared moments ago has none.
    pub fn drop_undeclared(&self, tenant: &str, present: &std::collections::HashSet<String>) -> u64 {
        let prefix = crate::handlers::tenant_queue_key(tenant, EPH_PREFIX);
        let doomed: Vec<String> = self
            .queues
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, q)| {
                k.starts_with(&prefix)
                    && q.declared.load(Ordering::Relaxed)
                    && !present.contains(&q.name)
            })
            .map(|(_, q)| q.name.clone())
            .collect();
        let mut n = 0u64;
        for name in doomed {
            if self.remove(tenant, &name) {
                n += 1;
            }
        }
        n
    }

    /// The tenants a grant row exists for, plus whatever the caller adds. The
    /// config refresh uses it to decide WHOSE declared configs to load: the
    /// per-tenant list SP takes a tenant, and with `require_grant` on a tenant
    /// without a row cannot declare anything anyway (§2).
    pub fn granted_tenants(&self) -> Vec<String> {
        self.tenants
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, t)| t.granted.load(Ordering::Relaxed))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Adopt the grant table (§1.6 rung 2). The ONLY writer of the allowance
    /// fields, called by `refresh_once` below.
    ///
    /// A row that has DISAPPEARED must revoke, not linger: with `require_grant`
    /// on, deleting a row is how the control plane turns the feature off for a
    /// tenant, and a broker that kept the last grant it saw would keep serving a
    /// customer whose plan ended. So every known tenant is cleared first and only
    /// the rows present re-grant. Occupancy is NOT touched — lowering a cap below
    /// what is already held blocks the next arrival and evicts nothing
    /// (`Budget::set_cap`), because this budget bounds admission, never contents.
    pub fn apply_grants(&self, rows: Vec<Grant>) {
        let mut m = self.tenants.lock().unwrap();
        for t in m.values() {
            t.granted.store(false, Ordering::Relaxed);
            t.enabled.store(true, Ordering::Relaxed);
            t.bytes.set_cap(0);
            t.queues_cap.store(0, Ordering::Relaxed);
            t.rate_per_s.store(0, Ordering::Relaxed);
        }
        let cap = self.knobs.max_tenants;
        let burst = self.knobs.burst;
        for r in rows {
            if !m.contains_key(&r.tenant) && m.len() >= cap {
                // The map denies rather than evicting (see `with_tenant`). A
                // grant row that does not fit is a configuration the operator can
                // see and fix; silently dropping a LIVE tenant to make room is
                // not.
                continue;
            }
            let e = m.entry(r.tenant.clone()).or_insert_with(|| TenantUsage {
                bytes: Budget::new(0),
                queues: AtomicI64::new(0),
                queues_cap: AtomicI64::new(0),
                granted: AtomicBool::new(false),
                enabled: AtomicBool::new(true),
                rate_per_s: AtomicU32::new(0),
                rate: Mutex::new(RateBucket::new(burst)),
            });
            e.granted.store(true, Ordering::Relaxed);
            e.enabled.store(r.enabled, Ordering::Relaxed);
            e.bytes.set_cap(r.max_bytes.unwrap_or(0).max(0));
            e.queues_cap.store(r.max_queues.unwrap_or(0).max(0), Ordering::Relaxed);
            e.rate_per_s
                .store(r.max_msgs_per_sec.unwrap_or(0).clamp(0, u32::MAX as i64) as u32, Ordering::Relaxed);
        }
    }

    /// The `n` heaviest tenants by ephemeral bytes, plus how many there are in
    /// total. `(tenant, bytes, queues, cap)`.
    ///
    /// A LOG LINE AND NOT A PROMETHEUS LABEL, the §14.1 rule this repo already
    /// applies to the KV occupancy: a per-tenant series on a value the caller
    /// chooses is a cardinality the caller chooses. Ranked by bytes because on
    /// this class bytes are the scarce thing — the whole feature is bounded by
    /// one process's RAM, and the top of this list is who to call.
    pub fn tenant_top(&self, n: usize) -> (Vec<(String, i64, i64, i64)>, usize) {
        let m = self.tenants.lock().unwrap();
        let total = m.len();
        let mut v: Vec<(String, i64, i64, i64)> = m
            .iter()
            .map(|(k, t)| {
                (
                    k.clone(),
                    t.bytes.used(),
                    t.queues.load(Ordering::Relaxed),
                    t.bytes.cap(),
                )
            })
            .collect();
        drop(m);
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| b.2.cmp(&a.2)));
        v.truncate(n);
        (v, total)
    }

    /// How many tenants the grant table last granted — the refresh log line's
    /// number, never a per-tenant Prometheus label (§14.1).
    pub fn granted_count(&self) -> usize {
        self.tenants
            .lock()
            .unwrap()
            .values()
            .filter(|t| t.granted.load(Ordering::Relaxed))
            .count()
    }

    /// How many messages `group` never saw because eviction ran under it, for
    /// ONE partition. The HTTP depth read publishes the per-queue sum instead
    /// (`depth_detail`), so this has no production caller; it is how the engine's
    /// own suite pins the per-partition attribution, which a sum cannot show.
    #[allow(dead_code)]
    pub fn skipped(&self, tenant: &str, name: &str, partition: &str, group: &str) -> u64 {
        let Some(q) = self.lookup(tenant, name, false) else { return 0 };
        let Some(ring) = q.ring(partition) else { return 0 };
        let r = ring.lock().unwrap();
        match r.gix.get(group) {
            Some(&i) => r.groups[i as usize].skipped,
            None => 0,
        }
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
// §2 — the cold path: grants and declared configs, read from Postgres
// ===========================================================================
//
// This is the ONLY code in the feature that awaits, and it runs at boot and then
// on a slow cadence. Push, pop and ack never reach it.

/// Parse `queen.eph_quota_list_v1`'s array.
fn parse_grants(txt: &str) -> Vec<Grant> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(txt) else {
        return Vec::new();
    };
    let Some(arr) = v.as_array() else { return Vec::new() };
    arr.iter()
        .filter_map(|t| {
            let num = |k: &str| t.get(k).and_then(|x| x.as_i64()).filter(|n| *n > 0);
            Some(Grant {
                tenant: t.get("tenant")?.as_str()?.to_string(),
                // Absent `enabled` reads as TRUE: the column is NOT NULL DEFAULT
                // TRUE, so its absence can only mean an older row shape, and a
                // row that exists is a grant.
                enabled: t.get("enabled").and_then(|x| x.as_bool()).unwrap_or(true),
                max_bytes: num("maxBytes"),
                max_queues: num("maxQueues"),
                max_msgs_per_sec: num("maxMsgsPerSec"),
            })
        })
        .collect()
}

/// Parse `queen.eph_config_list_v1`'s array into (queue, options) pairs.
///
/// UNKNOWN KEYS ARE IGNORED HERE, where the `configure` verb rejects them with a
/// 400. That asymmetry is deliberate and one-directional: the verb is the gate
/// (nothing reaches the table without passing it), and the boot load is reading
/// back rows that a FUTURE broker version may have written with options this one
/// does not know. Refusing them would make a rolling downgrade lose the whole
/// queue's configuration instead of the one option it cannot honour.
fn parse_options(v: &serde_json::Value) -> QueueOptions {
    let mut o = QueueOptions::default();
    let g = |k: &str| v.get(k).and_then(|x| x.as_i64());
    o.max_bytes = g("maxBytes");
    o.max_length = g("maxLength");
    o.ttl_ms = g("ttlSeconds").map(|s| s.saturating_mul(1000));
    o.lease_ms = g("leaseSeconds").map(|s| s.saturating_mul(1000));
    o.retry_limit = g("retryLimit").map(|n| n.clamp(0, u32::MAX as i64) as u32);
    o.policy = v.get("policy").and_then(|x| x.as_str()).and_then(Policy::parse);
    o.window = v.get("windowBuffer").and_then(|w| {
        let ms = w.get("ms").and_then(|x| x.as_u64()).unwrap_or(0);
        let count = w.get("count").and_then(|x| x.as_u64()).unwrap_or(0) as usize;
        (ms > 0 || count > 0).then_some(Window { ms, count })
    });
    o
}

/// One refresh of both cold-path reads.
///
/// SEPARATED FROM THE LOOP so the boot can await exactly one before the listener
/// opens — the `quota::refresh_once` rule, and it matters more here: with
/// `require_grant` on, an empty grant map DENIES, so a cell that started serving
/// before its first read would answer `feature_gated` to every tenant for a
/// refresh period on every single rollout.
///
/// WHOSE CONFIGS ARE LOADED, and why it is not "everyone's". The list SP is
/// per-tenant (§2) and there is deliberately no cell-wide config scan: the set
/// of tenants that can HAVE a declared queue is the set with a grant row, plus
/// the default tenant (which is the only tenant at all when tenancy is off). A
/// tenant with no grant row and tenancy on cannot reach `configure` — rung 2
/// refuses it — so its declared configs cannot exist. When tenancy is off,
/// `require_grant` is off too and the default tenant is everybody.
pub async fn refresh_once(
    eph: &Ephemeral,
    pool: &deadpool_postgres::Pool,
    max_tenants: usize,
) -> Result<(usize, usize), String> {
    let client = pool.get().await.map_err(|e| e.to_string())?;
    let cap = max_tenants.clamp(1, i32::MAX as usize) as i32;
    let txt = crate::db::eph_quota_list(&client, cap).await.map_err(|e| {
        // Name the 42883 case: it is the broker-new/database-old shape and its
        // remedy is one environment variable away.
        match e.as_db_error().map(|d| d.code().code().to_string()) {
            Some(code) if code.starts_with("42") => format!(
                "{code}: queen.eph_quota_list_v1 is missing or does not match — apply the \
                 schema (QUEEN_APPLY_SCHEMA=1) and restart"
            ),
            _ => e.to_string(),
        }
    })?;
    let grants = parse_grants(&txt);
    let n_grants = grants.len();
    eph.apply_grants(grants);

    let mut tenants = eph.granted_tenants();
    if !tenants.iter().any(|t| t == crate::config::DEFAULT_TENANT) {
        tenants.push(crate::config::DEFAULT_TENANT.to_string());
    }
    let mut n_configs = 0usize;
    for t in tenants {
        let txt = match crate::db::eph_config_list(&client, &t, cap).await {
            Ok(v) => v,
            // One tenant's read failing must not abort the others': a malformed
            // tenant id in the grant table (the column is a uuid, but the SP
            // casts) would otherwise cost every OTHER tenant its configuration.
            Err(e) => return Err(e.to_string()),
        };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) else { continue };
        let Some(arr) = v.as_array() else { continue };
        // §10 Q4 — the row set, for the ghost backstop below. Built from the
        // SAME read that applies the configs, so the two can never disagree
        // about what the table said.
        let mut present: std::collections::HashSet<String> =
            std::collections::HashSet::with_capacity(arr.len());
        for row in arr {
            let Some(queue) = row.get("queue").and_then(|x| x.as_str()) else { continue };
            let opts = row.get("options").map(parse_options).unwrap_or_default();
            // `declared = true` vivifies the queue EMPTY (§1.2): the
            // configuration is what survives a restart, never the contents.
            eph.set_config(&t, queue, opts, true);
            present.insert(queue.to_string());
            n_configs += 1;
        }
        // The other direction, and the one the mesh phase needs: a `delete` whose
        // broadcast this broker never received left a declared queue here whose
        // row is gone. One refresh interval later it is gone here too (§10 Q4).
        //
        // BOUNDED READ CAVEAT: the list SP caps at `cap` rows. A tenant with more
        // declared queues than that would see the overflow treated as deleted, so
        // the cap is also the ceiling on declared queues per tenant — the same
        // number `max_tenants` bounds the grant read by, and orders of magnitude
        // above the tier this class is built for (implicit inboxes are the
        // cardinality story, §1.1).
        if arr.len() < cap as usize {
            let dropped = eph.drop_undeclared(&t, &present);
            if dropped > 0 {
                tracing::info!(
                    target: "ephemeral", tenant = %t, dropped,
                    "dropped declared ephemeral queues whose row is gone (missed delete)"
                );
            }
        }
    }
    Ok((n_grants, n_configs))
}

/// §3.5 — apply a peer's `config_set` broadcast: re-read THAT queue's declared
/// row and hand it to the engine.
///
/// One row, not the whole tenant's list: the frame names the queue, and a full
/// list read per admin verb would make a script that declares a thousand queues
/// into a thousand full scans on every broker. It is also the first caller of
/// `db::eph_config_get`, which existed for exactly this.
///
/// A MISSING ROW IS A NO-OP and not a delete. `configure` writes the row before
/// it broadcasts, so `None` here can only mean the row was removed in between —
/// and the `delete` that removed it broadcasts its own frame. Treating an absent
/// row as an instruction to drop would make this function a second, weaker
/// delete path racing the real one; the refresh above is the backstop that
/// converges either way.
pub async fn reload_config(
    eph: &Ephemeral,
    pool: &deadpool_postgres::Pool,
    tenant: &str,
    queue: &str,
) -> Result<bool, String> {
    let client = pool.get().await.map_err(|e| e.to_string())?;
    let Some(txt) = crate::db::eph_config_get(&client, tenant, queue)
        .await
        .map_err(|e| e.to_string())?
    else {
        return Ok(false);
    };
    let v: serde_json::Value = serde_json::from_str(&txt).map_err(|e| e.to_string())?;
    let opts = v.get("options").map(parse_options).unwrap_or_default();
    eph.set_config(tenant, queue, opts, true);
    Ok(true)
}

static EPH_REFRESH_ERR: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// The periodic re-read, on the SAME cadence knob the kv/timers gate uses
/// (`QUEEN_KV_QUOTA_REFRESH_MS`).
///
/// ONE KNOB AND NOT A SECOND ONE, deliberately. The knob's meaning is "how long a
/// grant change takes to reach a broker", and that is one property of the cell,
/// not one per feature: an operator who tuned the KV cadence and found the
/// ephemeral grants still stale a minute later would be reading a knob that lied
/// about its own scope. It is also the cadence that backstops a lost `reset` /
/// `delete` broadcast when the mesh phase lands (§10 Q4).
///
/// Returns the handle so the embedded broker can abort it on `shutdown()`.
pub fn spawn_refresh(
    eph: Arc<Ephemeral>,
    pool: deadpool_postgres::Pool,
    interval_ms: u64,
    max_tenants: usize,
) -> tokio::task::JoinHandle<()> {
    let interval = std::time::Duration::from_millis(interval_ms.max(1000));
    tracing::info!(
        target: "ephemeral",
        interval_ms = interval.as_millis() as u64,
        max_tenants,
        require_grant = eph.knobs.require_grant,
        "ephemeral grant/config refresh started"
    );
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            // §3.7 — the periodic half of the membership-change wipe, run BEFORE
            // the database read and unconditionally: it needs no connection, and
            // a cell whose pool is down must still stop holding rings it no
            // longer owns. Zero work and zero allocation with no mesh.
            let wiped = eph.reap_foreign();
            if wiped > 0 {
                tracing::info!(
                    target: "ephemeral", rings = wiped,
                    "dropped ephemeral rings whose partition moved to another broker"
                );
            }
            match refresh_once(&eph, &pool, max_tenants).await {
                Ok((g, c)) => tracing::debug!(
                    target: "ephemeral", grants = g, configs = c, "ephemeral grants refreshed"
                ),
                Err(e) => {
                    // Logged and swallowed. A stale grant map UNBLOCKS nothing:
                    // the caps a broker already adopted stay in force, and a
                    // tenant that was refused stays refused — which is the safe
                    // direction for a budget paid in RAM.
                    if let Some(suppressed) = EPH_REFRESH_ERR.tick_now() {
                        tracing::warn!(
                            target: "ephemeral", error = %e, suppressed,
                            "ephemeral grant refresh failed; the last grants stay in force"
                        );
                    }
                }
            }
        }
    })
}

// ===========================================================================
// Tests (EPHEMERAL_QUEUES.md §7.1, the "pure logic" half)
// ===========================================================================

#[cfg(test)]
#[path = "tests_unit/ephemeral_engine.rs"]
mod ephemeral_engine_tests;

// §3.7 — the placement half: the rendezvous hash, the single-broker
// short-circuit, the ownership-move wipe and the ghost backstop.
#[cfg(test)]
#[path = "tests_unit/ephemeral_placement.rs"]
mod ephemeral_placement_tests;
