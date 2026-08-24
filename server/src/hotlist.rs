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
//! ## Keying (Track B, PLAN_QUEEN_PROXY_CLOUD.md §5)
//! Every `qkey` parameter below is the COMPOSITE `handlers::tenant_queue_key(tenant,
//! queue)` = `"<tenant>\x1f<queue>"`, never a bare queue name: on a shared cell the
//! names `orders` / `workers` collide across tenants, and a shared ring means one
//! tenant's pop checks out (and its `Empty`/leased `Took` checkin then clears or
//! wheels) another tenant's candidate. The module treats the key as OPAQUE; only the
//! display/match sites (`traced`, the trace prints, `ring_sizes`) split it back with
//! `handlers::split_tenant_queue`. The key MUST be byte-identical to the one the
//! `Notifier` gates use — [`HotList::wake`] forwards it straight into
//! `Notifier::wake_local_hint`, so a mismatch is a silent lost wake.
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
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::sync::Arc;

use crate::notify::Notifier;

// Entry state (one byte in the flat `state` array).
/// Hand the pages a trim just freed back to the OPERATING SYSTEM, not merely back to
/// the allocator.
///
/// Measured on the soak cell 2026-08-24, and it is why this call exists: after the
/// unserved-ring trim dropped 209 rings holding 73 155 ready entries — verified, the
/// hot-list line went `209rings/73155ready` → `0rings/0ready` and stayed there — the
/// broker's RSS did not move at all, flat at 1548 MB across the whole observation. The
/// memory WAS freed and reused (had it not been, RSS would have climbed ~1.5 GB per
/// 30 s cycle instead of ~1 MB), but glibc keeps freed arena pages for its own reuse,
/// and RSS is the number the OOM killer reads. On a 15.6 GiB cell where Postgres alone
/// wants ~10.4 GB, "available for this process to reuse later" is not good enough.
///
/// Called only on a pass that actually evicted something: the walk is not free, and a
/// no-op trim has nothing to release. glibc-only — the libc crate exposes it just there,
/// and a musl or macOS build must still compile, hence the cfg pair.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn release_freed_pages() {
    // SAFETY: malloc_trim takes an integer padding argument and returns an integer. It
    // has no pointer contract and no memory-safety obligation for the caller; the worst
    // it can do is decline to release anything (returns 0).
    unsafe {
        libc::malloc_trim(0);
    }
}

/// Non-glibc builds (musl, macOS): nothing to do — the allocator either returns pages on
/// its own or offers no equivalent knob.
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn release_freed_pages() {}

const IDLE: u8 = 0; // not tracked (not in ready ring / wheel / in-flight)
const READY: u8 = 1; // in the ready ring, claimable now
const WHEEL: u8 = 2; // deferred / leased — promoted to READY at revisit_at
const INFLIGHT: u8 = 3; // checked out by a pop, dispatch in flight

// Sentinel for "no link" in the intrusive ring (u32::MAX).
const NIL: u32 = u32::MAX;

// §6: scheduling padding on every PG-derived deadline (the broker clock only
// schedules the retry; PG remains the visibility authority).
// Wheel-park padding: absorbs PG<->broker clock skew + commit visibility lag.
// 300 is the distributed-era calibration; on a single-host cell skew is ~0 and
// QUEEN_HOTLIST_pad_ms() can drop it (2026-08-03 timer review).
// Safety-floor telemetry (2026-08-03 timer review): how often the class-B
// timers actually fire. Sampled summary ~1 line / 10s via obs::Sampler.
pub static FLOOR_LEASED_PARKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static FLOOR_PROMOTE_CLEAR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static FLOOR_PROMOTE_REQUEUE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
fn floor_summary() {
    static S: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
    if S.tick_now().is_some() {
        use std::sync::atomic::Ordering::Relaxed;
        tracing::info!(target: "hotlist",
            leased_parks = FLOOR_LEASED_PARKS.load(Relaxed),
            promote_clear = FLOOR_PROMOTE_CLEAR.load(Relaxed),
            promote_requeue = FLOOR_PROMOTE_REQUEUE.load(Relaxed),
            "floor timers");
    }
}

fn pad_ms() -> i64 {
    static V: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("QUEEN_HOTLIST_PAD_MS").ok()
            .and_then(|v| v.parse().ok()).unwrap_or(300)
    })
}
// Cap on how long a leased entry is parked in the wheel before it is re-probed.
// The lease-expiry deadline is only an OPTIMISATION to avoid probing a genuinely
// held partition on every delivery — but a lease is routinely released EARLY by
// an ack, and the ring learns of an early release only two ways: the acking
// pop's `promote_ack`, or a re-probe. `promote_ack` loses a race whenever a
// pop's SQL snapshot still saw the lease live and its `Leased` checkin re-parks
// the entry AFTER the releasing ack already fired: no future ack then references
// that partition and it sits dark until the ORIGINAL lease expiry — up to
// leaseTime, 300s by default (a multi-minute single-partition delivery stall,
// root-caused on the bench VM 2026-07-29). Capping the park turns that unbounded
// stall into at most one empty re-probe per this interval per still-leased
// partition — bounded and self-healing. Fast consumers almost never hit it:
// their ack's promote_ack wins the race and re-arms the entry first.
const MAX_LEASE_REVISIT_MS: i64 = 1000;
// §6: bounded backoff for a revisit on a deferral queue that popped empty.
const REVISIT_MIN_MS: i64 = 50;
const REVISIT_MAX_MS: i64 = 1000;
// §8: the de-phasing span of the periodic reseed floor at the DEFAULT 30s cadence,
// and the unit each ring's fixed phase is drawn in. A fixed random offset per
// (queue,group) de-synchronizes the reseed of many groups that registered together
// (cold start), so the background floor never fires as one synchronized keyset-scan
// storm.
//
// D1 (PLAN_HOTLIST_FOLLOWUP.md): this constant was sized for a 30s cadence and was
// then reused verbatim as the de-phasing of the 300s FULL walk — a tenth of the
// spread it needs. Every ring cold-starts together after a restart, so production's
// 63 rings came due inside the same 15s band every 5 minutes: the average is ~10x
// below the pre-windowing floor, the peak was not. `GroupRing::jitter_ms` now scales
// the span to the interval it actually de-phases; at the 30s cadence the floor below
// keeps it at exactly today's [0, 15_000), so that behaviour is unchanged.
pub const RESEED_JITTER_MS: i64 = 15_000;
// D1: how much of a LONG interval is spent spreading its walks — a fifth, which keeps
// the default full walk inside the 300s + 60s worst-case repair the release notes
// state, and turns 63 co-registered rings from ~4 walks/s in one band into ~1/s.
const RESEED_JITTER_DIV: i64 = 5;

// B2 (PLAN_HOTLIST_FOLLOWUP.md): how many CONSECUTIVE failed walks on one ring are
// worth a WARN. A failed full walk deliberately does not advance `full_reseed_ms`, so
// the ring stays pinned to Full and keeps retrying the expensive query — the
// conservative direction and the pre-windowing behaviour, but until now a silent one:
// nothing said that a ring had been failing its full walk for an hour. Three at the
// 30s floor is ~90s of a ring not reseeding: long enough to sit out a single pool
// hiccup, short enough to beat the 300s full cadence the failure is stalling.
const RESEED_FAIL_WARN_AT: u32 = 3;

/// Does a streak of `n` consecutive failed walks warrant a line?
///
/// Past the third, only the powers of two do. A database refusing this query refuses
/// it for every ring, so "one line per ring per pass" is 63 lines every 30s in
/// production — a flood that buries the cause it is reporting — while a doubling
/// sequence keeps the count itself visible for as long as the stall lasts.
fn reseed_fail_warns(n: u32) -> bool {
    n == RESEED_FAIL_WARN_AT || (n > RESEED_FAIL_WARN_AT && n.is_power_of_two())
}

/// The widest offset [`GroupRing::jitter_ms`] can draw for `interval_ms` — the span
/// itself, which is the de-phasing band every ring's fixed phase is spread across.
///
/// Public because it is half of a CONFIGURATION invariant and not merely a detail of
/// the tick: the longest a ring can go between two consecutive passes is
/// `interval_ms + max_reseed_jitter_ms(interval_ms)`, and `config::from_env` floors the
/// windowed reseed's lookback at exactly that (C1) — a shorter window leaves a band of
/// time no pass ever covers. One definition, so the two cannot drift apart.
pub const fn max_reseed_jitter_ms(interval_ms: i64) -> i64 {
    let scaled = interval_ms / RESEED_JITTER_DIV;
    if scaled > RESEED_JITTER_MS { scaled } else { RESEED_JITTER_MS }
}

/// One (tenant, queue, group) ring whose periodic reseed floor (§8) is due. A named
/// struct, not a tuple: the caller must not be able to mis-order the composite key
/// and the group.
pub struct ReseedDue {
    /// `handlers::tenant_queue_key(tenant, queue)` — the tenant is IN the key.
    pub qkey: String,
    pub group: String,
}

/// Which reseed scan to run for a (queue, group) — see [`HotList::reseed_begin`].
///
/// The two differ ONLY in the lower bound they pin the one reseed statement to
/// (`scan_bounds`): `Full` walks every partition of the queue, `Window(ms)` the ones
/// written in the last `ms`. Both feed rows through `reseed_row`, so every ring-repair
/// behaviour the rows carry (re-add IDLE, reclaim a stranded INFLIGHT, promote a stale
/// WHEEL on a non-deferral queue) is identical between them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReseedMode {
    /// Every partition of the queue. The correctness floor.
    Full,
    /// Only partitions written in the last N ms — sound because a push is the only
    /// thing that can CREATE pendingness, and every push bumps `last_write_at`.
    Window(i64),
}

impl ReseedMode {
    pub fn is_full(self) -> bool {
        matches!(self, ReseedMode::Full)
    }

    /// The (p_window_ms, p_cutoff) pair the walk binds to `log_hotlist_reseed_window_v1`
    /// — the ONE statement both modes run. `Window(ms)` lets the first page derive
    /// `now() - ms` and pins it for the rest of the walk; `Full` pins the bound to
    /// '-infinity' up front, so the window is never read (the SQL's COALESCE takes the
    /// cutoff first). The dedicated full-walk statement, `log_hotlist_reseed_v1`, is no
    /// longer called: under the generic plan `prepare_cached` converges to, it read every
    /// partition in the cell per ring — its header in 004_log_pop.sql has the numbers.
    pub fn scan_bounds(self) -> (i64, Option<&'static str>) {
        match self {
            ReseedMode::Full => (0, Some("-infinity")),
            ReseedMode::Window(ms) => (ms, None),
        }
    }
}

/// A claim on ONE ring for the duration of ONE reseed walk.
///
/// The walk used to re-resolve (queue, group) BY NAME on every row and again when it
/// stamped, so a `forget_group` + recreate mid-walk let the stamp land on a ring that
/// never earned it — the new ring then believed it had just been full-walked and
/// skipped its cold start for up to `hotlist_reseed_full_ms` (B3). The ticket carries
/// the Arcs the walk started on, and [`HotList::reseed_finish`] stamps only if the ring
/// is still the one in the map.
///
/// Holding the `QueueState` Arc also pins it against `evict_idle`, whose
/// `Arc::strong_count == 1` guard cannot fire while a ticket lives — so the QUEUE half
/// of the identity is stable by construction and only the group ring can be swapped
/// underneath a walk.
pub struct ReseedTicket {
    qkey: Box<str>,
    group: Box<str>,
    // Both None ⇔ the hot-list is disabled. `reseed_begin` must not even look the queue
    // up in that case (an unpolled name has to cost nothing), so there is nothing to
    // pin and every ticket method degrades to a no-op.
    qstate: Option<Arc<QueueState>>,
    ring: Option<Arc<GroupRing>>,
    mode: ReseedMode,
    started_ms: i64,
}

impl ReseedTicket {
    fn new(
        qkey: &str,
        group: &str,
        qstate: Arc<QueueState>,
        ring: Arc<GroupRing>,
        mode: ReseedMode,
        started_ms: i64,
    ) -> ReseedTicket {
        ReseedTicket {
            qkey: qkey.into(),
            group: group.into(),
            qstate: Some(qstate),
            ring: Some(ring),
            mode,
            started_ms,
        }
    }

    fn disabled(qkey: &str, group: &str, started_ms: i64) -> ReseedTicket {
        ReseedTicket {
            qkey: qkey.into(),
            group: group.into(),
            qstate: None,
            ring: None,
            mode: ReseedMode::Full,
            started_ms,
        }
    }

    pub fn mode(&self) -> ReseedMode {
        self.mode
    }
    #[allow(dead_code)]
    pub fn is_full(&self) -> bool {
        self.mode.is_full()
    }
    /// The composite key the walk binds to SQL and logs under.
    pub fn qkey(&self) -> &str {
        &self.qkey
    }
    pub fn group(&self) -> &str {
        &self.group
    }
    /// The instant the walk claimed the ring — the one clock every row and the stamp
    /// are dated from, so a slow walk cannot date its own rows after its stamp.
    #[allow(dead_code)]
    pub fn started_ms(&self) -> i64 {
        self.started_ms
    }
}

/// One coalesced hot-list dirty hint: "(tenant,queue)/partition became pending".
///
/// `group` is the SCOPE of the hint and mirrors the scope of the mark that produced
/// it (see `mark_inner`): `None` = "pending for every consumer group" — what a push
/// means, and the only shape a pre-1.0.1 peer can send or understand. `Some(g)` =
/// "pending for group `g` only" — what a seek / consumer-group delete means, where a
/// queue-wide mark would set 9,563 partitions pending on every OTHER group's ring for
/// nothing.
///
/// `group` is part of `Hash`/`Eq`, so `(q,p,None)` and `(q,p,Some(g))` are distinct
/// members and neither masks the other. A push during a seek's flush window produces
/// both; the peer applies both; the ungrouped one is a superset of the grouped one and
/// marking is idempotent, so the result is the ungrouped semantics — never less.
///
/// Field order is load-bearing for the derived `Ord`: (qkey, partition, group) gives
/// tests a stable sort.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DirtyHint {
    /// The composite `handlers::tenant_queue_key(tenant, queue)`; split onto the wire.
    pub qkey: Box<str>,
    pub partition: Box<str>,
    pub group: Option<Box<str>>,
}

impl DirtyHint {
    pub fn new(qkey: &str, partition: &str, group: Option<&str>) -> DirtyHint {
        DirtyHint {
            qkey: qkey.into(),
            partition: partition.into(),
            group: group.map(|g| g.into()),
        }
    }
}

/// One ring's observability snapshot (`ring_sizes`). `ready` is exact; `wheel` is the
/// heap length (an upper bound — it includes not-yet-drained stale entries).
pub struct RingSize {
    pub tenant: String,
    pub queue: String,
    pub group: String,
    pub ready: usize,
    pub wheel: usize,
    /// F1: ms since this ring's last SUCCESSFUL full walk; -1 = never full-walked.
    /// The one number that answers "which mode is this ring in, and when was it last
    /// repaired" without reading Query Insights — a ring pinned to Full by a failing
    /// walk (B2) is invisible without it, because such a ring is typically EMPTY.
    pub full_age_ms: i64,
    /// F1: ms since its last reseed of any mode; -1 = never reseeded.
    pub reseed_age_ms: i64,
}

/// A candidate handed to the handler for one SQL `log_pop_list_v1` call.
#[derive(Clone, Debug)]
pub struct Candidate {
    pub name: String,
    /// epoch snapshot for the empty-path CAS (§4).
    pub epoch: u32,
    /// READY-AGE at checkout: `now - ready_since`, i.e. how long this servable
    /// lane waited for a pop to visit it. The SAME number `take_batch` feeds to
    /// `LapStats::record_age` for the admission arbiter — carried out to the
    /// caller so the POP AUTOPILOT (server/src/pop_autopilot.rs) can steer on the
    /// arbiter's own variable per (queue, group) without measuring it a second
    /// time. Costs one i64 per candidate and no extra lock.
    pub ready_age_ms: i64,
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
///
/// `min_pop_wait_ms` (TASK M) rides along in the same row/fetch/TTL because it is
/// read on exactly the same pop path — it is NOT a deferral: it never changes when
/// a message becomes visible, only how long ONE pop holds an under-full claim
/// back. `is_deferral()` therefore deliberately ignores it.
#[derive(Clone, Copy, Default)]
struct DeferCfg {
    delayed: i32,
    window: i32,
    min_pop_wait_ms: i32,
    fetched_ms: i64,
}

impl DeferCfg {
    #[inline]
    fn is_deferral(&self) -> bool {
        self.delayed > 0 || self.window > 0
    }
}

// ---------------------------------------------------------------- interning

/// Parse a UUID in text form — dashed or not — into its 16 bytes. Allocation-free, and
/// `None` for anything that is not exactly 32 hex digits.
///
/// It exists so the ack bridge can key on the uuid's VALUE rather than its spelling: see
/// [`QueueIntern`] for why 36 bytes of text twice per partition was the single largest
/// line in the broker's memory.
fn parse_uuid16(s: &str) -> Option<[u8; 16]> {
    let mut out = [0u8; 16];
    let mut n = 0usize;
    let mut hi: Option<u8> = None;
    for c in s.bytes() {
        if c == b'-' {
            continue;
        }
        let v = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            b'A'..=b'F' => c - b'A' + 10,
            _ => return None,
        };
        match hi {
            None => hi = Some(v),
            Some(h) => {
                if n == 16 {
                    return None; // too long
                }
                out[n] = (h << 4) | v;
                n += 1;
                hi = None;
            }
        }
    }
    (n == 16 && hi.is_none()).then_some(out)
}

/// Partition name ⇄ dense slot index, plus the partition-id bridge the ack path uses.
///
/// APPEND-ONLY BY DESIGN, and that is the memory bound: a broker's occupancy grows with
/// how many distinct partitions it has ever HEARD OF, not with how much work is pending.
/// On the 2026-08-24 soak cell that ceiling is the whole cell — 827 000 partitions — and
/// it measured ~1.9 KB apiece, roughly six times the structural content, because the
/// excess was allocator overhead and fragmentation across FOUR heap allocations per
/// partition. Both duplications are now gone:
///
///   * the name was stored twice, once as the map key and once in `idx_to_name`. It is
///     now ONE `Arc<str>` shared by both, so cloning the key is a refcount bump.
///   * the partition id was stored twice AND as 36 bytes of uuid TEXT. A uuid is 16
///     bytes, so both copies are now inline arrays: zero heap allocations, and the map
///     key needs no indirection to hash or compare.
///
/// Four allocations per partition became one. An id that does not parse as a uuid simply
/// gets no bridge entry, which the ack path already treats as "unknown partition" and
/// handles by returning — the ack still lands in Postgres, it just skips the broker-side
/// promote.
struct QueueIntern {
    name_to_idx: HashMap<Arc<str>, u32>,
    idx_to_name: Vec<Arc<str>>,
    // partition-id ↔ index, learned lazily for the ack bridge. Keyed by the uuid's 16
    // bytes, never its text.
    id_to_idx: HashMap<[u8; 16], u32>,
    idx_to_id: Vec<Option<[u8; 16]>>,
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
        // ONE allocation for the name: the map key and the reverse vector share it, so
        // the `clone` below is a refcount bump rather than a second copy of the string.
        let a: Arc<str> = Arc::from(name);
        self.name_to_idx.insert(a.clone(), i);
        self.idx_to_name.push(a);
        self.idx_to_id.push(None);
        i
    }
    fn idx_of(&self, name: &str) -> Option<u32> {
        self.name_to_idx.get(name).copied()
    }
    fn name_of(&self, idx: u32) -> Option<&str> {
        self.idx_to_name.get(idx as usize).map(|a| &**a)
    }
    /// Learn (or correct) the id↔slot bridge. The name is interned FIRST and
    /// unconditionally: callers such as `reseed_row` rely on this to seed the ring, so an
    /// id that cannot be parsed must still register its partition.
    fn note_id(&mut self, name: &str, id: &str) {
        let i = self.intern(name);
        let Some(bytes) = parse_uuid16(id) else {
            return;
        };
        if self.idx_to_id[i as usize] != Some(bytes) {
            self.id_to_idx.insert(bytes, i);
            self.idx_to_id[i as usize] = Some(bytes);
        }
    }
    fn idx_of_id(&self, id: &str) -> Option<u32> {
        self.id_to_idx.get(&parse_uuid16(id)?).copied()
    }
}

// ----------------------------------------------------------- burst telemetry

/// How an entry ENTERED a ready list — the provenance half of the burst window
/// below. Three sources, because three is what the question has: did a producer
/// make this partition ready, did a scheduled deadline come due, or did the
/// reseed floor learn it from PostgreSQL?
///
/// The mapping of the twelve `ready_push_tail` call sites onto the three, since
/// two of them are not obvious from the name:
///   * `Mark` — `mark_ring`'s four arms AND every `checkin` re-append (a mark that
///     raced the dispatch, a budget-skipped `Requeue`, an auto-ack `Took` re-arm).
///     All of them assert the same fact from the same side: this partition holds
///     data, said by a write or by a claim that did not consume it.
///   * `Wheel` — `wheel_drain_due`, plus `promote_ack`'s WHEEL branch, which is
///     the same promotion with its deadline cut short by an ack.
///   * `Reseed` — `reseed_row`, the only path that learns from PostgreSQL.
///
/// Together they cover EVERY entry into a ready list, so the three window counters
/// sum to the entries that arrived — which is what makes them readable against
/// `ring_depth_max` on the same log line.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReadySrc {
    Mark,
    Wheel,
    Reseed,
}

/// BURST-RESOLVED TELEMETRY (soak cell, 2026-08-24).
///
/// e2e p99 ran 330-470 ms while the client's own push RTT p99 was 46-88 ms. The
/// difference is time an entry spends READY-BUT-UNCLAIMED, and nothing in the log
/// could see it: the `sizes` line samples `ring_sizes` once per 10 s window and the
/// per-lane lines reported ready=0 through that same window, while the global
/// `ring_oldest_ms` ran p99 276 ms / max 387 and `ring_depth` p99 172 / max 393.
/// The bursts live and die INSIDE one sampling interval — a 10 s sample of a 400 ms
/// event is not a measurement of it.
///
/// So these are WINDOWED MAXIMA: `fetch_max`ed on the paths that ALREADY hold the
/// value and reset when the `rates` line is emitted. The cost is one relaxed
/// `fetch_max` per ring transition — no clock read (every caller already carries
/// `now_ms` or is stamping one), no lock of its own (each update happens under a
/// sub-ring lock the caller is holding), no allocation.
///
/// `max_lane_ready` is THE DISCRIMINATOR between the two regimes that a global
/// depth of 393 cannot tell apart:
///   * CONCENTRATED — one lane briefly holds 100+ ready, so the per-request claim
///     WIDTH is the binder (the burst bypass in `pop_autopilot.rs`).
///   * SPREAD — 229 lanes at ~1.7 ready each, so the ~9-slot pop admission lane is,
///     and `pop_wait_max` (admission.rs, same log line) is that regime's witness.
///
/// Global drain is ~9 concurrent pop queries x W=1 x ~7 ms RTT ~= 1100 partitions/s,
/// which is why a spike of 393 costs ~360 ms whichever regime produced it.
///
/// One instance per [`HotList`], not a process static: two hot lists in one test
/// binary must not pollute each other's window (the `FLOOR_*` statics above are the
/// counter-example, and they are cumulative rather than windowed).
pub struct BurstStats {
    /// LIVE count of entries across every ready list of this hot list, maintained
    /// at exactly the two points `SubRing::len_ready` is (+1 in `ready_push_tail`,
    /// -1 in `ready_unlink`) plus the whole-list subtraction in `Drop for SubRing`
    /// — without which `trim_unserved` dropping a standby's 827 000-entry ring
    /// would leak the gauge upward for the process lifetime.
    live_ready: AtomicI64,
    depth_max: AtomicI64,
    oldest_max_ms: AtomicI64,
    lane_ready_max: AtomicI64,
    from_mark: AtomicU64,
    from_wheel: AtomicU64,
    from_reseed: AtomicU64,
}

/// One window's worth of [`BurstStats`], read and reset together.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BurstWindow {
    pub depth_max: u64,
    pub oldest_max_ms: i64,
    pub lane_ready_max: u64,
    pub from_mark: u64,
    pub from_wheel: u64,
    pub from_reseed: u64,
}

impl BurstStats {
    fn new() -> Arc<BurstStats> {
        Arc::new(BurstStats {
            live_ready: AtomicI64::new(0),
            depth_max: AtomicI64::new(0),
            oldest_max_ms: AtomicI64::new(0),
            lane_ready_max: AtomicI64::new(0),
            from_mark: AtomicU64::new(0),
            from_wheel: AtomicU64::new(0),
            from_reseed: AtomicU64::new(0),
        })
    }

    /// One entry entered a ready list. The increment and its `fetch_max` run under
    /// the caller's sub-ring lock, so they cannot interleave with another entry of
    /// the SAME sub-ring; across sub-rings a racing pair only ever loses the lower
    /// of two maxima, which is the direction that cannot invent a burst.
    #[inline]
    fn entered(&self, src: ReadySrc) {
        let live = self.live_ready.fetch_add(1, Ordering::Relaxed) + 1;
        self.depth_max.fetch_max(live, Ordering::Relaxed);
        match src {
            ReadySrc::Mark => &self.from_mark,
            ReadySrc::Wheel => &self.from_wheel,
            ReadySrc::Reseed => &self.from_reseed,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    /// `n` entries left the ready lists — one on an unlink, a whole list when a
    /// sub-ring is dropped. Nothing to `fetch_max`: a departure can never raise a
    /// maximum.
    #[inline]
    fn left(&self, n: i64) {
        if n != 0 {
            self.live_ready.fetch_sub(n, Ordering::Relaxed);
        }
    }

    /// One LANE's instantaneous ready count, from a caller holding it already
    /// (`ready_peek`, and `take_batch`'s pre-claim pass over the sub-rings).
    #[inline]
    pub fn note_lane_ready(&self, ready: usize) {
        self.lane_ready_max
            .fetch_max(ready as i64, Ordering::Relaxed);
    }

    /// The age of a ready entry a caller just read, ms — `ready_peek`'s head age
    /// and `take_batch`'s per-candidate `ready_age_ms`, both already computed.
    #[inline]
    pub fn note_oldest_ms(&self, age_ms: i64) {
        if age_ms > 0 {
            self.oldest_max_ms.fetch_max(age_ms, Ordering::Relaxed);
        }
    }

    /// Read and RESET the window. Called once per `rates` line and nowhere else: a
    /// second reader would silently halve every maximum the first one saw.
    pub fn take_window(&self) -> BurstWindow {
        BurstWindow {
            depth_max: self.depth_max.swap(0, Ordering::Relaxed).max(0) as u64,
            oldest_max_ms: self.oldest_max_ms.swap(0, Ordering::Relaxed).max(0),
            lane_ready_max: self.lane_ready_max.swap(0, Ordering::Relaxed).max(0) as u64,
            from_mark: self.from_mark.swap(0, Ordering::Relaxed),
            from_wheel: self.from_wheel.swap(0, Ordering::Relaxed),
            from_reseed: self.from_reseed.swap(0, Ordering::Relaxed),
        }
    }

    /// The live gauge itself — the instantaneous global ready depth, without the
    /// 10 s sampling `ring_sizes` imposes. Test-only for now (`adjust_count` in
    /// pop_autopilot.rs is the precedent): the log line wants the WINDOWED maximum,
    /// and an unused public reader would be dead code on every build.
    #[cfg(test)]
    pub fn live_ready(&self) -> i64 {
        self.live_ready.load(Ordering::Relaxed)
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
    // Epoch-ms stamp of the entry's LAST transition into the ready list: the
    // difference now - ready_since at claim time is the READY-AGE — how long a
    // servable lane waited for a pop to visit it. This is the lap of the ring
    // measured directly, the latency variable every admission law so far has
    // been blind to (instrumented 2026-08-03).
    ready_since: Vec<i64>,
    // Epoch-ms of the last time something ASSERTED this partition pending — a mark
    // (push, mesh hint, transaction commit) or a reseed row, the only two ways an
    // entry ever enters the ring. Read on exactly one path: the deferral-queue empty
    // verdict, which uses it to tell a partition whose data is merely not visible YET
    // from one that holds nothing at all (see `ghost`).
    pending_since: Vec<i64>,
    ready_head: u32, // shard-local pos, NIL if empty
    ready_tail: u32,
    // (revisit_at snapshot, local pos). Stale entries (state changed or
    // rescheduled) are skipped lazily on drain.
    wheel: BinaryHeap<Reverse<(i64, u32)>>,
    len_ready: usize,
    // The hot list's burst window (see `BurstStats`). Held here rather than passed
    // down from `HotList` on every call because the two mutation points ARE these
    // two methods: an `Arc` in the struct keeps all twelve `ready_push_tail` call
    // sites and `Drop` honest without threading a parameter through any of them.
    burst: Arc<BurstStats>,
}

impl SubRing {
    fn new(burst: Arc<BurstStats>) -> Self {
        SubRing {
            burst,
            next: Vec::new(),
            prev: Vec::new(),
            epoch: Vec::new(),
            drained: Vec::new(),
            ready_since: Vec::new(),
            pending_since: Vec::new(),
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
            self.ready_since.resize(need, 0);
            self.pending_since.resize(need, 0);
        }
    }

    fn ready_push_tail(&mut self, l: u32, src: ReadySrc) {
        self.burst.entered(src);
        self.ready_since[l as usize] = crate::util::now_epoch_ms();
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
        self.burst.left(1);
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
            self.ready_push_tail(l, ReadySrc::Wheel);
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

impl Drop for SubRing {
    /// Every ring-drop path funnels through here — `evict_idle` (which demands an
    /// empty ready list, so 0), `forget_group`, a `QueueState` replaced under a
    /// reseed walk, and above all `trim_unserved`, which drops rings that are FULL
    /// by design (the standby broker of 2026-08-24 held 827 000 ready entries).
    /// Subtracting here rather than at each of those call sites is what makes the
    /// global gauge impossible to leak.
    fn drop(&mut self) {
        self.burst.left(self.len_ready as i64);
    }
}

/// Has a deferral-queue entry outlived the only reason an empty claim is ambiguous
/// on such a queue?
///
/// `Empty` on a queue with `delayedProcessing`/`windowBuffer` has two readings: the
/// partition holds nothing, or it holds something the visibility cut still hides. So
/// the empty path revisits instead of clearing — but until 2026-08-12 it revisited
/// FOREVER, and that made the deferral state a one-way door: no sequence of pops
/// could return an entry to IDLE. Two measured consequences (the E2 tests below):
/// the entry ping-ponged READY→INFLIGHT→WHEEL→READY at the revisit floor — 1,098
/// empty claims per 60s of simulated time on a windowBuffer queue, ~18/s — and,
/// because `evict_idle` requires every entry IDLE, the whole `QueueState` was pinned
/// for the process lifetime. One ordinary push, consumed, was enough to arm it; a
/// peer's seek broadcast arms it on every partition of the queue at once (9,563 in
/// production), on the seeking group's ring — and, from a peer that predates the
/// group-carrying hint, on every ring the queue has. PLAN_HOTLIST_FOLLOWUP.md E2.
///
/// The ambiguity has a deadline, and it is exactly the cut: a partition asserted
/// pending at T can hide data no later than T + delayed/window. Past that, an empty
/// claim with no mark since (the epoch CAS at the call site) says the partition holds
/// nothing — a mesh over-mark, a seek broadcast, a partition another consumer of the
/// group drained — so it clears exactly like a plain queue's. `pending_since` is
/// bumped by every mark and every reseed row, so a partition still being written
/// keeps sliding its own deadline forward and is never called a ghost. The 2×pad
/// grace plus one full `REVISIT_MAX_MS` buys at least one confirming revisit on
/// EITHER queue shape — the windowBuffer backoff is the 50ms floor, the delayed one
/// the 1s clamp — so no single probe that raced the cut on a skewed clock can clear
/// an entry, and the broker has to be ~1.6s out from PG before it can be wrong at all.
///
/// Being wrong here delays, never loses: a wrongly cleared entry is re-added by the
/// next mark, or by the §8 reseed floor — the WINDOWED one, since a partition whose
/// data a windowBuffer is still hiding was by definition written moments ago.
///
/// Reached only from a checkin, i.e. only for an entry a `take_batch` claimed, i.e.
/// only for one a mark or a reseed put in the ring — so `pending_since` is always
/// set by the time this reads it.
fn ghost(sub: &SubRing, local: u32, cfg: &DeferCfg, now_ms: i64) -> bool {
    let cut_ms = cfg.delayed.max(cfg.window) as i64 * 1000;
    now_ms - sub.pending_since[local as usize] >= cut_ms + 2 * pad_ms() + REVISIT_MAX_MS
}

// ------------------------------------------------------------------ group

struct GroupRing {
    subs: Vec<Mutex<SubRing>>,
    // reseed bookkeeping (§8): last reseed of ANY kind (the cadence clock every
    // caller throttles on), and a coarse round-robin cursor for take() fairness
    // across sub-rings.
    reseed_ms: Mutex<i64>,
    // §8: last FULL walk. The windowed reseed (log_hotlist_reseed_window_v1) only
    // sees partitions written inside its window, which covers everything a push can
    // create; the classes it CANNOT see are the ones that make a partition pending
    // with no write at all — a ring entry wrongly cleared, a stranded INFLIGHT, a
    // stale WHEEL park, a dropped mesh hint. The full walk is their recovery, so
    // this clock is what bounds how long any of them can hide.
    full_reseed_ms: Mutex<i64>,
    // §8: fixed per-group phase in [0, RESEED_JITTER_MS), scaled by `jitter_ms` into
    // the span of whichever interval is being de-phased, so the periodic reseed floor
    // of co-registered groups lands at staggered instants (never one storm). Applied
    // to BOTH clocks: the full walk is the expensive one, so de-phasing it matters
    // more than de-phasing the windowed pass — which is exactly what a CONSTANT
    // offset stopped doing once the two clocks were 10x apart (D1).
    reseed_phase_ms: i64,
    // B2: consecutive walks on THIS ring that ended on a SQL error, reset by the first
    // one that does not. Per ring rather than global because that is the unit that gets
    // pinned to Full: a queue whose reseed query times out while every other queue's
    // succeeds is invisible in an aggregate. Read and written only by `reseed_finish`.
    reseed_fail_streak: std::sync::atomic::AtomicU32,
    take_cursor: Mutex<usize>,
}

impl GroupRing {
    fn new(shards: usize, burst: &Arc<BurstStats>) -> Self {
        let mut subs = Vec::with_capacity(shards);
        for _ in 0..shards {
            subs.push(Mutex::new(SubRing::new(burst.clone())));
        }
        GroupRing {
            subs,
            reseed_ms: Mutex::new(0),
            full_reseed_ms: Mutex::new(0),
            reseed_phase_ms: (rand::random::<u32>() as i64) % RESEED_JITTER_MS,
            reseed_fail_streak: std::sync::atomic::AtomicU32::new(0),
            take_cursor: Mutex::new(0),
        }
    }

    /// D1: this ring's fixed offset for an interval of `interval_ms`, in
    /// `[0, max(interval_ms / RESEED_JITTER_DIV, RESEED_JITTER_MS))`.
    ///
    /// The floor keeps the 30s cadence byte-identical to the pre-D1 broker (span
    /// 15_000, so the phase IS the offset); a long interval — the 300s full walk —
    /// gets a span proportional to itself, which is what actually spreads the walks
    /// instead of bunching them into one 15s band per period. Monotone in the
    /// interval, so no configuration ends up de-phased LESS than it used to be.
    fn jitter_ms(&self, interval_ms: i64) -> i64 {
        let span = max_reseed_jitter_ms(interval_ms);
        self.reseed_phase_ms.saturating_mul(span) / RESEED_JITTER_MS
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
    // Idle-eviction second chance (CLOCK): set by every `qstate`/`qstate_existing`
    // access (mark, take, checkin, has_ready, reseed, cfg), cleared by each evictor
    // sweep. A queue survives at least one full sweep after its last access, so the
    // effective idle window is [sweep, 2×sweep). A flag, not a timestamp, so the
    // per-access cost is one relaxed store and never a clock read.
    active: std::sync::atomic::AtomicBool,
    // POP LIVENESS — the standby-ring fix (2026-08-24). `active` above answers "did
    // ANYTHING touch this queue", because every accessor funnels through `qstate`:
    // mark, take, checkin, has_ready, reseed, cfg. On a busy cell the answer is always
    // yes, which is why the idle sweep evicts nothing there. This flag is stamped ONLY
    // where a CONSUMER touches the ring (`take_batch`, `has_ready`, `ready_peek`,
    // `checkin`), so its ABSENCE means precisely: this broker served no pop for this
    // queue.
    //
    // Why the distinction is load-bearing. Rings are FILLED by paths independent of
    // this broker's own traffic — the reseed lane's timer and mesh marks from a peer's
    // pushes — and DRAINED only by serving pops. A broker behind an active/passive LB
    // therefore accumulates the monotonic UNION of every (partition, group) that was
    // ever momentarily pending. Measured on the soak cell 2026-08-24: the PASSIVE
    // broker held 827 000 ready entries / 2.12 GB RSS against the active one's ~1 ready
    // / 727 MB, and it was a proximate cause of two global OOMs. `evict_idle` cannot
    // reclaim any of it — it demands `len_ready == 0`, which a ring nobody drains never
    // reaches — and since `QueueIntern` is APPEND-ONLY, dropping the whole QueueState is
    // the only thing that frees the memory at all.
    //
    // A flag and not a timestamp, for the same reason `active` is one: one relaxed store
    // on the pop path, never a clock read. The sweep owns the clock (below).
    served: std::sync::atomic::AtomicBool,
    // Epoch-ms of the first trim pass that found `served` unset; 0 = "being served".
    // Written and read ONLY by `trim_unserved`, which already holds a clock.
    unserved_since_ms: std::sync::atomic::AtomicI64,
    // The owning hot list's burst window, carried so `group` can hand it to a ring
    // it creates without the six `s.group(...)` call sites growing a parameter.
    burst: Arc<BurstStats>,
}

impl QueueState {
    fn new(burst: Arc<BurstStats>) -> Self {
        QueueState {
            burst,
            intern: Mutex::new(QueueIntern::new()),
            cfg: Mutex::new(DeferCfg::default()),
            groups: Mutex::new(HashMap::new()),
            wake_pending: std::sync::atomic::AtomicBool::new(false),
            active: std::sync::atomic::AtomicBool::new(true),
            // Born NOT served, unlike `active` above. `active` is a pure CLOCK with no
            // time term, so a fresh ring needs the birth stamp to survive its first
            // sweep; the trim has an explicit threshold instead, which already
            // guarantees any ring at least `unserved_trim_ms` of life whatever pass
            // first sees it. A birth stamp here would only add a pass of grace nobody
            // asked for, and the first pop stamps it anyway.
            served: std::sync::atomic::AtomicBool::new(false),
            unserved_since_ms: std::sync::atomic::AtomicI64::new(0),
        }
    }
    fn group(&self, group: &str, shards: usize) -> Arc<GroupRing> {
        let mut g = self.groups.lock().unwrap();
        if let Some(r) = g.get(group) {
            return r.clone();
        }
        let r = Arc::new(GroupRing::new(shards, &self.burst));
        g.insert(group.to_string(), r.clone());
        r
    }
}

// ------------------------------------------------------------------ HotList

/// Rolling ready-age / visit statistics for the whole hot list (all rings).
/// Ring buffer + counters, same pattern as metrics::OpMetrics.
pub struct LapStats {
    visits: std::sync::atomic::AtomicU64,
    cands: std::sync::atomic::AtomicU64,
    ages: std::sync::Mutex<Vec<f64>>,
    head: std::sync::atomic::AtomicU64,
}

const LAP_CAP: usize = 4096;

impl LapStats {
    fn new() -> Self {
        LapStats {
            visits: std::sync::atomic::AtomicU64::new(0),
            cands: std::sync::atomic::AtomicU64::new(0),
            ages: std::sync::Mutex::new(vec![-1.0; LAP_CAP]),
            head: std::sync::atomic::AtomicU64::new(0),
        }
    }
    fn record_age(&self, ms: f64) {
        let h = (self.head.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as usize) % LAP_CAP;
        if let Ok(mut a) = self.ages.lock() {
            a[h] = ms.max(0.0);
        }
    }
    fn note_visit(&self, n: usize) {
        self.visits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.cands.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn visits(&self) -> u64 {
        self.visits.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// (visits, candidates, ready-age p50 ms, p95 ms) — counters cumulative,
    /// ages over the rolling window.
    pub fn snapshot(&self) -> (u64, u64, f64, f64) {
        let mut v = { self.ages.lock().unwrap().clone() };
        v.retain(|&x| x >= 0.0);
        let (p50, p95) = if v.is_empty() {
            (0.0, 0.0)
        } else {
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            (
                v[v.len() / 2],
                v[(v.len() * 95 / 100).min(v.len() - 1)],
            )
        };
        (
            self.visits.load(std::sync::atomic::Ordering::Relaxed),
            self.cands.load(std::sync::atomic::Ordering::Relaxed),
            p50,
            p95,
        )
    }
}

pub struct HotList {
    pub lap: LapStats,
    /// The windowed burst maxima + provenance counters (see [`BurstStats`]). Read
    /// and reset once per `rates` line by `obs::spawn_reporter`.
    pub burst: Arc<BurstStats>,
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
    // QUEEN_TENANCY_HEADER. OFF (the OSS default) means no non-default tenant can
    // exist, so a tenant-less peer frame provably addresses `DEFAULT_TENANT` and the
    // legacy fan-out below must NOT run: it would turn one hash lookup into a scan of
    // every ring on a path (rolling HA upgrade) every OSS cluster traverses.
    tenancy: bool,
    // Keyed by the COMPOSITE `tenant_queue_key(tenant, queue)` (see the module note):
    // two tenants sharing a queue name own disjoint rings, interning tables and
    // deferral configs.
    queues: Mutex<HashMap<String, Arc<QueueState>>>,
    // Secondary index, bare queue name → the composite keys holding it. Sole reader is
    // the tenant-less (pre-Track-B peer) mark, which would otherwise scan `queues`
    // whole ON THE MESH INBOUND TASK. Maintained ONLY when `tenancy` is on — with the
    // flag off nothing reads it, so the OSS path pays nothing — and mutated in exactly
    // the two places `queues` is: the create branch of `qstate` and `evict_idle`. It
    // therefore cannot outlive its primary entries (an empty name-set is removed), so
    // it is bounded BY `queues` and adds no second leak.
    by_queue: Mutex<HashMap<String, HashSet<String>>>,
    notifier: OnceLock<Arc<Notifier>>,
    trace_prefix: Option<String>,
    // Coalesced local vuoto→pending transitions, drained by main's mesh flusher (§5),
    // which splits the qkey back onto the wire. Received (remote) marks never enqueue
    // here (no re-broadcast). The element carries the SCOPE of the mark that produced
    // it (`DirtyHint::group`), so a push still means "pending for every group" while a
    // seek's repair costs the peers one ring instead of all of them.
    dirty: Mutex<HashSet<DirtyHint>>,
    // TASK M: has ANY queue on this broker ever been seen with
    // min_pop_wait_time > 0? A monotonic latch, set by set_queue_cfg. It exists
    // so the pop path can answer "is this feature in play at all" with ONE relaxed
    // atomic load instead of taking the global `queues` mutex on every pop
    // attempt: the OSS default is that no queue configures it, and that lane must
    // pay nothing. Never cleared — turning the option back off leaves the broker
    // on the (still correct, per-queue-gated) slower predicate until restart,
    // which is the cheap direction to be wrong in.
    any_min_pop_wait: std::sync::atomic::AtomicBool,
    // Observability. F1 (PLAN_HOTLIST_FOLLOWUP.md): the reseed used to bump ONE
    // counter for both scans, so the 30s log line reported full and windowed passes
    // summed — and the two differ by a factor of 130 in database cost (49ms vs
    // 0.375ms measured in prod on 2026-08-11). The split is what makes the walk's cost
    // readable from the broker instead of from Query Insights. Bumped in exactly one
    // place, `reseed_finish`.
    pub reseeds_full: std::sync::atomic::AtomicU64,
    pub reseeds_window: std::sync::atomic::AtomicU64,
    /// Walks that ended on a SQL error, in either mode. A full walk that keeps failing
    /// keeps its ring pinned to Full (B2) — this is the global signal that says so.
    pub reseeds_failed: std::sync::atomic::AtomicU64,
    /// Walks whose ring was replaced under them (B3): the stamp was discarded, so the
    /// new ring still owes its cold-start full walk.
    pub reseeds_dropped: std::sync::atomic::AtomicU64,
    pub marks_local: std::sync::atomic::AtomicU64,
    pub marks_remote: std::sync::atomic::AtomicU64,
    /// Rings dropped by [`HotList::trim_unserved`] because this broker serves no pops for
    /// them. Reported by the sweep so a trim is never silent — an operator staring at a
    /// shrinking RSS needs to see WHICH mechanism shrank it.
    pub trims_unserved: std::sync::atomic::AtomicU64,
    /// How long a ring may go without a single served pop before the trim drops it, ms.
    /// 0 disables the trim entirely (the kill switch: pre-2026-08-24 behaviour, byte for
    /// byte — no clock read, no walk, no eviction). Set once at boot from
    /// `QUEEN_HOTLIST_UNSERVED_TRIM_MS`; an atomic only so the setter needs no `&mut`.
    unserved_trim_ms: std::sync::atomic::AtomicI64,
}

impl HotList {
    pub fn new(
        enabled: bool,
        shards: usize,
        window_batch: u32,
        mesh_active: bool,
        tenancy: bool,
    ) -> Arc<HotList> {
        Arc::new(HotList {
            lap: LapStats::new(),
            burst: BurstStats::new(),
            enabled,
            shards: shards.max(1),
            window_batch: window_batch.max(1),
            mesh_active,
            tenancy,
            queues: Mutex::new(HashMap::new()),
            by_queue: Mutex::new(HashMap::new()),
            notifier: OnceLock::new(),
            trace_prefix: std::env::var("QUEEN_HOTLIST_TRACE").ok().filter(|v| !v.is_empty()),
            dirty: Mutex::new(HashSet::new()),
            any_min_pop_wait: std::sync::atomic::AtomicBool::new(false),
            reseeds_full: std::sync::atomic::AtomicU64::new(0),
            reseeds_window: std::sync::atomic::AtomicU64::new(0),
            reseeds_failed: std::sync::atomic::AtomicU64::new(0),
            reseeds_dropped: std::sync::atomic::AtomicU64::new(0),
            marks_local: std::sync::atomic::AtomicU64::new(0),
            marks_remote: std::sync::atomic::AtomicU64::new(0),
            trims_unserved: std::sync::atomic::AtomicU64::new(0),
            // Off unless a boot path opts in, so every existing constructor call site
            // (and every test) keeps the historical behaviour until told otherwise.
            unserved_trim_ms: std::sync::atomic::AtomicI64::new(0),
        })
    }

    /// Arm the unserved-ring trim (see [`HotList::trim_unserved`]). Called once at boot
    /// from `QUEEN_HOTLIST_UNSERVED_TRIM_MS`, before any traffic — same shape as
    /// [`HotList::attach_notifier`]. 0 leaves it off.
    pub fn set_unserved_trim_ms(&self, ms: i64) {
        self.unserved_trim_ms.store(ms.max(0), std::sync::atomic::Ordering::Relaxed);
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
    /// beyond one Option check. The prefix is matched against the QUEUE half of the
    /// composite key, so the operator still writes a plain queue prefix.
    pub fn traced(&self, qkey: &str) -> bool {
        match &self.trace_prefix {
            Some(p) => crate::handlers::split_tenant_queue(qkey).1.starts_with(p.as_str()),
            None => false,
        }
    }

    /// The hotlist↔notifier key contract: the gate is keyed by the SAME composite
    /// key as the ring, so a mark wakes exactly the pops parked on that
    /// (tenant, queue) — never another tenant's.
    fn wake(&self, qkey: &str) {
        if let Some(n) = self.notifier.get() {
            // Empty partition hint = wake the queue gate only (no targeted hint).
            n.wake_local_hint(qkey, "");
        }
    }

    /// get-or-create. Every accessor funnels through here (or [`qstate_existing`]), so
    /// this is the ONE place that stamps the second-chance `active` flag the idle
    /// evictor reads — no clock read, no extra lock, and no way to add an access path
    /// that forgets to keep its own state alive.
    fn qstate(&self, qkey: &str) -> Arc<QueueState> {
        let mut q = self.queues.lock().unwrap();
        if let Some(s) = q.get(qkey) {
            s.active.store(true, std::sync::atomic::Ordering::Relaxed);
            return s.clone();
        }
        let s = Arc::new(QueueState::new(self.burst.clone()));
        q.insert(qkey.to_string(), s.clone());
        drop(q);
        if self.tenancy {
            let (_, queue) = crate::handlers::split_tenant_queue(qkey);
            self.by_queue
                .lock()
                .unwrap()
                .entry(queue.to_string())
                .or_default()
                .insert(qkey.to_string());
        }
        s
    }

    fn qstate_existing(&self, qkey: &str) -> Option<Arc<QueueState>> {
        let s = self.queues.lock().unwrap().get(qkey).cloned();
        if let Some(s) = &s {
            s.active.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        s
    }

    // -------------------------------------------------------- queue config

    /// Cache a queue's deferral config (from queen.queues; the handler fetches
    /// it, TTL-throttled). `now_ms` stamps the fetch for the TTL check. Per
    /// (tenant, queue): `delayed`/`windowBuffer` are per-tenant queue config, so a
    /// same-named queue of another tenant must never install its deferral here.
    pub fn set_queue_cfg(&self, qkey: &str, delayed: i32, window: i32, min_pop_wait_ms: i32, now_ms: i64) {
        let s = self.qstate(qkey);
        let mut c = s.cfg.lock().unwrap();
        c.delayed = delayed;
        c.window = window;
        c.min_pop_wait_ms = min_pop_wait_ms;
        c.fetched_ms = now_ms;
        if min_pop_wait_ms > 0 {
            self.any_min_pop_wait
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// TASK M — the cheap global gate (see `any_min_pop_wait`). False ⇒ no queue
    /// on this broker has ever configured a minimum pop wait, so the pop path can
    /// skip the per-queue lookup entirely.
    #[inline]
    pub fn min_pop_wait_in_play(&self) -> bool {
        self.any_min_pop_wait
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Is the cached config fresh enough (within `ttl_ms`)? Absent ⇒ false.
    pub fn cfg_fresh(&self, qkey: &str, now_ms: i64, ttl_ms: i64) -> bool {
        match self.qstate_existing(qkey) {
            Some(s) => {
                let c = s.cfg.lock().unwrap();
                c.fetched_ms != 0 && now_ms - c.fetched_ms < ttl_ms
            }
            None => false,
        }
    }

    fn cfg_of(&self, qkey: &str) -> DeferCfg {
        match self.qstate_existing(qkey) {
            Some(s) => *s.cfg.lock().unwrap(),
            None => DeferCfg::default(),
        }
    }

    /// Should the list pop skip the SQL windowBuffer debounce for this queue?
    /// Yes only when the broker KNOWS windowBuffer>0 (cache hit) and is holding
    /// it in the wheel — otherwise the SQL debounce stays the (safe) floor.
    pub fn skip_window(&self, qkey: &str) -> bool {
        self.cfg_of(qkey).window > 0
    }

    /// TASK M — the queue's configured minimum pop wait, in milliseconds.
    /// 0 (the default, and the value for any queue whose config has not been
    /// fetched yet) means the feature is OFF for this queue and the pop path is
    /// byte-identical to the pre-feature broker.
    pub fn min_pop_wait_ms(&self, qkey: &str) -> i32 {
        self.cfg_of(qkey).min_pop_wait_ms
    }

    // -------------------------------------------------------------- marks

    /// Apply one mark to ONE ring. Returns whether the partition became newly
    /// claimable on it (a vuoto→pending transition, i.e. something to wake for).
    fn mark_ring(
        &self,
        ring: &GroupRing,
        qkey: &str,
        partition: &str,
        idx: u32,
        cfg: &DeferCfg,
        count: u32,
        now_ms: i64,
    ) -> bool {
        let mut woke = false;
        let sh = (idx as usize) % self.shards;
        let local = idx / self.shards as u32;
        let mut sub = ring.subs[sh].lock().unwrap();
        sub.ensure(local);
        if self.trace_prefix.is_some() && self.traced(qkey) {
            let (t, q) = crate::handlers::split_tenant_queue(qkey);
            eprintln!("[hlt] mark q={} p={} st={} tn={} t={}", q, partition,
                sub.state[local as usize], t, crate::hotlist::trace_now_ms());
        }
        sub.epoch[local as usize] = sub.epoch[local as usize].wrapping_add(1);
        sub.batch_count[local as usize] =
            sub.batch_count[local as usize].saturating_add(count);
        // A write restarts the deferral clock the empty verdict reasons about:
        // on a windowBuffer queue THIS mark is what pushes the visibility cut
        // forward, so the ghost test below must never fire on a partition that
        // is still being written to.
        sub.pending_since[local as usize] = now_ms;
        match sub.state[local as usize] {
            IDLE => {
                if cfg.delayed > 0 {
                    // §6: delayed ⇒ wheel at commit_ts + D (broker now ≈ PG
                    // commit; SQL enforces the exact cut, wheel only schedules).
                    let at = now_ms + cfg.delayed as i64 * 1000 + pad_ms();
                    sub.wheel_schedule(local, at);
                } else if cfg.window > 0 {
                    // §6: windowBuffer ⇒ hold until first_mark + W, unless the
                    // batch is already fat (early promotion).
                    if sub.batch_count[local as usize] >= self.window_batch {
                        sub.ready_push_tail(local, ReadySrc::Mark);
                        woke = true;
                    } else {
                        let at = now_ms + cfg.window as i64 * 1000;
                        sub.wheel_schedule(local, at);
                    }
                } else {
                    sub.ready_push_tail(local, ReadySrc::Mark);
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
                    sub.ready_push_tail(local, ReadySrc::Mark);
                    woke = true;
                } else if cfg.window == 0
                    && cfg.delayed == 0
                    && sub.revisit_at[local as usize] <= now_ms + pad_ms()
                {
                    // Plain queue: every WHEEL entry is a lease park shaped
                    // core+PAD (own post-serve backstop `now+lease+PAD`, or a
                    // foreign `until+PAD`, both capped MAX_LEASE_REVISIT_MS).
                    // revisit_at ≤ now+PAD ⟺ the lease core has passed and only
                    // the safety pad remains — and this mark PROVES data is
                    // pending, so promote instead of sitting out the pad.
                    // NEVER promote inside the core: a claim there re-finds the
                    // live lease (`leased` verdict → re-park churn), which is
                    // the race the park exists to avoid. Idempotent by
                    // construction (state flips to READY; later marks take the
                    // READY arm), so no reschedule and no wheel-heap spam.
                    // Canaries for the herd risk: `leased`/`empty` verdict
                    // rates and pop_empty_pct must not rise.
                    sub.revisit_at[local as usize] = 0;
                    sub.ready_push_tail(local, ReadySrc::Mark);
                    woke = true;
                }
            }
            READY => { /* already claimable */ }
            INFLIGHT => { /* epoch bump ⇒ checkin re-appends (§4) */ }
            _ => {}
        }
        woke
    }

    /// `only_group` is the SCOPE of the mark, and the hint it queues carries the same
    /// scope — there is no way to emit a hint that claims more than the mark did:
    ///
    /// * `None` — every EXISTING group ring of the queue. This is what a push means:
    ///   the partition is pending for every consumer group already polling it. A group
    ///   nobody polls has no ring (no allocation) and discovers the partition via
    ///   reseed on first contact (§8).
    /// * `Some(g)` — only `g`'s ring, and only if it already exists. The seek /
    ///   consumer-group-delete repair: those make partitions pending for ONE group, and
    ///   a queue-wide mark would put 9,563 partitions in every other group's ring for
    ///   nothing.
    ///
    /// Returns whether the targeted ring existed (always true for `None`).
    fn mark_inner(
        &self,
        qkey: &str,
        partition: &str,
        count: u32,
        now_ms: i64,
        broadcast: bool,
        wake: bool,
        only_group: Option<&str>,
    ) -> bool {
        if !self.enabled || partition.is_empty() {
            return false;
        }
        // One global `queues` lookup for the whole mark (was two: a separate
        // cfg_of + qstate each locked it — hot under the push rate).
        let s = self.qstate(qkey);
        let cfg = *s.cfg.lock().unwrap();
        let idx = s.intern.lock().unwrap().intern(partition);
        let groups = s.groups.lock().unwrap();
        // R4: a queue with NO ring for what we are marking still has parked pops to
        // wake. A partition-targeted long-poll (handle_pop_partition) parks on the
        // queue GATE and never registers a ring, so gating the wake purely on a ring
        // transition strands a tenant whose only consumer is partition-targeted: its
        // own push would never wake it and it would burn the whole long-poll on the
        // backoff floor. A ring-less mark therefore always counts as a transition —
        // the wake is still coalesced through the tick, so this is one notify per
        // queue per tick, never a per-push storm. A group-scoped mark for a group with
        // no local ring extends that in the same conservative direction: one
        // notify_waiters on an admin path, and every woken pop short-circuits in
        // memory (data.rs) without touching the pool.
        let mut woke;
        let hit;
        match only_group {
            None => {
                woke = groups.is_empty();
                hit = true;
                for ring in groups.values() {
                    woke |= self.mark_ring(ring, qkey, partition, idx, &cfg, count, now_ms);
                }
            }
            Some(g) => {
                // `get`, never `QueueState::group`: a hint must not be able to ALLOCATE
                // per-group state, or a peer — or an untrusted tenant's group name —
                // could pin a ring per name it invents.
                match groups.get(g) {
                    Some(ring) => {
                        hit = true;
                        woke = self.mark_ring(ring, qkey, partition, idx, &cfg, count, now_ms);
                    }
                    None => {
                        hit = false;
                        woke = true;
                    }
                }
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
                    d.insert(DirtyHint::new(qkey, partition, only_group));
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
                self.wake(qkey);
            } else {
                s.wake_pending
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }
        hit
    }

    /// Local mark that ALSO wakes parked pops (transaction commit / DLQ move / any
    /// path that does not separately notify): sets the partition pending on every
    /// group ring, wakes on a vuoto→pending transition, and (with peers) queues a
    /// coalesced mesh dirty hint (§5).
    pub fn mark_local(&self, qkey: &str, partition: &str, count: u32, now_ms: i64) {
        self.mark_inner(qkey, partition, count, now_ms, true, true, None);
    }

    /// Local mark WITHOUT the internal wake — for the push handler, which already
    /// calls notify_pushed_batch (one wake per push, issued AFTER the ring is
    /// marked). Still queues the mesh dirty hint (with peers). Avoids the redundant
    /// second notify_waiters per push (the flag-on push-RTT regression).
    pub fn mark_local_quiet(&self, qkey: &str, partition: &str, count: u32, now_ms: i64) {
        self.mark_inner(qkey, partition, count, now_ms, true, false, None);
    }

    /// Remote mark (a peer's mesh dirty hint): same effect, wakes parked pops (the
    /// receiving broker has no other wake for this), no re-broadcast (§5).
    pub fn mark_remote(&self, qkey: &str, partition: &str, now_ms: i64) {
        self.mark_inner(qkey, partition, 1, now_ms, false, true, None);
    }

    /// One partition made pending for ONE group with no write behind it — a
    /// per-partition seek. Marks that group's ring, wakes, and queues a
    /// group-carrying hint: the targeted twin of [`Self::mark_local`], for the case
    /// where the queue-wide walk-and-broadcast would be 9,563 partitions of work to
    /// repair one cursor. Returns whether this broker held a ring for the group.
    ///
    /// (The per-partition seek that calls it is A2 in PLAN_HOTLIST_FOLLOWUP.md; the
    /// mark and its hint land first because the seek path is only cheap once they exist.)
    #[allow(dead_code)]
    pub fn mark_local_group(&self, qkey: &str, partition: &str, group: &str, now_ms: i64) -> bool {
        self.mark_inner(qkey, partition, 1, now_ms, true, true, Some(group))
    }

    /// A peer's group-carrying hint: marks only that ring, no re-broadcast. An EMPTY
    /// group degrades to [`Self::mark_remote`] — over-marking every ring costs one
    /// empty SKIP LOCKED probe each, while marking a ring named "" would be a silent
    /// no-op, so the safe direction is the wider one.
    pub fn mark_remote_group(&self, qkey: &str, partition: &str, group: &str, now_ms: i64) -> bool {
        if group.is_empty() {
            self.mark_remote(qkey, partition, now_ms);
            return true;
        }
        self.mark_inner(qkey, partition, 1, now_ms, false, true, Some(group))
    }

    /// Remote mark from a peer that sent NO tenant (a pre-Track-B build, i.e. a
    /// rolling upgrade). Runs on the mesh INBOUND connection task, once per frame
    /// ITEM (main drains up to 50k hints per batch), so it may never scan `queues`.
    ///
    /// * Tenancy OFF (the OSS default): no non-default tenant can exist, so the frame
    ///   provably names the default tenant — ONE hash lookup, exactly the pre-Track-B
    ///   cost. Every OSS HA rolling upgrade traverses this branch.
    /// * Tenancy ON: the bare name is genuinely ambiguous, so the only sound reading is
    ///   "some tenant holding this queue name got data" — mark each of them. Attributing
    ///   it to the default tenant would silently drop the wake for every other tenant;
    ///   over-waking only costs redundant empty SKIP LOCKED probes (~0.2ms each, the §1
    ///   "stale in eccesso" contract). The `by_queue` index makes that proportional to
    ///   the tenants ACTUALLY holding the name, not to the whole map.
    pub fn mark_remote_all_tenants(&self, queue: &str, partition: &str, now_ms: i64) {
        if !self.enabled || queue.is_empty() || partition.is_empty() {
            return;
        }
        if !self.tenancy {
            let qkey = crate::handlers::tenant_queue_key(crate::config::DEFAULT_TENANT, queue);
            self.mark_inner(&qkey, partition, 1, now_ms, false, true, None);
            return;
        }
        let keys: Vec<String> = match self.by_queue.lock().unwrap().get(queue) {
            Some(set) => set.iter().cloned().collect(),
            None => return,
        };
        for k in keys {
            self.mark_inner(&k, partition, 1, now_ms, false, true, None);
        }
    }

    /// Group-carrying hint from a peer that sent NO tenant. Same two-lane contract as
    /// [`Self::mark_remote_all_tenants`] — tenancy OFF ⇒ one DEFAULT_TENANT key and no
    /// scan; tenancy ON ⇒ fan out over the `by_queue` index only, never over `queues`,
    /// because this runs on the mesh INBOUND task once per frame item. Returns whether
    /// ANY tenant held a ring for the group.
    pub fn mark_remote_group_all_tenants(
        &self,
        queue: &str,
        partition: &str,
        group: &str,
        now_ms: i64,
    ) -> bool {
        if !self.enabled || queue.is_empty() || partition.is_empty() {
            return false;
        }
        if group.is_empty() {
            self.mark_remote_all_tenants(queue, partition, now_ms);
            return true;
        }
        if !self.tenancy {
            let qkey = crate::handlers::tenant_queue_key(crate::config::DEFAULT_TENANT, queue);
            return self.mark_inner(&qkey, partition, 1, now_ms, false, true, Some(group));
        }
        let keys: Vec<String> = match self.by_queue.lock().unwrap().get(queue) {
            Some(set) => set.iter().cloned().collect(),
            None => return false,
        };
        let mut hit = false;
        for k in keys {
            hit |= self.mark_inner(&k, partition, 1, now_ms, false, true, Some(group));
        }
        hit
    }

    /// Learn a partition's id↔name (from a pop response / reseed) for the ack
    /// bridge (§7 promote-on-ack is keyed by partition id). Per (tenant, queue):
    /// two tenants' same-named partitions have DIFFERENT ids and must not share an
    /// intern slot (the same-name/different-uuid collision).
    pub fn note_partition_id(&self, qkey: &str, name: &str, id: &str) {
        if !self.enabled || name.is_empty() || id.is_empty() {
            return;
        }
        let s = self.qstate(qkey);
        s.intern.lock().unwrap().note_id(name, id);
    }

    /// Ack promote (§7): a completed ack released the lease on (partition_id,
    /// group) — if the entry is still pending (pushes arrived during the lease)
    /// promote it to ready NOW + wake, so the batch is claimable immediately
    /// instead of at lease expiry. No-op when the id/queue/entry is unknown
    /// (the empty-path CAS / reseed floor still covers it).
    pub fn promote_ack(
        &self,
        qkey: &str,
        group: &str,
        partition_id: &str,
        now_ms: i64,
        covered: bool,
    ) {
        if !self.enabled {
            return;
        }
        let s = match self.qstate_existing(qkey) {
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
        if self.traced(qkey) {
            let (_, q) = crate::handlers::split_tenant_queue(qkey);
            eprintln!("[hlt] promote q={} g={} idx={} st={} cov={} bc={} dr={} t={}",
                q, group, idx, sub.state[local as usize], covered,
                sub.batch_count[local as usize], sub.drained[local as usize],
                trace_now_ms());
        }
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
                    FLOOR_PROMOTE_CLEAR.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    sub.revisit_at[local as usize] = 0;
                    sub.ready_push_tail(local, ReadySrc::Wheel);
                    drop(sub);
                    self.wake(qkey);
                    FLOOR_PROMOTE_REQUEUE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                floor_summary();
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
    /// Claim up to `k` ready candidates for one pop serve, stopping EARLY once
    /// the accumulated mark estimate of the claimed entries covers `want`
    /// messages (budget-aware claim, 2026-07-31). Every claimed candidate is
    /// held INFLIGHT for the serve's entire SQL leg, so the claim SIZE is what
    /// bounds concurrent serves: the old fixed over-claim (max_parts x 8,
    /// floor 16) let one serve hold 16-80 partitions of a 100-partition ring -
    /// ~6 serves in flight, measured as the whole T1 pop ceiling (806 serves/s
    /// x 863 msg avg ~ 700k/s while push ran at 1M). Summing `batch_count` -
    /// the same estimate `ready_est` trusts, and biased LOW by construction
    /// (a reseeded backlog carries count 1) - lets a deep partition satisfy
    /// the budget alone (claim 1-2, tens of serves in flight) while a sparse
    /// ring still claims up to `k` exactly as before: a low estimate degrades
    /// to the pre-fix behaviour, never past it. At least one candidate is
    /// always claimed when the ring has one, whatever `want` says.
    pub fn take_batch(
        &self,
        qkey: &str,
        group: &str,
        k: usize,
        want: u32,
        now_ms: i64,
    ) -> Vec<Candidate> {
        if !self.enabled || k == 0 {
            return Vec::new();
        }
        let s = match self.qstate_existing(qkey) {
            Some(s) => s,
            None => return Vec::new(),
        };
        // POP LIVENESS (see `QueueState::served`): the claim itself is the strongest
        // possible evidence that this broker serves this queue.
        s.served.store(true, std::sync::atomic::Ordering::Relaxed);
        let ring = s.group(group, self.shards);
        // Drain due wheel entries into the ready ring across all sub-rings.
        //
        // The `lane_ready` sum rides this pass for free: the burst window's
        // discriminator is ONE lane's instantaneous ready count (see `BurstStats`),
        // and here every sub-ring lock is already held for the wheel probe. Taken
        // AFTER the drain and BEFORE the claim, so it is exactly the set this pop is
        // about to compete for — and it covers pops the autopilot never sees
        // (`ready_peek` is skipped entirely when QUEEN_POP_AUTOPILOT=off).
        let mut lane_ready = 0usize;
        for sub in ring.subs.iter() {
            let mut sg = sub.lock().unwrap();
            if sg.wheel_peek_due(now_ms) {
                sg.wheel_drain_due(now_ms);
            }
            lane_ready += sg.len_ready;
        }
        self.burst.note_lane_ready(lane_ready);
        let intern = s.intern.lock().unwrap();
        let want = want.max(1) as u64;
        let mut got: u64 = 0;
        let mut out = Vec::with_capacity(k);
        let mut cursor = *ring.take_cursor.lock().unwrap();
        let mut empty_streak = 0usize;
        while out.len() < k && (out.is_empty() || got < want) && empty_streak < self.shards {
            let sh = cursor % self.shards;
            cursor = cursor.wrapping_add(1);
            let mut sub = ring.subs[sh].lock().unwrap();
            match sub.ready_pop_head() {
                Some(local) => {
                    empty_streak = 0;
                    let age_ms = now_ms - sub.ready_since[local as usize];
                    self.lap.record_age(age_ms as f64);
                    // Same sample, kept as a windowed MAX rather than a percentile
                    // over a 4096-slot ring: `ready_age_p95` is a distribution over
                    // the claims that happened, which is precisely what a burst
                    // shorter than the window hides (see `BurstStats`).
                    self.burst.note_oldest_ms(age_ms);
                    sub.state[local as usize] = INFLIGHT;
                    // drained is a claim-scoped fact: reset here so only THIS
                    // claim's Took checkin can re-assert it. Without the reset a
                    // racing Leased checkin (10 concurrent poppers, one winner)
                    // can park the entry while a STALE drained=true from an older
                    // cycle survives - the next covered ack would then clear a
                    // backlogged partition (measured 2026-07-24: the intermittent
                    // testConsumerOrderingConcurrency stall).
                    sub.drained[local as usize] = false;
                    let ep = sub.epoch[local as usize];
                    let global = local * self.shards as u32 + sh as u32;
                    if let Some(name) = intern.name_of(global) {
                        // The budget reads the same mark estimate ready_est
                        // walks; saturating, a pathological count cannot wrap.
                        got = got.saturating_add(sub.batch_count[local as usize] as u64);
                        out.push(Candidate {
                            name: name.to_string(),
                            epoch: ep,
                            ready_age_ms: age_ms,
                        });
                    } else {
                        // interning lost the name (should not happen) - drop it.
                        sub.state[local as usize] = IDLE;
                    }
                }
                None => empty_streak += 1,
            }
        }
        *ring.take_cursor.lock().unwrap() = cursor;
        if !out.is_empty() {
            self.lap.note_visit(out.len());
        }
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
    pub fn has_ready(&self, qkey: &str, group: &str, now_ms: i64) -> bool {
        if !self.enabled {
            return false;
        }
        let s = match self.qstate_existing(qkey) {
            Some(s) => s,
            None => return false,
        };
        // POP LIVENESS (see `QueueState::served`). This is the probe a PARKED long-poll
        // re-runs every backoff interval (POP_WAIT_MAX_INTERVAL_MS, default 1s), which is
        // what makes a parked consumer keep its own ring alive without the trim needing
        // to interrogate the notifier.
        s.served.store(true, std::sync::atomic::Ordering::Relaxed);
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

    /// POP AUTOPILOT (server/src/pop_autopilot.rs) — the two ring facts the width
    /// controller steers on, read in ONE pass: how many PARTITIONS are ready in
    /// the (queue, group) ring, and how long the OLDEST of them has been waiting.
    ///
    /// Both are measured over the whole ready set, which is the point. The
    /// controller's first law sampled ready-age at CLAIM time, on the partitions
    /// the sweep visited — so at width 1 it only ever measured the promptly-served
    /// head and reported health while the rest of the ring starved (the bench A/B
    /// of 2026-08-23; the numbers are in the pop_autopilot module note). These two
    /// see the partitions nobody visited, which is the population that matters.
    ///
    /// O(shards), with no scan and no new bookkeeping: `len_ready` is maintained
    /// by the list itself, and the ready list is a FIFO stamped on entry
    /// (`ready_push_tail`), so its HEAD is by construction its oldest entry — one
    /// pointer dereference per sub-ring. Same non-destructive discipline as
    /// [`has_ready`], whose shape this is: one brief lock per sub-ring, no
    /// partition state touched, no ring ever created (a pop of a queue nobody has
    /// pushed to must allocate nothing).
    ///
    /// Deliberately does NOT count due WHEEL entries, which `take_batch` promotes
    /// on its way in: counting them would mean walking the heap, and the bias is
    /// the safe direction — it can only make the controller narrower than the ring
    /// could serve, which the feedback loop then corrects. The caller smooths
    /// both; the instantaneous count is 0 right after a claim took everything
    /// INFLIGHT.
    ///
    /// Returns `(ready partitions, age in ms of the oldest ready entry)`; the age
    /// is 0 when the ring holds nothing ready, which the caller reads as "no
    /// sample" rather than "age zero".
    pub fn ready_peek(&self, qkey: &str, group: &str, now_ms: i64) -> (usize, i64) {
        if !self.enabled {
            return (0, 0);
        }
        let s = match self.qstate_existing(qkey) {
            Some(s) => s,
            None => return (0, 0),
        };
        // POP LIVENESS (see `QueueState::served`): reached only from the autopilot ticket
        // a pop builds, so it is a consumer touch — but it is NOT the load-bearing one,
        // since QUEEN_POP_AUTOPILOT=off removes this call entirely.
        s.served.store(true, std::sync::atomic::Ordering::Relaxed);
        let ring = {
            let g = s.groups.lock().unwrap();
            match g.get(group) {
                Some(r) => r.clone(),
                None => return (0, 0),
            }
        };
        let mut n = 0usize;
        let mut oldest_ms = i64::MAX;
        for sub in ring.subs.iter() {
            let sg = sub.lock().unwrap();
            n += sg.len_ready;
            if sg.ready_head != NIL {
                let since = sg.ready_since[sg.ready_head as usize];
                if since < oldest_ms {
                    oldest_ms = since;
                }
            }
        }
        // Both numbers into the burst window on the way out, where they are already
        // in hand: this is the ONE reader that sees a whole lane's ready set without
        // claiming any of it, so it is where `max_lane_ready` is exact.
        self.burst.note_lane_ready(n);
        if oldest_ms == i64::MAX {
            return (n, 0);
        }
        let oldest = (now_ms - oldest_ms).max(0);
        self.burst.note_oldest_ms(oldest);
        (n, oldest)
    }

    /// TASK M — how many messages a `take_batch` right now would plausibly be
    /// able to claim, from the marks already accumulated in the ring. Same
    /// non-destructive discipline as [`has_ready`]: brief sub-ring locks, no
    /// state touched, no ring ever created.
    ///
    /// This is an ESTIMATE and is only ever used to decide whether to *hold an
    /// under-full pop back*, never to decide what to serve — the SQL claim
    /// remains the sole authority on what exists. Two known biases, both
    /// deliberate and both safe:
    ///   * LOW when a partition entered the ring by reseed / remote mesh hint
    ///     (count 1) while carrying a large backlog. The pop then waits its
    ///     bounded window once; the claim that follows still takes the whole
    ///     backlog. Bounded latency, never lost work.
    ///   * HIGH is impossible to act on wrongly: over-counting only makes the
    ///     pop stop waiting EARLIER, i.e. it degrades to today's behaviour.
    /// Saturating, so a pathological mark count cannot wrap.
    pub fn ready_est(&self, qkey: &str, group: &str, now_ms: i64) -> u64 {
        if !self.enabled {
            return 0;
        }
        let s = match self.qstate_existing(qkey) {
            Some(s) => s,
            None => return 0,
        };
        let ring = {
            let g = s.groups.lock().unwrap();
            match g.get(group) {
                Some(r) => r.clone(),
                None => return 0,
            }
        };
        let mut total: u64 = 0;
        for sub in ring.subs.iter() {
            let sg = sub.lock().unwrap();
            // A due WHEEL entry (expired lease, elapsed delayed/windowBuffer) is
            // about to become claimable and its content is NOT described by
            // batch_count. Report "effectively unbounded" so the caller serves at
            // once instead of holding it: the wait must never add latency to work
            // whose size we cannot see.
            if sg.wheel_peek_due(now_ms) {
                return u64::MAX;
            }
            // Walk the READY list only — O(pending partitions), the same order
            // take_batch itself walks, never O(interned partitions).
            let mut l = sg.ready_head;
            let mut hops = 0usize;
            while l != NIL && hops <= sg.len_ready {
                total = total.saturating_add(sg.batch_count[l as usize] as u64);
                l = sg.next[l as usize];
                hops += 1;
            }
        }
        total
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
        qkey: &str,
        group: &str,
        results: Vec<CheckinResult>,
        now_ms: i64,
        auto_ack: bool,
        lease_ms: i64,
    ) {
        if !self.enabled || results.is_empty() {
            return;
        }
        let cfg = self.cfg_of(qkey);
        let s = match self.qstate_existing(qkey) {
            Some(s) => s,
            None => return,
        };
        // POP LIVENESS (see `QueueState::served`): a consumer returning a lease is as
        // much "serving this queue" as taking one — a slow consumer that acks every few
        // seconds without claiming anything new must not have its ring trimmed.
        s.served.store(true, std::sync::atomic::Ordering::Relaxed);
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
                    // Bounded (MAX_LEASE_REVISIT_MS): our own ack normally promotes
                    // this before the deadline, but if that promote is lost to the
                    // race the cap re-probes within ~1s instead of at lease expiry.
                    let park = (now_ms + lease_ms.max(1) + pad_ms())
                        .min(now_ms + MAX_LEASE_REVISIT_MS);
                    sub.wheel_schedule(local, park);
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
                        sub.ready_push_tail(local, ReadySrc::Mark);
                    }
                }
                Verdict::Requeue => {
                    // Budget/cap-skipped, un-leased: re-append promptly (never window).
                    sub.batch_count[local as usize] = 0;
                    sub.ready_push_tail(local, ReadySrc::Mark);
                }
                Verdict::Leased(until_ms) => {
                    // Never clear a leased candidate (§7): wheel at T + pad, but
                    // BOUNDED by MAX_LEASE_REVISIT_MS. `until_ms` came from the SQL
                    // snapshot and may already be stale (the foreign lease released
                    // between the pop and this checkin); parking at the full expiry
                    // strands the partition until then. The cap re-probes within
                    // ~1s so a stale lease is rediscovered promptly.
                    let park = (until_ms + pad_ms()).min(now_ms + MAX_LEASE_REVISIT_MS);
                    sub.wheel_schedule(local, park);
                    FLOOR_LEASED_PARKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    floor_summary();
                }
                Verdict::Empty => {
                    // Epoch-CAS (§4): clear only if no mark raced.
                    if sub.epoch[local as usize] == r.epoch {
                        if cfg.is_deferral() && !ghost(&sub, local, &cfg, now_ms) {
                            // §6: deferral ⇒ never hard-clear while the partition
                            // may simply not be VISIBLE yet; revisit with a bounded
                            // backoff (a fixed floor here; phase 2 could read the
                            // exact earliest-visible from the SQL meta).
                            let back = if cfg.delayed > 0 {
                                (cfg.delayed as i64 * 1000).clamp(REVISIT_MIN_MS, REVISIT_MAX_MS)
                            } else {
                                REVISIT_MIN_MS
                            };
                            sub.wheel_schedule(local, now_ms + back);
                        } else {
                            // Plain queue, or a deferral entry the grace has
                            // outlived (`ghost`): nothing is pending here.
                            sub.state[local as usize] = IDLE;
                            sub.batch_count[local as usize] = 0;
                            sub.pending_since[local as usize] = 0;
                        }
                    } else {
                        // A mark raced during dispatch — re-append (§4).
                        sub.ready_push_tail(local, ReadySrc::Mark);
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------ reseed

    /// Reseed policy (§8): does this (queue, group) need a keyset reseed now
    /// (ring cold / stale beyond `interval_ms`)? The caller then opens the walk with
    /// [`Self::reseed_begin`], feeds rows via [`Self::reseed_row`] and closes it with
    /// [`Self::reseed_finish`]. Read-only: this stamps nothing.
    pub fn reseed_due(&self, qkey: &str, group: &str, now_ms: i64, interval_ms: i64) -> bool {
        if !self.enabled {
            return false;
        }
        let s = self.qstate(qkey);
        let ring = s.group(group, self.shards);
        let last = *ring.reseed_ms.lock().unwrap();
        last == 0 || now_ms - last >= interval_ms
    }

    /// Open a reseed walk on this (queue, group): pick the scan and CLAIM the ring the
    /// rows will land on. Replaces the old by-name `reseed_mode`; see [`ReseedTicket`]
    /// for why the identity has to be carried rather than re-resolved.
    ///
    /// The mode policy is unchanged. [`ReseedMode::Full`] whenever the ring has never
    /// been full-walked (cold start — a fresh ring, a restarted broker, a group whose
    /// ring was evicted or dropped by `forget_group`, a group whose first contact
    /// bulk-seeded every partition of the queue at once) or whenever the last full walk
    /// is older than `full_interval_ms` plus this ring's phase offset.
    /// [`ReseedMode::Window`] otherwise.
    ///
    /// `full_interval_ms <= 0` pins every scan to Full: the exact pre-windowing
    /// behaviour, kept as a config-only kill switch (`QUEEN_HOTLIST_RESEED_FULL_MS=0`)
    /// so a suspect deployment can revert without a rebuild.
    ///
    /// The cold-start test is `full_reseed_ms == 0`, NOT `reseed_ms == 0`: a ring that
    /// somehow ran a windowed pass first must still owe its full walk.
    pub fn reseed_begin(
        &self,
        qkey: &str,
        group: &str,
        now_ms: i64,
        full_interval_ms: i64,
        window_ms: i64,
    ) -> ReseedTicket {
        // Disabled first, and before qstate: every other hook is a no-op when the
        // hot-list is off, and `qstate` would ALLOCATE a ring for the name — the
        // shared-cell memory bound says an unpolled name must cost nothing.
        if !self.enabled {
            return ReseedTicket::disabled(qkey, group, now_ms);
        }
        let s = self.qstate(qkey);
        let ring = s.group(group, self.shards);
        let mode = if full_interval_ms <= 0 {
            ReseedMode::Full
        } else {
            let last_full = *ring.full_reseed_ms.lock().unwrap();
            if last_full == 0
                || now_ms - last_full >= full_interval_ms + ring.jitter_ms(full_interval_ms)
            {
                ReseedMode::Full
            } else {
                ReseedMode::Window(window_ms)
            }
        };
        ReseedTicket::new(qkey, group, s, ring, mode, now_ms)
    }

    /// A ticket pinned to Full whatever the clocks say — the seek / group-delete repair
    /// path, whose whole reason to exist is that a cursor moved backwards and no
    /// windowed scan can see it.
    pub fn reseed_begin_full(&self, qkey: &str, group: &str, now_ms: i64) -> ReseedTicket {
        if !self.enabled {
            return ReseedTicket::disabled(qkey, group, now_ms);
        }
        let s = self.qstate(qkey);
        let ring = s.group(group, self.shards);
        ReseedTicket::new(qkey, group, s, ring, ReseedMode::Full, now_ms)
    }

    /// A3: a durable repair marker (`queen.hotlist_repairs`, published by the seek and
    /// consumer-group-delete SQL) says a cursor of this (queue, group) moved BACKWARDS
    /// on some broker. Put the ring back where a cold start would leave it — owing a
    /// full walk, and owing it now — so the repair does not wait for the 300s full
    /// cadence on a broker whose mesh hint was dropped.
    ///
    /// It only ever flips the two clocks; the walk itself belongs to the background
    /// floor, which already owns the "one pooled client at a time" discipline.
    ///
    /// NEVER allocates. A marker naming a (queue, group) this broker holds no ring for
    /// is correctly ignored: that group has no ring precisely because nothing here polls
    /// it, and it reseeds cold on first contact (§8). Allocating would let one tenant's
    /// seek loop pin per-group state on every broker of a shared cell.
    ///
    /// `reseed_ms = 1`, not 0, is load-bearing: 0 is the never-reseeded sentinel and
    /// `periodic_reseed_due` skips it deliberately (cold start is the pop path's job,
    /// and claiming it here would be the boot-time scan storm the phase offsets exist to
    /// avoid). A zero would therefore defer the repair until the next pop — which on a
    /// group whose consumers are all parked in a long poll is exactly the stall being
    /// repaired. Backdated to 1, the 2s floor loop claims the ring on its next tick.
    ///
    /// Returns whether a ring was found, so the caller can report what it repaired.
    pub fn request_full_walk(&self, qkey: &str, group: &str) -> bool {
        if !self.enabled {
            return false;
        }
        let Some(s) = self.qstate_existing(qkey) else {
            return false;
        };
        // Non-creating lookup, for the same reason the group-scoped mark uses one.
        let Some(ring) = s.groups.lock().unwrap().get(group).cloned() else {
            return false;
        };
        *ring.full_reseed_ms.lock().unwrap() = 0;
        *ring.reseed_ms.lock().unwrap() = 1;
        true
    }

    /// Queue a coalesced mesh dirty hint for (queue, partition) without touching the
    /// local ring — the peers' half of a reseed that discovered pendingness no push
    /// created (a backward seek). Same bounded set and same no-peers fast path as the
    /// push-path hint in [`Self::mark_inner`].
    ///
    /// `group` scopes the hint exactly as it scopes a mark: `Some(g)` tells a peer that
    /// only `g`'s ring is affected, which is what a seek actually did; `None` is the
    /// queue-wide over-mark, costing a peer at most one empty SKIP LOCKED probe per
    /// ring (§1 "stale in eccesso"). The field rides the EXISTING frame as an optional
    /// JSON key, so a peer on an older binary simply ignores it and does the over-mark
    /// it always did — a new frame type would have been dropped outright.
    pub fn note_dirty(&self, qkey: &str, partition: &str, group: Option<&str>) {
        if !self.enabled || !self.mesh_active || qkey.is_empty() || partition.is_empty() {
            return;
        }
        let mut d = self.dirty.lock().unwrap();
        if d.len() < 200_000 {
            d.insert(DirtyHint::new(qkey, partition, group));
        }
    }

    /// Feed one reseed row: intern the name, remember the id, and mark the partition
    /// pending WITHOUT broadcasting (a reseed is local discovery).
    ///
    /// The row lands on the TICKET's ring, not on whatever a by-name lookup resolves to
    /// now, so a `forget_group` mid-walk cannot make these rows the new ring's problem —
    /// and the per-row `queues`/`groups` lookups disappear (9,563 rows × 2 mutex
    /// acquisitions on production's full walk). `now_ms` is the ticket's start, which is
    /// what every caller passed here anyway.
    pub fn reseed_row(&self, ticket: &ReseedTicket, id: &str, name: &str) {
        let (Some(s), Some(ring)) = (ticket.qstate.as_ref(), ticket.ring.as_ref()) else {
            return;
        };
        let now_ms = ticket.started_ms;
        let cfg = *s.cfg.lock().unwrap();
        // `note_id` interns the name itself, so one lock answers both.
        let idx = {
            let mut it = s.intern.lock().unwrap();
            it.note_id(name, id);
            it.intern(name)
        };
        let sh = (idx as usize) % self.shards;
        let local = idx / self.shards as u32;
        let mut sub = ring.subs[sh].lock().unwrap();
        sub.ensure(local);
        // PG says this partition is pending, which is the same assertion a mark
        // makes — restart the deferral clock so an entry the floor just re-seeded
        // is not cleared by the next empty verdict on its deferral grace.
        sub.pending_since[local as usize] = now_ms;
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
        match sub.state[local as usize] {
            IDLE | INFLIGHT => {
                if cfg.delayed > 0 {
                    sub.wheel_schedule(local, now_ms + cfg.delayed as i64 * 1000 + pad_ms());
                } else {
                    sub.ready_push_tail(local, ReadySrc::Reseed);
                }
            }
            WHEEL if !cfg.is_deferral() => {
                sub.revisit_at[local as usize] = 0;
                sub.ready_push_tail(local, ReadySrc::Reseed);
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
    ///
    /// The returned `qkey` carries the tenant, so the background caller reseeds each
    /// ring under ITS OWN tenant — there is no tenant registry to enumerate and none
    /// is needed: a ring only exists for a (tenant, queue, group) some pop actually
    /// touched, so a tenant with no consumer costs nothing here.
    pub fn periodic_reseed_due(&self, now_ms: i64, interval_ms: i64) -> Vec<ReseedDue> {
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
                if last != 0 && now_ms - last >= interval_ms + ring.jitter_ms(interval_ms) {
                    out.push(ReseedDue { qkey: qname.clone(), group: gname });
                }
            }
        }
        out
    }

    /// Observability (§10 / VM plan): per (tenant, queue, group) ring sizes. The
    /// composite key is split here so the log/metrics surface shows a readable queue
    /// name attributed to the right tenant.
    /// (oldest ready-entry age ms, total ready entries) across every ring.
    /// The ready lists are FIFO per shard, so each shard's HEAD is its oldest
    /// entry: the global oldest is the max over heads — O(rings x shards) and
    /// free of the survivorship bias of age-at-visit sampling (a lane never
    /// visited never samples; the head always counts). This is the pop-lane
    /// controller's objective signal (v5) and the direct answer to "quanto e'
    /// vecchia la entry piu' vecchia".
    pub fn ready_probe(&self) -> (f64, u64) {
        let now = crate::util::now_epoch_ms();
        let queues: Vec<Arc<QueueState>> = {
            self.queues.lock().unwrap().values().cloned().collect()
        };
        let mut oldest_ms = 0.0f64;
        let mut depth = 0u64;
        for s in queues {
            let groups: Vec<Arc<GroupRing>> = {
                s.groups.lock().unwrap().values().cloned().collect()
            };
            for ring in groups {
                for sub in ring.subs.iter() {
                    let sg = sub.lock().unwrap();
                    depth += sg.len_ready as u64;
                    if sg.ready_head != NIL {
                        let age = (now - sg.ready_since[sg.ready_head as usize]) as f64;
                        if age > oldest_ms {
                            oldest_ms = age;
                        }
                    }
                }
            }
        }
        (oldest_ms.max(0.0), depth)
    }

    /// F1: `now_ms` is the clock the two ages are taken against — a ring that has never
    /// been walked in a given mode reports -1 rather than a huge age, so "never" and
    /// "long ago" stay distinguishable in the log line.
    pub fn ring_sizes(&self, now_ms: i64) -> Vec<RingSize> {
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
            let (tenant, queue) = crate::handlers::split_tenant_queue(&qname);
            for (gname, ring) in groups {
                let mut ready = 0usize;
                let mut wheel = 0usize;
                for sub in ring.subs.iter() {
                    let sg = sub.lock().unwrap();
                    ready += sg.len_ready;
                    wheel += sg.wheel.len();
                }
                let age = |stamp: i64| if stamp == 0 { -1 } else { (now_ms - stamp).max(0) };
                let full_age_ms = age(*ring.full_reseed_ms.lock().unwrap());
                let reseed_age_ms = age(*ring.reseed_ms.lock().unwrap());
                out.push(RingSize {
                    tenant: tenant.to_string(),
                    queue: queue.to_string(),
                    group: gname,
                    ready,
                    wheel,
                    full_age_ms,
                    reseed_age_ms,
                });
            }
        }
        out
    }

    /// Close a reseed walk: stamp the clocks, count the pass, and wake parked pops (the
    /// walk may have populated the ring). Consumes the ticket, so a walk cannot stamp
    /// twice. `ok` = every page came back without a SQL error.
    ///
    /// The stamp lands only if the ring is still the one the walk started on. A stamp is
    /// not evidence, it is a CLAIM OF COVERAGE — "this ring has been walked" — and a ring
    /// created after the walk began has no coverage. Dropping the claim leaves its
    /// `full_reseed_ms` at 0, so its very next [`Self::reseed_begin`] is Full and it
    /// cold-starts: exactly what `forget_group` intends. The rows already fed are NOT
    /// rolled back — they went to the ticket's own ring, which after a forget is
    /// unreachable and dies with the last Arc.
    ///
    /// `mode.is_full() && ok` is what advances the full-walk clock, which is what
    /// schedules the next one; a windowed pass must never advance it, or the classes
    /// only a full walk can see would be deferred forever. A FAILED full walk likewise
    /// leaves it, so the ring stays pinned to Full and keeps retrying (B2) — the
    /// conservative direction, now at least counted in `reseeds_failed`.
    ///
    /// Returns whether the stamp landed.
    pub fn reseed_finish(&self, ticket: ReseedTicket, ok: bool) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        let (Some(s), Some(ring)) = (ticket.qstate.as_ref(), ticket.ring.as_ref()) else {
            return false;
        };
        // NON-creating lookup, on the ticket's OWN QueueState: creating here would
        // resurrect a forgotten ring carrying a fresh stamp, which is precisely the bug.
        let current = s.groups.lock().unwrap().get(&*ticket.group).cloned();
        let same = matches!(&current, Some(r) if Arc::ptr_eq(r, ring));
        if !same {
            self.reseeds_dropped.fetch_add(1, Relaxed);
            let (tenant, queue) = crate::handlers::split_tenant_queue(&ticket.qkey);
            tracing::warn!(
                target: "hotlist",
                tenant = %tenant,
                queue = %queue,
                group = %ticket.group,
                full = ticket.mode.is_full(),
                "reseed stamp dropped: the ring was replaced mid-walk (the new one still owes its full walk)"
            );
            self.wake(&ticket.qkey);
            return false;
        }
        *ring.reseed_ms.lock().unwrap() = ticket.started_ms;
        if ticket.mode.is_full() {
            if ok {
                *ring.full_reseed_ms.lock().unwrap() = ticket.started_ms;
            }
            self.reseeds_full.fetch_add(1, Relaxed);
        } else {
            self.reseeds_window.fetch_add(1, Relaxed);
        }
        if ok {
            ring.reseed_fail_streak.store(0, Relaxed);
        } else {
            self.reseeds_failed.fetch_add(1, Relaxed);
            // B2: the ring is now pinned to Full until a walk succeeds. Say so —
            // periodically, not per failure (see `reseed_fail_warns`) — and do NOT fall
            // back to the windowed scan, which would trade a loud stall for a quiet
            // blindness to exactly the classes only the full walk can see.
            let streak = ring.reseed_fail_streak.fetch_add(1, Relaxed) + 1;
            if reseed_fail_warns(streak) {
                let (tenant, queue) = crate::handlers::split_tenant_queue(&ticket.qkey);
                tracing::warn!(
                    target: "hotlist",
                    tenant = %tenant,
                    queue = %queue,
                    group = %ticket.group,
                    full = ticket.mode.is_full(),
                    consecutive_failures = streak,
                    "reseed walk keeps failing; this ring stays pinned to the FULL walk \
                     until one succeeds, so nothing it alone can discover is being found"
                );
            }
        }
        self.wake(&ticket.qkey);
        true
    }

    /// F1: what the single `reseeds` counter used to report, for anyone who wants the
    /// old number (the log line still prints it as `reseeds_delta`, which it computes
    /// from the two deltas so the sum and its parts come from one read each).
    #[allow(dead_code)]
    pub fn reseeds_total(&self) -> u64 {
        use std::sync::atomic::Ordering::Relaxed;
        self.reseeds_full.load(Relaxed) + self.reseeds_window.load(Relaxed)
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
                    let (tn, q) = crate::handlers::split_tenant_queue(&qname);
                    eprintln!("[hlt] tickwake q={} tn={} t={}", q, tn, trace_now_ms());
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
    pub fn ensure_group(&self, qkey: &str, group: &str) {
        if !self.enabled {
            return;
        }
        let s = self.qstate(qkey);
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
    pub fn forget_group(&self, qkey: &str, group: &str) {
        if !self.enabled {
            return;
        }
        if let Some(s) = self.qstate_existing(qkey) {
            let removed = s.groups.lock().unwrap().remove(group).is_some();
            if removed {
                self.wake(qkey);
            }
        }
    }

    /// Invalidate the hot-list ring of a deleted consumer group ACROSS EVERY QUEUE
    /// OF ONE TENANT (the delete-consumer-group admin op, all queues). Same rationale
    /// as [`forget_group`], applied to every queue that currently holds a ring for the
    /// group; each such queue is woken. Tenant-scoped to match the SQL delete
    /// (014_consumer_groups.sql) and the `seeded_groups` purge beside it: on a shared
    /// cell `workers` is a universal group name, and one tenant's DELETE must not
    /// cold-start every other tenant's ring.
    ///
    /// Returns the composite keys whose ring was actually dropped. A1: the local ring
    /// cold-starts by itself, but the PEERS keep a stamped ring for the same group name
    /// and no write dates the pendingness a cursor delete creates — so the caller walks
    /// exactly these queues once more and broadcasts what it finds.
    pub fn forget_group_all_queues(&self, tenant: &str, group: &str) -> Vec<String> {
        if !self.enabled {
            return Vec::new();
        }
        let queues: Vec<(String, Arc<QueueState>)> = {
            self.queues
                .lock()
                .unwrap()
                .iter()
                .filter(|(k, _)| crate::handlers::split_tenant_queue(k).0 == tenant)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        let mut dropped = Vec::new();
        for (qname, s) in queues {
            let removed = s.groups.lock().unwrap().remove(group).is_some();
            if removed {
                self.wake(&qname);
                dropped.push(qname);
            }
        }
        dropped
    }

    /// Idle eviction (shared-cell memory bound). Drop the whole `QueueState` — interning
    /// tables, group rings, wheels — of every (tenant, queue) that has had NO access
    /// since the previous sweep AND holds no live ring state. Without this the map only
    /// ever grows: on a shared free tier an untrusted tenant can loop
    /// `GET /pop/queue/<random>` and pin one `QueueState` per distinct name for the
    /// process lifetime.
    ///
    /// Three conditions, all required, all checked while holding `queues` so nothing can
    /// slip in between them:
    /// * `!active.swap(false)` — untouched for a full sweep (see `QueueState::active`).
    /// * `Arc::strong_count == 1` — the map is the ONLY holder. Any concurrent operation
    ///   cloned the Arc out of `qstate`, so this is an exact "nobody is mid-operation"
    ///   test, and no new clone can appear while we hold the lock.
    /// * every ring entry is `IDLE` — no READY candidate, no WHEEL deferral/lease, no
    ///   INFLIGHT claim. Stale heap entries whose state is already IDLE are droppable by
    ///   construction (the wheel skips them lazily anyway).
    ///
    /// Evicting is never a correctness event even if all three were somehow wrong: the
    /// ring is a cache over PG, and the §8 periodic reseed floor rebuilds it on first
    /// contact. Returns how many queues were dropped.
    pub fn evict_idle(&self) -> usize {
        if !self.enabled {
            return 0;
        }
        // `queues` is held across the index cleanup, not released between the two. The
        // only writer of `by_queue` is `qstate`'s create branch, which reaches it AFTER
        // inserting into `queues` — so holding `queues` here makes the pair atomic
        // against a re-create and the index can never end up missing a live key. Lock
        // order is always queues → by_queue (the fan-out reader drops its `by_queue`
        // guard before it marks), so this nesting cannot invert.
        let mut q = self.queues.lock().unwrap();
        let mut evicted: Vec<String> = Vec::new();
        q.retain(|qkey, s| {
            // Untouched for a full sweep? (Consumes the second chance.)
            if s.active.swap(false, std::sync::atomic::Ordering::Relaxed) {
                return true;
            }
            // The map is the sole holder ⇒ nobody is mid-operation on this state, and
            // no new clone can appear while we hold `queues`.
            if Arc::strong_count(s) != 1 {
                return true;
            }
            // No READY candidate, no WHEEL deferral/lease, no INFLIGHT claim anywhere.
            let groups = s.groups.lock().unwrap();
            let quiet = groups.values().all(|ring| {
                ring.subs.iter().all(|sub| {
                    let sg = sub.lock().unwrap();
                    sg.len_ready == 0 && sg.state.iter().all(|&st| st == IDLE)
                })
            });
            drop(groups);
            if !quiet {
                return true;
            }
            evicted.push(qkey.clone());
            false
        });
        if self.tenancy && !evicted.is_empty() {
            let mut idx = self.by_queue.lock().unwrap();
            for qkey in &evicted {
                let (_, queue) = crate::handlers::split_tenant_queue(qkey);
                if let Some(set) = idx.get_mut(queue) {
                    set.remove(qkey);
                    if set.is_empty() {
                        idx.remove(queue);
                    }
                }
            }
        }
        evicted.len()
    }

    /// Drop the rings this broker does not SERVE — the standby-broker fix (2026-08-24).
    ///
    /// ## Why `evict_idle` cannot do this
    ///
    /// `evict_idle` requires `len_ready == 0` and every entry `IDLE`. A ring nobody pops
    /// satisfies neither: its entries are `READY` and there are as many of them as the
    /// cell has pending work. Measured on the soak cell, the passive broker of an
    /// active/passive pair held `229rings/827000ready/0wheel` and 2.12 GB RSS while the
    /// active one held ~1 ready and 727 MB — and since [`QueueIntern`] is append-only,
    /// only dropping the whole [`QueueState`] returns that memory to the allocator.
    ///
    /// ## The discriminator is CLAIMS, NOT AGE
    ///
    /// This is the whole subtlety, and getting it wrong would cause an outage. The ready
    /// list is FIFO (`ready_push_tail`; claims walk from `ready_head`), so with ~1060
    /// pops/s each taking ~1 candidate, a LEGITIMATE backlog of 827 000 ready entries
    /// puts the head at ~780 s. A head age of 13 minutes is therefore exactly what a
    /// healthy, serving, badly-backlogged broker looks like, and an age-based rule would
    /// discard real work precisely when a cell is most behind. What actually separates
    /// the two is whether entries are LEAVING: a served ring drains, however slowly; on
    /// the standby `ring_oldest_ms` advanced exactly +10 s per 10 s — the same entry sat
    /// at position zero forever. So the signal is `QueueState::served`, stamped only by
    /// consumer paths, and never entry age.
    ///
    /// ## Parked long-poll pops need no separate interlock
    ///
    /// The wait loop re-probes through `has_ready` at most POP_WAIT_MAX_INTERVAL_MS apart
    /// (default 1 s), so anything parked re-stamps `served` tens of times per trim window
    /// and can never be a candidate. That is stronger than asking the notifier and costs
    /// no cross-module lock. (`Arc::strong_count` still covers a pop mid-flight.)
    ///
    /// ## What blocks a trim, and what is deliberately droppable
    ///
    /// `INFLIGHT` blocks it: a checked-out candidate has a dispatch whose `checkin` /
    /// `promote_ack` would find no ring. `READY` and `WHEEL` entries are droppable —
    /// both are re-derived by the §8 reseed floor, which `spawn_idle_sweep` already
    /// leans on ("the reseed floor rebuilds a ring that was dropped too eagerly"), and a
    /// wheel entry stranded by a lease has the same net (see the reseed-floor test that
    /// reclaims a stale lease-parked wheel entry). Nothing here is a source of truth:
    /// the work lives in Postgres, this is a cache of where to look.
    ///
    /// ## The third state, accepted on purpose
    ///
    /// Consumers that exist but have STOPPED popping — crash-looping, expired leases, a
    /// 429-throttled tenant — are indistinguishable from a standby and get trimmed while
    /// holding a real backlog. That is the right trade only because it is cheap to
    /// reverse: their next pop re-creates the ring and takes the cold-start full walk.
    /// It is also self-limiting in the other direction — a ring nobody reads delivers no
    /// value however full it is.
    ///
    /// ## Cost
    ///
    /// The `INFLIGHT` scan is O(entries of that queue) under the `queues` lock, but it
    /// runs only for a queue that has ALREADY failed the `served` check — i.e. only on a
    /// broker with no consumer traffic for it, where there are no pops to contend with.
    /// A serving broker returns on the first branch and never walks.
    pub fn trim_unserved(&self, now_ms: i64) -> usize {
        let trim_ms = self.unserved_trim_ms.load(std::sync::atomic::Ordering::Relaxed);
        if !self.enabled || trim_ms <= 0 {
            return 0;
        }
        let mut q = self.queues.lock().unwrap();
        let mut evicted: Vec<String> = Vec::new();
        q.retain(|qkey, s| {
            // Consume the pop-liveness stamp. Set ⇒ a consumer touched this ring since
            // the last pass ⇒ keep it whatever its depth. THIS is the branch that saves
            // the lagged-but-healthy queue.
            if s.served.swap(false, std::sync::atomic::Ordering::Relaxed) {
                s.unserved_since_ms.store(0, std::sync::atomic::Ordering::Relaxed);
                return true;
            }
            // Same argument as `evict_idle`: the map being the sole holder is what proves
            // nobody is mid-operation, and holding `queues` stops a new clone appearing.
            if Arc::strong_count(s) != 1 {
                return true;
            }
            // Start the clock on the first pass that finds it unserved, so the effective
            // grace is [trim_ms, trim_ms + one pass) rather than a single missed stamp.
            let since = s.unserved_since_ms.load(std::sync::atomic::Ordering::Relaxed);
            if since == 0 {
                s.unserved_since_ms.store(now_ms, std::sync::atomic::Ordering::Relaxed);
                return true;
            }
            if now_ms.saturating_sub(since) < trim_ms {
                return true;
            }
            let groups = s.groups.lock().unwrap();
            let dispatching = groups.values().any(|ring| {
                ring.subs.iter().any(|sub| {
                    let sg = sub.lock().unwrap();
                    sg.state.contains(&INFLIGHT)
                })
            });
            drop(groups);
            if dispatching {
                return true;
            }
            evicted.push(qkey.clone());
            false
        });
        // Secondary index, same lock order and atomicity argument as `evict_idle`.
        if self.tenancy && !evicted.is_empty() {
            let mut idx = self.by_queue.lock().unwrap();
            for qkey in &evicted {
                let (_, queue) = crate::handlers::split_tenant_queue(qkey);
                if let Some(set) = idx.get_mut(queue) {
                    set.remove(qkey);
                    if set.is_empty() {
                        idx.remove(queue);
                    }
                }
            }
        }
        drop(q);
        if !evicted.is_empty() {
            release_freed_pages();
        }
        self.trims_unserved
            .fetch_add(evicted.len() as u64, std::sync::atomic::Ordering::Relaxed);
        evicted.len()
    }

    /// Number of live (tenant, queue) ring states — the map the memory bound is stated
    /// over, reported by the idle sweep.
    pub fn queue_count(&self) -> usize {
        self.queues.lock().unwrap().len()
    }

    #[cfg(test)]
    fn index_len(&self) -> usize {
        self.by_queue.lock().unwrap().len()
    }

    /// Drain up to `max` coalesced local dirty transitions for the mesh flusher (§5);
    /// the drained entries are cleared. The hints go to the wire as they are — the
    /// flusher never re-shapes them, so a 50k drain every 20ms costs no second Vec.
    pub fn drain_dirty(&self, max: usize) -> Vec<DirtyHint> {
        if !self.enabled {
            return Vec::new();
        }
        let mut d = self.dirty.lock().unwrap();
        if d.is_empty() {
            return Vec::new();
        }
        let take: Vec<DirtyHint> = d.iter().take(max).cloned().collect();
        for k in &take {
            d.remove(k);
        }
        take
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
        HotList::new(true, 4, 100, true, false)
    }
    // single-shard variant makes ring order deterministic for the ordering tests.
    fn hl1() -> Arc<HotList> {
        HotList::new(true, 1, 100, true, false)
    }
    // …with QUEEN_TENANCY_HEADER on (a shared cell). The tenant-less legacy paths
    // behave differently by design on the two lanes, so the flag is never implicit.
    fn hl1t() -> Arc<HotList> {
        HotList::new(true, 1, 100, true, true)
    }

    fn names(c: &[Candidate]) -> Vec<String> {
        c.iter().map(|x| x.name.clone()).collect()
    }

    // Partition ids are uuids in production (they come straight off a PG `uuid` column),
    // and the intern now keys the ack bridge on the parsed 16 bytes rather than the text,
    // so a fixture id has to be a real uuid to reach the bridge at all. These two stand
    // in for the old ID0 / IDB placeholders.
    const ID0: &str = "00000000-0000-4000-8000-000000000000";
    const ID1: &str = "00000000-0000-4000-8000-000000000001";
    const IDA: &str = "00000000-0000-4000-8000-00000000000a";
    const IDB: &str = "00000000-0000-4000-8000-00000000000b";

    // ---------------------------------------------------------------------
    // Unserved-ring trim (the standby-broker fix, 2026-08-24).
    //
    // The rig these tests model: an active/passive pair behind one LB sharing one
    // Postgres. Both brokers' rings are FILLED by the reseed timer and by mesh marks
    // from the peer's pushes; only the active one's are DRAINED, because only it is
    // sent pops. On the soak cell the passive broker reached 827 000 ready entries /
    // 2.12 GB RSS against the active one's ~1 / 727 MB.
    // ---------------------------------------------------------------------

    // The ack bridge now keys on a uuid's VALUE, not its spelling. That is strictly more
    // forgiving than the old text key — Postgres always hands back the same lowercase
    // dashed form — but it is a real semantic change, so it is pinned here.
    #[test]
    fn the_ack_bridge_keys_on_the_uuid_value_not_its_spelling() {
        let mut it = QueueIntern::new();
        it.note_id("p0", "6ba7b810-9dad-41d1-80b4-00c04fd430c8");
        let want = Some(0);
        assert_eq!(it.idx_of_id("6ba7b810-9dad-41d1-80b4-00c04fd430c8"), want, "as written");
        assert_eq!(it.idx_of_id("6BA7B810-9DAD-41D1-80B4-00C04FD430C8"), want, "upper case");
        assert_eq!(it.idx_of_id("6ba7b8109dad41d180b400c04fd430c8"), want, "no dashes");
        assert_eq!(it.idx_of_id("6ba7b810-9dad-41d1-80b4-00c04fd430c9"), None, "a different id");
    }

    // An id that is not a uuid costs the bridge entry and NOTHING else: the partition is
    // still interned and still reachable by name, because `reseed_row` seeds the ring
    // through this path. The ack path treats a missing bridge entry as "unknown
    // partition" and returns, so the ack still lands in Postgres.
    #[test]
    fn an_unparseable_id_still_interns_its_partition() {
        let mut it = QueueIntern::new();
        it.note_id("p0", "not-a-uuid");
        assert_eq!(it.idx_of("p0"), Some(0), "the partition is interned regardless");
        assert_eq!(it.name_of(0), Some("p0"));
        assert_eq!(it.idx_of_id("not-a-uuid"), None, "but it gets no bridge entry");
    }

    #[test]
    fn parse_uuid16_rejects_what_is_not_a_uuid() {
        assert!(parse_uuid16("00000000-0000-4000-8000-000000000000").is_some());
        assert!(parse_uuid16("").is_none(), "empty");
        assert!(parse_uuid16("00000000-0000-4000-8000-00000000000").is_none(), "too short");
        assert!(parse_uuid16("00000000-0000-4000-8000-0000000000000").is_none(), "too long");
        assert!(parse_uuid16("0000000g-0000-4000-8000-000000000000").is_none(), "non-hex");
        // Dashes are ignored wherever they fall — this is a value parser, not a format
        // validator, and the only producer is Postgres.
        assert_eq!(parse_uuid16("--0000000000000000000000000000000a"), parse_uuid16("0000000000000000000000000000000a"));
    }

    // ------------------------------------------------ burst-resolved telemetry

    // The window's provenance counters and its three maxima, driven through the
    // three ways an entry can ever reach a ready list. This is the log line the
    // 2026-08-24 soak cell had no way of writing: `ring_depth`/`ring_oldest_ms` are
    // one SAMPLE per 10s window, and the bursts that produced a 330-470 ms e2e p99
    // formed and drained inside it.
    #[test]
    fn the_burst_window_attributes_every_entry_and_peaks_at_the_real_depth() {
        let h = hl1();
        reg(&h, "q", "g");

        // 1) MARK — three partitions made ready by a push.
        for p in 0..3 {
            h.mark_local("q", &format!("p{p}"), 1, 0);
        }
        // 2) WHEEL — a deferred partition promoted when its deadline came due.
        h.set_queue_cfg("q", 2, 0, 0, 0); // delayed 2s ⇒ the mark parks in the wheel
        h.mark_local("q", "w0", 1, 0);
        assert_eq!(h.burst.live_ready(), 3, "a wheel park is not ready yet");
        h.tick(2400); // past commit + D + pad

        // 3) RESEED — a row the §8 floor found in PostgreSQL. Back to a plain
        // queue, or the deferral config would wheel the row instead of readying it.
        h.set_queue_cfg("q", 0, 0, 0, 0);
        walk(&h, "q", "g", 0, &[(ID0, "r0")]);

        assert_eq!(h.burst.live_ready(), 5, "3 marks + 1 wheel + 1 reseed");
        // The lane peek is where `max_lane_ready` is exact, and it is also the
        // discriminator: 5 entries in ONE lane, not 5 lanes holding one each. The
        // clock is the real one (`ready_push_tail` stamps `now_epoch_ms`), so the
        // age has to be measured against it.
        let now = crate::util::now_epoch_ms() + 250;
        assert_eq!(h.ready_peek("q", "g", now).0, 5);

        let w = h.burst.take_window();
        assert_eq!(w.from_mark, 3, "{w:?}");
        assert_eq!(w.from_wheel, 1, "{w:?}");
        assert_eq!(w.from_reseed, 1, "{w:?}");
        assert_eq!(w.depth_max, 5, "the window's high-water depth");
        assert_eq!(w.lane_ready_max, 5, "one lane held all five");
        assert!(
            w.oldest_max_ms >= 250,
            "the head's age at the peek: {}",
            w.oldest_max_ms
        );

        // Emitting the line RESETS the window — that is what makes it a window and
        // not a since-boot maximum. The live gauge is not a window and does not move.
        let after = h.burst.take_window();
        assert_eq!(
            after,
            BurstWindow::default(),
            "reset on emission: {after:?}"
        );
        assert_eq!(
            h.burst.live_ready(),
            5,
            "the live gauge is not part of the reset"
        );

        // A NEW window peaks at what happens inside it, from zero, even though the
        // ring never emptied: the entries that were already standing are not
        // arrivals, so only the depth the next transition reaches is recorded.
        h.mark_local("q", "p9", 1, 0);
        let w2 = h.burst.take_window();
        assert_eq!(w2.from_mark, 1);
        assert_eq!(w2.from_wheel, 0);
        assert_eq!(w2.from_reseed, 0);
        assert_eq!(w2.depth_max, 6, "the live gauge is global and cumulative");
    }

    // The maxima are FETCH_MAX, not last-write: a burst that has already drained by
    // the time the reporter looks must still be in the line. This is precisely the
    // failure the feature exists to fix — `ring_depth` sampled 0 while the tail was
    // being made.
    #[test]
    fn the_maxima_survive_a_burst_that_is_over_before_the_line_is_written() {
        let h = hl1();
        reg(&h, "q", "g");
        for p in 0..40 {
            h.mark_local("q", &format!("p{p}"), 1, 0);
        }
        // …and the whole burst is claimed away again, well before any reporter tick.
        let claimed = h.take_batch("q", "g", 64, u32::MAX, crate::util::now_epoch_ms());
        assert_eq!(claimed.len(), 40);
        assert_eq!(h.burst.live_ready(), 0, "the ring is empty again");
        assert_eq!(h.ready_peek("q", "g", crate::util::now_epoch_ms()).0, 0);

        let w = h.burst.take_window();
        assert_eq!(w.depth_max, 40, "the peak, not the value at reporting time");
        assert_eq!(
            w.lane_ready_max, 40,
            "take_batch sees the lane before it claims"
        );
        assert_eq!(w.from_mark, 40);
    }

    // The gauge is LIVE, so every path that removes entries has to be on it — and the
    // one that matters is `trim_unserved`, which drops rings that are FULL by design
    // (827 000 ready entries on the standby of the incident). `Drop for SubRing` is
    // what makes that impossible to forget.
    #[test]
    fn dropping_a_ring_full_of_ready_entries_returns_the_gauge_to_zero() {
        let h = hl_trim(30_000);
        reg(&h, "q", "g");
        for p in 0..25 {
            h.mark_local("q", &format!("p{p}"), 1, 10_000);
        }
        assert_eq!(h.burst.live_ready(), 25);
        assert_eq!(
            trim_twice(&h, 10_000, 30_000),
            1,
            "the unserved ring is dropped"
        );
        assert_eq!(h.queue_count(), 0);
        assert_eq!(
            h.burst.live_ready(),
            0,
            "a dropped ring must not leak its depth into the gauge forever"
        );
        // …and the window's arrivals are unaffected: they DID arrive.
        assert_eq!(h.burst.take_window().from_mark, 25);
    }

    // Two hot lists in one process keep separate windows. The `FLOOR_*` statics above
    // are the counter-example this avoids: a per-process gauge would make one test's
    // ring pollute another's maxima, and on a broker it would make the number
    // unattributable.
    #[test]
    fn each_hot_list_owns_its_own_burst_window() {
        let a = hl1();
        let b = hl1();
        reg(&a, "q", "g");
        reg(&b, "q", "g");
        for p in 0..7 {
            a.mark_local("q", &format!("p{p}"), 1, 0);
        }
        b.mark_local("q", "p0", 1, 0);
        assert_eq!(a.burst.live_ready(), 7);
        assert_eq!(b.burst.live_ready(), 1);
        assert_eq!(a.burst.take_window().depth_max, 7);
        assert_eq!(b.burst.take_window().depth_max, 1);
    }

    // A trim-armed hot-list. Single shard so the ring walk is deterministic.
    fn hl_trim(trim_ms: i64) -> Arc<HotList> {
        let h = HotList::new(true, 1, 100, true, false);
        h.set_unserved_trim_ms(trim_ms);
        h
    }

    // Run the trim twice `trim_ms` apart, which is what a real broker's timer does:
    // the first pass starts the unserved clock, a later one past the threshold acts.
    fn trim_twice(h: &HotList, t0: i64, trim_ms: i64) -> usize {
        h.trim_unserved(t0);
        h.trim_unserved(t0 + trim_ms + 1)
    }

    // THE test this whole design exists to pass. A ring with a huge, ANCIENT backlog
    // that IS being served must never be trimmed: the ready list is FIFO, so at the
    // soak cell's ~1060 claims/s an honest 827k-entry backlog puts the head at ~780 s,
    // and anything keyed on entry age would throw away real work exactly when the cell
    // is furthest behind. Here the entries are 30 MINUTES old — two orders of magnitude
    // past the threshold — and a claim is arriving.
    #[test]
    fn a_lagged_but_served_ring_is_never_trimmed_however_old_its_head_is() {
        let h = hl_trim(30_000);
        reg(&h, "q", "g");
        let t0 = 1_000_000;
        for i in 0..64 {
            h.mark_local("q", &format!("p{i}"), 1, t0);
        }
        // 30 minutes later the backlog is still there and a consumer is still claiming.
        let much_later = t0 + 1_800_000;
        let got = h.take_batch("q", "g", 1, 1, much_later);
        assert_eq!(got.len(), 1, "a serving consumer is claiming from the backlog");
        assert!(
            h.ready_est("q", "g", much_later) > 1,
            "the backlog must still be deep for this test to mean anything"
        );

        assert_eq!(h.trim_unserved(much_later), 0, "first pass: served ⇒ keep");
        assert_eq!(
            h.trim_unserved(much_later + 30_001),
            0,
            "still served since the last pass ⇒ keep, whatever the head age"
        );
        assert_eq!(h.queue_count(), 1, "the ring survived");
        assert_eq!(h.trims_unserved.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    // The standby: the ring fills from marks and reseed rows, nothing ever pops it.
    // This is the case that must reclaim, and note WHAT it asserts — queue_count going
    // to 0 means the whole QueueState was dropped, which is the only thing that frees
    // the append-only QueueIntern.
    #[test]
    fn a_ring_no_pop_ever_touched_is_trimmed_whole() {
        let h = hl_trim(30_000);
        reg(&h, "q", "g");
        let t0 = 500_000;
        for i in 0..32 {
            h.mark_remote("q", &format!("p{i}"), t0);
        }
        walk_rows(&h, "q", "g", t0, &[("11111111-1111-4111-8111-111111111111", "px")]);
        assert_eq!(h.queue_count(), 1);

        assert_eq!(trim_twice(&h, t0, 30_000), 1, "nothing popped it ⇒ trimmed");
        assert_eq!(h.queue_count(), 0, "the whole QueueState is gone (intern freed)");
        assert_eq!(h.trims_unserved.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    // The discriminator itself: the paths that FILL a ring must not look like service.
    // If a mark or a reseed row stamped pop-liveness, a standby taking 1060 marks/s
    // would keep every ring alive forever and the fix would be a no-op.
    #[test]
    fn marks_and_reseed_rows_do_not_count_as_serving() {
        let h = hl_trim(30_000);
        reg(&h, "q", "g");
        let t0 = 700_000;
        // Keep marking and reseeding across BOTH passes, exactly as a peer's pushes and
        // the reseed timer would on a standby.
        h.mark_local("q", "a", 1, t0);
        h.mark_remote("q", "b", t0);
        h.trim_unserved(t0);
        h.mark_local("q", "c", 1, t0 + 15_000);
        walk_rows(&h, "q", "g", t0 + 20_000, &[("22222222-2222-4222-8222-222222222222", "d")]);
        h.note_partition_id("q", "e", "33333333-3333-4333-8333-333333333333");
        assert_eq!(
            h.trim_unserved(t0 + 30_001),
            1,
            "fill paths are not service: the ring is still trimmed"
        );
    }

    // Each of the four consumer paths must keep its own ring alive. checkin matters as
    // much as take_batch: a slow consumer that acks without claiming anything new is
    // still serving.
    #[test]
    fn every_consumer_path_counts_as_serving() {
        for probe in ["take_batch", "has_ready", "ready_peek", "checkin"] {
            let h = hl_trim(30_000);
            reg(&h, "q", "g");
            let t0 = 900_000;
            h.mark_local("q", "p0", 1, t0);
            h.trim_unserved(t0); // start the unserved clock
            let t1 = t0 + 20_000;
            match probe {
                "take_batch" => {
                    h.take_batch("q", "g", 1, 1, t1);
                }
                "has_ready" => {
                    h.has_ready("q", "g", t1);
                }
                "ready_peek" => {
                    h.ready_peek("q", "g", t1);
                }
                // A real result, because an EMPTY checkin is a no-op that returns before
                // it ever reaches the ring — correctly, it is not a consumer touch.
                _ => {
                    let c = h.take_batch("q", "g", 1, 1, t0);
                    h.trim_unserved(t0 + 1); // discard take_batch's stamp, restart the clock
                    h.checkin(
                        "q",
                        "g",
                        vec![CheckinResult {
                            name: "p0".into(),
                            epoch: c[0].epoch,
                            verdict: Verdict::Empty,
                            drained: false,
                        }],
                        t1,
                        false,
                        0,
                    );
                }
            }
            assert_eq!(
                h.trim_unserved(t0 + 30_001),
                0,
                "{probe} is a consumer touch and must keep the ring"
            );
        }
    }

    // A dispatch in flight blocks the trim: checkin/promote_ack for that candidate would
    // otherwise find no ring. READY and WHEEL entries are deliberately droppable.
    #[test]
    fn an_inflight_dispatch_blocks_the_trim() {
        let h = hl_trim(30_000);
        reg(&h, "q", "g");
        let t0 = 1_100_000;
        h.mark_local("q", "p0", 1, t0);
        // Claim it and do NOT check it back in: the entry stays INFLIGHT.
        assert_eq!(h.take_batch("q", "g", 1, 1, t0).len(), 1);
        // Two full windows with no further service — only INFLIGHT is holding it.
        h.trim_unserved(t0 + 40_000);
        assert_eq!(
            h.trim_unserved(t0 + 80_000),
            0,
            "a checked-out candidate must keep its ring"
        );
        assert_eq!(h.queue_count(), 1);
    }

    // Grace: one missed stamp is not enough. The first pass only starts the clock, so the
    // effective window is [trim_ms, trim_ms + one pass) — a parked long-poll re-probing
    // every POP_WAIT_MAX_INTERVAL_MS (1 s default) can never fall through it.
    #[test]
    fn the_first_unserved_pass_only_starts_the_clock() {
        let h = hl_trim(30_000);
        reg(&h, "q", "g");
        let t0 = 1_200_000;
        h.mark_local("q", "p0", 1, t0);
        assert_eq!(h.trim_unserved(t0), 0, "pass 1 only starts the clock");
        assert_eq!(h.trim_unserved(t0 + 20), 0, "still inside the window");
        assert_eq!(h.trim_unserved(t0 + 30_020), 1, "past the window ⇒ trimmed");
    }

    // Service resets the clock, so an intermittent consumer gets the full window again
    // rather than accumulating credit toward a trim.
    #[test]
    fn service_resets_the_unserved_clock() {
        let h = hl_trim(30_000);
        reg(&h, "q", "g");
        let t0 = 1_300_000;
        h.mark_local("q", "p0", 1, t0);
        h.trim_unserved(t0);
        h.trim_unserved(t0 + 10); // clock starts
        h.has_ready("q", "g", t0 + 25_000); // a consumer shows up late in the window
        assert_eq!(h.trim_unserved(t0 + 30_020), 0, "the stamp clears the clock");
        assert_eq!(
            h.trim_unserved(t0 + 30_030),
            0,
            "and the window restarts from here, not from t0"
        );
        assert_eq!(h.trim_unserved(t0 + 60_050), 1, "a full fresh window later ⇒ trimmed");
    }

    // A trimmed ring is a cache drop, never a data loss: the next reseed walk rebuilds it
    // and the entries are claimable again. This is the §8 reseed floor that the idle
    // sweep's own doc already relies on.
    #[test]
    fn a_trimmed_ring_is_rebuilt_by_the_next_reseed_walk() {
        let h = hl_trim(30_000);
        reg(&h, "q", "g");
        let t0 = 1_400_000;
        h.mark_local("q", "p0", 1, t0);
        assert_eq!(trim_twice(&h, t0, 30_000), 1);
        assert_eq!(h.queue_count(), 0);

        // The consumer comes back: register, reseed from PG, claim.
        let t1 = t0 + 100_000;
        reg(&h, "q", "g");
        walk(&h, "q", "g", t1, &[("44444444-4444-4444-8444-444444444444", "p0")]);
        let got = h.take_batch("q", "g", 1, 1, t1);
        assert_eq!(names(&got), vec!["p0".to_string()], "the work is discoverable again");
    }

    // …and the rebuild is a FULL walk, not a windowed one. This is the assertion that
    // makes the trim safe rather than merely cheap: a windowed walk only sees partitions
    // written in the last N ms, so if a trimmed ring were rebuilt from a window, a
    // partition whose last write predates it would hold pending work that no pop could
    // ever discover. Both gates below force Full for a ring with no history — `reseed_due`
    // (last == 0) makes the pop reseed at all, and `reseed_begin` (last_full == 0) makes
    // that reseed a full walk — so a trimmed ring cannot become an invisible backlog.
    #[test]
    fn a_trimmed_ring_is_rebuilt_by_a_full_walk_never_a_windowed_one() {
        let h = hl_trim(30_000);
        reg(&h, "q", "g");
        let t0 = 1_450_000;
        h.mark_local("q", "p0", 1, t0);
        // Give the ring a completed FULL walk, so only the trim can reset that history.
        assert!(walk(&h, "q", "g", t0, &[("55555555-5555-4555-8555-555555555555", "p0")]));
        assert!(
            !h.reseed_begin("q", "g", t0 + 1_000, 600_000, 120_000).mode().is_full(),
            "a ring that just full-walked would otherwise take the cheap windowed walk"
        );

        assert_eq!(trim_twice(&h, t0 + 2_000, 30_000), 1, "the ring is trimmed");

        let t1 = t0 + 200_000;
        assert!(
            h.reseed_due("q", "g", t1, 30_000),
            "a rebuilt ring owes a reseed before a pop may conclude it is empty"
        );
        assert!(
            h.reseed_begin("q", "g", t1, 600_000, 120_000).mode().is_full(),
            "and that reseed MUST be the full walk, whatever the full-walk interval says"
        );
    }

    // Kill switch: 0 restores the pre-fix behaviour exactly — no clock, no scan, no
    // eviction, however long nothing pops.
    #[test]
    fn the_trim_is_off_at_zero() {
        let h = hl_trim(0);
        reg(&h, "q", "g");
        let t0 = 1_500_000;
        h.mark_local("q", "p0", 1, t0);
        for step in 0..5 {
            assert_eq!(h.trim_unserved(t0 + step * 1_000_000), 0, "disabled ⇒ never trims");
        }
        assert_eq!(h.queue_count(), 1);
        assert_eq!(h.trims_unserved.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    // A disabled hot-list must not grow a trim either (the OSS default path).
    #[test]
    fn a_disabled_hotlist_never_trims() {
        let h = HotList::new(false, 1, 100, true, false);
        h.set_unserved_trim_ms(30_000);
        assert_eq!(h.trim_unserved(1), 0);
        assert_eq!(h.trim_unserved(10_000_000), 0);
    }

    // With tenancy on, the trim must maintain the by_queue secondary index in lockstep,
    // exactly as evict_idle does — a stale entry there would let a tenant-less peer wake
    // fan out to a ring that no longer exists.
    #[test]
    fn the_trim_keeps_the_tenant_index_in_lockstep() {
        let h = HotList::new(true, 1, 100, true, true);
        h.set_unserved_trim_ms(30_000);
        let qkey = crate::handlers::tenant_queue_key("t1", "q");
        reg(&h, &qkey, "g");
        h.mark_local(&qkey, "p0", 1, 10_000);
        assert_eq!(h.index_len(), 1, "the index tracks the live ring");
        assert_eq!(trim_twice(&h, 10_000, 30_000), 1);
        assert_eq!(h.queue_count(), 0);
        assert_eq!(h.index_len(), 0, "and is cleaned up with it");
    }

    // Two brokers, one cell: the standby trims while the active one keeps everything.
    // This is the shape of the actual production incident.
    #[test]
    fn the_standby_trims_while_the_active_broker_keeps_its_rings() {
        let active = hl_trim(30_000);
        let standby = hl_trim(30_000);
        let t0 = 2_000_000;
        for h in [&active, &standby] {
            reg(h, "q", "g");
        }
        // Every push marks BOTH brokers: local on the one that took it, remote on its
        // peer. Only the active broker is sent pops. Run long enough that the standby
        // must reclaim more than once — the steady state is what matters, not one trim.
        let mut active_trims = 0;
        let mut standby_trims = 0;
        let mut standby_peak_ready = 0u64;
        for round in 0..12 {
            let now = t0 + round * 10_000;
            for i in 0..8 {
                let p = format!("p{round}_{i}");
                active.mark_local("q", &p, 1, now);
                standby.mark_remote("q", &p, now);
            }
            active.take_batch("q", "g", 8, 64, now);
            // The standby's ring is re-created by the next mark after a trim, so its
            // depth is bounded by one trim window of marks — never the accumulated union,
            // which is the whole point (827 000 entries in the incident).
            standby_peak_ready = standby_peak_ready.max(standby.ready_est("q", "g", now));
            active_trims += active.trim_unserved(now);
            standby_trims += standby.trim_unserved(now);
        }
        assert_eq!(active_trims, 0, "the serving broker never loses its ring");
        assert_eq!(active.queue_count(), 1);
        assert!(
            standby_trims >= 2,
            "the standby must reclaim repeatedly, got {standby_trims} trims"
        );
        // 12 rounds × 8 partitions = 96 distinct partitions marked, which is what an
        // un-trimmed standby would hold (this is the 827 000 of the incident, in
        // miniature). The trim window is [30 s, 30 s + one 10 s pass), so at 8 marks per
        // round the ceiling is 4 rounds' worth = 32. The gap between 32 and 96 IS the fix.
        assert!(
            standby_peak_ready <= 32,
            "the standby's depth must stay bounded by one trim window of marks (<=32), \
             peaked at {standby_peak_ready}"
        );
    }

    // A consumer's first poll registers the (queue, group) ring (then reseeds
    // from PG, §8); only after that do marks reach it. Tests register explicitly
    // to isolate the ring mechanics from the DB-backed reseed.
    fn reg(h: &HotList, q: &str, g: &str) {
        h.ensure_group(q, g);
    }

    // One reseed walk exactly as data.rs runs it: claim the ring, feed the rows PG
    // returned, stamp. Returns whether the stamp landed (B3's identity check).
    fn walk(h: &HotList, q: &str, g: &str, now: i64, rows: &[(&str, &str)]) -> bool {
        let t = h.reseed_begin_full(q, g, now);
        for (id, name) in rows {
            h.reseed_row(&t, id, name);
        }
        h.reseed_finish(t, true)
    }

    // …and the rows without the stamp, for the tests that only care what a row does
    // to the ring (an unfinished ticket is simply dropped, like an abandoned walk).
    fn walk_rows(h: &HotList, q: &str, g: &str, now: i64, rows: &[(&str, &str)]) {
        let t = h.reseed_begin_full(q, g, now);
        for (id, name) in rows {
            h.reseed_row(&t, id, name);
        }
    }

    // D1: the widest offset any ring can draw for an interval — the bound every
    // "is it due yet" assertion has to hold for, since the phase is random per ring.
    fn max_jitter_ms(interval_ms: i64) -> i64 {
        (interval_ms / RESEED_JITTER_DIV).max(RESEED_JITTER_MS)
    }

    fn hint(q: &str, p: &str) -> DirtyHint {
        DirtyHint::new(q, p, None)
    }

    #[test]
    fn take_batch_budget_stops_at_covered_backlog() {
        // Budget-aware claim (2026-07-31): a partition whose accumulated marks
        // already cover the requested batch satisfies the claim ALONE, leaving
        // the rest of the ring claimable by CONCURRENT serves - the fixed
        // over-claim held every claimed partition INFLIGHT for the serve's
        // whole SQL leg and was the T1 pop concurrency ceiling.
        let h = hl1();
        reg(&h, "q", "g");
        for p in 0..4 {
            h.mark_local("q", &format!("p{p}"), 2_000, 0); // deep: 2k marks each
        }
        let c = h.take_batch("q", "g", 10, 1_000, 0);
        assert_eq!(names(&c), vec!["p0"], "deep head partition covers the budget alone");
        // The other three are claimable RIGHT NOW by concurrent serves.
        assert_eq!(names(&h.take_batch("q", "g", 10, 1_000, 0)), vec!["p1"]);
        assert_eq!(names(&h.take_batch("q", "g", 10, 1_000, 0)), vec!["p2"]);

        // Sparse ring (marks far below want): claims up to k exactly as before.
        let h2 = hl1();
        reg(&h2, "q", "g");
        for p in 0..6 {
            h2.mark_local("q", &format!("s{p}"), 10, 0);
        }
        let c2 = h2.take_batch("q", "g", 4, 1_000, 0);
        assert_eq!(c2.len(), 4, "sparse marks: claim up to k as before");
    }

    // Two distinct tenants sharing every NAME — the shared-cell shape (free tier).
    const TA: &str = "11111111-1111-1111-1111-111111111111";
    const TB: &str = "22222222-2222-2222-2222-222222222222";

    // The composite ring/gate key. `tenant_queue_key` is the ONE producer of these
    // keys in the broker; a test that builds its own would not prove the contract.
    fn k(t: &str, q: &str) -> String {
        crate::handlers::tenant_queue_key(t, q)
    }

    fn due_keys(d: &[ReseedDue]) -> Vec<(String, String)> {
        d.iter().map(|x| (x.qkey.clone(), x.group.clone())).collect()
    }

    #[test]
    fn disabled_is_all_noop() {
        let h = HotList::new(false, 4, 100, true, false);
        h.ensure_group("q", "g");
        h.mark_local("q", "p0", 1, 0);
        assert!(h.take_batch("q", "g", 10, u32::MAX, 0).is_empty());
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
        it.note_id("b", IDB);
        assert_eq!(it.idx_of_id(IDB), Some(1));
    }

    #[test]
    fn mark_before_group_registered_is_lost_then_reseed_recovers() {
        // A mark for a group with no ring is a no-op — the group discovers via
        // reseed on first poll (§8). This is the cold-start contract.
        let h = hl1();
        h.mark_local("q", "p0", 1, 0); // no "g" ring yet → lost
        assert!(h.take_batch("q", "g", 10, u32::MAX, 0).is_empty());
        // reseed is what recovers it
        walk(&h, "q", "g", 0, &[(ID0, "p0")]);
        assert_eq!(names(&h.take_batch("q", "g", 10, u32::MAX, 0)), vec!["p0"]);
    }

    #[test]
    fn mark_idle_to_ready_then_take() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        h.mark_local("q", "p1", 1, 0);
        let c = h.take_batch("q", "g", 10, u32::MAX, 0);
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
        assert_eq!(names(&h.take_batch("q", "g", 10, u32::MAX, 0)), vec!["p0"]);
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
        h.set_queue_cfg("q", 0, 5, 0, 0); // window_buffer = 5s ⇒ mark parks in the wheel
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
        let c = h.take_batch("q", "g", 10, u32::MAX, 0);
        assert_eq!(names(&c), vec!["p0"]); // no duplicate ring entry
    }

    #[test]
    fn round_robin_reappend_on_took() {
        let h = hl1();
        reg(&h, "q", "g");
        for i in 0..3 {
            h.mark_local("q", &format!("p{i}"), 1, 0);
        }
        let c = h.take_batch("q", "g", 3, u32::MAX, 0);
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
        let c2 = h.take_batch("q", "g", 3, u32::MAX, 0);
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
        h.note_partition_id("q", "p0", ID0);
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, u32::MAX, 0);
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
        assert!(h.take_batch("q", "g", 1, u32::MAX, 200).is_empty());
        // our ack releases the lease → promote pulls it back at once
        h.promote_ack("q", "g", ID0, 300, false);
        assert_eq!(names(&h.take_batch("q", "g", 1, u32::MAX, 300)), vec!["p0"]);
    }

    // Spec §2 clear-su-ack: covered ack + nothing arrived during the lease ⇒
    // IDLE, not READY — no guaranteed-empty probe (the 2026-07-24 2x-attempts
    // vegas-queue amplifier).
    #[test]
    fn covered_ack_with_no_marks_clears_to_idle() {
        let h = hl1();
        reg(&h, "q", "g");
        h.note_partition_id("q", "p0", ID0);
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, u32::MAX, 0);
        assert_eq!(names(&c), vec!["p0"]);
        h.checkin(
            "q",
            "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Took, drained: true }],
            100,
            false,
            60_000,
        );
        h.promote_ack("q", "g", ID0, 300, true);
        assert!(
            h.take_batch("q", "g", 1, u32::MAX, 400).is_empty(),
            "caught-up partition must not be re-probed after a covered ack"
        );
    }

    // Re-arm stall (root-caused on the bench VM 2026-07-29): a Leased verdict
    // carries the foreign lease's expiry from the SQL snapshot, which may already
    // be stale (the lease released between the pop and this checkin). Parking the
    // entry at that expiry stranded it for up to leaseTime (300s default) because
    // the releasing ack's promote_ack had already fired for that partition and no
    // future ack referenced it. The park is now capped at MAX_LEASE_REVISIT_MS so
    // a stale lease is re-probed within ~1s instead of held to expiry.
    #[test]
    fn leased_verdict_is_reprobed_within_the_cap_not_at_lease_expiry() {
        let h = hl1();
        reg(&h, "q", "g");
        h.note_partition_id("q", "p0", ID0);
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, u32::MAX, 0);
        assert_eq!(names(&c), vec!["p0"]);
        // SQL snapshot says a foreign lease is held until 300s from epoch.
        h.checkin(
            "q",
            "g",
            vec![CheckinResult {
                name: "p0".into(),
                epoch: c[0].epoch,
                verdict: Verdict::Leased(300_000),
                drained: false,
            }],
            100,
            false,
            60_000,
        );
        // Not claimable inside the cap window (the lease may be genuinely held)...
        assert!(h.take_batch("q", "g", 1, u32::MAX, 500).is_empty());
        // ...but re-probed at the cap (claim now_ms 100 + MAX_LEASE_REVISIT_MS),
        // NOT held to the reported 300s expiry.
        assert_eq!(
            names(&h.take_batch("q", "g", 1, u32::MAX, 100 + MAX_LEASE_REVISIT_MS + 1)),
            vec!["p0"],
            "a possibly-stale foreign lease must be re-probed within the cap, not at lease expiry"
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
        h.note_partition_id("q", "p0", ID0);
        // Cycle 1: single message, clean drain — entry ends IDLE with
        // drained=true persisted at its Took.
        h.mark_local("q", "p0", 1, 0);
        let c1 = h.take_batch("q", "g", 1, u32::MAX, 0);
        h.checkin("q", "g",
            vec![CheckinResult { name: "p0".into(), epoch: c1[0].epoch, verdict: Verdict::Took, drained: true }],
            10, false, 60_000);
        h.promote_ack("q", "g", ID0, 20, true); // covered+drained ⇒ IDLE
        // Cycle 2: a 100-message backlog lands; OUR claim's Took checkin loses
        // the race to a concurrent popper's Leased verdict (ownership gate has
        // already been passed by the Leased one in this simulation).
        h.mark_local("q", "p0", 100, 30);
        let c2 = h.take_batch("q", "g", 1, u32::MAX, 30);
        h.checkin("q", "g",
            vec![CheckinResult { name: "p0".into(), epoch: c2[0].epoch, verdict: Verdict::Leased(60_030), drained: false }],
            40, false, 60_000);
        // The winner's ack arrives (covered): the stale drained=true from cycle
        // 1 must NOT cause a clear — the entry must come back claimable.
        h.promote_ack("q", "g", ID0, 50, true);
        assert_eq!(
            names(&h.take_batch("q", "g", 1, u32::MAX, 60)),
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
        h.note_partition_id("q", "p0", ID0);
        h.mark_local("q", "p0", 1000, 0);
        let c = h.take_batch("q", "g", 1, u32::MAX, 0);
        h.checkin(
            "q",
            "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Took, drained: false }],
            100,
            false,
            60_000,
        );
        h.promote_ack("q", "g", ID0, 300, true);
        assert_eq!(
            names(&h.take_batch("q", "g", 1, u32::MAX, 400)),
            vec!["p0"],
            "undrained backlog must be claimable immediately after the covered ack"
        );
    }

    #[test]
    fn covered_ack_with_in_lease_mark_promotes() {
        let h = hl1();
        reg(&h, "q", "g");
        h.note_partition_id("q", "p0", ID0);
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, u32::MAX, 0);
        h.checkin(
            "q",
            "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Took, drained: true }],
            100,
            false,
            60_000,
        );
        h.mark_local("q", "p0", 1, 150); // push lands during the lease
        h.promote_ack("q", "g", ID0, 300, true);
        assert_eq!(
            names(&h.take_batch("q", "g", 1, u32::MAX, 400)),
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
        h.note_partition_id("q", "p0", ID0);
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, u32::MAX, 0);
        h.mark_local("q", "p0", 1, 50); // races the in-flight pop
        h.checkin(
            "q",
            "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Took, drained: true }],
            100,
            false,
            60_000,
        );
        h.promote_ack("q", "g", ID0, 300, true);
        assert_eq!(
            names(&h.take_batch("q", "g", 1, u32::MAX, 400)),
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
        let c = h.take_batch("q", "g", 1, u32::MAX, 0); // snapshots epoch, INFLIGHT
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
        let c2 = h.take_batch("q", "g", 1, u32::MAX, 0);
        assert_eq!(names(&c2), vec!["p0"], "raced mark must keep the entry");
    }

    // §4 interleaving 2: no race ⇒ empty CAS succeeds ⇒ cleared (IDLE).
    #[test]
    fn cas_no_race_clears_entry() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, u32::MAX, 0);
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
        assert!(h.take_batch("q", "g", 1, u32::MAX, 0).is_empty(), "clean empty clears");
    }

    // §4 interleaving 3: mark delayed PAST the clear ⇒ clear passes, late mark re-adds.
    #[test]
    fn cas_mark_after_clear_readds() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, u32::MAX, 0);
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
        assert!(h.take_batch("q", "g", 1, u32::MAX, 0).is_empty());
        // a late mark (the delayed push) re-adds
        h.mark_local("q", "p0", 1, 0);
        assert_eq!(names(&h.take_batch("q", "g", 1, u32::MAX, 0)), vec!["p0"]);
    }

    #[test]
    fn leased_goes_to_wheel_not_cleared() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, u32::MAX, 1000);
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
        // not cleared, not immediately claimable: it is parked in the wheel.
        assert!(h.take_batch("q", "g", 1, u32::MAX, 1500).is_empty());
        // re-probed at the bounded cap (claim now_ms 1000 + MAX_LEASE_REVISIT_MS),
        // NOT held to the reported 5000ms lease expiry (2026-07-29 re-arm fix).
        assert_eq!(
            names(&h.take_batch("q", "g", 1, u32::MAX, 1000 + MAX_LEASE_REVISIT_MS + 1)),
            vec!["p0"]
        );
    }

    #[test]
    fn ack_promote_pulls_leased_entry_early() {
        let h = hl1();
        reg(&h, "q", "g");
        h.note_partition_id("q", "p0", ID0);
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, u32::MAX, 0);
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
        assert!(h.take_batch("q", "g", 1, u32::MAX, 100).is_empty());
        // ack releases the lease early → promote by id
        h.promote_ack("q", "g", ID0, 100, false);
        assert_eq!(names(&h.take_batch("q", "g", 1, u32::MAX, 100)), vec!["p0"]);
    }

    #[test]
    fn delayed_mark_holds_until_wheel_fires() {
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 2, 0, 0, 0); // delayed 2s
        h.mark_local("q", "p0", 1, 0);
        // held in the wheel — not claimable immediately
        assert!(h.take_batch("q", "g", 1, u32::MAX, 500).is_empty());
        // tick before deadline → still nothing
        h.tick(1000);
        assert!(h.take_batch("q", "g", 1, u32::MAX, 1000).is_empty());
        // after commit_ts + D + pad
        h.tick(2400);
        assert_eq!(names(&h.take_batch("q", "g", 1, u32::MAX, 2400)), vec!["p0"]);
    }

    #[test]
    fn window_buffer_delivers_at_window_and_early_on_full_batch() {
        // window 5s, threshold 100 (hl* default)
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 0, 5, 0, 0);
        // small batch: held until the window expires
        h.mark_local("q", "p0", 10, 0);
        assert!(h.take_batch("q", "g", 1, u32::MAX, 1000).is_empty());
        h.tick(5100);
        assert_eq!(names(&h.take_batch("q", "g", 1, u32::MAX, 5100)), vec!["p0"]);

        // fat batch on a fresh partition: promoted early (before the window)
        h.mark_local("q", "p1", 100, 10_000); // batch_count >= threshold
        assert_eq!(names(&h.take_batch("q", "g", 1, u32::MAX, 10_050)), vec!["p1"]);
    }

    #[test]
    fn window_buffer_early_promote_when_batch_fattens_in_wheel() {
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 0, 5, 0, 0);
        h.mark_local("q", "p0", 40, 0); // below threshold → wheel
        assert!(h.take_batch("q", "g", 1, u32::MAX, 100).is_empty());
        h.mark_local("q", "p0", 70, 100); // now batch_count=110 ≥ 100 → early promote
        assert_eq!(names(&h.take_batch("q", "g", 1, u32::MAX, 200)), vec!["p0"]);
    }

    #[test]
    fn deferral_empty_revisits_not_clears() {
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 2, 0, 0, 0); // delayed queue
        // put p0 ready via the wheel firing
        h.mark_local("q", "p0", 1, 0);
        h.tick(2400);
        let c = h.take_batch("q", "g", 1, u32::MAX, 2400);
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
        assert!(h.take_batch("q", "g", 1, u32::MAX, 2400).is_empty());
        h.tick(10_000);
        assert_eq!(names(&h.take_batch("q", "g", 1, u32::MAX, 10_000)), vec!["p0"]);
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
        assert_eq!(d, vec![hint("q", "p0")]);
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
        assert_eq!(names(&h.take_batch("q", "g", 5, u32::MAX, 0)), vec!["p0"]);
        // the mesh dirty hint is still enqueued (peers, if any, get it).
        assert_eq!(h.drain_dirty(10), vec![hint("q", "p0")]);
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
        let h = HotList::new(true, 1, 100, false, false); // mesh_active = false
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        // still marks the ring (discovery works locally)…
        assert_eq!(names(&h.take_batch("q", "g", 5, u32::MAX, 0)), vec!["p0"]);
        // …but nothing was queued for a mesh that isn't there.
        assert!(h.drain_dirty(10).is_empty());
    }

    #[test]
    fn remote_mark_makes_candidate_discoverable() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_remote("q", "p9", 0);
        assert_eq!(names(&h.take_batch("q", "g", 5, u32::MAX, 0)), vec!["p9"]);
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
        walk(&h, "q", "g", base, &[]);
        h.mark_local("q", "p_keep", 1, base); // a continuously-written partition
        h.mark_local("q", "p_lost", 1, base); // has backlog, about to be falsely cleared
        let c = h.take_batch("q", "g", 2, u32::MAX, base);
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
        assert_eq!(
            h.ring_sizes(base)[0].ready,
            1,
            "ring must be non-empty (p_keep ready)"
        );
        // Not due before the interval; due after interval + the max per-group jitter,
        // regardless of the busy ring.
        assert!(h.periodic_reseed_due(base + 1_000, 30_000).is_empty());
        let due_at = base + 30_000 + max_jitter_ms(30_000) + 1;
        assert_eq!(
            due_keys(&h.periodic_reseed_due(due_at, 30_000)),
            vec![("q".to_string(), "g".to_string())],
            "periodic floor must be due even with a non-empty ring"
        );
        // The background task runs the keyset scan: reseed_row for the still-pending
        // p_lost (exactly what the reseed walk returns: last_offset>committed).
        walk(&h, "q", "g", due_at, &[("id_lost", "p_lost")]);
        let got: HashSet<String> = names(&h.take_batch("q", "g", 5, u32::MAX, due_at)).into_iter().collect();
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
        h.set_queue_cfg("q", 0, 5, 0, 0); // windowBuffer 5s, no delayed
        h.mark_local("q", "p0", 10, 0); // small batch → wheel
        h.tick(5100); // window fires → ready
        let c = h.take_batch("q", "g", 1, u32::MAX, 5100);
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
            h.take_batch("q", "g", 1, u32::MAX, 5200).is_empty(),
            "windowBuffer must re-arm, not re-probe immediately"
        );
        // Claimable again only after the next window.
        h.tick(10_300);
        assert_eq!(names(&h.take_batch("q", "g", 1, u32::MAX, 10_300)), vec!["p0"]);
    }

    // Guard: a NON-window queue's auto-ack Took still re-appends to READY at once
    // (the P3 re-arm is scoped to windowBuffer — no behavioural change elsewhere).
    #[test]
    fn non_window_took_still_goes_ready() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, u32::MAX, 0);
        h.checkin(
            "q",
            "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Took, drained: false }],
            0,
            true,
            60_000,
        );
        assert_eq!(names(&h.take_batch("q", "g", 1, u32::MAX, 0)), vec!["p0"], "no window ⇒ immediate READY");
    }

    #[test]
    fn reseed_seeds_ring_and_sets_clock() {
        let h = hl1();
        assert!(h.reseed_due("q", "g", 1000, 30_000)); // cold
        walk(&h, "q", "g", 1000, &[(ID0, "p0"), (ID1, "p1")]);
        assert_eq!(names(&h.take_batch("q", "g", 5, u32::MAX, 1000)), vec!["p0", "p1"]);
        assert!(!h.reseed_due("q", "g", 1000, 30_000)); // just reseeded
        assert!(h.reseed_due("q", "g", 40_000, 30_000)); // stale again
    }

    // ---------------------------------------------------------------- §8 windowed
    //
    // The windowed reseed (2026-08-11) replaces the every-interval full walk with a
    // walk bounded to recently-written partitions, keeping the full one as a slower
    // repair floor. These tests pin the POLICY; the two scans differ only in the SQL
    // they call, and every row either produces goes through `reseed_row`, so the
    // ring-repair behaviours above already cover the row handling for both.

    // The mode the next walk WOULD run. `reseed_begin` is the only way to ask, and its
    // ticket is a claim on the ring: dropping it unstamped is exactly the no-op an
    // abandoned walk is.
    fn mode_now(h: &HotList, q: &str, g: &str, now: i64, full_ms: i64, win_ms: i64) -> ReseedMode {
        h.reseed_begin(q, g, now, full_ms, win_ms).mode()
    }

    // One completed pass in whichever mode the clocks choose; returns that mode.
    fn pass(h: &HotList, q: &str, g: &str, now: i64, full_ms: i64, win_ms: i64) -> ReseedMode {
        let t = h.reseed_begin(q, g, now, full_ms, win_ms);
        let m = t.mode();
        h.reseed_finish(t, true);
        m
    }

    #[test]
    fn reseed_mode_is_full_until_a_full_walk_has_run() {
        let h = hl1();
        // A ring nobody has ever reseeded owes a full walk: cold start, a restarted
        // broker, an evicted ring, a group whose first contact bulk-seeded every
        // partition of the queue at once (log_pop_v1's seed INSERT) — none of those
        // partitions were necessarily written recently.
        assert_eq!(mode_now(&h, "q", "g", 1_000, 300_000, 120_000), ReseedMode::Full);
        // A pass that did NOT complete its full walk cannot satisfy that debt either.
        let t = h.reseed_begin("q", "g", 1_000, 300_000, 120_000);
        assert!(h.reseed_finish(t, false));
        assert_eq!(
            mode_now(&h, "q", "g", 2_000, 300_000, 120_000),
            ReseedMode::Full,
            "a failed walk must never stand in for the cold-start full walk"
        );
        // Only a completed full one does.
        assert_eq!(pass(&h, "q", "g", 2_000, 300_000, 120_000), ReseedMode::Full);
        assert_eq!(
            mode_now(&h, "q", "g", 3_000, 300_000, 120_000),
            ReseedMode::Window(120_000)
        );
    }

    #[test]
    fn reseed_mode_returns_to_full_after_the_full_interval() {
        let h = hl1();
        // Not t=0: 0 is the "never reseeded" sentinel on both clocks, exactly as it
        // already is for reseed_due. Real callers pass now_epoch_ms().
        let base = 1_000_000;
        walk(&h, "q", "g", base, &[]);
        // Inside the interval: windowed, whatever the ring's jitter is.
        assert_eq!(
            mode_now(&h, "q", "g", base + 299_999, 300_000, 120_000),
            ReseedMode::Window(120_000)
        );
        // Past interval + the maximum jitter: full, whatever the ring's jitter is.
        // (The bound is stated with the widest offset the interval admits, not the
        // ring's own, because the offset is random per ring by design — the assertion
        // has to hold for every draw.)
        assert_eq!(
            mode_now(&h, "q", "g", base + 300_000 + max_jitter_ms(300_000), 300_000, 120_000),
            ReseedMode::Full
        );
    }

    #[test]
    fn windowed_passes_never_postpone_the_full_walk() {
        // The regression this guards: stamping the full clock on every reseed would
        // make the repair floor unreachable under a steady poll, because a windowed
        // pass runs far more often than the full interval.
        let h = hl1();
        let base = 1_000_000;
        walk(&h, "q", "g", base, &[]);
        let mut t = 0;
        for _ in 0..30 {
            t += 30_000;
            if pass(&h, "q", "g", base + t, 300_000, 120_000).is_full() {
                break;
            }
        }
        // Bound = interval + max jitter + one polling step: the floor only samples
        // every `interval_ms`, so the first tick at or past the deadline can be up to
        // one step late. Anything beyond that means the full clock is being moved.
        assert!(
            t <= 300_000 + max_jitter_ms(300_000) + 30_000,
            "a full walk must come due within one interval (+jitter, +one step) of the \
             last one, got {t}ms of windowed passes"
        );
    }

    #[test]
    fn scan_bounds_pin_the_full_walk_to_minus_infinity() {
        // A full walk is the windowed statement with its cutoff already bound, so
        // p_window_ms is unread; a windowed one derives its cutoff on the first page.
        assert_eq!(ReseedMode::Full.scan_bounds(), (0, Some("-infinity")));
        assert_eq!(ReseedMode::Window(120_000).scan_bounds(), (120_000, None));
    }

    #[test]
    fn full_interval_zero_pins_every_reseed_to_full() {
        // QUEEN_HOTLIST_RESEED_FULL_MS=0 is the config-only revert to the pre-windowing
        // behaviour. It must hold even right after a full walk.
        let h = hl1();
        walk(&h, "q", "g", 1_000, &[]);
        assert_eq!(mode_now(&h, "q", "g", 1_001, 0, 120_000), ReseedMode::Full);
        assert_eq!(mode_now(&h, "q", "g", 999_999, 0, 120_000), ReseedMode::Full);
    }

    #[test]
    fn reseed_mode_is_per_group_and_per_tenant() {
        let h = hl1t();
        let (a, b) = (k("t1", "orders"), k("t2", "orders"));
        walk(&h, &a, "g1", 1_000, &[]);
        // A full walk for one (tenant, queue, group) says nothing about any other.
        assert_eq!(mode_now(&h, &a, "g1", 2_000, 300_000, 120_000), ReseedMode::Window(120_000));
        assert_eq!(mode_now(&h, &a, "g2", 2_000, 300_000, 120_000), ReseedMode::Full);
        assert_eq!(mode_now(&h, &b, "g1", 2_000, 300_000, 120_000), ReseedMode::Full);
    }

    #[test]
    fn note_dirty_queues_a_peer_hint_only_when_there_are_peers() {
        // The seek path's broadcast half: a backward seek makes old partitions pending
        // with no write to date them, so peers cannot discover it from a windowed pass
        // and must be told over the existing HOTLIST_DIRTY frame.
        let h = hl1();
        h.note_dirty("q", "p0", None);
        h.note_dirty("q", "p0", None); // the set coalesces
        h.note_dirty("q", "p1", None);
        let mut got = h.drain_dirty(100);
        got.sort();
        assert_eq!(got, vec![hint("q", "p0"), hint("q", "p1")]);
        assert!(h.drain_dirty(100).is_empty(), "drain clears the set");

        // No peers ⇒ nothing is queued at all (the single-broker fast path).
        let solo = HotList::new(true, 1, 100, false, false);
        solo.note_dirty("q", "p0", None);
        assert!(solo.drain_dirty(100).is_empty());

        // Disabled ⇒ no-op, like every other hook.
        let off = HotList::new(false, 1, 100, true, false);
        off.note_dirty("q", "p0", None);
        assert!(off.drain_dirty(100).is_empty());
    }

    // ------------------------------------------------------- group-scoped hints
    //
    // The hint carries the SCOPE of the mark that produced it. A push means "pending
    // for every group" and keeps sending no group at all; a seek / group-delete means
    // "pending for THIS group", and saying so is what keeps the peers' repair from
    // costing every other group of the queue a full ring of ghost entries.

    // The two scopes are different set members: a push landing inside a seek's 20ms
    // flush window must not be swallowed by the seek's narrower hint, nor the other way
    // round. Both are sent, the peer applies both, and the wider one wins by being a
    // superset — the direction that is never wrong.
    #[test]
    fn dirty_hints_of_different_scope_do_not_mask_each_other() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0); // the push: queue-wide
        h.mark_local_group("q", "p0", "g", 0); // the seek: group-scoped
        h.mark_local("q", "p0", 1, 0); // …and each scope still coalesces
        h.mark_local_group("q", "p0", "g", 0);
        let mut got = h.drain_dirty(100);
        got.sort();
        assert_eq!(
            got,
            vec![DirtyHint::new("q", "p0", None), DirtyHint::new("q", "p0", Some("g"))]
        );
    }

    #[test]
    fn group_scoped_mark_touches_only_that_ring() {
        let h = hl1();
        reg(&h, "q", "g1");
        reg(&h, "q", "g2");
        h.mark_remote_group("q", "p0", "g1", 0);
        assert_eq!(names(&h.take_batch("q", "g1", 5, u32::MAX, 0)), vec!["p0"]);
        assert!(
            h.take_batch("q", "g2", 5, u32::MAX, 0).is_empty(),
            "a hint scoped to g1 must leave every other group's ring alone"
        );
        // A remote mark never re-broadcasts, group-scoped or not.
        assert!(h.drain_dirty(10).is_empty());
    }

    // A hint must never be able to ALLOCATE per-group state — otherwise a peer, or an
    // untrusted tenant inventing group names, pins one ring per name. It must still
    // wake the queue gate: a partition-targeted long-poll registers no ring at all
    // (R4), and a woken pop that finds nothing costs one in-memory short-circuit.
    #[tokio::test]
    async fn group_scoped_mark_never_creates_a_ring() {
        let h = hl1();
        let n = Notifier::new(false);
        h.attach_notifier(n.clone());
        reg(&h, "q", "known");
        let n2 = n.clone();
        let waiter =
            tokio::spawn(async move { n2.wait_queue("q", std::time::Duration::from_secs(2)).await });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !h.mark_remote_group("q", "p0", "never-polled", 0),
            "an unheld group must report a miss"
        );
        assert!(waiter.await.unwrap(), "the queue gate must still be woken");
        let rings = h.ring_sizes(0);
        assert_eq!(rings.len(), 1, "no ring may be allocated by a hint: {:?}", rings.len());
        assert_eq!(rings[0].group, "known");
    }

    // Tenancy on, one queue name held by two tenants: a group-carrying hint from a peer
    // that sent no tenant fans out over `by_queue` exactly like the tenant-less push
    // hint does — and still only over the named group's rings.
    #[test]
    fn group_scoped_hint_from_a_tenantless_peer_fans_out() {
        let h = hl1t();
        for t in [TA, TB] {
            reg(&h, &k(t, "orders"), "g");
            reg(&h, &k(t, "orders"), "other");
        }
        assert!(h.mark_remote_group_all_tenants("orders", "p0", "g", 0));
        for t in [TA, TB] {
            assert_eq!(
                names(&h.take_batch(&k(t, "orders"), "g", 5, u32::MAX, 0)),
                vec!["p0"],
                "every tenant holding the name must be marked"
            );
            assert!(
                h.take_batch(&k(t, "orders"), "other", 5, u32::MAX, 0).is_empty(),
                "…and only on the group the hint named"
            );
        }
        // A name no tenant holds is a pure miss — no scan, nothing marked.
        assert!(!h.mark_remote_group_all_tenants("no-such-queue", "p0", "g", 0));
    }

    // Degradation: an EMPTY group is not a ring named "" — it is a peer whose field we
    // could not read, so it must widen to the queue-wide mark, never narrow to nothing.
    #[test]
    fn an_empty_group_on_a_hint_degrades_to_the_queue_wide_mark() {
        let h = hl1();
        reg(&h, "q", "g1");
        reg(&h, "q", "g2");
        assert!(h.mark_remote_group("q", "p0", "", 0));
        assert_eq!(names(&h.take_batch("q", "g1", 5, u32::MAX, 0)), vec!["p0"]);
        assert_eq!(names(&h.take_batch("q", "g2", 5, u32::MAX, 0)), vec!["p0"]);
    }

    #[test]
    fn reseed_mode_never_creates_a_ring_it_did_not_need() {
        // reseed_begin is called on the pop path before the scan; it must not be a way
        // for an unpolled name to allocate state (the shared-cell memory bound).
        let h = hl1();
        assert_eq!(h.queue_count(), 0);
        let off = HotList::new(false, 1, 100, true, false);
        let t = off.reseed_begin("q", "g", 1_000, 300_000, 120_000);
        assert_eq!(t.mode(), ReseedMode::Full);
        assert_eq!(off.queue_count(), 0, "disabled must allocate nothing");
        // …and a disabled ticket is inert end to end: no row lands, no stamp does.
        off.reseed_row(&t, ID0, "p0");
        assert!(!off.reseed_finish(t, true));
        assert_eq!(off.queue_count(), 0);
    }

    // ------------------------------------------------------------ B3: the ticket
    //
    // The walk used to re-resolve (queue, group) by NAME on every row and again when it
    // stamped. A `forget_group` + recreate mid-walk therefore landed the stamp on a ring
    // that had never been walked, and that ring then skipped its cold start for up to
    // one full interval — the delete's whole purpose, silently undone.

    // (full, window, failed, dropped) in one read.
    fn counters(h: &HotList) -> (u64, u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            h.reseeds_full.load(Relaxed),
            h.reseeds_window.load(Relaxed),
            h.reseeds_failed.load(Relaxed),
            h.reseeds_dropped.load(Relaxed),
        )
    }

    #[test]
    fn reseed_finish_stamps_when_the_ring_is_unchanged() {
        let h = hl1();
        reg(&h, "q", "g");
        let t = h.reseed_begin("q", "g", 1_000, 300_000, 120_000);
        assert!(t.mode().is_full(), "a cold ring owes a full walk");
        h.reseed_row(&t, ID0, "p0");
        assert!(h.reseed_finish(t, true), "an untouched ring must take its stamp");
        assert_eq!(
            mode_now(&h, "q", "g", 2_000, 300_000, 120_000),
            ReseedMode::Window(120_000),
            "the full walk is now on the clock"
        );
        assert_eq!(counters(&h), (1, 0, 0, 0));
    }

    #[test]
    fn reseed_finish_drops_the_stamp_when_the_ring_was_forgotten() {
        let h = hl1();
        reg(&h, "q", "g");
        let t = h.reseed_begin("q", "g", 1_000, 300_000, 120_000);
        h.reseed_row(&t, ID0, "p0");
        // The group is deleted and immediately re-registered by the next poll.
        h.forget_group("q", "g");
        h.ensure_group("q", "g");
        assert!(
            !h.reseed_finish(t, true),
            "a ring created after the walk began has no coverage to claim"
        );
        assert_eq!(
            mode_now(&h, "q", "g", 2_000, 300_000, 120_000),
            ReseedMode::Full,
            "the new ring must still cold-start"
        );
        assert_eq!(counters(&h), (0, 0, 0, 1), "the pass counts as dropped and nothing else");
    }

    #[test]
    fn reseed_finish_drops_the_stamp_after_forget_group_all_queues() {
        let h = hl1();
        let qk = k(TA, "orders");
        reg(&h, &qk, "workers");
        let t = h.reseed_begin(&qk, "workers", 1_000, 300_000, 120_000);
        h.forget_group_all_queues(TA, "workers");
        h.ensure_group(&qk, "workers");
        assert!(!h.reseed_finish(t, true));
        assert_eq!(mode_now(&h, &qk, "workers", 2_000, 300_000, 120_000), ReseedMode::Full);
    }

    // The rows are NOT rolled back on a dropped stamp — they went to the ring the walk
    // started on, which after the forget is unreachable and dies with the last Arc. What
    // must not happen is the new ring inheriting them: a mark is evidence from a snapshot
    // the delete has since invalidated.
    #[test]
    fn reseed_rows_land_on_the_ticket_ring_not_the_recreated_one() {
        let h = hl1();
        reg(&h, "q", "g");
        let t = h.reseed_begin_full("q", "g", 0);
        h.reseed_row(&t, ID0, "p0"); // before the delete
        h.forget_group("q", "g");
        h.ensure_group("q", "g");
        h.reseed_row(&t, ID1, "p1"); // after it — same ticket, dead ring
        h.reseed_finish(t, true);
        assert!(
            h.take_batch("q", "g", 5, u32::MAX, 0).is_empty(),
            "no row from the abandoned walk may reach the recreated ring"
        );
    }

    // A failed walk still throttles: NOT stamping the cadence clock would turn a
    // database refusing this query into a hot retry loop on the pop path. The full clock
    // stays put, so the ring keeps retrying the full walk — the conservative direction,
    // and the one B2 asks to be made visible rather than "fixed" into blindness.
    #[test]
    fn a_failed_full_walk_still_stamps_the_cadence_clock_but_not_the_full_clock() {
        let h = hl1();
        reg(&h, "q", "g");
        let t = h.reseed_begin("q", "g", 1_000, 300_000, 120_000);
        assert!(h.reseed_finish(t, false), "the stamp lands: the ring is unchanged");
        assert!(!h.reseed_due("q", "g", 1_500, 30_000), "the cadence clock moved");
        assert_eq!(
            mode_now(&h, "q", "g", 2_000, 300_000, 120_000),
            ReseedMode::Full,
            "the full clock did not: the next attempt is full again"
        );
    }

    // ---------------------------------------------------------------- F1 counters

    #[test]
    fn per_mode_counters_split_full_and_window() {
        let h = hl1();
        reg(&h, "q", "g");
        assert!(pass(&h, "q", "g", 1_000, 300_000, 120_000).is_full());
        assert_eq!(counters(&h), (1, 0, 0, 0));
        assert_eq!(
            pass(&h, "q", "g", 2_000, 300_000, 120_000),
            ReseedMode::Window(120_000)
        );
        assert_eq!(counters(&h), (1, 1, 0, 0));
        assert_eq!(h.reseeds_total(), 2, "the old single counter is the sum");
    }

    #[test]
    fn a_failed_walk_counts_in_its_mode_and_in_failed() {
        let h = hl1();
        reg(&h, "q", "g");
        let t = h.reseed_begin("q", "g", 1_000, 300_000, 120_000);
        h.reseed_finish(t, false);
        assert_eq!(
            counters(&h),
            (1, 0, 1, 0),
            "a failed pass is still a full pass attempted — and is what B2 counts"
        );
    }

    #[test]
    fn a_dropped_stamp_counts_only_as_dropped() {
        let h = hl1();
        reg(&h, "q", "g");
        let t = h.reseed_begin("q", "g", 1_000, 300_000, 120_000);
        h.forget_group("q", "g");
        h.ensure_group("q", "g");
        h.reseed_finish(t, true);
        assert_eq!(counters(&h), (0, 0, 0, 1), "nothing was stamped, so no mode counted it");
        assert_eq!(h.reseeds_total(), 0);
    }

    // The per-ring log line has to answer "when was this ring last repaired" without
    // arithmetic, and to keep "never" distinguishable from "long ago".
    #[test]
    fn ring_sizes_reports_the_age_since_the_last_full_walk() {
        let h = hl1();
        reg(&h, "q", "g");
        let r = &h.ring_sizes(1_000)[0];
        assert_eq!((r.full_age_ms, r.reseed_age_ms), (-1, -1), "never walked");
        walk(&h, "q", "g", 1_000, &[]);
        let r = &h.ring_sizes(1_500)[0];
        assert_eq!((r.full_age_ms, r.reseed_age_ms), (500, 500));
        // A windowed pass resets the cadence age and leaves the full one growing —
        // which is exactly the pair that says "this ring is in windowed mode".
        assert_eq!(
            pass(&h, "q", "g", 2_000, 300_000, 120_000),
            ReseedMode::Window(120_000)
        );
        let r = &h.ring_sizes(2_500)[0];
        assert_eq!((r.full_age_ms, r.reseed_age_ms), (1_500, 500));
    }

    // ------------------------------------------------------------ B2 failure streak

    // The re-warn shape. A database refusing this query refuses it for every ring, so
    // the sequence has to keep the count visible without turning 63 rings into a log
    // flood every 30s.
    #[test]
    fn a_failing_walk_warns_on_a_streak_and_then_only_on_doublings() {
        assert!(!reseed_fail_warns(1), "one failure is a hiccup, not a stall");
        assert!(!reseed_fail_warns(2));
        assert!(reseed_fail_warns(3), "the first line, ~90s into a ring not reseeding");
        assert!(!reseed_fail_warns(5), "…then quiet until the count doubles");
        assert!(reseed_fail_warns(8));
        assert!(reseed_fail_warns(1024), "and it never goes fully silent");
    }

    // The streak is per RING, not global: a queue whose reseed query times out while
    // every other queue's succeeds is exactly what an aggregate hides.
    #[test]
    fn the_failure_streak_is_per_ring_and_resets_on_success() {
        use std::sync::atomic::Ordering::Relaxed;
        let h = hl1();
        reg(&h, "q", "g");
        reg(&h, "q", "other");
        let streak = |g: &str| {
            h.queues.lock().unwrap()["q"].groups.lock().unwrap()[g]
                .reseed_fail_streak
                .load(Relaxed)
        };
        for i in 0..3 {
            let t = h.reseed_begin("q", "g", 1_000 + i, 300_000, 120_000);
            h.reseed_finish(t, false);
        }
        assert_eq!(streak("g"), 3, "three consecutive failures on this ring");
        assert_eq!(streak("other"), 0, "…and none on the queue's other ring");
        let t = h.reseed_begin("q", "g", 2_000, 300_000, 120_000);
        h.reseed_finish(t, true);
        assert_eq!(streak("g"), 0, "one success closes the stall");
    }

    // ------------------------------------------------------- A3 repair markers

    // A durable repair marker is the floor under a mesh hint the peer never received.
    // Honouring it means putting the ring back where a cold start would leave it: owing
    // a FULL walk, because the pendingness a moved cursor creates has no write to date
    // it and no windowed pass can find it.
    #[test]
    fn request_full_walk_puts_the_ring_back_into_full_mode() {
        let h = hl1();
        reg(&h, "q", "g");
        walk(&h, "q", "g", 1_000, &[]);
        assert_eq!(
            mode_now(&h, "q", "g", 2_000, 300_000, 120_000),
            ReseedMode::Window(120_000),
            "a freshly full-walked ring is windowed"
        );
        assert!(h.request_full_walk("q", "g"));
        assert_eq!(mode_now(&h, "q", "g", 2_000, 300_000, 120_000), ReseedMode::Full);
    }

    // The regression guard for `reseed_ms = 1` instead of 0. Zero is the never-reseeded
    // sentinel that `periodic_reseed_due` deliberately skips (cold start belongs to the
    // pop path), so a zero here would defer the repair until the next pop — and a group
    // whose consumers are all parked in a long poll is precisely the stall being
    // repaired. Silent if you get it wrong, hence the explicit test.
    #[test]
    fn request_full_walk_makes_the_background_floor_claim_the_ring_now() {
        let h = hl1();
        reg(&h, "q", "g");
        let base = 1_000_000;
        walk(&h, "q", "g", base, &[]);
        assert!(
            h.periodic_reseed_due(base + 1_000, 30_000).is_empty(),
            "just reseeded: not due"
        );
        h.request_full_walk("q", "g");
        assert_eq!(
            due_keys(&h.periodic_reseed_due(base + 1_000, 30_000)),
            vec![("q".to_string(), "g".to_string())],
            "the 2s floor loop must claim it on its next tick, not at the next pop"
        );
    }

    // A marker naming something this broker does not poll must cost nothing and, above
    // all, allocate nothing: the table is written by any tenant's seek, and a repair that
    // could create per-group state would let one of them pin memory on every broker of a
    // shared cell.
    #[test]
    fn request_full_walk_never_creates_a_ring_it_did_not_hold() {
        let h = hl1();
        reg(&h, "q", "g");
        assert!(!h.request_full_walk("q", "unknown-group"), "no ring, nothing repaired");
        assert!(!h.request_full_walk("unknown-queue", "g"));
        assert_eq!(h.queue_count(), 1, "no QueueState was allocated");
        assert_eq!(h.ring_sizes(0).len(), 1, "and no ring was");
        // Disabled ⇒ no-op, like every other hook.
        let off = HotList::new(false, 1, 100, true, false);
        assert!(!off.request_full_walk("q", "g"));
    }

    // ------------------------------------------------------------------- D1 jitter

    // Every ring cold-starts together after a restart and is then stamped at the same
    // instant, so the de-phasing is the ONLY thing that keeps the full walks from firing
    // as one burst per period. A constant 15s offset spread a 30s cadence well and a
    // 300s one barely at all (63 production rings inside one 15s band, ~4 walks/s of the
    // 49ms query); the span now scales with the interval.
    #[test]
    fn full_walk_de_phasing_scales_with_the_full_interval() {
        let h = hl1();
        let base = 1_000_000i64;
        let rings = 200;
        for i in 0..rings {
            walk(&h, &format!("q{i}"), "g", base, &[]);
        }
        let full_at = |at: i64| {
            (0..rings)
                .filter(|i| mode_now(&h, &format!("q{i}"), "g", at, 300_000, 120_000).is_full())
                .count()
        };
        // Nobody early, everybody by the widest offset the interval admits.
        assert_eq!(full_at(base + 299_999), 0, "no ring may come due before its interval");
        assert_eq!(
            full_at(base + 300_000 + max_jitter_ms(300_000)),
            rings,
            "every ring must come due within interval + max jitter"
        );
        // The point of D1: the spread is no longer capped at the 30s cadence's constant.
        // With 200 rings drawing uniform phases, all of them landing inside the old 15s
        // band has probability 0.25^200 — this is a behavioural assertion, not a flaky one.
        assert!(
            full_at(base + 300_000 + RESEED_JITTER_MS) < rings,
            "the full walks must spread past the old constant band"
        );
    }

    // …and the 30s cadence keeps EXACTLY its old de-phasing: the scaled span floors at
    // RESEED_JITTER_MS, which for a 30s interval is what it always was.
    #[test]
    fn the_reseed_cadence_keeps_its_original_jitter_band() {
        let h = hl1();
        let base = 1_000_000i64;
        let rings = 200;
        for i in 0..rings {
            walk(&h, &format!("q{i}"), "g", base, &[]);
        }
        assert_eq!(max_jitter_ms(30_000), RESEED_JITTER_MS);
        assert!(
            h.periodic_reseed_due(base + 30_000 - 1, 30_000).is_empty(),
            "no ring is due before the interval"
        );
        assert_eq!(
            h.periodic_reseed_due(base + 30_000 + RESEED_JITTER_MS, 30_000).len(),
            rings,
            "every ring is due within interval + the original 15s band"
        );
    }

    #[test]
    fn subshard_round_robin_spreads_and_no_resize_panic() {
        // many partitions across 4 shards; take spreads across sub-rings.
        let h = hl();
        reg(&h, "q", "g");
        for i in 0..40 {
            h.mark_local("q", &format!("p{i}"), 1, 0);
        }
        let c = h.take_batch("q", "g", 40, u32::MAX, 0);
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
        let c = h.take_batch("q", "g", 1, u32::MAX, 0);
        assert_eq!(names(&c), vec!["p0"]);
        // Stuck: not claimable, and a fresh mark cannot re-link an INFLIGHT entry.
        assert!(h.take_batch("q", "g", 1, u32::MAX, 0).is_empty());
        h.mark_local("q", "p0", 1, 0); // epoch bump only — still not in the ring
        assert!(h.take_batch("q", "g", 1, u32::MAX, 0).is_empty(), "mark cannot re-add INFLIGHT");
        // The reseed floor re-adds the still-pending partition (last_offset>committed).
        walk(&h, "q", "g", 0, &[(ID0, "p0")]);
        assert_eq!(
            names(&h.take_batch("q", "g", 1, u32::MAX, 0)),
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
        let c = h.take_batch("q", "g", 1, u32::MAX, 0); // p0 INFLIGHT, epoch snapshot
        // reseed reclaims it back to READY while the "pop" is still in flight.
        walk(&h, "q", "g", 0, &[(ID0, "p0")]);
        // A second pop can now take it (redundant probe — allowed).
        assert_eq!(names(&h.take_batch("q", "g", 1, u32::MAX, 0)), vec!["p0"]);
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
        let got = names(&h.take_batch("q", "g", 5, u32::MAX, 0));
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
        h.note_partition_id("q", "p0", ID0);
        h.mark_local("q", "p0", 1, 0);
        let c = h.take_batch("q", "g", 1, u32::MAX, 0);
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
        // Not claimable yet: parked in the wheel (the bounded revisit at
        // now + MAX_LEASE_REVISIT_MS has not fired at t=500).
        assert!(h.take_batch("q", "g", 1, u32::MAX, 500).is_empty());
        // The reseed floor reports p0 pending (last_offset > committed) and MUST
        // reclaim the stale wheel entry — a secondary backstop below the bounded
        // re-probe, for entries stranded by other paths (e.g. a dropped INFLIGHT
        // pop) that the lease cap does not cover.
        walk_rows(&h, "q", "g", 500, &[(ID0, "p0")]);
        assert_eq!(
            names(&h.take_batch("q", "g", 1, u32::MAX, 500)),
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
        h.set_queue_cfg("q", 0, 5, 0, 0); // window_buffer = 5s ⇒ deferral queue
        h.note_partition_id("q", "p0", ID0);
        h.mark_local("q", "p0", 1, 0); // parked in the wheel at 5000ms (window)
        assert!(h.take_batch("q", "g", 1, u32::MAX, 1_000).is_empty());
        // Reseed must leave the deferral deadline intact.
        walk_rows(&h, "q", "g", 1_000, &[(ID0, "p0")]);
        assert!(
            h.take_batch("q", "g", 1, u32::MAX, 1_000).is_empty(),
            "deferral wheel entry must stay parked until its window is due"
        );
        // Still delivered once the window elapses.
        assert_eq!(names(&h.take_batch("q", "g", 1, u32::MAX, 6_000)), vec!["p0"]);
    }

    #[test]
    fn per_group_rings_are_independent() {
        let h = hl1();
        reg(&h, "q", "g1");
        reg(&h, "q", "g2");
        // a mark reaches BOTH registered group rings
        h.mark_local("q", "p0", 1, 0);
        assert_eq!(names(&h.take_batch("q", "g1", 5, u32::MAX, 0)), vec!["p0"]);
        assert_eq!(names(&h.take_batch("q", "g2", 5, u32::MAX, 0)), vec!["p0"]);
        // g1 draining (checkin empty) does not affect g2
        let c = h.take_batch("q", "g1", 5, u32::MAX, 0); // empty (already taken above)
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
        walk(&h, "q", "g1", 1000, &[]);
        walk(&h, "q", "g2", 1000, &[]);
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

        h.forget_group_all_queues(crate::config::DEFAULT_TENANT, "g");
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
        let off = HotList::new(false, 1, 100, true, false);
        off.forget_group("q", "g");
        off.forget_group_all_queues(crate::config::DEFAULT_TENANT, "g");

        let h = hl1();
        h.forget_group("never-seen", "g"); // unknown queue
        reg(&h, "q", "g");
        h.forget_group("q", "other"); // unregistered group in a known queue
        h.forget_group_all_queues(crate::config::DEFAULT_TENANT, "still-unknown");
        // The registered ring is still intact and usable.
        h.mark_local("q", "p0", 1, 0);
        assert_eq!(names(&h.take_batch("q", "g", 5, u32::MAX, 0)), vec!["p0"]);
    }

    // ---------------------------------------------------------------- tenancy
    // Track B (§5). The shared free-tier cell runs many tenants on ONE broker with
    // colliding queue and consumer-group names, so every ring must be keyed by
    // (tenant, queue). Each test below fails on a bare-name key.

    #[test]
    fn two_tenants_same_queue_and_group_have_independent_rings() {
        let h = hl1();
        reg(&h, &k(TA, "orders"), "workers");
        reg(&h, &k(TB, "orders"), "workers");
        h.mark_local(&k(TA, "orders"), "p0", 1, 0);
        // B has nothing pending: its take must not see A's candidate.
        assert!(h.take_batch(&k(TB, "orders"), "workers", 10, u32::MAX, 0).is_empty());
        assert_eq!(names(&h.take_batch(&k(TA, "orders"), "workers", 10, u32::MAX, 0)), vec!["p0"]);
    }

    // The headline shared-cell defect: with one shared ring, tenant B's pop checks
    // A's candidate out INFLIGHT, its SQL returns nothing (the claim is tenant-scoped)
    // and its Empty checkin CAS-clears the entry — A's message becomes invisible until
    // the ≤30s periodic floor.
    #[test]
    fn empty_checkin_by_one_tenant_does_not_clear_the_other() {
        let h = hl1();
        reg(&h, &k(TA, "orders"), "workers");
        reg(&h, &k(TB, "orders"), "workers");
        h.mark_local(&k(TA, "orders"), "p0", 1, 0);
        // B pops its own (empty) ring and checks in whatever it got.
        let cb = h.take_batch(&k(TB, "orders"), "workers", 10, u32::MAX, 0);
        let back: Vec<CheckinResult> = cb
            .iter()
            .map(|c| CheckinResult {
                name: c.name.clone(),
                epoch: c.epoch,
                verdict: Verdict::Empty,
                drained: false,
            })
            .collect();
        h.checkin(&k(TB, "orders"), "workers", back, 0, true, 60_000);
        assert_eq!(
            names(&h.take_batch(&k(TA, "orders"), "workers", 10, u32::MAX, 0)),
            vec!["p0"],
            "tenant B's empty verdict must not clear tenant A's candidate"
        );
    }

    // Worst variant of the same family: B's leased Took parks the entry in the wheel
    // for the whole leaseTime (300s by default), so A waits minutes for a message
    // that is already committed.
    #[test]
    fn leased_took_by_one_tenant_does_not_park_the_other() {
        let h = hl1();
        reg(&h, &k(TA, "orders"), "workers");
        reg(&h, &k(TB, "orders"), "workers");
        h.mark_local(&k(TA, "orders"), "p0", 1, 0);
        let cb = h.take_batch(&k(TB, "orders"), "workers", 10, u32::MAX, 0);
        let back: Vec<CheckinResult> = cb
            .iter()
            .map(|c| CheckinResult {
                name: c.name.clone(),
                epoch: c.epoch,
                verdict: Verdict::Took,
                drained: false,
            })
            .collect();
        // auto_ack=false ⇒ wheel_schedule(now + lease + pad).
        h.checkin(&k(TB, "orders"), "workers", back, 0, false, 300_000);
        assert_eq!(
            names(&h.take_batch(&k(TA, "orders"), "workers", 10, u32::MAX, 0)),
            vec!["p0"],
            "tenant B's lease must not wheel tenant A's candidate"
        );
    }

    // The interning table is nested in the per-(tenant,queue) state, so two tenants'
    // same-NAMED partitions (different uuids) can no longer share a dense index.
    #[test]
    fn promote_ack_is_scoped_by_tenant() {
        let h = hl1();
        reg(&h, &k(TA, "orders"), "workers");
        reg(&h, &k(TB, "orders"), "workers");
        h.note_partition_id(&k(TA, "orders"), "shared-part", IDA);
        h.note_partition_id(&k(TB, "orders"), "shared-part", IDB);
        // Park both in the wheel: mark, take, leased-Took checkin.
        for t in [TA, TB] {
            h.mark_local(&k(t, "orders"), "shared-part", 1, 0);
            let c = h.take_batch(&k(t, "orders"), "workers", 10, u32::MAX, 0);
            let back: Vec<CheckinResult> = c
                .iter()
                .map(|x| CheckinResult {
                    name: x.name.clone(),
                    epoch: x.epoch,
                    verdict: Verdict::Took,
                    drained: false,
                })
                .collect();
            h.checkin(&k(t, "orders"), "workers", back, 0, false, 300_000);
        }
        // B's partition id must be unknown to A's ring: a shared intern table would
        // resolve it to the same dense index and promote A's entry.
        h.promote_ack(&k(TA, "orders"), "workers", IDB, 1, false);
        assert!(
            h.take_batch(&k(TA, "orders"), "workers", 10, u32::MAX, 1).is_empty(),
            "another tenant's partition id must not promote this tenant's entry"
        );
        // A's own ack releases A's lease — and only A's.
        h.promote_ack(&k(TA, "orders"), "workers", IDA, 1, false);
        assert_eq!(
            names(&h.take_batch(&k(TA, "orders"), "workers", 10, u32::MAX, 1)),
            vec!["shared-part"]
        );
        assert!(
            h.take_batch(&k(TB, "orders"), "workers", 10, u32::MAX, 1).is_empty(),
            "tenant B's entry must stay wheeled until B's own ack"
        );
    }

    // D3: the periodic floor must cover every tenant with a live ring, not just the
    // default one — a non-default tenant otherwise has NO correctness floor at all.
    #[test]
    fn periodic_reseed_due_reports_every_tenant() {
        let h = hl1();
        let base = 100i64;
        for t in [TA, TB] {
            reg(&h, &k(t, "orders"), "workers");
            walk(&h, &k(t, "orders"), "workers", base, &[]);
        }
        assert!(h.periodic_reseed_due(base + 1_000, 30_000).is_empty());
        let due_at = base + 30_000 + RESEED_JITTER_MS + 1;
        let got: HashSet<(String, String)> =
            due_keys(&h.periodic_reseed_due(due_at, 30_000)).into_iter().collect();
        assert!(got.contains(&(k(TA, "orders"), "workers".to_string())), "got {got:?}");
        assert!(got.contains(&(k(TB, "orders"), "workers".to_string())), "got {got:?}");
        // And the key round-trips to the tenant the background scan will bind to SQL.
        assert_eq!(
            crate::handlers::split_tenant_queue(&k(TB, "orders")),
            (TB, "orders")
        );
    }

    // D4: one tenant's DELETE /consumer-groups/:g must not cold-start every other
    // tenant's ring for the same (universal) group name.
    #[test]
    fn forget_group_all_queues_is_tenant_scoped() {
        let h = hl1();
        for key in [k(TA, "qa"), k(TA, "qb"), k(TB, "qa")] {
            reg(&h, &key, "g");
            h.mark_local(&key, "p0", 1, 0);
        }
        h.forget_group_all_queues(TB, "g");
        assert!(!h.has_ready(&k(TB, "qa"), "g", 0), "B's ring is dropped");
        assert!(h.has_ready(&k(TA, "qa"), "g", 0), "A's ring is untouched");
        assert!(h.has_ready(&k(TA, "qb"), "g", 0), "A's ring is untouched");
    }

    // F3: DeferCfg is per-(tenant, queue) too — tenant B's windowBuffer must not
    // defer tenant A's messages on a same-named queue.
    #[test]
    fn defer_cfg_is_scoped_by_tenant() {
        let h = hl1();
        reg(&h, &k(TA, "orders"), "workers");
        reg(&h, &k(TB, "orders"), "workers");
        h.set_queue_cfg(&k(TB, "orders"), 0, 60, 0, 0); // B: windowBuffer=60s
        h.mark_local(&k(TA, "orders"), "p0", 1, 0);
        assert!(!h.skip_window(&k(TA, "orders")));
        assert!(h.skip_window(&k(TB, "orders")));
        assert_eq!(
            names(&h.take_batch(&k(TA, "orders"), "workers", 10, u32::MAX, 0)),
            vec!["p0"],
            "A must be claimable now — B's window may not wheel it"
        );
    }

    // D2b: with tenancy ON a tenant-less remote mark (an older peer, rolling upgrade)
    // fans out to EVERY tenant holding that queue name. Attributing it to the default
    // tenant would silently drop the wake for every other tenant.
    #[test]
    fn tenantless_remote_mark_fans_out_to_every_tenant() {
        let h = hl1t();
        for key in [k(TA, "orders"), k(TB, "orders"), k(TA, "other")] {
            reg(&h, &key, "workers");
        }
        h.mark_remote_all_tenants("orders", "p0", 0);
        assert!(h.has_ready(&k(TA, "orders"), "workers", 0));
        assert!(h.has_ready(&k(TB, "orders"), "workers", 0));
        assert!(!h.has_ready(&k(TA, "other"), "workers", 0), "other queues untouched");
    }

    // R1 — the flag-OFF invariant. With QUEEN_TENANCY_HEADER unset no non-default
    // tenant can exist, so a tenant-less frame is NOT ambiguous and must resolve with a
    // single hash lookup, exactly as it did pre-Track-B. This is the path every OSS HA
    // rolling upgrade takes, so a fan-out here would be a pure regression. Proven two
    // ways: the default tenant's ring gets the mark, and a ring that only a scan could
    // have reached (another tenant, same queue name) does NOT.
    #[test]
    fn tenantless_remote_mark_is_o1_default_tenant_when_tenancy_off() {
        let h = hl1();
        let def = k(crate::config::DEFAULT_TENANT, "orders");
        reg(&h, &def, "workers");
        reg(&h, &k(TB, "orders"), "workers"); // unreachable without tenancy — a scan would hit it
        h.mark_remote_all_tenants("orders", "p0", 0);
        assert!(h.has_ready(&def, "workers", 0), "the default tenant's ring is marked");
        assert!(
            !h.has_ready(&k(TB, "orders"), "workers", 0),
            "flag-off must not scan the ring map — only the default tenant is reachable"
        );
        // …and the fan-out index nothing reads is never even built on this lane.
        assert_eq!(h.index_len(), 0, "flag-off must not maintain the fan-out index");
    }

    // R2 — the fan-out is served by the bare-name → composite-keys index, so its cost is
    // the number of tenants holding THAT NAME, not the size of the ring map. A queue name
    // no tenant holds costs one miss, not a scan.
    #[test]
    fn tenantless_fanout_index_tracks_only_holders_of_the_name() {
        let h = hl1t();
        for key in [k(TA, "orders"), k(TB, "orders"), k(TA, "other")] {
            reg(&h, &key, "workers");
        }
        // Two distinct bare names indexed; "orders" is held by exactly two tenants.
        assert_eq!(h.index_len(), 2);
        h.mark_remote_all_tenants("no-such-queue", "p0", 0); // pure miss, no scan
        assert!(!h.has_ready(&k(TA, "orders"), "workers", 0));
        assert!(!h.has_ready(&k(TB, "orders"), "workers", 0));
        assert!(!h.has_ready(&k(TA, "other"), "workers", 0));
    }

    // R3 — the shared-cell memory bound. Before: `queues` had no removal path at all, so
    // every distinct (tenant, queue) string ever seen pinned a QueueState for the process
    // lifetime. After: an entry survives at least one sweep (second chance) and is then
    // dropped iff nothing is holding it and no ring entry is live.
    #[test]
    fn idle_queues_are_evicted_after_a_full_sweep() {
        let h = hl1();
        reg(&h, "q", "g");
        assert_eq!(h.queue_count(), 1);
        // Second chance: the sweep that follows the access keeps it…
        assert_eq!(h.evict_idle(), 0, "an entry accessed this sweep is kept");
        assert_eq!(h.queue_count(), 1);
        // …the next one, with no access in between, drops it.
        assert_eq!(h.evict_idle(), 1);
        assert_eq!(h.queue_count(), 0);
    }

    // A tenant looping GET /pop/queue/<random> is the actual vector: each distinct name
    // used to cost one permanent QueueState. The bound is now "names touched in the last
    // sweep window", not "names ever seen".
    #[test]
    fn unbounded_distinct_names_collapse_to_the_active_set() {
        let h = hl1();
        for i in 0..10_000 {
            reg(&h, &k(TA, &format!("junk-{i}")), "g");
        }
        assert_eq!(h.queue_count(), 10_000);
        h.evict_idle(); // second chance
        assert_eq!(h.evict_idle(), 10_000, "every idle, empty ring is reclaimed");
        assert_eq!(h.queue_count(), 0);
    }

    // Never evict a queue with live ring state: a READY candidate is pending work, and
    // dropping it would hide a committed message until the §8 floor.
    #[test]
    fn a_queue_with_a_pending_candidate_is_never_evicted() {
        let h = hl1();
        reg(&h, "q", "g");
        h.mark_local("q", "p0", 1, 0);
        h.evict_idle();
        h.evict_idle();
        assert_eq!(h.queue_count(), 1, "a READY candidate blocks eviction");

        let c = h.take_batch("q", "g", 5, u32::MAX, 0);
        assert_eq!(names(&c), vec!["p0"]);
        // Now INFLIGHT (checked out by a pop, checkin still to come) — also protected.
        h.evict_idle();
        h.evict_idle();
        assert_eq!(h.queue_count(), 1, "an INFLIGHT claim blocks eviction");
        // Drain it: Empty verdict clears to IDLE, and only then may it be reclaimed.
        h.checkin(
            "q", "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Empty, drained: false }],
            0, true, 60_000,
        );
        h.evict_idle();
        assert_eq!(h.evict_idle(), 1);
    }

    // A queue holding a wheel entry (a delayed/windowBuffer deferral, or a lease parking)
    // has work scheduled in the future — evicting it would drop that schedule.
    #[test]
    fn a_queue_with_a_wheeled_entry_is_never_evicted() {
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 0, 5, 0, 0); // windowBuffer ⇒ the mark parks in the wheel
        h.mark_local("q", "p0", 1, 0);
        h.evict_idle();
        h.evict_idle();
        assert_eq!(h.queue_count(), 1);
        assert_eq!(names(&h.take_batch("q", "g", 5, u32::MAX, 6000)), vec!["p0"], "the deferral survived");
    }

    // Eviction must take the fan-out index with it, or the index becomes the second leak
    // (bare names accumulating dead composite keys forever).
    #[test]
    fn eviction_reclaims_the_fanout_index_too() {
        let h = hl1t();
        reg(&h, &k(TA, "orders"), "g");
        reg(&h, &k(TB, "orders"), "g");
        assert_eq!(h.index_len(), 1); // one bare name, two holders
        h.evict_idle();
        assert_eq!(h.evict_idle(), 2);
        assert_eq!(h.queue_count(), 0);
        assert_eq!(h.index_len(), 0, "the emptied name-set is removed, not left behind");
        // A tenant-less frame for the now-unknown name is a miss, not a stale wake.
        h.mark_remote_all_tenants("orders", "p0", 0);
        assert_eq!(h.queue_count(), 0, "a fan-out must not resurrect evicted rings");
    }

    // R4 (missed wake, empty groups map): a (tenant, queue) whose ONLY consumer is a
    // partition-targeted long-poll has NO group ring — handle_pop_partition parks on
    // the queue gate and never registers one. The push path's local wake is the
    // coalescing tick, which fires only for queues flagged by a mark, so a queue with
    // an empty groups map must still flag: otherwise the tenant's own push never wakes
    // its own consumer and the pop burns its whole long-poll on the backoff floor.
    #[tokio::test]
    async fn quiet_mark_wakes_a_gate_with_no_group_ring() {
        let h = hl1();
        let n = Notifier::new(false);
        h.attach_notifier(n.clone());
        let qk = k(TA, "orders");
        // NB: no reg() — a partition-targeted pop registers no group ring.
        let n2 = n.clone();
        let qk2 = qk.clone();
        let waiter = tokio::spawn(async move {
            n2.wait_queue(&qk2, std::time::Duration::from_secs(2)).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        h.mark_local_quiet(&qk, "p0", 1, 0);
        assert_eq!(h.wake_tick(), 1, "a push to a ring-less queue must flag the wake tick");
        assert!(
            waiter.await.unwrap(),
            "the partition-targeted pop parked on this queue must be woken by its own push"
        );
    }

    // …and the same on the immediate-wake path (transaction commit / DLQ move), which
    // has no tick behind it at all: a lost wake there is the full backoff interval.
    #[tokio::test]
    async fn immediate_mark_wakes_a_gate_with_no_group_ring() {
        let h = hl1();
        let n = Notifier::new(false);
        h.attach_notifier(n.clone());
        let qk = k(TA, "orders");
        let n2 = n.clone();
        let qk2 = qk.clone();
        let waiter = tokio::spawn(async move {
            n2.wait_queue(&qk2, std::time::Duration::from_secs(2)).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        h.mark_local(&qk, "p0", 1, 0);
        assert!(waiter.await.unwrap(), "commit-path mark must wake a ring-less queue's gate");
    }

    // The hotlist↔notifier key contract (see the module note): a mark must wake the
    // gate the matching pop parks on. A mismatch is latency-only — no other test
    // would catch it.
    #[tokio::test]
    async fn a_mark_wakes_the_gate_the_pop_parks_on() {
        let h = hl1();
        let n = Notifier::new(false);
        h.attach_notifier(n.clone());
        reg(&h, &k(TA, "orders"), "workers");
        let qk = k(TA, "orders");
        let n2 = n.clone();
        let qk2 = qk.clone();
        let waiter = tokio::spawn(async move {
            n2.wait_queue(&qk2, std::time::Duration::from_secs(3)).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        h.mark_local(&qk, "p0", 1, 0);
        assert!(waiter.await.unwrap(), "the mark must wake the pop parked on the same qkey");
    }

    // …and it must NOT wake a pop parked on another tenant's same-named queue.
    #[tokio::test]
    async fn a_mark_does_not_wake_another_tenants_parked_pop() {
        let h = hl1();
        let n = Notifier::new(false);
        h.attach_notifier(n.clone());
        reg(&h, &k(TA, "orders"), "workers");
        let n2 = n.clone();
        let kb = k(TB, "orders");
        let waiter = tokio::spawn(async move {
            n2.wait_queue(&kb, std::time::Duration::from_millis(300)).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        h.mark_local(&k(TA, "orders"), "p0", 1, 0);
        assert!(!waiter.await.unwrap(), "tenant B's gate must time out");
    }

    // ------------------------------------------------- TASK M: minimum pop wait

    // ready_est must count the marks a take_batch could actually claim, and count
    // nothing at all when the ring is empty — the empty case is the long-poll's,
    // and a non-zero estimate there would make an idle consumer wait for nothing.
    #[test]
    fn ready_est_counts_claimable_marks_only() {
        let h = hl1();
        reg(&h, "q", "g");
        assert_eq!(h.ready_est("q", "g", 0), 0, "empty ring ⇒ 0 (long-poll's case)");
        h.mark_local("q", "p0", 3, 0);
        h.mark_local("q", "p1", 4, 0);
        assert_eq!(h.ready_est("q", "g", 0), 7);
        // Accumulating marks on the same partition fatten the same entry.
        h.mark_local("q", "p0", 5, 0);
        assert_eq!(h.ready_est("q", "g", 0), 12);
        // Checked-out (INFLIGHT) candidates are NOT claimable by another pop.
        let c = h.take_batch("q", "g", 10, u32::MAX, 0);
        assert_eq!(c.len(), 2);
        assert_eq!(h.ready_est("q", "g", 0), 0);
    }

    // An unknown group / unknown queue must never allocate and must read 0.
    #[test]
    fn ready_est_never_creates_a_ring() {
        let h = hl1();
        assert_eq!(h.ready_est("nope", "g", 0), 0);
        assert_eq!(h.queue_count(), 0, "a peek must not allocate a QueueState");
    }

    // A due wheel entry (expired lease, elapsed delayed/windowBuffer) carries a
    // backlog batch_count cannot describe. It must read as "unbounded" so the
    // caller serves at once rather than holding work whose size it cannot see.
    #[test]
    fn ready_est_reports_unbounded_for_a_due_wheel_entry() {
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 0, 5, 0, 0); // windowBuffer ⇒ the mark parks in the wheel
        h.mark_local("q", "p0", 1, 0);
        assert_eq!(h.ready_est("q", "g", 0), 0, "not due yet ⇒ nothing claimable");
        assert_eq!(
            h.ready_est("q", "g", 6_000),
            u64::MAX,
            "due wheel entry ⇒ serve now, never wait"
        );
    }

    // The config carrier: min_pop_wait rides in DeferCfg but is NOT a deferral —
    // it must not turn a plain queue into one (which would change empty-verdict
    // handling from hard-clear to revisit).
    // The global latch is what keeps the DEFAULT lane free: it must stay false
    // while every queue configures the option to 0, and flip only when a queue
    // really asks for a window.
    #[test]
    fn min_pop_wait_global_latch_stays_off_until_a_queue_asks() {
        let h = hl1();
        assert!(!h.min_pop_wait_in_play(), "fresh broker ⇒ feature not in play");
        h.set_queue_cfg("a", 0, 0, 0, 1); // plain queue
        h.set_queue_cfg("b", 3, 5, 0, 1); // deferrals, but no pop wait
        assert!(!h.min_pop_wait_in_play(), "deferrals must not arm the pop wait");
        h.set_queue_cfg("c", 0, 0, 10, 1);
        assert!(h.min_pop_wait_in_play(), "a configured window arms it");
    }

    // ---------------------------------------------------------------- E2
    // A whole cycle of main's background loops over one simulated minute: the 50ms
    // wheel tick, a consumer claiming whatever the ring offers, and the SQL coming
    // back with nothing. Returns how many claims the ring handed out.
    fn drive_empty(h: &HotList, from_ms: i64, to_ms: i64) -> usize {
        let mut claims = 0;
        let mut t = from_ms;
        while t < to_ms {
            h.tick(t);
            let c = h.take_batch("q", "g", 8, u32::MAX, t);
            if !c.is_empty() {
                claims += 1;
                h.checkin(
                    "q",
                    "g",
                    c.iter()
                        .map(|x| CheckinResult {
                            name: x.name.clone(),
                            epoch: x.epoch,
                            verdict: Verdict::Empty,
                            drained: false,
                        })
                        .collect(),
                    t,
                    true,
                    60_000,
                );
            }
            t += 50;
        }
        claims
    }

    // E2 (PLAN_HOTLIST_FOLLOWUP.md). The mesh dirty hint carries no consumer group, so
    // a peer's seek broadcast marks the partition on EVERY ring the peer holds for the
    // queue — including rings with nothing pending. On a plain queue that ghost costs
    // one empty SKIP LOCKED probe and clears. On a DEFERRAL queue the empty verdict
    // used to revisit unconditionally, which made the entry permanent: measured here
    // before the fix, 1,100 empty claims per simulated minute (~18/s, the 50ms revisit
    // floor), still claimable an hour later, and — since `evict_idle` requires every
    // entry IDLE — the QueueState pinned for the process lifetime.
    #[test]
    fn deferral_ghost_from_a_seek_broadcast_clears_and_frees_the_ring() {
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 0, 5, 0, 0); // windowBuffer 5s ⇒ deferral queue
        h.mark_remote("q", "p0", 0); // one T_HOTLIST_DIRTY_BATCH item, no data behind it
        // The ghost is allowed its ladder — one cut plus the confirming grace — and
        // nothing after it.
        assert!(drive_empty(&h, 0, 30_000) <= 200, "the revisit ladder must be bounded");
        assert_eq!(
            drive_empty(&h, 30_000, 60_000),
            0,
            "a ghost must stop probing, not ping-pong READY→WHEEL forever"
        );
        // The consumer leaves. The reconcile sweep must now be able to reclaim the ring
        // (the claims above kept it active, so the first sweep is its second chance).
        h.evict_idle();
        assert_eq!(h.evict_idle(), 1, "a queue holding only ghosts must be reclaimable");
        assert_eq!(h.queue_count(), 0);
    }

    // …and no seek is needed to arm it: one ordinary push, delivered, left the same
    // permanent entry (1,098 empty claims per simulated minute before the fix), so
    // every deferral queue that had ever served a message pinned its own ring.
    #[test]
    fn a_drained_deferral_partition_returns_to_idle() {
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 0, 5, 0, 0);
        h.mark_local("q", "p0", 1, 0);
        h.tick(5_100); // the window elapses
        let c = h.take_batch("q", "g", 8, u32::MAX, 5_100);
        assert_eq!(names(&c), vec!["p0"]);
        h.checkin(
            "q",
            "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Took, drained: true }],
            5_100,
            true,
            60_000,
        );
        assert_eq!(
            drive_empty(&h, 5_100, 65_000),
            1,
            "the caught-up partition takes one confirming probe, then clears"
        );
        h.evict_idle();
        assert_eq!(h.evict_idle(), 1, "a fully drained deferral queue must be reclaimable");
    }

    // The other side of the ghost test, and the reason it is dated from the last MARK
    // and not from the claim: on a windowBuffer queue the SQL debounce measures a quiet
    // period since the last write, so a partition under continuous writes claims EMPTY
    // while holding data. Every mark slides `pending_since`, so such an entry keeps its
    // place in the ring for as long as the writes last, however many empty claims it
    // serves in between.
    #[test]
    fn continuous_writes_never_make_a_deferral_entry_a_ghost() {
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 0, 5, 0, 0);
        let mut t = 0i64;
        while t < 60_000 {
            h.mark_local_quiet("q", "p0", 1, t); // the push that keeps the window unquiet
            let claimed = h.take_batch("q", "g", 8, u32::MAX, t);
            if !claimed.is_empty() {
                h.checkin(
                    "q",
                    "g",
                    vec![CheckinResult {
                        name: "p0".into(),
                        epoch: claimed[0].epoch,
                        verdict: Verdict::Empty,
                        drained: false,
                    }],
                    t,
                    true,
                    60_000,
                );
            }
            h.tick(t);
            t += 500;
        }
        h.evict_idle();
        h.evict_idle();
        assert_eq!(h.queue_count(), 1, "a written-to partition must stay tracked");
        h.tick(t + 6_000);
        assert_eq!(
            names(&h.take_batch("q", "g", 8, u32::MAX, t + 6_000)),
            vec!["p0"],
            "…and must still be delivered once the window finally goes quiet"
        );
    }

    // The broker schedules the wheel from ITS OWN clock and the SQL enforces the cut
    // from PG's, so a single empty claim on a deferral queue can be a probe that raced
    // the cut. One is never enough to clear an entry — the grace is one full
    // REVISIT_MAX_MS past the cut, which is at least one confirming revisit on either
    // queue shape (50ms floor for windowBuffer, 1s clamp for delayed).
    #[test]
    fn one_empty_claim_never_clears_a_deferral_entry() {
        let h = hl1();
        reg(&h, "q", "g");
        h.set_queue_cfg("q", 2, 0, 0, 0); // delayed 2s
        h.mark_local("q", "p0", 1, 0);
        h.tick(2_400); // mark + D + pad
        let c = h.take_batch("q", "g", 1, u32::MAX, 2_400);
        assert_eq!(names(&c), vec!["p0"]);
        h.checkin(
            "q", "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Empty, drained: false }],
            2_400, true, 60_000,
        );
        h.tick(4_600);
        let c = h.take_batch("q", "g", 1, u32::MAX, 4_600);
        assert_eq!(names(&c), vec!["p0"], "the first empty claim must only revisit");
        // Past cut + 2×pad + REVISIT_MAX_MS with no mark since: a ghost.
        h.checkin(
            "q", "g",
            vec![CheckinResult { name: "p0".into(), epoch: c[0].epoch, verdict: Verdict::Empty, drained: false }],
            4_600, true, 60_000,
        );
        h.tick(10_000);
        assert!(
            h.take_batch("q", "g", 1, u32::MAX, 10_000).is_empty(),
            "the confirmed ghost clears to IDLE"
        );
    }

    #[test]
    fn min_pop_wait_is_not_a_deferral() {
        let h = hl1();
        assert_eq!(h.min_pop_wait_ms("q"), 0, "absent config ⇒ OFF");
        h.set_queue_cfg("q", 0, 0, 25, 1);
        assert_eq!(h.min_pop_wait_ms("q"), 25);
        assert!(!h.cfg_of("q").is_deferral(), "min pop wait is not a deferral");
        assert!(!h.skip_window("q"), "min pop wait must not bypass the SQL window");
    }
}


pub(crate) fn trace_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
