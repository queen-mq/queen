// Broker-side dedup cache (doc 18 §5) — exact, HA-safe, storage-free.
//
// CORRECTNESS: this cache is an OPTIMIZATION ONLY. Its single load-bearing
// output is the `p_verified` watermark sent with a push: it tells SQL "the
// broker vouches that none of this bundle's hashes occur in (-inf, p_verified]",
// which lets log_push_one_v1 shrink its dedup probe to (p_verified, last_offset].
// Whenever the cache cannot vouch — unknown partition, hydration gap, kill
// switch — it answers -1, which forces the full-window SQL probe and is ALWAYS
// sound. A disabled cache is therefore exactly equivalent to always-(-1).
//
// CAP-PRESSURE SUPPRESSION (doc 18 §5 degradation fix) is a second always-sound
// lever built on that same guarantee. Under memory pressure the cache may
// decline to hydrate an evicted partition and answer -1 for it (the SUPPRESSED
// state), trading broker CPU/RAM for a PG full-window probe on that partition
// only. Like every other -1 it CANNOT change the dedup verdict — the server-side
// probe is authoritative — so suppression is purely a broker-resource trade. It
// exists because the pre-fix "re-hydrate the whole window on every push to an
// evicted partition" behaviour turned a full cache into a broker-side thrash
// loop (multi-MB hydration → evicts a hot partition → it re-hydrates → …) that
// collapsed throughput. See the constants block and needs_hydration.
// Positive membership (`LocalDuplicate`) is the other direction: a hash present
// in the cache was committed within the window. The current caller (fusion.rs
// `segment_verified`) NEVER short-circuits on a LocalDuplicate — it routes the
// segment to SQL with p_verified = -1 for the authoritative verdict WITH the
// original offsets the wire response needs. So LocalDuplicate is only a *hint*
// that steers the segment to the full-window SQL probe; it is never itself the
// dedup decision. A LocalDuplicate that is a shade stale (see the block-expiry
// note below) therefore costs at most one extra SQL probe, never a wrong verdict.
//
// Validity rule (§5): the cache may claim `p_verified = verified_upto` ONLY if
// it knows every hash in (hydrated_from, verified_upto] AND hydrated_from is at
// or below the window start. Both hold structurally here: entries are only born
// complete from a full-window hydration (`hydrate` with rows from log_txns over
// `created_at >= now()-window`), interleave gaps immediately mark the entry
// incomplete (`needs_topup` → -1 until the span is hydrated), and expiry raises
// `hydrated_from` exactly to the end of the blocks it drops (which are, by
// construction, entirely older than the window).
//
// This module is STORAGE-FREE: the caller performs all SQL (hydration queries,
// pushes) and feeds results in. No tokio, no pg types.
//
// -------------------------------------------------------------- storage model
// Each partition's in-window hashes live in a temporal ring of immutable, SORTED
// blocks (`sealed`), plus one mutable arrival-order accumulation buffer (`hot`).
// There is NO per-hash membership map: the block layout gives O(16B/hash) exact
// footprint and O(1)-amortised, BOUNDED allocation, which the old
// `HashMap<u128,u32>` (≈48–56 B/hash of bucket overhead, and window-proportional
// rehashes) could not. Three invariants make the block ring correct:
//
//   (1) NO REFCOUNT NEEDED. The old `set` was a refcount map so that two
//       in-window segments carrying the same hash (possible after a window
//       reconfig, or across brokers) did not un-know the second occurrence when
//       the first expired. Immutable blocks give that for free: a hash lives
//       *independently* in every block that contains it, membership is "present
//       in SOME surviving block" (binary-search each sealed block, scan `hot`),
//       and expiry drops WHOLE blocks — so a cross-block duplicate stays known
//       until its LAST containing block is dropped. Exactly the multiset
//       semantics the refcount provided, with none of its per-hash cost.
//
//   (2) BLOCK-GRANULAR EXPIRY (coarser than the old per-segment expiry) stays
//       sound. Blocks are time-ordered front→back (`max_created_ms` non-
//       decreasing — segments append in commit order, which is created_at-
//       monotone per the PUSHSER invariant, and a full rebuild sorts by
//       base==commit order). expire() drops a FRONT block only when its whole
//       time span is stale (`max_created_ms < cutoff`); it never splits a block.
//       Consequences, all sound:
//         • Coverage: every in-window hash sits in a block whose max_created_ms
//           is ≥ its own created_ms ≥ cutoff, so that block is retained ⇒ the
//           cache still knows every hash in (window_start, verified_upto]. The
//           resident set is a SUPERSET of the in-window hashes (a partly-stale
//           front block is kept until fully stale), which can only produce a
//           false LocalDuplicate — never a false "no-dup" — so vouching keeps no
//           false negatives.
//         • hydrated_from: a dropped block is entirely pre-window, so raising
//           hydrated_from to its `max_end_offset` discards only pre-window
//           offsets; every offset > hydrated_from with in-window time survives.
//           hydrated_from therefore stays at or below the window start.
//         • The only visible difference from per-segment expiry is that a hash
//           which just fell out of the window may linger (in a not-yet-fully-
//           stale block, or in `hot` on an idle partition) and yield a false
//           LocalDuplicate. Since the caller sends that segment to SQL with -1
//           regardless, the effect is at most one extra full-window probe — the
//           authoritative verdict is unchanged. Lingering `hot` is bounded by
//           one block and self-heals: it seals when it fills, then expires like
//           any other block; a cold partition is reclaimed whole by LRU.
//
//   (3) BOUNDED ALLOCATION. `hot` grows by ordinary Vec doubling but is SEALED
//       the instant it reaches `block_cap` (a power of two ⇒ the doubling lands
//       exactly on it, so the seal is a no-copy `into_boxed_slice` hand-off to an
//       immutable `Box<[u128]>`). Its capacity therefore never exceeds one block,
//       so the LARGEST allocation the cache can ever make is `block_cap * 16`
//       bytes. There is no allocation proportional to the window — the old
//       `HashMap` doubled its table toward the FULL window size (millions of
//       entries), so ~200 uniformly-loaded partitions crossing that threshold in
//       lockstep produced the synchronized multi-GB rehash bursts this design
//       removes; a capped 64 KiB hot buffer, sealed on a staggered per-partition
//       fill schedule, cannot. Letting the buffer double (rather than pre-
//       reserving block_cap) also means a lightly-loaded partition carries no
//       fixed slack — it allocates only for the hashes it holds. Hydration pre-
//       sizes the sealed ring from the row count so a full-window load allocates
//       each block once.
//
//   (4) BLOOM FRONT. Probing "absent" — the overwhelmingly common answer under
//       unique traffic — used to cost a linear scan of `hot` plus one binary
//       search PER sealed block: thousands of memory touches per hash once a
//       high-rate window accumulated hundreds of blocks (measured 2026-07-31:
//       60% of broker CPU at 1M msg/s × 300 s window, the T1 shape). Each entry
//       now fronts the exact ring with a temporal ring of GENERATIONAL BLOCKED
//       BLOOM FILTERS (`gens`): 16 bits/hash, k=7, all 7 probe bits inside one
//       64-byte cache line, so a definitive "absent" costs one line per live
//       generation (a handful) and skips `hot` and `sealed` entirely; a "maybe"
//       falls through to the exact path, which stays the sole authority for
//       LocalDuplicate. Soundness is unchanged: a bloom has NO false negatives,
//       every hash is inserted into the newest generation in the same
//       `append_run` that adds it to `hot`, and a generation is dropped only
//       when its `max_created_ms` watermark is entirely pre-window — the
//       whole-block expiry rule of invariant 2 at coarser grain, so any
//       in-window hash keeps its generation alive. A pre-window hash lingering
//       in a boundary-spanning block (or in `hot`) may answer "absent" once its
//       generation drops; that is CORRECT — vouching a pre-window hash cannot
//       admit an in-window duplicate — where the old behaviour (false
//       LocalDuplicate → extra SQL probe) was merely the slower sound answer.
//       A false positive costs one exact check. Generation capacities tier up
//       ×8 from `block_cap` to 1 M hashes (2 MiB of filter), so an idle
//       partition carries one tiny filter and a full-rate window settles at
//       ~3-4 live generations (~2 B/hash on top of the exact 16).
//
// hydrate() ALWAYS rebuilds the entry from the rows it is given, which the caller
// (fusion.rs hydrate_partition) always fetches as the FULL window
// (`created_at >= now()-window ORDER BY base_offset`). The old incremental
// span-merge branch is gone: full-window rows make it unnecessary, and the block
// layout keeps no per-hash offset to merge a gap span on. A rebuild from a
// current full-window snapshot is complete and sound under fusion's per-
// partition single-flight discipline (no push interleaves its own hydrate).
//
// Partition keys are `&str` (the partition id as text, as it flows through the
// broker's JSON) — the crate has no uuid dependency and gains nothing from one.
//
// CONCURRENCY (why the locking is sharded per entry): the cache is one
// `Arc<DedupCache>` shared by every fusion shard, but fusion flushes a given
// PARTITION single-flight — expire → needs_hydration → hydrate →
// verified_for_push → on_push_committed run sequentially for one pid, never
// concurrently, and partitions are shard-affine. The only cross-thread
// contention on a partition is therefore EVICTION, driven by OTHER partitions'
// flushes. So instead of one global Mutex around the whole map — which under
// uniform 1M-msg/s load serialized every flush behind whichever partition
// happened to be resizing — the map is a
// `RwLock<HashMap<pid, Arc<Mutex<Entry>>>>`. The common path read-locks the map
// only long enough to look up and clone the Arc, then does all its work under
// the PER-ENTRY Mutex, so a partition's block seal blocks only that partition
// (already single-flight ⇒ zero throughput cost), never its neighbours. A full
// rebuild builds the new Entry OFF-lock (it is not yet published) and takes the
// map write lock only to swap the Arc in. Global bookkeeping (the LRU byte
// budget `total_bytes`, the suppression cooldown and last-footprint maps) lives
// behind a separate small `Mutex<GlobalMeta>` taken only for O(1) updates.
// Eviction is the sole cross-partition operation: it takes the map write lock
// (only once actually over budget), scans with try_lock on each entry (skipping
// any mid-mutation entry — it is in use, hence recently used anyway), and removes
// whole partitions. `Entry::alive`, cleared under the entry lock on eviction/
// replacement, stops a stale single-flight owner (only reachable during a
// >RECENT_USE_GUARD_MS stall) from posting its byte delta against an entry that
// has already left the map.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// Answer for a push about to be flushed (one segment's hash bundle).
#[derive(Debug, PartialEq, Eq)]
pub enum PushCheck {
    /// These indices of the input bundle are present in the cache (committed
    /// within — or, under coarse block expiry, at the very edge of — the
    /// window). The caller routes such a segment to the authoritative SQL probe;
    /// it is a hint, never the verdict. Original-offset resolution is SQL's job.
    LocalDuplicate(Vec<usize>),
    /// Send this value as `p_verified`. -1 = cache can't vouch; SQL probes the
    /// whole window (always sound).
    Verified(i64),
}

/// What (if anything) the caller should hydrate for a partition before the
/// cache can vouch again. The CALLER runs the SQL and feeds rows to `hydrate`.
/// NOTE: the caller always fetches the FULL window regardless of the variant
/// (see fusion.rs hydrate_partition); the `Span` payload is retained as an
/// observable hint that an interleave gap was detected.
#[derive(Debug, PartialEq, Eq)]
pub enum HydrationNeed {
    /// Fetch the full window:
    ///   SELECT base_offset, end_offset, created_at, hashes FROM queen.log_txns
    ///   WHERE partition_id=$1 AND created_at >= now() - window
    Full,
    /// An interleave gap `[from, to]` (offsets, inclusive) was detected. The
    /// caller still fetches the full window; the entry rebuilds from it.
    Span { from: i64, to: i64 },
}

// -------------------------------------------------------------- memory model
// Approximate byte accounting (LRU eviction budget). Deliberately coarse but,
// unlike the old HashMap model, dominated by the raw 16 B hash: sorted blocks
// store one bare u128 per hash, plus a small fixed per-block header and the
// pre-reserved `hot` buffer's capacity (real resident bytes, so the LRU budget
// reflects the true footprint even while a hot block is only partly filled).
const BYTES_PER_HASH: usize = 16; // one u128 in a sealed block or in `hot`
const BYTES_PER_BLOCK: usize = 48; // Box fat-ptr + 2×i64 header + deque slot
const BYTES_PER_ENTRY: usize = 192; // fixed Entry overhead
const BYTES_PER_GEN: usize = 64; // BloomGen fixed overhead (fat ptr + counters + slot)

// Bloom-front generation sizing (invariant 4). A generation's filter is
// `cap / 32` cache-line blocks = `cap × 2` bytes (16 bits per expected hash,
// k=7). Capacities tier up ×8 per generation from `block_cap` so an idle
// partition pays one tiny filter while a full-rate 300 s window converges to a
// few 2 MiB generations (probe cost: one cache line each).
const GEN_TIER_FACTOR: usize = 8;
const GEN_CAP_MAX: usize = 1 << 20;

// Default accumulation-block capacity in hashes. A block is `block_cap * 16`
// bytes; MUST be a power of two so the hot buffer's doubling lands exactly on it
// and the seal stays no-copy (invariant 3). 4096 (a 64 KiB block) keeps the
// sealed-block count — hence probe cost — low (a 300 s / 1 M-msg/s window is
// ~1.5 M hashes/partition ≈ 366 blocks, a few thousand comparisons per probed
// hash) while the largest single allocation stays a negligible 64 KiB.
// Overridable in tests via `with_params` to exercise multi-block paths
// deterministically. NOT an env knob (kill switch + sizing stay in §5's
// QUEEN_DEDUP_CACHE / QUEEN_DEDUP_CACHE_MB).
const DEDUP_BLOCK_CAP: usize = 4096;

// ------------------------------------------------ cap-pressure suppression (§5)
// Cooldown a SUPPRESSED partition serves before it is re-considered for
// admission. Chosen at 30s: long enough that a hot resident set churning right
// at the cap does NOT re-test (and re-thrash) a rejected partition on every
// flush; short enough that a partition recovers within seconds once the pressure
// eases (window shrinks via /configure, offered load drops, or a cluster peer
// leaves and frees the shared budget). Correctness is independent of the value —
// a suppressed partition just uses the sound full-window SQL probe meanwhile.
const SUPPRESS_COOLDOWN_MS: i64 = 30_000;

// Admission hysteresis: a FULL (re)hydration is allowed only if the partition's
// estimated window footprint fits under HYDRATION_FIT_NUM/HYDRATION_FIT_DEN of
// the cap AFTER excluding its own resident bytes. The <100% headroom is what
// stops admission from immediately forcing eviction of the set it just joined —
// i.e. it converts ping-pong into a stable resident set + suppressed overflow.
const HYDRATION_FIT_NUM: usize = 9;
const HYDRATION_FIT_DEN: usize = 10;

// Eviction guard: never evict a partition used within this window to admit
// another — prefer suppressing the newcomer. Together with the fit test this
// pins a stable hot resident set instead of a churn loop.
const RECENT_USE_GUARD_MS: i64 = 5_000;

// Rate-limit for the one-line cap-pressure warning (once per minute, process-wide).
const SUPPRESS_WARN_INTERVAL_MS: i64 = 60_000;

// Soft cap for the auxiliary bookkeeping maps (per-pid cooldown deadlines and
// last-footprint hints). Beyond it we prune/reset so a workload that churns
// through unboundedly many distinct partition names can't leak these maps; the
// only cost of a reset is a re-measured footprint estimate on the next eviction.
const AUX_MAP_SOFT_CAP: usize = 100_000;

/// An immutable, sorted block of hashes. Sealed once and never mutated: this is
/// what makes cross-block duplicate membership free (invariant 1) and expiry a
/// whole-block drop (invariant 2).
struct Block {
    /// Ascending u128 hashes → O(log n) membership by binary search.
    hashes: Box<[u128]>,
    /// Newest constituent segment's commit time. Expiry drops this block only
    /// when this is strictly below the window cutoff (whole block is stale).
    max_created_ms: i64,
    /// Highest offset among this block's segments. On drop, `hydrated_from` is
    /// raised to this — every offset ≤ it is then, by construction, pre-window.
    max_end_offset: i64,
}

/// One generation of the bloom front (invariant 4): a register-blocked bloom
/// filter — `nblocks` 64-byte blocks, k=7 probe bits all inside the one block a
/// hash maps to — plus the same expiry watermark a sealed `Block` carries. The
/// filter is append-only while the generation is open (`len < cap`) and
/// immutable afterwards; expiry drops whole generations from the front exactly
/// like blocks, so the no-false-negative guarantee reduces to the same
/// "watermark entirely pre-window" argument.
struct BloomGen {
    /// `nblocks × 8` u64 words; block `b` = words[b*8 .. b*8+8].
    words: Box<[u64]>,
    nblocks: u64,
    /// Hashes inserted so far; at `cap` the generation is full and the next
    /// insert opens a new one.
    len: usize,
    /// Insert capacity this filter was sized for (16 bits/hash at `cap`).
    cap: usize,
    /// Newest constituent commit time — same expiry semantics as Block.
    max_created_ms: i64,
}

impl BloomGen {
    fn new(cap: usize) -> BloomGen {
        let cap = cap.max(32); // at least one 512-bit block
        let nblocks = cap / 32; // cap × 16 bits ÷ 512 bits per block
        BloomGen {
            words: vec![0u64; nblocks * 8].into_boxed_slice(),
            nblocks: nblocks as u64,
            len: 0,
            cap,
            max_created_ms: i64::MIN,
        }
    }

    /// Word index of the first word of `h`'s block. The block is picked by
    /// multiply-shift range reduction on the LOW 64 bits; the 7 probe bits come
    /// from disjoint 9-bit fields of the HIGH 64 bits, so block choice and bit
    /// choice never correlate. The input is already a content hash — no extra
    /// mixing is needed.
    #[inline]
    fn block_base(&self, h: u128) -> usize {
        ((((h as u64) as u128) * (self.nblocks as u128)) >> 64) as usize * 8
    }

    #[inline]
    fn insert(&mut self, h: u128, created_ms: i64) {
        let base = self.block_base(h);
        let hi = (h >> 64) as u64;
        for i in 0..7 {
            let p = ((hi >> (9 * i)) & 511) as usize;
            self.words[base + (p >> 6)] |= 1u64 << (p & 63);
        }
        self.len += 1;
        if created_ms > self.max_created_ms {
            self.max_created_ms = created_ms;
        }
    }

    #[inline]
    fn maybe(&self, h: u128) -> bool {
        let base = self.block_base(h);
        let hi = (h >> 64) as u64;
        for i in 0..7 {
            let p = ((hi >> (9 * i)) & 511) as usize;
            if self.words[base + (p >> 6)] & (1u64 << (p & 63)) == 0 {
                return false;
            }
        }
        true
    }

    fn bytes(&self) -> usize {
        self.words.len() * 8 + BYTES_PER_GEN
    }
}

struct Entry {
    /// Offsets strictly greater than this are fully known to the cache.
    hydrated_from: i64,
    /// The partition's last_offset as of our last successful push/hydration.
    verified_upto: i64,
    /// Time-ordered ring of sealed blocks, front = oldest, back = newest.
    sealed: VecDeque<Block>,
    /// Arrival-order accumulation buffer for the newest hashes. Doubles up to
    /// block_cap and is sealed the instant it reaches it, so its capacity never
    /// exceeds one block (invariant 3). Unsorted → membership scans it linearly
    /// (bounded by block_cap). It is always the newest data, so expiry never
    /// touches it.
    hot: Vec<u128>,
    /// Running max commit time of the segments currently in `hot` (i64::MIN when
    /// empty) — frozen into the block's `max_created_ms` on seal.
    hot_max_created_ms: i64,
    /// Running max offset of the segments currently in `hot` (i64::MIN when
    /// empty) — frozen into the block's `max_end_offset` on seal.
    hot_max_end_offset: i64,
    /// Bloom front (invariant 4): time-ordered generations, front = oldest.
    /// Superset of the in-window hashes of `hot` ∪ `sealed` — a definitive
    /// bloom miss short-circuits `contains` without touching either.
    gens: VecDeque<BloomGen>,
    /// Capacity for the NEXT generation (tiering state). 0 = start over from
    /// `block_cap` (fresh entry, or every generation expired).
    gen_next_cap: usize,
    /// Some((from, to)) = an interleave gap of unknown offsets; the entry can't
    /// vouch (-1) until it is rebuilt. Widened, never shrunk, by further
    /// interleaves; cleared by a (re)hydration.
    needs_topup: Option<(i64, i64)>,
    /// Defensive: the entry saw something impossible (offset overlap) and must
    /// be fully rebuilt before vouching again.
    needs_full: bool,
    /// Approximate footprint (kept in sync with `GlobalMeta::total_bytes`).
    bytes: usize,
    /// LRU stamp (global monotone tick) — orders eviction victims.
    last_used: u64,
    /// Wall-clock (epoch ms) of the last use — drives the RECENT_USE_GUARD_MS
    /// eviction guard so a partition touched in the last few seconds is never
    /// evicted to admit another. Separate from `last_used` because the guard is
    /// a real-time window, not an ordering.
    last_used_ms: i64,
    /// True while this Entry is the map's live representative for its pid.
    /// Cleared under the entry lock when eviction removes it or a full rebuild
    /// replaces it, so a single-flight owner that is (only under a multi-second
    /// stall) still mutating a now-detached Entry does not post its byte delta
    /// against `total_bytes` for a partition that has already left the map.
    alive: bool,
}

impl Entry {
    /// A fresh, empty entry (used by the full-rebuild path before it is filled).
    fn empty(now_ms: i64, tick: u64) -> Entry {
        Entry {
            hydrated_from: -1,
            verified_upto: -1,
            sealed: VecDeque::new(),
            hot: Vec::new(),
            hot_max_created_ms: i64::MIN,
            hot_max_end_offset: i64::MIN,
            gens: VecDeque::new(),
            gen_next_cap: 0,
            needs_topup: None,
            needs_full: false,
            bytes: 0,
            last_used: tick,
            last_used_ms: now_ms,
            alive: true,
        }
    }

    fn approx_bytes(&self) -> usize {
        let sealed_hashes: usize = self.sealed.iter().map(|b| b.hashes.len()).sum();
        BYTES_PER_ENTRY
            + self.sealed.len() * BYTES_PER_BLOCK
            + sealed_hashes * BYTES_PER_HASH
            + self.hot.capacity() * BYTES_PER_HASH
            + self.gens.iter().map(|g| g.bytes()).sum::<usize>()
    }

    fn hash_count(&self) -> usize {
        self.sealed.iter().map(|b| b.hashes.len()).sum::<usize>() + self.hot.len()
    }

    /// Membership: present in some surviving block, or in the hot buffer. No
    /// false negatives (soundness of the vouch rests on this); false positives
    /// are tolerated (a stale-but-retained hash routes its segment to SQL).
    ///
    /// The bloom front answers the common case: a definitive miss across every
    /// live generation proves absence (the generations are a superset of the
    /// in-window content of `hot` ∪ `sealed` — invariant 4) without touching
    /// the hot scan or a single block's binary search. Only a "maybe" pays the
    /// exact path.
    fn contains(&self, h: u128) -> bool {
        if !self.gen_maybe(h) {
            return false;
        }
        if self.hot.contains(&h) {
            return true;
        }
        self.sealed.iter().any(|b| b.hashes.binary_search(&h).is_ok())
    }

    /// Bloom-front probe: could `h` be present? Newest generation first —
    /// a real duplicate is most likely recent.
    #[inline]
    fn gen_maybe(&self, h: u128) -> bool {
        self.gens.iter().rev().any(|g| g.maybe(h))
    }

    /// Insert one hash into the newest generation, opening (and tiering) a new
    /// one when the current is full. Called by `append_run` for every hash that
    /// enters the entry — the source of the superset invariant.
    fn gen_insert(&mut self, h: u128, created_ms: i64, block_cap: usize) {
        let need_new = match self.gens.back() {
            Some(g) => g.len >= g.cap,
            None => true,
        };
        if need_new {
            let cap = if self.gen_next_cap == 0 { block_cap } else { self.gen_next_cap };
            let g = BloomGen::new(cap);
            self.gen_next_cap = (g.cap * GEN_TIER_FACTOR).min(GEN_CAP_MAX);
            self.gens.push_back(g);
        }
        self.gens.back_mut().unwrap().insert(h, created_ms);
    }

    /// Seal the hot buffer into an immutable sorted block. Called the instant hot
    /// fills (len == block_cap == capacity) so `into_boxed_slice` is a no-copy
    /// hand-off; the fresh hot buffer is re-reserved lazily on the next append.
    fn seal_hot(&mut self) {
        if self.hot.is_empty() {
            return;
        }
        let mut v = std::mem::take(&mut self.hot);
        v.sort_unstable();
        self.sealed.push_back(Block {
            hashes: v.into_boxed_slice(),
            max_created_ms: self.hot_max_created_ms,
            max_end_offset: self.hot_max_end_offset,
        });
        self.hot_max_created_ms = i64::MIN;
        self.hot_max_end_offset = i64::MIN;
    }

    /// Append one committed segment's hashes (all at commit time `created_ms`,
    /// occupying offsets `start_off..start_off+hs.len()-1`) to the hot buffer,
    /// sealing full blocks as it goes. The hot buffer grows by the usual Vec
    /// doubling but is SEALED the instant it reaches block_cap, so its capacity
    /// never exceeds block_cap (a power of two ⇒ doubling lands exactly on it,
    /// making the seal a no-copy `into_boxed_slice`). The largest allocation is
    /// therefore one block (block_cap*16 B); a lightly-loaded partition only ever
    /// allocates enough for the few hashes it holds, so there is no fixed
    /// per-partition slack and — the load-bearing point of invariant 3 — no
    /// resize is ever proportional to the whole window.
    fn append_run(&mut self, start_off: i64, created_ms: i64, hs: &[u128], block_cap: usize) {
        for (i, &h) in hs.iter().enumerate() {
            self.gen_insert(h, created_ms, block_cap);
            self.hot.push(h);
            let off = start_off + i as i64;
            if created_ms > self.hot_max_created_ms {
                self.hot_max_created_ms = created_ms;
            }
            if off > self.hot_max_end_offset {
                self.hot_max_end_offset = off;
            }
            if self.hot.len() >= block_cap {
                self.seal_hot();
            }
        }
    }
}

/// Global bookkeeping shared across all partitions. Every critical section here
/// is O(1) (or the O(#partitions) eviction scan, which holds the map write lock
/// and does no per-hash work) — this lock is NEVER held across a block seal.
struct GlobalMeta {
    total_bytes: usize,
    /// pid → cooldown deadline (epoch ms). Present ⇒ the partition is SUPPRESSED:
    /// `needs_hydration` returns None (skip the multi-MB query) and — because a
    /// suppressed partition is not resident — `verified_for_push` returns -1,
    /// until `now_ms >= deadline`, when the fit test is re-run.
    suppressed_until: HashMap<String, i64>,
    /// pid → resident footprint captured at its last eviction. The fit estimate
    /// for a re-hydration: a partition that filled the window before will do so
    /// again. Absent ⇒ never resident ⇒ estimate 0 (empty window, cheap to admit).
    last_bytes: HashMap<String, usize>,
}

/// Global broker-side dedup cache. Cheap to share (`Arc<DedupCache>`). Locking is
/// sharded PER ENTRY (see the CONCURRENCY note at the top of this module): the
/// outer `RwLock` guards only the map's shape; each partition's heavy work runs
/// under its own `Mutex<Entry>`, so a block seal blocks only that (single-flight)
/// partition, and `GlobalMeta` carries the cross-partition byte budget.
pub struct DedupCache {
    /// pid → the partition's entry, each behind its own Mutex. Read-locked for
    /// the common-path lookup+clone; write-locked only to insert (full rebuild)
    /// or remove (eviction) a partition.
    map: RwLock<HashMap<String, Arc<Mutex<Entry>>>>,
    /// LRU byte budget + suppression bookkeeping. See `GlobalMeta`.
    global: Mutex<GlobalMeta>,
    /// Monotone LRU tick, bumped on every touch that orders eviction victims.
    /// Atomic so the common path never takes the global lock just to advance it.
    tick: AtomicU64,
    max_bytes: usize,
    /// Fixed accumulation-block capacity in hashes (see DEDUP_BLOCK_CAP).
    block_cap: usize,
    enabled: bool,
    // Observability — minimal atomics for a metrics hookup, read lock-free from
    // fusion. `hydrations_total` counts hydration queries actually run (the thing
    // suppression exists to bound); `suppressed_gauge` tracks the live count of
    // suppressed partitions (≈ `suppressed_until.len()`).
    hydrations_total: AtomicU64,
    suppressed_gauge: AtomicI64,
    // Rate-limit stamp (epoch ms) for the one-line cap-pressure warning.
    last_warn_ms: AtomicI64,
}

fn parse_hash_blob(blob: &[u8]) -> Vec<u128> {
    blob.chunks_exact(16)
        .map(|c| u128::from_be_bytes(c.try_into().unwrap()))
        .collect()
}

impl DedupCache {
    /// `max_mb` = QUEEN_DEDUP_CACHE_MB (default 512); `enabled` =
    /// QUEEN_DEDUP_CACHE != "0". Disabled, every query answers "can't vouch".
    pub fn new(max_mb: usize, enabled: bool) -> DedupCache {
        DedupCache::with_params(max_mb.max(1) * 1024 * 1024, enabled, DEDUP_BLOCK_CAP)
    }

    #[cfg(test)]
    fn with_max_bytes(max_bytes: usize, enabled: bool) -> DedupCache {
        DedupCache::with_params(max_bytes, enabled, DEDUP_BLOCK_CAP)
    }

    fn with_params(max_bytes: usize, enabled: bool, block_cap: usize) -> DedupCache {
        DedupCache {
            map: RwLock::new(HashMap::new()),
            global: Mutex::new(GlobalMeta {
                total_bytes: 0,
                suppressed_until: HashMap::new(),
                last_bytes: HashMap::new(),
            }),
            tick: AtomicU64::new(0),
            max_bytes,
            block_cap: block_cap.max(1),
            enabled,
            hydrations_total: AtomicU64::new(0),
            suppressed_gauge: AtomicI64::new(0),
            last_warn_ms: AtomicI64::new(0),
        }
    }

    /// Total hydration queries the cache has asked its caller to run (Full or
    /// Span). Read-only accessor for a metrics hookup; suppression exists to keep
    /// this from exploding under cap pressure.
    pub fn hydrations_total(&self) -> u64 {
        self.hydrations_total.load(Ordering::Relaxed)
    }

    /// Approximate live count of SUPPRESSED partitions (gauge-ish). Read-only
    /// accessor for a metrics hookup.
    pub fn suppressed_partitions(&self) -> i64 {
        self.suppressed_gauge.load(Ordering::Relaxed)
    }

    /// Resident footprint (bytes) tracked by the LRU budget. O(1). Used by the
    /// fusion observability line to report the achieved bytes/hash.
    pub fn resident_bytes(&self) -> usize {
        self.global.lock().unwrap().total_bytes
    }

    /// Approximate count of resident hashes across all partitions. Iterates the
    /// map with try_lock (an entry mid-mutation is skipped), so it is a snapshot
    /// hint for the observability line, not an exact gauge — read it between
    /// flushes for an exact number.
    pub fn resident_hashes(&self) -> u64 {
        let map = self.map.read().unwrap();
        let mut n = 0u64;
        for arc in map.values() {
            if let Ok(e) = arc.try_lock() {
                n += e.hash_count() as u64;
            }
        }
        n
    }

    /// Next LRU tick.
    fn next_tick(&self) -> u64 {
        self.tick.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Look up a partition's entry and clone the Arc, holding the outer read
    /// lock only for the lookup itself. The per-entry work then happens under
    /// the returned `Mutex<Entry>`, off the map lock.
    fn get_arc(&self, pid: &str) -> Option<Arc<Mutex<Entry>>> {
        self.map.read().unwrap().get(pid).cloned()
    }

    /// Pre-push check for one segment's hash bundle (frame order).
    /// - `LocalDuplicate(idxs)`: those input indices are known committed within
    ///   (or at the edge of) the window → the caller routes the segment to SQL.
    /// - `Verified(w)`: send `p_verified = w`; `w = -1` when the cache can't
    ///   vouch (unknown partition, SUPPRESSED partition, pending top-up/rebuild,
    ///   kill switch). A SUPPRESSED partition is simply absent from the map, so
    ///   it falls into the `None => Verified(-1)` arm — always sound.
    pub fn verified_for_push(&self, pid: &str, hashes: &[[u8; 16]], now_ms: i64) -> PushCheck {
        if !self.enabled {
            return PushCheck::Verified(-1);
        }
        let tick = self.next_tick();
        let arc = match self.get_arc(pid) {
            Some(a) => a,
            None => return PushCheck::Verified(-1),
        };
        let mut e = arc.lock().unwrap();
        e.last_used = tick;
        e.last_used_ms = now_ms;
        let dups: Vec<usize> = hashes
            .iter()
            .enumerate()
            .filter(|(_, h)| e.contains(u128::from_be_bytes(**h)))
            .map(|(i, _)| i)
            .collect();
        if !dups.is_empty() {
            return PushCheck::LocalDuplicate(dups);
        }
        if e.needs_full || e.needs_topup.is_some() {
            return PushCheck::Verified(-1);
        }
        PushCheck::Verified(e.verified_upto)
    }

    /// Record a committed push: SQL returned base offset `base` for this bundle
    /// of `hashes` (frame order). §5 rules:
    /// - contiguous (`base == verified_upto+1`): append + advance.
    /// - gap (`base > verified_upto+1`): another broker interleaved. The push
    ///   itself was correct (SQL probed past our watermark), but the cache no
    ///   longer knows every hash below `base` — record the gap as `needs_topup`
    ///   so `verified_for_push` answers -1 until the entry is rebuilt.
    /// - overlap (`base <= verified_upto`): impossible under the monotone
    ///   allocator → mark for full rehydration (defensive).
    /// No-op for partitions never hydrated (the caller hydrates on first use;
    /// a push-seeded entry could not satisfy the window-coverage validity rule).
    pub fn on_push_committed(&self, pid: &str, base: i64, hashes: &[[u8; 16]], now_ms: i64) {
        if !self.enabled || hashes.is_empty() {
            return;
        }
        let tick = self.next_tick();
        let arc = match self.get_arc(pid) {
            Some(a) => a,
            None => return,
        };
        {
            let mut e = arc.lock().unwrap();
            e.last_used = tick;
            e.last_used_ms = now_ms;
            if e.needs_full {
                return; // unusable until rebuilt; don't grow it
            }
            let old_b = e.bytes;
            if base <= e.verified_upto {
                e.needs_full = true;
                return;
            }
            if base > e.verified_upto + 1 {
                let gap_from = e.verified_upto + 1;
                let gap_to = base - 1;
                e.needs_topup = Some(match e.needs_topup {
                    Some((f, _)) => (f.min(gap_from), gap_to),
                    None => (gap_from, gap_to),
                });
            }
            let hs: Vec<u128> = hashes.iter().map(|h| u128::from_be_bytes(*h)).collect();
            e.append_run(base, now_ms, &hs, self.block_cap);
            e.verified_upto = base + hashes.len() as i64 - 1;
            e.bytes = e.approx_bytes();
            let new_b = e.bytes;
            self.apply_bytes_delta(&e, old_b, new_b);
        }
        self.evict_lru(Some(pid), now_ms);
    }

    /// Build/rebuild an entry from log_txns rows
    /// `(base_offset, end_offset, created_ms, hashes_blob)`. The caller runs the
    /// SQL and always passes the FULL window (see the module header); rows outside
    /// the window (`created_ms < window_start_ms`) are ignored defensively, and a
    /// blob whose length disagrees with its offset span poisons the entry
    /// (needs_full). `current_last_offset` is the partition's last_offset fetched
    /// in the same round trip (-1 when unknown) — consulted only when the window
    /// holds no rows, per §5. Always a full rebuild published atomically.
    pub fn hydrate(
        &self,
        pid: &str,
        rows: Vec<(i64, i64, i64, Vec<u8>)>,
        window_start_ms: i64,
        current_last_offset: i64,
        now_ms: i64,
    ) {
        if !self.enabled {
            return;
        }
        // The caller only reaches here after needs_hydration approved a Full/Span
        // fetch, so this is exactly one hydration query.
        self.hydrations_total.fetch_add(1, Ordering::Relaxed);
        let tick = self.next_tick();

        // Parse + validate + sort rows by base (== commit == time order, PUSHSER).
        struct Hseg {
            base: i64,
            created_ms: i64,
            hashes: Vec<u128>,
        }
        let mut segs: Vec<Hseg> = Vec::with_capacity(rows.len());
        let mut malformed = false;
        let mut total_hashes = 0usize;
        for (base, end, created_ms, blob) in rows {
            if created_ms < window_start_ms {
                continue;
            }
            let hashes = parse_hash_blob(&blob);
            if hashes.is_empty() || hashes.len() as i64 != end - base + 1 {
                malformed = true;
                continue;
            }
            total_hashes += hashes.len();
            segs.push(Hseg { base, created_ms, hashes });
        }
        segs.sort_by_key(|s| s.base);

        // hydrated_from = MIN(base)-1, or -1 when the window is empty (anything
        // older is outside the window by construction). verified_upto = MAX(end),
        // or the partition's current last_offset when the window is empty. Build
        // on a LOCAL entry so all the block seals happen off any shared lock; the
        // map write lock is taken only to publish it. Pre-size the ring from the
        // row count so the load allocates each block exactly once.
        let (hydrated_from, verified_upto) = match (segs.first(), segs.last()) {
            (Some(first), Some(last)) => (
                first.base - 1,
                last.base + last.hashes.len() as i64 - 1,
            ),
            _ => (-1, current_last_offset),
        };
        let mut e = Entry::empty(now_ms, tick);
        e.hydrated_from = hydrated_from;
        e.verified_upto = verified_upto;
        e.needs_full = malformed;
        e.sealed.reserve(total_hashes / self.block_cap + 1);
        for seg in segs {
            e.append_run(seg.base, seg.created_ms, &seg.hashes, self.block_cap);
        }
        e.bytes = e.approx_bytes();
        let new_b = e.bytes;

        // Publish under the map write lock; the displaced entry (if any) is
        // marked not-alive so a stale owner can't post a delta for it.
        let old_arc = {
            let mut map = self.map.write().unwrap();
            map.insert(pid.to_string(), Arc::new(Mutex::new(e)))
        };
        let old_b = match old_arc {
            Some(a) => {
                let mut oe = a.lock().unwrap();
                oe.alive = false;
                oe.bytes
            }
            None => 0,
        };
        {
            let mut g = self.global.lock().unwrap();
            g.total_bytes = g.total_bytes + new_b - old_b;
        }
        // The partition is resident again: drop any SUPPRESSED marker and its
        // stale eviction footprint so bookkeeping stays bounded.
        {
            let mut g = self.global.lock().unwrap();
            Self::clear_suppressed_locked(&mut g, &self.suppressed_gauge, pid);
            g.last_bytes.remove(pid);
        }
        self.evict_lru(Some(pid), now_ms);
    }

    /// Apply a `[old_b → new_b]` footprint change to the global byte budget,
    /// but only while the entry is still the map's live representative. During a
    /// stall an owner can be mid-mutation on an entry that eviction already
    /// removed (and already subtracted); the `alive` gate keeps `total_bytes`
    /// from drifting up in that (rare, self-limited) window. Must be called with
    /// the entry locked (the `&Entry` proves it).
    fn apply_bytes_delta(&self, e: &Entry, old_b: usize, new_b: usize) {
        if !e.alive {
            return;
        }
        let mut g = self.global.lock().unwrap();
        g.total_bytes = g.total_bytes + new_b - old_b;
    }

    /// Does this partition need SQL hydration before the cache can vouch?
    /// None = fresh, cache disabled, OR SUPPRESSED under cap pressure: in the
    /// suppressed case the caller must NOT run any hydration query and the
    /// partition simply pushes with p_verified = -1 (full-window SQL probe —
    /// always sound). `now_ms` drives the suppression cooldown.
    ///
    /// Resident partitions are never suppressed: a clean entry needs nothing, and
    /// a needs_topup entry rebuilds cheaply. Only a FULL rebuild of an
    /// absent/poisoned partition — the multi-MB query that drove the thrash loop
    /// — is gated by the fit test + cooldown.
    pub fn needs_hydration(&self, pid: &str, now_ms: i64) -> Option<HydrationNeed> {
        if !self.enabled {
            return None;
        }
        if let Some(arc) = self.get_arc(pid) {
            let e = arc.lock().unwrap();
            if !e.needs_full {
                // Resident + vouching (None) or a resident interleave gap (Span);
                // both are cheap and always allowed, even under cap pressure.
                return e.needs_topup.map(|(from, to)| HydrationNeed::Span { from, to });
            }
        }
        // Absent, or resident-but-poisoned (needs_full): a FULL rebuild is
        // required — the thrash-prone path. Gate it.
        self.eval_full_hydration(pid, now_ms)
    }

    /// Decide whether a FULL (re)hydration of `pid` may run right now, or whether
    /// the partition should stay SUPPRESSED. Returns Some(Full) to admit, None to
    /// suppress. Marks/clears the cooldown + gauge as a side effect.
    fn eval_full_hydration(&self, pid: &str, now_ms: i64) -> Option<HydrationNeed> {
        // Estimate the re-hydration footprint. A poisoned resident entry uses its
        // own current bytes; an evicted one its footprint at eviction; a
        // never-seen partition estimates 0 (empty window — cheap to admit, which
        // is why brand-new partitions are never suppressed on first contact).
        // Snapshot the entry's bytes (if resident) off the global lock.
        let resident_bytes = self
            .get_arc(pid)
            .map(|a| a.lock().unwrap().bytes)
            .unwrap_or(0);

        let mut g = self.global.lock().unwrap();
        // Still inside the cooldown → stay suppressed, skip the multi-MB query.
        if let Some(&until) = g.suppressed_until.get(pid) {
            if now_ms < until {
                return None;
            }
            // Cooldown elapsed: fall through and re-test the fit.
        }
        let est = if resident_bytes > 0 {
            resident_bytes
        } else {
            g.last_bytes.get(pid).copied().unwrap_or(0)
        };
        // Fit test: everything EXCEPT this partition's own resident bytes (a
        // rebuild replaces them) plus the estimate must stay under the hysteresis
        // fraction of the cap. Integer form of `others + est <= cap * 9/10`.
        let others = g.total_bytes.saturating_sub(resident_bytes);
        let budget = self.max_bytes / HYDRATION_FIT_DEN * HYDRATION_FIT_NUM;
        if others + est <= budget {
            // Fits: admit. Clear any prior suppression so the caller hydrates.
            Self::clear_suppressed_locked(&mut g, &self.suppressed_gauge, pid);
            Some(HydrationNeed::Full)
        } else {
            // Doesn't fit: SUPPRESS with a fresh cooldown and decline the query.
            // Sound: the partition pushes with p_verified = -1 (SQL full probe).
            Self::mark_suppressed_locked(&mut g, &self.suppressed_gauge, pid, now_ms);
            drop(g);
            self.warn_cap_pressure(now_ms);
            None
        }
    }

    /// Record a SUPPRESSED cooldown for `pid` and keep the gauge in sync. Prunes
    /// already-expired deadlines first if the map has grown past its soft cap.
    fn mark_suppressed_locked(g: &mut GlobalMeta, gauge: &AtomicI64, pid: &str, now_ms: i64) {
        if g.suppressed_until.len() > AUX_MAP_SOFT_CAP {
            let before = g.suppressed_until.len();
            g.suppressed_until.retain(|_, &mut d| d > now_ms);
            let pruned = (before - g.suppressed_until.len()) as i64;
            gauge.fetch_sub(pruned, Ordering::Relaxed);
        }
        if g
            .suppressed_until
            .insert(pid.to_string(), now_ms + SUPPRESS_COOLDOWN_MS)
            .is_none()
        {
            gauge.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Clear `pid`'s SUPPRESSED marker (if any) and keep the gauge in sync.
    fn clear_suppressed_locked(g: &mut GlobalMeta, gauge: &AtomicI64, pid: &str) {
        if g.suppressed_until.remove(pid).is_some() {
            gauge.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// One rate-limited (once/min, process-wide) warning line while suppression
    /// is active, carrying the resident/cap footprint and the sizing formula so
    /// an operator can size QUEEN_DEDUP_CACHE_MB.
    fn warn_cap_pressure(&self, now_ms: i64) {
        let prev = self.last_warn_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(prev) < SUPPRESS_WARN_INTERVAL_MS {
            return;
        }
        if self
            .last_warn_ms
            .compare_exchange(prev, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return; // another thread just logged
        }
        let resident_mb = self.global.lock().unwrap().total_bytes / (1024 * 1024);
        let cap_mb = self.max_bytes / (1024 * 1024);
        let suppressed = self.suppressed_gauge.load(Ordering::Relaxed).max(0);
        tracing::warn!(
            target: "dedup",
            resident_mb,
            cap_mb,
            suppressed,
            knob = "QUEEN_DEDUP_CACHE_MB",
            sizing = "needed_mb ≈ 18 × msg_rate × window_seconds / 1e6",
            "cap pressure — degraded to SQL probes (sound, slower); raise QUEEN_DEDUP_CACHE_MB"
        );
    }

    /// Drop sealed blocks whose whole time span is older than the dedup window
    /// (raising `hydrated_from` to the end of everything dropped — those offsets
    /// are no longer known, but they no longer matter: outside the window they
    /// can't be duplicates). Callers run this once per flush per partition, BEFORE
    /// `verified_for_push`, so vouching reasons over the current window. Also
    /// enforces the global byte budget (whole-partition LRU eviction). The hot
    /// buffer is the newest data, so it is never dropped here — it seals and then
    /// expires like any other block, or is reclaimed whole by LRU on a cold pid.
    pub fn expire(&self, pid: &str, window_ms: i64, now_ms: i64) {
        if !self.enabled {
            return;
        }
        let cutoff = now_ms - window_ms;
        if let Some(arc) = self.get_arc(pid) {
            let mut e = arc.lock().unwrap();
            // We are actively flushing this partition this instant: mark it used
            // so the eviction guard below never evicts it out from under us.
            e.last_used_ms = now_ms;
            let old_b = e.bytes;
            while let Some(front) = e.sealed.front() {
                if front.max_created_ms >= cutoff {
                    break;
                }
                let block = e.sealed.pop_front().unwrap();
                e.hydrated_from = e.hydrated_from.max(block.max_end_offset);
            }
            // Bloom generations expire by the same whole-unit rule: a front
            // generation goes only when EVERYTHING it covers is pre-window, so
            // no in-window hash ever loses its bloom coverage (invariant 4). An
            // emptied ring restarts the capacity tiering from block_cap.
            while let Some(front) = e.gens.front() {
                if front.max_created_ms >= cutoff {
                    break;
                }
                e.gens.pop_front();
            }
            if e.gens.is_empty() {
                e.gen_next_cap = 0;
            }
            e.bytes = e.approx_bytes();
            let new_b = e.bytes;
            self.apply_bytes_delta(&e, old_b, new_b);
        }
        self.evict_lru(None, now_ms);
    }

    /// Evict least-recently-used partitions until under budget, with hysteresis.
    /// Two partitions are NEVER evicted to admit another:
    ///   - `protect` (the partition being served right now), and
    ///   - any partition used within RECENT_USE_GUARD_MS — evicting a hot
    ///     partition to admit another just starts the ping-pong the guard kills.
    /// If nothing is evictable, a stable hot resident set slightly over cap is
    /// TOLERATED (returning to under-cap is bounded by expire()/window and by
    /// suppression declining new admissions) — far cheaper than thrash. An
    /// evicted partition's footprint is remembered in `last_bytes` so its next
    /// re-hydration can be fit-tested before it is admitted. Evicted partitions
    /// re-hydrate on next push only if they then fit.
    ///
    /// Sharded-locking notes: the fast path (under budget) never takes the map
    /// write lock. When over budget it holds the map write lock — but does NO
    /// per-hash work, only try_lock scans (an entry currently mid-mutation is
    /// skipped, it is in use) and whole-partition removes. `alive` is cleared on
    /// the victim under its entry lock so a concurrent owner cannot resurrect its
    /// byte contribution.
    fn evict_lru(&self, protect: Option<&str>, now_ms: i64) {
        // Fast path: under budget → nothing to evict, and no map write lock.
        {
            let g = self.global.lock().unwrap();
            if g.total_bytes <= self.max_bytes {
                return;
            }
        }
        let mut map = self.map.write().unwrap();
        loop {
            {
                let g = self.global.lock().unwrap();
                if g.total_bytes <= self.max_bytes {
                    break;
                }
            }
            // Scan for the LRU evictable victim. try_lock each entry so an entry
            // that is mid-mutation (in use) is skipped, never blocked on.
            let victim = map
                .iter()
                .filter_map(|(k, arc)| {
                    let e = arc.try_lock().ok()?;
                    Some((k.clone(), e.last_used, e.last_used_ms))
                })
                .filter(|(k, _, lum)| {
                    Some(k.as_str()) != protect
                        && now_ms.saturating_sub(*lum) >= RECENT_USE_GUARD_MS
                })
                .min_by_key(|(_, lu, _)| *lu)
                .map(|(k, _, _)| k);
            match victim {
                Some(k) => {
                    if let Some(arc) = map.remove(&k) {
                        // Detach + read the footprint under the entry lock. This
                        // may briefly wait on a single-flight owner that grabbed
                        // the lock between the scan and here, but that critical
                        // section is CPU-bounded (never an .await).
                        let b = {
                            let mut e = arc.lock().unwrap();
                            e.alive = false;
                            e.bytes
                        };
                        let mut g = self.global.lock().unwrap();
                        g.total_bytes = g.total_bytes.saturating_sub(b);
                        // Bound the hint map under churn (cheap self-healing reset;
                        // the only cost is a re-measured estimate next eviction).
                        if g.last_bytes.len() > AUX_MAP_SOFT_CAP {
                            g.last_bytes.clear();
                        }
                        g.last_bytes.insert(k, b);
                    }
                }
                // Only protected/recently-used partitions remain → tolerate overage.
                None => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(n: u8) -> [u8; 16] {
        [n; 16]
    }

    fn blob(hs: &[[u8; 16]]) -> Vec<u8> {
        hs.iter().flatten().copied().collect()
    }

    fn total_bytes(c: &DedupCache) -> usize {
        c.global.lock().unwrap().total_bytes
    }

    // White-box accessors adapted to the per-entry-mutex layout.
    fn entry(c: &DedupCache, pid: &str) -> Arc<Mutex<Entry>> {
        c.map.read().unwrap().get(pid).unwrap().clone()
    }

    fn contains(c: &DedupCache, pid: &str) -> bool {
        c.map.read().unwrap().contains_key(pid)
    }

    fn last_bytes_of(c: &DedupCache, pid: &str) -> Option<usize> {
        c.global.lock().unwrap().last_bytes.get(pid).copied()
    }

    // Sum of every live entry's approx_bytes — must equal the global total.
    fn sum_entry_bytes(c: &DedupCache) -> usize {
        let map = c.map.read().unwrap();
        map.values().map(|a| a.lock().unwrap().bytes).sum()
    }

    // A wall-clock (epoch ms) far enough in the past that RECENT_USE_GUARD_MS
    // never accidentally protects an entry in the big-cap tests below.
    const T0: i64 = 1_000;

    #[test]
    fn contiguous_advance() {
        let c = DedupCache::with_max_bytes(1 << 20, true);
        // Unknown partition: can't vouch, wants full hydration.
        assert_eq!(c.verified_for_push("p1", &[h(1)], T0), PushCheck::Verified(-1));
        assert_eq!(c.needs_hydration("p1", T0), Some(HydrationNeed::Full));
        // Empty-window hydration → complete entry, watermark -1 (nothing to vouch past).
        c.hydrate("p1", vec![], 0, -1, T0);
        assert_eq!(c.needs_hydration("p1", T0), None);
        assert_eq!(c.verified_for_push("p1", &[h(1)], T0), PushCheck::Verified(-1));
        // Contiguous commits advance the watermark.
        c.on_push_committed("p1", 0, &[h(1), h(2)], 1_000);
        assert_eq!(c.verified_for_push("p1", &[h(3)], T0), PushCheck::Verified(1));
        c.on_push_committed("p1", 2, &[h(3)], 1_001);
        assert_eq!(c.verified_for_push("p1", &[h(4)], T0), PushCheck::Verified(2));
    }

    #[test]
    fn local_duplicate_detection() {
        let c = DedupCache::with_max_bytes(1 << 20, true);
        c.hydrate("p1", vec![], 0, -1, T0);
        c.on_push_committed("p1", 0, &[h(1), h(2)], 1_000);
        assert_eq!(
            c.verified_for_push("p1", &[h(9), h(1), h(2)], T0),
            PushCheck::LocalDuplicate(vec![1, 2])
        );
        // Positive hits stay sound even while incomplete (gap pending).
        c.on_push_committed("p1", 10, &[h(5)], 1_002);
        assert_eq!(
            c.verified_for_push("p1", &[h(2)], T0),
            PushCheck::LocalDuplicate(vec![0])
        );
    }

    #[test]
    fn interleave_gap_then_full_rebuild() {
        let c = DedupCache::with_max_bytes(1 << 20, true);
        c.hydrate("p1", vec![], 0, -1, T0);
        c.on_push_committed("p1", 0, &[h(1), h(2)], 1_000);
        // Another broker wrote offsets 2..=4: our push landed at 5.
        c.on_push_committed("p1", 5, &[h(6)], 1_001);
        assert_eq!(c.verified_for_push("p1", &[h(9)], T0), PushCheck::Verified(-1));
        // The interleave surfaces as a Span hint (the caller still fetches full).
        assert_eq!(
            c.needs_hydration("p1", T0),
            Some(HydrationNeed::Span { from: 2, to: 4 })
        );
        // Caller always re-fetches the FULL window; the entry rebuilds from it.
        c.hydrate(
            "p1",
            vec![
                (0, 1, 1_000, blob(&[h(1), h(2)])),
                (2, 4, 1_000, blob(&[h(3), h(4), h(5)])),
                (5, 5, 1_001, blob(&[h(6)])),
            ],
            0,
            5,
            T0,
        );
        assert_eq!(c.needs_hydration("p1", T0), None);
        assert_eq!(c.verified_for_push("p1", &[h(9)], T0), PushCheck::Verified(5));
        // Every rebuilt hash — including the gap span and the original tail — is
        // now a known duplicate.
        for x in [1u8, 2, 3, 4, 5, 6] {
            assert_eq!(
                c.verified_for_push("p1", &[h(x)], T0),
                PushCheck::LocalDuplicate(vec![0]),
                "h({x}) should be a known duplicate after rebuild"
            );
        }
    }

    #[test]
    fn block_granular_expiry_raises_hydrated_from() {
        // block_cap = 1 → each segment seals into its own block, so expiry is
        // exercised at block granularity. Two segments, only the older stale.
        let c = DedupCache::with_params(1 << 20, true, 1);
        c.hydrate(
            "p1",
            vec![
                (0, 0, 1_000, blob(&[h(1)])),
                (1, 1, 5_000, blob(&[h(3)])),
            ],
            0,
            1,
            1_000,
        );
        assert_eq!(
            c.verified_for_push("p1", &[h(1)], 6_000),
            PushCheck::LocalDuplicate(vec![0])
        );
        // Window = 3s at t=6s → the t=1s block falls out; the t=5s one stays.
        c.expire("p1", 3_000, 6_000);
        {
            let arc = entry(&c, "p1");
            let e = arc.lock().unwrap();
            assert_eq!(e.hydrated_from, 0); // raised to end of the dropped block
            assert_eq!(e.verified_upto, 1);
        }
        // Expired hash is no longer a duplicate; the entry still vouches; the
        // surviving block's hash is still a known duplicate.
        assert_eq!(c.verified_for_push("p1", &[h(1)], 6_000), PushCheck::Verified(1));
        assert_eq!(
            c.verified_for_push("p1", &[h(3)], 6_000),
            PushCheck::LocalDuplicate(vec![0])
        );
    }

    #[test]
    fn coarse_expiry_keeps_partial_block_but_stays_sound() {
        // block_cap = 4: both segments share ONE block, so a partly-stale block
        // is retained whole. The stale hash lingers (a false LocalDuplicate that
        // the caller sends to SQL — sound) and hydrated_from is NOT advanced.
        let c = DedupCache::with_params(1 << 20, true, 4);
        c.hydrate(
            "p1",
            vec![
                (0, 0, 1_000, blob(&[h(1)])),
                (1, 1, 5_000, blob(&[h(3)])),
            ],
            0,
            1,
            1_000,
        );
        c.expire("p1", 3_000, 6_000); // cutoff 3s; block max_created 5s ≥ cutoff → kept
        {
            let arc = entry(&c, "p1");
            let e = arc.lock().unwrap();
            assert_eq!(e.hydrated_from, -1, "partial block not dropped");
        }
        // The stale hash is still (falsely) a local duplicate → routed to SQL.
        assert_eq!(
            c.verified_for_push("p1", &[h(1)], 6_000),
            PushCheck::LocalDuplicate(vec![0])
        );
    }

    #[test]
    fn duplicate_hash_across_blocks_survives_partial_expiry() {
        // The refcount replacement: the SAME hash in two in-window blocks must
        // stay known when only the first block expires. block_cap = 1 forces two
        // blocks; only the older is stale.
        let c = DedupCache::with_params(1 << 20, true, 1);
        c.hydrate(
            "p1",
            vec![
                (0, 0, 1_000, blob(&[h(1)])),
                (1, 1, 5_000, blob(&[h(1)])),
            ],
            0,
            1,
            1_000,
        );
        c.expire("p1", 3_000, 6_000); // drops the first block only
        assert_eq!(
            c.verified_for_push("p1", &[h(1)], 6_000),
            PushCheck::LocalDuplicate(vec![0]),
            "cross-block duplicate must survive the first block's expiry"
        );
    }

    #[test]
    fn probe_hits_across_sealed_blocks_and_hot() {
        // block_cap = 2 → hashes land across several sealed blocks plus a partial
        // hot buffer. Every committed hash must be found (binary search across
        // sealed blocks + linear scan of hot); a never-seen hash must not.
        let c = DedupCache::with_params(1 << 20, true, 2);
        c.hydrate("p1", vec![], 0, -1, T0);
        // 5 hashes → two sealed blocks of 2 + one hot of 1.
        c.on_push_committed("p1", 0, &[h(1), h(2), h(3), h(4), h(5)], 1_000);
        {
            let arc = entry(&c, "p1");
            let e = arc.lock().unwrap();
            assert_eq!(e.sealed.len(), 2, "two sealed blocks");
            assert_eq!(e.hot.len(), 1, "one hot remainder");
            // Sealed blocks are sorted (binary-search invariant).
            for b in &e.sealed {
                assert!(b.hashes.windows(2).all(|w| w[0] <= w[1]), "block sorted");
            }
        }
        for x in [1u8, 2, 3, 4, 5] {
            assert_eq!(
                c.verified_for_push("p1", &[h(x)], T0),
                PushCheck::LocalDuplicate(vec![0]),
                "h({x}) present"
            );
        }
        assert_eq!(c.verified_for_push("p1", &[h(9)], T0), PushCheck::Verified(4));
    }

    #[test]
    fn lru_eviction_evicts_whole_partitions() {
        // Measure one entry's footprint, then budget for ~two and a half.
        let probe = DedupCache::with_max_bytes(1 << 20, true);
        probe.hydrate("p", vec![(0, 0, 1_000, blob(&[h(1)]))], 0, 0, T0);
        let one = total_bytes(&probe);

        let c = DedupCache::with_max_bytes(one * 2 + one / 2, true);
        c.hydrate("p1", vec![(0, 0, 1_000, blob(&[h(1)]))], 0, 0, T0);
        c.hydrate("p2", vec![(0, 0, 1_000, blob(&[h(2)]))], 0, 0, T0);
        // Age past the eviction guard so p2 is evictable; touch p1 so p2 is the LRU.
        let t = T0 + RECENT_USE_GUARD_MS + 1;
        assert_eq!(c.verified_for_push("p1", &[h(9)], t), PushCheck::Verified(0));
        // Third partition overflows the budget → p2 (LRU, past the guard) evicted whole.
        c.hydrate("p3", vec![(0, 0, 1_000, blob(&[h(3)]))], 0, 0, t);
        assert!(!contains(&c, "p2"), "p2 (LRU) evicted whole");
        assert!(contains(&c, "p1") && contains(&c, "p3"));
        // Its footprint is remembered so a re-hydration can be fit-tested.
        assert_eq!(last_bytes_of(&c, "p2"), Some(one));
        // Evicted partition is no longer resident → sound -1 until it re-hydrates.
        assert_eq!(c.verified_for_push("p2", &[h(9)], t), PushCheck::Verified(-1));
        assert!(total_bytes(&c) <= one * 2 + one / 2);
    }

    #[test]
    fn hot_partition_not_evicted_by_newcomer() {
        // The eviction guard must never evict a partition used within
        // RECENT_USE_GUARD_MS to admit another — a stable hot set is preserved
        // (overage tolerated) instead of ping-ponging.
        let probe = DedupCache::with_max_bytes(1 << 20, true);
        probe.hydrate("p", vec![(0, 0, 1_000, blob(&[h(1)]))], 0, 0, T0);
        let one = total_bytes(&probe);

        // Cap holds ~2 entries; two hot residents both used "now".
        let c = DedupCache::with_max_bytes(one * 2 + one / 2, true);
        c.hydrate("p1", vec![(0, 0, 1_000, blob(&[h(1)]))], 0, 0, 10_000);
        c.hydrate("p2", vec![(0, 0, 1_000, blob(&[h(2)]))], 0, 0, 10_000);
        // A third partition arrives while BOTH residents were used <5s ago. The
        // guard forbids evicting either hot partition, so all three stay resident.
        c.hydrate("p3", vec![(0, 0, 1_000, blob(&[h(3)]))], 0, 0, 10_001);
        assert!(contains(&c, "p1"), "hot p1 not evicted");
        assert!(contains(&c, "p2"), "hot p2 not evicted");
        assert!(contains(&c, "p3"), "newcomer admitted (overage tolerated)");
    }

    #[test]
    fn suppression_cooldown_returns_minus_one_without_hydration() {
        // An evicted partition that no longer fits is SUPPRESSED — no full
        // re-hydration query — and pushes with p_verified = -1 for a cooldown,
        // then recovers once pressure eases.
        let probe = DedupCache::with_max_bytes(1 << 20, true);
        probe.hydrate("p", vec![(0, 0, 1_000, blob(&[h(1)]))], 0, 0, T0);
        let one = total_bytes(&probe);

        // Cap ~2 entries; one hot resident, and "cold" seeded as previously
        // evicted at footprint ~one (deterministic setup of last_bytes).
        let c = DedupCache::with_max_bytes(one * 2, true); // budget = 0.9 * 2one = 1.8one
        c.hydrate("hot", vec![(0, 0, 1_000, blob(&[h(1)]))], 0, 0, 5_000);
        c.global.lock().unwrap().last_bytes.insert("cold".to_string(), one);

        let hydrations_before = c.hydrations_total();
        // others(one) + est(one) = 2one > budget(1.8one) → SUPPRESS, no query.
        assert_eq!(c.needs_hydration("cold", 5_000), None);
        assert_eq!(c.suppressed_partitions(), 1);
        // Suppressed ⇒ sound -1 without any hydration need.
        assert_eq!(
            c.verified_for_push("cold", &[h(2)], 5_000),
            PushCheck::Verified(-1)
        );
        // Still suppressed inside the cooldown; no hydration ran either time.
        assert_eq!(
            c.needs_hydration("cold", 5_000 + SUPPRESS_COOLDOWN_MS - 1),
            None
        );
        assert_eq!(c.hydrations_total(), hydrations_before);

        // Pressure eases (hot partition drops away) and the cooldown elapses →
        // the fit test now passes → a Full hydration is admitted, marker cleared.
        {
            let mut map = c.map.write().unwrap();
            map.clear();
            drop(map);
            c.global.lock().unwrap().total_bytes = 0;
        }
        assert_eq!(
            c.needs_hydration("cold", 5_000 + SUPPRESS_COOLDOWN_MS),
            Some(HydrationNeed::Full)
        );
        assert_eq!(c.suppressed_partitions(), 0);
    }

    #[test]
    fn resident_gap_topup_allowed_under_pressure() {
        // A resident partition's interleave gap is ALWAYS offered a hydration
        // hint, even at/over cap — it rebuilds cheaply and can't thrash.
        let probe = DedupCache::with_max_bytes(1 << 20, true);
        probe.hydrate("p", vec![(0, 0, 1_000, blob(&[h(1)]))], 0, 0, T0);
        let one = total_bytes(&probe);

        let c = DedupCache::with_max_bytes(one, true); // deliberately tight
        c.hydrate("p1", vec![], 0, -1, 1_000);
        c.on_push_committed("p1", 0, &[h(1), h(2)], 1_000);
        // Interleave gap → needs a rebuild. Still offered despite cap pressure.
        c.on_push_committed("p1", 5, &[h(6)], 1_001);
        assert_eq!(
            c.needs_hydration("p1", 2_000),
            Some(HydrationNeed::Span { from: 2, to: 4 })
        );
    }

    #[test]
    fn overlap_degrades_to_full_rehydration() {
        let c = DedupCache::with_max_bytes(1 << 20, true);
        c.hydrate("p1", vec![], 0, -1, T0);
        c.on_push_committed("p1", 0, &[h(1), h(2)], 1_000);
        // Impossible under the monotone allocator → defensive full rebuild.
        c.on_push_committed("p1", 1, &[h(3)], 1_001);
        assert_eq!(c.verified_for_push("p1", &[h(9)], T0), PushCheck::Verified(-1));
        assert_eq!(c.needs_hydration("p1", T0), Some(HydrationNeed::Full));
        // A full hydration heals it.
        c.hydrate("p1", vec![(0, 1, 1_000, blob(&[h(1), h(2)]))], 0, 1, T0);
        assert_eq!(c.verified_for_push("p1", &[h(9)], T0), PushCheck::Verified(1));
    }

    #[test]
    fn disabled_cache_always_declines() {
        let c = DedupCache::with_max_bytes(1 << 20, false);
        c.hydrate("p1", vec![(0, 0, 1_000, blob(&[h(1)]))], 0, 0, T0);
        c.on_push_committed("p1", 1, &[h(2)], 1_001);
        assert_eq!(c.needs_hydration("p1", T0), None);
        assert_eq!(c.verified_for_push("p1", &[h(1)], T0), PushCheck::Verified(-1));
        assert_eq!(total_bytes(&c), 0);
        // Disabled ⇒ no hydration bookkeeping at all.
        assert_eq!(c.hydrations_total(), 0);
    }

    #[test]
    fn byte_accounting_matches_sum_of_entries() {
        // The global total must always equal the sum of live entries' footprints,
        // through hydration, contiguous growth (crossing seal boundaries),
        // expiry (whole-block drop), and eviction.
        let c = DedupCache::with_params(64 << 20, true, 4);
        // Two partitions, one loaded across several blocks.
        let hs: Vec<[u8; 16]> = (0..30u16).map(|i| hb((i >> 8) as u8, i as u8)).collect();
        c.hydrate("p1", vec![(0, 29, 1_000, blob(&hs))], 0, 29, T0);
        c.hydrate("p2", vec![(0, 0, 2_000, blob(&[h(200)]))], 0, 0, T0);
        assert_eq!(total_bytes(&c), sum_entry_bytes(&c), "after hydrate");
        // Grow p1 across more seal boundaries.
        let more: Vec<[u8; 16]> = (30..50u16).map(|i| hb((i >> 8) as u8, i as u8)).collect();
        c.on_push_committed("p1", 30, &more, 1_500);
        assert_eq!(total_bytes(&c), sum_entry_bytes(&c), "after growth");
        // Expire some of p1's older blocks (block_cap=4 → several blocks).
        c.expire("p1", 200, 1_000_000);
        assert_eq!(total_bytes(&c), sum_entry_bytes(&c), "after expiry");
        assert!(total_bytes(&c) > 0);
    }

    #[test]
    fn hydrate_presizes_and_seals_blocks() {
        // A full hydration builds sorted sealed blocks + a hot remainder, pre-
        // sized from the row count, with an exact hash count.
        let c = DedupCache::with_params(64 << 20, true, 128);
        let hs: Vec<[u8; 16]> = (0..300u16).map(|i| hb((i >> 8) as u8, i as u8)).collect();
        c.hydrate("p1", vec![(0, 299, 1_000, blob(&hs))], 0, 299, T0);
        {
            let arc = entry(&c, "p1");
            let e = arc.lock().unwrap();
            // 300 hashes, block_cap 128 → 2 sealed blocks (256) + 44 hot.
            assert_eq!(e.sealed.len(), 2, "two sealed blocks");
            assert_eq!(e.hot.len(), 300 - 256, "hot remainder");
            assert_eq!(e.hash_count(), 300, "exact count");
            for b in &e.sealed {
                assert!(b.hashes.windows(2).all(|w| w[0] <= w[1]), "sorted block");
            }
        }
        // Contiguous batch of 100 more: seals another block, count grows.
        let more: Vec<[u8; 16]> = (300..400u16).map(|i| hb((i >> 8) as u8, i as u8)).collect();
        c.on_push_committed("p1", 300, &more, 1_001);
        {
            let arc = entry(&c, "p1");
            let e = arc.lock().unwrap();
            assert_eq!(e.hash_count(), 400, "exact count after growth");
        }
        assert_eq!(total_bytes(&c), sum_entry_bytes(&c));
    }

    // ---------------------------------------------------------------- sharding

    // Build a 16-byte hash uniquely from (a, b) so probes never collide with
    // pushed hashes in the concurrency tests.
    fn hb(a: u8, b: u8) -> [u8; 16] {
        let mut x = [0u8; 16];
        x[0] = a;
        x[1] = b;
        x
    }

    #[test]
    fn concurrent_distinct_partitions_are_independent() {
        // Sharding correctness: N threads driving the full API against DISTINCT
        // partitions must each see their own partition's state, with no global
        // lock corrupting another's.
        use std::thread;
        let c = Arc::new(DedupCache::with_max_bytes(64 << 20, true));
        let mut handles = Vec::new();
        for t in 1..=8u8 {
            let c = c.clone();
            handles.push(thread::spawn(move || {
                let pid = format!("p{t}");
                c.hydrate(&pid, vec![], 0, -1, 1_000);
                for i in 0..50i64 {
                    // exercise the gate + expire paths concurrently too
                    let _ = c.needs_hydration(&pid, 1_000 + i);
                    c.expire(&pid, 1_000_000, 1_000 + i);
                    c.on_push_committed(&pid, i, &[hb(t, i as u8)], 1_000 + i);
                    match c.verified_for_push(&pid, &[hb(t, 200)], 1_000 + i) {
                        PushCheck::Verified(w) => assert_eq!(w, i, "{pid} watermark @{i}"),
                        other => panic!("{pid}: unexpected {other:?}"),
                    }
                    // its own committed hash is a local duplicate
                    assert_eq!(
                        c.verified_for_push(&pid, &[hb(t, i as u8)], 1_000 + i),
                        PushCheck::LocalDuplicate(vec![0]),
                        "{pid} self-dup @{i}"
                    );
                }
            }));
        }
        for hnd in handles {
            hnd.join().unwrap();
        }
        // All 8 partitions resident, each with its full 50-deep watermark.
        assert_eq!(c.map.read().unwrap().len(), 8);
        for t in 1..=8u8 {
            let pid = format!("p{t}");
            assert_eq!(
                c.verified_for_push(&pid, &[hb(t, 201)], 3_000),
                PushCheck::Verified(49),
                "{pid} final watermark"
            );
        }
    }

    #[test]
    fn eviction_under_contention_does_not_deadlock() {
        // The try_lock eviction scan + blocking detach must not deadlock when
        // many threads churn partitions under a tight cap. If the locking were
        // wrong this test would hang; reaching the asserts is the signal. Also
        // sanity-checks the budget stays bounded.
        use std::thread;
        let probe = DedupCache::with_max_bytes(1 << 20, true);
        probe.hydrate("p", vec![(0, 0, 1_000, blob(&[h(1)]))], 0, 0, T0);
        let one = total_bytes(&probe);

        let c = Arc::new(DedupCache::with_max_bytes(one * 4, true));
        let mut handles = Vec::new();
        for t in 0..8u32 {
            let c = c.clone();
            handles.push(thread::spawn(move || {
                for r in 0..300u32 {
                    let pid = format!("p{}_{}", t, r % 24); // churn > cap
                    let base = r as i64;
                    let now = 10_000 + r as i64;
                    c.hydrate(
                        &pid,
                        vec![(base, base, now, blob(&[h((r % 250) as u8)]))],
                        0,
                        base,
                        now,
                    );
                    c.expire(&pid, 1_000, now + RECENT_USE_GUARD_MS + 1);
                    let _ = c.verified_for_push(&pid, &[h(200)], now + RECENT_USE_GUARD_MS + 1);
                }
            }));
        }
        for hnd in handles {
            hnd.join().unwrap();
        }
        // Reaching here at all is the deadlock-freedom signal. The resident set
        // is bounded by the DISTINCT working set (8 threads × 24 keys = 192
        // partitions), not by the 2400 total hydrations.
        let distinct = 8 * 24;
        assert!(
            total_bytes(&c) <= one * (distinct + 64),
            "resident set bounded by working set, not history: {} bytes",
            total_bytes(&c)
        );
    }

    // -------------------------------------------------------------- bloom front

    #[test]
    fn bloom_front_never_hides_a_present_hash() {
        // Superset invariant end-to-end: block_cap=1 forces one block per hash
        // and a small first-tier generation (32), so 200 committed hashes span
        // many sealed blocks AND several generations. Every one must still be
        // reported as a LocalDuplicate (bloom "maybe" → exact hit), and a
        // never-committed hash must vouch — the exact path stays the authority
        // on every "maybe".
        let c = DedupCache::with_params(64 << 20, true, 1);
        c.hydrate("p1", vec![], 0, -1, T0);
        let hs: Vec<[u8; 16]> = (0..200u16).map(|i| hb(0, i as u8)).collect();
        c.on_push_committed("p1", 0, &hs, 1_000);
        {
            let arc = entry(&c, "p1");
            let e = arc.lock().unwrap();
            assert!(e.gens.len() >= 2, "several generations live: {}", e.gens.len());
        }
        for (i, x) in hs.iter().enumerate() {
            assert_eq!(
                c.verified_for_push("p1", &[*x], T0),
                PushCheck::LocalDuplicate(vec![0]),
                "hash {i} must stay visible through the bloom front"
            );
        }
        assert_eq!(
            c.verified_for_push("p1", &[hb(9, 201)], T0),
            PushCheck::Verified(199)
        );
    }

    #[test]
    fn gen_expiry_drops_only_prewindow_coverage() {
        // Two runs far apart in time. After an expiry whose cutoff falls between
        // them: the old run's blocks AND its fully-pre-window generation are
        // gone, the shared generation survives on the new run's watermark, so
        // every in-window hash stays a LocalDuplicate and every pre-window hash
        // correctly vouches as absent (whether its generation was dropped or a
        // surviving generation yields a false positive, the exact path settles
        // it — the assertion is deterministic either way).
        let c = DedupCache::with_params(64 << 20, true, 1);
        let old: Vec<[u8; 16]> = (0..40u16).map(|i| hb(1, i as u8)).collect();
        let new: Vec<[u8; 16]> = (0..5u16).map(|i| hb(2, i as u8)).collect();
        c.hydrate("p1", vec![], 0, -1, T0);
        c.on_push_committed("p1", 0, &old, 1_000);
        c.on_push_committed("p1", 40, &new, 50_000);
        {
            let arc = entry(&c, "p1");
            let e = arc.lock().unwrap();
            assert!(e.gens.len() >= 2, "two generations before expiry");
        }
        // Window 10 s at t=55 s → cutoff 45 s: the t=1 s run is pre-window whole.
        c.expire("p1", 10_000, 55_000);
        {
            let arc = entry(&c, "p1");
            let e = arc.lock().unwrap();
            assert_eq!(e.gens.len(), 1, "pre-window generation dropped");
            assert!(e.gens.front().unwrap().max_created_ms >= 45_000);
        }
        for x in &new {
            assert_eq!(
                c.verified_for_push("p1", &[*x], 55_000),
                PushCheck::LocalDuplicate(vec![0]),
                "in-window hash must survive generation expiry"
            );
        }
        for x in &old {
            assert_eq!(
                c.verified_for_push("p1", &[*x], 55_000),
                PushCheck::Verified(44),
                "pre-window hash vouches as absent"
            );
        }
    }

    #[test]
    fn bloom_bytes_are_accounted() {
        // Generation filters are resident bytes: they must flow through
        // approx_bytes into the global budget, and expiring generations must
        // release them (with the tiering reset for the next fill).
        let c = DedupCache::with_params(64 << 20, true, 1);
        c.hydrate("p1", vec![], 0, -1, T0);
        let hs: Vec<[u8; 16]> = (0..64u16).map(|i| hb(3, i as u8)).collect();
        c.on_push_committed("p1", 0, &hs, 1_000);
        assert_eq!(total_bytes(&c), sum_entry_bytes(&c), "gens included in budget");
        let before = total_bytes(&c);
        // Everything pre-window → all blocks and all generations drop; hot is
        // empty (block_cap=1 seals every hash), so only fixed overhead remains.
        c.expire("p1", 1_000, 1_000_000);
        {
            let arc = entry(&c, "p1");
            let e = arc.lock().unwrap();
            assert!(e.gens.is_empty(), "all generations expired");
            assert_eq!(e.gen_next_cap, 0, "tiering reset for the next fill");
        }
        assert_eq!(total_bytes(&c), sum_entry_bytes(&c), "after expiry");
        assert!(total_bytes(&c) < before, "filter bytes released");
    }
}
