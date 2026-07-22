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
// in the cache was committed within the window, so the broker may short-circuit
// the push without SQL. That claim is only window-accurate if the caller runs
// `expire(pid, window_ms, now_ms)` with the queue's current dedup window before
// consulting `verified_for_push` in the same flush — fusion does this once per
// flush per partition.
//
// Validity rule (§5): the cache may claim `p_verified = verified_upto` ONLY if
// it knows every hash in (hydrated_from, verified_upto] AND hydrated_from is at
// or below the window start. Both hold structurally here: entries are only born
// complete from a full-window hydration (`hydrate` with rows from log_txns over
// `created_at >= now()-window`), interleave gaps immediately mark the entry
// incomplete (`needs_topup` → -1 until the span is hydrated), and expiry raises
// `hydrated_from` exactly to the end of the segments it drops (which are, by
// construction, older than the window).
//
// This module is STORAGE-FREE: the caller performs all SQL (hydration queries,
// pushes) and feeds results in. No tokio, no pg types.
//
// Deviations from the §5 sketch, both documented for the reviewer:
// - The per-segment ring stores the segment's hashes (`Vec<u128>`) rather than a
//   bare count: window expiry must be able to REMOVE the expired hashes from the
//   set, otherwise stale members would (a) leak memory unboundedly within a
//   partition and (b) produce false `LocalDuplicate` rejects for hashes whose
//   originals already fell out of the window — a correctness bug, since
//   LocalDuplicate short-circuits the push. Count alone cannot do that.
// - `set` is a refcount map (u128 → u32) instead of a HashSet: after a window
//   reconfiguration (off→on, or a widened window) two in-window segments can
//   legitimately carry the same hash; plain-set removal on the first segment's
//   expiry would silently un-know the second occurrence and let a real
//   duplicate through the vouched prefix. Membership semantics are identical.
// - Partition keys are `&str` (the partition id as text, as it flows through
//   the broker's JSON) — the crate has no uuid dependency and gains nothing
//   from one here.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;

/// Answer for a push about to be flushed (one segment's hash bundle).
#[derive(Debug, PartialEq, Eq)]
pub enum PushCheck {
    /// These indices of the input bundle are already present in the cache
    /// (committed within the window) — the broker may short-circuit them as
    /// duplicates without SQL. Original-offset resolution is the caller's job.
    LocalDuplicate(Vec<usize>),
    /// Send this value as `p_verified`. -1 = cache can't vouch; SQL probes the
    /// whole window (always sound).
    Verified(i64),
}

/// What (if anything) the caller should hydrate for a partition before the
/// cache can vouch again. The CALLER runs the SQL and feeds rows to `hydrate`.
#[derive(Debug, PartialEq, Eq)]
pub enum HydrationNeed {
    /// Fetch the full window:
    ///   SELECT base_offset, end_offset, created_at, hashes FROM queen.log_txns
    ///   WHERE partition_id=$1 AND created_at >= now() - window
    Full,
    /// Fetch only the interleave gap `[from, to]` (offsets, inclusive):
    ///   ... AND end_offset >= from AND base_offset <= to
    Span { from: i64, to: i64 },
}

// -------------------------------------------------------------- memory model
// Approximate byte accounting (LRU eviction budget). Deliberately coarse and
// pessimistic: 16B key + 4B refcount + hashmap bucket overhead for the set,
// 16B per hash in the ring, fixed per-segment and per-partition overheads.
const BYTES_PER_SET_HASH: usize = 56;
const BYTES_PER_RING_HASH: usize = 16;
const BYTES_PER_SEG: usize = 64;
const BYTES_PER_ENTRY: usize = 256;

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

/// One committed segment inside the window, in base-offset (== commit-time,
/// per the PUSHSER invariant) order.
struct RingSeg {
    base: i64,
    created_ms: i64,
    hashes: Vec<u128>, // frame order; count = hashes.len()
}

impl RingSeg {
    fn end(&self) -> i64 {
        self.base + self.hashes.len() as i64 - 1
    }
}

struct Entry {
    /// Offsets strictly greater than this are fully known to the cache.
    hydrated_from: i64,
    /// The partition's last_offset as of our last successful push/hydration.
    verified_upto: i64,
    /// Refcounted membership over every hash currently inside the window.
    set: HashMap<u128, u32>,
    /// Per-segment ring for window expiry, sorted by base offset.
    ring: VecDeque<RingSeg>,
    /// Some((from, to)) = an interleave gap of unknown offsets; the entry can't
    /// vouch (-1) until the span is hydrated. Widened, never shrunk, by
    /// further interleaves; cleared by a Span hydration.
    needs_topup: Option<(i64, i64)>,
    /// Defensive: the entry saw something impossible (offset overlap, malformed
    /// hydration row) and must be fully rebuilt before vouching again.
    needs_full: bool,
    /// Approximate footprint (kept in sync with `total_bytes`).
    bytes: usize,
    /// LRU stamp (global monotone tick) — orders eviction victims.
    last_used: u64,
    /// Wall-clock (epoch ms) of the last use — drives the RECENT_USE_GUARD_MS
    /// eviction guard so a partition touched in the last few seconds is never
    /// evicted to admit another (Fix 2). Separate from `last_used` because the
    /// guard is a real-time window, not an ordering.
    last_used_ms: i64,
}

impl Entry {
    fn approx_bytes(&self) -> usize {
        let ring_hashes: usize = self.ring.iter().map(|s| s.hashes.len()).sum();
        BYTES_PER_ENTRY
            + self.set.len() * BYTES_PER_SET_HASH
            + ring_hashes * BYTES_PER_RING_HASH
            + self.ring.len() * BYTES_PER_SEG
    }

    fn insert_hash(&mut self, h: u128) {
        *self.set.entry(h).or_insert(0) += 1;
    }

    fn remove_hash(&mut self, h: u128) {
        if let Some(c) = self.set.get_mut(&h) {
            if *c <= 1 {
                self.set.remove(&h);
            } else {
                *c -= 1;
            }
        }
    }
}

struct Inner {
    map: HashMap<String, Entry>,
    total_bytes: usize,
    tick: u64,
    /// pid → cooldown deadline (epoch ms). Present ⇒ the partition is SUPPRESSED:
    /// `needs_hydration` returns None (skip the multi-MB query) and — because a
    /// suppressed partition is not resident — `verified_for_push` returns -1,
    /// until `now_ms >= deadline`, when the fit test is re-run (Fix 1).
    suppressed_until: HashMap<String, i64>,
    /// pid → resident footprint captured at its last eviction. The fit estimate
    /// for a re-hydration: a partition that filled the window before will do so
    /// again. Absent ⇒ never resident ⇒ estimate 0 (empty window, cheap to admit).
    last_bytes: HashMap<String, usize>,
}

/// Global broker-side dedup cache. Cheap to share (`Arc<DedupCache>`); one
/// mutex around the whole map — contention is per-flush, not per-message.
pub struct DedupCache {
    inner: Mutex<Inner>,
    max_bytes: usize,
    enabled: bool,
    // Observability (Fix 3) — minimal atomics for a future metrics hookup, read
    // lock-free from fusion. `hydrations_total` counts hydration queries actually
    // run (the thing suppression exists to bound); `suppressed_gauge` tracks the
    // live count of suppressed partitions (≈ `suppressed_until.len()`).
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
        DedupCache::with_max_bytes(max_mb.max(1) * 1024 * 1024, enabled)
    }

    fn with_max_bytes(max_bytes: usize, enabled: bool) -> DedupCache {
        DedupCache {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                total_bytes: 0,
                tick: 0,
                suppressed_until: HashMap::new(),
                last_bytes: HashMap::new(),
            }),
            max_bytes,
            enabled,
            hydrations_total: AtomicU64::new(0),
            suppressed_gauge: AtomicI64::new(0),
            last_warn_ms: AtomicI64::new(0),
        }
    }

    /// Total hydration queries the cache has asked its caller to run (Full or
    /// Span). Read-only accessor for a future metrics hookup; suppression exists
    /// to keep this from exploding under cap pressure.
    pub fn hydrations_total(&self) -> u64 {
        self.hydrations_total.load(Ordering::Relaxed)
    }

    /// Approximate live count of SUPPRESSED partitions (gauge-ish). Read-only
    /// accessor for a future metrics hookup.
    pub fn suppressed_partitions(&self) -> i64 {
        self.suppressed_gauge.load(Ordering::Relaxed)
    }

    /// Pre-push check for one segment's hash bundle (frame order).
    /// - `LocalDuplicate(idxs)`: those input indices are known committed within
    ///   the window → broker short-circuit. Sound whenever the caller has run
    ///   `expire` with the queue's window this flush (positive membership never
    ///   depends on completeness).
    /// - `Verified(w)`: send `p_verified = w`; `w = -1` when the cache can't
    ///   vouch (unknown partition, SUPPRESSED partition, pending top-up/rebuild,
    ///   kill switch). A SUPPRESSED partition is simply absent from the map, so
    ///   it falls into the `None => Verified(-1)` arm — always sound.
    pub fn verified_for_push(&self, pid: &str, hashes: &[[u8; 16]], now_ms: i64) -> PushCheck {
        if !self.enabled {
            return PushCheck::Verified(-1);
        }
        let mut inner = self.inner.lock().unwrap();
        inner.tick += 1;
        let tick = inner.tick;
        let e = match inner.map.get_mut(pid) {
            Some(e) => e,
            None => return PushCheck::Verified(-1),
        };
        e.last_used = tick;
        e.last_used_ms = now_ms;
        let dups: Vec<usize> = hashes
            .iter()
            .enumerate()
            .filter(|(_, h)| e.set.contains_key(&u128::from_be_bytes(**h)))
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
    /// - contiguous (`base == verified_upto+1`): insert + advance.
    /// - gap (`base > verified_upto+1`): another broker interleaved. The push
    ///   itself was correct (SQL probed past our watermark), but the cache no
    ///   longer knows every hash below `base` — record the gap as `needs_topup`
    ///   so `verified_for_push` answers -1 until the span is hydrated.
    /// - overlap (`base <= verified_upto`): impossible under the monotone
    ///   allocator → mark for full rehydration (defensive).
    /// No-op for partitions never hydrated (the caller hydrates on first use;
    /// a push-seeded entry could not satisfy the window-coverage validity rule).
    pub fn on_push_committed(&self, pid: &str, base: i64, hashes: &[[u8; 16]], now_ms: i64) {
        if !self.enabled || hashes.is_empty() {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        inner.tick += 1;
        let tick = inner.tick;
        let (old_b, new_b) = {
            let e = match inner.map.get_mut(pid) {
                Some(e) => e,
                None => return,
            };
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
            for &h in &hs {
                e.insert_hash(h);
            }
            e.ring.push_back(RingSeg { base, created_ms: now_ms, hashes: hs });
            e.verified_upto = base + hashes.len() as i64 - 1;
            e.bytes = e.approx_bytes();
            (old_b, e.bytes)
        };
        inner.total_bytes = inner.total_bytes - old_b + new_b;
        Self::evict_lru(&mut inner, self.max_bytes, Some(pid), now_ms);
    }

    /// Build (Full) or top-up (Span) an entry from log_txns rows
    /// `(base_offset, end_offset, created_ms, hashes_blob)`. The caller runs the
    /// SQL; rows outside the window (`created_ms < window_start_ms`) are ignored
    /// defensively. `current_last_offset` is the partition's last_offset fetched
    /// in the same round trip (-1 when unknown) — consulted only when the window
    /// holds no rows, per §5.
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
        // fetch, so this is exactly one hydration query (Fix 3 counter).
        self.hydrations_total.fetch_add(1, Ordering::Relaxed);
        let mut inner = self.inner.lock().unwrap();
        inner.tick += 1;
        let tick = inner.tick;

        // Parse + validate rows. A blob whose length disagrees with the offset
        // span can't be trusted for coverage → poison the whole hydration.
        let mut segs: Vec<RingSeg> = Vec::with_capacity(rows.len());
        let mut malformed = false;
        for (base, end, created_ms, blob) in rows {
            if created_ms < window_start_ms {
                continue;
            }
            let hashes = parse_hash_blob(&blob);
            if hashes.is_empty() || hashes.len() as i64 != end - base + 1 {
                malformed = true;
                continue;
            }
            segs.push(RingSeg { base, created_ms, hashes });
        }
        segs.sort_by_key(|s| s.base);

        let span_topup = matches!(
            inner.map.get(pid),
            Some(e) if !e.needs_full && e.needs_topup.is_some()
        ) && !malformed;

        if span_topup {
            // Merge the gap rows into the existing entry (skip bases we already
            // hold — refcounts stay 1 per (segment, hash)), keep the ring in
            // base order (== commit-time order per the PUSHSER invariant).
            let (old_b, new_b) = {
                let e = inner.map.get_mut(pid).unwrap();
                e.last_used = tick;
                e.last_used_ms = now_ms;
                let old_b = e.bytes;
                for seg in segs {
                    if e.ring.iter().any(|r| r.base == seg.base) {
                        continue;
                    }
                    for &h in &seg.hashes {
                        e.insert_hash(h);
                    }
                    e.verified_upto = e.verified_upto.max(seg.end());
                    e.ring.push_back(seg);
                }
                let mut v: Vec<RingSeg> = e.ring.drain(..).collect();
                v.sort_by_key(|s| s.base);
                e.ring = v.into();
                e.needs_topup = None;
                e.bytes = e.approx_bytes();
                (old_b, e.bytes)
            };
            inner.total_bytes = inner.total_bytes - old_b + new_b;
        } else {
            // Full (re)build. hydrated_from = MIN(base)-1, or -1 when the window
            // is empty (anything older is outside the window by construction, so
            // "we know everything above -1 that could still matter" holds).
            let (hydrated_from, verified_upto) = match (segs.first(), segs.last()) {
                (Some(first), Some(last)) => (first.base - 1, last.end()),
                _ => (-1, current_last_offset),
            };
            let mut e = Entry {
                hydrated_from,
                verified_upto,
                set: HashMap::new(),
                ring: VecDeque::with_capacity(segs.len()),
                needs_topup: None,
                needs_full: malformed,
                bytes: 0,
                last_used: tick,
                last_used_ms: now_ms,
            };
            for seg in segs {
                for &h in &seg.hashes {
                    e.insert_hash(h);
                }
                e.ring.push_back(seg);
            }
            e.bytes = e.approx_bytes();
            let old_b = inner.map.get(pid).map(|p| p.bytes).unwrap_or(0);
            let new_b = e.bytes;
            inner.map.insert(pid.to_string(), e);
            inner.total_bytes = inner.total_bytes - old_b + new_b;
        }
        // The partition is resident again: drop any SUPPRESSED marker and its
        // stale eviction footprint so bookkeeping stays bounded (Fix 1).
        self.clear_suppressed(&mut inner, pid);
        inner.last_bytes.remove(pid);
        Self::evict_lru(&mut inner, self.max_bytes, Some(pid), now_ms);
    }

    /// Does this partition need SQL hydration before the cache can vouch?
    /// None = fresh, cache disabled, OR SUPPRESSED under cap pressure (Fix 1):
    /// in the suppressed case the caller must NOT run any hydration query and the
    /// partition simply pushes with p_verified = -1 (full-window SQL probe —
    /// always sound). `now_ms` drives the suppression cooldown.
    ///
    /// Resident partitions are never suppressed: a clean entry needs nothing, and
    /// a needs_topup entry gets its cheap incremental Span (topping up an
    /// already-resident entry adds only the gap rows, so it can't thrash). Only a
    /// FULL rebuild of an absent/poisoned partition — the multi-MB query that
    /// drove the thrash loop — is gated by the fit test + cooldown.
    pub fn needs_hydration(&self, pid: &str, now_ms: i64) -> Option<HydrationNeed> {
        if !self.enabled {
            return None;
        }
        let mut inner = self.inner.lock().unwrap();
        if let Some(e) = inner.map.get(pid) {
            if !e.needs_full {
                // Resident + vouching (None) or a resident interleave gap (Span);
                // both are cheap and always allowed, even under cap pressure.
                return e.needs_topup.map(|(from, to)| HydrationNeed::Span { from, to });
            }
        }
        // Absent, or resident-but-poisoned (needs_full): a FULL rebuild is
        // required — the thrash-prone path. Gate it.
        self.eval_full_hydration(&mut inner, pid, now_ms)
    }

    /// Decide whether a FULL (re)hydration of `pid` may run right now, or whether
    /// the partition should stay SUPPRESSED (Fix 1). Returns Some(Full) to admit,
    /// None to suppress. Marks/clears the cooldown + gauge as a side effect.
    fn eval_full_hydration(
        &self,
        inner: &mut Inner,
        pid: &str,
        now_ms: i64,
    ) -> Option<HydrationNeed> {
        // Still inside the cooldown → stay suppressed, skip the multi-MB query.
        if let Some(&until) = inner.suppressed_until.get(pid) {
            if now_ms < until {
                return None;
            }
            // Cooldown elapsed: fall through and re-test the fit.
        }
        // Estimate the re-hydration footprint. A poisoned resident entry uses its
        // own current bytes; an evicted one its footprint at eviction; a
        // never-seen partition estimates 0 (empty window — cheap to admit, which
        // is why brand-new partitions are never suppressed on first contact).
        let resident_bytes = inner.map.get(pid).map(|e| e.bytes).unwrap_or(0);
        let est = if resident_bytes > 0 {
            resident_bytes
        } else {
            inner.last_bytes.get(pid).copied().unwrap_or(0)
        };
        // Fit test: everything EXCEPT this partition's own resident bytes (a
        // rebuild replaces them) plus the estimate must stay under the hysteresis
        // fraction of the cap. Integer form of `others + est <= cap * 9/10`.
        let others = inner.total_bytes.saturating_sub(resident_bytes);
        let budget = self.max_bytes / HYDRATION_FIT_DEN * HYDRATION_FIT_NUM;
        if others + est <= budget {
            // Fits: admit. Clear any prior suppression so the caller hydrates.
            self.clear_suppressed(inner, pid);
            Some(HydrationNeed::Full)
        } else {
            // Doesn't fit: SUPPRESS with a fresh cooldown and decline the query.
            // Sound: the partition pushes with p_verified = -1 (SQL full probe).
            self.mark_suppressed(inner, pid, now_ms);
            self.warn_cap_pressure(inner, now_ms);
            None
        }
    }

    /// Record a SUPPRESSED cooldown for `pid` and keep the gauge in sync. Prunes
    /// already-expired deadlines first if the map has grown past its soft cap.
    fn mark_suppressed(&self, inner: &mut Inner, pid: &str, now_ms: i64) {
        if inner.suppressed_until.len() > AUX_MAP_SOFT_CAP {
            let before = inner.suppressed_until.len();
            inner.suppressed_until.retain(|_, &mut d| d > now_ms);
            let pruned = (before - inner.suppressed_until.len()) as i64;
            self.suppressed_gauge.fetch_sub(pruned, Ordering::Relaxed);
        }
        if inner
            .suppressed_until
            .insert(pid.to_string(), now_ms + SUPPRESS_COOLDOWN_MS)
            .is_none()
        {
            self.suppressed_gauge.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Clear `pid`'s SUPPRESSED marker (if any) and keep the gauge in sync.
    fn clear_suppressed(&self, inner: &mut Inner, pid: &str) {
        if inner.suppressed_until.remove(pid).is_some() {
            self.suppressed_gauge.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// One rate-limited (once/min, process-wide) warning line while suppression
    /// is active, carrying the resident/cap footprint and the sizing formula so
    /// an operator can size QUEEN_DEDUP_CACHE_MB (Fix 3).
    fn warn_cap_pressure(&self, inner: &Inner, now_ms: i64) {
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
        let resident_mb = inner.total_bytes / (1024 * 1024);
        let cap_mb = self.max_bytes / (1024 * 1024);
        let suppressed = self.suppressed_gauge.load(Ordering::Relaxed).max(0);
        eprintln!(
            "[dedup] cap pressure: resident={resident_mb}MB cap={cap_mb}MB \
             suppressed={suppressed} partitions using SQL probes (sound, slower); \
             raise QUEEN_DEDUP_CACHE_MB — needed_mb ≈ 16 × msg_rate × window_seconds / 1e6"
        );
    }

    /// Drop segments older than the dedup window (raising `hydrated_from` to the
    /// end of everything dropped — those offsets are no longer known, but they
    /// no longer matter: outside the window they can't be duplicates). Callers
    /// run this once per flush per partition, BEFORE `verified_for_push`, so
    /// LocalDuplicate answers stay window-accurate. Also enforces the global
    /// byte budget (whole-partition LRU eviction).
    pub fn expire(&self, pid: &str, window_ms: i64, now_ms: i64) {
        if !self.enabled {
            return;
        }
        let cutoff = now_ms - window_ms;
        let mut inner = self.inner.lock().unwrap();
        if let Some(e) = inner.map.get_mut(pid) {
            // We are actively flushing this partition this instant: mark it used
            // so the eviction guard below never evicts it out from under us.
            e.last_used_ms = now_ms;
            let old_b = e.bytes;
            while let Some(front) = e.ring.front() {
                if front.created_ms >= cutoff {
                    break;
                }
                let seg = e.ring.pop_front().unwrap();
                for &h in &seg.hashes {
                    e.remove_hash(h);
                }
                e.hydrated_from = e.hydrated_from.max(seg.end());
            }
            e.bytes = e.approx_bytes();
            let new_b = e.bytes;
            inner.total_bytes = inner.total_bytes - old_b + new_b;
        }
        Self::evict_lru(&mut inner, self.max_bytes, None, now_ms);
    }

    /// Evict least-recently-used partitions until under budget, with hysteresis
    /// (Fix 2). Two partitions are NEVER evicted to admit another:
    ///   - `protect` (the partition being served right now), and
    ///   - any partition used within RECENT_USE_GUARD_MS — evicting a hot
    ///     partition to admit another just starts the ping-pong the fix kills.
    /// If nothing is evictable, a stable hot resident set slightly over cap is
    /// TOLERATED (returning to under-cap is bounded by expire()/window and by
    /// suppression declining new admissions) — far cheaper than thrash. An
    /// evicted partition's footprint is remembered in `last_bytes` so its next
    /// re-hydration can be fit-tested before it is admitted (Fix 1). Evicted
    /// partitions re-hydrate on next push only if they then fit.
    fn evict_lru(inner: &mut Inner, max_bytes: usize, protect: Option<&str>, now_ms: i64) {
        while inner.total_bytes > max_bytes {
            let victim = inner
                .map
                .iter()
                .filter(|(k, e)| {
                    Some(k.as_str()) != protect
                        && now_ms.saturating_sub(e.last_used_ms) >= RECENT_USE_GUARD_MS
                })
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone());
            match victim {
                Some(k) => {
                    if let Some(e) = inner.map.remove(&k) {
                        inner.total_bytes -= e.bytes;
                        // Bound the hint map under churn (cheap self-healing reset;
                        // the only cost is a re-measured estimate next eviction).
                        if inner.last_bytes.len() > AUX_MAP_SOFT_CAP {
                            inner.last_bytes.clear();
                        }
                        inner.last_bytes.insert(k, e.bytes);
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
        c.inner.lock().unwrap().total_bytes
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
    fn interleave_gap_then_topup() {
        let c = DedupCache::with_max_bytes(1 << 20, true);
        c.hydrate("p1", vec![], 0, -1, T0);
        c.on_push_committed("p1", 0, &[h(1), h(2)], 1_000);
        // Another broker wrote offsets 2..=4: our push landed at 5.
        c.on_push_committed("p1", 5, &[h(6)], 1_001);
        assert_eq!(c.verified_for_push("p1", &[h(9)], T0), PushCheck::Verified(-1));
        assert_eq!(
            c.needs_hydration("p1", T0),
            Some(HydrationNeed::Span { from: 2, to: 4 })
        );
        // Top-up with the missing span → complete again, watermark = 5.
        c.hydrate("p1", vec![(2, 4, 1_000, blob(&[h(3), h(4), h(5)]))], 0, 5, T0);
        assert_eq!(c.needs_hydration("p1", T0), None);
        assert_eq!(c.verified_for_push("p1", &[h(9)], T0), PushCheck::Verified(5));
        // The hydrated span's hashes are now known duplicates.
        assert_eq!(
            c.verified_for_push("p1", &[h(4)], T0),
            PushCheck::LocalDuplicate(vec![0])
        );
    }

    #[test]
    fn expiry_raises_hydrated_from_and_forgets_hashes() {
        let c = DedupCache::with_max_bytes(1 << 20, true);
        c.hydrate(
            "p1",
            vec![
                (0, 1, 1_000, blob(&[h(1), h(2)])),
                (2, 2, 5_000, blob(&[h(3)])),
            ],
            0,
            2,
            1_000,
        );
        assert_eq!(
            c.verified_for_push("p1", &[h(1)], 6_000),
            PushCheck::LocalDuplicate(vec![0])
        );
        // Window = 3s at t=6s → the t=1s segment falls out; the t=5s one stays.
        c.expire("p1", 3_000, 6_000);
        {
            let inner = c.inner.lock().unwrap();
            let e = inner.map.get("p1").unwrap();
            assert_eq!(e.hydrated_from, 1); // raised to end of the dropped segment
            assert_eq!(e.verified_upto, 2);
        }
        // Expired hashes are no longer duplicates; the entry still vouches.
        assert_eq!(c.verified_for_push("p1", &[h(1)], 6_000), PushCheck::Verified(2));
        assert_eq!(
            c.verified_for_push("p1", &[h(3)], 6_000),
            PushCheck::LocalDuplicate(vec![0])
        );
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
        {
            let inner = c.inner.lock().unwrap();
            assert!(!inner.map.contains_key("p2"), "p2 (LRU) evicted whole");
            assert!(inner.map.contains_key("p1") && inner.map.contains_key("p3"));
            // Its footprint is remembered so a re-hydration can be fit-tested.
            assert_eq!(inner.last_bytes.get("p2"), Some(&one));
        }
        // Evicted partition is no longer resident → sound -1 until it re-hydrates.
        assert_eq!(c.verified_for_push("p2", &[h(9)], t), PushCheck::Verified(-1));
        assert!(total_bytes(&c) <= one * 2 + one / 2);
    }

    #[test]
    fn hot_partition_not_evicted_by_newcomer() {
        // Fix 2: the eviction guard must never evict a partition used within
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
        let inner = c.inner.lock().unwrap();
        assert!(inner.map.contains_key("p1"), "hot p1 not evicted");
        assert!(inner.map.contains_key("p2"), "hot p2 not evicted");
        assert!(inner.map.contains_key("p3"), "newcomer admitted (overage tolerated)");
    }

    #[test]
    fn suppression_cooldown_returns_minus_one_without_hydration() {
        // Fix 1: an evicted partition that no longer fits is SUPPRESSED — no
        // full re-hydration query — and pushes with p_verified = -1 for a
        // cooldown, then recovers once pressure eases.
        let probe = DedupCache::with_max_bytes(1 << 20, true);
        probe.hydrate("p", vec![(0, 0, 1_000, blob(&[h(1)]))], 0, 0, T0);
        let one = total_bytes(&probe);

        // Cap ~2 entries; one hot resident, and "cold" seeded as previously
        // evicted at footprint ~one (deterministic setup of last_bytes).
        let c = DedupCache::with_max_bytes(one * 2, true); // budget = 0.9 * 2one = 1.8one
        c.hydrate("hot", vec![(0, 0, 1_000, blob(&[h(1)]))], 0, 0, 5_000);
        c.inner.lock().unwrap().last_bytes.insert("cold".to_string(), one);

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
            let mut inner = c.inner.lock().unwrap();
            inner.map.clear();
            inner.total_bytes = 0;
        }
        assert_eq!(
            c.needs_hydration("cold", 5_000 + SUPPRESS_COOLDOWN_MS),
            Some(HydrationNeed::Full)
        );
        assert_eq!(c.suppressed_partitions(), 0);
    }

    #[test]
    fn resident_span_topup_allowed_under_pressure() {
        // A resident partition's incremental Span top-up is ALWAYS allowed, even
        // at/over cap — it can't thrash (no full window re-read).
        let probe = DedupCache::with_max_bytes(1 << 20, true);
        probe.hydrate("p", vec![(0, 0, 1_000, blob(&[h(1)]))], 0, 0, T0);
        let one = total_bytes(&probe);

        let c = DedupCache::with_max_bytes(one, true); // deliberately tight
        c.hydrate("p1", vec![], 0, -1, 1_000);
        c.on_push_committed("p1", 0, &[h(1), h(2)], 1_000);
        // Interleave gap → needs a Span top-up. Still offered despite cap pressure.
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
    fn refcounted_set_survives_duplicate_hash_across_segments() {
        // Two in-window segments sharing a hash (possible after a window
        // reconfig): expiring the first must NOT forget the second occurrence.
        let c = DedupCache::with_max_bytes(1 << 20, true);
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
        c.expire("p1", 3_000, 6_000); // drops the first segment only
        assert_eq!(
            c.verified_for_push("p1", &[h(1)], 6_000),
            PushCheck::LocalDuplicate(vec![0])
        );
    }
}
