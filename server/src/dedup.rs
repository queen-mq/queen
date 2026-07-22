// Broker-side dedup cache (doc 18 §5) — exact, HA-safe, storage-free.
//
// CORRECTNESS: this cache is an OPTIMIZATION ONLY. Its single load-bearing
// output is the `p_verified` watermark sent with a push: it tells SQL "the
// broker vouches that none of this bundle's hashes occur in (-inf, p_verified]",
// which lets log_push_one_v1 shrink its dedup probe to (p_verified, last_offset].
// Whenever the cache cannot vouch — unknown partition, hydration gap, kill
// switch — it answers -1, which forces the full-window SQL probe and is ALWAYS
// sound. A disabled cache is therefore exactly equivalent to always-(-1).
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
    /// LRU stamp (global monotone tick).
    last_used: u64,
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
}

/// Global broker-side dedup cache. Cheap to share (`Arc<DedupCache>`); one
/// mutex around the whole map — contention is per-flush, not per-message.
pub struct DedupCache {
    inner: Mutex<Inner>,
    max_bytes: usize,
    enabled: bool,
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
            }),
            max_bytes,
            enabled,
        }
    }

    /// Pre-push check for one segment's hash bundle (frame order).
    /// - `LocalDuplicate(idxs)`: those input indices are known committed within
    ///   the window → broker short-circuit. Sound whenever the caller has run
    ///   `expire` with the queue's window this flush (positive membership never
    ///   depends on completeness).
    /// - `Verified(w)`: send `p_verified = w`; `w = -1` when the cache can't
    ///   vouch (unknown partition, pending top-up/rebuild, kill switch).
    pub fn verified_for_push(&self, pid: &str, hashes: &[[u8; 16]]) -> PushCheck {
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
        Self::evict_lru(&mut inner, self.max_bytes, Some(pid));
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
    ) {
        if !self.enabled {
            return;
        }
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
        Self::evict_lru(&mut inner, self.max_bytes, Some(pid));
    }

    /// Does this partition need SQL hydration before the cache can vouch?
    /// None = fresh (or cache disabled — never hydrate then).
    pub fn needs_hydration(&self, pid: &str) -> Option<HydrationNeed> {
        if !self.enabled {
            return None;
        }
        let inner = self.inner.lock().unwrap();
        match inner.map.get(pid) {
            None => Some(HydrationNeed::Full),
            Some(e) if e.needs_full => Some(HydrationNeed::Full),
            Some(e) => e
                .needs_topup
                .map(|(from, to)| HydrationNeed::Span { from, to }),
        }
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
        Self::evict_lru(&mut inner, self.max_bytes, None);
    }

    /// Evict least-recently-used partitions until under budget. The partition
    /// being served right now (`protect`) is never evicted in the same call —
    /// a single over-budget partition is tolerated rather than thrashed;
    /// evicted partitions simply re-hydrate on next push.
    fn evict_lru(inner: &mut Inner, max_bytes: usize, protect: Option<&str>) {
        while inner.total_bytes > max_bytes {
            let victim = inner
                .map
                .iter()
                .filter(|(k, _)| Some(k.as_str()) != protect)
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone());
            match victim {
                Some(k) => {
                    if let Some(e) = inner.map.remove(&k) {
                        inner.total_bytes -= e.bytes;
                    }
                }
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

    #[test]
    fn contiguous_advance() {
        let c = DedupCache::with_max_bytes(1 << 20, true);
        // Unknown partition: can't vouch, wants full hydration.
        assert_eq!(c.verified_for_push("p1", &[h(1)]), PushCheck::Verified(-1));
        assert_eq!(c.needs_hydration("p1"), Some(HydrationNeed::Full));
        // Empty-window hydration → complete entry, watermark -1 (nothing to vouch past).
        c.hydrate("p1", vec![], 0, -1);
        assert_eq!(c.needs_hydration("p1"), None);
        assert_eq!(c.verified_for_push("p1", &[h(1)]), PushCheck::Verified(-1));
        // Contiguous commits advance the watermark.
        c.on_push_committed("p1", 0, &[h(1), h(2)], 1_000);
        assert_eq!(c.verified_for_push("p1", &[h(3)]), PushCheck::Verified(1));
        c.on_push_committed("p1", 2, &[h(3)], 1_001);
        assert_eq!(c.verified_for_push("p1", &[h(4)]), PushCheck::Verified(2));
    }

    #[test]
    fn local_duplicate_detection() {
        let c = DedupCache::with_max_bytes(1 << 20, true);
        c.hydrate("p1", vec![], 0, -1);
        c.on_push_committed("p1", 0, &[h(1), h(2)], 1_000);
        assert_eq!(
            c.verified_for_push("p1", &[h(9), h(1), h(2)]),
            PushCheck::LocalDuplicate(vec![1, 2])
        );
        // Positive hits stay sound even while incomplete (gap pending).
        c.on_push_committed("p1", 10, &[h(5)], 1_002);
        assert_eq!(
            c.verified_for_push("p1", &[h(2)]),
            PushCheck::LocalDuplicate(vec![0])
        );
    }

    #[test]
    fn interleave_gap_then_topup() {
        let c = DedupCache::with_max_bytes(1 << 20, true);
        c.hydrate("p1", vec![], 0, -1);
        c.on_push_committed("p1", 0, &[h(1), h(2)], 1_000);
        // Another broker wrote offsets 2..=4: our push landed at 5.
        c.on_push_committed("p1", 5, &[h(6)], 1_001);
        assert_eq!(c.verified_for_push("p1", &[h(9)]), PushCheck::Verified(-1));
        assert_eq!(
            c.needs_hydration("p1"),
            Some(HydrationNeed::Span { from: 2, to: 4 })
        );
        // Top-up with the missing span → complete again, watermark = 5.
        c.hydrate("p1", vec![(2, 4, 1_000, blob(&[h(3), h(4), h(5)]))], 0, 5);
        assert_eq!(c.needs_hydration("p1"), None);
        assert_eq!(c.verified_for_push("p1", &[h(9)]), PushCheck::Verified(5));
        // The hydrated span's hashes are now known duplicates.
        assert_eq!(
            c.verified_for_push("p1", &[h(4)]),
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
        );
        assert_eq!(
            c.verified_for_push("p1", &[h(1)]),
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
        assert_eq!(c.verified_for_push("p1", &[h(1)]), PushCheck::Verified(2));
        assert_eq!(
            c.verified_for_push("p1", &[h(3)]),
            PushCheck::LocalDuplicate(vec![0])
        );
    }

    #[test]
    fn lru_eviction_evicts_whole_partitions() {
        // Measure one entry's footprint, then budget for two.
        let probe = DedupCache::with_max_bytes(1 << 20, true);
        probe.hydrate("p", vec![(0, 0, 1_000, blob(&[h(1)]))], 0, 0);
        let one = total_bytes(&probe);

        let c = DedupCache::with_max_bytes(one * 2 + one / 2, true);
        c.hydrate("p1", vec![(0, 0, 1_000, blob(&[h(1)]))], 0, 0);
        c.hydrate("p2", vec![(0, 0, 1_000, blob(&[h(2)]))], 0, 0);
        // Touch p1 so p2 is the LRU.
        assert_eq!(c.verified_for_push("p1", &[h(9)]), PushCheck::Verified(0));
        // Third partition overflows the budget → p2 (LRU) is evicted whole.
        c.hydrate("p3", vec![(0, 0, 1_000, blob(&[h(3)]))], 0, 0);
        assert_eq!(c.needs_hydration("p2"), Some(HydrationNeed::Full));
        assert_eq!(c.needs_hydration("p1"), None);
        assert_eq!(c.needs_hydration("p3"), None);
        assert!(total_bytes(&c) <= one * 2 + one / 2);
    }

    #[test]
    fn overlap_degrades_to_full_rehydration() {
        let c = DedupCache::with_max_bytes(1 << 20, true);
        c.hydrate("p1", vec![], 0, -1);
        c.on_push_committed("p1", 0, &[h(1), h(2)], 1_000);
        // Impossible under the monotone allocator → defensive full rebuild.
        c.on_push_committed("p1", 1, &[h(3)], 1_001);
        assert_eq!(c.verified_for_push("p1", &[h(9)]), PushCheck::Verified(-1));
        assert_eq!(c.needs_hydration("p1"), Some(HydrationNeed::Full));
        // A full hydration heals it.
        c.hydrate("p1", vec![(0, 1, 1_000, blob(&[h(1), h(2)]))], 0, 1);
        assert_eq!(c.verified_for_push("p1", &[h(9)]), PushCheck::Verified(1));
    }

    #[test]
    fn disabled_cache_always_declines() {
        let c = DedupCache::with_max_bytes(1 << 20, false);
        c.hydrate("p1", vec![(0, 0, 1_000, blob(&[h(1)]))], 0, 0);
        c.on_push_committed("p1", 1, &[h(2)], 1_001);
        assert_eq!(c.needs_hydration("p1"), None);
        assert_eq!(c.verified_for_push("p1", &[h(1)]), PushCheck::Verified(-1));
        assert_eq!(total_bytes(&c), 0);
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
        );
        c.expire("p1", 3_000, 6_000); // drops the first segment only
        assert_eq!(
            c.verified_for_push("p1", &[h(1)]),
            PushCheck::LocalDuplicate(vec![0])
        );
    }
}
