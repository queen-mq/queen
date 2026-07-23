// In-memory ACK REGISTRY fast path (doc 18 companion; explicit-ack ceiling fix).
//
// PROBLEM. Every explicit ack resolves txn→offset in PG via
// queen.log_ack_by_hash_v1: it unnests the leased range's log_txns hash blobs
// and hash-joins them against the acked hashes (several passes over the window).
// Measured at ~240ms of PG CPU per 500-msg batch — the wall that caps explicit
// consume at ~100k msg/s/side while PG melts at ~42 busy backends. But the
// broker ALREADY holds everything the common case needs at pop-serve time: it
// unpacks the frames (writing each transactionId into the response) and knows
// the leased batch's end offset. This registry remembers, per active lease,
// (worker, batch_end, the batch's txn-hash set); when the client later acks the
// WHOLE batch completed, the ack becomes a single positional cursor advance
// (queen.log_ack_at_v1 → committed = batch_end) with NO hash resolution at all.
//
// WHAT IT IS. A process-local map keyed (partition_id, consumer_group) → Entry.
// It is an OPTIMIZATION ONLY: a miss (or a disabled/evicted/expired entry) falls
// straight through to the unchanged queen.log_ack_by_hash_v1 path, which is the
// authority. The registry never decides anything the SQL path would decide
// differently — see the correctness argument below.
//
// CORRECTNESS GUARDRAILS (why the fast path is always sound). The fast path may
// fire for a (partition, worker) ack group ONLY when all three hold:
//   (a) EVERY item in the group has status `completed` (checked by the caller);
//   (b) the acked txn-hash set EXACTLY equals the lease's delivered-batch
//       hash set (checked by `take_if_full_batch` — see the set-equality note);
//   (c) the request's worker/leaseId matches the lease's worker.
// When all three hold, advancing the cursor to `batch_end` is EXACTLY what
// queen.log_ack_by_hash_v1 would compute: with every delivered message acked
// completed and none failed/retried/dlq'd, the implicit-ack rule advances
// `committed` to the max acked position (= batch_end), and there is nothing
// below the cursor, no explicit signal to clamp at, and no retry/DLQ handoff.
// Every other case — a partial ack, ANY non-completed status, a worker
// mismatch, an unknown/evicted/expired entry — takes NO fast path and hits the
// verbatim SQL path, so below-cursor honesty, explicit-signal-never-skipped,
// the retry budget, and DLQ handoff are all preserved BY CONSTRUCTION (those
// code paths are literally untouched). The positional SQL function itself
// (log_ack_at_v1) still re-validates the lease under the consumer row lock —
// so even a stale registry HIT for an expired/reassigned lease is rejected by
// PG and falls back to the SQL path; the registry can never *grant* an ack PG
// would refuse.
//
// SET-EQUALITY, NOT MULTISET. The Entry stores the batch's DISTINCT txn-hash
// set, not the frame-ordered multiset the spec sketches. The exact-cover test
// is order- and multiplicity-insensitive by design ("duplicates in the request
// tolerated"), and nothing else reads frame order, so the distinct set is both
// sufficient and cheaper. The rare degenerate case — two delivered messages
// sharing one transactionId — collapses to one set member; acking that txn
// completed acks BOTH offsets in the SQL model too, so advancing to batch_end
// stays correct. Storing the distinct set also lets the hot compare be a single
// `HashSet == HashSet` (Rust set equality: equal length + subset), with the
// request side deduped OFF the lock.
//
// CONCURRENCY. A single global `Mutex<State>` guards the map + byte budget.
// This is deliberate and measured-adequate, not lazy: the per-op work under the
// lock is tiny — insert builds/rehomes one small Entry; `take_if_full_batch`
// does one lookup + one `HashSet==` (O(batch) membership on a PREBUILT set,
// the request set is built off-lock) + a remove. At a 1M-msg/s ceiling the ack
// rate is ~msg_rate/pop_batch ≈ a few thousand ops/s, each a few microseconds
// under the lock → low single-digit % lock duty. If a future workload with
// tiny pop batches pushed this into contention, sharding is a drop-in (the map
// is keyed and self-contained: shard on a hash of partition_id and give each
// shard its own byte budget) — but the numbers here don't warrant the extra
// code, so we keep one lock and one honest global LRU budget.
//
// LIFECYCLE. An entry is BORN on a leasing pop that delivered ≥1 frame (all
// leasing pop variants: wildcard bin/wire, specific, discover, and the mesh
// hint-targeted specific pop). It DIES on: a fast-path HIT (consumed by
// `take_if_full_batch`), an `evict` from any SQL-path ack outcome for that
// (partition, group) (lease release, nack, DLQ handoff, partial ack), TTL
// expiry (lease_seconds + margin — a coarse memory bound; PG lease validation,
// not this TTL, is what actually protects a late ack), or LRU eviction under
// the byte cap. Every death mode degrades to the SQL path — always sound.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;

// TTL margin past the lease's own expiry before an entry is swept: an entry is
// useless once the lease it describes has expired (a late ack then fails PG
// lease validation anyway), but we keep it a little longer so a client acking
// right at the lease boundary still gets the fast path. Pure memory bound.
const TTL_MARGIN_MS: i64 = 10_000;

// Fallback lease seconds for TTL when the pop could not tell us the real value
// (the discover pop resolves the lease per-partition inside SQL). Only affects
// how long a memory slot lingers, never correctness.
const TTL_FALLBACK_LEASE_SECS: i64 = 60;

// Opportunistic TTL sweep cadence: `insert_lease` runs a full sweep at most this
// often (process-wide), so expired slots are reclaimed without a background task.
const SWEEP_INTERVAL_MS: i64 = 5_000;

// Rate-limit for the periodic hit-rate line (mirrors dedup.rs's [dedup] line).
const REPORT_INTERVAL_MS: i64 = 10_000;

// Coarse, pessimistic byte accounting for the LRU budget. 16B key + bucket
// overhead per distinct hash; fixed per-entry overhead covers the worker string,
// the two HashMap slots, and Entry scalars.
const BYTES_PER_HASH: usize = 48;
const BYTES_PER_ENTRY: usize = 160;

struct Entry {
    /// The leaseId (== the pop's worker uuid) the client must echo to ack.
    worker: String,
    /// Inclusive end offset of the leased batch — identical to the value
    /// queen.log_pop_v1 wrote to queen.log_consumers.batch_end. A HIT advances
    /// `committed` here in one positional ack.
    batch_end: i64,
    /// DISTINCT txn hashes (xxh3_128 as u128) of the delivered batch. Set
    /// equality against the acked set is the exact-cover guardrail.
    distinct: std::collections::HashSet<u128>,
    /// Wall-clock (epoch ms) after which this slot may be swept (lease expiry +
    /// margin). Memory bound only.
    expires_ms: i64,
    /// Approximate footprint, kept in sync with `State::total_bytes`.
    bytes: usize,
    /// LRU stamp (monotone tick) — orders eviction victims.
    last_used: u64,
}

struct State {
    /// key = "partition_id\x1fconsumer_group" → Entry.
    map: HashMap<String, Entry>,
    total_bytes: usize,
    tick: u64,
    last_sweep_ms: i64,
}

/// Process-local ack-registry. Cheap to share (`Arc<AckRegistry>`).
pub struct AckRegistry {
    state: Mutex<State>,
    max_bytes: usize,
    enabled: bool,
    hits: AtomicU64,
    misses: AtomicU64,
    last_report_ms: AtomicI64,
}

#[inline]
fn key(pid: &str, group: &str) -> String {
    let mut s = String::with_capacity(pid.len() + 1 + group.len());
    s.push_str(pid);
    s.push('\u{1f}');
    s.push_str(group);
    s
}

fn entry_bytes(worker: &str, distinct_len: usize) -> usize {
    BYTES_PER_ENTRY + worker.len() + distinct_len * BYTES_PER_HASH
}

impl AckRegistry {
    /// `max_mb` = QUEEN_ACK_REGISTRY_MB (default 64). `enabled=false` makes every
    /// op a no-op (all acks take the SQL path — exactly today's behaviour).
    pub fn new(max_mb: usize, enabled: bool) -> AckRegistry {
        AckRegistry {
            state: Mutex::new(State {
                map: HashMap::new(),
                total_bytes: 0,
                tick: 0,
                last_sweep_ms: 0,
            }),
            max_bytes: max_mb.max(1) * 1024 * 1024,
            enabled,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            last_report_ms: AtomicI64::new(0),
        }
    }

    /// Record a fresh lease that delivered a batch. `hashes` are the delivered
    /// frames' xxh3_128 txn fingerprints (any order; deduped here). `batch_end`
    /// MUST be the same offset queen.log_pop_v1 wrote to log_consumers.batch_end
    /// (last delivered offset). Replaces any prior entry for (pid, group) — a
    /// re-lease by a new worker supersedes the old one. No-op when disabled or
    /// when the batch is empty (nothing to fast-path).
    pub fn insert_lease(
        &self,
        pid: &str,
        group: &str,
        worker: &str,
        batch_end: i64,
        hashes: &[u128],
        lease_seconds: i32,
        now_ms: i64,
    ) {
        if !self.enabled || worker.is_empty() || hashes.is_empty() {
            return;
        }
        let distinct: std::collections::HashSet<u128> = hashes.iter().copied().collect();
        let ls = if lease_seconds > 0 {
            lease_seconds as i64
        } else {
            TTL_FALLBACK_LEASE_SECS
        };
        let bytes = entry_bytes(worker, distinct.len());
        let k = key(pid, group);
        let mut st = self.state.lock().unwrap();
        st.tick += 1;
        let tick = st.tick;
        let e = Entry {
            worker: worker.to_string(),
            batch_end,
            distinct,
            expires_ms: now_ms + ls * 1000 + TTL_MARGIN_MS,
            bytes,
            last_used: tick,
        };
        if let Some(old) = st.map.insert(k, e) {
            st.total_bytes -= old.bytes;
        }
        st.total_bytes += bytes;
        // Opportunistic TTL reclaim (no background task): bounded to once per
        // SWEEP_INTERVAL_MS process-wide.
        if now_ms - st.last_sweep_ms >= SWEEP_INTERVAL_MS {
            st.last_sweep_ms = now_ms;
            Self::sweep_locked(&mut st, now_ms);
        }
        Self::evict_lru_locked(&mut st, self.max_bytes);
    }

    /// Fast-path probe for a fully-completed ack group. Returns Some(batch_end)
    /// — and REMOVES the entry — iff (worker matches) AND (the acked hash set
    /// EXACTLY equals the lease's delivered-batch hash set). Otherwise None and
    /// the entry is left untouched (a worker-mismatch belongs to the CURRENT
    /// leaseholder and must not be disturbed; a partial ack is dropped by the
    /// caller's `evict`). Updates hit/miss counters. `acked` may contain
    /// duplicates (collapsed here). No-op (None) when disabled.
    pub fn take_if_full_batch(
        &self,
        pid: &str,
        group: &str,
        worker: &str,
        acked: &[u128],
    ) -> Option<i64> {
        if !self.enabled {
            return None;
        }
        // Dedup the request set OFF the lock; the hot compare under the lock is
        // then a single HashSet== against the entry's prebuilt distinct set.
        let acked_set: std::collections::HashSet<u128> = acked.iter().copied().collect();
        let k = key(pid, group);
        let mut st = self.state.lock().unwrap();
        let is_hit = matches!(
            st.map.get(&k),
            Some(e) if e.worker == worker && e.distinct == acked_set
        );
        if is_hit {
            let e = st.map.remove(&k).unwrap();
            st.total_bytes -= e.bytes;
            drop(st);
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(e.batch_end)
        } else {
            drop(st);
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Drop the entry for (pid, group) IFF it is owned by `worker`. Called by the
    /// ack path after any SQL-path outcome for a group whose leaseId is `worker`
    /// (lease release, nack, DLQ handoff, partial ack): the cached batch_end/hash
    /// set no longer describes a live full-batch shortcut for THAT lease, so it is
    /// removed. The worker guard is deliberate — a late ack from an OLD worker
    /// (after the batch was re-leased to someone else) must NOT drop the CURRENT
    /// holder's fresh entry. Removing an entry can only cost a future SQL ack,
    /// never correctness. No-op when disabled, or `worker` empty (a lease-less ack
    /// owns no entry).
    pub fn evict(&self, pid: &str, group: &str, worker: &str) {
        if !self.enabled || worker.is_empty() {
            return;
        }
        let k = key(pid, group);
        let mut st = self.state.lock().unwrap();
        if matches!(st.map.get(&k), Some(e) if e.worker == worker) {
            if let Some(e) = st.map.remove(&k) {
                st.total_bytes -= e.bytes;
            }
        }
    }

    /// Remove every entry whose lease (plus margin) has expired. Exposed for a
    /// caller-driven sweep; `insert_lease` also runs it opportunistically.
    #[allow(dead_code)]
    pub fn ttl_sweep(&self, now_ms: i64) {
        if !self.enabled {
            return;
        }
        let mut st = self.state.lock().unwrap();
        Self::sweep_locked(&mut st, now_ms);
    }

    fn sweep_locked(st: &mut State, now_ms: i64) {
        let mut freed = 0usize;
        st.map.retain(|_, e| {
            if e.expires_ms <= now_ms {
                freed += e.bytes;
                false
            } else {
                true
            }
        });
        st.total_bytes = st.total_bytes.saturating_sub(freed);
    }

    /// Evict least-recently-used whole entries until under the byte budget.
    /// Entries are short-lived (consumed on ack), so the map stays ~= in-flight
    /// leases and this rarely fires; when it does, a scan for the min-tick victim
    /// is cheap relative to the ack it saves. An evicted lease simply takes the
    /// SQL path on its ack — sound.
    fn evict_lru_locked(st: &mut State, max_bytes: usize) {
        while st.total_bytes > max_bytes {
            let victim = st
                .map
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone());
            match victim {
                Some(k) => {
                    if let Some(e) = st.map.remove(&k) {
                        st.total_bytes = st.total_bytes.saturating_sub(e.bytes);
                    }
                }
                None => break,
            }
        }
    }

    /// Emit a rate-limited (once/REPORT_INTERVAL_MS, process-wide) hit-rate line
    /// so a run shows the fast-path effectiveness — mirrors dedup.rs's [dedup]
    /// line. Cheap: an atomic guard, and the lock is taken only when it actually
    /// prints. Call it from the ack path.
    pub fn maybe_report(&self, now_ms: i64) {
        if !self.enabled {
            return;
        }
        let prev = self.last_report_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(prev) < REPORT_INTERVAL_MS {
            return;
        }
        if self
            .last_report_ms
            .compare_exchange(prev, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return; // another thread just printed
        }
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let rate = if total > 0 {
            (hits as f64) * 100.0 / (total as f64)
        } else {
            0.0
        };
        let (entries, mb) = {
            let st = self.state.lock().unwrap();
            (st.map.len(), st.total_bytes as f64 / (1024.0 * 1024.0))
        };
        eprintln!(
            "[ack-reg] hits={hits} misses={misses} hit_rate={rate:.1}% \
             entries={entries} resident={mb:.1}MB"
        );
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.state.lock().unwrap().map.len()
    }

    #[cfg(test)]
    fn bytes(&self) -> usize {
        self.state.lock().unwrap().total_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: i64 = 1_000_000;

    fn reg() -> AckRegistry {
        AckRegistry::new(64, true)
    }

    #[test]
    fn hit_advances_and_consumes() {
        let r = reg();
        r.insert_lease("p1", "g", "w1", 42, &[1, 2, 3], 60, T0);
        // Exact cover, worker matches → HIT returns batch_end and removes it.
        assert_eq!(r.take_if_full_batch("p1", "g", "w1", &[3, 2, 1]), Some(42));
        assert_eq!(r.len(), 0);
        assert_eq!(r.bytes(), 0);
        // Second ack of the same batch: entry gone → MISS (SQL path handles the
        // below-cursor duplicate).
        assert_eq!(r.take_if_full_batch("p1", "g", "w1", &[1, 2, 3]), None);
        assert_eq!(r.hits.load(Ordering::Relaxed), 1);
        assert_eq!(r.misses.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn miss_on_partial_set_leaves_entry() {
        let r = reg();
        r.insert_lease("p1", "g", "w1", 42, &[1, 2, 3], 60, T0);
        // Missing a member → MISS, entry untouched (a later completing ack could
        // still hit — though in practice the caller evicts on the SQL path).
        assert_eq!(r.take_if_full_batch("p1", "g", "w1", &[1, 2]), None);
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn miss_on_extra_hash() {
        let r = reg();
        r.insert_lease("p1", "g", "w1", 42, &[1, 2, 3], 60, T0);
        // An extra hash not in the batch → not an exact cover → MISS.
        assert_eq!(r.take_if_full_batch("p1", "g", "w1", &[1, 2, 3, 9]), None);
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn miss_on_worker_mismatch_does_not_evict() {
        let r = reg();
        r.insert_lease("p1", "g", "w1", 42, &[1, 2, 3], 60, T0);
        // Wrong worker (e.g. a late ack after the batch was re-leased) → MISS,
        // and the CURRENT holder's entry must survive.
        assert_eq!(r.take_if_full_batch("p1", "g", "w2", &[1, 2, 3]), None);
        assert_eq!(r.len(), 1);
        // The real holder still hits.
        assert_eq!(r.take_if_full_batch("p1", "g", "w1", &[1, 2, 3]), Some(42));
    }

    #[test]
    fn duplicates_in_request_tolerated() {
        let r = reg();
        r.insert_lease("p1", "g", "w1", 7, &[10, 20], 60, T0);
        // Client re-sent an ack for txn 10 → still an exact cover of {10,20}.
        assert_eq!(r.take_if_full_batch("p1", "g", "w1", &[10, 10, 20, 20]), Some(7));
    }

    #[test]
    fn shared_txn_collapses_to_distinct_cover() {
        let r = reg();
        // Two delivered frames share a transactionId (hash 10); batch_end spans
        // both offsets. Acking {10,20} covers the distinct set → HIT (SQL would
        // resolve hash 10 to both offsets, so advancing to batch_end is correct).
        r.insert_lease("p1", "g", "w1", 5, &[10, 10, 20], 60, T0);
        assert_eq!(r.take_if_full_batch("p1", "g", "w1", &[10, 20]), Some(5));
    }

    #[test]
    fn evict_removes_own_entry() {
        let r = reg();
        r.insert_lease("p1", "g", "w1", 42, &[1, 2, 3], 60, T0);
        r.evict("p1", "g", "w1");
        assert_eq!(r.len(), 0);
        assert_eq!(r.bytes(), 0);
        assert_eq!(r.take_if_full_batch("p1", "g", "w1", &[1, 2, 3]), None);
    }

    #[test]
    fn evict_guarded_by_worker() {
        let r = reg();
        r.insert_lease("p1", "g", "w1", 42, &[1, 2, 3], 60, T0);
        // A stale ack from a DIFFERENT worker must not drop the current holder's
        // entry (nor does an empty/lease-less worker).
        r.evict("p1", "g", "w2");
        r.evict("p1", "g", "");
        assert_eq!(r.len(), 1);
        assert_eq!(r.take_if_full_batch("p1", "g", "w1", &[1, 2, 3]), Some(42));
    }

    #[test]
    fn ttl_sweep_reclaims_expired() {
        let r = reg();
        r.insert_lease("p1", "g", "w1", 1, &[1], 60, T0); // expires at T0+70s
        r.insert_lease("p2", "g", "w2", 2, &[2], 60, T0);
        assert_eq!(r.len(), 2);
        // Before expiry: nothing swept.
        r.ttl_sweep(T0 + 60_000);
        assert_eq!(r.len(), 2);
        // Past lease+margin: both gone.
        r.ttl_sweep(T0 + 71_000);
        assert_eq!(r.len(), 0);
        assert_eq!(r.bytes(), 0);
    }

    #[test]
    fn replace_same_key_adjusts_bytes() {
        let r = reg();
        r.insert_lease("p1", "g", "w1", 1, &[1, 2, 3], 60, T0);
        let b1 = r.bytes();
        // Re-lease same (pid,group) with a new worker and a bigger batch.
        r.insert_lease("p1", "g", "w2", 9, &[1, 2, 3, 4, 5], 60, T0);
        assert_eq!(r.len(), 1);
        assert!(r.bytes() > b1);
        // Old worker no longer matches; new one hits at the new batch_end.
        assert_eq!(r.take_if_full_batch("p1", "g", "w1", &[1, 2, 3, 4, 5]), None);
        assert_eq!(r.take_if_full_batch("p1", "g", "w2", &[1, 2, 3, 4, 5]), Some(9));
    }

    #[test]
    fn lru_eviction_bounds_memory() {
        // Tiny cap: one entry's footprint plus a hair. Inserting a second evicts
        // the LRU first. Sized off a measured entry so the test tracks the model.
        let probe = AckRegistry::new(64, true);
        probe.insert_lease("p", "g", "w", 0, &[1], 60, T0);
        let one = probe.bytes();

        // A registry whose byte cap holds ~1.5 entries (constructed directly to
        // set an exact sub-MB budget the MB-granular public ctor can't express).
        let r = AckRegistry {
            state: Mutex::new(State {
                map: HashMap::new(),
                total_bytes: 0,
                tick: 0,
                last_sweep_ms: 0,
            }),
            max_bytes: one + one / 2,
            enabled: true,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            last_report_ms: AtomicI64::new(0),
        };
        r.insert_lease("p1", "g", "w1", 1, &[1], 60, T0);
        r.insert_lease("p2", "g", "w2", 2, &[2], 60, T0 + 1); // p1 is LRU
        assert_eq!(r.len(), 1, "over-budget insert evicted the LRU entry");
        assert!(r.bytes() <= one + one / 2);
        // p1 was evicted → its ack misses (falls to SQL); p2 still hits.
        assert_eq!(r.take_if_full_batch("p1", "g", "w1", &[1]), None);
        assert_eq!(r.take_if_full_batch("p2", "g", "w2", &[2]), Some(2));
    }

    #[test]
    fn disabled_is_all_noop() {
        let r = AckRegistry::new(64, false);
        r.insert_lease("p1", "g", "w1", 42, &[1, 2, 3], 60, T0);
        assert_eq!(r.len(), 0);
        assert_eq!(r.take_if_full_batch("p1", "g", "w1", &[1, 2, 3]), None);
        // Disabled ⇒ not even miss bookkeeping.
        assert_eq!(r.hits.load(Ordering::Relaxed), 0);
        assert_eq!(r.misses.load(Ordering::Relaxed), 0);
    }
}
