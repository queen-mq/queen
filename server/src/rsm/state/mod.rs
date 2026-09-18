//! The committed view: what the planner and apply see.
//!
//! Two halves, and the split is I1's:
//!
//! - **the store** — the committed rows, read through a read transaction
//!   (planner) or through the apply thread's open write transaction (apply).
//!   Typed by [`crate::rsm::store::TypedReads`].
//! - **[`Derived`]** — the RAM indexes of §6.3, which hold nothing the store
//!   does not: the ready rings per (tenant, queue, group), the visibility
//!   deadlines, and the lease deadlines. They are REBUILT at open and at
//!   leadership start ([`Derived::rebuild`]) from `pending` and
//!   `leases_by_worker`, so their size follows the work outstanding, never the
//!   volume retained (I8).
//!
//! [`Committed`] joins the two for the planner. It is read-only by
//! construction: it borrows `&Derived`, so a planner cannot pop a ring or
//! promote a deadline. Apply holds `&mut Derived` and is the only caller of
//! the mutators.
//!
//! # Ported from pgless, minus the hazard
//!
//! `ReadyIndex` and the ring discipline come from
//! `git show 6e96e228:server/src/native/state.rs`: a FIFO ring of candidates
//! plus a deadline heap, COARSE BY CONTRACT (an entry means "this partition
//! looked claimable when we last touched it"; the claim re-verifies). What did
//! NOT come across is pgless's `BucketState::apply`, which the live write path
//! called at PLAN time: that is hazard D-1, and in the RSM the planner
//! produces effects and apply executes them.
//!
//! The other difference is where the truth lives. In pgless the rings were
//! derived from an in-RAM partition table; here they are derived from the
//! `pending` keyspace, which apply maintains (one row per (tenant, queue,
//! group, partition) with work) — so a restart rebuilds them in O(pending)
//! with one ordered scan, and nothing walks the partitions.

// I2, enforced rather than reviewed: `clippy.toml` lists the clock,
// environment and randomness calls this side of the line may not make,
// `[lints.clippy]` in Cargo.toml switches the lint off for the rest of the
// package (the postgres class, and every integration test), and this is where
// it is switched back on — for this module and every module under it.
#![deny(clippy::disallowed_methods)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use crate::rsm::effect::{CursorRow, Pid, QueueConfig};
use crate::rsm::store::keys::Counter;
use crate::rsm::store::rows::{GarbageRow, GroupRow, PartitionRow};
use crate::rsm::store::{Reads, Result, TypedReads};

/// Which ring: `(tenant, queue, group)`.
pub type RingKey = (String, String, String);

/// Rows per chunk of the `pending` scan in [`Derived::rebuild`].
pub const REBUILD_CHUNK: usize = 4096;

fn ring_key(tenant: &str, queue: &str, group: &str) -> RingKey {
    (tenant.to_string(), queue.to_string(), group.to_string())
}

// ---------------------------------------------------------------------------
// The candidate ring
// ---------------------------------------------------------------------------

/// The per-(queue, group) candidate ring the wildcard pop walks, ported from
/// pgless `native/state.rs`.
///
/// COARSE BY CONTRACT, like the SQL candidate scan it replaces: an entry means
/// "this partition looked claimable when we last touched it", and the claim
/// re-verifies against the cursor row. `deferred` holds revisit deadlines — a
/// lease expiry, a `delayed_processing` or `window_buffer` visibility deadline
/// — and [`Derived::promote_due`] moves them back into `ready`.
///
/// `ready` may hold stale entries: removal only clears `in_ready`, and the
/// walker skips anything no longer in the set. [`ReadyIndex::compact`] keeps
/// that slack bounded — on every push AND on every removal — so neither a
/// partition that cycles ready → not-ready without ever being popped nor a
/// ring that DRAINS can leave the deque bigger than `max(16, 2 × live)`.
///
/// # One slot per partition (the invariant `walk` rests on)
///
/// `queued` is the set of pids the deque holds, and it is what keeps the deque
/// FREE OF DUPLICATES. pgless could do without it: its only walker was the
/// CONSUMING `pop_front`, which dropped a stale entry as it passed it, so the
/// slack healed itself. Here the planner reads with [`ReadyIndex::walk`] and
/// mutates nothing (I1), so a stale entry is never dropped by a read — and
/// without `queued` the ordinary apply cycle (`Append` → `set_pending`, the
/// group catches up → `clear_pending`, the next `Append` → `set_pending`) left
/// one stale entry per turn that `walk` could NOT skip, because the pid was
/// live again. The wildcard pop's budget walk (004) would then spend its
/// budget offering one hot partition many times and never reach the others.
///
/// So: liveness is `in_ready`, presence in the deque is `queued`, and
/// `in_ready ⊆ queued`. A partition that becomes ready again reuses the slot it
/// already holds (keeping its FIFO position, which is its first-ready order),
/// and only [`ReadyIndex::pop_front`] and [`ReadyIndex::compact`] release one.
/// The same discipline holds for `deferred`: `(deadline, pid)` in deadline
/// order, plus `deferred_at` so that re-deferring a partition MOVES its
/// deadline instead of adding a second one — the shape [`Derived::note_lease`]
/// already uses for lease deadlines. A debounced queue re-defers its partition
/// on every append, so an append-rate-sized heap of duplicates is exactly what
/// I8 asks RAM not to hold.
#[derive(Clone, Debug, Default)]
pub struct ReadyIndex {
    ready: VecDeque<Pid>,
    /// The pids `ready` holds — exactly one entry each.
    queued: HashSet<Pid>,
    /// The pids that are live candidates. Always a subset of `queued`.
    in_ready: HashSet<Pid>,
    /// `(revisit_at_us, pid)` in deadline order — one entry per pid.
    deferred: BTreeSet<(i64, Pid)>,
    /// `pid` → the deadline it has in `deferred`.
    deferred_at: HashMap<Pid, i64>,
}

impl ReadyIndex {
    pub fn push(&mut self, pid: Pid) {
        // Ready now supersedes any revisit deadline: apply is telling us what
        // is true of this partition today.
        self.clear_deferred(pid);
        if self.in_ready.insert(pid) && self.queued.insert(pid) {
            self.ready.push_back(pid);
            self.compact();
        }
    }

    pub fn remove(&mut self, pid: Pid) {
        if self.in_ready.remove(&pid) {
            // Removal is what DRAINS a ring, and a drain is the only way the
            // deque grows far past what is live: a burst makes every partition
            // of a queue pending, the group catches up, and the slots stay
            // behind with nothing ever pushed again to trigger a compaction.
            // Every later walk would then pay for them (see `compact`).
            self.compact();
        }
    }

    /// Drop every trace of a partition — it is DELETED, not merely idle — so
    /// no parked deadline can put a dead pid back in the ring.
    pub fn forget(&mut self, pid: Pid) {
        self.clear_deferred(pid);
        self.remove(pid);
    }

    fn clear_deferred(&mut self, pid: Pid) {
        if let Some(at) = self.deferred_at.remove(&pid) {
            self.deferred.remove(&(at, pid));
        }
    }

    pub fn contains(&self, pid: Pid) -> bool {
        self.in_ready.contains(&pid)
    }

    /// Take the next live candidate in FIFO order, dropping stale entries.
    /// APPLY-SIDE: the planner walks with [`ReadyIndex::walk`] instead.
    pub fn pop_front(&mut self) -> Option<Pid> {
        while let Some(pid) = self.ready.pop_front() {
            self.queued.remove(&pid);
            if self.in_ready.remove(&pid) {
                return Some(pid);
            }
        }
        None
    }

    /// Park a partition until `at_us`. A partition already parked MOVES to the
    /// new deadline; it never gets a second one.
    pub fn defer(&mut self, at_us: i64, pid: Pid) {
        self.remove(pid);
        self.clear_deferred(pid);
        self.deferred.insert((at_us, pid));
        self.deferred_at.insert(pid, at_us);
    }

    /// Move every deadline that has passed back into `ready`. The claim
    /// re-verifies, so promotion asks no question of the store (§6.3: the ring
    /// is a hint).
    pub fn promote_due(&mut self, now_us: i64) -> usize {
        let mut n = 0;
        while let Some(&(at, pid)) = self.deferred.iter().next() {
            if at > now_us {
                break;
            }
            self.deferred.remove(&(at, pid));
            self.deferred_at.remove(&pid);
            self.push(pid);
            n += 1;
        }
        n
    }

    /// The live candidates in FIFO order, WITHOUT mutating: what the planner
    /// reads (I1). Stops when `cb` returns false or after `limit` candidates.
    ///
    /// Each live partition is offered EXACTLY ONCE (the `queued` invariant
    /// above), so `limit` is a budget of distinct partitions — which is what
    /// 004's budget walk spends — and one hot partition cannot starve the
    /// rest. The walk costs O(deque), which [`ReadyIndex::compact`] keeps at
    /// `max(16, 2 × live)` — compacting on removal as well as on push, since a
    /// drained ring is all removals.
    pub fn walk(&self, limit: usize, cb: &mut dyn FnMut(Pid) -> bool) -> usize {
        let mut n = 0;
        for pid in self.ready.iter() {
            if !self.in_ready.contains(pid) {
                continue;
            }
            n += 1;
            if !cb(*pid) || n >= limit {
                break;
            }
        }
        n
    }

    /// Live candidates.
    pub fn live_len(&self) -> usize {
        self.in_ready.len()
    }

    /// The earliest deadline parked here, if any: what a long-poll re-check
    /// wakes on.
    pub fn next_deadline(&self) -> Option<i64> {
        self.deferred.iter().next().map(|(at, _)| *at)
    }

    pub fn deferred_len(&self) -> usize {
        self.deferred.len()
    }

    /// Drop the slots of partitions that are no longer live, so the deque
    /// stays at `max(16, 2 × live)` — and with it the cost of a walk.
    ///
    /// Called from BOTH sides of the ring's life, and the removal side is the
    /// one that matters: a ring only ever grows past its live set when a group
    /// drains, and a drain is all removals. Called from `push` alone, the
    /// guard below could never fire during one (the deque and the live set
    /// shrink together in the ratio, or rather the deque does not shrink at
    /// all), so 60 000 drained partitions stayed in the deque and every later
    /// `walk` — every wildcard pop attempt, every long-poll re-check — paid
    /// for all of them.
    ///
    /// The cost is AMORTIZED O(1) per mutation: the pass is O(deque) and it
    /// leaves `deque == live`, so the next one needs the live set to halve
    /// again, which takes at least `deque / 2` more removals.
    fn compact(&mut self) {
        if self.ready.len() <= 16 || self.ready.len() <= self.in_ready.len() * 2 {
            return;
        }
        let live = std::mem::take(&mut self.in_ready);
        self.ready.retain(|pid| live.contains(pid));
        self.queued.clear();
        self.queued.extend(self.ready.iter().copied());
        self.in_ready = live;
        debug_assert_eq!(
            self.queued.len(),
            self.ready.len(),
            "the deque holds a partition twice"
        );
    }
}

// ---------------------------------------------------------------------------
// Derived
// ---------------------------------------------------------------------------

/// The RAM half of §6.3: rings, visibility deadlines and lease deadlines.
///
/// Iteration is over `BTreeMap`/`BTreeSet`, never a hash map: I2 forbids
/// apply depending on hash-map order, and a reproducible differential fuzzer
/// needs the same from the planner's candidate walk.
#[derive(Clone, Debug, Default)]
pub struct Derived {
    rings: BTreeMap<RingKey, ReadyIndex>,
    /// `(expires_at_us, pid, group, worker)` — the lease deadline order.
    lease_deadlines: BTreeSet<(i64, Pid, String, String)>,
    /// `(pid, group) → (expires_at_us, worker)`, so a renew can move exactly
    /// one entry of the set above.
    lease_at: BTreeMap<(Pid, String), (i64, String)>,
    /// `pending` rows seen: a size metric, not a source of truth.
    pending_rows: u64,
}

impl Derived {
    /// Rebuild from committed state (§6.3: at open, and at leadership start).
    ///
    /// One ordered scan of `pending` and one of `leases_by_worker`. Both are
    /// bounded by the work outstanding — partitions with pending frames, and
    /// live leases — never by the volume retained, which is I8.
    pub fn rebuild<R: Reads + ?Sized>(reads: &R, now_us: i64) -> Result<Derived> {
        let mut d = Derived::default();
        let mut from: Vec<u8> = Vec::new();
        let max_key = reads.max_key_len();
        // Chunked so one scan never holds the whole keyspace in a callback
        // frame. The resume key is [`resume_after`], NOT `last ‖ 0x00`: a
        // `pending` key is `(tenant, queue, group, pid)`, three unbounded
        // names, so it can be exactly as long as the engine allows — and a
        // key one byte over the limit is refused by every later scan, which
        // would leave the ready rings unbuildable and this node unable to
        // finish §11.5 and serve.
        loop {
            let mut last: Option<Vec<u8>> = None;
            let mut seen = 0usize;
            reads.scan_pending(&from, REBUILD_CHUNK, &mut |t, q, g, pid, ready_at| {
                d.set_pending_inner(t, q, g, pid, ready_at, now_us);
                d.pending_rows += 1;
                seen += 1;
                last = Some(crate::rsm::store::keys::pending(t, q, g, pid));
                true
            })?;
            if seen < REBUILD_CHUNK {
                break;
            }
            match last.and_then(|k| crate::rsm::store::resume_after(&k, max_key)) {
                Some(k) => from = k,
                // No storable key is greater than the last one seen: the
                // keyspace is exhausted.
                None => break,
            }
        }

        // Leases: the index is keyed by worker, so one full ordered scan gives
        // every live lease once.
        let mut err: Option<crate::rsm::store::StoreError> = None;
        reads.scan_raw(
            crate::rsm::store::Keyspace::LeasesByWorker,
            &[],
            &[],
            usize::MAX,
            &mut |k, v| match (
                crate::rsm::store::keys::leases_by_worker_parts(k),
                crate::rsm::store::rows::i64_decode(v),
            ) {
                (Some((worker, pid, group)), Ok(at)) => {
                    d.note_lease(&worker, pid, &group, at);
                    true
                }
                _ => {
                    err = Some(crate::rsm::store::StoreError::corrupt(
                        crate::rsm::store::Keyspace::LeasesByWorker,
                        "lease row",
                    ));
                    false
                }
            },
        )?;
        if let Some(e) = err {
            return Err(e);
        }
        Ok(d)
    }

    // ------------------------------------------------------------- mutators
    // Apply only (I1).

    fn set_pending_inner(
        &mut self,
        tenant: &str,
        queue: &str,
        group: &str,
        pid: Pid,
        ready_at_us: i64,
        now_us: i64,
    ) {
        let ring = self
            .rings
            .entry(ring_key(tenant, queue, group))
            .or_default();
        if ready_at_us <= now_us {
            ring.push(pid);
        } else {
            ring.defer(ready_at_us, pid);
        }
    }

    /// A partition has work for a group from `ready_at_us` on. Mirrors the
    /// `pending` row apply just wrote.
    pub fn set_pending(
        &mut self,
        tenant: &str,
        queue: &str,
        group: &str,
        pid: Pid,
        ready_at_us: i64,
        now_us: i64,
    ) {
        self.set_pending_inner(tenant, queue, group, pid, ready_at_us, now_us);
    }

    /// The partition has no work for the group any more: apply has deleted its
    /// `pending` row, so neither the candidate nor any revisit deadline it
    /// carried survives — the ring mirrors `pending`, nothing more.
    pub fn clear_pending(&mut self, tenant: &str, queue: &str, group: &str, pid: Pid) {
        if let Some(ring) = self.rings.get_mut(&ring_key(tenant, queue, group)) {
            ring.forget(pid);
        }
    }

    /// Make sure a ring exists, so a group that has registered but has no
    /// pending partition still has a place to be seeded into. Returns true
    /// when this call created it (pgless's `ensure_group`, which 004's
    /// `v_first_seen > 0` decides on).
    pub fn ensure_ring(&mut self, tenant: &str, queue: &str, group: &str) -> bool {
        let k = ring_key(tenant, queue, group);
        if self.rings.contains_key(&k) {
            return false;
        }
        self.rings.insert(k, ReadyIndex::default());
        true
    }

    /// Forget a group's ring (a group delete, 014).
    pub fn drop_ring(&mut self, tenant: &str, queue: &str, group: &str) {
        self.rings.remove(&ring_key(tenant, queue, group));
    }

    /// Record or move a lease deadline (the RAM twin of `leases_by_worker`).
    pub fn note_lease(&mut self, worker: &str, pid: Pid, group: &str, expires_at_us: i64) {
        let key = (pid, group.to_string());
        if let Some((old_at, old_worker)) = self.lease_at.remove(&key) {
            self.lease_deadlines
                .remove(&(old_at, pid, group.to_string(), old_worker));
        }
        self.lease_deadlines
            .insert((expires_at_us, pid, group.to_string(), worker.to_string()));
        self.lease_at
            .insert(key, (expires_at_us, worker.to_string()));
    }

    /// The lease is gone (acked, released, expired and reclaimed).
    pub fn clear_lease(&mut self, pid: Pid, group: &str) {
        if let Some((at, worker)) = self.lease_at.remove(&(pid, group.to_string())) {
            self.lease_deadlines
                .remove(&(at, pid, group.to_string(), worker));
        }
    }

    /// Everything a partition delete drops from RAM.
    pub fn forget_partition(&mut self, pid: Pid) {
        let groups: Vec<(Pid, String)> = self
            .lease_at
            .range((pid, String::new())..)
            .take_while(|((p, _), _)| *p == pid)
            .map(|(k, _)| k.clone())
            .collect();
        for (p, g) in groups {
            self.clear_lease(p, &g);
        }
        for ring in self.rings.values_mut() {
            ring.forget(pid);
        }
    }

    /// Promote one ring's deadlines that have passed.
    pub fn promote_due(&mut self, tenant: &str, queue: &str, group: &str, now_us: i64) -> usize {
        match self.rings.get_mut(&ring_key(tenant, queue, group)) {
            Some(ring) => ring.promote_due(now_us),
            None => 0,
        }
    }

    /// Promote every ring. O(rings) by design: it runs on a slow tick, not on
    /// the pop path, which promotes its own ring inline.
    pub fn tick(&mut self, now_us: i64) -> usize {
        let keys: Vec<RingKey> = self.rings.keys().cloned().collect();
        let mut n = 0;
        for (t, q, g) in keys {
            n += self.promote_due(&t, &q, &g, now_us);
        }
        n
    }

    // ---------------------------------------------------------------- reads

    pub fn ring(&self, tenant: &str, queue: &str, group: &str) -> Option<&ReadyIndex> {
        self.rings.get(&ring_key(tenant, queue, group))
    }

    pub fn rings_len(&self) -> usize {
        self.rings.len()
    }

    pub fn pending_rows(&self) -> u64 {
        self.pending_rows
    }

    /// Leases whose deadline has passed, in deadline order:
    /// `(pid, group, worker, expires_at_us)`. READ-ONLY — reclaiming one is a
    /// planned effect, not a RAM edit.
    pub fn expired_leases(&self, now_us: i64, limit: usize) -> Vec<(Pid, String, String, i64)> {
        self.lease_deadlines
            .iter()
            .take_while(|(at, _, _, _)| *at <= now_us)
            .take(limit)
            .map(|(at, pid, group, worker)| (*pid, group.clone(), worker.clone(), *at))
            .collect()
    }

    /// The earliest lease deadline, for the loop that schedules the next
    /// re-check.
    pub fn next_lease_deadline(&self) -> Option<i64> {
        self.lease_deadlines.iter().next().map(|(at, ..)| *at)
    }

    pub fn lease_count(&self) -> usize {
        self.lease_at.len()
    }
}

// ---------------------------------------------------------------------------
// The committed view
// ---------------------------------------------------------------------------

/// What the planner reads: committed rows plus the derived indexes, and
/// nothing it can change (I1).
///
/// It is generic over the handle so the same code runs over a read
/// transaction (planning) and over the apply thread's open write transaction
/// (an apply-time lookup), which is what keeps the two from drifting.
pub struct Committed<'a, R: Reads + ?Sized> {
    reads: &'a R,
    derived: &'a Derived,
}

impl<'a, R: Reads + ?Sized> Committed<'a, R> {
    pub fn new(reads: &'a R, derived: &'a Derived) -> Committed<'a, R> {
        Committed { reads, derived }
    }

    /// The raw handle, for a read this view has no typed name for yet.
    pub fn reads(&self) -> &'a R {
        self.reads
    }

    pub fn derived(&self) -> &'a Derived {
        self.derived
    }

    // ------------------------------------------------------------ the clock

    /// `max(wall, last committed now + 1, max_created_at + 1)` — the planner's
    /// stamp (D5, §7.4, I5). The WALL CLOCK IS THE CALLER'S: nothing under
    /// `rsm/state/` reads a clock (I2), so the planner passes what it read.
    pub fn plan_now(&self, wall_us: i64) -> Result<i64> {
        let last = self.reads.last_now_us()?;
        let maxc = self.reads.max_created_at_us()?;
        Ok(wall_us
            .max(last.saturating_add(1))
            .max(maxc.saturating_add(1)))
    }

    // ----------------------------------------------------------- name lookups

    pub fn queue(&self, tenant: &str, queue: &str) -> Result<Option<QueueConfig>> {
        self.reads.queue(tenant, queue)
    }

    pub fn group(&self, tenant: &str, queue: &str, group: &str) -> Result<Option<GroupRow>> {
        self.reads.group(tenant, queue, group)
    }

    /// The pid of a partition NAME, `None` when it does not exist or when it
    /// is in the garbage set (§5.2 rules: readers and planners ignore garbage
    /// pids, and the name is reusable at once).
    pub fn pid_of(&self, tenant: &str, queue: &str, partition: &str) -> Result<Option<Pid>> {
        let Some(pid) = self.reads.pid_of(tenant, queue, partition)? else {
            return Ok(None);
        };
        if self.reads.garbage(pid)?.is_some() {
            return Ok(None);
        }
        Ok(Some(pid))
    }

    /// The partition row, `None` for a garbage pid.
    pub fn partition(&self, pid: Pid) -> Result<Option<PartitionRow>> {
        if self.reads.garbage(pid)?.is_some() {
            return Ok(None);
        }
        self.reads.partition(pid)
    }

    pub fn garbage(&self, pid: Pid) -> Result<Option<GarbageRow>> {
        self.reads.garbage(pid)
    }

    pub fn cursor(&self, pid: Pid, group: &str) -> Result<Option<CursorRow>> {
        self.reads.cursor(pid, group)
    }

    // -------------------------------------------------------------- counters

    pub fn partition_counter(&self, pid: Pid, c: Counter) -> Result<i64> {
        self.reads.partition_counter(pid, c)
    }

    pub fn queue_counter(&self, tenant: &str, queue: &str, c: Counter) -> Result<i64> {
        self.reads.queue_counter(tenant, queue, c)
    }

    // --------------------------------------------------------- the ring walk

    /// Candidates for a (tenant, queue, group), in ring order, WITHOUT
    /// touching the ring. The claim re-verifies each one against its cursor.
    pub fn candidates(
        &self,
        tenant: &str,
        queue: &str,
        group: &str,
        limit: usize,
        cb: &mut dyn FnMut(Pid) -> bool,
    ) -> usize {
        match self.derived.ring(tenant, queue, group) {
            Some(r) => r.walk(limit, cb),
            None => 0,
        }
    }

    /// The request-id window (D6, I6): the recorded outcome of a command that
    /// has already been logged, or `None`.
    pub fn recorded_outcome(&self, id: &[u8; 16]) -> Result<Option<Vec<u8>>> {
        Ok(self.reads.request_outcome(id)?.map(|r| r.outcome))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ring_is_fifo_and_skips_stale_entries() {
        let mut r = ReadyIndex::default();
        r.push(1);
        r.push(2);
        r.push(3);
        r.remove(2);
        assert_eq!(r.live_len(), 2);
        let mut seen = Vec::new();
        r.walk(10, &mut |p| {
            seen.push(p);
            true
        });
        assert_eq!(seen, vec![1, 3]);
        // The walk did not consume anything (I1: the planner does not mutate).
        assert_eq!(r.live_len(), 2);
        assert_eq!(r.pop_front(), Some(1));
        assert_eq!(r.pop_front(), Some(3));
        assert_eq!(r.pop_front(), None);
    }

    #[test]
    fn pushing_twice_does_not_duplicate() {
        let mut r = ReadyIndex::default();
        r.push(7);
        r.push(7);
        assert_eq!(r.live_len(), 1);
        assert_eq!(r.pop_front(), Some(7));
        assert_eq!(r.pop_front(), None);
    }

    #[test]
    fn compaction_bounds_the_deque() {
        let mut r = ReadyIndex::default();
        for i in 0..1000u64 {
            r.push(i);
            r.remove(i);
        }
        r.push(999_999);
        assert!(r.ready.len() < 64, "deque grew to {}", r.ready.len());
    }

    #[test]
    fn a_drained_ring_does_not_leave_the_walk_walking_stale_slots() {
        // The shape the cost bound above is really about: a burst makes every
        // partition of a (tenant, queue, group) pending, then the group
        // catches up and apply clears every `pending` row. Nothing is pushed
        // again, so a compaction that only ever ran inside `push` never ran —
        // and every later wildcard pop and long-poll re-check on that group
        // walked all 60 000 dead slots.
        const N: u64 = 60_000;
        let mut r = ReadyIndex::default();
        for pid in 0..N {
            r.push(pid);
        }
        assert_eq!(r.ready.len() as u64, N);
        for pid in 0..N {
            r.remove(pid);
        }
        assert_eq!(r.live_len(), 0);
        assert!(
            r.ready.len() <= 16,
            "the drained ring still holds {} slots",
            r.ready.len()
        );
        let mut visited = 0;
        r.walk(10, &mut |_| {
            visited += 1;
            true
        });
        assert_eq!(visited, 0);
    }

    #[test]
    fn the_deque_stays_inside_its_bound_through_any_mixture() {
        // The bound `walk` documents, asserted after EVERY mutation rather
        // than at the end: pushes, removals, defers and promotions mixed.
        let mut r = ReadyIndex::default();
        let bound = |r: &ReadyIndex| std::cmp::max(16, r.live_len() * 2);
        for round in 0..200u64 {
            let pid = round % 37;
            match round % 4 {
                0 => r.push(pid),
                1 => r.remove(pid),
                2 => r.defer(round as i64 + 1, pid),
                _ => {
                    r.promote_due(round as i64);
                }
            }
            assert!(
                r.ready.len() <= bound(&r),
                "round {round}: deque {} over the bound {}",
                r.ready.len(),
                bound(&r)
            );
        }
        // And the whole ring drains to nothing.
        for pid in 0..37u64 {
            r.forget(pid);
        }
        assert_eq!(r.live_len(), 0);
        assert_eq!(r.deferred_len(), 0);
        assert!(r.ready.len() <= 16, "{} slots left", r.ready.len());
    }

    #[test]
    fn deferred_entries_promote_in_deadline_order() {
        let mut d = Derived::default();
        d.set_pending("t", "q", "g", 1, 100, 0);
        d.set_pending("t", "q", "g", 2, 50, 0);
        d.set_pending("t", "q", "g", 3, 0, 0);
        let ring = d.ring("t", "q", "g").unwrap();
        assert_eq!(ring.live_len(), 1, "only pid 3 is ready at now=0");
        assert_eq!(ring.next_deadline(), Some(50));
        assert_eq!(d.promote_due("t", "q", "g", 60), 1);
        let ring = d.ring("t", "q", "g").unwrap();
        assert!(ring.contains(2));
        assert!(!ring.contains(1));
        assert_eq!(d.tick(1000), 1);
        assert!(d.ring("t", "q", "g").unwrap().contains(1));
    }

    #[test]
    fn a_renew_moves_one_lease_deadline() {
        let mut d = Derived::default();
        d.note_lease("w1", 1, "g", 100);
        d.note_lease("w1", 2, "g", 200);
        assert_eq!(d.lease_count(), 2);
        assert_eq!(d.next_lease_deadline(), Some(100));
        d.note_lease("w1", 1, "g", 300);
        assert_eq!(d.lease_count(), 2, "the old deadline did not linger");
        assert_eq!(d.next_lease_deadline(), Some(200));
        let exp = d.expired_leases(250, 10);
        assert_eq!(exp.len(), 1);
        assert_eq!(exp[0].0, 2);
        d.clear_lease(2, "g");
        assert_eq!(d.lease_count(), 1);
        assert!(d.expired_leases(250, 10).is_empty());
    }

    #[test]
    fn forgetting_a_partition_clears_its_leases_and_ring_entries() {
        let mut d = Derived::default();
        d.set_pending("t", "q", "g", 5, 0, 0);
        d.note_lease("w", 5, "g", 100);
        d.note_lease("w", 6, "g", 100);
        d.forget_partition(5);
        assert!(!d.ring("t", "q", "g").unwrap().contains(5));
        assert_eq!(d.lease_count(), 1);
    }

    #[test]
    fn a_partition_that_cycles_ready_is_offered_once() {
        // The ordinary apply cycle: Append -> set_pending, the group catches
        // up -> clear_pending, the next Append -> set_pending. Each turn used
        // to leave one stale deque entry that `walk` could not skip (the pid
        // is live again), so the planner saw the same partition N times.
        let mut r = ReadyIndex::default();
        for _ in 0..4 {
            r.push(1);
            r.remove(1);
        }
        r.push(1);
        assert_eq!(r.live_len(), 1);
        let mut seen = Vec::new();
        r.walk(10, &mut |p| {
            seen.push(p);
            true
        });
        assert_eq!(seen, vec![1], "the walk offered one partition many times");
    }

    #[test]
    fn a_hot_partition_cannot_starve_the_others_in_the_budget() {
        // The wildcard pop's budget walk (004) asks for `limit` DISTINCT
        // candidates. A partition that cycles must not spend the budget.
        let mut r = ReadyIndex::default();
        for _ in 0..3 {
            for pid in 1..=3u64 {
                r.push(pid);
                r.remove(pid);
            }
        }
        for pid in 1..=3u64 {
            r.push(pid);
        }
        assert_eq!(r.live_len(), 3);
        let mut seen = Vec::new();
        r.walk(3, &mut |p| {
            seen.push(p);
            true
        });
        assert_eq!(seen, vec![1, 2, 3], "the budget went to one partition");
    }

    #[test]
    fn a_deferred_partition_keeps_one_slot_when_it_promotes() {
        // defer() clears liveness and leaves the deque entry; the promotion
        // must reuse that entry, never append a second one.
        let mut d = Derived::default();
        for _ in 0..5 {
            d.set_pending("t", "q", "g", 1, 100, 0); // deferred: 100 > now 0
            assert_eq!(d.promote_due("t", "q", "g", 200), 1);
        }
        let ring = d.ring("t", "q", "g").unwrap();
        assert_eq!(ring.live_len(), 1);
        let mut seen = Vec::new();
        ring.walk(10, &mut |p| {
            seen.push(p);
            true
        });
        assert_eq!(seen, vec![1]);
    }

    #[test]
    fn the_deque_holds_each_partition_at_most_once() {
        // The invariant the two tests above rest on, stated directly.
        let mut r = ReadyIndex::default();
        for round in 0..50u64 {
            for pid in 1..=5u64 {
                r.push(pid);
                if (pid + round) % 2 == 0 {
                    r.remove(pid);
                }
            }
        }
        let mut uniq: HashSet<Pid> = HashSet::new();
        for pid in r.ready.iter() {
            assert!(uniq.insert(*pid), "pid {pid} is in the deque twice");
        }
        assert!(r.ready.len() <= 5, "the deque grew to {}", r.ready.len());
    }

    #[test]
    fn a_re_deferred_partition_keeps_one_deadline() {
        // A debounced queue re-defers its partition on every append. Each one
        // used to add an entry; the heap then held one per append, and every
        // stale entry re-pushed the partition at its own time.
        let mut r = ReadyIndex::default();
        for at in [500, 400, 900, 300] {
            r.defer(at, 1);
        }
        assert_eq!(r.deferred_len(), 1, "one deadline per partition");
        assert_eq!(r.next_deadline(), Some(300), "the LAST word wins");
        assert_eq!(r.promote_due(300), 1);
        assert_eq!(r.deferred_len(), 0, "a stale deadline lingered");
        assert!(r.contains(1));
        assert_eq!(r.promote_due(10_000), 0, "it was promoted twice");
    }

    #[test]
    fn becoming_ready_cancels_the_parked_deadline() {
        let mut r = ReadyIndex::default();
        r.defer(1_000, 7);
        r.push(7);
        assert_eq!(r.deferred_len(), 0);
        assert_eq!(r.next_deadline(), None);
        assert!(r.contains(7));
    }

    #[test]
    fn clearing_pending_cancels_the_parked_deadline() {
        // `pending` is the truth; the ring mirrors it. A deleted row must not
        // leave a deadline that re-offers the partition later.
        let mut d = Derived::default();
        d.set_pending("t", "q", "g", 3, 9_000, 0);
        d.clear_pending("t", "q", "g", 3);
        assert_eq!(d.ring("t", "q", "g").unwrap().deferred_len(), 0);
        assert_eq!(d.tick(100_000), 0);
        assert!(!d.ring("t", "q", "g").unwrap().contains(3));
    }

    #[test]
    fn forgetting_a_partition_drops_its_parked_deadline() {
        // A deleted pid must not come back into the ring at its old deadline.
        let mut d = Derived::default();
        d.set_pending("t", "q", "g", 5, 9_000, 0);
        assert_eq!(d.ring("t", "q", "g").unwrap().deferred_len(), 1);
        d.forget_partition(5);
        assert_eq!(d.tick(100_000), 0, "a deleted partition was promoted");
        assert!(!d.ring("t", "q", "g").unwrap().contains(5));
    }

    #[test]
    fn ensure_ring_reports_the_registrar() {
        let mut d = Derived::default();
        assert!(d.ensure_ring("t", "q", "g"));
        assert!(!d.ensure_ring("t", "q", "g"));
        d.drop_ring("t", "q", "g");
        assert!(d.ensure_ring("t", "q", "g"));
    }
}
