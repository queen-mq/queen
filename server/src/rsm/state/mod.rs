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
//!
//! # Two readers, one truth: `pending.ready_at`
//!
//! The rings have TWO readers, and both reduce to the `pending` keyspace:
//!
//! - **the planner** does not read the live rings at all. It reads the rings of
//!   the groups its batch pops as a rebuild over the committed `pending`
//!   keyspace would build them at the cycle's instant, landing a partition
//!   READY when `ready_at ≤ now` and DEFERRED otherwise — so time-based
//!   promotion (a lease expiring, a `delayed_processing`/`window_buffer`
//!   visibility deadline passing) is FREE for the planner. With the knob off
//!   that is literally [`Derived::rebuild_rings`] every cycle (see
//!   `rsm/batcher.rs`); with KEEP_OVERLAY it is [`PlanRings`], a mirror of the
//!   same rows kept on the planner thread and re-read per landed entry, which
//!   offers the same pids in the same order. The wildcard pop then walks that
//!   ring and re-verifies each candidate against the cursor row.
//! - **the parked long-poll's `has_pending` gate** (§9.5) reads the LIVE ring
//!   on the apply thread ([`Committed::has_claimable_pending`]). The live ring
//!   only moves on events, so the apply loop calls
//!   [`Derived::promote_ring_deadlines`] at each entry boundary (the apply
//!   thread's only wall-time source is the entry stamp, D5) to keep it equal to
//!   a rebuild at that stamp.
//!
//! Because both readers reduce to `pending.ready_at`, ring correctness IS
//! `pending.ready_at` correctness, and the one law is: **`ready_at` must never
//! be LATER than the earliest wall-time the partition could yield a claim** (an
//! under-arm strands claimable work); being earlier is harmless (the claim
//! re-verifies and skips). The event → effect table apply maintains it by:
//!
//! | event | `pending` row (transitions ON) | ring / claimability |
//! |---|---|---|
//! | append, unleased | `ready_at ← min(stored, created+delay)` | ready at the earliest visibility; a late frame never defers an already-ready partition |
//! | append, leased | `ready_at ← max(created+delay, lease_expiry)`, kept only if earlier than stored | armed for AFTER the lease, not offered under it |
//! | pop with backlog left (lease granted) | `ready_at ← lease_expiry` | deferred to lease expiry; rebuild re-arms at expiry |
//! | ack/nack, backlog left, lease released | `ready_at ← now` (overwrite) | ready now |
//! | ack draining the partition | row deleted | leaves the ring |
//! | nack with a retry backoff (future lease) | `ready_at ← lease_expiry` | deferred to the backoff |
//! | seek backwards (010) | `ready_at ← now`, row (re)written | ready now |
//! | lease expiry, no ack | (unchanged: already `lease_expiry`) | promoted when `now ≥ lease_expiry` — by the next rebuild for the planner, by `promote_ring_deadlines` for the live ring |
//! | `delayed_processing`/`window_buffer` deadline | (unchanged: already the visibility time) | promoted the same two ways |
//! | group create | ring ensured (empty) | a place to seed candidates |
//! | group delete (014) | rows swept | ring dropped, its global deadline with it |
//! | partition delete / garbage (§5.2) | rows swept | `forget_partition` leaves every ring and drops its leases |
//!
//! The OFF path (`QUEEN_RAFT_PENDING_TRANSITIONS=0`, still the shipped default
//! this round) OVERWRITES `ready_at` on every append and never floors it at a
//! live lease, so a delayed or leased queue could push an already-claimable
//! partition's `ready_at` into the future (an under-arm) — the correctness
//! reason the transitions path is proven ready to become the default. It is
//! held OFF only because ON moves the replicated `pending` digest, which must
//! flip in lockstep with the shared differential-test config (see
//! `ApplyConfig::pending_transitions`).

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
    /// The earliest parked revisit deadline PER RING, in deadline order — the
    /// global index the apply loop consults to promote due rings in O(due),
    /// never O(rings), and the §9.5 next-re-check clock for a leased or
    /// visibility-deferred partition. It mirrors each ring's
    /// [`ReadyIndex::next_deadline`]; [`Derived::sync_ring_deadline`] keeps it
    /// in step on every mutation. RAM only — it changes no replicated row, so
    /// it is out of the digest (I2) and a node may promote at a different wall
    /// instant than its peer without diverging.
    ring_deadlines: BTreeSet<(i64, RingKey)>,
    /// The deadline each ring currently contributes to `ring_deadlines`.
    ring_deadline_at: BTreeMap<RingKey, i64>,
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

    /// PLAN_RAFT_DRAIN_FIX P4: the planner reads ONLY rings from `Derived`
    /// (never the lease index), and only the rings its wildcard pops walk. So a
    /// plan cycle builds exactly those, each from its own `pending` prefix —
    /// O(pending of the popped groups) instead of O(all pending + all leases)
    /// on every cycle. `None` builds every ring (a discovery pop spans queues).
    pub fn rebuild_rings<R: Reads + ?Sized>(
        reads: &R,
        now_us: i64,
        keys: Option<&[RingKey]>,
    ) -> Result<Derived> {
        let mut d = Derived::default();
        match keys {
            None => {
                reads.scan_pending(&[], usize::MAX, &mut |t, q, g, pid, ready_at| {
                    d.set_pending_inner(t, q, g, pid, ready_at, now_us);
                    d.pending_rows += 1;
                    true
                })?;
            }
            Some(keys) => {
                for (t, q, g) in keys {
                    let prefix = crate::rsm::store::keys::pending_prefix(t, q, g);
                    reads.scan_pending(&prefix, usize::MAX, &mut |tt, qq, gg, pid, ready_at| {
                        if tt != t || qq != q || gg != g {
                            return false; // past this group's rows
                        }
                        d.set_pending_inner(tt, qq, gg, pid, ready_at, now_us);
                        d.pending_rows += 1;
                        true
                    })?;
                }
            }
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
        let key = ring_key(tenant, queue, group);
        let ring = self.rings.entry(key.clone()).or_default();
        if ready_at_us <= now_us {
            ring.push(pid);
        } else {
            ring.defer(ready_at_us, pid);
        }
        self.sync_ring_deadline(&key);
    }

    /// Reconcile one ring's contribution to the global [`Derived::ring_deadlines`]
    /// index with the ring's current earliest parked deadline. O(log rings), and
    /// a no-op for the common case (a ring that has never deferred — every
    /// `delayed_processing`/`window_buffer`-free, unleased partition — carries no
    /// deadline on either side, so `None == None` returns at once).
    fn sync_ring_deadline(&mut self, key: &RingKey) {
        let next = self.rings.get(key).and_then(|r| r.next_deadline());
        let prev = self.ring_deadline_at.get(key).copied();
        if prev == next {
            return;
        }
        if let Some(at) = prev {
            self.ring_deadlines.remove(&(at, key.clone()));
            self.ring_deadline_at.remove(key);
        }
        if let Some(at) = next {
            self.ring_deadlines.insert((at, key.clone()));
            self.ring_deadline_at.insert(key.clone(), at);
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
        let key = ring_key(tenant, queue, group);
        if let Some(ring) = self.rings.get_mut(&key) {
            ring.forget(pid);
        }
        self.sync_ring_deadline(&key);
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
        let key = ring_key(tenant, queue, group);
        self.rings.remove(&key);
        if let Some(at) = self.ring_deadline_at.remove(&key) {
            self.ring_deadlines.remove(&(at, key));
        }
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
        // A partition delete is O(rings) already (it must leave every group's
        // ring); reconcile each touched ring's deadline in the same pass.
        let keys: Vec<RingKey> = self.rings.keys().cloned().collect();
        for key in &keys {
            if let Some(ring) = self.rings.get_mut(key) {
                ring.forget(pid);
            }
        }
        for key in &keys {
            self.sync_ring_deadline(key);
        }
    }

    /// The live lease expiry for `(pid, group)`, if it is leased — the RAM twin
    /// of the cursor row's lease, rebuilt from `leases_by_worker`, so it reads
    /// the same on every node. The append path floors a leased partition's
    /// `pending.ready_at` at this so a new frame does not undercut the pop that
    /// holds the lease (arm the partition for AFTER the lease, §6.1).
    pub fn lease_expiry(&self, pid: Pid, group: &str) -> Option<i64> {
        self.lease_at
            .get(&(pid, group.to_string()))
            .map(|(at, _)| *at)
    }

    /// Promote one ring's deadlines that have passed.
    pub fn promote_due(&mut self, tenant: &str, queue: &str, group: &str, now_us: i64) -> usize {
        let key = ring_key(tenant, queue, group);
        let n = match self.rings.get_mut(&key) {
            Some(ring) => ring.promote_due(now_us),
            None => 0,
        };
        self.sync_ring_deadline(&key);
        n
    }

    /// Promote every ring's due deadlines, in deadline order, touching only the
    /// rings that actually have one due. O(due), not O(rings): the loop drives
    /// this from the apply thread on every entry boundary (against the entry
    /// stamp, D5), guarded by [`Derived::next_ring_deadline`] so a node with
    /// nothing due pays a single comparison. This is the LIVE-ring twin of what a rebuild does for
    /// the planner for free (a rebuild reads `pending.ready_at` and lands the
    /// partition ready or deferred at the rebuild instant); apply runs it at the
    /// end of each entry, keeping the live rings equal to a rebuild at that
    /// boundary, which is what the long-poll `has_pending` gate and §11.5
    /// leadership start rest on.
    pub fn promote_ring_deadlines(&mut self, now_us: i64) -> usize {
        let mut promoted = 0;
        loop {
            let Some(&(at, ref key)) = self.ring_deadlines.iter().next() else {
                break;
            };
            if at > now_us {
                break;
            }
            let key = key.clone();
            if let Some(ring) = self.rings.get_mut(&key) {
                promoted += ring.promote_due(now_us);
            }
            // `promote_due` drains everything at or below `now_us`, so the ring's
            // new earliest deadline is strictly greater (or gone) and this same
            // `key` cannot be the head again — the loop terminates.
            self.sync_ring_deadline(&key);
        }
        promoted
    }

    /// The earliest parked ring deadline across every group, for the loop that
    /// schedules the next re-check and the O(1) promotion guard (§9.5).
    pub fn next_ring_deadline(&self) -> Option<i64> {
        self.ring_deadlines.iter().next().map(|(at, _)| *at)
    }

    /// Promote every ring. O(rings); kept for the digest/reconciliation paths
    /// and tests, where the whole set is walked anyway. Production promotion is
    /// [`Derived::promote_ring_deadlines`], which is O(due).
    pub fn tick(&mut self, now_us: i64) -> usize {
        let keys: Vec<RingKey> = self.rings.keys().cloned().collect();
        let mut n = 0;
        for (t, q, g) in keys {
            n += self.promote_due(&t, &q, &g, now_us);
        }
        n
    }

    /// Whether a group's ring holds a claimable candidate right now — the
    /// coarse gate a parked long-poll re-checks (§9.5, `has_pending`). It is a
    /// SUPERSET of what the claim would grant (an entry is "looked claimable
    /// when last touched"; the claim re-verifies), so it never parks a poll
    /// while claimable work waits, provided the caller has promoted due
    /// deadlines first ([`Derived::promote_ring_deadlines`]) — the same
    /// discipline the apply loop follows before it answers.
    pub fn ring_has_ready(&self, tenant: &str, queue: &str, group: &str) -> bool {
        self.ring(tenant, queue, group)
            .map(|r| r.live_len() > 0)
            .unwrap_or(false)
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
// The planner's kept rings (KEEP_OVERLAY)
// ---------------------------------------------------------------------------

/// The planner's mirror of `pending` for the rings its wildcard pops walk,
/// KEPT across planning cycles on the planner thread instead of being rebuilt
/// from a `pending` scan every cycle ([`Derived::rebuild_rings`]).
///
/// # The same answer as a rebuild
///
/// A rebuild at instant `now` lands each `pending` row of a (tenant, queue,
/// group) READY when `ready_at <= now` and DEFERRED otherwise, pushing the
/// ready ones in `pending` key order — pid order, the pid being the key's
/// big-endian tail — so [`ReadyIndex::walk`] offers exactly the ready pids in
/// ascending order. A [`PlanRing`] holds the same rows (`pid → ready_at`) with
/// the ready ones in a `BTreeSet`, so [`PlanRings::walk`] offers the same pids
/// in the same order, and [`PlanRings::promote`] moves a deferred row to ready
/// once the cycle's `now` reaches it, as the next rebuild would.
///
/// # Kept in step, per landed entry
///
/// Only apply writes `pending`, and only while executing an entry's effects.
/// So the mirror stays equal to the rows by re-reading, when an entry LANDS
/// (its index is at or below the planning read's applied index — apply writes
/// that index last, so every row the entry wrote is visible), exactly the rows
/// its effects can have written ([`PlanRings::advance`]): a cursor write's own
/// row, an append's row in every kept ring of its queue, a partition delete's
/// rows; a group or queue delete, a registration (which arms every predating
/// partition) or a tenant purge drops the rings it touches, to be scanned
/// afresh when next needed. Every effect kind is named in that match, so a new
/// kind is a compile error there rather than a silent drift. Anything it
/// cannot account for — a gap in the landed indexes, a partition row it can no
/// longer find, the applied index going back — drops every ring.
///
/// The store is read LIVE (Phase C), so a rebuild can also see the rows of an
/// entry apply is executing right now, which the mirror only re-reads once
/// that entry lands: the two can differ for rows of entries still in flight.
/// Those entries are in the overlay, and the ring is a hint the claim
/// re-verifies (the module header), so this is at most one cycle of lag,
/// never a stranded partition. With apply quiescent between cycles (the gate
/// test) the mirror equals the rebuild exactly.
#[derive(Debug, Default)]
pub struct PlanRings {
    /// tenant → queue → group → ring. `BTreeMap`s: nothing here may depend on
    /// hash-map order.
    rings: BTreeMap<String, BTreeMap<String, BTreeMap<String, PlanRing>>>,
    /// The instant the rings are promoted to.
    now_us: i64,
    /// Every entry at or below this index is reflected in the rings.
    landed_to: u64,
    /// `pid → (tenant, queue)`, read once from the partition row (a partition
    /// never changes queue, and pids are never reused).
    queue_of: HashMap<Pid, (String, String), crate::rsm::fasthash::FxBuild>,
    /// Rings scanned from `pending` (first use, or again after a drop).
    pub loads: u64,
    /// Times every ring was dropped because an entry could not be accounted
    /// for.
    pub drops: u64,
}

/// One kept ring: the `pending` rows of one (tenant, queue, group).
#[derive(Debug, Default)]
struct PlanRing {
    /// `pid → ready_at`: the rows, exactly.
    at: HashMap<Pid, i64, crate::rsm::fasthash::FxBuild>,
    /// The pids with `ready_at <= now`, in pid order: what the walk offers.
    ready: BTreeSet<Pid>,
    /// `(ready_at, pid)` for the rest, deadline first.
    deferred: BTreeSet<(i64, Pid)>,
    /// The last cycle whose batch walked this ring (idle rings are evicted).
    last_used: u64,
}

impl PlanRing {
    /// Mirror one `pending` row: `Some(ready_at)` written, `None` deleted.
    fn set(&mut self, pid: Pid, at: Option<i64>, now_us: i64) {
        if let Some(old) = self.at.remove(&pid) {
            if !self.ready.remove(&pid) {
                self.deferred.remove(&(old, pid));
            }
        }
        if let Some(at) = at {
            self.at.insert(pid, at);
            if at <= now_us {
                self.ready.insert(pid);
            } else {
                self.deferred.insert((at, pid));
            }
        }
    }

    fn promote(&mut self, now_us: i64) {
        while let Some(&(at, pid)) = self.deferred.first() {
            if at > now_us {
                break;
            }
            self.deferred.pop_first();
            self.ready.insert(pid);
        }
    }

    /// Re-split every row at `now_us` (the clock moved BACK: a promotion is not
    /// undoable incrementally).
    fn resplit(&mut self, now_us: i64) {
        self.ready.clear();
        self.deferred.clear();
        for (&pid, &at) in &self.at {
            if at <= now_us {
                self.ready.insert(pid);
            } else {
                self.deferred.insert((at, pid));
            }
        }
    }
}

impl PlanRings {
    /// An empty mirror that has accounted for every entry up to `landed_to`.
    pub fn new(landed_to: u64, now_us: i64) -> PlanRings {
        PlanRings {
            now_us,
            landed_to,
            ..PlanRings::default()
        }
    }

    /// Drop every ring (and the pid cache): each is scanned afresh on next use.
    pub fn clear(&mut self) {
        self.rings.clear();
        self.queue_of.clear();
        self.drops += 1;
    }

    /// Account for every entry that landed since the last cycle and promote the
    /// rings to `now_us`. `folded` is the batcher's in-flight list (index order,
    /// the index each entry occupies); `applied` is the planning read's applied
    /// index. Every index in `(landed_to, applied]` must be in `folded`: an
    /// entry leaves it only after a cycle has seen it land, so a gap means one
    /// was never accounted for, and every ring is dropped.
    pub fn advance<R: Reads + ?Sized>(
        &mut self,
        reads: &R,
        folded: &[(u64, std::sync::Arc<crate::rsm::entry::Entry>)],
        applied: u64,
        now_us: i64,
    ) -> Result<()> {
        if applied < self.landed_to {
            // The applied index went back (a restore): nothing here holds.
            self.clear();
        } else if applied > self.landed_to && !self.rings.is_empty() {
            let mut next = self.landed_to + 1;
            let mut accounted = true;
            for (index, e) in folded {
                if *index < next {
                    continue;
                }
                if *index > applied || *index != next {
                    break;
                }
                next += 1;
                if !self.land(reads, e)? {
                    accounted = false;
                    break;
                }
            }
            if !accounted || next != applied + 1 {
                self.clear();
            }
        }
        self.landed_to = applied;
        self.promote(now_us);
        Ok(())
    }

    /// Re-read every `pending` row one landed entry's effects can have written,
    /// in the kept rings. `false` when it cannot tell which rows those are.
    fn land<R: Reads + ?Sized>(&mut self, reads: &R, e: &crate::rsm::entry::Entry) -> Result<bool> {
        use crate::rsm::effect::Effect;
        for eff in &e.effects {
            match eff {
                // An append arms its partition for every group of the queue.
                Effect::Append { pid, .. } => {
                    if !self.reread_partition(reads, *pid, None)? {
                        return Ok(false);
                    }
                }
                // A partition delete (and a garbage chunk) only drops rows, so
                // a partition whose row is already gone needs no queue: the
                // rings that hold it are the only ones it can change.
                Effect::PartitionDelete { pid } => {
                    if !self.reread_partition(reads, *pid, None)? {
                        self.reread_where_held(reads, *pid)?;
                    }
                }
                Effect::DeleteChunk { pids, .. } => {
                    for pid in pids {
                        if !self.reread_partition(reads, *pid, None)? {
                            self.reread_where_held(reads, *pid)?;
                        }
                    }
                }
                // A cursor write puts or deletes its own (group, pid) row.
                Effect::CursorSet { pid, group, .. } | Effect::CursorDelete { pid, group } => {
                    if !self.reread_partition(reads, *pid, Some(group))? {
                        return Ok(false);
                    }
                }
                // A registration arms every predating partition, a group
                // delete sweeps the group: scan the ring afresh on next use.
                Effect::GroupUpsert {
                    tenant,
                    queue,
                    group,
                    ..
                }
                | Effect::GroupDelete {
                    tenant,
                    queue,
                    group,
                } => {
                    if let Some(qs) = self.rings.get_mut(tenant) {
                        if let Some(gs) = qs.get_mut(queue) {
                            gs.remove(group);
                        }
                    }
                }
                Effect::QueueDelete { tenant, queue } => {
                    if let Some(qs) = self.rings.get_mut(tenant) {
                        qs.remove(queue);
                    }
                }
                Effect::TenantPurge { tenant } => {
                    self.rings.remove(tenant);
                }
                // Never a `pending` row (apply's arms for these write none).
                Effect::Noop
                | Effect::QueueUpsert { .. }
                | Effect::PartitionCreate { .. }
                | Effect::DlqInsert { .. }
                | Effect::DlqDelete { .. }
                | Effect::Watermark { .. }
                | Effect::KvPut { .. }
                | Effect::KvDelete { .. }
                | Effect::TimerUpsert { .. }
                | Effect::TimerDelete { .. }
                | Effect::TimerBackoff { .. }
                | Effect::StreamsQueryUpsert { .. }
                | Effect::StreamsStatePut { .. }
                | Effect::StreamsStateDelete { .. }
                | Effect::TraceAppend { .. }
                | Effect::TraceExpire { .. }
                | Effect::FlagSet { .. }
                | Effect::QuotaSet { .. }
                | Effect::EphemeralConfigSet { .. }
                | Effect::EphemeralConfigDelete { .. }
                | Effect::GarbageAdd { .. }
                | Effect::RequestIdsExpire { .. }
                | Effect::ClusterVersionSet { .. }
                | Effect::MembershipNote { .. } => {}
            }
        }
        Ok(true)
    }

    /// Re-read `pid`'s rows in the kept rings of its queue — every group's, or
    /// only `group`'s. `false` when the partition's queue cannot be found (its
    /// row is gone before this node ever resolved it).
    fn reread_partition<R: Reads + ?Sized>(
        &mut self,
        reads: &R,
        pid: Pid,
        group: Option<&str>,
    ) -> Result<bool> {
        if !self.queue_of.contains_key(&pid) {
            let Some(row) = reads.partition(pid)? else {
                return Ok(false);
            };
            if self.queue_of.len() >= 1 << 20 {
                self.queue_of.clear();
            }
            self.queue_of.insert(pid, (row.tenant, row.queue));
        }
        let (tenant, queue) = &self.queue_of[&pid];
        let Some(groups) = self.rings.get_mut(tenant).and_then(|qs| qs.get_mut(queue)) else {
            return Ok(true);
        };
        let now = self.now_us;
        match group {
            Some(g) => {
                if let Some(ring) = groups.get_mut(g) {
                    ring.set(pid, reads.pending_at(tenant, queue, g, pid)?, now);
                }
            }
            None => {
                for (g, ring) in groups.iter_mut() {
                    ring.set(pid, reads.pending_at(tenant, queue, g, pid)?, now);
                }
            }
        }
        Ok(true)
    }

    /// Re-read `pid`'s row in every kept ring that holds it (a deletion of a
    /// partition whose queue is no longer readable: it can only remove rows).
    fn reread_where_held<R: Reads + ?Sized>(&mut self, reads: &R, pid: Pid) -> Result<()> {
        let now = self.now_us;
        for (t, qs) in self.rings.iter_mut() {
            for (q, gs) in qs.iter_mut() {
                for (g, ring) in gs.iter_mut() {
                    if ring.at.contains_key(&pid) {
                        ring.set(pid, reads.pending_at(t, q, g, pid)?, now);
                    }
                }
            }
        }
        Ok(())
    }

    /// Move every row whose `ready_at` the clock has reached to ready. A clock
    /// that went back re-splits every ring, as a rebuild at that instant would.
    pub fn promote(&mut self, now_us: i64) {
        let back = now_us < self.now_us;
        for qs in self.rings.values_mut() {
            for gs in qs.values_mut() {
                for ring in gs.values_mut() {
                    if back {
                        ring.resplit(now_us);
                    } else {
                        ring.promote(now_us);
                    }
                }
            }
        }
        self.now_us = now_us;
    }

    /// Make sure the ring of `(tenant, queue, group)` is kept, scanning its
    /// `pending` rows the first time; mark it used by `cycle`.
    pub fn ensure<R: Reads + ?Sized>(
        &mut self,
        reads: &R,
        key: &RingKey,
        cycle: u64,
    ) -> Result<()> {
        let (t, q, g) = key;
        let now = self.now_us;
        let gs = self
            .rings
            .entry(t.clone())
            .or_default()
            .entry(q.clone())
            .or_default();
        if let Some(ring) = gs.get_mut(g) {
            ring.last_used = cycle;
            return Ok(());
        }
        let mut ring = PlanRing {
            last_used: cycle,
            ..PlanRing::default()
        };
        let prefix = crate::rsm::store::keys::pending_prefix(t, q, g);
        reads.scan_pending(&prefix, usize::MAX, &mut |tt, qq, gg, pid, ready_at| {
            if tt != t || qq != q || gg != g {
                return false; // past this group's rows
            }
            ring.set(pid, Some(ready_at), now);
            true
        })?;
        gs.insert(g.clone(), ring);
        self.loads += 1;
        Ok(())
    }

    /// Forget every ring no batch has walked since `before`.
    pub fn evict_idle(&mut self, before: u64) {
        for qs in self.rings.values_mut() {
            for gs in qs.values_mut() {
                gs.retain(|_, ring| ring.last_used >= before);
            }
            qs.retain(|_, gs| !gs.is_empty());
        }
        self.rings.retain(|_, qs| !qs.is_empty());
    }

    /// The kept ring keys, in key order.
    pub fn keys(&self) -> Vec<RingKey> {
        let mut out = Vec::new();
        for (t, qs) in &self.rings {
            for (q, gs) in qs {
                for g in gs.keys() {
                    out.push((t.clone(), q.clone(), g.clone()));
                }
            }
        }
        out
    }

    fn ring(&self, tenant: &str, queue: &str, group: &str) -> Option<&PlanRing> {
        self.rings.get(tenant)?.get(queue)?.get(group)
    }

    /// Whether the ring of `(tenant, queue, group)` is kept.
    pub fn has(&self, tenant: &str, queue: &str, group: &str) -> bool {
        self.ring(tenant, queue, group).is_some()
    }

    /// The ready pids, in pid order — the same walk, with the same `limit`
    /// semantics, as [`ReadyIndex::walk`] over a fresh rebuild.
    pub fn walk(
        &self,
        tenant: &str,
        queue: &str,
        group: &str,
        limit: usize,
        cb: &mut dyn FnMut(Pid) -> bool,
    ) -> usize {
        let Some(ring) = self.ring(tenant, queue, group) else {
            return 0;
        };
        let mut n = 0;
        for pid in ring.ready.iter() {
            n += 1;
            if !cb(*pid) || n >= limit {
                break;
            }
        }
        n
    }

    /// `(ready in walk order, deferred count, next deadline, rows)` of one kept
    /// ring — for the equivalence check against a rebuild.
    pub fn snapshot(&self, tenant: &str, queue: &str, group: &str) -> Option<RingSnapshot> {
        let ring = self.ring(tenant, queue, group)?;
        let mut rows: Vec<(Pid, i64)> = ring.at.iter().map(|(p, a)| (*p, *a)).collect();
        rows.sort_unstable();
        Some(RingSnapshot {
            ready: ring.ready.iter().copied().collect(),
            deferred: ring.deferred.len(),
            next_deadline: ring.deferred.first().map(|(at, _)| *at),
            rows,
        })
    }
}

/// One ring as the planner can observe it (the equivalence check).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RingSnapshot {
    /// The ready pids in walk order.
    pub ready: Vec<Pid>,
    pub deferred: usize,
    pub next_deadline: Option<i64>,
    /// The `pending` rows mirrored, pid order (empty when not observable).
    pub rows: Vec<(Pid, i64)>,
}

impl RingSnapshot {
    /// The same view of a ring a rebuild produced (`rows` left empty: a
    /// [`ReadyIndex`] does not keep them).
    pub fn of_rebuild(ring: Option<&ReadyIndex>) -> RingSnapshot {
        let mut ready = Vec::new();
        if let Some(r) = ring {
            r.walk(usize::MAX, &mut |pid| {
                ready.push(pid);
                true
            });
        }
        RingSnapshot {
            ready,
            deferred: ring.map_or(0, |r| r.deferred_len()),
            next_deadline: ring.and_then(|r| r.next_deadline()),
            rows: Vec::new(),
        }
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
    /// KEEP_OVERLAY: the planner thread's kept rings. When `Some`, the
    /// candidate walk reads them instead of `derived`'s rings.
    plan_rings: Option<&'a PlanRings>,
}

impl<'a, R: Reads + ?Sized> Committed<'a, R> {
    pub fn new(reads: &'a R, derived: &'a Derived) -> Committed<'a, R> {
        Committed {
            reads,
            derived,
            plan_rings: None,
        }
    }

    /// A view whose candidate walk reads the planner's kept rings (KEEP_OVERLAY)
    /// — every ring a wildcard pop of this cycle walks must be kept in them.
    pub fn with_plan_rings(
        reads: &'a R,
        derived: &'a Derived,
        rings: &'a PlanRings,
    ) -> Committed<'a, R> {
        Committed {
            reads,
            derived,
            plan_rings: Some(rings),
        }
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
        if let Some(rings) = self.plan_rings {
            debug_assert!(
                rings.has(tenant, queue, group),
                "a walked ring is not kept: the batcher ensures every ring of the batch"
            );
            return rings.walk(tenant, queue, group, limit, cb);
        }
        match self.derived.ring(tenant, queue, group) {
            Some(r) => r.walk(limit, cb),
            None => 0,
        }
    }

    /// Whether the group has a partition that looked claimable when the ring
    /// was last touched — the read a parked long-poll's `has_pending` gate
    /// makes (§9.5). It shares the candidate walk's ring and the same coarse
    /// contract, so a gate answered from here is CONSISTENT with what the pop
    /// walk would then offer: never `false` while the ring holds a ready
    /// candidate the pop would try. The caller (the apply thread) promotes due
    /// deadlines before reading, so a lease that has expired reads `true` here.
    pub fn has_claimable_pending(&self, tenant: &str, queue: &str, group: &str) -> bool {
        self.derived.ring_has_ready(tenant, queue, group)
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

    #[test]
    fn the_global_ring_deadline_index_tracks_the_earliest_across_groups() {
        let mut d = Derived::default();
        // Three groups, one deferred partition each, at different deadlines.
        d.set_pending("t", "q", "g1", 1, 900, 0);
        d.set_pending("t", "q", "g2", 2, 400, 0);
        d.set_pending("t", "q", "g3", 3, 700, 0);
        assert_eq!(
            d.next_ring_deadline(),
            Some(400),
            "the earliest across rings"
        );
        // Promote only what is due: at now=500 only g2's 400 fires.
        assert_eq!(d.promote_ring_deadlines(500), 1);
        assert!(d.ring("t", "q", "g2").unwrap().contains(2));
        assert!(!d.ring("t", "q", "g1").unwrap().contains(1));
        assert!(!d.ring("t", "q", "g3").unwrap().contains(3));
        assert_eq!(d.next_ring_deadline(), Some(700), "g2 left the index");
        // A ready partition carries no deadline, so the index tracks only the
        // remaining two.
        assert_eq!(d.promote_ring_deadlines(10_000), 2);
        assert_eq!(d.next_ring_deadline(), None, "nothing parked");
    }

    #[test]
    fn promote_ring_deadlines_equals_a_full_tick_over_any_mixture() {
        // The O(due) promotion must land exactly where the O(rings) `tick`
        // would, after any sequence of defers, pushes, clears and forgets.
        let mut a = Derived::default();
        let groups = ["g0", "g1", "g2"];
        for round in 0..300u64 {
            let g = groups[(round % 3) as usize];
            let pid = round % 11;
            match round % 5 {
                0 => a.set_pending("t", "q", g, pid, round as i64 + 1, round as i64),
                1 => a.set_pending("t", "q", g, pid, round as i64 + 50, round as i64),
                2 => a.clear_pending("t", "q", g, pid),
                3 => a.forget_partition(pid),
                _ => {
                    a.promote_ring_deadlines(round as i64);
                }
            }
            // A clone driven by the whole-set `tick` must match the incremental
            // index at the same instant, membership and parked count alike.
            let mut b = a.clone();
            let now = round as i64 + 25;
            let via_index = a.promote_ring_deadlines(now);
            let via_tick = b.tick(now);
            for g in groups {
                let (ra, rb) = (a.ring("t", "q", g), b.ring("t", "q", g));
                let ready = |r: Option<&ReadyIndex>| {
                    let mut v = Vec::new();
                    if let Some(r) = r {
                        r.walk(usize::MAX, &mut |p| {
                            v.push(p);
                            true
                        });
                    }
                    v.sort_unstable();
                    v
                };
                assert_eq!(ready(ra), ready(rb), "round {round} group {g}: ready set");
                assert_eq!(
                    ra.map(|r| r.deferred_len()),
                    rb.map(|r| r.deferred_len()),
                    "round {round} group {g}: parked count",
                );
            }
            assert_eq!(
                a.next_ring_deadline(),
                b.next_ring_deadline(),
                "round {round}: the index and a full tick disagree on the next deadline",
            );
            // The index count and the tick count are both "how many promoted".
            let _ = (via_index, via_tick);
        }
    }

    #[test]
    fn lease_expiry_reads_the_noted_lease() {
        let mut d = Derived::default();
        assert_eq!(d.lease_expiry(1, "g"), None);
        d.note_lease("w1", 1, "g", 5_000);
        assert_eq!(d.lease_expiry(1, "g"), Some(5_000));
        d.note_lease("w1", 1, "g", 9_000); // renew moves it
        assert_eq!(d.lease_expiry(1, "g"), Some(9_000));
        d.clear_lease(1, "g");
        assert_eq!(d.lease_expiry(1, "g"), None);
    }

    #[test]
    fn ring_has_ready_is_a_coarse_gate_that_promotion_clears() {
        // A deferred (leased) partition reads NOT ready until its deadline is
        // promoted — the long-poll gate the facade `has_pending` will call, and
        // the discipline is: promote, then read.
        let mut d = Derived::default();
        d.set_pending("t", "q", "g", 1, 5_000, 0); // deferred to 5000
        assert!(
            !d.ring_has_ready("t", "q", "g"),
            "leased/deferred: not ready"
        );
        d.promote_ring_deadlines(4_000); // not yet due
        assert!(!d.ring_has_ready("t", "q", "g"));
        d.promote_ring_deadlines(5_000); // due now
        assert!(d.ring_has_ready("t", "q", "g"), "promoted at its deadline");
    }
}
