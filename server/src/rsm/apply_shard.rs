//! Sharded apply (`QUEEN_RAFT_APPLY_SHARDS`, [`super::ApplyConfig::apply_shards`]).
//!
//! One apply thread executed every effect of every entry, and it was the stage
//! that clocked the pipeline (entries/s × apply µs ≈ 1 in 24 of 26 measured
//! load shapes, 2026-09-30). The target shape — 1-20 messages per push, each on
//! its own partition, a million partitions and more — needs about three
//! million apply events a second, which one thread at 18-65 µs an event does
//! not have. This module is the split.
//!
//! # Why a split apply is still apply (I1, I2)
//!
//! An effect is PID-KEYED ([`pid_keyed`]) when every row it reads or writes is
//! keyed by its partition id — the partition row, its cursors, its `seg_loc`,
//! `txns` and `dedup` rows, its `dlq_by_pos` lists — or by names together with
//! that pid (`pending (tenant, queue, group, pid)`, `leases_by_worker (worker,
//! pid, group)`, `queue_partitions (tenant, queue, pid)`, `streams_state
//! (query, pid, key)`), and the only shared rows it READS are catalogue rows
//! (`queues`, `groups`, `garbage`) that no pid-keyed effect writes. Two such
//! effects on different pids therefore touch disjoint rows and commute: any
//! interleaving leaves the same bytes. Effects on ONE pid go to one shard
//! (`pid % shards`) and run there in entry order.
//!
//! The apply thread (the coordinator) walks the entry in order. A pid-keyed
//! effect joins the current RUN ([`Run`]); anything else ends it: the run
//! executes, every shard its own effects in entry order, and only then does
//! the other effect run, on the coordinator, with no shard active. So every
//! global effect sees exactly the state the one-thread order gives it, and
//! every pid-keyed effect sees exactly its own pid's history plus a catalogue
//! nothing in the run can change. That is the whole argument; the rest of this
//! module is the bookkeeping that keeps it true:
//!
//! - Two pid-keyed kinds can name a row two pids share, and stay out of a run
//!   when they could ([`Run::admit`]): a `PartitionCreate` writes the name
//!   index `partitions_by_key (tenant, queue, partition)` — a second create of
//!   the same name in one run ends the run — and a `DlqInsert` whose id is
//!   already in use overwrites ANOTHER pid's dead letter (a planner bug the
//!   single-threaded path tolerates), so it only joins a run when its id is
//!   fresh. `PartitionDelete` and `DlqDelete` are global: the first frees a
//!   name a create in the same run may take, the second names its pid only
//!   through the row it deletes.
//! - COUNTERS. Each shard folds its bumps into its own overlay
//!   ([`super::CounterCache`]); a read or a sweep on the coordinator consults
//!   every overlay and a commit flushes them all. Additive counters fold by
//!   sum and stamps by max, so the committed value is the single overlay's —
//!   the argument that made the overlay transparent in the first place.
//! - NODE-LOCAL side effects: segment appends and releases, the apply-owned
//!   qlog buffer, the D17 journal. Their ORDER is in the bytes they leave, so
//!   a shard never makes them ([`Side::Deferred`]): the coordinator writes a
//!   run's frames BEFORE it in entry order (a pre-pass), and applies the
//!   releases and journal records the shards kept AFTER it, in entry order
//!   again. On the live path (the queue logs written by the log writer, D8's
//!   `qlog_writer_external`) there are no frames and no releases at all.
//! - WAKES. A shard counts them per (tenant, queue, group); the coordinator
//!   merges the counts and emits the sequence one thread would have emitted
//!   ([`super::Applier`]'s `emit_wakes`).
//! - REFUSALS. A shard stops at its first refusal; the coordinator reports the
//!   LOWEST failing ordinal, which is the one a single thread would have
//!   stopped at (a failure on one pid cannot depend on another pid's effects),
//!   and a shard stops early once a lower ordinal has failed.
//!
//! One shard (`QUEEN_RAFT_APPLY_SHARDS=1`) is today's path: no run is built,
//! every effect executes in entry order on the apply thread with its side
//! effects inline ([`Side::Inline`]). So is an entry with fewer pid-keyed
//! effects than `QUEEN_RAFT_APPLY_SHARD_MIN`, where a hand-off would cost more
//! than it saves.
//!
//! # Threads
//!
//! A run with more than one busy shard goes to the [`Pool`]: `shards - 1`
//! persistent threads, each lent one shard's state and a store SHARD WRITER
//! (`Store::shard_writer`, a handle that writes the RAM tables alongside the
//! write handle, for keys no other writer touches) for the length of the run;
//! the apply thread runs the last busy shard itself on the write handle, then
//! waits for the others (a latch, spin-then-sleep). Every shard writer is
//! dropped before the run returns, so the store never commits or cuts a
//! checkpoint with one alive. The same pool flushes the partition-scope half
//! of the counter overlays at a commit (each shard its own pids' keys); the
//! shared keys — a queue's or a tenant's counters, which several shards bump —
//! are only ever written by the apply thread, which is what the shard writers'
//! read-modify-writes require.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread::JoinHandle;

use super::{
    partition_scoped, ApplyConfig, ApplyError, ApplyStats, CounterCache, LeaseIndex, Result,
    DLQ_TRIM_PER_WATERMARK,
};
use crate::rsm::dedup;
use crate::rsm::effect::{CursorRow, Effect, Pid};
use crate::rsm::fasthash::{FxBuild, FxHasher};
use crate::rsm::local_metrics::LocalMetrics;
use crate::rsm::qlog::set::QLogSet;
use crate::rsm::segments::{Position, Release, Segments};
use crate::rsm::store::keys::{self, Counter, CounterScope};
use crate::rsm::store::rows::{self, DlqRow, PartitionRow, SegLocRow};
use crate::rsm::store::{Keyspace, Reads, StoreError, TypedReads, TypedWrites, Writes};

// ---------------------------------------------------------------------------
// Classification
// ---------------------------------------------------------------------------

/// The partition a PID-KEYED effect belongs to (module header), `None` for a
/// global one. Only the kinds whose every row is keyed by the pid are here;
/// [`Run::admit`] adds the two conditions `PartitionCreate` and `DlqInsert`
/// carry.
pub(super) fn pid_keyed(e: &Effect) -> Option<Pid> {
    match e {
        Effect::Append { pid, .. }
        | Effect::CursorSet { pid, .. }
        | Effect::CursorDelete { pid, .. }
        | Effect::Watermark { pid, .. }
        | Effect::DlqInsert { pid, .. }
        | Effect::PartitionCreate { pid, .. }
        | Effect::StreamsStatePut { pid, .. }
        | Effect::StreamsStateDelete { pid, .. } => Some(*pid),
        _ => None,
    }
}

/// The shard of a pid.
#[inline]
pub(super) fn shard_of(pid: Pid, shards: usize) -> usize {
    (pid % shards as u64) as usize
}

/// The pid-keyed effects of an entry waiting for the next global effect (or
/// the end of the entry), split by shard. Reused from run to run.
pub(super) struct Run {
    /// Per shard, the ordinals of its effects, in entry order.
    pub(super) ords: Vec<Vec<u32>>,
    /// Every ordinal of the run, in entry order (the coordinator's pre-pass).
    pub(super) all: Vec<u32>,
    /// The `(tenant, queue, partition)` names this run creates, hashed. A
    /// collision only ends a run early.
    names: HashSet<u64, FxBuild>,
    /// The dead-letter ids this run inserts.
    dlq_ids: HashSet<[u8; 16], FxBuild>,
}

impl Run {
    pub(super) fn new(shards: usize) -> Run {
        Run {
            ords: (0..shards).map(|_| Vec::new()).collect(),
            all: Vec::new(),
            names: HashSet::default(),
            dlq_ids: HashSet::default(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.all.is_empty()
    }

    pub(super) fn clear(&mut self) {
        for o in &mut self.ords {
            o.clear();
        }
        self.all.clear();
        self.names.clear();
        self.dlq_ids.clear();
    }

    /// Take effect `ord` into the run if it is pid-keyed and cannot meet a
    /// row of another pid inside it. `false`: the caller ends the run and
    /// executes `e` on its own. `reads` sees the state before the run (none
    /// of its effects has executed yet), which is all [`Run::admit`] asks it.
    pub(super) fn admit<R: Reads + ?Sized>(
        &mut self,
        reads: &R,
        ord: u32,
        e: &Effect,
    ) -> Result<bool> {
        let Some(pid) = pid_keyed(e) else {
            return Ok(false);
        };
        match e {
            Effect::PartitionCreate {
                tenant,
                queue,
                partition,
                ..
            } => {
                let mut h = FxHasher::default();
                tenant.hash(&mut h);
                queue.hash(&mut h);
                partition.hash(&mut h);
                if !self.names.insert(h.finish()) {
                    return Ok(false);
                }
            }
            Effect::DlqInsert {
                dlq_id,
                tenant,
                queue,
                ..
            } => {
                // A fresh id names a row nothing in this run can reach: only a
                // `DlqInsert` creates dead-letter rows, and the other paths
                // that delete them find them through their OWN pid's
                // `dlq_by_pos` list. An id already in the store, or already
                // inserted by this run, may belong to another pid.
                if self.dlq_ids.contains(dlq_id)
                    || reads
                        .get_raw(Keyspace::Dlq, &keys::dlq(tenant, queue, dlq_id))?
                        .is_some()
                {
                    return Ok(false);
                }
                self.dlq_ids.insert(*dlq_id);
            }
            _ => {}
        }
        let s = shard_of(pid, self.ords.len());
        self.ords[s].push(ord);
        self.all.push(ord);
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// What a pid-keyed effect is executed with
// ---------------------------------------------------------------------------

/// The entry an effect belongs to, read-only for every shard.
pub(super) struct Cx<'a> {
    pub(super) cfg: &'a ApplyConfig,
    pub(super) index: u64,
    pub(super) now_us: i64,
    /// [`super::Notify::wants_appended`], read once per entry.
    pub(super) wants_appended: bool,
    /// [`super::Notify::wants_append_wakes`], read once per entry.
    pub(super) wants_append_wakes: bool,
    /// The segment positions the coordinator's pre-pass gave this run's
    /// `Append`s, by effect ordinal (empty on the inline path).
    pub(super) positions: &'a [Option<Position>],
}

/// Where a pid-keyed effect's NODE-LOCAL side effects go (module header).
pub(super) enum Side<'a> {
    /// One shard, or the coordinator: straight into the segment writer, the
    /// apply-owned qlog and the journal, exactly as the one-thread path did.
    Inline {
        segments: &'a mut Segments,
        qlog: Option<&'a mut QLogSet>,
        metrics: &'a LocalMetrics,
    },
    /// A shard in a run: the frames were written by the pre-pass
    /// ([`Cx::positions`]); releases and journal records are kept in the
    /// shard, by ordinal, for the coordinator to apply after the run.
    Deferred,
}

/// A retention record a shard keeps for the coordinator
/// ([`LocalMetrics::record_retention`]'s arguments).
pub(super) struct RetentionRec {
    pub(super) tenant: String,
    pub(super) queue: String,
    pub(super) pid: Pid,
    pub(super) log_from: u64,
    pub(super) log_to: u64,
    pub(super) txns_from: u64,
    pub(super) txns_to: u64,
}

// ---------------------------------------------------------------------------
// The per-queue cache (B41)
// ---------------------------------------------------------------------------

/// What the append path needs of a queue, cached per shard ACROSS commits:
/// its registered groups and its configured delay. Both are catalogue rows no
/// pid-keyed effect writes, and every apply-side write to them (a queue or
/// group upsert or delete, a tenant purge) invalidates the entry
/// ([`QueueCache::invalidate`]); apply is the only writer of the store (I1),
/// so the cache answers exactly what a read would (I2). The group list used
/// to be re-read after every commit and the queue row decoded on every append.
///
/// The same slot counts the entry's WAKES for the queue, by group, so an
/// event costs an increment instead of three `String` clones and a sort.
pub(super) struct QueueCtx {
    pub(super) tenant: Arc<str>,
    pub(super) queue: Arc<str>,
    /// The registered groups in key order (what `scan_groups` gives), `None`
    /// until read and after a change to the queue's group set.
    groups: Option<Arc<[Arc<str>]>>,
    /// `max(delayed_processing, window_buffer)` in µs (0 without a queue
    /// row), `None` until read and after a queue upsert or delete.
    delay_us: Option<i64>,
    /// This entry's APPEND wakes, parallel to `groups` (valid only for the
    /// list they were counted against; frozen into `named` when it goes).
    by_index: Vec<u32>,
    /// This entry's other group wakes: `(group, from appends, from lease
    /// releases)`.
    named: Vec<(Arc<str>, u32, u32)>,
    /// A queue-wide (`group = None`) wake this entry.
    all: bool,
    /// On the cache's touched list this entry.
    touched: bool,
}

impl QueueCtx {
    fn new(tenant: &str, queue: &str) -> QueueCtx {
        QueueCtx {
            tenant: Arc::from(tenant),
            queue: Arc::from(queue),
            groups: None,
            delay_us: None,
            by_index: Vec::new(),
            named: Vec::new(),
            all: false,
            touched: false,
        }
    }

    /// Move the index-counted wakes under their names: the list they index
    /// is about to change.
    fn freeze(&mut self) {
        if self.by_index.iter().all(|n| *n == 0) {
            self.by_index.clear();
            return;
        }
        if let Some(groups) = self.groups.clone() {
            for (g, n) in groups.iter().zip(std::mem::take(&mut self.by_index)) {
                if n > 0 {
                    self.named_slot(g).1 += n;
                }
            }
        }
        self.by_index.clear();
    }

    fn named_slot(&mut self, g: &str) -> &mut (Arc<str>, u32, u32) {
        let i = match self.named.iter().position(|(n, _, _)| &**n == g) {
            Some(i) => i,
            None => {
                self.named.push((Arc::from(g), 0, 0));
                self.named.len() - 1
            }
        };
        &mut self.named[i]
    }
}

/// The per-shard queue slots ([`QueueCtx`]), keyed by the escaped
/// `(tenant, queue)` bytes so a lookup builds its key in a scratch buffer.
#[derive(Default)]
pub(super) struct QueueCache {
    index: HashMap<Box<[u8]>, u32, FxBuild>,
    slots: Vec<QueueCtx>,
    /// Slots with wakes this entry.
    touched: Vec<u32>,
    key: Vec<u8>,
}

/// Slots a shard keeps before it starts over (at a commit, between entries).
/// A slot is ~150 B plus its group names, so this bounds the cache at a few
/// MiB per shard however many queues come and go.
const QUEUE_CACHE_MAX: usize = 16_384;

impl QueueCache {
    fn key_of(&mut self, tenant: &str, queue: &str) {
        self.key.clear();
        keys::push_name(&mut self.key, tenant);
        keys::push_name(&mut self.key, queue);
    }

    /// The slot of `(tenant, queue)`, made on first use.
    pub(super) fn slot(&mut self, tenant: &str, queue: &str) -> u32 {
        self.key_of(tenant, queue);
        if let Some(&i) = self.index.get(self.key.as_slice()) {
            return i;
        }
        let i = self.slots.len() as u32;
        self.slots.push(QueueCtx::new(tenant, queue));
        self.index.insert(Box::from(self.key.as_slice()), i);
        i
    }

    /// Forget what was read for `(tenant, queue)`: its group set or its
    /// configuration changed. The entry's wake counts stay.
    pub(super) fn invalidate(&mut self, tenant: &str, queue: &str) {
        self.key_of(tenant, queue);
        if let Some(&i) = self.index.get(self.key.as_slice()) {
            let q = &mut self.slots[i as usize];
            q.freeze();
            q.groups = None;
            q.delay_us = None;
        }
    }

    /// Start over when the cache has grown past its bound. Only between
    /// entries (no wake is pending then).
    pub(super) fn trim(&mut self) {
        if self.slots.len() > QUEUE_CACHE_MAX && self.touched.is_empty() {
            self.index.clear();
            self.slots.clear();
        }
    }

    fn touch(&mut self, slot: u32) {
        let q = &mut self.slots[slot as usize];
        if !q.touched {
            q.touched = true;
            self.touched.push(slot);
        }
    }

    /// One append wake for group `gi` of the slot's current group list.
    fn wake_index(&mut self, slot: u32, gi: usize) {
        self.touch(slot);
        let q = &mut self.slots[slot as usize];
        let len = q.groups.as_ref().map_or(0, |g| g.len());
        if q.by_index.len() < len {
            q.by_index.resize(len, 0);
        }
        q.by_index[gi] += 1;
    }

    /// One wake for a group named by an effect (a released lease).
    fn wake_release(&mut self, slot: u32, group: &str) {
        self.touch(slot);
        self.slots[slot as usize].named_slot(group).2 += 1;
    }

    /// The queue-wide wake (deduplicated per entry by construction).
    fn wake_all(&mut self, slot: u32) {
        self.touch(slot);
        self.slots[slot as usize].all = true;
    }

    /// Every wake this shard counted in the entry, as `(tenant, queue, group,
    /// from appends, from releases)`; `group = None` is the queue-wide one.
    pub(super) fn wakes<'s>(
        &'s self,
        out: &mut Vec<(&'s str, &'s str, Option<&'s str>, u32, u32)>,
    ) {
        for &i in &self.touched {
            let q = &self.slots[i as usize];
            if q.all {
                out.push((&q.tenant, &q.queue, None, 0, 0));
            }
            if let Some(groups) = &q.groups {
                for (g, &n) in groups.iter().zip(q.by_index.iter()) {
                    if n > 0 {
                        out.push((&q.tenant, &q.queue, Some(&**g), n, 0));
                    }
                }
            }
            for (g, a, r) in &q.named {
                if *a > 0 || *r > 0 {
                    out.push((&q.tenant, &q.queue, Some(&**g), *a, *r));
                }
            }
        }
    }

    /// The entry's wakes are out: reset the counts.
    pub(super) fn clear_wakes(&mut self) {
        for i in std::mem::take(&mut self.touched) {
            let q = &mut self.slots[i as usize];
            q.touched = false;
            q.all = false;
            q.by_index.iter_mut().for_each(|n| *n = 0);
            q.named.clear();
        }
    }
}

// ---------------------------------------------------------------------------
// One shard
// ---------------------------------------------------------------------------

/// Everything a shard owns: its counter overlay, its partitions' leases, its
/// queue cache, what it counted for the entry, and the node-local side
/// effects it keeps for the coordinator. The coordinator owns every `Shard`
/// and lends one to a thread for the length of a run.
pub(super) struct Shard {
    /// The counter overlay of this shard's bumps (module header).
    pub(super) ctr: CounterCache,
    /// The live lease of each leased (partition, group) of this shard's pids
    /// (the append path floors a leased partition's `pending.ready_at` at it).
    pub(super) leases: LeaseIndex,
    pub(super) queues: QueueCache,
    /// `(tenant, queue, partition)` of this entry's appends, for
    /// [`super::Notify::appended`], collected only while someone listens.
    pub(super) appended: Vec<(String, String, String)>,
    /// Cumulative counts of what this shard executed (summed into
    /// [`super::Applier::stats`]).
    pub(super) stats: ApplyStats,
    /// The highest `created_at` this shard's appends wrote (§7.4); the
    /// coordinator takes the maximum over shards.
    pub(super) max_created_at_us: i64,
    /// Deferred segment releases of the current run, by ordinal.
    pub(super) releases: Vec<(u32, Position, Release)>,
    /// Deferred retention records of the current run, by ordinal.
    pub(super) retention: Vec<(u32, RetentionRec)>,
    /// Ordinals of the current run's `PartitionCreate`s (the churn journal).
    pub(super) churn: Vec<u32>,
    /// The first refusal of the current run: `(ordinal, error)`.
    pub(super) failed: Option<(u32, ApplyError)>,
    /// Scratch: a counter key.
    key: Vec<u8>,
    /// Scratch: an append's `(hash, offset)` list for the dedup record.
    accepted: Vec<([u8; 16], u64)>,
    /// Scratch: one `request_ids` row, when the shard records outcomes.
    pub(super) row: Vec<u8>,
}

impl Shard {
    pub(super) fn new(batch_counters: bool, max_created_at_us: i64) -> Shard {
        Shard {
            ctr: CounterCache::new(batch_counters),
            leases: LeaseIndex::default(),
            queues: QueueCache::default(),
            appended: Vec::new(),
            stats: ApplyStats::default(),
            max_created_at_us,
            releases: Vec::new(),
            retention: Vec::new(),
            churn: Vec::new(),
            failed: None,
            key: Vec::with_capacity(64),
            accepted: Vec::new(),
            row: Vec::new(),
        }
    }

    /// Execute this shard's effects of a run, in entry order, with its
    /// side effects deferred. Stops at its first refusal, and at any ordinal
    /// at or past the lowest one refused so far (`first_fail`, the pre-pass
    /// included): that entry is lost anyway, and the report names the lowest
    /// ordinal.
    pub(super) fn run<W: Writes + ?Sized>(
        &mut self,
        w: &mut W,
        cx: &Cx<'_>,
        effects: &[Effect],
        ords: &[u32],
        first_fail: &AtomicU32,
    ) {
        let mut side = Side::Deferred;
        let last = effects.len().saturating_sub(1) as u32;
        for &ord in ords {
            if ord >= first_fail.load(Ordering::Relaxed) {
                break;
            }
            if let Err(e) = self.effect(w, cx, &mut side, ord, &effects[ord as usize]) {
                first_fail.fetch_min(ord, Ordering::Relaxed);
                self.failed = Some((ord, e));
                break;
            }
            // §13.5 `apply.mid_entry`, once per effect that is not the entry's
            // last, wherever it executes: the crash matrix arms it by count,
            // and the count is the one-thread path's. A kill here leaves
            // other shards' effects half done, which is still "some effects
            // written, nothing committed" — the entry replays whole (I11).
            if ord < last {
                crate::rsm::faults::hit("apply.mid_entry");
                #[cfg(test)]
                crate::rsm::faults::mid_entry_hook();
            }
        }
    }

    /// One pid-keyed effect. `ord` is its ordinal in the entry.
    pub(super) fn effect<W: Writes + ?Sized>(
        &mut self,
        w: &mut W,
        cx: &Cx<'_>,
        side: &mut Side<'_>,
        ord: u32,
        e: &Effect,
    ) -> Result<()> {
        match e {
            Effect::Append {
                pid,
                bucket,
                base_offset,
                count,
                created_at_us,
                hashes,
                blob,
            } => self.append(
                w,
                cx,
                side,
                ord,
                AppendArgs {
                    pid: *pid,
                    bucket: *bucket,
                    base_offset: *base_offset,
                    count: *count,
                    created_at_us: *created_at_us,
                    hashes,
                    blob,
                },
            ),
            Effect::CursorSet { pid, group, row } => self.cursor_set(w, cx, *pid, group, row),
            Effect::CursorDelete { pid, group } => self.cursor_delete(w, *pid, group),
            Effect::Watermark {
                pid,
                log_start,
                txns_start,
            } => self.watermark(w, cx, side, ord, *pid, *log_start, *txns_start),
            Effect::DlqInsert {
                dlq_id,
                tenant,
                queue,
                pid,
                group,
                offset,
                message_id,
                txn,
                payload,
                error,
                retry_count,
                failed_at_us,
            } => {
                let row = DlqRow {
                    pid: *pid,
                    group: group.clone(),
                    offset: *offset,
                    message_id: *message_id,
                    txn: txn.clone(),
                    payload: payload.clone(),
                    error: error.clone(),
                    retry_count: *retry_count,
                    failed_at_us: *failed_at_us,
                };
                // The row is keyed by id and the second index by position, so
                // an id written twice would leave the FIRST position pointing
                // at a row that now names another one. Reusing an id is a
                // planner bug; leaving a dangling index behind would be this
                // node's. (A run only takes a fresh id, [`Run::admit`], so on
                // a shard this finds nothing.)
                if let Some(old) = w.dlq(tenant, queue, dlq_id)? {
                    w.del_dlq(tenant, queue, dlq_id, &old)?;
                    settle_dlq_count(w, &mut self.ctr, &mut self.key, old.pid, tenant, queue, 1)?;
                }
                w.put_dlq(tenant, queue, dlq_id, &row)?;
                self.bump(w, *pid, tenant, queue, None, Counter::DlqCount, 1)?;
                Ok(())
            }
            Effect::PartitionCreate {
                pid,
                uuid,
                tenant,
                queue,
                partition,
                created_at_us,
            } => {
                if w.partition(*pid)?.is_some() {
                    return Err(ApplyError::Inconsistent {
                        what: "PartitionCreate",
                        detail: format!("pid {pid} already exists"),
                    });
                }
                let row = PartitionRow::new(*uuid, tenant, queue, partition, *created_at_us);
                w.create_partition(*pid, &row)?;
                // The dashboard's partition churn (node-local, D17).
                match side {
                    Side::Inline { metrics, .. } => {
                        metrics.record_churn(cx.now_us, tenant, queue, 1, 0)
                    }
                    Side::Deferred => self.churn.push(ord),
                }
                Ok(())
            }
            Effect::StreamsStatePut {
                query_id,
                pid,
                key,
                value,
                updated_at_us,
            } => {
                w.put_streams_state(
                    query_id,
                    *pid,
                    key,
                    &rows::StreamsStateRow {
                        value: value.clone(),
                        updated_at_us: *updated_at_us,
                    },
                )?;
                Ok(())
            }
            Effect::StreamsStateDelete { query_id, pid, key } => {
                if !w.del_streams_state(query_id, *pid, key)? {
                    self.stats.missing_rows += 1;
                }
                Ok(())
            }
            other => Err(ApplyError::Inconsistent {
                what: "sharded apply",
                detail: format!("{} is not a pid-keyed effect", other.kind().name()),
            }),
        }
    }

    // -- append ------------------------------------------------------------

    fn append<W: Writes + ?Sized>(
        &mut self,
        w: &mut W,
        cx: &Cx<'_>,
        side: &mut Side<'_>,
        ord: u32,
        a: AppendArgs<'_>,
    ) -> Result<()> {
        let AppendArgs {
            pid,
            bucket,
            base_offset,
            count,
            created_at_us,
            hashes,
            blob,
        } = a;
        let Some(mut p) = w.partition(pid)? else {
            return Err(ApplyError::Inconsistent {
                what: "Append",
                detail: format!("no partition row for pid {pid}"),
            });
        };
        // Offsets are gapless and the planner allocates them from the row this
        // node holds. A mismatch means the planner planned against different
        // state, which is a divergence and not something to paper over.
        let want = (p.last_offset + 1) as u64;
        if base_offset != want {
            return Err(ApplyError::Inconsistent {
                what: "Append",
                detail: format!("pid {pid}: base_offset {base_offset}, partition wants {want}"),
            });
        }
        if count == 0 {
            return Err(ApplyError::Inconsistent {
                what: "Append",
                detail: format!("pid {pid}: an append of no messages"),
            });
        }

        // A3b (`ALICE_PGLESS_NEWARCH.md` §5, the double-write kill): on the LIVE
        // path (`qlog_writer_external`) the payload lives ONCE, in the qlog —
        // written + fsync'd by the log writer BEFORE this entry was made durable,
        // never in a segment. So apply files NO segment; `retained_len` is still
        // the frame length the segment WOULD have taken (`frame::encoded_len`
        // == the segment's own `pos.len`, a pure function of `count` and the
        // payload size), so `RetainedBytes` — a REPLICATED counter — is
        // byte-for-byte the knob-off value and the off-vs-on digest stays
        // transparent. Knob OFF, or the unit-test path where apply owns the qlog
        // (`!qlog_writer_external`): file the payload into a segment and record
        // its location, exactly today — the seg + qlog are both written there so
        // the read-match tests can compare them.
        // B07: the frame length retention gives back rides on the append's
        // `txns` row (a v2 row, `dedup::record_with_len`) whenever the index
        // mode writes one — every mode but `DEDUP_INDEX=segment`.
        let carry = dedup::txns_carry_len() && !hashes.is_empty();
        let retained_len: u64 = if cx.cfg.qlog && cx.cfg.qlog_writer_external {
            // No segment (the payload lives once, in the qlog). The reference
            // entry carries the payload's 4-byte FRAME LENGTH in place of the
            // payload (`effect::write_effect_payload_free`), and the writer hands
            // apply that SAME length-only form live — so `len` is read from the
            // entry IDENTICALLY whether this `Append` came live or from a replay
            // of the payload-free log. That is what makes `RetainedBytes` — a
            // REPLICATED counter — REPLAY-STABLE (I2).
            let len: u64 = if blob.len() == 4 {
                u32::from_le_bytes(blob.try_into().expect("4-byte frame length")) as u64
            } else {
                // Defensive only: the writer/replay never hand apply a full blob on
                // this path. Account a stray shape rather than lose it.
                crate::rsm::segments::frame::encoded_len(count, blob.len()) as u64
            };
            // The length's only other copy used to be a node-local `seg_loc`
            // row with a sentinel `file_id = 0`, offset 0 (no segment claim),
            // written per append just so the watermark could give the bytes
            // back — a second B-tree row for every append, ~45% of the
            // per-append RAM at one message per append. With the length on
            // the txns row it is not written; `DEDUP_INDEX=segment`, which
            // writes no txns row, keeps it.
            if !carry {
                w.put_seg_loc(
                    pid,
                    base_offset,
                    &SegLocRow {
                        bucket,
                        file_id: 0,
                        offset: 0,
                        len: len as u32,
                    },
                )?;
            }
            len
        } else {
            let pos = match side {
                Side::Inline { segments, .. } => {
                    let pos = segments.append(
                        bucket,
                        pid,
                        base_offset,
                        count,
                        created_at_us,
                        hashes,
                        blob,
                    )?;
                    // §13.5 `apply.segment_written`: the payload bytes are in a
                    // segment file (page cache, unsynced) and the store commit
                    // that records this append and the applied index has NOT
                    // happened. A crash here reopens at the previous applied
                    // index; recovery truncates the file to the length the
                    // reopened store records and the entry replays (I11).
                    crate::rsm::faults::hit("apply.segment_written");
                    pos
                }
                // The coordinator wrote the frame before the run, in entry
                // order, so the file bytes are the one-thread path's.
                Side::Deferred => cx
                    .positions
                    .get(ord as usize)
                    .copied()
                    .flatten()
                    .ok_or_else(|| ApplyError::Inconsistent {
                        what: "Append",
                        detail: format!("pid {pid}: no frame position from the run's pre-pass"),
                    })?,
            };
            w.put_seg_loc(
                pid,
                base_offset,
                &SegLocRow {
                    bucket: pos.bucket,
                    file_id: pos.file_id,
                    offset: pos.offset,
                    len: pos.len,
                },
            )?;
            pos.len as u64
        };
        // The dedup index and the txns row (D10 option (a), lean), the row
        // carrying the retained length (B07) on EVERY path, the segment one
        // included: the replicated `txns` keyspace is then the same whether
        // this node's payloads live in the queue logs or in segment files (a
        // v1 row on one path and a v2 on the other would split the digest).
        // Apply re-reads the row it extends rather than carrying the planner's
        // view forward: the probe ran in another transaction, on another node.
        // `DEDUP_INDEX=segment` records no row at all, so the list is not built.
        let end = base_offset + count as u64 - 1;
        if dedup::record_index_mode() != dedup::IndexMode::Segment {
            self.accepted.clear();
            self.accepted
                .extend(hashes.chunks_exact(16).enumerate().map(|(i, h)| {
                    let mut a = [0u8; 16];
                    a.copy_from_slice(h);
                    (a, base_offset + i as u64)
                }));
            dedup::record_with_len(
                w,
                pid,
                base_offset,
                end,
                &self.accepted,
                created_at_us,
                retained_len as u32,
            )?;
        }

        p.last_offset = end as i64;
        p.last_write_at_us = created_at_us;
        p.last_created_at_us = created_at_us;
        if p.oldest_live_at_us.is_none() {
            p.oldest_live_at_us = Some(created_at_us);
        }
        if cx.wants_appended {
            self.appended
                .push((p.tenant.clone(), p.queue.clone(), p.partition.clone()));
        }
        w.put_partition(pid, &p)?;
        let (tenant, queue) = (p.tenant.as_str(), p.queue.as_str());

        // The apply-owned qlog record (unit tests, A1-A3a): buffered here on
        // the inline path, flushed at the end of the entry and fsync'd at the
        // commit. A run's records were buffered by the coordinator's pre-pass,
        // in entry order; the live path's external writer wrote them before
        // the entry was durable.
        if let Side::Inline {
            qlog: Some(qlog), ..
        } = side
        {
            qlog.buffer(
                tenant,
                queue,
                cx.index,
                pid,
                base_offset,
                count,
                created_at_us,
                hashes,
                blob,
            );
        }

        // §7.4: `meta.max_created_at_us` is the floor the next planner stamp
        // has to clear, and segment stamps can run ahead of `now` by a
        // microsecond per segment in a cycle.
        if created_at_us > self.max_created_at_us {
            self.max_created_at_us = created_at_us;
        }

        // Counters (§6.4, D16): O(1) per effect at queue and tenant scope (and
        // the partition's retained bytes), plus O(subscribed groups) below.
        let n = count as i64;
        self.bump(w, pid, tenant, queue, None, Counter::Pushed, n)?;
        self.bump(
            w,
            pid,
            tenant,
            queue,
            None,
            Counter::RetainedBytes,
            retained_len as i64,
        )?;
        self.stamp(w, pid, tenant, queue, Counter::LastPushUs, created_at_us)?;

        // `pending`, one row per subscribed group (§6.1): the ready rings
        // rebuild from it in O(pending), so nothing here walks partitions.
        let slot = self.queues.slot(tenant, queue);
        let ready_at = self.ready_at(w, slot, created_at_us)?;
        let groups = self.groups_of(w, cx.cfg, slot)?;
        let transitions = cx.cfg.pending_transitions;
        for (gi, g) in groups.iter().enumerate() {
            self.append_pending(w, tenant, queue, g, pid, ready_at, transitions)?;
            key_group(&mut self.key, tenant, queue, g, Counter::Pending);
            self.ctr.add(w, &self.key, n)?;
            // P2.2: a frame appended under a live lease is not claimable by
            // anyone (the transitions path floors it at the lease); the lease's
            // own release wakes a pop, so waking one here would only burn a
            // pipeline trip on a pop that must come back empty.
            let leased = transitions
                && self
                    .leases
                    .expiry(pid, g)
                    .is_some_and(|exp| exp > cx.now_us);
            if !leased {
                if cx.cfg.batch_counters {
                    self.queues.wake_index(slot, gi);
                } else {
                    // A fresh scan each append (the ablation path): the list
                    // the counts would index is not the cached one.
                    self.queues.touch(slot);
                    self.queues.slots[slot as usize].named_slot(g).1 += 1;
                }
            }
        }
        if cx.wants_append_wakes {
            self.queues.wake_all(slot);
        }

        self.stats.appends += 1;
        self.stats.messages += count as u64;
        self.stats.bytes_appended += retained_len;
        Ok(())
    }

    /// When a partition is next worth looking at for a group after an append.
    ///
    /// COARSE BY CONTRACT, exactly like the ring entry it becomes: the claim
    /// re-verifies everything against the cursor row. `delayed_processing` and
    /// `window_buffer` are the two configured reasons a fresh frame is not
    /// claimable yet. Read once per queue per shard ([`QueueCtx`]), not once
    /// per append.
    fn ready_at<R: Reads + ?Sized>(&mut self, r: &R, slot: u32, created_at_us: i64) -> Result<i64> {
        let q = &mut self.queues.slots[slot as usize];
        let delay_us = match q.delay_us {
            Some(d) => d,
            None => {
                let d = match r.queue(&q.tenant, &q.queue)? {
                    None => 0,
                    Some(cfg) => {
                        let delay = cfg.delayed_processing.max(cfg.window_buffer).max(0) as i64;
                        delay.saturating_mul(1_000_000)
                    }
                };
                q.delay_us = Some(d);
                d
            }
        };
        Ok(created_at_us.saturating_add(delay_us))
    }

    /// The queue's subscribed group names, in key order. With
    /// `batch_counters` on they are cached in the slot across commits and
    /// invalidated on any change to the queue's group set; with it off, a
    /// fresh scan each call, exactly as before (the ablation path).
    fn groups_of<R: Reads + ?Sized>(
        &mut self,
        r: &R,
        cfg: &ApplyConfig,
        slot: u32,
    ) -> Result<Arc<[Arc<str>]>> {
        let q = &mut self.queues.slots[slot as usize];
        if cfg.batch_counters {
            if let Some(g) = &q.groups {
                return Ok(g.clone());
            }
        }
        let mut names: Vec<Arc<str>> = Vec::new();
        r.scan_groups(&q.tenant, &q.queue, usize::MAX, &mut |g, _row| {
            names.push(Arc::from(g));
            true
        })?;
        let list: Arc<[Arc<str>]> = names.into();
        if cfg.batch_counters {
            q.freeze();
            q.groups = Some(list.clone());
        }
        Ok(list)
    }

    /// Maintain one group's `pending` row for an append (§6.1).
    ///
    /// With `transitions` off: an unconditional `put_pending` on every append,
    /// which OVERWRITES the stored `ready_at` with this frame's. With it on
    /// (the default), `ready_at` is maintained to the EARLIEST wall-time the
    /// partition could yield a claim: a live lease floors it at the lease
    /// expiry, and the row is written only on a TRANSITION (no row yet, or an
    /// earlier `ready_at`). Every input is committed state plus the lease's
    /// RAM twin (rebuilt from `leases_by_worker`), so the decision is a pure,
    /// cadence-free function of the replicated log (I2).
    #[allow(clippy::too_many_arguments)]
    fn append_pending<W: Writes + ?Sized>(
        &mut self,
        w: &mut W,
        tenant: &str,
        queue: &str,
        group: &str,
        pid: Pid,
        ready_at: i64,
        transitions: bool,
    ) -> Result<()> {
        if transitions {
            let floored = match self.leases.expiry(pid, group) {
                Some(exp) if exp > ready_at => exp,
                _ => ready_at,
            };
            if let Some(stored) = w.pending_at(tenant, queue, group, pid)? {
                if floored >= stored {
                    return Ok(());
                }
            }
            w.put_pending(tenant, queue, group, pid, floored)?;
            return Ok(());
        }
        w.put_pending(tenant, queue, group, pid, ready_at)?;
        Ok(())
    }

    // -- cursors -----------------------------------------------------------

    fn cursor_set<W: Writes + ?Sized>(
        &mut self,
        w: &mut W,
        cx: &Cx<'_>,
        pid: Pid,
        group: &str,
        row: &CursorRow,
    ) -> Result<()> {
        let Some(p) = w.partition(pid)? else {
            return Err(ApplyError::Inconsistent {
                what: "CursorSet",
                detail: format!("no partition row for pid {pid}"),
            });
        };
        let (tenant, queue) = (p.tenant.as_str(), p.queue.as_str());
        let old = w.cursor(pid, group)?;

        // `leases_by_worker` is the derived index a lease renew walks.
        // It mirrors the cursor row exactly: one entry while the row names a
        // worker and an expiry, none otherwise.
        let had_lease = old
            .as_ref()
            .map(|c| c.worker.is_some() && c.lease_expires_at_us.is_some())
            .unwrap_or(false);
        if let Some(worker) = old.as_ref().and_then(|c| c.worker.as_deref()) {
            w.del_lease(worker, pid, group)?;
        }
        let has_lease = match (&row.worker, row.lease_expires_at_us) {
            (Some(worker), Some(exp)) => {
                w.put_lease(worker, pid, group, exp)?;
                self.leases.note(pid, group, exp);
                true
            }
            _ => {
                self.leases.clear(pid, group);
                false
            }
        };

        w.put_cursor(pid, group, row)?;

        // Counters (§6.4). `committed` is "last acked offset", so the delta is
        // the frames this write completed; a seek backwards gives a negative
        // delta.
        let old_committed = old.as_ref().map(|c| c.committed).unwrap_or(-1);
        let delta = row.committed - old_committed;
        if delta != 0 {
            self.bump(
                w,
                pid,
                tenant,
                queue,
                Some(group),
                Counter::Completed,
                delta,
            )?;
            key_group(&mut self.key, tenant, queue, group, Counter::Pending);
            self.ctr.add(w, &self.key, -delta)?;
        }
        let old_consumed = old.as_ref().map(|c| c.total_consumed).unwrap_or(0);
        if row.total_consumed > old_consumed {
            let d = (row.total_consumed - old_consumed) as i64;
            self.bump(w, pid, tenant, queue, Some(group), Counter::Consumed, d)?;
        }
        let old_retries = old.as_ref().map(|c| c.batch_retry_count).unwrap_or(0);
        if row.batch_retry_count > old_retries {
            self.bump(w, pid, tenant, queue, Some(group), Counter::Failed, 1)?;
        }
        if let Some(at) = row.lease_acquired_at_us {
            self.stamp(w, pid, tenant, queue, Counter::LastPopUs, at)?;
        }

        // `pending` mirrors "this partition has work for this group". A live
        // lease is not work for anyone else until it expires, which is what
        // the ring's deferral is for.
        if row.committed >= p.last_offset {
            w.del_pending(tenant, queue, group, pid)?;
        } else {
            let ready_at = if has_lease {
                row.lease_expires_at_us.unwrap_or(cx.now_us)
            } else {
                cx.now_us
            };
            w.put_pending(tenant, queue, group, pid, ready_at)?;
        }

        // A released lease is the other half of a wake (§9.5): the partition
        // became claimable for whoever is parked on it — only if it still holds
        // work (P2.2: a drained partition would wake a pop into an empty trip).
        if had_lease && !has_lease && row.committed < p.last_offset {
            let slot = self.queues.slot(tenant, queue);
            self.queues.wake_release(slot, group);
        }
        Ok(())
    }

    /// A cursor, its lease index row and its `pending` row. Also the
    /// coordinator's, for a group delete's and a partition's chunks.
    pub(super) fn cursor_delete<W: Writes + ?Sized>(
        &mut self,
        w: &mut W,
        pid: Pid,
        group: &str,
    ) -> Result<()> {
        let Some(old) = w.cursor(pid, group)? else {
            self.stats.missing_rows += 1;
            return Ok(());
        };
        if let Some(worker) = &old.worker {
            w.del_lease(worker, pid, group)?;
        }
        self.leases.clear(pid, group);
        w.del_cursor(pid, group)?;
        if let Some(p) = w.partition(pid)? {
            w.del_pending(&p.tenant, &p.queue, group, pid)?;
        }
        Ok(())
    }

    // -- watermarks (retention, §5.2, §11.7, D10) --------------------------

    /// Move a partition's two watermarks.
    ///
    /// `log_start` is where the payload begins, `txns_start` where the hash
    /// lists begin, and `txns_start ≤ log_start` — the hash lists outlive the
    /// segments retention deletes, because the dedup probe and ack-by-hash
    /// below the cursor still read them inside the txns window (D10). Each
    /// frame is therefore released TWICE, once per watermark, and only the
    /// second release lets its file die (§11.7).
    #[allow(clippy::too_many_arguments)]
    fn watermark<W: Writes + ?Sized>(
        &mut self,
        w: &mut W,
        cx: &Cx<'_>,
        side: &mut Side<'_>,
        ord: u32,
        pid: Pid,
        log_start: u64,
        txns_start: u64,
    ) -> Result<()> {
        let Some(mut p) = w.partition(pid)? else {
            self.stats.missing_rows += 1;
            return Ok(());
        };
        if log_start < p.log_start || txns_start < p.txns_start {
            return Err(ApplyError::Inconsistent {
                what: "Watermark",
                detail: format!(
                    "pid {pid}: watermarks never move back \
                     (log {} → {log_start}, txns {} → {txns_start})",
                    p.log_start, p.txns_start
                ),
            });
        }
        if txns_start > log_start {
            return Err(ApplyError::Inconsistent {
                what: "Watermark",
                detail: format!("pid {pid}: txns_start {txns_start} above log_start {log_start}"),
            });
        }
        let old_log_start = p.log_start;
        let old_txns_start = p.txns_start;

        // 1. The payload: every frame whose base crossed `log_start` this
        //    time. Its bytes stop being retained; its `seg_loc` row (if it has
        //    one) stays, because the hash list still needs to be findable.
        //    What it retained is the length its txns row carries (B07), or,
        //    for a frame whose txns row carries none (an older build's, or no
        //    txns row in `DEDUP_INDEX=segment`), its `seg_loc` row's. Where
        //    both exist they are the same number (the frame's encoded length).
        let mut carried: Vec<u64> = Vec::new();
        let mut bytes_freed: i64 = 0;
        if dedup::txns_carry_len() {
            let prefix = keys::txns_prefix(pid);
            let start = keys::txns(pid, p.log_start);
            let mut bad = false;
            w.scan_raw(Keyspace::Txns, &start, &prefix, usize::MAX, &mut |k, v| {
                let Some(base) = keys::txns_base_of(k) else {
                    bad = true;
                    return false;
                };
                if base >= log_start {
                    return false;
                }
                if let Some(len) = dedup::TxnsRow::retained_len_of(v) {
                    carried.push(base);
                    bytes_freed += len as i64;
                }
                true
            })?;
            if bad {
                return Err(StoreError::corrupt(Keyspace::Txns, "txns key").into());
            }
        }
        let mut released: Vec<(u64, SegLocRow)> = Vec::new();
        w.scan_seg_loc(pid, p.log_start, usize::MAX, &mut |base, row| {
            if base >= log_start {
                return false;
            }
            released.push((base, row));
            true
        })?;
        for (base, row) in &released {
            self.release_seg_loc(cx.cfg, side, ord, row, Release::Retained);
            // Both scans are in base order, so `carried` is sorted.
            if carried.binary_search(base).is_err() {
                bytes_freed += row.len as i64;
            }
        }
        if bytes_freed != 0 {
            self.bump(
                w,
                pid,
                &p.tenant,
                &p.queue,
                None,
                Counter::RetainedBytes,
                -bytes_freed,
            )?;
        }

        // 2. The hash lists: every frame whose base crossed `txns_start`. Now
        //    the frame is gone for good, so its `seg_loc` row goes with it.
        let mut expired: Vec<(u64, SegLocRow)> = Vec::new();
        w.scan_seg_loc(pid, p.txns_start, usize::MAX, &mut |base, row| {
            if base >= txns_start {
                return false;
            }
            expired.push((base, row));
            true
        })?;
        for (base, row) in &expired {
            self.release_seg_loc(cx.cfg, side, ord, row, Release::Window);
            w.del_seg_loc(pid, *base)?;
        }
        self.expire_hashes(w, pid, p.txns_start, txns_start)?;

        // 3. The dead letters follow the queue's retention: the ones whose
        //    message just left the log go with it.
        self.trim_dlq(w, pid, &p.tenant, &p.queue, log_start)?;

        p.log_start = log_start;
        p.txns_start = txns_start;
        // `oldestMessage`: the stamp of the oldest message still held, the
        // frame at the new `log_start`. A partition without txns rows keeps
        // what it had; an emptied one holds nothing.
        p.oldest_live_at_us = if log_start as i64 > p.last_offset {
            None
        } else {
            frame_stamp_from(w, pid, log_start)?.or(p.oldest_live_at_us)
        };
        w.put_partition(pid, &p)?;
        match side {
            Side::Inline { metrics, .. } => metrics.record_retention(
                cx.now_us,
                &p.tenant,
                &p.queue,
                pid,
                old_log_start,
                log_start,
                old_txns_start,
                txns_start,
            ),
            Side::Deferred => {
                // The journal ignores a watermark that moved nothing; so does
                // the record kept for it.
                if log_start > old_log_start || txns_start > old_txns_start {
                    self.retention.push((
                        ord,
                        RetentionRec {
                            tenant: std::mem::take(&mut p.tenant),
                            queue: std::mem::take(&mut p.queue),
                            pid,
                            log_from: old_log_start,
                            log_to: log_start,
                            txns_from: old_txns_start,
                            txns_to: txns_start,
                        },
                    ));
                }
            }
        }
        Ok(())
    }

    /// Retire one `seg_loc` row's claim on its segment file (§11.7).
    ///
    /// A row the qlog path filed (`file_id` 0, offset 0: the payload lives
    /// once, in the qlog) holds no claim. File 0 is nonetheless a real file of
    /// its bucket, so releasing against it would drive that file's counters
    /// below the truth ("a segment claim was released twice").
    fn release_seg_loc(
        &mut self,
        cfg: &ApplyConfig,
        side: &mut Side<'_>,
        ord: u32,
        row: &SegLocRow,
        what: Release,
    ) {
        if cfg.qlog && cfg.qlog_writer_external && row.file_id == 0 && row.offset == 0 {
            return;
        }
        let pos = Position {
            bucket: row.bucket,
            file_id: row.file_id,
            offset: row.offset,
            len: row.len,
        };
        match side {
            Side::Inline { segments, .. } => segments.release(pos, what),
            Side::Deferred => self.releases.push((ord, pos, what)),
        }
    }

    /// Dead letters follow the queue's retention: one goes when `log_start`
    /// passes the offset of the message it holds. A timer's dead letter files
    /// at −1, holds no message of the log, and stays.
    ///
    /// At most [`DLQ_TRIM_PER_WATERMARK`] ids per call, so one entry's apply
    /// stays short; the rest go with the next watermarks. Every call starts
    /// again from each group's lowest offset, so a replica that applied
    /// earlier watermarks without this rule catches up at its first one.
    fn trim_dlq<W: Writes + ?Sized>(
        &mut self,
        w: &mut W,
        pid: Pid,
        tenant: &str,
        queue: &str,
        log_start: u64,
    ) -> Result<()> {
        let pid_prefix = keys::dlq_by_pos_pid_prefix(pid);
        let mut from = pid_prefix.clone();
        let mut found: Vec<(Vec<u8>, Vec<[u8; 16]>)> = Vec::new();
        let mut ids = 0usize;
        let mut bad = false;
        while ids < DLQ_TRIM_PER_WATERMARK && !bad {
            // The next group with dead letters in this partition.
            let mut first: Option<Vec<u8>> = None;
            w.scan_raw(Keyspace::DlqByPos, &from, &pid_prefix, 1, &mut |k, _v| {
                first = Some(k.to_vec());
                false
            })?;
            let Some(first) = first else { break };
            let Some(group_len) = first.len().checked_sub(8) else {
                bad = true;
                break;
            };
            let group_prefix = first[..group_len].to_vec();
            w.scan_raw(
                Keyspace::DlqByPos,
                &first,
                &group_prefix,
                usize::MAX,
                &mut |k, v| match keys::dlq_by_pos_offset_of(k) {
                    Some(offset) if offset < 0 => true,
                    Some(offset) if (offset as u64) < log_start => match rows::dlq_ids_decode(v) {
                        Ok(list) => {
                            ids += list.len();
                            found.push((k.to_vec(), list));
                            ids < DLQ_TRIM_PER_WATERMARK
                        }
                        Err(_) => {
                            bad = true;
                            false
                        }
                    },
                    Some(_) => false,
                    None => {
                        bad = true;
                        false
                    }
                },
            )?;
            // Past every key of this group: its prefix, then more than eight
            // bytes of 0xFF.
            from = group_prefix;
            from.extend_from_slice(&[0xFF; 9]);
        }
        if bad {
            return Err(StoreError::corrupt(Keyspace::DlqByPos, "dlq position key").into());
        }
        let mut gone = 0i64;
        for (key, list) in &found {
            for id in list {
                if let Some(row) = w.dlq(tenant, queue, id)? {
                    w.del_dlq(tenant, queue, id, &row)?;
                    gone += 1;
                }
            }
            // Whatever the list still names (a row that was never there) is
            // below `log_start` too.
            w.del_raw(Keyspace::DlqByPos, key)?;
        }
        settle_dlq_count(w, &mut self.ctr, &mut self.key, pid, tenant, queue, gone)
    }

    /// Drop the dedup occurrences and the `txns` rows below `to`.
    ///
    /// The `txns` row of an append that STRADDLES the new watermark is left
    /// alone: it carries the hash list of offsets above it too, and a row is
    /// the unit D10 stores. Keeping a few hashes longer than asked is exact in
    /// the direction that matters — a duplicate is still found — while
    /// deleting them early would answer "new" for a transaction id that is
    /// still a duplicate.
    fn expire_hashes<W: Writes + ?Sized>(
        &mut self,
        w: &mut W,
        pid: Pid,
        from: u64,
        to: u64,
    ) -> Result<()> {
        // STORAGE_V2 Lever 2 (`DEDUP_INDEX=segment`): there are NO `Txns` (or
        // `Dedup`) rows to expire — `record` wrote none. The dedup window is
        // enforced entirely by the segment scan's `created >= floor` filter and
        // by segment-file GC. So this is a no-op: no store write on the
        // retention path.
        if dedup::record_index_mode() == dedup::IndexMode::Segment {
            return Ok(());
        }
        if to <= from {
            return Ok(());
        }
        let mut victims: Vec<(u64, dedup::TxnsRow)> = Vec::new();
        let prefix = keys::txns_prefix(pid);
        let start = keys::txns(pid, from);
        let mut bad: Option<&'static str> = None;
        w.scan_raw(Keyspace::Txns, &start, &prefix, usize::MAX, &mut |k, v| {
            let Some(base) = keys::txns_base_of(k) else {
                bad = Some("txns key");
                return false;
            };
            match dedup::TxnsRow::decode(v) {
                Ok(row) => {
                    if row.end >= to {
                        return false;
                    }
                    victims.push((base, row));
                    true
                }
                Err(e) => {
                    bad = Some(e);
                    false
                }
            }
        })?;
        if let Some(e) = bad {
            return Err(StoreError::corrupt(Keyspace::Txns, e).into());
        }

        for (base, row) in &victims {
            for h in row.iter_hashes() {
                let k = keys::dedup(pid, &h);
                let Some(cur) = w.get_raw(Keyspace::Dedup, &k)? else {
                    continue;
                };
                dedup::check_occurrences(cur)?;
                let mut keep: Vec<u8> = Vec::with_capacity(cur.len());
                for (off, created) in dedup::occurrences(cur) {
                    if off >= to {
                        dedup::push_occurrence(&mut keep, off, created);
                    }
                }
                let changed = keep.len() != cur.len();
                if keep.is_empty() {
                    w.del_raw(Keyspace::Dedup, &k)?;
                } else if changed {
                    w.put_raw(Keyspace::Dedup, &k, &keep)?;
                }
            }
            w.del_raw(Keyspace::Txns, &keys::txns(pid, *base))?;
            self.stats.rows_swept += 1;
        }
        Ok(())
    }

    // -- counters ----------------------------------------------------------

    /// One counter at queue and tenant scope — and partition scope for the
    /// two a partition keeps ([`partition_scoped`]) — plus the group scope
    /// when the effect names a group (§6.4, D16). Keys are built in the
    /// shard's scratch buffer: no allocation per bump.
    #[allow(clippy::too_many_arguments)]
    fn bump<W: Writes + ?Sized>(
        &mut self,
        w: &mut W,
        pid: Pid,
        tenant: &str,
        queue: &str,
        group: Option<&str>,
        c: Counter,
        delta: i64,
    ) -> Result<()> {
        if delta == 0 {
            return Ok(());
        }
        if partition_scoped(c) {
            key_partition(&mut self.key, pid, c);
            self.ctr.add(w, &self.key, delta)?;
        }
        key_queue(&mut self.key, tenant, queue, c);
        self.ctr.add(w, &self.key, delta)?;
        key_tenant(&mut self.key, tenant, c);
        self.ctr.add(w, &self.key, delta)?;
        if let Some(g) = group {
            key_group(&mut self.key, tenant, queue, g, c);
            self.ctr.add(w, &self.key, delta)?;
        }
        Ok(())
    }

    /// A "latest time" counter: monotone, so an out-of-order effect cannot
    /// make a queue look idle.
    fn stamp<W: Writes + ?Sized>(
        &mut self,
        w: &mut W,
        pid: Pid,
        tenant: &str,
        queue: &str,
        c: Counter,
        at_us: i64,
    ) -> Result<()> {
        if partition_scoped(c) {
            key_partition(&mut self.key, pid, c);
            self.ctr.stamp(w, &self.key, at_us)?;
        }
        key_queue(&mut self.key, tenant, queue, c);
        self.ctr.stamp(w, &self.key, at_us)?;
        Ok(())
    }
}

/// An `Append`'s fields, borrowed from the effect.
struct AppendArgs<'e> {
    pid: Pid,
    bucket: u16,
    base_offset: u64,
    count: u32,
    created_at_us: i64,
    hashes: &'e [u8],
    blob: &'e [u8],
}

// ---------------------------------------------------------------------------
// Helpers shared with the coordinator
// ---------------------------------------------------------------------------

/// The stamp of the first frame at or above `from`: its txns row's
/// `created_at`. `None` when no row is there.
pub(super) fn frame_stamp_from<R: Reads + ?Sized>(
    r: &R,
    pid: Pid,
    from: u64,
) -> Result<Option<i64>> {
    let prefix = keys::txns_prefix(pid);
    let start = keys::txns(pid, from);
    let mut stamp = None;
    let mut bad = false;
    r.scan_raw(Keyspace::Txns, &start, &prefix, 1, &mut |_k, v| {
        match dedup::TxnsRow::decode(v) {
            Ok(row) => stamp = Some(row.created_at_us),
            Err(_) => bad = true,
        }
        false
    })?;
    if bad {
        return Err(StoreError::corrupt(Keyspace::Txns, "txns row").into());
    }
    Ok(stamp)
}

/// May rows removed under `pid` still settle the QUEUE's and the TENANT's
/// gauges?
///
/// Not a test on the name. §5.2 ratifies that the name is reusable the
/// instant a delete lands, while the pid-keyed rows of the queue that had it
/// are deleted in chunks for as long as that takes; a name test therefore
/// subtracts a dead queue's dead letters and retained bytes from whatever
/// queue holds the name when the chunk runs. D16 says the counters ARE the
/// answer — there is no aggregation to correct them — so the recreated queue
/// reports a negative count, for ever.
///
/// A pid in the `garbage` set answers from the id its `GarbageAdd` recorded:
/// the gauges are still its own only while the live row carries that same id,
/// and a queue or tenant delete recorded `None`, having settled and swept them
/// itself. A pid that is not garbage is an ordinary partition of a live queue.
pub(super) fn queue_gauges_live<R: Reads + ?Sized>(
    r: &R,
    pid: Pid,
    tenant: &str,
    queue: &str,
) -> Result<bool> {
    let live = r.queue(tenant, queue)?;
    match r.garbage(pid)? {
        Some(g) => Ok(match (g.queue_id, live) {
            (Some(id), Some(cfg)) => cfg.id == id,
            _ => false,
        }),
        None => Ok(live.is_some()),
    }
}

/// Retire dead letters from the `dlq_count` GAUGE at every scope that still
/// has one (§6.4).
///
/// Every path that removes a dead letter passes through here, not only
/// `DlqDelete`: a consumer-group delete and a partition delete remove the rows
/// too, and a gauge that counts only some of the removals drifts up for ever.
/// The queue and tenant rows are touched only while the queue those rows
/// belong to is still there ([`queue_gauges_live`]): the queue delete settles
/// the tenant from the queue and sweeps both, and `add_counter` on a swept row
/// would recreate it holding a negative number — or take it out of the queue
/// that has since been created under the same name.
#[allow(clippy::too_many_arguments)]
pub(super) fn settle_dlq_count<W: Writes + ?Sized>(
    w: &mut W,
    ctr: &mut CounterCache,
    key: &mut Vec<u8>,
    pid: Pid,
    tenant: &str,
    queue: &str,
    gone: i64,
) -> Result<()> {
    if gone == 0 {
        return Ok(());
    }
    let queue_alive = queue_gauges_live(w, pid, tenant, queue)?;
    key_partition(key, pid, Counter::DlqCount);
    ctr.add(w, key, -gone)?;
    if queue_alive {
        key_queue(key, tenant, queue, Counter::DlqCount);
        ctr.add(w, key, -gone)?;
        key_tenant(key, tenant, Counter::DlqCount);
        ctr.add(w, key, -gone)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Counter keys into a scratch buffer (B42)
// ---------------------------------------------------------------------------
//
// Byte-for-byte [`keys::counter_partition`], [`keys::counter_queue`],
// [`keys::counter_tenant`] and [`keys::counter_group`] (a test holds them
// equal), written into a reused buffer instead of a fresh `Vec` per bump: an
// append bumped five to seven counters, each a key allocation.

pub(super) fn key_partition(out: &mut Vec<u8>, pid: Pid, c: Counter) {
    out.clear();
    out.push(CounterScope::Partition as u8);
    keys::push_u64(out, pid);
    keys::push_u16(out, c as u16);
}

pub(super) fn key_queue(out: &mut Vec<u8>, tenant: &str, queue: &str, c: Counter) {
    out.clear();
    out.push(CounterScope::Queue as u8);
    keys::push_name(out, tenant);
    keys::push_name(out, queue);
    keys::push_u16(out, c as u16);
}

pub(super) fn key_tenant(out: &mut Vec<u8>, tenant: &str, c: Counter) {
    out.clear();
    out.push(CounterScope::Tenant as u8);
    keys::push_name(out, tenant);
    keys::push_u16(out, c as u16);
}

pub(super) fn key_group(out: &mut Vec<u8>, tenant: &str, queue: &str, group: &str, c: Counter) {
    out.clear();
    out.push(CounterScope::Group as u8);
    keys::push_name(out, tenant);
    keys::push_name(out, queue);
    keys::push_name(out, group);
    keys::push_u16(out, c as u16);
}

// ---------------------------------------------------------------------------
// The pool: persistent shard threads, lent borrowed work one run at a time
// ---------------------------------------------------------------------------

/// A job lent to a worker for the length of one [`Pool::scope`] call, its
/// lifetime erased (see the SAFETY note in [`Pool::scope`]).
struct Job(*mut (dyn FnMut() + Send + 'static));

// SAFETY: the pointee is `Send` (the bound is in the type), and the pointer
// is only dereferenced by the one worker it was posted to, while
// `Pool::scope` keeps the pointee alive and untouched on the posting thread.
unsafe impl Send for Job {}

struct Slot {
    job: Mutex<Option<Job>>,
    /// A job is waiting in `job` (read while spinning, before the lock).
    posted: AtomicBool,
    cv: Condvar,
}

struct PoolShared {
    slots: Vec<Slot>,
    /// Jobs of the current scope not finished yet.
    pending: AtomicUsize,
    done_lock: Mutex<()>,
    done_cv: Condvar,
    /// The first panic of a job in the current scope, re-raised by `scope`.
    panic: Mutex<Option<Box<dyn Any + Send>>>,
    stop: AtomicBool,
}

/// How long a thread busy-waits before it sleeps on a condition variable:
/// the runs of one entry come back to back, and a futex round trip each way
/// (~5-10 µs on Linux) is longer than the gap usually is.
const SPINS: u32 = 1 << 10;

impl PoolShared {
    /// Until every job of the current scope has finished.
    fn wait_all(&self) {
        let mut spins = 0;
        while self.pending.load(Ordering::Acquire) != 0 {
            if spins < SPINS {
                std::hint::spin_loop();
                spins += 1;
                continue;
            }
            let g = self
                .done_lock
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let _g = self
                .done_cv
                .wait_while(g, |_| self.pending.load(Ordering::Acquire) != 0)
                .unwrap_or_else(PoisonError::into_inner);
            return;
        }
    }
}

/// `workers` persistent threads that run borrowed closures, one scope at a
/// time: the shard threads of sharded apply. `std` only — the scoped-pool
/// pattern (a job's borrow is erased, and the scope does not end before the
/// job has) — so a run pays two wake-ups, not two thread spawns.
pub(super) struct Pool {
    shared: Arc<PoolShared>,
    threads: Vec<JoinHandle<()>>,
}

impl Pool {
    pub(super) fn new(workers: usize) -> std::io::Result<Pool> {
        let shared = Arc::new(PoolShared {
            slots: (0..workers)
                .map(|_| Slot {
                    job: Mutex::new(None),
                    posted: AtomicBool::new(false),
                    cv: Condvar::new(),
                })
                .collect(),
            pending: AtomicUsize::new(0),
            done_lock: Mutex::new(()),
            done_cv: Condvar::new(),
            panic: Mutex::new(None),
            stop: AtomicBool::new(false),
        });
        let mut pool = Pool {
            shared: shared.clone(),
            threads: Vec::with_capacity(workers),
        };
        for i in 0..workers {
            let shared = shared.clone();
            // On a spawn failure `pool` drops here and stops the threads
            // already running.
            let t = std::thread::Builder::new()
                .name(format!("queen-rsm-apply-{}", i + 1))
                .spawn(move || worker(&shared, i))?;
            pool.threads.push(t);
        }
        Ok(pool)
    }

    pub(super) fn workers(&self) -> usize {
        self.shared.slots.len()
    }

    /// Run `jobs[i]` on worker `i` and `inline` on the calling thread, and
    /// return once every one of them has finished. A panic in a job is
    /// re-raised here, after all of them have finished.
    pub(super) fn scope(
        &mut self,
        jobs: &mut [&mut (dyn FnMut() + Send)],
        inline: &mut dyn FnMut(),
    ) {
        assert!(
            jobs.len() <= self.shared.slots.len(),
            "more jobs than workers"
        );
        /// Waits for the posted jobs even while `inline` unwinds: nothing a
        /// job borrows may be freed before the job is done with it.
        struct WaitAll<'a>(&'a PoolShared);
        impl Drop for WaitAll<'_> {
            fn drop(&mut self) {
                self.0.wait_all();
            }
        }

        let shared = &*self.shared;
        shared.pending.store(jobs.len(), Ordering::Release);
        let guard = WaitAll(shared);
        for (i, job) in jobs.iter_mut().enumerate() {
            let short: *mut (dyn FnMut() + Send + '_) = &mut **job;
            // SAFETY: only the lifetime is erased (the pointer keeps its
            // layout and vtable). The pointee is borrowed from `jobs` for
            // this call, and the call does not return — nor unwind, `guard`
            // sees to that — before `pending` says every posted job has
            // finished; a worker decrements `pending` only after the call
            // through the pointer has returned (or panicked, caught), and
            // never touches the pointer again. `&mut self` makes the scopes
            // of one pool sequential, so a slot holds at most one job.
            let long: *mut (dyn FnMut() + Send + 'static) = unsafe { std::mem::transmute(short) };
            let slot = &shared.slots[i];
            *slot.job.lock().unwrap_or_else(PoisonError::into_inner) = Some(Job(long));
            slot.posted.store(true, Ordering::Release);
            slot.cv.notify_one();
        }
        inline();
        drop(guard);
        let panicked = shared
            .panic
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(p) = panicked {
            resume_unwind(p);
        }
    }
}

fn worker(shared: &PoolShared, i: usize) {
    let slot = &shared.slots[i];
    loop {
        let mut spins = 0;
        while !slot.posted.load(Ordering::Acquire) && spins < SPINS {
            if shared.stop.load(Ordering::Acquire) {
                break;
            }
            std::hint::spin_loop();
            spins += 1;
        }
        let job = {
            let mut g = slot.job.lock().unwrap_or_else(PoisonError::into_inner);
            loop {
                if let Some(j) = g.take() {
                    break Some(j);
                }
                if shared.stop.load(Ordering::Acquire) {
                    break None;
                }
                g = slot.cv.wait(g).unwrap_or_else(PoisonError::into_inner);
            }
        };
        let Some(job) = job else {
            return;
        };
        slot.posted.store(false, Ordering::Relaxed);
        // SAFETY: see `Pool::scope`: the closure is alive and not touched by
        // the posting thread until `pending` is decremented below.
        let r = catch_unwind(AssertUnwindSafe(|| unsafe { (*job.0)() }));
        if let Err(p) = r {
            let mut first = shared.panic.lock().unwrap_or_else(PoisonError::into_inner);
            if first.is_none() {
                *first = Some(p);
            }
        }
        if shared.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
            let _g = shared
                .done_lock
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            shared.done_cv.notify_all();
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        for s in &self.shared.slots {
            let _g = s.job.lock().unwrap_or_else(PoisonError::into_inner);
            s.cv.notify_all();
        }
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch_counter_keys_are_the_store_s_keys() {
        let mut k = Vec::new();
        for c in [
            Counter::Pushed,
            Counter::Pending,
            Counter::Completed,
            Counter::Failed,
            Counter::DlqCount,
            Counter::RetainedBytes,
            Counter::LastPushUs,
            Counter::LastPopUs,
            Counter::Consumed,
        ] {
            for pid in [0u64, 1, 255, 1 << 40, u64::MAX] {
                key_partition(&mut k, pid, c);
                assert_eq!(k, keys::counter_partition(pid, c));
            }
            for (t, q, g) in [
                ("t1", "orders", "g1"),
                ("", "", ""),
                ("a\0b", "q\0", "\0g"),
                ("tenant-ü", "queue/x", "grp"),
            ] {
                key_queue(&mut k, t, q, c);
                assert_eq!(k, keys::counter_queue(t, q, c));
                key_tenant(&mut k, t, c);
                assert_eq!(k, keys::counter_tenant(t, c));
                key_group(&mut k, t, q, g, c);
                assert_eq!(k, keys::counter_group(t, q, g, c));
            }
        }
    }

    #[test]
    fn a_scope_runs_every_job_on_borrowed_data_and_returns_after_all() {
        let mut pool = Pool::new(3).expect("pool");
        let mut data = vec![0u64; 4];
        for round in 1..=200u64 {
            let (head, rest) = data.split_at_mut(1);
            let mut jobs_state: Vec<&mut u64> = rest.iter_mut().collect();
            let mut closures: Vec<_> = jobs_state
                .iter_mut()
                .map(|x| {
                    move || {
                        for _ in 0..100 {
                            **x += round;
                        }
                    }
                })
                .collect();
            let mut refs: Vec<&mut (dyn FnMut() + Send)> = closures
                .iter_mut()
                .map(|c| c as &mut (dyn FnMut() + Send))
                .collect();
            pool.scope(&mut refs, &mut || head[0] += round * 100);
        }
        let want: u64 = (1..=200u64).sum::<u64>() * 100;
        assert_eq!(data, vec![want; 4]);
    }

    #[test]
    fn a_panicking_job_is_re_raised_after_every_job_finished() {
        let mut pool = Pool::new(2).expect("pool");
        let finished = AtomicUsize::new(0);
        let r = catch_unwind(AssertUnwindSafe(|| {
            let mut a = || panic!("job 0 fails");
            let mut b = || {
                std::thread::sleep(std::time::Duration::from_millis(20));
                finished.fetch_add(1, Ordering::SeqCst);
            };
            let mut refs: Vec<&mut (dyn FnMut() + Send)> = vec![&mut a, &mut b];
            pool.scope(&mut refs, &mut || {});
        }));
        assert!(r.is_err(), "the job's panic reaches the caller");
        assert_eq!(
            finished.load(Ordering::SeqCst),
            1,
            "the other job finished first"
        );
        // The pool is still usable after a panicked scope.
        let mut n = 0u32;
        let mut c = || n += 1;
        let mut refs: Vec<&mut (dyn FnMut() + Send)> = vec![&mut c];
        pool.scope(&mut refs, &mut || {});
        assert_eq!(n, 1);
    }

    #[test]
    fn a_scope_waits_for_its_jobs_even_when_the_inline_part_panics() {
        let mut pool = Pool::new(1).expect("pool");
        let done = AtomicBool::new(false);
        let r = catch_unwind(AssertUnwindSafe(|| {
            let mut slow = || {
                std::thread::sleep(std::time::Duration::from_millis(30));
                done.store(true, Ordering::SeqCst);
            };
            let mut refs: Vec<&mut (dyn FnMut() + Send)> = vec![&mut slow];
            pool.scope(&mut refs, &mut || panic!("inline fails"));
        }));
        assert!(r.is_err());
        assert!(
            done.load(Ordering::SeqCst),
            "the scope unwound before its job finished"
        );
    }
}
