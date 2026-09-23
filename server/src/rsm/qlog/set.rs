//! `rsm/qlog/set.rs` — the applier-owned per-queue [`QLog`] registry and the
//! cloneable [`QLogReader`] the facade + planner read through
//! (`ALICE_PGLESS_NEWARCH.md` §1–§5, Phase A1 write / Phase A2 read).
//!
//! # A1 write, A2 read
//!
//! A0 built the per-queue store ([`QLog`]); A1 made this registry SHADOW-write
//! every `Append` a second time (behind `QUEEN_RAFT_QLOG`, default off) into its
//! queue's log, alongside the still-authoritative segment write. A2 switches the
//! READS over: with the knob on, the pop payload read and the
//! `DEDUP_INDEX=segment` dedup authority read from the queue log instead of the
//! `.seg` files and the LMDB keyspaces. The segments and the raft-log blob are
//! still written (removed in A3), so off-vs-on stays behaviourally identical.
//!
//! # The single writer, and the concurrent readers
//!
//! One [`QLog`] per queue, opened under `<data_dir>/qlog/q<queue_id>/` (sibling
//! to `seg/`, `log/`, `store/`). The applier is the SINGLE writer (`buffer` +
//! `flush`, on the apply thread), exactly as it is the single writer of
//! [`crate::rsm::segments::Segments`]. But the READS run on OTHER threads — the
//! pop render on the blocking pool, the dedup probe on the batcher — so each
//! queue's [`QLog`] lives behind an `RwLock`, and the map of them behind another,
//! shared with a cloneable [`QLogReader`] the way `Segments::reader()` shares the
//! segment file set. A write takes the queue's write lock only for its one
//! `append_group` (one `write` + one fsync); a read takes the read lock.
//!
//! # Reopen on restart (A2)
//!
//! [`QLogSet::reopen_all`] discovers every existing `q<id>/` directory at
//! `Applier::open` and reopens it ([`QLog::open`] — torn-tail truncate + `.qidx`
//! rebuild), so a reopened node serves reads from the qlog immediately, before
//! any new append. A genuinely new queue is still opened lazily on its first
//! flush; A0's `QLog::open` reopens an existing directory rather than
//! `create_new`-colliding with it, so the lazy path and the reopen path share one
//! constructor.
//!
//! # An `Append` is buffered, written per entry, fsync'd per durable point
//!
//! An `Append` is [`QLogSet::buffer`]ed — copied into an owned record keyed by
//! its queue — during `apply`, because the effect's `hashes`/`blob` borrow the
//! entry and are freed when `apply` returns. [`QLogSet::flush`] then drains each
//! queue's buffer into ONE [`QLog::write_group`] (a `write`, NO fsync) at the END
//! of that entry's apply — right after `segments.flush_writes`, before the leader
//! answers. That per-entry write is the A2 read invariant: the leader answers a
//! pop right after apply, and a pop can claim (through the overlay) an offset an
//! in-flight push appended in the SAME uncommitted commit window, so the record
//! MUST be readable from the qlog the moment the entry is applied — not one store
//! commit later. The segment path does exactly this (`flush_writes` per entry).
//! [`QLogSet::sync`] fsyncs the queues written since the last durable point, so
//! the barrier stays batched, the qlog twin of the segment durable point.
//!
//! # Determinism (I2)
//!
//! Nothing here reads a clock, the environment or randomness. `seq` and
//! `created_at_us` are the leader's inputs (from the entry/effect); `queue_id`
//! is a stable [`xxhash_rust::xxh3`] of `(tenant, queue)` (see
//! [`QLogSet::queue_id_of`]) — the same shape `planner::bucket_of` uses. The
//! queue id is node-local anyway (it only names a directory, never replicated
//! state), but keeping it a pure function of the two names keeps the shadow
//! path free of any environment the I2 deny-gate forbids.

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::rsm::qlog::{
    BandFrame, CommittedFrame, EntryRecord, OwnedRecord, QLog, QLogOptions, ReclaimProgress,
    RecordInput, WriteRecord,
};

/// Phase C: the reserved queue id of the SYSTEM log (`<root>/q0/`). It holds the
/// entry record of every entry whose effects touch no queue — a `Noop`, a
/// `RequestIdsExpire`, trace / flag / quota / KV / timer / streams / ephemeral
/// config writes, garbage bookkeeping — and of an entry one of whose pid-scoped
/// effects names a partition the writer cannot resolve (a deleted one). It is
/// never read by pop or dedup (no queue name maps to it:
/// [`QLogSet::queue_id_of`] never returns 0 for a real queue); recovery merges
/// it with every queue log ([`QLogSet::scan_entries`]).
pub const SYSTEM_QUEUE_ID: u64 = 0;

/// What [`QLogSet::scan_entries`] found and delivered.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EntryScan {
    /// Entries handed to the callback: exactly `from_seq .. next_seq`, gapless,
    /// each one complete (all of its copies found, all identical).
    pub delivered: u64,
    /// The first seq NOT delivered: where the durable, acknowledgeable prefix
    /// ends. Every record at or above it belongs to a group whose fsyncs did not
    /// all land (nothing of which was acknowledged) — [`QLogSet::truncate_from`]
    /// drops it before the writer reuses those seqs.
    pub next_seq: u64,
    /// The highest entry-record seq found in any log (`0` when none).
    pub max_seq_found: u64,
    /// Distinct entries found at or above `next_seq` (the discarded tail).
    pub discarded: u64,
    /// Why the walk stopped before `max_seq_found`, if it did.
    pub stopped: Option<String>,
}

/// The shared map of per-queue logs. The applier owns the [`QLogSet`] that
/// writes them; a [`QLogReader`] clones this `Arc` and reads them. The outer
/// `RwLock` guards the MAP (opens/removes, rare, apply-thread only); each inner
/// `RwLock` guards ONE queue's [`QLog`] (its writes are the applier's, its reads
/// the pool's / the batcher's).
type SharedLogs = Arc<RwLock<BTreeMap<u64, Arc<RwLock<QLog>>>>>;

#[derive(Default)]
struct SharedTotals {
    files: AtomicU64,
    bytes: AtomicU64,
}

fn replace_total(total: &AtomicU64, before: u64, after: u64) {
    if after >= before {
        total.fetch_add(after - before, Ordering::AcqRel);
    } else {
        total.fetch_sub(before - after, Ordering::AcqRel);
    }
}

/// One `Append` buffered until the next flush, OWNING its bytes.
///
/// The effect's `hashes`/`blob` are borrowed from the entry and go away when
/// `apply` returns; the flush that writes them runs later (at a commit or a
/// durable point), so the buffer keeps its own copies. Bounded by one
/// store-commit window's worth of appends (the flush drains it every commit).
struct Buffered {
    seq: u64,
    pid: u64,
    base_offset: u64,
    count: u32,
    created_at_us: i64,
    hashes: Vec<u8>,
    payload: Vec<u8>,
}

/// The per-queue [`QLog`] registry (Phase A1 shadow write / A2 read). One
/// instance per applier; the write methods take `&mut self` (the applier is the
/// single writer). Reads go through a [`QLogReader`] clone, off other threads.
pub struct QLogSet {
    /// `<data_dir>/qlog`. Each queue's files live under `q<queue_id>/` below it.
    root: PathBuf,
    /// Roll size + fsync mode, mirrored from the segment writer's options so the
    /// shadow rolls and fsyncs on the same terms the authoritative store does.
    opts: QLogOptions,
    /// One open log per queue id, shared with every [`QLogReader`].
    logs: SharedLogs,
    /// Records buffered within ONE entry, per queue id. Drained (written to the
    /// file + index) at the end of that entry's apply by [`QLogSet::flush`], so a
    /// record is readable before the leader answers. Apply-thread only.
    pending: BTreeMap<u64, Vec<Buffered>>,
    /// Queue ids written since the last [`QLogSet::sync`]: their active file has
    /// an un-fsync'd tail, fsync'd together at the durable point. Apply-thread
    /// only.
    dirty: std::collections::BTreeSet<u64>,
    /// A3a durable index (`ALICE_PGLESS_NEWARCH.md` §5). The highest record `seq`
    /// (the leader's order stamp = the entry index) that [`QLogSet::flush`] has
    /// written to a file (page cache), across every queue. Apply-thread only.
    written_seq: u64,
    /// The highest `seq` that [`QLogSet::sync`] has made DURABLE (fsync'd). Every
    /// queue [`QLogSet::flush`] wrote is marked `dirty` and [`QLogSet::sync`]
    /// fsyncs all of them, so once a sync returns every written record is on the
    /// platter and this equals `written_seq`. The applier records it into the
    /// store (`meta::QLOG_DURABLE_INDEX`) in the SAME commit that follows the
    /// sync, so recovery can reconcile the qlog's durable tail against it.
    durable_seq: u64,
    /// Phase C recovery floor, shared with every log this set opens and with
    /// every [`QLogReader`]: no file holding a record with `seq >` it is ever
    /// unlinked ([`QLog::unlink_dead_files`]). `0` until someone raises it (so a
    /// fresh set deletes nothing); the `LocalReplicator` seeds it with the
    /// store's durable index at open and raises it at every durable point.
    floor: Arc<AtomicU64>,
    /// Files / valid bytes across every open log (the replicator's `log_files` /
    /// `log_bytes` metrics once the queue logs are the WAL). Shared with the
    /// maintenance reader because retention may compact a log off the writer
    /// thread.
    totals: Arc<SharedTotals>,
    /// Fair starting point for the globally bounded local-GC step.
    reclaim_queue_cursor: Arc<AtomicU64>,
    /// While non-zero, retention unlinks and rewrites nothing
    /// ([`QLogReader::pause_reclaim`]): a snapshot is linking the files.
    reclaim_paused: Arc<AtomicU64>,
}

impl QLogSet {
    /// A fresh registry rooted at `root` (`<data_dir>/qlog`). Opens no file yet;
    /// call [`QLogSet::reopen_all`] to pick up an existing directory's queues,
    /// and each genuinely new queue's log is created on the first flush that
    /// carries a record for it.
    pub fn new(root: PathBuf, opts: QLogOptions) -> QLogSet {
        QLogSet {
            root,
            opts,
            logs: Arc::new(RwLock::new(BTreeMap::new())),
            pending: BTreeMap::new(),
            dirty: std::collections::BTreeSet::new(),
            written_seq: 0,
            durable_seq: 0,
            floor: Arc::new(AtomicU64::new(0)),
            totals: Arc::new(SharedTotals::default()),
            reclaim_queue_cursor: Arc::new(AtomicU64::new(0)),
            reclaim_paused: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The shared recovery-floor handle (Phase C). Whoever learns a new durable
    /// index raises it with [`QLogSet::set_recovery_floor`] (or through a
    /// [`QLogReader`], which shares the same handle).
    pub fn recovery_floor_handle(&self) -> Arc<AtomicU64> {
        self.floor.clone()
    }

    /// The current recovery floor.
    pub fn recovery_floor(&self) -> u64 {
        self.floor.load(Ordering::Acquire)
    }

    /// Raise the recovery floor to `durable_index` (monotone: a lower value is
    /// ignored, so two callers reporting the same durable points never fight).
    pub fn set_recovery_floor(&self, durable_index: u64) {
        self.floor.fetch_max(durable_index, Ordering::AcqRel);
    }

    /// `(files, valid bytes)` across every open log.
    pub fn totals(&self) -> (u64, u64) {
        (
            self.totals.files.load(Ordering::Acquire),
            self.totals.bytes.load(Ordering::Acquire),
        )
    }

    /// The A3a durable index: the highest record `seq` fsync'd by
    /// [`QLogSet::sync`] (`ALICE_PGLESS_NEWARCH.md` §5). The applier writes it to
    /// `meta::QLOG_DURABLE_INDEX` in the commit that follows the sync; recovery
    /// reconciles the reopened qlog's durable tail against it.
    pub fn durable_seq(&self) -> u64 {
        self.durable_seq
    }

    /// The stable per-queue id: `xxh3_64(tenant ␟ queue)`, the two-name twin of
    /// `planner::bucket_of`'s `xxh3(tenant ␟ queue ␟ partition)`. Deterministic
    /// and collision-free on the `0x1F` separator (it cannot occur inside a name
    /// segment). A1 has no catalog-assigned queue id — queues are keyed by their
    /// `(tenant, queue)` names throughout — so this hash IS the id; a later
    /// phase may replace it with a catalog id without changing any record's
    /// bytes.
    ///
    /// Phase C: `0` is [`SYSTEM_QUEUE_ID`], reserved for the system log, so a
    /// name whose hash is 0 (probability 2^-64) is remapped to 1.
    pub fn queue_id_of(tenant: &str, queue: &str) -> u64 {
        let mut buf = Vec::with_capacity(tenant.len() + queue.len() + 1);
        buf.extend_from_slice(tenant.as_bytes());
        buf.push(0x1F);
        buf.extend_from_slice(queue.as_bytes());
        match xxhash_rust::xxh3::xxh3_64(&buf) {
            SYSTEM_QUEUE_ID => 1,
            h => h,
        }
    }

    /// A cloneable reader over this set's per-queue logs, for the pop payload
    /// read (facade) and the `DEDUP_INDEX=segment` dedup read (planner). It
    /// shares the applier's live logs, so it sees every append the moment
    /// [`QLogSet::flush`] lands it — the qlog twin of `Segments::reader()`. It
    /// also carries the set's recovery-floor handle (Phase C), so whoever holds a
    /// reader can raise the floor after a durable point.
    pub fn reader(&self) -> QLogReader {
        QLogReader {
            logs: self.logs.clone(),
            floor: self.floor.clone(),
            totals: self.totals.clone(),
            reclaim_queue_cursor: self.reclaim_queue_cursor.clone(),
            reclaim_paused: self.reclaim_paused.clone(),
            root: self.root.clone(),
        }
    }

    /// Reopen every existing `q<id>/` directory under the root (A2 recovery), so
    /// a reopened applier serves reads from the qlog before it writes anything.
    /// Each is [`QLog::open`] — torn-tail truncate on the active file, `.qidx`
    /// rebuild on the sealed files. A missing root (a node that never turned the
    /// knob on) is not an error: there is simply nothing to reopen.
    ///
    /// Returns the qlog's DURABLE TAIL (A3a, `ALICE_PGLESS_NEWARCH.md` §5): the
    /// highest record `seq` that survived recovery across every queue. The
    /// applier reconciles it against the store's `meta::QLOG_DURABLE_INDEX`, and
    /// this seeds `written_seq`/`durable_seq` so the next commit's recorded index
    /// never goes backwards over the reopened tail.
    pub fn reopen_all(&mut self) -> io::Result<u64> {
        let rd = match std::fs::read_dir(&self.root) {
            Ok(rd) => rd,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };
        let mut ids: Vec<u64> = Vec::new();
        for entry in rd {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(rest) = name.strip_prefix('q') {
                if let Ok(id) = rest.parse::<u64>() {
                    ids.push(id);
                }
            }
        }
        ids.sort_unstable();
        let mut tail = 0u64;
        for id in ids {
            // Only the apply thread mutates the map (this call is at open, before
            // any reader thread exists), so a check-then-open-then-insert is
            // race-free; open OUTSIDE the map lock so a reader never blocks on
            // the recovery scan of a queue it is not asking about.
            if self
                .logs
                .read()
                .expect("qlog set poisoned")
                .contains_key(&id)
            {
                continue;
            }
            let (mut log, rec) = QLog::open(&self.root, id, self.opts)?;
            log.set_recovery_floor(self.floor.clone());
            tail = tail.max(rec.max_seq);
            self.logs
                .write()
                .expect("qlog set poisoned")
                .insert(id, Arc::new(RwLock::new(log)));
        }
        // The reopened tail is on the platter (it was fsync'd at or before the
        // last commit), so the next commit's recorded durable index starts here,
        // never below it.
        self.written_seq = self.written_seq.max(tail);
        self.durable_seq = self.durable_seq.max(tail);
        self.recompute_totals();
        Ok(tail)
    }

    /// Phase C recovery: every [`crate::rsm::qlog::record::REC_ENTRY`] record of
    /// EVERY open log (the system log included) with `seq >= from_seq`, MERGED by
    /// `seq`, DE-DUPLICATED (an entry is written into every queue log it touches),
    /// and handed to `cb` in strictly increasing `seq` order — the replay the
    /// raft log's `scan_from` used to be. Call it after [`QLogSet::reopen_all`],
    /// which already truncated every torn tail (the payload reopen's rule); a
    /// damaged record in a sealed file is surfaced as corruption.
    ///
    /// It delivers only the DURABLE PREFIX: starting at `from_seq`, it stops at
    /// the first seq that is missing (a gap) or incomplete (fewer than `copies`
    /// distinct logs hold it). An entry is acknowledged only after the fsync of
    /// EVERY log its group touched, and groups are written one after another, so
    /// a missing or incomplete entry means its group's fsyncs did not all land:
    /// nothing of that group was acknowledged and no later group was written.
    /// Stopping there is exactly the raft log's torn-tail truncation; the caller
    /// then drops everything at or above [`EntryScan::next_seq`] with
    /// [`QLogSet::truncate_from`] before any seq is reused. Two copies of one
    /// seq that disagree, a seq appearing twice in one log, or more copies than
    /// the record claims are refused (never guessed).
    ///
    /// Boot-time only: it reads every log's candidate files and holds the
    /// entries since `from_seq` in memory (one durable interval's worth).
    pub fn scan_entries(
        &self,
        from_seq: u64,
        cb: &mut dyn FnMut(EntryRecord) -> io::Result<()>,
    ) -> io::Result<EntryScan> {
        struct Agg {
            rec: EntryRecord,
            found: u32,
        }
        // Entry indexes start at 1.
        let from_seq = from_seq.max(1);
        let logs: Vec<(u64, Arc<RwLock<QLog>>)> = self
            .logs
            .read()
            .expect("qlog set poisoned")
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        let mut merged: BTreeMap<u64, Agg> = BTreeMap::new();
        for (qid, log) in logs {
            let recs = log
                .read()
                .expect("qlog poisoned")
                .entry_records_from(from_seq)?;
            let mut prev: Option<u64> = None;
            for r in recs {
                if prev.is_some_and(|p| r.seq <= p) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "qlog q{qid}: entry record seq {} follows seq {} (a queue log's \
                             entry seqs must strictly ascend)",
                            r.seq,
                            prev.unwrap_or(0)
                        ),
                    ));
                }
                prev = Some(r.seq);
                match merged.entry(r.seq) {
                    std::collections::btree_map::Entry::Vacant(v) => {
                        v.insert(Agg { rec: r, found: 1 });
                    }
                    std::collections::btree_map::Entry::Occupied(mut o) => {
                        let a = o.get_mut();
                        if a.rec != r {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "qlog q{qid}: the copies of entry {} disagree across the \
                                     queue logs",
                                    r.seq
                                ),
                            ));
                        }
                        a.found += 1;
                    }
                }
            }
        }
        let mut out = EntryScan {
            next_seq: from_seq,
            max_seq_found: merged.keys().next_back().copied().unwrap_or(0),
            ..EntryScan::default()
        };
        let total = merged.len() as u64;
        for (seq, a) in merged {
            if seq != out.next_seq {
                out.stopped = Some(format!(
                    "entry {} is missing from every queue log (next found: {seq})",
                    out.next_seq
                ));
                break;
            }
            if a.found > a.rec.copies {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "entry {seq} is in {} queue logs but was written to {}",
                        a.found, a.rec.copies
                    ),
                ));
            }
            if a.found < a.rec.copies {
                out.stopped = Some(format!(
                    "entry {seq} is incomplete: {} of its {} copies survived",
                    a.found, a.rec.copies
                ));
                break;
            }
            cb(a.rec)?;
            out.delivered += 1;
            out.next_seq += 1;
        }
        out.discarded = total - out.delivered;
        Ok(out)
    }

    /// Phase C recovery: drop every record with `seq >= cut` from every open
    /// log ([`QLog::truncate_seq_from`]) — the unacknowledged tail
    /// [`QLogSet::scan_entries`] stopped before — durably, before the writer
    /// reuses those seqs. Returns the bytes dropped.
    pub fn truncate_from(&mut self, cut: u64) -> io::Result<u64> {
        let logs: Vec<Arc<RwLock<QLog>>> = self
            .logs
            .read()
            .expect("qlog set poisoned")
            .values()
            .cloned()
            .collect();
        let mut dropped = 0u64;
        for log in logs {
            dropped += log.write().expect("qlog poisoned").truncate_seq_from(cut)?;
        }
        self.recompute_totals();
        Ok(dropped)
    }

    /// A Raft truncation of every log at `cut`: [`QLog::truncate_seq_from_across`]
    /// (a conflicting suffix or the tail above a snapshot may sit in a sealed
    /// file). Returns the bytes dropped.
    pub fn truncate_from_across(&mut self, cut: u64) -> io::Result<u64> {
        let logs: Vec<Arc<RwLock<QLog>>> = self
            .logs
            .read()
            .expect("qlog set poisoned")
            .values()
            .cloned()
            .collect();
        let mut dropped = 0u64;
        for log in logs {
            dropped += log
                .write()
                .expect("qlog poisoned")
                .truncate_seq_from_across(cut)?;
        }
        self.recompute_totals();
        Ok(dropped)
    }

    /// Recount `(files, bytes)` over every open log (boot / truncation only).
    fn recompute_totals(&mut self) {
        let (mut files, mut bytes) = (0u64, 0u64);
        for log in self.logs.read().expect("qlog set poisoned").values() {
            let g = log.read().expect("qlog poisoned");
            files += g.file_count() as u64;
            bytes += g.bytes();
        }
        self.totals.files.store(files, Ordering::Release);
        self.totals.bytes.store(bytes, Ordering::Release);
    }

    /// Run one write against `qid`'s log (opened lazily), keeping the running
    /// `(files, bytes)` totals current and marking the queue dirty for the next
    /// [`QLogSet::sync`].
    fn write_tracked(
        &mut self,
        qid: u64,
        write: impl FnOnce(&mut QLog) -> io::Result<()>,
    ) -> io::Result<()> {
        let log = self.get_or_open(qid)?;
        let mut g = log.write().expect("qlog poisoned");
        let (f0, b0) = (g.file_count() as u64, g.bytes());
        let res = write(&mut g);
        let (f1, b1) = (g.file_count() as u64, g.bytes());
        drop(g);
        replace_total(&self.totals.files, f0, f1);
        replace_total(&self.totals.bytes, b0, b1);
        res?;
        self.dirty.insert(qid);
        Ok(())
    }

    /// Buffer one `Append` for its queue (a shadow of the `segments.append`
    /// that just ran). Copies the bytes so the record can outlive the entry.
    /// `seq` is the entry index (the leader's global order stamp).
    #[allow(clippy::too_many_arguments)]
    pub fn buffer(
        &mut self,
        tenant: &str,
        queue: &str,
        seq: u64,
        pid: u64,
        base_offset: u64,
        count: u32,
        created_at_us: i64,
        hashes: &[u8],
        payload: &[u8],
    ) {
        let qid = Self::queue_id_of(tenant, queue);
        self.pending.entry(qid).or_default().push(Buffered {
            seq,
            pid,
            base_offset,
            count,
            created_at_us,
            hashes: hashes.to_vec(),
            payload: payload.to_vec(),
        });
    }

    /// Drain every queue's buffer into ONE [`QLog::write_group`] per queue — one
    /// `write` each, NO fsync — opening a genuinely new queue's log lazily. Apply
    /// calls it at the END of every entry (right after `segments.flush_writes`,
    /// before the leader answers), so a record is page-cache readable the moment
    /// the entry is applied — which is what a pop rendered right after apply, and
    /// a planner claim over committed state, both need (the A2 read invariant).
    /// The fsync is deferred to [`QLogSet::sync`] at the durable point, exactly as
    /// the segment path fsyncs per durable point, not per entry.
    ///
    /// A group is taken out of `pending` BEFORE the (fallible) open/write, so a
    /// mid-flush I/O error does not leave a half-written group buffered for a
    /// retry: the applier poisons and is dropped, and the shadow log is reopened
    /// (torn tail truncated) on restart.
    pub fn flush(&mut self) -> io::Result<()> {
        let qids: Vec<u64> = self
            .pending
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .map(|(k, _)| *k)
            .collect();
        for qid in qids {
            let bufs = std::mem::take(self.pending.get_mut(&qid).expect("pending queue present"));
            // The highest `seq` in this group advances the A3a written watermark
            // (the buffer is filled in apply order, so the last is the highest).
            let group_max_seq = bufs.iter().map(|b| b.seq).max().unwrap_or(0);
            let inputs: Vec<RecordInput<'_>> = bufs
                .iter()
                .map(|b| RecordInput {
                    seq: b.seq,
                    pid: b.pid,
                    base_offset: b.base_offset,
                    count: b.count,
                    created_at_us: b.created_at_us,
                    txn: None,
                    hashes: &b.hashes,
                    payload: &b.payload,
                })
                .collect();
            self.write_tracked(qid, |log| log.write_group(&inputs).map(|_| ()))?;
            self.written_seq = self.written_seq.max(group_max_seq);
        }
        Ok(())
    }

    /// Write one queue's group of records directly (one `write`, NO fsync),
    /// opening the queue's log lazily — the log-native WRITER path
    /// (`ALICE_PGLESS_NEWARCH.md` A3b). The writer thread keys by `queue_id` (it
    /// holds a `pid -> queue_id` map, not the queue names the [`QLogSet::buffer`]
    /// applier path carries), groups a written raft-log group's `Append`s by
    /// queue, and calls this once per touched queue BEFORE it fsyncs — then
    /// [`QLogSet::sync`] makes them durable, and only THEN is the referencing
    /// raft-log entry written and fsynced (the payload-before-entry ordering that
    /// keeps a committed entry from ever pointing at a missing payload). `records`
    /// carry `seq` = the entry index (the leader's order stamp), exactly as the
    /// applier path's buffer did. Marks the queue dirty for the next `sync` and
    /// advances the written watermark.
    pub fn write_group_for_qid(&mut self, qid: u64, records: &[RecordInput<'_>]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let group_max_seq = records.iter().map(|r| r.seq).max().unwrap_or(0);
        self.write_tracked(qid, |log| log.write_group(records).map(|_| ()))?;
        self.written_seq = self.written_seq.max(group_max_seq);
        Ok(())
    }

    /// Phase C: [`QLogSet::write_group_for_qid`] for a MIXED group — each
    /// entry's payload records followed by its payload-free entry record
    /// ([`QLog::write_mixed`]), one `write`, NO fsync. `qid` may be
    /// [`SYSTEM_QUEUE_ID`]. Marks the queue dirty, so the next
    /// [`QLogSet::sync`] fsyncs it even when the group carried no message (a
    /// pop / ack / create entry is acknowledged only after that fsync too).
    pub fn write_mixed_for_qid(&mut self, qid: u64, records: &[WriteRecord<'_>]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let group_max_seq = records.iter().map(|r| r.seq()).max().unwrap_or(0);
        self.write_tracked(qid, |log| log.write_mixed(records).map(|_| ()))?;
        self.written_seq = self.written_seq.max(group_max_seq);
        Ok(())
    }

    /// Fsync every queue written since the last sync (§5: a durable point leaves
    /// every buffered record fsync'd). One fsync per touched queue, batched across
    /// the entries since the last durable point — the qlog twin of the segment
    /// durable point, not a per-entry barrier. Apply calls it at the durable point
    /// only.
    pub fn sync(&mut self) -> io::Result<()> {
        let dirty = std::mem::take(&mut self.dirty);
        // READ-PARALLELISM FIX: the fsync used to run under the queue's WRITE
        // lock (`log.write().sync()`), which froze all of a hot queue's pop
        // readers for the fsync's whole duration (FAT100: 64 consumers starved
        // → 34k/s, 2M backlog). Instead: clone the active fd under a brief READ
        // lock (concurrent with the pop reads), release, and fsync OUTSIDE any
        // lock. The fsync flushes the inode regardless of the fd; the single
        // writer never rolls between the clone and the fsync, so the clone is
        // the current active file. Pop reads now proceed during the fsync.
        let mut handles: Vec<std::fs::File> = Vec::with_capacity(dirty.len());
        for qid in dirty {
            let arc = self
                .logs
                .read()
                .expect("qlog set poisoned")
                .get(&qid)
                .cloned();
            if let Some(log) = arc {
                if let Some(f) = log.read().expect("qlog poisoned").active_clone()? {
                    handles.push(f);
                }
            }
        }
        // The dirty logs are independent files: fsync them CONCURRENTLY, so the
        // durable point costs the slowest fsync, not their sum (3 hot queues
        // made log_fsync 6 → 13.5 ms per group when they ran one after another).
        let mode = self.opts.fsync;
        match handles.as_slice() {
            [] => {}
            [one] => crate::rsm::qlog::fsync_file(one, mode)?,
            [first, rest @ ..] => std::thread::scope(|s| -> io::Result<()> {
                let joins: Vec<std::thread::ScopedJoinHandle<'_, io::Result<()>>> = rest
                    .iter()
                    .map(|f| s.spawn(move || crate::rsm::qlog::fsync_file(f, mode)))
                    .collect();
                let mut res = crate::rsm::qlog::fsync_file(first, mode);
                for j in joins {
                    let r = j
                        .join()
                        .unwrap_or_else(|_| Err(io::Error::other("qlog fsync thread panicked")));
                    if res.is_ok() {
                        res = r;
                    }
                }
                res
            })?,
        }
        self.durable_seq = self.written_seq;
        Ok(())
    }

    /// The open log for `qid`, opening it (and inserting it into the shared map)
    /// if it is new. The `QLog::open` runs OUTSIDE the map lock — only the apply
    /// thread writes the map, and a genuinely new queue has no committed offset a
    /// reader could be asking for yet, so no reader misses a claimable record
    /// during the open.
    fn get_or_open(&self, qid: u64) -> io::Result<Arc<RwLock<QLog>>> {
        if let Some(l) = self.logs.read().expect("qlog set poisoned").get(&qid) {
            return Ok(l.clone());
        }
        let (mut log, _rec) = QLog::open(&self.root, qid, self.opts)?;
        log.set_recovery_floor(self.floor.clone());
        let arc = Arc::new(RwLock::new(log));
        self.logs
            .write()
            .expect("qlog set poisoned")
            .insert(qid, arc.clone());
        Ok(arc)
    }

    /// Drop a deleted queue's log handle and any records still buffered for it
    /// (§NA-I5: a log for a dropped queue is GC'd). A1/A2 drop only the in-RAM
    /// handle and buffer; unlinking the on-disk `q<id>/` directory is retention
    /// (§3.3), a later phase.
    pub fn remove(&mut self, tenant: &str, queue: &str) {
        let qid = Self::queue_id_of(tenant, queue);
        if let Some(log) = self.logs.write().expect("qlog set poisoned").remove(&qid) {
            let log = log.read().expect("qlog poisoned");
            replace_total(&self.totals.files, log.file_count() as u64, 0);
            replace_total(&self.totals.bytes, log.bytes(), 0);
        }
        self.pending.remove(&qid);
        self.dirty.remove(&qid);
    }

    /// The open log for a queue id, for the read-match / reopen tests. Test-only:
    /// the product reads the shadow set through a [`QLogReader`].
    #[cfg(test)]
    pub fn log(&self, queue_id: u64) -> Option<Arc<RwLock<QLog>>> {
        self.logs
            .read()
            .expect("qlog set poisoned")
            .get(&queue_id)
            .cloned()
    }
}

/// A cloneable, `Send + Sync` reader over an applier's per-queue logs (Phase
/// A2). The facade clones one for each blocking pop render; the batcher clones
/// one for the planner's `DEDUP_INDEX=segment` dedup read. Every method keys by
/// `queue_id` ([`QLogSet::queue_id_of`]) and reads under the queue's read lock,
/// off the SAME live logs the applier appends to.
#[derive(Clone)]
pub struct QLogReader {
    logs: SharedLogs,
    /// The set's recovery floor (Phase C), shared: see
    /// [`QLogReader::set_recovery_floor`].
    floor: Arc<AtomicU64>,
    /// Running qlog sizes, shared with the writer so copy-forward retention is
    /// visible to the replicator metrics without waiting for another append.
    totals: Arc<SharedTotals>,
    reclaim_queue_cursor: Arc<AtomicU64>,
    /// Shared with the set: see [`QLogReader::pause_reclaim`].
    reclaim_paused: Arc<AtomicU64>,
    /// `<data_dir>/qlog`, for a snapshot that links the files.
    root: PathBuf,
}

/// Retention stays paused while one of these is alive.
pub struct ReclaimPause(Arc<AtomicU64>);

impl Drop for ReclaimPause {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl QLogReader {
    /// Pause retention (no file is unlinked or rewritten) until the guard is
    /// dropped: a snapshot links every file and needs them to stay put. A
    /// retention step already running finishes its one file first.
    pub fn pause_reclaim(&self) -> ReclaimPause {
        self.reclaim_paused.fetch_add(1, Ordering::AcqRel);
        ReclaimPause(self.reclaim_paused.clone())
    }

    /// `<data_dir>/qlog`.
    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    /// Phase C: raise the queue logs' RECOVERY FLOOR to `durable_index` — the
    /// index the store's last durable point covers. The queue logs are the only
    /// write-ahead log, so recovery replays every entry above the store's
    /// durable index FROM them; retention ([`QLog::unlink_dead_files`]) must never
    /// delete a file that holds any record above it. Call it (on the apply
    /// thread) after each durable point has landed; monotone (`fetch_max`), so a
    /// stale or repeated value is harmless. The `LocalReplicator` already does
    /// this from `Notify::durable` and seeds it at open, so an apply-side call
    /// is belt-and-braces, never a conflict.
    pub fn set_recovery_floor(&self, durable_index: u64) {
        self.floor.fetch_max(durable_index, Ordering::AcqRel);
    }

    /// The current recovery floor (`0` until the first durable point / open).
    pub fn recovery_floor(&self) -> u64 {
        self.floor.load(Ordering::Acquire)
    }

    /// `(files, valid bytes)` across every open log.
    pub fn totals(&self) -> (u64, u64) {
        (
            self.totals.files.load(Ordering::Acquire),
            self.totals.bytes.load(Ordering::Acquire),
        )
    }

    /// The shared floor handle itself, for a caller that wants to hold it.
    pub fn recovery_floor_handle(&self) -> Arc<AtomicU64> {
        self.floor.clone()
    }

    /// Node-local retention for one queue. The committed RSM watermarks are
    /// supplied by the leader maintenance read. Work is bounded by file count;
    /// `try_write` ensures GC never queues in front of an append already using
    /// this queue.
    pub(crate) fn reclaim_below_txns(
        &self,
        queue_id: u64,
        txns_starts: &std::collections::HashMap<u64, u64>,
        max_files: usize,
    ) -> io::Result<ReclaimProgress> {
        if self.reclaim_paused.load(Ordering::Acquire) > 0 {
            return Ok(ReclaimProgress {
                more: true,
                ..ReclaimProgress::default()
            });
        }
        match self.log(queue_id) {
            Some(log) => {
                let Ok(mut log) = log.try_write() else {
                    return Ok(ReclaimProgress {
                        more: true,
                        ..ReclaimProgress::default()
                    });
                };
                let (files_before, bytes_before) = (log.file_count() as u64, log.bytes());
                let result = log.unlink_below_txns_bounded(txns_starts, max_files);
                let (files_after, bytes_after) = (log.file_count() as u64, log.bytes());
                replace_total(&self.totals.files, files_before, files_after);
                replace_total(&self.totals.bytes, bytes_before, bytes_after);
                result
            }
            None => Ok(ReclaimProgress::default()),
        }
    }

    pub(crate) fn log_ids(&self) -> Vec<u64> {
        self.logs
            .read()
            .expect("qlog set poisoned")
            .keys()
            .copied()
            .collect()
    }

    pub(crate) fn reclaim_queue_cursor(&self) -> u64 {
        self.reclaim_queue_cursor.load(Ordering::Acquire)
    }

    pub(crate) fn advance_reclaim_queue_cursor(&self, queue_id: u64) {
        self.reclaim_queue_cursor
            .store(queue_id.wrapping_add(1), Ordering::Release);
    }

    /// The consensus-log read below the openraft storage's in-memory window:
    /// every COMPLETE entry record with `from_seq <= seq < end_seq`, merged
    /// across every queue log (the system log included) in `seq` order — the
    /// live-read twin of [`QLogSet::scan_entries`]. It stops at the first seq
    /// that is missing or incomplete; the caller decides whether a short answer
    /// is an error. Reads every candidate file of every log: a rare path (a
    /// follower catching up from before this node's window), never the hot one.
    pub fn entry_records_range(
        &self,
        from_seq: u64,
        end_seq: u64,
    ) -> io::Result<Vec<(EntryRecord, Vec<u64>)>> {
        let from_seq = from_seq.max(1);
        if from_seq >= end_seq {
            return Ok(Vec::new());
        }
        let logs: Vec<(u64, Arc<RwLock<QLog>>)> = self
            .logs
            .read()
            .expect("qlog set poisoned")
            .iter()
            .map(|(qid, l)| (*qid, l.clone()))
            .collect();
        // seq -> (the record, the queue logs holding a copy of it)
        let mut merged: BTreeMap<u64, (EntryRecord, Vec<u64>)> = BTreeMap::new();
        for (qid, log) in logs {
            let recs = log
                .read()
                .expect("qlog poisoned")
                .entry_records_between(from_seq, end_seq)?;
            for r in recs {
                match merged.entry(r.seq) {
                    std::collections::btree_map::Entry::Vacant(v) => {
                        v.insert((r, vec![qid]));
                    }
                    std::collections::btree_map::Entry::Occupied(mut o) => {
                        if o.get().0 != r {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "the copies of entry {} disagree across the queue logs",
                                    r.seq
                                ),
                            ));
                        }
                        o.get_mut().1.push(qid);
                    }
                }
            }
        }
        let mut out = Vec::with_capacity(merged.len());
        let mut next = from_seq;
        for (seq, (rec, qids)) in merged {
            if seq != next || (qids.len() as u32) < rec.copies {
                break;
            }
            out.push((rec, qids));
            next += 1;
        }
        Ok(out)
    }

    /// The open log for `queue_id`, or `None` when the applier has never written
    /// (or has removed) that queue. Clones the inner `Arc` so the caller reads
    /// without holding the map lock across the read.
    fn log(&self, queue_id: u64) -> Option<Arc<RwLock<QLog>>> {
        self.logs
            .read()
            .expect("qlog set poisoned")
            .get(&queue_id)
            .cloned()
    }

    /// The whole record holding `(pid, offset)` — the pop payload read. `None`
    /// when the queue is unknown or no live file holds the offset (a gap the pop
    /// render skips, exactly as a segment `read_at_within` miss).
    pub fn read_owned(
        &self,
        queue_id: u64,
        pid: u64,
        offset: u64,
    ) -> io::Result<Option<OwnedRecord>> {
        match self.log(queue_id) {
            Some(l) => l.read().expect("qlog poisoned").read_owned(pid, offset),
            None => Ok(None),
        }
    }

    /// The O(claimed) forward claim walk of `pid` from `from_offset`, bounded to
    /// `committed_end` (the exactly-once invariant), for the dedup delivered set
    /// / resolve. The qlog twin of `segments::Reader::claim_frames`. A no-op when
    /// the queue is unknown.
    #[allow(clippy::too_many_arguments)]
    pub fn claim_frames(
        &self,
        queue_id: u64,
        pid: u64,
        from_offset: u64,
        committed_end: u64,
        want_hashes: bool,
        cb: &mut dyn FnMut(u64, u64, i64, Option<Vec<u8>>) -> bool,
    ) -> io::Result<()> {
        match self.log(queue_id) {
            Some(l) => l.read().expect("qlog poisoned").claim_walk(
                pid,
                from_offset,
                committed_end,
                want_hashes,
                cb,
            ),
            None => Ok(()),
        }
    }

    /// The committed dedup rows (or shape, when `!with_hashes`) of `pid` from
    /// `from_base`, bounded to `committed_end` — the dedup probe / resolve / seed
    /// authority. The qlog twin of `segments::Reader::committed_dedup_rows` /
    /// `committed_dedup_shape`. Empty when the queue is unknown.
    ///
    /// PLAN_RAFT_DRAIN_FIX P3: the index walk + cache probes run under the
    /// queue's read lock; the misses' hashes-only `pread`s run AFTER it is
    /// released (committed records are immutable, each miss holds its fd), so a
    /// whole-window read never holds the applier's append behind its I/O.
    pub fn committed_frames(
        &self,
        queue_id: u64,
        pid: u64,
        from_base: u64,
        committed_end: u64,
        with_hashes: bool,
    ) -> io::Result<Vec<CommittedFrame>> {
        match self.log(queue_id) {
            Some(l) => {
                let plan = l.read().expect("qlog poisoned").committed_frames_plan(
                    pid,
                    from_base,
                    committed_end,
                    with_hashes,
                )?;
                Ok(super::committed_of(plan.finish()?))
            }
            None => Ok(Vec::new()),
        }
    }

    /// PLAN_RAFT_DRAIN_FIX P3.1: the committed records of `pid` overlapping any
    /// of the dedup front's `(min_base, max_off)` bands, bounded to
    /// `committed_end`, with their hash blocks — see
    /// [`QLog::committed_hashes_in_bands`]. Same lock split as
    /// [`QLogReader::committed_frames`]. Empty when the queue is unknown.
    pub fn committed_hashes_in_bands(
        &self,
        queue_id: u64,
        pid: u64,
        bands: &[(u64, u64)],
        committed_end: u64,
    ) -> io::Result<Vec<BandFrame>> {
        match self.log(queue_id) {
            Some(l) => {
                let plan = l
                    .read()
                    .expect("qlog poisoned")
                    .band_plan(pid, bands, committed_end)?;
                plan.finish()
            }
            None => Ok(Vec::new()),
        }
    }

    /// `queue_id` for a `(tenant, queue)`, so a caller that holds the names can
    /// key the reads without reaching for [`QLogSet`].
    pub fn queue_id_of(tenant: &str, queue: &str) -> u64 {
        QLogSet::queue_id_of(tenant, queue)
    }

    /// Whether the reader has an open log for `queue_id` — for the reopen test,
    /// which asserts a restart rebuilt the map.
    #[cfg(test)]
    pub fn has_queue(&self, queue_id: u64) -> bool {
        self.logs
            .read()
            .expect("qlog set poisoned")
            .contains_key(&queue_id)
    }
}
