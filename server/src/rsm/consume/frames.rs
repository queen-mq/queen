//! A partition's segments as the claim and the ack read them: `(base, end,
//! created_at, hashes)` per `Append`, from the committed dedup authority.
//!
//! Under `QUEEN_RAFT_DEDUP_INDEX=txns` (the product default) and `rows` the
//! authority is the `txns` keyspace: one row per append, keyed `(pid, base)`,
//! written by apply before the append's hook runs (every keyspace is a RAM
//! table read LIVE, so what apply wrote is visible at once). Under `segment`
//! no `txns` row exists: the frames come from the queue log (or the segment
//! files) through the readers the facade installs
//! ([`super::Engine::set_segment_readers`]).
//!
//! # Messages without a row (catalogue version 6)
//!
//! A `txns` row leaves the store when it leaves its queue's txns window, even
//! while its message is retained (`rows_start` on the partition row). So a
//! walk can meet offsets that are retained — at or past `log_start` — and have
//! no row: the queue log's own index answers for them
//! ([`Frames::walk`]). Two rules keep that exact:
//!
//! - **A missing row is never taken for a retention gap.** The rows of one
//!   partition are gapless, so a walk checks that each row begins where the
//!   last one ended. Where one does not, it reads the partition row — AFTER
//!   the scan, so a row that expired under it shows — and reads `[gap,
//!   rows_start)` from the queue log. Apply moves `rows_start` before it
//!   deletes the rows, so whatever a reader finds missing is below the
//!   `rows_start` it reads afterwards.
//! - **A reader that finds every row pays nothing for this**: the walk of a
//!   consumer at the tail is the same two scans it always was.
//!
//! The queue log is a file: a walk under the consume shard lock does not read
//! it ([`Frames::defer_cold`]). It notes what it needed ([`ColdNeed`]), the
//! pop reads those frames with no lock held ([`Frames::preload`]) and claims
//! again.

use std::cell::RefCell;
use std::collections::HashMap;

use crate::rsm::dedup::{AckRes, IndexMode, TXNS_HEADER_LEN, TXNS_LEN_TAIL};
use crate::rsm::effect::Pid;
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::segments;
use crate::rsm::store::{keys, Keyspace, Reads, StoreError, TypedReads};

type Result<T> = std::result::Result<T, StoreError>;

/// The walk's callback: `(base, end, created_at, hashes)`; `false` stops.
pub(crate) type WalkFn<'c> = dyn FnMut(u64, u64, i64, Option<&[u8]>) -> bool + 'c;

/// One append as a claim sees it.
#[derive(Clone, Debug)]
pub(crate) struct Seg {
    pub base: u64,
    /// Inclusive.
    pub end: u64,
    pub created_at_us: i64,
    /// Per frame, in frame order (when asked for).
    pub hashes: Option<Vec<[u8; 16]>>,
}

/// The segment-authority readers (`DEDUP_INDEX=segment`).
#[derive(Clone)]
pub(crate) struct SegSource {
    pub reader: segments::Reader,
    pub qlog: Option<QLogReader>,
}

/// What a walk needed from the queue log and was not allowed to read
/// ([`Frames::defer_cold`]): the frames of `pid` from `from`, which have no
/// row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ColdNeed {
    pub pid: Pid,
    pub from: u64,
}

/// One append read ahead from the queue log ([`Frames::preload`]).
struct ColdSeg {
    base: u64,
    /// Inclusive.
    end: u64,
    created_at_us: i64,
    hashes: Option<Vec<u8>>,
}

/// The frames of one partition read ahead: contiguous from `from`.
struct ColdReady {
    from: u64,
    hashes: bool,
    segs: Vec<ColdSeg>,
}

#[derive(Default)]
pub(crate) struct ColdState {
    need: Option<ColdNeed>,
    ready: HashMap<Pid, ColdReady>,
}

/// The most messages a hash ack searches in the queue log for a hash no row
/// holds ([`Frames::resolve`]): a lease-less ack names no batch end.
const UNROWED_ACK_SCAN: u64 = 65_536;

/// What the searches of one span `[a, b)` of a partition's messages without
/// rows have read so far: every hash up to `next`, with its lowest offset.
struct UnrowedScan {
    pid: Pid,
    a: u64,
    b: u64,
    /// The first offset not read yet (`b` once the span is read whole).
    next: u64,
    seen: HashMap<[u8; 16], u64>,
}

/// How far below the cursor the search for an ack already taken reaches, one
/// span after the other ([`Frames::acked_unrowed`]).
const UNROWED_ACK_REACH: [u64; 3] = [1_024, 8_192, UNROWED_ACK_SCAN];

/// How many spans one [`Frames`] remembers: an ack searches one above its
/// cursor and up to [`UNROWED_ACK_REACH`]'s three below it, per target.
const UNROWED_SCANS: usize = 4;

/// The frame reads of one store read transaction.
pub(crate) struct Frames<'a, R: Reads + ?Sized> {
    pub r: &'a R,
    pub seg: Option<&'a SegSource>,
    pub mode: IndexMode,
    /// Do not read the queue log in a walk (the caller holds the consume
    /// shard lock): note the need ([`Frames::take_cold_need`]) and stop.
    pub defer_cold: bool,
    pub cold: RefCell<ColdState>,
    /// What the hash searches among the messages without rows have read
    /// ([`Frames::find_unrowed`]).
    scans: RefCell<Vec<UnrowedScan>>,
}

/// What a partition says of its rows and its log, read in one go.
struct PartFacts {
    log_start: u64,
    /// Where the rows begin (`rows_start`, never below `txns_start`).
    rows_at: u64,
    /// One past the tail.
    end: u64,
    queue_id: u64,
}

/// How a walk goes on after the offsets that have no row.
enum Unrowed {
    /// The walk is over: the callback stopped it, nothing is there, or the
    /// queue log was needed and deferred.
    Stop,
    /// The rows take over at this offset.
    Rows(u64),
}

/// A stored `txns` row, borrowed: `(end, created_at, hash bytes)`.
fn txns_row(v: &[u8]) -> Option<(u64, i64, &[u8])> {
    if v.len() < TXNS_HEADER_LEN {
        return None;
    }
    let body = v.len() - TXNS_HEADER_LEN;
    let hashes_end = match body % 16 {
        0 => v.len(),
        TXNS_LEN_TAIL => v.len() - TXNS_LEN_TAIL,
        _ => return None,
    };
    Some((
        u64::from_le_bytes(v[0..8].try_into().ok()?),
        i64::from_le_bytes(v[8..16].try_into().ok()?),
        &v[TXNS_HEADER_LEN..hashes_end],
    ))
}

fn hashes_of(b: &[u8]) -> Vec<[u8; 16]> {
    b.chunks_exact(16)
        .map(|c| <[u8; 16]>::try_from(c).expect("16 bytes"))
        .collect()
}

impl<'a, R: Reads + ?Sized> Frames<'a, R> {
    /// Whether this node can read the frames at all (a `segment` authority
    /// with no readers installed cannot: nothing is claimed, and nothing is
    /// sealed on the strength of a read that could not see the data).
    pub fn available(&self) -> bool {
        self.mode != IndexMode::Segment || self.seg.is_some()
    }

    pub fn new(r: &'a R, seg: Option<&'a SegSource>, mode: IndexMode) -> Frames<'a, R> {
        Frames {
            r,
            seg,
            mode,
            defer_cold: false,
            cold: RefCell::new(ColdState::default()),
            scans: RefCell::new(Vec::new()),
        }
    }

    /// What the last walk needed from the queue log and did not read
    /// ([`Frames::defer_cold`]). Whatever that walk handed over is PARTIAL:
    /// the caller drops it, reads the frames off the lock
    /// ([`Frames::preload`]) and walks again.
    pub fn take_cold_need(&self) -> Option<ColdNeed> {
        self.cold.borrow_mut().need.take()
    }

    /// Frames of `pid` were read ahead ([`Frames::preload`]).
    pub fn has_ready(&self, pid: Pid) -> bool {
        self.cold.borrow().ready.contains_key(&pid)
    }

    /// Drop what was read ahead for `pid`: its claim was made (or given up).
    pub fn forget_ready(&self, pid: Pid) {
        self.cold.borrow_mut().ready.remove(&pid);
    }

    fn facts(&self, pid: Pid) -> Result<Option<PartFacts>> {
        self.r.partition_with(pid, |p| PartFacts {
            log_start: p.head.log_start,
            rows_at: p.head.rows_start.max(p.head.txns_start),
            end: (p.head.last_offset + 1).max(0) as u64,
            queue_id: QLogReader::queue_id_of(p.tenant, p.queue),
        })
    }

    fn qlog(&self) -> Result<&QLogReader> {
        self.seg.and_then(|s| s.qlog.as_ref()).ok_or_else(|| {
            StoreError::Io(
                "messages without a row need the queue log, and this node has no reader for it"
                    .into(),
            )
        })
    }

    /// Read ahead, with no lock held, the frames of `pid` that have no row,
    /// from `from`: enough for `budget` messages. The next walk of this
    /// handle serves them without touching the queue log.
    pub fn preload(&self, pid: Pid, from: u64, budget: i64, want_hashes: bool) -> Result<()> {
        if self.mode == IndexMode::Segment {
            return Ok(());
        }
        self.cold.borrow_mut().ready.remove(&pid);
        let Some(f) = self.facts(pid)? else {
            return Ok(());
        };
        let lo = from.max(f.log_start);
        let to = f.rows_at.min(f.end);
        if lo >= to {
            // Nothing of it is without a row (any more): an empty read-ahead
            // says so, and the next walk goes straight to the rows.
            self.cold.borrow_mut().ready.insert(
                pid,
                ColdReady {
                    from: lo,
                    hashes: want_hashes,
                    segs: Vec::new(),
                },
            );
            return Ok(());
        }
        let q = self.qlog()?;
        let mut segs: Vec<ColdSeg> = Vec::new();
        let mut avail: i64 = 0;
        q.walk_frames(
            f.queue_id,
            pid,
            lo,
            to,
            want_hashes,
            &mut |base, end, created_at_us, hashes| {
                avail += end as i64 - base.max(lo) as i64 + 1;
                segs.push(ColdSeg {
                    base,
                    end,
                    created_at_us,
                    hashes,
                });
                avail < budget.max(1)
            },
        )
        .map_err(|e| StoreError::Io(format!("qlog claim walk: {e}")))?;
        self.cold.borrow_mut().ready.insert(
            pid,
            ColdReady {
                from: lo,
                hashes: want_hashes,
                segs,
            },
        );
        Ok(())
    }

    /// Walk the appends of `pid` forward, from the one covering `from`:
    /// `cb(base, end, created_at, hashes)`; `false` stops the walk.
    ///
    /// Every retained append from there on is handed over, in order and with
    /// no hole: from its `txns` row where it has one, from the queue log
    /// where it has none (the module note).
    pub fn walk(&self, pid: Pid, from: u64, want_hashes: bool, cb: &mut WalkFn<'_>) -> Result<()> {
        self.walk_from(pid, from, want_hashes, self.defer_cold, cb)
    }

    fn walk_from(
        &self,
        pid: Pid,
        from: u64,
        want_hashes: bool,
        defer: bool,
        cb: &mut WalkFn<'_>,
    ) -> Result<()> {
        if self.mode == IndexMode::Segment {
            return self.walk_segments(pid, from, want_hashes, cb);
        }
        let mut pos = from;
        let mut last_gap: Option<u64> = None;
        loop {
            // `None`: the rows served the walk to its end.
            let Some(at) = self.walk_rows(pid, pos, want_hashes, cb)? else {
                return Ok(());
            };
            match self.walk_unrowed(pid, at, want_hashes, defer, cb)? {
                Unrowed::Stop => return Ok(()),
                Unrowed::Rows(next) => {
                    // The partition says a row holds `next`, and a scan that
                    // ran after it said so found none: a store that lost a
                    // row is not walked around.
                    if last_gap == Some(next) {
                        return Err(StoreError::corrupt(
                            Keyspace::Txns,
                            format!("pid {pid}: no row holds offset {next}, at or past rows_start"),
                        ));
                    }
                    last_gap = Some(next);
                    pos = next;
                }
            }
        }
    }

    /// The rows' part of a walk, from the row covering `pos`. `None`: the
    /// walk is over (the callback stopped it, or the rows ran to their end
    /// with no hole). `Some(at)`: no row holds offset `at`, the first one the
    /// walk could not hand over.
    fn walk_rows(
        &self,
        pid: Pid,
        pos: u64,
        want_hashes: bool,
        cb: &mut WalkFn<'_>,
    ) -> Result<Option<u64>> {
        let prefix = keys::txns_prefix(pid);
        let mut start = pos;
        let mut bad = false;
        self.r.scan_rev_raw(
            Keyspace::Txns,
            &keys::txns(pid, pos),
            &prefix,
            1,
            &mut |k, _| {
                match keys::txns_base_of(k) {
                    Some(b) => start = b,
                    None => bad = true,
                }
                false
            },
        )?;
        if bad {
            return Err(StoreError::corrupt(Keyspace::Txns, "txns key"));
        }
        // Where the next row must begin: the row the probe found (or `pos`
        // itself when none lies at or below it), then one past each row.
        let mut expect = start;
        let mut delivered = false;
        let mut stopped = false;
        let mut gap: Option<u64> = None;
        self.r.scan_raw(
            Keyspace::Txns,
            &keys::txns(pid, start),
            &prefix,
            usize::MAX,
            &mut |k, v| match (keys::txns_base_of(k), txns_row(v)) {
                (Some(base), Some((end, created, hashes))) => {
                    if base != expect {
                        // A row is missing here: the one the probe found is
                        // gone already, or the next one is.
                        gap = Some(if delivered { expect } else { pos });
                        return false;
                    }
                    expect = end.saturating_add(1);
                    delivered = true;
                    let keep = cb(base, end, created, want_hashes.then_some(hashes));
                    stopped = !keep;
                    keep
                }
                _ => {
                    bad = true;
                    false
                }
            },
        )?;
        if bad {
            return Err(StoreError::corrupt(Keyspace::Txns, "txns row"));
        }
        if stopped {
            return Ok(None);
        }
        if gap.is_some() {
            return Ok(gap);
        }
        // The rows ran out. With none handed over, whatever lies at `pos` has
        // no row; the partition row says whether anything does.
        Ok((!delivered).then_some(pos))
    }

    /// The part of a walk no row serves: offset `at` has none. Hands over the
    /// retained appends of `[at, rows_start)` from the queue log (or from the
    /// frames read ahead), and says where the rows take over.
    fn walk_unrowed(
        &self,
        pid: Pid,
        at: u64,
        want_hashes: bool,
        defer: bool,
        cb: &mut WalkFn<'_>,
    ) -> Result<Unrowed> {
        // Read AFTER the scan that found the hole (the module note).
        let Some(f) = self.facts(pid)? else {
            return Ok(Unrowed::Stop);
        };
        if at >= f.end {
            return Ok(Unrowed::Stop);
        }
        if at >= f.rows_at {
            // The rows hold it now (an append that landed meanwhile): they
            // are read again; a second miss is the caller's error.
            return Ok(Unrowed::Rows(at));
        }
        // Below `log_start` nothing is retained: that hole is retention's.
        let lo = at.max(f.log_start);
        let to = f.rows_at.min(f.end);
        if lo >= to {
            return Ok(Unrowed::Rows(f.rows_at));
        }
        let mut next = lo;
        {
            let cold = self.cold.borrow();
            if let Some(ready) = cold.ready.get(&pid) {
                if ready.from == lo && (ready.hashes || !want_hashes) {
                    for s in &ready.segs {
                        if s.base > next || s.end < next || s.end >= to {
                            break;
                        }
                        let hashes = if want_hashes {
                            s.hashes.as_deref()
                        } else {
                            None
                        };
                        next = s.end + 1;
                        if !cb(s.base, s.end, s.created_at_us, hashes) {
                            return Ok(Unrowed::Stop);
                        }
                    }
                }
            }
        }
        if next >= to {
            return Ok(Unrowed::Rows(f.rows_at));
        }
        if defer {
            self.cold.borrow_mut().need = Some(ColdNeed { pid, from: lo });
            return Ok(Unrowed::Stop);
        }
        let q = self.qlog()?;
        let mut stopped = false;
        q.walk_frames(
            f.queue_id,
            pid,
            next,
            to,
            want_hashes,
            &mut |base, end, created, hashes| {
                next = end + 1;
                let keep = cb(base, end, created, hashes.as_deref());
                stopped = !keep;
                keep
            },
        )
        .map_err(|e| StoreError::Io(format!("qlog claim walk: {e}")))?;
        if stopped {
            return Ok(Unrowed::Stop);
        }
        if next < to {
            // Retention may have passed `next` since the facts were read (its
            // files went with it): the walk ends with what it handed over,
            // and the next one starts at the new log start.
            if self.facts(pid)?.is_none_or(|now| now.log_start > next) {
                return Ok(Unrowed::Stop);
            }
            // A retained message with neither a row nor a record: never
            // answered as "nothing there" (a claim would skip it).
            return Err(StoreError::Io(format!(
                "the queue log holds no record of pid {pid} at offset {next}: \
                 it is retained (log_start {}) and below rows_start {}",
                f.log_start, f.rows_at
            )));
        }
        Ok(Unrowed::Rows(f.rows_at))
    }

    fn walk_segments(
        &self,
        pid: Pid,
        from: u64,
        want_hashes: bool,
        cb: &mut WalkFn<'_>,
    ) -> Result<()> {
        let Some(src) = self.seg else {
            return Ok(());
        };
        let Some((end, bucket, qid)) = self.r.partition_with(pid, |p| {
            (
                (p.head.last_offset + 1).max(0) as u64,
                crate::rsm::planner::bucket_of(p.tenant, p.queue, p.partition),
                QLogReader::queue_id_of(p.tenant, p.queue),
            )
        })?
        else {
            return Ok(());
        };
        if end == 0 {
            return Ok(());
        }
        let mut shim = |base: u64, end_incl: u64, created: i64, hashes: Option<Vec<u8>>| {
            cb(base, end_incl, created, hashes.as_deref())
        };
        if let Some(q) = &src.qlog {
            return q
                .claim_frames(qid, pid, from, end, want_hashes, &mut shim)
                .map_err(|e| StoreError::Io(format!("qlog claim walk: {e}")));
        }
        let mut sealed: Vec<u32> = Vec::new();
        self.r.scan_partition_files(pid, usize::MAX, &mut |f| {
            sealed.push(f);
            true
        })?;
        src.reader
            .claim_frames(bucket, pid, from, end, &sealed, want_hashes, &mut shim)
            .map_err(|e| StoreError::Io(format!("segment claim walk: {e}")))
    }

    /// The appends a plain claim from `wanted` can touch: from the one covering
    /// `wanted`, stopping once `budget` frames are available from `wanted`, at
    /// the first append not yet `fresh` (delayed processing), or at `tail`.
    /// The hashes ride along when `want_hashes`.
    pub fn gather(
        &self,
        pid: Pid,
        wanted: u64,
        budget: i64,
        tail: i64,
        fresh: &dyn Fn(i64) -> bool,
        want_hashes: bool,
    ) -> Result<Vec<Seg>> {
        let mut out: Vec<Seg> = Vec::with_capacity(4);
        let mut avail: i64 = 0;
        self.walk(
            pid,
            wanted,
            want_hashes,
            &mut |base, end, created, hashes| {
                if tail >= 0 && base as i64 > tail {
                    return false;
                }
                let deferred = !fresh(created);
                let from = (base as i64).max(wanted as i64);
                if end as i64 >= from {
                    avail += end as i64 - from + 1;
                }
                out.push(Seg {
                    base,
                    end,
                    created_at_us: created,
                    hashes: hashes.map(hashes_of),
                });
                !(deferred || avail >= budget)
            },
        )?;
        Ok(out)
    }

    /// The newest append at or after `from` that is `fresh` (a conflating
    /// claim serves its last frame), bounded by `tail`. `deadline` is what
    /// `fresh` tests (`created <= deadline`; `None`: everything is fresh):
    /// the queue log's index is searched by it where the rows do not reach
    /// down to `from`.
    pub fn newest_fresh(
        &self,
        pid: Pid,
        from: u64,
        tail: i64,
        fresh: &dyn Fn(i64) -> bool,
        deadline: Option<i64>,
    ) -> Result<Option<Seg>> {
        if self.mode != IndexMode::Segment {
            // Backwards from the tail: the first fresh one is the newest.
            let prefix = keys::txns_prefix(pid);
            let top = if tail < 0 { 0 } else { tail as u64 };
            let mut found: Option<Seg> = None;
            let mut bad = false;
            // The rows reach down to `from` (one at or below it was seen).
            let mut reached = false;
            self.r.scan_rev_raw(
                Keyspace::Txns,
                &keys::txns(pid, top),
                &prefix,
                usize::MAX,
                &mut |k, v| match (keys::txns_base_of(k), txns_row(v)) {
                    (Some(base), Some((end, created, _))) => {
                        if base <= from {
                            reached = true;
                        }
                        if end < from {
                            return false;
                        }
                        if fresh(created) {
                            found = Some(Seg {
                                base,
                                end,
                                created_at_us: created,
                                hashes: None,
                            });
                            return false;
                        }
                        true
                    }
                    _ => {
                        bad = true;
                        false
                    }
                },
            )?;
            if bad {
                return Err(StoreError::corrupt(Keyspace::Txns, "txns row"));
            }
            if found.is_some() || reached {
                return Ok(found);
            }
            // No fresh row, and the rows stop above `from`: the retained
            // messages below them have none (the module note). The newest
            // fresh one of those is the last record of the queue log's index
            // below `rows_start` stamped at or before the deadline.
            let Some(f) = self.facts(pid)? else {
                return Ok(None);
            };
            let lo = from.max(f.log_start);
            let to = f.rows_at.min(f.end).min((tail + 1).max(0) as u64);
            if lo >= to {
                return Ok(None);
            }
            let before = deadline.map_or(i64::MAX, |d| d.saturating_add(1));
            let rec = self.qlog()?.record_before(f.queue_id, pid, to, before);
            return Ok(rec.filter(|r| r.end > lo).map(|r| Seg {
                base: r.base_offset,
                end: r.end - 1,
                created_at_us: r.created_at_us,
                hashes: None,
            }));
        }
        let mut found: Option<Seg> = None;
        self.walk(pid, from, false, &mut |base, end, created, _| {
            if tail >= 0 && base as i64 > tail {
                return false;
            }
            if fresh(created) {
                found = Some(Seg {
                    base,
                    end,
                    created_at_us: created,
                    hashes: None,
                });
            }
            true
        })?;
        Ok(found)
    }

    /// The created_at of the append holding offset `off` (the window buffer's
    /// newest segment, the delay of a head frame).
    pub fn created_at(&self, pid: Pid, off: u64) -> Result<Option<i64>> {
        let mut out = None;
        self.walk(pid, off, false, &mut |base, end, created, _| {
            if base <= off && off <= end {
                out = Some(created);
            }
            false
        })?;
        Ok(out)
    }

    /// The hash of every offset in `[lo, hi]`, in offset order (offsets no
    /// append holds are skipped).
    pub fn hashes(&self, pid: Pid, lo: u64, hi: u64) -> Result<Vec<(u64, [u8; 16])>> {
        let mut out = Vec::new();
        if hi < lo {
            return Ok(out);
        }
        // Never deferred: its callers read a lease's own frames, a handful of
        // records at most.
        self.walk_from(pid, lo, true, false, &mut |base, _end, _created, hashes| {
            if base > hi {
                return false;
            }
            if let Some(h) = hashes {
                for (i, c) in h.chunks_exact(16).enumerate() {
                    let off = base + i as u64;
                    if off >= lo && off <= hi {
                        out.push((off, <[u8; 16]>::try_from(c).expect("16 bytes")));
                    }
                }
            }
            true
        })?;
        Ok(out)
    }

    /// Resolve one hash for a hash ack (005): `eff` = the lowest offset in
    /// `[lo, hi]` carrying it, `below` = it also occurs at or below the cursor
    /// (inside the dedup window, from `txns_start`).
    pub fn resolve(
        &self,
        pid: Pid,
        hash: &[u8; 16],
        lo: u64,
        hi: u64,
        committed: i64,
        txns_start: u64,
    ) -> Result<AckRes> {
        let mut res = match self.mode {
            IndexMode::Rows => crate::rsm::dedup::resolve(self.r, pid, hash, lo, hi, committed)?,
            IndexMode::Txns => {
                crate::rsm::dedup::resolve_txns(self.r, pid, hash, lo, hi, committed, txns_start)?
            }
            IndexMode::Segment => {
                let upper = hi.max(committed.max(0) as u64);
                let mut res = AckRes::default();
                self.walk(pid, txns_start, true, &mut |base, _end, _c, hashes| {
                    if base > upper {
                        return false;
                    }
                    if let Some(h) = hashes {
                        for (i, c) in h.chunks_exact(16).enumerate() {
                            if c != hash {
                                continue;
                            }
                            let off = base + i as u64;
                            if (off as i64) <= committed {
                                res.below = true;
                            }
                            if off >= lo && off <= hi {
                                res.eff = Some(res.eff.map_or(off, |b: u64| b.min(off)));
                            }
                        }
                    }
                    true
                })?;
                return Ok(res);
            }
        };
        // The span may begin among the retained messages that have no row (a
        // consumer far behind, acking without a lease that holds its
        // frames): their hashes are in the queue log.
        if let Some(off) = self.resolve_unrowed(pid, hash, lo, hi)? {
            res.eff = Some(res.eff.map_or(off, |e| e.min(off)));
        }
        // A miss may be the repeat of an ack that was taken (its answer lost,
        // a leader changed): of a message with a row the rows said so above,
        // of one without the queue log does.
        if res.eff.is_none() && !res.below {
            res.below = self.acked_unrowed(pid, hash, committed)?;
        }
        Ok(res)
    }

    /// The lowest offset in `[lo, hi]` carrying `hash` among the retained
    /// messages that have no row, from the queue log. `None` when the span
    /// holds none of them, or the hash is not there. At most
    /// [`UNROWED_ACK_SCAN`] messages are searched.
    fn resolve_unrowed(&self, pid: Pid, hash: &[u8; 16], lo: u64, hi: u64) -> Result<Option<u64>> {
        let Some(f) = self.facts(pid)? else {
            return Ok(None);
        };
        let to = f.rows_at.min(f.end);
        let a = lo.max(f.log_start);
        let b = hi
            .saturating_add(1)
            .min(to)
            .min(a.saturating_add(UNROWED_ACK_SCAN));
        Ok(self.find_unrowed(&f, pid, hash, a, b, to)?)
    }

    /// Whether `hash` is carried by a retained message at or below the cursor
    /// that has no row: the [`UNROWED_ACK_SCAN`] of them nearest the cursor
    /// are searched, in the queue log, the nearest first. A repeated ack
    /// names a message its group consumed a moment ago (its last batch, or
    /// the one before), so the first, short span usually answers.
    fn acked_unrowed(&self, pid: Pid, hash: &[u8; 16], committed: i64) -> Result<bool> {
        if committed < 0 {
            return Ok(false);
        }
        let Some(f) = self.facts(pid)? else {
            return Ok(false);
        };
        let to = f.rows_at.min(f.end);
        let top = (committed as u64).saturating_add(1).min(to);
        let floor = f.log_start.max(top.saturating_sub(UNROWED_ACK_SCAN));
        let mut b = top;
        for reach in UNROWED_ACK_REACH {
            let a = floor.max(top.saturating_sub(reach));
            if self.find_unrowed(&f, pid, hash, a, b, to)?.is_some() {
                return Ok(true);
            }
            if a <= floor {
                break;
            }
            b = a;
        }
        Ok(false)
    }

    /// The lowest offset in `[a, b)` whose message carries `hash`, read from
    /// the queue log's records below `to` (where the rows begin). The walk is
    /// given `to` and not `b`: it yields whole records only, and `b` may fall
    /// inside one (a batch that ended in the middle of an append).
    ///
    /// What a search read is kept for the next one over the same span
    /// ([`UnrowedScan`]): the items of one ack share their span, and a
    /// search per item from its start would read the span once per item.
    fn find_unrowed(
        &self,
        f: &PartFacts,
        pid: Pid,
        hash: &[u8; 16],
        a: u64,
        b: u64,
        to: u64,
    ) -> Result<Option<u64>> {
        if a >= b {
            return Ok(None);
        }
        let Some(q) = self.seg.and_then(|s| s.qlog.as_ref()) else {
            return Ok(None);
        };
        let mut scans = self.scans.borrow_mut();
        let at = match scans.iter().position(|s| (s.pid, s.a, s.b) == (pid, a, b)) {
            Some(at) => at,
            None => {
                if scans.len() >= UNROWED_SCANS {
                    scans.remove(0);
                }
                scans.push(UnrowedScan {
                    pid,
                    a,
                    b,
                    next: a,
                    seen: HashMap::new(),
                });
                scans.len() - 1
            }
        };
        let scan = &mut scans[at];
        if let Some(off) = scan.seen.get(hash) {
            return Ok(Some(*off));
        }
        if scan.next >= b {
            return Ok(None);
        }
        // On from where the last search of this span stopped, until the hash
        // turns up or the span ends.
        let mut found: Option<u64> = None;
        let mut next = scan.next;
        let seen = &mut scan.seen;
        q.walk_frames(
            f.queue_id,
            pid,
            next,
            to,
            true,
            &mut |base, end, _c, hashes| {
                if base >= b {
                    next = b;
                    return false;
                }
                if let Some(h) = hashes {
                    for (i, c) in h.chunks_exact(16).enumerate() {
                        let off = base + i as u64;
                        if off < a || off >= b {
                            continue;
                        }
                        let c = <[u8; 16]>::try_from(c).expect("16 bytes");
                        seen.entry(c).or_insert(off);
                        if found.is_none() && &c == hash {
                            found = Some(off);
                        }
                    }
                }
                next = end + 1;
                found.is_none()
            },
        )
        .map_err(|e| StoreError::Io(format!("qlog ack walk: {e}")))?;
        // A walk that ended with nothing found has read all there is.
        scan.next = if found.is_some() { next } else { b };
        Ok(found)
    }

    /// The cursor a subscription instant seeds: just before the first
    /// retained append stamped at or after `ts`; the tail when none is.
    pub fn seed_from_ts(&self, pid: Pid, log_start: u64, tail: i64, ts: i64) -> Result<i64> {
        if tail < log_start as i64 {
            return Ok(tail);
        }
        if self.mode == IndexMode::Segment {
            let mut seed = tail;
            self.walk(pid, log_start, false, &mut |base, _end, created, _| {
                if base as i64 > tail {
                    return false;
                }
                if base >= log_start && created >= ts {
                    seed = base as i64 - 1;
                    return false;
                }
                true
            })?;
            return Ok(seed);
        }
        // Stamps rise with the offsets, so the first append at or after `ts`
        // is found by bisection: in the queue log's index for the retained
        // messages that have no row, in the rows for the rest.
        let Some(f) = self.facts(pid)? else {
            return Ok(tail);
        };
        let end = (tail + 1).max(0) as u64;
        let rows_from = log_start.max(f.rows_at);
        if log_start < f.rows_at.min(end) {
            // The newest record below the rows stamped BEFORE `ts`: the seed
            // is its last offset. None retained: the first retained append
            // is already at or after `ts`.
            let below = f.rows_at.min(end);
            let last_older = self
                .qlog()?
                .record_before(f.queue_id, pid, below, ts)
                .map_or(log_start, |r| r.end.max(log_start));
            if last_older < below {
                return Ok(last_older as i64 - 1);
            }
        }
        if rows_from >= end {
            return Ok(tail);
        }
        Ok(
            match crate::rsm::dedup::first_appended_at_or_after(self.r, pid, rows_from, end, ts)? {
                Some((base, _)) => base as i64 - 1,
                None => tail,
            },
        )
    }
}
