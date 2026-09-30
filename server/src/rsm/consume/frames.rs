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

/// The frame reads of one store read transaction.
pub(crate) struct Frames<'a, R: Reads + ?Sized> {
    pub r: &'a R,
    pub seg: Option<&'a SegSource>,
    pub mode: IndexMode,
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

    /// Walk the appends of `pid` forward, from the one covering `from`:
    /// `cb(base, end, created_at, hashes)`; `false` stops the walk.
    pub fn walk(&self, pid: Pid, from: u64, want_hashes: bool, cb: &mut WalkFn<'_>) -> Result<()> {
        if self.mode == IndexMode::Segment {
            return self.walk_segments(pid, from, want_hashes, cb);
        }
        let prefix = keys::txns_prefix(pid);
        let mut start = from;
        let mut bad = false;
        self.r.scan_rev_raw(
            Keyspace::Txns,
            &keys::txns(pid, from),
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
        self.r.scan_raw(
            Keyspace::Txns,
            &keys::txns(pid, start),
            &prefix,
            usize::MAX,
            &mut |k, v| match (keys::txns_base_of(k), txns_row(v)) {
                (Some(base), Some((end, created, hashes))) => {
                    cb(base, end, created, want_hashes.then_some(hashes))
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
        Ok(())
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
    /// claim serves its last frame), bounded by `tail`.
    pub fn newest_fresh(
        &self,
        pid: Pid,
        from: u64,
        tail: i64,
        fresh: &dyn Fn(i64) -> bool,
    ) -> Result<Option<Seg>> {
        if self.mode != IndexMode::Segment {
            // Backwards from the tail: the first fresh one is the newest.
            let prefix = keys::txns_prefix(pid);
            let top = if tail < 0 { 0 } else { tail as u64 };
            let mut found: Option<Seg> = None;
            let mut bad = false;
            self.r.scan_rev_raw(
                Keyspace::Txns,
                &keys::txns(pid, top),
                &prefix,
                usize::MAX,
                &mut |k, v| match (keys::txns_base_of(k), txns_row(v)) {
                    (Some(base), Some((end, created, _))) => {
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
            return Ok(found);
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
        self.walk(pid, lo, true, &mut |base, _end, _created, hashes| {
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
        match self.mode {
            IndexMode::Rows => crate::rsm::dedup::resolve(self.r, pid, hash, lo, hi, committed),
            IndexMode::Txns => {
                crate::rsm::dedup::resolve_txns(self.r, pid, hash, lo, hi, committed, txns_start)
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
                Ok(res)
            }
        }
    }

    /// The cursor a subscription instant seeds: just before the first
    /// retained append stamped at or after `ts`; the tail when none is.
    pub fn seed_from_ts(&self, pid: Pid, log_start: u64, tail: i64, ts: i64) -> Result<i64> {
        let mut seed = tail;
        if tail < log_start as i64 {
            return Ok(tail);
        }
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
        Ok(seed)
    }
}
