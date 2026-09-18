//! Dedup: D10 option (a) in the LEAN encoding, ratified at G0.
//!
//! Two keyspaces, no blooms, no RAM window:
//!
//! - [`Keyspace::Dedup`] — `(pid, hash) → occurrence list`, where an
//!   occurrence is `(offset, created_at_us)`. This row is the authority for
//!   both readers: the push dedup probe (003) and `log_ack_by_hash_v1` (005).
//!   It must be an occurrence LIST, not a single offset and not a (min, max)
//!   pair: 005 resolves a hash to `eff = MIN(voff)` over
//!   `[max(committed+1, txns_start), batch_end]` **and** to
//!   `below = bool_or(voff <= committed)`, two different questions over the
//!   same set, and S2 found 5 591 multi-occurrence rows in 17.6 M (R-21,
//!   R-22).
//! - [`Keyspace::Txns`] — `(pid, base_offset) → [end][created][hashes]`, ONE
//!   sequential row per `Append`. This is the lean part: it replaces option
//!   (a)'s `(created_at, pid, hash)` secondary index, which cost 40 of 79.6
//!   logical B/message, one random put per message and one random delete per
//!   message at prune time. Measured on the VM: store bytes per message
//!   171.4 → **119.0 B** (−31%), store ops per message 2.48 → **1.58** (−36%),
//!   RSS 985 → **689 MiB** (−30%), with rate, latency and exactness unchanged.
//!
//! # The window
//!
//! Hash lists outlive the SEGMENTS retention deletes. A partition therefore
//! carries two watermarks (§6.1): `log_start`, below which the payload is
//! gone, and `txns_start`, below which the hash lists are gone.
//! `txns_start ≤ log_start`, and the gap is the txns window,
//! `max(dedup_window, completed_retention, 900 s)` (D10, retention.rs ≈16–17).
//! Inside that gap a re-push is still a duplicate and an ack-by-hash below the
//! cursor still resolves, exactly as postgres answers.
//!
//! # Where the probe runs, and where the record runs
//!
//! The probe is PLANNING (a read transaction, leader only); the record is
//! APPLY (the write transaction, every node). They are different transactions
//! on different nodes, so apply re-reads the row it extends instead of
//! carrying the planner's value forward — the S2 harness could keep it because
//! probe and record were one call there. That is one `get` per accepted hash
//! on the apply path, and it is what makes apply a pure function of (committed
//! state, entry) (I2) rather than of something the planner saw.

use crate::rsm::effect::Pid;
use crate::rsm::store::keys;
use crate::rsm::store::{Keyspace, Reads, Result, StoreError, Writes};

/// One occurrence: `offset:u64 | created_at_us:i64`, little-endian.
pub const OCCURRENCE_LEN: usize = 16;

/// The header of a `txns` row: `end:u64 | created_at_us:i64`, then
/// `16 × count` hash bytes.
pub const TXNS_HEADER_LEN: usize = 16;

/// What `log_ack_by_hash_v1` needs from one hash (005).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AckRes {
    /// `MIN(voff)` inside the asked range: the offset the ack applies to.
    pub eff: Option<u64>,
    /// Whether the hash also occurs at or below the cursor: the
    /// "below-cursor honesty" leg, which turns an ack into a `noop` instead of
    /// a `stale` (005).
    pub below: bool,
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// The occurrences of a stored `(pid, hash)` row, in write order.
pub fn occurrences(v: &[u8]) -> impl Iterator<Item = (u64, i64)> + '_ {
    v.chunks_exact(OCCURRENCE_LEN).map(|c| {
        (
            u64::from_le_bytes(c[0..8].try_into().unwrap()),
            i64::from_le_bytes(c[8..16].try_into().unwrap()),
        )
    })
}

fn push_occurrence(v: &mut Vec<u8>, offset: u64, created_at_us: i64) {
    v.extend_from_slice(&offset.to_le_bytes());
    v.extend_from_slice(&created_at_us.to_le_bytes());
}

/// One `txns` row: the `Append`'s end offset, its stamp, and its hash list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnsRow {
    /// Inclusive last offset of the append.
    pub end: u64,
    pub created_at_us: i64,
    /// `16 × count` bytes, in frame order — exactly the `Append` effect's
    /// `hashes` field.
    pub hashes: Vec<u8>,
}

impl TxnsRow {
    pub fn count(&self) -> usize {
        self.hashes.len() / 16
    }

    pub fn hash_at(&self, i: usize) -> Option<[u8; 16]> {
        self.hashes.get(i * 16..i * 16 + 16)?.try_into().ok()
    }

    pub fn iter_hashes(&self) -> impl Iterator<Item = [u8; 16]> + '_ {
        self.hashes
            .chunks_exact(16)
            .map(|c| <[u8; 16]>::try_from(c).unwrap())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(TXNS_HEADER_LEN + self.hashes.len());
        v.extend_from_slice(&self.end.to_le_bytes());
        v.extend_from_slice(&self.created_at_us.to_le_bytes());
        v.extend_from_slice(&self.hashes);
        v
    }

    pub fn decode(b: &[u8]) -> std::result::Result<TxnsRow, &'static str> {
        if b.len() < TXNS_HEADER_LEN || !(b.len() - TXNS_HEADER_LEN).is_multiple_of(16) {
            return Err("txns row length");
        }
        Ok(TxnsRow {
            end: u64::from_le_bytes(b[0..8].try_into().unwrap()),
            created_at_us: i64::from_le_bytes(b[8..16].try_into().unwrap()),
            hashes: b[TXNS_HEADER_LEN..].to_vec(),
        })
    }
}

// ---------------------------------------------------------------------------
// Probe (003) — planning
// ---------------------------------------------------------------------------

/// The push dedup verdict for one partition's hashes, in input order.
///
/// `Some(offset)` is a DUPLICATE and carries the ORIGINAL offset, which is
/// what 003 answers and what the planner echoes; the message is not appended
/// and nothing is written. `None` is new.
///
/// A row older than the window is not a duplicate even though its bytes are
/// still there: the physical prune is a background walk, and readers must not
/// wait for it (the same rule KV liveness follows).
pub fn probe<R: Reads + ?Sized>(
    reads: &R,
    pid: Pid,
    hashes: &[[u8; 16]],
    now_us: i64,
    window_us: i64,
    out: &mut Vec<Option<u64>>,
) -> Result<()> {
    out.clear();
    out.reserve(hashes.len());
    let lo = now_us.saturating_sub(window_us);
    for h in hashes {
        out.push(probe_one(reads, pid, h, lo)?);
    }
    Ok(())
}

/// One hash, against an already-computed window floor.
pub fn probe_one<R: Reads + ?Sized>(
    reads: &R,
    pid: Pid,
    hash: &[u8; 16],
    window_floor_us: i64,
) -> Result<Option<u64>> {
    let k = keys::dedup(pid, hash);
    let Some(v) = reads.get_raw(Keyspace::Dedup, &k)? else {
        return Ok(None);
    };
    check_occurrences(v)?;
    let mut best: Option<u64> = None;
    for (off, created) in occurrences(v) {
        if created >= window_floor_us {
            best = Some(best.map_or(off, |b: u64| b.min(off)));
        }
    }
    Ok(best)
}

fn check_occurrences(v: &[u8]) -> Result<()> {
    if v.is_empty() || !v.len().is_multiple_of(OCCURRENCE_LEN) {
        return Err(StoreError::corrupt(
            Keyspace::Dedup,
            format!("occurrence list of {} bytes", v.len()),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Record — apply
// ---------------------------------------------------------------------------

/// Record an `Append`'s hashes: one occurrence per accepted message on its
/// `(pid, hash)` row, plus ONE `txns` row for the whole append.
///
/// `accepted` is `(hash, offset)` in frame order; `base` and `end` are the
/// append's inclusive offset range. Apply calls this for every `Append`
/// effect, so a follower builds the same index the leader probed.
pub fn record<W: Writes + ?Sized>(
    writes: &mut W,
    pid: Pid,
    base: u64,
    end: u64,
    accepted: &[([u8; 16], u64)],
    created_at_us: i64,
) -> Result<()> {
    if accepted.is_empty() {
        return Ok(());
    }
    for (h, off) in accepted {
        let k = keys::dedup(pid, h);
        let mut v = match writes.get_raw(Keyspace::Dedup, &k)? {
            Some(b) => {
                check_occurrences(b)?;
                let mut v = Vec::with_capacity(b.len() + OCCURRENCE_LEN);
                v.extend_from_slice(b);
                v
            }
            None => Vec::with_capacity(OCCURRENCE_LEN),
        };
        push_occurrence(&mut v, *off, created_at_us);
        writes.put_raw(Keyspace::Dedup, &k, &v)?;
    }
    let mut hashes = Vec::with_capacity(accepted.len() * 16);
    for (h, _) in accepted {
        hashes.extend_from_slice(h);
    }
    let row = TxnsRow {
        end,
        created_at_us,
        hashes,
    };
    let k = keys::txns(pid, base);
    writes.put_raw(Keyspace::Txns, &k, &row.encode())
}

// ---------------------------------------------------------------------------
// Resolve (005) — planning
// ---------------------------------------------------------------------------

/// Resolve one hash for `log_ack_by_hash_v1`.
///
/// `lo..=hi` is the span the ack may apply to — 005 uses
/// `[max(committed + 1, txns_start), batch_end]` — and `committed` is the
/// cursor, for the below-cursor leg. Both legs come from the same row, in one
/// `get`.
pub fn resolve<R: Reads + ?Sized>(
    reads: &R,
    pid: Pid,
    hash: &[u8; 16],
    lo: u64,
    hi: u64,
    committed: i64,
) -> Result<AckRes> {
    let k = keys::dedup(pid, hash);
    let Some(v) = reads.get_raw(Keyspace::Dedup, &k)? else {
        return Ok(AckRes::default());
    };
    check_occurrences(v)?;
    let mut res = AckRes::default();
    for (off, _created) in occurrences(v) {
        if (off as i64) <= committed {
            res.below = true;
        }
        if off >= lo && off <= hi {
            res.eff = Some(res.eff.map_or(off, |b: u64| b.min(off)));
        }
    }
    Ok(res)
}

// ---------------------------------------------------------------------------
// Prune — apply, behind a Watermark effect
// ---------------------------------------------------------------------------

/// What one prune step did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PruneStep {
    /// `txns` rows removed.
    pub appends: usize,
    /// Occurrences removed (a row is deleted when its last one goes).
    pub occurrences: usize,
    /// `(pid, hash)` rows deleted outright.
    pub rows_deleted: usize,
    /// The partition's new `txns_start`.
    pub txns_start: u64,
    /// Nothing else was expired at this cutoff.
    pub exhausted: bool,
}

/// Expire one partition's hash lists up to `cutoff_us`, at most `budget`
/// appends, walking forward from `txns_start`.
///
/// This is 006's rotating per-partition purge step, and it is why the lean
/// encoding needs no secondary index: per expired append, one SEQUENTIAL row
/// read and one sequential delete, then the unavoidable random `get` + write
/// per hash on the `(pid, hash)` row the probe reads.
///
/// The caller (the `Watermark` effect's apply, §5.2) writes the returned
/// `txns_start` onto the partition row. Stopping early is normal: the step is
/// bounded so a durable point stays bounded (§11.4).
pub fn prune<W: Writes + ?Sized>(
    writes: &mut W,
    pid: Pid,
    txns_start: u64,
    cutoff_us: i64,
    budget: usize,
) -> Result<PruneStep> {
    let mut step = PruneStep {
        txns_start,
        exhausted: true,
        ..Default::default()
    };
    if budget == 0 {
        step.exhausted = false;
        return Ok(step);
    }

    // Collect first: the scan borrows the transaction and the deletes need it
    // mutably. `budget` bounds the buffer.
    let prefix = keys::txns_prefix(pid);
    let from = keys::txns(pid, txns_start);
    let mut victims: Vec<(u64, TxnsRow)> = Vec::with_capacity(budget.min(256));
    let mut stopped_early = false;
    let mut bad: Option<&'static str> = None;
    writes.scan_raw(Keyspace::Txns, &from, &prefix, budget, &mut |k, v| {
        let Some(base) = keys::txns_base_of(k) else {
            bad = Some("txns key");
            return false;
        };
        match TxnsRow::decode(v) {
            Ok(row) => {
                if row.created_at_us >= cutoff_us {
                    stopped_early = true;
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
        return Err(StoreError::corrupt(Keyspace::Txns, e));
    }
    // The budget ran out before the cutoff did: there is more to do.
    if !stopped_early && victims.len() == budget {
        step.exhausted = false;
    }

    for (base, row) in &victims {
        for h in row.iter_hashes() {
            let k = keys::dedup(pid, &h);
            let Some(cur) = writes.get_raw(Keyspace::Dedup, &k)? else {
                continue;
            };
            check_occurrences(cur)?;
            let mut keep: Vec<u8> = Vec::with_capacity(cur.len());
            for (off, created) in occurrences(cur) {
                if created >= cutoff_us {
                    push_occurrence(&mut keep, off, created);
                } else {
                    step.occurrences += 1;
                }
            }
            if keep.is_empty() {
                writes.del_raw(Keyspace::Dedup, &k)?;
                step.rows_deleted += 1;
            } else if keep.len() != cur.len() {
                writes.put_raw(Keyspace::Dedup, &k, &keep)?;
            }
        }
        let k = keys::txns(pid, *base);
        writes.del_raw(Keyspace::Txns, &k)?;
        step.appends += 1;
        step.txns_start = row.end + 1;
    }
    Ok(step)
}

/// Drop every dedup and txns row of a partition, at most `limit` of each, in
/// key order, resuming from `resume`. The shape a `DeleteChunk` and a
/// `PartitionDelete` need (§5.2 rules).
pub fn delete_partition_chunk<W: Writes + ?Sized>(
    writes: &mut W,
    pid: Pid,
    resume: &[u8],
    limit: usize,
) -> Result<(usize, Option<Vec<u8>>)> {
    let prefix = keys::dedup_prefix(pid);
    let from = if resume.is_empty() {
        &prefix[..]
    } else {
        resume
    };
    let (n, next) = writes.delete_range(Keyspace::Dedup, from, &prefix, limit)?;
    if n < limit {
        // The dedup rows are done; sweep the txns rows in the same call so a
        // partition is finished by a bounded number of chunks.
        let tprefix = keys::txns_prefix(pid);
        let (m, _tnext) = writes.delete_range(Keyspace::Txns, &tprefix, &tprefix, limit - n)?;
        let more = m == limit - n;
        return Ok((n + m, if more { Some(Vec::new()) } else { None }));
    }
    Ok((n, next))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txns_row_round_trips() {
        let row = TxnsRow {
            end: 41,
            created_at_us: 1_700_000_000_000_000,
            hashes: [[1u8; 16], [2u8; 16]].concat(),
        };
        assert_eq!(TxnsRow::decode(&row.encode()).unwrap(), row);
        assert_eq!(row.count(), 2);
        assert_eq!(row.hash_at(1), Some([2u8; 16]));
        assert_eq!(row.hash_at(2), None);
    }

    #[test]
    fn a_malformed_txns_row_is_refused() {
        assert!(TxnsRow::decode(&[0u8; 8]).is_err());
        assert!(TxnsRow::decode(&[0u8; 17]).is_err());
    }

    #[test]
    fn occurrences_round_trip_in_write_order() {
        let mut v = Vec::new();
        push_occurrence(&mut v, 7, 100);
        push_occurrence(&mut v, 3, 200);
        let got: Vec<(u64, i64)> = occurrences(&v).collect();
        assert_eq!(got, vec![(7, 100), (3, 200)]);
    }
}
