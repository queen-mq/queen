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
//! cursor still resolves.
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

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use crate::rsm::effect::Pid;
use crate::rsm::store::keys;
use crate::rsm::store::{Keyspace, Reads, Result, StoreError, Writes};

// ---------------------------------------------------------------------------
// The dedup index mode (PERF-E, 2026-09-19)
// ---------------------------------------------------------------------------
//
// `QUEEN_RAFT_DEDUP_INDEX` selects which keyspace is the dedup AUTHORITY:
//
//   * `txns` (product default) — the authority is the `txns` rows we already
//     write, one sequential row per `Append`. Apply writes NO per-message
//     `(pid, hash)` row (the random read-modify-write that PERF-2 blamed for
//     the RAM growth (R-124) and the durable-point store cost). The planner's
//     PERF-B bloom front, restructured to carry each generation's OFFSET range,
//     turns a "maybe" into a bounded ordered range scan of the `txns` rows; a
//     partition with no warm filter (a restart, a preloaded store) probes the
//     whole txns window exactly, then warms up. Ack-by-hash (005) resolves by
//     the same range scan over `[txns_start, batch_end]`.
//   * `rows` — today's per-message `(pid, hash) → occurrence list` index, kept
//     verbatim for ablation. `record` writes it AND the txns row; the planner
//     reads it with [`probe_one`] / [`resolve`].
//
// The two indexes are behaviourally EXACT for every reader (the difffuzz in
// `tests` proves 0 divergences over 50 seeds): the txns rows already carry the
// same `(offset, created_at)` facts, keyed by append rather than by hash.
//
// # How the mode reaches the two sides
//
// The planner reads it from `PlanConfig::index_mode`, threaded from the boot
// seam (`BatcherConfig::from_env`). `record` runs on the apply thread with a
// fixed call signature (apply.rs owns that call), so it reads a process global
// resolved ONCE at boot by the same seam — an atomic load, never an env read on
// the apply path, so I2 (apply is a pure function of committed state + entry)
// holds exactly as it does for every other boot-resolved knob. The global
// DEFAULTS to `rows` when the seam never ran (the unit-test harness, which
// constructs `Applier` directly): that keeps every existing rows-authority test
// green, while production and the VM go through `from_env` and get `txns`.

/// Which keyspace is the dedup authority. See the module note.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexMode {
    /// The `txns` rows are the authority; no per-message `(pid, hash)` row.
    Txns,
    /// Today's per-message `(pid, hash)` index (kept for ablation).
    Rows,
    /// PERF-E / STORAGE_V2 Lever 2: NO store keyspace is the authority. Apply
    /// writes neither `Dedup` nor `Txns` rows; the committed dedup facts are
    /// served from the SEGMENT files (frame hashes + the `.qidx`/RAM active
    /// index), bounded to the committed partition `last_offset`. The single
    /// scattered per-append `Txns` write — STORAGE_V2's biggest per-append
    /// value — is gone. Read side lives in the planner + `segments::Reader`.
    Segment,
}

impl IndexMode {
    /// `QUEEN_RAFT_DEDUP_INDEX` (default `txns`). A case-insensitive `rows` or
    /// `segment` selects those; anything else is `txns`, so a typo fails safe
    /// to the product default.
    pub fn from_env() -> IndexMode {
        match std::env::var("QUEEN_RAFT_DEDUP_INDEX") {
            Ok(v) if v.trim().eq_ignore_ascii_case("rows") => IndexMode::Rows,
            Ok(v) if v.trim().eq_ignore_ascii_case("segment") => IndexMode::Segment,
            _ => IndexMode::Txns,
        }
    }

    fn code(self) -> u8 {
        match self {
            IndexMode::Rows => 1,
            IndexMode::Txns => 2,
            IndexMode::Segment => 3,
        }
    }
}

/// The apply-side (`record`) mode. `0` = unresolved → `rows` (the unit-test
/// default; the boot seam always sets it explicitly for production).
static RECORD_INDEX_MODE: AtomicU8 = AtomicU8::new(0);

/// Set the apply-side mode. Called ONCE at boot by `BatcherConfig::from_env`
/// (the WP-1.7 seam) so [`record`] agrees with `PlanConfig::index_mode`. Tests
/// that drive the real `record` set it too, but the primitive-level tests use
/// [`record_txns`] / [`record_rows`] directly and never touch this global.
pub fn set_record_index_mode(mode: IndexMode) {
    RECORD_INDEX_MODE.store(mode.code(), Ordering::Relaxed);
}

/// The apply-side mode `record` writes under. Defaults to [`IndexMode::Rows`]
/// until the boot seam sets it.
pub fn record_index_mode() -> IndexMode {
    match RECORD_INDEX_MODE.load(Ordering::Relaxed) {
        2 => IndexMode::Txns,
        3 => IndexMode::Segment,
        _ => IndexMode::Rows,
    }
}

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

/// Append one occurrence to a `(pid, hash)` row's value.
///
/// Public because the `Watermark` effect (§5.2) expires hash lists by OFFSET
/// rather than by the time [`prune`] walks, and apply (WP-1.4) must rewrite a
/// row that keeps some of its occurrences without owning a second copy of this
/// encoding.
pub fn push_occurrence(v: &mut Vec<u8>, offset: u64, created_at_us: i64) {
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
        if b.len() < TXNS_HEADER_LEN {
            return Err("txns row length");
        }
        let body = b.len() - TXNS_HEADER_LEN;
        let hashes_end = match body % 16 {
            0 => b.len(),
            // A v2 row: the retained frame length follows the hashes.
            TXNS_LEN_TAIL => b.len() - TXNS_LEN_TAIL,
            _ => return Err("txns row length"),
        };
        Ok(TxnsRow {
            end: u64::from_le_bytes(b[0..8].try_into().unwrap()),
            created_at_us: i64::from_le_bytes(b[8..16].try_into().unwrap()),
            hashes: b[TXNS_HEADER_LEN..hashes_end].to_vec(),
        })
    }

    /// The append's retained frame bytes, when the row carries them (v2, see
    /// [`TXNS_LEN_TAIL`]); `None` for a row an older build wrote.
    pub fn retained_len_of(b: &[u8]) -> Option<u32> {
        if b.len() < TXNS_HEADER_LEN + TXNS_LEN_TAIL
            || (b.len() - TXNS_HEADER_LEN) % 16 != TXNS_LEN_TAIL
        {
            return None;
        }
        Some(u32::from_le_bytes(b[b.len() - 4..].try_into().unwrap()))
    }

    /// The v2 encoding: this row with the retained frame length after the
    /// hashes.
    pub fn encode_with_len(&self, retained_len: u32) -> Vec<u8> {
        let mut v = self.encode();
        v.extend_from_slice(&retained_len.to_le_bytes());
        v
    }
}

/// A v2 `txns` row carries, after its hashes, the frame bytes the append
/// retains (`u32`, little endian): what `RetainedBytes` counted for it and what
/// the watermark gives back when retention passes it. On the queue-log path
/// the only other copy was a node-local `seg_loc` row per append, written just
/// to remember that length — a second B-tree row for every append (~45% of the
/// per-append RAM with one message per append). The row's length tells the
/// two formats apart: hashes are 16-byte multiples, so the body of a v1 row is
/// `0 mod 16` and a v2 row's `4 mod 16`.
pub const TXNS_LEN_TAIL: usize = 4;

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

pub fn check_occurrences(v: &[u8]) -> Result<()> {
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

/// Record an `Append`'s hashes. Dispatches on the apply-side index mode
/// ([`record_index_mode`], resolved at boot): under `txns` (the product
/// default) it writes ONLY the sequential txns row; under `rows` it also writes
/// the per-message `(pid, hash)` occurrence rows.
///
/// `accepted` is `(hash, offset)` in frame order; `base` and `end` are the
/// append's inclusive offset range. Apply calls this for every `Append`
/// effect, so a follower builds the same index the leader probed. The call
/// signature is fixed (apply.rs owns the call); the mode is the process global
/// the boot seam set, so a follower and the leader write the same rows.
pub fn record<W: Writes + ?Sized>(
    writes: &mut W,
    pid: Pid,
    base: u64,
    end: u64,
    accepted: &[([u8; 16], u64)],
    created_at_us: i64,
) -> Result<()> {
    match record_index_mode() {
        IndexMode::Txns => record_txns(writes, pid, base, end, accepted, created_at_us),
        IndexMode::Rows => record_rows(writes, pid, base, end, accepted, created_at_us),
        // STORAGE_V2 Lever 2: the segment append (apply.rs, just before this
        // call) already persisted pid/base/count/created/hashes/blob into the
        // frame, which IS the committed dedup authority in this mode. So apply
        // writes ZERO store rows here — no `Txns` row (the biggest scattered
        // per-append value), no `Dedup` row. The planner serves every dedup
        // read from the segments, bounded to the committed tail.
        IndexMode::Segment => Ok(()),
    }
}

/// The `txns`-authority record: ONE sequential row per append, no per-message
/// random write. The txns row already carries every `(offset, created)` fact
/// (offset = `base + i` for the `i`-th hash), so the planner's txns range scan
/// and 005's resolve read it directly. This is the PERF-E write path.
pub fn record_txns<W: Writes + ?Sized>(
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
    write_txns_row(writes, pid, base, end, accepted, created_at_us)
}

/// The `rows`-authority record (kept for ablation): one occurrence per accepted
/// message on its `(pid, hash)` row, plus the txns row. This is today's write
/// path verbatim.
pub fn record_rows<W: Writes + ?Sized>(
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
    write_txns_row(writes, pid, base, end, accepted, created_at_us)
}

/// [`record`], carrying the append's retained frame length in its `txns`
/// row (v2, [`TXNS_LEN_TAIL`]) so apply needs no `seg_loc` row to give the
/// bytes back at retention. `Segment` mode writes no `txns` row: the caller
/// keeps its `seg_loc` row there.
pub fn record_with_len<W: Writes + ?Sized>(
    writes: &mut W,
    pid: Pid,
    base: u64,
    end: u64,
    accepted: &[([u8; 16], u64)],
    created_at_us: i64,
    retained_len: u32,
) -> Result<()> {
    if accepted.is_empty() {
        return Ok(());
    }
    if record_index_mode() == IndexMode::Rows {
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
    } else if record_index_mode() == IndexMode::Segment {
        return Ok(());
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
    writes.put_raw(Keyspace::Txns, &k, &row.encode_with_len(retained_len))
}

/// Whether apply's `record_with_len` writes a `txns` row (every mode but
/// `Segment`), i.e. whether the append's length lives there instead of in a
/// `seg_loc` row.
pub fn txns_carry_len() -> bool {
    record_index_mode() != IndexMode::Segment
}

/// Write the one `txns` row an append leaves — shared by both record paths.
fn write_txns_row<W: Writes + ?Sized>(
    writes: &mut W,
    pid: Pid,
    base: u64,
    end: u64,
    accepted: &[([u8; 16], u64)],
    created_at_us: i64,
) -> Result<()> {
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
// Probe over the txns authority (003) — planning, DEDUP_INDEX=txns
// ---------------------------------------------------------------------------

/// The push dedup verdict for one hash under `txns`, scanning the given
/// committed txns OFFSET ranges (each a bloom generation the front matched).
///
/// Returns the ORIGINAL offset — `MIN` over the in-window (`created ≥
/// floor_us`) occurrences the scan finds — or `None` when the hash is a bloom
/// false positive that no txns row actually holds. Ranges are `(min_base,
/// max_off)`, ascending and oldest-first, so the first in-window match found is
/// the global minimum and the scan can stop there.
pub fn scan_txns_for_hash<R: Reads + ?Sized>(
    reads: &R,
    pid: Pid,
    hash: &[u8; 16],
    ranges: &[(u64, u64)],
    floor_us: i64,
) -> Result<Option<u64>> {
    for &(min_base, max_off) in ranges {
        if let Some(off) = scan_txns_range_for_hash(reads, pid, hash, min_base, max_off, floor_us)?
        {
            return Ok(Some(off));
        }
    }
    Ok(None)
}

/// The push dedup verdict for one hash under `txns` with no warm filter: scan
/// the WHOLE txns window of the partition (exact, unbounded by a generation).
/// This is what a restart / preloaded partition pays until the front warms.
pub fn scan_txns_for_hash_whole<R: Reads + ?Sized>(
    reads: &R,
    pid: Pid,
    hash: &[u8; 16],
    floor_us: i64,
) -> Result<Option<u64>> {
    scan_txns_range_for_hash(reads, pid, hash, 0, u64::MAX, floor_us)
}

/// Scan the txns rows of `pid` whose base is in `[min_base, max_off]` for
/// `hash`, returning the minimum in-window (`created ≥ floor_us`) offset it
/// occurs at, or `None`. The scan is ascending, so it stops at the first match.
fn scan_txns_range_for_hash<R: Reads + ?Sized>(
    reads: &R,
    pid: Pid,
    hash: &[u8; 16],
    min_base: u64,
    max_off: u64,
    floor_us: i64,
) -> Result<Option<u64>> {
    let prefix = keys::txns_prefix(pid);
    let from = keys::txns(pid, min_base);
    let mut found: Option<u64> = None;
    let mut bad: Option<&'static str> = None;
    reads.scan_raw(Keyspace::Txns, &from, &prefix, usize::MAX, &mut |k, v| {
        let Some(base) = keys::txns_base_of(k) else {
            bad = Some("txns key");
            return false;
        };
        if base > max_off {
            return false; // no row past max_off can hold an in-range offset
        }
        match TxnsRow::decode(v) {
            Ok(row) => {
                if row.created_at_us < floor_us {
                    return true; // whole append out of window; keep walking
                }
                for (i, h) in row.iter_hashes().enumerate() {
                    if &h == hash {
                        found = Some(base + i as u64);
                        return false; // ascending: first hit is the minimum
                    }
                }
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
    Ok(found)
}

/// Resolve one hash for `log_ack_by_hash_v1` (005) over the txns authority: the
/// same two legs as [`resolve`], computed by an ordered range scan of the txns
/// rows over `[txns_start, …]` (the below-cursor leg needs occurrences down to
/// `txns_start`). `lo..=hi` is the `eff` span, `committed` the cursor.
pub fn resolve_txns<R: Reads + ?Sized>(
    reads: &R,
    pid: Pid,
    hash: &[u8; 16],
    lo: u64,
    hi: u64,
    committed: i64,
    txns_start: u64,
) -> Result<AckRes> {
    // The scan needs to cover both legs: `eff` over [lo, hi] and `below` over
    // [txns_start, committed]. `lo ≥ txns_start` by construction, so the union
    // starts at txns_start and ends at max(hi, committed).
    let upper = hi.max(committed.max(0) as u64);
    let prefix = keys::txns_prefix(pid);
    let from = keys::txns(pid, txns_start);
    let mut res = AckRes::default();
    let mut bad: Option<&'static str> = None;
    reads.scan_raw(Keyspace::Txns, &from, &prefix, usize::MAX, &mut |k, v| {
        let Some(base) = keys::txns_base_of(k) else {
            bad = Some("txns key");
            return false;
        };
        if base > upper {
            return false;
        }
        match TxnsRow::decode(v) {
            Ok(row) => {
                for (i, h) in row.iter_hashes().enumerate() {
                    if &h != hash {
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
    Ok(res)
}

// ---------------------------------------------------------------------------
// Probe / resolve over the SEGMENT authority (PERF-E, DEDUP_INDEX=segment)
// ---------------------------------------------------------------------------
//
// Under `segment` the planner reconstructs a partition's committed `txns` rows
// from the SEGMENT files — bounded to the committed `last_offset`, see
// `segments::Reader::committed_dedup_rows` — into a base-sorted
// `[(base, TxnsRow)]` slice, then answers every dedup question from it. The
// per-hash logic is IDENTICAL to the `txns`-keyspace scans above
// (`scan_txns_range_for_hash` / `resolve_txns`); only the source differs (a RAM
// slice vs an ordered store scan), so the verdicts are exact — the differential
// fuzzer proves 0 divergences. `TxnsRow::end` here is INCLUSIVE (the last
// offset), exactly as a stored `txns` row carries it (the planner converts the
// segment frame's exclusive end when it builds the slice).

/// The push dedup verdict for one hash over the committed segment rows: the MIN
/// in-window (`created >= floor_us`) offset it occurs at, or `None`. `rows` is
/// ascending by base offset (as `committed_dedup_rows` returns), so the first
/// in-window match is the global minimum and the scan stops there — the exact
/// behaviour of [`scan_txns_for_hash_whole`].
pub fn scan_seg_rows_for_hash(
    rows: &[(u64, TxnsRow)],
    hash: &[u8; 16],
    floor_us: i64,
) -> Option<u64> {
    for (base, row) in rows {
        if row.created_at_us < floor_us {
            continue; // whole append out of window; keep walking
        }
        for (i, h) in row.iter_hashes().enumerate() {
            if &h == hash {
                return Some(base + i as u64); // ascending: first hit is the minimum
            }
        }
    }
    None
}

/// Resolve one hash for `log_ack_by_hash_v1` (005) over the committed segment
/// rows: the same two legs as [`resolve_txns`] — `eff` = MIN over `[lo, hi]`,
/// `below` = any occurrence at or below `committed` — reading occurrences at or
/// above `txns_start` (the scan floor). Sourced from the segment rows instead of
/// the `txns` keyspace.
pub fn resolve_seg_rows(
    rows: &[(u64, TxnsRow)],
    hash: &[u8; 16],
    lo: u64,
    hi: u64,
    committed: i64,
    txns_start: u64,
) -> AckRes {
    let mut res = AckRes::default();
    for (base, row) in rows {
        if *base < txns_start {
            continue; // below the txns window floor (the txns scan starts there)
        }
        for (i, h) in row.iter_hashes().enumerate() {
            if &h != hash {
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
    res
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

// ===========================================================================
// Dedup front (PERF-B, 2026-09-19) — a sound over-approximation of the
// committed dedup window that lets the PLANNER skip the per-message LMDB probe.
// ===========================================================================
//
// The baseline planner reads the authoritative `(pid, hash)` index once per
// pushed message ([`probe_one`]). Under the overwhelmingly common shape — every
// message a NEW hash — every one of those random LMDB gets returns "absent",
// pure waste (PERF-2 diagnosis (c): `store::keys::read_name` + `mdb_txn_begin`
// dominate the C1000 fan-out; on the A shapes the probe is one random read per
// arrival). The front removes that per-message scan with a per-partition ring
// of generational register-blocked bloom filters that answers, for a hash,
// EITHER "certainly not in the committed window — skip the probe" OR "maybe —
// probe". It NEVER answers "duplicate": the LMDB index (and the overlay) stay
// the sole authority for that. So a disabled front is exactly equivalent to
// always-probe, and the whole lever is `QUEEN_RAFT_DEDUP_FRONT`.
//
// # Soundness (no false "absent" for a committed in-window hash)
//
// A bloom has NO false negatives, so soundness reduces to the invariant:
//
//   for every partition the front will answer "absent" for (a SEEDED
//   partition), `front(pid)` ⊇ { h : (pid, h) has a committed occurrence in
//   the dedup window the planner is asking about }.
//
// Two facts keep it:
//
//   (1) COVERAGE ON ENTRY. A partition becomes SEEDED one of two ways.
//       * BORN seeded: the planner minted the pid this front-lifetime
//         ([`DedupFront::note_created`]). Pids come from a monotone counter and
//         are never reused, so a fresh pid's committed dedup keyspace is empty —
//         the empty filter is already a complete cover.
//       * SEEDED by scan: a pid that pre-existed this front (a restart, a
//         preloaded store) is covered by [`DedupFront::install_seed`] from the
//         committed `txns` rows the planner scans PLUS the overlay's in-flight
//         appends — the union of everything committed-or-in-flight for that pid.
//         If that union exceeds the seed cap the partition is marked FALLBACK
//         (always-probe) instead, which is sound and bounded.
//   (2) COVERAGE STAYS. Every later append is PLANNED by this same leader, and
//       the planner inserts each planned survivor's hash into the front at plan
//       time (before it commits — [`DedupFront::insert`]). So by the time an
//       append is committed and visible to a probe it is already in the front.
//       Inserting at plan time can only ADD hashes that never commit (a dropped
//       entry) — a false positive, an extra probe, never a false "absent".
//
// The one thing (2) rests on is that THIS planner sees every committed append —
// true while one leader is the continuous author (phase 1 is a single node with
// the local replicator, which never rolls a proposed entry back). A leadership
// change can introduce appends this front never planned, so the driver MUST
// [`DedupFront::reset`] on becoming leader; every partition then re-seeds from
// the committed snapshot, which a raft leader is guaranteed to hold complete.
// Phase 1 never changes leadership, so reset is only the documented hook.
//
// # Aging and memory (bounded, reported)
//
// Generations are created-time ordered (PUSHSER makes `created_at` strictly
// monotone per partition, so plan order == created order == generation order).
// A front (oldest) generation is dropped once its `max_created_us` is entirely
// below the window floor — every hash it holds is then out of window, so the
// drop cannot lose an in-window hash. Per-partition memory therefore tracks
// the window, not all of history.
// A global byte cap ([`DedupFront::byte_cap`], `QUEEN_RAFT_DEDUP_FRONT_MB`)
// bounds the total: a partition that would grow the front past the cap is
// dropped to FALLBACK (always-probe) instead — an always-sound resource
// trade. At 16 bits/hash the front costs ~2 B per in-window message; the
// smoke reports the measured figure.

/// Bits per expected hash a generation is sized for (its filter is
/// `cap × 2` bytes).
const FRONT_BITS_PER_HASH: usize = 16;
/// A generation's fixed overhead (boxed words + counters + deque slot).
const FRONT_BYTES_PER_GEN: usize = 64;
/// Smallest generation capacity, in hashes (a 512 B filter). Powers of two are
/// not required here — a generation is filled by counting, never by a no-copy
/// buffer seal — but keeping it a round number keeps `nblocks` clean. Sized for
/// the many quiet partitions of a wide queue: at 4,096 (8 KiB) 500,000
/// partitions a few generations each wanted ~20 GiB, so all but ~13,000 fell
/// back to scanning their whole window for every message (2026-09-29, 1M msg/s:
/// 60% of the lanes' time); a busy partition still tiers up by
/// [`FRONT_GEN_TIER`] per full generation.
const FRONT_GEN_CAP_MIN: usize = 256;
/// Largest generation capacity (a 2 MiB filter), tiering ×8 from the minimum so
/// an idle partition pays one tiny filter and a hot one settles at a few big
/// generations.
const FRONT_GEN_CAP_MAX: usize = 1 << 20;
const FRONT_GEN_TIER: usize = 8;
/// A generation also closes once it spans this fraction of the dedup window
/// (`FRONT_GEN_SLICES` slices per window, never under `FRONT_GEN_SLICE_MIN_US`).
/// Closing by count alone let one generation of a hot partition span minutes
/// (262k hashes at 1,000 msg/s = ~262 s): every "maybe" then read the committed
/// records of that whole span, and its false-positive rate climbed as it sat
/// near full. Measured 2026-09-23 at 200k msg/s over 200 partitions: dedup band
/// reads grew from 20k to 282k records/s between t=200 s and t=300 s and
/// collapsed the serial planner. Sliced, a band covers at most one slice.
const FRONT_GEN_SLICES: i64 = 4;
const FRONT_GEN_SLICE_MIN_US: i64 = 250_000;

/// The time slice a generation may span for a window ending at `created_us`
/// whose floor is `floor_us`.
fn gen_slice_us(created_us: i64, floor_us: i64) -> i64 {
    (created_us.saturating_sub(floor_us) / FRONT_GEN_SLICES).max(FRONT_GEN_SLICE_MIN_US)
}
/// How many shards the front's per-partition map is split into. Small shards
/// keep the sweeper's hold on one short: ~500 partitions at 500,000.
const FRONT_SHARDS: usize = 1024;
/// The sweeper ([`DedupFront::spawn_sweeper`]) wakes every tick and ages this
/// many shards, so a whole pass takes about a second.
const FRONT_SWEEP_TICK: std::time::Duration = std::time::Duration::from_millis(10);
const FRONT_SWEEP_SHARDS_PER_TICK: usize = FRONT_SHARDS / 100;

/// Default global cap in MiB (`QUEEN_RAFT_DEDUP_FRONT_MB`). 2 GiB fronts ~1 G
/// in-window hashes at 2 B each, or 500,000 quiet partitions with a few
/// minimum generations each, before any partition falls back.
pub const FRONT_DEFAULT_CAP_MB: usize = 2048;
/// The most hashes one first-touch seed scan of a pre-existing partition will
/// read before giving up and marking it FALLBACK. Bounds the one-off planning
/// stall a large preloaded partition can cause; fresh partitions are born
/// seeded and never scan. Read by the planner, which drives the scan.
pub const FRONT_SEED_MAX: usize = 1 << 20;

/// One generation of the front: a register-blocked bloom filter (`nblocks`
/// 64-byte blocks, k=7 probe bits all inside the one block a hash maps to) plus
/// the `max_created_us` watermark that decides when the whole generation ages
/// out. Copied from `server/src/dedup.rs`'s validated `BloomGen`.
struct FrontGen {
    words: Box<[u64]>,
    nblocks: u64,
    len: usize,
    cap: usize,
    max_created_us: i64,
    /// The earliest `created_us` inserted (i64::MAX while empty): the start of
    /// the time slice this generation covers.
    min_created_us: i64,
    /// The smallest append BASE offset any hash in this generation came from,
    /// and the largest message offset — the `[min_base, max_off]` band the txns
    /// range scan walks on a "maybe" (PERF-E). `min_base` (not the first
    /// message offset) so the scan starts at the append that CONTAINS that
    /// message, even when a generation boundary fell mid-append. `u64::MAX /
    /// 0` until the first insert makes the band empty.
    min_base: u64,
    max_off: u64,
}

impl FrontGen {
    fn new(cap: usize) -> FrontGen {
        let cap = cap.max(32); // at least one 512-bit block
        let nblocks = cap / 32; // cap × 16 bits ÷ 512 bits per block
        FrontGen {
            words: vec![0u64; nblocks * 8].into_boxed_slice(),
            nblocks: nblocks as u64,
            len: 0,
            cap,
            max_created_us: i64::MIN,
            min_created_us: i64::MAX,
            min_base: u64::MAX,
            max_off: 0,
        }
    }

    /// Word index of the first word of `h`'s block: multiply-shift range
    /// reduction on the low 64 bits (block choice) vs disjoint 9-bit fields of
    /// the high 64 bits (the 7 probe bits), so choice and bits never correlate.
    #[inline]
    fn block_base(&self, h: u128) -> usize {
        ((((h as u64) as u128) * (self.nblocks as u128)) >> 64) as usize * 8
    }

    #[inline]
    fn insert(&mut self, h: u128, base_off: u64, msg_off: u64, created_us: i64) {
        let base = self.block_base(h);
        let hi = (h >> 64) as u64;
        for i in 0..7 {
            let p = ((hi >> (9 * i)) & 511) as usize;
            self.words[base + (p >> 6)] |= 1u64 << (p & 63);
        }
        self.len += 1;
        if created_us > self.max_created_us {
            self.max_created_us = created_us;
        }
        if created_us < self.min_created_us {
            self.min_created_us = created_us;
        }
        if base_off < self.min_base {
            self.min_base = base_off;
        }
        if msg_off > self.max_off {
            self.max_off = msg_off;
        }
    }

    #[inline]
    fn maybe(&self, h: u128) -> bool {
        let base = self.block_base(h);
        let hi = (h >> 64) as u64;
        for i in 0..7 {
            let p = ((hi >> (9 * i)) & 511) as usize;
            if self.words[base + (p >> 6)] & (1u64 << (p & 63)) == 0 {
                return false;
            }
        }
        true
    }

    fn bytes(&self) -> usize {
        self.words.len() * 8 + FRONT_BYTES_PER_GEN
    }
}

/// The front state of one partition. A partition is present in the map iff it
/// is SEEDED (born or scanned) or FALLBACK; absence means "not yet seeded".
struct PartFront {
    /// Always-probe: the front declines to answer for this partition (seed
    /// overflow or global cap pressure). Its `gens` are empty.
    fallback: bool,
    /// Time-ordered generations, front = oldest.
    gens: VecDeque<FrontGen>,
    /// Capacity for the NEXT generation (tiering state); 0 = start from the min.
    gen_next_cap: usize,
    /// Resident filter bytes (kept in step with the global `total_bytes`).
    bytes: usize,
    /// The newest stamp this partition may hold in its window: the newest hash
    /// planned here or seeded, fallback or not (`i64::MAX` = unknown, after a
    /// seed overflow). Once the window floor passes it, nothing of this
    /// partition is in window, so an empty front is exact again.
    last_created_us: i64,
    /// Its queue's dedup window as last seen (`i64::MAX` = not yet known): the
    /// sweeper ages the partition at `now - window_us`. Never under the window,
    /// so it never ages too early.
    window_us: i64,
}

impl PartFront {
    fn seeded() -> PartFront {
        PartFront {
            fallback: false,
            gens: VecDeque::new(),
            gen_next_cap: 0,
            bytes: 0,
            last_created_us: i64::MIN,
            window_us: i64::MAX,
        }
    }

    /// A FALLBACK partition becomes an empty seeded front again once nothing it
    /// may hold is in window (its newest stamp is below `floor_us`). Returns
    /// whether it did.
    fn recover_if_quiet(&mut self, floor_us: i64) -> bool {
        if self.fallback && self.last_created_us < floor_us {
            self.fallback = false;
            self.gen_next_cap = 0;
            true
        } else {
            false
        }
    }

    /// B26: nothing of this partition can be in window any more — no
    /// generation left, its window known, and its newest planned or seeded
    /// stamp below the floor — so dropping its front loses nothing a re-seed
    /// would not rebuild.
    fn quiet(&self, floor_us: i64) -> bool {
        !self.fallback
            && self.gens.is_empty()
            && self.window_us != i64::MAX
            && self.last_created_us < floor_us
    }

    /// Drop whole front generations that are entirely below the window floor.
    /// Returns the bytes freed (for the global accounting). Sound because
    /// generations are created-monotone, so the oldest holds the smallest
    /// stamps.
    fn age(&mut self, floor_us: i64) -> usize {
        let mut freed = 0;
        while let Some(g) = self.gens.front() {
            if g.max_created_us < floor_us {
                freed += g.bytes();
                self.gens.pop_front();
            } else {
                break;
            }
        }
        self.bytes -= freed;
        freed
    }

    fn maybe(&self, h: u128) -> bool {
        // Newest first: a real duplicate is most likely recent.
        self.gens.iter().rev().any(|g| g.maybe(h))
    }

    /// Collect the `(min_base, max_off)` band of every generation whose bloom
    /// says "maybe", OLDEST first (ascending offset), for the txns range scan.
    /// Oldest-first so the scanner returns the minimum matching offset and can
    /// stop at the first hit. Returns whether any band was pushed.
    fn matching_ranges(&self, h: u128, out: &mut Vec<(u64, u64)>) -> bool {
        let before = out.len();
        for g in &self.gens {
            if g.len > 0 && g.maybe(h) {
                out.push((g.min_base, g.max_off));
            }
        }
        out.len() > before
    }

    fn needs_new_gen(&self, created_us: i64, slice_us: i64) -> bool {
        match self.gens.back() {
            Some(g) => {
                g.len >= g.cap
                    || (g.len > 0 && created_us.saturating_sub(g.min_created_us) >= slice_us)
            }
            None => true,
        }
    }

    /// Capacity of the generation the next roll opens: tier up (×8) when the
    /// current one filled by count; when it closed by time, fit the successor
    /// to the rate it observed (×1.25, rounded up to a power of two) so slices
    /// of a steady stream do not keep tiering up into the byte cap.
    fn next_gen_cap(&self) -> usize {
        match self.gens.back() {
            Some(g) if g.len >= g.cap => (g.cap * FRONT_GEN_TIER).min(FRONT_GEN_CAP_MAX),
            Some(g) => (g.len + g.len / 4)
                .next_power_of_two()
                .clamp(FRONT_GEN_CAP_MIN, FRONT_GEN_CAP_MAX),
            None if self.gen_next_cap == 0 => FRONT_GEN_CAP_MIN,
            None => self.gen_next_cap,
        }
    }

    fn next_gen_bytes(&self) -> usize {
        let cap = self.next_gen_cap();
        // `cap × 16 bits`, rounded to whole 512-bit blocks, plus the header.
        let cap = cap.max(32);
        (cap / 32) * 8 * 8 + FRONT_BYTES_PER_GEN
    }

    /// Insert `h` (from append `base_off`, at message offset `msg_off`), opening
    /// (and tiering) a new generation when the current is full. Returns the
    /// bytes added (a new generation's, else 0).
    fn insert(
        &mut self,
        h: u128,
        base_off: u64,
        msg_off: u64,
        created_us: i64,
        slice_us: i64,
    ) -> usize {
        let mut added = 0;
        if self.needs_new_gen(created_us, slice_us) {
            let cap = self.next_gen_cap();
            let g = FrontGen::new(cap);
            // Remembered for a roll that finds no generation (all aged out).
            self.gen_next_cap = cap;
            added = g.bytes();
            self.bytes += added;
            self.gens.push_back(g);
        }
        self.gens
            .back_mut()
            .unwrap()
            .insert(h, base_off, msg_off, created_us);
        self.last_created_us = self.last_created_us.max(created_us);
        added
    }

    fn mark_fallback(&mut self) -> usize {
        let freed = self.bytes;
        self.gens.clear();
        self.gens.shrink_to_fit();
        self.gen_next_cap = 0;
        self.bytes = 0;
        self.fallback = true;
        freed
    }
}

/// A point-in-time snapshot of the front's counters, for the smoke report and
/// the `stats` tests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FrontStats {
    /// `should_probe` calls (≈ messages the dedup path saw).
    pub messages: u64,
    /// LMDB committed probes the front let through (`maybe`, fallback, disabled).
    pub probes_issued: u64,
    /// LMDB committed probes the front avoided (definitely absent).
    pub probes_skipped: u64,
    /// Of the probes issued, those that scanned the WHOLE window (unseeded,
    /// fallback or disabled front) rather than one generation's band.
    pub probes_whole: u64,
    /// Hashes inserted at plan time.
    pub inserts: u64,
    /// Partitions currently tracked (seeded + fallback).
    pub partitions: u64,
    /// Of those, in the always-probe fallback state.
    pub fallback_partitions: u64,
    /// Fallback partitions that went quiet for a whole window and got an empty
    /// front back.
    pub fallback_recovered: u64,
    /// Resident filter bytes.
    pub bytes: u64,
}

impl FrontStats {
    /// Committed probes issued per message the dedup path saw (1.0 == baseline).
    pub fn probes_per_message(&self) -> f64 {
        if self.messages == 0 {
            0.0
        } else {
            self.probes_issued as f64 / self.messages as f64
        }
    }

    /// Resident filter bytes per message seen.
    pub fn bytes_per_message(&self) -> f64 {
        if self.messages == 0 {
            0.0
        } else {
            self.bytes as f64 / self.messages as f64
        }
    }
}

/// The planner-side dedup front: one shared, persistent instance per broker,
/// owned by the batcher across planning cycles. The planner's lanes probe and
/// feed it; the sweeper thread ages the partitions nobody touches.
pub struct DedupFront {
    enabled: bool,
    byte_cap: usize,
    /// Per-partition fronts, sharded by pid ([`FRONT_SHARDS`]): the planner's
    /// lanes plan different partitions at the same time, and one mutex over
    /// every partition serialized them.
    parts: Vec<Mutex<Shard>>,
    total_bytes: AtomicU64,
    /// The planning clock the batcher last published ([`DedupFront::set_clock`];
    /// `i64::MIN` until the first cycle): never ahead of any cycle still to
    /// plan, so the sweeper's floors never pass the planner's.
    clock_us: AtomicI64,
    // Counters of a DISABLED front (an enabled one counts per shard, under the
    // lock it already holds: eight lanes bumping shared atomics for every
    // message kept one cache line bouncing between their cores).
    messages: AtomicU64,
    probes_issued: AtomicU64,
    probes_skipped: AtomicU64,
    probes_whole: AtomicU64,
    inserts: AtomicU64,
    recovered: AtomicU64,
    /// Partitions tracked, and of those in fallback: counted as they change
    /// (under the shard lock), so [`DedupFront::stats`] never walks the map.
    n_parts: AtomicU64,
    n_fallback: AtomicU64,
}

/// One shard of the front: its partitions and the per-message counters of the
/// probes and inserts that went through it.
#[derive(Default)]
struct Shard {
    map: HashMap<Pid, PartFront>,
    messages: u64,
    probes_issued: u64,
    probes_skipped: u64,
    probes_whole: u64,
    inserts: u64,
}

/// One in-window occurrence the seed scan collected, with everything the front
/// needs to rebuild a generation: the hash, its stamp, the append BASE it came
/// from and its own message offset (for the txns range band, PERF-E).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeedHash {
    pub hash: [u8; 16],
    pub created_us: i64,
    pub base_off: u64,
    pub msg_off: u64,
}

/// The seed the planner collected for a pre-existing partition.
pub enum Seed {
    /// The full in-window union fit under the cap, in created (= offset) order.
    Complete(Vec<SeedHash>),
    /// The scan hit [`FRONT_SEED_MAX`]; mark the partition always-probe.
    Overflow,
}

/// B31: one hash's plan from [`DedupFront::probe_plan_many`] — its verdict
/// and, for [`ProbeVerdict::Ranges`], its bands: `bands[from..to]` of the
/// buffer the caller passed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HashPlan {
    pub verdict: ProbeVerdict,
    pub from: usize,
    pub to: usize,
}

/// What the front tells the planner to do for one hash under `DEDUP_INDEX=txns`
/// ([`DedupFront::probe_plan`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeVerdict {
    /// Certainly absent from the committed window — new, no scan.
    Skip,
    /// Scan the committed txns offset bands written to the caller's buffer.
    Ranges,
    /// No warm filter (unseeded / fallback / front disabled) — scan the whole
    /// txns window, exact but unbounded, and warm the filter for next time.
    Whole,
}

impl DedupFront {
    pub fn new(enabled: bool, byte_cap: usize) -> DedupFront {
        DedupFront {
            enabled,
            byte_cap,
            parts: (0..FRONT_SHARDS)
                .map(|_| Mutex::new(Shard::default()))
                .collect(),
            total_bytes: AtomicU64::new(0),
            clock_us: AtomicI64::new(i64::MIN),
            messages: AtomicU64::new(0),
            probes_issued: AtomicU64::new(0),
            probes_skipped: AtomicU64::new(0),
            probes_whole: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            recovered: AtomicU64::new(0),
            n_parts: AtomicU64::new(0),
            n_fallback: AtomicU64::new(0),
        }
    }

    /// Publish the clock of the cycle about to plan. The planner's own clock is
    /// never below it, now or later (`plan_now` only moves forward from the
    /// wall), which is what lets the sweeper age with it.
    pub fn set_clock(&self, now_us: i64) {
        self.clock_us.fetch_max(now_us, Ordering::AcqRel);
    }

    /// Start the thread that ages the partitions nobody touches. Aging on touch
    /// alone keeps a quiet partition's stale generations for as long as it stays
    /// quiet: 500,000 partitions written round robin, each once every ~450 s,
    /// grew the front ~5 MB/s into its cap in under five minutes and pushed
    /// every newly written partition to whole-window scans (2026-09-29, 1M
    /// msg/s). The thread holds the front weakly and ends with it.
    pub fn spawn_sweeper(front: &Arc<DedupFront>) {
        if !front.enabled {
            return;
        }
        let weak = Arc::downgrade(front);
        let spawned = std::thread::Builder::new()
            .name("queen-rsm-dedupsweep".into())
            .spawn(move || {
                let mut shard = 0usize;
                loop {
                    std::thread::sleep(FRONT_SWEEP_TICK);
                    let Some(front) = weak.upgrade() else {
                        return;
                    };
                    let now_us = front.clock_us.load(Ordering::Acquire);
                    if now_us == i64::MIN {
                        continue;
                    }
                    for _ in 0..FRONT_SWEEP_SHARDS_PER_TICK {
                        front.sweep_shard(shard, now_us);
                        shard = (shard + 1) % FRONT_SHARDS;
                    }
                }
            });
        if let Err(e) = spawned {
            tracing::warn!(target: "rsm", error = %e, "dedup front sweeper not started: quiet partitions age only when touched");
        }
    }

    /// Age every partition of shard `s` at `now_us` less its own window, give
    /// fallback partitions that went quiet an empty front back, and forget
    /// the partitions with nothing left in window (B26): each kept a
    /// `PartFront` for ever — ~136 B a partition, and every sweep walked all
    /// of them. A forgotten partition is unseeded again: its next push seeds
    /// it, and that seed reads no committed row when the partition's newest
    /// stamp is out of window ([`Planner::front_prepare`]). So the map, and
    /// the walk, follow the partitions with an in-window hash.
    fn sweep_shard(&self, s: usize, now_us: i64) {
        let (mut freed, mut recovered, mut forgotten) = (0usize, 0u64, 0u64);
        let mut parts = self.parts[s].lock().unwrap();
        parts.map.retain(|_, pf| {
            let floor_us = now_us.saturating_sub(pf.window_us);
            if pf.fallback {
                if !pf.recover_if_quiet(floor_us) {
                    return true;
                }
                recovered += 1;
            }
            let f = pf.age(floor_us);
            if f > 0 && pf.gens.is_empty() {
                pf.gens.shrink_to_fit();
            }
            freed += f;
            if pf.quiet(floor_us) {
                freed += pf.bytes;
                forgotten += 1;
                return false;
            }
            true
        });
        // Still under the shard lock, like every other adjustment: a reset
        // clears this shard either before the walk above or after these.
        if freed > 0 {
            self.total_bytes.fetch_sub(freed as u64, Ordering::Relaxed);
        }
        if recovered > 0 {
            self.recovered.fetch_add(recovered, Ordering::Relaxed);
            self.n_fallback.fetch_sub(recovered, Ordering::Relaxed);
        }
        if forgotten > 0 {
            self.n_parts.fetch_sub(forgotten, Ordering::Relaxed);
        }
        drop(parts);
    }

    /// B26: forget partitions that are gone (a delete planned): their fronts
    /// would only age out a window later. Sound whatever becomes of the
    /// delete — a partition the front does not hold is seeded afresh.
    pub fn forget(&self, pids: &[Pid]) {
        if !self.enabled {
            return;
        }
        for pid in pids {
            let mut parts = self.shard(*pid);
            if let Some(pf) = parts.map.remove(pid) {
                self.total_bytes
                    .fetch_sub(pf.bytes as u64, Ordering::Relaxed);
                self.n_parts.fetch_sub(1, Ordering::Relaxed);
                if pf.fallback {
                    self.n_fallback.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// `(partitions, fallback)` counted by walking the map (tests: the counters
    /// must agree).
    #[cfg(test)]
    fn walk_counts(&self) -> (u64, u64) {
        let (mut partitions, mut fallback) = (0u64, 0u64);
        for shard in &self.parts {
            let parts = shard.lock().unwrap();
            partitions += parts.map.len() as u64;
            fallback += parts.map.values().filter(|p| p.fallback).count() as u64;
        }
        (partitions, fallback)
    }

    /// Sweep every shard once (tests).
    #[cfg(test)]
    fn sweep_all(&self, now_us: i64) {
        for s in 0..FRONT_SHARDS {
            self.sweep_shard(s, now_us);
        }
    }

    /// A front that always says "probe": the `QUEEN_RAFT_DEDUP_FRONT=0` ablation
    /// and the tests' default. Exactly equivalent to the baseline planner.
    pub fn disabled() -> DedupFront {
        DedupFront::new(false, 0)
    }

    /// Resolve from the environment (WP-1.7 style). `QUEEN_RAFT_DEDUP_FRONT`
    /// (default 1) is the kill switch; `QUEEN_RAFT_DEDUP_FRONT_MB` (default
    /// [`FRONT_DEFAULT_CAP_MB`]) is the global cap.
    pub fn from_env() -> DedupFront {
        let enabled = match std::env::var("QUEEN_RAFT_DEDUP_FRONT") {
            Ok(v) => {
                let v = v.trim();
                !(v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off"))
            }
            Err(_) => true,
        };
        let mb = std::env::var("QUEEN_RAFT_DEDUP_FRONT_MB")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(FRONT_DEFAULT_CAP_MB);
        DedupFront::new(enabled, mb << 20)
    }

    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// The shard holding partition `pid`'s front.
    #[inline]
    fn shard(&self, pid: Pid) -> std::sync::MutexGuard<'_, Shard> {
        self.parts[(pid % FRONT_SHARDS as u64) as usize]
            .lock()
            .unwrap()
    }

    /// Drop everything: the leadership-change hook (see the module note). Every
    /// partition re-seeds from committed state on its next touch.
    pub fn reset(&self) {
        if !self.enabled {
            return;
        }
        for shard in &self.parts {
            shard.lock().unwrap().map.clear();
        }
        self.total_bytes.store(0, Ordering::Relaxed);
        self.n_parts.store(0, Ordering::Relaxed);
        self.n_fallback.store(0, Ordering::Relaxed);
    }

    /// True iff this partition still needs a first-touch seed scan (it is
    /// neither seeded nor fallback). Cheap check the planner does once per push
    /// command before its per-hash loop.
    pub fn needs_seed(&self, pid: Pid) -> bool {
        if !self.enabled {
            return false;
        }
        !self.shard(pid).map.contains_key(&pid)
    }

    /// A partition minted this front-lifetime: born seeded, empty and complete.
    pub fn note_created(&self, pid: Pid) {
        if !self.enabled {
            return;
        }
        if let std::collections::hash_map::Entry::Vacant(v) = self.shard(pid).map.entry(pid) {
            v.insert(PartFront::seeded());
            self.n_parts.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Install a first-touch seed the planner collected. `Complete` builds the
    /// generations (respecting the global cap); `Overflow` marks fallback.
    pub fn install_seed(&self, pid: Pid, seed: Seed, floor_us: i64) {
        if !self.enabled {
            return;
        }
        let mut parts = self.shard(pid);
        // A concurrent path never runs (single planning thread), but a second
        // seed of the same pid this cycle is a no-op.
        if parts.map.contains_key(&pid) {
            return;
        }
        match seed {
            Seed::Overflow => {
                let mut pf = PartFront::seeded();
                pf.fallback = true;
                // What it holds is unknown: it never recovers by going quiet.
                pf.last_created_us = i64::MAX;
                parts.map.insert(pid, pf);
                self.n_parts.fetch_add(1, Ordering::Relaxed);
                self.n_fallback.fetch_add(1, Ordering::Relaxed);
            }
            Seed::Complete(hashes) => {
                let mut pf = PartFront::seeded();
                let newest = hashes
                    .iter()
                    .map(|sh| sh.created_us)
                    .max()
                    .unwrap_or(floor_us);
                // Covers the hashes a cap fallback below never gets to insert.
                pf.last_created_us = newest;
                let slice_us = gen_slice_us(newest, floor_us);
                for sh in hashes {
                    if sh.created_us < floor_us {
                        continue; // out of window: never a duplicate, skip
                    }
                    let cap = self.byte_cap as u64;
                    if pf.needs_new_gen(sh.created_us, slice_us)
                        && self.total_bytes.load(Ordering::Relaxed) + pf.next_gen_bytes() as u64
                            > cap
                    {
                        // Cap pressure mid-seed: give up on this partition.
                        let freed = pf.mark_fallback();
                        self.total_bytes.fetch_sub(freed as u64, Ordering::Relaxed);
                        self.n_fallback.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    let added = pf.insert(
                        u128::from_le_bytes(sh.hash),
                        sh.base_off,
                        sh.msg_off,
                        sh.created_us,
                        slice_us,
                    );
                    if added > 0 {
                        self.total_bytes.fetch_add(added as u64, Ordering::Relaxed);
                    }
                }
                parts.map.insert(pid, pf);
                self.n_parts.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// The per-message decision: should the planner issue the LMDB committed
    /// probe for `(pid, hash)`? `false` means "certainly absent from the
    /// committed window — treat as new". Counts every call as a message.
    #[inline]
    pub fn should_probe(&self, pid: Pid, hash: &[u8; 16], floor_us: i64) -> bool {
        if !self.enabled {
            self.messages.fetch_add(1, Ordering::Relaxed);
            self.probes_issued.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        let h = u128::from_le_bytes(*hash);
        let mut parts = self.shard(pid);
        let issue = match parts.map.get_mut(&pid) {
            None => true, // not seeded: probe (planner seeds first)
            Some(pf) => {
                if pf.fallback && !self.recover(pf, floor_us) {
                    true
                } else {
                    let freed = pf.age(floor_us);
                    if freed > 0 {
                        self.total_bytes.fetch_sub(freed as u64, Ordering::Relaxed);
                    }
                    pf.maybe(h)
                }
            }
        };
        parts.messages += 1;
        if issue {
            parts.probes_issued += 1;
        } else {
            parts.probes_skipped += 1;
        }
        issue
    }

    /// The per-message plan under `DEDUP_INDEX=txns`: what the planner should do
    /// to resolve `(pid, hash)` against the committed txns authority. On
    /// [`ProbeVerdict::Ranges`] the caller's `out` holds the offset bands to
    /// scan (oldest-first). Counts every call as a message, and a scan (Ranges
    /// or Whole) as a probe issued. `front_prepare` must have run for this pid.
    #[inline]
    pub fn probe_plan(
        &self,
        pid: Pid,
        hash: &[u8; 16],
        floor_us: i64,
        out: &mut Vec<(u64, u64)>,
    ) -> ProbeVerdict {
        out.clear();
        if !self.enabled {
            self.messages.fetch_add(1, Ordering::Relaxed);
            self.probes_issued.fetch_add(1, Ordering::Relaxed);
            return ProbeVerdict::Whole;
        }
        let h = u128::from_le_bytes(*hash);
        let mut parts = self.shard(pid);
        let verdict = match parts.map.get_mut(&pid) {
            None => ProbeVerdict::Whole, // not seeded: the planner seeds first
            Some(pf) => {
                if pf.fallback && !self.recover(pf, floor_us) {
                    ProbeVerdict::Whole
                } else {
                    let freed = pf.age(floor_us);
                    if freed > 0 {
                        self.total_bytes.fetch_sub(freed as u64, Ordering::Relaxed);
                    }
                    if pf.matching_ranges(h, out) {
                        ProbeVerdict::Ranges
                    } else {
                        ProbeVerdict::Skip
                    }
                }
            }
        };
        parts.messages += 1;
        match verdict {
            ProbeVerdict::Skip => parts.probes_skipped += 1,
            ProbeVerdict::Whole => {
                parts.probes_issued += 1;
                parts.probes_whole += 1;
            }
            ProbeVerdict::Ranges => parts.probes_issued += 1,
        }
        verdict
    }

    /// B31: [`DedupFront::probe_plan`] for every hash of one push under ONE
    /// hold of the partition's shard (it took one per message, and one more
    /// per survivor to insert it). The verdicts are the ones the calls one by
    /// one would give: what a probe changes — a fallback partition that went
    /// quiet recovered, whole generations aged out — depends only on the
    /// floor, the same for every hash of the command, so it happens once
    /// here. `window_us` is the queue's dedup window (the sweeper ages the
    /// partition by it). `false`, with nothing planned or counted, when the
    /// partition is not seeded yet: the planner seeds it and asks again.
    pub fn probe_plan_many<'h>(
        &self,
        pid: Pid,
        hashes: impl Iterator<Item = &'h [u8; 16]>,
        floor_us: i64,
        window_us: i64,
        bands: &mut Vec<(u64, u64)>,
        out: &mut Vec<HashPlan>,
    ) -> bool {
        bands.clear();
        out.clear();
        let whole = HashPlan {
            verdict: ProbeVerdict::Whole,
            from: 0,
            to: 0,
        };
        if !self.enabled {
            out.extend(hashes.map(|_| whole));
            let n = out.len() as u64;
            self.messages.fetch_add(n, Ordering::Relaxed);
            self.probes_issued.fetch_add(n, Ordering::Relaxed);
            return true;
        }
        let mut parts = self.shard(pid);
        let parts = &mut *parts;
        let Some(pf) = parts.map.get_mut(&pid) else {
            return false;
        };
        // The queue's window as this probe sees it: the sweeper ages and
        // forgets the partition by it (a push that finds only duplicates
        // inserts nothing, and would leave it unknown).
        pf.window_us = window_us.max(0);
        let fallback = pf.fallback && !self.recover(pf, floor_us);
        if !fallback {
            let freed = pf.age(floor_us);
            if freed > 0 {
                self.total_bytes.fetch_sub(freed as u64, Ordering::Relaxed);
            }
        }
        for hash in hashes {
            let plan = if fallback {
                whole
            } else {
                let from = bands.len();
                if pf.matching_ranges(u128::from_le_bytes(*hash), bands) {
                    HashPlan {
                        verdict: ProbeVerdict::Ranges,
                        from,
                        to: bands.len(),
                    }
                } else {
                    HashPlan {
                        verdict: ProbeVerdict::Skip,
                        from,
                        to: from,
                    }
                }
            };
            parts.messages += 1;
            match plan.verdict {
                ProbeVerdict::Skip => parts.probes_skipped += 1,
                ProbeVerdict::Whole => {
                    parts.probes_issued += 1;
                    parts.probes_whole += 1;
                }
                ProbeVerdict::Ranges => parts.probes_issued += 1,
            }
            out.push(plan);
        }
        true
    }

    /// B31: [`DedupFront::insert`] of every survivor of one push — `(hash,
    /// message offset)` in offset order, all from append `base_off` stamped
    /// `created_us` — under one hold of the shard. Exactly the inserts one by
    /// one.
    pub fn insert_many<'h>(
        &self,
        pid: Pid,
        hashes: impl Iterator<Item = (&'h [u8; 16], u64)>,
        base_off: u64,
        created_us: i64,
        floor_us: i64,
    ) {
        if !self.enabled {
            return;
        }
        let mut parts = self.shard(pid);
        let parts = &mut *parts;
        let Some(pf) = parts.map.get_mut(&pid) else {
            return;
        };
        for (hash, msg_off) in hashes {
            pf.window_us = created_us.saturating_sub(floor_us);
            let fallback = pf.fallback && !self.recover(pf, floor_us);
            pf.last_created_us = pf.last_created_us.max(created_us);
            if fallback {
                continue;
            }
            let freed = pf.age(floor_us);
            if freed > 0 {
                self.total_bytes.fetch_sub(freed as u64, Ordering::Relaxed);
            }
            let slice_us = gen_slice_us(created_us, floor_us);
            if pf.needs_new_gen(created_us, slice_us) {
                let cap = self.byte_cap as u64;
                if self.total_bytes.load(Ordering::Relaxed) + pf.next_gen_bytes() as u64 > cap {
                    let freed = pf.mark_fallback();
                    self.total_bytes.fetch_sub(freed as u64, Ordering::Relaxed);
                    self.n_fallback.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }
            let added = pf.insert(
                u128::from_le_bytes(*hash),
                base_off,
                msg_off,
                created_us,
                slice_us,
            );
            if added > 0 {
                self.total_bytes.fetch_add(added as u64, Ordering::Relaxed);
            }
            parts.inserts += 1;
        }
    }

    /// Record a planned (survivor) append hash (from append `base_off`, at
    /// message offset `msg_off`). Keeps the front a superset of the committed
    /// index it fronts (see the module note) and its generations' offset bands
    /// current. No-op for a fallback or unseeded partition.
    #[inline]
    pub fn insert(
        &self,
        pid: Pid,
        hash: &[u8; 16],
        base_off: u64,
        msg_off: u64,
        created_us: i64,
        floor_us: i64,
    ) {
        if !self.enabled {
            return;
        }
        let h = u128::from_le_bytes(*hash);
        let mut parts = self.shard(pid);
        let parts = &mut *parts;
        let Some(pf) = parts.map.get_mut(&pid) else {
            return;
        };
        pf.window_us = created_us.saturating_sub(floor_us);
        let fallback = pf.fallback && !self.recover(pf, floor_us);
        // Counted before any cap decision: the hash that tips the partition into
        // fallback is planned all the same.
        pf.last_created_us = pf.last_created_us.max(created_us);
        if fallback {
            return;
        }
        let freed = pf.age(floor_us);
        if freed > 0 {
            self.total_bytes.fetch_sub(freed as u64, Ordering::Relaxed);
        }
        let slice_us = gen_slice_us(created_us, floor_us);
        if pf.needs_new_gen(created_us, slice_us) {
            let cap = self.byte_cap as u64;
            if self.total_bytes.load(Ordering::Relaxed) + pf.next_gen_bytes() as u64 > cap {
                let freed = pf.mark_fallback();
                self.total_bytes.fetch_sub(freed as u64, Ordering::Relaxed);
                self.n_fallback.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        let added = pf.insert(h, base_off, msg_off, created_us, slice_us);
        if added > 0 {
            self.total_bytes.fetch_add(added as u64, Ordering::Relaxed);
        }
        parts.inserts += 1;
    }

    /// A snapshot of the counters and the resident footprint.
    pub fn stats(&self) -> FrontStats {
        let mut st = FrontStats {
            messages: self.messages.load(Ordering::Relaxed),
            probes_issued: self.probes_issued.load(Ordering::Relaxed),
            probes_skipped: self.probes_skipped.load(Ordering::Relaxed),
            probes_whole: self.probes_whole.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            partitions: self.n_parts.load(Ordering::Relaxed),
            fallback_partitions: self.n_fallback.load(Ordering::Relaxed),
            fallback_recovered: self.recovered.load(Ordering::Relaxed),
            bytes: self.total_bytes.load(Ordering::Relaxed),
        };
        for shard in &self.parts {
            let sh = shard.lock().unwrap();
            st.messages += sh.messages;
            st.probes_issued += sh.probes_issued;
            st.probes_skipped += sh.probes_skipped;
            st.probes_whole += sh.probes_whole;
            st.inserts += sh.inserts;
        }
        st
    }

    /// [`PartFront::recover_if_quiet`], counted.
    fn recover(&self, pf: &mut PartFront, floor_us: i64) -> bool {
        let recovered = pf.recover_if_quiet(floor_us);
        if recovered {
            self.recovered.fetch_add(1, Ordering::Relaxed);
            self.n_fallback.fetch_sub(1, Ordering::Relaxed);
        }
        recovered
    }
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
        // A body of 4 mod 16 is a v2 row, anything else but 0 is damage.
        assert!(TxnsRow::decode(&[0u8; 16 + 16 + 5]).is_err());
    }

    #[test]
    fn a_v2_txns_row_carries_its_retained_length_and_the_same_hashes() {
        let row = TxnsRow {
            end: 9,
            created_at_us: 1_700_000_000_000_000,
            hashes: [[7u8; 16], [8u8; 16], [9u8; 16]].concat(),
        };
        let v2 = row.encode_with_len(4_321);
        assert_eq!(v2.len(), TXNS_HEADER_LEN + 48 + TXNS_LEN_TAIL);
        assert_eq!(
            TxnsRow::decode(&v2).unwrap(),
            row,
            "the same row for every reader"
        );
        assert_eq!(TxnsRow::retained_len_of(&v2), Some(4_321));
        assert_eq!(
            TxnsRow::retained_len_of(&row.encode()),
            None,
            "a v1 row has none"
        );
        // One message: a 36 B row.
        let one = TxnsRow {
            end: 0,
            created_at_us: 5,
            hashes: vec![3u8; 16],
        };
        assert_eq!(
            TxnsRow::retained_len_of(&one.encode_with_len(300)),
            Some(300)
        );
        assert_eq!(TxnsRow::decode(&one.encode_with_len(300)).unwrap(), one);
    }

    #[test]
    fn occurrences_round_trip_in_write_order() {
        let mut v = Vec::new();
        push_occurrence(&mut v, 7, 100);
        push_occurrence(&mut v, 3, 200);
        let got: Vec<(u64, i64)> = occurrences(&v).collect();
        assert_eq!(got, vec![(7, 100), (3, 200)]);
    }

    // ---- dedup front (PERF-B) ------------------------------------------------

    /// A distinct 16-byte hash from a counter, spread across the 128-bit space
    /// so the block-choice and bit fields the bloom reads are well mixed.
    fn fh(n: u64) -> [u8; 16] {
        let x = (n.wrapping_mul(0x9E37_79B9_7F4A_7C15)) as u128
            | ((n.wrapping_mul(0xD1B5_4A32_D192_ED03) as u128) << 64);
        x.to_le_bytes()
    }

    #[test]
    fn generations_close_by_time_so_bands_stay_within_a_slice() {
        // The 2026-09-23 collapse shape: one hot partition at 1,000 msg/s with a
        // 60 s dedup window, for 300 s. Count-only generations let one span
        // ~262 s, so a "maybe" band read minutes of records; sliced, a
        // generation spans at most window/4 and bands stay narrow.
        let f = DedupFront::new(true, 64 << 20);
        f.note_created(9);
        let window_us: i64 = 60_000_000;
        let slice_us = window_us / FRONT_GEN_SLICES;
        let t0: i64 = 1_000_000_000_000;
        let n: u64 = 300_000; // 300 s at 1,000/s
        for i in 0..n {
            let created = t0 + (i as i64) * 1_000;
            f.insert(9, &fh(i), i - (i % 100), i, created, created - window_us);
        }
        let now = t0 + (n as i64) * 1_000;
        let floor = now - window_us;
        {
            let parts = f.shard(9);
            let pf = parts.map.get(&9).expect("partition tracked");
            assert!(!pf.fallback, "must not fall back");
            for g in &pf.gens {
                let span = g.max_created_us - g.min_created_us;
                assert!(
                    span < slice_us,
                    "generation spans {span} us > slice {slice_us}"
                );
            }
            // Aged to roughly window + one slice of history.
            assert!(
                pf.gens.len() <= (FRONT_GEN_SLICES as usize) + 2,
                "{} gens",
                pf.gens.len()
            );
        }
        // A duplicate of a recent message: its band covers at most one slice of
        // offsets (1,000 msg/s x 15 s) plus one append.
        let mut ranges = Vec::new();
        let v = f.probe_plan(9, &fh(n - 10), floor, &mut ranges);
        assert!(
            matches!(v, ProbeVerdict::Ranges),
            "a recent hash must probe"
        );
        for (lo, hi) in &ranges {
            assert!(
                hi - lo <= 15_000 + 100,
                "band {lo}..{hi} wider than one slice"
            );
        }
        // No false negatives anywhere in the window.
        for i in (n - 59_000)..n {
            assert!(
                f.should_probe(9, &fh(i), floor),
                "in-window hash {i} skipped"
            );
        }
        // Bounded memory: well under a MiB for ~75 s of 1,000/s at 2 B/hash.
        assert!(
            f.stats().bytes < (1 << 20),
            "front holds {} bytes",
            f.stats().bytes
        );
    }

    #[test]
    fn front_never_says_skip_for_an_inserted_hash() {
        // No false negatives: every hash the front was told about must probe.
        let f = DedupFront::new(true, 64 << 20);
        f.note_created(7);
        let n = 50_000u64; // spans several tiered generations
        for i in 0..n {
            f.insert(7, &fh(i), i, i, 1_000 + i as i64, i64::MIN);
        }
        for i in 0..n {
            assert!(
                f.should_probe(7, &fh(i), i64::MIN),
                "inserted hash {i} was wrongly skipped"
            );
        }
        // And it actually skips the vast majority of never-seen hashes.
        let f2 = DedupFront::new(true, 64 << 20);
        f2.note_created(1);
        for i in 0..n {
            f2.insert(1, &fh(i), i, i, 1_000, i64::MIN);
        }
        let mut skipped = 0;
        for i in n..(2 * n) {
            if !f2.should_probe(1, &fh(i), i64::MIN) {
                skipped += 1;
            }
        }
        // 16 bits/hash, k=7 → well under 1% false positive; require ≥98% skip.
        assert!(
            skipped >= (n as usize) * 98 / 100,
            "only {skipped}/{n} skipped"
        );
    }

    #[test]
    fn front_disabled_is_exactly_baseline() {
        let f = DedupFront::disabled();
        assert!(!f.needs_seed(9)); // planner never seeds
        f.note_created(9); // no-op
        f.insert(9, &fh(1), 1, 1, 1_000, i64::MIN); // no-op
                                                    // Every message probes; nothing is skipped or tracked.
        assert!(f.should_probe(9, &fh(1), i64::MIN));
        assert!(f.should_probe(9, &fh(2), i64::MIN));
        let s = f.stats();
        assert_eq!(s.probes_skipped, 0);
        assert_eq!(s.probes_issued, 2);
        assert_eq!(s.messages, 2);
        assert_eq!(s.partitions, 0);
    }

    #[test]
    fn front_ages_out_whole_stale_generations() {
        let f = DedupFront::new(true, 64 << 20);
        f.note_created(3);
        // Fill exactly one generation (the min cap) with old hashes, then one
        // new hash opens a second generation.
        for i in 0..FRONT_GEN_CAP_MIN as u64 {
            f.insert(3, &fh(i), i, i, 1_000, i64::MIN);
        }
        assert!(f.stats().bytes > 0);
        f.insert(3, &fh(1_000_000), 1_000_000, 1_000_000, 9_000, i64::MIN); // opens gen1 at t=9000
        let both = f.stats().bytes; // gen0 (stale) + gen1 (fresh)
                                    // Age with a floor above the old stamps but below the new one: gen0 is
                                    // entirely stale and must be dropped, gen1 kept.
        assert!(f.should_probe(3, &fh(1_000_000), 5_000)); // ages, then maybe
        let after = f.stats().bytes;
        assert!(
            after < both,
            "aging did not free the stale generation: {both} -> {after}"
        );
        // The old hashes now fall outside every surviving generation → skipped.
        let mut skipped = 0;
        for i in 0..FRONT_GEN_CAP_MIN as u64 {
            if !f.should_probe(3, &fh(i), 5_000) {
                skipped += 1;
            }
        }
        assert!(
            skipped > FRONT_GEN_CAP_MIN * 9 / 10,
            "stale hashes not aged out"
        );
    }

    #[test]
    fn front_falls_back_under_the_byte_cap() {
        // A cap below one minimum generation forces the partition to always-probe
        // instead of exceeding it — always sound, never a wrong verdict.
        let f = DedupFront::new(true, 128); // bytes, < one 8 KiB filter
        f.note_created(5);
        f.insert(5, &fh(1), 1, 1, 1_000, i64::MIN);
        let s = f.stats();
        assert_eq!(s.fallback_partitions, 1);
        assert!(s.bytes <= 128);
        // A fallback partition probes everything, including its own hash.
        assert!(f.should_probe(5, &fh(1), i64::MIN));
        assert!(f.should_probe(5, &fh(2), i64::MIN));
        assert_eq!(f.stats().probes_skipped, 0);
    }

    #[test]
    fn the_sweeper_ages_a_partition_nobody_touches() {
        let f = DedupFront::new(true, 64 << 20);
        let w = 60_000_000i64;
        f.note_created(7);
        for i in 0..10u64 {
            f.insert(7, &fh(i), i, i, 1_000_000, 1_000_000 - w);
        }
        assert!(f.stats().bytes > 0);
        // The floor reaches the newest stamp but not past it: all kept.
        f.sweep_all(1_000_000 + w);
        assert!(f.stats().bytes > 0);
        for i in 0..10u64 {
            assert!(f.should_probe(7, &fh(i), 1_000_000 - w), "hash {i} lost");
        }
        // One µs later the whole generation is out of window, untouched —
        // and with nothing left in window the partition is forgotten (B26):
        // its next push seeds it again.
        f.sweep_all(1_000_001 + w);
        assert_eq!(f.stats().bytes, 0);
        assert_eq!(f.stats().partitions, 0);
        assert!(f.needs_seed(7));
    }

    #[test]
    fn the_sweeper_leaves_a_partition_whose_window_it_does_not_know() {
        let f = DedupFront::new(true, 64 << 20);
        let hashes: Vec<SeedHash> = (0..100)
            .map(|i| SeedHash {
                hash: fh(i),
                created_us: 1_000 + i as i64,
                base_off: i,
                msg_off: i,
            })
            .collect();
        f.install_seed(8, Seed::Complete(hashes), 500);
        f.sweep_all(i64::MAX - 1);
        for i in 0..100u64 {
            assert!(f.should_probe(8, &fh(i), 500), "seeded hash {i} swept");
        }
    }

    #[test]
    fn a_fallback_partition_recovers_once_its_window_is_quiet() {
        let f = DedupFront::new(true, 128); // below one generation: every roll falls back
        let w = 60_000_000i64;
        let mut out = Vec::new();
        f.note_created(5);
        f.insert(5, &fh(1), 1, 1, 1_000_000, 1_000_000 - w);
        assert_eq!(f.stats().fallback_partitions, 1);
        // Planned while in fallback: moves the quiet mark forward.
        f.insert(5, &fh(2), 2, 2, 2_000_000, 2_000_000 - w);
        assert_eq!(
            f.probe_plan(5, &fh(2), 1_500_000, &mut out),
            ProbeVerdict::Whole
        );
        f.sweep_all(1_500_000 + w);
        assert_eq!(f.stats().fallback_partitions, 1);
        // Past both stamps: empty and exact again, by the sweeper — which, the
        // partition having nothing in window, forgets it (B26): seeded afresh
        // (here, empty) on its next push...
        f.sweep_all(2_000_001 + w);
        let s = f.stats();
        assert_eq!((s.fallback_partitions, s.fallback_recovered), (0, 1));
        assert_eq!(s.partitions, 0);
        assert!(f.needs_seed(5));
        f.install_seed(5, Seed::Complete(Vec::new()), 2_000_001);
        assert_eq!(
            f.probe_plan(5, &fh(2), 2_000_001, &mut out),
            ProbeVerdict::Skip
        );
        // ...or on the next touch.
        f.insert(5, &fh(3), 3, 3, 3_000_000, 3_000_000 - w);
        assert_eq!(f.stats().fallback_partitions, 1);
        assert_eq!(
            f.probe_plan(5, &fh(3), 3_000_001, &mut out),
            ProbeVerdict::Skip
        );
        assert_eq!(f.stats().fallback_recovered, 2);
    }

    #[test]
    fn an_overflowed_seed_never_recovers_by_going_quiet() {
        let f = DedupFront::new(true, 64 << 20);
        let mut out = Vec::new();
        f.install_seed(11, Seed::Overflow, 0);
        f.insert(11, &fh(1), 1, 1, 1_000, 0);
        f.sweep_all(i64::MAX - 1);
        assert_eq!(
            f.probe_plan(11, &fh(9), i64::MAX - 1, &mut out),
            ProbeVerdict::Whole
        );
        assert_eq!(f.stats().fallback_recovered, 0);
    }

    #[test]
    fn sweeps_and_recoveries_never_skip_an_in_window_hash() {
        // A few partitions written in bursts, a second apart, a 5 s window, a
        // cap that forces fallbacks, and a sweep every other second at a clock
        // that lags the planner's: no in-window hash may ever come back Skip.
        let w = 5_000_000i64;
        let f = DedupFront::new(true, 6 * 600);
        let mut out = Vec::new();
        let mut planned: Vec<(u64, u64, i64)> = Vec::new(); // (pid, hash, created)
        let mut n = 0u64;
        for t in 1..200i64 {
            let now = t * 1_000_000;
            let floor = now - w;
            for pid in 0..8u64 {
                // Active ten seconds in thirty: quiet long enough to recover.
                if !(t as u64 / 10 + pid).is_multiple_of(3) {
                    continue;
                }
                if f.needs_seed(pid) {
                    f.note_created(pid);
                }
                for _ in 0..(1 + (t as u64 * 7 + pid) % 40) {
                    n += 1;
                    f.insert(pid, &fh(n), n, n, now, floor);
                    planned.push((pid, n, now));
                }
            }
            if t % 2 == 0 {
                f.sweep_all(now - 1);
            }
            for &(pid, h, created) in &planned {
                if created >= floor {
                    assert_ne!(
                        f.probe_plan(pid, &fh(h), floor, &mut out),
                        ProbeVerdict::Skip,
                        "t={t} pid={pid} hash={h} created={created}"
                    );
                }
            }
            planned.retain(|&(_, _, c)| c >= floor);
            let s = f.stats();
            assert_eq!(
                (s.partitions, s.fallback_partitions),
                f.walk_counts(),
                "t={t}"
            );
        }
        let s = f.stats();
        assert!(
            s.fallback_recovered > 0,
            "the cap never forced a recovery: {s:?}"
        );
    }

    #[test]
    fn install_seed_overflow_marks_fallback() {
        let f = DedupFront::new(true, 64 << 20);
        f.install_seed(11, Seed::Overflow, 0);
        assert!(!f.needs_seed(11));
        assert_eq!(f.stats().fallback_partitions, 1);
        assert!(f.should_probe(11, &fh(1), i64::MIN));
    }

    /// Two fronts fed the same stream, one call per hash and one call per
    /// command (B31): the verdicts, the bands and the counters agree.
    #[test]
    fn one_lock_per_command_plans_and_inserts_exactly_as_one_call_per_hash() {
        struct R(u64);
        impl R {
            fn below(&mut self, n: u64) -> u64 {
                self.0 ^= self.0 << 13;
                self.0 ^= self.0 >> 7;
                self.0 ^= self.0 << 17;
                self.0 % n.max(1)
            }
        }
        // A roomy cap, and a tiny one that sends partitions to fallback.
        for cap in [64usize << 20, 4 << 10] {
            let (a, b) = (DedupFront::new(true, cap), DedupFront::new(true, cap));
            let w = 10_000_000i64;
            let mut rng = R(0xB31 ^ cap as u64);
            let mut now = 1_000_000_000i64;
            let mut next_off = [0u64; 8];
            let (mut ranges, mut bands, mut plans) = (Vec::new(), Vec::new(), Vec::new());
            for pid in 0..8u64 {
                if pid < 6 {
                    a.note_created(pid);
                    b.note_created(pid);
                }
            }
            for round in 0..400u64 {
                now += rng.below(400_000) as i64;
                let floor = now - w;
                let pid = rng.below(8);
                let n = 1 + rng.below(40);
                let hashes: Vec<[u8; 16]> = (0..n).map(|_| fh(rng.below(3_000))).collect();
                // Probe: every hash, one call each vs one call.
                let planned =
                    b.probe_plan_many(pid, hashes.iter(), floor, w, &mut bands, &mut plans);
                assert_eq!(planned, !a.needs_seed(pid), "round {round}: seeded");
                if planned {
                    for (h, plan) in hashes.iter().zip(&plans) {
                        let v = a.probe_plan(pid, h, floor, &mut ranges);
                        assert_eq!(v, plan.verdict, "round {round}");
                        assert_eq!(&ranges[..], &bands[plan.from..plan.to], "round {round}");
                    }
                }
                // Insert the survivors (here: the first half), one append.
                let base = next_off[pid as usize];
                let survivors: Vec<([u8; 16], u64)> = hashes
                    .iter()
                    .take((n as usize).div_ceil(2))
                    .enumerate()
                    .map(|(k, h)| (*h, base + k as u64))
                    .collect();
                next_off[pid as usize] += survivors.len() as u64;
                for (h, off) in &survivors {
                    a.insert(pid, h, base, *off, now, floor);
                }
                b.insert_many(
                    pid,
                    survivors.iter().map(|(h, o)| (h, *o)),
                    base,
                    now,
                    floor,
                );
                assert_eq!(a.stats(), b.stats(), "round {round}");
            }
        }
    }

    #[test]
    fn a_deleted_partition_is_forgotten_at_once() {
        let f = DedupFront::new(true, 64 << 20);
        for pid in 0..4u64 {
            f.note_created(pid);
            f.insert(pid, &fh(pid), 0, 0, 1_000_000, 0);
        }
        let s0 = f.stats();
        assert_eq!(s0.partitions, 4);
        f.forget(&[1, 3, 99]);
        let s = f.stats();
        assert_eq!(s.partitions, 2);
        assert!(s.bytes < s0.bytes, "their filters are given back");
        assert!(f.needs_seed(1) && f.needs_seed(3) && !f.needs_seed(0));
        assert_eq!((s.partitions, s.fallback_partitions), f.walk_counts());
    }

    #[test]
    fn install_seed_complete_covers_its_hashes() {
        let f = DedupFront::new(true, 64 << 20);
        let hashes: Vec<SeedHash> = (0..1000)
            .map(|i| SeedHash {
                hash: fh(i),
                created_us: 1_000 + i as i64,
                base_off: i,
                msg_off: i,
            })
            .collect();
        f.install_seed(12, Seed::Complete(hashes), 500);
        assert!(!f.needs_seed(12));
        for i in 0..1000u64 {
            assert!(f.should_probe(12, &fh(i), 500), "seeded hash {i} skipped");
        }
    }
}
