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

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

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
// arrival). The front removes it exactly the way `server/src/dedup.rs`'s bloom
// front removes the postgres cache's exact scan: a per-partition ring of
// generational register-blocked bloom filters that answers, for a hash,
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
// Two facts keep it, both mirrored from the postgres front:
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
// drop cannot lose an in-window hash (postgres invariant 4 at generation
// grain). Per-partition memory therefore tracks the window, not all of history.
// A global byte cap ([`DedupFront::byte_cap`], `QUEEN_RAFT_DEDUP_FRONT_MB`)
// bounds the total: a partition that would grow the front past the cap is
// dropped to FALLBACK (always-probe) instead — the same always-sound resource
// trade as the postgres cache's SUPPRESSED state. At 16 bits/hash the front
// costs ~2 B per in-window message; the smoke reports the measured figure.

/// Bits per expected hash a generation is sized for (its filter is
/// `cap × 2` bytes). Matches the postgres front (16 bits, k=7).
const FRONT_BITS_PER_HASH: usize = 16;
/// A generation's fixed overhead (boxed words + counters + deque slot).
const FRONT_BYTES_PER_GEN: usize = 64;
/// Smallest generation capacity, in hashes (a 8 KiB filter). Powers of two are
/// not required here — a generation is filled by counting, never by a no-copy
/// buffer seal — but keeping it a round number keeps `nblocks` clean.
const FRONT_GEN_CAP_MIN: usize = 4096;
/// Largest generation capacity (a 2 MiB filter), tiering ×8 from the minimum so
/// an idle partition pays one tiny filter and a hot one settles at a few big
/// generations.
const FRONT_GEN_CAP_MAX: usize = 1 << 20;
const FRONT_GEN_TIER: usize = 8;
/// Default global cap in MiB (`QUEEN_RAFT_DEDUP_FRONT_MB`). 512 MiB fronts
/// ~256 M in-window hashes at 2 B each before any partition falls back.
pub const FRONT_DEFAULT_CAP_MB: usize = 512;
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
    fn insert(&mut self, h: u128, created_us: i64) {
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
}

impl PartFront {
    fn seeded() -> PartFront {
        PartFront {
            fallback: false,
            gens: VecDeque::new(),
            gen_next_cap: 0,
            bytes: 0,
        }
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

    fn needs_new_gen(&self) -> bool {
        match self.gens.back() {
            Some(g) => g.len >= g.cap,
            None => true,
        }
    }

    fn next_gen_bytes(&self) -> usize {
        let cap = if self.gen_next_cap == 0 {
            FRONT_GEN_CAP_MIN
        } else {
            self.gen_next_cap
        };
        // `cap × 16 bits`, rounded to whole 512-bit blocks, plus the header.
        let cap = cap.max(32);
        (cap / 32) * 8 * 8 + FRONT_BYTES_PER_GEN
    }

    /// Insert `h`, opening (and tiering) a new generation when the current is
    /// full. Returns the bytes added (a new generation's, else 0).
    fn insert(&mut self, h: u128, created_us: i64) -> usize {
        let mut added = 0;
        if self.needs_new_gen() {
            let cap = if self.gen_next_cap == 0 {
                FRONT_GEN_CAP_MIN
            } else {
                self.gen_next_cap
            };
            let g = FrontGen::new(cap);
            self.gen_next_cap = (g.cap * FRONT_GEN_TIER).min(FRONT_GEN_CAP_MAX);
            added = g.bytes();
            self.bytes += added;
            self.gens.push_back(g);
        }
        self.gens.back_mut().unwrap().insert(h, created_us);
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
    /// Hashes inserted at plan time.
    pub inserts: u64,
    /// Partitions currently tracked (seeded + fallback).
    pub partitions: u64,
    /// Of those, in the always-probe fallback state.
    pub fallback_partitions: u64,
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
/// owned by the batcher across planning cycles. All mutation happens under the
/// single planning thread, so the map lock is uncontended; it exists only to
/// make the front `Sync` for the `spawn_blocking` hand-off.
pub struct DedupFront {
    enabled: bool,
    byte_cap: usize,
    parts: Mutex<HashMap<Pid, PartFront>>,
    total_bytes: AtomicU64,
    // counters (own source of truth; mirrored to timing::metrics on publish)
    messages: AtomicU64,
    probes_issued: AtomicU64,
    probes_skipped: AtomicU64,
    inserts: AtomicU64,
}

/// The seed the planner collected for a pre-existing partition.
pub enum Seed {
    /// The full in-window union fit under the cap: `(hash, created_us)` in
    /// created order.
    Complete(Vec<([u8; 16], i64)>),
    /// The scan hit [`FRONT_SEED_MAX`]; mark the partition always-probe.
    Overflow,
}

impl DedupFront {
    pub fn new(enabled: bool, byte_cap: usize) -> DedupFront {
        DedupFront {
            enabled,
            byte_cap,
            parts: Mutex::new(HashMap::new()),
            total_bytes: AtomicU64::new(0),
            messages: AtomicU64::new(0),
            probes_issued: AtomicU64::new(0),
            probes_skipped: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
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

    /// Drop everything: the leadership-change hook (see the module note). Every
    /// partition re-seeds from committed state on its next touch.
    pub fn reset(&self) {
        if !self.enabled {
            return;
        }
        self.parts.lock().unwrap().clear();
        self.total_bytes.store(0, Ordering::Relaxed);
    }

    /// True iff this partition still needs a first-touch seed scan (it is
    /// neither seeded nor fallback). Cheap check the planner does once per push
    /// command before its per-hash loop.
    pub fn needs_seed(&self, pid: Pid) -> bool {
        if !self.enabled {
            return false;
        }
        !self.parts.lock().unwrap().contains_key(&pid)
    }

    /// A partition minted this front-lifetime: born seeded, empty and complete.
    pub fn note_created(&self, pid: Pid) {
        if !self.enabled {
            return;
        }
        self.parts
            .lock()
            .unwrap()
            .entry(pid)
            .or_insert_with(PartFront::seeded);
    }

    /// Install a first-touch seed the planner collected. `Complete` builds the
    /// generations (respecting the global cap); `Overflow` marks fallback.
    pub fn install_seed(&self, pid: Pid, seed: Seed, floor_us: i64) {
        if !self.enabled {
            return;
        }
        let mut parts = self.parts.lock().unwrap();
        // A concurrent path never runs (single planning thread), but a second
        // seed of the same pid this cycle is a no-op.
        if parts.contains_key(&pid) {
            return;
        }
        match seed {
            Seed::Overflow => {
                let mut pf = PartFront::seeded();
                pf.fallback = true;
                parts.insert(pid, pf);
            }
            Seed::Complete(hashes) => {
                let mut pf = PartFront::seeded();
                for (h, created) in hashes {
                    if created < floor_us {
                        continue; // out of window: never a duplicate, skip
                    }
                    let cap = self.byte_cap as u64;
                    if pf.needs_new_gen()
                        && self.total_bytes.load(Ordering::Relaxed) + pf.next_gen_bytes() as u64
                            > cap
                    {
                        // Cap pressure mid-seed: give up on this partition.
                        let freed = pf.mark_fallback();
                        self.total_bytes.fetch_sub(freed as u64, Ordering::Relaxed);
                        break;
                    }
                    let added = pf.insert(u128::from_le_bytes(h), created);
                    if added > 0 {
                        self.total_bytes.fetch_add(added as u64, Ordering::Relaxed);
                    }
                }
                parts.insert(pid, pf);
            }
        }
    }

    /// The per-message decision: should the planner issue the LMDB committed
    /// probe for `(pid, hash)`? `false` means "certainly absent from the
    /// committed window — treat as new". Counts every call as a message.
    #[inline]
    pub fn should_probe(&self, pid: Pid, hash: &[u8; 16], floor_us: i64) -> bool {
        self.messages.fetch_add(1, Ordering::Relaxed);
        if !self.enabled {
            self.probes_issued.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        let h = u128::from_le_bytes(*hash);
        let mut parts = self.parts.lock().unwrap();
        let issue = match parts.get_mut(&pid) {
            None => true, // not seeded: probe (planner seeds first)
            Some(pf) if pf.fallback => true,
            Some(pf) => {
                let freed = pf.age(floor_us);
                if freed > 0 {
                    self.total_bytes.fetch_sub(freed as u64, Ordering::Relaxed);
                }
                pf.maybe(h)
            }
        };
        if issue {
            self.probes_issued.fetch_add(1, Ordering::Relaxed);
        } else {
            self.probes_skipped.fetch_add(1, Ordering::Relaxed);
        }
        issue
    }

    /// Record a planned (survivor) append hash. Keeps the front a superset of
    /// the committed index it fronts (see the module note). No-op for a
    /// fallback or unseeded partition.
    #[inline]
    pub fn insert(&self, pid: Pid, hash: &[u8; 16], created_us: i64, floor_us: i64) {
        if !self.enabled {
            return;
        }
        let h = u128::from_le_bytes(*hash);
        let mut parts = self.parts.lock().unwrap();
        let Some(pf) = parts.get_mut(&pid) else {
            return;
        };
        if pf.fallback {
            return;
        }
        let freed = pf.age(floor_us);
        if freed > 0 {
            self.total_bytes.fetch_sub(freed as u64, Ordering::Relaxed);
        }
        if pf.needs_new_gen() {
            let cap = self.byte_cap as u64;
            if self.total_bytes.load(Ordering::Relaxed) + pf.next_gen_bytes() as u64 > cap {
                let freed = pf.mark_fallback();
                self.total_bytes.fetch_sub(freed as u64, Ordering::Relaxed);
                return;
            }
        }
        let added = pf.insert(h, created_us);
        if added > 0 {
            self.total_bytes.fetch_add(added as u64, Ordering::Relaxed);
        }
        self.inserts.fetch_add(1, Ordering::Relaxed);
    }

    /// A snapshot of the counters and the resident footprint.
    pub fn stats(&self) -> FrontStats {
        let parts = self.parts.lock().unwrap();
        let fallback = parts.values().filter(|p| p.fallback).count() as u64;
        FrontStats {
            messages: self.messages.load(Ordering::Relaxed),
            probes_issued: self.probes_issued.load(Ordering::Relaxed),
            probes_skipped: self.probes_skipped.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            partitions: parts.len() as u64,
            fallback_partitions: fallback,
            bytes: self.total_bytes.load(Ordering::Relaxed),
        }
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
    fn front_never_says_skip_for_an_inserted_hash() {
        // No false negatives: every hash the front was told about must probe.
        let f = DedupFront::new(true, 64 << 20);
        f.note_created(7);
        let n = 50_000u64; // spans several tiered generations
        for i in 0..n {
            f.insert(7, &fh(i), 1_000 + i as i64, i64::MIN);
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
            f2.insert(1, &fh(i), 1_000, i64::MIN);
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
        f.insert(9, &fh(1), 1_000, i64::MIN); // no-op
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
            f.insert(3, &fh(i), 1_000, i64::MIN);
        }
        assert!(f.stats().bytes > 0);
        f.insert(3, &fh(1_000_000), 9_000, i64::MIN); // opens gen1 at t=9000
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
        f.insert(5, &fh(1), 1_000, i64::MIN);
        let s = f.stats();
        assert_eq!(s.fallback_partitions, 1);
        assert!(s.bytes <= 128);
        // A fallback partition probes everything, including its own hash.
        assert!(f.should_probe(5, &fh(1), i64::MIN));
        assert!(f.should_probe(5, &fh(2), i64::MIN));
        assert_eq!(f.stats().probes_skipped, 0);
    }

    #[test]
    fn install_seed_overflow_marks_fallback() {
        let f = DedupFront::new(true, 64 << 20);
        f.install_seed(11, Seed::Overflow, 0);
        assert!(!f.needs_seed(11));
        assert_eq!(f.stats().fallback_partitions, 1);
        assert!(f.should_probe(11, &fh(1), i64::MIN));
    }

    #[test]
    fn install_seed_complete_covers_its_hashes() {
        let f = DedupFront::new(true, 64 << 20);
        let hashes: Vec<([u8; 16], i64)> = (0..1000).map(|i| (fh(i), 1_000 + i as i64)).collect();
        f.install_seed(12, Seed::Complete(hashes), 500);
        assert!(!f.needs_seed(12));
        for i in 0..1000u64 {
            assert!(f.should_probe(12, &fh(i), 500), "seeded hash {i} skipped");
        }
    }
}
