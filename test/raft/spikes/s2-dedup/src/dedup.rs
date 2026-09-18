//! The two dedup designs of D10 / WP-0.4, behind one trait.
//!
//! Both answer the two questions the SQL spec asks (the semantics are the
//! specification, not this harness):
//!
//!   * 003 `log_push_one_v1`: for each incoming hash, is there an occurrence in
//!     this partition inside the dedup window, and at which ORIGINAL offset?
//!     (MIN occurrence; the verdict writes nothing.)
//!   * 005 `log_ack_by_hash_v1`: for one hash, the MIN offset inside the
//!     ackable span and whether ANY occurrence sits at or below the cursor
//!     (the noop/stale answer). No time filter here — the ack path reads the
//!     whole txns window, which is why the hash records must outlive the
//!     segments retention deletes (D10, §11.7).
//!
//! Option (a) `StoreIndex`: one store row per (partition, hash) carrying the
//! occurrence list; expiry by an index on created_at.
//! Option (b) `HashLists`: the hash list stays inside the segment frame; a
//! bloom per file and a bounded recent cache keep the probe out of the files.

use crate::cache::RecentCache;
use crate::engines::*;
use crate::files::FileTable;
use crate::seg::{Pos, Segments};

/// Partition state (§6.1 `partitions`), kept in RAM by the driver and written
/// to the store on every append.
#[derive(Clone, Copy)]
pub struct Part {
    pub last_offset: u64,
    pub log_start: u64,
    pub txns_start: u64,
    pub committed: u64,
    pub last_created: i64,
    pub bucket: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckRes {
    /// MIN offset inside the ackable span (None = unresolvable there).
    pub eff: Option<u64>,
    /// Any occurrence at or below the cursor (005 below-cursor honesty).
    pub below: bool,
}

pub struct Ctx<'a> {
    pub eng: &'a dyn Engine,
    pub ops: &'a mut OpBuf,
    pub segs: &'a mut Segments,
    pub files: &'a mut FileTable,
}

#[derive(Default)]
pub struct Counters {
    pub gets: u64,
    pub puts: u64,
    pub dels: u64,
    pub scan_rows: u64,
    pub bloom_tests: u64,
    pub bloom_hits: u64,
    pub bloom_false_hits: u64,
    pub frame_reads: u64,
    pub cache_vouched: u64,
    pub cache_maybe: u64,
    pub disk_probes: u64,
    /// key+value bytes this design writes for dedup ALONE (the frame's hash
    /// list counts for option (b)), so the two can be compared without the
    /// engine's own overhead.
    pub logical_bytes: u64,
}

pub trait Dedup {
    fn name(&self) -> &'static str;
    /// Does the payload frame have to carry the hash list?
    fn frame_hashes(&self) -> bool;
    /// 003's probe. `out[i]` = the original offset when hash i is a duplicate.
    #[allow(clippy::too_many_arguments)]
    fn probe(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        part: &Part,
        hashes: &[u128],
        now_us: i64,
        out: &mut Vec<Option<u64>>,
    ) -> Result<(), String>;
    /// Records an accepted append: `accepted[i] = (hash, absolute offset)`.
    #[allow(clippy::too_many_arguments)]
    fn record(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        base: u64,
        accepted: &[(u128, u64)],
        now_us: i64,
        pos: &Pos,
        end: u64,
    ) -> Result<(), String>;
    /// 005's ack-by-hash resolution.
    fn resolve(&mut self, c: &mut Ctx, pid: u64, part: &Part, hash: u128)
        -> Result<AckRes, String>;
    /// 005's ack-by-hash resolution for a WHOLE ack: the `p_hashes` array of
    /// one `log_ack_by_hash_v1` call comes from ONE partition and is answered
    /// in ONE pass ("ONE join, ONE materialized resolved set", 005 header).
    /// The default is the per-hash loop, which is what option (a) genuinely
    /// costs (one point get per hash, exactly the SQL's index lookups) and is
    /// what the 2026-09-17/18 campaign measured for BOTH options.
    fn resolve_batch(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        part: &Part,
        hashes: &[u128],
        out: &mut Vec<AckRes>,
    ) -> Result<(), String> {
        out.clear();
        for h in hashes {
            let r = self.resolve(c, pid, part, *h)?;
            out.push(r);
        }
        Ok(())
    }
    /// The same question as `probe`, answered the slow exhaustive way (no
    /// cache, no blooms, every record in the partition's txns window). Used
    /// only to audit a miss: if this finds what `probe` did not, the fast path
    /// is wrong; if it finds nothing either, the record is genuinely gone.
    fn audit(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        part: &Part,
        hash: u128,
        now_us: i64,
    ) -> Result<Option<u64>, String>;
    /// Every occurrence of `hash` this design can still find in `pid`, as
    /// (offset, created_at), with NO time filter. Diagnostic only: it is what
    /// answers "did the probe return a DIFFERENT, later occurrence?" when a
    /// verdict disagrees with the oracle (WP-0.4 part 2).
    fn explain(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        part: &Part,
        hash: u128,
    ) -> Result<Vec<(u64, i64)>, String>;

    /// Bounded prune of everything stamped before `cutoff_us`.
    fn prune(
        &mut self,
        c: &mut Ctx,
        parts: &mut [Part],
        cutoff_us: i64,
        budget: usize,
    ) -> Result<u64, String>;
    fn ram_bytes(&self) -> usize;
    fn counters(&self) -> &Counters;
    fn extra(&self) -> Vec<(String, String)>;
}

// ---------------------------------------------------------------- keys ------

pub fn k_pid_off(pid: u64, off: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[0..8].copy_from_slice(&pid.to_be_bytes());
    k[8..16].copy_from_slice(&off.to_be_bytes());
    k
}

fn k_dedup(pid: u64, h: u128) -> [u8; 24] {
    let mut k = [0u8; 24];
    k[0..8].copy_from_slice(&pid.to_be_bytes());
    k[8..24].copy_from_slice(&h.to_be_bytes());
    k
}

fn k_expiry(created: i64, pid: u64, h: u128) -> [u8; 32] {
    let mut k = [0u8; 32];
    k[0..8].copy_from_slice(&created.to_be_bytes());
    k[8..16].copy_from_slice(&pid.to_be_bytes());
    k[16..32].copy_from_slice(&h.to_be_bytes());
    k
}

/// txns locator row (option b): what survives retention so the hash list in
/// the file stays reachable.
fn v_txns(end: u64, created: i64, pos: &Pos, count: u32) -> [u8; 38] {
    let mut v = [0u8; 38];
    v[0..8].copy_from_slice(&end.to_le_bytes());
    v[8..16].copy_from_slice(&created.to_le_bytes());
    v[16..18].copy_from_slice(&pos.bucket.to_le_bytes());
    v[18..22].copy_from_slice(&pos.file_id.to_le_bytes());
    v[22..30].copy_from_slice(&pos.offset.to_le_bytes());
    v[30..34].copy_from_slice(&pos.len.to_le_bytes());
    v[34..38].copy_from_slice(&count.to_le_bytes());
    v
}

pub struct TxnRow {
    pub base: u64,
    pub end: u64,
    pub created: i64,
    pub pos: Pos,
}

pub fn parse_txns(k: &[u8], v: &[u8]) -> TxnRow {
    TxnRow {
        base: u64::from_be_bytes(k[8..16].try_into().unwrap()),
        end: u64::from_le_bytes(v[0..8].try_into().unwrap()),
        created: i64::from_le_bytes(v[8..16].try_into().unwrap()),
        pos: Pos {
            bucket: u16::from_le_bytes(v[16..18].try_into().unwrap()),
            file_id: u32::from_le_bytes(v[18..22].try_into().unwrap()),
            offset: u64::from_le_bytes(v[22..30].try_into().unwrap()),
            len: u32::from_le_bytes(v[30..34].try_into().unwrap()),
        },
    }
}

// =================================================== option (a): store index

pub struct StoreIndex {
    window_us: i64,
    probed: Vec<(u128, Option<Vec<u8>>)>,
    prune_cursor: Vec<u8>,
    c: Counters,
    pub rows: i64,
    pub multi_occurrence: u64,
    /// `--option a-lean`: drop the 32-byte-key `(created_at, pid, hash)`
    /// expiry index (40 of option (a)'s 79.6 logical B/message, one random
    /// put per message and one delete per message at prune time) and expire
    /// through ONE sequential `txns` row per Append instead — key
    /// `(pid, base_offset)`, value `[end][created][hashes]` — walked per
    /// partition from its own watermark, exactly like 006's purge step and
    /// like option (b)'s locator rows. The probe and the ack resolution are
    /// byte-identical to plain (a): only the expiry mechanism changes.
    pub lean: bool,
    lean_cursor: usize,
    pub prune_partitions: usize,
}

impl StoreIndex {
    pub fn new(window_us: i64) -> Self {
        Self {
            window_us,
            probed: Vec::new(),
            prune_cursor: Vec::new(),
            c: Counters::default(),
            rows: 0,
            multi_occurrence: 0,
            lean: false,
            lean_cursor: 0,
            prune_partitions: 32,
        }
    }

    fn occurrences(v: &[u8]) -> impl Iterator<Item = (u64, i64)> + '_ {
        v.chunks_exact(16).map(|c| {
            (
                u64::from_le_bytes(c[0..8].try_into().unwrap()),
                i64::from_le_bytes(c[8..16].try_into().unwrap()),
            )
        })
    }
}

impl StoreIndex {
    /// `--option a-lean`'s expiry: 006's rotating per-partition purge step over
    /// the Append-keyed `txns` rows. Per expired Append: one sequential row
    /// read and one sequential delete, then the unavoidable random
    /// `get`+`delete` per hash on the `(pid, hash)` row the probe needs.
    fn prune_lean(
        &mut self,
        c: &mut Ctx,
        parts: &mut [Part],
        cutoff_us: i64,
        budget: usize,
    ) -> Result<u64, String> {
        let np = parts.len();
        let mut done = 0u64;
        let mut visited = 0usize;
        let max_visits = self.prune_partitions.min(np);
        while done < budget as u64 && visited < max_visits {
            let pid = self.lean_cursor % np;
            self.lean_cursor = self.lean_cursor.wrapping_add(1);
            visited += 1;
            let part = parts[pid];
            let prefix = (pid as u64).to_be_bytes();
            let from = k_pid_off(pid as u64, part.txns_start);
            let mut victims: Vec<(u64, Vec<u128>)> = Vec::new();
            let mut newstart = part.txns_start;
            let n = c.eng.range(T_TXNS, &from, &prefix, 64, &mut |k, v| {
                let base = u64::from_be_bytes(k[8..16].try_into().unwrap());
                let end = u64::from_le_bytes(v[0..8].try_into().unwrap());
                let created = i64::from_le_bytes(v[8..16].try_into().unwrap());
                if created >= cutoff_us {
                    return false;
                }
                let hs: Vec<u128> = v[16..]
                    .chunks_exact(16)
                    .map(|ch| u128::from_le_bytes(ch.try_into().unwrap()))
                    .collect();
                victims.push((base, hs));
                newstart = end + 1;
                true
            })?;
            self.c.scan_rows += n;
            for (base, hs) in &victims {
                for h in hs {
                    let dk = k_dedup(pid as u64, *h);
                    let cur = c.eng.get(T_DEDUP, &dk)?;
                    self.c.gets += 1;
                    if let Some(b) = cur {
                        let keep: Vec<u8> = b
                            .chunks_exact(16)
                            .filter(|ch| {
                                i64::from_le_bytes(ch[8..16].try_into().unwrap()) >= cutoff_us
                            })
                            .flat_map(|ch| ch.to_vec())
                            .collect();
                        if keep.is_empty() {
                            c.ops.del(T_DEDUP, &dk);
                            self.c.dels += 1;
                            self.rows -= 1;
                        } else if keep.len() != b.len() {
                            c.ops.put(T_DEDUP, &dk, &keep);
                            self.c.puts += 1;
                        }
                    }
                    done += 1;
                }
                c.ops.del(T_TXNS, &k_pid_off(pid as u64, *base));
                self.c.dels += 1;
            }
            parts[pid].txns_start = newstart;
        }
        Ok(done)
    }
}

impl Dedup for StoreIndex {
    fn name(&self) -> &'static str {
        if self.lean {
            "a-lean:store-index+txns-expiry"
        } else {
            "a:store-index"
        }
    }

    fn frame_hashes(&self) -> bool {
        // The store row is the authority; the frame needs no hash list, which
        // is 16 B/message of disk this option does not pay.
        false
    }

    fn probe(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        _part: &Part,
        hashes: &[u128],
        now_us: i64,
        out: &mut Vec<Option<u64>>,
    ) -> Result<(), String> {
        out.clear();
        self.probed.clear();
        let lo = now_us - self.window_us;
        for h in hashes {
            let v = c.eng.get(T_DEDUP, &k_dedup(pid, *h))?;
            self.c.gets += 1;
            let mut best: Option<u64> = None;
            if let Some(b) = &v {
                for (off, cre) in Self::occurrences(b) {
                    if cre >= lo {
                        best = Some(best.map_or(off, |x: u64| x.min(off)));
                    }
                }
            }
            out.push(best);
            self.probed.push((*h, v));
        }
        Ok(())
    }

    fn record(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        base: u64,
        accepted: &[(u128, u64)],
        now_us: i64,
        _pos: &Pos,
        end: u64,
    ) -> Result<(), String> {
        for (h, off) in accepted {
            let mut val: Vec<u8> = match self.probed.iter().find(|(ph, _)| ph == h) {
                Some((_, Some(old))) => {
                    self.multi_occurrence += 1;
                    old.clone()
                }
                _ => {
                    self.rows += 1;
                    Vec::with_capacity(16)
                }
            };
            val.extend_from_slice(&off.to_le_bytes());
            val.extend_from_slice(&now_us.to_le_bytes());
            c.ops.put(T_DEDUP, &k_dedup(pid, *h), &val);
            if self.lean {
                self.c.puts += 1;
                self.c.logical_bytes += (24 + val.len()) as u64;
            } else {
                c.ops.put(
                    T_DEDUP_EXPIRY,
                    &k_expiry(now_us, pid, *h),
                    &off.to_le_bytes(),
                );
                self.c.puts += 2;
                self.c.logical_bytes += (24 + val.len() + 32 + 8) as u64;
            }
        }
        if self.lean && !accepted.is_empty() {
            // ONE sequential row per Append: [end][created][hash]*n.
            let mut v = Vec::with_capacity(16 + 16 * accepted.len());
            v.extend_from_slice(&end.to_le_bytes());
            v.extend_from_slice(&now_us.to_le_bytes());
            for (h, _) in accepted {
                v.extend_from_slice(&h.to_le_bytes());
            }
            self.c.logical_bytes += (16 + v.len()) as u64;
            self.c.puts += 1;
            c.ops.put(T_TXNS, &k_pid_off(pid, base), &v);
        }
        Ok(())
    }

    fn resolve(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        part: &Part,
        hash: u128,
    ) -> Result<AckRes, String> {
        let v = c.eng.get(T_DEDUP, &k_dedup(pid, hash))?;
        self.c.gets += 1;
        let mut eff: Option<u64> = None;
        let mut below = false;
        if let Some(b) = &v {
            let lo = part.committed + 1;
            for (off, _cre) in Self::occurrences(b) {
                if off <= part.committed {
                    below = true;
                }
                if off >= lo && off <= part.last_offset {
                    eff = Some(eff.map_or(off, |x: u64| x.min(off)));
                }
            }
        }
        Ok(AckRes { eff, below })
    }

    fn audit(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        part: &Part,
        hash: u128,
        now_us: i64,
    ) -> Result<Option<u64>, String> {
        // The store index IS the exhaustive answer.
        let mut out = Vec::new();
        self.probe(c, pid, part, &[hash], now_us, &mut out)?;
        Ok(out[0])
    }

    fn explain(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        _part: &Part,
        hash: u128,
    ) -> Result<Vec<(u64, i64)>, String> {
        let v = c.eng.get(T_DEDUP, &k_dedup(pid, hash))?;
        Ok(v.map(|b| Self::occurrences(&b).collect())
            .unwrap_or_default())
    }

    fn prune(
        &mut self,
        c: &mut Ctx,
        parts: &mut [Part],
        cutoff_us: i64,
        budget: usize,
    ) -> Result<u64, String> {
        if self.lean {
            return self.prune_lean(c, parts, cutoff_us, budget);
        }
        let mut victims: Vec<([u8; 32], u64, u128, i64)> = Vec::with_capacity(budget.min(4096));
        // Resume key, not "scan from the start": every key before it has
        // already been deleted, and on an LSM a scan from the start walks over
        // every tombstone it left behind until compaction removes them (the
        // same reason 006 carries a watermark per partition). Sound because
        // the key is (created_at, pid, hash) and new rows are always stamped
        // after the cutoff this walk is draining.
        let from = if self.prune_cursor.is_empty() {
            vec![0u8; 0]
        } else {
            self.prune_cursor.clone()
        };
        let mut stop = false;
        let n = c
            .eng
            .range(T_DEDUP_EXPIRY, &from, &[], budget, &mut |k, v| {
                let created = i64::from_be_bytes(k[0..8].try_into().unwrap());
                if created >= cutoff_us {
                    stop = true;
                    return false;
                }
                let pid = u64::from_be_bytes(k[8..16].try_into().unwrap());
                let h = u128::from_be_bytes(k[16..32].try_into().unwrap());
                let mut kk = [0u8; 32];
                kk.copy_from_slice(k);
                let _ = v;
                victims.push((kk, pid, h, created));
                true
            })?;
        self.c.scan_rows += n;
        if let Some((k, _, _, _)) = victims.last() {
            self.prune_cursor.clear();
            self.prune_cursor.extend_from_slice(k);
            // strictly after the last deleted key
            self.prune_cursor.push(0);
        }
        if stop && victims.is_empty() {
            // caught up with the cutoff: keep the cursor where it is, the next
            // rows to expire are the ones just above it.
        }
        for (ek, pid, h, _created) in &victims {
            let dk = k_dedup(*pid, *h);
            let cur = c.eng.get(T_DEDUP, &dk)?;
            self.c.gets += 1;
            if let Some(b) = cur {
                let keep: Vec<u8> = b
                    .chunks_exact(16)
                    .filter(|ch| i64::from_le_bytes(ch[8..16].try_into().unwrap()) >= cutoff_us)
                    .flat_map(|ch| ch.to_vec())
                    .collect();
                if keep.is_empty() {
                    c.ops.del(T_DEDUP, &dk);
                    self.c.dels += 1;
                    self.rows -= 1;
                } else if keep.len() != b.len() {
                    c.ops.put(T_DEDUP, &dk, &keep);
                    self.c.puts += 1;
                }
            }
            c.ops.del(T_DEDUP_EXPIRY, ek);
            self.c.dels += 1;
        }
        Ok(victims.len() as u64)
    }

    fn ram_bytes(&self) -> usize {
        // Nothing resident by design: the index is the store's problem.
        0
    }

    fn counters(&self) -> &Counters {
        &self.c
    }

    fn extra(&self) -> Vec<(String, String)> {
        vec![
            ("dedup rows".into(), format!("{}", self.rows)),
            (
                "rows with >1 occurrence".into(),
                format!("{}", self.multi_occurrence),
            ),
            (
                "expiry".into(),
                if self.lean {
                    "txns rows per Append (a-lean)".into()
                } else {
                    "(created_at,pid,hash) index".to_string()
                },
            ),
        ]
    }
}

// ============================================= option (b): hash lists + blooms

pub struct HashLists {
    window_us: i64,
    pub cache: RecentCache,
    verify_frames: bool,
    /// Stop an ack-by-hash resolution at the first occurrence found. Exact only
    /// while a hash occurs at most once in the partition's txns window — a
    /// second occurrence needs a re-push AFTER the dedup window expired but
    /// inside the txns window, which 003 accepts. Measured separately.
    pub ack_early_stop: bool,
    /// How many partitions one prune step may LOOK AT. The walk resumes per
    /// partition at its own `txns_start`, but without a cap a step with a small
    /// row budget wanders through thousands of partitions that have nothing to
    /// drop (measured: 4.7 ms per entry at 4096 partitions, 65 % of the run).
    pub prune_partitions: usize,
    prune_cursor: usize,
    c: Counters,
    cand: Vec<u32>,
    hits: Vec<(u32, usize)>,
    hbuf: Vec<u128>,
    pub rows: i64,
    pub scan_rows_skipped: u64,
}

impl HashLists {
    pub fn new(window_us: i64, cache: RecentCache, verify_frames: bool) -> Self {
        Self {
            window_us,
            cache,
            verify_frames,
            ack_early_stop: false,
            prune_partitions: 32,
            prune_cursor: 0,
            c: Counters::default(),
            cand: Vec::new(),
            hits: Vec::new(),
            hbuf: Vec::new(),
            rows: 0,
            scan_rows_skipped: 0,
        }
    }
}

impl Dedup for HashLists {
    fn name(&self) -> &'static str {
        "b:hash-lists+blooms"
    }

    fn frame_hashes(&self) -> bool {
        true
    }

    fn probe(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        part: &Part,
        hashes: &[u128],
        now_us: i64,
        out: &mut Vec<Option<u64>>,
    ) -> Result<(), String> {
        out.clear();
        out.resize(hashes.len(), None);
        let lo = now_us - self.window_us;
        // 1. the recent cache: a negative is authoritative for
        //    [covered_from, now], so a hash it rules out only has to be looked
        //    for in files older than that floor.
        let floor = if self.cache.enabled() {
            self.cache.covered_from_us()
        } else {
            now_us
        };
        let mut hi_for: Vec<i64> = Vec::with_capacity(hashes.len());
        let mut any = false;
        for h in hashes {
            let maybe = self.cache.maybe_contains(*h);
            if maybe {
                self.c.cache_maybe += 1;
                hi_for.push(now_us);
            } else {
                self.c.cache_vouched += 1;
                hi_for.push(floor);
            }
            if hi_for[hi_for.len() - 1] >= lo {
                any = true;
            }
        }
        if !any {
            return Ok(()); // the cache covered the whole window: no disk at all
        }
        // 2. blooms of the files of this partition's bucket overlapping the span
        c.files.overlapping(part.bucket, lo, now_us, &mut self.cand);
        self.hits.clear();
        for fid in &self.cand {
            let Some(fi) = c.files.get(part.bucket, *fid) else {
                continue;
            };
            let Some(bl) = &fi.bloom else { continue };
            for (i, h) in hashes.iter().enumerate() {
                if fi.min_created > hi_for[i] || fi.max_created < lo {
                    continue;
                }
                self.c.bloom_tests += 1;
                if bl.contains(*h) {
                    self.c.bloom_hits += 1;
                    self.hits.push((*fid, i));
                }
            }
        }
        if self.hits.is_empty() {
            return Ok(());
        }
        // 3. exact: walk this partition's txns rows once, read the hash lists
        //    of the frames that live in a hit file.
        self.c.disk_probes += 1;
        let mut rows: Vec<TxnRow> = Vec::new();
        let prefix = pid.to_be_bytes();
        let from = k_pid_off(pid, part.txns_start);
        let mut skipped = 0u64;
        let n = c.eng.range(T_TXNS, &from, &prefix, 1 << 20, &mut |k, v| {
            let r = parse_txns(k, v);
            if r.created < lo {
                skipped += 1;
                return true;
            }
            if self
                .hits
                .iter()
                .any(|(f, _)| *f == r.pos.file_id && r.pos.bucket == part.bucket)
            {
                rows.push(r);
            }
            true
        })?;
        self.c.scan_rows += n;
        self.scan_rows_skipped += skipped;
        for r in &rows {
            let (_, base, created) =
                c.segs
                    .read_hashes(&r.pos, self.verify_frames, &mut self.hbuf)?;
            self.c.frame_reads += 1;
            if created < lo {
                continue;
            }
            for (fid, i) in self.hits.iter() {
                if *fid != r.pos.file_id {
                    continue;
                }
                if let Some(j) = self.hbuf.iter().position(|x| *x == hashes[*i]) {
                    let off = base + j as u64;
                    out[*i] = Some(out[*i].map_or(off, |x: u64| x.min(off)));
                }
            }
        }
        for (_, i) in self.hits.iter() {
            if out[*i].is_none() {
                self.c.bloom_false_hits += 1;
            }
        }
        Ok(())
    }

    fn record(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        base: u64,
        accepted: &[(u128, u64)],
        now_us: i64,
        pos: &Pos,
        end: u64,
    ) -> Result<(), String> {
        let hs: Vec<u128> = accepted.iter().map(|(h, _)| *h).collect();
        c.ops.put(
            T_TXNS,
            &k_pid_off(pid, base),
            &v_txns(end, now_us, pos, accepted.len() as u32),
        );
        self.c.puts += 1;
        self.rows += 1;
        self.c.logical_bytes += (16 + 38 + 16 * accepted.len()) as u64;
        self.cache.insert_run(&hs, now_us);
        Ok(())
    }

    fn resolve(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        part: &Part,
        hash: u128,
    ) -> Result<AckRes, String> {
        // No time filter (005): the whole txns window, bounded above by the
        // leased batch end — here the partition head.
        c.files
            .overlapping(part.bucket, i64::MIN, i64::MAX, &mut self.cand);
        self.hits.clear();
        for fid in &self.cand {
            let Some(fi) = c.files.get(part.bucket, *fid) else {
                continue;
            };
            let Some(bl) = &fi.bloom else { continue };
            self.c.bloom_tests += 1;
            if bl.contains(hash) {
                self.c.bloom_hits += 1;
                self.hits.push((*fid, 0));
            }
        }
        if self.hits.is_empty() {
            return Ok(AckRes {
                eff: None,
                below: false,
            });
        }
        self.c.disk_probes += 1;
        let mut rows: Vec<TxnRow> = Vec::new();
        let prefix = pid.to_be_bytes();
        let from = k_pid_off(pid, part.txns_start);
        let n = c.eng.range(T_TXNS, &from, &prefix, 1 << 20, &mut |k, v| {
            let r = parse_txns(k, v);
            if self
                .hits
                .iter()
                .any(|(f, _)| *f == r.pos.file_id && r.pos.bucket == part.bucket)
            {
                rows.push(r);
            }
            true
        })?;
        self.c.scan_rows += n;
        let mut eff: Option<u64> = None;
        let mut below = false;
        for r in &rows {
            let (_, base, _created) =
                c.segs
                    .read_hashes(&r.pos, self.verify_frames, &mut self.hbuf)?;
            self.c.frame_reads += 1;
            if let Some(j) = self.hbuf.iter().position(|x| *x == hash) {
                let off = base + j as u64;
                if off <= part.committed {
                    below = true;
                } else if off <= part.last_offset {
                    eff = Some(eff.map_or(off, |x: u64| x.min(off)));
                }
                if self.ack_early_stop {
                    break;
                }
            }
        }
        if eff.is_none() && !below {
            self.c.bloom_false_hits += 1;
        }
        Ok(AckRes { eff, below })
    }

    /// 005's resolution of a WHOLE ack in one pass: the union of the input
    /// hashes' bloom candidates, ONE ordered scan of the partition's `txns`
    /// rows, and ONE read of each frame those rows point at, with every input
    /// hash matched against the frame's hash list while it is in hand. This is
    /// the shape of the SQL (`occ` is exploded once and joined once); the
    /// per-hash `resolve` above is the same work repeated per hash.
    fn resolve_batch(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        part: &Part,
        hashes: &[u128],
        out: &mut Vec<AckRes>,
    ) -> Result<(), String> {
        out.clear();
        out.resize(
            hashes.len(),
            AckRes {
                eff: None,
                below: false,
            },
        );
        if hashes.is_empty() {
            return Ok(());
        }
        c.files
            .overlapping(part.bucket, i64::MIN, i64::MAX, &mut self.cand);
        self.hits.clear();
        for fid in &self.cand {
            let Some(fi) = c.files.get(part.bucket, *fid) else {
                continue;
            };
            let Some(bl) = &fi.bloom else { continue };
            let mut hit = false;
            for h in hashes {
                self.c.bloom_tests += 1;
                if bl.contains(*h) {
                    hit = true;
                    break;
                }
            }
            if hit {
                self.c.bloom_hits += 1;
                self.hits.push((*fid, 0));
            }
        }
        if self.hits.is_empty() {
            return Ok(());
        }
        self.c.disk_probes += 1;
        let mut rows: Vec<TxnRow> = Vec::new();
        let prefix = pid.to_be_bytes();
        let from = k_pid_off(pid, part.txns_start);
        let n = c.eng.range(T_TXNS, &from, &prefix, 1 << 20, &mut |k, v| {
            let r = parse_txns(k, v);
            if self
                .hits
                .iter()
                .any(|(f, _)| *f == r.pos.file_id && r.pos.bucket == part.bucket)
            {
                rows.push(r);
            }
            true
        })?;
        self.c.scan_rows += n;
        let mut any = false;
        for r in &rows {
            let (_, base, _created) =
                c.segs
                    .read_hashes(&r.pos, self.verify_frames, &mut self.hbuf)?;
            self.c.frame_reads += 1;
            for (i, h) in hashes.iter().enumerate() {
                if let Some(j) = self.hbuf.iter().position(|x| *x == *h) {
                    let off = base + j as u64;
                    if off <= part.committed {
                        out[i].below = true;
                        any = true;
                    } else if off <= part.last_offset {
                        out[i].eff = Some(out[i].eff.map_or(off, |x: u64| x.min(off)));
                        any = true;
                    }
                }
            }
        }
        if !any {
            self.c.bloom_false_hits += 1;
        }
        Ok(())
    }

    fn audit(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        part: &Part,
        hash: u128,
        now_us: i64,
    ) -> Result<Option<u64>, String> {
        let lo = now_us - self.window_us;
        let mut rows: Vec<TxnRow> = Vec::new();
        let prefix = pid.to_be_bytes();
        let from = k_pid_off(pid, part.txns_start);
        c.eng.range(T_TXNS, &from, &prefix, 1 << 20, &mut |k, v| {
            let r = parse_txns(k, v);
            if r.created >= lo {
                rows.push(r);
            }
            true
        })?;
        let mut best: Option<u64> = None;
        for r in &rows {
            let (_, base, created) = c.segs.read_hashes(&r.pos, false, &mut self.hbuf)?;
            if created < lo {
                continue;
            }
            if let Some(j) = self.hbuf.iter().position(|x| *x == hash) {
                let off = base + j as u64;
                best = Some(best.map_or(off, |x: u64| x.min(off)));
            }
        }
        Ok(best)
    }

    fn explain(
        &mut self,
        c: &mut Ctx,
        pid: u64,
        part: &Part,
        hash: u128,
    ) -> Result<Vec<(u64, i64)>, String> {
        let mut rows: Vec<TxnRow> = Vec::new();
        let prefix = pid.to_be_bytes();
        let from = k_pid_off(pid, part.txns_start);
        c.eng.range(T_TXNS, &from, &prefix, 1 << 20, &mut |k, v| {
            rows.push(parse_txns(k, v));
            true
        })?;
        let mut out = Vec::new();
        for r in &rows {
            let (_, base, created) = c.segs.read_hashes(&r.pos, false, &mut self.hbuf)?;
            for (j, x) in self.hbuf.iter().enumerate() {
                if *x == hash {
                    out.push((base + j as u64, created));
                }
            }
        }
        Ok(out)
    }

    fn prune(
        &mut self,
        c: &mut Ctx,
        parts: &mut [Part],
        cutoff_us: i64,
        budget: usize,
    ) -> Result<u64, String> {
        // Rotating per-partition walk: the txns purge of 006
        // (log_txns_purge_step_v1) with its own watermark.
        let np = parts.len();
        let mut done = 0u64;
        let mut visited = 0usize;
        let max_visits = self.prune_partitions.min(np);
        while done < budget as u64 && visited < max_visits {
            let pid = self.prune_cursor % np;
            self.prune_cursor = self.prune_cursor.wrapping_add(1);
            visited += 1;
            let part = parts[pid];
            let prefix = (pid as u64).to_be_bytes();
            let from = k_pid_off(pid as u64, part.txns_start);
            let mut victims: Vec<(u64, u64, Pos)> = Vec::new();
            let mut newstart = part.txns_start;
            c.eng.range(T_TXNS, &from, &prefix, 256, &mut |k, v| {
                let r = parse_txns(k, v);
                if r.created >= cutoff_us {
                    return false;
                }
                victims.push((r.base, r.end, r.pos));
                newstart = r.end + 1;
                true
            })?;
            for (base, _end, pos) in &victims {
                c.ops.del(T_TXNS, &k_pid_off(pid as u64, *base));
                self.c.dels += 1;
                c.files.release_txn(pos.bucket, pos.file_id);
                self.rows -= 1;
                done += 1;
            }
            parts[pid].txns_start = newstart;
        }
        self.cache.expire(cutoff_us);
        Ok(done)
    }

    fn ram_bytes(&self) -> usize {
        self.cache.ram_bytes()
    }

    fn counters(&self) -> &Counters {
        &self.c
    }

    fn extra(&self) -> Vec<(String, String)> {
        vec![
            ("txns rows".into(), format!("{}", self.rows)),
            (
                "cache RAM".into(),
                format!("{:.1} MiB", self.cache.ram_bytes() as f64 / 1048576.0),
            ),
            (
                "cache blocks dropped".into(),
                format!("{}", self.cache.blocks_dropped),
            ),
            (
                "cache floor age at end".into(),
                format!("{}", self.cache.covered_from_us()),
            ),
            (
                "txns rows skipped by scans".into(),
                format!("{}", self.scan_rows_skipped),
            ),
        ]
    }
}
