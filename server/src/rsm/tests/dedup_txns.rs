//! PERF-E — the `txns` dedup authority against a real store, and the difffuzz
//! that proves it is EXACT against the `rows` authority (`QUEEN_RAFT_DEDUP_INDEX
//! = txns | rows`).
//!
//! The two indexes carry the same `(offset, created_at)` facts — `rows` keyed
//! by `(pid, hash)`, `txns` keyed by `(pid, base_offset)` — so every reader
//! must answer identically. These tests use the primitives directly
//! ([`dedup::record_rows`] / [`dedup::record_txns`], [`dedup::probe_one`] /
//! [`dedup::scan_txns_for_hash*`], [`dedup::resolve`] / [`dedup::resolve_txns`])
//! rather than the mode-gated `record`, so they are deterministic and never
//! touch the process-wide record mode a concurrent test might read.

use crate::rsm::dedup::{self, AckRes, DedupFront, ProbeVerdict, Seed, SeedHash, TxnsRow};
use crate::rsm::store::{keys, HeedStore, Keyspace, Reads, Store, Writes};

use super::store::TempStore;

const WINDOW: i64 = 3_600_000_000; // 1 h in µs, the product default

/// A well-mixed 16-byte hash from a counter (block choice and probe bits of the
/// bloom read disjoint halves of the 128-bit space).
fn fh(n: u64) -> [u8; 16] {
    let x = (n.wrapping_mul(0x9E37_79B9_7F4A_7C15)) as u128
        | ((n.wrapping_mul(0xD1B5_4A32_D192_ED03) as u128) << 64);
    x.to_le_bytes()
}

/// Record one append into BOTH keyspaces (the `rows` path also writes the txns
/// row, so one store carries both authorities for a like-for-like compare).
fn record_both(s: &HeedStore, pid: u64, base: u64, hashes: &[[u8; 16]], created: i64) {
    let end = base + hashes.len() as u64 - 1;
    let accepted: Vec<([u8; 16], u64)> = hashes
        .iter()
        .enumerate()
        .map(|(i, h)| (*h, base + i as u64))
        .collect();
    let mut w = s.write().unwrap();
    dedup::record_rows(&mut w, pid, base, end, &accepted, created).unwrap();
    w.commit().unwrap();
}

/// Record one append into ONLY the txns keyspace (the PERF-E write path).
fn record_txns_only(s: &HeedStore, pid: u64, base: u64, hashes: &[[u8; 16]], created: i64) {
    let end = base + hashes.len() as u64 - 1;
    let accepted: Vec<([u8; 16], u64)> = hashes
        .iter()
        .enumerate()
        .map(|(i, h)| (*h, base + i as u64))
        .collect();
    let mut w = s.write().unwrap();
    dedup::record_txns(&mut w, pid, base, end, &accepted, created).unwrap();
    w.commit().unwrap();
}

/// The `rows` authority push verdict (the min in-window occurrence).
fn probe_rows(s: &HeedStore, pid: u64, hash: &[u8; 16], floor: i64) -> Option<u64> {
    s.read(|r| dedup::probe_one(r, pid, hash, floor)).unwrap()
}

/// The `txns` authority push verdict, whole-window (cold filter).
fn probe_txns_whole(s: &HeedStore, pid: u64, hash: &[u8; 16], floor: i64) -> Option<u64> {
    s.read(|r| dedup::scan_txns_for_hash_whole(r, pid, hash, floor))
        .unwrap()
}

/// The `txns` authority push verdict THROUGH a warm front: `probe_plan` names
/// the generation bands, and the scan is bounded to them (or whole on Skip /
/// fallback). This is exactly the planner's `dedup_probe_txns`.
fn probe_txns_front(
    front: &DedupFront,
    s: &HeedStore,
    pid: u64,
    hash: &[u8; 16],
    floor: i64,
) -> Option<u64> {
    let mut ranges: Vec<(u64, u64)> = Vec::new();
    let verdict = front.probe_plan(pid, hash, floor, &mut ranges);
    s.read(|r| match verdict {
        ProbeVerdict::Skip => Ok(None),
        ProbeVerdict::Ranges => dedup::scan_txns_for_hash(r, pid, hash, &ranges, floor),
        ProbeVerdict::Whole => dedup::scan_txns_for_hash_whole(r, pid, hash, floor),
    })
    .unwrap()
}

/// Seed a fresh front for `pid` from the committed txns rows (mirrors the
/// planner's `front_collect_seed` / `install_seed`).
fn seed_front(front: &DedupFront, s: &HeedStore, pid: u64, floor: i64) {
    let prefix = keys::txns_prefix(pid);
    let mut seeds: Vec<SeedHash> = Vec::new();
    s.read(|r| {
        r.scan_raw(Keyspace::Txns, &prefix, &prefix, usize::MAX, &mut |k, v| {
            let base = keys::txns_base_of(k).unwrap();
            let row = TxnsRow::decode(v).unwrap();
            if row.created_at_us >= floor {
                for (i, h) in row.iter_hashes().enumerate() {
                    seeds.push(SeedHash {
                        hash: h,
                        created_us: row.created_at_us,
                        base_off: base,
                        msg_off: base + i as u64,
                    });
                }
            }
            true
        })?;
        Ok(())
    })
    .unwrap();
    front.install_seed(pid, Seed::Complete(seeds), floor);
}

/// The `rows` authority ack resolve.
fn resolve_rows(
    s: &HeedStore,
    pid: u64,
    hash: &[u8; 16],
    lo: u64,
    hi: u64,
    committed: i64,
) -> AckRes {
    s.read(|r| dedup::resolve(r, pid, hash, lo, hi, committed))
        .unwrap()
}

/// The `txns` authority ack resolve.
fn resolve_txns(
    s: &HeedStore,
    pid: u64,
    hash: &[u8; 16],
    lo: u64,
    hi: u64,
    committed: i64,
    txns_start: u64,
) -> AckRes {
    s.read(|r| dedup::resolve_txns(r, pid, hash, lo, hi, committed, txns_start))
        .unwrap()
}

// ---------------------------------------------------------------------------
// Named exactness cases
// ---------------------------------------------------------------------------

#[test]
fn txns_only_writes_no_dedup_rows_but_still_probes() {
    let t = TempStore::new();
    record_txns_only(t.s(), 1, 0, &[fh(1), fh(2)], 1_000);
    record_txns_only(t.s(), 1, 2, &[fh(1)], 2_000); // a re-push of fh(1)
    t.s()
        .read(|r| {
            // No per-message rows at all — the whole point of PERF-E.
            assert_eq!(
                r.count(Keyspace::Dedup)?,
                0,
                "txns mode writes no dedup rows"
            );
            assert_eq!(r.count(Keyspace::Txns)?, 2);
            // The txns authority still answers the min occurrence.
            assert_eq!(dedup::scan_txns_for_hash_whole(r, 1, &fh(1), 0)?, Some(0));
            assert_eq!(dedup::scan_txns_for_hash_whole(r, 1, &fh(2), 0)?, Some(1));
            assert_eq!(dedup::scan_txns_for_hash_whole(r, 1, &fh(9), 0)?, None);
            Ok(())
        })
        .unwrap();
}

#[test]
fn multi_occurrence_hash_takes_the_first_over_both_authorities() {
    let t = TempStore::new();
    // fh(1) at offsets 0, 5 and 9; the probe must answer 0 either way.
    record_both(t.s(), 1, 0, &[fh(1)], 1_000);
    record_both(t.s(), 1, 5, &[fh(1)], 2_000);
    record_both(t.s(), 1, 9, &[fh(1)], 3_000);
    assert_eq!(probe_rows(t.s(), 1, &fh(1), 0), Some(0));
    assert_eq!(probe_txns_whole(t.s(), 1, &fh(1), 0), Some(0));
    // And 005 over the leased batch (5, 9]: below-cursor at 5, eff at 9.
    let a = resolve_rows(t.s(), 1, &fh(1), 6, 9, 5);
    let b = resolve_txns(t.s(), 1, &fh(1), 6, 9, 5, 0);
    assert_eq!(a, b);
    assert_eq!(a.eff, Some(9));
    assert!(a.below);
}

#[test]
fn a_hash_outside_the_window_is_new_over_both_authorities() {
    let t = TempStore::new();
    record_both(t.s(), 1, 0, &[fh(1)], 1_000);
    // now well past the window: 1_000 is below floor = now - WINDOW.
    let floor = 1_000 + WINDOW + 1;
    assert_eq!(probe_rows(t.s(), 1, &fh(1), floor), None);
    assert_eq!(probe_txns_whole(t.s(), 1, &fh(1), floor), None);
}

#[test]
fn hash_lists_outlive_retention_deleted_segments_over_txns() {
    // The D10 / WP-0.4 case: the segment is gone (log_start moved) but the hash
    // list is still inside the txns window (txns_start did not move). A re-push
    // is still a duplicate and an ack below the cursor still resolves — the
    // txns scan reads the txns rows, indifferent to the segments.
    let t = TempStore::new();
    record_both(t.s(), 1, 0, &[fh(1), fh(2)], 1_000);
    // txns_start stays 0 though a retention would have advanced log_start.
    assert_eq!(probe_txns_whole(t.s(), 1, &fh(1), 0), Some(0));
    let res = resolve_txns(t.s(), 1, &fh(1), 0, 1, 1, 0);
    assert!(res.below, "an ack below the cursor still resolves");
}

#[test]
fn prune_trims_the_txns_authority_in_step_with_rows() {
    let t = TempStore::new();
    for i in 0..6u64 {
        record_both(t.s(), 1, i, &[fh(i as u64 + 1)], 1_000 + i as i64);
    }
    // Prune appends 0..=3 (cutoff 1_004 covers created 1_000..1_003).
    {
        let mut w = t.s().write().unwrap();
        let step = dedup::prune(&mut w, 1, 0, 1_004, 100).unwrap();
        assert_eq!(step.txns_start, 4);
        w.commit().unwrap();
    }
    // The pruned hashes are gone from BOTH authorities; the survivors resolve.
    for i in 0..6u64 {
        let h = fh(i + 1);
        let rows = probe_rows(t.s(), 1, &h, 0);
        let txns = probe_txns_whole(t.s(), 1, &h, 0);
        assert_eq!(rows, txns, "authorities diverged for fh({})", i + 1);
        if i < 4 {
            assert_eq!(rows, None, "pruned fh({}) is gone", i + 1);
        } else {
            assert_eq!(rows, Some(i), "survivor fh({}) at {i}", i + 1);
        }
    }
}

#[test]
fn a_warm_front_and_a_cold_scan_agree() {
    // The generation offset bands must bound the scan without ever missing an
    // in-window occurrence: warm-front probe == whole-window probe.
    let t = TempStore::new();
    let front = DedupFront::new(true, 64 << 20);
    front.note_created(1); // born seeded, then filled at "plan" time below
    let n = 20_000u64; // spans several tiered generations
    for i in 0..n {
        record_txns_only(t.s(), 1, i, &[fh(i)], 1_000 + i as i64);
        front.insert(1, &fh(i), i, i, 1_000 + i as i64, i64::MIN);
    }
    // Every recorded hash: warm front and cold whole-scan give the same offset.
    for i in (0..n).step_by(7) {
        let warm = probe_txns_front(&front, t.s(), 1, &fh(i), i64::MIN);
        let cold = probe_txns_whole(t.s(), 1, &fh(i), i64::MIN);
        assert_eq!(warm, cold, "warm/cold diverged at recorded fh({i})");
        assert_eq!(warm, Some(i));
    }
    // Never-seen hashes: both say new (the front mostly Skips, saving the scan).
    for i in n..n + 2_000 {
        let warm = probe_txns_front(&front, t.s(), 1, &fh(i), i64::MIN);
        assert_eq!(warm, None, "false duplicate for unseen fh({i})");
    }
}

#[test]
fn a_seeded_front_matches_the_whole_scan_across_a_simulated_restart() {
    // On restart the front is empty; the planner rebuilds it lazily from the
    // txns rows. A front seeded that way must answer exactly as the cold scan.
    let t = TempStore::new();
    let n = 5_000u64;
    for i in 0..n {
        // Two messages per append, so generation boundaries fall mid-append.
        record_txns_only(
            t.s(),
            1,
            i * 2,
            &[fh(i * 2), fh(i * 2 + 1)],
            1_000 + i as i64,
        );
    }
    let front = DedupFront::new(true, 64 << 20);
    seed_front(&front, t.s(), 1, i64::MIN);
    for i in (0..n * 2).step_by(3) {
        let warm = probe_txns_front(&front, t.s(), 1, &fh(i), i64::MIN);
        let cold = probe_txns_whole(t.s(), 1, &fh(i), i64::MIN);
        assert_eq!(warm, cold, "seeded/cold diverged at fh({i})");
        assert_eq!(warm, Some(i), "the mid-append band still finds fh({i})");
    }
}

// ---------------------------------------------------------------------------
// The difffuzz: 50 seeds, rows vs txns, 0 divergences
// ---------------------------------------------------------------------------

/// SplitMix64, written out so the corpus is identical on every machine (the
/// same PRNG the codec fuzz uses).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next() % n
        }
    }
}

#[test]
fn difffuzz_txns_vs_rows_50_seeds() {
    const PIDS: u64 = 4;
    let mut divergences = 0u64;
    for seed in 0..50u64 {
        let mut rng = Rng(0xD1FF_0000_0000_0001 ^ seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let t = TempStore::new();
        // Per-pid running state: next base offset, monotone created stamp,
        // txns_start after prunes, and the pool of hashes seen (for dup reuse).
        let mut next_base = [0u64; PIDS as usize];
        let mut created = [1_000i64; PIDS as usize];
        let mut txns_start = [0u64; PIDS as usize];
        let mut seen: Vec<Vec<u64>> = vec![Vec::new(); PIDS as usize];
        let mut counter = 0u64; // distinct-hash source

        // ---- build a random committed history, into BOTH keyspaces.
        let appends = 200 + rng.below(200);
        for _ in 0..appends {
            let pid = rng.below(PIDS);
            let p = pid as usize;
            let count = 1 + rng.below(4);
            let mut hashes = Vec::new();
            for _ in 0..count {
                // 30% reuse an earlier hash (a duplicate at a fresh offset),
                // else a brand-new hash.
                let n = if !seen[p].is_empty() && rng.below(10) < 3 {
                    seen[p][rng.below(seen[p].len() as u64) as usize]
                } else {
                    counter += 1;
                    let n = counter;
                    seen[p].push(n);
                    n
                };
                hashes.push(fh(n));
            }
            // created strictly monotone per pid (PUSHSER), and it jumps by a
            // random amount so some appends age past a floor.
            created[p] += 1 + rng.below(1_000_000) as i64;
            let base = next_base[p];
            record_both(t.s(), pid, base, &hashes, created[p]);
            next_base[p] = base + count;

            // Occasionally prune a pid up to a random cutoff.
            if rng.below(20) == 0 {
                let cutoff = created[p] - rng.below(2_000_000) as i64;
                let mut w = t.s().write().unwrap();
                let mut start = txns_start[p];
                loop {
                    let step = dedup::prune(&mut w, pid, start, cutoff, 64).unwrap();
                    start = step.txns_start;
                    if step.exhausted {
                        break;
                    }
                }
                w.commit().unwrap();
                txns_start[p] = start;
            }
        }

        // ---- probe queries: rows vs txns (cold) vs txns (warm front).
        // One warm front per pid, seeded from the committed txns rows.
        let front = DedupFront::new(true, 256 << 20);
        for pid in 0..PIDS {
            seed_front(&front, t.s(), pid, i64::MIN);
        }
        let queries = 400 + rng.below(400);
        for _ in 0..queries {
            let pid = rng.below(PIDS);
            let p = pid as usize;
            // A hash that exists (~70%) or a never-recorded one (~30%).
            let hash = if !seen[p].is_empty() && rng.below(10) < 7 {
                fh(seen[p][rng.below(seen[p].len() as u64) as usize])
            } else {
                fh(1_000_000_000 + rng.next())
            };
            // A floor: sometimes wide open, sometimes mid-history.
            let floor = if rng.below(3) == 0 {
                i64::MIN
            } else {
                1_000 + rng.below((created[p] as u64).max(1)) as i64
            };
            let rows = probe_rows(t.s(), pid, &hash, floor);
            let cold = probe_txns_whole(t.s(), pid, &hash, floor);
            let warm = probe_txns_front(&front, t.s(), pid, &hash, floor);
            if rows != cold || rows != warm {
                divergences += 1;
                eprintln!(
                    "seed {seed} pid {pid} hash probe divergence: rows={rows:?} cold={cold:?} warm={warm:?} floor={floor}"
                );
            }

            // ---- resolve queries: rows vs txns.
            let hi_base = next_base[p].max(1);
            let committed = rng.below(hi_base + 1) as i64 - 1; // -1..=last
            let ts = txns_start[p];
            let lo = ((committed + 1).max(ts as i64)).max(0) as u64;
            let hi = if rng.below(2) == 0 {
                lo + rng.below(hi_base + 1)
            } else {
                u64::MAX
            };
            let rr = resolve_rows(t.s(), pid, &hash, lo, hi, committed);
            let rt = resolve_txns(t.s(), pid, &hash, lo, hi, committed, ts);
            if rr != rt {
                divergences += 1;
                eprintln!(
                    "seed {seed} pid {pid} resolve divergence: rows={rr:?} txns={rt:?} lo={lo} hi={hi} committed={committed} txns_start={ts}"
                );
            }
        }
    }
    assert_eq!(divergences, 0, "txns and rows authorities diverged");
}

// ---------------------------------------------------------------------------
// PERF-E laptop measurement (ignored): probe cost warm vs cold, filter bytes
// per hash, and the store-put count per message before/after. Run:
//   cargo test -p queen-engine --lib \
//     rsm::tests::dedup_txns::measure_probe_cost -- --ignored --nocapture
// The 60 s A20k / FAT100 goload smokes with the PERF-1 histograms are the
// coordinator's combined-binary pass; this isolates the dedup read/write cost.
// ---------------------------------------------------------------------------

fn pctl(v: &mut [u128], q: f64) -> u128 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    let i = ((v.len() as f64 - 1.0) * q).round() as usize;
    v[i]
}

#[test]
#[ignore = "laptop measurement; run explicitly with --nocapture"]
fn measure_probe_cost() {
    use std::time::Instant;

    // An A20k-shaped window: 10 messages/append, 5 000 appends = 50 000 msgs,
    // all distinct hashes (the dominant real shape: every message new).
    const APPENDS: u64 = 5_000;
    const BATCH: u64 = 10;
    let msgs = APPENDS * BATCH;

    let t = TempStore::new();
    let front = DedupFront::new(true, 512 << 20);
    front.note_created(1);
    let mut counter = 0u64;
    for a in 0..APPENDS {
        let base = a * BATCH;
        let mut hashes = Vec::with_capacity(BATCH as usize);
        for _ in 0..BATCH {
            counter += 1;
            hashes.push(fh(counter));
        }
        record_txns_only(t.s(), 1, base, &hashes, 1_000 + a as i64);
        for (i, h) in hashes.iter().enumerate() {
            front.insert(1, h, base, base + i as u64, 1_000 + a as i64, i64::MIN);
        }
    }

    // Probe corpus: half hits (a recorded hash), half misses (never seen).
    let mut hits: Vec<[u8; 16]> = (1..=2_000u64).map(|n| fh(n * 17 % counter + 1)).collect();
    let mut misses: Vec<[u8; 16]> = (0..2_000u64).map(|n| fh(10_000_000 + n)).collect();
    hits.append(&mut misses);
    let corpus = hits;

    // Warm: the front bounds a hit to one generation band and Skips a miss.
    let mut warm_ns: Vec<u128> = Vec::with_capacity(corpus.len());
    for h in &corpus {
        let s = Instant::now();
        let _ = probe_txns_front(&front, t.s(), 1, h, i64::MIN);
        warm_ns.push(s.elapsed().as_nanos());
    }
    // Cold: no filter — the whole-window scan (restart / unseeded partition).
    let mut cold_ns: Vec<u128> = Vec::with_capacity(corpus.len());
    for h in &corpus {
        let s = Instant::now();
        let _ = probe_txns_whole(t.s(), 1, h, i64::MIN);
        cold_ns.push(s.elapsed().as_nanos());
    }

    let st = front.stats();
    let bytes_per_hash = st.bytes as f64 / msgs as f64;
    let skip_frac = st.probes_skipped as f64 / st.messages.max(1) as f64;

    eprintln!("--- PERF-E dedup=txns laptop measurement ({msgs} msgs, {APPENDS} appends, batch {BATCH}) ---");
    eprintln!(
        "probe WARM (front-bounded):  p50 {:>7} ns  p99 {:>8} ns",
        pctl(&mut warm_ns, 0.50),
        pctl(&mut warm_ns, 0.99)
    );
    eprintln!(
        "probe COLD (whole window):   p50 {:>7} ns  p99 {:>8} ns  (scans up to {APPENDS} appends)",
        pctl(&mut cold_ns, 0.50),
        pctl(&mut cold_ns, 0.99)
    );
    eprintln!(
        "filter: {bytes_per_hash:.2} B/hash resident, {:.1}% of probes skipped (no scan), {} B total",
        skip_frac * 100.0,
        st.bytes
    );
    // Store-put accounting (exact, from the record paths):
    //   rows:  per message 1 Dedup get + 1 Dedup put, plus 1 Txns put per append
    //          → ~2 + 1/BATCH store ops/msg   ({:.2} at batch {BATCH})
    //   txns:  1 Txns put per append only      → 1/BATCH store ops/msg   ({:.2})
    let rows_ops = 2.0 + 1.0 / BATCH as f64;
    let txns_ops = 1.0 / BATCH as f64;
    eprintln!(
        "store dedup ops/msg:  rows {rows_ops:.2}  →  txns {txns_ops:.2}  ({:.0}x fewer at batch {BATCH})",
        rows_ops / txns_ops
    );
    t.s()
        .read(|r| {
            eprintln!(
                "keyspace rows: Dedup {}  Txns {}  (txns mode leaves Dedup empty)",
                r.count(Keyspace::Dedup)?,
                r.count(Keyspace::Txns)?
            );
            Ok(())
        })
        .unwrap();
}
