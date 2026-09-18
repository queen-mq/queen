//! S2 (PLAN_RAFT.md WP-0.4, D10): which dedup design can the RSM carry?
//!
//! Replays a push stream shaped like §7.1 against ONE of the two designs D10
//! names, on top of the S1 store seam:
//!
//!   (a) `--option a`: a store index per message hash → (offset, created_at),
//!       expired by window.
//!   (b) `--option b`: the hash list inside each Append record in the segment
//!       files + one bloom per file + a bounded recent cache; probe = cache,
//!       then the blooms of the files still inside the txns window, then the
//!       file's hash list.
//!
//! Both are measured on the same stream: pushes at `--rate` msg/s in batches
//! of `--batch`, `--dup-pct` of the messages replaced by duplicates whose age
//! is uniform in [0, `--dup-max-age-s`] (wider than the dedup window on
//! purpose), plus ack-by-hash probes below the cursor at `--ack-rate`/s, plus
//! retention that deletes segments long before the txns window ends — the case
//! D10 and §11.7 call out: the hashes of a deleted segment must still answer.
//!
//! Subcommands: run | rebuild | info.

mod bloom;
mod cache;
mod dedup;
mod engines;
mod files;
mod seg;
mod workload;

#[allow(dead_code)]
#[path = "../../s1-store/src/hist.rs"]
mod hist;
#[allow(dead_code)]
#[path = "../../s1-store/src/sys.rs"]
mod sys;

use cache::RecentCache;
use dedup::{k_pid_off, parse_txns, AckRes, Ctx, Dedup, HashLists, Part, StoreIndex};
use engines::*;
use files::FileTable;
use hist::Hist;
use seg::{FsyncMode, Pos, Segments};
use std::collections::{HashMap, HashSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use workload::{hash_of, Oracle, Rng, Sample};
use xxhash_rust::xxh3::xxh3_64;

// ------------------------------------------------------------------ args ----

struct Args(HashMap<String, String>, Vec<String>);

impl Args {
    fn parse() -> Self {
        let mut m = HashMap::new();
        let mut pos = Vec::new();
        let argv: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        while i < argv.len() {
            let a = &argv[i];
            if let Some(k) = a.strip_prefix("--") {
                if let Some((k, v)) = k.split_once('=') {
                    m.insert(k.to_string(), v.to_string());
                } else if i + 1 < argv.len() && !argv[i + 1].starts_with("--") {
                    m.insert(k.to_string(), argv[i + 1].clone());
                    i += 1;
                } else {
                    m.insert(k.to_string(), "1".into());
                }
            } else {
                pos.push(a.clone());
            }
            i += 1;
        }
        Args(m, pos)
    }
    fn s(&self, k: &str, d: &str) -> String {
        self.0.get(k).cloned().unwrap_or_else(|| d.to_string())
    }
    fn u(&self, k: &str, d: u64) -> u64 {
        self.0.get(k).and_then(|v| v.parse().ok()).unwrap_or(d)
    }
    fn f(&self, k: &str, d: f64) -> f64 {
        self.0.get(k).and_then(|v| v.parse().ok()).unwrap_or(d)
    }
    fn b(&self, k: &str, d: bool) -> bool {
        match self.0.get(k).map(|s| s.as_str()) {
            Some("0") | Some("false") | Some("no") => false,
            Some(_) => true,
            None => d,
        }
    }
}

fn mib(b: u64) -> f64 {
    b as f64 / (1024.0 * 1024.0)
}

// ---------------------------------------------------------------- values ----

/// `segments` row: what retention deletes (§6.1).
fn v_seg(end: u64, created: i64, bytes: u32, pos: &Pos, count: u32) -> [u8; 30] {
    let mut v = [0u8; 30];
    v[0..8].copy_from_slice(&end.to_le_bytes());
    v[8..16].copy_from_slice(&created.to_le_bytes());
    v[16..20].copy_from_slice(&bytes.to_le_bytes());
    v[20..22].copy_from_slice(&pos.bucket.to_le_bytes());
    v[22..26].copy_from_slice(&pos.file_id.to_le_bytes());
    v[26..30].copy_from_slice(&count.to_le_bytes());
    v
}

fn seg_created(v: &[u8]) -> i64 {
    i64::from_le_bytes(v[8..16].try_into().unwrap())
}
fn seg_end(v: &[u8]) -> u64 {
    u64::from_le_bytes(v[0..8].try_into().unwrap())
}
fn seg_file(v: &[u8]) -> (u16, u32) {
    (
        u16::from_le_bytes(v[20..22].try_into().unwrap()),
        u32::from_le_bytes(v[22..26].try_into().unwrap()),
    )
}

/// node-local `seg_loc` row (§6.2, D8).
fn v_loc(pos: &Pos) -> [u8; 18] {
    let mut v = [0u8; 18];
    v[0..2].copy_from_slice(&pos.bucket.to_le_bytes());
    v[2..6].copy_from_slice(&pos.file_id.to_le_bytes());
    v[6..14].copy_from_slice(&pos.offset.to_le_bytes());
    v[14..18].copy_from_slice(&pos.len.to_le_bytes());
    v
}

fn v_part(p: &Part) -> [u8; 40] {
    let mut v = [0u8; 40];
    v[0..8].copy_from_slice(&p.last_offset.to_le_bytes());
    v[8..16].copy_from_slice(&p.log_start.to_le_bytes());
    v[16..24].copy_from_slice(&p.txns_start.to_le_bytes());
    v[24..32].copy_from_slice(&p.last_created.to_le_bytes());
    v[32..40].copy_from_slice(&p.committed.to_le_bytes());
    v
}

fn bucket_of(pid: u64) -> u16 {
    (xxh3_64(&pid.to_le_bytes()) % 256) as u16
}

fn bloom_path(root: &Path, bucket: u16, id: u32) -> PathBuf {
    root.join("blooms")
        .join(format!("b{bucket:03}"))
        .join(format!("f{id:06}.bloom"))
}

// ------------------------------------------------------------------ main ----

fn main() {
    let a = Args::parse();
    let cmd = a.1.first().cloned().unwrap_or_else(|| "run".into());
    let r = match cmd.as_str() {
        "run" => cmd_run(&a),
        "rebuild" => cmd_rebuild(&a),
        "info" => {
            println!("s2-dedup: options a (store index) and b (hash lists + blooms + cache)");
            println!("engines: {:?}", engines::ALL);
            Ok(0)
        }
        other => Err(format!("unknown subcommand {other} (run|rebuild|info)")),
    };
    match r {
        Ok(c) => std::process::exit(c),
        Err(e) => {
            eprintln!("ERROR: {e}");
            std::process::exit(2);
        }
    }
}

struct P {
    rate: u64,
    batch: u64,
    entry_appends: u64,
    payload: usize,
    partitions: u64,
    dup_pct: f64,
    dup_max_age_s: i64,
    window_s: i64,
    txns_s: i64,
    retention_s: i64,
    ack_rate: u64,
    samples_per_s: usize,
    seed: u64,
}

#[allow(clippy::too_many_lines)]
fn cmd_run(a: &Args) -> Result<i32, String> {
    let opt = a.s("option", "b");
    // "a" | "a-lean" | "b"; a-lean is option (a) with the expiry index
    // replaced by one sequential txns row per Append (dedup.rs StoreIndex).
    let opt_a = opt.starts_with('a');
    let lean = opt == "a-lean";
    let engine = a.s("engine", "fjall");
    let dir = PathBuf::from(a.s("dir", "/var/tmp/s2-dedup"));
    let duration = a.f("duration", 300.0);
    let durable_ms = a.u("durable-ms", 1000);
    let segment_bytes = a.u("segment-bytes", 8 << 20);
    let map_size = (a.u("map-size-gb", 32) as usize) << 30;
    let store_cache = a.u("store-cache-mb", 256) << 20;
    let fsync_threads = a.u("fsync-threads", 1) as usize;
    let fsync_mode = FsyncMode::parse(&a.s("fsync-mode", "full"));
    let cache_mb = a.u("cache-mb", 64);
    let cache_block = a.u("cache-block", 16384) as usize;
    let bloom_bits = a.u("bloom-bits", 16) as u32;
    let verify_frames = a.b("verify-frames", false);
    let hash_compact = a.b("hash-compact", true);
    let retention_budget = a.u("retention-partitions", 8) as usize;
    let audit_misses = a.b("audit-misses", true);
    let ack_early_stop = a.b("ack-early-stop", false);
    // How many hashes one ack-by-hash resolution carries. 005 resolves the
    // WHOLE p_hashes array of one ack in one pass over one partition's rows;
    // the 2026-09-17/18 campaign asked one hash per call (= 1 here), which
    // charges each hash a full scan. The nominal hash rate stays --ack-rate:
    // the driver makes --ack-rate/--ack-batch calls per second.
    let ack_batch = a.u("ack-batch", 1).max(1) as usize;
    // The pre-fix sample selection, kept so the defect it caused can be shown
    // and re-shown: the first 4096 ack probes, and no filter for hashes the run
    // itself re-pushed. See RESULTS-vm.md sec 3.
    let legacy_samples = a.b("legacy-samples", false);
    let label = a.s("label", "");
    let p = P {
        rate: a.u("rate", 50_000),
        batch: a.u("batch", 10),
        entry_appends: a.u("entry-appends", 10),
        payload: a.u("payload", 96) as usize,
        partitions: a.u("partitions", 4096),
        dup_pct: a.f("dup-pct", 1.0),
        dup_max_age_s: a.u("dup-max-age-s", 350) as i64,
        window_s: a.u("window-s", 300) as i64,
        txns_s: a.u("txns-s", 360) as i64,
        retention_s: a.u("retention-s", 60) as i64,
        ack_rate: a.u("ack-rate", 5_000),
        samples_per_s: a.u("samples-per-s", 1024) as usize,
        seed: a.u("seed", 1),
    };
    if a.b("fresh", true) {
        let _ = std::fs::remove_dir_all(&dir);
    }
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let nofile = sys::raise_nofile(4096)?;

    let msgs_per_entry = p.batch * p.entry_appends;
    let entries_per_s = (p.rate as f64 / msgs_per_entry as f64).max(1.0);
    let window_us = p.window_s * 1_000_000;
    let txns_us = p.txns_s * 1_000_000;
    let retention_us = p.retention_s * 1_000_000;
    let expect_keys_per_file = (segment_bytes / (p.payload as u64 + 16)).max(1024);

    println!("=== s2-dedup run ===");
    println!("host        {}", sys::host_line());
    println!(
        "option      {}",
        if opt == "a-lean" {
            "(a-lean) store index per hash, expiry through per-Append txns rows"
        } else if opt_a {
            "(a) store index per hash + (created_at,pid,hash) expiry index"
        } else {
            "(b) hash lists in files + per-file blooms + recent cache"
        }
    );
    println!("engine      {engine}    dir {}", dir.display());
    println!(
        "load        {} msg/s, batch {}, {} appends/entry -> {:.0} entries/s, payload {} B, {} partitions",
        p.rate, p.batch, p.entry_appends, entries_per_s, p.payload, p.partitions
    );
    println!(
        "dedup       window {} s, txns window {} s, retention {} s, duplicates {:.1} % at ages 0..{} s, ack-by-hash {} /s",
        p.window_s, p.txns_s, p.retention_s, p.dup_pct, p.dup_max_age_s, p.ack_rate
    );
    println!(
        "structures  cache {} MiB (blocks of {} hashes), blooms {} bits/key sized for {} keys/file, frames verified on read: {}, hash-only compaction: {}, ack early stop: {}",
        cache_mb, cache_block, bloom_bits, expect_keys_per_file, verify_frames, hash_compact, ack_early_stop
    );
    println!(
        "durability  non-durable commit per entry, durable point every {durable_ms} ms; files roll at {:.0} MiB; fsync {fsync_mode:?} x{fsync_threads}; store cache {} MiB; nofile={nofile}",
        mib(segment_bytes),
        mib(store_cache)
    );
    if !label.is_empty() {
        println!("label       {label}");
    }

    let t_open = Instant::now();
    let mut eng = engines::open(&engine, &dir, map_size, store_cache)?;
    let open_ms = t_open.elapsed().as_secs_f64() * 1e3;
    let mut segs = Segments::open(&dir, segment_bytes).map_err(|e| e.to_string())?;
    let use_blooms = !opt_a;
    let mut ftab = FileTable::new(seg::NBUCKETS, expect_keys_per_file, bloom_bits, use_blooms);
    let t0_us = sys::now_micros();
    let mut dd: Box<dyn Dedup> = if opt_a {
        let mut si = StoreIndex::new(window_us);
        si.lean = lean;
        si.prune_partitions = a.u("prune-partitions", 32) as usize;
        Box::new(si)
    } else {
        let mut h = HashLists::new(
            window_us,
            RecentCache::new((cache_mb as usize) << 20, cache_block, bloom_bits, t0_us),
            verify_frames,
        );
        h.ack_early_stop = ack_early_stop;
        h.prune_partitions = a.u("prune-partitions", 32) as usize;
        Box::new(h)
    };
    let frame_hashes = dd.frame_hashes();

    let mut parts: Vec<Part> = (0..p.partitions)
        .map(|pid| Part {
            last_offset: 0,
            log_start: 0,
            txns_start: 0,
            committed: 0,
            last_created: 0,
            bucket: bucket_of(pid),
        })
        .collect();
    let mut oracle = Oracle::new(p.dup_max_age_s.max(p.txns_s), p.samples_per_s, t0_us);
    let mut rng = Rng(p.seed | 1);
    let mut seq: u64 = 0;

    let mut h_probe = Hist::new();
    let mut h_probe_disk = Hist::new();
    // h_ack keeps the per-HASH figure the campaign reported (call / batch);
    // h_ack_call is what one 005 call costs.
    let mut h_ack = Hist::new();
    let mut h_ack_call = Hist::new();
    let mut ack_calls: u64 = 0;
    let mut ack_hashes: Vec<u128> = Vec::with_capacity(64);
    let mut ack_out: Vec<AckRes> = Vec::with_capacity(64);
    let mut h_entry = Hist::new();
    let mut h_commit = Hist::new();
    let mut h_durable = Hist::new();
    let mut h_prune = Hist::new();
    let mut h_ret = Hist::new();
    let mut h_compact = Hist::new();
    let mut rss_series: Vec<(f64, u64)> = Vec::new();

    // exactness
    let (mut dup_in, mut dup_out) = (0u64, 0u64);
    let (mut dup_hit, mut dup_missed, mut dup_false, mut dup_wrong_off) = (0u64, 0u64, 0u64, 0u64);
    let mut dup_false_fresh = 0u64;
    let (mut miss_fastpath, mut miss_absent, mut miss_reported) = (0u64, 0u64, 0u64);
    let (mut ack_probes, mut ack_below_ok, mut ack_below_missing) = (0u64, 0u64, 0u64);
    let (mut ack_seg_deleted, mut ack_eff_unexpected) = (0u64, 0u64);
    let mut samples_for_rebuild: Vec<Sample> = Vec::new();
    let mut sample_seen: u64 = 0;
    // Hashes that were injected as a duplicate and ACCEPTED (their age was past
    // the dedup window, so 003 appends them): from that moment the hash has a
    // second, younger occurrence, and a sample taken from the first one no
    // longer says what the answer must be. They are dropped from samples.bin.
    let mut repushed: HashSet<u128> = HashSet::new();

    let mut msgs = 0u64;
    let mut appends = 0u64;
    let mut dup_appends = 0u64;
    let mut files_unlinked = 0u64;
    let mut bytes_unlinked = 0u64;
    let mut compactions = 0u64;
    let (mut compact_before, mut compact_after) = (0u64, 0u64);
    let mut blooms_saved = 0u64;
    let mut bloom_bytes_saved = 0u64;
    let mut pruned = 0u64;
    let mut seg_rows_deleted = 0u64;
    let mut store_ops = 0u64;

    let blob = {
        let mut v = vec![0u8; p.payload * p.batch as usize];
        for c in v.chunks_mut(8) {
            let x = rng.next().to_le_bytes();
            let n = c.len();
            c.copy_from_slice(&x[..n]);
        }
        v
    };
    let mut hashes: Vec<u128> = Vec::with_capacity(p.batch as usize);
    let mut expect: Vec<Option<Sample>> = Vec::with_capacity(p.batch as usize);
    let mut verdicts: Vec<Option<u64>> = Vec::new();
    let mut accepted: Vec<(u128, u64)> = Vec::new();
    let mut ops = OpBuf::new();
    // The planner's overlay (§7.2): the effects planned in THIS cycle, which
    // the store has not committed yet. Without it a duplicate of a message
    // pushed earlier in the same entry reads as new — the hazard §7.2 exists
    // for. It is the same for both options, so it lives in the driver.
    let mut overlay: HashMap<(u64, u128), u64> = HashMap::new();
    let mut overlay_hits = 0u64;
    let mut dead: Vec<(u16, u32)> = Vec::new();
    let mut hashonly: Vec<(u16, u32, u64)> = Vec::new();
    let mut sealed_pending: Vec<(u16, u32)> = Vec::new();

    let io0 = sys::disk_written_bytes();
    let seg_dir = segs.dir().to_path_buf();
    let store_bytes = |d: &Path, s: &Path| sys::dir_alloc_bytes(d) - sys::dir_alloc_bytes(s);
    let store0 = store_bytes(&dir, &seg_dir);
    let seg0 = sys::dir_alloc_bytes(&seg_dir);
    let rss0 = sys::rss_bytes();

    let start = Instant::now();
    let interval = Duration::from_nanos((1e9 / entries_per_s) as u64);
    let mut idx: u64 = 0;
    let mut ret_cursor = 0usize;
    let mut ack_acc = 0f64;
    // calls per entry, each carrying ack_batch hashes: the nominal hash load
    // is --ack-rate either way.
    let ack_per_entry = p.ack_rate as f64 / entries_per_s / ack_batch as f64;
    let mut last_durable = Instant::now();
    let mut last_rss = Instant::now();
    // Per-minute progress: the shortfall against the offered rate is either
    // there from the first minute (a per-entry cost above the budget) or it
    // appears when a regime starts (retention at t=retention_s, the txns prune
    // at t=txns_s). One line a minute is what tells the two apart.
    let mut last_min = Instant::now();
    let (mut min_idx0, mut min_msgs0, mut min_busy_us) = (0u64, 0u64, 0u64);
    let mut max_lag_us = 0u64;
    let mut behind = 0u64;
    let mut durable_points = 0u64;
    let mut last_now_us = t0_us;

    loop {
        let elapsed = start.elapsed();
        if elapsed.as_secs_f64() >= duration {
            break;
        }
        let target = interval.mul_f64(idx as f64);
        if target > elapsed {
            let wait = target - elapsed;
            if wait > Duration::from_micros(300) {
                std::thread::sleep(wait - Duration::from_micros(100));
            }
            while start.elapsed() < target {
                std::hint::spin_loop();
            }
        } else {
            let lag = (elapsed - target).as_micros() as u64;
            max_lag_us = max_lag_us.max(lag);
            if lag > 50_000 {
                behind += 1;
            }
        }

        let now_us = sys::now_micros().max(last_now_us + 1);
        last_now_us = now_us;
        let t_entry = Instant::now();
        ops.clear();
        overlay.clear();
        sealed_pending.clear();

        for _ in 0..p.entry_appends {
            // A duplicate push is a client retry of the same transaction id, so
            // it routes to the SAME partition as the original: 003 keys dedup
            // on (partition, hash). Draw the duplicate first, then the
            // partition, so the injection is a duplicate by the SQL's rules and
            // not just a repeated hash somewhere else.
            let mut dup: Option<Sample> = None;
            if (rng.below(10_000) as f64) < p.dup_pct * 100.0 * p.batch as f64 {
                let age = rng.below(p.dup_max_age_s as u64 + 1) as i64;
                dup = oracle.take(now_us, age);
            }
            let dup_slot = rng.below(p.batch) as usize;
            let pid = match &dup {
                Some(s) => s.pid,
                None => rng.below(p.partitions),
            };
            let part = parts[pid as usize];
            hashes.clear();
            expect.clear();
            for i in 0..p.batch as usize {
                match (&dup, i == dup_slot) {
                    (Some(s), true) => {
                        hashes.push(s.h);
                        expect.push(Some(*s));
                    }
                    _ => {
                        seq += 1;
                        hashes.push(hash_of(seq));
                        expect.push(None);
                    }
                }
            }
            msgs += p.batch;

            let disk_before = dd.counters().disk_probes;
            let t_p = Instant::now();
            {
                let mut c = Ctx {
                    eng: eng.as_ref(),
                    ops: &mut ops,
                    segs: &mut segs,
                    files: &mut ftab,
                };
                dd.probe(&mut c, pid, &part, &hashes, now_us, &mut verdicts)?;
            }
            for (i, h) in hashes.iter().enumerate() {
                if let Some(off) = overlay.get(&(pid, *h)) {
                    if verdicts[i].is_none() {
                        overlay_hits += 1;
                    }
                    verdicts[i] = Some(verdicts[i].map_or(*off, |x: u64| x.min(*off)));
                }
            }
            let probe_us = t_p.elapsed().as_micros() as u64;
            h_probe.record(probe_us);
            if dd.counters().disk_probes != disk_before {
                h_probe_disk.record(probe_us);
            }

            // ---- exactness of the verdict, against the oracle ----
            let mut any_dup = false;
            for (i, v) in verdicts.iter().enumerate() {
                match (&expect[i], v) {
                    (Some(s), Some(off)) => {
                        any_dup = true;
                        let age = now_us - s.created;
                        if age <= window_us {
                            dup_hit += 1;
                            if *off != s.off {
                                dup_wrong_off += 1;
                            }
                        } else {
                            dup_false += 1;
                        }
                    }
                    (Some(s), None) => {
                        let age = now_us - s.created;
                        if age <= window_us {
                            dup_missed += 1;
                            if audit_misses {
                                let found = {
                                    let mut c = Ctx {
                                        eng: eng.as_ref(),
                                        ops: &mut ops,
                                        segs: &mut segs,
                                        files: &mut ftab,
                                    };
                                    dd.audit(&mut c, pid, &part, s.h, now_us)?
                                };
                                if found.is_some() {
                                    miss_fastpath += 1;
                                } else {
                                    miss_absent += 1;
                                }
                                if miss_reported < 12 {
                                    miss_reported += 1;
                                    let row = eng.get(T_TXNS, &k_pid_off(pid, s.base))?.is_some();
                                    let segrow =
                                        eng.get(T_SEGMENTS, &k_pid_off(pid, s.base))?.is_some();
                                    println!(
                                        "  MISS pid={pid} base={} off={} age={} ms txns_start={} log_start={} txns_row={row} seg_row={segrow} audit={:?}",
                                        s.base,
                                        s.off,
                                        age / 1000,
                                        part.txns_start,
                                        part.log_start,
                                        found
                                    );
                                }
                            }
                        }
                    }
                    (None, Some(_)) => {
                        any_dup = true;
                        dup_false_fresh += 1;
                    }
                    (None, None) => {}
                }
            }
            for (i, e) in expect.iter().enumerate() {
                if let Some(s) = e {
                    if now_us - s.created <= window_us {
                        dup_in += 1;
                    } else {
                        dup_out += 1;
                    }
                    // Not detected -> it is about to be appended (the repack
                    // below keeps exactly the hashes whose verdict is None), so
                    // this hash gains a second occurrence.
                    if verdicts[i].is_none() {
                        repushed.insert(s.h);
                    }
                }
            }
            if any_dup {
                dup_appends += 1;
            }

            // ---- repack the survivors and append (003's repack path) ----
            accepted.clear();
            let base = part.last_offset;
            for (i, v) in verdicts.iter().enumerate() {
                if v.is_none() {
                    accepted.push((hashes[i], base + accepted.len() as u64));
                }
            }
            if accepted.is_empty() {
                continue;
            }
            let created = now_us.max(part.last_created + 1);
            let n = accepted.len() as u64;
            let hs: Vec<u128> = accepted.iter().map(|(h, _)| *h).collect();
            let empty: [u128; 0] = [];
            let (pos, sealed) = segs
                .append(
                    part.bucket,
                    pid,
                    base,
                    created,
                    if frame_hashes { &hs } else { &empty[..] },
                    &blob[..n as usize * p.payload],
                )
                .map_err(|e| format!("append: {e}"))?;
            if let Some(sid) = sealed {
                sealed_pending.push((part.bucket, sid));
            }
            ftab.on_append(
                part.bucket,
                pos.file_id,
                created,
                pos.len as u64,
                &hs,
                sealed,
            );
            appends += 1;

            let end = base + n - 1;
            ops.put(
                T_SEGMENTS,
                &k_pid_off(pid, base),
                &v_seg(
                    end,
                    created,
                    (n as usize * p.payload) as u32,
                    &pos,
                    n as u32,
                ),
            );
            ops.put(T_SEG_LOC, &k_pid_off(pid, base), &v_loc(&pos));
            {
                let mut c = Ctx {
                    eng: eng.as_ref(),
                    ops: &mut ops,
                    segs: &mut segs,
                    files: &mut ftab,
                };
                dd.record(&mut c, pid, base, &accepted, created, &pos, end)?;
            }
            // Sample a few FRESH messages for the oracle. A re-pushed duplicate
            // (one whose age was past the window) must not be sampled: its hash
            // would then have two occurrences and MIN would answer the older
            // one, which is correct per 003 but not what the sample says.
            for (h, off) in accepted.iter() {
                if *h != dup.map(|d| d.h).unwrap_or(0) && rng.below(32) == 0 {
                    oracle.offer(Sample {
                        h: *h,
                        pid,
                        off: *off,
                        base,
                        created,
                    });
                }
            }
            for (h, off) in accepted.iter() {
                overlay.insert((pid, *h), *off);
            }
            let pm = &mut parts[pid as usize];
            pm.last_offset = base + n;
            pm.last_created = created;
            pm.committed = pm.committed.max(base.saturating_sub(p.batch));
            ops.put(T_PARTITIONS, &pid.to_be_bytes(), &v_part(pm));
            ops.put(T_CURSORS, &pid.to_be_bytes(), &pm.committed.to_le_bytes());
        }

        // ---- ack-by-hash probes below the cursor (005) ----
        ack_acc += ack_per_entry;
        while ack_acc >= 1.0 {
            ack_acc -= 1.0;
            let lo = p.retention_s + 5;
            let hi = (p.txns_s * 4 / 5).max(lo + 1);
            let age = lo + rng.below((hi - lo) as u64) as i64;
            let Some(s) = oracle.peek(now_us, age, &mut rng) else {
                continue;
            };
            let part = parts[s.pid as usize];
            if s.off > part.committed {
                continue;
            }
            let seg_gone = eng.get(T_SEGMENTS, &k_pid_off(s.pid, s.base))?.is_none();
            // The hashes of ONE ack: the sample's own, plus ack_batch-1
            // fillers. A real ack carries the hashes of the leased batch, all
            // from this partition and (at --batch messages per Append) mostly
            // from the same frames; the fillers stand for those siblings. They
            // add per-hash matching work inside frames the call already reads
            // and inside the bloom tests it already runs; they add no scan and
            // no frame read, which is exactly 005's "ONE join".
            ack_hashes.clear();
            ack_hashes.push(s.h);
            while ack_hashes.len() < ack_batch {
                ack_hashes.push(rng.next() as u128 | ((rng.next() as u128) << 64));
            }
            let t_a = Instant::now();
            {
                let mut c = Ctx {
                    eng: eng.as_ref(),
                    ops: &mut ops,
                    segs: &mut segs,
                    files: &mut ftab,
                };
                dd.resolve_batch(&mut c, s.pid, &part, &ack_hashes, &mut ack_out)?;
            }
            let call_us = t_a.elapsed().as_micros() as u64;
            h_ack_call.record(call_us);
            h_ack.record(call_us / ack_batch as u64);
            ack_calls += 1;
            let res: AckRes = ack_out[0];
            ack_probes += 1;
            if seg_gone {
                ack_seg_deleted += 1;
            }
            if res.below {
                ack_below_ok += 1;
            } else {
                ack_below_missing += 1;
            }
            if res.eff.is_some() {
                ack_eff_unexpected += 1;
            }
            // Reservoir over the WHOLE run, not the first 4096 probes: with
            // "first 4096" every saved sample was created in the first seconds
            // of the run, so at the end they were all long past the window and
            // the restart check only ever asked the `expect None` direction.
            sample_seen += 1;
            if legacy_samples {
                if samples_for_rebuild.len() < 4096 && rng.below(8) == 0 {
                    samples_for_rebuild.push(s);
                }
            } else if samples_for_rebuild.len() < 4096 {
                samples_for_rebuild.push(s);
            } else {
                let j = rng.below(sample_seen) as usize;
                if j < samples_for_rebuild.len() {
                    samples_for_rebuild[j] = s;
                }
            }
        }

        // ---- txns prune (006 log_txns_purge_step_v1) ----
        let t_pr = Instant::now();
        let budget = if opt_a {
            (msgs_per_entry * 2) as usize
        } else {
            (p.entry_appends * 2) as usize
        };
        {
            let mut c = Ctx {
                eng: eng.as_ref(),
                ops: &mut ops,
                segs: &mut segs,
                files: &mut ftab,
            };
            pruned += dd.prune(&mut c, &mut parts, now_us - txns_us, budget)?;
        }
        h_prune.record(t_pr.elapsed().as_micros() as u64);

        // ---- retention: delete segments rows (006 rules 1-2) ----
        let t_rt = Instant::now();
        let cutoff = now_us - retention_us;
        for _ in 0..retention_budget {
            let pid = (ret_cursor % p.partitions as usize) as u64;
            ret_cursor += 1;
            let part = parts[pid as usize];
            if part.log_start >= part.last_offset {
                continue;
            }
            let prefix = pid.to_be_bytes();
            let from = k_pid_off(pid, part.log_start);
            let mut victims: Vec<(u64, u64, u16, u32)> = Vec::new();
            eng.range(T_SEGMENTS, &from, &prefix, 128, &mut |k, v| {
                if seg_created(v) >= cutoff {
                    return false;
                }
                let base = u64::from_be_bytes(k[8..16].try_into().unwrap());
                let (b, f) = seg_file(v);
                victims.push((base, seg_end(v), b, f));
                true
            })?;
            let mut newstart = part.log_start;
            for (base, end, b, f) in &victims {
                ops.del(T_SEGMENTS, &k_pid_off(pid, *base));
                ops.del(T_SEG_LOC, &k_pid_off(pid, *base));
                ftab.release_segment(*b, *f);
                newstart = end + 1;
                seg_rows_deleted += 1;
            }
            parts[pid as usize].log_start = newstart;
        }
        h_ret.record(t_rt.elapsed().as_micros() as u64);

        store_ops += ops.len() as u64;
        let t_c = Instant::now();
        eng.commit(&ops, false)?;
        h_commit.record(t_c.elapsed().as_micros() as u64);

        // ---- local file maintenance between entries (§11.7) ----
        for (b, id) in sealed_pending.iter() {
            if let Some(fi) = ftab.get(*b, *id) {
                if let Some(bl) = &fi.bloom {
                    let bp = bloom_path(&dir, *b, *id);
                    let _ = std::fs::create_dir_all(bp.parent().unwrap());
                    if bl.save(&bp).is_ok() {
                        blooms_saved += 1;
                        bloom_bytes_saved += bl.bytes() as u64;
                    }
                }
            }
        }
        ftab.collect_dead(&mut dead);
        for (b, id) in dead.iter() {
            bytes_unlinked += segs.unlink(*b, *id);
            files_unlinked += 1;
            ftab.remove(*b, *id);
            let _ = std::fs::remove_file(bloom_path(&dir, *b, *id));
        }
        if hash_compact && use_blooms {
            ftab.collect_hash_only(&mut hashonly);
            if let Some((b, id, _bytes)) = hashonly.first().copied() {
                let t_hc = Instant::now();
                // Keep exactly the frames a `txns` row still points at: the
                // prune walk is budgeted and lags the cutoff, and dropping a
                // frame a row still references would leave a dangling position.
                let mut keep = |pid: u64, base: u64, _created: i64| -> bool {
                    eng.get(T_TXNS, &k_pid_off(pid, base))
                        .map(|o| o.is_some())
                        .unwrap_or(true)
                };
                let (newpos, before, after) =
                    segs.rewrite_hash_only(b, id, &mut keep, fsync_mode)?;
                ops.clear();
                for (pid, base, np) in &newpos {
                    if let Some(mut v) = eng.get(T_TXNS, &k_pid_off(*pid, *base))? {
                        v[16..18].copy_from_slice(&np.bucket.to_le_bytes());
                        v[18..22].copy_from_slice(&np.file_id.to_le_bytes());
                        v[22..30].copy_from_slice(&np.offset.to_le_bytes());
                        v[30..34].copy_from_slice(&np.len.to_le_bytes());
                        ops.put(T_TXNS, &k_pid_off(*pid, *base), &v);
                    }
                }
                eng.commit(&ops, false)?;
                if let Some(fi) = ftab.get_mut(b, id) {
                    fi.hash_only = true;
                    fi.bytes = after;
                    fi.live_txns = newpos.len() as u32;
                }
                compactions += 1;
                compact_before += before;
                compact_after += after;
                h_compact.record(t_hc.elapsed().as_micros() as u64);
            }
        }

        // ---- durable point (§11.4) ----
        if last_durable.elapsed() >= Duration::from_millis(durable_ms) {
            let t_d = Instant::now();
            segs.sync_dirty(fsync_threads, fsync_mode)
                .map_err(|e| e.to_string())?;
            ops.clear();
            let mut m = [0u8; 24];
            m[0..8].copy_from_slice(&idx.to_le_bytes());
            m[8..16].copy_from_slice(&1u64.to_le_bytes());
            m[16..24].copy_from_slice(&now_us.to_le_bytes());
            ops.put(T_META, M_APPLIED, &m);
            ops.put(T_META, M_DURABLE, &m);
            ops.put(T_META, M_FILELENS, &segs.file_lengths());
            eng.commit(&ops, true)?;
            h_durable.record(t_d.elapsed().as_micros() as u64);
            durable_points += 1;
            last_durable = Instant::now();
        }
        if last_rss.elapsed() >= Duration::from_secs(1) {
            rss_series.push((start.elapsed().as_secs_f64(), sys::rss_bytes()));
            last_rss = Instant::now();
        }
        let entry_us = t_entry.elapsed().as_micros() as u64;
        h_entry.record(entry_us);
        min_busy_us += entry_us;
        idx += 1;
        if last_min.elapsed() >= Duration::from_secs(60) {
            let span = last_min.elapsed().as_secs_f64();
            let de = idx - min_idx0;
            let dm = msgs - min_msgs0;
            println!(
                "  minute {:>2}: {:.0} entries/s ({:.0} msg/s), entry mean {:.3} ms, busy {:.0} % of the wall, store {:.0} MiB, files {:.0} MiB, rss {:.0} MiB, pacing lag {:.0} s",
                (start.elapsed().as_secs_f64() / 60.0).round() as i64,
                de as f64 / span,
                dm as f64 / span,
                min_busy_us as f64 / 1000.0 / de.max(1) as f64,
                100.0 * min_busy_us as f64 / 1e6 / span,
                mib(store_bytes(&dir, &seg_dir).saturating_sub(store0)),
                mib(sys::dir_alloc_bytes(&seg_dir).saturating_sub(seg0)),
                mib(sys::rss_bytes()),
                (start.elapsed().as_secs_f64() - interval.mul_f64(idx as f64).as_secs_f64()).max(0.0),
            );
            min_idx0 = idx;
            min_msgs0 = msgs;
            min_busy_us = 0;
            last_min = Instant::now();
        }
    }

    let wall = start.elapsed().as_secs_f64();
    let end_us = last_now_us;
    // final durable point
    segs.sync_dirty(fsync_threads, fsync_mode)
        .map_err(|e| e.to_string())?;
    ops.clear();
    let mut m = [0u8; 24];
    m[0..8].copy_from_slice(&idx.to_le_bytes());
    m[8..16].copy_from_slice(&1u64.to_le_bytes());
    m[16..24].copy_from_slice(&end_us.to_le_bytes());
    ops.put(T_META, M_APPLIED, &m);
    ops.put(T_META, M_DURABLE, &m);
    ops.put(T_META, M_FILELENS, &segs.file_lengths());
    eng.commit(&ops, true)?;

    let io1 = sys::disk_written_bytes();
    let store1 = store_bytes(&dir, &seg_dir);
    let seg1 = sys::dir_alloc_bytes(&seg_dir);
    let bloom_disk = sys::dir_alloc_bytes(&dir.join("blooms"));
    let rss1 = sys::rss_bytes();
    let rss_max = rss_series.iter().map(|(_, r)| *r).max().unwrap_or(rss1);

    // persist what `rebuild` needs to verify exactness after a restart
    {
        let before = samples_for_rebuild.len();
        if !legacy_samples {
            samples_for_rebuild.retain(|s| !repushed.contains(&s.h));
        }
        println!(
            "rebuild samples: {} kept of {} seen ({} dropped: the hash was re-pushed out of window during the run and now has a second occurrence; legacy selection: {legacy_samples}, hashes re-pushed in the run: {})",
            samples_for_rebuild.len(),
            sample_seen,
            before - samples_for_rebuild.len(),
            repushed.len()
        );
        let mut f = std::fs::File::create(dir.join("samples.bin")).map_err(|e| e.to_string())?;
        f.write_all(&end_us.to_le_bytes())
            .map_err(|e| e.to_string())?;
        f.write_all(&(samples_for_rebuild.len() as u64).to_le_bytes())
            .map_err(|e| e.to_string())?;
        for s in &samples_for_rebuild {
            f.write_all(&s.h.to_le_bytes()).map_err(|e| e.to_string())?;
            f.write_all(&s.pid.to_le_bytes())
                .map_err(|e| e.to_string())?;
            f.write_all(&s.off.to_le_bytes())
                .map_err(|e| e.to_string())?;
            f.write_all(&s.base.to_le_bytes())
                .map_err(|e| e.to_string())?;
            f.write_all(&s.created.to_le_bytes())
                .map_err(|e| e.to_string())?;
        }
        f.sync_all().map_err(|e| e.to_string())?;
    }

    let c = dd.counters();
    println!("\n--- throughput ---");
    println!(
        "wall {wall:.1} s, {idx} entries ({:.0}/s target {:.0}/s), {msgs} messages ({:.0} msg/s), {appends} appends, {dup_appends} appends carrying a duplicate",
        idx as f64 / wall,
        entries_per_s,
        msgs as f64 / wall
    );
    println!(
        "store ops {store_ops} ({:.0}/s), pruned hash records {pruned}, segment rows deleted {seg_rows_deleted}, max pacing lag {:.1} ms, entries behind >50 ms: {behind}",
        store_ops as f64 / wall,
        max_lag_us as f64 / 1000.0
    );
    println!("store open {open_ms:.0} ms, durable points {durable_points}");

    println!("\n--- probe latency (one push of {} hashes) ---", p.batch);
    prow("probe", &h_probe);
    if h_probe_disk.count() > 0 {
        prow("probe (disk)", &h_probe_disk);
    }
    prow("ack-by-hash", &h_ack);
    prow("entry total", &h_entry);
    prow("commit", &h_commit);
    prow("durable point", &h_durable);
    prow("prune step", &h_prune);
    prow("retention step", &h_ret);
    if h_compact.count() > 0 {
        prow("hash-only compaction", &h_compact);
    }

    println!("\n--- dedup work ---");
    println!(
        "store gets {} puts {} dels {} scan rows {}",
        c.gets, c.puts, c.dels, c.scan_rows
    );
    println!(
        "bloom tests {} hits {} (false hits {}), frame reads {} ({:.1} MiB), probes that touched disk {}",
        c.bloom_tests,
        c.bloom_hits,
        c.bloom_false_hits,
        c.frame_reads,
        mib(segs.frame_read_bytes),
        c.disk_probes
    );
    println!(
        "recent cache: vouched {} maybe {}",
        c.cache_vouched, c.cache_maybe
    );
    for (k, v) in dd.extra() {
        println!("{k}: {v}");
    }

    println!("\n--- exactness ---");
    println!(
        "duplicates injected: {} inside the window, {} outside it (oracle misses {})",
        dup_in, dup_out, oracle.empty
    );
    println!(
        "inside the window : detected {dup_hit}, MISSED {dup_missed}, wrong original offset {dup_wrong_off}"
    );
    println!("outside the window: falsely reported {dup_false}");
    println!("fresh hashes falsely reported as duplicates: {dup_false_fresh}");
    println!(
        "duplicates caught by the planner overlay (same entry, not committed yet): {overlay_hits}"
    );
    if audit_misses {
        println!(
            "misses audited by an exhaustive scan: {miss_fastpath} were THERE (fast-path bug), {miss_absent} were genuinely absent"
        );
    }
    println!(
        "ack-by-hash calls {ack_calls} of {ack_batch} hashes each: per call p50 {} us p99 {} us mean {:.3} us; per hash p50 {} us p99 {} us",
        h_ack_call.pct(0.50),
        h_ack_call.pct(0.99),
        h_ack_call.mean(),
        h_ack.pct(0.50),
        h_ack.pct(0.99)
    );
    println!(
        "ack-by-hash below the cursor: {ack_probes} probes, resolved {ack_below_ok}, UNRESOLVED {ack_below_missing}, of which the segment was already deleted by retention: {ack_seg_deleted}; unexpected in-span offsets {ack_eff_unexpected}"
    );
    let exact = dup_missed == 0
        && dup_false == 0
        && dup_false_fresh == 0
        && dup_wrong_off == 0
        && ack_below_missing == 0;

    println!("\n--- footprint ---");
    println!(
        "store {:.1} MiB, segment files {:.1} MiB, bloom sidecars {:.1} MiB ({blooms_saved} saved, {:.1} MiB written)",
        mib(store1 - store0),
        mib(seg1 - seg0),
        mib(bloom_disk),
        mib(bloom_bytes_saved)
    );
    println!(
        "files live {} (opened {}), unlinked {} ({:.1} MiB), hash-only rewrites {} ({:.1} -> {:.1} MiB)",
        ftab.live_files(),
        ftab.files_opened,
        files_unlinked,
        mib(bytes_unlinked),
        compactions,
        mib(compact_before),
        mib(compact_after)
    );
    println!(
        "RAM: dedup structures {:.1} MiB (blooms {:.1} MiB + cache {:.1} MiB), oracle {:.1} MiB, RSS {:.0} -> {:.0} MiB (max {:.0})",
        mib((dd.ram_bytes() + ftab.ram_bytes()) as u64),
        mib(ftab.bloom_bytes as u64),
        mib(dd.ram_bytes() as u64),
        mib(oracle.ram_bytes() as u64),
        mib(rss0),
        mib(rss1),
        mib(rss_max)
    );
    println!(
        "kernel bytes written {:.1} MiB ({:.2} MiB/s), payload appended {:.1} MiB",
        mib(io1.saturating_sub(io0)),
        mib(io1.saturating_sub(io0)) / wall,
        mib(segs.appended_bytes)
    );
    println!(
        "per message: {:.1} B of store, {:.1} B of files, {:.2} B of bloom RAM",
        (store1 - store0) as f64 / msgs as f64,
        (seg1 - seg0) as f64 / msgs as f64,
        ftab.bloom_bytes as f64 / msgs as f64
    );
    println!(
        "dedup alone writes {:.1} B per message logically ({:.1} MiB over the run)",
        c.logical_bytes as f64 / msgs as f64,
        mib(c.logical_bytes)
    );

    println!(
        "\nRESULT option={} engine={} rate={:.0} probe_p50_us={} probe_p99_us={} ack_batch={} ack_call_p50_us={} ack_call_p99_us={} ack_p50_us={} ack_p99_us={} store_mib={:.1} seg_mib={:.1} ram_mib={:.1} rss_max_mib={:.0} exact={} label={}",
        opt,
        engine,
        msgs as f64 / wall,
        h_probe.pct(0.50),
        h_probe.pct(0.99),
        ack_batch,
        h_ack_call.pct(0.50),
        h_ack_call.pct(0.99),
        h_ack.pct(0.50),
        h_ack.pct(0.99),
        mib(store1 - store0),
        mib(seg1 - seg0),
        mib((dd.ram_bytes() + ftab.ram_bytes()) as u64),
        mib(rss_max),
        exact,
        label
    );
    Ok(if exact { 0 } else { 1 })
}

fn prow(name: &str, h: &Hist) {
    if h.count() == 0 {
        return;
    }
    println!(
        "{name:<22} n={:<9} p50 {:>8.3} ms  p99 {:>8.3} ms  p99.9 {:>8.3} ms  max {:>8.3} ms  mean {:>7.3} ms",
        h.count(),
        h.pct(0.50) as f64 / 1000.0,
        h.pct(0.99) as f64 / 1000.0,
        h.pct(0.999) as f64 / 1000.0,
        h.max() as f64 / 1000.0,
        h.mean() / 1000.0
    );
}

// --------------------------------------------------------------- rebuild ----

/// What a node has to do after a restart before it can answer a dedup probe
/// (§11.5 step 4): option (a) nothing, option (b) the file table and the
/// blooms. Verifies exactness afterwards against the samples the run saved.
fn cmd_rebuild(a: &Args) -> Result<i32, String> {
    let opt = a.s("option", "b");
    let opt_a = opt.starts_with('a');
    let engine = a.s("engine", "fjall");
    let dir = PathBuf::from(a.s("dir", "/var/tmp/s2-dedup"));
    let mode = a.s("mode", "blooms");
    let map_size = (a.u("map-size-gb", 32) as usize) << 30;
    let store_cache = a.u("store-cache-mb", 256) << 20;
    let segment_bytes = a.u("segment-bytes", 8 << 20);
    let bloom_bits = a.u("bloom-bits", 16) as u32;
    let window_s = a.u("window-s", 300) as i64;
    let payload = a.u("payload", 96) as usize;
    let cache_mb = a.u("cache-mb", 64);
    let cache_block = a.u("cache-block", 16384) as usize;
    let verify_frames = a.b("verify-frames", false);

    println!("=== s2-dedup rebuild (option {opt}, mode {mode}) ===");
    println!("host        {}", sys::host_line());
    let t_open = Instant::now();
    let eng = engines::open(&engine, &dir, map_size, store_cache)?;
    let open_ms = t_open.elapsed().as_secs_f64() * 1e3;
    let mut segs = Segments::open(&dir, segment_bytes).map_err(|e| e.to_string())?;
    let expect_keys = (segment_bytes / (payload as u64 + 16)).max(1024);
    let mut ftab = FileTable::new(seg::NBUCKETS, expect_keys, bloom_bits, !opt_a);

    // 1. the node-local file table: who still points into which file.
    let t_tab = Instant::now();
    let mut seg_rows = 0u64;
    let mut txn_rows = 0u64;
    let mut seen: Vec<(u16, u32, i64, i64)> = Vec::new();
    eng.scan_cb(T_SEG_LOC, &mut |_k, v| {
        let b = u16::from_le_bytes(v[0..2].try_into().unwrap());
        let f = u32::from_le_bytes(v[2..6].try_into().unwrap());
        seg_rows += 1;
        seen.push((b, f, 0, 0));
        true
    })?;
    let mut txn_pos: Vec<(u16, u32, i64)> = Vec::new();
    if !opt_a {
        eng.scan_cb(T_TXNS, &mut |k, v| {
            let r = parse_txns(k, v);
            txn_rows += 1;
            txn_pos.push((r.pos.bucket, r.pos.file_id, r.created));
            true
        })?;
    }
    let tab_ms = t_tab.elapsed().as_secs_f64() * 1e3;

    // 2. blooms
    let t_bl = Instant::now();
    let mut files: HashMap<(u16, u32), (u32, u32, i64, i64)> = HashMap::new();
    for (b, f, _, _) in &seen {
        files
            .entry((*b, *f))
            .or_insert((0, 0, i64::MAX, i64::MIN))
            .0 += 1;
    }
    for (b, f, c) in &txn_pos {
        let e = files.entry((*b, *f)).or_insert((0, 0, i64::MAX, i64::MIN));
        e.1 += 1;
        e.2 = e.2.min(*c);
        e.3 = e.3.max(*c);
    }
    let mut loaded = 0u64;
    let mut walked = 0u64;
    let mut walked_frames = 0u64;
    let mut bloom_bytes = 0u64;
    if !opt_a {
        let keys: Vec<(u16, u32)> = files.keys().copied().collect();
        for (b, f) in keys {
            let e = files[&(b, f)];
            let mut bl = None;
            if mode == "blooms" {
                if let Ok(x) = bloom::Bloom::load(&bloom_path(&dir, b, f)) {
                    bloom_bytes += x.bytes() as u64;
                    loaded += 1;
                    bl = Some(x);
                }
            }
            if bl.is_none() {
                let mut x = bloom::Bloom::new(expect_keys, bloom_bits);
                let mut mn = i64::MAX;
                let mut mx = i64::MIN;
                let n = segs.walk_file(b, f, &mut |_pid, _base, created, _o, _l, hs| {
                    for h in hs {
                        x.insert(*h);
                    }
                    mn = mn.min(created);
                    mx = mx.max(created);
                })?;
                walked += 1;
                walked_frames += n;
                bloom_bytes += x.bytes() as u64;
                bl = Some(x);
                let _ = (mn, mx);
            }
            ftab.install(b, f, e.0, e.1, e.2, e.3, bl);
        }
    }
    let bloom_ms = t_bl.elapsed().as_secs_f64() * 1e3;

    // 3. probe the saved samples and check the verdicts (exactness after a restart)
    let raw = std::fs::read(dir.join("samples.bin")).map_err(|e| e.to_string())?;
    let as_of = a.u(
        "as-of",
        i64::from_le_bytes(raw[0..8].try_into().unwrap()) as u64,
    ) as i64;
    let n = u64::from_le_bytes(raw[8..16].try_into().unwrap()) as usize;
    let mut dd: Box<dyn Dedup> = if opt_a {
        let mut si = StoreIndex::new(window_s * 1_000_000);
        si.lean = opt == "a-lean";
        Box::new(si)
    } else {
        Box::new(HashLists::new(
            window_s * 1_000_000,
            RecentCache::new((cache_mb as usize) << 20, cache_block, bloom_bits, as_of),
            verify_frames,
        ))
    };
    let mut ops = OpBuf::new();
    let mut hist = Hist::new();
    let (mut ok, mut bad, mut checked) = (0u64, 0u64, 0u64);
    let (mut checked_in, mut checked_out, mut bad_younger) = (0u64, 0u64, 0u64);
    for i in 0..n {
        let o = 16 + i * 48;
        let h = u128::from_le_bytes(raw[o..o + 16].try_into().unwrap());
        let pid = u64::from_le_bytes(raw[o + 16..o + 24].try_into().unwrap());
        let off = u64::from_le_bytes(raw[o + 24..o + 32].try_into().unwrap());
        let created = i64::from_le_bytes(raw[o + 40..o + 48].try_into().unwrap());
        // partition state from the store
        let Some(pv) = eng.get(T_PARTITIONS, &pid.to_be_bytes())? else {
            continue;
        };
        let part = Part {
            last_offset: u64::from_le_bytes(pv[0..8].try_into().unwrap()),
            log_start: u64::from_le_bytes(pv[8..16].try_into().unwrap()),
            txns_start: u64::from_le_bytes(pv[16..24].try_into().unwrap()),
            last_created: i64::from_le_bytes(pv[24..32].try_into().unwrap()),
            committed: u64::from_le_bytes(pv[32..40].try_into().unwrap()),
            bucket: bucket_of(pid),
        };
        let mut out = Vec::new();
        let t = Instant::now();
        {
            let mut c = Ctx {
                eng: eng.as_ref(),
                ops: &mut ops,
                segs: &mut segs,
                files: &mut ftab,
            };
            dd.probe(&mut c, pid, &part, &[h], as_of, &mut out)?;
        }
        hist.record(t.elapsed().as_micros() as u64);
        checked += 1;
        let inside = as_of - created <= window_s * 1_000_000 && off >= part.txns_start;
        if inside {
            checked_in += 1;
        } else {
            checked_out += 1;
        }
        match (inside, out[0]) {
            (true, Some(x)) if x == off => ok += 1,
            (false, None) => ok += 1,
            _ => {
                bad += 1;
                // What did the design actually find? An occurrence YOUNGER than
                // this sample means the hash was pushed again during the run and
                // the sample's expectation is stale (a harness artefact), not a
                // record that survived or vanished across the restart.
                let occ = {
                    let mut c = Ctx {
                        eng: eng.as_ref(),
                        ops: &mut ops,
                        segs: &mut segs,
                        files: &mut ftab,
                    };
                    dd.explain(&mut c, pid, &part, h)?
                };
                let younger = occ.iter().any(|(o, cr)| *o != off && *cr > created);
                if younger {
                    bad_younger += 1;
                }
                if bad < 6 {
                    println!(
                        "  MISMATCH pid={pid} off={off} created_age={} s inside={inside} got={:?}",
                        (as_of - created) / 1_000_000,
                        out[0]
                    );
                    let mut desc: Vec<String> = occ
                        .iter()
                        .map(|(o, cr)| format!("off={o} age={} s", (as_of - cr) / 1_000_000))
                        .collect();
                    desc.sort();
                    println!(
                        "    occurrences still in the store: [{}] -> a younger one exists: {younger}",
                        desc.join(", ")
                    );
                }
            }
        }
    }

    println!("store open                {open_ms:>8.0} ms");
    println!("file table (scan seg_loc {seg_rows} rows + txns {txn_rows} rows) {tab_ms:>8.0} ms");
    println!(
        "blooms: {loaded} loaded from sidecars, {walked} rebuilt by walking {walked_frames} frames -> {bloom_ms:>8.0} ms, {:.1} MiB resident",
        mib(bloom_bytes)
    );
    println!(
        "total ready-to-probe      {:>8.0} ms",
        open_ms + tab_ms + bloom_ms
    );
    prow("probe after restart", &hist);
    println!(
        "samples checked {checked} ({checked_in} expect the original offset, {checked_out} expect no answer): {ok} correct, {bad} WRONG, of the wrong ones {bad_younger} have a YOUNGER occurrence of the same hash in the store (re-pushed during the run: a stale sample, not a restart defect)"
    );
    println!(
        "RESULT rebuild option={opt} mode={mode} open_ms={open_ms:.0} table_ms={tab_ms:.0} bloom_ms={bloom_ms:.0} total_ms={:.0} bloom_mib={:.1} checked={checked} wrong={bad}",
        open_ms + tab_ms + bloom_ms,
        mib(bloom_bytes)
    );
    Ok(if bad == 0 { 0 } else { 1 })
}
