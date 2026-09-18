//! S1 (PLAN_RAFT.md WP-0.3): which embedded ordered store can carry the RSM?
//!
//! Replays a synthetic apply stream (§6.1, §7.1) against redb, fjall or heed
//! (LMDB), a durable point every second (§11.4), payload bytes in 256
//! append-only segment files (D9, §11.2). Prints the numbers WP-0.3 asks for
//! and a machine-readable RESULT line for the scripts.
//!
//! TWO STORE SHAPES (`--shape`), because G0 amended the design after the first
//! campaign and the first campaign therefore measured a store that no longer
//! exists:
//!   legacy    one non-durable write transaction PER APPLIED ENTRY, `segments`
//!             rows in the store, no dedup keyspace. The 2026-09-17 numbers.
//!   ratified  §11.3 as amended: the write transaction is committed every
//!             `--store-commit-ms` (4) or `--store-commit-entries` (256),
//!             whichever comes first; `segments` is NOT in the store (§6.1
//!             amendment); a `dedup` keyspace of uniformly random (pid, hash)
//!             keys carries D10 option (a), one row per message.
//!
//! Subcommands:
//!   run     replay the apply stream, then export / reclaim / recover
//!   verify  reopen a store dir after a crash and check that the applied index
//!           it reports and the segment bytes it references agree (I11)
//!   iter    bulk-load N keys and measure ordered iteration
//!   reads   concurrent read benchmark against an existing store (D15 read
//!           path): N threads of gets and prefix lists, plus the reader-slot
//!           ceiling probe
//!   info    print each engine's capabilities

mod engines;
mod hist;
mod seg;
mod sys;
mod workload;

use engines::*;
use hist::Hist;
use seg::Segments;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use workload::{Gen, Params};

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

fn main() {
    let a = Args::parse();
    let cmd = a.1.first().cloned().unwrap_or_else(|| "run".into());
    let code = match cmd.as_str() {
        "run" => cmd_run(&a),
        "verify" => cmd_verify(&a),
        "iter" => cmd_iter(&a),
        "reads" => cmd_reads(&a),
        "info" => cmd_info(&a),
        other => Err(format!(
            "unknown subcommand {other} (run|verify|iter|reads|info)"
        )),
    };
    match code {
        Ok(c) => std::process::exit(c),
        Err(e) => {
            eprintln!("ERROR: {e}");
            std::process::exit(2);
        }
    }
}

fn eng_opts(a: &Args) -> EngOpts {
    EngOpts {
        map_size: (a.u("map-size-gb", 32) as usize) << 30,
        cache_bytes: a.u("cache-mb", 256) << 20,
        heed_flags: a.s("heed-flags", "nosync"),
        heed_no_tls: a.b("heed-no-tls", false),
        heed_max_readers: a.u("heed-max-readers", 0) as u32,
    }
}

/// `legacy` = the pre-G0 shape the 2026-09-17 campaign measured;
/// `ratified` = §11.3 + §6.1 as amended at G0. Individual switches
/// (`--segments-in-store`, `--dedup-rows`) override the shape.
fn shape(a: &Args) -> Result<(bool, bool), String> {
    let (seg, dedup) = match a.s("shape", "legacy").as_str() {
        "legacy" => (true, false),
        "ratified" => (false, true),
        other => return Err(format!("unknown --shape {other} (want legacy|ratified)")),
    };
    Ok((a.b("segments-in-store", seg), a.b("dedup-rows", dedup)))
}

fn params(a: &Args) -> Params {
    let rate = a.u("rate", 20_000);
    let (segments_in_store, dedup_rows) = shape(a).unwrap_or((true, false));
    Params {
        segments_in_store,
        dedup_rows,
        dedup_window_s: a.u("dedup-window-s", 0) as i64,
        rate,
        batch: a.u("batch", 10),
        payload: a.u("payload", 512) as usize,
        partitions: a.u("partitions", 4096),
        queues: a.u("queues", 64),
        tenants: a.u("tenants", 8),
        groups: a.u("groups", 1),
        kv_rate: a.u("kv-rate", (rate * 2 / 5).min(40_000)),
        kv_keys: a.u("kv-keys", 200_000),
        kv_value: a.u("kv-value", 64) as usize,
        timer_rate: a.u("timer-rate", rate / 50),
        request_window_s: a.u("request-window-s", 60) as i64,
        seed: a.u("seed", 1),
    }
}

// ---------------------------------------------------------------- run -------

fn cmd_run(a: &Args) -> Result<i32, String> {
    let engine = a.s("engine", "redb");
    let dir = PathBuf::from(a.s("dir", "/tmp/s1-store"));
    let duration = a.f("duration", 60.0);
    let durable_ms = a.u("durable-ms", 1000);
    let durable_bytes = a.u("durable-bytes", 256 << 20);
    let segment_bytes = a.u("segment-bytes", 64 << 20);
    let eopts = eng_opts(a);
    let fsync_threads = a.u("fsync-threads", 1) as usize;
    // §11.3 G0 amendment: NOT one store transaction per applied entry.
    // 0/1 keep the original per-entry shape so the old cells stay reproducible.
    let commit_entries = a.u("store-commit-entries", 1).max(1);
    let commit_ms = a.u("store-commit-ms", 0);
    let fsync_mode = seg::FsyncMode::parse(&a.s("fsync-mode", "full"));
    let self_kill_ms = a.u("self-kill-after-ms", 0);
    let kill_point = a.s("kill-point", "random");
    let label = a.s("label", "");
    let p = params(a);

    if a.b("fresh", true) {
        let _ = std::fs::remove_dir_all(&dir);
    }
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let nofile = sys::raise_nofile(4096)?;

    let entries_per_s = p.rate as f64 / p.batch as f64;
    println!("=== s1-store run ===");
    println!("host        {}", sys::host_line());
    println!("engine      {engine}");
    println!("dir         {}", dir.display());
    println!(
        "load        {} msg/s, batch {} -> {:.0} entries/s, payload {} B, {} partitions, {} queues, {} groups",
        p.rate, p.batch, entries_per_s, p.payload, p.partitions, p.queues, p.groups
    );
    println!(
        "mix         kv {} ops/s over {} keys, timers {} ops/s, request-id window {} s",
        p.kv_rate, p.kv_keys, p.timer_rate, p.request_window_s
    );
    println!(
        "shape       {} (segments rows in store: {}, dedup rows per message: {}, dedup window {} s)",
        a.s("shape", "legacy"),
        p.segments_in_store,
        p.dedup_rows,
        p.dedup_window_s
    );
    println!(
        "durability  store commit every {} entries or {} ms ({}), durable point every {durable_ms} ms or {} MiB; segment files roll at {} MiB; fsync {fsync_mode:?} x{fsync_threads} threads; store cache {} MiB; nofile={nofile}",
        commit_entries,
        commit_ms,
        if commit_entries == 1 && commit_ms == 0 {
            "one write txn PER ENTRY, the pre-G0 shape"
        } else {
            "§11.3 as amended at G0"
        },
        mib(durable_bytes),
        mib(segment_bytes),
        mib(eopts.cache_bytes)
    );
    if !label.is_empty() {
        println!("label       {label}");
    }

    let t_open = Instant::now();
    let mut eng = engines::open(&engine, &dir, &eopts)?;
    let open_ms = t_open.elapsed().as_secs_f64() * 1e3;
    println!("config      {}", eng.config_note());
    let mut segs = Segments::open(&dir, segment_bytes).map_err(|e| e.to_string())?;
    let mut gen = Gen::new(p.clone());

    let mut h_nondur = Hist::new();
    let mut h_entry = Hist::new();
    let mut h_durable = Hist::new();
    let mut h_fsync = Hist::new();
    let mut h_read = Hist::new();
    let mut h_list = Hist::new();
    let mut rss_series: Vec<(f64, u64)> = Vec::new();

    let io0 = sys::disk_written_bytes();
    let seg_dir = segs.dir().to_path_buf();
    let store_bytes = |d: &Path, s: &Path| sys::dir_alloc_bytes(d) - sys::dir_alloc_bytes(s);
    let store0 = store_bytes(&dir, &seg_dir);
    let seg0 = sys::dir_alloc_bytes(&seg_dir);
    let rss0 = sys::rss_bytes();

    let mut ops = OpBuf::new();
    let start = Instant::now();
    let interval = Duration::from_nanos((1e9 / entries_per_s) as u64);
    let mut idx: u64 = 0;
    let mut last_durable = Instant::now();
    let mut bytes_since_durable: u64 = 0;
    let mut max_lag_us: u64 = 0;
    let mut durable_points: u64 = 0;
    let mut fsync_files: u64 = 0;
    let mut behind_entries: u64 = 0;
    let mut last_rss = Instant::now();
    let mut last_commit = Instant::now();
    let mut entries_since_commit: u64 = 0;
    let mut store_commits: u64 = 0;
    let kill_at = if self_kill_ms > 0 {
        Some(Duration::from_millis(self_kill_ms))
    } else {
        None
    };
    // soak instrumentation (WP-0.3 part 2): RSS every --sample-ms, file growth
    // every --growth-every-s, and one mid-run delete phase at --delete-at-s.
    let sample = Duration::from_millis(a.u("sample-ms", 1000).max(1));
    let growth_every = a.u("growth-every-s", 0);
    let delete_at_s = a.f("delete-at-s", 0.0);
    let delete_pct = a.f("delete-pct", 30.0);
    let run_start_us = sys::now_micros();
    let mut last_growth = Instant::now();
    let mut pace_offset = Duration::ZERO;
    let mut mid_delete_done = delete_at_s <= 0.0;
    if growth_every > 0 {
        println!(
            "GROWTH t_s=0.0 rss_mib={:.1} store_mib={:.1} seg_mib={:.1} seg_files={} entries=0 msgs=0",
            mib(rss0),
            mib(store0),
            mib(seg0),
            sys::file_count(&seg_dir)
        );
    }

    loop {
        let elapsed = start.elapsed();
        if elapsed.as_secs_f64() >= duration {
            break;
        }
        // open-loop pacing
        let target = interval.mul_f64(idx as f64) + pace_offset;
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
            if lag > max_lag_us {
                max_lag_us = lag;
            }
            if lag > 50_000 {
                behind_entries += 1;
            }
        }

        let now_us = sys::now_micros();
        let t_entry = Instant::now();
        let logical_before = ops.logical_bytes();
        gen.entry(
            idx,
            now_us,
            &mut ops,
            &mut segs,
            eng.as_ref(),
            &mut h_read,
            &mut h_list,
        )?;
        bytes_since_durable += ops.logical_bytes() - logical_before;
        entries_since_commit += 1;

        if let Some(k) = kill_at {
            if start.elapsed() >= k
                && (kill_point == "append" || (kill_point == "random" && idx % 3 == 0))
            {
                eprintln!("KILLPOINT append idx={idx}");
                kill_self();
            }
        }
        let due = entries_since_commit >= commit_entries
            || (commit_ms > 0 && last_commit.elapsed() >= Duration::from_millis(commit_ms));
        if due {
            let t_c = Instant::now();
            if let Some(k) = kill_at {
                if start.elapsed() >= k
                    && (kill_point == "precommit" || (kill_point == "random" && idx % 3 == 1))
                {
                    eprintln!("KILLPOINT precommit idx={idx}");
                    kill_self();
                }
            }
            eng.commit(&ops, false)?;
            h_nondur.record(t_c.elapsed().as_micros() as u64);
            ops.clear();
            entries_since_commit = 0;
            store_commits += 1;
            last_commit = Instant::now();
            if let Some(k) = kill_at {
                if start.elapsed() >= k
                    && (kill_point == "postcommit" || (kill_point == "random" && idx % 3 == 2))
                {
                    eprintln!("KILLPOINT postcommit idx={idx}");
                    kill_self();
                }
            }
        }
        h_entry.record(t_entry.elapsed().as_micros() as u64);

        // ---- durable point (§11.4) ----
        if last_durable.elapsed() >= Duration::from_millis(durable_ms)
            || bytes_since_durable >= durable_bytes
        {
            let t_d = Instant::now();
            let (files, _) = segs
                .sync_dirty(fsync_threads, fsync_mode)
                .map_err(|e| e.to_string())?;
            let fsync_ms = t_d.elapsed().as_micros() as u64;
            h_fsync.record(fsync_ms);
            fsync_files += files;
            let mut m = [0u8; 24];
            m[0..8].copy_from_slice(&idx.to_le_bytes());
            m[8..16].copy_from_slice(&1u64.to_le_bytes());
            m[16..24].copy_from_slice(&now_us.to_le_bytes());
            ops.put(T_META, M_APPLIED, &m);
            ops.put(T_META, M_DURABLE, &m);
            ops.put(T_META, M_FILELENS, &segs.file_lengths());
            eng.commit(&ops, true)?;
            ops.clear();
            entries_since_commit = 0;
            store_commits += 1;
            last_commit = Instant::now();
            h_durable.record(t_d.elapsed().as_micros() as u64);
            durable_points += 1;
            last_durable = Instant::now();
            bytes_since_durable = 0;
        }
        if last_rss.elapsed() >= sample {
            rss_series.push((start.elapsed().as_secs_f64(), sys::rss_bytes()));
            last_rss = Instant::now();
        }
        if growth_every > 0 && last_growth.elapsed() >= Duration::from_secs(growth_every) {
            println!(
                "GROWTH t_s={:.1} rss_mib={:.1} store_mib={:.1} seg_mib={:.1} seg_files={} entries={} msgs={}",
                start.elapsed().as_secs_f64(),
                mib(sys::rss_bytes()),
                mib(store_bytes(&dir, &seg_dir).saturating_sub(store0)),
                mib(sys::dir_alloc_bytes(&seg_dir).saturating_sub(seg0)),
                sys::file_count(&seg_dir),
                gen.c.entries,
                gen.c.msgs
            );
            last_growth = Instant::now();
        }
        // ---- mid-run delete phase: drop the oldest --delete-pct of the rows
        // written so far, unlink the sealed files below them, run the engine's
        // maintenance, and report what came back (§11.7).
        if !mid_delete_done && start.elapsed().as_secs_f64() >= delete_at_s {
            mid_delete_done = true;
            // the delete phase reads and deletes rows; commit whatever the
            // open write transaction still holds so it cannot resurrect them
            if ops.len() > 0 {
                eng.commit(&ops, false)?;
                ops.clear();
                entries_since_commit = 0;
                store_commits += 1;
                last_commit = Instant::now();
            }
            let el_us = (start.elapsed().as_secs_f64() * 1e6) as i64;
            let cutoff = run_start_us + (el_us as f64 * delete_pct / 100.0) as i64;
            let before_store = store_bytes(&dir, &seg_dir);
            let before_seg = sys::dir_alloc_bytes(&seg_dir);
            let rows_before = gen.c.entries;
            let t0 = Instant::now();
            let (deleted, min_surviving) =
                reclaim_rows(eng.as_mut(), cutoff, a.u("delete-chunk", 2000) as usize)?;
            let del_ms = t0.elapsed().as_secs_f64() * 1e3;
            let mid_store = store_bytes(&dir, &seg_dir);
            let (unlinked, freed) = segs
                .unlink_sealed_below(&min_surviving)
                .map_err(|e| e.to_string())?;
            let mid_seg = sys::dir_alloc_bytes(&seg_dir);
            let t1 = Instant::now();
            let note = eng.maintain()?;
            let maint_ms = t1.elapsed().as_secs_f64() * 1e3;
            let after_store = store_bytes(&dir, &seg_dir);
            let after_seg = sys::dir_alloc_bytes(&seg_dir);
            let mid_delete_line = format!(
                "MIDDELETE t_s={:.1} pct={delete_pct:.0} rows_written={rows_before} rows_deleted={deleted} del_ms={del_ms:.0} \
maint_ms={maint_ms:.0} store_before_mib={:.1} store_after_del_mib={:.1} store_after_maint_mib={:.1} \
seg_before_mib={:.1} seg_after_unlink_mib={:.1} seg_after_maint_mib={:.1} files_unlinked={unlinked} files_freed_mib={:.1} rss_mib={:.1} maint=\"{note}\"",
                start.elapsed().as_secs_f64(),
                mib(before_store.saturating_sub(store0)),
                mib(mid_store.saturating_sub(store0)),
                mib(after_store.saturating_sub(store0)),
                mib(before_seg.saturating_sub(seg0)),
                mib(mid_seg.saturating_sub(seg0)),
                mib(after_seg.saturating_sub(seg0)),
                mib(freed),
                mib(sys::rss_bytes())
            );
            println!("{mid_delete_line}");
            // the delete phase is not load: re-base the pacing so the entries
            // it blocked are not replayed as a burst.
            pace_offset = start.elapsed().saturating_sub(interval.mul_f64(idx as f64));
        }
        idx += 1;
    }

    let wall = start.elapsed().as_secs_f64();
    // final durable point, so the run ends at a known state
    let t_d = Instant::now();
    segs.sync_dirty(fsync_threads, fsync_mode)
        .map_err(|e| e.to_string())?;
    let now_us = sys::now_micros();
    let mut m = [0u8; 24];
    m[0..8].copy_from_slice(&(idx - 1).to_le_bytes());
    m[8..16].copy_from_slice(&1u64.to_le_bytes());
    m[16..24].copy_from_slice(&now_us.to_le_bytes());
    ops.put(T_META, M_APPLIED, &m);
    ops.put(T_META, M_DURABLE, &m);
    ops.put(T_META, M_FILELENS, &segs.file_lengths());
    eng.commit(&ops, true)?;
    let final_durable_ms = t_d.elapsed().as_secs_f64() * 1e3;

    let io1 = sys::disk_written_bytes();
    let store1 = store_bytes(&dir, &seg_dir);
    let seg1 = sys::dir_alloc_bytes(&seg_dir);
    let rss1 = sys::rss_bytes();
    let c = &gen.c;
    let logical = c.logical_bytes + c.payload_bytes;
    let io_written = io1.saturating_sub(io0);
    let dir_growth = (store1 - store0) + (seg1 - seg0);

    println!("\n--- throughput ---");
    println!(
        "wall {wall:.1} s, {} entries ({:.0}/s target {:.0}/s), {} messages ({:.0} msg/s)",
        c.entries,
        c.entries as f64 / wall,
        entries_per_s,
        c.msgs,
        c.msgs as f64 / wall
    );
    println!(
        "store ops {} ({:.0}/s), kv put/cas/del/list {}/{}/{}/{}, timers sched/fired {}/{}, request ids expired {}",
        c.store_ops,
        c.store_ops as f64 / wall,
        c.kv_puts,
        c.kv_cas,
        c.kv_dels,
        c.kv_lists,
        c.timers_scheduled,
        c.timers_fired,
        c.reqs_expired
    );
    println!(
        "dedup rows written {} ({:.0}/s), pruned {}",
        c.dedup_rows,
        c.dedup_rows as f64 / wall,
        c.dedup_pruned
    );
    println!(
        "pacing: max lag {:.1} ms, entries more than 50 ms late {}",
        max_lag_us as f64 / 1000.0,
        behind_entries
    );

    println!("\n--- latency (ms) ---");
    println!(
        "non-durable store commit ({} commits, {:.0}/s, {:.1} entries per commit)",
        store_commits,
        store_commits as f64 / wall,
        c.entries as f64 / store_commits.max(1) as f64
    );
    println!("non-durable commit   {}", h_nondur.line());
    println!("entry total          {}", h_entry.line());
    println!("durable point        {}", h_durable.line());
    println!("  of which seg fsync {}", h_fsync.line());
    println!("kv read (get)        {}", h_read.line());
    println!("kv prefix list       {}", h_list.line());
    println!("durable points {durable_points}, segment fsyncs {fsync_files}, final durable point {final_durable_ms:.1} ms");

    println!("\n--- write amplification ---");
    println!(
        "logical: store keys+values {:.1} MiB + payloads {:.1} MiB = {:.1} MiB",
        mib(c.logical_bytes),
        mib(c.payload_bytes),
        mib(logical)
    );
    if io_written > 0 {
        println!(
            "process disk writes {:.1} MiB  ->  WA(io) {:.2}x",
            mib(io_written),
            io_written as f64 / logical as f64
        );
    } else {
        println!("process disk writes UNAVAILABLE on this host (kernel counter returned 0)");
    }
    println!(
        "on-disk growth: store {:.1} MiB + segments {:.1} MiB = {:.1} MiB  ->  WA(files) {:.2}x  (store alone {:.2}x of its own {:.1} MiB of rows)",
        mib(store1 - store0),
        mib(seg1 - seg0),
        mib(dir_growth),
        dir_growth as f64 / logical as f64,
        (store1 - store0) as f64 / c.logical_bytes as f64,
        mib(c.logical_bytes)
    );
    println!(
        "segment files: {} frames, {:.1} MiB appended, {} files, engine reports {:.1} MiB of store",
        segs.appended_frames,
        mib(segs.appended_bytes),
        sys::file_count(segs.dir()),
        mib(eng.reported_disk())
    );

    println!("\n--- RSS ---");
    println!("start {:.1} MiB, end {:.1} MiB", mib(rss0), mib(rss1));
    let rmax = rss_series.iter().map(|x| x.1).max().unwrap_or(0);
    println!("max {:.1} MiB over {} samples", mib(rmax), rss_series.len());
    let step = (rss_series.len() / 12).max(1);
    let line: Vec<String> = rss_series
        .iter()
        .step_by(step)
        .map(|(t, r)| format!("{t:.0}s:{:.0}M", mib(*r)))
        .collect();
    println!("series {}", line.join(" "));

    // ---- consistent export (snapshot, §11.6 step 3) ----
    let mut export_ms = 0.0;
    let mut export_bytes = 0u64;
    if a.b("export", true) {
        println!("\n--- consistent export (snapshot) ---");
        let dest = dir.join("export.bin");
        let parts = eng.export(&dest)?;
        for p in &parts {
            println!(
                "{:<36} {:>9.1} MiB in {:>8.0} ms ({:.0} MiB/s)",
                p.method,
                mib(p.bytes),
                p.ms,
                mib(p.bytes) / (p.ms / 1000.0)
            );
            if export_ms == 0.0 {
                export_ms = p.ms;
                export_bytes = p.bytes;
            }
        }
        let caps = eng.caps();
        println!("incremental checkpoint: {}", caps.incremental_checkpoint);
        println!("writer pause:           {}", caps.writer_pause);
        let _ = std::fs::remove_file(&dest);
        let _ = std::fs::remove_file(dest.with_extension("compacted"));
    }

    // ---- ordered iteration over what is in the store ----
    // In the ratified shape `segments` does not exist; the equivalent ordered
    // walk is over the node-local `seg_loc` (same key, §6.2) and, for the
    // keyspace that dominates that store, over `dedup`.
    let scan_table = if p.segments_in_store {
        T_SEGMENTS
    } else {
        T_SEG_LOC
    };
    let t0 = Instant::now();
    let (rows, bytes) = eng.scan(scan_table)?;
    let scan_s = t0.elapsed().as_secs_f64();
    if p.dedup_rows {
        let t = Instant::now();
        let (drows, dbytes) = eng.scan(T_DEDUP)?;
        let ds = t.elapsed().as_secs_f64();
        println!("\n--- ordered iteration (dedup, the ratified store's largest keyspace) ---");
        println!(
            "{drows} rows, {:.1} MiB in {ds:.2} s = {:.0} k rows/s, {:.0} MiB/s",
            mib(dbytes),
            drows as f64 / ds / 1000.0,
            mib(dbytes) / ds
        );
    }
    println!(
        "\n--- ordered iteration ({} table as it stands) ---",
        TABLES[scan_table as usize]
    );
    println!(
        "{rows} rows, {:.1} MiB in {:.2} s = {:.0} k rows/s, {:.0} MiB/s",
        mib(bytes),
        scan_s,
        rows as f64 / scan_s / 1000.0,
        mib(bytes) / scan_s
    );

    // ---- reclamation after deletes (retention, §11.7) ----
    let mut reclaim_line = String::new();
    if a.b("reclaim", true) {
        println!("\n--- file growth and reclamation after deletes ---");
        let cutoff = sys::now_micros() - ((wall * 1e6 / 2.0) as i64); // delete the older half
        let before_store = store_bytes(&dir, &seg_dir);
        let before_seg = sys::dir_alloc_bytes(&seg_dir);
        let t0 = Instant::now();
        let (deleted, min_surviving) =
            reclaim_rows(eng.as_mut(), cutoff, a.u("delete-chunk", 2000) as usize)?;
        let del_ms = t0.elapsed().as_secs_f64() * 1e3;
        let mid_store = store_bytes(&dir, &seg_dir);
        let (unlinked, freed) = segs
            .unlink_sealed_below(&min_surviving)
            .map_err(|e| e.to_string())?;
        let mid_seg = sys::dir_alloc_bytes(&seg_dir);
        let t1 = Instant::now();
        let note = eng.maintain()?;
        let maint_ms = t1.elapsed().as_secs_f64() * 1e3;
        let after_store = store_bytes(&dir, &seg_dir);
        println!(
            "deleted {deleted} segment rows (+ the same number of seg_loc rows) in {del_ms:.0} ms",
        );
        println!(
            "store: {:.1} MiB before -> {:.1} MiB after the deletes -> {:.1} MiB after maintenance ({maint_ms:.0} ms)",
            mib(before_store),
            mib(mid_store),
            mib(after_store)
        );
        println!("maintenance: {note}");
        println!(
            "segment files: {:.1} MiB before -> {:.1} MiB after unlinking {unlinked} sealed files ({:.1} MiB freed)",
            mib(before_seg),
            mib(mid_seg),
            mib(freed)
        );
        reclaim_line = format!(
            "reclaim_rows={deleted} reclaim_del_ms={del_ms:.0} store_before_mib={:.1} store_after_del_mib={:.1} store_after_maint_mib={:.1} maint_ms={maint_ms:.0} seg_files_unlinked={unlinked}",
            mib(before_store),
            mib(mid_store),
            mib(after_store)
        );
    }

    // ---- clean close + reopen (the good case of §11.5) ----
    drop(eng);
    let t0 = Instant::now();
    let eng2 = engines::open(&engine, &dir, &eopts)?;
    let reopen_ms = t0.elapsed().as_secs_f64() * 1e3;
    let applied = read_u64(&*eng2, M_APPLIED)?;
    let durable = read_u64(&*eng2, M_DURABLE)?;
    println!("\n--- clean reopen ---");
    println!(
        "open {open_ms:.0} ms (fresh) / {reopen_ms:.0} ms (populated); applied index {applied:?}, durable index {durable:?}"
    );

    println!(
        "\nRESULT engine={engine} rate={} batch={} payload={} duration={wall:.1} entries={} msgs={} entries_per_s={:.0} msgs_per_s={:.0} \
nondur_p50_ms={:.3} nondur_p99_ms={:.3} nondur_p999_ms={:.3} dur_p50_ms={:.3} dur_p99_ms={:.3} dur_max_ms={:.3} fsync_p99_ms={:.3} \
read_p99_ms={:.3} list_p99_ms={:.3} wa_io={:.2} wa_files={:.2} logical_mib={:.1} io_mib={:.1} store_growth_mib={:.1} seg_growth_mib={:.1} \
rss_start_mib={:.1} rss_max_mib={:.1} rss_end_mib={:.1} export_mib={:.1} export_ms={:.0} scan_rows={rows} scan_rows_per_s={:.0} \
reopen_ms={reopen_ms:.0} max_lag_ms={:.1} late_entries={behind_entries} durable_points={durable_points} \
shape={} store_commits={store_commits} store_commits_per_s={:.0} entries_per_commit={:.1} dedup_rows={} heed_flags={} heed_no_tls={} {reclaim_line}",
        a.u("rate", 20_000),
        a.u("batch", 10),
        a.u("payload", 512),
        c.entries,
        c.msgs,
        c.entries as f64 / wall,
        c.msgs as f64 / wall,
        h_nondur.pct(0.50) as f64 / 1000.0,
        h_nondur.pct(0.99) as f64 / 1000.0,
        h_nondur.pct(0.999) as f64 / 1000.0,
        h_durable.pct(0.50) as f64 / 1000.0,
        h_durable.pct(0.99) as f64 / 1000.0,
        h_durable.max() as f64 / 1000.0,
        h_fsync.pct(0.99) as f64 / 1000.0,
        h_read.pct(0.99) as f64 / 1000.0,
        h_list.pct(0.99) as f64 / 1000.0,
        if io_written > 0 { io_written as f64 / logical as f64 } else { 0.0 },
        dir_growth as f64 / logical as f64,
        mib(logical),
        mib(io_written),
        mib(store1 - store0),
        mib(seg1 - seg0),
        mib(rss0),
        mib(rmax),
        mib(rss1),
        mib(export_bytes),
        export_ms,
        rows as f64 / scan_s,
        max_lag_us as f64 / 1000.0,
        a.s("shape", "legacy"),
        store_commits as f64 / wall,
        c.entries as f64 / store_commits.max(1) as f64,
        c.dedup_rows,
        a.s("heed-flags", "nosync"),
        a.b("heed-no-tls", false),
    );
    Ok(0)
}

fn kill_self() -> ! {
    // SIGKILL to ourselves: no destructor, no flush, exactly what kill -9 does.
    unsafe {
        libc::raise(libc::SIGKILL);
    }
    std::process::exit(9);
}

fn read_u64(eng: &dyn Engine, key: &[u8]) -> Result<Option<(u64, u64, i64)>, String> {
    Ok(eng.get(T_META, key)?.map(|v| {
        (
            u64::from_le_bytes(v[0..8].try_into().unwrap()),
            u64::from_le_bytes(v[8..16].try_into().unwrap()),
            i64::from_le_bytes(v[16..24].try_into().unwrap()),
        )
    }))
}

/// Chunked deletes of every segment older than `cutoff`, returning the number
/// of rows deleted and, per bucket, the lowest file id that still holds a live
/// segment (so the files below it can be unlinked, §11.7).
fn reclaim_rows(
    eng: &mut dyn Engine,
    cutoff: i64,
    chunk: usize,
) -> Result<(u64, Vec<u32>), String> {
    const CAP: usize = 2_000_000;
    let mut deleted = 0u64;
    let mut min_surviving = vec![u32::MAX; seg::NBUCKETS];
    loop {
        let mut victims: Vec<[u8; 16]> = Vec::new();
        min_surviving = vec![u32::MAX; seg::NBUCKETS];
        let mut more = false;
        eng.scan_cb(T_SEG_LOC, &mut |k, v| {
            let created = i64::from_le_bytes(v[26..34].try_into().unwrap());
            let bucket = u16::from_le_bytes(v[0..2].try_into().unwrap()) as usize;
            let file_id = u32::from_le_bytes(v[2..6].try_into().unwrap());
            if created < cutoff {
                if victims.len() < CAP {
                    let mut key = [0u8; 16];
                    key.copy_from_slice(k);
                    victims.push(key);
                } else {
                    more = true;
                    return false;
                }
            } else if file_id < min_surviving[bucket] {
                min_surviving[bucket] = file_id;
            }
            true
        })?;
        if victims.is_empty() {
            break;
        }
        let mut ops = OpBuf::new();
        for (i, k) in victims.iter().enumerate() {
            ops.del(T_SEGMENTS, k);
            ops.del(T_SEG_LOC, k);
            if ops.len() >= chunk || i + 1 == victims.len() {
                eng.commit(&ops, false)?;
                ops.clear();
            }
        }
        deleted += victims.len() as u64;
        if !more {
            break;
        }
    }
    let mut ops = OpBuf::new();
    ops.put(T_META, b"reclaimed", &deleted.to_le_bytes());
    eng.commit(&ops, true)?;
    Ok((deleted, min_surviving))
}

// ------------------------------------------------------------- verify -------

fn cmd_verify(a: &Args) -> Result<i32, String> {
    let engine = a.s("engine", "redb");
    let dir = PathBuf::from(a.s("dir", "/tmp/s1-store"));
    let eopts = eng_opts(a);
    let limit = a.u("verify-limit", 200_000) as usize;
    let all = a.b("verify-all", false);

    let t0 = Instant::now();
    let eng = engines::open(&engine, &dir, &eopts)?;
    let open_ms = t0.elapsed().as_secs_f64() * 1e3;
    let applied = read_u64(&*eng, M_APPLIED)?;
    let durable = read_u64(&*eng, M_DURABLE)?;
    let (ai, _at, _an) = applied.unwrap_or((0, 0, 0));
    let (di, _dt, _dn) = durable.unwrap_or((0, 0, 0));
    let past_durable = ai > di;

    // recorded file lengths at the last durable point (I11)
    let mut recorded: HashMap<(u16, u32), u64> = HashMap::new();
    if let Some(v) = eng.get(T_META, M_FILELENS)? {
        for ch in v.chunks_exact(14) {
            let b = u16::from_le_bytes(ch[0..2].try_into().unwrap());
            let f = u32::from_le_bytes(ch[2..6].try_into().unwrap());
            let l = u64::from_le_bytes(ch[6..14].try_into().unwrap());
            recorded.insert((b, f), l);
        }
    }

    let t1 = Instant::now();
    let mut checked = 0u64;
    let mut missing_frame = 0u64;
    let mut bad_checksum = 0u64;
    let mut short_file = 0u64;
    let mut missing_seg_row: u64 = 0;
    let mut past_recorded_len = 0u64;
    let mut max_entry_idx = 0u64;
    let mut errs: Vec<String> = Vec::new();
    let mut rows_seen = 0u64;

    eng.scan_cb(T_SEG_LOC, &mut |_k, v| {
        rows_seen += 1;
        if !all && checked >= limit as u64 {
            return false;
        }
        let bucket = u16::from_le_bytes(v[0..2].try_into().unwrap());
        let file_id = u32::from_le_bytes(v[2..6].try_into().unwrap());
        let offset = u64::from_le_bytes(v[6..14].try_into().unwrap());
        let len = u32::from_le_bytes(v[14..18].try_into().unwrap());
        let hash = u64::from_le_bytes(v[18..26].try_into().unwrap());
        checked += 1;
        match seg::read_frame(&dir, bucket, file_id, offset, len) {
            Ok((h, _pid, _base, _count)) => {
                if h != hash {
                    bad_checksum += 1;
                    if errs.len() < 5 {
                        errs.push(format!(
                            "b{bucket} f{file_id}@{offset}: stored hash != frame hash"
                        ));
                    }
                }
            }
            Err(e) => {
                if e.contains("short file") {
                    short_file += 1;
                } else if e.contains("checksum") {
                    bad_checksum += 1;
                } else {
                    missing_frame += 1;
                }
                if errs.len() < 5 {
                    errs.push(format!("b{bucket} f{file_id}@{offset}+{len}: {e}"));
                }
            }
        }
        if let Some(rl) = recorded.get(&(bucket, file_id)) {
            if offset + len as u64 > *rl {
                past_recorded_len += 1;
            }
        }
        true
    })?;

    // Cross-table check without nesting read transactions (LMDB binds a read
    // txn to the thread, so a `get` inside a scan is not allowed): scan both
    // tables independently and compare row counts and an order-independent
    // digest of their keys.
    let mut seg_rows = 0u64;
    let mut seg_digest = 0u64;
    let mut loc_rows = 0u64;
    let mut loc_digest = 0u64;
    eng.scan_cb(T_SEGMENTS, &mut |k, v| {
        seg_rows += 1;
        seg_digest ^= xxhash_rust::xxh3::xxh3_64(k);
        if v.len() >= 36 {
            let e = u64::from_le_bytes(v[28..36].try_into().unwrap());
            if e > max_entry_idx {
                max_entry_idx = e;
            }
        }
        true
    })?;
    eng.scan_cb(T_SEG_LOC, &mut |k, _v| {
        loc_rows += 1;
        loc_digest ^= xxhash_rust::xxh3::xxh3_64(k);
        true
    })?;
    // In the ratified shape (§6.1 G0 amendment) `segments` is not in the store
    // at all, so there is no cross-table pair to compare; `seg_loc` alone is
    // the node-local index, and every frame it names has been read above.
    let cross_table_checked = seg_rows > 0;
    if cross_table_checked && (seg_rows != loc_rows || seg_digest != loc_digest) {
        missing_seg_row = seg_rows.abs_diff(loc_rows).max(1);
        errs.push(format!(
            "segments and seg_loc disagree: {seg_rows} vs {loc_rows} rows, digest {seg_digest:#x} vs {loc_digest:#x}"
        ));
    }
    let verify_ms = t1.elapsed().as_secs_f64() * 1e3;

    let agree = missing_frame == 0 && bad_checksum == 0 && short_file == 0 && missing_seg_row == 0;
    let torn = max_entry_idx > ai;
    println!("=== s1-store verify ({engine}, {}) ===", dir.display());
    println!(
        "open {open_ms:.0} ms, checked {checked} of {rows_seen} seg_loc rows in {verify_ms:.0} ms"
    );
    println!("applied index {ai}, last durable index {di} -> reopened_past_durable={past_durable} (delta {} entries)", ai.saturating_sub(di));
    println!(
        "frames: missing {missing_frame}, bad checksum {bad_checksum}, past EOF {short_file}, beyond the recorded durable length {past_recorded_len}"
    );
    println!("cross-table: segments {seg_rows} rows / seg_loc {loc_rows} rows, key digests {}; max entry index in segments {max_entry_idx} (applied {ai}) torn={torn}",
        if !cross_table_checked { "NOT CHECKED (ratified shape: segments is not in the store)" }
        else if seg_digest == loc_digest { "equal" } else { "DIFFERENT" });
    for e in &errs {
        println!("  ! {e}");
    }
    println!(
        "VERIFY {} agree={agree} torn={torn} applied={ai} durable={di} past_durable={past_durable} checked={checked} rows={rows_seen} missing={missing_frame} badsum={bad_checksum} shortfile={short_file} cross_table_mismatch={missing_seg_row} beyond_recorded={past_recorded_len} open_ms={open_ms:.0} verify_ms={verify_ms:.0}",
        if agree && !torn { "PASS" } else { "FAIL" }
    );
    Ok(if agree && !torn { 0 } else { 1 })
}

// --------------------------------------------------------------- iter -------

fn cmd_iter(a: &Args) -> Result<i32, String> {
    let engine = a.s("engine", "redb");
    let dir = PathBuf::from(a.s("dir", "/tmp/s1-iter"));
    let keys = a.u("iter-keys", 10_000_000);
    let per_txn = a.u("iter-txn", 10_000);
    // A durable commit every N transactions. It matters more than it looks:
    // redb only frees pages at a commit with a durability above None, so a
    // bulk load that never takes a durable point grows without bound.
    let durable_every = a.u("iter-durable-txns", 10);
    let eopts = eng_opts(a);
    if a.b("fresh", true) {
        let _ = std::fs::remove_dir_all(&dir);
    }
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let mut eng = engines::open(&engine, &dir, &eopts)?;
    println!(
        "=== s1-store iter ({engine}) === {keys} keys of 16 B -> 40 B, {per_txn} per transaction"
    );

    let t0 = Instant::now();
    let mut ops = OpBuf::new();
    let mut i = 0u64;
    let mut txns = 0u64;
    while i < keys {
        ops.clear();
        for _ in 0..per_txn {
            if i >= keys {
                break;
            }
            // (pid, base_offset): 4096 partitions, ascending offsets, so the
            // key order is the order a digest or a snapshot export walks.
            let pid = i % 4096;
            let off = i / 4096;
            let mut k = [0u8; 16];
            k[0..8].copy_from_slice(&pid.to_be_bytes());
            k[8..16].copy_from_slice(&(off * 10).to_be_bytes());
            let mut v = [0u8; 40];
            v[0..8].copy_from_slice(&(off * 10 + 10).to_le_bytes());
            v[28..36].copy_from_slice(&i.to_le_bytes());
            ops.put(T_SEGMENTS, &k, &v);
            i += 1;
        }
        txns += 1;
        eng.commit(&ops, txns % durable_every == 0)?;
    }
    ops.clear();
    ops.put(T_META, M_APPLIED, &[0u8; 24]);
    eng.commit(&ops, true)?;
    let load_s = t0.elapsed().as_secs_f64();
    let size = sys::dir_alloc_bytes(&dir);
    println!(
        "load {keys} keys in {load_s:.1} s = {:.0} k keys/s; on disk {:.1} MiB ({:.1} B/key, logical 56 B/key)",
        keys as f64 / load_s / 1000.0,
        mib(size),
        size as f64 / keys as f64
    );

    let mut best = f64::MAX;
    for pass in 0..2 {
        let t = Instant::now();
        let (rows, bytes) = eng.scan(T_SEGMENTS)?;
        let s = t.elapsed().as_secs_f64();
        best = best.min(s);
        println!(
            "pass {pass}: {rows} rows, {:.1} MiB in {s:.2} s = {:.0} k rows/s, {:.0} MiB/s",
            mib(bytes),
            rows as f64 / s / 1000.0,
            mib(bytes) / s
        );
    }
    // range scans of 1000 rows from 100 random starts
    let mut h = Hist::new();
    for j in 0..100u64 {
        let pid = (j * 41) % 4096;
        let t = Instant::now();
        let n = eng.prefix_count(T_SEGMENTS, &pid.to_be_bytes(), 1000)?;
        h.record(t.elapsed().as_micros() as u64);
        if n == 0 {
            return Err("empty prefix scan".into());
        }
    }
    println!("range scan of 1000 rows: {}", h.line());
    println!(
        "RESULT-ITER engine={engine} keys={keys} load_s={load_s:.1} load_keys_per_s={:.0} disk_mib={:.1} bytes_per_key={:.1} scan_s={best:.2} scan_rows_per_s={:.0} range1000_p50_ms={:.3} range1000_p99_ms={:.3}",
        keys as f64 / load_s,
        mib(size),
        size as f64 / keys as f64,
        keys as f64 / best,
        h.pct(0.5) as f64 / 1000.0,
        h.pct(0.99) as f64 / 1000.0
    );
    Ok(0)
}

// -------------------------------------------------------------- reads -------

/// The D15 read path, measured the way the broker would run it: N THREADS of
/// point gets and prefix lists against a store an earlier `run` left behind.
///
/// This exists because the first campaign reported `get p99 2-3 us`,
/// `list p99 11 us` and `44.2 M rows/s` for heed from a SINGLE thread with
/// heed's default `WithTls` reader slots — a mode in which `RoTxn` is not
/// `Send` (it cannot cross an `.await` or move between tokio workers) and a
/// nested read txn on one thread is illegal. `--heed-no-tls` opens the same
/// store with MDB_NOTLS, and `--readers` probes the reader-slot ceiling
/// (`MDB_READERS_FULL`), which is a node-local liveness cliff, not a latency.
fn cmd_reads(a: &Args) -> Result<i32, String> {
    let engine = a.s("engine", "heed");
    let dir = PathBuf::from(a.s("dir", "/tmp/s1-store"));
    let eopts = eng_opts(a);
    let threads = a.u("read-threads", 1).max(1) as usize;
    let secs = a.f("duration", 10.0);
    let kv_keys = a.u("kv-keys", 200_000);
    let partitions = a.u("partitions", 4096);
    let list_every = a.u("list-every", 8).max(1);
    let probe = a.u("readers", 0) as usize;

    let eng = engines::open(&engine, &dir, &eopts)?;
    println!("=== s1-store reads ({engine}) === {}", dir.display());
    println!("config      {}", eng.config_note());
    println!("threads     {threads}, duration {secs} s, 1 prefix list per {list_every} gets");

    if probe > 0 {
        // one thread holds as many simultaneous read transactions as it can
        let (opened, err) = eng.hold_readers(probe)?;
        println!(
            "reader-slot probe: asked for {probe} simultaneous read txns, opened {opened}{}",
            if err.is_empty() {
                " (no ceiling hit)".to_string()
            } else {
                format!(", then: {err}")
            }
        );
        println!("READERS engine={engine} asked={probe} opened={opened} err=\"{err}\"");
    }

    let engr: &dyn Engine = &*eng;
    let stop = std::time::Instant::now() + Duration::from_secs_f64(secs);
    let mut per: Vec<(u64, u64, Hist, Hist)> = Vec::new();
    std::thread::scope(|sc| -> Result<(), String> {
        let mut hs = Vec::new();
        for t in 0..threads {
            hs.push(sc.spawn(move || -> Result<(u64, u64, Hist, Hist), String> {
                let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (t as u64 + 1);
                let mut hg = Hist::new();
                let mut hl = Hist::new();
                let (mut gets, mut lists) = (0u64, 0u64);
                while Instant::now() < stop {
                    for _ in 0..64 {
                        rng ^= rng >> 12;
                        rng ^= rng << 25;
                        rng ^= rng >> 27;
                        let r = rng.wrapping_mul(0x2545_F491_4F6C_DD1D);
                        if gets % list_every == 0 {
                            let pid = r % partitions;
                            let t0 = Instant::now();
                            let _ = engr.prefix_count(T_SEG_LOC, &pid.to_be_bytes(), 100)?;
                            hl.record(t0.elapsed().as_micros() as u64);
                            lists += 1;
                        }
                        let mut k = [0u8; 12];
                        k[0..4].copy_from_slice(&0u32.to_be_bytes());
                        k[4..12].copy_from_slice(&(r % kv_keys).to_be_bytes());
                        let t0 = Instant::now();
                        let _ = engr.get(T_KV, &k)?;
                        hg.record(t0.elapsed().as_micros() as u64);
                        gets += 1;
                    }
                }
                Ok((gets, lists, hg, hl))
            }));
        }
        for h in hs {
            per.push(
                h.join()
                    .map_err(|_| "reader thread panicked".to_string())??,
            );
        }
        Ok(())
    })?;

    let gets: u64 = per.iter().map(|x| x.0).sum();
    let lists: u64 = per.iter().map(|x| x.1).sum();
    let mut hg = Hist::new();
    let mut hl = Hist::new();
    for (_, _, g, l) in &per {
        hg.merge(g);
        hl.merge(l);
    }
    println!(
        "gets  {gets} ({:.0}/s aggregate) {}",
        gets as f64 / secs,
        hg.line()
    );
    println!(
        "lists {lists} ({:.0}/s aggregate) {}",
        lists as f64 / secs,
        hl.line()
    );
    println!(
        "RESULT-READS engine={engine} threads={threads} duration={secs} heed_no_tls={} heed_flags={} \
gets={gets} gets_per_s={:.0} get_p50_ms={:.3} get_p99_ms={:.3} lists={lists} lists_per_s={:.0} \
list_p50_ms={:.3} list_p99_ms={:.3}",
        a.b("heed-no-tls", false),
        a.s("heed-flags", "nosync"),
        gets as f64 / secs,
        hg.pct(0.50) as f64 / 1000.0,
        hg.pct(0.99) as f64 / 1000.0,
        lists as f64 / secs,
        hl.pct(0.50) as f64 / 1000.0,
        hl.pct(0.99) as f64 / 1000.0,
    );
    Ok(0)
}

// --------------------------------------------------------------- info -------

fn cmd_info(_a: &Args) -> Result<i32, String> {
    let tmp = std::env::temp_dir().join(format!("s1-info-{}", std::process::id()));
    for e in engines::ALL {
        let d = tmp.join(e);
        let eng = engines::open(
            e,
            &d,
            &EngOpts {
                map_size: 1 << 30,
                cache_bytes: 64 << 20,
                ..Default::default()
            },
        )?;
        let c = eng.caps();
        println!("== {e} ==");
        println!("  export:        {}", c.export_method);
        println!("  incremental:   {}", c.incremental_checkpoint);
        println!("  writer pause:  {}", c.writer_pause);
        println!("  crash model:   {}", c.crash_model);
        println!("  config:        {}", eng.config_note());
        drop(eng);
    }
    let _ = std::fs::remove_dir_all(&tmp);
    Ok(0)
}
