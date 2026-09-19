//! Numbers this WP owes the record, run on demand:
//! `cargo test --lib rsm::segments::tests::measure -- --ignored --nocapture`.
//!
//! They are LAPTOP numbers and prove nothing about throughput (§0.3: numbers
//! to quote come from the Linux VM). What they do pin down is the two shapes
//! WP-1.11 has to budget for: how much `.qidx` a segment file costs, and how
//! many records the active file's RAM index holds per bucket — the I8 bound of
//! `index::ActiveIndexes`.

use std::time::Instant;

use super::super::*;
use super::*;

fn report(label: &str, messages_per_frame: u32, blob_len: usize) {
    let d = TmpDir::new("measure");
    // One 4 MiB file, so the whole thing stays small on a laptop with 10 GiB
    // free; the ratios do not depend on the file size.
    let segment_bytes = 4 * 1024 * 1024;
    let mut s = fresh(&d, segment_bytes);
    let mut frames = 0u64;
    let mut base = 0u64;
    while s.active_len(0) + frame::encoded_len(messages_per_frame, blob_len) as u64 <= segment_bytes
    {
        push(&mut s, 0, 1, base, messages_per_frame, blob_len);
        base += messages_per_frame as u64;
        frames += 1;
    }
    let seg_bytes = s.active_len(0);
    let ram = s.active_index_len();
    s.roll(0).expect("roll");
    s.forget_sealed(0, 0);
    let qidx_bytes = std::fs::metadata(qidx_file(&d, 0, 0)).expect("meta").len();

    // Lookups against the sealed file, through the mapped index.
    let probes = 20_000u32;
    let t = Instant::now();
    let mut hits = 0u64;
    for i in 0..probes {
        let off = (i as u64 * 7919) % base;
        if s.locate(0, 1, off, &[0]).expect("locate").is_some() {
            hits += 1;
        }
    }
    let per = t.elapsed().as_nanos() as f64 / probes as f64;
    assert_eq!(hits, probes as u64);

    let t = Instant::now();
    for i in 0..probes {
        let off = (i as u64 * 7919) % base;
        let l = s.locate(0, 1, off, &[0]).expect("locate").expect("hit");
        s.read_blob(l.position).expect("read");
    }
    let per_read = t.elapsed().as_nanos() as f64 / probes as f64;

    println!(
        "{label:22} frames {frames:6}  msgs {:8}  seg {:9} B  qidx {qidx_bytes:8} B \
         ({:.2}% of the file, {:.1} B/frame)  RAM records {ram:6}  \
         locate {per:7.0} ns  locate+read {per_read:7.0} ns",
        base,
        seg_bytes,
        100.0 * qidx_bytes as f64 / seg_bytes as f64,
        qidx_bytes as f64 / frames as f64,
    );
}

/// How long a boot spends rebuilding one bucket's active-file RAM index by
/// scanning it (§11.5 step 4). Multiply by the number of buckets that have a
/// non-empty active file to get the whole boot cost: it is bounded by
/// `256 * QUEEN_RAFT_SEGMENT_BYTES`, never by retained volume, but the
/// constant is what WP-1.4 has to live with.
fn report_boot(label: &str, messages_per_frame: u32, blob_len: usize) {
    let d = TmpDir::new("measure-boot");
    let segment_bytes = 4 * 1024 * 1024;
    let mut s = fresh(&d, segment_bytes);
    let mut base = 0u64;
    while s.active_len(0) + frame::encoded_len(messages_per_frame, blob_len) as u64 <= segment_bytes
    {
        push(&mut s, 0, 1, base, messages_per_frame, blob_len);
        base += messages_per_frame as u64;
    }
    s.durable_point().expect("durable point");
    let state: Vec<FileState> = s.file_states();
    let bytes = s.active_len(0);
    drop(s);

    // The FIXED cost first: reopening a tree whose 256 buckets are all empty.
    // That is 256 create_dir_all + read_dir + metadata + open calls, and it is
    // paid once however much data the node holds. Without subtracting it, a
    // "scan throughput" number is mostly syscalls.
    let e = TmpDir::new("measure-boot-empty");
    let empty_state: Vec<FileState> = fresh(&e, segment_bytes).file_states();
    let t = Instant::now();
    drop(Segments::open(&e.seg(), Options::testing(segment_bytes), &empty_state).expect("open"));
    let fixed = t.elapsed();

    let t = Instant::now();
    let (s2, rep) =
        Segments::open(&d.seg(), Options::testing(segment_bytes), &state).expect("open");
    let el = t.elapsed();
    let scan = el.saturating_sub(fixed);
    println!(
        "{label:22} reopen: 256 empty buckets {fixed:?} (fixed); with one {bytes} B active file {el:?} \
         → scan {scan:?} for {} frames ({:.0} MiB/s). A FULL 64 MiB active file in every bucket \
         would add {:?} to boot",
        rep.scanned_frames,
        bytes as f64 / 1024.0 / 1024.0 / scan.as_secs_f64().max(1e-9),
        scan * 16 * 256,
    );
    assert_eq!(s2.active_len(0), bytes);
}

#[test]
#[ignore = "a measurement, not an assertion"]
fn measure_index_overhead_and_lookup() {
    println!();
    // The three shapes of §13.6's regimes: fat batches, ordinary batches, and
    // the sparse one-message-per-push case that makes the index dearest.
    report("fat batch 500x256B", 500, 500 * 256);
    report("batch 50x256B", 50, 50 * 256);
    report("one 256B message", 1, 256);
    println!();
    report_boot("fat batch 500x256B", 500, 500 * 256);
    report_boot("one 256B message", 1, 256);
}

// ---------------------------------------------------------------------------
// PERF-F: the bucket-count fan-out, on the laptop
// ---------------------------------------------------------------------------
//
// The number strace -c would count on the VM — write(2) per entry and fsync(2)
// per durable point — measured here directly and exactly, since `segment_writes`
// is the write() count the segment path issues and a durable point's
// `files_synced` is its fsync fan-out. strace itself is Linux-only, and the
// A20k/C1000/FAT100 60 s goload smokes with `strace -c` and the Prometheus
// `queen_raft_apply_segment_seconds` histogram are the VM pass; this is the
// laptop proxy that proves the lever moves the counts. The three regimes are run
// at `QUEEN_RAFT_BUCKETS` 256 (before), 16 (the new default) and 1: at 1 an
// entry's frames are one buffered run, one write, and the point one fsync.
struct Shape {
    label: &'static str,
    parts: usize,
    batch: u32,
    appends_per_entry: usize,
    blob: usize,
}

fn fanout_report(shape: &Shape, nbuckets: usize) {
    let d = TmpDir::new("measure-fanout");
    // Large files so rolls are rare and the numbers are steady-state fan-out,
    // not roll churn; buffering + the adaptive pool, the shipped write path.
    let (mut s, _) = Segments::open(&d.seg(), Options::testing_buckets(64 << 20, nbuckets), &[])
        .expect("open the fan-out tree");
    s.configure_writes(true, 4);
    let writes0 = s.segment_writes();
    let entries = 300u64;
    let durable_every = 60u64;
    let mut base = vec![0u64; shape.parts];
    let mut cursor = 0usize;
    let mut durable_files = 0u64;
    let mut durable_pts = 0u64;
    for e in 0..entries {
        for _ in 0..shape.appends_per_entry {
            let p = cursor % shape.parts;
            cursor += 1;
            let logical = (p % LOGICAL_BUCKETS) as u16;
            let pid = p as u64 + 1;
            s.append(
                logical,
                pid,
                base[p],
                shape.batch,
                1_700_000_000_000_000 + base[p] as i64,
                &hashes(pid, shape.batch),
                &blob(pid, shape.blob * shape.batch as usize),
            )
            .expect("append");
            base[p] += shape.batch as u64;
        }
        s.flush_writes().expect("flush the entry");
        if e % durable_every == durable_every - 1 {
            let dp = s.durable_point().expect("durable point");
            durable_files += dp.files_synced;
            durable_pts += 1;
            let _ = s.take_touched();
        }
    }
    let writes = s.segment_writes() - writes0;
    let appends = entries * shape.appends_per_entry as u64;
    println!(
        "{:8} buckets {nbuckets:3}: {entries} entries, {appends} appends  →  \
         write()s {writes:6} ({:.2}/entry, {:.2}/append)   \
         fsync fan-out {:.1} files/durable over {durable_pts} points",
        shape.label,
        writes as f64 / entries as f64,
        writes as f64 / appends as f64,
        durable_files as f64 / durable_pts.max(1) as f64,
    );
}

#[test]
#[ignore = "a measurement, not an assertion"]
fn measure_bucket_fanout() {
    println!();
    let shapes = [
        Shape {
            label: "A20k",
            parts: 100,
            batch: 10,
            appends_per_entry: 3,
            blob: 32,
        },
        Shape {
            label: "C1000",
            parts: 1000,
            batch: 1,
            appends_per_entry: 5,
            blob: 32,
        },
        Shape {
            label: "FAT100",
            parts: 100,
            batch: 100,
            appends_per_entry: 12,
            blob: 32,
        },
    ];
    for shape in &shapes {
        for nbuckets in [256usize, 16, 1] {
            fanout_report(shape, nbuckets);
        }
        println!();
    }
}
