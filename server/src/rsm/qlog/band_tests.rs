//! PLAN_RAFT_DRAIN_FIX P3 — the band-limited, hashes-only, cached dedup read of
//! the queue log:
//!
//! - the band read returns the SAME min in-window offset as the whole-window
//!   scan, with bands from a real [`DedupFront`] (a hash present in two
//!   records, an out-of-window earlier copy, an applied-but-uncommitted tail, a
//!   neighbour partition), and returns exactly the records overlapping the
//!   bands (active + sealed files, edge bands);
//! - the hashes-only `pread` equals the full-decode hashes (txn-envelope
//!   fallback and count-0 records included) and refuses a header that disagrees
//!   with its index entry;
//! - a second read is served from the cache: identical bytes, the same `Arc`;
//!   retention clears it.

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::set::QLogSet;
use super::{
    file_path, index, min_in_window_offset, pread_hashes, BandFrame, QLog, QLogOptions,
    RecordInput, TxnInput,
};
use crate::rsm::dedup::{self, DedupFront, ProbeVerdict, Seed, SeedHash, TxnsRow};

/// A directory that removes itself, named after its test.
struct TmpDir(PathBuf);

impl TmpDir {
    fn new(tag: &str) -> TmpDir {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "queen-rsm-qlog-p3-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).expect("temp dir");
        TmpDir(p)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// ---------------------------------------------------------------------------
// The workload: partition P (many records, 2 front generations) + neighbour Q
// ---------------------------------------------------------------------------

const P: u64 = 5;
const Q: u64 = 6;
/// Messages per P record.
const PER: u64 = 50;
/// P records written; the last two are applied but NOT committed.
const RECS: u64 = 120;
const COMMITTED_RECS: u64 = 118;
/// Records 0 and 1 are below the dedup window floor.
const FLOOR_REC: u64 = 2;

/// The duplicate: out of window at P:10, in window at P:130 and P:4500 (two
/// records, two front generations), and in the neighbour at Q:7.
const DUP: [u8; 16] = [0xD0; 16];
/// Present only in the uncommitted P record 119.
const TAIL: [u8; 16] = [0x7A; 16];

fn created(k: u64) -> i64 {
    1_000_000 + k as i64
}

fn floor() -> i64 {
    created(FLOOR_REC)
}

fn hash_at(pid: u64, off: u64) -> [u8; 16] {
    match (pid, off) {
        (P, 10) | (P, 130) | (P, 4500) | (Q, 7) => DUP,
        (P, 5950) => TAIL,
        _ => {
            let mut k = [0u8; 16];
            k[..8].copy_from_slice(&pid.to_le_bytes());
            k[8..].copy_from_slice(&off.to_le_bytes());
            xxhash_rust::xxh3::xxh3_128(&k).to_le_bytes()
        }
    }
}

fn record_hashes(pid: u64, base: u64, count: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(count as usize * 16);
    for off in base..base + count {
        out.extend_from_slice(&hash_at(pid, off));
    }
    out
}

/// Write the P/Q workload into `q` (small files, so it spans several sealed
/// files plus the active one).
fn write_workload(q: &mut QLog) {
    let payload = vec![0xEEu8; 64];
    for k in 0..RECS {
        let hp = record_hashes(P, k * PER, PER);
        let hq = record_hashes(Q, k * 3, 3);
        q.append_group(&[
            RecordInput {
                seq: 2 * k + 1,
                pid: P,
                base_offset: k * PER,
                count: PER as u32,
                created_at_us: created(k),
                txn: None,
                hashes: &hp,
                payload: &payload,
            },
            RecordInput {
                seq: 2 * k + 2,
                pid: Q,
                base_offset: k * 3,
                count: 3,
                created_at_us: created(k),
                txn: None,
                hashes: &hq,
                payload: &payload,
            },
        ])
        .expect("append");
    }
}

/// The whole committed window of `pid` in the planner's `(base, TxnsRow)` shape
/// (the `committed_txns_rows` conversion: exclusive end -> inclusive).
fn whole_rows(q: &QLog, pid: u64, committed_end: u64) -> Vec<(u64, TxnsRow)> {
    q.committed_frames(pid, 0, committed_end, true)
        .expect("whole-window read")
        .into_iter()
        .map(|f| {
            (
                f.base_offset,
                TxnsRow {
                    end: f.end - 1,
                    created_at_us: f.created_at_us,
                    hashes: f.hashes,
                },
            )
        })
        .collect()
}

/// The front the planner would hold for P: every in-window hash, committed or
/// in flight (the seed + plan-time inserts keep it a superset), in created order.
fn seeded_front() -> DedupFront {
    let front = DedupFront::new(true, 64 << 20);
    let mut seed: Vec<SeedHash> = Vec::new();
    for k in FLOOR_REC..RECS {
        for off in k * PER..(k + 1) * PER {
            seed.push(SeedHash {
                hash: hash_at(P, off),
                created_us: created(k),
                base_off: k * PER,
                msg_off: off,
            });
        }
    }
    front.install_seed(P, Seed::Complete(seed), floor());
    front
}

/// Brute-force overlap: `base <= hi && end > lo` for any non-empty band.
fn overlaps(f: &BandFrame, bands: &[(u64, u64)]) -> bool {
    bands
        .iter()
        .any(|&(lo, hi)| lo <= hi && f.base_offset <= hi && f.end > lo)
}

#[test]
fn band_read_min_equals_the_whole_window_scan() {
    let td = TmpDir::new("band-min");
    let (mut q, _) = QLog::open(td.path(), 1, QLogOptions::testing(16 * 1024)).unwrap();
    write_workload(&mut q);
    assert!(
        q.files().iter().filter(|f| f.sealed).count() >= 3,
        "the fixture must span several sealed files plus the active one"
    );
    let committed_end = COMMITTED_RECS * PER;
    let rows = whole_rows(&q, P, committed_end);
    assert_eq!(
        rows.len() as u64,
        COMMITTED_RECS,
        "whole window = every committed P record"
    );
    let front = seeded_front();

    // Probe a spread of P offsets (in and out of window, committed and not), the
    // specials, the neighbour's hashes and never-written ones.
    let mut probes: Vec<[u8; 16]> = (0..RECS * PER).step_by(37).map(|o| hash_at(P, o)).collect();
    probes.extend([DUP, TAIL, hash_at(P, 5000), hash_at(P, 99), hash_at(P, 100)]);
    probes.extend((0..40).map(|o| hash_at(Q, o)));
    probes.extend((0..40u64).map(|i| hash_at(99, i)));

    let mut ranged = 0;
    let mut bounded = 0;
    let mut ranges: Vec<(u64, u64)> = Vec::new();
    for h in &probes {
        let whole = dedup::scan_seg_rows_for_hash(&rows, h, floor());
        match front.probe_plan(P, h, floor(), &mut ranges) {
            ProbeVerdict::Skip => {
                assert_eq!(whole, None, "the front skipped a committed in-window hash");
            }
            ProbeVerdict::Ranges => {
                ranged += 1;
                let frames = q
                    .committed_hashes_in_bands(P, &ranges, committed_end)
                    .expect("band read");
                assert_eq!(
                    min_in_window_offset(&frames, h, floor()),
                    whole,
                    "band read disagrees with the whole-window scan for bands {ranges:?}"
                );
                if frames.len() < rows.len() {
                    bounded += 1;
                }
            }
            ProbeVerdict::Whole => panic!("a seeded front never answers Whole"),
        }
    }
    assert!(
        ranged > 50,
        "the probes must exercise the band path ({ranged})"
    );
    assert!(
        bounded > 0,
        "a younger-generation band must read less than the whole window"
    );

    // The specials, spelled out.
    assert_eq!(
        dedup::scan_seg_rows_for_hash(&rows, &DUP, floor()),
        Some(130),
        "DUP: the out-of-window P:10 is ignored, the first in-window copy wins"
    );
    let v = front.probe_plan(P, &DUP, floor(), &mut ranges);
    assert_eq!(v, ProbeVerdict::Ranges);
    assert!(
        ranges.len() >= 2,
        "DUP sits in both generations: {ranges:?}"
    );
    let frames = q
        .committed_hashes_in_bands(P, &ranges, committed_end)
        .unwrap();
    assert_eq!(min_in_window_offset(&frames, &DUP, floor()), Some(130));
    let v = front.probe_plan(P, &TAIL, floor(), &mut ranges);
    assert_eq!(
        v,
        ProbeVerdict::Ranges,
        "the in-flight TAIL is in the front"
    );
    let frames = q
        .committed_hashes_in_bands(P, &ranges, committed_end)
        .unwrap();
    assert!(
        frames.iter().all(|f| f.end <= committed_end),
        "never an uncommitted record"
    );
    assert_eq!(min_in_window_offset(&frames, &TAIL, floor()), None);
    // A younger generation's band only: gen 2 starts mid-record 83 (the 4097th
    // in-window hash, offset 4196), so the band read stays far below the whole
    // window (records 83..=117).
    let v = front.probe_plan(P, &hash_at(P, 5000), floor(), &mut ranges);
    assert_eq!(v, ProbeVerdict::Ranges);
    let frames = q
        .committed_hashes_in_bands(P, &ranges, committed_end)
        .unwrap();
    assert_eq!(
        min_in_window_offset(&frames, &hash_at(P, 5000), floor()),
        Some(5000)
    );
    assert!(
        frames.len() < 40,
        "gen-2 band read {} records",
        frames.len()
    );

    // The band read returns EXACTLY the committed records overlapping the
    // bands — edge bands included (a band inside one record, touching a base,
    // one below a base, adjacent / overlapping / empty bands, past the tail).
    let whole: Vec<BandFrame> = q
        .committed_hashes_in_bands(P, &[(0, u64::MAX)], committed_end)
        .unwrap();
    assert_eq!(whole.len() as u64, COMMITTED_RECS);
    let cases: Vec<Vec<(u64, u64)>> = vec![
        vec![(0, 0)],
        vec![(10, 20)],
        vec![(49, 50)],
        vec![(50, 50)],
        vec![(51, 99)],
        vec![(100, 149), (150, 199)],
        vec![(300, 700), (500, 900), (2000, 2000)],
        vec![(4150, 5999)],
        vec![(5899, 5899)],
        vec![(5900, 9000)],
        vec![(7, 3)],
        vec![(u64::MAX, u64::MAX)],
        vec![(0, u64::MAX)],
    ];
    for bands in cases {
        let got: Vec<(u64, u64)> = q
            .committed_hashes_in_bands(P, &bands, committed_end)
            .unwrap()
            .iter()
            .map(|f| (f.base_offset, f.end))
            .collect();
        let want: Vec<(u64, u64)> = whole
            .iter()
            .filter(|f| overlaps(f, &bands))
            .map(|f| (f.base_offset, f.end))
            .collect();
        assert_eq!(got, want, "band set {bands:?}");
    }
    // The neighbour partition's copy of DUP never leaks into P's read, and Q's
    // own read finds it.
    let qf = q
        .committed_hashes_in_bands(Q, &[(0, 100)], RECS * 3)
        .unwrap();
    assert_eq!(min_in_window_offset(&qf, &DUP, floor()), Some(7));
}

// ---------------------------------------------------------------------------
// Hashes-only read == full decode
// ---------------------------------------------------------------------------

/// Every `(file_id, record)` of `pid`, sealed files then the active one.
fn all_records(q: &QLog, pid: u64) -> Vec<(u64, index::Record)> {
    let mut out = Vec::new();
    for (fid, view) in &q.sealed {
        out.extend(view.records_of(pid).into_iter().map(|r| (*fid, r)));
    }
    if let Some(fid) = q.active_index.file_id() {
        out.extend(
            q.active_index
                .records_of_from(pid, 0)
                .into_iter()
                .map(|r| (fid, r)),
        );
    }
    out
}

#[test]
fn hashes_only_read_equals_the_full_decode() {
    let td = TmpDir::new("hashes-only");
    let (mut q, _) = QLog::open(td.path(), 3, QLogOptions::testing(2048)).unwrap();
    let parts = [11u64, 12, 13];
    let mut base = 1u64; // a count-0 record at base 0 would index nothing
    for k in 0..24u64 {
        let count = (k % 4) as u32 * 7; // 0, 7, 14, 21 messages
        let h = record_hashes(9, base, count as u64);
        let payload = vec![k as u8; 300 + k as usize];
        let txn = (k % 5 == 3).then_some(TxnInput {
            gtid: 0xABCD + k as u128,
            participants: &parts,
        });
        q.append_group(&[RecordInput {
            seq: k + 1,
            pid: 9,
            base_offset: base,
            count,
            created_at_us: created(k),
            txn,
            hashes: &h,
            payload: &payload,
        }])
        .unwrap();
        base += count as u64 + (count == 0) as u64; // keep bases distinct
    }
    let recs = all_records(&q, 9);
    assert!(q.files().iter().any(|f| f.sealed), "the fixture must roll");
    assert!(
        recs.len() >= 18,
        "count-0 records at base>0 are indexed too"
    );
    let mut scratch = Vec::new();
    let mut txn_seen = 0;
    for (fid, rec) in &recs {
        let full = q.read_record(*fid, rec.offset).expect("full read");
        txn_seen += full.txn.is_some() as usize;
        let f = q.cache.file(*fid).unwrap();
        let fast = pread_hashes(&f, &q.dir, *fid, rec, &mut scratch).expect("hashes-only read");
        assert_eq!(
            &fast[..],
            &full.hashes[..],
            "record at {} of file {fid}",
            rec.offset
        );
        assert_eq!(fast.len(), rec.count as usize * 16);
        assert_eq!(q.read_record_hashes(*fid, rec).unwrap(), full.hashes);
    }
    assert!(txn_seen >= 4, "the txn-envelope fallback must be exercised");

    // A header that disagrees with its index entry is refused, not believed.
    let (fid, rec) = recs
        .iter()
        .find(|(_, r)| r.count > 0)
        .copied()
        .expect("a message record");
    let path = file_path(&q.dir, fid);
    let fh = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    fh.write_all_at(&777u64.to_le_bytes(), rec.offset + 20)
        .unwrap(); // the pid field
    let f = q.cache.file(fid).unwrap();
    let err = pread_hashes(&f, &q.dir, fid, &rec, &mut scratch).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{err}");
}

// ---------------------------------------------------------------------------
// The cross-cycle hash cache, through the reader
// ---------------------------------------------------------------------------

fn write_p(set: &mut QLogSet, qid: u64, from_rec: u64, to_rec: u64) {
    let payload = vec![0x55u8; 200];
    for k in from_rec..to_rec {
        let h = record_hashes(P, k * PER, PER);
        set.write_group_for_qid(
            qid,
            &[RecordInput {
                seq: k + 1,
                pid: P,
                base_offset: k * PER,
                count: PER as u32,
                created_at_us: created(k),
                txn: None,
                hashes: &h,
                payload: &payload,
            }],
        )
        .unwrap();
    }
}

#[test]
fn a_cache_hit_returns_identical_bytes() {
    let td = TmpDir::new("cache");
    let mut set = QLogSet::new(td.path().to_path_buf(), QLogOptions::testing(8 * 1024));
    let qid = QLogSet::queue_id_of("t", "q");
    write_p(&mut set, qid, 0, 30);
    let reader = set.reader();
    let log = set.log(qid).expect("log open");
    let all = [(0u64, u64::MAX)];
    let end1 = 30 * PER;

    let r1 = reader
        .committed_hashes_in_bands(qid, P, &all, end1)
        .unwrap();
    assert_eq!(r1.len(), 30);
    assert_eq!(
        log.read().unwrap().cache.cached_blocks(),
        30,
        "every miss was cached"
    );
    assert!(
        !log.read().unwrap().cache.fds.lock().unwrap().is_empty(),
        "the read fds were cached"
    );
    let r2 = reader
        .committed_hashes_in_bands(qid, P, &all, end1)
        .unwrap();
    assert_eq!(r1, r2, "identical frames and bytes");
    for (a, b) in r1.iter().zip(&r2) {
        assert!(
            Arc::ptr_eq(&a.hashes, &b.hashes),
            "record {} was re-read",
            a.base_offset
        );
        assert_eq!(&a.hashes[..], &record_hashes(P, a.base_offset, PER)[..]);
    }
    // The whole-window read serves the same bytes (from the same cache).
    let whole = reader.committed_frames(qid, P, 0, end1, true).unwrap();
    assert_eq!(whole.len(), r1.len());
    for (w, b) in whole.iter().zip(&r1) {
        assert_eq!(
            (w.base_offset, w.end, w.created_at_us),
            (b.base_offset, b.end, b.created_at_us)
        );
        assert_eq!(&w.hashes[..], &b.hashes[..]);
    }

    // A later commit: the new records are read (and cached), the old stay hits.
    write_p(&mut set, qid, 30, 36);
    let end2 = 36 * PER;
    let r3 = reader
        .committed_hashes_in_bands(qid, P, &all, end2)
        .unwrap();
    assert_eq!(r3.len(), 36);
    for (a, b) in r1.iter().zip(&r3) {
        assert!(Arc::ptr_eq(&a.hashes, &b.hashes));
    }
    assert_eq!(&r3[35].hashes[..], &record_hashes(P, 35 * PER, PER)[..]);
    // An uncommitted tail is neither returned nor cached.
    write_p(&mut set, qid, 36, 38);
    let r4 = reader
        .committed_hashes_in_bands(qid, P, &all, end2)
        .unwrap();
    assert_eq!(r4.len(), 36);
    assert_eq!(log.read().unwrap().cache.cached_blocks(), 36);

    // Retention drops whole sealed files and clears the cache; the survivors
    // still read back identically (re-read from disk).
    let dead = log
        .read()
        .unwrap()
        .files()
        .iter()
        .find(|f| f.sealed)
        .map(|f| f.id)
        .unwrap();
    let dropped = log
        .write()
        .unwrap()
        .unlink_dead_files(|m| m.id == dead)
        .unwrap();
    assert_eq!(dropped, 1);
    assert_eq!(log.read().unwrap().cache.cached_blocks(), 0);
    let r5 = reader
        .committed_hashes_in_bands(qid, P, &all, end2)
        .unwrap();
    assert!(
        !r5.is_empty() && r5.len() < 36,
        "the dropped file's records are gone"
    );
    for f in &r5 {
        assert_eq!(&f.hashes[..], &record_hashes(P, f.base_offset, PER)[..]);
    }
}

// ---------------------------------------------------------------------------
// End to end through the planner: DEDUP_INDEX=segment over the qlog
// ---------------------------------------------------------------------------

mod planner_e2e {
    use std::collections::HashMap;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Arc;

    use super::TmpDir;
    use crate::rsm::apply::{Applier, ApplyConfig, Committed as ApplyCommitted, NoNotify};
    use crate::rsm::dedup::{DedupFront, IndexMode};
    use crate::rsm::effect::QueueConfig;
    use crate::rsm::entry::{Entry, Outcome, PushVerdict};
    use crate::rsm::planner::{Overlay, Plan, PlanConfig, Planned, Planner, PushCommand, PushItem};
    use crate::rsm::qlog::set::QLogReader;
    use crate::rsm::segments;
    use crate::rsm::state::{Committed, Derived};
    use crate::rsm::store::{HeedStore, Store, StoreOpts, TypedReads};

    const BASE_US: i64 = 1_800_000_000_000_000;

    fn qcfg() -> QueueConfig {
        QueueConfig {
            id: [0xC1; 16],
            namespace: None,
            task: None,
            priority: 0,
            lease_time: 30,
            retry_limit: 3,
            retry_delay: 0,
            ttl: 0,
            dead_letter_queue: true,
            dlq_after_max_retries: true,
            delayed_processing: 0,
            window_buffer: 0,
            retention_seconds: 0,
            completed_retention_seconds: 0,
            retention_enabled: false,
            encryption_enabled: false,
            max_wait_time_seconds: 0,
            max_queue_size: 0,
            min_pop_wait_time: 0,
            dedup_window_seconds: 3600, // the auto-created default the fix targets
            retention_sink_hold: String::new(),
            retention_sink_hold_max_seconds: 0,
            created_at_us: BASE_US,
        }
    }

    fn push(id: u64, txns: &[String]) -> PushCommand {
        let mut request_id = [0u8; 16];
        request_id[..8].copy_from_slice(&id.to_be_bytes());
        request_id[8] = 0x3B;
        PushCommand {
            request_id,
            tenant: "t1".to_string(),
            queue: "orders".to_string(),
            partition: "p0".to_string(),
            items: txns
                .iter()
                .map(|t| PushItem {
                    hash: crate::util::txn_hash128(t),
                    frame: format!("{{\"t\":\"{t}\"}}").into_bytes(),
                })
                .collect(),
            create_cfg: qcfg(),
        }
    }

    /// One planning cycle over the committed store, `DEDUP_INDEX=segment` with
    /// the qlog as the committed dedup authority (the batcher's wiring).
    fn plan(
        store: &HeedStore,
        front: &DedupFront,
        ql: &QLogReader,
        wall: i64,
        cmds: &[PushCommand],
    ) -> (Vec<Planned>, Option<Entry>) {
        store
            .read(|r| {
                let d0 = Derived::default();
                let now = Committed::new(r, &d0).plan_now(wall)?;
                let d = Derived::rebuild(r, now)?;
                let committed = Committed::new(r, &d);
                let mut ov = Overlay::new(r.next_pid()?, r.kv_version_next()?);
                ov.mark_cycle_start();
                let cfg = PlanConfig {
                    index_mode: IndexMode::Segment,
                    ..PlanConfig::default()
                };
                let mut planner = Planner::new(committed, now, cfg, front, None);
                planner.set_qlog_reader(Some(ql.clone()));
                let results: Vec<Planned> =
                    cmds.iter().map(|c| planner.plan_push(&mut ov, c)).collect();
                let mut entry = Entry::new(now, ov.cycle_pid_base(), ov.cycle_kv_base());
                let mut any = false;
                for (c, res) in cmds.iter().zip(&results) {
                    if let Ok(Plan::Logged { effects, outcome }) = res {
                        entry
                            .add_command(c.request_id, outcome.clone(), effects.clone())
                            .expect("add command");
                        any = true;
                    }
                }
                Ok((results, any.then_some(entry)))
            })
            .expect("plan read")
    }

    fn verdicts(p: &Planned) -> Vec<PushVerdict> {
        match p {
            Ok(Plan::Logged {
                outcome: Outcome::Push(o),
                ..
            })
            | Ok(Plan::Empty(Outcome::Push(o))) => o.items.clone(),
            other => panic!("push not decided: {other:?}"),
        }
    }

    #[test]
    fn planner_band_probe_equals_the_whole_window_probe() {
        let td = TmpDir::new("planner");
        let store = HeedStore::open(
            &td.path().join("store"),
            &StoreOpts {
                map_bytes: Some(256 << 20),
                ..Default::default()
            },
        )
        .expect("open store");
        let seg = segments::Options {
            segment_bytes: 64 << 10,
            fsync: segments::FsyncMode::Data,
            fsync_threads: 1,
            nbuckets: segments::NBUCKETS,
        };
        let acfg = ApplyConfig {
            qlog: true,
            qlog_writer_external: false,
            ..ApplyConfig::default()
        };
        let (mut a, _) = Applier::open(
            &store,
            &td.path().join("seg"),
            seg,
            acfg,
            Arc::new(NoNotify),
        )
        .expect("open applier");
        let ql = a.qlog_reader().expect("qlog on");
        let front = DedupFront::new(true, 64 << 20);

        // 48 committed pushes of 100 fresh messages: 4800 hashes, so the front
        // (born seeded at the partition's creation) holds two generations.
        let mut wall = BASE_US;
        let mut offset_of: HashMap<String, u64> = HashMap::new();
        for c in 0..48u64 {
            let txns: Vec<String> = (0..100).map(|i| format!("m{c}-{i}")).collect();
            let (res, entry) = plan(&store, &front, &ql, wall, &[push(c + 1, &txns)]);
            for (t, v) in txns.iter().zip(verdicts(&res[0])) {
                match v {
                    PushVerdict::Created { offset, .. } => {
                        offset_of.insert(t.clone(), offset);
                    }
                    other => panic!("fresh message {t} was not created: {other:?}"),
                }
            }
            a.apply(&ApplyCommitted {
                index: c + 1,
                term: 1,
                entry: entry.expect("a logged push"),
            })
            .expect("apply");
            a.durable_point().expect("durable point");
            wall += 1_000;
        }

        // Duplicates from old (gen 1) and young (gen 2) records, one twice, a
        // fresh message, and an in-cycle duplicate the OVERLAY must catch.
        let dups = [
            "m0-5", "m3-99", "m20-0", "m40-7", "m47-99", "m47-99", "m10-10",
        ];
        let mut probe: Vec<String> = dups.iter().map(|s| s.to_string()).collect();
        probe.push("fresh-1".to_string());
        let cmds = [
            push(1001, &["fresh-2".to_string()]),
            push(1002, &probe),
            push(1003, &["fresh-2".to_string(), "m5-5".to_string()]),
        ];

        let b0 = crate::rsm::dbgctr::C.push_dedup_build.load(Relaxed);
        let (band, _) = plan(&store, &front, &ql, wall, &cmds);
        let b1 = crate::rsm::dbgctr::C.push_dedup_build.load(Relaxed);
        assert!(
            b1 - b0 >= 8,
            "every committed duplicate must take the band path ({} reads)",
            b1 - b0
        );
        // The same committed state through the whole-window path (a disabled
        // front answers Whole for every hash).
        let disabled = DedupFront::disabled();
        let (whole, _) = plan(&store, &disabled, &ql, wall, &cmds);
        for i in 0..cmds.len() {
            assert_eq!(verdicts(&band[i]), verdicts(&whole[i]), "command {i}");
        }

        let v = verdicts(&band[1]);
        for (k, t) in dups.iter().enumerate() {
            match &v[k] {
                PushVerdict::Duplicate { offset, .. } => {
                    assert_eq!(
                        *offset, offset_of[*t],
                        "{t} must resolve to its original offset"
                    )
                }
                other => panic!("{t} was not caught as a duplicate: {other:?}"),
            }
        }
        assert!(
            matches!(v[dups.len()], PushVerdict::Created { .. }),
            "fresh-1 is new"
        );
        let fresh2 = match &verdicts(&band[0])[0] {
            PushVerdict::Created { offset, .. } => *offset,
            other => panic!("fresh-2 was not created: {other:?}"),
        };
        let v3 = verdicts(&band[2]);
        assert!(
            matches!(v3[0], PushVerdict::Duplicate { offset, .. } if offset == fresh2),
            "the overlay catches a duplicate of an in-flight append: {:?}",
            v3[0]
        );
        assert!(
            matches!(v3[1], PushVerdict::Duplicate { offset, .. } if offset == offset_of["m5-5"]),
            "{:?}",
            v3[1]
        );
    }
}
