//! WP-1.6a — the consensus seam (§12.1), its `LocalReplicator` (§12.2), the
//! local write-ahead log, and a `FakeReplicator` for the branches a single
//! real node never produces.
//!
//! What this file proves:
//!
//! - **the log** ([`log_appends_and_scans_round_trip`], [`log_rolls_across_files`],
//!   [`log_truncates_a_torn_tail`], [`log_refuses_a_corrupt_sealed_file`],
//!   [`log_drops_files_behind_the_durable_point`]): append → scan is exact
//!   across rolls; a torn or checksum-failing tail is truncated at reopen and
//!   the acknowledged frames below it are intact; a break in a SEALED file is
//!   refused (it is an acknowledged entry, not a tail); files below a durable
//!   point are dropped, the active file and uncovered files are kept.
//! - **the replicator drives the real apply thread**
//!   ([`propose_applies_through_the_real_apply_thread`]): the state the
//!   proposals build is byte-equal to WP-1.4's own `run_workload` of the same
//!   entries; a clean reopen reproduces it ([`a_clean_reopen_reproduces_the_state`]).
//! - **the log leads the durable store** ([`the_log_leads_the_durable_store`]):
//!   entries are acknowledged (applied) while the store commit that holds them
//!   is not yet durable — the WAL property the whole design rests on, and the
//!   precondition the crash test then falsifies.
//! - **the fake's branches** ([`fake_commits`], [`fake_fails_and_not_leader`],
//!   [`fake_times_out_and_keeps_the_entry_in_flight`], [`fake_role_gates_propose`]):
//!   the §7.1 error semantics WP-1.6b's batcher is written against.
//!
//! The kill -9 half — a torn tail and acknowledged entries surviving a crash —
//! is [`super::replicator_crash`]. The throughput smoke
//! ([`local_log_throughput_smoke`]) is `#[ignore]`: laptop numbers are smoke
//! only (§0.3), the numbers to quote come from the VM.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::rsm::apply::{self, state_digest, SystemClock};
use crate::rsm::entry::encode_entry;
use crate::rsm::qlog::set::QLogSet;
use crate::rsm::qlog::QLogOptions;
use crate::rsm::replicator::fake::{FakeReplicator, Step};
use crate::rsm::replicator::local::{LocalReplicator, NoWaker, OpenConfig};
use crate::rsm::replicator::log::{Fsync, LogOptions, LogStore, LOG_TERM};
use crate::rsm::replicator::{AppliedAt, ProposeError, Replicator, Role};
use crate::rsm::segments::FsyncMode;
use crate::rsm::store::{HeedStore, Store, TypedReads};

use super::apply::{cfg, run_workload, seg_opts, settle, store_opts, Node, Workload};

static SEQ: AtomicU64 = AtomicU64::new(0);

const SEED: u64 = 0x1_6A00_0000_0001;

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-repl-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

// ---------------------------------------------------------------------------
// The log on its own
// ---------------------------------------------------------------------------

fn open_log(dir: &Path, segment_bytes: u64) -> LogStore {
    LogStore::open(dir, LogOptions::testing(segment_bytes))
        .expect("open log")
        .0
}

/// A run of entry bodies, deterministic and of varied length.
fn bodies(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let len = 8 + (i * 37) % 400;
            (0..len).map(|b| (b as u64 ^ i as u64) as u8).collect()
        })
        .collect()
}

#[test]
fn log_appends_and_scans_round_trip() {
    let dir = scratch("log-rt");
    let bodies = bodies(200);
    {
        let mut log = open_log(&dir, 64 << 10);
        // Append in a few groups of different sizes.
        for chunk in bodies.chunks(7) {
            let slices: Vec<&[u8]> = chunk.iter().map(Vec::as_slice).collect();
            log.append_group(&slices).expect("append");
        }
        assert_eq!(log.last_index(), bodies.len() as u64);
    }
    // Reopen and scan every frame from index 1.
    let log = open_log(&dir, 64 << 10);
    let mut seen: Vec<(u64, Vec<u8>)> = Vec::new();
    log.scan_from(1, &mut |index, term, body| {
        assert_eq!(term, 1);
        seen.push((index, body.to_vec()));
        Ok(())
    })
    .expect("scan");
    assert_eq!(seen.len(), bodies.len());
    for (i, (index, body)) in seen.iter().enumerate() {
        assert_eq!(*index, i as u64 + 1);
        assert_eq!(body, &bodies[i]);
    }
    // A scan from the middle returns exactly the tail.
    let mut from_mid = 0u64;
    let log = open_log(&dir, 64 << 10);
    log.scan_from(101, &mut |index, _t, _b| {
        assert!(index >= 101);
        from_mid += 1;
        Ok(())
    })
    .expect("scan mid");
    assert_eq!(from_mid, 100);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn log_rolls_across_files() {
    let dir = scratch("log-roll");
    let bodies = bodies(300);
    {
        let mut log = open_log(&dir, 2 << 10); // tiny: many files
        for b in &bodies {
            log.append_group(&[b.as_slice()]).expect("append");
        }
        assert!(
            log.file_count() > 3,
            "expected several files, got {}",
            log.file_count()
        );
    }
    // Every frame survives the rolls, in order, after reopen.
    let log = open_log(&dir, 2 << 10);
    assert_eq!(log.last_index(), bodies.len() as u64);
    let mut n = 0u64;
    log.scan_from(1, &mut |index, _t, body| {
        n += 1;
        assert_eq!(index, n);
        assert_eq!(body, bodies[(n - 1) as usize].as_slice());
        Ok(())
    })
    .expect("scan");
    assert_eq!(n, bodies.len() as u64);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Find the highest-numbered `.qlog` file (the active one).
fn active_log_file(dir: &Path) -> PathBuf {
    let mut best: Option<(u64, PathBuf)> = None;
    for e in std::fs::read_dir(dir).unwrap() {
        let e = e.unwrap();
        let name = e.file_name().to_string_lossy().into_owned();
        if let Some(num) = name.strip_prefix('r').and_then(|r| r.strip_suffix(".qlog")) {
            if let Ok(id) = num.parse::<u64>() {
                if best.as_ref().map(|(b, _)| id > *b).unwrap_or(true) {
                    best = Some((id, e.path()));
                }
            }
        }
    }
    best.expect("a log file").1
}

#[test]
fn log_truncates_a_torn_tail() {
    let dir = scratch("log-torn");
    let bodies = bodies(20);
    {
        let mut log = open_log(&dir, 1 << 20);
        for b in &bodies {
            log.append_group(&[b.as_slice()]).expect("append");
        }
        assert_eq!(log.last_index(), 20);
    }
    // A process died mid-append: a partial frame is glued to the end of the
    // active file (a length prefix and a few bytes, no body, no valid xxh3).
    {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(active_log_file(&dir))
            .unwrap();
        f.write_all(&[0x40, 0, 0, 0]).unwrap(); // a `len` of 64
        f.write_all(&[0xAB; 12]).unwrap(); // a stub of the rest, then nothing
        f.sync_all().unwrap();
    }
    // Reopen: the torn tail is truncated, the 20 acknowledged frames remain.
    let (log, rec) = LogStore::open(&dir, LogOptions::testing(1 << 20)).expect("reopen");
    assert!(
        rec.truncated_tail,
        "the torn tail should have been truncated"
    );
    assert_eq!(log.last_index(), 20, "no acknowledged frame is lost");
    let mut n = 0u64;
    log.scan_from(1, &mut |_i, _t, _b| {
        n += 1;
        Ok(())
    })
    .expect("scan");
    assert_eq!(n, 20);
    // And appending resumes cleanly at 21.
    let mut log = log;
    let first = log
        .append_group(&[b"twenty-one".as_slice()])
        .expect("append");
    assert_eq!(first, 21);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn log_truncates_a_checksum_failing_tail() {
    let dir = scratch("log-badsum");
    let bodies = bodies(10);
    let good_len;
    {
        let mut log = open_log(&dir, 1 << 20);
        for b in &bodies {
            log.append_group(&[b.as_slice()]).expect("append");
        }
        good_len = std::fs::metadata(active_log_file(&dir)).unwrap().len();
    }
    // Append a COMPLETE frame whose body's last byte is wrong for its xxh3:
    // encode a valid frame, then flip a body byte.
    {
        let mut buf = Vec::new();
        // len=4, then a fake xxh3, index=11, term=1, body 4 bytes.
        let body = [1u8, 2, 3, 4];
        buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes()); // deliberately wrong checksum
        buf.extend_from_slice(&11u64.to_le_bytes());
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&body);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(active_log_file(&dir))
            .unwrap();
        f.write_all(&buf).unwrap();
        f.sync_all().unwrap();
    }
    let (log, rec) = LogStore::open(&dir, LogOptions::testing(1 << 20)).expect("reopen");
    assert!(rec.truncated_tail);
    assert_eq!(log.last_index(), 10);
    assert_eq!(
        std::fs::metadata(active_log_file(&dir)).unwrap().len(),
        good_len,
        "the file was truncated back to the last valid frame",
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn log_refuses_a_corrupt_sealed_file() {
    let dir = scratch("log-sealed");
    {
        let mut log = open_log(&dir, 2 << 10); // small: forces a seal
        for b in &bodies(300) {
            log.append_group(&[b.as_slice()]).expect("append");
        }
        assert!(log.file_count() > 2);
    }
    // Corrupt a frame body inside the FIRST (sealed) file. A break there is
    // corruption of an acknowledged entry, not a torn tail: recovery must
    // refuse, never truncate a sealed file (§11.5 / I11 shape for the log).
    let first = {
        let mut ids: Vec<u64> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| {
                let n = e.unwrap().file_name().to_string_lossy().into_owned();
                n.strip_prefix('r')
                    .and_then(|r| r.strip_suffix(".qlog"))
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .collect();
        ids.sort_unstable();
        dir.join(format!("r{:08}.qlog", ids[0]))
    };
    {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&first)
            .unwrap();
        // Flip a byte well past the header, inside some frame's body.
        f.write_at(&[0xFF], 60).unwrap();
        f.sync_all().unwrap();
    }
    let err = LogStore::open(&dir, LogOptions::testing(2 << 10));
    assert!(err.is_err(), "a corrupt sealed file must be refused");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn log_drops_files_behind_the_durable_point() {
    let dir = scratch("log-drop");
    let mut log = open_log(&dir, 2 << 10);
    for b in &bodies(300) {
        log.append_group(&[b.as_slice()]).expect("append");
    }
    let before = log.file_count();
    assert!(before > 4);
    let last = log.last_index();

    // A durable point at index 0 drops nothing.
    assert_eq!(log.truncate_through(0).expect("truncate 0"), 0);
    assert_eq!(log.file_count(), before);

    // A durable point halfway drops the sealed files fully below it, keeps the
    // one that straddles it and the active file.
    let dropped = log.truncate_through(last / 2).expect("truncate half");
    assert!(dropped > 0, "some sealed files should drop");
    assert!(log.file_count() < before);

    // Every remaining frame from `last/2 + 1` is still readable: nothing above
    // the durable point was dropped.
    let mut n = 0u64;
    log.scan_from(last / 2 + 1, &mut |index, _t, _b| {
        assert!(index > last / 2);
        n += 1;
        Ok(())
    })
    .expect("scan");
    assert_eq!(n, last - (last / 2));
    // The active file is never dropped, so appends continue.
    let first = log.append_group(&[b"after".as_slice()]).expect("append");
    assert_eq!(first, last + 1);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// The replicator over the real apply thread
// ---------------------------------------------------------------------------

fn test_config(dir: &Path, log_segment: u64, fsync: Fsync) -> OpenConfig {
    OpenConfig {
        node_id: 1,
        log_dir: dir.join("log"),
        log_opts: LogOptions {
            segment_bytes: log_segment,
            fsync,
        },
        seg_root: dir.join("seg"),
        seg_opts: seg_opts(),
        apply_cfg: cfg(),
        apply_channel_capacity: 64,
        replay_deadline: Duration::from_secs(30),
        writer_pipeline: crate::rsm::replicator::local::writer_pipeline_from_env(),
    }
}

fn open_repl(dir: &Path, store: Arc<HeedStore>) -> LocalReplicator<HeedStore> {
    LocalReplicator::open(
        store,
        test_config(dir, 16 << 10, Fsync::Off),
        Arc::new(NoWaker),
        Arc::new(SystemClock),
    )
    .expect("open replicator")
}

/// Open a replicator with the write/fsync pipeline forced on or off, over a
/// REAL `Data` barrier (so the syncer's fsync actually takes time and the
/// writer forms the next group during it — PERF-G).
fn open_repl_wp(
    dir: &Path,
    store: Arc<HeedStore>,
    writer_pipeline: bool,
) -> LocalReplicator<HeedStore> {
    let cfg = OpenConfig {
        writer_pipeline,
        ..test_config(dir, 16 << 10, Fsync::Data)
    };
    LocalReplicator::open(store, cfg, Arc::new(NoWaker), Arc::new(SystemClock))
        .expect("open replicator")
}

fn entry_bytes(n: u64) -> Vec<Bytes> {
    let mut w = Workload::new(SEED);
    (0..n)
        .map(|_| Bytes::from(encode_entry(&w.next().entry).expect("encode")))
        .collect()
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

/// Read the replicated digest of a store, then close it so the path can be
/// reopened (heed refuses a second open in one process).
fn digest_and_close(store: Arc<HeedStore>) -> apply::StateDigest {
    let store = Arc::try_unwrap(store)
        .unwrap_or_else(|_| panic!("the store is still shared after shutdown"));
    let d = store
        .read(|r| Ok(state_digest(r).expect("digest")))
        .expect("read");
    store.close();
    d
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn propose_applies_through_the_real_apply_thread() {
    const N: u64 = 80;
    // The reference: WP-1.4's own applier over the same entries.
    let want = {
        let node = Node::new("repl-ref");
        run_workload(&node, SEED, N, 97)
    };

    let dir = scratch("repl-apply");
    let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
    let repl = open_repl(&dir, store);

    let entries = entry_bytes(N);
    for (i, e) in entries.into_iter().enumerate() {
        let at = repl.propose(e, deadline()).await.expect("propose");
        assert_eq!(at.index, i as u64 + 1, "indices are assigned in order");
        assert_eq!(at.term, 1);
    }
    assert_eq!(repl.applied_index(), N);
    assert_eq!(repl.metrics().proposals, N);
    // read_barrier on a single node is the applied index, no round trip.
    assert_eq!(repl.read_barrier(deadline()).await.expect("barrier"), N);

    let (_stats, store) = repl.shutdown().expect("shutdown");
    let got = digest_and_close(store);
    assert_eq!(
        got.whole,
        want.whole,
        "the proposed state differs from run_workload, first at {:?}",
        got.first_difference(&want),
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Open a replicator with `QUEEN_RAFT_QLOG` ON (Phase A2), everything else the
/// shared test config.
fn open_repl_qlog(dir: &Path, store: Arc<HeedStore>) -> LocalReplicator<HeedStore> {
    let ocfg = OpenConfig {
        apply_cfg: apply::ApplyConfig {
            qlog: true,
            ..cfg()
        },
        ..test_config(dir, 16 << 10, Fsync::Off)
    };
    LocalReplicator::open(store, ocfg, Arc::new(NoWaker), Arc::new(SystemClock))
        .expect("open replicator (qlog on)")
}

/// Phase A2: the full replicator + apply path with the qlog knob ON. Proves the
/// apply thread PUBLISHES the qlog reader (the `OnceLock` spin-wait in
/// `LocalReplicator::open` completes rather than hanging), the
/// flush-before-store-commit ordering runs on every commit and durable point,
/// and the resulting REPLICATED state is byte-identical to the reference — the
/// shadow perturbs nothing, proven through the real threads, not only the unit
/// applier. The qlog READ equality itself is `qlog_read.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qlog_on_drives_the_real_apply_thread_and_publishes_a_reader() {
    const N: u64 = 80;
    // The reference: the same entries with the knob OFF (today's exact path).
    let want = {
        let node = Node::new("repl-qlog-ref");
        run_workload(&node, SEED, N, 97)
    };

    let dir = scratch("repl-qlog");
    let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
    let repl = open_repl_qlog(&dir, store);
    // The apply thread published a qlog reader (knob on). `open` returning at all
    // means the spin-wait succeeded; this asserts it is actually `Some`.
    assert!(
        repl.qlog_reader().is_some(),
        "the qlog reader was not published with the knob on"
    );

    let entries = entry_bytes(N);
    for (i, e) in entries.into_iter().enumerate() {
        let at = repl.propose(e, deadline()).await.expect("propose");
        assert_eq!(at.index, i as u64 + 1, "indices are assigned in order");
    }
    assert_eq!(repl.applied_index(), N);

    let (_stats, store) = repl.shutdown().expect("shutdown");
    let got = digest_and_close(store);
    assert_eq!(
        got.whole,
        want.whole,
        "qlog-on replicated state differs from the knob-off reference, first at {:?}",
        got.first_difference(&want),
    );
    // The shadow really was written, so the equality above is not vacuous.
    assert!(
        dir.join("qlog").exists(),
        "the knob was on but no qlog/ directory was created"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// One valid entry that creates a partition and appends `blob` to it, framed for
/// the replicator (A3b payload-once proof). Bypasses the batcher — it is raw
/// bytes the writer accepts — so `blob` is exactly what reaches the qlog.
fn magic_push_entry(blob: &[u8]) -> Bytes {
    use crate::rsm::effect::Effect;
    use crate::rsm::entry::{Entry, Outcome};
    let now = 1_000_000i64;
    // A fresh store's counters are 1-based (pid_base = kv_version_base = 1); this
    // entry assigns pid 1 to the partition and no KV version.
    let mut e = Entry::new(now, 1, 1);
    // Create the partition (pid = pid_base + 0 = 1).
    e.add_command(
        super::samples::uuid(1),
        Outcome::Empty,
        vec![Effect::PartitionCreate {
            pid: 1,
            uuid: super::samples::uuid(2),
            tenant: "t".into(),
            queue: "q".into(),
            partition: "p".into(),
            created_at_us: now,
        }],
    )
    .unwrap();
    // Append the distinctive payload (count 1, one 16-byte hash).
    e.add_command(
        super::samples::uuid(3),
        Outcome::Empty,
        vec![Effect::Append {
            pid: 1,
            bucket: 0,
            base_offset: 0,
            count: 1,
            created_at_us: now,
            hashes: super::samples::uuid(9).to_vec(),
            blob: blob.to_vec(),
        }],
    )
    .unwrap();
    Bytes::from(encode_entry(&e).unwrap())
}

/// True if any file under `dir` (recursively) contains `needle` as a byte run.
fn dir_contains(dir: &Path, needle: &[u8]) -> bool {
    if !dir.exists() {
        return false;
    }
    for entry in std::fs::read_dir(dir).expect("read_dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            if dir_contains(&path, needle) {
                return true;
            }
        } else {
            let bytes = std::fs::read(&path).expect("read file");
            if bytes.windows(needle.len()).any(|w| w == needle) {
                return true;
            }
        }
    }
    false
}

/// A3b (`ALICE_PGLESS_NEWARCH.md` §5, the double-write kill): with the qlog knob
/// ON the payload is written ONCE — to its queue's qlog, by the writer, before
/// the referencing raft-log entry — so the raft log no longer carries it. This
/// proves it at the BYTE level: a distinctive payload is ABSENT from the raft log
/// (`log/`) and PRESENT in the qlog (`qlog/`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_payload_lives_once_in_the_qlog_not_the_raft_log() {
    // 4 KiB of a magic run that cannot occur by chance in the entry metadata.
    let magic: Vec<u8> =
        std::iter::repeat_n([0xDEu8, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE], 512)
            .flatten()
            .collect();
    let dir = scratch("repl-payload-once");
    {
        let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
        let repl = open_repl_qlog(&dir, store);
        repl.propose(magic_push_entry(&magic), deadline())
            .await
            .expect("propose");
        assert_eq!(repl.applied_index(), 1);
        let (_stats, store) = repl.shutdown().expect("shutdown");
        Arc::try_unwrap(store)
            .unwrap_or_else(|_| panic!("store still shared"))
            .close();
    }
    // The raft log entry is PAYLOAD-FREE: the magic is not in `log/`.
    assert!(
        !dir_contains(&dir.join("log"), &magic),
        "the payload is in the RAFT LOG — the double-write was NOT killed"
    );
    // The payload lives ONCE, in the qlog: the magic IS in `qlog/`.
    assert!(
        dir_contains(&dir.join("qlog"), &magic),
        "the payload is not in the qlog — the writer did not write it there"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A3b (`ALICE_PGLESS_NEWARCH.md` §5): the replicated digest is REPLAY-STABLE
/// with the qlog on — the crash-recovery replay of the payload-free entries
/// reproduces the live digest BYTE-FOR-BYTE, including `RetainedBytes`. This is
/// the I2 fix: the reference entry carries the payload's frame LENGTH (not the
/// payload), so apply computes `RetainedBytes` from the same value whether an
/// `Append` is applied live (the writer hands apply the length-carrying form) or
/// replayed from the log. Before the fix this test FAILS at "counters" — live
/// computed `RetainedBytes` from the real payload, replay from an empty blob.
///
/// Phase C: with the qlog on the queue logs are the ONLY write-ahead log (the
/// raft `log/` is no longer written), so the replay reads the entry records
/// from them, exactly as `LocalReplicator::open` recovers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_replicated_digest_is_replay_stable_with_the_qlog_on() {
    const N: u64 = 60;
    let dir = scratch("repl-replay-stable-live");

    // 1. LIVE: run the workload knob-on through the real writer + apply, take a
    //    durable point (shutdown), and read the replicated digest.
    let live = {
        let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
        let repl = open_repl_qlog(&dir, store);
        for e in entry_bytes(N) {
            repl.propose(e, deadline()).await.expect("propose");
        }
        let (_stats, store) = repl.shutdown().expect("shutdown");
        digest_and_close(store)
    };

    // 2. REPLAY: read the PAYLOAD-FREE entry records the live run wrote into the
    //    QUEUE LOGS — the WAL on the qlog path — the way `LocalReplicator::open`
    //    does (`reopen_all`, then `scan_entries` from the store's durable index
    //    + 1), and apply them into a FRESH applier configured for recovery (qlog
    //    on + writer-external), simulating a crash restart onto an empty store.
    //    The payload lives only in the qlog; these entries carry the frame
    //    LENGTH, so apply must derive `RetainedBytes` from it exactly as the
    //    live path did.
    let replay_dir = scratch("repl-replay-stable-fresh");
    let recovered = {
        let store = Arc::new(
            HeedStore::open(&replay_dir.join("store"), &store_opts()).expect("fresh store"),
        );
        let from = store
            .read(|r| r.durable_index())
            .expect("read durable index")
            + 1;
        assert_eq!(from, 1, "a fresh store replays the whole history");

        // Options mirror the segment writer's, as `LocalReplicator::open` sets
        // them.
        let so = seg_opts();
        let mut qlogs = QLogSet::new(
            dir.join("qlog"),
            QLogOptions {
                segment_bytes: so.segment_bytes,
                fsync: match so.fsync {
                    FsyncMode::Full => crate::rsm::qlog::Fsync::Full,
                    FsyncMode::Data => crate::rsm::qlog::Fsync::Data,
                },
            },
        );
        qlogs.reopen_all().expect("reopen the queue logs");
        let mut entries: Vec<(u64, crate::rsm::entry::Entry)> = Vec::new();
        let scan = qlogs
            .scan_entries(from, &mut |rec| {
                entries.push((
                    rec.seq,
                    crate::rsm::entry::decode_entry(&rec.entry).expect("decode replayed entry"),
                ));
                Ok(())
            })
            .expect("scan the queue logs");
        assert_eq!(
            (scan.delivered, scan.next_seq, scan.stopped.as_deref()),
            (N, N + 1, None),
            "every proposed entry is replayable from the queue logs, gapless and complete"
        );
        drop(qlogs);

        {
            let recovery_cfg = apply::ApplyConfig {
                qlog: true,
                qlog_writer_external: true,
                ..cfg()
            };
            let (mut a, _rec) = apply::Applier::open(
                &*store,
                &replay_dir.join("seg"),
                seg_opts(),
                recovery_cfg,
                Arc::new(apply::NoNotify),
            )
            .expect("open recovery applier");
            // The queue logs carry no term: `LocalReplicator` replays at its
            // single one, as it proposed.
            for (index, entry) in entries {
                a.apply(&apply::Committed {
                    index,
                    term: LOG_TERM,
                    entry,
                })
                .expect("apply replayed entry");
            }
            settle(&mut a);
        }
        digest_and_close(store)
    };

    assert_eq!(
        recovered.whole,
        live.whole,
        "the crash-recovered digest DRIFTS from the live digest, first at {:?} \
         — RetainedBytes must be computed from the frame length on both paths (I2)",
        recovered.first_difference(&live),
    );
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&replay_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clean_reopen_reproduces_the_state() {
    const N: u64 = 60;
    let want = {
        let node = Node::new("repl-reopen-ref");
        run_workload(&node, SEED, N, 97)
    };

    let dir = scratch("repl-reopen");

    // Run 1: propose, then a graceful shutdown (which takes a durable point).
    {
        let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
        let repl = open_repl(&dir, store);
        for e in entry_bytes(N) {
            repl.propose(e, deadline()).await.expect("propose");
        }
        let (_s, store) = repl.shutdown().expect("shutdown");
        let d = digest_and_close(store);
        assert_eq!(d.whole, want.whole);
    }

    // Run 2: reopen the SAME data directory. The replicator recovers the log,
    // replays after the store's durable index (nothing, after a clean stop),
    // and comes up at the same state — no proposals.
    {
        let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("reopen"));
        let repl = open_repl(&dir, store);
        assert_eq!(
            repl.applied_index(),
            N,
            "reopened at the same applied index"
        );
        let (_s, store) = repl.shutdown().expect("shutdown");
        let got = digest_and_close(store);
        assert_eq!(
            got.whole,
            want.whole,
            "the reopened state differs, first at {:?}",
            got.first_difference(&want),
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// PERF-G: the write/fsync pipeline (`QUEEN_RAFT_WRITER_PIPELINE`) is a pure
/// scheduling change — an entry is acknowledged only after its own group's
/// fsync (I4), and the syncer hands groups to apply in index order — so the
/// committed state it produces is byte-identical to the inline writer's (I2),
/// over a real `Data` barrier that opens the overlap window. This runs the SAME
/// entries through both writers and asserts identical digests, and that both
/// match WP-1.4's reference applier.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_write_fsync_pipeline_reproduces_the_inline_writers_state() {
    const N: u64 = 120;
    let want = {
        let node = Node::new("repl-wp-ref");
        run_workload(&node, SEED, N, 97)
    };

    async fn digest_for(tag: &str, n: u64, writer_pipeline: bool) -> apply::StateDigest {
        let dir = scratch(tag);
        let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
        let repl = open_repl_wp(&dir, store, writer_pipeline);
        for (i, e) in entry_bytes(n).into_iter().enumerate() {
            let at = repl.propose(e, deadline()).await.expect("propose");
            assert_eq!(at.index, i as u64 + 1, "{tag}: indices assigned in order");
        }
        assert_eq!(repl.applied_index(), n, "{tag}: every propose applied");
        let (_s, store) = repl.shutdown().expect("shutdown");
        let d = digest_and_close(store);
        let _ = std::fs::remove_dir_all(&dir);
        d
    }

    let pipelined = digest_for("repl-wp-on", N, true).await;
    let inline = digest_for("repl-wp-off", N, false).await;
    assert_eq!(
        pipelined.whole,
        inline.whole,
        "the pipeline and inline writers must produce identical state, first at {:?}",
        pipelined.first_difference(&inline),
    );
    assert_eq!(
        pipelined.whole,
        want.whole,
        "the pipeline writer must match the reference applier, first at {:?}",
        pipelined.first_difference(&want),
    );
}

/// PERF-G: the write/fsync pipeline under GENUINE concurrent load — many
/// proposals in flight AT ONCE, so the writer forms and writes group N+1 while
/// the syncer is still fsyncing group N. This is the overlap the pipeline
/// exists for, and the one
/// [`the_write_fsync_pipeline_reproduces_the_inline_writers_state`] does NOT
/// exercise: it awaits each propose, so only one entry is ever in flight and
/// every group holds exactly one entry — the syncer never has a next group to
/// build. Here all N proposals are enqueued before any is answered (propose
/// enqueues on the writer channel before it awaits), so the writer batches them
/// into multi-entry groups and the syncer's fsync of group N overlaps the
/// writer forming group N+1.
///
/// The futures are polled in submission order on ONE task, so the writer (which
/// reads its channel FIFO) still assigns indices 1..=N in that order — the
/// committed log is the natural order, only the group boundaries and the
/// write/fsync overlap change. Over a real `Data` barrier this asserts the
/// overlap is I4/I2-safe: every proposal is answered exactly once with a
/// gapless index, and the committed state is byte-identical to WP-1.4's
/// reference applier — which it would NOT be if the syncer ever handed a later
/// group to apply ahead of an earlier one, or dropped/duplicated a group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_write_fsync_pipeline_holds_under_concurrent_in_flight_groups() {
    use std::future::Future;
    const N: u64 = 200;

    // The reference: WP-1.4's own applier over the same entries, natural order.
    let want = {
        let node = Node::new("repl-wp-conc-ref");
        run_workload(&node, SEED, N, 97)
    };

    // Pipelined writer, WP on, real Data fsync so the syncer's barrier actually
    // takes time and the overlap window is real. Small segments (16 KiB) roll
    // mid-run, exercising the deferred-append dup-fd across a roll too.
    let dir = scratch("repl-wp-conc");
    let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
    let repl = Arc::new(open_repl_wp(&dir, store, true));

    // One boxed propose future per entry; each owns an Arc clone so the futures
    // are 'static. `poll_fn` drives them all on this task, polling in order, so
    // the first pass enqueues all N on the writer channel (propose enqueues
    // before it awaits) before any resolves — every entry is in flight at once.
    #[allow(clippy::type_complexity)]
    let mut futs: Vec<std::pin::Pin<Box<dyn Future<Output = AppliedAt> + Send>>> = entry_bytes(N)
        .into_iter()
        .map(|e| {
            let r = repl.clone();
            Box::pin(async move { r.propose(e, deadline()).await.expect("propose") })
                as std::pin::Pin<Box<dyn Future<Output = AppliedAt> + Send>>
        })
        .collect();
    let mut index_of: Vec<u64> = vec![0; futs.len()];
    let mut done: Vec<bool> = vec![false; futs.len()];
    std::future::poll_fn(|cx| {
        let mut all = true;
        for i in 0..futs.len() {
            if !done[i] {
                match futs[i].as_mut().poll(cx) {
                    std::task::Poll::Ready(at) => {
                        index_of[i] = at.index;
                        done[i] = true;
                    }
                    std::task::Poll::Pending => all = false,
                }
            }
        }
        if all {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    })
    .await;
    // Release the Arc clones the completed futures still hold.
    drop(futs);

    // Exactly-once, gapless, in submission order: entry i took index i+1. A
    // lost, duplicated or reordered index breaks this.
    let expected: Vec<u64> = (1..=N).collect();
    assert_eq!(
        index_of, expected,
        "every in-flight proposal took its own gapless index in order",
    );

    let repl = Arc::try_unwrap(repl).unwrap_or_else(|_| panic!("a propose task outlived the join"));
    assert_eq!(repl.applied_index(), N, "every proposal applied");
    let (_s, store) = repl.shutdown().expect("shutdown");
    let pipelined = digest_and_close(store);
    let _ = std::fs::remove_dir_all(&dir);

    // The overlap applied the identical log to the identical state: a syncer
    // that handed a later group to apply before an earlier one would diverge.
    assert_eq!(
        pipelined.whole,
        want.whole,
        "the pipeline's concurrent overlap must build the reference state, \
         first difference at {:?}",
        pipelined.first_difference(&want),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_log_leads_the_durable_store() {
    // With the default durable cadence (1 s / 256 MiB) a short, small run takes
    // no durable point: every entry is acknowledged (applied to the store's
    // page cache and answered) while the store commit that holds it is NOT yet
    // durable. That is the write-ahead-log property the design rests on, and
    // the state a kill -9 leaves for the crash test to replay.
    const N: u64 = 50;
    let dir = scratch("repl-wal");
    let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
    let repl = open_repl(&dir, store);
    for e in entry_bytes(N) {
        repl.propose(e, deadline()).await.expect("propose");
    }
    let m = repl.metrics();
    assert_eq!(m.applied_index, N, "everything acknowledged is applied");
    assert_eq!(m.last_log_index, N, "and fsynced in the log");
    assert!(
        m.durable_index < m.applied_index,
        "the store durable point ({}) should lag the acknowledged log ({}) \
         before any durable cadence fires",
        m.durable_index,
        m.applied_index,
    );
    assert!(
        m.inflight == 0,
        "nothing is in flight once every propose returned"
    );
    let (_s, store) = repl.shutdown().expect("shutdown");
    // The graceful shutdown flushed: now the platter holds it all.
    let store = Arc::try_unwrap(store).unwrap_or_else(|_| panic!("shared"));
    store.close();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// The fake replicator's branches (for WP-1.6b's batcher)
// ---------------------------------------------------------------------------

fn some_entry() -> Bytes {
    let mut w = Workload::new(SEED);
    Bytes::from(encode_entry(&w.next().entry).expect("encode"))
}

#[tokio::test]
async fn fake_commits() {
    let f = FakeReplicator::new(7);
    assert!(f.role().is_leader());
    let at = f.propose(some_entry(), deadline()).await.expect("commit");
    assert_eq!(at.index, 1);
    assert_eq!(f.applied_index(), 1);
    assert_eq!(f.proposals(), vec![some_entry()]);

    // A slow-but-healthy commit still lands.
    f.set_default(Step::After(Duration::from_millis(5)));
    let at = f
        .propose(some_entry(), deadline())
        .await
        .expect("slow commit");
    assert_eq!(at.index, 2);
    assert_eq!(f.applied_index(), 2);
}

#[tokio::test]
async fn fake_fails_and_not_leader() {
    let f = FakeReplicator::new(1);
    f.push_step(Step::Fail(ProposeError::Refused("nope".into())));
    match f.propose(some_entry(), deadline()).await {
        Err(ProposeError::Refused(s)) => assert_eq!(s, "nope"),
        other => panic!("want Refused, got {other:?}"),
    }
    // A scripted failure appends nothing.
    assert_eq!(f.applied_index(), 0);
    assert!(f.proposals().is_empty());

    // Not the leader: refused before the script is even read, with the hint.
    f.step_down(Some(42));
    match f.propose(some_entry(), deadline()).await {
        Err(ProposeError::NotLeader { hint }) => assert_eq!(hint, Some(42)),
        other => panic!("want NotLeader, got {other:?}"),
    }
    assert!(matches!(f.role(), Role::Follower { leader: Some(42) }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fake_times_out_and_keeps_the_entry_in_flight() {
    let f = Arc::new(FakeReplicator::new(1));

    // A pure timeout: the caller sees Timeout, the entry is appended (in
    // flight) but never commits.
    f.push_step(Step::Timeout);
    let short = Instant::now() + Duration::from_millis(30);
    match f.propose(some_entry(), short).await {
        Err(ProposeError::Timeout) => {}
        other => panic!("want Timeout, got {other:?}"),
    }
    assert_eq!(f.applied_index(), 0, "a pure timeout does not commit");
    assert_eq!(f.proposal_count(), 1);

    // The I3 case: Timeout to the caller, then the entry commits anyway.
    f.push_step(Step::TimeoutThenCommit(Duration::from_millis(20)));
    let short = Instant::now() + Duration::from_millis(30);
    match f.propose(some_entry(), short).await {
        Err(ProposeError::Timeout) => {}
        other => panic!("want Timeout, got {other:?}"),
    }
    // The planner kept it in flight; the fake commits it late.
    let target = f.committed_index();
    let end = Instant::now() + Duration::from_secs(2);
    while f.applied_index() < target && Instant::now() < end {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(
        f.applied_index() >= target,
        "the entry the caller left in flight committed late (applied {} of {target})",
        f.applied_index(),
    );
}

#[tokio::test]
async fn fake_role_gates_propose() {
    let f = FakeReplicator::new(3);
    // read_barrier refuses off the leader, answers on it.
    assert_eq!(f.read_barrier(deadline()).await.expect("barrier"), 0);
    f.step_down(None);
    assert!(matches!(
        f.read_barrier(deadline()).await,
        Err(ProposeError::NotLeader { .. })
    ));
    // transfer puts it back to a follower of the target; a fresh leader term
    // re-enables propose.
    f.set_role(Role::Leader { term: 5 });
    let at = f.propose(some_entry(), deadline()).await.expect("commit");
    assert_eq!(at.term, 5);
}

// ---------------------------------------------------------------------------
// Throughput smoke (laptop, #[ignore]) — §0.3: the numbers to quote are the VM's
// ---------------------------------------------------------------------------

#[test]
#[ignore = "smoke: laptop numbers, run with --ignored --nocapture"]
fn local_log_throughput_smoke() {
    let dir = scratch("log-smoke");
    // A real barrier, so the fsync p50 is a real fsync (Data: fdatasync on
    // Linux, plain fsync on macOS — see §0.3 on macOS's F_FULLFSYNC).
    let mut log = LogStore::open(
        &dir,
        LogOptions {
            segment_bytes: 64 << 20,
            fsync: Fsync::Data,
        },
    )
    .expect("open")
    .0;

    let body = vec![0xADu8; 256]; // a small entry
    const GROUPS: usize = 5000;
    let mut fsync_us: Vec<u128> = Vec::with_capacity(GROUPS);
    let start = Instant::now();
    for _ in 0..GROUPS {
        let t = Instant::now();
        log.append_group(&[body.as_slice()]).expect("append");
        fsync_us.push(t.elapsed().as_micros());
    }
    let elapsed = start.elapsed();
    fsync_us.sort_unstable();
    let p50 = fsync_us[fsync_us.len() / 2];
    let p99 = fsync_us[fsync_us.len() * 99 / 100];
    let per_s = GROUPS as f64 / elapsed.as_secs_f64();
    println!(
        "local log smoke: {GROUPS} single-entry groups in {:?} = {per_s:.0} entries/s; \
         per-group (incl. fsync) p50 {p50} µs, p99 {p99} µs",
        elapsed,
    );
    let _ = std::fs::remove_dir_all(&dir);
}
