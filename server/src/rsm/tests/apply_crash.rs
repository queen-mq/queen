//! `kill -9` of a process in the middle of applying, then the repair §11.5
//! names for a single voter: re-apply the local log from the durable index.
//!
//! This is the crash half of the idempotence WP-1.4 owes. The in-process half
//! ([`super::apply::re_applying_from_the_durable_index_changes_nothing`]) shows
//! that a replay across the applied index is a sequence of no-ops; this one
//! shows it against a state no orderly shutdown produced — a store that was
//! killed inside a transaction, segment files with a tail nothing recorded,
//! and an applied index the kill chose.
//!
//! # What is checked
//!
//! 1. The reopened node is CONSISTENT: `applied ≥ durable`, the segment tree
//!    reconciles against the recorded lengths without a disagreement (I11), and
//!    the ready rings rebuild.
//! 2. Re-applying the log from `durable_index + 1` to the end produces the
//!    SAME state as an uninterrupted run of the same entries, compared as the
//!    digest of every replicated keyspace in key order. Not "no error": equal
//!    bytes.
//! 3. The NODE-LOCAL keyspaces too (`seg_loc`, `files`, `partition_files`):
//!    where every frame is, how long every file is, which files exist at all.
//!    They are not in `state_digest` — they are not replicated state (D8, I7)
//!    — so a crash test that compared only that one proved nothing about the
//!    file table, which is the half of I11 that decides whether the node can
//!    boot. Both runs also collect files as they go (§11.7), so every round
//!    crosses a GC.
//!
//! # What is NOT checked here: durability
//!
//! `kill -9` leaves the page cache intact, so this file cannot tell a durable
//! point from an ordinary commit — the same limit `store_crash.rs` states, and
//! finding R-02/R-106. What it falsifies is the BOOKKEEPING: the recorded file
//! lengths, the applied index, the resume points, and every counter and index
//! apply maintains. The dropped-unflushed-writes run belongs to the Linux VM
//! (§13.6, WP-1.8/WP-1.11).
//!
//! The child is this same test binary re-executed with one test selected and
//! [`CRASH_DIR_ENV`] set; without it [`crash_child_applier`] returns at once,
//! so an ordinary `cargo test` run writes nothing.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::rsm::apply::{Applier, Committed, NoNotify, StateDigest};
use crate::rsm::effect::{Effect, Pid};

use super::apply::{
    cfg, fresh_cursor, group_meta, hashes, queue_config, seg_opts, seg_opts_buckets, settle, uuid,
    Build, Node, Workload, BASE_US, QUEUE, TENANT,
};

const CRASH_DIR_ENV: &str = "QUEEN_RSM_APPLY_CRASH_DIR";
const CRASH_N_ENV: &str = "QUEEN_RSM_APPLY_CRASH_N";
const CRASH_SEED_ENV: &str = "QUEEN_RSM_APPLY_CRASH_SEED";
/// PERF-F: the bucket count the fault child opens its segment tree at. Unset =
/// the legacy 256, so every existing cell is unchanged; the small-count cells
/// set it to 1 and 16.
const CRASH_BUCKETS_ENV: &str = "QUEEN_RSM_APPLY_CRASH_BUCKETS";
const CHILD_TEST: &str = "rsm::tests::apply_crash::crash_child_applier";

/// Entries per round. Enough that the child is still applying when the kill
/// lands, small enough that the parent's replay is a second of laptop time.
const ENTRIES: u64 = 3000;
/// The child takes a durable point this often, so the kill lands at every
/// phase of the cadence across the rounds.
const DURABLE_EVERY: u64 = 97;
/// And commits (non-durably, §11.3) this often. Both cadences matter: the
/// store then reopens AHEAD of the durable point, which is the case §11.5
/// step 2 warns about and the one the replay has to cross.
const COMMIT_EVERY: u64 = 8;
/// The workload seed. The same in the child and in the parent: that is what
/// makes the two runs comparable at all.
const SEED: u64 = 0x51EE_D000_1234;

/// Where the SIGKILL lands, in wall time after the child said it was applying.
/// Six of them, spread, because the slowest thing the child does is the
/// durable point: a single delay tends to land inside one of those every time,
/// and then the store reopens exactly AT the durable index and the replay has
/// nothing to skip.
const KILL_AFTER: [Duration; 6] = [
    Duration::from_millis(40),
    Duration::from_millis(65),
    Duration::from_millis(95),
    Duration::from_millis(130),
    Duration::from_millis(185),
    Duration::from_millis(260),
];

// ---------------------------------------------------------------------------
// The child
// ---------------------------------------------------------------------------

/// Apply entry after entry until killed. Never writes anything unless the
/// parent set [`CRASH_DIR_ENV`].
#[test]
fn crash_child_applier() {
    let Ok(dir) = std::env::var(CRASH_DIR_ENV) else {
        return;
    };
    let n: u64 = std::env::var(CRASH_N_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(ENTRIES);
    let seed: u64 = std::env::var(CRASH_SEED_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(SEED);
    let dir = PathBuf::from(dir);
    // NOT a `Node`: its `Drop` removes the directory, and this process is
    // meant to die with the data in place.
    let store = crate::rsm::store::HeedStore::open(&dir.join("store"), &super::apply::store_opts())
        .expect("child: open store");
    let (mut a, _rec) = Applier::open(
        &store,
        &dir.join("seg"),
        seg_opts(),
        cfg(),
        Arc::new(NoNotify),
    )
    .expect("child: open applier");

    let mut w = Workload::new(seed);
    for i in 1..=n {
        let c = w.next();
        a.apply(&c).expect("child: apply");
        if i % DURABLE_EVERY == 0 {
            a.durable_point().expect("child: durable point");
        } else if i % COMMIT_EVERY == 0 {
            a.commit().expect("child: commit");
        }
        // Exactly what `apply::run` does on every turn, so a kill can land
        // inside either phase of a file GC (§11.7): between the commit that
        // stopped naming a file and the unlink that follows it, or after the
        // unlink and before the commit that recorded it.
        a.gc_pass().expect("child: gc");
        if i == 1 {
            // From here the child is inside the apply loop, which is where the
            // kill has to land.
            say("WRITING");
        }
    }
    say(&format!("EXHAUSTED {n}"));
    std::thread::sleep(Duration::from_secs(120));
    panic!("child: was not killed");
}

fn say(line: &str) {
    use std::io::Write;
    println!("{line}");
    let _ = std::io::stdout().flush();
}

// ---------------------------------------------------------------------------
// The parent
// ---------------------------------------------------------------------------

struct Kid {
    child: Child,
    lines: mpsc::Receiver<String>,
}

impl Kid {
    fn spawn(dir: &Path, n: u64, seed: u64) -> Kid {
        let exe = std::env::current_exe().expect("test binary");
        let mut child = Command::new(&exe)
            .args(["--exact", CHILD_TEST, "--nocapture", "--test-threads", "1"])
            .env(CRASH_DIR_ENV, dir)
            .env(CRASH_N_ENV, n.to_string())
            .env(CRASH_SEED_ENV, seed.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the child");
        let stdout = child.stdout.take().expect("piped stdout");
        let (tx, lines) = mpsc::channel::<String>();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout)
                .lines()
                .map_while(std::result::Result::ok)
            {
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        Kid { child, lines }
    }

    fn wait_for(&self, marker: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            match self.lines.recv_timeout(Duration::from_secs(5)) {
                Ok(line) if line.contains(marker) => return true,
                Ok(_) => continue,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => return false,
            }
        }
        false
    }

    /// Did the child run out of entries before the kill? The round then proves
    /// less: the SIGKILL landed on a sleeping process.
    fn exhausted(&self) -> bool {
        self.lines.try_iter().any(|l| l.contains("EXHAUSTED"))
    }

    fn kill(mut self) {
        self.child.kill().expect("kill -9");
        let status = self.child.wait().expect("reap");
        assert!(!status.success(), "the child was supposed to be killed");
    }
}

/// The state an uninterrupted run of the same entries produces, replicated
/// and node-local.
fn reference_digests() -> (StateDigest, StateDigest) {
    let node = Node::new("crash-ref");
    let replicated = super::apply::run_workload(&node, SEED, ENTRIES, DURABLE_EVERY);
    (replicated, node.local_digest())
}

#[test]
fn a_node_killed_mid_apply_repairs_by_replaying_from_the_durable_index() {
    let (want, want_local) = reference_digests();

    let mut weak_rounds = 0;
    // Rounds in which the store reopened past its durable point, so the replay
    // actually crossed the applied index.
    let mut crossed = 0;
    for (round, delay) in KILL_AFTER.iter().enumerate() {
        let dir = std::env::temp_dir().join(format!(
            "queen-rsm-apply-crash-{}-{round}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("round dir");

        let kid = Kid::spawn(&dir, ENTRIES, SEED);
        assert!(
            kid.wait_for("WRITING"),
            "round {round}: the child never started"
        );
        std::thread::sleep(*delay);
        if kid.exhausted() {
            weak_rounds += 1;
        }
        kid.kill();

        // §11.5, from step 2: reopen, reconcile the files against the recorded
        // lengths, rebuild the RAM, and replay.
        let node = Node::at(dir.clone());
        {
            let (mut a, rec) = Applier::open(
                node.store(),
                &node.seg_dir(),
                seg_opts(),
                cfg(),
                Arc::new(NoNotify),
            )
            .expect("round: the killed node reopens");
            assert!(
                rec.applied_index >= rec.durable_index,
                "round {round}: reopened at {} BEHIND the durable point {}",
                rec.applied_index,
                rec.durable_index
            );
            assert_eq!(rec.replay_after, rec.durable_index);
            assert!(
                rec.applied_index > 0,
                "round {round}: the child applied nothing"
            );

            // The repair: every entry after the durable point, in order. The
            // ones the store already holds are skipped; the rest are applied,
            // including the one the kill interrupted.
            let mut w = Workload::new(SEED);
            let mut replayed = 0u64;
            let mut skipped = 0u64;
            for i in 1..=ENTRIES {
                let c = w.next();
                if i <= rec.replay_after {
                    continue;
                }
                match a.apply(&c).expect("replay") {
                    crate::rsm::apply::Applied::Skipped => skipped += 1,
                    crate::rsm::apply::Applied::Executed { .. } => replayed += 1,
                }
                a.gc_pass().expect("gc");
            }
            settle(&mut a);
            assert_eq!(
                a.segments_mut().saturated_releases(),
                0,
                "round {round}: a segment claim was released twice"
            );
            assert!(replayed > 0, "round {round}: nothing was left to replay");
            if rec.applied_index > rec.durable_index {
                // The case §11.5 step 2 is written for: the store reopened
                // PAST the durable point (pin 1 plus an intact page cache),
                // and the replay walked back over entries it already holds.
                assert!(
                    skipped > 0,
                    "round {round}: applied {} is past durable {}, so the replay \
                     had to skip",
                    rec.applied_index,
                    rec.durable_index
                );
                crossed += 1;
            }
        }

        let got = node.digest();
        assert_eq!(
            got.whole,
            want.whole,
            "round {round} ({delay:?}): the repaired state differs, first at {:?}",
            got.first_difference(&want)
        );
        // And the file table: same files, same recorded lengths, same
        // liveness, same positions. This is what says the node can boot.
        let got_local = node.local_digest();
        assert_eq!(
            got_local.whole,
            want_local.whole,
            "round {round} ({delay:?}): the node-local state differs, first at {:?} \
             ({:?} vs {:?})",
            got_local.first_difference(&want_local),
            got_local.per_keyspace,
            want_local.per_keyspace,
        );
        // `node` drops here and takes the directory with it.
    }
    assert!(
        weak_rounds < KILL_AFTER.len(),
        "every round killed a child that had already finished: the delays need raising"
    );
    assert!(
        crossed > 0,
        "no round reopened past its durable point, so nothing exercised the \
         idempotence the repair needs"
    );
}

// ---------------------------------------------------------------------------
// Fault-driven crash points (§13.5, WP-1.8)
// ---------------------------------------------------------------------------
//
// The crash matrix (test/raft/crash) drives a raft1 broker over HTTP and
// reaches 11 of the 13 phase-1 points. The two GC points — `gc.before_unlink`,
// `gc.after_unlink` — cannot be reached from a phase-1 push/pop/ack workload (a
// file becomes collectable only through retention or a delete, §10.3, WP-2.7),
// and the two segment-roll points of R-107 — `seg.rolled`, `seg.qidx_written` —
// are not in the HTTP matrix. This test proves ALL of those, plus the apply/
// durable points, DO fire — by arming each in a child that runs the apply
// workload (which crosses GC, durable points and 8 KiB segment rolls) and then
// aborts itself at the point — and that the node REPAIRS to the same state a
// clean run produces (§11.5). It is R-107's "re-run the roll cases against the
// fault points", extended to every apply-side point.
//
// Unlike the timing kill above, this kill is DETERMINISTIC: `faults::hit`
// aborts at exactly the armed point, so a failure names the point.

const FAULT_ENV: &str = "QUEEN_TEST_FAULTS";
const FAULT_CHILD_TEST: &str = "rsm::tests::apply_crash::crash_child_fault_applier";

/// The points this test arms, with the nth hit and how many entries the child
/// runs to reach it. Every one is reached by the apply workload; the child
/// aborts at it. `apply.mid_entry` needs an entry with ≥2 effects (the
/// workload's multi-command entries), `seg.*` need a roll (8 KiB segments,
/// early), `gc.*` need a fully dead file — the workload's watermarks retire one
/// only after ~6000 entries (measured), so those cells run longer. The parent's
/// reference is a clean run of the SAME entry count.
const FAULT_POINTS: &[(&str, u64, u64)] = &[
    ("apply.mid_entry", 1, ENTRIES),
    ("apply.segment_written", 1, ENTRIES),
    ("apply.store_committed", 1, ENTRIES),
    ("durable.files_synced", 1, ENTRIES),
    ("durable.store_committed", 1, ENTRIES),
    ("seg.rolled", 1, ENTRIES),
    ("seg.qidx_written", 1, ENTRIES),
    ("gc.before_unlink", 1, ENTRIES_GC),
    ("gc.after_unlink", 1, ENTRIES_GC),
];

/// Entries a GC cell runs. The workload's slow watermark retires the first full
/// segment file at ~6000 entries (measured: 0 unlinks by 3000, 4 by 6000), so
/// arm `gc.*:1` over a longer run to be sure it fires.
const ENTRIES_GC: u64 = 8000;

/// The child: arm the fault from the environment, then apply until it aborts.
/// Never runs unless the parent set both [`CRASH_DIR_ENV`] and [`FAULT_ENV`].
#[test]
fn crash_child_fault_applier() {
    let Ok(dir) = std::env::var(CRASH_DIR_ENV) else {
        return;
    };
    if std::env::var(FAULT_ENV).is_err() {
        return;
    }
    // Arm the crash point (§13.5). From here `faults::hit` aborts the process
    // at the named point.
    crate::rsm::faults::init_from_env();
    let n: u64 = std::env::var(CRASH_N_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(ENTRIES);
    let dir = PathBuf::from(dir);
    let store = crate::rsm::store::HeedStore::open(&dir.join("store"), &super::apply::store_opts())
        .expect("child: open store");
    let (mut a, _rec) = Applier::open(
        &store,
        &dir.join("seg"),
        child_seg_opts(),
        cfg(),
        Arc::new(NoNotify),
    )
    .expect("child: open applier");

    let mut w = Workload::new(SEED);
    say("WRITING");
    for i in 1..=n {
        let c = w.next();
        a.apply(&c).expect("child: apply");
        if i % DURABLE_EVERY == 0 {
            a.durable_point().expect("child: durable point");
        } else if i % COMMIT_EVERY == 0 {
            a.commit().expect("child: commit");
        }
        a.gc_pass().expect("child: gc");
    }
    // If control reaches here the armed point NEVER FIRED — the code path was
    // not taken. Say so and exit non-abort, so the parent's SIGABRT assert
    // fails with a clear message instead of hanging.
    say("EXHAUSTED-NO-FAULT");
    std::process::exit(3);
}

/// The segment options the fault child opens with: the small-file testing
/// options at the bucket count [`CRASH_BUCKETS_ENV`] names (PERF-F), or the
/// legacy 256 when it is unset.
fn child_seg_opts() -> crate::rsm::segments::Options {
    match std::env::var(CRASH_BUCKETS_ENV)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
    {
        Some(n) => seg_opts_buckets(n),
        None => seg_opts(),
    }
}

/// Spawn the fault child with one point armed over `n` entries, and let it
/// abort itself.
fn spawn_fault_child(dir: &Path, fault: &str, n: u64) -> Child {
    spawn_fault_child_buckets(dir, fault, n, None)
}

/// [`spawn_fault_child`] with an explicit bucket count for the PERF-F cells.
fn spawn_fault_child_buckets(dir: &Path, fault: &str, n: u64, nbuckets: Option<usize>) -> Child {
    let exe = std::env::current_exe().expect("test binary");
    let mut cmd = Command::new(&exe);
    cmd.args([
        "--exact",
        FAULT_CHILD_TEST,
        "--nocapture",
        "--test-threads",
        "1",
    ])
    .env(CRASH_DIR_ENV, dir)
    .env(CRASH_SEED_ENV, SEED.to_string())
    .env(CRASH_N_ENV, n.to_string())
    .env(FAULT_ENV, fault)
    .stdout(Stdio::null())
    .stderr(Stdio::null());
    if let Some(n) = nbuckets {
        cmd.env(CRASH_BUCKETS_ENV, n.to_string());
    }
    cmd.spawn().expect("spawn the fault child")
}

/// A clean run of `n` entries, replicated and node-local digests: the state
/// the child + replay must reproduce. Cached per `n` so the two counts are
/// each computed once.
fn reference_for(n: u64) -> (StateDigest, StateDigest) {
    let node = Node::new(&format!("crash-ref-{n}"));
    let replicated = super::apply::run_workload(&node, SEED, n, DURABLE_EVERY);
    (replicated, node.local_digest())
}

#[test]
fn each_fault_point_fires_and_the_node_repairs() {
    use std::collections::HashMap;
    use std::os::unix::process::ExitStatusExt;

    let mut refs: HashMap<u64, (StateDigest, StateDigest)> = HashMap::new();

    for (point, nth, n) in FAULT_POINTS.iter().copied() {
        let (want, want_local) = refs.entry(n).or_insert_with(|| reference_for(n)).clone();
        let fault = format!("{point}:{nth}");
        let dir = std::env::temp_dir().join(format!(
            "queen-rsm-fault-{}-{}",
            std::process::id(),
            point.replace('.', "_"),
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("fault dir");

        let mut child = spawn_fault_child(&dir, &fault, n);
        // Wait for the child to abort ITSELF at the point (no kill from here).
        let deadline = Instant::now() + Duration::from_secs(60);
        let status = loop {
            if let Some(s) = child.try_wait().expect("try_wait") {
                break s;
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("{fault}: the point never fired within 60s (code path not taken)");
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        assert_eq!(
            status.signal(),
            Some(9),
            "{fault}: the child exited {status:?}, not by SIGKILL — \
             the fault did not fire (a code=3 exit means it ran to the end)"
        );

        // §11.5: reopen the aborted state, replay every entry after the durable
        // index, and prove the repaired state is byte-equal to a clean run.
        let node = Node::at(dir.clone());
        {
            let (mut a, rec) = super::apply::open_at(&node);
            assert!(
                rec.applied_index >= rec.durable_index,
                "{fault}: reopened at {} behind durable {}",
                rec.applied_index,
                rec.durable_index
            );
            let mut w = Workload::new(SEED);
            let mut replayed = 0u64;
            for i in 1..=n {
                let c = w.next();
                if i <= rec.replay_after {
                    continue;
                }
                if let crate::rsm::apply::Applied::Executed { .. } = a.apply(&c).expect("replay") {
                    replayed += 1;
                }
                a.gc_pass().expect("gc");
            }
            settle(&mut a);
            assert_eq!(
                a.segments_mut().saturated_releases(),
                0,
                "{fault}: a segment claim was released twice"
            );
            assert!(replayed > 0, "{fault}: nothing was left to replay");
        }
        let got = node.digest();
        assert_eq!(
            got.whole,
            want.whole,
            "{fault}: the repaired replicated state differs, first at {:?}",
            got.first_difference(&want)
        );
        let got_local = node.local_digest();
        assert_eq!(
            got_local.whole,
            want_local.whole,
            "{fault}: the repaired node-local state differs, first at {:?}",
            got_local.first_difference(&want_local)
        );
    }
}

// ---------------------------------------------------------------------------
// PERF-F: the same crash + repair at a small bucket count
// ---------------------------------------------------------------------------
//
// The fold and the smaller durable-point file set (`QUEEN_RAFT_BUCKETS` = 1 and
// 16) change the segment layout and what a durable point fsyncs, so recovery has
// to reconcile a folded tree against the recorded lengths just the same. This
// arms the two points that matter for that — `apply.segment_written` (the append
// crashed with its bytes in a folded bucket file, unrecorded) and
// `durable.files_synced` (crashed mid durable point) — at buckets 1 and 16, and
// proves the aborted node reopens CONSISTENT (I11: no disagreement, applied ≥
// durable) and replays its tail to completion. The bucket count is pinned in the
// tree manifest by the child, so the parent reopens at the same count.

#[test]
fn segment_written_and_durable_points_repair_at_small_bucket_counts() {
    use std::os::unix::process::ExitStatusExt;

    // Fires on the FIRST append and the FIRST durable point, so each child dies
    // early — these cells are cheap even across two bucket counts.
    let points = ["apply.segment_written", "durable.files_synced"];
    for nbuckets in [1usize, 16] {
        for point in points {
            let n = ENTRIES;
            let fault = format!("{point}:1");
            let dir = std::env::temp_dir().join(format!(
                "queen-rsm-fault-b{nbuckets}-{}-{}",
                std::process::id(),
                point.replace('.', "_"),
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("fault dir");

            let mut child = spawn_fault_child_buckets(&dir, &fault, n, Some(nbuckets));
            let deadline = Instant::now() + Duration::from_secs(60);
            let status = loop {
                if let Some(s) = child.try_wait().expect("try_wait") {
                    break s;
                }
                if Instant::now() > deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("{fault} @ b{nbuckets}: the point never fired within 60s");
                }
                std::thread::sleep(Duration::from_millis(20));
            };
            assert_eq!(
                status.signal(),
                Some(9),
                "{fault} @ b{nbuckets}: the child exited {status:?}, not by SIGKILL"
            );

            // Reopen the aborted state at the SAME count (the manifest pins it),
            // replay every entry after the durable index, prove it reaches the
            // end. `Applier::open` returning Ok is itself the I11 check: a folded
            // tree that did not reconcile is a disagreement, an error, not Ok.
            let store =
                crate::rsm::store::HeedStore::open(&dir.join("store"), &super::apply::store_opts())
                    .expect("reopen store");
            let (mut a, rec) = Applier::open(
                &store,
                &dir.join("seg"),
                seg_opts_buckets(nbuckets),
                cfg(),
                Arc::new(NoNotify),
            )
            .expect("reopen applier: the folded tree reconciles (I11)");
            assert_eq!(
                a.segments_mut().nbuckets(),
                nbuckets,
                "{fault} @ b{nbuckets}: the manifest pinned the count"
            );
            assert!(
                rec.applied_index >= rec.durable_index,
                "{fault} @ b{nbuckets}: reopened at {} behind durable {}",
                rec.applied_index,
                rec.durable_index
            );
            let mut w = Workload::new(SEED);
            let mut replayed = 0u64;
            for i in 1..=n {
                let c = w.next();
                if i <= rec.replay_after {
                    continue;
                }
                if let crate::rsm::apply::Applied::Executed { .. } = a.apply(&c).expect("replay") {
                    replayed += 1;
                }
                a.gc_pass().expect("gc");
            }
            settle(&mut a);
            assert!(
                replayed > 0,
                "{fault} @ b{nbuckets}: nothing was left to replay"
            );
            assert_eq!(
                a.segments_mut().saturated_releases(),
                0,
                "{fault} @ b{nbuckets}: a segment claim was released twice"
            );
            drop(a);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

// ---------------------------------------------------------------------------
// A COMPLETION crashed mid-apply (§13.5 `apply.mid_entry`, I1/I11; WP-1.8)
// ---------------------------------------------------------------------------
//
// The HTTP crash matrix (test/raft/crash) reaches `apply.mid_entry` only on the
// first entries of a run, which are unavoidably pushes (a claim needs a prior
// push): its `nth∈{1,2}` cells crash on an `Append`, never on a cursor. So "a
// claim/ack entry crashed MID-APPLY" is NOT exercised by the HTTP matrix — the
// WP-1.8 refutation. `each_fault_point_fires_and_the_node_repairs` above proves
// the GENERAL mechanism (an uncommitted store txn is discarded and the whole
// entry replays, identical across effect kinds), but its `apply.mid_entry:1`
// lands on the shared `Workload`'s SEED entry (QueueUpsert + GroupUpsert +
// PartitionCreate) — pure setup, no completion.
//
// This test closes that gap deterministically: it builds a three-entry script
// whose LAST entry carries a `CursorSet` (a completion: it advances `committed`
// and bumps the Completed counter) as its FIRST of two effects, arms
// `apply.mid_entry:3` so the kill lands AFTER that completion is written to the
// open store txn but BEFORE the entry commits, and proves the reopened node
// replays to a state byte-equal to a clean run — replicated AND node-local.
// The count of 3: the seed entry's three effects fire `apply.mid_entry` twice
// (after effects 0 and 1), the append entry has one effect and fires it not at
// all, so the completion entry's only mid-entry point is the third hit.

/// The child selected by [`a_completion_entry_crashed_mid_apply_repairs`]. Runs
/// only when the parent set both [`CRASH_DIR_ENV`] and [`FAULT_ENV`].
const CURSOR_CHILD_TEST: &str = "rsm::tests::apply_crash::crash_child_cursor_mid_entry";

/// A fixed three-entry script whose third entry's first effect is a completion
/// (`CursorSet` advancing `committed`). Deterministic and clock-free, so the
/// child, the reference run and the replay all build identical entries. The
/// per-entry `pid_base`/`kv_version_base`/`now_us` follow what a fresh applier
/// expects (I18/I5): pid_base is the running `next_pid` (1, then 2, then 2),
/// kv_version_base is 1 throughout (no KV writes), now_us is strictly monotone.
fn cursor_mid_apply_script() -> Vec<Committed> {
    let g = "g1";
    let pid: Pid = 1;
    let bucket = (pid % 8) as u16;
    let (t1, t2, t3) = (BASE_US + 1_000, BASE_US + 2_000, BASE_US + 3_000);

    // Entry 1 (index 1): create the queue, one group and one partition. Three
    // effects in one command — `apply.mid_entry` fires twice inside it.
    let e1 = Build::new(t1, 1, 0)
        .cmd(vec![
            Effect::QueueUpsert {
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                cfg: queue_config(t1),
            },
            Effect::GroupUpsert {
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                group: g.into(),
                meta: group_meta(0, t1),
            },
            Effect::PartitionCreate {
                pid,
                uuid: uuid(pid),
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                partition: "p0".into(),
                created_at_us: t1,
            },
        ])
        .at(1, 1);

    // Entry 2 (index 2): append four messages, so offsets 0..3 exist for a
    // cursor to advance over. One effect — fires `apply.mid_entry` not at all.
    let e2 = Build::new(t2, 2, 100)
        .cmd(vec![Effect::Append {
            pid,
            bucket,
            base_offset: 0,
            count: 4,
            created_at_us: t2,
            hashes: hashes(0xAB, 4),
            blob: vec![0xAB; 24 * 4],
        }])
        .at(2, 1);

    // Entry 3 (index 3): a COMPLETION (advance committed to 1, two messages
    // consumed) then an append. Two effects, so `apply.mid_entry` fires once —
    // after the completion, before the append and before the commit.
    let mut row = fresh_cursor(1, t3);
    row.total_consumed = 2;
    let e3 = Build::new(t3, 2, 200)
        .cmd(vec![Effect::CursorSet {
            pid,
            group: g.into(),
            row,
        }])
        .cmd(vec![Effect::Append {
            pid,
            bucket,
            base_offset: 4,
            count: 1,
            created_at_us: t3,
            hashes: hashes(0xCD, 1),
            blob: vec![0xAB; 24],
        }])
        .at(3, 1);

    vec![e1, e2, e3]
}

/// The child: apply entries 1 and 2 and COMMIT them (so the store reopens with
/// them durable and the crash is one PAST a commit), then apply entry 3 with
/// `apply.mid_entry:3` armed — the kill lands after the completion is in the
/// open txn. Never runs unless the parent set both env vars.
#[test]
fn crash_child_cursor_mid_entry() {
    let Ok(dir) = std::env::var(CRASH_DIR_ENV) else {
        return;
    };
    if std::env::var(FAULT_ENV).is_err() {
        return;
    }
    crate::rsm::faults::init_from_env();
    let dir = PathBuf::from(dir);
    let store = crate::rsm::store::HeedStore::open(&dir.join("store"), &super::apply::store_opts())
        .expect("child: open store");
    let (mut a, _rec) = Applier::open(
        &store,
        &dir.join("seg"),
        seg_opts(),
        cfg(),
        Arc::new(NoNotify),
    )
    .expect("child: open applier");

    let script = cursor_mid_apply_script();
    a.apply(&script[0]).expect("child: apply seed"); // survives 2 mid_entry hits
    a.gc_pass().expect("child: gc");
    a.apply(&script[1]).expect("child: apply append"); // no mid_entry hit
    a.gc_pass().expect("child: gc");
    a.commit().expect("child: commit entries 1 and 2");
    // The completion entry: `apply.mid_entry` fires after the `CursorSet` and
    // aborts the process (SIGKILL). Control does not return.
    a.apply(&script[2]).expect("child: apply completion entry");
    // Reached only if the fault never fired (a wrong nth): make the parent's
    // SIGKILL assert fail loudly instead of hanging.
    say("EXHAUSTED-NO-FAULT");
    std::process::exit(3);
}

fn spawn_cursor_child(dir: &Path, fault: &str) -> Child {
    let exe = std::env::current_exe().expect("test binary");
    Command::new(&exe)
        .args([
            "--exact",
            CURSOR_CHILD_TEST,
            "--nocapture",
            "--test-threads",
            "1",
        ])
        .env(CRASH_DIR_ENV, dir)
        .env(FAULT_ENV, fault)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn the cursor child")
}

#[test]
fn a_completion_entry_crashed_mid_apply_repairs() {
    use std::os::unix::process::ExitStatusExt;

    let script = cursor_mid_apply_script();

    // Reference: a clean, uninterrupted run of the same three entries.
    let (want, want_local) = {
        let node = Node::new("cursor-mid-ref");
        {
            let (mut a, _rec) = Applier::open(
                node.store(),
                &node.seg_dir(),
                seg_opts(),
                cfg(),
                Arc::new(NoNotify),
            )
            .expect("ref: open");
            for c in &script {
                a.apply(c).expect("ref: apply");
                a.gc_pass().expect("ref: gc");
            }
            settle(&mut a);
        }
        (node.digest(), node.local_digest())
    };

    let dir = std::env::temp_dir().join(format!("queen-rsm-cursor-mid-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");

    let mut child = spawn_cursor_child(&dir, "apply.mid_entry:3");
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(s) = child.try_wait().expect("try_wait") {
            break s;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("apply.mid_entry:3 never fired on the completion entry (code path not taken)");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        status.signal(),
        Some(9),
        "the child exited {status:?}, not by SIGKILL — the completion mid-apply \
         fault did not fire (a code=3 exit means it ran to the end)"
    );

    // §11.5: reopen the aborted state, prove the completion entry was NOT
    // committed (the kill landed mid-apply), replay it, and prove byte-equal.
    let node = Node::at(dir.clone());
    {
        let (mut a, rec) = super::apply::open_at(&node);
        assert!(
            rec.applied_index >= rec.durable_index,
            "reopened at {} behind durable {}",
            rec.applied_index,
            rec.durable_index
        );
        assert!(
            rec.applied_index < 3,
            "the completion entry (index 3) was already committed at reopen \
             (applied={}); the kill did not land mid-apply",
            rec.applied_index
        );
        let mut replayed = 0u64;
        for (i, c) in script.iter().enumerate() {
            let idx = i as u64 + 1;
            if idx <= rec.replay_after {
                continue;
            }
            if let crate::rsm::apply::Applied::Executed { .. } = a.apply(c).expect("replay") {
                replayed += 1;
            }
            a.gc_pass().expect("gc");
        }
        settle(&mut a);
        assert!(replayed > 0, "nothing was left to replay");
    }
    let got = node.digest();
    assert_eq!(
        got.whole,
        want.whole,
        "the repaired replicated state differs, first at {:?}",
        got.first_difference(&want)
    );
    let got_local = node.local_digest();
    assert_eq!(
        got_local.whole,
        want_local.whole,
        "the repaired node-local state differs, first at {:?}",
        got_local.first_difference(&want_local)
    );
}
