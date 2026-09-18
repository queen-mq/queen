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

use crate::rsm::apply::{Applier, NoNotify, StateDigest};

use super::apply::{cfg, seg_opts, settle, Node, Workload};

const CRASH_DIR_ENV: &str = "QUEEN_RSM_APPLY_CRASH_DIR";
const CRASH_N_ENV: &str = "QUEEN_RSM_APPLY_CRASH_N";
const CRASH_SEED_ENV: &str = "QUEEN_RSM_APPLY_CRASH_SEED";
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
