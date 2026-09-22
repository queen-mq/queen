//! `kill -9` of a writing process, then reopen: what the store survives, and
//! what this test canNOT decide.
//!
//! This is the check G0 asked WP-1.2 to make. D9 was ratified with heed's
//! crash evidence explicitly incomplete — "WP-1.2's own crash tests are the
//! check, and the decision is re-opened if they disagree" (PLAN_RAFT.md §2 D9,
//! RAFT_STATUS.md R-01/R-02).
//!
//! # What is checked, and with what power
//!
//! 1. **A reopened store holds a whole number of committed transactions.** The
//!    child commits continuously and the parent kills it after a delay, so the
//!    SIGKILL lands at an arbitrary point — usually INSIDE `mdb_txn_commit`,
//!    while dirty pages are being written or the meta page is being flipped,
//!    which is the only moment at which a store can come back torn. The
//!    reopened applied index and the rows then have to agree exactly:
//!    `rows == applied × ROWS`, every value right, nothing from the entry
//!    after it. [`a_store_killed_while_committing_reopens_whole`].
//! 2. **An uncommitted transaction leaves nothing**, killed where it stands —
//!    and since Phase C neither does a plain commit: only the durable point
//!    survives. [`an_uncommitted_transaction_is_invisible_after_a_kill`].
//! 3. **The applied index never goes backwards across a crash-restart
//!    sequence.** Each round continues in the SAME directory from the index it
//!    reopened with, so a store that lost a committed transaction fails the
//!    next round's comparison. This is the local shape of the property S3
//!    reproduced as R-37 ("WENT BACKWARDS on 1001").
//!
//! # What is NOT checked here: durability
//!
//! `kill -9` leaves the page cache intact, so this file cannot tell a durable
//! point that reached the platter from one that did not: it passes unchanged
//! if `force_sync` were a no-op. What it DOES tell apart since Phase C is the
//! checkpoint from a plain commit: every keyspace is a RAM table that only
//! [`Writes::durable_commit`] writes into LMDB, so the store reopens EXACTLY at
//! the last durable point (`reopen_and_check` asserts `applied == durable`) —
//! no longer past it, as S1 measured in 12 of 15 runs before. The run that
//! gives the durable point its platter meaning is the dropped-unflushed-writes
//! one (dm-flakey on the Linux VM: WP-1.8's crash matrix, and the deferred
//! D-01/D-02). **Until that run exists, D9's durability leg is not discharged
//! by this file** — the legs above are.
//!
//! # The power of check 1, demonstrated
//!
//! A test that cannot fail proves nothing, so this one was run against a
//! deliberately broken writer: the child committing its applied index in one
//! transaction and the entry's rows in the NEXT (the defect class the check
//! exists for). Both tests caught it at the first reopen — "applied index 226
//! promises 7232 rows, the store holds 7200" and, in the mid-transaction case,
//! "applied index 4 promises 128 rows, the store holds 96". The kill therefore
//! lands where a torn store would show.
//!
//! The child is this same test binary, re-executed with one test selected,
//! [`CRASH_DIR_ENV`] set and [`CRASH_MODE_ENV`] naming what it should do.
//! Without those variables [`crash_child_writer`] returns at once, so an
//! ordinary `cargo test` run never writes anything.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::rsm::store::keys::{self, Counter};
use crate::rsm::store::{
    HeedStore, Keyspace, Reads, Store, StoreOpts, TypedReads, TypedWrites, Writes,
};

/// Set by the parent; names the store directory the child writes into.
const CRASH_DIR_ENV: &str = "QUEEN_RSM_STORE_CRASH_DIR";
/// `steady` (commit in a loop until killed) or `midtxn` (stop inside an
/// uncommitted transaction and wait).
const CRASH_MODE_ENV: &str = "QUEEN_RSM_STORE_CRASH_MODE";
/// The child's own name, as libtest filters it.
const CHILD_TEST: &str = "rsm::tests::store_crash::crash_child_writer";

/// Rows per simulated entry.
const ROWS: u64 = 32;
/// The child takes a durable point (§11.4) every this many entries.
const DURABLE_EVERY: u64 = 64;
/// The child stops writing after this many entries in one run and waits to be
/// killed. Only a bound on the data a run can leave behind: every kill delay
/// below is far shorter than the time this takes.
const MAX_ENTRIES_PER_RUN: u64 = 40_000;
/// How long the parent lets the child commit before the kill. Three values, so
/// the SIGKILL lands at three different points of the write path.
const KILL_AFTER: [Duration; 3] = [
    Duration::from_millis(40),
    Duration::from_millis(110),
    Duration::from_millis(230),
];

fn opts() -> StoreOpts {
    StoreOpts {
        map_bytes: Some(512 << 20),
        ..Default::default()
    }
}

/// The row key of the `n`-th row: an ordinary counter key, so the crash test
/// exercises a real keyspace rather than a private one.
fn row_key(n: u64) -> Vec<u8> {
    keys::counter_partition(n, Counter::Pushed)
}

// ---------------------------------------------------------------------------
// The child
// ---------------------------------------------------------------------------

/// `steady`: continue from the applied index the store reopens with and commit
/// entry after entry until killed. `midtxn`: take a durable point at the 2nd
/// entry, a plain commit at the 3rd, then stage a 4th that is never committed
/// and wait to be killed inside it.
#[test]
fn crash_child_writer() {
    let Ok(dir) = std::env::var(CRASH_DIR_ENV) else {
        // The ordinary run: this test is a no-op.
        return;
    };
    let mode = std::env::var(CRASH_MODE_ENV).unwrap_or_else(|_| "steady".into());
    let dir = PathBuf::from(dir);
    let store = HeedStore::open(&dir, &opts()).expect("child: open");
    let from = store
        .read(|r| r.applied_index())
        .expect("child: read applied");
    let mut w = store.write().expect("child: write txn");

    let mut e = from;
    let stop_at = from + MAX_ENTRIES_PER_RUN;
    while e < stop_at {
        e += 1;
        for i in 0..ROWS {
            let n = (e - 1) * ROWS + i;
            w.set_counter(&row_key(n), n as i64).expect("child: put");
        }
        w.set_applied(e, 1).expect("child: applied");

        if mode == "midtxn" && e == from + 4 {
            // The transaction that must leave NOTHING behind: a whole entry's
            // rows and a new applied index, never committed.
            say(&format!("MIDTXN {}", e - 1));
            std::thread::sleep(Duration::from_secs(120));
            panic!("child: was not killed");
        }

        // `midtxn` needs a durable point BELOW its plain commit: Phase C
        // persists nothing else, so that is what a kill must reopen at.
        if e % DURABLE_EVERY == 0 || (mode == "midtxn" && e == from + 2) {
            w.set_meta_u64(crate::rsm::store::meta::DURABLE_INDEX, e)
                .expect("child: durable index");
            w.durable_commit().expect("child: durable commit");
        } else {
            w.commit().expect("child: commit");
        }
        if e == from + 1 {
            // The parent starts its clock here: from now on the child is
            // inside the commit loop, which is where the kill must land.
            say("WRITING");
        }
    }
    say(&format!("EXHAUSTED {e}"));
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

/// A child writing into `dir`, plus the lines it prints.
struct Writer {
    child: Child,
    lines: mpsc::Receiver<String>,
}

impl Writer {
    fn spawn(dir: &Path, mode: &str) -> Writer {
        let exe = std::env::current_exe().expect("test binary");
        let mut child = Command::new(&exe)
            .args(["--exact", CHILD_TEST, "--nocapture", "--test-threads", "1"])
            .env(CRASH_DIR_ENV, dir)
            .env(CRASH_MODE_ENV, mode)
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
        Writer { child, lines }
    }

    /// Wait for a line containing `marker`, up to 60 s.
    fn wait_for(&self, marker: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            match self.lines.recv_timeout(Duration::from_secs(5)) {
                // libtest's own progress line has no newline yet, so the
                // child's first marker can share a line with it.
                Ok(line) if line.contains(marker) => return true,
                Ok(_) => continue,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => return false,
            }
        }
        false
    }

    /// Did the child say it had run out of entries before we killed it? If so
    /// the kill landed on a sleeping process and the round proved less.
    fn exhausted(&self) -> bool {
        self.lines
            .try_iter()
            .any(|l| l.contains("EXHAUSTED") || l.contains("MIDTXN"))
    }

    fn kill(mut self) {
        self.child.kill().expect("kill -9");
        let status = self.child.wait().expect("reap");
        assert!(!status.success(), "the child was supposed to be killed");
    }
}

/// What a reopen found.
#[derive(Clone, Copy, Debug)]
struct Reopened {
    applied: u64,
    durable: u64,
    rows: u64,
}

/// Reopen exactly as §11.5 step 2 does and check the two properties that do
/// not depend on anything reaching the platter.
fn reopen_and_check(dir: &Path) -> Reopened {
    let store = HeedStore::open(dir, &opts()).expect("reopen after kill -9");
    let out = store
        .read(|r| {
            let applied = r.applied_index()?;
            let durable = r.durable_index()?;
            // Phase C: every keyspace is a RAM table checkpointed only at the
            // durable point, so a reopen lands EXACTLY on it — never behind,
            // and never past it on a plain commit that reached the page cache.
            assert_eq!(
                applied, durable,
                "reopened at applied {applied}, not at the durable point {durable}"
            );

            // The rows and the applied index agree EXACTLY: a whole number of
            // transactions, never a fragment of the one the kill interrupted.
            let want = applied * ROWS;
            let rows = r.count(Keyspace::Counters)?;
            assert_eq!(
                rows, want,
                "applied index {applied} promises {want} rows, the store holds {rows}"
            );
            for n in 0..want {
                let v = r.counter_at(&row_key(n))?;
                assert_eq!(v, n as i64, "row {n} came back as {v}");
            }
            // Nothing from the interrupted transaction leaked in.
            assert_eq!(
                r.counter_at(&row_key(want))?,
                0,
                "a row of entry {} survived without its transaction",
                applied + 1
            );
            Ok(Reopened {
                applied,
                durable,
                rows,
            })
        })
        .expect("read after reopen");
    store.close();
    out
}

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("queen-rsm-crash-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

#[test]
fn a_store_killed_while_committing_reopens_whole() {
    let dir = temp_dir("steady");
    let mut prev = Reopened {
        applied: 0,
        durable: 0,
        rows: 0,
    };
    let mut landed_in_the_loop = 0;

    for (round, delay) in KILL_AFTER.iter().enumerate() {
        let w = Writer::spawn(&dir, "steady");
        assert!(
            w.wait_for("WRITING"),
            "round {round}: the child never reached its commit loop"
        );
        std::thread::sleep(*delay);
        let still_writing = !w.exhausted();
        w.kill();
        if still_writing {
            landed_in_the_loop += 1;
        }

        let now = reopen_and_check(&dir);
        // The store came back with everything the previous round had, and with
        // whatever this round committed on top: an index that went backwards
        // is a committed transaction lost (R-37's shape, locally).
        assert!(
            now.applied >= prev.applied,
            "round {round}: applied went BACKWARDS, {} -> {}",
            prev.applied,
            now.applied
        );
        assert!(
            now.durable >= prev.durable,
            "round {round}: the durable point went backwards, {} -> {}",
            prev.durable,
            now.durable
        );
        assert!(
            now.applied > prev.applied,
            "round {round}: the child reached no durable point, so nothing was tested"
        );
        println!(
            "round {round}: killed {:?} into the commit loop; reopened at applied={} \
             (durable={}, rows={}), at the durable point",
            delay, now.applied, now.durable, now.rows,
        );
        prev = now;
    }

    assert!(
        landed_in_the_loop > 0,
        "every kill landed on a sleeping child: the test had no power"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_uncommitted_transaction_is_invisible_after_a_kill() {
    let dir = temp_dir("midtxn");
    let w = Writer::spawn(&dir, "midtxn");
    assert!(
        w.wait_for("MIDTXN"),
        "the child never reached its uncommitted transaction"
    );
    // SIGKILL, with the write transaction open and unflushed.
    w.kill();

    let now = reopen_and_check(&dir);
    // Phase C: only the durable point survives. The plain commit of entry 3
    // wrote nothing into LMDB (every keyspace is a RAM table), and the staged
    // entry 4 was never committed at all; `reopen_and_check` proved neither
    // left a row behind.
    assert_eq!(
        (now.applied, now.durable),
        (2, 2),
        "the child took a durable point at entry 2, plain-committed entry 3 and \
         staged a 4th: only the durable point survives a kill"
    );
    println!(
        "kill -9 inside an open transaction: reopened at applied={} with {} rows",
        now.applied, now.rows
    );
    let _ = std::fs::remove_dir_all(&dir);
}
