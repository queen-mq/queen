//! `kill -9` of a process proposing through a `LocalReplicator`, then the
//! reopen (PLAN_RAFT.md §12.2, §11.5): the local log recovers — a torn tail is
//! truncated — and every ACKNOWLEDGED entry is present after the reopen
//! replays the log after the store's durable index.
//!
//! This is the crash half of two things WP-1.6a owes:
//!
//! 1. **the local log kill -9 test** — a torn tail truncated, acknowledged
//!    entries all present after reopen and replay;
//! 2. **replay idempotence with the real apply thread** — the reopened,
//!    replayed state is byte-equal to WP-1.4's own `run_workload` of the first
//!    N entries, where N is the index the reopen came up at.
//!
//! The in-process, non-crash cases are in [`super::replicator`].
//!
//! # What is checked
//!
//! - The killed data directory REOPENS: the log recovers, the store recovery
//!   of §11.5 runs, and the replicator replays the log after the store's
//!   durable index without a disagreement.
//! - `applied_index` after reopen (`A`) is at least the highest index the
//!   child acknowledged (`propose` returned `Ok`): no acknowledged entry is
//!   lost, because it was fsynced in the log before it was answered (I4).
//! - The REPLICATED digest after reopen equals the digest of an uninterrupted
//!   `run_workload` of the first `A` entries: the replay reproduced the state
//!   exactly, not merely "some" state.
//!
//! # What is NOT checked here: durability under dropped writes
//!
//! `kill -9` keeps the page cache, so the log's fsynced-vs-unfsynced bytes are
//! all present and this test falsifies the BOOKKEEPING (torn-tail detection,
//! the replay boundary, index contiguity), not unsynced bytes. That is finding
//! R-02 / R-106 restated at the replicator; the dropped-unflushed-writes run
//! belongs to the Linux VM (§13.6, WP-1.8 / WP-1.11).
//!
//! The child is this same test binary re-executed with [`CRASH_DIR_ENV`] set;
//! without it [`crash_child_replicator`] returns at once, so an ordinary
//! `cargo test` writes nothing.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::rsm::apply::{state_digest, StateDigest, SystemClock};
use crate::rsm::entry::encode_entry;
use crate::rsm::replicator::local::{LocalReplicator, NoWaker, OpenConfig};
use crate::rsm::replicator::log::{Fsync, LogOptions};
use crate::rsm::replicator::Replicator;
use crate::rsm::segments;
use crate::rsm::store::{HeedStore, Store};

use super::apply::{run_workload, store_opts, Node, Workload};

const CRASH_DIR_ENV: &str = "QUEEN_RSM_REPL_CRASH_DIR";
const CRASH_N_ENV: &str = "QUEEN_RSM_REPL_CRASH_N";
const CRASH_SEED_ENV: &str = "QUEEN_RSM_REPL_CRASH_SEED";
const CHILD_TEST: &str = "rsm::tests::replicator_crash::crash_child_replicator";

const ENTRIES: u64 = 30_000;
const SEED: u64 = 0x1_6A00_C0DE_0001;
/// Print an ACK line this often, so the parent has a lower bound on what the
/// child acknowledged when the kill lands (printing every ack would dominate
/// the child's time).
const ACK_EVERY: u64 = 128;

/// Where the SIGKILL lands after the child said it was proposing. Spread, so
/// it falls at different points of the store-commit and durable cadences.
const KILL_AFTER: [Duration; 6] = [
    Duration::from_millis(35),
    Duration::from_millis(60),
    Duration::from_millis(90),
    Duration::from_millis(130),
    Duration::from_millis(190),
    Duration::from_millis(270),
];

static REF_SEQ: AtomicU64 = AtomicU64::new(0);

fn child_config(dir: &Path) -> OpenConfig {
    OpenConfig {
        node_id: 1,
        log_dir: dir.join("log"),
        // Small log files, so the crash also crosses rolls; a page-cache-safe
        // barrier (kill -9 keeps the cache anyway, see the header).
        log_opts: LogOptions {
            segment_bytes: 64 << 10,
            fsync: Fsync::Off,
        },
        seg_root: dir.join("seg"),
        seg_opts: segments::Options {
            segment_bytes: 32 << 10,
            fsync: segments::FsyncMode::Data,
            fsync_threads: 1,
            nbuckets: segments::NBUCKETS,
        },
        apply_cfg: crate::rsm::apply::ApplyConfig::default(),
        apply_channel_capacity: 128,
        replay_deadline: Duration::from_secs(60),
    }
}

// ---------------------------------------------------------------------------
// The child
// ---------------------------------------------------------------------------

/// Propose entry after entry until killed. Writes nothing unless the parent
/// set [`CRASH_DIR_ENV`].
#[test]
fn crash_child_replicator() {
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

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("child runtime");
    rt.block_on(async move {
        let store = Arc::new(
            HeedStore::open(&dir.join("store"), &store_opts()).expect("child: open store"),
        );
        let repl = LocalReplicator::open(
            store,
            child_config(&dir),
            Arc::new(NoWaker),
            Arc::new(SystemClock),
        )
        .expect("child: open replicator");

        let mut w = Workload::new(seed);
        for i in 1..=n {
            let e = Bytes::from(encode_entry(&w.next().entry).expect("child: encode"));
            let deadline = Instant::now() + Duration::from_secs(30);
            match repl.propose(e, deadline).await {
                Ok(at) => {
                    if at.index % ACK_EVERY == 0 {
                        say(&format!("ACK {}", at.index));
                    }
                }
                Err(err) => {
                    say(&format!("PROPOSE-ERR {err}"));
                    break;
                }
            }
            if i == 1 {
                say("WRITING");
            }
        }
        say(&format!("EXHAUSTED {n}"));
        std::thread::sleep(Duration::from_secs(120));
        panic!("child: was not killed");
    });
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

    /// Highest index the child printed an ACK for, and whether it ran out of
    /// entries before the kill.
    fn drain(&self) -> (u64, bool) {
        let mut max_ack = 0u64;
        let mut exhausted = false;
        for l in self.lines.try_iter() {
            if let Some(rest) = l.strip_prefix("ACK ") {
                if let Ok(v) = rest.trim().parse::<u64>() {
                    max_ack = max_ack.max(v);
                }
            } else if l.contains("EXHAUSTED") {
                exhausted = true;
            }
        }
        (max_ack, exhausted)
    }

    fn kill(mut self) {
        self.child.kill().expect("kill -9");
        let status = self.child.wait().expect("reap");
        assert!(!status.success(), "the child was supposed to be killed");
    }
}

/// The state an uninterrupted run of the first `n` entries produces.
fn reference(n: u64) -> StateDigest {
    let node = Node::new(&format!(
        "repl-crash-ref-{}",
        REF_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    run_workload(&node, SEED, n, 97)
}

fn digest_and_close(store: Arc<HeedStore>) -> StateDigest {
    let store = Arc::try_unwrap(store).unwrap_or_else(|_| panic!("store still shared"));
    let d = store
        .read(|r| Ok(state_digest(r).expect("digest")))
        .expect("read");
    store.close();
    d
}

#[test]
fn a_killed_node_reopens_and_replays_every_acknowledged_entry() {
    let mut weak = 0;
    for (round, delay) in KILL_AFTER.iter().enumerate() {
        let dir = std::env::temp_dir().join(format!(
            "queen-rsm-repl-crash-{}-{round}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("round dir");

        let kid = Kid::spawn(&dir, ENTRIES, SEED);
        assert!(
            kid.wait_for("WRITING"),
            "round {round}: the child never started proposing"
        );
        std::thread::sleep(*delay);
        let (max_ack, exhausted) = kid.drain();
        if exhausted {
            weak += 1;
        }
        kid.kill();

        // Reopen: the log recovers (torn tail truncated), the store recovers
        // (§11.5), and the replicator replays the log after the durable index.
        let store = Arc::new(
            HeedStore::open(&dir.join("store"), &store_opts()).expect("round: reopen store"),
        );
        let repl = LocalReplicator::open(
            store,
            child_config(&dir),
            Arc::new(NoWaker),
            Arc::new(SystemClock),
        )
        .unwrap_or_else(|e| panic!("round {round}: the killed node did not reopen: {e}"));

        let applied = repl.applied_index();
        assert!(
            applied > 0,
            "round {round}: reopened at applied 0 — the child acknowledged nothing"
        );
        assert!(
            applied >= max_ack,
            "round {round}: the child acknowledged up to {max_ack} but the reopen \
             only reached {applied} — an acknowledged entry was lost"
        );

        let (_stats, store) = repl.shutdown().expect("round: shutdown");
        let got = digest_and_close(store);

        // Replay reproduced the state exactly: byte-equal to a clean run of the
        // first `applied` entries.
        let want = reference(applied);
        assert_eq!(
            got.whole,
            want.whole,
            "round {round} ({delay:?}): the replayed state differs at index {applied}, \
             first at {:?}",
            got.first_difference(&want),
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
    assert!(
        weak < KILL_AFTER.len(),
        "every round killed a child that had already finished proposing: raise ENTRIES \
         or lower the delays"
    );
}
