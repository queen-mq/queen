//! The openraft replicator (`rsm/replicator/raft`).
//!
//! What this file proves:
//!
//! - **openraft's own storage suite passes on the queue-log storage**
//!   ([`openraft_storage_suite_passes_on_the_queue_logs`]): append, read,
//!   truncate, purge, vote, the log state and the startup recovery openraft
//!   computes from them, exactly as openraft specifies them.
//! - **same entries, same state** ([`raft_builds_the_same_state_as_the_local_replicator`]):
//!   every replicated keyspace is byte-equal to the local replicator's after the
//!   same entries, except the two log positions (see [`without_log_positions`]).
//! - **a clean reopen continues** ([`raft_reopens_and_continues`]): the log is
//!   recovered from the queue logs, a new term is elected, and the state after
//!   more entries equals an uninterrupted run.
//! - **kill -9** ([`a_killed_raft_node_reopens_with_every_acknowledged_entry`]):
//!   every acknowledged entry survives, and the state equals a clean run of
//!   exactly the entries that landed.

use std::io;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use openraft::storage::{EntryResponder, RaftStateMachine};
use openraft::testing::log::{StoreBuilder, Suite};
use openraft::type_config::TypeConfigExt;
use openraft::{EntryPayload, OptionalSend, RaftSnapshotBuilder, StorageError};

use crate::rsm::apply::{self, state_digest, StateDigest, SystemClock};
use crate::rsm::entry::encode_entry;
use crate::rsm::qlog::QLogOptions;
use crate::rsm::replicator::local::{LocalReplicator, NoWaker, OpenConfig};
use crate::rsm::replicator::log::{Fsync, LogOptions};
use crate::rsm::replicator::raft::log_store::{LogStore, OpenCfg};
use crate::rsm::replicator::raft::types::{LogId, SnapshotMeta, StoredMembership, TypeConfig};
use crate::rsm::replicator::raft::RaftReplicator;
use crate::rsm::replicator::{Replicator, Role};
use crate::rsm::segments;
use crate::rsm::store::{HeedStore, Store, TypedReads};

use super::apply::{cfg, run_workload, seg_opts, store_opts, Node, Workload, BASE_US};

static SEQ: AtomicU64 = AtomicU64::new(0);

const SEED: u64 = 0x0_0A0F_7000_0001;

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-raft-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

// ---------------------------------------------------------------------------
// 1. openraft's storage suite
// ---------------------------------------------------------------------------

/// A minimal in-memory state machine for the suite, which runs our LOG store
/// beside it (the suite's state machine cases only need applied state,
/// membership and snapshots).
#[derive(Clone, Default)]
struct TestSm(Arc<Mutex<TestSmState>>);

#[derive(Default)]
struct TestSmState {
    last_applied: Option<LogId>,
    membership: StoredMembership,
    snapshot: Option<(SnapshotMeta, TestSnap)>,
}

#[derive(Clone, Debug)]
struct TestSnap;

type TestSnapshot = openraft::alias::SnapshotOf<TypeConfig, TestSnap>;

impl RaftStateMachine<TypeConfig> for TestSm {
    type SnapshotData = TestSnap;
    type SnapshotBuilder = TestSm;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
        let s = self.0.lock().expect("sm");
        Ok((s.last_applied, s.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        while let Some((e, responder)) = entries.try_next().await? {
            {
                let mut s = self.0.lock().expect("sm");
                s.last_applied = Some(e.log_id);
                if let EntryPayload::Membership(m) = &e.payload {
                    s.membership = StoredMembership::new(Some(e.log_id), m.clone());
                }
            }
            if let Some(r) = responder {
                r.send(());
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> TestSm {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: TestSnap,
    ) -> Result<(), io::Error> {
        let mut s = self.0.lock().expect("sm");
        s.last_applied = meta.last_log_id;
        s.membership = meta.last_membership.clone();
        s.snapshot = Some((meta.clone(), snapshot));
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<TestSnapshot>, io::Error> {
        let s = self.0.lock().expect("sm");
        Ok(s.snapshot
            .clone()
            .map(|(meta, snapshot)| TestSnapshot { meta, snapshot }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for TestSm {
    type SnapshotData = TestSnap;

    async fn build_snapshot(&mut self) -> Result<TestSnapshot, io::Error> {
        let mut s = self.0.lock().expect("sm");
        let meta = SnapshotMeta {
            last_log_id: s.last_applied,
            last_membership: s.membership.clone(),
        };
        s.snapshot = Some((meta.clone(), TestSnap));
        Ok(TestSnapshot {
            meta,
            snapshot: TestSnap,
        })
    }
}

/// Removes the scratch directory when the suite drops a case.
struct Guard(PathBuf);

impl Drop for Guard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct QlogStoreBuilder;

impl StoreBuilder<TypeConfig, LogStore, TestSm, Guard> for QlogStoreBuilder {
    async fn build(&self) -> Result<(Guard, LogStore, TestSm), StorageError<TypeConfig>> {
        let dir = scratch("suite");
        let opened = LogStore::open(OpenCfg {
            qlog_root: dir.join("qlog"),
            qopts: QLogOptions::testing(1 << 20),
            state_dir: dir.join("raft"),
            lookup: Arc::new(|_| Ok(None)),
            durable_index: 0,
            applied: None,
            qlog_durable_index: 0,
            qlog_tail: Box::new(|_| Ok(None)),
            poison: Arc::new(Mutex::new(None)),
            cache_cap: 1 << 30,
        })
        .map_err(|e| StorageError::write(TypeConfig::err_from_error(&e)))?;
        // Detached: the writer exits once the suite drops the store.
        drop(opened.writer);
        Ok((Guard(dir), opened.store, TestSm::default()))
    }
}

#[test]
fn openraft_storage_suite_passes_on_the_queue_logs() {
    TypeConfig::run(async {
        Suite::test_all(QlogStoreBuilder)
            .await
            .expect("openraft's storage suite on the queue-log storage");
    });
}

// ---------------------------------------------------------------------------
// The replicator over the real apply thread
// ---------------------------------------------------------------------------

fn repl_config(dir: &Path) -> OpenConfig {
    OpenConfig {
        node_id: 1,
        log_dir: dir.join("log"),
        log_opts: LogOptions {
            segment_bytes: 64 << 10,
            fsync: Fsync::Off,
        },
        seg_root: dir.join("seg"),
        seg_opts: seg_opts(),
        apply_cfg: apply::ApplyConfig {
            qlog: true,
            ..cfg()
        },
        apply_channel_capacity: 64,
        replay_deadline: Duration::from_secs(30),
        writer_pipeline: false,
    }
}

fn open_raft(dir: &Path, cfg: OpenConfig) -> RaftReplicator<HeedStore> {
    let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
    RaftReplicator::open(store, cfg, Arc::new(NoWaker), Arc::new(SystemClock))
        .expect("open the openraft replicator")
}

fn workload(seed: u64, n: u64) -> Vec<Bytes> {
    let mut w = Workload::new(seed);
    (0..n)
        .map(|_| Bytes::from(encode_entry(&w.next().entry).expect("encode")))
        .collect()
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

fn digest_and_close(store: Arc<HeedStore>) -> StateDigest {
    let store = Arc::try_unwrap(store).unwrap_or_else(|_| panic!("the store is still shared"));
    let d = store
        .read(|r| Ok(state_digest(r).expect("digest")))
        .expect("read");
    store.close();
    d
}

/// Every replicated keyspace but the two that record LOG POSITIONS: `meta`
/// (the applied index and term) and `groups` (`reg_index`, the index of the
/// entry that registered the group, which `new` subscriptions seed from).
/// openraft's own entries shift every index and openraft has terms, so those
/// differ by design between the replicators — identically on every node of
/// one log. [`group_rows`] checks the rest of each group row.
fn without_log_positions(d: &StateDigest) -> Vec<(&'static str, u128, u64)> {
    d.per_keyspace
        .iter()
        .filter(|(name, _, _)| *name != "meta" && *name != "groups")
        .cloned()
        .collect()
}

/// The workload's group rows with the log position set aside.
fn group_rows(store: &HeedStore) -> Vec<String> {
    use super::apply::{QUEUE, TENANT};
    store
        .read(|r| {
            let mut out = Vec::new();
            for g in ["g1", "g2"] {
                let row = r.group(TENANT, QUEUE, g)?;
                out.push(format!("{g}: {:?}", row.map(|r| (r.meta, r.reg_effect))));
            }
            Ok(out)
        })
        .expect("read group rows")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_builds_the_same_state_as_the_local_replicator() {
    const N: u64 = 400;
    let entries = workload(SEED, N);

    let local = {
        let dir = scratch("parity-local");
        let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
        let repl = LocalReplicator::open(
            store,
            repl_config(&dir),
            Arc::new(NoWaker),
            Arc::new(SystemClock),
        )
        .expect("open the local replicator");
        for e in entries.clone() {
            repl.propose(e, deadline()).await.expect("local propose");
        }
        let (_s, store) = repl.shutdown().expect("shutdown");
        let groups = group_rows(&store);
        let d = digest_and_close(store);
        let _ = std::fs::remove_dir_all(&dir);
        (d, groups)
    };

    let raft = {
        let dir = scratch("parity-raft");
        let repl = open_raft(&dir, repl_config(&dir));
        assert!(
            matches!(repl.role(), Role::Leader { .. }),
            "open returns a ready leader"
        );
        let mut last = 0;
        for e in entries {
            let at = repl.propose(e, deadline()).await.expect("raft propose");
            assert!(at.index > last, "indexes ascend");
            last = at.index;
        }
        assert_eq!(
            repl.applied_index(),
            last,
            "propose answers after local apply"
        );
        let (_s, store) = repl.shutdown().expect("shutdown");
        let groups = group_rows(&store);
        let d = digest_and_close(store);
        let _ = std::fs::remove_dir_all(&dir);
        (d, groups)
    };

    assert_eq!(
        without_log_positions(&raft.0),
        without_log_positions(&local.0),
        "the openraft replicator built different replicated state from the same entries"
    );
    assert_eq!(
        raft.1, local.1,
        "the group rows differ beyond their log position"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_reopens_and_continues() {
    const N: u64 = 150;
    let entries = workload(SEED, 2 * N);
    let dir = scratch("reopen");

    let first_term = {
        let repl = open_raft(&dir, repl_config(&dir));
        let Role::Leader { term } = repl.role() else {
            panic!("not a ready leader");
        };
        for e in &entries[..N as usize] {
            repl.propose(e.clone(), deadline()).await.expect("propose");
        }
        let (_s, store) = repl.shutdown().expect("shutdown");
        Arc::try_unwrap(store)
            .unwrap_or_else(|_| panic!("store shared"))
            .close();
        term
    };

    let repl = open_raft(&dir, repl_config(&dir));
    let Role::Leader { term } = repl.role() else {
        panic!("the reopened node is not a ready leader");
    };
    assert!(
        term > first_term,
        "a reopen elects a new term ({term} after {first_term})"
    );
    let now = repl
        .store_for_test()
        .read(|r| r.last_now_us())
        .expect("read last_now");
    assert_eq!(
        now,
        BASE_US + N as i64 * 1_000,
        "the reopen replayed exactly the first N entries"
    );
    for e in &entries[N as usize..] {
        repl.propose(e.clone(), deadline())
            .await
            .expect("propose after reopen");
    }
    let (_s, store) = repl.shutdown().expect("shutdown");
    let got = digest_and_close(store);
    let want = run_workload(&Node::new("raft-reopen-ref"), SEED, 2 * N, 97);
    assert_eq!(
        without_log_positions(&got),
        without_log_positions(&want),
        "the state after a reopen and more entries differs from an uninterrupted run"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// kill -9
// ---------------------------------------------------------------------------

const CRASH_DIR_ENV: &str = "QUEEN_RSM_RAFT_CRASH_DIR";
const CHILD_TEST: &str = "rsm::tests::raft::raft_crash_child";
const CRASH_ENTRIES: u64 = 30_000;
const CRASH_SEED: u64 = 0x0_0A0F_C0DE_0001;
const ACK_EVERY: u64 = 64;
const KILL_AFTER: [Duration; 5] = [
    Duration::from_millis(40),
    Duration::from_millis(90),
    Duration::from_millis(150),
    Duration::from_millis(230),
    Duration::from_millis(330),
];

fn crash_config(dir: &Path) -> OpenConfig {
    OpenConfig {
        // Small queue-log files, so a kill also lands across rolls.
        seg_opts: segments::Options {
            segment_bytes: 32 << 10,
            fsync: segments::FsyncMode::Data,
            fsync_threads: 1,
            nbuckets: segments::NBUCKETS,
        },
        apply_cfg: apply::ApplyConfig {
            qlog: true,
            ..apply::ApplyConfig::default()
        },
        replay_deadline: Duration::from_secs(60),
        ..repl_config(dir)
    }
}

/// Propose entry after entry through the openraft replicator until killed.
/// Writes nothing unless the parent set [`CRASH_DIR_ENV`].
#[test]
fn raft_crash_child() {
    let Ok(dir) = std::env::var(CRASH_DIR_ENV) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("child runtime");
    rt.block_on(async move {
        let repl = open_raft(&dir, crash_config(&dir));
        let mut w = Workload::new(CRASH_SEED);
        for i in 1..=CRASH_ENTRIES {
            let e = Bytes::from(encode_entry(&w.next().entry).expect("encode"));
            match repl.propose(e, deadline()).await {
                Ok(_) => {
                    if i % ACK_EVERY == 0 {
                        say(&format!("ACK {i}"));
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
        say("EXHAUSTED");
        std::thread::sleep(Duration::from_secs(120));
        panic!("child: was not killed");
    });
}

fn say(line: &str) {
    use std::io::Write;
    println!("{line}");
    let _ = std::io::stdout().flush();
}

struct Kid {
    child: Child,
    lines: mpsc::Receiver<String>,
}

impl Kid {
    fn spawn(dir: &Path) -> Kid {
        let exe = std::env::current_exe().expect("test binary");
        let mut child = Command::new(&exe)
            .args(["--exact", CHILD_TEST, "--nocapture", "--test-threads", "1"])
            .env(CRASH_DIR_ENV, dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the child");
        let stdout = child.stdout.take().expect("piped stdout");
        let (tx, lines) = mpsc::channel::<String>();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        Kid { child, lines }
    }

    fn wait_for(&self, marker: &str) -> bool {
        let end = Instant::now() + Duration::from_secs(60);
        while Instant::now() < end {
            match self.lines.recv_timeout(Duration::from_secs(5)) {
                Ok(line) if line.contains(marker) => return true,
                Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => return false,
            }
        }
        false
    }

    /// The highest acknowledged entry count, and whether the child errored.
    fn drain(&self) -> (u64, Option<String>) {
        let mut max_ack = 0;
        let mut err = None;
        for l in self.lines.try_iter() {
            if let Some(rest) = l.strip_prefix("ACK ") {
                if let Ok(v) = rest.trim().parse::<u64>() {
                    max_ack = max_ack.max(v);
                }
            } else if l.starts_with("PROPOSE-ERR") {
                err = Some(l);
            }
        }
        (max_ack, err)
    }

    fn kill(mut self) {
        self.child.kill().expect("kill -9");
        let status = self.child.wait().expect("reap");
        assert!(!status.success(), "the child was supposed to be killed");
    }
}

#[test]
fn a_killed_raft_node_reopens_with_every_acknowledged_entry() {
    for (round, delay) in KILL_AFTER.iter().enumerate() {
        let dir = scratch(&format!("crash-{round}"));
        let kid = Kid::spawn(&dir);
        assert!(
            kid.wait_for("WRITING"),
            "round {round}: the child never started proposing"
        );
        std::thread::sleep(*delay);
        let (max_ack, err) = kid.drain();
        assert!(err.is_none(), "round {round}: the child failed: {err:?}");
        kid.kill();

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let (landed, got) = rt.block_on(async {
            let repl = open_raft(&dir, crash_config(&dir));
            assert!(
                matches!(repl.role(), Role::Leader { .. }),
                "round {round}: the killed node did not come back as a ready leader"
            );
            let now = repl
                .store_for_test()
                .read(|r| r.last_now_us())
                .expect("read last_now");
            let landed = ((now - BASE_US) / 1_000).max(0) as u64;
            let (_s, store) = repl.shutdown().expect("shutdown");
            (landed, digest_and_close(store))
        });
        assert!(
            landed >= max_ack,
            "round {round} ({delay:?}): the child acknowledged {max_ack} entries but only \
             {landed} survived the kill"
        );
        let want = run_workload(
            &Node::new(&format!("raft-crash-ref-{round}")),
            CRASH_SEED,
            landed,
            97,
        );
        assert_eq!(
            without_log_positions(&got),
            without_log_positions(&want),
            "round {round} ({delay:?}): the recovered state differs from a clean run of the \
             {landed} entries that landed"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
