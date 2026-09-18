//! WP-1.6b — the cycle driver ([`crate::rsm::batcher`]).
//!
//! Two halves:
//!
//! - **the [`FakeReplicator`] branches** — the §7.1 error semantics a single
//!   real node never produces: a delayed commit, a fatal commit failure,
//!   `NotLeader`, a `Timeout` that keeps the entry in flight and later commits
//!   (I3: no duplicate offsets), a step-down with four entries in flight (the
//!   log they built applies to a consistent state), and a request-id replay
//!   across cycles (I6).
//! - **the real path** — an end-to-end push / pop / ack through the batcher over
//!   the [`LocalReplicator`] and WP-1.4's apply thread, a restart, and recovery;
//!   and an `#[ignore]` throughput smoke of four in flight against one (laptop
//!   numbers, §0.3, smoke only).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::rsm::apply::{
    self, state_digest, Applier, Committed as ApplyCommitted, NoNotify, SystemClock,
};
use crate::rsm::batcher::{Batcher, BatcherConfig, Command, CommandTx, Reply, Submission};
use crate::rsm::entry::{decode_entry, Outcome, PushVerdict};
use crate::rsm::planner::{
    AckCommand, AckItem, AckStatus, AckTarget, PopCommand, PushCommand, SubIntent,
};
use crate::rsm::replicator::fake::{FakeReplicator, Step};
use crate::rsm::replicator::local::{LocalReplicator, NoWaker, OpenConfig};
use crate::rsm::replicator::log::{Fsync, LogOptions};
use crate::rsm::replicator::{ProposeError, Replicator};
use crate::rsm::store::{HeedStore, Store, TypedReads};

use super::apply::{cfg, seg_opts, store_opts};
use super::planner_harness::{item, qcfg, rid, TENANT};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-batcher-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

// ---------------------------------------------------------------------------
// Command builders (the phase-1 subset the planner already covers)
// ---------------------------------------------------------------------------

fn push(id: u64, queue: &str, partition: &str, txns: &[&str]) -> Command {
    Command::Push(PushCommand {
        request_id: rid(id),
        tenant: TENANT.to_string(),
        queue: queue.to_string(),
        partition: partition.to_string(),
        items: txns.iter().map(|t| item(t)).collect(),
        create_cfg: qcfg(),
    })
}

fn pop_pinned(id: u64, queue: &str, partition: &str, group: &str, worker: &str) -> Command {
    Command::PopPinned(PopCommand {
        request_id: rid(id),
        tenant: TENANT.to_string(),
        queue: queue.to_string(),
        partition: Some(partition.to_string()),
        group: group.to_string(),
        worker: worker.to_string(),
        budget: 100,
        max_parts: 10,
        lease_seconds: 60,
        auto_ack: false,
        conflate: false,
        sub: SubIntent {
            mode: "all".to_string(),
            from_us: None,
            now: false,
        },
        skip_window_debounce: false,
        namespace: String::new(),
        task: String::new(),
        create_cfg: Some(qcfg()),
    })
}

fn ack_ok(id: u64, pid: u64, queue: &str, group: &str, worker: &str, txns: &[&str]) -> Command {
    Command::Ack(AckCommand {
        request_id: rid(id),
        targets: vec![AckTarget {
            pid,
            tenant: TENANT.to_string(),
            queue: queue.to_string(),
            group: group.to_string(),
            worker: worker.to_string(),
            items: txns
                .iter()
                .map(|t| AckItem {
                    hash: crate::util::txn_hash128(t),
                    status: AckStatus::Ok,
                    error: None,
                    snapshot: None,
                })
                .collect(),
        }],
    })
}

// ---------------------------------------------------------------------------
// Reply helpers
// ---------------------------------------------------------------------------

async fn submit(tx: &CommandTx, command: Command) -> Reply {
    let (sub, rx) = Submission::new(command);
    tx.send(sub).await.expect("send command");
    rx.await.expect("await reply")
}

fn done(reply: &Reply) -> (&Outcome, Option<u64>) {
    match reply {
        Reply::Done { outcome, at } => (outcome, at.map(|a| a.index)),
        other => panic!("expected Done, got {other:?}"),
    }
}

/// The `(pid, offset)` of the first item of a push outcome, which must be a
/// `Created` verdict.
fn push_created(reply: &Reply) -> (u64, u64) {
    match done(reply).0 {
        Outcome::Push(p) => match &p.items[0] {
            PushVerdict::Created { pid, offset, .. } => (*pid, *offset),
            other => panic!("expected Created, got {other:?}"),
        },
        other => panic!("expected Push outcome, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The fake fixture: a real (frozen) store, a scripted replicator, the batcher.
// ---------------------------------------------------------------------------

struct FakeFixture {
    tx: CommandTx,
    handle: tokio::task::JoinHandle<()>,
    fake: Arc<FakeReplicator>,
    dir: PathBuf,
    // Kept so the store env is not the batcher's only reference.
    _store: Arc<HeedStore>,
}

impl FakeFixture {
    fn open(tag: &str, cfg: BatcherConfig) -> FakeFixture {
        let dir = scratch(tag);
        let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
        let fake = Arc::new(FakeReplicator::new(1));
        let batcher = Batcher::new(store.clone(), fake.clone(), cfg);
        let (tx, handle) = batcher.spawn();
        FakeFixture {
            tx,
            handle,
            fake,
            dir,
            _store: store,
        }
    }

    /// Drop the command sender and wait for the driver to finish.
    async fn close(self) {
        drop(self.tx);
        let _ = self.handle.await;
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn small_pipeline(pipeline: usize, propose_ms: u64) -> BatcherConfig {
    BatcherConfig {
        pipeline,
        batch_max_cmds: 1,
        propose_ms,
        // The tests drive the expiry step explicitly where they want it.
        request_expire_every_ms: 3_600_000,
        ..BatcherConfig::default()
    }
}

// ---------------------------------------------------------------------------
// FakeReplicator branches
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delayed_commit_is_answered_when_it_lands() {
    let fx = FakeFixture::open("delayed", small_pipeline(4, 5_000));
    // A slow but healthy commit.
    fx.fake.set_default(Step::After(Duration::from_millis(20)));

    let started = Instant::now();
    let reply = submit(&fx.tx, push(1, "q", "p0", &["a"])).await;
    assert!(
        started.elapsed() >= Duration::from_millis(15),
        "waited for the delayed commit"
    );
    let (_pid, offset) = push_created(&reply);
    assert_eq!(offset, 0, "fresh partition, first offset");
    assert_eq!(done(&reply).1, Some(1), "answered with the commit index");

    fx.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fatal_commit_failure_retries_the_waiter_and_stops_the_driver() {
    let fx = FakeFixture::open("fatal", small_pipeline(4, 5_000));
    fx.fake
        .push_step(Step::Fail(ProposeError::Fatal("disk is gone".into())));

    let reply = submit(&fx.tx, push(1, "q", "p0", &["a"])).await;
    assert!(
        matches!(reply, Reply::Retry { .. }),
        "a fatal failure retries the waiter, got {reply:?}"
    );
    // The driver stopped itself; the join completes on its own.
    let _ = fx.handle.await;
    let _ = std::fs::remove_dir_all(&fx.dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn not_leader_retries_the_waiter_with_the_hint() {
    let fx = FakeFixture::open("notleader", small_pipeline(4, 5_000));
    fx.fake
        .push_step(Step::Fail(ProposeError::NotLeader { hint: Some(9) }));

    let reply = submit(&fx.tx, push(1, "q", "p0", &["a"])).await;
    match reply {
        Reply::Retry { hint } => assert_eq!(hint, Some(9), "the leader hint is carried"),
        other => panic!("expected Retry, got {other:?}"),
    }
    fx.close().await;
}

/// I3: a `Timeout` keeps the entry in flight, so a later command does not
/// reuse its offset, and the timed-out entry still commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timeout_keeps_the_entry_in_flight_without_duplicate_offsets() {
    let fx = FakeFixture::open("timeout", small_pipeline(2, 60));
    // Entry 1: the caller sees Timeout, but the fake commits it late (I3).
    fx.fake
        .push_step(Step::TimeoutThenCommit(Duration::from_millis(15)));
    // Everything after commits at once.
    fx.fake.set_default(Step::Now);

    // The first push times out and is answered Retry.
    let first = submit(&fx.tx, push(1, "q", "p0", &["a"])).await;
    assert!(
        matches!(first, Reply::Retry { .. }),
        "a timeout answers Retry, got {first:?}"
    );

    // The second push (a different transaction to the SAME partition) must not
    // reuse offset 0: the timed-out entry is still folded into the overlay.
    let second = submit(&fx.tx, push(2, "q", "p0", &["b"])).await;
    let (_pid, offset) = push_created(&second);
    assert_eq!(
        offset, 1,
        "the second push allocated the offset after the entry kept in flight"
    );

    fx.close().await;
}

/// I3: four entries in flight, then a step-down. Every waiter retries, and the
/// log the batcher built applies to a consistent, gapless state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn step_down_with_four_in_flight_leaves_committed_state_equal_to_the_log() {
    let fx = FakeFixture::open("stepdown", small_pipeline(4, 60_000));
    // Every propose appends at once but does not commit until well after the
    // step-down, so four stay in flight.
    fx.fake.set_default(Step::After(Duration::from_secs(30)));

    // Four pushes to ONE partition, distinct transactions: the log entries must
    // chain offsets 0..3 through the overlay.
    let mut rxs = Vec::new();
    for (i, txn) in ["a", "b", "c", "d"].iter().enumerate() {
        let (sub, rx) = Submission::new(push(100 + i as u64, "q", "p0", &[txn]));
        fx.tx.send(sub).await.expect("send");
        rxs.push(rx);
    }

    // Wait until all four have been proposed (in flight).
    let end = Instant::now() + Duration::from_secs(5);
    while fx.fake.proposal_count() < 4 && Instant::now() < end {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(fx.fake.proposal_count(), 4, "four entries in flight");

    // Step down: every overlay is dropped together and every waiter retries.
    fx.fake.step_down(Some(2));
    for rx in rxs {
        let reply = rx.await.expect("reply");
        assert!(
            matches!(reply, Reply::Retry { hint: Some(2) }),
            "a step-down retries each waiter with the hint, got {reply:?}"
        );
    }

    // The log the batcher built (the four proposed entries) applies to a
    // consistent state: partition p0 with a gapless tail at offset 3.
    let proposals = fx.fake.proposals();
    assert_eq!(proposals.len(), 4);
    let apply_dir = scratch("stepdown-apply");
    {
        let store = HeedStore::open(&apply_dir.join("store"), &store_opts()).expect("store");
        let (mut a, _rec) = Applier::open(
            &store,
            &apply_dir.join("seg"),
            seg_opts(),
            cfg(),
            Arc::new(NoNotify),
        )
        .expect("open applier");
        for (i, bytes) in proposals.iter().enumerate() {
            let entry = decode_entry(bytes).expect("decode");
            a.apply(&ApplyCommitted {
                index: i as u64 + 1,
                term: 1,
                entry,
            })
            .expect("apply");
        }
        a.durable_point().expect("durable");
        drop(a);
        let last = store
            .read(|r| r.partition(1u64))
            .expect("read")
            .expect("partition p0 exists");
        assert_eq!(
            last.last_offset, 3,
            "four single-message pushes chained to a gapless offset 3"
        );
        store.close();
    }
    let _ = std::fs::remove_dir_all(&apply_dir);

    fx.close().await;
}

/// I6: a retry with the same request id returns the recorded outcome and plans
/// nothing (here across cycles via the in-flight overlay, §5.4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_id_replay_returns_the_recorded_outcome_and_plans_nothing() {
    let fx = FakeFixture::open("replay", small_pipeline(4, 5_000));
    fx.fake.set_default(Step::Now);

    let first = submit(&fx.tx, push(7, "q", "p0", &["a"])).await;
    let (_pid, offset0) = push_created(&first);
    assert_eq!(offset0, 0);
    assert_eq!(fx.fake.proposal_count(), 1);

    // The SAME request id (a forwarding retry): it is found in the entry still
    // in flight and answered from its outcome; nothing new is proposed.
    let replay = submit(&fx.tx, push(7, "q", "p0", &["a"])).await;
    let (_pid, offset1) = push_created(&replay);
    assert_eq!(offset1, 0, "the replay returns the original offset");
    assert_eq!(
        fx.fake.proposal_count(),
        1,
        "the replay planned and proposed nothing"
    );

    fx.close().await;
}

/// §10.1: the driver proposes a `RequestIdsExpire` step on its own cadence,
/// with no client command behind it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_driver_proposes_a_request_id_expiry_step_on_its_cadence() {
    let cfg = BatcherConfig {
        pipeline: 4,
        request_expire_every_ms: 25,
        ..BatcherConfig::default()
    };
    let fx = FakeFixture::open("expire", cfg);
    fx.fake.set_default(Step::Now);

    // No commands are sent: wait for the cadence to fire.
    let end = Instant::now() + Duration::from_secs(2);
    while fx.fake.proposal_count() == 0 && Instant::now() < end {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        fx.fake.proposal_count() >= 1,
        "the expiry step was proposed with no client command"
    );

    // The proposed entry carries exactly one RequestIdsExpire effect.
    let first = fx.fake.proposals().remove(0);
    let entry = decode_entry(&first).expect("decode");
    assert_eq!(entry.effects.len(), 1);
    assert!(
        matches!(
            entry.effects[0],
            crate::rsm::effect::Effect::RequestIdsExpire { .. }
        ),
        "the step is a RequestIdsExpire, got {:?}",
        entry.effects[0]
    );

    fx.close().await;
}

// ---------------------------------------------------------------------------
// The real path: LocalReplicator + the apply thread
// ---------------------------------------------------------------------------

fn open_repl(dir: &Path, store: Arc<HeedStore>) -> LocalReplicator<HeedStore> {
    LocalReplicator::open(
        store,
        OpenConfig {
            node_id: 1,
            log_dir: dir.join("log"),
            log_opts: LogOptions {
                segment_bytes: 64 << 10,
                fsync: Fsync::Off,
            },
            seg_root: dir.join("seg"),
            seg_opts: seg_opts(),
            apply_cfg: cfg(),
            apply_channel_capacity: 64,
            replay_deadline: Duration::from_secs(30),
        },
        Arc::new(NoWaker),
        Arc::new(SystemClock),
    )
    .expect("open replicator")
}

/// Read the replicated digest of a store, then close it (heed refuses a second
/// open in one process).
fn digest_and_close(store: Arc<HeedStore>) -> apply::StateDigest {
    let store = Arc::try_unwrap(store).unwrap_or_else(|_| panic!("store still shared"));
    let d = store
        .read(|r| Ok(state_digest(r).expect("digest")))
        .expect("read");
    store.close();
    d
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn end_to_end_push_pop_ack_then_restart_and_recover() {
    let dir = scratch("e2e");

    let (digest1, applied1) = {
        let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
        let repl = Arc::new(open_repl(&dir, store.clone()));
        let batcher = Batcher::new(store.clone(), repl.clone(), small_pipeline(4, 5_000));
        let (tx, handle) = batcher.spawn();

        // Push three messages.
        let p = submit(&tx, push(1, "q", "p0", &["a", "b", "c"])).await;
        let (pid, first_offset) = push_created(&p);
        assert_eq!(first_offset, 0, "gapless from 0");

        // Pop them (first contact registers the group and seeds `all`).
        let popped = submit(&tx, pop_pinned(2, "q", "p0", "g", "w1")).await;
        let claim = match done(&popped).0 {
            Outcome::Pop(o) => {
                assert_eq!(o.claims.len(), 1, "one claim");
                o.claims[0].clone()
            }
            other => panic!("expected Pop, got {other:?}"),
        };
        assert_eq!(claim.pid, pid);
        assert_eq!(claim.start_offset, 0);
        assert_eq!(claim.end_offset, 2);

        // Ack all three.
        let acked = submit(&tx, ack_ok(3, pid, "q", "g", "w1", &["a", "b", "c"])).await;
        match done(&acked).0 {
            Outcome::Ack(o) => {
                assert_eq!(o.results.len(), 1);
                assert_eq!(o.results[0].committed, 2, "cursor advanced past the batch");
            }
            other => panic!("expected Ack, got {other:?}"),
        }

        // A second pop finds nothing pending: an empty outcome, answered once
        // the pipeline the plan read has drained.
        let empty = submit(&tx, pop_pinned(4, "q", "p0", "g", "w1")).await;
        match done(&empty).0 {
            Outcome::Pop(o) => assert!(o.claims.is_empty(), "nothing left to claim"),
            other => panic!("expected empty Pop, got {other:?}"),
        }

        // Drain the driver, then reclaim the replicator and shut it down.
        drop(tx);
        handle.await.expect("driver join");
        let repl = Arc::try_unwrap(repl)
            .unwrap_or_else(|_| panic!("replicator still shared after the driver stopped"));
        let applied1 = repl.applied_index();
        let (_stats, store2) = repl.shutdown().expect("shutdown");
        drop(store); // the test's clone, so the returned Arc is sole
        (digest_and_close(store2), applied1)
    };

    // Run 2: reopen the SAME data directory and recover.
    {
        let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("reopen"));
        let repl = open_repl(&dir, store.clone());
        assert_eq!(
            repl.applied_index(),
            applied1,
            "recovery reopened at the same applied index"
        );
        let (_s, store2) = repl.shutdown().expect("shutdown");
        drop(store);
        let digest2 = digest_and_close(store2);
        assert_eq!(
            digest2.whole,
            digest1.whole,
            "the recovered state matches, first difference at {:?}",
            digest2.first_difference(&digest1),
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Throughput smoke: four in flight vs one (laptop, §0.3 — not a quotable number)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "laptop throughput smoke; the numbers to quote come from the VM (§0.3)"]
async fn throughput_four_in_flight_vs_one() {
    async fn run(pipeline: usize, n: u64) -> (f64, u128) {
        let dir = scratch(&format!("smoke-p{pipeline}"));
        let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
        let repl = Arc::new(open_repl(&dir, store.clone()));
        let cfg = BatcherConfig {
            pipeline,
            batch_max_cmds: 1,
            request_expire_every_ms: 3_600_000,
            ..BatcherConfig::default()
        };
        let batcher = Batcher::new(store.clone(), repl.clone(), cfg);
        let (tx, handle) = batcher.spawn();

        let started = Instant::now();
        // Submit everything, then await every reply, so several proposes are in
        // flight at once when the pipeline allows it.
        let mut rxs = Vec::with_capacity(n as usize);
        for i in 0..n {
            let (sub, rx) = Submission::new(push(1000 + i, "q", "p0", &[&format!("t{i}")]));
            tx.send(sub).await.expect("send");
            rxs.push(rx);
        }
        let mut lat: Vec<u128> = Vec::with_capacity(n as usize);
        for rx in rxs {
            let t = Instant::now();
            let _ = rx.await.expect("reply");
            lat.push(t.elapsed().as_micros());
        }
        let elapsed = started.elapsed();
        let eps = n as f64 / elapsed.as_secs_f64();

        drop(tx);
        handle.await.expect("join");
        let repl = Arc::try_unwrap(repl).unwrap_or_else(|_| panic!("shared"));
        let (_s, store2) = repl.shutdown().expect("shutdown");
        drop(store);
        drop(store2);
        let _ = std::fs::remove_dir_all(&dir);

        lat.sort_unstable();
        let p50 = lat[lat.len() / 2];
        (eps, p50)
    }

    const N: u64 = 400;
    let (eps1, p50_1) = run(1, N).await;
    let (eps4, p50_4) = run(4, N).await;
    eprintln!(
        "throughput smoke (laptop, N={N}): 1-in-flight {eps1:.0} entries/s p50 {p50_1} µs; \
         4-in-flight {eps4:.0} entries/s p50 {p50_4} µs"
    );
}
