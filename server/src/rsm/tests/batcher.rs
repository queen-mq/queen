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
//! - **the WP-1.11 F-1 submission-order guards** — that the log index follows
//!   plan order under the pipeline (I5). The pure fake-number reorder the old
//!   spawn-per-propose form allowed is a scheduler race that does not reproduce
//!   on a laptop, so the DETERMINISTIC guard
//!   ([`every_propose_submits_on_the_one_driver_task_in_plan_order`]) witnesses
//!   the driver task the submission runs on — one id on the fix, N distinct ids
//!   on the old form — while the fake and real-log ordering tests are positive
//!   PASSES of the fixed path.

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
use crate::rsm::replicator::{
    AppliedAt, Membership, MembershipChange, NodeId, ProposeError, ReplError, ReplMetrics,
    Replicator, Role,
};
use crate::rsm::store::{HeedStore, Store, TypedReads};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::watch;

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

/// §7.1 / I13, the D4 "drop every overlay together on any propose error" path,
/// reached by a per-entry `Poll::Ready(Err)` ARRIVING MID-PIPELINE — not by a
/// role-watch step-down. A depth-4 pipeline of healthy, slow-committing entries
/// is left concurrently in flight (already handed to the log — the fake has
/// appended them and consumed their indexes), then ONE later `propose` resolves
/// `NotLeader`. That single error must drop the WHOLE overlay: every waiter,
/// including the three entries that were already in flight, is answered `Retry`
/// with the hint, and the driver pauses (a retryable loss, not a Fatal stop).
///
/// Covers the residual gap the WP-1.11 refuters named: the other error tests
/// fail on the FIRST propose (nothing else in flight), and the depth-4 drop was
/// otherwise exercised only via `step_down`, never via a per-entry error landing
/// while a full pipeline of other entries is live.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_propose_error_mid_pipeline_drops_the_whole_depth_four_overlay() {
    let fx = FakeFixture::open("err-mid-pipeline", small_pipeline(4, 5_000));
    // Three healthy-but-slow commits stay in flight for the whole test (the
    // fake caps the sleep at the propose deadline, so they never resolve before
    // the error drops them), then the fourth propose refuses `NotLeader`.
    fx.fake.push_steps([
        Step::After(Duration::from_secs(30)),
        Step::After(Duration::from_secs(30)),
        Step::After(Duration::from_secs(30)),
        Step::Fail(ProposeError::NotLeader { hint: Some(7) }),
    ]);

    // Submit all four before awaiting, so three proposes are genuinely in flight
    // (in-order, one entry each) when the fourth's error lands.
    let mut rxs = Vec::with_capacity(4);
    for i in 0..4u64 {
        let (sub, rx) = Submission::new(push(1_000 + i, "q", "p0", &[&format!("t{i}")]));
        fx.tx.send(sub).await.expect("send");
        rxs.push(rx);
    }

    // Every waiter — the three that were in flight AND the one that erred — is
    // answered Retry with the hint: the overlay was dropped as a group.
    for (i, rx) in rxs.into_iter().enumerate() {
        let reply = rx.await.expect("reply");
        match reply {
            Reply::Retry { hint } => assert_eq!(
                hint,
                Some(7),
                "push {i}: the whole overlay drops to Retry with the NotLeader hint"
            ),
            other => panic!("push {i}: expected Retry (overlay dropped), got {other:?}"),
        }
    }

    // All four proposes reached the replicator; the three healthy ones had
    // already been appended to the log (indexes consumed) before the error
    // dropped them — the point of the D4 path.
    assert_eq!(fx.fake.proposal_count(), 4, "all four proposes ran");
    assert_eq!(
        fx.fake.proposals().len(),
        3,
        "the three in-flight entries were appended before the NotLeader dropped the overlay"
    );

    // A NotLeader is a retryable loss: the driver paused, it did not stop as a
    // Fatal would. Closing drains cleanly with nothing left in flight.
    fx.close().await;
}

/// I3: a `Timeout` keeps the entry in flight, so a later command does not
/// reuse its offset, and the timed-out entry still commits.
///
/// This pins the pure propose-future timeout path (`QUEEN_RAFT_DRIVER_NOTIFY`
/// OFF, the semantics a backend that exposes no applied-index wake takes — the
/// phase-3 openraft path, GH#2080). There the driver learns the outcome only
/// from the propose future, which the fake parks to the deadline and answers
/// `Timeout`, so the waiter retries even though the entry commits late. The
/// driver-notify ON path answers that same entry `Done` at apply time (see
/// [`a_timeout_then_commit_is_answered_done_under_driver_notify`]); the offset
/// safety this test's second push proves holds on both paths.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timeout_keeps_the_entry_in_flight_without_duplicate_offsets() {
    let fx = FakeFixture::open(
        "timeout",
        BatcherConfig {
            driver_notify: false,
            ..small_pipeline(2, 60)
        },
    );
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

/// PERF-G (`QUEEN_RAFT_DRIVER_NOTIFY`, default on): when the applied index
/// advances past an in-flight entry the driver answers its waiter `Done` off
/// the applied-index wake, WITHOUT waiting for the propose future — so an entry
/// that commits + applies well inside the deadline is answered the real
/// outcome at apply time, not `Retry` at the deadline. The pipeline-safety I3
/// cares about (no reused offset) still holds: the second push to the same
/// partition allocates the next offset.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timeout_then_commit_is_answered_done_under_driver_notify() {
    // driver_notify defaults on; the fake exposes the applied-index wake.
    let fx = FakeFixture::open("driver-notify", small_pipeline(2, 5_000));
    // The fake parks its propose future to the deadline (would answer Timeout),
    // but commits + advances applied at 15 ms and pulses the applied wake.
    fx.fake
        .push_step(Step::TimeoutThenCommit(Duration::from_millis(15)));
    fx.fake.set_default(Step::Now);

    // The applied-index wake resolves it Done at apply time, not Retry at the
    // 5 s deadline (the await would have blocked far longer than 15 ms).
    let first = submit(&fx.tx, push(1, "q", "p0", &["a"])).await;
    let (_pid, offset) = push_created(&first);
    assert_eq!(offset, 0, "the entry is answered its real outcome, Done");

    // Offset safety still holds: the next push takes the following offset.
    let second = submit(&fx.tx, push(2, "q", "p0", &["b"])).await;
    let (_pid, offset) = push_created(&second);
    assert_eq!(
        offset, 1,
        "no reused offset under the driver-notify fast path"
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
            writer_pipeline: crate::rsm::replicator::local::writer_pipeline_from_env(),
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
            let reply = rx.await.expect("reply");
            // WP-1.11 F-1: a reorder under the pipeline poisons the node (I5),
            // and every later reply comes back `Retry`. Assert `Done` so a
            // poison FAILS this smoke instead of passing silently.
            assert!(
                matches!(reply, Reply::Done { .. }),
                "pipeline={pipeline}: every reply must be Done (a poison/Retry means I5 \
                 was violated, WP-1.11 F-1), got {reply:?}"
            );
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

// ---------------------------------------------------------------------------
// WP-1.11 F-1 regression: the log index must follow plan order (I5)
// ---------------------------------------------------------------------------

/// The submission-order contract (§7.1, I5). The batcher stamps `now_us`
/// monotone in plan order; whatever order the entries reach the replicator's
/// `propose` — and thus the order the log assigns their index — must be that
/// same plan order, or apply sees `now_us` go backwards with the index and
/// refuses (WP-1.11 F-1).
///
/// This drives a real (frozen) store and a [`FakeReplicator`] whose `propose`
/// records each entry's bytes in submission order (`proposals()`), with a small
/// commit delay so the pipeline actually holds several in flight at once. The
/// fix submits proposes inline on the one driver task, in plan order, so the
/// recorded order is strictly increasing in `now_us`.
///
/// HONEST SCOPE: this is a deterministic PASS of the fix, NOT a repro of the old
/// bug. Before the fix — one `tokio::spawn` per propose — the recorded order
/// COULD diverge under a multi-worker runtime, but that reorder is a pure
/// scheduler race that does not reproduce on a laptop (verified: this test and
/// the real-log one below stay green on the reverted spawn form over 250+ runs).
/// The DETERMINISTIC guard that fails on the old form is
/// [`every_propose_submits_on_the_one_driver_task_in_plan_order`], which
/// witnesses the task the submission runs on rather than racing for a reorder.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_batcher_submits_proposes_to_the_replicator_in_plan_order() {
    const N: u64 = 64;
    let fx = FakeFixture::open("plan-order", small_pipeline(4, 5_000));
    // A slow-but-healthy commit, so up to `pipeline` proposes are in flight
    // together and their submissions can race if they are spawned.
    fx.fake.set_default(Step::After(Duration::from_millis(3)));

    // Submit everything before awaiting, so several proposes are live at once.
    let mut rxs = Vec::with_capacity(N as usize);
    for i in 0..N {
        let (sub, rx) = Submission::new(push(1_000 + i, "q", "p0", &[&format!("t{i}")]));
        fx.tx.send(sub).await.expect("send");
        rxs.push(rx);
    }
    for rx in rxs {
        let reply = rx.await.expect("reply");
        assert!(
            matches!(reply, Reply::Done { .. }),
            "every push must commit (a Retry would mean the node poisoned), got {reply:?}"
        );
    }

    // The order the entries reached the replicator == the order their `now_us`
    // was stamped: strictly increasing.
    let proposals = fx.fake.proposals();
    assert_eq!(proposals.len(), N as usize, "one entry per push");
    let mut last = i64::MIN;
    for (k, bytes) in proposals.iter().enumerate() {
        let entry = decode_entry(bytes).expect("decode proposed entry");
        assert!(
            entry.now_us > last,
            "submission {k}: now_us {} is not above the previous {last} — the log index \
             diverged from plan order (WP-1.11 F-1, I5)",
            entry.now_us,
        );
        last = entry.now_us;
    }

    fx.close().await;
}

/// The same property end to end, over the REAL `LocalReplicator` and WP-1.4's
/// apply thread at the ratified pipeline depth (D4). Under concurrent load the
/// apply I5 gate is live: a reorder poisons the node and every later reply is
/// `Retry`. Asserting every reply `Done`, that the node never stopped, and that
/// it applied every entry proves the message path is I5-correct at
/// `QUEEN_RAFT_PIPELINE=4` (the config the O14 comparison assumes).
///
/// HONEST SCOPE: this is a positive test of the fixed path, NOT the CI guard
/// that discriminates the bug. It stays green on the reverted spawn form too
/// (the F-1 reorder is a scheduler race that does not reproduce on a laptop —
/// verified over 40 runs on the old form), so on its own it could not have
/// caught F-1. The deterministic guard is
/// [`every_propose_submits_on_the_one_driver_task_in_plan_order`]; this test's
/// job is to exercise the real log + apply gate at pipeline=4 and confirm the
/// gate turns any residual reorder into a loud poison, never silent corruption.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_four_over_the_real_log_stays_i5_correct_under_load() {
    const N: u64 = 1_500;
    let dir = scratch("i5-load");
    let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
    let repl = Arc::new(open_repl(&dir, store.clone()));
    let cfg = BatcherConfig {
        pipeline: 4,
        batch_max_cmds: 1, // one command per cycle == one entry per propose: the
        // most distinct in-flight proposes, the worst case for a reorder.
        request_expire_every_ms: 3_600_000,
        ..BatcherConfig::default()
    };
    let batcher = Batcher::new(store.clone(), repl.clone(), cfg);
    let (tx, handle) = batcher.spawn();

    // Spread across a few partitions so pushes really run concurrently through
    // the planner, not all serialised on one partition's ring.
    let mut rxs = Vec::with_capacity(N as usize);
    for i in 0..N {
        let part = format!("p{}", i % 8);
        let (sub, rx) = Submission::new(push(10_000 + i, "q", &part, &[&format!("t{i}")]));
        tx.send(sub).await.expect("send");
        rxs.push(rx);
    }
    for (i, rx) in rxs.into_iter().enumerate() {
        let reply = rx.await.expect("reply");
        assert!(
            matches!(reply, Reply::Done { .. }),
            "push {i}: every reply must be Done at pipeline=4; a Retry means the apply \
             I5 gate refused a reordered log and the node poisoned (WP-1.11 F-1), got {reply:?}"
        );
    }

    // The node is still a leader (never poisoned) and applied every entry.
    assert!(
        repl.role().is_leader(),
        "the node must still be leader; a poison would have stopped it"
    );
    assert!(
        repl.applied_index() >= N,
        "every entry applied (I5 held for all {N}); applied_index={}",
        repl.applied_index()
    );

    drop(tx);
    handle.await.expect("driver join");
    let repl = Arc::try_unwrap(repl).unwrap_or_else(|_| panic!("replicator still shared"));
    let (_stats, store2) = repl.shutdown().expect("shutdown");
    drop(store);
    drop(store2);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// WP-1.11 F-1 deterministic guard: the submission runs on the ONE driver task
// ---------------------------------------------------------------------------

/// One recorded `propose` submission: the tokio task it ran on, the entry's
/// `now_us` stamp, and the log index it was assigned — everything the
/// first-poll synchronous prefix produces (§7.1, mod.rs `propose` contract).
#[derive(Clone, Copy, Debug)]
struct SubmitWitness {
    task: Option<tokio::task::Id>,
    now_us: i64,
    index: u64,
}

/// A [`Replicator`] whose `propose` does its whole submission — record the
/// driver task, read the stamp, assign the index — in the first-poll
/// synchronous prefix (before its only `.await`), exactly where
/// [`LocalReplicator`] sends on its writer channel. It then commits after a
/// small delay so up to `pipeline` proposes are in flight together.
///
/// This is the DETERMINISTIC witness of the WP-1.11 F-1 fix, and it needs no
/// scheduler race. The fix drives every propose's first poll INLINE on the one
/// driver task (`RunState::propose`), so every submission runs on THAT task —
/// `task` is identical across all of them. The old spawn-per-propose form ran
/// each `repl.propose()` on its own freshly-spawned task, so the submissions
/// would carry N DISTINCT task ids. Asserting one id therefore fails on the old
/// form and passes on the fix, on any machine (the fake-number reorder the pure
/// race would need does not reproduce on a laptop; the task identity does).
struct TaskWitnessReplicator {
    role_rx: watch::Receiver<Role>,
    // Kept so the watch channel stays open for the batcher's `watch_role`.
    _role_tx: watch::Sender<Role>,
    next_index: AtomicU64,
    applied: AtomicU64,
    submissions: std::sync::Mutex<Vec<SubmitWitness>>,
}

impl TaskWitnessReplicator {
    fn new() -> TaskWitnessReplicator {
        let (tx, rx) = watch::channel(Role::Leader { term: 1 });
        TaskWitnessReplicator {
            role_rx: rx,
            _role_tx: tx,
            next_index: AtomicU64::new(1),
            applied: AtomicU64::new(0),
            submissions: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn submissions(&self) -> Vec<SubmitWitness> {
        self.submissions.lock().expect("witness").clone()
    }
}

#[async_trait]
impl Replicator for TaskWitnessReplicator {
    async fn propose(&self, entry: Bytes, deadline: Instant) -> Result<AppliedAt, ProposeError> {
        // --- submission: the first-poll synchronous prefix (before any .await) ---
        let task = tokio::task::try_id();
        let now_us = decode_entry(&entry).expect("decode proposed entry").now_us;
        let index = self.next_index.fetch_add(1, Ordering::AcqRel);
        self.submissions
            .lock()
            .expect("witness")
            .push(SubmitWitness {
                task,
                now_us,
                index,
            });
        // --- the only suspension: a slow-but-healthy commit ---
        let until = Instant::now() + Duration::from_millis(3);
        tokio::time::sleep_until(tokio::time::Instant::from_std(until.min(deadline))).await;
        self.applied.fetch_max(index, Ordering::AcqRel);
        Ok(AppliedAt { index, term: 1 })
    }

    fn role(&self) -> Role {
        *self.role_rx.borrow()
    }

    fn watch_role(&self) -> watch::Receiver<Role> {
        self.role_rx.clone()
    }

    async fn read_barrier(&self, _deadline: Instant) -> Result<u64, ProposeError> {
        Ok(self.applied.load(Ordering::Acquire))
    }

    fn applied_index(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }

    async fn transfer_leadership(
        &self,
        _to: Option<NodeId>,
        _deadline: Instant,
    ) -> Result<(), ReplError> {
        Ok(())
    }

    async fn membership(&self) -> Membership {
        Membership::single(1)
    }

    async fn change_membership(
        &self,
        _change: MembershipChange,
        _deadline: Instant,
    ) -> Result<(), ReplError> {
        Err(ReplError::Unsupported("task-witness replicator".into()))
    }

    fn metrics(&self) -> ReplMetrics {
        ReplMetrics {
            term: 1,
            leader: Some(1),
            is_leader: true,
            last_log_index: self.applied.load(Ordering::Acquire),
            committed_index: self.applied.load(Ordering::Acquire),
            applied_index: self.applied.load(Ordering::Acquire),
            durable_index: self.applied.load(Ordering::Acquire),
            inflight: 0,
            proposals: self.submissions.lock().expect("witness").len() as u64,
            log_files: 1,
            log_bytes: 0,
        }
    }
}

/// WP-1.11 F-1, the deterministic regression guard the reviewer required. The
/// fix's guarantee is that the log submission of every entry runs INLINE on the
/// one driver task, in plan order (`RunState::propose` drives the first poll
/// itself, §7.1 Threads). The old `tokio::spawn(repl.propose(..))` form ran
/// each submission on its own task, which let the writer assign the log index
/// out of `now_us` order and poison apply (I5) under load.
///
/// A pure fake-number reorder cannot be forced on a laptop (the reviewers and
/// this WP both confirmed it: 250+ runs of the ordering tests never reordered
/// on the old form). This test instead witnesses the STRUCTURAL property the
/// fix establishes and the old form violated — the task the submission runs on:
///
/// * every submission ran on ONE task (the driver) — false on the spawn form,
///   where each runs on a distinct spawned task;
/// * the submissions are in plan order, strictly increasing in `now_us` and in
///   the index the replicator assigned.
///
/// It is deterministic: no sleep-race, no scheduler dependence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_propose_submits_on_the_one_driver_task_in_plan_order() {
    const N: u64 = 64;
    let dir = scratch("task-witness");
    let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
    let repl = Arc::new(TaskWitnessReplicator::new());
    let batcher = Batcher::new(store.clone(), repl.clone(), small_pipeline(4, 5_000));
    let (tx, handle) = batcher.spawn();

    // Submit everything before awaiting, so up to `pipeline` proposes are live
    // at once: on the old spawn form these would be N separate tasks.
    let mut rxs = Vec::with_capacity(N as usize);
    for i in 0..N {
        let (sub, rx) = Submission::new(push(2_000 + i, "q", "p0", &[&format!("t{i}")]));
        tx.send(sub).await.expect("send");
        rxs.push(rx);
    }
    for rx in rxs {
        let reply = rx.await.expect("reply");
        assert!(
            matches!(reply, Reply::Done { .. }),
            "every push must commit, got {reply:?}"
        );
    }

    let subs = repl.submissions();
    assert_eq!(subs.len(), N as usize, "one submission per push");

    // (1) Every submission ran on the SAME tokio task — the one driver task.
    // The old spawn-per-propose form fails here: N distinct spawned tasks.
    let first_task = subs[0].task;
    assert!(
        first_task.is_some(),
        "the submission ran inside a tokio task"
    );
    let distinct = subs
        .iter()
        .map(|s| s.task)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        distinct.len(),
        1,
        "every propose must submit on the ONE driver task (WP-1.11 F-1: the fix \
         drives the first poll inline; the old form spawned each propose on its \
         own task). Saw {} distinct submission tasks across {N} proposes.",
        distinct.len(),
    );

    // (2) Submission order == plan order: strictly increasing now_us AND index.
    let mut last_now = i64::MIN;
    let mut last_index = 0u64;
    for (k, s) in subs.iter().enumerate() {
        assert!(
            s.now_us > last_now,
            "submission {k}: now_us {} not above previous {last_now} (I5)",
            s.now_us
        );
        assert!(
            s.index > last_index,
            "submission {k}: index {} not above previous {last_index}",
            s.index
        );
        last_now = s.now_us;
        last_index = s.index;
    }

    drop(tx);
    handle.await.expect("driver join");
    let _ = std::fs::remove_dir_all(&dir);
}
