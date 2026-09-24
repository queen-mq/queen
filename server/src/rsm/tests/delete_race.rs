//! A delete racing the commands that name a partition.
//!
//! A queue delete (013), a tenant purge (031) and retention's partition delete
//! (006) take partitions away in an entry the planner may still have IN FLIGHT
//! when it plans the next command — or earlier in the SAME cycle: a delete is a
//! non-push command, so the priority lane drains it ahead of the pushes. What a
//! later command reads through the overlay must already see them gone:
//!
//! - a push to the deleted queue re-creates it with a NEW partition. An
//!   `Append` to the old pid is refused by apply (`no partition row`, fatal:
//!   the node poisons itself) once the delete's first chunk has removed a small
//!   partition's row, and files the message into a partition being deleted (it
//!   is lost with it) when the chunk has not finished;
//! - a lease renew never sets a cursor on a deleted pid: a `CursorSet` on a
//!   missing partition row is just as fatal.
//!
//! The first half runs the real batcher over this node's real replicator
//! (`QUEEN_RAFT_REPLICATOR`: local or openraft) and apply, behind a [`Gate`]
//! that holds proposals: a held entry is planned and proposed but not applied,
//! which is exactly "in flight" for the batcher. The delete is built the way
//! the facade builds it (`api_delete_queue`, `api_delete_tenant`: the pids
//! read from committed state, `GarbageAdd` + a first `DeleteChunk` in the same
//! entry, then chunks until no garbage is left).
//!
//! The second half drives the batcher's cycle function alone, for what the
//! planner does around a delete on its own: a delete names the partitions
//! created after the receiver read them, retention never deletes a partition
//! a push has just written, and a purge in flight hides the tenant's KV rows
//! and timers.

use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::json;
use tokio::sync::{mpsc, oneshot, watch};

use crate::rsm::apply::{Applier, Committed as ApplyCommitted, NoNotify, SystemClock};
use crate::rsm::batcher::{
    plan_cycle_blocking, Batcher, BatcherConfig, Command, CommandTx, KeepCfg, PlannerState, Reply,
    Submission,
};
use crate::rsm::dedup::DedupFront;
use crate::rsm::effect::{Effect, GarbageScope, Pid};
use crate::rsm::entry::{decode_entry, Entry, Outcome, PushVerdict};
use crate::rsm::planner::kv::parse_ops;
use crate::rsm::planner::timers::{parse_timer_ops, TimerFireConfig, TimersCommand};
use crate::rsm::planner::{
    EffectsCommand, KvCommand, PlanConfig, PopCommand, PushCommand, RenewCommand, SubIntent,
};
use crate::rsm::replicator::local::{NoWaker, OpenConfig};
use crate::rsm::replicator::node::{NodeReplicator, ReplicatorKind};
use crate::rsm::replicator::{
    AppliedAt, Membership, MembershipChange, NodeId, ProposeError, ReplError, ReplMetrics,
    Replicator, Role,
};
use crate::rsm::store::{HeedStore, Store, TypedReads};

use super::apply::{cfg as apply_cfg, seg_opts, store_opts, Node as ApplyNode};
use super::planner_harness::{item, qcfg, rid, BASE_US, TENANT};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-delete-race-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

// ---------------------------------------------------------------------------
// The gate: proposals held in plan order, released on demand
// ---------------------------------------------------------------------------

struct Held {
    bytes: Bytes,
    planned: Arc<Entry>,
    deadline: Instant,
    reply: oneshot::Sender<Result<AppliedAt, ProposeError>>,
}

/// A [`Replicator`] in front of the node's own that holds every proposal while
/// closed. The submission happens in the first poll (onto an ordered channel),
/// and ONE forwarder hands the held entries to the inner replicator in that
/// order — first-polling each one, as the batcher does — so the index order
/// is still the plan order (the submission contract, I5).
struct Gate<R: Replicator> {
    inner: Arc<R>,
    tx: mpsc::UnboundedSender<Held>,
    open: watch::Sender<bool>,
    held: AtomicU64,
}

impl<R: Replicator> Gate<R> {
    fn new(inner: Arc<R>) -> Arc<Gate<R>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let (open, open_rx) = watch::channel(true);
        tokio::spawn(forward(inner.clone(), rx, open_rx));
        Arc::new(Gate {
            inner,
            tx,
            open,
            held: AtomicU64::new(0),
        })
    }

    fn close(&self) {
        self.open.send_replace(false);
    }

    fn open(&self) {
        self.open.send_replace(true);
    }

    /// How many proposals have reached the gate.
    fn proposals(&self) -> u64 {
        self.held.load(Ordering::Acquire)
    }

    /// Wait until `n` proposals have reached the gate: the commands they carry
    /// have been planned.
    async fn wait_proposals(&self, n: u64) {
        let end = Instant::now() + Duration::from_secs(10);
        while self.proposals() < n {
            assert!(
                Instant::now() < end,
                "only {} of {n} proposals reached the gate",
                self.proposals()
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

async fn forward<R: Replicator>(
    inner: Arc<R>,
    mut rx: mpsc::UnboundedReceiver<Held>,
    mut open: watch::Receiver<bool>,
) {
    while let Some(Held {
        bytes,
        planned,
        deadline,
        reply,
    }) = rx.recv().await
    {
        while !*open.borrow_and_update() {
            if open.changed().await.is_err() {
                return;
            }
        }
        let inner = inner.clone();
        let mut fut = Box::pin(async move { inner.propose_entry(bytes, planned, deadline).await });
        let mut cx = Context::from_waker(Waker::noop());
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(res) => {
                let _ = reply.send(res);
            }
            Poll::Pending => {
                tokio::spawn(async move {
                    let _ = reply.send(fut.await);
                });
            }
        }
    }
}

#[async_trait]
impl<R: Replicator> Replicator for Gate<R> {
    async fn propose(&self, entry: Bytes, deadline: Instant) -> Result<AppliedAt, ProposeError> {
        let planned = decode_entry(&entry)
            .map_err(|e| ProposeError::Refused(format!("proposal does not decode: {e:?}")))?;
        self.propose_entry(entry, Arc::new(planned), deadline).await
    }

    fn wants_bytes(&self) -> bool {
        self.inner.wants_bytes()
    }

    async fn propose_entry(
        &self,
        entry: Bytes,
        planned: Arc<Entry>,
        deadline: Instant,
    ) -> Result<AppliedAt, ProposeError> {
        let (reply, rx) = oneshot::channel();
        self.held.fetch_add(1, Ordering::AcqRel);
        if self
            .tx
            .send(Held {
                bytes: entry,
                planned,
                deadline,
                reply,
            })
            .is_err()
        {
            return Err(ProposeError::Fatal("gate forwarder gone".into()));
        }
        rx.await
            .unwrap_or_else(|_| Err(ProposeError::Fatal("gate reply dropped".into())))
    }

    fn role(&self) -> Role {
        self.inner.role()
    }

    fn watch_role(&self) -> watch::Receiver<Role> {
        self.inner.watch_role()
    }

    fn applied_notify(&self) -> Option<Arc<tokio::sync::Notify>> {
        self.inner.applied_notify()
    }

    async fn read_barrier(&self, deadline: Instant) -> Result<u64, ProposeError> {
        self.inner.read_barrier(deadline).await
    }

    fn applied_index(&self) -> u64 {
        self.inner.applied_index()
    }

    async fn transfer_leadership(
        &self,
        to: Option<NodeId>,
        deadline: Instant,
    ) -> Result<(), ReplError> {
        self.inner.transfer_leadership(to, deadline).await
    }

    async fn membership(&self) -> Membership {
        self.inner.membership().await
    }

    async fn change_membership(
        &self,
        change: MembershipChange,
        deadline: Instant,
    ) -> Result<(), ReplError> {
        self.inner.change_membership(change, deadline).await
    }

    fn metrics(&self) -> ReplMetrics {
        self.inner.metrics()
    }
}

// ---------------------------------------------------------------------------
// The fixture: store, the node's replicator, the gate, the batcher
// ---------------------------------------------------------------------------

type Node = NodeReplicator<HeedStore>;

struct Fx {
    dir: PathBuf,
    store: Arc<HeedStore>,
    repl: Arc<Node>,
    gate: Arc<Gate<Node>>,
    tx: CommandTx,
    handle: tokio::task::JoinHandle<()>,
}

fn batcher_cfg(pipeline: usize) -> BatcherConfig {
    BatcherConfig {
        pipeline,
        // Held entries must not time out while the test holds them.
        propose_ms: 30_000,
        request_expire_every_ms: 3_600_000,
        maintenance_every_ms: 0,
        ..BatcherConfig::default()
    }
}

impl Fx {
    async fn open(tag: &str, cfg: BatcherConfig) -> Fx {
        let dir = scratch(tag);
        let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
        let kind = ReplicatorKind::from_env().expect("QUEEN_RAFT_REPLICATOR");
        let repl = Arc::new(
            NodeReplicator::open(
                kind,
                store.clone(),
                OpenConfig::new(1, dir.clone()),
                Arc::new(NoWaker),
                Arc::new(SystemClock),
            )
            .expect("open the replicator"),
        );
        // A single openraft voter elects itself; the local one leads at once.
        let mut role = repl.watch_role();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !role.borrow_and_update().is_leader() {
                role.changed().await.expect("role watch");
            }
        })
        .await
        .expect("the node became leader");
        let gate = Gate::new(repl.clone());
        let batcher = Batcher::new(store.clone(), gate.clone(), cfg)
            .with_reader(repl.reader())
            .with_qlog_reader(repl.qlog_reader());
        let (tx, handle) = batcher.spawn();
        Fx {
            dir,
            store,
            repl,
            gate,
            tx,
            handle,
        }
    }

    /// Send a command without waiting for its answer.
    async fn send(&self, command: Command) -> oneshot::Receiver<Reply> {
        let (sub, rx) = Submission::new(command);
        self.tx.send(sub).await.expect("send command");
        rx
    }

    async fn submit(&self, command: Command) -> Reply {
        self.send(command).await.await.expect("reply")
    }

    /// Wait until an independent read of the store sees `index` applied (the
    /// reply is sent on apply; the store commit follows on its cadence).
    async fn settle(&self, index: u64) {
        let end = Instant::now() + Duration::from_secs(10);
        loop {
            let applied = self
                .store
                .read(|r| r.applied_index())
                .expect("read applied");
            if applied >= index {
                return;
            }
            assert!(
                Instant::now() < end,
                "the store stayed at {applied} < {index}"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// Whether the node still applies: a linearizable read index answers.
    async fn alive(&self) -> Result<u64, ProposeError> {
        self.repl
            .read_barrier(Instant::now() + Duration::from_secs(5))
            .await
    }

    /// The facade's `finish_delete_chunks`: bounded chunks until none of the
    /// pids is garbage any more.
    async fn finish_chunks(&self, pids: &[Pid], scope: GarbageScope, limit: u32) {
        for n in 0..10_000u64 {
            let pending = self
                .store
                .read(|r| {
                    for pid in pids {
                        if r.garbage(*pid)?.is_some() {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                })
                .expect("read garbage");
            if !pending {
                return;
            }
            let chunk = effects(
                9_000_000 + n,
                vec![Effect::DeleteChunk {
                    pids: pids.to_vec(),
                    scope: scope.clone(),
                    resume: Vec::new(),
                    limit,
                }],
            );
            let (_, at) = done(&self.submit(chunk).await);
            self.settle(at).await;
        }
        panic!("the delete never finished");
    }

    async fn close(self) {
        let Fx {
            dir,
            store,
            repl,
            gate,
            tx,
            handle,
        } = self;
        drop(tx);
        let _ = handle.await;
        drop(gate);
        // The forwarder holds the last other handle; let it see the closed
        // channel before the node shuts down.
        for _ in 0..500 {
            if Arc::strong_count(&repl) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        drop(store);
        if let Ok(node) = Arc::try_unwrap(repl) {
            let _ = tokio::task::spawn_blocking(move || node.shutdown()).await;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

// ---------------------------------------------------------------------------
// Commands, built the way the receivers build them
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

fn pop_wildcard(id: u64, queue: &str, group: &str, worker: &str) -> Command {
    Command::PopWildcard(PopCommand {
        wait: false,
        request_id: rid(id),
        tenant: TENANT.to_string(),
        queue: queue.to_string(),
        partition: None,
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
        deadline_us: 0,
    })
}

fn renew(id: u64, worker: &str) -> Command {
    Command::Renew(RenewCommand {
        request_id: rid(id),
        worker: worker.to_string(),
        seconds: 120,
    })
}

fn effects(id: u64, effects: Vec<Effect>) -> Command {
    Command::Effects(EffectsCommand {
        request_id: rid(id),
        tenant: TENANT.to_string(),
        effects,
    })
}

fn wall_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros() as i64
}

/// `api_delete_queue` (phase2.rs): the queue's pids from committed state, the
/// name-keyed rows at once, the pid-keyed rows behind a `GarbageAdd` and a
/// first bounded chunk in the same entry. `limit` is the facade's 1,000; a
/// test passes less to model a partition the first chunk does not finish.
fn queue_delete(store: &HeedStore, id: u64, queue: &str, limit: u32) -> (Command, Vec<Pid>) {
    let pids = store
        .read(|r| {
            let mut pids = Vec::new();
            r.scan_queue_partitions(TENANT, queue, None, usize::MAX, &mut |pid| {
                pids.push(pid);
                true
            })?;
            Ok(pids)
        })
        .expect("read the queue's partitions");
    let mut effs = vec![Effect::QueueDelete {
        tenant: TENANT.to_string(),
        queue: queue.to_string(),
    }];
    if !pids.is_empty() {
        effs.push(Effect::GarbageAdd {
            pids: pids.clone(),
            scope: GarbageScope::Queue,
            deleted_at_us: wall_us(),
        });
        effs.push(Effect::DeleteChunk {
            pids: pids.clone(),
            scope: GarbageScope::Queue,
            resume: Vec::new(),
            limit,
        });
    }
    (effects(id, effs), pids)
}

/// `api_delete_tenant` (phase2.rs), for [`TENANT`].
fn tenant_purge(store: &HeedStore, id: u64) -> (Command, Vec<Pid>) {
    let pids = store
        .read(|r| {
            let mut queues = Vec::new();
            r.scan_queues(TENANT, usize::MAX, &mut |queue, _| {
                queues.push(queue.to_string());
                true
            })?;
            let mut pids = Vec::new();
            for queue in queues {
                r.scan_queue_partitions(TENANT, &queue, None, usize::MAX, &mut |pid| {
                    pids.push(pid);
                    true
                })?;
            }
            Ok(pids)
        })
        .expect("read the tenant's partitions");
    let mut effs = vec![Effect::TenantPurge {
        tenant: TENANT.to_string(),
    }];
    if !pids.is_empty() {
        effs.push(Effect::GarbageAdd {
            pids: pids.clone(),
            scope: GarbageScope::Tenant,
            deleted_at_us: wall_us(),
        });
        effs.push(Effect::DeleteChunk {
            pids: pids.clone(),
            scope: GarbageScope::Tenant,
            resume: Vec::new(),
            limit: 1_000,
        });
    }
    (effects(id, effs), pids)
}

// ---------------------------------------------------------------------------
// Reply helpers
// ---------------------------------------------------------------------------

/// The outcome and the commit index of a `Done`, or a failure naming what the
/// node answered instead.
fn done(reply: &Reply) -> (&Outcome, u64) {
    match reply {
        Reply::Done {
            outcome,
            at: Some(at),
        } => (outcome, at.index),
        other => panic!("expected a committed Done, got {other:?}"),
    }
}

fn created(reply: &Reply) -> (Pid, u64) {
    match done(reply).0 {
        Outcome::Push(p) => match &p.items[0] {
            PushVerdict::Created { pid, offset, .. } => (*pid, *offset),
            other => panic!("expected Created, got {other:?}"),
        },
        other => panic!("expected a push outcome, got {other:?}"),
    }
}

/// `(pid, start, end)` of every claim of a pop.
fn claims(reply: &Reply) -> Vec<(Pid, u64, u64)> {
    match done(reply).0 {
        Outcome::Pop(o) => o
            .claims
            .iter()
            .map(|c| (c.pid, c.start_offset, c.end_offset))
            .collect(),
        other => panic!("expected a pop outcome, got {other:?}"),
    }
}

/// A command's reply, and whether the node survived it: a poisoned apply
/// answers the waiter `Retry` and the read index `Fatal`.
async fn reply_and_health(fx: &Fx, rx: oneshot::Receiver<Reply>, what: &str) -> Reply {
    let reply = tokio::time::timeout(Duration::from_secs(10), rx).await;
    if let Err(e) = fx.alive().await {
        panic!(
            "{what}: the node stopped applying ({e}); the command was answered {reply:?} \
             (role {:?}, applied {})",
            fx.repl.role(),
            fx.repl.applied_index(),
        );
    }
    // The local replicator learns that its apply thread stopped only at the
    // next hand-off, so its read index can still answer: the stuck applied
    // index is what shows it.
    reply
        .unwrap_or_else(|_| {
            panic!(
                "{what}: no answer; apply is stuck at index {} (role {:?}, last log index {})",
                fx.repl.applied_index(),
                fx.repl.role(),
                fx.repl.metrics().last_log_index,
            )
        })
        .expect("reply")
}

// ---------------------------------------------------------------------------
// A push racing a queue delete
// ---------------------------------------------------------------------------

/// Push one message to `q/p0` and wait until the store shows it: the
/// partition the delete will read and take away.
async fn seed(fx: &Fx, id: u64) -> Pid {
    let reply = fx.submit(push(id, "q", "p0", &["seed"])).await;
    let (pid, offset) = created(&reply);
    assert_eq!(offset, 0);
    fx.settle(done(&reply).1).await;
    pid
}

/// After the race: the delete finishes, and the message the push was answered
/// for is in a live partition of the re-created queue, where a pop finds it.
async fn assert_push_survives(fx: &Fx, old: Pid, pids: &[Pid], new: Pid, id: u64) {
    assert_ne!(
        new, old,
        "the push after the delete was filed into the deleted partition {old}"
    );
    fx.finish_chunks(pids, GarbageScope::Queue, 1_000).await;
    let popped = fx.submit(pop_wildcard(id, "q", "g", "w1")).await;
    assert_eq!(
        claims(&popped),
        vec![(new, 0, 0)],
        "the pushed message is claimable in its new partition"
    );
    let gone = fx
        .store
        .read(|r| Ok(r.partition(old)?.is_none() && r.garbage(old)?.is_none()))
        .expect("read");
    assert!(gone, "the deleted partition {old} is gone");
}

/// Same cycle: a push drained with the delete that takes its partition away.
/// The delete sits in the priority lane, so it is planned first; the push must
/// see the queue as gone and create a new partition. A one-deep pipeline held
/// full by an unrelated entry makes the driver drain both in one batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_push_planned_after_a_queue_delete_in_the_same_cycle_gets_a_new_partition() {
    let fx = Fx::open("same-cycle", batcher_cfg(1)).await;
    let old = seed(&fx, 1).await;
    let (delete, pids) = queue_delete(&fx.store, 2, "q", 1_000);
    assert_eq!(pids, vec![old]);

    fx.gate.close();
    let blocker = fx.send(push(3, "other", "p0", &["x"])).await;
    fx.gate.wait_proposals(2).await;
    let del = fx.send(delete).await;
    let racing = fx.send(push(4, "q", "p0", &["raced"])).await;
    // Both are queued behind the full pipeline before it frees.
    tokio::time::sleep(Duration::from_millis(20)).await;
    fx.gate.open();

    let _ = done(&blocker.await.expect("blocker"));
    let del = reply_and_health(&fx, del, "the queue delete").await;
    let racing = reply_and_health(&fx, racing, "the racing push").await;
    assert_eq!(
        done(&del).1,
        done(&racing).1,
        "the delete and the push were planned into one entry"
    );
    let (new, offset) = created(&racing);
    assert_eq!(offset, 0, "a fresh partition starts at 0");
    fx.settle(done(&racing).1).await;
    assert_push_survives(&fx, old, &pids, new, 5).await;
    fx.close().await;
}

/// In flight: the delete's entry is proposed but not applied when the push is
/// planned. The first chunk removes the (small) partition's row, so an append
/// to it would poison apply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_push_planned_while_a_queue_delete_is_in_flight_gets_a_new_partition() {
    let fx = Fx::open("in-flight", batcher_cfg(4)).await;
    let old = seed(&fx, 1).await;
    let (delete, pids) = queue_delete(&fx.store, 2, "q", 1_000);

    fx.gate.close();
    let del = fx.send(delete).await;
    fx.gate.wait_proposals(2).await;
    let racing = fx.send(push(3, "q", "p0", &["raced"])).await;
    fx.gate.wait_proposals(3).await;
    fx.gate.open();

    let del = reply_and_health(&fx, del, "the queue delete").await;
    let racing = reply_and_health(&fx, racing, "the racing push").await;
    assert!(
        done(&racing).1 > done(&del).1,
        "the push was planned into a later entry"
    );
    let (new, _) = created(&racing);
    fx.settle(done(&racing).1).await;
    assert_push_survives(&fx, old, &pids, new, 4).await;
    fx.close().await;
}

/// In flight, and the first chunk does NOT finish the partition (a partition
/// over the facade's 1,000-row chunk; here a chunk of one row). Apply accepts
/// an append to the old pid — its row is still there — and the message is
/// deleted with the partition by the chunks that follow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_push_racing_the_delete_of_a_large_partition_is_not_lost() {
    let fx = Fx::open("large", batcher_cfg(4)).await;
    let old = seed(&fx, 1).await;
    let (delete, pids) = queue_delete(&fx.store, 2, "q", 1);

    fx.gate.close();
    let del = fx.send(delete).await;
    fx.gate.wait_proposals(2).await;
    let racing = fx.send(push(3, "q", "p0", &["raced"])).await;
    fx.gate.wait_proposals(3).await;
    fx.gate.open();

    let _ = reply_and_health(&fx, del, "the queue delete").await;
    let racing = reply_and_health(&fx, racing, "the racing push").await;
    let (new, _) = created(&racing);
    fx.settle(done(&racing).1).await;
    assert_push_survives(&fx, old, &pids, new, 4).await;
    fx.close().await;
}

// ---------------------------------------------------------------------------
// The other deletes, and a cursor on a deleted pid
// ---------------------------------------------------------------------------

/// Retention's partition delete (006) in flight: the partition row and its
/// name go when it lands, so the push must create the partition anew.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_push_planned_while_a_partition_delete_is_in_flight_gets_a_new_partition() {
    let fx = Fx::open("partition-delete", batcher_cfg(4)).await;
    let old = seed(&fx, 1).await;

    fx.gate.close();
    let del = fx
        .send(effects(2, vec![Effect::PartitionDelete { pid: old }]))
        .await;
    fx.gate.wait_proposals(2).await;
    let racing = fx.send(push(3, "q", "p0", &["raced"])).await;
    fx.gate.wait_proposals(3).await;
    fx.gate.open();

    let _ = reply_and_health(&fx, del, "the partition delete").await;
    let racing = reply_and_health(&fx, racing, "the racing push").await;
    let (new, offset) = created(&racing);
    assert_ne!(new, old, "the push was filed into the deleted partition");
    assert_eq!(offset, 0, "a fresh partition starts at 0");
    fx.settle(done(&racing).1).await;
    let bound = fx
        .store
        .read(|r| r.pid_of(TENANT, "q", "p0"))
        .expect("read");
    assert_eq!(bound, Some(new), "the name is bound to the new partition");
    fx.close().await;
}

/// A tenant purge (031) in flight: every queue of the tenant goes, so a push
/// to one of them re-creates it, with a new partition.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_push_planned_while_a_tenant_purge_is_in_flight_recreates_the_queue() {
    let fx = Fx::open("tenant-purge", batcher_cfg(4)).await;
    let old = seed(&fx, 1).await;
    let (purge, pids) = tenant_purge(&fx.store, 2);
    assert_eq!(pids, vec![old]);

    fx.gate.close();
    let del = fx.send(purge).await;
    fx.gate.wait_proposals(2).await;
    let racing = fx.send(push(3, "q", "p0", &["raced"])).await;
    fx.gate.wait_proposals(3).await;
    fx.gate.open();

    let _ = reply_and_health(&fx, del, "the tenant purge").await;
    let racing = reply_and_health(&fx, racing, "the racing push").await;
    let (new, _) = created(&racing);
    assert_ne!(new, old, "the push was filed into the purged partition");
    fx.settle(done(&racing).1).await;
    let queue = fx.store.read(|r| r.queue(TENANT, "q")).expect("read");
    assert!(queue.is_some(), "the push re-created the purged queue");
    fx.finish_chunks(&pids, GarbageScope::Tenant, 1_000).await;
    let popped = fx.submit(pop_wildcard(4, "q", "g", "w1")).await;
    assert_eq!(claims(&popped), vec![(new, 0, 0)]);
    fx.close().await;
}

/// A lease renew planned while the delete of the leased partition is in
/// flight: the renew walks the worker's committed leases, and the partition
/// is gone by the time its `CursorSet` would apply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_renew_planned_while_a_queue_delete_is_in_flight_skips_the_deleted_lease() {
    let fx = Fx::open("renew", batcher_cfg(4)).await;
    let old = seed(&fx, 1).await;
    let popped = fx.submit(pop_wildcard(2, "q", "g", "w1")).await;
    assert_eq!(claims(&popped), vec![(old, 0, 0)], "w1 leases the seed");
    fx.settle(done(&popped).1).await;
    let (delete, _pids) = queue_delete(&fx.store, 3, "q", 1_000);

    fx.gate.close();
    let base = fx.gate.proposals();
    let del = fx.send(delete).await;
    fx.gate.wait_proposals(base + 1).await;
    let renewed = fx.send(renew(4, "w1")).await;
    // The renew either plans nothing (answered at once) or proposes.
    tokio::time::sleep(Duration::from_millis(50)).await;
    fx.gate.open();

    let _ = reply_and_health(&fx, del, "the queue delete").await;
    let renewed = reply_and_health(&fx, renewed, "the racing renew").await;
    match &renewed {
        Reply::Done {
            outcome: Outcome::Renew(r),
            ..
        } => assert_eq!(r.renewed, 0, "the deleted partition's lease is not renewed"),
        other => panic!("expected a renew outcome, got {other:?}"),
    }
    fx.close().await;
}

/// A seek writes cursors the receiver built from committed state and submits
/// them as plain effects. One planned while the delete of its partition is in
/// flight is refused (retryable: the retry reads the partitions again), never
/// logged to reach apply after the partition row is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seek_planned_while_a_queue_delete_is_in_flight_is_refused() {
    let fx = Fx::open("seek", batcher_cfg(4)).await;
    let old = seed(&fx, 1).await;
    let (delete, _pids) = queue_delete(&fx.store, 2, "q", 1_000);

    fx.gate.close();
    let del = fx.send(delete).await;
    fx.gate.wait_proposals(2).await;
    let seek = effects(
        3,
        vec![Effect::CursorSet {
            pid: old,
            group: "g".to_string(),
            row: crate::rsm::store::rows::cursor_fresh(0, wall_us()),
        }],
    );
    // A refusal is answered at once; a logged seek would wait at the gate.
    let seek = tokio::time::timeout(Duration::from_secs(5), fx.send(seek).await)
        .await
        .unwrap_or_else(|_| {
            fx.gate.open();
            panic!("the seek of a partition being deleted was logged, not refused")
        })
        .expect("reply");
    match &seek {
        Reply::Refused(r) => assert!(r.retryable, "a retryable refusal, got {r:?}"),
        other => panic!("the seek of a partition being deleted was {other:?}"),
    }
    fx.gate.open();
    let _ = reply_and_health(&fx, del, "the queue delete").await;
    fx.close().await;
}

// ---------------------------------------------------------------------------
// The planning cycle alone: the delete's own planning and the leader steps
// ---------------------------------------------------------------------------
//
// These drive the batcher's cycle function (`plan_cycle_blocking`, what the
// driver runs on its planner thread) directly, naming the entries in flight
// and landing them through the real applier, with KEEP_OVERLAY verified
// against a rebuild every cycle.

struct Cycles {
    node: ApplyNode,
    state: PlannerState,
    front: DedupFront,
    inflight: Vec<(u64, Arc<Entry>)>,
    next_index: u64,
    applied: u64,
    wall: i64,
}

impl Cycles {
    fn new(tag: &str) -> Cycles {
        Cycles {
            node: ApplyNode::new(tag),
            state: PlannerState::default(),
            front: DedupFront::disabled(),
            inflight: Vec::new(),
            next_index: 1,
            applied: 0,
            wall: BASE_US,
        }
    }

    fn store(&self) -> &HeedStore {
        self.node.store()
    }

    /// One cycle over everything still in flight; its entry stays in flight.
    fn plan(
        &mut self,
        batch: Vec<Command>,
        maintenance: Option<crate::rsm::maintenance::Config>,
        fire: Option<TimerFireConfig>,
    ) -> Option<Arc<Entry>> {
        let keep = KeepCfg {
            enabled: true,
            epoch: 0,
            verify: true,
            verify_rings: true,
            reset_every: 0,
        };
        let out = plan_cycle_blocking(
            self.node.store(),
            &self.front,
            &mut self.state,
            keep,
            None,
            None,
            self.inflight.clone(),
            batch,
            PlanConfig {
                plan_budget_ms: 3_600_000,
                ..PlanConfig::default()
            },
            self.wall,
            None,
            None,
            fire,
            maintenance,
        )
        .expect("plan cycle");
        assert!(
            self.state.stats.mismatches.is_empty(),
            "the kept overlay differs from a rebuild: {:#?}",
            self.state.stats.mismatches
        );
        self.wall += 1_000;
        if let Some(e) = &out.entry {
            self.inflight.push((self.next_index, e.clone()));
            self.next_index += 1;
        }
        out.entry
    }

    /// Apply every entry in flight, in order.
    fn land(&mut self) {
        let (mut a, _) = Applier::open(
            self.node.store(),
            &self.node.seg_dir(),
            seg_opts(),
            apply_cfg(),
            Arc::new(NoNotify),
        )
        .expect("open applier");
        for (index, e) in self.inflight.drain(..) {
            if index <= self.applied {
                continue;
            }
            a.apply(&ApplyCommitted {
                index,
                term: 1,
                entry: (*e).clone(),
            })
            .unwrap_or_else(|err| panic!("apply entry {index}: {err:?}"));
            self.applied = index;
        }
        a.durable_point().expect("durable point");
    }

    /// Chunk the pids until none is garbage (the facade's loop).
    fn finish(&mut self, pids: &[Pid], scope: GarbageScope) {
        for n in 0..1_000u64 {
            let pending = self
                .store()
                .read(|r| {
                    for pid in pids {
                        if r.garbage(*pid)?.is_some() {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                })
                .expect("read garbage");
            if !pending {
                return;
            }
            let chunk = Effect::DeleteChunk {
                pids: pids.to_vec(),
                scope: scope.clone(),
                resume: Vec::new(),
                limit: 1_000,
            };
            self.plan(vec![effects(8_000_000 + n, vec![chunk])], None, None);
            self.land();
        }
        panic!("the delete never finished");
    }
}

/// The pid a planned entry created for `queue/partition`.
fn created_pid(e: &Entry, queue: &str, partition: &str) -> Pid {
    e.effects
        .iter()
        .find_map(|eff| match eff {
            Effect::PartitionCreate {
                pid,
                queue: q,
                partition: p,
                ..
            } if q == queue && p == partition => Some(*pid),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no partition {queue}/{partition} was created"))
}

fn deletes_partition(e: &Entry, pid: Pid) -> bool {
    e.effects
        .iter()
        .any(|eff| matches!(eff, Effect::PartitionDelete { pid: p } if *p == pid))
}

/// The receiver named the queue's partitions from committed state, and the
/// planner met partitions created since: one still in flight, one committed
/// after the read. Both go with the queue — named in its garbage, deleted by
/// its chunks — instead of losing their names and staying for ever.
#[test]
fn a_queue_delete_takes_the_partitions_created_since_its_read() {
    let mut c = Cycles::new("cover");
    let e = c
        .plan(vec![push(1, "q", "p0", &["a"])], None, None)
        .expect("push p0");
    let p0 = created_pid(&e, "q", "p0");
    c.land();

    let (delete, read) = queue_delete(c.store(), 2, "q", 1_000);
    assert_eq!(read, vec![p0], "the receiver read p0 alone");
    let e = c
        .plan(vec![push(3, "q", "p1", &["b"])], None, None)
        .expect("push p1");
    let p1 = created_pid(&e, "q", "p1");
    c.land();
    let e = c
        .plan(vec![push(4, "q", "p2", &["c"])], None, None)
        .expect("push p2");
    let p2 = created_pid(&e, "q", "p2");
    // p1 is committed, p2 still in flight, when the delete is planned.
    let e = c.plan(vec![delete], None, None).expect("the delete");
    let garbage: Vec<Pid> = e
        .effects
        .iter()
        .filter_map(|eff| match eff {
            Effect::GarbageAdd { pids, .. } => Some(pids.clone()),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(garbage, vec![p0, p1, p2], "every live partition is garbage");
    c.land();

    c.finish(&[p0, p1, p2], GarbageScope::Queue);
    for pid in [p0, p1, p2] {
        let left = c
            .store()
            .read(|r| Ok(r.partition(pid)?.is_some() || r.garbage(pid)?.is_some()))
            .expect("read");
        assert!(!left, "partition {pid} outlived the delete of its queue");
    }
}

fn retention_setup(tag: &str) -> (Cycles, Pid, Pid, crate::rsm::maintenance::Config) {
    let mut c = Cycles::new(tag);
    let e = c
        .plan(
            vec![push(1, "q", "p0", &["a"]), push(2, "q", "p1", &["b"])],
            None,
            None,
        )
        .expect("the pushes");
    let (p0, p1) = (created_pid(&e, "q", "p0"), created_pid(&e, "q", "p1"));
    c.land();
    // Retention has already dropped their one message each.
    let marks = [p0, p1]
        .into_iter()
        .map(|pid| Effect::Watermark {
            pid,
            log_start: 1,
            txns_start: 1,
        })
        .collect();
    c.plan(vec![effects(3, marks)], None, None);
    c.land();
    // Two days on, with a one-day cleanup: both are idle and empty — dead.
    c.wall += 2 * 86_400 * 1_000_000;
    let cfg = crate::rsm::maintenance::Config {
        partition_cleanup_days: 1,
        ..crate::rsm::maintenance::Config::default()
    };
    (c, p0, p1, cfg)
}

/// Retention runs after the cycle's commands and judges a partition dead from
/// committed state. A push to it earlier in the SAME cycle makes it live: the
/// delete must wait, or it drops the message the push was answered for.
#[test]
fn retention_does_not_delete_a_partition_a_push_wrote_in_the_same_cycle() {
    let (mut c, p0, p1, cfg) = retention_setup("retention-same-cycle");
    let e = c
        .plan(vec![push(4, "q", "p0", &["c"])], Some(cfg), None)
        .expect("the push and retention");
    assert!(!deletes_partition(&e, p0), "p0 was just written");
    assert!(deletes_partition(&e, p1), "p1 is still dead");
    c.land();
    let row = c
        .store()
        .read(|r| r.partition(p0))
        .expect("read")
        .expect("p0 survives");
    assert_eq!(row.last_offset, 1, "the pushed message is there");
}

/// The same with the push in an entry still in flight.
#[test]
fn retention_does_not_delete_a_partition_a_push_in_flight_writes() {
    let (mut c, p0, p1, cfg) = retention_setup("retention-in-flight");
    c.plan(vec![push(4, "q", "p0", &["c"])], None, None)
        .expect("the push");
    let e = c.plan(Vec::new(), Some(cfg), None).expect("retention");
    assert!(!deletes_partition(&e, p0), "p0 is being written");
    assert!(deletes_partition(&e, p1), "p1 is still dead");
    c.land();
    let row = c
        .store()
        .read(|r| r.partition(p0))
        .expect("read")
        .expect("p0 survives");
    assert_eq!(row.last_offset, 1, "the pushed message is there");
}

fn kv_put(id: u64, tenant: &str, key: &str, value: i64, expect: Option<u64>) -> Command {
    let mut op = json!({"op":"put","ns":"n","key":key,"value":{"v":value},"forever":true});
    if let Some(v) = expect {
        op["expect"] = json!(v);
    }
    Command::Kv(KvCommand {
        request_id: rid(id),
        tenant: tenant.to_string(),
        ops: parse_ops(&[op], tenant, false, 511).expect("kv op"),
    })
}

fn timer_now(id: u64, tenant: &str, key: &str) -> Command {
    let op = json!({
        "op":"schedule","queue":"qt","timerKey":key,
        "delayMs": 0, "txn": format!("{tenant}-{key}"), "payload": "e30=",
    });
    Command::Timers(TimersCommand {
        request_id: rid(id),
        tenant: tenant.to_string(),
        ops: parse_timer_ops(&[op], Some("svc")).expect("timer op"),
    })
}

/// A tenant purge in flight deletes the tenant's KV rows and timers when it
/// applies: a conditional write planned after it is judged against an absent
/// row, and a due timer of the purged tenant does not fire (it would re-create
/// the queue the purge removes). Another tenant is untouched.
#[test]
fn a_purge_in_flight_hides_the_tenants_kv_rows_and_timers() {
    const OTHER: &str = "t2";
    let mut c = Cycles::new("purge-kv-timers");
    c.plan(
        vec![
            kv_put(1, TENANT, "k", 1, None),
            kv_put(2, OTHER, "k", 1, None),
            timer_now(3, TENANT, "x"),
            timer_now(4, OTHER, "x"),
        ],
        None,
        None,
    )
    .expect("keys and timers");
    c.land();
    let version = |tenant: &str| {
        c.store()
            .read(|r| r.kv(tenant, "n", "k"))
            .expect("read")
            .expect("the key")
            .version
    };
    let (mine, other) = (version(TENANT), version(OTHER));

    let (purge, pids) = tenant_purge(c.store(), 5);
    assert!(pids.is_empty(), "no partitions yet");
    c.plan(vec![purge], None, None).expect("the purge");
    let e = c
        .plan(
            vec![
                kv_put(6, TENANT, "k", 2, Some(mine)),
                kv_put(7, OTHER, "k", 2, Some(other)),
            ],
            None,
            Some(TimerFireConfig::default()),
        )
        .expect("the other tenant's write and fire");
    let names = |tenant: &str| {
        e.effects.iter().any(|eff| match eff {
            Effect::KvPut { tenant: t, .. }
            | Effect::TimerDelete { tenant: t, .. }
            | Effect::QueueUpsert { tenant: t, .. }
            | Effect::PartitionCreate { tenant: t, .. } => t == tenant,
            _ => false,
        })
    };
    assert!(
        !names(TENANT),
        "a write or a fire of the purged tenant was planned: {:?}",
        e.effects
    );
    assert!(
        names(OTHER),
        "the other tenant's write and fire were planned"
    );
    c.land();
    let row = c.store().read(|r| r.kv(TENANT, "n", "k")).expect("read");
    assert!(row.is_none(), "the purged key came back: {row:?}");
    let queue = c.store().read(|r| r.queue(TENANT, "qt")).expect("read");
    assert!(queue.is_none(), "a purged timer re-created its queue");
}

/// The retention walk visits at most `visit_cap` partitions of a queue per pass
/// and resumes where the last pass stopped: five dead partitions under a cap of
/// two go in passes of 2, 2 and 1, each exactly once — a queue with a million
/// partitions no longer has all of them read on the planning thread every pass.
#[test]
fn retention_walks_a_queue_in_bounded_resumable_passes() {
    let mut c = Cycles::new("retention-walk");
    let names = ["p0", "p1", "p2", "p3", "p4"];
    let e = c
        .plan(
            names
                .iter()
                .enumerate()
                .map(|(i, n)| push(i as u64 + 1, "q", n, &["m"]))
                .collect(),
            None,
            None,
        )
        .expect("the pushes");
    let pids: Vec<Pid> = names.iter().map(|n| created_pid(&e, "q", n)).collect();
    c.land();
    let marks = pids
        .iter()
        .map(|&pid| Effect::Watermark {
            pid,
            log_start: 1,
            txns_start: 1,
        })
        .collect();
    c.plan(vec![effects(10, marks)], None, None);
    c.land();
    c.wall += 2 * 86_400 * 1_000_000;
    let cfg = crate::rsm::maintenance::Config {
        partition_cleanup_days: 1,
        visit_cap: 2,
        ..crate::rsm::maintenance::Config::default()
    };
    let mut deleted: Vec<Pid> = Vec::new();
    let mut per_pass: Vec<usize> = Vec::new();
    for _ in 0..3 {
        let e = c
            .plan(Vec::new(), Some(cfg.clone()), None)
            .expect("a retention pass");
        let now: Vec<Pid> = pids
            .iter()
            .copied()
            .filter(|&p| deletes_partition(&e, p))
            .collect();
        per_pass.push(now.len());
        deleted.extend(now);
        c.land();
    }
    assert_eq!(per_pass, vec![2, 2, 1], "bounded passes: {per_pass:?}");
    deleted.sort_unstable();
    let mut want = pids.clone();
    want.sort_unstable();
    assert_eq!(deleted, want, "every partition exactly once");
}
