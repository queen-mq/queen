//! A leader whose term ended with entries in flight: those entries must never
//! be answered `Done` because this node's applied index passed the index the
//! batcher PREDICTED for them. After a term change the index holds the NEW
//! leader's entry, and the stale entry never committed.
//!
//! Jepsen W3c `claim` under the pause nemesis (2026-09-26): n4 led term 1, was
//! SIGSTOPped, n1 was elected (term 2, its noop at the index n4 would plan
//! next). At SIGCONT n4's batcher still ran as term 1's leader: it planned the
//! claims queued during the pause (four forwarded over `/raft/v1/submit`, one
//! local) — the first a put, the rest "exists" empties waiting on its entry —
//! and proposed. n4 then stepped down and applied n1's entries at those same
//! indexes; `RunState::resolve_applied` (the PERF-G applied-index wake) saw
//! `applied >= predicted index` and answered every waiter `Done` with the
//! PLANNED outcome. The followers' `wait_applied(at.index)` passed at once
//! (they had applied n1's entry there long before), and the clients were told
//! "applied, version 2" / "exists, current = p5" for a put that no log holds:
//! every node then read the key as absent.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::json;
use tokio::sync::{oneshot, watch, Notify};

use crate::rsm::batcher::{Batcher, BatcherConfig, Command, Reply, Submission};
use crate::rsm::entry::{KvOpOutcome, Outcome};
use crate::rsm::planner::kv::parse_ops;
use crate::rsm::planner::KvCommand;
use crate::rsm::replicator::{
    AppliedAt, Membership, MembershipChange, NodeId, ProposeError, ReplError, ReplMetrics,
    Replicator, Role,
};
use crate::rsm::store::HeedStore;

use super::apply::store_opts;
use super::planner_harness::{rid, TENANT};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-stale-leader-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

/// A term-1 leader whose log suffix the term-2 leader replaces.
///
/// Every propose is appended at the next index and parked: it never commits
/// in term 1. [`SupersededLeader::new_leader_applied`] is this node, now a
/// follower, applying the term-2 leader's entries at those same indexes (the
/// applied index moves, the wake pulses); [`SupersededLeader::discard`] is
/// openraft's `LogEntryDiscarded` for the truncated suffix; and
/// [`SupersededLeader::step_down`] the role watch catching up. On a real node
/// these three come from three different threads (the apply thread notifies
/// the batcher directly; the propose results travel openraft's responder and a
/// spawned task; the role is published by the replicator's watch task, which
/// at SIGCONT was busy in its overdue tick — the `quorum_step` hand-off logged
/// at 22.5449 ran on the pre-pause metrics), in no fixed order.
struct SupersededLeader {
    role_tx: watch::Sender<Role>,
    role_rx: watch::Receiver<Role>,
    next_index: AtomicU64,
    applied: AtomicU64,
    applied_notify: Arc<Notify>,
    /// `(index, term)` of what this node applied (the term-2 leader's).
    applied_terms: Mutex<Vec<(u64, u64)>>,
    parked: Mutex<Vec<oneshot::Sender<Result<AppliedAt, ProposeError>>>>,
    proposals: AtomicU64,
}

impl SupersededLeader {
    fn new() -> SupersededLeader {
        let (role_tx, role_rx) = watch::channel(Role::Leader { term: 1 });
        SupersededLeader {
            role_tx,
            role_rx,
            next_index: AtomicU64::new(1),
            applied: AtomicU64::new(0),
            applied_notify: Arc::new(Notify::new()),
            applied_terms: Mutex::new(Vec::new()),
            parked: Mutex::new(Vec::new()),
            proposals: AtomicU64::new(0),
        }
    }

    fn proposals(&self) -> u64 {
        self.proposals.load(Ordering::Acquire)
    }

    /// This node applies the term-`term` leader's entries up to `upto`.
    fn new_leader_applied(&self, upto: u64, term: u64) {
        let from = self.applied.load(Ordering::Acquire) + 1;
        let mut t = self.applied_terms.lock().expect("terms");
        for i in from..=upto {
            t.push((i, term));
        }
        drop(t);
        self.applied.fetch_max(upto, Ordering::AcqRel);
        self.applied_notify.notify_one();
    }

    /// openraft truncates the term-1 suffix: every parked propose resolves
    /// `LogEntryDiscarded` (the replicator maps it to `OutcomeUnknown`).
    fn discard(&self) {
        for tx in self.parked.lock().expect("parked").drain(..) {
            let _ = tx.send(Err(ProposeError::OutcomeUnknown));
        }
    }

    fn step_down(&self, leader: NodeId) {
        let _ = self.role_tx.send(Role::Follower {
            leader: Some(leader),
        });
    }
}

#[async_trait]
impl Replicator for SupersededLeader {
    async fn propose(&self, _entry: Bytes, deadline: Instant) -> Result<AppliedAt, ProposeError> {
        if !matches!(*self.role_rx.borrow(), Role::Leader { .. }) {
            return Err(ProposeError::NotLeader {
                hint: self.role_rx.borrow().leader_hint(),
            });
        }
        // Appended now, in first-poll order (the submission contract).
        let _index = self.next_index.fetch_add(1, Ordering::AcqRel);
        let (tx, rx) = oneshot::channel();
        self.parked.lock().expect("parked").push(tx);
        self.proposals.fetch_add(1, Ordering::AcqRel);
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(ProposeError::OutcomeUnknown),
            Err(_) => Err(ProposeError::Timeout),
        }
    }

    fn role(&self) -> Role {
        *self.role_rx.borrow()
    }

    fn watch_role(&self) -> watch::Receiver<Role> {
        self.role_rx.clone()
    }

    fn applied_notify(&self) -> Option<Arc<Notify>> {
        Some(self.applied_notify.clone())
    }

    async fn read_barrier(&self, _deadline: Instant) -> Result<u64, ProposeError> {
        Ok(self.applied.load(Ordering::Acquire))
    }

    fn applied_index(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }

    fn applied_term_at(&self, index: u64) -> Option<u64> {
        self.applied_terms
            .lock()
            .expect("terms")
            .iter()
            .find(|(i, _)| *i == index)
            .map(|(_, t)| *t)
    }

    async fn transfer_leadership(
        &self,
        _to: Option<NodeId>,
        _deadline: Instant,
    ) -> Result<(), ReplError> {
        Ok(())
    }

    async fn membership(&self) -> Membership {
        Membership::single(4)
    }

    async fn change_membership(
        &self,
        _change: MembershipChange,
        _deadline: Instant,
    ) -> Result<(), ReplError> {
        Err(ReplError::Unsupported("test replicator".into()))
    }

    fn metrics(&self) -> ReplMetrics {
        let applied = self.applied.load(Ordering::Acquire);
        let last = self.next_index.load(Ordering::Acquire) - 1;
        let role = *self.role_rx.borrow();
        ReplMetrics {
            term: match role {
                Role::Leader { term } => term,
                _ => 2,
            },
            leader: Some(4),
            is_leader: role.is_leader(),
            last_log_index: last,
            committed_index: applied,
            applied_index: applied,
            durable_index: applied,
            inflight: last.saturating_sub(applied),
            proposals: self.proposals(),
            log_files: 1,
            log_bytes: 0,
        }
    }
}

/// Jepsen W3c's claim: `PUT /api/v1/kv/jepsen/<key>` `{value, forever, expect: 0}`.
fn claim(id: u64, key: &str, value: &str) -> Command {
    let ops = json!([{
        "op": "put", "ns": "jepsen", "key": key,
        "value": value, "forever": true, "expect": 0
    }]);
    Command::Kv(KvCommand {
        request_id: rid(id),
        tenant: TENANT.into(),
        ops: parse_ops(ops.as_array().expect("ops"), TENANT, false, 511).expect("valid claim"),
    })
}

async fn until(what: &str, f: impl Fn() -> bool) {
    let end = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// What a reply tells the client: `Some(text)` when it acknowledges an
/// outcome (`Done`), `None` for a retry/refusal (the caller retries under the
/// same request id, or learns nothing).
fn acknowledged(reply: &Reply) -> Option<String> {
    match reply {
        Reply::Done {
            outcome: Outcome::Kv(o),
            at,
        } => {
            let what = match o.results.first() {
                Some(KvOpOutcome::Write(w)) if w.applied => {
                    format!("applied, version {}", w.version)
                }
                Some(KvOpOutcome::Write(w)) => format!(
                    "not applied ({:?}), current {} version {}",
                    w.reason,
                    w.value
                        .as_deref()
                        .map(|v| String::from_utf8_lossy(v).into_owned())
                        .unwrap_or_default(),
                    w.version
                ),
                other => format!("{other:?}"),
            };
            Some(format!("Done at {at:?}: {what}"))
        }
        Reply::Done { outcome, at } => Some(format!("Done at {at:?}: {outcome:?}")),
        _ => None,
    }
}

async fn a_superseded_leader_never_acknowledges_its_uncommitted_entries(
    lanes: u64,
    driver_notify: bool,
) {
    let dir = scratch(&format!("lanes{lanes}-notify{driver_notify}"));
    let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
    let repl = Arc::new(SupersededLeader::new());
    let cfg = BatcherConfig {
        pipeline: 4,
        batch_max_cmds: 1,
        propose_ms: 60_000,
        request_expire_every_ms: 3_600_000,
        kv_sweep_every_ms: 3_600_000,
        lanes,
        driver_notify,
        ..BatcherConfig::default()
    };
    let (tx, handle) = Batcher::new(store.clone(), repl.clone(), cfg).spawn();

    // SIGCONT: the claims queued during the pause reach the batcher, which
    // still leads term 1. p5's is planned first: a put, logged at index 1.
    let (s5, r5) = Submission::new(claim(5, "c1", "p5"));
    tx.send(s5).await.expect("send");
    until("p5's entry proposed", || repl.proposals() == 1).await;
    // p6's claim of the same key is planned against the overlay: "exists,
    // current p5" — an empty plan that waits on p5's entry (§7.2).
    let (s6, r6) = Submission::new(claim(6, "c1", "p6"));
    tx.send(s6).await.expect("send");
    // A claim of another key, logged at index 2: once it is proposed, p6's
    // claim (ahead of it in the same lane) has been planned and routed.
    let (s7, r7) = Submission::new(claim(7, "c2", "p7"));
    tx.send(s7).await.expect("send");
    until("p7's entry proposed", || repl.proposals() == 2).await;

    // This node, now following term 2's leader, applies that leader's entries
    // at indexes 1 and 2 — its own term-1 entries there were truncated.
    repl.new_leader_applied(2, 2);
    // The watch task and the discarded proposes catch up a moment later.
    tokio::time::sleep(Duration::from_millis(200)).await;
    repl.discard();
    repl.step_down(1);

    let mut acked = Vec::new();
    for (who, rx) in [("p5", r5), ("p6", r6), ("p7", r7)] {
        let reply = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .unwrap_or_else(|_| panic!("{who}: no reply within 10 s"))
            .expect("reply");
        if let Some(what) = acknowledged(&reply) {
            acked.push(format!("{who}: {what}"));
        }
    }
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
    drop(store);
    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        acked.is_empty(),
        "QUEEN_LANES={lanes} QUEEN_RAFT_DRIVER_NOTIFY={driver_notify}: nothing this node proposed in term 1 ever committed (the \
         indexes hold term 2's entries, applied by term {:?}), yet its waiters were answered \
         as committed — a forwarding follower's wait_applied(at.index) passes at once and the \
         client gets an acknowledged write no node will ever read:\n  {}",
        repl.applied_terms.lock().expect("terms").clone(),
        acked.join("\n  ")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_superseded_leader_never_acknowledges_its_uncommitted_entries_single_planner() {
    a_superseded_leader_never_acknowledges_its_uncommitted_entries(1, true).await;
}

/// The Jepsen nodes' shape (`QUEEN_LANES=16`): KV plans on control, same path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_superseded_leader_never_acknowledges_its_uncommitted_entries_lanes() {
    a_superseded_leader_never_acknowledges_its_uncommitted_entries(16, true).await;
}

/// The control: with the PERF-G applied-index wake off
/// (`QUEEN_RAFT_DRIVER_NOTIFY=0`) the same interleaving answers every waiter
/// `Retry` — only the propose future (openraft's responder, exact to the log
/// id) resolves an entry, and it says `LogEntryDiscarded`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_the_applied_index_wake_a_superseded_leader_answers_retry() {
    a_superseded_leader_never_acknowledges_its_uncommitted_entries(16, false).await;
}
