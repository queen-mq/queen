//! `FakeReplicator`: a scriptable [`Replicator`] with no log, no store and no
//! apply thread, for the tests that must drive the seam's every branch —
//! above all WP-1.6b's batcher, which has to keep an entry in flight on a
//! `Timeout` (I3), drop the overlay on `NotLeader` / `OutcomeUnknown`, and
//! retry after leadership loss (§7.1). A real `LocalReplicator` on a single
//! node never produces most of those, so they cannot be tested against it.
//!
//! Each `propose` consumes one [`Step`] step from a queue (or the default
//! when the queue is empty). The current role gates first: a `propose` while
//! not leader is `NotLeader` before the script is even read, exactly as the
//! planner expects.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{watch, Notify};

use super::{
    AppliedAt, Membership, MembershipChange, NodeId, ProposeError, ReplError, ReplMetrics,
    Replicator, Role,
};

/// What one scripted `propose` does.
#[derive(Clone, Debug)]
pub enum Step {
    /// Append at the next index and resolve `Ok` at once.
    Now,
    /// Resolve `Ok` after a delay (a slow but healthy commit).
    After(Duration),
    /// Resolve with this error before appending (`NotLeader`, `Refused`, …).
    Fail(ProposeError),
    /// Never resolve inside the caller's deadline: the caller sees `Timeout`.
    /// The entry is "appended" (an index is consumed) but never committed —
    /// the pure timeout of a stalled disk.
    Timeout,
    /// The I3 case: the caller sees `Timeout`, and after `delay` the entry
    /// commits and `applied_index` advances anyway, so the planner that kept
    /// it in flight sees it land.
    TimeoutThenCommit(Duration),
}

struct Shared {
    node_id: NodeId,
    term: AtomicU64,
    applied: AtomicU64,
    committed: AtomicU64,
    next_index: AtomicU64,
    proposals: AtomicU64,
    /// Every proposed entry's bytes, in order, for a test to inspect.
    log: Mutex<Vec<Bytes>>,
    script: Mutex<VecDeque<Step>>,
    default: Mutex<Step>,
    /// PERF-G: pulsed whenever `applied` advances, so a driver in
    /// `QUEEN_RAFT_DRIVER_NOTIFY` mode resolves the entry off this signal
    /// (mirrors `LocalReplicator`, so the batcher tests exercise the same
    /// path the shipped binary takes).
    applied_notify: Arc<Notify>,
}

impl Shared {
    /// Move `applied`/`committed` up to `index` and wake any driver-notify
    /// waiter. Every place that advances `applied` goes through here so the
    /// notify is never missed.
    fn advance_applied(&self, index: u64) {
        self.applied.fetch_max(index, Ordering::AcqRel);
        self.committed.fetch_max(index, Ordering::AcqRel);
        self.applied_notify.notify_one();
    }
}

/// A `Replicator` a test drives step by step.
pub struct FakeReplicator {
    shared: Arc<Shared>,
    role_tx: watch::Sender<Role>,
    role_rx: watch::Receiver<Role>,
}

impl FakeReplicator {
    /// A leader at term 1 that commits every proposal at once.
    pub fn new(node_id: NodeId) -> FakeReplicator {
        let (role_tx, role_rx) = watch::channel(Role::Leader { term: 1 });
        FakeReplicator {
            shared: Arc::new(Shared {
                node_id,
                term: AtomicU64::new(1),
                applied: AtomicU64::new(0),
                committed: AtomicU64::new(0),
                next_index: AtomicU64::new(1),
                proposals: AtomicU64::new(0),
                log: Mutex::new(Vec::new()),
                script: Mutex::new(VecDeque::new()),
                default: Mutex::new(Step::Now),
                applied_notify: Arc::new(Notify::new()),
            }),
            role_tx,
            role_rx,
        }
    }

    /// What a `propose` does when the script queue is empty.
    pub fn set_default(&self, step: Step) {
        *self.shared.default.lock().expect("default") = step;
    }

    /// Queue one step, consumed by the next `propose` (FIFO).
    pub fn push_step(&self, step: Step) {
        self.shared.script.lock().expect("script").push_back(step);
    }

    /// Queue several steps.
    pub fn push_steps<I: IntoIterator<Item = Step>>(&self, steps: I) {
        let mut g = self.shared.script.lock().expect("script");
        g.extend(steps);
    }

    /// Set the role and publish it on the role watch. `Follower`/`Learner`/
    /// `Candidate`/`Stopped` make the next `propose` refuse with `NotLeader`.
    pub fn set_role(&self, role: Role) {
        if let Role::Leader { term } = role {
            self.shared.term.store(term, Ordering::Release);
        }
        let _ = self.role_tx.send(role);
    }

    /// Step down to a follower that knows `leader`.
    pub fn step_down(&self, leader: Option<NodeId>) {
        self.set_role(Role::Follower { leader });
    }

    /// The proposed entries, in order.
    pub fn proposals(&self) -> Vec<Bytes> {
        self.shared.log.lock().expect("log").clone()
    }

    pub fn proposal_count(&self) -> u64 {
        self.shared.proposals.load(Ordering::Acquire)
    }

    pub fn committed_index(&self) -> u64 {
        self.shared.committed.load(Ordering::Acquire)
    }

    /// Force the applied index forward (a test committing an entry the caller
    /// left in flight after a `Timeout`).
    pub fn set_applied(&self, index: u64) {
        self.shared.advance_applied(index);
    }

    fn take_step(&self) -> Step {
        if let Some(s) = self.shared.script.lock().expect("script").pop_front() {
            s
        } else {
            self.shared.default.lock().expect("default").clone()
        }
    }

    fn append(&self, entry: &Bytes) -> u64 {
        let index = self.shared.next_index.fetch_add(1, Ordering::AcqRel);
        self.shared.committed.fetch_max(index, Ordering::AcqRel);
        self.shared.log.lock().expect("log").push(entry.clone());
        index
    }

    fn commit(&self, index: u64) {
        self.shared.advance_applied(index);
    }

    fn term(&self) -> u64 {
        self.shared.term.load(Ordering::Acquire)
    }
}

#[async_trait]
impl Replicator for FakeReplicator {
    async fn propose(&self, entry: Bytes, deadline: Instant) -> Result<AppliedAt, ProposeError> {
        self.shared.proposals.fetch_add(1, Ordering::AcqRel);

        // Role gate first, like the real planner path.
        if let Role::Leader { .. } = *self.role_rx.borrow() {
        } else {
            let hint = self.role_rx.borrow().leader_hint();
            return Err(ProposeError::NotLeader { hint });
        }

        match self.take_step() {
            Step::Fail(e) => Err(e),
            Step::Now => {
                let index = self.append(&entry);
                self.commit(index);
                Ok(AppliedAt {
                    index,
                    term: self.term(),
                })
            }
            Step::After(d) => {
                let index = self.append(&entry);
                sleep_until(deadline, d).await;
                self.commit(index);
                Ok(AppliedAt {
                    index,
                    term: self.term(),
                })
            }
            Step::Timeout => {
                // The entry is appended (in flight) but never committed.
                let _index = self.append(&entry);
                park_until(deadline).await;
                Err(ProposeError::Timeout)
            }
            Step::TimeoutThenCommit(d) => {
                let index = self.append(&entry);
                // Commit it in the background, after `d`.
                let shared = self.shared.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(d).await;
                    shared.advance_applied(index);
                });
                park_until(deadline).await;
                Err(ProposeError::Timeout)
            }
        }
    }

    fn role(&self) -> Role {
        *self.role_rx.borrow()
    }

    fn watch_role(&self) -> watch::Receiver<Role> {
        self.role_rx.clone()
    }

    fn applied_notify(&self) -> Option<Arc<Notify>> {
        Some(self.shared.applied_notify.clone())
    }

    async fn read_barrier(&self, _deadline: Instant) -> Result<u64, ProposeError> {
        if let Role::Leader { .. } = *self.role_rx.borrow() {
            Ok(self.shared.applied.load(Ordering::Acquire))
        } else {
            Err(ProposeError::NotLeader {
                hint: self.role_rx.borrow().leader_hint(),
            })
        }
    }

    fn applied_index(&self) -> u64 {
        self.shared.applied.load(Ordering::Acquire)
    }

    async fn transfer_leadership(
        &self,
        to: Option<NodeId>,
        _deadline: Instant,
    ) -> Result<(), ReplError> {
        self.step_down(to);
        Ok(())
    }

    async fn membership(&self) -> Membership {
        Membership::single(self.shared.node_id)
    }

    async fn change_membership(
        &self,
        _change: MembershipChange,
        _deadline: Instant,
    ) -> Result<(), ReplError> {
        Err(ReplError::Unsupported("fake replicator".into()))
    }

    fn metrics(&self) -> ReplMetrics {
        let applied = self.shared.applied.load(Ordering::Acquire);
        let committed = self.shared.committed.load(Ordering::Acquire);
        ReplMetrics {
            term: self.term(),
            leader: Some(self.shared.node_id),
            is_leader: self.role_rx.borrow().is_leader(),
            last_log_index: committed,
            committed_index: committed,
            applied_index: applied,
            durable_index: applied,
            inflight: committed.saturating_sub(applied),
            proposals: self.shared.proposals.load(Ordering::Acquire),
            log_files: 1,
            log_bytes: 0,
        }
    }
}

/// Sleep for `d`, but never past `deadline` (so a healthy-but-slow commit
/// still lands before the caller's timeout fires when the test set it to).
async fn sleep_until(deadline: Instant, d: Duration) {
    let until = Instant::now() + d;
    let at = until.min(deadline);
    tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await;
}

/// Park until the caller's deadline, so `propose` returns `Timeout` exactly
/// when the caller's `timeout_at` would have fired.
async fn park_until(deadline: Instant) {
    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
}
