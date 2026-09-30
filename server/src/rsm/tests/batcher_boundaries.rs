use super::*;
use std::sync::Mutex;

struct BoundaryReplicator {
    inner: Arc<FakeReplicator>,
    after_first: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    completion_gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}
#[async_trait]
impl Replicator for BoundaryReplicator {
    async fn propose(&self, b: Bytes, d: Instant) -> Result<AppliedAt, ProposeError> {
        let result = self.inner.propose(b, d).await;
        // Run once after the first commit, still on the driver's first poll.
        // The backlog has already left cmd_rx, and the next cycle can plan.
        if let Some(hook) = self.after_first.lock().unwrap().take() {
            hook();
        }
        let gate = self.completion_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            let _ = gate.await;
        }
        result
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
    fn applied_term_at(&self, i: u64) -> Option<u64> {
        self.inner.applied_term_at(i)
    }
    fn applied_index(&self) -> u64 {
        self.inner.applied_index()
    }
    async fn read_barrier(&self, d: Instant) -> Result<u64, ProposeError> {
        self.inner.read_barrier(d).await
    }
    async fn transfer_leadership(&self, to: Option<NodeId>, d: Instant) -> Result<(), ReplError> {
        self.inner.transfer_leadership(to, d).await
    }
    async fn membership(&self) -> Membership {
        self.inner.membership().await
    }
    async fn change_membership(&self, c: MembershipChange, d: Instant) -> Result<(), ReplError> {
        self.inner.change_membership(c, d).await
    }
    fn metrics(&self) -> ReplMetrics {
        self.inner.metrics()
    }
}

struct BoundaryFixture {
    tx: CommandTx,
    handle: tokio::task::JoinHandle<()>,
    repl: Arc<BoundaryReplicator>,
    dir: PathBuf,
    _store: Arc<HeedStore>,
}
impl BoundaryFixture {
    fn new(
        lanes: u64,
        quiesce: Option<tokio::sync::mpsc::UnboundedReceiver<crate::rsm::batcher::QuiesceReq>>,
    ) -> Self {
        let dir = scratch("cycle-boundaries");
        let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).unwrap());
        let repl = Arc::new(BoundaryReplicator {
            inner: Arc::new(FakeReplicator::new(1)),
            after_first: Mutex::new(None),
            completion_gate: Mutex::new(None),
        });
        let mut batcher = Batcher::new(
            store.clone(),
            repl.clone(),
            BatcherConfig {
                lanes,
                ..small_pipeline(8, 5_000)
            },
        );
        if let Some(rx) = quiesce {
            batcher = batcher.with_quiesce(rx);
        }
        let (tx, handle) = batcher.spawn();
        Self {
            tx,
            handle,
            repl,
            dir,
            _store: store,
        }
    }
    fn backlog(&self) -> Vec<tokio::sync::oneshot::Receiver<Reply>> {
        (1..=64)
            .map(|i| {
                let (sub, rx) = Submission::new(push(i, "q", &format!("p{i}"), &["a"]));
                self.tx.try_send(sub).unwrap();
                rx
            })
            .collect()
    }
    fn after_first(&self, f: impl FnOnce() + Send + 'static) {
        *self.repl.after_first.lock().unwrap() = Some(Box::new(f));
    }
    async fn close(self) {
        drop(self.tx);
        tokio::time::timeout(Duration::from_secs(10), self.handle)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            Arc::strong_count(&self.repl),
            1,
            "the joined driver must release every proposal's replicator reference"
        );
        drop(self._store);
        let _ = std::fs::remove_dir_all(self.dir);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_joins_a_completion_task_after_the_applied_wake_answers_the_client() {
    for lanes in [1, 8] {
        let fx = BoundaryFixture::new(lanes, None);
        let (release, gate) = tokio::sync::oneshot::channel();
        *fx.repl.completion_gate.lock().unwrap() = Some(gate);
        let (sub, rx) = Submission::new(push(1, "q", "p", &["a"]));
        fx.tx.try_send(sub).unwrap();
        // Apply is published, but the backend's propose future cannot return
        // while this test retains `release`. The applied wake answers alone.
        let reply = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(reply, Reply::Done { .. }));
        assert!(!release.is_closed(), "the completion is still pending");
        fx.close().await;
        assert!(release.is_closed(), "shutdown dropped the pending future");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_priority_arrival_enters_the_next_batch_before_the_push_backlog() {
    for lanes in [1, 8] {
        let fx = BoundaryFixture::new(lanes, None);
        let id = rid(10000);
        let (sub, marker_rx) =
            Submission::new(Command::Effects(crate::rsm::planner::EffectsCommand {
                request_id: id,
                tenant: TENANT.into(),
                effects: vec![crate::rsm::effect::Effect::Noop],
            }));
        let tx = fx.tx.clone();
        fx.after_first(move || tx.try_send(sub).expect("priority arrival"));
        // On the current-thread runtime, all pushes precede the first driver poll.
        let replies = fx.backlog();
        tokio::time::timeout(Duration::from_secs(10), async {
            assert!(matches!(marker_rx.await.unwrap(), Reply::Done { .. }));
            for rx in replies {
                assert!(matches!(rx.await.unwrap(), Reply::Done { .. }));
            }
        })
        .await
        .unwrap();
        let log = fx.repl.inner.proposals();
        let position = log
            .iter()
            .position(|b| {
                decode_entry(b)
                    .unwrap()
                    .commands
                    .iter()
                    .any(|c| c.request_id == id)
            })
            .unwrap();
        assert_eq!(
            position, 1,
            "priority command must enter proposal 2, lanes={lanes}"
        );
        let ids: Vec<_> = log
            .iter()
            .flat_map(|b| {
                decode_entry(b)
                    .unwrap()
                    .commands
                    .into_iter()
                    .map(|c| c.request_id)
            })
            .filter(|r| *r != id)
            .collect();
        assert_eq!(
            ids,
            (1..=64).map(rid).collect::<Vec<_>>(),
            "push order is preserved"
        );
        fx.close().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_role_change_is_observed_before_planning_another_backlogged_batch() {
    for lanes in [1, 8] {
        let fx = BoundaryFixture::new(lanes, None);
        let fake = fx.repl.inner.clone();
        fx.after_first(move || fake.step_down(Some(9)));
        let replies = fx.backlog();
        tokio::time::timeout(Duration::from_secs(10), async {
            for (i, rx) in replies.into_iter().enumerate() {
                let reply = rx.await.unwrap();
                if i == 0 {
                    // The role notification can win over resolution of the
                    // committed first proposal. Either reply is legal here.
                    assert!(matches!(
                        reply,
                        Reply::Done { .. } | Reply::Retry { hint: Some(9) }
                    ));
                } else {
                    assert!(matches!(reply, Reply::Retry { hint: Some(9) }));
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            fx.repl.inner.proposal_count(),
            1,
            "no proposal after step-down, lanes={lanes}"
        );
        fx.close().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_quiesce_request_interrupts_a_backlog_and_resume_finishes_it() {
    for lanes in [1, 8] {
        let (qtx, qrx) = tokio::sync::mpsc::unbounded_channel();
        let fx = BoundaryFixture::new(lanes, Some(qrx));
        let (drained, drained_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume) = tokio::sync::oneshot::channel();
        fx.after_first(move || {
            qtx.send(crate::rsm::batcher::QuiesceReq { drained, resume })
                .unwrap()
        });
        let replies = fx.backlog();
        tokio::time::timeout(Duration::from_secs(10), drained_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            fx.repl.inner.proposal_count(),
            1,
            "quiesce precedes proposal 2, lanes={lanes}"
        );
        drop(resume_tx);
        tokio::time::timeout(Duration::from_secs(10), async {
            for rx in replies {
                assert!(matches!(rx.await.unwrap(), Reply::Done { .. }));
            }
        })
        .await
        .unwrap();
        fx.close().await;
    }
}
