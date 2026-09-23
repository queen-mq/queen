//! The openraft replicator: the Raft protocol from openraft, everything else
//! from the RSM.
//!
//! openraft decides who leads, which entries are committed and in what order
//! they apply. It stores nothing of its own:
//!
//! - its **log** is the queue logs ([`log_store::LogStore`]): the same group
//!   write and the same single fsync the local replicator does, the entry
//!   record now carrying its term;
//! - its **state machine** is the apply thread ([`state_machine::QueenSm`]);
//! - its **snapshot** is the store's durable checkpoint.
//!
//! openraft runs on a runtime of its own (`queen-raft-*` threads), so request
//! handling load never delays an election or a commit.
//!
//! # Proposals and the plan order (I5, WP-1.11 F-1)
//!
//! [`Replicator::propose_entry`] puts the entry on an unbounded channel in its
//! synchronous prefix — the batcher's first poll — and one task hands the
//! entries to openraft in channel order. openraft assigns indexes in the order
//! it receives writes, so the index order is the plan order.
//!
//! # Leadership (I13)
//!
//! The role this replicator reports is `Leader` only once the leader has
//! APPLIED an entry of its own term (the blank entry openraft writes on
//! election). Everything before it is then applied too, so the batcher plans
//! over complete committed state, and its predicted index for the next entry
//! (`last_log_index + 1`) is exact: nothing but the batcher appends while this
//! node leads.
//!
//! # Scope
//!
//! Single-voter clusters. The network ([`network::NoNetwork`]) and the
//! snapshot transfer are the multi-node step; the storage, the state machine,
//! recovery, votes and membership are complete here.

pub(crate) mod log_store;
mod network;
pub(crate) mod state_machine;
pub mod types;

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use openraft::async_runtime::WatchReceiver;
use openraft::errors::ClientWriteError;
use openraft::impls::ProgressResponder;
use openraft::raft::ReadPolicy;
use openraft::{ChangeMembers, ServerState, SnapshotPolicy};
use tokio::sync::{mpsc, oneshot, watch, Notify as ApplyWake};

use self::log_store::{LogStore, OpenCfg, Poison};
use self::network::NoNetwork;
use self::state_machine::QueenSm;
use self::types::{applied_log_id, rsm_index, term_of, AppEntry, Node, TypeConfig};
use super::local::{OpenConfig, PartitionLookup, Waker};
use super::{
    AppliedAt, Membership, MembershipChange, NodeId, ProposeError, ReplError, ReplMetrics,
    Replicator, Role,
};
use crate::rsm::apply::{self, ApplyStats, Notify};
use crate::rsm::qlog::set::{QLogReader, QLogSet};
use crate::rsm::segments;
use crate::rsm::store::{Store, TypedReads};

type RaftHandle<S> = openraft::Raft<TypeConfig, QueenSm<S>>;
type WriteResponder = openraft::alias::WriteResponderOf<TypeConfig>;

/// State shared by the replicator, the state machine and the apply thread's
/// notifier.
pub(crate) struct Shared {
    node_id: NodeId,
    applied_index: AtomicU64,
    applied_term: AtomicU64,
    durable_index: AtomicU64,
    proposals: AtomicU64,
    poison: Poison,
    /// RSM index → the state machine call waiting for that entry's apply.
    waiters: Mutex<HashMap<u64, oneshot::Sender<AppliedAt>>>,
    /// Pulsed when the applied index advances (the batcher's driver-notify).
    applied_notify: Arc<ApplyWake>,
}

impl Shared {
    fn poisoned(&self) -> Option<String> {
        self.poison.lock().expect("poison").clone()
    }
}

/// The apply thread's notifier. When the apply thread exits (a refused entry
/// stops the node) this is dropped with it, and every waiter is failed, so no
/// state machine call waits forever on an entry that will never apply.
struct RaftNotify {
    shared: Arc<Shared>,
    waker: Arc<dyn Waker>,
    qlog_floor: Arc<AtomicU64>,
}

impl Notify for RaftNotify {
    fn applied(&self, index: u64, term: u64, _commands: &[crate::rsm::entry::CommandRecord]) {
        self.shared.applied_index.fetch_max(index, Ordering::AcqRel);
        self.shared.applied_term.store(term, Ordering::Release);
        if let Some(tx) = self.shared.waiters.lock().expect("waiters").remove(&index) {
            let _ = tx.send(AppliedAt { index, term });
        }
        self.shared.applied_notify.notify_one();
    }

    fn wake(&self, tenant: &str, queue: &str, group: Option<&str>) {
        self.waker.wake(tenant, queue, group);
    }

    fn durable(&self, index: u64) {
        self.shared.durable_index.fetch_max(index, Ordering::AcqRel);
        // The queue logs may now give up files wholly at or below `index`.
        self.qlog_floor.fetch_max(index, Ordering::AcqRel);
    }
}

impl Drop for RaftNotify {
    fn drop(&mut self) {
        self.shared.waiters.lock().expect("waiters").clear();
    }
}

/// One proposal on its way to openraft.
struct Submit {
    app: AppEntry,
    responder: WriteResponder,
}

/// `QUEEN_RAFT_ELECTION_MS` / `QUEEN_RAFT_HEARTBEAT_MS`: openraft's timers.
fn raft_config() -> io::Result<Arc<openraft::Config>> {
    fn num(name: &str, default: u64) -> u64 {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default)
    }
    let election = num("QUEEN_RAFT_ELECTION_MS", 150);
    let config = openraft::Config {
        cluster_name: "queen".to_string(),
        heartbeat_interval: num("QUEEN_RAFT_HEARTBEAT_MS", 50),
        election_timeout_min: election,
        election_timeout_max: election * 2,
        // The store's checkpoint is the snapshot: nothing to build on a
        // schedule.
        snapshot_policy: SnapshotPolicy::Never,
        // A restarted node always runs an election, so a new term's blank entry
        // commits (and applies) everything its log holds before it plans.
        enable_leader_restore: Some(false),
        ..Default::default()
    };
    Ok(Arc::new(config.validate().map_err(|e| {
        io::Error::other(format!("openraft config: {e}"))
    })?))
}

/// The role openraft's metrics describe, with I13 applied: a leader is
/// `Leader` only once an entry of its own term has been applied.
fn role_of(m: &openraft::RaftMetrics<TypeConfig>) -> Role {
    if m.running_state.is_err() {
        return Role::Stopped;
    }
    match m.state {
        ServerState::Leader => {
            let term = m.current_term;
            if m.last_applied.as_ref().map(term_of) == Some(term) {
                Role::Leader { term }
            } else {
                Role::Candidate
            }
        }
        ServerState::Follower => Role::Follower {
            leader: m.current_leader,
        },
        ServerState::Candidate => Role::Candidate,
        ServerState::Learner => Role::Learner,
        ServerState::Shutdown => Role::Stopped,
    }
}

/// The openraft replicator. See the module header.
pub struct RaftReplicator<S: Store + 'static> {
    shared: Arc<Shared>,
    raft: Option<RaftHandle<S>>,
    rt: Option<tokio::runtime::Runtime>,
    role_rx: watch::Receiver<Role>,
    submit_tx: Option<mpsc::UnboundedSender<Submit>>,
    log: LogStore,
    writer_join: Option<JoinHandle<()>>,
    apply_join: Option<JoinHandle<apply::Result<ApplyStats>>>,
    store: Arc<S>,
    reader: segments::Reader,
    qlog_reader: QLogReader,
    qlog_codec: bool,
}

impl<S: Store + 'static> RaftReplicator<S> {
    /// Open the node: recover the log from the queue logs, start the apply
    /// thread and openraft, and return once this node leads with everything in
    /// its log applied. A boot call (blocking I/O); never on a runtime worker
    /// that must stay responsive.
    pub fn open(
        store: Arc<S>,
        cfg: OpenConfig,
        waker: Arc<dyn Waker>,
        clock: Arc<dyn apply::Clock>,
    ) -> io::Result<RaftReplicator<S>> {
        let mut apply_cfg = cfg.apply_cfg;
        if !apply_cfg.qlog {
            return Err(io::Error::other(
                "the openraft replicator keeps its log in the queue logs: QUEEN_RAFT_QLOG must be on",
            ));
        }
        let data_dir = cfg
            .seg_root
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let state_dir = data_dir.join("raft");

        let (applied, term, durable, qlog_durable) = store
            .read(|r| {
                Ok((
                    r.applied_index()?,
                    r.applied_term()?,
                    r.durable_index()?,
                    r.meta_u64(crate::rsm::store::meta::QLOG_DURABLE_INDEX)?
                        .unwrap_or(0),
                ))
            })
            .map_err(|e| io::Error::other(format!("read the store's recovery point: {e}")))?;
        if applied > 0 && !state_dir.join("state.json").exists() {
            return Err(io::Error::other(format!(
                "{} was written by the local replicator: the openraft replicator does not adopt \
                 an existing directory (start it on an empty one)",
                data_dir.display()
            )));
        }

        let qopts = crate::rsm::qlog::QLogOptions {
            segment_bytes: cfg.seg_opts.segment_bytes,
            fsync: match cfg.seg_opts.fsync {
                segments::FsyncMode::Full => crate::rsm::qlog::Fsync::Full,
                segments::FsyncMode::Data => crate::rsm::qlog::Fsync::Data,
            },
        };
        let store_for_lookup = store.clone();
        let lookup: PartitionLookup = Arc::new(move |pid| {
            store_for_lookup
                .read(|r| {
                    Ok(r.partition(pid)?
                        .map(|p| QLogSet::queue_id_of(&p.tenant, &p.queue)))
                })
                .map_err(|e| {
                    io::Error::other(format!("qlog route: partition read for pid {pid}: {e}"))
                })
        });
        let poison: Poison = Arc::new(Mutex::new(None));
        let opened = LogStore::open(OpenCfg {
            qlog_root: data_dir.join("qlog"),
            qopts,
            state_dir: state_dir.clone(),
            lookup,
            durable_index: durable,
            applied: applied_log_id(applied, term),
            qlog_durable_index: qlog_durable,
            poison: poison.clone(),
        })?;
        let log = opened.store;
        let mut writer_join = Some(opened.writer);

        let shared = Arc::new(Shared {
            node_id: cfg.node_id,
            applied_index: AtomicU64::new(applied),
            applied_term: AtomicU64::new(term),
            durable_index: AtomicU64::new(durable),
            proposals: AtomicU64::new(0),
            poison,
            waiters: Mutex::new(HashMap::new()),
            applied_notify: Arc::new(ApplyWake::new()),
        });

        // The apply thread: the queue logs are written by our log writer, never
        // by the applier.
        apply_cfg.qlog_writer_external = true;
        let (apply_tx, apply_rx) = apply::channel(cfg.apply_channel_capacity);
        let notify: Arc<dyn Notify> = Arc::new(RaftNotify {
            shared: shared.clone(),
            waker,
            qlog_floor: opened.reader.recovery_floor_handle(),
        });
        let reader_sink: Arc<OnceLock<segments::Reader>> = Arc::new(OnceLock::new());
        let mut apply_join = Some(apply::spawn_with_reader(
            store.clone(),
            cfg.seg_root.clone(),
            cfg.seg_opts,
            apply_cfg,
            notify,
            clock,
            apply_rx,
            Some(reader_sink.clone()),
        ));

        // Everything below owns a thread or a runtime: on failure, unwind in
        // order (openraft, then the log writer, then the apply thread).
        let fail = |e: io::Error,
                    rt: Option<(tokio::runtime::Runtime, Option<RaftHandle<S>>)>,
                    writer_join: &mut Option<JoinHandle<()>>,
                    apply_join: &mut Option<JoinHandle<apply::Result<ApplyStats>>>,
                    log: &LogStore| {
            if let Some((rt, raft)) = rt {
                shutdown_raft(rt, raft);
            }
            log.close();
            if let Some(j) = writer_join.take() {
                let _ = j.join();
            }
            if let Some(j) = apply_join.take() {
                let _ = j.join();
            }
            e
        };

        let reader = match wait_reader(
            &reader_sink,
            apply_join.as_ref().unwrap(),
            cfg.replay_deadline,
        ) {
            Ok(r) => r,
            Err(e) => {
                drop(apply_tx);
                return Err(fail(e, None, &mut writer_join, &mut apply_join, &log));
            }
        };

        let sm = match QueenSm::new(
            store.clone(),
            state_dir,
            apply_tx,
            shared.clone(),
            log.clone(),
        ) {
            Ok(sm) => sm,
            Err(e) => return Err(fail(e, None, &mut writer_join, &mut apply_join, &log)),
        };
        let config = match raft_config() {
            Ok(c) => c,
            Err(e) => return Err(fail(e, None, &mut writer_join, &mut apply_join, &log)),
        };
        let rt = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("queen-raft")
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => return Err(fail(e, None, &mut writer_join, &mut apply_join, &log)),
        };

        // openraft is built and initialized on a plain thread, never inside the
        // caller's runtime (a `block_on` there would panic).
        let node_id = cfg.node_id;
        let fresh = opened.fresh;
        let handle = rt.handle().clone();
        let log_for_raft = log.clone();
        let built: io::Result<RaftHandle<S>> = std::thread::scope(|s| {
            s.spawn(move || {
                handle.block_on(async move {
                    let raft = openraft::Raft::new(node_id, config, NoNetwork, log_for_raft, sm)
                        .await
                        .map_err(|e| io::Error::other(format!("openraft start: {e}")))?;
                    if fresh {
                        let members: BTreeMap<NodeId, Node> =
                            BTreeMap::from([(node_id, Node::default())]);
                        raft.initialize(members)
                            .await
                            .map_err(|e| io::Error::other(format!("openraft initialize: {e}")))?;
                    }
                    Ok(raft)
                })
            })
            .join()
            .unwrap_or_else(|_| Err(io::Error::other("openraft start panicked")))
        });
        let raft = match built {
            Ok(r) => r,
            Err(e) => {
                return Err(fail(
                    e,
                    Some((rt, None)),
                    &mut writer_join,
                    &mut apply_join,
                    &log,
                ))
            }
        };

        // The role watch, derived from openraft's metrics.
        let (role_tx, role_rx) = watch::channel(Role::Candidate);
        let mut metrics = raft.metrics();
        rt.spawn(async move {
            loop {
                let role = role_of(&metrics.borrow_watched());
                role_tx.send_if_modified(|r| {
                    if *r != role {
                        *r = role;
                        true
                    } else {
                        false
                    }
                });
                if role == Role::Stopped || metrics.changed().await.is_err() {
                    let _ = role_tx.send(Role::Stopped);
                    break;
                }
            }
        });

        // The submitter: proposals reach openraft in the order the batcher
        // first-polled them.
        let (submit_tx, mut submit_rx) = mpsc::unbounded_channel::<Submit>();
        let r2 = raft.clone();
        rt.spawn(async move {
            while let Some(s) = submit_rx.recv().await {
                if r2.client_write_ff(s.app, Some(s.responder)).await.is_err() {
                    // openraft stopped: every later proposal sees its responder
                    // dropped, and the role watch reports `Stopped`.
                    break;
                }
            }
        });

        // Wait until this node leads with everything applied (I13).
        let end = Instant::now() + cfg.replay_deadline;
        loop {
            let role = *role_rx.borrow();
            match role {
                Role::Leader { .. } => break,
                Role::Stopped => {
                    let why = shared
                        .poisoned()
                        .unwrap_or_else(|| "openraft stopped during recovery".into());
                    return Err(fail(
                        io::Error::other(why),
                        Some((rt, Some(raft))),
                        &mut writer_join,
                        &mut apply_join,
                        &log,
                    ));
                }
                _ => {}
            }
            if Instant::now() >= end {
                return Err(fail(
                    io::Error::other(format!(
                        "the node did not become a ready leader within {:?} (role {role:?})",
                        cfg.replay_deadline
                    )),
                    Some((rt, Some(raft))),
                    &mut writer_join,
                    &mut apply_join,
                    &log,
                ));
            }
            std::thread::sleep(Duration::from_millis(1));
        }

        tracing::info!(
            target: "rsm",
            node = node_id,
            store_applied = applied,
            store_durable = durable,
            recovered = opened.recovered,
            applied = shared.applied_index.load(Ordering::Acquire),
            last_log = ?log.last_log_id(),
            fresh,
            "openraft replicator open",
        );

        Ok(RaftReplicator {
            shared,
            raft: Some(raft),
            rt: Some(rt),
            role_rx,
            submit_tx: Some(submit_tx),
            log,
            writer_join,
            apply_join,
            store,
            reader,
            qlog_reader: opened.reader,
            qlog_codec: true,
        })
    }

    /// The node id.
    pub fn node_id(&self) -> NodeId {
        self.shared.node_id
    }

    /// The store, for a test's reads.
    #[cfg(test)]
    pub(crate) fn store_for_test(&self) -> Arc<S> {
        self.store.clone()
    }

    /// The segment reader for pop payloads (§7.5).
    pub fn reader(&self) -> segments::Reader {
        self.reader.clone()
    }

    /// The per-queue-log reader: pop payloads and the planner's dedup reads.
    pub fn qlog_reader(&self) -> Option<QLogReader> {
        Some(self.qlog_reader.clone())
    }

    /// Stop the node and return the apply thread's stats and the store handle.
    pub fn shutdown(mut self) -> io::Result<(ApplyStats, Arc<S>)> {
        let stats = self.stop()?;
        Ok((stats, self.store.clone()))
    }

    fn stop(&mut self) -> io::Result<ApplyStats> {
        self.submit_tx.take();
        if let Some(rt) = self.rt.take() {
            shutdown_raft(rt, self.raft.take());
        }
        // openraft has dropped its log store clones and the state machine (the
        // apply thread's only sender); ours goes too, and the threads drain.
        self.log.close();
        if let Some(j) = self.writer_join.take() {
            j.join()
                .map_err(|_| io::Error::other("raft log writer panicked"))?;
        }
        match self.apply_join.take() {
            Some(j) => match j.join() {
                Ok(Ok(stats)) => Ok(stats),
                Ok(Err(e)) => Err(io::Error::other(format!("apply thread: {e}"))),
                Err(_) => Err(io::Error::other("apply thread panicked")),
            },
            None => Ok(ApplyStats::default()),
        }
    }

    fn metrics_now(&self) -> Option<openraft::RaftMetrics<TypeConfig>> {
        self.raft
            .as_ref()
            .map(|r| r.metrics().borrow_watched().clone())
    }
}

impl<S: Store + 'static> Drop for RaftReplicator<S> {
    fn drop(&mut self) {
        if self.rt.is_some() || self.writer_join.is_some() || self.apply_join.is_some() {
            let _ = self.stop();
        }
    }
}

/// Stop openraft and its runtime from a plain thread (never inside a runtime).
fn shutdown_raft<S: Store + 'static>(rt: tokio::runtime::Runtime, raft: Option<RaftHandle<S>>) {
    std::thread::scope(|s| {
        s.spawn(move || {
            if let Some(raft) = raft {
                let _ = rt.block_on(raft.shutdown());
                drop(raft);
            }
            rt.shutdown_timeout(Duration::from_secs(5));
        });
    });
}

/// Wait for the apply thread to publish its segment reader.
fn wait_reader(
    sink: &OnceLock<segments::Reader>,
    apply_join: &JoinHandle<apply::Result<ApplyStats>>,
    deadline: Duration,
) -> io::Result<segments::Reader> {
    let end = Instant::now() + deadline;
    loop {
        if let Some(r) = sink.get() {
            return Ok(r.clone());
        }
        if apply_join.is_finished() {
            return Err(io::Error::other(
                "apply thread exited before it published the segment reader",
            ));
        }
        if Instant::now() >= end {
            return Err(io::Error::other(
                "apply thread did not publish the segment reader in time",
            ));
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn repl_err<E: std::fmt::Display>(e: E) -> ReplError {
    ReplError::Fatal(e.to_string())
}

#[async_trait]
impl<S: Store + 'static> Replicator for RaftReplicator<S> {
    async fn propose(&self, entry: Bytes, deadline: Instant) -> Result<AppliedAt, ProposeError> {
        let decoded = crate::rsm::entry::decode_entry(&entry)
            .map_err(|e| ProposeError::Refused(format!("proposal does not decode: {e:?}")))?;
        self.propose_entry(entry, Arc::new(decoded), deadline).await
    }

    fn wants_bytes(&self) -> bool {
        false
    }

    async fn propose_entry(
        &self,
        _entry: Bytes,
        planned: Arc<crate::rsm::entry::Entry>,
        deadline: Instant,
    ) -> Result<AppliedAt, ProposeError> {
        // Everything up to the channel send runs in the caller's FIRST poll:
        // that is what fixes the index order to the plan order.
        if let Some(why) = self.shared.poisoned() {
            return Err(ProposeError::Fatal(why));
        }
        match *self.role_rx.borrow() {
            Role::Leader { .. } => {}
            Role::Stopped => return Err(ProposeError::Fatal("openraft stopped".into())),
            r => {
                return Err(ProposeError::NotLeader {
                    hint: r.leader_hint(),
                })
            }
        }
        let pre = if self.qlog_codec {
            planned
                .effects
                .iter()
                .enumerate()
                .filter_map(|(i, eff)| match eff {
                    crate::rsm::effect::Effect::Append { .. } => {
                        Some(crate::rsm::qlog::codec::Pre::start_append(&planned, i))
                    }
                    _ => None,
                })
                .collect()
        } else {
            Vec::new()
        };
        let (responder, rx) = ProgressResponder::<TypeConfig, _>::complete_only();
        let sent = match self.submit_tx.as_ref() {
            Some(tx) => tx
                .send(Submit {
                    app: AppEntry::proposed(planned, pre),
                    responder,
                })
                .is_ok(),
            None => false,
        };
        if !sent {
            return Err(ProposeError::Fatal(
                "the openraft replicator is stopping".into(),
            ));
        }
        self.shared.proposals.fetch_add(1, Ordering::Relaxed);

        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), rx).await {
            // I3: still in flight; it may commit. The batcher holds on it.
            Err(_elapsed) => Err(ProposeError::Timeout),
            Ok(Err(_dropped)) => Err(match self.shared.poisoned() {
                Some(why) => ProposeError::Fatal(why),
                None => ProposeError::OutcomeUnknown,
            }),
            Ok(Ok(Ok(resp))) => Ok(AppliedAt {
                index: rsm_index(resp.log_id.index),
                term: term_of(&resp.log_id),
            }),
            Ok(Ok(Err(ClientWriteError::ForwardToLeader(f)))) => {
                Err(ProposeError::NotLeader { hint: f.leader_id })
            }
            // Appended, then discarded on a leader change: it may still commit
            // under the new leader (GH#2095). The request id finds it on retry.
            Ok(Ok(Err(ClientWriteError::LogEntryDiscarded(_)))) => {
                Err(ProposeError::OutcomeUnknown)
            }
            Ok(Ok(Err(e))) => Err(ProposeError::Refused(e.to_string())),
        }
    }

    fn role(&self) -> Role {
        *self.role_rx.borrow()
    }

    fn watch_role(&self) -> watch::Receiver<Role> {
        self.role_rx.clone()
    }

    fn applied_notify(&self) -> Option<Arc<ApplyWake>> {
        Some(self.shared.applied_notify.clone())
    }

    async fn read_barrier(&self, deadline: Instant) -> Result<u64, ProposeError> {
        if let Some(why) = self.shared.poisoned() {
            return Err(ProposeError::Fatal(why));
        }
        let Some(raft) = self.raft.as_ref() else {
            return Err(ProposeError::Fatal("openraft stopped".into()));
        };
        let read = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            raft.ensure_linearizable(ReadPolicy::ReadIndex),
        )
        .await
        .map_err(|_| ProposeError::Timeout)?
        .map_err(|e| match e.forward_to_leader() {
            Some(f) => ProposeError::NotLeader { hint: f.leader_id },
            None => ProposeError::Refused(e.to_string()),
        })?;
        // The read index is an openraft index; the answer is the RSM index the
        // caller must see applied before it reads (openraft waited for it).
        Ok(rsm_index(read.index()))
    }

    fn applied_index(&self) -> u64 {
        self.shared.applied_index.load(Ordering::Acquire)
    }

    async fn transfer_leadership(
        &self,
        to: Option<NodeId>,
        _deadline: Instant,
    ) -> Result<(), ReplError> {
        let Some(to) = to.filter(|n| *n != self.shared.node_id) else {
            return Ok(());
        };
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| repl_err("openraft stopped"))?;
        raft.trigger().transfer_leader(to).await.map_err(repl_err)
    }

    async fn membership(&self) -> Membership {
        match self.metrics_now() {
            Some(m) => {
                let mc = m.membership_config.membership();
                Membership {
                    voters: mc.voter_ids().collect(),
                    learners: mc.learner_ids().collect(),
                }
            }
            None => Membership::single(self.shared.node_id),
        }
    }

    async fn change_membership(
        &self,
        change: MembershipChange,
        _deadline: Instant,
    ) -> Result<(), ReplError> {
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| repl_err("openraft stopped"))?;
        match change {
            MembershipChange::AddLearner { node, addr } => raft
                .add_learner(node, Node { addr }, true)
                .await
                .map(|_| ())
                .map_err(repl_err),
            MembershipChange::Promote { node } => raft
                .change_membership(ChangeMembers::AddVoterIds([node].into()), false)
                .await
                .map(|_| ())
                .map_err(repl_err),
            MembershipChange::Remove { node } => raft
                .change_membership(ChangeMembers::RemoveVoters([node].into()), false)
                .await
                .map(|_| ())
                .map_err(repl_err),
        }
    }

    fn metrics(&self) -> ReplMetrics {
        let applied = self.shared.applied_index.load(Ordering::Acquire);
        let (files, bytes) = self.qlog_reader.totals();
        let role = *self.role_rx.borrow();
        let m = self.metrics_now();
        let last_log = m
            .as_ref()
            .and_then(|m| m.last_log_index)
            .map(rsm_index)
            .unwrap_or(0);
        ReplMetrics {
            term: m.as_ref().map(|m| m.current_term).unwrap_or(0),
            leader: m.as_ref().and_then(|m| m.current_leader),
            is_leader: role.is_leader(),
            last_log_index: last_log,
            committed_index: m
                .as_ref()
                .and_then(|m| m.local_committed.as_ref())
                .map(|c| rsm_index(c.index))
                .unwrap_or(0),
            applied_index: applied,
            durable_index: self.shared.durable_index.load(Ordering::Acquire),
            inflight: last_log.saturating_sub(applied),
            proposals: self.shared.proposals.load(Ordering::Relaxed),
            log_files: files,
            log_bytes: bytes,
        }
    }
}
