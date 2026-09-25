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
//! - its **snapshot** is the store's durable checkpoint, copied with the queue
//!   logs when a follower needs one ([`snapshot`]).
//!
//! openraft runs on a runtime of its own (`queen-raft-*` threads), with the
//! Raft RPC server of a cluster node, so request handling load never delays an
//! election or a commit.
//!
//! # One node or a cluster
//!
//! Without `QUEEN_RAFT_PEERS` the node is a single voter with no network. With
//! it ([`cluster::ClusterConfig`]) the node serves the Raft RPCs on
//! `QUEEN_RAFT_LISTEN` and reaches its peers over HTTP ([`network`]). Only the
//! leader plans and proposes; a follower's facade forwards client requests to
//! the leader (`handlers/raft.rs`).
//!
//! # Changing the membership, and the repairs an operator can make
//!
//! - The membership changes (`/api/v1/system/raft/membership/*`): add a learner,
//!   promote learners, set the voters, remove a member — checked on the leader
//!   so that none leaves the cluster without a quorum ([`admin`]); a follower
//!   forwards them there. Once committed, the membership in the log is the
//!   cluster's, whatever `QUEEN_RAFT_PEERS` lists ([`cluster`]).
//! - A node that joins an existing cluster never initializes one
//!   ([`RaftOpts::join`]).
//! - `QUEEN_RAFT_FORCE_RECOVER`: a survivor of a lost majority becomes its
//!   cluster's only voter, at boot ([`repair`]).
//! - `QUEEN_RAFT_APPLY_SKIP`: an entry apply refuses deterministically is
//!   replaced by a skip marker in every node's log, at boot, and applies as a
//!   no-op ([`repair`], `apply::execute_skip_marker`).
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
//! # The log's life
//!
//! An entry stays in memory until it is applied here AND every live follower
//! has it (or the cache is over its cap), then lives on in the queue logs. A
//! background step (the purge driver in [`watch`]) builds a snapshot at the
//! store's durable index and lets openraft purge the log behind it — but never
//! past what a live follower still needs. Retention reclaims queue-log files
//! only below the purge point ([`log_store::FloorGate`]). A follower that was
//! away for longer than `QUEEN_RAFT_PURGE_HOLD_S` gets a snapshot instead.

mod admin;
pub mod cluster;
pub(crate) mod log_store;
mod members;
mod network;
mod repair;
pub mod snapshot;
pub(crate) mod state_machine;
pub mod types;
mod wire;

pub use self::admin::MembershipStatus;
pub use self::repair::SkipSpec;

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
use openraft::errors::{ClientWriteError, InitializeError, RaftError};
use openraft::impls::ProgressResponder;
use openraft::raft::ReadPolicy;
use openraft::{ServerState, SnapshotPolicy};
use tokio::sync::{mpsc, oneshot, watch, Notify as ApplyWake};

pub use self::cluster::ClusterConfig;
use self::log_store::{FloorGate, LogStore, OpenCfg, Poison};
use self::network::{HttpNetwork, NoNetwork};
use self::snapshot::{RecvCtx, Restart, SendCtx};
use self::state_machine::QueenSm;
use self::types::{applied_log_id, rsm_index, term_of, AppEntry, Node, QueenNode, TypeConfig};
use super::local::{OpenConfig, PartitionLookup, Waker};
use super::{
    AppliedAt, Membership, MembershipChange, NodeId, ProposeError, ReplError, ReplMetrics,
    Replicator, Role,
};
use crate::rsm::apply::{self, ApplyStats, Notify};
use crate::rsm::qlog::set::{QLogReader, QLogSet};
use crate::rsm::segments;
use crate::rsm::store::{Store, TypedReads};

pub(crate) type RaftHandle<S> = openraft::Raft<TypeConfig, QueenSm<S>>;
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
    /// The applied index, for every waiter at once ([`RaftReplicator::wait_applied`]:
    /// `applied_notify` wakes ONE waiter per pulse, the batcher's).
    applied_watch: watch::Sender<u64>,
    /// On the leader: the openraft index every live follower has replicated
    /// (the cache keeps entries above it). `u64::MAX` otherwise.
    replicated: AtomicU64,
    /// RSM index → term, at every index where the applied term changed: the
    /// term of the store's durable index, for a snapshot.
    terms: Mutex<BTreeMap<u64, u64>>,
    last_term: AtomicU64,
    /// Set when a received snapshot waits for a restart.
    restart: Arc<Restart>,
    /// The leader's client address while another node leads, kept by the
    /// watch task: a follower forwards every client request there.
    leader_http: std::sync::RwLock<Option<Arc<str>>>,
    /// The leader's Raft RPC address while another node leads: where a
    /// follower sends its prepared commands and read-index requests.
    leader_raft: std::sync::RwLock<Option<Arc<str>>>,
    /// Plans a follower's prepared command on this node (set by the facade).
    remote: Arc<std::sync::OnceLock<RemoteHandler>>,
    /// Answers a peer's gather of this node's node-local dashboard data
    /// (`/raft/v1/local`, set by the facade; PLAN_RAFT.md D17).
    local: Arc<std::sync::OnceLock<RemoteHandler>>,
    /// Who is in the cluster and when the leader last heard from each member:
    /// published on every append while this node leads, received on the
    /// leader's appends while it follows ([`members`]).
    members: Arc<members::MembersState>,
    /// The operator's membership changes ([`admin`]): one at a time, and the
    /// thresholds their checks use. Shared with the Raft RPC server, which runs
    /// the changes a follower forwards.
    admin: Arc<admin::AdminCtx>,
    /// The entry apply refused, when it did (this node then stops).
    apply_failure: Mutex<Option<apply::ApplyFailure>>,
    /// `<data_dir>/raft`: the vote, the commit point, and the operator
    /// repairs' records ([`repair`]).
    state_dir: PathBuf,
}

/// One member of the cluster as a node sees it ([`RaftReplicator::cluster_view`]).
#[derive(Clone, Debug, Default)]
pub struct MemberView {
    pub node_id: NodeId,
    pub raft_addr: String,
    pub http_addr: String,
    pub voter: bool,
    /// On the leader: the RSM index this member has replicated (0 when it has
    /// nothing yet); `None` on a follower or for the leader itself.
    pub matched: Option<u64>,
    /// On the leader: milliseconds since this member last acknowledged a
    /// heartbeat.
    pub heartbeat_age_ms: Option<u64>,
}

/// The membership as one node sees it.
#[derive(Clone, Debug, Default)]
pub struct ClusterView {
    pub node_id: NodeId,
    pub leader: Option<NodeId>,
    pub term: u64,
    pub members: Vec<MemberView>,
}

/// Plans one follower's prepared command on the leader and answers the encoded
/// reply (the facade's; the body carries its own deadline).
pub type RemoteHandler = Arc<
    dyn Fn(
            bytes::Bytes,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<bytes::Bytes, String>> + Send>,
        > + Send
        + Sync,
>;

/// Why a call to the leader did not get an answer.
#[derive(Debug)]
pub enum RemoteError {
    /// No leader is known (an election), or this node is not in a cluster.
    NoLeader,
    /// The leader could not be reached or did not answer: retry.
    Transport(String),
}

/// `QUEEN_RAFT_CLIENT_OFFLOAD` (default on): a follower serves its clients
/// itself — parses, prepares, sends the leader only the prepared command, and
/// renders the answer from its own state — instead of proxying the whole HTTP
/// request to the leader.
pub fn client_offload_from_env() -> bool {
    match std::env::var("QUEEN_RAFT_CLIENT_OFFLOAD") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        Err(_) => true,
    }
}

/// `QUEEN_RAFT_FWD_INFLIGHT` (default 4096): calls from this node to the
/// leader in flight at once (forwarded commands, read indexes). Each holds a
/// connection, and one destination has ~28k ephemeral ports. Raft's own RPCs
/// are not gated.
fn forward_gate() -> &'static tokio::sync::Semaphore {
    static GATE: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
    GATE.get_or_init(|| {
        tokio::sync::Semaphore::new(env_u64("QUEEN_RAFT_FWD_INFLIGHT", 4096) as usize)
    })
}

impl Shared {
    fn poisoned(&self) -> Option<String> {
        self.poison.lock().expect("poison").clone()
    }

    pub(crate) fn replicated(&self) -> u64 {
        self.replicated.load(Ordering::Acquire)
    }

    /// The store's durable index as last reported.
    pub(crate) fn durable(&self) -> u64 {
        self.durable_index.load(Ordering::Acquire)
    }

    /// The term of the entry at RSM index `index` (at or above the store's
    /// applied index at open, and applied since).
    pub(crate) fn term_at(&self, index: u64) -> Option<u64> {
        self.terms
            .lock()
            .expect("terms")
            .range(..=index)
            .next_back()
            .map(|(_, t)| *t)
    }

    fn note_term(&self, index: u64, term: u64) {
        if self.last_term.swap(term, Ordering::AcqRel) != term {
            let mut t = self.terms.lock().expect("terms");
            t.insert(index, term);
            while t.len() > 1024 {
                t.pop_first();
            }
        }
    }
}

/// The apply thread's notifier. When the apply thread exits (a refused entry
/// stops the node) this is dropped with it, and every waiter is failed, so no
/// state machine call waits forever on an entry that will never apply.
struct RaftNotify {
    shared: Arc<Shared>,
    waker: Arc<dyn Waker>,
    gate: Arc<FloorGate>,
    /// This node's raft group (`QUEEN_RAFT_GROUPS`), for the skip setting.
    group: usize,
}

impl Notify for RaftNotify {
    fn applied(&self, index: u64, term: u64, _commands: &[crate::rsm::entry::CommandRecord]) {
        self.shared.applied_index.fetch_max(index, Ordering::AcqRel);
        self.shared.applied_term.store(term, Ordering::Release);
        self.shared.note_term(index, term);
        if let Some(tx) = self.shared.waiters.lock().expect("waiters").remove(&index) {
            let _ = tx.send(AppliedAt { index, term });
        }
        self.shared.applied_notify.notify_one();
        self.shared.applied_watch.send_replace(index);
    }

    fn wake(&self, tenant: &str, queue: &str, group: Option<&str>) {
        self.waker.wake(tenant, queue, group);
    }

    fn wants_append_wakes(&self) -> bool {
        self.waker.wants_append_wakes()
    }

    fn wants_appended(&self) -> bool {
        self.waker.wants_appended()
    }

    fn appended(&self, tenant: &str, queue: &str, partition: &str) {
        self.waker.appended(tenant, queue, partition);
    }

    fn durable(&self, index: u64) {
        self.shared.durable_index.fetch_max(index, Ordering::AcqRel);
        // The queue logs may now give up files wholly at or below `index` —
        // and below what openraft purged.
        self.gate.durable(index);
    }

    fn failed(&self, failure: &apply::ApplyFailure) {
        // Recorded where the next boot's QUEEN_RAFT_APPLY_SKIP checks read it.
        if let Err(e) = repair::record_failure(&self.shared.state_dir, failure) {
            tracing::warn!(target: "rsm", error = %e, "raft: could not record the apply failure");
        }
        if failure.deterministic() {
            let setting = match self.group {
                0 => failure.skip_value(),
                g => format!("g{g}/{}", failure.skip_value()),
            };
            tracing::error!(
                target: "rsm",
                node = self.shared.node_id,
                group = self.group,
                "raft node {} stops: set QUEEN_RAFT_APPLY_SKIP={setting} on EVERY node to step \
                 over entry {} (term {}, digest {}), then restart them all",
                self.shared.node_id,
                failure.index,
                failure.term,
                failure.digest,
            );
        }
        *self.shared.apply_failure.lock().expect("apply failure") = Some(failure.clone());
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

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

/// `QUEEN_RAFT_ELECTION_MS` / `QUEEN_RAFT_HEARTBEAT_MS`: openraft's timers.
/// A cluster defaults to a 100 ms heartbeat and a 1-2 s election timeout
/// (a leader under a disk stall is not deposed by a slow fsync); a single
/// voter elects itself at once.
fn raft_config(cluster: bool) -> io::Result<Arc<openraft::Config>> {
    let (hb, el) = if cluster { (100, 1000) } else { (50, 150) };
    let election = env_u64("QUEEN_RAFT_ELECTION_MS", el);
    let config = openraft::Config {
        cluster_name: "queen".to_string(),
        heartbeat_interval: env_u64("QUEEN_RAFT_HEARTBEAT_MS", hb),
        election_timeout_min: election,
        election_timeout_max: election * 2,
        // Snapshots are built by the purge driver, never on a schedule.
        snapshot_policy: SnapshotPolicy::Never,
        // ...and the purge driver alone purges ([`purge_step`]: the floor of
        // what live members hold, minus `QUEEN_RAFT_LOG_KEEP`). openraft's own
        // purge after every snapshot build keeps only this many entries below
        // the snapshot (default 1000), whatever a lagging member still needs:
        // with it, followers purged 1000 below each snapshot and a new leader
        // had to send full snapshots to members the old one served from its
        // log. u64::MAX turns it off; `trigger().purge_log` ignores it.
        max_in_snapshot_log_to_keep: u64::MAX,
        // A restarted node always runs an election, so a new term's blank entry
        // commits (and applies) everything its log holds before it plans.
        enable_leader_restore: Some(false),
        // A partitioned node that comes back does not depose a healthy leader.
        enable_pre_vote: Some(cluster),
        // Entries are batches (up to the batcher's byte cap each); the wire
        // cuts a request at wire::MAX_APPEND_BYTES too.
        max_payload_entries: 64,
        // A snapshot is streamed whole; the transport has its own deadline.
        install_snapshot_timeout: 3_600_000,
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

/// The background step's knobs.
struct WatchOpts {
    /// Exit the process when a received snapshot needs a restart (the binary;
    /// tests reopen the node instead).
    exit_on_restart: bool,
    /// `QUEEN_RAFT_PURGE_HOLD_S` (default 600): how long a follower that makes
    /// no progress still holds the log back from being purged.
    hold: Duration,
    /// `QUEEN_RAFT_LOG_KEEP` (default 4096): entries kept below the purge point
    /// anyway, for a follower a moment behind.
    keep: u64,
    /// Purge in steps of at least this many entries.
    batch: u64,
    /// The member this group would rather have lead ([`ClusterConfig::for_group`]):
    /// a leader that is not it hands leadership over once it is caught up.
    preferred: Option<NodeId>,
    /// A leader with no quorum acknowledgement for this long hands its
    /// leadership away ([`quorum_step`]); zero: never.
    quorum_loss: Duration,
}

/// How a node runs beyond its [`OpenConfig`]. [`RaftOpts::from_env`] is the
/// binary's; a test sets what it exercises.
#[derive(Clone, Debug)]
pub struct RaftOpts {
    /// Exit the process when a received snapshot needs a restart to be loaded
    /// (the binary). Otherwise the node just stops and
    /// [`RaftReplicator::restart_requested`] says why.
    pub exit_on_restart: bool,
    /// `QUEEN_RAFT_PURGE_HOLD_S` (default 600 s): how long a follower that
    /// makes no progress still holds the log back from being purged.
    pub purge_hold: Duration,
    /// `QUEEN_RAFT_LOG_KEEP` (default 4096): entries kept below the purge point
    /// anyway, for a follower a moment behind.
    pub log_keep: u64,
    /// Purge in steps of at least this many entries (1024).
    pub purge_batch: u64,
    /// `QUEEN_RAFT_LOG_CACHE_MB` (default 512 MiB): over this many cached
    /// bytes, applied entries leave memory even if a follower still needs them.
    pub cache_cap: usize,
    /// `QUEEN_RAFT_JOIN` (default off): this node joins a cluster that already
    /// exists. On its first start (an empty data directory) it does NOT
    /// initialize a cluster from `QUEEN_RAFT_PEERS`, and it does not wait for a
    /// leader: it serves 503 until the leader adds it
    /// (`POST /api/v1/system/raft/membership/learners`) and replicates to it. A fresh
    /// node without it still asks its peers first and joins when one of them
    /// already holds cluster state ([`network`] `/raft/v1/state`).
    pub join: bool,
    /// `QUEEN_RAFT_FORCE_RECOVER=<this node's id>`: the unsafe single-survivor
    /// recovery, at boot ([`repair`]). Refused unless it names this node.
    pub force_recover: Option<NodeId>,
    /// `QUEEN_RAFT_APPLY_SKIP=[g<group>/]<index>[:<digest>],...`: entries this
    /// node rewrites into skip markers at boot ([`repair`]); only this node's
    /// group's are used.
    pub apply_skip: Vec<SkipSpec>,
    /// `QUEEN_RAFT_PROMOTE_MAX_LAG` (default 1000): the most entries behind the
    /// leader a learner may be and still be promoted without `force`.
    pub promote_max_lag: u64,
}

impl RaftOpts {
    pub fn from_env(exit_on_restart: bool) -> io::Result<RaftOpts> {
        let force_recover = match std::env::var("QUEEN_RAFT_FORCE_RECOVER") {
            Ok(v) if !v.trim().is_empty() => Some(v.trim().parse::<NodeId>().map_err(|_| {
                io::Error::other(format!(
                    "QUEEN_RAFT_FORCE_RECOVER={v}: expected this node's id (a number)"
                ))
            })?),
            _ => None,
        };
        let apply_skip =
            repair::parse_apply_skip(&std::env::var("QUEEN_RAFT_APPLY_SKIP").unwrap_or_default())
                .map_err(io::Error::other)?;
        let join = std::env::var("QUEEN_RAFT_JOIN").is_ok_and(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "on" | "yes"
            )
        });
        Ok(RaftOpts {
            exit_on_restart,
            purge_hold: Duration::from_secs(env_u64("QUEEN_RAFT_PURGE_HOLD_S", 600)),
            log_keep: env_u64("QUEEN_RAFT_LOG_KEEP", 4096),
            purge_batch: 1024,
            cache_cap: log_store::cache_cap_from_env(),
            join,
            force_recover,
            apply_skip,
            promote_max_lag: admin::promote_max_lag_from_env(),
        })
    }
}

impl WatchOpts {
    fn of(o: &RaftOpts) -> WatchOpts {
        WatchOpts {
            exit_on_restart: o.exit_on_restart,
            hold: o.purge_hold,
            keep: o.log_keep,
            batch: o.purge_batch.max(1),
            preferred: None,
            quorum_loss: Duration::ZERO,
        }
    }
}

/// On the leader: the openraft index every LIVE follower has replicated. A
/// follower is live while it is caught up, or answered a heartbeat or made
/// progress within `hold` — one that is up but behind (catching up, or
/// installing a snapshot) keeps the log it still needs, so the purge never
/// overtakes a follower that is working to catch up. `u64::MAX` when this
/// node is not leading (or leads alone).
fn replicated_floor(
    m: &openraft::RaftMetrics<TypeConfig>,
    seen: &mut HashMap<NodeId, (Option<u64>, Instant)>,
    hold: Duration,
) -> u64 {
    let Some(rep) = &m.replication else {
        seen.clear();
        return u64::MAX;
    };
    let now = Instant::now();
    let mut floor = u64::MAX;
    for (id, matched) in rep.iter() {
        if *id == m.id {
            continue;
        }
        let idx = matched.as_ref().map(|l| l.index);
        let e = seen.entry(*id).or_insert((idx, now));
        if e.0 != idx {
            *e = (idx, now);
        }
        let caught_up = idx.is_some() && idx == m.last_log_index;
        let acked = m
            .heartbeat
            .as_ref()
            .and_then(|h| h.get(id))
            .and_then(|t| t.as_ref())
            .is_some_and(|t| openraft::Instant::elapsed(&**t) < hold);
        if caught_up || acked || now.duration_since(e.1) < hold {
            floor = floor.min(idx.unwrap_or(0));
        }
    }
    seen.retain(|id, _| rep.contains_key(id));
    floor
}

/// One purge-driver step: snapshot at the store's durable index, then purge
/// the log up to it — never past what a live follower needs, and keeping
/// `keep` entries below that anyway.
async fn purge_step<S: Store + 'static>(
    raft: &RaftHandle<S>,
    shared: &Shared,
    m: &openraft::RaftMetrics<TypeConfig>,
    floor: u64,
    opts: &WatchOpts,
) {
    let Some(applied) = m.last_applied.as_ref().map(|l| l.index) else {
        return;
    };
    let durable = shared.durable();
    if durable == 0 {
        return;
    }
    let upto = (durable - 1)
        .min(applied)
        .min(floor)
        .saturating_sub(opts.keep);
    let next = m.purged.as_ref().map_or(0, |p| p.index + 1);
    if upto < next + opts.batch {
        return;
    }
    match m.snapshot.as_ref().map(|s| s.index) {
        Some(s) if s >= upto => {
            if let Err(e) = raft.trigger().purge_log(upto).await {
                tracing::warn!(target: "rsm", error = %e, "raft purge");
            }
        }
        // The purge follows on the next step, once the snapshot is built.
        _ => {
            if let Err(e) = raft.trigger().snapshot().await {
                tracing::warn!(target: "rsm", error = %e, "raft snapshot");
            }
        }
    }
}

/// The background task of a node: the role watch (I13), the followers'
/// progress (cache eviction), the purge driver, and the restart a received
/// snapshot needs.
async fn watch<S: Store + 'static>(
    raft: RaftHandle<S>,
    shared: Arc<Shared>,
    log: LogStore,
    role_tx: watch::Sender<Role>,
    opts: WatchOpts,
) {
    let mut metrics = raft.metrics();
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut seen: HashMap<NodeId, (Option<u64>, Instant)> = HashMap::new();
    let mut last_handoff: Option<Instant> = None;
    let mut last_quorum_step: Option<Instant> = None;
    loop {
        let m = metrics.borrow_watched().clone();
        shared.members.note(&m);
        let role = role_of(&m);
        let leader_node = m
            .current_leader
            .filter(|l| *l != m.id)
            .and_then(|l| m.membership_config.membership().get_node(&l).cloned());
        let leader_http: Option<Arc<str>> = leader_node
            .as_ref()
            .map(|n| n.http.clone())
            .filter(|h| !h.is_empty())
            .map(Arc::from);
        let leader_raft: Option<Arc<str>> = leader_node
            .as_ref()
            .map(|n| n.raft.clone())
            .filter(|h| !h.is_empty())
            .map(Arc::from);
        {
            let mut cur = shared.leader_http.write().expect("leader_http");
            if *cur != leader_http {
                *cur = leader_http;
            }
        }
        {
            let mut cur = shared.leader_raft.write().expect("leader_raft");
            if *cur != leader_raft {
                *cur = leader_raft;
            }
        }
        role_tx.send_if_modified(|r| {
            if *r != role {
                *r = role;
                true
            } else {
                false
            }
        });
        if role == Role::Stopped {
            if let Some(why) = shared.restart.requested() {
                if opts.exit_on_restart {
                    tracing::error!(target: "rsm", why, "raft: the process exits to load a received snapshot");
                    std::process::exit(75);
                }
                tracing::warn!(target: "rsm", why, "raft: a received snapshot waits for a restart");
            }
            break;
        }
        let floor = replicated_floor(&m, &mut seen, opts.hold);
        let before = shared.replicated.swap(floor, Ordering::AcqRel);
        if floor != before {
            if let Some(la) = m.last_applied.as_ref() {
                log.evict(la.index, floor);
            }
        }
        tokio::select! {
            r = metrics.changed() => {
                if r.is_err() {
                    let _ = role_tx.send(Role::Stopped);
                    break;
                }
            }
            _ = tick.tick() => {
                // A follower purges by the floor its leader keeps, from the
                // leader's view received on the appends: whoever is elected
                // next still holds what a lagging member needs. No view yet:
                // nothing is purged.
                let purge_floor = if m.replication.is_some() {
                    floor
                } else {
                    shared.members.follower_floor(m.id, opts.hold).unwrap_or(0)
                };
                purge_step(&raft, &shared, &m, purge_floor, &opts).await;
                prefer_step(&raft, &shared.members, &m, &opts, &mut last_handoff).await;
                quorum_step(&raft, &shared.members, &m, &opts, &mut last_quorum_step).await;
            }
        }
    }
}

/// A node that handed its leadership away is not handed it back for this long:
/// it is leaving (the binary hands off on SIGTERM,
/// [`RaftReplicator::hand_off_leadership`]), and leading again would end in an
/// election when it exits. Longer than a drain; the node that comes back from
/// the restart is preferred again once it has passed.
const HANDED_OFF_HOLD: Duration = Duration::from_secs(60);

/// A member is handed leadership only if it acknowledged an RPC this recently
/// (a cluster heartbeats every 100 ms). The leader keeps a member's matched
/// index after the member stops, and a transfer to a stopped node leaves the
/// group without a leader until the election timeout.
const HAND_TO_ACKED_WITHIN: Duration = Duration::from_secs(1);

/// How often a hand-off asks again while no other node has taken over: a
/// target a few entries behind ignores the request, and the leader takes no
/// writes meanwhile, so it has them by the next one.
const HAND_OFF_RETRY: Duration = Duration::from_millis(100);

/// Whether the leader's metrics show `id` acknowledging an RPC within `within`.
fn acked_within(m: &openraft::RaftMetrics<TypeConfig>, id: NodeId, within: Duration) -> bool {
    matches!(
        m.heartbeat.as_ref().and_then(|h| h.get(&id)),
        Some(Some(t)) if openraft::Instant::elapsed(&**t) < within
    )
}

/// The voter, other than this leader, with the most of its log: among those
/// that acknowledged an RPC within [`HAND_TO_ACKED_WITHIN`] and did not just
/// hand leadership away themselves.
fn most_caught_up(
    m: &openraft::RaftMetrics<TypeConfig>,
    members: &members::MembersState,
) -> Option<NodeId> {
    let voters: std::collections::BTreeSet<NodeId> =
        m.membership_config.membership().voter_ids().collect();
    m.replication
        .as_ref()?
        .iter()
        .filter(|(id, _)| {
            **id != m.id
                && voters.contains(*id)
                && acked_within(m, **id, HAND_TO_ACKED_WITHIN)
                && !members.handed_off_within(**id, HANDED_OFF_HOLD)
        })
        .max_by_key(|(_, matched)| matched.as_ref().map(|l| l.index))
        .map(|(id, _)| *id)
}

/// The voter, other than this leader, with the most of its log, heard from or
/// not.
fn furthest_voter(m: &openraft::RaftMetrics<TypeConfig>) -> Option<NodeId> {
    let rep = m.replication.as_ref();
    m.membership_config
        .membership()
        .voter_ids()
        .filter(|id| *id != m.id)
        .max_by_key(|id| {
            rep.and_then(|r| r.get(id))
                .and_then(|l| l.as_ref())
                .map(|l| l.index)
        })
}

/// openraft keeps a leader in office for as long as its followers hear it,
/// whether or not their answers reach it: a leader that can send but not
/// receive commits nothing, and the followers, still receiving its appends,
/// never elect another (openraft GH#2080; Jepsen P5 `leader-deaf`: every write
/// stalled for the whole fault, 25 s in `repro/leader-deaf-stall.sh`). A leader
/// with no quorum acknowledgement for [`WatchOpts::quorum_loss`] hands its
/// leadership to the most caught-up voter: the transfer travels the direction
/// that still works, and the voters elect the target. At most once per
/// `quorum_loss`; a leader cut off both ways only stops heartbeating, and the
/// other side elects as it would anyway.
async fn quorum_step<S: Store + 'static>(
    raft: &RaftHandle<S>,
    members: &members::MembersState,
    m: &openraft::RaftMetrics<TypeConfig>,
    opts: &WatchOpts,
    last: &mut Option<Instant>,
) {
    if opts.quorum_loss.is_zero()
        || m.state != ServerState::Leader
        || m.membership_config.membership().voter_ids().count() < 2
        || last.is_some_and(|t| t.elapsed() < opts.quorum_loss)
    {
        return;
    }
    let silent = match m.last_quorum_acked.as_ref() {
        Some(t) => openraft::Instant::elapsed(&**t),
        None => match members.leading_for(m.current_term) {
            Some(d) => d,
            None => return,
        },
    };
    if silent < opts.quorum_loss {
        return;
    }
    let Some(to) = most_caught_up(m, members).or_else(|| furthest_voter(m)) else {
        return;
    };
    *last = Some(Instant::now());
    match raft.trigger().transfer_leader(to).await {
        Ok(()) => tracing::warn!(
            target: "rsm",
            to,
            silent_ms = silent.as_millis() as u64,
            "raft: no quorum acknowledgement: handing leadership to another voter",
        ),
        Err(e) => tracing::warn!(target: "rsm", to, error = %e, "raft: quorum-loss hand-off"),
    }
}

/// Several groups per process spread their leaders over the nodes: a leader
/// that is not its group's preferred member hands leadership to it once that
/// member has every entry but a few (at most one attempt per 10 s, so a
/// preferred node that keeps failing does not keep the group without a
/// leader). Never to a member that stopped answering, or that just handed
/// leadership away.
async fn prefer_step<S: Store + 'static>(
    raft: &RaftHandle<S>,
    members: &members::MembersState,
    m: &openraft::RaftMetrics<TypeConfig>,
    opts: &WatchOpts,
    last: &mut Option<Instant>,
) {
    let Some(pref) = opts.preferred else {
        return;
    };
    if pref == m.id || m.state != ServerState::Leader {
        return;
    }
    if last.is_some_and(|t| t.elapsed() < Duration::from_secs(10)) {
        return;
    }
    if !acked_within(m, pref, HAND_TO_ACKED_WITHIN)
        || members.handed_off_within(pref, HANDED_OFF_HOLD)
    {
        return;
    }
    let Some(rep) = &m.replication else {
        return;
    };
    let matched = rep.get(&pref).and_then(|l| l.as_ref()).map(|l| l.index);
    let last_index = m.last_log_index.unwrap_or(0);
    if matched.is_some_and(|i| i + 64 >= last_index) {
        *last = Some(Instant::now());
        match raft.trigger().transfer_leader(pref).await {
            Ok(()) => tracing::info!(
                target: "rsm",
                to = pref,
                "raft: handing leadership to this group's preferred node",
            ),
            Err(e) => tracing::warn!(target: "rsm", to = pref, error = %e, "raft: leadership hand-off"),
        }
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
    /// Stops the Raft RPC server (a cluster node).
    server_stop: Option<oneshot::Sender<()>>,
    /// A cluster node's client for its own calls to the leader (prepared
    /// commands, read index), and the cluster token.
    rpc: Option<(network::HttpClient, Option<Arc<str>>)>,
}

/// The queue logs' options: the segment roll size and fsync mode.
pub fn qlog_options(cfg: &OpenConfig) -> crate::rsm::qlog::QLogOptions {
    crate::rsm::qlog::QLogOptions {
        segment_bytes: cfg.seg_opts.segment_bytes,
        fsync: match cfg.seg_opts.fsync {
            segments::FsyncMode::Full => crate::rsm::qlog::Fsync::Full,
            segments::FsyncMode::Data => crate::rsm::qlog::Fsync::Data,
        },
    }
}

/// The network a node runs.
enum Net {
    None(NoNetwork),
    Http(HttpNetwork),
}

impl<S: Store + 'static> RaftReplicator<S> {
    /// Open a single voter. See [`RaftReplicator::open_with`].
    pub fn open(
        store: Arc<S>,
        cfg: OpenConfig,
        waker: Arc<dyn Waker>,
        clock: Arc<dyn apply::Clock>,
    ) -> io::Result<RaftReplicator<S>> {
        RaftReplicator::open_with(store, cfg, None, waker, clock, RaftOpts::from_env(false)?)
    }

    /// Open the node: recover the log from the queue logs, start the apply
    /// thread and openraft (and, for a node of `cluster`, the Raft RPC
    /// server). A single voter returns once it leads with everything in its log
    /// applied; a cluster node once a leader is known (or after 30 s: its
    /// peers may still be starting). A boot call (blocking I/O); never on a
    /// runtime worker that must stay responsive.
    ///
    /// `opts`: see [`RaftOpts`].
    pub fn open_with(
        store: Arc<S>,
        cfg: OpenConfig,
        cluster: Option<ClusterConfig>,
        waker: Arc<dyn Waker>,
        clock: Arc<dyn apply::Clock>,
        opts: RaftOpts,
    ) -> io::Result<RaftReplicator<S>> {
        let mut apply_cfg = cfg.apply_cfg;
        if !apply_cfg.qlog {
            return Err(io::Error::other(
                "the openraft replicator keeps its log in the queue logs: QUEEN_RAFT_QLOG must be on",
            ));
        }
        if let Some(c) = &cluster {
            if c.node_id != cfg.node_id {
                return Err(io::Error::other(format!(
                    "the cluster config is for node {}, the node is {}",
                    c.node_id, cfg.node_id
                )));
            }
        }
        if let Some(id) = opts.force_recover {
            if id != cfg.node_id {
                tracing::error!(
                    target: "rsm",
                    named = id,
                    node = cfg.node_id,
                    "QUEEN_RAFT_FORCE_RECOVER names another node: refusing to start",
                );
                return Err(io::Error::other(format!(
                    "QUEEN_RAFT_FORCE_RECOVER={id}, but this node is {}: it names the ONE \
                     surviving node to recover, and is set on that node only",
                    cfg.node_id
                )));
            }
        }
        let data_dir = cfg
            .seg_root
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let state_dir = data_dir.join("raft");
        let group = repair::group_of_dir(&data_dir);

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

        // The Raft RPC listener first: a taken port fails the open, not a task.
        #[cfg(feature = "server")]
        let listener = match &cluster {
            Some(c) => Some(network::bind(&c.listen)?),
            None => None,
        };
        #[cfg(not(feature = "server"))]
        if cluster.is_some() {
            return Err(io::Error::other(
                "a raft cluster needs the `server` feature (the Raft RPC server)",
            ));
        }

        let qopts = qlog_options(&cfg);
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
            qlog_tail: {
                let store = store.clone();
                Box::new(move |log| {
                    store
                        .read(|r| r.meta_u64(&crate::rsm::store::meta::qlog_tail_key(log)))
                        .map_err(|e| io::Error::other(format!("read qlog tail of q{log}: {e}")))
                })
            },
            poison: poison.clone(),
            cache_cap: opts.cache_cap,
        })?;
        let qlog_tails = opened.tails.clone();
        let log = opened.store;
        let mut writer_join = Some(opened.writer);

        let restart = Arc::new(Restart::default());
        // A member counts as live for the membership checks while it answered
        // within two election timeouts (2 s by default: heartbeats go out
        // every 100 ms).
        let live_within = Duration::from_millis(
            2 * env_u64(
                "QUEEN_RAFT_ELECTION_MS",
                if cluster.is_some() { 1000 } else { 150 },
            ),
        )
        .max(Duration::from_secs(1));
        let shared = Arc::new(Shared {
            node_id: cfg.node_id,
            applied_index: AtomicU64::new(applied),
            applied_term: AtomicU64::new(term),
            durable_index: AtomicU64::new(durable),
            proposals: AtomicU64::new(0),
            poison,
            waiters: Mutex::new(HashMap::new()),
            applied_notify: Arc::new(ApplyWake::new()),
            applied_watch: watch::channel(0).0,
            replicated: AtomicU64::new(u64::MAX),
            terms: Mutex::new(if applied > 0 {
                BTreeMap::from([(applied, term)])
            } else {
                BTreeMap::new()
            }),
            last_term: AtomicU64::new(term),
            restart: restart.clone(),
            leader_http: std::sync::RwLock::new(None),
            leader_raft: std::sync::RwLock::new(None),
            remote: Arc::new(std::sync::OnceLock::new()),
            local: Arc::new(std::sync::OnceLock::new()),
            members: Arc::new(members::MembersState::default()),
            admin: Arc::new(admin::AdminCtx::new(opts.promote_max_lag, live_within)),
            apply_failure: Mutex::new(None),
            state_dir: state_dir.clone(),
        });

        // The apply thread: the queue logs are written by our log writer, never
        // by the applier.
        apply_cfg.qlog_writer_external = true;
        let (apply_tx, apply_rx) = apply::channel(cfg.apply_channel_capacity);
        let notify: Arc<dyn Notify> = Arc::new(RaftNotify {
            shared: shared.clone(),
            waker,
            gate: opened.gate.clone(),
            group,
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
            Some(qlog_tails),
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
            state_dir.clone(),
            apply_tx,
            shared.clone(),
            log.clone(),
        ) {
            Ok(sm) => sm,
            Err(e) => return Err(fail(e, None, &mut writer_join, &mut apply_join, &log)),
        };
        let config = match raft_config(cluster.is_some()) {
            Ok(c) => c,
            Err(e) => return Err(fail(e, None, &mut writer_join, &mut apply_join, &log)),
        };
        // Past the leader lease (`election_timeout_max`) and one more election
        // timeout: 3 s by default.
        let quorum_loss =
            Duration::from_millis(config.election_timeout_max + config.election_timeout_min);
        let rt = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("queen-raft")
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => return Err(fail(e, None, &mut writer_join, &mut apply_join, &log)),
        };

        // The operator's repairs, before openraft reads the log ([`repair`]):
        // an apply-skip rewrite a crash interrupted, the entries
        // `QUEEN_RAFT_APPLY_SKIP` names, then `QUEEN_RAFT_FORCE_RECOVER`.
        {
            let skips: Vec<SkipSpec> = opts
                .apply_skip
                .iter()
                .filter(|s| s.group == group)
                .cloned()
                .collect();
            let skipped_in_store: std::collections::BTreeSet<u64> =
                match store.read(|r| apply::skipped_entries(r)) {
                    Ok(v) => v.into_iter().map(|(i, _)| i).collect(),
                    Err(e) => {
                        // The state machine holds the apply thread's only
                        // sender: it goes first, or the thread never ends.
                        drop(sm);
                        return Err(fail(
                            io::Error::other(format!("read the skipped entries: {e}")),
                            Some((rt, None)),
                            &mut writer_join,
                            &mut apply_join,
                            &log,
                        ));
                    }
                };
            let self_node = cluster
                .as_ref()
                .and_then(|c| c.members.get(&cfg.node_id).cloned());
            let force = opts.force_recover;
            let node_id = cfg.node_id;
            let applied_id = applied_log_id(applied, term);
            let mut log_r = log.clone();
            let state_r = state_dir.clone();
            let handle = rt.handle().clone();
            let repaired: io::Result<()> = std::thread::scope(|s| {
                s.spawn(move || {
                    handle.block_on(async move {
                        repair::finish_journal(&mut log_r, &state_r).await?;
                        repair::apply_skip(
                            &mut log_r,
                            &state_r,
                            &skips,
                            applied,
                            &skipped_in_store,
                        )
                        .await?;
                        if force.is_some() {
                            repair::force_recover(
                                &mut log_r, &state_r, node_id, self_node, applied_id,
                            )
                            .await?;
                        }
                        Ok(())
                    })
                })
                .join()
                .unwrap_or_else(|_| Err(io::Error::other("the boot repair panicked")))
            });
            if let Err(e) = repaired {
                tracing::error!(target: "rsm", error = %e, "raft: the boot repair refused; this node does not start");
                // The state machine holds the apply thread's only sender: it
                // goes first, or the thread never ends and the join hangs.
                drop(sm);
                return Err(fail(
                    e,
                    Some((rt, None)),
                    &mut writer_join,
                    &mut apply_join,
                    &log,
                ));
            }
        }

        let net = match &cluster {
            None => Net::None(NoNetwork),
            Some(c) => Net::Http(HttpNetwork::new(
                c.token.clone(),
                Arc::new(SendCtx::new(
                    data_dir.clone(),
                    store.clone(),
                    opened.reader.clone(),
                )),
                shared.members.clone(),
            )),
        };
        let members: BTreeMap<NodeId, Node> = match &cluster {
            Some(c) => c.members.clone(),
            None => BTreeMap::from([(cfg.node_id, QueenNode::default())]),
        };

        // openraft is built and initialized on a plain thread, never inside the
        // caller's runtime (a `block_on` there would panic).
        let node_id = cfg.node_id;
        let fresh = opened.fresh;
        let handle = rt.handle().clone();
        let log_for_raft = log.clone();
        let join_flag = opts.join;
        let probe = cluster.clone().filter(|c| c.members.len() > 1);
        // Whether this fresh node joins a cluster that exists already (it then
        // does not wait for a leader below: it has none until it is added).
        let mut joining = false;
        let joining_out = &mut joining;
        // The Raft RPC server starts as soon as openraft exists, BEFORE a fresh
        // node initializes: a fresh peer asks it whether it holds cluster
        // state (`/raft/v1/state`) and must get an answer, not a timeout.
        #[allow(unused_mut)]
        let mut server_stop = None;
        #[cfg(feature = "server")]
        let rpc_serve = match (listener, &cluster) {
            (Some(listener), Some(c)) => {
                let (stop_tx, stop_rx) = oneshot::channel::<()>();
                server_stop = Some(stop_tx);
                Some((
                    listener,
                    c.token.clone().map(Arc::<str>::from),
                    stop_rx,
                    Arc::new(RecvCtx {
                        data_dir: data_dir.clone(),
                        restart: restart.clone(),
                    }),
                    shared.remote.clone(),
                    shared.local.clone(),
                    shared.members.clone(),
                    shared.admin.clone(),
                ))
            }
            _ => None,
        };
        let built: io::Result<RaftHandle<S>> = std::thread::scope(|s| {
            s.spawn(move || {
                handle.block_on(async move {
                    let raft = match net {
                        Net::None(n) => {
                            openraft::Raft::new(node_id, config, n, log_for_raft, sm).await
                        }
                        Net::Http(n) => {
                            openraft::Raft::new(node_id, config, n, log_for_raft, sm).await
                        }
                    }
                    .map_err(|e| io::Error::other(format!("openraft start: {e}")))?;
                    #[cfg(feature = "server")]
                    if let Some((listener, token, stop_rx, snap, remote, local, view, admin)) =
                        rpc_serve
                    {
                        let state = network::RpcState {
                            raft: raft.clone(),
                            token,
                            snap,
                            remote,
                            local,
                            members: view,
                            admin,
                        };
                        tokio::spawn(network::serve(listener, state, async move {
                            let _ = stop_rx.await;
                        }));
                    }
                    if fresh {
                        // A node that joins an existing cluster must NOT
                        // initialize one: with two such nodes a fresh majority
                        // of the old voter set would elect a leader of an
                        // empty log and overwrite the members that hold the
                        // data. `QUEEN_RAFT_JOIN` says so; without it, a peer
                        // that already holds cluster state says so.
                        let found = match (&probe, join_flag) {
                            (Some(c), false) => peer_with_state(c).await,
                            _ => None,
                        };
                        if join_flag || found.is_some() {
                            *joining_out = true;
                            tracing::warn!(
                                target: "rsm",
                                node = node_id,
                                explicit = join_flag,
                                peer_with_state = ?found,
                                "raft: this fresh node joins an existing cluster: it does not \
                                 initialize one, and serves 503 until the leader adds it \
                                 (POST /api/v1/system/raft/membership/learners)",
                            );
                        } else {
                            // Every node of a new cluster initializes with the
                            // same members; one that already heard from a peer
                            // is not allowed to, which is fine.
                            match raft.initialize(members).await {
                                Ok(()) => {}
                                Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {}
                                Err(e) => {
                                    return Err(io::Error::other(format!(
                                        "openraft initialize: {e}"
                                    )))
                                }
                            }
                        }
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

        // A cluster node's own calls to the leader (prepared commands, read
        // index, membership changes): one pooled client. Every node with a Raft
        // network has one, whatever `QUEEN_RAFT_PEERS` listed at its first
        // start: the membership lives in the log and grows past that list (a
        // node started alone, or a force-recovered survivor, gains members).
        let rpc = cluster
            .as_ref()
            .map(|c| (network::http_client(), c.token.clone().map(Arc::from)));

        // The role watch, the followers' progress and the purge driver.
        let (role_tx, role_rx) = watch::channel(Role::Candidate);
        rt.spawn(watch(
            raft.clone(),
            shared.clone(),
            log.clone(),
            role_tx,
            WatchOpts {
                preferred: cluster.as_ref().and_then(|c| c.preferred_leader),
                quorum_loss,
                ..WatchOpts::of(&opts)
            },
        ));

        // The submitter: proposals reach openraft in the order the batcher
        // first-polled them. Each one's payload compression (started on the
        // codec pool at propose) is awaited first, in that order, so the
        // leader's writer and every follower get the same stored bytes.
        let (submit_tx, mut submit_rx) = mpsc::unbounded_channel::<Submit>();
        let r2 = raft.clone();
        rt.spawn(async move {
            while let Some(s) = submit_rx.recv().await {
                s.app.settle_codec().await;
                if r2.client_write_ff(s.app, Some(s.responder)).await.is_err() {
                    // openraft stopped: every later proposal sees its responder
                    // dropped, and the role watch reports `Stopped`.
                    break;
                }
            }
        });

        // A single voter: wait until it leads with everything applied (I13). A
        // cluster node: until a leader is known, for a while. A node joining a
        // cluster has no leader until the leader adds it: it does not wait.
        let end = Instant::now()
            + if joining {
                Duration::ZERO
            } else if cluster.is_some() {
                cfg.replay_deadline.min(Duration::from_secs(30))
            } else {
                cfg.replay_deadline
            };
        loop {
            let role = *role_rx.borrow();
            let ready = match role {
                Role::Leader { .. } => true,
                Role::Follower { leader: Some(_) } => cluster.is_some(),
                // A learner (a member being added) is ready once it hears a
                // leader, like a follower.
                Role::Learner => {
                    cluster.is_some() && raft.metrics().borrow_watched().current_leader.is_some()
                }
                Role::Stopped => {
                    let why = shared
                        .poisoned()
                        .or_else(|| restart.requested())
                        .unwrap_or_else(|| "openraft stopped during recovery".into());
                    return Err(fail(
                        io::Error::other(why),
                        Some((rt, Some(raft))),
                        &mut writer_join,
                        &mut apply_join,
                        &log,
                    ));
                }
                _ => false,
            };
            if ready {
                break;
            }
            if Instant::now() >= end {
                if cluster.is_some() {
                    tracing::warn!(target: "rsm", node = node_id, role = ?role, "raft: no leader yet; serving 503 until one is elected");
                    break;
                }
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
            cluster = cluster.as_ref().map(|c| c.members.len()).unwrap_or(1),
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
            server_stop,
            rpc,
        })
    }

    /// Install the handler that plans a follower's prepared command on this
    /// node (the facade, once it exists). Set once.
    pub fn set_remote_handler(&self, h: RemoteHandler) {
        let _ = self.shared.remote.set(h);
    }

    /// Install the handler that answers a peer's `/raft/v1/local` gather of
    /// this node's own dashboard data. Set once.
    pub fn set_local_handler(&self, h: RemoteHandler) {
        let _ = self.shared.local.set(h);
    }

    /// The membership as this node sees it, with the leader's view of each
    /// follower's replication when this node leads (the dashboard's Raft view).
    pub fn cluster_view(&self) -> ClusterView {
        let Some(m) = self.metrics_now() else {
            return ClusterView {
                node_id: self.shared.node_id,
                ..ClusterView::default()
            };
        };
        let mc = m.membership_config.membership();
        let voters: std::collections::BTreeSet<NodeId> = mc.voter_ids().collect();
        let mut ids: Vec<NodeId> = voters.iter().copied().collect();
        ids.extend(mc.learner_ids());
        ids.sort_unstable();
        ids.dedup();
        let members = ids
            .into_iter()
            .map(|id| {
                let node = mc.get_node(&id);
                let matched = m
                    .replication
                    .as_ref()
                    .and_then(|r| r.get(&id))
                    .map(|l| l.as_ref().map(|l| rsm_index(l.index)).unwrap_or(0));
                let heartbeat_age_ms = m
                    .heartbeat
                    .as_ref()
                    .and_then(|h| h.get(&id))
                    .and_then(|t| t.as_ref())
                    .map(|t| openraft::Instant::elapsed(&**t).as_millis() as u64);
                MemberView {
                    node_id: id,
                    raft_addr: node.map(|n| n.raft.clone()).unwrap_or_default(),
                    http_addr: node.map(|n| n.http.clone()).unwrap_or_default(),
                    voter: voters.contains(&id),
                    matched,
                    heartbeat_age_ms,
                }
            })
            .collect();
        ClusterView {
            node_id: self.shared.node_id,
            leader: m.current_leader,
            term: m.current_term,
            members,
        }
    }

    /// POST `body` to a peer's Raft RPC `path` and return the answer's bytes
    /// (a dashboard gather; never on the message path).
    pub async fn call_peer(
        &self,
        raft_addr: &str,
        path: &str,
        body: bytes::Bytes,
        ttl: Duration,
    ) -> Result<bytes::Bytes, String> {
        let Some((client, token)) = self.rpc.as_ref() else {
            return Err("not a cluster node".to_string());
        };
        let url = format!("http://{raft_addr}{path}");
        network::post(
            client,
            &url,
            token.as_deref(),
            "application/json",
            axum::body::Body::from(body),
            ttl,
        )
        .await
        .map_err(|e| match e {
            network::Fail::Unreachable(m) | network::Fail::Network(m) => m,
        })
    }

    /// Whether this node is part of a multi-node cluster (it has peers to
    /// send prepared commands to).
    pub fn is_cluster(&self) -> bool {
        self.rpc.is_some()
    }

    /// The Raft RPC address of the leader when another node leads.
    pub fn leader_raft(&self) -> Option<String> {
        self.shared
            .leader_raft
            .read()
            .expect("leader_raft")
            .as_deref()
            .map(str::to_string)
    }

    /// POST `body` to the leader's `path` (`/raft/v1/...`) and return the
    /// answer's bytes. At most `QUEEN_RAFT_FWD_INFLIGHT` calls are in flight.
    async fn call_leader(
        &self,
        path: &str,
        content_type: &str,
        body: bytes::Bytes,
        ttl: Duration,
    ) -> Result<bytes::Bytes, RemoteError> {
        let Some((client, token)) = self.rpc.as_ref() else {
            return Err(RemoteError::NoLeader);
        };
        let Some(addr) = self.leader_raft() else {
            return Err(RemoteError::NoLeader);
        };
        let url = format!("http://{addr}{path}");
        // Every call holds one HTTP/1 connection to the leader while in flight,
        // so the calls in flight are the connections open. A push is forwarded
        // as one call per partition, all at once: 32 pushes of 1,000 new
        // partitions each put ~26,500 connections on one follower, the
        // ephemeral ports ran out, and connect() spun on every core of it.
        // Past the gate a call waits here for a free slot, and the connection
        // that slot frees is reused.
        let _slot = match tokio::time::timeout(ttl, forward_gate().acquire()).await {
            Ok(Ok(slot)) => slot,
            _ => return Err(RemoteError::Transport("no forward slot within the deadline".into())),
        };
        network::post(
            client,
            &url,
            token.as_deref(),
            content_type,
            axum::body::Body::from(body),
            ttl,
        )
        .await
        .map_err(|e| match e {
            network::Fail::Unreachable(m) | network::Fail::Network(m) => RemoteError::Transport(m),
        })
    }

    /// Send a prepared command to the leader; the answer is the encoded reply.
    pub async fn forward_command(
        &self,
        body: bytes::Bytes,
        ttl: Duration,
    ) -> Result<bytes::Bytes, RemoteError> {
        self.call_leader("/raft/v1/submit", "application/octet-stream", body, ttl)
            .await
    }

    /// The leader's read index (RSM numbering) for a linearizable read here.
    pub async fn leader_read_index(&self, ttl: Duration) -> Result<u64, RemoteError> {
        let b = self
            .call_leader(
                "/raft/v1/read_index",
                "application/json",
                bytes::Bytes::new(),
                ttl,
            )
            .await?;
        #[derive(serde::Deserialize)]
        struct Read {
            index: u64,
        }
        serde_json::from_slice::<Read>(&b)
            .map(|r| r.index)
            .map_err(|e| RemoteError::Transport(format!("read index answer: {e}")))
    }

    /// Wait until this node has applied `index`; `false` at the deadline.
    pub async fn wait_applied(&self, index: u64, deadline: Instant) -> bool {
        let mut rx = self.shared.applied_watch.subscribe();
        loop {
            if self.shared.applied_index.load(Ordering::Acquire) >= index {
                return true;
            }
            match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), rx.changed())
                .await
            {
                Ok(Ok(())) => {}
                // The node stopped, or the deadline passed.
                _ => return self.shared.applied_index.load(Ordering::Acquire) >= index,
            }
        }
    }

    /// The HTTP address of the leader when another node leads (a follower
    /// forwards client requests there). `None` when this node leads, no
    /// leader is known, or the leader's address is not in the membership.
    pub fn leader_http(&self) -> Option<String> {
        self.shared
            .leader_http
            .read()
            .expect("leader_http")
            .as_deref()
            .map(str::to_string)
    }

    /// Why this node stopped to restart, if it did (a received snapshot).
    pub fn restart_requested(&self) -> Option<String> {
        self.shared.restart.requested()
    }

    /// Entries in memory and their bytes.
    pub fn log_cache(&self) -> (usize, usize) {
        self.log.cached()
    }

    /// The RSM index of the last entry openraft purged (0 for none).
    pub fn purged_index(&self) -> u64 {
        self.metrics_now()
            .and_then(|m| m.purged)
            .map(|p| rsm_index(p.index))
            .unwrap_or(0)
    }

    /// The node id.
    pub fn node_id(&self) -> NodeId {
        self.shared.node_id
    }

    /// Who is in the cluster and when the leader last heard from each member,
    /// as this node knows it ([`members`]).
    pub fn members(&self) -> super::ClusterMembers {
        let m = self.metrics_now();
        self.shared.members.members(m.as_ref(), self.shared.node_id)
    }

    /// The membership for an operator (`GET /api/v1/system/raft/membership`): the
    /// leader's own view — taken here when this node leads, fetched from the
    /// leader over the Raft RPC port otherwise — or, with no leader to ask,
    /// this node's membership with the last members view the leader sent it
    /// (`source: "follower"`).
    pub async fn membership_status(&self, deadline: Instant) -> MembershipStatus {
        let Some(m) = self.metrics_now() else {
            return MembershipStatus {
                node_id: self.shared.node_id,
                source: "stopped".into(),
                ..MembershipStatus::default()
            };
        };
        if m.state == ServerState::Leader || self.rpc.is_none() {
            return admin::status(&m, &self.shared.members, &self.shared.admin);
        }
        let ttl = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(5));
        let call = admin::AdminCall {
            change: None,
            timeout_ms: ttl.as_millis() as u64,
        };
        if let Ok(body) = serde_json::to_vec(&call) {
            if let Ok(b) = self
                .call_leader(
                    "/raft/v1/membership",
                    "application/json",
                    Bytes::from(body),
                    ttl,
                )
                .await
            {
                if let Ok(Ok(st)) =
                    serde_json::from_slice::<admin::AdminAnswer>(&b).map(|a| a.into_result())
                {
                    return st;
                }
            }
        }
        admin::status(&m, &self.shared.members, &self.shared.admin)
    }

    /// Change the membership (the operator endpoints, [`admin`]): checked and
    /// made on the leader, which answers once the change committed, with the
    /// membership it left. A follower forwards the change to the leader over
    /// the Raft RPC port; with no leader known it answers `NotLeader`.
    pub async fn admin_change(
        &self,
        change: MembershipChange,
        deadline: Instant,
    ) -> Result<MembershipStatus, ReplError> {
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| repl_err("openraft stopped"))?;
        if self.rpc.is_none() {
            return Err(ReplError::Unsupported(
                "this node runs as a single voter without a Raft network (QUEEN_RAFT_PEERS \
                 unset): restart it with QUEEN_RAFT_PEERS listing at least itself before \
                 changing its membership"
                    .into(),
            ));
        }
        let Some(m) = self.metrics_now() else {
            return Err(repl_err("openraft stopped"));
        };
        if m.state == ServerState::Leader {
            return admin::change_on_leader(
                raft,
                &self.shared.members,
                &self.shared.admin,
                change,
                deadline,
            )
            .await;
        }
        let ttl = deadline.saturating_duration_since(Instant::now());
        let call = admin::AdminCall {
            change: Some(change),
            timeout_ms: ttl.as_millis() as u64,
        };
        let body = serde_json::to_vec(&call).map_err(repl_err)?;
        match self
            .call_leader(
                "/raft/v1/membership",
                "application/json",
                Bytes::from(body),
                ttl + Duration::from_secs(1),
            )
            .await
        {
            Ok(b) => serde_json::from_slice::<admin::AdminAnswer>(&b)
                .map_err(|e| ReplError::Fatal(format!("the leader's answer: {e}")))?
                .into_result(),
            Err(RemoteError::NoLeader) => Err(ReplError::NotLeader {
                hint: m.current_leader.filter(|l| *l != m.id),
            }),
            Err(RemoteError::Transport(msg)) => Err(ReplError::refused(
                "leader_unreachable",
                format!(
                    "the leader did not answer ({msg}): the change may or may not have been \
                     made; read the membership before repeating it"
                ),
            )),
        }
    }

    /// What `/health` shows about apply: the entries this node applied as skip
    /// markers (an operator's `QUEEN_RAFT_APPLY_SKIP`, with their digests where
    /// this node rewrote them itself), and the entry that stopped apply here,
    /// if one did.
    pub fn apply_status(&self) -> serde_json::Value {
        let in_store = self
            .store
            .read(|r| apply::skipped_entries(r))
            .unwrap_or_default();
        let rewritten = repair::skipped_here(&self.shared.state_dir).unwrap_or_default();
        let skipped: Vec<serde_json::Value> = in_store
            .into_iter()
            .map(|(index, term)| {
                let digest = rewritten
                    .iter()
                    .find(|s| s.index == index)
                    .map(|s| s.digest.clone());
                serde_json::json!({"index": index, "term": term, "digest": digest})
            })
            .collect();
        let failure = self
            .shared
            .apply_failure
            .lock()
            .expect("apply failure")
            .clone();
        serde_json::json!({ "skipped": skipped, "failure": failure })
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

    /// On the way out (the binary's SIGTERM): a leader hands its leadership to
    /// the most caught-up live voter and waits, up to `wait`, until another
    /// node leads — so stopping the leader (a rolling restart) costs the
    /// cluster one transfer instead of an election timeout without a leader
    /// (1-2 s). This node also stops campaigning, for good: leading again would
    /// end in an election when it exits. Returns the new leader; `None` when
    /// this node did not lead, had no live voter to hand to, or no other node
    /// took over in time (the cluster then elects as it would have anyway).
    pub async fn hand_off_leadership(&self, wait: Duration) -> Option<NodeId> {
        let raft = self.raft.as_ref()?;
        raft.runtime_config().elect(false);
        let me = self.shared.node_id;
        let mut metrics = raft.metrics();
        {
            let m = metrics.borrow_watched();
            if m.state != ServerState::Leader
                || m.membership_config.membership().voter_ids().count() < 2
            {
                return None;
            }
        }
        let t0 = Instant::now();
        let mut asked: Option<(NodeId, Instant)> = None;
        loop {
            let m = metrics.borrow_watched().clone();
            if let Some(l) = m.current_leader.filter(|l| *l != me) {
                tracing::info!(
                    target: "rsm",
                    to = l,
                    ms = t0.elapsed().as_millis() as u64,
                    "raft: leadership handed off"
                );
                return Some(l);
            }
            if m.state == ServerState::Leader
                && asked.is_none_or(|(_, at)| at.elapsed() >= HAND_OFF_RETRY)
            {
                let Some(to) = most_caught_up(&m, &self.shared.members) else {
                    tracing::warn!(target: "rsm", "raft: no live voter to hand leadership to");
                    return None;
                };
                if let Err(e) = raft.trigger().transfer_leader(to).await {
                    tracing::warn!(target: "rsm", to, error = %e, "raft: leadership hand-off");
                    return None;
                }
                asked = Some((to, Instant::now()));
            }
            let left = wait.saturating_sub(t0.elapsed());
            if left.is_zero() {
                tracing::warn!(
                    target: "rsm",
                    to = ?asked.map(|(to, _)| to),
                    ms = wait.as_millis() as u64,
                    "raft: no other node took leadership over in time"
                );
                return None;
            }
            if let Ok(Err(_)) =
                tokio::time::timeout(left.min(HAND_OFF_RETRY), metrics.changed()).await
            {
                return None;
            }
        }
    }

    /// Stop the node and return the apply thread's stats and the store handle.
    pub fn shutdown(mut self) -> io::Result<(ApplyStats, Arc<S>)> {
        let stats = self.stop()?;
        Ok((stats, self.store.clone()))
    }

    fn stop(&mut self) -> io::Result<ApplyStats> {
        self.submit_tx.take();
        if let Some(stop) = self.server_stop.take() {
            let _ = stop.send(());
        }
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

/// What `POST /raft/v1/state` answers: whether a node holds cluster state (a
/// fresh node asks its peers before it initializes a cluster).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NodeState {
    pub node_id: NodeId,
    pub term: u64,
    /// RSM numbering; `None` for an empty log.
    pub last_log_index: Option<u64>,
    pub voters: Vec<NodeId>,
    /// It has a log, a term or a membership: a cluster exists around it.
    pub initialized: bool,
}

impl NodeState {
    pub(crate) fn of(m: &openraft::RaftMetrics<TypeConfig>) -> NodeState {
        let mem = m.membership_config.membership();
        NodeState {
            node_id: m.id,
            term: m.current_term,
            last_log_index: m.last_log_index.map(rsm_index),
            voters: mem.voter_ids().collect(),
            initialized: m.last_log_index.is_some()
                || m.current_term > 0
                || mem.nodes().next().is_some(),
        }
    }
}

/// A fresh node's question to its peers before it initializes a cluster from
/// `QUEEN_RAFT_PEERS`: does one of them hold cluster state already? Then this
/// node is a replacement joining that cluster, not a founder. Asked for up to
/// 1.5 s while some peer does not answer (at a new cluster's first start the
/// peers are starting too); the first peer that holds state answers it.
async fn peer_with_state(c: &ClusterConfig) -> Option<NodeId> {
    let client = network::http_client();
    let end = Instant::now() + Duration::from_millis(1500);
    loop {
        let mut silent = false;
        for (id, node) in &c.members {
            if *id == c.node_id || node.raft.is_empty() {
                continue;
            }
            let url = format!("http://{}/raft/v1/state", node.raft);
            match network::post(
                &client,
                &url,
                c.token.as_deref(),
                "application/json",
                axum::body::Body::empty(),
                Duration::from_millis(400),
            )
            .await
            {
                Ok(b) => match serde_json::from_slice::<NodeState>(&b) {
                    Ok(st) if st.initialized => return Some(*id),
                    Ok(_) => {}
                    Err(_) => silent = true,
                },
                Err(_) => silent = true,
            }
        }
        if !silent || Instant::now() >= end {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
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
        // A follower serving its own clients asks the leader for the read
        // index and waits until it has applied that far itself.
        if self.is_cluster()
            && client_offload_from_env()
            && !matches!(*self.role_rx.borrow(), Role::Leader { .. })
        {
            let ttl = deadline.saturating_duration_since(Instant::now());
            let index = match self.leader_read_index(ttl).await {
                Ok(i) => i,
                Err(RemoteError::NoLeader) => {
                    return Err(ProposeError::NotLeader { hint: None });
                }
                Err(RemoteError::Transport(m)) => return Err(ProposeError::Refused(m)),
            };
            if !self.wait_applied(index, deadline).await {
                return Err(ProposeError::Timeout);
            }
            return Ok(index);
        }
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

    /// Checked and made on the leader ([`admin`]); a follower forwards it
    /// there. See [`RaftReplicator::admin_change`].
    async fn change_membership(
        &self,
        change: MembershipChange,
        deadline: Instant,
    ) -> Result<(), ReplError> {
        self.admin_change(change, deadline).await.map(|_| ())
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
