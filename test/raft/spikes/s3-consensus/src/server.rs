//! One node = one process listening on real TCP.
//!
//! The process serves both the Raft protocol and the client/admin calls the
//! scenarios need, on one framed port (D12 puts forwarding on the same port in
//! the real design too).

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use openraft::errors::ClientWriteError;
use openraft::errors::RaftError;
use openraft::raft::ReadPolicy;
use openraft::Config;
use openraft::SnapshotPolicy;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::logstore::CommittedDurability;
use crate::logstore::WalLogStore;
use crate::net::NetworkFactory;
use crate::sm::FsyncMode;
use crate::sm::Manifest;
use crate::sm::SmStore;
use crate::sm::SnapshotHandle;
use crate::types::AppRequest;
use crate::types::Node;
use crate::types::NodeId;
use crate::types::Raft;
use crate::types::SnapshotMeta;
use crate::types::TypeConfig;
use crate::types::Vote;
use crate::util::term_of;
use crate::wire;
use crate::wire::Req;
use crate::wire::Resp;
use crate::wire::StatusResp;
use crate::wire::WriteErr;

#[derive(Clone, Debug)]
pub struct NodeOpts {
    pub id: NodeId,
    pub dir: PathBuf,
    pub listen: String,
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
    pub fsync: FsyncMode,
    pub file_bytes: u64,
    /// D14 wants pre-vote on; the flag exists to show the difference.
    pub pre_vote: bool,
    /// §12.3: openraft's docs call `true` unsafe with a volatile state machine.
    pub leader_restore: bool,
    pub max_in_snapshot_log_to_keep: u64,
    /// 2 ms batching window for linearizable reads (§9.4).
    pub lin_batch_ms: u64,
    /// Interval of the periodic durable point (§11.4, default 1000 ms).
    pub durable_ms: u64,
    /// How durable `save_committed` is (§12.3 says "persisted"; openraft's
    /// example does not flush at all).
    pub committed: CommittedDurability,
    /// Call `Raft::wait_for_recovery` after start and record how long it takes.
    /// openraft offers it as the alternative to a durable `save_committed`
    /// (storage/v2/raft_log_storage.rs:63-97).
    pub wait_recovery: bool,
}

type LinWaiter = oneshot::Sender<Result<u64, String>>;

/// What this process found on disk when it opened its log and state machine,
/// captured BEFORE openraft could replicate anything into it. This is the
/// evidence a crash-restart scenario needs: everything the running node
/// reports later is already mixed with what the leader re-sent.
#[derive(Clone, Debug, Default)]
pub struct ReopenInfo {
    pub last_log: Option<u64>,
    pub purged: Option<u64>,
    pub committed: Option<u64>,
    pub vote_term: Option<u64>,
    pub sm_applied: Option<u64>,
    pub sm_applied_count: u64,
    pub sm_digest: u64,
}

struct App {
    id: NodeId,
    raft: Raft,
    sm: SmStore,
    lin_window_tx: mpsc::Sender<LinWaiter>,
    lin_coalesce_tx: mpsc::Sender<LinWaiter>,
    shutdown: Arc<tokio::sync::Notify>,
    reopen: ReopenInfo,
    /// A second handle on the same log, used only by `VerifyLog`.
    log_store: WalLogStore<TypeConfig>,
    /// Peers whose Raft RPCs are processed but never answered (one-way fault).
    drop_resp: std::sync::Mutex<std::collections::BTreeSet<NodeId>>,
    /// ms that `Raft::wait_for_recovery` took, once it returned.
    recovery_ms: Arc<std::sync::atomic::AtomicI64>,
}

pub async fn run(opts: NodeOpts) -> anyhow::Result<()> {
    fs::create_dir_all(&opts.dir)?;

    let config = Config {
        cluster_name: "s3-spike".to_string(),
        heartbeat_interval: opts.heartbeat_ms,
        election_timeout_min: opts.election_min_ms,
        election_timeout_max: opts.election_max_ms,
        enable_pre_vote: Some(opts.pre_vote),
        enable_leader_restore: Some(opts.leader_restore),
        // Snapshots are triggered by hand in this spike (§11.6: by log bytes
        // and time in the real system).
        snapshot_policy: SnapshotPolicy::Never,
        max_in_snapshot_log_to_keep: opts.max_in_snapshot_log_to_keep,
        purge_batch_size: 64,
        ..Default::default()
    };
    let config = Arc::new(config.validate().context("openraft config")?);

    let mut log_store = WalLogStore::<TypeConfig>::open(
        opts.dir.join("log").display().to_string(),
        opts.committed,
    )?;
    let sm = SmStore::open(&opts.dir, opts.fsync, opts.file_bytes)?;

    // Read what is on disk before openraft can touch it (crash-restart
    // evidence, see `ReopenInfo`).
    let reopen = {
        use openraft::storage::RaftLogStorage;
        use openraft::RaftLogReader;
        let state = log_store.get_log_state().await?;
        let vote = log_store.read_vote().await?;
        let committed = log_store.read_committed().await?;
        let (meta, _) = sm.status().await;
        ReopenInfo {
            last_log: state.last_log_id.as_ref().map(|l| l.index),
            purged: state.last_purged_log_id.as_ref().map(|l| l.index),
            committed: committed.as_ref().map(|l| l.index),
            vote_term: vote.as_ref().map(|v| term_of_vote(v)),
            sm_applied: meta.last_applied.as_ref().map(|l| l.index),
            sm_applied_count: meta.applied_count,
            sm_digest: meta.digest,
        }
    };
    tracing::info!(
        "node {} reopened: log last={:?} purged={:?} committed={:?} vote_term={:?}; sm applied={:?} count={} digest={:x}",
        opts.id,
        reopen.last_log,
        reopen.purged,
        reopen.committed,
        reopen.vote_term,
        reopen.sm_applied,
        reopen.sm_applied_count,
        reopen.sm_digest
    );

    let log_handle = log_store.clone();
    let raft = Raft::new(opts.id, config, NetworkFactory, log_store, sm.clone())
        .await
        .context("Raft::new")?;

    let recovery_ms = Arc::new(std::sync::atomic::AtomicI64::new(-1));
    if opts.wait_recovery {
        let raft2 = raft.clone();
        let slot = recovery_ms.clone();
        tokio::spawn(async move {
            let t0 = Instant::now();
            match raft2.wait_for_recovery(Some(Duration::from_secs(30))).await {
                Ok(_) => {
                    let ms = t0.elapsed().as_secs_f64() * 1000.0;
                    slot.store(ms as i64, std::sync::atomic::Ordering::Relaxed);
                    tracing::info!("wait_for_recovery returned after {ms:.0} ms");
                }
                Err(e) => {
                    slot.store(-2, std::sync::atomic::Ordering::Relaxed);
                    tracing::error!("wait_for_recovery failed: {e}");
                }
            }
        });
    }

    // §11.4: the durable point is a loop, not part of the write path.
    if opts.durable_ms > 0 {
        let sm2 = sm.clone();
        let every = Duration::from_millis(opts.durable_ms);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                if let Err(e) = sm2.durable().await {
                    tracing::error!("durable point failed: {e}");
                }
            }
        });
    }

    let (lin_window_tx, lin_window_rx) = mpsc::channel::<LinWaiter>(8192);
    tokio::spawn(lin_batcher(
        raft.clone(),
        lin_window_rx,
        Some(Duration::from_millis(opts.lin_batch_ms)),
    ));
    let (lin_coalesce_tx, lin_coalesce_rx) = mpsc::channel::<LinWaiter>(8192);
    tokio::spawn(lin_batcher(raft.clone(), lin_coalesce_rx, None));

    let app = Arc::new(App {
        id: opts.id,
        raft: raft.clone(),
        sm,
        lin_window_tx,
        lin_coalesce_tx,
        shutdown: Arc::new(tokio::sync::Notify::new()),
        reopen,
        log_store: log_handle,
        drop_resp: std::sync::Mutex::new(std::collections::BTreeSet::new()),
        recovery_ms,
    });

    let listener = TcpListener::bind(&opts.listen).await.context("bind")?;
    tracing::info!("node {} listening on {}", opts.id, opts.listen);
    // The scenario driver waits for this line.
    println!("READY {} {}", opts.id, opts.listen);

    let shutdown = app.shutdown.clone();
    loop {
        tokio::select! {
            _ = shutdown.notified() => break,
            accepted = listener.accept() => {
                let (sock, _peer) = accepted?;
                let app = app.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve_conn(app, sock).await {
                        if e.kind() != io::ErrorKind::UnexpectedEof {
                            tracing::debug!("connection ended: {e}");
                        }
                    }
                });
            }
        }
    }

    tracing::info!("node {} shutting down", opts.id);
    let _ = raft.shutdown().await;
    Ok(())
}

/// The application's batching layer in front of `ensure_linearizable` (§9.4):
/// openraft sends one RPC round per call, so coalescing is our job.
///
/// `window = Some(d)` is the plan's fixed window: wait `d`, then one barrier
/// for everyone who arrived. `window = None` is the cheaper variant: issue the
/// barrier at once and let everything that arrives while it is in flight ride
/// on the next one, so a read never pays for a window that has no company.
async fn lin_batcher(raft: Raft, mut rx: mpsc::Receiver<LinWaiter>, window: Option<Duration>) {
    loop {
        let Some(first) = rx.recv().await else { return };
        let mut waiters = vec![first];
        match window {
            None => {
                while let Ok(w) = rx.try_recv() {
                    waiters.push(w);
                }
            }
            Some(window) => {
                let deadline = Instant::now() + window;
                loop {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        break;
                    }
                    match tokio::time::timeout(left, rx.recv()).await {
                        Ok(Some(w)) => waiters.push(w),
                        Ok(None) => break,
                        Err(_) => break,
                    }
                }
            }
        }
        let res = raft
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .map(|r| r.index())
            .map_err(|e| e.to_string());
        for w in waiters {
            let _ = w.send(res.clone());
        }
    }
}

/// Per-connection snapshot receive session.
#[derive(Default)]
struct RecvSession {
    vote: Option<Vote>,
    meta: Option<SnapshotMeta>,
    manifest: Option<Manifest>,
    dir: Option<PathBuf>,
    file: Option<(String, std::fs::File, xxhash_rust::xxh3::Xxh3)>,
    /// A failure seen while streaming a file; reported at `SnapFileEnd`.
    error: Option<String>,
}

async fn serve_conn(app: Arc<App>, mut sock: TcpStream) -> io::Result<()> {
    sock.set_nodelay(true)?;
    let mut session = RecvSession::default();
    loop {
        let body = wire::read_frame(&mut sock).await?;
        let req: Req = wire::decode(&body)?;
        let stop = matches!(req, Req::Shutdown);
        // One-way fault: the request IS processed (a heartbeat still renews
        // this node's follower lease), the answer never leaves.
        let silent = sender_of(&req)
            .map(|from| app.drop_resp.lock().unwrap().contains(&from))
            .unwrap_or(false);
        // `SnapFileStart` and `SnapChunk` are not acknowledged: the sender
        // streams them back to back and `SnapFileEnd` carries the verdict.
        if let Some(resp) = handle(&app, req, &mut session).await {
            if silent {
                return Ok(());
            }
            wire::send(&mut sock, &resp).await?;
        }
        if stop {
            app.shutdown.notify_waiters();
            return Ok(());
        }
    }
}

/// Which node sent a Raft RPC, from the vote it carries.
fn sender_of(req: &Req) -> Option<NodeId> {
    match req {
        Req::Append(rpc) => Some(rpc.vote.leader_id().node_id),
        Req::Vote(rpc) | Req::PreVote(rpc) => Some(rpc.vote.leader_id().node_id),
        _ => None,
    }
}

async fn handle(app: &Arc<App>, req: Req, session: &mut RecvSession) -> Option<Resp> {
    match req {
        Req::SnapFileStart { name } => {
            match session.dir.clone() {
                None => session.error = Some("SnapFileStart before SnapBegin".into()),
                Some(dir) => match std::fs::File::create(dir.join(format!("{name}.part"))) {
                    Ok(f) => session.file = Some((name, f, xxhash_rust::xxh3::Xxh3::new())),
                    Err(e) => session.error = Some(e.to_string()),
                },
            }
            None
        }
        Req::SnapChunk { data } => {
            if let Some((_, f, h)) = session.file.as_mut() {
                use std::io::Write;
                if let Err(e) = f.write_all(&data) {
                    session.error = Some(e.to_string());
                } else {
                    h.update(&data);
                }
            }
            None
        }
        other => Some(handle_replying(app, other, session).await),
    }
}

async fn handle_replying(app: &Arc<App>, req: Req, session: &mut RecvSession) -> Resp {
    match req {
        Req::Append(rpc) => Resp::Append(
            app.raft
                .append_entries(rpc)
                .await
                .map_err(|e| e.to_string()),
        ),
        Req::Vote(rpc) => Resp::Vote(app.raft.vote(rpc).await.map_err(|e| e.to_string())),
        Req::PreVote(rpc) => Resp::Vote(app.raft.pre_vote(rpc).await.map_err(|e| e.to_string())),
        Req::TransferLeader(rpc) => Resp::Unit(
            app.raft
                .handle_transfer_leader(rpc)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string()),
        ),

        Req::SnapBegin {
            vote,
            meta,
            manifest,
        } => {
            let dir = app.sm.recv_dir(&meta).await;
            if let Err(e) = fs::create_dir_all(&dir) {
                return Resp::Unit(Err(e.to_string()));
            }
            // Drop the staging directory of any older snapshot: only the one
            // being received now is worth keeping (§11.6 retention).
            if let Some(parent) = dir.parent() {
                if let Ok(entries) = fs::read_dir(parent) {
                    for e in entries.flatten() {
                        if e.path() != dir {
                            let _ = fs::remove_dir_all(e.path());
                        }
                    }
                }
            }
            // Resume: a file already here with the manifest's size and hash
            // does not have to be sent again (§11.6).
            let mut missing = Vec::new();
            for f in &manifest.files {
                let path = dir.join(&f.name);
                let ok = match fs::metadata(&path) {
                    Ok(m) if m.len() == f.size => crate::sm::hash_file_prefix(&path, f.size)
                        .map(|h| h == f.xxh3)
                        .unwrap_or(false),
                    _ => false,
                };
                if !ok {
                    missing.push(f.name.clone());
                }
            }
            session.vote = Some(vote);
            session.meta = Some(meta);
            session.manifest = Some(manifest);
            session.dir = Some(dir);
            Resp::SnapNeed { missing }
        }

        // SnapFileStart and SnapChunk never reach here: `handle` takes them.
        Req::SnapFileStart { .. } | Req::SnapChunk { .. } => Resp::Unit(Ok(())),

        Req::SnapFileEnd { xxh3 } => {
            if let Some(e) = session.error.take() {
                session.file = None;
                return Resp::Unit(Err(e));
            }
            let dir = match session.dir.clone() {
                Some(d) => d,
                None => return Resp::Unit(Err("SnapFileEnd before SnapBegin".into())),
            };
            let Some((name, f, h)) = session.file.take() else {
                return Resp::Unit(Err("no file open".into()));
            };
            let got = h.digest();
            if got != xxh3 {
                return Resp::Unit(Err(format!("{name}: xxh3 {got:x} != announced {xxh3:x}")));
            }
            if let Err(e) = f.sync_all() {
                return Resp::Unit(Err(e.to_string()));
            }
            drop(f);
            if let Err(e) = fs::rename(dir.join(format!("{name}.part")), dir.join(&name)) {
                return Resp::Unit(Err(e.to_string()));
            }
            Resp::Unit(Ok(()))
        }

        Req::SnapEnd => {
            let (Some(vote), Some(meta), Some(manifest), Some(dir)) = (
                session.vote.clone(),
                session.meta.clone(),
                session.manifest.clone(),
                session.dir.clone(),
            ) else {
                return Resp::Snapshot(Err("SnapEnd without a session".into()));
            };
            let handle = SnapshotHandle {
                dir: dir.clone(),
                manifest: manifest.clone(),
            };
            match serde_json::to_vec(&manifest)
                .map_err(io::Error::other)
                .and_then(|b| fs::write(dir.join("MANIFEST.json"), b))
            {
                Ok(()) => {}
                Err(e) => return Resp::Snapshot(Err(e.to_string())),
            }
            let snapshot = openraft::alias::SnapshotOf::<TypeConfig, SnapshotHandle> {
                meta,
                snapshot: handle,
            };
            let res = app.raft.install_full_snapshot(vote, snapshot).await;
            if res.is_ok() {
                // The live directory holds hard links to these files now.
                let _ = fs::remove_dir_all(&dir);
            }
            Resp::Snapshot(res.map_err(|e| e.to_string()))
        }

        Req::Init { members } => {
            let members: BTreeMap<NodeId, Node> = members
                .into_iter()
                .map(|(id, addr)| (id, Node::new(addr, "")))
                .collect();
            Resp::Unit(
                app.raft
                    .initialize(members)
                    .await
                    .map_err(|e| e.to_string()),
            )
        }

        Req::Write { id, payload } => {
            let res = app.raft.client_write(AppRequest { id, payload }).await;
            Resp::Write(match res {
                Ok(r) => Ok((r.log_id.index, r.data.applied_count)),
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                    Err(WriteErr::NotLeader {
                        hint: f.leader_id.clone(),
                        reason: format!("{:?}", f.reason),
                    })
                }
                // GH#2095: this is the variant that only exists on the pinned
                // commit; on 0.10.0-alpha.34 it arrives as ForwardToLeader.
                Err(RaftError::APIError(ClientWriteError::LogEntryDiscarded(f))) => {
                    Err(WriteErr::OutcomeUnknown {
                        detail: format!("log entry discarded, leader hint {:?}", f.leader_id),
                    })
                }
                Err(RaftError::APIError(e)) => Err(WriteErr::Fatal {
                    detail: e.to_string(),
                }),
                Err(RaftError::Fatal(e)) => Err(WriteErr::Fatal {
                    detail: e.to_string(),
                }),
            })
        }

        Req::LinRead { mode } => {
            let read_index = match mode {
                0 => app
                    .raft
                    .ensure_linearizable(ReadPolicy::ReadIndex)
                    .await
                    .map(|r| r.index())
                    .map_err(|e| e.to_string()),
                m => {
                    let tx_ch = if m == 1 {
                        &app.lin_window_tx
                    } else {
                        &app.lin_coalesce_tx
                    };
                    let (tx, rx) = oneshot::channel();
                    if tx_ch.send(tx).await.is_err() {
                        return Resp::LinRead(Err("batcher gone".into()));
                    }
                    match rx.await {
                        Ok(r) => r,
                        Err(_) => Err("batcher dropped the request".to_string()),
                    }
                }
            };
            match read_index {
                Ok(index) => {
                    let (meta, _) = app.sm.status().await;
                    let applied = meta.last_applied.as_ref().map(|l| l.index).unwrap_or(0);
                    Resp::LinRead(Ok((index, applied)))
                }
                Err(e) => Resp::LinRead(Err(e)),
            }
        }

        Req::Status => {
            let m = {
                use openraft::async_runtime::watch::WatchReceiver;
                app.raft.metrics().borrow_watched().clone()
            };
            let (meta, live) = app.sm.status().await;
            Resp::Status(StatusResp {
                id: app.id,
                is_leader: app.raft.is_leader(),
                current_leader: m.current_leader.clone(),
                term: m.current_term,
                last_log_index: m.last_log_index,
                last_applied: m.last_applied.as_ref().map(|l| l.index),
                applied_count: meta.applied_count,
                digest: meta.digest,
                voters: app.raft.voter_ids().collect(),
                learners: app.raft.learner_ids().collect(),
                snapshot_index: m.snapshot.as_ref().map(|l| l.index),
                purged: m.purged.as_ref().map(|l| l.index),
                live_dir: live.display().to_string(),
                replication: m.replication.as_ref().map(|r| {
                    r.iter()
                        .map(|(id, matched)| (*id, matched.as_ref().map(|l| l.index)))
                        .collect()
                }),
                vote_term: term_of_vote(&m.vote),
                committed: m.local_committed.as_ref().map(|l| l.index),
                reopen_last_log: app.reopen.last_log,
                reopen_purged: app.reopen.purged,
                reopen_committed: app.reopen.committed,
                reopen_vote_term: app.reopen.vote_term,
                reopen_sm_applied: app.reopen.sm_applied,
                reopen_sm_applied_count: app.reopen.sm_applied_count,
                reopen_sm_digest: app.reopen.sm_digest,
                recovery_ms: match app.recovery_ms.load(std::sync::atomic::Ordering::Relaxed) {
                    -1 => None,
                    v => Some(v),
                },
                quorum_acked_ms_ago: {
                    use openraft::Instant as _;
                    m.last_quorum_acked
                        .as_ref()
                        .map(|t| t.elapsed().as_millis() as i64)
                },
            })
        }

        Req::AddLearner { id, addr } => Resp::Unit(
            app.raft
                .add_learner(id, Node::new(addr, ""), false)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string()),
        ),

        Req::ChangeMembership { voters } => {
            let set: std::collections::BTreeSet<NodeId> = voters.into_iter().collect();
            Resp::Unit(
                app.raft
                    .change_membership(set, false)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
            )
        }

        Req::TriggerTransfer { to } => Resp::Unit(
            app.raft
                .trigger()
                .transfer_leader(to)
                .await
                .map_err(|e| e.to_string()),
        ),

        Req::TriggerSnapshot => Resp::Unit(
            app.raft
                .trigger()
                .snapshot()
                .await
                .map_err(|e| e.to_string()),
        ),

        Req::PurgeLog { upto } => Resp::Unit(
            app.raft
                .trigger()
                .purge_log(upto)
                .await
                .map_err(|e| e.to_string()),
        ),

        Req::AppliedIds => match app.sm.applied_ids().await {
            Ok(ids) => Resp::AppliedIds(ids.into_iter().collect()),
            Err(e) => {
                tracing::error!("applied_ids: {e}");
                Resp::AppliedIds(Vec::new())
            }
        },

        Req::VerifyLog => {
            use openraft::storage::RaftLogStorage;
            use openraft::RaftLogReader;
            let mut store = app.log_store.clone();
            let verdict = async {
                let state = store.get_log_state().await?;
                let purged = state.last_purged_log_id.as_ref().map(|l| l.index);
                let last = state.last_log_id.as_ref().map(|l| l.index);
                let from = purged.map(|p| p + 1).unwrap_or(0);
                let (expected, mut read, mut hole_at) = match last {
                    Some(l) if l >= from => (l - from + 1, 0u64, None),
                    _ => (0, 0, None),
                };
                if expected > 0 {
                    // Read one index at a time: a hole must be located, not
                    // just counted.
                    for i in from..=last.unwrap() {
                        match store.try_get_log_entries(i..i + 1).await {
                            Ok(v) if v.len() == 1 => read += 1,
                            _ => {
                                hole_at = Some(i);
                                break;
                            }
                        }
                    }
                }
                Ok::<_, io::Error>(crate::wire::LogVerdict {
                    purged,
                    last,
                    read,
                    expected,
                    hole_at,
                    error: None,
                })
            }
            .await;
            Resp::VerifyLog(verdict.map_err(|e: io::Error| e.to_string()))
        }

        Req::DropResponsesTo { peers } => {
            let mut set = app.drop_resp.lock().unwrap();
            set.clear();
            set.extend(peers.iter().copied());
            tracing::warn!("one-way fault: answering no Raft RPC from {:?}", set);
            Resp::Unit(Ok(()))
        }

        Req::Shutdown => Resp::Unit(Ok(())),
    }
}

/// Used by the scenarios when they print a node's state.
pub fn describe(s: &StatusResp) -> String {
    format!(
        "node {} leader={:?} term={} last_log={:?} applied={:?} count={} voters={:?} learners={:?} snap={:?} purged={:?}",
        s.id,
        s.current_leader,
        s.term,
        s.last_log_index,
        s.last_applied,
        s.applied_count,
        s.voters,
        s.learners,
        s.snapshot_index,
        s.purged
    )
}

/// The term inside a vote. `Vote`'s accessors move between alphas; the leader
/// id's term is what every scenario compares, and `LeaderId` exposes it.
pub fn term_of_vote(v: &Vote) -> u64 {
    v.leader_id().term
}

/// Only used to keep `term_of` linked when no scenario needs it.
#[allow(dead_code)]
pub fn term_hint(id: &crate::types::LogId) -> u64 {
    term_of(id)
}
