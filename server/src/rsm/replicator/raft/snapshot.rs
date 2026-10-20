//! Snapshots between the nodes of a cluster.
//!
//! openraft sends a snapshot to a follower only when the entries it needs were
//! purged (the purge driver keeps them while every live follower still needs
//! them, so that is a node that was down for long, or a new one). The snapshot
//! is this node's data as crash recovery sees it:
//!
//! - `store/data.mdb`: a consistent copy of the store's committed image
//!   ([`crate::rsm::store::Store::copy_checkpoint`]), which reopens at its
//!   checkpoint `applied` index;
//! - `qlog/…` and `seg/…`: every queue-log and segment file, hard-linked at
//!   send time (retention paused meanwhile), so every payload the store names
//!   and every entry above the checkpoint is there.
//!
//! # A delta
//!
//! A member that has most of the log already — it was away, and the leader
//! purged past it — is sent only what it lacks. Before it builds anything the
//! sender asks the member where its log stands (`/raft/v1/snapshot/basis`:
//! the index it applied, and its queue-log layout); with the same layout, the
//! snapshot carries the store and, instead of every queue-log file, the RUNS
//! of records above that index ([`crate::rsm::qlog::SeqRun`], `qdelta/…`).
//! The member keeps its own files, cuts them above the basis, and appends the
//! runs byte for byte ([`crate::rsm::qlog::QLog::append_raw`]): a member back
//! from an hour away receives an hour of records, not the leader's whole log.
//! A member that does not answer, or answers with another layout, or an empty
//! one, gets the whole state as before.
//!
//! # The stream
//!
//! ```text
//! "QSNP" | version:u8 | header_len:u32 | header JSON {vote, meta, applied, applied_term, files}
//! file bytes, in header order, each exactly its `len`
//! xxh3_64 of every file byte:u64
//! ```
//!
//! # Installing
//!
//! A snapshot replaces the store and the queue logs under the apply thread,
//! the planner and every reader. That is done at BOOT, never live:
//!
//! 1. the follower stages the files (`snapshots/recv-*`, fsynced) and writes
//!    `snapshot.pending` BEFORE it hands the snapshot to openraft — openraft
//!    purges its log right after an install, so a crash anywhere later must
//!    still find the snapshot;
//! 2. openraft calls the state machine's `install_snapshot`, which asks for a
//!    restart and stops openraft (nothing more may be applied to the store the
//!    snapshot replaces); the node exits;
//! 3. at the next boot, [`apply_pending`] swaps the staged directories in,
//!    cuts the queue logs above the store's checkpoint, and records the
//!    snapshot as the purge point. A marker that openraft never acted on (the
//!    node already had as much) is discarded instead. The swap is resumable:
//!    `snapshot.swapping` marks it started.

use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use futures_util::StreamExt;
use openraft::errors::RaftError;
use openraft::raft::SnapshotResponse;
use serde::{Deserialize, Serialize};

use super::log_store::{state_after_snapshot, write_atomic};
use super::network::{post, Fail, HttpClient};
use super::state_machine::{Checkpoint, MembershipAt, Snapshot};
use super::types::{
    applied_log_id, LogId, NodeId, SnapshotMeta, StoredMembership, TypeConfig, Vote,
};
use super::RaftHandle;
use crate::rsm::qlog::set::{QLogReader, QLogSet};
use crate::rsm::qlog::QLogOptions;
use crate::rsm::store::Store;

const MAGIC: &[u8; 4] = b"QSNP";
const VERSION: u8 = 1;
/// The data directory's snapshot area: `send-*` (links being sent),
/// `recv-*` (a received snapshot, staged), `replaced` (what a swap moved out).
pub const SNAP_DIR: &str = "snapshots";
pub const PENDING: &str = "snapshot.pending";
pub const SWAPPING: &str = "snapshot.swapping";
const CHUNK: usize = 1 << 20;

/// A restart this node asked for: a received snapshot waits to be swapped in.
#[derive(Default)]
pub(crate) struct Restart {
    requested: AtomicBool,
    why: Mutex<Option<String>>,
}

impl Restart {
    pub(crate) fn request(&self, why: String) {
        *self.why.lock().expect("restart") = Some(why);
        self.requested.store(true, Ordering::Release);
    }

    /// Why, once asked.
    pub(crate) fn requested(&self) -> Option<String> {
        if self.requested.load(Ordering::Acquire) {
            self.why.lock().expect("restart").clone()
        } else {
            None
        }
    }
}

/// Copies the store's checkpoint into a directory; answers the applied index
/// and term the copy holds.
pub(crate) type CopyStore = Box<dyn Fn(&Path) -> io::Result<(u64, u64)> + Send + Sync>;

/// Copies the trace environment (`rsm/traces.rs`) into a directory when the
/// snapshot carries it; answers whether it does.
pub(crate) type CopyTraces = Box<dyn Fn(&Path) -> io::Result<bool> + Send + Sync>;

/// Told the RSM index of the checkpoint a streamed snapshot holds
/// ([`stream_checkpoint`]).
pub(crate) type Held = Arc<dyn Fn(u64) + Send + Sync>;

/// Told, while a snapshot goes to a member, which member and the RSM index of
/// the checkpoint it carries: once the copy is made and again with every piece
/// the member makes room for. The leader keeps its log from there for that
/// member ([`super::RaftOpts::member_hold`]), so the member, once it restarted
/// on the snapshot, catches up from the log instead of being sent another.
pub(crate) type Sending = Arc<dyn Fn(NodeId, u64) + Send + Sync>;

/// What a node needs to send its snapshot.
pub(crate) struct SendCtx {
    pub(crate) data_dir: PathBuf,
    pub(crate) copy_store: CopyStore,
    pub(crate) copy_traces: CopyTraces,
    pub(crate) qlog: QLogReader,
    /// The membership in force at the copied checkpoint.
    pub(crate) membership_at: MembershipAt,
    /// See [`Sending`]; `None`: nothing is held for the member.
    pub(crate) sending: Option<Sending>,
}

impl SendCtx {
    pub(crate) fn new<S: Store + 'static>(
        data_dir: PathBuf,
        store: Arc<S>,
        qlog: QLogReader,
        membership_at: MembershipAt,
    ) -> SendCtx {
        let traces_dir = data_dir.join(crate::rsm::traces::DIR);
        let versions = store.clone();
        SendCtx {
            data_dir,
            copy_store: Box::new(move |dir| {
                store
                    .copy_checkpoint(dir)
                    .map_err(|e| io::Error::other(format!("copy the store: {e}")))
            }),
            copy_traces: Box::new(move |dir| {
                // Catalogue version 4 only (traces on disk). Below it no trace
                // was ever written there — the receiver's correct trace store
                // is the empty one — and a 2.0.x receiver refuses a `traces/`
                // path, so nothing is sent.
                use crate::rsm::store::TypedReads;
                let cluster = versions
                    .read(|r| r.cluster_version())
                    .map_err(|e| io::Error::other(format!("read the cluster version: {e}")))?;
                if !crate::rsm::effect::cluster_allows(cluster, crate::rsm::effect::VERSION_4) {
                    return Ok(false);
                }
                let traces = crate::rsm::traces::lookup(&traces_dir).ok_or_else(|| {
                    io::Error::other("the trace store is not open on this node; try again")
                })?;
                traces
                    .copy_to(dir)
                    .map_err(|e| io::Error::other(format!("copy the trace store: {e}")))?;
                Ok(true)
            }),
            qlog,
            membership_at,
            sending: None,
        }
    }
}

/// What a node needs to receive one.
pub(crate) struct RecvCtx {
    pub(crate) data_dir: PathBuf,
    pub(crate) restart: Arc<Restart>,
    /// One transfer at a time: every transfer writes the same
    /// `snapshot.pending` (through the same `.tmp`), so a second is refused
    /// while one runs, and the leader sends again.
    pub(crate) receiving: tokio::sync::Mutex<()>,
    /// Where this node's log stands, for a sender that may send a delta.
    /// `None`: it is not told, and gets the whole state.
    pub(crate) basis: Option<BasisOf>,
}

#[derive(Serialize, Deserialize)]
struct Header {
    vote: Vote,
    meta: SnapshotMeta,
    /// Where the copied store reopens: its checkpoint.
    applied: u64,
    applied_term: u64,
    files: Vec<FileEntry>,
    /// A DELTA (see the module header): the receiver's log already holds
    /// every record up to this RSM index, and the files carry only the runs
    /// above it (`qdelta/…`) besides the store. `None`: the whole state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    basis: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
struct FileEntry {
    /// Relative to the snapshot root, `/`-separated.
    path: String,
    len: u64,
    /// Where the sender reads it, when not at `path` from its first byte: a
    /// file under the staging directory and an offset (a delta's run is a
    /// slice of a queue-log file). Never sent.
    #[serde(skip)]
    source: Option<(String, u64)>,
}

/// Where a member's log stands, as it answers `/raft/v1/snapshot/basis`
/// before a snapshot is built for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Basis {
    /// The RSM index the member applied: its queue logs hold every record up
    /// to it.
    pub(crate) applied: u64,
    /// Its queue-log layout ([`QLogReader::lanes`], [`QLogReader::shards`]):
    /// a delta's runs land in the logs of the same name only under the same.
    pub(crate) lanes: u64,
    pub(crate) shards: u64,
}

/// Answers `/raft/v1/snapshot/basis` on a member.
pub(crate) type BasisOf = Box<dyn Fn() -> Basis + Send + Sync>;

/// `snapshot.pending` / `snapshot.swapping`.
#[derive(Serialize, Deserialize)]
struct Pending {
    /// The staged snapshot, relative to the data directory.
    staged: String,
    applied: u64,
    applied_term: u64,
    last_log_id: Option<LogId>,
    membership: StoredMembership,
    /// Whether the snapshot carries the trace environment (`traces/`). One
    /// that does not replaces this node's with an empty one at the swap. A
    /// marker an older build wrote has no such field: it carried none.
    #[serde(default)]
    traces: bool,
    /// A delta's basis ([`Header::basis`]): the swap keeps this node's queue
    /// logs, cut above it, and appends the runs. `None`: the whole state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    basis: Option<u64>,
}

fn stamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    fs::File::open(dir)?.sync_all()
}

// ---------------------------------------------------------------------------
// Sending
// ---------------------------------------------------------------------------

/// Hard-link every regular file under `src` into `dst` (same layout), except
/// temporaries. A sealed file's `.qidx` goes with it: it indexes bytes that
/// are sent unchanged, so the receiver opens it (verifying it, and rebuilding
/// only one that fails) instead of reading every file of the snapshot again
/// to rebuild them all. A file that vanishes between the listing and the link
/// was reclaimed as dead: skipped.
///
/// A queue log's files are taken NEWEST first, and a file created ahead of
/// its roll and not taken yet is left out (`qlog::is_precreated_file`): each
/// file is sent as far as it went when it was linked, so the receiver must
/// never get a live file newer than one it got mid-write — that older file
/// would reopen as SEALED with a torn tail, which is corruption. Newest first,
/// a file that is live when linked had its predecessor sealed (whole and
/// fsynced) before, and that predecessor is linked after; a file created ahead
/// may turn live after the listing, so it is never sent.
fn link_tree(src: &Path, dst: &Path, rel: &str, out: &mut Vec<FileEntry>) -> io::Result<()> {
    let rd = match fs::read_dir(src) {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    fs::create_dir_all(dst)?;
    let mut names: Vec<_> = rd.collect::<Result<Vec<_>, _>>()?;
    names.sort_by_key(|e| std::cmp::Reverse(e.file_name()));
    for e in names {
        let name = e.file_name();
        let Some(n) = name.to_str() else { continue };
        let ft = e.file_type()?;
        let child_rel = format!("{rel}/{n}");
        if ft.is_dir() {
            // A retired queue-log directory (`q<id>.dead`): on its way out.
            if n.contains(".dead") {
                continue;
            }
            link_tree(&e.path(), &dst.join(n), &child_rel, out)?;
            continue;
        }
        if !ft.is_file() || n.ends_with(".tmp") || n.contains(".compact") || n.ends_with(".dead") {
            continue;
        }
        if n.ends_with(".qlog") {
            match crate::rsm::qlog::is_precreated_file(&e.path()) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            }
        }
        match fs::hard_link(e.path(), dst.join(n)) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        }
        let len = fs::metadata(dst.join(n))?.len();
        out.push(FileEntry {
            path: child_rel,
            len,
            source: None,
        });
    }
    Ok(())
}

/// Build what is sent: the store copy, then links to every log file — or,
/// for a member whose log already reaches `basis`, the runs of records above
/// it ([`materialize_runs`]). Answers the store's checkpoint, its term, the
/// files, and the basis it used (none when it built the whole state).
fn materialize(
    ctx: &SendCtx,
    dir: &Path,
    basis: Option<u64>,
) -> io::Result<(u64, u64, Vec<FileEntry>, Option<u64>)> {
    fs::create_dir_all(dir)?;
    let _paused = ctx.qlog.pause_reclaim();
    let (applied, term) = (ctx.copy_store)(&dir.join("store"))?;
    let mut files = vec![FileEntry {
        path: "store/data.mdb".into(),
        len: fs::metadata(dir.join("store").join("data.mdb"))?.len(),
        source: None,
    }];
    // The trace environment AFTER the store: every trace of an entry at or
    // below the store copy's index was committed before that copy began, so
    // this copy holds them, and its own applied index tells the receiver which
    // entries above the store's index to skip the trace half of.
    let traces = dir.join(crate::rsm::traces::DIR);
    if (ctx.copy_traces)(&traces)? {
        files.push(FileEntry {
            path: format!("{}/data.mdb", crate::rsm::traces::DIR),
            len: fs::metadata(traces.join("data.mdb"))?.len(),
            source: None,
        });
    }
    // A basis at or above the copy: the member has as much as this already;
    // the whole state is what openraft asked for.
    let basis = basis.filter(|b| *b < applied);
    match basis {
        Some(after) => materialize_runs(ctx, dir, after, applied, &mut files)?,
        None => link_tree(ctx.qlog.root(), &dir.join("qlog"), "qlog", &mut files)?,
    }
    link_tree(
        &ctx.data_dir.join("seg"),
        &dir.join("seg"),
        "seg",
        &mut files,
    )?;
    Ok((applied, term, files, basis))
}

/// A delta's queue-log part: every run of records with `seq` in
/// `(after, upto]` ([`QLogReader::seq_runs`]), each its own file
/// `qdelta/<log>/r<file id>.part`, read at send time from a hard link to the
/// queue-log file it slices (`qsrc/…`), so retention cannot take the file
/// away while the stream reads it.
fn materialize_runs(
    ctx: &SendCtx,
    dir: &Path,
    after: u64,
    upto: u64,
    files: &mut Vec<FileEntry>,
) -> io::Result<()> {
    for run in ctx.qlog.seq_runs(after, upto)? {
        let name = run
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| io::Error::other("a queue-log file without a name"))?
            .to_string();
        let link_rel = format!("qsrc/{}/{name}", run.log_dir);
        let link = dir.join(&link_rel);
        if !link.exists() {
            if let Some(parent) = link.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::hard_link(&run.path, &link)?;
        }
        files.push(FileEntry {
            path: format!("qdelta/{}/r{:08}.part", run.log_dir, run.file_id),
            len: run.end - run.start,
            source: Some((link_rel, run.start)),
        });
    }
    Ok(())
}

/// The request body: prefix, every file, the checksum. Read on a plain thread,
/// which holds `keep` (the staging directory the files are read from) until
/// it has read the last of them or the receiver went away, and calls
/// `reading` after every piece of a file the receiver made room for.
fn body_of(
    dir: PathBuf,
    prefix: Vec<u8>,
    files: Vec<FileEntry>,
    keep: Option<Arc<Staging>>,
    reading: Option<Box<dyn Fn() + Send>>,
) -> Body {
    let (tx, rx) = tokio::sync::mpsc::channel::<io::Result<Bytes>>(8);
    std::thread::Builder::new()
        .name("queen-raft-snap-send".into())
        .spawn(move || {
            let _keep = keep;
            let run = || -> io::Result<()> {
                if tx.blocking_send(Ok(Bytes::from(prefix))).is_err() {
                    return Ok(());
                }
                let mut h = xxhash_rust::xxh3::Xxh3::new();
                for f in &files {
                    let mut file = match &f.source {
                        Some((rel, at)) => {
                            let mut file = fs::File::open(dir.join(rel))?;
                            std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(*at))?;
                            file
                        }
                        None => fs::File::open(dir.join(&f.path))?,
                    };
                    let mut left = f.len;
                    while left > 0 {
                        let n = (left as usize).min(CHUNK);
                        let mut buf = vec![0u8; n];
                        file.read_exact(&mut buf)?;
                        h.update(&buf);
                        left -= n as u64;
                        if tx.blocking_send(Ok(Bytes::from(buf))).is_err() {
                            return Ok(());
                        }
                        if let Some(reading) = &reading {
                            reading();
                        }
                    }
                }
                let _ = tx.blocking_send(Ok(Bytes::copy_from_slice(&h.digest().to_le_bytes())));
                Ok(())
            };
            if let Err(e) = run() {
                let _ = tx.blocking_send(Err(e));
            }
        })
        .expect("spawn the snapshot sender");
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });
    Body::from_stream(stream)
}

/// Where `target`'s log stands, when a delta can be built for it: its
/// applied index, if it answers (a node before 2.6 does not) with this node's
/// queue-log layout and some log. `None`: the whole state is sent.
async fn ask_basis(
    ctx: &SendCtx,
    client: &HttpClient,
    url: &str,
    token: Option<&str>,
    target: NodeId,
) -> Option<u64> {
    // A member that is starting answers its snapshot route a moment after it
    // first refuses connections, or after a slow answer: asked again, so a
    // member whose log is there is not sent the whole state for a race. A
    // node before 2.6 answers 404: the whole state at once.
    let mut answer = None;
    for attempt in 0..5 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
        let asked = post(
            client,
            &format!("{url}/basis"),
            token,
            "application/json",
            Body::from("{}"),
            Duration::from_secs(10),
        )
        .await;
        match asked {
            Ok(bytes) => {
                answer = serde_json::from_slice::<Basis>(&bytes).ok();
                break;
            }
            Err(Fail::Network(why)) if why.contains("404") => break,
            Err(_) => continue,
        }
    }
    let usable = answer
        .filter(|b| b.applied > 0 && b.lanes == ctx.qlog.lanes() && b.shards == ctx.qlog.shards());
    tracing::info!(
        target: "rsm",
        target_node = target,
        basis = ?answer,
        delta = usable.is_some(),
        "raft: a member's log, before a snapshot is built for it",
    );
    usable.map(|b| b.applied)
}

/// Send `snapshot` to the node at `url`. `Ok(Err(..))` is the follower's
/// refusal; `Err` a transport failure.
pub(crate) async fn send(
    ctx: &Arc<SendCtx>,
    client: &HttpClient,
    url: &str,
    token: Option<&str>,
    target: NodeId,
    vote: Vote,
    snapshot: Snapshot,
) -> Result<Result<SnapshotResponse<TypeConfig>, RaftError<TypeConfig>>, Fail> {
    let dir = ctx
        .data_dir
        .join(SNAP_DIR)
        .join(format!("send-{target}-{}", stamp()));
    // Removed when the last of its owners lets go: this future, or the copy
    // below, which runs on after openraft cancels the send (a leader change).
    let staging = Arc::new(Staging(dir.clone()));
    let asked = ask_basis(ctx, client, url, token, target).await;
    let c = ctx.clone();
    let s = staging.clone();
    let made = tokio::task::spawn_blocking(move || materialize(&c, &s.0, asked))
        .await
        .map_err(|e| Fail::Network(format!("snapshot build: {e}")))?;
    let (applied, applied_term, files, basis) =
        made.map_err(|e| Fail::Network(format!("snapshot build: {e}")))?;
    if let Some(sending) = &ctx.sending {
        sending(target, applied);
    }
    // The copy is the store's checkpoint NOW, which can be past the one openraft
    // built this snapshot at: name the copy's own checkpoint and the membership
    // in force there, so the follower installs one checkpoint, not two (an older
    // membership beside a newer image is a node on a stale voter set).
    let last_log_id = applied_log_id(applied, applied_term);
    let Some(last_membership) = (ctx.membership_at)(&last_log_id) else {
        return Err(Fail::Network(format!(
            "snapshot at {last_log_id:?}: the membership in force there is no longer \
             known; the next attempt copies a later checkpoint"
        )));
    };
    let total: u64 = files.iter().map(|f| f.len).sum();
    tracing::info!(
        target: "rsm",
        target_node = target,
        built = ?snapshot.meta.last_log_id,
        last_log_id = ?last_log_id,
        applied,
        delta_after = ?basis,
        files = files.len(),
        bytes = total,
        "raft: sending a snapshot",
    );
    let header = Header {
        vote,
        meta: SnapshotMeta {
            last_log_id,
            last_membership,
        },
        applied,
        applied_term,
        files: files.clone(),
        basis,
    };
    let header = serde_json::to_vec(&header).map_err(|e| Fail::Network(e.to_string()))?;
    let mut prefix = Vec::with_capacity(9 + header.len());
    prefix.extend_from_slice(MAGIC);
    prefix.push(VERSION);
    prefix.extend_from_slice(&(header.len() as u32).to_le_bytes());
    prefix.extend_from_slice(&header);
    // A floor of 10 MB/s, plus a minute for the follower's fsyncs.
    let ttl = Duration::from_secs(60 + total / (10 << 20));
    let reading = ctx
        .sending
        .clone()
        .map(|sending| Box::new(move || sending(target, applied)) as Box<dyn Fn() + Send>);
    let res = post(
        client,
        url,
        token,
        "application/octet-stream",
        body_of(dir.clone(), prefix, files, None, reading),
        ttl,
    )
    .await;
    drop(staging);
    let bytes = res?;
    serde_json::from_slice(&bytes).map_err(|e| Fail::Network(format!("snapshot answer: {e}")))
}

/// This node's snapshot as a stream, for a caller that is not a follower: a
/// standby cluster's seed ([`crate::rsm::link::seed`]). The same stream a
/// follower receives — the store's checkpoint as it is now, every queue-log
/// and segment file, the checksum — built the same way ([`materialize`]). The
/// header's vote says nothing (the receiver starts a cluster of its own) and
/// its membership is this cluster's, which the receiver replaces.
///
/// The staging directory lives as long as the stream is read.
///
/// `held` is told the RSM index of the checkpoint the snapshot holds: once the
/// copy is made, before a byte of it is sent, and again with every piece of a
/// file the receiver makes room for — as long as the stream is read, and no
/// longer. Whoever keeps this node's log for the receiver's first read
/// ([`super::link::LinkSource::seed`]) learns from it where to keep it from,
/// and that the receiver is still there. It is called off the runtime, and
/// may block.
pub(crate) async fn stream_checkpoint(ctx: &Arc<SendCtx>, held: Option<Held>) -> io::Result<Body> {
    let dir = ctx
        .data_dir
        .join(SNAP_DIR)
        .join(format!("seed-{}", stamp()));
    let staging = Arc::new(Staging(dir.clone()));
    let c = ctx.clone();
    let s = staging.clone();
    let first = held.clone();
    let (applied, applied_term, files, _) = tokio::task::spawn_blocking(move || {
        let made = materialize(&c, &s.0, None)?;
        if let Some(held) = &first {
            held(made.0);
        }
        Ok::<_, io::Error>(made)
    })
    .await
    .map_err(|e| io::Error::other(format!("snapshot build: {e}")))??;
    let last_log_id = applied_log_id(applied, applied_term);
    let Some(last_membership) = (ctx.membership_at)(&last_log_id) else {
        return Err(io::Error::other(format!(
            "snapshot at {last_log_id:?}: the membership in force there is no longer known; \
             ask again"
        )));
    };
    let total: u64 = files.iter().map(|f| f.len).sum();
    tracing::info!(
        target: "rsm",
        last_log_id = ?last_log_id,
        applied,
        files = files.len(),
        bytes = total,
        "raft: streaming a snapshot to a standby cluster's seed",
    );
    let header = Header {
        // No leader sends this: the receiver starts a cluster of its own.
        vote: Vote::new(applied_term, 0),
        meta: SnapshotMeta {
            last_log_id,
            last_membership,
        },
        applied,
        applied_term,
        files: files.clone(),
        basis: None,
    };
    let header = serde_json::to_vec(&header).map_err(io::Error::other)?;
    let mut prefix = Vec::with_capacity(9 + header.len());
    prefix.extend_from_slice(MAGIC);
    prefix.push(VERSION);
    prefix.extend_from_slice(&(header.len() as u32).to_le_bytes());
    prefix.extend_from_slice(&header);
    let reading = held.map(|held| Box::new(move || held(applied)) as Box<dyn Fn() + Send>);
    Ok(body_of(dir, prefix, files, Some(staging), reading))
}

/// A sender's staging directory (an LMDB copy, hard links to every queue-log
/// and segment file), removed when dropped: on success, on error, and when
/// openraft cancels the send, which drops the future wherever it stands. The
/// links pin the linked files' blocks after retention unlinks them, so a left
/// directory is disk that nothing frees until a restart.
struct Staging(PathBuf);

impl Drop for Staging {
    fn drop(&mut self) {
        let dir = std::mem::take(&mut self.0);
        match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                rt.spawn_blocking(move || {
                    let _ = fs::remove_dir_all(dir);
                });
            }
            Err(_) => {
                let _ = fs::remove_dir_all(dir);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Receiving
// ---------------------------------------------------------------------------

/// What the receiving writer thread is told.
enum W {
    Open(PathBuf),
    Data(Bytes),
    Close,
}

/// Write the files the stream carries; returns their xxh3.
fn writer(root: PathBuf, mut rx: tokio::sync::mpsc::Receiver<W>) -> io::Result<u64> {
    let mut h = xxhash_rust::xxh3::Xxh3::new();
    let mut cur: Option<fs::File> = None;
    let mut dirs: Vec<PathBuf> = vec![root.clone()];
    while let Some(w) = rx.blocking_recv() {
        match w {
            W::Open(rel) => {
                let path = root.join(&rel);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                    if !dirs.iter().any(|d| d == parent) {
                        dirs.push(parent.to_path_buf());
                    }
                }
                cur = Some(fs::File::create(&path)?);
            }
            W::Data(b) => {
                h.update(&b);
                cur.as_mut()
                    .ok_or_else(|| io::Error::other("snapshot data before a file"))?
                    .write_all(&b)?;
            }
            W::Close => {
                if let Some(f) = cur.take() {
                    f.sync_all()?;
                }
            }
        }
    }
    for d in dirs.iter().rev() {
        sync_dir(d)?;
    }
    Ok(h.digest())
}

fn bad(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// A relative path from the header, refused if it could leave the staging
/// directory.
fn safe_rel(p: &str) -> io::Result<PathBuf> {
    let path = PathBuf::from(p);
    let ok = !p.is_empty()
        && path
            .components()
            .all(|c| matches!(c, std::path::Component::Normal(_)))
        && ["store/", "qlog/", "seg/", "traces/", "qdelta/"]
            .iter()
            .any(|pre| p.starts_with(pre));
    if ok {
        Ok(path)
    } else {
        Err(bad(format!("snapshot file path `{p}` is not allowed")))
    }
}

/// One checkpoint: the meta names the copied store's own, with the membership
/// in force there. A sender that named an older one would leave this node on
/// that older voter set, since openraft re-reads membership only from the log
/// above the store's checkpoint.
fn one_checkpoint(meta: &SnapshotMeta, applied: u64, applied_term: u64) -> io::Result<()> {
    if meta.last_log_id != applied_log_id(applied, applied_term)
        || meta.last_membership.log_id() > &meta.last_log_id
    {
        return Err(bad(format!(
            "snapshot header names {:?} (membership at {:?}) for a store copied at {applied}: \
             two checkpoints",
            meta.last_log_id,
            meta.last_membership.log_id(),
        )));
    }
    Ok(())
}

/// Receive a snapshot, stage it, write the marker, and hand it to openraft.
pub(crate) async fn receive<S: Store + 'static>(
    ctx: &RecvCtx,
    raft: &RaftHandle<S>,
    body: Body,
) -> io::Result<Result<SnapshotResponse<TypeConfig>, RaftError<TypeConfig>>> {
    if let Some(why) = ctx.restart.requested() {
        // Already stopped on a snapshot: it loads at the restart.
        return Err(io::Error::other(format!("this node is restarting: {why}")));
    }
    let Ok(_one) = ctx.receiving.try_lock() else {
        return Err(io::Error::other("another snapshot is being received here"));
    };
    let (header, rel_dir) = stage_stream(&ctx.data_dir, body.into_data_stream()).await?;
    let staged = ctx.data_dir.join(&rel_dir);

    // The marker goes down BEFORE openraft sees the snapshot: see the module
    // header, step 1.
    write_pending(
        &ctx.data_dir,
        &header,
        rel_dir,
        header.meta.last_membership.clone(),
    )?;
    tracing::info!(
        target: "rsm",
        last_log_id = ?header.meta.last_log_id,
        applied = header.applied,
        "raft: a snapshot is staged; handing it to openraft",
    );
    let snap = Snapshot {
        meta: header.meta.clone(),
        snapshot: Checkpoint {
            last_log_id: header.meta.last_log_id,
        },
    };
    match raft.install_full_snapshot(header.vote, snap).await {
        Ok(resp) => {
            if ctx.restart.requested().is_none() {
                // openraft did not install it: this node already has as much.
                let _ = fs::remove_file(ctx.data_dir.join(PENDING));
                let _ = fs::remove_dir_all(&staged);
            }
            Ok(Ok(resp))
        }
        // The state machine stopped openraft to load the snapshot at the
        // restart: it is durably staged, so tell the leader it is installed (it
        // then replicates from the snapshot on instead of sending it again).
        Err(_) if ctx.restart.requested().is_some() => Ok(Ok(SnapshotResponse::new(header.vote))),
        Err(fatal) => Ok(Err(RaftError::Fatal(fatal))),
    }
}

/// Write `snapshot.pending` for the snapshot staged at `rel_dir`: at the next
/// boot [`apply_pending`] swaps it in, with `membership` as the node's.
fn write_pending(
    data_dir: &Path,
    header: &Header,
    rel_dir: String,
    membership: StoredMembership,
) -> io::Result<()> {
    let traces_prefix = format!("{}/", crate::rsm::traces::DIR);
    let pending = Pending {
        staged: rel_dir,
        applied: header.applied,
        applied_term: header.applied_term,
        last_log_id: header.meta.last_log_id,
        membership,
        traces: header
            .files
            .iter()
            .any(|f| f.path.starts_with(&traces_prefix)),
        basis: header.basis,
    };
    write_atomic(
        data_dir,
        PENDING,
        &serde_json::to_vec(&pending).map_err(io::Error::other)?,
    )
}

/// What a standby's seed staged ([`stage_seed`]): where the source's log
/// stood in the snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Seeded {
    /// The source's RSM index the snapshot's store had applied.
    pub applied: u64,
    pub applied_term: u64,
}

/// Stage a source cluster's snapshot stream ([`stream_checkpoint`]) under
/// `data_dir` as the first state of a NEW cluster whose only voter is this
/// node: at the next boot [`apply_pending`] swaps it in.
///
/// Two things make it a cluster of its own, not a member of the source's:
///
/// - the membership the node boots with names `node_id` alone, at `node`'s
///   addresses — the source's membership never reaches the disk;
/// - the node's vote is its own, in the term AFTER the snapshot's last entry,
///   so the first entry it writes as leader does not take the log's terms
///   backwards (what `QUEEN_RAFT_FORCE_RECOVER` does for a survivor).
///
/// `data_dir` must hold no data: the caller checked.
pub async fn stage_seed<St, E>(
    data_dir: &Path,
    stream: St,
    node_id: NodeId,
    node: super::types::QueenNode,
) -> io::Result<Seeded>
where
    St: futures_util::Stream<Item = Result<Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    let (header, rel_dir) = stage_stream(data_dir, stream).await?;
    let membership = super::types::Membership::new(
        vec![std::collections::BTreeSet::from([node_id])],
        std::collections::BTreeMap::from([(node_id, node)]),
    )
    .map_err(|e| io::Error::other(format!("the seeded membership: {e}")))?;
    let term = header.applied_term + 1;
    super::log_store::seed_vote(&data_dir.join("raft"), Vote::new(term, node_id))?;
    write_pending(
        data_dir,
        &header,
        rel_dir,
        StoredMembership::new(header.meta.last_log_id, membership),
    )?;
    sync_dir(data_dir)?;
    tracing::warn!(
        target: "rsm",
        node = node_id,
        applied = header.applied,
        applied_term = header.applied_term,
        files = header.files.len(),
        "raft: a source cluster's snapshot is staged as this node's first state",
    );
    Ok(Seeded {
        applied: header.applied,
        applied_term: header.applied_term,
    })
}

/// Read a snapshot stream into a fresh staging directory under `data_dir`
/// and check it: the header, every file, the checksum. Returns the header and
/// the staging directory, relative to `data_dir`. Nothing is left behind on
/// an error.
async fn stage_stream<St, E>(data_dir: &Path, mut stream: St) -> io::Result<(Header, String)>
where
    St: futures_util::Stream<Item = Result<Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    let mut buf: Vec<u8> = Vec::new();
    // The prefix and header.
    let header: Header = loop {
        if buf.len() >= 9 {
            if &buf[..4] != MAGIC || buf[4] != VERSION {
                return Err(bad("not a Queen snapshot stream"));
            }
            let hlen = u32::from_le_bytes(buf[5..9].try_into().expect("4")) as usize;
            if buf.len() >= 9 + hlen {
                let h: Header = serde_json::from_slice(&buf[9..9 + hlen])
                    .map_err(|e| bad(format!("snapshot header: {e}")))?;
                buf.drain(..9 + hlen);
                break h;
            }
        }
        match stream.next().await {
            Some(Ok(b)) => buf.extend_from_slice(&b),
            Some(Err(e)) => return Err(io::Error::other(format!("snapshot stream: {e}"))),
            None => return Err(bad("snapshot stream ended in its header")),
        }
    };
    one_checkpoint(&header.meta, header.applied, header.applied_term)?;
    let rels: Vec<PathBuf> = header
        .files
        .iter()
        .map(|f| safe_rel(&f.path))
        .collect::<io::Result<_>>()?;
    let rel_dir = format!("{SNAP_DIR}/recv-{}", stamp());
    let staged = data_dir.join(&rel_dir);
    let _ = fs::remove_dir_all(&staged);
    fs::create_dir_all(&staged)?;

    let (tx, rx) = tokio::sync::mpsc::channel::<W>(16);
    let root = staged.clone();
    let join = std::thread::Builder::new()
        .name("queen-raft-snap-recv".into())
        .spawn(move || writer(root, rx))
        .map_err(|e| io::Error::other(format!("spawn the snapshot writer: {e}")))?;
    let stream_result: io::Result<[u8; 8]> = async {
        let closed = || io::Error::other("the snapshot writer stopped");
        let mut pending: Bytes = Bytes::from(std::mem::take(&mut buf));
        for (f, rel) in header.files.iter().zip(&rels) {
            tx.send(W::Open(rel.clone())).await.map_err(|_| closed())?;
            let mut left = f.len;
            while left > 0 {
                if pending.is_empty() {
                    pending = match stream.next().await {
                        Some(Ok(b)) => b,
                        Some(Err(e)) => {
                            return Err(io::Error::other(format!("snapshot stream: {e}")))
                        }
                        None => return Err(bad(format!("snapshot stream ended in {}", f.path))),
                    };
                    continue;
                }
                let n = (left as usize).min(pending.len());
                let part = pending.split_to(n);
                left -= n as u64;
                tx.send(W::Data(part)).await.map_err(|_| closed())?;
            }
            tx.send(W::Close).await.map_err(|_| closed())?;
        }
        let mut trailer: Vec<u8> = pending.to_vec();
        while trailer.len() < 8 {
            match stream.next().await {
                Some(Ok(b)) => trailer.extend_from_slice(&b),
                Some(Err(e)) => return Err(io::Error::other(format!("snapshot stream: {e}"))),
                None => return Err(bad("snapshot stream ended before its checksum")),
            }
        }
        if trailer.len() != 8 {
            return Err(bad("snapshot stream has trailing bytes"));
        }
        Ok(trailer[..8].try_into().expect("8"))
    }
    .await;
    drop(tx);
    let digest = tokio::task::spawn_blocking(move || join.join())
        .await
        .map_err(|e| io::Error::other(format!("snapshot writer: {e}")))?
        .map_err(|_| io::Error::other("the snapshot writer panicked"))?;
    let trailer = match (stream_result, digest) {
        (Ok(t), Ok(d)) if u64::from_le_bytes(t) == d => t,
        (Ok(_), Ok(_)) => Err(bad("snapshot checksum mismatch")).inspect_err(|_e| {
            let _ = fs::remove_dir_all(&staged);
        })?,
        (Err(e), _) | (_, Err(e)) => {
            let _ = fs::remove_dir_all(&staged);
            return Err(e);
        }
    };
    let _ = trailer;
    Ok((header, rel_dir))
}

// ---------------------------------------------------------------------------
// Boot: swap a received snapshot in
// ---------------------------------------------------------------------------

/// The highest complete entry the queue logs under `data_dir` hold.
fn qlog_tail(data_dir: &Path, qopts: QLogOptions) -> io::Result<u64> {
    let mut set = QLogSet::new(data_dir.join("qlog"), qopts);
    set.reopen_all()
}

/// A delta's queue-log half: cut every log above `after`, then append the
/// staged runs (`qdelta/<log>/r<file id>.part`, in log and file order) to the
/// logs of the same names ([`QLogSet::append_raw_into`]).
fn apply_runs(data_dir: &Path, staged: &Path, after: u64, qopts: QLogOptions) -> io::Result<()> {
    let t0 = std::time::Instant::now();
    let sorted = |dir: &Path| -> io::Result<Vec<PathBuf>> {
        let mut out: Vec<PathBuf> = match fs::read_dir(dir) {
            Ok(rd) => rd.map(|e| e.map(|e| e.path())).collect::<Result<_, _>>()?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e),
        };
        out.sort();
        Ok(out)
    };
    // Every log, cut above the basis on its files before it opens: what this
    // node applied after the basis may be sealed already, and the runs carry it
    // again ([`crate::rsm::qlog::cut_log_above`]).
    let root = data_dir.join("qlog");
    let mut cut = 0u64;
    for dir in sorted(&root)? {
        let is_log = dir.is_dir()
            && dir
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_prefix('q'))
                .is_some_and(|n| n.parse::<u64>().is_ok());
        if is_log {
            cut += crate::rsm::qlog::cut_log_above(&dir, after)?;
        }
    }
    let mut set = QLogSet::new(root, qopts);
    set.reopen_all()?;
    let (mut records, mut bytes, mut runs) = (0u64, 0u64, 0usize);
    for log_dir in sorted(&staged.join("qdelta"))? {
        let log_id = log_dir
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix('q'))
            .and_then(|n| n.parse::<u64>().ok())
            .ok_or_else(|| bad(format!("{}: not a queue log's name", log_dir.display())))?;
        for part in sorted(&log_dir)? {
            let run = fs::read(&part)?;
            bytes += run.len() as u64;
            records += set.append_raw_into(log_id, &run)?;
            runs += 1;
        }
    }
    tracing::warn!(
        target: "rsm",
        after,
        cut_bytes = cut,
        runs,
        records,
        bytes,
        ms = t0.elapsed().as_millis() as u64,
        "raft: a delta snapshot's runs are appended to this node's queue logs",
    );
    Ok(())
}

/// At boot, before the store opens: swap a received snapshot in, if one is
/// pending. Returns whether it did.
pub fn apply_pending(data_dir: &Path, qopts: QLogOptions) -> io::Result<bool> {
    let pending_path = data_dir.join(PENDING);
    let swapping_path = data_dir.join(SWAPPING);
    let resuming = swapping_path.exists();
    let marker = if resuming {
        Some(&swapping_path)
    } else if pending_path.exists() {
        Some(&pending_path)
    } else {
        None
    };
    let p: Option<Pending> = match marker {
        Some(m) => Some(
            serde_json::from_slice(&fs::read(m)?)
                .map_err(|e| bad(format!("{}: {e}", m.display())))?,
        ),
        None => None,
    };
    // Whatever else is in the snapshot area is left over from a transfer that
    // never finished (either side): gone.
    if let Ok(rd) = fs::read_dir(data_dir.join(SNAP_DIR)) {
        let keep = p.as_ref().map(|p| data_dir.join(&p.staged));
        for e in rd.flatten() {
            let path = e.path();
            if Some(&path) != keep.as_ref() && !path.ends_with("replaced") {
                let _ = fs::remove_dir_all(&path);
            }
        }
    }
    let Some(p) = p else {
        return Ok(false);
    };
    let staged = data_dir.join(&p.staged);
    if !resuming {
        let (local, _) =
            crate::rsm::store::heed_store::read_checkpoint_meta(&data_dir.join("store"))
                .map_err(|e| io::Error::other(format!("read the local store: {e}")))?;
        let tail = qlog_tail(data_dir, qopts)?;
        // A delta needs this node's log up to its basis: cut short (a disk
        // that lost its tail), it cannot be used, and the leader sends again.
        let short_of_basis = p.basis.is_some_and(|b| tail < b);
        if !staged.exists() || local >= p.applied || tail >= p.applied || short_of_basis {
            tracing::warn!(
                target: "rsm",
                snapshot_applied = p.applied,
                local_store = local,
                local_log = tail,
                basis = ?p.basis,
                "raft: discarding a received snapshot this node does not need or cannot use",
            );
            let _ = fs::remove_dir_all(&staged);
            fs::remove_file(&pending_path)?;
            sync_dir(data_dir)?;
            return Ok(false);
        }
        write_atomic(data_dir, SWAPPING, &fs::read(&pending_path)?)?;
    }
    tracing::warn!(
        target: "rsm",
        snapshot = ?p.last_log_id,
        applied = p.applied,
        basis = ?p.basis,
        resuming,
        "raft: swapping a received snapshot in",
    );
    if let Some(after) = p.basis {
        // A delta: this node's queue logs stay, cut above the basis, and take
        // the runs. Repeatable as a whole — a resumed swap cuts what a crash
        // left half-appended and appends again.
        apply_runs(data_dir, &staged, after, qopts)?;
    }
    let replaced = data_dir.join(SNAP_DIR).join("replaced");
    fs::create_dir_all(&replaced)?;
    // The trace environment is swapped with the rest, so it must not be open:
    // an instance still open in this process would go on serving the files
    // moved out from under it.
    let traces_dir = data_dir.join(crate::rsm::traces::DIR);
    let until = std::time::Instant::now() + Duration::from_secs(10);
    while crate::rsm::traces::lookup(&traces_dir).is_some() {
        // In-process reopens (tests): the last owner may be finishing a read.
        if std::time::Instant::now() >= until {
            return Err(io::Error::other(format!(
                "{} is open in this process: a snapshot is swapped in before it opens",
                traces_dir.display()
            )));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if !p.traces && traces_dir.exists() {
        // A snapshot without the trace environment comes from a cluster below
        // catalogue version 4, where no trace was ever written to it: this
        // node's goes too, and it reopens empty, which is that state.
        let old = replaced.join(crate::rsm::traces::DIR);
        if old.exists() {
            fs::remove_dir_all(&old)?;
        }
        fs::rename(&traces_dir, &old)?;
    }
    for comp in ["store", "qlog", "seg", crate::rsm::traces::DIR] {
        let src = staged.join(comp);
        if !src.exists() {
            continue;
        }
        let dst = data_dir.join(comp);
        if dst.exists() {
            let old = replaced.join(comp);
            if old.exists() {
                fs::remove_dir_all(&old)?;
            }
            fs::rename(&dst, &old)?;
        }
        fs::rename(&src, &dst)?;
    }
    sync_dir(data_dir)?;
    // Nothing above the store's checkpoint: the leader sends the rest.
    {
        let mut set = QLogSet::new(data_dir.join("qlog"), qopts);
        set.reopen_all()?;
        set.truncate_from_across(p.applied + 1)?;
    }
    let raft_dir = data_dir.join("raft");
    state_after_snapshot(&raft_dir, p.last_log_id)?;
    write_atomic(
        &raft_dir,
        "membership.json",
        &serde_json::to_vec(&p.membership).map_err(io::Error::other)?,
    )?;
    match fs::remove_file(&pending_path) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
        _ => {}
    }
    fs::remove_file(&swapping_path)?;
    sync_dir(data_dir)?;
    let _ = fs::remove_dir_all(&replaced);
    let _ = fs::remove_dir_all(&staged);
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::{applied_log_id, body_of, one_checkpoint, FileEntry, Staging, CHUNK};
    use crate::rsm::replicator::raft::types::{SnapshotMeta, StoredMembership};

    fn meta_at(index: u64, term: u64) -> SnapshotMeta {
        SnapshotMeta {
            last_log_id: applied_log_id(index, term),
            last_membership: StoredMembership::default(),
        }
    }

    #[test]
    fn a_snapshot_names_the_checkpoint_of_the_store_it_carries() {
        assert!(one_checkpoint(&meta_at(200, 3), 200, 3).is_ok());
        // Built at 100, the store copied at send time at 200: two checkpoints.
        let e = one_checkpoint(&meta_at(100, 3), 200, 3).unwrap_err();
        assert!(e.to_string().contains("two checkpoints"), "{e}");
    }

    #[tokio::test]
    async fn a_cancelled_send_leaves_no_staging_behind() {
        let dir = std::env::temp_dir().join(format!(
            "queen-snap-staging-{}-{}",
            std::process::id(),
            super::stamp()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("data.mdb"), b"copy").unwrap();
        let staging = Arc::new(Staging(dir.clone()));
        // The copy, still running when openraft drops the send.
        let held = staging.clone();
        let copy = tokio::task::spawn_blocking(move || {
            std::thread::sleep(Duration::from_millis(100));
            drop(held);
        });
        drop(staging);
        assert!(dir.exists(), "the copy still uses it");
        copy.await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while dir.exists() {
            assert!(Instant::now() < deadline, "the staging directory was left");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// What keeps a seed's hold alive ([`super::stream_checkpoint`]): the
    /// sender says the stream is being read once for every piece of a file
    /// the receiver made room for, and stops with the receiver.
    #[tokio::test]
    async fn a_stream_says_it_is_read_for_as_long_as_it_is() {
        use futures_util::StreamExt;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let dir = std::env::temp_dir().join(format!(
            "queen-snap-reading-{}-{}",
            std::process::id(),
            super::stamp()
        ));
        std::fs::create_dir_all(dir.join("store")).unwrap();
        // More pieces than the sender may run ahead of its receiver by.
        const PIECES: usize = 20;
        std::fs::write(
            dir.join("store").join("data.mdb"),
            vec![7u8; PIECES * CHUNK],
        )
        .unwrap();
        let files = || {
            vec![FileEntry {
                path: "store/data.mdb".into(),
                len: (PIECES * CHUNK) as u64,
                source: None,
            }]
        };
        let stream_of = |said: &Arc<AtomicUsize>| {
            let said = said.clone();
            let reading: Box<dyn Fn() + Send> = Box::new(move || {
                said.fetch_add(1, Ordering::SeqCst);
            });
            body_of(dir.clone(), b"head".to_vec(), files(), None, Some(reading)).into_data_stream()
        };
        // The sender is gone once it let go of what it was given.
        async fn sender_gone(said: &Arc<AtomicUsize>) {
            let deadline = Instant::now() + Duration::from_secs(10);
            while Arc::strong_count(said) > 1 {
                assert!(Instant::now() < deadline, "the sender is still there");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        // A receiver that reads to the end: every piece was said.
        let said = Arc::new(AtomicUsize::new(0));
        let mut stream = stream_of(&said);
        let mut bytes = 0usize;
        while let Some(piece) = stream.next().await {
            bytes += piece.expect("a piece").len();
        }
        assert_eq!(bytes, 4 + PIECES * CHUNK + 8, "head, file, checksum");
        sender_gone(&said).await;
        assert_eq!(said.load(Ordering::SeqCst), PIECES);

        // A receiver that leaves after the head: the sender stops where its
        // buffer let it get to, far from the end.
        let said = Arc::new(AtomicUsize::new(0));
        let mut stream = stream_of(&said);
        stream.next().await.expect("the head").expect("a piece");
        drop(stream);
        sender_gone(&said).await;
        let n = said.load(Ordering::SeqCst);
        assert!(n < PIECES, "{n} pieces said to a receiver that took one");
        let _ = std::fs::remove_dir_all(dir);
    }
}
