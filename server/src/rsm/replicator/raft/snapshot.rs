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

/// What a node needs to send its snapshot.
pub(crate) struct SendCtx {
    pub(crate) data_dir: PathBuf,
    pub(crate) copy_store: CopyStore,
    pub(crate) qlog: QLogReader,
    /// The membership in force at the copied checkpoint.
    pub(crate) membership_at: MembershipAt,
}

impl SendCtx {
    pub(crate) fn new<S: Store + 'static>(
        data_dir: PathBuf,
        store: Arc<S>,
        qlog: QLogReader,
        membership_at: MembershipAt,
    ) -> SendCtx {
        SendCtx {
            data_dir,
            copy_store: Box::new(move |dir| {
                store
                    .copy_checkpoint(dir)
                    .map_err(|e| io::Error::other(format!("copy the store: {e}")))
            }),
            qlog,
            membership_at,
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
}

#[derive(Serialize, Deserialize)]
struct Header {
    vote: Vote,
    meta: SnapshotMeta,
    /// Where the copied store reopens: its checkpoint.
    applied: u64,
    applied_term: u64,
    files: Vec<FileEntry>,
}

#[derive(Clone, Serialize, Deserialize)]
struct FileEntry {
    /// Relative to the snapshot root, `/`-separated.
    path: String,
    len: u64,
}

/// `snapshot.pending` / `snapshot.swapping`.
#[derive(Serialize, Deserialize)]
struct Pending {
    /// The staged snapshot, relative to the data directory.
    staged: String,
    applied: u64,
    applied_term: u64,
    last_log_id: Option<LogId>,
    membership: StoredMembership,
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
/// indexes and temporaries (the receiver rebuilds indexes). A file that
/// vanishes between the listing and the link was reclaimed as dead: skipped.
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
            link_tree(&e.path(), &dst.join(n), &child_rel, out)?;
            continue;
        }
        if !ft.is_file() || n.ends_with(".qidx") || n.ends_with(".tmp") || n.contains(".compact") {
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
        });
    }
    Ok(())
}

/// Build what is sent: the store copy, then links to every log file.
fn materialize(ctx: &SendCtx, dir: &Path) -> io::Result<(u64, u64, Vec<FileEntry>)> {
    fs::create_dir_all(dir)?;
    let _paused = ctx.qlog.pause_reclaim();
    let (applied, term) = (ctx.copy_store)(&dir.join("store"))?;
    let mut files = vec![FileEntry {
        path: "store/data.mdb".into(),
        len: fs::metadata(dir.join("store").join("data.mdb"))?.len(),
    }];
    link_tree(ctx.qlog.root(), &dir.join("qlog"), "qlog", &mut files)?;
    link_tree(
        &ctx.data_dir.join("seg"),
        &dir.join("seg"),
        "seg",
        &mut files,
    )?;
    Ok((applied, term, files))
}

/// The request body: prefix, every file, the checksum. Read on a plain thread.
fn body_of(dir: PathBuf, prefix: Vec<u8>, files: Vec<FileEntry>) -> Body {
    let (tx, rx) = tokio::sync::mpsc::channel::<io::Result<Bytes>>(8);
    std::thread::Builder::new()
        .name("queen-raft-snap-send".into())
        .spawn(move || {
            let run = || -> io::Result<()> {
                if tx.blocking_send(Ok(Bytes::from(prefix))).is_err() {
                    return Ok(());
                }
                let mut h = xxhash_rust::xxh3::Xxh3::new();
                for f in &files {
                    let mut file = fs::File::open(dir.join(&f.path))?;
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
    let c = ctx.clone();
    let s = staging.clone();
    let made = tokio::task::spawn_blocking(move || materialize(&c, &s.0))
        .await
        .map_err(|e| Fail::Network(format!("snapshot build: {e}")))?;
    let (applied, applied_term, files) =
        made.map_err(|e| Fail::Network(format!("snapshot build: {e}")))?;
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
    };
    let header = serde_json::to_vec(&header).map_err(|e| Fail::Network(e.to_string()))?;
    let mut prefix = Vec::with_capacity(9 + header.len());
    prefix.extend_from_slice(MAGIC);
    prefix.push(VERSION);
    prefix.extend_from_slice(&(header.len() as u32).to_le_bytes());
    prefix.extend_from_slice(&header);
    // A floor of 10 MB/s, plus a minute for the follower's fsyncs.
    let ttl = Duration::from_secs(60 + total / (10 << 20));
    let res = post(
        client,
        url,
        token,
        "application/octet-stream",
        body_of(dir.clone(), prefix, files),
        ttl,
    )
    .await;
    drop(staging);
    let bytes = res?;
    serde_json::from_slice(&bytes).map_err(|e| Fail::Network(format!("snapshot answer: {e}")))
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
        && ["store/", "qlog/", "seg/"]
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
    let mut stream = body.into_data_stream();
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
    let staged = ctx.data_dir.join(&rel_dir);
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

    // The marker goes down BEFORE openraft sees the snapshot: see the module
    // header, step 1.
    let pending = Pending {
        staged: rel_dir,
        applied: header.applied,
        applied_term: header.applied_term,
        last_log_id: header.meta.last_log_id,
        membership: header.meta.last_membership.clone(),
    };
    write_atomic(
        &ctx.data_dir,
        PENDING,
        &serde_json::to_vec(&pending).map_err(io::Error::other)?,
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

// ---------------------------------------------------------------------------
// Boot: swap a received snapshot in
// ---------------------------------------------------------------------------

/// The highest complete entry the queue logs under `data_dir` hold.
fn qlog_tail(data_dir: &Path, qopts: QLogOptions) -> io::Result<u64> {
    let mut set = QLogSet::new(data_dir.join("qlog"), qopts);
    set.reopen_all()
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
        if !staged.exists() || local >= p.applied || tail >= p.applied {
            tracing::warn!(
                target: "rsm",
                snapshot_applied = p.applied,
                local_store = local,
                local_log = tail,
                "raft: discarding a received snapshot this node does not need",
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
        resuming,
        "raft: swapping a received snapshot in",
    );
    let replaced = data_dir.join(SNAP_DIR).join("replaced");
    fs::create_dir_all(&replaced)?;
    for comp in ["store", "qlog", "seg"] {
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

    use super::{applied_log_id, one_checkpoint, Staging};
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
}
