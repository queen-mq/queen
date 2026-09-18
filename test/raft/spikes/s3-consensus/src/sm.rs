//! The state machine of the spike: entry payloads appended to files, applied
//! index in a small meta file, snapshots as an app-defined manifest.
//!
//! This is deliberately shaped like PLAN_RAFT.md §11: a live state directory
//! named by a `CURRENT` file (I17), rolled data files that become immutable
//! once sealed (§11.2), a durable point that records the applied index
//! together with the file lengths it refers to (I11), and snapshots that are a
//! directory of immutable files plus a MANIFEST with a per-file xxh3 (§11.6).
//!
//! What it does NOT model: the ordered store, positions, retention, GC. The
//! spike measures consensus, not the RSM.

use std::collections::BTreeSet;
use std::fs;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use futures::lock::Mutex;
use futures::Stream;
use futures::TryStreamExt;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::SnapshotOf;
use openraft::alias::StoredMembershipOf;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use openraft::EntryPayload;
use openraft::RaftSnapshotBuilder;
use serde::Deserialize;
use serde::Serialize;
use xxhash_rust::xxh3::xxh3_64;
use xxhash_rust::xxh3::Xxh3;

use crate::types::AppResponse;
use crate::types::LogId;
use crate::types::SnapshotMeta;
use crate::types::StoredMembership;
use crate::types::TypeConfig;

/// How hard a durable point pushes on the disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsyncMode {
    /// PLAN_RAFT.md §11.4: the durable point runs on a timer, not per entry.
    /// The Raft log is what makes a committed entry survive a crash (D7/I4 ask
    /// for commit + apply, not for a state-machine fsync), and recovery
    /// truncates the data files to the lengths the last durable point recorded
    /// (I11), re-applying the rest from the log.
    Periodic,
    /// fsync the data file and the meta after every apply batch: the strictest
    /// reading, and the one that shows what a per-entry durable point costs.
    Batch,
    /// Never fsync. Isolates consensus cost from disk cost.
    Never,
}

impl std::str::FromStr for FsyncMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "periodic" => Ok(FsyncMode::Periodic),
            "batch" => Ok(FsyncMode::Batch),
            "never" => Ok(FsyncMode::Never),
            other => Err(format!("unknown fsync mode {other}")),
        }
    }
}

/// One file of a state directory or of a snapshot manifest.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileMeta {
    pub name: String,
    pub size: u64,
    /// xxh3-64 of the whole file (§11.6).
    pub xxh3: u64,
}

/// The applied state, written next to the data files.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SmMeta {
    pub last_applied: Option<LogId>,
    pub membership: StoredMembership,
    /// Number of application entries applied (blanks and membership excluded).
    pub applied_count: u64,
    /// Order-dependent rolling hash over applied write ids.
    pub digest: u64,
    /// Data files in write order, with the length apply had made durable (I11).
    pub files: Vec<(String, u64)>,
    /// Monotonic sequence of this durable point; the higher of the two meta
    /// slots wins at open.
    #[serde(default)]
    pub seq: u64,
}

/// A snapshot: a directory of immutable files plus the state they reproduce.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub meta: SmMeta,
    pub files: Vec<FileMeta>,
}

/// What openraft carries as `SnapshotData` for us: a handle to a complete
/// snapshot directory. It is never the bytes themselves; the transport streams
/// the files the manifest names (PLAN_RAFT.md §11.6, D8).
#[derive(Clone, Debug)]
pub struct SnapshotHandle {
    pub dir: PathBuf,
    pub manifest: Manifest,
}

impl SnapshotHandle {
    pub fn read_manifest(dir: &Path) -> io::Result<Self> {
        let bytes = fs::read(dir.join("MANIFEST.json"))?;
        let manifest: Manifest = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        Ok(SnapshotHandle {
            dir: dir.to_path_buf(),
            manifest,
        })
    }
}

struct Inner {
    /// `<data>/sm`
    root: PathBuf,
    /// The live state directory named by CURRENT.
    live: PathBuf,
    meta: SmMeta,
    active: Option<File>,
    active_name: String,
    file_bytes: u64,
    fsync: FsyncMode,
    /// Set when this node built or installed a snapshot in this process.
    current_snapshot: Option<SnapshotHandle>,
}

#[derive(Clone)]
pub struct SmStore(Arc<Mutex<Inner>>);

impl SmStore {
    /// Open (or create) the state machine under `<dir>/sm`.
    pub fn open(dir: &Path, fsync: FsyncMode, file_bytes: u64) -> io::Result<Self> {
        let root = dir.join("sm");
        fs::create_dir_all(&root)?;
        fs::create_dir_all(root.join("snapshots"))?;
        fs::create_dir_all(root.join("recv"))?;

        let current = root.join("CURRENT");
        let live_name = if current.exists() {
            fs::read_to_string(&current)?.trim().to_string()
        } else {
            let name = "sm-0-0".to_string();
            fs::create_dir_all(root.join(&name))?;
            write_atomic(&current, name.as_bytes())?;
            name
        };
        let live = root.join(&live_name);
        fs::create_dir_all(&live)?;

        let mut meta = read_meta(&live)?;

        // I11: trust the meta the store reopens with; truncate the data files
        // to the lengths it records, discarding anything the crash left behind.
        for (name, len) in meta.files.clone() {
            let path = live.join(&name);
            let f = File::options().write(true).open(&path)?;
            let on_disk = f.metadata()?.len();
            if on_disk > len {
                tracing::warn!("sm: truncating {name} from {on_disk} to the durable {len}");
                f.set_len(len)?;
                f.sync_all()?;
            } else if on_disk < len {
                return Err(io::Error::other(format!(
                    "sm: {name} is {on_disk} bytes but the durable point says {len}"
                )));
            }
        }

        let mut store = Inner {
            root,
            live,
            active_name: String::new(),
            active: None,
            meta: SmMeta::default(),
            file_bytes,
            fsync,
            current_snapshot: None,
        };
        std::mem::swap(&mut store.meta, &mut meta);
        store.open_active()?;
        Ok(SmStore(Arc::new(Mutex::new(store))))
    }

    /// Applied ids, read back from the data files (works after a restart or a
    /// snapshot install, where nothing is cached).
    pub async fn applied_ids(&self) -> io::Result<BTreeSet<u64>> {
        let inner = self.0.lock().await;
        let mut ids = BTreeSet::new();
        for (name, len) in &inner.meta.files {
            let path = inner.live.join(name);
            let mut r = BufReader::new(File::open(&path)?);
            let mut pos = 0u64;
            while pos < *len {
                let mut hdr = [0u8; 20];
                if r.read_exact(&mut hdr).is_err() {
                    break;
                }
                let plen = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as u64;
                let id = u64::from_le_bytes(hdr[4..12].try_into().unwrap());
                ids.insert(id);
                r.seek_relative(plen as i64)?;
                pos += 20 + plen;
            }
        }
        Ok(ids)
    }

    /// Take a durable point now (§11.4: the periodic one).
    pub async fn durable(&self) -> io::Result<()> {
        let mut inner = self.0.lock().await;
        inner.durable_point(true)
    }

    pub async fn status(&self) -> (SmMeta, PathBuf) {
        let inner = self.0.lock().await;
        (inner.meta.clone(), inner.live.clone())
    }

    /// Stage directory for an incoming snapshot; kept across attempts so a
    /// killed transfer resumes per file (§11.6).
    pub async fn recv_dir(&self, meta: &SnapshotMeta) -> PathBuf {
        let inner = self.0.lock().await;
        let key = match &meta.last_log_id {
            Some(id) => format!("{}-{}", id.index, crate::util::term_of(id)),
            None => "empty".to_string(),
        };
        inner.root.join("recv").join(key)
    }
}

impl Inner {
    fn open_active(&mut self) -> io::Result<()> {
        // The active file is the last one in the meta list, unless it is full.
        let (name, len) = match self.meta.files.last() {
            Some((n, l)) if *l < self.file_bytes => (n.clone(), *l),
            _ => {
                let name = format!("data-{:06}.log", self.meta.files.len());
                self.meta.files.push((name.clone(), 0));
                (name, 0)
            }
        };
        let path = self.live.join(&name);
        let mut f = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        f.seek(SeekFrom::Start(len))?;
        self.active = Some(f);
        self.active_name = name;
        Ok(())
    }

    fn append_payload(&mut self, id: u64, payload: &[u8]) -> io::Result<()> {
        let f = self.active.as_mut().expect("active file");
        let mut frame = Vec::with_capacity(20 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&id.to_le_bytes());
        frame.extend_from_slice(&xxh3_64(payload).to_le_bytes());
        frame.extend_from_slice(payload);
        f.write_all(&frame)?;

        let last = self.meta.files.last_mut().expect("meta file entry");
        last.1 += frame.len() as u64;
        let rolled = last.1 >= self.file_bytes;

        self.meta.applied_count += 1;
        self.meta.digest = self.meta.digest.rotate_left(7) ^ id;

        if rolled {
            self.durable_point(true)?;
            self.open_active()?;
        }
        Ok(())
    }

    /// A durable point: file bytes on disk, then the meta that names their
    /// lengths (I11).
    ///
    /// The meta alternates between two slots, each self-checked, so a durable
    /// point costs two fsyncs and no rename: a torn or half-written slot is
    /// simply the loser at open.
    fn durable_point(&mut self, force: bool) -> io::Result<()> {
        let sync = force || self.fsync == FsyncMode::Batch;
        if let Some(f) = self.active.as_mut() {
            f.flush()?;
            if sync && self.fsync != FsyncMode::Never {
                f.sync_data()?;
            }
        }
        if !sync {
            return Ok(());
        }
        self.meta.seq += 1;
        let slot = self.meta.seq % 2;
        let bytes = serde_json::to_vec(&self.meta).map_err(io::Error::other)?;
        let mut framed = Vec::with_capacity(bytes.len() + 8);
        framed.extend_from_slice(&bytes);
        framed.extend_from_slice(&xxh3_64(&bytes).to_le_bytes());
        let path = self.live.join(format!("META-{slot}.json"));
        let mut f = File::options()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        f.write_all(&framed)?;
        if self.fsync != FsyncMode::Never {
            f.sync_data()?;
        }
        Ok(())
    }

    /// Seal every data file, then hard-link them into a snapshot directory
    /// with a MANIFEST (§11.6 steps 1-6).
    fn build_snapshot_dir(&mut self) -> io::Result<SnapshotHandle> {
        // 1+2: seal the active file and take a durable point that records it.
        self.durable_point(true)?;
        let sealed = self.meta.files.clone();
        self.active = None;
        let next = format!("data-{:06}.log", self.meta.files.len());
        self.meta.files.push((next, 0));
        self.open_active()?;
        self.durable_point(true)?;

        let (index, term) = match &self.meta.last_applied {
            Some(id) => (id.index, crate::util::term_of(id)),
            None => (0, 0),
        };
        let dir = self.root.join("snapshots").join(format!("{index}-{term}"));
        let tmp = self
            .root
            .join("snapshots")
            .join(format!(".tmp-{index}-{term}"));
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp)?;

        // 4: hard-link the sealed files; 5: MANIFEST with per-file xxh3.
        let mut files = Vec::new();
        for (name, len) in &sealed {
            if *len == 0 {
                continue;
            }
            let src = self.live.join(name);
            let dst = tmp.join(name);
            fs::hard_link(&src, &dst)?;
            // The link may show a longer file than the durable length; the
            // manifest records the durable length and the hash of that prefix.
            let xxh3 = hash_file_prefix(&dst, *len)?;
            if fs::metadata(&dst)?.len() != *len {
                let f = File::options().write(true).open(&dst)?;
                f.set_len(*len)?;
            }
            files.push(FileMeta {
                name: name.clone(),
                size: *len,
                xxh3,
            });
        }
        let mut meta = self.meta.clone();
        meta.files = sealed.into_iter().filter(|(_, l)| *l > 0).collect();
        let manifest = Manifest { meta, files };
        let bytes = serde_json::to_vec(&manifest).map_err(io::Error::other)?;
        write_atomic(&tmp.join("MANIFEST.json"), &bytes)?;
        fsync_dir(&tmp)?;
        let _ = fs::remove_dir_all(&dir);
        fs::rename(&tmp, &dir)?;
        fsync_dir(&self.root.join("snapshots"))?;

        Ok(SnapshotHandle { dir, manifest })
    }
}

/// Read the newer of the two meta slots, ignoring a torn one.
fn read_meta(live: &Path) -> io::Result<SmMeta> {
    let mut best: Option<SmMeta> = None;
    for slot in 0..2u64 {
        let path = live.join(format!("META-{slot}.json"));
        let Ok(bytes) = fs::read(&path) else { continue };
        if bytes.len() < 8 {
            continue;
        }
        let (body, sum) = bytes.split_at(bytes.len() - 8);
        if xxh3_64(body).to_le_bytes() != sum {
            tracing::warn!("sm: meta slot {slot} is torn, ignoring it");
            continue;
        }
        let Ok(meta) = serde_json::from_slice::<SmMeta>(body) else {
            continue;
        };
        if best.as_ref().map(|b| meta.seq > b.seq).unwrap_or(true) {
            best = Some(meta);
        }
    }
    Ok(best.unwrap_or_default())
}

/// Write a meta slot into a freshly staged directory (slot 0, seq kept).
fn write_meta_slot(dir: &Path, meta: &SmMeta) -> io::Result<()> {
    let bytes = serde_json::to_vec(meta).map_err(io::Error::other)?;
    let mut framed = bytes.clone();
    framed.extend_from_slice(&xxh3_64(&bytes).to_le_bytes());
    let path = dir.join(format!("META-{}.json", meta.seq % 2));
    let mut f = File::create(&path)?;
    f.write_all(&framed)?;
    f.sync_all()
}

#[allow(dead_code)]
fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        fsync_dir(parent)?;
    }
    Ok(())
}

pub fn fsync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// xxh3-64 of the first `len` bytes of a file.
pub fn hash_file_prefix(path: &Path, len: u64) -> io::Result<u64> {
    let mut f = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut hasher = Xxh3::new();
    let mut left = len;
    let mut buf = vec![0u8; 1 << 20];
    while left > 0 {
        let want = std::cmp::min(left as usize, buf.len());
        f.read_exact(&mut buf[..want])?;
        hasher.update(&buf[..want]);
        left -= want as u64;
    }
    Ok(hasher.digest())
}

impl RaftSnapshotBuilder<TypeConfig> for SmStore {
    type SnapshotData = SnapshotHandle;

    async fn build_snapshot(
        &mut self,
    ) -> Result<SnapshotOf<TypeConfig, SnapshotHandle>, io::Error> {
        let mut inner = self.0.lock().await;
        let handle = inner.build_snapshot_dir()?;
        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: inner.meta.last_applied.clone(),
            last_membership: inner.meta.membership.clone(),
        };
        inner.current_snapshot = Some(handle.clone());
        Ok(SnapshotOf::<TypeConfig, SnapshotHandle> {
            meta,
            snapshot: handle,
        })
    }
}

impl RaftStateMachine<TypeConfig> for SmStore {
    type SnapshotData = SnapshotHandle;
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>), io::Error> {
        let inner = self.0.lock().await;
        Ok((
            inner.meta.last_applied.clone(),
            inner.meta.membership.clone(),
        ))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>>
            + Unpin
            + openraft::OptionalSend,
    {
        let mut inner = self.0.lock().await;
        let mut responders = Vec::new();
        while let Some((entry, responder)) = entries.try_next().await? {
            inner.meta.last_applied = Some(entry.log_id.clone());
            match &entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(req) => {
                    inner.append_payload(req.id, &req.payload)?;
                }
                EntryPayload::Membership(m) => {
                    inner.meta.membership =
                        StoredMembership::new(Some(entry.log_id.clone()), m.clone());
                }
            }
            let count = inner.meta.applied_count;
            if let Some(responder) = responder {
                responders.push((
                    responder,
                    AppResponse {
                        applied_count: count,
                    },
                ));
            }
        }
        // §11.4: apply advances the state; the durable point is periodic
        // unless the mode asks for one per batch.
        inner.durable_point(false)?;
        for (responder, resp) in responders {
            responder.send(resp);
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        let mut inner = self.0.lock().await;
        let (index, term) = match &meta.last_log_id {
            Some(id) => (id.index, crate::util::term_of(id)),
            None => (0, 0),
        };
        // I17: stage into a new versioned directory, verify, fsync, then flip
        // CURRENT. A crash before the rename leaves the old state live.
        let staged = inner.root.join(format!("sm-{index}-{term}"));
        let _ = fs::remove_dir_all(&staged);
        fs::create_dir_all(&staged)?;
        for f in &snapshot.manifest.files {
            let src = snapshot.dir.join(&f.name);
            let dst = staged.join(&f.name);
            if fs::hard_link(&src, &dst).is_err() {
                fs::copy(&src, &dst)?;
            }
            let got = fs::metadata(&dst)?.len();
            if got != f.size {
                return Err(io::Error::other(format!(
                    "install: {} is {got} bytes, manifest says {}",
                    f.name, f.size
                )));
            }
        }
        let mut new_meta = snapshot.manifest.meta.clone();
        new_meta.last_applied = meta.last_log_id.clone();
        new_meta.membership = meta.last_membership.clone();
        write_meta_slot(&staged, &new_meta)?;
        fsync_dir(&staged)?;

        let old_live = inner.live.clone();
        write_atomic(
            &inner.root.join("CURRENT"),
            staged.file_name().unwrap().as_encoded_bytes(),
        )?;
        inner.live = staged;
        inner.meta = new_meta;
        inner.active = None;
        inner.open_active()?;
        inner.durable_point(true)?;
        inner.current_snapshot = Some(snapshot);
        if old_live != inner.live {
            let _ = fs::remove_dir_all(&old_live);
        }
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<TypeConfig, Self::SnapshotData>>, io::Error> {
        let mut inner = self.0.lock().await;
        if inner.current_snapshot.is_none() {
            // Pick the newest complete snapshot directory on disk; one without
            // a MANIFEST is an interrupted build and is dropped (§11.6).
            let mut best: Option<(u64, SnapshotHandle)> = None;
            for e in fs::read_dir(inner.root.join("snapshots"))? {
                let e = e?;
                let path = e.path();
                if !path.is_dir() {
                    continue;
                }
                if !path.join("MANIFEST.json").exists() {
                    let _ = fs::remove_dir_all(&path);
                    continue;
                }
                let handle = SnapshotHandle::read_manifest(&path)?;
                let index = handle
                    .manifest
                    .meta
                    .last_applied
                    .as_ref()
                    .map(|id| id.index)
                    .unwrap_or(0);
                if best.as_ref().map(|(i, _)| index > *i).unwrap_or(true) {
                    best = Some((index, handle));
                }
            }
            inner.current_snapshot = best.map(|(_, h)| h);
        }
        let Some(handle) = inner.current_snapshot.clone() else {
            return Ok(None);
        };
        let meta = SnapshotMeta {
            last_log_id: handle.manifest.meta.last_applied.clone(),
            last_membership: handle.manifest.meta.membership.clone(),
        };
        Ok(Some(SnapshotOf::<TypeConfig, SnapshotHandle> {
            meta,
            snapshot: handle,
        }))
    }
}
