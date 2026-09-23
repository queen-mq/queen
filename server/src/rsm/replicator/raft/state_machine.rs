//! openraft's state machine: our apply thread.
//!
//! openraft decides WHEN an entry is committed; the apply thread (WP-1.4)
//! stays the only mutator of committed state (I1). [`QueenSm::apply`] hands
//! each committed entry to the apply thread in index order — an application
//! entry in its payload-free form, openraft's own entries as [`Entry::noop`] —
//! and returns only once the apply thread has applied the last of them, so
//! openraft's `last_applied` never runs ahead of the store (a linearizable
//! read waits on it).
//!
//! # Snapshots
//!
//! The store's durable checkpoint is the snapshot: the store reopens exactly
//! there and the entries above it replay from the queue logs. Building one is
//! therefore free — a [`Checkpoint`] naming the applied log id. Moving one to
//! another node (and installing it) needs the snapshot transport of the
//! multi-node step and is refused until then.

use std::io;
use std::path::PathBuf;
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{Stream, TryStreamExt};
use openraft::storage::{EntryResponder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend, RaftSnapshotBuilder};
use tokio::sync::oneshot;

use super::types::{
    applied_log_id, rsm_index, term_of, LogId, SnapshotMeta, StoredMembership, TypeConfig,
};
use super::{LogStore, Shared};
use crate::rsm::apply::Committed;
use crate::rsm::entry::Entry;
use crate::rsm::replicator::AppliedAt;
use crate::rsm::store::{Store, TypedReads};

pub type Snapshot = openraft::alias::SnapshotOf<TypeConfig, Checkpoint>;

/// A snapshot: the store's state at `last_log_id`. It carries no bytes; the
/// store and the queue logs on this node ARE the snapshot.
#[derive(Clone, Debug)]
pub struct Checkpoint {
    pub last_log_id: Option<LogId>,
}

const MEMBERSHIP_FILE: &str = "membership.json";

/// The pieces of the state machine that outlive one openraft call.
struct Parts<S: Store> {
    store: Arc<S>,
    membership: Mutex<StoredMembership>,
    membership_path: PathBuf,
    current: Mutex<Option<Snapshot>>,
}

impl<S: Store> Parts<S> {
    fn applied(&self) -> io::Result<Option<LogId>> {
        let (index, term) = self
            .store
            .read(|r| Ok((r.applied_index()?, r.applied_term()?)))
            .map_err(|e| io::Error::other(format!("read the applied index: {e}")))?;
        Ok(applied_log_id(index, term))
    }

    fn build(&self) -> io::Result<Snapshot> {
        let last_log_id = self.applied()?;
        let snap = Snapshot {
            meta: SnapshotMeta {
                last_log_id,
                last_membership: self.membership.lock().expect("membership").clone(),
            },
            snapshot: Checkpoint { last_log_id },
        };
        *self.current.lock().expect("snapshot") = Some(snap.clone());
        Ok(snap)
    }
}

/// openraft's [`RaftStateMachine`] over the apply thread.
pub(crate) struct QueenSm<S: Store> {
    parts: Arc<Parts<S>>,
    apply_tx: SyncSender<Committed>,
    shared: Arc<Shared>,
    log: LogStore,
}

impl<S: Store + 'static> QueenSm<S> {
    pub(crate) fn new(
        store: Arc<S>,
        state_dir: PathBuf,
        apply_tx: SyncSender<Committed>,
        shared: Arc<Shared>,
        log: LogStore,
    ) -> io::Result<QueenSm<S>> {
        let membership_path = state_dir.join(MEMBERSHIP_FILE);
        let membership = match std::fs::read(&membership_path) {
            Ok(b) => serde_json::from_slice(&b).map_err(|e| {
                io::Error::other(format!("raft/{MEMBERSHIP_FILE} does not parse: {e}"))
            })?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => StoredMembership::default(),
            Err(e) => return Err(e),
        };
        Ok(QueenSm {
            parts: Arc::new(Parts {
                store,
                membership: Mutex::new(membership),
                membership_path,
                current: Mutex::new(None),
            }),
            apply_tx,
            shared,
            log,
        })
    }

    /// The membership as of the last applied membership entry, durable before
    /// the entry counts as applied (openraft may report a membership newer than
    /// the store's checkpoint; it re-reads the log from the checkpoint on).
    fn save_membership(&self, m: StoredMembership) -> io::Result<()> {
        let bytes = serde_json::to_vec(&m).map_err(io::Error::other)?;
        let dir = self
            .parts
            .membership_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_default();
        let tmp = dir.join(format!("{MEMBERSHIP_FILE}.tmp"));
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.parts.membership_path)?;
        std::fs::File::open(&dir)?.sync_all()?;
        *self.parts.membership.lock().expect("membership") = m;
        Ok(())
    }

    /// Hand one committed entry to the apply thread, waiting (without blocking
    /// a runtime thread) while its bounded channel is full.
    async fn send(&self, mut c: Committed) -> io::Result<()> {
        loop {
            match self.apply_tx.try_send(c) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(back)) => {
                    c = back;
                    tokio::time::sleep(Duration::from_micros(200)).await;
                }
                Err(TrySendError::Disconnected(_)) => {
                    return Err(io::Error::other("the apply thread has stopped"))
                }
            }
        }
    }
}

impl<S: Store + 'static> RaftStateMachine<TypeConfig> for QueenSm<S> {
    type SnapshotData = Checkpoint;
    type SnapshotBuilder = SnapshotBuilder<S>;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
        Ok((
            self.parts.applied()?,
            self.parts.membership.lock().expect("membership").clone(),
        ))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        let mut waiting: Vec<(
            Option<openraft::storage::ApplyResponder<TypeConfig>>,
            oneshot::Receiver<AppliedAt>,
        )> = Vec::new();
        let mut last: Option<u64> = None;
        while let Some((entry, responder)) = entries.try_next().await? {
            let index = rsm_index(entry.log_id.index);
            let term = term_of(&entry.log_id);
            // Already in the store (it reopened past it): the apply thread would
            // skip it without a word, so answer here instead of waiting on it.
            if index
                <= self
                    .shared
                    .applied_index
                    .load(std::sync::atomic::Ordering::Acquire)
            {
                if let Some(r) = responder {
                    r.send(());
                }
                last = Some(entry.log_id.index);
                continue;
            }
            let e: Entry = match &entry.payload {
                EntryPayload::Normal(app) => (*app.payload_free()?).clone(),
                EntryPayload::Blank => Entry::noop(),
                EntryPayload::Membership(m) => {
                    self.save_membership(StoredMembership::new(Some(entry.log_id), m.clone()))?;
                    Entry::noop()
                }
            };
            // Registered BEFORE the send, so the apply thread's notify always
            // finds it.
            let (tx, rx) = oneshot::channel();
            self.shared
                .waiters
                .lock()
                .expect("waiters")
                .insert(index, tx);
            self.send(Committed {
                index,
                term,
                entry: e,
            })
            .await?;
            waiting.push((responder, rx));
            last = Some(entry.log_id.index);
        }
        for (responder, rx) in waiting {
            rx.await.map_err(|_| {
                io::Error::other("the apply thread stopped before applying an entry")
            })?;
            if let Some(r) = responder {
                r.send(());
            }
        }
        if let Some(l) = last {
            self.log.evict_applied(l);
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            parts: self.parts.clone(),
        }
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta,
        _snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        Err(io::Error::other(
            "installing a snapshot needs the snapshot transport (multi-node), not built yet",
        ))
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, io::Error> {
        Ok(self.parts.current.lock().expect("snapshot").clone())
    }
}

/// Builds a [`Checkpoint`] snapshot: free, see the module header.
pub(crate) struct SnapshotBuilder<S: Store> {
    parts: Arc<Parts<S>>,
}

impl<S: Store + 'static> RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder<S> {
    type SnapshotData = Checkpoint;

    async fn build_snapshot(&mut self) -> Result<Snapshot, io::Error> {
        self.parts.build()
    }
}
