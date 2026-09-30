//! openraft's state machine: our apply thread.
//!
//! openraft decides WHEN an entry is committed; the apply thread (WP-1.4)
//! stays the only mutator of committed state (I1). Committed entries reach it
//! in index order — an application entry in its payload-free form, shared
//! with the log (an `Arc`, never copied), openraft's own entries as
//! [`Entry::noop`] — handed over by whichever of two comes first:
//!
//! - the **feeder** ([`Feeder`], `QUEEN_RAFT_APPLY_AHEAD`, default on): openraft
//!   saves the commit point ([`LogStore`]'s `save_committed`) the moment it
//!   advances, before it queues the apply; that wakes a task which hands the
//!   newly committed entries over at once;
//! - [`QueenSm::apply`] itself, for every entry the feeder has not handed over
//!   yet (the entries openraft re-applies at startup, or all of them with the
//!   feeder off).
//!
//! [`QueenSm::apply`] still returns only once the apply thread has applied the
//! last entry openraft gave it, and answers each entry's responder once THAT
//! entry is applied, so openraft's `last_applied` never runs ahead of the
//! store: the role's I13 check, a linearizable read on the leader (openraft
//! waits on its own `last_applied`), the purge driver and the snapshot all
//! keep reading an honest value. What the feeder removes is the gap: openraft
//! hands the state machine its next committed batch only after the previous
//! `apply` returned, so without it the apply thread sat idle at every batch
//! boundary — the answers, a task wake-up, the next batch's read — while the
//! commit latency absorbed the apply queue (3x16 vCPU at 1M msg/s:
//! corr(commit, apply) 0.82, against 0.18 for the append RPC).
//!
//! # Snapshots
//!
//! The store's durable checkpoint is the snapshot: the store reopens exactly
//! there and the entries above it replay from the queue logs. Building one is
//! therefore free — a [`Checkpoint`] naming the log id of the store's DURABLE
//! index, which is what lets openraft purge the log behind it. Sending one to a
//! follower copies the checkpoint and the queue logs ([`super::snapshot`]);
//! installing one replaces this node's data directory, which cannot be done
//! under a running apply thread: [`QueenSm::install_snapshot`] asks for a
//! restart, and the next boot swaps the received snapshot in.

use std::collections::VecDeque;
use std::io;
use std::path::PathBuf;
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use futures_util::{Stream, TryStreamExt};
use openraft::storage::{ApplyResponder, EntryResponder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend, RaftSnapshotBuilder};
use tokio::sync::{oneshot, watch};

use super::types::{
    applied_log_id, log_id, rsm_index, term_of, LogId, REntry, SnapshotMeta, StoredMembership,
    TypeConfig,
};
use super::{LogStore, Shared};
use crate::rsm::apply::Committed;
use crate::rsm::entry::Entry;
use crate::rsm::replicator::AppliedAt;
use crate::rsm::store::{Store, TypedReads};

pub type Snapshot = openraft::alias::SnapshotOf<TypeConfig, Checkpoint>;

/// A snapshot: the store's state at `last_log_id`. It carries no bytes; the
/// store and the queue logs on this node ARE the snapshot, copied when one is
/// sent.
#[derive(Clone, Debug)]
pub struct Checkpoint {
    pub last_log_id: Option<LogId>,
}

const MEMBERSHIP_FILE: &str = "membership.json";

/// Applied memberships kept (newest last), each change being two entries
/// (joint, then uniform): a snapshot names the membership of its OWN
/// checkpoint, which can trail the latest by a few changes.
const MEMBERSHIP_HISTORY: usize = 64;

/// The membership in force at a log id: the newest applied one at or below
/// it, `None` when the history no longer reaches back that far.
pub(crate) type MembershipAt =
    Box<dyn Fn(&Option<LogId>) -> Option<StoredMembership> + Send + Sync>;

/// `QUEEN_RAFT_APPLY_AHEAD` (default on): hand committed entries to the apply
/// thread the moment openraft commits them ([`Feeder`]). Off: only when
/// openraft calls [`QueenSm::apply`] — the behaviour before the feeder, kept
/// for an A/B.
pub(crate) fn apply_ahead_from_env() -> bool {
    match std::env::var("QUEEN_RAFT_APPLY_AHEAD") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        Err(_) => true,
    }
}

/// The applied memberships, durable in `raft/membership.json` (the latest).
struct Memberships {
    /// The latest applied membership, and the ones before it (newest last):
    /// a snapshot below the latest one names the membership of its own index.
    hist: Mutex<Vec<StoredMembership>>,
    path: PathBuf,
}

impl Memberships {
    fn open(state_dir: &std::path::Path) -> io::Result<Memberships> {
        let path = state_dir.join(MEMBERSHIP_FILE);
        let membership: StoredMembership = match std::fs::read(&path) {
            Ok(b) => serde_json::from_slice(&b).map_err(|e| {
                io::Error::other(format!("raft/{MEMBERSHIP_FILE} does not parse: {e}"))
            })?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => StoredMembership::default(),
            Err(e) => return Err(e),
        };
        Ok(Memberships {
            hist: Mutex::new(vec![membership]),
            path,
        })
    }

    fn latest(&self) -> StoredMembership {
        self.hist
            .lock()
            .expect("membership")
            .last()
            .cloned()
            .unwrap_or_default()
    }

    fn at(&self, at: &Option<LogId>) -> Option<StoredMembership> {
        let hist = self.hist.lock().expect("membership");
        hist.iter().rev().find(|m| m.log_id() <= at).cloned()
    }

    /// The newest at or below `at`, else the oldest kept.
    fn at_or_oldest(&self, at: &Option<LogId>) -> StoredMembership {
        let hist = self.hist.lock().expect("membership");
        hist.iter()
            .rev()
            .find(|m| m.log_id() <= at)
            .cloned()
            .unwrap_or_else(|| hist.first().cloned().unwrap_or_default())
    }

    /// The membership as of the last applied membership entry, durable before
    /// the entry is handed to apply (openraft may report a membership newer
    /// than the store's checkpoint; it re-reads the log from the checkpoint
    /// on).
    fn save(&self, m: StoredMembership) -> io::Result<()> {
        {
            let hist = self.hist.lock().expect("membership");
            if hist.last() == Some(&m) {
                return Ok(());
            }
        }
        let bytes = serde_json::to_vec(&m).map_err(io::Error::other)?;
        let dir = self
            .path
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
        std::fs::rename(&tmp, &self.path)?;
        std::fs::File::open(&dir)?.sync_all()?;
        let mut hist = self.hist.lock().expect("membership");
        hist.push(m);
        if hist.len() > MEMBERSHIP_HISTORY {
            hist.remove(0);
        }
        Ok(())
    }
}

/// The pieces of the state machine that outlive one openraft call.
struct Parts<S: Store> {
    store: Arc<S>,
    members: Arc<Memberships>,
    current: Mutex<Option<Snapshot>>,
    shared: Arc<Shared>,
}

impl<S: Store> Parts<S> {
    fn applied(&self) -> io::Result<Option<LogId>> {
        let (index, term) = self
            .store
            .read(|r| Ok((r.applied_index()?, r.applied_term()?)))
            .map_err(|e| io::Error::other(format!("read the applied index: {e}")))?;
        Ok(applied_log_id(index, term))
    }

    /// A checkpoint at the store's durable index: everything at or below it is
    /// in the store's image on disk, so openraft may purge it.
    fn build(&self) -> io::Result<Snapshot> {
        let durable = self.shared.durable();
        let last_log_id = match durable {
            0 => None,
            d => Some(log_id(
                self.shared.term_at(d).ok_or_else(|| {
                    io::Error::other(format!("the term of durable index {d} is unknown"))
                })?,
                d - 1,
            )),
        };
        let snap = Snapshot {
            meta: SnapshotMeta {
                last_log_id,
                last_membership: self.members.at_or_oldest(&last_log_id),
            },
            snapshot: Checkpoint { last_log_id },
        };
        *self.current.lock().expect("snapshot") = Some(snap.clone());
        Ok(snap)
    }
}

// ---------------------------------------------------------------------------
// The feeder: committed entries to the apply thread, in order, at commit
// ---------------------------------------------------------------------------

/// What the feeder needs from the replicator's shared state. [`Shared`] in the
/// product; a test plays the apply thread's notifier itself.
pub(crate) trait FeedHost: Send + Sync + 'static {
    /// The RSM index the store has applied.
    fn applied_index(&self) -> u64;
    /// Register the waiter the apply thread's notifier answers once `index`
    /// is applied (before the entry is sent, so the notify always finds it).
    fn register(&self, index: u64, tx: oneshot::Sender<AppliedAt>);
    /// Forget a waiter whose entry could not be sent.
    fn unregister(&self, index: u64);
}

impl FeedHost for Shared {
    fn applied_index(&self) -> u64 {
        self.applied_index
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn register(&self, index: u64, tx: oneshot::Sender<AppliedAt>) {
        self.waiters.lock().expect("waiters").insert(index, tx);
    }

    fn unregister(&self, index: u64) {
        self.waiters.lock().expect("waiters").remove(&index);
    }
}

/// At most this many entries are read out of the log per feeder step.
const FEED_CHUNK: u64 = 64;

/// One entry handed to the apply thread and not yet collected by
/// [`QueenSm::apply`].
#[derive(Debug)]
pub(crate) struct Handed {
    /// Its RSM index.
    index: u64,
    applied: oneshot::Receiver<AppliedAt>,
    at: Instant,
}

/// Where the feeder task and [`QueenSm::apply`] meet: one hand-over at a time,
/// strictly in index order, under one async lock.
struct FeedState {
    /// The apply thread's sender — its only one: when the state machine goes,
    /// the apply thread ends.
    tx: SyncSender<Committed>,
    /// The openraft index of the next entry to hand over.
    next: u64,
    /// Entries handed over whose apply [`QueenSm::apply`] has not collected
    /// yet, in index order.
    handed: VecDeque<Handed>,
    /// The apply thread is gone: nothing more can be handed over.
    failed: Option<String>,
}

/// Hands committed entries to the apply thread. See the module header.
pub(crate) struct Feeder {
    state: tokio::sync::Mutex<FeedState>,
    host: Arc<dyn FeedHost>,
    log: LogStore,
    members: Arc<Memberships>,
    /// Entries the feeder task handed over ahead of openraft (a gauge for the
    /// stage line: apply ahead is working when it grows).
    ahead: std::sync::atomic::AtomicU64,
}

impl Feeder {
    fn new(
        tx: SyncSender<Committed>,
        next: u64,
        host: Arc<dyn FeedHost>,
        log: LogStore,
        members: Arc<Memberships>,
    ) -> Feeder {
        Feeder {
            state: tokio::sync::Mutex::new(FeedState {
                tx,
                next,
                handed: VecDeque::new(),
                failed: None,
            }),
            host,
            log,
            members,
            ahead: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Hand every committed entry up to openraft index `upto` over, from the
    /// log's memory. An entry not there (never expected for an unapplied one)
    /// or an error ends the step: [`QueenSm::apply`] hands that entry over
    /// itself when openraft gives it, and meets the same error.
    async fn feed_upto(&self, upto: u64) -> io::Result<()> {
        let mut st = self.state.lock().await;
        while st.failed.is_none() && st.next <= upto {
            let end = upto
                .saturating_add(1)
                .min(st.next.saturating_add(FEED_CHUNK));
            let entries = self.log.cached_range(st.next, end);
            if entries.is_empty() {
                return Ok(());
            }
            for e in &entries {
                if e.log_id.index != st.next {
                    return Ok(());
                }
                self.hand_over(&mut st, e).await?;
                self.ahead
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                AHEAD_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// Hand `e` — the entry at `st.next` — to the apply thread: after this
    /// node's own queue-log write of it (see `log_store::Written`), with its
    /// waiter registered first. An entry the store already holds (it reopened
    /// past it) is not sent: the apply thread would skip it without a word.
    async fn hand_over(&self, st: &mut FeedState, e: &REntry) -> io::Result<()> {
        let i = e.log_id.index;
        debug_assert_eq!(i, st.next);
        let index = rsm_index(i);
        if index <= self.host.applied_index() {
            st.next = i + 1;
            return Ok(());
        }
        let t_commit = Instant::now();
        if let EntryPayload::Normal(app) = &e.payload {
            if let Some(p) = app.proposed_at() {
                STAGES.add(0, t_commit.duration_since(p));
            }
        }
        // Never ahead of this node's own queue logs.
        self.log.wait_written(i).await?;
        let t_written = Instant::now();
        STAGES.add(1, t_written.duration_since(t_commit));
        let entry: Arc<Entry> = match &e.payload {
            // Set by the log writer right after its write: shared, no decode.
            EntryPayload::Normal(app) => app.payload_free()?,
            EntryPayload::Blank => Arc::new(Entry::noop()),
            EntryPayload::Membership(m) => {
                self.members
                    .save(StoredMembership::new(Some(e.log_id), m.clone()))?;
                Arc::new(Entry::noop())
            }
        };
        STAGES.add(2, t_written.elapsed());
        let (tx, rx) = oneshot::channel();
        self.host.register(index, tx);
        let c = Committed {
            index,
            term: term_of(&e.log_id),
            entry,
        };
        if let Err(err) = send(&st.tx, c).await {
            self.host.unregister(index);
            st.failed = Some(err.to_string());
            return Err(err);
        }
        st.handed.push_back(Handed {
            index,
            applied: rx,
            at: Instant::now(),
        });
        st.next = i + 1;
        Ok(())
    }

    /// For [`QueenSm::apply`]: make sure `e` is handed over (in order), and
    /// take what tells when it is applied — `None` when the store already
    /// held it.
    async fn claim(&self, e: &REntry) -> io::Result<Option<Handed>> {
        let mut st = self.state.lock().await;
        let i = e.log_id.index;
        if i >= st.next {
            if let Some(why) = &st.failed {
                return Err(io::Error::other(why.clone()));
            }
            if i != st.next {
                return Err(io::Error::other(format!(
                    "openraft applies entry {i}, but the next entry for the apply thread is {}",
                    st.next
                )));
            }
            self.hand_over(&mut st, e).await?;
        }
        let index = rsm_index(i);
        // Every handed entry is collected in order; one below `index` would
        // mean openraft skipped an entry, which it never does.
        while st.handed.front().is_some_and(|h| h.index < index) {
            let h = st.handed.pop_front().expect("front");
            tracing::warn!(target: "rsm", index = h.index, "raft apply: a handed entry was never collected");
        }
        Ok(match st.handed.front() {
            Some(h) if h.index == index => st.handed.pop_front(),
            _ => None,
        })
    }
}

/// Hand one committed entry to the apply thread, waiting (without blocking a
/// runtime thread) while its bounded channel is full.
async fn send(tx: &SyncSender<Committed>, mut c: Committed) -> io::Result<()> {
    loop {
        match tx.try_send(c) {
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

/// The feeder task: each time openraft saves a new commit point, hand the
/// entries up to it over. Holds the feeder weakly and is aborted with the
/// state machine, so the apply thread's sender goes when openraft drops it.
async fn run_feeder(feeder: Weak<Feeder>, mut committed: watch::Receiver<(u64, u64)>) {
    loop {
        if committed.changed().await.is_err() {
            return;
        }
        let (index, _term) = *committed.borrow_and_update();
        if index == 0 {
            continue;
        }
        let Some(f) = feeder.upgrade() else {
            return;
        };
        if let Err(e) = f.feed_upto(index - 1).await {
            // openraft's apply of the same entry meets it and stops the node.
            tracing::debug!(target: "rsm", error = %e, "raft apply ahead stopped");
        }
    }
}

/// Collect every entry at the front of `waiting` already applied, answering
/// its responder.
fn answer_applied(
    waiting: &mut VecDeque<(Option<ApplyResponder<TypeConfig>>, Option<Handed>)>,
) -> io::Result<()> {
    while let Some((_, handed)) = waiting.front_mut() {
        if let Some(h) = handed {
            match h.applied.try_recv() {
                Ok(_) => STAGES.add(3, h.at.elapsed()),
                Err(oneshot::error::TryRecvError::Empty) => return Ok(()),
                Err(oneshot::error::TryRecvError::Closed) => {
                    return Err(io::Error::other(
                        "the apply thread stopped before applying an entry",
                    ))
                }
            }
        }
        let (responder, _) = waiting.pop_front().expect("front");
        if let Some(r) = responder {
            r.send(());
        }
    }
    Ok(())
}

/// openraft's [`RaftStateMachine`] over the apply thread.
pub(crate) struct QueenSm<S: Store> {
    parts: Arc<Parts<S>>,
    feeder: Arc<Feeder>,
    /// The feeder task, when apply ahead is on: aborted with the state
    /// machine.
    ahead: Option<tokio::task::JoinHandle<()>>,
    shared: Arc<Shared>,
    log: LogStore,
}

impl<S: Store> Drop for QueenSm<S> {
    fn drop(&mut self) {
        if let Some(h) = self.ahead.take() {
            h.abort();
        }
    }
}

impl<S: Store + 'static> QueenSm<S> {
    pub(crate) fn new(
        store: Arc<S>,
        state_dir: PathBuf,
        apply_tx: SyncSender<Committed>,
        shared: Arc<Shared>,
        log: LogStore,
    ) -> io::Result<QueenSm<S>> {
        let members = Arc::new(Memberships::open(&state_dir)?);
        // The next entry for the apply thread: right after the store's applied
        // one (RSM index = openraft index + 1).
        let next = FeedHost::applied_index(&*shared);
        let feeder = Arc::new(Feeder::new(
            apply_tx,
            next,
            shared.clone(),
            log.clone(),
            members.clone(),
        ));
        Ok(QueenSm {
            parts: Arc::new(Parts {
                store,
                members,
                current: Mutex::new(None),
                shared: shared.clone(),
            }),
            feeder,
            ahead: None,
            shared,
            log,
        })
    }

    /// Start the feeder task on openraft's runtime ([`apply_ahead_from_env`]).
    pub(crate) fn start_apply_ahead(&mut self, rt: &tokio::runtime::Handle) {
        if self.ahead.is_some() {
            return;
        }
        let rx = self.log.subscribe_committed();
        self.ahead = Some(rt.spawn(run_feeder(Arc::downgrade(&self.feeder), rx)));
    }

    /// For the snapshot sender ([`super::snapshot::SendCtx`]): it ships the
    /// store's checkpoint as of the SEND, and names that checkpoint's
    /// membership, not the one of the snapshot openraft built earlier.
    pub(crate) fn membership_at(&self) -> MembershipAt {
        let members = self.parts.members.clone();
        Box::new(move |at| members.at(at))
    }
}

impl<S: Store + 'static> RaftStateMachine<TypeConfig> for QueenSm<S> {
    type SnapshotData = Checkpoint;
    type SnapshotBuilder = SnapshotBuilder<S>;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
        Ok((self.parts.applied()?, self.parts.members.latest()))
    }

    /// Returns once every entry of the batch is applied, each responder
    /// answered as soon as its own entry is. The entries are usually handed
    /// over already (the feeder); those that are not are handed over here.
    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        let mut waiting: VecDeque<(Option<ApplyResponder<TypeConfig>>, Option<Handed>)> =
            VecDeque::new();
        let mut last: Option<u64> = None;
        while let Some((entry, responder)) = entries.try_next().await? {
            let handed = self.feeder.claim(&entry).await?;
            waiting.push_back((responder, handed));
            last = Some(entry.log_id.index);
            answer_applied(&mut waiting)?;
        }
        for (responder, handed) in waiting {
            if let Some(h) = handed {
                h.applied.await.map_err(|_| {
                    io::Error::other("the apply thread stopped before applying an entry")
                })?;
                STAGES.add(3, h.at.elapsed());
            }
            if let Some(r) = responder {
                r.send(());
            }
        }
        // Applied here, so no longer needed in memory once every live follower
        // has it too.
        if let Some(l) = last {
            self.log.evict(l, self.shared.replicated());
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            parts: self.parts.clone(),
        }
    }

    /// The received snapshot is already on disk, staged, with its marker
    /// written ([`super::snapshot::receive`]); it replaces the data directory
    /// at the next boot. Stop here: openraft must not apply anything more to
    /// the store this snapshot replaces.
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        _snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        let why = format!(
            "installing the snapshot at {:?}: the node restarts to load it",
            meta.last_log_id
        );
        self.shared.restart.request(why.clone());
        Err(io::Error::other(why))
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, io::Error> {
        let current = self.parts.current.lock().expect("snapshot").clone();
        match current {
            Some(s) => Ok(Some(s)),
            // After a restart: the store's checkpoint is still the snapshot.
            None => self.parts.build().map(Some),
        }
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

/// Per-entry stage timings of the apply path, logged every 10 s (diagnostics):
/// 0 proposed -> committed (handed toward the apply thread), 1 waiting for
/// this node's own queue-log write, 2 taking the payload-free entry, 3 handed
/// to the apply thread -> seen applied, 4 a follower decoding an append, 5 a
/// follower's `append_entries` (log write + flush), 6 the leader's append RPC
/// round trip, 7 this node's log flush (append -> fsync'd group).
struct Stages {
    sum_us: [std::sync::atomic::AtomicU64; 8],
    n: [std::sync::atomic::AtomicU64; 8],
    max_us: [std::sync::atomic::AtomicU64; 8],
    last: std::sync::Mutex<Option<std::time::Instant>>,
}

/// Record one sample of stage `i` (diagnostics; see [`Stages`]).
pub(crate) fn stage_add(i: usize, d: std::time::Duration) {
    STAGES.add(i, d);
}

static STAGES: Stages = Stages {
    sum_us: [const { std::sync::atomic::AtomicU64::new(0) }; 8],
    n: [const { std::sync::atomic::AtomicU64::new(0) }; 8],
    max_us: [const { std::sync::atomic::AtomicU64::new(0) }; 8],
    last: std::sync::Mutex::new(None),
};

/// Entries the feeder task handed to the apply thread ahead of openraft's
/// apply, process-wide (the stage line reports the delta).
static AHEAD_TOTAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl Stages {
    fn add(&self, i: usize, d: std::time::Duration) {
        use std::sync::atomic::Ordering::Relaxed;
        let us = d.as_micros() as u64;
        self.sum_us[i].fetch_add(us, Relaxed);
        self.n[i].fetch_add(1, Relaxed);
        self.max_us[i].fetch_max(us, Relaxed);
        if i == 3 {
            self.maybe_log();
        }
    }

    fn maybe_log(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let mut last = self.last.lock().unwrap_or_else(|p| p.into_inner());
        let now = std::time::Instant::now();
        if last.is_some_and(|t| now.duration_since(t) < std::time::Duration::from_secs(10)) {
            return;
        }
        *last = Some(now);
        let mut avg = [0u64; 8];
        let mut max = [0u64; 8];
        let mut cnt = [0u64; 8];
        for i in 0..8 {
            cnt[i] = self.n[i].load(Relaxed);
            let n = self.n[i].swap(0, Relaxed).max(1);
            avg[i] = self.sum_us[i].swap(0, Relaxed) / n;
            max[i] = self.max_us[i].swap(0, Relaxed);
        }
        tracing::info!(
            target: "rsm",
            commit_us = avg[0],
            commit_max_us = max[0],
            written_us = avg[1],
            written_max_us = max[1],
            clone_us = avg[2],
            clone_max_us = max[2],
            apply_us = avg[3],
            apply_max_us = max[3],
            applied = cnt[3],
            ahead = AHEAD_TOTAL.swap(0, Relaxed),
            fdecode_us = avg[4],
            fappend_us = avg[5],
            fappend_max_us = max[5],
            fappends = cnt[5],
            rpc_us = avg[6],
            rpc_max_us = max[6],
            rpcs = cnt[6],
            flush_us = avg[7],
            flush_max_us = max[7],
            "apply path stages (means since the last line)"
        );
    }
}

#[cfg(test)]
mod tests {
    //! The feeder on a real log store, with the test playing the apply thread.

    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc::Receiver;

    use openraft::storage::{IOFlushed, RaftLogStorage};
    use openraft::type_config::TypeConfigExt;

    use super::super::log_store::OpenCfg;
    use super::super::types::{Membership, QueenNode};
    use super::*;
    use crate::rsm::qlog::QLogOptions;

    static SEQ: AtomicU64 = AtomicU64::new(0);

    /// `f`, or a panic naming `what` after 20 s: no test here may hang.
    async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(20), f)
            .await
            .unwrap_or_else(|_| panic!("{what}: nothing within 20 s"))
    }

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "queen-raft-feed-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    /// The replicator's shared state as the feeder sees it, with the waiters
    /// the test's apply thread answers.
    #[derive(Default)]
    struct Host {
        applied: AtomicU64,
        waiters: Mutex<HashMap<u64, oneshot::Sender<AppliedAt>>>,
    }

    impl FeedHost for Host {
        fn applied_index(&self) -> u64 {
            self.applied.load(Ordering::Acquire)
        }
        fn register(&self, index: u64, tx: oneshot::Sender<AppliedAt>) {
            self.waiters.lock().unwrap().insert(index, tx);
        }
        fn unregister(&self, index: u64) {
            self.waiters.lock().unwrap().remove(&index);
        }
    }

    fn open_log(dir: &Path) -> LogStore {
        let opened = LogStore::open(OpenCfg {
            qlog_root: dir.join("qlog"),
            qopts: QLogOptions::testing(1 << 20),
            state_dir: dir.join("raft"),
            lookup: Arc::new(|_| Ok(None)),
            durable_index: 0,
            applied: None,
            qlog_durable_index: 0,
            qlog_tail: Box::new(|_| Ok(None)),
            poison: Arc::new(Mutex::new(None)),
            cache_cap: 1 << 30,
        })
        .expect("open the log");
        // Detached: the writer exits once every clone of the store is gone.
        drop(opened.writer);
        opened.store
    }

    /// Append `entries` and wait for their write (and flush).
    async fn append(log: &LogStore, entries: Vec<REntry>) {
        let (tx, rx) = TypeConfig::oneshot();
        let mut l = log.clone();
        within("append", l.append(entries, IOFlushed::signal(tx)))
            .await
            .expect("append");
        within("flush", rx)
            .await
            .expect("flush answered")
            .expect("flushed");
    }

    fn blanks(term: u64, from: u64, n: u64) -> Vec<REntry> {
        (from..from + n)
            .map(|i| REntry {
                log_id: log_id(term, i),
                payload: EntryPayload::Blank,
            })
            .collect()
    }

    fn feeder(
        dir: &Path,
        log: &LogStore,
        next: u64,
        host: Arc<Host>,
        cap: usize,
    ) -> (Arc<Feeder>, Receiver<Committed>) {
        let (tx, rx) = std::sync::mpsc::sync_channel(cap);
        let members = Arc::new(Memberships::open(&dir.join("raft")).expect("memberships"));
        let f = Arc::new(Feeder::new(tx, next, host, log.clone(), members));
        (f, rx)
    }

    /// What the apply thread does with one entry, as far as the feeder sees
    /// it: the applied index moves and the entry's waiter is answered.
    fn apply_one(host: &Host, c: &Committed) {
        assert!(
            c.entry.is_noop(),
            "blank and membership entries apply as no-ops"
        );
        host.applied.fetch_max(c.index, Ordering::AcqRel);
        if let Some(tx) = host.waiters.lock().unwrap().remove(&c.index) {
            let _ = tx.send(AppliedAt {
                index: c.index,
                term: c.term,
            });
        }
    }

    /// Plays the apply thread: applies what arrives, in order; returns the
    /// indexes it saw.
    fn apply_thread(rx: Receiver<Committed>, host: Arc<Host>) -> std::thread::JoinHandle<Vec<u64>> {
        std::thread::spawn(move || {
            let mut seen = Vec::new();
            while let Ok(c) = rx.recv() {
                apply_one(&host, &c);
                seen.push(c.index);
            }
            seen
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn committed_entries_reach_the_apply_thread_before_openraft_asks() {
        let dir = scratch("ahead");
        let log = open_log(&dir);
        let host = Arc::new(Host::default());
        let (f, rx) = feeder(&dir, &log, 0, host.clone(), 64);
        let task = tokio::spawn(run_feeder(Arc::downgrade(&f), log.subscribe_committed()));
        append(&log, blanks(1, 0, 10)).await;

        // Nothing is committed: nothing is handed over.
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());

        // openraft commits the first seven: they go at once, in order, with no
        // apply call.
        log.clone()
            .save_committed(Some(log_id(1, 6)))
            .await
            .expect("save committed");
        for want in 1..=7u64 {
            let c = rx
                .recv_timeout(Duration::from_secs(5))
                .expect("a committed entry is handed over");
            assert_eq!((c.index, c.term), (want, 1));
            // The test is the apply thread for these.
            apply_one(&host, &c);
        }
        assert!(
            rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "never past the commit point"
        );
        assert_eq!(f.ahead.load(Ordering::Relaxed), 7);

        // openraft's apply then collects them — and hands over the rest itself.
        let applier = apply_thread(rx, host.clone());
        let entries = blanks(1, 0, 10);
        for e in &entries {
            let h = within("claim", f.claim(e))
                .await
                .expect("claim")
                .expect("handed over");
            assert_eq!(h.index, rsm_index(e.log_id.index));
            within("apply", h.applied).await.expect("applied");
        }
        assert_eq!(f.ahead.load(Ordering::Relaxed), 7, "8..=10 came from apply");
        task.abort();
        let _ = within("feeder task", task).await;
        drop(f);
        assert_eq!(applier.join().unwrap(), (8..=10).collect::<Vec<_>>());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn entries_the_store_already_holds_are_never_handed_over() {
        let dir = scratch("held");
        let log = open_log(&dir);
        let host = Arc::new(Host::default());
        host.applied.store(3, Ordering::Release);
        append(&log, blanks(1, 0, 5)).await;
        // A feeder that starts below the store's applied index (the store
        // reopened past what openraft re-applies) skips what it holds.
        let (f, rx) = feeder(&dir, &log, 0, host.clone(), 64);
        let entries = blanks(1, 0, 5);
        for e in &entries[..3] {
            assert!(within("claim", f.claim(e)).await.expect("claim").is_none());
        }
        assert!(rx.try_recv().is_err(), "nothing sent for entries 1..=3");
        let applier = apply_thread(rx, host.clone());
        for e in &entries[3..] {
            let h = within("claim", f.claim(e))
                .await
                .expect("claim")
                .expect("handed over");
            within("apply", h.applied).await.expect("applied");
        }
        drop(f);
        assert_eq!(applier.join().unwrap(), vec![4, 5]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_apply_out_of_order_is_refused() {
        let dir = scratch("order");
        let log = open_log(&dir);
        let host = Arc::new(Host::default());
        append(&log, blanks(1, 0, 3)).await;
        let (f, _rx) = feeder(&dir, &log, 0, host, 64);
        let err = within("claim", f.claim(&blanks(1, 2, 1)[0]))
            .await
            .expect_err("entry 2 before entry 0");
        assert!(err.to_string().contains("next entry"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stopped_apply_thread_fails_every_later_hand_over() {
        let dir = scratch("stopped");
        let log = open_log(&dir);
        let host = Arc::new(Host::default());
        append(&log, blanks(1, 0, 2)).await;
        let (f, rx) = feeder(&dir, &log, 0, host.clone(), 64);
        drop(rx);
        let entries = blanks(1, 0, 2);
        let err = within("claim", f.claim(&entries[0]))
            .await
            .expect_err("no apply thread");
        assert!(err.to_string().contains("apply thread"), "{err}");
        assert!(
            host.waiters.lock().unwrap().is_empty(),
            "the waiter of an entry never sent is forgotten"
        );
        assert!(within("claim", f.claim(&entries[0])).await.is_err());
        assert!(within("claim", f.claim(&entries[1])).await.is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_membership_entry_is_saved_before_it_is_handed_over() {
        let dir = scratch("membership");
        let log = open_log(&dir);
        let host = Arc::new(Host::default());
        let m = Membership::new(
            vec![std::collections::BTreeSet::from([1u64])],
            std::collections::BTreeMap::from([(1u64, QueenNode::new("a:1", "a:2"))]),
        )
        .expect("a membership");
        let e = REntry {
            log_id: log_id(1, 0),
            payload: EntryPayload::Membership(m),
        };
        append(&log, vec![e.clone()]).await;
        let (f, rx) = feeder(&dir, &log, 0, host.clone(), 64);
        let applier = apply_thread(rx, host.clone());
        let h = within("claim", f.claim(&e))
            .await
            .expect("claim")
            .expect("handed over");
        let saved: StoredMembership = serde_json::from_slice(
            &std::fs::read(dir.join("raft").join(MEMBERSHIP_FILE)).expect("membership file"),
        )
        .expect("parses");
        assert_eq!(saved.log_id(), &Some(log_id(1, 0)));
        within("apply", h.applied).await.expect("applied");
        drop(f);
        assert_eq!(applier.join().unwrap(), vec![1]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_commit_point_only_moves_forward() {
        let dir = scratch("commit");
        let log = open_log(&dir);
        let rx = log.subscribe_committed();
        assert_eq!(*rx.borrow(), (0, 0));
        let mut l = log.clone();
        l.save_committed(Some(log_id(1, 5))).await.unwrap();
        assert_eq!(*rx.borrow(), (6, 1));
        l.save_committed(Some(log_id(1, 3))).await.unwrap();
        assert_eq!(*rx.borrow(), (6, 1), "never backwards");
        l.save_committed(Some(log_id(2, 7))).await.unwrap();
        assert_eq!(*rx.borrow(), (8, 2));
        drop((l, log));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
