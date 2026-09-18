//! Raft log storage on `raft-log` 0.4.6.
//!
//! Adapted from openraft's own `examples/log-wal` (same commit as the pinned
//! openraft, MIT OR Apache-2.0) with the generic parameter pinned to our
//! `TypeConfig`. It is kept here rather than depended on because the example is
//! not published to crates.io.
//!
//! The contract PLAN_RAFT.md §12.3 lists is honored as follows:
//!   - `append` asks `raft_log` for a flush and hands the callback the result,
//!     so openraft counts the entries only after fsync;
//!   - `save_vote` awaits the fsync before returning;
//!   - `truncate_after` is exclusive, `purge` inclusive;
//!   - `get_log_state` reports the last purged id on an empty log;
//!   - `save_committed`/`read_committed` are implemented, and how durable
//!     `save_committed` is now a knob: [`CommittedDurability`]. openraft's own
//!     `examples/log-wal` does not flush it at all, which is a DIVERGENCE from
//!     §12.3's "`save_committed` persisted" — see `MEMO.md`, "Refutations".
//!
//! What `cargo test` proves and what it does not: openraft's
//! `testing::log::Suite` is an in-process API suite. `run_test`
//! (openraft/src/testing/log/suite.rs:1794-1805 in the pinned tree) builds ONE
//! store per case and never reopens it, `save_vote`
//! (suite.rs:966-973) reads the vote back from the same live instance, and
//! `get_initial_state_re_apply_committed` (suite.rs:929-963) calls
//! `read_committed()` on that same instance (skipping itself if it is None).
//! Nothing in the suite closes a store, crashes a process or drops a write, so
//! it cannot see the three durability clauses of §12.3. The test below runs it
//! against all three `CommittedDurability` settings on purpose: it passes for
//! every one of them, including the one that never flushes.

use std::fmt::Debug;
use std::fs;
use std::io;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogStorage;
use openraft::LogIdOptionExt;
use openraft::LogState;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftTypeConfig;
use raft_log::api::raft_log_writer::RaftLogWriter;
use raft_log::RaftLog;
use tokio::sync::oneshot;
use tokio::sync::RwLock;

mod codec;

pub use codec::MsgPack;
pub use codec::MsgPackVote;

/// What `save_committed` does with the commit record.
///
/// PLAN_RAFT.md §12.3 lists "`save_committed` persisted" among the storage
/// contract's clauses; openraft's `examples/log-wal`, from which this adapter
/// comes, writes the record and returns without any flush. The three settings
/// are what the spike measured (RESULTS-vm.md §8):
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CommittedDurability {
    /// The upstream example: `commit()` and return. The record reaches the
    /// flush worker only with the next `append`, so a `kill -9` in between
    /// loses it and the state machine recovers to its own durable point.
    #[default]
    None,
    /// `flush(false, _)`: the record is handed to the worker (page cache) but
    /// no fsync is paid. raft-log promotes it to a sync write as soon as it
    /// batches with any `sync = true` flush (raft-log 0.4.6
    /// api/raft_log_writer.rs:115-131). Survives a process crash, not a power
    /// cut.
    Buffered,
    /// `flush(true, _)`, awaited: the literal reading of §12.3.
    Fsync,
}

impl std::str::FromStr for CommittedDurability {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "none" => Ok(CommittedDurability::None),
            "buffered" => Ok(CommittedDurability::Buffered),
            "fsync" => Ok(CommittedDurability::Fsync),
            other => Err(format!("unknown save_committed durability {other}")),
        }
    }
}

impl std::fmt::Display for CommittedDurability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            CommittedDurability::None => "none",
            CommittedDurability::Buffered => "buffered",
            CommittedDurability::Fsync => "fsync",
        };
        f.write_str(s)
    }
}

/// The callback `raft_log` runs when a flush finishes.
pub enum Callback<C>
where
    C: RaftTypeConfig,
{
    IoFlushed(IOFlushed<C>),
    Oneshot(oneshot::Sender<Result<(), io::Error>>),
}

impl<C> raft_log::Callback for Callback<C>
where
    C: RaftTypeConfig,
{
    fn send(self, res: Result<(), io::Error>) {
        match self {
            Self::IoFlushed(io_flushed) => io_flushed.io_completed(res),
            Self::Oneshot(tx) => {
                if tx.send(res).is_err() {
                    tracing::warn!("logstore: nobody is waiting for this flush result any more");
                }
            }
        }
    }
}

/// Binds openraft's types to `raft_log`'s type slots.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WalTypes<C>(std::marker::PhantomData<C>);

impl<C> raft_log::Types for WalTypes<C>
where
    C: RaftTypeConfig,
    EntryOf<C>: Clone,
{
    type LogId = MsgPack<LogIdOf<C>>;
    type LogPayload = MsgPack<EntryOf<C>>;
    type Vote = MsgPackVote<C>;
    type Callback = Callback<C>;
    type UserData = MsgPack<()>;

    fn log_index(log_id: &Self::LogId) -> u64 {
        log_id.0.index
    }

    fn payload_size(payload: &Self::LogPayload) -> u64 {
        payload.encoded_len()
    }
}

/// A `raft_log`-backed [`RaftLogStorage`].
#[derive(Debug, Clone)]
pub struct WalLogStore<C>
where
    C: RaftTypeConfig,
    EntryOf<C>: Clone,
{
    inner: Arc<RwLock<RaftLog<WalTypes<C>>>>,
    committed: CommittedDurability,
}

impl<C> WalLogStore<C>
where
    C: RaftTypeConfig,
    EntryOf<C>: Clone,
{
    pub fn open(dir: impl ToString, committed: CommittedDurability) -> Result<Self, io::Error> {
        let config = raft_log::Config::new(dir);
        // raft_log creates its lock file in the directory but not the
        // directory itself.
        fs::create_dir_all(&config.wal.dir)?;
        let raft_log = RaftLog::open(Arc::new(config))?;
        Ok(Self {
            inner: Arc::new(RwLock::new(raft_log)),
            committed,
        })
    }
}

impl<C> RaftLogReader<C> for WalLogStore<C>
where
    C: RaftTypeConfig,
    EntryOf<C>: Clone,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<C>>, io::Error> {
        let (start, end) = range_boundary(range);
        let log = self.inner.read().await;
        let entries = log
            .read(start, end)
            .map(|res| res.map(|(_log_id, payload)| payload.0))
            .collect::<Result<Vec<_>, io::Error>>()?;
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
        let log = self.inner.read().await;
        Ok(log.log_state().vote().map(|vote| vote.0.clone()))
    }
}

impl<C> RaftLogStorage<C> for WalLogStore<C>
where
    C: RaftTypeConfig,
    EntryOf<C>: Clone,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        let log = self.inner.read().await;
        let state = log.log_state();
        Ok(LogState {
            last_purged_log_id: state.purged().map(|log_id| log_id.0.clone()),
            last_log_id: state.last().map(|log_id| log_id.0.clone()),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), io::Error> {
        let (tx, rx) = oneshot::channel();
        {
            let mut log = self.inner.write().await;
            log.save_vote(MsgPackVote(vote.clone()))?;
            log.flush(true, Some(Callback::Oneshot(tx)))?;
        }
        // A vote decides an election: it must be on disk before we return.
        rx.await.map_err(io::Error::other)??;
        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        let Some(committed) = committed else {
            return Ok(());
        };
        match self.committed {
            CommittedDurability::None => {
                // openraft's example: the record reaches the worker only with
                // the next append. A `kill -9` in between loses it.
                let mut log = self.inner.write().await;
                log.commit(MsgPack(committed))?;
            }
            CommittedDurability::Buffered => {
                let mut log = self.inner.write().await;
                log.commit(MsgPack(committed))?;
                log.flush(false, None)?;
            }
            CommittedDurability::Fsync => {
                let (tx, rx) = oneshot::channel();
                {
                    let mut log = self.inner.write().await;
                    log.commit(MsgPack(committed))?;
                    log.flush(true, Some(Callback::Oneshot(tx)))?;
                }
                rx.await.map_err(io::Error::other)??;
            }
        }
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, io::Error> {
        let log = self.inner.read().await;
        Ok(log.log_state().committed().map(|log_id| log_id.0.clone()))
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<C>> + OptionalSend,
    {
        let entries = entries.into_iter().map(|entry| {
            let log_id = entry.log_id();
            (MsgPack(log_id), MsgPack(entry))
        });
        let mut log = self.inner.write().await;
        log.append(entries)?;
        log.flush(true, Some(Callback::IoFlushed(callback)))?;
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        let truncate_at = last_log_id.next_index();
        let mut log = self.inner.write().await;
        let curr_last = log.log_state().last().map(|log_id| log_id.0.clone());
        if truncate_at >= curr_last.next_index() {
            return Ok(());
        }
        log.truncate(truncate_at)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
        let mut log = self.inner.write().await;
        let curr_purged = log.log_state().purged().map(|log_id| log_id.0.clone());
        if log_id.index < curr_purged.next_index() {
            return Ok(());
        }
        log.purge(MsgPack(log_id))?;
        // raft_log unlinks a purged chunk only after the purge record is on
        // disk. The flush is queued, not awaited.
        log.flush(true, None)?;
        Ok(())
    }
}

fn range_boundary<RB: RangeBounds<u64>>(range: RB) -> (u64, u64) {
    let start = match range.start_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n + 1,
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(&n) => n + 1,
        Bound::Excluded(&n) => n,
        Bound::Unbounded => u64::MAX,
    };
    (start, end)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use openraft::testing::log::StoreBuilder;
    use openraft::testing::log::Suite;
    use openraft::type_config::TypeConfigExt;
    use openraft::StorageError;
    use openraft_memstore::BlockConfig;
    use openraft_memstore::MemStateMachine;
    use openraft_memstore::TypeConfig;
    use tempfile::TempDir;

    use super::CommittedDurability;
    use super::WalLogStore;

    struct WalStoreBuilder {
        committed: CommittedDurability,
    }

    impl StoreBuilder<TypeConfig, WalLogStore<TypeConfig>, Arc<MemStateMachine>, TempDir>
        for WalStoreBuilder
    {
        async fn build(
            &self,
        ) -> Result<
            (TempDir, WalLogStore<TypeConfig>, Arc<MemStateMachine>),
            StorageError<TypeConfig>,
        > {
            let temp_dir =
                TempDir::new().map_err(|e| StorageError::write(TypeConfig::err_from_error(&e)))?;
            let dir = temp_dir.path().display().to_string();
            let log_store = WalLogStore::open(dir, self.committed)
                .map_err(|e| StorageError::write(TypeConfig::err_from_error(&e)))?;
            let sm = Arc::new(MemStateMachine::new(BlockConfig::default()));
            Ok((temp_dir, log_store, sm))
        }
    }

    /// openraft's own log-store suite (PLAN_RAFT.md §12.3 cites it).
    ///
    /// It is run against all three `save_committed` settings, INCLUDING the one
    /// that never flushes, because that is the point: the suite passes either
    /// way, so passing it says nothing about the durability clauses of §12.3.
    #[test]
    fn wal_log_store_passes_openraft_suite() {
        for committed in [
            CommittedDurability::None,
            CommittedDurability::Buffered,
            CommittedDurability::Fsync,
        ] {
            TypeConfig::run(async move {
                Suite::test_all(WalStoreBuilder { committed })
                    .await
                    .unwrap();
            });
            println!("openraft testing::log::Suite passed with save_committed={committed}");
        }
    }
}
