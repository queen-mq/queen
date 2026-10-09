//! The consumption engine: the ONE writer of every consumer group's cursors and
//! leases. Pops, acks, nacks, renews, DLQ heads, positional acks, seeks,
//! positions and the consumption half of a transaction are served here, on the
//! leader, from memory; the log carries pushes, the catalog and the engine's
//! CHECKPOINTS (whole cursor rows with their leases, DLQ rows, the group
//! registrations). Design contract: `scratchpad/consume/DESIGN.md`
//! (2026-09-30).
//!
//! # Shape
//!
//! * One [`Engine`] per facade ([`Engine::new`]); it serves only while its
//!   node leads and the replicator confirmed a quorum recently
//!   ([`Engine::set_leader_lease`]). Everything else answers `Retry`.
//! * State ([`state`]): the groups the engine holds, their partitions ("parts")
//!   sharded by `pid % 16` behind one mutex each, per group and shard a ready
//!   list (oldest ready first), per shard a timer heap (lease expiries, delay
//!   and window holds) and a per-worker lease index. Loaded LAZILY from the
//!   committed store rows: a group's first contact reads its cursor rows,
//!   partition heads, queue config and group row ([`load`]).
//! * Semantics ([`pop`], [`ack`], [`txn`]): the planner's 004/005 port, moved
//!   here verbatim in behaviour — the budget across a pop, the width, the
//!   subscription seeding, conflation, delayed processing and the window
//!   buffer, lease time, auto-ack, delivery attempts, the ack by transaction
//!   hash inside the leased batch with the O16 fast path, partial acks, the
//!   retry budget, the DLQ, positional acks, nacks, renews, the DLQ head,
//!   positions.
//! * Durability ([`checkpoint`]): a change marks its part dirty; every
//!   `QUEEN_CONSUME_CHECKPOINT_MS` the ticker takes a checkpoint (one
//!   `Effects` command per (tenant, lane) of whole `CursorSet` rows and their
//!   `DlqInsert`s, one per tenant for the catalog) and an answer is released
//!   only once every row it depends on committed ([`Engine::checkpoint_resolved`]);
//!   `QUEEN_CONSUME_FAST=1` answers at once.
//! * Long polls ([`wait`]): a `wait` pop that finds nothing is HELD here until a
//!   partition of its group becomes claimable or its deadline (minus the reply
//!   margin) comes; the engine's serve thread answers it.
//! * Apply hooks ([`hooks`]): every node's apply reports appends and catalog
//!   effects; only the leader's engine holds anything to update.
//! * Memory ([`unload`]): a whole-queue group's part that holds nothing (no
//!   lease, nothing to deliver, everything durable) and stayed so for
//!   `QUEEN_CONSUME_IDLE_UNLOAD_S` is dropped; its partition keeps the group as
//!   a watcher, and the next append there loads the part back from its cursor
//!   row, the way a new leader loads it.

mod ack;
mod checkpoint;
mod frames;
mod hooks;
mod load;
mod pop;
mod state;
mod txn;
mod unload;
mod wait;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_ack;
#[cfg(test)]
mod tests_pop;
#[cfg(test)]
mod tests_unload;

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::rsm::batcher::{Command, Reply};
use crate::rsm::dedup::IndexMode;
use crate::rsm::effect::Effect;
use crate::rsm::entry::{AckOutcome, Outcome, PopOutcome, RequestId};
use crate::rsm::planner::txn::TxnCommand;
use crate::rsm::planner::{EffectsCommand, Refusal};
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::segments;
use crate::rsm::store::HeedStore;

use state::{lock, read, write, Answers, Held, Registry, Shard, Tickets, SHARDS};

/// What the engine did with a command.
pub enum Served {
    /// Answered now.
    Now(Reply),
    /// Answered later: a long-poll pop held until data or its deadline, or a
    /// claim/ack whose checkpoint must commit first.
    Later(oneshot::Receiver<Reply>),
    /// Not a consumption command: the planner's.
    NotMine,
}

/// One checkpoint: the commands that carry it, and the ticket its answers wait
/// on ([`Engine::checkpoint_resolved`]).
pub struct Checkpoint {
    pub ticket: u64,
    pub commands: Vec<EffectsCommand>,
}

/// The consumption half of a transaction: the effects its entry must carry
/// (whole cursor rows after the acks, DLQ rows, a first position's group
/// registration) and the per-target results.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TxnPart {
    pub effects: Vec<Effect>,
    pub acks: AckOutcome,
    /// Positions on partition names that do not exist yet: the planner
    /// creates those partitions (their pids are its to allocate) and writes
    /// their cursor rows in the same entry; the engine learns them when the
    /// entry applies.
    pub planner_positions: Vec<crate::rsm::planner::positions::PositionOp>,
}

/// The first bytes of every checkpoint command's request id. A request id
/// the receiver mints is a uuidv7 (these bytes are its millisecond clock, in
/// the 2020s near `0x0199`): no client command carries this prefix.
const CHECKPOINT_MARK: [u8; 4] = [0xFF, 0x51, 0x43, 0x4B];

/// A request id for one checkpoint command.
pub(crate) fn checkpoint_id() -> RequestId {
    let mut id = crate::util::uuidv7_bytes();
    id[..4].copy_from_slice(&CHECKPOINT_MARK);
    id
}

/// Whether a command's request id is a consumption engine checkpoint's. The
/// batcher plans one only while the engine that took it still has it in
/// flight ([`Engine::answers_at_commit`]): a checkpoint of an incarnation that
/// is gone — its node stopped leading, then led again before its queue was
/// answered — carries that engine's whole cursor rows, older than what a
/// leader in between may have written (acks undone, leases revived).
pub fn is_checkpoint_id(id: &RequestId) -> bool {
    id[..4] == CHECKPOINT_MARK
}

/// The refusal code of an answer in doubt ([`in_doubt`]).
pub const IN_DOUBT: &str = "in_doubt";

/// The answer to a command whose change may or may not be in the log when
/// this node stops leading: a checkpoint carrying it was on its way. Not
/// retryable — the next leader would run the command again on a state that
/// may already hold it (an ack would find its lease released and call its
/// messages stale); the caller learns the outcome is unknown (the facade's
/// `503 outcome_unknown`), as it would from a lost reply.
pub fn in_doubt() -> Refusal {
    Refusal::client(
        IN_DOUBT,
        "the leader changed while this change was being made durable: it may or may not have \
         applied",
    )
}

/// The leader lease probe: the age of the last quorum ack (`None` when this
/// node does not lead; `ZERO` for a single voter).
pub type LeaderLease = Arc<dyn Fn() -> Option<Duration> + Send + Sync>;

/// Wakes the pops a facade parked itself on `(tenant, queue, group)`.
pub type Waker = Arc<dyn Fn(&str, &str, &str) + Send + Sync>;

/// The knobs, read once when the engine is built.
#[derive(Clone, Debug)]
pub(crate) struct Knobs {
    /// `QUEEN_LANES` (default 8): a checkpoint is one command per (tenant,
    /// lane), `lane = pid % lanes`, so the lanes plan it in parallel.
    pub lanes: u64,
    /// `QUEEN_CONSUME_CHECKPOINT_MS` (default 5).
    pub ckpt_ms: u64,
    /// `QUEEN_CONSUME_FAST` (default off): answer before the checkpoint.
    pub fast: bool,
    /// `QUEEN_CONSUME_LEADER_LEASE_MS` (default 400).
    pub lease_ms: u64,
    /// `QUEEN_RAFT_MAX_CLOCK_SKEW_MS` (default 500), in µs: a lease another
    /// node's clock timed is held this much past its expiry, and a new
    /// leader serves nothing for the checkpoint interval plus this.
    pub skew_us: i64,
    /// `QUEEN_RAFT_POP_REPLY_MARGIN_MS` (default 50), in µs: a pop whose
    /// deadline is closer than this is answered empty, never claimed for.
    pub margin_us: i64,
    /// `QUEEN_CONSUME_ROWS_PER_COMMAND` (default 4096): the most cursor rows
    /// one checkpoint command carries.
    pub rows_per_cmd: usize,
    /// `QUEEN_CONSUME_TXN_TTL_MS` (default 30000), in µs: a transaction
    /// reservation nobody resolves is released after this.
    pub txn_ttl_us: i64,
    /// `QUEEN_CONSUME_IDLE_UNLOAD_S` (default 600; `0` keeps every part), in
    /// µs: how long a whole-queue group's part must have held nothing before
    /// it is dropped from memory ([`unload`]).
    pub idle_unload_us: i64,
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_flag(name: &str) -> bool {
    matches!(
        std::env::var(name)
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1" | "true" | "on" | "yes")
    )
}

impl Knobs {
    pub fn from_env() -> Knobs {
        Knobs {
            lanes: env_u64("QUEEN_LANES", 8).max(1),
            ckpt_ms: env_u64("QUEEN_CONSUME_CHECKPOINT_MS", 5).max(1),
            fast: env_flag("QUEEN_CONSUME_FAST"),
            lease_ms: env_u64("QUEEN_CONSUME_LEADER_LEASE_MS", 400),
            skew_us: std::env::var("QUEEN_RAFT_MAX_CLOCK_SKEW_MS")
                .ok()
                .and_then(|v| v.trim().parse::<i64>().ok())
                .map_or(500_000, |ms| ms.max(0) * 1000),
            margin_us: std::env::var("QUEEN_RAFT_POP_REPLY_MARGIN_MS")
                .ok()
                .and_then(|v| v.trim().parse::<i64>().ok())
                .unwrap_or(50)
                .max(0)
                * 1000,
            rows_per_cmd: env_u64("QUEEN_CONSUME_ROWS_PER_COMMAND", 4096).max(1) as usize,
            txn_ttl_us: env_u64("QUEEN_CONSUME_TXN_TTL_MS", 30_000) as i64 * 1000,
            idle_unload_us: env_u64("QUEEN_CONSUME_IDLE_UNLOAD_S", 600) as i64 * SEC_US,
        }
    }
}

/// The wall clock in µs.
pub(crate) fn wall_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as i64)
}

/// This process's monotonic clock in µs (from an arbitrary origin).
fn mono_us() -> i64 {
    static ORIGIN: std::sync::LazyLock<std::time::Instant> =
        std::sync::LazyLock::new(std::time::Instant::now);
    ORIGIN.elapsed().as_micros().min(i64::MAX as u128) as i64
}

pub(crate) const SEC_US: i64 = 1_000_000;

/// Appends a busy shard queued for the serve thread: `(pid, last offset)`.
pub(crate) type QueuedAppends = Vec<(crate::rsm::effect::Pid, i64)>;

/// The engine. One per facade; it serves only while this node leads.
pub struct Engine {
    me: Weak<Engine>,
    pub(crate) store: Arc<HeedStore>,
    pub(crate) k: Knobs,
    lease: RwLock<Option<LeaderLease>>,
    waker: RwLock<Option<Waker>>,
    seg: RwLock<Option<frames::SegSource>>,
    /// Test override of the dedup authority (`None`: the process's).
    mode_override: RwLock<Option<IndexMode>>,
    pub(crate) leader: AtomicBool,
    /// This node leads a STANDBY cluster ([`crate::rsm::link`]): it takes no
    /// client command, and this engine is not told it leads until the cluster
    /// is promoted. Set by the batcher, read by the leader's intake.
    standby: AtomicBool,
    /// Bumped at every leadership change: work begun under an older one is
    /// dropped.
    pub(crate) gen: AtomicU64,
    /// Serve nothing before this instant (a new leader's pause).
    pub(crate) serve_after_us: AtomicI64,
    /// Take no new command before this instant: the leader drains before it
    /// hands its leadership over ([`Engine::begin_drain`]).
    pub(crate) drain_until_us: AtomicI64,
    /// Held SHARED by whatever changes the state and registers answers — a
    /// command's run (`run`, a parked pop's `retry_waiter`), a checkpoint's
    /// `take`, a transaction's `prepare` — and EXCLUSIVELY by `reset`: a
    /// step-down never lands between a command's change and its answer's
    /// registration. Without it, an ack whose parts a reset wiped before its
    /// answer waited on them was answered `Done` (the parts looked durable)
    /// for a change that never reached the log, and one a checkpoint had
    /// already carried could be answered `Retry` before it was marked sent.
    pub(crate) serving: RwLock<()>,
    /// When this node's term began (leases acquired before it are foreign).
    pub(crate) term_start_us: AtomicI64,
    /// The engine's clock is `mono_us() + clock_offset` ([`Engine::now_us`]).
    pub(crate) clock_offset: AtomicI64,
    /// The log's last index when this node began to lead: every entry past it
    /// is this term's, and a cursor row such an entry writes is the engine's
    /// own (a checkpoint, a transaction's rows, a seek it served) or one for a
    /// partition it cannot hold yet (the planner's positions on a partition
    /// the same entry creates). Only rows of earlier terms — another leader's
    /// checkpoints this node applies late — are adopted ([`hooks`]).
    /// `u64::MAX` until known: every row is looked at.
    pub(crate) own_from: AtomicU64,
    /// The committed cluster version (§12.8, D20) this engine writes under:
    /// read from the store when it begins to lead, then moved by the batcher
    /// as its planning reads it ([`Engine::note_cluster_version`]) — never
    /// ahead of what the batcher checks every entry against, so a row this
    /// engine writes is never one the batcher refuses. The rows it logs are
    /// [`CursorRow::admit`]ted to it, and an ack keeps no released lease
    /// below [`crate::rsm::effect::VERSION_3`] ([`Engine::cluster_allows`]).
    ///
    /// [`CursorRow::admit`]: crate::rsm::effect::CursorRow::admit
    pub(crate) cluster: AtomicU32,
    pub(crate) reg: RwLock<Registry>,
    pub(crate) shards: Box<[Mutex<Shard>]>,
    /// Per shard: appends apply reported while the shard was busy, for the
    /// serve thread ([`Engine::drain_appends`]); `appends_pending` says some
    /// wait.
    pub(crate) appends: Box<[Mutex<QueuedAppends>]>,
    pub(crate) appends_pending: AtomicBool,
    pub(crate) answers: Mutex<Answers>,
    pub(crate) tickets: Mutex<Tickets>,
    /// Groups whose catalog effects (registration, implicit queue) are owed.
    pub(crate) cat_dirty: Mutex<Vec<Arc<state::Group>>>,
    pub(crate) txns: Mutex<txn::Reservations>,
    /// Commands held through a new leader's pause.
    pub(crate) paused: Mutex<Vec<Held>>,
    /// The serve thread's queue and its condition (shared with the thread,
    /// which holds the engine only by a `Weak` while it waits).
    pub(crate) waking: Arc<wait::Waking>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Group loads running (each re-runs the commands it held when it ends).
    pub(crate) loading: std::sync::atomic::AtomicUsize,
    /// The idle-part unloader's pacing and counters ([`unload`]).
    pub(crate) unload: unload::Unload,
}

impl Drop for Engine {
    fn drop(&mut self) {
        {
            let mut w = lock(&self.waking.q);
            w.stop = true;
        }
        self.waking.cv.notify_all();
    }
}

impl Engine {
    /// The engine of one facade (every node builds one; it serves only while
    /// its node leads).
    pub fn new(store: Arc<HeedStore>) -> Arc<Engine> {
        Engine::with_knobs(store, Knobs::from_env())
    }

    pub(crate) fn with_knobs(store: Arc<HeedStore>, k: Knobs) -> Arc<Engine> {
        Arc::new_cyclic(|me| Engine {
            me: me.clone(),
            store,
            k,
            lease: RwLock::new(None),
            waker: RwLock::new(None),
            seg: RwLock::new(None),
            mode_override: RwLock::new(None),
            leader: AtomicBool::new(false),
            standby: AtomicBool::new(false),
            gen: AtomicU64::new(0),
            serve_after_us: AtomicI64::new(0),
            drain_until_us: AtomicI64::new(0),
            serving: RwLock::new(()),
            term_start_us: AtomicI64::new(0),
            clock_offset: AtomicI64::new(wall_us() - mono_us()),
            own_from: AtomicU64::new(u64::MAX),
            cluster: AtomicU32::new(crate::rsm::effect::BASELINE_KINDS_VERSION),
            reg: RwLock::new(Registry::default()),
            shards: (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect(),
            appends: (0..SHARDS).map(|_| Mutex::new(Vec::new())).collect(),
            appends_pending: AtomicBool::new(false),
            answers: Mutex::new(Answers::default()),
            tickets: Mutex::new(Tickets {
                next: 1,
                ..Tickets::default()
            }),
            cat_dirty: Mutex::new(Vec::new()),
            txns: Mutex::new(txn::Reservations::default()),
            paused: Mutex::new(Vec::new()),
            waking: Arc::new(wait::Waking::default()),
            thread: Mutex::new(None),
            loading: std::sync::atomic::AtomicUsize::new(0),
            unload: unload::Unload::default(),
        })
    }

    /// Install the leader lease probe (right after [`Engine::new`]): the engine
    /// serves only while it answers an age within `QUEEN_CONSUME_LEADER_LEASE_MS`.
    pub fn set_leader_lease(&self, f: LeaderLease) {
        *write(&self.lease) = Some(f);
    }

    /// Install the waker of the pops a facade parks itself (the engine calls
    /// it when a partition of a group becomes claimable).
    pub fn set_waker(&self, w: Waker) {
        *write(&self.waker) = Some(w);
    }

    /// The segment-authority readers (`QUEEN_RAFT_DEDUP_INDEX=segment`), where
    /// no `txns` row exists: the claims and the ack resolution read the frames
    /// through them.
    pub fn set_segment_readers(&self, reader: segments::Reader, qlog: Option<QLogReader>) {
        *write(&self.seg) = Some(frames::SegSource { reader, qlog });
    }

    #[cfg(test)]
    pub(crate) fn set_mode(&self, m: IndexMode) {
        *write(&self.mode_override) = Some(m);
    }

    /// Wait until no group load runs (tests: loads run on their own thread).
    #[cfg(test)]
    pub(crate) fn wait_loads(&self) {
        for _ in 0..10_000 {
            if self.loading.load(Ordering::Acquire) == 0 {
                return;
            }
            std::thread::sleep(Duration::from_micros(200));
        }
        panic!("a group load never ended");
    }

    pub(crate) fn mode(&self) -> IndexMode {
        (*read(&self.mode_override)).unwrap_or_else(crate::rsm::dedup::record_index_mode)
    }

    pub(crate) fn arc(&self) -> Option<Arc<Engine>> {
        self.me.upgrade()
    }

    pub(crate) fn seg_source(&self) -> Option<frames::SegSource> {
        read(&self.seg).clone()
    }

    pub(crate) fn wake_facade(&self, tenant: &str, queue: &str, group: &str) {
        let w = read(&self.waker).clone();
        if let Some(w) = w {
            w(tenant, queue, group);
        }
    }

    /// The skew grace a foreign lease is held for.
    pub(crate) fn grace(&self) -> i64 {
        self.k.skew_us
    }

    /// The batcher read this cluster's place in a link
    /// ([`crate::rsm::link`]): a standby (`true`) or not.
    pub fn set_standby(&self, on: bool) {
        self.standby.store(on, Ordering::Release);
    }

    /// Whether this node leads a standby cluster: the leader's intake then
    /// answers every client command with the standby refusal.
    pub fn is_standby(&self) -> bool {
        self.standby.load(Ordering::Acquire)
    }

    /// The batcher planned a cycle under committed cluster version `version`
    /// (D20): from now on this engine may write what it admits. Never down.
    /// Not from apply's hook: apply reports an effect before the store commits
    /// it, and the batcher checks each entry against what its read of the
    /// store says.
    pub fn note_cluster_version(&self, version: u32) {
        self.cluster.fetch_max(version, Ordering::AcqRel);
    }

    /// Whether the cluster this engine leads lets it write what was minted at
    /// catalogue `version` (D20, [`crate::rsm::effect::cluster_allows`]).
    pub(crate) fn cluster_allows(&self, version: u16) -> bool {
        crate::rsm::effect::cluster_allows(self.cluster.load(Ordering::Acquire), version)
    }

    /// The cursor row this engine logs for `cur`: its whole row, admitted to
    /// the cluster version ([`crate::rsm::effect::CursorRow::admit`]). Every
    /// `CursorSet` the engine writes — a checkpoint's, a transaction's — is
    /// one of these.
    pub(crate) fn logged_row(&self, cur: &state::Cur) -> crate::rsm::effect::CursorRow {
        let mut row = cur.row();
        row.admit(self.cluster.load(Ordering::Acquire));
        row
    }

    /// Why this node may not serve now (`None`: it may).
    fn gate(&self) -> Option<Reply> {
        if !self.leader.load(Ordering::Acquire) {
            return Some(Reply::Retry { hint: None });
        }
        // Draining to hand the leadership over: a command that never ran here
        // runs on the next leader.
        if self.now_us() < self.drain_until_us.load(Ordering::Acquire) {
            return Some(Reply::Retry { hint: None });
        }
        let probe = read(&self.lease).clone();
        if let Some(f) = probe {
            match f() {
                Some(age) if age <= Duration::from_millis(self.k.lease_ms) => {}
                _ => return Some(Reply::Retry { hint: None }),
            }
        }
        None
    }

    /// Whether a command is the engine's.
    fn mine(cmd: &Command) -> bool {
        match cmd {
            Command::PopWildcard(_)
            | Command::PopPinned(_)
            | Command::PopDiscover(_)
            | Command::Ack(_)
            | Command::AckPositional(_)
            | Command::Nack(_)
            | Command::Renew(_)
            | Command::DlqHead(_) => true,
            Command::Effects(c) => ack::is_seek(c),
            _ => false,
        }
    }

    /// Serve a consumption command on the leader (local or forwarded).
    pub fn serve(&self, cmd: &Command, now_us: i64) -> Served {
        if !Engine::mine(cmd) {
            return Served::NotMine;
        }
        if let Some(r) = self.gate() {
            return Served::Now(r);
        }
        let (tx, rx) = oneshot::channel();
        let mut sink = Some(tx);
        match self.run(cmd, now_us, &mut sink) {
            Some(reply) => Served::Now(reply),
            None => Served::Later(rx),
        }
    }

    /// Run a command: `Some(reply)` answers it now; `None` means the sink was
    /// taken (the answer comes later through it).
    pub(crate) fn run(
        &self,
        cmd: &Command,
        now_us: i64,
        sink: &mut Option<oneshot::Sender<Reply>>,
    ) -> Option<Reply> {
        let _serving = read(&self.serving);
        // The gate again, under the guard: a command that passed it just
        // before a step-down or a drain began must not run now (a drain that
        // already saw no exact answer pending would hand off under it).
        if !self.leader.load(Ordering::Acquire)
            || self.now_us() < self.drain_until_us.load(Ordering::Acquire)
        {
            return Some(Reply::Retry { hint: None });
        }
        if now_us < self.serve_after_us.load(Ordering::Acquire) {
            // A new leader's pause: held, served when it ends.
            if let Some(tx) = sink.take() {
                lock(&self.paused).push(Held {
                    cmd: cmd.clone(),
                    sink: tx,
                    at_us: now_us,
                });
                self.nudge();
                return None;
            }
        }
        let out = match cmd {
            Command::PopWildcard(c) => self.pop(c, pop::Kind::Wildcard, now_us, sink),
            Command::PopPinned(c) => self.pop(c, pop::Kind::Pinned, now_us, sink),
            Command::PopDiscover(c) => self.pop(c, pop::Kind::Discover, now_us, sink),
            Command::Ack(c) => self.ack_cmd(c, now_us, sink),
            Command::AckPositional(c) => self.positional_cmd(c, now_us, sink),
            Command::Nack(c) => self.nack_cmd(c, now_us, sink),
            Command::Renew(c) => self.renew_cmd(c, now_us, sink),
            Command::DlqHead(c) => self.dlq_head_cmd(c, now_us, sink),
            Command::Effects(c) => self.seek_cmd(c, now_us, sink),
            _ => Err(Refusal::client("internal", "not a consumption command")),
        };
        match out {
            Ok(r) => r,
            Err(refusal) => Some(Reply::Refused(refusal)),
        }
    }

    /// Run a held command and deliver its answer through its own sink.
    pub(crate) fn run_held(&self, h: Held, now_us: i64) {
        if let Some(r) = self.gate() {
            let _ = h.sink.send(r);
            return;
        }
        let mut sink = Some(h.sink);
        let now_us = now_us.max(h.at_us);
        if let Some(reply) = self.run(&h.cmd, now_us, &mut sink) {
            if let Some(tx) = sink.take() {
                let _ = tx.send(reply);
            }
        }
    }

    /// Validate and reserve the consumption half of a transaction.
    pub fn txn_prepare(&self, txn: &TxnCommand, now_us: i64) -> Result<Option<TxnPart>, Refusal> {
        if txn.acks.is_empty() && txn.positional_acks.is_empty() && txn.positions.is_empty() {
            return Ok(None);
        }
        if let Some(Reply::Retry { .. }) = self.gate() {
            return Err(Refusal::retry("not_leader", "this node does not lead"));
        }
        self.prepare(txn, now_us).map(Some)
    }

    /// Whether a logged command with this request id committed and is still
    /// in the request-id window (D6): a retry of it is answered from that
    /// record, never prepared again.
    pub fn recorded(&self, id: &RequestId) -> bool {
        use crate::rsm::store::{Store, TypedReads};
        self.store
            .read(|r| r.request_outcome(id))
            .ok()
            .flatten()
            .is_some()
    }

    /// Whether this engine holds the reservation of a transaction it
    /// prepared ([`Engine::txn_prepare`]): the batcher plans an engine-prepared
    /// transaction only then (a step-down, or the reservation's TTL, drops it,
    /// and with it the validity of the rows the transaction carries).
    pub fn holds_reservation(&self, id: &RequestId) -> bool {
        lock(&self.txns).map.contains_key(id)
    }

    /// The transaction's entry committed (`true`) or never will (`false`).
    pub fn txn_resolve(&self, id: RequestId, committed: bool) {
        self.resolve_txn(id, committed, self.now_us());
    }

    /// Apply appended to `pid` up to `last_offset` (every node).
    pub fn on_append(&self, pid: crate::rsm::effect::Pid, last_offset: i64) {
        if !self.leader.load(Ordering::Acquire) {
            return;
        }
        let t = std::time::Instant::now();
        self.hook_append(pid, last_offset);
        hooks::note_hook(t.elapsed());
    }

    /// Apply applied a catalog or cursor effect of the entry at `index`
    /// (every node).
    pub fn on_effect(&self, e: &Effect, index: u64) {
        if !self.leader.load(Ordering::Acquire) {
            return;
        }
        // This term's cursor rows are the engine's own: nothing to adopt, and
        // no shard lock taken on apply's thread for the ~2 rows a message
        // batch writes (its lease, its ack).
        if matches!(e, Effect::CursorSet { .. }) && index > self.own_from.load(Ordering::Acquire) {
            return;
        }
        let t = std::time::Instant::now();
        self.hook_effect(e);
        hooks::note_hook(t.elapsed());
    }

    /// The engine's clock (µs): the wall clock as it read when this node
    /// began to lead — never behind the log's own clock, never behind this
    /// engine's earlier reading — carried forward by the monotonic clock. A
    /// lease is timed on it, so a wall clock that jumps while this node leads
    /// cannot cut one short: with the raw wall clock, a leader whose clock
    /// flipped between true time and +59 s every millisecond (Jepsen's clock
    /// strobe) expired 3 s leases at once and handed their messages out again
    /// while their holders still worked on them (2026-09-30, p11-w2-clock:
    /// 223 lease overlaps).
    pub fn now_us(&self) -> i64 {
        mono_us().saturating_add(self.clock_offset.load(Ordering::Acquire))
    }

    /// Re-anchor the clock to the wall clock, never below `floor_us` (the
    /// log's clock) nor below what it already reads.
    fn anchor_clock(&self, floor_us: i64) {
        let target = wall_us().max(floor_us).max(self.now_us());
        self.clock_offset
            .store(target.saturating_sub(mono_us()), Ordering::Release);
    }

    /// This node leads (again): forget everything, pause, load lazily. The
    /// pause (the checkpoint interval plus the clock skew) covers answers and
    /// leases another leader may still have given; a single voter (the lease
    /// probe reports an age of zero) has had no other leader, and does not.
    ///
    /// `last_index`: the log's last index as this node begins to lead (every
    /// later entry is this term's, [`Engine::own_from`]).
    pub fn on_leader(&self, _term: u64, last_index: u64) {
        // Nothing runs on the incarnation being replaced while it is (a term
        // change without a step-down in between: the batcher's first look).
        self.leader.store(false, Ordering::Release);
        // The cluster version as the store has it now: never above the
        // cluster's (the batcher's cycles move it up as their reads see more,
        // `note_cluster_version`); the baseline if the read fails.
        let (floor, cluster) = {
            use crate::rsm::store::{Store, TypedReads};
            self.store
                .read(|r| Ok((r.last_now_us()?, r.cluster_version()?)))
                .unwrap_or((0, crate::rsm::effect::BASELINE_KINDS_VERSION))
        };
        self.cluster.store(cluster, Ordering::Release);
        self.anchor_clock(floor);
        let now = self.now_us();
        self.reset(Reply::Retry { hint: None });
        self.own_from.store(last_index, Ordering::Release);
        self.term_start_us.store(now, Ordering::Release);
        let probe = read(&self.lease).clone();
        let single = probe.is_some_and(|f| f().is_some_and(|age| age.is_zero()));
        let pause = if single {
            0
        } else {
            now + self.k.ckpt_ms as i64 * 1000 + self.k.skew_us
        };
        self.serve_after_us.store(pause, Ordering::Release);
        // (The generation moved inside the reset's fence: a take or a load
        // of the incarnation before sees it change, none of this one does.)
        self.leader.store(true, Ordering::Release);
        self.start_thread();
        self.nudge();
    }

    /// This leader is about to hand its leadership over (a graceful stop):
    /// for at most `max`, every new command is answered `Retry` (it never ran
    /// here: the next leader runs it), while the ticker keeps checkpointing
    /// what was already served. The caller waits until no exact answer is
    /// pending ([`Engine::exact_pending`]) and then hands off: an ack answered
    /// from memory before the stop reaches the log and its client, instead of
    /// being answered in doubt ([`in_doubt`]) at the step-down. The drain
    /// ends by itself after `max` should the caller never end it.
    pub fn begin_drain(&self, max: Duration) {
        let until = self
            .now_us()
            .saturating_add(max.as_micros().min(i64::MAX as u128) as i64);
        self.drain_until_us.store(until, Ordering::Release);
        // A command that passed the gate before the drain began ends before
        // this returns: its exact answer is then one `exact_pending` sees.
        drop(write(&self.serving));
    }

    /// The drain is over (the hand-off happened, or did not and this node
    /// serves on).
    pub fn end_drain(&self) {
        self.drain_until_us.store(0, Ordering::Release);
    }

    /// Whether an exact command (an ack, a nack, a positional ack, a DLQ head)
    /// still waits for its checkpoint to commit.
    pub fn exact_pending(&self) -> bool {
        lock(&self.answers).map.values().any(|a| a.exact)
    }

    /// This node no longer leads: every held answer gets `Retry` — but for
    /// an exact command's whose change a checkpoint carried ([`in_doubt`]).
    pub fn on_step_down(&self) {
        self.leader.store(false, Ordering::Release);
        self.own_from.store(u64::MAX, Ordering::Release);
        self.reset(Reply::Retry { hint: None });
    }

    /// Whether `id` is a checkpoint command in flight: the batcher answers it
    /// when its entry commits ([`state::Tickets::ids`]).
    pub fn answers_at_commit(&self, id: &crate::rsm::entry::RequestId) -> bool {
        lock(&self.tickets).ids.contains(id)
    }

    /// The checkpoint the ticker submits (leader).
    pub fn take_checkpoint(&self, now_us: i64) -> Option<Checkpoint> {
        if !self.leader.load(Ordering::Acquire) {
            return None;
        }
        self.take(now_us)
    }

    /// The checkpoint's commands ALL committed (`true`), or at least one was
    /// refused (`false`: its rows are written again, from the state as it is
    /// then, by a later checkpoint).
    pub fn checkpoint_resolved(&self, ticket: u64, committed: bool) {
        self.resolve_ticket(ticket, committed);
    }

    /// Deadlines, lease expiries, delayed readiness, reservation TTLs.
    pub fn tick(&self, now_us: i64) {
        if !self.leader.load(Ordering::Acquire) {
            return;
        }
        self.tick_inner(now_us);
    }

    /// How many partitions of the group are claimable now; `None` when the
    /// engine does not hold the group. (The autopilot's width reads the same
    /// count inside the claim, [`pop::width`].)
    #[cfg(test)]
    pub fn ready_count(&self, tenant: &str, queue: &str, group: &str) -> Option<usize> {
        let g = read(&self.reg).get(tenant, queue, group)?;
        Some(g.ready_n.load(Ordering::Acquire))
    }

    /// Drop every piece of state, answering what waits with `reply`.
    fn reset(&self, reply: Reply) {
        // Every command, take and prepare in progress ends first; none starts
        // until this reset is done ([`Engine::serving`]).
        let _fence = write(&self.serving);
        // A checkpoint being taken now is of the incarnation that ends here:
        // it must not register (checkpoint.rs `take`).
        self.gen.fetch_add(1, Ordering::AcqRel);
        // Held commands (the pause).
        for h in std::mem::take(&mut *lock(&self.paused)) {
            let _ = h.sink.send(reply.clone());
        }
        // Groups: their waiters and the commands their loads held.
        let groups: Vec<Arc<state::Group>> = {
            let mut reg = write(&self.reg);
            let gs = reg.by_id.values().cloned().collect();
            // Ids are never reused: a load of the old state still running
            // must not land under a new group's id.
            let next = reg.next;
            *reg = Registry::default();
            reg.next = next;
            gs
        };
        for g in groups {
            g.dead.store(true, Ordering::Release);
            let (waiters, held) = {
                let mut st = lock(&g.st);
                st.dropped = true;
                (
                    std::mem::take(&mut st.waiters),
                    std::mem::take(&mut st.held),
                )
            };
            g.waiting.store(0, Ordering::Release);
            for w in waiters {
                let _ = w.sink.send(reply.clone());
            }
            for h in held {
                let _ = h.sink.send(reply.clone());
            }
        }
        for s in self.shards.iter() {
            *lock(s) = Shard::default();
        }
        for a in self.appends.iter() {
            lock(a).clear();
        }
        self.appends_pending.store(false, Ordering::Release);
        lock(&self.cat_dirty).clear();
        {
            let mut ts = lock(&self.tickets);
            ts.inflight.clear();
            ts.ids.clear();
        }
        let answers = std::mem::take(&mut lock(&self.answers).map);
        for (_, a) in answers {
            // A retry of an exact command whose change may be in the log
            // would run it a second time: in doubt instead.
            let r = if a.exact && a.sent {
                Reply::Refused(in_doubt())
            } else {
                reply.clone()
            };
            let _ = a.sink.send(r);
        }
        *lock(&self.txns) = txn::Reservations::default();
        lock(&self.waking.q).clear();
    }

    /// The empty pop answer.
    pub(crate) fn empty_pop() -> Reply {
        Reply::Done {
            outcome: Outcome::Pop(PopOutcome::default()),
            at: None,
        }
    }
}
