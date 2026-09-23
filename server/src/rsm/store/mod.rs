//! The ordered store: the adapter, its keyspaces and the four ratified pins
//! (PLAN_RAFT.md §6.1, §6.2, §11.3, §11.8, D9).
//!
//! Everything that is not a payload lives here. Payload bytes go to the
//! segment files (§11.2, WP-1.3) and the index of a sealed segment file to its
//! own `.qidx` beside it (the §6.1 G0 amendment), so this store holds the rows
//! whose *shape* is a key-value one: queues, groups, partitions and their name
//! indexes, cursors and the two indexes derived from them, the dedup index,
//! dead letters, request ids, counters and `meta`.
//!
//! # The engine and its four pins (D9, RATIFIED at G0 2026-09-18)
//!
//! heed 0.22 (LMDB), EVERYWHERE — 3 voters, raft1 and embedded alike. The
//! numbers D9 was chosen on do not survive a change to any of these four, so
//! each one is named where it is implemented in [`heed_store`]:
//!
//! 1. **`MDB_NOSYNC`**, never `MDB_NOMETASYNC`. The Raft log is the
//!    write-ahead log; the store reaches the platter at a durable point
//!    (§11.4) and nowhere else. NOMETASYNC was measured at 87% of the rate,
//!    1.8× the kernel writes and a **123 ms** store-commit p99 on the apply
//!    thread (RAFT_STATUS.md R-05).
//! 2. **A read-transaction HANDLE, never a free-standing `get`/`scan`.** With
//!    thread-local reader slots a second read transaction on one thread is
//!    `MDB_BAD_RSLOT`, and the `Send`-capable mode (`MDB_NOTLS`) does not
//!    scale: 2.06/1.97/2.08 M gets/s at 1/4/8 threads against 2.10/8.18/**9.96**
//!    M with TLS slots (R-06). So [`Store::read`] takes a closure: a read
//!    BEGINS AND ENDS inside one blocking call, never crosses an `.await`, and
//!    a nested one is refused by construction ([`StoreError::NestedRead`])
//!    instead of reaching LMDB and coming back as `MDB_BAD_RSLOT`.
//! 3. **`max_readers` ≥ the blocking pool**, and `MDB_READERS_FULL` is a
//!    REFUSAL ([`StoreError::ReadersFull`], retryable), never a panic.
//! 4. **The map-size rule** (§11.8): the map is sized at open from what the
//!    store already holds, its usage is reported for `Status` and for the
//!    planner's gate ([`Store::map_usage`]), and `MDB_MAP_FULL` on the apply
//!    path is a typed error with a metric ([`StoreError::MapFull`]), because
//!    it is a node-local liveness cliff the leader cannot see (R-18).
//!
//! # What an error means, and the two questions WP-1.4 asks it
//!
//! [`StoreError::retryable`] and [`StoreError::fatal`] are the whole
//! vocabulary the apply thread has, so every condition this module can answer
//! is classified on purpose — and the ones that are NEITHER are named as such
//! ([`StoreError::KeyTooLong`], a 4xx refusal for the planner; `Mdb` and `Io`,
//! where the caller decides). Two of them used to hide in that last group and
//! must not:
//!
//! - **a commit that did not happen** ([`StoreError::CommitFailed`]). The
//!   durable point of §11.4 is `mdb_txn_commit` plus `mdb_env_sync`, and its
//!   step 3 reports a durable index that bounds recovery replay and lets a
//!   replicator truncate its log. A failed sync that read as "carry on" would
//!   therefore let the node report a durable point that is not on the platter,
//!   and on Linux the kernel consumes the error and drops the pages, so the
//!   next sync succeeds and nothing ever says otherwise.
//!   [`StoreError::lost_durable_point`] is what the caller checks.
//! - **`MDB_PANIC`** ([`StoreError::EnvDead`]), the one code LMDB raises to
//!   say the environment is dead.
//!
//! # Phase C: RAM keyspaces, LMDB as their checkpoint
//!
//! The hot keyspaces ([`Keyspace::is_ram`]) live in RAM, loaded in full at
//! open; LMDB holds their CHECKPOINT, written only at the durable point
//! ([`Writes::durable_commit`]). A plain [`Writes::commit`] commits the
//! LMDB-direct keyspaces only, so after a crash the RAM keyspaces — `meta`
//! and its `applied_index` included — reopen exactly at the last durable
//! point and the WAL (the per-queue logs) replays the rest. RAM keyspaces are
//! read LIVE (read-uncommitted) by every handle; LMDB-direct ones keep their
//! snapshot semantics; [`Writes::abort`] does not undo RAM writes. The full
//! contract is in [`heed_store`]'s module header.
//!
//! # One environment, two scopes
//!
//! Replicated keyspaces (§6.1) and node-local ones (§6.2, `seg_loc` and
//! `files`) live in the SAME environment. They have to: I11 says every apply
//! commit records the applied index AND the lengths of the segment files it
//! touched ATOMICALLY, and the file lengths are node-local. Two environments
//! would mean two commits and a window in which they disagree.
//!
//! The price is that a raw file copy of the store (`mdb_env_copy`, §11.6
//! step 3) carries this node's positions with it, which D8 and I7 forbid
//! shipping. That is why every keyspace declares a [`Scope`]: a snapshot build
//! filters the node-local ones out of the export, an install clears them with
//! [`Writes::clear_node_local`] before rebuilding them from the node's own
//! files, and the digest (§12.9) hashes replicated keyspaces only. WP-4.6 and
//! WP-4.7 own those paths; this module owns the label that makes them
//! possible.
//!
//! ## `partition_files` is NODE-LOCAL, and the plan is not consistent here
//!
//! PLAN_RAFT.md lists `partition_files (pid, file_id)` in §6.1's REPLICATED
//! table, and everything else the plan says makes its key node-local: §6.2
//! puts `files (bucket, file_id)` and `seg_loc` among the node-local state,
//! §11.7 has each node compact live segments into a NEW LOCAL file of its own
//! choosing, D8 says positions never appear in an entry or a digest, and I7's
//! test is a snapshot install onto a node whose file boundaries differ. Two
//! correct nodes therefore hold DIFFERENT `(pid, file_id)` rows, so:
//!
//! - as replicated state the keyspace would make the §12.9 digest report
//!   divergence between two nodes that agree on every message; and
//! - [`Writes::clear_node_local`] would leave a receiver holding rows that
//!   name the SENDER's files after an install — the pgless failure D8 was
//!   written against.
//!
//! This module takes the node-local reading, which is the only one that keeps
//! D8 and I7. **PLAN_RAFT.md §6.1 needs the corresponding edit**, which is not
//! this WP's to make (§0.3: a ratified decision changes only with Alice).
//! Nothing consumes the keyspace yet, so the label is free to fix today and
//! expensive at WP-4.6/4.7.
//!
//! # Keys
//!
//! Keys are byte strings ordered by `memcmp`, which is LMDB's default
//! comparator, so every range scan in this file is a scan in the order §6.1
//! names. Integers are big-endian, signed ones with the sign bit flipped;
//! names are escaped so that a composite key of two names cannot be confused
//! with another pair ([`keys`]).
//!
//! # What is NOT here
//!
//! - No clock, no randomness, no environment reads. I2 makes apply a pure
//!   function of (committed state, entry), and everything under `rsm/store/`,
//!   `rsm/state/` and `rsm/apply.rs` is on apply's side of that line. Commit
//!   *latency* is therefore measured by the caller and by the tests, never by
//!   a `Instant::now()` in here; the metrics below are counts.
//! - No `async`. Every call in this module blocks; callers run it on a
//!   blocking thread (the apply thread, or `spawn_blocking` for a read),
//!   which is also pin 2's "one blocking call".
//! - **No deadline on a store call**, and I15 asks for one. LMDB has no API
//!   for it: a `get` or a range step is a page fault on an mmap, and a commit
//!   is a write plus (at a durable point) an fsync, none of which can be
//!   cancelled. What is bounded instead is the QUEUEING, and it is ENFORCED,
//!   not assumed: [`Store::write`] hands out AT MOST ONE write handle at a
//!   time and refuses a second caller at once with
//!   [`StoreError::WriterBusy`] (retryable) instead of letting it block inside
//!   LMDB's writer mutex, which has no timeout and no typed error. So the
//!   apply thread's handle never waits behind another write transaction, a
//!   read never waits at all, and no call in this module can block on a lock a
//!   second caller holds. Across PROCESSES the guards are the data dir's LOCK
//!   (§11.1, WP-1.7) and heed's own refusal to open one path twice. The
//!   caller's deadline therefore lives one level up — the blocking task, the
//!   request — and a stalled DISK shows up there as a slow apply, which
//!   §11.8's gate and the readiness check of §14.1 are what turn into a
//!   refusal. WP-1.4 owns that end.

// I2, enforced rather than reviewed: `clippy.toml` lists the clock,
// environment and randomness calls this side of the line may not make,
// `[lints.clippy]` in Cargo.toml switches the lint off for the rest of the
// package (the postgres class, and every integration test), and this is where
// it is switched back on — for this module and every module under it.
#![deny(clippy::disallowed_methods)]

use std::sync::atomic::{AtomicU64, Ordering};

pub mod heed_store;
pub mod keys;
#[cfg(test)]
mod ram_tests;
pub mod rows;
pub mod typed;

pub use heed_store::HeedStore;
// `TypedWrites` has no caller until apply exists (WP-1.4); the re-export is
// the pair's other half and must not be dropped in the meantime.
#[allow(unused_imports)]
pub use typed::{TypedReads, TypedWrites};

/// The store this build uses. D9 ratified ONE engine for every deployment
/// shape, so this alias is the only place a second one would appear.
pub type DefaultStore = HeedStore;

// ---------------------------------------------------------------------------
// Keyspaces
// ---------------------------------------------------------------------------

/// Whether a keyspace is part of the replicated state machine or belongs to
/// this node alone (§6.2, D8, I7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    /// Replicated: in the digest, in the snapshot export, equal on every node.
    Replicated,
    /// Node-local: positions and file lengths. Never in an entry, never in a
    /// digest, shipped only together with the files it indexes.
    NodeLocal,
}

/// The keyspaces of §6.1 (message path) and §6.2 (node-local).
///
/// The discriminant is NOT a stored value — LMDB names its databases by
/// string — so it may be reordered; [`Keyspace::name`] may not, because it is
/// what an existing data directory reopens with.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Keyspace {
    /// Fixed names → the applied index and term, `next_pid`,
    /// `kv_version_next`, `last_now_us`, `max_created_at_us`, the cluster
    /// version, the membership and the digest chain. See [`meta`].
    Meta,
    /// `pid → (deleted_at, scope)`: partitions whose name-keyed rows are gone
    /// and whose pid-keyed data is still being deleted in chunks (§5.2 rules).
    Garbage,
    /// `(tenant, queue) → QueueConfig`.
    Queues,
    /// `(tenant, queue, group) → GroupRow`.
    Groups,
    /// `pid → PartitionRow`.
    Partitions,
    /// `(tenant, queue, partition) → pid`. The unique name index.
    PartitionsByKey,
    /// `(tenant, queue, pid) → ()`. The scan index for wildcard pops and admin.
    QueuePartitions,
    /// NODE-LOCAL. `(pid, file_id) → ()`: which sealed segment files hold data
    /// of this partition, written once per file seal (the §6.1 G0 amendment).
    /// The per-segment rows are NOT in the store.
    ///
    /// The plan lists this row in §6.1's replicated table and its key in
    /// §6.2's node-local one; see the module header for why the node-local
    /// reading is the only consistent one.
    PartitionFiles,
    /// `(pid, group) → CursorRow`.
    Cursors,
    /// `(worker, pid, group) → lease_expires_at_us`. The derived index
    /// `log_renew_lease_v1` walks in key order, never a hash map.
    LeasesByWorker,
    /// `(tenant, queue, group, pid) → ready_at_us`: partitions with work for a
    /// group, so the ready rings of §6.3 rebuild in O(pending).
    Pending,
    /// `(tenant, queue, dlq_id) → DlqRow`.
    Dlq,
    /// `(pid, group, offset) → dlq_id`. The second index of §6.1's `dlq` row.
    DlqByPos,
    /// `(pid, hash) → occurrence list`. D10 option (a), lean encoding.
    Dedup,
    /// `(pid, base_offset) → [end][created][hashes]`: one sequential row per
    /// `Append`, the expiry half of D10 option (a) lean. Walked per partition
    /// from its own `txns_start`, exactly like 006's purge step.
    Txns,
    /// `request_id → (now_us, outcome)` (D6, §5.4).
    RequestIds,
    /// `(now_us, request_id) → ()`: the expiry index of `request_ids`.
    RequestExpiry,
    /// `(scope, id, counter) → i64` (D16, §6.4).
    Counters,
    /// NODE-LOCAL. `(pid, base_offset) → (bucket, file_id, offset, len)`:
    /// where this node's copy of a segment's bytes is (§6.2).
    SegLoc,
    /// NODE-LOCAL. `(bucket, file_id) → (length at the last durable point,
    /// sealed, live bytes, snapshot references)` (§6.2, I11).
    Files,
    /// `(tenant, ns, key) → KvRow` (024 `queen.kv`, WP-2.2). The tenant and the
    /// namespace are escaped names and the KEY is the raw, unterminated tail, so
    /// the keys of one namespace sort in byte order (the SQL's `COLLATE "C"`)
    /// and a key prefix is a store-key prefix ([`keys::kv`]).
    Kv,
    /// `(expires_at_us, version) → kv store key`: the expiry index of [`Keyspace::Kv`]
    /// — one row per row that carries an expiry, oldest first, so the leader's
    /// sweep (026's `kv_expire_step_v1`) is O(expired) instead of a scan of
    /// every key. The version is unique per write (I18), so the pair is unique.
    KvExpiry,
    /// `(tenant, queue, timer_key) → TimerRow` (025 `queen.log_timers`, WP-2.3).
    /// Keyed by NAMES like the SQL table: a timer names its destination queue
    /// and partition, which are born at the fire, never at the schedule.
    Timers,
    /// `(due_us, tenant, queue, timer_key) → ()`: the fire order. `due_us` is
    /// the row's effective visibility (`deliver_at`, pushed out by a backoff),
    /// the RSM twin of 025's generated `visible_at` column and its only index.
    /// The leader's fire step walks it from the front while `due_us <= now`.
    TimersDue,
    /// `(tenant, query_id) → StreamsQueryRow` (`queen_streams.queries`).
    StreamsQueries,
    /// `(query_id, pid, key) → StreamsStateRow` (`queen_streams.state`).
    StreamsState,
    /// `flag name → JSON` (`queen.system_state`).
    Flags,
    /// `(quota kind, tenant) → QuotaGrant`.
    Quotas,
    /// `(tenant, queue) → ephemeral queue options JSON`.
    EphConfig,
    /// `(tenant, pid?, transaction, sequence) → TraceEvent`.
    Traces,
    /// `(tenant, trace name, created_at, trace_id) → primary trace key`.
    TraceNames,
    /// `(created_at, trace_id) → primary trace key`.
    TraceExpiry,
}

impl Keyspace {
    /// Every keyspace this build opens. Phase 2 appends its own (streams,
    /// traces, flags, quotas, ephemeral config) after `kv`, `kv_expiry`
    /// (WP-2.2), `timers` and `timers_due` (WP-2.3); the environment is opened
    /// with room for them ([`MAX_DBS`]). The ORDER is [`Keyspace::slot`]'s, so
    /// a new keyspace goes at the END of the enum and of this list alike.
    pub const ALL: [Keyspace; 32] = [
        Keyspace::Meta,
        Keyspace::Garbage,
        Keyspace::Queues,
        Keyspace::Groups,
        Keyspace::Partitions,
        Keyspace::PartitionsByKey,
        Keyspace::QueuePartitions,
        Keyspace::PartitionFiles,
        Keyspace::Cursors,
        Keyspace::LeasesByWorker,
        Keyspace::Pending,
        Keyspace::Dlq,
        Keyspace::DlqByPos,
        Keyspace::Dedup,
        Keyspace::Txns,
        Keyspace::RequestIds,
        Keyspace::RequestExpiry,
        Keyspace::Counters,
        Keyspace::SegLoc,
        Keyspace::Files,
        Keyspace::Kv,
        Keyspace::KvExpiry,
        Keyspace::Timers,
        Keyspace::TimersDue,
        Keyspace::StreamsQueries,
        Keyspace::StreamsState,
        Keyspace::Flags,
        Keyspace::Quotas,
        Keyspace::EphConfig,
        Keyspace::Traces,
        Keyspace::TraceNames,
        Keyspace::TraceExpiry,
    ];

    /// The LMDB database name. PERMANENT: it is what an existing data
    /// directory reopens with.
    pub fn name(self) -> &'static str {
        match self {
            Keyspace::Meta => "meta",
            Keyspace::Garbage => "garbage",
            Keyspace::Queues => "queues",
            Keyspace::Groups => "groups",
            Keyspace::Partitions => "partitions",
            Keyspace::PartitionsByKey => "partitions_by_key",
            Keyspace::QueuePartitions => "queue_partitions",
            Keyspace::PartitionFiles => "partition_files",
            Keyspace::Cursors => "cursors",
            Keyspace::LeasesByWorker => "leases_by_worker",
            Keyspace::Pending => "pending",
            Keyspace::Dlq => "dlq",
            Keyspace::DlqByPos => "dlq_by_pos",
            Keyspace::Dedup => "dedup",
            Keyspace::Txns => "txns",
            Keyspace::RequestIds => "request_ids",
            Keyspace::RequestExpiry => "request_expiry",
            Keyspace::Counters => "counters",
            Keyspace::SegLoc => "seg_loc",
            Keyspace::Files => "files",
            Keyspace::Kv => "kv",
            Keyspace::KvExpiry => "kv_expiry",
            Keyspace::Timers => "timers",
            Keyspace::TimersDue => "timers_due",
            Keyspace::StreamsQueries => "streams_queries",
            Keyspace::StreamsState => "streams_state",
            Keyspace::Flags => "flags",
            Keyspace::Quotas => "quotas",
            Keyspace::EphConfig => "eph_config",
            Keyspace::Traces => "traces",
            Keyspace::TraceNames => "trace_names",
            Keyspace::TraceExpiry => "trace_expiry",
        }
    }

    /// Replicated or node-local (§6.2, D8). See the module header.
    ///
    /// The rule, and it is the one D8 states: a keyspace is NODE-LOCAL when
    /// its key or its value names a FILE of this node — a `file_id`, a
    /// position, a length. Those three are [`Keyspace::SegLoc`],
    /// [`Keyspace::Files`] and [`Keyspace::PartitionFiles`].
    pub fn scope(self) -> Scope {
        match self {
            Keyspace::SegLoc | Keyspace::Files | Keyspace::PartitionFiles => Scope::NodeLocal,
            _ => Scope::Replicated,
        }
    }

    /// The index into the adapter's database array. Derived from [`ALL`]'s
    /// order, which is why nothing outside this module may rely on it.
    ///
    /// [`ALL`]: Keyspace::ALL
    pub(crate) fn slot(self) -> usize {
        self as usize
    }

    /// Phase C: served from an in-RAM table that LMDB only CHECKPOINTS at the
    /// durable point, rather than written through the LMDB transaction. See
    /// [`heed_store`]'s module header for the commit and isolation semantics
    /// (RAM keyspaces are read LIVE, and reopen at the last durable point).
    ///
    /// EVERY keyspace, deliberately: one visibility horizon and one durability
    /// horizon. A split (hot keyspaces in RAM, the rest committed straight to
    /// LMDB) measurably broke both — the planner drops a landed entry from its
    /// overlay at the LIVE applied index, so an LMDB-direct effect of an applied
    /// but not-yet-committed entry was invisible to it (a dedup miss); and after
    /// a crash the LMDB-direct keyspaces reopened AHEAD of the checkpoint, so
    /// replay applied their effects twice. With every keyspace here, the store
    /// reopens exactly at the durable point and replay from `durable_index + 1`
    /// applies each later entry exactly once. The price is RAM for the cold
    /// keyspaces too (the DLQ, the segment index); none is per-message on the
    /// qlog + `DEDUP_INDEX=segment` path.
    pub fn is_ram(self) -> bool {
        true
    }
}

/// `max_dbs` for the environment. LMDB fixes it at open, so it carries the
/// whole §6.1 catalogue plus headroom: phase 2 adds `kv`, `kv_expiry` (both
/// present since WP-2.2), `timers`, `timers_due` (both present since WP-2.3),
/// `quotas`, `eph_config`, `streams_queries`, `streams_state`, `flags`,
/// `traces`, `trace_names` and `trace_expiry` to [`Keyspace`] and must not need
/// a data-directory migration to do it.
pub const MAX_DBS: u32 = 64;

// ---------------------------------------------------------------------------
// meta
// ---------------------------------------------------------------------------

/// The fixed keys of [`Keyspace::Meta`] (§6.1). Their values are fixed-width
/// little-endian scalars, except `membership` and `digest_chain`, which are
/// opaque blobs owned by phase 3 and phase 4.
pub mod meta {
    /// `u64`: the index of the last entry apply has executed.
    pub const APPLIED_INDEX: &[u8] = b"applied_index";
    /// `u64`: the term of that entry.
    pub const APPLIED_TERM: &[u8] = b"applied_term";
    /// `u64`: the index the last DURABLE point (§11.4) covered. Recovery
    /// reads it to know how far the segment file lengths below are trusted.
    pub const DURABLE_INDEX: &[u8] = b"durable_index";
    /// `u64`: the highest record `seq` the per-queue qlog was fsync'd through as
    /// of the store commit that carries this key (`QUEEN_RAFT_QLOG`,
    /// `ALICE_PGLESS_NEWARCH.md` §5, Phase A3a). NODE-LOCAL, like
    /// [`DURABLE_INDEX`]: it names this node's qlog platter, not cluster state,
    /// so it is kept OUT of the §12.9 digest. Recovery reconciles the reopened
    /// qlog's durable tail against it — the qlog must be AHEAD of or EQUAL to it,
    /// never behind for a committed record (NA-QLOG-I1).
    pub const QLOG_DURABLE_INDEX: &[u8] = b"qlog_durable_index";
    /// `u64`: the next partition id the planner may assign (I18, §5.1).
    pub const NEXT_PID: &[u8] = b"next_pid";
    /// `u64`: the next KV version (I18, §5.1).
    pub const KV_VERSION_NEXT: &[u8] = b"kv_version_next";
    /// `i64`: the `now_us` of the last applied entry (D5, I5).
    pub const LAST_NOW_US: &[u8] = b"last_now_us";
    /// `i64`: the highest `created_at` any `Append` has written (§7.4).
    pub const MAX_CREATED_AT_US: &[u8] = b"max_created_at_us";
    /// `u32`: the replicated cluster version (D20, §12.8).
    pub const CLUSTER_VERSION: &[u8] = b"cluster_version";
    /// Opaque: the membership record, including the initial voter set.
    pub const MEMBERSHIP: &[u8] = b"membership";
    /// Opaque: the digest chain (§12.9).
    pub const DIGEST_CHAIN: &[u8] = b"digest_chain";
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// What the store refuses with. Nothing in this module panics on a condition
/// the environment can produce: "refuse, never guess" (§0.3) applies to the
/// store exactly as it applies to the planner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreError {
    /// Pin 2. A read transaction was asked for while this thread already held
    /// one. LMDB would answer `MDB_BAD_RSLOT` (measured: the 2nd of 200 on one
    /// thread, R-06); the adapter refuses first, so the caller gets a named
    /// error instead of a reader-slot corruption.
    NestedRead,
    /// Pin 3. `MDB_READERS_FULL`: every reader slot is taken. RETRYABLE — the
    /// caller answers a retryable error and the slot frees when a read ends.
    ReadersFull { max_readers: u32 },
    /// A write handle was asked for while one was already out. I1 gives the
    /// apply thread the only one; a second caller is REFUSED here (retryable)
    /// rather than left blocking inside LMDB's writer mutex, which has no
    /// deadline (I15). See the module header.
    WriterBusy,
    /// Pin 4. `MDB_MAP_FULL`: the environment's map is full. On the apply path
    /// this is a node-local liveness cliff (§11.8, R-18): the write
    /// transaction is aborted, nothing is lost (the Raft log is the WAL), and
    /// the node must stop and reopen with a larger map rather than keep
    /// applying. [`StoreError::fatal`] is true.
    MapFull { used_bytes: u64, map_bytes: u64 },
    /// A key longer than LMDB accepts (511 B by default; [`Store::max_key_len`]
    /// reports what this build got). A composite name key — say
    /// `(tenant, queue, group)` — can reach it, and the postgres class has no
    /// such limit, so this is a refusal the planner must turn into a 4xx, not
    /// a panic and not a truncation.
    KeyTooLong {
        keyspace: &'static str,
        len: usize,
        max: usize,
    },
    /// A stored row did not decode. These bytes are what a quorum committed,
    /// so this is never a torn tail to truncate (I11) and never a row to skip
    /// (I16): it is FATAL for this node.
    Corrupt {
        keyspace: &'static str,
        detail: String,
    },
    /// A store commit DID NOT HAPPEN. `durable` says which commit it was:
    ///
    /// - `false` — the ordinary commit of §11.3. The rows of the open
    ///   transaction are gone; the Raft log still holds the entries, but this
    ///   node's store is now behind the applied index the apply thread thinks
    ///   it has, so it must stop and recover (§11.5) rather than apply on.
    /// - `true` — the DURABLE POINT of §11.4, whose two legs are the commit
    ///   and the environment sync. The caller must NOT report a durable index
    ///   to the replicator: a reported durable index bounds recovery replay
    ///   and lets `LocalReplicator` truncate its log behind it, so a durable
    ///   point that never reached the platter would turn a later crash into
    ///   acknowledged effects that no log can replay (I4, I11). On Linux an
    ///   fsync error is consumed by the kernel and the dirty pages are
    ///   dropped, so the NEXT sync succeeds and reports nothing: this error is
    ///   the only moment at which the failure is visible.
    ///
    /// Both are FATAL and never retryable — there is no state left to retry
    /// on.
    CommitFailed { durable: bool, detail: String },
    /// `MDB_PANIC`: LMDB says the environment had a fatal error (a failed
    /// update of a meta page), and every later transaction on it will answer
    /// the same. It is the engine's end of life, so it is FATAL rather than
    /// "anything else LMDB answered".
    EnvDead { detail: String },
    /// Anything else LMDB answered.
    Mdb(String),
    /// The data directory, the lock file, the map size rule.
    Io(String),
}

impl StoreError {
    /// A retryable condition: the caller answers "retry" and the state is
    /// untouched.
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            StoreError::ReadersFull { .. } | StoreError::NestedRead | StoreError::WriterBusy
        )
    }

    /// This node cannot continue applying. It stops (no votes, no acks, no
    /// apply) and an operator or a restart with a larger map resolves it.
    ///
    /// Every condition that leaves committed state and what this node has
    /// ANSWERED out of step is here, and a commit that did not happen is one
    /// of them: the taxonomy has no "continue and hope" slot, because
    /// [`StoreError::retryable`] is the only other answer and there is nothing
    /// to retry.
    pub fn fatal(&self) -> bool {
        matches!(
            self,
            StoreError::MapFull { .. }
                | StoreError::Corrupt { .. }
                | StoreError::CommitFailed { .. }
                | StoreError::EnvDead { .. }
        )
    }

    /// The durable point of §11.4 did not happen. The caller reports NO
    /// durable index for it (step 3), so nothing truncates a log behind a
    /// point that is not on the platter.
    pub fn lost_durable_point(&self) -> bool {
        matches!(self, StoreError::CommitFailed { durable: true, .. })
    }

    pub(crate) fn corrupt(ks: Keyspace, detail: impl Into<String>) -> StoreError {
        StoreError::Corrupt {
            keyspace: ks.name(),
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::NestedRead => f.write_str(
                "store: a read transaction is already open on this thread \
                 (pin 2: one read begins and ends inside one blocking call)",
            ),
            StoreError::ReadersFull { max_readers } => write!(
                f,
                "store: MDB_READERS_FULL, all {max_readers} reader slots are in use (retryable)"
            ),
            StoreError::WriterBusy => f.write_str(
                "store: a write transaction is already open (I1: the apply thread is the \
                 only writer); refused rather than queued behind it (retryable)",
            ),
            StoreError::MapFull {
                used_bytes,
                map_bytes,
            } => write!(
                f,
                "store: MDB_MAP_FULL, {used_bytes} B used of a {map_bytes} B map \
                 (§11.8: reopen with a larger map)"
            ),
            StoreError::KeyTooLong { keyspace, len, max } => write!(
                f,
                "store: {keyspace} key of {len} B exceeds the {max} B limit"
            ),
            StoreError::Corrupt { keyspace, detail } => {
                write!(f, "store: {keyspace} row did not decode: {detail}")
            }
            StoreError::CommitFailed { durable, detail } => {
                let what = if *durable {
                    "the DURABLE POINT (§11.4) did not happen (report no durable index)"
                } else {
                    "a store commit (§11.3) did not happen"
                };
                write!(f, "store: {what}: {detail}")
            }
            StoreError::EnvDead { detail } => write!(
                f,
                "store: MDB_PANIC, the environment had a fatal error and every later \
                 transaction will answer the same: {detail}"
            ),
            StoreError::Mdb(m) => write!(f, "store: {m}"),
            StoreError::Io(m) => write!(f, "store: {m}"),
        }
    }
}

impl std::error::Error for StoreError {}

/// The result of every call in this module.
pub type Result<T> = std::result::Result<T, StoreError>;

// ---------------------------------------------------------------------------
// Key successors (chunked scans and prefix bounds)
// ---------------------------------------------------------------------------

/// The first key AFTER every key that starts with `prefix`, or `None` when
/// there is none (an empty prefix, or one that is all `0xFF`).
///
/// This is the EXCLUSIVE upper bound of a prefix range, and a range scan needs
/// it: without it a scan that starts at the end of the whole keyspace — a
/// reverse listing with no resume point — begins outside its prefix and stops
/// at once.
pub fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != 0xFF {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// Where a chunked scan resumes after `key`: the SMALLEST key an engine with
/// this key limit can hold that is strictly greater than `key`, or `None` when
/// no such key exists, which means the range is exhausted.
///
/// The obvious answer, `key ‖ 0x00`, is right only while the key is shorter
/// than the limit. A key AT the limit (511 B on LMDB's default build, and
/// three unbounded names — `(tenant, queue, group)` — reach it) has no
/// storable extension at all, so the successor is the prefix-end above; and
/// `key ‖ 0x00` would be one byte OVER the limit, which every later scan of
/// the chunk loop refuses with [`StoreError::KeyTooLong`]. That refusal is
/// permanent: it is neither retryable nor fatal, so a ready-ring rebuild
/// (§11.5 step 4) or a `DeleteChunk` loop (§5.2) that hit it could never
/// finish and the node could never serve.
pub fn resume_after(key: &[u8], max_key_len: usize) -> Option<Vec<u8>> {
    if key.is_empty() {
        return Some(vec![0]);
    }
    if key.len() < max_key_len {
        let mut k = Vec::with_capacity(key.len() + 1);
        k.extend_from_slice(key);
        k.push(0);
        return Some(k);
    }
    // At (or over) the limit: nothing longer can be stored, so the next
    // storable key is the first one past this one's prefix range.
    prefix_end(key)
}

// ---------------------------------------------------------------------------
// Open options and the map-size rule
// ---------------------------------------------------------------------------

/// The smallest map the rule will ever pick. Below this, an empty broker would
/// hit [`StoreError::MapFull`] on its first busy second.
pub const MIN_MAP_BYTES: usize = 1 << 30; // 1 GiB

/// The map the rule picks when nothing is configured and the store is empty.
/// LMDB reserves ADDRESS SPACE, not disk: the file is sparse and grows with
/// the data, so a large reservation costs nothing until it is used. §11.8's
/// disk gate (85%) is what bounds the bytes.
pub const DEFAULT_MAP_BYTES: usize = 64 << 30; // 64 GiB

/// Grow at the next open when the store already uses more than this share of
/// its map. Half, not 85%: the gate at 85% is the planner's refusal, and by
/// then a restart must already have room to apply what the log holds.
pub const MAP_GROW_AT: f64 = 0.5;

/// The share of the map above which the planner refuses new data (§11.8, the
/// same shape as the disk gate, and reported in `Status` for the leader to
/// take the worst voter's value).
pub const MAP_HIGH_PCT: f64 = 85.0;
/// The share below which normal service resumes.
pub const MAP_LOW_PCT: f64 = 80.0;

/// How the environment is opened. WP-1.7 fills it from `QUEEN_RAFT_*`; nothing
/// in this module reads the environment itself (I2).
#[derive(Clone, Debug)]
pub struct StoreOpts {
    /// The map size in bytes, or `None` for the rule
    /// ([`StoreOpts::map_size_for`]). Proposed knob: `QUEEN_RAFT_MAP_BYTES`.
    pub map_bytes: Option<usize>,
    /// Pin 3: at least the size of the blocking pool, plus the apply thread,
    /// the loops and a margin. LMDB's own default is 126.
    pub max_readers: u32,
    /// Only a test opens the store with fsync on every commit. Production is
    /// pin 1 (`MDB_NOSYNC`) and a durable commit at the durable point.
    pub sync_every_commit: bool,
}

impl Default for StoreOpts {
    fn default() -> StoreOpts {
        StoreOpts {
            map_bytes: None,
            // The broker's blocking pool is 512 threads by default (tokio's
            // `max_blocking_threads`), and a read holds a slot for the length
            // of one blocking call. 1024 leaves room for the apply thread, the
            // leader loops and a snapshot reader.
            max_readers: 1024,
            sync_every_commit: false,
        }
    }
}

impl StoreOpts {
    /// The map-size rule of §11.8 (R-18), in one place so the open path and
    /// the tests cannot drift.
    ///
    /// - an UNCONFIGURED store gets [`DEFAULT_MAP_BYTES`], never below
    ///   [`MIN_MAP_BYTES`];
    /// - a CONFIGURED size is honoured as given (down to one page): an
    ///   operator who asks for a small map gets it, and §11.8's gate plus the
    ///   typed [`StoreError::MapFull`] are what make that visible rather than
    ///   silent;
    /// - either way, a store already using more than [`MAP_GROW_AT`] of the
    ///   map it would get is given four times its used size, so a reopen never
    ///   starts one busy second away from [`StoreError::MapFull`] and a store
    ///   can never fail to open because its map shrank under it;
    /// - the result is rounded up to a page multiple.
    pub fn map_size_for(&self, used_bytes: u64, page_size: usize) -> usize {
        let page = page_size.max(1);
        let used = used_bytes as usize;
        let mut size = match self.map_bytes {
            Some(v) => v.max(page),
            None => DEFAULT_MAP_BYTES.max(MIN_MAP_BYTES),
        };
        if used as f64 > size as f64 * MAP_GROW_AT {
            size = size.max(used.saturating_mul(4));
        }
        size.div_ceil(page) * page
    }
}

/// What [`Store::map_usage`] reports: the numbers §11.8 asks every node to put
/// in `Status`, so the leader can gate the planner on the WORST voter.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MapUsage {
    pub map_bytes: u64,
    pub used_bytes: u64,
    /// Reader slots in use, and the ceiling pin 3 set.
    pub readers_in_use: u32,
    pub max_readers: u32,
}

impl MapUsage {
    pub fn pct(&self) -> f64 {
        if self.map_bytes == 0 {
            return 0.0;
        }
        self.used_bytes as f64 * 100.0 / self.map_bytes as f64
    }

    /// The §11.8 gate: above [`MAP_HIGH_PCT`] the planner refuses pushes,
    /// timer schedules, KV puts and trace records with 507 `storage_full`.
    pub fn over_high_water(&self) -> bool {
        self.pct() >= MAP_HIGH_PCT
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Counters the adapter keeps. COUNTS ONLY: there is no clock under
/// `rsm/store/` (I2), so latency is measured by the caller and by the tests.
///
/// WP-2.6 maps these to `queen_raft_store_*` in the Prometheus output.
#[derive(Debug, Default)]
pub struct StoreMetrics {
    /// Non-durable commits (§11.3: one every 4 ms or 256 entries).
    pub commits: AtomicU64,
    /// Durable commits (§11.4: the durable point).
    pub durable_commits: AtomicU64,
    /// Write transactions opened.
    pub write_txns: AtomicU64,
    /// Read transactions opened.
    pub read_txns: AtomicU64,
    pub rows_put: AtomicU64,
    pub rows_deleted: AtomicU64,
    /// Key + value bytes handed to `put`: the DENOMINATOR of write
    /// amplification (the numerator is the file growth, which the caller
    /// measures).
    pub logical_bytes: AtomicU64,
    /// Pin 4 fired: `MDB_MAP_FULL` on a write.
    pub map_full: AtomicU64,
    /// Pin 3 fired: `MDB_READERS_FULL`.
    pub readers_full: AtomicU64,
    /// Pin 2 fired: a nested read refused before it reached LMDB.
    pub nested_read: AtomicU64,
    /// A second write handle was refused (I1, I15).
    pub writer_busy: AtomicU64,
    /// A key over the engine's limit was refused.
    pub key_too_long: AtomicU64,
    /// A commit or a durable point did not happen
    /// ([`StoreError::CommitFailed`]). Never expected: it is an alert, and the
    /// node stops.
    pub commit_failed: AtomicU64,
}

impl StoreMetrics {
    pub(crate) fn inc(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(c: &AtomicU64) -> u64 {
        c.load(Ordering::Relaxed)
    }

    /// A stable snapshot for a test or a report.
    pub fn snapshot(&self) -> Vec<(&'static str, u64)> {
        vec![
            ("commits", Self::get(&self.commits)),
            ("durable_commits", Self::get(&self.durable_commits)),
            ("write_txns", Self::get(&self.write_txns)),
            ("read_txns", Self::get(&self.read_txns)),
            ("rows_put", Self::get(&self.rows_put)),
            ("rows_deleted", Self::get(&self.rows_deleted)),
            ("logical_bytes", Self::get(&self.logical_bytes)),
            ("map_full", Self::get(&self.map_full)),
            ("readers_full", Self::get(&self.readers_full)),
            ("nested_read", Self::get(&self.nested_read)),
            ("writer_busy", Self::get(&self.writer_busy)),
            ("key_too_long", Self::get(&self.key_too_long)),
            ("commit_failed", Self::get(&self.commit_failed)),
        ]
    }
}

// ---------------------------------------------------------------------------
// The seam
// ---------------------------------------------------------------------------

/// Reading committed state. Implemented by both handles — the read
/// transaction and the apply thread's open write transaction — because apply
/// must read its own uncommitted writes (a counter it just bumped, a dedup
/// occurrence list it just extended) and the planner must not.
///
/// PHASE C exception: a RAM keyspace ([`Keyspace::is_ram`]) is read LIVE by
/// both handles — a read handle sees what apply wrote a moment ago, before
/// any commit. Only the LMDB-direct keyspaces keep the snapshot above; the
/// planner stays correct because the batcher folds every not-yet-applied
/// entry into its overlay (§7.2).
///
/// The raw pair ([`Reads::get_raw`], [`Reads::scan_raw`]) is the whole
/// surface an engine has to provide; everything typed is a provided method, so
/// the row codecs live in one place ([`rows`]) and a second engine — if D9 is
/// ever re-opened — implements two functions.
pub trait Reads {
    /// The bytes stored at `key`, borrowed from the transaction.
    fn get_raw(&self, ks: Keyspace, key: &[u8]) -> Result<Option<&[u8]>>;

    /// The longest key this engine accepts (LMDB: 511 B by default). A caller
    /// that builds a RESUME key needs it — see [`resume_after`] — so it is on
    /// the handle and not only on [`Store`], which a rebuild running against a
    /// read transaction does not hold.
    fn max_key_len(&self) -> usize;

    /// Ascending range scan. Starts at the first key ≥ `from`, stops at the
    /// first key that does not start with `prefix`, after `limit` rows, or
    /// when `cb` returns false. Returns the number of rows passed to `cb`.
    ///
    /// An empty `prefix` scans to the end of the keyspace. An empty `from`
    /// starts at the beginning of the PREFIX RANGE — not at the beginning of
    /// the keyspace, which would put the walk outside its prefix and stop it
    /// at the first row of another tenant.
    fn scan_raw(
        &self,
        ks: Keyspace,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
        cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<usize>;

    /// Descending range scan, starting at the last key ≤ `from` and keeping
    /// the same `prefix` and `limit` contract as [`Reads::scan_raw`].
    /// Newest-first listings — the DLQ, traces — are ordinary in the read
    /// paths of §8.
    ///
    /// An empty `from` starts at the END OF THE PREFIX RANGE, which is the
    /// only useful reading: starting at the last key of the whole keyspace
    /// would hand a tenant asking for its newest dead letters the rows of
    /// whichever tenant sorts last, see them fail the prefix test and answer
    /// an EMPTY list — a silent wrong answer, not an error. A `from` beyond
    /// the prefix range is clamped to it for the same reason.
    fn scan_rev_raw(
        &self,
        ks: Keyspace,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
        cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<usize>;

    /// Rows in a keyspace. O(n): for tests, digests and reports only.
    fn count(&self, ks: Keyspace) -> Result<u64> {
        let mut n = 0u64;
        self.scan_raw(ks, &[], &[], usize::MAX, &mut |_k, _v| {
            n += 1;
            true
        })?;
        Ok(n)
    }
}

/// Writing committed state. Only the apply thread holds one of these (I1).
///
/// The handle stays open ACROSS entries: §11.3's G0 amendment says the write
/// transaction is committed every `QUEEN_RAFT_STORE_COMMIT_MS` (4) or
/// `QUEEN_RAFT_STORE_COMMIT_ENTRIES` (256), not once per entry, because
/// per-entry commits are what drove the write amplification S1 measured. The
/// Raft log is the write-ahead log, so nothing is lost between commits.
pub trait Writes: Reads {
    fn put_raw(&mut self, ks: Keyspace, key: &[u8], val: &[u8]) -> Result<()>;

    /// Returns whether a row was there.
    fn del_raw(&mut self, ks: Keyspace, key: &[u8]) -> Result<bool>;

    /// Delete every key in `[from, …)` that starts with `prefix`, at most
    /// `limit` of them, in key order. Returns `(deleted, next_resume_key)`;
    /// `None` means the range is exhausted. This is the shape `DeleteChunk`
    /// needs (§5.2 rules).
    ///
    /// The resume key is [`resume_after`], not `last ‖ 0x00`: a keyspace whose
    /// keys can reach the engine's limit — `groups`, `cursors`, `pending`,
    /// `dlq` all carry unbounded names — would otherwise resume with a key one
    /// byte OVER it and refuse every later chunk for good.
    fn delete_range(
        &mut self,
        ks: Keyspace,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
    ) -> Result<(usize, Option<Vec<u8>>)>;

    /// End the transaction and start a new one. NON-DURABLE (pin 1): the
    /// bytes are in the map, readers see them, and a power loss may lose them
    /// — which is exactly what §11.3 asks for.
    ///
    /// Phase C: this commits the LMDB-direct keyspaces ONLY. The RAM
    /// keyspaces are not written to LMDB here; they stay dirty until the next
    /// [`Writes::durable_commit`], and a crash reopens them at the last one.
    fn commit(&mut self) -> Result<()>;

    /// End the transaction, start a new one, and make everything committed so
    /// far survive a power loss: the store half of the durable point (§11.4).
    ///
    /// Phase C: it first CHECKPOINTS the RAM keyspaces — every key changed
    /// since the last durable commit is written into the transaction — so the
    /// LMDB image of them is consistent as of this call.
    fn durable_commit(&mut self) -> Result<()>;

    /// Throw the open transaction away. Used by a test and by the crash paths;
    /// dropping the handle does the same.
    ///
    /// Phase C: RAM writes are NOT undone (they stay dirty for the next
    /// checkpoint). Apply poisons itself and the process restarts on any
    /// apply failure; recovery rebuilds from the checkpoint plus the WAL.
    fn abort(&mut self) -> Result<()>;

    /// Empty every [`Scope::NodeLocal`] keyspace. A snapshot install (§11.6,
    /// I17) calls it before rebuilding positions from the node's own files,
    /// so a sender's positions can never become a receiver's (D8, I7).
    fn clear_node_local(&mut self) -> Result<()> {
        for ks in Keyspace::ALL {
            if ks.scope() == Scope::NodeLocal {
                let mut resume: Option<Vec<u8>> = Some(Vec::new());
                while let Some(from) = resume {
                    let (n, next) = self.delete_range(ks, &from, &[], 4096)?;
                    if n == 0 {
                        break;
                    }
                    resume = next;
                }
            }
        }
        Ok(())
    }
}

/// The store itself.
///
/// Not object-safe on purpose: [`Store::read`] is generic in what the closure
/// returns, which is the shape pin 2 asks for (the handle never escapes the
/// call). D9 ratified ONE engine, so nothing needs `dyn Store`; a second
/// engine would be a second `impl` and a type parameter on the apply thread.
pub trait Store: Send + Sync {
    /// The read handle. `!Send` by construction with thread-local reader
    /// slots, which is half of why it cannot cross an `.await`.
    type Read<'s>: Reads
    where
        Self: 's;
    /// The write handle, owned by the apply thread.
    type Write<'s>: Writes
    where
        Self: 's;

    /// Run `f` inside ONE read transaction (pin 2). The transaction begins
    /// and ends inside this call; the handle cannot escape it, and a nested
    /// call on the same thread is [`StoreError::NestedRead`].
    fn read<R>(&self, f: impl FnOnce(&Self::Read<'_>) -> Result<R>) -> Result<R>;

    /// Open the write transaction. There is ONE writer — the apply thread
    /// (I1) — and this call enforces it: while a handle is out, a second
    /// caller is refused with [`StoreError::WriterBusy`] instead of blocking
    /// inside the engine's writer mutex with no deadline (I15). The handle
    /// releases the right on drop; [`Writes::commit`], [`Writes::abort`] and
    /// [`Writes::durable_commit`] keep it, because they open the next
    /// transaction on the same handle.
    fn write(&self) -> Result<Self::Write<'_>>;

    fn metrics(&self) -> &StoreMetrics;

    /// §11.8: what this node reports in `Status`.
    fn map_usage(&self) -> MapUsage;

    /// Per RAM keyspace: its name, its live rows, and its keys dirty since the
    /// last checkpoint (for the memory gauges). Empty for a store without RAM
    /// keyspaces.
    fn ram_stats(&self) -> Vec<(&'static str, usize, usize)> {
        Vec::new()
    }

    /// The longest key this build accepts (LMDB: 511 B by default).
    fn max_key_len(&self) -> usize;

    /// Flush everything committed so far to the platter without ending the
    /// open write transaction. The durable point (§11.4) uses
    /// [`Writes::durable_commit`]; this is for a caller that holds no handle.
    fn force_sync(&self) -> Result<()>;
}

#[cfg(test)]
mod key_successor_tests {
    use super::*;

    const MAX: usize = 511; // LMDB's default, and what this build reports.

    #[test]
    fn the_prefix_end_is_the_first_key_past_the_range() {
        assert_eq!(prefix_end(b"ab").unwrap(), b"ac".to_vec());
        // Trailing 0xFF bytes fall away: nothing between `a\xFF\xFF` and `b`.
        assert_eq!(prefix_end(&[b'a', 0xFF, 0xFF]).unwrap(), b"b".to_vec());
        // No key sorts above an all-0xFF prefix, so the range has no end.
        assert!(prefix_end(&[0xFF, 0xFF]).is_none());
        assert!(prefix_end(b"").is_none());
    }

    #[test]
    fn a_resume_key_below_the_limit_is_the_zero_successor() {
        let k = b"abc".to_vec();
        let next = resume_after(&k, MAX).unwrap();
        assert_eq!(next, b"abc\x00".to_vec());
        assert!(next > k, "a resume key must be strictly greater");
        assert!(next.len() <= MAX);
    }

    #[test]
    fn a_resume_key_at_the_limit_stays_inside_the_limit() {
        // The defect this function exists for: `key ‖ 0x00` is 512 B, one over
        // what the engine can hold, and every later scan of the chunk loop
        // would be refused with `KeyTooLong` — for good, since the error is
        // neither retryable nor fatal.
        let k = vec![b'x'; MAX];
        let next = resume_after(&k, MAX).expect("there are keys above it");
        assert!(
            next.len() <= MAX,
            "resume key of {} B over the {MAX} B limit",
            next.len()
        );
        assert!(next > k);
        // Nothing storable sits between the two: a key with `k` as a prefix
        // would be longer than the limit.
        assert_eq!(next, {
            let mut e = vec![b'x'; MAX - 1];
            e.push(b'y');
            e
        });
    }

    #[test]
    fn a_max_length_all_ff_key_ends_the_range() {
        let k = vec![0xFFu8; MAX];
        assert!(
            resume_after(&k, MAX).is_none(),
            "no storable key is greater, so the chunk loop is done"
        );
    }
}

#[cfg(test)]
mod map_rule_tests {
    use super::*;

    #[test]
    fn the_default_map_never_goes_below_the_floor() {
        let o = StoreOpts::default();
        assert!(o.map_size_for(0, 4096) >= MIN_MAP_BYTES);
    }

    #[test]
    fn a_configured_map_is_honoured_as_given() {
        // An operator asking for a small map gets one: §11.8's gate and the
        // typed MapFull error are what make that choice visible.
        let o = StoreOpts {
            map_bytes: Some(1 << 20),
            ..Default::default()
        };
        assert_eq!(o.map_size_for(0, 4096), 1 << 20);
    }

    #[test]
    fn the_rule_never_shrinks_a_map_under_its_store() {
        let o = StoreOpts {
            map_bytes: Some(1 << 20),
            ..Default::default()
        };
        let used: u64 = 900 << 10;
        assert_eq!(o.map_size_for(used, 4096), (used as usize) * 4);
    }

    #[test]
    fn rule_grows_for_a_store_already_half_full() {
        let o = StoreOpts {
            map_bytes: Some(MIN_MAP_BYTES),
            ..Default::default()
        };
        // Just under half: the configured size stands.
        assert_eq!(o.map_size_for(400 << 20, 4096), MIN_MAP_BYTES);
        // Over half: four times what is used.
        let used: u64 = 600 << 20;
        assert_eq!(o.map_size_for(used, 4096), (used as usize) * 4);
    }

    #[test]
    fn rule_rounds_up_to_a_page() {
        let o = StoreOpts {
            map_bytes: Some(MIN_MAP_BYTES + 1),
            ..Default::default()
        };
        let got = o.map_size_for(0, 4096);
        assert_eq!(got % 4096, 0);
        assert!(got > MIN_MAP_BYTES);
    }

    #[test]
    fn every_keyspace_that_names_a_local_file_is_node_local() {
        // D8/I7: a `file_id` or a position is this node's, so a keyspace whose
        // KEY or VALUE carries one is never replicated — it would be shipped
        // in an export, compared in a digest, and left behind by an install.
        // `partition_files` is keyed `(pid, file_id)`; PLAN_RAFT.md §6.1 lists
        // it in the replicated table, which the module header answers.
        let node_local = [Keyspace::SegLoc, Keyspace::Files, Keyspace::PartitionFiles];
        for ks in Keyspace::ALL {
            let want = if node_local.contains(&ks) {
                Scope::NodeLocal
            } else {
                Scope::Replicated
            };
            assert_eq!(ks.scope(), want, "{}", ks.name());
        }
    }

    #[test]
    fn names_are_unique_and_slots_match_all() {
        let mut names: Vec<&str> = Keyspace::ALL.iter().map(|k| k.name()).collect();
        names.sort_unstable();
        let n = names.len();
        names.dedup();
        assert_eq!(names.len(), n, "duplicate keyspace name");
        for (i, ks) in Keyspace::ALL.iter().enumerate() {
            assert_eq!(ks.slot(), i, "{} is not at its slot", ks.name());
        }
        assert!(Keyspace::ALL.len() as u32 <= MAX_DBS);
    }
}
