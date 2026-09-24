//! The effect catalogue and its codec (PLAN_RAFT.md §5.2, §5.3).
//!
//! An EFFECT is a deterministic state change. Entries carry effects, never
//! commands (D3), so a follower never re-runs planning and cannot diverge on a
//! clock, a cache or a hash-map order. `apply` executes effects and knows no
//! semantics (I2).
//!
//! # Wire form
//!
//! One effect, framed:
//!
//! ```text
//! kind:u16 | version:u16 | body_len:u32 | body[body_len]
//! ```
//!
//! Little-endian throughout. Strings are `u32 len | utf8`, byte blobs
//! `u32 len | bytes`, counted vectors `u32 count | items`. The codec is
//! hand-rolled, in the style of the pgless `native/record.rs`
//! (`git show 6e96e228:server/src/native/record.rs`): no serde on disk or on
//! the wire, every field written in one place and read in one place.
//!
//! The body checksum lives one level up, on the entry ([`super::entry`]): an
//! effect never travels alone, and one xxh3 over the whole entry body is
//! cheaper than one per effect.
//!
//! # Kinds and versions (§5.3, D20, I16)
//!
//! Every kind carries `(kind_id, version)`. The version is drawn from ONE
//! catalogue sequence shared by every kind, not from a per-kind counter: a
//! kind introduced later, or an existing kind whose shape changes, takes the
//! next catalogue version. That is what makes the entry header's
//! `kinds_version` — the maximum version used inside — a correct gate: a node
//! that supports catalogue version N can decode every effect in an entry whose
//! `kinds_version` is ≤ N, and the cluster version (§12.8) only rises when
//! every voter supports the new one.
//!
//! Everything in phase 1 is version [`VERSION_1`]. Adding a field to a kind
//! means a NEW version of that kind, decoded beside the old one; it never
//! means editing an existing encoder. A golden fixture changing its bytes is a
//! format change and needs that bump (see `rsm/tests/golden/`).
//!
//! Decoding an unknown kind or an unknown version is
//! [`CodecError::UnknownKind`] / [`CodecError::UnknownVersion`], which
//! [`CodecError::fatal`] reports as "stop this node": I16 forbids skipping an
//! effect this build does not understand.
//!
//! # What is modelled here
//!
//! The whole catalogue of §5.2, so later phases only add versions. The
//! message-path kinds (queues, groups, partitions, appends, cursors, DLQ,
//! watermarks, garbage, the meta kinds) are modelled field by field against
//! the SQL that specifies them. The kinds no phase-1 planner emits — KV,
//! timers, streams, traces, flags, quotas, ephemeral config — carry their
//! fields as the SQL and §6.1 define them, and their planners (phase 2) prove
//! them with conformance tests.

// ---------------------------------------------------------------------------
// Catalogue
// ---------------------------------------------------------------------------

/// The one catalogue version phase 1 ships. See the module header.
pub const VERSION_1: u16 = 1;

/// The highest catalogue version this build can decode and apply. The
/// replicated cluster version (§12.8) may be lower; it never rises above the
/// minimum of every voter's value (D20).
pub const SUPPORTED_KINDS_VERSION: u32 = VERSION_1 as u32;

/// Refuse a length prefix above this before allocating: a corrupt file or a
/// hostile peer must not drive an OOM. Far above
/// `QUEEN_RAFT_ENTRY_MAX_BYTES` (96 MiB, the largest PLANNED command, §5.1),
/// which is the limit that actually governs; this one only keeps a damaged
/// header from being believed.
pub const MAX_BODY_LEN: u32 = 256 * 1024 * 1024;

/// `kind:u16 | version:u16 | body_len:u32`.
pub const EFFECT_HEADER_LEN: usize = 2 + 2 + 4;

/// The most elements a counted vector reserves up front, whatever its length
/// prefix claims. See [`Reader::cap`].
const MAX_CAP_HINT: usize = 4096;

/// The effect kinds of §5.2. Ids are permanent: a retired kind's id is never
/// reused, and a new kind takes the next free number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
pub enum Kind {
    /// Carries nothing. Used by tests and by an entry that must exist without
    /// changing state; the consensus library's own blank entries are NOT this
    /// (they never reach [`super::apply`]).
    Noop = 0,
    QueueUpsert = 1,
    QueueDelete = 2,
    GroupUpsert = 3,
    GroupDelete = 4,
    PartitionCreate = 5,
    PartitionDelete = 6,
    Append = 7,
    CursorSet = 8,
    CursorDelete = 9,
    DlqInsert = 10,
    DlqDelete = 11,
    Watermark = 12,
    KvPut = 13,
    KvDelete = 14,
    TimerUpsert = 15,
    TimerDelete = 16,
    TimerBackoff = 17,
    StreamsQueryUpsert = 18,
    StreamsStatePut = 19,
    StreamsStateDelete = 20,
    TraceAppend = 21,
    TraceExpire = 22,
    FlagSet = 23,
    QuotaSet = 24,
    EphemeralConfigSet = 25,
    EphemeralConfigDelete = 26,
    GarbageAdd = 27,
    DeleteChunk = 28,
    RequestIdsExpire = 29,
    ClusterVersionSet = 30,
    MembershipNote = 31,
    /// Remove every tenant-name-keyed row in one apply transaction. The
    /// tenant's partition-owned rows are retired separately through the
    /// bounded `GarbageAdd` / `DeleteChunk` protocol.
    TenantPurge = 32,
}

/// `Effect` travels between nodes (a follower's prepared command) in its own
/// binary codec: `(kind, version, body)`.
impl serde::Serialize for Effect {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut t = s.serialize_tuple(3)?;
        t.serialize_element(&(self.kind() as u16))?;
        t.serialize_element(&self.version())?;
        t.serialize_element(serde_bytes::Bytes::new(&self.encode_body()))?;
        t.end()
    }
}

impl<'de> serde::Deserialize<'de> for Effect {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Effect, D::Error> {
        let (kind, version, body): (u16, u16, serde_bytes::ByteBuf) =
            serde::Deserialize::deserialize(d)?;
        let kind = Kind::from_u16(kind)
            .ok_or_else(|| serde::de::Error::custom(format!("unknown effect kind {kind}")))?;
        Effect::decode_body(kind, version, &body)
            .map_err(|e| serde::de::Error::custom(format!("effect body: {e:?}")))
    }
}

impl Kind {
    /// `None` = a kind this build does not know (I16: fatal, never skipped).
    pub fn from_u16(v: u16) -> Option<Kind> {
        Some(match v {
            0 => Kind::Noop,
            1 => Kind::QueueUpsert,
            2 => Kind::QueueDelete,
            3 => Kind::GroupUpsert,
            4 => Kind::GroupDelete,
            5 => Kind::PartitionCreate,
            6 => Kind::PartitionDelete,
            7 => Kind::Append,
            8 => Kind::CursorSet,
            9 => Kind::CursorDelete,
            10 => Kind::DlqInsert,
            11 => Kind::DlqDelete,
            12 => Kind::Watermark,
            13 => Kind::KvPut,
            14 => Kind::KvDelete,
            15 => Kind::TimerUpsert,
            16 => Kind::TimerDelete,
            17 => Kind::TimerBackoff,
            18 => Kind::StreamsQueryUpsert,
            19 => Kind::StreamsStatePut,
            20 => Kind::StreamsStateDelete,
            21 => Kind::TraceAppend,
            22 => Kind::TraceExpire,
            23 => Kind::FlagSet,
            24 => Kind::QuotaSet,
            25 => Kind::EphemeralConfigSet,
            26 => Kind::EphemeralConfigDelete,
            27 => Kind::GarbageAdd,
            28 => Kind::DeleteChunk,
            29 => Kind::RequestIdsExpire,
            30 => Kind::ClusterVersionSet,
            31 => Kind::MembershipNote,
            32 => Kind::TenantPurge,
            _ => return None,
        })
    }

    /// The stable name used in metrics, logs and golden fixture file names.
    pub fn name(self) -> &'static str {
        match self {
            Kind::Noop => "noop",
            Kind::QueueUpsert => "queue_upsert",
            Kind::QueueDelete => "queue_delete",
            Kind::GroupUpsert => "group_upsert",
            Kind::GroupDelete => "group_delete",
            Kind::PartitionCreate => "partition_create",
            Kind::PartitionDelete => "partition_delete",
            Kind::Append => "append",
            Kind::CursorSet => "cursor_set",
            Kind::CursorDelete => "cursor_delete",
            Kind::DlqInsert => "dlq_insert",
            Kind::DlqDelete => "dlq_delete",
            Kind::Watermark => "watermark",
            Kind::KvPut => "kv_put",
            Kind::KvDelete => "kv_delete",
            Kind::TimerUpsert => "timer_upsert",
            Kind::TimerDelete => "timer_delete",
            Kind::TimerBackoff => "timer_backoff",
            Kind::StreamsQueryUpsert => "streams_query_upsert",
            Kind::StreamsStatePut => "streams_state_put",
            Kind::StreamsStateDelete => "streams_state_delete",
            Kind::TraceAppend => "trace_append",
            Kind::TraceExpire => "trace_expire",
            Kind::FlagSet => "flag_set",
            Kind::QuotaSet => "quota_set",
            Kind::EphemeralConfigSet => "ephemeral_config_set",
            Kind::EphemeralConfigDelete => "ephemeral_config_delete",
            Kind::GarbageAdd => "garbage_add",
            Kind::DeleteChunk => "delete_chunk",
            Kind::RequestIdsExpire => "request_ids_expire",
            Kind::ClusterVersionSet => "cluster_version_set",
            Kind::MembershipNote => "membership_note",
            Kind::TenantPurge => "tenant_purge",
        }
    }

    /// Every kind, in id order. The golden test walks this, so a kind added
    /// without a fixture fails the build.
    pub const ALL: [Kind; 33] = [
        Kind::Noop,
        Kind::QueueUpsert,
        Kind::QueueDelete,
        Kind::GroupUpsert,
        Kind::GroupDelete,
        Kind::PartitionCreate,
        Kind::PartitionDelete,
        Kind::Append,
        Kind::CursorSet,
        Kind::CursorDelete,
        Kind::DlqInsert,
        Kind::DlqDelete,
        Kind::Watermark,
        Kind::KvPut,
        Kind::KvDelete,
        Kind::TimerUpsert,
        Kind::TimerDelete,
        Kind::TimerBackoff,
        Kind::StreamsQueryUpsert,
        Kind::StreamsStatePut,
        Kind::StreamsStateDelete,
        Kind::TraceAppend,
        Kind::TraceExpire,
        Kind::FlagSet,
        Kind::QuotaSet,
        Kind::EphemeralConfigSet,
        Kind::EphemeralConfigDelete,
        Kind::GarbageAdd,
        Kind::DeleteChunk,
        Kind::RequestIdsExpire,
        Kind::ClusterVersionSet,
        Kind::MembershipNote,
        Kind::TenantPurge,
    ];
}

// ---------------------------------------------------------------------------
// Row types carried by effects
// ---------------------------------------------------------------------------

/// A partition id: `meta.next_pid` at plan time plus the ordinal of the
/// creation inside the entry (I18). Node-independent, unlike a position (D8).
pub type Pid = u64;

/// `queen.queues` as the RSM holds it (§6.1: every column except `storage`
/// and the pgless-only `replication_factor`). `tenant` and `queue` are the
/// keyspace key and live on the effect, not here.
///
/// The integer and boolean columns are NOT nullable here although several are
/// nullable in Postgres: they all carry a DEFAULT, every writer sets them, and
/// the planner resolves the value before the effect is built. WP-2.5 (the
/// configure merge) proves that against `configure_merge_semantics.rs`; if a
/// genuine NULL must survive to the API, it arrives as a new version of
/// [`Kind::QueueUpsert`]. `namespace` and `task` ARE optional: a queue with no
/// namespace is ordinary.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct QueueConfig {
    /// `queen.queues.id`, minted by the planner (uuidv7 bytes).
    pub id: [u8; 16],
    pub namespace: Option<String>,
    pub task: Option<String>,
    pub priority: i32,
    pub lease_time: i32,
    pub retry_limit: i32,
    pub retry_delay: i32,
    pub ttl: i32,
    pub dead_letter_queue: bool,
    pub dlq_after_max_retries: bool,
    pub delayed_processing: i32,
    pub window_buffer: i32,
    pub retention_seconds: i32,
    pub completed_retention_seconds: i32,
    pub retention_enabled: bool,
    pub encryption_enabled: bool,
    pub max_wait_time_seconds: i32,
    pub max_queue_size: i32,
    pub min_pop_wait_time: i32,
    pub dedup_window_seconds: i32,
    /// The S3 sink whose commit pointer floors retention; `""` = off.
    pub retention_sink_hold: String,
    pub retention_sink_hold_max_seconds: i32,
    pub created_at_us: i64,
}

/// `consumer_groups_metadata.subscription_mode`. The SQL's three values; a
/// fourth would be a new version of [`Kind::GroupUpsert`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum SubscriptionMode {
    All = 0,
    New = 1,
    Timestamp = 2,
}

impl SubscriptionMode {
    pub fn from_u8(v: u8) -> Option<SubscriptionMode> {
        Some(match v {
            0 => SubscriptionMode::All,
            1 => SubscriptionMode::New,
            2 => SubscriptionMode::Timestamp,
            _ => return None,
        })
    }
}

/// `consumer_groups_metadata` as the RSM holds it (§6.1). A DISCOVERY
/// registration names no queue: it carries `namespace`/`task` and the effect's
/// queue is empty.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupMeta {
    pub id: [u8; 16],
    /// `''` = the whole queue, as in the SQL.
    pub partition_name: String,
    pub namespace: String,
    pub task: String,
    pub mode: SubscriptionMode,
    pub subscription_timestamp_us: i64,
    pub conflation: bool,
    /// The first-contact seeding of §8 has run for this group. Seeding itself
    /// is lazy and position-based: apply records the registration's
    /// (entry index, effect position) on the group row, which is why no
    /// position field appears HERE — the planner cannot know the index.
    pub seeded: bool,
    pub registered_at_us: i64,
}

/// The full `log_consumers` row of a (partition, group), written whole on
/// every mutation, as pgless wrote it: apply stays a plain overwrite and
/// recovery is order-independent within a partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorRow {
    /// Last acked offset; `-1` = nothing acked. Next wanted is `committed + 1`.
    pub committed: i64,
    /// Inclusive end of the leased batch; `None` = no lease.
    pub batch_end: Option<u64>,
    /// The worker holding the lease. On the wire this IS `leaseId`
    /// (handlers/data.rs ≈1050), so no separate lease id is minted.
    pub worker: Option<String>,
    pub lease_expires_at_us: Option<i64>,
    pub lease_acquired_at_us: Option<i64>,
    pub batch_retry_count: u32,
    pub attempt_offset: Option<u64>,
    pub attempt_count: u32,
    pub total_consumed: u64,
    /// The lease this row holds was a CONFLATING one.
    pub lease_conflated: bool,
    /// O16, ratified at G0: the DELIVERED SET recorded in the claim, bounded
    /// by the batch size. The distinct xxh3_128 transaction hashes actually
    /// delivered (ack_registry.rs stores the same set in RAM today, and states
    /// why the distinct set — not the frame-ordered multiset — is the right
    /// one). Empty when the row holds no lease. It is what lets the ack fast
    /// path be deterministic instead of depending on a RAM map that a failover
    /// loses.
    pub delivered: Vec<[u8; 16]>,
    /// `queen.log_consumers.created_at` (001 ≈233): when this (partition,
    /// group) row was first written.
    ///
    /// It is load-bearing, not bookkeeping. `log_partition_dead_v1`
    /// (006 ≈623) spares an empty, long-idle partition while ANY of its cursor
    /// rows was created inside the cleanup window — `c.created_at >= p_cutoff`
    /// — and for an autoAck-only group that leg is the only one that can
    /// speak: 004 NULLs `lease_acquired_at` on an auto-ack pop and such a row
    /// holds no lease, so both timestamp legs are NULL (006 ≈596). Without
    /// this column WP-2.7's cleanup would delete a partition, its cursors and
    /// their `total_consumed` where the postgres oracle keeps them: a
    /// dual-backend conformance failure (G-1), found by a conformance test two
    /// phases from here at the price of a format change on the highest-rate
    /// effect in the catalogue.
    ///
    /// [`GroupMeta::registered_at_us`] is not a substitute: it is per
    /// (tenant, queue, group), older than the per-partition row, so it would
    /// spare in the permissive direction.
    ///
    /// The row is written WHOLE on every mutation, so the planner carries the
    /// existing value forward and stamps `now` only when it creates the row.
    pub created_at_us: i64,
}

/// `log_timers` as the RSM holds it (§6.1), minus the claim columns: a fire is
/// one command whose apply deletes the rows and appends atomically, so
/// `claimed_until` / `claim_token` (025 ≈778) have nothing to protect.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerRow {
    pub partition: String,
    pub deliver_at_us: i64,
    /// The backoff gate: `None` = due at `deliver_at_us`.
    pub visible_at_us: Option<i64>,
    /// The packed frame, exactly the bytes the push path will carry.
    pub frame: Vec<u8>,
    pub payload_zstd: bool,
    pub encrypted: bool,
    /// The FIXED transaction id of the future frame (mandatory in a schedule).
    pub txn: String,
    /// Minted at SCHEDULE time, so the schedule answers with the id the
    /// delivery will carry.
    pub message_id: [u8; 16],
    /// PERMANENT failures only.
    pub attempts: i32,
    pub last_error: Option<String>,
    /// The authenticated sub of whoever scheduled; never a client field.
    pub producer_sub: Option<String>,
    pub created_at_us: i64,
    pub updated_at_us: i64,
}

/// `queen_streams.queries` (002).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamsQueryRow {
    pub name: String,
    pub source_queue: String,
    pub sink_queue: Option<String>,
    pub config_hash: String,
    pub created_at_us: i64,
    pub updated_at_us: i64,
}

/// One `message_traces` row plus its `message_trace_names` (D18). The
/// keyspace key is (tenant, pid, txn, seq); `seq` is assigned by apply, which
/// is deterministic because apply reads the last seq of that key — the planner
/// cannot know it without reserving a counter it would then have to carry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceEvent {
    pub trace_id: [u8; 16],
    pub tenant: String,
    pub pid: Option<Pid>,
    pub message_id: Option<[u8; 16]>,
    pub txn: String,
    pub consumer_group: Option<String>,
    pub event_type: String,
    /// The `data` JSONB, raw.
    pub data: Vec<u8>,
    pub worker: Option<String>,
    pub names: Vec<String>,
    pub created_at_us: i64,
}

/// Which grant table a [`Kind::QuotaSet`] writes (§6.1 `quotas`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum QuotaKind {
    /// `queen.kv_quota` (024): rows, bytes, timers, horizon, read/write rates.
    Kv = 0,
    /// `queen.ephemeral_quota` (030): bytes, queues, msgs/s.
    Ephemeral = 1,
    /// `queen_streams.quota` (002): queries.
    Streams = 2,
}

impl QuotaKind {
    pub fn from_u8(v: u8) -> Option<QuotaKind> {
        Some(match v {
            0 => QuotaKind::Kv,
            1 => QuotaKind::Ephemeral,
            2 => QuotaKind::Streams,
            _ => return None,
        })
    }
}

/// The union of the three grant tables; `None` = unlimited, as in the SQL,
/// and the ABSENCE of the row is a denial once the grant is required. A field
/// a kind does not use is `None`.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct QuotaGrant {
    pub enabled: bool,
    pub max_rows: Option<i64>,
    pub max_bytes: Option<i64>,
    pub max_timers: Option<i64>,
    pub max_timer_horizon_s: Option<i64>,
    pub max_reads_per_sec: Option<i32>,
    pub max_writes_per_sec: Option<i32>,
    pub max_queues: Option<i32>,
    pub max_msgs_per_sec: Option<i32>,
    pub max_queries: Option<i64>,
    pub updated_at_us: i64,
}

/// What a [`Kind::GarbageAdd`] / [`Kind::DeleteChunk`] pair is allowed to
/// delete under the pids it names (§5.2 rules).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GarbageScope {
    /// A queue delete (013): every pid-keyed row of those partitions —
    /// segments, cursors, DLQ, dedup, counters. Timers and KV are untouched.
    Queue,
    /// A tenant purge (031): as `Queue`, and the tenant's name-keyed rows go
    /// with the command that opened the garbage.
    Tenant,
    /// A consumer group delete (014): only the `(pid, group)` rows.
    Group { group: String },
}

// ---------------------------------------------------------------------------
// The catalogue
// ---------------------------------------------------------------------------

/// One deterministic state change (§5.2). Apply executes these in the order
/// they appear in the entry and nothing else.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Nothing.
    Noop,

    /// Create or replace a queue's configuration (012, and the implicit
    /// creation of 003/004/005/007/025).
    QueueUpsert {
        tenant: String,
        queue: String,
        cfg: QueueConfig,
    },
    /// Remove the name-keyed rows of a queue (013). The pid-keyed data goes in
    /// `DeleteChunk`s behind a `GarbageAdd`, and the NAME is reusable at once.
    QueueDelete { tenant: String, queue: String },

    /// Register or update a consumer group (004 first contact, 014).
    GroupUpsert {
        tenant: String,
        queue: String,
        group: String,
        meta: GroupMeta,
    },
    /// Remove a group's name-keyed row (014).
    GroupDelete {
        tenant: String,
        queue: String,
        group: String,
    },

    /// A new partition (003/004/005/007/025 implicit creation). `pid` is
    /// `entry.pid_base + ordinal` (I18).
    PartitionCreate {
        pid: Pid,
        /// `log_partitions.id`, kept because reads and traces echo it.
        uuid: [u8; 16],
        tenant: String,
        queue: String,
        partition: String,
        created_at_us: i64,
    },
    /// Drop a partition and everything keyed by it (006, 013, 031).
    PartitionDelete { pid: Pid },

    /// Messages appended to a partition (003, 005 txn, 007 sink, 025 fire,
    /// 016 DLQ move). The blob is the survivors' packed frames, concatenated
    /// and RAW (the queue log may store it zstd-compressed — a node-local codec,
    /// `qlog::codec`, never part of the entry) — and `hashes` is `16 * count` bytes in
    /// frame order (the xxh3_128 of each frame's transaction id), which the
    /// dedup index and ack-by-hash both read. The POSITION the bytes end up
    /// at is node-local and never appears here (D8, I7).
    Append {
        pid: Pid,
        /// `xxh3(tenant ␟ queue ␟ partition) % 256`: which segment file group
        /// the payload lands in. Carried so every node files the bytes the
        /// same way without re-hashing names it may not have cached.
        bucket: u16,
        base_offset: u64,
        count: u32,
        created_at_us: i64,
        hashes: Vec<u8>,
        blob: Vec<u8>,
    },

    /// The whole cursor row of a (partition, group) (004 claim/seed, 005
    /// ack/nack/renew/DLQ head, 007, 010 seek, 014).
    CursorSet {
        pid: Pid,
        group: String,
        row: CursorRow,
    },
    /// Remove a cursor row (014, 010).
    CursorDelete { pid: Pid, group: String },

    /// File a dead letter (005 `log_dlq_head_v1`, 025 `log_timers_dlq_v1`,
    /// 016). `tenant` and `queue` are carried, not derived from `pid`, because
    /// they are the primary key of the row: `(tenant, queue, dlq_id)`.
    /// `offset` is `-1` for a timer's DLQ row, whose group is `__timer__`.
    DlqInsert {
        dlq_id: [u8; 16],
        tenant: String,
        queue: String,
        pid: Pid,
        group: String,
        offset: i64,
        message_id: Option<[u8; 16]>,
        txn: String,
        /// The payload JSONB, raw. Present because a dead letter outlives the
        /// segment it came from.
        payload: Vec<u8>,
        error: String,
        retry_count: u32,
        failed_at_us: i64,
    },
    /// Remove a dead letter (016 replay, purge).
    DlqDelete {
        dlq_id: [u8; 16],
        tenant: String,
        queue: String,
    },

    /// Move a partition's watermarks (006 retention, txns purge, max-wait
    /// eviction): `log_start` deletes segments below it, `txns_start` the
    /// dedup hash lists, which outlive the segments (D10).
    Watermark {
        pid: Pid,
        log_start: u64,
        txns_start: u64,
    },

    /// Write a KV row (024, and the KV riders of 005). `version` is
    /// `entry.kv_version_base + ordinal` of the versioned write inside the
    /// entry (I18), so two commands writing one key in one entry get different
    /// versions and a stale `expect` cannot win.
    KvPut {
        tenant: String,
        ns: String,
        key: String,
        /// The value JSONB, raw. `null` is a legal value.
        value: Vec<u8>,
        version: u64,
        /// `None` = forever, an explicit opt-in, never a default.
        expires_at_us: Option<i64>,
        created_at_us: i64,
        updated_at_us: i64,
    },
    /// Remove a KV row (024, and the physical prune of 026).
    KvDelete {
        tenant: String,
        ns: String,
        key: String,
    },

    /// Schedule or reschedule a timer (025 `log_timers_apply_v1`).
    TimerUpsert {
        tenant: String,
        queue: String,
        key: String,
        row: TimerRow,
    },
    /// Cancel a timer, or delete the rows a fire delivered (025).
    TimerDelete {
        tenant: String,
        queue: String,
        key: String,
    },
    /// A permanent failure's backoff: a new visibility, one more attempt, the
    /// error (025 `log_timers_fail_v1`). A row merely backing off stays
    /// cancellable, which is why this is not a `TimerUpsert`.
    TimerBackoff {
        tenant: String,
        queue: String,
        key: String,
        visible_at_us: i64,
        attempts: i32,
        last_error: Option<String>,
        updated_at_us: i64,
    },

    /// Register or update a streams query (008).
    StreamsQueryUpsert {
        query_id: [u8; 16],
        tenant: String,
        row: StreamsQueryRow,
    },
    /// A streams state cell (007). `pid` replaces the SQL's partition uuid.
    StreamsStatePut {
        query_id: [u8; 16],
        pid: Pid,
        key: String,
        value: Vec<u8>,
        updated_at_us: i64,
    },
    /// Drop a streams state cell (007).
    StreamsStateDelete {
        query_id: [u8; 16],
        pid: Pid,
        key: String,
    },

    /// Record a trace event (010 `record_trace_v1`).
    TraceAppend { event: TraceEvent },
    /// Drop every trace created before the cutoff (D18).
    TraceExpire { cutoff_us: i64 },

    /// A `system_state` row: maintenance mode and the ephemeral / kv-timers
    /// switches (maintenance.rs). `value` is the JSONB, raw.
    FlagSet { key: String, value: Vec<u8> },

    /// A quota grant (024 `kv_quota`, 030 `ephemeral_quota`, 002
    /// `queen_streams.quota`).
    QuotaSet {
        kind: QuotaKind,
        tenant: String,
        grant: QuotaGrant,
    },

    /// An ephemeral queue's configuration (030). The engine itself stays in
    /// RAM and outside Raft (D24); only its configuration is replicated.
    EphemeralConfigSet {
        tenant: String,
        queue: String,
        options: Vec<u8>,
        updated_at_us: i64,
    },
    /// Forget an ephemeral queue's configuration (030).
    EphemeralConfigDelete { tenant: String, queue: String },

    /// Move pids to the garbage set (013, 014, 031). Readers and planners
    /// ignore garbage pids from this point, so the name is reusable at once.
    GarbageAdd {
        pids: Vec<Pid>,
        scope: GarbageScope,
        deleted_at_us: i64,
    },
    /// One bounded step of the deletion behind a `GarbageAdd`: delete at most
    /// `limit` pid-keyed rows of `pids` within `scope`, in key order, starting
    /// at `resume`. Apply leaves the next resume key in state; the leader's
    /// loop reads it and proposes the next chunk. `resume` empty = start at
    /// the beginning.
    DeleteChunk {
        pids: Vec<Pid>,
        scope: GarbageScope,
        resume: Vec<u8>,
        limit: u32,
    },

    /// Drop recorded outcomes older than the cutoff (D6).
    RequestIdsExpire { cutoff_us: i64 },

    /// Raise the replicated cluster version (D20, §12.8). Proposed only when
    /// every voter reports support for it.
    ClusterVersionSet { version: u32 },

    /// What this node knows about a member's identity (D21), recorded beside
    /// the library's own membership so a wiped or REVERTED disk is detectable.
    MembershipNote {
        node_id: u64,
        generation: u64,
        disk_uuid: [u8; 16],
        address: String,
    },

    /// Atomically remove every name-keyed resource owned by `tenant` (031).
    /// Partition-owned rows are already hidden by `GarbageAdd` in the same
    /// entry and are reclaimed by bounded `DeleteChunk` entries.
    TenantPurge { tenant: String },
}

/// The counter an effect consumes from the entry header's bases (I18, §5.1).
/// See [`Effect::assigns`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Assigns {
    /// Neither a partition id nor a KV version.
    Nothing,
    /// A partition id, which must be `entry.pid_base + ordinal`.
    Pid(Pid),
    /// A KV version, which must be `entry.kv_version_base + ordinal`.
    KvVersion(u64),
}

impl Effect {
    pub fn kind(&self) -> Kind {
        match self {
            Effect::Noop => Kind::Noop,
            Effect::QueueUpsert { .. } => Kind::QueueUpsert,
            Effect::QueueDelete { .. } => Kind::QueueDelete,
            Effect::GroupUpsert { .. } => Kind::GroupUpsert,
            Effect::GroupDelete { .. } => Kind::GroupDelete,
            Effect::PartitionCreate { .. } => Kind::PartitionCreate,
            Effect::PartitionDelete { .. } => Kind::PartitionDelete,
            Effect::Append { .. } => Kind::Append,
            Effect::CursorSet { .. } => Kind::CursorSet,
            Effect::CursorDelete { .. } => Kind::CursorDelete,
            Effect::DlqInsert { .. } => Kind::DlqInsert,
            Effect::DlqDelete { .. } => Kind::DlqDelete,
            Effect::Watermark { .. } => Kind::Watermark,
            Effect::KvPut { .. } => Kind::KvPut,
            Effect::KvDelete { .. } => Kind::KvDelete,
            Effect::TimerUpsert { .. } => Kind::TimerUpsert,
            Effect::TimerDelete { .. } => Kind::TimerDelete,
            Effect::TimerBackoff { .. } => Kind::TimerBackoff,
            Effect::StreamsQueryUpsert { .. } => Kind::StreamsQueryUpsert,
            Effect::StreamsStatePut { .. } => Kind::StreamsStatePut,
            Effect::StreamsStateDelete { .. } => Kind::StreamsStateDelete,
            Effect::TraceAppend { .. } => Kind::TraceAppend,
            Effect::TraceExpire { .. } => Kind::TraceExpire,
            Effect::FlagSet { .. } => Kind::FlagSet,
            Effect::QuotaSet { .. } => Kind::QuotaSet,
            Effect::EphemeralConfigSet { .. } => Kind::EphemeralConfigSet,
            Effect::EphemeralConfigDelete { .. } => Kind::EphemeralConfigDelete,
            Effect::GarbageAdd { .. } => Kind::GarbageAdd,
            Effect::DeleteChunk { .. } => Kind::DeleteChunk,
            Effect::RequestIdsExpire { .. } => Kind::RequestIdsExpire,
            Effect::ClusterVersionSet { .. } => Kind::ClusterVersionSet,
            Effect::MembershipNote { .. } => Kind::MembershipNote,
            Effect::TenantPurge { .. } => Kind::TenantPurge,
        }
    }

    /// The catalogue version this effect encodes to.
    ///
    /// Everything phase 1 emits is [`VERSION_1`], and the match is EXHAUSTIVE
    /// over [`Kind`] rather than a single constant: with a constant (or a
    /// wildcard arm) a kind added in phase 2 would report catalogue version 1
    /// by default, [`super::entry::catalogue_version_of`] would report 1 for
    /// the entry carrying it, §12.8's cluster-version gate would let it be
    /// proposed, and an older voter — which by D20 must never see that kind —
    /// would take it and stop mid-log with [`CodecError::UnknownKind`]. The
    /// compiler is the gate: a new kind does not build until this match says
    /// which catalogue version it is minted at.
    pub fn version(&self) -> u16 {
        match self.kind() {
            Kind::Noop
            | Kind::QueueUpsert
            | Kind::QueueDelete
            | Kind::GroupUpsert
            | Kind::GroupDelete
            | Kind::PartitionCreate
            | Kind::PartitionDelete
            | Kind::Append
            | Kind::CursorSet
            | Kind::CursorDelete
            | Kind::DlqInsert
            | Kind::DlqDelete
            | Kind::Watermark
            | Kind::KvPut
            | Kind::KvDelete
            | Kind::TimerUpsert
            | Kind::TimerDelete
            | Kind::TimerBackoff
            | Kind::StreamsQueryUpsert
            | Kind::StreamsStatePut
            | Kind::StreamsStateDelete
            | Kind::TraceAppend
            | Kind::TraceExpire
            | Kind::FlagSet
            | Kind::QuotaSet
            | Kind::EphemeralConfigSet
            | Kind::EphemeralConfigDelete
            | Kind::GarbageAdd
            | Kind::DeleteChunk
            | Kind::RequestIdsExpire
            | Kind::ClusterVersionSet
            | Kind::MembershipNote => VERSION_1,
            Kind::TenantPurge => VERSION_1,
        }
    }

    /// What this effect takes from the entry header's counter bases (I18).
    ///
    /// The planner assigns partition ids as `pid_base + ordinal` and KV
    /// versions as `kv_version_base + ordinal of the versioned write`, both
    /// counted over the entry's effects in apply order (§5.1). This is the one
    /// place that says which kinds consume an ordinal;
    /// [`super::entry::Entry::validate`] walks the effects with it and refuses
    /// an entry whose assignments do not match its own header.
    ///
    /// Exhaustive on purpose, for the same reason as [`Effect::version`]: a
    /// later kind that hands out a pid or a KV version must say so here, or an
    /// entry could assign one twice and no check would notice.
    pub fn assigns(&self) -> Assigns {
        match self {
            Effect::PartitionCreate { pid, .. } => Assigns::Pid(*pid),
            Effect::KvPut { version, .. } => Assigns::KvVersion(*version),

            Effect::Noop
            | Effect::QueueUpsert { .. }
            | Effect::QueueDelete { .. }
            | Effect::GroupUpsert { .. }
            | Effect::GroupDelete { .. }
            | Effect::PartitionDelete { .. }
            | Effect::Append { .. }
            | Effect::CursorSet { .. }
            | Effect::CursorDelete { .. }
            | Effect::DlqInsert { .. }
            | Effect::DlqDelete { .. }
            | Effect::Watermark { .. }
            | Effect::KvDelete { .. }
            | Effect::TimerUpsert { .. }
            | Effect::TimerDelete { .. }
            | Effect::TimerBackoff { .. }
            | Effect::StreamsQueryUpsert { .. }
            | Effect::StreamsStatePut { .. }
            | Effect::StreamsStateDelete { .. }
            | Effect::TraceAppend { .. }
            | Effect::TraceExpire { .. }
            | Effect::FlagSet { .. }
            | Effect::QuotaSet { .. }
            | Effect::EphemeralConfigSet { .. }
            | Effect::EphemeralConfigDelete { .. }
            | Effect::GarbageAdd { .. }
            | Effect::DeleteChunk { .. }
            | Effect::RequestIdsExpire { .. }
            | Effect::ClusterVersionSet { .. }
            | Effect::MembershipNote { .. } => Assigns::Nothing,
            Effect::TenantPurge { .. } => Assigns::Nothing,
        }
    }

    /// The cross-field invariants of one effect that the codec can check
    /// without knowing any semantics. Today there is exactly one: an
    /// [`Kind::Append`]'s hash list is 16 bytes per frame, in frame order,
    /// because the dedup index and ack-by-hash index into it by ordinal (D10,
    /// 005).
    ///
    /// The DECODER already refuses a stride that does not match — it cannot
    /// know which of the two fields lied. This is the same check on the ENCODE
    /// side, run by [`super::entry::Entry::validate`], so a planner bug is
    /// refused before the cluster commits an entry no node can read back.
    pub fn check(&self) -> Result<(), CodecError> {
        match self {
            Effect::Append { count, hashes, .. } => {
                if hashes.len() != (*count as usize).saturating_mul(16) {
                    return Err(CodecError::Layout("append hashes stride"));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// The body only, without the `kind | version | len` header.
    pub fn encode_body(&self) -> Vec<u8> {
        let mut w = Writer::with_capacity(self.body_size_hint());
        match self {
            Effect::Noop => {}

            Effect::QueueUpsert { tenant, queue, cfg } => {
                w.str(tenant);
                w.str(queue);
                w.bytes16(&cfg.id);
                w.opt_str(cfg.namespace.as_deref());
                w.opt_str(cfg.task.as_deref());
                w.i32(cfg.priority);
                w.i32(cfg.lease_time);
                w.i32(cfg.retry_limit);
                w.i32(cfg.retry_delay);
                w.i32(cfg.ttl);
                w.bool(cfg.dead_letter_queue);
                w.bool(cfg.dlq_after_max_retries);
                w.i32(cfg.delayed_processing);
                w.i32(cfg.window_buffer);
                w.i32(cfg.retention_seconds);
                w.i32(cfg.completed_retention_seconds);
                w.bool(cfg.retention_enabled);
                w.bool(cfg.encryption_enabled);
                w.i32(cfg.max_wait_time_seconds);
                w.i32(cfg.max_queue_size);
                w.i32(cfg.min_pop_wait_time);
                w.i32(cfg.dedup_window_seconds);
                w.str(&cfg.retention_sink_hold);
                w.i32(cfg.retention_sink_hold_max_seconds);
                w.i64(cfg.created_at_us);
            }
            Effect::QueueDelete { tenant, queue } => {
                w.str(tenant);
                w.str(queue);
            }

            Effect::GroupUpsert {
                tenant,
                queue,
                group,
                meta,
            } => {
                w.str(tenant);
                w.str(queue);
                w.str(group);
                w.bytes16(&meta.id);
                w.str(&meta.partition_name);
                w.str(&meta.namespace);
                w.str(&meta.task);
                w.u8(meta.mode as u8);
                w.i64(meta.subscription_timestamp_us);
                w.bool(meta.conflation);
                w.bool(meta.seeded);
                w.i64(meta.registered_at_us);
            }
            Effect::GroupDelete {
                tenant,
                queue,
                group,
            } => {
                w.str(tenant);
                w.str(queue);
                w.str(group);
            }

            Effect::PartitionCreate {
                pid,
                uuid,
                tenant,
                queue,
                partition,
                created_at_us,
            } => {
                w.u64(*pid);
                w.bytes16(uuid);
                w.str(tenant);
                w.str(queue);
                w.str(partition);
                w.i64(*created_at_us);
            }
            Effect::PartitionDelete { pid } => w.u64(*pid),

            Effect::Append {
                pid,
                bucket,
                base_offset,
                count,
                created_at_us,
                hashes,
                blob,
            } => {
                w.u64(*pid);
                w.u16(*bucket);
                w.u64(*base_offset);
                w.u32(*count);
                w.i64(*created_at_us);
                w.blob(hashes);
                w.blob(blob);
            }

            Effect::CursorSet { pid, group, row } => {
                w.u64(*pid);
                w.str(group);
                w.i64(row.committed);
                w.opt_u64(row.batch_end);
                w.opt_str(row.worker.as_deref());
                w.opt_i64(row.lease_expires_at_us);
                w.opt_i64(row.lease_acquired_at_us);
                w.u32(row.batch_retry_count);
                w.opt_u64(row.attempt_offset);
                w.u32(row.attempt_count);
                w.u64(row.total_consumed);
                w.bool(row.lease_conflated);
                w.vec_bytes16(&row.delivered);
                w.i64(row.created_at_us);
            }
            Effect::CursorDelete { pid, group } => {
                w.u64(*pid);
                w.str(group);
            }

            Effect::DlqInsert {
                dlq_id,
                tenant,
                queue,
                pid,
                group,
                offset,
                message_id,
                txn,
                payload,
                error,
                retry_count,
                failed_at_us,
            } => {
                w.bytes16(dlq_id);
                w.str(tenant);
                w.str(queue);
                w.u64(*pid);
                w.str(group);
                w.i64(*offset);
                w.opt_bytes16(message_id.as_ref());
                w.str(txn);
                w.blob(payload);
                w.str(error);
                w.u32(*retry_count);
                w.i64(*failed_at_us);
            }
            Effect::DlqDelete {
                dlq_id,
                tenant,
                queue,
            } => {
                w.bytes16(dlq_id);
                w.str(tenant);
                w.str(queue);
            }

            Effect::Watermark {
                pid,
                log_start,
                txns_start,
            } => {
                w.u64(*pid);
                w.u64(*log_start);
                w.u64(*txns_start);
            }

            Effect::KvPut {
                tenant,
                ns,
                key,
                value,
                version,
                expires_at_us,
                created_at_us,
                updated_at_us,
            } => {
                w.str(tenant);
                w.str(ns);
                w.str(key);
                w.blob(value);
                w.u64(*version);
                w.opt_i64(*expires_at_us);
                w.i64(*created_at_us);
                w.i64(*updated_at_us);
            }
            Effect::KvDelete { tenant, ns, key } => {
                w.str(tenant);
                w.str(ns);
                w.str(key);
            }

            Effect::TimerUpsert {
                tenant,
                queue,
                key,
                row,
            } => {
                w.str(tenant);
                w.str(queue);
                w.str(key);
                w.str(&row.partition);
                w.i64(row.deliver_at_us);
                w.opt_i64(row.visible_at_us);
                w.blob(&row.frame);
                w.bool(row.payload_zstd);
                w.bool(row.encrypted);
                w.str(&row.txn);
                w.bytes16(&row.message_id);
                w.i32(row.attempts);
                w.opt_str(row.last_error.as_deref());
                w.opt_str(row.producer_sub.as_deref());
                w.i64(row.created_at_us);
                w.i64(row.updated_at_us);
            }
            Effect::TimerDelete { tenant, queue, key } => {
                w.str(tenant);
                w.str(queue);
                w.str(key);
            }
            Effect::TimerBackoff {
                tenant,
                queue,
                key,
                visible_at_us,
                attempts,
                last_error,
                updated_at_us,
            } => {
                w.str(tenant);
                w.str(queue);
                w.str(key);
                w.i64(*visible_at_us);
                w.i32(*attempts);
                w.opt_str(last_error.as_deref());
                w.i64(*updated_at_us);
            }

            Effect::StreamsQueryUpsert {
                query_id,
                tenant,
                row,
            } => {
                w.bytes16(query_id);
                w.str(tenant);
                w.str(&row.name);
                w.str(&row.source_queue);
                w.opt_str(row.sink_queue.as_deref());
                w.str(&row.config_hash);
                w.i64(row.created_at_us);
                w.i64(row.updated_at_us);
            }
            Effect::StreamsStatePut {
                query_id,
                pid,
                key,
                value,
                updated_at_us,
            } => {
                w.bytes16(query_id);
                w.u64(*pid);
                w.str(key);
                w.blob(value);
                w.i64(*updated_at_us);
            }
            Effect::StreamsStateDelete { query_id, pid, key } => {
                w.bytes16(query_id);
                w.u64(*pid);
                w.str(key);
            }

            Effect::TraceAppend { event } => {
                w.bytes16(&event.trace_id);
                w.str(&event.tenant);
                w.opt_u64(event.pid);
                w.opt_bytes16(event.message_id.as_ref());
                w.str(&event.txn);
                w.opt_str(event.consumer_group.as_deref());
                w.str(&event.event_type);
                w.blob(&event.data);
                w.opt_str(event.worker.as_deref());
                w.vec_str(&event.names);
                w.i64(event.created_at_us);
            }
            Effect::TraceExpire { cutoff_us } => w.i64(*cutoff_us),

            Effect::FlagSet { key, value } => {
                w.str(key);
                w.blob(value);
            }

            Effect::QuotaSet {
                kind,
                tenant,
                grant,
            } => {
                w.u8(*kind as u8);
                w.str(tenant);
                w.bool(grant.enabled);
                w.opt_i64(grant.max_rows);
                w.opt_i64(grant.max_bytes);
                w.opt_i64(grant.max_timers);
                w.opt_i64(grant.max_timer_horizon_s);
                w.opt_i32(grant.max_reads_per_sec);
                w.opt_i32(grant.max_writes_per_sec);
                w.opt_i32(grant.max_queues);
                w.opt_i32(grant.max_msgs_per_sec);
                w.opt_i64(grant.max_queries);
                w.i64(grant.updated_at_us);
            }

            Effect::EphemeralConfigSet {
                tenant,
                queue,
                options,
                updated_at_us,
            } => {
                w.str(tenant);
                w.str(queue);
                w.blob(options);
                w.i64(*updated_at_us);
            }
            Effect::EphemeralConfigDelete { tenant, queue } => {
                w.str(tenant);
                w.str(queue);
            }

            Effect::GarbageAdd {
                pids,
                scope,
                deleted_at_us,
            } => {
                w.vec_u64(pids);
                w.scope(scope);
                w.i64(*deleted_at_us);
            }
            Effect::DeleteChunk {
                pids,
                scope,
                resume,
                limit,
            } => {
                w.vec_u64(pids);
                w.scope(scope);
                w.blob(resume);
                w.u32(*limit);
            }

            Effect::RequestIdsExpire { cutoff_us } => w.i64(*cutoff_us),
            Effect::ClusterVersionSet { version } => w.u32(*version),
            Effect::MembershipNote {
                node_id,
                generation,
                disk_uuid,
                address,
            } => {
                w.u64(*node_id);
                w.u64(*generation);
                w.bytes16(disk_uuid);
                w.str(address);
            }
            Effect::TenantPurge { tenant } => w.str(tenant),
        }
        w.into_inner()
    }

    fn body_size_hint(&self) -> usize {
        match self {
            Effect::Append { hashes, blob, .. } => 48 + hashes.len() + blob.len(),
            Effect::DlqInsert {
                payload,
                txn,
                error,
                ..
            } => 128 + payload.len() + txn.len() + error.len(),
            Effect::TimerUpsert { row, .. } => 160 + row.frame.len(),
            Effect::CursorSet { row, .. } => 96 + row.delivered.len() * 16,
            Effect::KvPut { value, key, .. } => 96 + value.len() + key.len(),
            Effect::TraceAppend { event } => 160 + event.data.len(),
            _ => 128,
        }
    }

    /// Decode a body of the given kind and version. Errors name the field that
    /// failed, so a corrupt entry is diagnosable from one log line.
    pub fn decode_body(kind: Kind, version: u16, body: &[u8]) -> Result<Effect, CodecError> {
        if version != VERSION_1 {
            return Err(CodecError::UnknownVersion {
                kind: kind as u16,
                version,
            });
        }
        let mut r = Reader::new(body);
        let eff = match kind {
            Kind::Noop => Effect::Noop,

            Kind::QueueUpsert => Effect::QueueUpsert {
                tenant: r.str("tenant")?,
                queue: r.str("queue")?,
                cfg: QueueConfig {
                    id: r.bytes16("cfg.id")?,
                    namespace: r.opt_str("namespace")?,
                    task: r.opt_str("task")?,
                    priority: r.i32("priority")?,
                    lease_time: r.i32("lease_time")?,
                    retry_limit: r.i32("retry_limit")?,
                    retry_delay: r.i32("retry_delay")?,
                    ttl: r.i32("ttl")?,
                    dead_letter_queue: r.bool("dead_letter_queue")?,
                    dlq_after_max_retries: r.bool("dlq_after_max_retries")?,
                    delayed_processing: r.i32("delayed_processing")?,
                    window_buffer: r.i32("window_buffer")?,
                    retention_seconds: r.i32("retention_seconds")?,
                    completed_retention_seconds: r.i32("completed_retention_seconds")?,
                    retention_enabled: r.bool("retention_enabled")?,
                    encryption_enabled: r.bool("encryption_enabled")?,
                    max_wait_time_seconds: r.i32("max_wait_time_seconds")?,
                    max_queue_size: r.i32("max_queue_size")?,
                    min_pop_wait_time: r.i32("min_pop_wait_time")?,
                    dedup_window_seconds: r.i32("dedup_window_seconds")?,
                    retention_sink_hold: r.str("retention_sink_hold")?,
                    retention_sink_hold_max_seconds: r.i32("retention_sink_hold_max_seconds")?,
                    created_at_us: r.i64("created_at")?,
                },
            },
            Kind::QueueDelete => Effect::QueueDelete {
                tenant: r.str("tenant")?,
                queue: r.str("queue")?,
            },

            Kind::GroupUpsert => Effect::GroupUpsert {
                tenant: r.str("tenant")?,
                queue: r.str("queue")?,
                group: r.str("group")?,
                meta: GroupMeta {
                    id: r.bytes16("meta.id")?,
                    partition_name: r.str("partition_name")?,
                    namespace: r.str("namespace")?,
                    task: r.str("task")?,
                    mode: SubscriptionMode::from_u8(r.u8("mode")?)
                        .ok_or(CodecError::Field("mode"))?,
                    subscription_timestamp_us: r.i64("subscription_timestamp")?,
                    conflation: r.bool("conflation")?,
                    seeded: r.bool("seeded")?,
                    registered_at_us: r.i64("registered_at")?,
                },
            },
            Kind::GroupDelete => Effect::GroupDelete {
                tenant: r.str("tenant")?,
                queue: r.str("queue")?,
                group: r.str("group")?,
            },

            Kind::PartitionCreate => Effect::PartitionCreate {
                pid: r.u64("pid")?,
                uuid: r.bytes16("uuid")?,
                tenant: r.str("tenant")?,
                queue: r.str("queue")?,
                partition: r.str("partition")?,
                created_at_us: r.i64("created_at")?,
            },
            Kind::PartitionDelete => Effect::PartitionDelete { pid: r.u64("pid")? },

            Kind::Append => {
                let pid = r.u64("pid")?;
                let bucket = r.u16("bucket")?;
                let base_offset = r.u64("base_offset")?;
                let count = r.u32("count")?;
                let created_at_us = r.i64("created_at")?;
                let hashes = r.blob("hashes")?;
                let blob = r.blob("blob")?;
                // 16 bytes of hash per frame, frame order: the dedup index and
                // ack-by-hash both index into this by ordinal.
                if hashes.len() != count as usize * 16 {
                    return Err(CodecError::Field("hashes stride"));
                }
                Effect::Append {
                    pid,
                    bucket,
                    base_offset,
                    count,
                    created_at_us,
                    hashes,
                    blob,
                }
            }

            Kind::CursorSet => Effect::CursorSet {
                pid: r.u64("pid")?,
                group: r.str("group")?,
                row: CursorRow {
                    committed: r.i64("committed")?,
                    batch_end: r.opt_u64("batch_end")?,
                    worker: r.opt_str("worker")?,
                    lease_expires_at_us: r.opt_i64("lease_expires_at")?,
                    lease_acquired_at_us: r.opt_i64("lease_acquired_at")?,
                    batch_retry_count: r.u32("batch_retry_count")?,
                    attempt_offset: r.opt_u64("attempt_offset")?,
                    attempt_count: r.u32("attempt_count")?,
                    total_consumed: r.u64("total_consumed")?,
                    lease_conflated: r.bool("lease_conflated")?,
                    delivered: r.vec_bytes16("delivered")?,
                    created_at_us: r.i64("cursor.created_at")?,
                },
            },
            Kind::CursorDelete => Effect::CursorDelete {
                pid: r.u64("pid")?,
                group: r.str("group")?,
            },

            Kind::DlqInsert => Effect::DlqInsert {
                dlq_id: r.bytes16("dlq_id")?,
                tenant: r.str("tenant")?,
                queue: r.str("queue")?,
                pid: r.u64("pid")?,
                group: r.str("group")?,
                offset: r.i64("offset")?,
                message_id: r.opt_bytes16("message_id")?,
                txn: r.str("txn")?,
                payload: r.blob("payload")?,
                error: r.str("error")?,
                retry_count: r.u32("retry_count")?,
                failed_at_us: r.i64("failed_at")?,
            },
            Kind::DlqDelete => Effect::DlqDelete {
                dlq_id: r.bytes16("dlq_id")?,
                tenant: r.str("tenant")?,
                queue: r.str("queue")?,
            },

            Kind::Watermark => Effect::Watermark {
                pid: r.u64("pid")?,
                log_start: r.u64("log_start")?,
                txns_start: r.u64("txns_start")?,
            },

            Kind::KvPut => Effect::KvPut {
                tenant: r.str("tenant")?,
                ns: r.str("ns")?,
                key: r.str("key")?,
                value: r.blob("value")?,
                version: r.u64("version")?,
                expires_at_us: r.opt_i64("expires_at")?,
                created_at_us: r.i64("created_at")?,
                updated_at_us: r.i64("updated_at")?,
            },
            Kind::KvDelete => Effect::KvDelete {
                tenant: r.str("tenant")?,
                ns: r.str("ns")?,
                key: r.str("key")?,
            },

            Kind::TimerUpsert => Effect::TimerUpsert {
                tenant: r.str("tenant")?,
                queue: r.str("queue")?,
                key: r.str("key")?,
                row: TimerRow {
                    partition: r.str("partition")?,
                    deliver_at_us: r.i64("deliver_at")?,
                    visible_at_us: r.opt_i64("visible_at")?,
                    frame: r.blob("frame")?,
                    payload_zstd: r.bool("payload_zstd")?,
                    encrypted: r.bool("encrypted")?,
                    txn: r.str("txn")?,
                    message_id: r.bytes16("message_id")?,
                    attempts: r.i32("attempts")?,
                    last_error: r.opt_str("last_error")?,
                    producer_sub: r.opt_str("producer_sub")?,
                    created_at_us: r.i64("created_at")?,
                    updated_at_us: r.i64("updated_at")?,
                },
            },
            Kind::TimerDelete => Effect::TimerDelete {
                tenant: r.str("tenant")?,
                queue: r.str("queue")?,
                key: r.str("key")?,
            },
            Kind::TimerBackoff => Effect::TimerBackoff {
                tenant: r.str("tenant")?,
                queue: r.str("queue")?,
                key: r.str("key")?,
                visible_at_us: r.i64("visible_at")?,
                attempts: r.i32("attempts")?,
                last_error: r.opt_str("last_error")?,
                updated_at_us: r.i64("updated_at")?,
            },

            Kind::StreamsQueryUpsert => Effect::StreamsQueryUpsert {
                query_id: r.bytes16("query_id")?,
                tenant: r.str("tenant")?,
                row: StreamsQueryRow {
                    name: r.str("name")?,
                    source_queue: r.str("source_queue")?,
                    sink_queue: r.opt_str("sink_queue")?,
                    config_hash: r.str("config_hash")?,
                    created_at_us: r.i64("created_at")?,
                    updated_at_us: r.i64("updated_at")?,
                },
            },
            Kind::StreamsStatePut => Effect::StreamsStatePut {
                query_id: r.bytes16("query_id")?,
                pid: r.u64("pid")?,
                key: r.str("key")?,
                value: r.blob("value")?,
                updated_at_us: r.i64("updated_at")?,
            },
            Kind::StreamsStateDelete => Effect::StreamsStateDelete {
                query_id: r.bytes16("query_id")?,
                pid: r.u64("pid")?,
                key: r.str("key")?,
            },

            Kind::TraceAppend => Effect::TraceAppend {
                event: TraceEvent {
                    trace_id: r.bytes16("trace_id")?,
                    tenant: r.str("tenant")?,
                    pid: r.opt_u64("pid")?,
                    message_id: r.opt_bytes16("message_id")?,
                    txn: r.str("txn")?,
                    consumer_group: r.opt_str("consumer_group")?,
                    event_type: r.str("event_type")?,
                    data: r.blob("data")?,
                    worker: r.opt_str("worker")?,
                    names: r.vec_str("names")?,
                    created_at_us: r.i64("created_at")?,
                },
            },
            Kind::TraceExpire => Effect::TraceExpire {
                cutoff_us: r.i64("cutoff")?,
            },

            Kind::FlagSet => Effect::FlagSet {
                key: r.str("key")?,
                value: r.blob("value")?,
            },

            Kind::QuotaSet => Effect::QuotaSet {
                kind: QuotaKind::from_u8(r.u8("quota kind")?)
                    .ok_or(CodecError::Field("quota kind"))?,
                tenant: r.str("tenant")?,
                grant: QuotaGrant {
                    enabled: r.bool("enabled")?,
                    max_rows: r.opt_i64("max_rows")?,
                    max_bytes: r.opt_i64("max_bytes")?,
                    max_timers: r.opt_i64("max_timers")?,
                    max_timer_horizon_s: r.opt_i64("max_timer_horizon_s")?,
                    max_reads_per_sec: r.opt_i32("max_reads_per_sec")?,
                    max_writes_per_sec: r.opt_i32("max_writes_per_sec")?,
                    max_queues: r.opt_i32("max_queues")?,
                    max_msgs_per_sec: r.opt_i32("max_msgs_per_sec")?,
                    max_queries: r.opt_i64("max_queries")?,
                    updated_at_us: r.i64("updated_at")?,
                },
            },

            Kind::EphemeralConfigSet => Effect::EphemeralConfigSet {
                tenant: r.str("tenant")?,
                queue: r.str("queue")?,
                options: r.blob("options")?,
                updated_at_us: r.i64("updated_at")?,
            },
            Kind::EphemeralConfigDelete => Effect::EphemeralConfigDelete {
                tenant: r.str("tenant")?,
                queue: r.str("queue")?,
            },

            Kind::GarbageAdd => Effect::GarbageAdd {
                pids: r.vec_u64("pids")?,
                scope: r.scope("scope")?,
                deleted_at_us: r.i64("deleted_at")?,
            },
            Kind::DeleteChunk => Effect::DeleteChunk {
                pids: r.vec_u64("pids")?,
                scope: r.scope("scope")?,
                resume: r.blob("resume")?,
                limit: r.u32("limit")?,
            },

            Kind::RequestIdsExpire => Effect::RequestIdsExpire {
                cutoff_us: r.i64("cutoff")?,
            },
            Kind::ClusterVersionSet => Effect::ClusterVersionSet {
                version: r.u32("version")?,
            },
            Kind::MembershipNote => Effect::MembershipNote {
                node_id: r.u64("node_id")?,
                generation: r.u64("generation")?,
                disk_uuid: r.bytes16("disk_uuid")?,
                address: r.str("address")?,
            },
            Kind::TenantPurge => Effect::TenantPurge {
                tenant: r.str("tenant")?,
            },
        };
        if !r.done() {
            return Err(CodecError::Field("trailing bytes"));
        }
        Ok(eff)
    }
}

/// The highest catalogue version used by a set of EFFECTS. An empty set
/// reports 0.
///
/// It is half of the entry header's `kinds_version`: outcomes are versioned
/// from the same one catalogue sequence and count too, so the header's value
/// is [`super::entry::catalogue_version_of`], never this function alone
/// (§5.1, §5.3, I16, D20).
pub fn kinds_version_of(effects: &[Effect]) -> u32 {
    effects
        .iter()
        .map(|e| e.version() as u32)
        .max()
        .unwrap_or(0)
}

/// One effect, framed: `kind:u16 | version:u16 | body_len:u32 | body`.
pub fn encode_effect(e: &Effect) -> Vec<u8> {
    let mut out = Vec::with_capacity(EFFECT_HEADER_LEN + e.body_size_hint());
    write_effect(&mut out, e);
    out
}

/// Append one framed effect to `out` (what the entry encoder uses).
pub fn write_effect(out: &mut Vec<u8>, e: &Effect) {
    let body = e.encode_body();
    out.extend_from_slice(&(e.kind() as u16).to_le_bytes());
    out.extend_from_slice(&e.version().to_le_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
}

/// Like [`write_effect`], but an [`Effect::Append`] is framed with an EMPTY
/// payload (`blob`) while every other field — `pid`, `bucket`, `base_offset`,
/// `count`, `created_at_us` and the whole hash list — is byte-identical to what
/// [`write_effect`] writes. Every non-`Append` effect is written verbatim by
/// [`write_effect`].
///
/// This is the ONE serialization difference the log-native write path
/// introduces (`ALICE_PGLESS_NEWARCH.md` A3b): the payload lives ONCE, in its
/// queue's qlog, so the raft-log entry that references it carries no payload —
/// its `blob` field instead holds the payload's 4-byte FRAME LENGTH (`u32`,
/// little-endian: `frame::encoded_len(count, payload_len)`, i.e. the segment's
/// own `pos.len`).
///
/// Carrying the LENGTH (not the payload) is what makes the replicated digest
/// REPLAY-STABLE (I2): apply computes `RetainedBytes` — a replicated counter —
/// and the length-only `seg_loc` from this same value whether the entry is
/// applied live (the writer hands apply this same length-carrying form) or
/// replayed from the payload-free log, so no path ever reads it off an empty
/// blob. Four bytes is not the payload, so the double-write stays dead (the
/// payload lives ONCE, in the qlog; `tests/replicator.rs` proves a KiB payload is
/// still absent from the raft log). A drift test in `tests/roundtrip.rs` pins the
/// encoding to `write_effect` of the same `Append` built with the 4-byte length
/// as its blob.
pub fn write_effect_payload_free(out: &mut Vec<u8>, e: &Effect) {
    match e {
        Effect::Append {
            pid,
            bucket,
            base_offset,
            count,
            created_at_us,
            hashes,
            blob,
        } => {
            let frame_len = crate::rsm::segments::frame::encoded_len(*count, blob.len()) as u32;
            let mut w = Writer::with_capacity(HEADER_HINT_APPEND + hashes.len());
            w.u64(*pid);
            w.u16(*bucket);
            w.u64(*base_offset);
            w.u32(*count);
            w.i64(*created_at_us);
            w.blob(hashes);
            // The payload lives once, in the qlog; the entry carries its frame
            // LENGTH (4 bytes) so RetainedBytes is identical live and on replay.
            w.blob(&frame_len.to_le_bytes());
            let body = w.into_inner();
            out.extend_from_slice(&(e.kind() as u16).to_le_bytes());
            out.extend_from_slice(&e.version().to_le_bytes());
            out.extend_from_slice(&(body.len() as u32).to_le_bytes());
            out.extend_from_slice(&body);
        }
        other => write_effect(out, other),
    }
}

/// Bytes reserved for an `Append` body before its hashes: the six fixed fields
/// plus the two blob length prefixes.
const HEADER_HINT_APPEND: usize = 8 + 2 + 8 + 4 + 8 + 4 + 4;

/// Decode one framed effect; returns it and the bytes consumed.
///
/// An effect never travels alone: in the product it is decoded inside an entry
/// body whose xxh3 has already matched, and the entry decoder re-classifies
/// what comes back from here with [`CodecError::after_checksum`]. Called
/// directly, on bytes nothing has verified, its [`CodecError::Field`] really
/// can mean damage — which is why the promotion is the caller's, not this
/// function's.
pub fn decode_effect(b: &[u8]) -> Result<(Effect, usize), CodecError> {
    if b.len() < EFFECT_HEADER_LEN {
        return Err(CodecError::Truncated);
    }
    let kind_id = u16::from_le_bytes([b[0], b[1]]);
    let version = u16::from_le_bytes([b[2], b[3]]);
    let body_len = u32::from_le_bytes([b[4], b[5], b[6], b[7]]);
    if body_len > MAX_BODY_LEN {
        return Err(CodecError::Header);
    }
    let total = EFFECT_HEADER_LEN + body_len as usize;
    if b.len() < total {
        return Err(CodecError::Truncated);
    }
    // I16: an unknown kind or version is fatal for this node, never skipped —
    // which is why the length prefix above is NOT used to step over it.
    let kind = Kind::from_u16(kind_id).ok_or(CodecError::UnknownKind(kind_id))?;
    let eff = Effect::decode_body(kind, version, &b[EFFECT_HEADER_LEN..total])?;
    Ok((eff, total))
}

/// The body checksum of an entry (§5.1). One function so encoder and decoder
/// cannot drift.
pub fn checksum(body: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(body)
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CodecError {
    /// A length prefix or a header that cannot be believed.
    Header,
    /// The buffer ends inside the record.
    Truncated,
    /// xxh3 of the body does not match the header.
    Checksum,
    /// A field did not decode; the name is the field. Raised on UNVERIFIED
    /// bytes — a standalone [`decode_effect`], the tail of a file — where it
    /// really can be damage. Inside a body whose checksum has already matched
    /// it is not: the entry decoder promotes it to [`CodecError::Malformed`]
    /// (see [`CodecError::after_checksum`]).
    Field(&'static str),
    /// An effect kind this build does not know (I16).
    UnknownKind(u16),
    /// A known kind, or an outcome tag, at a version this build does not know
    /// (I16, §5.3).
    UnknownVersion { kind: u16, version: u16 },
    /// An outcome tag this build does not know (§5.4).
    UnknownOutcome(u16),
    /// An entry layout version this build does not know (§5.1 `format`).
    UnknownFormat(u16),
    /// An entry declaring an effect-catalogue version above
    /// [`SUPPORTED_KINDS_VERSION`] (§5.3, §12.8): the cluster version rose
    /// past this binary. Refused whole, before any body is read (I16).
    UnknownCatalogue(u32),
    /// The entry's own layout is impossible: a command spanning effects that
    /// are not there, an empty command (§5.1: `effect_count ≥ 1`), an effect
    /// belonging to no command, a header that understates what it carries, a
    /// body past the codec's limit. Raised on a VALUE (by
    /// [`super::entry::Entry::validate`], which the encoder runs) or on
    /// unverified bytes; after a checksum it becomes
    /// [`CodecError::Malformed`].
    Layout(&'static str),
    /// The frame VERIFIED — a believable length prefix and a matching xxh3 —
    /// and the body still did not decode.
    ///
    /// Then these bytes are exactly what the leader wrote and what a quorum
    /// committed: not a torn tail, not corruption this node may repair on its
    /// own. §11.5's rule for damage (truncate the tail; discard the state
    /// directory and take a snapshot) must NOT fire, because truncating or
    /// skipping a COMMITTED entry is the silent divergence I16 forbids and
    /// I11 assumes cannot happen. The node stops instead, which is why
    /// [`CodecError::fatal`] reports it.
    Malformed(&'static str),
}

impl CodecError {
    /// True when the node must STOP rather than skip, truncate or repair.
    ///
    /// Two cases: the bytes are well formed and describe something this build
    /// does not support (the `Unknown*` family, I16), or they passed a
    /// checksum and still did not decode ([`CodecError::Malformed`]), which
    /// means the cluster committed something this node cannot apply.
    /// Everything else is a torn tail or corruption in transit, which recovery
    /// (§11.5) handles.
    pub fn fatal(&self) -> bool {
        matches!(
            self,
            CodecError::UnknownKind(_)
                | CodecError::UnknownVersion { .. }
                | CodecError::UnknownOutcome(_)
                | CodecError::UnknownFormat(_)
                | CodecError::UnknownCatalogue(_)
                | CodecError::Malformed(_)
        )
    }

    /// Re-classify an error raised while decoding a body whose FRAME has
    /// already been verified. Everything that would otherwise read as damage
    /// becomes [`CodecError::Malformed`]; the `Unknown*` family already says
    /// something sharper and is kept.
    ///
    /// The entry decoder runs every post-checksum error through this, so the
    /// distinction is made in one place instead of at every call site that has
    /// to decide whether a record may be thrown away.
    pub fn after_checksum(self) -> CodecError {
        match self {
            CodecError::Field(what) | CodecError::Layout(what) | CodecError::Malformed(what) => {
                CodecError::Malformed(what)
            }
            CodecError::Truncated => CodecError::Malformed("the body ends inside a record"),
            CodecError::Header => CodecError::Malformed("a length prefix inside the body"),
            CodecError::Checksum => CodecError::Malformed("a checksum inside the body"),
            other => other,
        }
    }
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::Header => write!(f, "bad record header"),
            CodecError::Truncated => write!(f, "truncated record"),
            CodecError::Checksum => write!(f, "record checksum mismatch"),
            CodecError::Field(name) => write!(f, "bad record field: {name}"),
            CodecError::UnknownKind(k) => write!(f, "unknown effect kind {k}"),
            CodecError::UnknownVersion { kind, version } => {
                // `kind` is an effect kind id or an outcome tag: one catalogue
                // sequence versions both (§5.3), and one variant reports both.
                write!(
                    f,
                    "unknown version {version} of effect kind or outcome tag {kind}"
                )
            }
            CodecError::UnknownOutcome(t) => write!(f, "unknown outcome tag {t}"),
            CodecError::UnknownFormat(v) => write!(f, "unknown entry format {v}"),
            CodecError::UnknownCatalogue(v) => {
                write!(f, "unknown effect catalogue version {v}")
            }
            CodecError::Layout(what) => write!(f, "bad entry layout: {what}"),
            CodecError::Malformed(what) => {
                write!(f, "malformed record inside a verified body: {what}")
            }
        }
    }
}

impl std::error::Error for CodecError {}

// ---------------------------------------------------------------------------
// Primitive writer / reader
// ---------------------------------------------------------------------------

/// The primitives every effect and entry field is written with. Little-endian;
/// strings, blobs and counted vectors all carry a `u32` prefix (pgless used a
/// `u16` for strings and asserted on overflow — an error string or a KV key is
/// not bounded that tightly here, and a codec must not panic on data).
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub fn with_capacity(n: usize) -> Writer {
        Writer {
            buf: Vec::with_capacity(n),
        }
    }
    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }
    pub fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }
    pub fn bool(&mut self, v: bool) {
        self.buf.push(v as u8);
    }
    pub fn u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    pub fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    pub fn i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    pub fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    pub fn i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    pub fn bytes16(&mut self, v: &[u8; 16]) {
        self.buf.extend_from_slice(v);
    }
    pub fn str(&mut self, s: &str) {
        self.blob(s.as_bytes());
    }
    pub fn blob(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(b);
    }
    pub fn opt_u64(&mut self, v: Option<u64>) {
        match v {
            Some(x) => {
                self.buf.push(1);
                self.u64(x);
            }
            None => self.buf.push(0),
        }
    }
    pub fn opt_i64(&mut self, v: Option<i64>) {
        match v {
            Some(x) => {
                self.buf.push(1);
                self.i64(x);
            }
            None => self.buf.push(0),
        }
    }
    pub fn opt_i32(&mut self, v: Option<i32>) {
        match v {
            Some(x) => {
                self.buf.push(1);
                self.i32(x);
            }
            None => self.buf.push(0),
        }
    }
    pub fn opt_str(&mut self, v: Option<&str>) {
        match v {
            Some(s) => {
                self.buf.push(1);
                self.str(s);
            }
            None => self.buf.push(0),
        }
    }
    pub fn opt_bytes16(&mut self, v: Option<&[u8; 16]>) {
        match v {
            Some(b) => {
                self.buf.push(1);
                self.bytes16(b);
            }
            None => self.buf.push(0),
        }
    }
    pub fn vec_u64(&mut self, v: &[u64]) {
        self.u32(v.len() as u32);
        for x in v {
            self.u64(*x);
        }
    }
    pub fn vec_bytes16(&mut self, v: &[[u8; 16]]) {
        self.u32(v.len() as u32);
        for x in v {
            self.bytes16(x);
        }
    }
    pub fn vec_str(&mut self, v: &[String]) {
        self.u32(v.len() as u32);
        for s in v {
            self.str(s);
        }
    }
    pub fn scope(&mut self, s: &GarbageScope) {
        match s {
            GarbageScope::Queue => self.u8(0),
            GarbageScope::Tenant => self.u8(1),
            GarbageScope::Group { group } => {
                self.u8(2);
                self.str(group);
            }
        }
    }
}

pub struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(b: &'a [u8]) -> Reader<'a> {
        Reader { b, pos: 0 }
    }
    pub fn done(&self) -> bool {
        self.pos == self.b.len()
    }
    pub fn remaining(&self) -> usize {
        self.b.len() - self.pos
    }
    fn take(&mut self, n: usize, field: &'static str) -> Result<&'a [u8], CodecError> {
        if n > self.remaining() {
            return Err(CodecError::Field(field));
        }
        let s = &self.b[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    /// How many items to RESERVE for a counted vector. Every counted vector
    /// sizes its allocation with this: a corrupt `u32` count must not turn
    /// into a multi-gigabyte `Vec::with_capacity` (the classic way a
    /// hand-rolled decoder is made to abort on hostile bytes). Bounded twice —
    /// by what could possibly still fit, and by [`MAX_CAP_HINT`] — and the
    /// vector grows geometrically past it, so a real large entry costs a few
    /// reallocations and nothing else.
    fn cap(&self, count: u32, item_min: usize) -> usize {
        let room = self.remaining() / item_min.max(1);
        (count as usize).min(room).min(MAX_CAP_HINT)
    }
    pub fn u8(&mut self, f: &'static str) -> Result<u8, CodecError> {
        Ok(self.take(1, f)?[0])
    }
    pub fn bool(&mut self, f: &'static str) -> Result<bool, CodecError> {
        match self.u8(f)? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(CodecError::Field(f)),
        }
    }
    pub fn u16(&mut self, f: &'static str) -> Result<u16, CodecError> {
        Ok(u16::from_le_bytes(self.take(2, f)?.try_into().unwrap()))
    }
    pub fn u32(&mut self, f: &'static str) -> Result<u32, CodecError> {
        Ok(u32::from_le_bytes(self.take(4, f)?.try_into().unwrap()))
    }
    pub fn i32(&mut self, f: &'static str) -> Result<i32, CodecError> {
        Ok(i32::from_le_bytes(self.take(4, f)?.try_into().unwrap()))
    }
    pub fn u64(&mut self, f: &'static str) -> Result<u64, CodecError> {
        Ok(u64::from_le_bytes(self.take(8, f)?.try_into().unwrap()))
    }
    pub fn i64(&mut self, f: &'static str) -> Result<i64, CodecError> {
        Ok(i64::from_le_bytes(self.take(8, f)?.try_into().unwrap()))
    }
    pub fn bytes16(&mut self, f: &'static str) -> Result<[u8; 16], CodecError> {
        Ok(self.take(16, f)?.try_into().unwrap())
    }
    pub fn str(&mut self, f: &'static str) -> Result<String, CodecError> {
        let n = self.u32(f)? as usize;
        let s = self.take(n, f)?;
        String::from_utf8(s.to_vec()).map_err(|_| CodecError::Field(f))
    }
    pub fn blob(&mut self, f: &'static str) -> Result<Vec<u8>, CodecError> {
        let n = self.u32(f)? as usize;
        Ok(self.take(n, f)?.to_vec())
    }
    pub fn opt_u64(&mut self, f: &'static str) -> Result<Option<u64>, CodecError> {
        match self.u8(f)? {
            0 => Ok(None),
            1 => Ok(Some(self.u64(f)?)),
            _ => Err(CodecError::Field(f)),
        }
    }
    pub fn opt_i64(&mut self, f: &'static str) -> Result<Option<i64>, CodecError> {
        match self.u8(f)? {
            0 => Ok(None),
            1 => Ok(Some(self.i64(f)?)),
            _ => Err(CodecError::Field(f)),
        }
    }
    pub fn opt_i32(&mut self, f: &'static str) -> Result<Option<i32>, CodecError> {
        match self.u8(f)? {
            0 => Ok(None),
            1 => Ok(Some(self.i32(f)?)),
            _ => Err(CodecError::Field(f)),
        }
    }
    pub fn opt_str(&mut self, f: &'static str) -> Result<Option<String>, CodecError> {
        match self.u8(f)? {
            0 => Ok(None),
            1 => Ok(Some(self.str(f)?)),
            _ => Err(CodecError::Field(f)),
        }
    }
    pub fn opt_bytes16(&mut self, f: &'static str) -> Result<Option<[u8; 16]>, CodecError> {
        match self.u8(f)? {
            0 => Ok(None),
            1 => Ok(Some(self.bytes16(f)?)),
            _ => Err(CodecError::Field(f)),
        }
    }
    pub fn vec_u64(&mut self, f: &'static str) -> Result<Vec<u64>, CodecError> {
        let n = self.u32(f)?;
        let mut v = Vec::with_capacity(self.cap(n, 8));
        for _ in 0..n {
            v.push(self.u64(f)?);
        }
        Ok(v)
    }
    pub fn vec_bytes16(&mut self, f: &'static str) -> Result<Vec<[u8; 16]>, CodecError> {
        let n = self.u32(f)?;
        let mut v = Vec::with_capacity(self.cap(n, 16));
        for _ in 0..n {
            v.push(self.bytes16(f)?);
        }
        Ok(v)
    }
    pub fn vec_str(&mut self, f: &'static str) -> Result<Vec<String>, CodecError> {
        let n = self.u32(f)?;
        let mut v = Vec::with_capacity(self.cap(n, 4));
        for _ in 0..n {
            v.push(self.str(f)?);
        }
        Ok(v)
    }
    pub fn scope(&mut self, f: &'static str) -> Result<GarbageScope, CodecError> {
        match self.u8(f)? {
            0 => Ok(GarbageScope::Queue),
            1 => Ok(GarbageScope::Tenant),
            2 => Ok(GarbageScope::Group {
                group: self.str(f)?,
            }),
            _ => Err(CodecError::Field(f)),
        }
    }
    /// Items of a counted vector whose element size is not fixed; used by the
    /// entry decoder for effects and commands.
    pub(crate) fn cap_hint(&self, count: u32, item_min: usize) -> usize {
        self.cap(count, item_min)
    }
    pub(crate) fn rest(&self) -> &'a [u8] {
        &self.b[self.pos..]
    }
    pub(crate) fn skip(&mut self, n: usize, field: &'static str) -> Result<(), CodecError> {
        self.take(n, field)?;
        Ok(())
    }
}
