//! The VALUES of the keyspaces: one codec per stored row.
//!
//! The codec is the entry codec's ([`Writer`]/[`Reader`], hand-rolled, no
//! serde), for the same reason: one place writes a field and one place reads
//! it. It is NOT the effect codec: an effect is what the leader proposes and a
//! row is what apply keeps, and the two differ wherever apply knows something
//! the planner cannot.
//!
//! [`GroupRow`] is the example. The `GroupUpsert` effect carries a
//! [`GroupMeta`]; the stored row carries that meta PLUS the (entry index,
//! effect position) of the registration, which §8 makes the anchor of lazy,
//! position-based subscription seeding — and which the planner cannot put in
//! the effect, because the log index does not exist until the library assigns
//! it.
//!
//! Every row starts with a one-byte row version. A row this build cannot read
//! is [`super::StoreError::Corrupt`], which is FATAL for the node: these bytes
//! are what apply wrote from an entry a quorum committed, so skipping them is
//! the silent divergence I16 forbids.

use crate::rsm::effect::{
    CodecError, CursorRow, GarbageScope, GroupMeta, Pid, QueueConfig, QuotaGrant, Reader,
    StreamsQueryRow, SubscriptionMode, TimerRow, TraceEvent, Writer,
};

/// The row layout this build writes. A field added to a row is a new version,
/// decoded beside this one.
pub const ROW_V1: u8 = 1;

/// The second layout of the two rows WP-1.4 had to WIDEN after WP-1.2 shipped
/// them (`files`, `garbage`).
///
/// The version byte is per row, not per build: a row whose body changed takes
/// the next number and everything else stays at [`ROW_V1`]. Keeping the widened
/// bodies at version 1 is what makes a data directory written by the WP-1.2 cut
/// decode its version byte as valid and then mis-read the body — a
/// [`CodecError::Field`] naming some field in the middle of the row instead of
/// the version mismatch that it is. These two say which it is.
pub const ROW_V2: u8 = 2;

fn head(w: &mut Writer) {
    w.u8(ROW_V1);
}

/// The version byte of a row whose body has changed since [`ROW_V1`].
fn head_v2(w: &mut Writer) {
    w.u8(ROW_V2);
}

fn expect_v1(r: &mut Reader<'_>, what: &'static str) -> Result<(), CodecError> {
    expect_row(r, ROW_V1, what)
}

/// The version gate of one row.
///
/// A mismatch is [`CodecError::UnknownVersion`] with `kind: 0` — a STORED ROW,
/// never an effect kind or an outcome tag, which is what a non-zero kind means
/// (§5.3). The caller turns it into `StoreError::Corrupt`, which names the
/// keyspace, so the pair reads "keyspace `files`: unknown version 1". That is
/// fatal for the node, exactly as an unreadable row must be (see the module
/// header): these bytes are what apply wrote from an entry a quorum committed.
fn expect_row(r: &mut Reader<'_>, want: u8, what: &'static str) -> Result<(), CodecError> {
    let v = r.u8(what)?;
    if v != want {
        return Err(CodecError::UnknownVersion {
            kind: 0,
            version: v as u16,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Scalars
// ---------------------------------------------------------------------------

/// A bare `u64` value (`meta`, `partitions_by_key`). No row version: these are
/// fixed-width by definition and a version byte would only be a way to get
/// them wrong.
pub fn u64_encode(v: u64) -> [u8; 8] {
    v.to_le_bytes()
}

pub fn u64_decode(b: &[u8]) -> Result<u64, CodecError> {
    Ok(u64::from_le_bytes(
        b.try_into().map_err(|_| CodecError::Field("u64 value"))?,
    ))
}

pub fn i64_encode(v: i64) -> [u8; 8] {
    v.to_le_bytes()
}

pub fn i64_decode(b: &[u8]) -> Result<i64, CodecError> {
    Ok(i64::from_le_bytes(
        b.try_into().map_err(|_| CodecError::Field("i64 value"))?,
    ))
}

pub fn u32_encode(v: u32) -> [u8; 4] {
    v.to_le_bytes()
}

pub fn u32_decode(b: &[u8]) -> Result<u32, CodecError> {
    Ok(u32::from_le_bytes(
        b.try_into().map_err(|_| CodecError::Field("u32 value"))?,
    ))
}

/// The value of an index row that carries nothing.
pub const UNIT: &[u8] = &[];

// ---------------------------------------------------------------------------
// queues
// ---------------------------------------------------------------------------

pub fn queue_encode(c: &QueueConfig) -> Vec<u8> {
    let mut w = Writer::with_capacity(160);
    head(&mut w);
    w.bytes16(&c.id);
    w.opt_str(c.namespace.as_deref());
    w.opt_str(c.task.as_deref());
    w.i32(c.priority);
    w.i32(c.lease_time);
    w.i32(c.retry_limit);
    w.i32(c.retry_delay);
    w.i32(c.ttl);
    w.bool(c.dead_letter_queue);
    w.bool(c.dlq_after_max_retries);
    w.i32(c.delayed_processing);
    w.i32(c.window_buffer);
    w.i32(c.retention_seconds);
    w.i32(c.completed_retention_seconds);
    w.bool(c.retention_enabled);
    w.bool(c.encryption_enabled);
    w.i32(c.max_wait_time_seconds);
    w.i32(c.max_queue_size);
    w.i32(c.min_pop_wait_time);
    w.i32(c.dedup_window_seconds);
    w.str(&c.retention_sink_hold);
    w.i32(c.retention_sink_hold_max_seconds);
    w.i64(c.created_at_us);
    w.into_inner()
}

pub fn queue_decode(b: &[u8]) -> Result<QueueConfig, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "queue row version")?;
    Ok(QueueConfig {
        id: r.bytes16("queue id")?,
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
        created_at_us: r.i64("created_at_us")?,
    })
}

// ---------------------------------------------------------------------------
// groups
// ---------------------------------------------------------------------------

/// A consumer group as the STORE holds it: the replicated meta plus where its
/// registration sits in the log.
///
/// `reg_index` / `reg_effect` are the (entry index, effect position) §8 calls
/// the registration's position. Subscription seeding is position-based, never
/// time-based: a partition without a cursor for this group is seeded on first
/// use, and `new` means "the last offset appended at a position lower than
/// this one". Apply fills both fields, because only apply knows the index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupRow {
    pub meta: GroupMeta,
    /// The log index of the entry that registered this group.
    pub reg_index: u64,
    /// The position of the `GroupUpsert` effect inside that entry.
    pub reg_effect: u32,
}

pub fn group_encode(g: &GroupRow) -> Vec<u8> {
    let mut w = Writer::with_capacity(96);
    head(&mut w);
    w.bytes16(&g.meta.id);
    w.str(&g.meta.partition_name);
    w.str(&g.meta.namespace);
    w.str(&g.meta.task);
    w.u8(g.meta.mode as u8);
    w.i64(g.meta.subscription_timestamp_us);
    w.bool(g.meta.conflation);
    w.bool(g.meta.seeded);
    w.i64(g.meta.registered_at_us);
    w.u64(g.reg_index);
    w.u32(g.reg_effect);
    w.into_inner()
}

pub fn group_decode(b: &[u8]) -> Result<GroupRow, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "group row version")?;
    let id = r.bytes16("group id")?;
    let partition_name = r.str("partition_name")?;
    let namespace = r.str("namespace")?;
    let task = r.str("task")?;
    let mode = SubscriptionMode::from_u8(r.u8("subscription_mode")?)
        .ok_or(CodecError::Field("subscription_mode"))?;
    let subscription_timestamp_us = r.i64("subscription_timestamp_us")?;
    let conflation = r.bool("conflation")?;
    let seeded = r.bool("seeded")?;
    let registered_at_us = r.i64("registered_at_us")?;
    Ok(GroupRow {
        meta: GroupMeta {
            id,
            partition_name,
            namespace,
            task,
            mode,
            subscription_timestamp_us,
            conflation,
            seeded,
            registered_at_us,
        },
        reg_index: r.u64("reg_index")?,
        reg_effect: r.u32("reg_effect")?,
    })
}

// ---------------------------------------------------------------------------
// partitions
// ---------------------------------------------------------------------------

/// `queen.log_partitions` as the RSM holds it (§6.1).
///
/// There is no `hw` (the pgless "durable and visible tail"): in the RSM a
/// segment exists only because apply executed a COMMITTED `Append`, so
/// `last_offset` IS the visible tail. That distinction was pgless's, where the
/// live path allocated an offset before the write confirmed it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionRow {
    /// `log_partitions.id`, echoed by reads and traces.
    pub uuid: [u8; 16],
    pub tenant: String,
    pub queue: String,
    pub partition: String,
    /// The allocated (and, here, visible) tail. `-1` when empty.
    pub last_offset: i64,
    /// Retention watermark: every offset below it is gone.
    pub log_start: u64,
    /// Dedup watermark: hash lists below it have been pruned. It trails
    /// `log_start`, because the hash lists outlive the segments (D10, §11.7).
    pub txns_start: u64,
    /// `log_partitions.last_write_at`.
    pub last_write_at_us: i64,
    /// The stamp of the oldest live segment: the retention work list's key.
    /// `None` when the partition holds nothing.
    pub oldest_live_at_us: Option<i64>,
    pub created_at_us: i64,
    /// The newest stamp written here, and the FLOOR for the next one:
    /// `created_at = max(now, last_created_at + 1 µs)` (003, PUSHSER).
    pub last_created_at_us: i64,
}

impl PartitionRow {
    /// A fresh partition. The creation stamp is a FLOOR for the first segment,
    /// not a segment stamp, so `last_created_at` starts one microsecond below
    /// it and the first push stamps exactly `now` (ported from pgless
    /// `PartitionState::new`).
    pub fn new(
        uuid: [u8; 16],
        tenant: &str,
        queue: &str,
        partition: &str,
        created_at_us: i64,
    ) -> PartitionRow {
        PartitionRow {
            uuid,
            tenant: tenant.to_string(),
            queue: queue.to_string(),
            partition: partition.to_string(),
            last_offset: -1,
            log_start: 0,
            txns_start: 0,
            last_write_at_us: created_at_us,
            oldest_live_at_us: None,
            created_at_us,
            last_created_at_us: created_at_us - 1,
        }
    }

    /// The first offset a pop can still be served, in the "last acked" form
    /// the cursor arithmetic uses (`log_start - 1`).
    pub fn floor(&self) -> i64 {
        self.log_start as i64 - 1
    }

    /// Pending frames for a cursor at `committed`: the lag arithmetic of
    /// `get_consumer_groups_v4`,
    /// `GREATEST(last_offset - GREATEST(committed, log_start - 1), 0)`.
    pub fn pending_from(&self, committed: i64) -> u64 {
        (self.last_offset - committed.max(self.floor())).max(0) as u64
    }
}

pub fn partition_encode(p: &PartitionRow) -> Vec<u8> {
    let mut w = Writer::with_capacity(96);
    head(&mut w);
    w.bytes16(&p.uuid);
    w.str(&p.tenant);
    w.str(&p.queue);
    w.str(&p.partition);
    w.i64(p.last_offset);
    w.u64(p.log_start);
    w.u64(p.txns_start);
    w.i64(p.last_write_at_us);
    w.opt_i64(p.oldest_live_at_us);
    w.i64(p.created_at_us);
    w.i64(p.last_created_at_us);
    w.into_inner()
}

pub fn partition_decode(b: &[u8]) -> Result<PartitionRow, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "partition row version")?;
    Ok(PartitionRow {
        uuid: r.bytes16("uuid")?,
        tenant: r.str("tenant")?,
        queue: r.str("queue")?,
        partition: r.str("partition")?,
        last_offset: r.i64("last_offset")?,
        log_start: r.u64("log_start")?,
        txns_start: r.u64("txns_start")?,
        last_write_at_us: r.i64("last_write_at_us")?,
        oldest_live_at_us: r.opt_i64("oldest_live_at_us")?,
        created_at_us: r.i64("created_at_us")?,
        last_created_at_us: r.i64("last_created_at_us")?,
    })
}

// ---------------------------------------------------------------------------
// cursors
// ---------------------------------------------------------------------------

/// A cursor row without metadata keeps its [`ROW_V1`] bytes; one that carries
/// a position's metadata is [`ROW_V2`]: the same body with the metadata last.
/// The same split as the effect's (catalogue version 2 of `CursorSet`), for
/// the same reason — the native consumer protocol writes this row on every
/// claim and ack, and its bytes do not change.
pub fn cursor_encode(c: &CursorRow) -> Vec<u8> {
    let mut w = Writer::with_capacity(96 + c.metadata.len());
    if c.metadata.is_empty() {
        head(&mut w);
    } else {
        head_v2(&mut w);
    }
    w.i64(c.committed);
    w.opt_u64(c.batch_end);
    w.opt_str(c.worker.as_deref());
    w.opt_i64(c.lease_expires_at_us);
    w.opt_i64(c.lease_acquired_at_us);
    w.u32(c.batch_retry_count);
    w.opt_u64(c.attempt_offset);
    w.u32(c.attempt_count);
    w.u64(c.total_consumed);
    w.bool(c.lease_conflated);
    w.vec_bytes16(&c.delivered);
    w.i64(c.created_at_us);
    if !c.metadata.is_empty() {
        w.str(&c.metadata);
    }
    w.into_inner()
}

pub fn cursor_decode(b: &[u8]) -> Result<CursorRow, CodecError> {
    let mut r = Reader::new(b);
    let version = r.u8("cursor row version")?;
    if version != ROW_V1 && version != ROW_V2 {
        return Err(CodecError::UnknownVersion {
            kind: 0,
            version: version as u16,
        });
    }
    Ok(CursorRow {
        committed: r.i64("committed")?,
        batch_end: r.opt_u64("batch_end")?,
        worker: r.opt_str("worker")?,
        lease_expires_at_us: r.opt_i64("lease_expires_at_us")?,
        lease_acquired_at_us: r.opt_i64("lease_acquired_at_us")?,
        batch_retry_count: r.u32("batch_retry_count")?,
        attempt_offset: r.opt_u64("attempt_offset")?,
        attempt_count: r.u32("attempt_count")?,
        total_consumed: r.u64("total_consumed")?,
        lease_conflated: r.bool("lease_conflated")?,
        delivered: r.vec_bytes16("delivered")?,
        created_at_us: r.i64("created_at_us")?,
        metadata: if version == ROW_V2 {
            r.str("metadata")?
        } else {
            String::new()
        },
    })
}

/// The claim predicate of 004: `worker_id IS NULL OR lease_expires_at IS NULL
/// OR lease_expires_at < now` is CLAIMABLE, so a live lease is a set worker
/// AND a future expiry (ported from pgless `Cursor::lease_live`).
pub fn lease_live(c: &CursorRow, now_us: i64) -> bool {
    c.worker.is_some() && c.lease_expires_at_us.is_some_and(|e| e > now_us)
}

/// A never-consumed cursor seeded at `committed`.
pub fn cursor_fresh(committed: i64, created_at_us: i64) -> CursorRow {
    CursorRow {
        committed,
        batch_end: None,
        worker: None,
        lease_expires_at_us: None,
        lease_acquired_at_us: None,
        batch_retry_count: 0,
        attempt_offset: None,
        attempt_count: 0,
        total_consumed: 0,
        lease_conflated: false,
        delivered: Vec::new(),
        created_at_us,
        metadata: String::new(),
    }
}

// ---------------------------------------------------------------------------
// dead letters
// ---------------------------------------------------------------------------

/// `queen.log_dlq` minus the key columns (tenant, queue, dlq_id).
///
/// The payload is a SNAPSHOT, never a reference: a dead letter outlives the
/// segment it came from, and retention may have unlinked those bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DlqRow {
    pub pid: Pid,
    pub group: String,
    /// `-1` for a timer's dead letter (025), whose group is `__timer__`.
    pub offset: i64,
    pub message_id: Option<[u8; 16]>,
    pub txn: String,
    pub payload: Vec<u8>,
    pub error: String,
    pub retry_count: u32,
    pub failed_at_us: i64,
}

pub fn dlq_encode(d: &DlqRow) -> Vec<u8> {
    let mut w = Writer::with_capacity(128 + d.payload.len());
    head(&mut w);
    w.u64(d.pid);
    w.str(&d.group);
    w.i64(d.offset);
    w.opt_bytes16(d.message_id.as_ref());
    w.str(&d.txn);
    w.blob(&d.payload);
    w.str(&d.error);
    w.u32(d.retry_count);
    w.i64(d.failed_at_us);
    w.into_inner()
}

pub fn dlq_decode(b: &[u8]) -> Result<DlqRow, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "dlq row version")?;
    Ok(DlqRow {
        pid: r.u64("pid")?,
        group: r.str("group")?,
        offset: r.i64("offset")?,
        message_id: r.opt_bytes16("message_id")?,
        txn: r.str("txn")?,
        payload: r.blob("payload")?,
        error: r.str("error")?,
        retry_count: r.u32("retry_count")?,
        failed_at_us: r.i64("failed_at_us")?,
    })
}

// ---------------------------------------------------------------------------
// garbage
// ---------------------------------------------------------------------------

/// A pid whose name-keyed rows are gone and whose pid-keyed data is still
/// being deleted in chunks (§5.2 rules). Readers and planners ignore it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GarbageRow {
    pub deleted_at_us: i64,
    pub scope: GarbageScope,
    /// Which QUEUE INCARNATION this pid belonged to when it became garbage,
    /// and therefore whose gauges its chunks may still settle.
    ///
    /// §5.2 makes the name reusable the instant the delete lands ("a push,
    /// configure or pop right after the delete recreates it"), while the
    /// pid-keyed rows go on being deleted in chunks for as long as that takes.
    /// A chunk that decided by NAME whether the queue is still there would
    /// subtract a dead queue's dead letters and retained bytes from the live
    /// queue that took the name — and D16 says the counters ARE the answer, so
    /// both gauges stay wrong, and negative, for ever.
    ///
    /// `Some(id)` is the `queues` row's id (a fresh uuid per creation) as it
    /// stood when the pids were moved: its gauges are the right ones while the
    /// live row still carries that id. `None` means the queue's own name-keyed
    /// rows went in the same command — a queue or tenant delete — which
    /// settled its gauges from its own counters and swept them; nothing these
    /// chunks remove may touch them again.
    pub queue_id: Option<[u8; 16]>,
    /// Where the next `DeleteChunk` resumes, in key order. Empty = the
    /// beginning.
    pub resume: Vec<u8>,
}

pub fn garbage_encode(g: &GarbageRow) -> Vec<u8> {
    let mut w = Writer::with_capacity(64);
    head_v2(&mut w);
    w.i64(g.deleted_at_us);
    w.scope(&g.scope);
    w.opt_bytes16(g.queue_id.as_ref());
    w.blob(&g.resume);
    w.into_inner()
}

pub fn garbage_decode(b: &[u8]) -> Result<GarbageRow, CodecError> {
    let mut r = Reader::new(b);
    expect_row(&mut r, ROW_V2, "garbage row version")?;
    Ok(GarbageRow {
        deleted_at_us: r.i64("deleted_at_us")?,
        scope: r.scope("scope")?,
        queue_id: r.opt_bytes16("queue_id")?,
        resume: r.blob("resume")?,
    })
}

// ---------------------------------------------------------------------------
// request ids (D6, §5.4)
// ---------------------------------------------------------------------------

/// `request_id → (now, outcome)`: what a retry of a logged command is answered
/// from (I6). The outcome is framed exactly as it is inside an entry
/// ([`crate::rsm::entry::Outcome::encode`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestIdRow {
    /// The planner's `now` for the entry that logged the command.
    pub now_us: i64,
    pub outcome: Vec<u8>,
}

pub fn request_id_encode(r: &RequestIdRow) -> Vec<u8> {
    let mut w = Writer::with_capacity(32 + r.outcome.len());
    head(&mut w);
    w.i64(r.now_us);
    w.blob(&r.outcome);
    w.into_inner()
}

pub fn request_id_decode(b: &[u8]) -> Result<RequestIdRow, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "request_id row version")?;
    Ok(RequestIdRow {
        now_us: r.i64("now_us")?,
        outcome: r.blob("outcome")?,
    })
}

// ---------------------------------------------------------------------------
// kv (024, WP-2.2)
// ---------------------------------------------------------------------------

/// One `queen.kv` row as the RSM holds it. `(tenant, ns, key)` is the store key
/// ([`super::keys::kv`]); the shard column of 024 has no counterpart (it is a
/// Postgres contention spreader, and the expiry index replaces its one
/// reader).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvRow {
    /// The value JSON, raw (the compact text the receiver serialized). `null`
    /// is a legal value and is NOT an absent row.
    pub value: Vec<u8>,
    /// `kv_version_base + ordinal` of the write that produced this row (I18):
    /// unique across the whole store and never re-issued, so a key that
    /// expired and was recreated cannot hand an old holder its version back.
    pub version: u64,
    /// `None` = forever (an explicit opt-in on the wire, never a default).
    pub expires_at_us: Option<i64>,
    pub created_at_us: i64,
    pub updated_at_us: i64,
}

impl KvRow {
    /// `queen.kv_live_v1`: `expires IS NULL OR expires > now`. A row exactly at
    /// `now` is dead for the reader AND for the sweep — one boundary for the
    /// whole feature (§5.7).
    pub fn live(&self, now_us: i64) -> bool {
        self.expires_at_us.is_none_or(|e| e > now_us)
    }

    /// `queen.kv_ver_v1`: the version under the expiry rule, `0` when expired.
    pub fn effective_version(&self, now_us: i64) -> u64 {
        if self.live(now_us) {
            self.version
        } else {
            0
        }
    }
}

pub fn kv_encode(k: &KvRow) -> Vec<u8> {
    let mut w = Writer::with_capacity(48 + k.value.len());
    head(&mut w);
    w.blob(&k.value);
    w.u64(k.version);
    w.opt_i64(k.expires_at_us);
    w.i64(k.created_at_us);
    w.i64(k.updated_at_us);
    w.into_inner()
}

pub fn kv_decode(b: &[u8]) -> Result<KvRow, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "kv row version")?;
    let row = KvRow {
        value: r.blob("value")?,
        version: r.u64("version")?,
        expires_at_us: r.opt_i64("expires_at")?,
        created_at_us: r.i64("created_at")?,
        updated_at_us: r.i64("updated_at")?,
    };
    if !r.done() {
        return Err(CodecError::Field("kv row trailing bytes"));
    }
    Ok(row)
}

/// The value of a `kv_expiry` row: the [`super::keys::kv`] key of the row it
/// indexes, so the sweep can name the row without a reverse index.
pub fn kv_expiry_encode(kv_key: &[u8]) -> Vec<u8> {
    let mut w = Writer::with_capacity(8 + kv_key.len());
    head(&mut w);
    w.blob(kv_key);
    w.into_inner()
}

pub fn kv_expiry_decode(b: &[u8]) -> Result<Vec<u8>, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "kv_expiry row version")?;
    let key = r.blob("kv key")?;
    if !r.done() {
        return Err(CodecError::Field("kv_expiry row trailing bytes"));
    }
    Ok(key)
}

// ---------------------------------------------------------------------------
// node-local (§6.2)
// ---------------------------------------------------------------------------

/// Where THIS node put a segment's bytes. Node-local: never in an entry, never
/// in a digest, shipped only with the files it indexes (D8, I7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegLocRow {
    pub bucket: u16,
    pub file_id: u32,
    pub offset: u64,
    pub len: u32,
}

pub fn seg_loc_encode(s: &SegLocRow) -> [u8; 18] {
    let mut v = [0u8; 18];
    v[0..2].copy_from_slice(&s.bucket.to_le_bytes());
    v[2..6].copy_from_slice(&s.file_id.to_le_bytes());
    v[6..14].copy_from_slice(&s.offset.to_le_bytes());
    v[14..18].copy_from_slice(&s.len.to_le_bytes());
    v
}

pub fn seg_loc_decode(b: &[u8]) -> Result<SegLocRow, CodecError> {
    if b.len() != 18 {
        return Err(CodecError::Field("seg_loc row"));
    }
    Ok(SegLocRow {
        bucket: u16::from_le_bytes(b[0..2].try_into().unwrap()),
        file_id: u32::from_le_bytes(b[2..6].try_into().unwrap()),
        offset: u64::from_le_bytes(b[6..14].try_into().unwrap()),
        len: u32::from_le_bytes(b[14..18].try_into().unwrap()),
    })
}

/// The value of the `dlq_by_pos` index: the dead letters filed at one
/// `(pid, group, offset)`, in the order they were filed.
///
/// Plain 16-byte ids, no header: the length says how many. Postgres's index on
/// `(partition_id, consumer_group, "offset")` is not unique (005), so this one
/// cannot be either — a replayed dead letter that dies again is filed at the
/// same position, and an index that held the newest made the older row
/// unreachable by every delete path that walks it.
pub fn dlq_ids_encode(ids: &[[u8; 16]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(ids.len() * 16);
    for id in ids {
        out.extend_from_slice(id);
    }
    out
}

pub fn dlq_ids_decode(b: &[u8]) -> Result<Vec<[u8; 16]>, CodecError> {
    if !b.len().is_multiple_of(16) || b.is_empty() {
        return Err(CodecError::Field("dlq id list"));
    }
    Ok(b.chunks_exact(16)
        .map(|c| {
            let mut id = [0u8; 16];
            id.copy_from_slice(c);
            id
        })
        .collect())
}

// ---------------------------------------------------------------------------
// timers (025, WP-2.3)
// ---------------------------------------------------------------------------

/// The instant a timer becomes fireable: `deliver_at`, pushed out by a
/// backoff's `visible_at` (025's generated `visible_at` column:
/// `CASE WHEN claimed_until IS NULL OR claimed_until < deliver_at THEN
/// deliver_at ELSE claimed_until END`). The `timers_due` index is keyed by it.
pub fn timer_due_us(r: &TimerRow) -> i64 {
    match r.visible_at_us {
        Some(v) => v.max(r.deliver_at_us),
        None => r.deliver_at_us,
    }
}

/// The stored `timers` row: the effect's [`TimerRow`] whole. There is nothing
/// apply knows that the planner does not — the fire is one entry, so there is
/// no claim to record (see [`TimerRow`]).
pub fn timer_encode(t: &TimerRow) -> Vec<u8> {
    let mut w = Writer::with_capacity(160 + t.frame.len() + t.txn.len());
    head(&mut w);
    w.str(&t.partition);
    w.i64(t.deliver_at_us);
    w.opt_i64(t.visible_at_us);
    w.blob(&t.frame);
    w.bool(t.payload_zstd);
    w.bool(t.encrypted);
    w.str(&t.txn);
    w.bytes16(&t.message_id);
    w.i32(t.attempts);
    w.opt_str(t.last_error.as_deref());
    w.opt_str(t.producer_sub.as_deref());
    w.i64(t.created_at_us);
    w.i64(t.updated_at_us);
    w.into_inner()
}

pub fn timer_decode(b: &[u8]) -> Result<TimerRow, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "timer row version")?;
    let row = TimerRow {
        partition: r.str("partition")?,
        deliver_at_us: r.i64("deliver_at_us")?,
        visible_at_us: r.opt_i64("visible_at_us")?,
        frame: r.blob("frame")?,
        payload_zstd: r.bool("payload_zstd")?,
        encrypted: r.bool("encrypted")?,
        txn: r.str("txn")?,
        message_id: r.bytes16("message_id")?,
        attempts: r.i32("attempts")?,
        last_error: r.opt_str("last_error")?,
        producer_sub: r.opt_str("producer_sub")?,
        created_at_us: r.i64("created_at_us")?,
        updated_at_us: r.i64("updated_at_us")?,
    };
    if !r.done() {
        return Err(CodecError::Field("timer row trailing bytes"));
    }
    Ok(row)
}

// ---------------------------------------------------------------------------
// phase-2 control plane
// ---------------------------------------------------------------------------

pub fn streams_query_encode(q: &StreamsQueryRow) -> Vec<u8> {
    let mut w = Writer::with_capacity(96);
    head(&mut w);
    w.str(&q.name);
    w.str(&q.source_queue);
    w.opt_str(q.sink_queue.as_deref());
    w.str(&q.config_hash);
    w.i64(q.created_at_us);
    w.i64(q.updated_at_us);
    w.into_inner()
}

pub fn streams_query_decode(b: &[u8]) -> Result<StreamsQueryRow, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "streams query row version")?;
    let row = StreamsQueryRow {
        name: r.str("name")?,
        source_queue: r.str("source_queue")?,
        sink_queue: r.opt_str("sink_queue")?,
        config_hash: r.str("config_hash")?,
        created_at_us: r.i64("created_at_us")?,
        updated_at_us: r.i64("updated_at_us")?,
    };
    if !r.done() {
        return Err(CodecError::Field("streams query row trailing bytes"));
    }
    Ok(row)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamsStateRow {
    pub value: Vec<u8>,
    pub updated_at_us: i64,
}

pub fn streams_state_encode(s: &StreamsStateRow) -> Vec<u8> {
    let mut w = Writer::with_capacity(16 + s.value.len());
    head(&mut w);
    w.blob(&s.value);
    w.i64(s.updated_at_us);
    w.into_inner()
}

pub fn streams_state_decode(b: &[u8]) -> Result<StreamsStateRow, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "streams state row version")?;
    let row = StreamsStateRow {
        value: r.blob("value")?,
        updated_at_us: r.i64("updated_at_us")?,
    };
    if !r.done() {
        return Err(CodecError::Field("streams state row trailing bytes"));
    }
    Ok(row)
}

pub fn quota_encode(q: &QuotaGrant) -> Vec<u8> {
    let mut w = Writer::with_capacity(80);
    head(&mut w);
    w.bool(q.enabled);
    w.opt_i64(q.max_rows);
    w.opt_i64(q.max_bytes);
    w.opt_i64(q.max_timers);
    w.opt_i64(q.max_timer_horizon_s);
    w.opt_i32(q.max_reads_per_sec);
    w.opt_i32(q.max_writes_per_sec);
    w.opt_i32(q.max_queues);
    w.opt_i32(q.max_msgs_per_sec);
    w.opt_i64(q.max_queries);
    w.i64(q.updated_at_us);
    w.into_inner()
}

pub fn quota_decode(b: &[u8]) -> Result<QuotaGrant, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "quota row version")?;
    let row = QuotaGrant {
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
        updated_at_us: r.i64("updated_at_us")?,
    };
    if !r.done() {
        return Err(CodecError::Field("quota row trailing bytes"));
    }
    Ok(row)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EphConfigRow {
    pub options: Vec<u8>,
    pub updated_at_us: i64,
}

pub fn eph_config_encode(e: &EphConfigRow) -> Vec<u8> {
    let mut w = Writer::with_capacity(16 + e.options.len());
    head(&mut w);
    w.blob(&e.options);
    w.i64(e.updated_at_us);
    w.into_inner()
}

pub fn eph_config_decode(b: &[u8]) -> Result<EphConfigRow, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "ephemeral config row version")?;
    let row = EphConfigRow {
        options: r.blob("options")?,
        updated_at_us: r.i64("updated_at_us")?,
    };
    if !r.done() {
        return Err(CodecError::Field("ephemeral config row trailing bytes"));
    }
    Ok(row)
}

pub fn trace_encode(t: &TraceEvent) -> Vec<u8> {
    let mut w = Writer::with_capacity(160 + t.data.len());
    head(&mut w);
    w.bytes16(&t.trace_id);
    w.str(&t.tenant);
    w.opt_u64(t.pid);
    w.opt_bytes16(t.message_id.as_ref());
    w.str(&t.txn);
    w.opt_str(t.consumer_group.as_deref());
    w.str(&t.event_type);
    w.blob(&t.data);
    w.opt_str(t.worker.as_deref());
    w.u32(t.names.len() as u32);
    for name in &t.names {
        w.str(name);
    }
    w.i64(t.created_at_us);
    w.into_inner()
}

pub fn trace_decode(b: &[u8]) -> Result<TraceEvent, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "trace row version")?;
    let trace_id = r.bytes16("trace_id")?;
    let tenant = r.str("tenant")?;
    let pid = r.opt_u64("pid")?;
    let message_id = r.opt_bytes16("message_id")?;
    let txn = r.str("txn")?;
    let consumer_group = r.opt_str("consumer_group")?;
    let event_type = r.str("event_type")?;
    let data = r.blob("data")?;
    let worker = r.opt_str("worker")?;
    let name_count = r.u32("name count")? as usize;
    let mut names = Vec::with_capacity(name_count);
    for _ in 0..name_count {
        names.push(r.str("trace name")?);
    }
    let created_at_us = r.i64("created_at_us")?;
    if !r.done() {
        return Err(CodecError::Field("trace row trailing bytes"));
    }
    Ok(TraceEvent {
        trace_id,
        tenant,
        pid,
        message_id,
        txn,
        consumer_group,
        event_type,
        data,
        worker,
        names,
        created_at_us,
    })
}

/// One segment file as this node knows it (§6.2, I11).
///
/// `len` is the length AT THE LAST STORE COMMIT that recorded it — the number
/// recovery truncates the file to (§11.5 step 3). It is written in the same
/// transaction as `meta.applied_index`, which is the whole of I11.
///
/// The other fields are the file's LIVENESS, and they are here because
/// `segments::Segments::open` refuses to reopen without them (WP-1.3
/// `FileState`): a row that carried only `len` and `sealed` came back from a
/// restart with every counter at zero, which is `FileMeta::is_dead` for every
/// sealed file — a two-phase GC that unlinks unacked payloads, hash lists
/// still inside the txns window and files a live snapshot hard-links (I10,
/// §11.7). The two structs are therefore the same row: WP-1.4's apply thread
/// writes one from the other at every store commit.
///
/// Widening it is what [`ROW_V2`] is for: the WP-1.2 cut wrote
/// `len | sealed | live_bytes | snapshot_refs`, and a data directory from that
/// build must be refused by its VERSION rather than decoded into whatever the
/// new field order makes of those bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileRow {
    pub len: u64,
    /// The length at the last DURABLE point (§11.4), at or below `len`. It is
    /// the floor of recovery's checksum verification: the frames above it are
    /// the ones no barrier has covered (§11.5 step 3).
    pub durable_len: u64,
    pub sealed: bool,
    /// Frames written into it, ever. Never decreases.
    pub frames: u64,
    /// Frames whose payload retention still keeps, and their bytes (§11.7):
    /// the "live bytes per file" a GC candidate must have none of.
    pub retained_frames: u64,
    pub retained_bytes: u64,
    /// Frames whose hash list is still inside their partition's txns window —
    /// the lists that outlive the segments retention deletes (D10).
    pub window_frames: u64,
    /// How many snapshot manifests hold a hard link to it.
    pub snapshot_refs: u32,
}

pub fn file_encode(f: &FileRow) -> Vec<u8> {
    let mut w = Writer::with_capacity(56);
    head_v2(&mut w);
    w.u64(f.len);
    w.u64(f.durable_len);
    w.bool(f.sealed);
    w.u64(f.frames);
    w.u64(f.retained_frames);
    w.u64(f.retained_bytes);
    w.u64(f.window_frames);
    w.u32(f.snapshot_refs);
    w.into_inner()
}

pub fn file_decode(b: &[u8]) -> Result<FileRow, CodecError> {
    let mut r = Reader::new(b);
    expect_row(&mut r, ROW_V2, "file row version")?;
    Ok(FileRow {
        len: r.u64("len")?,
        durable_len: r.u64("durable_len")?,
        sealed: r.bool("sealed")?,
        frames: r.u64("frames")?,
        retained_frames: r.u64("retained_frames")?,
        retained_bytes: r.u64("retained_bytes")?,
        window_frames: r.u64("window_frames")?,
        snapshot_refs: r.u32("snapshot_refs")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rsm::tests::samples;

    #[test]
    fn queue_row_round_trips() {
        let c = samples::queue_config();
        assert_eq!(queue_decode(&queue_encode(&c)).unwrap(), c);
    }

    fn a_group_meta() -> GroupMeta {
        GroupMeta {
            id: samples::uuid(4),
            partition_name: String::new(),
            namespace: "ns".into(),
            task: "t".into(),
            mode: SubscriptionMode::Timestamp,
            subscription_timestamp_us: 1_700_000_000_000_000,
            conflation: true,
            seeded: true,
            registered_at_us: 1_700_000_000_000_001,
        }
    }

    #[test]
    fn group_row_round_trips_with_its_registration_position() {
        let g = GroupRow {
            meta: a_group_meta(),
            reg_index: 4242,
            reg_effect: 7,
        };
        assert_eq!(group_decode(&group_encode(&g)).unwrap(), g);
    }

    #[test]
    fn cursor_row_round_trips() {
        let c = samples::cursor_row();
        assert_eq!(cursor_decode(&cursor_encode(&c)).unwrap(), c);
    }

    #[test]
    fn partition_row_round_trips() {
        let p = PartitionRow::new([9u8; 16], "t", "q", "p0", 1_700_000_000_000_000);
        assert_eq!(partition_decode(&partition_encode(&p)).unwrap(), p);
        assert_eq!(p.last_created_at_us, p.created_at_us - 1);
        assert_eq!(p.floor(), -1);
        assert_eq!(p.pending_from(-1), 0);
    }

    #[test]
    fn dlq_row_round_trips_including_a_timer_dead_letter() {
        let d = DlqRow {
            pid: 3,
            group: "__timer__".into(),
            offset: -1,
            message_id: None,
            txn: "t-1".into(),
            payload: b"{\"a\":1}".to_vec(),
            error: "boom".into(),
            retry_count: 2,
            failed_at_us: 5,
        };
        assert_eq!(dlq_decode(&dlq_encode(&d)).unwrap(), d);
    }

    #[test]
    fn garbage_row_round_trips() {
        let g = GarbageRow {
            deleted_at_us: 11,
            scope: GarbageScope::Group { group: "g".into() },
            queue_id: Some([4u8; 16]),
            resume: vec![1, 2, 3],
        };
        assert_eq!(garbage_decode(&garbage_encode(&g)).unwrap(), g);
        // The other half of the field: the queue went with the command that
        // opened the garbage, so nothing it leaves behind may settle a queue
        // gauge again.
        let g = GarbageRow {
            queue_id: None,
            scope: GarbageScope::Queue,
            ..g
        };
        assert_eq!(garbage_decode(&garbage_encode(&g)).unwrap(), g);
    }

    /// The WP-1.2 cut's `files` row, byte for byte: version 1, then
    /// `len | sealed | live_bytes | snapshot_refs`.
    ///
    /// A data directory written by `be28a050` must be refused by its VERSION.
    /// Decoded as a version-2 row these 22 bytes read `durable_len` out of
    /// `sealed ‖ live_bytes` and then run out of buffer — a
    /// `CodecError::Field("frames")`, which names a field that build never
    /// wrote and says nothing about what actually happened.
    #[test]
    fn the_old_file_row_is_refused_by_its_version_and_not_mis_read() {
        let mut old = Writer::with_capacity(24);
        old.u8(ROW_V1);
        old.u64(1 << 26); // len
        old.bool(true); // sealed
        old.u64(42); // live_bytes, the field WP-1.4 split into four
        old.u32(1); // snapshot_refs
        let old = old.into_inner();
        assert_eq!(old.len(), 22);
        match file_decode(&old) {
            Err(CodecError::UnknownVersion { kind, version }) => {
                assert_eq!((kind, version), (0, ROW_V1 as u16));
            }
            other => panic!("the old layout must be refused by its version: {other:?}"),
        }
        // And this build's own row still round trips, at version 2.
        let f = FileRow {
            len: 1 << 26,
            durable_len: 1 << 25,
            sealed: true,
            frames: 9,
            retained_frames: 3,
            retained_bytes: 42,
            window_frames: 7,
            snapshot_refs: 1,
        };
        let bytes = file_encode(&f);
        assert_eq!(bytes[0], ROW_V2);
        assert_eq!(file_decode(&bytes).unwrap(), f);
    }

    /// The `dlq_by_pos` value did NOT need a version: it never had a header.
    ///
    /// WP-1.2 wrote the 16 raw bytes of one dlq id; WP-1.4 writes the list,
    /// because the postgres index on `(partition_id, consumer_group, "offset")`
    /// is not unique and a second dead letter at one position made the first
    /// unreachable. The widening is compatible by construction — a stored
    /// value of 16 bytes IS a one-element list — and this is the evidence.
    #[test]
    fn an_old_single_id_dlq_index_value_reads_as_a_one_element_list() {
        let id = [9u8; 16];
        assert_eq!(dlq_ids_decode(&id).unwrap(), vec![id]);
        assert_eq!(dlq_ids_encode(&[id]), id.to_vec());
        // Anything that is not a whole number of ids is damage, not a version.
        assert!(matches!(
            dlq_ids_decode(&id[..15]),
            Err(CodecError::Field(_))
        ));
        assert!(matches!(dlq_ids_decode(&[]), Err(CodecError::Field(_))));
    }

    #[test]
    fn node_local_rows_round_trip() {
        let s = SegLocRow {
            bucket: 255,
            file_id: 7,
            offset: 1 << 40,
            len: 4096,
        };
        assert_eq!(seg_loc_decode(&seg_loc_encode(&s)).unwrap(), s);
        let f = FileRow {
            len: 1 << 26,
            durable_len: 1 << 25,
            sealed: true,
            frames: 9,
            retained_frames: 3,
            retained_bytes: 42,
            window_frames: 7,
            snapshot_refs: 1,
        };
        assert_eq!(file_decode(&file_encode(&f)).unwrap(), f);
    }

    #[test]
    fn request_id_row_carries_a_framed_outcome() {
        let o = crate::rsm::entry::Outcome::Empty;
        let row = RequestIdRow {
            now_us: 7,
            outcome: o.encode(),
        };
        let back = request_id_decode(&request_id_encode(&row)).unwrap();
        assert_eq!(back, row);
        assert_eq!(
            crate::rsm::entry::Outcome::decode(&back.outcome).unwrap(),
            o
        );
    }

    #[test]
    fn an_unknown_row_version_is_refused_not_guessed() {
        let mut b = queue_encode(&samples::queue_config());
        b[0] = 99;
        let err = queue_decode(&b).unwrap_err();
        assert!(err.fatal(), "{err:?}");
    }

    #[test]
    fn a_timer_row_round_trips_and_its_due_honours_the_backoff() {
        let mut t = TimerRow {
            partition: "Default".into(),
            deliver_at_us: 100,
            visible_at_us: None,
            frame: vec![1, 2, 3],
            payload_zstd: false,
            encrypted: true,
            txn: "tx".into(),
            message_id: [9u8; 16],
            attempts: 2,
            last_error: Some("boom".into()),
            producer_sub: Some("svc".into()),
            created_at_us: 5,
            updated_at_us: 6,
        };
        assert_eq!(timer_decode(&timer_encode(&t)).unwrap(), t);
        assert_eq!(timer_due_us(&t), 100);
        t.visible_at_us = Some(250);
        assert_eq!(timer_decode(&timer_encode(&t)).unwrap(), t);
        assert_eq!(
            timer_due_us(&t),
            250,
            "a backoff pushes the due instant out"
        );
        t.visible_at_us = Some(50);
        assert_eq!(timer_due_us(&t), 100, "never earlier than deliver_at");
        let mut b = timer_encode(&t);
        b[0] = 42;
        assert!(timer_decode(&b).unwrap_err().fatal());
    }

    #[test]
    fn lease_liveness_matches_004() {
        let mut c = cursor_fresh(-1, 0);
        assert!(!lease_live(&c, 10));
        c.worker = Some("w".into());
        assert!(!lease_live(&c, 10), "a worker with no expiry is claimable");
        c.lease_expires_at_us = Some(5);
        assert!(!lease_live(&c, 10), "an expired lease is claimable");
        c.lease_expires_at_us = Some(20);
        assert!(lease_live(&c, 10));
    }
}
