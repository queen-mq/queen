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
    CodecError, CursorRow, GarbageScope, GroupMeta, Pid, QueueConfig, Reader, SubscriptionMode,
    Writer,
};

/// The row layout this build writes. A field added to a row is a new version,
/// decoded beside this one.
pub const ROW_V1: u8 = 1;

fn head(w: &mut Writer) {
    w.u8(ROW_V1);
}

fn expect_v1(r: &mut Reader<'_>, what: &'static str) -> Result<(), CodecError> {
    let v = r.u8(what)?;
    if v != ROW_V1 {
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

pub fn cursor_encode(c: &CursorRow) -> Vec<u8> {
    let mut w = Writer::with_capacity(96);
    head(&mut w);
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
    w.into_inner()
}

pub fn cursor_decode(b: &[u8]) -> Result<CursorRow, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "cursor row version")?;
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
    /// Where the next `DeleteChunk` resumes, in key order. Empty = the
    /// beginning.
    pub resume: Vec<u8>,
}

pub fn garbage_encode(g: &GarbageRow) -> Vec<u8> {
    let mut w = Writer::with_capacity(48);
    head(&mut w);
    w.i64(g.deleted_at_us);
    w.scope(&g.scope);
    w.blob(&g.resume);
    w.into_inner()
}

pub fn garbage_decode(b: &[u8]) -> Result<GarbageRow, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "garbage row version")?;
    Ok(GarbageRow {
        deleted_at_us: r.i64("deleted_at_us")?,
        scope: r.scope("scope")?,
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

/// One segment file as this node knows it (§6.2, I11).
///
/// `len` is the length AT THE LAST STORE COMMIT that recorded it — the number
/// recovery truncates the file to (§11.5 step 3). It is written in the same
/// transaction as `meta.applied_index`, which is the whole of I11.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileRow {
    pub len: u64,
    pub sealed: bool,
    /// Bytes still referenced by a retained segment or by a hash list inside
    /// the txns window (§11.7). Zero means the file may be unlinked after the
    /// next durable point that no longer references it (I10).
    pub live_bytes: u64,
    /// How many snapshot manifests hold a hard link to it.
    pub snapshot_refs: u32,
}

pub fn file_encode(f: &FileRow) -> Vec<u8> {
    let mut w = Writer::with_capacity(24);
    head(&mut w);
    w.u64(f.len);
    w.bool(f.sealed);
    w.u64(f.live_bytes);
    w.u32(f.snapshot_refs);
    w.into_inner()
}

pub fn file_decode(b: &[u8]) -> Result<FileRow, CodecError> {
    let mut r = Reader::new(b);
    expect_v1(&mut r, "file row version")?;
    Ok(FileRow {
        len: r.u64("len")?,
        sealed: r.bool("sealed")?,
        live_bytes: r.u64("live_bytes")?,
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
            resume: vec![1, 2, 3],
        };
        assert_eq!(garbage_decode(&garbage_encode(&g)).unwrap(), g);
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
            sealed: true,
            live_bytes: 42,
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
