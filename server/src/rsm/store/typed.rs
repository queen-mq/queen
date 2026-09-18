//! The typed keyspace accessors.
//!
//! [`Reads`] and [`Writes`] are the whole surface an engine implements: five
//! functions over bytes. Everything the planner and apply actually call lives
//! here, as two extension traits with blanket implementations, so that:
//!
//! - a key is built in exactly one place ([`super::keys`]) and a row is
//!   decoded in exactly one place ([`super::rows`]);
//! - a caller cannot reach a keyspace with the wrong key shape, which is what
//!   "typed keyspace accessors" buys over `get(b"...")`;
//! - both handles — the read transaction and the apply thread's open write
//!   transaction — get the same reads, because apply must see its own
//!   uncommitted writes and the planner must not see them at all.
//!
//! A row that does not decode is [`StoreError::Corrupt`]: fatal for this node,
//! never skipped (see [`super::rows`]).

use crate::rsm::effect::{CursorRow, Pid, QueueConfig};

use super::keys::{self, Counter};
use super::rows::{
    self, DlqRow, FileRow, GarbageRow, GroupRow, PartitionRow, RequestIdRow, SegLocRow,
};
use super::{Keyspace, Reads, Result, StoreError, Writes};

fn decode<T>(
    ks: Keyspace,
    v: Option<&[u8]>,
    f: impl FnOnce(&[u8]) -> std::result::Result<T, crate::rsm::effect::CodecError>,
) -> Result<Option<T>> {
    match v {
        None => Ok(None),
        Some(b) => match f(b) {
            Ok(t) => Ok(Some(t)),
            Err(e) => Err(StoreError::corrupt(ks, format!("{e}"))),
        },
    }
}

/// Typed reads over committed state.
pub trait TypedReads: Reads {
    // ----------------------------------------------------------------- meta

    fn meta_u64(&self, name: &[u8]) -> Result<Option<u64>> {
        decode(Keyspace::Meta, self.get_raw(Keyspace::Meta, name)?, |b| {
            rows::u64_decode(b)
        })
    }

    fn meta_i64(&self, name: &[u8]) -> Result<Option<i64>> {
        decode(Keyspace::Meta, self.get_raw(Keyspace::Meta, name)?, |b| {
            rows::i64_decode(b)
        })
    }

    fn meta_u32(&self, name: &[u8]) -> Result<Option<u32>> {
        decode(Keyspace::Meta, self.get_raw(Keyspace::Meta, name)?, |b| {
            rows::u32_decode(b)
        })
    }

    fn meta_blob(&self, name: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.get_raw(Keyspace::Meta, name)?.map(|b| b.to_vec()))
    }

    /// The applied index, `0` when nothing has been applied (§11.4, I11).
    fn applied_index(&self) -> Result<u64> {
        Ok(self.meta_u64(super::meta::APPLIED_INDEX)?.unwrap_or(0))
    }

    fn applied_term(&self) -> Result<u64> {
        Ok(self.meta_u64(super::meta::APPLIED_TERM)?.unwrap_or(0))
    }

    /// The index the last durable point covered (§11.4 step 2).
    fn durable_index(&self) -> Result<u64> {
        Ok(self.meta_u64(super::meta::DURABLE_INDEX)?.unwrap_or(0))
    }

    /// The next partition id the planner may assign (I18).
    fn next_pid(&self) -> Result<u64> {
        Ok(self.meta_u64(super::meta::NEXT_PID)?.unwrap_or(1))
    }

    fn kv_version_next(&self) -> Result<u64> {
        Ok(self.meta_u64(super::meta::KV_VERSION_NEXT)?.unwrap_or(1))
    }

    /// The `now_us` of the last applied entry: the floor the planner's clock
    /// cannot go below (D5, I5).
    fn last_now_us(&self) -> Result<i64> {
        Ok(self.meta_i64(super::meta::LAST_NOW_US)?.unwrap_or(0))
    }

    fn max_created_at_us(&self) -> Result<i64> {
        Ok(self.meta_i64(super::meta::MAX_CREATED_AT_US)?.unwrap_or(0))
    }

    fn cluster_version(&self) -> Result<u32> {
        Ok(self.meta_u32(super::meta::CLUSTER_VERSION)?.unwrap_or(0))
    }

    // --------------------------------------------------------------- queues

    fn queue(&self, tenant: &str, queue: &str) -> Result<Option<QueueConfig>> {
        let k = keys::queues(tenant, queue);
        decode(
            Keyspace::Queues,
            self.get_raw(Keyspace::Queues, &k)?,
            rows::queue_decode,
        )
    }

    /// Every queue of a tenant, in name order, into `cb(queue_name, config)`.
    fn scan_queues(
        &self,
        tenant: &str,
        limit: usize,
        cb: &mut dyn FnMut(&str, QueueConfig) -> bool,
    ) -> Result<usize> {
        let prefix = keys::queues_prefix(tenant);
        let mut err: Option<StoreError> = None;
        let n = self.scan_raw(Keyspace::Queues, &prefix, &prefix, limit, &mut |k, v| {
            let name = match keys::read_name(k, prefix.len()) {
                Some((n, _)) => n,
                None => {
                    err = Some(StoreError::corrupt(Keyspace::Queues, "queue name in key"));
                    return false;
                }
            };
            match rows::queue_decode(v) {
                Ok(c) => cb(&name, c),
                Err(e) => {
                    err = Some(StoreError::corrupt(Keyspace::Queues, format!("{e}")));
                    false
                }
            }
        })?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    // --------------------------------------------------------------- groups

    fn group(&self, tenant: &str, queue: &str, group: &str) -> Result<Option<GroupRow>> {
        let k = keys::groups(tenant, queue, group);
        decode(
            Keyspace::Groups,
            self.get_raw(Keyspace::Groups, &k)?,
            rows::group_decode,
        )
    }

    /// Every group of a queue, in name order. The pop path needs the whole set
    /// to maintain `pending` on an `Append` (O(groups), §6.1).
    fn scan_groups(
        &self,
        tenant: &str,
        queue: &str,
        limit: usize,
        cb: &mut dyn FnMut(&str, GroupRow) -> bool,
    ) -> Result<usize> {
        let prefix = keys::groups_prefix(tenant, queue);
        let mut err: Option<StoreError> = None;
        let n = self.scan_raw(Keyspace::Groups, &prefix, &prefix, limit, &mut |k, v| {
            let name = match keys::read_name(k, prefix.len()) {
                Some((n, _)) => n,
                None => {
                    err = Some(StoreError::corrupt(Keyspace::Groups, "group name in key"));
                    return false;
                }
            };
            match rows::group_decode(v) {
                Ok(g) => cb(&name, g),
                Err(e) => {
                    err = Some(StoreError::corrupt(Keyspace::Groups, format!("{e}")));
                    false
                }
            }
        })?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    // ----------------------------------------------------------- partitions

    fn partition(&self, pid: Pid) -> Result<Option<PartitionRow>> {
        let k = keys::pid(pid);
        decode(
            Keyspace::Partitions,
            self.get_raw(Keyspace::Partitions, &k)?,
            rows::partition_decode,
        )
    }

    /// The name index: `(tenant, queue, partition) → pid`.
    fn pid_of(&self, tenant: &str, queue: &str, partition: &str) -> Result<Option<Pid>> {
        let k = keys::partitions_by_key(tenant, queue, partition);
        decode(
            Keyspace::PartitionsByKey,
            self.get_raw(Keyspace::PartitionsByKey, &k)?,
            rows::u64_decode,
        )
    }

    /// Every partition of a queue, in pid order: the wildcard and admin scan.
    fn scan_queue_partitions(
        &self,
        tenant: &str,
        queue: &str,
        from_pid: Option<Pid>,
        limit: usize,
        cb: &mut dyn FnMut(Pid) -> bool,
    ) -> Result<usize> {
        let prefix = keys::queue_partitions_prefix(tenant, queue);
        let from = match from_pid {
            Some(p) => keys::queue_partitions(tenant, queue, p),
            None => prefix.clone(),
        };
        let mut err: Option<StoreError> = None;
        let n = self.scan_raw(
            Keyspace::QueuePartitions,
            &from,
            &prefix,
            limit,
            &mut |k, _v| match keys::queue_partitions_pid_of(k) {
                Some(p) => cb(p),
                None => {
                    err = Some(StoreError::corrupt(Keyspace::QueuePartitions, "pid in key"));
                    false
                }
            },
        )?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    /// Whether a pid is in the garbage set: readers and planners ignore it
    /// (§5.2 rules), so every lookup that starts from a name must check it.
    fn garbage(&self, pid: Pid) -> Result<Option<GarbageRow>> {
        let k = keys::pid(pid);
        decode(
            Keyspace::Garbage,
            self.get_raw(Keyspace::Garbage, &k)?,
            rows::garbage_decode,
        )
    }

    /// The sealed segment files that hold data of a partition, in file order
    /// (§6.1, G0 amendment). NODE-LOCAL: the file ids are this node's, so these
    /// rows are never in a digest and never shipped (D8, I7; see the
    /// [`super`] module header on the plan's wording).
    fn scan_partition_files(
        &self,
        pid: Pid,
        limit: usize,
        cb: &mut dyn FnMut(u32) -> bool,
    ) -> Result<usize> {
        let prefix = keys::partition_files_prefix(pid);
        let mut err: Option<StoreError> = None;
        let n = self.scan_raw(
            Keyspace::PartitionFiles,
            &prefix,
            &prefix,
            limit,
            &mut |k, _v| match keys::partition_files_file_of(k) {
                Some(f) => cb(f),
                None => {
                    err = Some(StoreError::corrupt(
                        Keyspace::PartitionFiles,
                        "file id in key",
                    ));
                    false
                }
            },
        )?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    // -------------------------------------------------------------- cursors

    fn cursor(&self, pid: Pid, group: &str) -> Result<Option<CursorRow>> {
        let k = keys::cursors(pid, group);
        decode(
            Keyspace::Cursors,
            self.get_raw(Keyspace::Cursors, &k)?,
            rows::cursor_decode,
        )
    }

    /// Every group's cursor on one partition, in group-name order.
    fn scan_cursors(
        &self,
        pid: Pid,
        limit: usize,
        cb: &mut dyn FnMut(&str, CursorRow) -> bool,
    ) -> Result<usize> {
        let prefix = keys::cursors_prefix(pid);
        let mut err: Option<StoreError> = None;
        let n = self.scan_raw(Keyspace::Cursors, &prefix, &prefix, limit, &mut |k, v| {
            let g = match keys::cursors_group_of(k) {
                Some(g) => g,
                None => {
                    err = Some(StoreError::corrupt(Keyspace::Cursors, "group in key"));
                    return false;
                }
            };
            match rows::cursor_decode(v) {
                Ok(c) => cb(&g, c),
                Err(e) => {
                    err = Some(StoreError::corrupt(Keyspace::Cursors, format!("{e}")));
                    false
                }
            }
        })?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    /// One worker's live leases, in `(pid, group)` order — the iteration
    /// `log_renew_lease_v1` needs and which a hash map could not give (§8).
    fn scan_worker_leases(
        &self,
        worker: &str,
        limit: usize,
        cb: &mut dyn FnMut(Pid, &str, i64) -> bool,
    ) -> Result<usize> {
        let prefix = keys::leases_by_worker_prefix(worker);
        let mut err: Option<StoreError> = None;
        let n = self.scan_raw(
            Keyspace::LeasesByWorker,
            &prefix,
            &prefix,
            limit,
            &mut |k, v| {
                let parts = keys::leases_by_worker_parts(k);
                let at = rows::i64_decode(v);
                match (parts, at) {
                    (Some((_w, pid, g)), Ok(at)) => cb(pid, &g, at),
                    _ => {
                        err = Some(StoreError::corrupt(Keyspace::LeasesByWorker, "lease row"));
                        false
                    }
                }
            },
        )?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    /// `pending`: when this partition is next worth looking at for this group.
    fn pending_at(&self, tenant: &str, queue: &str, group: &str, pid: Pid) -> Result<Option<i64>> {
        let k = keys::pending(tenant, queue, group, pid);
        decode(
            Keyspace::Pending,
            self.get_raw(Keyspace::Pending, &k)?,
            rows::i64_decode,
        )
    }

    /// Every `pending` row, in key order: the O(pending) rebuild of the ready
    /// rings (§6.3). `cb` gets (tenant, queue, group, pid, ready_at_us).
    fn scan_pending(
        &self,
        from: &[u8],
        limit: usize,
        cb: &mut dyn FnMut(&str, &str, &str, Pid, i64) -> bool,
    ) -> Result<usize> {
        let mut err: Option<StoreError> = None;
        let n = self.scan_raw(Keyspace::Pending, from, &[], limit, &mut |k, v| match (
            keys::pending_parts(k),
            rows::i64_decode(v),
        ) {
            (Some((t, q, g, pid)), Ok(at)) => cb(&t, &q, &g, pid, at),
            _ => {
                err = Some(StoreError::corrupt(Keyspace::Pending, "pending row"));
                false
            }
        })?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    // ---------------------------------------------------------- dead letters

    fn dlq(&self, tenant: &str, queue: &str, dlq_id: &[u8; 16]) -> Result<Option<DlqRow>> {
        let k = keys::dlq(tenant, queue, dlq_id);
        decode(
            Keyspace::Dlq,
            self.get_raw(Keyspace::Dlq, &k)?,
            rows::dlq_decode,
        )
    }

    /// The `(pid, group, offset)` index → the dead letter's id.
    fn dlq_id_at(&self, pid: Pid, group: &str, offset: i64) -> Result<Option<[u8; 16]>> {
        let k = keys::dlq_by_pos(pid, group, offset);
        match self.get_raw(Keyspace::DlqByPos, &k)? {
            None => Ok(None),
            Some(b) => b
                .try_into()
                .map(Some)
                .map_err(|_| StoreError::corrupt(Keyspace::DlqByPos, "dlq id")),
        }
    }

    // ----------------------------------------------------------- request ids

    /// The recorded outcome of a command, if its id is still in the window
    /// (D6, I6).
    fn request_outcome(&self, id: &[u8; 16]) -> Result<Option<RequestIdRow>> {
        let k = keys::request_ids(id);
        decode(
            Keyspace::RequestIds,
            self.get_raw(Keyspace::RequestIds, &k)?,
            rows::request_id_decode,
        )
    }

    /// Request ids oldest first, for `RequestIdsExpire` (D6).
    fn scan_request_expiry(
        &self,
        limit: usize,
        cb: &mut dyn FnMut(i64, [u8; 16]) -> bool,
    ) -> Result<usize> {
        let mut err: Option<StoreError> = None;
        let n = self.scan_raw(Keyspace::RequestExpiry, &[], &[], limit, &mut |k, _v| {
            match keys::request_expiry_parts(k) {
                Some((at, id)) => cb(at, id),
                None => {
                    err = Some(StoreError::corrupt(Keyspace::RequestExpiry, "expiry key"));
                    false
                }
            }
        })?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    // ------------------------------------------------------------- counters

    /// A counter, `0` when it has never been written (D16).
    fn counter_at(&self, key: &[u8]) -> Result<i64> {
        Ok(decode(
            Keyspace::Counters,
            self.get_raw(Keyspace::Counters, key)?,
            rows::i64_decode,
        )?
        .unwrap_or(0))
    }

    fn partition_counter(&self, pid: Pid, c: Counter) -> Result<i64> {
        self.counter_at(&keys::counter_partition(pid, c))
    }

    fn queue_counter(&self, tenant: &str, queue: &str, c: Counter) -> Result<i64> {
        self.counter_at(&keys::counter_queue(tenant, queue, c))
    }

    fn tenant_counter(&self, tenant: &str, c: Counter) -> Result<i64> {
        self.counter_at(&keys::counter_tenant(tenant, c))
    }

    fn group_counter(&self, tenant: &str, queue: &str, group: &str, c: Counter) -> Result<i64> {
        self.counter_at(&keys::counter_group(tenant, queue, group, c))
    }

    // ----------------------------------------------------------- node-local

    fn seg_loc(&self, pid: Pid, base_offset: u64) -> Result<Option<SegLocRow>> {
        let k = keys::seg_loc(pid, base_offset);
        decode(
            Keyspace::SegLoc,
            self.get_raw(Keyspace::SegLoc, &k)?,
            rows::seg_loc_decode,
        )
    }

    /// A partition's positions, in offset order.
    fn scan_seg_loc(
        &self,
        pid: Pid,
        from_base: u64,
        limit: usize,
        cb: &mut dyn FnMut(u64, SegLocRow) -> bool,
    ) -> Result<usize> {
        let prefix = keys::seg_loc_prefix(pid);
        let from = keys::seg_loc(pid, from_base);
        let mut err: Option<StoreError> = None;
        let n = self.scan_raw(Keyspace::SegLoc, &from, &prefix, limit, &mut |k, v| match (
            keys::seg_loc_base_of(k),
            rows::seg_loc_decode(v),
        ) {
            (Some(b), Ok(row)) => cb(b, row),
            _ => {
                err = Some(StoreError::corrupt(Keyspace::SegLoc, "seg_loc row"));
                false
            }
        })?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    /// One segment file's row: its length at the last store commit, whether it
    /// is sealed, its live bytes and its snapshot references (I11, §11.7).
    fn file(&self, bucket: u16, file_id: u32) -> Result<Option<FileRow>> {
        let k = keys::files(bucket, file_id);
        decode(
            Keyspace::Files,
            self.get_raw(Keyspace::Files, &k)?,
            rows::file_decode,
        )
    }

    /// Every segment file this node knows, in `(bucket, file_id)` order. This
    /// is what recovery truncates to (§11.5 step 3).
    fn scan_files(
        &self,
        limit: usize,
        cb: &mut dyn FnMut(u16, u32, FileRow) -> bool,
    ) -> Result<usize> {
        let mut err: Option<StoreError> = None;
        let n = self.scan_raw(Keyspace::Files, &[], &[], limit, &mut |k, v| match (
            keys::files_parts(k),
            rows::file_decode(v),
        ) {
            (Some((b, f)), Ok(row)) => cb(b, f, row),
            _ => {
                err = Some(StoreError::corrupt(Keyspace::Files, "file row"));
                false
            }
        })?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }
}

impl<T: Reads + ?Sized> TypedReads for T {}

/// Typed writes. ONLY apply holds a handle that implements this (I1).
pub trait TypedWrites: Writes {
    // ----------------------------------------------------------------- meta

    fn set_meta_u64(&mut self, name: &[u8], v: u64) -> Result<()> {
        self.put_raw(Keyspace::Meta, name, &rows::u64_encode(v))
    }

    fn set_meta_i64(&mut self, name: &[u8], v: i64) -> Result<()> {
        self.put_raw(Keyspace::Meta, name, &rows::i64_encode(v))
    }

    fn set_meta_u32(&mut self, name: &[u8], v: u32) -> Result<()> {
        self.put_raw(Keyspace::Meta, name, &rows::u32_encode(v))
    }

    fn set_meta_blob(&mut self, name: &[u8], v: &[u8]) -> Result<()> {
        self.put_raw(Keyspace::Meta, name, v)
    }

    /// Record the applied position. I11: this happens in the SAME transaction
    /// as the file lengths of every segment file the entry touched.
    fn set_applied(&mut self, index: u64, term: u64) -> Result<()> {
        self.set_meta_u64(super::meta::APPLIED_INDEX, index)?;
        self.set_meta_u64(super::meta::APPLIED_TERM, term)
    }

    // --------------------------------------------------------------- queues

    fn put_queue(&mut self, tenant: &str, queue: &str, cfg: &QueueConfig) -> Result<()> {
        let k = keys::queues(tenant, queue);
        self.put_raw(Keyspace::Queues, &k, &rows::queue_encode(cfg))
    }

    fn del_queue(&mut self, tenant: &str, queue: &str) -> Result<bool> {
        let k = keys::queues(tenant, queue);
        self.del_raw(Keyspace::Queues, &k)
    }

    // --------------------------------------------------------------- groups

    fn put_group(&mut self, tenant: &str, queue: &str, group: &str, row: &GroupRow) -> Result<()> {
        let k = keys::groups(tenant, queue, group);
        self.put_raw(Keyspace::Groups, &k, &rows::group_encode(row))
    }

    fn del_group(&mut self, tenant: &str, queue: &str, group: &str) -> Result<bool> {
        let k = keys::groups(tenant, queue, group);
        self.del_raw(Keyspace::Groups, &k)
    }

    // ----------------------------------------------------------- partitions

    /// Write the partition row AND both name indexes. One call, because a
    /// partition that exists in one of the three and not the others is a state
    /// no reader could make sense of.
    fn create_partition(&mut self, pid: Pid, row: &PartitionRow) -> Result<()> {
        self.put_partition(pid, row)?;
        let k = keys::partitions_by_key(&row.tenant, &row.queue, &row.partition);
        self.put_raw(Keyspace::PartitionsByKey, &k, &rows::u64_encode(pid))?;
        let k = keys::queue_partitions(&row.tenant, &row.queue, pid);
        self.put_raw(Keyspace::QueuePartitions, &k, rows::UNIT)
    }

    /// Overwrite the partition row alone (offsets, watermarks, stamps).
    fn put_partition(&mut self, pid: Pid, row: &PartitionRow) -> Result<()> {
        let k = keys::pid(pid);
        self.put_raw(Keyspace::Partitions, &k, &rows::partition_encode(row))
    }

    /// Remove the partition row and both name indexes.
    fn del_partition(&mut self, pid: Pid, row: &PartitionRow) -> Result<bool> {
        let k = keys::partitions_by_key(&row.tenant, &row.queue, &row.partition);
        self.del_raw(Keyspace::PartitionsByKey, &k)?;
        let k = keys::queue_partitions(&row.tenant, &row.queue, pid);
        self.del_raw(Keyspace::QueuePartitions, &k)?;
        let k = keys::pid(pid);
        self.del_raw(Keyspace::Partitions, &k)
    }

    fn put_garbage(&mut self, pid: Pid, row: &GarbageRow) -> Result<()> {
        let k = keys::pid(pid);
        self.put_raw(Keyspace::Garbage, &k, &rows::garbage_encode(row))
    }

    fn del_garbage(&mut self, pid: Pid) -> Result<bool> {
        let k = keys::pid(pid);
        self.del_raw(Keyspace::Garbage, &k)
    }

    /// Record that this node's sealed file `file_id` holds data of `pid`
    /// (node-local, written at seal, §11.7 reads it to decide a file's fate).
    fn put_partition_file(&mut self, pid: Pid, file_id: u32) -> Result<()> {
        let k = keys::partition_files(pid, file_id);
        self.put_raw(Keyspace::PartitionFiles, &k, rows::UNIT)
    }

    fn del_partition_file(&mut self, pid: Pid, file_id: u32) -> Result<bool> {
        let k = keys::partition_files(pid, file_id);
        self.del_raw(Keyspace::PartitionFiles, &k)
    }

    // -------------------------------------------------------------- cursors

    fn put_cursor(&mut self, pid: Pid, group: &str, row: &CursorRow) -> Result<()> {
        let k = keys::cursors(pid, group);
        self.put_raw(Keyspace::Cursors, &k, &rows::cursor_encode(row))
    }

    fn del_cursor(&mut self, pid: Pid, group: &str) -> Result<bool> {
        let k = keys::cursors(pid, group);
        self.del_raw(Keyspace::Cursors, &k)
    }

    fn put_lease(&mut self, worker: &str, pid: Pid, group: &str, expires_at_us: i64) -> Result<()> {
        let k = keys::leases_by_worker(worker, pid, group);
        self.put_raw(
            Keyspace::LeasesByWorker,
            &k,
            &rows::i64_encode(expires_at_us),
        )
    }

    fn del_lease(&mut self, worker: &str, pid: Pid, group: &str) -> Result<bool> {
        let k = keys::leases_by_worker(worker, pid, group);
        self.del_raw(Keyspace::LeasesByWorker, &k)
    }

    fn put_pending(
        &mut self,
        tenant: &str,
        queue: &str,
        group: &str,
        pid: Pid,
        ready_at_us: i64,
    ) -> Result<()> {
        let k = keys::pending(tenant, queue, group, pid);
        self.put_raw(Keyspace::Pending, &k, &rows::i64_encode(ready_at_us))
    }

    fn del_pending(&mut self, tenant: &str, queue: &str, group: &str, pid: Pid) -> Result<bool> {
        let k = keys::pending(tenant, queue, group, pid);
        self.del_raw(Keyspace::Pending, &k)
    }

    // ---------------------------------------------------------- dead letters

    /// Write the row AND its position index.
    fn put_dlq(
        &mut self,
        tenant: &str,
        queue: &str,
        dlq_id: &[u8; 16],
        row: &DlqRow,
    ) -> Result<()> {
        let k = keys::dlq(tenant, queue, dlq_id);
        self.put_raw(Keyspace::Dlq, &k, &rows::dlq_encode(row))?;
        let k = keys::dlq_by_pos(row.pid, &row.group, row.offset);
        self.put_raw(Keyspace::DlqByPos, &k, dlq_id)
    }

    fn del_dlq(
        &mut self,
        tenant: &str,
        queue: &str,
        dlq_id: &[u8; 16],
        row: &DlqRow,
    ) -> Result<bool> {
        let k = keys::dlq_by_pos(row.pid, &row.group, row.offset);
        self.del_raw(Keyspace::DlqByPos, &k)?;
        let k = keys::dlq(tenant, queue, dlq_id);
        self.del_raw(Keyspace::Dlq, &k)
    }

    // ----------------------------------------------------------- request ids

    /// Record `request_id → (now, outcome)` and its expiry index (D6, §5.4).
    fn put_request_outcome(&mut self, id: &[u8; 16], now_us: i64, outcome: &[u8]) -> Result<()> {
        let row = RequestIdRow {
            now_us,
            outcome: outcome.to_vec(),
        };
        let k = keys::request_ids(id);
        self.put_raw(Keyspace::RequestIds, &k, &rows::request_id_encode(&row))?;
        let k = keys::request_expiry(now_us, id);
        self.put_raw(Keyspace::RequestExpiry, &k, rows::UNIT)
    }

    fn del_request_id(&mut self, id: &[u8; 16], now_us: i64) -> Result<bool> {
        let k = keys::request_expiry(now_us, id);
        self.del_raw(Keyspace::RequestExpiry, &k)?;
        let k = keys::request_ids(id);
        self.del_raw(Keyspace::RequestIds, &k)
    }

    // ------------------------------------------------------------- counters

    /// Add to a counter and return the new value (D16: O(1) per effect, no
    /// periodic aggregation).
    fn add_counter(&mut self, key: &[u8], delta: i64) -> Result<i64> {
        let cur = self.counter_at(key)?;
        let next = cur.saturating_add(delta);
        self.put_raw(Keyspace::Counters, key, &rows::i64_encode(next))?;
        Ok(next)
    }

    fn set_counter(&mut self, key: &[u8], v: i64) -> Result<()> {
        self.put_raw(Keyspace::Counters, key, &rows::i64_encode(v))
    }

    // ----------------------------------------------------------- node-local

    fn put_seg_loc(&mut self, pid: Pid, base_offset: u64, row: &SegLocRow) -> Result<()> {
        let k = keys::seg_loc(pid, base_offset);
        self.put_raw(Keyspace::SegLoc, &k, &rows::seg_loc_encode(row))
    }

    fn del_seg_loc(&mut self, pid: Pid, base_offset: u64) -> Result<bool> {
        let k = keys::seg_loc(pid, base_offset);
        self.del_raw(Keyspace::SegLoc, &k)
    }

    fn put_file(&mut self, bucket: u16, file_id: u32, row: &FileRow) -> Result<()> {
        let k = keys::files(bucket, file_id);
        self.put_raw(Keyspace::Files, &k, &rows::file_encode(row))
    }

    fn del_file(&mut self, bucket: u16, file_id: u32) -> Result<bool> {
        let k = keys::files(bucket, file_id);
        self.del_raw(Keyspace::Files, &k)
    }

    /// Record a file's length at this commit: the second half of I11, always
    /// in the same transaction as [`TypedWrites::set_applied`].
    fn set_file_len(&mut self, bucket: u16, file_id: u32, len: u64) -> Result<()> {
        let mut row = self.file(bucket, file_id)?.unwrap_or(FileRow {
            len: 0,
            sealed: false,
            live_bytes: 0,
            snapshot_refs: 0,
        });
        row.len = len;
        self.put_file(bucket, file_id, &row)
    }
}

impl<T: Writes + ?Sized> TypedWrites for T {}
