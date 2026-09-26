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

use crate::rsm::effect::{
    CursorRow, Pid, QueueConfig, QuotaGrant, QuotaKind, StreamsQueryRow, TimerRow, TraceEvent,
};

use super::keys::{self, Counter};
use super::rows::{
    self, DlqRow, EphConfigRow, FileRow, GarbageRow, GroupRow, KvRow, PartitionRow, RequestIdRow,
    SegLocRow, StreamsStateRow,
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

    /// The `(pid, group, offset)` index → the dead letters filed AT that
    /// position, in the order they were filed.
    ///
    /// A list, not one id: this index is not unique, and a
    /// message that is replayed from the DLQ and dies again is filed at the
    /// same position twice. An index that kept only the newest left the
    /// older row reachable by nothing — a delete of the partition or of the
    /// consumer group walks this index, so the row and its `dlq_count` would
    /// outlive the queue itself.
    fn dlq_ids_at(&self, pid: Pid, group: &str, offset: i64) -> Result<Vec<[u8; 16]>> {
        let k = keys::dlq_by_pos(pid, group, offset);
        match self.get_raw(Keyspace::DlqByPos, &k)? {
            None => Ok(Vec::new()),
            Some(b) => rows::dlq_ids_decode(b)
                .map_err(|_| StoreError::corrupt(Keyspace::DlqByPos, "dlq id list")),
        }
    }

    /// The first dead letter filed at a position, if any.
    fn dlq_id_at(&self, pid: Pid, group: &str, offset: i64) -> Result<Option<[u8; 16]>> {
        Ok(self.dlq_ids_at(pid, group, offset)?.first().copied())
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

    // ------------------------------------------------------------------- kv

    /// One KV row, expired or not: liveness is the CALLER's predicate
    /// ([`KvRow::live`]), because the sweep and the console read expired rows
    /// that every other reader treats as absent (§5.7).
    fn kv(&self, tenant: &str, ns: &str, key: &str) -> Result<Option<KvRow>> {
        let k = keys::kv(tenant, ns, key);
        if k.len() > self.max_key_len() {
            // A key the store could never hold is a key that is not there. The
            // writers refuse it before apply (R-108); a read must not turn it
            // into a `KeyTooLong`.
            return Ok(None);
        }
        decode(
            Keyspace::Kv,
            self.get_raw(Keyspace::Kv, &k)?,
            rows::kv_decode,
        )
    }

    /// The rows of one namespace whose key starts with `key_prefix`, in key
    /// BYTE order (`COLLATE "C"`), starting strictly after `after` when it
    /// is given — the exclusive keyset cursor of getPrefix and of the console
    /// list. `cb(key, row)`; expired rows are passed too (see [`TypedReads::kv`]).
    ///
    /// A prefix or a cursor longer than any storable key is not an error: the
    /// prefix matches nothing, and the cursor resumes at the first storable key
    /// above it ([`super::resume_after`] of its longest storable prefix).
    fn scan_kv(
        &self,
        tenant: &str,
        ns: &str,
        key_prefix: &str,
        after: Option<&str>,
        limit: usize,
        cb: &mut dyn FnMut(&str, KvRow) -> bool,
    ) -> Result<usize> {
        let max = self.max_key_len();
        let base = keys::kv_ns_prefix(tenant, ns);
        let mut prefix = base.clone();
        prefix.extend_from_slice(key_prefix.as_bytes());
        if prefix.len() > max {
            return Ok(0);
        }
        let from: Vec<u8> = match after {
            Some(a) => {
                let full = keys::kv(tenant, ns, a);
                let cut = &full[..full.len().min(max)];
                match super::resume_after(cut, max) {
                    Some(f) => f,
                    None => return Ok(0),
                }
            }
            None => Vec::new(),
        };
        let mut err: Option<StoreError> = None;
        let n = self.scan_raw(Keyspace::Kv, &from, &prefix, limit, &mut |k, v| {
            let key = match k.get(base.len()..).map(std::str::from_utf8) {
                Some(Ok(s)) => s,
                _ => {
                    err = Some(StoreError::corrupt(Keyspace::Kv, "kv key tail"));
                    return false;
                }
            };
            match rows::kv_decode(v) {
                Ok(row) => cb(key, row),
                Err(e) => {
                    err = Some(StoreError::corrupt(Keyspace::Kv, format!("{e}")));
                    false
                }
            }
        })?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    /// Every row of one tenant, namespace by namespace, in key order:
    /// `cb(ns, key, row)`. Θ(keys of the tenant); the namespace listing is
    /// its one reader.
    fn scan_kv_tenant(
        &self,
        tenant: &str,
        limit: usize,
        cb: &mut dyn FnMut(&str, &str, KvRow) -> bool,
    ) -> Result<usize> {
        let prefix = keys::kv_tenant_prefix(tenant);
        let mut err: Option<StoreError> = None;
        let n = self.scan_raw(Keyspace::Kv, &[], &prefix, limit, &mut |k, v| {
            let Some((_t, ns, key)) = keys::kv_parts(k) else {
                err = Some(StoreError::corrupt(Keyspace::Kv, "kv key"));
                return false;
            };
            match rows::kv_decode(v) {
                Ok(row) => cb(&ns, &key, row),
                Err(e) => {
                    err = Some(StoreError::corrupt(Keyspace::Kv, format!("{e}")));
                    false
                }
            }
        })?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    /// The expiry index, oldest first: `cb(expires_at_us, version, kv_key)`.
    fn scan_kv_expiry(
        &self,
        limit: usize,
        cb: &mut dyn FnMut(i64, u64, &[u8]) -> bool,
    ) -> Result<usize> {
        let mut err: Option<StoreError> = None;
        let n = self.scan_raw(Keyspace::KvExpiry, &[], &[], limit, &mut |k, v| match (
            keys::kv_expiry_parts(k),
            rows::kv_expiry_decode(v),
        ) {
            (Some((at, version)), Ok(kv_key)) => cb(at, version, &kv_key),
            _ => {
                err = Some(StoreError::corrupt(Keyspace::KvExpiry, "kv_expiry row"));
                false
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

    // --------------------------------------------------------------- timers

    /// One timer (PK `(tenant, queue, timer_key)`).
    fn timer(&self, tenant: &str, queue: &str, key: &str) -> Result<Option<TimerRow>> {
        let k = keys::timers(tenant, queue, key);
        decode(
            Keyspace::Timers,
            self.get_raw(Keyspace::Timers, &k)?,
            rows::timer_decode,
        )
    }

    /// One queue's timers in timer-key BYTE order, starting strictly AFTER
    /// `after` when given (the exclusive keyset cursor).
    fn scan_timers(
        &self,
        tenant: &str,
        queue: &str,
        after: Option<&str>,
        limit: usize,
        cb: &mut dyn FnMut(&str, TimerRow) -> bool,
    ) -> Result<usize> {
        let prefix = keys::timers_prefix(tenant, queue);
        let from = match after {
            // The smallest key strictly above `after` inside this queue.
            Some(a) => {
                match super::resume_after(&keys::timers(tenant, queue, a), self.max_key_len()) {
                    Some(f) => f,
                    None => return Ok(0),
                }
            }
            None => prefix.clone(),
        };
        let mut err: Option<StoreError> = None;
        let n = self.scan_raw(Keyspace::Timers, &from, &prefix, limit, &mut |k, v| {
            let name = match keys::timers_key_of(k, prefix.len()) {
                Some(n) => n,
                None => {
                    err = Some(StoreError::corrupt(Keyspace::Timers, "timer key"));
                    return false;
                }
            };
            match rows::timer_decode(v) {
                Ok(row) => cb(&name, row),
                Err(e) => {
                    err = Some(StoreError::corrupt(Keyspace::Timers, format!("{e}")));
                    false
                }
            }
        })?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    /// Exactly how many of one queue's timers have a key starting with the
    /// LITERAL `prefix`: a prefix range walk, no row decoded.
    fn count_timers_with_prefix(&self, tenant: &str, queue: &str, prefix: &str) -> Result<u64> {
        let p = keys::timers_key_prefix(tenant, queue, prefix);
        let mut n = 0u64;
        self.scan_raw(Keyspace::Timers, &p, &p, usize::MAX, &mut |_k, _v| {
            n += 1;
            true
        })?;
        Ok(n)
    }

    /// The fire order: `(due_us, tenant, queue, timer_key)`, earliest first,
    /// at most `limit` entries, until `cb` returns false.
    fn scan_timers_due(
        &self,
        limit: usize,
        cb: &mut dyn FnMut(i64, &str, &str, &str) -> bool,
    ) -> Result<usize> {
        let mut err: Option<StoreError> = None;
        let n = self.scan_raw(Keyspace::TimersDue, &[], &[], limit, &mut |k, _v| {
            match keys::timers_due_parts(k) {
                Some((due, t, q, key)) => cb(due, &t, &q, &key),
                None => {
                    err = Some(StoreError::corrupt(Keyspace::TimersDue, "timers_due key"));
                    false
                }
            }
        })?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    // ----------------------------------------------------- phase-2 control

    fn streams_query(&self, tenant: &str, query_id: &[u8; 16]) -> Result<Option<StreamsQueryRow>> {
        let k = keys::streams_query(tenant, query_id);
        decode(
            Keyspace::StreamsQueries,
            self.get_raw(Keyspace::StreamsQueries, &k)?,
            rows::streams_query_decode,
        )
    }

    fn scan_streams_queries(
        &self,
        tenant: &str,
        limit: usize,
        cb: &mut dyn FnMut([u8; 16], StreamsQueryRow) -> bool,
    ) -> Result<usize> {
        let prefix = keys::streams_queries_prefix(tenant);
        let mut err = None;
        let n = self.scan_raw(
            Keyspace::StreamsQueries,
            &prefix,
            &prefix,
            limit,
            &mut |k, v| match (
                keys::streams_query_id_of(k, prefix.len()),
                rows::streams_query_decode(v),
            ) {
                (Some(id), Ok(row)) => cb(id, row),
                _ => {
                    err = Some(StoreError::corrupt(Keyspace::StreamsQueries, "query row"));
                    false
                }
            },
        )?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    fn streams_state(
        &self,
        query_id: &[u8; 16],
        pid: Pid,
        key: &str,
    ) -> Result<Option<StreamsStateRow>> {
        let k = keys::streams_state(query_id, pid, key);
        decode(
            Keyspace::StreamsState,
            self.get_raw(Keyspace::StreamsState, &k)?,
            rows::streams_state_decode,
        )
    }

    fn flag(&self, name: &str) -> Result<Option<Vec<u8>>> {
        Ok(self
            .get_raw(Keyspace::Flags, &keys::flag(name))?
            .map(ToOwned::to_owned))
    }

    fn quota(&self, kind: QuotaKind, tenant: &str) -> Result<Option<QuotaGrant>> {
        let k = keys::quota(kind, tenant);
        decode(
            Keyspace::Quotas,
            self.get_raw(Keyspace::Quotas, &k)?,
            rows::quota_decode,
        )
    }

    fn eph_config(&self, tenant: &str, queue: &str) -> Result<Option<EphConfigRow>> {
        let k = keys::eph_config(tenant, queue);
        decode(
            Keyspace::EphConfig,
            self.get_raw(Keyspace::EphConfig, &k)?,
            rows::eph_config_decode,
        )
    }

    fn scan_eph_configs(
        &self,
        limit: usize,
        cb: &mut dyn FnMut(&str, &str, EphConfigRow) -> bool,
    ) -> Result<usize> {
        let mut err = None;
        let n = self.scan_raw(Keyspace::EphConfig, &[], &[], limit, &mut |k, v| {
            let parsed = (|| {
                let (tenant, at) = keys::read_name(k, 0)?;
                let (queue, _) = keys::read_name(k, at)?;
                Some((tenant, queue))
            })();
            match (parsed, rows::eph_config_decode(v)) {
                (Some((tenant, queue)), Ok(row)) => cb(&tenant, &queue, row),
                _ => {
                    err = Some(StoreError::corrupt(
                        Keyspace::EphConfig,
                        "ephemeral config row",
                    ));
                    false
                }
            }
        })?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    fn scan_traces(
        &self,
        limit: usize,
        cb: &mut dyn FnMut(&[u8], TraceEvent) -> bool,
    ) -> Result<usize> {
        let mut err = None;
        let n =
            self.scan_raw(
                Keyspace::Traces,
                &[],
                &[],
                limit,
                &mut |k, v| match rows::trace_decode(v) {
                    Ok(row) => cb(k, row),
                    Err(e) => {
                        err = Some(StoreError::corrupt(Keyspace::Traces, format!("{e}")));
                        false
                    }
                },
            )?;
        match err {
            Some(e) => Err(e),
            None => Ok(n),
        }
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
        // The CAUSE travels with the refusal: a row this build cannot read is
        // fatal for the node (§11.5 answers a disagreement by discarding the
        // state directory), so "file row" alone leaves the operator unable to
        // tell a version mismatch from damage.
        let n = self.scan_raw(Keyspace::Files, &[], &[], limit, &mut |k, v| match (
            keys::files_parts(k),
            rows::file_decode(v),
        ) {
            (Some((b, f)), Ok(row)) => cb(b, f, row),
            (None, _) => {
                err = Some(StoreError::corrupt(Keyspace::Files, "file row key"));
                false
            }
            (_, Err(e)) => {
                err = Some(StoreError::corrupt(
                    Keyspace::Files,
                    format!("file row: {e}"),
                ));
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
        let mut ids = self.dlq_ids_at(row.pid, &row.group, row.offset)?;
        if !ids.contains(dlq_id) {
            ids.push(*dlq_id);
        }
        let k = keys::dlq_by_pos(row.pid, &row.group, row.offset);
        self.put_raw(Keyspace::DlqByPos, &k, &rows::dlq_ids_encode(&ids))
    }

    fn del_dlq(
        &mut self,
        tenant: &str,
        queue: &str,
        dlq_id: &[u8; 16],
        row: &DlqRow,
    ) -> Result<bool> {
        // Only THIS dead letter leaves the position: another one filed at the
        // same `(pid, group, offset)` keeps its place in the list.
        let mut ids = self.dlq_ids_at(row.pid, &row.group, row.offset)?;
        ids.retain(|id| id != dlq_id);
        let k = keys::dlq_by_pos(row.pid, &row.group, row.offset);
        if ids.is_empty() {
            self.del_raw(Keyspace::DlqByPos, &k)?;
        } else {
            self.put_raw(Keyspace::DlqByPos, &k, &rows::dlq_ids_encode(&ids))?;
        }
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

    // ------------------------------------------------------------------- kv

    /// Write a KV row AND keep its expiry index exact: the old row's index
    /// entry goes, the new one's (if it expires) comes. One call, like
    /// [`TypedWrites::put_dlq`], so a row and its index can never disagree.
    fn put_kv(&mut self, tenant: &str, ns: &str, key: &str, row: &KvRow) -> Result<()> {
        let k = keys::kv(tenant, ns, key);
        if let Some(old) = self.kv(tenant, ns, key)? {
            if let Some(at) = old.expires_at_us {
                self.del_raw(Keyspace::KvExpiry, &keys::kv_expiry(at, old.version))?;
            }
        }
        self.put_raw(Keyspace::Kv, &k, &rows::kv_encode(row))?;
        if let Some(at) = row.expires_at_us {
            self.put_raw(
                Keyspace::KvExpiry,
                &keys::kv_expiry(at, row.version),
                &rows::kv_expiry_encode(&k),
            )?;
        }
        Ok(())
    }

    /// Remove a KV row and its expiry index entry; the row that was there, if
    /// any.
    fn del_kv(&mut self, tenant: &str, ns: &str, key: &str) -> Result<Option<KvRow>> {
        let Some(old) = self.kv(tenant, ns, key)? else {
            return Ok(None);
        };
        if let Some(at) = old.expires_at_us {
            self.del_raw(Keyspace::KvExpiry, &keys::kv_expiry(at, old.version))?;
        }
        self.del_raw(Keyspace::Kv, &keys::kv(tenant, ns, key))?;
        Ok(Some(old))
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

    // --------------------------------------------------------------- timers

    /// Write a timer row AND its fire-order index entry, replacing the old
    /// index entry when the row already existed (its due instant may move).
    /// One call, because a row whose `timers_due` entry names a different
    /// instant is a timer the fire step would visit at the wrong time.
    fn put_timer(&mut self, tenant: &str, queue: &str, key: &str, row: &TimerRow) -> Result<()> {
        if let Some(old) = self.timer(tenant, queue, key)? {
            let old_due = keys::timers_due(rows::timer_due_us(&old), tenant, queue, key);
            self.del_raw(Keyspace::TimersDue, &old_due)?;
        }
        let k = keys::timers(tenant, queue, key);
        self.put_raw(Keyspace::Timers, &k, &rows::timer_encode(row))?;
        let due = keys::timers_due(rows::timer_due_us(row), tenant, queue, key);
        self.put_raw(Keyspace::TimersDue, &due, rows::UNIT)
    }

    /// Remove a timer row and its fire-order index entry. `row` is the row as
    /// stored (its due instant names the index entry).
    fn del_timer(&mut self, tenant: &str, queue: &str, key: &str, row: &TimerRow) -> Result<bool> {
        let due = keys::timers_due(rows::timer_due_us(row), tenant, queue, key);
        self.del_raw(Keyspace::TimersDue, &due)?;
        let k = keys::timers(tenant, queue, key);
        self.del_raw(Keyspace::Timers, &k)
    }

    // ----------------------------------------------------- phase-2 control

    fn put_streams_query(
        &mut self,
        tenant: &str,
        query_id: &[u8; 16],
        row: &StreamsQueryRow,
    ) -> Result<()> {
        self.put_raw(
            Keyspace::StreamsQueries,
            &keys::streams_query(tenant, query_id),
            &rows::streams_query_encode(row),
        )
    }

    fn put_streams_state(
        &mut self,
        query_id: &[u8; 16],
        pid: Pid,
        key: &str,
        row: &StreamsStateRow,
    ) -> Result<()> {
        self.put_raw(
            Keyspace::StreamsState,
            &keys::streams_state(query_id, pid, key),
            &rows::streams_state_encode(row),
        )
    }

    fn del_streams_state(&mut self, query_id: &[u8; 16], pid: Pid, key: &str) -> Result<bool> {
        self.del_raw(
            Keyspace::StreamsState,
            &keys::streams_state(query_id, pid, key),
        )
    }

    fn put_flag(&mut self, name: &str, value: &[u8]) -> Result<()> {
        self.put_raw(Keyspace::Flags, &keys::flag(name), value)
    }

    fn put_quota(&mut self, kind: QuotaKind, tenant: &str, row: &QuotaGrant) -> Result<()> {
        self.put_raw(
            Keyspace::Quotas,
            &keys::quota(kind, tenant),
            &rows::quota_encode(row),
        )
    }

    fn put_eph_config(&mut self, tenant: &str, queue: &str, row: &EphConfigRow) -> Result<()> {
        self.put_raw(
            Keyspace::EphConfig,
            &keys::eph_config(tenant, queue),
            &rows::eph_config_encode(row),
        )
    }

    fn del_eph_config(&mut self, tenant: &str, queue: &str) -> Result<bool> {
        self.del_raw(Keyspace::EphConfig, &keys::eph_config(tenant, queue))
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
    ///
    /// It preserves the row's LIVENESS, which is the half of the row apply
    /// must not lose (see [`FileRow`]); a caller that has the whole file state
    /// — the apply thread, at every store commit — writes it with
    /// [`TypedWrites::put_file`] instead.
    fn set_file_len(&mut self, bucket: u16, file_id: u32, len: u64) -> Result<()> {
        let mut row = self.file(bucket, file_id)?.unwrap_or(FileRow {
            len: 0,
            durable_len: 0,
            sealed: false,
            frames: 0,
            retained_frames: 0,
            retained_bytes: 0,
            window_frames: 0,
            snapshot_refs: 0,
        });
        row.len = len;
        self.put_file(bucket, file_id, &row)
    }
}

impl<T: Writes + ?Sized> TypedWrites for T {}
