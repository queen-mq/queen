//! Key encodings for the keyspaces of §6.1 and §6.2.
//!
//! LMDB orders keys by `memcmp`, so the encoding IS the index order: every
//! "in key order" the plan asks for — the retention walk, `log_renew_lease_v1`
//! iterating a worker's leases, the wildcard candidate scan, a `DeleteChunk`
//! resuming where the last one stopped — is a range scan over one of the
//! functions below, and nothing sorts in RAM.
//!
//! Three rules hold everywhere:
//!
//! - **Unsigned integers are big-endian.** `memcmp` on big-endian bytes is
//!   numeric order.
//! - **Signed integers are big-endian with the sign bit flipped**
//!   ([`push_i64`]), so −1 sorts below 0. Offsets are `u64` in the keyspaces
//!   that hold them, but `log_consumers.committed` and a timer's DLQ offset
//!   are −1 in the SQL, and a key that carries one must still order.
//! - **Names are escaped and terminated** ([`push_name`]): `0x00` becomes
//!   `0x00 0xFF` and the name ends with `0x00 0x00`. Without the escape,
//!   `("ab", "c")` and `("a", "bc")` would encode to the same bytes; with it,
//!   the encoding is unambiguous AND order-preserving (a terminator, `0x00
//!   0x00`, sorts below any escaped content byte, so a prefix sorts before a
//!   longer name, which is plain string order).
//!
//! A composite key of names can therefore exceed LMDB's 511-byte limit —
//! `(tenant, queue, group)` is three unbounded names in the postgres schema
//! (`consumer_groups_metadata.consumer_group` is `TEXT`). The adapter refuses
//! such a key with [`super::StoreError::KeyTooLong`] rather than truncating
//! it; see the note in `super`'s header.

use crate::rsm::effect::{Pid, QuotaKind};

// ---------------------------------------------------------------------------
// Primitives
// ---------------------------------------------------------------------------

/// A name, escaped and terminated. See the module header.
pub fn push_name(out: &mut Vec<u8>, s: &str) {
    for &b in s.as_bytes() {
        if b == 0x00 {
            out.push(0x00);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out.push(0x00);
    out.push(0x00);
}

/// Read one escaped name back, returning it and the offset just past its
/// terminator. `None` when the bytes end before the terminator.
pub fn read_name(b: &[u8], mut at: usize) -> Option<(String, usize)> {
    let mut out: Vec<u8> = Vec::new();
    while at < b.len() {
        let c = b[at];
        if c != 0x00 {
            out.push(c);
            at += 1;
            continue;
        }
        let next = *b.get(at + 1)?;
        at += 2;
        match next {
            0x00 => return Some((String::from_utf8(out).ok()?, at)),
            0xFF => out.push(0x00),
            _ => return None,
        }
    }
    None
}

pub fn push_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_be_bytes());
}

pub fn push_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}

pub fn push_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_be_bytes());
}

/// Big-endian with the sign bit flipped, so negatives sort below zero.
pub fn push_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&((v as u64) ^ (1u64 << 63)).to_be_bytes());
}

pub fn read_u16(b: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_be_bytes(b.get(at..at + 2)?.try_into().ok()?))
}

pub fn read_u32(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

pub fn read_u64(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_be_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

pub fn read_i64(b: &[u8], at: usize) -> Option<i64> {
    Some((read_u64(b, at)? ^ (1u64 << 63)) as i64)
}

fn with(cap: usize) -> Vec<u8> {
    Vec::with_capacity(cap)
}

// ---------------------------------------------------------------------------
// queues, groups
// ---------------------------------------------------------------------------

/// `(tenant, queue)`.
pub fn queues(tenant: &str, queue: &str) -> Vec<u8> {
    let mut k = with(tenant.len() + queue.len() + 4);
    push_name(&mut k, tenant);
    push_name(&mut k, queue);
    k
}

/// Every queue of a tenant, in name order.
pub fn queues_prefix(tenant: &str) -> Vec<u8> {
    let mut k = with(tenant.len() + 2);
    push_name(&mut k, tenant);
    k
}

/// `(tenant, queue, group)`.
pub fn groups(tenant: &str, queue: &str, group: &str) -> Vec<u8> {
    let mut k = with(tenant.len() + queue.len() + group.len() + 6);
    push_name(&mut k, tenant);
    push_name(&mut k, queue);
    push_name(&mut k, group);
    k
}

/// Every group of a queue, in name order.
pub fn groups_prefix(tenant: &str, queue: &str) -> Vec<u8> {
    queues(tenant, queue)
}

/// The group name of a `groups` key.
pub fn groups_group_of(k: &[u8]) -> Option<String> {
    let (_t, at) = read_name(k, 0)?;
    let (_q, at) = read_name(k, at)?;
    Some(read_name(k, at)?.0)
}

// ---------------------------------------------------------------------------
// partitions and their indexes
// ---------------------------------------------------------------------------

/// `pid`. Also [`super::Keyspace::Garbage`]'s key.
pub fn pid(p: Pid) -> Vec<u8> {
    p.to_be_bytes().to_vec()
}

pub fn pid_of(k: &[u8]) -> Option<Pid> {
    read_u64(k, 0)
}

/// `(tenant, queue, partition)` → pid.
pub fn partitions_by_key(tenant: &str, queue: &str, partition: &str) -> Vec<u8> {
    let mut k = with(tenant.len() + queue.len() + partition.len() + 6);
    push_name(&mut k, tenant);
    push_name(&mut k, queue);
    push_name(&mut k, partition);
    k
}

/// Every partition NAME of a queue, in name order.
pub fn partitions_by_key_prefix(tenant: &str, queue: &str) -> Vec<u8> {
    queues(tenant, queue)
}

/// `(tenant, queue, pid)` → (): the scan index for wildcard pops and admin.
pub fn queue_partitions(tenant: &str, queue: &str, p: Pid) -> Vec<u8> {
    let mut k = with(tenant.len() + queue.len() + 12);
    push_name(&mut k, tenant);
    push_name(&mut k, queue);
    push_u64(&mut k, p);
    k
}

/// Every partition of a queue, in pid order.
pub fn queue_partitions_prefix(tenant: &str, queue: &str) -> Vec<u8> {
    queues(tenant, queue)
}

/// The pid at the end of a `queue_partitions` key.
pub fn queue_partitions_pid_of(k: &[u8]) -> Option<Pid> {
    read_u64(k, k.len().checked_sub(8)?)
}

/// `(pid, file_id)` → (): which sealed segment files hold data of a partition
/// (§6.1, G0 amendment).
pub fn partition_files(p: Pid, file_id: u32) -> Vec<u8> {
    let mut k = with(12);
    push_u64(&mut k, p);
    push_u32(&mut k, file_id);
    k
}

pub fn partition_files_prefix(p: Pid) -> Vec<u8> {
    pid(p)
}

pub fn partition_files_file_of(k: &[u8]) -> Option<u32> {
    read_u32(k, 8)
}

// ---------------------------------------------------------------------------
// cursors and the two indexes derived from them
// ---------------------------------------------------------------------------

/// `(pid, group)`.
pub fn cursors(p: Pid, group: &str) -> Vec<u8> {
    let mut k = with(group.len() + 10);
    push_u64(&mut k, p);
    push_name(&mut k, group);
    k
}

/// Every group's cursor on one partition, in group-name order.
pub fn cursors_prefix(p: Pid) -> Vec<u8> {
    pid(p)
}

pub fn cursors_group_of(k: &[u8]) -> Option<String> {
    Some(read_name(k, 8)?.0)
}

/// `(worker, pid, group)` → `lease_expires_at_us`. `log_renew_lease_v1` walks
/// one worker's leases in this order (§8).
pub fn leases_by_worker(worker: &str, p: Pid, group: &str) -> Vec<u8> {
    let mut k = with(worker.len() + group.len() + 12);
    push_name(&mut k, worker);
    push_u64(&mut k, p);
    push_name(&mut k, group);
    k
}

pub fn leases_by_worker_prefix(worker: &str) -> Vec<u8> {
    let mut k = with(worker.len() + 2);
    push_name(&mut k, worker);
    k
}

/// `(pid, group)` of a `leases_by_worker` key.
pub fn leases_by_worker_parts(k: &[u8]) -> Option<(String, Pid, String)> {
    let (worker, at) = read_name(k, 0)?;
    let p = read_u64(k, at)?;
    let (group, _) = read_name(k, at + 8)?;
    Some((worker, p, group))
}

/// `(tenant, queue, group, pid)` → `ready_at_us` (§6.1 `pending`).
pub fn pending(tenant: &str, queue: &str, group: &str, p: Pid) -> Vec<u8> {
    let mut k = with(tenant.len() + queue.len() + group.len() + 14);
    push_name(&mut k, tenant);
    push_name(&mut k, queue);
    push_name(&mut k, group);
    push_u64(&mut k, p);
    k
}

/// Every partition with work for one (tenant, queue, group), in pid order:
/// the O(pending) rebuild of the ready rings (§6.3).
pub fn pending_prefix(tenant: &str, queue: &str, group: &str) -> Vec<u8> {
    groups(tenant, queue, group)
}

/// Everything pending for one tenant, for the rebuild's outer walk.
pub fn pending_tenant_prefix(tenant: &str) -> Vec<u8> {
    queues_prefix(tenant)
}

/// `(tenant, queue, group, pid)` of a `pending` key.
pub fn pending_parts(k: &[u8]) -> Option<(String, String, String, Pid)> {
    let (t, at) = read_name(k, 0)?;
    let (q, at) = read_name(k, at)?;
    let (g, at) = read_name(k, at)?;
    let p = read_u64(k, at)?;
    Some((t, q, g, p))
}

// ---------------------------------------------------------------------------
// dead letters
// ---------------------------------------------------------------------------

/// `(tenant, queue, dlq_id)`: the primary key of `log_dlq`.
pub fn dlq(tenant: &str, queue: &str, dlq_id: &[u8; 16]) -> Vec<u8> {
    let mut k = with(tenant.len() + queue.len() + 20);
    push_name(&mut k, tenant);
    push_name(&mut k, queue);
    k.extend_from_slice(dlq_id);
    k
}

pub fn dlq_prefix(tenant: &str, queue: &str) -> Vec<u8> {
    queues(tenant, queue)
}

/// `(tenant, queue, dlq_id)` of a [`dlq`] key.
pub fn dlq_parts(k: &[u8]) -> Option<(String, String, [u8; 16])> {
    let (tenant, at) = read_name(k, 0)?;
    let (queue, at) = read_name(k, at)?;
    let id = k.get(at..at + 16)?.try_into().ok()?;
    (at + 16 == k.len()).then_some((tenant, queue, id))
}

/// `(pid, group, offset)` → dlq_id: the second index of §6.1's `dlq`. The
/// offset is signed because a timer's dead letter files at −1 (025).
pub fn dlq_by_pos(p: Pid, group: &str, offset: i64) -> Vec<u8> {
    let mut k = with(group.len() + 18);
    push_u64(&mut k, p);
    push_name(&mut k, group);
    push_i64(&mut k, offset);
    k
}

pub fn dlq_by_pos_prefix(p: Pid, group: &str) -> Vec<u8> {
    cursors(p, group)
}

pub fn dlq_by_pos_pid_prefix(p: Pid) -> Vec<u8> {
    pid(p)
}

/// The offset of a [`dlq_by_pos`] key: its last eight bytes.
pub fn dlq_by_pos_offset_of(k: &[u8]) -> Option<i64> {
    let b: [u8; 8] = k.get(k.len().checked_sub(8)?..)?.try_into().ok()?;
    Some((u64::from_be_bytes(b) ^ (1u64 << 63)) as i64)
}

// ---------------------------------------------------------------------------
// dedup (D10 option (a), lean)
// ---------------------------------------------------------------------------

/// `(pid, hash)` → the occurrence list. The hash is the xxh3_128 of the
/// transaction id, exactly the 16 bytes the `Append` effect carries.
pub fn dedup(p: Pid, hash: &[u8; 16]) -> Vec<u8> {
    let mut k = with(24);
    push_u64(&mut k, p);
    k.extend_from_slice(hash);
    k
}

pub fn dedup_prefix(p: Pid) -> Vec<u8> {
    pid(p)
}

/// `(pid, base_offset)` → `[end][created][hashes]`: one row per `Append`, the
/// expiry half of the lean encoding. Walked forward from the partition's
/// `txns_start`.
pub fn txns(p: Pid, base_offset: u64) -> Vec<u8> {
    let mut k = with(16);
    push_u64(&mut k, p);
    push_u64(&mut k, base_offset);
    k
}

pub fn txns_prefix(p: Pid) -> Vec<u8> {
    pid(p)
}

pub fn txns_base_of(k: &[u8]) -> Option<u64> {
    read_u64(k, 8)
}

// ---------------------------------------------------------------------------
// request ids (D6, §5.4)
// ---------------------------------------------------------------------------

pub fn request_ids(id: &[u8; 16]) -> Vec<u8> {
    id.to_vec()
}

/// `(now_us, request_id)`: oldest first, so the expiry loop is one forward
/// scan and a `DeleteChunk`-shaped resume.
pub fn request_expiry(now_us: i64, id: &[u8; 16]) -> Vec<u8> {
    let mut k = with(24);
    push_i64(&mut k, now_us);
    k.extend_from_slice(id);
    k
}

pub fn request_expiry_parts(k: &[u8]) -> Option<(i64, [u8; 16])> {
    let t = read_i64(k, 0)?;
    let id: [u8; 16] = k.get(8..24)?.try_into().ok()?;
    Some((t, id))
}

// ---------------------------------------------------------------------------
// counters (D16, §6.4)
// ---------------------------------------------------------------------------

/// What a counter is counted FOR. The byte is the first of the key, so a scan
/// over one scope is a prefix scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum CounterScope {
    Partition = 0,
    Queue = 1,
    Tenant = 2,
    /// A (queue, group) pair: the lag inputs.
    Group = 3,
}

/// The counters of §6.4. Ids are permanent: a retired counter's id is never
/// reused. The full list is WP-2.6's to close against every read of §8; the
/// message path needs these.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum Counter {
    Pushed = 0,
    Pending = 1,
    Completed = 2,
    Failed = 3,
    DlqCount = 4,
    RetainedBytes = 5,
    LastPushUs = 6,
    LastPopUs = 7,
    /// Frames delivered at least once: `total_consumed`'s queue-level twin.
    Consumed = 8,
}

fn counter_head(out: &mut Vec<u8>, scope: CounterScope) {
    out.push(scope as u8);
}

fn counter_tail(out: &mut Vec<u8>, c: Counter) {
    push_u16(out, c as u16);
}

pub fn counter_partition(p: Pid, c: Counter) -> Vec<u8> {
    let mut k = with(11);
    counter_head(&mut k, CounterScope::Partition);
    push_u64(&mut k, p);
    counter_tail(&mut k, c);
    k
}

pub fn counter_queue(tenant: &str, queue: &str, c: Counter) -> Vec<u8> {
    let mut k = with(tenant.len() + queue.len() + 7);
    counter_head(&mut k, CounterScope::Queue);
    push_name(&mut k, tenant);
    push_name(&mut k, queue);
    counter_tail(&mut k, c);
    k
}

pub fn counter_tenant(tenant: &str, c: Counter) -> Vec<u8> {
    let mut k = with(tenant.len() + 5);
    counter_head(&mut k, CounterScope::Tenant);
    push_name(&mut k, tenant);
    counter_tail(&mut k, c);
    k
}

pub fn counter_group(tenant: &str, queue: &str, group: &str, c: Counter) -> Vec<u8> {
    let mut k = with(tenant.len() + queue.len() + group.len() + 9);
    counter_head(&mut k, CounterScope::Group);
    push_name(&mut k, tenant);
    push_name(&mut k, queue);
    push_name(&mut k, group);
    counter_tail(&mut k, c);
    k
}

/// Every counter of one partition (the `PartitionDelete` sweep).
pub fn counter_partition_prefix(p: Pid) -> Vec<u8> {
    let mut k = with(9);
    counter_head(&mut k, CounterScope::Partition);
    push_u64(&mut k, p);
    k
}

/// Every queue counter belonging to one tenant.
pub fn counter_queue_tenant_prefix(tenant: &str) -> Vec<u8> {
    let mut k = with(tenant.len() + 3);
    counter_head(&mut k, CounterScope::Queue);
    push_name(&mut k, tenant);
    k
}

/// Every tenant counter belonging to one tenant.
pub fn counter_tenant_prefix(tenant: &str) -> Vec<u8> {
    let mut k = with(tenant.len() + 3);
    counter_head(&mut k, CounterScope::Tenant);
    push_name(&mut k, tenant);
    k
}

/// Every consumer-group counter belonging to one tenant.
pub fn counter_group_tenant_prefix(tenant: &str) -> Vec<u8> {
    let mut k = with(tenant.len() + 3);
    counter_head(&mut k, CounterScope::Group);
    push_name(&mut k, tenant);
    k
}

// ---------------------------------------------------------------------------
// kv (024, WP-2.2)
// ---------------------------------------------------------------------------

/// `(tenant, ns)`: every key of one namespace, in key byte order.
pub fn kv_ns_prefix(tenant: &str, ns: &str) -> Vec<u8> {
    let mut k = with(tenant.len() + ns.len() + 4);
    push_name(&mut k, tenant);
    push_name(&mut k, ns);
    k
}

/// `(tenant)`: every key of every namespace of one tenant, namespace by
/// namespace (the console's namespace selector walks it).
pub fn kv_tenant_prefix(tenant: &str) -> Vec<u8> {
    let mut k = with(tenant.len() + 2);
    push_name(&mut k, tenant);
    k
}

/// `(tenant, ns, key)`. The tenant and the namespace are escaped and
/// terminated like every other name; the KEY is the raw, UNTERMINATED tail.
/// That is what makes the SQL's two ordering promises structural here:
///
/// - inside one namespace the store order IS the byte order of the key
///   strings — `COLLATE "C"`, the order getPrefix pages in and the order its
///   `after` cursor is exclusive in;
/// - a key PREFIX is a store-key prefix (`kv_ns_prefix ‖ prefix`), so a prefix
///   read is one range scan, with no escaping in the way and no metacharacter
///   (024's `starts_with`, never a LIKE).
///
/// The tail needs no terminator because nothing follows it, and no escape
/// because the prefix before it is self-delimiting.
pub fn kv(tenant: &str, ns: &str, key: &str) -> Vec<u8> {
    let mut k = kv_ns_prefix(tenant, ns);
    k.extend_from_slice(key.as_bytes());
    k
}

/// The length [`kv`] would produce, without building it: the planner and the
/// receiver refuse a key the store could never hold BEFORE it reaches apply
/// (R-108), where a `KeyTooLong` would stop the node.
pub fn kv_len(tenant: &str, ns: &str, key: &str) -> usize {
    fn name_len(s: &str) -> usize {
        s.len() + s.bytes().filter(|b| *b == 0).count() + 2
    }
    name_len(tenant) + name_len(ns) + key.len()
}

/// `(tenant, ns, key)` of a [`kv`] key.
pub fn kv_parts(k: &[u8]) -> Option<(String, String, String)> {
    let (tenant, at) = read_name(k, 0)?;
    let (ns, at) = read_name(k, at)?;
    let key = std::str::from_utf8(k.get(at..)?).ok()?.to_string();
    Some((tenant, ns, key))
}

/// `(expires_at_us, version)` → the [`kv`] key: the expiry index, oldest
/// first. The version is unique per write (`kv_version_base + ordinal`, I18),
/// so two rows expiring in the same microsecond never share an index key.
pub fn kv_expiry(expires_at_us: i64, version: u64) -> Vec<u8> {
    let mut k = with(16);
    push_i64(&mut k, expires_at_us);
    push_u64(&mut k, version);
    k
}

pub fn kv_expiry_parts(k: &[u8]) -> Option<(i64, u64)> {
    if k.len() != 16 {
        return None;
    }
    Some((read_i64(k, 0)?, read_u64(k, 8)?))
}

// ---------------------------------------------------------------------------
// timers (025, WP-2.3)
// ---------------------------------------------------------------------------

/// `(tenant, queue, timer_key)`: the primary key of `queen.log_timers`. The
/// escaped names keep BYTE order, so a keyset walk over one queue's timers is
/// 025's `ORDER BY timer_key COLLATE "C"`.
pub fn timers(tenant: &str, queue: &str, key: &str) -> Vec<u8> {
    let mut k = with(tenant.len() + queue.len() + key.len() + 6);
    push_name(&mut k, tenant);
    push_name(&mut k, queue);
    push_name(&mut k, key);
    k
}

/// Every timer of one queue, in timer-key byte order.
pub fn timers_prefix(tenant: &str, queue: &str) -> Vec<u8> {
    queues(tenant, queue)
}

/// The scan prefix of every timer of one queue whose key STARTS WITH `prefix`
/// (025 `log_timers_count_v1`): the queue prefix plus the escaped prefix bytes
/// WITHOUT a terminator, so `starts_with` on the encoded key is `starts_with`
/// on the name (a NUL is escaped the same way inside and outside the prefix).
pub fn timers_key_prefix(tenant: &str, queue: &str, prefix: &str) -> Vec<u8> {
    let mut k = timers_prefix(tenant, queue);
    for &b in prefix.as_bytes() {
        if b == 0x00 {
            k.push(0x00);
            k.push(0xFF);
        } else {
            k.push(b);
        }
    }
    k
}

/// The timer key of a `timers` key, given the length of its queue prefix.
pub fn timers_key_of(k: &[u8], prefix_len: usize) -> Option<String> {
    Some(read_name(k, prefix_len)?.0)
}

/// `(tenant, queue, timer_key)` of a timer primary key.
pub fn timers_parts(k: &[u8]) -> Option<(String, String, String)> {
    let (tenant, at) = read_name(k, 0)?;
    let (queue, at) = read_name(k, at)?;
    let (key, at) = read_name(k, at)?;
    (at == k.len()).then_some((tenant, queue, key))
}

/// `(due_us, tenant, queue, timer_key)` → (): the fire order, earliest first.
/// Signed (a `delayMs` in the past is legal and yields a due instant below
/// any real clock only in a test, but the order must hold through zero).
pub fn timers_due(due_us: i64, tenant: &str, queue: &str, key: &str) -> Vec<u8> {
    let mut k = with(tenant.len() + queue.len() + key.len() + 14);
    push_i64(&mut k, due_us);
    push_name(&mut k, tenant);
    push_name(&mut k, queue);
    push_name(&mut k, key);
    k
}

/// `(due_us, tenant, queue, timer_key)` of a `timers_due` key.
pub fn timers_due_parts(k: &[u8]) -> Option<(i64, String, String, String)> {
    let due = read_i64(k, 0)?;
    let (t, at) = read_name(k, 8)?;
    let (q, at) = read_name(k, at)?;
    let (key, _) = read_name(k, at)?;
    Some((due, t, q, key))
}

// ---------------------------------------------------------------------------
// phase-2 control plane
// ---------------------------------------------------------------------------

/// `(tenant, query_id)`; tenant is part of the key so query ids cannot cross
/// the authentication boundary even when a caller supplies an id.
pub fn streams_query(tenant: &str, query_id: &[u8; 16]) -> Vec<u8> {
    let mut k = with(tenant.len() + 18);
    push_name(&mut k, tenant);
    k.extend_from_slice(query_id);
    k
}

pub fn streams_queries_prefix(tenant: &str) -> Vec<u8> {
    let mut k = with(tenant.len() + 2);
    push_name(&mut k, tenant);
    k
}

pub fn streams_query_id_of(k: &[u8], prefix_len: usize) -> Option<[u8; 16]> {
    k.get(prefix_len..prefix_len + 16)?.try_into().ok()
}

/// `(query_id, pid, key)`; query ids are globally unique UUIDs and the query
/// row carries the tenant boundary.
pub fn streams_state(query_id: &[u8; 16], p: Pid, key: &str) -> Vec<u8> {
    let mut k = with(26 + key.len());
    k.extend_from_slice(query_id);
    push_u64(&mut k, p);
    push_name(&mut k, key);
    k
}

pub fn streams_state_prefix(query_id: &[u8; 16], p: Pid) -> Vec<u8> {
    let mut k = with(24);
    k.extend_from_slice(query_id);
    push_u64(&mut k, p);
    k
}

pub fn streams_query_state_prefix(query_id: &[u8; 16]) -> Vec<u8> {
    query_id.to_vec()
}

pub fn streams_state_parts(k: &[u8]) -> Option<([u8; 16], Pid, String)> {
    let query_id = k.get(..16)?.try_into().ok()?;
    let pid = read_u64(k, 16)?;
    let (key, _) = read_name(k, 24)?;
    Some((query_id, pid, key))
}

pub fn streams_state_key_of(k: &[u8]) -> Option<String> {
    Some(read_name(k, 24)?.0)
}

pub fn flag(name: &str) -> Vec<u8> {
    let mut k = with(name.len() + 2);
    push_name(&mut k, name);
    k
}

pub fn quota(kind: QuotaKind, tenant: &str) -> Vec<u8> {
    let mut k = with(tenant.len() + 3);
    k.push(kind as u8);
    push_name(&mut k, tenant);
    k
}

pub fn quota_parts(k: &[u8]) -> Option<(QuotaKind, String)> {
    let kind = QuotaKind::from_u8(*k.first()?)?;
    let (tenant, end) = read_name(k, 1)?;
    (end == k.len()).then_some((kind, tenant))
}

pub fn eph_config(tenant: &str, queue: &str) -> Vec<u8> {
    queues(tenant, queue)
}

pub fn eph_config_prefix(tenant: &str) -> Vec<u8> {
    queues_prefix(tenant)
}

fn push_optional_pid(out: &mut Vec<u8>, p: Option<Pid>) {
    match p {
        Some(p) => {
            out.push(1);
            push_u64(out, p);
        }
        None => {
            out.push(0);
            push_u64(out, 0);
        }
    }
}

/// Primary trace order: `(tenant, pid?, transaction, sequence)`.
pub fn trace(tenant: &str, p: Option<Pid>, txn: &str, seq: u64) -> Vec<u8> {
    let mut k = with(tenant.len() + txn.len() + 21);
    push_name(&mut k, tenant);
    push_optional_pid(&mut k, p);
    push_name(&mut k, txn);
    push_u64(&mut k, seq);
    k
}

pub fn trace_prefix(tenant: &str, p: Option<Pid>, txn: &str) -> Vec<u8> {
    let mut k = with(tenant.len() + txn.len() + 13);
    push_name(&mut k, tenant);
    push_optional_pid(&mut k, p);
    push_name(&mut k, txn);
    k
}

pub fn trace_seq_of(k: &[u8]) -> Option<u64> {
    read_u64(k, k.len().checked_sub(8)?)
}

/// Secondary index used by `/traces/by-name/:name`.
pub fn trace_name(tenant: &str, name: &str, created_at_us: i64, trace_id: &[u8; 16]) -> Vec<u8> {
    let mut k = with(tenant.len() + name.len() + 28);
    push_name(&mut k, tenant);
    push_name(&mut k, name);
    push_i64(&mut k, created_at_us);
    k.extend_from_slice(trace_id);
    k
}

pub fn trace_name_prefix(tenant: &str, name: &str) -> Vec<u8> {
    let mut k = with(tenant.len() + name.len() + 4);
    push_name(&mut k, tenant);
    push_name(&mut k, name);
    k
}

pub fn trace_expiry(created_at_us: i64, trace_id: &[u8; 16]) -> Vec<u8> {
    let mut k = with(24);
    push_i64(&mut k, created_at_us);
    k.extend_from_slice(trace_id);
    k
}

pub fn trace_expiry_created_of(k: &[u8]) -> Option<i64> {
    read_i64(k, 0)
}

// ---------------------------------------------------------------------------
// node-local (§6.2)
// ---------------------------------------------------------------------------

/// `(pid, base_offset)` → where THIS node put the bytes. Node-local (D8).
pub fn seg_loc(p: Pid, base_offset: u64) -> Vec<u8> {
    txns(p, base_offset)
}

pub fn seg_loc_prefix(p: Pid) -> Vec<u8> {
    pid(p)
}

pub fn seg_loc_base_of(k: &[u8]) -> Option<u64> {
    read_u64(k, 8)
}

/// `(bucket, file_id)` → the file's row. Node-local (§6.2, I11).
pub fn files(bucket: u16, file_id: u32) -> Vec<u8> {
    let mut k = with(6);
    push_u16(&mut k, bucket);
    push_u32(&mut k, file_id);
    k
}

pub fn files_prefix(bucket: u16) -> Vec<u8> {
    let mut k = with(2);
    push_u16(&mut k, bucket);
    k
}

pub fn files_parts(k: &[u8]) -> Option<(u16, u32)> {
    Some((read_u16(k, 0)?, read_u32(k, 2)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip_including_nul() {
        for s in ["", "a", "tenant␟x", "with\0nul", "\0\0", "ümlaut"] {
            let mut b = Vec::new();
            push_name(&mut b, s);
            let (back, at) = read_name(&b, 0).expect("decodes");
            assert_eq!(back, s);
            assert_eq!(at, b.len());
        }
    }

    #[test]
    fn composite_names_are_unambiguous() {
        assert_ne!(queues("ab", "c"), queues("a", "bc"));
    }

    #[test]
    fn name_order_is_string_order() {
        let mut pairs = vec!["", "a", "aa", "ab", "b", "a\0b", "a\u{1}"];
        let mut encoded: Vec<(Vec<u8>, &str)> = pairs
            .iter()
            .map(|s| {
                let mut b = Vec::new();
                push_name(&mut b, s);
                (b, *s)
            })
            .collect();
        encoded.sort();
        pairs.sort();
        let got: Vec<&str> = encoded.iter().map(|(_, s)| *s).collect();
        assert_eq!(got, pairs);
    }

    #[test]
    fn signed_keys_order_through_zero() {
        let mut v: Vec<Vec<u8>> = [-5i64, -1, 0, 1, 5, i64::MIN, i64::MAX]
            .iter()
            .map(|n| {
                let mut b = Vec::new();
                push_i64(&mut b, *n);
                b
            })
            .collect();
        v.sort();
        let back: Vec<i64> = v.iter().map(|b| read_i64(b, 0).unwrap()).collect();
        assert_eq!(back, vec![i64::MIN, -5, -1, 0, 1, 5, i64::MAX]);
    }

    #[test]
    fn unsigned_keys_order_numerically() {
        let mut v: Vec<Vec<u8>> = [0u64, 1, 255, 256, u64::MAX]
            .iter()
            .map(|n| {
                let mut b = Vec::new();
                push_u64(&mut b, *n);
                b
            })
            .collect();
        v.sort();
        let back: Vec<u64> = v.iter().map(|b| read_u64(b, 0).unwrap()).collect();
        assert_eq!(back, vec![0, 1, 255, 256, u64::MAX]);
    }

    #[test]
    fn prefixes_really_prefix_their_keys() {
        assert!(queues("t", "q").starts_with(&queues_prefix("t")));
        assert!(groups("t", "q", "g").starts_with(&groups_prefix("t", "q")));
        assert!(pending("t", "q", "g", 7).starts_with(&pending_prefix("t", "q", "g")));
        assert!(queue_partitions("t", "q", 9).starts_with(&queue_partitions_prefix("t", "q")));
        assert!(cursors(3, "g").starts_with(&cursors_prefix(3)));
        assert!(leases_by_worker("w", 3, "g").starts_with(&leases_by_worker_prefix("w")));
        assert!(dedup(3, &[7u8; 16]).starts_with(&dedup_prefix(3)));
        assert!(txns(3, 12).starts_with(&txns_prefix(3)));
        assert!(dlq("t", "q", &[1u8; 16]).starts_with(&dlq_prefix("t", "q")));
        assert!(dlq_by_pos(3, "g", -1).starts_with(&dlq_by_pos_prefix(3, "g")));
        assert!(partition_files(3, 4).starts_with(&partition_files_prefix(3)));
        assert!(files(2, 4).starts_with(&files_prefix(2)));
        assert!(counter_partition(3, Counter::Pushed).starts_with(&counter_partition_prefix(3)));
    }

    #[test]
    fn composite_parts_decode() {
        let k = pending("t", "q", "g", 42);
        assert_eq!(
            pending_parts(&k),
            Some(("t".into(), "q".into(), "g".into(), 42))
        );
        let k = leases_by_worker("w", 42, "g");
        assert_eq!(
            leases_by_worker_parts(&k),
            Some(("w".into(), 42, "g".into()))
        );
        assert_eq!(cursors_group_of(&cursors(42, "g")), Some("g".into()));
        assert_eq!(groups_group_of(&groups("t", "q", "g")), Some("g".into()));
        assert_eq!(
            queue_partitions_pid_of(&queue_partitions("t", "q", 9)),
            Some(9)
        );
        assert_eq!(txns_base_of(&txns(1, 77)), Some(77));
        assert_eq!(seg_loc_base_of(&seg_loc(1, 77)), Some(77));
        assert_eq!(files_parts(&files(255, 9)), Some((255, 9)));
        assert_eq!(partition_files_file_of(&partition_files(1, 9)), Some(9));
        let id = [3u8; 16];
        assert_eq!(
            request_expiry_parts(&request_expiry(-7, &id)),
            Some((-7, id))
        );
    }

    #[test]
    fn kv_keys_order_by_key_bytes_inside_a_namespace() {
        // COLLATE "C": byte order, and a namespace never bleeds into its
        // neighbour ("a" vs "ab") nor a tenant into another.
        let mut keys = vec!["b", "a", "a%b", "a%bc", "ab", "a_b", "\u{e9}", "Z"];
        let mut enc: Vec<(Vec<u8>, &str)> = keys.iter().map(|k| (kv("t", "ns", k), *k)).collect();
        enc.sort();
        keys.sort();
        assert_eq!(enc.iter().map(|(_, k)| *k).collect::<Vec<_>>(), keys);
        assert!(!kv("t", "ab", "x").starts_with(&kv_ns_prefix("t", "a")));
        assert!(!kv("tt", "a", "x").starts_with(&kv_tenant_prefix("t")));
        assert!(kv("t", "a", "x").starts_with(&kv_tenant_prefix("t")));
        // A key prefix is a store-key prefix.
        assert!(kv("t", "ns", "order/9f1/items").starts_with(&kv("t", "ns", "order/")));
        assert_eq!(kv_len("t", "ns", "order/9"), kv("t", "ns", "order/9").len());
        assert_eq!(kv_len("t\0", "ns", "k"), kv("t\0", "ns", "k").len());
        assert_eq!(
            kv_parts(&kv("t", "ns", "a/b")),
            Some(("t".into(), "ns".into(), "a/b".into()))
        );
        assert_eq!(kv_expiry_parts(&kv_expiry(-3, 9)), Some((-3, 9)));
        assert!(kv_expiry(1, u64::MAX) < kv_expiry(2, 0), "oldest first");
    }

    #[test]
    fn timer_keys_decode_and_order_by_due_then_name() {
        let k = timers_due(-5, "t", "q", "a\0b");
        assert_eq!(
            timers_due_parts(&k),
            Some((-5, "t".into(), "q".into(), "a\0b".into()))
        );
        // Earlier due first, whatever the names.
        assert!(timers_due(1, "z", "z", "z") < timers_due(2, "a", "a", "a"));
        assert!(timers_due(-1, "z", "z", "z") < timers_due(0, "a", "a", "a"));
        // Same due: name order.
        assert!(timers_due(7, "t", "q", "a") < timers_due(7, "t", "q", "b"));
        let tk = timers("t", "q", "laravel:job-1");
        assert!(tk.starts_with(&timers_prefix("t", "q")));
        assert!(tk.starts_with(&timers_key_prefix("t", "q", "laravel:")));
        assert!(!tk.starts_with(&timers_key_prefix("t", "q", "laravel:x")));
        assert_eq!(
            timers_key_of(&tk, timers_prefix("t", "q").len()),
            Some("laravel:job-1".into())
        );
        // A prefix never reaches into the next queue.
        assert!(!timers("t", "qq", "a").starts_with(&timers_key_prefix("t", "q", "")));
    }

    #[test]
    fn a_partition_scan_is_bounded_by_its_prefix() {
        // pid 1's rows must not be reachable from pid 0's prefix.
        let p0 = txns_prefix(0);
        assert!(!txns(1, 0).starts_with(&p0));
    }
}
