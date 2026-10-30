//! The store keys an effect makes apply write, measured BEFORE the effect is
//! logged.
//!
//! Apply refuses a key longer than the store accepts (`KeyTooLong`, LMDB's
//! 511 bytes), and that refusal is DETERMINISTIC (`ApplyError::deterministic`):
//! every node stops on the entry, and a restart replays it and stops again,
//! until an operator sets `QUEEN_RAFT_APPLY_SKIP` on every node, which skips
//! the whole entry, other clients' commands included. So no client name may
//! reach apply in a key that does not fit. The receivers check the names they
//! know about (`check_message_key_names`), but a key is often longer than the
//! name the receiver saw: apply also writes DERIVED keys (a timer's due index
//! is 8 bytes longer than the timer, a group's counters 3 bytes longer than
//! the group), and the encoder writes each `0x00` byte of a name as two bytes.
//! This module computes the longest key each effect derives, exactly as
//! `store::keys` builds it, so the planner can refuse the command (4xx
//! `name_too_long`) instead.
//!
//! The lengths are arithmetic, not built keys, so the check costs nothing on
//! the hot path; the tests compare every formula with the real key builder.

use crate::rsm::effect::{Effect, GarbageScope};

/// The bytes a name takes inside a store key: escaped (each `0x00` byte is
/// written as two) and terminated (two bytes), as `keys::push_name` writes it.
pub fn name_len(s: &str) -> usize {
    s.len() + s.bytes().filter(|&b| b == 0).count() + 2
}

/// `keys::queues(t, q)`, `keys::eph_config(t, q)`.
fn queues_len(t: &str, q: &str) -> usize {
    name_len(t) + name_len(q)
}

/// `keys::counter_queue(t, q, _)`: scope byte + names + counter id (u16).
fn counter_queue_len(t: &str, q: &str) -> usize {
    1 + queues_len(t, q) + 2
}

/// `keys::counter_group(t, q, g, _)` (the longest key a group derives; its
/// row, `keys::groups`, is 3 bytes shorter).
pub fn counter_group_len(t: &str, q: &str, g: &str) -> usize {
    1 + name_len(t) + name_len(q) + name_len(g) + 2
}

/// `keys::partitions_by_key(t, q, p)`.
pub fn partitions_by_key_len(t: &str, q: &str, p: &str) -> usize {
    name_len(t) + name_len(q) + name_len(p)
}

/// `keys::queue_partitions(t, q, pid)`.
fn queue_partitions_len(t: &str, q: &str) -> usize {
    queues_len(t, q) + 8
}

/// The longest key a partition (and the queue a first push creates with it)
/// makes: its name index, the queue's partition list, the queue's counters.
pub fn partition_keys_len(t: &str, q: &str, p: &str) -> usize {
    partitions_by_key_len(t, q, p)
        .max(queue_partitions_len(t, q))
        .max(counter_queue_len(t, q))
}

/// `keys::cursors(pid, g)`.
fn cursors_len(g: &str) -> usize {
    8 + name_len(g)
}

/// `keys::dlq(t, q, id)`.
fn dlq_len(t: &str, q: &str) -> usize {
    queues_len(t, q) + 16
}

/// `keys::dlq_by_pos(pid, g, offset)`.
fn dlq_by_pos_len(g: &str) -> usize {
    8 + name_len(g) + 8
}

/// `keys::timers_due(due, t, q, k)` (the `timers` row key is 8 bytes shorter).
pub fn timers_due_len(t: &str, q: &str, k: &str) -> usize {
    8 + name_len(t) + name_len(q) + name_len(k)
}

/// `keys::streams_state(query_id, pid, key)`.
pub fn streams_state_len(key: &str) -> usize {
    16 + 8 + name_len(key)
}

/// `keys::streams_query(t, query_id)`.
fn streams_query_len(t: &str) -> usize {
    name_len(t) + 16
}

/// `keys::quota(kind, t)`.
fn quota_len(t: &str) -> usize {
    1 + name_len(t)
}

/// `keys::counter_tenant(t, _)`: the longest key a tenant name alone makes.
fn counter_tenant_len(t: &str) -> usize {
    1 + name_len(t) + 2
}

/// `keys::flag(name)`.
fn flag_len(n: &str) -> usize {
    name_len(n)
}

/// One key that is too long: its length and the client field to blame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Overlong {
    pub key_len: usize,
    pub field: &'static str,
    pub field_len: usize,
}

impl Overlong {
    pub fn message(&self, max: usize) -> String {
        format!(
            "{} is {} B: the store key it makes would be {} B, the limit is {max} B",
            self.field, self.field_len, self.key_len
        )
    }
}

/// The field whose name is longest among `names`, for the error message.
fn blame(names: &[(&'static str, &str)]) -> (&'static str, usize) {
    names
        .iter()
        .max_by_key(|(_, s)| s.len())
        .map(|(f, s)| (*f, s.len()))
        .unwrap_or(("name", 0))
}

fn over(len: usize, max: usize, names: &[(&'static str, &str)]) -> Option<Overlong> {
    if len <= max {
        return None;
    }
    let (field, field_len) = blame(names);
    Some(Overlong {
        key_len: len,
        field,
        field_len,
    })
}

/// The longest key `e` makes apply write or delete, when it is over `max`.
///
/// Effects keyed only by pids, ids and numbers never are. `CursorSet` and
/// `CursorDelete` are checked here on their `(pid, group)` key; the group's
/// counter key also needs the partition's tenant and queue, which the planner
/// reads ([`counter_group_len`]). Traces have their own check
/// (`traces::check_keys`).
pub fn overlong(e: &Effect, max: usize) -> Option<Overlong> {
    match e {
        Effect::QueueUpsert { tenant, queue, .. } | Effect::QueueDelete { tenant, queue } => over(
            counter_queue_len(tenant, queue),
            max,
            &[("tenant", tenant), ("queue", queue)],
        ),
        Effect::GroupUpsert {
            tenant,
            queue,
            group,
            ..
        }
        | Effect::GroupDelete {
            tenant,
            queue,
            group,
        } => over(
            counter_group_len(tenant, queue, group),
            max,
            &[
                ("tenant", tenant),
                ("queue", queue),
                ("consumerGroup", group),
            ],
        ),
        Effect::PartitionCreate {
            tenant,
            queue,
            partition,
            ..
        } => over(
            partition_keys_len(tenant, queue, partition),
            max,
            &[
                ("tenant", tenant),
                ("queue", queue),
                ("partition", partition),
            ],
        ),
        Effect::CursorSet { group, .. } | Effect::CursorDelete { group, .. } => {
            over(cursors_len(group), max, &[("consumerGroup", group)])
        }
        Effect::DlqInsert {
            tenant,
            queue,
            group,
            ..
        } => over(
            dlq_len(tenant, queue)
                .max(dlq_by_pos_len(group))
                .max(counter_group_len(tenant, queue, group)),
            max,
            &[
                ("tenant", tenant),
                ("queue", queue),
                ("consumerGroup", group),
            ],
        ),
        Effect::DlqDelete { tenant, queue, .. } => over(
            dlq_len(tenant, queue),
            max,
            &[("tenant", tenant), ("queue", queue)],
        ),
        Effect::KvPut {
            tenant, ns, key, ..
        }
        | Effect::KvDelete { tenant, ns, key } => over(
            crate::rsm::store::keys::kv_len(tenant, ns, key),
            max,
            &[("tenant", tenant), ("ns", ns), ("key", key)],
        ),
        Effect::TimerUpsert {
            tenant,
            queue,
            key,
            row,
        } => over(
            timers_due_len(tenant, queue, key).max(partitions_by_key_len(
                tenant,
                queue,
                &row.partition,
            )),
            max,
            &[
                ("tenant", tenant),
                ("queue", queue),
                ("timerKey", key),
                ("partition", &row.partition),
            ],
        ),
        Effect::TimerDelete { tenant, queue, key }
        | Effect::TimerBackoff {
            tenant, queue, key, ..
        } => over(
            timers_due_len(tenant, queue, key),
            max,
            &[("tenant", tenant), ("queue", queue), ("timerKey", key)],
        ),
        Effect::StreamsQueryUpsert { tenant, .. } => {
            over(streams_query_len(tenant), max, &[("tenant", tenant)])
        }
        Effect::StreamsStatePut { key, .. } | Effect::StreamsStateDelete { key, .. } => {
            over(streams_state_len(key), max, &[("state key", key)])
        }
        Effect::FlagSet { key, .. } => over(flag_len(key), max, &[("flag", key)]),
        Effect::QuotaSet { tenant, .. } => over(quota_len(tenant), max, &[("tenant", tenant)]),
        Effect::EphemeralConfigSet { tenant, queue, .. }
        | Effect::EphemeralConfigDelete { tenant, queue } => over(
            queues_len(tenant, queue),
            max,
            &[("tenant", tenant), ("queue", queue)],
        ),
        Effect::TenantPurge { tenant } => over(
            counter_tenant_len(tenant).max(quota_len(tenant)),
            max,
            &[("tenant", tenant)],
        ),
        Effect::GarbageAdd {
            scope: GarbageScope::Group { group },
            ..
        }
        | Effect::DeleteChunk {
            scope: GarbageScope::Group { group },
            ..
        } => over(
            cursors_len(group).max(dlq_by_pos_len(group)),
            max,
            &[("consumerGroup", group)],
        ),
        _ => None,
    }
}

/// A command's planned bytes in the log, estimated the way the push and KV
/// planners estimate theirs: a fixed cost per effect plus its variable bytes.
/// Used against `QUEEN_RAFT_ENTRY_MAX_BYTES` for commands that carry many
/// effects (admin deletes, seeks, resets): one entry over the follower's frame
/// limit never replicates, and one over the codec limit stops the log writer.
pub fn planned_bytes(effects: &[Effect]) -> usize {
    effects.iter().map(effect_bytes).sum()
}

fn effect_bytes(e: &Effect) -> usize {
    const FIXED: usize = 96;
    FIXED
        + match e {
            Effect::QueueUpsert { tenant, queue, .. } => tenant.len() + queue.len() + 256,
            Effect::QueueDelete { tenant, queue }
            | Effect::EphemeralConfigDelete { tenant, queue } => tenant.len() + queue.len(),
            Effect::GroupUpsert {
                tenant,
                queue,
                group,
                ..
            }
            | Effect::GroupDelete {
                tenant,
                queue,
                group,
            } => tenant.len() + queue.len() + group.len(),
            Effect::PartitionCreate {
                tenant,
                queue,
                partition,
                ..
            } => tenant.len() + queue.len() + partition.len(),
            Effect::Append { hashes, blob, .. } => hashes.len() + blob.len(),
            Effect::CursorSet { group, .. } | Effect::CursorDelete { group, .. } => {
                group.len() + 64
            }
            Effect::DlqInsert {
                tenant,
                queue,
                group,
                txn,
                payload,
                error,
                ..
            } => tenant.len() + queue.len() + group.len() + txn.len() + payload.len() + error.len(),
            Effect::DlqDelete { tenant, queue, .. } => tenant.len() + queue.len(),
            Effect::KvPut {
                tenant,
                ns,
                key,
                value,
                ..
            } => tenant.len() + ns.len() + key.len() + value.len(),
            Effect::KvDelete { tenant, ns, key } => tenant.len() + ns.len() + key.len(),
            Effect::TimerUpsert {
                tenant,
                queue,
                key,
                row,
            } => {
                tenant.len()
                    + queue.len()
                    + key.len()
                    + row.frame.len()
                    + row.txn.len()
                    + row.partition.len()
            }
            Effect::TimerDelete { tenant, queue, key } => tenant.len() + queue.len() + key.len(),
            Effect::TimerBackoff {
                tenant,
                queue,
                key,
                last_error,
                ..
            } => {
                tenant.len() + queue.len() + key.len() + last_error.as_ref().map_or(0, String::len)
            }
            Effect::StreamsQueryUpsert { tenant, .. } => tenant.len() + 256,
            Effect::StreamsStatePut { key, value, .. } => key.len() + value.len(),
            Effect::StreamsStateDelete { key, .. } => key.len(),
            Effect::FlagSet { key, value } => key.len() + value.len(),
            Effect::QuotaSet { tenant, .. } | Effect::TenantPurge { tenant } => tenant.len() + 64,
            Effect::EphemeralConfigSet {
                tenant,
                queue,
                options,
                ..
            } => tenant.len() + queue.len() + options.len(),
            Effect::GarbageAdd { pids, .. } => pids.len() * 8,
            Effect::DeleteChunk { pids, resume, .. } => pids.len() * 8 + resume.len(),
            Effect::MembershipNote { address, .. } => address.len(),
            Effect::TraceAppend { .. } | Effect::TraceRecord { .. } => 1024,
            _ => 0,
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rsm::store::keys;

    const NAMES: &[&str] = &["", "a", "orders", "x\0y", "\0\0", "naïve-ü"];

    #[test]
    fn formulas_match_the_key_builders() {
        for t in NAMES {
            for q in NAMES {
                for g in NAMES {
                    assert_eq!(queues_len(t, q), keys::queues(t, q).len());
                    assert_eq!(queues_len(t, q), keys::eph_config(t, q).len());
                    assert_eq!(
                        counter_queue_len(t, q),
                        keys::counter_queue(t, q, keys::Counter::Pushed).len()
                    );
                    assert_eq!(
                        counter_group_len(t, q, g),
                        keys::counter_group(t, q, g, keys::Counter::Pending).len()
                    );
                    assert!(counter_group_len(t, q, g) >= keys::groups(t, q, g).len());
                    assert_eq!(
                        partitions_by_key_len(t, q, g),
                        keys::partitions_by_key(t, q, g).len()
                    );
                    assert_eq!(
                        queue_partitions_len(t, q),
                        keys::queue_partitions(t, q, 7).len()
                    );
                    assert_eq!(cursors_len(g), keys::cursors(7, g).len());
                    assert_eq!(dlq_len(t, q), keys::dlq(t, q, &[1; 16]).len());
                    assert_eq!(dlq_by_pos_len(g), keys::dlq_by_pos(7, g, 9).len());
                    assert_eq!(timers_due_len(t, q, g), keys::timers_due(-5, t, q, g).len());
                    assert!(timers_due_len(t, q, g) > keys::timers(t, q, g).len());
                    assert_eq!(
                        streams_state_len(g),
                        keys::streams_state(&[2; 16], 7, g).len()
                    );
                    assert_eq!(streams_query_len(t), keys::streams_query(t, &[3; 16]).len());
                    assert_eq!(flag_len(t), keys::flag(t).len());
                    assert_eq!(
                        counter_tenant_len(t),
                        keys::counter_tenant(t, keys::Counter::Pushed).len()
                    );
                }
            }
        }
    }

    #[test]
    fn a_long_state_key_is_caught_before_apply() {
        let ok = Effect::StreamsStateDelete {
            query_id: [0; 16],
            pid: 1,
            key: "k".repeat(485),
        };
        assert_eq!(overlong(&ok, 511), None, "485 B + 26 fits exactly");
        let bad = Effect::StreamsStateDelete {
            query_id: [0; 16],
            pid: 1,
            key: "k".repeat(486),
        };
        let o = overlong(&bad, 511).expect("486 B is one byte too long");
        assert_eq!(o.key_len, 512);
        assert_eq!(o.field, "state key");
    }

    #[test]
    fn a_nul_byte_counts_twice() {
        let mut key = "k".repeat(484);
        key.push('\0');
        let e = Effect::StreamsStateDelete {
            query_id: [0; 16],
            pid: 1,
            key,
        };
        assert!(
            overlong(&e, 511).is_some(),
            "485 bytes, one of them NUL, makes 512"
        );
    }

    #[test]
    fn the_derived_timer_key_is_the_one_measured() {
        let tenant = "t".repeat(36);
        // timers = 36+2 + q+2 + k+2; timers_due is 8 bytes more.
        let e = Effect::TimerDelete {
            tenant,
            queue: "q".repeat(200),
            key: "k".repeat(265),
        };
        // 8 + 38 + 202 + 267 = 515 > 511, while the timers row alone (507) fits.
        assert_eq!(overlong(&e, 511).map(|o| o.key_len), Some(515));
    }
}
