//! Positions: set or forget where a consumer group reads a partition, as a
//! rider of the transaction wire (`"positions": [...]`, served inside the one
//! `Transaction` command, so it is all-or-nothing with everything else the
//! bundle carries — a `required` KV precondition beside it included).
//!
//! This module is the receiver's half: the wire shape and its validation
//! ([`parse_position_ops`]). The consumption engine ([`crate::rsm::consume`])
//! owns every cursor and serves the ops (its `txn_prepare`), all but one
//! kind: an op that names a partition that does not exist yet. Allocating a
//! partition is the planner's, so the engine leaves those in the bundle and
//! [`Planner::plan_positions`] creates the partition, registers the group on
//! its first contact and writes the cursor row, in the bundle's entry. What
//! follows is the contract both keep.
//!
//! A POSITION is the next offset a group reads on a partition. It is stored as
//! the group's cursor row, `committed = offset - 1` — the same row Queen's own
//! pop and ack keep — so a group positioned here and a group that consumed
//! there are one group: a native consumer resumes where a position was set,
//! and a position read back reflects what a native consumer acked. Nothing in
//! the planner, apply or the store knows who set it.
//!
//! What one op does, in order:
//!
//! - the QUEUE must exist (a position is never what creates a queue);
//! - a partition NAME with no partition yet is created, exactly as a first
//!   push creates it — a partition is a name, and a position on an empty one
//!   is a position all the same (forgetting one there is a no-op);
//! - the GROUP is registered on the queue if it is not yet (the one-time
//!   `GroupUpsert` a first pop writes, from the op's subscription intent), so
//!   the group is a group everywhere a registered one is — its pending rows
//!   and counters, the console, a later delete of the group;
//! - the cursor row is written WHOLE, carrying forward what it had
//!   (`total_consumed`, `created_at_us`) and releasing any lease: a position
//!   set is authoritative, like a seek, and a batch leased before it is not
//!   acked against it. The op's metadata goes on the row
//!   ([`crate::rsm::effect::CursorRow::metadata`]). No clamping: a position
//!   below the log start or past its end is stored as given, which is what a
//!   client that set it will read back;
//! - a set that would leave the row exactly as it is writes NOTHING — no
//!   effect, and no entry when nothing else in the bundle writes either.

use std::collections::{HashMap, HashSet};

use crate::rsm::effect::{CursorRow, Effect, Pid};
use crate::rsm::store::rows::cursor_fresh;
use crate::rsm::store::Reads;

use super::{group_meta_for_registration, Overlay, Planner, Refusal, SubIntent};

/// The most positions one call may carry. A bound on one command's planning
/// time on the serial planner (a few µs per op) and on its entry, not a
/// semantic limit: a caller with more sends more calls.
pub const MAX_POSITIONS_PER_CALL: usize = 16_384;

/// The longest metadata string one position may carry, in bytes — Kafka's own
/// `offset.metadata.max.bytes` default, so a Kafka client meets the number it
/// already knows.
pub const MAX_METADATA_BYTES: usize = 4096;

/// One position op, receiver-validated ([`parse_position_ops`]).
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PositionOp {
    pub queue: String,
    pub partition: String,
    pub group: String,
    /// The next offset the group reads; the cursor stores `offset - 1`.
    /// `None` FORGETS the position: the cursor row is removed.
    pub offset: Option<u64>,
    /// Stored on the cursor row; `""` = none.
    pub metadata: String,
    /// How the group is registered when this op is its first contact with the
    /// queue (a pop's intent, normalized by the receiver).
    pub sub: SubIntent,
}

/// A receiver-side refusal: the transaction's `reason` code and the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionInvalid {
    pub reason: &'static str,
    pub detail: String,
}

fn invalid(i: usize, detail: impl Into<String>) -> PositionInvalid {
    PositionInvalid {
        reason: "bad_request",
        detail: format!("positions[{i}]: {}", detail.into()),
    }
}

/// The registration mode a position uses when the op names none: the broker's
/// `DEFAULT_SUBSCRIPTION_MODE`, as a pop without one would use it.
fn default_mode() -> &'static str {
    static MODE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    MODE.get_or_init(|| {
        crate::config::normalize_subscription_mode(
            &std::env::var("DEFAULT_SUBSCRIPTION_MODE").unwrap_or_else(|_| "new".to_string()),
        )
    })
}

/// Validate the `positions` rider:
///
/// ```text
/// {"queue": "orders", "partition": "3", "consumerGroup": "billing",
///  "offset": 42 | null, "metadata": "…"?, "subscriptionMode": "new"|"all"?}
/// ```
///
/// `partition` defaults to `Default` (as a push's does); `offset` is REQUIRED
/// and is the next offset to read, or `null` to forget the position. Names are
/// checked against the store key budget here, before anything is planned.
pub fn parse_position_ops(
    values: &[serde_json::Value],
    tenant: &str,
) -> Result<Vec<PositionOp>, PositionInvalid> {
    if values.len() > MAX_POSITIONS_PER_CALL {
        return Err(PositionInvalid {
            reason: "bad_request",
            detail: format!(
                "{} positions in one call, the ceiling is {MAX_POSITIONS_PER_CALL}",
                values.len()
            ),
        });
    }
    let mut out = Vec::with_capacity(values.len());
    for (i, v) in values.iter().enumerate() {
        let o = v
            .as_object()
            .ok_or_else(|| invalid(i, "a position is an object"))?;
        let text = |k: &str| o.get(k).and_then(serde_json::Value::as_str);
        let queue = text("queue")
            .filter(|q| !q.is_empty())
            .ok_or_else(|| invalid(i, "queue is required"))?;
        let group = text("consumerGroup")
            .filter(|g| !g.is_empty())
            .ok_or_else(|| invalid(i, "consumerGroup is required"))?;
        let partition = text("partition")
            .filter(|p| !p.is_empty())
            .unwrap_or("Default");
        let offset = match o.get("offset") {
            Some(serde_json::Value::Null) => None,
            Some(n) => Some(
                n.as_u64()
                    .filter(|n| *n <= i64::MAX as u64)
                    .ok_or_else(|| invalid(i, "offset is a non-negative integer or null"))?,
            ),
            None => return Err(invalid(i, "offset is required (null forgets the position)")),
        };
        let metadata = match o.get("metadata") {
            None | Some(serde_json::Value::Null) => String::new(),
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(_) => return Err(invalid(i, "metadata is a string")),
        };
        if metadata.len() > MAX_METADATA_BYTES {
            return Err(PositionInvalid {
                reason: "too_large",
                detail: format!(
                    "positions[{i}]: metadata of {} B exceeds {MAX_METADATA_BYTES} B",
                    metadata.len()
                ),
            });
        }
        let mode = match text("subscriptionMode") {
            Some(m) => crate::config::normalize_subscription_mode(m),
            None => default_mode().to_string(),
        };
        crate::rsm::facade::check_message_key_names(tenant, queue, Some(group), Some(partition))
            .map_err(|e| PositionInvalid {
                reason: "name_too_long",
                detail: format!("positions[{i}]: {e}"),
            })?;
        out.push(PositionOp {
            queue: queue.to_string(),
            partition: partition.to_string(),
            group: group.to_string(),
            offset,
            metadata,
            sub: SubIntent {
                mode,
                from_us: None,
                now: false,
            },
        });
    }
    Ok(out)
}

impl<'a, R: Reads + ?Sized> Planner<'a, R> {
    /// Plan the positions of one call on partitions that do not exist yet,
    /// against `ov` WITHOUT folding them (the caller folds the bundle whole).
    /// The effects, in op order: the `PartitionCreate` of a partition the op
    /// names first, a `GroupUpsert` for a group's first contact with the
    /// queue, and the `CursorSet`. Forgetting a position on a partition that
    /// does not exist writes nothing. An op on a partition that exists
    /// (committed or in flight) is the engine's and is refused.
    pub fn plan_positions(
        &self,
        ov: &Overlay,
        tenant: &str,
        ops: &[PositionOp],
    ) -> Result<Vec<Effect>, Refusal> {
        let mut effects: Vec<Effect> = Vec::with_capacity(ops.len() * 2);
        // What this call already decided, layered over the overlay.
        let mut queues: HashSet<&str> = HashSet::new();
        let mut created: HashMap<(&str, &str), Pid> = HashMap::new();
        let mut next_pid = ov.peek_pid();
        let mut registered: HashSet<(&str, &str)> = HashSet::new();
        // The rows this call wrote on the partitions it created.
        let mut rows: HashMap<(Pid, &str), Option<CursorRow>> = HashMap::new();
        let mut planned_bytes = 0usize;
        // A position's metadata is a catalogue version 2 shape (D20).
        let cluster = self.cluster_version()?;

        for op in ops {
            if !queues.contains(op.queue.as_str()) {
                if self.queue_cfg(ov, tenant, &op.queue)?.is_none() {
                    return Err(Refusal::client(
                        "queue_not_found",
                        format!("queue {} does not exist", op.queue),
                    ));
                }
                queues.insert(op.queue.as_str());
            }
            let key = (op.queue.as_str(), op.partition.as_str());
            let pid = match created.get(&key) {
                Some(pid) => *pid,
                None => {
                    if self.pid_of(ov, tenant, &op.queue, &op.partition)?.is_some() {
                        return Err(Refusal::retry(
                            "internal",
                            format!(
                                "a position on the existing partition {}/{} is served by the \
                                 consumption engine",
                                op.queue, op.partition
                            ),
                        ));
                    }
                    let Some(_) = op.offset else {
                        continue;
                    };
                    let pid = next_pid;
                    next_pid += 1;
                    effects.push(Effect::PartitionCreate {
                        pid,
                        uuid: crate::util::uuidv7_bytes(),
                        tenant: tenant.to_string(),
                        queue: op.queue.clone(),
                        partition: op.partition.clone(),
                        created_at_us: self.now_us,
                    });
                    created.insert(key, pid);
                    pid
                }
            };
            let rkey = (pid, op.group.as_str());
            let current = rows.get(&rkey).cloned().flatten();
            let Some(offset) = op.offset else {
                // Forgotten on a partition this call created: the row goes if
                // an earlier op of the call wrote one.
                if current.is_some() {
                    effects.push(Effect::CursorDelete {
                        pid,
                        group: op.group.clone(),
                    });
                }
                rows.insert(rkey, None);
                continue;
            };
            let gkey = (op.queue.as_str(), op.group.as_str());
            if !registered.contains(&gkey) {
                if self.group(ov, tenant, &op.queue, &op.group)?.is_none() {
                    effects.push(Effect::GroupUpsert {
                        tenant: tenant.to_string(),
                        queue: op.queue.clone(),
                        group: op.group.clone(),
                        meta: group_meta_for_registration(
                            crate::util::uuidv7_bytes(),
                            "",
                            "",
                            "",
                            &op.sub,
                            false,
                            self.now_us,
                        ),
                    });
                }
                registered.insert(gkey);
            }
            let mut row = cursor_fresh(-1, self.now_us);
            // `offset <= i64::MAX` (the receiver checked it), so this is exact.
            row.committed = offset as i64 - 1;
            row.metadata = op.metadata.clone();
            row.admit(cluster);
            if current.as_ref() == Some(&row) {
                // Set again to where an earlier op of the call put it.
                continue;
            }
            planned_bytes += 96 + op.group.len() + row.metadata.len();
            effects.push(Effect::CursorSet {
                pid,
                group: op.group.clone(),
                row: row.clone(),
            });
            rows.insert(rkey, Some(row));
        }
        // §5.1 413: the planned size of the command's positions.
        if planned_bytes > self.cfg.entry_max_bytes {
            return Err(Refusal::client(
                "too_large",
                format!(
                    "planned positions of {planned_bytes} B exceed QUEEN_RAFT_ENTRY_MAX_BYTES ({})",
                    self.cfg.entry_max_bytes
                ),
            ));
        }
        Ok(effects)
    }
}
