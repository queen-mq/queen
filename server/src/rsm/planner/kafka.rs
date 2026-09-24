//! A Kafka append — one partition's RecordBatch v2 bytes stored VERBATIM
//! (phase 2 of the Kafka-on-raft plan, [`crate::rsm::kafka_batch`]).
//!
//! It arrives as a [`PushCommand`] with ONE item whose frame is a stored Kafka
//! payload (`MAGIC | VERSION | batches`), so it rides the push path end to end
//! — the batcher, the drain lanes, the reply — and [`Planner::plan_push`]
//! hands it here. What differs from a push is exactly what Kafka semantics ask:
//!
//! - **no dedup probe.** Kafka deduplicates by producer sequence (the facade's
//!   window), never by message id; a Kafka record has no id of its own to probe.
//! - **one append of `count` = the records in the batches**, numbered from the
//!   partition tail, each batch's `baseOffset` stamped into its header.
//! - **synthetic hashes**, one per record (`kafka:<offset>`), so a native
//!   consumer's delivered set and ack-by-hash work on Kafka records unchanged.
//!
//! The queue and partition are resolved and created exactly as a push resolves
//! them (003 first contact) — the same lines, kept beside the push's rather than
//! shared, so the push planner stays the code it was.

use crate::rsm::effect::{Effect, Pid, QueueConfig};
use crate::rsm::entry::{Outcome, PushOutcome, PushVerdict};
use crate::rsm::kafka_batch;
use crate::rsm::store::Reads;

use super::{Overlay, Plan, Planned, Planner, PushCommand, Refusal};

/// Whether a push command is a Kafka append: one item, a stored Kafka payload.
pub(super) fn is_kafka_append(cmd: &PushCommand) -> bool {
    cmd.items.len() == 1 && kafka_batch::is_kafka(&cmd.items[0].frame)
}

/// Whether a push command is a Kafka append that rewrites its producer's
/// window ([`kafka_batch::writes_window`]). It assigns a KV version, so the
/// lanes route it to the control step and never plan it on a lane.
pub(crate) fn writes_kv(cmd: &PushCommand) -> bool {
    is_kafka_append(cmd) && kafka_batch::writes_window(&cmd.items[0].frame)
}

impl<'a, R: Reads + ?Sized> Planner<'a, R> {
    /// Plan a Kafka append (module header).
    pub fn plan_kafka_append(&self, ov: &mut Overlay, cmd: &PushCommand) -> Planned {
        let frame = &cmd.items[0].frame;
        let batches = kafka_batch::batches(frame)
            .ok_or_else(|| Refusal::client("bad_batch", "unknown stored Kafka payload version"))?;
        let heads = kafka_batch::scan(batches).map_err(|e| Refusal::client("bad_batch", e))?;
        let count = kafka_batch::record_count(&heads);
        if count == 0 || count > u32::MAX as u64 {
            return Err(Refusal::client(
                "bad_batch",
                format!("a Kafka append of {count} records"),
            ));
        }

        // ---- the queue and the partition, as `plan_push` resolves them.
        let existing_cfg = self.queue_cfg(ov, &cmd.tenant, &cmd.queue)?;
        let create_queue = existing_cfg.is_none();
        let cfg: QueueConfig = match &existing_cfg {
            Some(c) => c.clone(),
            None => {
                let mut c = cmd.create_cfg.clone();
                c.created_at_us = self.now_us;
                c
            }
        };
        let existing_pid = self.pid_of(ov, &cmd.tenant, &cmd.queue, &cmd.partition)?;
        let (pid, part_last_offset, part_last_created): (Pid, i64, i64) = match existing_pid {
            Some(pid) => {
                let part = self
                    .partition(ov, pid)?
                    .ok_or_else(|| Refusal::retry("unavailable", "partition vanished"))?;
                (pid, part.last_offset, part.last_created_at_us)
            }
            None => (ov.peek_pid(), -1, self.now_us - 1),
        };

        // ---- the idempotent producer: the facade's window, durable. One KV row
        // per (partition, producer), read here through the overlay and
        // rewritten in THIS entry, so an admitted batch and the window that
        // admitted it commit together ([`kafka_batch::check_sequence`]).
        let window_key = heads
            .first()
            .filter(|h| h.producer_id >= 0)
            .map(|h| kafka_batch::producer_key(h.producer_id, pid));
        let window_row = match (&window_key, existing_pid) {
            (Some(key), Some(_)) => self
                .kv_row(ov, &cmd.tenant, kafka_batch::PRODUCER_NS, key)?
                .filter(|r| r.expires_at_us.is_none_or(|at| at > self.now_us)),
            _ => None,
        };
        let window = window_row
            .as_ref()
            .and_then(|r| serde_json::from_slice::<kafka_batch::ProducerWindow>(&r.value).ok());
        match kafka_batch::check_sequence(&heads, window.as_ref()) {
            kafka_batch::SeqVerdict::NotIdempotent | kafka_batch::SeqVerdict::Accept => {}
            // Kafka answers a resend as the success it was, with the offsets
            // the original got, and writes nothing.
            kafka_batch::SeqVerdict::Duplicate(offset) => {
                return Ok(Plan::Empty(Outcome::Push(PushOutcome {
                    items: vec![PushVerdict::Duplicate { pid, offset }],
                })));
            }
            kafka_batch::SeqVerdict::Refuse { code, message } => {
                return Err(Refusal::client(code, message));
            }
        }

        let base = (part_last_offset + 1) as u64;
        let created_at = self.now_us.max(part_last_created + 1);

        // ---- the payload, stamped. A copy, as a push concatenates its frames
        // into a fresh blob: the command is borrowed.
        let mut blob = frame.clone();
        kafka_batch::stamp(&mut blob[kafka_batch::PREFIX_LEN..], &heads, base);
        let hashes = kafka_batch::hashes(base, count);

        let planned = blob.len() + hashes.len() + 256;
        if planned > self.cfg.entry_max_bytes {
            return Err(Refusal::client(
                "too_large",
                format!(
                    "planned Kafka append of {planned} B exceeds QUEEN_RAFT_ENTRY_MAX_BYTES ({})",
                    self.cfg.entry_max_bytes
                ),
            ));
        }

        let bucket = super::bucket_of(&cmd.tenant, &cmd.queue, &cmd.partition);
        let mut effects: Vec<Effect> = Vec::with_capacity(3);
        if create_queue {
            effects.push(Effect::QueueUpsert {
                tenant: cmd.tenant.clone(),
                queue: cmd.queue.clone(),
                cfg,
            });
        }
        if existing_pid.is_none() {
            effects.push(Effect::PartitionCreate {
                pid,
                uuid: crate::util::uuidv7_bytes(),
                tenant: cmd.tenant.clone(),
                queue: cmd.queue.clone(),
                partition: cmd.partition.clone(),
                created_at_us: self.now_us,
            });
        }
        effects.push(Effect::Append {
            pid,
            bucket,
            base_offset: base,
            count: count as u32,
            created_at_us: created_at,
            hashes,
            blob,
        });
        if let Some(key) = window_key {
            let next = kafka_batch::advance(window, &heads, base);
            effects.push(Effect::KvPut {
                tenant: cmd.tenant.clone(),
                ns: kafka_batch::PRODUCER_NS.to_string(),
                key,
                value: serde_json::to_vec(&next).unwrap_or_default(),
                // The overlay's next KV version, as `plan_kv_writes` assigns
                // one; folding the effect below advances it (I18).
                version: ov.next_kv_version,
                expires_at_us: Some(self.now_us + kafka_batch::PRODUCER_TTL_US),
                created_at_us: window_row.as_ref().map_or(self.now_us, |r| r.created_at_us),
                updated_at_us: self.now_us,
            });
        }

        // Fold for the rest of the cycle: the new tail, the new partition and
        // the synthetic hashes a native ack may resolve before this commits.
        // The dedup FRONT is not told: no push ever probes a Kafka record.
        ov.apply_effects(&effects);

        Ok(Plan::logged(
            effects,
            Outcome::Push(PushOutcome {
                items: vec![PushVerdict::Created {
                    pid,
                    offset: base,
                    created_at_us: created_at,
                }],
            }),
        ))
    }
}
