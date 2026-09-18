//! Push — the port of `003_log_push.sql` (`log_push_one_v1` /
//! `log_push_multi_v1`).
//!
//! Probe then allocate. A duplicate carries the ORIGINAL offset (the min over
//! the dedup window) and contributes nothing to the append; the survivors get
//! one gapless run. Offsets are gapless and `created_at` is strictly monotone
//! per partition (`max(now, last_created_at + 1)`, PUSHSER) so every timestamp
//! walk downstream — retention, seeding, the pop head probe — holds.
//!
//! Divergence from the SQL shape, not its behaviour (the same note pgless
//! carried): `log_push_one_v1` answers `duplicate` for a whole segment and
//! leaves fusion.rs to repack the survivors into a second call. Here the window
//! is an exact store index, so the split happens once — and O20 keeps the split
//! on the RECEIVER: it pre-packs one frame per message, and the survivors'
//! frames are CONCATENATED into the append blob, never repacked here.

use crate::rsm::effect::{Effect, Pid, QueueConfig};
use crate::rsm::entry::{Outcome, PushOutcome, PushVerdict};

use super::{Overlay, Plan, Planned, Planner, PushCommand, SEC_US};
use crate::rsm::store::Reads;

impl<'a, R: Reads + ?Sized> Planner<'a, R> {
    /// Plan a push of one partition (003).
    pub fn plan_push(&self, ov: &mut Overlay, cmd: &PushCommand) -> Planned {
        if cmd.items.is_empty() {
            // Nothing to do and nothing to answer beyond an empty verdict list.
            return Ok(Plan::Empty(Outcome::Push(PushOutcome::default())));
        }

        // Resolve the queue, remembering whether it must be created (003 first
        // contact provisions the queue and the partition).
        let existing_cfg = self.queue_cfg(ov, &cmd.tenant, &cmd.queue)?;
        let create_queue = existing_cfg.is_none();
        let cfg: QueueConfig = match &existing_cfg {
            Some(c) => c.clone(),
            None => {
                let mut c = cmd.create_cfg.clone();
                // The planner stamps the creation time (D5); the receiver only
                // minted the id.
                c.created_at_us = self.now_us;
                c
            }
        };

        // Resolve the partition. Only an existing partition can hold a
        // duplicate — a fresh one has no dedup rows — so the probe runs only
        // then, and an all-duplicate push therefore never created anything.
        let existing_pid = self.pid_of(ov, &cmd.tenant, &cmd.queue, &cmd.partition)?;

        let (pid, part_last_offset, part_last_created): (Pid, i64, i64) = match existing_pid {
            Some(pid) => {
                let part = self
                    .partition(ov, pid)?
                    .ok_or_else(|| super::Refusal::retry("unavailable", "partition vanished"))?;
                (pid, part.last_offset, part.last_created_at_us)
            }
            None => {
                // A partition created here: its creation stamp is a FLOOR for
                // the first segment, so `last_created` starts one µs below
                // `now` and the first frame stamps exactly `now`
                // (PartitionRow::new).
                (ov.peek_pid(), -1, self.now_us - 1)
            }
        };

        // ---- dedup verdicts (input order), only against an existing partition.
        let mut verdicts: Vec<Option<PushVerdict>> = vec![None; cmd.items.len()];
        if existing_pid.is_some() && cfg.dedup_window_seconds > 0 {
            let floor = self
                .now_us
                .saturating_sub(cfg.dedup_window_seconds as i64 * SEC_US);
            for (i, it) in cmd.items.iter().enumerate() {
                if let Some(off) = self.dedup_probe_one(ov, pid, &it.hash, floor)? {
                    verdicts[i] = Some(PushVerdict::Duplicate { pid, offset: off });
                }
            }
        }

        let survivors: Vec<usize> = (0..cmd.items.len())
            .filter(|i| verdicts[*i].is_none())
            .collect();

        if survivors.is_empty() {
            // Every frame was a duplicate: no effect, nothing created, answered
            // at once and not logged (§5.4). The verdicts are read back exactly
            // on a retry, so recording the id would only make cost follow the
            // push-retry rate (G-3).
            let items = verdicts.into_iter().map(|v| v.unwrap()).collect();
            return Ok(Plan::Empty(Outcome::Push(PushOutcome { items })));
        }

        let base = (part_last_offset + 1) as u64;
        let created_at = self.now_us.max(part_last_created + 1);

        // ---- assemble the append: survivors' frames concatenated (O20), their
        // hashes concatenated in frame order (16 B each, the codec asserts the
        // stride).
        let mut blob: Vec<u8> = Vec::new();
        let mut hashes: Vec<u8> = Vec::with_capacity(survivors.len() * 16);
        for &i in &survivors {
            blob.extend_from_slice(&cmd.items[i].frame);
            hashes.extend_from_slice(&cmd.items[i].hash);
        }

        // §5.1 413: a single command whose planned size exceeds
        // `QUEEN_RAFT_ENTRY_MAX_BYTES`. The blob and the hashes dominate; the
        // header and the verdicts are bounded by the item count.
        let planned = blob.len() + hashes.len() + cmd.items.len() * 32 + 256;
        if planned > self.cfg.entry_max_bytes {
            return Err(super::Refusal::client(
                "too_large",
                format!(
                    "planned push of {planned} B exceeds QUEEN_RAFT_ENTRY_MAX_BYTES ({})",
                    self.cfg.entry_max_bytes
                ),
            ));
        }

        let bucket = super::bucket_of(&cmd.tenant, &cmd.queue, &cmd.partition);
        let count = survivors.len() as u32;

        let mut effects: Vec<Effect> = Vec::with_capacity(3);
        if create_queue {
            effects.push(Effect::QueueUpsert {
                tenant: cmd.tenant.clone(),
                queue: cmd.queue.clone(),
                cfg: cfg.clone(),
            });
        }
        if existing_pid.is_none() {
            // Apply builds the row with `PartitionRow::new`, which sets
            // `last_created = created_at - 1`; that is why the first push above
            // stamped exactly `now`.
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
            count,
            created_at_us: created_at,
            hashes,
            blob,
        });

        for (k, &i) in survivors.iter().enumerate() {
            verdicts[i] = Some(PushVerdict::Created {
                pid,
                offset: base + k as u64,
                created_at_us: created_at,
            });
        }
        let items = verdicts.into_iter().map(|v| v.unwrap()).collect();

        // Fold the command's effects so a later command in the cycle sees the
        // new partition, the new tail and the new dedup occurrences (§7.2).
        ov.apply_effects(&effects);
        Ok(Plan::logged(effects, Outcome::Push(PushOutcome { items })))
    }
}
