//! Ack, renew and the DLQ head — the port of `005_log_ack.sql`.
//!
//! * [`Planner::plan_ack`] — the hash-resolved ack (`log_ack_by_hash_v1` /
//!   `log_ack_multi_v1`) with the O16 full-batch fast path: implicit ack, the
//!   explicit signal that is never skipped, below-cursor honesty, the single
//!   retry budget charged only by an explicit `failed`, the forced-DLQ handoff
//!   (filed IN THE SAME entry per O7, from the receiver's snapshot per O20).
//! * [`Planner::plan_ack_positional`] — `log_ack_v1` / `log_ack_at_v1`: advance
//!   the cursor to an absolute offset, validated against the lease.
//! * [`Planner::plan_nack`] — release a lease, cursor untouched.
//! * [`Planner::plan_renew`] — `log_renew_lease_v1`: renew every live lease of a
//!   worker, GREATEST so a renew never shortens, reporting the MIN expiry.
//! * [`Planner::plan_dlq_head`] — `log_dlq_head_v1` as a standalone command.
//!
//! The O16 resolution of the hazard §8 names: the delivered set is recorded on
//! the cursor at claim, so which path an ack takes is a function of committed
//! state, not of a RAM map a failover loses.

use std::collections::BTreeSet;

use crate::rsm::effect::{CursorRow, Effect, Pid};
use crate::rsm::entry::{AckOutcome, AckResult, DlqHeadOutcome, Outcome, RenewOutcome};
use crate::rsm::store::{Reads, TypedReads};

use super::{
    store_err, AckCommand, AckPositionalCommand, AckStatus, AckTarget, DlqHeadCommand, DlqSnapshot,
    NackCommand, Overlay, Plan, Planned, Planner, Refusal, RenewCommand, SEC_US,
};

const DEFAULT_DLQ_ERROR: &str = "Retries exhausted";

fn is_signal(s: AckStatus) -> bool {
    matches!(s, AckStatus::Failed | AckStatus::Dlq | AckStatus::Retry)
}

/// At one offset `dlq` beats `failed` beats `retry` (005's ORDER BY tie-break).
fn signal_rank(s: AckStatus) -> u8 {
    match s {
        AckStatus::Dlq => 0,
        AckStatus::Failed => 1,
        _ => 2,
    }
}

impl<'a, R: Reads + ?Sized> Planner<'a, R> {
    /// Hash-resolved ack of one or more `(pid, group)` targets (005).
    pub fn plan_ack(&self, ov: &mut Overlay, cmd: &AckCommand) -> Planned {
        let mut effects: Vec<Effect> = Vec::new();
        let mut results: Vec<AckResult> = Vec::with_capacity(cmd.targets.len());
        for target in &cmd.targets {
            let (res, mut effs) = self.ack_target(ov, target)?;
            results.push(res);
            effects.append(&mut effs);
        }
        let outcome = Outcome::Ack(AckOutcome { results });
        if effects.is_empty() {
            return Ok(Plan::Empty(outcome));
        }
        ov.apply_effects(&effects);
        Ok(Plan::logged(effects, outcome))
    }

    pub(super) fn ack_target(
        &self,
        ov: &Overlay,
        target: &AckTarget,
    ) -> Result<(AckResult, Vec<Effect>), Refusal> {
        let pid = target.pid;
        let group = &target.group;
        let worker = &target.worker;
        let now = self.now_us;

        let reject = |committed: i64, hashes: &[super::AckItem]| AckResult {
            pid,
            committed,
            acked: 0,
            conflated: 0,
            dlq: 0,
            lease_released: false,
            batch_retry_count: 0,
            noop_hashes: Vec::new(),
            // The outcome shape (WP-1.1) carries no per-item error, so a target
            // that cannot be acked reports its hashes as stale — the receiver
            // maps them back and answers the client (R-101 refines the shape).
            stale_hashes: hashes.iter().map(|i| i.hash).collect(),
        };

        use crate::rsm::dbgctr::{inc, C};
        let Some(part) = self.partition(ov, pid)? else {
            inc(&C.ack_reject, 1);
            return Ok((reject(-1, &target.items), Vec::new()));
        };
        let Some(cur0) = self.cursor(ov, pid, group)? else {
            inc(&C.ack_reject, 1);
            return Ok((reject(-1, &target.items), Vec::new()));
        };
        // The lease is validated ONLY when a non-empty leaseId was supplied; a
        // lease-less ack falls through and still advances (005 RUSTFIX item 11).
        if !worker.is_empty()
            && (cur0.worker.as_deref() != Some(worker.as_str())
                || cur0.lease_expires_at_us.is_none_or(|e| e < now))
        {
            inc(&C.ack_reject, 1);
            return Ok((reject(cur0.committed, &target.items), Vec::new()));
        }

        let committed = cur0.committed;
        let has_lease = cur0.batch_end.is_some();
        let batch_end = cur0.batch_end.unwrap_or(u64::MAX);
        let txns_start = part.txns_start;
        let lo = ((committed + 1).max(txns_start as i64)).max(0) as u64;
        let hi = if has_lease { batch_end } else { u64::MAX };

        // ---- O16 full-batch fast path: the OK hashes exactly cover the
        // delivered set of a live lease, and there is no explicit signal.
        let acked_ok: BTreeSet<[u8; 16]> = target
            .items
            .iter()
            .filter(|i| matches!(i.status, AckStatus::Ok))
            .map(|i| i.hash)
            .collect();
        let has_signal = target.items.iter().any(|i| is_signal(i.status));
        let delivered_set: BTreeSet<[u8; 16]> = cur0.delivered.iter().copied().collect();
        if has_lease
            && !has_signal
            && !delivered_set.is_empty()
            && acked_ok == delivered_set
            && cur0.worker.as_deref() == Some(worker.as_str())
        {
            let mut cur = cur0.clone();
            let new = batch_end as i64;
            let delta = (new - committed).max(0);
            let conflated = if cur.lease_conflated {
                (delta - 1).max(0)
            } else {
                0
            };
            release_lease(&mut cur);
            cur.committed = new;
            cur.attempt_offset = None;
            cur.attempt_count = 0;
            cur.batch_retry_count = 0;
            cur.total_consumed += delta as u64;
            let res = AckResult {
                pid,
                committed: new,
                acked: delta as u32,
                conflated: conflated as u32,
                dlq: 0,
                lease_released: true,
                batch_retry_count: 0,
                noop_hashes: Vec::new(),
                stale_hashes: Vec::new(),
            };
            let effs = if cur != cur0 {
                vec![Effect::CursorSet {
                    pid,
                    group: group.clone(),
                    row: cur,
                }]
            } else {
                Vec::new()
            };
            inc(&C.ack_fast, 1);
            return Ok((res, effs));
        }

        // ---- slow path: resolve every item, then the pgless branch logic.
        struct Resolved {
            eff: Option<i64>,
            below: bool,
            ok: bool,
            signal: Option<AckStatus>,
        }
        let mut resolved: Vec<Resolved> = Vec::with_capacity(target.items.len());
        for it in &target.items {
            let r = self.dedup_resolve(ov, pid, &it.hash, lo, hi, committed, txns_start)?;
            resolved.push(Resolved {
                eff: r.eff.map(|o| o as i64),
                below: r.below,
                ok: matches!(it.status, AckStatus::Ok),
                signal: if is_signal(it.status) {
                    Some(it.status)
                } else {
                    None
                },
            });
        }

        // The head signal: the LOWEST explicit signal in the span.
        let mut sig: Option<(i64, AckStatus, usize)> = None;
        for (i, r) in resolved.iter().enumerate() {
            if let (Some(off), Some(kind)) = (r.eff, r.signal) {
                let better = match sig {
                    None => true,
                    Some((o, k, _)) => off < o || (off == o && signal_rank(kind) < signal_rank(k)),
                };
                if better {
                    sig = Some((off, kind, i));
                }
            }
        }
        let sig_off = sig.map(|(o, _, _)| o);
        let sig_kind = sig.map(|(_, k, _)| k);

        // Implicit ack, clamped below the head signal.
        let max_ok = resolved
            .iter()
            .filter(|r| r.ok)
            .filter_map(|r| r.eff)
            .filter(|off| sig_off.is_none_or(|s| *off < s))
            .max();

        let mut cur = cur0.clone();
        let mut new = max_ok.unwrap_or(committed);
        let mut delta = (new - committed).max(0);
        let mut conflated: i64 = 0;
        let mut dlq_filed = 0u32;
        let mut effects: Vec<Effect> = Vec::new();

        let cfg = self.queue_cfg(ov, &target.tenant, &target.queue)?;
        let retry_limit = cfg
            .as_ref()
            .map(|c| c.retry_limit.max(0) as u32)
            .unwrap_or(0);
        // DLQ defaults to TRUE for an unconfigured queue (005 RUSTFIX item 2).
        let dlq_enabled = cfg
            .as_ref()
            .map(|c| c.dead_letter_queue || c.dlq_after_max_retries)
            .unwrap_or(true);

        let dlq_error = || -> String {
            sig.and_then(|(_, _, i)| target.items[i].error.clone())
                .or_else(|| {
                    target
                        .items
                        .iter()
                        .filter(|i| !matches!(i.status, AckStatus::Ok))
                        .find_map(|i| i.error.clone())
                })
                .unwrap_or_else(|| DEFAULT_DLQ_ERROR.to_string())
        };
        let dlq_snapshot = || -> DlqSnapshot {
            sig.and_then(|(_, _, i)| target.items[i].snapshot.clone())
                .unwrap_or_default()
        };

        if sig_kind == Some(AckStatus::Dlq) && has_lease {
            // Forced DLQ: advance the completed prefix and file the poison in
            // this entry (O7); the lease is released here (the RSM does not need
            // pgless's keep-lease-for-a-second-call handoff, since the receiver
            // already snapshotted the frame, O20).
            cur.committed = new;
            cur.total_consumed += delta as u64;
            self.file_dlq(
                &mut effects,
                target,
                cur.committed,
                cur.batch_retry_count,
                sig_off.unwrap(),
                &dlq_error(),
                dlq_snapshot(),
                now,
            );
            dlq_filed = 1;
            // Advance past the poison and release, resetting the batch.
            cur.committed = cur.committed.max(sig_off.unwrap());
            release_lease(&mut cur);
            cur.attempt_offset = None;
            cur.attempt_count = 0;
            cur.batch_retry_count = 0;
            cur.total_consumed += 1;
            new = cur.committed;
            delta = (new - committed).max(0);
        } else if sig_kind == Some(AckStatus::Failed) && has_lease {
            let retry_ct = cur.batch_retry_count;
            if retry_ct < retry_limit {
                // Budget remains: release so the poison redelivers; charge once.
                cur.committed = new;
                release_lease(&mut cur);
                cur.batch_retry_count = retry_ct + 1;
                cur.total_consumed += delta as u64;
            } else if dlq_enabled {
                // Budget exhausted + DLQ: file the poison, release, reset.
                cur.committed = new;
                cur.total_consumed += delta as u64;
                self.file_dlq(
                    &mut effects,
                    target,
                    cur.committed,
                    cur.batch_retry_count,
                    sig_off.unwrap(),
                    &dlq_error(),
                    dlq_snapshot(),
                    now,
                );
                dlq_filed = 1;
                cur.committed = cur.committed.max(sig_off.unwrap());
                release_lease(&mut cur);
                cur.attempt_offset = None;
                cur.attempt_count = 0;
                cur.batch_retry_count = 0;
                cur.total_consumed += 1;
                new = cur.committed;
                delta = (new - committed).max(0);
            } else {
                // Budget exhausted, no DLQ: drop the poison, advance past it.
                new = sig_off.unwrap();
                cur.committed = new;
                release_lease(&mut cur);
                cur.batch_retry_count = 0;
                cur.total_consumed += delta as u64 + 1;
                delta = (new - committed).max(0);
            }
        } else if sig_kind == Some(AckStatus::Retry) && has_lease {
            // Budget-free retry: release, cursor at the completed prefix.
            cur.committed = new;
            release_lease(&mut cur);
            cur.total_consumed += delta as u64;
        } else {
            // All completed, or a lease-less ack.
            if cur.lease_conflated && has_lease && max_ok.is_some() {
                // A conflating lease delivered ONE frame at batch_end; a clean
                // ack completes the whole leased span (§2.4).
                new = batch_end as i64;
                delta = (new - committed).max(0);
                conflated = (delta - 1).max(0);
            }
            let reached_end = has_lease && new >= batch_end as i64;
            if reached_end {
                cur.committed = new;
                release_lease(&mut cur);
                cur.attempt_offset = None;
                cur.attempt_count = 0;
                cur.batch_retry_count = 0;
                cur.total_consumed += delta as u64;
            } else {
                cur.committed = new;
                if has_lease {
                    // Keep the attempt marker on the first uncommitted frame of
                    // a live partial lease.
                    cur.attempt_offset = Some((new + 1) as u64);
                }
                cur.total_consumed += delta as u64;
            }
        }

        // Per-item classification into the noop/stale vocabulary.
        let mut noop_hashes: Vec<[u8; 16]> = Vec::new();
        let mut stale_hashes: Vec<[u8; 16]> = Vec::new();
        let mut honored = 0u32;
        for (r, it) in resolved.iter().zip(&target.items) {
            if r.eff.is_none() && !r.below {
                // Unresolvable: the cursor did not move for it.
                stale_hashes.push(it.hash);
            } else if r.below && r.eff.is_none() {
                if r.signal.is_some() {
                    stale_hashes.push(it.hash);
                } else {
                    noop_hashes.push(it.hash);
                }
            } else {
                honored += 1;
            }
        }

        let lease_released = cur.worker.is_none() && cur0.worker.is_some();
        if !has_lease {
            inc(&C.ack_slow_nolease, 1);
        } else if cur.worker.is_none() {
            inc(&C.ack_slow_released, 1);
        } else {
            inc(&C.ack_slow_kept, 1);
        }
        let res = AckResult {
            pid,
            committed: cur.committed,
            acked: delta.max(0) as u32,
            conflated: conflated.max(0) as u32,
            dlq: dlq_filed,
            lease_released: lease_released || (cur.batch_end.is_none() && cur0.batch_end.is_some()),
            batch_retry_count: cur.batch_retry_count,
            noop_hashes,
            stale_hashes,
        };
        let _ = honored;

        if cur != cur0 {
            effects.push(Effect::CursorSet {
                pid,
                group: group.clone(),
                row: cur,
            });
        }
        Ok((res, effects))
    }

    #[allow(clippy::too_many_arguments)]
    fn file_dlq(
        &self,
        effects: &mut Vec<Effect>,
        target: &AckTarget,
        _committed: i64,
        retry_count: u32,
        offset: i64,
        error: &str,
        snap: DlqSnapshot,
        now_us: i64,
    ) {
        effects.push(Effect::DlqInsert {
            dlq_id: crate::util::uuidv7_bytes(),
            tenant: target.tenant.clone(),
            queue: target.queue.clone(),
            pid: target.pid,
            group: target.group.clone(),
            offset,
            message_id: snap.message_id,
            txn: snap.txn,
            payload: snap.payload,
            error: error.to_string(),
            retry_count,
            failed_at_us: now_us,
        });
    }

    /// Positional ack of one leased batch (`log_ack_v1` / `log_ack_at_v1`).
    pub fn plan_ack_positional(&self, ov: &mut Overlay, cmd: &AckPositionalCommand) -> Planned {
        let pid = cmd.pid;
        let group = &cmd.group;
        let now = self.now_us;
        let empty = || Outcome::Ack(AckOutcome::default());

        let Some(cur0) = self.cursor(ov, pid, group)? else {
            return Ok(Plan::Empty(empty()));
        };
        if !cmd.worker.is_empty()
            && (cur0.worker.as_deref() != Some(cmd.worker.as_str())
                || cur0.lease_expires_at_us.is_none_or(|e| e < now))
        {
            return Err(Refusal::client("bad_lease", "invalid or expired lease"));
        }
        let mut cur = cur0.clone();
        let old_committed = cur0.committed;
        let (acked, conflated): (u32, u32) = match cmd.ok {
            true => {
                let Some(be) = cur.batch_end else {
                    return Err(Refusal::client("no_batch", "no leased batch"));
                };
                // A full Streams cycle intentionally names no absolute offset:
                // the recorded batch_end is the authority. A partial gate
                // cycle advances exactly `acked_count` delivered frames from
                // the attempt head and keeps the lease for the denied tail.
                let upto = cmd.upto.unwrap_or_else(|| {
                    if cmd.release_lease {
                        be as i64
                    } else if cmd.acked_count <= 0 {
                        cur.committed
                    } else {
                        let start = cur.attempt_offset.unwrap_or((cur.committed + 1) as u64);
                        start
                            .saturating_add(cmd.acked_count.max(0) as u64)
                            .saturating_sub(1)
                            .min(be) as i64
                    }
                });
                if upto > be as i64 {
                    return Err(Refusal::client(
                        "beyond_batch",
                        "position beyond leased batch",
                    ));
                }
                let retired = if cur.lease_conflated {
                    (upto - cur.committed).max(0) as u64
                } else {
                    cmd.acked_count.max(0) as u64
                };
                let conflated = if cur.lease_conflated {
                    (upto - cur.committed - 1).max(0) as u32
                } else {
                    0
                };
                cur.committed = upto;
                if cmd.release_lease {
                    release_lease(&mut cur);
                    cur.attempt_offset = None;
                    cur.attempt_count = 0;
                    cur.batch_retry_count = 0;
                } else {
                    cur.attempt_offset = Some((upto + 1).max(0) as u64);
                }
                cur.total_consumed += retired;
                (cmd.acked_count.max(0) as u32, conflated)
            }
            false => {
                // nack: release, cursor untouched.
                release_lease(&mut cur);
                (0, 0)
            }
        };
        let res = AckResult {
            pid,
            committed: cur.committed,
            acked,
            conflated,
            dlq: 0,
            lease_released: cur0.batch_end.is_some() && (!cmd.ok || cmd.release_lease),
            batch_retry_count: cur.batch_retry_count,
            noop_hashes: Vec::new(),
            stale_hashes: Vec::new(),
        };
        let outcome = Outcome::Ack(AckOutcome { results: vec![res] });
        if cur == cur0 {
            let _ = old_committed;
            return Ok(Plan::Empty(outcome));
        }
        let effects = vec![Effect::CursorSet {
            pid,
            group: group.clone(),
            row: cur,
        }];
        ov.apply_effects(&effects);
        Ok(Plan::logged(effects, outcome))
    }

    /// Release a worker's lease on a `(pid, group)` without moving the cursor
    /// (the nack the whole batch redelivers from).
    pub fn plan_nack(&self, ov: &mut Overlay, cmd: &NackCommand) -> Planned {
        let pid = cmd.pid;
        let group = &cmd.group;
        let now = self.now_us;
        let Some(cur0) = self.cursor(ov, pid, group)? else {
            return Ok(Plan::Empty(Outcome::Ack(AckOutcome::default())));
        };
        if !cmd.worker.is_empty()
            && (cur0.worker.as_deref() != Some(cmd.worker.as_str())
                || cur0.lease_expires_at_us.is_none_or(|e| e < now))
        {
            return Err(Refusal::client("bad_lease", "invalid or expired lease"));
        }
        let mut cur = cur0.clone();
        release_lease(&mut cur);
        let res = AckResult {
            pid,
            committed: cur.committed,
            acked: 0,
            conflated: 0,
            dlq: 0,
            lease_released: cur0.batch_end.is_some(),
            batch_retry_count: cur.batch_retry_count,
            noop_hashes: Vec::new(),
            stale_hashes: Vec::new(),
        };
        let outcome = Outcome::Ack(AckOutcome { results: vec![res] });
        if cur == cur0 {
            return Ok(Plan::Empty(outcome));
        }
        let effects = vec![Effect::CursorSet {
            pid,
            group: group.clone(),
            row: cur,
        }];
        ov.apply_effects(&effects);
        Ok(Plan::logged(effects, outcome))
    }

    /// Renew every LIVE lease of a worker (`log_renew_lease_v1`), GREATEST so a
    /// renew never shortens; the answer carries the MIN expiry. Walks
    /// `leases_by_worker` in key order (never a hash map, §8), merged with any
    /// lease this worker took earlier in the same cycle (the overlay).
    pub fn plan_renew(&self, ov: &mut Overlay, cmd: &RenewCommand) -> Planned {
        let now = self.now_us;
        let want = now + cmd.seconds.max(1) as i64 * SEC_US;

        // (pid, group) targets, in key order, from committed leases_by_worker.
        let mut targets: Vec<(Pid, String)> = Vec::new();
        let mut seen: BTreeSet<(Pid, String)> = BTreeSet::new();
        self.committed
            .reads()
            .scan_worker_leases(&cmd.worker, usize::MAX, &mut |pid, group, exp| {
                if exp > now && seen.insert((pid, group.to_string())) {
                    targets.push((pid, group.to_string()));
                }
                true
            })
            .map_err(store_err)?;
        // Overlay leases this worker holds (a pop earlier this cycle).
        for ((pid, group), row) in ov.cursors_iter() {
            if let Some(c) = row {
                if c.worker.as_deref() == Some(cmd.worker.as_str())
                    && c.lease_expires_at_us.is_some_and(|e| e > now)
                    && seen.insert((*pid, group.clone()))
                {
                    targets.push((*pid, group.clone()));
                }
            }
        }
        targets.sort();

        let mut effects: Vec<Effect> = Vec::new();
        let mut renewed = 0u32;
        let mut earliest: Option<i64> = None;
        for (pid, group) in targets {
            let Some(cur0) = self.cursor(ov, pid, &group)? else {
                continue;
            };
            if cur0.worker.as_deref() != Some(cmd.worker.as_str())
                || cur0.lease_expires_at_us.is_none_or(|e| e <= now)
            {
                continue;
            }
            let current = cur0.lease_expires_at_us.unwrap();
            let next = current.max(want);
            let mut cur = cur0.clone();
            cur.lease_expires_at_us = Some(next);
            effects.push(Effect::CursorSet {
                pid,
                group: group.clone(),
                row: cur,
            });
            renewed += 1;
            earliest = Some(earliest.map_or(next, |e: i64| e.min(next)));
        }
        let outcome = Outcome::Renew(RenewOutcome {
            renewed,
            min_expires_at_us: earliest,
        });
        if effects.is_empty() {
            return Ok(Plan::Empty(outcome));
        }
        ov.apply_effects(&effects);
        Ok(Plan::logged(effects, outcome))
    }

    /// File the poison HEAD frame and advance past it (`log_dlq_head_v1`), from
    /// the receiver's snapshot (O20). Calling it twice is safe: the first
    /// released the lease, so the second fails the worker match and writes
    /// nothing.
    pub fn plan_dlq_head(&self, ov: &mut Overlay, cmd: &DlqHeadCommand) -> Planned {
        let pid = cmd.pid;
        let group = &cmd.group;
        let now = self.now_us;
        let Some(cur0) = self.cursor(ov, pid, group)? else {
            return Ok(Plan::Empty(Outcome::Empty));
        };
        if !cmd.worker.is_empty()
            && (cur0.worker.as_deref() != Some(cmd.worker.as_str())
                || cur0.lease_expires_at_us.is_none_or(|e| e < now))
        {
            return Ok(Plan::Empty(Outcome::Empty));
        }
        let dlq_id = crate::util::uuidv7_bytes();
        let mut cur = cur0.clone();
        let mut effects: Vec<Effect> = Vec::with_capacity(2);
        effects.push(Effect::DlqInsert {
            dlq_id,
            tenant: cmd.tenant.clone(),
            queue: cmd.queue.clone(),
            pid,
            group: group.clone(),
            offset: cmd.offset as i64,
            message_id: cmd.snapshot.message_id,
            txn: cmd.snapshot.txn.clone(),
            payload: cmd.snapshot.payload.clone(),
            error: cmd.error.clone(),
            retry_count: cur.batch_retry_count,
            failed_at_us: now,
        });
        // GREATEST guards a stale caller from moving the cursor backward.
        cur.committed = cur.committed.max(cmd.offset as i64);
        release_lease(&mut cur);
        cur.attempt_offset = None;
        cur.attempt_count = 0;
        cur.batch_retry_count = 0;
        cur.total_consumed += 1;
        let released = cur0.batch_end.is_some();
        effects.push(Effect::CursorSet {
            pid,
            group: group.clone(),
            row: cur.clone(),
        });
        ov.apply_effects(&effects);
        Ok(Plan::logged(
            effects,
            Outcome::DlqHead(DlqHeadOutcome {
                pid,
                dlq_id,
                offset: cmd.offset as i64,
                committed: cur.committed,
                lease_released: released,
            }),
        ))
    }
}

/// Clear every lease field of a cursor row, in one place so the branches cannot
/// forget one (the ack registry's `delivered` goes too).
fn release_lease(cur: &mut CursorRow) {
    cur.worker = None;
    cur.lease_expires_at_us = None;
    cur.lease_acquired_at_us = None;
    cur.batch_end = None;
    cur.lease_conflated = false;
    cur.delivered = Vec::new();
}

impl Overlay {
    /// The cursor rows this cycle's overlay set or deleted, for the renew walk.
    fn cursors_iter(&self) -> impl Iterator<Item = (&(Pid, String), &Option<CursorRow>)> {
        self.cursors.iter().map(|(k, t)| (k, &t.v))
    }
}
