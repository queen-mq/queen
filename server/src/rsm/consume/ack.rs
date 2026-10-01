//! Acks — the port of the planner's `005_log_ack` (`planner/ack.rs` at
//! 08050fed): the hash ack inside the leased batch with the O16 full-batch fast
//! path, implicit ack, the explicit signal that is never skipped, below-cursor
//! honesty (noop vs stale), the single retry budget charged only by an
//! explicit `failed`, the DLQ (the receiver's snapshot, filed beside the
//! cursor row that moves past it), the settling of a batch nacked on a spent
//! budget; the positional ack (Streams: `upto`, `release_lease`,
//! `acked_count`, `ok=false`), the nack, the renew (every live lease of a
//! worker, tenant-scoped, the MIN expiry), the DLQ head; and a seek (an admin
//! cursor write) taken over as the one writer.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tokio::sync::oneshot;

use crate::rsm::batcher::Reply;
use crate::rsm::effect::{Effect, Pid};
use crate::rsm::entry::{AckOutcome, AckResult, DlqHeadOutcome, Outcome, RenewOutcome};
use crate::rsm::planner::{
    AckCommand, AckItem, AckPositionalCommand, AckStatus, AckTarget, DlqHeadCommand, DlqSnapshot,
    EffectsCommand, NackCommand, Refusal, RenewCommand,
};
use crate::rsm::store::{Reads, Store, StoreError, TypedReads};

use super::checkpoint::Deps;
use super::frames::Frames;
use super::load::Seed;
use super::state::{lock, shard_of, Cur, Gid, Group, Part};
use super::{Engine, SEC_US};

type Result<T> = std::result::Result<T, StoreError>;

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

/// Whether an `Effects` command is a seek: whole cursor rows an admin wrote.
pub(crate) fn is_seek(c: &EffectsCommand) -> bool {
    !c.effects.is_empty()
        && c.effects
            .iter()
            .all(|e| matches!(e, Effect::CursorSet { .. }))
}

/// The ack of one target, computed on a cursor (the live one, or a
/// transaction's shadow): the result, the dead letters it files, and whether
/// it changed the cursor.
pub(crate) struct Acked {
    pub res: AckResult,
    pub dlq: Vec<Effect>,
    pub changed: bool,
    /// Redelivery after a `failed` with budget left.
    pub released: bool,
}

fn file_dlq(
    target: &AckTarget,
    retry_count: u32,
    offset: i64,
    error: &str,
    snap: DlqSnapshot,
    now_us: i64,
) -> Effect {
    Effect::DlqInsert {
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
    }
}

/// Resolve one hash: from the leased run when it covers the span, from the
/// dedup authority otherwise (and for the below-cursor leg of a miss).
#[allow(clippy::too_many_arguments)]
fn resolve<R: Reads + ?Sized>(
    fr: &Frames<'_, R>,
    cur: &Cur,
    pid: Pid,
    hash: &[u8; 16],
    lo: u64,
    hi: u64,
    committed: i64,
    txns_start: u64,
) -> Result<crate::rsm::dedup::AckRes> {
    if let Some(l) = cur.lease.as_ref() {
        if hi != u64::MAX && l.covers(lo, hi) {
            if let Some(off) = l.find(hash, lo, hi) {
                return Ok(crate::rsm::dedup::AckRes {
                    eff: Some(off),
                    below: false,
                });
            }
            // Not in the span: only the below-cursor leg is left to learn.
            let mut r = fr.resolve(pid, hash, lo, hi, committed, txns_start)?;
            r.eff = None;
            return Ok(r);
        }
    }
    fr.resolve(pid, hash, lo, hi, committed, txns_start)
}

/// Hash-resolved ack of one `(pid, group)` target (005), on `cur` in place.
/// `has_row`: the group has a cursor on the partition (none: rejected).
/// `keep_released`: the cluster reads a released lease in a cursor row
/// (catalogue version 3, [`Engine::cluster_allows`]); below it an ack that
/// settles a batch keeps none, and its repeat is stale, as before beta.1.
#[allow(clippy::too_many_arguments)]
pub(crate) fn ack_target<R: Reads + ?Sized>(
    fr: &Frames<'_, R>,
    cur: &mut Cur,
    has_row: bool,
    txns_start: u64,
    target: &AckTarget,
    cfg: &crate::rsm::effect::QueueConfig,
    now: i64,
    repeat_ok: bool,
    keep_released: bool,
) -> Result<Acked> {
    let pid = target.pid;
    let worker = &target.worker;
    let reject = |committed: i64, items: &[AckItem]| Acked {
        res: AckResult {
            pid,
            committed,
            acked: 0,
            conflated: 0,
            dlq: 0,
            lease_released: false,
            batch_retry_count: 0,
            noop_hashes: Vec::new(),
            // The outcome shape carries no per-item error, so a target that
            // cannot be acked reports its hashes as stale.
            stale_hashes: items.iter().map(|i| i.hash).collect(),
        },
        dlq: Vec::new(),
        changed: false,
        released: false,
    };
    if !has_row {
        return Ok(reject(-1, &target.items));
    }
    let cur0 = cur.clone();
    // The lease is validated ONLY when a non-empty leaseId was supplied; a
    // lease-less ack falls through and still advances.
    if !worker.is_empty()
        && (cur0.worker() != Some(worker.as_str()) || cur0.expires().is_none_or(|e| e < now))
    {
        // The same ack again, after it released this lease: its first reply
        // was lost, or the leader changed and the client (an SDK retrying a
        // 503 `outcome_unknown`) sends it once more. Answered as it was the
        // first time, on any leader: the released lease rides in the row.
        // Only for a plain ack: a transaction acking a message its lease
        // already settled is refused (its other legs would commit twice).
        let repeat = if repeat_ok {
            repeat_of_released(fr, &cur0, pid, worker, target, txns_start)?
        } else {
            None
        };
        if let Some(res) = repeat {
            return Ok(Acked {
                res,
                dlq: Vec::new(),
                changed: false,
                released: false,
            });
        }
        return Ok(reject(cur0.committed, &target.items));
    }

    let committed = cur0.committed;
    let has_lease = cur0.lease.is_some();
    let batch_end = cur0.lease.as_ref().map_or(u64::MAX, |l| l.batch_end);
    let lo = ((committed + 1).max(txns_start as i64)).max(0) as u64;
    let hi = if has_lease { batch_end } else { u64::MAX };

    // ---- O16 full-batch fast path: the OK hashes exactly cover the delivered
    // set of a live lease, and there is no explicit signal.
    let has_signal = target.items.iter().any(|i| is_signal(i.status));
    if has_lease && !has_signal && cur0.worker() == Some(worker.as_str()) {
        let delivered = &cur0.lease.as_ref().expect("lease").delivered;
        if !delivered.is_empty() {
            let acked_ok: BTreeSet<[u8; 16]> = target
                .items
                .iter()
                .filter(|i| matches!(i.status, AckStatus::Ok))
                .map(|i| i.hash)
                .collect();
            let set: BTreeSet<[u8; 16]> = delivered.iter().copied().collect();
            if acked_ok == set {
                let new = batch_end as i64;
                let delta = (new - committed).max(0);
                let conflated = if cur0.lease.as_ref().is_some_and(|l| l.conflated) {
                    (delta - 1).max(0)
                } else {
                    0
                };
                cur.release();
                cur.released = cur0
                    .lease
                    .as_ref()
                    .filter(|_| keep_released)
                    .map(|l| (l.worker.clone(), (committed + 1).max(0) as u64, batch_end));
                cur.committed = new;
                cur.attempt_offset = None;
                cur.attempt_count = 0;
                cur.batch_retry_count = 0;
                cur.total_consumed += delta as u64;
                return Ok(Acked {
                    res: AckResult {
                        pid,
                        committed: new,
                        acked: delta as u32,
                        conflated: conflated as u32,
                        dlq: 0,
                        lease_released: true,
                        batch_retry_count: 0,
                        noop_hashes: Vec::new(),
                        stale_hashes: Vec::new(),
                    },
                    dlq: Vec::new(),
                    changed: true,
                    released: false,
                });
            }
        }
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
        let r = resolve(fr, &cur0, pid, &it.hash, lo, hi, committed, txns_start)?;
        resolved.push(Resolved {
            eff: r.eff.map(|o| o as i64),
            below: r.below,
            ok: matches!(it.status, AckStatus::Ok),
            signal: is_signal(it.status).then_some(it.status),
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

    let mut new = max_ok.unwrap_or(committed);
    let mut delta = (new - committed).max(0);
    let mut conflated: i64 = 0;
    let mut dlq_filed = 0u32;
    let mut dlq: Vec<Effect> = Vec::new();
    let mut released_for_retry = false;

    let retry_limit = cfg.retry_limit.max(0) as u32;
    let dlq_enabled = cfg.dead_letter_queue || cfg.dlq_after_max_retries;

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

    // Above the head: the strongest item this call names at each position
    // (dlq > failed > retry > ok).
    let mut above: BTreeMap<i64, (AckStatus, usize)> = BTreeMap::new();
    if let Some(head) = sig_off {
        for (i, r) in resolved.iter().enumerate() {
            let Some(off) = r.eff.filter(|o| *o > head) else {
                continue;
            };
            let status = match r.signal {
                Some(s) => s,
                None if r.ok => AckStatus::Ok,
                None => continue,
            };
            let rank = |s: AckStatus| {
                if s == AckStatus::Ok {
                    3
                } else {
                    signal_rank(s)
                }
            };
            let slot = above.entry(off).or_insert((status, i));
            if rank(status) < rank(slot.0) {
                *slot = (status, i);
            }
        }
    }
    let max_ok_all = resolved.iter().filter(|r| r.ok).filter_map(|r| r.eff).max();
    enum Step {
        File(usize),
        Drop,
        Complete,
    }
    // The positions settled above the head, contiguous from head + 1.
    let settle_above = |exhausted: bool| -> (Vec<(i64, Step)>, bool) {
        let mut steps = Vec::new();
        let Some(head) = sig_off else {
            return (steps, false);
        };
        let mut p = head + 1;
        while has_lease && (p as u64) <= batch_end {
            match above.get(&p) {
                Some((AckStatus::Dlq, i)) => steps.push((p, Step::File(*i))),
                Some((AckStatus::Failed, i)) if exhausted => steps.push((
                    p,
                    if dlq_enabled {
                        Step::File(*i)
                    } else {
                        Step::Drop
                    },
                )),
                Some((AckStatus::Failed, _)) => return (steps, true),
                Some((AckStatus::Ok, _)) => steps.push((p, Step::Complete)),
                Some(_) => break,
                None if max_ok_all.is_some_and(|m| m > p) => steps.push((p, Step::Complete)),
                None => break,
            }
            p += 1;
        }
        (steps, false)
    };
    let settle =
        |dlq: &mut Vec<Effect>, cur: &mut Cur, filed: &mut u32, exhausted: bool, rc: u32| {
            let (steps, stopped_at_failed) = settle_above(exhausted);
            for (off, step) in &steps {
                if let Step::File(i) = step {
                    let item = &target.items[*i];
                    let error = item.error.clone().unwrap_or_else(&dlq_error);
                    dlq.push(file_dlq(
                        target,
                        rc,
                        *off,
                        &error,
                        item.snapshot.clone().unwrap_or_default(),
                        now,
                    ));
                    *filed += 1;
                }
            }
            if let Some((last, _)) = steps.last() {
                cur.total_consumed += (*last - cur.committed).max(0) as u64;
                cur.committed = *last;
            }
            stopped_at_failed
        };

    if sig_kind == Some(AckStatus::Dlq) && has_lease {
        // Forced DLQ: advance the completed prefix and file the poison beside
        // the row (O7); the lease is released here.
        cur.committed = new;
        cur.total_consumed += delta as u64;
        dlq.push(file_dlq(
            target,
            cur.batch_retry_count,
            sig_off.unwrap(),
            &dlq_error(),
            dlq_snapshot(),
            now,
        ));
        dlq_filed = 1;
        cur.committed = cur.committed.max(sig_off.unwrap());
        cur.release();
        cur.attempt_offset = None;
        cur.attempt_count = 0;
        cur.batch_retry_count = 0;
        cur.total_consumed += 1;
        let exhausted = cur0.batch_retry_count >= retry_limit;
        if settle(
            &mut dlq,
            cur,
            &mut dlq_filed,
            exhausted,
            cur0.batch_retry_count,
        ) {
            // Stopped at a `failed` whose budget remains: the rest of the batch
            // keeps the budget it has used.
            cur.batch_retry_count = cur0.batch_retry_count;
        }
        new = cur.committed;
        delta = (new - committed).max(0);
    } else if sig_kind == Some(AckStatus::Failed) && has_lease {
        let retry_ct = cur.batch_retry_count;
        if retry_ct < retry_limit {
            // Budget remains: release so the poison redelivers; charge once.
            cur.committed = new;
            cur.release();
            cur.batch_retry_count = retry_ct + 1;
            cur.total_consumed += delta as u64;
            released_for_retry = true;
        } else if dlq_enabled {
            // Budget exhausted + DLQ: file the poison, release, reset.
            cur.committed = new;
            cur.total_consumed += delta as u64;
            dlq.push(file_dlq(
                target,
                cur.batch_retry_count,
                sig_off.unwrap(),
                &dlq_error(),
                dlq_snapshot(),
                now,
            ));
            dlq_filed = 1;
            cur.committed = cur.committed.max(sig_off.unwrap());
            cur.release();
            cur.attempt_offset = None;
            cur.attempt_count = 0;
            cur.batch_retry_count = 0;
            cur.total_consumed += 1;
            settle(&mut dlq, cur, &mut dlq_filed, true, retry_ct);
            new = cur.committed;
            delta = (new - committed).max(0);
        } else {
            // Budget exhausted, no DLQ: drop the poison, advance past it.
            new = sig_off.unwrap();
            cur.committed = new;
            cur.release();
            cur.batch_retry_count = 0;
            cur.total_consumed += delta as u64 + 1;
            settle(&mut dlq, cur, &mut dlq_filed, true, retry_ct);
            new = cur.committed;
            delta = (new - committed).max(0);
        }
    } else if sig_kind == Some(AckStatus::Retry) && has_lease {
        // Budget-free retry: release, cursor at the completed prefix.
        cur.committed = new;
        cur.release();
        cur.total_consumed += delta as u64;
    } else {
        // All completed, or a lease-less ack.
        let lease_conflated = cur0.lease.as_ref().is_some_and(|l| l.conflated);
        if lease_conflated && has_lease && max_ok.is_some() {
            // A conflating lease delivered ONE frame at batch_end; a clean ack
            // completes the whole leased span.
            new = batch_end as i64;
            delta = (new - committed).max(0);
            conflated = (delta - 1).max(0);
        }
        let reached_end = has_lease && new >= batch_end as i64;
        if reached_end {
            cur.committed = new;
            cur.release();
            cur.attempt_offset = None;
            cur.attempt_count = 0;
            cur.batch_retry_count = 0;
            cur.total_consumed += delta as u64;
        } else {
            cur.committed = new;
            if has_lease {
                // Keep the attempt marker on the first uncommitted frame of a
                // live partial lease.
                cur.attempt_offset = Some((new + 1) as u64);
            }
            cur.total_consumed += delta as u64;
        }
    }

    // Per-item classification into the noop/stale vocabulary.
    let mut noop_hashes: Vec<[u8; 16]> = Vec::new();
    let mut stale_hashes: Vec<[u8; 16]> = Vec::new();
    for (r, it) in resolved.iter().zip(&target.items) {
        if r.eff.is_none() && !r.below {
            stale_hashes.push(it.hash);
        } else if r.below && r.eff.is_none() {
            if r.signal.is_some() {
                stale_hashes.push(it.hash);
            } else {
                noop_hashes.push(it.hash);
            }
        }
    }
    // This ack settled the lease's whole batch: remember the lease, so a
    // repeat of this ack is answered as this one is ([`repeat_of_released`]).
    // Not a release for redelivery (a `failed` with budget left): those
    // messages come back, and a repeat is stale.
    if keep_released
        && has_lease
        && !released_for_retry
        && cur.lease.is_none()
        && cur.committed >= batch_end as i64
        && !worker.is_empty()
    {
        if let Some(l) = cur0.lease.as_ref() {
            cur.released = Some((l.worker.clone(), (committed + 1).max(0) as u64, batch_end));
        }
    }
    let lease_released = cur.worker().is_none() && cur0.worker().is_some();
    let changed = !same(cur, &cur0);
    Ok(Acked {
        res: AckResult {
            pid,
            committed: cur.committed,
            acked: delta.max(0) as u32,
            conflated: conflated.max(0) as u32,
            dlq: dlq_filed,
            lease_released: lease_released || (cur.lease.is_none() && cur0.lease.is_some()),
            batch_retry_count: cur.batch_retry_count,
            noop_hashes,
            stale_hashes,
        },
        dlq,
        changed,
        released: released_for_retry,
    })
}

/// An ack under the lease the last ack released (`cur.released`): every
/// item that is one of that batch's messages is answered success, as the
/// first ack answered it (every one of them is at or below the cursor now);
/// any other item stale. `None` when the lease is not that one, or no item
/// is of its batch: the ordinary refusal.
fn repeat_of_released<R: Reads + ?Sized>(
    fr: &Frames<'_, R>,
    cur: &Cur,
    pid: Pid,
    worker: &str,
    target: &AckTarget,
    txns_start: u64,
) -> Result<Option<AckResult>> {
    let Some((released, lo, hi)) = cur.released.as_ref() else {
        return Ok(None);
    };
    if &**released != worker {
        return Ok(None);
    }
    let mut stale_hashes = Vec::new();
    for it in &target.items {
        let r = fr.resolve(pid, &it.hash, *lo, *hi, cur.committed, txns_start)?;
        if r.eff.is_none_or(|off| (off as i64) > cur.committed) {
            stale_hashes.push(it.hash);
        }
    }
    if stale_hashes.len() == target.items.len() {
        return Ok(None);
    }
    Ok(Some(AckResult {
        pid,
        committed: cur.committed,
        acked: 0,
        conflated: 0,
        dlq: 0,
        lease_released: true,
        batch_retry_count: cur.batch_retry_count,
        noop_hashes: Vec::new(),
        stale_hashes,
    }))
}

/// Whether two cursors are the same row.
pub(crate) fn same(a: &Cur, b: &Cur) -> bool {
    a.row() == b.row()
}

/// Positional ack of one leased batch (`log_ack_v1` / `log_ack_at_v1`), on
/// `cur` in place: the result, or the refusal.
pub(crate) fn positional(
    cur: &mut Cur,
    has_row: bool,
    cmd: &AckPositionalCommand,
    now: i64,
) -> std::result::Result<Option<AckResult>, Refusal> {
    if !has_row {
        return Ok(None);
    }
    let cur0 = cur.clone();
    if !cmd.worker.is_empty()
        && (cur0.worker() != Some(cmd.worker.as_str()) || cur0.expires().is_none_or(|e| e < now))
    {
        return Err(Refusal::client("bad_lease", "invalid or expired lease"));
    }
    let (acked, conflated): (u32, u32) = if cmd.ok {
        let Some(be) = cur.lease.as_ref().map(|l| l.batch_end) else {
            return Err(Refusal::client("no_batch", "no leased batch"));
        };
        let lease_conflated = cur.lease.as_ref().is_some_and(|l| l.conflated);
        // A full Streams cycle names no absolute offset: the recorded batch end
        // is the authority. A partial gate cycle advances exactly `acked_count`
        // delivered frames from the attempt head and keeps the lease.
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
        let retired = if lease_conflated {
            (upto - cur.committed).max(0) as u64
        } else {
            cmd.acked_count.max(0) as u64
        };
        let conflated = if lease_conflated {
            (upto - cur.committed - 1).max(0) as u32
        } else {
            0
        };
        cur.committed = upto;
        if cmd.release_lease {
            cur.release();
            cur.attempt_offset = None;
            cur.attempt_count = 0;
            cur.batch_retry_count = 0;
        } else {
            cur.attempt_offset = Some((upto + 1).max(0) as u64);
        }
        cur.total_consumed += retired;
        (cmd.acked_count.max(0) as u32, conflated)
    } else {
        // nack: release, cursor untouched.
        cur.release();
        (0, 0)
    };
    Ok(Some(AckResult {
        pid: cmd.pid,
        committed: cur.committed,
        acked,
        conflated,
        dlq: 0,
        lease_released: cur0.lease.is_some() && (!cmd.ok || cmd.release_lease),
        batch_retry_count: cur.batch_retry_count,
        noop_hashes: Vec::new(),
        stale_hashes: Vec::new(),
    }))
}

impl Engine {
    /// The group and part for a target, loaded if needed. `None` when the
    /// queue or the partition is not there.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn target_part<R: Reads + ?Sized>(
        &self,
        r: &R,
        fr: &Frames<'_, R>,
        tenant: &str,
        queue: &str,
        group: &str,
        pid: Pid,
        now: i64,
    ) -> Result<Option<Arc<Group>>> {
        let Some(g) = self.group_for(r, tenant, queue, group)? else {
            return Ok(None);
        };
        if !self.ensure_part(r, fr, &g, pid, Seed::Policy, now)? {
            return Ok(None);
        }
        Ok(Some(g))
    }

    /// Apply a changed cursor to its live part: bump, dirty, lease index,
    /// timers, re-arm. Returns the version the answer waits on.
    pub(crate) fn commit_cur(
        &self,
        sh: &mut super::state::Shard,
        gid: Gid,
        pid: Pid,
        new: Cur,
        dlq: Vec<Effect>,
        now: i64,
    ) -> Option<u64> {
        let grace = self.grace();
        let (old_worker, new_worker, ver) = {
            let p: &mut Part = sh.groups.get_mut(&gid)?.parts.get_mut(&pid)?;
            let old_worker = p.cur.worker().map(Arc::<str>::from);
            p.cur = new;
            p.dlq.extend(dlq);
            let new_worker = p.cur.lease.as_ref().map(|l| l.worker.clone());
            let ver = super::pop::touch(p, &mut sh.dirty, pid, gid);
            (old_worker, new_worker, ver)
        };
        if let Some(w) = old_worker {
            sh.unindex_lease(&w, pid, gid);
        }
        if let Some(w) = new_worker {
            if !w.is_empty() {
                sh.index_lease(&w, pid, gid);
            }
        }
        let super::state::Shard { groups, timers, .. } = sh;
        if let Some(p) = groups.get_mut(&gid).and_then(|gs| gs.parts.get_mut(&pid)) {
            if let Some(t) = p.cur.lease_until(grace) {
                super::state::schedule(timers, p, t, gid, pid);
            }
        }
        let _ = now;
        Some(ver)
    }

    /// `POST /ack`, `/ack/batch`: every target, in input order.
    pub(crate) fn ack_cmd(
        &self,
        c: &AckCommand,
        now: i64,
        sink: &mut Option<oneshot::Sender<Reply>>,
    ) -> std::result::Result<Option<Reply>, Refusal> {
        let seg = self.seg_source();
        let mut woken: Vec<Gid> = Vec::new();
        let res = self.store.read(|r| {
            let fr = self.frames(r, &seg);
            let mut deps = Deps {
                exact: true,
                ..Deps::default()
            };
            let mut results = Vec::with_capacity(c.targets.len());
            // A target a transaction holds refuses the whole ack (retryable)
            // before any target moves.
            let mut groups = Vec::with_capacity(c.targets.len());
            for t in &c.targets {
                let g = self.target_part(r, &fr, &t.tenant, &t.queue, &t.group, t.pid, now)?;
                if let Some(g) = &g {
                    let sh = lock(&self.shards[shard_of(t.pid)]);
                    if sh
                        .groups
                        .get(&g.id)
                        .and_then(|gs| gs.parts.get(&t.pid))
                        .is_some_and(|p| p.reserved.is_some())
                    {
                        return Ok(Err(Refusal::retry(
                            "reserved",
                            "a transaction holds this partition's cursor",
                        )));
                    }
                }
                groups.push(g);
            }
            // Once a target changed, the command must not be refused (nor
            // fail retryably): a re-run would find that target's lease
            // released and call it stale. A target that cannot be acked from
            // then on is answered stale on its own; every target keeps its
            // place in the results (the render reads them by position).
            let stale = |t: &crate::rsm::planner::AckTarget| AckResult {
                pid: t.pid,
                committed: -1,
                acked: 0,
                conflated: 0,
                dlq: 0,
                lease_released: false,
                batch_retry_count: 0,
                noop_hashes: Vec::new(),
                stale_hashes: t.items.iter().map(|i| i.hash).collect(),
            };
            for (t, g) in c.targets.iter().zip(groups) {
                let Some(g) = g else {
                    results.push(stale(t));
                    continue;
                };
                let cfg = g.cfg();
                let mut sh = lock(&self.shards[shard_of(t.pid)]);
                let sh = &mut *sh;
                let txns_start = sh.pids.get(&t.pid).map_or(0, |pi| pi.txns_start);
                let Some(p) = sh
                    .groups
                    .get_mut(&g.id)
                    .and_then(|gs| gs.parts.get_mut(&t.pid))
                else {
                    results.push(stale(t));
                    continue;
                };
                if p.reserved.is_some() {
                    if deps.rows.is_empty() {
                        return Ok(Err(Refusal::retry(
                            "reserved",
                            "a transaction holds this partition's cursor",
                        )));
                    }
                    results.push(stale(t));
                    continue;
                }
                let mut cur = p.cur.clone();
                let has_row = p.has_row;
                let acked = match ack_target(
                    &fr,
                    &mut cur,
                    has_row,
                    txns_start,
                    t,
                    &cfg.queue,
                    now,
                    true,
                    self.cluster_allows(crate::rsm::effect::VERSION_3),
                ) {
                    Ok(a) => a,
                    Err(e) if deps.rows.is_empty() => return Err(e),
                    Err(_) => {
                        results.push(stale(t));
                        continue;
                    }
                };
                if acked.changed || !acked.dlq.is_empty() {
                    if let Some(v) = self.commit_cur(sh, g.id, t.pid, cur, acked.dlq, now) {
                        deps.rows.push((t.pid, g.id, v));
                    }
                    if super::pop::arm(sh, g.id, t.pid, now, self.grace()) {
                        woken.push(g.id);
                    }
                }
                results.push(acked.res);
            }
            Ok(Ok((results, deps)))
        });
        for gid in woken {
            self.wake_group(gid);
        }
        let (results, deps) = res.map_err(Refusal::from_store)??;
        let reply = Reply::Done {
            outcome: Outcome::Ack(AckOutcome { results }),
            at: None,
        };
        Ok(self.answer(reply, deps, sink))
    }

    /// A positional ack (or a positional nack, `ok=false`).
    pub(crate) fn positional_cmd(
        &self,
        c: &AckPositionalCommand,
        now: i64,
        sink: &mut Option<oneshot::Sender<Reply>>,
    ) -> std::result::Result<Option<Reply>, Refusal> {
        let seg = self.seg_source();
        let mut woken = None;
        let res = self.store.read(|r| {
            let fr = self.frames(r, &seg);
            let Some(g) = self.target_part(r, &fr, &c.tenant, &c.queue, &c.group, c.pid, now)?
            else {
                return Ok(Ok((None, Deps::default())));
            };
            let mut sh = lock(&self.shards[shard_of(c.pid)]);
            let sh = &mut *sh;
            let Some(p) = sh
                .groups
                .get_mut(&g.id)
                .and_then(|gs| gs.parts.get_mut(&c.pid))
            else {
                return Ok(Ok((None, Deps::default())));
            };
            if p.reserved.is_some() {
                return Ok(Err(Refusal::retry(
                    "reserved",
                    "a transaction holds this partition's cursor",
                )));
            }
            let mut cur = p.cur.clone();
            let res = match positional(&mut cur, p.has_row, c, now) {
                Ok(r) => r,
                Err(refusal) => return Ok(Err(refusal)),
            };
            let mut deps = Deps {
                exact: true,
                ..Deps::default()
            };
            if res.is_some() && !same(&cur, &p.cur) {
                if let Some(v) = self.commit_cur(sh, g.id, c.pid, cur, Vec::new(), now) {
                    deps.rows.push((c.pid, g.id, v));
                }
                if super::pop::arm(sh, g.id, c.pid, now, self.grace()) {
                    woken = Some(g.id);
                }
            }
            Ok(Ok((res, deps)))
        });
        if let Some(gid) = woken {
            self.wake_group(gid);
        }
        let (res, deps) = res.map_err(Refusal::from_store)??;
        let reply = Reply::Done {
            outcome: Outcome::Ack(AckOutcome {
                results: res.into_iter().collect(),
            }),
            at: None,
        };
        Ok(self.answer(reply, deps, sink))
    }

    /// Release a worker's lease on a `(pid, group)` without moving the cursor.
    pub(crate) fn nack_cmd(
        &self,
        c: &NackCommand,
        now: i64,
        sink: &mut Option<oneshot::Sender<Reply>>,
    ) -> std::result::Result<Option<Reply>, Refusal> {
        let seg = self.seg_source();
        let mut woken = None;
        let res = self.store.read(|r| {
            let fr = self.frames(r, &seg);
            let Some(g) = self.target_part(r, &fr, &c.tenant, &c.queue, &c.group, c.pid, now)?
            else {
                return Ok(Ok((None, Deps::default())));
            };
            let mut sh = lock(&self.shards[shard_of(c.pid)]);
            let sh = &mut *sh;
            let Some(p) = sh
                .groups
                .get_mut(&g.id)
                .and_then(|gs| gs.parts.get_mut(&c.pid))
            else {
                return Ok(Ok((None, Deps::default())));
            };
            if !p.has_row {
                return Ok(Ok((None, Deps::default())));
            }
            if p.reserved.is_some() {
                return Ok(Err(Refusal::retry(
                    "reserved",
                    "a transaction holds this partition's cursor",
                )));
            }
            if !c.worker.is_empty()
                && (p.cur.worker() != Some(c.worker.as_str())
                    || p.cur.expires().is_none_or(|e| e < now))
            {
                return Ok(Err(Refusal::client(
                    "bad_lease",
                    "invalid or expired lease",
                )));
            }
            let had = p.cur.lease.is_some();
            let mut cur = p.cur.clone();
            cur.release();
            let res = AckResult {
                pid: c.pid,
                committed: cur.committed,
                acked: 0,
                conflated: 0,
                dlq: 0,
                lease_released: had,
                batch_retry_count: cur.batch_retry_count,
                noop_hashes: Vec::new(),
                stale_hashes: Vec::new(),
            };
            let mut deps = Deps {
                exact: true,
                ..Deps::default()
            };
            if had {
                if let Some(v) = self.commit_cur(sh, g.id, c.pid, cur, Vec::new(), now) {
                    deps.rows.push((c.pid, g.id, v));
                }
                if super::pop::arm(sh, g.id, c.pid, now, self.grace()) {
                    woken = Some(g.id);
                }
            }
            Ok(Ok((Some(res), deps)))
        });
        if let Some(gid) = woken {
            self.wake_group(gid);
        }
        let (res, deps) = res.map_err(Refusal::from_store)??;
        let reply = Reply::Done {
            outcome: Outcome::Ack(AckOutcome {
                results: res.into_iter().collect(),
            }),
            at: None,
        };
        Ok(self.answer(reply, deps, sink))
    }

    /// Renew every LIVE lease of a worker (`log_renew_lease_v1`), GREATEST so a
    /// renew never shortens; the answer carries the MIN expiry.
    pub(crate) fn renew_cmd(
        &self,
        c: &RenewCommand,
        now: i64,
        sink: &mut Option<oneshot::Sender<Reply>>,
    ) -> std::result::Result<Option<Reply>, Refusal> {
        let want = now + c.seconds.max(1) as i64 * SEC_US;
        let seg = self.seg_source();
        // A new leader holds only what it loaded: the worker's leases the
        // committed index names are loaded first.
        let _ = self.store.read(|r| {
            let fr = self.frames(r, &seg);
            let mut found: Vec<(Pid, String)> = Vec::new();
            r.scan_worker_leases(&c.worker, usize::MAX, &mut |pid, group, exp| {
                if exp > now {
                    found.push((pid, group.to_string()));
                }
                true
            })?;
            for (pid, group) in found {
                let Some((t, q)) =
                    r.partition_with(pid, |p| (p.tenant.to_string(), p.queue.to_string()))?
                else {
                    continue;
                };
                if c.tenant.as_deref().is_some_and(|x| x != t) {
                    continue;
                }
                let _ = self.target_part(r, &fr, &t, &q, &group, pid, now)?;
            }
            Ok(())
        });
        let grace = self.grace();
        let mut deps = Deps::default();
        let mut renewed = 0u32;
        let mut earliest: Option<i64> = None;
        for s in self.shards.iter() {
            let mut sh = lock(s);
            let sh = &mut *sh;
            let Some(set) = sh.workers.get(c.worker.as_str()) else {
                continue;
            };
            let mut targets: Vec<(Pid, Gid)> = set.iter().copied().collect();
            targets.sort_unstable();
            for (pid, gid) in targets {
                let Some(gs) = sh.groups.get_mut(&gid) else {
                    continue;
                };
                if c.tenant.as_deref().is_some_and(|t| t != gs.g.tenant) {
                    continue;
                }
                let Some(p) = gs.parts.get_mut(&pid) else {
                    continue;
                };
                if p.reserved.is_some()
                    || p.cur.worker() != Some(c.worker.as_str())
                    || p.cur.expires().is_none_or(|e| e <= now)
                {
                    continue;
                }
                let l = p.cur.lease.as_mut().expect("leased");
                let next = l.expires_us.max(want);
                l.expires_us = next;
                let v = super::pop::touch(p, &mut sh.dirty, pid, gid);
                deps.rows.push((pid, gid, v));
                if let Some(t) = p.cur.lease_until(grace) {
                    super::state::schedule(&mut sh.timers, p, t, gid, pid);
                }
                renewed += 1;
                earliest = Some(earliest.map_or(next, |e: i64| e.min(next)));
            }
        }
        let reply = Reply::Done {
            outcome: Outcome::Renew(RenewOutcome {
                renewed,
                min_expires_at_us: earliest,
            }),
            at: None,
        };
        Ok(self.answer(reply, deps, sink))
    }

    /// File the poison HEAD frame and advance past it (`log_dlq_head_v1`).
    pub(crate) fn dlq_head_cmd(
        &self,
        c: &DlqHeadCommand,
        now: i64,
        sink: &mut Option<oneshot::Sender<Reply>>,
    ) -> std::result::Result<Option<Reply>, Refusal> {
        let seg = self.seg_source();
        let mut woken = None;
        let res = self.store.read(|r| {
            let fr = self.frames(r, &seg);
            let Some(g) = self.target_part(r, &fr, &c.tenant, &c.queue, &c.group, c.pid, now)?
            else {
                return Ok(Ok((None, Deps::default())));
            };
            let mut sh = lock(&self.shards[shard_of(c.pid)]);
            let sh = &mut *sh;
            let Some(p) = sh
                .groups
                .get_mut(&g.id)
                .and_then(|gs| gs.parts.get_mut(&c.pid))
            else {
                return Ok(Ok((None, Deps::default())));
            };
            if !p.has_row {
                return Ok(Ok((None, Deps::default())));
            }
            if p.reserved.is_some() {
                return Ok(Err(Refusal::retry(
                    "reserved",
                    "a transaction holds this partition's cursor",
                )));
            }
            if !c.worker.is_empty()
                && (p.cur.worker() != Some(c.worker.as_str())
                    || p.cur.expires().is_none_or(|e| e < now))
            {
                return Ok(Ok((None, Deps::default())));
            }
            let dlq_id = crate::util::uuidv7_bytes();
            let mut cur = p.cur.clone();
            let effect = Effect::DlqInsert {
                dlq_id,
                tenant: c.tenant.clone(),
                queue: c.queue.clone(),
                pid: c.pid,
                group: c.group.clone(),
                offset: c.offset as i64,
                message_id: c.snapshot.message_id,
                txn: c.snapshot.txn.clone(),
                payload: c.snapshot.payload.clone(),
                error: c.error.clone(),
                retry_count: cur.batch_retry_count,
                failed_at_us: now,
            };
            // GREATEST guards a stale caller from moving the cursor backward.
            cur.committed = cur.committed.max(c.offset as i64);
            let released = cur.lease.is_some();
            cur.release();
            cur.attempt_offset = None;
            cur.attempt_count = 0;
            cur.batch_retry_count = 0;
            cur.total_consumed += 1;
            let committed = cur.committed;
            let mut deps = Deps {
                exact: true,
                ..Deps::default()
            };
            if let Some(v) = self.commit_cur(sh, g.id, c.pid, cur, vec![effect], now) {
                deps.rows.push((c.pid, g.id, v));
            }
            if super::pop::arm(sh, g.id, c.pid, now, self.grace()) {
                woken = Some(g.id);
            }
            Ok(Ok((
                Some(DlqHeadOutcome {
                    pid: c.pid,
                    dlq_id,
                    offset: c.offset as i64,
                    committed,
                    lease_released: released,
                }),
                deps,
            )))
        });
        if let Some(gid) = woken {
            self.wake_group(gid);
        }
        let (out, deps) = res.map_err(Refusal::from_store)??;
        let outcome = match out {
            Some(o) => Outcome::DlqHead(o),
            None => Outcome::Empty,
        };
        Ok(self.answer(Reply::Done { outcome, at: None }, deps, sink))
    }

    /// A seek (whole cursor rows an admin computed): the engine takes the rows
    /// as the one writer, and answers once they committed.
    pub(crate) fn seek_cmd(
        &self,
        c: &EffectsCommand,
        now: i64,
        sink: &mut Option<oneshot::Sender<Reply>>,
    ) -> std::result::Result<Option<Reply>, Refusal> {
        let seg = self.seg_source();
        let term = self
            .term_start_us
            .load(std::sync::atomic::Ordering::Acquire);
        let mut woken: Vec<Gid> = Vec::new();
        let res = self.store.read(|r| {
            let fr = self.frames(r, &seg);
            let mut deps = Deps::default();
            for e in &c.effects {
                let Effect::CursorSet { pid, group, row } = e else {
                    continue;
                };
                if r.is_garbage(*pid)? {
                    return Ok(Err(Refusal::retry(
                        "partition_gone",
                        format!("partition {pid} is gone or being deleted"),
                    )));
                }
                let Some((t, q)) =
                    r.partition_with(*pid, |p| (p.tenant.to_string(), p.queue.to_string()))?
                else {
                    return Ok(Err(Refusal::retry(
                        "partition_gone",
                        format!("partition {pid} is gone or being deleted"),
                    )));
                };
                if t != c.tenant {
                    return Ok(Err(Refusal::client(
                        "bad_request",
                        "partition of another tenant",
                    )));
                }
                let Some(g) = self.target_part(r, &fr, &t, &q, group, *pid, now)? else {
                    continue;
                };
                let mut sh = lock(&self.shards[shard_of(*pid)]);
                let sh = &mut *sh;
                if sh
                    .groups
                    .get(&g.id)
                    .and_then(|gs| gs.parts.get(pid))
                    .is_some_and(|p| p.reserved.is_some())
                {
                    return Ok(Err(Refusal::retry(
                        "reserved",
                        "a transaction holds this partition's cursor",
                    )));
                }
                let mut cur = Cur::from_row(row, term);
                if let Some(l) = cur.lease.as_mut() {
                    l.foreign = false;
                }
                if let Some(p) = sh
                    .groups
                    .get_mut(&g.id)
                    .and_then(|gs| gs.parts.get_mut(pid))
                {
                    p.seed_ts = None;
                    p.ready_at = 0;
                }
                if let Some(v) = self.commit_cur(sh, g.id, *pid, cur, Vec::new(), now) {
                    deps.rows.push((*pid, g.id, v));
                }
                if super::pop::arm(sh, g.id, *pid, now, self.grace()) {
                    woken.push(g.id);
                }
            }
            Ok(Ok(deps))
        });
        for gid in woken {
            self.wake_group(gid);
        }
        let deps = res.map_err(Refusal::from_store)??;
        Ok(self.answer(
            Reply::Done {
                outcome: Outcome::Empty,
                at: None,
            },
            deps,
            sink,
        ))
    }
}
