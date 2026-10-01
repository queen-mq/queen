//! The consumption half of a transaction (`/transaction`: `acks`,
//! `positional_acks`, `positions`; `requiredLeases` reach here as the acks'
//! workers).
//!
//! [`Engine::prepare`] validates every leg against the live state on SHADOW
//! cursors — an ack must hold its lease and resolve every hash (a stale one
//! rolls the whole bundle back, `QTXN`), a positional ack its batch, a
//! position its queue — and RESERVES the parts it touches: no pop, ack,
//! expiry or checkpoint touches them until [`Engine::resolve_txn`]. The
//! effects (whole cursor rows after the acks, their dead letters, a first
//! position's group registration, a forgotten position's `CursorDelete`) ride
//! in the transaction's own entry. Committed: the shadows become the live
//! state, durable with that entry. Refused or lost: the reservation goes and
//! nothing changed. A prepare repeated under the same request id returns the
//! same part; a reservation nobody resolves goes after
//! `QUEEN_CONSUME_TXN_TTL_MS`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::rsm::effect::{Effect, Pid};
use crate::rsm::entry::{AckOutcome, RequestId};
use crate::rsm::planner::txn::TxnCommand;
use crate::rsm::planner::Refusal;
use crate::rsm::store::{Store, TypedReads};

use super::ack::{ack_target, positional, same};
use super::load::{registration_meta, Seed};
use super::state::{lock, shard_of, Cur, Gid, Group, GroupCfg};
use super::{Engine, TxnPart};

/// One reservation.
pub(crate) struct Reservation {
    pub part: TxnPart,
    /// The shadows to make live on commit: (pid, gid, cursor, has_row, delete).
    pub shadows: Vec<(Pid, Gid, Cur, bool, bool)>,
    /// Groups registered by the bundle (their meta goes live on commit).
    pub registers: Vec<(Arc<Group>, crate::rsm::effect::GroupMeta)>,
    pub at_us: i64,
}

#[derive(Default)]
pub(crate) struct Reservations {
    pub map: HashMap<RequestId, Reservation>,
    /// Bundles with acks that committed recently: a retry of the same
    /// request id (the batcher answers it from its record) gets the same
    /// part back instead of a stale-ack refusal. Kept for less than the
    /// batcher's request-id window, and bounded.
    pub recent: HashMap<RequestId, TxnPart>,
    pub recent_order: std::collections::VecDeque<(RequestId, i64)>,
}

/// How many committed bundles [`Reservations::recent`] keeps at most.
const RECENT_MAX: usize = 65_536;

/// How long a committed bundle's part is kept for a retry: just under the
/// batcher's request-id window (`QUEEN_RAFT_REQUEST_ID_WINDOW_S`, 60 s).
fn recent_ttl_us() -> i64 {
    static TTL: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *TTL.get_or_init(|| {
        let window_s = std::env::var("QUEEN_RAFT_REQUEST_ID_WINDOW_S")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(60);
        (window_s * 1_000_000 * 9 / 10).max(1_000_000)
    })
}

/// A part as a bundle sees it: (cursor, has_row, delete, dead letters, the
/// live version the shadow was read at).
type Shadow = (Cur, bool, bool, Vec<Effect>, u64);

fn qtxn(n: usize, pid: Pid) -> Refusal {
    Refusal::client(
        "rejected_ack",
        format!(
            "QTXN {n} acked message(s) on partition {pid} are not leased by this worker or were \
             already acked; the transaction rolled back"
        ),
    )
}

impl Engine {
    pub(crate) fn prepare(&self, txn: &TxnCommand, now: i64) -> Result<TxnPart, Refusal> {
        // One incarnation from the shadows to the reservation ([`Engine::serving`]).
        let _serving = super::state::read(&self.serving);
        if !self.leader.load(std::sync::atomic::Ordering::Acquire)
            || self.now_us()
                < self
                    .drain_until_us
                    .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Refusal::retry("not_leader", "this node does not lead"));
        }
        {
            let t = lock(&self.txns);
            if let Some(r) = t.map.get(&txn.request_id) {
                return Ok(r.part.clone());
            }
            if let Some(p) = t.recent.get(&txn.request_id) {
                return Ok(p.clone());
            }
        }
        let seg = self.seg_source();
        let out = self.store.read(|r| {
            let fr = self.frames(r, &seg);
            // The shadows, keyed by (pid, gid), in first-touch order.
            let mut order: Vec<(Pid, Gid)> = Vec::new();
            // (cursor, has_row, delete, dead letters, the live version read).
            let mut shadows: HashMap<(Pid, Gid), Shadow> = HashMap::new();
            let mut groups: HashMap<Gid, Arc<Group>> = HashMap::new();
            let mut acks = AckOutcome::default();
            let mut effects: Vec<Effect> = Vec::new();
            let mut registers: Vec<(Arc<Group>, crate::rsm::effect::GroupMeta)> = Vec::new();
            let mut planner_positions = Vec::new();

            // Load a shadow (the live part's cursor, unless this bundle
            // touched it already).
            macro_rules! shadow {
                ($g:expr, $pid:expr) => {{
                    let key = ($pid, $g.id);
                    if !shadows.contains_key(&key) {
                        let sh = lock(&self.shards[shard_of($pid)]);
                        let Some(p) = sh.groups.get(&$g.id).and_then(|gs| gs.parts.get(&$pid))
                        else {
                            return Ok(Err(Refusal::retry("unavailable", "part not loaded")));
                        };
                        if p.reserved.is_some_and(|id| id != txn.request_id) || p.in_flight() {
                            return Ok(Err(Refusal::retry(
                                "reserved",
                                "another transaction or a checkpoint holds this cursor; retry",
                            )));
                        }
                        shadows.insert(key, (p.cur.clone(), p.has_row, false, Vec::new(), p.ver));
                        order.push(key);
                        groups.insert($g.id, $g.clone());
                    }
                    shadows.get_mut(&key).expect("inserted")
                }};
            }

            for t in &txn.acks {
                let Some(g) =
                    self.target_part(r, &fr, &t.tenant, &t.queue, &t.group, t.pid, now)?
                else {
                    return Ok(Err(qtxn(t.items.len(), t.pid)));
                };
                let cfg = g.cfg();
                let txns_start = {
                    let sh = lock(&self.shards[shard_of(t.pid)]);
                    sh.pids.get(&t.pid).map_or(0, |pi| pi.txns_start)
                };
                let s = shadow!(g, t.pid);
                let acked = ack_target(&fr, &mut s.0, s.1, txns_start, t, &cfg.queue, now, false)?;
                if !acked.res.stale_hashes.is_empty() {
                    return Ok(Err(qtxn(acked.res.stale_hashes.len(), t.pid)));
                }
                s.3.extend(acked.dlq);
                acks.results.push(acked.res);
            }
            for a in &txn.positional_acks {
                let Some(g) =
                    self.target_part(r, &fr, &a.tenant, &a.queue, &a.group, a.pid, now)?
                else {
                    continue;
                };
                let s = shadow!(g, a.pid);
                match positional(&mut s.0, s.1, a, now) {
                    Ok(Some(res)) => acks.results.push(res),
                    Ok(None) => {}
                    Err(refusal) => return Ok(Err(refusal)),
                }
            }
            // Positions: after the acks, before the planner's keys.
            let mut registered: std::collections::HashSet<Gid> = Default::default();
            for op in &txn.positions {
                let Some(g) = self.group_for(r, &txn.tenant, &op.queue, &op.group)? else {
                    return Ok(Err(Refusal::client(
                        "queue_not_found",
                        format!("queue {} does not exist", op.queue),
                    )));
                };
                let live = match r.pid_of(&txn.tenant, &op.queue, &op.partition)? {
                    Some(pid) if !r.is_garbage(pid)? => Some(pid),
                    _ => None,
                };
                let Some(pid) = live else {
                    // No partition by that name yet: the planner creates it
                    // (it allocates pids) with its cursor row, in this entry.
                    if op.offset.is_some() {
                        planner_positions.push(op.clone());
                    }
                    continue;
                };
                if !self.ensure_part(r, &fr, &g, pid, Seed::Policy, now)? {
                    continue;
                }
                let s = shadow!(g, pid);
                let Some(offset) = op.offset else {
                    if s.1 {
                        s.1 = false;
                        s.2 = true;
                    }
                    continue;
                };
                let mut row = s.0.clone();
                row.release();
                row.batch_retry_count = 0;
                row.attempt_offset = None;
                row.attempt_count = 0;
                row.committed = offset as i64 - 1;
                row.metadata = op.metadata.clone();
                if s.1 && same(&row, &s.0) {
                    // Already exactly there: nothing is written.
                    continue;
                }
                if !s.1 {
                    row.created_at_us = now;
                    row.total_consumed = 0;
                }
                s.0 = row;
                s.1 = true;
                s.2 = false;
                if g.cfg().meta.is_none() && registered.insert(g.id) {
                    let meta = registration_meta("", "", &op.sub, false, now);
                    effects.push(Effect::GroupUpsert {
                        tenant: txn.tenant.clone(),
                        queue: op.queue.clone(),
                        group: op.group.clone(),
                        meta: meta.clone(),
                    });
                    registers.push((g.clone(), meta));
                }
            }

            // Reserve, and write out the rows that changed.
            let mut reserved: Vec<(Pid, Gid)> = Vec::new();
            let mut out_shadows = Vec::new();
            for key in &order {
                let (pid, gid) = *key;
                let (cur, has_row, delete, dlq, ver) = shadows.remove(key).expect("ordered");
                let mut sh = lock(&self.shards[shard_of(pid)]);
                let Some(p) = sh
                    .groups
                    .get_mut(&gid)
                    .and_then(|gs| gs.parts.get_mut(&pid))
                else {
                    drop(sh);
                    self.unreserve(&reserved, &txn.request_id);
                    return Ok(Err(Refusal::retry("unavailable", "partition went away")));
                };
                if p.reserved.is_some_and(|id| id != txn.request_id)
                    || p.in_flight()
                    || p.ver != ver
                {
                    // Held by another bundle or a checkpoint, or changed since
                    // the shadow was read (a claim in between): the bundle
                    // retries against the new state.
                    drop(sh);
                    self.unreserve(&reserved, &txn.request_id);
                    return Ok(Err(Refusal::retry(
                        "reserved",
                        "another transaction or a checkpoint holds this cursor; retry",
                    )));
                }
                let changed =
                    !same(&cur, &p.cur) || has_row != p.has_row || delete || !dlq.is_empty();
                p.reserved = Some(txn.request_id);
                reserved.push((pid, gid));
                if !changed {
                    continue;
                }
                // The bundle's row carries the part's current state — with any
                // change an ack, a nack or a DLQ head made since a checkpoint
                // last took it (a reserved part is no checkpoint's): from here
                // those answers are in doubt at a step-down, as if a
                // checkpoint carried them (the entry may commit under the next
                // leader, and a re-run would find its lease released).
                p.doubt_ver = p.doubt_ver.max(p.ver);
                let carried: Vec<u64> = p
                    .waiters
                    .iter()
                    .filter(|(_, v)| *v <= p.ver)
                    .map(|(a, _)| *a)
                    .collect();
                if !carried.is_empty() {
                    self.mark_sent(&carried);
                }
                let g = &groups[&gid];
                if delete {
                    effects.push(Effect::CursorDelete {
                        pid,
                        group: g.name.clone(),
                    });
                } else if has_row {
                    effects.push(Effect::CursorSet {
                        pid,
                        group: g.name.clone(),
                        row: cur.row(),
                    });
                }
                effects.extend(dlq);
                out_shadows.push((pid, gid, cur, has_row, delete));
            }
            // Parts reserved with nothing to write are released at once.
            let keep: std::collections::HashSet<(Pid, Gid)> =
                out_shadows.iter().map(|(p, g, ..)| (*p, *g)).collect();
            let idle: Vec<(Pid, Gid)> = reserved
                .iter()
                .copied()
                .filter(|k| !keep.contains(k))
                .collect();
            self.unreserve(&idle, &txn.request_id);
            Ok(Ok((
                TxnPart {
                    effects,
                    acks,
                    planner_positions,
                },
                out_shadows,
                registers,
            )))
        });
        let (part, shadows, registers) = out.map_err(Refusal::from_store)??;
        if !shadows.is_empty() || !registers.is_empty() {
            lock(&self.txns).map.insert(
                txn.request_id,
                Reservation {
                    part: part.clone(),
                    shadows,
                    registers,
                    at_us: now,
                },
            );
        }
        Ok(part)
    }

    /// Release the reservation of `keys` held by `id`.
    fn unreserve(&self, keys: &[(Pid, Gid)], id: &RequestId) {
        let now = self.now_us();
        let grace = self.grace();
        let mut woken = Vec::new();
        for (pid, gid) in keys {
            let mut sh = lock(&self.shards[shard_of(*pid)]);
            let sh = &mut *sh;
            if let Some(p) = sh.groups.get_mut(gid).and_then(|gs| gs.parts.get_mut(pid)) {
                if p.reserved == Some(*id) {
                    // (A dirty part stayed in the dirty list while reserved:
                    // the checkpoint keeps what it cannot send yet.)
                    p.reserved = None;
                }
            }
            if super::pop::arm(sh, *gid, *pid, now, grace) {
                woken.push(*gid);
            }
        }
        for gid in woken {
            self.wake_group(gid);
        }
    }

    pub(crate) fn resolve_txn(&self, id: RequestId, committed: bool, now: i64) {
        let res = {
            let mut t = lock(&self.txns);
            let Some(res) = t.map.remove(&id) else {
                return;
            };
            if committed && !res.part.acks.results.is_empty() {
                t.recent.insert(id, res.part.clone());
                t.recent_order.push_back((id, now));
            }
            res
        };
        let keys: Vec<(Pid, Gid)> = res.shadows.iter().map(|(p, g, ..)| (*p, *g)).collect();
        if committed {
            for (g, meta) in &res.registers {
                if g.cfg().meta.is_none() {
                    let cfg = g.cfg();
                    g.set_cfg(GroupCfg {
                        queue: cfg.queue.clone(),
                        meta: Some(meta.clone()),
                    });
                }
            }
            let grace = self.grace();
            let mut settled = Vec::new();
            let deleted: Vec<(Pid, Gid, (), (), bool)> = res
                .shadows
                .iter()
                .filter(|s| s.4)
                .map(|s| (s.0, s.1, (), (), true))
                .collect();
            for (pid, gid, cur, has_row, delete) in res.shadows {
                let mut sh = lock(&self.shards[shard_of(pid)]);
                let sh = &mut *sh;
                let (old_worker, new_worker) = {
                    let Some(p) = sh
                        .groups
                        .get_mut(&gid)
                        .and_then(|gs| gs.parts.get_mut(&pid))
                    else {
                        continue;
                    };
                    if p.reserved != Some(id) {
                        continue;
                    }
                    let old = p.cur.worker().map(Arc::<str>::from);
                    p.cur = cur;
                    p.has_row = has_row && !delete;
                    p.seed_ts = None;
                    // The entry wrote the row: durable at this version, and
                    // whatever waited on an earlier one with it.
                    p.ver += 1;
                    p.durable_ver = p.ver;
                    let durable = p.durable_ver;
                    p.waiters.retain(|(a, v)| {
                        if *v <= durable {
                            settled.push(*a);
                            false
                        } else {
                            true
                        }
                    });
                    (old, p.cur.lease.as_ref().map(|l| l.worker.clone()))
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
            }
            for a in settled {
                self.settle(a, 1);
            }
            // A forgotten position: the part is a first contact again, seeded
            // by the group's policy when it is next needed.
            for (pid, gid, _, _, delete) in deleted {
                let g = {
                    let mut sh = lock(&self.shards[shard_of(pid)]);
                    let sh = &mut *sh;
                    let g = sh.groups.get(&gid).map(|gs| gs.g.clone());
                    if let Some(gs) = sh.groups.get_mut(&gid) {
                        if let Some(p) = gs.parts.get(&pid) {
                            if p.reserved == Some(id) && delete {
                                if p.queued {
                                    gs.ready.retain(|x| *x != pid);
                                    gs.g.unready();
                                }
                                gs.parts.remove(&pid);
                            }
                        }
                    }
                    if let Some(pi) = sh.pids.get_mut(&pid) {
                        pi.watchers.retain(|x| *x != gid);
                    }
                    g
                };
                if let Some(g) = g {
                    let mut st = lock(&g.st);
                    if st.load != super::state::Load::Partial {
                        st.late.push(pid);
                    }
                }
            }
        }
        self.unreserve(&keys, &id);
        let _ = now;
    }

    /// Reservations nobody resolved: released (as refused).
    pub(crate) fn expire_txns(&self, now: i64) {
        {
            let mut t = lock(&self.txns);
            let ttl = recent_ttl_us();
            while let Some((id, at)) = t.recent_order.front().copied() {
                if now.saturating_sub(at) <= ttl && t.recent_order.len() <= RECENT_MAX {
                    break;
                }
                t.recent_order.pop_front();
                t.recent.remove(&id);
            }
        }
        let ttl = self.k.txn_ttl_us;
        let stale: Vec<RequestId> = lock(&self.txns)
            .map
            .iter()
            .filter(|(_, r)| now.saturating_sub(r.at_us) > ttl)
            .map(|(id, _)| *id)
            .collect();
        for id in stale {
            self.resolve_txn(id, false, now);
        }
    }
}
