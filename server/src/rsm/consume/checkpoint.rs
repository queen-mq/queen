//! Checkpoints and the answers that wait on them.
//!
//! A change to a part bumps its version and marks it dirty. [`Engine::take`]
//! drains the dirty parts into one checkpoint: per (tenant, lane) one
//! `Effects` command of whole `CursorSet` rows (a forgotten position is a
//! `CursorDelete`) with the part's `DlqInsert`s beside its row, and per tenant
//! one command of catalog effects (registrations, a pop's implicit queue). A
//! part whose previous row is still in flight waits for the next checkpoint,
//! so one (pid, group) is never in two tickets at once: rows land in order
//! whichever planner lane or control step carries them.
//!
//! An answer lists the (part, version) rows — and catalog versions — it
//! depends on; it is sent once every one of them is durable. A committed
//! ticket makes its rows durable at the version it carried; a refused one
//! leaves its parts dirty (the next checkpoint writes their CURRENT state) and
//! puts its dead letters and catalog effects back. `QUEEN_CONSUME_FAST=1`
//! answers at once.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::sync::oneshot;

use crate::rsm::batcher::Reply;
use crate::rsm::effect::{Effect, Pid};
use crate::rsm::planner::EffectsCommand;
use crate::rsm::store::{Store, TypedReads};

use super::state::{lock, read, shard_of, Gid, Group, PendingAnswer, Ticket};
use super::{Checkpoint, Engine};

/// What an answer waits on.
#[derive(Clone, Default)]
pub(crate) struct Deps {
    /// (pid, gid, version) rows.
    pub rows: Vec<(Pid, Gid, u64)>,
    /// (group, catalog version).
    pub cats: Vec<(Arc<Group>, u64)>,
    /// The leases a claim answer grants: handed back when nobody receives it.
    pub claims: Vec<(Pid, Gid, Arc<str>, u64)>,
    /// The command must not run twice ([`PendingAnswer::exact`]): if this
    /// node stops leading while its change may be in the log, the answer is
    /// [`super::in_doubt`], never a `Retry` another leader would run again —
    /// an ack's second run finds the lease its first run released and calls
    /// every message stale (2026-09-30, Jepsen p11-w2-restart: a graceful
    /// leader restart, an ack answered stale, its message never delivered
    /// again).
    pub exact: bool,
}

impl Deps {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.cats.is_empty()
    }
}

impl Engine {
    /// Deliver `reply` once `deps` are durable: `Some(reply)` when it can go
    /// now (the caller delivers it), `None` when the sink was taken.
    pub(crate) fn answer(
        &self,
        reply: Reply,
        deps: Deps,
        sink: &mut Option<oneshot::Sender<Reply>>,
    ) -> Option<Reply> {
        if self.k.fast || deps.is_empty() {
            return Some(reply);
        }
        let Some(tx) = sink.take() else {
            return Some(reply);
        };
        let n = deps.rows.len() + deps.cats.len();
        let id = {
            let mut a = lock(&self.answers);
            a.next += 1;
            let id = a.next;
            a.map.insert(
                id,
                PendingAnswer {
                    remaining: n + 1,
                    reply,
                    sink: tx,
                    claims: deps.claims.clone(),
                    exact: deps.exact,
                    sent: false,
                },
            );
            id
        };
        let mut met = 1usize; // the registration guard
                              // A checkpoint may have taken the change before this answer waited
                              // on it (the command changed the part, then let the shard go): in
                              // flight, or resolved without a commit seen, it is in doubt already.
        let mut sent = false;
        for (pid, gid, ver) in &deps.rows {
            let mut sh = lock(&self.shards[shard_of(*pid)]);
            match sh.groups.get_mut(gid).and_then(|gs| gs.parts.get_mut(pid)) {
                Some(p) if p.durable_ver < *ver => {
                    p.waiters.push((id, *ver));
                    sent |= p.sent_ver >= *ver || p.doubt_ver >= *ver;
                }
                _ => met += 1,
            }
        }
        for (g, ver) in &deps.cats {
            let mut st = lock(&g.st);
            if st.cat_durable < *ver && !st.dropped {
                st.cat_waiters.push((id, *ver));
                sent |= st.cat_sent_ver >= *ver || st.cat_doubt >= *ver;
            } else {
                met += 1;
            }
        }
        if sent {
            self.mark_sent(&[id]);
        }
        self.settle(id, met);
        None
    }

    /// The answers `ids` wait on a change a checkpoint (or a transaction's
    /// entry) carried.
    pub(crate) fn mark_sent(&self, ids: &[u64]) {
        let mut a = lock(&self.answers);
        for id in ids {
            if let Some(p) = a.map.get_mut(id) {
                p.sent = true;
            }
        }
    }

    /// `n` more dependencies of answer `id` are durable.
    pub(crate) fn settle(&self, id: u64, n: usize) {
        let done = {
            let mut a = lock(&self.answers);
            match a.map.get_mut(&id) {
                Some(p) => {
                    p.remaining = p.remaining.saturating_sub(n);
                    if p.remaining == 0 {
                        a.map.remove(&id)
                    } else {
                        None
                    }
                }
                None => None,
            }
        };
        if let Some(p) = done {
            if let Err(reply) = p.sink.send(p.reply) {
                self.release_claims(&p.claims, &reply);
            }
        }
    }

    /// Nobody receives a claim answer: release its leases at once (the nack
    /// the caller can no longer send).
    pub(crate) fn release_claims(&self, claims: &[(Pid, Gid, Arc<str>, u64)], reply: &Reply) {
        if !matches!(reply, Reply::Done { .. }) {
            return;
        }
        let now = self.now_us();
        let grace = self.grace();
        let mut woken: Vec<Gid> = Vec::new();
        for (pid, gid, worker, end) in claims {
            let mut sh = lock(&self.shards[shard_of(*pid)]);
            let sh = &mut *sh;
            let Some(p) = sh.groups.get_mut(gid).and_then(|gs| gs.parts.get_mut(pid)) else {
                continue;
            };
            let holds = p
                .cur
                .lease
                .as_ref()
                .is_some_and(|l| &l.worker == worker && l.batch_end == *end);
            if !holds || p.reserved.is_some() {
                continue;
            }
            p.cur.release_undelivered();
            super::pop::touch(p, &mut sh.dirty, *pid, *gid);
            sh.unindex_lease(worker, *pid, *gid);
            if super::pop::arm(sh, *gid, *pid, now, grace) {
                woken.push(*gid);
            }
        }
        for gid in woken {
            self.wake_group(gid);
        }
    }

    /// Drain the dirty state into one checkpoint.
    pub(crate) fn take(&self, now_us: i64) -> Option<Checkpoint> {
        // A reset waits for this take to finish ([`Engine::serving`]): the
        // answers it marks sent are marked before the reset reads them.
        let _serving = super::state::read(&self.serving);
        // The incarnation this checkpoint is of: one that ends while it is
        // taken (a step-down resets the engine) registers nothing, so the
        // batcher never plans its rows ([`super::is_checkpoint_id`]).
        let gen = self.gen.load(Ordering::Acquire);
        let lanes = self.k.lanes.max(1);
        // (tenant, lane) -> the effects, part by part (a part's row and its
        // dead letters stay together).
        let mut per: HashMap<(String, u64), Vec<Vec<Effect>>> = HashMap::new();
        let mut ticket = Ticket::default();
        // The answers waiting on a change this checkpoint carries.
        let mut sent: Vec<u64> = Vec::new();
        for s in self.shards.iter() {
            let mut sh = lock(s);
            let sh = &mut *sh;
            if sh.dirty.is_empty() {
                continue;
            }
            let dirty = std::mem::take(&mut sh.dirty);
            let mut keep: Vec<(Pid, Gid)> = Vec::new();
            for (pid, gid) in dirty {
                let Some(gs) = sh.groups.get_mut(&gid) else {
                    continue;
                };
                let (tenant, group) = (gs.g.tenant.clone(), gs.g.name.clone());
                let Some(p) = gs.parts.get_mut(&pid) else {
                    continue;
                };
                if !p.dirty {
                    continue;
                }
                if p.in_flight() || p.reserved.is_some() {
                    keep.push((pid, gid));
                    continue;
                }
                p.dirty = false;
                let mut effs: Vec<Effect> = Vec::with_capacity(1 + p.dlq.len());
                if p.delete_row {
                    p.delete_row = false;
                    effs.push(Effect::CursorDelete {
                        pid,
                        group: group.clone(),
                    });
                } else if p.has_row {
                    effs.push(Effect::CursorSet {
                        pid,
                        group: group.clone(),
                        row: self.logged_row(&p.cur),
                    });
                }
                let dlq = std::mem::take(&mut p.dlq);
                effs.extend(dlq.iter().cloned());
                p.dlq_sent.extend(dlq);
                p.sent_ver = p.ver;
                sent.extend(
                    p.waiters
                        .iter()
                        .filter(|(_, v)| *v <= p.ver)
                        .map(|(a, _)| *a),
                );
                ticket.rows.push((pid, gid, p.ver));
                if !effs.is_empty() {
                    per.entry((tenant, pid % lanes)).or_default().push(effs);
                }
            }
            sh.dirty.extend(keep);
        }
        // The catalog: registrations and implicit queues, per tenant.
        let mut cats: HashMap<String, Vec<Effect>> = HashMap::new();
        {
            let groups = std::mem::take(&mut *lock(&self.cat_dirty));
            let mut keep = Vec::new();
            for g in groups {
                let mut st = lock(&g.st);
                if st.dropped {
                    continue;
                }
                if !st.cat_sent.is_empty() {
                    drop(st);
                    keep.push(g);
                    continue;
                }
                st.cat_dirty = false;
                let effs = std::mem::take(&mut st.cat);
                st.cat_sent = effs.clone();
                st.cat_sent_ver = st.cat_ver;
                let ver = st.cat_ver;
                sent.extend(
                    st.cat_waiters
                        .iter()
                        .filter(|(_, v)| *v <= ver)
                        .map(|(a, _)| *a),
                );
                ticket.cats.push((g.id, st.cat_ver));
                drop(st);
                cats.entry(g.tenant.clone()).or_default().extend(effs);
            }
            lock(&self.cat_dirty).extend(keep);
        }
        // An implicit queue a push created in the meantime is not created
        // twice.
        if cats
            .values()
            .flatten()
            .any(|e| matches!(e, Effect::QueueUpsert { .. }))
        {
            let _ = self.store.read(|r| {
                for effs in cats.values_mut() {
                    let mut keep = Vec::with_capacity(effs.len());
                    for e in effs.drain(..) {
                        if let Effect::QueueUpsert { tenant, queue, .. } = &e {
                            if r.queue(tenant, queue)?.is_some() {
                                continue;
                            }
                        }
                        keep.push(e);
                    }
                    *effs = keep;
                }
                Ok(())
            });
        }
        if ticket.rows.is_empty() && ticket.cats.is_empty() {
            return None;
        }
        let mut commands: Vec<EffectsCommand> = Vec::new();
        let mut tenants: Vec<String> = cats.keys().cloned().collect();
        tenants.sort();
        for t in tenants {
            let effects = cats.remove(&t).unwrap_or_default();
            if !effects.is_empty() {
                commands.push(EffectsCommand {
                    request_id: super::checkpoint_id(),
                    tenant: t,
                    effects,
                });
            }
        }
        let mut keys: Vec<(String, u64)> = per.keys().cloned().collect();
        keys.sort();
        for key in keys {
            let parts = per.remove(&key).unwrap_or_default();
            let mut cur: Vec<Effect> = Vec::new();
            let mut rows = 0usize;
            for effs in parts {
                if rows >= self.k.rows_per_cmd && !cur.is_empty() {
                    commands.push(EffectsCommand {
                        request_id: super::checkpoint_id(),
                        tenant: key.0.clone(),
                        effects: std::mem::take(&mut cur),
                    });
                    rows = 0;
                }
                rows += 1;
                cur.extend(effs);
            }
            if !cur.is_empty() {
                commands.push(EffectsCommand {
                    request_id: super::checkpoint_id(),
                    tenant: key.0.clone(),
                    effects: cur,
                });
            }
        }
        let _ = now_us;
        ticket.ids = commands.iter().map(|c| c.request_id).collect();
        let id = {
            let mut t = lock(&self.tickets);
            if self.gen.load(Ordering::Acquire) != gen {
                return None;
            }
            let id = t.next;
            t.next += 1;
            t.ids.extend(ticket.ids.iter().copied());
            t.inflight.insert(id, ticket);
            id
        };
        // Registered: the batcher may now plan it, so what it carries is in
        // doubt from here (not before: a take that registers nothing hands
        // the batcher nothing).
        if !sent.is_empty() {
            self.mark_sent(&sent);
        }
        if commands.is_empty() {
            // Nothing to log (the rows were first contacts that went away):
            // durable as it is.
            self.resolve_ticket(id, true);
            return None;
        }
        Some(Checkpoint {
            ticket: id,
            commands,
        })
    }

    /// A ticket committed (every command of it) or was refused (one at least).
    pub(crate) fn resolve_ticket(&self, id: u64, committed: bool) {
        let t = {
            let mut ts = lock(&self.tickets);
            let Some(t) = ts.inflight.remove(&id) else {
                return;
            };
            for rid in &t.ids {
                ts.ids.remove(rid);
            }
            t
        };
        let now = self.now_us();
        let mut gone: Vec<Pid> = Vec::new();
        let mut settled: Vec<u64> = Vec::new();
        for (pid, gid, ver) in &t.rows {
            let mut sh = lock(&self.shards[shard_of(*pid)]);
            let sh = &mut *sh;
            let Some(p) = sh.groups.get_mut(gid).and_then(|gs| gs.parts.get_mut(pid)) else {
                continue;
            };
            if p.sent_ver == *ver {
                p.sent_ver = 0;
            }
            if committed {
                p.durable_ver = p.durable_ver.max(*ver);
                p.dlq_sent.clear();
                let durable = p.durable_ver;
                p.waiters.retain(|(a, v)| {
                    if *v <= durable {
                        settled.push(*a);
                        false
                    } else {
                        true
                    }
                });
            } else {
                // Refused, or its fate unknown (a step-down answers the
                // batcher's commands `Retry` whether or not their entry is
                // in the log): an answer at this version is in doubt.
                p.doubt_ver = p.doubt_ver.max(*ver);
                // Its dead letters go again, before any decided since.
                let mut back = std::mem::take(&mut p.dlq_sent);
                back.append(&mut p.dlq);
                p.dlq = back;
                if !p.dirty {
                    p.dirty = true;
                    sh.dirty.push((*pid, *gid));
                }
                gone.push(*pid);
            }
        }
        let by_id: Vec<(Arc<Group>, u64)> = {
            let reg = read(&self.reg);
            t.cats
                .iter()
                .filter_map(|(gid, v)| reg.by_id.get(gid).map(|g| (g.clone(), *v)))
                .collect()
        };
        for (g, ver) in by_id {
            let mut st = lock(&g.st);
            if committed {
                st.cat_durable = st.cat_durable.max(ver);
                st.cat_sent.clear();
                let durable = st.cat_durable;
                st.cat_waiters.retain(|(a, v)| {
                    if *v <= durable {
                        settled.push(*a);
                        false
                    } else {
                        true
                    }
                });
            } else {
                st.cat_doubt = st.cat_doubt.max(ver);
                let mut back = std::mem::take(&mut st.cat_sent);
                back.append(&mut st.cat);
                st.cat = back;
                if !st.cat_dirty {
                    st.cat_dirty = true;
                    drop(st);
                    lock(&self.cat_dirty).push(g.clone());
                }
            }
        }
        for a in settled {
            self.settle(a, 1);
        }
        if !committed && !gone.is_empty() {
            // A refused checkpoint names a partition a delete in flight takes
            // away, most likely: drop the ones that are gone, so the next one
            // is not refused again.
            gone.sort_unstable();
            gone.dedup();
            let dead: Vec<Pid> = self
                .store
                .read(|r| {
                    let mut dead = Vec::new();
                    for pid in &gone {
                        if r.is_garbage(*pid)? || !r.has_partition(*pid)? {
                            dead.push(*pid);
                        }
                    }
                    Ok(dead)
                })
                .unwrap_or_default();
            for pid in dead {
                self.drop_pid(pid, None);
            }
            let _ = now;
        }
        let _ = Ordering::Relaxed;
    }
}
