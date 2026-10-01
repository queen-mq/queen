//! Pops — the port of the planner's `004_log_pop` (`planner/pop.rs` at
//! 08050fed): pinned, wildcard and discovery claims sharing one budget, the
//! width (`max_parts`), first-contact registration and seeding by the
//! subscription instant, conflation (the newest frame, leasing the whole
//! span), delayed processing and the window buffer, the lease, auto-ack, the
//! delivery attempt, the empty-partition seal, and `deadline_us` (never claim
//! for a pop nobody can receive).
//!
//! What changes from the planner: the ready ring is the engine's own (a part
//! is armed by an append, a released lease, an expired one or a hold running
//! out), and a long-poll pop that finds nothing is held here ([`super::wait`]).

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::sync::oneshot;

use crate::rsm::batcher::{Command, Reply};
use crate::rsm::effect::Pid;
use crate::rsm::entry::{Outcome, PopClaim, PopOutcome};
use crate::rsm::planner::{PopCommand, Refusal, MAX_PARTS_AUTO};
use crate::rsm::store::{Reads, Store, StoreError, TypedReads};

use super::checkpoint::Deps;
use super::frames::Frames;
use super::load::{contiguous, registration_meta, Seed};
use super::state::{
    lock, schedule, shard_of, Gid, Group, GroupCfg, Held, Lease, Load, Shard, Waiter, SHARDS,
};
use super::{Engine, SEC_US};

type Result<T> = std::result::Result<T, StoreError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Wildcard,
    Pinned,
    Discover,
}

/// Arm `(gid, pid)`: into the ready list when claimable now, or a timer when
/// a lease or a hold ends later. `true` when it became ready.
pub(crate) fn arm(sh: &mut Shard, gid: Gid, pid: Pid, now_us: i64, grace_us: i64) -> bool {
    let Shard {
        pids,
        groups,
        timers,
        ..
    } = sh;
    let (Some(pi), Some(gs)) = (pids.get(&pid), groups.get_mut(&gid)) else {
        return false;
    };
    let Some(part) = gs.parts.get_mut(&pid) else {
        return false;
    };
    if part.queued || part.reserved.is_some() {
        return false;
    }
    if part.cur.leased(now_us, grace_us) {
        if let Some(t) = part.cur.lease_until(grace_us) {
            schedule(timers, part, t, gid, pid);
        }
        return false;
    }
    if pi.tail <= part.cur.committed && part.seed_ts.is_none() {
        return false;
    }
    if part.ready_at > now_us {
        let t = part.ready_at;
        schedule(timers, part, t, gid, pid);
        return false;
    }
    part.ready_at = 0;
    part.queued = true;
    gs.ready.push_back(pid);
    gs.g.ready_n.fetch_add(1, Ordering::AcqRel);
    gs.g.bump();
    true
}

/// What one claim attempt did.
pub(crate) enum Claimed {
    /// Delivered: the claim, and the frames it took.
    Took(PopClaim, i64),
    /// Nothing now; claimable again from this instant (a delay, a window).
    Hold(i64),
    /// Nothing (caught up, leased, or a seal that moved the cursor).
    Nothing,
}

/// The claim knobs of one pop.
pub(crate) struct ClaimReq<'a> {
    pub worker: &'a Arc<str>,
    pub lease_seconds: i32,
    pub auto_ack: bool,
    pub conflate: bool,
    pub skip_window: bool,
}

/// Claim frames of ONE part for one group — the claim core (004
/// `log_pop_v1`, the planner's `claim_one`). Mutates the part in place and
/// marks it changed when it delivered (or sealed).
#[allow(clippy::too_many_arguments)]
pub(crate) fn claim_one<R: Reads + ?Sized>(
    fr: &Frames<'_, R>,
    sh: &mut Shard,
    gid: Gid,
    pid: Pid,
    cfg: &GroupCfg,
    req: &ClaimReq<'_>,
    budget: i64,
    now: i64,
    grace: i64,
) -> Result<Claimed> {
    let Shard {
        pids,
        groups,
        workers,
        dirty,
        ..
    } = sh;
    let (Some(pi), Some(gs)) = (pids.get(&pid), groups.get_mut(&gid)) else {
        return Ok(Claimed::Nothing);
    };
    let Some(part) = gs.parts.get_mut(&pid) else {
        return Ok(Claimed::Nothing);
    };
    if part.reserved.is_some() || !fr.available() {
        return Ok(Claimed::Nothing);
    }
    let budget = budget.max(1);
    let tail = pi.tail;
    let log_start = pi.log_start;
    // The STORED policy wins over the request.
    let conflate = match &cfg.meta {
        Some(m) => m.conflation,
        None => req.conflate,
    };
    let q = &cfg.queue;

    // window_buffer quiet debounce: a partition written within the last
    // window_buffer seconds delivers nothing (a burst is one batch).
    let has_live = tail >= log_start as i64;
    if q.window_buffer > 0 && !req.skip_window && has_live {
        let win = q.window_buffer as i64 * SEC_US;
        let newest = match fr.r.partition_head(pid)? {
            Some(h) if h.last_offset >= h.log_start as i64 => Some(h.last_created_at_us),
            _ => None,
        };
        if let Some(newest) = newest {
            if newest > now - win {
                return Ok(Claimed::Hold(newest + win));
            }
        }
    }

    if part.cur.leased(now, grace) {
        return Ok(Claimed::Nothing);
    }

    // A first contact whose seed moves (an instant in the future).
    if let Some(ts) = part.seed_ts {
        let floor = log_start as i64 - 1;
        let s = fr
            .seed_from_ts(pid, log_start, tail, ts)?
            .max(floor)
            .max(-1);
        part.cur.committed = s;
        if ts <= now {
            part.seed_ts = None;
        }
    }

    let committed = part.cur.committed;
    let wanted = committed + 1;
    let deadline = (q.delayed_processing > 0).then(|| now - q.delayed_processing as i64 * SEC_US);
    let fresh = |c: i64| deadline.is_none_or(|d| c <= d);

    let mut taken: i64 = 0;
    let mut start: Option<i64> = None;
    let mut last: i64 = -1;
    let mut hold: Option<i64> = None;
    let mut run: Vec<(u64, [u8; 16])> = Vec::new();

    if has_live && tail >= wanted {
        let from = wanted.max(log_start as i64).max(0) as u64;
        if conflate {
            // ONE backward step to the newest fresh segment: its last frame is
            // served, (committed, its end] is leased.
            match fr.newest_fresh(pid, from, tail, &fresh)? {
                Some(head) if head.end as i64 >= wanted => {
                    taken = 1;
                    start = Some(wanted);
                    last = head.end as i64;
                }
                _ => {
                    if deadline.is_some() {
                        let first = fr.gather(pid, from, 1, tail, &|_| true, false)?;
                        if let Some(s) = first.iter().find(|s| s.base >= log_start) {
                            hold = Some(s.created_at_us + q.delayed_processing as i64 * SEC_US);
                        }
                    }
                }
            }
        } else {
            let segs: Vec<_> = fr
                .gather(pid, from, budget, tail, &fresh, !req.auto_ack)?
                .into_iter()
                .filter(|s| s.base >= log_start && s.base as i64 <= tail)
                .collect();
            // Head probe: the greatest base <= wanted.
            if let Some(h) = segs.iter().rev().find(|s| (s.base as i64) <= wanted) {
                if h.end as i64 >= wanted {
                    if fresh(h.created_at_us) {
                        let avail = h.end as i64 - wanted + 1;
                        let take = avail.min(budget);
                        taken = take;
                        start = Some(wanted);
                        last = wanted + take - 1;
                    } else {
                        hold = Some(h.created_at_us + q.delayed_processing as i64 * SEC_US);
                    }
                }
            }
            if taken < budget && hold.is_none() {
                for s in segs.iter().filter(|s| s.base as i64 > wanted) {
                    if !fresh(s.created_at_us) {
                        if taken == 0 {
                            hold = Some(s.created_at_us + q.delayed_processing as i64 * SEC_US);
                        }
                        break; // created_at monotone: everything after is deferred too
                    }
                    let avail = s.end as i64 - s.base as i64 + 1;
                    let take = avail.min(budget - taken);
                    if take <= 0 {
                        break;
                    }
                    if start.is_none() {
                        start = Some(s.base as i64); // retention gap: the batch starts here
                    }
                    taken += take;
                    last = s.base as i64 + take - 1;
                    if taken >= budget {
                        break;
                    }
                }
            }
            if taken > 0 && !req.auto_ack {
                let lo = start.expect("taken") as u64;
                for s in &segs {
                    if let Some(hs) = &s.hashes {
                        for (i, h) in hs.iter().enumerate() {
                            let off = s.base + i as u64;
                            if off >= lo && off as i64 <= last {
                                run.push((off, *h));
                            }
                        }
                    }
                }
            }
        }
    }

    if taken == 0 {
        // Empty-partition seal: every segment was removed by retention, so
        // last_offset > committed would hold for ever. Seal to the tail, but
        // only with no live segment at all (a deferred one must not be
        // skipped).
        if tail > part.cur.committed && !has_live {
            part.cur.committed = tail;
            part.seed_ts = None;
            touch(part, dirty, pid, gid);
        }
        return Ok(match hold {
            Some(h) => Claimed::Hold(h),
            None => Claimed::Nothing,
        });
    }

    let start_off = start.expect("taken > 0 implies a start") as u64;
    let (read_start, read_end) = if conflate {
        (last as u64, last as u64)
    } else {
        (start_off, last as u64)
    };
    let delivery_attempt: u32;
    let lease_expires: Option<i64>;
    let old_worker = part.cur.worker().map(str::to_string);
    if req.auto_ack {
        part.cur.committed = last;
        part.cur.release();
        part.cur.total_consumed += taken as u64;
        delivery_attempt = 1;
        lease_expires = None;
    } else {
        // Attempt tracking: the SAME first offset as the previous non-auto
        // delivery is a redelivery; anywhere else resets to 1. The retry
        // budget is charged only by an explicit `failed`, never by an expiry.
        part.cur.attempt_count = if part.cur.attempt_offset == Some(start_off) {
            part.cur.attempt_count + 1
        } else {
            1
        };
        part.cur.attempt_offset = Some(start_off);
        let exp = now + req.lease_seconds.max(1) as i64 * SEC_US;
        let (frames_lo, frames, delivered) = if conflate {
            let d: Vec<[u8; 16]> = fr
                .hashes(pid, read_end, read_end)?
                .into_iter()
                .map(|(_, h)| h)
                .collect();
            (0, Vec::new(), d)
        } else {
            let mut seen: HashSet<[u8; 16]> = HashSet::with_capacity(run.len());
            let delivered: Vec<[u8; 16]> = run
                .iter()
                .filter(|(o, _)| *o >= read_start && *o <= read_end)
                .map(|(_, h)| *h)
                .filter(|h| seen.insert(*h))
                .collect();
            if contiguous(&run, start_off, last as u64) {
                (start_off, run.iter().map(|(_, h)| *h).collect(), delivered)
            } else {
                (0, Vec::new(), delivered)
            }
        };
        part.cur.lease = Some(Lease {
            worker: req.worker.clone(),
            batch_end: last as u64,
            expires_us: exp,
            acquired_us: Some(now),
            conflated: conflate,
            delivered,
            frames_lo,
            frames,
            foreign: false,
        });
        delivery_attempt = part.cur.attempt_count.max(1);
        lease_expires = Some(exp);
    }
    part.seed_ts = None;
    touch(part, dirty, pid, gid);
    if let Some(w) = old_worker {
        if let Some(set) = workers.get_mut(w.as_str()) {
            set.remove(&(pid, gid));
            if set.is_empty() {
                workers.remove(w.as_str());
            }
        }
    }
    if lease_expires.is_some() && !req.worker.is_empty() {
        workers
            .entry(req.worker.clone())
            .or_default()
            .insert((pid, gid));
    }
    Ok(Claimed::Took(
        PopClaim {
            pid,
            start_offset: read_start,
            end_offset: read_end,
            worker: req.worker.to_string(),
            lease_expires_at_us: lease_expires,
            delivery_attempt,
            conflated: conflate,
        },
        taken,
    ))
}

/// Mark a part changed (its row goes to the next checkpoint).
pub(crate) fn touch(
    part: &mut super::state::Part,
    dirty: &mut Vec<(Pid, Gid)>,
    pid: Pid,
    gid: Gid,
) -> u64 {
    part.ver += 1;
    part.has_row = true;
    part.delete_row = false;
    if !part.dirty {
        part.dirty = true;
        dirty.push((pid, gid));
    }
    part.ver
}

/// The widest claim the autopilot picks (partitions per pop).
const AUTO_WIDTH_MAX: usize = 64;

/// How many partitions a pop of `g` may claim: the client's `max_parts` (`0`:
/// unlimited), or, for [`MAX_PARTS_AUTO`] (the pop autopilot left the width to
/// the broker), a width chosen here from exact state: the group's partitions
/// ready now, shared among its pops waiting and this one, 1 to 64. Only the
/// leader holds that count, so a pop a follower forwards is sized here too
/// (2026-09-30: a follower read it from `pending` rows every node kept; with
/// those gone it fell back to one partition, and its consumers fell behind).
pub(crate) fn width(c: &PopCommand, g: &Group) -> usize {
    match c.max_parts {
        MAX_PARTS_AUTO => {
            let ready = g.ready_n.load(Ordering::Acquire);
            let pops = g.waiting.load(Ordering::Acquire) + 1;
            ready.div_ceil(pops).clamp(1, AUTO_WIDTH_MAX)
        }
        n if n <= 0 => usize::MAX,
        n => n as usize,
    }
}

/// Whether nobody can receive this pop's answer any more (P1.2).
pub(crate) fn expired(cmd: &PopCommand, now_us: i64, margin_us: i64) -> bool {
    cmd.deadline_us > 0 && now_us.saturating_add(margin_us) > cmd.deadline_us
}

/// The claims of one walk and what the answer waits on.
#[derive(Default)]
pub(crate) struct Walk {
    pub claims: Vec<PopClaim>,
    pub deps: Deps,
}

impl Engine {
    /// A pop command (the three entry points).
    pub(crate) fn pop(
        &self,
        c: &PopCommand,
        kind: Kind,
        now: i64,
        sink: &mut Option<oneshot::Sender<Reply>>,
    ) -> std::result::Result<Option<Reply>, Refusal> {
        if expired(c, now, self.k.margin_us) {
            return Ok(Some(Engine::empty_pop()));
        }
        let seg = self.seg_source();
        let gen = self.gen.load(Ordering::Acquire);
        let res = self.store.read(|r| {
            let fr = self.frames(r, &seg);
            match kind {
                Kind::Discover => self.pop_discover(r, &fr, c, now, sink),
                Kind::Wildcard => self.pop_queue(r, &fr, c, now, sink, None),
                Kind::Pinned => {
                    let Some(name) = c.partition.as_deref() else {
                        return Ok(PopStep::Refuse(Refusal::client(
                            "bad_request",
                            "a pinned pop needs a partition",
                        )));
                    };
                    self.pop_pinned(r, &fr, c, name, now, sink)
                }
            }
        });
        let step = res.map_err(Refusal::from_store)?;
        if self.gen.load(Ordering::Acquire) != gen {
            return Ok(Some(Reply::Retry { hint: None }));
        }
        match step {
            PopStep::Taken => Ok(None),
            PopStep::Refuse(r) => Err(r),
            PopStep::Walked(walk, g, pinned) => {
                let Walk { claims, deps } = walk;
                if claims.is_empty() && deps.rows.is_empty() {
                    // Nothing: hold a long poll (its group's registration
                    // commits meanwhile), answer the rest empty — once a
                    // registration it made is durable.
                    if let (true, Some(g)) = (c.wait && kind != Kind::Discover, g) {
                        if let Some(tx) = sink.take() {
                            self.park(&g, c.clone(), pinned, tx, now);
                            return Ok(None);
                        }
                    }
                    if deps.cats.is_empty() {
                        return Ok(Some(Engine::empty_pop()));
                    }
                }
                let reply = Reply::Done {
                    outcome: Outcome::Pop(PopOutcome { claims }),
                    at: None,
                };
                Ok(self.answer(reply, deps, sink))
            }
        }
    }

    /// A wildcard pop of one queue (`queue` overrides the command's, for a
    /// discovery walk): register the group on first contact, load it whole,
    /// claim its ready parts sharing the budget.
    fn pop_queue<R: Reads + ?Sized>(
        &self,
        r: &R,
        fr: &Frames<'_, R>,
        c: &PopCommand,
        now: i64,
        sink: &mut Option<oneshot::Sender<Reply>>,
        queue: Option<&str>,
    ) -> Result<PopStep> {
        let queue = queue.unwrap_or(&c.queue);
        let mut deps = Deps::default();
        let g = match self.group_for(r, &c.tenant, queue, &c.group)? {
            Some(g) => g,
            None => {
                // A wildcard pop MAY create a missing queue (004 ≈1046) so the
                // group can register against it; without a create config it
                // answers empty.
                let Some(create) = &c.create_cfg else {
                    return Ok(PopStep::Walked(Walk::default(), None, None));
                };
                let mut q = create.clone();
                q.created_at_us = now;
                let g = self.intern(
                    &c.tenant,
                    queue,
                    &c.group,
                    GroupCfg {
                        queue: q.clone(),
                        meta: None,
                    },
                    Load::Full,
                );
                if g.cfg().meta.is_none() {
                    let meta = registration_meta(&c.namespace, &c.task, &c.sub, c.conflate, now);
                    let v = self.register(&g, meta, Some(q));
                    deps.cats.push((g.clone(), v));
                }
                let w = Walk {
                    claims: Vec::new(),
                    deps,
                };
                return Ok(PopStep::Walked(w, Some(g), None));
            }
        };
        if g.cfg().meta.is_none() {
            // First contact (§8, 004 the-registrar): one GroupUpsert built from
            // the pop-carried intent.
            let meta = registration_meta(&c.namespace, &c.task, &c.sub, c.conflate, now);
            let v = self.register(&g, meta, None);
            deps.cats.push((g.clone(), v));
        } else if let Some(v) = self.cat_pending(&g) {
            deps.cats.push((g.clone(), v));
        }
        if !self.ensure_full(&g, c, sink, Kind::Wildcard, now)? {
            return Ok(PopStep::Taken);
        }
        self.attach_late(r, fr, &g, now)?;
        let mut walk = Walk {
            claims: Vec::new(),
            deps,
        };
        let budget = c.budget.max(1) as i64;
        let max_parts = width(c, &g);
        self.walk_group(fr, &g, c, now, budget, max_parts, &mut walk)?;
        Ok(PopStep::Walked(walk, Some(g), None))
    }

    /// The group's catalog version still owed to the log, if any.
    fn cat_pending(&self, g: &Arc<Group>) -> Option<u64> {
        let st = lock(&g.st);
        (st.cat_ver > st.cat_durable).then_some(st.cat_ver)
    }

    /// Make sure the group holds its whole queue; `false` when a load had to
    /// start (the command is held in the group and re-run when it ends).
    fn ensure_full(
        &self,
        g: &Arc<Group>,
        c: &PopCommand,
        sink: &mut Option<oneshot::Sender<Reply>>,
        kind: Kind,
        now: i64,
    ) -> Result<bool> {
        let start = {
            let mut st = lock(&g.st);
            match st.load {
                Load::Full => return Ok(true),
                Load::Loading => false,
                Load::Partial => {
                    st.load = Load::Loading;
                    true
                }
            }
        };
        let Some(tx) = sink.take() else {
            return Ok(true);
        };
        let cmd = match kind {
            Kind::Wildcard => Command::PopWildcard(c.clone()),
            Kind::Pinned => Command::PopPinned(c.clone()),
            Kind::Discover => Command::PopDiscover(c.clone()),
        };
        {
            let mut st = lock(&g.st);
            if st.load == Load::Full {
                // Finished in between: run it here after all.
                drop(st);
                *sink = Some(tx);
                return Ok(true);
            }
            st.held.push(Held {
                cmd,
                sink: tx,
                at_us: now,
            });
        }
        if start {
            self.spawn_load(g.clone());
        }
        Ok(false)
    }

    /// Claim the group's ready parts, shard by shard from a rotating start,
    /// sharing `budget` frames and `max_parts` claims.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn walk_group<R: Reads + ?Sized>(
        &self,
        fr: &Frames<'_, R>,
        g: &Arc<Group>,
        c: &PopCommand,
        now: i64,
        mut budget: i64,
        max_parts: usize,
        walk: &mut Walk,
    ) -> Result<()> {
        let cfg = g.cfg();
        let grace = self.grace();
        let worker: Arc<str> = Arc::from(c.worker.as_str());
        let req = ClaimReq {
            worker: &worker,
            lease_seconds: c.lease_seconds,
            auto_ack: c.auto_ack,
            conflate: c.conflate,
            skip_window: c.skip_window_debounce,
        };
        let start = g.rr.fetch_add(1, Ordering::Relaxed);
        let mut claimed = 0usize;
        for i in 0..SHARDS {
            if budget <= 0 || claimed >= max_parts {
                break;
            }
            let si = (start + i) % SHARDS;
            let mut sh = lock(&self.shards[si]);
            let sh = &mut *sh;
            let Some(gs) = sh.groups.get_mut(&g.id) else {
                continue;
            };
            let n = gs.ready.len();
            for _ in 0..n {
                if budget <= 0 || claimed >= max_parts {
                    break;
                }
                let gs = sh.groups.get_mut(&g.id).expect("present");
                let Some(pid) = gs.ready.pop_front() else {
                    break;
                };
                g.unready();
                if let Some(p) = gs.parts.get_mut(&pid) {
                    p.queued = false;
                } else {
                    continue;
                }
                self.claim_into(fr, sh, g.id, pid, &cfg, &req, &mut budget, now, grace, walk)?;
                if walk.claims.last().is_some_and(|cl| cl.pid == pid) {
                    claimed += 1;
                }
            }
        }
        Ok(())
    }

    /// One claim attempt on a part, recording the claim and its dependency,
    /// or re-arming the part for later.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn claim_into<R: Reads + ?Sized>(
        &self,
        fr: &Frames<'_, R>,
        sh: &mut Shard,
        gid: Gid,
        pid: Pid,
        cfg: &GroupCfg,
        req: &ClaimReq<'_>,
        budget: &mut i64,
        now: i64,
        grace: i64,
        walk: &mut Walk,
    ) -> Result<()> {
        let ver_before = sh
            .groups
            .get(&gid)
            .and_then(|gs| gs.parts.get(&pid))
            .map(|p| p.ver);
        match claim_one(fr, sh, gid, pid, cfg, req, *budget, now, grace)? {
            Claimed::Took(claim, taken) => {
                *budget -= taken;
                let Shard { groups, timers, .. } = &mut *sh;
                let part = groups
                    .get_mut(&gid)
                    .and_then(|gs| gs.parts.get_mut(&pid))
                    .expect("claimed");
                walk.deps.rows.push((pid, gid, part.ver));
                if let Some(l) = part.cur.lease.as_ref() {
                    walk.deps
                        .claims
                        .push((pid, gid, l.worker.clone(), l.batch_end));
                    if let Some(t) = part.cur.lease_until(grace) {
                        schedule(timers, part, t, gid, pid);
                    }
                } else {
                    // Auto-ack: more behind it stays ready.
                    arm(sh, gid, pid, now, grace);
                }
                walk.claims.push(claim);
            }
            Claimed::Hold(at) => {
                let Shard { groups, timers, .. } = &mut *sh;
                if let Some(p) = groups.get_mut(&gid).and_then(|gs| gs.parts.get_mut(&pid)) {
                    p.ready_at = at;
                    schedule(timers, p, at, gid, pid);
                }
            }
            Claimed::Nothing => {
                // A seal moved the cursor: its row is owed.
                if let Some(p) = sh.groups.get(&gid).and_then(|gs| gs.parts.get(&pid)) {
                    if Some(p.ver) != ver_before {
                        walk.deps.rows.push((pid, gid, p.ver));
                    }
                }
            }
        }
        Ok(())
    }

    /// A pinned pop of one named partition (`log_pop_specific_v1`). An unknown
    /// partition answers EMPTY and is never provisioned.
    fn pop_pinned<R: Reads + ?Sized>(
        &self,
        r: &R,
        fr: &Frames<'_, R>,
        c: &PopCommand,
        name: &str,
        now: i64,
        sink: &mut Option<oneshot::Sender<Reply>>,
    ) -> Result<PopStep> {
        let Some(g) = self.group_for(r, &c.tenant, &c.queue, &c.group)? else {
            return Ok(PopStep::Walked(Walk::default(), None, None));
        };
        // No such partition: empty, and not held (a pinned long poll parks
        // on its partition; the facade parks this one itself).
        let Some(pid) = r.pid_of(&c.tenant, &c.queue, name)? else {
            return Ok(PopStep::Walked(Walk::default(), None, None));
        };
        if r.is_garbage(pid)? {
            return Ok(PopStep::Walked(Walk::default(), None, None));
        }
        let mut deps = Deps::default();
        let registered = g.cfg().meta.is_some();
        let seed = if registered {
            Seed::Policy
        } else {
            Seed::Intent(&c.sub)
        };
        if !registered && c.conflate {
            // The CONFLATING registrar (004:226): only when the pinned
            // partition itself has no cursor yet (the outer first-contact
            // guard). It registers the group and bulk-seeds the queue.
            let held_row = {
                let sh = lock(&self.shards[shard_of(pid)]);
                sh.groups
                    .get(&g.id)
                    .and_then(|gs| gs.parts.get(&pid))
                    .map(|p| p.has_row)
            };
            let has_row = match held_row {
                Some(h) => h,
                None => r.has_cursor(pid, &c.group)?,
            };
            if !has_row {
                let meta = registration_meta(&c.namespace, &c.task, &c.sub, c.conflate, now);
                let v = self.register(&g, meta, None);
                deps.cats.push((g.clone(), v));
                lock(&g.st).bulk_pending = true;
            }
        } else if let Some(v) = self.cat_pending(&g) {
            deps.cats.push((g.clone(), v));
        }
        // The registrar's queue-wide bulk seed, once the whole queue is held.
        if lock(&g.st).bulk_pending {
            if !self.ensure_full(&g, c, sink, Kind::Pinned, now)? {
                return Ok(PopStep::Taken);
            }
            self.attach_late(r, fr, &g, now)?;
            let owed = std::mem::replace(&mut lock(&g.st).bulk_pending, false);
            if owed {
                self.bulk_seed(&g, &mut deps);
            }
        }
        if !self.ensure_part(r, fr, &g, pid, seed, now)? {
            return Ok(PopStep::Walked(
                Walk {
                    claims: Vec::new(),
                    deps,
                },
                None,
                None,
            ));
        }
        let cfg = g.cfg();
        let grace = self.grace();
        let worker: Arc<str> = Arc::from(c.worker.as_str());
        let req = ClaimReq {
            worker: &worker,
            lease_seconds: c.lease_seconds,
            auto_ack: c.auto_ack,
            conflate: c.conflate,
            skip_window: c.skip_window_debounce,
        };
        let mut walk = Walk {
            claims: Vec::new(),
            deps,
        };
        let mut budget = c.budget.max(1) as i64;
        {
            let mut sh = lock(&self.shards[shard_of(pid)]);
            self.claim_into(
                fr,
                &mut sh,
                g.id,
                pid,
                &cfg,
                &req,
                &mut budget,
                now,
                grace,
                &mut walk,
            )?;
        }
        Ok(PopStep::Walked(walk, Some(g), Some(pid)))
    }

    /// The registrar's queue-wide bulk seed (004:243-296): every part without
    /// a row gets one at its seed.
    fn bulk_seed(&self, g: &Arc<Group>, deps: &mut Deps) {
        for s in self.shards.iter() {
            let mut sh = lock(s);
            let sh = &mut *sh;
            let Some(gs) = sh.groups.get_mut(&g.id) else {
                continue;
            };
            let pids: Vec<Pid> = gs
                .parts
                .iter()
                .filter(|(_, p)| !p.has_row && p.seed_ts.is_none())
                .map(|(pid, _)| *pid)
                .collect();
            for pid in pids {
                let p = gs.parts.get_mut(&pid).expect("listed");
                let v = touch(p, &mut sh.dirty, pid, g.id);
                deps.rows.push((pid, g.id, v));
            }
        }
    }

    /// A discovery pop across every queue of a namespace/task
    /// (`log_pop_discover_*_v1`): the wildcard walk over each matching queue,
    /// sharing the pop's budget and `max_parts`.
    fn pop_discover<R: Reads + ?Sized>(
        &self,
        r: &R,
        fr: &Frames<'_, R>,
        c: &PopCommand,
        now: i64,
        sink: &mut Option<oneshot::Sender<Reply>>,
    ) -> Result<PopStep> {
        let (ns, task) = (&c.namespace, &c.task);
        let mut queues: Vec<String> = Vec::new();
        r.scan_queues(&c.tenant, usize::MAX, &mut |name, cfg| {
            let ns_ok = ns.is_empty() || cfg.namespace.as_deref() == Some(ns.as_str());
            let task_ok = task.is_empty() || cfg.task.as_deref() == Some(task.as_str());
            if ns_ok && task_ok {
                queues.push(name.to_string());
            }
            true
        })?;
        // Every matching queue's group must be whole before the walk.
        let mut groups: Vec<Arc<Group>> = Vec::with_capacity(queues.len());
        let mut deps = Deps::default();
        for q in &queues {
            let Some(g) = self.group_for(r, &c.tenant, q, &c.group)? else {
                continue;
            };
            if g.cfg().meta.is_none() {
                let meta = registration_meta(&c.namespace, &c.task, &c.sub, c.conflate, now);
                let v = self.register(&g, meta, None);
                deps.cats.push((g.clone(), v));
            } else if let Some(v) = self.cat_pending(&g) {
                deps.cats.push((g.clone(), v));
            }
            groups.push(g);
        }
        for g in &groups {
            if !self.ensure_full(g, c, sink, Kind::Discover, now)? {
                return Ok(PopStep::Taken);
            }
        }
        let mut walk = Walk {
            claims: Vec::new(),
            deps,
        };
        let mut budget = c.budget.max(1) as i64;
        let mut max_parts = match c.max_parts {
            MAX_PARTS_AUTO => groups.iter().map(|g| width(c, g)).max().unwrap_or(1),
            _ => groups.first().map_or(usize::MAX, |g| width(c, g)),
        };
        for g in &groups {
            if budget <= 0 || max_parts == 0 {
                break;
            }
            self.attach_late(r, fr, g, now)?;
            let before = walk.claims.len();
            self.walk_group(fr, g, c, now, budget, max_parts, &mut walk)?;
            for cl in &walk.claims[before..] {
                budget -= (cl.end_offset as i64 - cl.start_offset as i64 + 1).max(0);
                max_parts = max_parts.saturating_sub(1);
            }
        }
        Ok(PopStep::Walked(walk, None, None))
    }

    /// Serve one parked waiter again: the waiter back when it found nothing
    /// (it parks again), `None` when it was answered.
    pub(crate) fn retry_waiter(&self, g: &Arc<Group>, w: Waiter, now: i64) -> Option<Waiter> {
        // One incarnation from the claim to its answer ([`Engine::serving`]).
        let _serving = super::state::read(&self.serving);
        if w.sink.is_closed() {
            return None;
        }
        if expired(&w.cmd, now, self.k.margin_us) {
            let _ = w.sink.send(Engine::empty_pop());
            return None;
        }
        // Not leading any more, or draining to hand the leadership over: a
        // parked pop claims nothing here (a claim answered at the hand-off
        // would be retried elsewhere, its lease left to expire); it never
        // ran, so the next leader runs it.
        if !self.leader.load(std::sync::atomic::Ordering::Acquire)
            || self.now_us()
                < self
                    .drain_until_us
                    .load(std::sync::atomic::Ordering::Acquire)
        {
            let _ = w
                .sink
                .send(crate::rsm::batcher::Reply::Retry { hint: None });
            return None;
        }
        let seg = self.seg_source();
        let res = self.store.read(|r| {
            let fr = self.frames(r, &seg);
            let mut walk = Walk::default();
            if let Some(pid) = w.pinned {
                let cfg = g.cfg();
                let worker: Arc<str> = Arc::from(w.cmd.worker.as_str());
                let req = ClaimReq {
                    worker: &worker,
                    lease_seconds: w.cmd.lease_seconds,
                    auto_ack: w.cmd.auto_ack,
                    conflate: w.cmd.conflate,
                    skip_window: w.cmd.skip_window_debounce,
                };
                let mut budget = w.cmd.budget.max(1) as i64;
                let mut sh = lock(&self.shards[shard_of(pid)]);
                self.claim_into(
                    &fr,
                    &mut sh,
                    g.id,
                    pid,
                    &cfg,
                    &req,
                    &mut budget,
                    now,
                    self.grace(),
                    &mut walk,
                )?;
            } else {
                self.attach_late(r, &fr, g, now)?;
                let max_parts = width(&w.cmd, g);
                self.walk_group(
                    &fr,
                    g,
                    &w.cmd,
                    now,
                    w.cmd.budget.max(1) as i64,
                    max_parts,
                    &mut walk,
                )?;
            }
            Ok(walk)
        });
        let walk = match res {
            Ok(w) => w,
            Err(e) => {
                let _ = w.sink.send(Reply::Refused(Refusal::from_store(e)));
                return None;
            }
        };
        if walk.claims.is_empty() && walk.deps.is_empty() {
            return Some(w);
        }
        let reply = Reply::Done {
            outcome: Outcome::Pop(PopOutcome {
                claims: walk.claims,
            }),
            at: None,
        };
        let mut sink = Some(w.sink);
        if let Some(reply) = self.answer(reply, walk.deps.clone(), &mut sink) {
            if let Some(tx) = sink.take() {
                if let Err(reply) = tx.send(reply) {
                    // Nobody receives the claims: hand the leases back.
                    self.release_claims(&walk.deps.claims, &reply);
                }
            }
        }
        None
    }
}

/// What a pop's store-read step decided.
pub(crate) enum PopStep {
    /// The command was held (a load runs): the sink is taken.
    Taken,
    Refuse(Refusal),
    /// The walk ran: its claims, the group a long poll parks on, the pinned
    /// partition.
    Walked(Walk, Option<Arc<Group>>, Option<Pid>),
}
