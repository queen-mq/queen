//! The apply hooks (every node; only the leader's engine holds anything).
//!
//! * [`Engine::hook_append`]: a partition's tail moved — the groups holding it
//!   arm it, and their parked pops wake; a group whose part the unloader
//!   dropped gets it loaded back ([`super::unload`]).
//! * [`Engine::hook_effect`]: the catalog and cursor effects apply wrote —
//!   a created partition joins the groups that hold its whole queue, a delete
//!   (partition, garbage, queue, tenant, group) drops what it names, a queue
//!   or group upsert refreshes the config a claim reads, a watermark moves the
//!   retention floor, and a cursor row the engine did not write (an admin
//!   write the planner logged) is adopted, so no later checkpoint undoes it.
//!
//! Nothing here reads the store: apply may hold the writer.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use crate::rsm::effect::{Effect, GarbageScope, Pid};

use super::state::{lock, read, shard_of, write, Gid, Group, GroupCfg, Load};
use super::Engine;

/// Apply's time in the leader engine's hooks (diagnostics): calls, time in
/// them, the longest, time waking groups and appends queued for the serve
/// thread (their shard was busy), logged every 10 s from the apply thread.
struct HookStats {
    calls: AtomicU64,
    busy_ns: AtomicU64,
    max_ns: AtomicU64,
    wake_ns: AtomicU64,
    deferred: AtomicU64,
    last: std::sync::Mutex<Option<Instant>>,
}

static HOOKS: HookStats = HookStats {
    calls: AtomicU64::new(0),
    busy_ns: AtomicU64::new(0),
    max_ns: AtomicU64::new(0),
    wake_ns: AtomicU64::new(0),
    deferred: AtomicU64::new(0),
    last: std::sync::Mutex::new(None),
};

/// One hook call took `d`.
pub(crate) fn note_hook(d: Duration) {
    let ns = d.as_nanos() as u64;
    HOOKS.calls.fetch_add(1, Ordering::Relaxed);
    HOOKS.busy_ns.fetch_add(ns, Ordering::Relaxed);
    HOOKS.max_ns.fetch_max(ns, Ordering::Relaxed);
    let Ok(mut last) = HOOKS.last.try_lock() else {
        return;
    };
    let now = Instant::now();
    match *last {
        None => *last = Some(now),
        Some(t) if now.duration_since(t) >= Duration::from_secs(10) => {
            *last = Some(now);
            tracing::info!(
                target: "rsm",
                calls = HOOKS.calls.swap(0, Ordering::Relaxed),
                busy_us = HOOKS.busy_ns.swap(0, Ordering::Relaxed) / 1000,
                max_us = HOOKS.max_ns.swap(0, Ordering::Relaxed) / 1000,
                wake_us = HOOKS.wake_ns.swap(0, Ordering::Relaxed) / 1000,
                deferred = HOOKS.deferred.swap(0, Ordering::Relaxed),
                "engine apply hooks (since the last line)"
            );
        }
        Some(_) => {}
    }
}

impl Engine {
    /// A partition's tail moved (apply). Apply never waits for the engine:
    /// when the partition's shard is busy (a pop claiming, a checkpoint being
    /// taken) the append is queued for the serve thread, which applies it
    /// under the lock at once ([`Engine::drain_appends`]). Waiting there cost
    /// the leader's apply 10-15% of its time at 10k queues (2026-09-30), and
    /// apply is the one thread every answer waits behind.
    pub(crate) fn hook_append(&self, pid: Pid, last: i64) {
        if !self.leader.load(Ordering::Acquire) {
            return;
        }
        let si = shard_of(pid);
        let mut sh = match self.shards[si].try_lock() {
            Ok(sh) => sh,
            Err(std::sync::TryLockError::Poisoned(p)) => p.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                lock(&self.appends[si]).push((pid, last));
                HOOKS.deferred.fetch_add(1, Ordering::Relaxed);
                if !self.appends_pending.swap(true, Ordering::AcqRel) {
                    self.nudge();
                }
                return;
            }
        };
        let mut woken: Vec<Gid> = Vec::new();
        let mut reload: Vec<(Arc<Group>, Pid)> = Vec::new();
        self.append_locked(
            &mut sh,
            pid,
            last,
            self.now_us(),
            self.grace(),
            &mut woken,
            &mut reload,
        );
        drop(sh);
        let t_wake = Instant::now();
        if !reload.is_empty() {
            self.reload_parts(reload);
        }
        for gid in woken {
            self.wake_group(gid);
        }
        HOOKS
            .wake_ns
            .fetch_add(t_wake.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    /// Apply the appends queued while their shards were busy: the serve
    /// thread, every pass it finds some.
    pub(crate) fn drain_appends(&self) {
        if !self.appends_pending.swap(false, Ordering::AcqRel) {
            return;
        }
        let now = self.now_us();
        let grace = self.grace();
        let mut woken: Vec<Gid> = Vec::new();
        let mut reload: Vec<(Arc<Group>, Pid)> = Vec::new();
        for si in 0..self.appends.len() {
            let queued = std::mem::take(&mut *lock(&self.appends[si]));
            if queued.is_empty() {
                continue;
            }
            let mut sh = lock(&self.shards[si]);
            for (pid, last) in queued {
                self.append_locked(&mut sh, pid, last, now, grace, &mut woken, &mut reload);
            }
        }
        if !reload.is_empty() {
            self.reload_parts(reload);
        }
        woken.sort_unstable();
        woken.dedup();
        for gid in woken {
            self.wake_group(gid);
        }
    }

    /// One append under its shard's lock: the tail, and the groups that hold
    /// the partition arm it (`woken`: those whose parked pops may now claim;
    /// `reload`: those whose part the unloader dropped, loaded back once the
    /// shard is released, [`Engine::reload_parts`]). Also a part's load that
    /// finds the tail past what the watchers saw ([`Engine::ensure_part`]).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn append_locked(
        &self,
        sh: &mut super::state::Shard,
        pid: Pid,
        last: i64,
        now: i64,
        grace: i64,
        woken: &mut Vec<Gid>,
        reload: &mut Vec<(Arc<Group>, Pid)>,
    ) {
        let Some(pi) = sh.pids.get_mut(&pid) else {
            return;
        };
        if last <= pi.tail {
            return;
        }
        let idle_before = pi.tail;
        pi.tail = last;
        pi.last_append_us = now;
        let watchers = pi.watchers.clone();
        for gid in watchers {
            match sh.groups.get(&gid) {
                None => continue,
                // Dropped while idle ([`super::unload`]): its group still
                // follows the partition.
                Some(gs) if !gs.parts.contains_key(&pid) => {
                    reload.push((gs.g.clone(), pid));
                    continue;
                }
                Some(_) => {}
            }
            // A delayed or windowed queue holds a partition that had no
            // work until the new frames are old / quiet enough.
            let hold = sh.groups.get(&gid).and_then(|gs| {
                let q = &gs.g.cfg().queue;
                let p = gs.parts.get(&pid)?;
                if p.queued || p.cur.committed < idle_before {
                    return None;
                }
                let d = q.delayed_processing.max(0) as i64 * super::SEC_US;
                let w = q.window_buffer.max(0) as i64 * super::SEC_US;
                (d > 0 || w > 0).then_some(now + d.max(w))
            });
            if let Some(at) = hold {
                if let Some(p) = sh
                    .groups
                    .get_mut(&gid)
                    .and_then(|gs| gs.parts.get_mut(&pid))
                {
                    p.ready_at = p.ready_at.max(at);
                }
            }
            if super::pop::arm(sh, gid, pid, now, grace) {
                woken.push(gid);
            }
        }
    }

    pub(crate) fn hook_effect(&self, e: &Effect) {
        if !self.leader.load(Ordering::Acquire) {
            return;
        }
        match e {
            Effect::PartitionCreate {
                pid, tenant, queue, ..
            } => {
                let groups = read(&self.reg).of_queue(tenant, queue);
                for g in groups {
                    {
                        let mut st = lock(&g.st);
                        if st.load == Load::Partial {
                            continue;
                        }
                        st.late.push(*pid);
                    }
                    g.bump();
                    self.wake_group(g.id);
                }
            }
            Effect::PartitionDelete { pid } => self.drop_pid(*pid, None),
            Effect::GarbageAdd { pids, scope, .. } => match scope {
                GarbageScope::Group { group } => {
                    for pid in pids {
                        self.drop_pid(*pid, Some(group));
                    }
                }
                _ => {
                    for pid in pids {
                        self.drop_pid(*pid, None);
                    }
                }
            },
            Effect::DeleteChunk { pids, scope, .. } => match scope {
                GarbageScope::Group { group } => {
                    for pid in pids {
                        self.drop_pid(*pid, Some(group));
                    }
                }
                _ => {
                    for pid in pids {
                        self.drop_pid(*pid, None);
                    }
                }
            },
            Effect::QueueDelete { tenant, queue } => {
                let groups = read(&self.reg).of_queue(tenant, queue);
                for g in groups {
                    self.drop_group(&g);
                }
            }
            Effect::TenantPurge { tenant } => {
                let groups: Vec<Arc<Group>> = read(&self.reg)
                    .by_id
                    .values()
                    .filter(|g| g.tenant == *tenant)
                    .cloned()
                    .collect();
                for g in groups {
                    self.drop_group(&g);
                }
            }
            Effect::GroupDelete {
                tenant,
                queue,
                group,
            } => {
                let g = read(&self.reg).get(tenant, queue, group);
                if let Some(g) = g {
                    self.drop_group(&g);
                }
            }
            Effect::QueueUpsert { tenant, queue, cfg } => {
                for g in read(&self.reg).of_queue(tenant, queue) {
                    let old = g.cfg();
                    if old.queue != *cfg {
                        g.set_cfg(GroupCfg {
                            queue: cfg.clone(),
                            meta: old.meta.clone(),
                        });
                    }
                }
            }
            Effect::GroupUpsert {
                tenant,
                queue,
                group,
                meta,
            } => {
                let g = read(&self.reg).get(tenant, queue, group);
                if let Some(g) = g {
                    let old = g.cfg();
                    if old.meta.as_ref() != Some(meta) {
                        g.set_cfg(GroupCfg {
                            queue: old.queue.clone(),
                            meta: Some(meta.clone()),
                        });
                    }
                }
            }
            Effect::Watermark {
                pid,
                log_start,
                txns_start,
            } => {
                let mut sh = lock(&self.shards[shard_of(*pid)]);
                if let Some(pi) = sh.pids.get_mut(pid) {
                    pi.log_start = *log_start;
                    pi.txns_start = *txns_start;
                }
            }
            Effect::CursorSet { pid, group, row } => {
                let term = self.term_start_us.load(Ordering::Acquire);
                let now = self.now_us();
                let grace = self.grace();
                let mut woken = None;
                {
                    let mut sh = lock(&self.shards[shard_of(*pid)]);
                    let sh = &mut *sh;
                    let Some(gid) = sh.pids.get(pid).and_then(|pi| {
                        pi.watchers
                            .iter()
                            .copied()
                            .find(|gid| sh.groups.get(gid).is_some_and(|gs| gs.g.name == *group))
                    }) else {
                        return;
                    };
                    let Some(p) = sh.groups.get_mut(&gid).and_then(|gs| gs.parts.get_mut(pid))
                    else {
                        return;
                    };
                    // Ours (a checkpoint or a transaction in flight, or the
                    // state we hold already): nothing to adopt.
                    if p.in_flight() || p.reserved.is_some() || p.cur.row() == *row {
                        return;
                    }
                    let old_worker = p.cur.worker().map(str::to_string);
                    p.cur = super::state::Cur::from_row(row, term);
                    if let Some(l) = p.cur.lease.as_mut() {
                        // Timed by this node's clock through a foreign writer:
                        // no grace needed, but no delivered set either.
                        l.foreign = false;
                    }
                    p.has_row = true;
                    p.seed_ts = None;
                    p.ready_at = 0;
                    p.ver += 1;
                    p.durable_ver = p.ver;
                    let new_worker = p.cur.worker().map(str::to_string);
                    if let Some(w) = old_worker {
                        sh.unindex_lease(&w, *pid, gid);
                    }
                    if let Some(w) = new_worker {
                        let w: Arc<str> = Arc::from(w.as_str());
                        sh.index_lease(&w, *pid, gid);
                    }
                    if super::pop::arm(sh, gid, *pid, now, grace) {
                        woken = Some(gid);
                    }
                }
                if let Some(gid) = woken {
                    self.wake_group(gid);
                }
            }
            Effect::CursorDelete { pid, group } => {
                let mut guard = lock(&self.shards[shard_of(*pid)]);
                let sh = &mut *guard;
                let found = {
                    let Some(gid) = sh.pids.get(pid).and_then(|pi| {
                        pi.watchers
                            .iter()
                            .copied()
                            .find(|gid| sh.groups.get(gid).is_some_and(|gs| gs.g.name == *group))
                    }) else {
                        return;
                    };
                    let Some(gs) = sh.groups.get_mut(&gid) else {
                        return;
                    };
                    let ours = gs
                        .parts
                        .get(pid)
                        .is_some_and(|p| p.in_flight() || p.reserved.is_some() || !p.has_row);
                    if ours {
                        return;
                    }
                    // Somebody deleted the row (a group delete): the part
                    // goes; a whole-queue group takes the partition back as a
                    // first contact on its next pop.
                    let g = gs.g.clone();
                    (g, remove_part(sh, gid, *pid))
                };
                drop(guard);
                let (g, settled) = found;
                for a in settled {
                    self.settle(a, 1);
                }
                let mut st = lock(&g.st);
                if st.load != Load::Partial {
                    st.late.push(*pid);
                }
            }
            _ => {}
        }
    }

    /// Forget a partition (for every group, or only `group`'s part of it).
    pub(crate) fn drop_pid(&self, pid: Pid, group: Option<&str>) {
        let settled = {
            let mut sh = lock(&self.shards[shard_of(pid)]);
            let sh = &mut *sh;
            let Some(pi) = sh.pids.get(&pid) else {
                return;
            };
            let gids: Vec<Gid> = pi
                .watchers
                .iter()
                .copied()
                .filter(|gid| {
                    group.is_none_or(|n| sh.groups.get(gid).is_some_and(|gs| gs.g.name == n))
                })
                .collect();
            let mut settled = Vec::new();
            for gid in gids {
                settled.extend(remove_part(sh, gid, pid));
            }
            if group.is_none() {
                sh.pids.remove(&pid);
            }
            settled
        };
        for a in settled {
            self.settle(a, 1);
        }
    }

    /// Forget a group: its parts, its waiters (answered empty), what it holds.
    pub(crate) fn drop_group(&self, g: &Arc<Group>) {
        g.dead.store(true, Ordering::Release);
        write(&self.reg).remove(g.id);
        let (waiters, held, cat_waiters) = {
            let mut st = lock(&g.st);
            st.dropped = true;
            (
                std::mem::take(&mut st.waiters),
                std::mem::take(&mut st.held),
                std::mem::take(&mut st.cat_waiters),
            )
        };
        g.waiting.store(0, Ordering::Release);
        for w in waiters {
            let _ = w.sink.send(Engine::empty_pop());
        }
        if !held.is_empty() {
            // Re-run against whatever takes the name next — on the serve
            // thread: this may be apply's.
            lock(&self.paused).extend(held);
            self.nudge();
        }
        let mut settled: Vec<u64> = cat_waiters.into_iter().map(|(a, _)| a).collect();
        for s in self.shards.iter() {
            let mut sh = lock(s);
            let sh = &mut *sh;
            let Some(gs) = sh.groups.remove(&g.id) else {
                continue;
            };
            for (pid, p) in gs.parts {
                settled.extend(p.waiters.iter().map(|(a, _)| *a));
                if let Some(w) = p.cur.worker() {
                    let w = w.to_string();
                    sh.unindex_lease(&w, pid, g.id);
                }
                if let Some(pi) = sh.pids.get_mut(&pid) {
                    pi.watchers.retain(|x| *x != g.id);
                    if pi.watchers.is_empty() {
                        sh.pids.remove(&pid);
                    }
                }
            }
        }
        for a in settled {
            self.settle(a, 1);
        }
        // Its due entries find no group and go.
    }
}

/// Remove one part from its shard: the answers that waited on it (settled:
/// nothing of it can become durable any more).
fn remove_part(sh: &mut super::state::Shard, gid: Gid, pid: Pid) -> Vec<u64> {
    let mut settled = Vec::new();
    if let Some(gs) = sh.groups.get_mut(&gid) {
        if let Some(p) = gs.parts.remove(&pid) {
            settled.extend(p.waiters.iter().map(|(a, _)| *a));
            if p.queued {
                gs.ready.retain(|x| *x != pid);
                gs.g.unready();
            }
            if let Some(w) = p.cur.worker() {
                let w = w.to_string();
                sh.unindex_lease(&w, pid, gid);
            }
        }
    }
    if let Some(pi) = sh.pids.get_mut(&pid) {
        pi.watchers.retain(|x| *x != gid);
    }
    settled
}
