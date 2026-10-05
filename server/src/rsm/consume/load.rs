//! Loading groups and their parts from COMMITTED state: the queue config, the
//! group row (its subscription), the partition heads and the cursor rows the
//! checkpoints wrote — leases included, which stay LIVE (a leased batch is not
//! redelivered before its expiry; one another node's clock timed gets the skew
//! grace). The delivered set of a live lease is rebuilt from the partition's
//! stored hashes. Nothing here reads the `pending` rings.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::rsm::effect::{Effect, GroupMeta, Pid, QueueConfig, SubscriptionMode};
use crate::rsm::planner::SubIntent;
use crate::rsm::store::{Reads, Store, StoreError, TypedReads};

use super::frames::Frames;
use super::state::{
    lock, read, shard_of, write, Cur, Group, GroupCfg, GroupShard, Load, Part, PidInfo,
};
use super::Engine;

type Result<T> = std::result::Result<T, StoreError>;

/// A group's stored policy built for a first-contact registration from a
/// pop-carried intent: `now`/`new` → `new` at the registration instant; an
/// explicit instant → `timestamp`; otherwise `all`.
pub(crate) fn registration_meta(
    namespace: &str,
    task: &str,
    intent: &SubIntent,
    conflation: bool,
    now_us: i64,
) -> GroupMeta {
    let (mode, ts) = if let Some(ts) = intent.from_us {
        (SubscriptionMode::Timestamp, ts)
    } else if intent.now || intent.mode == "new" {
        (SubscriptionMode::New, now_us)
    } else {
        (SubscriptionMode::All, i64::MIN)
    };
    GroupMeta {
        id: crate::util::uuidv7_bytes(),
        partition_name: String::new(),
        namespace: namespace.to_string(),
        task: task.to_string(),
        mode,
        subscription_timestamp_us: ts,
        conflation,
        seeded: false,
        registered_at_us: now_us,
    }
}

/// How a part's first contact seeds (no cursor row yet).
#[derive(Clone, Copy, Debug)]
pub(crate) enum Seed<'a> {
    /// The group's stored policy (or the floor when it has none).
    Policy,
    /// A pop-carried intent (a plain pinned pop of an unregistered group).
    Intent(&'a SubIntent),
}

impl Engine {
    pub(crate) fn frames<'a, R: Reads + ?Sized>(
        &self,
        r: &'a R,
        seg: &'a Option<super::frames::SegSource>,
    ) -> Frames<'a, R> {
        Frames {
            r,
            seg: seg.as_ref(),
            mode: self.mode(),
        }
    }

    /// The group the engine holds for `(tenant, queue, name)`, created from the
    /// committed queue config and group row on first contact. `None` when the
    /// queue does not exist.
    pub(crate) fn group_for<R: Reads + ?Sized>(
        &self,
        r: &R,
        tenant: &str,
        queue: &str,
        name: &str,
    ) -> Result<Option<Arc<Group>>> {
        if let Some(g) = read(&self.reg).get(tenant, queue, name) {
            return Ok(Some(g));
        }
        let Some(qcfg) = r.queue(tenant, queue)? else {
            return Ok(None);
        };
        let meta = r.group(tenant, queue, name)?.map(|g| g.meta);
        Ok(Some(self.intern(
            tenant,
            queue,
            name,
            GroupCfg { queue: qcfg, meta },
            Load::Partial,
        )))
    }

    /// Insert a group unless another thread did first.
    pub(crate) fn intern(
        &self,
        tenant: &str,
        queue: &str,
        name: &str,
        cfg: GroupCfg,
        load: Load,
    ) -> Arc<Group> {
        let mut reg = write(&self.reg);
        if let Some(g) = reg.get(tenant, queue, name) {
            return g;
        }
        reg.next = reg.next.wrapping_add(1);
        let g = Arc::new(Group::new(reg.next, tenant, queue, name, cfg, load));
        reg.insert(g.clone());
        g
    }

    /// Register the group (its first contact) with `meta`: the `GroupUpsert`
    /// (and the implicit `QueueUpsert` of a pop that created its queue) is owed
    /// to the next checkpoint. Returns the catalog version answers wait on.
    pub(crate) fn register(
        &self,
        g: &Arc<Group>,
        meta: GroupMeta,
        create: Option<QueueConfig>,
    ) -> u64 {
        let mut cfg = (*g.cfg()).clone();
        cfg.meta = Some(meta.clone());
        if let Some(q) = &create {
            cfg.queue = q.clone();
        }
        g.set_cfg(cfg);
        let ver = {
            let mut st = lock(&g.st);
            if let Some(q) = create {
                st.cat.push(Effect::QueueUpsert {
                    tenant: g.tenant.clone(),
                    queue: g.queue.clone(),
                    cfg: q,
                });
            }
            st.cat.push(Effect::GroupUpsert {
                tenant: g.tenant.clone(),
                queue: g.queue.clone(),
                group: g.name.clone(),
                meta,
            });
            st.cat_ver += 1;
            let first = !st.cat_dirty;
            st.cat_dirty = true;
            if first {
                lock(&self.cat_dirty).push(g.clone());
            }
            st.cat_ver
        };
        // The parts a plain pinned pop seeded from its intent, with nothing
        // delivered: the group's policy seeds them now (the planner's first
        // contact had left no cursor for them either) — a whole-queue group
        // takes them back on its next pop.
        let mut dropped: Vec<Pid> = Vec::new();
        for s in self.shards.iter() {
            let mut sh = lock(s);
            let sh = &mut *sh;
            if let Some(gs) = sh.groups.get_mut(&g.id) {
                let drop: Vec<Pid> = gs
                    .parts
                    .iter()
                    .filter(|(_, p)| !p.has_row && p.cur.lease.is_none() && p.reserved.is_none())
                    .map(|(pid, _)| *pid)
                    .collect();
                for pid in drop {
                    if gs.parts.remove(&pid).is_some_and(|p| p.queued) {
                        g.unready();
                    }
                    gs.ready.retain(|x| *x != pid);
                    if let Some(pi) = sh.pids.get_mut(&pid) {
                        pi.watchers.retain(|x| *x != g.id);
                    }
                    dropped.push(pid);
                }
            }
        }
        if !dropped.is_empty() {
            let mut st = lock(&g.st);
            if st.load != Load::Partial {
                st.late.extend(dropped);
            }
        }
        ver
    }

    /// Make sure the engine holds `(pid, group)`: read its partition head and
    /// cursor row and insert it. `false` when the partition is not one of the
    /// group's queue (or is gone). `seed` says how a first contact seeds.
    pub(crate) fn ensure_part<R: Reads + ?Sized>(
        &self,
        r: &R,
        fr: &Frames<'_, R>,
        g: &Arc<Group>,
        pid: Pid,
        seed: Seed<'_>,
        now_us: i64,
    ) -> Result<bool> {
        let si = shard_of(pid);
        {
            let sh = lock(&self.shards[si]);
            if sh
                .groups
                .get(&g.id)
                .is_some_and(|gs| gs.parts.contains_key(&pid))
            {
                return Ok(true);
            }
        }
        if r.is_garbage(pid)? {
            return Ok(false);
        }
        let Some(head) = r.partition_with(pid, |p| {
            (p.tenant == g.tenant && p.queue == g.queue).then_some(p.head)
        })?
        else {
            return Ok(false);
        };
        let Some(head) = head else {
            return Ok(false);
        };
        let foreign_before = self.term_start_us.load(Ordering::Acquire);
        let cfg = g.cfg();
        let (mut cur, has_row, seed_ts) = match r.cursor(pid, &g.name)? {
            Some(row) => (Cur::from_row(&row, foreign_before), true, None),
            None => {
                let (c, ts) = self.seed(
                    fr,
                    pid,
                    head.log_start,
                    head.last_offset,
                    &cfg,
                    seed,
                    now_us,
                )?;
                (Cur::fresh(c, now_us), false, ts)
            }
        };
        // A lease carried by the row: rebuild what the ack needs from the
        // stored hashes of the leased run.
        if let Some(l) = cur.lease.as_mut() {
            if !l.worker.is_empty() {
                if l.conflated {
                    if l.delivered.is_empty() {
                        l.delivered = fr
                            .hashes(pid, l.batch_end, l.batch_end)?
                            .into_iter()
                            .map(|(_, h)| h)
                            .collect();
                    }
                } else {
                    let lo = ((cur.committed + 1).max(head.log_start as i64)).max(0) as u64;
                    let hs = fr.hashes(pid, lo, l.batch_end)?;
                    if contiguous(&hs, lo, l.batch_end) {
                        l.frames_lo = lo;
                        l.frames = hs.iter().map(|(_, h)| *h).collect();
                    }
                    if l.delivered.is_empty() {
                        let mut seen = std::collections::HashSet::new();
                        l.delivered = hs
                            .iter()
                            .map(|(_, h)| *h)
                            .filter(|h| seen.insert(*h))
                            .collect();
                    }
                }
            }
        }
        let mut guard = lock(&self.shards[si]);
        let sh = &mut *guard;
        if g.dead.load(Ordering::Acquire) {
            return Ok(false);
        }
        if sh
            .groups
            .get(&g.id)
            .is_some_and(|gs| gs.parts.contains_key(&pid))
        {
            return Ok(true);
        }
        // The tail as it is NOW (under the shard lock an append's hook takes):
        // an append applied since the read above is in it, a later one finds
        // this part watching.
        let now_head = r.partition_head(pid)?.unwrap_or(head);
        let grace = self.grace();
        // That append's own hook may not have run yet (apply writes the row
        // first): the parts already watching wait for it, and it would find
        // the tail raised here and arm none of them. Arm them now, as it would
        // have (2026-10-05: a new partition's first message stuck for the
        // groups that loaded it a moment before another group did).
        let (mut woken, mut reload) = (Vec::new(), Vec::new());
        if sh
            .pids
            .get(&pid)
            .is_some_and(|pi| now_head.last_offset > pi.tail)
        {
            self.append_locked(
                sh,
                pid,
                now_head.last_offset,
                now_us,
                grace,
                &mut woken,
                &mut reload,
            );
            // This group's part is the one being loaded.
            reload.retain(|(rg, _)| rg.id != g.id);
        }
        let pi = sh.pids.entry(pid).or_insert_with(|| PidInfo {
            tail: now_head.last_offset,
            log_start: now_head.log_start,
            txns_start: now_head.txns_start,
            last_append_us: 0,
            watchers: Default::default(),
        });
        pi.tail = pi.tail.max(now_head.last_offset);
        pi.log_start = pi.log_start.max(now_head.log_start);
        pi.txns_start = pi.txns_start.max(now_head.txns_start);
        if !pi.watchers.contains(&g.id) {
            pi.watchers.push(g.id);
        }
        if let Some(l) = cur.lease.as_ref() {
            if !l.worker.is_empty() {
                sh.index_lease(&l.worker.clone(), pid, g.id);
            }
        }
        // (A lease-less leftover of a batch end is never a hold.)
        if cur.lease.as_ref().is_some_and(|l| l.worker.is_empty()) {
            cur.lease = None;
        }
        let mut part = Part::new(cur, has_row);
        part.seed_ts = seed_ts;
        sh.groups
            .entry(g.id)
            .or_insert_with(|| GroupShard::new(g.clone()))
            .parts
            .insert(pid, part);
        self.unload.loaded.fetch_add(1, Ordering::Relaxed);
        super::pop::arm(sh, g.id, pid, now_us, grace);
        drop(guard);
        if !reload.is_empty() {
            self.reload_parts(reload);
        }
        for gid in woken {
            self.wake_group(gid);
        }
        Ok(true)
    }

    /// The committed seed of a first contact and, when it is not stable yet
    /// (a subscription instant in the future), that instant.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn seed<R: Reads + ?Sized>(
        &self,
        fr: &Frames<'_, R>,
        pid: Pid,
        log_start: u64,
        tail: i64,
        cfg: &GroupCfg,
        seed: Seed<'_>,
        now_us: i64,
    ) -> Result<(i64, Option<i64>)> {
        let floor = log_start as i64 - 1;
        let ts = match (seed, &cfg.meta) {
            (Seed::Intent(intent), None) => {
                if let Some(ts) = intent.from_us {
                    Some(ts)
                } else if intent.now || intent.mode == "new" {
                    return Ok((tail, None));
                } else {
                    // No stored policy and no instant: the planner's
                    // `seed_from_intent(..).unwrap_or(-1)` (a partition whose
                    // frames retention took is then sealed by the claim).
                    return Ok((-1, None));
                }
            }
            _ => cfg.seed_instant(),
        };
        let Some(ts) = ts else {
            return Ok((floor.max(-1), None));
        };
        let s = fr.seed_from_ts(pid, log_start, tail, ts)?;
        let s = s.max(floor).max(-1);
        Ok((s, (ts > now_us).then_some(ts)))
    }

    /// Enumerate the whole queue for the group (its first wildcard contact on
    /// this leader): every partition becomes a part. Runs off the tokio
    /// workers, in chunks, holding the engine only while a chunk loads (a
    /// facade that drops its engine is not kept waiting on a big queue);
    /// the commands it held run when it ends.
    pub(crate) fn load_full_weak(weak: &std::sync::Weak<Engine>, g: &Arc<Group>, gen: u64) {
        const CHUNK: usize = 1024;
        let pids: std::result::Result<Vec<Pid>, StoreError> = match weak.upgrade() {
            Some(me) => me.store.read(|r| {
                let mut pids: Vec<Pid> = Vec::new();
                r.scan_queue_partitions(&g.tenant, &g.queue, None, usize::MAX, &mut |pid| {
                    pids.push(pid);
                    true
                })?;
                Ok(pids)
            }),
            None => return,
        };
        let res: std::result::Result<(), StoreError> = match pids {
            Err(e) => Err(e),
            Ok(pids) => {
                let mut res = Ok(());
                for chunk in pids.chunks(CHUNK) {
                    let Some(me) = weak.upgrade() else {
                        return;
                    };
                    if me.gen.load(Ordering::Acquire) != gen {
                        break;
                    }
                    let now = me.now_us();
                    let seg = me.seg_source();
                    let r = me.store.read(|r| {
                        let fr = me.frames(r, &seg);
                        for pid in chunk {
                            me.ensure_part(r, &fr, g, *pid, Seed::Policy, now)?;
                        }
                        Ok(())
                    });
                    if r.is_err() {
                        res = r;
                        break;
                    }
                }
                res
            }
        };
        let Some(me) = weak.upgrade() else {
            return;
        };
        if let Err(e) = &res {
            tracing::warn!(target: "rsm", error = %e, tenant = %g.tenant, queue = %g.queue,
                group = %g.name, "consume: group load failed; its commands are refused");
        }
        let held = {
            let mut st = lock(&g.st);
            st.load = if res.is_ok() {
                Load::Full
            } else {
                Load::Partial
            };
            std::mem::take(&mut st.held)
        };
        if me.gen.load(Ordering::Acquire) != gen {
            for h in held {
                let _ = h
                    .sink
                    .send(crate::rsm::batcher::Reply::Retry { hint: None });
            }
            return;
        }
        let now = me.now_us();
        for h in held {
            me.run_held(h, now);
        }
    }

    /// Attach the partitions created since the group's load (and the parts a
    /// row delete dropped): each becomes a part seeded by the group's policy.
    pub(crate) fn attach_late<R: Reads + ?Sized>(
        &self,
        r: &R,
        fr: &Frames<'_, R>,
        g: &Arc<Group>,
        now_us: i64,
    ) -> Result<()> {
        let late = {
            let mut st = lock(&g.st);
            if st.late.is_empty() || st.load != Load::Full {
                return Ok(());
            }
            std::mem::take(&mut st.late)
        };
        for pid in late {
            self.ensure_part(r, fr, g, pid, Seed::Policy, now_us)?;
        }
        Ok(())
    }

    /// Start a group's full load on a blocking thread (the command that needed
    /// it is held in the group until the load ends).
    pub(crate) fn spawn_load(&self, g: Arc<Group>) {
        let Some(me) = self.arc() else {
            return;
        };
        let gen = self.gen.load(Ordering::Acquire);
        self.loading.fetch_add(1, Ordering::AcqRel);
        // Weak: a load must not keep the engine (and its store) alive past
        // its facade.
        let weak = Arc::downgrade(&me);
        drop(me);
        let job = move || {
            Engine::load_full_weak(&weak, &g, gen);
            if let Some(me) = weak.upgrade() {
                me.loading.fetch_sub(1, Ordering::AcqRel);
            }
        };
        match tokio::runtime::Handle::try_current() {
            Ok(h) => {
                h.spawn_blocking(job);
            }
            Err(_) => {
                std::thread::Builder::new()
                    .name("consume-load".into())
                    .spawn(job)
                    .expect("spawn the consume load thread");
            }
        }
    }
}

/// Whether `(off, hash)` pairs cover `lo..=hi` without a gap.
pub(crate) fn contiguous(hs: &[(u64, [u8; 16])], lo: u64, hi: u64) -> bool {
    if hi < lo {
        return hs.is_empty();
    }
    hs.len() as u64 == hi - lo + 1 && hs.first().is_some_and(|(o, _)| *o == lo)
}
