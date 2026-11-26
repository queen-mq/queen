//! Dropping the parts nobody needs in memory.
//!
//! The engine loads a whole-queue group's parts lazily — every partition of
//! the queue at the group's first wildcard contact, every partition created
//! since as it is created — and used to keep each one until its partition or
//! its group was deleted. A partition is deleted only once it has been empty
//! and idle for `PARTITION_CLEANUP_DAYS` (30 by default), so a workload that
//! keeps opening partitions (a chat per conversation, a key per entity) grew
//! the leader by every (group, partition) pair it ever touched: ~1.5 KB a pair
//! on 2.0.0, +130 MiB/h on the prod leader once smartchat moved (2026-10-05).
//! The 09-30 soak stayed flat only because its 1M partitions were all touched
//! in its first minutes.
//!
//! A part that holds nothing is a copy of its cursor row: no lease, nothing
//! to deliver (its cursor at the partition's tail), no hold, no timer, no
//! reservation, no dead letters, no answer waiting on it, every change durable.
//! Such a part, unchanged for `QUEEN_CONSUME_IDLE_UNLOAD_S`, is dropped, and its
//! partition keeps the group among its watchers. The next append there finds
//! the watcher without a part and puts the partition on the group's `late`
//! list — the path a partition created after the group's load already takes —
//! then wakes the group: its next pop, a parked one included, loads the part
//! back from the committed cursor row ([`Engine::ensure_part`]) as a new leader
//! would. Nothing of a dropped part lived only in memory, so nothing changes
//! but where it is read from.
//!
//! Only whole-queue groups ([`Load::Full`]) lose parts. A partial group's parts
//! are the partitions single-partition commands named, and no append would
//! bring one back.
//!
//! # When a part goes: its own entry in the shard's idle wheel (2026-11-25)
//!
//! The moment a part is found holding nothing — [`super::pop::arm`] finds it
//! caught up, or a checkpoint or a transaction makes its last change durable
//! — [`arm_idle`] notes that version and when, and puts the part in its
//! shard's [`IdleWheel`], due a window later. When it is due, the clock work
//! ([`Engine::run_timers`](super::Engine)) drops the part if it is still at
//! that version, still holds nothing and its partition has had no append for
//! a window ([`idle_due`]); a part that changed meanwhile is armed again the
//! next time it holds nothing. A group that becomes whole-queue arms every
//! part it already holds ([`Engine::arm_idle_group`]): its load ran while it
//! was not.
//!
//! The work is one wheel entry per part that goes quiet, whatever the number
//! of parts, groups or partitions ever created, and a part goes at the first
//! tick past its window. Until this date a pass walked each shard's partition
//! ids from the lowest it held to the highest it ever saw, one budget step per
//! group for every id, 1,024 steps every 5 ms, and dropped a part on a second
//! look a window after the first. Ids are never reused and that range never
//! narrowed, so every partition created since the oldest live one made every
//! pass longer: with 196 groups it walked about 1,000 ids a second, and idle
//! parts stayed for hours past their window (the prod leader held 117k parts
//! and dropped 3 to 45 a minute, 2026-11-24).
//!
//! Why a wheel and not the timer heap the leases use: the first version of
//! this (the same day) put the idle look in the part's heap timer. Every part
//! that went quiet then took two heap entries (the expiry of the lease its ack
//! had already ended, then its window), in heaps of hundreds of thousands per
//! shard at 40,000 new partitions a second, each push and pop a walk of cache
//! misses: the clock work took 2.5 times its old share of the leader, and the
//! consumers fell 7% behind once windows began to end (80,000 drops a
//! second). The wheel files an entry by the 10 ms slot it falls due in: a
//! push, and a walk of the oldest slot when due. And the look a checkpoint
//! arms reads only the part ([`holds_nothing`]): its partition, a lookup in a
//! table of every partition the shard holds, is read once, when the window
//! ends, not on the path every ack's answer waits for.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use crate::rsm::effect::Pid;

use super::state::{lock, Gid, Group, GroupShard, Load, Part, PidInfo, Shard};
use super::{Engine, SEC_US};

/// At most one log line about it this often.
const LOG_EVERY_US: i64 = 60 * SEC_US;

/// Parts [`Engine::arm_idle_group`] arms under one hold of a shard's lock.
const ARM_CHUNK: usize = 1024;

/// The width of an [`IdleWheel`] slot, in µs.
const IDLE_SLOT_US: i64 = 10_000;

/// The ends of a shard's idle windows: `(due, gid, pid)` filed by the
/// [`IDLE_SLOT_US`] slot it falls due in, oldest slot first. Windows end about
/// in the order they began, so a push lands in the newest slot or near it, and
/// the clock work takes the oldest ([`IdleWheel::take_due`]). An entry is a
/// look, not a promise: the part it names may have changed, gone, or come
/// back as another since ([`idle_due`] checks).
#[derive(Default)]
pub(crate) struct IdleWheel {
    slots: BTreeMap<i64, Vec<(i64, Gid, Pid)>>,
    len: usize,
}

impl IdleWheel {
    pub(crate) fn push(&mut self, due: i64, gid: Gid, pid: Pid) {
        self.slots
            .entry(due.div_euclid(IDLE_SLOT_US))
            .or_default()
            .push((due, gid, pid));
        self.len += 1;
    }

    /// Entries filed and not taken yet.
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Slots holding them (tests: an emptied slot is let go).
    #[cfg(test)]
    pub(crate) fn slots(&self) -> usize {
        self.slots.len()
    }

    /// Up to `budget` entries due at `now` into `out`: the slots wholly past
    /// first, then those of the slot `now` is in that are due.
    pub(crate) fn take_due(&mut self, now: i64, budget: usize, out: &mut Vec<(i64, Gid, Pid)>) {
        let current = now.div_euclid(IDLE_SLOT_US);
        while out.len() < budget {
            let Some(mut first) = self.slots.first_entry() else {
                break;
            };
            let slot = *first.key();
            if slot > current {
                break;
            }
            let v = first.get_mut();
            if slot < current {
                let n = (budget - out.len()).min(v.len());
                let from = v.len() - n;
                out.extend(v.drain(from..));
                self.len -= n;
            } else {
                let mut i = 0;
                while i < v.len() && out.len() < budget {
                    if v[i].0 <= now {
                        out.push(v.swap_remove(i));
                        self.len -= 1;
                    } else {
                        i += 1;
                    }
                }
            }
            if v.is_empty() {
                first.remove();
            } else {
                // The budget ran out, or the rest falls due later in `now`'s slot.
                break;
            }
        }
    }
}

/// The idle drop's counters.
#[derive(Default)]
pub(crate) struct Unload {
    /// Parts dropped and parts loaded (first contacts and loads back), since
    /// this engine was built.
    pub unloaded: AtomicU64,
    pub loaded: AtomicU64,
    /// Claims whose frames had no row any more and were read from the queue
    /// log with no shard lock held ([`super::pop::Claimed::Cold`]).
    pub cold_claims: AtomicU64,
    /// Parts dropped since the last log line, and when that was.
    since_log: AtomicU64,
    logged_us: AtomicI64,
}

/// The engine's size: what the leader holds in memory for consumption.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EngineStats {
    pub groups: usize,
    pub parts: usize,
    pub partitions: usize,
    pub unloaded_total: u64,
    pub loaded_total: u64,
    pub cold_claims_total: u64,
}

/// Whether nothing of the part lives only in memory or is due to happen in it,
/// as far as the part itself tells: no lease, no hold, no reservation, nothing
/// queued, waiting or owed, every change durable. (A heap timer still pending
/// is no reason to stay: without a lease and without a hold it is the expiry
/// of a lease already acked, and a timer finding no part does nothing.)
fn holds_nothing(p: &Part) -> bool {
    !p.queued
        && !p.dirty
        && !p.delete_row
        && p.cur.lease.is_none()
        && p.reserved.is_none()
        && p.ready_at == 0
        && p.seed_ts.is_none()
        && p.waiters.is_empty()
        && p.dlq.is_empty()
        && p.dlq_sent.is_empty()
        && p.durable_ver == p.ver
        && p.sent_ver <= p.durable_ver
        && p.doubt_ver <= p.durable_ver
}

/// [`holds_nothing`], and nothing to deliver: its cursor at the partition's
/// tail. Its cursor row then says everything a load back needs.
fn unloadable(p: &Part, pi: &PidInfo) -> bool {
    holds_nothing(p) && pi.tail <= p.cur.committed
}

/// A part found holding nothing at `now`: if it may be dropped at all (an idle
/// window, a whole-queue group), note its version and since when, and file it
/// in `wheel` for the end of the window. `pi`, its partition, when the caller
/// has it at hand: then a part with something to deliver is left too, and the
/// window ends a window after the partition's last append if that came later.
/// Without it (a checkpoint arms every part an ack changed, on the path the
/// ack's answer waits for) the part alone is read, and the look at the
/// window's end reads the partition. A part already filed no later is left as
/// it is: that look files it again if its window moved. A part that holds
/// something is left alone: it is armed the next time it holds nothing.
#[allow(clippy::too_many_arguments)]
pub(crate) fn arm_idle(
    wheel: &mut IdleWheel,
    p: &mut Part,
    pi: Option<&PidInfo>,
    g: &Group,
    idle_us: i64,
    now: i64,
    gid: Gid,
    pid: Pid,
) {
    if idle_us <= 0
        || !g.whole.load(Ordering::Acquire)
        || g.dead.load(Ordering::Acquire)
        || !holds_nothing(p)
        || pi.is_some_and(|pi| pi.tail > p.cur.committed)
    {
        return;
    }
    if p.idle_ver != p.ver {
        p.idle_ver = p.ver;
        p.idle_since_us = now;
    }
    let last_append = pi.map_or(0, |pi| pi.last_append_us);
    let due = p.idle_since_us.max(last_append).saturating_add(idle_us);
    if p.idle_at != 0 && p.idle_at <= due {
        return;
    }
    p.idle_at = due;
    wheel.push(due, gid, pid);
}

/// [`arm_idle`] for the part `(gid, pid)` of `sh`, if it holds one, its
/// partition read (a load, the tests).
pub(crate) fn arm_idle_in(sh: &mut Shard, gid: Gid, pid: Pid, now: i64) {
    let Shard {
        pids,
        groups,
        idle,
        idle_us,
        ..
    } = sh;
    let Some(gs) = groups.get_mut(&gid) else {
        return;
    };
    let GroupShard { g, parts, .. } = gs;
    if let Some(p) = parts.get_mut(&pid) {
        arm_idle(idle, p, pids.get(&pid), g, *idle_us, now, gid, pid);
    }
}

/// Drop the part `(gid, pid)` if it is still at the version it held nothing
/// at, holds nothing, has nothing to deliver, and it and its partition have
/// been quiet for the whole window. `true` when it went.
pub(crate) fn drop_if_idle(sh: &mut Shard, gid: Gid, pid: Pid, now: i64) -> bool {
    let idle = sh.idle_us;
    if idle <= 0 {
        return false;
    }
    let Shard { pids, groups, .. } = sh;
    let (Some(pi), Some(gs)) = (pids.get(&pid), groups.get_mut(&gid)) else {
        return false;
    };
    if !gs.g.whole.load(Ordering::Acquire) || gs.g.dead.load(Ordering::Acquire) {
        return false;
    }
    let Some(p) = gs.parts.get(&pid) else {
        return false;
    };
    if p.idle_ver != p.ver || !unloadable(p, pi) {
        return false;
    }
    if now.saturating_sub(p.idle_since_us.max(pi.last_append_us)) < idle {
        return false;
    }
    gs.parts.remove(&pid);
    true
}

/// The clock work's look when the wheel entry `(due, gid, pid)` is due, the
/// part and its partition looked up once. An entry the part no longer waits
/// on (it was filed again for another end, or went and came back as another
/// part) does nothing. Else the part goes if [`drop_if_idle`]'s rule says so;
/// one still holding nothing whose window moved (it changed and went quiet
/// again, or its partition got an append) is filed for the new end. `true`
/// when it went.
pub(crate) fn idle_due(sh: &mut Shard, due: i64, gid: Gid, pid: Pid, now: i64) -> bool {
    let Shard {
        pids,
        groups,
        idle,
        idle_us,
        ..
    } = sh;
    let Some(gs) = groups.get_mut(&gid) else {
        return false;
    };
    let GroupShard { g, parts, .. } = gs;
    let Some(p) = parts.get_mut(&pid) else {
        return false;
    };
    if p.idle_at != due {
        return false;
    }
    p.idle_at = 0;
    let Some(pi) = pids.get(&pid) else {
        return false;
    };
    if *idle_us > 0
        && g.whole.load(Ordering::Acquire)
        && !g.dead.load(Ordering::Acquire)
        && p.idle_ver == p.ver
        && unloadable(p, pi)
        && now.saturating_sub(p.idle_since_us.max(pi.last_append_us)) >= *idle_us
    {
        parts.remove(&pid);
        return true;
    }
    arm_idle(idle, p, Some(pi), g, *idle_us, now, gid, pid);
    false
}

/// A table keeps its size after removals: one that lost parts gives it back
/// once it holds under a quarter of it, and keeps room for twice what it
/// holds. (Shrunk to fit at half, a table whose parts come and go — loaded by
/// an append, dropped a window later — was rebuilt over and over: grown when
/// the next appends came, shrunk when their parts went.)
pub(crate) fn give_back(gs: &mut GroupShard) {
    let len = gs.parts.len();
    if gs.parts.capacity() > (len * 4).max(64) {
        gs.parts.shrink_to(len * 2);
    }
}

impl Engine {
    /// Count the parts the clock work dropped; a log line at most once a
    /// minute. Called with no shard held.
    pub(crate) fn note_unloaded(&self, n: u64, now: i64) {
        if n == 0 {
            return;
        }
        self.unload.unloaded.fetch_add(n, Ordering::AcqRel);
        let since = self.unload.since_log.fetch_add(n, Ordering::AcqRel) + n;
        if now.saturating_sub(self.unload.logged_us.load(Ordering::Acquire)) >= LOG_EVERY_US {
            self.unload.logged_us.store(now, Ordering::Release);
            self.unload.since_log.store(0, Ordering::Release);
            let s = self.engine_stats();
            tracing::info!(target: "rsm", unloaded = since, parts = s.parts, partitions = s.partitions,
                groups = s.groups, idle_s = self.k.idle_unload_us / SEC_US,
                "consume: idle parts unloaded (loaded back on their next append)");
        }
    }

    /// A group that has just become whole-queue: arm the idle look of every
    /// part it holds. Its load ran while it was not whole, so none of them
    /// was armed. Per shard, the ids are copied out under the lock, then armed
    /// [`ARM_CHUNK`] at a time, the lock taken again for each chunk: a queue of
    /// millions of partitions never holds a shard for the whole walk. Call it
    /// with no shard held.
    pub(crate) fn arm_idle_group(&self, g: &Group, now: i64) {
        if self.k.idle_unload_us <= 0 {
            return;
        }
        for s in self.shards.iter() {
            let pids: Vec<Pid> = match lock(s).groups.get(&g.id) {
                Some(gs) => gs.parts.keys().copied().collect(),
                None => continue,
            };
            for chunk in pids.chunks(ARM_CHUNK) {
                let mut sh = lock(s);
                for pid in chunk {
                    arm_idle_in(&mut sh, g.id, *pid, now);
                }
            }
        }
    }

    /// What the leader holds for consumption now (the gauges).
    pub fn engine_stats(&self) -> EngineStats {
        let groups = super::state::read(&self.reg).by_id.len();
        let (mut parts, mut partitions) = (0, 0);
        for s in self.shards.iter() {
            let sh = lock(s);
            partitions += sh.pids.len();
            parts += sh.groups.values().map(|gs| gs.parts.len()).sum::<usize>();
        }
        EngineStats {
            groups,
            parts,
            partitions,
            unloaded_total: self.unload.unloaded.load(Ordering::Acquire),
            loaded_total: self.unload.loaded.load(Ordering::Acquire),
            cold_claims_total: self.unload.cold_claims.load(Ordering::Acquire),
        }
    }

    /// Appends found watchers without a part (dropped while idle): each
    /// partition goes on its group's `late` list and the group is woken, so
    /// its next pop — a parked one first — loads the part back. Called with
    /// no shard held.
    pub(crate) fn reload_parts(&self, reload: Vec<(std::sync::Arc<Group>, Pid)>) {
        let mut woken: Vec<Gid> = Vec::with_capacity(reload.len());
        for (g, pid) in reload {
            {
                let mut st = lock(&g.st);
                if st.dropped || st.load == Load::Partial {
                    continue;
                }
                if !st.late.contains(&pid) {
                    st.late.push(pid);
                }
            }
            g.bump();
            woken.push(g.id);
        }
        woken.sort_unstable();
        woken.dedup();
        for gid in woken {
            self.wake_group(gid);
        }
    }

    /// Every part that has been idle for the window at `now`, in every shard,
    /// by the rule its own timer applies (tests: the clock work without the
    /// clock). How many went.
    #[cfg(test)]
    pub(crate) fn unload_all(&self, now: i64) -> u64 {
        self.unload_where(now, false)
    }

    /// Every part that could go, idle or not (tests: the suite run with
    /// `QUEEN_TEST_IDLE_UNLOAD_US` drops them before every command).
    #[cfg(test)]
    pub(crate) fn unload_now(&self, now: i64) -> u64 {
        self.unload_where(now, true)
    }

    /// Every group's [`Group::whole`] says what its `st` says (tests: run
    /// before every command, so a change of `load`, `dropped` or
    /// `bulk_pending` that forgot [`Group::sync_whole`] fails the suite).
    #[cfg(test)]
    pub(crate) fn assert_whole_flags(&self) {
        let groups: Vec<std::sync::Arc<Group>> = super::state::read(&self.reg)
            .by_id
            .values()
            .cloned()
            .collect();
        for g in groups {
            let st = lock(&g.st);
            let want = st.load == Load::Full && !st.dropped && !st.bulk_pending;
            assert_eq!(
                g.whole.load(Ordering::Acquire),
                want,
                "group {}/{}/{}: whole flag vs load {:?}, dropped {}, bulk_pending {}",
                g.tenant,
                g.queue,
                g.name,
                st.load,
                st.dropped,
                st.bulk_pending
            );
        }
    }

    #[cfg(test)]
    fn unload_where(&self, now: i64, force: bool) -> u64 {
        if self.k.idle_unload_us <= 0 {
            return 0;
        }
        let _serving = super::state::read(&self.serving);
        if !self.leader.load(Ordering::Acquire) {
            return 0;
        }
        let mut dropped = 0u64;
        for s in self.shards.iter() {
            let mut sh = lock(s);
            let sh = &mut *sh;
            let pids: Vec<(Gid, Pid)> = sh
                .groups
                .iter()
                .flat_map(|(gid, gs)| gs.parts.keys().map(move |pid| (*gid, *pid)))
                .collect();
            for (gid, pid) in pids {
                if force {
                    let gone = {
                        let Shard { pids, groups, .. } = &mut *sh;
                        let (Some(pi), Some(gs)) = (pids.get(&pid), groups.get_mut(&gid)) else {
                            continue;
                        };
                        let whole = gs.g.whole.load(Ordering::Acquire)
                            && !gs.g.dead.load(Ordering::Acquire);
                        whole
                            && gs.parts.get(&pid).is_some_and(|p| unloadable(p, pi))
                            && gs.parts.remove(&pid).is_some()
                    };
                    dropped += u64::from(gone);
                } else {
                    // The look its timer would take now: armed first, as the
                    // moment it held nothing would have.
                    arm_idle_in(sh, gid, pid, now);
                    dropped += u64::from(drop_if_idle(sh, gid, pid, now));
                }
            }
            for gs in sh.groups.values_mut() {
                give_back(gs);
            }
        }
        self.unload.unloaded.fetch_add(dropped, Ordering::AcqRel);
        dropped
    }
}
