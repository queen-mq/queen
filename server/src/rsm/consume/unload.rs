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

use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::rsm::effect::Pid;

use super::state::{lock, read, Gid, Group, Load, Part, PidInfo, Shard, SHARDS};
use super::{Engine, SEC_US};

/// The unloader looks at one shard this often (each shard every `SHARDS`).
const PERIOD_US: i64 = SEC_US;

/// At most one log line about it this often.
const LOG_EVERY_US: i64 = 60 * SEC_US;

/// The unloader's pacing and counters.
#[derive(Default)]
pub(crate) struct Unload {
    /// The next look (engine clock, µs).
    next_us: AtomicI64,
    /// The shard it takes.
    shard: AtomicUsize,
    /// Parts dropped and parts loaded (first contacts and loads back), since
    /// this engine was built.
    pub unloaded: AtomicU64,
    pub loaded: AtomicU64,
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
}

/// Whether nothing of the part lives only in memory or is due to happen in it:
/// its cursor row says everything a load back needs. (A timer still pending
/// is no reason to stay: without a lease and without a hold it is the expiry
/// of a lease already acked, and a timer finding no part does nothing.)
fn unloadable(p: &Part, pi: &PidInfo) -> bool {
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
        && pi.tail <= p.cur.committed
}

impl Engine {
    /// The clock work's turn: one shard, once a period.
    pub(crate) fn unload_idle(&self, now: i64) {
        if self.k.idle_unload_us <= 0 || now < self.unload.next_us.load(Ordering::Acquire) {
            return;
        }
        self.unload
            .next_us
            .store(now + PERIOD_US, Ordering::Release);
        let si = self.unload.shard.fetch_add(1, Ordering::AcqRel) % SHARDS;
        let n = self.unload_shards(&[si], now, false);
        if n == 0 {
            return;
        }
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

    /// Every shard at once, by the idle rule (tests).
    #[cfg(test)]
    pub(crate) fn unload_all(&self, now: i64) -> u64 {
        let all: Vec<usize> = (0..SHARDS).collect();
        self.unload_shards(&all, now, false)
    }

    /// Some shards, by the idle rule (tests: the cost of one pass).
    #[cfg(test)]
    pub(crate) fn unload_shards_for_test(&self, shards: &[usize], now: i64) -> u64 {
        self.unload_shards(shards, now, false)
    }

    /// Every part that could go, idle or not (tests: the suite run with
    /// `QUEEN_TEST_IDLE_UNLOAD_US` drops them before every command).
    #[cfg(test)]
    pub(crate) fn unload_now(&self, now: i64) -> u64 {
        let all: Vec<usize> = (0..SHARDS).collect();
        self.unload_shards(&all, now, true)
    }

    /// Drop the idle parts of whole-queue groups in `shards` (`force`: every
    /// part that could go, however recently it changed): how many went.
    fn unload_shards(&self, shards: &[usize], now: i64, force: bool) -> u64 {
        let idle = self.k.idle_unload_us;
        if idle <= 0 {
            return 0;
        }
        // A reset waits for this to end: no part of an incarnation that is
        // going is looked at after it went ([`Engine::serving`]).
        let _serving = read(&self.serving);
        if !self.leader.load(Ordering::Acquire) {
            return 0;
        }
        // The groups that may lose parts, decided before any shard is taken
        // (lock order: a group's `st`, then one shard). A group that stops
        // being one meanwhile only lost parts it could load back.
        let groups: Vec<Arc<Group>> = read(&self.reg).by_id.values().cloned().collect();
        let mut whole: HashSet<Gid> = HashSet::with_capacity(groups.len());
        for g in &groups {
            let st = lock(&g.st);
            if st.load == Load::Full && !st.dropped && !st.bulk_pending {
                whole.insert(g.id);
            }
        }
        if whole.is_empty() {
            return 0;
        }
        let mut dropped = 0u64;
        for &si in shards {
            let mut guard = lock(&self.shards[si]);
            let Shard { pids, groups, .. } = &mut *guard;
            for (gid, gs) in groups.iter_mut() {
                if !whole.contains(gid) || gs.g.dead.load(Ordering::Acquire) {
                    continue;
                }
                let before = gs.parts.len();
                gs.parts.retain(|pid, p| {
                    if force {
                        return pids.get(pid).is_none_or(|pi| !unloadable(p, pi));
                    }
                    keep(p, pids.get(pid), now, idle)
                });
                let gone = before - gs.parts.len();
                if gone > 0 {
                    dropped += gone as u64;
                    // The table keeps its size after removals: give it back.
                    if gs.parts.capacity() > (gs.parts.len() * 2).max(64) {
                        gs.parts.shrink_to_fit();
                    }
                }
            }
        }
        self.unload.unloaded.fetch_add(dropped, Ordering::AcqRel);
        dropped
    }

    /// What the leader holds for consumption now (the gauges).
    pub fn engine_stats(&self) -> EngineStats {
        let groups = read(&self.reg).by_id.len();
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
        }
    }

    /// Appends found watchers without a part (dropped while idle): each
    /// partition goes on its group's `late` list and the group is woken, so
    /// its next pop — a parked one first — loads the part back. Called with
    /// no shard held.
    pub(crate) fn reload_parts(&self, reload: Vec<(Arc<Group>, Pid)>) {
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
}

/// The unloader's look at one part: whether it stays.
fn keep(p: &mut Part, pi: Option<&PidInfo>, now: i64, idle: i64) -> bool {
    // Changed since the last look: idle from now on.
    if p.ver != p.seen_ver {
        p.seen_ver = p.ver;
        p.idle_since_us = now;
        return true;
    }
    let Some(pi) = pi else {
        return true;
    };
    let quiet_since = p.idle_since_us.max(pi.last_append_us);
    now.saturating_sub(quiet_since) < idle || !unloadable(p, pi)
}
