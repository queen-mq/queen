//! HOMES: where a key's state lives while its entries are in flight
//! (`PLAN_HOMES.md`).
//!
//! The overlay's partition state is split into [`PART_SHARDS`] maps. A
//! partition's shard is a hash of its tenant and its NAME, without its queue:
//! `orders-in/order-9` and `orders-out/order-9` share a shard, and so does the
//! KV key `order-9` ([`Homes::shard_of_key`]). A shard has one planning thread,
//! always the same ([`home_of_shard`]). So what one entity's step touches (the
//! partition it acks, the one it pushes to, its key) is planned by one thread,
//! with no other thread waiting for it: co-location.
//!
//! The overlay keys its partition state by pid, and a pid does not say its
//! name. This table does: `pid -> shard`, written once per pid and never
//! changed. A pid's shard is set from its name the first time the planner
//! resolves the name ([`Homes::note_name`], from `Planner::pid_of` and from
//! the numbering of a partition a cycle created). A pid nobody resolved by
//! name yet (a watermark, a cursor row of a partition this process never
//! pushed to) takes `pid % PART_SHARDS`, and KEEPS it: what matters for
//! correctness is that a pid's shard never changes, since its overlay rows sit
//! in that shard's map. The name only decides who shares the shard.
//!
//! Reads and writes are one atomic byte each: no lock, and any thread may ask.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};

use super::PART_SHARDS;
use crate::rsm::effect::Pid;
use crate::rsm::fasthash::FxHasher;

/// Not set yet.
const UNSET: u8 = u8::MAX;

/// Pids a chunk of the table holds.
const CHUNK: usize = 1 << 16;

/// Chunks the table can hold: 2^28 pids. A pid past them is not in the table
/// and takes `pid % PART_SHARDS` (a pure function, so it never changes either).
const CHUNKS: usize = 1 << 12;

const _: () = assert!(PART_SHARDS < UNSET as usize);

/// `pid -> shard`, see the module header.
pub(crate) struct Homes {
    chunks: Box<[OnceLock<Box<[AtomicU8]>>]>,
}

impl std::fmt::Debug for Homes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Homes")
    }
}

impl Homes {
    pub(crate) fn new() -> Arc<Homes> {
        Arc::new(Homes {
            chunks: (0..CHUNKS).map(|_| OnceLock::new()).collect(),
        })
    }

    fn slot(&self, pid: Pid) -> Option<&AtomicU8> {
        let chunk = self.chunks.get((pid / CHUNK as u64) as usize)?;
        let cells = chunk.get_or_init(|| (0..CHUNK).map(|_| AtomicU8::new(UNSET)).collect());
        cells.get((pid % CHUNK as u64) as usize)
    }

    /// The shard of a name: every partition called `partition` in `tenant`,
    /// whatever its queue.
    pub(crate) fn shard_of_name(tenant: &str, partition: &str) -> usize {
        use std::hash::Hasher;
        let mut h = FxHasher::default();
        h.write_usize(tenant.len());
        h.write(tenant.as_bytes());
        h.write(partition.as_bytes());
        // The high bits: Fx multiplies, so its low bits are its weakest.
        ((h.finish() >> 32) % PART_SHARDS as u64) as usize
    }

    /// The shard of a KV key or a timer key: the shard of the partition of the
    /// same name, so an entity's key sits with its partitions.
    pub(crate) fn shard_of_key(tenant: &str, key: &str) -> usize {
        Homes::shard_of_name(tenant, key)
    }

    /// The pid of `tenant`'s partition `partition` is `pid`: its shard from
    /// now on, unless it has one already. Returns the shard it has.
    pub(crate) fn note_name(&self, pid: Pid, tenant: &str, partition: &str) -> usize {
        let Some(cell) = self.slot(pid) else {
            return (pid % PART_SHARDS as u64) as usize;
        };
        let seen = cell.load(Ordering::Relaxed);
        if seen != UNSET {
            return seen as usize;
        }
        let want = Homes::shard_of_name(tenant, partition) as u8;
        match cell.compare_exchange(UNSET, want, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => want as usize,
            Err(other) => other as usize,
        }
    }

    /// The shard of `pid`. One nobody named yet takes `pid % PART_SHARDS` for
    /// good.
    pub(crate) fn shard_of(&self, pid: Pid) -> usize {
        let default = (pid % PART_SHARDS as u64) as u8;
        let Some(cell) = self.slot(pid) else {
            return default as usize;
        };
        let seen = cell.load(Ordering::Relaxed);
        if seen != UNSET {
            return seen as usize;
        }
        match cell.compare_exchange(UNSET, default, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => default as usize,
            Err(other) => other as usize,
        }
    }

    /// The shard of `pid` if it has one, without giving it one.
    pub(crate) fn peek(&self, pid: Pid) -> Option<usize> {
        let cell = self.slot(pid)?;
        match cell.load(Ordering::Relaxed) {
            UNSET => None,
            s => Some(s as usize),
        }
    }
}

/// The planning thread of a shard, of `threads`: always the same.
pub(crate) fn home_of_shard(shard: usize, threads: usize) -> usize {
    shard % threads.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_has_one_shard_whatever_its_queue_and_a_key_of_that_name_shares_it() {
        let a = Homes::shard_of_name("t", "order-9");
        assert_eq!(a, Homes::shard_of_name("t", "order-9"));
        assert_eq!(a, Homes::shard_of_key("t", "order-9"));
        assert!(a < PART_SHARDS);
        // Names spread over the shards.
        let mut seen = std::collections::HashSet::new();
        for i in 0..2000 {
            seen.insert(Homes::shard_of_name("t", &format!("order-{i}")));
        }
        assert_eq!(seen.len(), PART_SHARDS, "2000 names reach every shard");
        // The tenant counts: ("ab", "c") is not ("a", "bc").
        assert_ne!(
            (0..64)
                .map(|i| Homes::shard_of_name("ab", &format!("c{i}")))
                .collect::<Vec<_>>(),
            (0..64)
                .map(|i| Homes::shard_of_name("a", &format!("bc{i}")))
                .collect::<Vec<_>>(),
        );
    }

    #[test]
    fn a_pid_keeps_the_first_shard_it_got() {
        let h = Homes::new();
        // Named first: the name's shard, for good.
        let s = h.note_name(7, "t", "order-9");
        assert_eq!(s, Homes::shard_of_name("t", "order-9"));
        assert_eq!(h.shard_of(7), s);
        assert_eq!(h.note_name(7, "t", "another-name"), s);
        // Asked first: pid % shards, and a name that comes later does not move it.
        assert_eq!(h.peek(200), None);
        let d = h.shard_of(200);
        assert_eq!(d, 200 % PART_SHARDS);
        assert_eq!(h.note_name(200, "t", "order-9"), d);
        assert_eq!(h.peek(200), Some(d));
        // Past the table: the pure default, still stable.
        let far = (CHUNK * CHUNKS) as u64 + 5;
        assert_eq!(h.shard_of(far), (far % PART_SHARDS as u64) as usize);
        assert_eq!(
            h.note_name(far, "t", "x"),
            (far % PART_SHARDS as u64) as usize
        );
    }

    #[test]
    fn a_shard_has_one_home() {
        for threads in 1..=16 {
            for s in 0..PART_SHARDS {
                assert!(home_of_shard(s, threads) < threads);
                assert_eq!(home_of_shard(s, threads), home_of_shard(s, threads));
            }
        }
    }
}
