//! The committed view: what the planner reads.
//!
//! [`Committed`] reads the store's committed rows — through a read
//! transaction (planning) or through the apply thread's open write
//! transaction (an apply-time lookup) — typed by
//! [`crate::rsm::store::TypedReads`]. It is read-only by construction: the
//! planner cannot write committed state (I1); apply is the only mutator.
//!
//! There are no derived indexes here any more. The ready rings, visibility
//! deadlines and lease deadlines the planner once walked for a wildcard pop
//! (`pending`, `leases_by_worker`) went with the planning of consumption: the
//! consumption engine ([`crate::rsm::consume`]) holds every group's cursors,
//! leases and ready partitions in memory on the leader.

// I2, enforced rather than reviewed: `clippy.toml` lists the clock,
// environment and randomness calls this side of the line may not make,
// `[lints.clippy]` in Cargo.toml switches the lint off for the rest of the
// package (and every integration test), and this is where
// it is switched back on — for this module and every module under it.
#![deny(clippy::disallowed_methods)]

use crate::rsm::effect::{Pid, QueueConfig};
use crate::rsm::store::{Reads, Result, TypedReads};

// ---------------------------------------------------------------------------
// The committed view
// ---------------------------------------------------------------------------

/// What the planner reads: committed rows, and nothing it can change (I1).
///
/// It is generic over the handle so the same code runs over a read
/// transaction (planning) and over the apply thread's open write transaction
/// (an apply-time lookup), which is what keeps the two from drifting.
pub struct Committed<'a, R: Reads + ?Sized> {
    reads: &'a R,
}

impl<'a, R: Reads + ?Sized> Committed<'a, R> {
    pub fn new(reads: &'a R) -> Committed<'a, R> {
        Committed { reads }
    }

    /// The raw handle, for a read this view has no typed name for yet.
    pub fn reads(&self) -> &'a R {
        self.reads
    }

    // ------------------------------------------------------------ the clock

    /// `max(wall, last committed now + 1, max_created_at + 1)` — the planner's
    /// stamp (D5, §7.4, I5). The WALL CLOCK IS THE CALLER'S: nothing under
    /// `rsm/state/` reads a clock (I2), so the planner passes what it read.
    pub fn plan_now(&self, wall_us: i64) -> Result<i64> {
        let last = self.reads.last_now_us()?;
        let maxc = self.reads.max_created_at_us()?;
        Ok(wall_us
            .max(last.saturating_add(1))
            .max(maxc.saturating_add(1)))
    }

    // ----------------------------------------------------------- name lookups

    pub fn queue(&self, tenant: &str, queue: &str) -> Result<Option<QueueConfig>> {
        self.reads.queue(tenant, queue)
    }

    /// The pid of a partition NAME, `None` when it does not exist or when it
    /// is in the garbage set (§5.2 rules: readers and planners ignore garbage
    /// pids, and the name is reusable at once).
    pub fn pid_of(&self, tenant: &str, queue: &str, partition: &str) -> Result<Option<Pid>> {
        let Some(pid) = self.reads.pid_of(tenant, queue, partition)? else {
            return Ok(None);
        };
        // Whether a garbage row exists, without decoding it.
        if self.reads.is_garbage(pid)? {
            return Ok(None);
        }
        Ok(Some(pid))
    }

    /// The request-id window (D6, I6): the recorded outcome of a command that
    /// has already been logged, or `None`.
    pub fn recorded_outcome(&self, id: &[u8; 16]) -> Result<Option<Vec<u8>>> {
        Ok(self.reads.request_outcome(id)?.map(|r| r.outcome))
    }
}
