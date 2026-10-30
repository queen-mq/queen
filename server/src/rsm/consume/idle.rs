//! Idle claims (Streams): lease a partition the group has fully consumed,
//! with an EMPTY batch.
//!
//! A stream runner closes a window when time passes, not only when a message
//! arrives. On a quiet partition there is no message to pop, so before this
//! the close ran with no lease at all: a runner that restarted forgot its
//! partitions, and two runners could close the same window, or one could
//! delete a window the other had just extended (issue #153).
//!
//! An idle claim gives the close the lease every cycle already carries. It
//! takes a part only when nothing else could be working on it — no live
//! lease, no transaction holding it, no `retryDelay` hold — and when the group
//! has read everything there (`tail <= committed`): a partition with messages
//! waiting is not idle, its next pop delivers them and that cycle closes the
//! windows from the messages' own times. The lease's batch is empty
//! (`batch_end = committed`), so the cycle that closes the windows acks it
//! with `count: 0`: the cursor stays, the lease goes, and the engine refuses
//! the cycle whole when the lease is gone (`bad_lease`), exactly as for a
//! popped batch.
//!
//! The lease is the cursor row's ordinary lease (worker, batch end, expiry),
//! so nothing new reaches the log. The answer waits for that row's
//! checkpoint, like a pop's.

use tokio::sync::oneshot;

use crate::rsm::batcher::Reply;
use crate::rsm::effect::Pid;
use crate::rsm::entry::{Outcome, PopClaim, PopOutcome};
use crate::rsm::planner::Refusal;
use crate::rsm::store::{Store, TypedReads};

use super::checkpoint::Deps;
use super::load::Seed;
use super::state::{lock, read, shard_of, Lease};
use super::{Engine, Served, SEC_US};

/// What a runner asks: these partitions of one queue, for one group.
#[derive(Clone, Debug)]
pub struct IdleClaimCommand {
    pub tenant: String,
    pub queue: String,
    pub group: String,
    pub pids: Vec<Pid>,
    /// The lease id the claims get (the facade mints one per call).
    pub worker: String,
    pub lease_seconds: i32,
}

/// One partition's answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdleClaim {
    /// Leased with an empty batch until `expires_us`.
    Claimed { pid: Pid, expires_us: i64 },
    /// Someone works on it: a live lease, a transaction, a `retryDelay` hold.
    Busy { pid: Pid },
    /// The group has messages waiting there: its next pop delivers them.
    Backlog { pid: Pid },
    /// No such partition for this queue, or the group never read it.
    Missing { pid: Pid },
}

impl IdleClaim {
    pub fn pid(&self) -> Pid {
        match *self {
            IdleClaim::Claimed { pid, .. }
            | IdleClaim::Busy { pid }
            | IdleClaim::Backlog { pid }
            | IdleClaim::Missing { pid } => pid,
        }
    }
}

impl Engine {
    /// Claim every idle partition of `c.pids` (see the module header). The
    /// per-partition answers are known at once; the [`Served`] resolves when
    /// the claimed leases are durable (`Reply::Done`), or answers `Retry`
    /// when this node cannot serve (not leading, draining, a new leader's
    /// pause): then nothing was claimed.
    pub fn claim_idle(
        &self,
        c: &IdleClaimCommand,
        now: i64,
    ) -> Result<(Vec<IdleClaim>, Served), Refusal> {
        let retry = || Ok((Vec::new(), Served::Now(Reply::Retry { hint: None })));
        if let Some(r) = self.gate() {
            return Ok((Vec::new(), Served::Now(r)));
        }
        let _serving = read(&self.serving);
        if !self.leader.load(std::sync::atomic::Ordering::Acquire)
            || self.now_us()
                < self
                    .drain_until_us
                    .load(std::sync::atomic::Ordering::Acquire)
            || now
                < self
                    .serve_after_us
                    .load(std::sync::atomic::Ordering::Acquire)
        {
            return retry();
        }
        let seg = self.seg_source();
        let grace = self.grace();
        let worker: std::sync::Arc<str> = std::sync::Arc::from(c.worker.as_str());
        let lease_us = i64::from(c.lease_seconds.max(1)) * SEC_US;
        let res = self.store.read(|r| {
            let fr = self.frames(r, &seg);
            let mut out = Vec::with_capacity(c.pids.len());
            let mut deps = Deps::default();
            let mut claims = Vec::new();
            let Some(g) = self.group_for(r, &c.tenant, &c.queue, &c.group)? else {
                out.extend(c.pids.iter().map(|&pid| IdleClaim::Missing { pid }));
                return Ok((out, deps, claims));
            };
            for &pid in &c.pids {
                // The partition must be this queue's and alive.
                let mine = r
                    .partition(pid)?
                    .is_some_and(|p| p.tenant == c.tenant && p.queue == c.queue);
                if !mine
                    || r.is_garbage(pid)?
                    || !self.ensure_part(r, &fr, &g, pid, Seed::Policy, now)?
                {
                    out.push(IdleClaim::Missing { pid });
                    continue;
                }
                let mut sh = lock(&self.shards[shard_of(pid)]);
                let sh = &mut *sh;
                let Some(tail) = sh.pids.get(&pid).map(|pi| pi.tail) else {
                    out.push(IdleClaim::Missing { pid });
                    continue;
                };
                let Some(p) = sh
                    .groups
                    .get_mut(&g.id)
                    .and_then(|gs| gs.parts.get_mut(&pid))
                else {
                    out.push(IdleClaim::Missing { pid });
                    continue;
                };
                if p.reserved.is_some() || p.cur.leased(now, grace) || p.retry_at > now {
                    out.push(IdleClaim::Busy { pid });
                    continue;
                }
                // A group that never read here has no row, and a first
                // contact with a moving seed has no stable position: neither
                // holds windows to close.
                if !p.has_row || p.seed_ts.is_some() {
                    out.push(IdleClaim::Missing { pid });
                    continue;
                }
                if tail > p.cur.committed {
                    out.push(IdleClaim::Backlog { pid });
                    continue;
                }
                // Read nothing yet, on an empty partition.
                if p.cur.committed < 0 {
                    out.push(IdleClaim::Missing { pid });
                    continue;
                }
                let committed = p.cur.committed;
                let mut cur = p.cur.clone();
                cur.lease = Some(Lease {
                    worker: worker.clone(),
                    batch_end: committed as u64,
                    expires_us: now + lease_us,
                    acquired_us: Some(now),
                    conflated: false,
                    delivered: Vec::new(),
                    frames_lo: 0,
                    frames: Vec::new(),
                    foreign: false,
                });
                let Some(ver) = self.commit_cur(sh, g.id, pid, cur, Vec::new(), now) else {
                    out.push(IdleClaim::Missing { pid });
                    continue;
                };
                deps.rows.push((pid, g.id, ver));
                deps.claims
                    .push((pid, g.id, worker.clone(), committed as u64));
                claims.push(PopClaim {
                    pid,
                    start_offset: committed as u64 + 1,
                    end_offset: committed as u64,
                    worker: c.worker.clone(),
                    lease_expires_at_us: Some(now + lease_us),
                    delivery_attempt: 0,
                    conflated: false,
                });
                out.push(IdleClaim::Claimed {
                    pid,
                    expires_us: now + lease_us,
                });
            }
            Ok((out, deps, claims))
        });
        let (out, deps, claims) = res.map_err(Refusal::from_store)?;
        let reply = Reply::Done {
            outcome: Outcome::Pop(PopOutcome { claims }),
            at: None,
        };
        let (tx, rx) = oneshot::channel();
        let mut sink = Some(tx);
        let served = match self.answer(reply, deps, &mut sink) {
            Some(r) => Served::Now(r),
            None => Served::Later(rx),
        };
        Ok((out, served))
    }
}
