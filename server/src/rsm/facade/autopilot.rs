//! Pop autopilot on the raft engine: `autopilot=true` on a wildcard pop hands
//! the broker the dimensions the client left unset — the Postgres engine's wire
//! contract (`crate::pop_autopilot`): an explicit `partitions` or `batch` is the
//! client's and is never touched, and the choice is echoed in the body as
//! `"autopilot":{"partitions":W,"batch":B}`.
//!
//! The raft planner claims from exact state, so the inputs are exact too:
//!
//! - **W, the claim width**: the group's partitions ready NOW (the `pending`
//!   rows due, read from the store at the pop) divided among this lane's pops
//!   in flight on this node, clamped to `[1, 64]` (the checkout ceiling the
//!   Postgres engine measured). A pop's claim stops once its batch is full —
//!   the batch is the budget of the WHOLE pop — so over a backlog the first
//!   partition fills it and W is moot; W pays when partitions are sparse, where
//!   one pop and one ack collect a batch from several partitions instead of
//!   one pop and one ack per partition. Dividing by the live pops keeps one
//!   consumer from leasing every ready partition while the others idle.
//! - **B, the batch**: DRAIN-AWARE, the follow-up the Postgres controller named
//!   (`AUTO_BATCH_DEFAULT`'s note): the lane's measured drain rate — messages a
//!   consumer acknowledges per second of lease, from each lease's delivery to
//!   its ack — times a drain budget (`QUEEN_RAFT_AUTOPILOT_DRAIN_MS`, 200 ms),
//!   clamped to `[QUEEN_RAFT_AUTOPILOT_BATCH_MIN, …_MAX]` (100, 1000). A cold
//!   lane starts at the minimum, the SDKs' historical default. A fast consumer
//!   gets deep batches — fewer pops and acks per message, which is what the
//!   planner and the round trips cost (measured: batch 1000 consumed 111k msg/s
//!   where batch 100 did 79k) — and a slow one is never handed more than it
//!   retires within the budget, so the lease tail stays bounded.
//!
//! Node-local, in RAM, and outside planning: the choice becomes the
//! `PopCommand`'s `budget` and `max_parts`, so the plan is still a pure function
//! of the command (I2).

use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// The widest checkout (partitions per pop) any choice may make.
pub(crate) const W_CEILING: u32 = 64;

const SHARDS: usize = 32;
/// Outstanding leases kept per shard before the expired ones are swept.
const LEASE_SWEEP_AT: usize = 4096;
/// A lease not acknowledged within this long is forgotten (a crashed consumer,
/// an expired lease that redelivered): it gives no drain sample.
const LEASE_TTL: Duration = Duration::from_secs(300);
/// EWMA weight of one drain sample.
const ALPHA: f64 = 0.25;
/// How long a lane's ready count is reused: the count is a store scan, and a
/// lane with hundreds of consumers would otherwise scan per pop (measured at
/// ~5% of broker CPU with 10k partitions and 600 consumers).
const READY_TTL: Duration = Duration::from_millis(50);

/// `(tenant, queue, group)`.
pub(crate) type LaneKey = (String, String, String);

#[derive(Default)]
struct Lane {
    /// This lane's pops in flight on this node.
    live: u32,
    /// Messages acknowledged per second of lease, smoothed; 0 = no sample yet.
    drain: f64,
    /// The last ready count read for this lane, and when.
    ready: Option<(usize, Instant)>,
}

struct LeaseRec {
    lane: LaneKey,
    at: Instant,
    /// Messages delivered under the lease and not yet acknowledged.
    left: u32,
}

/// What a pop attempt uses, and echoes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Plan {
    pub partitions: u32,
    pub batch: u32,
}

pub(crate) struct Autopilot {
    drain_budget: f64,
    batch_min: u32,
    batch_max: u32,
    lanes: Vec<Mutex<HashMap<LaneKey, Lane>>>,
    leases: Vec<Mutex<HashMap<String, LeaseRec>>>,
    /// Leases waiting for their ack, so an ack on a node where no autopilot
    /// pop delivered anything costs one atomic load.
    tracked: AtomicUsize,
    hasher: std::collections::hash_map::RandomState,
}

/// Counts one pop of a lane as live until it drops.
pub(crate) struct Live<'a> {
    ap: &'a Autopilot,
    key: LaneKey,
}

impl Drop for Live<'_> {
    fn drop(&mut self) {
        let mut g = self.ap.lane_shard(&self.key).lock().expect("autopilot lanes");
        if let Some(l) = g.get_mut(&self.key) {
            l.live = l.live.saturating_sub(1);
        }
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

impl Autopilot {
    pub(crate) fn new(drain_budget_ms: u64, batch_min: u32, batch_max: u32) -> Autopilot {
        let batch_min = batch_min.clamp(1, 10_000);
        Autopilot {
            drain_budget: drain_budget_ms.max(1) as f64 / 1000.0,
            batch_min,
            batch_max: batch_max.clamp(batch_min, 10_000),
            lanes: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            leases: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            tracked: AtomicUsize::new(0),
            hasher: std::collections::hash_map::RandomState::new(),
        }
    }

    /// `QUEEN_RAFT_AUTOPILOT_DRAIN_MS` (200), `QUEEN_RAFT_AUTOPILOT_BATCH_MIN`
    /// (100), `QUEEN_RAFT_AUTOPILOT_BATCH_MAX` (1000).
    pub(crate) fn from_env() -> Autopilot {
        Autopilot::new(
            env_u64("QUEEN_RAFT_AUTOPILOT_DRAIN_MS", 200),
            env_u64("QUEEN_RAFT_AUTOPILOT_BATCH_MIN", 100) as u32,
            env_u64("QUEEN_RAFT_AUTOPILOT_BATCH_MAX", 1000) as u32,
        )
    }

    fn shard_of<K: Hash + ?Sized>(&self, k: &K) -> usize {
        let mut h = self.hasher.build_hasher();
        k.hash(&mut h);
        (h.finish() as usize) % SHARDS
    }

    fn lane_shard(&self, key: &LaneKey) -> &Mutex<HashMap<LaneKey, Lane>> {
        &self.lanes[self.shard_of(key)]
    }

    /// A pop of `key` starts: it counts as live until the guard drops.
    pub(crate) fn enter(&self, key: LaneKey) -> Live<'_> {
        {
            let mut g = self.lane_shard(&key).lock().expect("autopilot lanes");
            g.entry(key.clone()).or_default().live += 1;
        }
        Live { ap: self, key }
    }

    /// The knobs for one attempt of a pop of `key`. `ready` is the group's
    /// partitions due now (`None` when unknown); a dimension not delegated keeps
    /// the client's value.
    pub(crate) fn plan(
        &self,
        key: &LaneKey,
        ready: Option<usize>,
        auto_parts: bool,
        auto_batch: bool,
        parts: u32,
        batch: u32,
    ) -> Plan {
        let (live, drain) = {
            let g = self.lane_shard(key).lock().expect("autopilot lanes");
            g.get(key).map_or((1, 0.0), |l| (l.live.max(1), l.drain))
        };
        let partitions = if auto_parts {
            match ready {
                Some(r) => (r.div_ceil(live as usize) as u32).clamp(1, W_CEILING),
                None => parts.clamp(1, W_CEILING),
            }
        } else {
            parts
        };
        let batch = if auto_batch {
            if drain > 0.0 {
                ((drain * self.drain_budget).round() as u64)
                    .clamp(self.batch_min as u64, self.batch_max as u64) as u32
            } else {
                self.batch_min
            }
        } else {
            batch
        };
        Plan { partitions, batch }
    }

    /// The lane's ready count if one was read within [`READY_TTL`].
    pub(crate) fn cached_ready(&self, key: &LaneKey) -> Option<usize> {
        let g = self.lane_shard(key).lock().expect("autopilot lanes");
        let (n, at) = g.get(key)?.ready?;
        (at.elapsed() < READY_TTL).then_some(n)
    }

    /// Remember a ready count just read for the lane.
    pub(crate) fn store_ready(&self, key: &LaneKey, n: usize) {
        let mut g = self.lane_shard(key).lock().expect("autopilot lanes");
        g.entry(key.clone()).or_default().ready = Some((n, Instant::now()));
    }

    /// How many ready rows the width needs to read: enough to divide among the
    /// live pops up to the ceiling, bounded so the scan stays cheap.
    pub(crate) fn ready_scan_cap(&self, key: &LaneKey) -> usize {
        let live = {
            let g = self.lane_shard(key).lock().expect("autopilot lanes");
            g.get(key).map_or(1, |l| l.live.max(1))
        };
        (W_CEILING as usize) * (live as usize).min(16)
    }

    /// A pop of `key` delivered `n` messages under `lease` for a manual ack:
    /// the lease's drain clock starts.
    pub(crate) fn delivered(&self, key: &LaneKey, lease: &str, n: u32) {
        if n == 0 || lease.is_empty() {
            return;
        }
        let now = Instant::now();
        let mut g = self.leases[self.shard_of(lease)]
            .lock()
            .expect("autopilot leases");
        if g.len() >= LEASE_SWEEP_AT {
            let before = g.len();
            g.retain(|_, r| now.duration_since(r.at) < LEASE_TTL);
            self.tracked.fetch_sub(before - g.len(), Ordering::Relaxed);
        }
        let prev = g.insert(
            lease.to_string(),
            LeaseRec {
                lane: key.clone(),
                at: now,
                left: n,
            },
        );
        if prev.is_none() {
            self.tracked.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Whether any lease waits for an ack (see [`Autopilot::acked`]).
    pub(crate) fn tracking(&self) -> bool {
        self.tracked.load(Ordering::Relaxed) > 0
    }

    /// An ack of `n` messages under `lease` arrived: one drain sample for the
    /// lease's lane, `n` over the time since its delivery. The lease is
    /// forgotten once everything it delivered was acknowledged.
    pub(crate) fn acked(&self, lease: &str, n: u32) {
        if n == 0 || lease.is_empty() {
            return;
        }
        let now = Instant::now();
        let (lane, rate) = {
            let mut g = self.leases[self.shard_of(lease)]
                .lock()
                .expect("autopilot leases");
            let Some(r) = g.get_mut(lease) else { return };
            let secs = now.duration_since(r.at).as_secs_f64().max(0.001);
            let rate = n as f64 / secs;
            let lane = r.lane.clone();
            r.left = r.left.saturating_sub(n);
            if r.left == 0 {
                g.remove(lease);
                self.tracked.fetch_sub(1, Ordering::Relaxed);
            }
            (lane, rate)
        };
        let mut g = self.lane_shard(&lane).lock().expect("autopilot lanes");
        let l = g.entry(lane).or_default();
        l.drain = if l.drain == 0.0 {
            rate
        } else {
            (1.0 - ALPHA) * l.drain + ALPHA * rate
        };
    }
}

/// Add the choice to a pop's JSON body: `{…,"autopilot":{"partitions":W,"batch":B}}`.
/// Additive — a body that is not an object is left alone.
pub(crate) fn echo(body: &mut String, plan: Plan) {
    if !body.ends_with('}') {
        return;
    }
    body.pop();
    let sep = if body.ends_with('{') { "" } else { "," };
    body.push_str(&format!(
        "{sep}\"autopilot\":{{\"partitions\":{},\"batch\":{}}}}}",
        plan.partitions, plan.batch
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> LaneKey {
        ("t".into(), "q".into(), "g".into())
    }

    #[test]
    fn a_dimension_the_client_sent_is_never_touched() {
        let ap = Autopilot::new(200, 100, 1000);
        let _live = ap.enter(key());
        let p = ap.plan(&key(), Some(500), false, false, 3, 250);
        assert_eq!(p, Plan { partitions: 3, batch: 250 });
    }

    #[test]
    fn the_width_divides_the_ready_partitions_among_the_live_pops() {
        let ap = Autopilot::new(200, 100, 1000);
        let a = ap.enter(key());
        assert_eq!(ap.plan(&key(), Some(40), true, false, 1, 200).partitions, 40);
        let b = ap.enter(key());
        let c = ap.enter(key());
        let d = ap.enter(key());
        assert_eq!(ap.plan(&key(), Some(40), true, false, 1, 200).partitions, 10);
        assert_eq!(ap.plan(&key(), Some(1000), true, false, 1, 200).partitions, 64);
        assert_eq!(ap.plan(&key(), Some(0), true, false, 1, 200).partitions, 1);
        assert_eq!(ap.plan(&key(), None, true, false, 5, 200).partitions, 5);
        drop((a, b, c));
        assert_eq!(ap.plan(&key(), Some(40), true, false, 1, 200).partitions, 40);
        drop(d);
    }

    #[test]
    fn the_batch_follows_the_measured_drain_rate() {
        let ap = Autopilot::new(200, 100, 1000);
        // Cold: the minimum.
        assert_eq!(ap.plan(&key(), None, false, true, 1, 200).batch, 100);
        // A fast consumer: 1000 messages acknowledged ~10 ms after delivery.
        ap.delivered(&key(), "lease-fast", 1000);
        std::thread::sleep(Duration::from_millis(10));
        ap.acked("lease-fast", 1000);
        assert_eq!(ap.plan(&key(), None, false, true, 1, 200).batch, 1000);
        // The lease is forgotten once fully acknowledged: no second sample.
        ap.acked("lease-fast", 1000);

        // A slow consumer on another lane: 100 messages in ~0.5 s = 200/s,
        // times the 200 ms budget = 40, floored at the minimum.
        let slow: LaneKey = ("t".into(), "q".into(), "slow".into());
        ap.delivered(&slow, "lease-slow", 100);
        std::thread::sleep(Duration::from_millis(500));
        ap.acked("lease-slow", 100);
        assert_eq!(ap.plan(&slow, None, false, true, 1, 200).batch, 100);
    }

    #[test]
    fn the_echo_is_additive() {
        let mut b = "{\"messages\":[]}".to_string();
        echo(&mut b, Plan { partitions: 4, batch: 500 });
        assert_eq!(b, "{\"messages\":[],\"autopilot\":{\"partitions\":4,\"batch\":500}}");
        let mut empty = "{}".to_string();
        echo(&mut empty, Plan { partitions: 1, batch: 100 });
        assert_eq!(empty, "{\"autopilot\":{\"partitions\":1,\"batch\":100}}");
        let mut not_obj = "[]".to_string();
        echo(&mut not_obj, Plan { partitions: 1, batch: 100 });
        assert_eq!(not_obj, "[]");
    }
}
