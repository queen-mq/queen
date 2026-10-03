//! placement — which node runs which queue, beyond "whoever claimed it first".
//!
//! The lease ([`crate::lease`]) decides who writes a queue; it says nothing
//! about whether the queues of a sink are spread over the nodes that run it. A
//! node that booted alone — a Kubernetes StatefulSet starts its pods one after
//! another — claims every queue, and without this module nothing would ever
//! move one back. Three small mechanisms fix that, and none needs a knob beyond
//! the lease TTL.
//!
//! # Presence: how many nodes there are
//!
//! Each running sink keeps a TTL'd row in its tenant's KV store,
//! `s3:<sink>:node=<esc instance>` ([`node_key`]), written every third of the
//! lease TTL — the lease's own cadence — and deleted on a clean stop. One batch
//! writes the row and reads every live row under `s3:<sink>:node=` back, so a
//! node always counts itself: N, the sink's nodes alive. (`node=` and not
//! `node:`: `=` is escaped in a queue name, so no queue's documents —
//! `s3:<sink>:<esc queue>:…` — can ever fall under the prefix; a queue named
//! `node` would under `node:`.) A row expires a TTL after its node stopped
//! writing it, so a node that died stops being counted a TTL later, and one
//! that left cleanly at once.
//!
//! # Fair share: when a node gives a queue back
//!
//! `share = ceil(Q / N)` ([`fair_share`]), Q the queues the sink runs. A node
//! holding MORE than its share gives ONE queue back at a time: it signals that
//! queue's task to drain exactly as the shutdown drain does — the window in its
//! commit sequence finished, a filling window closed and committed when it is
//! worth it and abandoned otherwise, the lease released (with retries) — and
//! then waits until another node holds that queue, or a TTL has passed, before
//! it considers the next. The queue given back is the one with the least
//! buffered, ties by name ([`pick_to_shed`]): the drain finishes or abandons
//! what is buffered, so the emptiest queue costs the least to hand over — the
//! least to upload in a hurry, or to read again on the next node — and the name
//! makes the choice the same on every run.
//!
//! # Claiming: who takes it
//!
//! The load-aware wait stays ([`crate::sink::CLAIM_STEP`] per queue already
//! run). On top of it, a node at or above its share waits one more TTL before
//! it claims a free queue, and looks at a queue somebody else holds only once a
//! TTL — while a node BELOW its share looks every third of a TTL and claims at
//! once: the queue a node gave back goes to a node that is short. The node that
//! gave a queue back does not claim it again for two TTLs. None of these waits
//! is unbounded, and that is the liveness rule: a free queue nobody below its
//! share took is claimed by anyone after them.
//!
//! # Why nothing ping-pongs
//!
//! Count the excess, `Σ max(0, held − share)` over the nodes. In a steady state
//! (N and Q unchanged) every node holds at most its share once the excess is
//! zero, and then nothing moves: a node gives back only when it holds MORE than
//! its share, and no queue is free to claim. While there is an excess, some
//! node is below its share (the shares add up to at least Q), and a queue given
//! back goes to such a node: it notices within a third of a TTL and claims at
//! once, while every node at its share waits a TTL more and the giver two —
//! so each move lowers the excess by one, and the moves stop when it is zero.
//! A node joining or leaving changes N, hence the share, and moves at most the
//! new excess, one queue at a time per node.
//!
//! What could still make a queue go back and forth is two nodes disagreeing
//! about N for a moment — a row seen by one and expired for the other. Two
//! rules absorb that. A node gives a queue back only once it has held more than
//! its share for a whole TTL (the longest two views of the rows can disagree:
//! a row is live until a TTL after its last write); and it gives back the next
//! one only after the last is held elsewhere, so one node moves one queue per
//! settled observation, never a burst on a glimpse.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::time::Instant;

use crate::config::Config;
use crate::driver::{Runtime, Shutdown};
use crate::layout::escape;
use crate::lease::{delete_row_if_ours, mint_incarnation, result_at, Holder, Lease};
use crate::obs::{lock, now_epoch_ms, Sampler};
use crate::queen::{KvOp, QueenApi, Result};
use crate::types::{Lease as LeaseDoc, SinkError};

/// One line per thirty seconds for a presence row that cannot be written.
static PRESENCE_FAIL: Sampler = Sampler::new(30_000);

/// How long the node that gave a queue back leaves it to the others, in lease
/// TTLs.
pub const SHED_COOLDOWN_TTLS: u32 = 2;

/// `s3:<sink>:node=<esc instance>` — a node's presence row.
pub fn node_key(sink: &str, instance: &str) -> String {
    format!("{}{}", node_prefix(sink), escape(instance))
}

/// `s3:<sink>:node=` — what a `getPrefix` walks to count the nodes. Falls under
/// no queue's documents: `=` is escaped in a queue name.
pub fn node_prefix(sink: &str) -> String {
    format!("s3:{sink}:node=")
}

/// The fair share: `ceil(queues / nodes)`, `nodes` counted as at least one.
pub fn fair_share(queues: usize, nodes: usize) -> usize {
    queues.div_ceil(nodes.max(1))
}

/// The queue to give back: the least buffered, ties by name (module docs).
pub fn pick_to_shed<'a, I>(held: I) -> Option<&'a str>
where
    I: IntoIterator<Item = (&'a str, usize)>,
{
    held.into_iter()
        .min_by(|(qa, ba), (qb, bb)| ba.cmp(bb).then(qa.cmp(qb)))
        .map(|(q, _)| q)
}

// ---------------------------------------------------------------------------
// Presence
// ---------------------------------------------------------------------------

/// This node's presence row, and the count of everybody's.
pub(crate) struct Presence {
    queen: Arc<dyn QueenApi>,
    key: String,
    prefix: String,
    incarnation: String,
    doc: Value,
    ttl_seconds: u64,
}

impl Presence {
    pub(crate) fn new(
        queen: Arc<dyn QueenApi>,
        sink: &str,
        instance: &str,
        ttl_ms: u64,
    ) -> Presence {
        let incarnation = mint_incarnation();
        let doc = serde_json::to_value(LeaseDoc {
            instance: instance.to_string(),
            incarnation: incarnation.clone(),
            since_ms: now_epoch_ms(),
        })
        .unwrap_or(Value::Null);
        Presence {
            queen,
            key: node_key(sink, instance),
            prefix: node_prefix(sink),
            incarnation,
            doc,
            ttl_seconds: (ttl_ms / 1_000).max(1),
        }
    }

    /// Write this node's row, then count the live rows — this node's
    /// included, since the batch's read comes after its write.
    pub(crate) async fn beat(&self) -> Result<usize> {
        let results = self
            .queen
            .kv(vec![
                KvOp::put_ttl(self.key.clone(), self.doc.clone(), self.ttl_seconds),
                KvOp::get_prefix(self.prefix.clone(), 1_000, None),
            ])
            .await?;
        let page = result_at(&results, 1).ok_or_else(|| {
            SinkError::Body("kv answered no page for the presence count".to_string())
        })?;
        let mut count = page.rows.len();
        let mut mine = page.rows.iter().any(|r| r.key == self.key);
        let mut after = page.next_after.clone().filter(|_| page.truncated);
        while let Some(cursor) = after.take() {
            let more = self
                .queen
                .kv(vec![KvOp::get_prefix(
                    self.prefix.clone(),
                    1_000,
                    Some(cursor),
                )])
                .await?;
            let Some(page) = result_at(&more, 0) else {
                break;
            };
            count += page.rows.len();
            mine |= page.rows.iter().any(|r| r.key == self.key);
            after = page.next_after.clone().filter(|_| page.truncated);
        }
        Ok(count + usize::from(!mine))
    }

    /// Delete this node's row on a clean stop, so the others count one node
    /// fewer at once rather than a TTL later.
    pub(crate) async fn leave(&self) {
        delete_row_if_ours(&self.queen, &self.key, &self.incarnation, "presence row").await;
    }
}

// ---------------------------------------------------------------------------
// The shared view
// ---------------------------------------------------------------------------

/// A queue being given back, until another node holds it or a TTL has passed
/// since it was released.
#[derive(Clone, Debug)]
struct Shedding {
    queue: String,
    released_at: Option<Instant>,
}

/// What one sink's queue tasks and its rebalancer share about placement.
pub(crate) struct Placement {
    /// N, as the last presence count saw it; at least 1, this node.
    nodes: AtomicUsize,
    /// Q, the queues this sink runs.
    queues: AtomicUsize,
    /// The queues this sink's tasks hold right now.
    held: AtomicUsize,
    /// Per queue held here, the signal that hands it back.
    handoffs: Mutex<HashMap<String, Shutdown>>,
    /// Per queue this node gave back, when — its re-claim cooldown.
    shed_at: Mutex<HashMap<String, Instant>>,
    shedding: Mutex<Option<Shedding>>,
    ttl: Duration,
}

impl Placement {
    pub(crate) fn new(ttl: Duration) -> Placement {
        Placement {
            nodes: AtomicUsize::new(1),
            queues: AtomicUsize::new(0),
            held: AtomicUsize::new(0),
            handoffs: Mutex::new(HashMap::new()),
            shed_at: Mutex::new(HashMap::new()),
            shedding: Mutex::new(None),
            ttl,
        }
    }

    pub(crate) fn nodes(&self) -> usize {
        self.nodes.load(Ordering::SeqCst)
    }

    pub(crate) fn set_nodes(&self, n: usize) {
        self.nodes.store(n.max(1), Ordering::SeqCst);
    }

    pub(crate) fn queues(&self) -> usize {
        self.queues.load(Ordering::SeqCst)
    }

    pub(crate) fn set_queues(&self, q: usize) {
        self.queues.store(q, Ordering::SeqCst);
    }

    pub(crate) fn held(&self) -> usize {
        self.held.load(Ordering::SeqCst)
    }

    pub(crate) fn share(&self) -> usize {
        fair_share(self.queues(), self.nodes())
    }

    /// Whether this node already holds its share (or more): it then leaves a
    /// free queue to the nodes below theirs for a TTL.
    pub(crate) fn at_or_above_share(&self) -> bool {
        self.held() >= self.share()
    }

    /// The queue being given back, if any.
    pub(crate) fn shedding(&self) -> Option<String> {
        lock(&self.shedding).as_ref().map(|s| s.queue.clone())
    }

    /// When this node may claim `queue` again after giving it back.
    pub(crate) fn cooldown_until(&self, queue: &str) -> Option<Instant> {
        let at = *lock(&self.shed_at).get(queue)?;
        let until = at + self.ttl.saturating_mul(SHED_COOLDOWN_TTLS);
        (Instant::now() < until).then_some(until)
    }

    /// The task now holds `queue`: the signal that would hand it back.
    pub(crate) fn begin_ownership(&self, queue: &str) -> Shutdown {
        let signal = Shutdown::new();
        lock(&self.handoffs).insert(queue.to_string(), signal.clone());
        self.held.fetch_add(1, Ordering::SeqCst);
        signal
    }

    /// The task released `queue`; `handed_off` when it was given back.
    pub(crate) fn end_ownership(&self, queue: &str, handed_off: bool) {
        if lock(&self.handoffs).remove(queue).is_some() {
            self.held.fetch_sub(1, Ordering::SeqCst);
        }
        if handed_off {
            lock(&self.shed_at).insert(queue.to_string(), Instant::now());
        }
        if let Some(s) = lock(&self.shedding).as_mut() {
            if s.queue == queue && s.released_at.is_none() {
                s.released_at = Some(Instant::now());
            }
        }
    }

    /// The decision of one round: settle a hand-off in flight, or start one
    /// when this node has held more than its share for a TTL. `over_since` is
    /// the rebalancer's memory of when the excess began.
    async fn decide(&self, rt: &Runtime, cfg: &Config, over_since: &mut Option<Instant>) {
        let in_flight = lock(&self.shedding).clone();
        if let Some(s) = in_flight {
            let Some(released) = s.released_at else {
                return; // still draining
            };
            let lease = Lease::new(
                rt.queen.clone(),
                &cfg.sink,
                &s.queue,
                &cfg.instance,
                cfg.lease_ttl_ms,
            );
            let elsewhere = match lease.holder().await {
                Ok(Holder::Other(owner)) => Some(owner),
                _ => None,
            };
            if elsewhere.is_none() && released.elapsed() < self.ttl {
                return;
            }
            match &elsewhere {
                Some(owner) => tracing::info!(
                    target: "queen-s3",
                    queue = %s.queue,
                    owner = %owner,
                    "the queue given back is held by another node"
                ),
                None => tracing::info!(
                    target: "queen-s3",
                    queue = %s.queue,
                    "no node took the queue given back within a TTL; moving on"
                ),
            }
            *lock(&self.shedding) = None;
        }

        let (held, share) = (self.held(), self.share());
        if held <= share {
            *over_since = None;
            return;
        }
        let since = *over_since.get_or_insert_with(Instant::now);
        if since.elapsed() < self.ttl {
            return; // not for a glimpse: a whole TTL over the share
        }
        let candidates: Vec<(String, usize)> = lock(&self.handoffs)
            .keys()
            .map(|q| {
                let buffered = rt.status.queue(q).map(|s| s.buffered_bytes).unwrap_or(0);
                (q.clone(), buffered)
            })
            .collect();
        let Some(queue) =
            pick_to_shed(candidates.iter().map(|(q, b)| (q.as_str(), *b))).map(str::to_string)
        else {
            return;
        };
        let Some(signal) = lock(&self.handoffs).get(&queue).cloned() else {
            return;
        };
        tracing::info!(
            target: "queen-s3",
            queue = %queue,
            held,
            share,
            nodes = self.nodes(),
            queues = self.queues(),
            "this node holds more than its share: giving one queue back"
        );
        *lock(&self.shedding) = Some(Shedding {
            queue,
            released_at: None,
        });
        signal.trigger();
    }
}

/// The sink's rebalancer: the presence beat and the share decision, every
/// third of the lease TTL until the stop. A beat that fails is tried again
/// sooner — a second, doubling — so a node that met no leader at boot is
/// counted as soon as there is one.
pub(crate) async fn rebalance(
    rt: Runtime,
    cfg: Arc<Config>,
    placement: Arc<Placement>,
    presence: Arc<Presence>,
) {
    let ttl = Duration::from_millis(cfg.lease_ttl_ms);
    let every = (ttl / 3).max(Duration::from_millis(100));
    let mut over_since: Option<Instant> = None;
    let mut failures: u32 = 0;
    loop {
        let wait = match presence.beat().await {
            Ok(n) => {
                failures = 0;
                placement.set_nodes(n);
                every
            }
            Err(e) => {
                failures = failures.saturating_add(1);
                if let Some(suppressed) = PRESENCE_FAIL.tick_now() {
                    tracing::warn!(target: "queen-s3", error = %e, suppressed, "cannot write this node's presence row; retrying");
                }
                Duration::from_secs(1)
                    .saturating_mul(1u32 << failures.saturating_sub(1).min(16))
                    .min(every)
            }
        };
        if rt.shutdown.is_set() {
            break;
        }
        placement.decide(&rt, &cfg, &mut over_since).await;
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = rt.shutdown.wait() => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_share_is_the_ceiling_and_a_lone_node_holds_everything() {
        assert_eq!(fair_share(6, 3), 2);
        assert_eq!(fair_share(7, 3), 3);
        assert_eq!(fair_share(6, 1), 6, "one node: all of them");
        assert_eq!(fair_share(6, 0), 6, "no count yet: this node alone");
        assert_eq!(fair_share(2, 5), 1);
        assert_eq!(fair_share(0, 3), 0);
    }

    #[test]
    fn the_queue_given_back_is_the_least_buffered_then_the_first_by_name() {
        assert_eq!(pick_to_shed([("b", 10), ("a", 30), ("c", 10)]), Some("b"));
        assert_eq!(pick_to_shed([("z", 0), ("y", 0)]), Some("y"));
        assert_eq!(pick_to_shed(Vec::<(&str, usize)>::new()), None);
    }

    /// The node that gave a queue back leaves it to the others for two TTLs,
    /// and only that queue, and only when it was given back.
    #[tokio::test(start_paused = true)]
    async fn the_giver_leaves_a_queue_it_gave_back_alone_for_two_ttls() {
        let ttl = Duration::from_secs(30);
        let p = Placement::new(ttl);
        p.set_queues(2);
        let _ = p.begin_ownership("a");
        let _ = p.begin_ownership("b");
        assert_eq!(p.held(), 2);
        p.end_ownership("a", true);
        p.end_ownership("b", false);
        assert_eq!(p.held(), 0);
        let until = p.cooldown_until("a").expect("given back: a cooldown");
        assert_eq!(until - Instant::now(), 2 * ttl);
        assert_eq!(p.cooldown_until("b"), None, "stopped, not given back");
        tokio::time::sleep(2 * ttl).await;
        assert_eq!(p.cooldown_until("a"), None, "and it ends");
    }

    fn runtime(queen: Arc<crate::queen::FakeQueen>, cfg: &Config) -> Runtime {
        let metrics = Arc::new(crate::obs::Metrics::new());
        Runtime {
            cfg: Arc::new(crate::driver::DriverConfig::from_config(cfg)),
            queen,
            store: Arc::new(crate::s3::MemoryStore::new()),
            writers: Arc::from(crate::writer::factory(&cfg.writer)),
            health: Arc::new(crate::health::HealthState::new(metrics.clone(), 300_000)),
            metrics,
            status: Arc::new(crate::status::StatusBoard::new()),
            budget: Arc::new(crate::driver::MemoryBudget::new(1 << 30)),
            shutdown: Shutdown::new(),
        }
    }

    /// One queue at a time: a node over its share starts a second hand-off
    /// only once the first queue is held by another node — or, if nobody takes
    /// it, a TTL after it was released — and not before it has been over its
    /// share for a whole TTL.
    #[tokio::test(start_paused = true)]
    async fn the_next_queue_is_given_back_once_the_last_is_held_elsewhere() {
        let ttl = Duration::from_secs(30);
        let cfg = Config::from_pairs_with(
            &[
                ("QUEEN_S3_QUEUES", "q1,q2,q3,q4"),
                ("QUEEN_S3_ENDPOINT", "http://gw:7070"),
                ("QUEEN_S3_REGION", "us-east-1"),
                ("QUEEN_S3_BUCKET", "lake"),
                ("QUEEN_S3_ACCESS_KEY", "ak"),
                ("QUEEN_S3_SECRET_KEY", "sk"),
            ],
            "node-a",
        )
        .unwrap();
        let queen = Arc::new(crate::queen::FakeQueen::new());
        queen.follow_tokio_clock();
        let rt = runtime(queen.clone(), &cfg);
        let p = Placement::new(ttl);
        p.set_queues(4);
        p.set_nodes(2);
        let signals: Vec<Shutdown> = ["q1", "q2", "q3", "q4"]
            .iter()
            .map(|q| p.begin_ownership(q))
            .collect();
        let mut over = None;

        p.decide(&rt, &cfg, &mut over).await;
        assert_eq!(
            p.shedding(),
            None,
            "over the share for a moment: nothing yet"
        );
        tokio::time::sleep(ttl).await;
        p.decide(&rt, &cfg, &mut over).await;
        assert_eq!(
            p.shedding().as_deref(),
            Some("q1"),
            "all empty: the first by name"
        );
        assert!(signals[0].is_set());

        p.end_ownership("q1", true);
        p.decide(&rt, &cfg, &mut over).await;
        assert_eq!(
            p.shedding().as_deref(),
            Some("q1"),
            "released, not yet held elsewhere"
        );
        assert!(!signals[1].is_set());

        let other = Lease::new(queen.clone(), "default", "q1", "node-b", 30_000);
        assert_eq!(
            other.acquire().await.unwrap(),
            crate::lease::Acquired::Taken
        );
        p.decide(&rt, &cfg, &mut over).await;
        assert_eq!(
            p.shedding().as_deref(),
            Some("q2"),
            "settled: the next one goes"
        );
        assert!(signals[1].is_set());

        // Nobody takes q2: the node moves on a TTL after releasing it.
        p.end_ownership("q2", true);
        tokio::time::sleep(ttl / 2).await;
        p.decide(&rt, &cfg, &mut over).await;
        assert_eq!(p.shedding().as_deref(), Some("q2"));
        tokio::time::sleep(ttl / 2).await;
        p.decide(&rt, &cfg, &mut over).await;
        assert_eq!(p.held(), 2, "at its share now");
        assert_eq!(p.shedding(), None, "and nothing more to give back");
        assert!(!signals[2].is_set() && !signals[3].is_set());
    }

    /// The share counts this node, and holding it is what stops a claim.
    #[test]
    fn at_its_share_a_node_lets_the_others_claim_first() {
        let p = Placement::new(Duration::from_secs(30));
        p.set_queues(6);
        p.set_nodes(3);
        assert_eq!(p.share(), 2);
        assert!(!p.at_or_above_share());
        let _ = p.begin_ownership("q1");
        let _ = p.begin_ownership("q2");
        assert!(p.at_or_above_share());
        p.set_nodes(0);
        assert_eq!(p.nodes(), 1, "a count of nothing is this node alone");
        assert!(!p.at_or_above_share(), "alone, the share is every queue");
    }

    #[test]
    fn presence_rows_fall_under_no_queue_s_documents() {
        assert_eq!(
            node_key("default", "node-1@host"),
            "s3:default:node=node-1%40host"
        );
        // A queue literally named `node` keeps its documents out of the count.
        let queue_docs = crate::lease::lease_key("default", "node");
        assert!(
            !queue_docs.starts_with(&node_prefix("default")),
            "{queue_docs}"
        );
        // And a queue named `node=x` is escaped past it too.
        let tricky = crate::lease::lease_key("default", "node=x");
        assert!(!tricky.starts_with(&node_prefix("default")), "{tricky}");
    }
}
