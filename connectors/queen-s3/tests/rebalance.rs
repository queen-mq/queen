//! Rebalancing (`crate::placement`): the queues of a sink spread over the
//! nodes that run it, whatever order the nodes started in, and stay put once
//! they are spread.
//!
//! Every node here is a whole [`Sink`] over ONE [`FakeQueen`] and ONE bucket,
//! on a paused clock that the double's TTLs follow
//! ([`FakeQueen::follow_tokio_clock`]): a lease, or a node's presence row,
//! expires when virtual time passes it, as on a broker whose node stopped
//! renewing. The lease TTL is the product's, 30 s.

use std::sync::Arc;
use std::time::Duration;

use queen_s3::queen::FakeQueen;
use queen_s3::s3::MemoryStore;
use queen_s3::types::Micros;
use queen_s3::{Config, Sink};

#[path = "driver_support.rs"]
mod support;
use support::*;

const TTL: Duration = Duration::from_secs(30);
const QUEUES: [&str; 6] = ["q1", "q2", "q3", "q4", "q5", "q6"];

/// One node: its sink, and the task running it.
struct Node {
    sink: Arc<Sink>,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl Node {
    fn start(
        queen: &Arc<FakeQueen>,
        store: &Arc<MemoryStore>,
        n: u64,
        extra: &[(&str, &str)],
    ) -> Node {
        let list = QUEUES.join(",");
        let mut pairs: Vec<(&str, &str)> = vec![
            ("QUEEN_S3_QUEUES", list.as_str()),
            ("QUEEN_S3_ENDPOINT", "http://gw:7070"),
            ("QUEEN_S3_REGION", "us-east-1"),
            ("QUEEN_S3_BUCKET", "lake"),
            ("QUEEN_S3_ACCESS_KEY", "ak"),
            ("QUEEN_S3_SECRET_KEY", "sk"),
            ("QUEEN_S3_START", "earliest"),
            ("QUEEN_S3_SAFE_GUARD_MS", "0"),
            ("QUEEN_S3_DISCOVERY_INTERVAL_MS", "200"),
        ];
        for (k, v) in extra {
            pairs.retain(|(existing, _)| existing != k);
            pairs.push((k, v));
        }
        let cfg = Config::from_pairs_with(&pairs, &format!("node-{n}")).unwrap();
        // A fixed jitter per (node, queue): the same run every time.
        let jitter = move |q: &str| {
            let qi: u64 = q.trim_start_matches('q').parse().unwrap_or(0);
            Duration::from_millis((61 * n + 17 * qi) % 197)
        };
        let sink = Arc::new(
            Sink::new(cfg, queen.clone(), Some(store.clone()))
                .unwrap()
                .with_claim_jitter(jitter),
        );
        let (stop, rx) = tokio::sync::oneshot::channel::<()>();
        let task = {
            let sink = sink.clone();
            tokio::spawn(async move {
                sink.run(async {
                    let _ = rx.await;
                })
                .await
            })
        };
        Node { sink, stop, task }
    }

    /// A clean stop: drained, leases and presence row given back.
    async fn stop(self) {
        let _ = self.stop.send(());
        tokio::time::timeout(Duration::from_secs(600), self.task)
            .await
            .expect("the node drains")
            .expect("and does not panic");
    }

    /// A crash: nothing given back, everything left to expire.
    fn kill(self) {
        self.task.abort();
    }

    fn held(&self) -> Vec<String> {
        let st = self.sink.status();
        let mut held: Vec<String> = st["queues"]
            .as_array()
            .map(|qs| {
                qs.iter()
                    .filter(|q| q["ownedHere"] == true)
                    .map(|q| q["name"].as_str().unwrap_or_default().to_string())
                    .collect()
            })
            .unwrap_or_default();
        held.sort();
        held
    }

    fn given_back(&self) -> u64 {
        self.sink
            .prometheus()
            .lines()
            .find_map(|l| l.strip_prefix("queen_s3_queues_given_back_total "))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0)
    }
}

fn counts(nodes: &[&Node]) -> Vec<usize> {
    nodes.iter().map(|n| n.held().len()).collect()
}

/// Every queue held by exactly one of `nodes`.
fn all_held(nodes: &[&Node]) -> bool {
    let mut held: Vec<String> = nodes.iter().flat_map(|n| n.held()).collect();
    held.sort();
    held == QUEUES
}

/// Wait, a second at a time on the paused clock, until `done` — at most
/// `limit` — and say how long it took. Panics naming `what` and the placement.
async fn until(what: &str, limit: Duration, nodes: &[&Node], done: impl Fn() -> bool) -> Duration {
    let start = tokio::time::Instant::now();
    while start.elapsed() < limit {
        if done() {
            return start.elapsed();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!(
        "{what}: not within {limit:?}; placement {:?}",
        nodes.iter().map(|n| n.held()).collect::<Vec<_>>()
    );
}

/// Hold still for `span` and say whether anything moved: the placement and
/// every node's given-back count, before and after.
async fn moves_over(span: Duration, nodes: &[&Node]) -> (bool, Vec<u64>) {
    let before: Vec<(Vec<String>, u64)> =
        nodes.iter().map(|n| (n.held(), n.given_back())).collect();
    let mut moved = false;
    let start = tokio::time::Instant::now();
    while start.elapsed() < span {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now: Vec<Vec<String>> = nodes.iter().map(|n| n.held()).collect();
        moved |= now.iter().zip(&before).any(|(a, (b, _))| a != b);
    }
    let given: Vec<u64> = nodes
        .iter()
        .zip(&before)
        .map(|(n, (_, g))| n.given_back() - g)
        .collect();
    (moved || given.iter().any(|g| *g > 0), given)
}

fn seeded() -> (Arc<FakeQueen>, Arc<MemoryStore>) {
    let queen = Arc::new(FakeQueen::new());
    queen.follow_tokio_clock();
    for q in QUEUES {
        let partition = format!("{q}-a");
        seed_two_hours(&queen, q, &[partition.as_str()]);
    }
    (queen, Arc::new(MemoryStore::new()))
}

/// One node owns every queue and gives none back, however long it runs.
#[tokio::test(start_paused = true)]
async fn a_single_node_owns_everything() {
    let (queen, store) = seeded();
    let a = Node::start(&queen, &store, 0, &[]);
    until("one node holds all six", 60 * TTL, &[&a], || {
        a.held().len() == 6
    })
    .await;
    let (moved, given) = moves_over(20 * TTL, &[&a]).await;
    assert!(!moved, "a lone node moves nothing: {given:?}");
    let st = a.sink.status();
    assert_eq!(st["placement"]["nodes"], 1);
    assert_eq!(st["placement"]["share"], 6);
    assert_eq!(st["placement"]["held"], 6);
    let presence = |q: &FakeQueen| {
        q.kv_keys()
            .into_iter()
            .filter(|k| k.starts_with("s3:default:node="))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        presence(&queen),
        ["s3:default:node=node-0"],
        "the node's presence row"
    );
    a.stop().await;
    assert!(
        presence(&queen).is_empty(),
        "a clean stop deletes it: the others count one node fewer at once"
    );
}

/// A StatefulSet's cold start: one node alone for a while — it takes every
/// queue — then a second, then a third. The first gives queues back one at a
/// time, the newcomers below their share take them, and the placement ends at
/// the ceiling shares; then nothing moves for twenty TTLs.
#[tokio::test(start_paused = true)]
async fn a_staggered_start_converges_to_fair_shares_and_holds_still() {
    let (queen, store) = seeded();
    let a = Node::start(&queen, &store, 0, &[]);
    until("the first node holds all six", 60 * TTL, &[&a], || {
        a.held().len() == 6
    })
    .await;
    tokio::time::sleep(3 * TTL).await;
    let b = Node::start(&queen, &store, 1, &[]);
    tokio::time::sleep(TTL / 2).await;
    let c = Node::start(&queen, &store, 2, &[]);
    let nodes = [&a, &b, &c];

    let free = std::cell::Cell::new(0usize);
    let took = until("two queues each", 20 * TTL, &nodes, || {
        let held: usize = counts(&nodes).iter().sum();
        free.set(free.get().max(QUEUES.len() - held.min(QUEUES.len())));
        all_held(&nodes) && counts(&nodes) == [2, 2, 2]
    })
    .await;
    assert!(
        free.get() <= 1,
        "one queue given back at a time — the next only once the last is held elsewhere: \
         {} were free at once",
        free.get()
    );
    assert_eq!(
        a.given_back(),
        4,
        "the first node gave back exactly its excess"
    );
    assert_eq!(
        b.given_back() + c.given_back(),
        0,
        "and nobody else moved anything"
    );
    for n in nodes {
        let st = n.sink.status();
        assert_eq!(st["placement"]["nodes"], 3, "{st}");
        assert_eq!(st["placement"]["share"], 2, "{st}");
    }
    let (moved, given) = moves_over(20 * TTL, &nodes).await;
    assert!(!moved, "a steady state moves nothing: {given:?}");
    println!("staggered start converged in {took:?}");
    for n in [a, b, c] {
        n.stop().await;
    }
}

/// A node dies: its leases and its presence row expire a TTL later, the two
/// others take its queues — three each — and then hold still. It comes back:
/// it gets its share again within a bounded time, one queue from each.
#[tokio::test(start_paused = true)]
async fn a_dead_node_s_queues_move_and_it_gets_its_share_back_when_it_returns() {
    let (queen, store) = seeded();
    let a = Node::start(&queen, &store, 0, &[]);
    let b = Node::start(&queen, &store, 1, &[]);
    let c = Node::start(&queen, &store, 2, &[]);
    let three = [&a, &b, &c];
    until("two queues each", 20 * TTL, &three, || {
        all_held(&three) && counts(&three) == [2, 2, 2]
    })
    .await;

    c.kill();
    let two = [&a, &b];
    let took = until("the two survivors hold all six", 10 * TTL, &two, || {
        all_held(&two) && counts(&two) == [3, 3]
    })
    .await;
    assert!(
        took <= 3 * TTL,
        "a dead node's queues move within a TTL of its lease expiring: {took:?}"
    );
    let (moved, given) = moves_over(10 * TTL, &two).await;
    assert!(!moved, "and then hold still: {given:?}");

    let c = Node::start(&queen, &store, 2, &[]);
    let three = [&a, &b, &c];
    let back = until(
        "the returning node has its share again",
        20 * TTL,
        &three,
        || all_held(&three) && counts(&three) == [2, 2, 2],
    )
    .await;
    // A TTL over the share before the first hand-off, a third of one for the
    // newcomer below its share to notice the queue free, and the claim's
    // pacing: well inside two TTLs. (A node below its share that looked only
    // once a TTL would need more than two.)
    assert!(back <= 2 * TTL, "{back:?}");
    assert_eq!(
        a.given_back() + b.given_back(),
        2,
        "one queue from each survivor"
    );
    let (moved, given) = moves_over(10 * TTL, &three).await;
    assert!(!moved, "{given:?}");
    println!("failover {took:?}, rejoin {back:?}");
    for n in [a, b, c] {
        n.stop().await;
    }
}

/// Queues move while records keep arriving — a node alone, then two more —
/// and the lake still holds every record exactly once: a hand-off drains like
/// a stop, and the next owner starts from the commit pointer.
#[tokio::test(start_paused = true)]
async fn queues_move_under_traffic_and_every_record_lands_exactly_once() {
    let queen = Arc::new(FakeQueen::new());
    queen.follow_tokio_clock();
    let store = Arc::new(MemoryStore::new());
    let base = t("2026-09-04T10:00:00.000000Z");
    let window = [
        ("QUEEN_S3_MAX_WINDOW_MS", "10000"),
        ("QUEEN_S3_ALIGN", "none"),
    ];

    // One record per queue per second, for six minutes of traffic.
    let pusher = {
        let queen = queen.clone();
        tokio::spawn(async move {
            let mut pushed: Vec<(String, i64)> = Vec::new();
            for second in 0..360i64 {
                let ts = Micros(base.0 + second * 1_000_000);
                for q in QUEUES {
                    let partition = format!("{q}-p");
                    let offset = queen.push(q, &partition, ts, &["{\"n\":1}"]);
                    pushed.push((partition, offset));
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            pushed
        })
    };

    let a = Node::start(&queen, &store, 0, &window);
    tokio::time::sleep(2 * TTL).await;
    let b = Node::start(&queen, &store, 1, &window);
    tokio::time::sleep(TTL).await;
    let c = Node::start(&queen, &store, 2, &window);
    let nodes = [&a, &b, &c];
    let pushed = pusher.await.unwrap();
    assert!(
        a.given_back() > 0,
        "queues moved while the records arrived: {:?}",
        counts(&nodes)
    );

    // The broker's clock keeps moving after the last record, as the sink's
    // own lease refreshes move it, so the last windows close too.
    let ticker = {
        let queen = queen.clone();
        tokio::spawn(async move {
            loop {
                let now = queen.safe_time();
                queen.set_safe_time(Micros(now.0 + 1_000_000));
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    };
    until("every record in the lake", 40 * TTL, &nodes, || {
        all_rows(&store).len() >= pushed.len()
    })
    .await;
    ticker.abort();
    for n in [a, b, c] {
        n.stop().await;
    }
    assert_exactly_once(&store, &pushed);
    assert_no_gaps(&store);
}

/// A node that comes and goes within seconds — its presence row seen for a
/// moment — moves nothing: a node gives a queue back only once it has held
/// more than its share for a whole TTL.
#[tokio::test(start_paused = true)]
async fn a_node_that_comes_and_goes_within_seconds_moves_nothing() {
    let (queen, store) = seeded();
    let a = Node::start(&queen, &store, 0, &[]);
    let b = Node::start(&queen, &store, 1, &[]);
    let two = [&a, &b];
    until("three queues each", 20 * TTL, &two, || {
        all_held(&two) && counts(&two) == [3, 3]
    })
    .await;
    let (moved, given) = moves_over(2 * TTL, &two).await;
    assert!(!moved, "{given:?}");

    let c = Node::start(&queen, &store, 2, &[]);
    tokio::time::sleep(Duration::from_secs(12)).await;
    assert!(c.held().is_empty(), "nothing was free for it");
    c.stop().await;
    let (moved, given) = moves_over(3 * TTL, &two).await;
    assert!(!moved, "a glimpse of a third node moves nothing: {given:?}");
    for n in [a, b] {
        n.stop().await;
    }
}
