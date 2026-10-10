//! Messages that outlive their index rows, on the embedded broker booted the
//! way a node boots: the product's dedup index (`txns`), the background
//! retention scanner and the cluster version raised by the node itself.
//!
//! The process runs with a minimum row window of one second and a scan every
//! 20 ms, so a queue with a dedup window of zero loses its rows a second
//! after each push while it keeps every message. A second queue with an hour's window keeps
//! its rows, and takes the same pushes, pops and acks: every answer must be
//! the same on both. (`src/rsm/tests/rowless.rs` has the same checks on the
//! facade, where a unit test can read the store.)

use std::collections::HashMap;
use std::time::Duration;

use queen::protocol as qp;
use queen::{Broker, BrokerConfig};

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// One series of the broker's Prometheus text.
async fn series(broker: &Broker, name: &str) -> f64 {
    let text = broker.prometheus().await.expect("prometheus");
    text.lines()
        .find_map(|l| l.strip_prefix(name)?.trim().parse::<f64>().ok())
        .unwrap_or_else(|| panic!("no series {name}"))
}

async fn txns_rows(broker: &Broker) -> u64 {
    series(
        broker,
        "queen_raft_store_ram_rows{ks=\"txns\",kind=\"live\"}",
    )
    .await as u64
}

async fn wait_rows(broker: &Broker, want: u64) {
    let mut last = 0;
    for _ in 0..1_500 {
        last = txns_rows(broker).await;
        if last == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the store holds {last} index rows, expected {want}");
}

/// Push `n` messages to `(q, part)` in one request, numbered from `from`.
async fn push(broker: &Broker, q: &str, part: &str, from: u64, n: u64) {
    let items = (from..from + n)
        .map(|i| {
            qp::PushItem::new(q, serde_json::json!({ "p": part, "n": i }))
                .partition(part)
                .transaction_id(format!("{q}-{part}-{i}"))
        })
        .collect();
    let res = broker.push(items).await.expect("push");
    assert_eq!(res.len() as u64, n);
    assert!(
        res.iter().all(|r| r.status == qp::PushStatus::Queued),
        "{res:?}"
    );
}

fn params(group: &str, batch: i32, auto_ack: bool) -> qp::PopParams {
    qp::PopParams {
        batch: Some(batch),
        partitions: Some(2),
        auto_ack: auto_ack.then_some(true),
        consumer_group: Some(group.to_string()),
        subscription_mode: Some(qp::SubscriptionMode::All),
        ..Default::default()
    }
}

/// A pop, tried again while the broker says so (a claim whose queue-log
/// reads were on their way).
async fn pop(broker: &Broker, q: &str, p: &qp::PopParams) -> qp::PopResponse {
    for _ in 0..400 {
        match broker.pop(q, p).await {
            Ok(r) => return r,
            Err(e) if matches!(e.status(), Some(503 | 409)) => {
                tokio::time::sleep(Duration::from_millis(5)).await
            }
            Err(e) => panic!("pop of {q}: {e:?}"),
        }
    }
    panic!("the pop of {q} was told to retry for ever");
}

fn ack_items(msgs: &[qp::Message], lease: bool, status: qp::AckStatus) -> Vec<qp::AckBatchItem> {
    msgs.iter()
        .map(|m| qp::AckBatchItem {
            transaction_id: m.transaction_id.clone(),
            partition_id: m.partition_id.clone(),
            status,
            lease_id: lease.then(|| m.lease_id.clone()),
            error: None,
        })
        .collect()
}

async fn ack(broker: &Broker, group: &str, items: Vec<qp::AckBatchItem>) -> Vec<qp::AckResult> {
    broker
        .ack_batch(&qp::AckBatchRequest {
            acknowledgments: items,
            consumer_group: Some(group.to_string()),
        })
        .await
        .expect("ack")
}

/// Pop `q` as `group` until it stays empty, acking each batch unless the pop
/// did. Every message must be the one pushed with its number, and every
/// partition must come out in order with no gap. The numbers, per partition.
async fn drain(
    broker: &Broker,
    q: &str,
    group: &str,
    batch: i32,
    auto_ack: bool,
) -> HashMap<String, Vec<u64>> {
    let mut seen: HashMap<String, Vec<u64>> = HashMap::new();
    let p = params(group, batch, auto_ack);
    let mut empty = 0;
    while empty < 3 {
        let r = pop(broker, q, &p).await;
        if r.messages.is_empty() {
            empty += 1;
            tokio::time::sleep(Duration::from_millis(2)).await;
            continue;
        }
        empty = 0;
        for m in &r.messages {
            let n = m.data["n"].as_u64().expect("n");
            assert_eq!(m.data["p"], m.partition.as_str(), "{m:?}");
            assert_eq!(m.transaction_id, format!("{q}-{}-{n}", m.partition));
            let got = seen.entry(m.partition.clone()).or_default();
            if let Some(prev) = got.last() {
                assert_eq!(n, prev + 1, "{}: {n} after {prev} ({group})", m.partition);
            }
            got.push(n);
        }
        if !auto_ack {
            let res = ack(
                broker,
                group,
                ack_items(&r.messages, true, qp::AckStatus::Completed),
            )
            .await;
            assert!(res.iter().all(|a| a.success), "{res:?}");
        }
    }
    seen
}

fn expect(seen: &HashMap<String, Vec<u64>>, parts: &[&str], from: u64, end: u64, who: &str) {
    assert_eq!(seen.len(), parts.len(), "{who}: {:?}", seen.keys());
    for p in parts {
        assert_eq!(seen[*p].first().copied(), Some(from), "{who}: first of {p}");
        assert_eq!(seen[*p].len() as u64, end - from, "{who}: count of {p}");
    }
}

/// What an ack answered, without the ids that differ between two queues.
fn verdicts(res: &[qp::AckResult]) -> Vec<(bool, bool, bool, Option<String>)> {
    res.iter()
        .map(|a| (a.success, a.noop, a.lease_released, a.error.clone()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn messages_without_rows_are_consumed_and_acked_like_those_with_rows() {
    let dir = std::env::temp_dir().join(unique("queen-rowless"));
    for (k, v) in [
        ("QUEEN_RAFT_DISK_HIGH_PCT", "100"),
        ("QUEEN_RAFT_DISK_LOW_PCT", "100"),
        ("QUEEN_RAFT_MAP_BYTES", "268435456"),
        ("QUEEN_RAFT_TXN_WINDOW_MIN_S", "1"),
        ("RETENTION_INTERVAL", "20"),
        ("QUEEN_RAFT_CLUSTER_VERSION_MS", "10"),
    ] {
        std::env::set_var(k, v);
    }
    let broker = Broker::start(BrokerConfig::new().raft(&dir))
        .await
        .expect("broker start");
    for _ in 0..1_000 {
        if broker.health().await.expect("health")["raft"]["clusterVersion"].as_u64() >= Some(6) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let health = broker.health().await.expect("health");
    assert_eq!(health["raft"]["clusterVersion"], 6, "{health}");

    let (rowed, rowless) = (unique("rowed"), unique("rowless"));
    let parts = ["a", "b"];
    for (q, window) in [(&rowed, 3600), (&rowless, 0)] {
        broker
            .configure(
                &qp::ConfigureRequest::new(q.clone()).options(qp::QueueOptions {
                    dedup_window_seconds: Some(window),
                    ..Default::default()
                }),
            )
            .await
            .expect("configure");
    }
    // 40 messages a partition, in appends of 1, 2, 3, ... messages.
    let mut appends = 0;
    for p in parts {
        let (mut at, mut n) = (0, 1);
        while at < 40 {
            let now = n.min(40 - at);
            for q in [&rowed, &rowless] {
                push(&broker, q, p, at, now).await;
            }
            appends += 1;
            at += now;
            n += 1;
        }
    }
    // One queue keeps a row per append, the other none.
    wait_rows(&broker, appends).await;

    // The same acks on both queues, by a group that reads five at a time.
    let g = "repeat";
    let mut answers: HashMap<&str, Vec<Vec<(bool, bool, bool, Option<String>)>>> = HashMap::new();
    for q in [&rowed, &rowless] {
        let out = answers.entry(q.as_str()).or_default();
        let one_part = qp::PopParams {
            partitions: Some(1),
            ..params(g, 5, false)
        };
        let first = pop(&broker, q, &one_part).await;
        assert_eq!(first.messages.len(), 5, "{first:?}");
        let batch = ack_items(&first.messages, true, qp::AckStatus::Completed);
        // The batch with its lease, then the same ack again (its answer lost).
        out.push(verdicts(&ack(&broker, g, batch.clone()).await));
        out.push(verdicts(&ack(&broker, g, batch).await));
        // One of them alone with no lease, then a failure of it.
        let third = &first.messages[2..3];
        out.push(verdicts(
            &ack(
                &broker,
                g,
                ack_items(third, false, qp::AckStatus::Completed),
            )
            .await,
        ));
        out.push(verdicts(
            &ack(&broker, g, ack_items(third, false, qp::AckStatus::Failed)).await,
        ));
        // An id nobody pushed.
        let mut ghost = ack_items(third, false, qp::AckStatus::Completed);
        ghost[0].transaction_id = "ghost".into();
        out.push(verdicts(&ack(&broker, g, ghost).await));
        // The partition goes on where the batch ended, and a message of the
        // new batch is acked alone with no lease: the cursor moves to it.
        let next = pop(&broker, q, &one_part).await;
        assert_eq!(next.messages.len(), 5, "{next:?}");
        assert_eq!(next.messages[0].partition, first.messages[0].partition);
        assert_eq!(
            next.messages[0].data["n"].as_u64(),
            first.messages[4].data["n"].as_u64().map(|n| n + 1)
        );
        out.push(verdicts(
            &ack(
                &broker,
                g,
                ack_items(&next.messages[..2], false, qp::AckStatus::Completed),
            )
            .await,
        ));
        out.push(verdicts(
            &ack(
                &broker,
                g,
                ack_items(&next.messages[2..], true, qp::AckStatus::Completed),
            )
            .await,
        ));
    }
    let taken = &answers[rowless.as_str()];
    assert!(
        taken[0].iter().all(|a| a.0 && !a.1),
        "the batch: {:?}",
        taken[0]
    );
    assert!(
        taken[1].iter().all(|a| a.0),
        "the batch again: {:?}",
        taken[1]
    );
    assert!(
        taken[2][0].0 && taken[2][0].1,
        "acked before, no lease: {:?}",
        taken[2]
    );
    assert!(
        !taken[3][0].0,
        "a failure of an acked message: {:?}",
        taken[3]
    );
    assert!(!taken[4][0].0, "an unknown id: {:?}", taken[4]);
    assert!(
        taken[5].iter().all(|a| a.0 && !a.1),
        "no lease, ahead: {:?}",
        taken[5]
    );
    assert_eq!(
        answers[rowless.as_str()],
        answers[rowed.as_str()],
        "the same answers with and without rows"
    );

    // Whole backlogs: a leased group and an auto-ack group on each queue.
    for q in [&rowed, &rowless] {
        expect(
            &drain(&broker, q, "leased", 7, false).await,
            &parts,
            0,
            40,
            q,
        );
        expect(&drain(&broker, q, "auto", 33, true).await, &parts, 0, 40, q);
    }
    let cold = series(&broker, "queen_consume_cold_claims_total").await;
    assert!(cold >= 4.0, "claims served from the queue log: {cold}");
    assert_eq!(
        txns_rows(&broker).await,
        appends,
        "nothing came back into the store"
    );

    // Pushes while a group reads, then the groups that were at the end.
    let more = async {
        for round in 0..5u64 {
            for p in parts {
                for q in [&rowed, &rowless] {
                    push(&broker, q, p, 40 + round * 3, 3).await;
                }
            }
        }
    };
    let ((), during) = tokio::join!(more, drain(&broker, &rowless, "during", 9, false));
    let mut during = during;
    for (p, rest) in drain(&broker, &rowless, "during", 9, false).await {
        let got = during.entry(p).or_default();
        assert_eq!(rest.first().copied(), got.last().map(|n| n + 1).or(Some(0)));
        got.extend(rest);
    }
    expect(&during, &parts, 0, 55, "during the pushes");
    for q in [&rowed, &rowless] {
        expect(
            &drain(&broker, q, "leased", 7, false).await,
            &parts,
            40,
            55,
            q,
        );
    }

    broker.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
