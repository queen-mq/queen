//! Messages that outlive their `txns` rows (the rows watermark, catalogue
//! version 6), on a whole node: facade, planner, apply, the background
//! retention scanner, the consumption engine and the queue log.
//!
//! The node runs with no minimum row window and a scan every 20 ms, and the
//! queues keep their messages for ever (retention is off) with a dedup window
//! of zero, so a push's rows are gone a moment after it is logged while every
//! message stays. What the tests prove from there:
//!
//! - [`messages_without_rows_are_delivered_acked_and_sought_from_the_queue_log`]:
//!   the store holds no `txns` row and every partition's `rows_start` is its
//!   end; a leased group, an auto-ack group and queue mode each get every
//!   message, in order, with the payload and `transactionId` it was pushed
//!   with, and every ack is taken; pushes that land while a group reads are
//!   delivered after the old messages with no gap; a group that subscribes at
//!   an instant starts at the first message stamped at or after it; and a
//!   restart serves the same messages again to a new group.
//! - [`a_repeated_ack_is_answered_the_same_with_and_without_rows`]: the same
//!   acks (a batch with its lease, the batch again, one message with no
//!   lease, a failure of an acked message, an unknown id, and the repeats
//!   after a restart) get the same answers on a queue whose rows are gone as
//!   on one that still has them.
//! - [`retention_removes_messages_without_rows_and_frees_their_bytes`]: once
//!   retention is turned on, the messages go by their age read from the queue
//!   log, the partitions' `log_start` reaches their end and the byte counters
//!   return to zero.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{json, Value};

use crate::rsm::batcher::BatcherConfig;
use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{
    AckReq, ApiReq, Deadline, PopOptions, PopReq, PushReq, ReqCtx, Rsm, RsmBuildCtx, RsmError,
};
use crate::rsm::store::rows::PartitionRow;
use crate::rsm::store::{Keyspace, Reads, Store, TypedReads};

const T: &str = crate::config::DEFAULT_TENANT;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-rowless-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// A node whose rows live for the queue's own window and nothing more, judged
/// every 20 ms, and whose cluster version rises as soon as it can.
fn open(dir: &Path) -> RaftFacade {
    let mut cfg = BatcherConfig::from_env();
    cfg.maintenance_every_ms = 20;
    cfg.cluster_version_every_ms = 10;
    cfg.maintenance.txn_window_min_s = 0;
    RaftFacade::open_with(
        &RsmBuildCtx {
            data_dir: dir.display().to_string(),
            notifier: crate::notify::Notifier::new(false),
            disk_high_pct: 100.0,
            disk_low_pct: 99.0,
        },
        cfg,
    )
    .expect("open facade")
}

fn ctx() -> ReqCtx {
    ReqCtx::new(T, Deadline::after(Duration::from_secs(5)))
}

fn parse(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("bad JSON body: {e}\n{body}"))
}

async fn configure(facade: &RaftFacade, q: &str, options: Value) {
    let out = facade
        .api(
            ctx(),
            ApiReq {
                method: "POST".into(),
                path: "/api/v1/configure".into(),
                query: None,
                body: serde_json::to_vec(&json!({ "queue": q, "options": options })).unwrap(),
            },
        )
        .await
        .expect("configure");
    assert_eq!(out.status, 200, "{}", out.body);
}

/// Push `n` messages to `(q, part)` in one request, numbered from `from`: the
/// tests push each partition in order from 0, so a message's number is its
/// offset.
async fn push(facade: &RaftFacade, q: &str, part: &str, from: u64, n: u64) {
    let items: Vec<Value> = (from..from + n)
        .map(|i| {
            json!({
                "queue": q,
                "partition": part,
                "payload": { "p": part, "n": i },
                "transactionId": format!("{part}-{i}"),
            })
        })
        .collect();
    let out = facade
        .push(
            ctx(),
            PushReq {
                raw: serde_json::to_vec(&json!({ "items": items })).unwrap(),
            },
        )
        .await
        .expect("push");
    let body = parse(&out.body);
    let res = body.as_array().expect("push array");
    assert_eq!(res.len() as u64, n);
    for (i, it) in res.iter().enumerate() {
        assert_eq!(it["status"], "queued", "{it}");
        assert_eq!(it["offset"].as_u64(), Some(from + i as u64), "{it}");
    }
}

/// `(txns rows in the whole store, each partition's row)`.
fn state(facade: &RaftFacade, q: &str, parts: &[&str]) -> (u64, Vec<PartitionRow>) {
    let store = facade.store_for_test();
    store
        .read(|r| {
            let mut rows = Vec::new();
            for p in parts {
                let pid = r.pid_of(T, q, p)?.expect("the partition exists");
                rows.push(r.partition(pid)?.expect("its row"));
            }
            Ok((r.count(Keyspace::Txns)?, rows))
        })
        .expect("read the store")
}

/// Wait until every row of `(q, parts)` is gone while each partition still
/// starts where `log_start` says.
async fn rows_gone(facade: &RaftFacade, q: &str, parts: &[&str], log_start: u64) {
    rows_left(facade, q, parts, log_start, 0).await
}

/// [`rows_gone`] on a node where other queues keep `left` rows.
async fn rows_left(facade: &RaftFacade, q: &str, parts: &[&str], log_start: u64, left: u64) {
    let mut last = None;
    for _ in 0..1_500 {
        let (rows, ps) = state(facade, q, parts);
        if rows == left
            && ps
                .iter()
                .all(|p| p.rows_start == (p.last_offset + 1) as u64 && p.log_start == log_start)
        {
            return;
        }
        last = Some((rows, ps));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the rows of {q} never left: {last:?}");
}

async fn pop(
    facade: &RaftFacade,
    q: &str,
    group: Option<&str>,
    batch: u32,
    auto_ack: bool,
    options: &PopOptions,
) -> Option<Value> {
    for _ in 0..400 {
        let out = facade
            .pop_wildcard(
                ctx(),
                PopReq {
                    queue: q.into(),
                    group: group.map(str::to_string),
                    batch,
                    auto_ack,
                    wait: false,
                    timeout_ms: 1_000,
                    options: options.clone(),
                },
            )
            .await;
        match out {
            Ok(out) if out.empty => return None,
            Ok(out) => return Some(parse(&out.body)),
            // A claim whose queue-log reads were still on their way.
            Err(RsmError::Retry { .. }) => tokio::time::sleep(Duration::from_millis(5)).await,
            Err(e) => panic!("pop of {q}: {e:?}"),
        }
    }
    panic!("the pop of {q} was told to retry for ever");
}

fn all() -> PopOptions {
    PopOptions {
        subscription_mode: "all".into(),
        max_parts: 2,
        ..Default::default()
    }
}

/// Pop `q` as `group` until it stays empty, acking each batch unless the pop
/// acked it. Every message must be the one pushed at its offset, and every
/// partition must come out in order with no gap after its first message.
/// The offsets delivered, per partition.
async fn drain(
    facade: &RaftFacade,
    q: &str,
    group: Option<&str>,
    batch: u32,
    auto_ack: bool,
    options: &PopOptions,
) -> HashMap<String, Vec<u64>> {
    let mut seen: HashMap<String, Vec<u64>> = HashMap::new();
    let mut empty = 0;
    while empty < 3 {
        let Some(body) = pop(facade, q, group, batch, auto_ack, options).await else {
            empty += 1;
            tokio::time::sleep(Duration::from_millis(2)).await;
            continue;
        };
        empty = 0;
        let msgs = body["messages"].as_array().expect("messages");
        assert!(!msgs.is_empty(), "{body}");
        let mut acks = Vec::new();
        for m in msgs {
            let part = m["partition"].as_str().expect("partition").to_string();
            let off = m["offset"].as_u64().expect("offset");
            assert_eq!(
                m["data"]["n"].as_u64(),
                Some(off),
                "payload of {part}@{off}: {m}"
            );
            assert_eq!(m["data"]["p"], part.as_str(), "{m}");
            assert_eq!(m["transactionId"], format!("{part}-{off}"), "{m}");
            let got = seen.entry(part.clone()).or_default();
            if let Some(prev) = got.last() {
                assert_eq!(
                    off,
                    prev + 1,
                    "{part}: {off} after {prev} (group {group:?})"
                );
            }
            got.push(off);
            let lease = m["leaseId"]
                .as_str()
                .or_else(|| body["leaseId"].as_str())
                .unwrap_or_default();
            assert!(
                auto_ack || !lease.is_empty(),
                "a leased pop names its lease: {body}"
            );
            acks.push(json!({
                "transactionId": m["transactionId"],
                "partitionId": m["partitionId"].as_str().or_else(|| body["partitionId"].as_str()),
                "leaseId": lease,
                "status": "completed",
            }));
        }
        if auto_ack {
            continue;
        }
        let g = group.unwrap_or("__QUEUE_MODE__");
        let out = facade
            .ack(
                ctx(),
                AckReq {
                    queue: Some(q.into()),
                    group: g.into(),
                    raw: serde_json::to_vec(
                        &json!({ "consumerGroup": g, "acknowledgments": acks }),
                    )
                    .unwrap(),
                },
            )
            .await
            .expect("ack");
        let res = parse(&out.body);
        let res = res.as_array().expect("ack array");
        assert_eq!(res.len(), acks.len());
        for r in res {
            assert_eq!(r["success"], true, "ack refused: {r}");
        }
    }
    seen
}

fn expect_all(seen: &HashMap<String, Vec<u64>>, parts: &[&str], from: u64, end: u64, who: &str) {
    assert_eq!(seen.len(), parts.len(), "{who}: {:?}", seen.keys());
    for p in parts {
        let got = &seen[*p];
        assert_eq!(got.first().copied(), Some(from), "{who}: first of {p}");
        assert_eq!(got.len() as u64, end - from, "{who}: count of {p}");
    }
}

async fn version_six(facade: &RaftFacade) {
    for _ in 0..1_000 {
        if facade.cluster_version() >= u32::from(crate::rsm::effect::VERSION_6) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the cluster version stayed at {}", facade.cluster_version());
}

fn wall_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros() as i64
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn messages_without_rows_are_delivered_acked_and_sought_from_the_queue_log() {
    let dir = scratch("consume");
    let facade = open(&dir);
    let (q, parts) = ("old", ["a", "b", "c"]);
    configure(&facade, q, json!({ "dedupWindowSeconds": 0 })).await;
    version_six(&facade).await;

    // 60 messages a partition, in appends of 1, 2, ... so that a pop's batch
    // never lines up with them.
    for p in parts {
        let (mut at, mut n) = (0, 1);
        while at < 60 {
            let n_now = n.min(60 - at);
            push(&facade, q, p, at, n_now).await;
            at += n_now;
            n += 1;
        }
    }
    rows_gone(&facade, q, &parts, 0).await;

    // A pop reads the queue log for the parts it claims, not for every part
    // that has something: four messages come from one partition.
    let before = facade.cold_claims_for_test();
    let body = pop(&facade, q, Some("narrow"), 4, true, &all())
        .await
        .expect("a batch");
    assert_eq!(body["messages"].as_array().map(Vec::len), Some(4), "{body}");
    let read = facade.cold_claims_for_test() - before;
    assert!(
        (1..parts.len() as u64).contains(&read),
        "{read} parts read ahead for one claim"
    );

    // A leased group, an auto-ack group and queue mode: each reads everything.
    let seen = drain(&facade, q, Some("leased"), 7, false, &all()).await;
    expect_all(&seen, &parts, 0, 60, "leased");
    let seen = drain(&facade, q, Some("auto"), 50, true, &all()).await;
    expect_all(&seen, &parts, 0, 60, "auto");
    let seen = drain(&facade, q, None, 13, false, &all()).await;
    expect_all(&seen, &parts, 0, 60, "queue mode");
    // 180 messages each, none of them through a row.
    let cold = facade.cold_claims_for_test();
    assert!(cold >= 3 * 3, "claims served from the queue log: {cold}");

    // An instant between two pushes, then more messages. Whether a group
    // reads them from rows or from the queue log depends on when the scanner
    // came by, and must not show.
    tokio::time::sleep(Duration::from_millis(60)).await;
    let instant = wall_us();
    tokio::time::sleep(Duration::from_millis(60)).await;
    let reader = {
        let facade = &facade;
        async move { drain(facade, q, Some("during"), 9, false, &all()).await }
    };
    let writer = {
        let facade = &facade;
        async move {
            for round in 0..10u64 {
                for p in parts {
                    push(facade, q, p, 60 + round * 4, 4).await;
                }
            }
        }
    };
    let (mut seen, ()) = tokio::join!(reader, writer);
    // What the reader's last empty pops missed while the writer went on.
    for (p, more) in drain(&facade, q, Some("during"), 9, false, &all()).await {
        let got = seen.entry(p.clone()).or_default();
        assert_eq!(
            more.first().copied(),
            got.last().map(|o| o + 1).or(Some(0)),
            "{p}"
        );
        got.extend(more);
    }
    expect_all(&seen, &parts, 0, 100, "during the pushes");
    let seen = drain(&facade, q, Some("leased"), 7, false, &all()).await;
    expect_all(&seen, &parts, 60, 100, "the first group, later");

    // A group that subscribes at the instant starts at the first message
    // stamped at or after it, found in the queue log.
    rows_gone(&facade, q, &parts, 0).await;
    let from = PopOptions {
        subscription_from_us: Some(instant),
        ..all()
    };
    let seen = drain(&facade, q, Some("since"), 11, false, &from).await;
    expect_all(&seen, &parts, 60, 100, "from the instant");

    // The same data after a restart: nothing was rebuilt in the store.
    facade.shutdown().await;
    let facade = open(&dir);
    let (rows, ps) = state(&facade, q, &parts);
    assert_eq!(rows, 0);
    for p in &ps {
        assert_eq!((p.log_start, p.rows_start, p.last_offset), (0, 100, 99));
    }
    let seen = drain(&facade, q, Some("after-restart"), 33, true, &all()).await;
    expect_all(&seen, &parts, 0, 100, "after a restart");

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

async fn ack(facade: &RaftFacade, q: &str, group: &str, items: &[Value]) -> Value {
    let out = facade
        .ack(
            ctx(),
            AckReq {
                queue: Some(q.into()),
                group: group.into(),
                raw: serde_json::to_vec(
                    &json!({ "consumerGroup": group, "acknowledgments": items }),
                )
                .unwrap(),
            },
        )
        .await
        .expect("ack");
    parse(&out.body)
}

/// Pop five messages of `(q, "p")` as `group`: `(partition id, lease id,
/// their offsets)`.
async fn five(facade: &RaftFacade, q: &str, group: &str) -> (String, String, Vec<u64>) {
    let body = pop(facade, q, Some(group), 5, false, &all())
        .await
        .unwrap_or_else(|| panic!("{q}: a batch"));
    let msgs = body["messages"].as_array().expect("messages");
    let id = |k: &str| {
        msgs[0][k]
            .as_str()
            .or_else(|| body[k].as_str())
            .unwrap_or_else(|| panic!("{k}: {body}"))
            .to_string()
    };
    let offs = msgs
        .iter()
        .map(|m| m["offset"].as_u64().expect("offset"))
        .collect();
    (id("partitionId"), id("leaseId"), offs)
}

fn leased(pid: &str, lease: &str, offs: &[u64], status: &str) -> Vec<Value> {
    offs.iter()
        .map(|o| {
            json!({
                "transactionId": format!("p-{o}"),
                "partitionId": pid,
                "leaseId": lease,
                "status": status,
            })
        })
        .collect()
}

fn unleased(pid: &str, txn: &str, status: &str) -> Vec<Value> {
    vec![json!({ "transactionId": txn, "partitionId": pid, "status": status })]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_repeated_ack_is_answered_the_same_with_and_without_rows() {
    let dir = scratch("repeat");
    let facade = open(&dir);
    // The same pushes to two queues: one keeps its rows for an hour, the
    // other loses them at once.
    let (rowed, rowless, g) = ("rowed", "rowless", "workers");
    configure(&facade, rowed, json!({ "dedupWindowSeconds": 3600 })).await;
    configure(&facade, rowless, json!({ "dedupWindowSeconds": 0 })).await;
    version_six(&facade).await;
    for q in [rowed, rowless] {
        push(&facade, q, "p", 0, 6).await;
        push(&facade, q, "p", 6, 6).await;
    }
    rows_left(&facade, rowless, &["p"], 0, 2).await;
    let (rows, ps) = state(&facade, rowed, &["p"]);
    assert_eq!(
        (rows, ps[0].rows_start),
        (2, 0),
        "the other queue keeps its rows"
    );

    let mut answers: HashMap<&str, Vec<Value>> = HashMap::new();
    let mut held: HashMap<&str, (String, String)> = HashMap::new();
    for q in [rowed, rowless] {
        let out = answers.entry(q).or_default();
        let (pid, lease, offs) = five(&facade, q, g).await;
        assert_eq!(offs, [0, 1, 2, 3, 4], "{q}");
        // The batch with its lease, then the same ack again (its answer lost).
        out.push(ack(&facade, q, g, &leased(&pid, &lease, &offs, "completed")).await);
        out.push(ack(&facade, q, g, &leased(&pid, &lease, &offs, "completed")).await);
        // One of them alone with no lease, a failure of it, and an id nobody pushed.
        out.push(ack(&facade, q, g, &unleased(&pid, "p-2", "completed")).await);
        out.push(ack(&facade, q, g, &unleased(&pid, "p-2", "failed")).await);
        out.push(ack(&facade, q, g, &unleased(&pid, "ghost", "completed")).await);
        // None of it moved the cursor: the next batch follows the first.
        let (_, lease2, offs2) = five(&facade, q, g).await;
        assert_eq!(offs2, [5, 6, 7, 8, 9], "{q}");
        out.push(ack(&facade, q, g, &leased(&pid, &lease2, &offs2, "completed")).await);
        held.insert(q, (pid, lease));
    }
    let taken = &answers[rowless];
    for r in taken[0].as_array().expect("array") {
        assert_eq!(
            (&r["success"], &r["noop"]),
            (&json!(true), &json!(false)),
            "{r}"
        );
    }
    for r in taken[1].as_array().expect("array") {
        assert_eq!(r["success"], true, "the batch again: {r}");
    }
    assert_eq!(
        taken[2][0]["success"], true,
        "an acked message, no lease: {}",
        taken[2]
    );
    assert_eq!(taken[2][0]["noop"], true, "{}", taken[2]);
    assert_eq!(
        taken[3][0]["success"], false,
        "a failure of an acked message: {}",
        taken[3]
    );
    assert_eq!(taken[4][0]["success"], false, "an unknown id: {}", taken[4]);
    assert_eq!(
        answers[rowless], answers[rowed],
        "the same answers with rows"
    );

    // A new leader remembers no lease: the repeats are answered from what
    // the log and the store hold, the same on both queues.
    facade.shutdown().await;
    let facade = open(&dir);
    let mut after: HashMap<&str, Vec<Value>> = HashMap::new();
    for q in [rowed, rowless] {
        let (pid, lease) = &held[q];
        let out = after.entry(q).or_default();
        out.push(
            ack(
                &facade,
                q,
                g,
                &leased(pid, lease, &[0, 1, 2, 3, 4], "completed"),
            )
            .await,
        );
        out.push(ack(&facade, q, g, &unleased(pid, "p-7", "completed")).await);
        out.push(ack(&facade, q, g, &unleased(pid, "ghost", "completed")).await);
        let seen = drain(&facade, q, Some(g), 5, false, &all()).await;
        assert_eq!(seen["p"], [10, 11], "{q}: the rest, once");
    }
    assert_eq!(
        after[rowless][1][0]["success"], true,
        "{}",
        after[rowless][1]
    );
    assert_eq!(after[rowless][1][0]["noop"], true, "{}", after[rowless][1]);
    assert_eq!(
        after[rowless], after[rowed],
        "the same answers after a restart"
    );

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn retention_removes_messages_without_rows_and_frees_their_bytes() {
    let dir = scratch("retention");
    let facade = open(&dir);
    let (q, parts) = ("aged", ["x", "y"]);
    configure(&facade, q, json!({ "dedupWindowSeconds": 0 })).await;
    version_six(&facade).await;
    for p in parts {
        for at in (0..40).step_by(5) {
            push(&facade, q, p, at, 5).await;
        }
    }
    rows_gone(&facade, q, &parts, 0).await;
    let (_, before) = state(&facade, q, &parts);
    for p in &before {
        assert!(p.unrowed_bytes > 0, "the rows' bytes stay counted: {p:?}");
    }

    // Retention on: the messages are a second old at most, so nothing goes
    // at first, and all of it goes once they are older than that.
    configure(
        &facade,
        q,
        json!({ "dedupWindowSeconds": 0, "retentionEnabled": true, "retentionSeconds": 1 }),
    )
    .await;
    let mut last = None;
    for _ in 0..600 {
        let (rows, ps) = state(&facade, q, &parts);
        if ps.iter().all(|p| p.log_start == 40) {
            assert_eq!(rows, 0);
            for p in &ps {
                assert_eq!(
                    (p.rows_start, p.txns_start, p.unrowed_bytes),
                    (40, 40, 0),
                    "{p:?}"
                );
            }
            last = None;
            break;
        }
        last = Some(ps);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        last.is_none(),
        "retention never removed the messages: {last:?}"
    );
    assert!(
        drain(&facade, q, Some("late"), 10, true, &all())
            .await
            .is_empty(),
        "nothing is left to read"
    );

    // New messages after it are ordinary ones.
    for p in parts {
        push(&facade, q, p, 40, 3).await;
    }
    let seen = drain(&facade, q, Some("late"), 10, true, &all()).await;
    expect_all(&seen, &parts, 40, 43, "after retention");

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
