//! Kafka-facade consumer groups in the console's reads, end to end through
//! the real [`RaftFacade`] over a throwaway data directory per test, like
//! `positions.rs`.
//!
//! A Kafka consumer commits POSITIONS and never pops, so its cursors say
//! nothing about it: the listing used to call every Kafka group `Dead`. The
//! facade now writes the group's presence in the tenant's KV
//! (`qk:live:<group>`, queen-kafka presence.rs) beside the group index it
//! already wrote (`qk:groups:<group>`), and the reads report from them
//! (phase2/reads/kafka_groups.rs). Here the facade's rows are written as it
//! writes them, through the KV route, and what is proved is:
//! - a group the index names is `kind: "kafka"`, and without presence it is
//!   reported exactly as before (`Dead`, the new fields null);
//! - with members present it is `Stable` with its live members, last seen
//!   now and inactive for 0 s;
//! - a group whose last member left is `Empty`, dated, never 0 s inactive;
//! - a queue the members no longer subscribe to is `Empty` and undated;
//! - a native group is untouched;
//! - the group detail and the lagging list carry the same kind;
//! - the queue list marks the queues the facade created, pinned to their id.
//!
//! Freshness against time is the unit tests' (reads/kafka_groups.rs).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{json, Value};

use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{ApiReq, Deadline, KvReq, PushReq, ReqCtx, Rsm, RsmBuildCtx, TxnReq};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-kafka-groups-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn build_ctx(dir: &Path) -> RsmBuildCtx {
    RsmBuildCtx {
        data_dir: dir.display().to_string(),
        notifier: crate::notify::Notifier::new(false),
        disk_high_pct: 85.0,
        disk_low_pct: 80.0,
    }
}

fn ctx() -> ReqCtx {
    ReqCtx::new(
        crate::config::DEFAULT_TENANT,
        Deadline::after(Duration::from_secs(10)),
    )
}

fn parse(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("bad JSON body: {e}\n{body}"))
}

async fn push(f: &RaftFacade, queue: &str, partition: &str, n: usize) {
    let items: Vec<Value> = (0..n)
        .map(|i| json!({"queue": queue, "partition": partition, "payload": {"n": i}}))
        .collect();
    let out = f
        .push(
            ctx(),
            PushReq {
                raw: json!({ "items": items }).to_string().into_bytes(),
            },
        )
        .await
        .expect("push");
    for it in parse(&out.body).as_array().expect("push array") {
        assert_eq!(it["status"], "queued", "{it}");
    }
}

/// A Kafka commit as the facade sends it in a raft broker: positions in one
/// transaction (offset = the next record to read).
async fn commit(f: &RaftFacade, group: &str, queue: &str, partition: &str, offset: u64) {
    let out = f
        .transaction(
            ctx(),
            TxnReq {
                raw: json!({"positions": [{
                    "queue": queue,
                    "partition": partition,
                    "consumerGroup": group,
                    "offset": offset,
                    "metadata": "",
                }]})
                .to_string()
                .into_bytes(),
            },
        )
        .await
        .expect("transaction");
    let out = parse(&out.body);
    assert_eq!(out["success"], true, "{out}");
}

/// One KV row in the facade's namespace, kept for ever as the facade keeps it.
async fn put(f: &RaftFacade, key: &str, value: Value) {
    let out = f
        .kv(
            ctx(),
            KvReq {
                ops: vec![json!({
                    "op": "put",
                    "ns": "queen-kafka",
                    "key": key,
                    "value": value,
                    "forever": true,
                })],
            },
        )
        .await
        .unwrap_or_else(|e| panic!("kv put failed: {e:?}"));
    assert_eq!(out.results[0]["applied"], true, "{:?}", out.results);
}

async fn api(f: &RaftFacade, method: &str, path: &str, query: Option<&str>) -> Value {
    let out = f
        .api(
            ctx(),
            ApiReq {
                method: method.to_string(),
                path: path.to_string(),
                query: query.map(str::to_string),
                body: Vec::new(),
            },
        )
        .await
        .expect("api call");
    assert_eq!(out.status, 200, "{}", out.body);
    parse(&out.body)
}

/// The listing's row for (group, queue).
async fn row(f: &RaftFacade, group: &str, queue: &str) -> Value {
    let all = api(f, "GET", "/api/v1/consumer-groups", None).await;
    all.as_array()
        .expect("the listing is an array")
        .iter()
        .find(|r| r["name"] == group && r["queueName"] == queue)
        .unwrap_or_else(|| panic!("no row for {group} on {queue}: {all}"))
        .clone()
}

fn index(protocol_type: &str) -> Value {
    json!({"pt": protocol_type, "ts": 1_787_824_800_123i64})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_kafka_group_is_reported_by_its_members_and_not_by_its_cursors() {
    let dir = scratch("members");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    push(&f, "orders", "0", 5).await;
    commit(&f, "lg.consumer", "orders", "0", 3).await;

    // Positions alone: a group like any other, and the broker's old answer.
    let before = row(&f, "lg.consumer", "orders").await;
    assert_eq!(before["state"], "Dead", "{before}");
    assert_eq!(before["kind"], "queen", "{before}");
    assert_eq!(before["liveMembers"], Value::Null);
    assert_eq!(before["inactiveSeconds"], Value::Null);

    // The facade's index: a Kafka group, still reported as before without
    // presence (an older facade, or presence off).
    put(&f, "qk:groups:lg.consumer", index("consumer")).await;
    let indexed = row(&f, "lg.consumer", "orders").await;
    assert_eq!(indexed["kind"], "kafka", "{indexed}");
    assert_eq!(indexed["state"], "Dead", "{indexed}");
    assert_eq!(indexed["liveMembers"], Value::Null);
    assert_eq!(indexed["lastSeenAt"], Value::Null);
    assert_eq!(indexed["inactiveSeconds"], Value::Null);

    // Presence: two members heartbeating, subscribed to the queue.
    put(
        &f,
        "qk:live:lg.consumer",
        json!({"v": 1, "members": 2, "state": "Stable", "topics": {"orders": 2},
               "committing": false, "freshMs": 45_000, "source": "heartbeat"}),
    )
    .await;
    let live = row(&f, "lg.consumer", "orders").await;
    assert_eq!(live["state"], "Stable", "{live}");
    assert_eq!(live["kind"], "kafka");
    assert_eq!(live["liveMembers"], 2);
    assert_eq!(live["inactiveSeconds"], 0);
    assert!(live["lastSeenAt"].as_str().is_some(), "{live}");
    // What did not change meaning: `members` still counts partition cursors.
    assert_eq!(live["members"], 1);
    assert_eq!(live["partitionCursors"], 1);
    assert_eq!(live["totalLag"], 2, "offsets 3 and 4 are unread");

    // The last member left: Empty, dated, and never inactive for 0 s.
    put(
        &f,
        "qk:live:lg.consumer",
        json!({"v": 1, "members": 0, "state": "Empty", "topics": {"orders": 0},
               "committing": false, "freshMs": 45_000, "source": "leave"}),
    )
    .await;
    let left = row(&f, "lg.consumer", "orders").await;
    assert_eq!(left["state"], "Empty", "{left}");
    assert_eq!(left["liveMembers"], 0);
    assert!(
        left["inactiveSeconds"].as_i64().is_some_and(|s| s >= 1),
        "{left}"
    );
    assert!(left["lastSeenAt"].as_str().is_some(), "{left}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_queue_its_members_no_longer_read_is_empty_and_undated() {
    let dir = scratch("topics");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    push(&f, "orders", "0", 2).await;
    push(&f, "audit", "0", 2).await;
    commit(&f, "app", "orders", "0", 1).await;
    commit(&f, "app", "audit", "0", 1).await;
    put(&f, "qk:groups:app", index("consumer")).await;
    put(
        &f,
        "qk:live:app",
        json!({"v": 1, "members": 1, "topics": {"orders": 1}, "freshMs": 45_000}),
    )
    .await;

    let read = row(&f, "app", "orders").await;
    assert_eq!(read["state"], "Stable", "{read}");
    assert_eq!(read["liveMembers"], 1);
    let dropped = row(&f, "app", "audit").await;
    assert_eq!(dropped["state"], "Empty", "{dropped}");
    assert_eq!(dropped["liveMembers"], 0);
    assert_eq!(dropped["lastSeenAt"], Value::Null);
    assert_eq!(dropped["inactiveSeconds"], Value::Null);

    // A group with no members that commits (assign()) reads every queue.
    put(
        &f,
        "qk:live:app",
        json!({"v": 1, "members": 0, "committing": true, "topics": null, "freshMs": 45_000}),
    )
    .await;
    for queue in ["orders", "audit"] {
        let r = row(&f, "app", queue).await;
        assert_eq!(r["state"], "Stable", "{r}");
        assert_eq!(r["liveMembers"], 0, "{r}");
        assert_eq!(r["inactiveSeconds"], 0, "{r}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn the_detail_and_the_lagging_list_say_the_same_kind() {
    let dir = scratch("detail");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    push(&f, "orders", "0", 4).await;
    commit(&f, "k", "orders", "0", 1).await;
    commit(&f, "native", "orders", "0", 1).await;
    put(&f, "qk:groups:k", index("consumer")).await;
    put(
        &f,
        "qk:live:k",
        json!({"v": 1, "members": 3, "topics": {"orders": 3}, "freshMs": 45_000}),
    )
    .await;

    let detail = api(&f, "GET", "/api/v1/consumer-groups/k", None).await;
    let orders = &detail["orders"];
    assert_eq!(orders["kind"], "kafka", "{detail}");
    assert_eq!(orders["liveMembers"], 3);
    assert_eq!(orders["inactiveSeconds"], 0);
    assert_eq!(orders["partitions"].as_array().map(Vec::len), Some(1));
    let native = api(&f, "GET", "/api/v1/consumer-groups/native", None).await;
    assert_eq!(native["orders"]["kind"], "queen", "{native}");
    assert_eq!(native["orders"]["liveMembers"], Value::Null);

    let lagging = api(
        &f,
        "GET",
        "/api/v1/consumer-groups/lagging",
        Some("minLagSeconds=-1"),
    )
    .await;
    let kind_of = |g: &str| {
        lagging
            .as_array()
            .expect("an array")
            .iter()
            .find(|r| r["consumer_group"] == g)
            .map(|r| r["kind"].clone())
    };
    assert_eq!(kind_of("k"), Some(json!("kafka")), "{lagging}");
    assert_eq!(kind_of("native"), Some(json!("queen")), "{lagging}");

    // The native group's listing row is the one it always was.
    let n = row(&f, "native", "orders").await;
    assert_eq!(n["kind"], "queen");
    assert_eq!(n["state"], "Dead", "positions consume nothing: {n}");
    assert_eq!(n["liveMembers"], Value::Null);
    assert_eq!(n["lastSeenAt"], Value::Null);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn the_queue_list_marks_the_queues_the_facade_created() {
    let dir = scratch("topics-list");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    for q in ["orders", "recreated", "native", "pending"] {
        push(&f, q, "0", 1).await;
    }
    let listed = api(&f, "GET", "/api/v1/resources/queues", None).await;
    let id_of = |name: &str| {
        listed["queues"]
            .as_array()
            .expect("queues")
            .iter()
            .find(|q| q["name"] == name)
            .and_then(|q| q["id"].as_str())
            .unwrap_or_else(|| panic!("no id for {name}: {listed}"))
            .to_string()
    };
    let record = |qid: Value| json!({"qid": qid, "set": {}, "at": 1});
    put(&f, "qk:topiccfg:orders", record(json!(id_of("orders")))).await;
    // Dropped and recreated under the same name: the record is another queue's.
    put(
        &f,
        "qk:topiccfg:recreated",
        record(json!("00000000-0000-0000-0000-000000000000")),
    )
    .await;
    // Written ahead of a create that may have died: it marks nothing.
    put(
        &f,
        "qk:topiccfg:pending",
        json!({"qid": null, "set": {}, "at": 1, "pending": true}),
    )
    .await;

    let listed = api(&f, "GET", "/api/v1/resources/queues", None).await;
    let mark = |name: &str| {
        listed["queues"]
            .as_array()
            .expect("queues")
            .iter()
            .find(|q| q["name"] == name)
            .map(|q| q["kafkaTopic"].clone())
    };
    assert_eq!(mark("orders"), Some(json!(true)), "{listed}");
    assert_eq!(mark("recreated"), Some(json!(false)));
    assert_eq!(mark("native"), Some(json!(false)));
    assert_eq!(mark("pending"), Some(json!(false)));
}
