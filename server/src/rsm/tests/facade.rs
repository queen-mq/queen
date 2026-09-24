//! WP-1.7c — the real [`RaftFacade`] end to end (PLAN_RAFT.md §9.1, §7.5, D7).
//!
//! These build the facade DIRECTLY (not through the process-global builder hook,
//! which the boot paths register and the WP-1.7a seam tests must not see), each
//! over its own throwaway data directory, and drive the message path the way a
//! receiver does: a wire-shaped body in, the rendered wire body out, and — for a
//! pop — the payload bytes read back off this node's own segment files.
//!
//! What they prove:
//! - push renders `queued` verdicts with offsets, and a retry with the same
//!   `transactionId` renders `duplicate` at the ORIGINAL offset (the dedup smoke
//!   the WP asks for);
//! - a queue-mode pop claims the pushed messages, renders every wire field, and
//!   the payloads come back off the files (D7);
//! - renew reports the live lease; ack advances the cursor and a second pop is
//!   empty;
//! - a `kill`-free restart (shutdown + reopen of the same directory) recovers the
//!   pushed state — the facade over the LocalReplicator's recovery (§11.5).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::Value;

use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{
    AckReq, ApiReq, Deadline, PopReq, PushReq, RenewReq, ReqCtx, Rsm, RsmBuildCtx,
};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    // A small store map keeps the sparse file tiny on a laptop (§0.3 smoke).
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-facade-{tag}-{}-{}",
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
    }
}

fn ctx() -> ReqCtx {
    ReqCtx::new(
        crate::config::DEFAULT_TENANT,
        Deadline::after(Duration::from_secs(5)),
    )
}

fn parse(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("bad JSON body: {e}\n{body}"))
}

/// Every dead letter filed on this node, once at least `want` have become
/// visible to a fresh read. The reply to an ack is delivered on `applied()`
/// (I4) while the store write txn stays open; the commit that makes the row
/// visible to an independent read follows on the ratified store-commit cadence
/// (`QUEEN_RAFT_STORE_COMMIT_MS`, 4 ms), so a read the instant the ack returns
/// can miss it. Poll briefly rather than race the cadence.
async fn dlq_rows_settled(
    facade: &RaftFacade,
    want: usize,
) -> Vec<crate::rsm::store::rows::DlqRow> {
    let mut rows = facade.dlq_rows();
    for _ in 0..400 {
        if rows.len() >= want {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        rows = facade.dlq_rows();
    }
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn push_pop_renew_ack_render_end_to_end() {
    let dir = scratch("e2e");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    // ---- push three messages to one queue (queue implicitly created) --------
    let push = facade
        .push(
            ctx(),
            PushReq {
                raw: br#"{"items":[
                    {"queue":"orders","payload":{"n":1},"transactionId":"t1"},
                    {"queue":"orders","payload":{"n":2},"transactionId":"t2"},
                    {"queue":"orders","payload":{"n":3},"transactionId":"t3"}
                ]}"#
                .to_vec(),
            },
        )
        .await
        .expect("push");
    let arr = parse(&push.body);
    let items = arr.as_array().expect("push array");
    assert_eq!(items.len(), 3);
    for (i, it) in items.iter().enumerate() {
        assert_eq!(it["status"], "queued", "item {i}: {it}");
        assert_eq!(it["offset"].as_u64(), Some(i as u64), "gapless offsets");
        assert!(it["message_id"].as_str().is_some_and(|s| !s.is_empty()));
    }

    // ---- pop them (queue mode seeds `all`, so it sees the backlog) -----------
    let popped = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "orders".into(),
                group: None,
                batch: 10,
                auto_ack: false,
                wait: false,
                timeout_ms: 1000,
                options: Default::default(),
            },
        )
        .await
        .expect("pop");
    assert!(!popped.empty, "the pop must claim the backlog");
    let pop = parse(&popped.body);
    let msgs = pop["messages"].as_array().expect("messages");
    assert_eq!(msgs.len(), 3, "three messages: {}", popped.body);
    // Payloads come back off this node's files (D7), in order.
    assert_eq!(msgs[0]["data"]["n"], 1);
    assert_eq!(msgs[1]["data"]["n"], 2);
    assert_eq!(msgs[2]["data"]["n"], 3);
    assert_eq!(msgs[0]["transactionId"], "t1");
    assert_eq!(msgs[0]["offset"].as_u64(), Some(0));
    let partition_id = pop["partitionId"]
        .as_str()
        .expect("partitionId")
        .to_string();
    let lease_id = pop["leaseId"].as_str().expect("leaseId").to_string();
    assert!(
        !lease_id.is_empty(),
        "a leased (non-autoAck) pop has a leaseId"
    );

    // ---- renew the live lease ------------------------------------------------
    let renew = facade
        .renew(
            ctx(),
            RenewReq {
                lease_id: lease_id.clone(),
                seconds: 30,
            },
        )
        .await
        .expect("renew");
    let rn = parse(&renew.body);
    assert_eq!(rn["success"], true, "renew of a live lease: {}", renew.body);
    assert!(rn["renewed"].as_i64().unwrap_or(0) >= 1);

    // An unknown transaction is a resolved request with a negative verdict,
    // not a successful noop. Keep the wording both SDK conformance suites use
    // as their retry-safety signal.
    let unknown = facade
        .ack(
            ctx(),
            AckReq {
                queue: Some("orders".into()),
                group: "__QUEUE_MODE__".into(),
                raw: format!(
                    r#"{{"transactionId":"ghost","partitionId":"{partition_id}","status":"completed","leaseId":"{lease_id}"}}"#
                )
                .into_bytes(),
            },
        )
        .await
        .expect("unknown ack");
    let unknown = parse(&unknown.body);
    assert_eq!(unknown[0]["success"], false, "{unknown}");
    assert!(
        unknown[0]["error"]
            .as_str()
            .is_some_and(|error| error.contains("unresolv")),
        "{unknown}"
    );

    // ---- ack all three -------------------------------------------------------
    let ack_body = format!(
        r#"{{"consumerGroup":"__QUEUE_MODE__","acknowledgments":[
            {{"transactionId":"t1","partitionId":"{pid}","status":"completed","leaseId":"{lease}"}},
            {{"transactionId":"t2","partitionId":"{pid}","status":"completed","leaseId":"{lease}"}},
            {{"transactionId":"t3","partitionId":"{pid}","status":"completed","leaseId":"{lease}"}}
        ]}}"#,
        pid = partition_id,
        lease = lease_id,
    );
    let acked = facade
        .ack(
            ctx(),
            AckReq {
                queue: Some("orders".into()),
                group: "__QUEUE_MODE__".into(),
                raw: ack_body.into_bytes(),
            },
        )
        .await
        .expect("ack");
    let ack = parse(&acked.body);
    let results = ack.as_array().expect("ack array");
    assert_eq!(results.len(), 3);
    for (i, r) in results.iter().enumerate() {
        assert_eq!(r["success"], true, "ack result: {r}");
        assert_eq!(
            r["transactionId"],
            format!("t{}", i + 1),
            "ack echoes the txn"
        );
    }

    // ---- a second pop finds nothing ------------------------------------------
    let empty = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "orders".into(),
                group: None,
                batch: 10,
                auto_ack: false,
                wait: false,
                timeout_ms: 1000,
                options: Default::default(),
            },
        )
        .await
        .expect("second pop");
    assert!(empty.empty, "everything was acked: {}", empty.body);

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_retry_with_the_same_transaction_id_is_a_duplicate() {
    let dir = scratch("dedup");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    let body = br#"{"items":[{"queue":"q","payload":{"v":1},"transactionId":"dup"}]}"#.to_vec();

    let first = facade
        .push(ctx(), PushReq { raw: body.clone() })
        .await
        .expect("push 1");
    let f = parse(&first.body);
    assert_eq!(f[0]["status"], "queued");
    assert_eq!(f[0]["offset"].as_u64(), Some(0));

    // A fresh HTTP push of the same transactionId: the dedup window (003) makes
    // it a duplicate at the ORIGINAL offset. (A new request id per push, so this
    // is txn dedup, not request-id dedup.)
    let second = facade
        .push(ctx(), PushReq { raw: body })
        .await
        .expect("push 2");
    let s = parse(&second.body);
    assert_eq!(
        s[0]["status"], "duplicate",
        "retry of the same txn: {}",
        second.body
    );
    assert_eq!(s[0]["offset"].as_u64(), Some(0), "the original offset");
    assert_eq!(
        s[0]["message_id"], f[0]["message_id"],
        "a duplicate reports the original message id"
    );

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_restart_recovers_the_pushed_state() {
    let dir = scratch("recover");

    // Run 1: push two messages, then shut down cleanly.
    {
        let facade = RaftFacade::open(&build_ctx(&dir)).expect("open 1");
        let push = facade
            .push(
                ctx(),
                PushReq {
                    raw: br#"{"items":[
                        {"queue":"orders","payload":{"n":10},"transactionId":"r1"},
                        {"queue":"orders","payload":{"n":20},"transactionId":"r2"}
                    ]}"#
                    .to_vec(),
                },
            )
            .await
            .expect("push");
        let arr = parse(&push.body);
        assert_eq!(arr[0]["offset"].as_u64(), Some(0));
        assert_eq!(arr[1]["offset"].as_u64(), Some(1));
        facade.shutdown().await;
    }

    // Run 2: reopen the SAME directory and read the backlog back — proving the
    // segments and the store recovered (§11.5).
    {
        let facade = RaftFacade::open(&build_ctx(&dir)).expect("reopen");
        let popped = facade
            .pop_wildcard(
                ctx(),
                PopReq {
                    queue: "orders".into(),
                    group: None,
                    batch: 10,
                    auto_ack: true,
                    wait: false,
                    timeout_ms: 1000,
                    options: Default::default(),
                },
            )
            .await
            .expect("pop after restart");
        assert!(!popped.empty, "the pushed backlog survived the restart");
        let pop = parse(&popped.body);
        let msgs = pop["messages"].as_array().expect("messages");
        assert_eq!(msgs.len(), 2, "both recovered: {}", popped.body);
        assert_eq!(msgs[0]["data"]["n"], 10);
        assert_eq!(msgs[1]["data"]["n"], 20);
        facade.shutdown().await;
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Push two messages to one partition, lease both under one pop, then ONE batch
/// ack that completes the first and dead-letters the second. The two acks land
/// on the same `(partition, lease)` target, whose `AckResult` reports the DLQ as
/// a COUNT (`dlq: u32`, WP-1.1's shape). The render must attribute the flag to
/// the item that carried the signal, not broadcast the target's count onto the
/// completed sibling — the batch-ack mis-attribution the refutation caught. The
/// WP's e2e test acked all three as "completed", so it never exercised this.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_mixed_batch_ack_flags_dlq_per_item_not_per_target() {
    let dir = scratch("ack-mixed");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    let push = facade
        .push(
            ctx(),
            PushReq {
                raw: br#"{"items":[
                    {"queue":"mix","payload":{"n":1},"transactionId":"done"},
                    {"queue":"mix","payload":{"n":2},"transactionId":"poison"}
                ]}"#
                .to_vec(),
            },
        )
        .await
        .expect("push");
    let arr = parse(&push.body);
    assert_eq!(arr[0]["offset"].as_u64(), Some(0));
    assert_eq!(arr[1]["offset"].as_u64(), Some(1));

    // One pop leases BOTH messages of the one partition under one worker/lease.
    let popped = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "mix".into(),
                group: None,
                batch: 10,
                auto_ack: false,
                wait: false,
                timeout_ms: 1000,
                options: Default::default(),
            },
        )
        .await
        .expect("pop");
    let pop = parse(&popped.body);
    assert_eq!(pop["messages"].as_array().map(|m| m.len()), Some(2));
    let pid = pop["partitionId"]
        .as_str()
        .expect("partitionId")
        .to_string();
    let lease = pop["leaseId"].as_str().expect("leaseId").to_string();
    assert!(!lease.is_empty());

    // ONE target (same pid + lease), mixed statuses: complete `done`, DLQ
    // `poison`.
    let ack_body = format!(
        r#"{{"consumerGroup":"__QUEUE_MODE__","acknowledgments":[
            {{"transactionId":"done","partitionId":"{pid}","status":"completed","leaseId":"{lease}"}},
            {{"transactionId":"poison","partitionId":"{pid}","status":"dlq","leaseId":"{lease}"}}
        ]}}"#,
    );
    let acked = facade
        .ack(
            ctx(),
            AckReq {
                queue: Some("mix".into()),
                group: "__QUEUE_MODE__".into(),
                raw: ack_body.into_bytes(),
            },
        )
        .await
        .expect("ack");
    let results = parse(&acked.body);
    let results = results.as_array().expect("ack array");
    assert_eq!(results.len(), 2);

    // `done` completed and MUST NOT inherit the target's dlq flag.
    assert_eq!(results[0]["transactionId"], "done");
    assert_eq!(results[0]["success"], true, "done: {}", results[0]);
    assert_eq!(
        results[0]["dlq"], false,
        "the completed sibling must not be flagged dlq: {}",
        acked.body
    );
    // `poison` is the one dead letter.
    assert_eq!(results[1]["transactionId"], "poison");
    assert_eq!(results[1]["dlq"], true, "poison: {}", results[1]);

    // Exactly one dead letter was filed, and it is `poison` — not `done`.
    let dlq = dlq_rows_settled(&facade, 1).await;
    assert_eq!(dlq.len(), 1, "exactly one dead letter filed");
    assert_eq!(
        dlq[0].txn, "poison",
        "the dead letter is the poison, not done"
    );

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A forced-DLQ ack files a dead letter that carries the transaction id off the
/// ack wire and the original frame snapshot. DLQ replay must reproduce the
/// original payload rather than a content-less message.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_forced_dlq_ack_files_the_transaction_id() {
    let dir = scratch("dlq-txn");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    facade
        .push(
            ctx(),
            PushReq {
                raw: br#"{"items":[{"queue":"pq","payload":{"bad":true},"transactionId":"tx-poison"}]}"#
                    .to_vec(),
            },
        )
        .await
        .expect("push");

    let popped = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "pq".into(),
                group: None,
                batch: 10,
                auto_ack: false,
                wait: false,
                timeout_ms: 1000,
                options: Default::default(),
            },
        )
        .await
        .expect("pop");
    let pop = parse(&popped.body);
    let pid = pop["partitionId"]
        .as_str()
        .expect("partitionId")
        .to_string();
    let lease = pop["leaseId"].as_str().expect("leaseId").to_string();

    let ack_body = format!(
        r#"{{"transactionId":"tx-poison","partitionId":"{pid}","status":"dlq","leaseId":"{lease}"}}"#,
    );
    let acked = facade
        .ack(
            ctx(),
            AckReq {
                queue: Some("pq".into()),
                group: "__QUEUE_MODE__".into(),
                raw: ack_body.into_bytes(),
            },
        )
        .await
        .expect("ack");
    let res = parse(&acked.body);
    assert_eq!(res[0]["dlq"], true, "the ack reports the dead letter");

    let dlq = dlq_rows_settled(&facade, 1).await;
    assert_eq!(dlq.len(), 1, "one dead letter filed");
    assert_eq!(
        dlq[0].txn, "tx-poison",
        "the filed dead letter carries the txn, not a blank string"
    );
    assert!(
        dlq[0].message_id.is_some(),
        "the original message id is kept"
    );
    assert_eq!(dlq[0].payload, br#"{"bad":true}"#);

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn phase_two_admin_detail_and_stream_state_accept_the_raft_partition_id() {
    let dir = scratch("phase2-api");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let call = |method: &str, path: &str, body: Value| ApiReq {
        method: method.to_string(),
        path: path.to_string(),
        query: None,
        body: serde_json::to_vec(&body).unwrap(),
    };

    let configured = facade
        .api(
            ctx(),
            call(
                "POST",
                "/api/v1/configure",
                serde_json::json!({
                    "queue":"stream-source",
                    "namespace":"phase2",
                    "task":"state",
                    "options":{"leaseTime":17,"retryLimit":2}
                }),
            ),
        )
        .await
        .expect("configure");
    assert_eq!(configured.status, 200, "{}", configured.body);

    facade
        .push(
            ctx(),
            PushReq {
                raw: br#"{"items":[
                    {"queue":"stream-source","payload":{"n":1},"transactionId":"stream-tx"},
                    {"queue":"stream-source","payload":{"n":2},"transactionId":"stream-z"},
                    {"queue":"stream-source","payload":{"n":3},"transactionId":"stream-a"}
                ]}"#
                .to_vec(),
            },
        )
        .await
        .expect("push");
    let popped = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "stream-source".into(),
                group: Some("stream-group".into()),
                batch: 3,
                auto_ack: false,
                wait: false,
                timeout_ms: 1_000,
                options: crate::rsm::facade::PopOptions {
                    subscription_mode: "all".into(),
                    ..Default::default()
                },
            },
        )
        .await
        .expect("pop");
    let pop = parse(&popped.body);
    let pid = pop["partitionId"].as_str().expect("numeric pid");
    let lease = pop["leaseId"].as_str().expect("lease id");
    assert_eq!(pop["messages"].as_array().map(Vec::len), Some(3));
    assert!(pid.parse::<u64>().is_ok(), "Raft partition id: {pid}");

    let detail = facade
        .api(
            ctx(),
            call(
                "GET",
                &format!("/api/v1/messages/{pid}/stream-tx"),
                Value::Null,
            ),
        )
        .await
        .expect("message detail");
    assert_eq!(detail.status, 200, "{}", detail.body);
    let detail = parse(&detail.body);
    assert_eq!(detail["queueConfig"]["leaseTime"], 17);
    assert_eq!(detail["namespace"], "phase2");
    assert_eq!(detail["task"], "state");

    let registered = facade
        .api(
            ctx(),
            call(
                "POST",
                "/streams/v1/queries",
                serde_json::json!({
                    "name":"phase2-stream",
                    "source_queue":"stream-source",
                    "sink_queue":"stream-sink",
                    "config_hash":"hash-1"
                }),
            ),
        )
        .await
        .expect("register stream");
    assert_eq!(registered.status, 200, "{}", registered.body);
    let qid = parse(&registered.body)["query_id"]
        .as_str()
        .expect("query id")
        .to_string();

    let cycle = facade
        .api(
            ctx(),
            call(
                "POST",
                "/streams/v1/cycle",
                serde_json::json!({
                    "query_id":qid.clone(),
                    "partition_id":pid,
                    "consumer_group":"stream-group",
                    "state_ops":[{"type":"upsert","key":"count","value":{"n":1}}],
                    "push_items":[],
                    "ack":{"leaseId":lease,"status":"completed","count":3},
                    "release_lease":true
                }),
            ),
        )
        .await
        .expect("stream cycle");
    assert_eq!(cycle.status, 200, "{}", cycle.body);
    let cycle = parse(&cycle.body);
    assert_eq!(cycle["success"], true, "{cycle}");
    assert_eq!(cycle["ack_result"]["count"], 3, "{cycle}");
    assert_eq!(cycle["ack_result"]["lease_released"], true, "{cycle}");

    let empty = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "stream-source".into(),
                group: Some("stream-group".into()),
                batch: 3,
                auto_ack: false,
                wait: false,
                timeout_ms: 1_000,
                options: crate::rsm::facade::PopOptions {
                    subscription_mode: "all".into(),
                    ..Default::default()
                },
            },
        )
        .await
        .expect("post-cycle pop");
    assert!(
        empty.empty,
        "the Streams cycle committed the full leased batch"
    );

    let state = facade
        .api(
            ctx(),
            call(
                "POST",
                "/streams/v1/state/get",
                serde_json::json!({"query_id":qid,"partition_id":pid,"keys":["count"]}),
            ),
        )
        .await
        .expect("stream state");
    assert_eq!(state.status, 200, "{}", state.body);
    assert_eq!(parse(&state.body)["rows"][0]["value"]["n"], 1);

    let raft_status = facade
        .api(ctx(), call("GET", "/api/v1/raft/status", Value::Null))
        .await
        .expect("raft status");
    assert_eq!(raft_status.status, 200, "{}", raft_status.body);
    assert_eq!(parse(&raft_status.body)["role"], "leader");

    let postgres_stats = facade
        .api(
            ctx(),
            call("GET", "/api/v1/analytics/postgres-stats", Value::Null),
        )
        .await
        .expect("Postgres compatibility stats");
    assert_eq!(postgres_stats.status, 200, "{}", postgres_stats.body);
    assert_eq!(parse(&postgres_stats.body)["database"], "raft");

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// The two lean reads — the queue list with `?stats=lanes` and a queue's
/// per-partition `sizes` — answer the same numbers the full renders do, from an
/// index walk and one counter per partition instead of every partition's
/// cursors and segment files.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn the_lean_queue_reads_answer_what_the_full_ones_do() {
    let dir = scratch("lean-reads");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let items: Vec<Value> = (0..10)
        .map(|p| serde_json::json!({"queue":"wide","partition":p.to_string(),"payload":{"n":p}}))
        .chain(std::iter::once(
            serde_json::json!({"queue":"narrow","partition":"a","payload":{"x":"yyyyyyyy"}}),
        ))
        .collect();
    facade
        .push(
            ctx(),
            PushReq {
                raw: serde_json::to_vec(&serde_json::json!({ "items": items })).unwrap(),
            },
        )
        .await
        .expect("push");
    let get = |path: &str, query: Option<&str>| ApiReq {
        method: "GET".into(),
        path: path.into(),
        query: query.map(str::to_string),
        body: Vec::new(),
    };

    let full = facade
        .api(ctx(), get("/api/v1/resources/queues", None))
        .await
        .expect("full list");
    let lean = facade
        .api(ctx(), get("/api/v1/resources/queues", Some("stats=lanes")))
        .await
        .expect("lean list");
    assert_eq!(lean.status, 200, "{}", lean.body);
    let shape = |body: &str| {
        let mut v: Vec<(String, i64, String)> = parse(body)["queues"]
            .as_array()
            .unwrap()
            .iter()
            .map(|q| {
                (
                    q["name"].as_str().unwrap().to_string(),
                    q["partitions"].as_i64().unwrap(),
                    q["id"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        v.sort();
        v
    };
    assert_eq!(shape(&lean.body), shape(&full.body));
    assert_eq!(
        shape(&lean.body)
            .iter()
            .map(|(n, p, _)| (n.as_str(), *p))
            .collect::<Vec<_>>(),
        [("narrow", 1), ("wide", 10)]
    );
    let lean = parse(&lean.body);
    assert_eq!(lean["stats"], "lanes");
    assert!(
        lean.get("kvRows").is_none(),
        "the lean list scanned the KV rows"
    );
    assert!(lean["queues"][0].get("retainedBytes").is_none());

    let sizes = facade
        .api(ctx(), get("/api/v1/resources/queues/wide/sizes", None))
        .await
        .expect("sizes");
    assert_eq!(sizes.status, 200, "{}", sizes.body);
    let detail = facade
        .api(ctx(), get("/api/v1/resources/queues/wide", None))
        .await
        .expect("detail");
    let from_detail: std::collections::BTreeMap<String, i64> = parse(&detail.body)["partitions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| {
            (
                p["name"].as_str().unwrap().to_string(),
                p["retainedBytes"].as_i64().unwrap(),
            )
        })
        .collect();
    let from_sizes: std::collections::BTreeMap<String, i64> = parse(&sizes.body)["partitions"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.as_i64().unwrap()))
        .collect();
    assert_eq!(from_sizes.len(), 10);
    assert_eq!(from_sizes, from_detail);

    let missing = facade
        .api(ctx(), get("/api/v1/resources/queues/nope/sizes", None))
        .await
        .expect("sizes of nothing");
    assert_eq!(missing.status, 404);

    // A single node is a cluster of one, heard from now, and says so.
    let members = facade
        .api(ctx(), get("/api/v1/raft/members", None))
        .await
        .expect("members");
    assert_eq!(members.status, 200, "{}", members.body);
    let members = parse(&members.body);
    assert_eq!(members["viewAgeMs"], 0, "{members}");
    assert_eq!(members["members"].as_array().unwrap().len(), 1, "{members}");
    assert_eq!(members["members"][0]["lastAckMs"], 0, "{members}");
    assert_eq!(members["members"][0]["local"], true, "{members}");
    assert_eq!(members["leaderId"], members["nodeId"], "{members}");

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn raft_push_stamps_authenticated_subject_and_round_trips_encrypted_payload() {
    let dir = scratch("encrypted-producer");
    let mut facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    facade.set_encryption_for_test(crate::encryption::Encryption::for_test([7; 32]));
    let configured = facade
        .api(
            ctx(),
            ApiReq {
                method: "POST".into(),
                path: "/api/v1/configure".into(),
                query: None,
                body: serde_json::to_vec(&serde_json::json!({
                    "queue":"secret",
                    "options":{"encryptionEnabled":true}
                }))
                .unwrap(),
            },
        )
        .await
        .expect("configure encrypted queue");
    assert_eq!(configured.status, 200, "{}", configured.body);

    facade
        .push(
            ctx().with_producer_sub(Some("alice-producer".into())),
            PushReq {
                raw: br#"{"items":[{"queue":"secret","payload":{"secret":42},"transactionId":"enc-1","producerSub":"attacker"}]}"#
                    .to_vec(),
            },
        )
        .await
        .expect("encrypted push");
    let popped = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "secret".into(),
                group: None,
                batch: 1,
                auto_ack: true,
                wait: false,
                timeout_ms: 1_000,
                options: Default::default(),
            },
        )
        .await
        .expect("encrypted pop");
    let pop = parse(&popped.body);
    assert_eq!(pop["messages"][0]["data"]["secret"], 42, "{pop}");
    assert_eq!(
        pop["messages"][0]["producerSub"], "alice-producer",
        "body spoofing never wins: {pop}"
    );

    let pid = pop["partitionId"].as_str().expect("partition id");
    let detail = facade
        .api(
            ctx(),
            ApiReq {
                method: "GET".into(),
                path: format!("/api/v1/messages/{pid}/enc-1"),
                query: None,
                body: Vec::new(),
            },
        )
        .await
        .expect("encrypted message detail");
    let detail = parse(&detail.body);
    assert_eq!(detail["data"]["secret"], 42, "{detail}");
    assert_eq!(detail["producerSub"], "alice-producer", "{detail}");
    assert_eq!(detail["isEncrypted"], true, "{detail}");

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// PERF-1 (O18): the queen_raft_* timing surface is reachable and non-zero after
// a real push/pop/ack cycle, and the Prometheus exporter renders the families.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn timing_histograms_are_reachable_and_nonzero() {
    use crate::rsm::planner::CommandKind;
    use crate::rsm::timing;

    let dir = scratch("timing");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    // -- push three messages (payload bytes → qlog write/fsync) ----------------
    let push = facade
        .push(
            ctx(),
            PushReq {
                raw: br#"{"items":[
                    {"queue":"t","payload":{"n":1},"transactionId":"a1"},
                    {"queue":"t","payload":{"n":2},"transactionId":"a2"},
                    {"queue":"t","payload":{"n":3},"transactionId":"a3"}
                ]}"#
                .to_vec(),
            },
        )
        .await
        .expect("push");
    assert_eq!(parse(&push.body).as_array().map(|a| a.len()), Some(3));

    // -- pop them (pop payload read off the files) ----------------------------
    let popped = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "t".into(),
                group: None,
                batch: 10,
                auto_ack: false,
                wait: false,
                timeout_ms: 1000,
                options: Default::default(),
            },
        )
        .await
        .expect("pop");
    assert!(!popped.empty, "the pop claims the backlog");
    let pop = parse(&popped.body);
    let pid = pop["partitionId"].as_str().unwrap().to_string();
    let lease = pop["leaseId"].as_str().unwrap().to_string();

    // -- ack them (advances the cursor; exercises the ack planner) ------------
    let ack_body = format!(
        r#"{{"consumerGroup":"__QUEUE_MODE__","acknowledgments":[
            {{"transactionId":"a1","partitionId":"{pid}","status":"completed","leaseId":"{lease}"}},
            {{"transactionId":"a2","partitionId":"{pid}","status":"completed","leaseId":"{lease}"}},
            {{"transactionId":"a3","partitionId":"{pid}","status":"completed","leaseId":"{lease}"}}
        ]}}"#
    );
    facade
        .ack(
            ctx(),
            AckReq {
                queue: Some("t".into()),
                group: "__QUEUE_MODE__".into(),
                raw: ack_body.into_bytes(),
            },
        )
        .await
        .expect("ack");

    // Give the store-commit cadence (~4 ms) a moment so store_commit fires.
    tokio::time::sleep(Duration::from_millis(30)).await;

    let m = timing::metrics();
    // Every stage the cycle drives must have recorded at least once.
    let checks: [(&str, u64); 11] = [
        ("plan", m.plan.snapshot().count),
        ("drain_commands", m.drain_commands.snapshot().count),
        ("drain_messages", m.drain_messages.snapshot().count),
        (
            "arrival_to_proposed",
            m.arrival_to_proposed.snapshot().count,
        ),
        ("propose_roundtrip", m.propose_roundtrip.snapshot().count),
        (
            "proposed_to_committed",
            m.proposed_to_committed.snapshot().count,
        ),
        ("log_fsync", m.log_fsync.snapshot().count),
        ("group_entries", m.group_entries.snapshot().count),
        ("apply_entry", m.apply_entry.snapshot().count),
        (
            "apply_channel_depth",
            m.apply_channel_depth.snapshot().count,
        ),
        ("pop_read", m.pop_read.snapshot().count),
    ];
    for (name, count) in checks {
        assert!(count > 0, "histogram {name} was never recorded (count 0)");
    }
    // The push planner counter moved.
    assert!(
        m.kinds.planned(CommandKind::Push) > 0,
        "the push planner counter is zero",
    );

    // The exporter renders the families with precomputed quantiles.
    let mut body = String::new();
    timing::render_prometheus(&mut body);
    for needle in [
        "queen_raft_apply_entry_seconds",
        "queen_raft_plan_seconds",
        "queen_raft_pop_read_seconds",
        "queen_raft_planner_commands_total{kind=\"push\"}",
        "queen_raft_apply_stats{field=\"entries\"}",
        "quantile=\"0.99\"",
    ] {
        assert!(body.contains(needle), "exporter missing {needle}\n{body}");
    }

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// PERF-1 overhead microbench (#[ignore]; run explicitly). Mirrors the FULL
// per-cycle + per-entry instrumentation the leader pipeline does for one entry
// (the A20k shape: ~10 messages/entry, taken as 10 one-message commands and 10
// segment appends), in a tight loop, so the added cost can be priced with
// QUEEN_RAFT_METRICS on vs off — the "A20k with and without" delta, isolated
// from the fsync/network the real smoke is bound by. Run:
//   cargo test -p queen-engine --release --lib \
//     rsm::tests::facade::perf1_instrumentation_overhead -- --ignored --nocapture
// ONCE with QUEEN_RAFT_METRICS unset (on) and ONCE =0 (off); the lever's cost is
// the ON-minus-OFF delta in per_entry_ns.
//
// The refutation of the first version: it fed every `record_dur` a CONSTANT
// `Duration` and took no clock inside the measured loop, so it priced the
// histogram writes but NOT the ~dozens of `Instant::now()`/`elapsed()` per entry
// the real path takes to PRODUCE those durations — and, with the fix, those
// clock reads are exactly what the knob now ablates. So every stamp here goes
// through `timing::stamp()` (a real clock read when on, `None` when off) and its
// `elapsed()`, at the same call COUNT per entry as production: one per command
// at facade INGRESS (the `Submission::new` arrival stamp — the second refutation
// caught this one missing: it is per-command, not per-entry, and on the off path
// production used to pay it too, so both axes were wrong) and one per planned
// command (O18), one per segment append, plus plan / arrival→proposed / apply
// (×2 via the injected clock) / log-fsync / proposed→committed / propose-
// roundtrip. The recorded VALUES are meaningless (near-zero elapseds); only the
// CPU of the stamp+record calls is under test, which is the whole point.
#[test]
#[ignore]
fn perf1_instrumentation_overhead() {
    use crate::rsm::planner::CommandKind;
    use crate::rsm::timing;
    use std::time::Instant;

    let m = timing::metrics();
    const CMDS_PER_ENTRY: u64 = 10; // 1-message pushes, the A20k shape
    const MSGS_PER_ENTRY: u64 = 10; // one segment append per message
    const ITERS: u64 = 2_000_000; // "entries"

    // Warm the LazyLock and buckets before timing.
    for _ in 0..1000 {
        if let Some(s) = timing::stamp() {
            m.plan.record_dur(s.elapsed());
        }
    }

    let t0 = Instant::now();
    for i in 0..ITERS {
        // per command at facade ingress: the arrival stamp `Submission::new`
        // takes and stores in `received_at` (PERF-1 refutation). It is the
        // highest-frequency timing read — ONE per command, not per entry — and,
        // now gated through `timing::stamp()`, it is what the knob ablates on the
        // hot ingress path. Its `elapsed()` is charged later at propose
        // (arrival→proposed). `black_box` keeps the read from being elided.
        for _ in 0..CMDS_PER_ENTRY {
            std::hint::black_box(timing::stamp());
        }
        // per command: the O18 per-kind planning stamp + counter.
        for _ in 0..CMDS_PER_ENTRY {
            if let Some(s) = timing::stamp() {
                m.kinds.record(CommandKind::Push, s.elapsed(), false);
            }
        }
        // per cycle: drain sizes (no clock), plan duration, arrival→proposed
        // (one clock read at propose, one sample per proposed command).
        m.drain_commands.record(CMDS_PER_ENTRY);
        m.drain_messages.record(CMDS_PER_ENTRY);
        if let Some(s) = timing::stamp() {
            m.plan.record_dur(s.elapsed());
        }
        if let Some(now) = timing::stamp() {
            for _ in 0..CMDS_PER_ENTRY {
                m.arrival_to_proposed.record_dur(now.elapsed());
            }
        }
        // per entry: the writer group, the segment appends, the apply split.
        timing::apply_channel_send();
        timing::apply_channel_recv();
        let before = timing::segment_write_ns_total();
        for _ in 0..MSGS_PER_ENTRY {
            if let Some(s) = timing::stamp() {
                timing::record_segment_write(s.elapsed());
            }
        }
        // apply reads the injected clock twice (t0, then total) — modelled here
        // with two `stamp`s so the ablation prices both.
        if let (Some(t0e), Some(_probe)) = (timing::stamp(), timing::stamp()) {
            let total = t0e.elapsed();
            let seg = timing::segment_write_ns_total().saturating_sub(before);
            m.apply_entry.record_dur(total);
            m.apply_other
                .record((total.as_nanos() as u64).saturating_sub(seg));
        }
        if let Some(s) = timing::stamp() {
            m.log_fsync.record_dur(s.elapsed());
        }
        m.group_entries.record(1);
        m.group_bytes.record(4096 * MSGS_PER_ENTRY);
        if let Some(s) = timing::stamp() {
            m.proposed_to_committed.record_dur(s.elapsed());
        }
        if let Some(s) = timing::stamp() {
            m.propose_roundtrip.record_dur(s.elapsed());
        }
        std::hint::black_box(i);
    }
    let el = t0.elapsed();
    let per_entry_ns = el.as_nanos() as f64 / ITERS as f64;
    // At A20k (~10 messages/entry) the leader plans ~2000 entries/s.
    let cpu_per_s_at_a20k_us = per_entry_ns * 2000.0 / 1000.0;
    println!(
        "PERF1_OVERHEAD metrics_enabled={} iters={ITERS} elapsed={:?} per_entry_ns={:.1} \
         cmds/entry={CMDS_PER_ENTRY} msgs/entry={MSGS_PER_ENTRY} \
         => instrumentation_cpu_at_A20k={:.1}us/s ({:.4}% of one core). \
         Cost of the lever = this ns/entry with QUEEN_RAFT_METRICS on MINUS off.",
        timing::enabled(),
        el,
        per_entry_ns,
        cpu_per_s_at_a20k_us,
        cpu_per_s_at_a20k_us / 1_000_000.0 * 100.0,
    );
}

async fn pop_q(facade: &RaftFacade, q: &str) -> Value {
    let p = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: q.into(),
                group: None,
                batch: 10,
                auto_ack: false,
                wait: false,
                timeout_ms: 1000,
                options: Default::default(),
            },
        )
        .await
        .expect("pop");
    if p.empty {
        serde_json::json!({"messages": []})
    } else {
        parse(&p.body)
    }
}

async fn txn(facade: &RaftFacade, body: Value) -> Value {
    let out = facade
        .transaction(
            ctx(),
            crate::rsm::facade::TxnReq {
                raw: body.to_string().into_bytes(),
            },
        )
        .await
        .expect("transaction");
    parse(&out.body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_transaction_acks_the_input_and_pushes_the_output_all_or_nothing() {
    // Phase B: consume-transform-produce in ONE bundle — one command, one entry.
    let dir = scratch("txn");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    facade
        .push(
            ctx(),
            PushReq {
                raw: br#"{"items":[
                    {"queue":"inq","payload":{"n":1},"transactionId":"t1"},
                    {"queue":"inq","payload":{"n":2},"transactionId":"t2"}
                ]}"#
                .to_vec(),
            },
        )
        .await
        .expect("push");
    let pop = pop_q(&facade, "inq").await;
    assert_eq!(
        pop["messages"].as_array().map(|m| m.len()),
        Some(2),
        "{pop}"
    );
    let pid = pop["partitionId"]
        .as_str()
        .expect("partitionId")
        .to_string();
    let lease = pop["leaseId"].as_str().expect("leaseId").to_string();

    // ---- commit: ack both inputs, push one output -------------------------------
    let ok = txn(
        &facade,
        serde_json::json!({
            "operations": [
                {"type": "ack", "transactionId": "t1", "partitionId": pid, "leaseId": lease},
                {"type": "ack", "transactionId": "t2", "partitionId": pid, "leaseId": lease},
                {"type": "push", "items": [{"queue": "outq", "payload": {"sum": 3}, "transactionId": "o1"}]}
            ]
        }),
    )
    .await;
    assert_eq!(ok["success"], true, "{ok}");
    let res = ok["results"].as_array().expect("results");
    assert_eq!(res.len(), 3, "{ok}");
    assert_eq!(res[0]["type"], "ack");
    assert_eq!(res[2]["type"], "push");
    assert_eq!(res[2]["success"], true, "{ok}");
    let out = pop_q(&facade, "outq").await;
    let outm = out["messages"].as_array().expect("messages");
    assert_eq!(outm.len(), 1, "the output is poppable: {out}");
    assert_eq!(outm[0]["data"]["sum"], 3);
    assert_eq!(
        pop_q(&facade, "inq").await["messages"]
            .as_array()
            .map(|m| m.len()),
        Some(0),
        "the inputs were consumed by the same bundle"
    );

    // ---- rollback on a DUPLICATE push: the bundle's ack must NOT apply ---------
    facade
        .push(
            ctx(),
            PushReq {
                raw: br#"{"items":[{"queue":"inq","payload":{"n":3},"transactionId":"t3"}]}"#
                    .to_vec(),
            },
        )
        .await
        .expect("push t3");
    let pop3 = pop_q(&facade, "inq").await;
    let pid3 = pop3["partitionId"]
        .as_str()
        .expect("partitionId")
        .to_string();
    let lease3 = pop3["leaseId"].as_str().expect("leaseId").to_string();
    let dup = txn(
        &facade,
        serde_json::json!({
            "operations": [
                {"type": "ack", "transactionId": "t3", "partitionId": pid3, "leaseId": lease3},
                {"type": "push", "items": [{"queue": "outq", "payload": {"again": true}, "transactionId": "o1"}]}
            ]
        }),
    )
    .await;
    assert_eq!(
        dup["success"], false,
        "a duplicate push rolls the bundle back: {dup}"
    );
    assert_eq!(dup["reason"], "duplicate", "{dup}");
    // The ack of t3 did not happen: the same ack in a clean bundle still succeeds.
    let retry = txn(
        &facade,
        serde_json::json!({
            "operations": [
                {"type": "ack", "transactionId": "t3", "partitionId": pid3, "leaseId": lease3}
            ]
        }),
    )
    .await;
    assert_eq!(
        retry["success"], true,
        "the rolled-back ack is still owed: {retry}"
    );

    // ---- rollback on a REJECTED ack: the bundle's push must NOT appear ---------
    let bad = txn(
        &facade,
        serde_json::json!({
            "operations": [
                {"type": "push", "items": [{"queue": "outq", "payload": {"ghost": true}, "transactionId": "o2"}]},
                {"type": "ack", "transactionId": "t1", "partitionId": pid, "leaseId": "not-my-lease"}
            ]
        }),
    )
    .await;
    assert_eq!(
        bad["success"], false,
        "a rejected ack rolls the bundle back: {bad}"
    );
    assert_eq!(
        pop_q(&facade, "outq").await["messages"]
            .as_array()
            .map(|m| m.len()),
        Some(0),
        "the rolled-back push never reached the output queue"
    );
    facade.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_transaction_carries_a_kv_rider_all_or_nothing() {
    // Phase B4: the `kv` rider rides the SAME entry as the bundle's messages.
    let dir = scratch("txn-kv");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    // ---- commit: a push and a KV put in one bundle ------------------------------
    let ok = txn(
        &facade,
        serde_json::json!({
            "operations": [{"type": "push", "items": [{"queue": "kvq", "payload": {"a": 1}, "transactionId": "k1"}]}],
            "kv": [{"op": "put", "ns": "st", "key": "cursor", "value": {"at": 1}, "ttlSeconds": 600}]
        }),
    )
    .await;
    assert_eq!(ok["success"], true, "{ok}");
    let res = ok["results"].as_array().expect("results");
    assert_eq!(res.len(), 2, "{ok}");
    assert_eq!(res[0]["type"], "push");
    assert_eq!(res[1]["type"], "kv", "{ok}");
    assert_eq!(
        res[1]["index"], 1,
        "riders take the flat ordinals after the operations"
    );
    assert_eq!(res[1]["opIndex"], 0);
    let got = txn(
        &facade,
        serde_json::json!({"kv": [{"op": "get", "ns": "st", "key": "cursor"}]}),
    )
    .await;
    assert_eq!(got["success"], true, "{got}");
    assert_eq!(got["results"][0]["found"], true, "{got}");
    assert_eq!(
        got["results"][0]["value"],
        serde_json::json!({"at": 1}),
        "{got}"
    );

    // ---- rollback: a REQUIRED CAS that loses aborts the whole bundle ------------
    let lost = txn(
        &facade,
        serde_json::json!({
            "operations": [{"type": "push", "items": [{"queue": "kvq2", "payload": {"b": 1}, "transactionId": "k2"}]}],
            "kv": [{"op": "put", "ns": "st", "key": "cursor", "value": {"at": 2}, "ttlSeconds": 600,
                    "expect": 999999, "required": true}]
        }),
    )
    .await;
    assert_eq!(lost["success"], false, "{lost}");
    assert_eq!(lost["reason"], "kv_precondition", "{lost}");
    assert_eq!(
        lost["failedIndex"], 1,
        "the failed op in the FLAT space: {lost}"
    );
    assert_eq!(
        pop_q(&facade, "kvq2").await["messages"]
            .as_array()
            .map(|m| m.len()),
        Some(0),
        "the rolled-back bundle's push never landed"
    );
    let still = txn(
        &facade,
        serde_json::json!({"kv": [{"op": "get", "ns": "st", "key": "cursor"}]}),
    )
    .await;
    assert_eq!(
        still["results"][0]["value"],
        serde_json::json!({"at": 1}),
        "{still}"
    );
    facade.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_transaction_schedules_a_timer_that_fires_only_if_it_commits() {
    // Phase B4: the `timers` rider rides the transaction's one entry.
    let dir = scratch("txn-timer");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    // ---- commit: a push and a timer schedule in one bundle ----------------------
    let ok = txn(
        &facade,
        serde_json::json!({
            "operations": [{"type": "push", "items": [{"queue": "tq-in", "payload": {"a": 1}, "transactionId": "x1"}]}],
            "timers": [{"op": "schedule", "queue": "tq", "timerKey": "t1", "delayMs": 50,
                        "txn": "tt1", "payload": "eyJ0IjoxfQ=="}]
        }),
    )
    .await;
    assert_eq!(ok["success"], true, "{ok}");
    assert_eq!(ok["results"][1]["type"], "timer", "{ok}");
    assert_eq!(ok["results"][1]["index"], 1, "{ok}");
    let mut fired = serde_json::json!({"messages": []});
    for _ in 0..150 {
        fired = pop_q(&facade, "tq").await;
        if fired["messages"].as_array().is_some_and(|m| !m.is_empty()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        fired["messages"].as_array().map(|m| m.len()),
        Some(1),
        "the committed timer fired: {fired}"
    );
    assert_eq!(fired["messages"][0]["data"]["t"], 1, "{fired}");

    // ---- rollback: a timer in a bundle whose ack is rejected never fires --------
    let bad = txn(
        &facade,
        serde_json::json!({
            "operations": [{"type": "ack", "transactionId": "nope", "partitionId": "1", "leaseId": "x"}],
            "timers": [{"op": "schedule", "queue": "tq2", "timerKey": "t2", "delayMs": 10,
                        "txn": "tt2", "payload": "eyJ0IjoxfQ=="}]
        }),
    )
    .await;
    assert_eq!(bad["success"], false, "{bad}");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        pop_q(&facade, "tq2").await["messages"]
            .as_array()
            .map(|m| m.len()),
        Some(0),
        "the rolled-back timer never fires"
    );
    facade.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn an_autopilot_pop_fills_its_batch_from_several_sparse_partitions() {
    let dir = scratch("autopilot");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    // Eight sparse partitions, three messages each.
    for p in 0..8 {
        let items: Vec<String> = (0..3)
            .map(|i| {
                format!(
                    r#"{{"queue":"ap","partition":"p{p}","payload":{{"n":{i}}},"transactionId":"ap-{p}-{i}"}}"#
                )
            })
            .collect();
        facade
            .push(
                ctx(),
                PushReq {
                    raw: format!(r#"{{"items":[{}]}}"#, items.join(",")).into_bytes(),
                },
            )
            .await
            .expect("push");
    }
    let pop = |auto: bool| {
        let facade = &facade;
        async move {
            let out = facade
                .pop_wildcard(
                    ctx(),
                    PopReq {
                        queue: "ap".into(),
                        group: Some("apg".into()),
                        batch: 200,
                        auto_ack: false,
                        wait: false,
                        timeout_ms: 2_000,
                        options: crate::rsm::facade::PopOptions {
                            subscription_mode: "all".into(),
                            max_parts: 1,
                            auto_parts: auto,
                            auto_batch: auto,
                            ..Default::default()
                        },
                    },
                )
                .await
                .expect("pop");
            parse(&out.body)
        }
    };

    // A pop that did not opt in: one partition, no echo.
    let manual = pop(false).await;
    assert_eq!(manual["messages"].as_array().map(Vec::len), Some(3));
    assert!(manual.get("autopilot").is_none(), "no echo without the opt-in");

    // Opted in: seven partitions are ready and one pop is live, so the width
    // covers all of them and one pop collects 7 x 3 messages. A cold lane's
    // batch is the minimum, 100.
    let auto = pop(true).await;
    assert_eq!(auto["autopilot"]["partitions"], 7, "{auto}");
    assert_eq!(auto["autopilot"]["batch"], 100, "{auto}");
    let msgs = auto["messages"].as_array().expect("messages");
    assert_eq!(msgs.len(), 21, "{auto}");
    let lease = auto["leaseId"].as_str().expect("lease").to_string();

    // Its ack is a drain sample for the lane's next batch.
    let acks: Vec<String> = msgs
        .iter()
        .map(|m| {
            format!(
                r#"{{"transactionId":"{}","partitionId":"{}","status":"completed","leaseId":"{lease}"}}"#,
                m["transactionId"].as_str().expect("txn"),
                m["partitionId"].as_str().expect("pid"),
            )
        })
        .collect();
    let acked = facade
        .ack(
            ctx(),
            AckReq {
                queue: Some("ap".into()),
                group: "apg".into(),
                raw: format!(r#"{{"acknowledgments":[{}]}}"#, acks.join(",")).into_bytes(),
            },
        )
        .await
        .expect("ack");
    let res = parse(&acked.body);
    assert!(
        res.as_array().is_some_and(|a| a.iter().all(|r| r["success"] == true)),
        "{res}"
    );
    // Nothing is ready now: the width falls back to one, the batch keeps a
    // sample (at least the minimum).
    let after = pop(true).await;
    assert_eq!(after["messages"].as_array().map(Vec::len), Some(0), "{after}");
    assert!(after["autopilot"]["batch"].as_u64().unwrap_or(0) >= 100, "{after}");

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
