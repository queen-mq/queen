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
        disk_high_pct: 85.0,
        disk_low_pct: 80.0,
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

/// W7 fuzzing finding: the raft push is dispatched before the HTTP boundary's
/// `MAX_TXN_BYTES` check, so the facade enforces the u16 frame limit itself —
/// an over-long transactionId is a 400, never a truncated frame (or, in debug,
/// a `debug_assert` panic in `pack_frames`). Same for a transaction's push op.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn an_over_long_transaction_id_is_rejected_not_packed() {
    let dir = scratch("txnlen");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    let long = "t".repeat(u16::MAX as usize + 1);
    let body = serde_json::json!({"items": [
        {"queue": "q", "payload": 1, "transactionId": "ok"},
        {"queue": "q", "payload": 2, "transactionId": long},
    ]});
    let err = facade
        .push(
            ctx(),
            PushReq {
                raw: body.to_string().into_bytes(),
            },
        )
        .await
        .expect_err("an over-long transactionId must be refused");
    let shown = format!("{err:?}");
    assert!(
        shown.contains("item 1") && shown.contains("65535"),
        "{shown}"
    );

    let bundle = serde_json::json!({"operations": [
        {"type": "push", "items": [{"queue": "q", "payload": 3, "transactionId": long}]},
    ]});
    let err = facade
        .transaction(
            ctx(),
            crate::rsm::facade::TxnReq {
                raw: bundle.to_string().into_bytes(),
            },
        )
        .await
        .expect_err("an over-long transactionId in a bundle must be refused");
    assert!(format!("{err:?}").contains("65535"), "{err:?}");

    // At the limit exactly is still a valid push.
    let edge = "e".repeat(u16::MAX as usize);
    let body = serde_json::json!({"items": [{"queue": "q", "payload": 4, "transactionId": edge}]});
    let ok = facade
        .push(
            ctx(),
            PushReq {
                raw: body.to_string().into_bytes(),
            },
        )
        .await
        .expect("a 65535-byte transactionId is within the limit");
    assert_eq!(parse(&ok.body)[0]["status"], "queued", "{}", ok.body);

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

/// A batch nacked as a whole on a spent budget (`retryLimit` 0) goes to the
/// DLQ as a batch, and every item's `dlq` flag says what happened to it; a
/// `retry` above the head stops the settling and its message redelivers.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_nacked_batch_goes_to_the_dlq_as_a_batch_and_each_flag_is_true() {
    let dir = scratch("ack-nack-batch");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let configured = facade
        .api(
            ctx(),
            ApiReq {
                method: "POST".into(),
                path: "/api/v1/configure".into(),
                query: None,
                body: serde_json::to_vec(&serde_json::json!({
                    "queue":"nack","options":{"retryLimit":0,"deadLetterQueue":true}
                }))
                .unwrap(),
            },
        )
        .await
        .expect("configure");
    assert_eq!(configured.status, 200, "{}", configured.body);

    // Round 1: a, b, c all nacked -> all three dead letters.
    // Round 2: d failed, e retry, f failed -> d filed; e and f come back.
    let mut filed = 0;
    for (round, (txns, statuses, want)) in [
        (
            ["a", "b", "c"],
            ["failed", "failed", "failed"],
            [true, true, true],
        ),
        (
            ["d", "e", "f"],
            ["failed", "retry", "failed"],
            [true, false, false],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let items: Vec<String> = txns
            .iter()
            .map(|t| format!(r#"{{"queue":"nack","payload":{{"t":"{t}"}},"transactionId":"{t}"}}"#))
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
        let popped = facade
            .pop_wildcard(
                ctx(),
                PopReq {
                    queue: "nack".into(),
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
        assert_eq!(
            pop["messages"].as_array().map(|m| m.len()),
            Some(3),
            "round {round}"
        );
        let pid = pop["partitionId"]
            .as_str()
            .expect("partitionId")
            .to_string();
        let lease = pop["leaseId"].as_str().expect("leaseId").to_string();
        let acks: Vec<String> = txns
            .iter()
            .zip(statuses)
            .map(|(t, s)| {
                format!(
                    r#"{{"transactionId":"{t}","partitionId":"{pid}","status":"{s}","leaseId":"{lease}"}}"#
                )
            })
            .collect();
        let acked = facade
            .ack(
                ctx(),
                AckReq {
                    queue: Some("nack".into()),
                    group: "__QUEUE_MODE__".into(),
                    raw: format!(
                        r#"{{"consumerGroup":"__QUEUE_MODE__","acknowledgments":[{}]}}"#,
                        acks.join(",")
                    )
                    .into_bytes(),
                },
            )
            .await
            .expect("ack");
        let results = parse(&acked.body);
        let flags: Vec<bool> = results
            .as_array()
            .expect("ack array")
            .iter()
            .map(|r| r["dlq"].as_bool().expect("dlq flag"))
            .collect();
        assert_eq!(flags, want, "round {round}: {}", acked.body);
        filed += want.iter().filter(|w| **w).count();
        let rows = dlq_rows_settled(&facade, filed).await;
        assert_eq!(rows.len(), filed, "round {round}");
    }
    let mut txns: Vec<String> = dlq_rows_settled(&facade, 4)
        .await
        .into_iter()
        .map(|r| r.txn)
        .collect();
    txns.sort();
    assert_eq!(txns, ["a", "b", "c", "d"]);

    // e and f come back.
    let popped = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "nack".into(),
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
    let back: Vec<String> = parse(&popped.body)["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .map(|m| m["transactionId"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(back, ["e", "f"]);

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
    // `role` is the membership role, `state` the Raft state (WP-4.8).
    assert_eq!(parse(&raft_status.body)["role"], "voter");
    assert_eq!(parse(&raft_status.body)["state"], "leader");
    // The machine under the node, for the Overview's broker strip.
    let host = &parse(&raft_status.body)["host"];
    assert!(host["cpus"].as_u64().is_some_and(|n| n >= 1), "{host}");
    assert!(host["rssBytes"].as_u64().is_some_and(|n| n > 0), "{host}");
    assert!(
        host["memLimitBytes"].as_u64().is_some_and(|n| n > 0),
        "{host}"
    );
    // Tests run with the gate off; the directory is whichever facade of this
    // test binary opened first, which another test may already have removed.
    if let Some(pct) = host["disk"]["usedPct"].as_f64() {
        assert!((0.0..=100.0).contains(&pct), "{host}");
        assert_eq!(host["disk"]["gate"], false, "{host}");
        assert_eq!(host["disk"]["writesRefused"], false, "{host}");
    }

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
        .api(ctx(), get("/api/v1/raft/liveness", None))
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

/// `/resources/partitions` ranks the tenant's partitions by pending, counts
/// pending as the queue detail does, and carries a lag only while there is
/// something to consume.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn the_partition_read_ranks_by_pending_and_lags_only_the_unconsumed() {
    let dir = scratch("partitions-read");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    // wide/0 holds 1, wide/1 holds 2, wide/2 holds 3; narrow/a holds 1.
    let items: Vec<Value> = (0..3)
        .flat_map(|p| {
            (0..=p).map(move |n| {
                serde_json::json!({"queue":"wide","partition":p.to_string(),"payload":{"n":n}})
            })
        })
        .chain(std::iter::once(
            serde_json::json!({"queue":"narrow","partition":"a","payload":{}}),
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
    let get = |query: Option<&str>| ApiReq {
        method: "GET".into(),
        path: "/api/v1/resources/partitions".into(),
        query: query.map(str::to_string),
        body: Vec::new(),
    };
    let rows = |body: &str| -> Vec<(String, String, i64, Value)> {
        parse(body)["partitions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| {
                (
                    p["queue"].as_str().unwrap().to_string(),
                    p["partition"].as_str().unwrap().to_string(),
                    p["pending"].as_i64().unwrap(),
                    p["lagSeconds"].clone(),
                )
            })
            .collect()
    };

    let all = facade.api(ctx(), get(None)).await.expect("all partitions");
    assert_eq!(all.status, 200, "{}", all.body);
    let body = parse(&all.body);
    assert_eq!(body["walked"], 4, "{body}");
    assert_eq!(body["truncated"], false, "{body}");
    let all = rows(&all.body);
    assert_eq!(
        all.iter()
            .map(|(q, p, n, _)| (q.as_str(), p.as_str(), *n))
            .take(2)
            .collect::<Vec<_>>(),
        [("wide", "2", 3), ("wide", "1", 2)]
    );
    assert_eq!(all.len(), 4);
    assert!(
        all.iter().all(|r| r.3.as_i64().is_some_and(|s| s >= 0)),
        "every partition holds a message, so every one has a lag: {all:?}"
    );

    // The same pending the queue detail shows.
    let detail = facade
        .api(
            ctx(),
            ApiReq {
                method: "GET".into(),
                path: "/api/v1/resources/queues/wide".into(),
                query: None,
                body: Vec::new(),
            },
        )
        .await
        .expect("detail");
    let from_detail: std::collections::BTreeMap<String, i64> = parse(&detail.body)["partitions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| {
            (
                p["name"].as_str().unwrap().to_string(),
                p["messages"]["pending"].as_i64().unwrap(),
            )
        })
        .collect();
    let wide = facade
        .api(ctx(), get(Some("queue=wide&limit=2")))
        .await
        .expect("one queue");
    assert_eq!(wide.status, 200, "{}", wide.body);
    assert_eq!(parse(&wide.body)["walked"], 3);
    let wide = rows(&wide.body);
    assert_eq!(wide.len(), 2, "limit=2: {wide:?}");
    for (_, p, n, _) in &wide {
        assert_eq!(from_detail.get(p), Some(n), "partition {p}");
    }

    // Consumed: nothing pending, no lag.
    facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "narrow".into(),
                group: None,
                batch: 10,
                auto_ack: true,
                wait: false,
                timeout_ms: 1000,
                options: Default::default(),
            },
        )
        .await
        .expect("pop");
    let narrow = facade
        .api(ctx(), get(Some("queue=narrow")))
        .await
        .expect("narrow");
    assert_eq!(
        rows(&narrow.body),
        [("narrow".to_string(), "a".to_string(), 0, Value::Null)]
    );

    let missing = facade
        .api(ctx(), get(Some("queue=nope")))
        .await
        .expect("no such queue");
    assert_eq!(missing.status, 404);

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

    let listed = facade
        .api(
            ctx(),
            ApiReq {
                method: "GET".into(),
                path: "/api/v1/messages".into(),
                query: Some("queue=secret&limit=1&offset=0".into()),
                body: Vec::new(),
            },
        )
        .await
        .expect("encrypted message list");
    assert_eq!(listed.status, 200);
    let listed = parse(&listed.body);
    assert_eq!(listed["messages"].as_array().map(Vec::len), Some(1));
    assert_eq!(listed["messages"][0]["data"]["secret"], 42, "{listed}");
    assert_eq!(listed["messages"][0]["producerSub"], "alice-producer");
    assert_eq!(listed["messages"][0]["isEncrypted"], true);
    assert_eq!(
        listed["pagination"],
        serde_json::json!({"limit": 1, "offset": 0})
    );

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// The message routes take the ids off the path as the wire carries them: a
/// transaction id like channel-go's `metric-sample|<uuid>` arrives encoded
/// (`%7C`), and one with a `/` or a `%` must still name its message.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_message_detail_decodes_the_ids_in_its_path() {
    let dir = scratch("msg-detail-encoded");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let txn = "metric-sample|a/b%c d";
    facade
        .push(
            ctx(),
            PushReq {
                raw: format!(
                    r#"{{"items":[{{"queue":"ids","payload":{{"n":1}},"transactionId":"{txn}"}}]}}"#
                )
                .into_bytes(),
            },
        )
        .await
        .expect("push");
    let popped = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "ids".into(),
                group: None,
                batch: 1,
                auto_ack: false,
                wait: false,
                timeout_ms: 1_000,
                options: Default::default(),
            },
        )
        .await
        .expect("pop");
    let pid = parse(&popped.body)["partitionId"]
        .as_str()
        .expect("partition id")
        .to_string();
    let get = |path: String| {
        let facade = &facade;
        async move {
            facade
                .api(
                    ctx(),
                    ApiReq {
                        method: "GET".into(),
                        path,
                        query: None,
                        body: Vec::new(),
                    },
                )
                .await
                .expect("message detail")
        }
    };
    let enc = crate::handlers::raft::percent_encode;
    let found = get(format!("/api/v1/messages/{}/{}", enc(&pid), enc(txn))).await;
    assert_eq!(found.status, 200, "{}", found.body);
    assert_eq!(parse(&found.body)["data"]["n"], 1, "{}", found.body);
    let missing = get(format!(
        "/api/v1/messages/{pid}/{}",
        enc("metric-sample|nope")
    ))
    .await;
    assert_eq!(missing.status, 404, "{}", missing.body);

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

/// A transaction's reply lost after it committed (the leader restarted,
/// changed, or the forward stream broke): the retry — the same request id —
/// is answered from the record the committed entry left (I6). The engine that
/// served the first attempt is gone, and preparing the retry again would find
/// its ack applied (the lease released) and roll it back: a transaction that
/// committed, reported rolled back (Jepsen W5: a phantom output).
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_retried_transaction_that_committed_is_answered_from_its_record() {
    let dir = scratch("txn-retry");
    let body;
    let rid;
    {
        let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
        facade
            .push(
                ctx(),
                PushReq {
                    raw: br#"{"items":[{"queue":"inq","payload":{"n":1},"transactionId":"t1"}]}"#
                        .to_vec(),
                },
            )
            .await
            .expect("push");
        let pop = pop_q(&facade, "inq").await;
        let pid = pop["partitionId"]
            .as_str()
            .expect("partitionId")
            .to_string();
        let lease = pop["leaseId"].as_str().expect("leaseId").to_string();
        body = serde_json::json!({
            "operations": [
                {"type": "ack", "transactionId": "t1", "partitionId": pid, "leaseId": lease},
                {"type": "push", "items": [{"queue": "outq", "payload": {"n": 1}, "transactionId": "o-first"}]}
            ]
        });
        let c = ctx();
        rid = c.request_id;
        let out = facade
            .transaction(
                c,
                crate::rsm::facade::TxnReq {
                    raw: body.to_string().into_bytes(),
                },
            )
            .await
            .expect("transaction");
        assert_eq!(parse(&out.body)["success"], true, "{}", out.body);
        facade.shutdown().await;
    }
    // A new engine (the restart): the retry of the same command.
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("reopen facade");
    let mut c = ctx();
    c.request_id = rid;
    let out = facade
        .transaction(
            c,
            crate::rsm::facade::TxnReq {
                raw: body.to_string().into_bytes(),
            },
        )
        .await
        .expect("retried transaction");
    let v = parse(&out.body);
    assert_eq!(
        v["success"], true,
        "the committed bundle, not a rollback: {v}"
    );
    let outm = pop_q(&facade, "outq").await;
    assert_eq!(
        outm["messages"].as_array().map(|m| m.len()),
        Some(1),
        "the output was pushed once: {outm}"
    );
    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
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

/// A step guarded by a lock: the bundle's push and its state write commit
/// only while the lock's row is still at the version the holder was handed.
/// The guard is a `check` — it does not write the row, so the holder's token
/// stays the one acquire gave it — and a holder that was replaced commits
/// nothing: no message, no state.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_transaction_commits_only_while_its_check_holds() {
    let dir = scratch("txn-check");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    // A node alone raises the cluster version to its own on its first tick.
    let end = std::time::Instant::now() + Duration::from_secs(10);
    while !facade.cluster_allows(crate::rsm::effect::VERSION_5) {
        assert!(
            std::time::Instant::now() < end,
            "the cluster version never reached 5"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let taken = txn(
        &facade,
        serde_json::json!({"kv": [{"op": "putIfAbsent", "ns": "locks", "key": "job",
                                   "value": {"owner": "a"}, "ttlSeconds": 600}]}),
    )
    .await;
    assert_eq!(taken["results"][0]["applied"], true, "{taken}");
    let token = taken["results"][0]["version"].as_u64().expect("a version");
    let step = |token: u64, n: u64, id: &str| {
        serde_json::json!({
            "operations": [{"type": "push", "items": [
                {"queue": "guarded", "payload": {"n": n}, "transactionId": id}]}],
            "kv": [
                {"op": "check", "ns": "locks", "key": "job", "expect": token, "required": true},
                {"op": "put", "ns": "work", "key": "state", "value": {"n": n}, "forever": true}
            ]
        })
    };

    let ok = txn(&facade, step(token, 1, "g1")).await;
    assert_eq!(ok["success"], true, "{ok}");
    let res = ok["results"].as_array().expect("results");
    assert_eq!(res.len(), 3, "{ok}");
    assert_eq!(res[1]["type"], "kv");
    assert_eq!(res[1]["op"], "check");
    assert_eq!(res[1]["applied"], true, "{ok}");
    assert_eq!(res[1]["version"].as_u64(), Some(token));
    assert!(
        res[1].get("value").is_none(),
        "a held check hands back no value: {ok}"
    );
    assert_eq!(res[2]["applied"], true, "{ok}");

    // The lock changes hands: the row is rewritten, so its version moves.
    let other = txn(
        &facade,
        serde_json::json!({"kv": [{"op": "put", "ns": "locks", "key": "job",
                                   "value": {"owner": "b"}, "ttlSeconds": 600, "expect": token}]}),
    )
    .await;
    let token_b = other["results"][0]["version"].as_u64().expect("a version");
    assert!(token_b > token, "a later write on the key: {other}");

    let stale = txn(&facade, step(token, 2, "g2")).await;
    assert_eq!(stale["success"], false, "{stale}");
    assert_eq!(stale["reason"], "kv_precondition", "{stale}");
    assert_eq!(
        stale["failedIndex"], 1,
        "the check, in the flat space: {stale}"
    );
    assert_eq!(stale["kvReason"], "version", "{stale}");
    assert_eq!(stale["version"].as_u64(), Some(token_b), "{stale}");
    assert_eq!(stale["value"], serde_json::json!({"owner": "b"}), "{stale}");

    let msgs = pop_q(&facade, "guarded").await;
    assert_eq!(
        msgs["messages"].as_array().map(|m| m.len()),
        Some(1),
        "only the real holder's message: {msgs}"
    );
    let state = txn(
        &facade,
        serde_json::json!({"kv": [{"op": "get", "ns": "work", "key": "state"}]}),
    )
    .await;
    assert_eq!(
        state["results"][0]["value"],
        serde_json::json!({"n": 1}),
        "{state}"
    );
    // The new holder's step commits with its own token.
    let ok_b = txn(&facade, step(token_b, 3, "g3")).await;
    assert_eq!(ok_b["success"], true, "{ok_b}");
    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
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
    assert!(
        manual.get("autopilot").is_none(),
        "no echo without the opt-in"
    );

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
        res.as_array()
            .is_some_and(|a| a.iter().all(|r| r["success"] == true)),
        "{res}"
    );
    // Nothing is ready now: the width falls back to one, the batch keeps a
    // sample (at least the minimum).
    let after = pop(true).await;
    assert_eq!(
        after["messages"].as_array().map(Vec::len),
        Some(0),
        "{after}"
    );
    assert!(
        after["autopilot"]["batch"].as_u64().unwrap_or(0) >= 100,
        "{after}"
    );

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// PLAN_CONFLATION §3.1/§3.3 on the raft engine: every answer to a group whose
/// effective policy conflates says so (`"conflation":true`), empty ones included,
/// and a request that named another value is told the stored one won
/// (`"conflationConflict":true`). The SDKs stop a consumer that asked for
/// conflation and got an answer without the key (client-go `checkConflationEcho`).
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_conflating_group_echoes_its_policy_on_every_answer() {
    let dir = scratch("conflation-echo");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    facade
        .push(
            ctx(),
            PushReq {
                raw: br#"{"items":[
                    {"queue":"cfl","partition":"p1","payload":{"n":1},"transactionId":"c1"},
                    {"queue":"cfl","partition":"p1","payload":{"n":2},"transactionId":"c2"},
                    {"queue":"cfl","partition":"p1","payload":{"n":3},"transactionId":"c3"}
                ]}"#
                .to_vec(),
            },
        )
        .await
        .expect("push");
    let pop = |group: Option<&str>, requested: Option<bool>| {
        let facade = &facade;
        let group = group.map(str::to_string);
        async move {
            facade
                .pop_wildcard(
                    ctx(),
                    PopReq {
                        queue: "cfl".into(),
                        options: conflate_like_the_receiver(&group, requested),
                        group,
                        batch: 10,
                        auto_ack: false,
                        wait: false,
                        timeout_ms: 1_000,
                    },
                )
                .await
                .expect("pop")
        }
    };

    // The registrar: a conflating pop of a new group gets the newest frame only.
    let first = pop(Some("g-cfl"), Some(true)).await;
    let body = parse(&first.body);
    let msgs = body["messages"].as_array().expect("messages");
    assert_eq!(msgs.len(), 1, "last value only: {body}");
    assert_eq!(msgs[0]["data"]["n"], 3, "{body}");
    assert_eq!(body["conflation"], true, "{body}");
    assert!(body.get("conflationConflict").is_none(), "{body}");
    assert!(first.conflation && !first.conflation_conflict);

    // Empty (the partition is leased): the STORED policy still answers, so the
    // receiver sends a 200 with this body, not a bodiless 204.
    let empty = pop(Some("g-cfl"), Some(true)).await;
    assert!(empty.empty, "{}", empty.body);
    assert_eq!(parse(&empty.body)["conflation"], true, "{}", empty.body);
    assert!(empty.conflation && !empty.conflation_conflict);

    // A request that names the other value: the stored policy wins, and says so.
    let disagree = pop(Some("g-cfl"), Some(false)).await;
    let body = parse(&disagree.body);
    assert_eq!(body["conflation"], true, "{body}");
    assert_eq!(body["conflationConflict"], true, "{body}");
    assert!(disagree.conflation && disagree.conflation_conflict);

    // A request that names nothing is told the policy, and is not a conflict.
    let silent = pop(Some("g-cfl"), None).await;
    let body = parse(&silent.body);
    assert_eq!(body["conflation"], true, "{body}");
    assert!(body.get("conflationConflict").is_none(), "{body}");

    // A plain group: neither key, full or empty (the receiver's 204 stays).
    let plain = pop(Some("g-plain"), None).await;
    let body = parse(&plain.body);
    assert_eq!(body["messages"].as_array().map(Vec::len), Some(3), "{body}");
    assert!(body.get("conflation").is_none(), "{body}");
    assert!(!plain.conflation && !plain.conflation_conflict);
    let plain_empty = pop(Some("g-plain"), Some(false)).await;
    assert!(plain_empty.empty && !plain_empty.conflation && !plain_empty.conflation_conflict);
    assert!(
        !plain_empty.body.contains("conflation"),
        "{}",
        plain_empty.body
    );

    // Queue mode has no group to hang a policy on: never a key, never a conflict.
    let queue_mode = pop(None, Some(false)).await;
    assert!(!queue_mode.conflation && !queue_mode.conflation_conflict);
    assert!(
        !queue_mode.body.contains("conflation"),
        "{}",
        queue_mode.body
    );

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// The receiver's `PopOptions` for a wildcard pop (data.rs `handle_pop`): the
/// conflating width is the batch, the requested value rides along verbatim.
fn conflate_like_the_receiver(
    group: &Option<String>,
    requested: Option<bool>,
) -> crate::rsm::facade::PopOptions {
    let conflate = requested == Some(true) && group.is_some();
    crate::rsm::facade::PopOptions {
        subscription_mode: "all".into(),
        max_parts: if conflate { 10 } else { 1 },
        conflate,
        conflate_requested: requested,
        ..Default::default()
    }
}

/// The per-queue counters behind queue-ops, for one queue of the default
/// tenant: (pushed, acked, failed acks).
fn counted(q: &str) -> (u64, u64, u64) {
    let snap = crate::metrics::global()
        .expect("process metrics")
        .per_queue
        .snapshot();
    let c = snap
        .get(&crate::handlers::tenant_queue_key(
            crate::config::DEFAULT_TENANT,
            q,
        ))
        .copied()
        .unwrap_or_default();
    (c.push_messages, c.ack_success, c.ack_failed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_transaction_and_a_stream_cycle_count_what_they_store_and_settle() {
    // The dashboard's push − ack only balances when every way a message enters
    // or leaves a queue is counted on it: stage showed pops never acked on a
    // stream's source and acks never pushed on a transaction's target.
    if crate::metrics::global().is_none() {
        crate::metrics::install_global(std::sync::Arc::new(crate::metrics::Metrics::new()));
    }
    let dir = scratch("counted");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let call = |method: &str, path: &str, body: Value| ApiReq {
        method: method.to_string(),
        path: path.to_string(),
        query: None,
        body: serde_json::to_vec(&body).unwrap(),
    };
    let push = |raw: &'static [u8]| facade.push(ctx(), PushReq { raw: raw.to_vec() });

    // A push stores three; a retry of one of them stores nothing.
    push(
        br#"{"items":[
            {"queue":"counted-in","payload":{"n":1},"transactionId":"ci1"},
            {"queue":"counted-in","payload":{"n":2},"transactionId":"ci2"},
            {"queue":"counted-in","payload":{"n":3},"transactionId":"ci3"}
        ]}"#,
    )
    .await
    .expect("push");
    let again =
        push(br#"{"items":[{"queue":"counted-in","payload":{"n":1},"transactionId":"ci1"}]}"#)
            .await
            .expect("push again");
    assert_eq!(
        parse(&again.body)[0]["status"],
        "duplicate",
        "{}",
        again.body
    );
    assert_eq!(
        counted("counted-in").0,
        3,
        "a duplicate adds no message to the queue"
    );

    // One transaction acks the three and forwards two.
    let pop = pop_q(&facade, "counted-in").await;
    let pid = pop["partitionId"]
        .as_str()
        .expect("partitionId")
        .to_string();
    let lease = pop["leaseId"].as_str().expect("leaseId").to_string();
    let done = txn(
        &facade,
        serde_json::json!({"operations": [
            {"type": "ack", "transactionId": "ci1", "partitionId": pid, "leaseId": lease},
            {"type": "ack", "transactionId": "ci2", "partitionId": pid, "leaseId": lease},
            {"type": "ack", "transactionId": "ci3", "partitionId": pid, "leaseId": lease},
            {"type": "push", "items": [
                {"queue": "counted-mid", "payload": {"m": 1}, "transactionId": "cm1"},
                {"queue": "counted-mid", "payload": {"m": 2}, "transactionId": "cm2"}
            ]}
        ]}),
    )
    .await;
    assert_eq!(done["success"], true, "{done}");
    assert_eq!(counted("counted-in"), (3, 3, 0), "the transaction's acks");
    assert_eq!(counted("counted-mid").0, 2, "the transaction's pushes");

    // A stream cycle settles those two and emits one.
    let registered = facade
        .api(
            ctx(),
            call(
                "POST",
                "/streams/v1/queries",
                serde_json::json!({
                    "name": "counted-stream",
                    "source_queue": "counted-mid",
                    "sink_queue": "counted-out",
                    "config_hash": "counted-1"
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
    let popped = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "counted-mid".into(),
                group: Some("counted-group".into()),
                batch: 2,
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
        .expect("stream pop");
    let pop = parse(&popped.body);
    assert_eq!(pop["messages"].as_array().map(Vec::len), Some(2), "{pop}");
    let cycle = facade
        .api(
            ctx(),
            call(
                "POST",
                "/streams/v1/cycle",
                serde_json::json!({
                    "query_id": qid,
                    "partition_id": pop["partitionId"],
                    "consumer_group": "counted-group",
                    "state_ops": [],
                    "push_items": [{"queue": "counted-out", "payload": {"sum": 3}}],
                    "ack": {"leaseId": pop["leaseId"], "status": "completed", "count": 2},
                    "release_lease": true
                }),
            ),
        )
        .await
        .expect("stream cycle");
    assert_eq!(parse(&cycle.body)["success"], true, "{}", cycle.body);
    assert_eq!(counted("counted-mid"), (2, 2, 0), "the cycle's acks");
    assert_eq!(counted("counted-out").0, 1, "the cycle's sink push");

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn the_backlog_sample_is_what_the_overview_reports() {
    let dir = scratch("backlog");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    // A queue that exists and holds nothing.
    let made = facade
        .api(
            ctx(),
            ApiReq {
                method: "POST".into(),
                path: "/api/v1/configure".into(),
                query: None,
                body: br#"{"queue":"backlog-empty","options":{}}"#.to_vec(),
            },
        )
        .await
        .expect("configure");
    assert_eq!(made.status, 200, "{}", made.body);
    facade
        .push(
            ctx(),
            PushReq {
                raw: br#"{"items":[
                    {"queue":"backlog-q","payload":1,"transactionId":"b1"},
                    {"queue":"backlog-q","payload":2,"transactionId":"b2"},
                    {"queue":"backlog-q","payload":3,"transactionId":"b3"},
                    {"queue":"backlog-q","payload":4,"transactionId":"b4"},
                    {"queue":"backlog-q","payload":5,"transactionId":"b5"}
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
                queue: "backlog-q".into(),
                group: None,
                batch: 2,
                auto_ack: false,
                wait: false,
                timeout_ms: 1_000,
                options: Default::default(),
            },
        )
        .await
        .expect("pop two");
    assert_eq!(
        parse(&popped.body)["messages"].as_array().map(Vec::len),
        Some(2)
    );

    let overview = facade
        .api(
            ctx(),
            ApiReq {
                method: "GET".into(),
                path: "/api/v1/resources/overview".into(),
                query: None,
                body: Vec::new(),
            },
        )
        .await
        .expect("overview");
    let messages = parse(&overview.body)["messages"].clone();
    let sample = facade.backlogs();
    let of = |q: &str| {
        sample
            .iter()
            .find(|(t, name, _, _)| t == crate::config::DEFAULT_TENANT && name == q)
            .map(|r| (r.2, r.3))
    };
    assert_eq!(
        of(""),
        Some((3, 2)),
        "the tenant: three wait, two are leased: {sample:?}"
    );
    assert_eq!(
        (
            messages["pending"].as_i64(),
            messages["processing"].as_i64()
        ),
        (Some(3), Some(2)),
        "the chart's right edge is the overview's number: {messages}"
    );
    assert_eq!(
        of("backlog-q"),
        Some((3, 2)),
        "the queue's own reading: {sample:?}"
    );
    assert_eq!(
        of("backlog-empty"),
        None,
        "an empty queue writes no row: {sample:?}"
    );

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// The bodies made of client-built frames: the framed push and the binary
// transaction
// ---------------------------------------------------------------------------

/// One group of a framed body, as a client writes it: the messages in the
/// storage layout (`crate::frames`), compressed once. Returns the group's bytes
/// and the message ids the "client" minted.
fn frames_group_raw(
    queue: &str,
    partition: &str,
    msgs: &[(&str, &[u8])],
) -> (Vec<u8>, Vec<String>) {
    let mut raw = Vec::new();
    let mut ids = Vec::new();
    for (txn, payload) in msgs {
        let id = crate::util::uuidv7_bytes();
        ids.push(crate::frames::uuid_bytes_to_string(&id));
        raw.extend_from_slice(&crate::frames::pack_frames(&[crate::frames::FrameIn {
            message_id: id,
            txn,
            trace_id: None,
            producer_sub: None,
            payload,
            encrypted: false,
        }]));
    }
    (group_of(queue, partition, msgs.len(), &raw), ids)
}

fn group_of(queue: &str, partition: &str, count: usize, raw: &[u8]) -> Vec<u8> {
    let z = zstd::bulk::compress(raw, 1).expect("zstd");
    let mut g = Vec::new();
    g.extend_from_slice(&(queue.len() as u16).to_le_bytes());
    g.extend_from_slice(queue.as_bytes());
    g.extend_from_slice(&(partition.len() as u16).to_le_bytes());
    g.extend_from_slice(partition.as_bytes());
    g.extend_from_slice(&(count as u32).to_le_bytes());
    g.extend_from_slice(&(z.len() as u32).to_le_bytes());
    g.extend_from_slice(&z);
    g
}

fn frames_group(queue: &str, partition: &str, msgs: &[(&str, Value)]) -> (Vec<u8>, Vec<String>) {
    let payloads: Vec<String> = msgs.iter().map(|(_, v)| v.to_string()).collect();
    let refs: Vec<(&str, &[u8])> = msgs
        .iter()
        .zip(&payloads)
        .map(|((t, _), p)| (*t, p.as_bytes()))
        .collect();
    frames_group_raw(queue, partition, &refs)
}

/// `"QTB1" | flags | u32 len, head | u32 groups | the groups`.
fn txn_binary(flags: u8, head: &Value, groups: &[Vec<u8>]) -> Vec<u8> {
    let head = head.to_string();
    let mut b = b"QTB1".to_vec();
    b.push(flags);
    b.extend_from_slice(&(head.len() as u32).to_le_bytes());
    b.extend_from_slice(head.as_bytes());
    b.extend_from_slice(&(groups.len() as u32).to_le_bytes());
    for g in groups {
        b.extend_from_slice(g);
    }
    b
}

async fn txn_raw(facade: &RaftFacade, raw: Vec<u8>) -> (u16, Value) {
    let out = facade
        .transaction(ctx(), crate::rsm::facade::TxnReq { raw })
        .await
        .expect("transaction");
    (out.status, parse(&out.body))
}

fn popped(pop: &Value) -> Vec<Value> {
    pop["messages"].as_array().cloned().unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_binary_transaction_acks_and_pushes_what_the_json_one_does() {
    let dir = scratch("txn-bin");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    facade
        .push(
            ctx(),
            PushReq {
                raw: br#"{"items":[
                    {"queue":"bin-in","payload":{"n":1},"transactionId":"t1"},
                    {"queue":"bin-in","payload":{"n":2},"transactionId":"t2"}
                ]}"#
                .to_vec(),
            },
        )
        .await
        .expect("push");
    let pop = pop_q(&facade, "bin-in").await;
    assert_eq!(popped(&pop).len(), 2, "{pop}");
    let pid = pop["partitionId"]
        .as_str()
        .expect("partitionId")
        .to_string();
    let lease = pop["leaseId"].as_str().expect("leaseId").to_string();

    // The acks in the head, the pushes as two groups of client-built frames.
    let (g1, ids1) = frames_group(
        "bin-out",
        "Default",
        &[
            ("o1", serde_json::json!({"sum": 3})),
            ("o2", serde_json::json!({"text": "a \"quoted\"\nline"})),
            ("o3", serde_json::json!([1, 2, 3])),
        ],
    );
    let (g2, ids2) = frames_group("bin-out", "other", &[("o4", serde_json::json!(4))]);
    let head = serde_json::json!({"operations": [
        {"type": "ack", "transactionId": "t1", "partitionId": pid, "leaseId": lease},
        {"type": "ack", "transactionId": "t2", "partitionId": pid, "leaseId": lease},
    ]});
    let (status, ok) = txn_raw(&facade, txn_binary(0, &head, &[g1, g2])).await;
    assert_eq!(status, 200, "{ok}");
    assert_eq!(ok["success"], true, "{ok}");
    // The head's operations first, then the groups' messages, in order.
    let res = ok["results"].as_array().expect("results");
    assert_eq!(res.len(), 6, "{ok}");
    assert_eq!(res[0]["type"], "ack");
    assert_eq!(res[1]["type"], "ack");
    let ids: Vec<&String> = ids1.iter().chain(&ids2).collect();
    for (k, txn) in ["o1", "o2", "o3", "o4"].into_iter().enumerate() {
        let r = &res[2 + k];
        assert_eq!(r["type"], "push", "{ok}");
        assert_eq!(r["index"], 2 + k, "{ok}");
        assert_eq!(r["success"], true, "{ok}");
        assert_eq!(r["transactionId"], txn, "{ok}");
        assert_eq!(
            r["messageId"],
            ids[k].as_str(),
            "the id is the client's: {ok}"
        );
    }

    // The inputs are consumed and the outputs are there, payloads untouched.
    assert_eq!(popped(&pop_q(&facade, "bin-in").await).len(), 0);
    let mut got: Vec<(String, Value)> = Vec::new();
    for _ in 0..2 {
        for m in popped(&pop_q(&facade, "bin-out").await) {
            got.push((
                m["transactionId"].as_str().unwrap_or_default().to_string(),
                m["data"].clone(),
            ));
        }
    }
    got.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        got,
        vec![
            ("o1".to_string(), serde_json::json!({"sum": 3})),
            (
                "o2".to_string(),
                serde_json::json!({"text": "a \"quoted\"\nline"})
            ),
            ("o3".to_string(), serde_json::json!([1, 2, 3])),
            ("o4".to_string(), serde_json::json!(4)),
        ]
    );

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_binary_transaction_is_all_or_nothing() {
    let dir = scratch("txn-bin-rollback");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    // An ack of a message nobody holds: the bundle is refused and stores nothing.
    let (g, _) = frames_group("bin-rb", "Default", &[("r1", serde_json::json!({"n": 1}))]);
    let head = serde_json::json!({"operations": [
        {"type": "ack", "transactionId": "nobody", "partitionId": "424242", "leaseId": "no-lease"},
    ]});
    let (status, no) = txn_raw(&facade, txn_binary(0, &head, &[g.clone()])).await;
    assert_eq!(status, 200, "{no}");
    assert_eq!(no["success"], false, "{no}");
    assert_eq!(
        popped(&pop_q(&facade, "bin-rb").await).len(),
        0,
        "nothing was stored"
    );

    // The same group alone commits; sent again, the transaction id is a duplicate
    // and the bundle as a whole is refused.
    let (status, ok) = txn_raw(&facade, txn_binary(0, &serde_json::json!({}), &[g.clone()])).await;
    assert_eq!((status, ok["success"].as_bool()), (200, Some(true)), "{ok}");
    assert_eq!(popped(&pop_q(&facade, "bin-rb").await).len(), 1);

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_binary_transaction_answers_with_counts_when_asked() {
    let dir = scratch("txn-bin-short");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    // Two messages, the second with the first one's transaction id: within one
    // bundle the second collapses into the first, as in the JSON body.
    let (g, _) = frames_group(
        "bin-short",
        "Default",
        &[
            ("s1", serde_json::json!({"n": 1})),
            ("s2", serde_json::json!({"n": 2})),
            ("s1", serde_json::json!({"n": 3})),
        ],
    );
    let head = serde_json::json!({
        "kv": [{"op": "put", "ns": "st", "key": "cursor", "value": {"at": 2}, "ttlSeconds": 600}]
    });
    let (status, ok) = txn_raw(&facade, txn_binary(2, &head, &[g])).await;
    assert_eq!(status, 200, "{ok}");
    assert_eq!(ok["success"], true, "{ok}");
    assert_eq!(ok["pushed"], 2, "{ok}");
    assert_eq!(ok["duplicate"], 1, "{ok}");
    assert_eq!(ok["acked"], 0, "{ok}");
    // Only the rider's result is rendered, at its flat index: after the three pushes.
    let res = ok["results"].as_array().expect("results");
    assert_eq!(res.len(), 1, "{ok}");
    assert_eq!(res[0]["type"], "kv", "{ok}");
    assert_eq!(res[0]["index"], 3, "{ok}");
    assert_eq!(popped(&pop_q(&facade, "bin-short").await).len(), 2);

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_binary_transaction_that_is_not_well_formed_is_refused_whole() {
    let dir = scratch("txn-bin-bad");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let (good, _) = frames_group("bin-bad", "Default", &[("b1", serde_json::json!(1))]);
    let head = serde_json::json!({});

    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();
    cases.push(("unknown flags", txn_binary(1, &head, &[good.clone()])));
    let mut cut = txn_binary(0, &head, &[good.clone()]);
    cut.truncate(cut.len() - 3);
    cases.push(("truncated", cut));
    let mut more = txn_binary(0, &head, &[good.clone()]);
    more.push(0);
    cases.push(("bytes after the last group", more));
    let (not_json, _) = frames_group_raw("bin-bad", "Default", &[("b2", b"not json")]);
    cases.push((
        "a payload that is not JSON",
        txn_binary(0, &head, &[not_json]),
    ));
    // A frame whose id is not a version-7 UUID.
    let mut raw = crate::frames::pack_frames(&[crate::frames::FrameIn {
        message_id: crate::util::uuidv7_bytes(),
        txn: "b3",
        trace_id: None,
        producer_sub: None,
        payload: b"1",
        encrypted: false,
    }]);
    raw[4 + 1 + 6] = 0x40; // the version nibble
    cases.push((
        "an id that is not version 7",
        txn_binary(0, &head, &[group_of("bin-bad", "Default", 1, &raw)]),
    ));
    // A count that does not match the frames.
    let one = crate::frames::pack_frames(&[crate::frames::FrameIn {
        message_id: crate::util::uuidv7_bytes(),
        txn: "b4",
        trace_id: None,
        producer_sub: None,
        payload: b"1",
        encrypted: false,
    }]);
    cases.push((
        "more frames announced than sent",
        txn_binary(0, &head, &[group_of("bin-bad", "Default", 2, &one)]),
    ));
    for (what, raw) in cases {
        let (status, body) = txn_raw(&facade, raw).await;
        assert_eq!(status, 400, "{what}: {body}");
        assert_eq!(body["success"], false, "{what}: {body}");
        assert_eq!(body["reason"], "bad_request", "{what}: {body}");
    }
    assert_eq!(
        popped(&pop_q(&facade, "bin-bad").await).len(),
        0,
        "nothing was stored"
    );

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_framed_push_stores_the_clients_messages() {
    let dir = scratch("push-frames");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let (g, ids) = frames_group(
        "framed",
        "Default",
        &[
            ("f1", serde_json::json!({"n": 1})),
            ("f2", serde_json::json!({"n": 2})),
        ],
    );
    // "QPB1" | flags (8 = client-built frames) | u32 groups | the groups
    let mut raw = b"QPB1".to_vec();
    raw.push(8);
    raw.extend_from_slice(&1u32.to_le_bytes());
    raw.extend_from_slice(&g);
    let out = facade.push(ctx(), PushReq { raw }).await.expect("push");
    let res = parse(&out.body);
    assert_eq!(res[0]["status"], "queued", "{}", out.body);
    assert_eq!(res[1]["status"], "queued", "{}", out.body);
    let msgs = popped(&pop_q(&facade, "framed").await);
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0]["transactionId"], "f1");
    assert_eq!(msgs[0]["data"]["n"], 1);
    assert_eq!(msgs[1]["data"]["n"], 2);
    assert_eq!(
        msgs[0]["id"],
        ids[0].as_str(),
        "the id is the client's: {}",
        msgs[0]
    );

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A framed body compressed again as a whole (flags 1 and 8) hides its groups,
/// and the size each one declares, inside the outer frame: it is refused, and
/// nothing is stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_framed_push_compressed_again_as_a_whole_is_refused() {
    let dir = scratch("push-frames-twice");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let (g, _) = frames_group(
        "framed-twice",
        "Default",
        &[("f1", serde_json::json!({"n": 1}))],
    );
    let mut groups = 1u32.to_le_bytes().to_vec();
    groups.extend_from_slice(&g);
    // "QPB1" | flags (1 = one zstd frame, 8 = client-built frames) | u32 raw_len | the frame
    let mut raw = b"QPB1".to_vec();
    raw.push(1 | 8);
    raw.extend_from_slice(&(groups.len() as u32).to_le_bytes());
    raw.extend_from_slice(&zstd::bulk::compress(&groups, 1).expect("zstd"));
    let err = facade
        .push(ctx(), PushReq { raw })
        .await
        .expect_err("flags 1 and 8 together must be refused");
    let shown = format!("{err:?}");
    assert!(
        shown.contains("bad_body") && shown.contains("flags 1 and 8 together"),
        "{shown}"
    );
    assert_eq!(
        popped(&pop_q(&facade, "framed-twice").await).len(),
        0,
        "nothing was stored"
    );

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// A broker with an encryption key: the client's frames are kept, and answered
// as they are, for every queue that does not encrypt
// ---------------------------------------------------------------------------

/// One group of a framed body, compressed as the broker never does (level 19,
/// with a checksum), so a stored record shows whose bytes it holds. Each
/// message is `{"txn": <txn>, "pad": <600 x>}`, over the codec's 512-byte
/// floor alone. Returns the group and its zstd frame.
fn client_group(queue: &str, txns: &[&str]) -> (Vec<u8>, Vec<u8>) {
    let mut raw = Vec::new();
    for txn in txns {
        let payload = serde_json::json!({"txn": txn, "pad": "x".repeat(600)}).to_string();
        raw.extend_from_slice(&crate::frames::pack_frames(&[crate::frames::FrameIn {
            message_id: crate::util::uuidv7_bytes(),
            txn,
            trace_id: None,
            producer_sub: None,
            payload: payload.as_bytes(),
            encrypted: false,
        }]));
    }
    let mut c = zstd::bulk::Compressor::new(19).expect("zstd");
    c.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))
        .expect("checksum");
    let z = c.compress(&raw).expect("zstd");
    let mut g = Vec::new();
    g.extend_from_slice(&(queue.len() as u16).to_le_bytes());
    g.extend_from_slice(queue.as_bytes());
    g.extend_from_slice(&7u16.to_le_bytes());
    g.extend_from_slice(b"Default");
    g.extend_from_slice(&(txns.len() as u32).to_le_bytes());
    g.extend_from_slice(&(z.len() as u32).to_le_bytes());
    g.extend_from_slice(&z);
    (g, z)
}

async fn keyed_facade(tag: &str) -> (RaftFacade, PathBuf) {
    let dir = scratch(tag);
    let mut facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    facade.set_encryption_for_test(crate::encryption::Encryption::for_test([7; 32]));
    (facade, dir)
}

async fn set_queue_encryption(facade: &RaftFacade, queue: &str, on: bool) {
    let out = facade
        .api(
            ctx(),
            ApiReq {
                method: "POST".into(),
                path: "/api/v1/configure".into(),
                query: None,
                body: serde_json::to_vec(&serde_json::json!({
                    "queue": queue,
                    "options": {"encryptionEnabled": on}
                }))
                .unwrap(),
            },
        )
        .await
        .expect("configure");
    assert_eq!(out.status, 200, "{}", out.body);
}

/// `"QPB1" | flags 8 (client-built frames) | u32 groups | the groups`.
async fn push_framed(facade: &RaftFacade, groups: &[&[u8]]) {
    let mut raw = b"QPB1".to_vec();
    raw.push(8);
    raw.extend_from_slice(&(groups.len() as u32).to_le_bytes());
    for g in groups {
        raw.extend_from_slice(g);
    }
    let out = facade.push(ctx(), PushReq { raw }).await.expect("push");
    for r in parse(&out.body).as_array().expect("one result per message") {
        assert_eq!(r["status"], "queued", "{}", out.body);
    }
}

/// A pop that asks for the stored records (`frames=true`), auto-acked.
async fn pop_framed(facade: &RaftFacade, queue: &str) -> crate::rsm::facade::PopOut {
    let mut c = ctx();
    c.pop_frames = true;
    facade
        .pop_wildcard(
            c,
            PopReq {
                queue: queue.into(),
                group: None,
                batch: 10,
                auto_ack: true,
                wait: false,
                timeout_ms: 1000,
                options: Default::default(),
            },
        )
        .await
        .expect("pop")
}

/// Each record of a framed pop answer (`render_pop_frames`' layout, after the
/// route's first line): its codec byte and its bytes.
fn framed_records(b: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let u16_at = |p: &mut usize| {
        let v = u16::from_le_bytes([b[*p], b[*p + 1]]) as usize;
        *p += 2;
        v
    };
    let u32_at = |p: &mut usize| {
        let v = u32::from_le_bytes([b[*p], b[*p + 1], b[*p + 2], b[*p + 3]]) as usize;
        *p += 4;
        v
    };
    let mut p = 0usize;
    for _ in 0..3 {
        // queue, leaseId, consumerGroup
        let n = u16_at(&mut p);
        p += n;
    }
    let mut out = Vec::new();
    for _ in 0..u32_at(&mut p) {
        p += 8; // partitionId
        let n = u16_at(&mut p);
        p += n + 8 + 8 + 4; // partition, start, end, deliveryAttempt
        for _ in 0..u32_at(&mut p) {
            p += 8 + 8 + 4; // base_offset, created_at_us, count
            let codec = b[p];
            p += 1;
            let len = u32_at(&mut p);
            out.push((codec, b[p..p + len].to_vec()));
            p += len;
        }
    }
    assert_eq!(p, b.len(), "the whole answer read");
    out
}

/// The transaction ids and the `txn` each payload carries, of a JSON pop answer.
fn txns_and_data(out: &crate::rsm::facade::PopOut) -> Vec<(String, String)> {
    popped(&parse(&out.body))
        .iter()
        .map(|m| {
            (
                m["transactionId"].as_str().unwrap_or_default().to_string(),
                m["data"]["txn"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

fn pairs(txns: &[&str]) -> Vec<(String, String)> {
    txns.iter()
        .map(|t| (t.to_string(), t.to_string()))
        .collect()
}

/// The key encrypts only the queues that ask for it: a framed push to any
/// other queue is stored as the client compressed it, and a framed pop answers
/// those bytes as they are.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn with_a_key_a_plain_queue_keeps_the_clients_bytes_and_pops_framed() {
    let (facade, dir) = keyed_facade("key-plain").await;
    let (g, z) = client_group("plain-frames", &["p1", "p2"]);
    push_framed(&facade, &[&g]).await;
    let out = pop_framed(&facade, "plain-frames").await;
    let records = framed_records(out.frames.as_deref().expect("a framed answer"));
    assert_eq!(records, vec![(1, z)], "one record: the client's zstd frame");

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A queue that encrypts packs the frames again around the encrypted payloads,
/// and a framed pop of it answers JSON, decrypted.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn with_a_key_an_encrypted_queue_pops_json_decrypted_when_frames_are_asked() {
    let (facade, dir) = keyed_facade("key-secret").await;
    set_queue_encryption(&facade, "secret-frames", true).await;
    let (g, _) = client_group("secret-frames", &["s1", "s2"]);
    push_framed(&facade, &[&g]).await;
    let out = pop_framed(&facade, "secret-frames").await;
    assert!(
        out.frames.is_none(),
        "an encrypted message goes out as JSON"
    );
    assert_eq!(txns_and_data(&out), pairs(&["s1", "s2"]), "{}", out.body);

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Switching a queue's encryption off leaves its older messages encrypted on
/// disk: an answer that holds one is JSON, decrypted. Once they are consumed,
/// the queue's answers are framed again.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_queue_whose_encryption_was_switched_off_pops_its_older_messages_as_json() {
    let (facade, dir) = keyed_facade("key-was-secret").await;
    set_queue_encryption(&facade, "was-secret", true).await;
    let (old, _) = client_group("was-secret", &["old"]);
    push_framed(&facade, &[&old]).await;
    set_queue_encryption(&facade, "was-secret", false).await;
    let (new, _) = client_group("was-secret", &["new"]);
    push_framed(&facade, &[&new]).await;

    let out = pop_framed(&facade, "was-secret").await;
    assert!(out.frames.is_none(), "the older message is still encrypted");
    assert_eq!(txns_and_data(&out), pairs(&["old", "new"]), "{}", out.body);

    let (newer, z) = client_group("was-secret", &["newer"]);
    push_framed(&facade, &[&newer]).await;
    let out = pop_framed(&facade, "was-secret").await;
    let records = framed_records(out.frames.as_deref().expect("a framed answer"));
    assert_eq!(records, vec![(1, z)]);

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A binary transaction that pushes to an encrypted and a plain queue keeps
/// the client's bytes for the plain queue's group only.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn with_a_key_a_binary_transaction_keeps_the_clients_bytes_for_its_plain_queue() {
    let (facade, dir) = keyed_facade("key-txn").await;
    set_queue_encryption(&facade, "txn-secret", true).await;
    let (secret, _) = client_group("txn-secret", &["s1"]);
    let (plain, z) = client_group("txn-plain", &["p1"]);
    let (status, ok) = txn_raw(
        &facade,
        txn_binary(0, &serde_json::json!({}), &[secret, plain]),
    )
    .await;
    assert_eq!(status, 200, "{ok}");
    assert_eq!(ok["success"], true, "{ok}");

    let out = pop_framed(&facade, "txn-plain").await;
    let records = framed_records(out.frames.as_deref().expect("a framed answer"));
    assert_eq!(records, vec![(1, z)], "the client's zstd frame");
    let out = pop_framed(&facade, "txn-secret").await;
    assert!(
        out.frames.is_none(),
        "an encrypted message goes out as JSON"
    );
    assert_eq!(txns_and_data(&out), pairs(&["s1"]), "{}", out.body);

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// One `POST` through the facade: the status and parsed body of an answer, or
/// the code of a rejection (which the HTTP layer renders as a 4xx).
async fn post_api(facade: &RaftFacade, path: &str, body: Value) -> Result<(u16, Value), String> {
    let req = ApiReq {
        method: "POST".to_string(),
        path: path.to_string(),
        query: None,
        body: serde_json::to_vec(&body).unwrap(),
    };
    match facade.api(ctx(), req).await {
        Ok(out) => Ok((out.status, parse(&out.body))),
        Err(crate::rsm::facade::RsmError::Rejected { code, .. }) => Err(code),
        Err(e) => panic!("{path}: {e}"),
    }
}

/// Every page of `POST /streams/v1/state/partitions`, following `next`.
async fn state_partitions_walk(
    facade: &RaftFacade,
    qid: &str,
    limit: Option<u64>,
    untouched_for_ms: Option<i64>,
) -> Vec<Vec<(String, String)>> {
    let mut pages = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let mut body = serde_json::json!({"query_id": qid});
        if let Some(n) = limit {
            body["limit"] = n.into();
        }
        if let Some(ms) = untouched_for_ms {
            body["untouched_for_ms"] = ms.into();
        }
        if let Some(a) = &after {
            body["after"] = a.clone().into();
        }
        let (status, page) = post_api(facade, "/streams/v1/state/partitions", body)
            .await
            .expect("listing");
        assert_eq!(status, 200, "{page}");
        let rows: Vec<(String, String)> = page["partitions"]
            .as_array()
            .expect("partitions")
            .iter()
            .map(|p| {
                assert_eq!(p["queue"], "sp-src", "{p}");
                assert!(p["updated_at"].is_string(), "{p}");
                (
                    p["partition_id"]
                        .as_str()
                        .expect("partition_id")
                        .to_string(),
                    p["partition"].as_str().expect("partition").to_string(),
                )
            })
            .collect();
        pages.push(rows);
        match page["next"].as_str() {
            Some(n) => after = Some(n.to_string()),
            None => return pages,
        }
        assert!(pages.len() < 50, "the walk does not end: {pages:?}");
    }
}

/// `POST /streams/v1/state/partitions` lists the partitions holding a query's
/// state, pages in partition id order (under test a call reads 2 rows before
/// it stops at a partition boundary, so the walks cross pages), and keeps only
/// the partitions whose newest row is old enough when asked to.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn stream_state_partitions_lists_pages_and_filters_by_age() {
    let dir = scratch("state-partitions");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    facade
        .push(
            ctx(),
            PushReq {
                raw: br#"{"items":[
                    {"queue":"sp-src","partition":"a","payload":{"n":1}},
                    {"queue":"sp-src","partition":"b","payload":{"n":2}},
                    {"queue":"sp-src","partition":"c","payload":{"n":3}},
                    {"queue":"sp-src","partition":"d","payload":{"n":4}}
                ]}"#
                .to_vec(),
            },
        )
        .await
        .expect("push");
    let mut pids = std::collections::BTreeMap::new();
    for _ in 0..4 {
        let popped = facade
            .pop_wildcard(
                ctx(),
                PopReq {
                    queue: "sp-src".into(),
                    group: Some("sp-group".into()),
                    batch: 1,
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
        let name = pop["messages"][0]["partition"]
            .as_str()
            .expect("partition")
            .to_string();
        let pid = pop["partitionId"]
            .as_str()
            .expect("partitionId")
            .to_string();
        pids.insert(name, pid);
    }
    assert_eq!(pids.len(), 4, "{pids:?}");
    let pid = |name: &str| pids[name].clone();

    let (status, registered) = post_api(
        &facade,
        "/streams/v1/queries",
        serde_json::json!({"name":"sp","source_queue":"sp-src","config_hash":"sp-1"}),
    )
    .await
    .expect("register");
    assert_eq!(status, 200, "{registered}");
    let qid = registered["query_id"]
        .as_str()
        .expect("query id")
        .to_string();

    // State on a (one row), b (two rows) and d (one row); none on c. No ack:
    // a flush's shape.
    let write = |partition: String, ops: Value| {
        let qid = qid.clone();
        let facade = &facade;
        async move {
            let (status, out) = post_api(
                facade,
                "/streams/v1/cycle",
                serde_json::json!({"query_id": qid, "partition_id": partition,
                    "consumer_group": "sp-group", "state_ops": ops, "push_items": []}),
            )
            .await
            .expect("cycle");
            assert_eq!(status, 200, "{out}");
            assert_eq!(out["success"], true, "{out}");
        }
    };
    write(
        pid("a"),
        serde_json::json!([{"type":"upsert","key":"k1","value":{"n":1}}]),
    )
    .await;
    write(
        pid("b"),
        serde_json::json!([
            {"type":"upsert","key":"k1","value":{"n":1}},
            {"type":"upsert","key":"k2","value":{"n":2}}
        ]),
    )
    .await;
    write(
        pid("d"),
        serde_json::json!([{"type":"upsert","key":"k1","value":{"n":4}}]),
    )
    .await;

    let sorted = |mut v: Vec<(String, String)>| {
        v.sort();
        v
    };
    let a = (pid("a"), "a".to_string());
    let b = (pid("b"), "b".to_string());
    let d = (pid("d"), "d".to_string());

    // All of it, whatever the page size; never a partition twice. Unlimited,
    // the 2-row scan budget still stops the first page after b (4 rows in).
    for limit in [None, Some(1), Some(2), Some(3)] {
        let pages = state_partitions_walk(&facade, &qid, limit, None).await;
        if let Some(n) = limit {
            assert!(pages.iter().all(|p| p.len() <= n as usize), "{pages:?}");
        } else {
            assert_eq!(pages.len(), 2, "the scan budget pages: {pages:?}");
        }
        assert_eq!(
            sorted(pages.concat()),
            sorted(vec![a.clone(), b.clone(), d.clone()]),
            "limit {limit:?}: {pages:?}"
        );
    }

    // Age: a is written again, b is left alone.
    tokio::time::sleep(Duration::from_millis(300)).await;
    write(
        pid("a"),
        serde_json::json!([{"type":"upsert","key":"k1","value":{"n":2}}]),
    )
    .await;
    let pages = state_partitions_walk(&facade, &qid, None, Some(200)).await;
    assert_eq!(
        sorted(pages.concat()),
        sorted(vec![b.clone(), d.clone()]),
        "a was just written: {pages:?}"
    );
    let pages = state_partitions_walk(&facade, &qid, None, Some(60_000)).await;
    assert!(
        pages.concat().is_empty(),
        "nothing is untouched for a minute: {pages:?}"
    );

    // Deleting its rows drops b.
    write(
        pid("b"),
        serde_json::json!([{"type":"delete","key":"k1"},{"type":"delete","key":"k2"}]),
    )
    .await;
    let pages = state_partitions_walk(&facade, &qid, None, None).await;
    assert_eq!(
        sorted(pages.concat()),
        sorted(vec![a.clone(), d.clone()]),
        "{pages:?}"
    );

    // Past the last partition: an empty page.
    let (status, page) = post_api(
        &facade,
        "/streams/v1/state/partitions",
        serde_json::json!({"query_id": qid, "after": u64::MAX.to_string()}),
    )
    .await
    .expect("past the end");
    assert_eq!(status, 200, "{page}");
    assert_eq!(page["partitions"], serde_json::json!([]), "{page}");
    assert!(page["next"].is_null(), "{page}");

    // Refusals.
    let unknown = post_api(
        &facade,
        "/streams/v1/state/partitions",
        serde_json::json!({"query_id": "00000000-0000-7000-8000-000000000000"}),
    )
    .await
    .expect("unknown query answers");
    assert_eq!(unknown.0, 404, "{}", unknown.1);
    for bad in [
        serde_json::json!({}),
        serde_json::json!({"query_id": qid, "after": "x"}),
        serde_json::json!({"query_id": qid, "limit": 0}),
        serde_json::json!({"query_id": qid, "untouched_for_ms": -1}),
    ] {
        let refused = post_api(&facade, "/streams/v1/state/partitions", bad.clone()).await;
        assert_eq!(refused, Err("bad_request".to_string()), "{bad}");
    }

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// `POST /streams/v1/claim` (issue #153): an idle partition is leased with an
/// empty batch; a partition someone holds is `busy`, one with messages waiting
/// is `backlog`, an unknown one `missing`. A cycle that acks the claim with
/// `count: 0` closes the window under the lease, keeps the cursor and releases
/// the lease; the same cycle sent again, or one with another lease, is refused
/// whole and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn stream_claim_leases_idle_partitions_and_fences_cycles() {
    let dir = scratch("stream-claim");
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let push = |raw: &'static [u8]| {
        let facade = &facade;
        async move {
            facade
                .push(ctx(), PushReq { raw: raw.to_vec() })
                .await
                .expect("push");
        }
    };
    push(
        br#"{"items":[
        {"queue":"cl-src","partition":"a","payload":{"n":1}},
        {"queue":"cl-src","partition":"b","payload":{"n":2}},
        {"queue":"cl-src","partition":"c","payload":{"n":3}}
    ]}"#,
    )
    .await;

    let (status, registered) = post_api(
        &facade,
        "/streams/v1/queries",
        serde_json::json!({"name":"cl","source_queue":"cl-src","config_hash":"cl-1"}),
    )
    .await
    .expect("register");
    assert_eq!(status, 200, "{registered}");
    let qid = registered["query_id"]
        .as_str()
        .expect("query id")
        .to_string();

    // One pop per partition, as the stream's group (its default name).
    let mut popped = std::collections::BTreeMap::new();
    for _ in 0..3 {
        let out = facade
            .pop_wildcard(
                ctx(),
                PopReq {
                    queue: "cl-src".into(),
                    group: Some("streams.cl".into()),
                    batch: 1,
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
        let pop = parse(&out.body);
        let name = pop["messages"][0]["partition"]
            .as_str()
            .expect("partition")
            .to_string();
        let pid = pop["partitionId"]
            .as_str()
            .expect("partitionId")
            .to_string();
        let lease = pop["leaseId"].as_str().expect("leaseId").to_string();
        popped.insert(name, (pid, lease));
    }
    let pid = |name: &str| popped[name].0.clone();
    let lease = |name: &str| popped[name].1.clone();

    let cycle = |body: Value| {
        let facade = &facade;
        async move {
            let (status, out) = post_api(facade, "/streams/v1/cycle", body)
                .await
                .expect("cycle");
            assert_eq!(status, 200, "{out}");
            out
        }
    };
    let ack = |lease: String, count: i64| serde_json::json!({"transactionId": "", "leaseId": lease, "status": "completed", "count": count});
    // a: consumed, with an open window. c: consumed, then a new message
    // waits. b: still leased by its pop.
    let out = cycle(
        serde_json::json!({"query_id": qid, "partition_id": pid("a"),
        "consumer_group": "streams.cl", "release_lease": true, "ack": ack(lease("a"), 1),
        "state_ops": [{"type":"upsert","key":"w1","value":{"windowEnd": 1}}]}),
    )
    .await;
    assert_eq!(out["success"], true, "{out}");
    let out = cycle(
        serde_json::json!({"query_id": qid, "partition_id": pid("c"),
        "consumer_group": "streams.cl", "release_lease": true, "ack": ack(lease("c"), 1)}),
    )
    .await;
    assert_eq!(out["success"], true, "{out}");
    push(br#"{"items":[{"queue":"cl-src","partition":"c","payload":{"n":4}}]}"#).await;

    let claim = |pids: Vec<String>, lease_seconds: i64| {
        let facade = &facade;
        let qid = qid.clone();
        async move {
            let (status, out) = post_api(
                facade,
                "/streams/v1/claim",
                serde_json::json!({"query_id": qid, "partition_ids": pids,
                    "lease_seconds": lease_seconds}),
            )
            .await
            .expect("claim");
            assert_eq!(status, 200, "{out}");
            assert_eq!(out["success"], true, "{out}");
            out
        }
    };
    let statuses = |out: &Value| -> Vec<(String, String)> {
        out["partitions"]
            .as_array()
            .expect("partitions")
            .iter()
            .map(|p| {
                (
                    p["partition"].as_str().unwrap_or("").to_string(),
                    p["status"].as_str().expect("status").to_string(),
                )
            })
            .collect()
    };

    let out = claim(vec![pid("a"), pid("b"), pid("c"), "999999".into()], 30).await;
    assert_eq!(
        statuses(&out),
        vec![
            ("a".to_string(), "claimed".to_string()),
            ("b".to_string(), "busy".to_string()),
            ("c".to_string(), "backlog".to_string()),
            ("".to_string(), "missing".to_string()),
        ],
        "{out}"
    );
    let now_ms = out["now_ms"].as_i64().expect("now_ms");
    assert!(now_ms > 1_700_000_000_000, "{out}");
    let claimed = out["lease_id"].as_str().expect("lease_id").to_string();
    assert_eq!(
        out["partitions"][0]["lease_expires_at"].is_string(),
        true,
        "{out}"
    );

    // Claimed once is busy for everyone else, this caller included.
    let out = claim(vec![pid("a")], 30).await;
    assert_eq!(
        statuses(&out),
        vec![("a".to_string(), "busy".to_string())],
        "{out}"
    );

    let state_rows = |partition: String| {
        let facade = &facade;
        let qid = qid.clone();
        async move {
            let (status, out) = post_api(
                facade,
                "/streams/v1/state/get",
                serde_json::json!({"query_id": qid, "partition_id": partition}),
            )
            .await
            .expect("state get");
            assert_eq!(status, 200, "{out}");
            out["rows"].as_array().expect("rows").len()
        }
    };
    let close = |lease: String| {
        serde_json::json!({"query_id": qid, "partition_id": pid("a"),
            "consumer_group": "streams.cl", "release_lease": true, "ack": ack(lease, 0),
            "state_ops": [{"type":"delete","key":"w1"}],
            "push_items": [{"queue":"cl-sink","partition":"a","payload":{"closed":"w1"}}]})
    };

    // Another lease (b's, a pop's) cannot close a's window.
    let out = cycle(close(lease("b"))).await;
    assert_eq!(out["success"], false, "{out}");
    assert_eq!(out["reason"], "bad_lease", "{out}");
    assert_eq!(state_rows(pid("a")).await, 1);

    // The claim's lease closes it: state gone, result pushed, cursor kept.
    let out = cycle(close(claimed.clone())).await;
    assert_eq!(out["success"], true, "{out}");
    assert_eq!(out["ack_result"]["count"], 0, "{out}");
    assert_eq!(out["ack_result"]["lease_released"], true, "{out}");
    assert_eq!(state_rows(pid("a")).await, 0);

    // The same close again (an SDK retry after a lost answer) is refused:
    // the first one released the lease. Nothing is pushed twice.
    let out = cycle(close(claimed.clone())).await;
    assert_eq!(out["success"], false, "{out}");
    assert_eq!(out["reason"], "bad_lease", "{out}");
    let sink = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "cl-sink".into(),
                group: Some("check".into()),
                batch: 10,
                auto_ack: true,
                wait: false,
                timeout_ms: 1_000,
                options: crate::rsm::facade::PopOptions {
                    subscription_mode: "all".into(),
                    ..Default::default()
                },
            },
        )
        .await
        .expect("sink pop");
    let sink = parse(&sink.body);
    assert_eq!(sink["messages"].as_array().map(Vec::len), Some(1), "{sink}");

    // Released: claimable again. The cursor did not move, so a's next message
    // is backlog for the group, not skipped.
    let out = claim(vec![pid("a")], 1).await;
    assert_eq!(
        statuses(&out),
        vec![("a".to_string(), "claimed".to_string())],
        "{out}"
    );
    // A lease that ran out holds nothing.
    tokio::time::sleep(std::time::Duration::from_millis(1_300)).await;
    let out = claim(vec![pid("a")], 30).await;
    assert_eq!(
        statuses(&out),
        vec![("a".to_string(), "claimed".to_string())],
        "{out}"
    );
    let expired = cycle(close(claimed.clone())).await;
    assert_eq!(expired["success"], false, "{expired}");
    let renewed = out["lease_id"].as_str().expect("lease_id").to_string();
    let out = cycle(
        serde_json::json!({"query_id": qid, "partition_id": pid("a"),
        "consumer_group": "streams.cl", "release_lease": true, "ack": ack(renewed, 0)}),
    )
    .await;
    assert_eq!(out["success"], true, "{out}");
    push(br#"{"items":[{"queue":"cl-src","partition":"a","payload":{"n":5}}]}"#).await;
    let out = claim(vec![pid("a")], 30).await;
    assert_eq!(
        statuses(&out),
        vec![("a".to_string(), "backlog".to_string())],
        "{out}"
    );

    // A partition the group never read holds no lease: a cycle that names one
    // there is refused instead of committing its state with no lease (it used
    // to skip the ack and commit the rest).
    push(br#"{"items":[{"queue":"cl-src","partition":"d","payload":{"n":6}}]}"#).await;
    // d's id, from a separate group's pops (one partition per pop).
    let mut d_pid = None;
    for _ in 0..8 {
        let pop = facade
            .pop_wildcard(
                ctx(),
                PopReq {
                    queue: "cl-src".into(),
                    group: Some("finder".into()),
                    batch: 10,
                    auto_ack: true,
                    wait: false,
                    timeout_ms: 1_000,
                    options: crate::rsm::facade::PopOptions {
                        subscription_mode: "all".into(),
                        ..Default::default()
                    },
                },
            )
            .await
            .expect("finder pop");
        let pop = parse(&pop.body);
        if pop["messages"][0]["partition"] == "d" {
            d_pid = pop["partitionId"].as_str().map(str::to_string);
            break;
        }
    }
    let d_pid = d_pid.expect("d's id");
    let out = cycle(serde_json::json!({"query_id": qid, "partition_id": d_pid,
        "consumer_group": "streams.cl", "release_lease": true, "ack": ack("not-a-lease".into(), 1),
        "state_ops": [{"type":"upsert","key":"w9","value":{"n":1}}]}))
    .await;
    assert_eq!(out["success"], false, "{out}");
    assert_eq!(out["reason"], "bad_lease", "{out}");
    assert_eq!(state_rows(d_pid).await, 0);

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
