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
