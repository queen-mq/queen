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
use crate::rsm::facade::{AckReq, Deadline, PopReq, PushReq, RenewReq, ReqCtx, Rsm, RsmBuildCtx};

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
    ReqCtx::new("default", Deadline::after(Duration::from_secs(5)))
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
/// ack wire. The WP's ack render passed `snapshot: None` for every item, so the
/// row was filed with a BLANK txn (and message id / payload) — the refutation's
/// content-less dead letter. The payload and message id still wait on the
/// offset→frame pre-read (a later WP), but the txn is on the wire verbatim and
/// must reach the row so the dead letter is identifiable.
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

    facade.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
