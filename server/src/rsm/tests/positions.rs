//! Consumer-group POSITIONS: the transaction's `positions` rider, the read
//! route, and the cursor row that carries them — end to end through the real
//! [`RaftFacade`] (receiver, batcher, planner, apply, reads), over a
//! throwaway data directory per test, like `facade.rs`.
//!
//! What they prove:
//! - the codec: a cursor row with metadata is catalogue version 2 and row
//!   version 2, one without keeps its version-1 bytes, both round-trip;
//! - a position set is read back (offset and metadata), on a partition that
//!   had no data too; a forgotten one reads back as none;
//! - it is all-or-nothing with the bundle: a lost `required` fence writes no
//!   position, a missing queue refuses the whole bundle;
//! - a position IS the group's cursor: a native consumer of the same group
//!   resumes there, and what it acks is what the position reads back;
//! - positions survive a restart, metadata included;
//! - the group delete route decodes an encoded group name and forgets every
//!   position of the group.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{json, Value};

use crate::rsm::effect::{
    decode_effect, encode_effect, kinds_version_of, CodecError, CursorRow, Effect, Kind, VERSION_1,
    VERSION_2,
};
use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{
    AckReq, ApiReq, Deadline, PopReq, PushReq, ReqCtx, Rsm, RsmBuildCtx, TxnReq,
};
use crate::rsm::store::rows::{cursor_decode, cursor_encode, cursor_fresh, ROW_V1, ROW_V2};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-positions-{tag}-{}-{}",
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

async fn txn(f: &RaftFacade, body: Value) -> Value {
    let out = f
        .transaction(
            ctx(),
            TxnReq {
                raw: body.to_string().into_bytes(),
            },
        )
        .await
        .expect("transaction");
    parse(&out.body)
}

async fn api(f: &RaftFacade, method: &str, path: &str, query: Option<&str>, body: Value) -> Value {
    let out = f
        .api(
            ctx(),
            ApiReq {
                method: method.to_string(),
                path: path.to_string(),
                query: query.map(str::to_string),
                body: if body.is_null() {
                    Vec::new()
                } else {
                    body.to_string().into_bytes()
                },
            },
        )
        .await
        .expect("api call");
    assert_eq!(out.status, 200, "{}", out.body);
    parse(&out.body)
}

/// The positions of `group` on the asked `(queue, partition)`s, or all of them.
async fn positions(f: &RaftFacade, group: &str, entries: Option<&[(&str, &str)]>) -> Vec<Value> {
    let body = match entries {
        Some(e) => json!({
            "consumerGroup": group,
            "entries": e.iter().map(|(q, p)| json!({"queue": q, "partition": p})).collect::<Vec<_>>(),
        }),
        None => json!({ "consumerGroup": group }),
    };
    let v = api(f, "POST", "/api/v1/consumer-groups/positions", None, body).await;
    v["entries"].as_array().expect("entries").clone()
}

fn set(queue: &str, partition: &str, group: &str, offset: Option<u64>, metadata: &str) -> Value {
    json!({
        "queue": queue,
        "partition": partition,
        "consumerGroup": group,
        "offset": offset,
        "metadata": metadata,
    })
}

// ---------------------------------------------------------------------------
// The codec
// ---------------------------------------------------------------------------

fn row(metadata: &str) -> CursorRow {
    let mut r = cursor_fresh(41, 1_767_000_000_000_000);
    r.total_consumed = 7;
    r.metadata = metadata.to_string();
    r
}

#[test]
fn a_cursor_row_with_metadata_is_the_second_catalogue_version() {
    let plain = Effect::CursorSet {
        pid: 3,
        group: "g".into(),
        row: row(""),
    };
    let tagged = Effect::CursorSet {
        pid: 3,
        group: "g".into(),
        row: row("batch-7"),
    };
    assert_eq!(
        plain.version(),
        VERSION_1,
        "no metadata: the version-1 shape"
    );
    assert_eq!(tagged.version(), VERSION_2);
    assert_eq!(kinds_version_of(std::slice::from_ref(&plain)), 1);
    assert_eq!(kinds_version_of(&[plain.clone(), tagged.clone()]), 2);

    // Version 1 bytes are exactly the old body: the metadata adds nothing.
    let a = encode_effect(&plain);
    let b = encode_effect(&tagged);
    assert_eq!(
        b.len(),
        a.len() + 4 + "batch-7".len(),
        "version 2 is version 1 plus the metadata, last"
    );
    for e in [&plain, &tagged] {
        let bytes = encode_effect(e);
        let (back, used) = decode_effect(&bytes).expect("decode");
        assert_eq!(&back, e);
        assert_eq!(used, bytes.len());
    }
}

#[test]
fn version_2_exists_for_the_cursor_row_only() {
    let body = Effect::CursorDelete {
        pid: 1,
        group: "g".into(),
    }
    .encode_body();
    assert_eq!(
        Effect::decode_body(Kind::CursorDelete, VERSION_2, &body),
        Err(CodecError::UnknownVersion {
            kind: Kind::CursorDelete as u16,
            version: VERSION_2
        })
    );
}

#[test]
fn a_stored_cursor_row_keeps_its_bytes_until_it_carries_metadata() {
    let plain = cursor_encode(&row(""));
    assert_eq!(plain[0], ROW_V1);
    assert_eq!(cursor_decode(&plain).unwrap(), row(""));
    let tagged = cursor_encode(&row("m"));
    assert_eq!(tagged[0], ROW_V2);
    assert_eq!(cursor_decode(&tagged).unwrap(), row("m"));
    assert_eq!(
        &tagged[1..plain.len()],
        &plain[1..],
        "the same body, then the metadata"
    );
}

// ---------------------------------------------------------------------------
// Through the facade
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_position_is_read_back_and_forgotten() {
    let dir = scratch("roundtrip");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    push(&f, "orders", "0", 5).await;

    // Partition 1 has no data: the position creates it, as a push would.
    let out = txn(
        &f,
        json!({"positions": [
            set("orders", "0", "billing", Some(3), "batch-3"),
            set("orders", "1", "billing", Some(0), ""),
        ]}),
    )
    .await;
    assert_eq!(out["success"], true, "{out}");
    let results = out["results"].as_array().expect("results");
    assert_eq!(results.len(), 2, "{out}");
    assert!(results
        .iter()
        .all(|r| r["type"] == "position" && r["success"] == true));

    let read = positions(
        &f,
        "billing",
        Some(&[("orders", "0"), ("orders", "1"), ("orders", "2")]),
    )
    .await;
    assert_eq!(read[0]["offset"], 3, "{read:?}");
    assert_eq!(read[0]["metadata"], "batch-3");
    assert_eq!(
        read[1]["offset"], 0,
        "a position on an empty partition: {read:?}"
    );
    assert_eq!(read[2]["offset"], Value::Null, "no partition, no position");
    // Every position of the group, without naming them.
    let all = positions(&f, "billing", None).await;
    assert_eq!(all.len(), 2, "{all:?}");
    // Another group of the same queue holds nothing.
    assert_eq!(
        positions(&f, "other", Some(&[("orders", "0")])).await[0]["offset"],
        Value::Null
    );

    // Moving it keeps the latest; forgetting it removes it.
    let out = txn(
        &f,
        json!({"positions": [set("orders", "0", "billing", Some(5), "")]}),
    )
    .await;
    assert_eq!(out["success"], true, "{out}");
    assert_eq!(
        positions(&f, "billing", Some(&[("orders", "0")])).await[0]["offset"],
        5
    );
    let out = txn(
        &f,
        json!({"positions": [set("orders", "0", "billing", None, "")]}),
    )
    .await;
    assert_eq!(out["success"], true, "{out}");
    let read = positions(&f, "billing", Some(&[("orders", "0")])).await;
    assert_eq!(read[0]["offset"], Value::Null, "{read:?}");

    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// The applied index of the one node, off the shared-state route.
async fn applied(f: &RaftFacade) -> u64 {
    let v = api(f, "GET", "/api/v1/system/shared-state", None, Value::Null).await;
    v["appliedIndex"].as_u64().expect("appliedIndex")
}

/// Setting a position to where it already is writes nothing: no effect, and
/// with nothing else in the bundle no entry at all. A Kafka client re-sends
/// every partition whose previous commit is still in flight, so this is most of
/// what a wide group's commits carry.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_position_set_to_where_it_is_writes_nothing() {
    let dir = scratch("noop");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    push(&f, "orders", "0", 3).await;
    let body = json!({"positions": [set("orders", "0", "billing", Some(2), "m")]});
    let out = txn(&f, body.clone()).await;
    assert_eq!(out["success"], true, "{out}");
    // Background loops may log an entry of their own now and then, so the
    // claim is that SOME repeat logs nothing — a set that were written would
    // advance the index every time.
    let mut quiet = false;
    for _ in 0..5 {
        let before = applied(&f).await;
        let out = txn(&f, body.clone()).await;
        assert_eq!(out["success"], true, "{out}");
        if applied(&f).await == before {
            quiet = true;
            break;
        }
    }
    assert!(quiet, "an unchanged position was written again");
    // A different metadata, or a different offset, is a change.
    let before = applied(&f).await;
    let out = txn(
        &f,
        json!({"positions": [set("orders", "0", "billing", Some(2), "n")]}),
    )
    .await;
    assert_eq!(out["success"], true, "{out}");
    assert!(
        applied(&f).await > before,
        "a changed position was not written"
    );
    assert_eq!(
        positions(&f, "billing", Some(&[("orders", "0")])).await[0]["metadata"],
        "n"
    );
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_position_is_all_or_nothing_with_its_bundle() {
    let dir = scratch("atomic");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    push(&f, "orders", "0", 3).await;
    let fence = |expect: u64| {
        json!({"op": "put", "ns": "queen-kafka", "key": "qk:fence:billing",
               "value": {"node": 1}, "forever": true, "expect": expect, "required": true})
    };

    // The fence's first write is a `putIfAbsent` (expect 0), and it lands with
    // the position.
    let out = txn(
        &f,
        json!({"kv": [fence(0)], "positions": [set("orders", "0", "billing", Some(1), "")]}),
    )
    .await;
    assert_eq!(out["success"], true, "{out}");
    let version = out["results"][0]["version"]
        .as_u64()
        .expect("fence version");

    // A stale fence: the bundle rolls back and the position does not move.
    let out = txn(
        &f,
        json!({"kv": [fence(version + 1_000)], "positions": [set("orders", "0", "billing", Some(2), "")]}),
    )
    .await;
    assert_eq!(out["success"], false, "{out}");
    assert_eq!(out["reason"], "kv_precondition", "{out}");
    assert_eq!(
        positions(&f, "billing", Some(&[("orders", "0")])).await[0]["offset"],
        1
    );

    // A queue that does not exist refuses the whole bundle, fence included.
    let out = txn(
        &f,
        json!({"kv": [fence(version)], "positions": [
            set("orders", "0", "billing", Some(2), ""),
            set("nope", "0", "billing", Some(2), ""),
        ]}),
    )
    .await;
    assert_eq!(out["success"], false, "{out}");
    assert_eq!(out["reason"], "queue_not_found", "{out}");
    assert_eq!(
        positions(&f, "billing", Some(&[("orders", "0")])).await[0]["offset"],
        1
    );

    // Shapes the receiver refuses before planning.
    for bad in [
        json!({"positions": [{"queue": "orders", "consumerGroup": "g"}]}),
        json!({"positions": [{"queue": "orders", "consumerGroup": "g", "offset": -1}]}),
        json!({"positions": [{"queue": "orders", "offset": 1}]}),
    ] {
        let out = txn(&f, bad.clone()).await;
        assert_eq!(out["success"], false, "{bad} -> {out}");
    }
    let out = txn(
        &f,
        json!({"positions": [set("orders", "0", "g", Some(1), &"m".repeat(4097))]}),
    )
    .await;
    assert_eq!(out["reason"], "too_large", "{out}");

    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_position_is_the_groups_cursor_for_a_native_consumer_too() {
    let dir = scratch("shared");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    push(&f, "orders", "0", 5).await;

    // Set by one client...
    let out = txn(
        &f,
        json!({"positions": [set("orders", "0", "shared", Some(3), "")]}),
    )
    .await;
    assert_eq!(out["success"], true, "{out}");

    // ...and a native consumer of the same group resumes exactly there.
    let popped = f
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "orders".into(),
                group: Some("shared".into()),
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
    let msgs = pop["messages"].as_array().expect("messages");
    let offsets: Vec<u64> = msgs.iter().filter_map(|m| m["offset"].as_u64()).collect();
    assert_eq!(offsets, [3, 4], "{}", popped.body);

    // What it acks is what the position reads back.
    let pid = pop["partitionId"]
        .as_str()
        .expect("partitionId")
        .to_string();
    let lease = pop["leaseId"].as_str().expect("leaseId").to_string();
    let acks: Vec<Value> = msgs
        .iter()
        .map(|m| {
            json!({"transactionId": m["transactionId"], "partitionId": pid,
                   "status": "completed", "leaseId": lease})
        })
        .collect();
    let acked = f
        .ack(
            ctx(),
            AckReq {
                queue: Some("orders".into()),
                group: "shared".into(),
                raw: json!({"consumerGroup": "shared", "acknowledgments": acks})
                    .to_string()
                    .into_bytes(),
            },
        )
        .await
        .expect("ack");
    for r in parse(&acked.body).as_array().expect("ack array") {
        assert_eq!(r["success"], true, "{r}");
    }
    let read = positions(&f, "shared", Some(&[("orders", "0")])).await;
    assert_eq!(read[0]["offset"], 5, "{read:?}");

    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn positions_survive_a_restart_metadata_included() {
    let dir = scratch("restart");
    {
        let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
        push(&f, "orders", "0", 2).await;
        let out = txn(
            &f,
            json!({"positions": [
                set("orders", "0", "billing", Some(2), "deploy-42"),
                set("orders", "1", "billing", Some(0), ""),
            ]}),
        )
        .await;
        assert_eq!(out["success"], true, "{out}");
        f.shutdown().await;
    }
    let f = RaftFacade::open(&build_ctx(&dir)).expect("reopen facade");
    let read = positions(&f, "billing", Some(&[("orders", "0"), ("orders", "1")])).await;
    assert_eq!(read[0]["offset"], 2, "{read:?}");
    assert_eq!(read[0]["metadata"], "deploy-42");
    assert_eq!(read[1]["offset"], 0);
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn the_group_delete_route_decodes_the_name_and_forgets_every_position() {
    let dir = scratch("delete");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    push(&f, "orders", "0", 2).await;
    push(&f, "clicks", "0", 2).await;
    let group = "team a/billing";
    let out = txn(
        &f,
        json!({"positions": [
            set("orders", "0", group, Some(1), ""),
            set("orders", "1", group, Some(0), ""),
            set("clicks", "0", group, Some(2), ""),
            set("orders", "0", "bystander", Some(2), ""),
        ]}),
    )
    .await;
    assert_eq!(out["success"], true, "{out}");
    assert_eq!(positions(&f, group, None).await.len(), 3);

    let v = api(
        &f,
        "DELETE",
        "/api/v1/consumer-groups/team%20a%2Fbilling",
        Some("deleteMetadata=true"),
        Value::Null,
    )
    .await;
    assert_eq!(v["consumerGroup"], group, "{v}");
    assert_eq!(v["deletedPartitions"], 3, "{v}");
    assert!(positions(&f, group, None).await.is_empty());
    assert_eq!(
        positions(&f, "bystander", Some(&[("orders", "0")])).await[0]["offset"],
        2,
        "another group is untouched"
    );

    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
