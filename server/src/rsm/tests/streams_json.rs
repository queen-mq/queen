//! A Streams cycle stores what it was sent, `state/get` and `fetch` answer
//! what is stored, and a state value above `QUEEN_STREAMS_MAX_STATE_BYTES` is
//! refused before anything is planned (2026-10-29: windows of 20 to 65 MB,
//! read through JSON trees, ran the leader out of memory).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{json, Value};

use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{ApiOut, ApiReq, Deadline, PopReq, PushReq, ReqCtx, Rsm, RsmBuildCtx};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-streams-json-{tag}-{}-{}",
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

/// A POST with exactly these body bytes: spacing and key order are what the
/// test is about.
async fn post(f: &RaftFacade, path: &str, body: &[u8]) -> ApiOut {
    f.api(
        ctx(),
        ApiReq {
            method: "POST".into(),
            path: path.into(),
            query: None,
            body: body.to_vec(),
        },
    )
    .await
    .unwrap_or_else(|e| panic!("{path}: {e:?}"))
}

/// One partition read from offset 0 once it holds `want` records: a cycle's
/// push becomes readable with the store commit that follows its answer.
async fn fetch_records(f: &RaftFacade, queue: &str, partition: &str, want: usize) -> String {
    let body =
        format!(r#"{{"entries":[{{"queue":"{queue}","partition":"{partition}","offset":0}}]}}"#);
    for _ in 0..400 {
        let out = post(f, "/api/v1/fetch", body.as_bytes()).await;
        assert_eq!(out.status, 200, "{}", out.body);
        let n = parse(&out.body)["entries"][0]["records"]
            .as_array()
            .map_or(0, Vec::len);
        if n >= want {
            return out.body;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("{queue}/{partition} never held {want} records");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_cycle_stores_what_it_was_sent_and_refuses_an_oversized_state() {
    let dir = scratch("cycle");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    f.push(
        ctx(),
        PushReq {
            raw: br#"{"items":[{"queue":"src","partition":"t1","payload":{"n":1},"transactionId":"e1"}]}"#
                .to_vec(),
        },
    )
    .await
    .expect("push");
    let popped = f
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "src".into(),
                group: Some("g".into()),
                batch: 10,
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
    let pid = pop["partitionId"]
        .as_str()
        .expect("partition id")
        .to_string();
    let lease = pop["leaseId"].as_str().expect("lease id").to_string();
    let registered = post(
        &f,
        "/streams/v1/queries",
        br#"{"name":"windows","source_queue":"src","sink_queue":"sink","config_hash":"h"}"#,
    )
    .await;
    assert_eq!(registered.status, 200, "{}", registered.body);
    let qid = parse(&registered.body)["query_id"]
        .as_str()
        .expect("query id")
        .to_string();
    let state_get = format!(r#"{{"query_id":"{qid}","partition_id":"{pid}"}}"#);

    // A state and a sink payload with their keys out of order and a space in
    // the state: stored, and answered, as sent.
    let cycle = format!(
        r#"{{"query_id":"{qid}","partition_id":"{pid}","consumer_group":"g",
            "state_ops":[{{"type":"upsert","key":"sess","value":{{"z":1, "a":[1,2]}}}}],
            "push_items":[{{"queue":"sink","partition":"t1","payload":{{"y":2,"b":1}},"transactionId":"out-1"}}],
            "ack":{{"leaseId":"{lease}","status":"completed","count":1}},"release_lease":true}}"#
    );
    let out = post(&f, "/streams/v1/cycle", cycle.as_bytes()).await;
    assert_eq!(out.status, 200, "{}", out.body);
    let done = parse(&out.body);
    assert_eq!(done["success"], true, "{done}");
    assert_eq!(done["state_ops_applied"], 1, "{done}");
    assert_eq!(done["ack_result"]["count"], 1, "{done}");

    let state = post(&f, "/streams/v1/state/get", state_get.as_bytes()).await;
    assert_eq!(state.status, 200, "{}", state.body);
    assert!(
        state.body.contains(r#""value":{"z":1, "a":[1,2]}"#),
        "{}",
        state.body
    );
    assert_eq!(parse(&state.body)["rows"][0]["key"], "sess");

    let fetched = fetch_records(&f, "sink", "t1", 1).await;
    assert!(fetched.contains(r#""payload":{"y":2,"b":1}"#), "{fetched}");

    // Over the ceiling: refused whole. Neither the state nor the push lands.
    let big = format!(
        r#"{{"s":"{}"}}"#,
        "x".repeat(crate::rsm::facade::streams_max_state_bytes())
    );
    let cycle = format!(
        r#"{{"query_id":"{qid}","partition_id":"{pid}","consumer_group":"g",
            "state_ops":[{{"type":"upsert","key":"sess","value":{big}}}],
            "push_items":[{{"queue":"sink","partition":"t1","payload":{{"n":2}},"transactionId":"out-2"}}]}}"#
    );
    let out = post(&f, "/streams/v1/cycle", cycle.as_bytes()).await;
    assert_eq!(out.status, 413, "{}", out.body);
    let refused = parse(&out.body);
    assert_eq!(refused["success"], false, "{refused}");
    assert_eq!(refused["reason"], "state_too_large", "{refused}");
    let state = post(&f, "/streams/v1/state/get", state_get.as_bytes()).await;
    assert!(
        state.body.contains(r#""value":{"z":1, "a":[1,2]}"#),
        "{}",
        state.body
    );

    // A delete carries no value and is never refused for its size; the next
    // push lands right after `out-1`, so `out-2` never did.
    let cycle = format!(
        r#"{{"query_id":"{qid}","partition_id":"{pid}","state_ops":[{{"type":"delete","key":"sess"}}],
            "push_items":[{{"queue":"sink","partition":"t1","payload":{{"n":3}},"transactionId":"out-3"}}]}}"#
    );
    let out = post(&f, "/streams/v1/cycle", cycle.as_bytes()).await;
    assert_eq!(out.status, 200, "{}", out.body);
    let state = post(&f, "/streams/v1/state/get", state_get.as_bytes()).await;
    assert_eq!(parse(&state.body)["rows"], json!([]), "{}", state.body);
    let fetched = parse(&fetch_records(&f, "sink", "t1", 2).await);
    let sent: Vec<&str> = fetched["entries"][0]["records"]
        .as_array()
        .expect("records")
        .iter()
        .map(|r| r["transactionId"].as_str().unwrap_or(""))
        .collect();
    assert_eq!(sent, ["out-1", "out-3"]);

    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
