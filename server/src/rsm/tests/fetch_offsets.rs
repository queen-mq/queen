//! `POST /api/v1/fetch/offsets` end to end through the real [`RaftFacade`]
//! (receiver, batcher, planner, apply, reads), over a throwaway data directory
//! per test, like `positions.rs`.
//!
//! What they prove: a time finds the first offset appended at or after it, on
//! the clock the fetch route reports (`ts`); a time past the newest append
//! finds nothing and still reports the watermarks to start from; a partition
//! never written is an empty log and a queue that does not exist is the one
//! refusal; and the request is refused whole on a timestamp that is not one.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{json, Value};

use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{ApiReq, Deadline, PushReq, ReqCtx, Rsm, RsmBuildCtx};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-fetch-offsets-{tag}-{}-{}",
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
    let v: Value = serde_json::from_str(&out.body).expect("push body");
    for it in v.as_array().expect("push array") {
        assert_eq!(it["status"], "queued", "{it}");
    }
}

async fn api(f: &RaftFacade, path: &str, body: Value) -> (u16, Value) {
    let out = f
        .api(
            ctx(),
            ApiReq {
                method: "POST".to_string(),
                path: path.to_string(),
                query: None,
                body: body.to_string().into_bytes(),
            },
        )
        .await
        .expect("api call");
    let v = serde_json::from_str(&out.body)
        .unwrap_or_else(|e| panic!("bad JSON body: {e}\n{}", out.body));
    (out.status, v)
}

/// `(offset, epoch ms)` of every record of `queue`/`partition`, as the fetch
/// route reports them.
async fn records(f: &RaftFacade, queue: &str, partition: &str) -> Vec<(u64, i64)> {
    let (status, v) = api(
        f,
        "/api/v1/fetch",
        json!({"entries": [{"queue": queue, "partition": partition, "offset": 0}]}),
    )
    .await;
    assert_eq!(status, 200, "{v}");
    v["entries"][0]["records"]
        .as_array()
        .expect("records")
        .iter()
        .map(|r| {
            (
                r["offset"].as_u64().expect("offset"),
                crate::util::parse_iso_ms(r["ts"].as_str().expect("ts")).expect("iso"),
            )
        })
        .collect()
}

async fn lookup(f: &RaftFacade, entries: Value) -> Vec<Value> {
    let (status, v) = api(f, "/api/v1/fetch/offsets", json!({ "entries": entries })).await;
    assert_eq!(status, 200, "{v}");
    v["entries"].as_array().expect("entries").clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_time_finds_the_first_offset_appended_at_or_after_it() {
    let dir = scratch("basic");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    // Three appends, a few milliseconds apart so each carries its own stamp.
    for n in [3, 2, 4] {
        push(&f, "orders", "0", n).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let recs = records(&f, "orders", "0").await;
    assert_eq!(recs.len(), 9);
    let (first, second, third) = (recs[0].1, recs[3].1, recs[5].1);
    assert!(first < second && second < third, "{recs:?}");

    let got = lookup(
        &f,
        json!([
            {"queue": "orders", "partition": "0", "timestamp": 0},
            {"queue": "orders", "partition": "0", "timestamp": first},
            {"queue": "orders", "partition": "0", "timestamp": first + 1},
            {"queue": "orders", "partition": "0", "timestamp": second},
            // The ISO spelling every other route takes, the same instant.
            {"queue": "orders", "partition": "0",
             "timestamp": crate::rsm::planner::timers::iso_us(third * 1_000)},
            {"queue": "orders", "partition": "0", "timestamp": third + 1_000},
        ]),
    )
    .await;
    let offsets: Vec<Value> = got.iter().map(|e| e["offset"].clone()).collect();
    assert_eq!(
        offsets,
        [
            json!(0),
            json!(0),
            json!(3),
            json!(3),
            json!(5),
            Value::Null
        ],
        "{got:?}"
    );
    // The time of the append it found, on the fetch route's clock.
    assert_eq!(
        crate::util::parse_iso_ms(got[2]["ts"].as_str().unwrap()),
        Some(second)
    );
    // A time past the newest append finds nothing and says where to start.
    assert_eq!(got[5]["ts"], Value::Null);
    assert_eq!(got[5]["highWatermark"], json!(9));
    assert_eq!(got[5]["logStartOffset"], json!(0));
    assert!(got.iter().all(|e| e.get("error").is_none()), "{got:?}");
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn an_unwritten_partition_is_empty_and_a_missing_queue_is_unknown() {
    let dir = scratch("missing");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    push(&f, "orders", "0", 1).await;
    let got = lookup(
        &f,
        json!([
            {"queue": "orders", "partition": "7", "timestamp": 0},
            {"queue": "nope", "partition": "0", "timestamp": 0},
            // A partition named by number is the decimal name, as in a fetch.
            {"queue": "orders", "partition": 0, "timestamp": 0},
        ]),
    )
    .await;
    assert_eq!(got[0]["offset"], Value::Null);
    assert_eq!(got[0]["highWatermark"], json!(0));
    assert!(got[0].get("error").is_none(), "{got:?}");
    assert_eq!(got[1]["error"], json!("UNKNOWN_TOPIC_OR_PARTITION"));
    assert_eq!(got[2]["partition"], json!("0"));
    assert_eq!(got[2]["offset"], json!(0));
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_timestamp_that_is_not_one_refuses_the_request() {
    let dir = scratch("bad");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    for bad in [json!(-1), json!("later"), Value::Null] {
        let (status, v) = api(
            &f,
            "/api/v1/fetch/offsets",
            json!({"entries": [{"queue": "orders", "partition": "0", "timestamp": bad}]}),
        )
        .await;
        assert_eq!(status, 400, "{bad}: {v}");
    }
    let many: Vec<Value> = (0..1_025)
        .map(|_| json!({"queue": "orders", "partition": "0", "timestamp": 0}))
        .collect();
    let (status, _) = api(&f, "/api/v1/fetch/offsets", json!({ "entries": many })).await;
    assert_eq!(status, 400);
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
