//! Routing without copies (PLAN_ROUTING.md), end to end on the real
//! [`RaftFacade`]: headers, reading queues, filtered groups, `link`, the
//! catch-up, and the catalog rules around them. Each test opens its own
//! single-node facade over a throwaway directory and waits for the cluster
//! version to reach 8 (the batcher raises it by itself once every member reads
//! it, here at once).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{json, Value};

use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{
    AckReq, ApiReq, Deadline, PopOptions, PopReq, PushReq, ReqCtx, Rsm, RsmBuildCtx, TxnReq,
};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-routing-{tag}-{}-{}",
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
        disk_high_pct: 99.0,
        disk_low_pct: 98.0,
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

/// A facade whose cluster reads catalogue version 8.
async fn open(tag: &str) -> (RaftFacade, PathBuf) {
    let dir = scratch(tag);
    let facade = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    for _ in 0..400 {
        if facade.cluster_allows(crate::rsm::effect::VERSION_8) {
            return (facade, dir);
        }
        // Any write runs a cycle, and the cycle raises the version.
        let _ = facade
            .push(
                ctx(),
                PushReq {
                    raw: br#"{"items":[{"queue":"warmup","payload":{}}]}"#.to_vec(),
                },
            )
            .await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the cluster version never reached 8");
}

async fn api(
    facade: &RaftFacade,
    method: &str,
    path: &str,
    query: Option<&str>,
    body: Value,
) -> (u16, Value) {
    let out = facade
        .api(
            ctx(),
            ApiReq {
                method: method.into(),
                path: path.into(),
                query: query.map(str::to_string),
                body: if body.is_null() {
                    Vec::new()
                } else {
                    serde_json::to_vec(&body).unwrap()
                },
            },
        )
        .await;
    match out {
        Ok(o) => (
            o.status,
            serde_json::from_str(&o.body).unwrap_or(Value::String(o.body)),
        ),
        Err(e) => (400, json!({"error": e.to_string()})),
    }
}

async fn configure(facade: &RaftFacade, body: Value) -> (u16, Value) {
    api(facade, "POST", "/api/v1/configure", None, body).await
}

async fn push(facade: &RaftFacade, items: Value) -> Value {
    let out = facade
        .push(
            ctx(),
            PushReq {
                raw: json!({ "items": items }).to_string().into_bytes(),
            },
        )
        .await
        .expect("push");
    parse(&out.body)
}

async fn try_push(facade: &RaftFacade, items: Value) -> Result<Value, String> {
    facade
        .push(
            ctx(),
            PushReq {
                raw: json!({ "items": items }).to_string().into_bytes(),
            },
        )
        .await
        .map(|o| parse(&o.body))
        .map_err(|e| e.to_string())
}

async fn pop(
    facade: &RaftFacade,
    queue: &str,
    group: Option<&str>,
    options: PopOptions,
) -> Result<Value, String> {
    let out = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: queue.into(),
                group: group.map(str::to_string),
                batch: 100,
                auto_ack: false,
                wait: false,
                timeout_ms: 1000,
                options,
            },
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(if out.empty {
        json!({"messages": []})
    } else {
        parse(&out.body)
    })
}

fn group_options(where_filter: Option<&str>, mode: &str) -> PopOptions {
    PopOptions {
        max_parts: 64,
        subscription_mode: mode.into(),
        where_filter: where_filter.map(str::to_string),
        ..Default::default()
    }
}

fn messages(pop: &Value) -> Vec<Value> {
    pop["messages"].as_array().cloned().unwrap_or_default()
}

/// Every message a queue-mode reader holds, popped (and acked) until empty.
async fn drain(facade: &RaftFacade, queue: &str) -> Vec<Value> {
    let mut out = Vec::new();
    for _ in 0..20 {
        let p = pop(
            facade,
            queue,
            None,
            PopOptions {
                max_parts: 64,
                ..Default::default()
            },
        )
        .await
        .expect("pop");
        let m = messages(&p);
        if m.is_empty() {
            break;
        }
        ack_all(facade, None, &m).await;
        out.extend(m);
    }
    out
}

async fn ack_all(facade: &RaftFacade, group: Option<&str>, msgs: &[Value]) {
    if msgs.is_empty() {
        return;
    }
    let items: Vec<Value> = msgs
        .iter()
        .map(|m| {
            json!({
                "transactionId": m["transactionId"],
                "partitionId": m["partitionId"],
                "leaseId": m["leaseId"],
                "status": "completed",
            })
        })
        .collect();
    let group = group.unwrap_or("__QUEUE_MODE__").to_string();
    facade
        .ack(
            ctx(),
            AckReq {
                queue: None,
                group: group.clone(),
                raw: json!({ "acknowledgments": items, "consumerGroup": group })
                    .to_string()
                    .into_bytes(),
            },
        )
        .await
        .expect("ack");
}

fn data_of(msgs: &[Value], key: &str) -> Vec<Value> {
    msgs.iter().map(|m| m["data"][key].clone()).collect()
}

// ---------------------------------------------------------------- headers

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn headers_come_back_on_pop_fetch_and_the_feed() {
    let (facade, _dir) = open("headers").await;
    let out = push(
        &facade,
        json!([
            {"queue":"orders","partition":"o-1","transactionId":"t1","payload":{"n":1},
             "headers":{"type":"order.paid","traceparent":"00-abc-def-01"}},
            {"queue":"orders","partition":"o-1","transactionId":"t2","payload":{"n":2}}
        ]),
    )
    .await;
    assert_eq!(out[0]["status"], "queued", "{out}");
    let p = pop(
        &facade,
        "orders",
        None,
        PopOptions {
            max_parts: 8,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let m = messages(&p);
    assert_eq!(m.len(), 2, "{p}");
    assert_eq!(
        m[0]["headers"],
        json!({"traceparent":"00-abc-def-01","type":"order.paid"})
    );
    assert!(
        m[1].get("headers").is_none(),
        "no headers, no key: {}",
        m[1]
    );
    // The fetch carries them too.
    let (status, fetched) = api(
        &facade,
        "POST",
        "/api/v1/fetch",
        None,
        json!({"entries":[{"queue":"orders","partition":"o-1","offset":0,"maxBytes":1048576}]}),
    )
    .await;
    assert_eq!(status, 200, "{fetched}");
    assert_eq!(
        fetched["entries"][0]["records"][0]["headers"]["type"], "order.paid",
        "{fetched}"
    );
    // The feed carries them, and shows a reading queue's message read through
    // its link (the same id), never a filtered group's hidden queue.
    configure(
        &facade,
        json!({"queue":"paid","options":{"from":"orders","where":"headers.type = 'order.paid'"}}),
    )
    .await;
    let _ = pop(
        &facade,
        "orders",
        Some("billing"),
        group_options(Some("headers.type = 'order.paid'"), "new"),
    )
    .await
    .unwrap();
    push(
        &facade,
        json!([{"queue":"orders","partition":"o-2","transactionId":"t3","payload":{"n":3},
             "headers":{"type":"order.paid"}}]),
    )
    .await;
    let (status, fed) = api(
        &facade,
        "POST",
        "/api/v1/feed",
        None,
        json!({"from":"earliest"}),
    )
    .await;
    assert_eq!(status, 200, "{fed}");
    assert!(!shows_hidden(&fed), "{fed}");
    let es = fed["entries"].as_array().expect("entries");
    let t1 = es.iter().find(|e| e["transactionId"] == "t1").expect("t1");
    assert_eq!(t1["headers"]["type"], "order.paid", "{t1}");
    let t3: Vec<&Value> = es.iter().filter(|e| e["transactionId"] == "t3").collect();
    let queues: Vec<&str> = t3.iter().map(|e| e["queue"].as_str().unwrap()).collect();
    assert_eq!(queues, vec!["orders", "paid"], "{fed}");
    assert_eq!(t3[0]["id"], t3[1]["id"], "the same message");
    assert_eq!(t3[1]["payload"]["n"], 3, "read through the link");
    assert_eq!(t3[1]["headers"]["type"], "order.paid");
    // The reader's entry says where the message is at its source.
    assert!(t3[0].get("link").is_none(), "{}", t3[0]);
    assert_eq!(t3[1]["link"]["queue"], "orders", "{}", t3[1]);
    assert_eq!(t3[1]["link"]["partition"], "o-2", "{}", t3[1]);
    assert_eq!(t3[1]["link"]["offset"], t3[0]["offset"], "{}", t3[1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn bad_headers_are_refused_with_the_reason() {
    let (facade, _dir) = open("bad-headers").await;
    for (headers, says) in [
        (json!({"Type":"x"}), "header name"),
        (json!({"queen-x":"x"}), "broker's"),
        (json!({"n":1}), "must be a string"),
        (json!(["a"]), "object of strings"),
    ] {
        let e = try_push(
            &facade,
            json!([{"queue":"orders","payload":{},"headers":headers}]),
        )
        .await
        .unwrap_err();
        assert!(e.contains(says), "{headers}: {e}");
    }
}

// ---------------------------------------------------------------- reading queues

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_reading_queue_takes_what_its_where_takes_with_the_same_ids() {
    let (facade, _dir) = open("reader").await;
    let (s, _) = configure(
        &facade,
        json!({"queue":"orders","options":{"retentionEnabled":false}}),
    )
    .await;
    assert_eq!(s, 200);
    let (s, cfg) = configure(
        &facade,
        json!({"queue":"orders-it","options":{"from":"orders","where":"data.country = 'IT'"}}),
    )
    .await;
    assert_eq!(s, 200, "{cfg}");
    assert_eq!(cfg["route"]["from"][0]["queue"], "orders", "{cfg}");
    assert_eq!(
        cfg["route"]["from"][0]["where"], "data.country = 'IT'",
        "{cfg}"
    );
    let out = push(
        &facade,
        json!([
            {"queue":"orders","partition":"o-1","transactionId":"a","payload":{"country":"IT","n":1}},
            {"queue":"orders","partition":"o-2","transactionId":"b","payload":{"country":"FR","n":2}},
            {"queue":"orders","partition":"o-1","transactionId":"c","payload":{"country":"IT","n":3}}
        ]),
    )
    .await;
    let ids: Vec<Value> = out
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["message_id"].clone())
        .collect();
    let got = drain(&facade, "orders-it").await;
    assert_eq!(data_of(&got, "n"), vec![json!(1), json!(3)], "{got:?}");
    assert_eq!(got[0]["transactionId"], "a");
    assert_eq!(got[0]["id"], ids[0], "the same message, the same id");
    assert_eq!(got[1]["id"], ids[2]);
    assert_eq!(got[0]["partition"], "o-1", "the source's partition name");
    // The source still has all three.
    assert_eq!(drain(&facade, "orders").await.len(), 3);
    // Its source lists it.
    let (_, detail) = api(
        &facade,
        "GET",
        "/api/v1/resources/queues/orders",
        None,
        Value::Null,
    )
    .await;
    assert_eq!(
        detail["route"]["readers"][0]["queue"], "orders-it",
        "{detail}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_retried_push_links_once() {
    let (facade, _dir) = open("reader-dedup").await;
    configure(&facade, json!({"queue":"src","options":{}})).await;
    configure(&facade, json!({"queue":"rd","options":{"from":"src"}})).await;
    for _ in 0..3 {
        push(
            &facade,
            json!([{"queue":"src","partition":"p","transactionId":"same","payload":{"v":1}}]),
        )
        .await;
    }
    assert_eq!(drain(&facade, "rd").await.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_reading_queue_refuses_pushes_and_the_settings_its_sources_own() {
    let (facade, _dir) = open("reader-rules").await;
    configure(&facade, json!({"queue":"src","options":{}})).await;
    let (s, _) = configure(&facade, json!({"queue":"rd","options":{"from":"src"}})).await;
    assert_eq!(s, 200);
    let e = try_push(&facade, json!([{"queue":"rd","payload":{}}])).await;
    let refused = match e {
        Err(e) => e.contains("push to src"),
        Ok(v) => v.to_string().contains("push to src"),
    };
    assert!(refused, "a push into a reader is refused");
    let (s, body) = configure(
        &facade,
        json!({"queue":"rd","options":{"retentionSeconds":60}}),
    )
    .await;
    assert_ne!(s, 200, "{body}");
    assert!(
        body.to_string().contains("keeps what its sources keep"),
        "{body}"
    );
    // An ordinary queue that holds messages cannot become a reader.
    configure(&facade, json!({"queue":"plain","options":{}})).await;
    push(
        &facade,
        json!([{"queue":"plain","partition":"p","transactionId":"own","payload":{}}]),
    )
    .await;
    let (s, body) = configure(&facade, json!({"queue":"plain","options":{"from":"src"}})).await;
    assert_ne!(s, 200, "{body}");
    assert!(
        body.to_string().contains("holds messages"),
        "the refusal says why: {body}"
    );
    // Its own message is still its own, and it still takes pushes.
    let got = drain(&facade, "plain").await;
    assert_eq!(got.len(), 1, "{got:?}");
    let out = push(
        &facade,
        json!([{"queue":"plain","partition":"p","transactionId":"own-2","payload":{}}]),
    )
    .await;
    assert_eq!(out[0]["status"], "queued", "{out}");
}

/// Jepsen W16, 2026-11-14: the reading queue made in the middle of the run
/// was refused, three runs out of three. Its consumers had popped it first,
/// and a pop of a queue that does not exist creates it (so that the group is
/// registered from then on): the name then belonged to an "ordinary queue",
/// which nothing could turn into a reading queue. A queue that holds nothing
/// of its own is a name and its settings: it becomes the reading queue, and
/// the group that waited on it gets what its sources are sent from then on.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_queue_its_consumers_popped_first_becomes_a_reading_queue() {
    let (facade, _dir) = open("reader-after-pop").await;
    configure(&facade, json!({"queue":"src","options":{}})).await;
    // The consumer starts before the reading queue is configured.
    let first = pop(&facade, "rd", Some("g"), group_options(None, "all"))
        .await
        .expect("a pop of a queue that does not exist");
    assert!(messages(&first).is_empty(), "{first}");
    let (s, body) = configure(
        &facade,
        json!({"queue":"rd","options":{"from":"src","where":"data.kind = 'a'"}}),
    )
    .await;
    assert_eq!(s, 200, "the queue the pop made holds nothing: {body}");
    push(
        &facade,
        json!([
            {"queue":"src","partition":"p","transactionId":"t1","payload":{"kind":"a"}},
            {"queue":"src","partition":"p","transactionId":"t2","payload":{"kind":"b"}},
            {"queue":"src","partition":"p","transactionId":"t3","payload":{"kind":"a"}}
        ]),
    )
    .await;
    let got = pop(&facade, "rd", Some("g"), group_options(None, "all"))
        .await
        .expect("pop");
    let msgs = messages(&got);
    let txns: Vec<&str> = msgs
        .iter()
        .map(|m| m["transactionId"].as_str().unwrap_or(""))
        .collect();
    assert_eq!(txns, ["t1", "t3"], "{got}");
    // It is a reading queue in every other way: it takes no push of its own.
    let e = try_push(&facade, json!([{"queue":"rd","payload":{}}])).await;
    let refused = match e {
        Err(e) => e.contains("push to src"),
        Ok(v) => v.to_string().contains("push to src"),
    };
    assert!(refused, "a push into a reader is refused");

    // So does a queue that was configured and never pushed to.
    configure(&facade, json!({"queue":"empty","options":{"leaseTime":7}})).await;
    let (s, body) = configure(&facade, json!({"queue":"empty","options":{"from":"src"}})).await;
    assert_eq!(s, 200, "{body}");
    assert_eq!(
        body["options"]["leaseTime"], 7,
        "its own settings stay: {body}"
    );
    assert_eq!(body["route"]["from"][0]["queue"], "src", "{body}");
}

/// The check is the planner's, with every entry in flight in view: a push
/// that creates the queue's first partition in the same cycle as the
/// configure is seen, although nothing of it is committed yet.
#[test]
fn a_push_in_flight_keeps_its_queue_from_becoming_a_reader() {
    use super::planner_harness::{effects, push as push_cmd, qcfg, Cell, TENANT};
    use crate::rsm::effect::{Effect, QueueConfig, SourceRule};
    use crate::rsm::planner::Plan;
    let upsert = |queue: &str, cfg: QueueConfig| Effect::QueueUpsert {
        tenant: TENANT.to_string(),
        queue: queue.to_string(),
        cfg,
    };
    let reading = || {
        let mut cfg = qcfg();
        cfg.route_mut().sources = vec![SourceRule {
            queue: "src".to_string(),
            where_json: "[]".to_string(),
        }];
        cfg
    };
    let mut c = Cell::new("route-in-use");
    let made = c.run(&[effects(
        1,
        vec![
            upsert("src", qcfg()),
            upsert("busy", qcfg()),
            upsert("idle", qcfg()),
        ],
    )]);
    assert!(matches!(made.plan(0), Ok(Plan::Logged { .. })));
    let cycle = c.run(&[
        push_cmd(2, "busy", "p0", &["t1"]),
        effects(3, vec![upsert("busy", reading())]),
        effects(4, vec![upsert("idle", reading())]),
    ]);
    assert!(matches!(cycle.plan(0), Ok(Plan::Logged { .. })));
    match cycle.plan(1) {
        Err(r) => {
            assert_eq!(r.code, "bad_request");
            assert!(r.message.contains("holds messages"), "{}", r.message);
        }
        other => panic!("a queue with a push in flight became a reader: {other:?}"),
    }
    assert!(
        matches!(cycle.plan(2), Ok(Plan::Logged { .. })),
        "a queue that holds nothing becomes one: {:?}",
        cycle.plan(2)
    );
}

/// A message a reader cannot take (its `partitionBy` value is too long for a
/// partition name) is not routed to it; the push to its source is not refused,
/// and leaves no state behind that the next push would trip on.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_message_a_reader_cannot_take_does_not_refuse_its_source() {
    let (facade, _dir) = open("reader-too-long").await;
    configure(&facade, json!({"queue":"src","options":{}})).await;
    configure(
        &facade,
        json!({"queue":"by-key","options":{"from":"src","partitionBy":"data.k"}}),
    )
    .await;
    let long = "k".repeat(4000);
    let out = push(
        &facade,
        json!([
            {"queue":"src","partition":"p","transactionId":"t1","payload":{"k": long}},
            {"queue":"src","partition":"p","transactionId":"t2","payload":{"k":"short"}}
        ]),
    )
    .await;
    assert_eq!(out[0]["status"], "queued", "{out}");
    assert_eq!(out[1]["status"], "queued", "{out}");
    let out = push(
        &facade,
        json!([{"queue":"src","partition":"p","transactionId":"t3","payload":{"k":"short"}}]),
    )
    .await;
    assert_eq!(out[0]["status"], "queued", "{out}");
    assert_eq!(drain(&facade, "src").await.len(), 3);
    let got = drain(&facade, "by-key").await;
    let txns: Vec<&str> = got
        .iter()
        .map(|m| m["transactionId"].as_str().unwrap())
        .collect();
    assert_eq!(txns, vec!["t2", "t3"], "the long one is not routed");
}

/// A push to a NEW partition of a reading queue is refused alone: the router
/// hands out no partition id for it (a lane-local create would leave that id
/// unused, which refuses the whole cycle and every other command in it).
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_push_to_a_readers_new_partition_is_refused_alone() {
    let (facade, _dir) = open("reader-new-part").await;
    configure(&facade, json!({"queue":"src","options":{}})).await;
    configure(&facade, json!({"queue":"rd","options":{"from":"src"}})).await;
    configure(&facade, json!({"queue":"plain","options":{}})).await;
    for round in 0..3 {
        let items = json!([
            {"queue":"rd","partition":format!("fresh-{round}"),"transactionId":format!("r{round}"),"payload":{}},
            {"queue":"plain","partition":format!("new-{round}"),"transactionId":format!("p{round}"),"payload":{}}
        ]);
        let out = tokio::time::timeout(Duration::from_secs(10), try_push(&facade, items))
            .await
            .expect("answered, not retried for ever")
            .expect("one refused item does not fail the request");
        let rows = out.as_array().cloned().unwrap_or_default();
        let of = |txn: String| {
            rows.iter()
                .find(|r| r["transactionId"] == txn || r["transaction_id"] == txn)
                .cloned()
                .unwrap_or_else(|| panic!("no result for {txn}: {out}"))
        };
        let rd = of(format!("r{round}"));
        assert_eq!(rd["status"], "error", "{out}");
        assert!(rd.to_string().contains("push to src"), "{out}");
        assert_eq!(of(format!("p{round}"))["status"], "queued", "{out}");
    }
    assert_eq!(drain(&facade, "plain").await.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn loops_and_missing_sources_are_refused() {
    let (facade, _dir) = open("reader-loops").await;
    configure(&facade, json!({"queue":"a","options":{}})).await;
    let (s, _) = configure(&facade, json!({"queue":"b","options":{"from":"a"}})).await;
    assert_eq!(s, 200);
    let (s, _) = configure(&facade, json!({"queue":"c","options":{"from":"b"}})).await;
    assert_eq!(s, 200);
    // `a` cannot read `c` (c reads b reads a): an ordinary queue, so refused as
    // a conversion; a new reader `d` reading itself through `c` is a loop only
    // if c read d, so test the direct self-read and a missing source.
    let (s, body) = configure(&facade, json!({"queue":"e","options":{"from":"e"}})).await;
    assert_ne!(s, 200, "{body}");
    let (s, body) = configure(&facade, json!({"queue":"f","options":{"from":"nope"}})).await;
    assert_ne!(s, 200, "{body}");
    assert!(body.to_string().contains("does not exist"), "{body}");
    // b re-pointed to read c: c reads b, a loop.
    let (s, body) = configure(&facade, json!({"queue":"b","options":{"from":"c"}})).await;
    assert_ne!(s, 200, "{body}");
    assert!(body.to_string().contains("back"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn partition_by_rekeys_and_from_merges() {
    let (facade, _dir) = open("rekey").await;
    configure(&facade, json!({"queue":"orders","options":{}})).await;
    configure(&facade, json!({"queue":"refunds","options":{}})).await;
    let (s, body) = configure(
        &facade,
        json!({"queue":"by-customer","options":{"from":["orders","refunds"],"partitionBy":"data.customer"}}),
    )
    .await;
    assert_eq!(s, 200, "{body}");
    push(&facade, json!([
        {"queue":"orders","partition":"o-1","transactionId":"o1","payload":{"customer":"c-1","n":1}},
        {"queue":"orders","partition":"o-2","transactionId":"o2","payload":{"customer":"c-2","n":2}},
        {"queue":"orders","partition":"o-3","transactionId":"o3","payload":{"n":9}}
    ])).await;
    push(&facade, json!([
        {"queue":"refunds","partition":"r-1","transactionId":"r1","payload":{"customer":"c-1","n":3}}
    ])).await;
    let got = drain(&facade, "by-customer").await;
    let mut parts: Vec<(String, i64)> = got
        .iter()
        .map(|m| {
            (
                m["partition"].as_str().unwrap().to_string(),
                m["data"]["n"].as_i64().unwrap(),
            )
        })
        .collect();
    parts.sort();
    assert_eq!(
        parts,
        vec![("c-1".into(), 1), ("c-1".into(), 3), ("c-2".into(), 2)],
        "partitions named by the field; a message without it is not routed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_chain_of_readers_points_at_the_original() {
    let (facade, _dir) = open("chain").await;
    configure(&facade, json!({"queue":"x","options":{}})).await;
    configure(
        &facade,
        json!({"queue":"y","options":{"from":"x","where":"data.k exists"}}),
    )
    .await;
    configure(
        &facade,
        json!({"queue":"z","options":{"from":"y","where":"data.k = 2"}}),
    )
    .await;
    push(
        &facade,
        json!([
            {"queue":"x","partition":"p","transactionId":"1","payload":{"k":1}},
            {"queue":"x","partition":"p","transactionId":"2","payload":{"k":2}},
            {"queue":"x","partition":"p","transactionId":"3","payload":{"j":3}}
        ]),
    )
    .await;
    assert_eq!(
        data_of(&drain(&facade, "y").await, "k"),
        vec![json!(1), json!(2)]
    );
    let z = drain(&facade, "z").await;
    assert_eq!(data_of(&z, "k"), vec![json!(2)], "{z:?}");
    assert_eq!(z[0]["transactionId"], "2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn headers_route_and_a_hop_adds_its_own() {
    let (facade, _dir) = open("header-route").await;
    configure(&facade, json!({"queue":"events","options":{}})).await;
    configure(
        &facade,
        json!({"queue":"paid","options":{"from":"events","where":"headers.type = 'order.paid'"}}),
    )
    .await;
    push(&facade, json!([
        {"queue":"events","partition":"e","transactionId":"1","payload":{"n":1},"headers":{"type":"order.paid"}},
        {"queue":"events","partition":"e","transactionId":"2","payload":{"n":2},"headers":{"type":"order.created"}}
    ])).await;
    let got = drain(&facade, "paid").await;
    assert_eq!(data_of(&got, "n"), vec![json!(1)]);
    assert_eq!(
        got[0]["headers"]["type"], "order.paid",
        "the source's headers come through"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_source_with_readers_is_deleted_only_by_force() {
    let (facade, _dir) = open("delete").await;
    configure(&facade, json!({"queue":"src","options":{}})).await;
    configure(&facade, json!({"queue":"rd","options":{"from":"src"}})).await;
    let (s, body) = api(
        &facade,
        "DELETE",
        "/api/v1/resources/queues/src",
        None,
        Value::Null,
    )
    .await;
    assert_eq!(s, 409, "{body}");
    assert_eq!(body["code"], "has_readers");
    let (s, body) = api(
        &facade,
        "DELETE",
        "/api/v1/resources/queues/src",
        Some("force=true"),
        Value::Null,
    )
    .await;
    assert_eq!(s, 200, "{body}");
    // The reader's delete takes it off its sources' list (here, gone already).
    let (s, _) = api(
        &facade,
        "DELETE",
        "/api/v1/resources/queues/rd",
        None,
        Value::Null,
    )
    .await;
    assert_eq!(s, 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_reader_deleted_leaves_its_source_unread() {
    let (facade, _dir) = open("reader-delete").await;
    configure(&facade, json!({"queue":"src","options":{}})).await;
    configure(&facade, json!({"queue":"rd","options":{"from":"src"}})).await;
    let (s, _) = api(
        &facade,
        "DELETE",
        "/api/v1/resources/queues/rd",
        None,
        Value::Null,
    )
    .await;
    assert_eq!(s, 200);
    let (_, detail) = api(
        &facade,
        "GET",
        "/api/v1/resources/queues/src",
        None,
        Value::Null,
    )
    .await;
    assert!(
        detail.get("route").is_none()
            || detail["route"]["readers"]
                .as_array()
                .is_none_or(|r| r.is_empty()),
        "{detail}"
    );
    // And pushes to it plan as before.
    let out = push(&facade, json!([{"queue":"src","payload":{"v":1}}])).await;
    assert_eq!(out[0]["status"], "queued");
}

// ---------------------------------------------------------------- catch-up

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_reader_started_with_history_has_it_then_the_live_ones_in_order() {
    let (facade, _dir) = open("catch-up").await;
    configure(&facade, json!({"queue":"src","options":{}})).await;
    let mut items = Vec::new();
    for i in 0..30 {
        items.push(json!({"queue":"src","partition":format!("p{}", i % 3),"transactionId":format!("h{i}"),"payload":{"i":i,"keep":i % 2 == 0}}));
    }
    push(&facade, Value::Array(items)).await;
    let (s, body) = configure(
        &facade,
        json!({"queue":"rd","options":{"from":"src","where":"data.keep = true","start":"all"}}),
    )
    .await;
    assert_eq!(s, 200, "{body}");
    assert_eq!(body["catchingUp"], false, "{body}");
    push(&facade, json!([{"queue":"src","partition":"p0","transactionId":"live","payload":{"i":99,"keep":true}}])).await;
    let got = drain(&facade, "rd").await;
    let mut by_part: std::collections::BTreeMap<String, Vec<i64>> = Default::default();
    for m in &got {
        by_part
            .entry(m["partition"].as_str().unwrap().into())
            .or_default()
            .push(m["data"]["i"].as_i64().unwrap());
    }
    assert_eq!(
        by_part["p0"],
        vec![0, 6, 12, 18, 24, 99],
        "history in order, then the live one: {by_part:?}"
    );
    assert_eq!(got.len(), 16);
}

// ---------------------------------------------------------------- filtered groups

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_filtered_group_gets_only_what_its_where_takes() {
    let (facade, _dir) = open("group").await;
    configure(&facade, json!({"queue":"events","options":{}})).await;
    // The first pop registers the filter (mode new: from now on).
    let first = pop(
        &facade,
        "events",
        Some("billing"),
        group_options(Some("data.kind in ('paid', 'refunded')"), "new"),
    )
    .await
    .unwrap();
    assert!(messages(&first).is_empty());
    push(
        &facade,
        json!([
            {"queue":"events","partition":"o-1","transactionId":"1","payload":{"kind":"created"}},
            {"queue":"events","partition":"o-1","transactionId":"2","payload":{"kind":"paid"}},
            {"queue":"events","partition":"o-2","transactionId":"3","payload":{"kind":"refunded"}}
        ]),
    )
    .await;
    let mut got = Vec::new();
    for _ in 0..200 {
        let p = pop(
            &facade,
            "events",
            Some("billing"),
            group_options(None, "new"),
        )
        .await
        .unwrap();
        let m = messages(&p);
        if !m.is_empty() {
            ack_all(&facade, Some("billing"), &m).await;
            got.extend(m);
        }
        if got.len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut kinds: Vec<String> = got
        .iter()
        .map(|m| m["data"]["kind"].as_str().unwrap().to_string())
        .collect();
    kinds.sort();
    assert_eq!(kinds, vec!["paid", "refunded"]);
    // Another where for the same group: refused.
    let e = pop(
        &facade,
        "events",
        Some("billing"),
        group_options(Some("data.kind = 'x'"), "new"),
    )
    .await
    .unwrap_err();
    assert!(e.contains("where") || e.contains("filters"), "{e}");
    // A where without a group: refused.
    let e = pop(
        &facade,
        "events",
        None,
        group_options(Some("data.kind = 'x'"), "new"),
    )
    .await
    .unwrap_err();
    assert!(
        e.contains("consumer group") || e.contains("consumerGroup"),
        "{e}"
    );
    // The hidden queue is not in the list.
    let (_, list) = api(
        &facade,
        "GET",
        "/api/v1/resources/queues",
        None,
        Value::Null,
    )
    .await;
    assert!(!shows_hidden(&list), "{list}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_filtered_group_says_so_on_every_pop_and_in_every_listing() {
    let (facade, _dir) = open("group-shown").await;
    configure(&facade, json!({"queue":"events","options":{}})).await;
    let pop_raw = |w: Option<&str>| {
        facade.pop_wildcard(
            ctx(),
            PopReq {
                queue: "events".into(),
                group: Some("billing".into()),
                batch: 10,
                auto_ack: false,
                wait: false,
                timeout_ms: 1000,
                options: group_options(w, "new"),
            },
        )
    };
    // Empty, and still an answer that says it filtered.
    let out = pop_raw(Some("data.kind = 'paid'")).await.expect("pop");
    assert!(out.empty);
    assert!(out.filtered);
    assert!(out.body.contains("\"filtered\":true"), "{}", out.body);
    // An ordinary group's answer has no such key.
    let plain = facade
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "events".into(),
                group: Some("plain".into()),
                batch: 10,
                auto_ack: false,
                wait: false,
                timeout_ms: 1000,
                options: group_options(None, "new"),
            },
        )
        .await
        .expect("pop");
    assert!(!plain.filtered);
    assert!(!plain.body.contains("filtered"), "{}", plain.body);
    push(
        &facade,
        json!([{"queue":"events","partition":"o-1","transactionId":"1","payload":{"kind":"paid"}}]),
    )
    .await;
    let mut got = Vec::new();
    for _ in 0..200 {
        let out = pop_raw(None).await.expect("pop");
        assert!(out.filtered, "{}", out.body);
        if !out.empty {
            got = messages(&parse(&out.body));
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(got.len(), 1);
    // Listings name the source and the where, never the hidden queue.
    let (_, groups) = api(&facade, "GET", "/api/v1/consumer-groups", None, Value::Null).await;
    assert!(!shows_hidden(&groups), "{groups}");
    let row = groups
        .as_array()
        .and_then(|a| a.iter().find(|r| r["name"] == "billing"))
        .cloned()
        .unwrap_or_else(|| panic!("{groups}"));
    assert_eq!(row["queueName"], "events", "{row}");
    assert_eq!(row["filtered"], true, "{row}");
    assert_eq!(row["where"], "data.kind = 'paid'", "{row}");
    let (_, one) = api(
        &facade,
        "GET",
        "/api/v1/consumer-groups/billing",
        None,
        Value::Null,
    )
    .await;
    assert_eq!(one["events"]["filtered"], true, "{one}");
    let (_, detail) = api(
        &facade,
        "GET",
        "/api/v1/resources/queues/events",
        None,
        Value::Null,
    )
    .await;
    let reader = &detail["route"]["readers"][0];
    assert_eq!(reader["queue"], "events", "{detail}");
    assert_eq!(reader["group"], "billing", "{detail}");
    assert_eq!(reader["where"], "data.kind = 'paid'", "{detail}");
    assert!(reader["oldestUnconsumedAt"].is_string(), "{detail}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_group_that_reads_unfiltered_cannot_start_filtering() {
    let (facade, _dir) = open("group-existing").await;
    configure(&facade, json!({"queue":"events","options":{}})).await;
    push(&facade, json!([{"queue":"events","payload":{"kind":"x"}}])).await;
    pop(&facade, "events", Some("plain"), group_options(None, "all"))
        .await
        .unwrap();
    let e = pop(
        &facade,
        "events",
        Some("plain"),
        group_options(Some("data.kind = 'x'"), "new"),
    )
    .await
    .unwrap_err();
    assert!(e.contains("new group"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_filtered_group_with_mode_all_starts_with_history() {
    let (facade, _dir) = open("group-all").await;
    configure(&facade, json!({"queue":"events","options":{}})).await;
    push(&facade, json!([
        {"queue":"events","partition":"o-1","transactionId":"1","payload":{"kind":"paid","n":1}},
        {"queue":"events","partition":"o-1","transactionId":"2","payload":{"kind":"created","n":2}}
    ])).await;
    let mut got = Vec::new();
    let first = pop(
        &facade,
        "events",
        Some("audit"),
        group_options(Some("data.kind = 'paid'"), "all"),
    )
    .await
    .unwrap();
    got.extend(messages(&first));
    ack_all(&facade, Some("audit"), &got.clone()).await;
    if got.is_empty() {
        let p = pop(&facade, "events", Some("audit"), group_options(None, "all"))
            .await
            .unwrap();
        got.extend(messages(&p));
    }
    assert_eq!(data_of(&got, "n"), vec![json!(1)], "{got:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_group_where_changes_from_the_next_push() {
    let (facade, _dir) = open("group-change").await;
    configure(&facade, json!({"queue":"events","options":{}})).await;
    pop(
        &facade,
        "events",
        Some("g"),
        group_options(Some("data.kind = 'a'"), "new"),
    )
    .await
    .unwrap();
    let (s, body) = api(
        &facade,
        "POST",
        "/api/v1/consumer-groups/g/subscription",
        None,
        json!({"queue":"events","where":"data.kind = 'b'"}),
    )
    .await;
    assert_eq!(s, 200, "{body}");
    push(
        &facade,
        json!([
            {"queue":"events","partition":"p","transactionId":"1","payload":{"kind":"a"}},
            {"queue":"events","partition":"p","transactionId":"2","payload":{"kind":"b"}}
        ]),
    )
    .await;
    let p = pop(
        &facade,
        "events",
        Some("g"),
        group_options(Some("data.kind = 'b'"), "new"),
    )
    .await
    .unwrap();
    assert_eq!(data_of(&messages(&p), "kind"), vec![json!("b")]);
}

// ---------------------------------------------------------------- link

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_link_forwards_the_message_it_acks_without_its_payload() {
    let (facade, _dir) = open("link").await;
    configure(&facade, json!({"queue":"orders","options":{}})).await;
    configure(&facade, json!({"queue":"review","options":{}})).await;
    push(&facade, json!([{"queue":"orders","partition":"o-1","transactionId":"big","payload":{"amount":9000},"headers":{"type":"order"}}])).await;
    let p = pop(
        &facade,
        "orders",
        None,
        PopOptions {
            max_parts: 8,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let m = messages(&p);
    assert_eq!(m.len(), 1);
    let msg = &m[0];
    let body = json!({
        "operations": [
            {"type":"ack","transactionId":msg["transactionId"],"partitionId":msg["partitionId"],"leaseId":msg["leaseId"],"consumerGroup":"__QUEUE_MODE__","status":"completed"},
            {"type":"link","queue":"review","partition":"o-1",
             "from":{"partitionId":msg["partitionId"],"offset":msg["offset"],"transactionId":msg["transactionId"]},
             "headers":{"review-reason":"amount"}}
        ]
    });
    let out = facade
        .transaction(
            ctx(),
            TxnReq {
                raw: body.to_string().into_bytes(),
            },
        )
        .await
        .expect("transaction");
    let out = parse(&out.body);
    assert_eq!(out["success"], true, "{out}");
    let got = drain(&facade, "review").await;
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(got[0]["id"], msg["id"], "the same message");
    assert_eq!(got[0]["data"]["amount"], 9000);
    assert_eq!(
        got[0]["headers"],
        json!({"review-reason":"amount","type":"order"}),
        "the source's headers, then the link's"
    );
    // The orders queue is acked.
    assert!(messages(
        &pop(&facade, "orders", None, PopOptions::default())
            .await
            .unwrap()
    )
    .is_empty());
    // A link into a reading queue is refused.
    configure(&facade, json!({"queue":"rd","options":{"from":"orders"}})).await;
    let body = json!({"operations":[{"type":"link","queue":"rd","partition":"p",
        "from":{"partitionId":msg["partitionId"],"offset":msg["offset"],"transactionId":msg["transactionId"]}}]});
    let out = facade
        .transaction(
            ctx(),
            TxnReq {
                raw: body.to_string().into_bytes(),
            },
        )
        .await
        .expect("txn");
    let out = parse(&out.body);
    assert_eq!(out["success"], false, "{out}");
    assert_eq!(out["reason"], "reading_queue", "{out}");
    // A wrong transaction id at the offset: refused.
    let body = json!({"operations":[{"type":"link","queue":"review","partition":"p",
        "from":{"partitionId":msg["partitionId"],"offset":msg["offset"],"transactionId":"other"}}]});
    let out = facade
        .transaction(
            ctx(),
            TxnReq {
                raw: body.to_string().into_bytes(),
            },
        )
        .await
        .expect("txn");
    assert_eq!(parse(&out.body)["reason"], "source_mismatch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_link_target_becomes_a_holder_of_its_source() {
    let (facade, _dir) = open("holder").await;
    configure(&facade, json!({"queue":"orders","options":{}})).await;
    configure(&facade, json!({"queue":"review","options":{}})).await;
    push(
        &facade,
        json!([{"queue":"orders","partition":"o","transactionId":"t","payload":{"v":1}}]),
    )
    .await;
    let p = pop(
        &facade,
        "orders",
        None,
        PopOptions {
            max_parts: 8,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let msg = messages(&p)[0].clone();
    let body = json!({"operations":[{"type":"link","queue":"review","partition":"o",
        "from":{"partitionId":msg["partitionId"],"offset":msg["offset"],"transactionId":msg["transactionId"]}}]});
    let out = facade
        .transaction(
            ctx(),
            TxnReq {
                raw: body.to_string().into_bytes(),
            },
        )
        .await
        .expect("txn");
    assert_eq!(parse(&out.body)["success"], true);
    let (_, detail) = api(
        &facade,
        "GET",
        "/api/v1/resources/queues/orders",
        None,
        Value::Null,
    )
    .await;
    assert_eq!(detail["route"]["holders"][0]["queue"], "review", "{detail}");
    // A holder takes nothing at push time: the source's pushes stay in their
    // lanes (batcher_lanes.rs, `queue_routed`), its retention still waits.
    let cfg = facade
        .queue_config_of(crate::config::DEFAULT_TENANT, "orders")
        .await
        .expect("read")
        .expect("orders");
    assert!(!cfg.has_readers(), "a holder is not a reader");
    assert_eq!(
        cfg.route().map(|r| r.holders.clone()),
        Some(vec!["review".to_string()])
    );
}

async fn txn(facade: &RaftFacade, body: Value) -> Value {
    let out = facade
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

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_second_link_of_a_message_is_a_no_op_not_a_rollback() {
    let (facade, _dir) = open("relink").await;
    configure(&facade, json!({"queue":"orders","options":{}})).await;
    configure(&facade, json!({"queue":"review","options":{}})).await;
    push(
        &facade,
        json!([{"queue":"orders","partition":"o","transactionId":"t","payload":{"v":1}}]),
    )
    .await;
    let p = pop(
        &facade,
        "orders",
        None,
        PopOptions {
            max_parts: 8,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let msg = messages(&p)[0].clone();
    let link = json!({"type":"link","queue":"review","partition":"o",
        "from":{"partitionId":msg["partitionId"],"offset":msg["offset"],"transactionId":msg["transactionId"]}});
    let out = txn(&facade, json!({"operations":[link.clone()]})).await;
    assert_eq!(out["success"], true, "{out}");
    assert_eq!(out["results"][0]["type"], "link", "{out}");
    assert!(out["results"][0].get("duplicate").is_none(), "{out}");
    // Again, with a push beside it: the push lands, the link is done already.
    let out = txn(
        &facade,
        json!({"operations":[link, {"type":"push","items":[{"queue":"audit","partition":"o","transactionId":"a1","payload":{}}]}]}),
    )
    .await;
    assert_eq!(out["success"], true, "{out}");
    assert_eq!(out["results"][0]["duplicate"], true, "{out}");
    assert_eq!(out["results"][0]["success"], true, "{out}");
    assert_eq!(drain(&facade, "review").await.len(), 1);
    assert_eq!(drain(&facade, "audit").await.len(), 1);
    // A pushed message's duplicate still rolls the bundle back: the gate.
    let out = txn(
        &facade,
        json!({"operations":[{"type":"push","items":[{"queue":"audit","partition":"o","transactionId":"a1","payload":{}}]}]}),
    )
    .await;
    assert_eq!(out["success"], false, "{out}");
    assert_eq!(out["reason"], "duplicate", "{out}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_stream_cycle_links_its_input_message() {
    let (facade, _dir) = open("cyclelink").await;
    configure(&facade, json!({"queue":"src","options":{}})).await;
    configure(&facade, json!({"queue":"sink","options":{}})).await;
    push(
        &facade,
        json!([{"queue":"src","partition":"p1","transactionId":"m1","payload":{"v":1},"headers":{"type":"a"}}]),
    )
    .await;
    let p = pop(&facade, "src", Some("g"), group_options(None, "all"))
        .await
        .unwrap();
    let msg = messages(&p)[0].clone();
    let (status, reg) = api(
        &facade,
        "POST",
        "/streams/v1/queries",
        None,
        json!({"name":"fwd","source_queue":"src","sink_queue":"sink","config_hash":"h"}),
    )
    .await;
    assert_eq!(status, 200, "{reg}");
    let qid = reg["query_id"].as_str().expect("query id").to_string();
    let cycle = |link: Value, ack: Value| {
        json!({"query_id": qid, "partition_id": msg["partitionId"], "consumer_group": "g",
            "push_items": [{"queue":"sink","partition":"p1","link": link,"headers":{"hop":"1"}}],
            "ack": ack, "release_lease": true})
    };
    let (status, out) = api(
        &facade,
        "POST",
        "/streams/v1/cycle",
        None,
        cycle(
            json!({"offset": msg["offset"], "transactionId": "m1"}),
            json!({"leaseId": msg["leaseId"], "status": "completed", "count": 1}),
        ),
    )
    .await;
    assert_eq!(status, 200, "{out}");
    assert_eq!(out["success"], true, "{out}");
    assert_eq!(out["push_results"][0]["status"], "created", "{out}");
    assert_eq!(out["push_results"][0]["transactionId"], "m1", "{out}");
    assert_eq!(out["push_results"][0]["messageId"], msg["id"], "{out}");
    let got = drain(&facade, "sink").await;
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(got[0]["id"], msg["id"], "the same message");
    assert_eq!(got[0]["data"]["v"], 1);
    assert_eq!(got[0]["headers"], json!({"hop":"1","type":"a"}));
    // The same link again: a no-op, the cycle commits.
    let (_, out) = api(
        &facade,
        "POST",
        "/streams/v1/cycle",
        None,
        cycle(
            json!({"offset": msg["offset"], "transactionId": "m1"}),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(out["success"], true, "{out}");
    assert_eq!(out["push_results"][0]["status"], "duplicate", "{out}");
    // A transaction id that is not the one at that offset fails the cycle.
    let (_, out) = api(
        &facade,
        "POST",
        "/streams/v1/cycle",
        None,
        cycle(
            json!({"offset": msg["offset"], "transactionId": "other"}),
            Value::Null,
        ),
    )
    .await;
    assert_eq!(out["success"], false, "{out}");
    assert_eq!(out["reason"], "source_mismatch", "{out}");
    // Into a reading queue: refused.
    configure(&facade, json!({"queue":"rd","options":{"from":"src"}})).await;
    let mut body = cycle(
        json!({"offset": msg["offset"], "transactionId": "m1"}),
        Value::Null,
    );
    body["push_items"][0]["queue"] = json!("rd");
    let (_, out) = api(&facade, "POST", "/streams/v1/cycle", None, body).await;
    assert_eq!(out["reason"], "reading_queue", "{out}");
}

// ---------------------------------------------------------------- dead letters

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_dead_letter_keeps_its_headers_and_its_replay_too() {
    let (facade, _dir) = open("dlqheaders").await;
    configure(
        &facade,
        json!({"queue":"jobs","options":{"deadLetterQueue":true,"retryLimit":0}}),
    )
    .await;
    push(
        &facade,
        json!([{"queue":"jobs","partition":"p","transactionId":"j1","payload":{"n":1},"headers":{"type":"job","traceparent":"00-ab"}}]),
    )
    .await;
    let p = pop(
        &facade,
        "jobs",
        None,
        PopOptions {
            max_parts: 8,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let m = messages(&p);
    assert_eq!(m.len(), 1, "{p}");
    facade
        .ack(
            ctx(),
            AckReq {
                queue: None,
                group: "__QUEUE_MODE__".into(),
                raw: json!({"acknowledgments":[{"transactionId":"j1","partitionId":m[0]["partitionId"],
                    "leaseId":m[0]["leaseId"],"status":"dlq","error":"poison"}],
                    "consumerGroup":"__QUEUE_MODE__"})
                .to_string()
                .into_bytes(),
            },
        )
        .await
        .expect("ack");
    let mut dead = Value::Null;
    for _ in 0..50 {
        let (_, d) = api(
            &facade,
            "GET",
            "/api/v1/dlq",
            Some("queue=jobs"),
            Value::Null,
        )
        .await;
        if d["messages"].as_array().is_some_and(|a| !a.is_empty()) {
            dead = d;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let row = &dead["messages"][0];
    assert_eq!(row["transactionId"], "j1", "{dead}");
    assert_eq!(
        row["headers"],
        json!({"traceparent":"00-ab","type":"job"}),
        "{dead}"
    );
    let id = row["id"].as_str().expect("dlq id").to_string();
    let (status, out) = api(
        &facade,
        "POST",
        &format!("/api/v1/dlq/{id}/replay"),
        None,
        Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{out}");
    let back = drain(&facade, "jobs").await;
    assert_eq!(back.len(), 1, "{back:?}");
    assert_eq!(back[0]["data"]["n"], 1);
    assert_eq!(
        back[0]["headers"],
        json!({"traceparent":"00-ab","type":"job"})
    );
}

// ------------------------------------------- lag and figures of a source

/// Whether a hidden queue's name shows anywhere in `v`: its separator, in a
/// key or a string (a body that did not parse keeps the escaped form).
fn shows_hidden(v: &Value) -> bool {
    match v {
        Value::String(s) => s.contains('\u{1}') || s.contains("\\u0001"),
        Value::Array(a) => a.iter().any(shows_hidden),
        Value::Object(o) => o
            .iter()
            .any(|(k, v)| k.contains('\u{1}') || shows_hidden(v)),
        _ => false,
    }
}

/// How many times transaction id `txn` shows in `v`, at any depth.
fn txn_count(v: &Value, txn: &str) -> usize {
    match v {
        Value::Array(a) => a.iter().map(|x| txn_count(x, txn)).sum(),
        Value::Object(o) => {
            let here = usize::from(o.get("transactionId").and_then(Value::as_str) == Some(txn));
            here + o.values().map(|x| txn_count(x, txn)).sum::<usize>()
        }
        _ => 0,
    }
}

/// The source of the lag tests: `lagsrc` (namespace `lagns`), partitions p1
/// to p4 with two messages each. Filtered group `ga` takes p1 and p2 and has
/// consumed all four of its messages; `gb` takes p3 and has consumed
/// nothing; nothing reads p4.
async fn lag_scenario(tag: &str) -> (RaftFacade, PathBuf) {
    let (facade, dir) = open(tag).await;
    let (status, out) = configure(
        &facade,
        json!({"queue":"lagsrc","namespace":"lagns","options":{}}),
    )
    .await;
    assert_eq!(status, 200, "{out}");
    for (group, filter) in [
        ("ga", "partition in ('p1', 'p2')"),
        ("gb", "partition = 'p3'"),
    ] {
        let first = pop(
            &facade,
            "lagsrc",
            Some(group),
            group_options(Some(filter), "new"),
        )
        .await
        .unwrap();
        assert!(messages(&first).is_empty(), "{first}");
    }
    let items: Vec<Value> = (1..=4)
        .flat_map(|p| {
            (0..2).map(move |i| {
                json!({"queue":"lagsrc","partition":format!("p{p}"),"transactionId":format!("t{p}-{i}"),"payload":{"p":p,"i":i}})
            })
        })
        .collect();
    push(&facade, Value::Array(items)).await;
    let mut got = 0;
    for _ in 0..300 {
        let p = pop(&facade, "lagsrc", Some("ga"), group_options(None, "new"))
            .await
            .unwrap();
        let m = messages(&p);
        if !m.is_empty() {
            ack_all(&facade, Some("ga"), &m).await;
            got += m.len();
        }
        if got >= 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(got, 4, "ga takes p1 and p2: four messages");
    // The acks applied: ga's own row (already right) has nothing left.
    for _ in 0..500 {
        let (_, one) = api(
            &facade,
            "GET",
            "/api/v1/consumer-groups/ga",
            None,
            Value::Null,
        )
        .await;
        let parts = one["lagsrc"]["partitions"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        if parts.len() == 2 && parts.iter().all(|p| p["offsetLag"] == 0) {
            return (facade, dir);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("ga's acks never applied");
}

/// The queue list's row of `queue`.
async fn listed(facade: &RaftFacade, queue: &str) -> Value {
    let (_, list) = api(facade, "GET", "/api/v1/resources/queues", None, Value::Null).await;
    list["queues"]
        .as_array()
        .and_then(|a| a.iter().find(|q| q["name"] == queue))
        .cloned()
        .unwrap_or_else(|| panic!("{queue} not listed: {list}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_source_counts_what_its_filtered_groups_still_have_and_what_nothing_reads() {
    let (facade, _dir) = lag_scenario("lag-list").await;
    let row = listed(&facade, "lagsrc").await;
    // ga's four are consumed; gb's two (p3) wait for gb; p4's two wait for
    // nobody: unread, and pending too, as every message nobody consumed.
    assert_eq!(row["partitions"], 4, "{row}");
    assert_eq!(row["messages"]["total"], 8, "{row}");
    assert_eq!(row["messages"]["pending"], 4, "{row}");
    assert_eq!(row["messages"]["unread"], 2, "{row}");
    assert_eq!(row["messages"]["completed"], 4, "{row}");
    // The overview and the label rows add up the queues the list shows: the
    // hidden queues are the groups', not queues of their own.
    let (_, list) = api(
        &facade,
        "GET",
        "/api/v1/resources/queues",
        None,
        Value::Null,
    )
    .await;
    let rows = list["queues"].as_array().cloned().unwrap_or_default();
    let sum = |f: &dyn Fn(&Value) -> i64| rows.iter().map(f).sum::<i64>();
    let (_, overview) = api(
        &facade,
        "GET",
        "/api/v1/resources/overview",
        None,
        Value::Null,
    )
    .await;
    assert_eq!(overview["queues"], rows.len(), "{overview}");
    assert_eq!(
        overview["partitions"],
        sum(&|q| q["partitions"].as_i64().unwrap_or(0)),
        "{overview}"
    );
    assert_eq!(
        overview["messages"]["total"],
        sum(&|q| q["messages"]["total"].as_i64().unwrap_or(0)),
        "{overview}"
    );
    assert_eq!(
        overview["messages"]["pending"],
        sum(&|q| q["messages"]["pending"].as_i64().unwrap_or(0)),
        "{overview}"
    );
    assert_eq!(
        overview["messages"]["unread"],
        sum(&|q| q["messages"]["unread"].as_i64().unwrap_or(0)),
        "{overview}"
    );
    let (_, ns) = api(
        &facade,
        "GET",
        "/api/v1/resources/namespaces",
        None,
        Value::Null,
    )
    .await;
    let lagns = ns["namespaces"]
        .as_array()
        .and_then(|a| a.iter().find(|n| n["namespace"] == "lagns"))
        .cloned()
        .unwrap_or_else(|| panic!("{ns}"));
    assert_eq!(lagns["queues"], 1, "{ns}");
    assert_eq!(lagns["partitions"], 4, "{ns}");
    assert_eq!(lagns["messages"]["pending"], 4, "{ns}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn the_backlog_chart_counts_a_filtered_message_once() {
    let (facade, _dir) = lag_scenario("lag-chart").await;
    let rows = facade.backlogs();
    assert!(
        !rows.iter().any(|(_, q, _, _)| q.contains('\u{1}')),
        "{rows:?}"
    );
    let source: Vec<_> = rows.iter().filter(|(_, q, _, _)| q == "lagsrc").collect();
    assert_eq!(source.len(), 1, "{rows:?}");
    assert_eq!((source[0].2, source[0].3), (4, 0), "{rows:?}");
    // The tenant's total is its queues' sum.
    let tenant = crate::config::DEFAULT_TENANT;
    let total = rows
        .iter()
        .find(|(t, q, _, _)| t == tenant && q.is_empty())
        .unwrap_or_else(|| panic!("{rows:?}"));
    let queues: i64 = rows
        .iter()
        .filter(|(t, q, _, _)| t == tenant && !q.is_empty())
        .map(|r| r.2)
        .sum();
    assert_eq!(total.2, queues, "{rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_filtered_groups_depth_is_what_its_where_took() {
    let (facade, _dir) = lag_scenario("lag-depth").await;
    let depth = |group: Option<&'static str>| {
        let facade = &facade;
        async move {
            let (status, v) = api(
                facade,
                "GET",
                "/api/v1/resources/queues/lagsrc/depth",
                group.map(|g| format!("group={g}")).as_deref(),
                Value::Null,
            )
            .await;
            assert_eq!(status, 200, "{v}");
            v
        }
    };
    let ga = depth(Some("ga")).await;
    assert_eq!(ga["queue"], "lagsrc", "{ga}");
    assert_eq!(ga["filtered"], true, "{ga}");
    assert_eq!(ga["pending"], 0, "{ga}");
    let names: Vec<&str> = ga["partitions"]
        .as_array()
        .map(|a| a.iter().filter_map(|p| p["partition"].as_str()).collect())
        .unwrap_or_default();
    assert_eq!(names, vec!["p1", "p2"], "{ga}");
    let gb = depth(Some("gb")).await;
    assert_eq!(gb["pending"], 2, "{gb}");
    assert_eq!(gb["partitionsPending"], 1, "{gb}");
    let all = depth(None).await;
    assert_eq!(all["pending"], 4, "{all}");
    assert_eq!(all["unread"], 2, "{all}");
    assert!(!shows_hidden(&ga) && !shows_hidden(&gb) && !shows_hidden(&all));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn the_queue_detail_shows_each_partitions_share() {
    let (facade, _dir) = lag_scenario("lag-detail").await;
    let (_, d) = api(
        &facade,
        "GET",
        "/api/v1/resources/queues/lagsrc",
        None,
        Value::Null,
    )
    .await;
    assert_eq!(d["totals"]["pending"], 4, "{d}");
    assert_eq!(d["totals"]["unread"], 2, "{d}");
    assert_eq!(d["totals"]["completed"], 4, "{d}");
    let part = |name: &str| {
        d["partitions"]
            .as_array()
            .and_then(|a| a.iter().find(|p| p["name"] == name))
            .cloned()
            .unwrap_or_else(|| panic!("{name}: {d}"))
    };
    for (name, pending, unread) in [("p1", 0, 0), ("p2", 0, 0), ("p3", 2, 0), ("p4", 2, 2)] {
        let p = part(name);
        assert_eq!(p["stats"]["pending"], pending, "{name}: {p}");
        assert_eq!(p["stats"]["unread"], unread, "{name}: {p}");
    }
    assert_eq!(part("p1")["cursor"]["totalConsumed"], 2, "{d}");
    assert_eq!(d["partitionCount"], 4, "{d}");
    // The partitions holding the most pending count it the same way, and
    // date it where something still has to read it.
    let (_, top) = api(
        &facade,
        "GET",
        "/api/v1/resources/partitions",
        Some("queue=lagsrc"),
        Value::Null,
    )
    .await;
    let row = |name: &str| {
        top["partitions"]
            .as_array()
            .and_then(|a| a.iter().find(|p| p["partition"] == name))
            .cloned()
            .unwrap_or_else(|| panic!("{name}: {top}"))
    };
    for (name, pending, unread) in [("p1", 0, 0), ("p3", 2, 0), ("p4", 2, 2)] {
        let r = row(name);
        assert_eq!(
            (r["pending"].clone(), r["unread"].clone()),
            (json!(pending), json!(unread)),
            "{r}"
        );
    }
    assert!(row("p3")["lagSeconds"].is_number(), "{top}");
    assert!(row("p1")["lagSeconds"].is_null(), "{top}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn group_lag_metrics_name_the_source_queue() {
    let (facade, _dir) = lag_scenario("lag-prom").await;
    let (rows, _) = facade.group_lags_for_test(usize::MAX);
    let rows = rows.expect("within the budget");
    assert!(
        !rows.iter().any(|l| l.queue.contains('\u{1}')),
        "{:?}",
        rows.iter()
            .map(|l| (&l.queue, &l.group))
            .collect::<Vec<_>>()
    );
    let of = |g: &str| {
        rows.iter()
            .find(|l| l.queue == "lagsrc" && l.group == g)
            .unwrap_or_else(|| panic!("no lag row for {g}"))
    };
    assert_eq!((of("ga").pending, of("ga").partitions_behind), (0, 0));
    assert_eq!((of("gb").pending, of("gb").partitions_behind), (2, 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn pop_and_ack_counters_name_the_source_queue() {
    crate::metrics::install_global(std::sync::Arc::new(crate::metrics::Metrics::new()));
    let (_facade, _dir) = lag_scenario("lag-counters").await;
    let snap = crate::metrics::global()
        .expect("installed")
        .per_queue
        .snapshot();
    assert!(
        !snap.keys().any(|k| k.contains('\u{1}')),
        "{:?}",
        snap.keys().collect::<Vec<_>>()
    );
    let key = crate::handlers::tenant_queue_key(crate::config::DEFAULT_TENANT, "lagsrc");
    let source = snap
        .get(&key)
        .unwrap_or_else(|| panic!("{:?}", snap.keys()));
    assert!(source.pop_count >= 4, "pops: {}", source.pop_count);
    assert!(source.ack_success >= 4, "acks: {}", source.ack_success);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_discovery_pop_reads_each_message_once() {
    let (facade, _dir) = lag_scenario("lag-discover").await;
    let mut seen = Vec::new();
    for _ in 0..20 {
        let out = facade
            .pop_discover(
                ctx(),
                crate::rsm::facade::PopDiscoverReq {
                    namespace: "lagns".into(),
                    task: String::new(),
                    group: Some("everything".into()),
                    batch: 100,
                    auto_ack: true,
                    wait: false,
                    timeout_ms: 1000,
                    options: group_options(None, "all"),
                },
            )
            .await
            .expect("discover");
        if out.empty {
            break;
        }
        let m = messages(&parse(&out.body));
        if m.is_empty() {
            break;
        }
        seen.extend(m);
    }
    let mut txns: Vec<String> = seen
        .iter()
        .map(|m| m["transactionId"].as_str().unwrap_or_default().to_string())
        .collect();
    txns.sort();
    let mut want: Vec<String> = (1..=4)
        .flat_map(|p| (0..2).map(move |i| format!("t{p}-{i}")))
        .collect();
    want.sort();
    assert_eq!(txns, want, "each message once, from the source");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn no_listing_or_walk_shows_a_hidden_queue() {
    let (facade, _dir) = lag_scenario("lag-walks").await;
    // Every place is checked, and every one that shows a hidden queue named.
    let mut wrong: Vec<String> = Vec::new();
    for (method, path, query, body) in [
        (
            "GET",
            "/api/v1/resources/queues",
            Some("stats=lanes"),
            Value::Null,
        ),
        ("GET", "/api/v1/resources/partitions", None, Value::Null),
        (
            "GET",
            "/api/v1/analytics/partition-liveness",
            None,
            Value::Null,
        ),
        ("GET", "/api/v1/status/queues", None, Value::Null),
        (
            "POST",
            "/api/v1/consumer-groups/positions",
            None,
            json!({"consumerGroup":"ga"}),
        ),
    ] {
        let (status, v) = api(&facade, method, path, query, body).await;
        if status != 200 || shows_hidden(&v) {
            wrong.push(format!("{path}: {status} {v}"));
        }
    }
    // The message browser lists a message once, at its source.
    let (_, msgs) = api(&facade, "GET", "/api/v1/messages", None, Value::Null).await;
    for txn in ["t1-0", "t3-1"] {
        if shows_hidden(&msgs) || txn_count(&msgs, txn) != 1 {
            wrong.push(format!(
                "/api/v1/messages: {txn} {} times",
                txn_count(&msgs, txn)
            ));
        }
    }
    // The shared state counts the queues a client can see.
    let (_, shared) = api(
        &facade,
        "GET",
        "/api/v1/system/shared-state",
        None,
        Value::Null,
    )
    .await;
    let (_, list) = api(
        &facade,
        "GET",
        "/api/v1/resources/queues",
        None,
        Value::Null,
    )
    .await;
    let visible = list["queues"].as_array().map_or(0, Vec::len);
    if shared["queues"] != visible {
        wrong.push(format!(
            "/api/v1/system/shared-state: {} queues, {visible} visible",
            shared["queues"]
        ));
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The queue list's `(pending, unread)` of `queue`, once `ok` holds for it
/// (a push's links and a pop's checkpoint apply just after their answers).
async fn settled(facade: &RaftFacade, queue: &str, ok: impl Fn(i64, i64) -> bool) -> (i64, i64) {
    let mut last = (0, 0);
    for _ in 0..300 {
        let row = listed(facade, queue).await;
        last = (
            row["messages"]["pending"].as_i64().unwrap_or(-1),
            row["messages"]["unread"].as_i64().unwrap_or(-1),
        );
        if ok(last.0, last.1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    last
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn unread_is_only_what_no_group_and_no_reader_can_take() {
    let (facade, _dir) = open("lag-unread").await;
    // Nobody reads warmup at all: every message is unread, and pending as
    // it always was.
    let row = listed(&facade, "warmup").await;
    assert!(
        row["messages"]["pending"].as_i64().unwrap_or(0) > 0,
        "{row}"
    );
    assert_eq!(
        row["messages"]["unread"], row["messages"]["pending"],
        "{row}"
    );

    // A content filter may take a message of any partition: none is unread,
    // and what it left out waits for nobody.
    configure(&facade, json!({"queue":"bykind","options":{}})).await;
    let first = pop(
        &facade,
        "bykind",
        Some("paid"),
        group_options(Some("data.kind = 'paid'"), "new"),
    )
    .await
    .unwrap();
    assert!(messages(&first).is_empty());
    push(
        &facade,
        json!([
            {"queue":"bykind","partition":"o-1","transactionId":"k1","payload":{"kind":"created"}},
            {"queue":"bykind","partition":"o-2","transactionId":"k2","payload":{"kind":"created"}},
            {"queue":"bykind","partition":"o-2","transactionId":"k3","payload":{"kind":"paid"}}
        ]),
    )
    .await;
    assert_eq!(
        settled(&facade, "bykind", |p, u| p == 1 && u == 0).await,
        (1, 0)
    );

    // A reading queue reads its source: what it may take is not unread there,
    // and its backlog is its own queue's; a partition its where rules out is.
    configure(&facade, json!({"queue":"rsrc","options":{}})).await;
    let (s, cfg) = configure(
        &facade,
        json!({"queue":"rsrc-p1","options":{"from":"rsrc","where":"partition = 'p1'"}}),
    )
    .await;
    assert_eq!(s, 200, "{cfg}");
    push(
        &facade,
        json!([
            {"queue":"rsrc","partition":"p1","transactionId":"r1","payload":{}},
            {"queue":"rsrc","partition":"p2","transactionId":"r2","payload":{}}
        ]),
    )
    .await;
    assert_eq!(
        settled(&facade, "rsrc", |p, u| p == 1 && u == 1).await,
        (1, 1)
    );
    assert_eq!(
        settled(&facade, "rsrc-p1", |p, u| p == 1 && u == 1).await,
        (1, 1)
    );

    // A group of the queue's own reads every partition: nothing is unread,
    // and its backlog counts with the filtered group's.
    configure(&facade, json!({"queue":"both","options":{}})).await;
    let first = pop(
        &facade,
        "both",
        Some("only-p1"),
        group_options(Some("partition = 'p1'"), "new"),
    )
    .await
    .unwrap();
    assert!(messages(&first).is_empty());
    let plain = pop(&facade, "both", Some("plain"), group_options(None, "new"))
        .await
        .unwrap();
    assert!(messages(&plain).is_empty());
    push(
        &facade,
        json!([
            {"queue":"both","partition":"p1","transactionId":"b1","payload":{}},
            {"queue":"both","partition":"p2","transactionId":"b2","payload":{}}
        ]),
    )
    .await;
    assert_eq!(
        settled(&facade, "both", |p, u| p == 2 && u == 0).await,
        (2, 0)
    );
}
