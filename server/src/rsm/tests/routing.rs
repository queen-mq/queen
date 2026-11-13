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
    assert!(!fed.to_string().contains('\u{1}'), "{fed}");
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
    // An ordinary queue cannot become a reader.
    configure(&facade, json!({"queue":"plain","options":{}})).await;
    let (s, body) = configure(&facade, json!({"queue":"plain","options":{"from":"src"}})).await;
    assert_ne!(s, 200, "{body}");
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
    assert!(!list.to_string().contains('\u{1}'), "{list}");
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
    assert!(!groups.to_string().contains('\u{1}'), "{groups}");
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
