//! Routing without copies (broker catalogue version 8), as it crosses the
//! socket: headers on every push and back on every read, a filtered group's
//! `where` on its pops, a reading queue's options, `link` in a transaction and
//! in a stream's cycle.
//!
//! These run against [`support::FakeBroker`] and need no stack. What is at
//! stake: the keys the broker reads (and nothing new on a request that uses
//! none of this), and that a broker older than links still gets what it
//! always got.

mod support;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use queen_mq::streams::{RunOptions, Stream};
use queen_mq::{
    BufferOptions, Config, LinkFrom, PushItem, Queen, QueueOptions, TxnPushItem, Where,
};
use serde_json::{json, Value};
use support::{FakeBroker, Hit, Reply};

fn client(broker: &FakeBroker, config: impl FnOnce(Config) -> Config) -> Queen {
    Queen::connect(config(Config::new(broker.url()).retry_attempts(1))).expect("fake broker url")
}

/// A push answer of `n` queued items, shaped as the broker renders it.
fn queued(n: usize) -> Reply {
    let items: Vec<String> = (0..n)
        .map(|i| {
            format!(
                r#"{{"index":{i},"message_id":"0190aaaa-0000-7000-8000-00000000000{i}","transaction_id":"t{i}","queueName":"orders","status":"queued","offset":{i}}}"#
            )
        })
        .collect();
    Reply::ok(format!("[{}]", items.join(",")))
}

fn hits_to(broker: &FakeBroker, method: &str, route: &str) -> Vec<Hit> {
    let mut hits = broker.hits();
    hits.retain(|h| h.method == method && h.route() == route);
    hits
}

fn pushes(broker: &FakeBroker) -> Vec<Hit> {
    hits_to(broker, "POST", "/api/v1/push")
}

fn transactions(broker: &FakeBroker) -> Vec<Hit> {
    hits_to(broker, "POST", "/api/v1/transaction")
}

fn headers(pairs: &[(&str, &str)]) -> Value {
    Value::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
            .collect(),
    )
}

/// A query value as the client percent-encodes it, decoded.
fn decoded(raw: &str) -> String {
    let b = raw.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            out.push(u8::from_str_radix(&raw[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

fn committed(results: &str) -> Reply {
    Reply::ok(format!(
        r#"{{"transactionId":"tx","success":true,"results":[{results}]}}"#
    ))
}

/// One pop answer: a message of partition `p1` (id 7) at offset 41, with headers.
const POP_WITH_HEADERS: &str = concat!(
    r#"{"success":true,"queue":"orders","partition":"p1","partitionId":"7","leaseId":"L1","consumerGroup":"billing","messages":["#,
    r#"{"id":"0198f0aa-0000-7000-8000-000000000001","transactionId":"t-1","traceId":null,"data":{"total":120},"producerSub":null,"#,
    r#""headers":{"traceparent":"00-ab-cd-01","type":"order.paid"},"#,
    r#""createdAt":"2026-11-12T10:00:00.000Z","partitionId":"7","partition":"p1","leaseId":"L1","consumerGroup":"billing","deliveryAttempt":1,"offset":41}"#,
    r#"],"partitionsClaimed":1}"#
);

// ===========================================================================
// Reading frames back
// ===========================================================================

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> &'a [u8] {
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        head
    }

    fn u16(&mut self) -> usize {
        u16::from_le_bytes(self.take(2).try_into().unwrap()) as usize
    }

    fn u32(&mut self) -> usize {
        u32::from_le_bytes(self.take(4).try_into().unwrap()) as usize
    }

    fn text16(&mut self) -> String {
        let n = self.u16();
        String::from_utf8(self.take(n).to_vec()).unwrap()
    }
}

/// One client-built frame: its flags, transaction id, headers and payload.
#[derive(Debug, PartialEq)]
struct Frame {
    flags: u8,
    txn: String,
    headers: Option<BTreeMap<String, String>>,
    payload: String,
}

/// The frames of every group of a framed body (after its magic and flags,
/// and a transaction body's head).
fn frames_of(groups: &[u8]) -> Vec<Frame> {
    let mut r = Reader(groups);
    let mut out = Vec::new();
    for _ in 0..r.u32() {
        let (_queue, _partition, _count) = (r.text16(), r.text16(), r.u32());
        let n = r.u32();
        let raw = zstd::stream::decode_all(r.take(n)).expect("one zstd frame");
        let mut f = Reader(&raw);
        while !f.0.is_empty() {
            let n = f.u32();
            let mut body = Reader(f.take(n));
            let flags = body.take(1)[0];
            let _id = body.take(16);
            let txn = body.text16();
            let headers = (flags & 8 != 0).then(|| {
                let len = body.u16();
                let mut block = Reader(body.take(len));
                let mut h = BTreeMap::new();
                for _ in 0..block.u16() {
                    let k = block.take(1)[0] as usize;
                    let name = String::from_utf8(block.take(k).to_vec()).unwrap();
                    h.insert(name, block.text16());
                }
                assert!(block.0.is_empty(), "bytes after the last header");
                h
            });
            out.push(Frame {
                flags,
                txn,
                headers,
                payload: String::from_utf8(body.0.to_vec()).unwrap(),
            });
        }
    }
    assert!(r.0.is_empty(), "bytes after the last group");
    out
}

fn btree(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

// ===========================================================================
// Headers on a push
// ===========================================================================

#[tokio::test]
async fn a_push_carries_the_builders_headers_on_every_message() {
    let broker = FakeBroker::start(vec![queued(2)]).await;
    client(&broker, |c| c)
        .queue("orders")
        .partition("order-1")
        .headers([("type", "order.paid")])
        .headers([("source", "web")])
        .push_many([json!({ "n": 1 }), json!({ "n": 2 })])
        .await
        .expect("push");

    let body = pushes(&broker).remove(0).json();
    for item in body["items"].as_array().unwrap() {
        assert_eq!(
            item["headers"],
            headers(&[("source", "web"), ("type", "order.paid")])
        );
    }
}

#[tokio::test]
async fn push_items_carry_their_own_headers_and_a_message_without_has_no_key() {
    let broker = FakeBroker::start(vec![queued(2)]).await;
    client(&broker, |c| c)
        .queue("orders")
        .headers([("ignored", "by push_items")])
        .push_items(vec![
            PushItem::new("orders", json!(1)).headers([("type", "a")]),
            PushItem::new("orders", json!(2)),
        ])
        .await
        .expect("push");

    let body = pushes(&broker).remove(0).json();
    assert_eq!(body["items"][0]["headers"], headers(&[("type", "a")]));
    assert!(
        body["items"][1].get("headers").is_none(),
        "a message without headers is the request it always was: {body}"
    );
}

#[tokio::test]
async fn a_header_name_the_broker_refuses_fails_before_anything_is_sent_or_buffered() {
    let broker = FakeBroker::start(vec![queued(1)]).await;
    let queen = client(&broker, |c| c);
    for name in ["Type", "queen-route", "", "with space"] {
        let err = queen
            .queue("orders")
            .headers([(name, "x")])
            .push(json!(1))
            .await
            .expect_err(name)
            .to_string();
        assert!(err.contains("header name"), "{name}: {err}");
    }
    let buffered = queen
        .queue("orders")
        .buffer(BufferOptions::default())
        .headers([("Bad", "x")])
        .push(json!(1))
        .await;
    assert!(buffered.is_err());
    assert_eq!(
        queen.buffer_stats().total_buffered_messages,
        0,
        "a message the broker would refuse is not buffered: its batch would be retried forever"
    );
    assert_eq!(broker.hit_count(), 0);
}

#[tokio::test]
async fn a_buffered_push_keeps_each_messages_headers() {
    let broker = FakeBroker::start(vec![queued(2)]).await;
    let queen = client(&broker, |c| c);
    let opts = BufferOptions {
        message_count: 100,
        time: Duration::from_secs(60),
        ..Default::default()
    };
    let lane = || queen.queue("orders").partition("p1").buffer(opts);
    lane()
        .headers([("type", "a")])
        .push(json!(1))
        .await
        .expect("buffered");
    lane().push(json!(2)).await.expect("buffered");
    lane()
        .headers([("type", "b")])
        .push(json!(3))
        .await
        .expect("buffered");
    lane().flush_buffer().await.expect("flush");

    let body = pushes(&broker).remove(0).json();
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 3, "one request for the three");
    assert_eq!(items[0]["headers"], headers(&[("type", "a")]));
    assert!(items[1].get("headers").is_none());
    assert_eq!(items[2]["headers"], headers(&[("type", "b")]));
}

#[tokio::test]
async fn the_framed_binary_push_carries_headers_in_each_frame() {
    let broker = FakeBroker::start(vec![queued(2)]).await;
    client(&broker, |c| c.binary_push(true).push_compression(true))
        .queue("orders")
        .push_items(vec![
            PushItem::new("orders", json!({ "a": 1 }))
                .partition("p1")
                .transaction_id("t-1")
                .headers([("type", "order.paid"), ("source", "web")]),
            PushItem::new("orders", json!({ "a": 2 }))
                .partition("p1")
                .transaction_id("t-2"),
        ])
        .await
        .expect("push");

    let hit = pushes(&broker).remove(0);
    assert!(hit.raw.starts_with(b"QPB1\x08"), "{:?}", &hit.raw[..8]);
    assert_eq!(
        frames_of(&hit.raw[5..]),
        vec![
            Frame {
                flags: 8,
                txn: "t-1".into(),
                headers: Some(btree(&[("source", "web"), ("type", "order.paid")])),
                payload: r#"{"a":1}"#.into(),
            },
            Frame {
                flags: 0,
                txn: "t-2".into(),
                headers: None,
                payload: r#"{"a":2}"#.into(),
            },
        ]
    );
}

/// The plain binary body has no place for headers: such a push goes as JSON,
/// and one without headers stays binary.
#[tokio::test]
async fn the_plain_binary_push_sends_json_when_a_message_has_headers() {
    let broker = FakeBroker::start(vec![queued(1)]).await;
    let queen = client(&broker, |c| c.binary_push(true));
    queen
        .queue("orders")
        .headers([("type", "a")])
        .push(json!(1))
        .await
        .expect("push");
    queen.queue("orders").push(json!(2)).await.expect("push");

    let hits = pushes(&broker);
    assert_eq!(hits[0].header("content-type"), Some("application/json"));
    assert_eq!(
        hits[0].json()["items"][0]["headers"],
        headers(&[("type", "a")])
    );
    assert!(hits[1].raw.starts_with(b"QPB1"));
}

// ===========================================================================
// Headers and link in a transaction
// ===========================================================================

#[tokio::test]
async fn a_transaction_push_carries_headers_as_json_and_as_frames() {
    let item = || {
        TxnPushItem::new("out", json!({ "n": 1 }))
            .partition("p1")
            .transaction_id("t-out")
            .headers([("type", "x")])
    };
    let broker = FakeBroker::start(vec![committed(
        r#"{"index":0,"type":"push","success":true}"#,
    )])
    .await;
    client(&broker, |c| c)
        .transaction()
        .push_item(item())
        .unwrap()
        .commit()
        .await
        .expect("commit");
    let body = transactions(&broker).remove(0).json();
    assert_eq!(
        body["operations"][0]["items"][0]["headers"],
        headers(&[("type", "x")])
    );

    let broker = FakeBroker::start(vec![committed(
        r#"{"index":0,"type":"push","success":true}"#,
    )])
    .await;
    client(&broker, |c| c.binary_push(true).push_compression(true))
        .transaction()
        .push_item(item())
        .unwrap()
        .commit()
        .await
        .expect("commit");
    let hit = transactions(&broker).remove(0);
    assert!(hit.raw.starts_with(b"QTB1"));
    let head_len = u32::from_le_bytes(hit.raw[5..9].try_into().unwrap()) as usize;
    let frames = frames_of(&hit.raw[9 + head_len..]);
    assert_eq!(frames.len(), 1);
    assert_eq!(
        (frames[0].flags, frames[0].headers.clone()),
        (8, Some(btree(&[("type", "x")])))
    );
}

/// The flagship: ack the message in hand and hand it on, without its payload.
#[tokio::test]
async fn link_names_a_popped_message_by_partition_id_offset_and_group() {
    let broker = FakeBroker::start_with(|_, hit| {
        if hit.route().starts_with("/api/v1/pop") {
            Reply::ok(POP_WITH_HEADERS)
        } else {
            committed(concat!(
                r#"{"index":0,"type":"ack","success":true,"transactionId":"t-1"},"#,
                r#"{"index":1,"type":"link","success":true,"duplicate":true,"transactionId":"t-1","queueName":"manual-review"}"#
            ))
        }
    })
    .await;
    let queen = client(&broker, |c| c);
    let msg = queen
        .queue("orders")
        .group("billing")
        .wait(false)
        .pop()
        .await
        .expect("pop")
        .remove(0);
    assert_eq!(msg.offset, Some(41));

    let res = queen
        .transaction()
        .ack(&msg)
        .link_with_headers(
            "manual-review",
            &msg.partition,
            [&msg],
            [("review-reason", "amount")],
        )
        .unwrap()
        .commit()
        .await
        .expect("commit");

    let body = transactions(&broker).remove(0).json();
    assert_eq!(body["operations"][0]["type"], "ack");
    assert_eq!(
        body["operations"][1],
        json!({
            "type": "link", "queue": "manual-review", "partition": "p1",
            "from": {"partitionId": "7", "offset": 41, "transactionId": "t-1", "consumerGroup": "billing"},
            "headers": {"review-reason": "amount"}
        })
    );
    assert_eq!(body["requiredLeases"], json!(["L1"]));
    // A target that already held it: a no-op that still commits.
    assert!(res.success);
    assert_eq!(res.results[1].op_type, "link");
    assert!(res.results[1].is_duplicate());
}

#[tokio::test]
async fn a_bundle_that_links_goes_as_json_even_with_frames_on() {
    let broker = FakeBroker::start(vec![committed(concat!(
        r#"{"index":0,"type":"push","success":true},"#,
        r#"{"index":1,"type":"link","success":true}"#
    ))])
    .await;
    client(&broker, |c| c.binary_push(true).push_compression(true))
        .transaction()
        .push_to("audit", "order-1", json!({ "n": 1 }))
        .unwrap()
        .link(
            "review",
            "order-1",
            [&LinkFrom::by_name("orders", "order-1", 7, "t-7")],
        )
        .unwrap()
        .commit()
        .await
        .expect("commit");

    let hit = transactions(&broker).remove(0);
    assert_eq!(hit.header("content-type"), Some("application/json"));
    let body = hit.json();
    assert_eq!(body["operations"][0]["type"], "push");
    assert_eq!(
        body["operations"][1]["from"],
        json!({"queue": "orders", "partition": "order-1", "offset": 7, "transactionId": "t-7"})
    );
}

// ===========================================================================
// Filtered groups
// ===========================================================================

/// An empty answer to a pop that sent a `where`: a 200 with a body that says
/// it filtered, never a bodiless 204.
const FILTERED_EMPTY: &str = concat!(
    r#"{"success":true,"queue":"orders","partition":"","partitionId":"","leaseId":"","#,
    r#""consumerGroup":"billing","messages":[],"partitionsClaimed":0,"filtered":true}"#
);

/// A broker before catalogue version 8 drops the `where` and answers as for an
/// unfiltered group: a 204 when nothing is there, the whole queue otherwise.
/// Either answer stops the pop, and a consumer, before a message is handed
/// over.
#[tokio::test]
async fn a_filtered_pop_answered_without_the_echo_is_an_error() {
    for answer in [Reply::status(204), Reply::ok(POP_WITH_HEADERS)] {
        let broker = FakeBroker::start(vec![answer]).await;
        let queen = client(&broker, |c| c);
        let filtered = queen
            .queue("orders")
            .group("billing")
            .where_("data.total", ">", json!(100))
            .wait(false);
        let err = filtered.pop().await.unwrap_err().to_string();
        assert!(err.contains("does not filter consumer groups"), "{err}");

        let handled = Arc::new(Mutex::new(0usize));
        let count = Arc::clone(&handled);
        let err = filtered
            .consume(move |_| {
                let count = Arc::clone(&count);
                async move {
                    *count.lock().unwrap() += 1;
                    Ok::<(), queen_mq::Error>(())
                }
            })
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not filter consumer groups"), "{err}");
        assert_eq!(
            *handled.lock().unwrap(),
            0,
            "no message reached the handler"
        );
    }
}

#[tokio::test]
async fn a_filtered_pop_answered_with_the_echo_hands_its_messages_over() {
    let answer = POP_WITH_HEADERS.replace(
        r#""partitionsClaimed":1}"#,
        r#""partitionsClaimed":1,"filtered":true}"#,
    );
    let broker = FakeBroker::start(vec![Reply::ok(answer)]).await;
    let got = client(&broker, |c| c)
        .queue("orders")
        .group("billing")
        .where_("data.total", ">", json!(100))
        .wait(false)
        .pop()
        .await
        .expect("a filtered answer");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].transaction_id, "t-1");
}

#[tokio::test]
async fn a_filtered_group_sends_its_where_on_every_pop() {
    let broker = FakeBroker::start(vec![Reply::ok(FILTERED_EMPTY)]).await;
    let queen = client(&broker, |c| c);
    let group = queen
        .queue("orders")
        .group("billing")
        .where_("data.kind", "in", json!(["order.paid", "order.refunded"]))
        .where_("headers.type", "exists", None)
        .wait(false);
    group.pop().await.expect("pop");
    group.pop().await.expect("pop");
    queen
        .queue("orders")
        .partition("p1")
        .group("audit")
        .where_str("data.total > 100")
        .where_str("headers.type prefix 'order.'")
        .wait(false)
        .pop()
        .await
        .expect("pop");

    let pops = hits_to(&broker, "GET", "/api/v1/pop/queue/orders");
    assert_eq!(pops.len(), 2);
    for hit in &pops {
        assert_eq!(
            decoded(hit.query("where").expect("the where travels on every pop")),
            r#"[{"field":"data.kind","op":"in","value":["order.paid","order.refunded"]},{"field":"headers.type","op":"exists"}]"#
        );
        assert_eq!(hit.query("consumerGroup"), Some("billing"));
    }
    let pinned = hits_to(&broker, "GET", "/api/v1/pop/queue/orders/partition/p1");
    assert_eq!(
        decoded(pinned[0].query("where").unwrap()),
        "data.total > 100 and headers.type prefix 'order.'"
    );
}

#[tokio::test]
async fn a_pop_without_a_where_sends_no_where_key() {
    let broker = FakeBroker::start(vec![Reply::status(204)]).await;
    client(&broker, |c| c)
        .queue("orders")
        .group("billing")
        .wait(false)
        .pop()
        .await
        .expect("pop");
    assert_eq!(broker.hits()[0].query("where"), None);
}

#[tokio::test]
async fn a_where_the_broker_would_refuse_is_refused_before_the_request() {
    let broker = FakeBroker::start(vec![Reply::status(204)]).await;
    let queen = client(&broker, |c| c);
    let no_group = queen
        .queue("orders")
        .where_("data.kind", "=", json!("x"))
        .wait(false);
    let err = no_group.pop().await.unwrap_err().to_string();
    assert!(err.contains("name one"), "{err}");
    let err = no_group
        .clone()
        .group("__QUEUE_MODE__")
        .pop()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("name one"), "{err}");
    // A consumer stops at once with the same error rather than polling.
    let err = no_group
        .consume(|_| async { Ok::<(), queen_mq::Error>(()) })
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("name one"), "{err}");

    let discovery = queen
        .queue_opt(None)
        .namespace("ns")
        .group("g")
        .where_str("data.kind = 'x'")
        .wait(false);
    assert!(discovery.pop().await.is_err());

    let mixed = queen
        .queue("orders")
        .group("g")
        .where_("data.a", "=", json!(1))
        .where_str("data.b = 2")
        .wait(false);
    assert!(mixed
        .pop()
        .await
        .unwrap_err()
        .to_string()
        .contains("not both"));

    let bad_op = queen
        .queue("orders")
        .group("g")
        .where_("data.a", "~", json!(1))
        .wait(false);
    assert!(bad_op
        .pop()
        .await
        .unwrap_err()
        .to_string()
        .contains("unknown op"));
    assert_eq!(broker.hit_count(), 0);
}

#[tokio::test]
async fn a_groups_where_changes_through_the_subscription_route() {
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"success":true,"consumerGroup":"billing","queueName":"orders","where":"set"}"#,
    )])
    .await;
    let admin = client(&broker, |c| c).admin();
    let out = admin
        .set_consumer_group_where("billing", "orders", "data.kind = 'order.paid'")
        .await
        .expect("set");
    assert_eq!(out["success"], true);
    admin
        .set_consumer_group_where(
            "billing",
            "orders",
            Where::new().and("data.total", ">", json!(100.0)),
        )
        .await
        .expect("set");

    let hits = hits_to(
        &broker,
        "POST",
        "/api/v1/consumer-groups/billing/subscription",
    );
    assert_eq!(
        hits[0].json(),
        json!({"queue": "orders", "where": "data.kind = 'order.paid'"})
    );
    assert_eq!(
        hits[1].json(),
        json!({"queue": "orders", "where": [{"field": "data.total", "op": ">", "value": 100}]})
    );
}

// ===========================================================================
// Reading queues
// ===========================================================================

#[tokio::test]
async fn a_reading_queue_is_configured_with_from_where_partition_by_and_start() {
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"configured":true,"queue":"orders-it","route":{"from":[{"queue":"orders"}],"partitionBy":"same"},"catchingUp":true}"#,
    )])
    .await;
    let out = client(&broker, |c| c)
        .queue("orders-it")
        .configure(QueueOptions {
            from: Some(json!("orders")),
            where_: Where::text("data.country = 'IT'").to_value().unwrap(),
            partition_by: Some("same".into()),
            start: Some("all".into()),
            lease_time: Some(30),
            retry_limit: Some(5),
            ..Default::default()
        })
        .await
        .expect("configure");

    assert_eq!(out["catchingUp"], true);
    assert_eq!(out["route"]["partitionBy"], "same");
    let body = hits_to(&broker, "POST", "/api/v1/configure")
        .remove(0)
        .json();
    assert_eq!(
        body,
        json!({"queue": "orders-it", "options": {
            "leaseTime": 30, "retryLimit": 5, "from": "orders",
            "where": "data.country = 'IT'", "partitionBy": "same", "start": "all"
        }}),
        "the keep-settings keys a reading queue refuses are not sent unless set"
    );
}

/// A source that reading queues read is deleted only by force.
#[tokio::test]
async fn a_source_is_deleted_by_force_only_when_asked() {
    let broker = FakeBroker::start(vec![
        Reply::ok(r#"{"deleted":true}"#),
        Reply::ok(r#"{"deleted":true}"#),
    ])
    .await;
    let queen = client(&broker, |c| c);
    queen.queue("orders").delete().await.expect("delete");
    queen
        .queue("orders")
        .delete_with(true)
        .await
        .expect("delete by force");
    let hits = hits_to(&broker, "DELETE", "/api/v1/resources/queues/orders");
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].query("force"), None);
    assert_eq!(hits[1].query("force"), Some("true"));
}

/// A broker that does not read other queues drops `from` and configures an
/// ordinary queue; its answer has no `route`, and that is an error. Stopping
/// to read (`from: null`) has no route to answer with.
#[tokio::test]
async fn a_reading_queue_answered_without_a_route_is_an_error() {
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"configured":true,"queue":"orders-it","options":{"leaseTime":300}}"#,
    )])
    .await;
    let queen = client(&broker, |c| c);
    let err = queen
        .queue("orders-it")
        .configure(QueueOptions {
            from: Some(json!(["orders", {"queue": "refunds", "where": "data.total > 0"}])),
            ..Default::default()
        })
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("does not read other queues"), "{err}");

    let stopped = queen
        .queue("orders-it")
        .configure(QueueOptions {
            from: Some(Value::Null),
            ..Default::default()
        })
        .await
        .expect("stop reading");
    assert_eq!(stopped["configured"], true);
    let bodies = hits_to(&broker, "POST", "/api/v1/configure");
    assert_eq!(bodies[1].json()["options"], json!({ "from": null }));
    // An ordinary configure is not checked.
    queen
        .queue("orders")
        .configure(QueueOptions::default())
        .await
        .expect("configure");
}

#[tokio::test]
async fn a_timer_carries_headers_to_the_message_it_fires() {
    let broker = FakeBroker::start(vec![Reply::ok(
        r#"{"results":[{"ok":true,"status":"scheduled","queue":"reminders","timerKey":"order-7","txn":"t-7"}]}"#,
    )])
    .await;
    let queen = client(&broker, |c| c);
    queen
        .timers()
        .schedule("reminders", "order-7", Duration::from_secs(60))
        .headers([("type", "reminder")])
        .txn("t-7")
        .payload_json(&json!({ "orderId": 7 }))
        .unwrap()
        .send()
        .await
        .expect("schedule");
    let body = hits_to(&broker, "POST", "/api/v1/timers").remove(0).json();
    assert_eq!(
        body["operations"][0]["headers"],
        headers(&[("type", "reminder")])
    );

    let err = queen
        .timers()
        .schedule("reminders", "order-8", Duration::from_secs(60))
        .headers([("queen-x", "1")])
        .payload(b"{}".to_vec())
        .operation()
        .unwrap_err()
        .to_string();
    assert!(err.contains("header name"), "{err}");
    assert_eq!(broker.hit_count(), 1);
}

// ===========================================================================
// Headers and offsets on the way back
// ===========================================================================

#[tokio::test]
async fn a_consumer_reads_each_messages_headers_and_offset() {
    let broker = FakeBroker::start_with(|_, hit| {
        if hit.route().starts_with("/api/v1/pop") {
            Reply::ok(POP_WITH_HEADERS)
        } else {
            Reply::ok(r#"[{"index":0,"transactionId":"t-1","success":true}]"#)
        }
    })
    .await;
    // (the `type` header, the offset) of each message the handler got.
    type Seen = Arc<Mutex<Vec<(Option<String>, Option<i64>)>>>;
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    client(&broker, |c| c)
        .queue("orders")
        .group("billing")
        .wait(false)
        .limit(1)
        .consume(move |msg| {
            let log = Arc::clone(&log);
            async move {
                let kind = msg.headers.as_ref().and_then(|h| h.get("type")).cloned();
                log.lock().unwrap().push((kind, msg.offset));
                Ok::<(), queen_mq::Error>(())
            }
        })
        .await
        .expect("consume");

    assert_eq!(
        *seen.lock().unwrap(),
        vec![(Some("order.paid".to_string()), Some(41))]
    );
}

fn text(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

/// A framed pop answer of one record at base offset 10 on partition 7, its
/// first frame with a headers block (flag 8), its second without.
fn framed_with_headers() -> Vec<u8> {
    let frame = |n: u8, txn: &str, headers: Option<&[(&str, &str)]>, payload: &str| {
        let mut body = vec![if headers.is_some() { 8 } else { 0 }];
        let mut id = [0u8; 16];
        (id[0], id[6], id[8]) = (n, 0x70, 0x80);
        body.extend_from_slice(&id);
        text(&mut body, txn);
        if let Some(pairs) = headers {
            let mut block = (pairs.len() as u16).to_le_bytes().to_vec();
            for (k, v) in pairs {
                block.push(k.len() as u8);
                block.extend_from_slice(k.as_bytes());
                text(&mut block, v);
            }
            body.extend_from_slice(&(block.len() as u16).to_le_bytes());
            body.extend_from_slice(&block);
        }
        body.extend_from_slice(payload.as_bytes());
        let mut f = (body.len() as u32).to_le_bytes().to_vec();
        f.extend_from_slice(&body);
        f
    };
    let raw = [
        frame(1, "t-10", Some(&[("type", "a")]), r#"{"n":10}"#),
        frame(2, "t-11", None, r#"{"n":11}"#),
    ]
    .concat();
    let own = r#"{"frames":true}"#;
    let mut b = b"QPF1\x01".to_vec();
    b.extend_from_slice(&(own.len() as u32).to_le_bytes());
    b.extend_from_slice(own.as_bytes());
    text(&mut b, "orders");
    text(&mut b, "lease-1");
    text(&mut b, "billing");
    b.extend_from_slice(&1u32.to_le_bytes()); // claims
    b.extend_from_slice(&7u64.to_le_bytes()); // partition id
    text(&mut b, "p7");
    b.extend_from_slice(&10u64.to_le_bytes()); // claim start
    b.extend_from_slice(&11u64.to_le_bytes()); // claim end
    b.extend_from_slice(&1u32.to_le_bytes()); // delivery attempt
    b.extend_from_slice(&1u32.to_le_bytes()); // records
    b.extend_from_slice(&10u64.to_le_bytes()); // base offset
    b.extend_from_slice(&1_700_000_000_000_000i64.to_le_bytes());
    b.extend_from_slice(&2u32.to_le_bytes()); // frames in the record
    b.push(0); // raw
    b.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    b.extend_from_slice(&raw);
    b
}

#[tokio::test]
async fn a_framed_pop_reads_headers_and_offsets_from_the_frames() {
    let broker = FakeBroker::start(vec![Reply::bytes(
        "application/x-queen-pop",
        framed_with_headers(),
    )])
    .await;
    let got = client(&broker, |c| c.framed_pop(true))
        .queue("orders")
        .group("billing")
        .wait(false)
        .pop()
        .await
        .expect("pop");

    assert_eq!(got.len(), 2);
    assert_eq!(got[0].headers, Some(btree(&[("type", "a")])));
    assert_eq!(got[0].data, json!({ "n": 10 }));
    assert_eq!((got[0].offset, got[1].offset), (Some(10), Some(11)));
    assert!(got[1].headers.is_none());
}

#[tokio::test]
async fn a_fetched_record_carries_its_headers() {
    let broker = FakeBroker::start(vec![Reply::ok(concat!(
        r#"{"entries":[{"queue":"orders","partition":"p1","records":["#,
        r#"{"headers":{"type":"order.paid"},"offset":0,"transactionId":"t-0","payload":{"n":0},"ts":"2026-11-12T10:00:00.000000Z"},"#,
        r#"{"offset":1,"transactionId":"t-1","payload":{"n":1},"ts":"2026-11-12T10:00:00.000000Z"}"#,
        r#"],"highWatermark":2,"logStartOffset":0}]}"#
    ))])
    .await;
    let read = client(&broker, |c| c)
        .queue("orders")
        .fetch(["p1"])
        .await
        .expect("fetch");
    let records = &read["p1"].records;
    assert_eq!(records[0].headers, Some(btree(&[("type", "order.paid")])));
    assert!(records[1].headers.is_none());
}

// ===========================================================================
// A stream over a queue: its cycle links what it lets through unchanged
// ===========================================================================

/// One message of partition `p1` (id 7) at offset 41, as a pop answers it.
const ONE_FOR_THE_STREAM: &str = concat!(
    r#"{"success":true,"queue":"src","partition":"p1","partitionId":"7","leaseId":"L1","consumerGroup":"streams.s","messages":["#,
    r#"{"id":"0198f0aa-0000-7000-8000-000000000001","transactionId":"t-1","traceId":null,"data":{"kind":"paid","n":1},"producerSub":null,"#,
    r#""createdAt":"2026-11-12T10:00:00.000Z","partitionId":"7","partition":"p1","leaseId":"L1","consumerGroup":"streams.s","deliveryAttempt":1,"offset":41}"#,
    r#"],"partitionsClaimed":1}"#
);

/// A fake broker for a stateless stream: registration, `pops` answers with
/// the message above and the rest empty, and the cycles answered by `cycle`
/// (the number of cycles seen so far, starting at 0).
async fn stream_broker(
    pops: usize,
    cycle: impl Fn(usize) -> Reply + Send + Sync + 'static,
) -> FakeBroker {
    let popped = Arc::new(Mutex::new(0usize));
    let cycles = Arc::new(Mutex::new(0usize));
    FakeBroker::start_with(move |_, hit| match hit.route() {
        "/streams/v1/queries" => Reply::ok(r#"{"success":true,"query_id":"q-1","fresh":true}"#),
        "/streams/v1/cycle" => {
            let mut n = cycles.lock().unwrap();
            *n += 1;
            cycle(*n - 1)
        }
        r if r.starts_with("/api/v1/pop") => {
            let mut n = popped.lock().unwrap();
            *n += 1;
            if *n <= pops {
                Reply::ok(ONE_FOR_THE_STREAM)
            } else {
                Reply::status(204)
            }
        }
        _ => Reply::status(404),
    })
    .await
}

async fn run_until_cycles(
    broker: &FakeBroker,
    stream: Stream,
    queen: &Queen,
    n: usize,
) -> Vec<Value> {
    let handle = stream
        .run(
            queen,
            RunOptions::new("s").max_wait(Duration::from_millis(50)),
        )
        .await
        .expect("run");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while hits_to(broker, "POST", "/streams/v1/cycle").len() < n
        && std::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    handle.stop().await.expect("stop");
    hits_to(broker, "POST", "/streams/v1/cycle")
        .iter()
        .map(|h| h.json())
        .collect()
}

#[tokio::test]
async fn a_stream_of_filters_links_its_input_messages_with_the_payload_kept() {
    let broker = stream_broker(1, |_| {
        Reply::ok(r#"{"success":true,"push_results":[{"status":"created"}]}"#)
    })
    .await;
    let queen = client(&broker, |c| c);
    let stream = Stream::from(queen.queue("src"))
        .filter(|r| r.text("kind") == Some("paid"))
        .to(queen.queue("out"));
    let cycles = run_until_cycles(&broker, stream, &queen, 1).await;

    assert_eq!(
        cycles[0]["push_items"],
        json!([{
            "queue": "out", "partition": "p1",
            "payload": {"kind": "paid", "n": 1},
            "link": {"offset": 41, "transactionId": "t-1"}
        }]),
        "a broker older than links reads the payload and pushes it"
    );
}

#[tokio::test]
async fn a_mapped_stream_pushes_and_link_false_pushes_too() {
    let broker = stream_broker(1, |_| Reply::ok(r#"{"success":true}"#)).await;
    let queen = client(&broker, |c| c);
    let mapped = Stream::from(queen.queue("src"))
        .map(|r| r.data.clone())
        .to(queen.queue("out"));
    let cycles = run_until_cycles(&broker, mapped, &queen, 1).await;
    assert!(cycles[0]["push_items"][0].get("link").is_none());

    let broker = stream_broker(1, |_| Reply::ok(r#"{"success":true}"#)).await;
    let queen = client(&broker, |c| c);
    let handle = Stream::from(queen.queue("src"))
        .to(queen.queue("out"))
        .run(
            &queen,
            RunOptions::new("s")
                .max_wait(Duration::from_millis(50))
                .link(false),
        )
        .await
        .expect("run");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while hits_to(&broker, "POST", "/streams/v1/cycle").is_empty()
        && std::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    handle.stop().await.expect("stop");
    let cycle = hits_to(&broker, "POST", "/streams/v1/cycle")
        .remove(0)
        .json();
    assert!(cycle["push_items"][0].get("link").is_none());
}

/// A cluster not yet at catalogue version 8 refuses the link: the same cycle
/// goes again as a push, and the stream pushes from then on.
#[tokio::test]
async fn a_stream_pushes_once_the_broker_refuses_a_link() {
    let broker = stream_broker(2, |n| {
        if n == 0 {
            Reply::json(
                400,
                r#"{"error":"headers, reading queues, filtered groups and link need every node on a broker that reads catalogue version 8","code":"cluster_version"}"#,
            )
        } else {
            Reply::ok(r#"{"success":true}"#)
        }
    })
    .await;
    let queen = client(&broker, |c| c);
    let stream = Stream::from(queen.queue("src")).to(queen.queue("out"));
    let cycles = run_until_cycles(&broker, stream, &queen, 3).await;

    assert!(cycles.len() >= 3, "{cycles:?}");
    assert!(cycles[0]["push_items"][0].get("link").is_some());
    let mut pushed = cycles[0]["push_items"].clone();
    pushed[0].as_object_mut().unwrap().remove("link");
    assert_eq!(
        cycles[1]["push_items"], pushed,
        "the refused cycle again, its outputs pushed"
    );
    assert!(
        cycles[2]["push_items"][0].get("link").is_none(),
        "and every cycle after it"
    );
}
