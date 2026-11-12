//! Feed-stream runners against the conformance kit's fake broker (node):
//! commits and ids, the stored position, poison messages, one runner per
//! queryId, routing, windows and patterns that close on a quiet feed,
//! restarts and resets, commits that go wrong. The behaviours of the JS
//! feed-unit suite (feed.test.js, feedStreams.test.js), on this SDK.

mod feed_support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use feed_support::*;
use queen_mq::feed::FeedFrom;
use queen_mq::streams::feed::{
    feed_output_id, output_prefix, FeedAggregate, FeedRunOptions, FeedSink, FeedSource, FeedStream,
    Join, Pattern, Poison, Reset,
};
use queen_mq::streams::Stream;
use queen_mq::Queen;
use serde_json::{json, Value};

fn earliest(q: &Queen) -> FeedStream {
    Stream::from_feed(q, FeedSource::new().from(FeedFrom::Earliest))
}

fn earliest_of(q: &Queen, queues: &[&str]) -> FeedStream {
    Stream::from_feed(
        q,
        FeedSource::new()
            .from(FeedFrom::Earliest)
            .queues(queues.iter().copied()),
    )
}

fn errors_into(
    sink: &Arc<Mutex<Vec<Value>>>,
) -> impl Fn(&queen_mq::streams::feed::FeedErrorInfo) + Send + Sync + 'static {
    let sink = Arc::clone(sink);
    move |info| {
        let mut c = info.ctx.clone();
        c["message"] = json!(info.message);
        c["code"] = json!(info.code);
        sink.lock().unwrap().push(c);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pushes_the_outputs_and_the_new_position_in_one_transaction_with_ids_made_from_the_entry() {
    let b = Fake::start();
    let p = b.proc();
    b.tx(&[
        ("orders", "c1", json!({ "n": 1, "country": "IT" })),
        ("orders", "c1", json!({ "n": 2, "country": "IT" })),
    ])
    .await;
    b.push("orders", "c1", json!({ "n": 3, "country": "IT" }))
        .await;
    b.push("orders", "c2", json!({ "n": 4, "country": "FR" }))
        .await;
    let s = Stream::from_feed(
        &p.queen,
        FeedSource::new()
            .queues(["orders"])
            .from(FeedFrom::Earliest),
    )
    .where_("data.country", "=", json!("IT"))
    .filter(|m, _| m["data"]["n"] != 2)
    .map(|m, _| json!({ "order": m["data"]["n"] }))
    .to("orders-it");
    let r = p.run(s, fast("orders-it")).await.unwrap();
    until(3000, "the outputs", || async {
        b.messages("orders-it").await.len() >= 2
    })
    .await;
    r.stop().await;
    let out = b.messages("orders-it").await;
    assert_eq!(
        out.iter().map(|e| e["payload"].clone()).collect::<Vec<_>>(),
        [json!({ "order": 1 }), json!({ "order": 3 })]
    );
    assert_eq!(out[0]["partition"], "c1", "the source partition by default");
    assert_eq!(out[0]["position"], out[1]["position"], "one commit");
    let src = &b.messages("orders").await[0];
    let entry = json!({ "position": src["position"], "queue": src["queue"], "partition": src["partition"], "offset": src["offset"] });
    assert_eq!(
        out[0]["transactionId"],
        json!(feed_output_id("orders-it", &entry, 0, 0, "orders-it"))
    );
    assert!(out[0]["transactionId"]
        .as_str()
        .unwrap()
        .starts_with(&output_prefix("orders-it")));
    let rec = b.record("orders-it").await.unwrap();
    // The fake broker gives a position to the transactions that push only: the commit's own is 4.
    assert!(rec["position"].as_u64().unwrap() >= 4);
    assert_eq!(rec["queryId"], "orders-it");
    assert_eq!(rec["epoch"], 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stored_position_is_where_it_starts() {
    let b = Fake::start();
    let p = b.proc();
    for n in 1..=3 {
        b.push("orders", "c1", json!({ "n": n })).await;
    }
    b.kv_put(NS, "copy", json!({ "position": 3, "queryId": "copy" }))
        .await;
    let r = p
        .run(
            Stream::from_feed(&p.queen, FeedSource::new()).to("copy"),
            fast("copy"),
        )
        .await
        .unwrap();
    until(3000, "the first copy", || async {
        !b.messages("copy").await.is_empty()
    })
    .await;
    b.push("orders", "c1", json!({ "n": 4 })).await;
    until(3000, "the second copy", || async {
        b.messages("copy").await.len() >= 2
    })
    .await;
    r.stop().await;
    let ns: Vec<Value> = b
        .payloads("copy")
        .await
        .iter()
        .map(|p| p["n"].clone())
        .collect();
    assert_eq!(ns, [json!(3), json!(4)], "from position 3 on");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_without_the_feed_fails_run_with_the_reason() {
    // A broker that answers 404 to everything: the fake broker of the HTTP tests.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 65536];
                let _ = sock.read(&mut buf).await;
                let body = r#"{"error":"not found"}"#;
                let resp = format!(
                    "HTTP/1.1 404 Not Found\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    let q = Queen::connect_to(format!("http://127.0.0.1:{port}")).unwrap();
    let err = Stream::from_feed(&q, FeedSource::new())
        .to("copy")
        .run(FeedRunOptions::new("old-broker"))
        .await
        .unwrap_err();
    assert_eq!(err.code().map(|c| c.as_str()), Some("no_feed_route"));
    assert!(err.to_string().contains("no POST /api/v1/feed"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cycle_error_is_reported_and_retried_and_the_retry_writes_once() {
    let b = Fake::start();
    let p = b.proc();
    b.push("orders", "c1", json!({ "n": 1 })).await;
    b.inject("/api/v1/transaction", "before", 503, 1).await;
    let errors = Arc::new(Mutex::new(Vec::new()));
    let r = p
        .run(
            earliest(&p.queen).to("copy"),
            fast("loud").on_error(errors_into(&errors)),
        )
        .await
        .unwrap();
    until(5000, "the copy", || async {
        !b.messages("copy").await.is_empty()
    })
    .await;
    r.stop().await;
    let errs = errors.lock().unwrap().clone();
    assert!(
        errs.iter()
            .any(|e| e["phase"] == "cycle" && e["message"].as_str().unwrap_or("").contains("503")),
        "{errs:?}"
    );
    assert_eq!(b.messages("copy").await.len(), 1, "the retry wrote it once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_a_chain_it_cannot_run() {
    let b = Fake::start();
    let p = b.proc();
    let e = p
        .run(
            Stream::from_feed(&p.queen, FeedSource::new()).map(|m, _| m.clone()),
            fast("nosink"),
        )
        .await
        .unwrap_err();
    assert!(e.to_string().contains("end the chain"), "{e}");
    let e = p
        .run(
            Stream::from_feed(&p.queen, FeedSource::new()).to("x"),
            fast("a#b"),
        )
        .await
        .unwrap_err();
    assert!(e.to_string().contains("without '#'"), "{e}");
    let e = p
        .run(
            Stream::from_feed(&p.queen, FeedSource::new()).to("x"),
            fast("p").on_poison(Poison::DeadLetter(String::new())),
        )
        .await
        .unwrap_err();
    assert!(
        e.to_string()
            .contains("onPoison is 'stop', 'skip' or a queue"),
        "{e}"
    );
}

// ---------------------------------------------------------------- poison

async fn orders(b: &Fake, n: i64) {
    for i in 1..=n {
        b.push("orders", &format!("c{i}"), json!({ "n": i })).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stops_at_a_message_that_keeps_failing_with_what_came_before_committed() {
    let b = Fake::start();
    let p = b.proc();
    orders(&b, 5).await;
    let calls = Arc::new(Mutex::new(0));
    let c2 = Arc::clone(&calls);
    let errors = Arc::new(Mutex::new(Vec::new()));
    let s = earliest(&p.queen)
        .map(move |m, _| -> Result<Value, String> {
            if m["data"]["n"] == 3 {
                *c2.lock().unwrap() += 1;
                return Err("bad order".into());
            }
            Ok(json!({ "n": m["data"]["n"] }))
        })
        .to("out");
    let r = p
        .run(
            s,
            fast("stop").max_records(2).on_error(errors_into(&errors)),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), r.done())
        .await
        .expect("the runner stopped on its own");
    assert_eq!(
        b.payloads("out").await,
        [json!({ "n": 1 }), json!({ "n": 2 })]
    );
    assert_eq!(*calls.lock().unwrap(), 4, "the first try and 3 retries");
    let third = &b.messages("orders").await[2];
    let rec = b.record("stop").await.unwrap();
    assert_eq!(
        rec["position"], third["position"],
        "the stored position is the message"
    );
    assert_eq!(rec["stopped"]["reason"], "poison");
    assert_eq!(rec["stopped"]["error"], "bad order");
    assert_eq!(rec["stopped"]["attempts"], 4);
    assert_eq!(rec["stopped"]["partition"], "c3");
    assert_eq!(rec["stopped"]["queue"], "orders");
    assert_eq!(rec["stopped"]["position"], third["position"]);
    assert_eq!(rec["stopped"]["offset"], third["offset"]);
    assert!(
        queen_mq::streams::feed::js_parse_date(rec["stopped"]["at"].as_str().unwrap())
            .is_some_and(|t| t > 0.0)
    );
    let metrics = r.metrics();
    assert_eq!(metrics["role"], "stopped");
    assert_eq!(metrics["stopped"]["reason"], "poison");
    assert_eq!(metrics["stopped"]["position"], third["position"]);
    let errs = errors.lock().unwrap().clone();
    let stop = errs
        .iter()
        .find(|e| e["action"] == "stopped")
        .expect("onError hears it");
    let msg = stop["message"].as_str().unwrap();
    assert!(
        msg.contains("(orders/c3, offset 1) failed 4 times: bad order"),
        "{msg}"
    );
    assert_eq!(stop["code"], "feed_poison");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skip_leaves_the_message_out_and_a_dead_letter_queue_gets_it_with_the_error() {
    let b = Fake::start();
    let p = b.proc();
    orders(&b, 5).await;
    let fails_on = |n: i64| {
        move |m: &Value, _: Option<&Value>| -> Result<Value, String> {
            if m["data"]["n"] == n {
                return Err("bad order".into());
            }
            Ok(json!({ "n": m["data"]["n"] }))
        }
    };
    let errs = Arc::new(Mutex::new(Vec::new()));
    let r = p
        .run(
            earliest(&p.queen).map(fails_on(3)).to("out"),
            fast("skip")
                .on_poison(Poison::Skip)
                .on_error(errors_into(&errs)),
        )
        .await
        .unwrap();
    until(3000, "four outputs", || async {
        b.messages("out").await.len() >= 4
    })
    .await;
    r.stop().await;
    let ns: Vec<Value> = b
        .payloads("out")
        .await
        .iter()
        .map(|p| p["n"].clone())
        .collect();
    assert_eq!(ns, [json!(1), json!(2), json!(4), json!(5)]);
    assert_eq!(m(&r, "skipped"), 1.0, "{:?}", errs.lock().unwrap());
    assert_eq!(m(&r, "poisoned"), 1.0);

    let r = p
        .run(
            earliest_of(&p.queen, &["orders"])
                .map(fails_on(2))
                .to("out2"),
            fast("dlq")
                .on_poison(Poison::DeadLetter("out-dlq".into()))
                .retries(1)
                .on_error(|_| {}),
        )
        .await
        .unwrap();
    until(3000, "the outputs", || async {
        b.messages("out2").await.len() >= 4
    })
    .await;
    r.stop().await;
    let dead = &b.messages("out-dlq").await[0];
    assert_eq!(dead["partition"], "c2", "in the partition it came from");
    assert_eq!(dead["payload"]["entry"]["data"], json!({ "n": 2 }));
    assert_eq!(dead["payload"]["entry"]["queue"], "orders");
    assert_eq!(dead["payload"]["error"]["message"], "bad order");
    assert_eq!(dead["payload"]["attempts"], 2);
    assert_eq!(dead["payload"]["queryId"], "dlq");
    assert!(
        dead["transactionId"]
            .as_str()
            .unwrap()
            .starts_with(&output_prefix("dlq")),
        "the query never reads it back"
    );
    assert_eq!(
        dead["position"],
        b.messages("out2").await[0]["position"],
        "one commit with the outputs"
    );
    assert_eq!(m(&r, "deadLettered"), 1.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_function_that_fails_then_passes_is_not_poison_and_a_retry_starts_from_the_state_before()
{
    let b = Fake::start();
    let p = b.proc();
    orders(&b, 3).await;
    let tries = Arc::new(Mutex::new(0));
    let t2 = Arc::clone(&tries);
    let s = earliest(&p.queen)
        .map(move |m, _| -> Result<Value, String> {
            if m["data"]["n"] == 2 {
                let mut t = t2.lock().unwrap();
                *t += 1;
                if *t < 3 {
                    return Err("flaky".into());
                }
            }
            Ok(json!({ "n": m["data"]["n"] }))
        })
        .to("out");
    let r = p.run(s, fast("flaky")).await.unwrap();
    until(3000, "three outputs", || async {
        b.messages("out").await.len() >= 3
    })
    .await;
    r.stop().await;
    assert_eq!(
        b.payloads("out")
            .await
            .iter()
            .map(|p| p["n"].clone())
            .collect::<Vec<_>>(),
        [json!(1), json!(2), json!(3)]
    );
    assert_eq!(m(&r, "retries"), 2.0);
    assert_eq!(m(&r, "poisoned"), 0.0);

    // A window's fold that fails once leaves nothing behind.
    b.at(1000.0).await;
    for i in 1..=3 {
        b.push("sums-in", "c1", json!({ "n": i })).await;
    }
    let failed = Arc::new(Mutex::new(false));
    let f2 = Arc::clone(&failed);
    let s = earliest_of(&p.queen, &["sums-in", "other"])
        .window_tumbling(60.0)
        .reduce(
            Some(json!({ "sum": 0, "count": 0 })),
            move |acc, v| -> Result<Value, String> {
                let mut acc = acc.unwrap();
                // The message on "other" only moves the clock past the window. It
                // lands in the next window, and it must fold there: a fold that
                // panicked on it made the window's result wait on timing.
                let Some(n) = v["n"].as_i64() else {
                    return Ok(acc);
                };
                acc["sum"] = json!(acc["sum"].as_i64().unwrap() + n);
                acc["count"] = json!(acc["count"].as_i64().unwrap() + 1);
                let mut f = f2.lock().unwrap();
                if v["n"] == 2 && !*f {
                    *f = true;
                    return Err("once".into());
                }
                Ok(acc)
            },
        )
        .to("sums");
    let r = p.run(s, fast("undo").now(b.clock())).await.unwrap();
    until(3000, "three read", || async { m(&r, "entriesRead") >= 3.0 }).await;
    b.push_at(61_000.0, "other", "x", json!({})).await;
    until(3000, "the sum", || async {
        !b.messages("sums").await.is_empty()
    })
    .await;
    r.stop().await;
    assert_eq!(
        b.payloads("sums").await,
        [json!({ "sum": 6, "count": 3 })],
        "the failed try left nothing behind"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_window_result_whose_foreach_keeps_failing_is_skipped_and_the_stream_goes_on() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    b.push("orders", "bad", json!({ "n": 1 })).await;
    b.push("orders", "good", json!({ "n": 2 })).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s2 = Arc::clone(&seen);
    let s = earliest_of(&p.queen, &["orders"])
        .window_tumbling(60.0)
        .aggregate(FeedAggregate::new().count("count"))
        .foreach(move |value: Option<Value>, ctx: Value| {
            let s2 = Arc::clone(&s2);
            async move {
                if ctx["key"] == "bad" {
                    return Err("downstream refused".to_string());
                }
                s2.lock()
                    .unwrap()
                    .push(json!({ "key": ctx["key"], "count": value.unwrap()["count"] }));
                Ok(())
            }
        });
    let r = p
        .run(
            s,
            fast("emit-skip")
                .now(b.clock())
                .on_poison(Poison::Skip)
                .on_error(|_| {}),
        )
        .await
        .unwrap();
    until(3000, "two read", || async { m(&r, "entriesRead") >= 2.0 }).await;
    b.push_at(61_000.0, "orders", "good", json!({ "n": 3 }))
        .await;
    until(3000, "the good window and the skip", || async {
        !seen.lock().unwrap().is_empty() && m(&r, "skipped") >= 1.0
    })
    .await;
    r.stop().await;
    assert_eq!(
        *seen.lock().unwrap(),
        [json!({ "key": "good", "count": 1 })]
    );
}

// ------------------------------------------------------- one runner per id

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_process_waits_and_each_message_runs_foreach_once() {
    let b = Fake::start();
    let (pa, pb) = (b.proc(), b.proc());
    for i in 1..=40 {
        b.push("orders", &format!("c{}", i % 5), json!({ "n": i }))
            .await;
    }
    let calls = Arc::new(Mutex::new(Vec::new()));
    let effect = |q: &Queen, who: &'static str| {
        let calls = Arc::clone(&calls);
        earliest(q).foreach(move |v: Option<Value>, _| {
            let calls = Arc::clone(&calls);
            async move {
                calls.lock().unwrap().push((who, v.unwrap()["n"].clone()));
            }
        })
    };
    let ra = pa
        .run(effect(&pa.queen, "a"), fast("fx").max_records(7))
        .await
        .unwrap();
    let rb = pb
        .run(effect(&pb.queen, "b"), fast("fx").max_records(7))
        .await
        .unwrap();
    until(5000, "40 calls", || async {
        calls.lock().unwrap().len() >= 40
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(role(&ra), "active");
    assert_eq!(role(&rb), "standby");
    let c = calls.lock().unwrap().clone();
    assert_eq!(c.len(), 40);
    assert!(c.iter().all(|(w, _)| *w == "a"));
    ra.stop().await;
    rb.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn when_the_active_one_stops_or_dies_the_standby_takes_over_from_the_stored_position() {
    let b = Fake::start();
    let (pa, pb) = (b.proc(), b.proc());
    for i in 1..=5 {
        b.push("orders", "c1", json!({ "n": i })).await;
    }
    let copy = |q: &Queen| earliest_of(q, &["orders"]).to("copy");
    let ra = pa
        .run(copy(&pa.queen), fast("copy").lock_ttl("2s"))
        .await
        .unwrap();
    let rb = pb
        .run(copy(&pb.queen), fast("copy").lock_ttl("2s"))
        .await
        .unwrap();
    until(3000, "five copies", || async {
        b.messages("copy").await.len() >= 5
    })
    .await;
    ra.stop().await;
    for i in 6..=8 {
        b.push("orders", "c1", json!({ "n": i })).await;
    }
    until(6000, "the standby to copy 6..8", || async {
        b.messages("copy").await.len() >= 8
    })
    .await;
    rb.stop().await;
    let ns: Vec<Value> = b
        .payloads("copy")
        .await
        .iter()
        .map(|p| p["n"].clone())
        .collect();
    assert_eq!(
        ns,
        (1..=8).map(|i| json!(i)).collect::<Vec<_>>(),
        "each once, in order"
    );
    assert_eq!(m(&rb, "takeovers"), 1.0);

    // A process that dies (its lock expires): the other one takes over.
    let b = Fake::start();
    let (mut pa, pb) = (b.proc(), b.proc());
    b.push("orders", "c1", json!({ "n": 1 })).await;
    let ra = pa
        .run(copy(&pa.queen), fast("cut").lock_ttl("1s").on_error(|_| {}))
        .await
        .unwrap();
    let rb = pb
        .run(copy(&pb.queen), fast("cut").lock_ttl("1s").on_error(|_| {}))
        .await
        .unwrap();
    until(3000, "the first copy", || async {
        !b.messages("copy").await.is_empty()
    })
    .await;
    let commits = m(&ra, "commits");
    pa.crash();
    b.push("orders", "c1", json!({ "n": 2 })).await;
    until(8000, "b to take over", || async {
        b.messages("copy").await.len() >= 2
    })
    .await;
    b.push("orders", "c1", json!({ "n": 3 })).await;
    until(4000, "the third copy", || async {
        b.messages("copy").await.len() >= 3
    })
    .await;
    assert_eq!(role(&rb), "active");
    assert_eq!(
        m(&ra, "commits"),
        commits,
        "the dead process committed nothing more"
    );
    let ns: Vec<Value> = b
        .payloads("copy")
        .await
        .iter()
        .map(|p| p["n"].clone())
        .collect();
    assert_eq!(ns, [json!(1), json!(2), json!(3)]);
    rb.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exclusive_false_runs_both_and_the_position_still_decides_who_commits() {
    let b = Fake::start();
    let (pa, pb) = (b.proc(), b.proc());
    for i in 1..=30 {
        b.push("orders", &format!("c{}", i % 3), json!({ "n": i }))
            .await;
    }
    let copy = |q: &Queen| earliest_of(q, &["orders"]).to("copy");
    let ra = pa
        .run(
            copy(&pa.queen),
            fast("both").exclusive(false).max_records(4),
        )
        .await
        .unwrap();
    let rb = pb
        .run(
            copy(&pb.queen),
            fast("both").exclusive(false).max_records(4),
        )
        .await
        .unwrap();
    until(8000, "30 copies", || async {
        b.messages("copy").await.len() >= 30
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    ra.stop().await;
    rb.stop().await;
    let ns: Vec<i64> = b
        .payloads("copy")
        .await
        .iter()
        .map(|p| p["n"].as_i64().unwrap())
        .collect();
    assert_eq!(ns.len(), 30);
    let set: std::collections::HashSet<i64> = ns.into_iter().collect();
    assert_eq!(set.len(), 30, "no message twice");
    assert_eq!(role(&ra), "stopped");
}

// ----------------------------------------------------------------- routing

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_function_picks_the_queue_and_a_list_sends_each_record_to_every_queue() {
    let b = Fake::start();
    let p = b.proc();
    b.push(
        "orders",
        "o1",
        json!({ "id": 1, "country": "IT", "total": 500 }),
    )
    .await;
    b.push(
        "orders",
        "o2",
        json!({ "id": 2, "country": "FR", "total": 5 }),
    )
    .await;
    b.push(
        "orders",
        "o3",
        json!({ "id": 3, "country": "IT", "total": 7 }),
    )
    .await;
    b.push("orders", "o4", json!({ "id": 4 })).await;
    let router = earliest_of(&p.queen, &["orders*"]).to(FeedSink::route(
        |v: Option<Value>, _m: Value| async move {
            let v = v.unwrap();
            match v.get("country") {
                Some(c) => json!(format!("orders-{}", c.as_str().unwrap())),
                None => Value::Null,
            }
        },
    ));
    let r1 = p.run(router, fast("router")).await.unwrap();
    let all = p
        .run(
            earliest_of(&p.queen, &["orders"]).to(["audit", "bi"]),
            fast("fanout"),
        )
        .await
        .unwrap();
    let picked = earliest_of(&p.queen, &["orders"]).to(FeedSink::route(|v: Option<Value>, _m: Value| async move {
        let v = v.unwrap();
        let big = v["total"].as_i64().unwrap_or(0) > 100;
        json!([{ "queue": "by-size", "partition": if big { "big" } else { "small" } }, if big { json!("review") } else { Value::Null }])
    }));
    let r3 = p.run(picked, fast("picker")).await.unwrap();
    until(5000, "the outputs", || async {
        b.messages("orders-IT").await.len() >= 2
            && !b.messages("orders-FR").await.is_empty()
            && b.messages("bi").await.len() >= 4
            && b.messages("by-size").await.len() >= 4
            && !b.messages("review").await.is_empty()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    r1.stop().await;
    all.stop().await;
    r3.stop().await;
    let ids = |v: Vec<Value>| v.iter().map(|p| p["id"].clone()).collect::<Vec<_>>();
    assert_eq!(ids(b.payloads("orders-IT").await), [json!(1), json!(3)]);
    assert_eq!(ids(b.payloads("orders-FR").await), [json!(2)]);
    assert_eq!(
        m(&r1, "entriesRead"),
        4.0,
        "orders* matches its own outputs, and it never reads them back"
    );
    assert_eq!(
        ids(b.payloads("audit").await),
        [json!(1), json!(2), json!(3), json!(4)]
    );
    assert_eq!(
        ids(b.payloads("bi").await),
        [json!(1), json!(2), json!(3), json!(4)]
    );
    let sizes: Vec<(Value, Value)> = b
        .messages("by-size")
        .await
        .iter()
        .map(|e| (e["payload"]["id"].clone(), e["partition"].clone()))
        .collect();
    assert_eq!(sizes[0], (json!(1), json!("big")));
    assert_eq!(sizes[1], (json!(2), json!("small")));
    assert_eq!(ids(b.payloads("review").await), [json!(1)]);
    assert_eq!(
        b.messages("review").await[0]["partition"],
        "o1",
        "the source partition by default"
    );
}

// ---------------------------------------------------- the feed's clock moves

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_window_on_a_quiet_feed_closes_on_time_the_runner_moves_the_broker_clock() {
    let b = Fake::start();
    let p = b.proc();
    b.push_at(5000.0, "orders", "c1", json!({})).await;
    b.push_at(6000.0, "orders", "c1", json!({})).await;
    let s = earliest(&p.queen)
        .window_tumbling(60.0)
        .aggregate(FeedAggregate::new().count("count"))
        .to("counts");
    let r = p.run(s, fast("quiet").now(b.clock())).await.unwrap();
    until(3000, "two read", || async { m(&r, "entriesRead") >= 2.0 }).await;
    // Nothing more is written: only time passes.
    b.at(59_500.0).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(b.messages("counts").await.is_empty(), "not before its end");
    b.at(60_000.0).await;
    until(
        3000,
        "the window to close with no message after it",
        || async { !b.messages("counts").await.is_empty() },
    )
    .await;
    r.stop().await;
    assert_eq!(b.payloads("counts").await, [json!({ "count": 2 })]);
    // (This fake broker moves its own clock with the control route, so no tick
    // is needed here; the live tests check the tick on a real broker.)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pattern_on_a_quiet_feed_sends_the_reminder_at_the_deadline() {
    let b = Fake::start();
    let p = b.proc();
    b.push_at(0.0, "orders", "order-2", json!({ "id": 2 }))
        .await;
    let s = earliest_of(&p.queen, &["orders", "payments"])
        .pattern(
            Pattern::new(
                |m| m["queue"] == "orders",
                |m| m["queue"] == "payments",
                "15m",
            )
            .on_match(|o, p| json!({ "order": o["data"]["id"], "paid": p["data"]["amount"] }))
            .on_timeout(|o| json!({ "order": o["data"]["id"], "remind": true })),
        )
        .to("followups");
    let r = p.run(s, fast("remind").now(b.clock())).await.unwrap();
    until(3000, "one read", || async { m(&r, "entriesRead") >= 1.0 }).await;
    b.at(900_000.0).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        b.messages("followups").await.is_empty(),
        "not at 15 minutes yet"
    );
    b.at(901_000.0).await;
    until(3000, "the reminder", || async {
        !b.messages("followups").await.is_empty()
    })
    .await;
    b.push_at(950_000.0, "payments", "order-2", json!({ "amount": 9 }))
        .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    r.stop().await;
    assert_eq!(
        b.payloads("followups").await,
        [json!({ "order": 2, "remind": true })]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_carries_the_open_windows_on_without_counting_anything_twice() {
    let b = Fake::start();
    let (p1, p2) = (b.proc(), b.proc());
    b.at(1000.0).await;
    for i in 0..3 {
        b.push("orders", "c1", json!({ "n": i })).await;
    }
    let chain = |q: &Queen| {
        earliest(q)
            .window_tumbling(60.0)
            .aggregate(FeedAggregate::new().count("count"))
            .to("counts")
    };
    let first = p1
        .run(chain(&p1.queen), fast("restart").now(b.clock()))
        .await
        .unwrap();
    until(3000, "three read", || async {
        m(&first, "entriesRead") >= 3.0
    })
    .await;
    first.stop().await;
    b.push_at(2000.0, "orders", "c1", json!({ "n": 3 })).await;
    let second = p2
        .run(chain(&p2.queen), fast("restart").now(b.clock()))
        .await
        .unwrap();
    assert_eq!(
        m(&second, "replays"),
        0.0,
        "the stop wrote a snapshot: nothing to read again"
    );
    until(3000, "one read", || async {
        m(&second, "entriesRead") >= 1.0
    })
    .await;
    b.push_at(61_000.0, "other", "x", json!({})).await;
    until(3000, "the count", || async {
        !b.messages("counts").await.is_empty()
    })
    .await;
    second.stop().await;
    assert_eq!(b.payloads("counts").await, [json!({ "count": 4 })]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chain_that_no_longer_fits_the_stored_state_is_refused_and_reset_starts_it_over() {
    let b = Fake::start();
    let p = b.proc();
    b.push_at(1000.0, "orders", "c1", json!({})).await;
    let first = p
        .run(
            earliest(&p.queen)
                .window_tumbling(60.0)
                .aggregate(FeedAggregate::new().count("count"))
                .to("counts"),
            fast("chain").now(b.clock()),
        )
        .await
        .unwrap();
    until(3000, "one key", || async { m(&first, "stateKeys") >= 1.0 }).await;
    first.stop().await;
    assert!(
        !b.kv_keys(NS, "chain#snap#").await.is_empty(),
        "a stop writes a snapshot"
    );
    let changed = |q: &Queen| {
        earliest(q)
            .window_tumbling(30.0)
            .aggregate(FeedAggregate::new().count("count"))
            .to("counts")
    };
    let err = p.run(changed(&p.queen), fast("chain")).await.unwrap_err();
    assert_eq!(err.code().map(|c| c.as_str()), Some("feed_chain_changed"));
    let again = p
        .run(
            changed(&p.queen),
            fast("chain")
                .reset(Reset::Token("v2".into()))
                .now(b.clock()),
        )
        .await
        .unwrap();
    until(3000, "one read", || async {
        m(&again, "entriesRead") >= 1.0
    })
    .await;
    again.stop().await;
    let stored = b.record("chain").await.unwrap();
    assert_eq!(stored["epoch"], 1);
    assert_eq!(stored["resetToken"], "v2");
    // The same reset string again carries on, it does not start over.
    let third = p
        .run(
            changed(&p.queen),
            fast("chain")
                .reset(Reset::Token("v2".into()))
                .now(b.clock()),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    third.stop().await;
    assert_eq!(b.record("chain").await.unwrap()["epoch"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_join_on_the_feeds_clock_times_out_the_one_left_alone() {
    let b = Fake::start();
    let p = b.proc();
    b.push_at(0.0, "shipments", "order-1", json!({ "carrier": "DHL" }))
        .await;
    b.push_at(1000.0, "orders", "order-1", json!({ "id": 1 }))
        .await;
    b.push_at(2000.0, "orders", "order-2", json!({ "id": 2 }))
        .await;
    let s = earliest_of(&p.queen, &["orders", "shipments"])
        .join(
            Join::new(
                |m| m["queue"] == "orders",
                |m| m["queue"] == "shipments",
                "1h",
            )
            .select(|o, s| json!({ "order": o["data"]["id"], "carrier": s["data"]["carrier"] }))
            .on_timeout(|m, side| json!({ "unshipped": m["data"]["id"], "side": side })),
        )
        .to("joined");
    let r = p.run(s, fast("join").now(b.clock())).await.unwrap();
    until(3000, "the match", || async {
        !b.messages("joined").await.is_empty()
    })
    .await;
    b.at(2000.0 + 3_601_000.0).await;
    until(3000, "the timeout of order 2", || async {
        b.messages("joined").await.len() >= 2
    })
    .await;
    r.stop().await;
    assert_eq!(
        b.payloads("joined").await,
        [
            json!({ "order": 1, "carrier": "DHL" }),
            json!({ "unshipped": 2, "side": "left" })
        ]
    );
}

// --------------------------------------------------- commits that go wrong

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_whose_answer_was_lost_is_not_written_twice() {
    let b = Fake::start();
    let p = b.proc();
    for i in 1..=3 {
        b.push("orders", "c1", json!({ "n": i })).await;
    }
    // The broker applies the commit, and the answer never comes back.
    b.inject("/api/v1/transaction", "after", 503, 1).await;
    let r = p
        .run(
            earliest_of(&p.queen, &["orders"]).to("copy"),
            fast("lost-answer").on_error(|_| {}),
        )
        .await
        .unwrap();
    until(4000, "the retry to find the commit stored", || async {
        m(&r, "conflicts") >= 1.0
    })
    .await;
    b.push("orders", "c1", json!({ "n": 4 })).await;
    until(3000, "four copies", || async {
        b.messages("copy").await.len() >= 4
    })
    .await;
    r.stop().await;
    let ns: Vec<Value> = b
        .payloads("copy")
        .await
        .iter()
        .map(|p| p["n"].clone())
        .collect();
    assert_eq!(ns, [json!(1), json!(2), json!(3), json!(4)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_with_more_outputs_than_max_pending_outputs_commits_between_positions() {
    let b = Fake::start();
    let p = b.proc();
    for i in 1..=7 {
        b.push("orders", &format!("c{i}"), json!({ "n": i })).await;
    }
    let before = b.stats().await["pushTxns"].as_u64().unwrap();
    let r = p
        .run(
            earliest_of(&p.queen, &["orders"]).to("copy"),
            fast("split").max_pending_outputs(2),
        )
        .await
        .unwrap();
    until(3000, "seven copies", || async {
        b.messages("copy").await.len() >= 7
    })
    .await;
    r.stop().await;
    let ns: Vec<Value> = b
        .payloads("copy")
        .await
        .iter()
        .map(|p| p["n"].clone())
        .collect();
    assert_eq!(ns, (1..=7).map(|i| json!(i)).collect::<Vec<_>>());
    let commits = b.stats().await["pushTxns"].as_u64().unwrap() - before;
    assert!(commits >= 4, "{commits} commits");
    let positions: std::collections::HashSet<u64> = b
        .messages("copy")
        .await
        .iter()
        .map(|e| e["position"].as_u64().unwrap())
        .collect();
    assert!(positions.len() >= 4, "at most two outputs per commit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_whose_answer_was_lost_does_not_count_twice_in_a_window() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    for i in 1..=3 {
        b.push("orders", "c1", json!({ "n": i })).await;
    }
    b.inject("/api/v1/transaction", "after", 503, 1).await;
    let s = earliest_of(&p.queen, &["orders"])
        .window_tumbling(60.0)
        .aggregate(FeedAggregate::new().count("count"))
        .to("counts");
    let r = p
        .run(s, fast("lost-window").now(b.clock()).on_error(|_| {}))
        .await
        .unwrap();
    until(4000, "the retry to find the commit stored", || async {
        m(&r, "conflicts") >= 1.0
    })
    .await;
    b.push_at(2000.0, "orders", "c1", json!({ "n": 4 })).await;
    b.push_at(61_000.0, "orders", "c2", json!({ "n": 5 })).await;
    until(3000, "the count", || async {
        !b.messages("counts").await.is_empty()
    })
    .await;
    r.stop().await;
    assert_eq!(b.payloads("counts").await, [json!({ "count": 4 })]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_commit_carries_on_from_the_position_that_won_writing_nothing_of_its_own() {
    let b = Fake::start();
    let p = b.proc();
    let r = p
        .run(
            earliest_of(&p.queen, &["orders"]).to("copy"),
            fast("copy")
                .exclusive(false)
                .position_commit_millis(3_600_000.0),
        )
        .await
        .unwrap();
    // Another process committed first: its position is past n=1 and n=2.
    let applied = b.dump().await["applied"].as_u64().unwrap();
    let winner = applied + 3;
    b.kv_put(NS, "copy", json!({ "position": winner, "queryId": "copy" }))
        .await;
    b.tx(&[("orders", "c1", json!({ "n": 1 }))]).await;
    b.tx(&[("orders", "c1", json!({ "n": 2 }))]).await;
    until(3000, "the lost commit", || async {
        m(&r, "conflicts") >= 1.0
    })
    .await;
    let n3 = b.push("orders", "c1", json!({ "n": 3 })).await;
    assert_eq!(
        n3, winner,
        "n=3 is the first message past the winner's position"
    );
    until(3000, "the copy", || async {
        !b.messages("copy").await.is_empty()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    r.stop().await;
    let ns: Vec<Value> = b
        .payloads("copy")
        .await
        .iter()
        .map(|p| p["n"].clone())
        .collect();
    assert_eq!(
        ns,
        [json!(3)],
        "n=1 lost to the winner, n=2 was the winner's"
    );
    assert_eq!(m(&r, "errors"), 0.0, "a lost race is not an error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_refused_as_duplicate_reads_on_from_the_stored_position() {
    let b = Fake::start();
    let p = b.proc();
    b.push("orders", "c1", json!({ "n": 1 })).await;
    let src = b.messages("orders").await[0].clone();
    // Another runner pushed this output, and then its position.
    let id = feed_output_id("race", &src, 0, 0, "race");
    let other = b
        .tx_ids(&[("copy", "c1", json!({ "n": 1 }), id.as_str())])
        .await;
    let r = p
        .run(
            earliest_of(&p.queen, &["orders"]).to("copy"),
            fast("race").exclusive(false).on_error(|_| {}),
        )
        .await
        .unwrap();
    b.kv_put(
        NS,
        "race",
        json!({ "position": other + 1, "queryId": "race" }),
    )
    .await;
    until(3000, "the duplicate", || async {
        m(&r, "conflicts") >= 1.0
    })
    .await;
    b.push("orders", "c1", json!({ "n": 2 })).await;
    until(3000, "the copy of n=2", || async {
        b.messages("copy").await.len() >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    r.stop().await;
    let copy = b.messages("copy").await;
    let ns: Vec<Value> = copy.iter().map(|e| e["payload"]["n"].clone()).collect();
    assert_eq!(
        ns,
        [json!(1), json!(2)],
        "n=1 once (the other runner's), then n=2"
    );
    assert_eq!(copy[0]["transactionId"], json!(id));
    assert_eq!(m(&r, "errors"), 0.0, "a lost race is not an error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_process_that_loses_its_lock_cannot_commit_and_the_other_one_takes_over() {
    let b = Fake::start();
    let proxy = Proxy::start(&b.url).await;
    let (pa, pb) = (proxy.proc(), b.proc());
    b.push("orders", "c1", json!({ "n": 1 })).await;
    let opts = || {
        fast("cut")
            .lock_ttl("1s")
            .retry_delay_millis(20.0)
            .on_error(|_| {})
    };
    let ra = pa
        .run(earliest_of(&pa.queen, &["orders"]).to("copy"), opts())
        .await
        .unwrap();
    let rb = pb
        .run(earliest_of(&pb.queen, &["orders"]).to("copy"), opts())
        .await
        .unwrap();
    until(3000, "the first copy", || async {
        !b.messages("copy").await.is_empty()
    })
    .await;
    assert_eq!(role(&ra), "active");
    // a's process is cut off (a partition, or a long pause): its lock expires.
    proxy.cut(true);
    b.push("orders", "c1", json!({ "n": 2 })).await;
    until(8000, "b to take over", || async {
        b.messages("copy").await.len() >= 2
    })
    .await;
    proxy.cut(false);
    b.push("orders", "c1", json!({ "n": 3 })).await;
    until(4000, "the third copy", || async {
        b.messages("copy").await.len() >= 3
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(role(&ra), "standby", "a found its lock gone");
    assert_eq!(role(&rb), "active");
    let ns: Vec<Value> = b
        .payloads("copy")
        .await
        .iter()
        .map(|p| p["n"].clone())
        .collect();
    assert_eq!(ns, [json!(1), json!(2), json!(3)]);
    ra.stop().await;
    rb.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_process_whose_lock_was_taken_can_commit_nothing_more_and_stands_by() {
    let b = Fake::start();
    let p = b.proc();
    b.push("orders", "c1", json!({ "n": 1 })).await;
    let r = p
        .run(
            earliest_of(&p.queen, &["orders"]).to("copy"),
            fast("stolen").on_error(|_| {}),
        )
        .await
        .unwrap();
    until(3000, "the first copy", || async {
        !b.messages("copy").await.is_empty()
    })
    .await;
    // The lock expired on the broker and somebody else holds it now.
    b.steal_lock("queen-feed/stolen").await;
    b.push("orders", "c1", json!({ "n": 2 })).await;
    until(3000, "the stand-down", || async { role(&r) == "standby" }).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let ns: Vec<Value> = b
        .payloads("copy")
        .await
        .iter()
        .map(|p| p["n"].clone())
        .collect();
    assert_eq!(ns, [json!(1)], "n=2 is the new holder's to copy");
    r.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_tried_again_counts_its_results_once_in_emitted_and_a_skipped_one_counts_none() {
    let b = Fake::start();
    let p = b.proc();
    orders(&b, 3).await;
    let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let calls = Arc::new(Mutex::new(Vec::<i64>::new()));
    let (f, c) = (Arc::clone(&failed), Arc::clone(&calls));
    let s = earliest(&p.queen)
        .flat_map(|m, _| {
            let n = m["data"]["n"].as_i64().unwrap();
            vec![json!(n), json!(n * 10)]
        })
        .foreach(move |v, _| {
            let (f, c) = (Arc::clone(&f), Arc::clone(&c));
            async move {
                let v = v.unwrap().as_i64().unwrap();
                c.lock().unwrap().push(v);
                if v == 20 && !f.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    return Err("once".to_string());
                }
                if v == 30 {
                    return Err("always".to_string());
                }
                Ok(())
            }
        });
    let r = p
        .run(
            s,
            fast("emitted-once")
                .on_poison(Poison::Skip)
                .on_error(|_| {}),
        )
        .await
        .unwrap();
    until(3000, "the skip", || async { m(&r, "skipped") >= 1.0 }).await;
    r.stop().await;
    assert_eq!(
        calls.lock().unwrap()[..5],
        [1, 10, 2, 20, 2],
        "message 2 runs again from its first result"
    );
    assert_eq!(
        m(&r, "emitted"),
        4.0,
        "the four results of messages 1 and 2, once each"
    );
    assert_eq!(m(&r, "skipped"), 1.0);
}
