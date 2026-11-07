//! `.foreach_batch(f, BatchOptions)` on a feed stream: the results in
//! batches, and the position saved only once `f` returns. The tests of the JS
//! feed-unit suite (feedBatch.test.js), against the conformance kit's fake
//! broker.

mod feed_support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use feed_support::*;
use queen_mq::feed::FeedFrom;
use queen_mq::streams::feed::{
    feed_output_id, output_prefix, BatchItem, BatchOptions, FeedAggregate, FeedSource, FeedStream,
    Poison,
};
use queen_mq::streams::Stream;
use queen_mq::Queen;
use serde_json::{json, Value};

type Calls = Arc<Mutex<Vec<Vec<BatchItem>>>>;

/// A batch function that records each call, then answers `then(items, call)` (to fail).
fn recorder(
    then: impl Fn(&[BatchItem], usize) -> Result<(), String> + Send + Sync + 'static,
) -> (
    Calls,
    impl Fn(Vec<BatchItem>) -> std::future::Ready<Result<(), String>> + Send + Sync + 'static,
) {
    let calls: Calls = Arc::default();
    let c = Arc::clone(&calls);
    let f = move |items: Vec<BatchItem>| {
        let n = {
            let mut c = c.lock().unwrap();
            c.push(items.clone());
            c.len()
        };
        std::future::ready(then(&items, n))
    };
    (calls, f)
}

fn ok(_: &[BatchItem], _: usize) -> Result<(), String> {
    Ok(())
}

fn calls_of(c: &Calls) -> Vec<Vec<BatchItem>> {
    c.lock().unwrap().clone()
}

fn ns(items: &[BatchItem]) -> Vec<i64> {
    items
        .iter()
        .map(|it| it.value.as_ref().unwrap()["n"].as_i64().unwrap())
        .collect()
}

fn ns_all(c: &Calls) -> Vec<Vec<i64>> {
    calls_of(c).iter().map(|items| ns(items)).collect()
}

fn keys_counts(items: &[BatchItem]) -> Vec<(Value, Value)> {
    items
        .iter()
        .map(|it| {
            (
                it.ctx["key"].clone(),
                it.value.as_ref().unwrap()["count"].clone(),
            )
        })
        .collect()
}

async fn orders(b: &Fake, from: i64, to: i64) {
    for i in from..=to {
        b.push("orders", &format!("c{i}"), json!({ "n": i })).await;
    }
}

/// Orders `from..=to`, each its own transaction, all in before a reader wakes
/// (as the JS in-process fake pushes them while a stream runs).
async fn orders_at_once(b: &Fake, from: i64, to: i64) {
    let items: Vec<(String, String, Value)> = (from..=to)
        .map(|i| ("orders".to_string(), format!("c{i}"), json!({ "n": i })))
        .collect();
    b.txs_one_each(&items).await;
}

async fn last_order(b: &Fake) -> Value {
    b.messages("orders").await.last().cloned().unwrap()
}

/// The record's position is the next one to read: past `e` once e is committed.
async fn past(b: &Fake, id: &str, e: &Value) -> bool {
    b.record(id)
        .await
        .and_then(|r| r["position"].as_u64())
        .unwrap_or(0)
        > e["position"].as_u64().unwrap()
}

async fn past_last(b: &Fake, id: &str) -> bool {
    let e = last_order(b).await;
    past(b, id, &e).await
}

fn numbers(q: &Queen) -> FeedStream {
    Stream::from_feed(q, FeedSource::new().from(FeedFrom::Earliest))
        .map(|m, _| json!({ "n": m["data"]["n"] }))
}

fn per_minute(q: &Queen) -> FeedStream {
    Stream::from_feed(
        q,
        FeedSource::new()
            .from(FeedFrom::Earliest)
            .queues(["orders"]),
    )
    .window_tumbling(60.0)
    .aggregate(FeedAggregate::new().count("count"))
}

type BoxFut = std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>;
type Positions = Arc<Mutex<Vec<Option<u64>>>>;

/// Items with the record's position at the time of each call.
fn position_recorder(
    url: String,
    id: &'static str,
) -> (
    Calls,
    Positions,
    impl Fn(Vec<BatchItem>) -> BoxFut + Send + Sync + 'static,
) {
    let calls: Calls = Arc::default();
    let positions: Positions = Arc::default();
    let (c, p) = (Arc::clone(&calls), Arc::clone(&positions));
    let f = move |items: Vec<BatchItem>| {
        let (c, p, url) = (Arc::clone(&c), Arc::clone(&p), url.clone());
        Box::pin(async move {
            c.lock().unwrap().push(items);
            let at = record_at(&url, id)
                .await
                .and_then(|r| r["position"].as_u64());
            p.lock().unwrap().push(at);
            Ok(())
        }) as BoxFut
    };
    (calls, positions, f)
}

// ------------------------------------------------------- batches and the position

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_batch_per_read_each_item_has_the_value_the_ctx_foreach_gets_and_the_id_of_the_output()
{
    let b = Fake::start();
    let p = b.proc();
    orders(&b, 1, 5).await;
    let (open_tx, open_rx) = tokio::sync::watch::channel(false);
    let calls: Calls = Arc::default();
    let c = Arc::clone(&calls);
    let r = p
        .run(
            numbers(&p.queen).foreach_batch(
                move |items| {
                    c.lock().unwrap().push(items);
                    let mut gate = open_rx.clone();
                    async move {
                        let _ = gate.wait_for(|open| *open).await;
                        Ok::<(), String>(())
                    }
                },
                BatchOptions::new(),
            ),
            fast("per-read"),
        )
        .await
        .unwrap();
    until(3000, "the call", || async { calls_of(&calls).len() == 1 }).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        b.record("per-read").await,
        None,
        "nothing committed while fn runs"
    );
    open_tx.send_replace(true);
    until(3000, "the commit after it", || async {
        past_last(&b, "per-read").await
    })
    .await;
    let items = calls_of(&calls)[0].clone();
    assert_eq!(ns(&items), [1, 2, 3, 4, 5], "the whole read, in log order");
    let msgs = b.messages("orders").await;
    for (it, e) in items.iter().zip(&msgs) {
        assert_eq!(
            it.ctx,
            json!({
                "position": e["position"], "queue": "orders", "partition": e["partition"],
                "partitionId": e["partitionId"], "offset": e["offset"], "transactionId": e["transactionId"]
            })
        );
        assert_eq!(
            it.id,
            feed_output_id("per-read", e, 0, 0, "per-read"),
            "the id a .to() output of it would have"
        );
    }
    let mm = r.metrics();
    assert_eq!(mm["batches"], 1);
    assert_eq!(mm["effects"], 5, "effects count the items delivered");
    assert_eq!(mm["emitted"], 5);
    assert_eq!(mm["pendingBatch"], 0);
    assert_eq!(calls_of(&calls).len(), 1);
    r.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_items_a_read_of_7_goes_in_calls_of_3_3_and_1_each_followed_by_its_commit() {
    let b = Fake::start();
    let p = b.proc();
    orders(&b, 1, 7).await;
    let (calls, positions, f) = position_recorder(b.url.clone(), "max-items");
    let r = p
        .run(
            numbers(&p.queen).foreach_batch(f, BatchOptions::new().max_items(3)),
            fast("max-items"),
        )
        .await
        .unwrap();
    until(3000, "the end", || async {
        past_last(&b, "max-items").await
    })
    .await;
    assert_eq!(ns_all(&calls), [vec![1, 2, 3], vec![4, 5, 6], vec![7]]);
    let msgs = b.messages("orders").await;
    assert_eq!(
        *positions.lock().unwrap(),
        [
            None,
            msgs[3]["position"].as_u64(),
            msgs[6]["position"].as_u64()
        ],
        "each call sees the commit of the one before, and no more"
    );
    r.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_items_also_splits_what_one_message_makes_and_the_commit_comes_after_the_last() {
    let b = Fake::start();
    let p = b.proc();
    b.push("orders", "c1", json!({ "n": 5 })).await;
    let (calls, positions, f) = position_recorder(b.url.clone(), "split-one");
    let s = Stream::from_feed(&p.queen, FeedSource::new().from(FeedFrom::Earliest))
        .flat_map(|m, _| {
            (0..m["data"]["n"].as_i64().unwrap())
                .map(|i| json!({ "n": i }))
                .collect::<Vec<_>>()
        })
        .foreach_batch(f, BatchOptions::new().max_items(2));
    let r = p.run(s, fast("split-one")).await.unwrap();
    until(3000, "the end", || async {
        past_last(&b, "split-one").await
    })
    .await;
    assert_eq!(ns_all(&calls), [vec![0, 1], vec![2, 3], vec![4]]);
    assert_eq!(
        *positions.lock().unwrap(),
        [None, None, None],
        "one message is one position: it commits once, after its last item"
    );
    let e = &b.messages("orders").await[0];
    let ids: Vec<String> = calls_of(&calls)
        .concat()
        .into_iter()
        .map(|it| it.id)
        .collect();
    assert_eq!(
        ids,
        (0..5)
            .map(|i| feed_output_id("split-one", e, i, 0, "split-one"))
            .collect::<Vec<_>>()
    );
    r.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_wait_millis_one_batch_across_reads_nothing_committed_while_it_waits_full_it_goes_at_once(
) {
    let b = Fake::start();
    let p = b.proc();
    b.at(0.0).await;
    orders(&b, 1, 2).await;
    let (calls, f) = recorder(ok);
    let r = p
        .run(
            numbers(&p.queen)
                .foreach_batch(f, BatchOptions::new().max_items(5).max_wait_millis(1000)),
            fast("linger").now(b.clock()),
        )
        .await
        .unwrap();
    until(3000, "two waiting", || async {
        m(&r, "pendingBatch") == 2.0
    })
    .await;
    b.at(400.0).await;
    orders(&b, 3, 3).await;
    until(3000, "three waiting", || async {
        m(&r, "pendingBatch") == 3.0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(calls_of(&calls).is_empty(), "it waits for more");
    assert_eq!(
        b.record("linger").await,
        None,
        "and nothing commits past the items that wait"
    );
    b.at(1000.0).await;
    until(3000, "the call", || async { calls_of(&calls).len() == 1 }).await;
    assert_eq!(
        ns(&calls_of(&calls)[0]),
        [1, 2, 3],
        "one batch of the two reads"
    );
    until(3000, "its commit", || async {
        past_last(&b, "linger").await
    })
    .await;
    // Five at once fill the batch: it goes without waiting.
    orders_at_once(&b, 4, 8).await;
    until(3000, "the second call", || async {
        calls_of(&calls).len() == 2
    })
    .await;
    assert_eq!(ns(&calls_of(&calls)[1]), [4, 5, 6, 7, 8]);
    until(3000, "its commit", || async {
        past_last(&b, "linger").await
    })
    .await;
    r.stop().await;
}

// ------------------------------------------------------------ stops and crashes

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_delivers_the_batch_that_waits_then_commits_with_the_stop_mark() {
    let b = Fake::start();
    let p = b.proc();
    orders(&b, 1, 3).await;
    let (calls, f) = recorder(ok);
    let r = p
        .run(
            numbers(&p.queen).foreach_batch(f, BatchOptions::new().max_wait_millis(3_600_000)),
            fast("stop-flush").now(b.clock()),
        )
        .await
        .unwrap();
    until(3000, "three waiting", || async {
        m(&r, "pendingBatch") == 3.0
    })
    .await;
    assert!(calls_of(&calls).is_empty());
    r.stop().await;
    assert_eq!(ns_all(&calls), [vec![1, 2, 3]]);
    assert!(
        past_last(&b, "stop-flush").await,
        "committed past the batch"
    );
    assert_eq!(
        b.record("stop-flush").await.unwrap()["stopped"]["reason"],
        "stop"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_that_fails_at_stop_is_tried_once_and_left_for_the_next_start_which_gets_the_same_items(
) {
    let b = Fake::start();
    let p = b.proc();
    orders(&b, 1, 2).await;
    let (first, f) = recorder(|items, _| {
        if items[0].value.as_ref().unwrap()["n"] == 3 {
            return Err("down".into());
        }
        Ok(())
    });
    let r = p
        .run(
            numbers(&p.queen).foreach_batch(
                f,
                BatchOptions::new().max_wait_millis(3_600_000).max_items(2),
            ),
            fast("stop-fail").now(b.clock()).retries(3).on_error(|_| {}),
        )
        .await
        .unwrap();
    until(3000, "the first batch committed", || async {
        past_last(&b, "stop-fail").await
    })
    .await;
    orders(&b, 3, 3).await;
    until(3000, "one waiting", || async {
        m(&r, "pendingBatch") == 1.0
    })
    .await;
    r.stop().await;
    assert_eq!(
        ns_all(&first),
        [vec![1, 2], vec![3]],
        "one try at the stop: no retries"
    );
    assert!(
        !past_last(&b, "stop-fail").await,
        "the item is not committed"
    );
    assert_eq!(
        b.record("stop-fail").await.unwrap()["stopped"]["reason"],
        "stop"
    );
    let p2 = b.proc();
    let (again, f) = recorder(ok);
    let second = p2
        .run(
            numbers(&p2.queen).foreach_batch(f, BatchOptions::new()),
            fast("stop-fail"),
        )
        .await
        .unwrap();
    until(3000, "the batch again", || async {
        calls_of(&again).len() == 1
    })
    .await;
    assert_eq!(ns(&calls_of(&again)[0]), [3]);
    assert_eq!(
        calls_of(&again)[0][0].id,
        calls_of(&first)[1][0].id,
        "the same id: an upsert by it writes once"
    );
    second.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_before_fn_returns_delivers_the_batch_again_with_the_same_ids_and_a_replay_delivers_nothing(
) {
    let b = Fake::start();
    let proxy = Proxy::start(&b.url).await;
    let pa = proxy.proc();
    b.at(1000.0).await;
    b.push("orders", "c1", json!({})).await;
    b.push("orders", "c2", json!({})).await;
    b.push_at(61_000.0, "orders", "c1", json!({})).await;
    // A delivers minute 0 and commits it; minute 1 closes in a batch it never commits.
    let cut = proxy.cutter();
    let was_cut = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let wc = Arc::clone(&was_cut);
    let (a, fa) = recorder(move |items, _| {
        if items
            .iter()
            .any(|it| it.ctx["windowStart"].as_f64() == Some(T0 + 60_000.0))
        {
            cut();
            wc.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    });
    let opts = || {
        fast("batch-crash")
            .now(b.clock())
            .lock_ttl("1s")
            .snapshot_every_millis(3_600_000.0)
            .retry_delay_millis(20.0)
            .on_error(|_| {})
    };
    let _first = pa
        .run(
            per_minute(&pa.queen).foreach_batch(fa, BatchOptions::new()),
            opts(),
        )
        .await
        .unwrap();
    until(3000, "minute 0 delivered and committed", || async {
        calls_of(&a).len() == 1 && past_last(&b, "batch-crash").await
    })
    .await;
    assert_eq!(
        keys_counts(&calls_of(&a)[0]),
        [(json!("c1"), json!(1)), (json!("c2"), json!(1))],
        "minute 0, one item per key"
    );
    b.push_at(121_000.0, "orders", "c3", json!({})).await;
    until(3000, "the cut", || async {
        was_cut.load(std::sync::atomic::Ordering::SeqCst)
    })
    .await;
    let lost = calls_of(&a)[1].clone();
    assert_eq!(keys_counts(&lost), [(json!("c1"), json!(1))]);
    let pb = b.proc();
    let (bb, fb) = recorder(ok);
    let second = pb
        .run(
            per_minute(&pb.queen).foreach_batch(fb, BatchOptions::new()),
            opts(),
        )
        .await
        .unwrap();
    until(6000, "the takeover", || async { role(&second) == "active" }).await;
    until(3000, "a call", || async { !calls_of(&bb).is_empty() }).await;
    assert!(
        m(&second, "replays") >= 1.0,
        "it read the feed again from the snapshot"
    );
    assert_eq!(
        keys_counts(&calls_of(&bb)[0]),
        [(json!("c1"), json!(1))],
        "minute 1 again, and minute 0 not: the replay sends nothing"
    );
    let ids = |items: &[BatchItem]| items.iter().map(|it| it.id.clone()).collect::<Vec<_>>();
    assert_eq!(
        ids(&calls_of(&bb)[0]),
        ids(&lost),
        "the same ids as the batch that never committed"
    );
    assert!(lost
        .iter()
        .all(|it| it.id.starts_with(&output_prefix("batch-crash"))));
    until(3000, "its commit", || async {
        past_last(&b, "batch-crash").await
    })
    .await;
    second.stop().await;
}

// ------------------------------------------------- failures and the poison policy

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_that_fails_then_passes_is_tried_again_whole_not_poison() {
    let b = Fake::start();
    let p = b.proc();
    orders(&b, 1, 3).await;
    let (calls, f) = recorder(|_, call| {
        if call <= 2 {
            Err("flaky".into())
        } else {
            Ok(())
        }
    });
    let r = p
        .run(
            numbers(&p.queen).foreach_batch(f, BatchOptions::new()),
            fast("batch-flaky"),
        )
        .await
        .unwrap();
    until(3000, "the end", || async {
        past_last(&b, "batch-flaky").await
    })
    .await;
    assert_eq!(
        ns_all(&calls),
        [vec![1, 2, 3], vec![1, 2, 3], vec![1, 2, 3]]
    );
    let all = calls_of(&calls);
    let ids = |items: &[BatchItem]| items.iter().map(|it| it.id.clone()).collect::<Vec<_>>();
    assert_eq!(ids(&all[2]), ids(&all[0]));
    let mm = r.metrics();
    assert_eq!(mm["retries"], 2);
    assert_eq!(mm["poisoned"], 0);
    assert_eq!(mm["batches"], 1);
    assert_eq!(mm["effects"], 3);
    r.stop().await;
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
async fn skip_tries_whole_then_item_by_item_the_item_that_fails_is_left_out_the_others_are_delivered(
) {
    let b = Fake::start();
    let p = b.proc();
    orders(&b, 1, 4).await;
    let errors = Arc::new(Mutex::new(Vec::new()));
    let (calls, f) = recorder(|items, _| {
        if items.iter().any(|it| it.value.as_ref().unwrap()["n"] == 3) {
            return Err("bad row".into());
        }
        Ok(())
    });
    let r = p
        .run(
            numbers(&p.queen).foreach_batch(f, BatchOptions::new()),
            fast("batch-skip")
                .on_poison(Poison::Skip)
                .retries(1)
                .on_error(errors_into(&errors)),
        )
        .await
        .unwrap();
    until(3000, "the end", || async {
        past_last(&b, "batch-skip").await
    })
    .await;
    assert_eq!(
        ns_all(&calls),
        [
            vec![1, 2, 3, 4],
            vec![1, 2, 3, 4],
            vec![1],
            vec![2],
            vec![3],
            vec![3],
            vec![4]
        ]
    );
    let mm = r.metrics();
    assert_eq!(mm["skipped"], 1);
    assert_eq!(mm["poisoned"], 1);
    assert_eq!(mm["effects"], 3, "items 1, 2 and 4");
    assert_eq!(mm["batches"], 3);
    let third = &b.messages("orders").await[2];
    let errs = errors.lock().unwrap().clone();
    let skip = errs
        .iter()
        .find(|e| e["action"] == "skipped")
        .expect("a skip");
    assert_eq!(skip["phase"], "poison");
    assert_eq!(skip["position"], third["position"]);
    assert_eq!(skip["partition"], "c3");
    let msg = skip["message"].as_str().unwrap();
    assert!(
        msg.starts_with("an item of the message at position ")
            && msg.ends_with(
                " (orders/c3, offset 1) failed 2 times in foreachBatch and was skipped: bad row"
            ),
        "{msg}"
    );
    r.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_letter_queue_gets_the_item_in_the_commit_that_moves_the_position_past_the_batch() {
    let b = Fake::start();
    let p = b.proc();
    orders(&b, 1, 3).await;
    let (calls, f) = recorder(|items, _| {
        if items.iter().any(|it| it.value.as_ref().unwrap()["n"] == 2) {
            return Err("no price".into());
        }
        Ok(())
    });
    let txns = b.stats().await["txns"].as_u64().unwrap();
    let r = p
        .run(
            numbers(&p.queen).foreach_batch(f, BatchOptions::new()),
            fast("batch-dlq")
                .on_poison(Poison::DeadLetter("batch-dlq-q".into()))
                .retries(0)
                .on_error(|_| {}),
        )
        .await
        .unwrap();
    until(3000, "the end", || async {
        past_last(&b, "batch-dlq").await
    })
    .await;
    assert_eq!(ns_all(&calls), [vec![1, 2, 3], vec![1], vec![2], vec![3]]);
    let dead = b.messages("batch-dlq-q").await;
    assert_eq!(dead.len(), 1, "dead-lettered");
    assert_eq!(dead[0]["partition"], "c2", "in the partition it came from");
    let second = &b.messages("orders").await[1];
    assert_eq!(
        dead[0]["payload"],
        json!({
            "queryId": "batch-dlq",
            "attempts": 1,
            "error": { "name": "Error", "message": "no price" },
            "item": { "value": { "n": 2 }, "ctx": calls_of(&calls)[0][1].ctx, "id": feed_output_id("batch-dlq", second, 0, 0, "batch-dlq") }
        })
    );
    assert!(
        dead[0]["transactionId"]
            .as_str()
            .unwrap()
            .starts_with(&output_prefix("batch-dlq")),
        "the query never reads it back"
    );
    assert_eq!(
        b.stats().await["txns"].as_u64().unwrap() - txns,
        1,
        "with the position past the batch, in one transaction"
    );
    assert_eq!(m(&r, "deadLettered"), 1.0);
    r.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_commits_nothing_of_the_batch_the_record_names_the_item_and_a_restart_gets_the_whole_batch(
) {
    let b = Fake::start();
    let p = b.proc();
    orders(&b, 1, 2).await;
    let errors = Arc::new(Mutex::new(Vec::new()));
    let (calls, f) = recorder(|items, _| {
        if items.iter().any(|it| it.value.as_ref().unwrap()["n"] == 4) {
            return Err("bad row".into());
        }
        Ok(())
    });
    let r = p
        .run(
            numbers(&p.queen).foreach_batch(f, BatchOptions::new()),
            fast("batch-stop").retries(1).on_error(errors_into(&errors)),
        )
        .await
        .unwrap();
    until(3000, "the first batch", || async {
        past_last(&b, "batch-stop").await
    })
    .await;
    let before = b.record("batch-stop").await.unwrap()["position"].clone();
    orders_at_once(&b, 3, 5).await;
    tokio::time::timeout(Duration::from_secs(10), r.done())
        .await
        .expect("it stops");
    assert_eq!(
        ns_all(&calls),
        [
            vec![1, 2],
            vec![3, 4, 5],
            vec![3, 4, 5],
            vec![3],
            vec![4],
            vec![4]
        ],
        "whole twice, then one by one up to the item"
    );
    let fourth = &b.messages("orders").await[3];
    let rec = b.record("batch-stop").await.unwrap();
    assert_eq!(rec["position"], before, "the last commit: before the batch");
    assert_eq!(rec["stopped"]["reason"], "poison");
    assert_eq!(
        rec["stopped"]["position"], fourth["position"],
        "the item that failed"
    );
    assert_eq!(rec["stopped"]["partition"], "c4");
    let mm = r.metrics();
    assert_eq!(mm["role"], "stopped");
    assert_eq!(mm["stopped"]["reason"], "poison");
    assert_eq!(mm["stopped"]["position"], fourth["position"]);
    let errs = errors.lock().unwrap().clone();
    let stop = errs
        .iter()
        .find(|e| e["action"] == "stopped")
        .expect("the stop");
    assert_eq!(stop["code"], "feed_poison");
    let msg = stop["message"].as_str().unwrap();
    assert!(
        msg.starts_with("an item of the message at position ")
            && msg.contains(" (orders/c4, offset 1) failed 2 times in foreachBatch: bad row. The stream stopped before its batch"),
        "{msg}"
    );
    let p2 = b.proc();
    let (again, f) = recorder(ok);
    let fixed = p2
        .run(
            numbers(&p2.queen).foreach_batch(f, BatchOptions::new()),
            fast("batch-stop"),
        )
        .await
        .unwrap();
    until(3000, "the end", || async {
        past_last(&b, "batch-stop").await
    })
    .await;
    assert_eq!(ns_all(&again), [vec![3, 4, 5]]);
    let ids = |items: &[BatchItem]| items.iter().map(|it| it.id.clone()).collect::<Vec<_>>();
    assert_eq!(ids(&calls_of(&again)[0]), ids(&calls_of(&calls)[1]));
    fixed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_that_stops_the_stream_comes_after_the_batch_that_waits_the_batch_goes_first() {
    let b = Fake::start();
    let p = b.proc();
    orders(&b, 1, 4).await;
    let (calls, f) = recorder(ok);
    let s = Stream::from_feed(&p.queen, FeedSource::new().from(FeedFrom::Earliest))
        .map(|m, _| -> Result<Value, String> {
            if m["data"]["n"] == 3 {
                return Err("bad order".into());
            }
            Ok(json!({ "n": m["data"]["n"] }))
        })
        .foreach_batch(f, BatchOptions::new());
    let r = p.run(s, fast("msg-stop").on_error(|_| {})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), r.done())
        .await
        .expect("it stops");
    assert_eq!(ns_all(&calls), [vec![1, 2]]);
    let third = &b.messages("orders").await[2];
    let rec = b.record("msg-stop").await.unwrap();
    assert_eq!(rec["position"], third["position"], "stopped at the message");
    assert_eq!(rec["stopped"]["reason"], "poison");
    assert_eq!(m(&r, "effects"), 2.0);
}

// ------------------------------------------------------------ windows and closes

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nothing_commits_while_a_window_result_waits_in_the_batch_not_even_a_tick() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    b.push("orders", "c1", json!({})).await;
    b.push_at(61_000.0, "orders", "c2", json!({})).await;
    let (calls, f) = recorder(ok);
    let r = p
        .run(
            per_minute(&p.queen).foreach_batch(f, BatchOptions::new().max_wait_millis(100_000)),
            fast("batch-tick").now(b.clock()),
        )
        .await
        .unwrap();
    until(3000, "minute 0 waiting", || async {
        m(&r, "pendingBatch") == 1.0
    })
    .await;
    let commits = m(&r, "commits");
    // Minute 1 is due by now on this clock: a stream without a waiting batch would tick.
    // (The runner's clock only: on this fake, moving the broker's would close minute 1 too.)
    b.local_at(125_000.0);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(m(&r, "ticks"), 0.0, "no tick while an item waits");
    assert_eq!(m(&r, "commits"), commits, "no commit");
    assert!(calls_of(&calls).is_empty());
    b.local_at(161_000.0);
    until(3000, "the call", || async { calls_of(&calls).len() == 1 }).await;
    assert_eq!(keys_counts(&calls_of(&calls)[0]), [(json!("c1"), json!(1))]);
    // The broker's clock past minute 1 (on this fake a write is stamped with the
    // broker's clock, so the commit after the call does not move it): minute 1
    // closes into the next batch.
    b.at(162_000.0).await;
    until(3000, "minute 1 waiting", || async {
        m(&r, "pendingBatch") == 1.0
    })
    .await;
    b.at(262_000.0).await;
    until(3000, "the second call", || async {
        calls_of(&calls).len() == 2
    })
    .await;
    assert_eq!(keys_counts(&calls_of(&calls)[1]), [(json!("c2"), json!(1))]);
    let prefix = output_prefix("batch-tick");
    assert!(calls_of(&calls)
        .concat()
        .iter()
        .all(|it| it.id.starts_with(&prefix) && it.id.len() == prefix.len() + 40));
    r.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_close_of_many_windows_at_once_goes_in_batches_of_max_items_with_commits_in_the_middle() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    let parts = 2500;
    let items: Vec<(String, String, Value)> = (0..parts)
        .map(|i| ("orders".to_string(), format!("p{i}"), json!({})))
        .collect();
    b.txs_one_each(&items).await;
    let (calls, f) = recorder(ok);
    let r = p
        .run(
            per_minute(&p.queen).foreach_batch(f, BatchOptions::new().max_items(1000)),
            fast("batch-mass").now(b.clock()),
        )
        .await
        .unwrap();
    until(10_000, "everything read and committed", || async {
        m(&r, "entriesRead") >= parts as f64
            && m(&r, "pendingBatch") == 0.0
            && m(&r, "commits") >= 1.0
    })
    .await;
    b.push_at(61_000.0, "other", "x", json!({})).await;
    until(10_000, "every result", || async {
        calls_of(&calls).concat().len() >= parts
    })
    .await;
    assert_eq!(
        calls_of(&calls).iter().map(|c| c.len()).collect::<Vec<_>>(),
        [1000, 1000, 500]
    );
    assert!(
        m(&r, "closeCommits") >= 1.0,
        "a commit in the middle of the close"
    );
    let ids: std::collections::HashSet<String> = calls_of(&calls)
        .concat()
        .into_iter()
        .map(|it| it.id)
        .collect();
    assert_eq!(ids.len(), parts, "every result its own id");
    r.stop().await;
}

// --------------------------------------------------------------------- the chain

#[test]
fn runs_on_stream_from_feed_only() {
    // A queue source has no .foreach_batch(): the compiler refuses it (see the
    // compile_fail example on FeedStream::foreach_batch), where JS fails run().
}

#[tokio::test]
async fn checks_its_options_and_its_place_at_the_end() {
    let q = Queen::connect_to("http://127.0.0.1:9").unwrap();
    let s = || Stream::from_feed(&q, FeedSource::new());
    let e = s()
        .foreach_batch(|_| async {}, BatchOptions::new().max_items(0))
        .chain_hash()
        .unwrap_err();
    assert!(
        e.to_string()
            .contains("maxItems is a whole number from 1, got 0"),
        "{e}"
    );
    let e = s()
        .foreach_batch(|_| async {}, BatchOptions::new())
        .map(|x, _| x.clone())
        .chain_hash()
        .unwrap_err();
    assert!(
        e.to_string().contains("must be the last in the chain"),
        "{e}"
    );
    assert!(e.to_string().contains(".foreachBatch"), "{e}");
    // The sink is not in the chain hash: changing max_items keeps the state.
    let a = s()
        .foreach_batch(|_| async {}, BatchOptions::new())
        .chain_hash()
        .unwrap();
    let b = s()
        .foreach_batch(|_| async {}, BatchOptions::new().max_items(7))
        .chain_hash()
        .unwrap();
    assert_eq!(a, b);
    // Without a sink, run() names it.
    let e = s()
        .map(|x, _| x.clone())
        .run(fast("no-sink"))
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains(".foreach(fn) or .foreachBatch(fn)"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_description_of_the_stream_names_the_sink_and_its_options() {
    let b = Fake::start();
    let p = b.proc();
    orders(&b, 1, 1).await;
    let r = p
        .run(
            Stream::from_feed(&p.queen, FeedSource::new().from(FeedFrom::Earliest)).foreach_batch(
                |_| async {},
                BatchOptions::new().max_items(50).max_wait_millis(250),
            ),
            fast("batch-about"),
        )
        .await
        .unwrap();
    until(3000, "the description", || async {
        b.record("batch-about#about").await.is_some()
    })
    .await;
    assert_eq!(
        b.record("batch-about#about").await.unwrap()["sink"],
        json!({ "kind": "foreachBatch", "maxItems": 50, "maxWaitMillis": 250 })
    );
    r.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shards_sum_batches_and_pending_batch() {
    let b = Fake::start();
    let p = b.proc();
    orders(&b, 1, 6).await;
    let (calls, f) = recorder(ok);
    let g = p
        .run(
            numbers(&p.queen).foreach_batch(f, BatchOptions::new()),
            fast("batch-shards").shards(2),
        )
        .await
        .unwrap();
    until(3000, "six delivered", || async {
        calls_of(&calls).concat().len() >= 6
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mm = g.metrics();
    let per: u64 = mm["perShard"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["batches"].as_u64().unwrap())
        .sum();
    assert_eq!(mm["batches"], per);
    assert!(per >= 1, "the shards' batches, summed");
    assert_eq!(mm["pendingBatch"], 0);
    assert_eq!(mm["effects"], 6);
    g.stop().await;
}
