// Queen in five minutes: the program behind the homepage guide, in Rust.
//
// The homepage shows the regions between `guide:start(<step>)` and `guide:end`,
// verbatim, minus lines ending in `guide:hide`. Everything outside them gives
// the program a clean start and checks that each step did what the page says.
//
// Run it against the node that step 1 of the guide starts:
//   docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.6.0
//   cargo run --bin 00_five_minutes
// run.sh runs a copy with localhost:6632 replaced by $QUEEN_URL.

// guide:start(configure)
use queen_mq::{Config, Queen, QueueOptions};
// guide:end
// guide:start(push)
use queen_mq::PushItem;
// guide:end
// guide:start(consume)
use queen_mq::Message;
// guide:end
// guide:start(consume-group)
use queen_mq::{AckStatus, SubscriptionMode};
// guide:end
// guide:start(transaction)
use queen_mq::{Expiry, TxnPushItem};
// guide:end
// guide:start(stream)
use queen_mq::streams::{RunOptions, Stream};
// guide:end

use serde_json::json;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn check(ok: bool, what: &str) {
    if !ok {
        eprintln!("FAIL: {what}");
        std::process::exit(1);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let admin = Queen::connect(Config::new("http://localhost:6632"))?;
    for q in ["orders", "shipments", "reminders", "reports", "revenue-per-minute"] {
        let _ = admin.queue(q).delete().await;
    }

    // guide:start(configure)
    let queen = Queen::connect(Config::new("http://localhost:6632"))?;

    queen
        .queue("orders")
        .namespace("shop") // groups queues in the dashboard
        .configure(QueueOptions {
            lease_time: Some(30),           // seconds a worker holds a message before it is redelivered
            retry_limit: Some(3),           // failures before the message goes to the dead-letter queue
            retention_seconds: Some(86400), // keep messages for a day
            ..Default::default()
        })
        .await?;
    // guide:end
    println!("ok: configure");

    // guide:start(push)
    queen
        .queue("orders")
        .push_items(vec![PushItem::new("orders", json!({ "orderId": 1001, "customer": 42, "total": 59.9 }))
            .partition("customer-42")      // one ordered partition per customer, created by this push
            .transaction_id("order-1001")]) // the dedup key: pushing the same id again is a no-op
        .await?;
    // guide:end
    println!("ok: push");

    // guide:start(consume)
    queen
        .queue("orders")
        .limit(1) // stop after one message; a worker leaves this out
        .consume(|msg: Message| async move {
            println!("{}", msg.data); // acked when this returns Ok, retried on an Err
            Ok::<_, queen_mq::Error>(())
        })
        .await?;
    // guide:end
    let left = queen.queue("orders").wait(false).pop().await?;
    check(left.is_empty(), "queue mode acked the order");
    println!("ok: consume");

    let billed: Arc<Mutex<Vec<i64>>> = Arc::default();
    let sink = billed.clone();
    // guide:start(consume-group)
    let client = queen.clone();
    queen
        .queue("orders")
        .group("billing")                     // every group gets every message
        .subscription_mode(SubscriptionMode::All) // a new group starts at the tail unless told otherwise
        .batch(100)                           // up to 100 messages per pop...
        .partitions(10)                       // ...from up to 10 partitions
        .renew_lease(Duration::from_secs(2))  // heartbeat every 2 s while the handler runs
        .auto_ack(false)                      // the handler decides the outcome
        .limit(1)
        .consume(move |msg: Message| {
            let client = client.clone();
            let sink = sink.clone(); // guide:hide
            async move {
                let ok = msg.data["total"].as_f64().unwrap_or(0.0) > 0.0;
                let status = if ok { AckStatus::Completed } else { AckStatus::Failed };
                client.ack_with(&msg, status, None).await?;
                sink.lock().unwrap().push(msg.data["orderId"].as_i64().unwrap_or(0)); // guide:hide
                Ok::<_, queen_mq::Error>(())
            }
        })
        .await?;
    // guide:end
    check(*billed.lock().unwrap() == vec![1001], "the billing group read the order pushed before it existed");
    println!("ok: consume-group");

    // guide:start(transaction)
    let client = queen.clone();
    queen
        .queue("orders")
        .group("fulfillment")
        .subscription_mode(SubscriptionMode::All)
        .auto_ack(false)
        .limit(1)
        .consume(move |msg: Message| {
            let client = client.clone();
            async move {
                let id = &msg.data["orderId"];
                client
                    .transaction()
                    .ack(&msg) // the input is done
                    .kv_put("orders", &format!("order-{id}"), json!({ "status": "paid" }),
                        Expiry::Seconds(7 * 86400))? // the order's state
                    .push_item(TxnPushItem { // the next event, same customer
                        queue: "shipments".into(),
                        partition: Some(msg.partition.clone()),
                        payload: json!({ "orderId": id }),
                        transaction_id: Some(format!("ship-{id}")),
                        trace_id: None,
                    })?
                    .schedule("reminders", &format!("review-{id}"), Duration::from_secs(86400), // a message tomorrow
                        json!({ "orderId": id, "customer": msg.data["customer"] }).to_string())
                    .commit()
                    .await?;
                Ok::<_, queen_mq::Error>(())
            }
        })
        .await?;
    // guide:end
    let state = queen.kv().get("orders", "order-1001").await?;
    check(state.value == Some(json!({ "status": "paid" })), "the KV write committed");
    let shipped = queen.queue("shipments").wait(false).pop().await?;
    check(shipped.len() == 1, "the shipment was pushed");
    check(shipped[0].partition == "customer-42", "into the same customer partition");
    println!("ok: transaction");

    let replayed: Arc<Mutex<Vec<i64>>> = Arc::default();
    let sink = replayed.clone();
    // guide:start(replay)
    queen
        .queue("orders")
        .group("audit")                            // a new group, with its own cursor
        .subscription_from("2026-10-22T00:00:00Z") // everything pushed from this time on
        .limit(1)
        .consume(move |msg: Message| {
            let sink = sink.clone(); // guide:hide
            async move {
                println!("again: {}", msg.data);
                sink.lock().unwrap().push(msg.data["orderId"].as_i64().unwrap_or(0)); // guide:hide
                Ok::<_, queen_mq::Error>(())
            }
        })
        .await?;
    // guide:end
    check(*replayed.lock().unwrap() == vec![1001], "the audit group read the order again");
    println!("ok: replay");

    // guide:start(fetch)
    let read = queen.queue("orders").fetch(["customer-42"]).await?; // no group, lease or ack
    for record in &read["customer-42"].records {
        println!("{} {}", record.offset, record.payload);
    }
    // guide:end
    check(read["customer-42"].records.first().map(|r| r.payload["orderId"] == json!(1001)) == Some(true), "fetch read the partition");
    println!("ok: fetch");

    // guide:start(lock)
    let lock = queen.lock("daily-report", Duration::from_secs(30)); // a lease with a fencing token
    if lock.acquire().await? {                                      // false: another worker has it
        let committed = queen
            .transaction()
            .guard(&lock) // commits only while the lock is ours
            .push_item(TxnPushItem {
                queue: "reports".into(),
                partition: None,
                payload: json!({ "day": "2026-10-22" }),
                transaction_id: Some("report-2026-10-22".into()),
                trace_id: None,
            })?
            .commit()
            .await;
        lock.release().await?;
        committed?;
    }
    // guide:end
    let reports = queen.queue("reports").wait(false).pop().await?;
    check(reports.len() == 1, "the guarded report was pushed");
    println!("ok: lock");

    // guide:start(stream)
    let revenue = Stream::from(queen.queue("orders")) // one window per partition: per customer
        .window_tumbling(60)
        .aggregate_count("count")
        .aggregate_sum("sum", |order| order.number("total"))
        .to(queen.queue("revenue-per-minute")) // one result per customer per minute
        .run(&queen, RunOptions::new("revenue-per-minute"))
        .await?;
    // guide:end
    tokio::time::sleep(Duration::from_secs(1)).await; // let the stream's group register at the tail
    let suffix = queen_mq::uuid::uuidv7();
    queen
        .queue("orders")
        .push_items(vec![PushItem::new("orders", json!({ "orderId": 1002, "customer": 42, "total": 40.1 }))
            .partition("customer-42")
            .transaction_id(format!("order-1002-{suffix}"))])
        .await?;
    let mut windows = Vec::new();
    for _ in 0..100 {
        windows = queen.queue("revenue-per-minute").wait(true).poll_timeout(Duration::from_secs(1)).pop().await?;
        if !windows.is_empty() {
            break;
        }
    }
    revenue.stop().await?;
    check(windows.len() == 1, "the window closed and its result was pushed");
    check(windows[0].data["count"].as_f64() == Some(1.0) && windows[0].data["sum"].as_f64() == Some(40.1), &format!("window totals: {}", windows[0].data));
    println!("ok: stream {}", windows[0].data);

    let client = queen.clone();
    let worker = tokio::spawn(async move {
        let queen = client;
        // guide:start(worker)
        let eph = queen.ephemeral();
        let batch = eph.pop("price-requests").group("pricers").wait(true).send().await?;
        for m in &batch.messages {
            let reply_to = m.payload["replyTo"].as_str().unwrap_or_default();
            eph.push(reply_to, json!({ "sku": m.payload["sku"], "price": 59.9 })).send().await?;
        }
        eph.ack(&batch).await?;
        // guide:end
        Ok::<_, queen_mq::Error>(())
    });
    tokio::time::sleep(Duration::from_millis(300)).await; // the worker is already waiting when a caller asks

    // guide:start(caller)
    let eph = queen.ephemeral();
    let inbox = format!("inbox-{}", queen_mq::uuid::uuidv7()); // a private reply queue
    eph.push("price-requests", json!({ "sku": "A-1", "replyTo": inbox })).send().await?;
    let reply = eph.pop(&inbox).wait(true).poll_timeout(Duration::from_secs(5))
        .commit_on_delivery(true).send().await?;
    println!("{}", reply.messages[0].payload); // {"price":59.9,"sku":"A-1"}
    // guide:end
    worker.await??;
    check(reply.messages[0].payload["price"] == json!(59.9), "the reply came back");
    println!("ok: ephemeral");

    println!("\nPASS");
    Ok(())
}
