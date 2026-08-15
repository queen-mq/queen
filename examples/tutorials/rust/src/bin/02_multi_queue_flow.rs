// docs:start(tut-rust-multi-queue-flow)
//
// Tutorial 2 of 5: a multi-queue flow.
//
// One queue partitioned per customer, two consumer groups reading it
// independently, and a second queue downstream. This is the shape most
// applications end up with, and it shows the three things that make it work:
// a partition keeps one entity's events in order, a consumer group is a cursor
// so every group sees everything, and a queue is created by the push.
//
//   orders (partition = customer)
//     ├── group "billing"    -> charges, and pushes to the shipping queue
//     └── group "analytics"  -> counts, and pushes nothing
//   shipping
//     └── group "warehouse"  -> ships
//
// Run it:
//   QUEEN_URL=http://localhost:6632 cargo run --bin 02_multi_queue_flow

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use queen_mq::{Config, Message, Queen, SubscriptionMode};
use serde_json::json;

// (orderId, customer, total)
const INPUT: [(&str, &str, f64); 5] = [
    ("A-1", "acme", 120.5),
    ("A-2", "acme", 12.0),
    ("B-1", "globex", 88.75),
    ("C-1", "initech", 310.0),
    ("A-3", "acme", 9.99),
];

struct Checks(usize);

impl Checks {
    fn assert(&mut self, condition: bool, description: &str) -> Result<(), String> {
        if !condition {
            return Err(description.to_string());
        }
        self.0 += 1;
        println!("  ok: {description}");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    match run().await {
        Ok(checks) => println!("\nPASS: {checks} checks"),
        Err(e) => {
            eprintln!("\nFAIL: {e}");
            std::process::exit(1);
        }
    }
}

async fn run() -> Result<usize, String> {
    let url = std::env::var("QUEEN_URL").unwrap_or_else(|_| "http://localhost:6632".into());
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let orders = format!("tut-rust-orders-{run_id}");
    let shipping = format!("tut-rust-shipping-{run_id}");

    let mut checks = Checks(0);
    println!("broker {url}");

    // Cheap to clone: every clone shares one connection pool and one set of
    // buffers, which is what lets the consume handlers below hold their own.
    let queen = Queen::connect(Config::new(&url)).map_err(|e| e.to_string())?;

    // Push each order into the partition named after its customer. Everything
    // about one customer stays in order; different customers never wait for
    // each other. The partition key is the only ordering decision you make.
    println!("\npushing");
    for (order_id, customer, total) in INPUT {
        queen
            .queue(&orders)
            .partition(customer)
            .push(json!({ "orderId": order_id, "customer": customer, "total": total }))
            .await
            .map_err(|e| e.to_string())?;
        println!("  {order_id} -> partition {customer}");
    }

    // Group one. It reads every order, charges it, and hands the paid ones to
    // the shipping queue. subscription_mode(All) matters: a group created after
    // the messages were pushed starts at the tail by default, so without it
    // this group would see nothing.
    //
    // consume() is the per-message loop — consume_batch() is the variant that
    // hands the whole claimed batch to one call. The handler is a plain async
    // closure; returning Ok acks the message, returning Err nacks it with the
    // error as the reason.
    //
    // idle() stops the loop after a stretch of silence, so a lost message fails
    // the run instead of hanging it. The check runs between polls, so with long
    // polling on, poll_timeout bounds how promptly it fires: a 30-second poll
    // window (the default) would notice a 5-second silence 30 seconds late.
    println!("\nbilling");
    let billed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let sink = Arc::clone(&billed);
        let producer = queen.clone();
        let shipping = shipping.clone();
        queen
            .queue(&orders)
            .group("tut-rust-billing")
            .subscription_mode(SubscriptionMode::All)
            .limit(INPUT.len() as u64)
            .poll_timeout(Duration::from_secs(1))
            .idle(Duration::from_secs(5))
            .consume(move |msg: Message| {
                let sink = Arc::clone(&sink);
                let producer = producer.clone();
                let shipping = shipping.clone();
                async move {
                    let order_id = msg.data["orderId"].as_str().unwrap_or_default().to_string();
                    let customer = msg.data["customer"].as_str().unwrap_or_default().to_string();
                    println!("  charged {order_id} ({})", msg.data["total"]);

                    // The push to the next queue creates it on first use,
                    // exactly like the first queue. Partitioning it by customer
                    // as well keeps a customer's shipments in the order their
                    // orders were charged.
                    producer
                        .queue(&shipping)
                        .partition(&customer)
                        .push(json!({ "orderId": order_id, "customer": customer }))
                        .await
                        .map_err(|e| e.to_string())?;

                    sink.lock().unwrap().push(order_id);
                    Ok::<_, String>(())
                }
            })
            .await
            .map_err(|e| e.to_string())?;
    }

    let billed = billed.lock().unwrap().clone();
    checks.assert(
        billed.len() == INPUT.len(),
        &format!("billing saw all {} orders", INPUT.len()),
    )?;

    // Group two reads the same stored messages through its own cursor. It was
    // not affected by billing acking them: that is what fan-out means here, and
    // it costs no extra copy of the data.
    println!("\nanalytics");
    let total = Arc::new(Mutex::new(0.0_f64));
    {
        let sink = Arc::clone(&total);
        queen
            .queue(&orders)
            .group("tut-rust-analytics")
            .subscription_mode(SubscriptionMode::All)
            .limit(INPUT.len() as u64)
            .poll_timeout(Duration::from_secs(1))
            .idle(Duration::from_secs(5))
            .consume(move |msg: Message| {
                let sink = Arc::clone(&sink);
                async move {
                    *sink.lock().unwrap() += msg.data["total"].as_f64().unwrap_or(0.0);
                    Ok::<_, String>(())
                }
            })
            .await
            .map_err(|e| e.to_string())?;
    }

    let counted = *total.lock().unwrap();
    let expected: f64 = INPUT.iter().map(|(_, _, t)| t).sum();
    checks.assert(
        (counted - expected).abs() < 0.001,
        "analytics summed every order, independently of billing",
    )?;

    // The order inside one partition is the order it was pushed in. Check the
    // customer with more than one order.
    println!("\nwarehouse");
    let acme_shipments: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let sink = Arc::clone(&acme_shipments);
        queen
            .queue(&shipping)
            .partition("acme")
            .group("tut-rust-warehouse")
            .subscription_mode(SubscriptionMode::All)
            .limit(3)
            .poll_timeout(Duration::from_secs(1))
            .idle(Duration::from_secs(5))
            .consume(move |msg: Message| {
                let sink = Arc::clone(&sink);
                async move {
                    let order_id = msg.data["orderId"].as_str().unwrap_or_default().to_string();
                    println!("  shipping {order_id}");
                    sink.lock().unwrap().push(order_id);
                    Ok::<_, String>(())
                }
            })
            .await
            .map_err(|e| e.to_string())?;
    }

    let acme_shipments = acme_shipments.lock().unwrap().clone();
    checks.assert(
        acme_shipments == ["A-1", "A-2", "A-3"],
        "one customer's shipments arrived in the order they were pushed",
    )?;

    queen
        .queue(&orders)
        .delete()
        .await
        .map_err(|e| e.to_string())?;
    queen
        .queue(&shipping)
        .delete()
        .await
        .map_err(|e| e.to_string())?;

    queen.close().await.map_err(|e| e.to_string())?;

    Ok(checks.0)
}
// docs:end
