// docs:start(full-rust-produce-consume)
//
// Queen MQ by example, 1 of 3: the smallest complete loop.
//
// Create a queue, push a handful of orders, consume them with a consumer group,
// acknowledge each one, and verify that every message arrived exactly once.
//
// Run it:
//   QUEEN_URL=http://localhost:6699 cargo run --bin produce-consume
//
// The program checks its own outcome and exits non-zero if any check fails.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use queen_mq::{Config, Message, PushStatus, Queen, QueueOptions};
use serde_json::json;

// A consumer group is a named cursor over the queue. Every group sees every
// message; acking moves that group's cursor forward and affects no other group.
const GROUP: &str = "ex-rust-shipping";

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
    let url = std::env::var("QUEEN_URL").unwrap_or_else(|_| "http://localhost:6699".into());

    // The name is prefixed per language so the examples can share a broker, and
    // suffixed per run so a second run cannot inherit anything from the first.
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let queue = format!("ex-rust-produce-consume-{run_id}");

    let orders = vec![
        json!({ "orderId": "A-1", "customer": "acme",    "total": 120.5 }),
        json!({ "orderId": "A-2", "customer": "acme",    "total": 12.0 }),
        json!({ "orderId": "B-1", "customer": "globex",  "total": 88.75 }),
        json!({ "orderId": "B-2", "customer": "globex",  "total": 4.2 }),
        json!({ "orderId": "C-1", "customer": "initech", "total": 310.0 }),
    ];

    let mut checks = Checks(0);
    println!("broker {url}");

    // Signal handlers are opt-in in this client, so nothing is installed here
    // and this program owns its own shutdown.
    let queen = Queen::connect(Config::new(&url)).map_err(|e| e.to_string())?;

    // configure() sends the whole configuration and is a full replace: keys you
    // leave out go back to the broker's defaults rather than keeping a previous
    // value.
    queen
        .queue(&queue)
        .configure(QueueOptions {
            // How long a popped message stays invisible to other consumers
            // before the broker assumes the consumer died and hands it on.
            lease_time: Some(30),
            retry_limit: Some(3),
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?;
    println!("queue {queue} created");

    println!("\npushing");
    // push_many takes anything serializable. The client mints a UUIDv7
    // transaction id for every item, and that id is the key the broker
    // deduplicates on.
    let pushed = queen
        .queue(&queue)
        .push_many(orders.clone())
        .await
        .map_err(|e| e.to_string())?;
    for r in &pushed {
        println!("  {} -> {:?}", r.transaction_id, r.status);
    }
    checks.assert(
        pushed.len() == orders.len(),
        &format!("broker answered for all {} items", orders.len()),
    )?;
    checks.assert(
        pushed.iter().all(|r| r.status == PushStatus::Queued),
        "every item was accepted as queued",
    )?;

    println!("\nconsuming");
    let received: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&received);
    let acker = queen.clone();

    queen
        .queue(&queue)
        .group(GROUP)
        .batch(10) // up to 10 messages per poll
        .auto_ack(false) // acknowledge by hand below, so the commit is visible
        .limit(orders.len() as u64) // stop once this many have been handled
        .wait(false)
        .idle(Duration::from_secs(5)) // and stop anyway after 5s of silence, so a
        // lost message fails the run instead of hanging it
        .consume(move |msg: Message| {
            let sink = Arc::clone(&sink);
            let acker = acker.clone();
            async move {
                println!(
                    "  {} ({}) from partition {}",
                    msg.data["orderId"], msg.data["customer"], msg.partition
                );

                // The acknowledgement is the commit. It moves this group's
                // cursor past the message and releases the lease; until it lands
                // the message is only on loan and would be redelivered when the
                // lease expires. The group and the lease are read off the
                // message, so they cannot be passed wrong.
                let ack = acker.ack(&msg).await.map_err(|e| e.to_string())?;

                // A rejected ack still arrives as HTTP 200 with success: false on
                // the item, so the per-item flag is the only proof the broker
                // took it.
                if !ack.success {
                    return Err(format!("ack rejected: {:?}", ack.error));
                }
                sink.lock().unwrap().push(msg);
                Ok::<_, String>(())
            }
        })
        .await
        .map_err(|e| e.to_string())?;

    println!("\nchecking");
    let received = received.lock().unwrap().clone();
    checks.assert(
        received.len() == orders.len(),
        &format!("consumed {} messages", orders.len()),
    )?;

    let arrived: Vec<&str> = received
        .iter()
        .filter_map(|m| m.data["orderId"].as_str())
        .collect();
    let unique: std::collections::HashSet<&&str> = arrived.iter().collect();
    checks.assert(unique.len() == arrived.len(), "no message was delivered twice")?;
    checks.assert(
        orders
            .iter()
            .all(|o| arrived.contains(&o["orderId"].as_str().unwrap())),
        "every pushed order arrived at least once",
    )?;

    // The cursor for this group is now past every message, so a further poll on
    // the same group finds nothing. wait(false) makes it return immediately.
    let leftovers = queen
        .queue(&queue)
        .group(GROUP)
        .batch(10)
        .wait(false)
        .pop()
        .await
        .map_err(|e| e.to_string())?;
    checks.assert(
        leftovers.is_empty(),
        &format!("nothing is left for group {GROUP}"),
    )?;

    // Clean up. Only on success, so a failed run leaves the queue on the broker.
    queen.queue(&queue).delete().await.map_err(|e| e.to_string())?;

    // close() flushes any client-side push buffers. This program uses none, but
    // running it is the habit that keeps a buffered one honest.
    queen.close().await.map_err(|e| e.to_string())?;

    Ok(checks.0)
}
// docs:end
