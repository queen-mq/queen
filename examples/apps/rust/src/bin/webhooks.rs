// docs:start(app-rust-webhooks)
//
// A webhook sender: ordered per endpoint, retried by the broker, and
// dead-lettered with its error when it never succeeds.
//
// Deliveries to one endpoint have to arrive in the order the events happened,
// an endpoint that is down must not slow anybody else down, a failure is
// retried a bounded number of times, and what never succeeds has to end up
// somewhere a person can read it. Each endpoint gets a partition of its own,
// created by its first delivery, so a dead endpoint backs up its own partition
// and nothing else. Retries are the broker's retry budget, and an exhausted
// delivery lands in the dead-letter queue with the error attached.
//
//   webhook-deliveries (one partition per endpoint)
//     └── group "sender"  POSTs each delivery; an Err spends one retry
//           └── retry_limit spent -> dead-letter queue, with the error
//
// Run it:
//   QUEEN_URL=http://localhost:6632 cargo run --bin webhooks

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use queen_mq::{Config, Message, PushItem, Queen, QueueOptions, SubscriptionMode};
use serde_json::json;

const GROUP: &str = "sender";

// Three subscribers. One of them answers 500 to everything. It is listed first,
// so its deliveries are the oldest in the queue and its partition is usually
// handed out first: a sender that let a failing endpoint hold up the others
// would fail the checks below.
//
// (endpoint, healthy)
const ENDPOINTS: [(&str, bool); 3] = [
    ("initech.example", false),
    ("acme.example", true),
    ("globex.example", true),
];
const EVENTS_PER_ENDPOINT: i64 = 3;
const RETRY_LIMIT: i32 = 2;

fn healthy(endpoint: &str) -> bool {
    ENDPOINTS
        .iter()
        .find(|(name, _)| *name == endpoint)
        .map(|(_, ok)| *ok)
        .unwrap_or(false)
}

/// Stands in for the HTTP POST to the subscriber. A real sender calls an HTTP
/// client and returns Err for any status that is not 2xx, which is what this
/// does.
async fn post_to_endpoint(endpoint: &str) -> Result<(), String> {
    if !healthy(endpoint) {
        return Err(format!("{endpoint} answered 500"));
    }
    Ok(())
}

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

// Rust has no exceptions, so the shape the JavaScript gets from try/catch comes
// from `run` returning a Result: every `?` on the way down is a failed check or
// a failed call, and main turns it into FAIL and a non-zero exit.
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
    let deliveries = format!("app-rust-webhooks-{run_id}");

    let mut checks = Checks(0);
    println!("broker {url}");

    let queen = Queen::connect(Config::new(&url)).map_err(|e| e.to_string())?;

    // retry_limit is the delivery budget: a delivery that fails RETRY_LIMIT + 1
    // times is filed in the dead-letter queue with its last error, because
    // dlq_after_max_retries is on. lease_time is how long the broker waits for
    // a sender that took a delivery and never came back before it hands the
    // delivery to another sender.
    queen
        .queue(&deliveries)
        .configure(QueueOptions {
            lease_time: Some(30),
            retry_limit: Some(RETRY_LIMIT),
            dlq_after_max_retries: Some(true),
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?;

    // ------------------------------------------------------------------ queuing
    //
    // The application emits events. Each delivery goes into the partition of
    // the endpoint it is for, which is what makes "in order per subscriber" a
    // property of the storage instead of something the sender has to arrange.
    println!("\nqueuing deliveries");
    for seq in 1..=EVENTS_PER_ENDPOINT {
        for (endpoint, _) in ENDPOINTS {
            // The event id. An application that retries its own emit does not
            // create a second delivery. push() would mint a UUIDv7 id of its
            // own, so the item is built by hand and sent with push_items().
            queen
                .queue(&deliveries)
                .partition(endpoint)
                .push_items(vec![PushItem::new(
                    &deliveries,
                    json!({
                        "endpoint": endpoint,
                        "seq": seq,
                        "type": "invoice.paid",
                        "invoiceId": format!("INV-{seq}"),
                    }),
                )
                .partition(endpoint)
                .transaction_id(format!("{endpoint}-evt-{seq}"))])
                .await
                .map_err(|e| e.to_string())?;
        }
    }
    println!(
        "  {} deliveries queued",
        EVENTS_PER_ENDPOINT as usize * ENDPOINTS.len()
    );

    // ------------------------------------------------------------------ sending
    //
    // The sender pool. A handler that returns Ok acknowledges the delivery and
    // a handler that returns Err gives it back with the error: the broker
    // redelivers it and counts one retry. The retries live in the broker, so
    // they survive the sender dying halfway, which a retry loop inside the
    // handler would not.
    //
    // partitions(1): every pop takes ONE endpoint. After a failed delivery the
    // client skips the rest of that pop, so deliveries to other endpoints that
    // came in the same pop would wait for their lease to run out.
    println!("\nsending");
    let delivered_to: Arc<Mutex<HashMap<String, Vec<i64>>>> = Arc::new(Mutex::new(HashMap::new()));
    let attempts: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    // The broker numbers every delivery (deliveryAttempt in the pop response),
    // but this client's Message does not carry that field, so the sender counts
    // its own attempts at each event, by event id, for the log line.
    let tried: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(HashMap::new()));

    {
        let delivered_to = Arc::clone(&delivered_to);
        let attempts = Arc::clone(&attempts);
        let tried = Arc::clone(&tried);
        queen
            .queue(&deliveries)
            .group(GROUP)
            .subscription_mode(SubscriptionMode::All)
            .concurrency(3)
            .partitions(1)
            // Every healthy delivery once and every dead one RETRY_LIMIT + 1
            // times. This client counts limit() across the three workers, so
            // the pool stops on that count; idle() is the deadline behind it,
            // and long polls end after a second so it is noticed promptly. A
            // service runs without these three.
            .limit(
                (EVENTS_PER_ENDPOINT * 2 + EVENTS_PER_ENDPOINT * (RETRY_LIMIT as i64 + 1)) as u64,
            )
            .poll_timeout(Duration::from_secs(1))
            .idle(Duration::from_secs(3))
            .consume(move |msg: Message| {
                let delivered_to = Arc::clone(&delivered_to);
                let attempts = Arc::clone(&attempts);
                let tried = Arc::clone(&tried);
                async move {
                    let endpoint = msg.data["endpoint"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string();
                    let seq = msg.data["seq"].as_i64().unwrap_or(0);
                    *attempts
                        .lock()
                        .unwrap()
                        .entry(endpoint.clone())
                        .or_insert(0) += 1;
                    let attempt = {
                        let mut tried = tried.lock().unwrap();
                        let n = tried.entry(msg.transaction_id.clone()).or_insert(0);
                        *n += 1;
                        *n
                    };

                    // An Err becomes a nack carrying this string, which is what
                    // the dead letter shows once the budget runs out.
                    if let Err(e) = post_to_endpoint(&endpoint).await {
                        println!("  {endpoint} <- event {seq} failed on attempt {attempt}: {e}");
                        return Err(e);
                    }

                    delivered_to
                        .lock()
                        .unwrap()
                        .entry(endpoint.clone())
                        .or_default()
                        .push(seq);
                    println!("  {endpoint} <- event {seq}");
                    Ok::<_, String>(())
                }
            })
            .await
            .map_err(|e| e.to_string())?;
    }

    // ------------------------------------------------------------------ checking
    println!("\nchecking");

    let delivered_to = delivered_to.lock().unwrap().clone();
    let attempts = attempts.lock().unwrap().clone();

    for (endpoint, ok) in ENDPOINTS {
        if !ok {
            continue;
        }
        let seqs = delivered_to.get(endpoint).cloned().unwrap_or_default();
        let listed: Vec<String> = seqs.iter().map(i64::to_string).collect();
        let listed = if listed.is_empty() {
            "none".to_string()
        } else {
            listed.join(",")
        };
        checks.assert(
            seqs.len() == EVENTS_PER_ENDPOINT as usize,
            &format!(
                "{endpoint} received all {EVENTS_PER_ENDPOINT} events (got {})",
                seqs.len()
            ),
        )?;
        checks.assert(
            seqs == [1, 2, 3],
            &format!("{endpoint} received its events in the order they happened (got {listed})"),
        )?;
    }

    checks.assert(
        !delivered_to.contains_key("initech.example"),
        "the dead endpoint received nothing",
    )?;
    let dead_attempts = attempts.get("initech.example").copied().unwrap_or(0);
    checks.assert(
        dead_attempts == EVENTS_PER_ENDPOINT as usize * (RETRY_LIMIT as usize + 1),
        &format!(
            "each dead delivery was tried {} times before it was given up ({dead_attempts} attempts)",
            RETRY_LIMIT + 1
        ),
    )?;

    // The dead letters are records you can query. Each one keeps the payload,
    // so it names the endpoint and the invoice, and the last error, which is
    // what answers "why did this customer not get the webhook". In this client
    // the page size and the offset are the arguments of dlq(), the only two
    // filters the broker honours besides the queue and the group.
    let dlq = queen
        .queue(&deliveries)
        .dlq(Some(50), None)
        .await
        .map_err(|e| e.to_string())?;
    let dead: Vec<_> = dlq
        .messages
        .iter()
        .filter(|m| m.data["endpoint"] == json!("initech.example"))
        .collect();

    checks.assert(
        dead.len() == EVENTS_PER_ENDPOINT as usize,
        &format!("all {EVENTS_PER_ENDPOINT} dead deliveries are in the dead-letter queue"),
    )?;
    checks.assert(
        dead.iter()
            .all(|m| m.error.as_deref().unwrap_or("").contains("answered 500")),
        "each dead letter carries the error that killed it",
    )?;
    checks.assert(
        dlq.messages.len() == dead.len(),
        "no healthy delivery ended up in the dead-letter queue",
    )?;

    let listed: Vec<String> = dead
        .iter()
        .map(|m| {
            format!(
                "{}/{}: {}",
                m.data["endpoint"].as_str().unwrap_or("?"),
                m.data["invoiceId"].as_str().unwrap_or("?"),
                m.error.as_deref().unwrap_or("")
            )
        })
        .collect();
    println!("\n  dead letters: {}", listed.join("; "));

    // Clean up on success only: a failed run leaves the queue and its dead
    // letters on the broker to be looked at.
    queen
        .queue(&deliveries)
        .delete()
        .await
        .map_err(|e| e.to_string())?;

    queen.close().await.map_err(|e| e.to_string())?;

    Ok(checks.0)
}
// docs:end
