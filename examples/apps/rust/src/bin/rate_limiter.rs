// docs:start(app-rust-rate-limiter)
//
// A rate limiter built from a streaming query.
//
// The textbook rate limiter counts requests per API key in a fixed window,
// usually with a counter in Redis, which is one more system to run and one
// more place where the count can drift from the requests.
//
// Here the counter is a windowed aggregation over the request queue itself.
// Each cycle of the stream commits the window state, the closed windows it
// emits and the ack of the requests it counted as one entry in the broker's
// log, so the count cannot drift from the requests, and it survives a restart
// of this process because the state is in the broker.
//
//   api-requests (one partition per API key)
//     └── streaming query: tumbling window, count per key
//           └── api-usage  -> the gate: over quota becomes a throttle decision
//                 └── api-throttled
//
// Run it:
//   QUEEN_URL=http://localhost:6632 cargo run --bin rate_limiter

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use queen_mq::streams::{RunOptions, Stream};
use queen_mq::{Config, Queen, QueueOptions, SubscriptionMode};
use serde_json::json;

const WINDOW_SECONDS: i64 = 2;
const QUOTA_PER_WINDOW: i64 = 5;

// Two tenants. One is a well behaved integration, the other is a runaway script
// someone left in a loop.
const QUIET_KEY: &str = "key-quiet";
const NOISY_KEY: &str = "key-noisy";
const QUIET_REQUESTS: i64 = 3;
const NOISY_REQUESTS: i64 = 20;

// Why those numbers make the check deterministic: a window is a slice of time,
// so a burst can land on either side of a boundary. Twenty requests split in
// any way at all leave at least ten on one side, which is over a quota of five,
// so the noisy key is always caught. Three requests cannot reach five however
// they are split, so the quiet key is never caught by accident.

const GATE_GROUP: &str = "rate-limiter-gate";

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
    let requests = format!("app-rust-api-requests-{run_id}");
    let usage = format!("app-rust-api-usage-{run_id}");
    let throttled = format!("app-rust-api-throttled-{run_id}");

    // The query id is this streaming query's identity in the broker. Its
    // window state is keyed by it, and the runner derives its consumer group
    // from it as `streams.{query_id}`.
    let query_id = format!("app-rust-rate-limiter-{run_id}");

    let mut checks = Checks(0);
    println!("broker {url}");

    let queen = Queen::connect(Config::new(&url)).map_err(|e| e.to_string())?;

    for q in [&requests, &usage, &throttled] {
        queen
            .queue(q)
            .configure(QueueOptions {
                lease_time: Some(30),
                retry_limit: Some(3),
                ..Default::default()
            })
            .await
            .map_err(|e| e.to_string())?;
    }

    // ------------------------------------------------------------- the counter
    //
    // The stream runs in this process, as a consumer group of its own. A new
    // group starts at the tail of the queue unless it asks otherwise, and a
    // request pushed while the stream was still starting would be missed, so it
    // asks for SubscriptionMode::All: every request in the queue is counted.
    //
    // The partition is the aggregation key, so the window state is per API key
    // without a word about keys here: the producer decides, by partition.
    //
    // Where the JavaScript client takes one options object for the window and
    // one for the aggregates, this client spells each of them as its own step in
    // the chain: window_tumbling, idle_flush_ms, then one aggregate_* per output
    // field.
    println!("\nstarting the counter");
    let counter = Stream::from(queen.queue(&requests))
        .window_tumbling(WINDOW_SECONDS)
        .idle_flush_ms(800)
        // aggregate_count is the count of records in the window. The extractors
        // receive a Record over the payload itself, not the envelope, so it is
        // r.number("cost") and not the message's `data` field. A missing or
        // non-numeric field yields None, so a request that carries no cost is
        // billed as one.
        .aggregate_count("requests")
        .aggregate_sum("cost", |r| Some(r.number("cost").unwrap_or(1.0)))
        .to(queen.queue(&usage))
        .run(
            &queen,
            RunOptions::new(&query_id)
                .batch_size(200)
                .max_partitions(8)
                .max_wait(Duration::from_millis(200))
                .subscription_mode(SubscriptionMode::All),
        )
        .await
        .map_err(|e| e.to_string())?;

    // ------------------------------------------------------------- the traffic
    println!("\ntaking traffic");
    for (key, n) in [(QUIET_KEY, QUIET_REQUESTS), (NOISY_KEY, NOISY_REQUESTS)] {
        for _ in 0..n {
            let at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            queen
                .queue(&requests)
                .partition(key)
                .push(json!({ "key": key, "path": "/v1/things", "cost": 1, "at": at }))
                .await
                .map_err(|e| e.to_string())?;
        }
        println!("  {key}: {n} requests");
    }

    // ---------------------------------------------------------------- the gate
    //
    // The enforcement point. It reads each closed window and turns the ones over
    // quota into throttle decisions. It is separate from the counter on purpose:
    // the counting is exact and stays the same, while the policy is yours and
    // changes on its own schedule.
    //
    // A window is a slice of time, so a burst can arrive as two windows. That is
    // why this accumulates per key and waits for the totals it expects, with a
    // deadline. Waiting for a quiet period would be a race: the last window
    // closes when its timer fires, whatever the reader is doing.
    println!("\nenforcing");
    let mut counted: HashMap<String, i64> = HashMap::new();
    let mut decisions: Vec<(String, i64)> = Vec::new();
    let complete = |counted: &HashMap<String, i64>| {
        counted.get(QUIET_KEY).copied().unwrap_or(0) == QUIET_REQUESTS
            && counted.get(NOISY_KEY).copied().unwrap_or(0) == NOISY_REQUESTS
    };
    let deadline = Instant::now() + Duration::from_secs(30);

    while !complete(&counted) && Instant::now() < deadline {
        // Each key's windows land in that key's partition. partitions(10) lets
        // one pop take the windows of both keys, with batch as the budget they
        // share.
        let windows = queen
            .queue(&usage)
            .group(GATE_GROUP)
            .subscription_mode(SubscriptionMode::All)
            .batch(50)
            .partitions(10)
            .wait(true)
            .poll_timeout(Duration::from_secs(2))
            .pop()
            .await
            .map_err(|e| e.to_string())?;

        for w in &windows {
            // The window's key is the partition it was computed for.
            let key = w.partition.clone();
            // The aggregates come back as JSON floating-point numbers (the
            // accumulator is an f64 whatever it counted), so `20` arrives as
            // `20.0` and as_i64() on it would be None. Read it as f64 and round.
            let in_window = w.data["requests"].as_f64().unwrap_or(0.0).round() as i64;
            *counted.entry(key.clone()).or_insert(0) += in_window;
            let over_by = in_window - QUOTA_PER_WINDOW;

            if over_by > 0 {
                // The decision is a message: whatever enforces it (an edge
                // worker, a gateway, the API itself) reads this queue and gets
                // the decisions in order, per key. It commits with the ack of
                // the window it came from, so a crash in between cannot lose a
                // decision or make two.
                queen
                    .transaction()
                    .push_to(
                        &throttled,
                        &key,
                        json!({
                            "key": key,
                            "window": in_window,
                            "quota": QUOTA_PER_WINDOW,
                            "overBy": over_by,
                        }),
                    )
                    .map_err(|e| e.to_string())?
                    .ack(w)
                    .commit()
                    .await
                    .map_err(|e| e.to_string())?;
                decisions.push((key.clone(), over_by));
                println!("  {key}: {in_window} in a window, over by {over_by}");
            } else {
                // pop() takes a lease and leaves the ack to the caller. This
                // client reads the consumer group and the lease id off the
                // message, so the ack cannot be pointed at the wrong cursor by
                // forgetting one.
                queen.ack(w).await.map_err(|e| e.to_string())?;
                println!("  {key}: {in_window} in a window, within quota");
            }
        }
    }

    // Stop the runner before checking, so nothing is still writing to the queues
    // the assertions read. stop() waits for the in-flight cycle and its flush,
    // and it consumes the handle: a stopped stream cannot be restarted by
    // mistake.
    counter.stop().await.map_err(|e| e.to_string())?;

    // --------------------------------------------------------------- checking
    println!("\nchecking");
    checks.assert(
        complete(&counted),
        "every request reached a closed window before the deadline",
    )?;
    checks.assert(
        counted.get(QUIET_KEY).copied().unwrap_or(0) == QUIET_REQUESTS,
        "the quiet key was counted exactly",
    )?;
    checks.assert(
        counted.get(NOISY_KEY).copied().unwrap_or(0) == NOISY_REQUESTS,
        "the noisy key was counted exactly",
    )?;

    checks.assert(!decisions.is_empty(), "the noisy key was throttled")?;
    checks.assert(
        decisions.iter().all(|(key, _)| key == NOISY_KEY),
        "the quiet key was never throttled, so the limiter is not just firing at everything",
    )?;

    // The decisions are readable by whatever enforces them, in order, per key.
    let gateway = queen
        .queue(&throttled)
        .batch(50)
        .partitions(10)
        .wait(true)
        .poll_timeout(Duration::from_secs(5))
        .pop()
        .await
        .map_err(|e| e.to_string())?;
    checks.assert(
        gateway.len() == decisions.len(),
        "every decision is on the queue the gateway reads",
    )?;
    checks.assert(
        gateway.iter().all(|m| {
            m.data["window"].as_i64().unwrap_or(0) > m.data["quota"].as_i64().unwrap_or(i64::MAX)
        }),
        "each decision carries the count and the quota that produced it",
    )?;

    // Clean up on success only: a failed run leaves the queues, and the query's
    // window state, on the broker to be looked at.
    for q in [&requests, &usage, &throttled] {
        queen.queue(q).delete().await.map_err(|e| e.to_string())?;
    }

    queen.close().await.map_err(|e| e.to_string())?;

    Ok(checks.0)
}
// docs:end
