// docs:start(app-rust-rate-limiter)
//
// A rate limiter, built out of a streaming query.
//
// Counting requests per API key in a fixed window is the textbook rate limiter,
// and the usual implementation is a counter in Redis: a second data system to
// run, to size, and to lose when it restarts.
//
// Here the counter is a windowed aggregation over the request stream itself.
// The window state, the decisions it emits and the acknowledgement of the
// requests it counted all commit in one PostgreSQL transaction, so the counter
// cannot drift from the stream it was computed from, and it survives a restart
// because it is a row rather than a process's memory.
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

    // The query id is this streaming query's identity in the database. Its
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
    // A stream is a running process: it has to be listening before the requests
    // arrive. Starting one over an existing backlog counts nothing.
    //
    // The partition is the aggregation key, so the window state is per API key
    // without anything being said about keys here: whoever pushes decides.
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
        // below receive a Record over the payload, not the envelope: it is
        // r.number("cost"), not the message's `data` field, and a missing or
        // non-numeric field yields None — so a request that carries no cost is
        // billed as one.
        .aggregate_count("requests")
        .aggregate_sum("cost", |r| Some(r.number("cost").unwrap_or(1.0)))
        .to(queen.queue(&usage))
        .run(
            &queen,
            RunOptions::new(&query_id)
                .batch_size(200)
                .max_partitions(8)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .map_err(|e| e.to_string())?;

    // run() registers the query and spawns the poll loop, but does not wait for
    // its first poll — and it is that first poll which creates the runner's
    // cursor. A new cursor starts at the tail, so requests pushed into the gap
    // would be counted by nobody. Waiting one poll window closes it.
    tokio::time::sleep(Duration::from_millis(500)).await;

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
    // quota into throttle decisions. Splitting it from the counter is
    // deliberate: the counting is exact and belongs to the broker, the policy is
    // yours and changes on a different schedule.
    //
    // A window is a slice of time, so a burst can arrive as two windows instead
    // of one. That is why this accumulates per key and waits for the totals it
    // expects, with a deadline. Waiting for a quiet period instead would be a
    // race: the last window closes when its timer says so, not when the reader
    // is tired of waiting.
    println!("\nenforcing");
    let mut counted: HashMap<String, i64> = HashMap::new();
    let mut decisions: Vec<(String, i64)> = Vec::new();
    let complete = |counted: &HashMap<String, i64>| {
        counted.get(QUIET_KEY).copied().unwrap_or(0) == QUIET_REQUESTS
            && counted.get(NOISY_KEY).copied().unwrap_or(0) == NOISY_REQUESTS
    };
    let deadline = Instant::now() + Duration::from_secs(30);

    while !complete(&counted) && Instant::now() < deadline {
        // A pop claims one partition unless you say otherwise, and the two keys
        // are two lanes: partitions(10) lets one call claim both, with batch as
        // the budget shared across them.
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
            // The aggregates come back as JSON floating-point numbers — the
            // accumulator is an f64 whatever it counted — so `20` arrives as
            // `20.0` and as_i64() on it would be None. Read it as f64 and round.
            let in_window = w.data["requests"].as_f64().unwrap_or(0.0).round() as i64;
            *counted.entry(key.clone()).or_insert(0) += in_window;
            let over_by = in_window - QUOTA_PER_WINDOW;

            if over_by > 0 {
                // The decision is a message, not a log line: whatever enforces
                // it (an edge worker, a gateway, the API itself) subscribes to
                // this queue and gets the decisions in order, per key.
                queen
                    .queue(&throttled)
                    .partition(&key)
                    .push(json!({
                        "key": key,
                        "window": in_window,
                        "quota": QUOTA_PER_WINDOW,
                        "overBy": over_by,
                    }))
                    .await
                    .map_err(|e| e.to_string())?;
                decisions.push((key.clone(), over_by));
                println!("  {key}: {in_window} in a window, over by {over_by}");
            } else {
                println!("  {key}: {in_window} in a window, within quota");
            }

            // pop() takes a lease and leaves it to you; only consume() settles
            // on your behalf. This client reads the consumer group and the lease
            // id off the message rather than taking them as arguments, so the
            // ack cannot be pointed at the wrong cursor by forgetting one.
            queen.ack(w).await.map_err(|e| e.to_string())?;
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
