// The little book of QueenMQ: the examples of chapter 2 ("Writing applications")
// that are not already in the five-minute guide (00_five_minutes.rs), in Rust.
//
// The book shows the regions between `guide:start(<id>)` and `guide:end`,
// verbatim, minus lines ending in `guide:hide`. Everything outside them gives
// the program a clean start and checks that each example does what the book
// says it does.
//
// Run it against the node that the book's first section starts:
//   docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.8.1
//   cargo run --bin book_client_guide
// run.sh runs a copy with localhost:6632 replaced by $QUEEN_URL.

// guide:start(push-callbacks)
use queen_mq::PushStatus;
// guide:end
// guide:start(push-buffer)
use queen_mq::BufferOptions;
// guide:end
// guide:start(seek)
use queen_mq::SeekRequest;
// guide:end
// guide:start(consume-run)
use queen_mq::Cancel;
use tokio::signal::unix::{signal, SignalKind};
// guide:end
// guide:start(kv)
use queen_mq::Expiry;
// guide:end
// guide:start(stream-hourly)
use queen_mq::streams::ops::parse_iso_ms;
use queen_mq::streams::Every;
// guide:end
// guide:start(stream-gate)
use queen_mq::streams::{GateCtx, Record};
// guide:end

use queen_mq::feed::{FeedFrom, FeedOptions};
use queen_mq::streams::feed::{
    BatchOptions, FeedAggregate, FeedHandle, FeedRunOptions, FeedSink, FeedSource, Gather, Join,
    Pattern, ShardBy, TableJoin, ViewOptions,
};
use queen_mq::streams::{RunOptions, Stream};
use queen_mq::{
    AckStatus, Config, KvReason, Message, PushItem, Queen, QueueOptions, SubscriptionMode,
};

use serde_json::{json, Value};
use std::error::Error;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn check(ok: bool, what: &str) {
    if !ok {
        eprintln!("FAIL: {what}");
        std::process::exit(1);
    }
}

// Test bookkeeping for the feed streams.
const FEED_QUERIES: [&str; 11] = [
    "orders-it", "orders-by-country", "per-minute", "order-followups", "order-status", "batches",
    "bookings-priced", "bursts", "gh-unanswered", "clicks-per-tenant", "orders-to-warehouse",
];

async fn feed_push(queen: &Queen, queue: &str, partition: &str, data: Value) -> Result<(), Box<dyn Error>> {
    let id = format!("{queue}-{partition}-{}", queen_mq::uuid::uuidv7());
    queen
        .queue(queue)
        .push_items(vec![PushItem::new(queue, data).partition(partition).transaction_id(id)])
        .await?;
    Ok(())
}

/// The messages of a queue, read back through the feed; waits for `n` of them.
async fn messages_of(queen: &Queen, queue: &str, n: usize) -> Result<Vec<Value>, Box<dyn Error>> {
    let mut i = 0;
    loop {
        let mut reader = queen.feed(FeedOptions::new().queues([queue]).from(FeedFrom::Earliest).follow(false))?;
        let mut got = Vec::new();
        while let Some(e) = reader.next_entry().await? {
            got.push(e.data.clone());
        }
        if got.len() >= n || i >= 100 {
            return Ok(got);
        }
        i += 1;
        sleep_ms(100).await;
    }
}

async fn until(what: &str, cond: impl Fn() -> bool) {
    for _ in 0..100 {
        if cond() {
            return;
        }
        sleep_ms(100).await;
    }
    check(false, &format!("timed out waiting for {what}"));
}

/// A table elsewhere, written by key (an upsert): a second write of a row changes nothing.
#[derive(Clone, Default)]
struct Table {
    rows: Arc<Mutex<std::collections::HashMap<String, Value>>>,
}

impl Table {
    async fn upsert(&self, rows: Vec<Value>) -> Result<(), String> {
        let mut map = self.rows.lock().unwrap();
        for r in rows {
            map.insert(r["key"].as_str().unwrap_or_default().to_string(), r);
        }
        Ok(())
    }

    /// The order ids the table holds, sorted.
    fn orders(&self) -> Vec<i64> {
        let mut out: Vec<i64> = self.rows.lock().unwrap().values().filter_map(|r| r["id"].as_i64()).collect();
        out.sort();
        out
    }
}

/// The value of the first open window of `key` in a stream's step.
fn window_value(h: &FeedHandle, step: &str, key: &str) -> Value {
    h.state(step).get(key)["value"]["windows"][0]["value"].clone()
}

async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

// What the hidden lines of the regions record, for the checks.
static ARCHIVED: AtomicUsize = AtomicUsize::new(0);
static RAN: AtomicUsize = AtomicUsize::new(0);
static WIRE_DATA: Mutex<Option<Value>> = Mutex::new(None);
static SENT_SMS: Mutex<Vec<Value>> = Mutex::new(Vec::new());

// The helpers the examples call. In a real program they are yours.
struct SendError {
    permanent: bool,
    message: String,
}

async fn send_email(_email: &Value) -> Result<(), SendError> {
    Ok(())
}

async fn run_task(_task: &Value) -> Result<(), String> {
    RAN.fetch_add(1, Ordering::SeqCst);
    sleep_ms(5).await;
    Ok(())
}

async fn send_sms(sms: Value) -> Result<(), String> {
    SENT_SMS.lock().unwrap().push(sms);
    Ok(())
}

// guide:start(stream-gate)
// The Rust client has no tokenBucketGate helper; this is the same bucket. Its
// state is kept per partition in the broker, and saved only when a message passes.
fn token_bucket_gate(
    capacity: f64,
    refill_per_sec: f64,
) -> impl Fn(&Record, &mut GateCtx<'_>) -> bool + Send + Sync + 'static {
    move |_msg, ctx| {
        let now = ctx.stream_time_ms as f64;
        let elapsed = ((now - ctx.num("lastRefillAt", now)) / 1000.0).max(0.0);
        let tokens = (ctx.num("tokens", capacity) + elapsed * refill_per_sec).min(capacity);
        ctx.set_num("lastRefillAt", now);
        if tokens < 1.0 {
            return false; // refused: it comes back, in order, when its lease ends
        }
        ctx.set_num("tokens", tokens - 1.0);
        true
    }
}
// guide:end

// One request as it crossed the wire: where it went, its content type and
// body, and the content type of the answer.
#[derive(Debug, Clone)]
struct Exchange {
    target: String,
    sent: String,
    body: Vec<u8>,
    got: String,
}

/// A forward HTTP proxy that relays every exchange unchanged and records it,
/// to check the binary forms of the wire-forms example from the outside.
fn recording_proxy(log: Arc<Mutex<Vec<Exchange>>>) -> std::io::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    std::thread::spawn(move || {
        for conn in listener.incoming().flatten() {
            let log = log.clone();
            std::thread::spawn(move || {
                let _ = relay(conn, &log);
            });
        }
    });
    Ok(format!("http://{address}"))
}

fn read_request(conn: &mut TcpStream) -> std::io::Result<(String, Vec<u8>)> {
    let mut data = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        if let Some(i) = data.windows(4).position(|w| w == b"\r\n\r\n") {
            break i;
        }
        let n = conn.read(&mut chunk)?;
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        data.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&data[..head_end]).to_string();
    let length = header(&head, "content-length").parse::<usize>().unwrap_or(0);
    let mut body = data[head_end + 4..].to_vec();
    while body.len() < length {
        let n = conn.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    Ok((head, body))
}

fn header(head: &str, name: &str) -> String {
    head.split("\r\n")
        .filter_map(|line| line.split_once(':'))
        .find(|(n, _)| n.trim().eq_ignore_ascii_case(name))
        .map(|(_, v)| v.trim().to_string())
        .unwrap_or_default()
}

fn relay(mut client: TcpStream, log: &Mutex<Vec<Exchange>>) -> std::io::Result<()> {
    let (head, body) = read_request(&mut client)?;
    let mut lines = head.split("\r\n");
    let mut request = lines.next().unwrap_or_default().split(' ');
    let (method, target) = (request.next().unwrap_or("GET"), request.next().unwrap_or("/"));
    let rest = target.strip_prefix("http://").unwrap_or(target);
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a.to_string(), format!("/{p}")),
        None => (rest.to_string(), "/".to_string()),
    };
    let mut out = format!("{method} {path} HTTP/1.1\r\n");
    for line in lines.filter(|l| !l.is_empty()) {
        let name = line.split(':').next().unwrap_or_default().trim().to_ascii_lowercase();
        if !matches!(name.as_str(), "connection" | "proxy-connection" | "keep-alive") {
            out.push_str(line);
            out.push_str("\r\n");
        }
    }
    out.push_str("connection: close\r\n\r\n");
    let mut upstream = TcpStream::connect(authority.as_str())?;
    upstream.write_all(out.as_bytes())?;
    upstream.write_all(&body)?;
    let mut answer = Vec::new();
    upstream.read_to_end(&mut answer)?;
    let answer_head = match answer.windows(4).position(|w| w == b"\r\n\r\n") {
        Some(i) => String::from_utf8_lossy(&answer[..i]).to_string(),
        None => String::new(),
    };
    log.lock().unwrap().push(Exchange {
        target: path,
        sent: header(&head, "content-type"),
        body,
        got: header(&answer_head, "content-type"),
    });
    client.write_all(&answer)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let queen = Queen::connect(Config::new("http://localhost:6632"))?;
    let queues = [
        "jobs", "emails", "events", "reviews", "prices", "metrics", "tasks", "payments",
        "logins", "login-rate", "clicks", "sales", "sales-per-hour", "outgoing-sms",
        "invoices",
        // the feed streams' (end of this program)
        "orders", "orders-it", "orders-IT", "orders-FR", "per-minute", "order-followups", "shipments",
        "order-status", "parts", "batches", "bookings", "property-settings", "bookings-priced", "bursts",
        "gh-issues", "gh-comments", "gh-unanswered", "clicks-per-tenant",
    ];
    for q in queues {
        let _ = queen.queue(q).delete().await;
    }

    // --- Configure ------------------------------------------------------------
    // guide:start(configure-jobs)
    let options = QueueOptions {
        lease_time: Some(120),                  // 2 minutes per job, then to another worker
        retry_limit: Some(5),                   // after 5 failures, the dead-letter queue
        retry_delay: Some(10000),               // a failed job returns after 10 s, not at once
        delayed_processing: Some(30),           // a new job waits 30 s before anyone takes it
        window_buffer: Some(5),                 // a burst waits 5 s, then goes out as one
        dedup_window_seconds: Some(86400),      // a transactionId is remembered for a day
        retention_enabled: Some(true),          // retention runs only when this is on:
        completed_retention_seconds: Some(3600), //   jobs all groups finished go after an hour
        max_wait_time_seconds: Some(604800),    // older than a week: gone, finished or not
        ..Default::default()
    };
    queen.queue("jobs").configure(options).await?;
    // guide:end
    {
        let queue = queen.admin().queue("jobs").await?;
        let o = &queue["options"];
        check(o["retryDelay"] == json!(10000), &format!("retryDelay stored: {o}"));
        check(o["delayedProcessing"] == json!(30), "delayedProcessing stored");
        check(o["windowBuffer"] == json!(5), "windowBuffer stored");
        check(o["retentionEnabled"] == json!(true), "retentionEnabled stored");
        check(o["completedRetentionSeconds"] == json!(3600), "completedRetentionSeconds stored");
        check(o["maxWaitTimeSeconds"] == json!(604800), "maxWaitTimeSeconds stored");
        println!("ok: configure-jobs");
    }

    // --- Push -----------------------------------------------------------------
    // guide:start(push-callbacks)
    let results = queen
        .queue("emails")
        .push_items(vec![PushItem::new("emails", json!({ "to": "ada@example.com" }))
            .partition("customer-42")
            .transaction_id("welcome-42")])
        .await?; // an Err only when the whole request failed
    for item in &results {
        // every message gets its own answer
        match item.status {
            PushStatus::Queued => println!("stored {}", item.transaction_id),
            PushStatus::Duplicate => println!("already stored {}", item.transaction_id),
            _ => eprintln!("refused {}", item.transaction_id),
        }
    }
    // guide:end
    {
        check(results.len() == 1 && results[0].status == PushStatus::Queued, "first push queued");
        let again = queen
            .queue("emails")
            .push_items(vec![PushItem::new("emails", json!({ "to": "ada@example.com" }))
                .partition("customer-42")
                .transaction_id("welcome-42")])
            .await?;
        check(
            again.len() == 1 && again[0].status == PushStatus::Duplicate,
            "the second push of the same id answered duplicate",
        );
        check(again[0].transaction_id == "welcome-42", "for welcome-42");
        println!("ok: push-callbacks");
    }

    // guide:start(push-buffer)
    let buffer = BufferOptions {
        message_count: 100,                     // 100 at a time,
        time: Duration::from_millis(1000),      // or every second
        ..Default::default()
    };
    for i in 0..1000 {
        let device = format!("device-{}", i % 10);
        let reading = PushItem::new("events", json!({ "device": i % 10, "value": i }))
            .partition(&device)
            .transaction_id(format!("reading-{i}"));
        queen
            .queue("events")
            .partition(&device)                 // one buffer per partition
            .buffer(buffer)
            .push_items(vec![reading])
            .await?;
    }
    queen.flush_all_buffers().await?; // before exiting, send what is left
    // guide:end
    {
        let names: Vec<String> = (0..10).map(|i| format!("device-{i}")).collect();
        let read = queen.queue("events").fetch(&names).await?;
        let total: usize = read.values().map(|p| p.records.len()).sum();
        check(total == 1000, &format!("every buffered reading was stored ({total})"));
        println!("ok: push-buffer");
    }

    // Record what goes over the wire, to check the binary forms below: the
    // client made in the example dials through a recording proxy. The SDK reads
    // the proxy variables when a client is built, so only that client uses it.
    let wire: Arc<Mutex<Vec<Exchange>>> = Arc::default();
    let proxy = recording_proxy(wire.clone())?;
    for v in ["NO_PROXY", "no_proxy", "ALL_PROXY", "all_proxy", "http_proxy"] {
        std::env::remove_var(v);
    }
    std::env::set_var("HTTP_PROXY", &proxy);
    // guide:start(wire-forms)
    let fast = Queen::connect(
        Config::new("http://localhost:6632")
            .binary_push(true)      // a push travels as one binary body, not JSON
            .push_compression(true) // needs binary_push: zstd-compresses each partition's batch
            .framed_pop(true),      // a pop gets the records as stored, and unpacks them here
    )?;
    std::env::remove_var("HTTP_PROXY"); // guide:hide

    fast.queue("invoices")
        .push_items(vec![PushItem::new("invoices", json!({ "orderId": 2001, "total": 12.5 }))
            .partition("customer-42")
            .transaction_id("invoice-2001")])
        .await?;

    fast.queue("invoices")
        .limit(1)
        .consume(|msg: Message| async move {
            *WIRE_DATA.lock().unwrap() = Some(msg.data.clone()); // guide:hide
            println!("{}", msg.data); // {"orderId":2001,"total":12.5}, as with JSON
            Ok::<_, queen_mq::Error>(())
        })
        .await?;
    // guide:end
    {
        let wire = wire.lock().unwrap().clone();
        let push = wire.iter().find(|w| w.target.starts_with("/api/v1/push"));
        let pop = wire
            .iter()
            .find(|w| w.target.starts_with("/api/v1/pop/") && w.target.contains("frames=true"));
        check(push.is_some() && pop.is_some(), &format!("the proxy saw the push and the pop: {wire:?}"));
        let (push, pop) = (push.unwrap(), pop.unwrap());
        check(push.sent == "application/x-queen-push", "the push went as a binary body");
        let zstd_magic = [0x28u8, 0xb5, 0x2f, 0xfd];
        check(push.body.windows(4).any(|w| w == zstd_magic), "the binary push carried a zstd frame");
        check(pop.got == "application/x-queen-pop", "the broker answered the pop with frames");
        let data = WIRE_DATA.lock().unwrap().clone();
        check(data == Some(json!({ "orderId": 2001, "total": 12.5 })), "the data came back as pushed");
        println!("ok: wire-forms");
    }

    // --- Consume --------------------------------------------------------------
    // guide:start(consume-batch)
    queen
        .queue("events")
        .group("archiver")
        .subscription_mode(SubscriptionMode::All)
        .batch(100)                             // up to 100 messages per call
        .limit(1000)
        .consume_batch(|messages: Vec<Message>| async move {
            println!("archiving {}", messages.len()); // the handler gets the whole batch
            ARCHIVED.fetch_add(messages.len(), Ordering::SeqCst); // guide:hide
            Ok::<_, queen_mq::Error>(())
        })                                      // all acked on Ok, all nacked on an Err
        .await?;
    // guide:end
    check(ARCHIVED.load(Ordering::SeqCst) == 1000, "the batch handler saw every reading once");
    println!("ok: consume-batch");

    // guide:start(pop-ack)
    let messages = queen
        .queue("events")
        .group("auditor")
        .subscription_mode(SubscriptionMode::All)
        .batch(50)
        .wait(true) // long poll: wait for messages instead of returning empty
        .pop()
        .await?;
    println!("auditing {}", messages.len());
    queen.ack_all(&messages).await?; // the whole batch at once
    // guide:end
    check(!messages.is_empty() && messages.len() <= 50, "pop returned a batch");
    println!("ok: pop-ack");

    queen
        .queue("reviews")
        .push_items(vec![
            PushItem::new("reviews", json!({ "stars": 5 })).partition("shop-3").transaction_id("review-1"),
            PushItem::new("reviews", json!({ "stars": 1 })).partition("shop-3").transaction_id("review-2"),
        ])
        .await?;
    let reviews = queen
        .queue("reviews")
        .partition("shop-3")
        .group("moderation")
        .subscription_mode(SubscriptionMode::All)
        .batch(2)
        .wait(true)
        .pop()
        .await?;
    check(reviews.len() == 2, "both reviews popped");
    let (first, second) = (&reviews[0], &reviews[1]);
    // guide:start(ack-each-status)
    // One request, each message with its own answer: in the Rust client that is
    // a transaction of acks.
    queen
        .transaction()
        .ack_with(first, AckStatus::Completed)
        .ack_with_reason(second, AckStatus::Failed, "timeout")
        .commit()
        .await?;
    // guide:end
    {
        // A long poll already waiting is not woken when retryDelay passes (a broker
        // gap, reported); a new pop after the delay gets the message.
        let mut back = None;
        for _ in 0..5 {
            let got = queen
                .queue("reviews")
                .partition("shop-3")
                .group("moderation")
                .batch(2)
                .wait(true)
                .poll_timeout(Duration::from_millis(1500))
                .pop()
                .await?;
            if let Some(m) = got.into_iter().next() {
                back = Some(m);
                break;
            }
        }
        check(
            back.as_ref().map(|m| m.transaction_id.as_str()) == Some("review-2"),
            "only the failed review came back",
        );
        queen.ack(back.as_ref().unwrap()).await?;
        println!("ok: ack-each-status");
    }

    // guide:start(consume-callbacks)
    let client = queen.clone();
    queen
        .queue("emails")
        .group("mailer")
        .subscription_mode(SubscriptionMode::All)
        .limit(1)
        .auto_ack(false) // no callbacks in the Rust client: the handler settles each message
        .consume(move |msg: Message| {
            let client = client.clone();
            async move {
                let (status, error) = match send_email(&msg.data).await {
                    Ok(()) => (AckStatus::Completed, None),
                    Err(e) if e.permanent => (AckStatus::Dlq, Some(e.message)),
                    Err(e) => (AckStatus::Failed, Some(e.message)),
                };
                client.ack_with(&msg, status, error).await?;
                Ok::<_, queen_mq::Error>(())
            }
        })
        .await?;
    // guide:end
    {
        let left = queen.queue("emails").group("mailer").wait(false).pop().await?;
        check(left.is_empty(), "the handler acked the email");
        println!("ok: consume-callbacks");
    }

    for price in [10, 11, 12] {
        queen
            .queue("prices")
            .push_items(vec![PushItem::new("prices", json!({ "sku": "A-1", "price": price }))
                .partition("sku-A-1")
                .transaction_id(format!("price-A-1-{price}"))])
            .await?;
    }
    // guide:start(conflation)
    let newest = queen
        .queue("prices")
        .group("repricer")
        .subscription_mode(SubscriptionMode::All)
        .conflation(true)                       // only the newest message of each partition
        .wait(false)
        .pop()
        .await?;
    let latest = &newest[0];
    println!("{}", latest.data); // {"price":12,"sku":"A-1"}: 10 and 11 are skipped
    queen.ack(latest).await?;
    // guide:end
    {
        check(latest.data["price"] == json!(12), "conflation delivered the newest price");
        let again = queen.queue("prices").group("repricer").conflation(true).wait(false).pop().await?;
        check(again.is_empty(), "and moved past the older ones");
        println!("ok: conflation");
    }

    for i in 0..5 {
        queen
            .queue("metrics")
            .push_items(vec![PushItem::new("metrics", json!({ "load": i }))
                .partition("cpu")
                .transaction_id(format!("cpu-{i}"))])
            .await?;
    }
    // guide:start(commit-on-delivery)
    let readings = queen
        .queue("metrics")
        .group("live-chart")
        .subscription_mode(SubscriptionMode::All)
        .commit_on_delivery(true) // the cursor moves as they are handed out: no ack
        .batch(100)
        .wait(false)
        .pop()
        .await?;
    // guide:end
    {
        check(readings.len() == 5, "commitOnDelivery handed out every reading");
        let left = queen.queue("metrics").group("live-chart").wait(false).pop().await?;
        check(left.is_empty(), "and nothing is left to ack or redeliver");
        println!("ok: commit-on-delivery");
    }

    // guide:start(seek)
    let since = SeekRequest {
        timestamp: Some("2026-10-22T00:00:00Z".into()),
        position: None,
    };
    queen.admin().seek_consumer_group("archiver", "events", &since).await?;
    // the archiver group now reads again everything pushed since that time

    // SeekRequest has no toEnd in the Rust client: a time after the newest
    // message moves the cursor to the same place.
    let to_end = SeekRequest {
        timestamp: Some("9999-12-31T00:00:00Z".into()),
        position: None,
    };
    queen.admin().seek_consumer_group("archiver", "events", &to_end).await?;
    // and now it skips everything it has not read yet
    // guide:end
    {
        let left = queen.queue("events").group("archiver").wait(false).pop().await?;
        check(left.is_empty(), "after a seek to the end the group has nothing left");
        println!("ok: seek");
    }

    for i in 0..40 {
        queen
            .queue("tasks")
            .push_items(vec![PushItem::new("tasks", json!({ "n": i }))
                .partition(format!("account-{}", i % 8))
                .transaction_id(format!("task-{i}"))])
            .await?;
    }
    // guide:start(consume-run)
    let stop = Cancel::new();
    let on_sigterm = stop.clone();
    tokio::spawn(async move {                   // a deploy stops the worker cleanly
        if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
            sigterm.recv().await;
            on_sigterm.cancel();
        }
    });
    let watcher = stop.clone(); // guide:hide
    tokio::spawn(async move { while RAN.load(Ordering::SeqCst) < 40 { sleep_ms(20).await } watcher.cancel() }); // guide:hide

    queen
        .queue("tasks")
        .group("workers")
        .subscription_mode(SubscriptionMode::All)
        .concurrency(4)                         // four handlers at once, on four partitions
        .renew_lease(Duration::from_secs(10))   // long tasks keep their lease alive
        .idle(Duration::from_secs(60))          // or stop after a minute with nothing to do
        .poll_timeout(Duration::from_secs(5))   // a stop waits for the poll in flight (5 s)
        .poll_timeout(Duration::from_millis(500)) // guide:hide
        .cancel(stop)
        .consume(|task: Message| async move { run_task(&task.data).await })
        .await?;
    // guide:end
    check(RAN.load(Ordering::SeqCst) == 40, "four workers ran every task, then the signal stopped them");
    println!("ok: consume-run");

    // --- Retries and the dead-letter queue --------------------------------------
    queen
        .queue("payments")
        .configure(QueueOptions { retry_limit: Some(1), retry_delay: Some(100), ..Default::default() })
        .await?;
    queen
        .queue("payments")
        .push_items(vec![PushItem::new("payments", json!({ "amount": 25 }))
            .partition("order-7")
            .transaction_id("charge-7")])
        .await?;
    queen
        .queue("payments")
        .group("charger")
        .subscription_mode(SubscriptionMode::All)
        .limit(2)
        .idle(Duration::from_secs(5))
        .poll_timeout(Duration::from_millis(500))
        .consume(|_msg: Message| async move { Err::<(), _>("card declined") })
        .await?;
    sleep_ms(300).await;
    // guide:start(dlq-read)
    let dead = queen.queue("payments").group("charger").dlq(Some(20), None).await?;
    for letter in &dead.messages {
        let id = letter.transaction_id.as_deref().unwrap_or_default();
        let error = letter.error.as_deref().unwrap_or_default();
        // retryCount has no field of its own in the Rust client: it is in `rest`
        let retries = letter.rest.get("retryCount").cloned().unwrap_or_default();
        println!("{id} {error} {retries}");
    }
    // guide:end
    check(dead.messages.len() == 1, "the failing payment is in the dead-letter queue");
    check(dead.messages[0].error.as_deref() == Some("card declined"), "with its error");
    println!("ok: dlq-read");

    // guide:start(dlq-replay)
    for letter in &dead.messages {
        // back into its partition, and out of the dead-letter queue
        if let (Some(partition_id), Some(transaction_id)) =
            (&letter.partition_id, &letter.transaction_id)
        {
            queen.admin().retry_message(partition_id, transaction_id).await?;
        }
    }
    // guide:end
    {
        let again = queen
            .queue("payments")
            .group("charger")
            .batch(10)
            .wait(true)
            .poll_timeout(Duration::from_secs(3))
            .pop()
            .await?;
        check(again.len() == 1, "the replayed payment is delivered again");
        queen.ack_all(&again).await?;
        let left = queen.queue("payments").group("charger").dlq(None, None).await?;
        check(left.messages.is_empty(), "and it left the dead-letter queue");
        println!("ok: dlq-replay");
    }

    {
        // The four ack statuses the book's table describes, checked here. 'retry'
        // gives a message back at once without spending a retry; 'dlq' files it now.
        // The Rust client's Message carries no retry count, so the budget is checked
        // by spending it: with retryLimit 1 a 'failed' after the 'retry' still comes
        // back, which it would not if the 'retry' had spent the one retry.
        for (partition, id, amount) in [("order-8", "charge-8", 9), ("order-9", "charge-9", 4)] {
            queen
                .queue("payments")
                .push_items(vec![PushItem::new("payments", json!({ "amount": amount }))
                    .partition(partition)
                    .transaction_id(id)])
                .await?;
        }
        let order8 = || {
            queen
                .queue("payments")
                .partition("order-8")
                .group("statuses")
                .subscription_mode(SubscriptionMode::All)
                .batch(1)
                .wait(true)
                .poll_timeout(Duration::from_millis(1500))
        };
        let r1 = order8().pop().await?;
        check(r1.len() == 1, "the first delivery");
        queen.ack_with(&r1[0], AckStatus::Retry, None).await?;
        let r2 = order8().pop().await?;
        check(
            r2.len() == 1 && r2[0].transaction_id == r1[0].transaction_id,
            "'retry' redelivered the same message at once",
        );
        queen.ack_with(&r2[0], AckStatus::Failed, Some("try once more".into())).await?;
        let mut r3 = Vec::new();
        for _ in 0..5 {
            r3 = order8().pop().await?;
            if !r3.is_empty() {
                break;
            }
        }
        check(
            r3.len() == 1 && r3[0].transaction_id == r1[0].transaction_id,
            "'retry' spent no retry: the 'failed' after it still came back",
        );
        queen.ack_with(&r3[0], AckStatus::Dlq, Some("bad payload".into())).await?;
        let filed = queen.queue("payments").group("statuses").dlq(None, None).await?;
        check(filed.messages.len() == 1, "'dlq' filed the message in the dead-letter queue");
        check(filed.messages[0].error.as_deref() == Some("bad payload"), "with its reason");
        println!("ok: ack statuses");
    }

    // --- KV -------------------------------------------------------------------
    queen.kv().delete("carts", "customer-42").send().await?;
    queen.kv().delete("limits", "api:customer-42").send().await?;
    // guide:start(kv)
    // Every write names its lifetime: a ttl, or forever.
    queen
        .kv()
        .put("carts", "customer-42", json!({ "items": 3 }), Expiry::seconds(7 * 86400))
        .send()
        .await?;
    let cart = queen.kv().get("carts", "customer-42").await?; // found, value, version, ...

    // Write only if nobody wrote since we read it: the version is the condition.
    let saved = queen
        .kv()
        .put("carts", "customer-42", json!({ "items": 4 }), Expiry::seconds(7 * 86400))
        .expect(cart.version.unwrap_or(0))
        .send()
        .await?;
    println!("{}", saved.applied()); // false if the version moved

    // A counter with a ceiling: past max, the increment does not apply.
    let hit = queen
        .kv()
        .incr("limits", "api:customer-42", 1, Expiry::seconds(60))
        .max(100)
        .send()
        .await?;
    if !hit.applied() {
        println!("over the limit"); // reason: KvReason::Limit
    }
    // guide:end
    {
        check(cart.found(), "the cart was read back");
        check(cart.value == Some(json!({ "items": 3 })), &format!("the cart: {:?}", cart.value));
        check(saved.applied(), &format!("the conditional write applied: {:?}", saved.reason));
        let stale = queen
            .kv()
            .put("carts", "customer-42", json!({ "items": 9 }), Expiry::seconds(7 * 86400))
            .expect(cart.version.unwrap_or(0))
            .send()
            .await?;
        check(!stale.applied(), "a write from the old version is refused");
        check(stale.reason == Some(KvReason::Version), &format!("reason: {:?}", stale.reason));
        check(
            stale.value == Some(json!({ "items": 4 })),
            &format!("and answers the current value: {:?}", stale.value),
        );
        check(hit.applied(), &format!("the first increment applied: {:?}", hit.reason));
        let capped = queen
            .kv()
            .incr("limits", "api:customer-42", 100, Expiry::seconds(60))
            .max(100)
            .send()
            .await?;
        check(!capped.applied(), "an increment past max does not apply");
        check(capped.reason == Some(KvReason::Limit), &format!("reason: {:?}", capped.reason));
        check(capped.counter() == Ok(1), &format!("and the counter keeps its value: {:?}", capped.value));
        println!("ok: kv");
    }

    // --- Streams ----------------------------------------------------------------
    // guide:start(stream-sliding)
    let login_rate = Stream::from(queen.queue("logins")) // one partition per user
        .window_sliding(600, 60)                // the last 10 minutes, every minute
        .aggregate_count("count")
        .to(queen.queue("login-rate"))
        .run(&queen, RunOptions::new("login-rate"))
        .await?;
    // guide:end
    sleep_ms(1200).await;
    login_rate.stop().await?;
    println!("ok: stream-sliding");

    // guide:start(stream-session)
    let visits = Stream::from(queen.queue("clicks")) // one partition per visitor
        .window_session(1800)                   // a visit ends after 30 minutes of quiet
        .aggregate_count("count")
        .foreach(|visit, _| async move {
            println!("visit ended {visit}");
            Ok(())
        })                                      // acked after this returns
        .run(&queen, RunOptions::new("visits"))
        .await?;
    // guide:end
    sleep_ms(1200).await;
    visits.stop().await?;
    println!("ok: stream-session");

    // guide:start(stream-hourly)
    let hourly = Stream::from(queen.queue("sales"))
        .window_cron(Every::Hour)               // on the clock: 10:00 to 11:00, 11:00 to 12:00
        // when the sale happened, not when it arrived
        .event_time(|sale| parse_iso_ms(sale.data["soldAt"].as_str()?))
        .allowed_lateness(120)                  // a sale up to 2 minutes late still counts
        .aggregate_count("count")
        .aggregate_sum("sum", |sale| sale.number("amount"))
        .aggregate_max("max", |sale| sale.number("amount"))
        .to(queen.queue("sales-per-hour"))
        .run(&queen, RunOptions::new("sales-per-hour").subscription_mode(SubscriptionMode::All))
        .await?;
    // guide:end
    sleep_ms(1200).await;
    hourly.stop().await?;
    println!("ok: stream-hourly");

    // guide:start(stream-gate)
    let limiter = Stream::from(queen.queue("outgoing-sms")) // one partition per customer
        .gate(token_bucket_gate(10.0, 1.0))     // 10 at once, then 1 a second
        .foreach(|sms, _| async move { send_sms(sms).await })
        .run(&queen, RunOptions::new("sms-limiter"))
        .await?;
    // guide:end
    sleep_ms(1000).await;
    let suffix = queen_mq::uuid::uuidv7();
    for i in 0..3 {
        queen
            .queue("outgoing-sms")
            .push_items(vec![PushItem::new("outgoing-sms", json!({ "to": "+390000000", "text": format!("hello {i}") }))
                .partition("customer-9")
                .transaction_id(format!("sms-{suffix}-{i}"))])
            .await?;
    }
    for _ in 0..100 {
        if SENT_SMS.lock().unwrap().len() >= 3 {
            break;
        }
        sleep_ms(100).await;
    }
    limiter.stop().await?;
    let sent = SENT_SMS.lock().unwrap().len();
    check(sent == 3, &format!("the gate let the first three messages through ({sent})"));
    println!("ok: stream-gate");

    // --- Streams from the tenant feed (broker 2.8.0) --------------------------------
    // A feed stream keeps its position and state in KV (`queen-feed`, under its
    // queryId): an earlier run's go first, so every run starts the same way.
    for id in FEED_QUERIES {
        let page = queen.kv().get_prefix("queen-feed", id).keys_only().limit(1000).send().await?;
        for row in page.rows() {
            queen.kv().delete("queen-feed", &row.key).send().await?;
        }
    }

    // guide:start(feed-route)
    let orders_it = Stream::from_feed(&queen, FeedSource::new().queues(["orders"]))
        .where_("data.country", "=", json!("IT"))
        .filter(|m, _| m["data"]["total"].as_f64().unwrap_or(0.0) >= 10.0)
        .to(queen.queue("orders-it"))
        .run(FeedRunOptions::new("orders-it"))
        .await?;
    // guide:end
    feed_push(&queen, "orders", "order-1", json!({ "id": 1, "country": "IT", "total": 20 })).await?;
    feed_push(&queen, "orders", "order-2", json!({ "id": 2, "country": "IT", "total": 5 })).await?;
    feed_push(&queen, "orders", "order-3", json!({ "id": 3, "country": "DE", "total": 30 })).await?;
    let got = messages_of(&queen, "orders-it", 1).await?;
    check(got == vec![json!({ "id": 1, "country": "IT", "total": 20 })], &format!("only the Italian order of 10 or more: {got:?}"));
    orders_it.stop().await;
    println!("ok: feed-route");

    // guide:start(feed-routing)
    let by_country = Stream::from_feed(&queen, FeedSource::new().queues(["orders"]))
        .to(FeedSink::route(|order: Option<Value>, _msg: Value| async move {
            json!(format!("orders-{}", order.unwrap_or_default()["country"].as_str().unwrap_or("xx")))
        }))
        .run(FeedRunOptions::new("orders-by-country"))
        .await?;
    // guide:end
    feed_push(&queen, "orders", "order-4", json!({ "id": 4, "country": "IT", "total": 12 })).await?;
    feed_push(&queen, "orders", "order-5", json!({ "id": 5, "country": "FR", "total": 8 })).await?;
    check(messages_of(&queen, "orders-IT", 1).await? == vec![json!({ "id": 4, "country": "IT", "total": 12 })], "routed to orders-IT");
    check(messages_of(&queen, "orders-FR", 1).await? == vec![json!({ "id": 5, "country": "FR", "total": 8 })], "routed to orders-FR");
    by_country.stop().await;
    println!("ok: feed-routing");

    let warehouse = Table::default();
    let table = warehouse.clone();
    // guide:start(feed-batch)
    let to_warehouse = Stream::from_feed(&queen, FeedSource::new().queues(["orders"]))
        .foreach_batch(
            move |items| {
                let table = table.clone();
                async move {
                    // One write for the batch. An item's id is the same each time it is delivered.
                    let rows = items.iter().map(|it| {
                        let order = it.value.clone().unwrap_or_default();
                        json!({ "key": it.id, "id": order["id"], "total": order["total"] })
                    });
                    table.upsert(rows.collect()).await
                }
            },
            BatchOptions::new().max_items(500).max_wait_millis(1000),
        )
        .run(FeedRunOptions::new("orders-to-warehouse"))
        .await?;
    // guide:end
    feed_push(&queen, "orders", "order-8", json!({ "id": 8, "total": 20 })).await?;
    feed_push(&queen, "orders", "order-9", json!({ "id": 9, "total": 30 })).await?;
    until("the batch", || warehouse.orders().len() == 2).await;
    check(warehouse.orders() == vec![8, 9], "orders 8 and 9 in the warehouse");
    to_warehouse.stop().await;
    println!("ok: feed-batch");

    // guide:start(feed-window)
    let per_minute = Stream::from_feed(&queen, FeedSource::new().queues(["orders", "payments"]))
        .window_tumbling(60.0)
        .aggregate(FeedAggregate::new().count("count").field("sum", |e: &Value| e["total"].clone()))
        .map(|totals, w| {
            let mut out = totals.clone();
            out["customer"] = w.map_or(Value::Null, |w| w["key"].clone());
            out["minute"] = w.map_or(Value::Null, |w| w["windowKey"].clone());
            out
        })
        .to(queen.queue("per-minute"))
        .run(FeedRunOptions::new("per-minute"))
        .await?;
    // guide:end
    feed_push(&queen, "orders", "customer-1", json!({ "total": 10 })).await?;
    feed_push(&queen, "payments", "customer-1", json!({ "total": 5 })).await?;
    // The window closes at the end of its minute; until then the stream shows it open.
    until("the open window", || window_value(&per_minute, "step1", "customer-1")["count"] == 2).await;
    check(window_value(&per_minute, "step1", "customer-1") == json!({ "count": 2, "sum": 15 }), "the window's totals");
    per_minute.stop().await;
    println!("ok: feed-window");

    // guide:start(feed-pattern)
    let followups = Stream::from_feed(&queen, FeedSource::new().queues(["orders", "payments"]))
        .pattern(
            Pattern::new(|m| m["queue"] == "orders", |m| m["queue"] == "payments", "15m")
                .on_match(|order, payment| json!({ "order": order["data"]["id"], "paid": payment["data"]["amount"] }))
                .on_timeout(|order| json!({ "order": order["data"]["id"], "remind": true })),
        )
        .to(queen.queue("order-followups"))
        .run(FeedRunOptions::new("order-followups"))
        .await?;
    // guide:end
    feed_push(&queen, "orders", "order-7", json!({ "id": 7 })).await?;
    feed_push(&queen, "payments", "order-7", json!({ "amount": 30 })).await?;
    check(messages_of(&queen, "order-followups", 1).await? == vec![json!({ "order": 7, "paid": 30 })], "the order and its payment matched");
    followups.stop().await;
    println!("ok: feed-pattern");

    // guide:start(feed-join)
    let order_status = Stream::from_feed(&queen, FeedSource::new().queues(["orders", "payments", "shipments"]))
        .join(
            Join::sides("2d")
                .side_queue("order", "orders")
                .side_queue("paid", "payments")
                .side_queue("sent", "shipments")
                .select_all(|got, order| json!({ "order": order, "carrier": got["sent"]["data"]["carrier"] }))
                .on_timeout_all(|_got, missing, order| json!({ "order": order, "missing": missing })),
        )
        .to(queen.queue("order-status"))
        .run(FeedRunOptions::new("order-status"))
        .await?;
    // guide:end
    feed_push(&queen, "orders", "order-9", json!({ "id": 9 })).await?;
    feed_push(&queen, "payments", "order-9", json!({ "amount": 5 })).await?;
    feed_push(&queen, "shipments", "order-9", json!({ "carrier": "dhl" })).await?;
    check(messages_of(&queen, "order-status", 1).await? == vec![json!({ "order": "order-9", "carrier": "dhl" })], "the three sides joined");
    order_status.stop().await;
    println!("ok: feed-join");

    // guide:start(feed-gather)
    let batches = Stream::from_feed(&queen, FeedSource::new().queues(["parts"]))
        .gather(
            Gather::count("10m", |m, _| m["data"]["of"].clone())
                .key(|m, _| m["data"]["batch"].clone())
                .on_complete(|got, batch| json!({ "batch": batch, "parts": got.as_array().map_or(0, |g| g.len()) }))
                .on_timeout(|_got, missing, batch| json!({ "batch": batch, "missing": missing })),
        )
        .to(queen.queue("batches"))
        .run(FeedRunOptions::new("batches"))
        .await?;
    // guide:end
    for i in 0..3 {
        feed_push(&queen, "parts", &format!("part-{i}"), json!({ "batch": "b-1", "of": 3 })).await?;
    }
    check(messages_of(&queen, "batches", 1).await? == vec![json!({ "batch": "b-1", "parts": 3 })], "the batch is complete");
    batches.stop().await;
    println!("ok: feed-gather");

    feed_push(&queen, "property-settings", "p-1", json!({ "currency": "EUR" })).await?;
    // guide:start(feed-table)
    let priced = Stream::from_feed(&queen, FeedSource::new().queues(["bookings"]))
        .join_table(TableJoin::new("property-settings", |m, _| m["data"]["propertyId"].clone()).select(
            |booking, settings, _| {
                let mut out = booking.cloned().unwrap_or_default();
                out["currency"] = settings.map_or(Value::Null, |s| s["currency"].clone());
                out
            },
        ))
        .to(queen.queue("bookings-priced"))
        .run(FeedRunOptions::new("bookings-priced"))
        .await?;
    // guide:end
    feed_push(&queen, "bookings", "booking-1", json!({ "id": "booking-1", "propertyId": "p-1" })).await?;
    feed_push(&queen, "bookings", "booking-2", json!({ "id": "booking-2", "propertyId": "p-2" })).await?;
    check(
        messages_of(&queen, "bookings-priced", 2).await?
            == vec![
                json!({ "id": "booking-1", "propertyId": "p-1", "currency": "EUR" }),
                json!({ "id": "booking-2", "propertyId": "p-2", "currency": null }),
            ],
        "each booking with its property's currency, as of the booking",
    );
    priced.stop().await;
    println!("ok: feed-table");

    // guide:start(feed-steps)
    let bursts = Stream::from_feed(&queen, FeedSource::new().queues(["clicks"]))
        .window_tumbling(60.0)
        .aggregate(FeedAggregate::new().count("clicks"))
        .name("perMinute")
        .pattern(
            Pattern::new(
                |m| m["data"]["clicks"].as_f64().unwrap_or(0.0) >= 100.0,
                |m| m["data"]["clicks"].as_f64().unwrap_or(0.0) < 100.0,
                "10m",
            )
            .on_match(|burst, _calm| json!({ "tenant": burst["partition"], "peak": burst["data"]["clicks"] })),
        )
        .to(queen.queue("bursts"))
        .run(FeedRunOptions::new("bursts"))
        .await?;
    // guide:end
    for _ in 0..3 {
        feed_push(&queen, "clicks", "tenant-42", json!({ "page": "/" })).await?;
    }
    until("three clicks in the minute", || window_value(&bursts, "perMinute", "tenant-42")["clicks"] == 3).await;
    println!("ok: feed-steps");

    // guide:start(feed-event-time)
    let unanswered = Stream::from_feed(&queen, FeedSource::new().queues(["gh-issues", "gh-comments"]).from(FeedFrom::Earliest))
        .pattern(
            Pattern::new(|m| m["queue"] == "gh-issues", |m| m["queue"] == "gh-comments", "1h")
                .key(|m, _| json!(format!("{}#{}", m["partition"].as_str().unwrap_or(""), m["data"]["number"])))
                .event_time(|m| m["data"]["created_at"].clone())
                .allowed_lateness("5m")
                .idle("1m")
                .on_match(|_issue, _comment| None::<Value>) // an answered issue sends nothing
                .on_timeout(|issue| json!({ "repo": issue["partition"], "issue": issue["data"]["number"] })),
        )
        .to(queen.queue("gh-unanswered"))
        .run(FeedRunOptions::new("gh-unanswered"))
        .await?;
    // guide:end
    {
        // Event times as epoch milliseconds here (an ISO string or a Date works too).
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as i64;
        // The comment arrives first; on event time it still answers the issue.
        feed_push(&queen, "gh-comments", "queen-mq/queen", json!({ "number": 1, "created_at": now + 60_000 })).await?;
        feed_push(&queen, "gh-issues", "queen-mq/queen", json!({ "number": 1, "created_at": now })).await?;
        // Both wait in the step, in event-time order, for allowed_lateness.
        until("the two held messages", || unanswered.metrics()["steps"][0]["held"] == 2).await;
    }
    unanswered.stop().await;
    println!("ok: feed-event-time");

    // guide:start(feed-shards)
    let per_tenant = Stream::from_feed(&queen, FeedSource::new().queues(["clicks"]))
        .key_by(|m| m["data"]["tenant"].clone())
        .window_tumbling(60.0)
        .aggregate(FeedAggregate::new().count("clicks"))
        .to(queen.queue("clicks-per-tenant"))
        .run(FeedRunOptions::new("clicks-per-tenant").shards(4).shard_by(ShardBy::key(|m| m["data"]["tenant"].clone())))
        .await?;
    // guide:end
    check(per_tenant.metrics()["running"] == 4, "four shards, all in this process");
    for tenant in ["acme", "globex", "acme"] {
        feed_push(&queen, "clicks", &format!("visitor-{tenant}"), json!({ "tenant": tenant })).await?;
    }
    until("a window per tenant", || per_tenant.metrics()["open"] == 2).await;
    per_tenant.stop().await;
    println!("ok: feed-shards");

    // guide:start(feed-view)
    let view = Stream::view(&queen, "bursts", ViewOptions::default())?;
    let answer = view.get("perMinute", "tenant-42").await?; // { found, value, shard, position }
    let page = view.list("perMinute", 50).await?; // { items, truncated }
    let shards = view.status().await?; // role, position, lag and open state per shard
    // guide:end
    check(answer["found"] == true, &format!("the view found the window: {answer}"));
    check(answer["value"]["windows"][0]["value"] == json!({ "clicks": 3 }), "the live window, asked from outside the process");
    check(page["items"].as_array().map_or(0, |i| i.len()) >= 1, "the view listed the step");
    check(shards.len() == 1 && shards[0]["role"] == "active", &format!("one active runner: {shards:?}"));
    bursts.stop().await;
    println!("ok: feed-view");

    queen.close().await?;
    println!("\nPASS");
    Ok(())
}
