// The little book of QueenMQ: the use cases of chapter 3 ("Real applications
// examples"), in Rust.
//
// The book shows the regions between `guide:start(<id>)` and `guide:end`,
// verbatim, minus lines ending in `guide:hide`. Everything outside them gives
// the program a clean start and checks that each use case does what the book
// says it does.
//
// Run it against a node:
//   docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.7.0
//   cargo run --bin book_use_cases
// run.sh runs a copy with localhost:6632 replaced by $QUEEN_URL.

// guide:start(uc-rate)
use queen_mq::streams::{GateCtx, Record};
// guide:end
// guide:start(uc-webhook)
use queen_mq::Cancel;
use tokio::signal::unix::{signal, SignalKind};
// guide:end

use queen_mq::streams::{RunOptions, Stream};
use queen_mq::{
    Config, Expiry, Message, PushItem, Queen, QueueOptions, SubscriptionMode, TimerStatus,
    TxnPushItem,
};

use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn check(ok: bool, what: &str) {
    if !ok {
        eprintln!("FAIL: {what}");
        std::process::exit(1);
    }
}

async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

// What the hidden lines of the regions record, for the checks.
static DONE: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());
static SEEN: Mutex<Vec<(String, i64)>> = Mutex::new(Vec::new());
static CALLS: AtomicUsize = AtomicUsize::new(0);
static QUESTION: Mutex<String> = Mutex::new(String::new());
static VERDICTS: Mutex<Vec<bool>> = Mutex::new(Vec::new());
static SENT: Mutex<Vec<(i64, u128)>> = Mutex::new(Vec::new());
static T4: Mutex<Option<Instant>> = Mutex::new(None);

fn seen(group: &str, event: &Message) {
    let order = event.data["orderId"].as_i64().unwrap_or(0);
    SEEN.lock().unwrap().push((group.to_string(), order));
}

// The helpers the examples call. In a real program they are yours.
async fn resize(photo: &str, worker: &str) {
    DONE.lock().unwrap().push((photo.to_string(), worker.to_string()));
    sleep_ms(50).await;
}

async fn send_sms(sms: Value) -> Result<(), String> {
    let at = T4.lock().unwrap().map(|t| t.elapsed().as_millis()).unwrap_or(0);
    SENT.lock().unwrap().push((sms["n"].as_i64().unwrap_or(0), at));
    Ok(())
}

// guide:start(uc-rate)
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

// A webhook receiver for the uc-webhook check: it fails the first call, and
// keeps each idempotency key once.
#[derive(Default)]
struct Receiver {
    calls: usize,
    received: BTreeMap<String, String>,
}

fn start_receiver(state: Arc<Mutex<Receiver>>) -> std::io::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    std::thread::spawn(move || {
        for conn in listener.incoming().flatten() {
            let _ = answer(conn, &state);
        }
    });
    Ok(format!("http://127.0.0.1:{port}/hooks"))
}

fn answer(mut conn: TcpStream, state: &Mutex<Receiver>) -> std::io::Result<()> {
    let (head, body) = read_request(&mut conn)?;
    let status = {
        let mut s = state.lock().unwrap();
        s.calls += 1;
        if s.calls == 1 {
            "500 Internal Server Error"
        } else {
            let key = header(&head, "idempotency-key");
            s.received.entry(key).or_insert_with(|| String::from_utf8_lossy(&body).to_string());
            "200 OK"
        }
    };
    conn.write_all(
        format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").as_bytes(),
    )
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

// --- 9. A job only one worker runs (lock + guard) ---------------------------
async fn nightly_worker(queen: Queen) -> queen_mq::Result<&'static str> {
    // guide:start(uc-singleton)
    let lock = queen.lock("nightly-report", Duration::from_secs(30));
    if !lock.acquire_within(Duration::from_secs(1)).await? {
        return Ok("skipped"); // another worker has it
    }
    let day = "2026-10-24";
    let committed = queen
        .transaction()
        .guard(&lock) // commits only while the lock is ours
        .push_item(TxnPushItem {
            queue: "daily-reports".into(),
            partition: None,
            payload: json!({ "day": day, "bookings": 412 }),
            transaction_id: Some(format!("report-{day}")),
            trace_id: None,
        })?
        .commit()
        .await;
    sleep_ms(1500).await; // guide:hide
    lock.release().await?; // whatever happened, give the lock back
    if !committed?.success {
        return Ok("lost the lock");
    }
    Ok("done")
    // guide:end
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let queen = Queen::connect(Config::new("http://localhost:6632"))?;
    let queues = [
        "thumbnails", "order-placed", "chat", "order-events", "shipping", "trace-events",
        "traces", "sms-out", "availability", "review-reminders", "webhooks", "daily-reports",
    ];
    for q in queues {
        let _ = queen.queue(q).delete().await;
    }
    let _ = queen.kv().delete("orders", "order-2001").send().await;
    let _ = queen.ephemeral().delete("quotes").await;

    // --- Work queue: competing consumers ---------------------------------------
    // guide:start(uc-work)
    // Twelve photos to resize, from four users.
    for i in 1..=12 {
        queen
            .queue("thumbnails")
            .partition(format!("user-{}", i % 4))
            .push(json!({ "photo": format!("p{i}.jpg") }))
            .await?;
    }

    // Three workers compete for the jobs: each job goes to exactly one of them.
    let worker = |name: &'static str| {
        let jobs = queen
            .queue("thumbnails")
            .partitions(1)                      // one user's photos at a time
            .idle(Duration::from_millis(1500)); // stop when there is nothing left
        async move {
            jobs.consume(move |job: Message| async move {
                resize(job.data["photo"].as_str().unwrap_or_default(), name).await;
                Ok::<_, queen_mq::Error>(())
            })
            .await
        }
    };

    tokio::try_join!(worker("w1"), worker("w2"), worker("w3"))?;
    // guide:end
    {
        let done = DONE.lock().unwrap().clone();
        check(done.len() == 12, &format!("twelve jobs handled ({})", done.len()));
        let photos: HashSet<&String> = done.iter().map(|(p, _)| p).collect();
        check(photos.len() == 12, "each job once");
        let mut shares: BTreeMap<&String, usize> = BTreeMap::new();
        for (_, w) in &done {
            *shares.entry(w).or_default() += 1;
        }
        check(shares.len() >= 2, &format!("the jobs were shared ({shares:?})"));
        println!("ok: uc-work {}", json!(shares));
    }

    // --- Publish/subscribe and fan-out: consumer groups ------------------------
    // guide:start(uc-pubsub)
    let subscribe = |group: &'static str| {
        let events = queen
            .queue("order-placed")
            .idle(Duration::from_millis(1000)) // guide:hide
            .group(group)                       // each group gets every message
            .subscription_mode(SubscriptionMode::All);
        async move {
            events
                .consume(move |event: Message| async move {
                    seen(group, &event); // guide:hide
                    println!("{group} {}", event.data["orderId"]);
                    Ok::<_, queen_mq::Error>(())
                })
                .await
        }
    };

    for order_id in [3001, 3002, 3003] {
        queen
            .queue("order-placed")
            .partition(format!("order-{order_id}"))
            .push(json!({ "orderId": order_id }))
            .await?;
    }
    tokio::try_join!(subscribe("billing"), subscribe("shipping"))?;

    // Months later, analytics subscribes too, and reads every order ever placed.
    subscribe("analytics").await?;
    // guide:end
    {
        let seen = SEEN.lock().unwrap().clone();
        for g in ["billing", "shipping", "analytics"] {
            let mut got: Vec<i64> = seen.iter().filter(|(s, _)| s == g).map(|(_, o)| *o).collect();
            got.sort();
            check(got == vec![3001, 3002, 3003], &format!("{g} got every order ({got:?})"));
        }
        // A group that does not ask for 'all' starts at the end of the queue.
        let late = queen.queue("order-placed").group("late").wait(false).batch(10).pop().await?;
        check(late.is_empty(), "a new group without subscriptionMode all skips the history");
        println!("ok: uc-pubsub");
    }

    // --- 1. Chat processing: a burst is one batch (windowBuffer) ---------------
    // guide:start(uc-chat)
    // A burst waits 2 seconds, then goes to the handler as one batch.
    queen
        .queue("chat")
        .configure(QueueOptions { window_buffer: Some(2), ..Default::default() })
        .await?;
    let t0 = Instant::now(); // guide:hide

    // The guest sends three messages in a row.
    for text in ["Hi!", "Is breakfast included?", "And is there parking?"] {
        queen.queue("chat").partition("conv-42").push(json!({ "text": text })).await?;
    }

    // The assistant answers the whole burst once.
    queen
        .queue("chat")
        .group("assistant")
        .subscription_mode(SubscriptionMode::All)
        .batch(10)                              // the handler gets every message of the burst
        .limit(3) // guide:hide
        .consume_batch(|messages: Vec<Message>| async move {
            CALLS.fetch_add(1, Ordering::SeqCst); // guide:hide
            let question = messages
                .iter()
                .filter_map(|m| m.data["text"].as_str())
                .collect::<Vec<_>>()
                .join(" ");
            *QUESTION.lock().unwrap() = question.clone(); // guide:hide
            println!("{question}"); // Hi! Is breakfast included? And is there parking?
            Ok::<_, queen_mq::Error>(())
        })
        .await?;
    // guide:end
    {
        let waited = t0.elapsed().as_millis();
        check(CALLS.load(Ordering::SeqCst) == 1, "the three messages reached the handler in one call");
        check(
            *QUESTION.lock().unwrap() == "Hi! Is breakfast included? And is there parking?",
            "the handler saw the whole burst, in order",
        );
        check(waited >= 1500, &format!("the burst was held for the window ({waited} ms)"));
        println!("ok: uc-chat (one call, after {waited} ms)");
    }

    // --- 2. Order workflow: transitions that cannot race (expect) --------------
    queen
        .kv()
        .put("orders", "order-2001", json!({ "status": "paid" }), Expiry::seconds(30 * 86400))
        .send()
        .await?;
    queen
        .queue("order-events")
        .push_items(vec![PushItem::new("order-events", json!({ "orderId": "order-2001" }))
            .partition("order-2001")
            .transaction_id("order-2001-paid")])
        .await?;
    // guide:start(uc-order)
    let client = queen.clone();
    queen
        .queue("order-events")
        .group("workflow")
        .subscription_mode(SubscriptionMode::All)
        .auto_ack(false)
        .limit(1) // guide:hide
        .consume(move |msg: Message| {
            let queen = client.clone();
            async move {
                let order_id = msg.data["orderId"].as_str().unwrap_or_default().to_string();
                let order = queen.kv().get("orders", &order_id).await?; // {"status":"paid"}
                if order.value.as_ref().and_then(|v| v["status"].as_str()) != Some("paid") {
                    queen.ack(&msg).await?; // already moved: just ack
                    return Ok(());
                }

                // Do the work of this step here: charge the card, book the courier, ...

                let thirty_days = Expiry::seconds(30 * 86400);
                let shipping = queen
                    .kv()
                    .put("orders", &order_id, json!({ "status": "shipping" }), thirty_days)
                    .expect(order.version.unwrap_or(0)) // only from what we read
                    .required()
                    .operation()?;
                let result = queen
                    .transaction()
                    .ack(&msg)
                    .kv(shipping)
                    .push_item(TxnPushItem {
                        queue: "shipping".into(),
                        partition: Some(order_id.clone()),
                        payload: json!({ "orderId": order_id }),
                        transaction_id: Some(format!("ship-{order_id}")),
                        trace_id: None,
                    })?
                    .commit()
                    .await?;
                VERDICTS.lock().unwrap().push(result.success); // guide:hide
                if !result.success {
                    return Ok(()); // moved by someone else: nothing written
                }
                Ok::<_, queen_mq::Error>(())
            }
        })
        .await?;
    // guide:end
    {
        check(*VERDICTS.lock().unwrap() == vec![true], "the step committed once");
        let order = queen.kv().get("orders", "order-2001").await?;
        check(order.value == Some(json!({ "status": "shipping" })), "the order moved to shipping");
        // The same transition from the old version is refused, and writes nothing.
        let stale = queen
            .kv()
            .put("orders", "order-2001", json!({ "status": "shipping" }), Expiry::seconds(30 * 86400))
            .expect(order.version.unwrap_or(0) - 1)
            .required()
            .operation()?;
        let stale = queen
            .transaction()
            .kv(stale)
            .push_item(TxnPushItem {
                queue: "shipping".into(),
                partition: Some("order-2001".into()),
                payload: json!({}),
                transaction_id: Some("ship-again".into()),
                trace_id: None,
            })?
            .commit()
            .await?;
        check(!stale.success, "a transition from a stale version rolls back");
        let read = queen.queue("shipping").fetch(["order-2001"]).await?;
        check(read["order-2001"].records.len() == 1, "one shipment, not two");
        println!("ok: uc-order");
    }

    // --- 3. Trace management: events merged per trace (session window) ---------
    // guide:start(uc-trace)
    // A trace is complete when it has been quiet for 2 seconds.
    let merger = Stream::from(queen.queue("trace-events"))
        .window_session(2)
        .idle_flush_ms(500)
        .reduce(json!({ "spans": 0, "ms": 0 }), |t, e| {
            json!({
                "spans": t["spans"].as_i64().unwrap_or(0) + 1,
                "ms": t["ms"].as_i64().unwrap_or(0) + e.field("ms").as_i64().unwrap_or(0),
            })
        })
        .to(queen.queue("traces"))              // one message per finished trace
        .run(&queen, RunOptions::new("trace-merger").subscription_mode(SubscriptionMode::All))
        .await?;

    // Every SDK call is one event, in the partition of its trace.
    let event = |span: &str, ms: i64| {
        PushItem::new("trace-events", json!({ "span": span, "ms": ms }))
            .partition("trace-7f3a")
            .transaction_id(format!("trace-7f3a:{span}"))
    };
    let events = queen.queue("trace-events");
    events.push_items(vec![event("http", 120), event("db", 45), event("cache", 3)]).await?;
    events.push_items(vec![event("db", 45)]).await?; // a retried call: stored once
    // guide:end
    {
        let mut merged = None;
        for _ in 0..40 {
            sleep_ms(250).await;
            let read = queen.queue("traces").fetch(["trace-7f3a"]).await?;
            merged = read.get("trace-7f3a").and_then(|p| p.records.first()).map(|r| r.payload.clone());
            if merged.is_some() {
                break;
            }
        }
        merger.stop().await?;
        check(merged.is_some(), "the finished trace reached the traces queue");
        let merged = merged.unwrap();
        let value = merged.get("value").cloned().unwrap_or_else(|| merged.clone());
        check(value["spans"] == json!(3), &format!("three spans: the retried event was stored once ({merged})"));
        check(value["ms"] == json!(168), "168 ms in all");
        println!("ok: uc-trace {merged}");
    }

    // --- 4. Rate limiting: a token bucket gate per customer -------------------
    *T4.lock().unwrap() = Some(Instant::now());
    // guide:start(uc-rate)
    // The SMS provider accepts 5 messages a second for each customer.
    // A refused message comes back when its lease ends: here after 1 second.
    queen
        .queue("sms-out")
        .configure(QueueOptions { lease_time: Some(1), ..Default::default() })
        .await?;

    let limiter = Stream::from(queen.queue("sms-out")) // one partition per customer
        .gate(token_bucket_gate(5.0, 5.0))      // 5 at once, then 5 a second
        .foreach(|sms, _| async move { send_sms(sms).await }) // only what the gate lets through
        .run(&queen, RunOptions::new("sms-gate").subscription_mode(SubscriptionMode::All))
        .await?;

    // Twelve messages for one customer, all at once.
    queen
        .queue("sms-out")
        .partition("customer-42")
        .push_many((1..=12).map(|n| json!({ "n": n, "text": format!("#{n}") })))
        .await?;
    // guide:end
    {
        for _ in 0..80 {
            if SENT.lock().unwrap().len() >= 12 {
                break;
            }
            sleep_ms(100).await;
        }
        limiter.stop().await?;
        let sent = SENT.lock().unwrap().clone();
        let order: Vec<i64> = sent.iter().map(|(n, _)| *n).collect();
        check(order == (1..=12).collect::<Vec<i64>>(), &format!("all twelve were sent, in order ({order:?})"));
        let start = sent[0].1;
        let first = sent.iter().filter(|(_, at)| *at < start + 800).count();
        check(first <= 5, &format!("at most 5 in the first burst ({first})"));
        let spread = sent[11].1 - start;
        check(spread >= 1000, &format!("the rest waited ({spread} ms)"));
        let offsets: Vec<u128> = sent.iter().map(|(_, at)| at - start).collect();
        println!("ok: uc-rate {}", json!(offsets));
    }

    // --- 5. Only the newest value (conflation) ----------------------------------
    // guide:start(uc-latest)
    // The availability of a hotel changes often; the screen needs only the last value.
    for rooms in [12, 11, 9] {
        queen
            .queue("availability")
            .partition("hotel-7")
            .push(json!({ "hotel": 7, "rooms": rooms }))
            .await?;
    }

    let newest = queen
        .queue("availability")
        .group("screen")
        .subscription_mode(SubscriptionMode::All)
        .conflation(true) // the newest message of each hotel, the rest skipped
        .wait(false)
        .pop()
        .await?;
    let latest = &newest[0];
    println!("{}", latest.data); // {"hotel":7,"rooms":9}
    queen.ack(latest).await?;
    // guide:end
    {
        check(latest.data == json!({ "hotel": 7, "rooms": 9 }), "the screen got the newest value");
        let again = queen.queue("availability").group("screen").wait(false).pop().await?;
        check(again.is_empty(), "the older values were skipped, not delivered later");
        println!("ok: uc-latest");
    }

    // --- 6. Reminders and delayed jobs (timers) ---------------------------------
    // guide:start(uc-reminder)
    // When an order is delivered, ask for a review in 24 hours.
    let scheduled = queen
        .timers()
        .schedule("review-reminders", "review-2001", Duration::from_secs(24 * 3600))
        .payload_json(&json!({ "orderId": 2001 }))?
        .send()
        .await?; // status: Scheduled

    // The delivery was late: schedule the same key again, and the reminder moves.
    let moved = queen
        .timers()
        .schedule("review-reminders", "review-2001", Duration::from_secs(48 * 3600))
        .payload_json(&json!({ "orderId": 2001 }))?
        .send()
        .await?; // status: Rescheduled

    // The guest wrote a review already: no reminder at all.
    queen.timers().cancel("review-reminders", "review-2001").await?;
    // guide:end
    {
        check(
            scheduled.status == TimerStatus::Scheduled && moved.status == TimerStatus::Rescheduled,
            &format!("scheduled, then rescheduled ({:?}, {:?})", scheduled.status, moved.status),
        );
        let peek = queen.timers().peek("review-reminders", "review-2001").await?;
        check(!peek.found, "the cancelled reminder is gone");
        // A timer fires as a real message of its queue.
        queen
            .timers()
            .schedule("review-reminders", "review-2002", Duration::from_millis(200))
            .payload_json(&json!({ "orderId": 2002 }))?
            .send()
            .await?;
        let fired: Arc<Mutex<Option<Value>>> = Arc::default();
        let sink = fired.clone();
        queen
            .queue("review-reminders")
            .group("mailer")
            .subscription_mode(SubscriptionMode::All)
            .limit(1)
            .consume(move |m: Message| {
                let sink = sink.clone();
                async move {
                    *sink.lock().unwrap() = Some(m.data);
                    Ok::<_, queen_mq::Error>(())
                }
            })
            .await?;
        let fired = fired.lock().unwrap().clone();
        check(fired == Some(json!({ "orderId": 2002 })), &format!("the timer arrived as a message ({fired:?})"));
        println!("ok: uc-reminder");
    }

    // --- 7. Request and reply between services (ephemeral queues) --------------
    // guide:start(uc-reply)
    // The pricing service answers quote requests, in memory.
    async fn pricing(queen: &Queen) -> queen_mq::Result<()> {
        let eph = queen.ephemeral();
        let batch = eph.pop("quotes").group("pricing").wait(true).send().await?;
        for m in &batch.messages {
            let reply_to = m.payload["replyTo"].as_str().unwrap_or_default();
            let answer = json!({ "room": m.payload["room"], "price": 129 });
            eph.push(reply_to, answer).send().await?;
        }
        eph.ack(&batch).await?;
        Ok(())
    }

    // The booking page asks, and waits at most half a second for the answer.
    async fn quote(queen: &Queen, room: &str) -> queen_mq::Result<Value> {
        let eph = queen.ephemeral();
        let inbox = format!("inbox-{}", queen_mq::uuid::uuidv7());
        eph.push("quotes", json!({ "room": room, "replyTo": inbox })).send().await?;
        let reply = eph
            .pop(&inbox)
            .wait(true)
            .poll_timeout(Duration::from_millis(500))
            .commit_on_delivery(true)
            .send()
            .await?;
        Ok(match reply.messages.first() {
            Some(m) => m.payload.clone(),
            None => json!({ "room": room, "price": null }), // too slow: show "ask us"
        })
    }

    let (deluxe, _) = tokio::try_join!(quote(&queen, "deluxe"), pricing(&queen))?;
    println!("{deluxe}"); // {"price":129,"room":"deluxe"}
    // guide:end
    {
        check(deluxe == json!({ "room": "deluxe", "price": 129 }), &format!("the answer came back ({deluxe})"));
        let t = Instant::now();
        let suite = quote(&queen, "suite").await?; // nobody answers now
        check(suite == json!({ "room": "suite", "price": null }), "no answer: price null");
        check(t.elapsed() < Duration::from_secs(3), "the caller gave up after its timeout");
        let eph = queen.ephemeral();
        let left = eph.pop("quotes").group("pricing").send().await?;
        eph.ack(&left).await?;
        println!("ok: uc-reply");
    }

    // --- 8. Calling a webhook (retries, dead letters, idempotency key) ----------
    let receiver: Arc<Mutex<Receiver>> = Arc::default();
    let hook_url = start_receiver(receiver.clone())?;
    queen
        .queue("webhooks")
        .configure(QueueOptions { retry_limit: Some(5), retry_delay: Some(1000), ..Default::default() })
        .await?;
    queen
        .queue("webhooks")
        .push_items(vec![PushItem::new(
            "webhooks",
            json!({ "url": hook_url, "event": { "type": "booking.confirmed", "id": 9001 } }),
        )
        .partition("customer-42")
        .transaction_id("evt-9001")])
        .await?;
    // guide:start(uc-webhook)
    let stop = Cancel::new();
    let on_sigterm = stop.clone();
    tokio::spawn(async move {                   // a deploy stops the sender cleanly
        if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
            sigterm.recv().await;
            on_sigterm.cancel();
        }
    });
    let (watched, watcher) = (receiver.clone(), stop.clone()); // guide:hide
    tokio::spawn(async move { while watched.lock().unwrap().received.is_empty() { sleep_ms(50).await } watcher.cancel() }); // guide:hide

    let http = reqwest::Client::new();
    queen
        .queue("webhooks")
        .group("sender")
        .subscription_mode(SubscriptionMode::All)
        .poll_timeout(Duration::from_secs(5))   // a stop waits for the poll in flight (5 s)
        .poll_timeout(Duration::from_millis(500)) // guide:hide
        .cancel(stop)
        .consume(move |msg: Message| {
            let http = http.clone();
            async move {
                let res = http
                    .post(msg.data["url"].as_str().unwrap_or_default())
                    .header("idempotency-key", &msg.transaction_id)
                    .json(&msg.data["event"])
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                if !res.status().is_success() {
                    // an Err is retried after retryDelay, then dead-lettered
                    return Err(format!("HTTP {}", res.status().as_u16()));
                }
                Ok(())
            }
        })
        .await?;
    // guide:end
    {
        let r = receiver.lock().unwrap();
        check(r.calls == 2, &format!("the failed call was retried once ({} calls)", r.calls));
        let keys: Vec<&String> = r.received.keys().collect();
        check(keys == vec!["evt-9001"], &format!("the receiver got the event under its key ({keys:?})"));
        let body: Value = serde_json::from_str(&r.received["evt-9001"])?;
        check(body == json!({ "type": "booking.confirmed", "id": 9001 }), "with the event as its body");
        println!("ok: uc-webhook");
    }

    // --- 9. A job only one worker runs (lock + guard) ---------------------------
    {
        let (a, b, c) = tokio::join!(
            nightly_worker(queen.clone()),
            nightly_worker(queen.clone()),
            nightly_worker(queen.clone())
        );
        let outcomes = vec![a?, b?, c?];
        let done = outcomes.iter().filter(|o| **o == "done").count();
        check(done == 1, &format!("one worker ran it ({outcomes:?})"));
        let read = queen.queue("daily-reports").fetch(["Default"]).await?;
        check(read["Default"].records.len() == 1, "one report");
        println!("ok: uc-singleton {}", json!(outcomes));
    }

    queen.close().await?;
    println!("all use cases checked");
    Ok(())
}
