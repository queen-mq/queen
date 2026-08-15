// docs:start(app-rust-chat)
//
// A chat messaging system.
//
// This is the application Queen was written for. A hotel messaging product ran
// on Kafka and kept stalling: some conversations need a translation or an agent
// reply before the next message can be handled, and on a shared partition one
// slow conversation holds up every conversation behind it.
//
// The fix is structural rather than operational: one ordered lane per
// conversation, created by the first message sent to it. A conversation that
// takes ten seconds delays itself and nothing else.
//
// What this program builds:
//
//   chat-messages (one partition per conversation)
//     ├── group "delivery"    fast, marks each message as delivered
//     └── group "enrichment"  slow on conversations that need translation
//
// And what it proves: every message reaches both groups exactly once, in the
// order it was sent inside its own conversation, and the conversations that
// need no translation finish while the slow one is still working.
//
// Run it:
//   QUEEN_URL=http://localhost:6632 cargo run --bin chat

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use queen_mq::{Config, Message, PushItem, PushStatus, Queen, QueueOptions, SubscriptionMode};
use serde_json::json;

// Three conversations. The one in Japanese needs a translation pass, which is
// the slow work: 400 ms a message against 10 ms for the rest.
//
// (conversationId, locale, needsTranslation)
const CONVERSATIONS: [(&str, &str, bool); 3] = [
    ("conv-en-1", "en", false),
    ("conv-en-2", "en", false),
    ("conv-jp-1", "jp", true),
];
const MESSAGES_PER_CONVERSATION: i64 = 6;

fn needs_translation(conversation_id: &str) -> bool {
    CONVERSATIONS
        .iter()
        .find(|(id, _, _)| *id == conversation_id)
        .map(|(_, _, slow)| *slow)
        .unwrap_or(false)
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
    let messages = format!("app-rust-chat-{run_id}");

    let mut checks = Checks(0);
    println!("broker {url}");

    // Signal handlers are opt-in in this client — they sit behind the `signals`
    // feature — so nothing process-wide is installed and this program owns its
    // own shutdown, through close() at the bottom.
    let queen = Queen::connect(Config::new(&url)).map_err(|e| e.to_string())?;

    // Leases are what make a crashed worker safe: a message whose handler dies
    // is redelivered once the lease expires. retry_limit bounds how many times
    // that can happen before the message is dead-lettered instead. configure()
    // is a full replace, so every key left out goes back to the broker's own
    // default rather than keeping a previous value.
    queen
        .queue(&messages)
        .configure(QueueOptions {
            lease_time: Some(60),
            retry_limit: Some(3),
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?;

    // ---------------------------------------------------------------- producing
    //
    // A chat client sends a message: one push, into the partition named after
    // the conversation. Nothing was declared for this conversation in advance,
    // and nothing has to be cleaned up when it goes quiet.
    println!("\nsending");
    let mut sent = 0usize;
    for seq in 1..=MESSAGES_PER_CONVERSATION {
        for (conversation_id, locale, _) in CONVERSATIONS {
            let sent_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            let payload = json!({
                "conversationId": conversation_id,
                "seq": seq,
                "locale": locale,
                "body": format!("message {seq} in {conversation_id}"),
                "sentAt": sent_at,
            });

            // push() mints a UUIDv7 transaction id for you. Here the id has to
            // be the client's own idempotency key — a retry of this send, from
            // a phone on a flaky network, must write nothing the second time —
            // so the item is built by hand and pushed through push_items().
            queen
                .queue(&messages)
                .partition(conversation_id)
                .push_items(vec![PushItem::new(&messages, payload)
                    .partition(conversation_id)
                    .transaction_id(format!("{conversation_id}-{seq}"))])
                .await
                .map_err(|e| e.to_string())?;
            sent += 1;
        }
    }
    println!(
        "  {sent} messages across {} conversations",
        CONVERSATIONS.len()
    );

    // A resend of the same message: the client retried because it never saw the
    // first answer. The broker recognises the transaction id and stores nothing.
    let resent = queen
        .queue(&messages)
        .partition("conv-en-1")
        .push_items(vec![PushItem::new(
            &messages,
            json!({ "conversationId": "conv-en-1", "seq": 1, "body": "resent by the phone" }),
        )
        .partition("conv-en-1")
        .transaction_id("conv-en-1-1")])
        .await
        .map_err(|e| e.to_string())?;
    let duplicate = resent
        .first()
        .ok_or("the broker answered the resend with no result")?;
    checks.assert(
        duplicate.status == PushStatus::Duplicate,
        "a resent message was deduplicated, not stored twice",
    )?;

    // --------------------------------------------------------------- delivering
    //
    // The delivery worker is what marks a message as delivered to the
    // recipients. It is fast and must never fall behind, which is why it is its
    // own consumer group: it shares no cursor with the slow work below.
    //
    // concurrency(3) runs three poll loops, and each pop claims a partition, so
    // the three conversations are drained in parallel by three workers. The
    // handler is a plain async closure; returning Ok acks the message, returning
    // Err nacks it. `limit` counts across all three workers, not per worker.
    //
    // idle() stops the loop after a stretch of silence, so a lost message fails
    // the run instead of hanging it. It is checked between polls, so
    // poll_timeout bounds how promptly it fires: with the default 30-second poll
    // window a 10-second silence would be noticed 30 seconds late.
    println!("\ndelivering");
    let delivered: Arc<Mutex<HashMap<String, Vec<i64>>>> = Arc::new(Mutex::new(HashMap::new()));
    {
        let sink = Arc::clone(&delivered);
        queen
            .queue(&messages)
            .group("delivery")
            .subscription_mode(SubscriptionMode::All)
            .concurrency(3)
            .limit(sent as u64)
            .poll_timeout(Duration::from_secs(1))
            .idle(Duration::from_secs(10))
            .consume(move |msg: Message| {
                let sink = Arc::clone(&sink);
                async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    let conversation_id = msg.data["conversationId"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string();
                    let seq = msg.data["seq"].as_i64().unwrap_or(0);
                    sink.lock()
                        .unwrap()
                        .entry(conversation_id)
                        .or_default()
                        .push(seq);
                    Ok::<_, String>(())
                }
            })
            .await
            .map_err(|e| e.to_string())?;
    }

    let delivered = delivered.lock().unwrap().clone();
    checks.assert(
        delivered.values().map(|seqs| seqs.len()).sum::<usize>() == sent,
        "delivery saw every message exactly once",
    )?;

    // A HashMap has no iteration order, so the lanes are checked by name: the
    // output of a passing run should not depend on how the entries happened to
    // land in the table.
    let mut lanes: Vec<&String> = delivered.keys().collect();
    lanes.sort_unstable();
    for conversation_id in lanes {
        let seqs = &delivered[conversation_id];
        let mut in_order = seqs.clone();
        in_order.sort_unstable();
        checks.assert(
            *seqs == in_order,
            &format!("{conversation_id} was delivered in order"),
        )?;
    }

    // -------------------------------------------------------------- enrichment
    //
    // The slow group. It reads the same messages through its own cursor, and the
    // Japanese conversation costs 400 ms a message because it has to be
    // translated before it can be answered.
    //
    // This is where a shared partition would hurt: on a hashed topic these
    // messages would sit in the same lane as the English ones and hold them up.
    // Here each conversation has its own lane, so the English conversations
    // finish while the Japanese one is still being translated. The timings below
    // are the proof.
    println!("\nenriching");
    let finished_at: Arc<Mutex<HashMap<String, u128>>> = Arc::new(Mutex::new(HashMap::new()));
    let started = Instant::now();
    {
        let sink = Arc::clone(&finished_at);
        queen
            .queue(&messages)
            .group("enrichment")
            .subscription_mode(SubscriptionMode::All)
            .concurrency(3)
            .limit(sent as u64)
            .poll_timeout(Duration::from_secs(1))
            .idle(Duration::from_secs(15))
            .consume(move |msg: Message| {
                let sink = Arc::clone(&sink);
                async move {
                    let conversation_id = msg.data["conversationId"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string();
                    let cost = if needs_translation(&conversation_id) {
                        400
                    } else {
                        10
                    };
                    tokio::time::sleep(Duration::from_millis(cost)).await;
                    sink.lock()
                        .unwrap()
                        .insert(conversation_id, started.elapsed().as_millis());
                    Ok::<_, String>(())
                }
            })
            .await
            .map_err(|e| e.to_string())?;
    }

    let finished_at = finished_at.lock().unwrap().clone();
    let at = |conversation_id: &str| -> Result<u128, String> {
        finished_at
            .get(conversation_id)
            .copied()
            .ok_or_else(|| format!("enrichment never finished {conversation_id}"))
    };
    let slow = at("conv-jp-1")?;
    let fast = at("conv-en-1")?.max(at("conv-en-2")?);
    println!("  english done after {fast} ms, japanese after {slow} ms");

    checks.assert(
        fast < slow,
        "the conversations needing no translation finished first, in the same worker pool",
    )?;
    checks.assert(
        slow > (MESSAGES_PER_CONVERSATION as u128) * 300,
        "the slow conversation really was slow, so the comparison means something",
    )?;

    // ------------------------------------------------------------------- replay
    //
    // A new feature needs the history: sentiment scoring over everything ever
    // said. It is a new consumer group reading from the beginning, and it costs
    // no producer change and no second copy of the data. SubscriptionMode::All
    // is what points the new cursor at the beginning — the default for a new
    // group is the tail, so without it this group would sit idle.
    println!("\nbackfilling a new consumer");
    let scored = Arc::new(Mutex::new(0usize));
    {
        let counter = Arc::clone(&scored);
        queen
            .queue(&messages)
            .group("sentiment")
            .subscription_mode(SubscriptionMode::All)
            .concurrency(3)
            .limit(sent as u64)
            .poll_timeout(Duration::from_secs(1))
            .idle(Duration::from_secs(10))
            .consume(move |_msg: Message| {
                let counter = Arc::clone(&counter);
                async move {
                    *counter.lock().unwrap() += 1;
                    Ok::<_, String>(())
                }
            })
            .await
            .map_err(|e| e.to_string())?;
    }

    let scored = *scored.lock().unwrap();
    checks.assert(scored == sent, "a group added today read the whole history")?;

    // Clean up on success only: a failed run leaves the queue on the broker to
    // be looked at.
    queen
        .queue(&messages)
        .delete()
        .await
        .map_err(|e| e.to_string())?;

    queen.close().await.map_err(|e| e.to_string())?;

    Ok(checks.0)
}
// docs:end
