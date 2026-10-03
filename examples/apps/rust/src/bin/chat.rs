// docs:start(app-rust-chat)
//
// A chat backend: one ordered partition per conversation.
//
// Queen started as the broker of a hotel messaging product. Some conversations
// need a translation or an agent before their next message can be handled, and
// on a hashed Kafka topic one slow conversation held up every conversation that
// shared its partition. Here every conversation is a partition of its own,
// created by the first message sent to it, so a slow conversation waits on
// itself and on nothing else.
//
//   chat-messages (one partition per conversation)
//     ├── group "delivery"    marks each message delivered, fast
//     ├── group "enrichment"  translates the Japanese conversation, slow
//     └── group "sentiment"   added later, reads the whole history
//
// The program checks what the design promises: every message reaches each
// group once and in the order of its conversation, and the English
// conversations finish while the Japanese one is still being translated.
//
// Run it:
//   QUEEN_URL=http://localhost:6632 cargo run --bin chat

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use queen_mq::{Config, Message, PushItem, PushStatus, Queen, QueueOptions, SubscriptionMode};
use serde_json::json;

// Three conversations. The Japanese one needs a translation pass, 400 ms a
// message against 10 ms for the others. It is listed first, so its messages are
// the oldest in the queue and its partition is usually handed out first: a
// consumer that let one conversation hold up another would fail the timing
// check below.
//
// (conversationId, locale, needsTranslation)
const CONVERSATIONS: [(&str, &str, bool); 3] = [
    ("conv-jp-1", "jp", true),
    ("conv-en-1", "en", false),
    ("conv-en-2", "en", false),
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
    // A fresh queue per run, so two runs never read each other's messages.
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let messages = format!("app-rust-chat-{run_id}");

    let mut checks = Checks(0);
    println!("broker {url}");

    // Signal handlers are opt-in in this client (they sit behind the `signals`
    // feature), so nothing process-wide is installed and this program owns its
    // shutdown, through close() at the bottom.
    let queen = Queen::connect(Config::new(&url)).map_err(|e| e.to_string())?;

    // A crashed worker's messages come back when its lease expires, and
    // retry_limit bounds how often a failing message is retried before it goes
    // to the dead-letter queue. configure() merges, so an option left out keeps
    // the value the queue already has; this queue is new, so those are the
    // broker's defaults.
    queen
        .queue(&messages)
        .configure(QueueOptions {
            lease_time: Some(60),
            retry_limit: Some(3),
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?;

    // ------------------------------------------------------------------ sending
    //
    // Sending a message is one push into the conversation's partition. Nothing
    // was declared for the conversation beforehand, and nothing has to be
    // cleaned up when it goes quiet.
    println!("\nsending");
    let mut sent = 0usize;
    for seq in 1..=MESSAGES_PER_CONVERSATION {
        for (conversation_id, locale, _) in CONVERSATIONS {
            let payload = json!({
                "conversationId": conversation_id,
                "seq": seq,
                "locale": locale,
                "body": format!("message {seq} in {conversation_id}"),
            });

            // The transaction id is the phone's own id for the message. A
            // phone that retries a send it never saw answered writes nothing
            // the second time. push() would mint a UUIDv7 id of its own, so
            // the item is built by hand and sent with push_items().
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

    // The phone resends message 1 because the answer got lost on a bad network.
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
    let resent = resent
        .first()
        .ok_or("the broker answered the resend with no result")?;
    checks.assert(
        resent.status == PushStatus::Duplicate,
        "a resent message was recognised and not stored twice",
    )?;

    // Three workers in one consumer group. partitions(1) makes every pop take
    // ONE conversation: by default a pop may sweep up several ready
    // conversations, and a worker handles the messages of one pop in order, so
    // a slow conversation would delay the others that came with it.
    //
    // In this client limit() counts across the three workers, so a phase ends
    // as soon as every message has been handled. idle() is the deadline behind
    // that count, for a message that never comes, and it is pool-wide too: the
    // first worker that has waited that long stops all three, even one that is
    // halfway through a conversation, so it has to outlast the 2.4 s the
    // Japanese conversation keeps one worker busy. Long polls end after a
    // second, so both are noticed promptly. A service sets none of the three
    // and runs until it is cancelled.
    //
    // The handlers run on three tasks at once, so what they record is behind a
    // mutex.
    let workers = |group: &str| {
        queen
            .queue(&messages)
            .group(group)
            // A group created after the messages starts at the tail otherwise.
            .subscription_mode(SubscriptionMode::All)
            .concurrency(3)
            .partitions(1)
            .limit(sent as u64)
            .poll_timeout(Duration::from_secs(1))
            .idle(Duration::from_secs(5))
    };

    // --------------------------------------------------------------- delivering
    //
    // Marking messages delivered is fast work and must never wait behind slow
    // work, so it is a consumer group of its own, with its own cursor. A
    // handler that returns Ok acks the message, and one that returns Err nacks
    // it.
    println!("\ndelivering");
    let delivered: Arc<Mutex<HashMap<String, Vec<i64>>>> = Arc::new(Mutex::new(HashMap::new()));
    {
        let sink = Arc::clone(&delivered);
        workers("delivery")
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
    let delivered_count: usize = delivered.values().map(Vec::len).sum();
    checks.assert(
        delivered_count == sent,
        &format!("delivery saw all {sent} messages once (got {delivered_count})"),
    )?;

    // Checked in the order of CONVERSATIONS, because a HashMap has no iteration
    // order of its own and a passing run should print the same lines each time.
    for (conversation_id, _, _) in CONVERSATIONS {
        let seqs = delivered.get(conversation_id).cloned().unwrap_or_default();
        let listed: Vec<String> = seqs.iter().map(i64::to_string).collect();
        checks.assert(
            seqs.iter().enumerate().all(|(i, &seq)| seq == i as i64 + 1),
            &format!(
                "{conversation_id} was delivered in order: {}",
                listed.join(",")
            ),
        )?;
    }

    // --------------------------------------------------------------- enrichment
    //
    // The slow group reads the same messages through its own cursor. On a topic
    // with a few hashed partitions, the Japanese conversation would sit in a
    // partition shared with English ones and hold them up. Here each worker
    // holds one conversation at a time, so the English conversations finish
    // while the Japanese one is still being translated.
    println!("\nenriching");
    let finished_at: Arc<Mutex<HashMap<String, u128>>> = Arc::new(Mutex::new(HashMap::new()));
    let started = Instant::now();
    {
        let sink = Arc::clone(&finished_at);
        workers("enrichment")
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
                    // One conversation is handled by one worker at a time, so
                    // the last write for a conversation is when it finished.
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
        "the English conversations finished while the Japanese one was still being translated",
    )?;
    checks.assert(
        slow >= (MESSAGES_PER_CONVERSATION as u128) * 400,
        "the Japanese conversation really took its six translations",
    )?;

    // ----------------------------------------------------------------- backfill
    //
    // A feature added later, sentiment scoring, wants every message ever sent.
    // It is one more consumer group starting from the oldest message: no
    // producer change, and no second copy of the data.
    println!("\nbackfilling a new group");
    let scored = Arc::new(Mutex::new(0usize));
    {
        let counter = Arc::clone(&scored);
        workers("sentiment")
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
    checks.assert(
        scored == sent,
        &format!("a group created now read the whole history ({scored} messages)"),
    )?;

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
