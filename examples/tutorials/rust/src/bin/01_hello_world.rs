// docs:start(tut-rust-hello-world)
//
// Tutorial 1 of 5: hello world.
//
// One message in, one message out. Nothing is created in advance: the queue and
// the partition come into existence with the push that names them.
//
// Run it:
//   QUEEN_URL=http://localhost:6632 cargo run --bin 01_hello_world
//
// The program checks its own outcome and exits non-zero if a check fails.

use std::time::{SystemTime, UNIX_EPOCH};

use queen_mq::{Config, PushStatus, Queen};
use serde_json::json;

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

// Rust has no exceptions, so the shape the other tutorials get from try/catch
// comes from `run` returning a Result: every `?` on the way down is a failed
// check or a failed call, and main turns it into FAIL and a non-zero exit.
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

    // The name is prefixed per language and suffixed per run, so every tutorial
    // in every language can share one broker and no run inherits state from
    // another.
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let queue = format!("tut-rust-hello-{run_id}");

    let mut checks = Checks(0);
    println!("broker {url}");

    // Signal handlers are opt-in in this client: they sit behind the `signals`
    // feature and nothing is installed unless the application asks. So SIGINT
    // and SIGTERM are left alone and this program owns its own shutdown,
    // through close() at the bottom.
    //
    // connect() validates the configuration but opens no socket; the first
    // request does that.
    let queen = Queen::connect(Config::new(&url)).map_err(|e| e.to_string())?;

    // A push names a queue and, optionally, a partition. Both are created by
    // this call if they do not exist, inside the transaction that stores the
    // message. There is no declare step and nothing to provision first.
    //
    // push() takes anything that serialises, and mints a UUIDv7 transaction id
    // client-side, which is the key the broker deduplicates on. One result per
    // item comes back.
    let pushed = queen
        .queue(&queue)
        .push(json!({ "greeting": "Hello World!" }))
        .await
        .map_err(|e| e.to_string())?;

    let stored = pushed
        .first()
        .ok_or("the broker answered the push with no result")?;
    println!("pushed {} -> {:?}", stored.transaction_id, stored.status);
    checks.assert(
        stored.status == PushStatus::Queued,
        "the broker stored the message",
    )?;

    // pop() takes messages under a lease: they are claimed until they are
    // acknowledged or the lease expires. wait(true) turns on long polling, so
    // the call parks until a message arrives instead of coming back empty. It
    // is this client's default, and it is spelled out here because the drain
    // check below turns it off.
    //
    // No consumer group is named here, so the read goes through the queue's own
    // cursor, which starts at the beginning. Named groups are tutorial 2: a
    // group created after a message was pushed starts at the tail and would see
    // nothing here.
    //
    // Unlike the JavaScript client, a failed pop is an Err rather than an empty
    // vector — turning a 403 or an exhausted retry budget into "no messages"
    // would make an outage look like an idle queue. An empty claim is still an
    // Ok with nothing in it.
    let messages = queen
        .queue(&queue)
        .batch(1)
        .wait(true)
        .pop()
        .await
        .map_err(|e| e.to_string())?;

    checks.assert(messages.len() == 1, "one message came back")?;
    let message = &messages[0];
    println!(
        "received \"{}\" from partition {}",
        message.data["greeting"].as_str().unwrap_or(""),
        message.partition
    );
    checks.assert(
        message.data["greeting"] == json!("Hello World!"),
        "the payload survived the round trip",
    )?;

    // No partition was named on the push, so the broker put the message in the
    // queue's default lane.
    checks.assert(
        message.partition == "Default",
        "it landed in the default partition",
    )?;

    // The acknowledgement is what commits consumption. It moves the cursor past
    // the message and releases the lease. This client reads the consumer group
    // and the lease id off the message rather than taking them as arguments, so
    // an ack cannot be pointed at the wrong cursor.
    //
    // A rejected ack still arrives as HTTP 200 with success: false on the item,
    // so the per-item flag is the only proof the broker took it.
    let ack = queen.ack(message).await.map_err(|e| e.to_string())?;
    checks.assert(ack.success, "the acknowledgement was accepted")?;

    // The cursor is now past the only message, so a further read finds nothing.
    // wait(false) returns immediately instead of long polling.
    let leftovers = queen
        .queue(&queue)
        .wait(false)
        .pop()
        .await
        .map_err(|e| e.to_string())?;
    checks.assert(leftovers.is_empty(), "the queue is drained")?;

    // Clean up on success only: a failed run leaves the queue on the broker to
    // be looked at.
    queen
        .queue(&queue)
        .delete()
        .await
        .map_err(|e| e.to_string())?;

    // close() flushes any client-side push buffers. This program uses none, and
    // nothing here holds the runtime open the way a Node event loop would, but
    // running it is the habit that keeps a buffered program honest.
    queen.close().await.map_err(|e| e.to_string())?;

    Ok(checks.0)
}
// docs:end
