# Queen MQ Rust Client

Rust client for [Queen MQ](https://github.com/queen-mq/queen) — a message queue that keeps its
data in PostgreSQL.

```toml
[dependencies]
queen-mq = "1.0.3"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
serde_json = "1"
```

## Quick start

```rust
use queen_mq::{Config, Queen};

#[tokio::main]
async fn main() -> queen_mq::Result<()> {
    let queen = Queen::connect(Config::new("http://localhost:6632"))?;

    // A queue is created on first push; configure it only when you want
    // something other than the defaults.
    queen.queue("orders")
        .partition("customer-42")
        .push(serde_json::json!({ "total": 19.99 }))
        .await?;

    queen.queue("orders")
        .group("fulfilment")
        .batch(10)
        .consume(|msg| async move {
            println!("{}", msg.data);
            Ok::<_, std::convert::Infallible>(())
        })
        .await?;

    Ok(())
}
```

The handler's return value settles the message: `Ok` acks it, `Err` nacks it and records the
reason on the DLQ row if that nack exhausts the retry budget. Turn that off with
`.auto_ack(false)` and call `queen.ack(&msg)` yourself.

## Sharing the protocol with the broker

This client is built on [`queen-protocol`](../../crates/queen-protocol), the crate that writes
down the `/api/v1` wire format. The broker depends on the same crate in its tests, where
conformance cases round-trip its own request parsers and rendered responses through these
types. A field that drifts on either side fails a test rather than reaching a client.

## What is here

Core protocol: push (with client-side batching), pop, consume, ack, transactions, DLQ,
consumer groups, leases, traces, maintenance, and the observability endpoints — plus the
full streams DSL.

Key/value state (`queen.kv()`) and scheduled deliveries (`queen.timers()`), including their
riders on `queen.transaction()`, which is the reason they exist: a marker written in the same
PostgreSQL transaction as the ack it guards. There is nothing to turn on: both surfaces ship
with the broker, on every cell, like push and pop. An operator can still *pause* one during an
incident through the runtime kill switch, and that is a 503 with `Retry-After` on the routes
(403, terminal, on a transaction rider) — a live surface stopped on purpose, never a broker
that was built without it.

```rust
use queen_mq::Expiry;
use std::time::Duration;

// Every write states its lifetime: Expiry::seconds, ::until or ::forever, never
// none of them. The builders end in .send().
let claimed = queen.kv()
    .put_if_absent("saga", "order-7", serde_json::json!({"step": "reserved"}), Expiry::seconds(86_400))
    .send()
    .await?;
if !claimed.applied() {
    return Ok(()); // somebody else won, and claimed.value is theirs: no second round trip
}

// With .max(), applied() IS the admission decision: a refusal spends no budget.
let hit = queen.kv().incr("quota", "acme:2026-08", 1, Expiry::seconds(3600))
    .max(1000)
    .send()
    .await?;

queen.timers()
    .schedule("reminders", "order-7", Duration::from_secs(1800))
    .payload_json(&serde_json::json!({ "orderId": 7 }))?
    .send()
    .await?;
queen.timers().cancel("reminders", "order-7").await?;

// The reason the riders exist: marker, push and ack on one commit.
let resp = queen.transaction()
    .ack(&message)
    .push("emails", mail)?
    .kv_put_if_absent("sent", &message.transaction_id, serde_json::json!(true), Expiry::seconds(86_400))?
    .commit()
    .await?;
if resp.lost_precondition().is_some() {
    return Ok(()); // a redelivery. Nothing was pushed, nothing was acked.
}
```

Reads are `get`, `get_many` and `get_prefix(..)` (a builder: `.after`, `.limit`, `.keys_only`);
writes are `put`, `put_if_absent`, `delete` and `incr`, each a builder taking `.expect(version)`
or `.required()`. Timers are `schedule` (an upsert, so `.reschedule()` is the same call under
another name), `cancel`, `cancel_expecting`, `peek` and `list`. On a transaction the riders are
`kv`, `kv_put`, `kv_put_if_absent`, `kv_delete`, `timer`, `schedule` and `cancel_timer`.

Two behaviours worth reading before using them:

* `commit()` **returns** on `{success: false, reason: "kv_precondition"}` instead of raising. A
  redelivery meeting its own idempotency marker is the system working, so it must not land in a
  retry policy. Read it with `resp.lost_precondition()`; `resp.success` is what says the bundle
  landed.
* Nothing else on these surfaces turns a verdict into an error. A lost `putIfAbsent`, a missing
  key, a cancel that found nothing: all `Ok`, with `applied()`, `found()` or `ok` saying so.

## Streams

```rust
use queen_mq::streams::{RunOptions, Stream};

let handle = Stream::from(queen.queue("clicks"))
    .filter(|r| r.text("kind") == Some("purchase"))
    .window_tumbling(60)
    .aggregate_count("count")
    .aggregate_sum("revenue", |r| r.number("amount"))
    .to(queen.queue("revenue-per-minute"))
    .run(&queen, RunOptions::new("revenue-rollup"))
    .await?;
```

Tumbling, sliding, session and wall-clock (`cron`) windows; event time with watermarks and a
late-event policy; `reduce` and the named aggregates; `key_by`; `gate` for rate limiting; and
`to` / `foreach` terminals. Each cycle commits its state change, its output and its source ack
in **one PostgreSQL transaction**, so a window cannot advance without its output being written.

The chain's `config_hash` is computed byte-identically to the JS, Go and Python SDKs — the test
suite pins it against vectors captured from the JS implementation — so the same query can be
redeployed in a different language without tripping the "changed operator chain" guard.

## Deliberate differences from the other SDKs

Each of these is a place where matching the other clients would have meant shipping something
worse:

| | Here | Elsewhere |
|---|---|---|
| A failed `pop` | returns `Err` | returns an empty list |
| Consumer group on an ack | read from the message | passed as a separate argument |
| `traceId` on a plain push | not offered | accepted, then dropped by the broker |
| Signal handlers | opt-in (`signals` feature) | installed by default |
| `limit` during consume | across all workers | per worker |
| `clearQueue`, `moveMessageToDLQ` | not offered | call routes that 404 |

The `pop` one matters most: turning a 403 or an exhausted retry budget into "no messages" makes
an outage look like an idle queue. An *empty* claim, and a claim refused because pop maintenance
is on, both still return `Ok` with nothing in it.

The `traceId` one is not a choice so much as a fact. The broker's push path has nowhere to store
a trace id — neither its request struct nor the frame it builds carries one — so a trace id sent
with a push is silently discarded and the message comes back with `traceId: null`. It *does*
work inside a transaction, so `TxnPushItem` exposes it there.

## Several brokers

```rust
let queen = Queen::connect(
    Config::urls(["http://broker-a:6632", "http://broker-b:6632"])
)?;
```

Brokers are stateless and interchangeable, but consumers should not bounce between them: two
replicas polling the same partition for the same group contend for the same claim. The default
`Strategy::Affinity` hashes each poll's `(queue, partition, group)` onto a consistent ring, so a
consumer stays put while its backend is healthy, and moves only when it is not.

## Behind a proxy

```rust
let queen = Queen::connect(
    Config::new("http://cell.eu1.queenmq.cloud")
        .bearer_token(std::env::var("QUEEN_TOKEN")?)
        .host_header("acme.eu1.queenmq.cloud")?
)?;
```

`host_header` advertises one Host while dialling another address, the way `curl --resolve` does.
It exists because a proxy picks the tenant cluster from the Host header's first DNS label, so
pointing a base URL at a shared address without it either 421s or — on a cell with a default
cluster — lands the traffic in another tenant's data. Setting a `Host` through `headers` is
rejected at construction rather than silently overridden.

HTTP 429 is retried in place with jittered backoff rather than failed over, since the backend is
alive and asking for less traffic. Long-poll pops retry unboundedly by default; everything else
gets ten attempts. HTTP 403 is terminal and stops a consumer instead of hot-looping.

## Tests

```bash
cargo test
```

Unit tests run anywhere. The integration suites need a broker and skip with a notice unless
`QUEEN_TEST_URL` is set:

```bash
QUEEN_TEST_URL=http://localhost:6632 cargo test
```

They cover the union of the JS, Go and Python suites — including areas only one of those three
tested — plus a breadth pass of their own:

| file | what it pins |
|---|---|
| `tests/core.rs` | push, pop, queue lifecycle, consume, buffering, discovery |
| `tests/semantics.rs` | ack-as-commit, retry budget, leases, dead-lease consumer behaviour |
| `tests/admin.rs` | transactions, DLQ, consumer groups, traces, failover |
| `tests/streams.rs` | every window kind, event time, gates, sinks, restart recovery |
| `tests/coverage.rs` | queue options, payload shapes, naming, ordering under concurrency |
| `tests/maintenance.rs` | push and pop maintenance (broker-global, so its own binary) |
| `tests/kv_timers_wire.rs` | the exact JSON body of every kv and timer operation, against a scripted server, no broker needed |
| `tests/kv_timers.rs` | kv and timers live: the transaction gate, the fence, the counter ceiling, a timer becoming a message |

`tests/kv_timers.rs` needs nothing beyond `QUEEN_TEST_URL`. It used to probe the broker and skip
when the surfaces answered 404, behind a `QUEEN_TEST_KVT` escape; both are gone with the boot
flags that made a 404 a legitimate answer. A kv call that fails is now a failure.

The repo-wide harness runs this as the `rust-client` suite:

```bash
test/run.sh --suite rust-client
```
