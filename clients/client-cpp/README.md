# Queen C++ Client

The Queen MQ client for C++17: one header, `queen_client.hpp`, with the API of the Go and Rust
clients. Version 2.7.1, for any 2.x broker.

```cpp
#include "queen_client.hpp"

int main() {
    queen::Queen queen("http://localhost:6632");

    // One ordered partition per entity, created by the first push.
    queen.queue("orders").partition("customer-42")
         .push({{"orderId", 1001}, {"total", 59.9}}, "order-1001");   // the dedup key

    // Acked when the handler returns, nacked (and retried) when it throws.
    queen.queue("orders").group("billing").subscription_mode("all").limit(1)
         .consume([](const queen::Message& m) { std::cout << m.data << "\n"; });

    queen.close();
}
```

## Features

- **Typed values**: `Message`, `PushResult`, `AckResult`, `TransactionResult`; payloads stay
  `nlohmann::json`
- **Push, pop, consume**: one message or a batch per handler call, any number of worker threads,
  autopilot sizing, multi-partition claims, conflation, commit at delivery
- **Acks** with every status (completed, failed, retry, dlq); the ack reads the consumer group and
  the lease from the message
- **Transactions**: acks, pushes to any partition, KV writes and timers in one log entry, guarded
  by a lock
- **KV** (the eight operations), **timers** (schedule, cancel, peek, list), **locks and
  semaphores** that renew in the background, **ephemeral queues**, **reads by offset**
- **`admin()`**: seek a consumer group, retry a dead letter, the queue, group and status routes
- **Transport**: a keep-alive connection pool, HTTPS with certificate verification, retry with
  backoff, 429 handling, failover and `affinity` load balancing across brokers
- **Client-side push buffering**, bounded and lossless
- **Binary push, push compression and framed pop** (opt-in)
- **Optional consumer supervision** for the dashboard

## Install

**With CMake**, add this directory or fetch it, and link the `queen::client` target. It finds
cpp-httplib 0.27 (or fetches it), links OpenSSL, and uses the program's own nlohmann/json when
`find_package` finds one, else the copy in `third_party/`.

```cmake
include(FetchContent)
FetchContent_Declare(queen
    GIT_REPOSITORY https://github.com/queen-mq/queen.git
    GIT_TAG        <release tag>
    SOURCE_SUBDIR  clients/client-cpp)
FetchContent_MakeAvailable(queen)

target_link_libraries(app PRIVATE queen::client)
```

| CMake option | Default | What it does |
| --- | --- | --- |
| `QUEEN_CLIENT_TLS` | `ON` | https:// support, links OpenSSL. Off defines `QUEEN_CLIENT_NO_TLS`. |
| `QUEEN_CLIENT_ZSTD` | `OFF` | zstd, for `push_compression` and `framed_pop`. |
| `QUEEN_CLIENT_FETCH_HTTPLIB` | `ON` | Fetch cpp-httplib v0.27.0 when it is not installed. |
| `QUEEN_CLIENT_BUILD_TESTS` | top-level only | The broker-free suites under `ctest`, `test_client` and the example. |

**Without CMake**, install cpp-httplib and OpenSSL (`brew install cpp-httplib openssl` on macOS;
or download `httplib.h` v0.27.0) and compile with one include path:

```bash
g++ -std=c++17 -O2 -pthread \
    -I clients/client-cpp \
    -I /opt/homebrew/include -I "$(brew --prefix openssl)/include" \
    app.cpp -o app \
    -L "$(brew --prefix openssl)/lib" -lssl -lcrypto -lpthread
```

- C++17 or later.
- **OpenSSL even over plain http**: the header turns on cpp-httplib's TLS support so `https://`
  works. Define `QUEEN_CLIENT_NO_TLS` to build without it; an `https://` URL is then refused at
  construction.
- **zstd** only for `push_compression` and `framed_pop`: `-DQUEEN_CLIENT_ZSTD` and `-lzstd`.

## Connect

```cpp
queen::Queen queen("http://localhost:6632");                     // one broker

queen::ClientConfig config;
config.bearer_token = "...";                                      // the proxy, Queen Cloud
queen::Queen cloud("https://cell.example.com", config);

queen::Queen cluster({"http://node1:6632", "http://node2:6632", "http://node3:6632"});
```

The examples on this page assume `using json = nlohmann::json;`. Create one client per process
and share it between threads: it holds one connection pool, one
load balancer and one set of push buffers. It opens no connection until the first call.
`QueenClient` is the 2.7.0 name of `Queen` and still works.

| `ClientConfig` | Default | What it does |
| --- | --- | --- |
| `timeout_millis` | 30000 | Per request; a long-poll pop adds its poll window. Milliseconds: 500 means half a second. |
| `retry_attempts`, `retry_delay_millis` | 3, 1000 | Tries on one backend for a 5xx or a network fault; the delay doubles each time. |
| `load_balancing` | `Affinity` | `Affinity` keeps one consumer group's pops on one backend (the same ring and hash as the other SDKs); `RoundRobin`; `Session`. |
| `affinity_hash_ring`, `health_retry_after_millis` | 128, 5000 | Virtual nodes per backend; how long a failed backend stays out. |
| `enable_failover` | true | With several URLs, a 5xx or a network fault moves to the next backend. |
| `bearer_token`, `headers` | | `Authorization: Bearer <token>`, and extra headers on every request. |
| `retry_429` | | The 429 policy, below. |
| `max_idle_connections_per_host`, `idle_connection_timeout_millis` | 256, 60000 | The keep-alive pool. |
| `tls_verify`, `ca_cert_path`, `ca_cert_dir` | true, system store | HTTPS certificate checks. Turn `tls_verify` off only for a self-signed test broker. |
| `handle_signals` | false | SIGINT/SIGTERM stop every `consume()` loop after its current message (see Shutdown). |
| `binary_push`, `push_compression`, `framed_pop` | false | The binary wire forms, below. |

## Push

```cpp
auto results = queen.queue("orders")
    .partition("customer-42")            // one ordered partition per entity
    .push({{"orderId", 1001}, {"total", 59.9}}, "order-1001");

for (const queen::PushResult& r : results) {   // one per message, in order
    if (r.status == queen::PushStatus::Queued) {
        std::cout << "stored at offset " << r.offset.value_or(-1) << "\n";
    } else if (r.status == queen::PushStatus::Duplicate) {
        std::cout << "already stored: " << r.transaction_id << "\n";
    } else {                                   // Error: refused, nothing written
        std::cerr << "refused: " << r.error << "\n";
    }
}
```

- `push(payload)` pushes one message; its payload travels as it is. The transaction id is minted
  (a UUIDv7). `push(payload, transaction_id)` gives it one derived from the work: a repeat inside
  the queue's dedup window (1 hour by default) is answered `Duplicate` and writes nothing.
- `push_many({p1, p2})` pushes one message per payload; `push_items({PushItem{payload, id}, ...})`
  when each needs its own id.
- A JSON array passed to `push()` throws `std::invalid_argument`: use `push_many()` for several
  messages, or `push_many({array})` for one message whose payload is an array.
- The broker answers HTTP 201 even when it refuses a message: read each result's `status`.
- No partition named: the message goes to the queue's `Default` partition.

### Client-side buffering

```cpp
queen::BufferOptions batches;
batches.message_count = 100;       // send 100 at a time,
batches.time_millis = 1000;        // or every second
batches.max_size = 400;            // backpressure bound (0 = 4 x message_count)

for (int i = 0; i < 10000; i++) {
    queen.queue("events").partition("device-" + std::to_string(i % 10))
         .buffer(batches)
         .push({{"value", i}}, "reading-" + std::to_string(i));
}
queen.flush_all_buffers();         // before exiting, send what is left
```

A buffered push returns nothing yet: its verdict comes with the flush. At `max_size` waiting
messages a buffered `push()` blocks until the flusher frees room. A batch whose request fails goes
back to the front of the buffer, in order, and is retried every `retry_delay_millis` until it
lands; it is never dropped. `flush_buffer(deadline_millis)` and `flush_all_buffers(deadline_millis)`
bound the retrying and throw `BufferFlushError` (with `unflushed_count()`) on expiry. `close()`
flushes for 30 s at most. Buffered messages live in this process's memory: use buffering for
telemetry-shaped traffic, not for messages that must not be lost.

## Consume

```cpp
auto summary = queen.queue("orders")
    .group("billing")              // every group gets every message; none = queue mode
    .subscription_mode("all")      // a NEW group starts at the tail unless told otherwise
    .concurrency(4)                // four worker threads, on four partitions
    .renew_lease(true, 10000)      // renew the lease every 10 s while the handler runs
    .consume([](const queen::Message& m) {
        charge(m.data);            // acked when it returns, nacked when it throws
    });
```

`consume()` blocks until a stop condition and returns a `ConsumeSummary{processed, acked, nacked,
stopped_by}`. `consume_batch()` hands the handler a whole claimed batch and settles it with one
request:

```cpp
queen.queue("events").group("archiver").batch(100)
     .consume_batch([](const std::vector<queen::Message>& batch) { archive(batch); });
```

| Option | What it does |
| --- | --- |
| `group(name)` | The consumer group. Workers of one group compete; every group gets every message. |
| `concurrency(n)` | n worker threads, each with its own poll loop. |
| `batch(n)`, `partitions(n)` | The pop's message budget and how many partitions one pop claims. Unset, the broker sizes them (autopilot). |
| `limit(n)` | Stop after n messages, counted across the workers. A soft cap: a worker finishes the message (with `consume_batch`, the batch) it is on. |
| `idle_millis(ms)` | Stop after this long without a message. |
| `cancel(token)` | Stop from another thread: `queen::Cancel token; ...; token.cancel();` |
| `wait(bool)`, `timeout_millis(ms)` | Long polling (on by default) and its window (30000). The stop and the idle deadline are read between polls, so a worker that must stop fast uses `wait(false)` or a short window. |
| `auto_ack(bool)` | On (the default): ack after the handler returns, nack with the exception's text when it throws. Off: the handler settles messages itself. |
| `renew_lease(true, ms)` | Renew the batch's lease every interval while the handler runs. |
| `subscription_mode("all")`, `subscription_from(iso)` | Where a NEW group starts. Fixed at the group's first pop; move an existing group with `admin().seek_consumer_group()`. |
| `conflation()` | The group gets only the newest message of each partition. Needs `group()`. |
| `supervision(SupervisionConfig{"app"})` | Publish the loop's status for the dashboard (below). |

**When a handler throws**, with `auto_ack` the message is nacked with the exception's text (capped
at 4096 bytes, invalid UTF-8 replaced): it spends one retry and reaches the dead-letter queue once
`retryLimit` is spent, with that text. The later messages of the same partition in that batch are
skipped, because the broker delivers them again after the failed one, in order; other partitions
of a multi-partition pop are handled. With `auto_ack(false)` nothing is sent, and the loop
abandons the rest of the batch.

**When a pop fails**, a 429 or a 5xx or a network fault makes the worker wait a second and poll
again. A 403 (a suspended cluster, a quota, a gated feature) and any other 4xx stop the loop, and
`consume()` throws that error once every worker has stopped.

### Pop

```cpp
std::vector<queen::Message> messages = queen.queue("events")
    .group("auditor")
    .batch(50)
    .wait(true)                    // long poll: park on the broker until a message arrives
    .pop();
for (const auto& m : messages) handle(m);
queen.ack_all(messages);           // the whole batch in one request
```

`pop()` claims messages under a lease: ack them, or they come back when the lease expires. It
**throws** on failure (`HttpError`, `NetworkError`, `ConflationUnsupportedError`), so an empty
vector means an empty queue. `pop_result()` also returns what the broker chose for the pop
(`autopilot.partitions`, `autopilot.batch`, `autopilot.wait_millis`).

`commit_on_delivery()` makes the broker move the cursor as it hands the messages out: no lease,
nothing to ack, at-most-once. It is a `pop()` option; `consume()` always leases and refuses it.

```cpp
auto readings = queen.queue("metrics").group("live-chart").commit_on_delivery().pop();
```

### Messages and acks

| `Message` | |
| --- | --- |
| `data` | The payload, as pushed (`nlohmann::json`). |
| `transaction_id`, `id` | The producer's dedup key; the broker's message id. |
| `queue`, `partition`, `partition_id` | Where it lives. |
| `lease_id`, `consumer_group` | Its claim. An ack reads both from here. |
| `delivery_attempt` | 1 the first time, more after a nack or an expired lease. |
| `offset`, `created_at`, `trace_id`, `producer_sub` | |

```cpp
queen.ack(m);                                       // completed: the cursor moves past it
queen.nack(m, "card declined");                     // failed: retried, then dead-lettered
queen.ack_with(m, queen::AckStatus::Retry);         // redelivered without spending a retry
queen.ack_with(m, queen::AckStatus::Dlq, "bad");    // dead-lettered now
queen.ack_all(messages);                            // one request; one group for all of them
```

An ack returns an `AckResult` (`success`, `error`, `dlq`, `lease_released`, `noop`). A refusal (the
lease expired or was released, the message is already settled) is `success == false`, not an
exception; a call that could not be made throws. `renew(message)` extends a lease by the broker's
default (60 s) and returns a `RenewResult`; every message of one pop shares its lease.

### Multi-partition pop and autopilot

`partitions(n)` claims up to n partitions per pop, with `batch(b)` as the budget across all of
them; every claimed partition shares one lease. Use it on queues with many partitions that each
hold a few messages. Leave `batch` and `partitions` unset and the broker sizes each pop from state
the client cannot see (autopilot, broker 1.2+); a knob you set is never changed.
`autopilot(false)` on a builder, or `QUEEN_SDK_POP_AUTOPILOT=off` for the process, restores the
client-side defaults (batch 1, partitions 1).

### Shutdown

`close()` flushes the push buffers (30 s at most), stops them and closes the pooled connections.
The client installs no signal handlers unless asked:

```cpp
queen::ClientConfig config;
config.handle_signals = true;      // SIGINT/SIGTERM stop consume() after the current message
queen::Queen worker("http://localhost:6632", config);

worker.queue("tasks").group("workers").concurrency(4).wait(false)
      .consume([](const queen::Message& t) { run(t.data); });
worker.close();                    // send what is buffered, then return from main
```

With `handle_signals`, the first SIGINT or SIGTERM stops every `consume()` loop
(`StopReason::Cancelled`) and makes `wait_for_signal()` return; a second one ends the process as
the signal would without the handlers. The signal count is process-wide.

## Transactions

One step, all or nothing: the acks, pushes, KV writes and timers of a bundle are one entry in the
broker's log.

```cpp
queen::TransactionResult result = queen.transaction()
    .ack(order)                                            // the input is done
    .kv("orders").put("order-" + id, {{"status", "paid"}},  // the order's state
                      queen::KvTtl::seconds(7 * 86400))
    .queue("shipments").partition(order.partition)          // the next event
        .push({{"orderId", id}}, "ship-" + id)
    .timers().schedule(reminder)                            // a message tomorrow
    .commit();
```

- `.ack(message, status, reason)`, `.nack(message, reason)` and `.ack_all(messages)` settle
  delivered messages; the group and the lease come from each message.
- `.queue(q).partition(p).push(payload, id)`, `.push_many(...)` and `.push_items(...)`; a
  `TxPushItem` can carry a `trace_id` (a UUID), which a transaction push stores.
- `commit()` returns a `TransactionResult`. A lost **required** KV precondition is returned, not
  thrown: `success == false`, `reason == "kv_precondition"` (`lost_precondition()`), with
  `failed_index`, `kv_reason`, `version` and `value`. It is the expected outcome of a legitimate
  redelivery. Every other rollback throws `queen::TransactionError` (with `.result()`), and a
  refusal by the proxy an `HttpError`.

```cpp
queen::KvWriteOptions gate;
gate.required = true;
auto step = queen.transaction()
    .ack(message)
    .kv("saga").put_if_absent("order-1", {{"claimed", true}}, queen::KvTtl::seconds(600), gate)
    .queue("orders-out").push({{"id", 1}}, "out-1")
    .commit();
if (step.lost_precondition()) {
    // a redelivery we have already handled: nothing in the bundle was written
}
```

## Key/Value state and scheduled messages

Both are part of the engine, like push and pop: nothing to turn on, nothing to probe. An operator
can pause either during an incident: a paused surface answers 503 with `Retry-After` on its routes
and refuses a transaction rider with 403, both as an `HttpError`.

### KV -- `queen.kv(ns)`

| Call | Answer (json) |
| --- | --- |
| `get(key)` | `{found, key, value, version, expiresAt, updatedAt}` |
| `get_many(keys)` | `{rows, missing, truncated}` |
| `get_prefix(prefix, KvPrefixOptions{after, limit, keys_only})` | `{rows, truncated, nextAfter}`; never inside a transaction |
| `put(key, value, ttl, KvWriteOptions{expect, required})` | `{applied, key, value, version, reason?}` |
| `put_if_absent(key, value, ttl, options)` | the same; exactly one concurrent caller wins |
| `del(key, options)` | `applied:false, reason:"absent"` when there was nothing |
| `check(key, version, required)` | the precondition alone, writing nothing |
| `incr(key, delta, ttl, KvIncrOptions{min, max, required})` | with `max`, `applied` is the admission decision |

`ttl` is a `KvTtl`: `KvTtl::seconds(n)`, `KvTtl::forever()` or `KvTtl::until(time_point)`. It is
mandatory on every write and exactly one thing.

**Status codes are never verdicts.** An absent key, a lost `put_if_absent` race and a delete that hit
nothing are HTTP 200 with an explicit field. Read `found` on a read and `applied` on a write:

```cpp
auto claimed = queen.kv("saga").put_if_absent("order-1", {{"state", "open"}},
                                              queen::KvTtl::seconds(600));
if (claimed["applied"].get<bool>()) {
    // we won; do the external effect
} else {
    json winner = claimed["value"];   // somebody else won; their value is here
}
```

- **Every write carries an expiry, and it never inherits one.** A key that expired is never
  returned and never counts as existing.
- **`put_if_absent` plus a TTL is not a distributed lock.** An expiry revokes nothing: carry the
  `version` you were given as `expect` on every later write, so a lapsed holder fails with
  `reason:"version"`.
- **With `max`, `applied` is the admission decision.** The call that would break the ceiling does
  not apply and returns the current value. Read a counter with `queen::kv_int64(value)`, which
  throws rather than lose precision.
- **No `once` helper** (the JS and Python clients have one): write the `put_if_absent` with
  `required` yourself, the same wire.
- `KvWriteOptions::expect` is a `std::optional`: compute the version before you build the options.

### Timers -- `queen.timers()`

```cpp
queen::TimerSchedule timer;
timer.queue = "compensation";
timer.timer_key = "saga-1";
timer.payload = {{"undo", "order-1"}};
timer.delay_millis = 30000;                    // RELATIVE, in milliseconds

auto out = queen.timers().schedule(timer);     // status: scheduled | rescheduled | too_late
json pending = queen.timers().peek("compensation", "saga-1");  // {found, deliverAt, payload, ...}
json page = queen.timers().list("compensation");               // {rows, truncated, nextAfter}
queen.timers().cancel("compensation", "saga-1", out["txn"]);
```

- A delay, never an absolute instant: one clock, the broker's. A delay in the past fires at once.
  `deliverAt` is **not before**, never exactly at.
- `schedule()` is the upsert: `status` says which happened, and a rescheduled timer gets a new
  `txn` and a reset retry budget.
- `peek` returns the payload as stored, base64 (encrypted when the queue encrypts at rest).
- `cancel()` uses its own route, which no quota blocks. In a bundle, a cancel shares the bundle's
  fate and authorization: use that form only when it must be atomic with the ack.
- **`status:"absent"` does not mean "never existed"**: a delivered timer has no row left. A
  compensation consumer checks the saga's KV state before compensating.

### Locks and semaphores

```cpp
queen::Lock lock = queen.lock("daily-report", 30);    // a 30 s lease, renewed in the background
if (!lock.acquire(5000)) return;                      // up to 5 s for our turn
auto result = queen.transaction()
    .guard(lock)                                      // commits only while the lock is ours
    .queue("reports").push(report, "report-2026-10-22")
    .commit();
lock.release();
```

A lock is a lease: one holder at a time, with a token that fences a holder that outlived it.
`queen.semaphore(name, n, ttl_seconds)` is the same with n permits; one handle is one permit.

- **It renews itself.** While held, a background thread renews the permit every third of the
  lifetime (`LockOptions::auto_renew`, on by default). `is_lost()` and `wait_lost(ms)` say when it
  is gone: the broker said so, or the lifetime passed with no renew. With `auto_renew = false`,
  call `keep_alive()` at the checkpoints of the work: it renews once a third of the lifetime has
  passed and returns false once the lock is gone.
- **It is a lease, not a mutex.** A paused process carries on past its lifetime while somebody else
  acquires. The guard keeps the old holder's work out: a guarded transaction rolls back with
  `kv_precondition` once the lock is no longer this handle's. Outside Queen, fence with `token()`,
  which only rises; it changes at every renew. A guard that lost only to the handle's own renewal
  (the token moved while the bundle was in flight) is sent again with the new token.
- `Lock` is movable, not copyable; the transaction keeps a pointer to it until `commit()`.
- `queen.locks()` is the wire with no state kept: `acquire`, `renew`, `release`, `get(name)`,
  `batch`.

## Ephemeral queues

RAM-class queues for request/reply, under `queen.ephemeral()`: `configure`, `reset`, `del`,
`push`, `pop`, `ack`, `queues`, `depth`. Their contents survive nothing (a restart, a crash, a
deploy); their configuration is durable. Read the note on `EphemeralBuilder` in the header first.

```cpp
auto inbox = "inbox-" + queen::util::generate_uuid_v7();          // a private reply queue
queen.ephemeral().push("price-requests", json::array({{{"sku", "A-1"}, {"replyTo", inbox}}}));
queen::EphemeralPopOptions reply_wait;
reply_wait.wait = true;
reply_wait.timeout_millis = 5000;
reply_wait.commit_on_delivery = true;
json reply = queen.ephemeral().pop(inbox, reply_wait);
```

## Reads by offset

```cpp
auto read = queen.queue("orders").fetch({"customer-42", "customer-7"});   // no lease, no ack
const auto& part = read["customer-42"];   // records {offset, transaction_id, payload, ts}, next_offset

queen::FetchOptions options;
options.offsets["customer-42"] = part.next_offset;                        // only what is new
auto later = queen.queue("orders").fetch({"customer-42"}, options);

auto changed = queen.queue("orders").changed_since(
    std::chrono::system_clock::now() - std::chrono::minutes(10));
auto newer = queen.queue("orders").changed_since(changed.safe_time);       // the next page of time
```

Neither call takes a lease, moves a cursor or needs an ack. `fetch` reads each partition up to
where it ended when the call first read it; `changed_since` lists the partitions written at or
after a time. Pass `safe_time` as the next `since`, and never compare it with your own clock.

## Admin

```cpp
queen::Admin admin = queen.admin();

queen::SeekOptions an_hour_ago;
an_hour_ago.timestamp = "2026-10-22T09:00:00Z";
admin.seek_consumer_group("billing", "orders", an_hour_ago);   // replay from a time
queen::SeekOptions to_end;
to_end.to_end = true;
admin.seek_consumer_group("billing", "orders", to_end);        // skip the backlog

json dead = queen.queue("payments").dlq("charger").limit(20).get();
for (const json& letter : dead["messages"]) {
    admin.retry_message(letter["partitionId"], letter["transactionId"]);  // back into its partition
}
```

Also: `overview`, `namespaces`, `tasks`, `list_queues`, `queue`, `queue_depth`, `partitions`,
`list_messages`, `message`, `delete_message`, `dlq`, `record_trace`, `trace_names`,
`traces_by_name`, `traces_for_message`, `list_consumer_groups`, `consumer_group`,
`lagging_consumers`, `delete_consumer_group`, `delete_consumer_group_for_queue`,
`seek_consumer_group_partition`, `set_subscription_timestamp`, `refresh_consumer_stats`,
`renew_lease`, `status`, `queue_stats`, `queue_detail`, `analytics`, `system_metrics`,
`worker_metrics`, `health`, `metrics` (Prometheus text). The answers are the broker's JSON. The
writes that change what consumers see are sent once, without the transport's resend.

## Queue configuration

```cpp
queen::QueueConfig jobs;
jobs.lease_time = 120;                    // seconds a worker holds a message
jobs.retry_limit = 5;                     // failures before the dead-letter queue
jobs.retry_delay = 10000;                 // MILLISECONDS before a failed message returns
jobs.dedup_window_seconds = 86400;        // how long a transactionId is remembered
jobs.retention_enabled = true;            // retention runs only when this is on
jobs.completed_retention_seconds = 3600;
queen.queue("jobs").namespace_name("shop").configure(jobs);
```

Every option is a `std::optional`, and only the ones set are sent; the broker's default owns the
rest. `configure()` is a full upsert: an option left unset goes back to its default, it does not
keep the value an earlier configure gave it. `config(cfg).create()` is the same call.

## Binary push, push compression and framed pop

Three opt-in forms of the push, pop and transaction requests. They change how messages travel
and nothing else: the calls, their results, deduplication and the delivery guarantees stay the
same. All three are off by default.

```cpp
queen::ClientConfig config;
config.binary_push = true;       // a push travels as a binary body
config.push_compression = true;  // with binary_push: framed and compressed here
config.framed_pop = true;        // a pop returns the stored records, unpacked here
queen::Queen fast("http://localhost:6632", config);
```

| Option | What it does |
| --- | --- |
| `binary_push` | A push, direct or buffered, sends its items as one length-prefixed binary body. |
| `push_compression` | With `binary_push`: each partition's batch is framed as the broker stores it, given its message ids and compressed with zstd, once. A transaction that pushes sends its pushes the same way. |
| `framed_pop` | Pops ask for the stored records (`frames=true`) and unpack them into the same messages. |

`push_compression` and `framed_pop` need zstd: define `QUEEN_CLIENT_ZSTD` and link `-lzstd`;
without it the client throws `std::invalid_argument` at construction when one of them is on.
`binary_push` and `push_compression` need broker 2.3.0 on every node (an older one answers 400 and
the client does not fall back). `framed_pop` is safe against any broker. A transaction with a trace
id on a push, a pop of a conflating group, and acks, KV, timers, locks and the DLQ stay JSON.

## The proxy and Queen Cloud

`bearer_token` sends `Authorization: Bearer <token>` on every request. The proxy's contract is
handled for you:

- **429 (rate limited)** is retried in place with ±20 % jittered backoff: `Retry-After` when sent,
  else `500 ms * 2^attempt` capped at 30 s. A long-poll pop backs off indefinitely, every other
  request gives up after 10 attempts (`retry_429.max_attempts`, `base_millis`, `cap_millis`
  change that). A 429 never fails over to another backend.
- **403** is terminal and surfaced at once.

## Errors

| Type | When |
| --- | --- |
| `queen::Error` | The base: a request that did not happen or did not succeed (a `std::runtime_error`). |
| `queen::HttpError` | A status of 400 or more: `status_code()`, `code()` (the proxy's, e.g. `rate_limited`, `cluster_suspended`), `body()`, `retry_after_seconds()`, `is_retryable()`, `is_terminal_refusal()`. |
| `queen::NetworkError` | No answer: the connection failed, broke or timed out. Retryable; for a write, its outcome is unknown. |
| `queen::TransactionError` | A transaction rolled back for a reason other than a lost precondition; `.result()`. |
| `queen::ConflationUnsupportedError` | Conflation asked of a broker older than 1.1. |
| `queen::BufferFlushError`, `queen::LockNotHeldError` | A flush deadline expired; a guard with no lock held. |
| `std::invalid_argument` | A misuse caught before anything is sent (no queue name, an ack of a message with no partition). |

A 503 or a timeout leaves a write's outcome unknown, and the client retries on its own: give every
push a transaction id derived from the work, and gate every step.

## Migrating from 2.7.0

| 2.7.0 | 2.7.1 |
| --- | --- |
| `QueenClient client(url)` | `queen::Queen queen(url)` (`QueenClient` still works) |
| `push({{{"data", x}}})`, `{"transactionId", id}` in the item | `push(x)`, `push(x, id)`, `push_many(...)`, `push_items(...)` |
| `push()` returned json | `std::vector<PushResult>` |
| `pop()` returned a json array, and `[]` on any failure | `std::vector<Message>`; throws on failure |
| `msg["data"]`, `msg["partitionId"]` | `msg.data`, `msg.partition_id` |
| `consume([](const json& batch) {...})` without `each()` | `consume_batch([](const std::vector<Message>&) {...})` |
| `.each().consume([](const json& msg) {...})` | `consume([](const Message&) {...})` |
| `consume(handler, &stop_flag)` | `.cancel(token).consume(handler)`, `token.cancel()` |
| `limit(n)` per worker | `limit(n)` across the workers |
| `concurrency(n)` capped at the number of cores | n threads |
| `ack(msg, true/false, {{"group", g}, {"error", e}})` | `ack(msg)`, `nack(msg, e)`, `ack_with(msg, AckStatus::..., e)`; the group comes from the message |
| `ack()` returned json, never threw | `AckResult`; throws when the call could not be made |
| `transaction().ack(msg, "completed", {{"consumerGroup", g}})` | `transaction().ack(msg)` |
| `transaction().queue(q).push({{{"data", x}}})`, no partition | `transaction().queue(q).partition(p).push(x, id)` |
| `commit()` returned json, threw `std::runtime_error` | `TransactionResult`; throws `TransactionError` |
| `QueueConfig` with nine fields, all sent | every option, `std::optional`, only the set ones sent |
| raw `get_http_client()->post(...)` for seek, DLQ replay, `getMany`, timer `peek` | `admin()`, `kv(ns).get_many()`, `timers().peek()` |
| `load_balancing_strategy = "round-robin"` | `load_balancing = LoadBalancing::Affinity` (the default) |
| `lock.keep_alive()` was the only renewal | the lock renews in the background; `is_lost()` |
| `renew()` returned json | `RenewResult` |
| `dlq().get()` returned an empty page on failure | throws |
| SIGINT/SIGTERM handlers always installed | none unless `ClientConfig::handle_signals` |
| an `https://` URL went as plain HTTP | HTTPS, certificate verified |
| one TCP connection per request | a keep-alive pool |
| `-I clients/server/vendor -I clients/server/include` | `-I clients/client-cpp`, or CMake `queen::client` |

## Tests

```bash
cd clients/client-cpp
make run-unit                        # every suite that needs no broker (in-process stubs)
make test && ./bin/test_client http://localhost:6632   # the live suite, against a broker
```

| Suite | What it pins |
| --- | --- |
| `test_retry429` | The proxy contract: bearer token, 429 backoff, terminal 403. |
| `test_kv_timers` | The KV and timer wire, standalone and as transaction riders. |
| `test_locks` | Locks, semaphores, `check` and the guard; background renewal. `<url>` adds a live half. |
| `test_conflation` | The conflation flag, the degrade-loudly error, the once-per-group warning. |
| `test_buffer` | The buffer: the blocking bound, a failed batch retried and never dropped. |
| `test_ephemeral` | The ephemeral routes and their two 404s. |
| `test_autopilot` | What pop autopilot puts on the wire. |
| `test_consumer` | The consume loop: settling, stops, limits, retries; ack and renew results. |
| `test_wire_forms` | The binary push, the framed pop and the binary transaction body. |
| `test_reads` | `fetch` and `changed_since`. `<url>` adds a live half. |
| `test_client` | End to end against a live broker. |

With CMake, `cmake -S clients/client-cpp -B build && cmake --build build && ctest --test-dir build`
runs the broker-free suites.

## Logging

`QUEEN_CLIENT_LOG=true` turns on the client's log lines: INFO on stdout, WARN and ERROR on stderr.
A conflation conflict and a buffer `close()` could not flush are printed on stderr whatever it
says. The client prints nothing else.

## Optional consumer supervision

```cpp
queen.queue("orders").group("billing")
    .supervision(queen::SupervisionConfig{"billing-production"})
    .concurrency(4).consume(handler);
```

Supervision is off by default; `.supervision(std::nullopt)` disables it. The group names an
application or deployment, independently of the consumer group. Each `consume()` call publishes a
unique `queen.consumer.status/v1` instance into the broker's `queen-supervisor` KV namespace every
10 seconds (30-second heartbeat timeout, 60-second TTL), plus a stopped status on an orderly exit.
Credentials need KV write access. A dedicated thread publishes with one attempt and a two-second
limit; failures do not affect consumers.

The Supervisors dashboard shows the worker threads, busy handlers, successful and failed handler
calls, the last completion and the oldest in-flight duration. Batch callbacks count once;
completion does not imply that the ack succeeded. Payloads and error text are never published.
Reporting does not restart processes, autoscale or enable remote commands.

Each `consume()` call gets a new instance id and fresh counters. A normal exit caused by `limit()`
or `idle_millis()` publishes `stopped`; repeated short calls show many stopped instances for a
process whose outer loop is still running. For a persistent consumer, enable supervision on one
call and stop it with a `Cancel`, then close the client. A scheduler that rotates queues through
short calls publishes the
[consumer status contract](https://queenmq.com/reference/supervisor-status/#reporting-an-application-owned-scheduler)
itself, with one id per scheduler lifetime.
