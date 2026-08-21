# 👑 Queen C++ Client

A comprehensive C++ client for Queen Message Queue, providing a fluent API matching the Node.js client.

## Features

- ✅ **Fluent API** - Chainable methods for intuitive queue operations
- ✅ **HTTP Client** - Built-in retry logic and failover support
- ✅ **Proxy/Cloud Ready** - Bearer-token auth, 429 backoff, terminal 403 codes
- ✅ **Load Balancing** - Round-robin and session-based strategies
- ✅ **Client-Side Buffering** - Automatic batching with time/count triggers
- ✅ **Consumer Groups** - Distributed message processing
- ✅ **Partitions** - Ordered message processing per partition
- ✅ **Atomic Transactions** - All-or-nothing operations
- ✅ **Key/Value State** - `get`, `put`, `putIfAbsent`, `delete`, `incr`, standalone or inside a transaction
- ✅ **Scheduled Messages** - `schedule` and `cancel` timers that push into a queue later
- ✅ **Lease Renewal** - Keep message locks active
- ✅ **Dead Letter Queue** - Query failed messages
- ✅ **Concurrent Consumers** - Uses `astp::ThreadPool` for parallel processing
- ✅ **UUIDv7 Generation** - Time-ordered unique identifiers
- ✅ **Graceful Shutdown** - Clean resource cleanup

## Dependencies

- **C++17 or later**
- **nlohmann/json** - JSON library (already in `server/vendor/json.hpp`)
- **cpp-httplib** - Header-only HTTP client (install separately or include)
- **astp::ThreadPool** - Thread pool library (already in `server/include/threadpool.hpp`)

### Installing cpp-httplib

```bash
# Option 1: System-wide installation (macOS with Homebrew)
brew install cpp-httplib

# Option 2: Download header-only library
curl -o httplib.h https://raw.githubusercontent.com/yhirose/cpp-httplib/master/httplib.h
```

## Quick Start

### Basic Usage

```cpp
#include "queen_client.hpp"

using namespace queen;

int main() {
    // Connect to Queen server
    QueenClient client("http://localhost:6632");
    
    // Create a queue
    client.queue("tasks").create();
    
    // Push a message
    client.queue("tasks").push({
        {{"data", {{"job", "send-email"}, {"to", "user@example.com"}}}}
    });
    
    // Pop a message
    auto messages = client.queue("tasks")
        .batch(1)
        .wait(false)
        .pop();
    
    if (!messages.empty()) {
        std::cout << "Received: " << messages[0] << std::endl;
    }
    
    client.close();
    return 0;
}
```

### Consumer Pattern

```cpp
#include "queen_client.hpp"

using namespace queen;

int main() {
    QueenClient client("http://localhost:6632");
    
    client.queue("tasks").create();
    
    // Long-running consumer
    client.queue("tasks").consume([](const json& msg) {
        std::cout << "Processing: " << msg["data"] << std::endl;
        // Process message here
        // Auto-ack on success, auto-nack on exception
    });
    
    return 0;
}
```

### With Partitions

```cpp
// Push to specific partition
client.queue("user-events")
    .partition("user-123")
    .push({
        {{"data", {{"event", "login"}, {"timestamp", time(nullptr)}}}}
    });

// Consume from partition
client.queue("user-events")
    .partition("user-123")
    .consume([](const json& msg) {
        // Messages in same partition are processed in order
        std::cout << "User 123 event: " << msg["data"]["event"] << std::endl;
    });
```

### Multi-Partition Pop (Drain Many Partitions Per Call)

```cpp
// One round-trip drains up to 200 messages spread across up to 50 partitions.
// batch(200) is the GLOBAL cap on total messages; partitions(50) is the
// hard cap on partitions claimed. All claimed partitions share one leaseId
// — a single renew() call extends every partition's lease atomically.
auto messages = client.queue("events")
    .batch(200)
    .partitions(50)
    .wait(true)
    .pop();

// Each message carries its own partition info (per-message partitionId,
// partition name, leaseId, consumerGroup) — ack and renew always work
// message-by-message regardless of how many partitions the batch spans.
for (const auto& m : messages) {
    std::cout << "from " << m["partition"] << ": " << m["data"].dump() << "\n";
}

// Same builder works on .consume() for long-running workers.
// batch(B) becomes a global cap on total messages across all claimed partitions.
client.queue("events")
    .batch(100)
    .partitions(8)
    .consume([](const json& msgs) {
        for (const auto& m : msgs) {
            // m["partitionId"] / m["partition"] baked in by the server
            process(m["data"]);
        }
    });
```

**When to use:** queues with many partitions where each partition only has
a handful of new messages per polling interval (per-customer event streams,
per-tenant work queues, per-device telemetry). Reduces network round-trips
from O(P) to O(P / N) while preserving per-partition FIFO ordering.

**When not to use:** few partitions, or each one busy enough to fill
`batch(B)` on its own. Default is `partitions(1)` which preserves the
legacy single-partition behaviour.

`.partitions(N)` only applies to **wildcard** pops; specifying
`.partition("name")` ignores the cap.

### Consumer Groups

```cpp
// Worker 1
client.queue("emails")
    .group("processors")
    .concurrency(2)
    .consume([](const json& msg) {
        send_email(msg["data"]);
    });

// Worker 2 (shares the load with Worker 1)
client.queue("emails")
    .group("processors")
    .concurrency(2)
    .consume([](const json& msg) {
        send_email(msg["data"]);
    });
```

### Client-Side Buffering

```cpp
// Buffer messages for performance
BufferOptions buffer_opts;
buffer_opts.message_count = 100;       // Flush after 100 messages
buffer_opts.time_millis = 1000;        // Or after 1 second
buffer_opts.max_size = 400;            // Backpressure bound (0 = 4 x message_count)
buffer_opts.retry_delay_millis = 250;  // Pause before retrying a failed batch (0 = 250)

for (int i = 0; i < 10000; i++) {
    client.queue("events")
        .buffer(buffer_opts)
        .push({
            {{"data", {{"id", i}, {"timestamp", time(nullptr)}}}}
        });
}

// Flush any remaining buffered messages (retries failed batches until they
// land; pass a deadline in ms to bound that -- see below)
client.flush_all_buffers();
```

The buffer is bounded and lossless under errors (the 1.0.6 buffer contract, shared with every
other SDK). At `max_size` waiting messages a buffered `push()` **blocks** until the flusher frees
room — "unbounded" is deliberately not expressible, because an unbounded buffer accepts messages
it will lose. A batch whose POST fails is re-queued at the **front** of the buffer, in order, and
retried every `retry_delay_millis` until it lands; it is never dropped. `flush_buffer(deadline_millis)`
and `flush_all_buffers(deadline_millis)` bound the retrying: on expiry they throw
`BufferFlushError`, whose `unflushed_count()` says how many messages are still buffered (still
buffered — not dropped). `close()` flushes under a 30 s deadline and reports anything left unsent;
a buffered push after `close()` throws.

### Transactions

```cpp
// Pop from input queue
auto messages = client.queue("raw-data").batch(1).pop();

if (!messages.empty()) {
    auto input = messages[0];
    
    // Process
    json processed = {{"result", input["data"]["value"].get<int>() * 2}};
    
    // Atomic: ack input + push output
    client.transaction()
        .ack(input)
        .queue("processed-data")
        .push({
            {{"data", processed}}
        })
        .commit();
}
```

### Queue Configuration

```cpp
QueueConfig config;
config.lease_time = 600;                    // 10 minutes
config.retry_limit = 5;
config.priority = 10;
config.delayed_processing = 60;             // 1 minute delay
config.max_size = 10000;
config.retention_seconds = 86400;           // 24 hours
config.encryption_enabled = true;

client.queue("important-tasks")
    .config(config)
    .create();
```

### Dead Letter Queue

```cpp
// Query DLQ for failed messages
auto dlq = client.queue("my-queue")
    .dlq()
    .limit(100)
    .from("2025-10-01")
    .to("2025-10-31")
    .get();

std::cout << "Failed messages: " << dlq["total"] << std::endl;

for (const auto& msg : dlq["messages"]) {
    std::cout << "Error: " << msg["errorMessage"] << std::endl;
}
```

### Lease Renewal

```cpp
// Pop message
auto messages = client.queue("long-tasks").pop();
auto msg = messages[0];

// Start long processing
std::thread processing_thread([&]() {
    // Renew lease every 30 seconds
    while (processing) {
        std::this_thread::sleep_for(std::chrono::seconds(30));
        client.renew(msg);
    }
});

// Do long work...
process_large_file(msg["data"]);

processing = false;
processing_thread.join();

// Acknowledge completion
client.ack(msg, true);
```

### High Availability

```cpp
// Connect to multiple servers for failover
std::vector<std::string> urls = {
    "http://server1:6632",
    "http://server2:6632",
    "http://server3:6632"
};

ClientConfig config;
config.load_balancing_strategy = "round-robin";
config.enable_failover = true;
config.retry_attempts = 3;

QueenClient client(urls, config);

// Requests are load-balanced across servers
// Automatic failover if a server is down
```

### Authentication (Queen Proxy / Cloud)

```cpp
ClientConfig config;
config.bearer_token = "eyJhbGciOi...";   // Authorization: Bearer <token> on every request

QueenClient client({"https://cell.example.com"}, config);
```

When the server is a Queen proxy it enforces per-tenant rate limits and quotas.
The client handles that contract for you:

- **429 (rate limited)** is retried in place with `±20%` jittered backoff —
  `Retry-After` (seconds) wins when present, otherwise `500ms * 2^attempt`
  capped at `30s`. A long-poll pop (`wait(true)`) backs off indefinitely; every
  other request gives up after 10 attempts. A 429 never fails over to another
  backend — it is a tenant-quota signal, not a backend-health one.
- **403 (forbidden)** is terminal and is surfaced immediately, never retried.

```cpp
ClientConfig config;
config.bearer_token = "...";
config.retry_429.max_attempts = 5;    // 0 = kind-based default (10, unbounded for waiting pop)
config.retry_429.base_millis = 500;   // 0 = 500
config.retry_429.cap_millis = 30000;  // 0 = 30000
```

Failed requests throw `queen::HttpError` (a `std::runtime_error`), which carries
the machine-readable proxy error code so callers do not have to match on message
text:

```cpp
try {
    client.queue("tasks").push({{{"data", {{"job", "test"}}}}});
} catch (const queen::HttpError& e) {
    // 429 -> "rate_limited" | "quota_exceeded"
    // 403 -> "cluster_suspended" | "storage_quota_exceeded" | "feature_gated" | "forbidden"
    std::cerr << e.status_code() << " [" << e.code() << "] " << e.what() << std::endl;

    if (e.is_cluster_suspended()) {
        // Terminal: only operator intervention resolves this. Stop producing.
    }
}
```

`consume()` applies the same rules per worker: it backs off through a 429 and
rethrows a 403 to the caller after its workers stop.

## Building

### Compile Test Suite

```bash
cd clients/client-cpp
make test
```

### Run Tests

```bash
# No broker needed: these serve their own responses in-process.
# retry429  = the proxy contract (bearer token, 429 backoff, terminal 403)
# kvtimers  = the KV/timer wire contract (exact JSON body, method and path)
make run-unit

# Against a live broker (localhost:6632 by default)
./bin/test_client
./bin/test_client http://my-server:6632
```

The KV and timer HTTP tests inside `test_client` run against every broker,
unconditionally: there is nothing to enable first, so a failure there is a real
failure. The wire contract is covered separately by `./bin/test_kv_timers`, and
that is where a wrong wire shape gets caught: a bundle whose KV riders sit in
the wrong place still commits against a live broker -- it merely commits without
the gate.

### Use in Your Project

#### Option 1: Include Header

```cpp
#include "path/to/queen_client.hpp"
```

#### Option 2: Add to Your Makefile

```makefile
INCLUDES = -I/path/to/queen/clients/client-cpp \
           -I/path/to/queen/server/vendor \
           -I/path/to/queen/server/include

LIBS = -lpthread

your_program: your_program.cpp
	g++ -std=c++17 $(INCLUDES) your_program.cpp -o your_program $(LIBS)
```

## Advanced Features

### Stop Signal for Consumers

```cpp
std::atomic<bool> stop_signal{false};

std::thread consumer_thread([&]() {
    client.queue("tasks").consume([](const json& msg) {
        // Process message
    }, &stop_signal);
});

// Later... gracefully stop consumer
stop_signal = true;
consumer_thread.join();
```

### Batch Processing

```cpp
// Process messages in batches of 10
client.queue("events")
    .batch(10)
    .consume([](const json& messages) {
        // messages is an array of 10 messages
        for (const auto& msg : messages) {
            process(msg);
        }
    });
```

### Concurrency

```cpp
// Run 5 concurrent workers
client.queue("tasks")
    .concurrency(5)
    .consume([](const json& msg) {
        // This handler runs in parallel across 5 workers
        process(msg);
    });
```

### Process Limit

```cpp
// Process exactly 100 messages then stop
client.queue("tasks")
    .limit(100)
    .consume([](const json& msg) {
        process(msg);
    });
```

### Manual ACK

```cpp
// Disable auto-ack for manual control
client.queue("tasks")
    .auto_ack(false)
    .consume([&](const json& msg) {
        try {
            process(msg);
            client.ack(msg, true);  // Manual success
        } catch (const std::exception& e) {
            client.ack(msg, false, {{"error", e.what()}});  // Manual failure
        }
    });
```

## Logging

Enable debug logging:

```bash
export QUEEN_CLIENT_LOG=true
./your_program
```

Output:
```
[2025-10-30T10:30:45.123Z] [INFO] [HttpClient.request] POST /api/v1/push
[2025-10-30T10:30:45.234Z] [INFO] [QueueBuilder.push] queue=tasks count=1
```

## API Reference

### QueenClient

- `QueenClient(url)` - Create client with single server
- `QueenClient(urls, config)` - Create client with multiple servers
- `queue(name)` - Get queue builder
- `transaction()` - Create transaction builder
- `ack(message, status, context)` - Acknowledge message(s)
- `renew(message)` - Renew message lease
- `flush_all_buffers(deadline_millis = -1)` - Flush all client-side buffers (retries failed batches; a deadline bounds that and throws `BufferFlushError` on expiry)
- `get_buffer_stats()` - Get buffer statistics
- `close()` - Graceful shutdown

### QueueBuilder

**Configuration:**
- `namespace_name(name)` - Set namespace
- `task(name)` - Set task type
- `config(options)` - Set queue configuration
- `partition(name)` - Set partition
- `group(name)` - Set consumer group

**Operations:**
- `create()` - Create queue
- `del()` - Delete queue
- `push(messages)` - Push messages
- `pop()` - Pop messages
- `consume(handler)` - Start consumer
- `dlq()` - Query dead letter queue

**Consumer Options:**
- `concurrency(count)` - Set worker count
- `batch(size)` - Set batch size
- `limit(count)` - Set message limit
- `auto_ack(enabled)` - Enable/disable auto-ack
- `wait(enabled)` - Enable/disable long polling
- `renew_lease(enabled, interval)` - Auto-renew leases

**Buffering:**
- `buffer(options)` - Enable client-side buffering (bounded: blocks at `max_size`)
- `flush_buffer(deadline_millis = -1)` - Flush queue buffer

### TransactionBuilder

- `ack(message)` - Add ack operation
- `queue(name).push(messages)` - Add push operation
- `kv(ns).get|put|put_if_absent|del|incr(...)` - Add a KV rider
- `timers().schedule|cancel(...)` - Add a timer rider
- `commit()` - Execute transaction atomically. **Returns** on a lost KV precondition, throws on everything else

### KvBuilder -- `client.kv(ns)`

- `get(key)` - `{found, key, value, version, expiresAt, updatedAt}`
- `put(key, value, ttl, options)` - upsert, or a compare-and-set with `options.expect`
- `put_if_absent(key, value, ttl, options)` - claim, exactly one winner
- `del(key, options)` - delete
- `incr(key, delta, ttl, options)` - atomic counter with an optional ceiling

`ttl` is a `KvTtl`: `KvTtl::seconds(n)`, `KvTtl::forever()`, or
`KvTtl::until(time_point)`. It is **mandatory** on every write and it is exactly
one thing -- the type makes "both" and "neither" inexpressible.

### TimersBuilder -- `client.timers()`

- `schedule(TimerSchedule)` - `{ok, status, queue, timerKey, txn, messageId, deliverAt}`
- `cancel(queue, timerKey, txn)` - `{ok, status, queue, timerKey, txn}`

## Key/Value state and scheduled messages

Both surfaces are **always there**. They are part of the engine, like push and
pop: there is no flag to turn on before the first call, and nothing to probe
for. An operator can still **pause** either one during an incident, through the
runtime kill switch on `/api/v1/system/kv-timers` -- and that is a different
thing from an absent feature. A paused surface answers **503** with
`Retry-After` on its own routes, and refuses a transaction rider permanently
with **403**; both arrive as an `HttpError`. Nothing answers 404 for being
switched off, and neither answer means "the key is missing".

### The rules that bite

**Status codes are never verdicts.** An absent key, a lost `put_if_absent` race
and a delete that hit nothing are all HTTP 200 with an explicit field. They
arrive as **values**, never as exceptions. Read `found` on a read and `applied`
on a write:

```cpp
auto claimed = client.kv("saga").put_if_absent("order-1", {{"state","open"}},
                                               KvTtl::seconds(600));
if (claimed["applied"].get<bool>()) {
    // we won; do the external effect
} else {
    // somebody else won. Their value is right here -- no second round trip.
    json winner = claimed["value"];
}
```

**Every write carries an expiry, and it never inherits one.** A `put` with no
TTL is not expressible. A key that expired is never returned and never counts as
existing, whether or not the sweeper has pruned the row yet.

**`put_if_absent` plus a TTL is not a distributed lock.** An expiry revokes
nothing: the old holder keeps working, it simply no longer has the row. Carry
the `version` you were given as `expect` on every later write so a lapsed holder
fails with `reason:"version"` instead of overwriting the new one.

**With `max`, `applied` is the admission decision.** `incr` does not saturate and
does not truncate -- the call that would break the ceiling does not apply and
returns the current value, so a rate limiter never spends budget it then has to
un-spend:

```cpp
KvIncrOptions limited;
limited.max = 1000;
auto out = client.kv("quota").incr("acme:2026-08", 1, KvTtl::seconds(3600), limited);
bool admitted = out["applied"].get<bool>();
long long used = kv_int64(out["value"]);   // throws rather than lose precision
```

### KV inside a transaction -- the gate

The ack transaction is the primary fence; `expect` is the secondary assertion. A
state write that shares the transaction with the ack is undone when an expired
lease makes the ack fail, which a compare-and-set cannot do. `required` escalates
a lost precondition into a rollback:

```cpp
KvWriteOptions gate;
gate.required = true;

json result = client.transaction()
    .queue("orders-out").push({{{"data", {{"id", 1}}}}})
    .ack(message)
    .kv("saga").put_if_absent("order-1", {{"claimed", true}}, KvTtl::seconds(600), gate)
    .commit();

if (!result.value("success", false)) {
    // reason == "kv_precondition": a redelivery we have already handled.
    // NOT an exception, NOT a retry -- failedIndex, kvReason, version and value
    // say exactly what lost and to whom.
}
```

`commit()` **returns** that verdict instead of throwing, because it is the
expected outcome of every legitimate redelivery. Every other failure still
throws, and a 403/429 from the gateway still arrives as an `HttpError`.

### Timers

```cpp
TimerSchedule timer;
timer.queue = "compensation";
timer.timer_key = "saga-1";
timer.payload = {{"undo", "order-1"}};
timer.delay_millis = 30000;                  // RELATIVE, in milliseconds

auto out = client.timers().schedule(timer);  // status: scheduled | rescheduled | too_late
client.timers().cancel("compensation", "saga-1", out["txn"]);
```

- `delayMs`, never an absolute instant: one clock, and it is the database's.
  Durations that can be sub-second are in milliseconds; the ones that cannot
  (a TTL) are in seconds. A delay in the past is legal and fires immediately.
- `deliverAt` is **not before**, never **exactly at**.
- There is no separate `reschedule()`. `schedule()` is the upsert; `status` tells
  you which happened, and a rescheduled timer gets a **new** `txn` and a reset
  retry budget.
- `cancel()` uses `DELETE /api/v1/timers/{queue}/{timerKey}`, its own route,
  which is the one guaranteed never to be blocked by a quota. A cancel sent
  inside a bundle inherits the bundle's authorization instead -- use the bundle
  form only when the cancel must be atomic with the ack.
- **`status:"absent"` carries `ok:false` and does not mean "never existed".**
  There is no tombstone: a delivered timer has no row left, so `absent` means
  "no longer pending", which **includes already delivered**. The authority is
  the log -- look for the returned `txn` in the destination queue. A compensation
  consumer must therefore check the saga's KV state before compensating: the
  timer may have fired five milliseconds before your cancel arrived.
- Billing follows the promise: a schedule counts one message, a reschedule
  counts another, a cancel counts zero and is not refunded.

### What this client does not have

Stated rather than implied. The broker has all of these; this client does not
wrap them, so use the HTTP routes directly if you need them.

| Missing here | Where it lives on the broker |
|---|---|
| `getMany` | `POST /api/v1/kv`, op `getMany` |
| `getPrefix` | `POST /api/v1/kv`, op `getPrefix` -- body only, never a query string, and refused inside a transaction |
| timer `peek` | `GET /api/v1/timers/{queue}/{timerKey}` |
| timer `list` | `GET /api/v1/timers/{queue}` |
| an `once(...)` helper | client-js and client-py; elsewhere it is `put_if_absent` plus `required` |
| a streams SDK | client-js, client-go, client-py, client-rust. No client exposes `kv` on a stream operator: inside a cycle the state primitive is `state_ops`, which commits with the sink and the ack |

Two smaller gaps against client-js, for the same honesty:

- **No "explicitly undefined `expect`" guard.** In JavaScript, writing
  `expect: undefined` means the caller intended to fence and computed nothing,
  and that client treats it as a bug rather than a silent downgrade to upsert.
  In C++ an unset `std::optional` is indistinguishable from never having asked,
  so compute the version before you build the options.
- **The KV path routes are not used**, so the `ETag` header they return is not
  surfaced. Everything goes through `POST /api/v1/kv`, which is the only route
  carrying `incr` anyway. The ETag saves bandwidth, never the round trip.

## Error Handling

```cpp
try {
    client.queue("tasks").push({
        {{"data", {{"value", 123}}}}
    });
} catch (const std::runtime_error& e) {
    std::cerr << "Push failed: " << e.what() << std::endl;
}
```

## Best Practices

1. **Use buffering for high throughput** - Batch messages for better performance
2. **Enable auto-ack for simplicity** - Let the client handle acknowledgments
3. **Use partitions for ordering** - Messages in same partition are processed in order
4. **Consumer groups for scaling** - Run multiple workers to share the load
5. **Transactions for atomicity** - Ensure all-or-nothing operations
6. **Graceful shutdown** - Always call `client.close()` before exiting

## Comparison with Node.js Client

The C++ client API closely matches the Node.js client:

**Node.js:**
```javascript
await queen.queue('tasks').push([{data: {job: 'test'}}]);
```

**C++:**
```cpp
client.queue("tasks").push({{{"data", {{"job", "test"}}}}});
```

**Node.js:**
```javascript
await queen.queue('tasks').consume(async (msg) => {
    console.log(msg.data);
});
```

**C++:**
```cpp
client.queue("tasks").consume([](const json& msg) {
    std::cout << msg["data"] << std::endl;
});
```

## License

Same as Queen Message Queue project.

## Support

For issues and questions, please refer to the main Queen repository.

