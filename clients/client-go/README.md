# Queen MQ Go Client

A high-performance Go client for [Queen MQ](https://github.com/queen-mq/queen) - a message queue backed by PostgreSQL.

## Installation

```bash
go get github.com/smartpricing/queen/clients/client-go
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    queen "github.com/smartpricing/queen/clients/client-go"
)

func main() {
    // Create client
    client, err := queen.New("http://localhost:6632")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close(context.Background())

    ctx := context.Background()

    // Create a queue
    _, err = client.Queue("my-queue").Create().Execute(ctx)
    if err != nil {
        log.Fatal(err)
    }

    // Push a message
    _, err = client.Queue("my-queue").Push(map[string]interface{}{
        "hello": "world",
    }).Execute(ctx)
    if err != nil {
        log.Fatal(err)
    }

    // Consume messages
    err = client.Queue("my-queue").
        Limit(1).
        Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
            fmt.Printf("Received: %v\n", msg.Data)
            return nil
        }).Execute(ctx)
    if err != nil {
        log.Fatal(err)
    }
}
```

## Streaming SDK (`streams` subpackage)

The `streams` subpackage adds a fluent streaming pipeline builder with
windowing, aggregation, gates (rate limiting), and exactly-once semantics
via `/streams/v1/cycle`. 1:1 feature-equivalent with the JS / Python
streaming SDKs.

```go
import (
    queen "github.com/smartpricing/queen/clients/client-go"
    "github.com/smartpricing/queen/clients/client-go/streams"
    "github.com/smartpricing/queen/clients/client-go/streams/helpers"
)

q, _ := queen.New("http://localhost:6632")

// Token-bucket rate limiter via .Gate()
gateFn := helpers.TokenBucketGate(helpers.TokenBucketGateOptions{
    Capacity:     100,
    RefillPerSec: 100,
})

stream := streams.From(q.Queue("requests").AsStreamSource()).
    Gate(gateFn).
    To(q.Queue("approved"))

runner, _ := stream.Run(context.Background(), streams.RunOptions{
    QueryID: "rate-limiter.airbnb",
    URL:     "http://localhost:6632",
})
defer runner.Stop()
```

**Available operators:** `Map`, `Filter`, `FlatMap`, `KeyBy`,
`WindowTumbling`, `WindowSliding`, `WindowSession`, `WindowCron`,
`Reduce`, `Aggregate`, `Gate`, `To`, `Foreach`.

**Helpers:** `helpers.TokenBucketGate(...)`, `helpers.SlidingWindowGate(...)`.

**Cross-language compatibility:** the SHA-256 `config_hash` of an operator
chain is identical across the JS, Python, and Go SDKs for the same logical
shape, so a query registered by one client can be resumed by a worker
written in another. Pass an explicit `fieldOrder` to `Aggregate` to
guarantee deterministic hashing across languages.

See [`examples/00-demo`](examples/00-demo/main.go) and
[`examples/01-stateless-enrichment`](examples/01-stateless-enrichment/main.go)
for runnable starting points.

## Features

- **Fluent API** - Intuitive builder pattern for all operations
- **Load Balancing** - Round-robin, session, and affinity-based strategies
- **Failover** - Automatic failover with health tracking
- **Client-side Buffering** - Batch messages for improved throughput
- **Consumer Groups** - Kafka-style consumer groups
- **Transactions** - Atomic ack + push operations
- **Lease Renewal** - Automatic lease renewal for long-running tasks
- **Tracing** - Built-in message tracing support

## Configuration

```go
// Single server
client, _ := queen.New("http://localhost:6632")

// Multiple servers with load balancing
client, _ := queen.New(queen.ClientConfig{
    URLs: []string{
        "http://server1:6632",
        "http://server2:6632",
    },
    LoadBalancingStrategy: "affinity",  // or "round-robin", "session"
    EnableFailover:        true,
    TimeoutMillis:         30000,
    RetryAttempts:         3,
    BearerToken:           "your-token",
})
```

## Queue Operations

### Create/Delete Queue

```go
// Create queue with default settings
client.Queue("my-queue").Create().Execute(ctx)

// Create queue with configuration
client.Queue("my-queue").Config(queen.QueueConfig{
    LeaseTime:         60,    // seconds
    RetryLimit:        3,     // retries before DLQ
    Priority:          10,    // higher = processed first
    MaxSize:           1000,  // max messages
    EncryptionEnabled: true,
}).Create().Execute(ctx)

// Delete queue
client.Queue("my-queue").Delete().Execute(ctx)
```

### Push Messages

```go
// Simple push
client.Queue("my-queue").Push(map[string]interface{}{
    "data": "value",
}).Execute(ctx)

// Push to specific partition
client.Queue("my-queue").Partition("partition-1").Push(data).Execute(ctx)

// Push with custom transaction ID
client.Queue("my-queue").Push(data).TransactionID("custom-id").Execute(ctx)

// Push multiple messages
client.Queue("my-queue").Push([]interface{}{msg1, msg2, msg3}).Execute(ctx)

// Buffered push (batches messages)
client.Queue("my-queue").Buffer(queen.BufferConfig{
    MessageCount: 100,  // flush after 100 messages
    TimeMillis:   1000, // or after 1 second
}).Push(data).Execute(ctx)

// Flush buffer manually
client.Queue("my-queue").FlushBuffer(ctx)
```

### Pop Messages

```go
// Pop single message
messages, err := client.Queue("my-queue").Pop(ctx)

// Pop with long polling
messages, err := client.Queue("my-queue").
    Wait(true).
    TimeoutMillis(30000).
    Pop(ctx)

// Pop batch
messages, err := client.Queue("my-queue").
    Batch(10).
    Pop(ctx)

// Pop with consumer group
messages, err := client.Queue("my-queue").
    Group("my-group").
    Pop(ctx)
```

### Consume Messages

```go
// Basic consumer
client.Queue("my-queue").
    Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
        // Process message
        return nil
    }).Execute(ctx)

// Consumer with options
client.Queue("my-queue").
    Group("my-group").           // Consumer group
    Concurrency(5).              // 5 parallel workers
    Batch(10).                   // 10 messages per poll
    Limit(100).                  // Stop after 100 messages
    IdleMillis(30000).           // Stop after 30s idle
    AutoAck(true).               // Auto-ack on success
    RenewLease(true, 10000).     // Renew lease every 10s
    Consume(ctx, handler).
    Execute(ctx)

// Batch consumer
client.Queue("my-queue").
    Batch(10).
    ConsumeBatch(ctx, func(ctx context.Context, msgs []*queen.Message) error {
        for _, msg := range msgs {
            // Process
        }
        return nil
    }).Execute(ctx)

// Consumer with subscription mode
client.Queue("my-queue").
    Group("my-group").
    SubscriptionMode(queen.SubscriptionModeNew). // Skip historical
    Consume(ctx, handler).
    Execute(ctx)
```

### Multi-Partition Pop (Drain Many Partitions Per Call)

```go
// One round-trip drains up to 200 messages spread across up to 50 partitions.
// Batch(200) is the GLOBAL cap on total messages; Partitions(50) is the
// hard cap on partitions claimed. All claimed partitions share one LeaseID
// — a single Renew() call extends every partition's lease atomically.
messages, err := client.Queue("events").
    Batch(200).
    Partitions(50).
    Wait(true).
    Pop(ctx)

// Each message carries its own partition info (per-message PartitionID,
// Partition name, LeaseID) — ACK and renew always work message-by-message
// regardless of how many partitions the batch spans.
for _, m := range messages {
    fmt.Printf("from %s: %v\n", m.Partition, m.Data)
}

// Same builder works on .Consume() for long-running workers.
// Batch(B) becomes a global cap on total messages across all claimed partitions.
client.Queue("events").
    Batch(100).
    Partitions(8).
    ConsumeBatch(ctx, func(ctx context.Context, msgs []*queen.Message) error {
        for _, m := range msgs {
            // m.PartitionID / m.Partition baked in by the server
            if err := process(m.Data); err != nil { return err }
        }
        return nil
    }).Execute(ctx)
```

**When to use:** queues with many partitions where each partition only has
a handful of new messages per polling interval (per-customer event streams,
per-tenant work queues, per-device telemetry). Reduces network round-trips
from O(P) to O(P / N) while preserving per-partition FIFO ordering.

**When not to use:** few partitions, or each one busy enough to fill
`Batch(B)` on its own. Default is `Partitions(1)` which preserves the
legacy single-partition behaviour.

`.Partitions(N)` only applies to **wildcard** pops; specifying
`.Partition("name")` ignores the cap.

### Acknowledge Messages

```go
// Ack single message
client.Ack(ctx, msg, true, queen.AckOptions{})

// Nack (reject) message
client.Ack(ctx, msg, false, queen.AckOptions{
    Error: "Processing failed",
})

// Batch ack
client.Ack(ctx, messages, true, queen.AckOptions{
    ConsumerGroup: "my-group",
})
```

### Renew Lease

```go
// Renew single message
client.Renew(ctx, msg)

// Renew multiple messages
client.Renew(ctx, messages)

// Renew by lease ID
client.Renew(ctx, "lease-id-123")
```

## Transactions

Atomic operations that either all succeed or all fail:

```go
result, err := client.Transaction().
    Ack(inputMsg, queen.AckStatusCompleted, queen.AckOptions{}).
    Queue("output-queue").Push(processedData).
    Commit(ctx)

if result.Success {
    fmt.Println("Transaction committed")
}
```

## Key/Value

Transactional state on the same Postgres, and on the same commit as your
messages. Every write states its lifetime: exactly one of a TTL and `Forever`,
never neither.

```go
kv := client.KV()

// putIfAbsent is the idempotency marker: applied answers "did I win?", and the
// LOSER gets the winner's value in the same answer, with no second round trip.
res, err := kv.PutIfAbsent(ctx, "saga", "order-7", map[string]any{"owner": "worker-1"}, queen.TTLSeconds(3600))
if err != nil {
    return err
}
if !res.Applied {
    // res.Reason == queen.KVReasonExists, res.Value is the winner's
    return nil // somebody else owns this order
}

// Reads. KVGetAs is the only generic function of the package.
type state struct{ Step string `json:"step"` }
s, found, err := queen.KVGetAs[state](ctx, kv, "saga", "order-7")

// A rate limiter in ONE call: with Max, Applied IS the admission decision, and
// a refused request has consumed no budget.
hit, err := kv.Incr(ctx, "quota", "acme:2026-08-17", 1, queen.TTL(time.Hour), queen.KVIncrOptions{Max: queen.Int64(1000)})
if !hit.Applied {
    return errTooManyRequests
}
```

**`applied:false` is not an error.** A lost race, a missing key, a delete that
hit nothing and an incr over its ceiling are all successful calls with an
explicit field in the body: check `Applied`/`Found`, not `err`.

The whole point is the transaction: the write and the messages commit together.

```go
resp, err := client.Transaction().
    Ack(msg, queen.AckStatusCompleted, queen.AckOptions{}).
    Queue("emails").Push(email).
    KV(queen.KVPutIfAbsentOp("sent", msg.TransactionID, true, queen.TTLSeconds(86400),
        queen.KVWriteOptions{Required: true})).
    Commit(ctx)

if err != nil {
    return err // the bundle did not commit
}
if resp.IsKVPrecondition() {
    // A redelivery: somebody already sent this email, so nothing was pushed and
    // nothing was acked. This is RETURNED, not raised -- it is the expected
    // outcome of a legitimate redelivery, not a failure.
}
```

`Required: true` is what makes it a gate: without it a lost precondition is only
a verdict in the results and the messages still go out.

## Timers

A message you schedule now and the broker delivers later, into a real queue.

```go
timers := client.Timers()

_, err := timers.Schedule(ctx, queen.TimerSchedule{
    Queue:    "compensations",
    TimerKey: "order-7",          // identity: scheduling it again is an upsert
    Delay:    30 * time.Minute,   // milliseconds on the wire; DeliverAt is sugar
    Payload:  map[string]any{"orderId": 7},
})

// Cancel goes through its own route, the one that is never blocked.
res, err := timers.Cancel(ctx, "compensations", "order-7")
```

Three things the API cannot hide from you:

- **`deliverAt` is "no earlier than", never "exactly at".**
- **`absent` on a cancel may mean ALREADY DELIVERED.** There is no tombstone: a
  fired timer has no row. The answer carries the `txn` of the message, so the
  authority is the destination queue. Any saga that cancels a compensation timer
  must have the compensation consumer check the saga's KV state before acting.
- **`too_late` is a verdict**, not a failure: a broker is already committing that
  payload. Bounded by the lease.

Both surfaces are **always there**. There is no flag to turn on first: KV and
timers are part of the engine, the way push and pop are, and every broker that
answers has them.

What an operator can still do, live and during an incident, is **pause** one of
them (`kv_enabled`, `timers_schedule_enabled`, `timers_fire_enabled` in
`queen.system_state`). That is a **503** with `Retry-After` and the code
`kv_disabled` / `timers_disabled` on `/api/v1/kv` and `/api/v1/timers` — back off
and come back — and a **403** on a `kv`/`timers` rider inside
`POST /api/v1/transaction`, permanent on purpose, because a bundle carries
messages and retrying it in a loop against a paused cell is a storm on the hot
path. Both arrive as `*queen.SurfaceError` (the transaction also hands back its
`TransactionResponse`, whose `Reason` carries the code): branch on `Code` or
`Reason`, never on the prose.

## Dead Letter Queue

```go
// Query DLQ
response, err := client.Queue("my-queue").
    DLQ("my-group").
    Limit(100).
    From("2024-01-01T00:00:00Z").
    To("2024-01-02T00:00:00Z").
    Get(ctx)

for _, msg := range response.Messages {
    fmt.Printf("DLQ message: %v\n", msg.Data)
}
```

## Admin API

```go
admin := client.Admin()

// Health check
health, _ := admin.Health(ctx)

// List queues
queues, _ := admin.ListQueues(ctx, queen.ListQueuesParams{})

// Get queue details
queue, _ := admin.GetQueue(ctx, "my-queue")

// Consumer groups
groups, _ := admin.ListConsumerGroups(ctx)
group, _ := admin.GetConsumerGroup(ctx, "my-group")

// System metrics
metrics, _ := admin.GetSystemMetrics(ctx, "", "")
```

## Message Tracing

```go
client.Queue("my-queue").
    Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
        // Add trace to message
        msg.Trace(ctx, queen.TraceConfig{
            TraceName: "processing-started",
            EventType: "info",
            Data: map[string]interface{}{
                "processor": "worker-1",
            },
        })
        
        // Process...
        
        msg.Trace(ctx, queen.TraceConfig{
            TraceName: "processing-completed",
            Data: map[string]interface{}{
                "duration_ms": 150,
            },
        })
        
        return nil
    }).Execute(ctx)
```

## Error Handling

```go
// Check HTTP errors
if httpErr, ok := err.(*queen.HTTPError); ok {
    fmt.Printf("HTTP %d: %s\n", httpErr.StatusCode, httpErr.Body)
}

// Transaction errors
result, err := client.Transaction().Ack(msg, "completed", queen.AckOptions{}).Commit(ctx)
if err != nil {
    // Network/request error
}
if !result.Success {
    // Transaction failed
    fmt.Printf("Transaction error: %s\n", result.Error)
}
```

## Logging

Enable debug logging with the `QUEEN_CLIENT_LOG` environment variable:

```bash
QUEEN_CLIENT_LOG=debug ./myapp
```

Log levels: `debug`, `info`, `warn`, `error`

## Testing

Run the test suite (requires a running Queen server):

```bash
# Set server URL
export QUEEN_SERVER_URL=http://localhost:6632

# Set database connection for cleanup
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=queen
export PG_USER=postgres
export PG_PASSWORD=postgres

# Run tests
cd clients/client-go
go test ./tests/... -v
```

## API Reference

### Types

- `Queen` - Main client
- `QueueBuilder` - Fluent API for queue operations
- `PushBuilder` - Fluent API for push operations
- `ConsumeBuilder` - Fluent API for consume operations
- `TransactionBuilder` - Fluent API for transactions
- `KV` - Key/value API (`client.KV()`), plus `Expiry`, `KVOp`, `KVResult` and `KVGetAs[T]`
- `Timers` - Timer API (`client.Timers()`), plus `TimerSchedule`, `TimerOp`, `TimerResult`
- `SurfaceError` - Error envelope of the kv/timer surfaces (`Code`, `Reason`, `Detail`)
- `Admin` - Administrative API
- `Message` - Message structure
- `ClientConfig` - Client configuration
- `QueueConfig` - Queue configuration
- `BufferConfig` - Buffer configuration

### Default Values

| Setting | Default |
|---------|---------|
| Timeout | 30 seconds |
| Retry attempts | 3 |
| Retry delay | 1 second (exponential) |
| Load balancing | affinity |
| Concurrency | 1 |
| Batch size | 1 |
| Auto-ack | true |
| Wait (long poll) | true (consume), false (pop) |
| Buffer count | 100 |
| Buffer time | 1 second |

## License

Apache-2.0
