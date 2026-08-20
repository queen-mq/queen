# Queen MQ - JavaScript Client

<div align="center">

**Modern, high-performance message queue client for Node.js**

[![npm](https://img.shields.io/npm/v/queen-mq.svg)](https://www.npmjs.com/package/queen-mq)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.md)
[![Node](https://img.shields.io/badge/node-%3E%3D22.0.0-brightgreen.svg)](https://nodejs.org/)

[Quick Start](#quick-start) • [Complete Guide](client-v2/README.md) • [Examples](#examples) • [API Reference](#api-reference)

</div>

---

## What is Queen MQ?

Queen MQ is a PostgreSQL-backed message queue system with a powerful feature set:

- **FIFO Partitions** - Unlimited ordered partitions within queues
- **Consumer Groups** - Kafka-style consumer groups for scalability
- **Flexible Semantics** - Exactly-once, at-least-once, and at-most-once delivery
- **Transactions** - Atomic operations across push and ack
- **High Performance** — 104K msg/s push, 165K msg/s fan-out with consumer groups on a single 32-core node ([benchmarks](https://github.com/queen-mq/queen/tree/master/benchmark-queen/2026-04-26))
- **Subscription Modes** - Process from beginning, new messages only, or from timestamp
- **Dead Letter Queue** - Automatic failure handling and monitoring
- **Message Tracing** - Debug distributed workflows with trace timelines
- **Client-Side Buffering** - 10x-100x throughput boost for high-volume pushes
- **Real-time Streaming** - Windowed aggregation and processing
- **Key/Value State and Timers** - Transactional state and scheduled messages

This client provides a fluent, promise-based API for Node.js applications.

---

## Installation

```bash
npm install queen-mq
```

**Requirements:** Node.js 22+

---

## Quick Start

```javascript
import { Queen } from 'queen-mq'

// Connect to Queen server
const queen = new Queen('http://localhost:6632')

// Create a queue
await queen.queue('tasks').create()

// Push messages
await queen.queue('tasks').push([
  { data: { task: 'send-email', to: 'alice@example.com' } }
])

// Consume messages
await queen.queue('tasks').consume(async (message) => {
  console.log('Processing:', message.data)
  // Auto-ack on success, auto-retry on error
})
```

---

## Core Concepts

### Queues

Logical containers for messages with configurable settings:
- **Lease time** - How long a consumer has to process a message
- **Retry limit** - Number of retry attempts before DLQ
- **Priority** - Queue priority for multi-queue consumers
- **Encryption** - Message payload encryption at rest
- **Retention** - Automatic cleanup policies

```javascript
await queen.queue('orders')
  .config({
    leaseTime: 300,        // 5 minutes
    retryLimit: 3,
    priority: 5,
    encryptionEnabled: false
  })
  .create()
```

### Partitions

Ordered lanes within a queue. Messages in the same partition are processed sequentially:

```javascript
// All messages for user-123 are processed in order
await queen.queue('user-events')
  .partition('user-123')
  .push([
    { data: { event: 'login' } },
    { data: { event: 'view-page' } },
    { data: { event: 'logout' } }
  ])
```

**Use cases:**
- Per-user ordering
- Per-tenant isolation
- Sharding for parallelism

### Consumer Groups

Multiple consumers sharing work, with independent progress tracking:

```javascript
// Worker 1 & 2 share the load
await queen.queue('emails')
  .group('processors')
  .consume(async (message) => {
    await sendEmail(message.data)
  })

// Separate group processes same messages independently
await queen.queue('emails')
  .group('analytics')
  .consume(async (message) => {
    await logMetrics(message.data)
  })
```

### Subscription Modes

Control whether consumer groups process historical messages:

```javascript
// Default: Process ALL messages (including backlog)
await queen.queue('events')
  .group('batch-analytics')
  .consume(async (message) => { /* all messages */ })

// Skip history, only new messages
await queen.queue('events')
  .group('realtime-monitor')
  .subscriptionMode('new')
  .consume(async (message) => { /* new only */ })

// Start from specific timestamp
await queen.queue('events')
  .group('replay')
  .subscriptionFrom('2025-10-28T10:00:00.000Z')
  .consume(async (message) => { /* from timestamp */ })
```

---

## Connection Options

### Single Server

```javascript
const queen = new Queen('http://localhost:6632')
```

### Multiple Servers (High Availability)

```javascript
const queen = new Queen([
  'http://server1:6632',
  'http://server2:6632'
])
```

### Full Configuration

```javascript
const queen = new Queen({
  urls: ['http://server1:6632', 'http://server2:6632'],
  timeoutMillis: 30000,
  retryAttempts: 3,
  loadBalancingStrategy: 'affinity',  // or 'round-robin', 'session'
  enableFailover: true
})
```

---

## Basic Usage Patterns

### Push Messages

```javascript
// Simple push
await queen.queue('tasks').push([
  { data: { job: 'resize-image', imageId: 123 } }
])

// With partition
await queen.queue('tasks')
  .partition('tenant-456')
  .push([{ data: { action: 'process' } }])

// With custom transaction ID (for exactly-once)
await queen.queue('tasks').push([
  {
    transactionId: 'unique-id-123',
    data: { value: 42 }
  }
])
```

### Consume Messages (Long-Running Workers)

```javascript
// Runs forever, processes messages as they arrive
await queen.queue('tasks')
  .concurrency(10)        // 10 parallel workers
  .batch(20)              // Fetch 20 at a time
  .consume(async (message) => {
    await processTask(message.data)
    // Auto-ack on success, auto-retry on error
  })

// Process with limit and stop
await queen.queue('tasks')
  .limit(100)
  .consume(async (message) => {
    await processTask(message.data)
  })
```

### Pop Messages (On-Demand Processing)

```javascript
// Grab messages manually
const messages = await queen.queue('tasks')
  .batch(10)
  .wait(true)  // Long polling
  .pop()

// Manual acknowledgment
for (const message of messages) {
  try {
    await processMessage(message.data)
    await queen.ack(message, true)  // Success
  } catch (error) {
    await queen.ack(message, false)  // Retry
  }
}
```

### Multi-Partition Pop (Drain Many Partitions Per Call)

```javascript
// One round-trip drains up to 200 messages spread across up to 50 partitions.
// batch(200) is the GLOBAL cap on total messages; partitions(50) is the
// hard cap on partitions claimed. All claimed partitions share one leaseId
// — a single renew() call extends every partition's lease atomically.
const messages = await queen.queue('events')
  .batch(200)
  .partitions(50)
  .wait(true)
  .pop()

// Each message carries its own partition info (per-message partitionId,
// partition name, leaseId, consumerGroup) — ACK and renew always work
// message-by-message regardless of how many partitions the batch spans.
for (const m of messages) {
  console.log(`from ${m.partition}:`, m.data)
}

// Same builder works on .consume() for long-running workers
await queen.queue('events')
  .batch(100)
  .partitions(8)
  .consume(async (msgs) => {
    for (const m of msgs) await process(m.data)
  })
```

**When to use:** queues with many partitions where each partition only has
a handful of new messages per polling interval (per-customer event streams,
per-tenant work queues, per-device telemetry). Reduces network round-trips
from O(P) to O(P / N) while preserving per-partition FIFO ordering.

**When not to use:** few partitions, or each one busy enough to fill
`batch(B)` on its own. Default is `partitions(1)` which preserves the
legacy single-partition behaviour.

`.partitions(N)` only applies to **wildcard** pops; specifying
`.partition('name')` ignores the cap.

### Transactions (Atomic Operations)

```javascript
// Pop from queue A
const messages = await queen.queue('input').pop()

// Atomically: ack input AND push output
await queen.transaction()
  .ack(messages[0])
  .queue('output')
  .push([{ data: processedResult }])
  .commit()

// If commit fails, nothing happens - message stays in input queue
```

### Client-Side Buffering (High Throughput)

```javascript
// Buffer messages locally, batch to server
for (let i = 0; i < 10000; i++) {
  await queen.queue('events')
    .buffer({ messageCount: 500, timeMillis: 1000 })
    .push([{ data: { id: i } }])
}

// Flush remaining buffered messages
await queen.flushAllBuffers()

// Result: 10x-100x faster than individual pushes
```

The buffer is **bounded and lossless**, and both properties are why `push()` must
be awaited:

| Option | Default | Meaning |
| --- | --- | --- |
| `messageCount` | `100` | Flush once this many messages are waiting |
| `timeMillis` | `1000` | Or this long after the first message arrives |
| `maxSize` | `4 x messageCount` | Backpressure bound: past this many buffered messages, `push()` WAITS for the flusher instead of growing the heap. There is no unbounded setting |
| `retryDelayMillis` | `250` | Delay before retrying a batch whose POST failed. Failed batches go back to the front of the buffer, in order, and are retried — never dropped |

A producer that outruns the flush pipeline is therefore paced down to the drain
rate, and a broker outage shows up as slow pushes with bounded memory rather
than as messages that quietly disappeared. `close()` flushes with a 30 second
deadline and logs how many messages were left unsent if it expires.

### Dead Letter Queue

```javascript
// Enable DLQ on queue
await queen.queue('risky')
  .config({ retryLimit: 3, dlqAfterMaxRetries: true })
  .create()

// Query failed messages
const dlq = await queen.queue('risky')
  .dlq()
  .limit(10)
  .get()

console.log(`Found ${dlq.total} failed messages`)
for (const msg of dlq.messages) {
  console.log('Error:', msg.errorMessage)
}
```

### Message Tracing

```javascript
await queen.queue('orders').consume(async (msg) => {
  const orderId = msg.data.orderId
  
  // Record trace with name for cross-service correlation
  await msg.trace({
    traceName: `order-${orderId}`,
    eventType: 'info',
    data: { text: 'Order processing started' }
  })
  
  await processOrder(msg.data)
  
  await msg.trace({
    traceName: `order-${orderId}`,
    eventType: 'processing',
    data: { 
      text: 'Order completed',
      total: msg.data.total
    }
  })
})

// View traces in webapp: Traces → Search "order-12345"
```

---

## Key/Value State and Timers

Both surfaces are **always there**. There is nothing to enable: kv and timers are part of the
broker the way push and pop are, on every cell that runs it. There is no capability to probe and
no 404 that means "this cell does not have the feature" — a 404 from these routes is a bug.

What an operator can still do is **pause** them, with the runtime kill switch in
`queen.system_state` (`kv_enabled`, `timers_schedule_enabled`, `timers_fire_enabled`) — the same
class of lever as maintenance mode, pulled live during an incident and expected to be pulled back.
A paused surface answers `503` with `Retry-After` and `error: 'kv_disabled'` / `'timers_disabled'`,
which this client retries like any other 5xx. Inside a transaction it is a `403` on the `kv` or
`timers` rider instead, so a bundle holding messages does not spin forever on a paused cell.

```javascript
try {
  await queen.kv.put('orders', 'order:9f1', { state: 'held' }, { ttl: '60s' })
} catch (e) {
  if (e.code === 'kv_disabled') { /* paused by an operator; it will come back */ }
  throw e
}
```

Branch on `e.code`, never on the message. And write that branch as "temporarily paused", not as
"this deployment lacks KV": handling the refusal is right, treating it as a configuration to check
before you use the surface is not.

### Key/Value

```javascript
// An expiry is MANDATORY on every write: exactly one of ttlSeconds (or the
// sugar ttl / until) and forever: true. A put never inherits the previous TTL.
await queen.kv.put('orders', 'order:9f1', { state: 'held' }, { ttl: '60s' })

const row = await queen.kv.get('orders', 'order:9f1')
if (row.found) console.log(row.value, row.version)   // found is separate: null is a legal value

// "Did I win?" in one call. This is the idempotency marker.
const { won, value } = await queen.kv.once('dedup', eventId, { ttl: '24h' })
if (!won) return                                     // somebody already did this

// Optimistic lock. expect: 0 means "must not exist"; expect: N is a pure
// update that creates nothing when it matches no row.
const res = await queen.kv.put('orders', 'order:9f1', { state: 'shipped' },
                               { ttl: '60s', expect: row.version })
if (!res.applied) console.log(res.reason)            // 'version' | 'exists' | 'absent' | 'limit' | 'type'

// Rate limiting without a CAS loop. With max, `applied` IS the admission
// decision: nothing saturates, nothing truncates, a refusal spends no budget.
const hit = await queen.kv.incr('quota', `${customer}:${hour}`, 1, { max: 1000, ttl: '1h' })
if (!hit.applied) throw new TooManyRequests()

for await (const r of queen.kv.listAll('saga', 'order:9f1:')) { /* follows nextAfter */ }
```

Seven operations: `get`, `getMany`, `getPrefix`, `put`, `putIfAbsent`, `delete`, `incr`, plus the
two conveniences this client owns, `once` and `listAll`.

**Every write returns an OBJECT, and objects are always truthy.** `if (await queen.kv.delete(ns,
key))` is always taken and is a bug. Read `.applied`, or `.won` on `once`, or `.found` on a read.
That holds for all five writes, and it is the one trap this language cannot defend against
structurally.

**A write that did not apply is not an error.** `applied: false` answers HTTP 200 with the current
value and version, so the loser needs no second round trip.

**Read-modify-write across two calls is safe only when the KV key derives from the partition key.**
Otherwise the lanes do not serialise it for you: use `incr`, or carry `expect`.

**`putIfAbsent` plus a TTL is not a distributed lock.** A lock that expires is not revoked: the old
holder keeps working, it simply no longer has the row. Carry your `version` as `expect` on every
later write so a lapsed holder fails with `reason: 'version'` instead of overwriting the new one.

### Timers

```javascript
// Fire no earlier than 30 minutes from now, into a real queue, through the log.
const res = await queen.timer('reminders')
  .key(`order:${orderId}`)
  .delay('30m')                    // or .delayMs(250)
  .payload({ orderId })
  .schedule()                      // status: 'scheduled' | 'rescheduled' | 'too_late'

await queen.timer('reminders').key(`order:${orderId}`).peek()
await queen.timer('reminders').list({ limit: 50 })
await queen.timer('reminders').key(`order:${orderId}`).cancel()
```

Scheduling the same `(queue, timerKey)` again is the same upsert, so a retry after a client crash
is safe by construction and `status` says which it was. A reschedule mints a new `txn` and resets
the retry budget.

Durations that can be sub-second are in **milliseconds** (`delayMs`), the ones that cannot are in
**seconds** (`ttlSeconds`). Only relative delays exist, because there is one clock and it is the
database's. A delay in the past is legal and fires on the first cycle.

`deliverAt` is **"not before"**, never "exactly at".

**`absent` means "no longer pending" and may mean ALREADY DELIVERED.** There is no tombstone: a
fired timer has no row left, so `absent` carries `ok: false` and the answer echoes the `txn` so the
authority, the log, can be consulted without a second API. A saga that cancels a compensation timer
must have the compensating consumer re-check the saga's KV state before compensating, because the
cancel may have arrived 5 ms after the fire.

Use `queen.timer(q).key(k).cancel()` rather than a cancel inside a bundle when the cancel must land
regardless: it takes the DELETE route, the one a quota is forbidden to block. A tenant that cannot
cancel keeps producing messages it cannot stop.

### Inside a transaction

The transaction is the **primary fence**; `expect` is only the secondary assertion. A state write
that shares the transaction with its ack is undone when an expired lease makes the ack fail, which
a compare-and-set cannot do.

```javascript
const result = await queen.transaction()
  .ack(message)
  .queue('emails').push([{ data: mail }])
  .once('sent', message.transactionId, { ttl: '24h' })   // the gate
  .timer('reminders').key(orderId).delay('24h').payload({ orderId }).schedule()
  .commit()

if (result.success === false) {
  // RETURNED, not thrown: a lost gate is the expected outcome of a legitimate
  // redelivery, so it stays out of your retry policy and your error metrics.
  result.reason        // 'kv_precondition'
  result.failedIndex, result.kvReason, result.version, result.value
  return
}
```

`once` is `putIfAbsent` with `required: true`, and `required` is what makes it a gate: without it a
lost precondition is only a verdict in the results, and the push and the ack still go through.

`kv.getPrefix` is not available inside a transaction and throws here rather than at the broker: its
cost is not bounded by the caller. `get` and `getMany` are allowed, because they are. Everything
other than the lost precondition still throws.

---

## Examples

### Complete Pipeline with Consumer Groups

```javascript
import { Queen } from 'queen-mq'

const queen = new Queen('http://localhost:6632')

// Stage 1: Ingest with buffering
async function ingestEvents() {
  for (let i = 0; i < 10000; i++) {
    await queen.queue('raw-events')
      .partition(`user-${i % 100}`)
      .buffer({ messageCount: 500, timeMillis: 1000 })
      .push([{ data: { userId: i % 100, event: 'page_view' } }])
  }
  await queen.flushAllBuffers()
}

// Stage 2: Process with transactions
async function processEvents() {
  await queen.queue('raw-events')
    .group('processors')
    .concurrency(5)
    .batch(10)
    .autoAck(false)
    .consume(async (messages) => {
      const results = messages.map(m => process(m.data))
      
      // Atomic: ack all inputs, push all outputs
      const txn = queen.transaction()
      for (const msg of messages) txn.ack(msg)
      txn.queue('processed-events').push(results.map(r => ({ data: r })))
      await txn.commit()
    })
}

// Stage 3: Separate analytics consumer (fan-out)
async function analytics() {
  await queen.queue('raw-events')
    .group('analytics')
    .subscriptionMode('new')  // Skip backlog
    .consume(async (message) => {
      await logMetrics(message.data)
    })
}

await ingestEvents()
await Promise.all([processEvents(), analytics()])
```

### Long-Running Tasks with Lease Renewal

```javascript
await queen.queue('video-processing')
  .renewLease(true, 60000)  // Renew every 60 seconds
  .consume(async (message) => {
    // Can take hours - lease keeps renewing automatically
    await processVideo(message.data)
  })
```

### Error Handling with Callbacks

```javascript
await queen.queue('tasks')
  .autoAck(false)
  .consume(async (message) => {
    return await riskyOperation(message.data)
  })
  .onSuccess(async (message, result) => {
    console.log('Success:', result)
    await queen.ack(message, true)
  })
  .onError(async (message, error) => {
    console.error('Failed:', error.message)
    
    // Custom retry logic
    if (error.message.includes('temporary')) {
      await queen.ack(message, false)  // Retry
    } else {
      await queen.ack(message, 'failed', { error: error.message })
    }
  })
```

---

## API Reference

### Queue Operations

```javascript
// Create
await queen.queue('my-queue').create()
await queen.queue('my-queue').config({ priority: 5 }).create()

// Delete
await queen.queue('my-queue').delete()

// Get info
const info = await queen.getQueueInfo('my-queue')
```

### Push

```javascript
await queen.queue('q').push([{ data: { value: 1 } }])
await queen.queue('q').partition('p1').push([{ data: { value: 1 } }])
await queen.queue('q').buffer({ messageCount: 100, timeMillis: 1000 }).push([...])
```

### Pop

```javascript
const msgs = await queen.queue('q').pop()
const msgs = await queen.queue('q').batch(10).pop()
const msgs = await queen.queue('q').batch(10).wait(true).pop()
const msgs = await queen.queue('q').batch(200).partitions(50).pop()  // multi-partition pop
```

### Consume

```javascript
await queen.queue('q').consume(async (msg) => { /* process */ })
await queen.queue('q').limit(10).consume(async (msg) => { /* process */ })
await queen.queue('q').concurrency(5).consume(async (msg) => { /* 5 workers */ })
await queen.queue('q').group('my-group').consume(async (msg) => { /* consumer group */ })
```

### Acknowledgment

```javascript
await queen.ack(message, true)   // Success
await queen.ack(message, false)  // Retry
await queen.ack(message, false, { error: 'reason' })
await queen.ack([msg1, msg2], true)  // Batch ack
```

### Transactions

```javascript
await queen.transaction()
  .ack(message)
  .queue('output')
  .push([{ data: { result: 'processed' } }])
  .commit()

// Riders. commit() RETURNS on a lost gate, and throws on everything else.
await queen.transaction()
  .ack(message)
  .kv.put('saga', sagaId, { step: 'charged' }, { ttl: '24h' })
  .once('dedup', message.transactionId, { ttl: '24h' })
  .timer('reminders').key(orderId).delay('30m').payload({ orderId }).schedule()
  .commit()
```

### Key/Value

```javascript
await queen.kv.get(ns, key)                      // {found, key, value, version, expiresAt, updatedAt}
await queen.kv.getMany(ns, [k1, k2])             // {rows, missing, truncated}
await queen.kv.getPrefix(ns, prefix, { limit: 100, after, keysOnly })
await queen.kv.put(ns, key, value, { ttl: '24h', expect, required })
await queen.kv.putIfAbsent(ns, key, value, { ttlSeconds: 86400 })
await queen.kv.delete(ns, key, { expect })
await queen.kv.incr(ns, key, 1, { max: 1000, min, ttl: '1h' })
await queen.kv.once(ns, key, { ttl: '24h' })     // {won, value, version, result}
for await (const row of queen.kv.listAll(ns, prefix)) { }
```

### Timers

```javascript
// Builder steps: .key(timerKey) required, .delayMs(250) or .delay('30m') required,
// .payload(anyJsonOrBuffer) required to schedule, .partition(name) optional
// (defaults to 'Default'), .txn(transactionId) optional (minted when absent).

await queen.timer(q).key(k).delay('30m').payload(p).schedule()   // {ok, status, txn, messageId, deliverAt}
await queen.timer(q).key(k).cancel()                             // {ok, status, txn}
await queen.timer(q).key(k).peek()                               // {found, ...}
await queen.timer(q).list({ limit: 50, after })                  // {rows, truncated, nextAfter}
```

### Lease Renewal

```javascript
await queen.renew(message)
await queen.renew([msg1, msg2, msg3])
await queen.queue('q').renewLease(true, 60000).consume(async (msg) => { /* auto-renew */ })
```

### Buffering

```javascript
await queen.flushAllBuffers()
await queen.queue('q').flushBuffer()
const stats = queen.getBufferStats()
```

### Dead Letter Queue

```javascript
const dlq = await queen.queue('q').dlq().limit(10).get()
const dlq = await queen.queue('q').dlq('consumer-group').limit(10).get()
const dlq = await queen.queue('q').dlq().from('2025-01-01').to('2025-01-31').get()
```

### Shutdown

```javascript
await queen.close()  // Flush buffers and close connections
```

---

## Configuration Defaults

### Client Defaults

```javascript
{
  timeoutMillis: 30000,
  retryAttempts: 3,
  retryDelayMillis: 1000,
  loadBalancingStrategy: 'affinity',   // 'affinity' | 'round-robin' | 'session'
  affinityHashRing: 128,
  enableFailover: true,
  healthRetryAfterMillis: 5000
}
```

### Queue Defaults

```javascript
{
  leaseTime: 300,         // 5 minutes
  retryLimit: 3,
  priority: 0,
  delayedProcessing: 0,
  windowBuffer: 0,
  maxSize: 0,            // Unlimited
  retentionSeconds: 0,   // Keep forever
  encryptionEnabled: false
}
```

### Consume Defaults

```javascript
{
  concurrency: 1,
  batch: 1,
  autoAck: true,
  wait: true,            // Long polling
  timeoutMillis: 30000,
  limit: null,           // Run forever
  renewLease: false
}
```

---

## Logging

Enable detailed logging for debugging:

```bash
export QUEEN_CLIENT_LOG=true
node your-app.js
```

Example output:
```
[2025-10-28T10:30:45.123Z] [INFO] [Queen.constructor] {"status":"initialized","urls":1}
[2025-10-28T10:30:45.234Z] [INFO] [QueueBuilder.push] {"queue":"tasks","partition":"Default","count":5}
```

---

## Best Practices

1. ✅ **Use `consume()` for workers** - Simpler API, handles retries automatically
2. ✅ **Use `pop()` for control** - When you need precise control over acking
3. ✅ **Buffer for speed** - Always use buffering when pushing many messages
4. ✅ **Partitions for order** - Use partitions when message order matters
5. ✅ **Consumer groups for scale** - Run multiple workers in the same group
6. ✅ **Transactions for consistency** - Use transactions for atomic operations
7. ✅ **Enable DLQ** - Always enable DLQ in production
8. ✅ **Renew long leases** - Use auto-renewal for long-running tasks
9. ✅ **Graceful shutdown** - Always call `queen.close()` before exiting
10. ✅ **Monitor DLQ** - Regularly check for failed messages

---

## TypeScript Support

Full TypeScript definitions included:

```typescript
import { Queen, Message, QueueConfig } from 'queen-mq'

const queen: Queen = new Queen('http://localhost:6632')

interface OrderData {
  orderId: number
  amount: number
}

const messages: Message<OrderData>[] = await queen.queue('orders').pop()
```

---

## Documentation

- **[Complete V2 Guide](client-v2/README.md)** — full tutorial with all features
- **[HTTP API Reference](https://github.com/queen-mq/queen/blob/master/server/API.md)** — raw HTTP endpoints
- **[Server Guide](https://github.com/queen-mq/queen/blob/master/server/README.md)** — server setup and configuration
- **[Architecture & internals](https://queenmq.com/architecture.html)** — published architecture overview
- **[libqueen design notes](https://github.com/queen-mq/queen/blob/master/cdocs/LIBQUEEN_IMPROVEMENTS.md)** — adaptive engine deep-dive

---

## Support

- **GitHub:** [queen-mq/queen](https://github.com/queen-mq/queen)
- **Issues:** [GitHub Issues](https://github.com/queen-mq/queen/issues)
- **LinkedIn:** [Smartness](https://www.linkedin.com/company/smartness-com/)

---

## License

Apache 2.0 - See [LICENSE.md](../LICENSE.md)

