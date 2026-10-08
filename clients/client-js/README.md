# Queen MQ - JavaScript Client

<div align="center">

**Modern, high-performance message queue client for Node.js**

[![npm](https://img.shields.io/npm/v/queen-mq.svg)](https://www.npmjs.com/package/queen-mq)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.md)
[![Node](https://img.shields.io/badge/node-%3E%3D24.0.0-brightgreen.svg)](https://nodejs.org/)

[Quick Start](#quick-start) • [Complete Guide](client-v2/README.md) • [Examples](#examples) • [API Reference](#api-reference)

</div>

---

## What is Queen MQ?

Queen MQ is a partitioned message queue broker that keeps its state in its own replicated log, with a powerful feature set:

- **FIFO Partitions** - Unlimited ordered partitions within queues
- **Consumer Groups** - Kafka-style consumer groups for scalability
- **Flexible Semantics** - Exactly-once, at-least-once, and at-most-once delivery
- **Transactions** - Atomic operations across push and ack
- **High Performance** — 1M msg/s in and out of one queue of 10M partitions on three nodes ([benchmarks](https://queenmq.com/benchmarks/))
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

**Requirements:** Node.js 24+

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

Control whether consumer groups process historical messages. A group's mode is fixed the first
time the group pops a queue; unset, it is the broker's `DEFAULT_SUBSCRIPTION_MODE`, which is `new`.

```javascript
// Default ('new'): start where the group first pops, skip what is already there
await queen.queue('events')
  .group('realtime-monitor')
  .consume(async (message) => { /* new only */ })

// Process ALL messages, including the backlog
await queen.queue('events')
  .group('batch-analytics')
  .subscriptionMode('all')
  .consume(async (message) => { /* all messages */ })

// Start from specific timestamp
await queen.queue('events')
  .group('replay')
  .subscriptionFrom('2025-10-28T10:00:00.000Z')
  .consume(async (message) => { /* from timestamp */ })
```

### Conflation (Last-Value Delivery)

For command-style queues where one partition is one logical task key — "recompute
customer 42", "this entity is dirty" — only the newest pending message matters.
`.conflation(true)` makes a pop of a partition deliver exactly one message, the
newest visible one, and commit past everything it skipped:

```javascript
// A backlog of 4 000 recompute requests across 12 entities becomes
// 12 handler calls, each with the freshest input.
await queen.queue('recompute')
  .group('workers')
  .conflation(true)
  .partitions(64)
  .consume(async (message) => {
    await recompute(message.data.entityId)
  })
```

The guarantee: **after the last push to a partition, at least one handler run
starts after that push committed.** Nothing is deleted — conflation is a delivery
policy, not compaction; retention still governs what is stored, and a
non-conflating group on the same queue still sees every message.

Notes:

- It is a property of the **consumer group**, fixed when that group first
  registers on the queue. A later consumer declaring the opposite does not flip
  it — the stored value wins, that consumer keeps working, and the SDK warns
  once per (queue, group).
- Skipping is per **partition**, so partitioning is the key: one partition = one
  logical key is the contract this workload has to hold up.
- A conflating pop returns at most one message per partition, so **partitions**
  size the round-trip, not `batch`. Left unset, the broker uses `.batch(N)` as
  the partition cap; either way it is clamped to 64, so a conflating pop returns
  at most 64 messages per round-trip whatever `batch` says.
- Refused with 400 by the broker without a `.group(...)`, and together with
  `.commitOnDelivery()` (a commit at delivery would turn the guarantee above
  into at-most-once). `pop()` raises that 400 instead of returning `[]`.
- Requires broker **>= 1.1.0**. An older broker ignores the flag and would
  quietly deliver the whole backlog, so the SDK raises
  `conflation was requested but this broker did not apply it` on the first
  response that does not echo it — before any message is processed.
- `admin.getQueueDepth(queue, group)` reports `effectivePending` (handler calls
  still owed) next to `pending` (log positions still to retire). For a
  conflating group `pending: 4000000, effectivePending: 12` is healthy.

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

**When the handler throws**, the consumer nacks what it was given (the message, or the whole batch)
and keeps consuming. The broker redelivers it, and moves it to the dead letter queue once the queue's
`retryLimit` is spent. With `.each()`, the later messages of the failed message's partition are
skipped after a nack (the broker redelivers them too); the other partitions of the same pop are
still handled.

This is the same with `.autoAck(false)`. That setting hands the *success* path to your handler (it
acks), not the failure path: a handler that threw never got to settle its messages. A message your
handler acked before throwing is not affected: it is already settled, so the broker refuses
its nack.

To handle failures yourself, add `.onError(async (message, error) => { ... })`. The error then never
reaches the consumer, nothing is nacked for you, and the message is yours to ack, nack, or leave
until its lease expires. To stop consuming on an error, abort the `signal` you passed to
`consume(handler, { signal })` from inside `onError`.

```javascript
// autoAck(false): the handler acks. A throw before the ack is nacked and retried.
await queen.queue('tasks')
  .group('workers')
  .autoAck(false)
  .each()                  // one message per call; without it the handler gets the popped array
  .consume(async (message) => {
    await processTask(message.data)
    await queen.ack(message, true)
  })

// Your own failure policy: nack, then stop consuming.
const stop = new AbortController()
await queen.queue('tasks')
  .group('workers')
  .autoAck(false)
  .each()
  .consume(async (message) => {
    await processTask(message.data)
    await queen.ack(message, true)
  }, { signal: stop.signal })
  .onError(async (message, error) => {
    await queen.ack(message, false, { error: error.message })
    stop.abort()
  })
```

Earlier versions stopped the consumer on a throw under `.autoAck(false)`: `consume()` rejected after
the first failure, and the message stayed leased until its lease expired.

**Stopping a consumer** (a graceful shutdown) is aborting its `signal`. The handler call in progress
finishes, with its ack, and `consume()` resolves. Nothing is left leased:

- The long poll in flight is closed at once. The broker hands nothing to a poll whose caller is gone,
  so a message that arrives during the shutdown goes to another consumer straight away.
- A pop answer that has already started arriving is read to the end: the broker leased its messages
  when it sent it, and the body says which ones they are.
- With `.each()`, messages already popped but not yet handed to the handler go back with a `retry`
  ack. The broker releases their lease and redelivers them first, in order, without charging a
  retry. The same happens to messages popped beyond `.limit()`.
- A wait between attempts (a 429 backoff, a retry after a 5xx or a network error) ends at once.

```javascript
const stop = new AbortController()
// consume() starts when awaited: Promise.resolve() starts it now and keeps the
// promise that settles once the consumer has stopped.
const consuming = Promise.resolve(queen.queue('tasks').group('workers').each()
  .consume(async (message) => { await processTask(message.data) }, { signal: stop.signal }))

process.once('SIGTERM', async () => {
  stop.abort()
  await consuming      // the message in the handler is finished and acked
  await queen.close()
})
```

Earlier versions checked the signal only between polls. A poll open at the abort stayed open for up
to its timeout, and `.each()` dropped what it brought back without settling it, so its partition
waited out the whole lease.

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
`batch(B)` on its own. Leaving `.partitions()` unset hands the sweep width to
the broker (see Pop Autopilot below); `.partitions(1)` pins the legacy
single-partition behaviour.

`.partitions(N)` only applies to **wildcard** pops; specifying
`.partition('name')` ignores the cap.

### Pop Autopilot (Let the Broker Size the Pop)

Since 1.2, `batch` and `partitions` that you do **not** set are chosen by the
broker, per pop, from state the client cannot see: how many partitions of the
group are ready, how old their oldest ready message is, how fast messages are
arriving. The knobs you *do* set are never touched.

```javascript
// Both knobs are the broker's: it picks the sweep width and the budget.
await queen.queue('events').group('workers')
  .consume(async (msgs) => { /* ... */ })

// One knob pinned, one delegated: this consumer stays on one partition
// forever, and the broker sizes the batch for it.
await queen.queue('events').group('workers').partitions(1)
  .consume(async (msgs) => { /* ... */ })
```

The request carries `autopilot=true` and simply omits the delegated knobs.
Setting both leaves nothing to decide, so nothing changes on the wire at all.

**Two ways to switch it off**, both restoring the previous client-side
defaults (batch 1, partitions 1) byte for byte:

```javascript
await queen.queue('events').autopilot(false).consume(handler)
```

```bash
QUEEN_SDK_POP_AUTOPILOT=off   # whole process, read once at client creation
```

**What the broker chose** rides back on the response and is there for the
reading, along with an optional pacing hint the consume loop honours in place
of its own delay between empty polls:

```javascript
const { messages, autopilot } = await queen.queue('events').group('workers').popResult()
if (autopilot) {
  console.log(`${autopilot.partitions} partitions, batch ${autopilot.batch}, ` +
              `poll again in ${autopilot.waitMillis}ms`)
}
```

**Requires broker >= 1.2.** An older broker ignores the parameter, so the
omitted knobs take *its* defaults (batch 200, partitions 1) instead of the old
client-side ones. That is a sizing difference and nothing else — no message is
lost, reordered or duplicated — so unlike conflation it degrades silently and
on purpose. Pin the values explicitly, or turn autopilot off, if you need the
old numbers against an old broker.

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

What an operator can still do is **pause** them, with the broker's runtime kill switches
(`kv_enabled`, `timers_schedule_enabled`, `timers_fire_enabled`) — a lever pulled live during an
incident and expected to be pulled back.
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
`queen.lock()` below is that, done for you.

**`check` asserts a version and writes nothing.** `kv.check(ns, key, { expect })` holds while the
key is at that version (`expect: 0`: while it is absent). With `required: true` in a transaction
it gates the commit on a key the transaction does not write.

### Locks and semaphores

A lock is a lease: one holder at a time, for a lifetime the holder renews, with a token that
fences a holder that outlived it. `queen.semaphore(name, n, opts)` is the same with `n` permits.

```javascript
const lock = queen.lock('daily-report', { ttl: '30s' })
if (!(await lock.acquire({ wait: '5s' }))) return   // somebody else holds it

try {
  await queen.transaction()
    .guard(lock)                                    // commits only while the lock is ours
    .queue('reports').push([{ data: report }])
    .commit()
} finally {
  await lock.release()
}

// Or: acquire, run, release.
const { acquired, value } = await queen.lock('sync:crm', { ttl: '1m' }).run(() => sync())
```

**It expires, and nobody tells the holder.** A paused or partitioned process carries on past its
lifetime while somebody else acquires. `.guard(lock)` is what keeps its work out: the transaction
rolls back with `reason: 'kv_precondition'` (returned from `commit()`, not thrown) when the lock is
no longer this handle's. Outside Queen, fence with `lock.token`, which only rises on a lock.

**The handle renews in the background**, every third of the lifetime (`autoRenew: false` to do it
yourself with `lock.renew()`), and says when the lock is gone: `lock.signal` aborts and
`lock.onLost(fn)` runs. Nothing stops your code: pass `lock.signal` to what the work awaits.

**The token changes at every renew.** Read `lock.token` and `lock.guard()` when you use them; do
not keep a copy across an `await`.

`queen.locks` is the wire, with no state kept: `acquire`, `renew`, `release`, `get(name)` (who
holds it, since when, until when) and `batch` for several locks in one call. `queen.close()`
releases the locks its handles still hold.

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
broker's. A delay in the past is legal and fires on the first cycle.

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
const msgs = await queen.queue('q').pop()                            // broker-sized (see Pop Autopilot)
const msgs = await queen.queue('q').batch(10).pop()
const msgs = await queen.queue('q').batch(10).wait(true).pop()
const msgs = await queen.queue('q').batch(200).partitions(50).pop()  // multi-partition pop
const { messages, autopilot } = await queen.queue('q').popResult()   // + what the broker chose
const msgs = await queen.queue('q').group('g').commitOnDelivery().pop()  // committed at delivery, nothing to ack
```

A pop long-polls, waiting up to `timeoutMillis` (30 s) for a message, unless you call
`.wait(false)`. Its messages come back leased, and the ack is yours.

`.commitOnDelivery()` changes that for `pop()` and `popResult()`. The broker moves the group's
cursor past the messages as it hands them out: there is no lease (`leaseId` is empty) and nothing
to ack. This is at-most-once delivery: a crash after the pop loses the messages. The broker
refuses it together with `.conflation()` (400). `consume()` always leases its messages, so it
throws before any request when the builder has `.commitOnDelivery()`.

`.autoAck()` is `consume()`'s ack after your handler and has no effect on a pop.

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

An ack is judged against the lease of the consumer group the message was popped under. `queen.ack()`
and `transaction().ack()` take that group from the message (`message.consumerGroup`, which every pop
returns) unless you name one: `{ group }` on `queen.ack()`, `{ consumerGroup }` or `{ group }` on a
transaction. A batch ack carries a single group, so `queen.ack()` throws on a batch that mixes groups.

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
await queen.renew(message)              // {leaseId, success, newExpiresAt, renewed}
await queen.renew([msg1, msg2, msg3])   // one result per distinct lease
await queen.queue('q').renewLease(true, 60000).consume(async (msg) => { /* auto-renew */ })
```

`success: false` (with an `error`) means nothing was renewed: the lease expired, an ack or nack
already released it, or it never existed. The messages it covered may already be redelivered.

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
  batch: 1,              // autopilot OFF only -- unset means the broker sizes it
  partitions: 1,         // autopilot OFF only -- unset means the broker sizes it
  autoAck: true,
  wait: true,            // Long polling
  timeoutMillis: 30000,
  limit: null,           // Run forever
  renewLease: false
}
```

`batch` and `partitions` are the **autopilot-off** defaults: with autopilot on
(the default) a knob you never set is not defaulted at all, it is delegated to
the broker. These values are what comes back with `.autopilot(false)` or
`QUEEN_SDK_POP_AUTOPILOT=off`.

### Pop Defaults

```javascript
{
  batch: 1,              // autopilot OFF only, as for consume
  wait: true,            // long-polls; .wait(false) returns at once
  timeoutMillis: 30000,  // the long-poll limit
  autoAck: false,        // consume()'s ack after the handler; no effect on pop()
  commitOnDelivery: false // leased; .commitOnDelivery() commits at delivery (at-most-once)
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

### Optional consumer supervision

```js
await queen.queue('orders').group('billing')
  .supervision({ group: 'billing-production' })
  .concurrency(4).each().consume(async message => { /* process message */ })
```

Supervision defaults to off; `.supervision(false)` disables it. The group names
an application/deployment in the dashboard, independently of the consumer group.
Each consume invocation publishes its own instance into the broker's
`queen-supervisor` KV namespace every 10 seconds (30-second heartbeat timeout,
60-second TTL), plus a final stopped observation. The credential needs KV write
access. Publication is serialized and best effort with a two-second deadline.

The Supervisors page supporting `queen.consumer.status/v1` shows live async
consumer loops, busy handlers, successful/failed handler calls and progress times.
A batch is one handler call; completion does not imply ACK success. No payloads or
error text are published. Event-loop starvation can stop heartbeats. Reporting
does not restart processes or tasks, change ACK/lease policies, or enable remote
control. With reporting off there is no additional timer or network traffic.
