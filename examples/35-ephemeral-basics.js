/**
 * Example 35: Ephemeral Queues — the basics
 *
 * This example demonstrates:
 * - configure(): declaring a queue and its bounds (the config is durable, the contents are not)
 * - push() / pop() / ack(): the flat verbs of queen.ephemeral
 * - consumer groups: same group competes, its own group fans out — exactly as on durable queues
 * - depth() and queues(): gauges read from broker RAM, free to poll
 * - reset(): drop everything, which is only legal because this class promises nothing about contents
 * - buffered push: the durable buffer machinery, pointed at the ephemeral route
 *
 * WHAT MAKES THESE QUEUES DIFFERENT: their contents live in broker RAM and
 * survive NOTHING — not a restart, not a crash, not a deploy, not the ownership
 * move a membership change causes. Treat a failover like a Redis restart. What
 * DOES survive is the configuration of a declared queue, which comes back after
 * a restart as configured and empty.
 *
 * Requires a broker >= 1.1. An older one answers 404 on the whole family and
 * the SDK maps that to one error with code `ephemeral_unsupported`.
 *
 * Run: node 35-ephemeral-basics.js
 */

import { Queen } from '../clients/client-js/client-v2/index.js'

const queen = new Queen('http://localhost:6632')

const QUEUE = 'presence-demo'
const ROOM = 'room-7'

// ---------------------------------------------------------------------------
// 1. Declare the queue.
//
// Optional: a push or a pop that names an unknown queue creates it implicitly
// with the tenant defaults, which is what makes thousands of short-lived
// req/reply inboxes affordable. Declare when you want your own bounds, or when
// you want the queue visible before its first message.
// ---------------------------------------------------------------------------

console.log('--- 1. configure ---')
await queen.ephemeral.configure(QUEUE, {
  maxLength: 1000,          // ring capacity, in messages
  maxBytes: 1024 * 1024,    // ...and in bytes; whichever binds first
  policy: 'dropOldest',     // at the bound: drop the OLDEST (feed semantics), vs 'reject' (429)
  ttlSeconds: 30,           // drop messages nobody consumed within 30s
  leaseSeconds: 15,         // an unacked message redelivers after this
  retryLimit: 3             // ...at most this many times, then it is dropped and counted
})
console.log(`Declared ${QUEUE}: bounded at 1000 messages / 1 MiB, dropping the oldest when full\n`)

// ---------------------------------------------------------------------------
// 2. Push. The envelope carries the queue and partition; each message is just
//    a payload. FIFO is per (queue, partition).
// ---------------------------------------------------------------------------

console.log('--- 2. push ---')
const pushResult = await queen.ephemeral.push(QUEUE, [
  { user: 'alice', typing: true },
  { user: 'bob', typing: true },
  { user: 'carol', typing: false }
], { partition: ROOM })
console.log(`Pushed ${pushResult.pushed} presence events to ${QUEUE}/${ROOM}\n`)

// ---------------------------------------------------------------------------
// 3. Gauges. These are read straight out of the broker's memory — no database
//    is touched, so a dashboard can poll them every second for free.
// ---------------------------------------------------------------------------

console.log('--- 3. depth / queues ---')
console.log('depth :', JSON.stringify(await queen.ephemeral.depth(QUEUE)))
console.log('queues:', JSON.stringify(await queen.ephemeral.queues()), '\n')

// ---------------------------------------------------------------------------
// 4. Pop with a group, then ack.
//
// The group IS the consumption semantics, exactly as on a durable queue:
// everyone sharing a group competes for the messages; a consumer with its own
// group gets its own cursor over the same ring and sees everything.
//
// `wait: true` is a real long poll parked on a RAM gate: no database behind it
// and no polling interval anywhere, which is why an empty ephemeral queue costs
// nothing to wait on.
// ---------------------------------------------------------------------------

console.log('--- 4. pop (group "widget") + ack ---')
const first = await queen.ephemeral.pop(QUEUE, {
  partition: ROOM,
  group: 'widget',
  batch: 10,
  wait: true,
  timeout: 2000
})
console.log(`Popped ${first.messages.length}:`, first.messages.map(m => m.payload.user).join(', '))

// Ack with the same group the pop used — cursors are per group. `attempts` on a
// redelivered message tells you how many times it has been tried; consumers
// still need to be idempotent, exactly as on durable queues.
const ackResult = await queen.ephemeral.ack(QUEUE, first.messages, { group: 'widget' })
console.log('Ack outcomes:', ackResult.results.map(r => r.outcome).join(', '), '\n')

// ---------------------------------------------------------------------------
// 5. Fan-out: a second group over the SAME ring sees everything the first one
//    already consumed. No configuration, no subscription — just another group.
// ---------------------------------------------------------------------------

console.log('--- 5. fan-out (group "audit") ---')
const audit = await queen.ephemeral.pop(QUEUE, { partition: ROOM, group: 'audit', batch: 10 })
console.log(`The audit group independently sees ${audit.messages.length} message(s)`)

// autoAck commits at delivery: at-most-once, no lease bookkeeping at all. The
// audit group has nothing to ack because its pop already advanced the cursor.
await queen.ephemeral.pop(QUEUE, { partition: ROOM, group: 'metrics', batch: 10, autoAck: true })
console.log('The metrics group popped with autoAck: nothing to ack, nothing to redeliver\n')

// ---------------------------------------------------------------------------
// 6. Buffered push: the same client-side batching the durable push has, sending
//    to the ephemeral route. Blocking backpressure at `maxSize`, a failed batch
//    retried from the front of the buffer, and queen.close() draining what is
//    left. Buffering is a latency/efficiency trade, not a durability change —
//    an unflushed message dies with the process, which is already inside this
//    class's contract.
// ---------------------------------------------------------------------------

console.log('--- 6. buffered push ---')
for (let i = 0; i < 250; i++) {
  await queen.ephemeral.push(QUEUE, [{ user: `user-${i}`, typing: i % 2 === 0 }], {
    partition: ROOM,
    buffered: { messageCount: 100, timeMillis: 200, maxSize: 400 }
  })
}
await queen.ephemeral.flush(QUEUE, { partition: ROOM })
console.log('250 events buffered and flushed in batches of 100')
console.log('depth :', JSON.stringify(await queen.ephemeral.depth(QUEUE)), '\n')

// ---------------------------------------------------------------------------
// 7. reset(): drop every message, void every lease, rewind every cursor.
//
// A verb that would be indefensible on a durable queue. Here it destroys
// nothing that was ever promised to survive. The declared configuration stays.
// ---------------------------------------------------------------------------

console.log('--- 7. reset ---')
const reset = await queen.ephemeral.reset(QUEUE)
console.log(`Reset dropped ${reset.dropped} message(s); the queue is still declared`)
console.log('depth :', JSON.stringify(await queen.ephemeral.depth(QUEUE)), '\n')

// Cleanup: delete removes the contents AND the declared configuration.
await queen.ephemeral.delete(QUEUE)
console.log(`Deleted ${QUEUE}`)

await queen.close()
