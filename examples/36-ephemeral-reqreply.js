/**
 * Example 36: Request/reply over ephemeral inbox queues
 *
 * This example demonstrates:
 * - implicit queues: an inbox that is created by the first message that names it
 *   and garbage-collected when it goes quiet — no configure, no PG row, no cleanup
 * - a long-poll pop parked on a RAM gate as the reply leg
 * - the correlation pattern: the requester names its own inbox, the responder
 *   pushes the answer straight into it
 *
 * WHY THIS IS THE FLAGSHIP CASE. Request/reply is where the durable engine is
 * least at home: every inbox is a partition, every reply is a serial claim
 * transaction, and the shape (thousands of short-lived queues, tiny payloads,
 * immediate consumption, worthless history) is exactly what costs the most on a
 * log built for durability. On the ephemeral engine the reply leg is a memory
 * write, a wake, and a socket — there is no database in the path at all, and no
 * polling interval anywhere: the pop is parked on a gate the push rings.
 *
 * The trade is stated once and never hidden: a reply in flight when the broker
 * restarts is GONE. Request/reply already has a timeout for exactly that class
 * of failure, which is what makes this workload the right first tenant of a
 * storage class that survives nothing.
 *
 * Run: node 36-ephemeral-reqreply.js
 */

import { randomUUID } from 'node:crypto'
import { Queen } from '../clients/client-js/client-v2/index.js'

const queen = new Queen('http://localhost:6632')

// One well-known queue for the requests. The inboxes are per-request and never
// declared: naming one in a push or a pop is what creates it.
const REQUESTS = 'rpc-requests'
const REQUEST_COUNT = 5

// ---------------------------------------------------------------------------
// The responder: pop requests, do the work, push the answer to the inbox the
// requester named. It never learns about an inbox before a message mentions one.
// ---------------------------------------------------------------------------

async function responder(stop) {
  let served = 0

  while (!stop.done) {
    const { messages } = await queen.ephemeral.pop(REQUESTS, {
      group: 'workers',      // competing consumers: run this process N times and they share the load
      batch: 10,
      wait: true,
      timeout: 1000          // short, so the loop notices `stop` promptly
    })

    if (messages.length === 0) continue

    for (const message of messages) {
      const { replyTo, n } = message.payload

      // The reply leg. `replyTo` is a queue that did not exist a moment ago.
      await queen.ephemeral.push(replyTo, [{ n, squared: n * n, servedBy: process.pid }])
      served++
    }

    // Ack with the same group that popped. Unacked messages redeliver when the
    // lease expires, with `attempts` incremented — at-least-once for as long as
    // the owning broker lives.
    await queen.ephemeral.ack(REQUESTS, messages, { group: 'workers' })
  }

  return served
}

// ---------------------------------------------------------------------------
// The requester: mint an inbox name, push the request carrying it, park on the
// inbox until the answer arrives.
// ---------------------------------------------------------------------------

async function request(n, timeoutMillis = 5000) {
  const inbox = `rpc-inbox-${randomUUID()}`

  await queen.ephemeral.push(REQUESTS, [{ n, replyTo: inbox }])

  const { messages } = await queen.ephemeral.pop(inbox, {
    batch: 1,
    wait: true,
    timeout: timeoutMillis,
    autoAck: true            // at-most-once, and exactly right here: the reply is
                             // consumed once, by the one caller waiting for it
  })

  if (messages.length === 0) {
    // The inbox is empty and idle now, so the broker will collect it on its own.
    throw new Error(`request ${n}: no reply within ${timeoutMillis}ms`)
  }

  return messages[0].payload
}

// ---------------------------------------------------------------------------

const stop = { done: false }
const running = responder(stop)

console.log(`Sending ${REQUEST_COUNT} requests, each with its own throw-away inbox...\n`)

const latencies = []
for (let n = 1; n <= REQUEST_COUNT; n++) {
  const started = Date.now()
  const reply = await request(n)
  const elapsed = Date.now() - started
  latencies.push(elapsed)
  console.log(`  ${n}² = ${reply.squared}  (${elapsed}ms round trip, served by pid ${reply.servedBy})`)
}

stop.done = true
const served = await running

const sorted = [...latencies].sort((a, b) => a - b)
console.log(`\nServed ${served} request(s).`)
console.log(`Round trip: min ${sorted[0]}ms, median ${sorted[Math.floor(sorted.length / 2)]}ms, max ${sorted[sorted.length - 1]}ms`)
console.log(`${REQUEST_COUNT} inboxes were created and abandoned; none of them was ever declared,`)
console.log('and none of them left anything behind in PostgreSQL.')

// The request queue is implicit too — left empty, it is collected on its own.
// Deleting it is a courtesy to whoever reads the dashboard next.
await queen.ephemeral.delete(REQUESTS)

await queen.close()
