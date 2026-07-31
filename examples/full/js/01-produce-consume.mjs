// docs:start(full-js-produce-consume)
//
// Queen MQ by example, 1 of 3: the smallest complete loop.
//
// Create a queue, push a handful of orders, consume them with a consumer group,
// acknowledge each one, and verify that every message arrived exactly once.
//
// Run it:
//   QUEEN_URL=http://localhost:6699 node 01-produce-consume.mjs
//
// The program checks its own outcome and exits non-zero if any check fails.

import { Queen } from 'queen-mq'

const QUEEN_URL = process.env.QUEEN_URL || 'http://localhost:6699'

// The name is prefixed per language so the JS, Python and Go examples can run
// against the same broker without colliding, and suffixed per run so a second
// run cannot inherit anything from the first.
const RUN_ID = Date.now().toString(36)
const QUEUE = `ex-js-produce-consume-${RUN_ID}`

// A consumer group is a named cursor over the queue. Every group sees every
// message; acking moves that group's cursor forward and affects no other group.
const GROUP = 'ex-js-shipping'

const ORDERS = [
  { orderId: 'A-1', customer: 'acme', total: 120.5 },
  { orderId: 'A-2', customer: 'acme', total: 12.0 },
  { orderId: 'B-1', customer: 'globex', total: 88.75 },
  { orderId: 'B-2', customer: 'globex', total: 4.2 },
  { orderId: 'C-1', customer: 'initech', total: 310.0 },
]

let checks = 0

function assert(condition, description) {
  if (!condition) throw new Error(description)
  checks++
  console.log(`  ok: ${description}`)
}

// handleSignals: false stops the client from installing its own SIGINT/SIGTERM
// handlers, because this script owns its shutdown through close() below.
const queen = new Queen({ url: QUEEN_URL, handleSignals: false })

try {
  console.log(`broker ${QUEEN_URL}`)

  // create() sends the whole configuration and is a full replace: keys you leave
  // out go back to the broker's defaults rather than keeping a previous value.
  await queen.queue(QUEUE).config({
    // How long a popped message stays invisible to other consumers before the
    // broker assumes the consumer died and hands it to someone else.
    leaseTime: 30,
    retryLimit: 3,
  }).create()
  console.log(`queue ${QUEUE} created`)

  console.log('\npushing')
  // push() accepts one item or an array. Each item's body is under `data`.
  // The client mints a UUIDv7 transactionId for any item that does not carry
  // one, and that id is the key the broker deduplicates on.
  const pushResults = await queen.queue(QUEUE).push(ORDERS.map(order => ({ data: order })))
  for (const result of pushResults) {
    console.log(`  ${result.transaction_id} -> ${result.status}`)
  }
  assert(pushResults.length === ORDERS.length, `broker answered for all ${ORDERS.length} items`)
  assert(pushResults.every(r => r.status === 'queued'), 'every item was accepted as queued')

  console.log('\nconsuming')
  const received = []
  await queen.queue(QUEUE)
    .group(GROUP)
    .each() // call the handler once per message instead of once per popped batch
    .batch(10) // up to 10 messages per poll
    .autoAck(false) // acknowledge by hand below, so the commit is visible in the code
    .limit(ORDERS.length) // stop the worker once it has handled this many messages
    .idleMillis(5000) // and stop anyway after 5s of silence, so a lost message fails the run instead of hanging it
    .consume(async (msg) => {
      console.log(`  ${msg.data.orderId} (${msg.data.customer}) from partition ${msg.partition}`)
      received.push(msg)

      // The acknowledgement is the commit. It moves this group's cursor past the
      // message and releases the lease; until it lands the message is only on
      // loan and would be redelivered when the lease expires.
      const ack = await queen.ack(msg, true, { group: GROUP })

      // A rejected ack still arrives as HTTP 200 with success: false on the item,
      // so the per-item flag is the only proof the broker took it.
      if (!ack.success) throw new Error(`ack rejected: ${ack.error}`)
    })

  console.log('\nchecking')
  assert(received.length === ORDERS.length, `consumed ${ORDERS.length} messages`)

  const arrivedIds = received.map(m => m.data.orderId)
  assert(new Set(arrivedIds).size === arrivedIds.length, 'no message was delivered twice')
  assert(
    ORDERS.every(order => arrivedIds.includes(order.orderId)),
    'every pushed order arrived at least once'
  )

  // The cursor for this group is now past every message, so a further poll on
  // the same group finds nothing. wait(false) makes it return immediately.
  const leftovers = await queen.queue(QUEUE).group(GROUP).batch(10).wait(false).pop()
  assert(leftovers.length === 0, `nothing is left for group ${GROUP}`)

  // Clean up. Only on success, so a failed run leaves the queue on the broker.
  await queen.queue(QUEUE).delete()

  console.log(`\nPASS: ${checks} checks`)
} catch (err) {
  console.error(`\nFAIL: ${err.message}`)
  process.exitCode = 1
} finally {
  // close() flushes buffers and destroys the HTTP dispatcher. Without it the
  // keep-alive sockets hold the Node event loop open and the process hangs.
  await queen.close()
}
// docs:end
