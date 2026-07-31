// docs:start(full-js-ordering-dedup)
//
// Queen MQ by example, 2 of 3: ordering and deduplication.
//
// Part one: one partition per customer. Messages for different customers are
// pushed interleaved, but each customer's messages come back in the order they
// were pushed, because a partition is the unit of ordering.
//
// Part two: pushing the same transactionId twice. The broker answers `duplicate`
// for the second push and stores nothing new.
//
// Run it:
//   QUEEN_URL=http://localhost:6699 node 02-ordering-and-dedup.mjs
//
// The program checks its own outcome and exits non-zero if any check fails.

import { Queen } from 'queen-mq'

const QUEEN_URL = process.env.QUEEN_URL || 'http://localhost:6699'

// Prefixed per language so the three languages never collide, suffixed per run
// so a second run starts from a genuinely empty queue.
const RUN_ID = Date.now().toString(36)
const QUEUE = `ex-js-ordering-dedup-${RUN_ID}`
const GROUP = 'ex-js-auditor'

// Three customers, three partitions. Ordering in Queen is per partition, never
// global, so the partition key is the identity whose history must stay in order.
const CUSTOMERS = ['acme', 'globex', 'initech']
const EVENTS_PER_CUSTOMER = 4

// The dedup demonstration gets its own partition so it can be counted on its own.
const DEDUP_PARTITION = 'dedup-demo'

let checks = 0

function assert(condition, description) {
  if (!condition) throw new Error(description)
  checks++
  console.log(`  ok: ${description}`)
}

const queen = new Queen({ url: QUEEN_URL, handleSignals: false })

try {
  console.log(`broker ${QUEEN_URL}`)

  await queen.queue(QUEUE).config({
    leaseTime: 30,
    retryLimit: 3,
    // How long a transactionId is remembered for deduplication, in seconds.
    dedupWindowSeconds: 600,
  }).create()
  console.log(`queue ${QUEUE} created`)

  // ---------------------------------------------------------------- ordering

  // Build a global sequence that interleaves the customers: acme#0, globex#0,
  // initech#0, acme#1, ... Nothing in Queen preserves this global order, and the
  // point of the example is that nothing has to.
  const interleaved = []
  for (let seq = 0; seq < EVENTS_PER_CUSTOMER; seq++) {
    for (const customer of CUSTOMERS) {
      interleaved.push({ customer, seq, event: `${customer}-event-${seq}` })
    }
  }

  console.log('\npushing interleaved')
  const pushedPerCustomer = new Map(CUSTOMERS.map(c => [c, []]))
  for (const item of interleaved) {
    // partition(customer) puts this message in that customer's lane. Messages
    // in one lane are delivered in push order; two lanes have no order between
    // them and can be consumed in parallel.
    const [result] = await queen.queue(QUEUE).partition(item.customer).push([{ data: item }])
    if (result.status !== 'queued') throw new Error(`push of ${item.event} answered ${result.status}`)
    pushedPerCustomer.get(item.customer).push(item.event)
    console.log(`  ${item.event} -> partition ${item.customer}`)
  }

  console.log('\nconsuming')
  const arrivedPerCustomer = new Map(CUSTOMERS.map(c => [c, []]))
  const globalArrival = []
  await queen.queue(QUEUE)
    .group(GROUP)
    .each()
    .batch(interleaved.length)
    // Claim up to three partitions per poll. All of them share one lease, and
    // messages still arrive in order within each partition.
    .partitions(CUSTOMERS.length)
    .autoAck(false)
    .limit(interleaved.length)
    .idleMillis(5000)
    .consume(async (msg) => {
      arrivedPerCustomer.get(msg.data.customer).push(msg.data.event)
      globalArrival.push(msg.data.event)
      const ack = await queen.ack(msg, true, { group: GROUP })
      if (!ack.success) throw new Error(`ack rejected: ${ack.error}`)
    })

  console.log(`  push order:    ${interleaved.map(i => i.event).join(' ')}`)
  console.log(`  arrival order: ${globalArrival.join(' ')}`)

  console.log('\nchecking ordering')
  assert(globalArrival.length === interleaved.length, `consumed all ${interleaved.length} messages`)
  for (const customer of CUSTOMERS) {
    const pushed = pushedPerCustomer.get(customer).join(' ')
    const arrived = arrivedPerCustomer.get(customer).join(' ')
    assert(pushed === arrived, `partition ${customer} kept its order: ${arrived}`)
  }
  // The global sequence is allowed to differ, and here it does: the consumer
  // drains a partition at a time. Ordering was promised per partition only.
  assert(
    globalArrival.join(' ') !== interleaved.map(i => i.event).join(' '),
    'the global order differs from the push order, as documented'
  )

  // ------------------------------------------------------------------- dedup

  console.log('\npushing the same transactionId twice')

  // A transactionId is the idempotency key. Supplying your own means a retry
  // after a lost response cannot create a second message. It only has to be
  // unique within this queue's dedup window, so a business id is the usual pick.
  const invoiceId = 'ex-js-invoice-7001'
  const payload = { data: { invoice: invoiceId, amount: 42.5 }, transactionId: invoiceId }

  const [first] = await queen.queue(QUEUE).partition(DEDUP_PARTITION).push([payload])
  console.log(`  first push:  status=${first.status} message_id=${first.message_id}`)

  // The identical push again, as an at-least-once producer would send it.
  const [second] = await queen.queue(QUEUE).partition(DEDUP_PARTITION).push([payload])
  console.log(`  second push: status=${second.status} message_id=${second.message_id}`)

  console.log('\nchecking dedup')
  assert(first.status === 'queued', 'the first push was queued')
  assert(second.status === 'duplicate', 'the second push was answered as duplicate')
  // The broker echoes the id of the message it already holds rather than
  // minting a new one, so the producer can still resolve what it pushed.
  assert(second.message_id === first.message_id, 'the duplicate echoes the original message id')

  // Nothing new was stored: the partition holds exactly one message.
  const stored = await queen.queue(QUEUE)
    .partition(DEDUP_PARTITION)
    .group(GROUP)
    .batch(10)
    .wait(true)
    .timeoutMillis(3000)
    .pop()
  assert(stored.length === 1, 'the partition holds exactly one message, not two')
  assert(stored[0].transactionId === invoiceId, 'that message is the one that was pushed first')
  const ack = await queen.ack(stored, true, { group: GROUP })
  assert(ack.success, 'the surviving message acknowledges cleanly')

  // Clean up. Only on success, so a failed run leaves the queue on the broker.
  await queen.queue(QUEUE).delete()

  console.log(`\nPASS: ${checks} checks`)
} catch (err) {
  console.error(`\nFAIL: ${err.message}`)
  process.exitCode = 1
} finally {
  await queen.close()
}
// docs:end
