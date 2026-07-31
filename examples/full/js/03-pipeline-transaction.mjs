// docs:start(full-js-pipeline)
//
// Queen MQ by example, 3 of 3: the transactional handoff.
//
// A pipeline stage consumes from "orders" and, in a SINGLE transaction, both
// acknowledges the input and pushes the derived message to "invoices". Either
// both happen or neither does, so no crash can leave a stage that consumed an
// order without emitting its invoice.
//
// The last section shows the other half of the contract: a message acknowledged
// as failed is redelivered, not lost.
//
// Run it:
//   QUEEN_URL=http://localhost:6699 node 03-pipeline-transaction.mjs
//
// The program checks its own outcome and exits non-zero if any check fails.

import { Queen } from 'queen-mq'

const QUEEN_URL = process.env.QUEEN_URL || 'http://localhost:6699'

// Prefixed per language, suffixed per run, so runs and languages never collide.
const RUN_ID = Date.now().toString(36)
const Q_ORDERS = `ex-js-pipeline-orders-${RUN_ID}`
const Q_INVOICES = `ex-js-pipeline-invoices-${RUN_ID}`

// Each stage reads with its own consumer group, so each stage has its own cursor.
const G_INVOICER = 'ex-js-invoicer' // stage one: orders  -> invoices
const G_LEDGER = 'ex-js-ledger' // stage two: invoices -> here

const ORDERS = [
  { orderId: 'ORD-1', customer: 'acme', total: 120.5 },
  { orderId: 'ORD-2', customer: 'globex', total: 88.75 },
  { orderId: 'ORD-3', customer: 'initech', total: 310.0 },
  { orderId: 'ORD-4', customer: 'acme', total: 12.0 },
]

const VAT = 1.22

let checks = 0

function assert(condition, description) {
  if (!condition) throw new Error(description)
  checks++
  console.log(`  ok: ${description}`)
}

// The derived message: what stage one makes out of an order.
function invoiceFor(order) {
  return {
    invoiceFor: order.orderId,
    customer: order.customer,
    amountDue: Math.round(order.total * VAT * 100) / 100,
  }
}

const queen = new Queen({ url: QUEEN_URL, handleSignals: false })

try {
  console.log(`broker ${QUEEN_URL}`)

  await queen.queue(Q_ORDERS).config({
    leaseTime: 30,
    // How many times a failed message is redelivered before it goes to the
    // dead letter queue. The failure section below spends one of these.
    retryLimit: 3,
  }).create()
  await queen.queue(Q_INVOICES).config({ leaseTime: 30, retryLimit: 3 }).create()
  console.log(`queues ${Q_ORDERS} and ${Q_INVOICES} created`)

  console.log('\npushing orders')
  for (const order of ORDERS) {
    // One partition per customer, so one customer's orders stay in order while
    // different customers are free to be processed in parallel.
    const [result] = await queen.queue(Q_ORDERS).partition(order.customer).push([{ data: order }])
    if (result.status !== 'queued') throw new Error(`push of ${order.orderId} answered ${result.status}`)
    console.log(`  ${order.orderId} -> partition ${order.customer}`)
  }

  // -------------------------------------------------- stage one: orders -> invoices

  console.log('\nstage one: ack the order and push the invoice in one transaction')
  let handled = 0
  while (handled < ORDERS.length) {
    // One message at a time keeps the unit of work equal to the unit of
    // transaction, which is the whole point of this stage.
    const batch = await queen.queue(Q_ORDERS)
      .group(G_INVOICER)
      .batch(1)
      .wait(true)
      .timeoutMillis(5000)
      .pop()

    // pop() returns an empty array both when the queue is empty and when the
    // call failed, so an empty result ends the loop and the assertion below
    // reports the shortfall instead of the loop spinning forever.
    if (batch.length === 0) break
    const order = batch[0]

    // The transaction: the ack of the input and the push of the output land in
    // the same PostgreSQL commit. The leaseId of the acked message travels with
    // it as a required lease, so if the lease had expired (another consumer
    // already took the order) the whole commit is refused, including the push.
    const result = await queen.transaction()
      .ack(order, 'completed', { consumerGroup: G_INVOICER })
      .queue(Q_INVOICES)
      .partition(order.partition)
      // Inside a transaction the broker mints the transactionId for the pushed
      // message; the client does not forward one of its own here.
      .push([{ data: invoiceFor(order.data) }])
      .commit()

    // commit() throws unless the broker answered success, so reaching this line
    // means both operations are durable.
    const kinds = result.results.map(r => `${r.type}:${r.success}`).join(' ')
    console.log(`  ${order.data.orderId} committed (${kinds})`)
    handled++
  }

  console.log('\nchecking stage one')
  assert(handled === ORDERS.length, `stage one handled all ${ORDERS.length} orders`)

  // Every ack in those transactions committed, so this group's cursor is at the
  // end of the queue and there is nothing left to hand off.
  const remaining = await queen.queue(Q_ORDERS).group(G_INVOICER).batch(10).wait(false).pop()
  assert(remaining.length === 0, 'no order is left unacknowledged')

  // -------------------------------------------- stage two: read what was derived

  console.log('\nstage two: consuming invoices')
  const invoices = []
  await queen.queue(Q_INVOICES)
    .group(G_LEDGER)
    .each()
    .batch(ORDERS.length)
    .partitions(3) // the three customer partitions, claimed in one poll
    .autoAck(false)
    .limit(ORDERS.length)
    .idleMillis(5000)
    .consume(async (msg) => {
      console.log(`  invoice for ${msg.data.invoiceFor}: ${msg.data.amountDue}`)
      invoices.push(msg.data)
      const ack = await queen.ack(msg, true, { group: G_LEDGER })
      if (!ack.success) throw new Error(`ack rejected: ${ack.error}`)
    })

  console.log('\nchecking the derived messages')
  assert(invoices.length === ORDERS.length, `every order produced one invoice (${ORDERS.length})`)
  const invoicedOrders = invoices.map(i => i.invoiceFor)
  assert(new Set(invoicedOrders).size === invoicedOrders.length, 'no invoice was produced twice')
  assert(
    ORDERS.every(order => {
      const invoice = invoices.find(i => i.invoiceFor === order.orderId)
      return invoice && invoice.amountDue === invoiceFor(order).amountDue
    }),
    'every invoice carries the amount derived from its order'
  )

  // ------------------------------------------------- what a failure would do

  console.log('\nfailure path: acknowledging one order as failed')
  const late = { orderId: 'ORD-5', customer: 'acme', total: 55.0 }
  const [pushed] = await queen.queue(Q_ORDERS).partition(late.customer).push([{ data: late }])

  const firstTry = await queen.queue(Q_ORDERS).group(G_INVOICER).batch(1).wait(true).timeoutMillis(5000).pop()
  assert(firstTry.length === 1, 'the new order was delivered once')
  assert(firstTry[0].transactionId === pushed.transaction_id, 'it is the order that was just pushed')

  // status false means `failed`. The broker releases the lease and clamps this
  // group's cursor at the failed message, so it is queued for redelivery rather
  // than being dropped. The error text is kept and lands on the dead letter row
  // if the retry budget ever runs out.
  const nack = await queen.ack(firstTry[0], false, {
    group: G_INVOICER,
    error: 'ledger unreachable, will retry',
  })
  assert(nack.success, 'the broker accepted the failed acknowledgement')

  const secondTry = await queen.queue(Q_ORDERS).group(G_INVOICER).batch(1).wait(true).timeoutMillis(5000).pop()
  assert(secondTry.length === 1, 'the failed order came back')
  assert(secondTry[0].transactionId === pushed.transaction_id, 'it is the same message, redelivered rather than lost')
  assert(secondTry[0].data.orderId === late.orderId, 'with its payload intact')

  // The retry succeeds this time, through the same transactional handoff.
  await queen.transaction()
    .ack(secondTry[0], 'completed', { consumerGroup: G_INVOICER })
    .queue(Q_INVOICES)
    .partition(secondTry[0].partition)
    .push([{ data: invoiceFor(late) }])
    .commit()

  const recovered = await queen.queue(Q_INVOICES).group(G_LEDGER).batch(5).wait(true).timeoutMillis(5000).pop()
  assert(recovered.length === 1, 'the retry produced exactly one invoice')
  assert(recovered[0].data.invoiceFor === late.orderId, `it is the invoice for ${late.orderId}`)
  const finalAck = await queen.ack(recovered, true, { group: G_LEDGER })
  assert(finalAck.success, 'and it acknowledges cleanly')

  // Clean up. Only on success, so a failed run leaves both queues on the broker.
  await queen.queue(Q_ORDERS).delete()
  await queen.queue(Q_INVOICES).delete()

  console.log(`\nPASS: ${checks} checks`)
} catch (err) {
  console.error(`\nFAIL: ${err.message}`)
  process.exitCode = 1
} finally {
  await queen.close()
}
// docs:end
