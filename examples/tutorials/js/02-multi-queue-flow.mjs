// docs:start(tut-js-multi-queue-flow)
//
// Tutorial 2 of 5: a multi-queue flow.
//
// One queue partitioned per customer, two consumer groups reading it
// independently, and a second queue downstream. This is the shape most
// applications end up with, and it shows the three things that make it work:
// a partition keeps one entity's events in order, a consumer group is a cursor
// so every group sees everything, and a queue is created by the push.
//
//   orders (partition = customer)
//     ├── group "billing"    -> charges, and pushes to the shipping queue
//     └── group "analytics"  -> counts, and pushes nothing
//   shipping
//     └── group "warehouse"  -> ships
//
// Run it:
//   QUEEN_URL=http://localhost:6632 node 02-multi-queue-flow.mjs

import { Queen } from 'queen-mq'

const QUEEN_URL = process.env.QUEEN_URL || 'http://localhost:6632'
const RUN = Date.now().toString(36)
const ORDERS = `tut-js-orders-${RUN}`
const SHIPPING = `tut-js-shipping-${RUN}`

const INPUT = [
  { orderId: 'A-1', customer: 'acme', total: 120.5 },
  { orderId: 'A-2', customer: 'acme', total: 12.0 },
  { orderId: 'B-1', customer: 'globex', total: 88.75 },
  { orderId: 'C-1', customer: 'initech', total: 310.0 },
  { orderId: 'A-3', customer: 'acme', total: 9.99 },
]

let checks = 0
const assert = (condition, description) => {
  if (!condition) throw new Error(description)
  checks++
  console.log(`  ok: ${description}`)
}

const queen = new Queen({ url: QUEEN_URL, handleSignals: false })

try {
  console.log(`broker ${QUEEN_URL}`)

  // Push each order into the partition named after its customer. Everything
  // about one customer stays in order; different customers never wait for each
  // other. The partition key is the only ordering decision you make.
  console.log('\npushing')
  for (const order of INPUT) {
    await queen.queue(ORDERS).partition(order.customer).push({ data: order })
    console.log(`  ${order.orderId} -> partition ${order.customer}`)
  }

  // Group one. It reads every order, charges it, and hands the paid ones to the
  // shipping queue. subscriptionMode('all') matters: a group created after the
  // messages were pushed starts at the tail by default, so without it this
  // group would see nothing.
  console.log('\nbilling')
  const billed = []
  await queen
    .queue(ORDERS)
    .group('tut-js-billing')
    .subscriptionMode('all')
    .each()
    .limit(INPUT.length)
    .idleMillis(5000) // stop after 5s of silence, so a lost message fails the run instead of hanging it
    .consume(async (msg) => {
      billed.push(msg.data.orderId)
      console.log(`  charged ${msg.data.orderId} (${msg.data.total})`)

      // The push to the next queue creates it on first use, exactly like the
      // first queue. Partitioning it by customer as well keeps a customer's
      // shipments in the order their orders were charged.
      await queen.queue(SHIPPING).partition(msg.data.customer).push({
        data: { orderId: msg.data.orderId, customer: msg.data.customer },
      })
    })

  assert(billed.length === INPUT.length, `billing saw all ${INPUT.length} orders`)

  // Group two reads the same stored messages through its own cursor. It was not
  // affected by billing acking them: that is what fan-out means here, and it
  // costs no extra copy of the data.
  console.log('\nanalytics')
  let total = 0
  await queen
    .queue(ORDERS)
    .group('tut-js-analytics')
    .subscriptionMode('all')
    .each()
    .limit(INPUT.length)
    .idleMillis(5000)
    .consume(async (msg) => {
      total += msg.data.total
    })

  assert(
    Math.abs(total - INPUT.reduce((sum, o) => sum + o.total, 0)) < 0.001,
    'analytics summed every order, independently of billing'
  )

  // The order inside one partition is the order it was pushed in. Check the
  // customer with more than one order.
  console.log('\nwarehouse')
  const acmeShipments = []
  await queen
    .queue(SHIPPING)
    .partition('acme')
    .group('tut-js-warehouse')
    .subscriptionMode('all')
    .each()
    .limit(3)
    .idleMillis(5000)
    .consume(async (msg) => {
      acmeShipments.push(msg.data.orderId)
      console.log(`  shipping ${msg.data.orderId}`)
    })

  assert(
    JSON.stringify(acmeShipments) === JSON.stringify(['A-1', 'A-2', 'A-3']),
    'one customer\'s shipments arrived in the order they were pushed'
  )

  await queen.queue(ORDERS).delete()
  await queen.queue(SHIPPING).delete()

  console.log(`\nPASS: ${checks} checks`)
} catch (err) {
  console.error(`\nFAIL: ${err.message}`)
  process.exitCode = 1
} finally {
  await queen.close()
}
// docs:end
