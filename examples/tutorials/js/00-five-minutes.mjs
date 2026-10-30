// Queen in five minutes: the program behind the homepage guide, in JavaScript.
//
// The homepage shows the regions between `guide:start(<step>)` and `guide:end`,
// verbatim. Everything outside them gives the program a clean start and checks
// that each step did what the page says it does.
//
// Run it against the node that step 1 of the guide starts:
//   docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.7.0
//   node 00-five-minutes.mjs
// run.sh runs a copy with localhost:6632 replaced by $QUEEN_URL.

import assert from 'node:assert/strict'
import * as setup from 'queen-mq'

const admin = new setup.Queen({ url: 'http://localhost:6632', handleSignals: false })
for (const q of ['orders', 'shipments', 'reminders', 'reports', 'revenue-per-minute']) {
  try { await admin.queue(q).delete() } catch {}
}

// guide:start(configure)
import { Queen } from 'queen-mq'

const queen = new Queen({ url: 'http://localhost:6632' })

await queen.queue('orders')
  .namespace('shop')              // groups queues in the dashboard
  .config({
    leaseTime: 30,                // seconds a worker holds a message before it is redelivered
    retryLimit: 3,                // failures before the message goes to the dead-letter queue
    retentionEnabled: true,       // retention runs only when this is on
    retentionSeconds: 86400,      // then messages older than a day are removed
  })
  .create()
// guide:end
console.log('ok: configure')

// guide:start(push)
await queen.queue('orders')
  .partition('customer-42')       // one ordered partition per customer, created by this push
  .push([{
    transactionId: 'order-1001',  // the dedup key: pushing the same id again is a no-op
    data: { orderId: 1001, customer: 42, total: 59.9 },
  }])
// guide:end
console.log('ok: push')

// guide:start(consume)
await queen.queue('orders')
  .each()                         // the handler gets one message at a time
  .limit(1)                       // stop after one message; a worker leaves this out
  .consume(async (msg) => {       // acked when the handler returns, retried if it throws
    console.log(msg.data)
  })
// guide:end
assert.equal((await queen.queue('orders').wait(false).pop()).length, 0, 'queue mode acked the order')
console.log('ok: consume')

let billed = null
// guide:start(consume-group)
await queen.queue('orders')
  .group('billing')               // every group gets every message
  .subscriptionMode('all')        // a new group starts at the tail unless told otherwise
  .batch(100)                     // up to 100 messages per pop...
  .partitions(10)                 // ...from up to 10 partitions
  .renewLease(true, 2000)         // heartbeat every 2 s while the handler runs
  .autoAck(false)                 // the handler decides the outcome
  .each()
  .limit(1)
  .consume(async (msg) => {
    const ok = msg.data.total > 0
    await queen.ack(msg, ok ? 'completed' : 'failed', { group: 'billing' })
    billed = msg.data.orderId // guide:hide
  })
// guide:end
assert.equal(billed, 1001, 'the billing group read the order pushed before it existed')
console.log('ok: consume-group')

// guide:start(transaction)
await queen.queue('orders')
  .group('fulfillment')
  .subscriptionMode('all')
  .autoAck(false)
  .each()
  .limit(1)
  .consume(async (msg) => {
    const { orderId, customer } = msg.data
    await queen.transaction()
      .ack(msg, 'completed', { consumerGroup: 'fulfillment' })  // the input is done
      .kv.put('orders', `order-${orderId}`, { status: 'paid' }, { ttl: '7d' })
      .queue('shipments').partition(msg.partition)              // the next event
      .push([{ transactionId: `ship-${orderId}`, data: { orderId } }])
      .timer('reminders').key(`review-${orderId}`).delay('24h') // a message tomorrow
      .payload({ orderId, customer }).schedule()
      .commit()
  })
// guide:end
const state = await queen.kv.get('orders', 'order-1001')
assert.equal(state.value.status, 'paid', 'the KV write committed')
const shipped = await queen.queue('shipments').wait(false).pop()
assert.equal(shipped.length, 1, 'the shipment was pushed')
assert.equal(shipped[0].partition, 'customer-42', 'into the same customer partition')
console.log('ok: transaction')

let replayed = null
// guide:start(replay)
await queen.queue('orders')
  .group('audit')                           // a new group, with its own cursor
  .subscriptionFrom('2026-10-22T00:00:00Z') // everything pushed from this time on
  .each()
  .limit(1)
  .consume(async (msg) => {
    console.log('again:', msg.data)
    replayed = msg.data.orderId // guide:hide
  })
// guide:end
assert.equal(replayed, 1001, 'the audit group read the order again')
console.log('ok: replay')

// guide:start(fetch)
const read = await queen.queue('orders').fetch(['customer-42']) // no group, lease or ack
for (const record of read.get('customer-42').records) {
  console.log(record.offset, record.payload)
}
// guide:end
assert.equal(read.get('customer-42').records[0]?.payload?.orderId, 1001, 'fetch read the partition from its first record')
console.log('ok: fetch')

// guide:start(lock)
const lock = queen.lock('daily-report', { ttl: '30s' }) // a lease with a fencing token
if (await lock.acquire()) {                             // false: another worker has it
  try {
    await queen.transaction()
      .guard(lock)                                      // commits only while the lock is ours
      .queue('reports').push([{ transactionId: 'report-2026-10-22', data: { day: '2026-10-22' } }])
      .commit()
  } finally {
    await lock.release()
  }
}
// guide:end
assert.equal((await queen.queue('reports').wait(false).pop()).length, 1, 'the guarded report was pushed')
console.log('ok: lock')

// guide:start(stream)
import { Stream } from 'queen-mq'

const revenue = await Stream
  .from(queen.queue('orders'))                 // one window per partition: per customer
  .windowTumbling({ seconds: 60 })
  .aggregate({ count: () => 1, sum: (order) => order.total })
  .to(queen.queue('revenue-per-minute'))       // one result per customer per minute
  .run({ queryId: 'revenue-per-minute', url: 'http://localhost:6632' })
// guide:end
await new Promise((r) => setTimeout(r, 1000)) // let the stream's group register at the tail
await queen.queue('orders').partition('customer-42')
  .push([{ transactionId: `order-1002-${Date.now()}`, data: { orderId: 1002, customer: 42, total: 40.1 } }])
let windows = []
for (let i = 0; i < 100 && windows.length === 0; i++) {
  windows = await queen.queue('revenue-per-minute').wait(true).timeoutMillis(1000).pop()
}
await revenue.stop()
assert.equal(windows.length, 1, 'the window closed and its result was pushed')
assert.equal(windows[0].data.count, 1, 'one order in the window')
assert.equal(windows[0].data.sum, 40.1, 'its total summed')
console.log('ok: stream', JSON.stringify(windows[0].data))

const worker = (async () => {
  // guide:start(worker)
  const { messages } = await queen.ephemeral.pop('price-requests', { group: 'pricers', wait: true })
  for (const m of messages) {
    await queen.ephemeral.push(m.payload.replyTo, [{ sku: m.payload.sku, price: 59.9 }])
  }
  await queen.ephemeral.ack('price-requests', messages, { group: 'pricers' })
  // guide:end
})()
await new Promise((r) => setTimeout(r, 300)) // the worker is already waiting when a caller asks
const caller = (async () => {
  // guide:start(caller)
  const inbox = `inbox-${crypto.randomUUID()}`           // a private reply queue
  await queen.ephemeral.push('price-requests', [{ sku: 'A-1', replyTo: inbox }])
  const reply = await queen.ephemeral.pop(inbox, { wait: true, timeout: 5000, commitOnDelivery: true })
  console.log(reply.messages[0]?.payload)                // { sku: 'A-1', price: 59.9 }
  // guide:end
  return reply
})()
const [, reply] = await Promise.all([worker, caller])
assert.equal(reply.messages[0]?.payload?.price, 59.9, 'the reply came back')
console.log('ok: ephemeral')

await queen.close()
await admin.close()
console.log('\nPASS')
