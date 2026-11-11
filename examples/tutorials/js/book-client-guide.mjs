// The little book of QueenMQ: the examples of chapter 2 ("Writing applications")
// that are not already in the five-minute guide (00-five-minutes.mjs).
//
// The book shows the regions between `guide:start(<id>)` and `guide:end`,
// verbatim, minus lines ending in `guide:hide`. Everything outside them gives
// the program a clean start and checks that each example does what the book
// says it does.
//
// Run it against the node that the book's first section starts:
//   docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.8.1
//   node book-client-guide.mjs
// run.sh runs a copy with localhost:6632 replaced by $QUEEN_URL.

import assert from 'node:assert/strict'
import { Queen, Stream } from 'queen-mq'

const queen = new Queen({ url: 'http://localhost:6632', handleSignals: false })
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const QUEUES = ['jobs', 'emails', 'events', 'reviews', 'prices', 'metrics', 'tasks', 'payments', 'payments-statuses', 'logins',
  'login-rate', 'clicks', 'sales', 'sales-per-hour', 'outgoing-sms', 'invoices',
  // the feed streams' (end of this program)
  'orders', 'orders-it', 'orders-IT', 'orders-FR', 'per-minute', 'order-followups', 'shipments', 'order-status', 'parts',
  'batches', 'bookings', 'property-settings', 'bookings-priced', 'bursts', 'gh-issues', 'gh-comments', 'gh-unanswered',
  'clicks-per-tenant']
for (const q of QUEUES) {
  try { await queen.queue(q).delete() } catch {}
}

// --- Configure ------------------------------------------------------------
// guide:start(configure-jobs)
await queen.queue('jobs')
  .config({
    leaseTime: 120,                   // 2 minutes per job, then it goes to another worker
    retryLimit: 5,                    // after 5 failures, the dead-letter queue
    retryDelay: 10000,                // a failed job comes back after 10 s, not at once
    delayedProcessing: 30,            // a new job waits 30 s before anyone can take it
    windowBuffer: 5,                  // a burst waits 5 s, then is handed over at once
    dedupWindowSeconds: 86400,        // a transactionId is remembered for a day
    retentionEnabled: true,           // retention runs only when this is on:
    completedRetentionSeconds: 3600,  //   jobs every group has finished go after an hour
    maxWaitTimeSeconds: 604800,       // anything older than a week goes, finished or not
  })
  .create()
// guide:end
{
  const { options } = await queen.admin.getQueue('jobs')
  assert.equal(options.retryDelay, 10000)
  assert.equal(options.delayedProcessing, 30)
  assert.equal(options.windowBuffer, 5)
  assert.equal(options.retentionEnabled, true)
  assert.equal(options.completedRetentionSeconds, 3600)
  assert.equal(options.maxWaitTimeSeconds, 604800)
  console.log('ok: configure-jobs')
}

// --- Push -----------------------------------------------------------------
// guide:start(push-callbacks)
await queen.queue('emails')
  .partition('customer-42')
  .push([{ transactionId: 'welcome-42', data: { to: 'ada@example.com' } }])
  .onSuccess((items) => console.log('stored', items.length))
  .onDuplicate((items) => console.log('already stored', items[0].transactionId))
  .onError((items, error) => console.error('refused', error.message))
// guide:end
{
  let duplicate = null
  await queen.queue('emails').partition('customer-42')
    .push([{ transactionId: 'welcome-42', data: { to: 'ada@example.com' } }])
    .onDuplicate((items) => { duplicate = items[0].transactionId })
  assert.equal(duplicate, 'welcome-42', 'the second push of the same id reached onDuplicate')
  console.log('ok: push-callbacks')
}

// guide:start(push-buffer)
for (let i = 0; i < 1000; i++) {
  await queen.queue('events')
    .partition(`device-${i % 10}`)
    .buffer({ messageCount: 100, timeMillis: 1000 })  // 100 at a time, or every second
    .push([{ transactionId: `reading-${i}`, data: { device: i % 10, value: i } }])
}
await queen.flushAllBuffers()                         // before exiting, send what is left
// guide:end
{
  const names = Array.from({ length: 10 }, (_, i) => `device-${i}`)
  const read = await queen.queue('events').fetch(names)
  const total = [...read.values()].reduce((n, p) => n + p.records.length, 0)
  assert.equal(total, 1000, 'every buffered reading was stored')
  console.log('ok: push-buffer')
}

// Record what goes over the wire, to check the binary forms below.
const wire = []
const realFetch = globalThis.fetch
globalThis.fetch = async (url, options = {}) => {
  const response = await realFetch(url, options)
  wire.push({ url: String(url), body: options.body, got: response.headers.get('content-type'),
    sent: new Headers(options.headers).get('content-type') })
  return response
}
let wireData = null
// guide:start(wire-forms)
const fast = new Queen({
  url: 'http://localhost:6632',
  handleSignals: false, // guide:hide
  binaryPush: true,       // a push travels as one binary body, not JSON
  pushCompression: true,  // with binaryPush: each partition's batch is zstd-compressed here
  framedPop: true,        // a pop gets the records as stored, and unpacks them here
})

await fast.queue('invoices').partition('customer-42')
  .push([{ transactionId: 'invoice-2001', data: { orderId: 2001, total: 12.5 } }])

await fast.queue('invoices').each().limit(1).consume(async (msg) => {
  wireData = msg.data // guide:hide
  console.log(msg.data)           // { orderId: 2001, total: 12.5 }, as with JSON
})
// guide:end
{
  globalThis.fetch = realFetch
  const push = wire.find((w) => w.url.includes('/api/v1/push'))
  const pop = wire.find((w) => w.url.includes('/api/v1/pop/') && w.url.includes('frames=true'))
  assert.equal(push.sent, 'application/x-queen-push', 'the push went as a binary body')
  const zstdMagic = Buffer.from([0x28, 0xb5, 0x2f, 0xfd])
  assert.ok(Buffer.from(push.body).includes(zstdMagic), 'the binary push carried a zstd frame')
  assert.equal(pop.got, 'application/x-queen-pop', 'the broker answered the pop with frames')
  assert.deepEqual(wireData, { orderId: 2001, total: 12.5 })
  console.log('ok: wire-forms')
}

// --- Consume --------------------------------------------------------------
let archived = 0
// guide:start(consume-batch)
await queen.queue('events')
  .group('archiver')
  .subscriptionMode('all')
  .batch(100)                         // up to 100 messages per call
  .limit(1000)
  .consume(async (messages) => {      // no .each(): the handler gets the whole batch
    console.log('archiving', messages.length)
    archived += messages.length // guide:hide
  })                                  // all acked if it returns, all nacked if it throws
// guide:end
assert.equal(archived, 1000, 'the batch handler saw every reading once')
console.log('ok: consume-batch')

// guide:start(pop-ack)
const messages = await queen.queue('events')
  .group('auditor')
  .subscriptionMode('all')
  .batch(50)
  .wait(true)                         // long poll: wait for messages, never return empty
  .pop()
console.log('auditing', messages.length)
await queen.ack(messages, 'completed', { group: 'auditor' })   // the whole batch at once
// guide:end
assert.ok(messages.length > 0 && messages.length <= 50, 'pop returned a batch')
console.log('ok: pop-ack')

await queen.queue('reviews').partition('shop-3')
  .push([{ transactionId: 'review-1', data: { stars: 5 } }, { transactionId: 'review-2', data: { stars: 1 } }])
const [first, second] = await queen.queue('reviews').partition('shop-3').group('moderation')
  .subscriptionMode('all').batch(2).wait(true).pop()
// guide:start(ack-each-status)
await queen.ack([
  { ...first, _status: 'completed' },
  { ...second, _status: 'failed', _error: 'timeout' },
], 'completed', { group: 'moderation' })
// guide:end
{
  // A long poll already waiting is not woken when retryDelay passes (a broker
  // gap, reported); a new pop after the delay gets the message.
  let back
  for (let i = 0; i < 5 && !back; i++) {
    ;[back] = await queen.queue('reviews').partition('shop-3').group('moderation')
      .batch(2).wait(true).timeoutMillis(1500).pop()
  }
  assert.equal(back?.transactionId, 'review-2', 'only the failed review came back')
  await queen.ack(back, 'completed', { group: 'moderation' })
  console.log('ok: ack-each-status')
}

{
  // The helpers the examples call. In a real program they are yours.
  globalThis.sendEmail = async () => {}
}
let mailed = null
// guide:start(consume-callbacks)
await queen.queue('emails')
  .group('mailer')
  .subscriptionMode('all')
  .each()
  .limit(1)
  .consume(async (msg) => sendEmail(msg.data))
  .onSuccess((msg) => queen.ack(msg, 'completed', { group: 'mailer' }))
  .onError((msg, error) => queen.ack(msg, error.permanent ? 'dlq' : 'failed',
    { group: 'mailer', error: error.message }))
// guide:end
mailed = (await queen.queue('emails').group('mailer').wait(false).pop()).length
assert.equal(mailed, 0, 'the onSuccess callback acked the email')
console.log('ok: consume-callbacks')

for (const price of [10, 11, 12]) {
  await queen.queue('prices').partition('sku-A-1')
    .push([{ transactionId: `price-A-1-${price}`, data: { sku: 'A-1', price } }])
}
// guide:start(conflation)
const [latest] = await queen.queue('prices')
  .group('repricer')
  .subscriptionMode('all')
  .conflation()                       // only the newest message of each partition
  .wait(false)
  .pop()
console.log(latest.data)              // { sku: 'A-1', price: 12 }: 10 and 11 are skipped
await queen.ack(latest, 'completed', { group: 'repricer' })
// guide:end
assert.equal(latest.data.price, 12, 'conflation delivered the newest price')
assert.equal((await queen.queue('prices').group('repricer').conflation().wait(false).pop()).length, 0,
  'and moved past the older ones')
console.log('ok: conflation')

for (let i = 0; i < 5; i++) {
  await queen.queue('metrics').partition('cpu').push([{ transactionId: `cpu-${i}`, data: { load: i } }])
}
// guide:start(commit-on-delivery)
const readings = await queen.queue('metrics')
  .group('live-chart')
  .subscriptionMode('all')
  .commitOnDelivery()                 // the cursor moves as they are handed out: no ack
  .batch(100)
  .wait(false)
  .pop()
// guide:end
assert.equal(readings.length, 5, 'commitOnDelivery handed out every reading')
assert.equal((await queen.queue('metrics').group('live-chart').wait(false).pop()).length, 0,
  'and nothing is left to ack or redeliver')
console.log('ok: commit-on-delivery')

// guide:start(seek)
await queen.admin.seekConsumerGroup('archiver', 'events', {
  timestamp: '2026-10-22T00:00:00Z',
})
// the archiver group now reads again everything pushed since that time

await queen.admin.seekConsumerGroup('archiver', 'events', { toEnd: true })
// and now it skips everything it has not read yet
// guide:end
assert.equal((await queen.queue('events').group('archiver').wait(false).pop()).length, 0,
  'after a seek to the end the group has nothing left')
console.log('ok: seek')

for (let i = 0; i < 40; i++) {
  await queen.queue('tasks').partition(`account-${i % 8}`).push([{ transactionId: `task-${i}`, data: { n: i } }])
}
let ran = 0
globalThis.runTask = async () => { ran++; await sleep(5) }
// guide:start(consume-run)
const stop = new AbortController()
process.once('SIGTERM', () => stop.abort())     // a deploy stops the worker cleanly
const watcher = setInterval(() => { if (ran >= 40) stop.abort() }, 20) // guide:hide

await queen.queue('tasks')
  .group('workers')
  .subscriptionMode('all')
  .concurrency(4)                               // four handlers at once, on four partitions
  .renewLease(true, 10000)                      // long tasks keep their lease alive
  .idleMillis(60000)                            // or stop after a minute with nothing to do
  .each()
  .consume(async (task) => runTask(task.data), { signal: stop.signal })
// guide:end
clearInterval(watcher)
assert.equal(ran, 40, 'four workers ran every task, then the signal stopped them')
console.log('ok: consume-run')

// --- Retries and the dead-letter queue --------------------------------------
await queen.queue('payments').config({ retryLimit: 1, retryDelay: 100 }).create()
await queen.queue('payments').partition('order-7').push([{ transactionId: 'charge-7', data: { amount: 25 } }])
await queen.queue('payments').group('charger').subscriptionMode('all').each().limit(2).idleMillis(5000)
  .consume(async () => { throw new Error('card declined') })
await sleep(300)
// guide:start(dlq-read)
const dead = await queen.queue('payments').dlq('charger').limit(20).get()
for (const letter of dead.messages) {
  console.log(letter.transactionId, letter.errorMessage, letter.retryCount)
}
// guide:end
assert.equal(dead.messages.length, 1, 'the failing payment is in the dead-letter queue')
assert.equal(dead.messages[0].errorMessage, 'card declined')
console.log('ok: dlq-read')

// guide:start(dlq-replay)
for (const letter of dead.messages) {
  // back into its partition, and out of the dead-letter queue
  await queen.admin.retryMessage(letter.partitionId, letter.transactionId)
}
// guide:end
{
  const again = await queen.queue('payments').group('charger').batch(10).wait(true).timeoutMillis(3000).pop()
  assert.equal(again.length, 1, 'the replayed payment is delivered again')
  await queen.ack(again, 'completed', { group: 'charger' })
  const left = await queen.queue('payments').dlq('charger').get()
  assert.equal(left.messages.length, 0, 'and it left the dead-letter queue')
  console.log('ok: dlq-replay')
}

{
  // The four ack statuses the book's table describes, checked here. 'retry'
  // gives a message back at once without spending a retry; 'dlq' files it now.
  // The queue allows one retry: after a free 'retry', a 'failed' still comes back.
  await queen.queue('payments-statuses').config({ retryLimit: 1, retryDelay: 0 }).create()
  const q = () => queen.queue('payments-statuses').partition('order-8').group('statuses').batch(1).wait(true)
  await queen.queue('payments-statuses').partition('order-8').push([{ transactionId: 'charge-8', data: { amount: 9 } }])
  const [r1] = await q().subscriptionMode('all').pop()
  await queen.ack(r1, 'retry', { group: 'statuses' })
  const [r2] = await q().pop()
  assert.equal(r2.transactionId, r1.transactionId, "'retry' redelivered the same message at once")
  await queen.ack(r2, 'failed', { group: 'statuses', error: 'try again' })
  const [r3] = await q().pop()
  assert.equal(r3?.transactionId, r1.transactionId, "'retry' spent no retry: the one retry is still there")
  await queen.ack(r3, 'dlq', { group: 'statuses', error: 'bad payload' })
  const filed = await queen.queue('payments-statuses').dlq('statuses').get()
  assert.equal(filed.messages.length, 1, "'dlq' filed the message in the dead-letter queue")
  assert.equal(filed.messages[0].errorMessage, 'bad payload')
  console.log('ok: ack statuses')
}

// --- KV -------------------------------------------------------------------
await queen.kv.delete('carts', 'customer-42')
await queen.kv.delete('limits', 'api:customer-42')
// guide:start(kv)
// Every write names its lifetime: a ttl, or forever.
await queen.kv.put('carts', 'customer-42', { items: 3 }, { ttl: '7d' })
const cart = await queen.kv.get('carts', 'customer-42')   // { found, value, version, ... }

// Write only if nobody wrote since we read it: the version is the condition.
const saved = await queen.kv.put('carts', 'customer-42', { items: 4 },
  { ttl: '7d', expect: cart.version })
console.log(saved.applied)                                // false if the version moved

// A counter with a ceiling: past max, the increment does not apply.
const hit = await queen.kv.incr('limits', 'api:customer-42', 1, { max: 100, ttl: '1m' })
if (!hit.applied) console.log('over the limit')           // reason: 'limit'
// guide:end
{
  assert.equal(cart.found, true, 'the cart was read back')
  assert.deepEqual(cart.value, { items: 3 })
  assert.equal(saved.applied, true, 'the conditional write applied')
  const stale = await queen.kv.put('carts', 'customer-42', { items: 9 }, { ttl: '7d', expect: cart.version })
  assert.equal(stale.applied, false, 'a write from the old version is refused')
  assert.equal(stale.reason, 'version')
  assert.deepEqual(stale.value, { items: 4 }, 'and answers the current value')
  assert.equal(hit.applied, true)
  const capped = await queen.kv.incr('limits', 'api:customer-42', 100, { max: 100, ttl: '1m' })
  assert.equal(capped.applied, false, 'an increment past max does not apply')
  assert.equal(capped.reason, 'limit')
  assert.equal(capped.value, 1, 'and the counter keeps its value')
}
console.log('ok: kv')

// --- Streams ----------------------------------------------------------------
// guide:start(stream-sliding)
const loginRate = await Stream
  .from(queen.queue('logins'))                   // one partition per user
  .windowSliding({ size: 600, slide: 60 })     // the last 10 minutes, every minute
  .aggregate({ count: () => 1 })
  .to(queen.queue('login-rate'))
  .run({ queryId: 'login-rate', url: 'http://localhost:6632' })
// guide:end
await sleep(1200)
await loginRate.stop()
console.log('ok: stream-sliding')

// guide:start(stream-session)
const visits = await Stream
  .from(queen.queue('clicks'))                   // one partition per visitor
  .windowSession({ gap: 1800 })                  // a visit ends after 30 minutes of quiet
  .aggregate({ count: () => 1 })
  .foreach(async (visit) => console.log('visit ended', visit))   // acked after this returns
  .run({ queryId: 'visits', url: 'http://localhost:6632' })
// guide:end
await sleep(1200)
await visits.stop()
console.log('ok: stream-session')

// guide:start(stream-hourly)
const hourly = await Stream
  .from(queen.queue('sales'))
  .windowCron({
    every: 'hour',                     // on the clock: 10:00 to 11:00, 11:00 to 12:00
    eventTime: (msg) => msg.data.soldAt,  // when the sale happened, not when it arrived
    allowedLateness: 120,              // a sale up to 2 minutes late still counts
  })
  .aggregate({ count: () => 1, sum: (sale) => sale.amount, max: (sale) => sale.amount })
  .to(queen.queue('sales-per-hour'))
  .run({ queryId: 'sales-per-hour', url: 'http://localhost:6632', subscriptionMode: 'all' })
// guide:end
{
  // One sale, to run the eventTime function: its hour stays open, so nothing is
  // emitted yet, but a time the function cannot read fails the cycle.
  await queen.queue('sales').partition('shop-1')
    .push([{ transactionId: `sale-${Date.now()}`, data: { soldAt: new Date().toISOString(), amount: 30 } }])
  for (let i = 0; i < 50 && hourly.metrics().messagesTotal === 0 && hourly.metrics().errorsTotal === 0; i++) await sleep(100)
  const m = hourly.metrics()
  assert.equal(m.errorsTotal, 0, 'eventTime read the sale time')
  assert.ok(m.messagesTotal >= 1, 'the hourly window took the sale')
}
await hourly.stop()
console.log('ok: stream-hourly')

const sent = []
globalThis.sendSms = async (sms) => { sent.push(sms) }
// guide:start(stream-gate)
import { tokenBucketGate } from 'queen-mq'

const limiter = await Stream
  .from(queen.queue('outgoing-sms'))             // one partition per customer
  .gate(tokenBucketGate({ capacity: 10, refillPerSec: 1 }))   // 10 at once, then 1 a second
  .foreach(async (sms) => sendSms(sms))
  .run({ queryId: 'sms-limiter', url: 'http://localhost:6632' })
// guide:end
await sleep(1000)
for (let i = 0; i < 3; i++) {
  await queen.queue('outgoing-sms').partition('customer-9')
    .push([{ transactionId: `sms-${Date.now()}-${i}`, data: { to: '+390000000', text: `hello ${i}` } }])
}
for (let i = 0; i < 100 && sent.length < 3; i++) await sleep(100)
await limiter.stop()
assert.equal(sent.length, 3, 'the gate let the first three messages through')
console.log('ok: stream-gate')

// --- Streams from the tenant feed (broker 2.8.0) --------------------------------
// A feed stream keeps its position and state in KV (`queen-feed`, under its
// queryId): an earlier run's go first, so every run starts the same way.
const FEED_QUERIES = ['orders-it', 'orders-by-country', 'per-minute', 'order-followups', 'order-status', 'batches',
  'bookings-priced', 'bursts', 'gh-unanswered', 'clicks-per-tenant', 'orders-to-warehouse']
for (const id of FEED_QUERIES) {
  const { rows } = await queen.kv.getPrefix('queen-feed', id, { keysOnly: true, limit: 1000 })
  for (const row of rows) await queen.kv.delete('queen-feed', row.key)
}
const push = (queue, partition, data) => queen.queue(queue).partition(partition)
  .push([{ transactionId: `${queue}-${partition}-${Date.now()}-${Math.random()}`, data }])
// The messages of a queue, read back through the feed; waits for `n` of them.
async function messagesOf(queue, n) {
  for (let i = 0; ; i++) {
    const got = []
    for await (const e of queen.feed({ queues: [queue], from: 'earliest', follow: false })) got.push(e.data)
    if (got.length >= n || i >= 100) return got
    await sleep(100)
  }
}
async function until(what, cond) {
  for (let i = 0; i < 100; i++) {
    if (await cond()) return
    await sleep(100)
  }
  assert.fail(`timed out waiting for ${what}`)
}

// guide:start(feed-route)
const ordersIt = await Stream
  .fromFeed(queen, { queues: ['orders'] })
  .where('data.country', '=', 'IT')
  .filter((m) => m.data.total >= 10)
  .to(queen.queue('orders-it'))
  .run({ queryId: 'orders-it' })
// guide:end
await push('orders', 'order-1', { id: 1, country: 'IT', total: 20 })
await push('orders', 'order-2', { id: 2, country: 'IT', total: 5 })
await push('orders', 'order-3', { id: 3, country: 'DE', total: 30 })
assert.deepEqual((await messagesOf('orders-it', 1)).map((o) => o.id), [1], 'only the Italian order of 10 or more')
await ordersIt.stop()
console.log('ok: feed-route')

// guide:start(feed-routing)
const byCountry = await Stream
  .fromFeed(queen, { queues: ['orders'] })
  .to((order) => `orders-${order.country}`)
  .run({ queryId: 'orders-by-country' })
// guide:end
await push('orders', 'order-4', { id: 4, country: 'IT', total: 12 })
await push('orders', 'order-5', { id: 5, country: 'FR', total: 8 })
assert.deepEqual((await messagesOf('orders-IT', 1)).map((o) => o.id), [4])
assert.deepEqual((await messagesOf('orders-FR', 1)).map((o) => o.id), [5])
await byCountry.stop()
console.log('ok: feed-routing')

// A table elsewhere, written by key (an upsert): a second write of a row changes nothing.
const warehouse = { rows: new Map(), async upsert(rows) { for (const r of rows) this.rows.set(r.key, r) } }
// guide:start(feed-batch)
const toWarehouse = await Stream
  .fromFeed(queen, { queues: ['orders'] })
  .foreachBatch(async (items) => {
    // One write for the batch. An item's id is the same each time it is delivered.
    await warehouse.upsert(items.map((it) => ({ key: it.id, ...it.value })))
  }, { maxItems: 500, maxWaitMillis: 1000 })
  .run({ queryId: 'orders-to-warehouse' })
// guide:end
await push('orders', 'order-8', { id: 8, total: 20 })
await push('orders', 'order-9', { id: 9, total: 30 })
await until('the batch', () => warehouse.rows.size === 2)
assert.deepEqual([...warehouse.rows.values()].map((r) => r.id).sort(), [8, 9])
await toWarehouse.stop()
console.log('ok: feed-batch')

// guide:start(feed-window)
const perMinute = await Stream
  .fromFeed(queen, { queues: ['orders', 'payments'] })
  .windowTumbling({ seconds: 60 })
  .aggregate({ count: () => 1, sum: (e) => e.total })
  .map((totals, w) => ({ ...totals, customer: w.key, minute: w.windowKey }))
  .to(queen.queue('per-minute'))
  .run({ queryId: 'per-minute' })
// guide:end
await push('orders', 'customer-1', { total: 10 })
await push('payments', 'customer-1', { total: 5 })
// The window closes at the end of its minute; until then the stream shows it open.
await until('the open window', () => perMinute.state('step1').get('customer-1').value?.windows[0].value.count === 2)
assert.deepEqual(perMinute.state('step1').get('customer-1').value.windows[0].value, { count: 2, sum: 15 })
await perMinute.stop()
console.log('ok: feed-window')

// guide:start(feed-pattern)
const followups = await Stream
  .fromFeed(queen, { queues: ['orders', 'payments'] })
  .pattern({
    start: (m) => m.queue === 'orders',
    then: (m) => m.queue === 'payments',
    within: '15m',
    onMatch: (order, payment) => ({ order: order.data.id, paid: payment.data.amount }),
    onTimeout: (order) => ({ order: order.data.id, remind: true })
  })
  .to(queen.queue('order-followups'))
  .run({ queryId: 'order-followups' })
// guide:end
await push('orders', 'order-7', { id: 7 })
await push('payments', 'order-7', { amount: 30 })
assert.deepEqual(await messagesOf('order-followups', 1), [{ order: 7, paid: 30 }])
await followups.stop()
console.log('ok: feed-pattern')

// guide:start(feed-join)
const orderStatus = await Stream
  .fromFeed(queen, { queues: ['orders', 'payments', 'shipments'] })
  .join({
    sides: { order: 'orders', paid: 'payments', sent: 'shipments' },
    within: '2d',
    select: (got, order) => ({ order, carrier: got.sent.data.carrier }),
    onTimeout: (got, missing, order) => ({ order, missing })
  })
  .to(queen.queue('order-status'))
  .run({ queryId: 'order-status' })
// guide:end
await push('orders', 'order-9', { id: 9 })
await push('payments', 'order-9', { amount: 5 })
await push('shipments', 'order-9', { carrier: 'dhl' })
assert.deepEqual(await messagesOf('order-status', 1), [{ order: 'order-9', carrier: 'dhl' }])
await orderStatus.stop()
console.log('ok: feed-join')

// guide:start(feed-gather)
const batches = await Stream
  .fromFeed(queen, { queues: ['parts'] })
  .gather({
    count: (m) => m.data.of,
    key: (m) => m.data.batch,
    within: '10m',
    onComplete: (got, batch) => ({ batch, parts: got.length }),
    onTimeout: (got, missing, batch) => ({ batch, missing })
  })
  .to(queen.queue('batches'))
  .run({ queryId: 'batches' })
// guide:end
for (let i = 0; i < 3; i++) await push('parts', `part-${i}`, { batch: 'b-1', of: 3 })
assert.deepEqual(await messagesOf('batches', 1), [{ batch: 'b-1', parts: 3 }])
await batches.stop()
console.log('ok: feed-gather')

await push('property-settings', 'p-1', { currency: 'EUR' })
// guide:start(feed-table)
const priced = await Stream
  .fromFeed(queen, { queues: ['bookings'] })
  .joinTable({
    queue: 'property-settings',
    on: (m) => m.data.propertyId,
    select: (booking, settings) => ({ ...booking, currency: settings ? settings.currency : null })
  })
  .to(queen.queue('bookings-priced'))
  .run({ queryId: 'bookings-priced' })
// guide:end
await push('bookings', 'booking-1', { id: 'booking-1', propertyId: 'p-1' })
await push('bookings', 'booking-2', { id: 'booking-2', propertyId: 'p-2' })
assert.deepEqual(await messagesOf('bookings-priced', 2), [
  { id: 'booking-1', propertyId: 'p-1', currency: 'EUR' },   // the row pushed before the stream started
  { id: 'booking-2', propertyId: 'p-2', currency: null }
])
await priced.stop()
console.log('ok: feed-table')

// guide:start(feed-steps)
const bursts = await Stream
  .fromFeed(queen, { queues: ['clicks'] })
  .windowTumbling({ seconds: 60 }).aggregate({ clicks: () => 1 }).name('perMinute')
  .pattern({
    start: (m) => m.data.clicks >= 100,
    then: (m) => m.data.clicks < 100,
    within: '10m',
    onMatch: (burst, calm) => ({ tenant: burst.partition, peak: burst.data.clicks })
  })
  .to(queen.queue('bursts'))
  .run({ queryId: 'bursts' })
// guide:end
for (let i = 0; i < 3; i++) await push('clicks', 'tenant-42', { page: '/' })
await until('three clicks in the minute', () => bursts.state('perMinute').get('tenant-42').value?.windows[0].value.clicks === 3)
console.log('ok: feed-steps')

// guide:start(feed-event-time)
const unanswered = await Stream
  .fromFeed(queen, { queues: ['gh-issues', 'gh-comments'], from: 'earliest' })
  .pattern({
    start: (m) => m.queue === 'gh-issues',
    then: (m) => m.queue === 'gh-comments',
    key: (m) => `${m.partition}#${m.data.number}`,
    within: '1h',
    eventTime: (m) => m.data.created_at,
    allowedLateness: '5m',
    idle: '1m',
    onMatch: () => null,   // an answered issue sends nothing
    onTimeout: (issue) => ({ repo: issue.partition, issue: issue.data.number })
  })
  .to(queen.queue('gh-unanswered'))
  .run({ queryId: 'gh-unanswered' })
// guide:end
{
  const at = Date.now()
  // The comment arrives first; on event time it still answers the issue.
  await push('gh-comments', 'queen-mq/queen', { number: 1, created_at: new Date(at + 60_000).toISOString() })
  await push('gh-issues', 'queen-mq/queen', { number: 1, created_at: new Date(at).toISOString() })
  // Both wait in the step, in event-time order, for allowedLateness.
  await until('the two held messages', () => unanswered.metrics().steps[0].held === 2)
}
await unanswered.stop()
console.log('ok: feed-event-time')

// guide:start(feed-shards)
const perTenant = await Stream
  .fromFeed(queen, { queues: ['clicks'] })
  .keyBy((m) => m.data.tenant)
  .windowTumbling({ seconds: 60 }).aggregate({ clicks: () => 1 })
  .to(queen.queue('clicks-per-tenant'))
  .run({ queryId: 'clicks-per-tenant', shards: 4, shardBy: (m) => m.data.tenant })
// guide:end
assert.equal(perTenant.metrics().running, 4, 'four shards, all in this process')
for (const tenant of ['acme', 'globex', 'acme']) await push('clicks', `visitor-${tenant}`, { tenant })
await until('a window per tenant', () => perTenant.metrics().open === 2)
await perTenant.stop()
console.log('ok: feed-shards')

// guide:start(feed-view)
const view = Stream.view(queen, 'bursts')
const { found, value } = await view.get('perMinute', 'tenant-42')
const { items } = await view.list('perMinute', { limit: 50 })
const shards = await view.status()   // role, position, lag and open state per shard
// guide:end
assert.equal(found, true)
assert.equal(value.windows[0].value.clicks, 3, 'the live window, asked from outside the process')
assert.ok(items.length >= 1)
assert.equal(shards.length, 1)
assert.equal(shards[0].role, 'active')
await bursts.stop()
console.log('ok: feed-view')

await queen.close()
console.log('\nPASS')
