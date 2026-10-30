// The little book of QueenMQ: the use cases of chapter 3 ("Real applications examples").
//
// The book shows the regions between `guide:start(<id>)` and `guide:end`,
// verbatim, minus lines ending in `guide:hide`. Everything outside them gives
// the program a clean start and checks that each use case does what the book
// says it does.
//
// Run it against a node:
//   docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.7.0
//   node book-use-cases.mjs
// run.sh runs a copy with localhost:6632 replaced by $QUEEN_URL.

import assert from 'node:assert/strict'
import http from 'node:http'
import { Queen, Stream, tokenBucketGate } from 'queen-mq'

const url = 'http://localhost:6632'
const queen = new Queen({ url, handleSignals: false })
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const QUEUES = ['thumbnails', 'order-placed', 'chat', 'order-events', 'shipping', 'trace-events',
  'traces', 'sms-out', 'availability', 'review-reminders', 'webhooks', 'daily-reports']
for (const q of QUEUES) {
  try { await queen.queue(q).delete() } catch {}
}
for (const [ns, key] of [['orders', 'order-2001']]) {
  try { await queen.kv.delete(ns, key) } catch {}
}

// --- Work queue: competing consumers ---------------------------------------
const done = []
const resize = async (photo, worker) => { done.push({ photo, worker }); await sleep(50) }
// guide:start(uc-work)
// Twelve photos to resize, from four users.
for (let i = 1; i <= 12; i++) {
  await queen.queue('thumbnails').partition(`user-${i % 4}`)
    .push([{ data: { photo: `p${i}.jpg` } }])
}

// Three workers compete for the jobs: each job goes to exactly one of them.
const worker = (name) => queen.queue('thumbnails')
  .partitions(1)                                  // one user's photos at a time
  .each()
  .idleMillis(1500)                               // stop when there is nothing left
  .consume(async (job) => resize(job.data.photo, name))

await Promise.all([worker('w1'), worker('w2'), worker('w3')])
// guide:end
{
  const photos = done.map((d) => d.photo)
  assert.equal(photos.length, 12, 'twelve jobs handled')
  assert.equal(new Set(photos).size, 12, 'each job once')
  const workers = new Set(done.map((d) => d.worker))
  assert.ok(workers.size >= 2, `the jobs were shared (${[...workers]})`)
  console.log('ok: uc-work', JSON.stringify(Object.fromEntries([...workers].map((w) =>
    [w, done.filter((d) => d.worker === w).length]))))
}

// --- Publish/subscribe and fan-out: consumer groups ------------------------
const seen = { billing: [], shipping: [], analytics: [] }
// guide:start(uc-pubsub)
const subscribe = (group) => queen.queue('order-placed')
  .group(group)                                   // each group gets every message
  .subscriptionMode('all')
  .each()
  .idleMillis(1000) // guide:hide
  .consume(async (event) => {
    seen[group].push(event.data.orderId) // guide:hide
    console.log(group, event.data.orderId)
  })

for (const orderId of [3001, 3002, 3003]) {
  await queen.queue('order-placed').partition(`order-${orderId}`)
    .push([{ data: { orderId } }])
}
await Promise.all([subscribe('billing'), subscribe('shipping')])

// Months later, analytics subscribes too, and reads every order ever placed.
await subscribe('analytics')
// guide:end
{
  for (const g of ['billing', 'shipping', 'analytics']) {
    assert.deepEqual([...seen[g]].sort(), [3001, 3002, 3003], `${g} got every order`)
  }
  // A group that does not ask for 'all' starts at the end of the queue.
  const late = await queen.queue('order-placed').group('late').wait(false).batch(10).pop()
  assert.equal(late.length, 0, 'a new group without subscriptionMode all skips the history')
  console.log('ok: uc-pubsub')
}

// --- 1. Chat processing: a burst is one batch (windowBuffer) ---------------
let t0 = 0
let calls = 0
let question = ''
// guide:start(uc-chat)
// A burst waits 2 seconds, then goes to the handler as one batch.
await queen.queue('chat').config({ windowBuffer: 2 }).create()
t0 = Date.now() // guide:hide

// The guest sends three messages in a row.
for (const text of ['Hi!', 'Is breakfast included?', 'And is there parking?']) {
  await queen.queue('chat').partition('conv-42').push([{ data: { text } }])
}

// The assistant answers the whole burst once.
await queen.queue('chat')
  .group('assistant')
  .subscriptionMode('all')
  .batch(10)                          // the handler gets every message of the burst
  .limit(3) // guide:hide
  .consume(async (messages) => {
    calls++ // guide:hide
    question = messages.map((m) => m.data.text).join(' ')
    console.log(question)             // Hi! Is breakfast included? And is there parking?
  })
// guide:end
{
  assert.equal(calls, 1, 'the three messages reached the handler in one call')
  assert.equal(question, 'Hi! Is breakfast included? And is there parking?')
  const waited = Date.now() - t0
  assert.ok(waited >= 1500, `the burst was held for the window (${waited} ms)`)
  console.log(`ok: uc-chat (one call, after ${waited} ms)`)
}

// --- 2. Order workflow: transitions that cannot race (expect) --------------
await queen.kv.put('orders', 'order-2001', { status: 'paid' }, { ttl: '30d' })
await queen.queue('order-events').partition('order-2001')
  .push([{ transactionId: 'order-2001-paid', data: { orderId: 'order-2001' } }])
let verdicts = []
// guide:start(uc-order)
await queen.queue('order-events')
  .group('workflow')
  .subscriptionMode('all')
  .autoAck(false)
  .each()
  .limit(1) // guide:hide
  .consume(async (msg) => {
    const { orderId } = msg.data
    const order = await queen.kv.get('orders', orderId)            // { status: 'paid' }
    if (order.value.status !== 'paid') {                   // already moved: just ack
      return queen.ack(msg, 'completed', { group: 'workflow' })
    }

    // Do the work of this step here: charge the card, book the courier, ...

    const result = await queen.transaction()
      .ack(msg, 'completed', { consumerGroup: 'workflow' })
      .kv.put('orders', orderId, { status: 'shipping' },
        { ttl: '30d', expect: order.version, required: true })  // only from what we read
      .queue('shipping').partition(orderId)
      .push([{ transactionId: `ship-${orderId}`, data: { orderId } }])
      .commit()
    verdicts.push(result.success !== false) // guide:hide
    if (result.success === false) return  // moved by someone else: nothing written
  })
// guide:end
{
  assert.deepEqual(verdicts, [true])
  const order = await queen.kv.get('orders', 'order-2001')
  assert.equal(order.value.status, 'shipping')
  // The same transition from the old version is refused, and writes nothing.
  const stale = await queen.transaction()
    .kv.put('orders', 'order-2001', { status: 'shipping' },
      { ttl: '30d', expect: order.version - 1, required: true })
    .queue('shipping').partition('order-2001')
    .push([{ transactionId: 'ship-again', data: {} }])
    .commit()
  assert.equal(stale.success, false, 'a transition from a stale version rolls back')
  const read = await queen.queue('shipping').fetch(['order-2001'])
  assert.equal(read.get('order-2001').records.length, 1, 'one shipment, not two')
  console.log('ok: uc-order')
}

// --- 3. Trace management: events merged per trace (session window) ---------
let merged = null
// guide:start(uc-trace)
// A trace is complete when it has been quiet for 2 seconds.
const merger = await Stream
  .from(queen.queue('trace-events'))
  .windowSession({ gap: 2, idleFlushMs: 500 })
  .reduce((t, e) => ({ spans: t.spans + 1, ms: t.ms + e.ms }), { spans: 0, ms: 0 })
  .to(queen.queue('traces'))                      // one message per finished trace
  .run({ queryId: 'trace-merger', url, subscriptionMode: 'all' })

// Every SDK call is one event, in the partition of its trace.
const event = (span, ms) => ({ transactionId: `trace-7f3a:${span}`, data: { span, ms } })
await queen.queue('trace-events').partition('trace-7f3a').push([
  event('http', 120), event('db', 45), event('cache', 3),
])
await queen.queue('trace-events').partition('trace-7f3a')
  .push([event('db', 45)])                        // a retried call: stored once
// guide:end
{
  for (let i = 0; i < 40 && !merged; i++) {
    await sleep(250)
    const read = await queen.queue('traces').fetch(['trace-7f3a'])
    merged = read.get('trace-7f3a')?.records?.[0]?.payload ?? null
  }
  await merger.stop()
  assert.ok(merged, 'the finished trace reached the traces queue')
  const value = merged.value ?? merged
  assert.equal(value.spans, 3, 'three spans: the retried event was stored once')
  assert.equal(value.ms, 168)
  console.log('ok: uc-trace', JSON.stringify(merged))
}

// --- 4. Rate limiting: a token bucket gate per customer -------------------
const sent = []
const t4 = Date.now()
const sendSms = async (sms) => { sent.push({ n: sms.n, at: Date.now() - t4 }) }
// guide:start(uc-rate)
// The SMS provider accepts 5 messages a second for each customer.
// A refused message comes back when its lease ends: here after 1 second.
await queen.queue('sms-out').config({ leaseTime: 1 }).create()

const limiter = await Stream
  .from(queen.queue('sms-out'))                          // one partition per customer
  .gate(tokenBucketGate({ capacity: 5, refillPerSec: 5 }))  // 5 at once, then 5 a second
  .foreach(async (sms) => sendSms(sms))                  // only what the gate lets through
  .run({ queryId: 'sms-gate', url, subscriptionMode: 'all' })

// Twelve messages for one customer, all at once.
await queen.queue('sms-out').partition('customer-42')
  .push(Array.from({ length: 12 }, (_, i) => ({ data: { n: i + 1, text: `#${i + 1}` } })))
// guide:end
{
  for (let i = 0; i < 80 && sent.length < 12; i++) await sleep(100)
  await limiter.stop()
  assert.deepEqual(sent.map((x) => x.n), Array.from({ length: 12 }, (_, i) => i + 1),
    'all twelve were sent, in order')
  const first = sent.filter((x) => x.at < sent[0].at + 800).length
  assert.ok(first <= 5, `at most 5 in the first burst (${first})`)
  assert.ok(sent[11].at - sent[0].at >= 1000, `the rest waited (${sent[11].at - sent[0].at} ms)`)
  console.log('ok: uc-rate', JSON.stringify(sent.map((x) => x.at - sent[0].at)))
}

// --- 5. Only the newest value (conflation) ----------------------------------
// guide:start(uc-latest)
// The availability of a hotel changes often; the screen needs only the last value.
for (const rooms of [12, 11, 9]) {
  await queen.queue('availability').partition('hotel-7')
    .push([{ data: { hotel: 7, rooms } }])
}

const [latest] = await queen.queue('availability')
  .group('screen')
  .subscriptionMode('all')
  .conflation()                       // the newest message of each hotel, the rest skipped
  .wait(false)
  .pop()
console.log(latest.data)              // { hotel: 7, rooms: 9 }
await queen.ack(latest, 'completed', { group: 'screen' })
// guide:end
{
  assert.deepEqual(latest.data, { hotel: 7, rooms: 9 })
  const again = await queen.queue('availability').group('screen').wait(false).pop()
  assert.equal(again.length, 0, 'the older values were skipped, not delivered later')
  console.log('ok: uc-latest')
}

// --- 6. Reminders and delayed jobs (timers) ---------------------------------
const statuses = []
// guide:start(uc-reminder)
// When an order is delivered, ask for a review in 24 hours.
await queen.timer('review-reminders').key('review-2001').delay('24h')
  .payload({ orderId: 2001 }).schedule()          // status: 'scheduled'
  .then((r) => statuses.push(r.status)) // guide:hide

// The delivery was late: schedule the same key again, and the reminder moves.
await queen.timer('review-reminders').key('review-2001').delay('48h')
  .payload({ orderId: 2001 }).schedule()          // status: 'rescheduled'
  .then((r) => statuses.push(r.status)) // guide:hide

// The guest wrote a review already: no reminder at all.
await queen.timer('review-reminders').key('review-2001').cancel()
// guide:end
{
  assert.deepEqual(statuses, ['scheduled', 'rescheduled'])
  const peek = await queen.timer('review-reminders').key('review-2001').peek()
  assert.notEqual(peek?.status, 'scheduled', 'the cancelled reminder is gone')
  // A timer fires as a real message of its queue.
  await queen.timer('review-reminders').key('review-2002').delayMs(200).payload({ orderId: 2002 }).schedule()
  let fired = null
  await queen.queue('review-reminders').group('mailer').subscriptionMode('all').each().limit(1)
    .consume(async (m) => { fired = m.data })
  assert.deepEqual(fired, { orderId: 2002 })
  console.log('ok: uc-reminder')
}

// --- 7. Request and reply between services (ephemeral queues) --------------
// guide:start(uc-reply)
// The pricing service answers quote requests, in memory.
async function pricing() {
  const { messages } = await queen.ephemeral.pop('quotes', { group: 'pricing', wait: true })
  for (const m of messages) {
    await queen.ephemeral.push(m.payload.replyTo, [{ room: m.payload.room, price: 129 }])
  }
  await queen.ephemeral.ack('quotes', messages, { group: 'pricing' })
}

// The booking page asks, and waits at most half a second for the answer.
async function quote(room) {
  const inbox = `inbox-${crypto.randomUUID()}`
  await queen.ephemeral.push('quotes', [{ room, replyTo: inbox }])
  const reply = await queen.ephemeral.pop(inbox,
    { wait: true, timeout: 500, commitOnDelivery: true })
  return reply.messages[0]?.payload ?? { room, price: null }    // too slow: show "ask us"
}

const [deluxe] = await Promise.all([quote('deluxe'), pricing()])
console.log(deluxe)                               // { room: 'deluxe', price: 129 }
// guide:end
{
  assert.deepEqual(deluxe, { room: 'deluxe', price: 129 })
  const t = Date.now()
  const suite = await quote('suite')              // nobody answers now
  assert.deepEqual(suite, { room: 'suite', price: null })
  assert.ok(Date.now() - t < 3000, 'the caller gave up after its timeout')
  await queen.ephemeral.pop('quotes', { group: 'pricing', wait: false })
    .then(({ messages }) => queen.ephemeral.ack('quotes', messages, { group: 'pricing' }))
  console.log('ok: uc-reply')
}

// --- 8. Calling a webhook (retries, dead letters, idempotency key) ----------
// A receiver that fails the first call, and keeps each idempotency key once.
const received = new Map()
let callsSeen = 0
const receiver = http.createServer((req, res) => {
  let body = ''
  req.on('data', (c) => { body += c })
  req.on('end', () => {
    callsSeen++
    if (callsSeen === 1) { res.writeHead(500); return res.end() }
    received.set(req.headers['idempotency-key'], body)
    res.writeHead(200); res.end()
  })
})
await new Promise((r) => receiver.listen(0, '127.0.0.1', r))
const hookUrl = `http://127.0.0.1:${receiver.address().port}/hooks`
await queen.queue('webhooks').config({ retryLimit: 5, retryDelay: 1000 }).create()
await queen.queue('webhooks').partition('customer-42').push([{ transactionId: 'evt-9001',
  data: { url: hookUrl, event: { type: 'booking.confirmed', id: 9001 } } }])
// guide:start(uc-webhook)
const stop = new AbortController()
process.once('SIGTERM', () => stop.abort())       // a deploy stops the sender cleanly
const hookWatch = setInterval(() => { if (received.size > 0) stop.abort() }, 50) // guide:hide

await queen.queue('webhooks')
  .group('sender')
  .subscriptionMode('all')
  .each()
  .consume(async (msg) => {
    const res = await fetch(msg.data.url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'idempotency-key': msg.transactionId },
      body: JSON.stringify(msg.data.event),
    })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)  // retried, then dead-lettered
  }, { signal: stop.signal })
// guide:end
{
  clearInterval(hookWatch)
  receiver.close()
  assert.equal(callsSeen, 2, 'the failed call was retried once')
  assert.deepEqual([...received.keys()], ['evt-9001'], 'the receiver got the event under its key')
  console.log('ok: uc-webhook')
}

// --- 9. A job only one worker runs (lock + guard) ---------------------------
async function nightlyWorker(name) {
  // guide:start(uc-singleton)
  const lock = queen.lock('nightly-report', { ttl: '30s' })
  if (!(await lock.acquire({ wait: '1s' }))) return 'skipped'   // another worker has it
  try {
    const report = { day: '2026-10-24', bookings: 412 }
    const result = await queen.transaction()
      .guard(lock)                                  // commits only while the lock is ours
      .queue('daily-reports').push([{ transactionId: `report-${report.day}`, data: report }])
      .commit()
    if (result.success === false) return 'lost the lock'
    await sleep(1500) // guide:hide
    return 'done'
  } finally {
    await lock.release()
  }
  // guide:end
}
{
  const outcomes = await Promise.all([nightlyWorker('a'), nightlyWorker('b'), nightlyWorker('c')])
  assert.equal(outcomes.filter((o) => o === 'done').length, 1, `one worker ran it (${outcomes})`)
  const read = await queen.queue('daily-reports').fetch(['Default'])
  assert.equal(read.get('Default').records.length, 1, 'one report')
  console.log('ok: uc-singleton', JSON.stringify(outcomes))
}

await queen.close?.()
console.log('all use cases checked')
process.exit(0)
