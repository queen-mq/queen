// docs:start(app-js-deferred-work)
//
// A quota that moves work to the next window instead of refusing it.
//
// A limiter that answers 429 above its quota pushes the problem back to the
// caller, and most callers retry, so refusals come back fastest when the
// service is busiest. For work somebody asked for, a report or an export, the
// caller wants the result and does not much mind when it is ready. This
// admission controller never refuses: it asks the counter for room, and when
// there is none it puts the same request on a timer that fires when the window
// opens again.
//
// Two primitives do it. incr with a max is the admission decision: the
// increment applies, or nothing is written and the answer says
// applied: false, reason: 'limit', so a refused request spends no budget. A
// timer carries the request forward: one message, scheduled now, delivered
// when the window opens, and cancellable by name until then.
//
//   requests (one partition per tenant)
//     └── group "admission"   ONE transaction: incr + gate + push + ack
//           ├── work           admitted now
//           └── a timer back onto `requests`, for the next window
//
// Run it:
//   QUEEN_URL=http://localhost:6632 node deferred-work.mjs

import { Queen } from 'queen-mq'

const QUEEN_URL = process.env.QUEEN_URL || 'http://localhost:6632'
const RUN = Date.now().toString(36)

const REQUESTS = `app-js-deferred-requests-${RUN}`
const WORK = `app-js-deferred-work-${RUN}`
const NS = `app-js-deferred-${RUN}`

// Three exports per tenant per window. The window is eight seconds here and an
// hour in a real service; nothing else in the program changes with it.
const QUOTA = 3
const WINDOW_S = 8

// Timers fire on the leader's next tick after they are due, so waiting for a
// deferred request needs the window plus a margin.
const TIMER_DEADLINE_MS = (WINDOW_S + 20) * 1000

let checks = 0
const assert = (condition, description) => {
  if (!condition) throw new Error(description)
  checks++
  console.log(`  ok: ${description}`)
}

const quotaKey = (tenant) => `quota:${tenant}`
const admittedKey = (requestId) => `admitted:${requestId}`
const timerKey = (requestId) => `req:${requestId}`

const admitted = []
const deferred = []
const alreadyAdmitted = []

const queen = new Queen({ url: QUEEN_URL, handleSignals: false })

// One partition per tenant, so a tenant's requests are decided in the order
// they were made.
let submitted = 0
const submit = (tenant, requestId) =>
  queen.queue(REQUESTS).partition(tenant).push({
    transactionId: `submit-${submitted++}-${requestId}`,
    data: { tenant, requestId },
  })

try {
  console.log(`broker ${QUEEN_URL}`)

  for (const q of [REQUESTS, WORK]) {
    await queen.queue(q).config({ leaseTime: 30, retryLimit: 3 }).create()
  }

  // --------------------------------------------------------------- admission
  //
  // One transaction decides, records and dispatches a request. The counter is
  // incremented BEFORE the duplicate check, so a redelivered request that was
  // already admitted spends budget on its way to being refused. It gets the
  // budget back because both are in one transaction: the lost gate rolls the
  // increment back with everything else.
  const decide = async (msg) => {
    const { tenant, requestId } = msg.data

    const res = await queen
      .transaction()
      .kv.incr(NS, quotaKey(tenant), 1, { max: QUOTA, ttl: `${WINDOW_S}s`, required: true })
      .kv.putIfAbsent(NS, admittedKey(requestId), { tenant }, { ttl: '1h', required: true })
      // No transactionId: the push commits only with the admitted:<id> entry,
      // so that entry is its idempotency key.
      .queue(WORK).partition(tenant).push({ data: { tenant, requestId } })
      .ack(msg, 'completed', { consumerGroup: msg.consumerGroup })
      .commit()

    if (res.success !== false) {
      admitted.push({ tenant, requestId })
      console.log(`  ${requestId}: admitted`)
      return
    }

    // A refused gate comes back as a value (HTTP 200, success: false, reason
    // 'kv_precondition'), and kvReason says which gate refused.
    if (res.kvReason === 'exists') {
      // A request that was admitted before. Nothing was written, the increment
      // included.
      alreadyAdmitted.push(requestId)
      await queen.ack(msg, 'completed', { group: msg.consumerGroup })
      console.log(`  ${requestId}: already admitted, nothing written`)
      return
    }
    if (res.kvReason !== 'limit') throw new Error(`${requestId}: unexpected verdict ${res.kvReason}`)

    // Over the quota. The counter's expiry is the window boundary: incr sets the
    // TTL only when it creates the key, so the counter lives exactly one window,
    // and the wait is read off the entry that refused the request.
    const counter = await queen.kv.get(NS, quotaKey(tenant))
    const delayMs = counter.found ? Math.max(0, new Date(counter.expiresAt).getTime() - Date.now()) : 0

    // The deferral is a transaction too: no timer, no ack, so the request comes
    // back and is decided again.
    const out = await queen
      .transaction()
      .timer(REQUESTS).key(timerKey(requestId)).partition(tenant).delayMs(delayMs)
      .payload({ tenant, requestId }).schedule()
      .ack(msg, 'completed', { consumerGroup: msg.consumerGroup })
      .commit()
    if (out.success === false) throw new Error(`${requestId}: deferral failed (${out.reason})`)

    deferred.push(requestId)
    console.log(`  ${requestId}: over quota, moved to the next window in ${delayMs} ms`)
  }

  // Each phase below ends after `limit` decisions, with the idle bound as the
  // deadline behind it.
  const admission = (limit, idleMillis) => queen
    .queue(REQUESTS)
    .group('admission')
    .subscriptionMode('all')
    .autoAck(false)
    .each()
    .limit(limit)
    .timeoutMillis(1000)
    .idleMillis(idleMillis)
    .consume(decide)

  // --------------------------------------------------- a duplicate request
  console.log('\na duplicate request, and the budget it must not spend')
  await submit('globex', 'G-1')
  await admission(1, 20_000)
  const afterFirst = await queen.kv.get(NS, quotaKey('globex'))
  console.log(`  globex counter: ${afterFirst.value}`)

  // The same request again, as a new message: what a redelivery looks like
  // from the admission controller's side.
  await submit('globex', 'G-1')
  await admission(1, 20_000)
  const afterDuplicate = await queen.kv.get(NS, quotaKey('globex'))
  console.log(`  globex counter: ${afterDuplicate.value}`)

  assert(alreadyAdmitted.join(',') === 'G-1', 'the duplicate lost the gate')
  assert(admitted.length === 1, 'the duplicate produced no second export')
  assert(
    afterDuplicate.value === afterFirst.value,
    `the rolled-back transaction gave the budget back (counter still ${afterDuplicate.value})`
  )

  // ------------------------------------------------------ over the quota
  console.log(`\nsix requests against a quota of ${QUOTA}`)
  const ACME = ['R-1', 'R-2', 'R-3', 'R-4', 'R-5', 'R-6']
  for (const requestId of ACME) await submit('acme', requestId)
  await admission(ACME.length, 20_000)

  const acmeAdmitted = () => admitted.filter(a => a.tenant === 'acme')
  assert(acmeAdmitted().length === QUOTA, `${QUOTA} of acme's requests ran now and none was refused`)
  assert(deferred.length === ACME.length - QUOTA, `the other ${ACME.length - QUOTA} were moved to the next window`)

  // ---------------------------------------------------- withdrawing one
  //
  // A deferred request can be inspected and called off by name while it waits.
  console.log('\nwithdrawing one deferred request')
  const withdrawn = deferred[deferred.length - 1]
  const cancelled = await queen.timer(REQUESTS).key(timerKey(withdrawn)).cancel()
  assert(cancelled.status === 'cancelled', `${withdrawn} was cancelled while it waited`)

  // ------------------------------------------------------ the next window
  //
  // The timers deliver the requests back onto the same queue, and the same
  // admission controller decides them again. A request coming back is nothing
  // special, which is what keeps the loop safe.
  console.log('\nthe next window')
  await admission(deferred.length - 1, TIMER_DEADLINE_MS)
  // A short pass with room for the withdrawn request, to show it never returns.
  await admission(1, 4000)

  // --------------------------------------------------------------- the work
  console.log('\nthe work that ran')
  const ran = []
  await queen
    .queue(WORK)
    .group('exporter')
    .subscriptionMode('all')
    .each()
    .timeoutMillis(1000)
    .idleMillis(3000)
    .consume(async (msg) => {
      ran.push(msg.data.requestId)
      console.log(`  exported ${msg.data.requestId}`)
    })

  // ----------------------------------------------------------------- checking
  console.log('\nchecking')
  const expected = ['G-1', ...ACME.filter(id => id !== withdrawn)].sort()
  assert(
    JSON.stringify([...ran].sort()) === JSON.stringify(expected),
    `exactly the requests not withdrawn ran, once each (${[...ran].sort().join(', ')})`
  )
  assert(
    JSON.stringify(admitted.map(a => a.requestId).sort()) === JSON.stringify(expected),
    'every export that ran was admitted by the counter'
  )
  assert(alreadyAdmitted.length === 1, 'the gate refused only the one duplicate, never a returning request')
  const left = await queen.timer(REQUESTS).list({ limit: 50 })
  assert(left.rows.length === 0, 'no timer is left pending')
  console.log(`\n  ran: ${ran.join(', ')}; withdrawn: ${withdrawn}`)

  for (const q of [REQUESTS, WORK]) await queen.queue(q).delete()
  console.log(`\nPASS: ${checks} checks`)
} catch (err) {
  console.error(`\nFAIL: ${err.message}`)
  process.exitCode = 1
} finally {
  await queen.close()
}
// docs:end
