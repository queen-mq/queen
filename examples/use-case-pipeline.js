/**
 * Use case: Multi-stage pipeline with retries, DLQ, and transactional handoffs.
 *
 * A 3-stage chat-style pipeline:
 *
 *   producer ──▶ uc-pipeline.translate ──▶ uc-pipeline.route ──▶ uc-pipeline.notify
 *
 * Each stage:
 *   - pops from its source queue
 *   - does work (here: a transformation, occasionally fails on purpose)
 *   - on success: queen.transaction().ack(input).queue(next).push(output).commit()
 *     — one PG transaction. Ack and next-stage push commit together.
 *   - on failure: ack with status='failed'. Per-queue retry_limit + DLQ kick in.
 *
 * The translate stage rejects every 7th message with a TerminalError-style
 * failure so we can verify:
 *   - retry happens up to retry_limit (=2)
 *   - poison messages end up in DLQ
 *   - the rest still flow through, unblocked
 *
 * Pass criteria:
 *   - Translate stage receives N messages.
 *   - Route stage receives N - poison.
 *   - Notify stage receives N - poison.
 *   - DLQ contains exactly the poison messages.
 *
 * Setup:
 *   - localhost:6632  Queen broker
 *
 * Run:
 *   nvm use 22 && node examples/use-case-pipeline.js
 */

import { Queen } from 'queen-mq'

const QUEEN_URL = process.env.QUEEN_URL || 'http://localhost:6632'

const Q_TRANSLATE = 'uc-pipeline.translate'
const Q_ROUTE     = 'uc-pipeline.route'
const Q_NOTIFY    = 'uc-pipeline.notify'

const N         = 100      // total messages
const POISON_EVERY = 7     // every 7th message at translate stage fails terminally
const RETRY_LIMIT  = 2     // retry once, then DLQ
const LEASE_SEC    = 8     // shorter lease for faster retries on failed acks

const q = new Queen({ url: QUEEN_URL })

// ---------- Setup queues ----------

console.log('Setting up demo queues...')
for (const name of [Q_TRANSLATE, Q_ROUTE, Q_NOTIFY]) {
  await q.queue(name).delete()
  await q.queue(name).config({
    leaseTime:                 LEASE_SEC,
    retryLimit:                RETRY_LIMIT,
    deadLetterQueue:           true,
    dlqAfterMaxRetries:        true,
    retentionEnabled:          true,
    retentionSeconds:          3600,
    completedRetentionSeconds: 300
  }).create()
}

// ---------- Producer ----------

console.log(`Pushing ${N} chat messages...`)
const batch = []
for (let i = 0; i < N; i++) {
  batch.push({
    transactionId: `chat-${i}`,
    data: { chatId: `chat-${i}`, idx: i, text: `Hello number ${i}!` }
  })
}
// One push per partition so we exercise per-chat ordering and per-chat
// leases concurrently. Partition by chatId.
const pushedPerPartition = new Map()
for (const item of batch) {
  await q.queue(Q_TRANSLATE).partition(item.data.chatId).push([item])
  pushedPerPartition.set(item.data.chatId, (pushedPerPartition.get(item.data.chatId) || 0) + 1)
}

// ---------- Stage workers ----------

const counters = {
  translateAttempts: 0,
  translateSuccess:  0,
  translateFailures: 0,
  routeSuccess:      0,
  notifySuccess:     0
}

const ac = new AbortController()

const translateWorker = (async () => {
  await q.queue(Q_TRANSLATE)
    .group('translate-worker')
    .batch(1)
    .each()
    .autoAck(false)
    .consume(async (msg) => {
      counters.translateAttempts++
      // Inject failures: every 7th idx fails terminally (will retry then DLQ).
      if (msg.data.idx % POISON_EVERY === 0 && msg.data.idx !== 0) {
        counters.translateFailures++
        throw new Error(`poison message idx=${msg.data.idx}`)
      }
      // Normal path: enrich + transactional handoff to next stage.
      const translated = {
        ...msg.data,
        translatedText: `[en] ${msg.data.text}`,
        translatedAt:   Date.now()
      }
      await q.transaction()
        .ack(msg, 'completed', { consumerGroup: 'translate-worker' })
        .queue(Q_ROUTE).partition(msg.data.chatId).push([{
          transactionId: `${msg.data.chatId}-routed`,
          data:          translated
        }])
        .commit()
      counters.translateSuccess++
    }, { signal: ac.signal })
    .onError(async (msg, err) => {
      // Manual nack so retry/DLQ kicks in.
      await q.ack(msg, false, { group: 'translate-worker', error: err.message })
    })
})()

const routeWorker = (async () => {
  await q.queue(Q_ROUTE)
    .group('route-worker')
    .batch(1)
    .each()
    .autoAck(false)
    .consume(async (msg) => {
      const routed = {
        ...msg.data,
        agentRoute: msg.data.idx % 2 === 0 ? 'human' : 'ai',
        routedAt:   Date.now()
      }
      await q.transaction()
        .ack(msg, 'completed', { consumerGroup: 'route-worker' })
        .queue(Q_NOTIFY).partition(msg.data.chatId).push([{
          transactionId: `${msg.data.chatId}-notified`,
          data:          routed
        }])
        .commit()
      counters.routeSuccess++
    }, { signal: ac.signal })
    .onError(async (msg, err) => {
      await q.ack(msg, false, { group: 'route-worker', error: err.message })
    })
})()

const notifyWorker = (async () => {
  await q.queue(Q_NOTIFY)
    .group('notify-worker')
    .batch(1)
    .each()
    .consume(async (msg) => {
      // Terminal stage: just count.
      counters.notifySuccess++
    }, { signal: ac.signal })
})()

// ---------- Wait for the pipeline to drain ----------

// Expected: translate completes (N - poison), route completes the same, notify the same.
const expectedThrough = N - Math.floor((N - 1) / POISON_EVERY)
const expectedPoison  = Math.floor((N - 1) / POISON_EVERY)
// Wait long enough for: produce + 2 retries × lease_sec × poison messages worst case.
const deadline = Date.now() + (LEASE_SEC * (RETRY_LIMIT + 1) + 25) * 1000

console.log(`Expected: ${expectedThrough} through pipeline, ${expectedPoison} into DLQ.`)
console.log('Draining (this includes retry/DLQ cycles, give it ~30s)...')

while (Date.now() < deadline) {
  if (counters.notifySuccess >= expectedThrough) break
  await new Promise(r => setTimeout(r, 500))
}

// ---------- Inspect DLQ ----------

const dlqRes = await fetch(`${QUEEN_URL}/api/v1/dlq?queue=${encodeURIComponent(Q_TRANSLATE)}&limit=500`)
const dlqJson = await dlqRes.json()
const dlqCount = (dlqJson.messages || []).length

// ---------- Verify ----------

const passNotify = counters.notifySuccess === expectedThrough
const passRoute  = counters.routeSuccess  === expectedThrough
// translate attempts include retries — should be ≥ (success + RETRY_LIMIT+1 × poison)
const minTranslateAttempts = expectedThrough + (RETRY_LIMIT + 1) * expectedPoison
const passTranslate = counters.translateAttempts >= minTranslateAttempts

console.log('')
console.log('======== Multi-stage pipeline result ========')
console.log(`  produced:              ${N}`)
console.log(`  poison messages:       ${expectedPoison}`)
console.log(`  translate attempts:    ${counters.translateAttempts} (≥ ${minTranslateAttempts} expected — incl. retries)`)
console.log(`  translate failures:    ${counters.translateFailures}`)
console.log(`  routed (stage 2):      ${counters.routeSuccess}    (expected ${expectedThrough})`)
console.log(`  notified (stage 3):    ${counters.notifySuccess}    (expected ${expectedThrough})`)
console.log(`  DLQ (uc-pipeline.translate): ${dlqCount}    (expected ${expectedPoison})`)
console.log(`  status: ${passNotify && passRoute && passTranslate ? 'PASS' : 'FAIL'}`)
console.log('=============================================')

ac.abort()
await Promise.allSettled([translateWorker, routeWorker, notifyWorker])
await q.close()

process.exit((passNotify && passRoute && passTranslate) ? 0 : 1)
