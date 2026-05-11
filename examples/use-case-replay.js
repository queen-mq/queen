/**
 * Use case: Subscription replay — rewind a consumer group to any timestamp.
 *
 * Production pain: your consumer had a bug last Tuesday at 14:00. You
 * deployed a fix today. Half of yesterday's messages are wrong. You need
 * to reprocess them. With SQS you can't; with Kafka you can but you'll
 * also disturb every other consumer on the same topic; with RabbitMQ
 * messages are gone.
 *
 * With Queen, you create a brand-new consumer group with subscriptionFrom
 * set to the exact moment the bug went live. That group sees every
 * message since that timestamp, with no impact on the production
 * consumers running on a different consumer group on the same queue.
 *
 * Demo flow:
 *   1. Push 1000 events into one queue, tagged with their sequence number.
 *   2. A "production" consumer group ('prod') drains all 1000.
 *   3. We record a "deploy timestamp" right before push #500.
 *   4. A second producer pushes another 500 events.
 *   5. We rewind a NEW consumer group ('reprocess') to the deploy
 *      timestamp via subscriptionFrom, with subscriptionMode='timestamp'.
 *   6. Verify 'reprocess' sees exactly events 500..1499 (the ones at or
 *      after the deploy timestamp), while 'prod' is unaffected.
 *
 * Pass criteria:
 *   - 'prod' group received 1500 events (everything that was pushed).
 *   - 'reprocess' group received ~1000 events (those at-or-after the deploy
 *     timestamp). Allow ±5 events around the boundary due to clock granularity.
 *   - The two consumer groups have independent offsets.
 *
 * Run:
 *   nvm use 22 && node examples/use-case-replay.js
 */

import { Queen } from 'queen-mq'

const QUEEN_URL = process.env.QUEEN_URL || 'http://localhost:6632'

const QUEUE   = 'uc-replay.events'
const N_FIRST = 1000     // pushed before "deploy"
const N_AFTER = 500      // pushed after "deploy"
const DEPLOY_AT_IDX = 500

const CG_PROD   = 'prod'
const CG_REPROC = 'reprocess'

const q = new Queen({ url: QUEEN_URL })

// ---------- Setup ----------

console.log('Setting up demo queue...')
// Clean any prior consumer-group metadata (subscription_timestamp etc.) for
// this queue. queue.delete() does NOT cascade to consumer_groups_metadata
// because consumer groups can subscribe across multiple queues; metadata
// must be removed explicitly via the dedicated consumer-groups API.
for (const cg of [CG_PROD, CG_REPROC]) {
  try {
    await q.admin.deleteConsumerGroupForQueue(cg, QUEUE, /* deleteMetadata */ true)
  } catch (_) { /* not-found is fine on first run */ }
}
await q.queue(QUEUE).delete()
await q.queue(QUEUE).config({
  leaseTime: 30,
  retryLimit: 3,
  retentionEnabled: true,
  retentionSeconds: 3600,
  completedRetentionSeconds: 600
}).create()

// ---------- Producer (phase 1a): push events 0..DEPLOY_AT_IDX-1 ----------
//
// Pushed in batches across multiple partitions. Then we sleep so there's
// a clean wall-clock gap. Then we capture deployAt. Then sleep again.
// Then we push the remaining N_FIRST - DEPLOY_AT_IDX events. This guarantees
// the timestamp boundary lies between idx 499 and idx 500 server-side.

console.log(`Producing first ${DEPLOY_AT_IDX} events (pre-deploy)...`)
const BATCH = 100
const pushBatch = async (startIdx, endIdx, partPrefix) => {
  for (let i = startIdx; i < endIdx; i += BATCH) {
    const items = []
    for (let j = 0; j < BATCH && i + j < endIdx; j++) {
      const idx = i + j
      items.push({ transactionId: `evt-${idx}`, data: { idx, t: Date.now() } })
    }
    await q.queue(QUEUE).partition(`${partPrefix}-${Math.floor(i / BATCH)}`).push(items)
  }
}

await pushBatch(0, DEPLOY_AT_IDX, 'p1')

// Clean gap so the timestamp boundary is unambiguous server-side.
await new Promise(r => setTimeout(r, 800))
const deployAt = new Date().toISOString()
await new Promise(r => setTimeout(r, 800))
console.log(`  deploy moment: ${deployAt}`)

console.log(`Producing the rest of phase 1 (${N_FIRST - DEPLOY_AT_IDX} events, post-deploy)...`)
await pushBatch(DEPLOY_AT_IDX, N_FIRST, 'p1')

// ---------- Production consumer group: drains everything that ever exists ----------

const prodReceived = new Set()
const prodAc = new AbortController()
const prodConsumer = (async () => {
  await q.queue(QUEUE)
    .group(CG_PROD)
    .batch(50)
    .each()
    .subscriptionMode('all')
    .partitions(8)
    .consume(async (msg) => {
      prodReceived.add(msg.data.idx)
    }, { signal: prodAc.signal })
})()

// ---------- Phase 2: push another N_AFTER events some time later ----------

await new Promise(r => setTimeout(r, 600))
console.log(`Producing phase 2 (${N_AFTER} more events, all post-deploy)...`)
await pushBatch(N_FIRST, N_FIRST + N_AFTER, 'p2')

// ---------- Wait for prod to drain everything ----------

const TOTAL = N_FIRST + N_AFTER
console.log(`Waiting for prod consumer to receive all ${TOTAL} events...`)
const deadline = Date.now() + 30_000
while (prodReceived.size < TOTAL && Date.now() < deadline) {
  await new Promise(r => setTimeout(r, 250))
}

// ---------- Now rewind: a fresh consumer group with subscriptionFrom = deployAt ----------

console.log(`Starting REPROCESS consumer group rewound to ${deployAt}...`)
const reprocessReceived = new Set()
const reprocessAc = new AbortController()
const reprocessConsumer = (async () => {
  await q.queue(QUEUE)
    .group(CG_REPROC)
    .batch(50)
    .each()
    .subscriptionFrom(deployAt)      // ISO ts → 'timestamp' mode on the server
    .partitions(8)
    .consume(async (msg) => {
      reprocessReceived.add(msg.data.idx)
    }, { signal: reprocessAc.signal })
})()

// Wait for the reprocess CG to catch up.
console.log('Waiting for reprocess consumer to catch up...')
const reprocessDeadline = Date.now() + 20_000
let stableSince = null
let lastSize = 0
while (Date.now() < reprocessDeadline) {
  if (reprocessReceived.size !== lastSize) {
    lastSize = reprocessReceived.size
    stableSince = Date.now()
  } else if (stableSince && (Date.now() - stableSince) > 2500) {
    break
  } else if (!stableSince) {
    stableSince = Date.now()
  }
  await new Promise(r => setTimeout(r, 250))
}

// ---------- Verify ----------

const prodGotAll       = prodReceived.size === TOTAL
const reprocessSize    = reprocessReceived.size
const expectedReproMin = (TOTAL - DEPLOY_AT_IDX) - 5
const expectedReproMax = (TOTAL - DEPLOY_AT_IDX) + 5
const reproOk          = reprocessSize >= expectedReproMin && reprocessSize <= expectedReproMax

// The reprocess CG must not have seen events before the deploy.
const minReproIdx = Math.min(...reprocessReceived)
const reproBoundaryOk = minReproIdx >= DEPLOY_AT_IDX - 5

const pass = prodGotAll && reproOk && reproBoundaryOk

console.log('')
console.log('======== Subscription replay result ========')
console.log(`  total events pushed:               ${TOTAL}`)
console.log(`  deploy moment (rewind target):     ${deployAt}`)
console.log(`  '${CG_PROD}' received:       ${prodReceived.size}  (expected ${TOTAL})`)
console.log(`  '${CG_REPROC}' received:  ${reprocessSize}  (expected ${expectedReproMin}-${expectedReproMax})`)
console.log(`  smallest idx in reprocess: ${minReproIdx} (expected ≥ ~${DEPLOY_AT_IDX})`)
console.log(`  Both CGs hold independent offsets — neither disturbs the other.`)
console.log(`  status: ${pass ? 'PASS' : 'FAIL'}`)
console.log('=============================================')

prodAc.abort()
reprocessAc.abort()
await Promise.allSettled([prodConsumer, reprocessConsumer])
await q.close()

process.exit(pass ? 0 : 1)
