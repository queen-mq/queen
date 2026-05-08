/**
 * Example 07 — Rate limiter via .gate() + per-partition lease back-pressure.
 *
 * Demonstrates a production-shaped token-bucket rate limiter built entirely
 * on streams primitives, preserving FIFO order per partition WITHOUT a
 * deferred queue. The "back-pressure" is the broker's per-partition lease:
 * when the gate denies a message, the runner commits a partial ack and
 * skips the lease release, so the un-acked tail returns to the queue when
 * the lease expires — in its original order.
 *
 * Architecture:
 *
 *   producer ──► gate.requests (3 partitions, leaseTime=2s)
 *                      │
 *                      ▼
 *               Stream .gate() token bucket
 *                  capacity=10, refill=5/sec PER TENANT
 *                      │
 *                      ▼
 *                gate.approved (sink)
 *                      │
 *                      ▼
 *                consumer (records arrival order + timestamps)
 *
 * What the test verifies (and prints PASS/FAIL on):
 *   1. CONTEGGIO: every produced message ends up in `gate.approved`.
 *   2. ORDINE FIFO PER TENANT: arrivals in the sink, grouped by tenant,
 *      have strictly increasing seq numbers (no reordering across the
 *      deny → expiry → redeliver loop).
 *   3. RATE: sustained throughput per tenant ≈ refill rate after the
 *      initial burst (within tolerance).
 *   4. ISOLAMENTO PER TENANT: tenants drain in parallel, each at its own
 *      pace, regardless of which one filled the bucket first.
 *
 * Run:
 *   QUEEN_URL=http://localhost:6632 node streams/examples/07-rate-limiter.js
 *
 * Tunable env:
 *   MSGS_PER_TENANT  (default 50)
 *   TENANTS          (default 3)
 *   CAPACITY         (default 10)
 *   REFILL_PER_SEC   (default 5)
 *   LEASE_SEC        (default 2)
 *   BATCH            (default 10)
 *   TIMEOUT_MS       (default 60000)
 */

import { Queen, Stream } from 'queen-mq'
const url             = process.env.QUEEN_URL || 'http://localhost:6632'
const MSGS_PER_TENANT = parseInt(process.env.MSGS_PER_TENANT || '50', 10)
const TENANTS         = parseInt(process.env.TENANTS         || '3',  10)
const CAPACITY        = parseInt(process.env.CAPACITY        || '10', 10)
const REFILL_PER_SEC  = parseFloat(process.env.REFILL_PER_SEC || '5')
const LEASE_SEC       = parseInt(process.env.LEASE_SEC       || '2',  10)
const BATCH           = parseInt(process.env.BATCH           || '10', 10)
const TIMEOUT_MS      = parseInt(process.env.TIMEOUT_MS      || '60000', 10)

const tag        = Date.now().toString(36)
const Q_REQUESTS = `rl_requests_${tag}`
const Q_APPROVED = `rl_approved_${tag}`

const TOTAL_MSGS = MSGS_PER_TENANT * TENANTS

const expectedDurationSec = Math.max(
  ((MSGS_PER_TENANT - CAPACITY) / REFILL_PER_SEC) + 1,
  1
)

console.log(`[rate-limiter] Queen     = ${url}`)
console.log(`[rate-limiter] tag       = ${tag}`)
console.log(`[rate-limiter] tenants   = ${TENANTS}, msgs/tenant = ${MSGS_PER_TENANT} (total ${TOTAL_MSGS})`)
console.log(`[rate-limiter] bucket    = capacity ${CAPACITY}, refill ${REFILL_PER_SEC}/sec, leaseTime ${LEASE_SEC}s`)
console.log(`[rate-limiter] expected  ≈ ${expectedDurationSec.toFixed(1)}s per tenant after the initial burst`)
console.log()

const q = new Queen({ url, handleSignals: false })

// ----------------------------------------------------------------- 1. Setup queues

console.log('[rate-limiter] creating queues...')
await q.queue(Q_REQUESTS).config({
  // Short lease ⇒ fast back-off after a DENY. With leaseTime=2s and
  // refill=5/sec, the bucket has ~10 fresh tokens by the time the lease
  // expires, which lines up nicely with capacity=10.
  leaseTime: LEASE_SEC,
  retryLimit: 100,                  // denies are not failures; allow many redeliveries
  retentionEnabled: true,
  retentionSeconds: 3600,
  completedRetentionSeconds: 3600
}).create()

await q.queue(Q_APPROVED).config({
  leaseTime: 30,
  retentionEnabled: true,
  retentionSeconds: 3600,
  completedRetentionSeconds: 3600
}).create()

// ----------------------------------------------------------------- 2. Producer: burst push

console.log(`[rate-limiter] burst-pushing ${TOTAL_MSGS} requests across ${TENANTS} partitions...`)
const pushStartAt = Date.now()
const tenants = Array.from({ length: TENANTS }, (_, i) => `tenant-${i}`)

for (const tenantId of tenants) {
  // Push all the tenant's messages in one shot so we get a real burst.
  // Sequence numbers are strict per-tenant so we can later assert order.
  const items = Array.from({ length: MSGS_PER_TENANT }, (_, seq) => ({
    data: {
      tenantId,
      seq,
      pushedAt: Date.now()
    }
  }))
  await q.queue(Q_REQUESTS).partition(tenantId).push(items)
}
console.log(`[rate-limiter] push done in ${Date.now() - pushStartAt}ms`)

// ----------------------------------------------------------------- 3. Stream with .gate()

console.log('[rate-limiter] starting gate stream...')

const stream = await Stream
  .from(q.queue(Q_REQUESTS))
  .gate((req, ctx) => {
    // Per-tenant token bucket. State lives in queen_streams.state under
    // (query_id, partition_id, key) where key defaults to partition_id —
    // i.e., one bucket per tenant. Source-queue partition == limit key,
    // which is the "right" way to use .gate() (no cross-partition contention).
    const cfg = { capacity: CAPACITY, refillPerSec: REFILL_PER_SEC }
    const now = ctx.streamTimeMs

    if (typeof ctx.state.tokens !== 'number') ctx.state.tokens = cfg.capacity
    if (typeof ctx.state.lastRefillAt !== 'number') ctx.state.lastRefillAt = now

    const elapsedSec = Math.max(0, (now - ctx.state.lastRefillAt) / 1000)
    ctx.state.tokens = Math.min(
      cfg.capacity,
      ctx.state.tokens + elapsedSec * cfg.refillPerSec
    )
    ctx.state.lastRefillAt = now

    if (ctx.state.tokens >= 1) {
      ctx.state.tokens -= 1
      // Stash a small audit trail so we can see decisions in psql:
      //   SELECT key, value FROM queen_streams.state
      //   WHERE query_id = (SELECT id FROM queen_streams.queries WHERE name='rate-limiter.<tag>');
      ctx.state.totalAllowed = (ctx.state.totalAllowed || 0) + 1
      return true
    }
    ctx.state.totalDeniedSeen = (ctx.state.totalDeniedSeen || 0) + 1
    return false
  })
  .to(q.queue(Q_APPROVED))
  .run({
    queryId:        `rate-limiter-${tag}`,
    url,
    batchSize:      BATCH,
    maxPartitions:  TENANTS,         // one slot per tenant for parallel draining
    maxWaitMillis:  500,
    reset:          true
  })

console.log('[rate-limiter] stream up')

// ----------------------------------------------------------------- 4. Sink consumer

const arrivalsByTenant = new Map(tenants.map(t => [t, []]))

let drained = 0
let drainStartAt = Date.now()
let drainStop = false

async function drain() {
  const cg = `rl-consumer-${tag}`
  while (!drainStop) {
    try {
      const batch = await q.queue(Q_APPROVED)
        .group(cg)
        .batch(50)
        .wait(true)
        .timeoutMillis(500)
        .pop()
      if (!batch || batch.length === 0) continue
      const arrivedAt = Date.now()
      for (const m of batch) {
        const { tenantId, seq } = m.data
        arrivalsByTenant.get(tenantId).push({ seq, arrivedAt })
        drained++
      }
      await q.ack(batch, true, { group: cg })
    } catch (err) {
      if (!drainStop) console.error(`[consumer] error: ${err.message}`)
    }
  }
}
const drainPromise = drain()

// ----------------------------------------------------------------- 5. Reporter

let lastReportAt = Date.now()
let lastReportDrained = 0
const reporter = setInterval(() => {
  const now = Date.now()
  const dtSec = (now - lastReportAt) / 1000
  const rate = dtSec > 0 ? (drained - lastReportDrained) / dtSec : 0
  const m = stream.metrics()
  console.log(
    `[T+${((now - drainStartAt) / 1000).toFixed(1)}s] ` +
    `drained=${drained}/${TOTAL_MSGS} ` +
    `rate=${rate.toFixed(1)} msg/sec  ` +
    `gate allows=${m.gateAllowsTotal || 0} denies=${m.gateDenialsTotal || 0} ` +
    `cycles=${m.cyclesTotal} stateOps=${m.stateOpsTotal}`
  )
  lastReportAt = now
  lastReportDrained = drained
}, 2000)

// ----------------------------------------------------------------- 6. Wait for completion or timeout

const overallStartAt = Date.now()
while (drained < TOTAL_MSGS && (Date.now() - overallStartAt) < TIMEOUT_MS) {
  await new Promise(r => setTimeout(r, 200))
}

clearInterval(reporter)
drainStop = true
await stream.stop()
await drainPromise

const overallElapsedSec = (Date.now() - overallStartAt) / 1000

// ----------------------------------------------------------------- 7. Verification

console.log('\n' + '='.repeat(80))
console.log('RATE LIMITER VERIFICATION')
console.log('='.repeat(80))

let allPassed = true
const fail = (msg) => { allPassed = false; console.log(`  ❌ ${msg}`) }
const pass = (msg) => { console.log(`  ✅ ${msg}`) }

// 1. Conteggio: tutti arrivati
console.log('\n[1] Conteggio totale')
if (drained === TOTAL_MSGS) {
  pass(`${drained}/${TOTAL_MSGS} messages drained`)
} else {
  fail(`${drained}/${TOTAL_MSGS} drained (${TOTAL_MSGS - drained} missing — likely test timeout)`)
}

// 2. Ordine FIFO per tenant
console.log('\n[2] Ordine FIFO per tenant (i seq devono essere strettamente crescenti)')
for (const tenantId of tenants) {
  const arr = arrivalsByTenant.get(tenantId)
  let inOrder = true
  for (let i = 1; i < arr.length; i++) {
    if (arr[i].seq <= arr[i - 1].seq) { inOrder = false; break }
  }
  if (arr.length === 0) {
    fail(`${tenantId}: no arrivals`)
  } else if (!inOrder) {
    const seqs = arr.map(a => a.seq)
    fail(`${tenantId}: out-of-order arrivals; first 20 seqs = ${seqs.slice(0, 20).join(',')}`)
  } else if (arr.length !== MSGS_PER_TENANT) {
    fail(`${tenantId}: ${arr.length}/${MSGS_PER_TENANT} arrived but in order`)
  } else {
    pass(`${tenantId}: all ${MSGS_PER_TENANT} arrived in order [0..${MSGS_PER_TENANT - 1}]`)
  }
}

// 3. Rate: dopo il burst iniziale (CAPACITY messaggi quasi istantanei),
//    il rate sostenuto deve essere ≈ REFILL_PER_SEC. Tolleranza 50% (il
//    test ha quantizzazione da leaseTime).
console.log(`\n[3] Rate sostenuto post-burst (atteso ≈ ${REFILL_PER_SEC}/sec per tenant)`)
const tolerance = 0.5
const minAcceptable = REFILL_PER_SEC * (1 - tolerance)
const maxAcceptable = REFILL_PER_SEC * (1 + tolerance)

for (const tenantId of tenants) {
  const arr = arrivalsByTenant.get(tenantId)
  if (arr.length <= CAPACITY + 1) {
    fail(`${tenantId}: too few arrivals (${arr.length}) to measure sustained rate`)
    continue
  }
  // Skip the initial burst (CAPACITY arrivals); measure rate over the rest.
  const postBurst = arr.slice(CAPACITY)
  const dtSec = (postBurst[postBurst.length - 1].arrivedAt - postBurst[0].arrivedAt) / 1000
  const sustainedRate = dtSec > 0 ? (postBurst.length - 1) / dtSec : Infinity
  const ok = sustainedRate >= minAcceptable && sustainedRate <= maxAcceptable
  const human = `${sustainedRate.toFixed(2)} msg/sec`
  if (ok) {
    pass(`${tenantId}: sustained ${human} (within [${minAcceptable.toFixed(1)}, ${maxAcceptable.toFixed(1)}])`)
  } else {
    fail(`${tenantId}: sustained ${human}, OUTSIDE [${minAcceptable.toFixed(1)}, ${maxAcceptable.toFixed(1)}]`)
  }
}

// 4. Isolamento: i tenant drenano in parallelo. Verifico che il tempo di
//    completamento dei diversi tenant sia simile (entro ±2s).
console.log('\n[4] Isolamento per tenant (drain in parallelo)')
const tenantDurations = tenants.map(tenantId => {
  const arr = arrivalsByTenant.get(tenantId)
  if (arr.length === 0) return { tenantId, durationSec: NaN }
  const startMs = arr[0].arrivedAt
  const endMs = arr[arr.length - 1].arrivedAt
  return { tenantId, durationSec: (endMs - startMs) / 1000 }
})
const finite = tenantDurations.filter(t => Number.isFinite(t.durationSec))
if (finite.length === tenants.length) {
  const minD = Math.min(...finite.map(t => t.durationSec))
  const maxD = Math.max(...finite.map(t => t.durationSec))
  const spread = maxD - minD
  for (const { tenantId, durationSec } of finite) {
    console.log(`     ${tenantId}: drained over ${durationSec.toFixed(2)}s`)
  }
  if (spread <= 2.5) {
    pass(`tenant durations spread = ${spread.toFixed(2)}s ≤ 2.5s ⇒ they ran in parallel`)
  } else {
    fail(`tenant durations spread = ${spread.toFixed(2)}s > 2.5s ⇒ likely serial draining`)
  }
} else {
  fail(`could not measure all tenant durations (some had 0 arrivals)`)
}

// 5. Stream metrics summary
console.log('\n[5] Stream metrics')
const finalMetrics = stream.metrics()
console.log(`     gate ALLOWS:    ${finalMetrics.gateAllowsTotal || 0}`)
console.log(`     gate DENIES:    ${finalMetrics.gateDenialsTotal || 0}`)
console.log(`     cycles total:   ${finalMetrics.cyclesTotal}`)
console.log(`     state ops:      ${finalMetrics.stateOpsTotal}`)
console.log(`     overall time:   ${overallElapsedSec.toFixed(2)}s`)

console.log('\n' + '='.repeat(80))
if (allPassed) {
  console.log('✅ ALL CHECKS PASSED — rate limiter works as designed')
  console.log('='.repeat(80))
  console.log(`\nQueues left in PG for inspection:`)
  console.log(`  - ${Q_REQUESTS}  (source, should be empty / fully consumed)`)
  console.log(`  - ${Q_APPROVED}  (sink)`)
  console.log(`Stream state in queen_streams.state where query name = rate-limiter-${tag}`)
  process.exit(0)
} else {
  console.log('❌ ONE OR MORE CHECKS FAILED — see above')
  console.log('='.repeat(80))
  process.exit(1)
}
