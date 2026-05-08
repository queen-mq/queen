/**
 * Example 08 — Rate limiter STRESS TEST.
 *
 * Realistic channel-manager-shaped workload to validate the .gate() pattern
 * under high concurrency:
 *   - many partitions in parallel (one per "OTA endpoint × tenant")
 *   - large burst pre-loaded into the source queue
 *   - per-key token bucket throttle
 *   - tail-of-batch denies → lease-expiry redelivery
 *
 * Default config (env-overridable):
 *   TENANTS              = 50                # parallel rate-limited keys
 *   MSGS_PER_TENANT      = 2000              # 100k total
 *   CAPACITY             = 100               # token bucket burst
 *   REFILL_PER_SEC       = 100               # per tenant
 *   LEASE_SEC            = 1                 # source queue lease (controls deny→retry latency)
 *   BATCH                = 100               # gate cycle batch size
 *   MAX_PARTITIONS       = TENANTS           # parallel partition slots
 *   TIMEOUT_MS           = 180000            # safety bail-out (3 min)
 *   ORDER_SAMPLE_TENANTS = 5                 # # tenants whose full order we verify
 *
 * IMPORTANT — sizing rule: the maximum sustainable rate per tenant equals
 *   min(REFILL_PER_SEC, CAPACITY / LEASE_SEC)
 * because deny→redeliver round-trips the broker once per LEASE_SEC and each
 * round-trip can drain at most CAPACITY tokens. To match REFILL_PER_SEC,
 * keep CAPACITY ≥ REFILL_PER_SEC × LEASE_SEC (default does exactly that).
 * Also keep BATCH = CAPACITY so a full bucket can be drained in one cycle.
 *
 * Expected back-of-envelope (defaults):
 *   per-tenant drain    = 2000 / 100  = 20s + ~0.5s burst
 *   aggregate throughput = 50 × 100   = 5000 msg/sec sustained
 *   total runtime       ≈ 25s         (push burst + drain)
 *   gate cycles         ≈ 50 tenants × 40 ≈ 2000
 *
 * What we measure (and PASS/FAIL on):
 *   1. ALL messages drained                                        (correctness)
 *   2. Order preserved on a sampled subset of tenants              (correctness)
 *   3. Per-tenant sustained rate ≈ REFILL_PER_SEC                  (rate enforcement)
 *   4. Aggregate sustained throughput ≈ TENANTS × REFILL_PER_SEC   (parallelism)
 *   5. Latency push→sink p50 / p95 / p99                           (observability)
 *   6. No errors, no missing messages                              (robustness)
 *
 * Run:
 *   QUEEN_URL=http://localhost:6632 node streams/examples/08-rate-limiter-stress.js
 *
 * Smaller smoke run:
 *   TENANTS=10 MSGS_PER_TENANT=500 node streams/examples/08-rate-limiter-stress.js
 *
 * Bigger run:
 *   TENANTS=100 MSGS_PER_TENANT=5000 node streams/examples/08-rate-limiter-stress.js
 */

import { Queen, Stream } from 'queen-mq'
// ---------------- config

const url             = process.env.QUEEN_URL || 'http://localhost:6632'
const TENANTS         = parseInt(process.env.TENANTS         || '50',   10)
const MSGS_PER_TENANT = parseInt(process.env.MSGS_PER_TENANT || '2000', 10)
const CAPACITY        = parseInt(process.env.CAPACITY        || '100',  10)
const REFILL_PER_SEC  = parseFloat(process.env.REFILL_PER_SEC || '100')
const LEASE_SEC       = parseInt(process.env.LEASE_SEC       || '1',    10)
const BATCH           = parseInt(process.env.BATCH           || '100',  10)
// How many concurrent Runner instances to spawn. Each runs the same .gate()
// query (same queryId, same state shards in queen_streams.state). Queen's
// per-partition lease guarantees that a given partition is processed by AT
// MOST ONE runner at any instant, so per-key state writes never collide,
// and ordering is preserved within each partition. Throughput scales
// roughly linearly with RUNNERS until the source queue's pop rate or the
// PG cycle rate becomes the bottleneck.
const RUNNERS         = parseInt(process.env.RUNNERS         || '4',    10)
const MAX_PARTITIONS  = parseInt(
  process.env.MAX_PARTITIONS ||
  String(Math.max(1, Math.ceil(TENANTS / RUNNERS))),  // ~equal split
  10
)
const TIMEOUT_MS      = parseInt(process.env.TIMEOUT_MS      || '180000', 10)
const ORDER_SAMPLE_TENANTS = parseInt(process.env.ORDER_SAMPLE_TENANTS || '5', 10)
const PUSH_BATCH      = parseInt(process.env.PUSH_BATCH      || '500', 10)
const PUSH_PARALLEL   = parseInt(process.env.PUSH_PARALLEL   || '8',   10)
const REPORT_EVERY_MS = parseInt(process.env.REPORT_EVERY_MS || '2000', 10)

const TOTAL_MSGS = TENANTS * MSGS_PER_TENANT
const tag        = Date.now().toString(36)
const Q_REQUESTS = `rl_stress_req_${tag}`
const Q_APPROVED = `rl_stress_app_${tag}`

// Real sustainable rate per tenant is capped by both refill and the
// capacity/lease quantum (see header comment). The test asserts against
// this MIN, not the raw refill, so a deliberately under-sized bucket is
// flagged with a clear message.
const effectiveRatePerTenant = Math.min(REFILL_PER_SEC, CAPACITY / LEASE_SEC)
const expectedDrainSec = (MSGS_PER_TENANT - CAPACITY) / effectiveRatePerTenant
const expectedAggregateRate = TENANTS * effectiveRatePerTenant

console.log('='.repeat(80))
console.log('RATE LIMITER STRESS TEST')
console.log('='.repeat(80))
console.log(`Queen URL          = ${url}`)
console.log(`tag                = ${tag}`)
console.log(`tenants            = ${TENANTS}`)
console.log(`msgs/tenant        = ${MSGS_PER_TENANT.toLocaleString()}`)
console.log(`total messages     = ${TOTAL_MSGS.toLocaleString()}`)
console.log(`bucket             = capacity ${CAPACITY}, refill ${REFILL_PER_SEC}/sec, leaseTime ${LEASE_SEC}s`)
console.log(`effective rate     = min(${REFILL_PER_SEC}, ${CAPACITY}/${LEASE_SEC}) = ${effectiveRatePerTenant.toFixed(1)} msg/sec/tenant`)
console.log(`expected drain     ≈ ${expectedDrainSec.toFixed(1)}s per tenant after burst`)
console.log(`expected aggregate ≈ ${expectedAggregateRate.toLocaleString()} msg/sec sustained`)
console.log(`gate batch         = ${BATCH}, max partitions/runner = ${MAX_PARTITIONS}`)
console.log(`runners            = ${RUNNERS} (parallel stream consumers, share state via queryId)`)
console.log(`order sample size  = ${ORDER_SAMPLE_TENANTS} tenants`)
if (CAPACITY < REFILL_PER_SEC * LEASE_SEC) {
  console.log()
  console.log(`⚠  CAPACITY (${CAPACITY}) < REFILL_PER_SEC × LEASE_SEC (${REFILL_PER_SEC * LEASE_SEC})`)
  console.log(`   bucket will be capacity-bound; effective rate clamped to ${effectiveRatePerTenant.toFixed(1)}/sec`)
  console.log(`   set CAPACITY=${REFILL_PER_SEC * LEASE_SEC} (= refill × lease) to hit ${REFILL_PER_SEC}/sec`)
}
console.log()

const q = new Queen({ url, handleSignals: false })

// ---------------- 1. setup queues

console.log('[1/5] creating queues...')
await q.queue(Q_REQUESTS).config({
  leaseTime: LEASE_SEC,
  retryLimit: 100000,                  // denies are not failures
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

// ---------------- 2. burst push

console.log(`[2/5] burst-pushing ${TOTAL_MSGS.toLocaleString()} messages across ${TENANTS} partitions...`)
const pushStartAt = Date.now()
const tenants = Array.from({ length: TENANTS }, (_, i) => `tenant-${String(i).padStart(3, '0')}`)

// Push each tenant in PUSH_BATCH-sized chunks, with PUSH_PARALLEL tenants
// being pushed in parallel. This avoids blowing up memory / round-trip count
// when MSGS_PER_TENANT is large.
async function pushTenant(tenantId) {
  let pushed = 0
  for (let off = 0; off < MSGS_PER_TENANT; off += PUSH_BATCH) {
    const end = Math.min(off + PUSH_BATCH, MSGS_PER_TENANT)
    const items = []
    for (let seq = off; seq < end; seq++) {
      items.push({ data: { tenantId, seq, pushedAt: Date.now() } })
    }
    await q.queue(Q_REQUESTS).partition(tenantId).push(items)
    pushed += items.length
  }
  return pushed
}

let pushedSoFar = 0
let pushReporter = setInterval(() => {
  console.log(`  ... push progress: ${pushedSoFar.toLocaleString()}/${TOTAL_MSGS.toLocaleString()}`)
}, 5000)

// Worker pool over tenants
const queue = [...tenants]
async function pushWorker() {
  while (queue.length) {
    const t = queue.shift()
    if (!t) break
    const n = await pushTenant(t)
    pushedSoFar += n
  }
}
await Promise.all(Array.from({ length: PUSH_PARALLEL }, () => pushWorker()))
clearInterval(pushReporter)
const pushElapsedSec = (Date.now() - pushStartAt) / 1000
console.log(`      done in ${pushElapsedSec.toFixed(2)}s ` +
            `(${(TOTAL_MSGS / pushElapsedSec).toFixed(0)} msg/sec push throughput)\n`)

// ---------------- 3. start gate streams (one or many, parallel)

console.log(`[3/5] starting ${RUNNERS} rate-limited stream consumer(s)...`)

const gateStartAt = Date.now()
const queryId = `rate-limiter-stress-${tag}`

// The .gate() body: identical across every runner. Per-key state lives in
// PG and is loaded at the start of each cycle, so it doesn't matter which
// runner picks up the partition next — they all see the same token count.
function gateFn(req, ctx) {
  const cap = CAPACITY
  const refill = REFILL_PER_SEC
  const now = ctx.streamTimeMs

  if (typeof ctx.state.tokens !== 'number') ctx.state.tokens = cap
  if (typeof ctx.state.lastRefillAt !== 'number') ctx.state.lastRefillAt = now

  const elapsedSec = Math.max(0, (now - ctx.state.lastRefillAt) / 1000)
  ctx.state.tokens = Math.min(cap, ctx.state.tokens + elapsedSec * refill)
  ctx.state.lastRefillAt = now

  if (ctx.state.tokens >= 1) {
    ctx.state.tokens -= 1
    ctx.state.allowed = (ctx.state.allowed || 0) + 1
    return true
  }
  ctx.state.deniedSeen = (ctx.state.deniedSeen || 0) + 1
  return false
}

// Build a stream definition (immutable, can be re-run by N runners).
const buildStream = () =>
  Stream
    .from(q.queue(Q_REQUESTS))
    .gate(gateFn)
    .to(q.queue(Q_APPROVED))

// Start the FIRST runner with reset:true so a re-run of the example begins
// from a clean slate. All subsequent runners attach with reset:false to
// avoid wiping the state the first runner has already started building.
const streams = []
streams.push(await buildStream().run({
  queryId,
  url,
  batchSize:     BATCH,
  maxPartitions: MAX_PARTITIONS,
  maxWaitMillis: 200,
  reset:         true
}))
for (let i = 1; i < RUNNERS; i++) {
  streams.push(await buildStream().run({
    queryId,
    url,
    batchSize:     BATCH,
    maxPartitions: MAX_PARTITIONS,
    maxWaitMillis: 200,
    reset:         false
  }))
}

// Aggregate metrics across all runners (they each track their own counters).
function aggregateMetrics() {
  const agg = {
    cyclesTotal: 0, messagesTotal: 0, pushItemsTotal: 0,
    stateOpsTotal: 0, errorsTotal: 0,
    gateAllowsTotal: 0, gateDenialsTotal: 0
  }
  for (const s of streams) {
    const m = s.metrics()
    agg.cyclesTotal      += m.cyclesTotal      || 0
    agg.messagesTotal    += m.messagesTotal    || 0
    agg.pushItemsTotal   += m.pushItemsTotal   || 0
    agg.stateOpsTotal    += m.stateOpsTotal    || 0
    agg.errorsTotal      += m.errorsTotal      || 0
    agg.gateAllowsTotal  += m.gateAllowsTotal  || 0
    agg.gateDenialsTotal += m.gateDenialsTotal || 0
    if (m.lastError && !agg.lastError) agg.lastError = m.lastError
  }
  return agg
}

console.log(`      ${RUNNERS} stream(s) up\n`)

// ---------------- 4. sink consumer

// We track:
//   - per-tenant: count + first/last arrival timestamps
//   - per-tenant: full seq list ONLY for ORDER_SAMPLE_TENANTS sampled tenants
//   - latency samples: for sampled tenants, push->arrival latency

const sampledTenants = new Set(
  // Spread the sample evenly across the tenant index space so we catch
  // both early-pushed and late-pushed ones.
  Array.from({ length: ORDER_SAMPLE_TENANTS }, (_, i) =>
    tenants[Math.floor((i + 0.5) * tenants.length / ORDER_SAMPLE_TENANTS)]
  )
)

const tenantStats = new Map()  // tenantId -> { count, firstAt, lastAt, seqs?, latencies? }
for (const t of tenants) {
  const sampled = sampledTenants.has(t)
  tenantStats.set(t, {
    count: 0,
    firstAt: null,
    lastAt: null,
    seqs: sampled ? [] : null,
    latencies: sampled ? [] : null
  })
}

let totalDrained = 0
let drainStop = false
const drainStartAt = Date.now()

async function drain(workerIdx) {
  const cg = `rl-stress-consumer-${tag}`
  while (!drainStop) {
    try {
      const batch = await q.queue(Q_APPROVED)
        .group(cg)
        .batch(200)
        .wait(true)
        .timeoutMillis(500)
        .pop()
      if (!batch || batch.length === 0) continue
      const arrivedAt = Date.now()
      for (const m of batch) {
        const { tenantId, seq, pushedAt } = m.data
        const ts = tenantStats.get(tenantId)
        if (ts) {
          ts.count++
          if (ts.firstAt === null) ts.firstAt = arrivedAt
          ts.lastAt = arrivedAt
          if (ts.seqs) ts.seqs.push(seq)
          if (ts.latencies && typeof pushedAt === 'number') {
            ts.latencies.push(arrivedAt - pushedAt)
          }
        }
        totalDrained++
      }
      await q.ack(batch, true, { group: cg })
    } catch (err) {
      if (!drainStop) console.error(`[drain ${workerIdx}] error: ${err.message}`)
    }
  }
}
const drainWorkers = Array.from({ length: 4 }, (_, i) => drain(i))

// ---------------- 5. reporter & wait

let lastReportAt = Date.now()
let lastReportDrained = 0
const reporter = setInterval(() => {
  const now = Date.now()
  const dtSec = (now - lastReportAt) / 1000
  const rate = dtSec > 0 ? (totalDrained - lastReportDrained) / dtSec : 0
  const m = aggregateMetrics()
  const elapsed = (now - gateStartAt) / 1000
  console.log(
    `[T+${elapsed.toFixed(1)}s] ` +
    `drained ${totalDrained.toLocaleString()}/${TOTAL_MSGS.toLocaleString()} ` +
    `(${(100 * totalDrained / TOTAL_MSGS).toFixed(1)}%)  ` +
    `rate=${rate.toFixed(0)}/s  ` +
    `gate allow=${m.gateAllowsTotal.toLocaleString()} ` +
    `deny=${m.gateDenialsTotal.toLocaleString()}  ` +
    `cycles=${m.cyclesTotal.toLocaleString()}`
  )
  lastReportAt = now
  lastReportDrained = totalDrained
}, REPORT_EVERY_MS)

const overallStartAt = Date.now()
while (totalDrained < TOTAL_MSGS && (Date.now() - overallStartAt) < TIMEOUT_MS) {
  await new Promise(r => setTimeout(r, 250))
}

clearInterval(reporter)
drainStop = true
await Promise.all(streams.map(s => s.stop()))
await Promise.all(drainWorkers)

const overallElapsedSec = (Date.now() - overallStartAt) / 1000
const aggregateRate = totalDrained / overallElapsedSec

// ---------------- 6. verification

console.log('\n' + '='.repeat(80))
console.log('STRESS TEST VERIFICATION')
console.log('='.repeat(80))

let allPassed = true
const fail = (msg) => { allPassed = false; console.log(`  ❌ ${msg}`) }
const pass = (msg) => { console.log(`  ✅ ${msg}`) }

// 1. Conteggio
console.log('\n[1] Total throughput')
if (totalDrained === TOTAL_MSGS) {
  pass(`${totalDrained.toLocaleString()}/${TOTAL_MSGS.toLocaleString()} drained`)
} else {
  fail(`${totalDrained.toLocaleString()}/${TOTAL_MSGS.toLocaleString()} drained ` +
       `(${TOTAL_MSGS - totalDrained} missing — likely timeout)`)
}

// 2. Per-tenant counts
console.log('\n[2] Per-tenant counts (every tenant must end at MSGS_PER_TENANT)')
let countErrors = 0
let firstCountErrors = []
for (const [tenantId, ts] of tenantStats.entries()) {
  if (ts.count !== MSGS_PER_TENANT) {
    countErrors++
    if (firstCountErrors.length < 5) {
      firstCountErrors.push(`${tenantId}: ${ts.count}/${MSGS_PER_TENANT}`)
    }
  }
}
if (countErrors === 0) {
  pass(`all ${TENANTS} tenants drained ${MSGS_PER_TENANT.toLocaleString()} messages`)
} else {
  fail(`${countErrors} tenants short of target. Sample: ${firstCountErrors.join(', ')}`)
}

// 3. Order preserved on sampled tenants
console.log(`\n[3] FIFO order on sampled tenants (${ORDER_SAMPLE_TENANTS})`)
let orderErrors = 0
for (const tenantId of sampledTenants) {
  const ts = tenantStats.get(tenantId)
  if (!ts || !ts.seqs) continue
  let inOrder = true
  for (let i = 1; i < ts.seqs.length; i++) {
    if (ts.seqs[i] <= ts.seqs[i - 1]) { inOrder = false; break }
  }
  if (inOrder && ts.seqs.length === MSGS_PER_TENANT) {
    pass(`${tenantId}: ${ts.seqs.length} arrivals all in order`)
  } else if (!inOrder) {
    orderErrors++
    fail(`${tenantId}: out-of-order detected; first 30 seqs = ${ts.seqs.slice(0, 30).join(',')}`)
  } else {
    fail(`${tenantId}: only ${ts.seqs.length}/${MSGS_PER_TENANT} arrived (in order though)`)
  }
}

// 4. Per-tenant sustained rate (skip the initial burst)
console.log(`\n[4] Per-tenant sustained rate (target ≈ ${effectiveRatePerTenant.toFixed(0)} msg/sec, tol ±30%)`)
const minRate = effectiveRatePerTenant * 0.7
const maxRate = effectiveRatePerTenant * 1.3
const tenantRates = []
for (const [tenantId, ts] of tenantStats.entries()) {
  if (ts.count <= CAPACITY + 1) {
    tenantRates.push({ tenantId, rate: NaN })
    continue
  }
  const dtSec = (ts.lastAt - ts.firstAt) / 1000
  const rate = dtSec > 0 ? (ts.count - 1) / dtSec : Infinity
  tenantRates.push({ tenantId, rate })
}
const finiteRates = tenantRates.filter(t => Number.isFinite(t.rate))
const ratesArr = finiteRates.map(t => t.rate).sort((a, b) => a - b)
const rateMin = ratesArr[0]
const rateMax = ratesArr[ratesArr.length - 1]
const rateMedian = ratesArr[Math.floor(ratesArr.length / 2)]
const rateAvg = ratesArr.reduce((a, b) => a + b, 0) / ratesArr.length
console.log(`     min=${rateMin?.toFixed(2)}  median=${rateMedian?.toFixed(2)}  ` +
            `avg=${rateAvg?.toFixed(2)}  max=${rateMax?.toFixed(2)} msg/sec`)
const inBand = finiteRates.filter(t => t.rate >= minRate && t.rate <= maxRate).length
const outBand = finiteRates.length - inBand
if (outBand === 0) {
  pass(`all ${finiteRates.length} tenants within [${minRate.toFixed(0)}, ${maxRate.toFixed(0)}] msg/sec`)
} else {
  fail(`${outBand}/${finiteRates.length} tenants out of band ` +
       `[${minRate.toFixed(0)}, ${maxRate.toFixed(0)}] msg/sec`)
}

// 5. Aggregate sustained throughput
console.log(`\n[5] Aggregate throughput (target ≈ ${expectedAggregateRate.toLocaleString()} msg/sec)`)
const aggMin = expectedAggregateRate * 0.5
const aggOk = aggregateRate >= aggMin
const aggMsg = `${aggregateRate.toFixed(0)} msg/sec over ${overallElapsedSec.toFixed(2)}s`
if (aggOk) {
  pass(`${aggMsg} ≥ ${aggMin.toFixed(0)} (50% of target)`)
} else {
  fail(`${aggMsg} BELOW ${aggMin.toFixed(0)} (50% of target)`)
}

// 6. Latency (sampled tenants)
console.log('\n[6] End-to-end latency push→sink (sampled tenants)')
const allLatencies = []
for (const tenantId of sampledTenants) {
  const ts = tenantStats.get(tenantId)
  if (ts && ts.latencies) allLatencies.push(...ts.latencies)
}
allLatencies.sort((a, b) => a - b)
const lp = (p) => allLatencies[Math.min(allLatencies.length - 1, Math.floor(allLatencies.length * p))]
if (allLatencies.length > 0) {
  console.log(`     p50 = ${lp(0.50)} ms`)
  console.log(`     p95 = ${lp(0.95)} ms`)
  console.log(`     p99 = ${lp(0.99)} ms`)
  console.log(`     max = ${allLatencies[allLatencies.length - 1]} ms`)
  // Theoretical max latency = burst takes 0 + tail of msg #N waits ~ N/refill
  // For sampled tenants the worst-case is roughly MSGS_PER_TENANT/REFILL_PER_SEC
  const theoreticalMaxSec = MSGS_PER_TENANT / REFILL_PER_SEC
  const observedMaxSec = allLatencies[allLatencies.length - 1] / 1000
  if (observedMaxSec <= theoreticalMaxSec * 1.5) {
    pass(`max latency ${observedMaxSec.toFixed(1)}s ≤ 1.5× theoretical (${theoreticalMaxSec.toFixed(1)}s)`)
  } else {
    fail(`max latency ${observedMaxSec.toFixed(1)}s > 1.5× theoretical (${theoreticalMaxSec.toFixed(1)}s)`)
  }
} else {
  fail('no latency samples collected')
}

// 7. Stream metrics (aggregated across all runners)
console.log(`\n[7] Stream metrics (aggregated across ${RUNNERS} runners)`)
const m = aggregateMetrics()
console.log(`     gate ALLOWS:    ${m.gateAllowsTotal.toLocaleString()}`)
console.log(`     gate DENIES:    ${m.gateDenialsTotal.toLocaleString()}`)
console.log(`     cycles total:   ${m.cyclesTotal.toLocaleString()}`)
console.log(`     state ops:      ${m.stateOpsTotal.toLocaleString()}`)
console.log(`     errors:         ${m.errorsTotal}`)
console.log(`     overall time:   ${overallElapsedSec.toFixed(2)}s`)
// Per-runner breakdown
console.log(`     per-runner:`)
for (let i = 0; i < streams.length; i++) {
  const sm = streams[i].metrics()
  console.log(`       runner #${i}:  cycles=${sm.cyclesTotal.toLocaleString()} ` +
              `allow=${(sm.gateAllowsTotal || 0).toLocaleString()} ` +
              `deny=${(sm.gateDenialsTotal || 0).toLocaleString()} ` +
              `errors=${sm.errorsTotal}`)
}
if (m.errorsTotal === 0) {
  pass('zero stream errors')
} else {
  fail(`${m.errorsTotal} stream errors (last: ${m.lastError || '?'})`)
}

// 8. Theoretical efficiency check: how close are we to the rate limiter's
//    theoretical wall-clock (= MSGS_PER_TENANT / REFILL_PER_SEC, since
//    tenants run in parallel)?
console.log('\n[8] Efficiency (overall time vs theoretical minimum)')
const theoreticalMinSec = MSGS_PER_TENANT / REFILL_PER_SEC
const efficiency = (theoreticalMinSec / overallElapsedSec) * 100
console.log(`     theoretical min = ${theoreticalMinSec.toFixed(2)}s  ` +
            `actual = ${overallElapsedSec.toFixed(2)}s  ` +
            `efficiency = ${efficiency.toFixed(0)}%`)
if (efficiency >= 50) {
  pass(`efficiency ≥ 50%`)
} else {
  console.log(`  ⚠️  efficiency ${efficiency.toFixed(0)}% — overhead dominates ` +
              `(consider increasing BATCH or LEASE_SEC)`)
}

console.log('\n' + '='.repeat(80))
if (allPassed) {
  console.log('✅ STRESS TEST PASSED')
} else {
  console.log('❌ STRESS TEST FAILED — see above')
}
console.log('='.repeat(80))
console.log(`\nQueues left in PG for inspection:`)
console.log(`  - ${Q_REQUESTS}`)
console.log(`  - ${Q_APPROVED}`)
console.log(`Cleanup:`)
console.log(`  DELETE FROM queen.queues WHERE name LIKE 'rl_stress_%${tag}%';`)
console.log(`  DELETE FROM queen_streams.queries WHERE name = 'rate-limiter-stress-${tag}';`)
process.exit(allPassed ? 0 : 1)
