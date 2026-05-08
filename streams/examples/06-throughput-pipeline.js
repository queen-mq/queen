/**
 * Example 06 — Realistic high-throughput pipeline.
 *
 * Architecture:
 *
 *   3 producers (async tasks)            ─┐
 *   ┌────────────────────────┐            │
 *   │ producer-A  customerIds 0..15 │     │ ─→ orders.raw  (50 partitions)
 *   │ producer-B  customerIds 16..32│     │      │
 *   │ producer-C  customerIds 33..49│     │      │
 *   └────────────────────────┘            │      │
 *                                                │
 *                         ┌──────────────────────┴──────────────────────┐
 *                         │                                             │
 *                  Stream A (totals)                          Stream B (high-value)
 *                  windowTumbling 1s                          filter amount > 500
 *                  aggregate count + sum                       map enrich w/ ts
 *                  → orders.totals                             → orders.high_value
 *                         │                                             │
 *                  Consumer A drains                          Consumer B drains
 *                  & counts emits                             & counts emits
 *
 * Throughput target: 1000+ msg/sec end-to-end.
 *
 * Run:
 *   QUEEN_URL=http://localhost:6632 DURATION_MS=30000 \
 *     node examples/06-throughput-pipeline.js
 *
 * While it runs, watch http://localhost:4000/ — you should see all four
 * queues with non-zero activity, and the runner stats at /metrics.
 */

import { Queen, Stream } from 'queen-mq'
const url = process.env.QUEEN_URL || 'http://localhost:6632'
const durationMs = parseInt(process.env.DURATION_MS || '30000', 10)

const tag = Date.now().toString(36)
const Q_RAW         = `pipeline_orders_raw_${tag}`
const Q_TOTALS      = `pipeline_orders_totals_${tag}`
const Q_HIGH_VALUE  = `pipeline_orders_high_value_${tag}`

const NUM_PARTITIONS  = 50
const BATCH_PER_PUSH  = 25      // events per push call per producer
const PUSH_INTERVAL_MS = 50      // each producer pushes a batch every 50ms
                                  // → ~500 ev/s per producer × 3 producers = ~1500 ev/s

console.log(`[pipeline] Queen   = ${url}`)
console.log(`[pipeline] tag     = ${tag}`)
console.log(`[pipeline] target  = ~${(BATCH_PER_PUSH * 3 * 1000) / PUSH_INTERVAL_MS} msg/sec for ${durationMs}ms`)
console.log()

const q = new Queen({ url, handleSignals: false })

// ----------------------------------------------------------------- 1. Setup queues

console.log('[pipeline] creating queues...')
await q.queue(Q_RAW).create()
await q.queue(Q_TOTALS).create()
await q.queue(Q_HIGH_VALUE).create()

// ----------------------------------------------------------------- 2. Producers

const stats = {
  pushedA: 0, pushedB: 0, pushedC: 0,
  consumedTotals: 0, consumedHighValue: 0,
  startedAt: null, stoppedAt: null
}

let running = true
let nextOrderId = 0

/**
 * Each producer cycles through a range of customer IDs and pushes one
 * BATCH_PER_PUSH-event batch per partition per tick.
 */
async function producer(name, custStart, custEnd, statKey) {
  let cidx = custStart
  while (running) {
    const cust = `cust-${cidx}`
    const items = []
    for (let i = 0; i < BATCH_PER_PUSH; i++) {
      const orderId = ++nextOrderId
      // 12% of orders are "high value" (>500); the rest 50..500.
      const isHigh = Math.random() < 0.12
      const amount = isHigh
        ? Math.floor(500 + Math.random() * 1500)
        : Math.floor(50 + Math.random() * 450)
      items.push({
        data: {
          orderId,
          customerId: cust,
          amount,
          createdAt: Date.now()
        }
      })
    }
    try {
      await q.queue(Q_RAW).partition(cust).push(items)
      stats[statKey] += items.length
    } catch (err) {
      console.error(`[${name}] push error:`, err.message)
    }
    cidx = cidx + 1
    if (cidx > custEnd) cidx = custStart
    await new Promise(r => setTimeout(r, PUSH_INTERVAL_MS))
  }
  console.log(`[${name}] stopped after pushing ${stats[statKey].toLocaleString()} events`)
}

// ----------------------------------------------------------------- 3. Streaming pipelines

console.log('[pipeline] starting Stream A (totals)...')
const streamA = await Stream
  .from(q.queue(Q_RAW))
  .windowTumbling({ seconds: 1, idleFlushMs: 500 })
  .aggregate({
    count: () => 1,
    sum:   m => m.amount,
    min:   m => m.amount,
    max:   m => m.amount
  })
  .map((agg, ctx) => ({
    customer:    ctx.partition,
    windowStart: new Date(ctx.windowStart).toISOString(),
    windowEnd:   new Date(ctx.windowEnd).toISOString(),
    count:       agg.count,
    sum:         agg.sum,
    min:         agg.min,
    max:         agg.max,
    avg:         Math.round(agg.sum / agg.count)
  }))
  .to(q.queue(Q_TOTALS))
  .run({
    queryId:       `pipeline.stream_a.totals.${tag}`,
    url,
    batchSize:     200,
    maxPartitions: 8,
    reset:         true
  })

console.log('[pipeline] starting Stream B (high-value)...')
const streamB = await Stream
  .from(q.queue(Q_RAW))
  .filter(m => m.data.amount > 500)
  .map(m => ({
    orderId:    m.data.orderId,
    customerId: m.data.customerId,
    amount:     m.data.amount,
    detectedAt: new Date().toISOString()
  }))
  .to(q.queue(Q_HIGH_VALUE))
  .run({
    queryId:       `pipeline.stream_b.high_value.${tag}`,
    url,
    batchSize:     200,
    maxPartitions: 8,
    reset:         true
  })

// ----------------------------------------------------------------- 4. Consumers

/**
 * Drain a sink continuously. Uses batch=1 + per-cycle ack so the lease is
 * released between pops (see test-v2/stream/_helpers.js for the same
 * pattern). For real applications you'd use bigger batches and explicitly
 * release leases after each batch.
 */
async function consumer(name, queueName, statKey) {
  const cg = `${name}-${tag}`
  while (running) {
    try {
      const popped = await q
        .queue(queueName)
        .group(cg)
        .batch(50)        // bigger batch is fine because we'll ack ALL of them
        .wait(true)
        .timeoutMillis(500)
        .pop()
      if (!popped || popped.length === 0) continue
      stats[statKey] += popped.length
      // Ack all popped messages — once acked_count >= batch_size the lease
      // is released and the next pop reclaims the partition.
      await q.ack(popped, true, { group: cg })
    } catch (err) {
      console.error(`[${name}] consume error:`, err.message)
    }
  }
  console.log(`[${name}] stopped after consuming ${stats[statKey].toLocaleString()} messages`)
}

// ----------------------------------------------------------------- 5. Status reporter

let lastReportAt = Date.now()
let lastReport = { pushedA: 0, pushedB: 0, pushedC: 0, consumedTotals: 0, consumedHighValue: 0 }

function startReporter() {
  return setInterval(() => {
    const now = Date.now()
    const dtSec = (now - lastReportAt) / 1000
    const pushedNow = stats.pushedA + stats.pushedB + stats.pushedC
    const pushedLast = lastReport.pushedA + lastReport.pushedB + lastReport.pushedC
    const consumedNow = stats.consumedTotals + stats.consumedHighValue
    const consumedLast = lastReport.consumedTotals + lastReport.consumedHighValue
    const pushRate = Math.round((pushedNow - pushedLast) / dtSec)
    const consumeRate = Math.round((consumedNow - consumedLast) / dtSec)
    const aMet = streamA.metrics()
    const bMet = streamB.metrics()
    console.log(
      `[T+${Math.round((now - stats.startedAt) / 1000)}s] ` +
      `push=${pushRate}/s (total ${pushedNow.toLocaleString()})  ` +
      `streamA cycles=${aMet.cyclesTotal} flushes=${aMet.flushCyclesTotal} pushes=${aMet.pushItemsTotal}  ` +
      `streamB cycles=${bMet.cyclesTotal} pushes=${bMet.pushItemsTotal}  ` +
      `consume=${consumeRate}/s (totals=${stats.consumedTotals} highValue=${stats.consumedHighValue})`
    )
    lastReportAt = now
    lastReport = { ...stats }
  }, 2000)
}

// ----------------------------------------------------------------- 6. Run it

console.log(`\n[pipeline] running for ${durationMs}ms...\n`)
stats.startedAt = Date.now()

const reporter = startReporter()

await Promise.all([
  producer('producer-A', 0,  15, 'pushedA'),
  producer('producer-B', 16, 32, 'pushedB'),
  producer('producer-C', 33, 49, 'pushedC'),
  consumer('consumer-totals',     Q_TOTALS,     'consumedTotals'),
  consumer('consumer-highvalue',  Q_HIGH_VALUE, 'consumedHighValue'),
  (async () => {
    await new Promise(r => setTimeout(r, durationMs))
    running = false
  })()
])

stats.stoppedAt = Date.now()
clearInterval(reporter)

console.log('\n[pipeline] producers + consumers stopped, waiting for streams to drain...')
// Give the streams ~5s to flush trailing windows.
await new Promise(r => setTimeout(r, 5000))
await streamA.stop()
await streamB.stop()

// ----------------------------------------------------------------- 7. Final report

const totalPushed = stats.pushedA + stats.pushedB + stats.pushedC
const totalConsumed = stats.consumedTotals + stats.consumedHighValue
const elapsedSec = (stats.stoppedAt - stats.startedAt) / 1000
const aMet = streamA.metrics()
const bMet = streamB.metrics()

console.log('\n' + '='.repeat(80))
console.log('PIPELINE FINAL STATS')
console.log('='.repeat(80))

console.log('\n[producers]')
console.log(`  producer-A:           ${stats.pushedA.toLocaleString()} events`)
console.log(`  producer-B:           ${stats.pushedB.toLocaleString()} events`)
console.log(`  producer-C:           ${stats.pushedC.toLocaleString()} events`)
console.log(`  total pushed:         ${totalPushed.toLocaleString()} events`)
console.log(`  push throughput:      ${Math.round(totalPushed / elapsedSec).toLocaleString()} msg/sec`)

console.log('\n[stream A — orders.totals  (windowTumbling 1s, aggregate per customer)]')
console.log(`  cycles run:           ${aMet.cyclesTotal.toLocaleString()}`)
console.log(`  flush cycles:         ${aMet.flushCyclesTotal.toLocaleString()}`)
console.log(`  messages processed:   ${aMet.messagesTotal.toLocaleString()}`)
console.log(`  closed windows:       ${aMet.pushItemsTotal.toLocaleString()}`)
console.log(`  state ops:            ${aMet.stateOpsTotal.toLocaleString()}`)
console.log(`  errors:               ${aMet.errorsTotal}`)
console.log(`  processing rate:      ${Math.round(aMet.messagesTotal / elapsedSec).toLocaleString()} msg/sec`)

console.log('\n[stream B — orders.high_value  (filter > 500 + enrich)]')
console.log(`  cycles run:           ${bMet.cyclesTotal.toLocaleString()}`)
console.log(`  messages processed:   ${bMet.messagesTotal.toLocaleString()}`)
console.log(`  high-value emitted:   ${bMet.pushItemsTotal.toLocaleString()}`)
console.log(`  errors:               ${bMet.errorsTotal}`)
console.log(`  processing rate:      ${Math.round(bMet.messagesTotal / elapsedSec).toLocaleString()} msg/sec`)

console.log('\n[consumers]')
console.log(`  consumer-totals:      ${stats.consumedTotals.toLocaleString()} aggregates drained`)
console.log(`  consumer-highvalue:   ${stats.consumedHighValue.toLocaleString()} alerts drained`)
console.log(`  total drained:        ${totalConsumed.toLocaleString()}`)
console.log(`  drain throughput:     ${Math.round(totalConsumed / elapsedSec).toLocaleString()} msg/sec`)

console.log('\n[end-to-end]')
console.log(`  duration:             ${elapsedSec.toFixed(1)}s`)
const totalProcessed = aMet.messagesTotal + bMet.messagesTotal
console.log(`  total processed:      ${totalProcessed.toLocaleString()} events (across both streams)`)
console.log(`  combined throughput:  ${Math.round(totalProcessed / elapsedSec).toLocaleString()} msg/sec processed`)
console.log()

const targetHit = Math.round(totalPushed / elapsedSec) >= 1000
console.log(targetHit
  ? `✅ Throughput target met: ≥1000 msg/sec`
  : `⚠️ Throughput below target. Tune BATCH_PER_PUSH / PUSH_INTERVAL_MS.`)

console.log(`\nQueues left in PG for dashboard inspection (http://localhost:4000):`)
console.log(`  - ${Q_RAW}`)
console.log(`  - ${Q_TOTALS}`)
console.log(`  - ${Q_HIGH_VALUE}`)
console.log(`Drop them with:  DELETE FROM queen.queues WHERE name LIKE 'pipeline_%${tag}%';`)
console.log()

await q.close()
process.exit(0)
