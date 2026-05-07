/**
 * Demo — push some data, run a streaming pipeline with v0.2 features,
 * watch the results.
 *
 * Showcases:
 *   - Tumbling windows with gracePeriod and idleFlushMs
 *   - Per-partition aggregation across 3 customers
 *   - .map(agg, ctx) post-reducer with window/partition metadata
 *   - .foreach(window, ctx) terminal side effect (+ a sink mirror)
 *   - The idle flush closing windows on quiet partitions
 *
 * Self-contained: creates the source and sink queues, pushes 30 messages
 * across 3 customer partitions over ~12s, lets the engine compute 3-second
 * windowed sums, prints each closed window's aggregate as it lands.
 *
 * Prerequisites:
 *   Queen v0.2+ running on QUEEN_URL with the queen-streams schema loaded
 *   (the SPs auto-load on Queen boot).
 *
 * Run:
 *   QUEEN_URL=http://localhost:6632 node examples/00-demo.js
 */

import { Queen } from 'queen-mq'
import { Stream } from '../index.js'

const url = process.env.QUEEN_URL || 'http://localhost:6632'
const tag = Date.now().toString(36)
const SOURCE_QUEUE = `streams_demo_orders_${tag}`
const SINK_QUEUE   = `streams_demo_totals_${tag}`
const QUERY_ID     = `streams.demo.${tag}`

const CUSTOMERS = ['cust-A', 'cust-B', 'cust-C']
const MESSAGES_PER_CUSTOMER = 10
const WINDOW_SECONDS = 3
const GRACE_SECONDS  = 1

console.log(`[demo] Queen at ${url}`)
console.log(`[demo] source = ${SOURCE_QUEUE}`)
console.log(`[demo] sink   = ${SINK_QUEUE}`)
console.log(`[demo] query  = ${QUERY_ID}`)

const q = new Queen({ url, handleSignals: false })

// ----------------------------------------------------------------- 1. Setup

console.log('\n[demo] creating queues...')
await q.queue(SOURCE_QUEUE).create()
await q.queue(SINK_QUEUE).create()

// ----------------------------------------------------------------- 2. Push data

const pushPromise = (async () => {
  console.log('\n[demo] pushing messages over ~12 seconds...')
  for (let i = 0; i < MESSAGES_PER_CUSTOMER; i++) {
    await Promise.all(CUSTOMERS.map((cust, idx) =>
      q.queue(SOURCE_QUEUE).partition(cust).push([{
        data: {
          customerId: cust,
          amount: 10 + idx * 5 + i,
          idx: i,
          wave: 'demo'
        }
      }])
    ))
    process.stdout.write('.')
    await new Promise(r => setTimeout(r, 1200))
  }
  console.log('\n[demo] all messages pushed')
})()

// ----------------------------------------------------------------- 3. Stream

console.log('\n[demo] starting stream...')

const handle = await Stream
  .from(q.queue(SOURCE_QUEUE))
  .windowTumbling({
    seconds:     WINDOW_SECONDS,
    gracePeriod: GRACE_SECONDS,    // accept events for 1 more second after windowEnd
    idleFlushMs: 1500              // close ripe windows on quiet partitions every 1.5s
  })
  .aggregate({
    count: () => 1,
    sum:   m => m.amount,
    min:   m => m.amount,
    max:   m => m.amount,
    avg:   m => m.amount
  })
  .map((agg, ctx) => {
    // Post-reducer .map gets the aggregate value + a ctx with window+partition.
    const { __avg_sum, __avg_count, ...clean } = agg
    return {
      patient:     ctx.partition,
      windowStart: new Date(ctx.windowStart).toISOString(),
      windowEnd:   new Date(ctx.windowEnd).toISOString(),
      ...clean
    }
  })
  .foreach((window, ctx) => {
    console.log(
      `[${window.windowStart}] ${window.patient}: ` +
      `count=${window.count} sum=${window.sum} avg=${window.avg.toFixed(1)} min=${window.min} max=${window.max}`
    )
  })
  .run({
    queryId:     QUERY_ID,
    url,
    batchSize:   50,
    maxPartitions: 4,
    reset: true
  })

// ----------------------------------------------------------------- 4. Wait

await pushPromise
console.log('\n[demo] all pushed; waiting for idle flush to close trailing windows (5s)...')
await new Promise(r => setTimeout(r, 5_000))

console.log('\n[demo] stopping stream...')
await handle.stop()

const m = handle.metrics()
console.log('\n[demo] final stream metrics:')
console.log(`  cycles:        ${m.cyclesTotal}`)
console.log(`  flush cycles:  ${m.flushCyclesTotal}`)
console.log(`  messages:      ${m.messagesTotal}`)
console.log(`  push items:    ${m.pushItemsTotal}`)
console.log(`  state ops:     ${m.stateOpsTotal}`)
console.log(`  late events:   ${m.lateEventsTotal}`)
console.log(`  errors:        ${m.errorsTotal}`)

await q.close()
process.exit(0)
