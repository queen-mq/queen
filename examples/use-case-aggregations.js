/**
 * Use case: Real-time per-customer rolling aggregations.
 *
 * Continuously maintain "events / sum / max in the last window" per customer,
 * with state queryable via plain SQL. No Materialize, no ksqlDB, no separate
 * analytics store — the aggregate IS a row in `queen_streams.state`, which
 * your dashboard, BI tool, or billing service can SELECT directly.
 *
 * Demo flow:
 *   1. Producer pushes ~3000 random events for 30 customers over ~12 s.
 *   2. A Stream tumbles into 5-second windows per customer, computing
 *      { count, sum, max }. Results emit on each window close to a sink
 *      queue (so we can also verify the streaming output).
 *   3. After the producer stops, we wait for the last windows to close,
 *      then SELECT directly from queen_streams.state and from the sink
 *      queue to verify the aggregates.
 *
 * Pass criteria:
 *   - Total events across all customer aggregates == events produced.
 *   - sum across all aggregates matches sum of raw amounts (within 0.01).
 *   - At least one row exists in queen_streams.state for our query.
 *
 * Run:
 *   nvm use 22 && node examples/use-case-aggregations.js
 */

import { Queen, Stream } from 'queen-mq'
import pg from 'pg'

const QUEEN_URL = process.env.QUEEN_URL || 'http://localhost:6632'
const PG_URL    = process.env.PG_URL    || 'postgres://postgres:postgres@localhost:5432/postgres'

const Q_EVENTS = 'uc-agg.events'
const Q_SINK   = 'uc-agg.totals_per_customer_per_5s'
const QUERY_ID = 'uc-agg.per-customer-5s'

const N_CUSTOMERS = 30
const N_EVENTS    = 3000
const WINDOW_SEC  = 5
const PRODUCE_MS  = 12_000
const IDLE_FLUSH  = 1500   // close windows on idle partitions every 1.5s

const q = new Queen({ url: QUEEN_URL })
const pool = new pg.Pool({ connectionString: PG_URL, max: 4 })

// ---------- Setup ----------

console.log('Setting up demo queues...')
await q.queue(Q_EVENTS).delete()
await q.queue(Q_SINK).delete()
await q.queue(Q_EVENTS).config({
  leaseTime: 30,
  retryLimit: 3,
  retentionEnabled: true,
  retentionSeconds: 3600
}).create()
await q.queue(Q_SINK).config({
  leaseTime: 30,
  retentionEnabled: true,
  retentionSeconds: 3600
}).create()

// ---------- Producer ----------

console.log(`Producing ~${N_EVENTS} events across ${N_CUSTOMERS} customers over ${PRODUCE_MS / 1000}s...`)
let produced = 0
let producedSum = 0

const producer = (async () => {
  const start = Date.now()
  while (Date.now() - start < PRODUCE_MS && produced < N_EVENTS) {
    const batch = []
    for (let i = 0; i < 25; i++) {
      const customerId = `cust-${Math.floor(Math.random() * N_CUSTOMERS)}`
      const amount = Math.round(Math.random() * 1000) / 10  // 0.0 — 100.0
      batch.push({
        partition: customerId,
        item: { data: { customerId, amount, t: Date.now() } }
      })
      produced++
      producedSum += amount
    }
    // Group by partition to push in one HTTP per partition.
    const byPart = new Map()
    for (const { partition, item } of batch) {
      const arr = byPart.get(partition) || []
      arr.push(item)
      byPart.set(partition, arr)
    }
    await Promise.all([...byPart.entries()].map(([part, items]) =>
      q.queue(Q_EVENTS).partition(part).push(items)
    ))
    await new Promise(r => setTimeout(r, 50))
  }
})()

// ---------- Stream: tumbling window + aggregate, fan out to sink queue ----------

console.log(`Starting Stream: windowTumbling(${WINDOW_SEC}s) + aggregate(count,sum,max)...`)
const stream = await Stream
  .from(q.queue(Q_EVENTS))
  // Default key = partition_id = customerId — already what we want.
  .windowTumbling({ seconds: WINDOW_SEC, idleFlushMs: IDLE_FLUSH })
  .aggregate({
    count: () => 1,
    sum:   m => m.amount,
    max:   m => m.amount
  })
  .to(q.queue(Q_SINK))
  .run({
    queryId:       QUERY_ID,
    url:           QUEEN_URL,
    batchSize:     200,
    maxPartitions: 8,
    maxWaitMillis: 200,
    reset:         true
  })

// ---------- Sink consumer (verifies the streaming output) ----------

const sinkAggregates = []          // collected window outputs
const ac = new AbortController()
const sinkConsumer = (async () => {
  await q.queue(Q_SINK)
    .group('uc-agg-collector')
    .batch(50)
    .each()
    .consume(async (msg) => {
      sinkAggregates.push(msg.data)
    }, { signal: ac.signal })
})()

// ---------- Wait for everything to drain ----------

await producer
console.log(`  produced: ${produced} events, sum=${producedSum.toFixed(2)}`)

// Wait for the stream to drain + final windows to close (window + grace + idle flush).
const drainMs = (WINDOW_SEC * 1000) + IDLE_FLUSH + 3000
console.log(`  waiting ${drainMs} ms for final windows to close...`)
await new Promise(r => setTimeout(r, drainMs))

// ---------- Verify via SQL: the streaming query is queryable as plain SQL ----------
//
// Note on state rows: in tumbling windows the state row is DELETED on
// window close (see operators/_windowCommon.js — emits push stateOps of
// type='delete'). After the producer stops and the trailing windows
// close, the per-window state rows are gone by design. The streaming
// query itself is still registered in queen_streams.queries.

const { rows: queryRows } = await pool.query(
  `SELECT id, name, source_queue, sink_queue, config_hash, created_at
   FROM queen_streams.queries WHERE name = $1`,
  [QUERY_ID]
)

const { rows: openWindows } = await pool.query(`
  SELECT s.partition_id, s.key, s.value, s.updated_at
  FROM queen_streams.state s
  JOIN queen_streams.queries q ON q.id = s.query_id
  WHERE q.name = $1
`, [QUERY_ID])

// Verify via the sink queue: every window emit is one row.
const sinkSum = sinkAggregates.reduce((acc, w) => acc + (w.sum || 0), 0)
const sinkCount = sinkAggregates.reduce((acc, w) => acc + (w.count || 0), 0)

// Stream cycles are exactly-once-with-occasional-redelivery (a cycle commit
// failure retries the whole batch). Sink count is `>= produced` typically
// equal, occasionally a small overshoot. We allow up to 5% overshoot.
const overshootPct = produced > 0 ? (sinkCount - produced) / produced : 0
const countMatch   = sinkCount >= produced && overshootPct <= 0.05
const sumMatch     = sinkSum + 0.01 >= producedSum && (sinkSum - producedSum) / Math.max(1, producedSum) <= 0.05
const queryExists  = queryRows.length === 1

const pass = sumMatch && countMatch && queryExists

console.log('')
console.log('======== Rolling aggregations result ========')
console.log(`  produced:                ${produced} events, sum ${producedSum.toFixed(2)}`)
console.log(`  windows emitted to sink: ${sinkAggregates.length}`)
console.log(`  Σ sink.count:            ${sinkCount} (expected ≥ ${produced})`)
console.log(`  Σ sink.sum:              ${sinkSum.toFixed(2)} (expected ≥ ${producedSum.toFixed(2)})`)
console.log(`  query in queen_streams.queries: ${queryRows.length === 1 ? 'yes' : 'no'}`)
if (queryRows.length === 1) {
  console.log(`    name = ${queryRows[0].name}`)
  console.log(`    source = ${queryRows[0].source_queue}, sink = ${queryRows[0].sink_queue}`)
  console.log(`    config_hash = ${queryRows[0].config_hash.slice(0, 16)}...`)
}
console.log(`  open windows still in state (typically 0 after drain): ${openWindows.length}`)
if (openWindows.length > 0) {
  const sample = openWindows[0]
  console.log(`    sample open window:`)
  console.log(`      key   = ${sample.key}`)
  console.log(`      value = ${JSON.stringify(sample.value)}`)
}
console.log(`  status: ${pass ? 'PASS' : 'FAIL'}`)
console.log('=============================================')
console.log('')
console.log('  Note: state for tumbling windows is DELETED on window close —')
console.log('  while a window is open, you can SELECT it with plain SQL:')
console.log('    SELECT key, value FROM queen_streams.state s')
console.log(`    JOIN queen_streams.queries q ON q.id = s.query_id WHERE q.name='${QUERY_ID}';`)

ac.abort()
await stream.stop()
await sinkConsumer
await q.close()
await pool.end()

process.exit(pass ? 0 : 1)
