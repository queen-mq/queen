/**
 * Use case: Idempotent webhook deduplication.
 *
 * Production pain: Stripe, Booking.com, Octorate, Twilio, GitHub all
 * redeliver webhooks — sometimes within minutes, sometimes within hours.
 * If your handler runs twice, you double-charge, double-email, or insert
 * a duplicate row. The standard fix is a Redis SET; now you have Redis to
 * operate, an eviction story, and an outage when Redis fills up.
 *
 * With Queen: a stream with a tumbling window per provider, holding the
 * set of seen eventIds inside the window. State lives in
 * queen_streams.state — durable, replicated by your PG backup, query-able
 * with plain SQL. No Redis.
 *
 * Demo flow:
 *   1. Push 1000 webhook events across 5 providers. Inject 300 duplicates
 *      with random delays so they arrive interleaved with originals — this
 *      mirrors real webhook redelivery from external providers.
 *   2. Stream: keyBy(providerId) -> windowTumbling(10s) -> reduce(seen set)
 *      -> filter(only emit if message was first-seen in window).
 *   3. Sink consumer counts how many unique events made it through.
 *
 * Pass criteria:
 *   - Sink receives one event per unique eventId within the window
 *     (1000 unique, ~1000 sink emits).
 *   - The deduplication held: NO duplicate eventIds in the sink output.
 *   - Throughput visible in the metrics.
 *
 * Run:
 *   nvm use 22 && node examples/use-case-dedup.js
 */

import { Queen, Stream } from 'queen-mq'

const QUEEN_URL = process.env.QUEEN_URL || 'http://localhost:6632'

const Q_INCOMING = 'uc-dedup.incoming'
const Q_VERIFIED = 'uc-dedup.verified'
const QUERY_ID   = 'uc-dedup.per-provider-10s'

const N_UNIQUE      = 1000
const N_DUPLICATES  = 300   // out of N_UNIQUE, this many get redelivered
const N_PROVIDERS   = 5
const WINDOW_SEC    = 10
const IDLE_FLUSH_MS = 1500

const q = new Queen({ url: QUEEN_URL })

// ---------- Setup ----------

console.log('Setting up demo queues...')
await q.queue(Q_INCOMING).delete()
await q.queue(Q_VERIFIED).delete()
await q.queue(Q_INCOMING).config({
  leaseTime: 30,
  retryLimit: 3,
  retentionEnabled: true,
  retentionSeconds: 3600
}).create()
await q.queue(Q_VERIFIED).config({
  leaseTime: 30,
  retentionEnabled: true,
  retentionSeconds: 3600
}).create()

// ---------- Producer: 1000 originals + 300 random redeliveries ----------

console.log(`Producing ${N_UNIQUE} unique events + ${N_DUPLICATES} duplicates across ${N_PROVIDERS} providers...`)
const allEvents = []
for (let i = 0; i < N_UNIQUE; i++) {
  allEvents.push({
    eventId:    `evt-${String(i).padStart(5, '0')}`,
    providerId: `prov-${i % N_PROVIDERS}`,
    seq:        i,
    nature:     'original'
  })
}
// Pick N_DUPLICATES random events to redeliver.
const dupes = []
for (let i = 0; i < N_DUPLICATES; i++) {
  const original = allEvents[Math.floor(Math.random() * N_UNIQUE)]
  dupes.push({ ...original, nature: 'duplicate' })
}
const all = [...allEvents, ...dupes]
// Shuffle so duplicates are interleaved with originals.
for (let i = all.length - 1; i > 0; i--) {
  const j = Math.floor(Math.random() * (i + 1))
  ;[all[i], all[j]] = [all[j], all[i]]
}

// Push in provider-partitioned batches so per-provider FIFO is preserved
// (matching real webhook delivery semantics).
const byProvider = new Map()
for (const e of all) {
  const arr = byProvider.get(e.providerId) || []
  arr.push(e)
  byProvider.set(e.providerId, arr)
}
let pushed = 0
const startPush = Date.now()
const BATCH = 100
for (const [providerId, events] of byProvider) {
  for (let i = 0; i < events.length; i += BATCH) {
    const slice = events.slice(i, i + BATCH)
    await q.queue(Q_INCOMING).partition(providerId).push(
      slice.map(e => ({ data: e }))
    )
    pushed += slice.length
  }
}
console.log(`  pushed ${pushed} events (${all.length - N_UNIQUE} duplicates mixed in), wall ${Date.now() - startPush} ms`)

// ---------- Stream: dedup within 10s tumbling windows per provider ----------

console.log(`Starting dedup stream: keyBy(providerId) -> windowTumbling(${WINDOW_SEC}s) -> reduce(seen-set) -> emit firsts...`)

// The reduce accumulator keeps:
//   - seen: Set of eventIds seen so far in this window
//   - emits: array of events that were first-seen (these become the window output)
const stream = await Stream
  .from(q.queue(Q_INCOMING))
  // Default key = partition_id = providerId, which is what we want.
  .windowTumbling({ seconds: WINDOW_SEC, idleFlushMs: IDLE_FLUSH_MS })
  .reduce(
    (acc, msg) => {
      if (acc.seen.includes(msg.eventId)) {
        // duplicate — drop
        return acc
      }
      return {
        seen:  [...acc.seen, msg.eventId],
        firsts: [...acc.firsts, msg]
      }
    },
    { seen: [], firsts: [] }
  )
  .flatMap(window => window.firsts)        // explode per-window aggregate into individual emits
  .to(q.queue(Q_VERIFIED))
  .run({
    queryId:       QUERY_ID,
    url:           QUEEN_URL,
    batchSize:     200,
    maxPartitions: N_PROVIDERS,
    maxWaitMillis: 200,
    reset:         true
  })

// ---------- Sink consumer: counts unique events delivered downstream ----------

const sinkSeen = new Set()
const sinkDup  = []
const ac = new AbortController()
const sinkConsumer = (async () => {
  await q.queue(Q_VERIFIED)
    .group('uc-dedup-collector')
    .batch(50)
    .each()
    .consume(async (msg) => {
      const id = msg.data.eventId
      if (sinkSeen.has(id)) sinkDup.push(id)
      sinkSeen.add(id)
    }, { signal: ac.signal })
})()

// ---------- Wait for windows to close + sink to drain ----------

// Generous drain: window + idle-flush + safety margin. Window-close in
// processing-time mode happens when:
//   - a later event arrives with createdAt past windowEnd (won't happen
//     here since the producer finished long ago), OR
//   - the idle-flush timer fires (IDLE_FLUSH_MS cadence) and sees
//     windowEnd + gracePeriod <= Date.now().
// So worst case: window_sec + idle_flush_interval + sink-pop latency.
const drainMs = (WINDOW_SEC * 1000) + IDLE_FLUSH_MS + 10_000
console.log(`Waiting ${drainMs} ms for final windows to close + sink to drain...`)
const drainStart = Date.now()
while (Date.now() - drainStart < drainMs) {
  await new Promise(r => setTimeout(r, 500))
  // Early exit if we've already seen >= N_UNIQUE events.
  if (sinkSeen.size >= N_UNIQUE) {
    // Give a final 1s of grace for any tail emits.
    await new Promise(r => setTimeout(r, 1000))
    break
  }
}

// ---------- Verify ----------

// We expect ~N_UNIQUE unique events through the sink. Allowing a small slop
// because if some events landed near a window boundary, an original might
// fall in window-A and its duplicate in window-B — both pass dedup. That's
// the documented semantic of in-window deduplication; for stricter dedup,
// increase the window or use a session-window variant.
const uniqueOut    = sinkSeen.size
const dupesInSink  = sinkDup.length
const passUnique   = uniqueOut === N_UNIQUE
const passNoDupes  = dupesInSink === 0

const pass = passUnique && passNoDupes

console.log('')
console.log('======== Webhook deduplication result ========')
console.log(`  unique events produced:       ${N_UNIQUE}`)
console.log(`  duplicates injected:          ${N_DUPLICATES}`)
console.log(`  events arriving at incoming:  ${all.length}`)
console.log(`  events through to verified:   ${uniqueOut}     (expected ${N_UNIQUE})`)
console.log(`  duplicates leaked to verified:${dupesInSink}   (expected 0)`)
console.log(`  dedup ratio:                  ${((1 - uniqueOut / all.length) * 100).toFixed(1)}% of incoming dropped`)
console.log(`  stream metrics:               cycles=${stream.metrics().cyclesTotal}, msgs=${stream.metrics().messagesTotal}`)
console.log(`  status: ${pass ? 'PASS' : 'FAIL'}`)
console.log('==============================================')
console.log('')
console.log('  Dedup state is just a row in queen_streams.state per (provider, window).')
console.log('  No Redis, no separate dedup service, no eviction policy to babysit —')
console.log("  the window closes and the state is GC'd automatically.")

ac.abort()
await stream.stop()
await sinkConsumer
await q.close()

process.exit(pass ? 0 : 1)
