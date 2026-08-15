// docs:start(tut-js-streaming)
//
// Tutorial 5 of 5: streaming.
//
// The four tutorials before this one move messages. This one aggregates them:
// a tumbling window per entity, whose state, output and acknowledgements commit
// in the same PostgreSQL transaction. That is exactly-once aggregation with no
// changelog topic and no state store to operate, because the state and the
// queue are already in the same database.
//
// A stream is a running process, so the order here is the order you would use
// in production: start it, then let events arrive.
//
// Run it:
//   QUEEN_URL=http://localhost:6632 node 05-streaming.mjs

import { Queen, Stream } from 'queen-mq'

const QUEEN_URL = process.env.QUEEN_URL || 'http://localhost:6632'
const RUN = Date.now().toString(36)
const EVENTS = `tut-js-stream-events-${RUN}`
const TOTALS = `tut-js-stream-totals-${RUN}`

// The query id is this streaming query's identity in the database. Its window
// state is keyed by it, so restarting the program with the same id resumes the
// same windows instead of starting new ones.
const QUERY_ID = `tut-js-totals-${RUN}`

const SALES = [
  { customer: 'acme', amount: 10 },
  { customer: 'acme', amount: 32.5 },
  { customer: 'globex', amount: 7.25 },
  { customer: 'acme', amount: 0.25 },
  { customer: 'globex', amount: 100 },
]

let checks = 0
const assert = (condition, description) => {
  if (!condition) throw new Error(description)
  checks++
  console.log(`  ok: ${description}`)
}

const queen = new Queen({ url: QUEEN_URL, handleSignals: false })
let stream = null

try {
  console.log(`broker ${QUEEN_URL}`)

  // Both queues are created up front here, rather than by the first push, so
  // the stream has something to attach to before any event exists.
  await queen.queue(EVENTS).config({ leaseTime: 30, retryLimit: 3 }).create()
  await queen.queue(TOTALS).config({ leaseTime: 30 }).create()

  console.log('\nstarting the stream')
  stream = await Stream
    .from(queen.queue(EVENTS))
    // Tumbling: fixed, non-overlapping windows, one set per partition. A window
    // closes when its time is up; idleFlushMs also closes one whose partition
    // has gone quiet, which is what lets a short program finish.
    .windowTumbling({ seconds: 2, idleFlushMs: 800 })
    // The extractors receive the message payload itself, not the envelope: it
    // is `s.amount`, not `s.data.amount`. The difference is silent, because a
    // missing field aggregates to zero.
    .aggregate({
      count: () => 1,
      sum: (s) => s.amount,
      max: (s) => s.amount,
    })
    // Every closed window is pushed here, in the same transaction that commits
    // the window state and acknowledges the inputs it was computed from.
    .to(queen.queue(TOTALS))
    .run({
      queryId: QUERY_ID,
      url: QUEEN_URL,
      batchSize: 100,
      maxPartitions: 8,
      maxWaitMillis: 200,
    })

  console.log('\npushing sales')
  for (const sale of SALES) {
    // The partition is the aggregation key: window state is per partition, so
    // one customer's totals are computed from that customer's lane alone.
    await queen.queue(EVENTS).partition(sale.customer).push({ data: sale })
    console.log(`  ${sale.customer} ${sale.amount}`)
  }

  console.log('\ncollecting closed windows')

  // A window is a slice of time, so a customer's sales can fall on either side
  // of a boundary and arrive as two windows instead of one. That is what
  // windowing is, and it is why this adds the windows up per customer instead
  // of expecting exactly one each.
  //
  // The loop waits for the totals it expects, with a deadline. Waiting for a
  // quiet period instead would be a race: the last window closes when its timer
  // says so, not when the reader is tired of waiting.
  const totals = {}
  const complete = () => totals.acme?.count === 3 && totals.globex?.count === 2
  const deadline = Date.now() + 30000

  while (!complete() && Date.now() < deadline) {
    const closed = await queen
      .queue(TOTALS)
      .group('tut-js-stream-collector')
      .subscriptionMode('all')
      .batch(20)
      .partitions(10)
      .wait(true)
      .timeoutMillis(2000)
      .pop()

    for (const msg of closed) {
      // The window's key is the partition it was computed for.
      const t = (totals[msg.partition] ??= { count: 0, sum: 0, max: 0 })
      t.count += msg.data.count
      t.sum += msg.data.sum
      t.max = Math.max(t.max, msg.data.max)
      console.log(`  ${msg.partition}: ${JSON.stringify(msg.data)}`)
      await queen.ack(msg, true, { group: 'tut-js-stream-collector' })
    }
  }

  assert(complete(), 'every sale reached a closed window before the deadline')
  assert(Math.abs(totals.acme.sum - 42.75) < 0.001, 'acme summed to 42.75 across its windows')
  assert(Math.abs(totals.globex.sum - 107.25) < 0.001, 'globex summed to 107.25')
  assert(totals.globex.max === 100, 'globex kept its largest single sale')

  await stream.stop()
  stream = null

  await queen.queue(EVENTS).delete()
  await queen.queue(TOTALS).delete()

  console.log(`\nPASS: ${checks} checks`)
} catch (err) {
  console.error(`\nFAIL: ${err.message}`)
  process.exitCode = 1
} finally {
  if (stream) await stream.stop()
  await queen.close()
}
// docs:end
