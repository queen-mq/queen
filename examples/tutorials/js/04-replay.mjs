// docs:start(tut-js-replay)
//
// Tutorial 4 of 5: replay.
//
// Acknowledging a message does not delete it. Consumption is a cursor per
// consumer group, and the messages stay until retention removes them, so a new
// group can read the whole history and an existing group can be moved back.
//
// This is the tutorial that shows what a cursor buys you: reprocessing after a
// bug, backfilling a new consumer, and auditing what was delivered, all without
// asking the producer to send anything twice.
//
// Run it:
//   QUEEN_URL=http://localhost:6632 node 04-replay.mjs

import { Queen } from 'queen-mq'

const QUEEN_URL = process.env.QUEEN_URL || 'http://localhost:6632'
const RUN = Date.now().toString(36)
const EVENTS = `tut-js-replay-${RUN}`

const EVENTS_IN = [
  { seq: 1, type: 'created' },
  { seq: 2, type: 'updated' },
  { seq: 3, type: 'shipped' },
  { seq: 4, type: 'delivered' },
]

let checks = 0
const assert = (condition, description) => {
  if (!condition) throw new Error(description)
  checks++
  console.log(`  ok: ${description}`)
}

const drain = async (group, expected, mode) => {
  const seen = []
  const builder = queen
    .queue(EVENTS)
    .partition('order-1')
    .group(group)
    .each()
    .limit(expected)
    .idleMillis(4000)
  if (mode) builder.subscriptionMode(mode)
  await builder.consume(async (msg) => {
    seen.push(msg.data.seq)
  })
  return seen
}

const queen = new Queen({ url: QUEEN_URL, handleSignals: false })

try {
  console.log(`broker ${QUEEN_URL}`)

  for (const event of EVENTS_IN) {
    await queen.queue(EVENTS).partition('order-1').push({ data: event })
  }
  console.log(`pushed ${EVENTS_IN.length} events`)

  // The live consumer. It drains the lane and commits as it goes.
  console.log('\nthe live consumer')
  const live = await drain('tut-js-live', 4, 'all')
  console.log(`  saw ${live.join(', ')}`)
  assert(JSON.stringify(live) === JSON.stringify([1, 2, 3, 4]), 'the live group read the lane in order')

  // A second group, created now, after every message was already stored and
  // acknowledged by someone else. subscriptionMode('all') is what points its
  // new cursor at the beginning: the default for a new group is the tail, so
  // without it this group would sit idle waiting for the next event.
  //
  // The mode applies when the cursor is created and never again, so it cannot
  // rewind a group that already exists. That is what seek below is for.
  console.log('\na new group, backfilled from the beginning')
  const audit = await drain('tut-js-audit', 4, 'all')
  console.log(`  saw ${audit.join(', ')}`)
  assert(JSON.stringify(audit) === JSON.stringify([1, 2, 3, 4]), 'a new group replayed the whole history')

  // Nothing was re-pushed and nothing was copied: both groups read the same
  // stored messages through their own cursors.
  console.log('\nrewinding an existing group')

  // Move the live group's cursor back an hour, which is before anything in this
  // run was pushed. The seek also releases any live lease, so an in-flight batch
  // is abandoned rather than acknowledged.
  await queen.admin.seekConsumerGroup('tut-js-live', EVENTS, {
    timestamp: new Date(Date.now() - 3600 * 1000).toISOString(),
  })

  const replayed = await drain('tut-js-live', 4)
  console.log(`  saw ${replayed.join(', ')}`)
  assert(
    JSON.stringify(replayed) === JSON.stringify([1, 2, 3, 4]),
    'the rewound group read the same events again, in the same order'
  )

  // Replay is per group. The audit group was not moved, so it stays where it
  // was and sees nothing new.
  const auditAgain = await queen
    .queue(EVENTS)
    .partition('order-1')
    .group('tut-js-audit')
    .batch(10)
    .wait(false)
    .pop()
  assert(auditAgain.length === 0, 'rewinding one group left the other where it was')

  await queen.queue(EVENTS).delete()

  console.log(`\nPASS: ${checks} checks`)
} catch (err) {
  console.error(`\nFAIL: ${err.message}`)
  process.exitCode = 1
} finally {
  await queen.close()
}
// docs:end
