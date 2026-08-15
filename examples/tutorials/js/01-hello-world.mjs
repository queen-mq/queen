// docs:start(tut-js-hello-world)
//
// Tutorial 1 of 5: hello world.
//
// One message in, one message out. Nothing is created in advance: the queue and
// the partition come into existence with the push that names them.
//
// Run it:
//   QUEEN_URL=http://localhost:6632 node 01-hello-world.mjs
//
// The program checks its own outcome and exits non-zero if a check fails.

import { Queen } from 'queen-mq'

const QUEEN_URL = process.env.QUEEN_URL || 'http://localhost:6632'

// The name is prefixed per language and suffixed per run, so every tutorial in
// every language can share one broker and no run inherits state from another.
const QUEUE = `tut-js-hello-${Date.now().toString(36)}`

let checks = 0
const assert = (condition, description) => {
  if (!condition) throw new Error(description)
  checks++
  console.log(`  ok: ${description}`)
}

// handleSignals: false leaves SIGINT and SIGTERM alone: this script owns its
// own shutdown, through close() at the bottom.
const queen = new Queen({ url: QUEEN_URL, handleSignals: false })

try {
  console.log(`broker ${QUEEN_URL}`)

  // A push names a queue and, optionally, a partition. Both are created by this
  // call if they do not exist, inside the transaction that stores the message.
  // There is no declare step and nothing to provision first.
  const [pushed] = await queen
    .queue(QUEUE)
    .push({ data: { greeting: 'Hello World!' } })

  console.log(`pushed ${pushed.transaction_id} -> ${pushed.status}`)
  assert(pushed.status === 'queued', 'the broker stored the message')

  // pop() takes messages under a lease: they are claimed until they are
  // acknowledged or the lease expires. wait(true) turns on long polling, so the
  // call parks until a message arrives instead of coming back empty.
  //
  // No consumer group is named here, so the read goes through the queue's own
  // cursor, which starts at the beginning. Named groups are tutorial 2: a group
  // created after a message was pushed starts at the tail and would see nothing
  // here.
  const messages = await queen
    .queue(QUEUE)
    .batch(1)
    .wait(true)
    .pop()

  assert(messages.length === 1, 'one message came back')
  const message = messages[0]
  console.log(`received "${message.data.greeting}" from partition ${message.partition}`)
  assert(message.data.greeting === 'Hello World!', 'the payload survived the round trip')

  // No partition was named on the push, so the broker put the message in the
  // queue's default lane.
  assert(message.partition === 'Default', 'it landed in the default partition')

  // The acknowledgement is what commits consumption. It moves the cursor past
  // the message and releases the lease. A rejected ack still arrives as HTTP
  // 200 with success: false on the item, so the per-item flag is the only proof
  // the broker took it.
  const ack = await queen.ack(message, true)
  assert(ack.success === true, 'the acknowledgement was accepted')

  // The cursor is now past the only message, so a further read finds nothing.
  // wait(false) returns immediately instead of long polling.
  const leftovers = await queen.queue(QUEUE).wait(false).pop()
  assert(leftovers.length === 0, 'the queue is drained')

  // Clean up on success only: a failed run leaves the queue on the broker to
  // be looked at.
  await queen.queue(QUEUE).delete()

  console.log(`\nPASS: ${checks} checks`)
} catch (err) {
  console.error(`\nFAIL: ${err.message}`)
  process.exitCode = 1
} finally {
  // close() flushes buffers and destroys the HTTP dispatcher. Without it the
  // keep-alive sockets hold the Node event loop open and the process hangs.
  await queen.close()
}
// docs:end
