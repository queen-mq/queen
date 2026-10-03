// docs:start(app-js-chat)
//
// A chat backend: one ordered partition per conversation.
//
// Queen started as the broker of a hotel messaging product. Some conversations
// need a translation or an agent before their next message can be handled, and
// on a hashed Kafka topic one slow conversation held up every conversation that
// shared its partition. Here every conversation is a partition of its own,
// created by the first message sent to it, so a slow conversation waits on
// itself and on nothing else.
//
//   chat-messages (one partition per conversation)
//     ├── group "delivery"    marks each message delivered, fast
//     ├── group "enrichment"  translates the Japanese conversation, slow
//     └── group "sentiment"   added later, reads the whole history
//
// The program checks what the design promises: every message reaches each
// group once and in the order of its conversation, and the English
// conversations finish while the Japanese one is still being translated.
//
// Run it:
//   QUEEN_URL=http://localhost:6632 node chat.mjs

import { Queen } from 'queen-mq'

const QUEEN_URL = process.env.QUEEN_URL || 'http://localhost:6632'
// A fresh queue per run, so two runs never read each other's messages.
const RUN = Date.now().toString(36)
const MESSAGES = `app-js-chat-${RUN}`

// Three conversations. The Japanese one needs a translation pass, 400 ms a
// message against 10 ms for the others. It is listed first, so its messages are
// the oldest in the queue and its partition is usually handed out first: a
// consumer that let one conversation hold up another would fail the timing
// check below.
const CONVERSATIONS = {
  'conv-jp-1': { locale: 'jp', needsTranslation: true },
  'conv-en-1': { locale: 'en', needsTranslation: false },
  'conv-en-2': { locale: 'en', needsTranslation: false },
}
const MESSAGES_PER_CONVERSATION = 6

let checks = 0
const assert = (condition, description) => {
  if (!condition) throw new Error(description)
  checks++
  console.log(`  ok: ${description}`)
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms))

const queen = new Queen({ url: QUEEN_URL, handleSignals: false })

// Three workers in one consumer group. partitions(1) makes every pop take ONE
// conversation: by default a pop may sweep up several ready conversations, and a
// worker handles the messages of one pop in order, so a slow conversation would
// delay the others that came with it. Long polls end after a second and a
// worker stops after two quiet seconds, which is what lets this program finish;
// a service calls consume() without those two lines and runs until stopped.
const workers = (group) => queen
  .queue(MESSAGES)
  .group(group)
  .subscriptionMode('all') // a group created after the messages starts at the tail otherwise
  .concurrency(3)
  .partitions(1)
  .each()
  .timeoutMillis(1000)
  .idleMillis(2000)

try {
  console.log(`broker ${QUEEN_URL}`)

  // A crashed worker's messages come back when its lease expires, and
  // retryLimit bounds how often a failing message is retried before it goes to
  // the dead-letter queue.
  await queen.queue(MESSAGES).config({ leaseTime: 60, retryLimit: 3 }).create()

  // ---------------------------------------------------------------- sending
  //
  // Sending a message is one push into the conversation's partition. Nothing
  // was declared for the conversation beforehand, and nothing has to be cleaned
  // up when it goes quiet.
  console.log('\nsending')
  let sent = 0
  for (let seq = 1; seq <= MESSAGES_PER_CONVERSATION; seq++) {
    for (const [conversationId, meta] of Object.entries(CONVERSATIONS)) {
      await queen.queue(MESSAGES).partition(conversationId).push({
        // The phone's own id for the message. A phone that retries a send it
        // never saw answered writes nothing the second time.
        transactionId: `${conversationId}-${seq}`,
        data: { conversationId, seq, locale: meta.locale, body: `message ${seq} in ${conversationId}` },
      })
      sent++
    }
  }
  console.log(`  ${sent} messages across ${Object.keys(CONVERSATIONS).length} conversations`)

  // The phone resends message 1 because the answer got lost on a bad network.
  const [resent] = await queen.queue(MESSAGES).partition('conv-en-1').push({
    transactionId: 'conv-en-1-1',
    data: { conversationId: 'conv-en-1', seq: 1, body: 'resent by the phone' },
  })
  assert(resent.status === 'duplicate', 'a resent message was recognised and not stored twice')

  // ------------------------------------------------------------- delivering
  //
  // Marking messages delivered is fast work and must never wait behind slow
  // work, so it is a consumer group of its own, with its own cursor.
  console.log('\ndelivering')
  const delivered = new Map()
  await workers('delivery').consume(async (msg) => {
    await sleep(10)
    const seqs = delivered.get(msg.data.conversationId) ?? []
    seqs.push(msg.data.seq)
    delivered.set(msg.data.conversationId, seqs)
  })

  const deliveredCount = [...delivered.values()].reduce((n, seqs) => n + seqs.length, 0)
  assert(deliveredCount === sent, `delivery saw all ${sent} messages once (got ${deliveredCount})`)
  for (const [conversationId, seqs] of delivered) {
    assert(
      seqs.every((seq, i) => seq === i + 1),
      `${conversationId} was delivered in order: ${seqs.join(',')}`
    )
  }

  // ------------------------------------------------------------- enrichment
  //
  // The slow group reads the same messages through its own cursor. On a topic
  // with a few hashed partitions, the Japanese conversation would sit in a
  // partition shared with English ones and hold them up. Here each worker holds
  // one conversation at a time, so the English conversations finish while the
  // Japanese one is still being translated.
  console.log('\nenriching')
  const finishedAt = new Map()
  const started = Date.now()
  await workers('enrichment').consume(async (msg) => {
    const { needsTranslation } = CONVERSATIONS[msg.data.conversationId]
    await sleep(needsTranslation ? 400 : 10)
    finishedAt.set(msg.data.conversationId, Date.now() - started)
  })

  const slow = finishedAt.get('conv-jp-1')
  const fast = Math.max(finishedAt.get('conv-en-1'), finishedAt.get('conv-en-2'))
  console.log(`  english done after ${fast} ms, japanese after ${slow} ms`)
  assert(fast < slow, 'the English conversations finished while the Japanese one was still being translated')
  assert(slow >= MESSAGES_PER_CONVERSATION * 400, 'the Japanese conversation really took its six translations')

  // --------------------------------------------------------------- backfill
  //
  // A feature added later, sentiment scoring, wants every message ever sent. It
  // is one more consumer group starting from the oldest message: no producer
  // change, and no second copy of the data.
  console.log('\nbackfilling a new group')
  let scored = 0
  await workers('sentiment').consume(async () => { scored++ })
  assert(scored === sent, `a group created now read the whole history (${scored} messages)`)

  await queen.queue(MESSAGES).delete()
  console.log(`\nPASS: ${checks} checks`)
} catch (err) {
  console.error(`\nFAIL: ${err.message}`)
  process.exitCode = 1
} finally {
  await queen.close()
}
// docs:end
