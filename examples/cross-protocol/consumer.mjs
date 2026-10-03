// docs:start(app-js-cross-consumer)
// Cross-protocol load pair, CONSUMER half: read, through Queen's own API, what
// producer.mjs writes over the Kafka wire protocol, forever.
//
//   npm install
//   TOPIC=orders node consumer.mjs     # the same TOPIC as the producer
//
// A Kafka topic is a Queen queue, and Kafka partition n is the Queen partition
// named "n", so nothing converts between the two. The facade stores each record
// as JSON with base64 bytes, because a Kafka key or value is arbitrary bytes:
//   { k: key, v: value, h: [{ k: name, v: value }], t: timestamp }
// h is left out when there are no headers and t when there is no timestamp.
// More on the mapping: https://queenmq.com/guides/kafka/

import { Queen } from 'queen-mq'

// The same variable, and the same default, as producer.mjs, so one TOPIC moves
// both halves.
const TOPIC = process.env.TOPIC ?? 'cross-topic'
const GROUP = process.env.GROUP ?? 'queen-side-group'
const QUEEN = process.env.QUEEN ?? 'http://localhost:6632'
const LOG_EVERY = 10000

const queen = new Queen(QUEEN)

function decode (data) {
  return {
    key: data.k == null ? null : Buffer.from(data.k, 'base64').toString(),
    value: data.v == null ? null : Buffer.from(data.v, 'base64').toString(),
    headers: (data.h ?? []).map(h => ({
      name: h.k,
      value: h.v == null ? null : Buffer.from(h.v, 'base64').toString(),
    })),
    timestamp: data.t ?? null,
  }
}

let count = 0
let nextLog = LOG_EVERY
let shownEnvelope = false
const startTime = Date.now()

console.log(`consuming ${TOPIC} as group ${GROUP} via ${QUEEN} (pid ${process.pid})`)

await queen.queue(TOPIC)
  .group(GROUP)
  // The Queen equivalent of fromBeginning: a new group otherwise starts at the
  // messages that arrive from now on.
  .subscriptionMode('all')
  .concurrency(16)
  // consume() hands the handler an array and acks the whole batch in one call;
  // .each() would hand it one message at a time, with one ack each.
  .consume(async (messages) => {
    // Show the stored payload once, from the first batch this group reads. (A
    // throwaway group used just to peek would keep a cursor that never moves,
    // and completed-message retention never reclaims past the slowest group.)
    if (!shownEnvelope) {
      shownEnvelope = true
      console.log('raw Queen payload as stored by the Kafka facade:')
      console.log(' ', JSON.stringify(messages[0].data))
      console.log('  decoded:', JSON.stringify(decode(messages[0].data)), '\n')
    }

    count += messages.length

    // A threshold, never `count % N === 0`: batches are sized by the broker,
    // so the counter moves in irregular jumps and would skip the multiples.
    if (count >= nextLog) {
      nextLog += LOG_EVERY
      const secs = (Date.now() - startTime) / 1000
      console.log(new Date().toISOString(), 'consumed', count, Math.round(count / secs), 'msg/s avg')
    }
  })
// docs:end
