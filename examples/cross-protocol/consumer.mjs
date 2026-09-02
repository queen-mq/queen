// docs:start(app-js-cross-consumer)
// Cross-protocol example, CONSUMER half.
//
//   npm install
//   node consumer.mjs                  # foreground
//   node consumer.mjs &                # detached from this shell
//
//   TOPIC=orders node consumer.mjs     # must match the producer's TOPIC
//
// Reads, through Queen's OWN API on 6632, the rows a Kafka producer wrote
// through the facade on 9092. Two translations make that work:
//
// 1. A Kafka topic IS a Queen queue, and the Kafka partition INDEX is the Queen
//    partition NAME in decimal. Nothing converts; they are the same rows.
//
// 2. The facade wraps each record in an envelope, base64 in the byte slots,
//    because a Queen payload is JSON and a Kafka key/value is arbitrary bytes:
//      { k: <base64 key|null>, v: <base64 value|null>,
//        h: [{ k: name, v: <base64> }],   // omitted when there are none
//        t: <producer timestamp ms> }     // omitted when unset

import { Queen } from 'queen-mq'

// Read from the environment, exactly as cross-producer.mjs does, so that ONE
// variable moves both halves. They were separately hardcoded before, which is a
// good way to spend ten minutes watching a consumer "hang" on an empty topic
// while the producer fills a different one.
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
  // messages arriving from now on.
  .subscriptionMode('all')
  .concurrency(16)
  // consume() hands the handler an ARRAY and acks the whole batch in one call.
  // .each() switches to per-message, which is one ack round trip each.
  .consume(async (messages) => {
    // Show the raw envelope once, from the first REAL batch. Doing this with a
    // throwaway consumer group instead would register a cursor that never
    // advances -- and retention rule 2 is capped at MIN(committed) across a
    // partition's consumer rows, so that idle group would pin the whole backlog
    // from ever being reclaimed (server/sql/procedures/006_log_maintenance.sql).
    if (!shownEnvelope) {
      shownEnvelope = true
      console.log('raw Queen payload as stored by the Kafka facade:')
      console.log(' ', JSON.stringify(messages[0].data))
      console.log('  decoded:', JSON.stringify(decode(messages[0].data)), '\n')
    }

    count += messages.length

    // A threshold, never `count % N === 0`: the pop autopilot sizes each batch
    // from live queue state, so the counter advances in irregular jumps and
    // would step straight over the multiples -- going silent for minutes while
    // consuming perfectly well.
    if (count >= nextLog) {
      nextLog += LOG_EVERY
      const secs = (Date.now() - startTime) / 1000
      console.log(new Date().toISOString(), 'consumed', count, Math.round(count / secs), 'msg/s avg')
    }
  })
// docs:end
