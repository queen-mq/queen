// docs:start(app-js-cross-producer)
// Cross-protocol load pair, PRODUCER half: produce over the Kafka wire protocol
// as fast as librdkafka will batch, forever. Its partner, consumer.mjs, reads the
// same messages through Queen's own API. Nothing is copied between them: a Kafka
// topic is a Queen queue. For a self-checking version of the same idea, see
// bridge.mjs in this directory.
//
//   npm install
//   TOPIC=orders PARTITIONS=32 node producer.mjs
//   BROKER=localhost:9092 TOPIC=orders node producer.mjs
//
// Producer and consumer are separate processes on purpose: in one Node process
// they compete for one event loop, and each one's rate is really a measure of
// the pair.
//
// A node with the Kafka listener on:
//   docker run -d -p 6632:6632 -p 9092:9092 -e QUEEN_KAFKA_EMBEDDED=true \
//     -e QUEEN_KAFKA_ADVERTISED_ADDR=localhost:9092 ghcr.io/queen-mq/queen:latest

import Confluent from '@confluentinc/kafka-javascript'

const TOPIC = process.env.TOPIC ?? 'cross-topic'
const BROKER = process.env.BROKER ?? 'localhost:9092'

// The width declared for TOPIC with CreateTopics. It has to be declared before
// anything produces to the topic, because a producer's auto-create would make
// it at the broker's default width first. How the facade advertises and stores
// a topic's width is on https://queenmq.com/guides/kafka/.
const PARTITIONS = Number(process.env.PARTITIONS ?? 1000)

const { KafkaJS } = Confluent

const kafka = new KafkaJS.Kafka({
  'bootstrap.servers': BROKER,
  kafkaJS: { clientId: 'cross-producer' },
})

// Declare the width before a single record exists, then report the width the
// broker advertises, which is what the consumer half sees as Queen partitions
// named "0" to "N-1".
async function declareWidth () {
  const admin = kafka.admin()
  await admin.connect()
  try {
    const created = await admin.createTopics({
      topics: [{ topic: TOPIC, numPartitions: PARTITIONS, replicationFactor: 1 }],
    })
    // This client returns the topic list itself, where kafkajs wraps it in
    // { topics: [...] }.
    const [described] = await admin.fetchTopicMetadata({ topics: [TOPIC] })
    const width = described.partitions.length
    console.log(
      created
        ? `created ${TOPIC} declaring ${PARTITIONS} partitions, advertised at ${width}`
        : `${TOPIC} already existed with ${width} partitions; the declared ${PARTITIONS} was not applied`
    )
    return width
  } finally {
    await admin.disconnect()
  }
}

// One message per send, never awaited individually, so librdkafka's per-partition
// accumulator can batch across them. linger.ms is what makes that pay.
const producer = kafka.producer({
  kafkaJS: { acks: -1, idempotent: false, allowAutoTopicCreation: true },
  'linger.ms': 1,
})

const WINDOW = 10000
const LOG_EVERY = 10000

let sent = 0
let nextLog = LOG_EVERY
const startTime = Date.now()

process.on('SIGINT', async () => {
  console.log('flushing...')
  await producer.flush({ timeout: 10000 }).catch(() => {})
  await producer.disconnect().catch(() => {})
  process.exit(0)
})

await declareWidth()

await producer.connect()
console.log(`producing to ${TOPIC} via ${BROKER} (pid ${process.pid})`)

while (true) {
  const inflight = []
  for (let k = 0; k < WINDOW; k++) {
    inflight.push(producer.send({ topic: TOPIC, messages: [{ value: 'mex-' + (sent + k) }] }))
  }
  await Promise.all(inflight)
  sent += WINDOW

  // A threshold, never `sent % N === 0`: the counter advances in batch-sized
  // jumps and would step straight over the multiples.
  if (sent >= nextLog) {
    nextLog += LOG_EVERY
    const secs = (Date.now() - startTime) / 1000
    console.log(new Date().toISOString(), 'produced', sent, Math.round(sent / secs), 'msg/s avg')
  }
}
// docs:end
