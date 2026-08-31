// What happens to a record carrying the SAME header name twice — legal Kafka
// (the header list is a list, and KIP-82 allows repeats), and the one place
// kafkajs's round-trip used to lose a value: the facade's decoder collapsed the
// header list into a map, so `x` arrived once. protocols/queen-kafka/src/wire.rs carries
// both now, and `run.mjs roundtrip` asserts it; this stays as the one-record
// probe that PRINTS what came back, for looking at a live facade by hand.
import { Kafka, logLevel } from 'kafkajs'

const kafka = new Kafka({
  clientId: 'dup-probe', brokers: [process.env.KAFKA_BOOTSTRAP || '127.0.0.1:19092'],
  logLevel: logLevel.NOTHING,
})
const topic = `js-dup-${Date.now()}`
const producer = kafka.producer()
await producer.connect()
await producer.send({
  topic, acks: -1,
  messages: [{ partition: 0, key: 'dup', value: 'dh', headers: { x: ['1', '2'], y: 'solo' } }],
})
await producer.disconnect()

const consumer = kafka.consumer({ groupId: `dup-${Date.now()}` })
await consumer.connect()
await consumer.subscribe({ topic, fromBeginning: true })
let done
const got = new Promise((r) => { done = r })
await consumer.run({ eachMessage: async ({ message }) => done(message) })
const m = await got
console.log('sent headers   : { x: ["1","2"], y: "solo" }')
console.log('received       :', JSON.stringify(m.headers, (k, v) =>
  (v && v.type === 'Buffer' ? Buffer.from(v.data).toString() : v)))
await consumer.disconnect()
