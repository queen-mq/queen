// The kafkajs half of the M6 client matrix: PLAN_QUEEN_KAFKA.md's named client,
// driven against the same rig the franz-go suite uses (compat/rig.sh, 19092).
//
//   node run.mjs <scenario>   # produce | gzip | acks0 | roundtrip | group
//                             # rebalance | resume | all
//
// Every scenario prints the API versions kafkajs actually negotiated, taken out
// of kafkajs's own debug log rather than assumed: the log creator below parses
// the "Response <Api>(key: N, version: V)" lines the broker connection emits.
//
// KAFKA_BOOTSTRAP overrides the bootstrap address; nothing else is configurable
// on purpose, so a failure here is a failure of the facade and not of a knob.
import { Kafka, logLevel, CompressionTypes } from 'kafkajs'

const BOOTSTRAP = process.env.KAFKA_BOOTSTRAP || '127.0.0.1:19092'
const RUN = process.env.RUN_ID || String(Date.now())

// --------------------------------------------------------------- observation
const negotiated = new Map() // "Metadata" -> Set(versions)
const retriables = []        // the UNKNOWN_TOPIC_OR_PARTITION dance and friends
const problems = []          // anything the client called an error

const API_LINE = /^(Request|Response) ([A-Za-z]+)\(key: (\d+), version: (\d+)\)$/

function logCreator() {
  return ({ level, log }) => {
    const { message, error, retryCount, ...rest } = log
    const m = API_LINE.exec(message || '')
    if (m) {
      const [, dir, api, key, version] = m
      if (dir === 'Response') {
        if (!negotiated.has(api)) negotiated.set(api, new Set())
        negotiated.get(api).add(`v${version} (key ${key})`)
      }
      return
    }
    if (level === logLevel.ERROR || level === logLevel.WARN) {
      const line = `${level === logLevel.ERROR ? 'ERROR' : 'WARN '} ${message}` +
        (error ? ` :: ${error}` : '') +
        (retryCount !== undefined ? ` (retry ${retryCount})` : '')
      // kafkajs's own retry dance on a topic it just asked to be created is
      // expected; anything else is worth reading.
      if (/UNKNOWN_TOPIC_OR_PARTITION|not host this topic|no leader|LEADER_NOT_AVAILABLE|not available/i.test(line)) {
        retriables.push(line)
      } else {
        problems.push(line)
      }
    }
  }
}

const kafka = new Kafka({
  clientId: `kjs-${RUN}`,
  brokers: [BOOTSTRAP],
  logLevel: logLevel.DEBUG,
  logCreator,
  retry: { retries: 8, initialRetryTime: 200 },
})

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const ok = (msg) => console.log(`  ok   ${msg}`)
const info = (msg) => console.log(`  ..   ${msg}`)
function assert(cond, msg) {
  if (!cond) throw new Error(`ASSERT FAILED: ${msg}`)
  ok(msg)
}

async function waitFor(pred, what, timeoutMs = 90000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (pred()) return true
    await sleep(200)
  }
  throw new Error(`TIMEOUT waiting for ${what} after ${timeoutMs}ms`)
}

// ------------------------------------------------------------------ scenarios

// 1. acks=all to a topic that does not exist yet: the record lands and the
//    response carries a real base offset. The retry dance is reported, not
//    treated as a failure.
async function produce() {
  const topic = `js-produce-${RUN}`
  const producer = kafka.producer({ allowAutoTopicCreation: true })
  const t0 = Date.now()
  await producer.connect()
  const first = await producer.send({
    topic, acks: -1,
    messages: [{ key: 'k0', value: 'first-ever-record', partition: 0 }],
  })
  info(`first send to a brand-new topic took ${Date.now() - t0}ms, ` +
       `${retriables.length} retriable metadata errors on the way`)
  console.log(`  first response: ${JSON.stringify(first)}`)
  assert(first[0].errorCode === 0, 'acks=all first send errorCode 0')
  assert(String(first[0].baseOffset) === '0', 'first record on a fresh partition is offset 0')

  const batch = await producer.send({
    topic, acks: -1,
    messages: Array.from({ length: 10 }, (_, i) => ({
      key: `k${i}`, value: `v${i}`, partition: i % 4,
    })),
  })
  console.log(`  batch response: ${JSON.stringify(batch)}`)
  assert(batch.every((r) => r.errorCode === 0), 'acks=all batch every partition errorCode 0')
  const p0 = batch.find((r) => r.partition === 0)
  assert(String(p0.baseOffset) === '1', `partition 0 continues at offset 1 (got ${p0.baseOffset})`)
  await producer.disconnect()
  return topic
}

// 2. gzip. kafkajs ships GZIP natively (snappy/lz4/zstd are plugins), so this
//    is the one codec a stock kafkajs can prove.
async function gzip() {
  const topic = `js-gzip-${RUN}`
  const producer = kafka.producer()
  await producer.connect()
  const big = 'x'.repeat(4096)
  const res = await producer.send({
    topic, acks: -1, compression: CompressionTypes.GZIP,
    messages: Array.from({ length: 20 }, (_, i) => ({
      key: `g${i}`, value: `${i}:${big}`, partition: i % 8,
    })),
  })
  assert(res.every((r) => r.errorCode === 0), 'gzip produce: every partition errorCode 0')
  await producer.disconnect()

  const seen = new Map()
  const consumer = kafka.consumer({ groupId: `gzip-check-${RUN}` })
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ message }) => { seen.set(message.key.toString(), message.value.toString()) },
  })
  await waitFor(() => seen.size === 20, 'the 20 gzip records to come back')
  await consumer.disconnect()
  const bad = [...seen.entries()].filter(([k, v]) => v !== `${k.slice(1)}:${big}`)
  assert(bad.length === 0, 'gzip payloads decode byte-exact through the facade')
}

// 3. acks=0: no response frame exists, so kafkajs invents the offsets. What is
//    under test is that the connection does not stall and the records land.
async function acks0() {
  const topic = `js-acks0-${RUN}`
  const producer = kafka.producer()
  await producer.connect()
  // Create the topic first with an acks=all write: an acks=0 produce to a topic
  // that does not exist has no response to carry the error either.
  await producer.send({ topic, acks: -1, messages: [{ key: 'seed', value: 'seed', partition: 0 }] })
  const t0 = Date.now()
  const res = await producer.send({
    topic, acks: 0,
    messages: Array.from({ length: 5 }, (_, i) => ({ key: `z${i}`, value: `z${i}`, partition: 1 })),
  })
  const dt = Date.now() - t0
  console.log(`  acks=0 response (client-invented): ${JSON.stringify(res)}`)
  assert(dt < 5000, `acks=0 send returned promptly (${dt}ms), the connection did not stall`)
  // The connection is still usable afterwards, which is the real risk with a
  // no-response request in a serially dispatched connection.
  const after = await producer.send({ topic, acks: -1, messages: [{ key: 'after', value: 'after', partition: 1 }] })
  assert(after[0].errorCode === 0, 'the connection still serves an acks=all send afterwards')
  await producer.disconnect()

  const values = []
  const consumer = kafka.consumer({ groupId: `acks0-check-${RUN}` })
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({ eachMessage: async ({ partition, message }) => {
    if (partition === 1) values.push(message.value.toString())
  } })
  await waitFor(() => values.length === 6, 'the 5 acks=0 records plus the acks=all one')
  await consumer.disconnect()
  assert(JSON.stringify(values) === JSON.stringify(['z0', 'z1', 'z2', 'z3', 'z4', 'after']),
    `acks=0 records landed in order: ${JSON.stringify(values)}`)
}

// 4. headers + key round-trip, byte-exact, including the null/empty edges.
async function roundtrip() {
  const topic = `js-rt-${RUN}`
  const producer = kafka.producer()
  await producer.connect()
  const nonUtf8 = Buffer.from([0xff, 0xfe, 0x00, 0x41, 0x80])
  const messages = [
    { partition: 0, key: 'plain', value: 'hello', headers: { a: 'one', b: 'two' } },
    { partition: 0, key: Buffer.from('bin-key-ÿ', 'latin1'), value: nonUtf8, headers: { h: nonUtf8 } },
    { partition: 0, key: null, value: 'null-key', headers: {} },
    { partition: 0, key: 'empty-value', value: '' },
    { partition: 0, key: 'null-value', value: null },
    { partition: 0, key: 'dup-headers', value: 'dh', headers: { x: ['1', '2'] } },
    { partition: 0, key: 'big', value: 'B'.repeat(65536) },
  ]
  await producer.send({ topic, acks: -1, messages })
  await producer.disconnect()

  const got = []
  const consumer = kafka.consumer({ groupId: `rt-${RUN}` })
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({ eachMessage: async ({ message }) => { got.push(message) } })
  await waitFor(() => got.length === messages.length, `all ${messages.length} round-trip records`)
  await consumer.disconnect()

  const hex = (b) => (b === null || b === undefined ? String(b) : Buffer.from(b).toString('hex'))
  for (let i = 0; i < messages.length; i++) {
    const want = messages[i], have = got[i]
    const wk = want.key === null ? null : Buffer.from(want.key)
    assert(hex(have.key) === hex(wk), `record ${i} key byte-exact (${hex(wk)})`)
    const wv = want.value === null ? null : Buffer.from(want.value)
    assert(hex(have.value) === hex(wv), `record ${i} value byte-exact (${(wv || '').length ?? 0} bytes)`)
    const wantH = want.headers || {}
    const haveH = have.headers || {}
    for (const [k, v] of Object.entries(wantH)) {
      const wanted = Array.isArray(v) ? v.map((x) => hex(Buffer.from(x))).join(',') : hex(Buffer.from(v))
      const hv = haveH[k]
      const found = Array.isArray(hv) ? hv.map((x) => hex(x)).join(',') : hex(hv)
      // A repeated header NAME is legal Kafka and is asserted like any other:
      // it used to be lost between the producer and the log (the decoder
      // collapsed the header list into a map), and queen-kafka's src/wire.rs
      // is what carries both values now. kafkajs hands a repeat back as an
      // array, which is why `wanted` joins one.
      assert(found === wanted, `record ${i} header ${k} byte-exact`)
    }
    assert(Object.keys(haveH).length === Object.keys(wantH).length,
      `record ${i} header count ${Object.keys(haveH).length}`)
    assert(String(have.offset) === String(i), `record ${i} at offset ${i}`)
    assert(typeof have.timestamp === 'string' && Number(have.timestamp) > 0,
      `record ${i} carries a timestamp (${have.timestamp})`)
  }
}

// 5. one consumer group, end to end, over every partition.
async function group() {
  const topic = `js-group-${RUN}`
  const N = 80
  const producer = kafka.producer()
  await producer.connect()
  await producer.send({
    topic, acks: -1,
    messages: Array.from({ length: N }, (_, i) => ({ key: `k${i}`, value: `v${i}`, partition: i % 8 })),
  })
  await producer.disconnect()

  const seen = new Map()
  const parts = new Set()
  const consumer = kafka.consumer({ groupId: `g-${RUN}`, sessionTimeout: 30000 })
  const t0 = Date.now()
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({ eachMessage: async ({ partition, message }) => {
    parts.add(partition)
    const k = message.key.toString()
    if (seen.has(k)) throw new Error(`duplicate delivery of ${k}`)
    seen.set(k, message.value.toString())
  } })
  await waitFor(() => seen.size === N, `all ${N} records through the group`)
  info(`group formed and drained ${N} records in ${Date.now() - t0}ms (3s of that is the join window)`)
  assert(parts.size === 8, `every one of the 8 partitions was read (${[...parts].sort((a, b) => a - b)})`)
  assert([...seen.entries()].every(([k, v]) => v === `v${k.slice(1)}`), 'every value matches its key')
  await consumer.disconnect()
}

// 6. two members of one group split the partitions and reassign when one dies.
//    The split is read off the ASSIGNMENT each member is handed (kafkajs's
//    GROUP_JOIN event) and not off which partitions each happened to read: a
//    member that was alone for one generation legitimately read everything.
async function rebalance() {
  const topic = `js-rebal-${RUN}`
  const groupId = `gr-${RUN}`
  const producer = kafka.producer()
  await producer.connect()
  await producer.send({ topic, acks: -1, messages: [{ key: 'seed', value: 'seed', partition: 0 }] })
  await producer.disconnect()

  const assigned = {}          // name -> [partitions] from the latest GROUP_JOIN
  const joins = { a: 0, b: 0 } // how many generations each member has seen
  const read = { a: new Set(), b: new Set() }
  const seen = new Map()
  let counting = false
  const consume = async (name) => {
    const c = kafka.consumer({ groupId, sessionTimeout: 30000, heartbeatInterval: 3000 })
    c.on(c.events.GROUP_JOIN, ({ payload }) => {
      const parts = (payload.memberAssignment || {})[topic] || []
      assigned[name] = parts
      joins[name]++
    })
    await c.connect()
    await c.subscribe({ topic, fromBeginning: true })
    await c.run({ eachMessage: async ({ partition, message }) => {
      if (!counting) return
      read[name].add(partition)
      const k = message.key.toString()
      seen.set(k, (seen.get(k) || 0) + 1)
    } })
    return c
  }
  const a = await consume('a')
  const b = await consume('b')
  const split = () => {
    const pa = assigned.a, pb = assigned.b
    if (!pa || !pb) return false
    const overlap = pa.filter((p) => pb.includes(p))
    return overlap.length === 0 && pa.length + pb.length === 8 && pa.length > 0 && pb.length > 0
  }
  await waitFor(split, 'both members to hold a disjoint half of the 8 partitions', 120000)
  info(`assignment: A=[${assigned.a.sort((x, y) => x - y)}] B=[${assigned.b.sort((x, y) => x - y)}] ` +
       `(A joined ${joins.a} generation(s), B ${joins.b})`)
  ok('the coordinator split 8 partitions across two members with no overlap')

  // Only now start counting, so every record below is produced into a stable
  // two-member group.
  counting = true
  const N = 80
  const p2 = kafka.producer()
  await p2.connect()
  await p2.send({
    topic, acks: -1,
    messages: Array.from({ length: N }, (_, i) => ({ key: `k${i}`, value: `v${i}`, partition: i % 8 })),
  })
  await p2.disconnect()
  await waitFor(() => seen.size === N, `all ${N} records across two members`)
  const dupes = [...seen.entries()].filter(([, n]) => n > 1)
  info(`member A read [${[...read.a].sort()}], member B read [${[...read.b].sort()}]`)
  assert(dupes.length === 0, `no record delivered twice (${dupes.length} dupes)`)
  const overlap = [...read.a].filter((p) => read.b.has(p))
  assert(overlap.length === 0, `no partition was read by both members (overlap ${JSON.stringify(overlap)})`)

  // Kill member B; A must take the whole topic and reach records produced after.
  await b.disconnect()
  const producer3 = kafka.producer()
  await producer3.connect()
  await producer3.send({
    topic, acks: -1,
    messages: Array.from({ length: 16 }, (_, i) => ({ key: `post${i}`, value: `p${i}`, partition: i % 8 })),
  })
  await producer3.disconnect()
  await waitFor(() => [...seen.keys()].filter((k) => k.startsWith('post')).length === 16,
    'the survivor to reach all 16 records produced after the rebalance', 180000)
  info(`after B left, A was assigned [${(assigned.a || []).sort((x, y) => x - y)}] ` +
       `over ${joins.a} generations`)
  assert(assigned.a.length === 8, 'the survivor was reassigned all 8 partitions')
  const postDupes = [...seen.entries()].filter(([k, n]) => k.startsWith('post') && n > 1)
  assert(postDupes.length === 0, `no post-rebalance record delivered twice (${postDupes.length})`)
  await a.disconnect()
}

// 7. commit, stop, restart: the group resumes from what it committed and not
//    from fromBeginning.
async function resume() {
  const topic = `js-resume-${RUN}`
  const groupId = `res-${RUN}`
  const producer = kafka.producer()
  await producer.connect()
  await producer.send({
    topic, acks: -1,
    messages: Array.from({ length: 40 }, (_, i) => ({ key: `a${i}`, value: `a${i}`, partition: i % 8 })),
  })
  await producer.disconnect()

  const first = []
  const c1 = kafka.consumer({ groupId, sessionTimeout: 30000 })
  await c1.connect()
  await c1.subscribe({ topic, fromBeginning: true })
  await c1.run({ autoCommit: false, eachMessage: async ({ topic: t, partition, message }) => {
    first.push(message.key.toString())
    await c1.commitOffsets([{ topic: t, partition, offset: String(Number(message.offset) + 1) }])
  } })
  await waitFor(() => first.length === 40, 'the first consumer to read and commit all 40')
  await c1.disconnect()
  ok(`first consumer read ${first.length} records and committed each one explicitly`)

  const producer2 = kafka.producer()
  await producer2.connect()
  await producer2.send({
    topic, acks: -1,
    messages: Array.from({ length: 24 }, (_, i) => ({ key: `b${i}`, value: `b${i}`, partition: i % 8 })),
  })
  await producer2.disconnect()

  const second = []
  const c2 = kafka.consumer({ groupId, sessionTimeout: 30000 })
  await c2.connect()
  // fromBeginning: true is the trap — a committed offset must win over it.
  await c2.subscribe({ topic, fromBeginning: true })
  await c2.run({ eachMessage: async ({ message }) => { second.push(message.key.toString()) } })
  await waitFor(() => second.length >= 24, 'the restarted consumer to read the 24 new records')
  await sleep(3000) // give it a chance to wrongly redeliver the first 40
  await c2.disconnect()
  const replayed = second.filter((k) => k.startsWith('a'))
  assert(replayed.length === 0, `the restarted consumer replayed nothing (${replayed.length} old records)`)
  assert(second.length === 24, `the restarted consumer read exactly the 24 new records (got ${second.length})`)
}

// 8. the auto-create dance from the CONSUMER side: subscribing to a topic that
//    has never existed. kafkajs is documented to retry UNKNOWN_TOPIC_OR_PARTITION
//    here, so what is being measured is how long the dance lasts and whether it
//    ends with the consumer reading.
async function freshtopic() {
  const topic = `js-fresh-${RUN}-${Math.floor(Math.random() * 1e6)}`
  const before = retriables.length
  const consumer = kafka.consumer({ groupId: `fresh-${RUN}`, allowAutoTopicCreation: true })
  const got = []
  const t0 = Date.now()
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({ eachMessage: async ({ message }) => { got.push(message.value.toString()) } })
  info(`subscribe+run on a never-seen topic returned after ${Date.now() - t0}ms, ` +
       `${retriables.length - before} retriable errors`)
  const producer = kafka.producer()
  await producer.connect()
  await producer.send({ topic, acks: -1, messages: [{ partition: 0, key: 'f', value: 'fresh' }] })
  await producer.disconnect()
  await waitFor(() => got.length === 1, 'the record produced to the just-created topic', 60000)
  assert(got[0] === 'fresh', 'the consumer read from a topic that did not exist when it subscribed')
  info(`the whole dance cost ${retriables.length - before} retriable errors`)
  await consumer.disconnect()
}

// ---------------------------------------------------------------------- main
const SCENARIOS = { produce, gzip, acks0, roundtrip, group, rebalance, resume, freshtopic }

const which = process.argv[2] || 'all'
const list = which === 'all' ? Object.keys(SCENARIOS) : [which]
let failed = 0
for (const name of list) {
  if (!SCENARIOS[name]) { console.error(`no scenario ${name}`); process.exit(2) }
  console.log(`\n=== ${name}`)
  try {
    await SCENARIOS[name]()
    console.log(`=== ${name}: PASS`)
  } catch (e) {
    failed++
    console.log(`=== ${name}: FAIL — ${e.stack || e}`)
  }
}

console.log('\n--- negotiated API versions (from kafkajs response frames)')
for (const [api, vs] of [...negotiated.entries()].sort()) {
  console.log(`  ${api}: ${[...vs].join(', ')}`)
}
console.log(`--- retriable/auto-create-dance log lines: ${retriables.length}`)
for (const l of retriables.slice(0, 12)) console.log(`  ${l}`)
console.log(`--- other client-reported problems: ${problems.length}`)
for (const l of problems.slice(0, 20)) console.log(`  ${l}`)
console.log(`\nRESULT: ${failed === 0 ? 'PASS' : `FAIL (${failed} scenario(s))`}`)
process.exit(failed === 0 ? 0 : 1)
