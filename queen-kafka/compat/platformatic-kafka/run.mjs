// @platformatic/kafka against the queen-kafka facade.
//
//   node run.mjs [bootstrap] [runId] [scenario]
//
// Default bootstrap 127.0.0.1:19092, default runId the epoch seconds, default
// scenario `all`. Every topic and group name carries the runId so reruns never
// collide (plt-<scenario>-<runId>).
//
// WHAT THIS SUITE PROVES
//
// @platformatic/kafka is a pure-JavaScript client (no librdkafka, no JNI) that
// ships every API version from the wire spec as its own module and picks one
// per API at runtime: clients/base/base.js kGetApi walks DOWN from the
// maxVersion the broker advertised in ApiVersions until it finds a version it
// implements. That makes it the single best probe of the facade's advertised
// ceiling in versions.rs, because whatever ceiling the facade names is exactly
// what this client will send. The suite prints the (apiKey, apiVersion) pairs
// the client actually put on the wire, read out of the client's OWN
// instrumentation — the node:diagnostics_channel tracing channel
// `plt:kafka:connections:api`, whose start event carries apiKey/apiVersion
// straight out of Connection#send (network/connection.js). Nothing is assumed.
//
// Covered: auto-create on first produce; 512 records over 8 partitions with
// keys and headers; acks=0, the produce with no response frame at all; all four
// codecs the client can compress with (gzip, snappy, lz4, zstd — snappy and lz4
// come from @platformatic/wasm-utils, zstd from node:zlib); byte-exact
// key/value/header round-trip including binary, empty, null and REPEATED header
// names; a consumer group draining every partition in order; commit + restart in
// the same group; earliest/latest listOffsets, listCommittedOffsets, getLag and
// a manual-offset seek; two members splitting the partitions and surviving one
// leaving; and an optional SASL/PLAIN (+TLS) lane.
//
// THE REBALANCE SETTLE WINDOW, AND WHY IT IS NOT THE FACADE'S
//
// With REBALANCE_SETTLE_MS=0 the rebalance scenario fails: the member that was
// ALONE in the previous generation keeps delivering records from the four
// partitions it just lost, for a few hundred milliseconds after its new
// assignment arrives. That reproduces IDENTICALLY against a real Apache Kafka
// 3.9.1 broker (2 of 3 runs, same shape: A reads all 8, B reads its 4), so it is
// a client-side window between SyncGroup landing and the MessagesStream
// re-pointing its fetches — not something the facade does. The default 4s settle
// measures steady-state ownership instead, which is what the facade owns.
//
// WHAT THE FACADE'S SURFACE COSTS THIS CLIENT
//
//  - Admin.createTopics / deleteTopics / listGroups WORK since M7 F1/F2. The
//    facade advertises 21 API keys now and the topics-admin trio is on the
//    list, so the `metadata` scenario creates a topic and deletes it again.
//    Before M7 this client raised UnsupportedApiError off ApiVersions without
//    sending a byte, and that refusal is what this file used to assert. One
//    deliberate deviation survives the change: a create's `partitions` is
//    accepted and NOT acted on, because Queen declares no per-topic width.
//  - `idempotent: true` needs InitProducerId (API key 22), advertised v0..4
//    since M7 F3. The `idempotent` scenario measures it.
//  - The client requires Node >= 22.22.0 || >= 24.6.0 (package.json engines).
//    On an older runtime npm only warns, and the zstd codec in particular needs
//    node:zlib zstd support. run.sh picks a satisfying node when it can find one.
//
// This file starts NOTHING. The stack is compat/rig.sh's job, or yours.

import {
  Producer,
  Consumer,
  Admin,
  ProduceAcks,
  CompressionAlgorithms,
  MessagesStreamModes,
  MessagesStreamFallbackModes,
  SASLMechanisms,
  protocolAPIsById,
  stringSerializers,
  stringDeserializers
} from '@platformatic/kafka'
import { tracingChannel } from 'node:diagnostics_channel'
import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'

// --------------------------------------------------------------------- config
const BOOTSTRAP = process.argv[2] || process.env.KAFKA_BOOTSTRAP || '127.0.0.1:19092'
const RUN = process.argv[3] || process.env.RUN_ID || String(Math.floor(Date.now() / 1000))
const ONLY = process.argv[4] || process.env.SCENARIO || 'all'
const PARTITIONS = Number(process.env.KAFKA_PARTITIONS || 8)
// How long an assignment must hold still before the rebalance scenario starts
// counting deliveries. See the note in rebalance().
const SETTLE_MS = Number(process.env.REBALANCE_SETTLE_MS || 4000)

// The SASL lane is opt-in: without KAFKA_SASL_BOOTSTRAP it is skipped with a note.
const SASL_BOOTSTRAP = process.env.KAFKA_SASL_BOOTSTRAP || ''
const SASL_TOKEN = process.env.KAFKA_SASL_TOKEN || ''
const SASL_TLS = (process.env.KAFKA_SASL_PROTOCOL || 'sasl_ssl') !== 'sasl_plaintext'
const SASL_CA = process.env.KAFKA_SSL_CA || ''
const SASL_INSECURE = process.env.KAFKA_SSL_INSECURE === '1'

const CLIENT_VERSION = (() => {
  try {
    const require = createRequire(import.meta.url)
    return require('@platformatic/kafka/package.json').version
  } catch {
    try {
      return JSON.parse(readFileSync(new URL('./node_modules/@platformatic/kafka/package.json', import.meta.url), 'utf8')).version
    } catch {
      return 'unknown'
    }
  }
})()

// ---------------------------------------------------------------- observation
// The client's own instrumentation. Connection#send publishes apiKey/apiVersion
// on this tracing channel for EVERY request, so this is what went on the wire.
const negotiated = new Map() // apiKey -> Set(version)
tracingChannel('plt:kafka:connections:api').subscribe({
  start (event) {
    if (typeof event?.apiKey !== 'number') return
    if (!negotiated.has(event.apiKey)) negotiated.set(event.apiKey, new Set())
    negotiated.get(event.apiKey).add(event.apiVersion)
  }
})

function printNegotiated () {
  section('API versions this client actually put on the wire')
  const rows = [...negotiated.entries()].sort((a, b) => a[0] - b[0])
  for (const [key, versions] of rows) {
    const name = protocolAPIsById[key] ?? `Api${key}`
    console.log(`  wire ${String(key).padStart(2)} ${name.padEnd(18)} v${[...versions].sort((a, b) => a - b).join(', v')}`)
  }
  if (!rows.length) console.log('  wire (nothing recorded — no connection was made)')
}

// ------------------------------------------------------------------- harness
let failures = 0
const section = (t) => console.log(`\n=== ${t}`)
const ok = (m) => console.log(`  ok   ${m}`)
const info = (m) => console.log(`  ..   ${m}`)
function bad (m) {
  failures++
  console.log(`  FAIL ${m}`)
}
function check (cond, m) {
  cond ? ok(m) : bad(m)
  return !!cond
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

// macOS has no timeout(1) and a hang is a result, so every blocking call in this
// file goes through one of these two deadlines.
function deadline (promise, ms, what) {
  let timer
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`TIMEOUT: ${what} did not settle in ${ms}ms`)), ms)
    })
  ]).finally(() => clearTimeout(timer))
}

// Drain a MessagesStream with the 'data' event until `isDone()` or the deadline.
function consumeUntil (stream, onMessage, isDone, ms, what) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup()
      reject(new Error(`TIMEOUT waiting for ${what} after ${ms}ms`))
    }, ms)
    const onData = (message) => {
      try {
        onMessage(message)
        if (isDone()) {
          cleanup()
          resolve()
        }
      } catch (error) {
        cleanup()
        reject(error)
      }
    }
    const onError = (error) => {
      cleanup()
      reject(error)
    }
    function cleanup () {
      clearTimeout(timer)
      stream.off('data', onData)
      stream.off('error', onError)
    }
    stream.on('data', onData)
    stream.on('error', onError)
  })
}

// Keep reading for `ms` more, collecting whatever arrives. Used to prove that
// nothing EXTRA shows up (a replay, a duplicate) rather than only that the
// expected records did.
function drainFor (stream, onMessage, ms) {
  return new Promise((resolve) => {
    const onData = (m) => onMessage(m)
    stream.on('data', onData)
    setTimeout(() => {
      stream.off('data', onData)
      resolve()
    }, ms)
  })
}

async function shutdown (stream, consumer, producer) {
  try {
    if (stream) await deadline(stream.close(), 30000, 'stream.close()')
  } catch (error) {
    info(`stream.close() said: ${error.message}`)
  }
  try {
    if (consumer) await deadline(consumer.close(), 30000, 'consumer.close()')
  } catch (error) {
    info(`consumer.close() said: ${error.message}`)
  }
  try {
    if (producer) await deadline(producer.close(), 30000, 'producer.close()')
  } catch (error) {
    info(`producer.close() said: ${error.message}`)
  }
}

const hex = (b) => (b === null || b === undefined ? String(b) : Buffer.from(b).toString('hex'))

// This client wraps a retried operation's failures in a MultipleErrors
// (code PLT_KFK_MULTIPLE) whose `errors` array holds the real cause, so the
// interesting text is never on the outermost error. Flatten before matching.
function flatten (error, out = []) {
  if (!error || out.includes(error)) return out
  out.push(error)
  for (const nested of error.errors ?? []) flatten(nested, out)
  if (error.cause) flatten(error.cause, out)
  return out
}
const describe = (error) => flatten(error).map((e) => `${e.constructor?.name ?? 'Error'}${e.code ? ` [${e.code}]` : ''}: ${e.message}`).join(' <- ')

function newProducer (name, extra = {}) {
  return new Producer({
    clientId: `plt-${name}-${RUN}`,
    bootstrapBrokers: [BOOTSTRAP],
    autocreateTopics: true,
    retries: 8,
    retryDelay: 250,
    ...extra
  })
}

function newConsumer (name, groupId, extra = {}) {
  return new Consumer({
    clientId: `plt-${name}-${RUN}`,
    groupId,
    bootstrapBrokers: [BOOTSTRAP],
    autocreateTopics: true,
    retries: 8,
    retryDelay: 250,
    ...extra
  })
}

// ----------------------------------------------------------------- scenarios

// 1. Bootstrap: what the broker advertises, and what this client does with the
//    keys it does NOT advertise. Also the auto-create-on-metadata path.
async function metadata () {
  section('metadata: ApiVersions, the advertised surface, and what is missing')
  const admin = new Admin({ clientId: `plt-admin-${RUN}`, bootstrapBrokers: [BOOTSTRAP], retries: 2 })
  try {
    const meta = await deadline(admin.metadata({ topics: [], autocreateTopics: false }), 30000, 'admin.metadata')
    info(`cluster id ${JSON.stringify(meta.id)}, controller ${meta.controllerId}, brokers ${[...meta.brokers.values()].map((b) => `${b.host}:${b.port}(id ${b.nodeId ?? '?'})`).join(', ')}`)
    check(meta.brokers.size >= 1, `metadata returned ${meta.brokers.size} broker(s)`)

    // A bare metadata request naming a fresh topic is the facade's auto-create
    // trigger; assert the topic comes back with the configured partition count.
    const fresh = `plt-meta-${RUN}`
    const m2 = await deadline(admin.metadata({ topics: [fresh], autocreateTopics: true }), 30000, 'admin.metadata(autocreate)')
    const t = m2.topics.get(fresh)
    check(!!t, `metadata auto-created ${fresh}`)
    if (t) check(t.partitionsCount === PARTITIONS, `${fresh} has ${t.partitionsCount} partitions (want ${PARTITIONS})`)

    // Names beginning with __ never exist anywhere in the facade.
    try {
      const m3 = await deadline(admin.metadata({ topics: ['__nope'], autocreateTopics: true }), 30000, 'admin.metadata(__nope)')
      info(`__nope came back as ${m3.topics.has('__nope') ? 'present' : 'absent'} (facade reserves the __ prefix)`)
      ok('a __-prefixed topic did not blow the client up')
    } catch (error) {
      info(`__nope raised ${describe(error)}`)
      ok('a __-prefixed topic raised a normal client error, not a hang')
    }

    // CreateTopics IS advertised since M7 F1 (key 19, v2..6), so this asserts a
    // success. Until F1 it asserted the opposite: this client reads ApiVersions
    // and throws UnsupportedApiError before a byte goes out, which was the
    // designed surface then and is a regression now.
    //
    // Two things are asserted about the ANSWER, because "the promise resolved"
    // would be satisfied by a facade that created nothing:
    //   * the topic is in a FRESH metadata call afterwards, and
    //   * the created topic reports a partition count.
    // The count is deliberately NOT asserted to be the 4 that were asked for.
    // Queen declares no per-topic width: every client sees max(live lanes,
    // QUEEN_KAFKA_DEFAULT_PARTITIONS), so num_partitions is accepted and not
    // acted on, and the CreateTopics v5+ response reports the width the topic
    // really has. That is a deliberate deviation on PLAN_QUEEN_KAFKA.md's list.
    const madeTopic = `plt-made-${RUN}`
    try {
      const created = await deadline(admin.createTopics({ topics: [madeTopic], partitions: 4, replicas: 1 }), 20000, 'admin.createTopics')
      const made = created?.find?.((t) => t.name === madeTopic)
      info(`admin.createTopics -> ${JSON.stringify(created)}`)
      if (check(!!made, `admin.createTopics created ${madeTopic} (CreateTopics, key 19)`)) {
        check(made.partitions === PARTITIONS,
          `the response reports the facade's own width of ${made.partitions}, not the 4 requested: num_partitions is accepted and not acted on`)
      }
      const m4 = await deadline(admin.metadata({ topics: [madeTopic], autocreateTopics: false }), 30000, 'admin.metadata(created)')
      check(m4.topics.has(madeTopic), `${madeTopic} is in a fresh Metadata, so the create really made something`)
    } catch (error) {
      bad(`admin.createTopics failed: ${describe(error)} — CreateTopics is advertised v2..6 since M7 F1, so this is a regression, not the documented refusal`)
    }

    // ...and the same call removed again, so the run leaves nothing behind.
    try {
      await deadline(admin.deleteTopics({ topics: [madeTopic] }), 20000, 'admin.deleteTopics')
      ok(`admin.deleteTopics removed ${madeTopic} (DeleteTopics, key 20)`)
    } catch (error) {
      bad(`admin.deleteTopics failed: ${describe(error)}`)
    }
  } finally {
    try { await deadline(admin.close(), 20000, 'admin.close()') } catch (error) { info(`admin.close(): ${error.message}`) }
  }
}

// 2. 512 records over every partition, keys + headers, acks=all, to a topic
//    that did not exist when the producer started.
const BULK_N = 512
function bulkMessages (topic) {
  return Array.from({ length: BULK_N }, (_, i) => ({
    topic,
    partition: i % PARTITIONS,
    key: Buffer.from(`k-${String(i).padStart(4, '0')}`),
    value: Buffer.from(`v-${String(i).padStart(4, '0')}-${'p'.repeat(i % 61)}`),
    headers: new Map([
      [Buffer.from('seq'), Buffer.from(String(i))],
      [Buffer.from('lane'), Buffer.from(`p${i % PARTITIONS}`)]
    ])
  }))
}

async function produce () {
  section(`produce: ${BULK_N} records over ${PARTITIONS} partitions, keys + headers, acks=all, fresh topic`)
  const topic = `plt-bulk-${RUN}`
  const producer = newProducer('bulk')
  const t0 = Date.now()
  try {
    const messages = bulkMessages(topic)
    // One send call carrying every partition: the client batches per partition
    // into a single Produce request per leader.
    const res = await deadline(producer.send({ messages, acks: ProduceAcks.ALL }), 90000, 'producer.send(512)')
    info(`send of ${BULK_N} records to a brand-new topic took ${Date.now() - t0}ms`)
    const offsets = res.offsets ?? []
    check(offsets.length === PARTITIONS, `the response carried one base offset per partition (${offsets.length})`)
    const zeroes = offsets.filter((o) => o.offset === 0n)
    check(zeroes.length === PARTITIONS, `every partition's first batch starts at offset 0 (${zeroes.length}/${PARTITIONS})`)
    check(!res.unwritableNodes || res.unwritableNodes.length === 0, 'no unwritable nodes reported')

    // A second send must continue where the first stopped.
    const more = await deadline(producer.send({
      messages: Array.from({ length: PARTITIONS }, (_, p) => ({
        topic, partition: p, key: Buffer.from(`tail-${p}`), value: Buffer.from(`tail-${p}`)
      })),
      acks: ProduceAcks.ALL
    }), 60000, 'producer.send(tail)')
    const expect = BigInt(BULK_N / PARTITIONS)
    const continued = (more.offsets ?? []).filter((o) => o.offset === expect)
    check(continued.length === PARTITIONS, `every partition continues at offset ${expect} (${continued.length}/${PARTITIONS})`)
  } finally {
    await shutdown(null, null, producer)
  }
  return topic
}

// 2b. acks=0 writes NO response frame at all. On a connection that dispatches
//     requests by correlation id, a request with no reply is the classic way to
//     wedge the pipeline: this client marks it `noResponse` in Connection#send
//     and must not stall. The offsets it reports for such a send are invented
//     client-side, so nothing is asserted about them.
async function acks0 () {
  section('acks0: a produce with no response frame must not wedge the connection')
  const topic = `plt-acks0-${RUN}`
  const producer = newProducer('acks0')
  try {
    // Seed with acks=all: an acks=0 produce to a topic that does not exist has
    // no response to carry the error either.
    await deadline(producer.send({
      messages: [{ topic, partition: 1, key: Buffer.from('seed'), value: Buffer.from('seed') }],
      acks: ProduceAcks.ALL
    }), 60000, 'acks0 seed')
    const t0 = Date.now()
    const res = await deadline(producer.send({
      messages: Array.from({ length: 5 }, (_, i) => ({
        topic, partition: 1, key: Buffer.from(`z${i}`), value: Buffer.from(`z${i}`)
      })),
      acks: ProduceAcks.NO_RESPONSE
    }), 30000, 'acks0 send')
    const dt = Date.now() - t0
    info(`acks=0 send returned in ${dt}ms; the offsets it reports are client-invented: ${JSON.stringify(res, (k, v) => (typeof v === 'bigint' ? v.toString() : v))}`)
    check(dt < 5000, `acks=0 send returned promptly (${dt}ms) — the connection did not stall on a missing reply`)
    const after = await deadline(producer.send({
      messages: [{ topic, partition: 1, key: Buffer.from('after'), value: Buffer.from('after') }],
      acks: ProduceAcks.ALL
    }), 30000, 'acks0 follow-up')
    check((after.offsets ?? []).length === 1, 'the same connection still serves an acks=all send afterwards')
  } catch (error) {
    bad(`acks0 raised ${describe(error)}`)
    await shutdown(null, null, producer)
    return
  }
  await shutdown(null, null, producer)

  const consumer = newConsumer('acks0', `plt-acks0-g-${RUN}`)
  let stream
  const values = []
  try {
    stream = await deadline(consumer.consume({
      topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: true, sessionTimeout: 30000, maxWaitTime: 500
    }), 60000, 'acks0 consume')
    await consumeUntil(stream, (m) => {
      if (m.partition === 1) values.push(m.value.toString())
    }, () => values.length === 7, 120000, 'the seed, the 5 acks=0 records and the follow-up')
    check(JSON.stringify(values) === JSON.stringify(['seed', 'z0', 'z1', 'z2', 'z3', 'z4', 'after']),
      `the acks=0 records landed, in order: ${JSON.stringify(values)}`)
  } catch (error) {
    bad(`acks0 read-back raised ${describe(error)} (got ${JSON.stringify(values)})`)
  } finally {
    await shutdown(stream, consumer, null)
  }
}

// 3. Every codec this client can compress with. The facade decompresses on the
//    produce path, so what is under test is that the bytes survive.
async function compression () {
  section('compression: gzip, snappy, lz4, zstd — produce compressed, read back byte-exact')
  const codecs = [
    CompressionAlgorithms.GZIP,
    CompressionAlgorithms.SNAPPY,
    CompressionAlgorithms.LZ4,
    CompressionAlgorithms.ZSTD
  ]
  for (const codec of codecs) {
    const topic = `plt-${codec}-${RUN}`
    const producer = newProducer(`c-${codec}`)
    const filler = Buffer.from('C'.repeat(4096))
    const N = 40
    const want = new Map()
    const wantHeaders = new Map()
    try {
      const messages = Array.from({ length: N }, (_, i) => {
        const key = Buffer.from(`${codec}-${i}`)
        const value = Buffer.concat([Buffer.from(`${i}:`), filler])
        want.set(key.toString(), value.toString('hex'))
        wantHeaders.set(key.toString(), `codec=${codec};seq=${i}`)
        // Headers ride inside the compressed record batch too, so they are part
        // of what the codec has to survive.
        const headers = new Map([
          [Buffer.from('codec'), Buffer.from(codec)],
          [Buffer.from('seq'), Buffer.from(String(i))]
        ])
        return { topic, partition: i % PARTITIONS, key, value, headers }
      })
      await deadline(producer.send({ messages, acks: ProduceAcks.ALL, compression: codec }), 60000, `send(${codec})`)
      ok(`${codec}: ${N} records accepted (acks=all)`)
    } catch (error) {
      bad(`${codec}: produce raised ${describe(error)}`)
      await shutdown(null, null, producer)
      continue
    }
    await shutdown(null, null, producer)

    const consumer = newConsumer(`c-${codec}`, `plt-${codec}-g-${RUN}`)
    let stream
    const got = new Map()
    try {
      stream = await deadline(consumer.consume({
        topics: [topic],
        mode: MessagesStreamModes.EARLIEST,
        autocommit: true,
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
        maxWaitTime: 500
      }), 60000, `consume(${codec})`)
      const gotHeaders = new Map()
      await consumeUntil(stream, (m) => {
        got.set(m.key.toString(), m.value.toString('hex'))
        gotHeaders.set(m.key.toString(), (m.headerEntries ?? []).map(([k, v]) => `${k.toString()}=${v.toString()}`).join(';'))
      }, () => got.size === N, 120000, `the ${N} ${codec} records`)
      const wrong = [...want.entries()].filter(([k, v]) => got.get(k) !== v)
      check(wrong.length === 0, `${codec}: all ${N} payloads decode byte-exact through the facade`)
      const wrongHeaders = [...wantHeaders.entries()].filter(([k, v]) => gotHeaders.get(k) !== v)
      check(wrongHeaders.length === 0, `${codec}: headers survived the compressed batch (${wrongHeaders.length} mismatches)`)
    } catch (error) {
      bad(`${codec}: consume raised ${describe(error)} (got ${got.size}/${N})`)
    } finally {
      await shutdown(stream, consumer, null)
    }
  }
}

// 4. Byte-exact round-trip of the awkward shapes: binary keys and values, an
//    empty value, a null key, a null value, and a REPEATED header name.
async function roundtrip () {
  section('roundtrip: binary / empty / null / repeated-header-name records, byte for byte')
  const topic = `plt-rt-${RUN}`
  const nonUtf8 = Buffer.from([0xff, 0xfe, 0x00, 0x41, 0x80])
  // Two distinct Buffer objects with the same bytes are two distinct Map keys,
  // which is how this client expresses a repeated header NAME on the wire.
  const dupHeaders = new Map([
    [Buffer.from('x'), Buffer.from('1')],
    [Buffer.from('x'), Buffer.from('2')]
  ])
  const messages = [
    { topic, partition: 0, key: Buffer.from('plain'), value: Buffer.from('hello'), headers: new Map([[Buffer.from('a'), Buffer.from('one')], [Buffer.from('b'), Buffer.from('two')]]) },
    { topic, partition: 0, key: nonUtf8, value: nonUtf8, headers: new Map([[Buffer.from('h'), nonUtf8]]) },
    { topic, partition: 0, key: undefined, value: Buffer.from('null-key') },
    { topic, partition: 0, key: Buffer.from('empty-value'), value: Buffer.alloc(0) },
    { topic, partition: 0, key: Buffer.from('null-value'), value: undefined },
    { topic, partition: 0, key: Buffer.from('dup-headers'), value: Buffer.from('dh'), headers: dupHeaders },
    { topic, partition: 0, key: Buffer.from('big'), value: Buffer.from('B'.repeat(65536)) }
  ]
  const producer = newProducer('rt')
  try {
    await deadline(producer.send({ messages, acks: ProduceAcks.ALL }), 60000, 'roundtrip send')
    ok(`${messages.length} edge-shaped records accepted on one partition`)
  } catch (error) {
    bad(`roundtrip produce raised ${describe(error)}`)
    await shutdown(null, null, producer)
    return
  }
  await shutdown(null, null, producer)

  const consumer = newConsumer('rt', `plt-rt-g-${RUN}`)
  let stream
  const got = []
  try {
    stream = await deadline(consumer.consume({
      topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: true, sessionTimeout: 30000, maxWaitTime: 500
    }), 60000, 'roundtrip consume')
    await consumeUntil(stream, (m) => got.push(m), () => got.length === messages.length, 120000, `all ${messages.length} round-trip records`)
  } catch (error) {
    bad(`roundtrip consume raised ${describe(error)} (got ${got.length})`)
    await shutdown(stream, consumer, null)
    return
  }

  got.sort((a, b) => Number(a.offset - b.offset))
  for (let i = 0; i < messages.length; i++) {
    const want = messages[i]
    const have = got[i]
    if (!have) {
      bad(`record ${i} never arrived`)
      continue
    }
    // The facade round-trips a null key/value as an empty buffer or a null; both
    // are accepted here, what is asserted is that a NON-empty payload is exact.
    const wantKey = want.key === undefined ? '' : hex(want.key)
    const haveKey = have.key === null || have.key === undefined ? '' : hex(have.key)
    check(haveKey === wantKey, `record ${i} key byte-exact (${wantKey.slice(0, 24) || '<null>'})`)
    const wantVal = want.value === undefined ? '' : hex(want.value)
    const haveVal = have.value === null || have.value === undefined ? '' : hex(have.value)
    check(haveVal === wantVal, `record ${i} value byte-exact (${(want.value?.length ?? 0)} bytes)`)
    check(String(have.offset) === String(i), `record ${i} at offset ${i} (got ${have.offset})`)
    check(typeof have.timestamp === 'bigint' && have.timestamp > 0n, `record ${i} carries a timestamp (${have.timestamp})`)

    const wantEntries = [...(want.headers ?? new Map()).entries()].map(([k, v]) => `${hex(k)}=${hex(v)}`).sort()
    const haveEntries = (have.headerEntries ?? []).map(([k, v]) => `${hex(k)}=${hex(v)}`).sort()
    check(JSON.stringify(haveEntries) === JSON.stringify(wantEntries),
      `record ${i} headers byte-exact (${wantEntries.length} entr${wantEntries.length === 1 ? 'y' : 'ies'})`)
  }
  // The repeated header name is the one that historically got collapsed.
  const dh = got.find((m) => m.key?.toString() === 'dup-headers')
  if (dh) {
    const xs = (dh.headerEntries ?? []).filter(([k]) => k.toString() === 'x').map(([, v]) => v.toString())
    check(xs.length === 2 && xs.sort().join(',') === '1,2',
      `a repeated header name survived as two entries (got ${JSON.stringify(xs)})`)
  }
  await shutdown(stream, consumer, null)
}

// 5. One consumer group over the bulk topic: count, per-partition order, and the
//    key/value/header contract on every record.
async function group (bulkTopic) {
  section('group: subscribe, drain every partition, check count / order / payloads')
  const topic = bulkTopic
  const groupId = `plt-g-${RUN}`
  const consumer = newConsumer('g', groupId, { sessionTimeout: 30000, heartbeatInterval: 3000 })
  let stream
  const perPartition = new Map()
  const seen = new Map()
  let dupes = 0
  const total = BULK_N + PARTITIONS // the bulk plus the tail records
  const t0 = Date.now()
  try {
    stream = await deadline(consumer.consume({
      topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: true, sessionTimeout: 30000, heartbeatInterval: 3000, maxWaitTime: 500
    }), 60000, 'group consume')
    await consumeUntil(stream, (m) => {
      const key = m.key.toString()
      if (seen.has(key)) dupes++
      seen.set(key, m)
      if (!perPartition.has(m.partition)) perPartition.set(m.partition, [])
      perPartition.get(m.partition).push(m)
    }, () => seen.size === total, 180000, `all ${total} records through one group`)
  } catch (error) {
    bad(`group consume raised ${describe(error)} (got ${seen.size}/${total})`)
    await shutdown(stream, consumer, null)
    return
  }
  info(`group formed and drained ${total} records in ${Date.now() - t0}ms (3s of that is the facade's join window)`)
  check(seen.size === total, `every one of the ${total} records arrived exactly once (${dupes} duplicates)`)
  check(perPartition.size === PARTITIONS, `every one of the ${PARTITIONS} partitions was read (${[...perPartition.keys()].sort((a, b) => a - b)})`)

  let orderBad = 0
  let seqBad = 0
  for (const [p, list] of perPartition) {
    for (let i = 1; i < list.length; i++) {
      if (list[i].offset <= list[i - 1].offset) orderBad++
    }
    // Offsets on a partition must be dense from 0.
    const offs = list.map((m) => Number(m.offset)).sort((a, b) => a - b)
    if (offs[0] !== 0 || offs[offs.length - 1] !== offs.length - 1) seqBad++
    // Every bulk record on partition p carries lane=p in its headers.
    for (const m of list) {
      const lane = (m.headerEntries ?? []).find(([k]) => k.toString() === 'lane')
      if (lane && lane[1].toString() !== `p${p}`) seqBad++
    }
  }
  check(orderBad === 0, `records arrived in offset order within every partition (${orderBad} inversions)`)
  check(seqBad === 0, `every partition carried a dense 0..n-1 offset run with matching headers (${seqBad} problems)`)
  const valueBad = [...seen.entries()].filter(([k, m]) =>
    k.startsWith('k-') && !m.value.toString().startsWith(`v-${k.slice(2)}-`))
  check(valueBad.length === 0, `every value matched its key (${valueBad.length} mismatches)`)
  await shutdown(stream, consumer, null)
}

// 6. Commit, stop, restart in the same group: the second instance must resume
//    from the committed offset and see only what is new.
async function resume () {
  section('resume: commit, close, restart in the same group — no loss, no replay')
  const topic = `plt-resume-${RUN}`
  const groupId = `plt-res-${RUN}`
  const first = 40
  const second = 24
  const producer = newProducer('resume')
  try {
    await deadline(producer.send({
      messages: Array.from({ length: first }, (_, i) => ({
        topic, partition: i % PARTITIONS, key: Buffer.from(`a${i}`), value: Buffer.from(`a${i}`)
      })),
      acks: ProduceAcks.ALL
    }), 60000, 'resume seed send')
  } finally {
    await shutdown(null, null, producer)
  }

  // Round one: autocommit off, commit every record by hand.
  const c1 = newConsumer('res1', groupId, { sessionTimeout: 30000, heartbeatInterval: 3000 })
  let s1
  const read1 = []
  try {
    s1 = await deadline(c1.consume({
      topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: false, sessionTimeout: 30000, heartbeatInterval: 3000, maxWaitTime: 500
    }), 60000, 'resume consume 1')
    const pending = []
    await consumeUntil(s1, (m) => {
      read1.push(m.key.toString())
      pending.push(m.commit())
    }, () => read1.length === first, 180000, `the first consumer to read all ${first}`)
    await deadline(Promise.all(pending), 60000, 'the explicit commits to settle')
    ok(`first consumer read ${read1.length} records and committed each one explicitly`)
  } catch (error) {
    bad(`resume round 1 raised ${describe(error)} (read ${read1.length})`)
    await shutdown(s1, c1, null)
    return
  }
  await shutdown(s1, c1, null)

  // What the coordinator now believes, read back through OffsetFetch.
  const probe = newConsumer('res-probe', groupId)
  try {
    const committed = await deadline(probe.listCommittedOffsets({
      topics: [{ topic, partitions: Array.from({ length: PARTITIONS }, (_, i) => i) }]
    }), 30000, 'listCommittedOffsets')
    const map = committed.get(topic)
    const total = map ? [...map].reduce((a, b) => a + Number(b < 0n ? 0n : b), 0) : -1
    info(`committed offsets per partition: ${map ? [...map].map(String).join(',') : '<none>'}`)
    check(total === first, `the committed offsets add up to the ${first} records read (got ${total})`)
  } catch (error) {
    bad(`listCommittedOffsets raised ${describe(error)}`)
  } finally {
    await shutdown(null, probe, null)
  }

  const producer2 = newProducer('resume2')
  try {
    await deadline(producer2.send({
      messages: Array.from({ length: second }, (_, i) => ({
        topic, partition: i % PARTITIONS, key: Buffer.from(`b${i}`), value: Buffer.from(`b${i}`)
      })),
      acks: ProduceAcks.ALL
    }), 60000, 'resume second send')
  } finally {
    await shutdown(null, null, producer2)
  }

  // Round two: mode COMMITTED with fallback EARLIEST — the trap is that the
  // fallback must NOT win, because a committed offset exists.
  const c2 = newConsumer('res2', groupId, { sessionTimeout: 30000, heartbeatInterval: 3000 })
  let s2
  const read2 = []
  try {
    s2 = await deadline(c2.consume({
      topics: [topic],
      mode: MessagesStreamModes.COMMITTED,
      fallbackMode: MessagesStreamFallbackModes.EARLIEST,
      autocommit: true,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
      maxWaitTime: 500
    }), 60000, 'resume consume 2')
    await consumeUntil(s2, (m) => read2.push(m.key.toString()), () => read2.length >= second, 180000, `the restarted consumer to read the ${second} new records`)
    // Give it a fair chance to wrongly replay the first batch.
    await drainFor(s2, (m) => read2.push(m.key.toString()), 4000)
  } catch (error) {
    bad(`resume round 2 raised ${describe(error)} (read ${read2.length})`)
    await shutdown(s2, c2, null)
    return
  }
  const replayed = read2.filter((k) => k.startsWith('a'))
  check(replayed.length === 0, `the restarted consumer replayed nothing (${replayed.length} old records)`)
  check(read2.length === second, `the restarted consumer read exactly the ${second} new records (got ${read2.length})`)
  await shutdown(s2, c2, null)
}

// 7. Offsets: earliest, latest, committed, lag, and a manual seek.
async function offsets () {
  section('offsets: listOffsets earliest/latest, listCommittedOffsets, getLag, manual seek')
  const topic = `plt-off-${RUN}`
  const N = 64
  const producer = newProducer('off')
  try {
    await deadline(producer.send({
      messages: Array.from({ length: N }, (_, i) => ({
        topic, partition: i % PARTITIONS, key: Buffer.from(`o${i}`), value: Buffer.from(`o${i}`)
      })),
      acks: ProduceAcks.ALL
    }), 60000, 'offsets seed send')
  } finally {
    await shutdown(null, null, producer)
  }

  const perPartition = N / PARTITIONS
  const consumer = newConsumer('off', `plt-off-g-${RUN}`)
  try {
    const latest = await deadline(consumer.listOffsets({ topics: [topic] }), 30000, 'listOffsets(latest)')
    const lat = latest.get(topic)
    info(`latest offsets: ${[...lat].map(String).join(',')}`)
    check(!!lat && [...lat].every((o) => Number(o) === perPartition),
      `listOffsets latest is ${perPartition} on every partition`)

    const earliest = await deadline(consumer.listOffsets({ topics: [topic], timestamp: -2n }), 30000, 'listOffsets(earliest)')
    const ear = earliest.get(topic)
    info(`earliest offsets: ${[...ear].map(String).join(',')}`)
    check(!!ear && [...ear].every((o) => Number(o) === 0), 'listOffsets earliest is 0 on every partition (nothing has been retained away)')

    // getLag is CLIENT-side arithmetic over this consumer's own live streams
    // (consumer.js getLag: `committeds` is built from stream.offsetsCommitted,
    // and a partition with no entry is reported as -1n meaning "not mine").
    // With no stream open it is therefore -1 everywhere. That is client
    // semantics, not a facade answer, so it is recorded and not asserted on.
    const virgin = await deadline(consumer.getLag({ topics: [topic] }), 30000, 'getLag(no stream)')
    info(`getLag with no open stream: ${[...virgin.get(topic)].map(String).join(',')} (-1 = "this consumer holds no committed offset for that partition")`)
  } catch (error) {
    bad(`offset queries raised ${describe(error)}`)
  } finally {
    await shutdown(null, consumer, null)
  }

  // The real lag check: drain the topic, commit every record, then ask. The
  // facade's OffsetCommit/ListOffsets pair has to agree for this to be 0.
  const lagger = newConsumer('lag', `plt-lag-g-${RUN}`, { sessionTimeout: 30000, heartbeatInterval: 3000 })
  let lagStream
  try {
    lagStream = await deadline(lagger.consume({
      topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: false, sessionTimeout: 30000, heartbeatInterval: 3000, maxWaitTime: 500
    }), 60000, 'lag consume')
    let read = 0
    const pending = []
    await consumeUntil(lagStream, (m) => {
      read++
      pending.push(m.commit())
    }, () => read === N, 180000, `all ${N} records before measuring lag`)
    await deadline(Promise.all(pending), 60000, 'the lag commits to settle')
    const lag = await deadline(lagger.getLag({ topics: [topic] }), 30000, 'getLag(caught up)')
    const lg = lag.get(topic)
    info(`lag after committing everything: ${[...lg].map(String).join(',')}`)
    check(!!lg && [...lg].every((o) => o === 0n), 'getLag is 0 on every partition once the group has committed the whole topic')

    const committed = await deadline(lagger.listCommittedOffsets({
      topics: [{ topic, partitions: Array.from({ length: PARTITIONS }, (_, i) => i) }]
    }), 30000, 'listCommittedOffsets(caught up)')
    const cm = committed.get(topic)
    info(`committed offsets read back through OffsetFetch: ${[...cm].map(String).join(',')}`)
    check(!!cm && [...cm].every((o) => Number(o) === perPartition),
      `every committed offset came back as ${perPartition} (the log end)`)
  } catch (error) {
    bad(`lag measurement raised ${describe(error)}`)
  } finally {
    await shutdown(lagStream, lagger, null)
  }

  // Manual seek: start the stream at offset 4 on every partition and prove the
  // first record on each is the 5th one produced there.
  const seeker = newConsumer('seek', `plt-seek-g-${RUN}`)
  let stream
  const firstPer = new Map()
  const SEEK = 4
  try {
    stream = await deadline(seeker.consume({
      topics: [topic],
      mode: MessagesStreamModes.MANUAL,
      offsets: Array.from({ length: PARTITIONS }, (_, p) => ({ topic, partition: p, offset: BigInt(SEEK) })),
      autocommit: false,
      sessionTimeout: 30000,
      maxWaitTime: 500
    }), 60000, 'manual-seek consume')
    await consumeUntil(stream, (m) => {
      if (!firstPer.has(m.partition)) firstPer.set(m.partition, m)
    }, () => firstPer.size === PARTITIONS, 120000, `the first record after seeking to ${SEEK} on every partition`)
    const wrong = [...firstPer.entries()].filter(([, m]) => Number(m.offset) !== SEEK)
    check(wrong.length === 0, `a manual seek to offset ${SEEK} delivered offset ${SEEK} first on all ${PARTITIONS} partitions`)
    const keys = [...firstPer.entries()].sort((a, b) => a[0] - b[0]).map(([p, m]) => `p${p}:${m.key.toString()}`)
    info(`first record per partition after the seek: ${keys.join(' ')}`)
    // Partition p offset 4 is the 5th record routed to p, i.e. o{4*PARTITIONS+p}.
    const mislabelled = [...firstPer.entries()].filter(([p, m]) => m.key.toString() !== `o${SEEK * PARTITIONS + p}`)
    check(mislabelled.length === 0, 'the seek landed on the record the produce order says it should')
  } catch (error) {
    bad(`manual seek raised ${describe(error)} (${firstPer.size}/${PARTITIONS})`)
  } finally {
    await shutdown(stream, seeker, null)
  }
}

// 8. Two members of one group split the partitions; when one leaves the other
//    takes the whole topic. The split is read off the ASSIGNMENT each member is
//    handed, not off what each happened to read.
async function rebalance () {
  section('rebalance: two members split the partitions, then one leaves')
  const topic = `plt-rebal-${RUN}`
  const groupId = `plt-rb-${RUN}`
  const producer = newProducer('rebal')
  try {
    await deadline(producer.send({
      messages: [{ topic, partition: 0, key: Buffer.from('seed'), value: Buffer.from('seed') }],
      acks: ProduceAcks.ALL
    }), 60000, 'rebalance seed')
  } finally {
    await shutdown(null, null, producer)
  }

  const assigned = {}
  const generation = {}
  const read = { a: new Map(), b: new Map() } // partition -> count
  const seen = new Map()
  let counting = false
  const members = {}
  const streams = {}

  const start = async (name) => {
    const c = newConsumer(`rb-${name}`, groupId, { sessionTimeout: 30000, heartbeatInterval: 3000 })
    c.on('consumer:group:join', (payload) => {
      assigned[name] = (payload.assignments ?? []).filter((a) => a.topic === topic).flatMap((a) => a.partitions)
      generation[name] = payload.generationId
    })
    members[name] = c
    const s = await deadline(c.consume({
      topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: true, sessionTimeout: 30000, heartbeatInterval: 3000, maxWaitTime: 500
    }), 90000, `rebalance consume ${name}`)
    streams[name] = s
    s.on('data', (m) => {
      if (!counting) return
      read[name].set(m.partition, (read[name].get(m.partition) ?? 0) + 1)
      const k = m.key.toString()
      seen.set(k, (seen.get(k) ?? 0) + 1)
    })
    return s
  }
  const showRead = (name) => `[${[...read[name].entries()].sort((a, b) => a[0] - b[0]).map(([p, n]) => `p${p}x${n}`).join(' ') || '-'}]`

  try {
    await start('a')
    await start('b')
    const split = () => {
      const pa = assigned.a
      const pb = assigned.b
      if (!pa || !pb) return false
      return pa.length > 0 && pb.length > 0 &&
        pa.filter((p) => pb.includes(p)).length === 0 &&
        pa.length + pb.length === PARTITIONS
    }
    const t0 = Date.now()
    while (!split()) {
      if (Date.now() - t0 > 150000) throw new Error(`TIMEOUT: the two members never held a disjoint split (a=${JSON.stringify(assigned.a)} b=${JSON.stringify(assigned.b)})`)
      await sleep(250)
    }
    info(`assignment: A=[${[...assigned.a].sort((x, y) => x - y)}] (gen ${generation.a}) B=[${[...assigned.b].sort((x, y) => x - y)}] (gen ${generation.b})`)
    ok(`the coordinator split ${PARTITIONS} partitions across two members with no overlap`)

    // A member that was ALONE in the previous generation was legitimately
    // fetching every partition a moment ago. The assignment landing is not the
    // same instant as its stream having re-pointed its fetches, so settle before
    // counting anything: what is under test is steady-state ownership, not the
    // width of that client-side window.
    const settleFrom = Date.now()
    let stableSince = Date.now()
    let lastSeen = JSON.stringify([assigned.a, assigned.b])
    while (Date.now() - stableSince < SETTLE_MS) {
      await sleep(200)
      const now = JSON.stringify([assigned.a, assigned.b])
      if (now !== lastSeen) {
        lastSeen = now
        stableSince = Date.now()
      }
      if (Date.now() - settleFrom > 60000) break
    }
    info(`assignment held steady for ${SETTLE_MS}ms before counting started`)

    counting = true
    const N = 80
    const p2 = newProducer('rebal2')
    try {
      await deadline(p2.send({
        messages: Array.from({ length: N }, (_, i) => ({
          topic, partition: i % PARTITIONS, key: Buffer.from(`k${i}`), value: Buffer.from(`v${i}`)
        })),
        acks: ProduceAcks.ALL
      }), 60000, 'rebalance bulk send')
    } finally {
      await shutdown(null, null, p2)
    }
    const t1 = Date.now()
    while (seen.size < N) {
      if (Date.now() - t1 > 150000) throw new Error(`TIMEOUT: only ${seen.size}/${N} records reached the two members`)
      await sleep(200)
    }
    await sleep(1500) // let a wrong extra delivery show up rather than hiding it
    const dupes = [...seen.values()].filter((n) => n > 1).length
    info(`member A read ${showRead('a')}, member B read ${showRead('b')}`)
    check(dupes === 0, `no record was delivered twice across the two members (${dupes})`)
    const overlap = [...read.a.keys()].filter((p) => read.b.has(p))
    check(overlap.length === 0, `no partition was read by both members (overlap ${JSON.stringify(overlap)})`)

    // B leaves; A must be reassigned everything and reach records produced after.
    await shutdown(streams.b, members.b, null)
    streams.b = null
    members.b = null
    const p3 = newProducer('rebal3')
    try {
      await deadline(p3.send({
        messages: Array.from({ length: PARTITIONS * 2 }, (_, i) => ({
          topic, partition: i % PARTITIONS, key: Buffer.from(`post${i}`), value: Buffer.from(`p${i}`)
        })),
        acks: ProduceAcks.ALL
      }), 60000, 'post-rebalance send')
    } finally {
      await shutdown(null, null, p3)
    }
    const wantPost = PARTITIONS * 2
    const t2 = Date.now()
    while ([...seen.keys()].filter((k) => k.startsWith('post')).length < wantPost) {
      if (Date.now() - t2 > 200000) {
        throw new Error(`TIMEOUT: the survivor reached only ${[...seen.keys()].filter((k) => k.startsWith('post')).length}/${wantPost} post-rebalance records`)
      }
      await sleep(250)
    }
    info(`after B left, A holds [${[...(assigned.a ?? [])].sort((x, y) => x - y)}]`)
    check((assigned.a ?? []).length === PARTITIONS, `the survivor was reassigned all ${PARTITIONS} partitions`)
    const postDupes = [...seen.entries()].filter(([k, n]) => k.startsWith('post') && n > 1).length
    check(postDupes === 0, `no post-rebalance record delivered twice (${postDupes})`)
  } catch (error) {
    bad(`rebalance raised ${describe(error)}`)
  } finally {
    await shutdown(streams.a, members.a, null)
    if (members.b) await shutdown(streams.b, members.b, null)
  }
}

// 9. Idempotent produce, MEASURED. This scenario used to be informational and
//    never failed the run: it recorded how `idempotent: true` died, because
//    InitProducerId was not advertised and this client raises UnsupportedApiError
//    off ApiVersions without sending anything. M7 F3 advertises key 22 v0..4 and
//    enforces the per-(producer, topic-partition) sequence window, so the
//    scenario now asserts instead of narrating.
//
//    Two sends and not one: the first proves the GRANT, the second proves the
//    sequence WINDOW advanced rather than rejecting or duplicating. Both offsets
//    are read out of the send results, so a facade that acknowledged without
//    writing could not pass.
async function idempotent () {
  section('idempotent: `idempotent: true` works since M7 F3')
  const topic = `plt-idem-${RUN}`
  const producer = newProducer('idem', { idempotent: true, retries: 1 })
  const offsets = []
  try {
    for (const seq of [0, 1]) {
      const res = await deadline(producer.send({
        messages: [{ topic, partition: 0, key: Buffer.from(`i${seq}`), value: Buffer.from(`i${seq}`) }],
        acks: ProduceAcks.ALL
      }), 45000, `idempotent send ${seq}`)
      const off = res?.offsets?.[0]?.offset ?? res?.[0]?.offset
      // Offsets come back as BigInt, which JSON.stringify throws on rather than
      // skipping, so the replacer is load-bearing and not decoration.
      info(`idempotent send ${seq} -> ${JSON.stringify(res, (_k, v) => (typeof v === 'bigint' ? v.toString() : v))}`)
      if (check(off !== undefined && off !== null, `idempotent send ${seq} landed and carries an offset (${off})`)) {
        offsets.push(Number(off))
      }
    }
    if (offsets.length === 2) {
      check(offsets[1] === offsets[0] + 1,
        `the two sends are consecutive on the partition (${offsets[0]} then ${offsets[1]}), so the sequence window advanced`)
    }
  } catch (error) {
    bad(`idempotent send failed: ${describe(error)} — InitProducerId is advertised v0..4 since M7 F3, so this is a regression, not the documented refusal`)
    if (error.cause) info(`  cause: ${error.cause.constructor?.name}: ${error.cause.message}`)
  } finally {
    try { await deadline(producer.close(true), 30000, 'idempotent producer.close') } catch (error) { info(`close: ${error.message}`) }
  }
}

// 10. SASL/PLAIN, optionally over TLS. Skipped with a note unless
//     KAFKA_SASL_BOOTSTRAP is set.
async function sasl () {
  section('sasl: SASL/PLAIN produce + consume')
  if (!SASL_BOOTSTRAP) {
    info('KAFKA_SASL_BOOTSTRAP is unset — SASL lane skipped')
    return
  }
  const [host] = SASL_BOOTSTRAP.split(':')
  // RFC 6066 forbids an IP literal in SNI and node warns about it, so only set
  // servername for a real name. The facade's QUEEN_KAFKA_FORWARD_SNI_HOST reads
  // that SNI, which is why the value matters and is not just cosmetic.
  const isIp = /^[0-9.]+$/.test(host) || host.includes(':')
  const sni = isIp ? {} : { servername: host }
  const tls = SASL_TLS
    ? (SASL_INSECURE
        ? { rejectUnauthorized: false, ...sni }
        : { ca: SASL_CA ? [readFileSync(SASL_CA)] : undefined, ...sni })
    : undefined
  const common = {
    bootstrapBrokers: [SASL_BOOTSTRAP],
    autocreateTopics: true,
    retries: 3,
    retryDelay: 250,
    sasl: { mechanism: SASLMechanisms.PLAIN, username: 'plt', password: SASL_TOKEN },
    ...(tls ? { tls } : {})
  }
  const topic = `plt-sasl-${RUN}`
  const producer = new Producer({ clientId: `plt-sasl-p-${RUN}`, ...common, serializers: stringSerializers })
  let stream
  let consumer
  try {
    const res = await deadline(producer.send({
      messages: Array.from({ length: 8 }, (_, i) => ({ topic, partition: i % PARTITIONS, key: `s${i}`, value: `sv${i}` })),
      acks: ProduceAcks.ALL
    }), 60000, 'sasl send')
    check((res.offsets ?? []).length > 0, `SASL/PLAIN${SASL_TLS ? '+TLS' : ''} produce accepted (${res.offsets.length} partitions)`)

    consumer = new Consumer({ clientId: `plt-sasl-c-${RUN}`, groupId: `plt-sasl-g-${RUN}`, ...common, deserializers: stringDeserializers })
    stream = await deadline(consumer.consume({
      topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: true, sessionTimeout: 30000, maxWaitTime: 500
    }), 60000, 'sasl consume')
    const got = new Map()
    await consumeUntil(stream, (m) => got.set(m.key, m.value), () => got.size === 8, 120000, 'the 8 SASL records')
    const wrong = [...got.entries()].filter(([k, v]) => v !== `sv${k.slice(1)}`)
    check(wrong.length === 0, 'every record read back through the SASL listener matched')
  } catch (error) {
    bad(`SASL lane raised ${describe(error)}`)
    if (error.cause) info(`  cause: ${error.cause.message}`)
  } finally {
    await shutdown(stream, consumer, producer)
  }

  // A wrong password must be refused, not silently accepted.
  const badProducer = new Producer({
    clientId: `plt-sasl-bad-${RUN}`,
    ...common,
    sasl: { mechanism: SASLMechanisms.PLAIN, username: 'plt', password: `${SASL_TOKEN}-WRONG` },
    retries: 0
  })
  try {
    await deadline(badProducer.send({
      messages: [{ topic, partition: 0, key: Buffer.from('nope'), value: Buffer.from('nope') }],
      acks: ProduceAcks.ALL
    }), 45000, 'sasl wrong-password send')
    bad('a WRONG SASL password was accepted')
  } catch (error) {
    info(`wrong password -> ${describe(error)}`)
    ok('a wrong SASL password was refused')
  } finally {
    try { await deadline(badProducer.close(true), 20000, 'bad producer close') } catch { /* already dead */ }
  }
}

// ---------------------------------------------------------------------- main
const SCENARIOS = ['metadata', 'produce', 'acks0', 'compression', 'roundtrip', 'group', 'resume', 'offsets', 'rebalance', 'idempotent', 'sasl']

async function main () {
  console.log(`@platformatic/kafka ${CLIENT_VERSION} on node ${process.version} (${process.platform}/${process.arch})`)
  console.log(`bootstrap=${BOOTSTRAP} runId=${RUN} partitions=${PARTITIONS} scenario=${ONLY}`)
  if (SASL_BOOTSTRAP) console.log(`sasl bootstrap=${SASL_BOOTSTRAP} protocol=${SASL_TLS ? 'sasl_ssl' : 'sasl_plaintext'} verify=${SASL_INSECURE ? 'off' : 'on'}`)

  const wanted = ONLY === 'all' ? SCENARIOS : ONLY.split(',').map((s) => s.trim())
  for (const name of wanted) {
    if (!SCENARIOS.includes(name)) {
      bad(`unknown scenario ${name} (want: ${SCENARIOS.join(' | ')} | all)`)
      continue
    }
  }

  let bulkTopic = `plt-bulk-${RUN}`
  const run = async (name, fn) => {
    if (!wanted.includes(name)) return
    try {
      return await fn()
    } catch (error) {
      bad(`scenario ${name} threw ${error.constructor?.name}: ${error.message}`)
      if (error.stack) console.log(error.stack.split('\n').slice(1, 4).join('\n'))
    }
  }

  await run('metadata', metadata)
  const produced = await run('produce', produce)
  if (produced) bulkTopic = produced
  await run('acks0', acks0)
  await run('compression', compression)
  await run('roundtrip', roundtrip)
  // `group` reads what `produce` wrote; when run alone it seeds its own copy.
  if (wanted.includes('group') && !wanted.includes('produce')) {
    await run('group', async () => {
      const p = newProducer('bulk')
      try {
        await deadline(p.send({ messages: bulkMessages(bulkTopic), acks: ProduceAcks.ALL }), 90000, 'group seed')
        await deadline(p.send({
          messages: Array.from({ length: PARTITIONS }, (_, i) => ({ topic: bulkTopic, partition: i, key: Buffer.from(`tail-${i}`), value: Buffer.from(`tail-${i}`) })),
          acks: ProduceAcks.ALL
        }), 60000, 'group seed tail')
      } finally {
        await shutdown(null, null, p)
      }
      return group(bulkTopic)
    })
  } else {
    await run('group', () => group(bulkTopic))
  }
  await run('resume', resume)
  await run('offsets', offsets)
  await run('rebalance', rebalance)
  await run('idempotent', idempotent)
  await run('sasl', sasl)

  printNegotiated()
  console.log('')
  if (failures === 0) {
    console.log('RESULT: PASS (@platformatic/kafka)')
  } else {
    console.log(`RESULT: FAIL (${failures}) (@platformatic/kafka)`)
  }
  process.exit(failures === 0 ? 0 : 1)
}

main().catch((error) => {
  console.error(error)
  printNegotiated()
  console.log(`RESULT: FAIL (harness threw: ${error.message}) (@platformatic/kafka)`)
  process.exit(1)
})
