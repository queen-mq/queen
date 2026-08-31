// @confluentinc/kafka-javascript against the queen-kafka facade — the second of
// the two Node librdkafka bindings here (node_rdkafka.mjs is the first) and the
// one Confluent points kafkajs users at. compat/js covers kafkajs itself.
//
//   node confluent_kafka_js.mjs [bootstrap] [runId]
//   KAFKA_BOOTSTRAP=... RUN_ID=... node confluent_kafka_js.mjs
//
// This drives the PROMISIFIED `KafkaJS` API — `kafka.producer().send()`,
// `consumer.run({ eachMessage })` — because that is the surface a team migrating
// off kafkajs actually writes, and it is a different code path from the
// node-rdkafka-shaped one: the kafkaJS layer owns the config translation, the
// offset bookkeeping around `eachMessage`, and the promise lifecycle. The
// librdkafka underneath is the same, so anything that behaves DIFFERENTLY from
// node_rdkafka.mjs is that layer and worth knowing about.
//
// What it proves: auto-create; 512 records over 8 partitions with binary keys,
// binary values and binary headers; every codec this build has; a consumer group
// reading them byte-exact in per-partition produce order; commits surviving the
// member that made them; earliest/latest offsets and seek; and, when
// KAFKA_SASL_BOOTSTRAP is set, the same over SASL/PLAIN.
//
// WHAT IS THE CLIENT'S FAULT AND NOT THE FACADE'S:
//
//   * librdkafka gates zstd PRODUCE on negotiating Fetch v10. The facade caps
//     Fetch at v6 deliberately (fetch sessions are v7; PLAN_QUEEN_KAFKA.md), so
//     a zstd send falls back to UNCOMPRESSED with "Broker does not support
//     compression type zstd" in the debug stream. The records still land. This
//     suite asserts the records, prints the fallback, and files nothing.
//   * The `admin` client's richer calls (topic creation, DescribeConfigs, group
//     listing) sit on API keys this facade does not advertise at all. That is
//     the deliberate surface, not a gap: a client that reads ApiVersions fails
//     fast and legibly. Where this suite touches admin it says which key it
//     needed and treats an UNSUPPORTED as information.
//   * Idempotent produce needs InitProducerId, which is M7. `idempotent: false`
//     everywhere; RUN_IDEMPOTENCE=1 records the failure mode of turning it on.
import Confluent from '@confluentinc/kafka-javascript'
import {
  PARTITIONS, COUNT, fixture, ok, bad, info, say, check, finish,
  sleep, deadline, Negotiated, verifyRoundTrip,
} from './fixture.mjs'

const { KafkaJS } = Confluent
const { Kafka, CompressionTypes, logLevel } = KafkaJS

const BOOTSTRAP = process.argv[2] || process.env.KAFKA_BOOTSTRAP || '127.0.0.1:19092'
const RUN = process.argv[3] || process.env.RUN_ID || String(Date.now())
const TAG = `ckjs-${RUN}`
const GROUP = `ckjs-g-${RUN}`

const FEATURES = Confluent.features
const HAS = (f) => FEATURES.includes(f)

const trace = new Negotiated()
// The kafkaJS layer routes librdkafka's own log stream into this logger, so
// `debug: 'protocol'` below still ends up somewhere this suite can read the
// negotiated versions out of — negotiated, never assumed.
const logger = {
  namespace: () => logger,
  setLogLevel: () => {},
  info: (m, e) => trace.line(text(m, e)),
  error: (m, e) => trace.line(text(m, e)),
  warn: (m, e) => trace.line(text(m, e)),
  debug: (m, e) => trace.line(text(m, e)),
}
const text = (m, extra) =>
  typeof m === 'string'
    ? `${m}${extra && extra.message ? ` ${extra.message}` : ''}`
    : JSON.stringify(m)

/** Client-level config shared by every handle: SASL is folded in when asked. */
function auth() {
  const bootstrap = process.env.KAFKA_SASL_BOOTSTRAP
  if (!bootstrap) return null
  const protocol = process.env.KAFKA_SASL_PROTOCOL || 'sasl_ssl'
  return {
    'bootstrap.servers': bootstrap,
    'security.protocol': protocol,
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'confluent-kafka-javascript',
    // The password IS the Queen bearer token; the username is a free label.
    'sasl.password': process.env.KAFKA_SASL_TOKEN || '',
    // Verification stays ON when a CA is supplied. The rig's self-signed cert
    // carries DNS:localhost and IP:127.0.0.1, so a HOST client dialling either
    // of those names validates for real — no `InsecureSkipVerify` equivalent
    // needed. KAFKA_SSL_INSECURE=1 is the escape hatch for an advertised name
    // the cert does not cover (host.docker.internal, notably).
    ...(protocol.includes('ssl')
      ? {
          ...(process.env.KAFKA_SSL_CA ? { 'ssl.ca.location': process.env.KAFKA_SSL_CA } : {}),
          ...(process.env.KAFKA_SSL_INSECURE === '1'
            ? { 'enable.ssl.certificate.verification': false }
            : {}),
        }
      : {}),
  }
}

function client(extra = {}) {
  return new Kafka({
    'bootstrap.servers': BOOTSTRAP,
    debug: 'protocol,cgrp,topic,broker',
    kafkaJS: { clientId: `${TAG}`, logLevel: logLevel.DEBUG, logger },
    ...extra,
  })
}

// The kafkaJS layer hands headers back as { name: Buffer | Buffer[] }.
// Normalise to the ordered [{name, value}] the fixture speaks; a repeated name
// arrives as an array and is flattened back into repeats, in order.
function normalizeHeaders(h) {
  const out = []
  for (const [name, v] of Object.entries(h || {})) {
    for (const one of Array.isArray(v) ? v : [v]) {
      out.push({ name, value: Buffer.from(one ?? []) })
    }
  }
  return out
}

// ------------------------------------------------------------------- producing
async function produce(topic, records, { compression, clientExtra } = {}) {
  const kafka = client(clientExtra)
  const producer = kafka.producer({
    // M7 is InitProducerId; idempotence stays off across this whole matrix.
    //
    // NOTE for anyone porting a kafkajs codebase: `compression` is a PRODUCER
    // property here, not a `send()` one. kafkajs takes it per send; this client
    // throws `'compression' is not supported as a property to 'send'` with a
    // before/after snippet. It is a client API divergence, nothing to do with
    // the facade, and it is a compile-time-ish error rather than a silent
    // difference — which is the good version of that story.
    kafkaJS: {
      acks: -1,
      idempotent: false,
      allowAutoTopicCreation: true,
      ...(compression ? { compression } : {}),
    },
  })
  await deadline(producer.connect(), 45000, 'confluent producer connect')
  const res = await deadline(
    producer.send({
      topic,
      messages: records.map((r) => ({
        key: r.key,
        value: r.value,
        partition: r.partition,
        headers: Object.fromEntries(r.headers.map((h) => [h.name, h.value])),
      })),
    }),
    120000,
    `send ${records.length} records to ${topic}`,
  )
  await deadline(producer.disconnect(), 30000, 'producer disconnect')
  return res
}

// ------------------------------------------------------------------- consuming
function newConsumer(groupId, extra = {}) {
  return client(extra).consumer({
    kafkaJS: {
      groupId,
      fromBeginning: true,
      autoCommit: false,
      sessionTimeout: 30000,
    },
  })
}

/**
 * Run a consumer until `want` messages have arrived, then linger for `linger` ms
 * so a redelivery would be caught, then stop. Every wait has a deadline: a hang
 * is a named failure, never a wedged process.
 */
async function drain(consumer, want, { timeoutMs = 180000, linger = 0, assignOnly } = {}) {
  const seen = []
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      seen.push({
        topic,
        partition,
        offset: Number(message.offset),
        key: message.key,
        value: message.value,
        headers: normalizeHeaders(message.headers),
      })
    },
  })
  const until = Date.now() + timeoutMs
  while (seen.length < want && Date.now() < until) await sleep(100)
  if (seen.length < want) {
    throw new Error(`TIMEOUT: read ${seen.length}/${want} records in ${timeoutMs}ms`)
  }
  if (linger) await sleep(linger)
  return seen
}

// ================================================================== scenarios

// 1 + 4. Auto-create, 512 records, 8 partitions, binary keys/values/headers.
async function scenarioProduce() {
  say(`produce ${COUNT} records to a topic that does not exist yet (auto-create)`)
  const topic = `${TAG}-main`
  const records = fixture(TAG)
  const t0 = Date.now()
  const res = await produce(topic, records)
  info(`send() resolved in ${Date.now() - t0}ms with ${res.length} partition results`)
  console.log(`  first result: ${JSON.stringify(res[0])}`)
  check(
    res.every((r) => r.errorCode === 0),
    `every partition result carries errorCode 0 (${res.map((r) => r.errorCode).join(',')})`,
  )
  const parts = new Set(res.map((r) => r.partition))
  check(parts.size === PARTITIONS, `all ${PARTITIONS} partitions were written (${parts.size})`)
  check(
    res.every((r) => String(r.baseOffset) === '0'),
    'every partition of a brand-new topic starts at base offset 0 ' +
      `(${res.map((r) => `p${r.partition}=${r.baseOffset}`).join(' ')})`,
  )
  return { topic, records }
}

// 1 (compression half).
async function scenarioCompression() {
  say('one topic per compression codec this build supports')
  info(`librdkafka ${Confluent.librdkafkaVersion} features: ${FEATURES.join(',')}`)
  const codecs = [
    ['gzip', CompressionTypes.GZIP],
    ['snappy', CompressionTypes.SNAPPY],
    ['lz4', CompressionTypes.LZ4],
    ['zstd', CompressionTypes.ZSTD],
  ]
  for (const [name, codec] of codecs) {
    if (!HAS(name)) {
      info(`${name}: NOT IN THIS BUILD — skipped (a binding build property, not the facade)`)
      continue
    }
    const topic = `${TAG}-${name}`
    const records = fixture(`${TAG}-${name}`, 64)
    const before = trace.errors.length
    try {
      const res = await produce(topic, records, { compression: codec })
      check(res.every((r) => r.errorCode === 0), `${name}: every partition acknowledged`)
    } catch (e) {
      bad(`${name}: send threw ${e.message}`)
      continue
    }
    const declined = trace.errors
      .slice(before)
      .filter((l) => /does not support compression|compression type/i.test(l))
    if (declined.length) {
      info(`${name}: librdkafka declined the codec and sent it UNCOMPRESSED — ${declined[0].slice(0, 180)}`)
      info(`${name}: that is the Fetch-v6 cap doing its job; the records below still have to be exact`)
    }
    const consumer = newConsumer(`${GROUP}-${name}`)
    await deadline(consumer.connect(), 45000, `${name} consumer connect`)
    await consumer.subscribe({ topics: [topic] })
    const seen = await drain(consumer, records.length, { timeoutMs: 120000 })
    await deadline(consumer.disconnect(), 45000, `${name} consumer disconnect`)
    verifyRoundTrip(seen, records, name)
  }
  // The codec conversation, verbatim, when the raw trace was kept. This is the
  // only place the zstd question gets a real answer: did librdkafka COMPRESS, or
  // did it decline and send plaintext? The records are exact either way, so the
  // assertions above cannot tell you and should not try.
  const codecTalk = trace.compressionLines()
  if (codecTalk.length) {
    info(`librdkafka's own words about compression (${codecTalk.length} lines):`)
    for (const l of codecTalk.slice(0, 8)) console.log(`     ${l.slice(0, 200)}`)
  } else if (process.env.NEGOTIATED_TRACE_FILE) {
    info('librdkafka said nothing about compression: no codec was declined or downgraded')
  } else {
    info('set NEGOTIATED_TRACE_FILE=<path> to capture what librdkafka said about each codec')
  }
}

// 2 + 3 + 5. The group, the commits, the watermarks, the seek.
async function scenarioGroup(topic, records) {
  say(`a consumer group reads all ${COUNT} records back (group ${GROUP})`)
  const consumer = newConsumer(GROUP)
  const t0 = Date.now()
  await deadline(consumer.connect(), 45000, 'consumer connect')
  await consumer.subscribe({ topics: [topic] })
  const seen = await drain(consumer, COUNT, { timeoutMs: 180000 })
  info(`group formed and drained in ${Date.now() - t0}ms (group.initial.rebalance.delay is 3000ms)`)
  const assignment = consumer.assignment()
  check(
    assignment.length === PARTITIONS,
    `the sole member holds all ${PARTITIONS} partitions ` +
      `(${assignment.map((a) => a.partition).sort((a, b) => a - b).join(',')})`,
  )
  verifyRoundTrip(seen, records, 'group')

  say('commit the whole read, then read the commits back over OffsetFetch')
  const last = new Map()
  for (const m of seen) last.set(m.partition, Math.max(last.get(m.partition) ?? -1, m.offset))
  const offsets = [...last.entries()].map(([partition, offset]) => ({
    topic, partition, offset: String(offset + 1),
  }))
  await deadline(consumer.commitOffsets(offsets), 30000, 'commitOffsets')
  ok(`commitOffsets of ${offsets.length} partitions returned`)
  const committed = await deadline(
    consumer.committed(Array.from({ length: PARTITIONS }, (_, p) => ({ topic, partition: p }))),
    30000,
    'committed()',
  )
  info('committed: ' + committed.map((t) => `p${t.partition}=${t.offset}`).join(' '))
  const sum = committed.reduce((a, t) => a + Number(t.offset), 0)
  check(committed.every((t) => Number(t.offset) > 0), 'every partition has a committed offset > 0')
  check(sum === COUNT, `the committed offsets sum to the ${COUNT} records read (${sum})`)

  say('earliest and latest offsets (ListOffsets), and seek')
  const expectP0 = records.filter((r) => r.partition === 0).length
  // The kafkaJS layer does not expose queryWatermarkOffsets; the admin client's
  // fetchTopicOffsets is the same ListOffsets conversation, so ask it there.
  const admin = client().admin()
  let watermarks = null
  try {
    await deadline(admin.connect(), 45000, 'admin connect')
    watermarks = await deadline(admin.fetchTopicOffsets(topic), 45000, 'fetchTopicOffsets')
    const p0 = watermarks.find((w) => w.partition === 0)
    info(`admin.fetchTopicOffsets p0: ${JSON.stringify(p0)}`)
    check(
      Number(p0.low) === 0 && Number(p0.high) === expectP0,
      `partition 0 is [0, ${expectP0}) over ListOffsets (got [${p0.low}, ${p0.high}))`,
    )
    const total = watermarks.reduce((a, w) => a + (Number(w.high) - Number(w.low)), 0)
    check(total === COUNT, `the watermarks account for all ${COUNT} records (${total})`)
  } catch (e) {
    info(`admin.fetchTopicOffsets failed: ${e.message}`)
    info('if this names an API key, that key is in versions.rs or deliberately absent — check before filing')
    bad(`admin.fetchTopicOffsets: ${e.message}`)
  } finally {
    try {
      await deadline(admin.disconnect(), 30000, 'admin disconnect')
    } catch {}
  }

  // seek: rewind partition 0 to the middle and read the tail again.
  const mid = Math.floor(expectP0 / 2)
  const tail = []
  consumer.pause([{ topic }])
  consumer.seek({ topic, partition: 0, offset: String(mid) })
  consumer.resume([{ topic }])
  const until = Date.now() + 60000
  const before = seen.length
  while (seen.length - before < expectP0 - mid && Date.now() < until) await sleep(100)
  for (const m of seen.slice(before)) if (m.partition === 0) tail.push(m)
  check(
    tail.length >= expectP0 - mid && tail[0]?.offset === mid,
    `seek(p0, ${mid}) replayed from offset ${tail[0]?.offset} and delivered ` +
      `${tail.length} records again (wanted >= ${expectP0 - mid})`,
  )
  const wantKeys = records.filter((r) => r.partition === 0).slice(mid).map((r) => r.key.toString())
  check(
    JSON.stringify(tail.slice(0, wantKeys.length).map((m) => m.key.toString())) ===
      JSON.stringify(wantKeys),
    'the records after the seek are exactly the fixture tail of partition 0',
  )
  await deadline(consumer.disconnect(), 45000, 'consumer disconnect')
  ok('the first member disconnected (LeaveGroup)')
}

// 3 (second half). A new member of the same group resumes, it does not replay.
async function scenarioResume(topic) {
  say('a second member of the same group starts where the first stopped')
  const more = fixture(`${TAG}-more`, 32)
  const res = await produce(topic, more)
  check(res.every((r) => r.errorCode === 0), '32 more records produced')

  const consumer = newConsumer(GROUP)
  await deadline(consumer.connect(), 45000, 'consumer connect')
  await consumer.subscribe({ topics: [topic] })
  const seen = await drain(consumer, more.length, { timeoutMs: 180000, linger: 6000 })
  await deadline(consumer.disconnect(), 45000, 'consumer disconnect')
  const replayed = seen.filter((m) => !m.key.toString().startsWith(`${TAG}-more-`))
  check(replayed.length === 0, `nothing already committed was replayed (${replayed.length} old records)`)
  check(seen.length === more.length, `exactly the ${more.length} new records (${seen.length})`)
  verifyRoundTrip(seen.filter((m) => m.key.toString().startsWith(`${TAG}-more-`)), more, 'resume')
}

async function scenarioFreshGroup(topic, total) {
  say('a group that never committed starts at earliest')
  const consumer = newConsumer(`${GROUP}-fresh`)
  await deadline(consumer.connect(), 45000, 'consumer connect')
  await consumer.subscribe({ topics: [topic] })
  const seen = await drain(consumer, total, { timeoutMs: 180000 })
  await deadline(consumer.disconnect(), 45000, 'consumer disconnect')
  check(seen.length === total, `a fresh group read all ${total} records from the start (${seen.length})`)
}

// 6. SASL/PLAIN, over TLS when this build has ssl.
async function scenarioSasl() {
  const a = auth()
  if (!a) {
    say('SASL lane')
    info('KAFKA_SASL_BOOTSTRAP is unset — skipped')
    return
  }
  say(`SASL/PLAIN over ${a['security.protocol']} at ${a['bootstrap.servers']}`)
  if (a['security.protocol'].includes('ssl') && !HAS('ssl')) {
    bad('sasl_ssl was asked for but this build has no ssl feature')
    return
  }
  const topic = `${TAG}-sasl`
  const records = fixture(`${TAG}-sasl`, 40)
  try {
    const res = await produce(topic, records, { clientExtra: a })
    check(res.every((r) => r.errorCode === 0), `authenticated produce: ${res.length} partitions acknowledged`)
  } catch (e) {
    bad(`authenticated produce failed: ${e.message}`)
    return
  }
  const consumer = newConsumer(`${GROUP}-sasl`, a)
  await deadline(consumer.connect(), 60000, 'sasl consumer connect')
  await consumer.subscribe({ topics: [topic] })
  const seen = await drain(consumer, records.length, { timeoutMs: 180000 })
  await deadline(consumer.disconnect(), 45000, 'sasl consumer disconnect')
  verifyRoundTrip(seen, records, 'sasl')

  say('a wrong SASL password is refused')
  const before = trace.errors.length
  const thrown = await produce(`${topic}-wrong`, records.slice(0, 1), {
    clientExtra: { ...a, 'sasl.password': `${a['sasl.password']}-WRONG` },
  }).then(
    () => 'ACCEPTED (no error at all)',
    (e) => String(e.message || e),
  )
  // Two different questions, and they have different answers here.
  //
  // 1. Was the credential refused? That is the facade's half, and the answer is
  //    in librdkafka's own stream, where the broker's reason arrives verbatim.
  const authLines = trace.errors
    .slice(before)
    .filter((l) => /SASL authentication error|_AUTHENTICATION/i.test(l))
  for (const l of authLines.slice(0, 2)) info(`librdkafka: ${l.slice(0, 260)}`)
  check(
    authLines.length > 0,
    'the credential was refused, and the broker\'s own reason reached the client',
  )
  check(
    authLines.some((l) => /401|bearer token|credential/i.test(l)),
    'the refusal names WHY (HTTP 401 / the password must be the bearer token), not just "auth failed"',
  )
  // 2. Does the promisified API hand that reason to application code? No — and
  //    this is worth knowing before you debug a bad token in production. See the
  //    file header.
  info(`what the send() promise rejected with: ${String(thrown).slice(0, 200)}`)
  if (!/authentication|401|credential/i.test(String(thrown))) {
    info(
      'the thrown error is GENERIC. librdkafka treats an auth failure as retriable and keeps ' +
        'reconnecting, so the kafkaJS layer surfaces whatever the eventual timeout was rather ' +
        'than the SASL reason. The reason is only in the log stream. A client-layer property; ' +
        'kafkajs itself raises SASLAuthenticationError here.',
    )
  } else {
    ok('the thrown error itself names the authentication failure')
  }
}

// The idempotence footgun, recorded not asserted.
async function scenarioIdempotence() {
  say('idempotent: true (unsupported: InitProducerId is M7)')
  // `eos` is librdkafka's idempotence/transactions facility: without it the
  // fatal error has no explanation in the debug stream, which defeats the point
  // of running this scenario at all.
  const kafka = client({ debug: 'protocol,broker,eos' })
  const producer = kafka.producer({ kafkaJS: { idempotent: true, acks: -1 } })
  const outcome = await Promise.race([
    (async () => {
      await producer.connect()
      await producer.send({
        topic: `${TAG}-idem`,
        messages: [{ key: Buffer.from('k'), value: Buffer.from('idempotent-probe'), partition: 0 }],
      })
      return 'the send SUCCEEDED with idempotence on'
    })().catch((e) => `threw: ${e.message}`),
    sleep(30000).then(() => 'no resolution within 30s'),
  ])
  info(`outcome: ${String(outcome).slice(0, 300)}`)
  // librdkafka's `eos` facility narrates exactly why, and it is worth quoting:
  // it never puts an InitProducerId on the wire at all. It reads ApiVersions,
  // sees the key is absent, and fails the producer locally and immediately.
  const eos = trace.linesMatching(/idempot|producerid|Fatal error/i)
  for (const l of eos.slice(0, 8)) console.log(`     ${l.slice(0, 220)}`)
  if (!eos.length) info('set NEGOTIATED_TRACE_FILE=<path> to capture librdkafka\'s eos narration')
  try {
    await deadline(producer.disconnect(), 20000, 'idem disconnect')
  } catch {}
}

// ======================================================================= main
async function main() {
  console.log(
    `@confluentinc/kafka-javascript (librdkafka ${Confluent.librdkafkaVersion}) ` +
      `against ${BOOTSTRAP}, run ${RUN}`,
  )
  console.log(`features: ${FEATURES.join(',')}`)
  const { topic, records } = await scenarioProduce()
  await scenarioCompression()
  await scenarioGroup(topic, records)
  await scenarioResume(topic)
  await scenarioFreshGroup(topic, COUNT + 32)
  await scenarioSasl()
  if (process.env.RUN_IDEMPOTENCE === '1') await scenarioIdempotence()
  trace.report()
  trace.reportErrors()
  trace.flush()
  return finish('@confluentinc/kafka-javascript')
}

main()
  .then((code) => process.exit(code))
  .catch((e) => {
    bad(`unhandled: ${e.stack || e.message}`)
    trace.report()
    trace.reportErrors()
    finish('@confluentinc/kafka-javascript')
    process.exit(1)
  })
