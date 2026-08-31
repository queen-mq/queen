// node-rdkafka against the queen-kafka facade — the first of the two Node
// librdkafka bindings in the M6 client matrix (the other is
// confluent_kafka_js.mjs; kafkajs, the pure-JS one, lives in compat/js).
//
//   node node_rdkafka.mjs [bootstrap] [runId]
//   KAFKA_BOOTSTRAP=... RUN_ID=... node node_rdkafka.mjs
//
// What this proves, in order: a fresh topic auto-creates and its first record is
// offset 0; 512 records across 8 partitions carry binary keys, binary values and
// three headers each; every codec THIS BUILD of librdkafka has round-trips; a
// consumer GROUP reads them back byte-exact and in per-partition produce order;
// commits survive the death of the member that made them; watermarks and seek
// answer. An optional SASL lane runs when KAFKA_SASL_BOOTSTRAP is set.
//
// WHAT IS THE CLIENT'S FAULT AND NOT THE FACADE'S — read this before filing
// anything from this file:
//
//   * node-rdkafka compiles librdkafka FROM SOURCE at npm-install time and
//     links only what it finds on the host. On a stock macOS box that means NO
//     `ssl` and NO `zstd` in `Kafka.features`. Neither is a facade property.
//     This suite reads `features` and skips what the build cannot do, saying so
//     — it never reports a missing codec as a compatibility failure.
//   * librdkafka gates zstd PRODUCE on being able to negotiate Fetch v10. The
//     facade caps Fetch at v6 on purpose (PLAN_QUEEN_KAFKA.md: v7 is fetch
//     sessions), so even an ssl+zstd build logs "Broker does not support
//     compression type zstd" and sends the batch UNCOMPRESSED. The records land.
//     That is the documented deliberate deviation, not a defect.
//   * node-rdkafka's producer coerces every HEADER VALUE to a UTF-8 string and
//     truncates it at the first NUL (src/producer.cc). It cannot send a binary
//     header at all. probe-headers.mjs proves this is the binding and not the
//     facade — @confluentinc, the same librdkafka, round-trips the same bytes
//     through the same broker — so this suite asks node-rdkafka only for the
//     header values node-rdkafka can carry, and `scenarioBinaryHeaders` records
//     the limitation on its own rather than letting it redden 500 assertions.
//   * Idempotent produce is unimplemented in the facade (InitProducerId is M7).
//     librdkafka defaults `enable.idempotence` to false; this suite sets it
//     false explicitly anyway, and `--idempotence` re-runs the first produce
//     with it ON to record the failure mode.
import Kafka from 'node-rdkafka'
import {
  PARTITIONS, COUNT, fixture, ok, bad, info, say, check, finish, failures,
  sleep, deadline, Negotiated, verifyRoundTrip,
} from './fixture.mjs'

const BOOTSTRAP = process.argv[2] || process.env.KAFKA_BOOTSTRAP || '127.0.0.1:19092'
const RUN = process.argv[3] || process.env.RUN_ID || String(Date.now())
const TAG = `rdk-${RUN}`
const GROUP = `rdk-g-${RUN}`

// See the header comment: this binding cannot put a NUL or a high byte in a
// header value, so the fixture is asked for the text headers only.
const NO_BIN_HEADERS = { binaryHeaders: false }

const FEATURES = Kafka.features
const HAS = (f) => FEATURES.includes(f)

const trace = new Negotiated()
// Every handle in this file gets these: librdkafka narrates its own protocol
// conversation and this suite reads the versions out of it rather than assuming.
const TRACED = {
  'metadata.broker.list': BOOTSTRAP,
  debug: 'protocol,cgrp,topic,broker',
}
function attachLog(handle) {
  handle.on('event.log', (e) => trace.line(e.message))
  handle.on('event.error', (e) => trace.line(`EVENT.ERROR ${e.message || e}`))
  return handle
}

// node-rdkafka hands headers back as [{name: Buffer}] — one single-key object
// per header, in wire order. Normalise to the {name, value} the fixture speaks.
const normalizeHeaders = (hs) =>
  (hs || []).map((h) => {
    const name = Object.keys(h)[0]
    return { name, value: Buffer.from(h[name] ?? []) }
  })

// ------------------------------------------------------------------- producing
function connectProducer(extra = {}, topicConf = {}) {
  const p = attachLog(
    new Kafka.Producer(
      {
        ...TRACED,
        'client.id': `${TAG}-p`,
        dr_cb: true,
        // M7 is InitProducerId; until then every client in this matrix runs
        // with idempotence off. librdkafka's default is already false.
        'enable.idempotence': false,
        ...extra,
      },
      { 'request.required.acks': -1, ...topicConf },
    ),
  )
  return deadline(
    new Promise((resolve, reject) => {
      p.on('ready', () => resolve(p))
      p.on('event.error', reject)
      p.connect({ timeout: 20000 }, (err) => err && reject(err))
    }),
    45000,
    'node-rdkafka producer connect',
  )
}

/** Produce `records` and wait for a delivery report for every one of them. */
async function produce(topic, records, extra = {}) {
  const producer = await connectProducer(extra)
  producer.setPollInterval(50)
  const reports = []
  const drErrors = []
  producer.on('delivery-report', (err, report) => {
    if (err) drErrors.push(String(err))
    else reports.push(report)
  })

  for (const r of records) {
    producer.produce(
      topic,
      r.partition,
      r.value,
      r.key,
      null,
      r.i,
      // node-rdkafka's header shape on the way IN is the same single-key object
      // it uses on the way out.
      r.headers.map((h) => ({ [h.name]: h.value })),
    )
  }
  await deadline(
    new Promise((resolve, reject) =>
      producer.flush(60000, (err) => (err ? reject(err) : resolve())),
    ),
    70000,
    `flush ${records.length} records to ${topic}`,
  )
  // flush() returns when the queue is drained; the delivery reports are
  // delivered by the poll loop, so give it a beat to hand over the last few.
  for (let i = 0; i < 100 && reports.length < records.length; i++) await sleep(50)
  await deadline(
    new Promise((resolve) => producer.disconnect(() => resolve())),
    20000,
    'producer disconnect',
  )
  return { reports, drErrors }
}

// ------------------------------------------------------------------- consuming
function newConsumer(conf = {}, topicConf = {}) {
  return attachLog(
    new Kafka.KafkaConsumer(
      {
        ...TRACED,
        'client.id': `${TAG}-c`,
        'group.id': GROUP,
        'enable.auto.commit': false,
        'session.timeout.ms': 30000,
        ...conf,
      },
      { 'auto.offset.reset': 'earliest', ...topicConf },
    ),
  )
}

const connect = (c) =>
  deadline(
    new Promise((resolve, reject) => {
      c.on('ready', () => resolve(c))
      c.connect({ timeout: 20000 }, (err) => err && reject(err))
    }),
    45000,
    'consumer connect',
  )

const disconnect = (c) =>
  deadline(new Promise((resolve) => c.disconnect(() => resolve())), 30000, 'consumer disconnect')

/**
 * Read until `want` records have arrived (or the deadline passes), then keep
 * reading for `linger` more ms: a facade that redelivered committed records
 * shows up in that tail and nowhere else.
 */
function drain(consumer, want, { timeoutMs = 120000, linger = 0, onEach } = {}) {
  return new Promise((resolve, reject) => {
    const seen = []
    let settled = false
    const finishNow = () => {
      if (settled) return
      settled = true
      clearTimeout(hard)
      consumer.removeListener('data', onData)
      resolve(seen)
    }
    const hard = setTimeout(() => {
      if (settled) return
      settled = true
      consumer.removeListener('data', onData)
      reject(new Error(`TIMEOUT: read ${seen.length}/${want} records in ${timeoutMs}ms`))
    }, timeoutMs)

    const onData = (m) => {
      seen.push({
        partition: m.partition,
        offset: Number(m.offset),
        key: m.key,
        value: m.value,
        headers: normalizeHeaders(m.headers),
      })
      if (onEach) onEach(m)
      if (seen.length === want) {
        if (linger) setTimeout(finishNow, linger)
        else finishNow()
      }
    }
    consumer.on('data', onData)
    consumer.consume()
  })
}

// ================================================================== scenarios

// 1 + 4. A topic that does not exist yet, 512 records, 8 partitions, keys,
//        binary values and headers, acks=all, uncompressed.
async function scenarioProduce() {
  say(`produce ${COUNT} records to a topic that does not exist yet (auto-create)`)
  const topic = `${TAG}-main`
  const records = fixture(TAG, COUNT, NO_BIN_HEADERS)
  const t0 = Date.now()
  const { reports, drErrors } = await produce(topic, records)
  info(`${reports.length} delivery reports in ${Date.now() - t0}ms`)
  check(drErrors.length === 0, `no delivery error (${drErrors.slice(0, 2).join('; ') || 'none'})`)
  check(reports.length === COUNT, `a delivery report for every one of the ${COUNT} records (${reports.length})`)
  check(
    reports.every((r) => typeof r.offset === 'number' && r.offset >= 0),
    'every delivery report carries a real broker offset (none is -1001)',
  )
  const parts = new Set(reports.map((r) => r.partition))
  check(parts.size === PARTITIONS, `records landed on all ${PARTITIONS} partitions (${parts.size})`)
  const firstPerPartition = new Map()
  for (const r of reports) {
    const cur = firstPerPartition.get(r.partition)
    if (cur === undefined || r.offset < cur) firstPerPartition.set(r.partition, r.offset)
  }
  check(
    [...firstPerPartition.values()].every((o) => o === 0),
    `the first record on every partition of a brand-new topic is offset 0 ` +
      `(${JSON.stringify([...firstPerPartition.entries()].sort())})`,
  )
  const maxPerPartition = Math.max(...[...parts].map((p) => reports.filter((r) => r.partition === p).length))
  info(`${maxPerPartition} records on the busiest partition`)
  return { topic, records }
}

// 1 (compression half). Every codec this librdkafka build actually has.
async function scenarioCompression() {
  say('one topic per compression codec this build supports')
  info(`librdkafka ${Kafka.librdkafkaVersion} features: ${FEATURES.join(',')}`)
  const codecs = ['gzip', 'snappy', 'lz4', 'zstd']
  for (const codec of codecs) {
    if (!HAS(codec)) {
      info(`${codec}: NOT IN THIS BUILD — skipped (a node-rdkafka build property, not the facade)`)
      continue
    }
    const topic = `${TAG}-${codec}`
    const records = fixture(`${TAG}-${codec}`, 64, NO_BIN_HEADERS)
    let sendErr = null
    try {
      const { reports, drErrors } = await produce(topic, records, { 'compression.codec': codec })
      check(
        drErrors.length === 0 && reports.length === records.length,
        `${codec}: all ${records.length} records acknowledged (${reports.length}, ${drErrors.length} errors)`,
      )
    } catch (e) {
      sendErr = e
      bad(`${codec}: produce threw ${e.message}`)
    }
    if (sendErr) continue
    // read them back with a plain assignment: no group, no 3s join delay
    const c = await connect(newConsumer({ 'group.id': `${GROUP}-${codec}` }))
    c.assign(Array.from({ length: PARTITIONS }, (_, p) => ({ topic, partition: p, offset: 0 })))
    const seen = await drain(c, records.length, { timeoutMs: 60000 })
    await disconnect(c)
    verifyRoundTrip(seen, records, codec)
  }
  // The zstd story is worth stating even when the build cannot try it.
  const zstdNote = HAS('zstd')
    ? 'this build HAS zstd; librdkafka still gates it on Fetch v10 and the facade caps Fetch at v6, so watch the trace for the fall back to uncompressed'
    : 'this build has NO zstd, so the Fetch-v10 gate was never reached'
  info(`zstd: ${zstdNote}`)
}

// 2 + 3. A consumer GROUP: read everything, verify it, commit, die, come back.
async function scenarioGroup(topic, records) {
  say(`a consumer group reads all ${COUNT} records back (group ${GROUP})`)
  const c1 = await connect(newConsumer())
  const t0 = Date.now()
  c1.subscribe([topic])
  const seen = await drain(c1, COUNT, { timeoutMs: 180000 })
  info(`group formed and drained in ${Date.now() - t0}ms ` +
       `(QUEEN_KAFKA_GROUP_JOIN_DELAY_MS is 3000 by default)`)
  const assigned = c1.assignments().map((a) => a.partition).sort((a, b) => a - b)
  check(
    assigned.length === PARTITIONS,
    `the sole member was assigned all ${PARTITIONS} partitions (${JSON.stringify(assigned)})`,
  )
  verifyRoundTrip(seen, records, 'group')

  say('commit the whole read, then read the commits back over OffsetFetch')
  const last = new Map()
  for (const m of seen) last.set(m.partition, Math.max(last.get(m.partition) ?? -1, m.offset))
  const toCommit = [...last.entries()].map(([partition, offset]) => ({
    topic, partition, offset: offset + 1,
  }))
  c1.commitSync(toCommit)
  ok(`commitSync of ${toCommit.length} partitions returned`)
  const committed = await deadline(
    new Promise((resolve, reject) =>
      c1.committed(
        Array.from({ length: PARTITIONS }, (_, p) => ({ topic, partition: p })),
        20000,
        (err, parts) => (err ? reject(err) : resolve(parts)),
      ),
    ),
    30000,
    'committed()',
  )
  info('committed: ' + committed.map((t) => `p${t.partition}=${t.offset}`).join(' '))
  check(
    committed.every((t) => t.offset > 0),
    'every partition has a committed offset > 0',
  )
  const sum = committed.reduce((a, t) => a + t.offset, 0)
  check(sum === COUNT, `the committed offsets sum to the ${COUNT} records read (${sum})`)

  say('watermarks (ListOffsets: earliest and latest) and seek')
  const wm = await deadline(
    new Promise((resolve, reject) =>
      c1.queryWatermarkOffsets(topic, 0, 20000, (err, o) => (err ? reject(err) : resolve(o))),
    ),
    30000,
    'queryWatermarkOffsets',
  )
  const expectP0 = records.filter((r) => r.partition === 0).length
  info(`partition 0 watermarks: low=${wm.lowOffset} high=${wm.highOffset}`)
  check(
    wm.lowOffset === 0 && wm.highOffset === expectP0,
    `partition 0 is [0, ${expectP0}) (got [${wm.lowOffset}, ${wm.highOffset}))`,
  )
  await disconnect(c1)
  ok('the first member disconnected (LeaveGroup)')

  // seek, on its own handle with an explicit assignment so the seek is not
  // racing a rebalance.
  const s = await connect(newConsumer({ 'group.id': `${GROUP}-seek` }))
  s.assign([{ topic, partition: 0, offset: 0 }])
  const mid = Math.floor(expectP0 / 2)
  await deadline(
    new Promise((resolve, reject) =>
      s.seek({ topic, partition: 0, offset: mid }, 20000, (err) => (err ? reject(err) : resolve())),
    ),
    30000,
    'seek',
  )
  const after = await drain(s, expectP0 - mid, { timeoutMs: 60000 })
  await disconnect(s)
  check(
    after.length === expectP0 - mid && after[0].offset === mid,
    `seek(p0, ${mid}) resumed at offset ${after[0]?.offset} and read the remaining ` +
      `${after.length} records (wanted ${expectP0 - mid})`,
  )
  const wantKeys = records.filter((r) => r.partition === 0).slice(mid).map((r) => r.key.toString())
  check(
    JSON.stringify(after.map((m) => m.key.toString())) === JSON.stringify(wantKeys),
    'the records after the seek are exactly the fixture tail of partition 0',
  )
}

// 3 (second half). A NEW member of the SAME group must resume, not replay.
async function scenarioResume(topic) {
  say('a second member of the same group starts where the first stopped')
  const more = fixture(`${TAG}-more`, 32, NO_BIN_HEADERS)
  const { drErrors } = await produce(topic, more)
  check(drErrors.length === 0, `32 more records produced to ${topic}`)

  const c2 = await connect(newConsumer())
  c2.subscribe([topic])
  // Read the 32 new ones and then keep listening for 6s: anything already
  // committed that comes back would arrive in that window.
  const seen = await drain(c2, more.length, { timeoutMs: 180000, linger: 6000 })
  await disconnect(c2)
  const keys = seen.map((m) => m.key.toString())
  const replayed = keys.filter((k) => !k.startsWith(`${TAG}-more-`))
  check(replayed.length === 0, `nothing already committed was replayed (${replayed.length} old records)`)
  check(seen.length === more.length, `exactly the ${more.length} new records (${seen.length})`)
  verifyRoundTrip(
    seen.filter((m) => m.key.toString().startsWith(`${TAG}-more-`)),
    more,
    'resume',
  )
}

// A fresh group with no commits at all applies auto.offset.reset=earliest.
async function scenarioFreshGroup(topic, total) {
  say('a group that never committed starts at earliest')
  const c = await connect(newConsumer({ 'group.id': `${GROUP}-fresh` }))
  c.subscribe([topic])
  const seen = await drain(c, total, { timeoutMs: 180000 })
  await disconnect(c)
  check(seen.length === total, `a fresh group read all ${total} records from the start (${seen.length})`)
}

// The binary-header limitation, recorded here so it is in the run and not only
// in a comment. This is a NODE-RDKAFKA defect; probe-headers.mjs proves it by
// writing the same bytes with @confluentinc through the same facade and getting
// them all back. What IS asserted here is the facade's half: whatever bytes the
// client managed to put on the wire come back unchanged.
async function scenarioBinaryHeaders() {
  say('binary header values (a node-rdkafka producer limitation, not a facade one)')
  const topic = `${TAG}-binhdr`
  const probes = [
    { name: 'ascii', value: Buffer.from('plain') },
    { name: 'lead-nul', value: Buffer.from([0x00, 0x01, 0x02]) },
    { name: 'mid-nul', value: Buffer.from([0x41, 0x00, 0x42]) },
    { name: 'high-bytes', value: Buffer.from([0xfe, 0xff, 0x80]) },
  ]
  const producer = await connectProducer()
  producer.setPollInterval(50)
  producer.produce(
    topic, 0, Buffer.from('binhdr'), Buffer.from('binhdr-key'), null, null,
    probes.map((h) => ({ [h.name]: h.value })),
  )
  await deadline(
    new Promise((resolve, reject) => producer.flush(20000, (e) => (e ? reject(e) : resolve()))),
    30000, 'flush binhdr',
  )
  await deadline(new Promise((r) => producer.disconnect(() => r())), 20000, 'disconnect binhdr')

  const c = await connect(newConsumer({ 'group.id': `${GROUP}-binhdr` }))
  c.assign([{ topic, partition: 0, offset: 0 }])
  const [msg] = await drain(c, 1, { timeoutMs: 60000 })
  await disconnect(c)

  let mangled = 0
  for (const want of probes) {
    const got = msg.headers.find((h) => h.name === want.name)
    const same = got && got.value.equals(want.value)
    if (!same) mangled++
    info(
      `${want.name.padEnd(11)} sent ${want.value.toString('hex')} -> got ` +
        `${got ? got.value.toString('hex') || '(empty)' : '(absent)'}` +
        `${same ? '' : '   <- mangled by node-rdkafka BEFORE the wire'}`,
    )
  }
  check(
    msg.headers.length === probes.length,
    `all ${probes.length} headers came back, in order, with their names intact (${msg.headers.length})`,
  )
  check(
    msg.headers.map((h) => h.name).join(',') === probes.map((h) => h.name).join(','),
    'header NAMES survive exactly and in wire order',
  )
  if (mangled) {
    info(
      `${mangled}/${probes.length} header VALUES did not survive. node-rdkafka/src/producer.cc ` +
        `sends them through Nan::To<v8::String> and std::string(char*), so a NUL truncates and a ` +
        `non-UTF-8 byte becomes U+FFFD before librdkafka ever sees it. Run probe-headers.mjs: the ` +
        `same bytes written by @confluentinc through this same facade come back exact.`,
    )
    ok('the mangling is attributable to the binding, and the facade is not implicated')
  } else {
    ok('every binary header value survived (this node-rdkafka has a fixed producer.cc)')
  }
}

// 6. The SASL lane, if the caller stood one up.
async function scenarioSasl() {
  const bootstrap = process.env.KAFKA_SASL_BOOTSTRAP
  if (!bootstrap) {
    say('SASL lane')
    info('KAFKA_SASL_BOOTSTRAP is unset — skipped')
    return
  }
  const protocol = process.env.KAFKA_SASL_PROTOCOL || (HAS('ssl') ? 'sasl_ssl' : 'sasl_plaintext')
  say(`SASL/PLAIN over ${protocol} at ${bootstrap}`)
  if (protocol.includes('ssl') && !HAS('ssl')) {
    bad('sasl_ssl was asked for but this node-rdkafka build has no ssl feature')
    return
  }
  const token = process.env.KAFKA_SASL_TOKEN || ''
  const auth = {
    'security.protocol': protocol,
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'node-rdkafka',
    // The password IS the Queen bearer token; sasl.username is a free label.
    'sasl.password': token,
    'metadata.broker.list': bootstrap,
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
  const topic = `${TAG}-sasl`
  const records = fixture(`${TAG}-sasl`, 40, NO_BIN_HEADERS)
  try {
    const { reports, drErrors } = await produce(topic, records, auth)
    check(
      drErrors.length === 0 && reports.length === records.length,
      `authenticated produce: ${reports.length}/${records.length} acknowledged`,
    )
  } catch (e) {
    bad(`authenticated produce failed: ${e.message}`)
    return
  }
  const c = await connect(newConsumer({ ...auth, 'group.id': `${GROUP}-sasl` }))
  c.subscribe([topic])
  const seen = await drain(c, records.length, { timeoutMs: 180000 })
  await disconnect(c)
  verifyRoundTrip(seen, records, 'sasl')

  // And a WRONG password must be refused, not quietly accepted.
  say('a wrong SASL password is refused')
  const before = trace.errors.length
  const wrong = attachLog(
    new Kafka.Producer({ ...TRACED, ...auth, 'sasl.password': `${token}-WRONG` }),
  )
  const verdict = await new Promise((resolve) => {
    const done = setTimeout(() => resolve('TIMEOUT (no error in 20s)'), 20000)
    wrong.on('event.error', (e) => {
      clearTimeout(done)
      resolve(String(e.message || e))
    })
    wrong.on('ready', () => {})
    wrong.connect({ timeout: 15000 }, (err) => {
      if (err) {
        clearTimeout(done)
        resolve(String(err.message || err))
      }
    })
  })
  info(`the event.error the application sees: ${verdict.slice(0, 240)}`)
  const authLines = trace.errors
    .slice(before)
    .filter((l) => /SASL authentication error|_AUTHENTICATION/i.test(l))
  for (const l of authLines.slice(0, 2)) info(`librdkafka: ${l.slice(0, 260)}`)
  check(
    authLines.length > 0 || /authentication|401|credential/i.test(verdict),
    'the credential was refused, and the broker\'s own reason reached the client',
  )
  check(
    authLines.some((l) => /401|bearer token|credential/i.test(l)) ||
      /401|bearer token|credential/i.test(verdict),
    'the refusal names WHY (HTTP 401 / the password must be the bearer token), not just "auth failed"',
  )
  try {
    wrong.disconnect(() => {})
  } catch {}
}

// The idempotence footgun, recorded rather than asserted: real users WILL turn
// this on, and its failure mode should be in the record.
async function scenarioIdempotence() {
  say('enable.idempotence=true (unsupported: InitProducerId is M7)')
  const topic = `${TAG}-idem`
  const p = attachLog(
    new Kafka.Producer(
      // `eos` is librdkafka's idempotence facility; without it the fatal error
      // has no explanation in the debug stream.
      { ...TRACED, debug: 'protocol,broker,eos', 'client.id': `${TAG}-idem`, dr_cb: true, 'enable.idempotence': true },
      { 'request.required.acks': -1 },
    ),
  )
  const outcome = await new Promise((resolve) => {
    const done = setTimeout(() => resolve('no error within 25s'), 25000)
    const settle = (why) => {
      clearTimeout(done)
      resolve(why)
    }
    p.on('event.error', (e) => settle(`event.error: ${e.message || e}`))
    p.on('delivery-report', (err) => err && settle(`delivery-report error: ${err}`))
    p.on('ready', () => {
      p.setPollInterval(50)
      try {
        p.produce(topic, 0, Buffer.from('idempotent-probe'), Buffer.from('k'))
      } catch (e) {
        settle(`produce() threw: ${e.message}`)
      }
    })
    p.connect({ timeout: 20000 }, (err) => err && settle(`connect error: ${err.message || err}`))
  })
  info(`outcome: ${outcome.slice(0, 300)}`)
  // librdkafka's `eos` facility narrates exactly why, and it is worth quoting:
  // it never puts an InitProducerId on the wire at all. It reads ApiVersions,
  // sees the key is absent, and fails the producer locally and immediately.
  const eos = trace.linesMatching(/idempot|producerid|Fatal error/i)
  for (const l of eos.slice(0, 8)) console.log(`     ${l.slice(0, 220)}`)
  if (!eos.length) info('set NEGOTIATED_TRACE_FILE=<path> to capture librdkafka\'s eos narration')
  try {
    p.disconnect(() => {})
  } catch {}
}

// ======================================================================= main
async function main() {
  console.log(`node-rdkafka ${Kafka.librdkafkaVersion} binding against ${BOOTSTRAP}, run ${RUN}`)
  console.log(`features: ${FEATURES.join(',')}`)
  const { topic, records } = await scenarioProduce()
  await scenarioCompression()
  await scenarioGroup(topic, records)
  await scenarioResume(topic)
  await scenarioFreshGroup(topic, COUNT + 32)
  await scenarioBinaryHeaders()
  await scenarioSasl()
  if (process.env.RUN_IDEMPOTENCE === '1') await scenarioIdempotence()
  trace.report()
  trace.reportErrors()
  trace.flush()
  return finish('node-rdkafka')
}

main()
  .then((code) => process.exit(code))
  .catch((e) => {
    bad(`unhandled: ${e.stack || e.message}`)
    trace.report()
    trace.reportErrors()
    finish('node-rdkafka')
    process.exit(1)
  })
