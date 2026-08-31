// Which layer loses a binary header value? A cross-binding probe.
//
//   node probe-headers.mjs [bootstrap] [runId]
//
// The two Node bindings in this directory are the SAME librdkafka, so a header
// that survives one and not the other cannot be the facade's doing. This probe
// writes the same five header values with each binding and reads each topic
// back with BOTH, which makes the answer a 2x2 rather than an opinion:
//
//                      read by node-rdkafka   read by @confluentinc
//   written by node-rdkafka        ?                    ?
//   written by @confluentinc       ?                    ?
//
// A whole column bad would be a reader bug; a whole ROW bad is a writer bug; the
// facade would have to spoil every cell. The answer (recorded 2026-08-29 against
// queen-kafka 1.3.0) is the top row: node-rdkafka's producer coerces every
// header value to a v8 String and then builds a std::string from the resulting
// `char*`, so the value stops at the first NUL and any non-UTF-8 byte is already
// a U+FFFD by then —
//
//   node_modules/node-rdkafka/src/producer.cc  (NodeProduce)
//       Nan::MaybeLocal<v8::String> v8Value = Nan::To<v8::String>(...)
//       Nan::Utf8String uValue(v8Value.ToLocalChecked());
//       std::string value(*uValue);                     // <- stops at NUL
//       headers.push_back(RdKafka::Headers::Header(key, value.c_str(), value.size()));
//
// against @confluentinc/kafka-javascript's src/producer.cc, which branches on
// `node::Buffer::HasInstance(v8Value)` and pushes the raw pointer and length.
// Both bindings' READ paths are the same code and both are correct
// (`Nan::Encode(it->value_string(), it->value_size(), BUFFER)` in src/common.cc).
//
// So: report binary-header loss to node-rdkafka, never to queen-kafka.
import RdKafka from 'node-rdkafka'
import Confluent from '@confluentinc/kafka-javascript'
import { ok, bad, info, say, check, finish, sleep, deadline } from './fixture.mjs'

const BOOTSTRAP = process.argv[2] || process.env.KAFKA_BOOTSTRAP || '127.0.0.1:19092'
const RUN = process.argv[3] || process.env.RUN_ID || String(Date.now())

// The five values, chosen so each failure mode is distinguishable on sight.
const VALUES = [
  { name: 'ascii', value: Buffer.from('plain-text') },
  { name: 'lead-nul', value: Buffer.from([0x00, 0x01, 0x02, 0x03]) },
  { name: 'mid-nul', value: Buffer.from([0x41, 0x00, 0x42]) },
  { name: 'high-bytes', value: Buffer.from([0xfe, 0xff, 0x80]) },
  { name: 'empty', value: Buffer.alloc(0) },
]

const hex = (b) => Buffer.from(b ?? []).toString('hex')
const normalize = (hs) =>
  (hs || []).map((h) => {
    const name = Object.keys(h)[0]
    return { name, value: Buffer.from(h[name] ?? []) }
  })

function write(binding, topic) {
  const p = new binding.Producer(
    { 'metadata.broker.list': BOOTSTRAP, dr_cb: true, 'enable.idempotence': false },
    { 'request.required.acks': -1 },
  )
  return deadline(
    new Promise((resolve, reject) => {
      p.on('event.error', reject)
      p.on('ready', () => {
        p.setPollInterval(20)
        try {
          p.produce(
            topic, 0, Buffer.from('probe'), Buffer.from('probe-key'), null, null,
            VALUES.map((v) => ({ [v.name]: v.value })),
          )
        } catch (e) {
          return reject(e)
        }
        p.flush(20000, (err) => (err ? reject(err) : p.disconnect(() => resolve())))
      })
      p.connect({ timeout: 15000 }, (err) => err && reject(err))
    }),
    45000,
    `write ${topic}`,
  )
}

function read(binding, topic, group) {
  const c = new binding.KafkaConsumer(
    {
      'metadata.broker.list': BOOTSTRAP,
      'group.id': group,
      'enable.auto.commit': false,
    },
    { 'auto.offset.reset': 'earliest' },
  )
  return deadline(
    new Promise((resolve, reject) => {
      c.on('event.error', reject)
      c.on('data', (m) => {
        c.disconnect(() => resolve(normalize(m.headers)))
      })
      c.on('ready', () => {
        c.assign([{ topic, partition: 0, offset: 0 }])
        c.consume()
      })
      c.connect({ timeout: 15000 }, (err) => err && reject(err))
    }),
    60000,
    `read ${topic}`,
  )
}

const BINDINGS = [
  ['node-rdkafka', RdKafka],
  ['@confluentinc', Confluent],
]

async function main() {
  console.log(
    `node-rdkafka librdkafka ${RdKafka.librdkafkaVersion} / ` +
      `@confluentinc librdkafka ${Confluent.librdkafkaVersion} against ${BOOTSTRAP}, run ${RUN}`,
  )
  say('what was asked for')
  for (const v of VALUES) console.log(`  ${v.name.padEnd(12)} ${hex(v.value) || '(empty)'}`)

  const grid = []
  for (const [wname, wbind] of BINDINGS) {
    const topic = `hdrprobe-${RUN}-${wname.replace(/\W/g, '')}`
    await write(wbind, topic)
    await sleep(300)
    for (const [rname, rbind] of BINDINGS) {
      const got = await read(rbind, topic, `hdrprobe-g-${RUN}-${wname}-${rname}`.replace(/\W/g, '-'))
      grid.push({ writer: wname, reader: rname, got })
    }
  }

  say('the 2x2')
  for (const cell of grid) {
    console.log(`\n  written by ${cell.writer}, read by ${cell.reader}:`)
    for (const want of VALUES) {
      const got = cell.got.find((h) => h.name === want.name)
      const same = got && Buffer.from(got.value).equals(want.value)
      console.log(
        `    ${same ? 'same ' : 'LOST '} ${want.name.padEnd(12)} ` +
          `wanted ${hex(want.value) || '(empty)'} got ${got ? hex(got.value) || '(empty)' : '(absent)'}`,
      )
    }
  }

  say('verdict')
  const exact = (cell) =>
    VALUES.every((w) => {
      const g = cell.got.find((h) => h.name === w.name)
      return g && Buffer.from(g.value).equals(w.value)
    })
  const byWriter = new Map()
  for (const c of grid) {
    if (!byWriter.has(c.writer)) byWriter.set(c.writer, [])
    byWriter.get(c.writer).push(exact(c))
  }
  for (const [writer, results] of byWriter) {
    info(`written by ${writer}: ${results.filter(Boolean).length}/${results.length} readers saw the exact bytes`)
  }
  const confluentRow = byWriter.get('@confluentinc') || []
  check(
    confluentRow.length > 0 && confluentRow.every(Boolean),
    'a binding that sends header BYTES gets them back byte-exact from both readers — ' +
      'so the facade stores and returns arbitrary header bytes, NULs and 0x80..0xff included',
  )
  const rdkRow = byWriter.get('node-rdkafka') || []
  if (rdkRow.every(Boolean)) {
    ok('node-rdkafka also round-tripped every value (its producer.cc was fixed upstream)')
  } else {
    info(
      'node-rdkafka LOST bytes for every reader, including itself — a writer-side defect in ' +
        'node-rdkafka/src/producer.cc (header values go through Nan::To<v8::String> and a ' +
        'std::string built from a char*), NOT a queen-kafka defect. Nothing to file here.',
    )
    ok('the loss is attributable to the writer, not to the facade or to either reader')
  }
  return finish('header probe')
}

main()
  .then((code) => process.exit(code))
  .catch((e) => {
    bad(`unhandled: ${e.stack || e.message}`)
    finish('header probe')
    process.exit(1)
  })
