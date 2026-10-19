// The Kafka facade's record envelope, read back for the message drawer. The
// recognition rule is the facade's own (records.rs `as_envelope`): a payload
// that fails it is a native one and must be shown as itself.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  fieldLine, hexDump, kafkaRecordOf, kafkaRecordPlain, recordTime,
} from '../src/composables/kafkaRecord.js'

const b64 = text => Buffer.from(text, 'utf8').toString('base64')
const b64bytes = bytes => Buffer.from(bytes).toString('base64')

// A loadgen record from stage, as the console received it.
const stage = {
  h: [
    { k: 'lg-run', v: 'c3RhZ2UtMjRoLTAwMg==' },
    { k: 'lg-ts', v: 'MTc5MjQzNjExNjAwMQ==' },
    { k: 'lg-seq', v: 'MjA1NjI4OQ==' },
  ],
  k: 'c2NyYXBlcnJlcy1jMzEyLTA=',
  t: 1792436116001,
  v: b64('":20694,"date":"2026-11-15","status":"pending"'),
}

test('a record written by the facade reads as its key, headers, time and value', () => {
  const record = kafkaRecordOf(stage)
  assert.deepEqual(record.key, { kind: 'text', size: 17, text: 'scraperres-c312-0' })
  assert.deepEqual(record.headers.map(h => [h.name, fieldLine(h.value)]), [
    ['lg-run', 'stage-24h-002'],
    ['lg-ts', '1792436116001'],
    ['lg-seq', '2056289'],
  ])
  assert.equal(record.timestamp, 1792436116001)
  assert.equal(recordTime(record.timestamp), '2026-10-19T18:55:16.001Z')
  // A slice of a JSON document is text, not JSON.
  assert.equal(record.value.kind, 'text')
  assert.equal(record.value.text, '":20694,"date":"2026-11-15","status":"pending"')
})

test('the payload may arrive as its JSON text', () => {
  assert.deepEqual(kafkaRecordOf(JSON.stringify(stage)), kafkaRecordOf(stage))
  assert.equal(kafkaRecordOf('not json'), null)
})

test('a JSON document value is shown as JSON, anything else UTF-8 as text', () => {
  const json = kafkaRecordOf({ k: null, v: b64('{"orderId":7,"lines":[1,2]}') })
  assert.equal(json.value.kind, 'json')
  assert.deepEqual(json.value.json, { orderId: 7, lines: [1, 2] })

  // A bare number or string is text: it reads better as what it is.
  assert.equal(kafkaRecordOf({ k: null, v: b64('42') }).value.kind, 'text')
  assert.equal(kafkaRecordOf({ k: null, v: b64('"hi"') }).value.kind, 'text')
  // Looks like JSON and is not: text.
  assert.equal(kafkaRecordOf({ k: null, v: b64('{oops') }).value.kind, 'text')
  // Empty is text, not null: Kafka tells the two apart.
  assert.deepEqual(kafkaRecordOf({ k: '', v: '' }).value, { kind: 'text', size: 0, text: '' })
})

test('bytes that are not UTF-8 are binary, shown as hex', () => {
  const bytes = [0x08, 0x96, 0x01, 0xff, 0x41]
  const record = kafkaRecordOf({ k: b64bytes([0xc3, 0x28]), v: b64bytes(bytes) })
  assert.equal(record.value.kind, 'binary')
  assert.equal(record.value.size, 5)
  assert.equal(fieldLine(record.key), 'binary, 2 bytes: c3 28')
  // Offset, the hex column padded to a full row of 16, then the printable bytes.
  assert.equal(hexDump(record.value.bytes), `00000000  ${'08 96 01 ff 41'.padEnd(47)}  ....A`)
  assert.equal(hexDump(new Uint8Array(40), 16).split('\n').at(-1), '… 24 more bytes')
})

test('a null key and a null value survive as null: no key, and a tombstone', () => {
  const record = kafkaRecordOf({ k: null, v: null })
  assert.deepEqual(record.key, { kind: 'null', size: 0 })
  assert.deepEqual(record.value, { kind: 'null', size: 0 })
  assert.deepEqual(record.headers, [])
  assert.equal(record.timestamp, null)
  assert.equal(recordTime(record.timestamp), null)
})

test('a header with a null value is a header', () => {
  const record = kafkaRecordOf({ h: [{ k: 'trace', v: null }], k: null, v: '' })
  assert.deepEqual(record.headers, [{ name: 'trace', value: { kind: 'null', size: 0 } }])
})

test('anything else is a native payload and is not decoded', () => {
  const notRecords = [
    null, 42, 'x', [], [stage],
    { orderId: 1 },
    { k: stage.k }, // no v
    { v: stage.v }, // no k
    { ...stage, partition: 3 }, // a key the facade never writes
    { k: 'not base64!', v: stage.v },
    { k: 'YWI', v: stage.v }, // unpadded: the facade always pads
    { k: 7, v: stage.v },
    { ...stage, h: {} },
    { ...stage, h: [{ k: 'a', v: null, x: 1 }] },
    { ...stage, h: [{ k: 1, v: null }] },
    { ...stage, h: [{ k: 'a' }] },
    { ...stage, h: [null] },
    { ...stage, t: 1.5 },
    { ...stage, t: '1792436116001' },
  ]
  for (const payload of notRecords) {
    assert.equal(kafkaRecordOf(payload), null, JSON.stringify(payload))
  }
})

test('the clipboard copy is plain JSON: text as text, JSON as JSON, binary as base64', () => {
  const record = kafkaRecordOf({
    h: [{ k: 'lg-run', v: b64('stage-24h-002') }, { k: 'blob', v: b64bytes([0xff]) }],
    k: b64('order-9'),
    t: 1792436116001,
    v: b64('{"orderId":9}'),
  })
  assert.deepEqual(kafkaRecordPlain(record), {
    key: 'order-9',
    timestamp: 1792436116001,
    time: '2026-10-19T18:55:16.001Z',
    headers: [
      { name: 'lg-run', value: 'stage-24h-002' },
      { name: 'blob', value: { base64: '/w==' } },
    ],
    value: { orderId: 9 },
  })
})
