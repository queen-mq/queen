// The settings document: what is read out of a stored value, which line wins,
// what a form may save, and that a save leaves alone what it does not know.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  LINES, QUEUE_FLAGS, QUEUE_LINES, LINE_META,
  fromInput, toInput, readSettings, resolveLines, summarizeQueue, validateLines, withDefaults, withQueue,
} from '../src/composables/settingsDoc.js'
import { THRESHOLDS } from '../src/composables/useSeverity.js'

test('every line that may be moved is a built-in line, and its pair exists', () => {
  for (const meta of LINES) {
    assert.equal(typeof THRESHOLDS[meta.key], 'number', meta.key)
    if (meta.below) assert.ok(LINE_META[meta.below], `${meta.key} → ${meta.below}`)
    if (meta.below) assert.ok(THRESHOLDS[meta.key] < THRESHOLDS[meta.below], meta.key)
  }
  // Per queue: what is judged with a queue in hand. The broker's lines are not.
  assert.deepEqual(QUEUE_LINES.map((m) => m.key), [
    'lagWarnSeconds', 'lagBadSeconds', 'backlogWarnSeconds', 'backlogBadSeconds', 'ackWarnRate', 'ackBadRate',
  ])
  assert.deepEqual(QUEUE_FLAGS.map((f) => f.key), ['noReaderOk'])
})

test('a queue line wins over the tenant line, which wins over the built-in one', () => {
  const s = readSettings({
    defaults: { lagWarnSeconds: 120, ackWarnRate: 0.02 },
    queues: { batch: { lagWarnSeconds: 900, lagBadSeconds: 3600 } },
  })
  const batch = resolveLines(THRESHOLDS, s, 'batch')
  assert.equal(batch.lagWarnSeconds, 900)
  assert.equal(batch.lagBadSeconds, 3600)
  assert.equal(batch.ackWarnRate, 0.02)
  const other = resolveLines(THRESHOLDS, s, 'orders')
  assert.equal(other.lagWarnSeconds, 120)
  assert.equal(other.lagBadSeconds, THRESHOLDS.lagBadSeconds)
  assert.equal(resolveLines(THRESHOLDS, s).lagWarnSeconds, 120)
  assert.equal(other.backlogWarnSeconds, THRESHOLDS.backlogWarnSeconds)
})

test('no document, or one that says nothing, changes nothing', () => {
  for (const raw of [undefined, null, {}, 'x', [], { defaults: null, queues: [] }]) {
    assert.deepEqual(resolveLines(THRESHOLDS, readSettings(raw), 'orders'), { ...THRESHOLDS })
  }
})

test('what the file cannot use is skipped, never guessed at', () => {
  const s = readSettings({
    defaults: { lagWarnSeconds: '120', lagBadSeconds: -5, ackWarnRate: 3, raftLagWarnEntries: 1, memWarnShare: 0.5 },
    queues: {
      a: { lagWarnSeconds: 1.5 },
      b: { lagBadSeconds: 900, memWarnShare: 0.5, noReaderOk: 'yes' },
      c: { noReaderOk: true },
      d: { noReaderOk: false },
      __proto__: { lagWarnSeconds: 7 },
    },
  })
  assert.deepEqual(s.defaults, { memWarnShare: 0.5 })
  // A broker line on a queue, a flag that is not `true`, a queue left with nothing: all skipped.
  assert.deepEqual([...s.queues], [['b', { lagBadSeconds: 900 }], ['c', { noReaderOk: true }]])
  assert.equal(resolveLines(THRESHOLDS, s, 'c').noReaderOk, true)
  assert.equal(resolveLines(THRESHOLDS, s, 'b').noReaderOk, undefined)
  assert.equal(resolveLines(THRESHOLDS, s).noReaderOk, undefined)
  assert.equal(resolveLines(THRESHOLDS, s, 'toString').lagWarnSeconds, THRESHOLDS.lagWarnSeconds)
})

test('a queue may be named like a property of Object', () => {
  const raw = withQueue(JSON.parse('{"queues":{"__proto__":{"lagWarnSeconds":7}}}'), 'constructor', { lagWarnSeconds: 9 })
  const s = readSettings(raw)
  assert.equal(resolveLines(THRESHOLDS, s, '__proto__').lagWarnSeconds, 7)
  assert.equal(resolveLines(THRESHOLDS, s, 'constructor').lagWarnSeconds, 9)
  assert.equal(resolveLines(THRESHOLDS, s, 'hasOwnProperty').lagWarnSeconds, THRESHOLDS.lagWarnSeconds)
})

test('an input is seconds as typed and a share as a percentage', () => {
  const lag = LINE_META.lagWarnSeconds
  const ack = LINE_META.ackWarnRate
  assert.equal(toInput(lag, 600), '600')
  assert.equal(toInput(ack, 0.01), '1')
  assert.equal(toInput(ack, 0.025), '2.5')
  assert.equal(toInput(lag, undefined), '')
  assert.equal(fromInput(lag, ' 600 '), 600)
  assert.equal(fromInput(lag, ''), null)
  assert.ok(Number.isNaN(fromInput(lag, '10m')))
  assert.ok(Number.isNaN(fromInput(lag, '1.5')))
  assert.equal(fromInput(ack, '2.5'), 0.025)
  assert.ok(Number.isNaN(fromInput(ack, '2,5')))
})

test('a form is refused what the reader would skip, and attention after failing', () => {
  assert.deepEqual(validateLines(THRESHOLDS, { lagWarnSeconds: 600, lagBadSeconds: 1800 }), {})
  assert.deepEqual(validateLines(THRESHOLDS, { lagWarnSeconds: null }), {})
  assert.deepEqual(Object.keys(validateLines(THRESHOLDS, { lagWarnSeconds: NaN, ackWarnRate: 2 })), ['lagWarnSeconds', 'ackWarnRate'])
  // Past the built-in second line while that one is left alone.
  assert.match(validateLines(THRESHOLDS, { lagWarnSeconds: 600 }).lagWarnSeconds, /lower than/)
  // Both typed, the wrong way round: said on the lower one.
  assert.deepEqual(Object.keys(validateLines(THRESHOLDS, { lagWarnSeconds: 900, lagBadSeconds: 600 })), ['lagWarnSeconds'])
  // Only the upper one typed, under the line it sits on.
  assert.deepEqual(Object.keys(validateLines(THRESHOLDS, { lagBadSeconds: 30 })), ['lagBadSeconds'])
  // A queue sits on the tenant's lines, and takes the two lag lines only.
  const tenant = { ...THRESHOLDS, lagWarnSeconds: 600, lagBadSeconds: 1800 }
  assert.deepEqual(validateLines(tenant, { lagBadSeconds: 900 }, { perQueue: true }), {})
  assert.match(validateLines(tenant, { lagBadSeconds: 300 }, { perQueue: true }).lagBadSeconds, /lower than/)
  // The broker's lines are not a queue's to set: not read, so not judged either.
  assert.deepEqual(validateLines(tenant, { memWarnShare: 9 }, { perQueue: true }), {})
  assert.deepEqual(Object.keys(validateLines(tenant, { ackWarnRate: 0.2 }, { perQueue: true })), ['ackWarnRate'])
  assert.deepEqual(validateLines(tenant, { ackWarnRate: 0.02, backlogBadSeconds: 7200 }, { perQueue: true }), {})
})

test("a queue's flag is stored as true, and gone when it is not", () => {
  const on = withQueue({ queues: { a: { lagWarnSeconds: 900, noReaderOk: true } } }, 'a', { lagWarnSeconds: 900 })
  assert.deepEqual(on.queues.a, { lagWarnSeconds: 900 })
  const only = withQueue(undefined, 'archive', { noReaderOk: true })
  assert.deepEqual(only, { queues: { archive: { noReaderOk: true } } })
  assert.deepEqual(withQueue(only, 'archive', { noReaderOk: false }), { queues: {} })
})

test("a queue's own settings read as short phrases", () => {
  assert.deepEqual(summarizeQueue({ lagWarnSeconds: 600, lagBadSeconds: 1800 }), ['Consumer lag 10m / 30m'])
  assert.deepEqual(
    summarizeQueue({ backlogWarnSeconds: 90, ackBadRate: 0.1, noReaderOk: true }),
    ['Backlog 1m 30s / —', 'Ack failures — / 10%', 'no reader expected'],
  )
  assert.deepEqual(summarizeQueue(undefined), [])
})

test('a save replaces the lines it knows and keeps everything else', () => {
  const stored = {
    note: 'kept',
    defaults: { lagWarnSeconds: 120, ackWarnRate: 0.02, somethingNewer: true },
    queues: { a: { lagWarnSeconds: 900, owner: 'ops' }, b: { lagBadSeconds: 600 } },
  }
  const d = withDefaults(stored, { lagWarnSeconds: 90 })
  assert.deepEqual(d.defaults, { somethingNewer: true, lagWarnSeconds: 90 })
  assert.equal(d.note, 'kept')
  assert.deepEqual(d.queues, stored.queues)

  const a = withQueue(stored, 'a', { lagBadSeconds: 3600 })
  assert.deepEqual(a.queues.a, { owner: 'ops', lagBadSeconds: 3600 })
  assert.deepEqual(a.queues.b, { lagBadSeconds: 600 })

  // No line left: the queue goes, unless it carries something of another writer's.
  assert.deepEqual(Object.keys(withQueue(stored, 'b', {}).queues), ['a'])
  assert.deepEqual(withQueue(stored, 'a', null).queues.a, { owner: 'ops' })
  // The first save, on a tenant with no document.
  assert.deepEqual(withQueue(undefined, 'q', { lagWarnSeconds: 5 }), { queues: { q: { lagWarnSeconds: 5 } } })
  assert.deepEqual(stored.queues.a, { lagWarnSeconds: 900, owner: 'ops' })
})
