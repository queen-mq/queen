/**
 * Unit tests for operator semantics. These exercise the Operator classes
 * directly (no HTTP, no Queen needed) using `node --test`.
 *
 * Run from the streams/ directory:
 *   node --test test/operators.test.js
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { MapOperator } from '../../client-v2/streams/operators/MapOperator.js'
import { FilterOperator } from '../../client-v2/streams/operators/FilterOperator.js'
import { FlatMapOperator } from '../../client-v2/streams/operators/FlatMapOperator.js'
import { KeyByOperator } from '../../client-v2/streams/operators/KeyByOperator.js'
import { WindowTumblingOperator } from '../../client-v2/streams/operators/WindowTumblingOperator.js'
import { WindowSlidingOperator } from '../../client-v2/streams/operators/WindowSlidingOperator.js'
import { WindowSessionOperator } from '../../client-v2/streams/operators/WindowSessionOperator.js'
import { WindowCronOperator } from '../../client-v2/streams/operators/WindowCronOperator.js'
import { ReduceOperator, stateKeyFor } from '../../client-v2/streams/operators/ReduceOperator.js'
import { AggregateOperator } from '../../client-v2/streams/operators/AggregateOperator.js'
import { configHashOf } from '../../client-v2/streams/util/configHash.js'

const env = (msg, key = 'p1') => ({ msg, key, value: msg.data })

describe('MapOperator', () => {
  it('transforms the envelope value', async () => {
    const op = new MapOperator(m => ({ doubled: m.data.x * 2 }))
    const out = await op.apply(env({ data: { x: 3 } }))
    assert.equal(out.length, 1)
    assert.deepEqual(out[0].value, { doubled: 6 })
  })

  it('supports async fns', async () => {
    const op = new MapOperator(async m => ({ x: m.data.x + 1 }))
    const out = await op.apply(env({ data: { x: 4 } }))
    assert.deepEqual(out[0].value, { x: 5 })
  })
})

describe('FilterOperator', () => {
  it('drops envelopes where predicate is false', async () => {
    const op = new FilterOperator(m => m.data.x > 10)
    assert.deepEqual(await op.apply(env({ data: { x: 5 } })), [])
    assert.equal((await op.apply(env({ data: { x: 20 } }))).length, 1)
  })
})

describe('FlatMapOperator', () => {
  it('emits zero or more outputs', async () => {
    const op = new FlatMapOperator(m => Array.from({ length: m.data.n }, (_, i) => i))
    assert.equal((await op.apply(env({ data: { n: 3 } }))).length, 3)
    assert.equal((await op.apply(env({ data: { n: 0 } }))).length, 0)
  })

  it('rejects non-array returns', async () => {
    const op = new FlatMapOperator(_ => 'not an array')
    await assert.rejects(op.apply(env({ data: {} })), /must return an array/)
  })
})

describe('KeyByOperator', () => {
  it('overrides the envelope key', async () => {
    const op = new KeyByOperator(m => m.data.userId)
    const out = await op.apply(env({ data: { userId: 'u-42' } }, 'partition-A'))
    assert.equal(out[0].key, 'u-42')
  })

  it('coerces keys to string', async () => {
    const op = new KeyByOperator(m => m.data.id)
    const out = await op.apply(env({ data: { id: 7 } }))
    assert.equal(out[0].key, '7')
    assert.equal(typeof out[0].key, 'string')
  })
})

describe('WindowTumblingOperator', () => {
  it('annotates envelope with windowStart/windowEnd/windowKey', async () => {
    const op = new WindowTumblingOperator({ seconds: 60 })
    // 2026-01-01T10:01:30Z -> floor to 10:01:00
    const out = await op.apply({ msg: { createdAt: '2026-01-01T10:01:30.000Z', data: {} }, key: 'p1', value: {} })
    const e = out[0]
    assert.equal(e.windowStart, Date.parse('2026-01-01T10:01:00.000Z'))
    assert.equal(e.windowEnd,   Date.parse('2026-01-01T10:02:00.000Z'))
    assert.equal(e.windowKey,   '2026-01-01T10:01:00.000Z')
  })

  it('throws on missing or invalid createdAt', async () => {
    const op = new WindowTumblingOperator({ seconds: 60 })
    await assert.rejects(
      op.apply({ msg: { data: {} }, key: 'p1', value: {} }),
      /could not determine timestamp/
    )
    await assert.rejects(
      op.apply({ msg: { createdAt: 'nope', data: {} }, key: 'p1', value: {} }),
      /could not determine timestamp/
    )
  })
})

describe('ReduceOperator', () => {
  const TAG = 'tumb:60'
  function envelope(key, ts, value) {
    const ws = Math.floor(Date.parse(ts) / 60_000) * 60_000
    return {
      key,
      windowStart: ws,
      windowEnd: ws + 60_000,
      windowKey: new Date(ws).toISOString(),
      operatorTag: TAG,
      value
    }
  }

  it('accumulates within a window and emits closed windows', async () => {
    const op = new ReduceOperator((acc, v) => acc + v, 0)
    const envelopes = [
      envelope('A', '2026-01-01T10:00:01Z', 1),
      envelope('A', '2026-01-01T10:00:30Z', 2),
      envelope('A', '2026-01-01T10:01:05Z', 5)   // crosses boundary -> closes 10:00 window
    ]
    const result = await op.run({
      envelopes,
      loadedState: new Map(),
      operatorTag: TAG,
      streamTimeMs: Date.parse('2026-01-01T10:01:05Z')
    })

    const closedEmits = result.emits
    assert.equal(closedEmits.length, 1)
    assert.equal(closedEmits[0].key, 'A')
    assert.equal(closedEmits[0].value, 3)
    assert.equal(closedEmits[0].windowKey, '2026-01-01T10:00:00.000Z')

    const upserts = result.stateOps.filter(o => o.type === 'upsert')
    const deletes = result.stateOps.filter(o => o.type === 'delete')
    assert.equal(upserts.length, 1)
    assert.equal(deletes.length, 0)
    assert.equal(upserts[0].value.acc, 5)
  })

  it('emits a delete for a seeded window when its end has passed', async () => {
    const op = new ReduceOperator((acc, v) => acc + v, 0)
    const ws00 = Date.parse('2026-01-01T10:00:00Z')
    const we00 = Date.parse('2026-01-01T10:01:00Z')
    const loadedState = new Map([
      [stateKeyFor(TAG, '2026-01-01T10:00:00.000Z', 'A'),
       { acc: 42, windowStart: ws00, windowEnd: we00 }]
    ])
    const envelopes = [envelope('A', '2026-01-01T10:01:05Z', 5)]
    const result = await op.run({
      envelopes,
      loadedState,
      operatorTag: TAG,
      streamTimeMs: Date.parse('2026-01-01T10:01:05Z')
    })

    const closedEmits = result.emits
    assert.equal(closedEmits.length, 1)
    assert.equal(closedEmits[0].value, 42)
    assert.equal(closedEmits[0].windowKey, '2026-01-01T10:00:00.000Z')

    const upserts = result.stateOps.filter(o => o.type === 'upsert')
    const deletes = result.stateOps.filter(o => o.type === 'delete')
    assert.equal(deletes.length, 1, 'seeded closed window emits a delete')
    assert.equal(upserts.length, 1, '10:01 window stays open with upsert acc=5')
    assert.equal(upserts[0].value.acc, 5)
  })

  it('respects loaded state across cycles', async () => {
    const op = new ReduceOperator((acc, v) => acc + v, 0)
    const envelopes = [envelope('A', '2026-01-01T10:00:30Z', 5)]
    const loadedState = new Map([
      [stateKeyFor(TAG, '2026-01-01T10:00:00.000Z', 'A'),
       {
         acc: 10,
         windowStart: Date.parse('2026-01-01T10:00:00Z'),
         windowEnd: Date.parse('2026-01-01T10:01:00Z')
       }]
    ])
    const result = await op.run({
      envelopes,
      loadedState,
      operatorTag: TAG,
      streamTimeMs: Date.parse('2026-01-01T10:00:30Z')
    })
    assert.equal(result.emits.length, 0)
    const upsert = result.stateOps.find(o => o.type === 'upsert')
    assert.equal(upsert.value.acc, 15)
  })

  it('respects gracePeriod — keeps window open during grace', async () => {
    const op = new ReduceOperator((acc, v) => acc + v, 0)
    const ws = Date.parse('2026-01-01T10:00:00Z')
    const we = Date.parse('2026-01-01T10:01:00Z')
    const loadedState = new Map([
      [stateKeyFor(TAG, '2026-01-01T10:00:00.000Z', 'A'),
       { acc: 42, windowStart: ws, windowEnd: we }]
    ])
    // streamTime is 5s after windowEnd; with 30s grace, still open.
    const result = await op.run({
      envelopes: [],
      loadedState,
      operatorTag: TAG,
      gracePeriodMs: 30_000,
      streamTimeMs: Date.parse('2026-01-01T10:01:05Z')
    })
    assert.equal(result.emits.length, 0, 'grace not yet exceeded — window stays open')
    assert.equal(result.stateOps.length, 0, 'no touched state — no upsert')
  })

  it('respects gracePeriod — closes after grace', async () => {
    const op = new ReduceOperator((acc, v) => acc + v, 0)
    const ws = Date.parse('2026-01-01T10:00:00Z')
    const we = Date.parse('2026-01-01T10:01:00Z')
    const loadedState = new Map([
      [stateKeyFor(TAG, '2026-01-01T10:00:00.000Z', 'A'),
       { acc: 42, windowStart: ws, windowEnd: we }]
    ])
    // streamTime is 35s after windowEnd; 30s grace < 35s → closed.
    const result = await op.run({
      envelopes: [],
      loadedState,
      operatorTag: TAG,
      gracePeriodMs: 30_000,
      streamTimeMs: Date.parse('2026-01-01T10:01:35Z')
    })
    assert.equal(result.emits.length, 1, 'grace exceeded — window closed')
    assert.equal(result.emits[0].value, 42)
  })

  it('ignores state rows owned by another operator', async () => {
    const op = new ReduceOperator((acc, v) => acc + v, 0)
    const ws = Date.parse('2026-01-01T10:00:00Z')
    const we = Date.parse('2026-01-01T10:01:00Z')
    const loadedState = new Map([
      [stateKeyFor('slide:60:10', '2026-01-01T10:00:00.000Z', 'A'),
       { acc: 99, windowStart: ws, windowEnd: we }],
      ['__wm__', { eventTimeMs: 1234 }]      // reserved internal key, must skip
    ])
    const result = await op.run({
      envelopes: [],
      loadedState,
      operatorTag: TAG,
      streamTimeMs: Date.parse('2026-01-01T10:05:00Z')
    })
    assert.equal(result.emits.length, 0, 'foreign state was not closed')
    assert.equal(result.stateOps.length, 0)
  })
})

describe('AggregateOperator', () => {
  const TAG = 'tumb:60'
  function envelope(key, ts, m) {
    const ws = Math.floor(Date.parse(ts) / 60_000) * 60_000
    return {
      key,
      windowStart: ws,
      windowEnd: ws + 60_000,
      windowKey: new Date(ws).toISOString(),
      operatorTag: TAG,
      value: m
    }
  }

  it('computes count, sum, avg, min, max', async () => {
    const op = new AggregateOperator({
      count: () => 1,
      sum:   m => m.amount,
      avg:   m => m.amount,
      min:   m => m.amount,
      max:   m => m.amount
    })
    const envs = [
      envelope('A', '2026-01-01T10:00:01Z', { amount: 10 }),
      envelope('A', '2026-01-01T10:00:30Z', { amount: 30 }),
      envelope('A', '2026-01-01T10:00:55Z', { amount: 20 }),
      envelope('A', '2026-01-01T10:01:05Z', { amount: 99 })
    ]
    const r = await op.run({
      envelopes: envs,
      loadedState: new Map(),
      operatorTag: TAG,
      streamTimeMs: Date.parse('2026-01-01T10:01:05Z')
    })
    assert.equal(r.emits.length, 1)
    const v = r.emits[0].value
    assert.equal(v.count, 3)
    assert.equal(v.sum,   60)
    assert.equal(v.avg,   20)
    assert.equal(v.min,   10)
    assert.equal(v.max,   30)
  })
})

describe('WindowSlidingOperator', () => {
  it('emits N envelopes per event for size=60, slide=10', async () => {
    const op = new WindowSlidingOperator({ size: 60, slide: 10 })
    const out = await op.apply({
      msg: { createdAt: '2026-01-01T10:00:35.000Z', data: {} },
      key: 'p1', value: {}
    })
    // size/slide = 6 windows. The event at :35 falls in windows starting at
    // [-:25, -:20, -:15, -:10, -:05, :00] roughly — i.e. all 6.
    assert.equal(out.length, 6)
    for (const annot of out) {
      assert.ok(annot.windowStart <= Date.parse('2026-01-01T10:00:35Z'))
      assert.equal(annot.windowEnd - annot.windowStart, 60_000)
      assert.equal(annot.operatorTag, 'slide:60:10')
    }
    // Distinct windowKeys.
    const keys = new Set(out.map(o => o.windowKey))
    assert.equal(keys.size, 6)
  })

  it('rejects size not a multiple of slide', () => {
    assert.throws(
      () => new WindowSlidingOperator({ size: 60, slide: 7 }),
      /must be an integer multiple/
    )
  })
})

describe('WindowCronOperator', () => {
  it('aligns minute boundaries', async () => {
    const op = new WindowCronOperator({ every: 'minute' })
    const out = await op.apply({
      msg: { createdAt: '2026-01-01T10:00:35.123Z', data: {} },
      key: 'p1', value: {}
    })
    assert.equal(out.length, 1)
    assert.equal(out[0].windowKey, '2026-01-01T10:00:00.000Z')
    assert.equal(out[0].operatorTag, 'cron:minute')
  })

  it('aligns hour boundaries', async () => {
    const op = new WindowCronOperator({ every: 'hour' })
    const out = await op.apply({
      msg: { createdAt: '2026-01-01T10:30:35.000Z', data: {} },
      key: 'p1', value: {}
    })
    assert.equal(out[0].windowKey, '2026-01-01T10:00:00.000Z')
  })

  it('aligns day boundaries to UTC midnight', async () => {
    const op = new WindowCronOperator({ every: 'day' })
    const out = await op.apply({
      msg: { createdAt: '2026-01-01T22:30:35.000Z', data: {} },
      key: 'p1', value: {}
    })
    assert.equal(out[0].windowKey, '2026-01-01T00:00:00.000Z')
  })

  it('aligns week boundaries to Monday 00:00 UTC', async () => {
    const op = new WindowCronOperator({ every: 'week' })
    // 2026-01-07 is a Wednesday; window should start on Mon 2026-01-05.
    const out = await op.apply({
      msg: { createdAt: '2026-01-07T12:00:00.000Z', data: {} },
      key: 'p1', value: {}
    })
    assert.equal(out[0].windowKey, '2026-01-05T00:00:00.000Z')
  })

  it('rejects unknown every:', () => {
    assert.throws(() => new WindowCronOperator({ every: 'fortnight' }), /every must be/)
  })
})

describe('WindowSessionOperator', () => {
  it('extends a session within gap', async () => {
    const op = new WindowSessionOperator({ gap: 30 })
    const reducerFn = (acc, m) => acc + m.x
    const reducerInitial = () => 0

    // Two events 10s apart — same session.
    const t1 = Date.parse('2026-01-01T10:00:00Z')
    const t2 = Date.parse('2026-01-01T10:00:10Z')
    const envelopes = [
      { key: 'A', eventTimeMs: t1, value: { x: 1 } },
      { key: 'A', eventTimeMs: t2, value: { x: 2 } }
    ]
    const result = await op.runSession({
      envelopes,
      loadedState: new Map(),
      reducerFn,
      reducerInitial,
      nowMs: t2
    })
    // No close (gap not yet exceeded), one upsert with acc=3.
    assert.equal(result.emits.length, 0)
    const upsert = result.stateOps.find(o => o.type === 'upsert')
    assert.equal(upsert.value.acc, 3)
    assert.equal(upsert.value.sessionStart, t1)
    assert.equal(upsert.value.lastEventTime, t2)
  })

  it('closes a session when gap is exceeded by next event', async () => {
    const op = new WindowSessionOperator({ gap: 30 })
    const reducerFn = (acc, m) => acc + m.x
    const reducerInitial = () => 0

    const t1 = Date.parse('2026-01-01T10:00:00Z')
    const t2 = Date.parse('2026-01-01T10:01:00Z')   // 60s later — exceeds 30s gap
    const envelopes = [
      { key: 'A', eventTimeMs: t1, value: { x: 1 } },
      { key: 'A', eventTimeMs: t2, value: { x: 5 } }
    ]
    const result = await op.runSession({
      envelopes,
      loadedState: new Map(),
      reducerFn,
      reducerInitial,
      nowMs: t2
    })
    // First session closed (acc=1), new session opens with x=5.
    assert.equal(result.emits.length, 1)
    assert.equal(result.emits[0].value, 1)

    const upsert = result.stateOps.find(o => o.type === 'upsert')
    assert.equal(upsert.value.acc, 5)
    assert.equal(upsert.value.sessionStart, t2)
  })

  it('idle-flushes a seeded session whose gap has expired', async () => {
    const op = new WindowSessionOperator({ gap: 30 })
    const reducerFn = (acc, m) => acc + m.x
    const reducerInitial = () => 0

    const t1 = Date.parse('2026-01-01T10:00:00Z')
    const flushTime = Date.parse('2026-01-01T10:02:00Z')   // way past gap
    const stateKey = op.openSessionStateKey('A')
    const loadedState = new Map([
      [stateKey, { acc: 99, sessionStart: t1, lastEventTime: t1 }]
    ])
    const result = await op.runSession({
      envelopes: [],
      loadedState,
      reducerFn,
      reducerInitial,
      nowMs: flushTime
    })
    assert.equal(result.emits.length, 1)
    assert.equal(result.emits[0].value, 99)
    const del = result.stateOps.find(o => o.type === 'delete')
    assert.equal(del.key, stateKey)
  })
})

describe('configHashOf', () => {
  it('is stable for the same chain shape', () => {
    const ops = [
      new MapOperator(m => m),
      new WindowTumblingOperator({ seconds: 60 }),
      new AggregateOperator({ count: () => 1 })
    ]
    const h1 = configHashOf(ops)
    const h2 = configHashOf([
      new MapOperator(_ => null),                  // different fn, same kind
      new WindowTumblingOperator({ seconds: 60 }),
      new AggregateOperator({ count: () => 1 })
    ])
    assert.equal(h1, h2, 'hash should ignore user functions and depend only on kinds + structural config')
  })

  it('differs when window size changes', () => {
    const a = configHashOf([new WindowTumblingOperator({ seconds: 60 })])
    const b = configHashOf([new WindowTumblingOperator({ seconds: 30 })])
    assert.notEqual(a, b)
  })

  it('differs when chain reorders', () => {
    const a = configHashOf([new MapOperator(_ => null), new FilterOperator(_ => true)])
    const b = configHashOf([new FilterOperator(_ => true), new MapOperator(_ => null)])
    assert.notEqual(a, b)
  })
})
