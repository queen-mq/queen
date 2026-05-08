/**
 * Stream._compile() + configHash sanity tests.
 *
 * These exercise the chain shape validation and ensure config_hash
 * faithfully reflects structural-but-not-functional changes.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Stream } from '../../client-v2/streams/Stream.js'

// Minimal queue-builder stub: anything with a name suffices for compile.
const fakeQueue = (name) => ({ _queueName: name, queueName: name })

describe('Stream chain validation', () => {
  it('compiles a simple stateless chain', () => {
    const s = Stream.from(fakeQueue('orders'))
      .map(m => m.data)
      .filter(v => v.amount > 0)
      .to(fakeQueue('orders.filtered'))
    const c = s._compile()
    assert.equal(c.stages.pre.length, 2)
    assert.equal(c.stages.sink.kind, 'sink')
    assert.equal(c.stages.sink.queueName, 'orders.filtered')
  })

  it('compiles a windowed-aggregation chain', () => {
    const s = Stream.from(fakeQueue('orders'))
      .windowTumbling({ seconds: 60 })
      .aggregate({ sum: m => m.amount })
      .to(fakeQueue('orders.totals'))
    const c = s._compile()
    assert.ok(c.stages.window)
    assert.ok(c.stages.reducer)
    assert.equal(c.stages.reducer.kind, 'aggregate')
  })

  it('allows post-reducer stateless ops', () => {
    const s = Stream.from(fakeQueue('m'))
      .windowTumbling({ seconds: 30 })
      .reduce((a, m) => a + m.value, 0)
      .map(v => ({ v }))
      .filter(v => v.v > 0)
      .to(fakeQueue('out'))
    const c = s._compile()
    assert.equal(c.stages.post.length, 2)
  })

  it('rejects reduce without a window', () => {
    const s = Stream.from(fakeQueue('x')).reduce((a, m) => a + m, 0)
    assert.throws(() => s._compile(), /reduce.*requires a preceding window/i)
  })

  it('rejects double window', () => {
    const s = Stream.from(fakeQueue('x'))
      .windowTumbling({ seconds: 10 })
      .windowTumbling({ seconds: 30 })
    assert.throws(() => s._compile(), /only one window operator/i)
  })

  it('rejects sink not at the end', () => {
    const s = Stream.from(fakeQueue('x'))
      .to(fakeQueue('y'))
      .map(m => m)
    assert.throws(() => s._compile(), /sink operators.*must be the last/i)
  })

  it('produces stable config_hash for identical chain shapes', () => {
    const a = Stream.from(fakeQueue('q'))
      .windowTumbling({ seconds: 60 })
      .aggregate({ sum: m => m.x })
      .to(fakeQueue('out'))
    const b = Stream.from(fakeQueue('q'))
      .windowTumbling({ seconds: 60 })
      .aggregate({ sum: m => m.y })   // different fn, same field name
      .to(fakeQueue('out'))
    assert.equal(a._compile().config_hash, b._compile().config_hash)
  })

  it('config_hash differs when sink queue changes', () => {
    const a = Stream.from(fakeQueue('q'))
      .map(m => m)
      .to(fakeQueue('out-a'))
    const b = Stream.from(fakeQueue('q'))
      .map(m => m)
      .to(fakeQueue('out-b'))
    assert.notEqual(a._compile().config_hash, b._compile().config_hash)
  })

  it('config_hash differs when aggregate field set changes', () => {
    const a = Stream.from(fakeQueue('q'))
      .windowTumbling({ seconds: 60 })
      .aggregate({ sum: () => 0 })
      .to(fakeQueue('out'))
    const b = Stream.from(fakeQueue('q'))
      .windowTumbling({ seconds: 60 })
      .aggregate({ sum: () => 0, count: () => 1 })
      .to(fakeQueue('out'))
    assert.notEqual(a._compile().config_hash, b._compile().config_hash)
  })
})
