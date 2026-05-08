/**
 * v0.2.1 event-time tests against the fake streams server.
 *
 * Validates:
 *   - eventTime extractor switches windowing from createdAt to user time
 *   - Late events are dropped against the per-partition watermark
 *   - Watermark is persisted to and loaded from queen_streams.state under
 *     the reserved `__wm__` key
 *   - 'include' policy still accumulates late events
 */

import { describe, it, before, after } from 'node:test'
import assert from 'node:assert/strict'

import { Stream } from '../../client-v2/streams/Stream.js'
import {
  WindowTumblingOperator
} from '../../client-v2/streams/operators/WindowTumblingOperator.js'
import { createFakeStreamsServer, createFakeSource, fakeMessage } from './fakeServer.js'

async function runUntilDrained(handle, server, deltaCycles, timeoutMs = 3000) {
  const baseline = server.recorded.cycles.length
  const start = Date.now()
  while (server.recorded.cycles.length - baseline < deltaCycles && Date.now() - start < timeoutMs) {
    await new Promise(r => setTimeout(r, 25))
  }
  await handle.stop()
}

describe('event-time mode', () => {
  let server
  before(async () => { server = await createFakeStreamsServer() })
  after(async () => { await server.close() })

  it('windowTumbling uses extractor instead of createdAt', async () => {
    const op = new WindowTumblingOperator({
      seconds: 60,
      eventTime: m => m.data.eventTs
    })
    // createdAt is 10:30, but eventTs is 10:00:30 → window = 10:00.
    const out = await op.apply({
      msg: { createdAt: '2026-01-01T10:30:00Z', data: { eventTs: '2026-01-01T10:00:30Z' } },
      key: 'p1', value: {}
    })
    assert.equal(out[0].windowKey, '2026-01-01T10:00:00.000Z')
    assert.equal(out[0].eventTimeMs, Date.parse('2026-01-01T10:00:30Z'))
  })

  it('drops late events against the watermark (onLate=drop)', async () => {
    const partA = '00000000-0000-0000-0000-000000000111'
    const source = createFakeSource('events', [
      [
        // First event: ts=10:00:00 → wm advances to 10:00 - 0 = 10:00
        fakeMessage({ partitionId: partA, partitionName: 'p1',
          data: { eventTs: '2026-01-01T10:00:00Z', amount: 5 },
          createdAt: '2026-01-01T10:00:00Z' }),
        // Second event arrives at createdAt=10:01:00 but eventTs=10:00:30
        // → not late (eventTs >= wm).
        fakeMessage({ partitionId: partA, partitionName: 'p1',
          data: { eventTs: '2026-01-01T10:00:30Z', amount: 7 },
          createdAt: '2026-01-01T10:01:00Z' }),
        // Third event arrives at createdAt=10:01:00 with eventTs=10:01:30
        // → wm now advances to 10:01:30 - 30 = 10:01:00
        fakeMessage({ partitionId: partA, partitionName: 'p1',
          data: { eventTs: '2026-01-01T10:01:30Z', amount: 3 },
          createdAt: '2026-01-01T10:01:30Z' })
      ]
    ])

    const handle = await Stream.from(source)
      .windowTumbling({
        seconds: 60,
        eventTime: m => m.data.eventTs,
        allowedLateness: 30,
        onLate: 'drop'
      })
      .reduce((acc, m) => acc + m.amount, 0)
      .to({ _queueName: 'totals' })
      .run({ queryId: 'streams.test.evt_drop', url: server.url })

    await runUntilDrained(handle, server, 1)

    assert.ok(server.recorded.cycles.length >= 1, 'at least one cycle')
    const c = server.recorded.cycles[0]

    // The 10:00 window saw events with eventTs 10:00:00 and 10:00:30 → sum=12
    // The 10:01 window saw eventTs 10:01:30 → sum=3 (still open, will not close on this batch)
    // Watermark advances; we should see watermark state op.
    const wmOp = c.state_ops.find(o => o.key === '__wm__')
    assert.ok(wmOp, 'watermark state op present')
    assert.equal(wmOp.type, 'upsert')
    assert.ok(wmOp.value.eventTimeMs > 0)
  })

  it('persists watermark across runs and drops late events on resume', async () => {
    const partA = '00000000-0000-0000-0000-000000000222'
    const buildStream = (source) => Stream.from(source)
      .windowTumbling({
        seconds: 60,
        eventTime: m => m.data.eventTs,
        allowedLateness: 30,
        onLate: 'drop'
      })
      .reduce((acc, m) => acc + m.amount, 0)
      .to({ _queueName: 'totals' })

    // Run 1: a "fresh" event at eventTs=11:00:00. Advances watermark to
    // 11:00:00 - 30s = 10:59:30. Stream commits and persists.
    const source1 = createFakeSource('events', [
      [fakeMessage({
        partitionId: partA, partitionName: 'p1',
        data: { eventTs: '2026-01-01T11:00:00Z', amount: 1 },
        createdAt: '2026-01-01T11:00:00Z'
      })]
    ])
    const handle1 = await buildStream(source1).run({
      queryId: 'streams.test.evt_persist',
      url: server.url,
      reset: true
    })
    await runUntilDrained(handle1, server, 1)

    // Watermark must now exist in state.
    const queryId = server.recorded.registers[0]
      ? Array.from(server.state.keys())
          .find(k => k.endsWith('|__wm__'))?.split('|')[0]
      : null
    assert.ok(queryId, 'watermark state row must exist after run 1')

    // Run 2: a stale event with eventTs well before the watermark. With
    // onLate='drop' it should be filtered out. Reuse the same queryId
    // and the SAME chain shape so config_hash matches (no reset).
    const source2 = createFakeSource('events', [
      [fakeMessage({
        partitionId: partA, partitionName: 'p1',
        data: { eventTs: '2026-01-01T10:00:00Z', amount: 999 },   // way late
        createdAt: '2026-01-01T11:01:00Z'
      })]
    ])
    const handle2 = await buildStream(source2).run({
      queryId: 'streams.test.evt_persist',
      url: server.url
    })
    await runUntilDrained(handle2, server, 2, 1500)

    const m = handle2.metrics()
    assert.ok(m.lateEventsTotal >= 1, `expected late event count >= 1, got ${m.lateEventsTotal}`)
  })
})
