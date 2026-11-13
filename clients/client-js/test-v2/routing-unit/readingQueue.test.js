/**
 * Reading queues (PLAN_ROUTING.md §5, the routing contract §4): `config({
 * from, where, partitionBy, start })` on `POST /api/v1/configure`.
 *
 * What is at stake: config() fills every option the caller leaves out with
 * QUEUE_DEFAULTS, and a reading queue refuses the settings its sources decide
 * (400 reading_queue). So for a reading queue those defaults must not go out
 * -- set by the caller they do, and the broker refuses them, which is right --
 * while every other queue's configure stays byte for byte what it was.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Queen, READING_QUEUES_UNSUPPORTED } from '../../client-v2/index.js'
import { QUEUE_DEFAULTS } from '../../client-v2/utils/defaults.js'
import { withPlanServer, ok } from '../kv-unit/_planServer.js'

// A configure's answer; a broker with reading queues adds the queue's route.
const ROUTED = { configured: true, route: { from: [{ queue: 'orders' }], partitionBy: 'same', catchingUp: false }, catchingUp: false }

// What the broker refuses on a reading queue (server/src/rsm/facade/real/phase2.rs READER_KEEPS).
const READER_KEEPS = [
  'retentionEnabled', 'retentionSeconds', 'completedRetentionSeconds', 'maxWaitTimeSeconds',
  'dedupWindowSeconds', 'encryptionEnabled', 'maxSize', 'retentionSinkHold', 'retentionSinkHoldMaxSeconds'
]

async function configured(name, options) {
  let body = null
  await withPlanServer([], ok({ ...ROUTED, queue: name }), async (url, hits) => {
    const queen = new Queen({ url, handleSignals: false })
    try {
      const q = queen.queue(name)
      await (options === undefined ? q.create() : q.config(options).create())
      assert.equal(hits[0].method, 'POST')
      assert.equal(hits[0].url, '/api/v1/configure')
      body = hits[0].body
    } finally {
      await queen.close()
    }
  })
  return body
}

describe('reading queues — config()', () => {
  it('sends from, where, partitionBy and start, and none of the defaults a reading queue refuses', async () => {
    const body = await configured('orders-it', {
      from: 'orders', where: "data.country = 'IT'", partitionBy: 'same', start: 'all', leaseTime: 30, retryLimit: 5
    })

    assert.equal(body.queue, 'orders-it')
    const o = body.options
    assert.equal(o.from, 'orders')
    assert.equal(o.where, "data.country = 'IT'")
    assert.equal(o.partitionBy, 'same')
    assert.equal(o.start, 'all')
    assert.equal(o.leaseTime, 30)
    assert.equal(o.retryLimit, 5)
    for (const key of READER_KEEPS) assert.equal(key in o, false, `${key} is the sources' to decide`)
    // The defaults a reading queue does take still go out.
    assert.equal(o.priority, QUEUE_DEFAULTS.priority)
    assert.equal(o.delayedProcessing, QUEUE_DEFAULTS.delayedProcessing)
    assert.equal(o.windowBuffer, QUEUE_DEFAULTS.windowBuffer)
  })

  it('sends a refused setting the caller set: the broker answers reading_queue, as it should', async () => {
    const o = (await configured('orders-it', { from: 'orders', retentionSeconds: 3600 })).options
    assert.equal(o.retentionSeconds, 3600)
    assert.equal('maxSize' in o, false)
    assert.equal('encryptionEnabled' in o, false)
  })

  it('reads where as .where() does: conditions become the checked list', async () => {
    const o = (await configured('orders-it', { from: 'orders', where: ['data.total', '>', 100] })).options
    assert.deepEqual(o.where, [{ field: 'data.total', op: '>', value: 100 }])

    const many = (await configured('orders-it', {
      from: ['orders', { queue: 'refunds', where: [['headers.type', '=', 'refund.done'], { field: 'data.total', op: '>', value: 0 }] }]
    })).options
    assert.deepEqual(many.from, [
      'orders',
      { queue: 'refunds', where: [{ field: 'headers.type', op: '=', value: 'refund.done' }, { field: 'data.total', op: '>', value: 0 }] }
    ])

    // One source as { queue, where } becomes the list the broker reads from as.
    const one = (await configured('orders-it', { from: { queue: 'orders', where: "data.kind = 'x'" }, partitionBy: 'data.customerId' })).options
    assert.deepEqual(one.from, [{ queue: 'orders', where: "data.kind = 'x'" }])
    assert.equal(one.partitionBy, 'data.customerId')

    await assert.rejects(configured('orders-it', { from: 'orders', where: [['data.kind', 'like', 'x']] }), /unknown op "like"/)
  })

  it('a where (or partitionBy, start) without from changes an existing reading queue: no refused defaults either', async () => {
    for (const change of [{ where: "data.country = 'FR'" }, { partitionBy: 'headers.customer' }, { start: 'now' }]) {
      const o = (await configured('orders-it', change)).options
      for (const key of READER_KEEPS) assert.equal(key in o, false, `${JSON.stringify(change)}: ${key}`)
    }
  })

  it('from: null makes it an ordinary queue again, with every default', async () => {
    const o = (await configured('orders-it', { from: null })).options
    assert.equal(o.from, null)
    assert.deepEqual({ ...o, from: undefined }, { ...QUEUE_DEFAULTS, from: undefined })
  })

  it('an ordinary queue\'s configure is what it always was', async () => {
    assert.deepEqual((await configured('orders', { leaseTime: 60 })).options, { ...QUEUE_DEFAULTS, leaseTime: 60 })
    assert.deepEqual((await configured('orders')).options, QUEUE_DEFAULTS)
  })
})

describe('reading queues — a broker that does not read other queues', () => {
  async function withAnswer(answer, run) {
    await withPlanServer([], ok(answer), async (url) => {
      const queen = new Queen({ url, handleSignals: false })
      try {
        await run(queen)
      } finally {
        await queen.close()
      }
    })
  }

  it('create() with from throws when the answer has no route: it made an ordinary queue nothing fills', async () => {
    await withAnswer({ configured: true, queue: 'orders-it', options: {} }, async (queen) => {
      await assert.rejects(
        async () => { await queen.queue('orders-it').config({ from: 'orders', where: "data.country = 'IT'" }).create() },
        (err) => err.code === READING_QUEUES_UNSUPPORTED && /does not read other queues/.test(err.message)
      )
      // With an onError callback, the error goes there, as an error answer's does.
      let seen = null
      const res = await queen.queue('orders-it').config({ from: 'orders' }).create().onError((err) => { seen = err })
      assert.equal(seen.code, READING_QUEUES_UNSUPPORTED)
      assert.equal(res.success, false)
    })
  })

  it('a configure without from, or with from: null, needs no route', async () => {
    await withAnswer({ configured: true, queue: 'orders', options: {} }, async (queen) => {
      assert.equal((await queen.queue('orders').config({ leaseTime: 60 }).create()).configured, true)
      assert.equal((await queen.queue('orders-it').config({ from: null }).create()).configured, true)
    })
  })
})

describe('reading queues — deleting a source', () => {
  it('delete() asks plainly, delete({ force: true }) adds force=true', async () => {
    await withPlanServer([], ok({ deleted: true }), async (url, hits) => {
      const queen = new Queen({ url, handleSignals: false })
      try {
        await queen.queue('orders').delete()
        await queen.queue('orders').delete({ force: true })
        assert.equal(hits[0].method, 'DELETE')
        assert.equal(hits[0].url, '/api/v1/resources/queues/orders')
        assert.equal(hits[1].url, '/api/v1/resources/queues/orders?force=true')
      } finally {
        await queen.close()
      }
    })
  })
})
