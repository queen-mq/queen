/**
 * Push per-item statuses.
 *
 * POST /api/v1/push answers 201 with one result per item, in input order, and
 * a refused item fails INSIDE a successful request, so the per-item status is
 * the only signal that a message was not stored. The statuses brokers send:
 *
 *   queued     stored                                            (every broker)
 *   duplicate  its transactionId is already stored               (every broker)
 *   error      refused, no per-item message                      (2.x)
 *   failed     refused, with `error`                             (1.x)
 *   buffered   written to the broker's failover file, replayed   (1.x)
 *
 * The client used to look for `failed` only, so a 2.x refusal (`error`) was
 * neither reported nor thrown: push() resolved as if the message were stored.
 * Found 2026-10-02 against 2.0.0-beta.6, which answers `error` when a
 * partition's planned append exceeds QUEEN_RAFT_ENTRY_MAX_BYTES.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Queen } from '../../client-v2/index.js'
import { withPlanServer } from '../kv-unit/_planServer.js'

const QUEUE = 'test-push-status'

const item = (index, status, extra = {}) => ({
  index,
  message_id: `0199aaaa-0000-7000-8000-00000000000${index}`,
  transaction_id: `tx-${index}`,
  queueName: QUEUE,
  status,
  ...extra
})

// The exact shape 2.0.0-beta.6 answered for an item it refused (no offset,
// no error text).
const created = (results) => ({ status: 201, body: results })

async function withQueen(response, run) {
  await withPlanServer([response], response, async (url, hits) => {
    const queen = new Queen({ url, handleSignals: false, retryAttempts: 1 })
    try {
      await run(queen, hits)
    } finally {
      await queen.close()
    }
  })
}

describe('push — a refused item is reported, whatever the broker calls it', () => {
  it('a 2.x `error` item makes push() reject, carrying the per-item results', async () => {
    await withQueen(created([item(0, 'error')]), async (queen) => {
      await assert.rejects(
        async () => { await queen.queue(QUEUE).push([{ data: { n: 1 } }]) },
        (err) => {
          assert.match(err.message, /rejected by the broker/)
          assert.match(err.message, /"error"/, 'the status the broker answered is named')
          assert.deepEqual(err.results.map(r => r.status), ['error'])
          return true
        }
      )
    })
  })

  it('a 2.x `error` item goes to onError, and the stored ones still go to onSuccess', async () => {
    await withQueen(created([item(0, 'queued', { offset: 0 }), item(1, 'error')]), async (queen) => {
      let failedItems = null
      let failure = null
      let succeeded = null
      const res = await queen.queue(QUEUE)
        .push([{ data: { n: 1 } }, { data: { n: 2 } }])
        .onError(async (items, err) => { failedItems = items; failure = err })
        .onSuccess(async (items) => { succeeded = items })

      assert.ok(Array.isArray(res), 'with onError the call resolves with the broker results')
      assert.equal(failedItems.length, 1)
      assert.deepEqual(failedItems[0].payload, { n: 2 })
      assert.equal(failedItems[0].result.status, 'error')
      assert.match(failure.message, /rejected by the broker/)
      assert.equal(succeeded.length, 1)
      assert.deepEqual(succeeded[0].payload, { n: 1 })
    })
  })

  it('a 1.x `failed` item is reported with the broker\'s own text', async () => {
    await withQueen(created([item(0, 'failed', { error: 'File buffer write failed' })]), async (queen) => {
      await assert.rejects(
        async () => { await queen.queue(QUEUE).push([{ data: { n: 1 } }]) },
        (err) => { assert.equal(err.message, 'File buffer write failed'); return true }
      )
    })
  })

  it('a status this client does not know is a failure, not a silent success', async () => {
    await withQueen(created([item(0, 'refused-by-something-new')]), async (queen) => {
      await assert.rejects(
        async () => { await queen.queue(QUEUE).push([{ data: { n: 1 } }]) },
        (err) => { assert.match(err.message, /refused-by-something-new/); return true }
      )
    })
  })

  it('several refused items: one error for the call, counting them', async () => {
    await withQueen(created([item(0, 'error'), item(1, 'queued', { offset: 0 }), item(2, 'error')]), async (queen) => {
      await assert.rejects(
        async () => { await queen.queue(QUEUE).push([{ data: 1 }, { data: 2 }, { data: 3 }]) },
        (err) => { assert.match(err.message, /^2 of 3 pushed items rejected/); return true }
      )
    })
  })
})

describe('push — accepted statuses stay accepted', () => {
  it('`queued` resolves with the broker results and reaches onSuccess', async () => {
    await withQueen(created([item(0, 'queued', { offset: 7 })]), async (queen) => {
      let succeeded = null
      const res = await queen.queue(QUEUE).push([{ data: { n: 1 } }]).onSuccess(async (items) => { succeeded = items })
      assert.equal(res[0].status, 'queued')
      assert.equal(succeeded.length, 1)
    })
  })

  it('a 1.x `buffered` item is accepted: the broker owns its delivery', async () => {
    await withQueen(created([item(0, 'buffered')]), async (queen) => {
      let succeeded = null
      const res = await queen.queue(QUEUE).push([{ data: { n: 1 } }]).onSuccess(async (items) => { succeeded = items })
      assert.equal(res[0].status, 'buffered')
      assert.equal(succeeded.length, 1)
    })
  })

  it('`duplicate` goes to onDuplicate and is not a failure', async () => {
    await withQueen(created([item(0, 'duplicate', { offset: 3 })]), async (queen) => {
      let dups = null
      const res = await queen.queue(QUEUE).push([{ data: { n: 1 } }]).onDuplicate(async (items) => { dups = items })
      assert.equal(res[0].status, 'duplicate')
      assert.equal(dups.length, 1)
    })
  })
})
