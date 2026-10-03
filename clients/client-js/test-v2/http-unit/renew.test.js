/**
 * Queen.renew() outcome.
 *
 * POST /api/v1/lease/:leaseId/extend answers HTTP 200 whether or not anything
 * was renewed. `success` in the body is the only signal, and the client used
 * to ignore it: renewing a lease that had expired, or that an ack had already
 * released, reported `success: true` with `newExpiresAt: null` (found
 * 2026-10-02 against 2.0.0-beta.6). The bodies below are what brokers send:
 *
 *   1.x and 2.x: {leaseId, success, renewed, newExpiresAt, expiresAt, lease_expires_at}
 *   C++:         [{index, leaseId, success, error, expiresAt}]
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Queen } from '../../client-v2/index.js'
import { withPlanServer } from '../kv-unit/_planServer.js'

const LEASE = '01a0fcf2-1a28-7001-8810-d74deda1fd42'
const EXPIRES = '2026-10-02T14:09:40.273Z'

// What 2.0.0-beta.6 answered for a live lease covering two partitions, and
// for a lease that no longer exists.
const renewed = (leaseId = LEASE) => ({
  leaseId, success: true, renewed: 2, newExpiresAt: EXPIRES, expiresAt: EXPIRES, lease_expires_at: EXPIRES
})
const notRenewed = (leaseId = LEASE) => ({
  leaseId, success: false, renewed: 0, newExpiresAt: null, expiresAt: null, lease_expires_at: null
})

async function withQueen(plan, run) {
  await withPlanServer(plan, { status: 200, body: renewed() }, async (url, hits) => {
    const queen = new Queen({ url, handleSignals: false, retryAttempts: 1 })
    try {
      await run(queen, hits)
    } finally {
      await queen.close()
    }
  })
}

describe('Queen.renew — the broker\'s verdict, not the HTTP status', () => {
  it('a renewed lease reports success, its new expiry, and how many partitions it covers', async () => {
    await withQueen([{ body: renewed() }], async (queen, hits) => {
      const res = await queen.renew(LEASE)
      assert.deepEqual(res, { leaseId: LEASE, success: true, newExpiresAt: EXPIRES, renewed: 2 })
      assert.equal(hits[0].url, `/api/v1/lease/${LEASE}/extend`)
    })
  })

  it('a lease that is gone reports failure, with a reason', async () => {
    await withQueen([{ body: notRenewed() }], async (queen) => {
      const res = await queen.renew({ leaseId: LEASE, transactionId: 'tx-1' })
      assert.equal(res.success, false)
      assert.equal(res.renewed, 0)
      assert.equal(res.newExpiresAt, null)
      assert.match(res.error, /not renewed/)
    })
  })

  it('an array of messages answers one outcome per distinct lease', async () => {
    const other = '01a0fcf2-0000-7000-8000-000000000000'
    await withQueen([{ body: renewed() }, { body: notRenewed(other) }], async (queen, hits) => {
      const res = await queen.renew([
        { leaseId: LEASE, transactionId: 'tx-1' },
        { leaseId: LEASE, transactionId: 'tx-2' },
        { leaseId: other, transactionId: 'tx-3' }
      ])
      assert.equal(hits.length, 2, 'one call per lease, not per message')
      assert.deepEqual(res.map(r => [r.leaseId, r.success]), [[LEASE, true], [other, false]])
    })
  })

  it('reads the C++ broker\'s array, including its error text', async () => {
    await withQueen([
      { body: [{ index: 0, leaseId: LEASE, success: true, error: null, expiresAt: EXPIRES }] },
      { body: [{ index: 0, leaseId: LEASE, success: false, error: 'Lease already expired', expiresAt: null }] }
    ], async (queen) => {
      const ok = await queen.renew(LEASE)
      assert.equal(ok.success, true)
      assert.equal(ok.newExpiresAt, EXPIRES)
      const gone = await queen.renew(LEASE)
      assert.equal(gone.success, false)
      assert.equal(gone.error, 'Lease already expired')
    })
  })

  it('an HTTP failure is still a failed renewal, not a throw', async () => {
    await withQueen([{ status: 500, body: { error: 'boom' } }], async (queen) => {
      const res = await queen.renew(LEASE)
      assert.equal(res.success, false)
      assert.ok(res.error)
    })
  })
})
