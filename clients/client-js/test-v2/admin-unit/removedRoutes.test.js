/**
 * Admin methods whose route the 2.x broker does not have.
 *
 * clearQueue() sent DELETE /api/v1/queues/:name/clear and moveMessageToDLQ()
 * sent POST /api/v1/messages/:partitionId/:transactionId/dlq. Neither route is
 * registered (server/src/rsm/facade/real/phase2/reads.rs: the messages family
 * is GET, DELETE and POST .../retry; there is no /api/v1/queues/... family), so
 * a 2.x broker answers both with 404 no_such_route, and the caller saw
 * "not found". Both now throw before any request, naming the way that works.
 *
 * Same style as conflation-unit/conflationWire.test.js: a real node:http
 * server records every request, so "before any request" is asserted on the
 * socket, not on a mock.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Queen } from '../../client-v2/index.js'
import { withPlanServer } from '../kv-unit/_planServer.js'

const notFound = { status: 404, body: { code: 'no_such_route', error: 'not found' } }

async function withQueen(run) {
  await withPlanServer([], notFound, async (url, hits) => {
    const queen = new Queen({ url, handleSignals: false })
    try {
      await run(queen, hits)
    } finally {
      await queen.close()
    }
  })
}

describe('Admin — routes the 2.x broker does not have', () => {
  it('moveMessageToDLQ() throws before any request and names the dlq ack', async () => {
    await withQueen(async (queen, hits) => {
      await assert.rejects(
        queen.admin.moveMessageToDLQ('7', 'tx-1'),
        (err) => {
          assert.match(err.message, /no route/)
          assert.match(err.message, /queen\.ack\(message, 'dlq', \{ group \}\)/)
          return true
        }
      )
      assert.equal(hits.length, 0, 'no request was sent')
    })
  })

  it('clearQueue() throws before any request and names the seek to the end', async () => {
    await withQueen(async (queen, hits) => {
      await assert.rejects(
        queen.admin.clearQueue('orders', 'p1'),
        (err) => {
          assert.match(err.message, /no route/)
          assert.match(err.message, /seekConsumerGroup\(group, 'orders', \{ toEnd: true \}\)/)
          return true
        }
      )
      assert.equal(hits.length, 0, 'no request was sent')
    })
  })
})
