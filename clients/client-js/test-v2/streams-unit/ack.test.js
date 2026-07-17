/**
 * Queen.ack() wire-format tests.
 *
 * The server (routes/ack.cpp + queen.ack_messages_v2) responds to both
 * /api/v1/ack and /api/v1/ack/batch with a top-level JSON array, one item per
 * acknowledgment in request order:
 *
 *   [{"index":0,"transactionId":"...","success":false,"error":"Invalid or expired lease",
 *     "queueName":"q","partitionName":"Default","leaseReleased":false,"dlq":false}]
 *
 * These tests pin that wire format so a rejected ack/nack (e.g. "Invalid or
 * expired lease", "Message not found") is never reported as success: true.
 * Mirror of clients/client-go/ack_test.go.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'

import { Queen } from '../../client-v2/index.js'

function ackResultItem(index, transactionId, success, error = null) {
  return {
    index,
    transactionId,
    success,
    error,
    queueName: 'test-queue',
    partitionName: 'Default',
    leaseReleased: success,
    dlq: false
  }
}

/**
 * Spin up a real HTTP server, point a Queen client at it, run the test body,
 * then tear both down. respond(body) returns the JSON payload to send back.
 * Requests are recorded as { path, body } for assertions.
 */
async function withAckServer(respond, run) {
  const requests = []
  const server = createServer((req, res) => {
    let raw = ''
    req.on('data', chunk => { raw += chunk })
    req.on('end', () => {
      const body = raw ? JSON.parse(raw) : {}
      requests.push({ path: req.url, body })
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify(respond(body)))
    })
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  const { port } = server.address()

  const client = new Queen({
    url: `http://127.0.0.1:${port}`,
    handleSignals: false,
    retryAttempts: 1
  })

  try {
    await run(client, requests)
  } finally {
    await client.close()
    await new Promise(resolve => server.close(resolve))
  }
}

const MSG = { transactionId: 'tx-1', partitionId: 'p-1', leaseId: 'lease-1' }

describe('Queen.ack single (/api/v1/ack)', () => {
  it('reports success for an accepted ack', async () => {
    await withAckServer(
      () => [ackResultItem(0, 'tx-1', true)],
      async (client, requests) => {
        const result = await client.ack(MSG, true)
        assert.equal(result.success, true)
        assert.equal(result.error, null)
        assert.equal(result.transactionId, 'tx-1')
        assert.equal(requests[0].path, '/api/v1/ack')
        assert.equal(requests[0].body.status, 'completed')
      }
    )
  })

  it('reports failure when the ack is rejected for an expired lease', async () => {
    await withAckServer(
      () => [ackResultItem(0, 'tx-1', false, 'Invalid or expired lease')],
      async (client) => {
        const result = await client.ack(MSG, true)
        assert.equal(result.success, false)
        assert.equal(result.error, 'Invalid or expired lease')
      }
    )
  })

  it('reports failure when a nack is rejected for a missing message', async () => {
    await withAckServer(
      () => [ackResultItem(0, 'tx-1', false, 'Message not found')],
      async (client, requests) => {
        const result = await client.ack(MSG, false)
        assert.equal(result.success, false)
        assert.equal(result.error, 'Message not found')
        assert.equal(requests[0].body.status, 'failed')
      }
    )
  })

  it('still fails when rejected without an error string', async () => {
    await withAckServer(
      () => [ackResultItem(0, 'tx-1', false, null)],
      async (client) => {
        const result = await client.ack(MSG, true)
        assert.equal(result.success, false)
        assert.equal(result.error, 'Acknowledgment rejected by server')
      }
    )
  })

  it('propagates a top-level error envelope', async () => {
    await withAckServer(
      () => ({ error: 'internal error' }),
      async (client) => {
        const result = await client.ack(MSG, true)
        assert.equal(result.success, false)
        assert.equal(result.error, 'internal error')
      }
    )
  })

  it('fails on a response that is neither a result array nor an error envelope', async () => {
    await withAckServer(
      () => ({ ok: true }),
      async (client) => {
        const result = await client.ack(MSG, true)
        assert.equal(result.success, false)
        assert.match(result.error, /Unexpected ack response format/)
      }
    )
  })
})

describe('Queen.ack batch (/api/v1/ack/batch)', () => {
  const MSGS = [
    { transactionId: 'tx-1', partitionId: 'p-1', leaseId: 'lease-1' },
    { transactionId: 'tx-2', partitionId: 'p-1', leaseId: 'lease-1' }
  ]

  it('reports success when every acknowledgment is accepted', async () => {
    await withAckServer(
      () => [ackResultItem(0, 'tx-1', true), ackResultItem(1, 'tx-2', true)],
      async (client, requests) => {
        const result = await client.ack(MSGS, true)
        assert.equal(result.success, true)
        assert.equal(result.processed, 2)
        assert.equal(result.results.length, 2)
        assert.equal(requests[0].path, '/api/v1/ack/batch')
        assert.equal(requests[0].body.acknowledgments.length, 2)
      }
    )
  })

  it('reports failure with per-item results on mixed outcomes', async () => {
    await withAckServer(
      () => [
        ackResultItem(0, 'tx-1', true),
        ackResultItem(1, 'tx-2', false, 'Invalid or expired lease')
      ],
      async (client) => {
        const result = await client.ack(MSGS, true)
        assert.equal(result.success, false)
        assert.equal(result.error, 'Invalid or expired lease')
        assert.equal(result.results[0].success, true)
        assert.equal(result.results[1].success, false)
        assert.equal(result.results[1].error, 'Invalid or expired lease')
      }
    )
  })

  it('fails when the server returns fewer results than acknowledgments sent', async () => {
    await withAckServer(
      () => [ackResultItem(0, 'tx-1', true)],
      async (client) => {
        const result = await client.ack(MSGS, true)
        assert.equal(result.success, false)
        assert.match(result.error, /1 results, expected 2/)
      }
    )
  })

  it('propagates a top-level error envelope to every acknowledgment', async () => {
    await withAckServer(
      () => ({ error: 'internal error' }),
      async (client) => {
        const result = await client.ack(MSGS, true)
        assert.equal(result.success, false)
        assert.equal(result.error, '2 of 2 acknowledgments rejected: internal error')
        assert.equal(result.results.length, 2)
        assert.equal(result.results[0].error, 'internal error')
      }
    )
  })
})
