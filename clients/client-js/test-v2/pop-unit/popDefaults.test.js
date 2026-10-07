/**
 * pop() and consume() share one builder but not their defaults.
 *
 *            autoAck                     commitOnDelivery        wait
 *   pop()    never sent                  off; on sends autoAck   true  (POP_DEFAULTS)
 *   consume  true, client-side           refused                 true  (CONSUME_DEFAULTS)
 *
 * autoAck() is consume()'s ack after the handler and never reaches the wire.
 * The broker's at-most-once commit at delivery is the pop's own option,
 * commitOnDelivery(), which sends autoAck=true; consume() always leases, so it
 * refuses that option before any request.
 * POP_DEFAULTS.wait said false while every pop long-polled; the long poll is
 * what callers rely on, so it stays, and POP_DEFAULTS and the guide say so.
 *
 * The broker contract (server/src/handlers/data.rs, PopParams): autoAck=true
 * commits the messages at delivery, with an empty leaseId; an absent autoAck
 * is false. An absent wait is false too, but this SDK always sends wait.
 *
 * Same style as conflation-unit/conflationWire.test.js: a real node:http
 * server playing a canned plan, real fetch, and every assertion is about the
 * bytes that crossed the socket.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { Queen } from '../../client-v2/index.js'
import { withPlanServer, ok } from '../kv-unit/_planServer.js'

const QUEUE = 'q'
const GROUP = 'g'

const frame = (n = 1) => ({
  transactionId: `txn-${n}`,
  partitionId: `part-${n}`,
  partition: 'Default',
  payload: { n },
  leaseId: 'lease-1',
  consumerGroup: GROUP
})

const popBody = (frames = [frame()]) => ok({ messages: frames, partitionsClaimed: frames.length })

// Acks answer with one result per acknowledgment; the plan only needs enough
// of them for the consume() cases below.
const ackBody = ok([{ index: 0, transactionId: 'txn-1', success: true, error: null }])

function query(url) {
  const i = url.indexOf('?')
  return new URLSearchParams(i < 0 ? '' : url.slice(i + 1))
}

const pops = (hits) => hits.filter(h => h.method === 'GET' && h.url.startsWith('/api/v1/pop'))
const acks = (hits) => hits.filter(h => h.method === 'POST' && h.url.startsWith('/api/v1/ack'))
const statusesOf = (hit) => (hit.body.acknowledgments || [hit.body]).map(a => a.status)

async function withQueen(plan, defaultResponse, run) {
  await withPlanServer(plan, defaultResponse, async (url, hits) => {
    const queen = new Queen({ url, handleSignals: false })
    try {
      await run(queen, hits)
    } finally {
      await queen.close()
    }
  })
}

describe('pop() — autoAck', () => {
  // autoAck() is consume()'s ack after the handler and never reaches the
  // wire: a pop comes back leased whatever autoAck() said. The broker's
  // at-most-once autoAck is commitOnDelivery(), below.
  for (const [label, build] of [
    ['autoAck() never called', (q) => q.group(GROUP)],
    ['autoAck(true)', (q) => q.group(GROUP).autoAck(true)],
    ['autoAck(false)', (q) => q.group(GROUP).autoAck(false)],
  ]) {
    it(`sends no autoAck after ${label}`, async () => {
      await withQueen([popBody(), popBody()], popBody(), async (queen, hits) => {
        await build(queen.queue(QUEUE)).pop()
        await build(queen.queue(QUEUE)).popResult()

        for (const hit of pops(hits)) {
          assert.equal(query(hit.url).has('autoAck'), false)
        }
      })
    })
  }
})

describe('pop() — commitOnDelivery', () => {
  // The broker's commit at delivery, on the wire name every 2.x broker reads:
  // the pop carries autoAck=true and nothing else changes.
  it('pop() and popResult() send autoAck=true and nothing else', async () => {
    await withQueen([popBody(), popBody(), popBody()], popBody(), async (queen, hits) => {
      await queen.queue(QUEUE).group(GROUP).pop()
      await queen.queue(QUEUE).group(GROUP).commitOnDelivery().pop()
      await queen.queue(QUEUE).group(GROUP).commitOnDelivery(true).popResult()

      const [plain, ...committed] = pops(hits)
      assert.equal(committed.length, 2)
      for (const hit of committed) {
        const q = query(hit.url)
        assert.equal(q.get('autoAck'), 'true')
        q.delete('autoAck')
        assert.deepEqual(Object.fromEntries(q), Object.fromEntries(query(plain.url)))
        assert.equal(hit.url.split('?')[0], plain.url.split('?')[0])
      }
    })
  })

  it('sends no autoAck after commitOnDelivery(false)', async () => {
    await withQueen([popBody()], popBody(), async (queen, hits) => {
      await queen.queue(QUEUE).group(GROUP).commitOnDelivery(true).commitOnDelivery(false).pop()

      assert.equal(query(pops(hits)[0].url).has('autoAck'), false)
    })
  })

  it('does not depend on autoAck(), which stays consume()\'s', async () => {
    await withQueen([popBody()], popBody(), async (queen, hits) => {
      await queen.queue(QUEUE).group(GROUP).autoAck(false).commitOnDelivery().pop()

      assert.equal(query(pops(hits)[0].url).get('autoAck'), 'true')
    })
  })

  it('with conflation(), sends both and raises the broker\'s 400 instead of returning []', async () => {
    // The broker refuses conflation together with a commit at delivery
    // (server/src/handlers/data.rs, conflation_refusal).
    const refusal = {
      status: 400,
      body: { success: false, error: 'conflation cannot be combined with autoAck', messages: [] }
    }
    await withQueen([refusal], refusal, async (queen, hits) => {
      await assert.rejects(
        () => queen.queue(QUEUE).group(GROUP).conflation().commitOnDelivery().pop(),
        (err) => err.status === 400
      )

      const q = query(pops(hits)[0].url)
      assert.equal(q.get('autoAck'), 'true')
      assert.equal(q.get('conflation'), 'true')
    })
  })
})

describe('consume() refuses commitOnDelivery()', () => {
  it('throws before any request', async () => {
    await withQueen([], popBody(), async (queen, hits) => {
      assert.throws(
        () => queen.queue(QUEUE).group(GROUP).commitOnDelivery().consume(async () => {}),
        {
          message: 'commitOnDelivery() is a pop() option; consume() always leases its messages'
        }
      )
      assert.equal(hits.length, 0)
    })
  })

  it('consumes as before after commitOnDelivery(false)', async () => {
    await withQueen([popBody(), ackBody], ackBody, async (queen, hits) => {
      await queen.queue(QUEUE).group(GROUP).commitOnDelivery(false).limit(1).consume(async () => {})

      assert.equal(query(pops(hits)[0].url).has('autoAck'), false)
      assert.equal(acks(hits).length, 1)
    })
  })
})

describe('pop() — wait', () => {
  it('long-polls when wait() was never called', async () => {
    await withQueen([popBody()], popBody(), async (queen, hits) => {
      await queen.queue(QUEUE).group(GROUP).pop()

      assert.equal(query(pops(hits)[0].url).get('wait'), 'true')
    })
  })

  it('long-polls for timeoutMillis() when only that was called', async () => {
    await withQueen([popBody()], popBody(), async (queen, hits) => {
      await queen.queue(QUEUE).timeoutMillis(2000).pop()

      const q = query(pops(hits)[0].url)
      assert.equal(q.get('wait'), 'true')
      assert.equal(q.get('timeout'), '2000')
    })
  })

  it('sends wait=false after wait(false)', async () => {
    await withQueen([popBody()], popBody(), async (queen, hits) => {
      await queen.queue(QUEUE).wait(false).pop()

      assert.equal(query(pops(hits)[0].url).get('wait'), 'false')
    })
  })
})

describe('consume() keeps its own defaults', () => {
  it('long-polls and never sends autoAck, also after autoAck(true): it acks after the handler', async () => {
    await withQueen([popBody(), ackBody], ackBody, async (queen, hits) => {
      await queen.queue(QUEUE).group(GROUP).autoAck(true).limit(1)
        .consume(async () => {})

      const q = query(pops(hits)[0].url)
      assert.equal(q.get('wait'), 'true')
      assert.equal(q.has('autoAck'), false)
      assert.equal(acks(hits).length, 1, 'the consumer acked after the handler returned')
      assert.deepEqual(statusesOf(acks(hits)[0]), ['completed'])
    })
  })

  it('acks after the handler when autoAck() was never called', async () => {
    await withQueen([popBody(), ackBody], ackBody, async (queen, hits) => {
      await queen.queue(QUEUE).group(GROUP).limit(1).consume(async () => {})

      assert.equal(query(pops(hits)[0].url).get('wait'), 'true')
      assert.equal(acks(hits).length, 1)
    })
  })

  it('sends no ack after autoAck(false)', async () => {
    await withQueen([popBody()], popBody(), async (queen, hits) => {
      await queen.queue(QUEUE).group(GROUP).autoAck(false).limit(1).consume(async () => {})

      assert.equal(acks(hits).length, 0)
    })
  })

  it('sends wait=false after wait(false)', async () => {
    await withQueen([popBody(), ackBody], ackBody, async (queen, hits) => {
      await queen.queue(QUEUE).group(GROUP).wait(false).limit(1).consume(async () => {})

      assert.equal(query(pops(hits)[0].url).get('wait'), 'false')
    })
  })
})
