/**
 * Conflation: wire contract + degrade-loudly (PLAN_CONFLATION.md §3.1, §3.3, §4).
 *
 * Three things are pinned here, and each of them is a defect that ships
 * silently if it is not:
 *
 *   1. `conflation` reaches the wire from BOTH entry points. pop() and
 *      consume() build their query strings in two SEPARATE places
 *      (QueueBuilder.pop's inline params and ConsumerManager#buildParams) —
 *      the standing comment at QueueBuilder.js documents a bug of exactly this
 *      shape, where the parameter was added to one builder and the other kept
 *      working while the option "did nothing". Both directions are asserted,
 *      including the negative: an undeclared conflation must not add the key at
 *      all, because the broker's response bytes are pinned byte-for-byte for
 *      flag-off deployments.
 *
 *   2. Degrade-loudly (§4). No SDK negotiates a version, so a new client against
 *      a pre-1.1.0 broker sends `conflation=true`, the broker ignores the unknown
 *      query parameter, and the consumer quietly drains the whole backlog one
 *      message at a time. The detection is response-driven: a conflating broker
 *      echoes `"conflation":true` on EVERY response, empty ones included (it
 *      answers 200-with-body instead of 204 precisely so the echo has somewhere
 *      to ride), so the first round trip is enough. An old broker's empty pop is
 *      a bodiless 204 => `null` here, which is why null must raise too.
 *
 *   3. Declaration conflict (§3.3). The STORED group policy wins; the consumer
 *      that disagreed keeps working and warns exactly ONCE per (queue, group)
 *      per process. Rejecting instead would take down the correct half of a
 *      rolling deploy (plan Q3), and warning per response would flood.
 *
 * Same style as http-unit/retry429.test.js and kv-unit/kvWire.test.js: a real
 * node:http server playing a canned plan, real fetch, real JSON. No mocking
 * framework, and every assertion is about bytes that actually crossed a socket.
 */

import { describe, it, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert/strict'

import { Queen } from '../../client-v2/index.js'
import { Runner } from '../../client-v2/streams/runtime/Runner.js'
import * as logger from '../../client-v2/utils/logger.js'
import {
  CONFLATION_UNSUPPORTED,
  resetConflationWarnings
} from '../../client-v2/utils/conflation.js'
import { withPlanServer, ok } from '../kv-unit/_planServer.js'

const QUEUE = 'test-conflation'
const GROUP = 'workers'

/** One delivered frame, shaped like a real pop response element. */
function frame(n = 1) {
  return {
    transactionId: `txn-${n}`,
    partitionId: `part-${n}`,
    partition: 'Default',
    payload: { n },
    leaseId: 'lease-1',
    consumerGroup: GROUP
  }
}

/** A 200 pop body; `extra` carries the conflation echo keys under test. */
function popBody(extra = {}, frames = [frame()]) {
  return ok({ messages: frames, partitionsClaimed: frames.length, ...extra })
}

/** A pre-1.1.0 broker's empty pop: bodiless 204 (HttpClient turns it into null). */
const emptyNoContent = { status: 204 }

/** Query parameters of a recorded hit. */
function query(url) {
  const i = url.indexOf('?')
  return new URLSearchParams(i < 0 ? '' : url.slice(i + 1))
}

/** Path (without query) of a recorded hit. */
function path(url) {
  const i = url.indexOf('?')
  return i < 0 ? url : url.slice(0, i)
}

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

// The conflict warning is deliberately once-per-process state, so every test
// starts from a clean registry.
beforeEach(() => { resetConflationWarnings() })
afterEach(() => { resetConflationWarnings(); logger.configure(null) })

// ---------------------------------------------------------------------------
// 1. The option reaches the wire — from pop() AND from consume().
// ---------------------------------------------------------------------------

describe('conflation — wire (pop)', () => {
  it('pop() sends conflation=true when the builder declares it', async () => {
    await withQueen([popBody({ conflation: true })], popBody({ conflation: true }), async (queen, hits) => {
      const messages = await queen.queue(QUEUE).group(GROUP).conflation(true).pop()

      assert.equal(messages.length, 1)
      assert.equal(hits.length, 1)
      assert.equal(path(hits[0].url), `/api/v1/pop/queue/${QUEUE}`)
      assert.equal(query(hits[0].url).get('conflation'), 'true')
      assert.equal(query(hits[0].url).get('consumerGroup'), GROUP)
    })
  })

  it('pop() omits the parameter entirely when conflation is not declared', async () => {
    await withQueen([popBody()], popBody(), async (queen, hits) => {
      await queen.queue(QUEUE).group(GROUP).pop()

      assert.equal(hits.length, 1)
      assert.equal(
        query(hits[0].url).has('conflation'),
        false,
        'a flag-off pop must be byte-identical to today: no conflation key at all'
      )
    })
  })
})

describe('conflation — wire (consume)', () => {
  it('consume() sends conflation=true through ConsumerManager#buildParams', async () => {
    await withQueen([popBody({ conflation: true })], popBody({ conflation: true }), async (queen, hits) => {
      let handled = 0
      await queen.queue(QUEUE)
        .group(GROUP)
        .conflation(true)
        .wait(false)
        .autoAck(false)
        .limit(1)
        .consume(async () => { handled++ })

      assert.equal(handled, 1)
      assert.ok(hits.length >= 1)
      assert.equal(path(hits[0].url), `/api/v1/pop/queue/${QUEUE}`)
      assert.equal(
        query(hits[0].url).get('conflation'),
        'true',
        'consume() builds its params in a DIFFERENT place than pop() — both must carry it'
      )
    })
  })

  it('consume() omits the parameter entirely when conflation is not declared', async () => {
    await withQueen([popBody()], popBody(), async (queen, hits) => {
      await queen.queue(QUEUE)
        .group(GROUP)
        .wait(false)
        .autoAck(false)
        .limit(1)
        .consume(async () => {})

      assert.ok(hits.length >= 1)
      assert.equal(query(hits[0].url).has('conflation'), false)
    })
  })
})

// ---------------------------------------------------------------------------
// 2. Degrade-loudly (§4): an old broker must never be silent.
// ---------------------------------------------------------------------------

describe('conflation — degrade loudly against a pre-1.1.0 broker', () => {
  it('pop() raises when the response carries messages but no conflation echo', async () => {
    await withQueen([popBody()], popBody(), async (queen) => {
      await assert.rejects(
        () => queen.queue(QUEUE).group(GROUP).conflation(true).pop(),
        (err) => {
          assert.equal(err.code, CONFLATION_UNSUPPORTED)
          assert.match(err.message, /requires broker >= 1\.1\.0/)
          return true
        }
      )
    })
  })

  it('pop() raises on the FIRST empty pop, which an old broker answers 204', async () => {
    await withQueen([emptyNoContent], emptyNoContent, async (queen, hits) => {
      await assert.rejects(
        () => queen.queue(QUEUE).group(GROUP).conflation(true).pop(),
        (err) => {
          assert.equal(err.code, CONFLATION_UNSUPPORTED)
          return true
        }
      )
      assert.equal(hits.length, 1, 'the error fires on the first round trip, before any message')
    })
  })

  it('consume() stops the loop and rejects before the handler ever runs', async () => {
    await withQueen([popBody()], popBody(), async (queen, hits) => {
      let handled = 0
      // consume() returns a thenable ConsumeBuilder, not a Promise: await it
      // inside the arrow so assert.rejects sees a real promise.
      await assert.rejects(
        async () => {
          await queen.queue(QUEUE)
            .group(GROUP)
            .conflation(true)
            .wait(false)
            .autoAck(false)
            .consume(async () => { handled++ })
        },
        (err) => {
          assert.equal(err.code, CONFLATION_UNSUPPORTED)
          assert.match(err.message, /requires broker >= 1\.1\.0/)
          return true
        }
      )
      assert.equal(handled, 0, 'not one message may be processed under a silent degrade')
      assert.equal(hits.length, 1, 'the loop stops instead of draining the backlog')
    })
  })

  it('leaves a conflation-less response alone when conflation was never requested', async () => {
    await withQueen([popBody()], popBody(), async (queen) => {
      const messages = await queen.queue(QUEUE).group(GROUP).pop()
      assert.equal(messages.length, 1, 'today\'s behaviour is untouched when the flag is off')
    })
  })
})

// ---------------------------------------------------------------------------
// 3. Declaration conflict (§3.3): the stored policy wins, loudly but once.
// ---------------------------------------------------------------------------

/** Collect every warning the SDK emits while `run` executes. */
async function captureWarnings(run) {
  const warnings = []
  logger.configure({
    info() {},
    warn(entry) { warnings.push(typeof entry === 'string' ? entry : JSON.stringify(entry)) },
    error() {}
  })
  try {
    await run()
  } finally {
    logger.configure(null)
  }
  return warnings
}

describe('conflation — declaration conflict', () => {
  it('warns exactly once per (queue, group) and keeps delivering messages', async () => {
    const conflicted = popBody({ conflationConflict: true })
    await withQueen([conflicted, conflicted, conflicted], conflicted, async (queen, hits) => {
      const warnings = await captureWarnings(async () => {
        for (let i = 0; i < 3; i++) {
          const messages = await queen.queue(QUEUE).group(GROUP).conflation(true).pop()
          assert.equal(messages.length, 1, 'a conflict never costs the consumer its messages')
        }
      })

      assert.equal(hits.length, 3)
      const conflictWarnings = warnings.filter(w => w.includes('Conflation.conflict'))
      assert.equal(
        conflictWarnings.length,
        1,
        `exactly one warning per (queue,group) per process, got: ${JSON.stringify(conflictWarnings)}`
      )
      assert.match(conflictWarnings[0], new RegExp(QUEUE))
      assert.match(conflictWarnings[0], new RegExp(GROUP))
    })
  })

  it('warns again for a different group on the same queue', async () => {
    const conflicted = popBody({ conflationConflict: true })
    await withQueen([conflicted, conflicted], conflicted, async (queen) => {
      const warnings = await captureWarnings(async () => {
        await queen.queue(QUEUE).group(GROUP).conflation(true).pop()
        await queen.queue(QUEUE).group('audit').conflation(true).pop()
      })

      const conflictWarnings = warnings.filter(w => w.includes('Conflation.conflict'))
      assert.equal(conflictWarnings.length, 2, 'the warn-once key is (queue, group), not queue')
    })
  })

  it('a conflict is never a degrade error — the broker plainly understood the flag', async () => {
    const conflicted = popBody({ conflationConflict: true })
    await withQueen([conflicted], conflicted, async (queen) => {
      const messages = await queen.queue(QUEUE).group(GROUP).conflation(true).pop()
      assert.equal(messages.length, 1)
    })
  })
})

// ---------------------------------------------------------------------------
// 4. The streams runtime pops through the same QueueBuilder — so the option has
//    to be threaded there too, or `run({ conflation: true })` is a no-op.
// ---------------------------------------------------------------------------

/** A QueueBuilder-shaped source that records the chain the Runner builds. */
function recordingSource(calls) {
  const qb = {
    batch(v) { calls.push(['batch', v]); return qb },
    wait(v) { calls.push(['wait', v]); return qb },
    timeoutMillis(v) { calls.push(['timeoutMillis', v]); return qb },
    group(v) { calls.push(['group', v]); return qb },
    partitions(v) { calls.push(['partitions', v]); return qb },
    subscriptionMode(v) { calls.push(['subscriptionMode', v]); return qb },
    subscriptionFrom(v) { calls.push(['subscriptionFrom', v]); return qb },
    conflation(v) { calls.push(['conflation', v]); return qb },
    async pop() { calls.push(['pop']); return [] }
  }
  return qb
}

function runnerWith(opts, calls) {
  return new Runner({
    url: 'http://127.0.0.1:1',
    queryId: 'test-conflation-query',
    stream: { source: recordingSource(calls), stages: {}, config_hash: 'hash' },
    ...opts
  })
}

describe('conflation — streams runtime', () => {
  it('Stream.run({ conflation: true }) declares it on the source pop', async () => {
    const calls = []
    await runnerWith({ conflation: true }, calls)._popMessages()
    assert.deepEqual(calls.filter(c => c[0] === 'conflation'), [['conflation', true]])
  })

  it('leaves the source pop untouched when the option is absent', async () => {
    const calls = []
    await runnerWith({}, calls)._popMessages()
    assert.deepEqual(calls.filter(c => c[0] === 'conflation'), [])
  })
})

// ---------------------------------------------------------------------------
// 5. Depth: the new fields must survive the admin call (§2.5/§5.3).
// ---------------------------------------------------------------------------

describe('conflation — queue depth', () => {
  it('getQueueDepth surfaces partitionsPending / conflation / effectivePending', async () => {
    const depth = ok({
      queue: QUEUE,
      group: GROUP,
      pending: 4000000,
      partitionsPending: 12,
      conflation: true,
      effectivePending: 12,
      partitions: [{ partition: 'a', pending: 333333 }]
    })
    await withQueen([depth], depth, async (queen, hits) => {
      const got = await queen.admin.getQueueDepth(QUEUE, GROUP)

      assert.equal(path(hits[0].url), `/api/v1/resources/queues/${QUEUE}/depth`)
      assert.equal(query(hits[0].url).get('group'), GROUP)
      // Log depth vs work depth: 4M positions to retire, 12 handler calls owed.
      assert.equal(got.pending, 4000000)
      assert.equal(got.partitionsPending, 12)
      assert.equal(got.conflation, true)
      assert.equal(got.effectivePending, 12)
    })
  })
})
