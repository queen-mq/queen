/**
 * Pop autopilot, client side.
 *
 * The four things a client can be wrong about here, and why each is asserted
 * against the WHOLE query string rather than against one parameter:
 *
 *  1. BOTH BUILDERS MUST AGREE. pop() and consume() assemble their query
 *     strings separately (QueueBuilder.pop's inline params vs
 *     ConsumerManager#buildParams) — the hazard the standing comment in
 *     QueueBuilder.js already names. Every case below is run through both, and
 *     both are compared to the same expected string, so a rule implemented in
 *     one and not the other cannot pass.
 *
 *  2. NOT ENGAGING AUTOPILOT MUST BE BYTE-IDENTICAL TO THE OLD SDK. The escape
 *     hatch is only worth having if it is exact, and "exact" is not something a
 *     test of one parameter can show: a stray autopilot=true, or a batch that
 *     stopped being emitted, is a different request. Hence full-string equality
 *     including the parameters this feature never touches, and including their
 *     ORDER — this SDK appends rather than sorting, so order is part of the
 *     bytes.
 *
 *  3. AN EXPLICIT VALUE IS SACRED, PER DIMENSION. partitions(1) and "never
 *     called partitions" both used to reach the wire as nothing at all; they are
 *     now different requests, and the pinned one must survive autopilot.
 *
 *  4. THE ADDITIVE RESPONSE FIELD MUST NOT BE LOAD-BEARING. A broker that does
 *     not send it, sends it half-filled, or sends it with fields this SDK has
 *     never heard of, all have to work.
 *
 * Same style as conflation-unit/conflationWire.test.js: a real node:http server
 * playing a canned plan, real fetch, real JSON, and every assertion is about
 * bytes that actually crossed a socket.
 */

import { describe, it, afterEach } from 'node:test'
import assert from 'node:assert/strict'

import { Queen } from '../../client-v2/index.js'
import { Runner } from '../../client-v2/streams/runtime/Runner.js'
import {
  ENV_POP_AUTOPILOT,
  popAutopilotDisabledByEnv,
  popSizing,
  parseAutopilotDecision,
  emptyPollDelayMillis,
  EMPTY_POLL_BACKOFF_MILLIS
} from '../../client-v2/utils/autopilot.js'
import { withPlanServer, ok } from '../kv-unit/_planServer.js'

const QUEUE = 'q'
const GROUP = 'g'

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

/** A 200 pop body; `extra` carries the additive autopilot echo under test. */
function popBody(extra = {}, frames = [frame()]) {
  return ok({ messages: frames, partitionsClaimed: frames.length, ...extra })
}

/** Query string of a recorded hit, exactly as it crossed the socket. */
function rawQuery(url) {
  const i = url.indexOf('?')
  return i < 0 ? '' : url.slice(i + 1)
}

/** Only the pop hits — consume also acks, and the ack is not under test here. */
function popHits(hits) {
  return hits.filter(h => h.url.startsWith('/api/v1/pop'))
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

afterEach(() => { delete process.env[ENV_POP_AUTOPILOT] })

// ---------------------------------------------------------------------------
// 1. Param assembly, from both builders.
// ---------------------------------------------------------------------------

// The shared spine of every case: a named queue and group, no long poll,
// default timeout. Everything that varies below is sizing.
const GROUP_AND_TAIL = `wait=false&timeout=30000&consumerGroup=${GROUP}`

const CASES = [
  {
    // (a) nothing set: both knobs go to the broker, neither travels.
    name: 'nothing set',
    build: qb => qb,
    want: `autopilot=true&${GROUP_AND_TAIL}`
  },
  {
    // (b) partitions pinned, batch left to the broker.
    name: 'partitions only',
    build: qb => qb.partitions(4),
    want: `autopilot=true&${GROUP_AND_TAIL}&partitions=4`
  },
  {
    // (b') the pin that used to be indistinguishable from unset. partitions(1)
    // is a decision — hold this consumer to one partition — and the broker has
    // to be told, or autopilot would widen it.
    name: 'partitions pinned to one',
    build: qb => qb.partitions(1),
    want: `autopilot=true&${GROUP_AND_TAIL}&partitions=1`
  },
  {
    // (c) batch pinned, sweep width left to the broker.
    name: 'batch only',
    build: qb => qb.batch(50),
    want: `autopilot=true&batch=50&${GROUP_AND_TAIL}`
  },
  {
    // (d) both set: nothing left to decide, so no autopilot parameter and the
    // exact request the pre-autopilot SDK sent.
    name: 'both set',
    build: qb => qb.batch(50).partitions(4),
    want: `batch=50&${GROUP_AND_TAIL}&partitions=4`
  },
  {
    // (d') both set with partitions at 1: still byte-identical to the old SDK,
    // which never emitted partitions=1.
    name: 'both set, partitions one',
    build: qb => qb.batch(50).partitions(1),
    want: `batch=50&${GROUP_AND_TAIL}`
  },
  {
    // (e) escape hatch, nothing set: the client-side defaults are back.
    name: 'autopilot off, nothing set',
    build: qb => qb.autopilot(false),
    want: `batch=1&${GROUP_AND_TAIL}`
  },
  {
    // (e') escape hatch with a pin: partitions=1 stays off the wire, exactly as
    // before autopilot existed.
    name: 'autopilot off, partitions pinned to one',
    build: qb => qb.autopilot(false).partitions(1),
    want: `batch=1&${GROUP_AND_TAIL}`
  },
  {
    name: 'autopilot off, both set',
    build: qb => qb.autopilot(false).batch(50).partitions(4),
    want: `batch=50&${GROUP_AND_TAIL}&partitions=4`
  },
  {
    // autopilot(true) is the default, spelled out. It must not change anything,
    // including for a caller who set both knobs.
    name: 'autopilot explicitly on, both set',
    build: qb => qb.autopilot(true).batch(50).partitions(4),
    want: `batch=50&${GROUP_AND_TAIL}&partitions=4`
  },
  {
    // batch(0) is not "a batch of zero" and never was: it is the absence of an
    // opinion, which now means the broker decides.
    name: 'batch zero is unset',
    build: qb => qb.batch(0),
    want: `autopilot=true&${GROUP_AND_TAIL}`
  }
]

describe('pop autopilot — param assembly (pop)', () => {
  for (const tc of CASES) {
    it(tc.name, async () => {
      await withQueen([popBody()], popBody(), async (queen, hits) => {
        await tc.build(queen.queue(QUEUE).group(GROUP).wait(false)).pop()

        assert.equal(popHits(hits).length, 1)
        assert.equal(rawQuery(popHits(hits)[0].url), tc.want)
      })
    })
  }
})

describe('pop autopilot — param assembly (consume)', () => {
  for (const tc of CASES) {
    it(tc.name, async () => {
      await withQueen([popBody()], popBody(), async (queen, hits) => {
        let handled = 0
        await tc.build(queen.queue(QUEUE).group(GROUP).wait(false))
          .limit(1)
          .consume(() => { handled++ })

        assert.equal(handled, 1)
        const pops = popHits(hits)
        assert.ok(pops.length >= 1, 'consume made no pop request')
        assert.equal(rawQuery(pops[0].url), tc.want)
      })
    })
  }
})

// ---------------------------------------------------------------------------
// 2. The process-wide rollback.
// ---------------------------------------------------------------------------

describe('pop autopilot — QUEEN_SDK_POP_AUTOPILOT', () => {
  it('a client built while the variable is set sends the pre-autopilot request', async () => {
    process.env[ENV_POP_AUTOPILOT] = 'off'
    await withQueen([popBody()], popBody(), async (queen, hits) => {
      await queen.queue(QUEUE).group(GROUP).wait(false).pop()

      assert.equal(rawQuery(popHits(hits)[0].url), `batch=1&${GROUP_AND_TAIL}`)
    })
  })

  it('is read ONCE, at construction — changing it later does not move a live client', async () => {
    await withQueen([popBody()], popBody(), async (queen, hits) => {
      process.env[ENV_POP_AUTOPILOT] = 'off'
      await queen.queue(QUEUE).group(GROUP).wait(false).pop()

      assert.equal(rawQuery(popHits(hits)[0].url), `autopilot=true&${GROUP_AND_TAIL}`)
    })
  })

  it('an explicit .autopilot(true) outranks the environment', async () => {
    process.env[ENV_POP_AUTOPILOT] = 'off'
    await withQueen([popBody()], popBody(), async (queen, hits) => {
      await queen.queue(QUEUE).group(GROUP).wait(false).autopilot(true).pop()

      assert.equal(rawQuery(popHits(hits)[0].url), `autopilot=true&${GROUP_AND_TAIL}`)
    })
  })

  it('accepts the whole vocabulary, and nothing else', () => {
    for (const v of ['off', 'OFF', ' off ', 'false', '0', 'no', 'disabled']) {
      process.env[ENV_POP_AUTOPILOT] = v
      assert.equal(popAutopilotDisabledByEnv(), true, `${v} should disable autopilot`)
    }
    for (const v of ['', 'on', 'true', '1', 'yes', 'nonsense']) {
      process.env[ENV_POP_AUTOPILOT] = v
      assert.equal(popAutopilotDisabledByEnv(), false, `${v} should leave autopilot on`)
    }
    delete process.env[ENV_POP_AUTOPILOT]
    assert.equal(popAutopilotDisabledByEnv(), false, 'unset leaves autopilot on')
  })
})

// ---------------------------------------------------------------------------
// 3. The additive response field.
// ---------------------------------------------------------------------------

describe('pop autopilot — the echo', () => {
  it('parses what the broker chose and ignores what it did not', () => {
    assert.equal(parseAutopilotDecision(null), null)
    assert.equal(parseAutopilotDecision({ messages: [] }), null, 'absent')
    assert.equal(parseAutopilotDecision({ autopilot: null }), null, 'null')
    assert.equal(parseAutopilotDecision({ autopilot: true }), null, 'not an object')
    assert.equal(parseAutopilotDecision({ autopilot: [] }), null, 'an array is not an object')

    assert.deepEqual(
      parseAutopilotDecision({ autopilot: { partitions: 8, batch: 200, waitMs: 25 } }),
      { partitions: 8, batch: 200, waitMillis: 25 }
    )
    // waitMs is optional: the broker sends it only when it has an opinion.
    assert.deepEqual(
      parseAutopilotDecision({ autopilot: { partitions: 4, batch: 64 } }),
      { partitions: 4, batch: 64, waitMillis: 0 }
    )
    // Forward compatibility: a newer broker growing a field must not cost this
    // client the fields it does understand.
    assert.deepEqual(
      parseAutopilotDecision({
        autopilot: { partitions: 2, batch: 10, waitMs: 5, reason: 'ready_age', confidence: 0.9 }
      }),
      { partitions: 2, batch: 10, waitMillis: 5 }
    )
    // A field of the wrong type is dropped, not fatal.
    assert.deepEqual(
      parseAutopilotDecision({ autopilot: { partitions: 'eight', batch: 10 } }),
      { partitions: 0, batch: 10, waitMillis: 0 }
    )
  })

  it('popResult() hands the caller what the broker chose', async () => {
    const body = popBody({ autopilot: { partitions: 8, batch: 200, waitMs: 25 } })
    await withQueen([body], body, async (queen) => {
      const res = await queen.queue(QUEUE).group(GROUP).wait(false).popResult()

      assert.equal(res.messages.length, 1)
      assert.deepEqual(res.autopilot, { partitions: 8, batch: 200, waitMillis: 25 })
    })
  })

  it('popResult() reports null when the broker said nothing — a 1.1 broker, say', async () => {
    await withQueen([popBody()], popBody(), async (queen) => {
      const res = await queen.queue(QUEUE).group(GROUP).wait(false).popResult()

      assert.equal(res.messages.length, 1)
      assert.equal(res.autopilot, null)
    })
  })

  it('pop() still returns a bare array of messages', async () => {
    const body = popBody({ autopilot: { partitions: 8, batch: 200 } })
    await withQueen([body], body, async (queen) => {
      const messages = await queen.queue(QUEUE).group(GROUP).wait(false).pop()

      assert.ok(Array.isArray(messages))
      assert.equal(messages.length, 1)
    })
  })
})

// ---------------------------------------------------------------------------
// 4. Empty-poll pacing.
// ---------------------------------------------------------------------------

describe('pop autopilot — empty-poll pacing', () => {
  it('honours the broker advice, and falls back to the historical delay', () => {
    assert.equal(emptyPollDelayMillis(null), EMPTY_POLL_BACKOFF_MILLIS)
    assert.equal(emptyPollDelayMillis({ partitions: 1, batch: 1, waitMillis: 0 }), EMPTY_POLL_BACKOFF_MILLIS)
    assert.equal(emptyPollDelayMillis({ partitions: 1, batch: 1, waitMillis: 250 }), 250)
  })
})

// ---------------------------------------------------------------------------
// 5. The rule itself, in isolation.
// ---------------------------------------------------------------------------

describe('pop autopilot — popSizing', () => {
  it('leaves a pinned dimension alone and delegates only the unset one', () => {
    assert.deepEqual(
      popSizing({ batch: null, maxPartitions: null, fallbackBatch: 1, autopilot: true }),
      { autopilot: true, batch: null, partitions: null }
    )
    assert.deepEqual(
      popSizing({ batch: 50, maxPartitions: null, fallbackBatch: 1, autopilot: true }),
      { autopilot: true, batch: '50', partitions: null }
    )
    assert.deepEqual(
      popSizing({ batch: null, maxPartitions: 1, fallbackBatch: 1, autopilot: true }),
      { autopilot: true, batch: null, partitions: '1' }
    )
    // Both set: nothing to decide, so the flag does not travel either.
    assert.deepEqual(
      popSizing({ batch: 50, maxPartitions: 4, fallbackBatch: 1, autopilot: true }),
      { autopilot: false, batch: '50', partitions: '4' }
    )
    // Off: the client-side default comes back and partitions keeps its >1 gate.
    assert.deepEqual(
      popSizing({ batch: null, maxPartitions: null, fallbackBatch: 1, autopilot: false }),
      { autopilot: false, batch: '1', partitions: null }
    )
    assert.deepEqual(
      popSizing({ batch: null, maxPartitions: 1, fallbackBatch: 1, autopilot: false }),
      { autopilot: false, batch: '1', partitions: null }
    )
  })
})

// ---------------------------------------------------------------------------
// 6. The streams runtime pins its width.
// ---------------------------------------------------------------------------

describe('pop autopilot — streams runtime', () => {
  /** A QueueBuilder stand-in that records the chain the runner builds. */
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
      async pop() { return [] }
    }
    return qb
  }

  it('pins maxPartitions even at 1, so autopilot cannot widen a stream cycle', async () => {
    const calls = []
    const runner = Object.create(Runner.prototype)
    Object.assign(runner, {
      stream: { source: recordingSource(calls) },
      batchSize: 100,
      maxWaitMillis: 1000,
      consumerGroup: 'stream-g',
      maxPartitions: 1,
      subscriptionMode: null,
      subscriptionFrom: null,
      conflation: false
    })

    await runner._popMessages()

    assert.deepEqual(
      calls.filter(([k]) => k === 'partitions'),
      [['partitions', 1]],
      'the streams layer always decided its own width; it must keep saying so'
    )
  })
})
