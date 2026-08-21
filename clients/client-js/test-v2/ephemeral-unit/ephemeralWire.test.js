/**
 * Ephemeral wire contract (EPHEMERAL_QUEUES.md §3.1, §4).
 *
 * These assert the EXACT request every verb of `queen.ephemeral` puts on the
 * socket, against a scripted plan server. The family is eight routes with no
 * stored procedure and no schema behind them: a misspelled field is not a
 * compile error anywhere, it is a 400 -- or worse, a knob the broker never
 * reads -- on somebody's first ephemeral queue.
 *
 * deepEqual, not "contains": an EXTRA field is as much a contract break as a
 * missing one. `partition:null` sent because the caller did not name one, a
 * `wait=false` that did not need to be on the query string, a `transactionId`
 * copied from the durable push wire -- all of them would ship silently if these
 * were subset checks.
 *
 * The rules pinned here that live in the CLIENT and nowhere else:
 *   * a message on this wire is `{payload}` and nothing else -- the envelope
 *     carries queue and partition, unlike the durable push, which repeats them
 *     on every item;
 *   * what the caller did not ask for does not reach the query string, so the
 *     broker's own defaults keep owning it (§3.1's `?` on every parameter but
 *     `queue`);
 *   * `wait:true` DOES send an explicit `timeout`, because the HTTP deadline is
 *     set past it and a client that let the two defaults drift apart would
 *     abort requests the broker was about to answer;
 *   * every 404 on the family is one error with `.code ===
 *     EPHEMERAL_UNSUPPORTED` (§4) -- the broker's missing route and the old
 *     proxy's `route_blocked` are the same fact to a caller;
 *   * empty is an empty ARRAY, never null: `{messages}` is destructured on the
 *     first line of every consumer ever written against this API.
 */

import { describe, it } from 'node:test'
import assert from 'node:assert/strict'

import { EPHEMERAL_UNSUPPORTED, EPHEMERAL_QUEUE_NOT_FOUND } from '../../client-v2/index.js'
import { withEphemeral, ok, pushed, popped, frame, acked, OLD_BROKER, OLD_PROXY, QUEUE_NOT_FOUND } from './_planServer.js'

const QUEUE = 'test-ephemeral'

describe('ephemeral wire — declaration', () => {
  it('configure sends the queue and its options under `options`', async () => {
    await withEphemeral([ok({ queue: QUEUE })], async (eph, hits) => {
      await eph.configure(QUEUE, {
        maxBytes: 1048576,
        maxLength: 1000,
        policy: 'dropOldest',
        ttlSeconds: 30,
        leaseSeconds: 15,
        retryLimit: 2,
        windowBuffer: { ms: 20, count: 50 }
      })

      assert.equal(hits.length, 1)
      assert.equal(hits[0].method, 'POST')
      assert.equal(hits[0].url, '/api/v1/ephemeral/configure')
      assert.deepEqual(hits[0].body, {
        queue: QUEUE,
        options: {
          maxBytes: 1048576,
          maxLength: 1000,
          policy: 'dropOldest',
          ttlSeconds: 30,
          leaseSeconds: 15,
          retryLimit: 2,
          windowBuffer: { ms: 20, count: 50 }
        }
      })
    })
  })

  it('configure sends only the options it was given', async () => {
    await withEphemeral([ok({ queue: QUEUE }), ok({ queue: QUEUE })], async (eph, hits) => {
      await eph.configure(QUEUE, { ttlSeconds: 30 })
      assert.deepEqual(hits[0].body, { queue: QUEUE, options: { ttlSeconds: 30 } })

      // A bare declaration: exists, with the tenant defaults.
      await eph.configure(QUEUE)
      assert.deepEqual(hits[1].body, { queue: QUEUE, options: {} })
    })
  })

  it('configure refuses an option this client does not know instead of dropping it', async () => {
    // Every one of these knobs bounds something. A silently ignored `ttlSecond`
    // is a ring that grows until a global budget answers 503.
    await withEphemeral([], async (eph, hits) => {
      await assert.rejects(() => eph.configure(QUEUE, { ttlSecond: 30 }), (e) => {
        assert.match(e.message, /ttlSecond/)
        return true
      })
      assert.equal(hits.length, 0, 'nothing may reach the broker')
    })
  })

  it('reset and delete name the queue where each route expects it', async () => {
    await withEphemeral([ok({ dropped: 7 }), ok({})], async (eph, hits) => {
      const res = await eph.reset(QUEUE)
      assert.equal(hits[0].method, 'POST')
      assert.equal(hits[0].url, '/api/v1/ephemeral/reset')
      assert.deepEqual(hits[0].body, { queue: QUEUE })
      assert.equal(res.dropped, 7)

      await eph.delete(QUEUE)
      assert.equal(hits[1].method, 'DELETE')
      assert.equal(hits[1].url, `/api/v1/ephemeral/queue/${QUEUE}`)
    })
  })

  it('percent-encodes a queue name that would otherwise change the path', async () => {
    await withEphemeral([ok({}), ok({})], async (eph, hits) => {
      await eph.delete('inbox/7')
      assert.equal(hits[0].url, '/api/v1/ephemeral/queue/inbox%2F7')

      await eph.depth('inbox/7')
      assert.equal(hits[1].url, '/api/v1/ephemeral/queues/inbox%2F7/depth')
    })
  })

  it('refuses a missing queue name before spending a request', async () => {
    await withEphemeral([], async (eph, hits) => {
      await assert.rejects(() => eph.configure('', {}))
      await assert.rejects(() => eph.push(undefined, [{ a: 1 }]))
      await assert.rejects(() => eph.pop(null))
      await assert.rejects(() => eph.ack(42, ['e:1']))
      assert.equal(hits.length, 0)
    })
  })
})

describe('ephemeral wire — push', () => {
  it('push sends the flat envelope with payload-only messages', async () => {
    await withEphemeral([pushed(2)], async (eph, hits) => {
      const res = await eph.push(QUEUE, [{ a: 1 }, { a: 2 }])

      assert.equal(hits.length, 1)
      assert.equal(hits[0].method, 'POST')
      assert.equal(hits[0].url, '/api/v1/ephemeral/push')
      assert.deepEqual(hits[0].body, { queue: QUEUE, messages: [{ payload: { a: 1 } }, { payload: { a: 2 } }] })
      assert.equal(res.pushed, 2)
    })
  })

  it('push omits `partition` when the caller named none, and sends it when they did', async () => {
    await withEphemeral([pushed(1), pushed(1)], async (eph, hits) => {
      await eph.push(QUEUE, { a: 1 })
      assert.ok(!('partition' in hits[0].body), 'which partition an unpartitioned push lands on is the broker\'s rule')

      await eph.push(QUEUE, { a: 1 }, { partition: 'room-7' })
      assert.deepEqual(hits[1].body, { queue: QUEUE, partition: 'room-7', messages: [{ payload: { a: 1 } }] })
    })
  })

  it('push accepts the durable push sugar: a bare value, {data}, or {payload}', async () => {
    await withEphemeral([pushed(4)], async (eph, hits) => {
      await eph.push(QUEUE, [{ a: 1 }, { data: { a: 2 } }, { payload: { a: 3 } }, 'plain'])
      assert.deepEqual(hits[0].body.messages, [
        { payload: { a: 1 } },
        { payload: { a: 2 } },
        { payload: { a: 3 } },
        { payload: 'plain' }
      ])
    })
  })

  it('push carries no transactionId — there is no dedup index to hold one', async () => {
    await withEphemeral([pushed(1)], async (eph, hits) => {
      await eph.push(QUEUE, [{ a: 1 }])
      const sent = hits[0].body.messages[0]
      assert.deepEqual(Object.keys(sent), ['payload'], 'a message on this wire is {payload} and nothing else')
    })
  })

  it('push of nothing answers pushed:0 without spending a request', async () => {
    await withEphemeral([], async (eph, hits) => {
      assert.deepEqual(await eph.push(QUEUE, []), { pushed: 0 })
      assert.equal(hits.length, 0)
    })
  })

  it('push refuses a null message rather than inventing a null payload', async () => {
    await withEphemeral([], async (eph, hits) => {
      await assert.rejects(() => eph.push(QUEUE, [null]), /null/)
      assert.equal(hits.length, 0)
    })
  })
})

describe('ephemeral wire — pop', () => {
  it('pop sends the queue and nothing else when nothing else was asked for', async () => {
    await withEphemeral([popped(QUEUE, [frame(1)])], async (eph, hits) => {
      const res = await eph.pop(QUEUE)

      assert.equal(hits[0].method, 'GET')
      assert.equal(hits[0].url, `/api/v1/ephemeral/pop?queue=${QUEUE}`)
      assert.equal(res.queue, QUEUE)
      assert.equal(res.messages.length, 1)
      assert.equal(res.messages[0].id, 'e:beef:Default:1')
      assert.equal(res.messages[0].attempts, 0)
    })
  })

  it('pop puts every declared parameter on the query string, in order', async () => {
    await withEphemeral([popped(QUEUE, [])], async (eph, hits) => {
      await eph.pop(QUEUE, { partition: 'room-7', batch: 10, wait: true, timeout: 2000, group: 'workers', autoAck: true })
      assert.equal(
        hits[0].url,
        `/api/v1/ephemeral/pop?queue=${QUEUE}&partition=room-7&batch=10&wait=true&timeout=2000&group=workers&autoAck=true`
      )
    })
  })

  it('pop sends an explicit timeout whenever it waits, and none when it does not', async () => {
    // The HTTP deadline is set PAST the server's, so the broker's long poll
    // always ends the request first. Letting the two defaults drift apart is
    // how a client starts aborting requests that were about to be answered.
    await withEphemeral([popped(QUEUE, []), popped(QUEUE, [])], async (eph, hits) => {
      await eph.pop(QUEUE, { wait: true })
      assert.equal(hits[0].url, `/api/v1/ephemeral/pop?queue=${QUEUE}&wait=true&timeout=30000`)

      await eph.pop(QUEUE, { batch: 5 })
      assert.equal(hits[1].url, `/api/v1/ephemeral/pop?queue=${QUEUE}&batch=5`, 'no wait, no timeout, no autoAck')
    })
  })

  it('pop accepts `timeoutMillis` as the same milliseconds, and refuses both spellings', async () => {
    await withEphemeral([popped(QUEUE, [])], async (eph, hits) => {
      await eph.pop(QUEUE, { wait: true, timeoutMillis: 1500 })
      assert.equal(hits[0].url, `/api/v1/ephemeral/pop?queue=${QUEUE}&wait=true&timeout=1500`)

      await assert.rejects(() => eph.pop(QUEUE, { wait: true, timeout: 1000, timeoutMillis: 1500 }))
      assert.equal(hits.length, 1)
    })
  })

  it('pop returns an EMPTY ARRAY on a timeout, and on a bodiless 204', async () => {
    await withEphemeral([popped(QUEUE, []), { status: 204 }], async (eph) => {
      const empty = await eph.pop(QUEUE, { wait: true, timeout: 10 })
      assert.deepEqual(empty, { queue: QUEUE, messages: [] })

      const noContent = await eph.pop(QUEUE)
      assert.deepEqual(noContent, { queue: QUEUE, messages: [] }, 'a 204 must not become `null` for the caller to trip over')
    })
  })
})

describe('ephemeral wire — ack', () => {
  it('ack sends the ids under `acks`, with the group beside them', async () => {
    await withEphemeral([acked({ id: 'e:beef:Default:1', outcome: 'acked' })], async (eph, hits) => {
      const res = await eph.ack(QUEUE, [frame(1)], { group: 'workers' })

      assert.equal(hits[0].method, 'POST')
      assert.equal(hits[0].url, '/api/v1/ephemeral/ack')
      assert.deepEqual(hits[0].body, { queue: QUEUE, group: 'workers', acks: [{ id: 'e:beef:Default:1' }] })
      assert.equal(res.results[0].outcome, 'acked')
    })
  })

  it('ack takes popped messages, bare id strings, or the wire objects themselves', async () => {
    await withEphemeral([acked(), acked()], async (eph, hits) => {
      await eph.ack(QUEUE, 'e:beef:Default:9')
      assert.deepEqual(hits[0].body, { queue: QUEUE, acks: [{ id: 'e:beef:Default:9' }] })

      await eph.ack(QUEUE, [{ id: 'a', status: 'retry' }, { id: 'b', status: 'failed', error: 'boom' }])
      assert.deepEqual(hits[1].body.acks, [
        { id: 'a', status: 'retry' },
        { id: 'b', status: 'failed', error: 'boom' }
      ])
    })
  })

  it('ack maps the boolean sugar and lets a per-message status win over the call-wide one', async () => {
    await withEphemeral([acked(), acked()], async (eph, hits) => {
      await eph.ack(QUEUE, ['a', 'b'], { status: false, error: 'handler threw' })
      assert.deepEqual(hits[0].body.acks, [
        { id: 'a', status: 'failed', error: 'handler threw' },
        { id: 'b', status: 'failed', error: 'handler threw' }
      ])

      await eph.ack(QUEUE, ['a', { id: 'b', status: 'retry' }], { status: true })
      assert.deepEqual(hits[1].body.acks, [
        { id: 'a', status: 'completed' },
        { id: 'b', status: 'retry' }
      ], 'the per-message status is the one the caller wrote LAST and closest to the message')
    })
  })

  it('ack omits `group` in queue mode and refuses an ack with no id', async () => {
    await withEphemeral([acked()], async (eph, hits) => {
      await eph.ack(QUEUE, ['a'])
      assert.ok(!('group' in hits[0].body), 'groupless is queue mode, not a group named null')

      await assert.rejects(() => eph.ack(QUEUE, [{ partition: 'Default' }]), /id/)
      assert.equal(hits.length, 1)
    })
  })

  it('ack of nothing answers empty results without spending a request', async () => {
    await withEphemeral([], async (eph, hits) => {
      assert.deepEqual(await eph.ack(QUEUE, []), { results: [] })
      assert.equal(hits.length, 0)
    })
  })
})

describe('ephemeral wire — status', () => {
  it('queues and depth are plain GETs on the status routes', async () => {
    await withEphemeral([ok({ queues: [] }), ok({ queue: QUEUE, depth: 3, bytes: 120 })], async (eph, hits) => {
      await eph.queues()
      assert.equal(hits[0].method, 'GET')
      assert.equal(hits[0].url, '/api/v1/ephemeral/queues')

      const depth = await eph.depth(QUEUE)
      assert.equal(hits[1].url, `/api/v1/ephemeral/queues/${QUEUE}/depth`)
      assert.equal(depth.depth, 3)
    })
  })
})

describe('ephemeral wire — the two kinds of 404', () => {
  // The status alone cannot tell these apart, which is exactly why the mapping
  // reads the body's CODE:
  //
  //   * no SDK negotiates a version (§4, §8), so a pre-1.1 broker (routes never
  //     registered) and a pre-1.1 proxy (`route_blocked`, fails closed on
  //     unknown API paths) both answer 404, and to a caller those are one fact:
  //     upgrade;
  //   * a broker that DOES support the family answers 404 with
  //     `ephemeral_queue_not_found` when `depth` names a queue that is not
  //     there — the only verb that can, since push and pop create implicitly,
  //     `reset` answers dropped:0 and `delete` answers deleted:false.
  //
  // Collapsing the second into the first sends somebody chasing a broker
  // version over a queue name typo.
  const oneClearError = (e) => {
    assert.equal(e.code, EPHEMERAL_UNSUPPORTED)
    assert.equal(e.message, 'broker/proxy does not support ephemeral queues (requires >= 1.1)')
    assert.equal(e.status, 404)
    return true
  }

  it('maps a missing broker route to the one clear error', async () => {
    await withEphemeral([OLD_BROKER], async (eph, hits) => {
      await assert.rejects(() => eph.pop(QUEUE), oneClearError)
      assert.equal(hits.length, 1, 'a 404 is not retried')
    }, { retryAttempts: 1 })
  })

  it('maps the old proxy `route_blocked` to the same error, and keeps the original as `.cause`', async () => {
    await withEphemeral([OLD_PROXY], async (eph) => {
      await assert.rejects(() => eph.configure(QUEUE, { ttlSeconds: 5 }), (e) => {
        oneClearError(e)
        assert.equal(e.cause.code, 'route_blocked', 'the proxy verdict is kept, it is just not what the caller branches on')
        return true
      })
    }, { retryAttempts: 1 })
  })

  it('a 404 for a MISSING QUEUE is its own error, not "your broker is too old"', async () => {
    await withEphemeral([QUEUE_NOT_FOUND], async (eph, hits) => {
      await assert.rejects(() => eph.depth(QUEUE), (e) => {
        assert.equal(e.code, EPHEMERAL_QUEUE_NOT_FOUND)
        assert.equal(e.status, 404)
        assert.equal(e.queue, QUEUE, 'the error names the queue that was not found')
        assert.match(e.message, /does not exist/)
        assert.doesNotMatch(e.message, /1\.1/, 'a missing queue must not read as a version problem')
        // Nothing the HTTP layer surfaced is lost by the mapping.
        assert.equal(e.cause.status, 404)
        assert.equal(e.cause.code, 'ephemeral_queue_not_found')
        assert.equal(e.cause.message, 'ephemeral queue not found')
        return true
      })
      assert.equal(hits[0].url, `/api/v1/ephemeral/queues/${QUEUE}/depth`)
    }, { retryAttempts: 1 })
  })

  it('tells the two 404s apart on the same verb, by the body and not the status', async () => {
    // The regression this pins: `depth` answering a real 404 while the routes
    // are demonstrably present, because the very next call succeeds.
    await withEphemeral([QUEUE_NOT_FOUND, OLD_BROKER], async (eph) => {
      await assert.rejects(() => eph.depth(QUEUE), e => e.code === EPHEMERAL_QUEUE_NOT_FOUND)
      await assert.rejects(() => eph.depth(QUEUE), e => e.code === EPHEMERAL_UNSUPPORTED)
    }, { retryAttempts: 1 })
  })

  it('leaves every other refusal alone, code and status intact', async () => {
    // 403 feature_gated (the cloud grant, §5.1) and 429 queue_full (§1.6) are
    // live answers of a broker that DOES support the family. Mapping them to
    // "upgrade" would send an operator chasing a version that is already right.
    const gated = { status: 403, body: { error: 'ephemeral is not enabled for this plan', code: 'feature_gated' } }
    await withEphemeral([gated], async (eph) => {
      await assert.rejects(() => eph.push(QUEUE, [{ a: 1 }]), (e) => {
        assert.equal(e.status, 403)
        assert.equal(e.code, 'feature_gated')
        return true
      })
    }, { retryAttempts: 1 })
  })
})
