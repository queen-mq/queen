/**
 * The scripted plan server, plus the ephemeral envelopes these tests answer
 * with.
 *
 * The server itself is kv-unit's -- a real node:http socket driven by a canned
 * list of responses, recording method, url, raw bytes and parsed body for every
 * request. It is IMPORTED rather than copied for the same reason
 * conflation-unit imports it: two plan servers would drift, and the day one of
 * them stops recording `raw` is the day a byte-identity pin silently becomes a
 * shape check.
 *
 * What is local here is the vocabulary: the ephemeral routes have their own
 * envelopes (§3.1), and a test that spells `{queue, messages}` out by hand at
 * every call site hides the wire behind its own noise.
 */

import { Queen } from '../../client-v2/index.js'
import { withPlanServer, ok } from '../kv-unit/_planServer.js'

export { withPlanServer, ok }

/** `POST /api/v1/ephemeral/push` -> 201 `{pushed}` (all-or-nothing per request). */
export function pushed(count) {
  return { status: 201, body: { pushed: count } }
}

/** `GET /api/v1/ephemeral/pop` -> 200 `{queue, messages}`; empty array on timeout. */
export function popped(queue, messages = []) {
  return ok({ queue, messages })
}

/** One delivered frame, shaped like a real ephemeral pop element (§3.1). */
export function frame(n = 1, extra = {}) {
  return { id: `e:beef:Default:${n}`, partition: 'Default', payload: { n }, attempts: 0, ...extra }
}

/** `POST /api/v1/ephemeral/ack` -> 200 `{results:[{id, outcome}]}`. */
export function acked(...results) {
  return ok({ results })
}

/**
 * What a broker or proxy older than 1.1 answers on every route of this family:
 * the broker because the routes do not exist, the proxy because an unknown API
 * path is `route_blocked` and fails closed.
 */
export const OLD_BROKER = { status: 404, body: { error: 'not_found' } }
export const OLD_PROXY = { status: 404, body: { error: 'route_blocked', code: 'route_blocked' } }

/**
 * Run `fn(ephemeral, hits, queen)` against a plan server, closing the client
 * afterwards. `handleSignals:false` because a test process must not have its
 * SIGINT handler replaced by every client it builds.
 */
export async function withEphemeral(plan, run, clientOptions = {}) {
  await withPlanServer(plan, ok({ ok: true }), async (url, hits) => {
    const queen = new Queen({ url, handleSignals: false, ...clientOptions })
    try {
      await run(queen.ephemeral, hits, queen)
    } finally {
      await queen.close()
    }
  })
}
