/**
 * The ephemeral surface (EPHEMERAL_QUEUES.md §1, §3.1, §4).
 *
 * Eight verbs over one route family, `/api/v1/ephemeral/*`: configure, reset,
 * delete, push, pop, ack, queues, depth. Flat functions, not a builder chain --
 * the durable `queue(name).partition(p).push(...)` fluency exists because a
 * durable queue has a dozen configured properties that read well as a sentence;
 * an ephemeral queue has a ring in a broker's RAM and a handful of bounds, and
 * a chain would only hide how few moving parts there are.
 *
 * WHAT THIS CLASS IS ABOUT, BEFORE ANY SIGNATURE: contents survive NOTHING
 * (§1.2). Not a restart, not a crash, not a deploy, not the ownership move that
 * a membership change causes. Treat a failover like a Redis restart. Declared
 * CONFIGURATION is durable -- it lives in PG and comes back after a restart, as
 * configured and EMPTY. There is no replay, no history, no subscriptionMode and
 * no DLQ, because none of those concepts has a referent when there is no
 * history to have.
 *
 * DELIVERY IS NOT "AT MOST ONCE" (§1.3), and the docs must not say it is. The
 * class picks what can be LOST; the ack mode picks the guarantee. `autoAck`
 * advances the cursor at delivery and is at-most-once. The default -- explicit
 * ack -- is at-least-once for as long as the owning broker incarnation lives:
 * an unacked message redelivers when its lease expires, with `attempts`
 * incremented, until `retryLimit`, after which it is DROPPED and counted (no
 * DLQ, §9). Consumers still need idempotency, exactly as on durable queues.
 *
 * CONSUMPTION SEMANTICS COME FROM THE GROUP, EXACTLY AS ON THE DURABLE ENGINE
 * (§1.5). There is no queue-level mode to choose:
 *
 *     pop(q, { group: 'workers' })     // competing consumers: one cursor
 *     pop(q, { group: 'tail-a' })      // fan-out: this subscriber's own cursor
 *     pop(q)                           // groupless queue mode, as on durable
 *
 * Every group has its own cursor over the ONE ring, so fan-out subscribers each
 * see everything and competing consumers of one group share the work.
 *
 * ORDERING is FIFO per (queue, partition) within one ownership incarnation.
 * Across an incarnation boundary the question is empty: the contents are gone.
 *
 * AND THE ONE ERROR YOU WILL MEET FIRST. No SDK negotiates a version. Against a
 * broker or proxy older than 1.1 the whole family answers 404 -- the broker
 * because the routes do not exist, the proxy because an unknown API path is
 * `route_blocked` -- so every 404 on this family is mapped to one error with
 * `.code === EPHEMERAL_UNSUPPORTED`, and the original is kept as `.cause`.
 * Branch on the code, never on the prose.
 */

import * as logger from '../utils/logger.js'
import { EPHEMERAL_SINK, ephemeralAddress, ephemeralDestination } from '../buffer/sinks.js'

/** `error.code` on the old-broker error, so callers branch on a code, not a message. */
export const EPHEMERAL_UNSUPPORTED = 'ephemeral_unsupported'

/** The message every SDK fixes for this case (§4). Keep it identical across clients. */
export const EPHEMERAL_UNSUPPORTED_MESSAGE =
  'broker/proxy does not support ephemeral queues (requires >= 1.1)'

/**
 * The seven knobs of `configure` (§3.1). A CLOSED list: an option this client
 * does not know is refused rather than dropped on the floor, because every one
 * of these bounds something (bytes, length, age, redelivery) and a silently
 * ignored `ttlSecond` is a ring that grows until a global budget answers 503.
 */
const CONFIGURE_OPTIONS = [
  'maxBytes',
  'maxLength',
  'policy',
  'ttlSeconds',
  'leaseSeconds',
  'retryLimit',
  'windowBuffer'
]

/** Long-poll default, matching the durable pop's, when `wait` is asked for without a timeout. */
const DEFAULT_WAIT_TIMEOUT_MILLIS = 30000

// The HTTP deadline must outlive the server's own long-poll timeout, or the
// client aborts a request the broker was about to answer. Same 5s slack the
// durable pop uses.
const WAIT_TIMEOUT_SLACK_MILLIS = 5000

function requireQueue(queue) {
  if (typeof queue !== 'string' || queue.length === 0) {
    throw new Error(`ephemeral: queue must be a non-empty string, got ${JSON.stringify(queue)}`)
  }
  return queue
}

/**
 * Every 404 on this family means the same thing (§4, §8): the routes are not
 * there. An old broker 404s because it never registered them; an old proxy
 * 404s `route_blocked` because it fails closed on unknown API paths. Both are
 * "upgrade", neither is "your queue is missing" -- the ephemeral verbs answer
 * an absent queue with a normal body, never a 404.
 */
function mapUnsupported(error) {
  if (!error || error.status !== 404) return error
  const mapped = new Error(EPHEMERAL_UNSUPPORTED_MESSAGE)
  mapped.code = EPHEMERAL_UNSUPPORTED
  mapped.status = 404
  mapped.cause = error
  return mapped
}

/** Options are forwarded in a fixed order, and only when the caller gave them. */
function buildConfigureOptions(options) {
  const given = options || {}
  if (typeof given !== 'object' || Array.isArray(given)) {
    throw new Error(`ephemeral: configure options must be an object, got ${JSON.stringify(options)}`)
  }

  const unknown = Object.keys(given).filter(key => !CONFIGURE_OPTIONS.includes(key))
  if (unknown.length > 0) {
    throw new Error(
      `ephemeral: unknown configure option(s) ${unknown.join(', ')} — ` +
      `an option this client does not know would be silently dropped. Known options: ${CONFIGURE_OPTIONS.join(', ')}`
    )
  }

  const out = {}
  for (const key of CONFIGURE_OPTIONS) {
    if (given[key] !== undefined) out[key] = given[key]
  }
  return out
}

/**
 * One message on the ephemeral wire is `{payload}` and nothing else -- no
 * transactionId, because there is no dedup index to hold one, and no queue or
 * partition, because the envelope already carries them.
 *
 * The `{data:...}` / `{payload:...}` sugar is the durable push's, deliberately
 * reproduced (QueueBuilder.push) so one mental model covers both families,
 * INCLUDING its trap: an object that happens to have a `data` key is read as
 * the sugar, and its other keys do not travel. Wrap it -- `{payload: obj}` --
 * when the object is the payload.
 */
function toMessage(item) {
  if (item === undefined || item === null) {
    throw new Error('ephemeral: a message may not be null or undefined — write { payload: null } to push a null payload')
  }
  if (typeof item === 'object' && !Array.isArray(item)) {
    if ('payload' in item) return { payload: item.payload }
    if ('data' in item) return { payload: item.data }
  }
  return { payload: item }
}

function toMessages(messages) {
  const list = Array.isArray(messages) ? messages : [messages]
  return list.map(toMessage)
}

/** `true`/`false` are sugar for the two statuses people actually mean. */
function normalizeStatus(status) {
  if (typeof status === 'boolean') return status ? 'completed' : 'failed'
  return status
}

/**
 * An ack is `{id, status?, error?}`. Accepts a popped message, a bare id
 * string, or the wire object itself; a per-entry status wins over the call-wide
 * default, which is how a mixed batch (some completed, one retry) is expressed
 * in a single request.
 */
function toAcks(acks, opts) {
  const list = Array.isArray(acks) ? acks : [acks]
  return list.map((entry, index) => {
    const isObject = entry !== null && typeof entry === 'object'
    const id = typeof entry === 'string' ? entry : (isObject ? entry.id : null)
    if (typeof id !== 'string' || id.length === 0) {
      throw new Error(`ephemeral: ack at index ${index} carries no message id — pass the popped message, or its \`id\``)
    }

    const ack = { id }

    const status = isObject && entry.status !== undefined ? entry.status : opts.status
    if (status !== undefined && status !== null) ack.status = normalizeStatus(status)

    const error = isObject && entry.error !== undefined ? entry.error : opts.error
    if (error !== undefined && error !== null) ack.error = error

    return ack
  })
}

export class Ephemeral {
  #httpClient
  #bufferManager

  constructor(httpClient, bufferManager = null) {
    this.#httpClient = httpClient
    this.#bufferManager = bufferManager
  }

  /** Every request in this class goes through here, so the 404 rule has one home. */
  async #call(method, path, body = null, { timeoutMillis = null, affinityKey = null, retryKind = null } = {}) {
    try {
      if (method === 'GET') return await this.#httpClient.get(path, timeoutMillis, affinityKey, retryKind)
      if (method === 'DELETE') return await this.#httpClient.delete(path, timeoutMillis, affinityKey, retryKind)
      return await this.#httpClient.post(path, body, timeoutMillis, affinityKey, retryKind)
    } catch (error) {
      logger.error('Ephemeral.request', { method, path, status: error.status, error: error.message, code: error.code })
      throw mapUnsupported(error)
    }
  }

  // ------------------------------------------------------------ declaration

  /**
   * Declare a queue and its bounds. Persists the OPTIONS in PG (§1.1): the
   * configuration survives a restart, the contents never do, and the queue
   * comes back declared and empty.
   *
   * Optional in every sense -- a push or a pop that names an unknown queue
   * creates it implicitly with the tenant defaults (§1.1). Declare when you
   * want non-default bounds, or when you want the queue to exist in the
   * dashboard before its first message.
   *
   * Options, all optional: `maxBytes` / `maxLength` (the per-queue budget, with
   * `policy` deciding whether breaching it rejects the push with 429 or drops
   * the OLDEST message -- feed semantics), `ttlSeconds` (drop messages older
   * than this; it is NOT the durable `retention`, which cleans consumed history
   * and never touches pending), `leaseSeconds` and `retryLimit` (redelivery),
   * `windowBuffer {ms, count}` (let a waiting pop fatten its batch).
   */
  async configure(queue, options = {}) {
    requireQueue(queue)
    const body = { queue, options: buildConfigureOptions(options) }
    logger.log('Ephemeral.configure', { queue, options: Object.keys(body.options) })
    return this.#call('POST', '/api/v1/ephemeral/configure', body)
  }

  /**
   * Drop every message, void every lease, rewind every group cursor. Returns
   * `{dropped}`.
   *
   * A verb that would be indefensible on a durable queue and is merely honest
   * here: it destroys nothing the class ever promised to keep (§1.2). The
   * declared configuration stays.
   */
  async reset(queue) {
    requireQueue(queue)
    logger.log('Ephemeral.reset', { queue })
    return this.#call('POST', '/api/v1/ephemeral/reset', { queue })
  }

  /** Delete the queue: contents, cursors, and the declared configuration in PG. */
  async delete(queue) {
    requireQueue(queue)
    logger.log('Ephemeral.delete', { queue })
    return this.#call('DELETE', `/api/v1/ephemeral/queue/${encodeURIComponent(queue)}`)
  }

  // ------------------------------------------------------------------ push

  /**
   * Push one message or many. All-or-nothing per request; returns `{pushed}`.
   *
   *     await queen.ephemeral.push('presence', [{ user: 'a', typing: true }])
   *     await queen.ephemeral.push('presence', msgs, { partition: 'room-7' })
   *
   * `opts.partition` picks the ring (FIFO is per partition, §1.4); omitted, the
   * broker picks and this client does not invent a default.
   *
   * `opts.buffered {intervalMillis, messageCount, maxSize}` batches client-side
   * through the SAME machinery the durable push uses (§4.1) -- blocking
   * backpressure at `maxSize`, a failed batch back at the FRONT and retried,
   * `queen.close()` draining it with a deadline. It resolves to
   * `{buffered:true, count}` once the messages are IN the buffer, not once they
   * are at the broker, and a buffered message that has not flushed dies with
   * the process. That last part is already inside this class's contract, which
   * is exactly why buffering is a reasonable default here and a considered
   * decision on a durable queue.
   */
  async push(queue, messages, opts = {}) {
    requireQueue(queue)
    const items = toMessages(messages)
    if (items.length === 0) return { pushed: 0 }

    const partition = opts.partition ?? null

    if (opts.buffered) {
      if (!this.#bufferManager) {
        throw new Error('ephemeral: buffered push needs the client\'s buffer manager — use queen.ephemeral, not a hand-built Ephemeral')
      }
      return this.#pushBuffered(queue, partition, items, opts)
    }

    const body = { queue }
    if (partition !== null) body.partition = partition
    body.messages = items

    logger.log('Ephemeral.push', { queue, partition, count: items.length })
    return this.#call('POST', EPHEMERAL_SINK.path, body)
  }

  /**
   * The buffered variant. One buffer per `eph:<queue>/<partition>` address, so
   * an ephemeral queue and a durable queue of the same name never share a
   * buffer or a drain (§4.1).
   */
  async #pushBuffered(queue, partition, items, opts) {
    const address = ephemeralAddress(queue, partition)
    const destination = ephemeralDestination(queue, partition)
    const bufferOptions = bufferOptionsFrom(opts.buffered)
    const accepted = []

    // Awaited one at a time: addMessage is where the maxSize bound blocks, so a
    // buffered push that resolved without awaiting would report success for
    // messages the buffer never accepted.
    try {
      for (const item of items) {
        await this.#bufferManager.addMessage(address, item, bufferOptions, { signal: opts.signal, destination })
        accepted.push(item)
      }
    } catch (error) {
      logger.error('Ephemeral.push', {
        queue,
        partition,
        status: 'not-buffered',
        count: items.length - accepted.length,
        error: error.message
      })
      throw error
    }

    logger.log('Ephemeral.push', { queue, partition, status: 'buffered', count: accepted.length })
    return { buffered: true, count: accepted.length }
  }

  /** Send everything buffered for one ephemeral queue/partition, now. */
  async flush(queue, opts = {}) {
    requireQueue(queue)
    if (!this.#bufferManager) return
    const address = ephemeralAddress(queue, opts.partition ?? null)
    logger.log('Ephemeral.flush', { queue, partition: opts.partition ?? null })
    await this.#bufferManager.flushBuffer(address, { deadlineMillis: opts.deadlineMillis ?? null })
  }

  // ------------------------------------------------------------------- pop

  /**
   * Take up to `batch` messages. Resolves to `{queue, messages}`, with
   * `messages` an EMPTY ARRAY when there was nothing -- never null, so the
   * destructure is always safe:
   *
   *     const { messages } = await queen.ephemeral.pop('inbox', { wait: true })
   *
   * Each message is `{id, partition, payload, attempts}`. The `id` is opaque:
   * it encodes the owning broker incarnation, which is what lets an ack that
   * arrives after a restart or an ownership move answer `stale` instead of
   * acking somebody else's message.
   *
   * `wait:true` is a real long poll, parked on a RAM gate with no database
   * behind it and no polling interval anywhere (§3.4) -- the structural reason
   * an ephemeral inbox answers in transport time. `timeout` is milliseconds
   * (default 30000 when waiting), and the HTTP deadline is set past it so the
   * broker's timeout always fires first.
   *
   * `group` is the whole of the consumption semantics (§1.5): same group =
   * competing consumers, own group = fan-out, no group = queue mode.
   * `autoAck:true` commits at delivery and is at-most-once.
   */
  async pop(queue, opts = {}) {
    requireQueue(queue)

    const partition = opts.partition ?? null
    const group = opts.group ?? null
    const wait = opts.wait === true
    const timeoutMillis = resolveTimeout(opts)

    const params = new URLSearchParams({ queue })
    if (partition !== null) params.append('partition', partition)
    if (opts.batch !== undefined && opts.batch !== null) params.append('batch', String(opts.batch))
    // Sent only when true, so a plain pop is the shortest query this route can
    // receive and the broker's own defaults own everything else.
    if (wait) {
      params.append('wait', 'true')
      params.append('timeout', String(timeoutMillis))
    }
    if (group !== null) params.append('group', group)
    if (opts.autoAck === true) params.append('autoAck', 'true')

    logger.log('Ephemeral.pop', { queue, partition, group, batch: opts.batch ?? null, wait })

    // Affinity so repeated pops of one queue land on one backend when the
    // client holds several URLs: the broker forwards to the rendezvous owner
    // either way, so this saves a hop, it does not create correctness.
    const affinityKey = `${queue}:${partition || '*'}:${group || '__QUEUE_MODE__'}`
    const result = await this.#call('GET', `/api/v1/ephemeral/pop?${params}`, null, {
      timeoutMillis: wait ? timeoutMillis + WAIT_TIMEOUT_SLACK_MILLIS : null,
      affinityKey,
      // A long poll that meets a 429 should back off and keep waiting rather
      // than give up after a handful of tries.
      retryKind: wait ? 'pop' : null
    })

    const messages = result && Array.isArray(result.messages) ? result.messages.filter(m => m != null) : []
    logger.log('Ephemeral.pop', { queue, status: messages.length > 0 ? 'success' : 'empty', count: messages.length })
    return { queue: (result && result.queue) || queue, messages }
  }

  // ------------------------------------------------------------------- ack

  /**
   * Acknowledge popped messages. Returns `{results:[{id, outcome}]}` with
   * `outcome ∈ {acked, redelivered, stale, unknown}`.
   *
   *     await queen.ephemeral.ack('inbox', messages, { group: 'workers' })
   *     await queen.ephemeral.ack('inbox', [{ id, status: 'retry' }])
   *
   * `stale` is NOT an error and never arrives as one: it is the answer to an
   * ack whose message belonged to a previous incarnation of the ring, which is
   * how this class fences a restart or an ownership move without a lease
   * protocol. Pass the same `group` the pop used -- cursors are per group.
   *
   * `status` is `completed` (default), `failed` or `retry`; `false` is sugar
   * for `failed`. A failed or retried message comes back with `attempts+1`
   * until `retryLimit`, then it is dropped and counted. There is no DLQ.
   */
  async ack(queue, acks, opts = {}) {
    requireQueue(queue)
    const list = toAcks(acks, opts)
    if (list.length === 0) return { results: [] }

    const body = { queue }
    if (opts.group !== undefined && opts.group !== null) body.group = opts.group
    body.acks = list

    logger.log('Ephemeral.ack', { queue, group: opts.group ?? null, count: list.length })
    return this.#call('POST', '/api/v1/ephemeral/ack', body)
  }

  // ---------------------------------------------------------------- status

  /**
   * Every ephemeral queue this tenant currently has, declared and implicit.
   *
   * Free to poll: the gauges are read out of the broker's own memory, with no
   * database behind them -- unlike the durable meter, whose 1s poll is
   * load-bearing on PG.
   */
  async queues() {
    logger.log('Ephemeral.queues', {})
    return this.#call('GET', '/api/v1/ephemeral/queues')
  }

  /** Depth gauges for one queue: ring length, bytes, and the per-group cursors. */
  async depth(queue) {
    requireQueue(queue)
    logger.log('Ephemeral.depth', { queue })
    return this.#call('GET', `/api/v1/ephemeral/queues/${encodeURIComponent(queue)}/depth`)
  }
}

/**
 * Buffer options are `QueueBuilder.buffer()`'s, unchanged (§4.1): the two
 * families share the machinery, so they share its vocabulary --
 * `{messageCount, timeMillis, maxSize, retryDelayMillis}`. `intervalMillis` is
 * accepted as a spelling of `timeMillis` because it is the name the ephemeral
 * plan's API sketch used; it is TRANSLATED rather than passed through, since
 * MessageBuffer carries unknown keys untouched and would silently ignore it --
 * a linger option that quietly does nothing is a producer that batches on count
 * alone and stalls below the threshold. Both spellings at once is refused.
 */
function bufferOptionsFrom(buffered) {
  const given = buffered === true ? {} : (buffered || {})
  if (typeof given !== 'object' || Array.isArray(given)) {
    throw new Error(`ephemeral: \`buffered\` must be true or an options object, got ${JSON.stringify(buffered)}`)
  }

  const { intervalMillis, ...options } = given
  if (intervalMillis !== undefined) {
    if (options.timeMillis !== undefined) {
      throw new Error('ephemeral: pass either `timeMillis` or `intervalMillis`, not both — they are the same linger')
    }
    options.timeMillis = intervalMillis
  }
  return options
}

/**
 * `timeout` is the wire's name and milliseconds is the SDK's unit, so both
 * spellings are accepted -- and BOTH AT ONCE is refused rather than silently
 * resolved, the same rule the KV expiry sugar follows.
 */
function resolveTimeout(opts) {
  const hasTimeout = opts.timeout !== undefined && opts.timeout !== null
  const hasMillis = opts.timeoutMillis !== undefined && opts.timeoutMillis !== null
  if (hasTimeout && hasMillis) {
    throw new Error('ephemeral: pass either `timeout` or `timeoutMillis`, not both — they are the same milliseconds')
  }
  if (hasTimeout) return opts.timeout
  if (hasMillis) return opts.timeoutMillis
  return DEFAULT_WAIT_TIMEOUT_MILLIS
}
