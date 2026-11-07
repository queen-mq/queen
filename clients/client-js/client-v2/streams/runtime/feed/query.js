/**
 * Live state of a running feed stream, read from outside its process.
 *
 * Each active runner (each shard) answers questions on an ephemeral queue of
 * its own, `queen-feed-q.<hash of the queryId>.<shard>`; the asker mints an
 * inbox, sends the question with it as `replyTo`, and waits there. Nothing is
 * stored, no extra port is opened: the request and the reply are two
 * in-memory messages on the broker.
 *
 *   const view = Stream.view(queen, 'reply-time')
 *   await view.get('perTenant', 'tenant-42')   // { found, value, shard, position }
 *   await view.list('perTenant', { limit: 50 })
 *   await view.status()                         // per shard: role, position, lag, open
 *
 * A sharded stream is asked on every shard (a key's shard depends on the
 * step's key, which only the stream knows); the answers merge. The state is
 * the live one, as of `position` (the next message the runner reads), which
 * may include results not committed yet.
 */
import crypto from 'node:crypto'
import * as logger from '../../../utils/logger.js'

const sha = (s) => crypto.createHash('sha256').update(s).digest('hex')
const DEFAULT_TIMEOUT_MILLIS = 5000
const SERVE_WAIT_MILLIS = 10000

/** The ephemeral queue the runner of `queryId` (shard `index`) answers on. */
export function queryQueueName(queryId, index = 0) {
  return `queen-feed-q.${sha(`queen-feed\n${queryId}`).slice(0, 16)}.${index}`
}

/** The loop that answers questions for one active runner. */
export class StateServer {
  #queen
  #queue
  #answer
  #waitMillis
  #stopped = false
  #loop = null
  #warned = false

  /**
   * @param {object} queen
   * @param {string} queue - the queue to answer on
   * @param {(req: object) => object} answer - the reply to a question
   */
  constructor(queen, queue, answer, { waitMillis = SERVE_WAIT_MILLIS } = {}) {
    this.#queen = queen
    this.#queue = queue
    this.#answer = answer
    this.#waitMillis = waitMillis
  }

  get running() {
    return this.#loop !== null && !this.#stopped
  }

  start() {
    if (this.#loop || !this.#queen.ephemeral) return
    this.#stopped = false
    this.#loop = this.#run().finally(() => { this.#loop = null })
  }

  /** Stops after the long poll in hand answers (at most `waitMillis`); not awaited by the runner. */
  stop() {
    this.#stopped = true
    return this.#loop || Promise.resolve()
  }

  async #run() {
    let backoff = 250
    while (!this.#stopped) {
      let got
      try {
        got = await this.#queen.ephemeral.pop(this.#queue, { wait: true, timeout: this.#waitMillis, batch: 32, commitOnDelivery: true })
        backoff = 250
      } catch (err) {
        if (this.#stopped) return
        if (err && (err.code === 'ephemeral_unsupported' || err.status === 404)) {
          logger.warn('FeedStateServer.unsupported', { queue: this.#queue, error: err.message })
          return
        }
        if (!this.#warned) {
          this.#warned = true
          logger.warn('FeedStateServer.pop', { queue: this.#queue, error: err?.message })
        }
        await new Promise((r) => setTimeout(r, backoff).unref?.())
        backoff = Math.min(backoff * 2, 5000)
        continue
      }
      for (const m of got.messages || []) {
        const req = m && m.payload
        if (!req || typeof req !== 'object' || typeof req.replyTo !== 'string') continue
        let reply
        try {
          reply = this.#answer(req)
        } catch (err) {
          reply = { error: err && err.message ? err.message : String(err) }
        }
        try {
          await this.#queen.ephemeral.push(req.replyTo, [{ payload: { ...reply, id: req.id } }])
        } catch (err) {
          logger.warn('FeedStateServer.reply', { queue: this.#queue, error: err?.message })
        }
      }
    }
  }
}

/** Ask the runners of a stream about their live state. */
export class FeedView {
  #queen
  #queryId
  #shards
  #timeout

  /**
   * @param {object} queen
   * @param {string} queryId
   * @param {object} [opts]
   * @param {number} [opts.shards] - how many shards it runs in; default: read from its layout in KV, else 1
   * @param {number} [opts.timeoutMillis=5000] - how long to wait for the answers
   */
  constructor(queen, queryId, { shards, timeoutMillis = DEFAULT_TIMEOUT_MILLIS } = {}) {
    if (!queen || !queen.ephemeral) throw new Error('Stream.view(queen, queryId): pass the Queen client (it asks over ephemeral queues)')
    if (typeof queryId !== 'string' || queryId.length === 0) throw new Error('Stream.view: queryId is the stream\'s queryId')
    this.#queen = queen
    this.#queryId = queryId
    this.#shards = shards
    this.#timeout = timeoutMillis
  }

  /** The value of one key in one step: `{ found, value?, shard?, position? }`. */
  async get(step, key) {
    const answers = await this.#ask({ op: 'get', step, key: String(key) })
    const hit = answers.find((a) => a.found)
    if (hit) return { found: true, value: hit.value, shard: hit.shard, position: hit.position }
    const err = answers.find((a) => a.error)
    if (err && !answers.some((a) => !a.error)) throw new Error(`Stream.view: ${err.error}`)
    return { found: false, answered: answers.length }
  }

  /** Up to `limit` entries of one step, from every shard: `{ items, truncated }`. */
  async list(step, { limit = 100 } = {}) {
    const answers = await this.#ask({ op: 'list', step, limit })
    const items = []
    let truncated = false
    for (const a of answers) {
      if (a.error) throw new Error(`Stream.view: ${a.error}`)
      items.push(...(a.items || []))
      truncated = truncated || !!a.truncated
    }
    return { items: items.slice(0, limit), truncated: truncated || items.length > limit, answered: answers.length }
  }

  /** Every shard's runner: role, position, lag, open state, per step. */
  async status() {
    return this.#ask({ op: 'status' })
  }

  async #shardCount() {
    if (Number.isInteger(this.#shards) && this.#shards > 0) return this.#shards
    try {
      const row = await this.#queen.kv.get('queen-feed', `${this.#queryId}#layout`)
      if (row && row.found && Number.isInteger(row.value?.shards)) return row.value.shards
    } catch (_) { /* no layout: one shard */ }
    return 1
  }

  async #ask(question) {
    const count = await this.#shardCount()
    const inbox = `queen-feed-r.${crypto.randomBytes(8).toString('hex')}`
    const id = crypto.randomBytes(6).toString('hex')
    const eph = this.#queen.ephemeral
    await Promise.all(Array.from({ length: count }, (_, i) =>
      eph.push(queryQueueName(this.#queryId, count > 1 ? i : 0), [{ payload: { ...question, id, replyTo: inbox } }])))
    const answers = []
    const deadline = Date.now() + this.#timeout
    while (answers.length < count) {
      const left = deadline - Date.now()
      if (left <= 0) break
      const r = await eph.pop(inbox, { wait: true, timeout: Math.min(left, 30000), batch: count, commitOnDelivery: true })
      for (const m of r.messages || []) if (m.payload && m.payload.id === id) answers.push(m.payload)
    }
    // The inbox goes away with the broker's own cleanup too; this is a courtesy.
    if (typeof eph.delete === 'function') Promise.resolve().then(() => eph.delete(inbox)).catch(() => {})
    if (!answers.length) {
      throw Object.assign(new Error(
        `Stream.view: no runner of ${this.#queryId} answered within ${this.#timeout} ms (is it running, with serveState on?)`
      ), { code: 'feed_view_timeout' })
    }
    return answers
  }
}
