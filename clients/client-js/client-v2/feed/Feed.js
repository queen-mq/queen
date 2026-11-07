/**
 * The tenant feed: every push to a tenant's queues, in the order the broker's
 * log committed them, read by position (`POST /api/v1/feed`).
 *
 *   // What happened to one booking today, then stop.
 *   for await (const e of queen.feed({ partitions: ['booking-9137'], since: '1d', follow: false })) {
 *     console.log(e.position, e.queue, e.partition, e.data)
 *   }
 *
 *   // Every Italian order from now on, as it is committed.
 *   for await (const e of queen.feed({ queues: ['orders'], where: [['data.country', '=', 'IT']] })) { ... }
 *
 * A position is the broker log entry that wrote a message. One number orders
 * every queue of the tenant, and the pushes of one transaction share theirs.
 * Nothing is leased, acked or moved: the feed is a read, and every consumer
 * group still gets its messages.
 *
 * Followers serve it. A leader answers 421 with the followers to ask, and the
 * reader then reads from those (and goes back to the configured URL if none
 * answers). A single broker serves it itself.
 */

const FEED_ON_LEADER = 'feed_on_leader'
const OPS = new Set(['=', '==', '!=', '<', '<=', '>', '>=', 'in', 'exists', 'prefix', '!prefix'])
const DEFAULT_WAIT_MILLIS = 5000
// A long poll may hold the request this long; the HTTP timeout must be longer.
const MAX_WAIT_MILLIS = 30000

/**
 * One `where` condition, checked: `{ field, op, value }`.
 *
 *   whereCondition('data.country', '=', 'IT')
 *   whereCondition('data.total', '>', 100)
 *   whereCondition('partition', 'prefix', 'booking-')
 *   whereCondition('transactionId', '!prefix', 'fd-')
 *   whereCondition('data.vip', 'exists')
 *
 * Fields: queue, partition, transactionId, offset, position, data, data.<path>.
 * Ops: = != < <= > >= in exists prefix !prefix. A field the message does not
 * have holds for no op, `!=` and `!prefix` included.
 */
export function whereCondition(field, op, value) {
  if (typeof field !== 'string' || field.length === 0) {
    throw new Error(`where: field must be a non-empty string, got ${JSON.stringify(field)}`)
  }
  if (!OPS.has(op)) {
    throw new Error(`where: unknown op ${JSON.stringify(op)} (= != < <= > >= in exists prefix !prefix)`)
  }
  if (op === 'in' && !Array.isArray(value)) {
    throw new Error(`where: in on ${field} needs an array`)
  }
  if ((op === 'prefix' || op === '!prefix') && typeof value !== 'string') {
    throw new Error(`where: ${op} on ${field} needs a string`)
  }
  const cond = { field, op }
  if (op !== 'exists') cond.value = value === undefined ? null : value
  return cond
}

/** `where` as the caller wrote it: a condition, `[field, op, value]`, or a list of either. */
export function normalizeWhere(where) {
  if (where === undefined || where === null) return []
  const list = Array.isArray(where) && (where.length === 0 || typeof where[0] !== 'string') ? where : [where]
  return list.map((c) => {
    if (Array.isArray(c)) return whereCondition(c[0], c[1], c[2])
    if (c && typeof c === 'object') return whereCondition(c.field, c.op, c.value)
    throw new Error(`where: a condition is { field, op, value } or [field, op, value], got ${JSON.stringify(c)}`)
  })
}

function jsonEq(a, b) {
  if (typeof a === 'number' && typeof b === 'number') return a === b
  if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') return a === b
  return JSON.stringify(a) === JSON.stringify(b)
}

function compare(a, b) {
  if (typeof a === 'number' && typeof b === 'number') return a < b ? -1 : a > b ? 1 : 0
  if (typeof a === 'string' && typeof b === 'string') return a < b ? -1 : a > b ? 1 : 0
  return null
}

function fieldValue(field, msg) {
  switch (field) {
    case 'queue': return msg.queue ?? msg.queueName
    case 'partition': return msg.partition ?? msg.partitionName
    case 'transactionId': return msg.transactionId
    case 'offset': return msg.offset
    case 'position': return msg.position
    case 'data': return msg.data
  }
  if (!field.startsWith('data.')) return undefined
  let cur = msg.data
  for (const key of field.slice(5).split('.')) {
    if (cur === null || typeof cur !== 'object') return undefined
    cur = Array.isArray(cur) ? cur[Number(key)] : cur[key]
    if (cur === undefined) return undefined
  }
  return cur
}

/**
 * Whether `cond` holds for a message, evaluated here: the same rules the
 * broker applies to a feed read, for a stream whose source is a queue.
 */
export function conditionHolds(cond, msg) {
  const v = fieldValue(cond.field, msg)
  if (v === undefined) return false
  switch (cond.op) {
    case 'exists': return true
    case '=': case '==': return jsonEq(v, cond.value)
    case '!=': return !jsonEq(v, cond.value)
    case 'in': return Array.isArray(cond.value) && cond.value.some((x) => jsonEq(v, x))
    case 'prefix': return typeof v === 'string' && typeof cond.value === 'string' && v.startsWith(cond.value)
    case '!prefix': return typeof v === 'string' && typeof cond.value === 'string' && !v.startsWith(cond.value)
    case '<': { const c = compare(v, cond.value); return c !== null && c < 0 }
    case '<=': { const c = compare(v, cond.value); return c !== null && c <= 0 }
    case '>': { const c = compare(v, cond.value); return c !== null && c > 0 }
    case '>=': { const c = compare(v, cond.value); return c !== null && c >= 0 }
  }
  return false
}

const UNITS = { ms: 1, s: 1000, m: 60000, h: 3600000, d: 86400000, w: 604800000 }

/**
 * A start time as the broker reads it: a Date, epoch milliseconds, an ISO
 * string, or a duration back from now ('30s', '15m', '2h', '1d', '1w').
 */
export function sinceToIso(since, now = Date.now()) {
  if (since instanceof Date) return since.toISOString()
  if (typeof since === 'number' && Number.isFinite(since)) return new Date(since).toISOString()
  if (typeof since === 'string') {
    const m = /^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)\s*$/.exec(since)
    if (m) return new Date(now - Number(m[1]) * UNITS[m[2]]).toISOString()
    const t = Date.parse(since)
    if (Number.isFinite(t)) return new Date(t).toISOString()
  }
  throw new Error(`feed: since must be a Date, epoch ms, an ISO time or a duration like '15m', got ${JSON.stringify(since)}`)
}

function list(v, what) {
  if (v === undefined || v === null) return undefined
  const out = Array.isArray(v) ? v : [v]
  for (const x of out) {
    if (typeof x !== 'string' || x.length === 0) {
      throw new Error(`feed: ${what} must be names (strings), got ${JSON.stringify(x)}`)
    }
  }
  return out.length ? out : undefined
}

/**
 * A 404 from the feed route: the broker predates the feed. Said plainly, and
 * marked `noFeedRoute`, so a runner stops instead of retrying forever.
 */
function noFeedRoute(cause) {
  const err = new Error(
    'feed: this broker has no POST /api/v1/feed (it answered 404): it is older than 2.8.0, ' +
    'the first broker with the tenant feed.'
  )
  err.status = 404
  err.code = 'no_feed_route'
  err.noFeedRoute = true
  err.cause = cause
  return err
}

/** `shard` as the broker reads it, checked: `{ count, index, queues? }`, or undefined. */
function shardOption(shard) {
  if (shard === undefined || shard === null) return undefined
  const { count, index, queues } = shard
  if (!Number.isInteger(count) || count < 1 || count > 4096) {
    throw new Error(`feed: shard.count is a whole number from 1 to 4096, got ${JSON.stringify(count)}`)
  }
  if (!Number.isInteger(index) || index < 0 || index >= count) {
    throw new Error(`feed: shard.index is from 0 to ${count - 1}, got ${JSON.stringify(index)}`)
  }
  const out = { count, index }
  const q = list(queues, 'shard.queues')
  if (q) out.queues = q
  return out
}

/** A feed entry as the reader hands it out: the message in Queen's shape, plus its position. */
function toEntry(e) {
  return {
    position: e.position,
    kind: e.kind,
    queue: e.queue,
    partition: e.partition,
    partitionId: e.partitionId,
    offset: e.offset,
    id: e.id,
    transactionId: e.transactionId,
    data: e.payload,
    ts: e.ts,
    createdAt: e.ts
  }
}

export class FeedReader {
  #http
  #clientFor
  #scheme
  #opts
  #position
  #stopped = false
  // The nodes a leader named, as clients, tried in order before the configured URL.
  #followers = []
  #followerAt = 0
  #fallbackUntil = 0

  /**
   * @param {object} httpClient - the Queen client's own HTTP client
   * @param {(baseUrl: string) => object} clientFor - a client to one node, same credentials
   * @param {string} scheme - 'http' or 'https', from the configured URL
   * @param {object} [options]
   * @param {string|string[]} [options.queues] - names or patterns (`orders*`); default every queue
   * @param {string|string[]} [options.partitions] - partition names
   * @param {Array} [options.where] - conditions sent to the broker ([field, op, value] or {field, op, value})
   * @param {number|'now'|'earliest'} [options.from] - first position; default 'now' ('earliest' with since)
   * @param {Date|number|string} [options.since] - only messages created at or after it ('1d', ISO, Date)
   * @param {boolean} [options.follow=true] - keep waiting for new entries; false stops once caught up
   * @param {number} [options.waitMillis=5000] - long poll of each read while caught up (max 30000)
   * @param {number} [options.maxRecords] - messages per read (broker default 1000)
   * @param {number} [options.maxBytes] - payload bytes per read (broker default 1 MiB)
   * @param {string[]} [options.followerUrls] - nodes to read from first, e.g. one follower's URL
   * @param {{count: number, index: number, queues?: string[]}} [options.shard] - only the partitions
   *   whose name hashes (FNV-1a, 32 bit, UTF-8) to `index` of `count`; with `queues`, only those
   *   queues are split and the others are read whole
   */
  constructor(httpClient, clientFor, scheme, options = {}) {
    const o = options || {}
    this.#http = httpClient
    this.#clientFor = clientFor
    this.#scheme = scheme || 'http'
    this.#opts = {
      queues: list(o.queues ?? o.queue, 'queues'),
      partitions: list(o.partitions ?? o.partition, 'partitions'),
      where: normalizeWhere(o.where),
      kinds: o.kinds,
      since: o.since !== undefined ? sinceToIso(o.since) : undefined,
      follow: o.follow !== false,
      waitMillis: Math.min(Math.max(o.waitMillis ?? DEFAULT_WAIT_MILLIS, 0), MAX_WAIT_MILLIS),
      maxRecords: o.maxRecords,
      maxBytes: o.maxBytes,
      shard: shardOption(o.shard)
    }
    for (const url of o.followerUrls || []) this.#followers.push({ url, client: clientFor(url) })
    this.#position = o.from ?? (o.since !== undefined ? 'earliest' : 'now')
    if (!(this.#position === 'now' || this.#position === 'earliest' || (Number.isInteger(this.#position) && this.#position >= 0))) {
      throw new Error(`feed: from must be a position, 'now' or 'earliest', got ${JSON.stringify(this.#position)}`)
    }
  }

  /** Where the next read starts: a position once a read has answered. */
  get position() {
    return this.#position
  }

  /** Move the reader (a runner whose commit lost goes back to the committed position). */
  seek(position) {
    this.#position = position
  }

  /**
   * One read: `{ entries, next, resolvedTs, caughtUp, applied }`, and the
   * reader moves to `next`. Waits up to `waitMillis` for an entry while there
   * is nothing new.
   */
  async read({ waitMillis, signal } = {}) {
    const wait = Math.min(Math.max(waitMillis ?? this.#opts.waitMillis, 0), MAX_WAIT_MILLIS)
    const body = { from: this.#position, maxWaitMs: wait }
    if (this.#opts.queues) body.queues = this.#opts.queues
    if (this.#opts.partitions) body.partitions = this.#opts.partitions
    if (this.#opts.where.length) body.where = this.#opts.where
    if (this.#opts.kinds) body.kinds = this.#opts.kinds
    if (this.#opts.since !== undefined) body.since = this.#opts.since
    if (this.#opts.maxRecords !== undefined) body.maxRecords = this.#opts.maxRecords
    if (this.#opts.maxBytes !== undefined) body.maxBytes = this.#opts.maxBytes
    if (this.#opts.shard) body.shard = this.#opts.shard
    const answer = await this.#post(body, wait + 10000, signal)
    this.#position = answer.next
    return {
      entries: (answer.entries || []).map(toEntry),
      next: answer.next,
      resolvedTs: answer.resolvedTs,
      caughtUp: answer.caughtUp === true,
      applied: answer.applied
    }
  }

  /**
   * One call that reads nothing (from 'now') and leaves the reader where it
   * is: it throws what a read would, a broker without the feed included.
   */
  async probe() {
    return this.#post({ from: 'now', maxWaitMs: 0, maxRecords: 1 }, 10000)
  }

  /** Every entry, in log order. Stops once caught up when `follow` is false, or at stop(). */
  async * [Symbol.asyncIterator]() {
    while (!this.#stopped) {
      const page = await this.read({ waitMillis: this.#opts.follow ? this.#opts.waitMillis : 0 })
      for (const e of page.entries) {
        yield e
        if (this.#stopped) return
      }
      if (!this.#opts.follow && page.caughtUp) return
    }
  }

  /** Ends the iteration after the entry in hand. */
  stop() {
    this.#stopped = true
  }

  /** Closes the clients this reader opened to followers. */
  async close() {
    this.#stopped = true
    await Promise.all(this.#followers.map((f) => f.client.destroy?.()))
    this.#followers = []
  }

  #learn(followers) {
    const seen = new Set(this.#followers.map((f) => f.url))
    for (const f of followers || []) {
      const addr = typeof f?.httpAddr === 'string' ? f.httpAddr.trim() : ''
      if (!addr) continue
      const url = /^https?:\/\//.test(addr) ? addr : `${this.#scheme}://${addr}`
      if (seen.has(url)) continue
      seen.add(url)
      this.#followers.push({ url, client: this.#clientFor(url) })
    }
  }

  /** `signal` (optional) cancels the read: it rejects with `aborted: true`, and the reader stays where it was. */
  async #post(body, timeoutMillis, signal = null) {
    let lastError = null
    // Each follower once, then the configured URL; a leader's 421 adds followers.
    for (let attempt = 0; attempt < this.#followers.length + 3; attempt++) {
      const useFollower = this.#followers.length > 0 && Date.now() >= this.#fallbackUntil
      const target = useFollower ? this.#followers[this.#followerAt % this.#followers.length] : null
      const client = target ? target.client : this.#http
      try {
        return await client.post('/api/v1/feed', body, timeoutMillis, null, null, signal)
      } catch (err) {
        if (err && err.aborted) throw err
        lastError = err
        if (err && err.status === 421 && err.code === FEED_ON_LEADER) {
          // A leader: read from the followers it names (this node may lead now).
          this.#learn(err.body && err.body.followers)
          if (target) this.#followerAt++
          this.#fallbackUntil = 0
          continue
        }
        if (target && (!err.status || err.status >= 500)) {
          // That follower is gone or busy: the next one, then the configured URL.
          this.#followerAt++
          if (this.#followerAt % this.#followers.length === 0) this.#fallbackUntil = Date.now() + 5000
          continue
        }
        if (err && err.status === 404) throw noFeedRoute(err)
        throw err
      }
    }
    const err = new Error(
      'feed: every node asked is the leader or did not answer; the feed is read from followers ' +
      '(pass followerUrls, or run the broker with QUEEN_FEED_ON_LEADER=true on a test cluster)'
    )
    err.cause = lastError
    throw err
  }
}
