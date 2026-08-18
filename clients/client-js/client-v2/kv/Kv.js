/**
 * The KV surface (PLAN_KV_TIMERS.md §5, §8.1).
 *
 * Seven operations, five code paths: get, getMany, getPrefix, put,
 * putIfAbsent (an alias that desugars to put with expect:0 inside the stored
 * procedure), delete, incr. Plus two conveniences this client owns: `once`,
 * which is the idempotency marker written the way people actually reach for
 * it, and `listAll`, which walks the keyset cursor of getPrefix.
 *
 * EVERYTHING GOES THROUGH `POST /api/v1/kv`, INCLUDING THE SINGLE-KEY READS.
 * The path routes (`GET|PUT|DELETE /api/v1/kv/:ns/*key`) exist and are correct,
 * but they are sugar "for the three cases people write by hand" (§8.1). An SDK
 * is not one of them, and the batch route buys three things a path route
 * cannot: it is the ONLY surface that accepts `incr` and `getPrefix`, so one
 * transport serves all seven ops; it never has to percent-encode a key into a
 * URL; and it keeps keys out of access logs, proxy samples and tracing spans,
 * which is the same reasoning §5.5 applies to prefixes and which does not stop
 * being true for a key.
 *
 * THE STATUS-CODE RULE YOU WILL NOTICE FIRST (§8.1): the HTTP status describes
 * the outcome of the CALL, never the verdict of the predicate. An absent key, a
 * lost putIfAbsent race, a delete that hit nothing -- all 200, with an explicit
 * field in the body. `applied:false` is the single most frequent outcome of
 * this product and it is not an error.
 *
 * WHICH LEADS TO THE ONE TRAP THIS LANGUAGE CANNOT DEFEND AGAINST
 * STRUCTURALLY (§10.4): **every write returns an OBJECT, and objects are
 * always truthy**.
 *
 *     if (await kv.delete(ns, key)) { ... }        // ALWAYS TAKEN. A bug.
 *     const r = await kv.delete(ns, key)
 *     if (r.applied) { ... }                       // the field is the answer
 *
 * That holds for all five writes -- put, putIfAbsent, delete, incr, once (whose
 * field is `won`).
 *
 * AND THE ONE ABOUT EXPIRY (§5.7): a key that has expired is never returned and
 * never counts as existing, even before the sweeper has pruned it. A `put`
 * does NOT inherit the previous TTL -- that is not expressible, because a put
 * that silently inherited an expiry is the fastest way to make a marker
 * immortal.
 */

import * as logger from '../utils/logger.js'
import { resolveExpiry } from './expiry.js'

// ---------------------------------------------------------------------------
// Op builders. Shared with TransactionBuilder, which puts the very same
// objects in the `kv` array of a bundle -- one definition of the wire shape,
// so the standalone route and the transaction rider can never drift.
//
// An entry is `{ base, expiry }`: `base` is the finished op minus its expiry,
// `expiry` is the caller's sugar, unresolved. `materializeKvOp` resolves it,
// and it is called at SEND time -- immediately here, at commit() there --
// because `until` is an instant and freezing it when the op was queued would
// ship a TTL that is already stale.
// ---------------------------------------------------------------------------

function requireName(value, what) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`kv: ${what} must be a non-empty string, got ${JSON.stringify(value)}`)
  }
  return value
}

/**
 * §5.3: an explicitly undefined or null `expect` is a CLIENT-SIDE BUG, never a
 * silent downgrade to upsert. The caller wrote the word `expect`, so they
 * declared the intention to fence; sending the op without it would perform the
 * unconditional write the fence existed to prevent.
 */
function withExpect(base, opts) {
  if ('expect' in opts) {
    if (opts.expect === undefined || opts.expect === null) {
      throw new Error(
        'kv: `expect` was written but has no value. An absent expect is not an upsert — it is a bug in the ' +
        'caller: drop the field to mean "unconditional", or pass 0 to mean "must not exist".'
      )
    }
    base.expect = opts.expect
  }
  return base
}

function withRequired(base, opts) {
  if (opts.required === true) base.required = true
  return base
}

export const kvOp = {
  get(ns, key) {
    return { base: { op: 'get', ns: requireName(ns, 'ns'), key: requireName(key, 'key') }, expiry: null }
  },

  getMany(ns, keys) {
    requireName(ns, 'ns')
    if (!Array.isArray(keys) || keys.length === 0) {
      throw new Error('kv: getMany needs a non-empty array of keys')
    }
    keys.forEach(k => requireName(k, 'key'))
    return { base: { op: 'getMany', ns, keys: [...keys] }, expiry: null }
  },

  getPrefix(ns, prefix, opts = {}) {
    requireName(ns, 'ns')
    // A namespace is not a table to enumerate: the empty prefix is the declared
    // boundary, not an oversight (§5.5).
    requireName(prefix, 'prefix')
    const base = { op: 'getPrefix', ns, prefix }
    if (opts.limit !== undefined) base.limit = opts.limit
    if (opts.after !== undefined && opts.after !== null) base.after = opts.after
    if (opts.keysOnly === true) base.keysOnly = true
    return { base, expiry: null }
  },

  put(ns, key, value, opts = {}) {
    requireName(ns, 'ns')
    requireName(key, 'key')
    const base = { op: 'put', ns, key, value }
    return { base: withRequired(withExpect(base, opts), opts), expiry: opts }
  },

  putIfAbsent(ns, key, value, opts = {}) {
    requireName(ns, 'ns')
    requireName(key, 'key')
    if ('expect' in opts) {
      throw new Error('kv: putIfAbsent IS expect:0 — passing a different expect is a contradiction the broker rejects')
    }
    const base = { op: 'putIfAbsent', ns, key, value }
    return { base: withRequired(base, opts), expiry: opts }
  },

  delete(ns, key, opts = {}) {
    requireName(ns, 'ns')
    requireName(key, 'key')
    const base = { op: 'delete', ns, key }
    return { base: withRequired(withExpect(base, opts), opts), expiry: null }
  },

  incr(ns, key, delta = 1, opts = {}) {
    requireName(ns, 'ns')
    requireName(key, 'key')
    if ('expect' in opts) {
      // §5.4: incr is the way OUT of CAS. A precondition on it would
      // reintroduce the very loop it exists to remove.
      throw new Error('kv: incr takes no expect — it is the escape from the CAS loop, not another CAS')
    }
    if (typeof delta !== 'number' || !Number.isFinite(delta)) {
      throw new Error(`kv: incr needs a finite numeric delta, got ${JSON.stringify(delta)}`)
    }
    const base = { op: 'incr', ns, key, delta }
    if (opts.min !== undefined) base.min = opts.min
    if (opts.max !== undefined) base.max = opts.max
    return { base: withRequired(base, opts), expiry: opts }
  }
}

/** Finish one op: merge the resolved expiry into the base. */
export function materializeKvOp(entry, nowMs = Date.now()) {
  if (!entry.expiry) return { ...entry.base }
  return { ...entry.base, ...resolveExpiry(entry.expiry, nowMs) }
}

// ---------------------------------------------------------------------------
// Response handling.
// ---------------------------------------------------------------------------

/**
 * The KV routes put the CODE in `error` (§9.5's closed taxonomy) and have no
 * separate `code` field, while the proxy's own refusals do. HttpClient maps
 * `body.error` onto `.message`, so on these routes the message IS the code.
 * Copying it into `.code` -- without ever overwriting a proxy-set one -- is
 * what lets callers branch on `err.code` here exactly as they do on a proxy
 * 403, and never on prose, which is forbidden everywhere in this codebase.
 */
function decorate(error) {
  if (error && !error.code && typeof error.message === 'string') {
    error.code = error.message
  }
  return error
}

/** A lost `required` gate arrives as HTTP 200 with this envelope (§8.3). */
function isPrecondition(body) {
  return body && typeof body === 'object' && body.ok === false && body.reason === 'kv_precondition'
}

/**
 * Render a precondition verdict as a WriteResult, so that `applied` remains
 * THE field to read whichever way the verdict arrived. `precondition:true`
 * says the transaction was rolled back rather than merely refused, and
 * `kvReason` is preserved under the name `reason` the other path uses.
 */
function preconditionResult(body, op, key) {
  return {
    op,
    key,
    applied: false,
    precondition: true,
    reason: body.kvReason ?? null,
    value: body.value,
    version: body.version,
    failedIndex: body.failedIndex
  }
}

/**
 * §5.4: past 2^53 `JSON.parse` loses precision SILENTLY, and `incr` runs on
 * `numeric` server-side while `version` is a BIGINT. A rate limiter reading a
 * counter that is quietly wrong is worse than one that fails, so this raises.
 */
function assertSafeNumber(value, what) {
  if (typeof value === 'number' && Math.abs(value) > Number.MAX_SAFE_INTEGER) {
    throw new Error(
      `kv: ${what} is ${value}, beyond 2^53 — JSON.parse has already lost precision on it. ` +
      'Refusing to hand back a number that is silently wrong.'
    )
  }
  return value
}

export class Kv {
  #httpClient

  constructor(httpClient) {
    this.#httpClient = httpClient
  }

  /**
   * The one path to the broker. Returns either `{ results }` (index-aligned to
   * the ops, §6.4) or `{ precondition }` for the 200-with-`ok:false` verdict.
   */
  async #apply(entries) {
    const now = Date.now()
    const operations = entries.map(e => materializeKvOp(e, now))
    logger.log('Kv.apply', { count: operations.length, ops: operations.map(o => o.op) })

    let body
    try {
      body = await this.#httpClient.post('/api/v1/kv', { operations })
    } catch (error) {
      logger.error('Kv.apply', { error: error.message, status: error.status, code: error.code })
      throw decorate(error)
    }

    if (isPrecondition(body)) return { precondition: body }

    const results = body && Array.isArray(body.results) ? body.results : null
    if (!results) {
      throw new Error('kv: unexpected response envelope — expected {"results":[...]}')
    }
    // The stored procedure guarantees one result per op and raises rather than
    // returning a short array (§6.4). A short one here means something between
    // the two rewrote the answer, and attributing result i to op j is how a
    // caller ends up trusting the wrong verdict.
    if (results.length !== operations.length) {
      throw new Error(`kv: got ${results.length} results for ${operations.length} operations`)
    }
    return { results }
  }

  async #one(entry) {
    const { results, precondition } = await this.#apply([entry])
    if (precondition) return { precondition }
    return { result: results[0] }
  }

  /** A read result, unwrapped. Reads have no precondition to lose. */
  async #read(entry) {
    const { result, precondition } = await this.#one(entry)
    if (precondition) {
      // Unreachable by construction (`required` is a write-only escalation),
      // and therefore exactly the kind of thing that must not silently return
      // `undefined` if the broker's envelope ever changes shape.
      throw new Error(`kv: ${entry.base.op} came back as a precondition verdict, which a read cannot lose`)
    }
    return result
  }

  /** A write result, with the precondition envelope folded into the same shape. */
  async #write(entry) {
    const { result, precondition } = await this.#one(entry)
    if (precondition) return preconditionResult(precondition, entry.base.op, entry.base.key)
    if (typeof result.applied !== 'boolean') {
      throw new Error(`kv: ${entry.base.op} result carries no \`applied\` field; refusing to guess the verdict`)
    }
    return result
  }

  // -------------------------------------------------------------- reads

  /**
   * One key. Returns the ROW, not the value:
   * `{found, key, value?, version?, expiresAt?, updatedAt?}`.
   *
   * `found` is separate from `value` because `null` is a legal JSONB value
   * (§5.5): `{found:true, value:null}` and `{found:false}` are different
   * things, and an SDK that returned "the value or null" would collapse them.
   */
  async get(ns, key) {
    return this.#read(kvOp.get(ns, key))
  }

  /**
   * Many keys of one namespace. Returns `{rows, missing, truncated}`.
   *
   * `missing` is EXPLICIT: absence is a datum, not a hole the caller computes
   * by difference. `truncated` means the byte budget cut the page -- those
   * keys are in neither list, because calling them absent would be a lie.
   */
  async getMany(ns, keys) {
    return this.#read(kvOp.getMany(ns, keys))
  }

  /**
   * One page of a prefix scan: `{rows, truncated, nextAfter}`.
   *
   * `limit` is CLAMPED by the broker (default 100, ceiling 1000) and never
   * rejected, plus a byte ceiling on the aggregate, with `truncated` telling
   * the truth about both. `after` is an exclusive keyset cursor, not an
   * offset.
   *
   * EACH PAGE IS ITS OWN SNAPSHOT. It is not an instant of the namespace: with
   * `after` it can miss a key inserted behind the cursor. Good for compacting
   * state, not for an exact count.
   */
  async getPrefix(ns, prefix, opts = {}) {
    return this.#read(kvOp.getPrefix(ns, prefix, opts))
  }

  /**
   * Every row under a prefix, one page at a time.
   *
   * An ASYNC GENERATOR, not an array: the page count is not knowable in
   * advance, and a method that quietly buffered a namespace into memory would
   * be the one thing this API is careful not to offer.
   *
   *     for await (const row of kv.listAll(ns, 'quota:acme:')) { ... }
   *
   * It inherits getPrefix's snapshot caveat, one page at a time.
   */
  async *listAll(ns, prefix, opts = {}) {
    let after = opts.after
    for (;;) {
      const page = await this.getPrefix(ns, prefix, { ...opts, after })
      for (const row of page.rows || []) yield row
      if (!page.truncated) return
      if (!page.nextAfter) return
      after = page.nextAfter
    }
  }

  // ------------------------------------------------------------- writes

  /**
   * Write a value. Exactly one of `ttlSeconds` (or its sugar `ttl` / `until`)
   * and `forever:true` is required -- the broker owns that rule so all seven
   * clients inherit it.
   *
   * `expect` is the optimistic lock: `0` means "must not exist" (and wins even
   * against an expired row not yet pruned), `N > 0` is a pure UPDATE that
   * creates NOTHING when it matches no row. The version handed to a loser is
   * ADVISORY -- never reuse it blindly as a fencing token.
   *
   * `required:true` escalates a lost precondition into a rolled-back
   * transaction; on this route that comes back as a WriteResult with
   * `precondition:true`, and inside a bundle it is what makes `commit()`
   * return the verdict.
   *
   * Returns a WriteResult: `{applied, op, key, value, version, reason?}`.
   * ALWAYS TRUTHY -- read `applied`.
   */
  async put(ns, key, value, opts = {}) {
    return this.#write(kvOp.put(ns, key, value, opts))
  }

  /** put with `expect:0`, under the name of the thing. Same WriteResult, and the loser gets the winner's value. */
  async putIfAbsent(ns, key, value, opts = {}) {
    return this.#write(kvOp.putIfAbsent(ns, key, value, opts))
  }

  /**
   * Delete a key, optionally fenced with `expect`.
   *
   * Returns a WriteResult. `if (await kv.delete(...))` is ALWAYS TRUE and is a
   * bug: read `.applied`.
   */
  async delete(ns, key, opts = {}) {
    return this.#write(kvOp.delete(ns, key, opts))
  }

  /**
   * Add `delta` to a numeric key, atomically and without a CAS loop.
   *
   * With `max`, **`applied` IS the admission decision**: an increment that
   * would break the ceiling does not apply, does not saturate and does not
   * truncate -- it comes back `applied:false, reason:'limit'` with the CURRENT
   * value. Comparing client-side after incrementing means the request that
   * broke the ceiling has already consumed budget.
   *
   * The TTL is CREATE-ONLY: a live row keeps its expiry, so a fixed-window
   * limiter actually closes its window. An expired row counts as zero and
   * starts a new window, which is what makes the limiter one call.
   */
  async incr(ns, key, delta = 1, opts = {}) {
    const res = await this.#write(kvOp.incr(ns, key, delta, opts))
    assertSafeNumber(res.value, `the counter at ${key}`)
    return res
  }

  /**
   * The idempotency marker, written the way it is actually used:
   *
   *     const { won } = await kv.once('test-idem', orderId, { ttl: '24h' })
   *     if (!won) return                       // somebody already did this
   *
   * `putIfAbsent` with a default value of `true`. Returns `{won, value,
   * version, result}` -- `value` being the WINNER's value, so the loser never
   * needs a second round trip.
   *
   * And the sentence that has to travel with it: **putIfAbsent plus a TTL is
   * not a distributed lock** (§5.7). A lock that expires is not revoked; the
   * old holder keeps working, it simply no longer has the row. The defence is
   * fencing -- carry the `version` as `expect` on every later write -- and
   * that limits the damage rather than removing it.
   */
  async once(ns, key, opts = {}) {
    const value = opts.value !== undefined ? opts.value : true
    const res = await this.putIfAbsent(ns, key, value, opts)
    return { won: res.applied === true, value: res.value, version: res.version, result: res }
  }
}
