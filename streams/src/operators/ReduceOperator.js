/**
 * ReduceOperator — fold envelopes within a window into a single value.
 *
 * Operates over a *batch* of envelopes (called once per cycle, after
 * stateless ops + window annotation). Returns:
 *   - state mutations (upserts for still-open windows, deletes for closed)
 *   - emit values (closed windows' final accumulator) for sink/foreach
 *
 * The reducer expects each envelope to carry `key`, `windowStart`,
 * `windowEnd`, and `windowKey` (from upstream KeyBy + WindowTumbling).
 *
 * Window-close semantics: a window is treated as closed when its
 * windowEnd <= maxWindowEnd in this cycle's batch. Any window equal to
 * maxWindowEnd is still open (more events may arrive on the next cycle).
 */
export class ReduceOperator {
  constructor(fn, initial) {
    this.kind = 'reduce'
    this.fn = fn
    this.initial = initial
    this.config = { hasInitial: initial !== undefined }
  }

  /**
   * Run the reducer over a batch of envelopes that all share the same
   * source partition_id (the cycle's scope).
   *
   * @param {{
   *   envelopes: Array<{
   *     key:string, windowStart:number, windowEnd:number, windowKey:string,
   *     value:any, operatorTag?:string, gracePeriodMs?:number
   *   }>,
   *   loadedState: Map<string, any>,        // state-key -> stored accumulator
   *   streamTimeMs?: number,                 // partition clock (max createdAt of batch)
   *   operatorTag?: string,                  // window operator's structural tag
   *   gracePeriodMs?: number,                // accept events for windowEnd + grace
   *   ignoreUnowned?: boolean                // skip seeded rows whose tag doesn't match
   * }} ctx
   * @returns {Promise<{
   *   stateOps: Array<{type:'upsert'|'delete',key:string,value?:any}>,
   *   emits: Array<{key:string, windowStart:number, windowKey:string, value:any}>
   * }>}
   *
   * Window-close semantics
   * ----------------------
   * A window is closed when the partition's stream time has advanced past
   * its windowEnd plus the grace period:
   *
   *   closed = (windowEnd + gracePeriodMs) <= effectiveStreamTime
   *
   * The caller seeds `loadedState` with ALL open windows for this partition
   * (Runner loads via /streams/v1/state/get with keys=[]) so windows which
   * received no traffic in this batch can still be detected as closed and
   * flushed.
   *
   * State key shape (v0.2+): `${operatorTag}\u001f${windowKey}\u001f${userKey}`.
   * Seeded state rows whose operatorTag doesn't match this reducer's are
   * left untouched (they belong to a different window operator in the same
   * partition shard, or are reserved internal keys like `__wm__`).
   */
  async run({ envelopes, loadedState, streamTimeMs, operatorTag = '', gracePeriodMs = 0, ignoreUnowned = true }) {
    // accumulators[stateKey] = { key, windowStart, windowEnd, windowKey, acc, touched, seeded }
    const accumulators = new Map()

    // Seed accumulators with every loaded state row owned by THIS operator.
    // Rows owned by other operators (or internal keys like __wm__) are
    // skipped — they're not ours to close or upsert.
    for (const [stateKey, value] of loadedState.entries()) {
      const parts = parseStateKey(stateKey)
      if (!parts) continue
      // Skip reserved internal keys.
      if (parts.operatorTag.startsWith('__')) continue
      // Skip rows owned by a different operator.
      if (ignoreUnowned && operatorTag && parts.operatorTag !== operatorTag) continue

      const ws = value && typeof value.windowStart === 'number'
        ? value.windowStart
        : Date.parse(parts.windowKey) || 0
      const we = value && typeof value.windowEnd === 'number'
        ? value.windowEnd
        : ws
      accumulators.set(stateKey, {
        key: parts.userKey,
        windowStart: ws,
        windowEnd: we,
        windowKey: parts.windowKey,
        acc: value && value.acc !== undefined ? value.acc : value,
        touched: false,
        seeded: true
      })
    }

    // Apply this batch's envelopes onto matching accumulators.
    let batchMaxWindowStart = -Infinity
    for (const env of envelopes) {
      const tag = env.operatorTag || operatorTag || ''
      const stateKey = stateKeyFor(tag, env.windowKey, env.key)
      let entry = accumulators.get(stateKey)
      if (!entry) {
        entry = {
          key: env.key,
          windowStart: env.windowStart,
          windowEnd: env.windowEnd,
          windowKey: env.windowKey,
          acc: this._initial(),
          touched: false,
          seeded: false
        }
        accumulators.set(stateKey, entry)
      }
      entry.acc = await this.fn(entry.acc, env.value !== undefined ? env.value : env.msg)
      entry.touched = true
      if (env.windowStart > batchMaxWindowStart) batchMaxWindowStart = env.windowStart
    }

    // Stream time = caller-provided clock OR the latest batch window's
    // start as a conservative fallback (keeps the latest window open).
    const effectiveStreamTime = typeof streamTimeMs === 'number' && streamTimeMs > -Infinity
      ? streamTimeMs
      : batchMaxWindowStart

    const stateOps = []
    const emits = []
    for (const [stateKey, entry] of accumulators.entries()) {
      const closed = (entry.windowEnd + gracePeriodMs) <= effectiveStreamTime
      if (closed) {
        emits.push({
          key: entry.key,
          windowStart: entry.windowStart,
          windowEnd: entry.windowEnd,
          windowKey: entry.windowKey,
          value: entry.acc
        })
        if (entry.seeded) {
          stateOps.push({ type: 'delete', key: stateKey })
        }
      } else if (entry.touched) {
        stateOps.push({
          type: 'upsert',
          key: stateKey,
          value: { acc: entry.acc, windowStart: entry.windowStart, windowEnd: entry.windowEnd }
        })
      }
    }
    return { stateOps, emits }
  }

  _initial() {
    if (this.initial === undefined) return undefined
    // Deep clone so each window starts fresh
    if (this.initial === null || typeof this.initial !== 'object') return this.initial
    return JSON.parse(JSON.stringify(this.initial))
  }
}

/**
 * Build a state key for a windowed reducer.
 *
 * Shape (v0.2+):
 *   `${operatorTag}\u001f${windowKey}\u001f${userKey}`
 *
 * The `operatorTag` (e.g. "tumb:60", "slide:60:10", "sess:30", "cron:minute")
 * lets multiple window operators within one query coexist in the same
 * `queen_streams.state` table without colliding, and lets the idle-flush
 * scan filter to a single operator's keys via prefix LIKE matching.
 *
 * Reserved key prefix: state keys starting with "__" are internal (e.g.
 * "__wm__" for per-partition watermarks). User code should never write
 * keys starting with "__".
 */
export function stateKeyFor(operatorTag, windowKey, userKey) {
  // Backwards-compat shim: if called with two arguments (the v0.1 shape),
  // route through with a synthetic "legacy" tag so existing tests keep
  // working until they explicitly pass an operatorTag.
  if (userKey === undefined) {
    return `${operatorTag}\u001f${windowKey}`
  }
  return `${operatorTag}\u001f${windowKey}\u001f${userKey}`
}

/**
 * Decompose a state key into its parts. Returns null if the key doesn't
 * match the expected shape.
 */
export function parseStateKey(stateKey) {
  if (typeof stateKey !== 'string') return null
  const parts = stateKey.split('\u001f')
  if (parts.length === 3) {
    return { operatorTag: parts[0], windowKey: parts[1], userKey: parts[2] }
  }
  if (parts.length === 2) {
    // Legacy v0.1 shape: ${windowKey}\u001f${userKey}
    return { operatorTag: '', windowKey: parts[0], userKey: parts[1] }
  }
  return null
}
