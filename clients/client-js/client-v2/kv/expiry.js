/**
 * Expiry sugar for the KV surface, and the ONE duration parser this SDK has.
 *
 * PLAN_KV_TIMERS.md §20.1 ratified `ttlSeconds` as the wire's unit: the server
 * speaks seconds everywhere (`dedup_window_seconds`, `lease_seconds`,
 * `retention_seconds`), so the product keeps ONE duration convention and the
 * SDKs do the converting. `ttlMillis` does not exist in any client and must not
 * be added -- it would reintroduce, through the service entrance, the double
 * convention that decision removed.
 *
 * §20.6 is the other half of the rule, and it is what this file's parser
 * serves twice: DURATIONS THAT CAN BE SUB-SECOND ARE IN MILLISECONDS, THE ONES
 * THAT CANNOT ARE IN SECONDS. A 250 ms retry backoff is a real and central use
 * of timers, so their wire is `delayMs`; a sub-second TTL is not a real use for
 * anybody, so the KV wire is `ttlSeconds`. The parser below returns
 * MILLISECONDS -- the finer of the two -- and each caller converts.
 *
 * NOT EXPORTED FROM THE BARREL, deliberately (§10.4). The client's stated
 * convention is "a number with the unit in its name", and this is its first
 * string duration. Confining the parser to this file is what keeps it from
 * becoming a general-purpose utility that then has to accept every format
 * anybody ever wrote.
 *
 * ROUNDING IS ALWAYS UP. A TTL rounded down can expire a marker BEFORE the
 * window it was supposed to cover, which is the failure this feature exists to
 * prevent; rounding up costs at most a second of retention.
 */

const UNIT_MS = {
  ms: 1,
  s: 1000,
  m: 60 * 1000,
  h: 60 * 60 * 1000,
  d: 24 * 60 * 60 * 1000
}

// One or more <number><unit> pairs: '30s', '1h30m', '250ms'. Anchored, so a
// typo is an error and never a silent partial parse ('1hour' does not become
// one hour).
const DURATION_RE = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/

/**
 * Parse a duration STRING into milliseconds. Numbers are refused on purpose:
 * `{ ttl: 5000 }` cannot be read as seconds or milliseconds without guessing,
 * and guessing here would be a factor-of-1000 bug in somebody's retention.
 * The numeric spellings are the ones with the unit in the name --
 * `ttlSeconds` and `delayMs`.
 */
export function parseDurationMs(value) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(
      `duration must be a string with a unit, e.g. '250ms', '30s', '15m', '24h', '7d' — got ${JSON.stringify(value)}. ` +
      'For a plain number use ttlSeconds (KV) or delayMs (timers), which carry their unit in the name.'
    )
  }
  const parts = value.trim().toLowerCase().match(/\d+(?:\.\d+)?[a-z]+/g)
  const joined = parts ? parts.join('') : ''
  if (!parts || joined !== value.trim().toLowerCase()) {
    throw new Error(`invalid duration '${value}': expected <number><unit> parts, e.g. '30s' or '1h30m'`)
  }
  let ms = 0
  for (const part of parts) {
    const m = DURATION_RE.exec(part)
    if (!m) {
      throw new Error(`invalid duration '${value}': unknown unit in '${part}' (use ms, s, m, h, d)`)
    }
    ms += Number(m[1]) * UNIT_MS[m[2]]
  }
  if (!(ms > 0)) {
    throw new Error(`invalid duration '${value}': must be greater than zero`)
  }
  return ms
}

/** Milliseconds since the epoch for a Date, an ISO string or an epoch number. */
function instantMs(value) {
  if (value instanceof Date) {
    const t = value.getTime()
    if (Number.isNaN(t)) throw new Error('until: invalid Date')
    return t
  }
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string') {
    const t = Date.parse(value)
    if (Number.isNaN(t)) throw new Error(`until: '${value}' is not a parseable date`)
    return t
  }
  throw new Error(`until must be a Date, an ISO 8601 string or an epoch-milliseconds number — got ${typeof value}`)
}

/**
 * Resolve the expiry fields of one KV write into the CANONICAL wire shape.
 *
 * Returns `{ ttlSeconds }`, `{ forever: true }`, or `{}`.
 *
 * The empty answer is deliberate and is the reason this function validates so
 * little: §5.1's rule -- exactly one of `ttlSeconds` and `forever:true`, zero
 * or two being the same error -- lives in `kv_apply_v1`, so that all seven
 * clients AND the embedded broker (which never passes through an HTTP handler)
 * inherit it without a line of their own. Re-implementing it here would give
 * the product two places that can disagree about when a key dies. What IS
 * checked here is only what the server cannot see, because it is sugar that
 * never reaches it: two spellings of the same field, a `forever` that is not
 * `true`, and an `until` already in the past.
 *
 * `nowMs` is a parameter so the conversion happens at SEND time. In a
 * transaction the ops are built when the caller writes them and sent at
 * `commit()`, and an `until` frozen at build time would ship a TTL that is
 * already stale by however long the bundle took to assemble.
 */
export function resolveExpiry(opts = {}, nowMs = Date.now()) {
  const spellings = ['ttlSeconds', 'ttl', 'until'].filter(k => opts[k] !== undefined)
  if (spellings.length > 1) {
    throw new Error(
      `expiry declared twice (${spellings.join(' and ')}): ttlSeconds is the canonical field, ` +
      '`ttl` is a duration string and `until` is an instant — pick one'
    )
  }

  const out = {}

  if (opts.forever !== undefined) {
    if (opts.forever !== true) {
      throw new Error('forever must be exactly true; there is no forever:false — omit it and declare a TTL')
    }
    out.forever = true
  }

  if (opts.ttlSeconds !== undefined) {
    out.ttlSeconds = opts.ttlSeconds
  } else if (opts.ttl !== undefined) {
    out.ttlSeconds = Math.ceil(parseDurationMs(opts.ttl) / 1000)
  } else if (opts.until !== undefined) {
    const deltaMs = instantMs(opts.until) - nowMs
    const seconds = Math.ceil(deltaMs / 1000)
    if (seconds <= 0) {
      throw new Error(
        `until is already in the past (${Math.round(deltaMs)}ms from now): a key written with a dead ` +
        'TTL would be refused by the broker, and a key written with none would be immortal'
      )
    }
    out.ttlSeconds = seconds
  }

  // Both declared: forwarded as-is on purpose, so the broker gives its own
  // `kv_expiry_not_specified` verdict and there is one rule, in one place.
  return out
}
