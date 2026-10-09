// Locks page rules — PURE functions, no Vue, no DOM (the useKvView.js and
// useTimers.js convention), so `npm test` can assert them without a browser.
//
// A LOCK IS A KV ROW, AND THIS PAGE READS IT AS ONE. A permit lives in the
// namespace `queen-locks` under the key `<name>#<slot>`, with the holder's
// owner as its value and the lifetime as its TTL (server/src/locks.rs). So the
// page needs no route of its own: it is the console's KV listing, asked for
// one namespace, and everything below is how a row of that listing reads as a
// permit. The page can therefore be opened by a Viewer, which the locks route
// itself (`POST /api/v1/locks`, read-write even for a `get`) could not be.
//
// Four of the rules are the ones that can be wrong without anybody noticing:
//
//  1. ONLY LIVE PERMITS. `includeExpired` is sent FALSE, the opposite of the
//     KV page: there an expired row is still an occupied one and the counts
//     beside it include it; here the question is "who holds this now", and a
//     permit past its expiry is held by nobody. The broker decides which rows
//     are live, against its own clock, never this browser's.
//
//  2. THE SLOT IS THE LAST `#`. A name cannot contain one (the broker refuses
//     it), so `<name>#<slot>` splits in exactly one place. A key that does not
//     end in the decimal the broker writes is not a permit: somebody put it
//     there through the KV routes, and the page shows it as what it is.
//
//  3. `since` IS THE ACQUIRE, `updatedAt` THE LAST RENEWAL. The row has no
//     creation time, so the broker keeps the acquire's in the value from the
//     first renewal on (`value.since`). A permit never renewed was taken when
//     it was last written.
//
//  4. THE TOKEN IS THE ROW'S VERSION, and it names ONE lease period: every
//     renewal changes it. The release call this page prints carries the token
//     of the row on screen, so a holder that renewed since is left alone.
import { parseBrokerInstant } from './useTimers.js'

/** The namespace every permit lives in (server/src/locks.rs NAMESPACE). */
export const LOCKS_NAMESPACE = 'queen-locks'

/**
 * The `POST /api/v1/resources/kv/list` body for one page of permits.
 *
 * `prefix` is the start of a lock's NAME. Optional fields are omitted rather
 * than sent empty, as in useKvView.kvListBody.
 */
export function locksListBody({ prefix = '', after = null, limit = null } = {}) {
  const body = { namespace: LOCKS_NAMESPACE, includeExpired: false }
  if (typeof prefix === 'string' && prefix !== '') body.prefix = prefix
  if (typeof after === 'string' && after !== '') body.after = after
  if (Number.isFinite(limit)) body.limit = limit
  return body
}

/**
 * `<name>#<slot>` → `{name, slot}`, or null for a key that is not a permit's.
 * The slot is the plain decimal the broker writes: `#07` and `#+7` are not it.
 */
export function splitPermitKey(key) {
  if (typeof key !== 'string') return null
  const at = key.lastIndexOf('#')
  if (at <= 0) return null
  const digits = key.slice(at + 1)
  if (!/^(0|[1-9][0-9]{0,3})$/.test(digits)) return null
  return { name: key.slice(0, at), slot: Number(digits) }
}

/**
 * One row of the KV listing as a permit.
 *
 * `foreign: true` marks a row that is in the namespace and is not a permit the
 * broker wrote: a key without a slot, or a value with no owner field at all.
 * It still occupies its key, so it is listed, as a row and not as a lock.
 */
export function permitOf(row) {
  if (!row || typeof row !== 'object' || typeof row.key !== 'string') return null
  const parts = splitPermitKey(row.key)
  const value = row.value && typeof row.value === 'object' && !Array.isArray(row.value) ? row.value : null
  const owner = value && typeof value.owner === 'string' && value.owner !== '' ? value.owner : null
  const since = value && typeof value.since === 'string' && value.since !== '' ? value.since : row.updatedAt
  return {
    key: row.key,
    name: parts ? parts.name : row.key,
    slot: parts ? parts.slot : null,
    owner,
    token: Number.isFinite(row.version) ? row.version : null,
    since: since ?? null,
    renewedAt: row.updatedAt ?? null,
    expiresAt: row.expiresAt ?? null,
    foreign: !parts || !value || !('owner' in value),
  }
}

/**
 * A page of the listing as permits, each told whether its lock is a semaphore
 * as far as this page can see: a slot above 0, or a second permit of the same
 * name on the page. A semaphore with one holder in slot 0 reads as a lock,
 * which is what it is at that moment; the limit is stored nowhere to ask.
 */
export function permitsOf(rows) {
  const permits = (Array.isArray(rows) ? rows : []).map(permitOf).filter(Boolean)
  const count = new Map()
  for (const p of permits) {
    if (p.slot !== null) count.set(p.name, (count.get(p.name) || 0) + 1)
  }
  return permits.map((p) => ({
    ...p,
    shared: p.slot !== null && (p.slot > 0 || count.get(p.name) > 1),
  }))
}

/** Two units at most, the larger first: `47s`, `12m 5s`, `2h 14m`, `3d 4h`. */
export function formatSpan(ms) {
  if (!Number.isFinite(ms) || ms < 0) return '—'
  const sec = Math.floor(ms / 1000)
  if (sec < 60) return `${sec}s`
  const min = Math.floor(sec / 60)
  if (min < 60) return sec % 60 ? `${min}m ${sec % 60}s` : `${min}m`
  const hours = Math.floor(min / 60)
  if (hours < 24) return min % 60 ? `${hours}h ${min % 60}m` : `${hours}h`
  const days = Math.floor(hours / 24)
  return hours % 24 ? `${days}d ${hours % 24}h` : `${days}d`
}

/**
 * How long the holder has had the permit, against `now` (the load instant, so
 * a whole page is rendered against the one moment its stamp names).
 */
export function formatHeldFor(since, now = Date.now()) {
  const at = parseBrokerInstant(since)
  if (!at) return '—'
  return formatSpan(Math.max(0, now - at.getTime()))
}

/** `8s ago`, for the last renewal. */
export function formatAgo(instant, now = Date.now()) {
  const at = parseBrokerInstant(instant)
  if (!at) return '—'
  const delta = now - at.getTime()
  return delta < 1000 ? 'just now' : `${formatSpan(delta)} ago`
}

/** `in 21s`: how long the permit lasts if nobody renews it. */
export function formatExpiresIn(expiresAt, now = Date.now()) {
  const at = parseBrokerInstant(expiresAt)
  if (!at) return '—'
  const delta = at.getTime() - now
  return delta < 1000 ? 'now' : `in ${formatSpan(delta)}`
}

/**
 * The lifetime the holder asked for at its last acquire or renewal, in
 * seconds: the distance between that write and the expiry it set. Null when
 * either stamp is missing.
 */
export function lifetimeSeconds(permit) {
  const from = parseBrokerInstant(permit?.renewedAt)
  const to = parseBrokerInstant(permit?.expiresAt)
  if (!from || !to) return null
  const seconds = Math.round((to.getTime() - from.getTime()) / 1000)
  return seconds > 0 ? seconds : null
}

/**
 * The KV operation that holds while this permit is still this holder's: what
 * `.guard(lock)` puts in a transaction (server/src/locks.rs guard()).
 */
export function guardOf(permit) {
  if (!permit || permit.token === null) return null
  return { op: 'check', ns: LOCKS_NAMESPACE, key: permit.key, expect: permit.token, required: true }
}

/**
 * The body that releases THIS lease period, for an operator to send by hand.
 *
 * It carries the token on screen, so it removes the permit only if the holder
 * has not renewed since the page was read: a live holder answers
 * `released: false, reason: "lost"` and keeps its lock. Null for a row that is
 * not a permit, which the locks route cannot address.
 */
export function releaseBody(permit) {
  if (!permit || permit.foreign || permit.slot === null || permit.token === null) return null
  const op = { op: 'release', name: permit.name, token: permit.token }
  if (permit.slot > 0) op.slot = permit.slot
  return { operations: [op] }
}

/** `releaseBody` as the curl line the drawer prints. */
export function releaseCommand(permit, origin = '') {
  const body = releaseBody(permit)
  if (!body) return null
  // Single quotes end the shell string; a lock name may contain one.
  const json = JSON.stringify(body).replace(/'/g, `'\\''`)
  return `curl -s -X POST ${origin}/api/v1/locks -H 'content-type: application/json' -d '${json}'`
}

/** What the foot of the page can say about its own end, or `''`. */
export function describeLocksEnd({ rowCount = 0, truncated = false, prefixed = false } = {}) {
  if (!Number.isFinite(rowCount) || rowCount <= 0 || truncated) return ''
  return prefixed ? 'end of the prefix' : 'every held lock'
}
