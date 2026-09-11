// The single error shape every API failure arrives as.
//
// The proxy answers `{error, code}` with a meaningful status on every non-2xx
// (proxy/src/errors.rs), and those three facts are the only way a view
// can tell "you may not" (403) from "not on this cell" (404 route_blocked)
// from "slow down" (429) from "the broker is gone" (0/502). Nothing may drop
// them: a bare `new Error(message)` makes every one of those look identical.

export const CODE_UNAUTHORIZED = 'unauthorized'
export const CODE_FORBIDDEN = 'forbidden'
export const CODE_ROUTE_BLOCKED = 'route_blocked'

// THE 403s THAT ARE NOT ABOUT THE PRINCIPAL (proxy/src/errors.rs `CODE_*`).
// Five of the proxy's codes arrive on the same status as a role refusal and
// mean something no change of role could fix: the cluster is over a cap, on a
// billing hold, suspended, or on a plan that does not carry the feature. They
// are named here rather than spelled at the call sites because three describers
// used to keep their own copy of the list and drift apart.
export const CODE_QUOTA_EXCEEDED = 'quota_exceeded'
export const CODE_STORAGE_QUOTA = 'storage_quota_exceeded'
export const CODE_PUSH_BLOCKED = 'push_blocked'
export const CODE_SUSPENDED = 'cluster_suspended'
export const CODE_FEATURE_GATED = 'feature_gated'

export class ApiError extends Error {
  constructor(message, { status = 0, code = null, retryAfter = null, body = null, path = null } = {}) {
    super(message || 'Request failed')
    this.name = 'ApiError'
    // 0 means the response never arrived (DNS, refused, timeout, CORS).
    this.status = status
    this.code = code
    this.retryAfter = retryAfter
    this.body = body
    this.path = path
  }

  /** No response at all — the proxy or the network is down. */
  get isOffline() { return this.status === 0 }
  get isForbidden() { return this.status === 403 }
  /** Route the proxy refuses to expose here (operator-only, or blocked outright). */
  get isBlocked() { return this.status === 404 && this.code === CODE_ROUTE_BLOCKED }
  get isNotFound() { return this.status === 404 && this.code !== CODE_ROUTE_BLOCKED }
  get isRateLimited() { return this.status === 429 }
  get isServerFault() { return this.status >= 500 }
}

/**
 * The refusal code an error carries, from whichever envelope it arrived in.
 *
 * WHERE THE CODE LIVES IS NOT ONE PLACE, and this reader exists to hide that.
 * The two components answer with two different envelopes:
 *
 *   proxy/src/errors.rs   {"error": "<human message>", "code": "<code>"}
 *   the broker's kv and   {"error": "<code>", "reason": …, "detail": …}
 *   timer handlers        (handlers/timers.rs err(), handlers/kv.rs err())
 *
 * api/httpClient.js reads `code` off the envelope, so a broker refusal — which
 * is what arrives whenever the proxy streams the broker's own answer through —
 * lands with `ApiError.code === null` and the code sitting in `body.error`.
 * Branching on `err.code` alone therefore works through the proxy's own gates
 * and silently fails on every refusal the broker minted itself.
 *
 * `body.error` is only ever consulted against a CLOSED SET of codes by the
 * callers below, so the proxy's human message in that same field can never be
 * mistaken for one.
 */
export function refusalCode(err) {
  if (!err) return null
  if (typeof err.code === 'string' && err.code) return err.code
  const fromBody = err.body && typeof err.body === 'object' ? err.body.error : null
  return typeof fromBody === 'string' && fromBody ? fromBody : null
}

/** The plan refusals that name a LIMIT: the server's own sentence carries it. */
const PLAN_LIMIT_CODES = new Set([CODE_QUOTA_EXCEEDED, CODE_STORAGE_QUOTA, CODE_PUSH_BLOCKED])

/** The proxy sends `cluster_suspended`; `suspended` is accepted for the day a
 *  component sends the bare status word (console.rs renders it as that). */
const SUSPENDED_CODES = new Set([CODE_SUSPENDED, 'suspended'])

/**
 * The 503 codes that mean "the cell is not serving this OPTIONAL FAMILY right
 * now" — KV, timers, ephemeral.
 *
 * TWO MINTERS, AND THE SECOND IS THE ONE THE PAGES ACTUALLY MEET:
 *
 *  · the switch ladder (server/src/switches.rs) answers `*_disabled` for an
 *    operator's runtime pause and `kv_unavailable` / `ephemeral_unavailable`
 *    for a full cell (Verdict::NoRoom);
 *  · each handler answers its OWN `unavailable()` for a pool exhaustion, a
 *    statement timeout or a dead connection — `kv_unavailable`
 *    (handlers/kv.rs) and `timers_unavailable` (handlers/timers.rs), which is
 *    not the family code the ladder would have used.
 *
 * On the four timer routes this dashboard calls, the second minter is the ONLY
 * one that can fire: switches.rs pins rung 1 to `true` for `Surface::TimerRead`
 * / `TimerCancel` (§9.6 — a read that answered 503 would stop a caller finding
 * out whether a timer it can no longer cancel is still pending) and rungs 2-3
 * hand both of them `Verdict::Allow` (quota.rs `check_timers_cancel`, and
 * `Verdict::Allow` outright for `TimerRead`). So `timers_disabled` is reachable
 * only on `POST /api/v1/timers`, which no view calls, and `timers_unavailable`
 * is what a broken cell actually sends here.
 *
 * All of them are 503 with `Retry-After: 1` on the HTTP routes. The STATUS is
 * deliberately not matched: inside /api/v1/transaction the same rung renders
 * 403 instead (Origin::Wire), and the CODE is the fact either way.
 */
const PAUSED_FAMILY_CODES = new Set([
  'kv_disabled',
  'timers_disabled',
  'ephemeral_disabled',
  'kv_unavailable',
  'timers_unavailable',
  'ephemeral_unavailable',
])

/**
 * 403 `feature_gated`: the plan does not carry this family (proxy/src/gateway.rs
 * `plan_gates`, or the broker's own `Verdict::NotGranted` when no
 * `queen.kv_quota` row grants it). NEVER inferred from a bare 403 — a 403
 * without that code is a permission answer and belongs to the identity store's
 * `can()`, not to a feature gate.
 */
export function isFeatureGatedError(err) {
  return !!err && err.status === 403 && refusalCode(err) === CODE_FEATURE_GATED
}

/** 503 (see the note above on the status) with one of the family switch codes. */
export function isPausedFamilyError(err) {
  return !!err && PAUSED_FAMILY_CODES.has(refusalCode(err))
}

/**
 * The three answers that mean "this route does not exist here": a plain 404,
 * the proxy's route_blocked, and the SPA fallback answering an API path
 * (httpClient turns that 200 text/html into not_an_api_response). Each is a
 * fact about the cell, not a fault of the caller, and asking again cannot
 * change it. Probes (httpClient `probe: true`) and the route-support store
 * both key off this one predicate.
 */
export function isMissingRouteError(err) {
  return !!err && (
    err.status === 404 || err.code === CODE_ROUTE_BLOCKED || err.code === 'not_an_api_response'
  )
}

/**
 * A STABLE verdict about an optional family — "not in this cluster's plan", or
 * "this cell is not serving it right now" — as opposed to a failure.
 *
 * Same class of answer as `isMissingRouteError`: a state the page renders as
 * one quiet card, with no poll and no retry. It is separate from that predicate
 * because the route-support store remembers "absent" for the whole cluster
 * epoch and these two are not epoch-stable in the same way (a plan can be
 * changed, a switch flipped). composables/useGatedVerdict.js turns the pair
 * into the 'gated' / 'paused' verdicts and owns the copy; httpClient uses this
 * predicate to keep a caller that renders them off the global toast surface.
 */
export function isGatedVerdictError(err) {
  return isFeatureGatedError(err) || isPausedFamilyError(err)
}

/**
 * One sentence a human can act on. Views use this for inline error states so
 * the wording of a 403 or a 429 is identical everywhere in the product.
 *
 * THE 403 IS BRANCHED ON THE CODE BEFORE THE ROLE SENTENCE. "Not permitted for
 * your role on this cluster" was once the answer to every 403, and it is the
 * wrong advice — sometimes actively harmful — for the majority of them: a
 * retention window over the plan's ceiling, a tenant at its queue cap, a
 * storage block, a billing hold, a suspended cluster and an ungranted feature
 * are all 403s that no change of role would fix, and the person reading the
 * toast would go and ask for a permission they already have. Observed as an
 * ADMIN: `/configure` refused 403 `quota_exceeded` ("retentionSeconds of
 * 1209600s exceeds the plan's max_retention_seconds (604800s)") and the toast
 * said the role was wrong.
 *
 * The server's own sentence is repeated for the limit codes because it NAMES
 * THE CAP that fired — which cap, and by how much — and nothing this function
 * could invent about a limit it cannot read would be more useful.
 */
export function describeApiError(err) {
  if (!(err instanceof ApiError)) return err?.message || 'Something went wrong'
  if (err.isOffline) return 'Cannot reach the API — the proxy or the broker is unreachable'
  if (err.isForbidden) return describeForbidden(err)
  if (err.isBlocked) return 'Not available on this cell'
  if (err.isRateLimited) {
    return err.retryAfter
      ? `Rate limited — retry in ${err.retryAfter}s`
      : 'Rate limited by the plan on this cluster'
  }
  if (err.isServerFault) {
    // A 5xx carrying one of the family switch codes is not a broken cell in
    // general — it is this OPTIONAL FAMILY not being served, which is the same
    // fact the quiet cards render. The pages that own a card for it never reach
    // this line; the ones that report the failure inline (views/Ephemeral.vue's
    // banner) would otherwise say "Server error (HTTP 503)" about a cell whose
    // durable queues are perfectly healthy.
    if (isPausedFamilyError(err)) return 'Not being served on this cell right now'
    return `Server error (HTTP ${err.status})`
  }
  return err.message
}

function describeForbidden(err) {
  const code = refusalCode(err)
  if (PLAN_LIMIT_CODES.has(code)) {
    const said = serverSentence(err)
    return said
      ? `The plan on this cluster refused it: ${said}`
      : 'The plan on this cluster refused it'
  }
  if (code === CODE_FEATURE_GATED) return 'Not in this cluster’s plan'
  if (SUSPENDED_CODES.has(code)) return 'This cluster is suspended'
  // The role sentence is for the refusals that ARE about the principal: the
  // proxy's `forbidden` (acting.rs, auth.rs, console.rs) and a 403 that carried
  // no code at all. Any other code is a refusal this dashboard has not been
  // taught to word, so the component's own sentence is repeated rather than
  // replaced by a claim about the role that may well be false.
  if (!code || code === CODE_FORBIDDEN) return 'Not permitted for your role on this cluster'
  return serverSentence(err) || 'Not permitted for your role on this cluster'
}

/**
 * The server's own sentence, or null when what arrived is not one.
 *
 * Exported for the call site that repeats a component's own wording for a
 * refusal this file does not word itself (composables/usePushVerdict.js's 413),
 * so the "is this a sentence or is it a code" test is made in one place.
 *
 * An ApiError ALWAYS carries some message: the constructor falls back to
 * 'Request failed' and httpClient falls back to the envelope's `code` when it
 * carried no `error` — which is exactly what a broker envelope looks like,
 * since its `error` field IS the code. None of those is a sentence, and none
 * of them names a cap.
 */
export function serverSentence(err) {
  const text = typeof err?.message === 'string' ? err.message.trim() : ''
  if (!text || text === 'Request failed' || text === 'Network error') return null
  return text === refusalCode(err) ? null : text
}
