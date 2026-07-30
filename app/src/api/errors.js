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
 * One sentence a human can act on. Views use this for inline error states so
 * the wording of a 403 or a 429 is identical everywhere in the product.
 */
export function describeApiError(err) {
  if (!(err instanceof ApiError)) return err?.message || 'Something went wrong'
  if (err.isOffline) return 'Cannot reach the API — the proxy or the broker is unreachable'
  if (err.isForbidden) return 'Not permitted for your role on this cluster'
  if (err.isBlocked) return 'Not available on this cell'
  if (err.isRateLimited) {
    return err.retryAfter
      ? `Rate limited — retry in ${err.retryAfter}s`
      : 'Rate limited by the plan on this cluster'
  }
  if (err.isServerFault) return `Server error (HTTP ${err.status})`
  return err.message
}
