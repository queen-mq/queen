/**
 * The credential a client presents (`bearerToken`): a fixed string — an API
 * key, or a token it was handed — or a FUNCTION that fetches one, typically a
 * client token the application's own backend minted for this device or user
 * (`queen.tokens.mint()` there, POST /api/v1/tokens).
 *
 * With a function, the client calls it before its first request and keeps
 * the token it answers. It calls it again:
 *   - shortly before a client token expires (its `exp`, read from the token
 *     itself, REFRESH_AHEAD_MS ahead), so a request rarely meets an expired
 *     token at all;
 *   - after an answer of 401, and then sends that request once more with the
 *     new token. A second 401 is the caller's.
 * One call at a time: requests that need a token while one is being fetched
 * wait for that call, and a 401 met by many requests at once refreshes once.
 */

/** How long before a client token's `exp` it is fetched again. */
export const REFRESH_AHEAD_MS = 30_000

/** What every client token starts with (the proxy's `clients::TOKEN_PREFIX`). */
export const CLIENT_TOKEN_PREFIX = 'qt_'

export class TokenSource {
  #fixed = null
  #fetch = null
  #token = null
  #expiresAtMs = null
  #pending = null

  /** @param {string|(() => string|Promise<string>)|null} bearerToken */
  constructor (bearerToken) {
    if (typeof bearerToken === 'function') {
      this.#fetch = bearerToken
    } else if (bearerToken != null && bearerToken !== '') {
      this.#fixed = String(bearerToken)
    }
  }

  /** Is there any credential at all? */
  get configured () {
    return this.#fixed !== null || this.#fetch !== null
  }

  /** Can a new token be fetched (a function was given)? */
  get refreshable () {
    return this.#fetch !== null
  }

  /**
   * The token to send. `failed`: the token a request was just refused with
   * (401): a new one is fetched unless another request already replaced it.
   * @returns {Promise<string|null>}
   */
  async get ({ failed = null } = {}) {
    if (this.#fixed !== null) return this.#fixed
    if (this.#fetch === null) return null
    const replaced = failed !== null && this.#token !== null && this.#token !== failed
    if (replaced) return this.#token
    if (failed === null && this.#token !== null && !this.#expiring()) return this.#token
    if (!this.#pending) {
      this.#pending = (async () => {
        let token
        try {
          token = await this.#fetch()
        } catch (cause) {
          throw tokenSourceError(`the token function failed: ${cause?.message ?? cause}`, cause)
        }
        if (typeof token !== 'string' || token.trim() === '') {
          throw tokenSourceError('the token function must resolve to a non-empty string')
        }
        this.#token = token.trim()
        this.#expiresAtMs = expiresAtMs(this.#token)
        return this.#token
      })().finally(() => { this.#pending = null })
    }
    return this.#pending
  }

  #expiring () {
    return this.#expiresAtMs !== null && this.#expiresAtMs - Date.now() < REFRESH_AHEAD_MS
  }
}

/**
 * The error of a request that never left because its token function failed.
 * It is no fault of the broker's: `tokenSource: true` tells the HTTP client
 * not to retry it, fail it over, or mark the node unhealthy.
 */
function tokenSourceError (message, cause) {
  const error = new Error(`token source: ${message}`, cause === undefined ? undefined : { cause })
  error.tokenSource = true
  return error
}

/**
 * A client token's expiry in milliseconds, read from its `exp` claim; null
 * for anything else (an API key, an opaque token). Read, never verified: the
 * broker is the one that verifies.
 * @param {string} token
 * @returns {number|null}
 */
export function expiresAtMs (token) {
  if (typeof token !== 'string' || !token.startsWith(CLIENT_TOKEN_PREFIX)) return null
  const parts = token.slice(CLIENT_TOKEN_PREFIX.length).split('.')
  if (parts.length !== 3) return null
  try {
    let b64 = parts[1].replace(/-/g, '+').replace(/_/g, '/')
    while (b64.length % 4) b64 += '='
    const claims = JSON.parse(atob(b64))
    return Number.isFinite(claims.exp) ? claims.exp * 1000 : null
  } catch {
    return null
  }
}
