/**
 * The credential a client presents (`bearerToken`): a fixed string (an API
 * key, or a token it was handed) or a FUNCTION that fetches one, typically a
 * client token the application's own backend minted for this device or user
 * (`queen.tokens.mint()` there).
 *
 * With a function, the client calls it before its first request and keeps
 * the token it answers. It calls it again shortly before a client token
 * expires, and after an answer of 401 (then sends that request once more).
 * One call at a time.
 */

import type { TokenFunction } from '../types.js'

/** How long before a client token's `exp` it is fetched again. */
export declare const REFRESH_AHEAD_MS: number

/** What every client token starts with. */
export declare const CLIENT_TOKEN_PREFIX: 'qt_'

export declare class TokenSource {
  constructor(bearerToken?: string | TokenFunction | null)
  /** Is there any credential at all? */
  get configured(): boolean
  /** Can a new token be fetched (a function was given)? */
  get refreshable(): boolean
  /**
   * The token to send. `failed`: the token a request was just refused with
   * (401): a new one is fetched unless another request already replaced it.
   */
  get(options?: { failed?: string | null | undefined }): Promise<string | null>
}

/**
 * A client token's expiry in milliseconds, read from its `exp` claim; null
 * for anything else (an API key, an opaque token). Read, never verified.
 */
export declare function expiresAtMs(token: string): number | null
