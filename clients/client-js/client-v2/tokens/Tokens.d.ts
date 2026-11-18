/**
 * Client tokens (POST /api/v1/tokens): a short-lived credential for ONE
 * entity (a device, a browser user), minted from a scoped key.
 *
 *     // backend, with the scoped key `devices`
 *     const queen = new Queen({ url, bearerToken: process.env.QUEEN_DEVICES_KEY })
 *     const { token, expiresAt } = await queen.tokens.mint({ sub: deviceId, ttlSeconds: 3600 })
 *
 *     // device: a function, so the client fetches a new token before this
 *     // one expires and after a 401
 *     const queen = new Queen({ url, bearerToken: () => fetchTokenFromMyBackend() })
 */

import type { JsonObject, QueenHttpClient } from '../types.js'

/** Options of `tokens.mint()`. */
export interface MintTokenOptions {
  /** The entity: 1 to 128 letters, digits and `. _ - @ +`. */
  sub: string
  /** Lifetime; default 3600, at most the key's ceiling. */
  ttlSeconds?: number | null | undefined
  /** The scoped key to mint for, by id or name: only when this client holds an `admin` key. */
  key?: string | null | undefined
  /** Grants that narrow the key's for this token. */
  allow?: JsonObject[] | null | undefined
}

/** A minted token. */
export interface MintedToken {
  token: string
  sub: string
  key: string
  expiresAt: string
  expiresIn: number
}

export declare class Tokens {
  constructor(httpClient: QueenHttpClient)
  /** Mint a token for `sub`. */
  mint(options: MintTokenOptions): Promise<MintedToken>
}
