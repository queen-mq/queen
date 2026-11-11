/**
 * Client tokens (POST /api/v1/tokens): a short-lived credential for ONE
 * entity — a device, a browser user — minted from a scoped key.
 *
 * The key is the role: its grants say which queues, partitions, KV keys and
 * timers it reaches, and may name the entity as `{sub}`
 * (`telemetry` / `{sub}`: each device its own partition). The token adds the
 * entity, its `sub`, and never reaches more than its key. It is minted on the
 * application's backend, which holds the key, and handed to the device:
 *
 *     // backend, with the scoped key `devices`
 *     const queen = new Queen({ url, bearerToken: process.env.QUEEN_DEVICES_KEY })
 *     const { token, expiresAt } = await queen.tokens.mint({ sub: deviceId, ttlSeconds: 3600 })
 *
 *     // device: a function, so the client fetches a new token before this
 *     // one expires and after a 401
 *     const queen = new Queen({ url, bearerToken: () => fetchTokenFromMyBackend() })
 *
 * Revoking or narrowing the key reaches every token it minted within about
 * a second; blocking one `sub` refuses that entity's tokens on every node.
 */
export class Tokens {
  #http

  constructor (httpClient) {
    this.#http = httpClient
  }

  /**
   * Mint a token for `sub`.
   *
   * @param {object} options
   * @param {string} options.sub - the entity: 1 to 128 letters, digits and `. _ - @ +`
   * @param {number} [options.ttlSeconds] - lifetime; default 3600, at most the key's ceiling
   * @param {string} [options.key] - the scoped key to mint for, by id or name: only when
   *   this client holds an `admin` key (a scoped key mints for itself)
   * @param {object[]} [options.allow] - grants that narrow the key's for this token
   * @returns {Promise<{token: string, sub: string, key: string, expiresAt: string, expiresIn: number}>}
   */
  async mint ({ sub, ttlSeconds, key, allow } = {}) {
    if (typeof sub !== 'string' || sub === '') {
      throw new Error('tokens.mint: sub is required (the entity the token is for)')
    }
    const body = { sub }
    if (ttlSeconds != null) body.ttlSeconds = ttlSeconds
    if (key != null) body.key = key
    if (allow != null) body.allow = allow
    return this.#http.post('/api/v1/tokens', body)
  }
}
