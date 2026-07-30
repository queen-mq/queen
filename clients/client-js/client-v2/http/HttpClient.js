/**
 * HTTP client with retry, load balancing, and failover support
 */

import * as logger from '../utils/logger.js'

// --------------------------------------------------------------------------
// Host-routed proxy support (queen_proxy selects the tenant cluster from the
// first DNS label of the Host header -- proxy/src/cache.rs
// slug_from_host).
//
// `fetch()` refuses to send a caller-supplied Host header: `host` is on the
// WHATWG forbidden-header list, so undici drops it *silently*. Sending the
// wrong Host is not a cosmetic problem at a proxy -- it either 421s
// (cluster_unknown) or, on a cell with a default cluster, lands the traffic on
// somebody else's cluster. So the Host is never taken from `headers`; it is a
// first-class option (`hostHeader`) that rewrites the request URL's authority
// while the connection stays pinned to the configured address (the same thing
// `curl --resolve` does), and a Host found in `headers` is mapped onto it with
// a loud warning rather than being ignored.
// --------------------------------------------------------------------------

// Warn once per distinct offending value: a second cluster misconfigured the
// same way must not be masked by the first. Bounded so a pathological caller
// can't grow it without limit (past the cap every occurrence warns, which is
// the safe direction).
const WARNED_HOST_TRAPS = new Set()
const WARNED_HOST_TRAPS_CAP = 64

function warnHostTrap(key, message) {
  logger.warn('HttpClient.hostHeader', message)
  if (WARNED_HOST_TRAPS.has(key)) return
  if (WARNED_HOST_TRAPS.size < WARNED_HOST_TRAPS_CAP) WARNED_HOST_TRAPS.add(key)
  // Deliberately not gated behind QUEEN_CLIENT_LOG: a request that reaches the
  // wrong tenant must never be quiet.
  if (typeof console !== 'undefined' && typeof console.warn === 'function') {
    console.warn(`[queen-mq] ${message}`)
  }
}

/**
 * Validate/parse a `hostHeader` value into the pieces we need.
 * Accepts a bare authority: 'acme', 'acme.eu1.queenmq.cloud', 'acme.local:6711',
 * '[::1]:6711'. Rejects anything URL-shaped so a mistake fails at construction
 * instead of routing somewhere unexpected at runtime.
 */
function parseHostOverride(value) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error("hostHeader must be a non-empty string, e.g. hostHeader: 'acme.eu1.queenmq.cloud'")
  }
  const authority = value.trim()
  if (authority.includes('://') || /[\s/\\?#@]/.test(authority)) {
    throw new Error(`hostHeader must be a bare authority ('acme.eu1.queenmq.cloud' or 'acme.local:6711'), not a URL — got '${authority}'`)
  }
  let parsed
  try {
    parsed = new URL(`http://${authority}`)
  } catch {
    throw new Error(`hostHeader is not a valid host[:port] — got '${authority}'`)
  }
  if (!parsed.hostname || parsed.pathname !== '/' || parsed.search || parsed.hash) {
    throw new Error(`hostHeader is not a valid host[:port] — got '${authority}'`)
  }
  return { authority, hostname: parsed.hostname, port: parsed.port }
}

/** Split a user headers object into { hostValue, headers-without-Host }. */
function splitHostHeader(headers) {
  const rest = {}
  let hostValue = null
  for (const [key, value] of Object.entries(headers || {})) {
    if (key.toLowerCase() === 'host') {
      hostValue = typeof value === 'string' ? value.trim() : String(value)
      continue
    }
    rest[key] = value
  }
  return { hostValue: hostValue || null, headers: rest }
}

export class HttpClient {
  #baseUrl
  #loadBalancer
  #timeoutMillis
  #retryAttempts
  #retryDelayMillis
  #enableFailover
  #bearerToken
  #headers
  #retry429
  // Per-client undici Agent (Node only). Owning our own dispatcher is what
  // makes Queen.close() deterministic: global-fetch keep-alive sockets belong
  // to the process-wide dispatcher and can't be released per client, which
  // left the event loop pinned after close(). `undefined` = not yet resolved,
  // `null` = unavailable (browser / import failed) -> fall back to global fetch.
  #dispatcher = undefined
  #destroyed = false
  // Host-routed proxy support: parsed `hostHeader` ({authority, hostname, port})
  // or null, plus one connection-pinned Agent per real backend (`host:port` ->
  // Agent) so load balancing still reaches each backend while every request
  // carries the same virtual Host.
  #hostOverride = null
  #pinnedDispatchers = new Map()

  constructor(options = {}) {
    const {
      baseUrl = null,
      loadBalancer = null,
      timeoutMillis = 30000,
      retryAttempts = 3,
      retryDelayMillis = 1000,
      enableFailover = true,
      bearerToken = null,
      headers = {},
      // Host to advertise, independent of the address we connect to. A
      // queen_proxy deployment resolves the tenant cluster from the Host
      // header's first DNS label, so this is what selects a cluster when the
      // base URL points at a shared address (an IP, a cell endpoint, a test
      // rig). The connection still goes to baseUrl/loadBalancer; only the
      // request's authority (and TLS SNI) is rewritten. Normally unnecessary
      // in production, where each cluster has its own subdomain and the base
      // URL already carries the right Host.
      hostHeader = null,
      // 429 (rate-limited) backoff policy, separate from the 5xx/network
      // retryAttempts above. { maxAttempts, baseMs, capMs }, all optional.
      // maxAttempts defaults to 10 for ordinary requests; long-poll pop
      // calls (marked via the `retryKind: 'pop'` request option) default to
      // unbounded (retry forever, paced by backoff) unless maxAttempts is
      // explicitly set here, in which case it applies to both.
      // See PLAN_QUEEN_PROXY_CLOUD.md §4/§9 (client 429 backoff, blocker B4).
      retry429 = {}
    } = options

    this.#baseUrl = baseUrl
    this.#loadBalancer = loadBalancer
    this.#timeoutMillis = timeoutMillis
    this.#retryAttempts = retryAttempts
    this.#retryDelayMillis = retryDelayMillis
    this.#enableFailover = enableFailover
    this.#bearerToken = bearerToken
    this.#retry429 = retry429 || {}

    // A Host in `headers` can never go out over fetch(); rather than let it be
    // dropped, map it onto the supported mechanism and say so out loud.
    const { hostValue, headers: plainHeaders } = splitHostHeader(headers)
    this.#headers = plainHeaders
    let effectiveHost = hostHeader
    if (hostValue) {
      if (!effectiveHost) {
        effectiveHost = hostValue
        warnHostTrap(`map:${hostValue}`, `headers: { Host: '${hostValue}' } cannot be sent by fetch() (Host is a forbidden header name); it has been mapped onto hostHeader: '${hostValue}'. Set hostHeader directly to silence this warning.`)
      } else if (String(effectiveHost).trim().toLowerCase() !== hostValue.toLowerCase()) {
        warnHostTrap(`conflict:${hostValue}=>${effectiveHost}`, `headers: { Host: '${hostValue}' } is ignored because hostHeader: '${effectiveHost}' is also configured — requests will carry Host: '${effectiveHost}'.`)
      }
    }
    this.#hostOverride = effectiveHost ? parseHostOverride(effectiveHost) : null

    logger.log('HttpClient.constructor', {
      hasLoadBalancer: !!loadBalancer,
      baseUrl: baseUrl || 'load-balanced',
      timeoutMillis,
      retryAttempts,
      enableFailover,
      hasAuth: !!bearerToken,
      customHeaders: Object.keys(this.#headers).length,
      hostHeader: this.#hostOverride ? this.#hostOverride.authority : null,
      retry429: this.#retry429
    })
  }

  /**
   * Resolve the effective 429 retry policy for a request kind.
   * - 'pop': long-poll pop (wait=true). Unbounded attempts by default (the
   *   long-poll loop is meant to keep waiting through transient rate
   *   limiting), unless retry429.maxAttempts was explicitly configured.
   * - anything else (push, admin calls, non-waiting pop, ...): bounded,
   *   defaults to 10 attempts.
   * baseMs/capMs always default to 500/30000 and apply to both kinds.
   */
  #retry429PolicyFor(retryKind) {
    const cfg = this.#retry429 || {}
    const baseMs = cfg.baseMs ?? 500
    const capMs = cfg.capMs ?? 30000
    const maxAttempts = cfg.maxAttempts != null
      ? cfg.maxAttempts
      : (retryKind === 'pop' ? Infinity : 10)
    return { maxAttempts, baseMs, capMs }
  }

  /**
   * Delay before the next 429 retry attempt (milliseconds).
   * Honors Retry-After (seconds) when the server sent one, with ±20%
   * jitter to avoid a synchronized thundering herd; otherwise falls back to
   * exponential backoff (baseMs * 2^attempt, capped at capMs), also
   * jittered ±20% for the same reason.
   */
  #computeRetry429DelayMs(attemptIndex, retryAfterSeconds, baseMs, capMs) {
    const hasRetryAfter = typeof retryAfterSeconds === 'number' && Number.isFinite(retryAfterSeconds) && retryAfterSeconds >= 0
    const baseDelay = hasRetryAfter
      ? retryAfterSeconds * 1000
      : Math.min(capMs, baseMs * Math.pow(2, attemptIndex))
    const jitterMultiplier = 1 + (Math.random() * 0.4 - 0.2) // ±20%
    return Math.max(0, Math.round(baseDelay * jitterMultiplier))
  }

  /**
   * Run a single logical request against one URL, transparently retrying
   * HTTP 429 responses with backoff until the policy for `retryKind` is
   * exhausted (or never, for unbounded pop policies). Any other error
   * (2xx-and-parse success aside) is thrown straight through — 429 is the
   * only status this layer treats as retryable; 5xx/network retry and
   * cross-backend failover are handled by the caller.
   */
  async #executeWithRetry429(url, method, body, requestTimeoutMillis, retryKind) {
    const { maxAttempts, baseMs, capMs } = this.#retry429PolicyFor(retryKind)
    let tries = 0
    // eslint-disable-next-line no-constant-condition
    while (true) {
      tries++
      try {
        return await this.#executeRequest(url, method, body, requestTimeoutMillis)
      } catch (error) {
        if (error.status !== 429) throw error

        if (tries >= maxAttempts) {
          logger.error('HttpClient.retry429', { method, url, error: 'max 429 attempts exhausted', attempts: tries, code: error.code })
          throw error
        }

        const delay = this.#computeRetry429DelayMs(tries - 1, error.retryAfterSeconds, baseMs, capMs)
        logger.warn('HttpClient.retry429', { method, url, attempt: tries, retryKind: retryKind || 'default', nextDelayMs: delay, retryAfterSeconds: error.retryAfterSeconds ?? null, code: error.code ?? null })
        await new Promise(resolve => setTimeout(resolve, delay))
      }
    }
  }

  // Resolve the per-client dispatcher once: undici Agent on Node, null in
  // browsers (global fetch used as before). undici is a regular dependency,
  // but the guarded dynamic import keeps browser bundles working.
  async #getDispatcher() {
    if (this.#dispatcher !== undefined) return this.#dispatcher
    try {
      const { Agent } = await import('undici')
      this.#dispatcher = new Agent({ keepAliveTimeout: 4000, keepAliveMaxTimeout: 30000 })
    } catch {
      this.#dispatcher = null
    }
    return this.#dispatcher
  }

  /**
   * Agent whose connector always dials `realHostname:realPort`, whatever
   * authority the request URL carries — so the URL (and therefore the Host
   * header, and TLS SNI) can name the tenant cluster while the socket still
   * goes to the configured address. One Agent per real backend keeps load
   * balancing and failover intact. Returns null when undici is unavailable
   * (browser) or the client was destroyed; the caller must then refuse to
   * send rather than fall back to a wrong Host.
   */
  #getPinnedDispatcher(realHostname, realPort) {
    const key = `${realHostname}:${realPort}`
    const existing = this.#pinnedDispatchers.get(key)
    if (existing) return existing
    if (this.#destroyed) return Promise.resolve(null)
    // Cache the in-flight promise, not the resolved Agent: concurrent first
    // requests to the same backend must share one Agent, or the extra ones
    // would hold keep-alive sockets that destroy() never sees.
    const pending = this.#createPinnedDispatcher(realHostname, realPort)
    this.#pinnedDispatchers.set(key, pending)
    return pending
  }

  async #createPinnedDispatcher(realHostname, realPort) {
    let undici
    try {
      undici = await import('undici')
    } catch {
      return null
    }

    const { Agent, buildConnector } = undici
    if (typeof buildConnector !== 'function' || typeof Agent !== 'function') return null

    const connector = buildConnector({})
    const servername = this.#hostOverride.hostname
    const connect = (opts, callback) => connector({
      ...opts,
      hostname: realHostname,
      host: realHostname,
      port: realPort,
      // SNI/cert validation follow the advertised host, not the dialed
      // address — same contract as `curl --resolve`.
      servername: opts.servername || servername
    }, callback)

    const agent = new Agent({ keepAliveTimeout: 4000, keepAliveMaxTimeout: 30000, connect })
    // destroy() may have run while the import was in flight; don't leave an
    // Agent behind that nobody will close.
    if (this.#destroyed) {
      try { await agent.destroy() } catch { /* nothing to release */ }
      return null
    }
    return agent
  }

  /**
   * Split a real backend URL into the URL we put on the wire (authority
   * replaced by the configured hostHeader, so fetch emits it as Host) and the
   * address the socket must actually dial.
   */
  #applyHostOverride(url) {
    const real = new URL(url)
    const virtual = new URL(url)
    virtual.hostname = this.#hostOverride.hostname
    virtual.port = this.#hostOverride.port
    return {
      requestUrl: virtual.toString(),
      // undici's own connector receives an already-unbracketed IPv6 literal;
      // we bypass that step, so strip the brackets here.
      realHostname: real.hostname.replace(/^\[(.*)\]$/, '$1'),
      realPort: real.port || (real.protocol === 'https:' ? '443' : '80'),
      realAuthority: real.host
    }
  }

  async #executeRequest(url, method, body = null, requestTimeoutMillis = null) {
    const effectiveTimeout = requestTimeoutMillis || this.#timeoutMillis
    logger.log('HttpClient.request', { method, url, hasBody: !!body, timeout: effectiveTimeout, host: this.#hostOverride ? this.#hostOverride.authority : undefined })

    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), effectiveTimeout)

    try {
      const headers = { 'Content-Type': 'application/json' }
      if (this.#bearerToken) {
        headers['Authorization'] = `Bearer ${this.#bearerToken}`
      }
      Object.assign(headers, this.#headers)

      const options = {
        method,
        signal: controller.signal,
        headers
      }

      let requestUrl = url
      if (this.#hostOverride) {
        const target = this.#applyHostOverride(url)
        const dispatcher = await this.#getPinnedDispatcher(target.realHostname, target.realPort)
        if (!dispatcher) {
          // Sending anyway would advertise Host: <address> and, at a proxy
          // with a default cluster, silently write to the wrong tenant.
          throw new Error(`hostHeader '${this.#hostOverride.authority}' cannot be honored here: no undici dispatcher available (browser environment, or this client was destroyed). Refusing to send with Host: ${target.realAuthority}, which would reach the wrong cluster.`)
        }
        options.dispatcher = dispatcher
        requestUrl = target.requestUrl
      } else {
        const dispatcher = await this.#getDispatcher()
        if (dispatcher) {
          options.dispatcher = dispatcher
        }
      }

      if (body) {
        options.body = JSON.stringify(body)
      }

      const response = await fetch(requestUrl, options)

      logger.log('HttpClient.response', { method, url, status: response.status })

      // Handle 204 No Content
      if (response.status === 204) {
        return null
      }

      // Handle errors
      if (!response.ok) {
        const error = new Error(`HTTP ${response.status}: ${response.statusText}`)
        error.status = response.status

        // Proxy error contract: 429 { error, code: 'rate_limited' | 'quota_exceeded' }
        // with Retry-After (seconds); 403 { error, code: 'cluster_suspended' |
        // 'storage_quota_exceeded' | 'feature_gated' | 'forbidden' }. `.code`
        // is attached whenever the body has one so callers can branch without
        // string-matching `.message`.
        if (response.status === 429) {
          const retryAfterHeader = response.headers.get('retry-after')
          const retryAfterSeconds = retryAfterHeader != null ? Number(retryAfterHeader) : NaN
          error.retryAfterSeconds = Number.isFinite(retryAfterSeconds) ? retryAfterSeconds : null
        }

        try {
          const text = await response.text()
          if (text) {
            const body = JSON.parse(text)
            error.message = body.error || error.message
            if (body.code) error.code = body.code
          }
        } catch (e) {
          // Ignore JSON parse errors
        }

        logger.error('HttpClient.request', { method, url, status: error.status, error: error.message, code: error.code })
        throw error
      }

      // Parse successful response
      const contentType = response.headers.get('content-type')
      const contentLength = response.headers.get('content-length')

      if (!contentType || !contentType.includes('application/json') || contentLength === '0') {
        const text = await response.text()
        if (!text || text.length === 0) {
          return null
        }
        try {
          return JSON.parse(text)
        } catch (e) {
          return null
        }
      }

      return response.json()

    } catch (error) {
      if (error.name === 'AbortError') {
        const timeoutError = new Error(`Request timeout after ${effectiveTimeout}ms`)
        timeoutError.name = 'AbortError'
        timeoutError.timeout = effectiveTimeout
        logger.error('HttpClient.request', { method, url, error: 'timeout', timeout: effectiveTimeout })
        throw timeoutError
      }
      // undici reports every network-level failure as a bare TypeError "fetch
      // failed" with the real reason (ECONNREFUSED, ECONNRESET, socket hang up,
      // ...) hidden in error.cause — surface it so failures are diagnosable.
      if (error.cause && error.message === 'fetch failed') {
        const cause = error.cause.code || error.cause.message || String(error.cause)
        error.message = `fetch failed (${cause})`
      }
      logger.error('HttpClient.request', { method, url, error: error.message })
      throw error
    } finally {
      clearTimeout(timeoutId)
    }
  }

  async #requestWithRetry(method, path, body = null, requestTimeoutMillis = null, retryKind = null) {
    let lastError = null

    for (let attempt = 0; attempt < this.#retryAttempts; attempt++) {
      try {
        const url = this.#getUrl() + path
        return await this.#executeWithRetry429(url, method, body, requestTimeoutMillis, retryKind)
      } catch (error) {
        lastError = error

        // Don't retry on client errors (4xx)
        if (error.status && error.status >= 400 && error.status < 500) {
          throw error
        }

        // Wait before retry (except on last attempt)
        if (attempt < this.#retryAttempts - 1) {
          const delay = this.#retryDelayMillis * Math.pow(2, attempt)
          logger.warn('HttpClient.retry', { method, path, attempt: attempt + 1, delay, error: error.message })
          await new Promise(resolve => setTimeout(resolve, delay))
        }
      }
    }

    logger.error('HttpClient.retry', { method, path, error: 'Max retries exceeded', attempts: this.#retryAttempts })
    throw lastError
  }

  async #requestWithFailover(method, path, body = null, requestTimeoutMillis = null, affinityKey = null, retryKind = null) {
    if (!this.#loadBalancer || !this.#enableFailover) {
      return this.#requestWithRetry(method, path, body, requestTimeoutMillis, retryKind)
    }

    const urls = this.#loadBalancer.getAllUrls()
    const attemptedUrls = new Set()
    let lastError = null

    logger.log('HttpClient.failover', { method, path, totalServers: urls.length, affinityKey })

    for (let i = 0; i < urls.length; i++) {
      // Pass affinity key to load balancer for consistent routing
      const url = this.#loadBalancer.getNextUrl(affinityKey)

      if (attemptedUrls.has(url)) {
        continue
      }

      attemptedUrls.add(url)

      try {
        // 429s are retried in place (same backend, backoff-paced) inside
        // #executeWithRetry429 -- they are not a backend-health signal, so
        // they must not trigger failover to a different server.
        const result = await this.#executeWithRetry429(url + path, method, body, requestTimeoutMillis, retryKind)

        // Mark backend as healthy on success
        this.#loadBalancer.markHealthy(url)

        return result
      } catch (error) {
        lastError = error

        // Mark backend as unhealthy on failure (5xx or network errors)
        if (!error.status || error.status >= 500) {
          this.#loadBalancer.markUnhealthy(url)
        }

        logger.warn('HttpClient.failover', { url, method, path, error: error.message, code: error.code })

        // Don't retry on client errors (4xx) -- includes a 429 whose retry429
        // policy has already been exhausted by #executeWithRetry429 above.
        if (error.status && error.status >= 400 && error.status < 500) {
          throw error
        }

        // Continue to next server for server errors or network issues
      }
    }

    logger.error('HttpClient.failover', { method, path, error: 'All servers failed', attempted: attemptedUrls.size })
    throw lastError || new Error('All servers failed')
  }

  #getUrl() {
    if (this.#loadBalancer) {
      return this.#loadBalancer.getNextUrl()
    }
    return this.#baseUrl
  }

  // `retryKind`: pass 'pop' for long-poll (wait=true) pop requests to get the
  // unbounded-with-backoff 429 policy; omit for everything else (push, admin
  // calls, non-waiting pop), which get the bounded default (10 attempts).
  async get(path, requestTimeoutMillis = null, affinityKey = null, retryKind = null) {
    return this.#requestWithFailover('GET', path, null, requestTimeoutMillis, affinityKey, retryKind)
  }

  async post(path, body = null, requestTimeoutMillis = null, affinityKey = null, retryKind = null) {
    return this.#requestWithFailover('POST', path, body, requestTimeoutMillis, affinityKey, retryKind)
  }

  async put(path, body = null, requestTimeoutMillis = null, affinityKey = null, retryKind = null) {
    return this.#requestWithFailover('PUT', path, body, requestTimeoutMillis, affinityKey, retryKind)
  }

  async delete(path, requestTimeoutMillis = null, affinityKey = null, retryKind = null) {
    return this.#requestWithFailover('DELETE', path, null, requestTimeoutMillis, affinityKey, retryKind)
  }

  getLoadBalancer() {
    return this.#loadBalancer
  }

  /**
   * Release the client's HTTP resources: force-close the per-client agent's
   * keep-alive sockets so the Node event loop can drain. Idempotent; requests
   * issued after destroy() fall back to the global fetch dispatcher.
   */
  async destroy() {
    if (this.#destroyed) return
    this.#destroyed = true
    const dispatcher = this.#dispatcher
    // Subsequent (stray) requests use global fetch instead of a dead agent.
    // With a hostHeader configured there is no such fallback (global fetch
    // could not carry the Host) — those requests fail loudly instead.
    this.#dispatcher = null
    const pending = [...this.#pinnedDispatchers.values()]
    this.#pinnedDispatchers.clear()
    // Entries are promises (see #getPinnedDispatcher); settle them so an Agent
    // created concurrently with close() is released too.
    const pinned = await Promise.all(pending.map(p => Promise.resolve(p).catch(() => null)))
    for (const agent of [dispatcher, ...pinned]) {
      if (!agent) continue
      try {
        await agent.destroy()
        logger.log('HttpClient.destroy', 'Agent destroyed')
      } catch (error) {
        logger.warn('HttpClient.destroy', { error: error.message })
      }
    }
  }
}

