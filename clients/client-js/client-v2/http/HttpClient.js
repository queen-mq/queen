/**
 * HTTP client with retry, load balancing, and failover support
 */

import * as logger from '../utils/logger.js'

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
    this.#headers = headers || {}
    this.#retry429 = retry429 || {}

    logger.log('HttpClient.constructor', {
      hasLoadBalancer: !!loadBalancer,
      baseUrl: baseUrl || 'load-balanced',
      timeoutMillis,
      retryAttempts,
      enableFailover,
      hasAuth: !!bearerToken,
      customHeaders: Object.keys(this.#headers).length,
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

  async #executeRequest(url, method, body = null, requestTimeoutMillis = null) {
    const effectiveTimeout = requestTimeoutMillis || this.#timeoutMillis
    logger.log('HttpClient.request', { method, url, hasBody: !!body, timeout: effectiveTimeout })
    
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

      const dispatcher = await this.#getDispatcher()
      if (dispatcher) {
        options.dispatcher = dispatcher
      }

      if (body) {
        options.body = JSON.stringify(body)
      }

      const response = await fetch(url, options)
      
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
    this.#dispatcher = null
    if (dispatcher) {
      try {
        await dispatcher.destroy()
        logger.log('HttpClient.destroy', 'Agent destroyed')
      } catch (error) {
        logger.warn('HttpClient.destroy', { error: error.message })
      }
    }
  }
}

