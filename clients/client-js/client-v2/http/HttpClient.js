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
      headers = {}
    } = options

    this.#baseUrl = baseUrl
    this.#loadBalancer = loadBalancer
    this.#timeoutMillis = timeoutMillis
    this.#retryAttempts = retryAttempts
    this.#retryDelayMillis = retryDelayMillis
    this.#enableFailover = enableFailover
    this.#bearerToken = bearerToken
    this.#headers = headers || {}
    
    logger.log('HttpClient.constructor', { 
      hasLoadBalancer: !!loadBalancer, 
      baseUrl: baseUrl || 'load-balanced', 
      timeoutMillis, 
      retryAttempts,
      enableFailover,
      hasAuth: !!bearerToken,
      customHeaders: Object.keys(this.#headers).length
    })
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

        try {
          const text = await response.text()
          if (text) {
            const body = JSON.parse(text)
            error.message = body.error || error.message
          }
        } catch (e) {
          // Ignore JSON parse errors
        }

        logger.error('HttpClient.request', { method, url, status: error.status, error: error.message })
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

  async #requestWithRetry(method, path, body = null, requestTimeoutMillis = null) {
    let lastError = null

    for (let attempt = 0; attempt < this.#retryAttempts; attempt++) {
      try {
        const url = this.#getUrl() + path
        return await this.#executeRequest(url, method, body, requestTimeoutMillis)
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

  async #requestWithFailover(method, path, body = null, requestTimeoutMillis = null, affinityKey = null) {
    if (!this.#loadBalancer || !this.#enableFailover) {
      return this.#requestWithRetry(method, path, body, requestTimeoutMillis)
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
        const result = await this.#executeRequest(url + path, method, body, requestTimeoutMillis)
        
        // Mark backend as healthy on success
        this.#loadBalancer.markHealthy(url)
        
        return result
      } catch (error) {
        lastError = error
        
        // Mark backend as unhealthy on failure (5xx or network errors)
        if (!error.status || error.status >= 500) {
          this.#loadBalancer.markUnhealthy(url)
        }
        
        logger.warn('HttpClient.failover', { url, method, path, error: error.message })

        // Don't retry on client errors (4xx)
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

  async get(path, requestTimeoutMillis = null, affinityKey = null) {
    return this.#requestWithFailover('GET', path, null, requestTimeoutMillis, affinityKey)
  }

  async post(path, body = null, requestTimeoutMillis = null, affinityKey = null) {
    return this.#requestWithFailover('POST', path, body, requestTimeoutMillis, affinityKey)
  }

  async put(path, body = null, requestTimeoutMillis = null, affinityKey = null) {
    return this.#requestWithFailover('PUT', path, body, requestTimeoutMillis, affinityKey)
  }

  async delete(path, requestTimeoutMillis = null, affinityKey = null) {
    return this.#requestWithFailover('DELETE', path, null, requestTimeoutMillis, affinityKey)
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

