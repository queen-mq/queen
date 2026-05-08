/**
 * Minimal HTTP client for /streams/v1/* endpoints.
 *
 * The fat-client SDK does not need to share a connection pool with the
 * user's Queen client — streams routes are a different surface (cycle
 * commits + state reads + query registration) and are sized differently
 * than push/pop. We therefore use Node's built-in fetch with a small retry
 * loop, scoped by the user's Queen base URL.
 */

import { makeBackoff, sleep } from '../util/backoff.js'

export class StreamsHttpClient {
  /**
   * @param {object} options
   * @param {string} options.url - base URL of Queen, e.g. http://localhost:6632
   * @param {string} [options.bearerToken]
   * @param {number} [options.timeoutMs=30000]
   * @param {number} [options.retryAttempts=3]
   */
  constructor({ url, bearerToken, timeoutMs = 30000, retryAttempts = 3 }) {
    if (!url) throw new Error('StreamsHttpClient requires url')
    this.url = url.replace(/\/$/, '')
    this.bearerToken = bearerToken
    this.timeoutMs = timeoutMs
    this.retryAttempts = retryAttempts
  }

  /**
   * POST a JSON body and return the parsed JSON response. Throws on
   * non-2xx after `retryAttempts` retries (with exponential backoff for
   * 5xx and network errors; 4xx are not retried).
   */
  async post(path, body) {
    const fullUrl = this.url + path
    const backoff = makeBackoff({ initialMs: 100, maxMs: 5000 })

    for (let attempt = 0; attempt <= this.retryAttempts; attempt++) {
      const controller = new AbortController()
      const timer = setTimeout(() => controller.abort(), this.timeoutMs)

      const headers = { 'Content-Type': 'application/json' }
      if (this.bearerToken) headers['Authorization'] = `Bearer ${this.bearerToken}`

      try {
        const res = await fetch(fullUrl, {
          method: 'POST',
          headers,
          body: JSON.stringify(body),
          signal: controller.signal
        })
        clearTimeout(timer)
        const text = await res.text()
        let parsed = null
        if (text) {
          try { parsed = JSON.parse(text) } catch { parsed = { raw: text } }
        }
        if (!res.ok) {
          const err = new Error(parsed && parsed.error ? parsed.error : `HTTP ${res.status}`)
          err.status = res.status
          err.body = parsed
          // 4xx (other than 408/429) is not retriable.
          if (res.status >= 400 && res.status < 500 && res.status !== 408 && res.status !== 429) {
            throw err
          }
          if (attempt < this.retryAttempts) {
            await sleep(backoff.next())
            continue
          }
          throw err
        }
        return parsed
      } catch (err) {
        clearTimeout(timer)
        if (err && err.status && err.status >= 400 && err.status < 500 && err.status !== 408 && err.status !== 429) {
          throw err
        }
        if (attempt >= this.retryAttempts) throw err
        await sleep(backoff.next())
      }
    }
  }
}
