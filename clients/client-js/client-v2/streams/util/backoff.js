/**
 * Exponential backoff helper for retrying transient failures (HTTP 5xx,
 * lease conflicts, etc.).
 */

export function makeBackoff({ initialMs = 50, maxMs = 5000, multiplier = 2 } = {}) {
  let attempt = 0
  return {
    next() {
      const ms = Math.min(maxMs, Math.floor(initialMs * Math.pow(multiplier, attempt)))
      attempt++
      // Add jitter up to 30% to spread retries.
      return ms + Math.floor(Math.random() * ms * 0.3)
    },
    reset() {
      attempt = 0
    }
  }
}

export function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}
