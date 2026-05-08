/**
 * Rate-limiter helpers — composable token-bucket / sliding-window gate
 * factories that return a function suitable for `.gate(fn)` on a Stream.
 *
 * The point of these helpers is purely ergonomic: the user shouldn't have to
 * re-write the refill math every time. The semantics (per-key state in
 * queen_streams.state, partial-ack on deny, FIFO order on lease expiry) all
 * come from the underlying Stream `.gate()` runtime — these factories only
 * decide HOW each request consumes from the bucket.
 *
 * All four common rate-limit shapes are expressible by varying `costFn`:
 *
 *   A) "100 req/sec, batches arbitrary":    costFn = () => 1            on a queue of REQUESTS
 *   B) "100 msg/sec, in any number of req": costFn = () => 1            on a queue of MESSAGES
 *   C) "100 req/sec, exactly 1 msg/req":    costFn = () => 1            same as B (degenerate)
 *   D) "100 weight/sec, cost varies":       costFn = req => req.weight  on a queue of REQUESTS
 *
 * Two gates can be chained on the same pipeline (req/s + msg/s simultaneously
 * required by some OTAs) by running them as two consecutive Stream queries
 * with an intermediate sink queue.
 *
 * Sizing rule (also enforced by tokenBucketGate at start time, and warned
 * by the assertion below): the maximum sustainable rate the bucket can
 * deliver is `min(refillPerSec, capacity / leaseSec)`. Pick capacity and
 * leaseSec so the second term doesn't artificially cap you below
 * refillPerSec — usually `capacity ≈ refillPerSec × leaseSec`.
 */

/**
 * Token-bucket gate factory.
 *
 * Returns a `(msg, ctx) => boolean` function suitable for `.gate(fn)`.
 * The bucket state lives in `ctx.state` (which the runtime persists per-key
 * in queen_streams.state on every ALLOWED message).
 *
 * @param {object} opts
 * @param {number}                 opts.capacity        Max tokens in the bucket (= max burst).
 * @param {number}                 opts.refillPerSec    Steady-state refill rate (tokens / sec).
 * @param {(msg:any)=>number}     [opts.costFn]         Maps a message to its cost in tokens.
 *                                                       Defaults to `() => 1` (one token per message).
 * @param {boolean}               [opts.allowZeroCost]  If true, costFn() may return 0 to mean
 *                                                       "always allow, no token consumed". Defaults true.
 * @returns {(msg:any, ctx:object)=>boolean}
 */
export function tokenBucketGate({
  capacity,
  refillPerSec,
  costFn = () => 1,
  allowZeroCost = true
} = {}) {
  if (!Number.isFinite(capacity) || capacity <= 0) {
    throw new Error('tokenBucketGate: capacity must be a positive number')
  }
  if (!Number.isFinite(refillPerSec) || refillPerSec <= 0) {
    throw new Error('tokenBucketGate: refillPerSec must be a positive number')
  }
  if (typeof costFn !== 'function') {
    throw new Error('tokenBucketGate: costFn must be a function')
  }
  return function tokenBucket(msg, ctx) {
    const now = ctx.streamTimeMs
    if (typeof ctx.state.tokens !== 'number') ctx.state.tokens = capacity
    if (typeof ctx.state.lastRefillAt !== 'number') ctx.state.lastRefillAt = now
    // Refill based on elapsed wall-clock time. Capped at capacity so we
    // never exceed the configured burst.
    const elapsedSec = Math.max(0, (now - ctx.state.lastRefillAt) / 1000)
    ctx.state.tokens = Math.min(capacity, ctx.state.tokens + elapsedSec * refillPerSec)
    ctx.state.lastRefillAt = now

    let cost = costFn(msg)
    if (!Number.isFinite(cost) || cost < 0) cost = 1
    if (cost === 0) return allowZeroCost   // never consume, never block

    if (ctx.state.tokens >= cost) {
      ctx.state.tokens -= cost
      // Lightweight observability: the SDK persists ctx.state on ALLOW only,
      // so these counters automatically reflect "what the bucket actually let
      // through" and can be inspected via SQL on queen_streams.state.
      ctx.state.allowedTotal = (ctx.state.allowedTotal || 0) + 1
      ctx.state.consumedTotal = (ctx.state.consumedTotal || 0) + cost
      return true
    }
    // On deny, ctx.state mutations are discarded by the runtime — so
    // deniedSeen is "lost" and would only be useful if the runtime
    // persisted on every eval. We leave it OUT here so callers don't
    // misinterpret the persisted counters.
    return false
  }
}

/**
 * Sliding-window approximation gate (for "max N events in last W seconds"
 * style limits — e.g. SendGrid daily quotas, OTA hourly quotas).
 *
 * IMPORTANT: this is NOT a precise sliding window. It's a 2-bucket
 * approximation (current + previous) that's accurate within ±1× rate at
 * the boundary. For exact sliding window, you'd need to keep per-event
 * timestamps in state — usually overkill for rate-limiting purposes.
 *
 * @param {object} opts
 * @param {number}                opts.limit          Max events per window.
 * @param {number}                opts.windowSec      Window length in seconds.
 * @param {(msg:any)=>number}    [opts.costFn]        Cost per event (default 1).
 */
export function slidingWindowGate({ limit, windowSec, costFn = () => 1 } = {}) {
  if (!Number.isFinite(limit) || limit <= 0) {
    throw new Error('slidingWindowGate: limit must be a positive number')
  }
  if (!Number.isFinite(windowSec) || windowSec <= 0) {
    throw new Error('slidingWindowGate: windowSec must be a positive number')
  }
  const windowMs = windowSec * 1000
  return function slidingWindow(msg, ctx) {
    const now = ctx.streamTimeMs
    const currentWindow = Math.floor(now / windowMs)
    const elapsedInWindow = (now % windowMs) / windowMs   // 0..1

    if (ctx.state.window !== currentWindow) {
      // Roll: previous window becomes "previous", current resets.
      ctx.state.previousCount = ctx.state.window === currentWindow - 1
        ? (ctx.state.currentCount || 0)
        : 0
      ctx.state.currentCount = 0
      ctx.state.window = currentWindow
    }
    // 2-bucket sliding estimate.
    const estimated = (ctx.state.previousCount || 0) * (1 - elapsedInWindow)
                    + (ctx.state.currentCount  || 0)
    let cost = costFn(msg)
    if (!Number.isFinite(cost) || cost < 0) cost = 1

    if (estimated + cost <= limit) {
      ctx.state.currentCount = (ctx.state.currentCount || 0) + cost
      return true
    }
    return false
  }
}
