/**
 * Rate-limiter helpers: token-bucket and sliding-window gate factories that
 * return a function suitable for `.gate(fn)` on a Stream. The semantics
 * (per-key state in the broker, partial-ack on deny, FIFO order on lease
 * expiry) come from `.gate()`; these only decide how each message consumes
 * from the bucket.
 *
 * Sizing rule: the maximum sustainable rate is
 * `min(refillPerSec, capacity / leaseSec)`, so usually
 * `capacity ≈ refillPerSec × leaseSec`.
 */

import type { GateFunction } from '../Stream.js'

export interface TokenBucketGateOptions<V = any> {
  /** Max tokens in the bucket (= max burst). */
  capacity: number
  /** Steady-state refill rate (tokens / sec). */
  refillPerSec: number
  /** A message's cost in tokens (default `() => 1`). */
  costFn?: ((value: V) => number) | undefined
  /** `costFn()` may return 0 to mean "always allow, no token consumed" (default true). */
  allowZeroCost?: boolean | undefined
}

export interface SlidingWindowGateOptions<V = any> {
  /** Max events per window. */
  limit: number
  /** Window length in seconds. */
  windowSec: number
  /** Cost per event (default 1). */
  costFn?: ((value: V) => number) | undefined
}

/** Token-bucket gate factory. */
export declare function tokenBucketGate<V = any>(opts: TokenBucketGateOptions<V>): GateFunction<V>

/**
 * Sliding-window approximation gate ("max N events in last W seconds"): a
 * 2-bucket approximation, accurate within ±1× rate at the boundary.
 */
export declare function slidingWindowGate<V = any>(opts: SlidingWindowGateOptions<V>): GateFunction<V>
