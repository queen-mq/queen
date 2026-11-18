/**
 * Locks: a lock and a semaphore, as leases with a fencing token.
 *
 *     const lock = queen.lock('daily-report', { ttl: '30s' })
 *     if (!(await lock.acquire())) return          // somebody else has it
 *     try {
 *       await queen.transaction()
 *         .guard(lock)                             // commits only while the lock is ours
 *         .queue('reports').push([{ data: report }])
 *         .commit()
 *     } finally {
 *       await lock.release()
 *     }
 *
 * It is a lease, not a mutex: a permit EXPIRES, and nobody tells its holder.
 * What makes the WORK exclusive is the token: `.guard(lock)` on a
 * transaction inside Queen, `lock.token` outside it. The token changes at
 * every renew: read it when you use it.
 */

import type { Duration, JsonObject, QueenHttpClient } from '../types.js'

/** `.code` of the error a guard throws for a handle that holds nothing, and of the reason `lock.signal` aborts with. */
export declare const LOCK_NOT_HELD: 'LOCK_NOT_HELD'

/**
 * A lock's lifetime, MANDATORY: exactly one of `ttl: '30s'` (a duration,
 * rounded up to seconds) and `ttlSeconds: 30` (a whole number above zero).
 * There is no `forever` and no default; none, or both, throws.
 */
export type LockLifetime =
  | { ttl: Duration; ttlSeconds?: undefined }
  | { ttlSeconds: number; ttl?: undefined }

/** The options of a lock handle, besides its lifetime and permits. */
export interface LockHandleOptions {
  /** The holder's identity; minted per handle when absent. Two handles with one owner are one holder. */
  owner?: string | null | undefined
  /** Renew in the background, every third of the lifetime (default true). */
  autoRenew?: boolean | undefined
  /** How often to renew, as a duration shorter than the lifetime. */
  renewEvery?: Duration | undefined
  /** First pause of a waiting acquire, in milliseconds (default 100). */
  retryMinMs?: number | undefined
  /** Longest pause of a waiting acquire, in milliseconds (default 1000). */
  retryMaxMs?: number | undefined
}

/** Options of a `Lock` handle: its lifetime, the handle options, and its permits (1 is a lock). */
export type LockOptions = LockLifetime & LockHandleOptions & { limit?: number | undefined }

/** Options of `lock.acquire()` and `lock.run()`. */
export interface LockAcquireOptions {
  /** Keep trying for this long (a duration string); without it there is one attempt. */
  wait?: Duration | undefined
  /** Gives the wait up early. */
  signal?: AbortSignal | undefined
}

/** Why a permit was lost. */
export type LockLostReason = 'renew' | 'guard' | 'expired'

/** One holder of a permit, as `locks.get()` lists it. */
export interface LockHolder {
  slot: number
  owner: string
  token: number
  since?: string
  expiresAt?: string
  renewedAt?: string
  [key: string]: unknown
}

/** `locks.acquire()`. */
export interface LockAcquireResult {
  acquired: boolean
  slot?: number
  token?: number
  owner?: string
  guard?: JsonObject
  already?: boolean
  /** Why not: `held` or `contended`. */
  reason?: string
  holders?: LockHolder[]
  [key: string]: unknown
}

/** `locks.renew()`. */
export interface LockRenewResult {
  renewed: boolean
  slot?: number
  /** The NEW token. */
  token?: number
  guard?: JsonObject
  reason?: string
  holders?: LockHolder[]
  [key: string]: unknown
}

/** `locks.release()`. */
export interface LockReleaseResult {
  released: boolean
  reason?: string
  [key: string]: unknown
}

/** `locks.get()`. */
export interface LockGetResult {
  held: boolean
  holders: LockHolder[]
  [key: string]: unknown
}

/** One operation of `locks.batch()`. */
export interface LockOperation {
  op: 'acquire' | 'renew' | 'release' | 'get'
  name: string
  ttlSeconds?: number
  owner?: string
  limit?: number
  token?: number
  slot?: number
}

/** The lifetime in whole seconds, rounded up. Throws when none is given. */
export declare function lockTtlSeconds(opts: LockLifetime): number

/**
 * The four lock operations as the broker speaks them, with no state kept:
 * you carry the token. `queen.lock()` is what most code wants; this is for
 * `get` (who holds it?) and for a caller with its own loop. Every result is
 * an OBJECT and objects are truthy: read the field.
 */
export declare class Locks {
  constructor(httpClient: QueenHttpClient)
  /** Several operations in one call, each on a different lock: one result per operation, in order. */
  batch(operations: LockOperation[]): Promise<JsonObject[]>
  /** Take a permit. `limit` above 1 makes it a semaphore of that many permits. */
  acquire(name: string, opts: LockLifetime & { owner?: string | null | undefined; limit?: number | undefined }): Promise<LockAcquireResult>
  /** Extend a permit: a NEW token, or `renewed: false` with `reason: 'lost'`. */
  renew(
    name: string,
    opts: LockLifetime & { token: number; slot?: number | undefined; owner?: string | null | undefined }
  ): Promise<LockRenewResult>
  /** Give a permit back. */
  release(name: string, opts: { token: number; slot?: number | null | undefined }): Promise<LockReleaseResult>
  /** Who holds it. */
  get(name: string): Promise<LockGetResult>
  /**
   * Give back every permit this client's handles hold, best effort. Called by
   * `queen.close()`.
   * @internal
   */
  releaseAll(): Promise<number>
}

/**
 * One holder's hold on one lock (or one permit of a semaphore). It keeps
 * the token current, renews in the background, and says when the permit is
 * gone: `lock.signal` aborts and `onLost` handlers run.
 */
export declare class Lock {
  constructor(locks: Locks, name: string, opts: LockOptions)
  get name(): string
  get owner(): string
  get limit(): number
  get ttlSeconds(): number
  /** Whether this handle holds a permit, as far as it can know. A belief with a deadline, not a proof. */
  get held(): boolean
  /** The fencing token of the current lease period; `null` when not held. */
  get token(): number | null
  /** The semaphore slot this handle holds (0 for a lock); `null` when not held. */
  get slot(): number | null
  /** Epoch milliseconds past which the permit must be considered gone unless a renew succeeds. */
  get validUntil(): number
  /** Aborts when the permit is lost. A release is not a loss. */
  get signal(): AbortSignal
  /** Run `fn(reason)` when the permit is lost. */
  onLost(fn: (reason: LockLostReason) => unknown): this
  /** The KV operation that holds while the permit is this handle's. Throws `LOCK_NOT_HELD` when not held. */
  guard(): JsonObject
  /** Take the permit: resolves `true` or `false`. */
  acquire(opts?: LockAcquireOptions): Promise<boolean>
  /** Extend the lease now: `true`, or `false` when the permit is gone. */
  renew(): Promise<boolean>
  /** Give the permit back: `true` when the broker removed it. */
  release(): Promise<boolean>
  /** Acquire, run `fn(lock)`, release. */
  run<R>(fn: (lock: this) => R | Promise<R>, opts?: LockAcquireOptions): Promise<{ acquired: false } | { acquired: true; value: R }>
}
