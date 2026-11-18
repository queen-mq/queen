/**
 * The KV surface: get, getMany, getPrefix, put, putIfAbsent, delete, incr,
 * check, plus `once` (the idempotency marker) and `listAll` (a prefix walk).
 *
 * The HTTP status describes the outcome of the CALL, never the verdict of
 * the predicate: an absent key, a lost putIfAbsent race, a delete that hit
 * nothing are all answered normally, with an explicit field.
 *
 * **Every write returns an OBJECT, and objects are always truthy.**
 *
 *     if (await kv.delete(ns, key)) { ... }        // ALWAYS TAKEN. A bug.
 *     const r = await kv.delete(ns, key)
 *     if (r.applied) { ... }                       // the field is the answer
 *
 * An expiry is MANDATORY on every write: exactly one of `ttlSeconds` (or the
 * sugar `ttl` / `until`) and `forever: true`.
 */

import type { Duration, Instant, JsonObject, QueenHttpClient } from '../types.js'

/**
 * The expiry of a KV write, MANDATORY on `put`, `putIfAbsent`, `incr` and
 * `once`: exactly one of
 * - `ttlSeconds`: lifetime in seconds, the canonical field;
 * - `ttl`: lifetime as a duration string ('24h'), rounded up to seconds;
 * - `until`: an instant to expire at (Date, ISO string or epoch ms), in the future;
 * - `forever: true`: never expires (there is no `forever: false`).
 *
 * None, or two, is refused (`kv_expiry_not_specified` from the broker, or a
 * client-side throw for two spellings of one lifetime).
 */
export type KvExpiryOptions =
  | { ttlSeconds: number; ttl?: undefined; until?: undefined; forever?: undefined }
  | { ttl: Duration; ttlSeconds?: undefined; until?: undefined; forever?: undefined }
  | { until: Instant; ttlSeconds?: undefined; ttl?: undefined; forever?: undefined }
  | { forever: true; ttlSeconds?: undefined; ttl?: undefined; until?: undefined }

/** `required: true` escalates a lost precondition into a rolled-back transaction. */
export interface KvRequiredOption {
  required?: boolean | undefined
}

/**
 * `expect` is the optimistic lock: `0` means "must not exist", `N > 0` a pure
 * update of version N. Leave it out for an unconditional write; an
 * explicitly undefined or null `expect` throws.
 */
export interface KvExpectOption {
  expect?: number
}

export type KvPutOptions = KvExpiryOptions & KvRequiredOption & KvExpectOption
export type KvPutIfAbsentOptions = KvExpiryOptions & KvRequiredOption & { expect?: never }
export type KvDeleteOptions = KvRequiredOption & KvExpectOption
export type KvIncrOptions = KvExpiryOptions & KvRequiredOption & {
  min?: number | undefined
  max?: number | undefined
  expect?: never
}
export type KvCheckOptions = KvRequiredOption & { expect: number }
export type KvOnceOptions = KvExpiryOptions & KvRequiredOption & {
  /** The marker's value (default `true`). */
  value?: unknown
}

export interface KvGetPrefixOptions {
  /** Clamped by the broker (default 100, ceiling 1000). */
  limit?: number | undefined
  /** An exclusive keyset cursor: the `nextAfter` of the page before. */
  after?: string | null | undefined
  keysOnly?: boolean | undefined
}

/** One key's row. `found` is separate from `value` because `null` is a legal value. */
export interface KvRow<V = any> {
  found: boolean
  key?: string
  value?: V
  version?: number
  expiresAt?: string | null
  updatedAt?: string
  [key: string]: unknown
}

export interface KvGetManyResult<V = any> {
  rows: KvRow<V>[]
  /** Explicit: absence is a datum. */
  missing: string[]
  /** The byte budget cut the page; those keys are in neither list. */
  truncated: boolean
  [key: string]: unknown
}

export interface KvGetPrefixResult<V = any> {
  rows: KvRow<V>[]
  truncated: boolean
  nextAfter: string | null
  [key: string]: unknown
}

/** A write's answer. ALWAYS TRUTHY: read `applied`. */
export interface KvWriteResult<V = any> {
  applied: boolean
  op: string
  key: string
  /** The current value; for a loser, the winner's. */
  value?: V
  version?: number
  /** Why it did not apply: `version`, `absent`, `exists`, `limit`, ... */
  reason?: string | null
  /** The verdict rolled a `required` transaction back. */
  precondition?: true
  failedIndex?: number
  [key: string]: unknown
}

/** `once()`: `won` is the answer, `value` the winner's. */
export interface KvOnceResult<V = any> {
  won: boolean
  value: V | undefined
  version: number | undefined
  result: KvWriteResult<V>
}

/** An op entry as the builders make it: the finished op minus its expiry, and the expiry sugar. */
export interface KvOpEntry {
  base: JsonObject
  expiry: KvExpiryOptions | null
}

/** Op builders, shared with TransactionBuilder. */
export declare const kvOp: {
  get(ns: string, key: string): KvOpEntry
  getMany(ns: string, keys: string[]): KvOpEntry
  getPrefix(ns: string, prefix: string, opts?: KvGetPrefixOptions): KvOpEntry
  put(ns: string, key: string, value: unknown, opts: KvPutOptions): KvOpEntry
  putIfAbsent(ns: string, key: string, value: unknown, opts: KvPutIfAbsentOptions): KvOpEntry
  delete(ns: string, key: string, opts?: KvDeleteOptions): KvOpEntry
  incr(ns: string, key: string, delta: number | undefined, opts: KvIncrOptions): KvOpEntry
  check(ns: string, key: string, opts: KvCheckOptions): KvOpEntry
}

/** Finish one op: merge the resolved expiry into the base. */
export declare function materializeKvOp(entry: KvOpEntry, nowMs?: number): JsonObject

export declare class Kv {
  constructor(httpClient: QueenHttpClient)
  /** One key. Returns the ROW, not the value. */
  get<V = any>(ns: string, key: string): Promise<KvRow<V>>
  /** Many keys of one namespace. */
  getMany<V = any>(ns: string, keys: string[]): Promise<KvGetManyResult<V>>
  /** One page of a prefix scan. Each page is its own snapshot. */
  getPrefix<V = any>(ns: string, prefix: string, opts?: KvGetPrefixOptions): Promise<KvGetPrefixResult<V>>
  /** Every row under a prefix, one page at a time. */
  listAll<V = any>(ns: string, prefix: string, opts?: KvGetPrefixOptions): AsyncGenerator<KvRow<V>, void, undefined>
  /** Write a value. An expiry is required. */
  put<V = any>(ns: string, key: string, value: V, opts: KvPutOptions): Promise<KvWriteResult<V>>
  /** put with `expect: 0`. The loser gets the winner's value. */
  putIfAbsent<V = any>(ns: string, key: string, value: V, opts: KvPutIfAbsentOptions): Promise<KvWriteResult<V>>
  /** Delete a key, optionally fenced with `expect`. */
  delete(ns: string, key: string, opts?: KvDeleteOptions): Promise<KvWriteResult>
  /** Add `delta` (`undefined` for 1) to a numeric key. With `max`, `applied` IS the admission decision. */
  incr(ns: string, key: string, delta: number | undefined, opts: KvIncrOptions): Promise<KvWriteResult<number>>
  /** Is the key still at this version? Writes nothing. */
  check(ns: string, key: string, opts: KvCheckOptions): Promise<KvWriteResult>
  /** The idempotency marker: `putIfAbsent` with a default value of `true`. */
  once<V = any>(ns: string, key: string, opts: KvOnceOptions): Promise<KvOnceResult<V>>
}
