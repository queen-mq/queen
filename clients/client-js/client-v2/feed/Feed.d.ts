/**
 * The tenant feed: every push to a tenant's queues, in the order the broker's
 * log committed them, read by position (`POST /api/v1/feed`).
 *
 *   // What happened to one booking today, then stop.
 *   for await (const e of queen.feed({ partitions: ['booking-9137'], since: '1d', follow: false })) {
 *     console.log(e.position, e.queue, e.partition, e.data)
 *   }
 *
 *   // Every Italian order from now on, as it is committed.
 *   for await (const e of queen.feed({ queues: ['orders'], where: [['data.country', '=', 'IT']] })) { ... }
 *
 * Nothing is leased, acked or moved: the feed is a read, and every consumer
 * group still gets its messages. Followers serve it.
 */

import type {
  Instant,
  JsonObject,
  MessageHeaders,
  QueenHttpClient,
  WhereCondition,
  WhereConditions,
  WhereOp
} from '../types.js'

/** A feed entry: the message in Queen's shape, plus its position. */
export interface FeedEntry<T = any> {
  /** The log entry that wrote it; one number orders every queue of the tenant. */
  position: number
  kind?: string
  queue: string
  partition: string
  /** The partition's UUID. */
  partitionId: string
  offset: number
  id: string
  transactionId: string
  data: T
  ts: string
  /** The same as `ts`. */
  createdAt: string
  /** Present only when the message has headers. */
  headers?: MessageHeaders
}

/** One `read()` of the feed. */
export interface FeedPage<T = any> {
  entries: FeedEntry<T>[]
  next: number
  resolvedTs?: string
  caughtUp: boolean
  applied?: number
}

/** Shard the feed: only the partitions whose name hashes to `index` of `count`. */
export interface FeedShardOption {
  count: number
  index: number
  /** Only these queues are split; the others are read whole. */
  queues?: string[] | undefined
}

/** Options of `queen.feed()`. */
export interface FeedOptions {
  /** Names or patterns (`orders*`); default every queue. */
  queues?: string | string[] | undefined
  queue?: string | string[] | undefined
  /** Partition names. */
  partitions?: string | string[] | undefined
  partition?: string | string[] | undefined
  /** Conditions the broker filters on. */
  where?: WhereConditions | null | undefined
  kinds?: string[] | undefined
  /** First position; default 'now' ('earliest' with `since`). */
  from?: number | 'now' | 'earliest' | undefined
  /** Only messages created at or after it: a Date, epoch ms, ISO string, or a duration back from now ('1d'). */
  since?: Instant | undefined
  /** Keep waiting for new entries (default true); false stops once caught up. */
  follow?: boolean | undefined
  /** Long poll of each read while caught up (default 5000, max 30000). */
  waitMillis?: number | undefined
  /** Messages per read (broker default 1000). */
  maxRecords?: number | undefined
  /** Payload bytes per read (broker default 1 MiB). */
  maxBytes?: number | undefined
  /** Nodes to read from first, e.g. one follower's URL. */
  followerUrls?: string[] | undefined
  shard?: FeedShardOption | null | undefined
}

/**
 * One `where` condition, checked: `{ field, op, value }`.
 *
 *   whereCondition('data.country', '=', 'IT')
 *   whereCondition('data.total', '>', 100)
 *   whereCondition('partition', 'prefix', 'booking-')
 *   whereCondition('data.vip', 'exists')
 *
 * Fields: queue, partition, transactionId, offset, position, data,
 * data.<path>, headers.<name>.
 */
export declare function whereCondition(field: string, op: WhereOp, value?: unknown): WhereCondition

/** `where` as the caller wrote it: a condition, `[field, op, value]`, or a list of either. */
export declare function normalizeWhere(where: WhereConditions | null | undefined): WhereCondition[]

/** A `where` as the broker reads it: one string, or the checked conditions; null for none. */
export declare function whereValue(where: string | WhereConditions | null | undefined): string | WhereCondition[] | null

/** Whether a message satisfies one condition. */
export declare function conditionHolds(cond: WhereCondition, msg: object): boolean

/**
 * A start time as the broker reads it: a Date, epoch milliseconds, an ISO
 * string, or a duration back from now ('30s', '15m', '2h', '1d', '1w').
 */
export declare function sinceToIso(since: Instant, now?: number): string

export declare class FeedReader<T = any> implements AsyncIterable<FeedEntry<T>> {
  /**
   * @param httpClient - the Queen client's own HTTP client
   * @param clientFor - a client to one node, same credentials
   * @param scheme - 'http' or 'https', from the configured URL
   */
  constructor(
    httpClient: QueenHttpClient,
    clientFor: (baseUrl: string) => QueenHttpClient,
    scheme: 'http' | 'https' | string,
    options?: FeedOptions
  )
  /** Where the next read starts: a position once a read has answered. */
  get position(): number | 'now' | 'earliest'
  /** Move the reader. */
  seek(position: number | 'now' | 'earliest'): void
  /**
   * One read; the reader moves to `next`. Waits up to `waitMillis` for an
   * entry while there is nothing new. `signal` cancels it.
   */
  read(options?: { waitMillis?: number | undefined; signal?: AbortSignal | undefined }): Promise<FeedPage<T>>
  /** One call that reads nothing and leaves the reader where it is: it throws what a read would. */
  probe(): Promise<JsonObject>
  /** Every entry, in log order. Stops once caught up when `follow` is false, or at stop(). */
  [Symbol.asyncIterator](): AsyncGenerator<FeedEntry<T>, void, undefined>
  /** Ends the iteration after the entry in hand. */
  stop(): void
  /** Closes the clients this reader opened to followers. */
  close(): Promise<void>
}
