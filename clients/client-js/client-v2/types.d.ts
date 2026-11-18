/**
 * Shared types of the Queen JS client.
 *
 * This file has no runtime counterpart: it only declares the shapes the
 * client takes and returns. Re-exported from the package entry point with
 * `export type`, so `import type { Message } from 'queen-mq'` works.
 */

// ---------------------------------------------------------------------------
// JSON and small helpers
// ---------------------------------------------------------------------------

/** A JSON object as the broker answers it, when the client does not shape it further. */
export interface JsonObject {
  [key: string]: unknown
}

/**
 * A duration string: a number and a unit, `ms`, `s`, `m`, `h` or `d`
 * (`'250ms'`, `'30s'`, `'15m'`, `'24h'`, `'1d'`). Numbers are refused on
 * purpose: the numeric spellings carry their unit in the name (`ttlSeconds`,
 * `delayMs`).
 */
export type Duration = string

/** An instant: a Date, epoch milliseconds or an ISO 8601 string. */
export type Instant = Date | number | string

/** Message headers: names 1 to 64 bytes of a-z, 0-9, '.', '_' or '-', none starting with `queen-`. */
export type MessageHeaders = Record<string, string>

// ---------------------------------------------------------------------------
// Client configuration
// ---------------------------------------------------------------------------

/**
 * The logger the client reports to. It must implement `info`, `warn` and
 * `error`; `debug` is optional and falls back to `info`. With
 * `structuredLogs: true` the client calls it pino/bunyan style:
 * `info(fields, message)`.
 */
export interface QueenLogger {
  info(...args: any[]): unknown
  warn(...args: any[]): unknown
  error(...args: any[]): unknown
  debug?(...args: any[]): unknown
}

/** A function that fetches a bearer token (a client token minted by your backend). */
export type TokenFunction = () => string | Promise<string>

/** Backoff policy for HTTP 429 (rate limited) answers from a queen_proxy deployment. */
export interface Retry429Options {
  /** Attempts before giving up; default 10 for push/admin calls, unbounded for long-poll pops. */
  maxAttempts?: number | undefined
  /** Base of the exponential backoff when the server sends no Retry-After (default 500). */
  baseMs?: number | undefined
  /** Ceiling of the exponential backoff (default 30000). */
  capMs?: number | undefined
}

/** How a multi-server client spreads requests. */
export type LoadBalancingStrategy = 'affinity' | 'round-robin' | 'session'

/**
 * The options of `new Queen(config)`. Exactly one of `url` and `urls` is
 * required. Every other field defaults to `CLIENT_DEFAULTS`.
 *
 * Leave a field out to keep its default: the options are spread over the
 * defaults, so a field set to `undefined` replaces the default with
 * `undefined`.
 */
export interface QueenConfig {
  /** One broker URL. */
  url?: string
  /** Several broker URLs, load balanced with `loadBalancingStrategy`. */
  urls?: string[]
  timeoutMillis?: number
  retryAttempts?: number
  retryDelayMillis?: number
  loadBalancingStrategy?: LoadBalancingStrategy
  /** Virtual nodes per server for the affinity strategy. */
  affinityHashRing?: number
  enableFailover?: boolean
  healthRetryAfterMillis?: number
  /** A fixed token (an API key), or a function that fetches one (refreshed before it expires and after a 401). */
  bearerToken?: string | TokenFunction | null
  /** Custom headers sent with every request. */
  headers?: Record<string, string>
  /** Host to advertise on every request, independent of the address dialed (bare authority). */
  hostHeader?: string | null
  /** Register SIGINT/SIGTERM handlers that flush buffers (disable when used as a library). */
  handleSignals?: boolean
  logger?: QueenLogger | null
  /** Call the logger pino/bunyan style: `info(fields, message)`. */
  structuredLogs?: boolean
  retry429?: Retry429Options | undefined
  /** Push with the binary push body instead of JSON (needs a broker that reads it). */
  binaryPush?: boolean
  /** With `binaryPush`: frame and zstd-compress each partition's batch client-side. */
  pushCompression?: boolean
  /** Ask pops for the stored records and unpack them client-side. */
  framedPop?: boolean
  /** Buffered pushes the whole client may hold before a buffered push waits. */
  bufferMaxMessages?: number
  /** Buffered messages the broker refused 5 times, given up on. Absent, they are reported on the console. */
  onBufferError?: ((items: unknown[], error: Error) => unknown) | null
}

// ---------------------------------------------------------------------------
// Queues
// ---------------------------------------------------------------------------

/** A where operator: `= != < <= > >= in exists prefix !prefix` (`==` reads as `=`). */
export type WhereOp = '=' | '==' | '!=' | '<' | '<=' | '>' | '>=' | 'in' | 'exists' | 'prefix' | '!prefix'

/** One checked where condition, as `whereCondition()` builds it. */
export interface WhereCondition {
  field: string
  op: WhereOp
  value?: unknown
}

/** One where condition as written: `{ field, op, value }` or `[field, op, value]`. */
export type WhereConditionInput =
  | { field: string; op: WhereOp; value?: unknown }
  | readonly [field: string, op: WhereOp, value?: unknown]

/** Conditions as `.where()`, `config({ where })` and the feed take them: one condition, or a list. */
export type WhereConditions = WhereConditionInput | readonly WhereConditionInput[]

/** A where as a filtered group or a reading queue takes it: one string for the broker to read, or conditions. */
export type WhereInput = string | WhereConditions

/** One source of a reading queue. */
export interface ReadingQueueSource {
  queue: string
  where?: WhereInput | null | undefined
}

/**
 * Queue options, sent by `create()`. Options not given are filled in from
 * `QUEUE_DEFAULTS` (a reading queue gets none of the options it takes from
 * its sources). `null` resets an option to the broker's default.
 */
export interface QueueConfig {
  /** Lease of a popped message, in seconds. */
  leaseTime?: number | null
  /** Retries before a failed message goes to the dead letter queue. */
  retryLimit?: number | null
  retryDelay?: number | null
  priority?: number | null
  /** Seconds a pushed message waits before it can be popped. */
  delayedProcessing?: number | null
  windowBuffer?: number | null
  /** Most messages the queue holds (0: no limit). */
  maxSize?: number | null
  ttl?: number | null
  deadLetterQueue?: boolean | null
  dlqAfterMaxRetries?: boolean | null
  retentionEnabled?: boolean | null
  retentionSeconds?: number | null
  completedRetentionSeconds?: number | null
  encryptionEnabled?: boolean | null
  maxWaitTimeSeconds?: number | null
  minPopWaitTime?: number | null
  dedupWindowSeconds?: number | null
  retentionSinkHold?: string | null
  retentionSinkHoldMaxSeconds?: number | null
  /** Reading queue: the queues it reads (a name, a list, or `[{ queue, where }]`); `null` stops reading. */
  from?: string | ReadingQueueSource | ReadonlyArray<string | ReadingQueueSource> | null
  /** Reading queue: the messages it takes, as `.where()` takes them. */
  where?: WhereInput | null
  /** Reading queue: `'same'`, `'data.<path>'` or `'headers.<name>'`. */
  partitionBy?: string | null
  /** Reading queue: `'now'`, or `'all'` for every match the sources still keep. */
  start?: 'now' | 'all' | null
}

/** The answer of a queue's `create()`. */
export interface QueueConfigureResult {
  configured: boolean
  queueId?: string
  queue?: string
  namespace?: string
  task?: string
  options?: JsonObject
  /** A reading queue's route (a configure with `from`). */
  route?: JsonObject
  [key: string]: unknown
}

/** The answer of a queue's `delete()`. */
export interface QueueDeleteResult {
  deleted?: boolean
  [key: string]: unknown
}

/**
 * Where a consumer group starts: `'new'` skips what is already there (the
 * broker's default), `'new-only'` is the same mode under its other name, and
 * `'all'` takes the backlog.
 */
export type SubscriptionMode = 'new' | 'new-only' | 'all'

// ---------------------------------------------------------------------------
// Push
// ---------------------------------------------------------------------------

interface PushEnvelopeBase {
  /** The partition of this item; overrides the builder's `.partition()`. */
  partition?: string | number | null | undefined
  /** Dedup id; minted (UUIDv7) when absent. */
  transactionId?: string | undefined
  /** A UUID; dropped when it is not one. */
  traceId?: string | undefined
  headers?: MessageHeaders | null | undefined
}

/**
 * A pushed item as an envelope: `{ data }` or `{ payload }`, with its own
 * partition, transactionId, traceId and headers. A bare object that has
 * neither key is its own payload.
 */
export type PushEnvelope<T = any> =
  | (PushEnvelopeBase & { data: T })
  | (PushEnvelopeBase & { payload: T })

/** One pushed item: an envelope, or the payload object itself. */
export type PushItem<T = any> = PushEnvelope<T> | (T & object)

/** The broker's answer for one pushed item. */
export interface PushItemResult {
  /** `queued` and `buffered` mean stored; `duplicate` a known transactionId; anything else a rejection. */
  status: 'queued' | 'buffered' | 'duplicate' | 'error' | 'failed' | (string & {})
  transactionId?: string
  messageId?: string
  error?: string
  [key: string]: unknown
}

/** What a buffered push resolves to: the messages are in the client's buffer, not yet at the broker. */
export interface BufferedPushResult {
  buffered: true
  count: number
}

/** A formatted item as the push sends it, handed to the push callbacks. */
export interface FormattedPushItem {
  queue: string
  partition: string
  payload: unknown
  transactionId: string
  traceId?: string
  headers?: MessageHeaders
  /** The broker's answer for this item (immediate pushes only). */
  result?: PushItemResult
  /** Why the broker rejected it (`onError` of an immediate push). */
  error?: string
}

/** The error a rejected push throws: `results` is the broker's per-item answer, in input order. */
export interface PushError extends Error {
  results?: PushItemResult[]
}

/** Options of `QueueBuilder.buffer()`. */
export interface BufferOptions {
  /** Flush once this many messages are waiting (default 100). */
  messageCount?: number | undefined
  /** Flush this long after the first message arrives (default 1000). */
  timeMillis?: number | undefined
  /** Backpressure bound: past this many buffered messages a push waits (default 4 x messageCount). */
  maxSize?: number | undefined
  /** Delay before retrying a batch whose POST failed (default 250). */
  retryDelayMillis?: number | undefined
}

/** `queen.getBufferStats()`. */
export interface BufferStats {
  [key: string]: unknown
}

// ---------------------------------------------------------------------------
// Messages, pop and consume
// ---------------------------------------------------------------------------

/** What `message.trace()` takes. */
export interface TraceConfig {
  /** One name or several, to find the traces by. */
  traceName?: string | string[] | undefined
  /** `info` (default), `error`, `step`, `processing`, ... */
  eventType?: string | undefined
  data: JsonObject
}

/** What `message.trace()` resolves to. It never rejects. */
export interface TraceResult {
  success: boolean
  error?: string
  [key: string]: unknown
}

/** A popped or consumed message. */
export interface Message<T = any> {
  /** The message id (a UUID). */
  id: string
  transactionId: string
  /** The partition's id: what an ack names. */
  partitionId: string
  /** The partition's name. */
  partition: string
  /** The payload. */
  data: T
  /** ISO 8601 time the broker stored the message. */
  createdAt: string
  /** The lease the ack must match; empty with `commitOnDelivery()`. */
  leaseId?: string | null
  /** The consumer group it was popped under. */
  consumerGroup?: string | null
  traceId?: string | null
  /** The `sub` of the credential that pushed it, when the broker stamped one. */
  producerSub?: string | null
  /** 1 on the first delivery, then one more on each redelivery. */
  deliveryAttempt?: number
  /** Its offset in the partition (2.x brokers). */
  offset?: number
  /** Present only when the message has headers. */
  headers?: MessageHeaders
  queue?: string
  /** Record a trace event for this message (consumed messages only). Never rejects. */
  trace?: (config: TraceConfig) => Promise<TraceResult>
}

/** What `popResult()` reports about how the broker sized the pop (null when it did not). */
export interface AutopilotDecision {
  partitions: number
  batch: number
  waitMillis: number
}

/** `popResult()`. */
export interface PopResult<T = any> {
  messages: Message<T>[]
  autopilot: AutopilotDecision | null
}

/** Consumer supervision: observations in the broker dashboard. */
export interface SupervisionConfig {
  /** The application or deployment name: `[A-Za-z0-9][A-Za-z0-9._-]{0,254}`, not `coordination`. */
  group: string
}

/** The second argument of `consume()`. */
export interface ConsumeOptions {
  /** Abort to stop consuming: the handler in progress finishes and `consume()` resolves. */
  signal?: AbortSignal | undefined
  supervision?: SupervisionConfig | false | undefined
}

/** What a consume handler gets: one message with `.each()`, the popped array without it. */
export type ConsumeInput<T, Each extends boolean> = Each extends true ? Message<T> : Message<T>[]

/** A consume handler. Throwing nacks what it was given (unless `.onError()` is set). */
export type ConsumeHandler<T, Each extends boolean> = (input: ConsumeInput<T, Each>) => unknown

// ---------------------------------------------------------------------------
// Ack and lease renewal
// ---------------------------------------------------------------------------

/** An ack status. `true` is `completed` and `false` is `failed`. */
export type AckStatus = 'completed' | 'failed' | 'retry' | 'dlq'

interface AckTargetFields {
  /** Required: an ack without it is refused (it would not name one message). */
  partitionId: string
  leaseId?: string | null | undefined
  consumerGroup?: string | null | undefined
  /** In a batch: this item's status, over the call's. */
  _status?: AckStatus | boolean | undefined
  /** In a batch: this item's error, over the call's. */
  _error?: string | null | undefined
}

/**
 * What an ack names: a popped message, or an object with its
 * `transactionId` (or `id`) and `partitionId`; `leaseId` and
 * `consumerGroup` when it has them. A bare transactionId string is not
 * enough: without a `partitionId` the ack always fails.
 */
export type AckTarget =
  | (AckTargetFields & { transactionId: string; id?: string | undefined })
  | (AckTargetFields & { id: string; transactionId?: string | undefined })

/** The third argument of `queen.ack()`. */
export interface AckContext {
  /** The consumer group; default the one the message was popped under. */
  group?: string | null | undefined
  error?: string | null | undefined
}

/** The broker's answer for one ack. */
export interface AckItemResult {
  success: boolean
  error: string | null
  index?: number
  transactionId?: string
  queueName?: string
  partitionName?: string
  leaseReleased?: boolean
  dlq?: boolean
  [key: string]: unknown
}

/**
 * What `queen.ack()` resolves to for a batch. A rejected ack resolves
 * `success: false` (read it); a malformed item or mixed consumer groups
 * reject the call instead.
 */
export interface AckBatchResult {
  success: boolean
  processed?: number
  error?: string
  results?: AckItemResult[]
}

/** What `queen.ack()` resolves to for one message. */
export interface AckSingleResult {
  success: boolean
  error?: string | null
  [key: string]: unknown
}

/** One lease's answer to `queen.renew()`. */
export interface RenewResult {
  leaseId?: string
  success: boolean
  newExpiresAt?: string | null
  renewed?: number
  error?: string
}

// ---------------------------------------------------------------------------
// Dead letter queue and reads by offset
// ---------------------------------------------------------------------------

/** `queue.dlq().get()`. Resolves `{ messages: [], total: 0 }` on an error. */
export interface DLQResult {
  messages: JsonObject[]
  total: number
  [key: string]: unknown
}

/** One record of a read by offset. */
export interface FetchRecord<T = any> {
  offset: number
  transactionId: string
  payload: T
  /** The time the push that wrote it was stored. */
  ts: string
  headers?: MessageHeaders
}

/** One partition of a read by offset. */
export interface FetchedPartition<T = any> {
  partition: string
  records: FetchRecord<T>[]
  /** The next offset the partition will assign. */
  highWatermark: number
  /** The oldest offset retention kept. */
  logStartOffset: number
  /** Where to start next time to read only what is new. */
  nextOffset: number
}

/** Options of `queue.fetch()`. */
export interface FetchOptions {
  /** Start offsets by partition name (default 0, the oldest kept record). */
  offsets?: Record<string, number> | Map<string, number> | null | undefined
  /** Payload bytes of one call (default 8 MiB). */
  maxBytes?: number | undefined
}

/** One partition in `queue.changedSince()`. */
export interface ChangedPartition {
  name: string
  id: string | null
  /** Offset of its last record (-1 when none). */
  lastOffset: number
  logStart: number
  lastWriteAt: string
}

/** `queue.changedSince()`. */
export interface ChangedSinceResult {
  partitions: ChangedPartition[]
  /** Pass as the next `since`; from the broker's clock. */
  safeTime: string | null
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

/** The `{ consumerGroup }` (or `{ group }`) a transaction's ack or link takes. */
export interface TransactionGroupContext {
  consumerGroup?: string | null | undefined
  group?: string | null | undefined
}

/** Options of a transaction's `.queue(q).link()`. */
export interface LinkOptions extends TransactionGroupContext {
  /** The link's own headers, laid over the message's. */
  headers?: MessageHeaders | null | undefined
}

/**
 * A message as a link takes it: as a pop returned it, or a feed entry. Its
 * `offset` is required at runtime (a 2.x pop and the feed give one); a
 * message without it throws.
 */
export interface LinkableMessage {
  transactionId: string
  offset?: number | string | undefined
  partitionId?: string | number | undefined
  queue?: string | undefined
  partition?: string | undefined
  consumerGroup?: string | null | undefined
}

/** What `commit()` resolves to. A lost `once`/guard/`required` gate is `success: false`, not a throw. */
export interface TransactionResult {
  success: boolean
  /** `kv_precondition` for a lost gate. */
  reason?: string
  kvReason?: string
  failedIndex?: number
  error?: string
  results?: JsonObject[]
  value?: unknown
  version?: number
  [key: string]: unknown
}

/** The error `commit()` throws for every failure but a lost gate. */
export interface TransactionError extends Error {
  reason?: string
  result?: TransactionResult
}

// ---------------------------------------------------------------------------
// Opaque internals
// ---------------------------------------------------------------------------

/**
 * The client's internal HTTP client. Opaque: only `Queen` makes one. Typed
 * so the classes exported "for embedding" (`Admin`, `Ephemeral`, `Tokens`,
 * `Kv`, `Locks`) can name it in their constructors.
 */
export interface QueenHttpClient {
  get(path: string, ...rest: any[]): Promise<any>
  post(path: string, body?: unknown, ...rest: any[]): Promise<any>
  put(path: string, body?: unknown, ...rest: any[]): Promise<any>
  delete(path: string, ...rest: any[]): Promise<any>
}

/** The client's internal buffer manager. Opaque. */
export interface QueenBufferManager {
  addMessage(...args: any[]): Promise<unknown>
  flushBuffer(...args: any[]): Promise<unknown>
}
