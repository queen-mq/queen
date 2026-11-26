/**
 * QueenMQ Client - Entry Point
 *
 * One npm package for everything:
 *   import { Queen, Stream, tokenBucketGate, slidingWindowGate } from 'queen-mq'
 *
 * The broker client (`Queen`, `Admin`) and the fluent streaming SDK
 * (`Stream`, `tokenBucketGate`, `slidingWindowGate`) ship in the same
 * package. The types below are declarations only: `import type` them.
 */

export { Admin } from './admin/Admin.js'
export {
  EPHEMERAL_QUEUE_NOT_FOUND,
  EPHEMERAL_UNSUPPORTED,
  Ephemeral
} from './ephemeral/Ephemeral.js'
export { FeedReader, sinceToIso, whereCondition } from './feed/Feed.js'
export { LOCK_NOT_HELD, Lock, Locks } from './locks/Locks.js'
export { Queen } from './Queen.js'
export {
  slidingWindowGate,
  tokenBucketGate
} from './streams/helpers/rateLimiter.js'
export { Stream } from './streams/Stream.js'
export { Tokens } from './tokens/Tokens.js'
export { CONFLATION_UNSUPPORTED } from './utils/conflation.js'
export {
  BUFFER_DEFAULTS,
  CLIENT_DEFAULTS,
  CONSUME_DEFAULTS,
  POP_DEFAULTS,
  QUEUE_DEFAULTS
} from './utils/defaults.js'
export {
  FILTERED_GROUPS_UNSUPPORTED,
  READING_QUEUES_UNSUPPORTED
} from './utils/routing.js'
export {
  CLIENT_TOKEN_PREFIX,
  expiresAtMs,
  TokenSource
} from './utils/tokenSource.js'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type {
  AdminQueryParams,
  QueueDepth,
  SeekOptions
} from './admin/Admin.js'
export type {
  ConsumeBuilder,
  DLQBuilder,
  OperationBuilder,
  PushBuilder,
  QueueBuilder
} from './builders/QueueBuilder.js'
export type {
  TimerBuilder,
  TimerCancelResult,
  TimerListResult,
  TimerPeekResult,
  TimerScheduleResult,
  TransactionTimerBuilder
} from './builders/TimerBuilder.js'
export type {
  TransactionBuilder,
  TransactionKv,
  TransactionQueue
} from './builders/TransactionBuilder.js'
export type {
  EphemeralAckOptions,
  EphemeralAckResult,
  EphemeralAckStatus,
  EphemeralAckTarget,
  EphemeralConfigureOptions,
  EphemeralMessage,
  EphemeralPopOptions,
  EphemeralPushOptions
} from './ephemeral/Ephemeral.js'
export type {
  FeedEntry,
  FeedOptions,
  FeedPage,
  FeedShardOption
} from './feed/Feed.js'
export type {
  Kv,
  KvCheckOptions,
  KvDeleteOptions,
  KvExpiryOptions,
  KvGetManyResult,
  KvGetPrefixOptions,
  KvGetPrefixResult,
  KvIncrOptions,
  KvOnceOptions,
  KvOnceResult,
  KvPutIfAbsentOptions,
  KvPutOptions,
  KvRow,
  KvWriteResult
} from './kv/Kv.js'
export type {
  LockAcquireOptions,
  LockAcquireResult,
  LockGetResult,
  LockHandleOptions,
  LockHolder,
  LockLifetime,
  LockLostReason,
  LockOperation,
  LockOptions,
  LockReleaseResult,
  LockRenewResult
} from './locks/Locks.js'
export type { StreamBuilder } from './stream/StreamBuilder.js'
export type { StreamConsumer } from './stream/StreamConsumer.js'
export type { Window } from './stream/Window.js'
export type {
  SlidingWindowGateOptions,
  TokenBucketGateOptions
} from './streams/helpers/rateLimiter.js'
export type {
  AggregateResult,
  CronWindowOptions,
  EventTimeOptions,
  FeedStreamHandle,
  FeedStreamRunOptions,
  FeedView,
  ForeachBatchItem,
  ForeachBatchOptions,
  FromFeedOptions,
  GateContext,
  GateDecision,
  GateFunction,
  GatherOptions,
  JoinOptions,
  JoinTableOptions,
  MultiJoinOptions,
  PatternOptions,
  QueueStreamHandle,
  QueueStreamMetrics,
  QueueStreamRunOptions,
  ReduceInput,
  SessionWindowOptions,
  SinkOptions,
  SinkTarget,
  SlidingWindowOptions,
  StreamContext,
  StreamHandle,
  StreamRunOptions,
  StreamSourceKind,
  TumblingWindowOptions
} from './streams/Stream.js'
export type { MintedToken, MintTokenOptions } from './tokens/Tokens.js'
export type {
  AckBatchResult,
  AckContext,
  AckItemResult,
  AckSingleResult,
  AckStatus,
  AckTarget,
  AutopilotDecision,
  BufferedPushResult,
  BufferOptions,
  BufferStats,
  ChangedPartition,
  ChangedSinceResult,
  ConsumeHandler,
  ConsumeInput,
  ConsumeOptions,
  DLQResult,
  Duration,
  FetchedPartition,
  FetchOptions,
  FetchRecord,
  FormattedPushItem,
  Instant,
  JsonObject,
  LinkableMessage,
  LinkOptions,
  LoadBalancingStrategy,
  Message,
  MessageHeaders,
  PopResult,
  PushEnvelope,
  PushError,
  PushItem,
  PushItemResult,
  QueenConfig,
  QueenLogger,
  QueueConfig,
  QueueConfigureResult,
  QueueDeleteResult,
  ReadingQueueSource,
  RenewResult,
  Retry429Options,
  SubscriptionMode,
  SupervisionConfig,
  TokenFunction,
  TraceConfig,
  TraceResult,
  TransactionError,
  TransactionGroupContext,
  TransactionResult,
  WhereCondition,
  WhereConditionInput,
  WhereConditions,
  WhereInput,
  WhereOp
} from './types.js'
