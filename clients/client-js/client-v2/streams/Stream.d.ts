/**
 * Stream — fluent builder for streaming pipelines.
 *
 * A Stream is an immutable chain of operators that gets compiled and run by
 * a runner. Each combinator (`.map`, `.filter`, ...) returns a NEW Stream
 * with the operator appended; the chain is started by `.run({ queryId })`.
 *
 * Type parameters, following what the runtime hands each callback:
 * - `M`: what `map`, `filter`, `flatMap` and `keyBy` callbacks receive. That
 *   is the SOURCE message (a popped message, or a feed entry), whatever
 *   maps come before; after a `reduce`/`aggregate` it is the aggregate.
 *   Two maps in a row both see the source message.
 * - `V`: the value that flows to `reduce`, `aggregate`, `gate`, `to`,
 *   `foreach` and `foreachBatch`: the payload (`data`) at the start, then
 *   what the last `map`/`flatMap` returned.
 * - `S`: the source kind, `'queue'` (`Stream.from`) or `'feed'`
 *   (`Stream.fromFeed`): it decides the options of `run()` and its handle.
 */

import type { QueueBuilder } from "../builders/QueueBuilder.js";
import type { FeedEntry, FeedShardOption } from "../feed/Feed.js";
import type { Duration, Instant, Message, WhereOp } from "../types.js";

/** Where a stream reads from. */
export type StreamSourceKind = "queue" | "feed";

/**
 * The context a callback gets. Before a window it is undefined on a queue
 * source; after one it carries the window; `.foreach()` always gets one.
 */
export interface StreamContext {
	partition?: string;
	partitionId?: string;
	key?: string;
	windowKey?: string;
	/** Epoch milliseconds. */
	windowStart?: number;
	/** Epoch milliseconds. */
	windowEnd?: number;
	[key: string]: unknown;
}

/** The context of a `.gate()` callback. `state` is the per-key state, persisted when the message is allowed. */
export interface GateContext {
	state: Record<string, any>;
	/** System clock of the cycle, for refill math. */
	streamTimeMs: number;
	/** The source partition_id (= state shard). */
	partitionId: string;
	partition?: string;
	key?: string;
}

/** What a gate decides: let the message through, or halt the batch here. */
export type GateDecision = boolean | { allow: boolean };

/** A gate function, as `.gate()` and the rate-limiter helpers take it. */
export type GateFunction<V = any> = (
	value: V,
	ctx: GateContext,
) => GateDecision | Promise<GateDecision>;

/** The event-time options every window and stateful step takes. */
export interface EventTimeOptions<M> {
	/** Event-time mode: the message's own time (Date, epoch ms or ISO string); `false` keeps the feed's clock. */
	eventTime?: ((msg: M) => number | Date | string) | false | undefined;
	/** Event time: drop events older than watermark - allowedLateness (seconds, or a duration). */
	allowedLateness?: number | Duration | undefined;
	onLate?: "drop" | "include" | undefined;
	/** Feed, the first step on event time: after this long without a message the watermark moves on. */
	idle?: number | Duration | undefined;
}

interface WindowCommonOptions<M> extends EventTimeOptions<M> {
	/** Accept events for windowEnd + grace before closing, in seconds. */
	gracePeriod?: number | undefined;
	/** Close ripe windows on quiet partitions every N ms (default 5000). */
	idleFlushMs?: number | undefined;
}

export interface TumblingWindowOptions<M> extends WindowCommonOptions<M> {
	seconds: number;
}

export interface SlidingWindowOptions<M> extends WindowCommonOptions<M> {
	size: number;
	slide: number;
}

export interface SessionWindowOptions<M> extends WindowCommonOptions<M> {
	/** A session extends while events arrive within `gap` seconds of each other. */
	gap: number;
}

export interface CronWindowOptions<M> extends WindowCommonOptions<M> {
	every: "second" | "minute" | "hour" | "day" | "week";
}

/**
 * What `.reduce()` and `.aggregate()` fold: the value, except that an
 * `undefined` value (a `.map()` that returned nothing) folds the message
 * itself instead.
 */
export type ReduceInput<M, V> = undefined extends string
	? // strictNullChecks off: no type can tell an undefined value apart
		V
	: undefined extends V
		? Exclude<V, undefined> | M
		: V;

/** What `.aggregate()` produces for its extractors: `min`/`max` start at null, the rest at 0. */
export type AggregateResult<E> = {
	[K in keyof E]: K extends "min" | "max" ? number | null : number;
};

/** Options of `.pattern()`. Feed sources only. */
export interface PatternOptions<M, V> extends EventTimeOptions<M> {
	start: (msg: M, value: V) => boolean;
	then: (msg: M, value: V) => boolean;
	within: Duration;
	key?: ((msg: M, value: V) => unknown) | undefined;
	onMatch?: ((start: M, then: M) => unknown) | undefined;
	onTimeout?: ((start: M) => unknown) | undefined;
	onRepeat?: "keep" | "restart" | undefined;
}

/** Options of `.join()` with two sides. Feed sources only. */
export interface JoinOptions<M, V> extends EventTimeOptions<M> {
	left: (msg: M, value: V) => boolean;
	right: (msg: M, value: V) => boolean;
	within: Duration;
	key?: ((msg: M, value: V) => unknown) | undefined;
	select?: ((left: M, right: M) => unknown) | undefined;
	onTimeout?: ((msg: M, side: "left" | "right") => unknown) | undefined;
	onRepeat?: "keep" | "restart" | undefined;
}

/** Options of `.join()` with several sides. Feed sources only. */
export interface MultiJoinOptions<M> extends EventTimeOptions<M> {
	sides: Record<string, unknown> | unknown[];
	within?: Duration | undefined;
	[option: string]: unknown;
}

/** Options of `.gather()`. Feed sources only. */
export interface GatherOptions<M, V> extends EventTimeOptions<M> {
	/** The parts: queue names, or `{ name: (msg, value) => boolean }`. */
	steps?: string[] | Record<string, (msg: M, value: V) => boolean> | undefined;
	/** How many distinct messages a run has, instead of `steps`. */
	count?: ((msg: M, value: V) => number) | undefined;
	within: Duration;
	key?: ((msg: M, value: V) => unknown) | undefined;
	onComplete?: ((got: any, key: string) => unknown) | undefined;
	onTimeout?: ((got: any, missing: any, key: string) => unknown) | undefined;
	onRepeat?: "keep" | "latest" | undefined;
	keep?: ((...args: any[]) => unknown) | undefined;
}

/** Options of `.joinTable()`. Feed sources only. */
export interface JoinTableOptions<M> {
	/** The table queue(s): one message per change, null deletes. */
	queue: string | string[];
	on: (msg: M) => unknown;
	key?: ((row: any) => unknown) | undefined;
	value?: ((row: any) => unknown) | undefined;
	select?: ((msg: M, row: any) => unknown) | undefined;
	missing?: "keep" | "drop" | undefined;
	initial?: "all" | "now" | undefined;
}

/** A sink target as a function picks it: a queue (builder or name), `{ queue, partition }`, a list, or null. */
export type SinkTarget =
	| QueueBuilder<any, any, any>
	| string
	| {
			queue: QueueBuilder<any, any, any> | string;
			partition?: string | undefined;
	  };

/** Options of `.to()`. */
export interface SinkOptions<V> {
	partition?: string | ((value: V) => string) | undefined;
	/** `false` pushes copies instead of linking the source message. */
	link?: boolean | undefined;
}

/** One item of a `.foreachBatch()` batch. `id` is the same on every delivery of that result. */
export interface ForeachBatchItem<V> {
	value: V;
	ctx: StreamContext;
	id: string;
}

/** Options of `.foreachBatch()`. */
export interface ForeachBatchOptions {
	/** Default 1000. */
	maxItems?: number | undefined;
	/** Default 0: a batch goes at the end of each read. */
	maxWaitMillis?: number | undefined;
}

/** Options of `run()` on a queue source. */
export interface QueueStreamRunOptions {
	/** Durable identity of this query. */
	queryId: string;
	/** The broker URL the runner calls `/streams/v1/*` on. Required on a queue source. */
	url: string;
	bearerToken?: string | (() => string | Promise<string>) | null | undefined;
	/** Messages per cycle (default 200). */
	batchSize?: number | undefined;
	/** Lease up to N partitions per cycle (default 4). */
	maxPartitions?: number | undefined;
	/** Long-poll wait for the source pop (default 1000). */
	maxWaitMillis?: number | undefined;
	/** Unset, the broker's default applies ('new'): pass 'all' to process the backlog. */
	subscriptionMode?: "new" | "all" | undefined;
	/** ISO timestamp or 'now'. */
	subscriptionFrom?: string | undefined;
	/** Last-value delivery on the source pop. Requires broker >= 1.1.0. */
	conflation?: boolean | undefined;
	/** Wipe state on a chain change. */
	reset?: boolean | undefined;
	/** Cycle error hook. */
	onError?: ((err: Error, ctx: Record<string, unknown>) => unknown) | undefined;
	/** External cancellation. */
	abortSignal?: AbortSignal | undefined;
	logger?: unknown;
	/** Default `streams.<queryId>`. */
	consumerGroup?: string | undefined;
}

/** Options of `run()` on a feed source. */
export interface FeedStreamRunOptions {
	queryId: string;
	/** Where a NEW query starts. */
	from?: number | "now" | "earliest" | undefined;
	waitMillis?: number | undefined;
	maxRecords?: number | undefined;
	maxBytes?: number | undefined;
	/** `true` starts over at every start; a string starts over once per string. */
	reset?: boolean | string | undefined;
	onError?: ((err: Error, ctx: Record<string, unknown>) => unknown) | undefined;
	abortSignal?: AbortSignal | undefined;
	positionCommitMillis?: number | undefined;
	commitEveryMillis?: number | undefined;
	maxPendingOutputs?: number | undefined;
	/** One process runs a queryId at a time (default true). */
	exclusive?: boolean | undefined;
	lockTtl?: Duration | undefined;
	/** What a message that keeps failing does: 'stop' (default), or where it goes. */
	onPoison?: unknown;
	retries?: number | undefined;
	retryDelayMillis?: number | undefined;
	snapshotEveryMillis?: number | undefined;
	snapshotMaxEntries?: number | undefined;
	snapshotChunkChars?: number | undefined;
	snapshotPieceBytes?: number | undefined;
	/** Run every shard of the query in this process (2 to 4096). */
	shards?: number | undefined;
	/** Run one shard. */
	shard?: { index: number; count: number } | FeedShardOption | undefined;
	shardBy?: "partition" | ((msg: any) => unknown) | undefined;
	/** Answer `Stream.view()` questions (default true). */
	serveState?: boolean | undefined;
	serveWaitMillis?: number | undefined;
	now?: (() => number) | undefined;
}

/** Metrics of a queue-sourced runner. */
export interface QueueStreamMetrics {
	cyclesTotal: number;
	flushCyclesTotal: number;
	messagesTotal: number;
	pushItemsTotal: number;
	stateOpsTotal: number;
	lateEventsTotal: number;
	errorsTotal: number;
	lastCycleAt: string | null;
	lastFlushAt: string | null;
	lastError: unknown;
	queryId: string;
	serverQueryId: string | null;
}

/** The handle of a queue-sourced stream. */
export interface QueueStreamHandle {
	/** Graceful drain. */
	stop(): Promise<void>;
	metrics(): QueueStreamMetrics;
}

/** The live state of one feed step, in this process. */
export interface FeedStepState {
	get(key: string): any;
	list(limit?: number): any;
}

/** The handle of a feed-sourced stream (one runner, or a group of shards). */
export interface FeedStreamHandle {
	stop(): Promise<void>;
	/** Settles when the stream stops. */
	readonly done: Promise<unknown>;
	metrics(): Record<string, any>;
	/** The live state of one step: `get(key)` and `list(limit)`. */
	state(step: string): FeedStepState;
}

/** What `run()` resolves to, by source kind. */
export type StreamHandle<S extends StreamSourceKind> = S extends "feed"
	? FeedStreamHandle
	: QueueStreamHandle;

/** What `run()` takes, by source kind. */
export type StreamRunOptions<S extends StreamSourceKind> = S extends "feed"
	? FeedStreamRunOptions
	: QueueStreamRunOptions;

/** `Stream.view()`: ask a running feed stream about its live state, from any process. */
export interface FeedView {
	/** The value of one key in one step. */
	get(
		step: string,
		key: string | number,
	): Promise<{
		found: boolean;
		value?: any;
		shard?: number;
		position?: number;
		answered?: number;
	}>;
	/** Up to `limit` entries of one step, from every shard. */
	list(
		step: string,
		opts?: { limit?: number | undefined },
	): Promise<{ items: any[]; truncated: boolean; answered: number }>;
	/** Every shard's runner: role, position, lag, open state, per step. */
	status(): Promise<any[]>;
}

/** Options of `Stream.fromFeed()`. */
export interface FromFeedOptions {
	/** Names or patterns; default every queue. */
	queues?: string | string[] | undefined;
	queue?: string | string[] | undefined;
	partitions?: string | string[] | undefined;
	partition?: string | string[] | undefined;
	/** Where a NEW query starts (default 'now'). */
	from?: number | "now" | "earliest" | undefined;
	/** A new query starts at messages created since ('1d', ISO, Date). */
	since?: Instant | undefined;
	/** Nodes to read from first. */
	followerUrls?: string[] | undefined;
	[option: string]: unknown;
}

/** The message a queue-sourced stream's callbacks receive. */
export type QueueStreamMessage<T> = Message<T>;

export declare class Stream<
	M = any,
	V = any,
	S extends StreamSourceKind = StreamSourceKind,
> {
	/**
	 * Build a Stream sourced from the tenant feed: every push to the tenant's
	 * queues (or the ones named), in log order.
	 */
	static fromFeed<T = any>(
		queen: unknown,
		options?: FromFeedOptions,
	): Stream<FeedEntry<T>, T, "feed">;
	/** Build a Stream sourced from a Queen QueueBuilder: `queen.queue('name')`. */
	static from<T = any>(
		queueBuilder: QueueBuilder<T, any, any>,
		options?: Record<string, unknown>,
	): Stream<Message<T>, T, "queue">;
	/** Ask a running feed stream about its live state, from any process. */
	static view(
		queen: unknown,
		queryId: string,
		opts?: { shards?: number | undefined; timeoutMillis?: number | undefined },
	): FeedView;

	constructor(args: {
		source: unknown;
		operators: unknown[];
		sourceOptions?: Record<string, unknown>;
	});

	// ------------------------------------------------------------- stateless

	/**
	 * Transform each record. `fn` gets the source message (or the aggregate
	 * after a reduce). Returning `undefined` means "no value": a later
	 * `.reduce()` or `.aggregate()` folds the message instead.
	 */
	map<U>(
		fn: (msg: M, ctx: StreamContext | undefined) => U | Promise<U>,
	): Stream<M, U, S>;
	/** Drop records. The predicate sees the source message, not a mapped value. */
	filter(
		predicate: (
			msg: M,
			ctx: StreamContext | undefined,
		) => boolean | Promise<boolean>,
	): Stream<M, V, S>;
	/**
	 * Keep only the source messages where `field op value` holds. On a feed
	 * source the condition is filtered by the broker.
	 */
	where(field: string, op: WhereOp, value?: unknown): Stream<M, V, S>;
	/** Emit zero or more values per record. */
	flatMap<U>(
		fn: (msg: M, ctx: StreamContext | undefined) => U[] | Promise<U[]>,
	): Stream<M, U, S>;

	// ---------------------------------------------------------------- keying

	/** Override the partition key for the stateful operators that follow. */
	keyBy(fn: (msg: M) => unknown): Stream<M, V, S>;

	// --------------------------------------------------------------- windows

	/** Fixed-size, non-overlapping windows. */
	windowTumbling(opts: TumblingWindowOptions<M>): Stream<M, V, S>;
	/** Overlapping fixed-size windows that hop every `slide` seconds. */
	windowSliding(opts: SlidingWindowOptions<M>): Stream<M, V, S>;
	/** Per-key activity-based windows. */
	windowSession(opts: SessionWindowOptions<M>): Stream<M, V, S>;
	/** Wall-clock-aligned windows. */
	windowCron(opts: CronWindowOptions<M>): Stream<M, V, S>;

	// ---------------------------------------------------------------- reduce

	/** Fold each window's values (an `undefined` value folds the message: see `ReduceInput`). */
	reduce<A>(
		fn: (acc: A, value: ReduceInput<M, V>) => A | Promise<A>,
		initial: A,
	): Stream<A, A, S>;
	/**
	 * Sugar over reduce: each extractor produces one named field
	 * (`count`, `sum`, `min`, `max`, `avg`, or a custom running total).
	 */
	aggregate<
		E extends Record<
			string,
			(
				value: ReduceInput<M, V>,
			) => number | null | undefined | Promise<number | null | undefined>
		>,
	>(extractors: E): Stream<AggregateResult<E>, AggregateResult<E>, S>;

	// -------------------------------------------------------------- patterns

	/** A `start` message, then a `then` message for the same entity within `within`. Feed sources only. */
	pattern(opts: PatternOptions<M, V>): Stream<any, any, S>;
	/** A `left` and a `right` message for the same entity, in either order. Feed sources only. */
	join(opts: JoinOptions<M, V> | MultiJoinOptions<M>): Stream<any, any, S>;
	/** The parts of one run, collected until all are in or the deadline passes. Feed sources only. */
	gather(opts: GatherOptions<M, V>): Stream<any, any, S>;
	/** Join every message with the latest row of a table queue. Feed sources only. */
	joinTable(opts: JoinTableOptions<M>): Stream<any, any, S>;
	/** Name the stateful step before this call, for live reads. */
	name(name: string): Stream<M, V, S>;

	// ------------------------------------------------------------------ gate

	/**
	 * Per-message ALLOW/DENY decision with persistent per-key state. Return
	 * `false` to halt the batch here: the denied message and its successors
	 * are redelivered in order.
	 */
	gate(fn: GateFunction<V>): Stream<M, V, S>;

	// ------------------------------------------------------------------ sink

	/**
	 * Sink to Queen queues. The cycle commits state + push + ack atomically.
	 * Several queues and functions run on `Stream.fromFeed` only.
	 */
	to(
		target:
			| QueueBuilder<any, any, any>
			| string
			| Array<QueueBuilder<any, any, any> | string>
			| ((value: V, msg: M) => SinkTarget | SinkTarget[] | null | undefined),
		opts?: SinkOptions<V>,
	): Stream<M, V, S>;
	/** Terminal at-least-once side effect. */
	foreach(fn: (value: V, ctx: StreamContext) => unknown): Stream<M, V, S>;
	/** Terminal at-least-once side effect, in batches (`Stream.fromFeed` only). */
	foreachBatch(
		fn: (items: ForeachBatchItem<V>[]) => unknown,
		opts?: ForeachBatchOptions,
	): Stream<M, V, S>;

	// ------------------------------------------------------------------- run

	/** Start the streaming runner. */
	run(runOptions: StreamRunOptions<S>): Promise<StreamHandle<S>>;
}
