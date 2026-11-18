/**
 * Queen Message Queue Client - Version 2
 * Clean, fluent API with smart defaults
 */

import type { Admin } from "./admin/Admin.js";
import type { QueueBuilder } from "./builders/QueueBuilder.js";
import type { TimerBuilder } from "./builders/TimerBuilder.js";
import type { TransactionBuilder } from "./builders/TransactionBuilder.js";
import type { Ephemeral } from "./ephemeral/Ephemeral.js";
import type { FeedOptions, FeedReader } from "./feed/Feed.js";
import type { Kv } from "./kv/Kv.js";
import type {
	Lock,
	LockHandleOptions,
	LockLifetime,
	Locks,
} from "./locks/Locks.js";
import type { StreamBuilder } from "./stream/StreamBuilder.js";
import type { StreamConsumer } from "./stream/StreamConsumer.js";
import type { Tokens } from "./tokens/Tokens.js";
import type {
	AckBatchResult,
	AckContext,
	AckSingleResult,
	AckStatus,
	AckTarget,
	BufferStats,
	QueenConfig,
	RenewResult,
} from "./types.js";

export declare class Queen {
	/**
	 * @param config - one broker URL, a list of URLs (load balanced), or the
	 *   full options (`url` or `urls` required).
	 */
	constructor(config?: string | string[] | QueenConfig);

	/**
	 * Whether pop autopilot is off for this client because the environment
	 * asked (QUEEN_SDK_POP_AUTOPILOT). A per-call `.autopilot(...)` still
	 * outranks it.
	 */
	get autopilotOff(): boolean;

	/** A queue builder. `T` is the payload of its messages (default `any`). */
	queue<T = any>(name?: string | null): QueueBuilder<T>;

	/** The Admin API (lazily initialized, singleton). */
	get admin(): Admin;

	/** Client tokens: mint a credential for one entity from a scoped key. */
	get tokens(): Tokens;

	/**
	 * Transactional key/value state, alongside the log.
	 *
	 *     const { won } = await queen.kv.once('idem', orderId, { ttl: '24h' })
	 *     const row = await queen.kv.get('saga', sagaId)   // {found, value, version, ...}
	 *
	 * Every write returns an OBJECT, and objects are always truthy: read
	 * `.applied`. An expiry is MANDATORY on every write.
	 */
	get kv(): Kv;

	/**
	 * A lock: one holder at a time, held as a lease, with a fencing token.
	 * Creating the handle sends nothing. A lifetime is mandatory:
	 * `ttl: '30s'` or `ttlSeconds: 30`.
	 */
	lock(
		name: string,
		opts: LockLifetime & LockHandleOptions & { limit?: 1 | undefined },
	): Lock;

	/** A semaphore: at most `limit` holders at a time. One handle is ONE permit. */
	semaphore(
		name: string,
		limit: number,
		opts: LockLifetime & LockHandleOptions,
	): Lock;

	/** The lock operations as the broker speaks them, with no state kept. */
	get locks(): Locks;

	/**
	 * RAM-class queues. CONTENTS SURVIVE NOTHING; a declared queue's options
	 * are durable. Requires broker/proxy >= 1.1.
	 */
	get ephemeral(): Ephemeral;

	/**
	 * Scheduled messages for one queue.
	 *
	 *     await queen.timer('orders').key(orderId).delay('30m')
	 *                .payload({ orderId }).schedule()
	 */
	timer(queueName: string): TimerBuilder;

	/** A transaction: acks, pushes, links, KV writes, guards and timers that commit together. */
	transaction(): TransactionBuilder;

	/**
	 * Every push to the tenant's queues, in log order:
	 *
	 *   for await (const e of queen.feed({ queues: ['orders*'], from: 'earliest', follow: false })) { ... }
	 */
	feed<T = any>(options?: FeedOptions): FeedReader<T>;

	/**
	 * Acknowledge a message. `true` is `completed`, `false` is `failed`. A
	 * rejected ack, a transport error and a message without `transactionId`
	 * or `partitionId` all resolve `success: false`.
	 */
	ack(
		message: AckTarget,
		status?: AckStatus | boolean,
		context?: AckContext,
	): Promise<AckSingleResult>;
	/**
	 * Acknowledge a batch. A rejected ack or a transport error resolves
	 * `success: false`, but the call REJECTS, before any request, when an
	 * item has no `transactionId` (or `id`) or no `partitionId`, or when the
	 * items come from different consumer groups and no `{ group }` is given.
	 */
	ack(
		message: AckTarget[],
		status?: AckStatus | boolean,
		context?: AckContext,
	): Promise<AckBatchResult>;

	/** Extend a lease: a message or a lease id. */
	renew(
		messageOrLeaseId: string | { leaseId?: string | null | undefined },
	): Promise<RenewResult>;
	/**
	 * Extend the leases of a list: one result per distinct lease. When no
	 * item carries a lease id (an empty list, or messages popped with
	 * `commitOnDelivery()`, whose `leaseId` is empty) it resolves ONE
	 * `{ success: false, error }` object, not an array: check
	 * `Array.isArray()` before iterating.
	 */
	renew(
		messagesOrLeaseIds: Array<string | { leaseId?: string | null | undefined }>,
	): Promise<RenewResult[] | RenewResult>;

	/** Send every buffered push now. */
	flushAllBuffers(): Promise<void>;
	getBufferStats(): BufferStats;

	/** Delete a consumer group and optionally its subscription metadata (default true). */
	deleteConsumerGroup(
		consumerGroup: string,
		deleteMetadata?: boolean,
	): Promise<unknown>;
	/** Update the subscription timestamp (ISO 8601) of a consumer group. */
	updateConsumerGroupTimestamp(
		consumerGroup: string,
		timestamp: string,
	): Promise<unknown>;

	/** Define a stream for windowed processing (legacy stream API; see `Stream`). */
	stream(name: string, namespace?: string): StreamBuilder;
	/** Create a consumer for a stream (legacy stream API). */
	consumer(streamName: string, consumerGroup: string): StreamConsumer;

	/** Register SIGINT/SIGTERM handlers that flush buffers before exit. */
	enableGracefulShutdown(): this;
	/** Remove the SIGINT/SIGTERM handlers. */
	disableGracefulShutdown(): this;

	/**
	 * Flush buffers (with a deadline), release held locks, remove the signal
	 * handlers and close the HTTP connections.
	 */
	close(): Promise<void>;
}
