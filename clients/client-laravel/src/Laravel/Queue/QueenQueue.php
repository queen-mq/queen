<?php

namespace Queen\Laravel\Queue;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Events\WorkerStopping;
use Illuminate\Queue\InvalidPayloadException;
use Illuminate\Queue\Queue as BaseQueue;
use JsonException;
use Queen\Exceptions\HttpException;
use Queen\Laravel\Contracts\QueenPartitionable;
use Queen\Queen;
use Queen\Support\Uuid;
use RuntimeException;
use UnexpectedValueException;

class QueenQueue extends BaseQueue implements QueueContract
{
    /** @var array<string, array{messages: list<array>, next: int}> */
    private array $prefetched = [];

    /** @var array<string, string> Delivery key to local pop-batch id. */
    private array $deliveryBatches = [];

    /** @var array<string, string> Queue name to the delivery currently handed to Laravel. */
    private array $activeDeliveries = [];

    /** @var array<string, string> Delivery key to its Laravel queue name. */
    private array $deliveryQueues = [];

    /** @var array<string, int> Number of jobs from a pop batch not handled yet. */
    private array $batchOutstanding = [];

    /** @var list<array{message: array, group: string, affinity_key: ?string}> */
    private array $pendingAcknowledgements = [];

    /** @var array<string, int> Locally unsettled deliveries per broker lease. */
    private array $leaseOutstanding = [];

    private int $nextBatchId = 0;

    private bool $workerStoppingListenerRegistered = false;

    private bool $shutDown = false;

    /**
     * @param (\Closure(string, \Closure(): mixed): mixed)|null $failedJobRetryHandler
     * @param (\Closure(list<array>, string, ?string): array)|null $shutdownTailReleaser
     */
    public function __construct(
        private Queen $queen,
        private string $defaultQueue = 'default',
        private string $consumerGroup = 'laravel',
        private int $partitionCount = 64,
        private string $partitionPrefix = 'laravel',
        private int $retryAfter = 90,
        private int $blockFor = 0,
        bool $dispatchAfterCommit = false,
        private int $prefetch = 1,
        private int $ackBatch = 1,
        private int $bulkBatch = 100,
        private ?LeaseRenewer $leaseRenewer = null,
        private ?\Closure $failedJobRetryHandler = null,
        private ?\Closure $shutdownTailReleaser = null,
    ) {
        $this->dispatchAfterCommit = $dispatchAfterCommit;
    }

    public function __destruct()
    {
        $this->shutdown();
    }

    /**
     * Laravel resolves a connection after WorkerStarting, so the queue itself
     * registers the stop hook as soon as QueueManager injects the container.
     */
    public function setContainer(Container $container): void
    {
        parent::setContainer($container);

        if ($this->workerStoppingListenerRegistered || !$container->bound('events')) {
            return;
        }

        $container->make('events')->listen(
            WorkerStopping::class,
            function (WorkerStopping $event): void {
                $this->shutdown();
            },
        );
        $this->workerStoppingListenerRegistered = true;
    }

    /**
     * Settle completed deferred ACKs and explicitly retry one representative
     * from every unhandled prefetched partition. The connector supplies a
     * single-attempt, two-second client for this final request. Any skipped,
     * rejected, or ambiguous item falls back to durable lease expiry.
     */
    public function shutdown(): void
    {
        if ($this->shutDown) {
            return;
        }
        $this->shutDown = true;

        try {
            $groups = $this->shutdownAcknowledgementGroups();
            if ($groups !== []) {
                // A synchronous Laravel worker can own a prefetched tail for
                // only one queue. Limit this best-effort path to one HTTP call
                // even if direct, re-entrant API use created several groups.
                $entries = reset($groups);
                $messages = array_column($entries, 'wire');
                $group = $entries[0]['group'];
                $affinityKey = $entries[0]['affinity_key'];
                $result = $this->shutdownTailReleaser !== null
                    ? ($this->shutdownTailReleaser)($messages, $group, $affinityKey)
                    : $this->queen->ack($messages, true, array_filter([
                        'group' => $group,
                        'affinityKey' => $affinityKey,
                    ], static fn (mixed $value): bool => $value !== null));
                $this->assertBatchAcknowledged($result, count($messages));

                foreach ($entries as $entry) {
                    if ($entry['type'] === 'completed') {
                        $this->settleLeaseMessage($entry['message']);
                    } else {
                        $this->discardPrefetchedSiblings($entry['message']);
                    }
                }
            }
        } catch (\Throwable $exception) {
            // A shutdown ACK is deliberately best effort. Its transaction may
            // be ambiguous, so never retry it locally; expiry/redelivery is the
            // only safe at-least-once fallback.
            error_log('Queen Laravel worker could not release its prefetched tail during shutdown: '
                . $exception->getMessage());
        } finally {
            $this->abandonUnsettledLocalState();
            $this->leaseRenewer?->close();
        }
    }

    public function size($queue = null): int
    {
        $depth = $this->depth($queue);

        // Laravel's size() includes ready and reserved jobs. Queen's `pending`
        // is exactly that total durable depth because a lease does not advance
        // the consumer cursor until it is acknowledged.
        return ($this->nonNegativeCount($depth, 'pending') ?? 0) + $this->delayedSize($queue);
    }

    public function pendingSize($queue = null): int
    {
        $depth = $this->depth($queue);

        // New brokers separate work that a worker can claim now from live
        // leases. Preserve the previous fallback order during rolling broker
        // upgrades, where the same endpoint may not expose `ready` yet.
        return $this->nonNegativeCount($depth, 'ready')
            ?? $this->nonNegativeCount($depth, 'effectivePending')
            ?? $this->nonNegativeCount($depth, 'pending')
            ?? 0;
    }

    /** Count Laravel-owned pending timers. This is not used by pop(). */
    public function delayedSize($queue = null): int
    {
        return $this->queen->timers()->count($this->getQueue($queue), 'laravel:');
    }

    public function reservedSize($queue = null): int
    {
        $depth = $this->depth($queue);
        $processing = $this->nonNegativeCount($depth, 'processing');
        if ($processing !== null) {
            return $processing;
        }

        // An empty depth is the queue-not-created-yet case handled by depth().
        // Do not turn a harmless monitoring probe into a second 404.
        if ($depth === []) {
            return 0;
        }

        // Rolling-safe fallback for brokers that predate group-scoped lease
        // metrics. Queue detail is queue-wide rather than group-scoped, but it
        // is the same conservative value this driver exposed historically.
        $detail = $this->queen->admin()->getQueueDetail($this->getQueue($queue));
        if (!is_array($detail)) {
            return 0;
        }

        $totals = is_array($detail['totals'] ?? null) ? $detail['totals'] : [];
        $messages = is_array($totals['messages'] ?? null) ? $totals['messages'] : [];

        return $this->nonNegativeCount($messages, 'processing')
            ?? $this->nonNegativeCount($totals, 'processing')
            ?? 0;
    }

    /**
     * Best-effort creation time of the oldest claimable job.
     *
     * Depth can prove there is no ready work without touching segments. When
     * work is ready, however, it carries no timestamps: queue detail's
     * `oldestMessage` is computed from the queue-wide worst cursor and can be
     * older than this group's first unleased job. Filtering it to partitions
     * with group-scoped ready work makes it a conservative approximation, not
     * an exact group-scoped age. Older brokers lack ready fields entirely and
     * retain the same queue-detail approximation used by previous clients.
     */
    public function creationTimeOfOldestPendingJob($queue = null): ?int
    {
        $depth = $this->depth($queue);
        if ($depth === []) {
            return null;
        }

        $ready = $this->nonNegativeCount($depth, 'ready');
        if ($ready === 0) {
            return null;
        }

        $readyPartitions = $ready !== null ? $this->readyPartitionNames($depth) : null;
        $detail = $this->queen->admin()->getQueueDetail($this->getQueue($queue));
        if (!is_array($detail)) {
            return null;
        }

        $partitions = $detail['partitions'] ?? null;
        if (!is_array($partitions)) {
            return null;
        }

        $oldest = null;

        foreach ($partitions as $partition) {
            if (!is_array($partition)) {
                continue;
            }

            if ($readyPartitions !== null) {
                $name = $partition['name'] ?? $partition['partition'] ?? null;
                if (!is_string($name) || !isset($readyPartitions[$name])) {
                    continue;
                }
            } else {
                // Old depth contract: queue detail is the only available
                // indication that a partition may contain claimable work.
                $messages = is_array($partition['messages'] ?? null) ? $partition['messages'] : [];
                $stats = is_array($partition['stats'] ?? null) ? $partition['stats'] : [];
                $pending = $this->nonNegativeCount($messages, 'pending')
                    ?? $this->nonNegativeCount($stats, 'pending')
                    ?? $this->nonNegativeCount($partition, 'pending')
                    ?? 0;
                if ($pending < 1) {
                    continue;
                }
            }

            $value = $partition['oldestMessage'] ?? null;
            $timestamp = is_string($value) ? strtotime($value) : false;
            if ($timestamp !== false && ($oldest === null || $timestamp < $oldest)) {
                $oldest = $timestamp;
            }
        }

        return $oldest;
    }

    public function push($job, $data = '', $queue = null): mixed
    {
        $queue = $this->getQueue($queue);

        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $queue, $data),
            $queue,
            null,
            fn ($payload, $queue) => $this->pushRaw($payload, $queue),
        );
    }

    public function pushRaw($payload, $queue = null, array $options = []): string
    {
        $queue = $this->getQueue($queue);
        $decoded = $this->decodePayload($payload);
        $partition = (string) ($options['partition'] ?? $this->partitionForPayload($decoded));
        $decoded['_queen'] = array_replace(
            ['attempts' => 0],
            is_array($decoded['_queen'] ?? null) ? $decoded['_queen'] : [],
            ['partition' => $partition],
        );
        $jobId = (string) ($decoded['uuid'] ?? Uuid::v7());
        $manualRetryId = $decoded['_queen']['manual_retry'] ?? null;
        $failedSource = $decoded['_queen']['failed_source'] ?? null;
        $retryFence = $decoded['_queen']['retry_fence'] ?? null;
        $manualRetry = (is_string($manualRetryId) && $manualRetryId !== '') || $manualRetryId === true;
        if ($manualRetry) {
            // Laravel's queue:retry republishes the failed payload verbatim.
            // A fresh Queen transaction ID bypasses the original dispatch's
            // dedup record, while attempts must restart from one.
            $decoded['_queen']['attempts'] = 0;
            unset(
                $decoded['_queen']['manual_retry'],
                $decoded['_queen']['failed_source'],
                $decoded['_queen']['retry_fence'],
            );
        }

        $publish = function () use (
            $queue,
            $partition,
            $decoded,
            $manualRetry,
            $manualRetryId,
            $jobId,
        ): void {
            $result = $this->queen->queue($queue)
                ->partition($partition)
                ->push([['data' => $decoded, 'transactionId' => $manualRetry
                    ? (is_string($manualRetryId) ? $manualRetryId : Uuid::v7())
                    : $jobId]])
                ->execute();

            $this->assertPushAccepted($result);
        };

        $cleanupFailedSource = function () use ($manualRetry, $failedSource): void {
            if ($manualRetry && is_array($failedSource)) {
                $this->deleteFailedSource($failedSource);
            }
        };

        if ($manualRetry && $this->failedJobRetryHandler !== null) {
            if ($retryFence === null) {
                throw new RuntimeException(
                    'The Queen failed-job retry fence is missing; select the job through Laravel queue:retry.',
                );
            }
            if (!is_string($retryFence) || preg_match('/^[0-9a-f]{64}$/D', $retryFence) !== 1) {
                throw new RuntimeException('The Queen failed-job retry fence is malformed.');
            }
            ($this->failedJobRetryHandler)(
                $retryFence,
                function () use ($cleanupFailedSource, $publish): void {
                    // Retire the old DLQ snapshot before making the retried
                    // job visible. If publication then fails or is ambiguous,
                    // the durable Laravel row and stable retry transaction ID
                    // make the hand-off repeatable. Once publish succeeds, the
                    // provider has no further network call before deleting the
                    // old row and releasing the lock, so an immediately failing
                    // new generation is not held behind broker I/O.
                    $cleanupFailedSource();
                    $publish();
                },
            );
        } else {
            // Standalone client use has no synchronized failed-store row. Keep
            // the legacy order and only remove a supplied snapshot after Queen
            // has accepted the retry.
            $publish();
            $cleanupFailedSource();
        }

        return $jobId;
    }

    /**
     * Push immediate Laravel jobs in bounded Queen batches.
     *
     * Jobs with a delay or after-commit contract retain Laravel's ordinary
     * one-at-a-time path because their scheduling/transaction boundary is part
     * of the job contract, not a transport optimization.
     */
    public function bulk($jobs, $data = '', $queue = null): void
    {
        $jobs = array_values((array) $jobs);
        if ($jobs === []) {
            return;
        }

        $requiresIndividualDispatch = false;
        foreach ($jobs as $job) {
            $requiresIndividualDispatch = $requiresIndividualDispatch
                || isset($job->delay)
                || $this->shouldDispatchAfterCommit($job);
        }
        if ($requiresIndividualDispatch) {
            foreach ($jobs as $job) {
                if (isset($job->delay)) {
                    $this->later($job->delay, $job, $data, $queue);
                } else {
                    $this->push($job, $data, $queue);
                }
            }
            return;
        }

        $queue = $this->getQueue($queue);
        foreach (array_chunk($jobs, $this->bulkBatch) as $jobChunk) {
            $chunk = [];
            foreach ($jobChunk as $job) {
                $payload = $this->createPayload($job, $queue, $data);
                $decoded = $this->decodePayload($payload);
                $partition = $this->partitionForPayload($decoded);
                $decoded['_queen'] = array_replace(
                    ['attempts' => 0],
                    is_array($decoded['_queen'] ?? null) ? $decoded['_queen'] : [],
                    ['partition' => $partition],
                );
                $jobId = (string) ($decoded['uuid'] ?? Uuid::v7());

                $chunk[] = [
                    'job' => $job,
                    'payload' => $payload,
                    'job_id' => $jobId,
                    'item' => [
                        'data' => $decoded,
                        'partition' => $partition,
                        'transactionId' => $jobId,
                    ],
                ];
                $this->raiseJobQueueingEvent($queue, $job, $payload, null);
            }

            $items = array_column($chunk, 'item');
            $result = $this->queen->queue($queue)
                ->partition($items[0]['partition'])
                ->push($items)
                ->execute();

            $this->assertBulkPushAccepted($result, count($chunk));

            foreach ($chunk as $entry) {
                $this->raiseJobQueuedEvent(
                    $queue,
                    $entry['job_id'],
                    $entry['job'],
                    $entry['payload'],
                    null,
                );
            }
        }
    }

    public function later($delay, $job, $data = '', $queue = null): mixed
    {
        $queue = $this->getQueue($queue);

        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $queue, $data, $delay),
            $queue,
            $delay,
            fn ($payload, $queue, $delay) => $this->laterRaw($delay, $payload, $queue),
        );
    }

    public function pop($queue = null): ?QueenJob
    {
        if ($this->shutDown) {
            throw new RuntimeException('Queen Laravel queue connection cannot pop after worker shutdown began.');
        }

        $queue = $this->getQueue($queue);
        if ($this->prefetch > 1 && isset($this->activeDeliveries[$queue])) {
            throw new RuntimeException(
                "Queen Laravel prefetch cannot pop [{$queue}] again before the current job is deleted or released.",
            );
        }

        $message = $this->takePrefetched($queue);
        if ($message !== null) {
            return $this->makeJob($message, $queue);
        }

        // QueueBuilder gives the HTTP request a further 5 s of slack. For a
        // non-blocking Laravel worker retain the normal 30 s request budget
        // instead of producing a pathological timeout=1 query.
        $pollTimeoutMillis = $this->blockFor > 0 ? $this->blockFor * 1000 : 30_000;
        $builder = $this->queen->queue($queue)
            ->group($this->consumerGroup)
            ->conflation(false)
            ->subscriptionMode('all')
            ->batch($this->prefetch)
            ->partitions($this->partitionCount)
            ->autoAck(false)
            ->leaseSeconds($this->retryAfter)
            ->wait($this->blockFor > 0)
            ->timeoutMillis($pollTimeoutMillis);

        // The lease is created inside this request. Starting the local deadline
        // before the request can only fence early (especially with long-poll),
        // never let a helper renew past an already-expired broker lease.
        $popStartedMillis = self::monotonicMillis();
        $messages = array_values($builder->pop());
        if ($messages === []) {
            return null;
        }

        if ($this->leaseRenewer !== null) {
            if ($this->retryAfter > intdiv(PHP_INT_MAX - $popStartedMillis, 1000)) {
                throw new RuntimeException('Queen Laravel retry_after is too large for a monotonic lease deadline.');
            }
            $this->registerLeaseRenewal(
                $messages,
                $popStartedMillis + $this->retryAfter * 1000,
            );
        }

        // Batch accounting exists only to know when deferred ACKs must flush.
        // The production-safe synchronous path handles every delivery before
        // returning to pop(), so allocating keys/counters for it is pure hot-
        // path overhead.
        if ($this->ackBatch > 1) {
            $this->registerPopBatch($messages);
        }
        if (count($messages) > 1) {
            $this->prefetched[$queue] = ['messages' => $messages, 'next' => 1];
        }

        return $this->makeJob($messages[0], $queue);
    }

    private function makeJob(array $message, string $queue): QueenJob
    {
        if ($this->leaseRenewer !== null) {
            $leaseId = $this->leaseId($message);
            try {
                $this->leaseRenewer->assertHealthy($leaseId);
            } catch (\Throwable $renewalFailure) {
                $this->abandonLease($message);
                throw $renewalFailure;
            }
        }

        if ($this->prefetch > 1) {
            $key = $this->deliveryKey($message);
            $this->activeDeliveries[$queue] = $key;
            $this->deliveryQueues[$key] = $queue;
        }

        $job = new QueenJob(
            $this->container,
            $this,
            $message,
            $this->connectionName,
            $queue,
            $this->consumerGroup,
        );

        try {
            $this->assertJobTimeoutIsSafe($job);
        } catch (\Throwable $timeoutFailure) {
            if ($this->leaseRenewer !== null) {
                $this->abandonLease($message);
            } else {
                $this->discardPrefetchedSiblings($message);
                $this->markDeliveryHandled($message);
            }
            throw $timeoutFailure;
        }

        return $job;
    }

    public function deleteReserved(
        array $message,
        string $group,
        bool $failed = false,
        ?\Throwable $exception = null,
        ?string $queue = null,
    ): void
    {
        $affinityKey = $queue !== null ? "{$queue}:Default:{$group}" : null;
        if (!$failed && $this->ackBatch > 1) {
            $this->pendingAcknowledgements[] = [
                'message' => $message,
                'group' => $group,
                'affinity_key' => $affinityKey,
            ];
            $batchComplete = $this->markDeliveryHandled($message);

            if (count($this->pendingAcknowledgements) >= $this->ackBatch || $batchComplete) {
                $this->flushAcknowledgements();
            }
            return;
        }

        try {
            // A failed job must reach the DLQ synchronously. Flush earlier
            // success acknowledgements first so a later batch failure cannot
            // obscure it. A DLQ transition invalidates same-partition tails.
            if ($failed) {
                $this->flushAcknowledgements();
                $this->discardPrefetchedSiblings($message);
            }

            $result = $this->queen->ack(
                $message,
                $failed ? 'dlq' : 'completed',
                array_filter([
                    'group' => $group,
                    'error' => $exception?->getMessage(),
                    'affinityKey' => $affinityKey,
                ], fn ($value) => $value !== null),
            );

            $this->assertSuccessful($result, $failed ? 'dead-letter job' : 'acknowledge job');
        } catch (\Throwable $acknowledgementFailure) {
            if ($this->leaseRenewer !== null) {
                $this->abandonLease($message);
            } else {
                $this->discardPrefetchedSiblings($message);
                $this->markDeliveryHandled($message);
            }
            throw $acknowledgementFailure;
        }
        $this->markDeliveryHandled($message);
        $this->settleLeaseMessage($message);
    }

    /** Flush successful ACKs deferred by ack_batch. */
    public function flushAcknowledgements(): void
    {
        while ($this->pendingAcknowledgements !== []) {
            $group = $this->pendingAcknowledgements[0]['group'];
            $affinityKey = $this->pendingAcknowledgements[0]['affinity_key'];
            $count = 0;
            $messages = [];
            foreach ($this->pendingAcknowledgements as $entry) {
                if ($entry['group'] !== $group || $entry['affinity_key'] !== $affinityKey) {
                    break;
                }
                $messages[] = $entry['message'];
                $count++;
            }

            try {
                $result = $this->queen->ack($messages, 'completed', array_filter([
                    'group' => $group,
                    'affinityKey' => $affinityKey,
                ], fn ($value) => $value !== null));
                $this->assertBatchAcknowledged($result, $count);
            } catch (\Throwable $acknowledgementFailure) {
                $abandonedLeases = [];
                foreach ($messages as $message) {
                    if ($this->leaseRenewer !== null) {
                        $leaseId = $this->leaseId($message);
                        if (isset($abandonedLeases[$leaseId])) {
                            continue;
                        }
                        $abandonedLeases[$leaseId] = true;
                        $this->abandonLease($message);
                    } else {
                        $this->discardPrefetchedSiblings($message);
                    }
                }
                throw $acknowledgementFailure;
            }
            array_splice($this->pendingAcknowledgements, 0, $count);
            foreach ($messages as $message) {
                $this->settleLeaseMessage($message);
            }
        }
    }

    public function releaseReserved(
        string $queue,
        array $message,
        string $payload,
        string $group,
        int $delay,
        int $attempts,
    ): void {
        try {
            $this->flushAcknowledgements();

            $decoded = $this->decodePayload($payload);
            $partition = (string) ($message['partition'] ?? $this->partitionForPayload($decoded));
            $decoded['_queen'] = array_replace(
                is_array($decoded['_queen'] ?? null) ? $decoded['_queen'] : [],
                ['partition' => $partition, 'attempts' => $attempts],
            );

            $releaseId = Uuid::v7();
            $transaction = $this->queen->transaction();
            $transaction->ack($message, 'completed', ['consumerGroup' => $group]);

            if ($delay > 0) {
                $timerKey = 'laravel:release:' . ($decoded['uuid'] ?? $message['id'] ?? $releaseId) . ':' . $releaseId;
                $transaction->timers($queue)->schedule(
                    $timerKey,
                    $delay * 1000,
                    $decoded,
                    ['txn' => $releaseId, 'partition' => $partition],
                );
            } else {
                $transaction->queue($queue)
                    ->partition($partition)
                    ->push([['data' => $decoded, 'transactionId' => $releaseId]]);
            }

            $transaction->commit();
        } catch (\Throwable $releaseFailure) {
            // The transaction outcome is ambiguous. Never execute a locally
            // buffered sibling whose partition lease may already have moved.
            if ($this->leaseRenewer !== null) {
                $this->abandonLease($message);
            } else {
                $this->discardPrefetchedSiblings($message);
                $this->markDeliveryHandled($message);
            }
            throw $releaseFailure;
        }

        // A successful completed ACK advances only through this message and
        // keeps the remaining same-partition lease valid.
        $this->markDeliveryHandled($message);
        $this->settleLeaseMessage($message);
    }

    public function getQueen(): Queen
    {
        return $this->queen;
    }

    public function getConsumerGroup(): string
    {
        return $this->consumerGroup;
    }

    /**
     * Remove the Queen DLQ row referenced by a Laravel failed-job payload.
     * Missing rows are an idempotent success: JobFailed is dispatched even if
     * the original DLQ acknowledgement lost its race or failed.
     */
    public function deleteFailedPayloadSource(string $payload): void
    {
        $decoded = $this->decodePayload($payload);
        $source = $decoded['_queen']['failed_source'] ?? null;

        if (is_array($source)) {
            $this->deleteFailedSource($source);
        }
    }

    protected function createPayloadArray($job, $queue, $data = ''): array
    {
        $payload = parent::createPayloadArray($job, $queue, $data);

        if ($job instanceof QueenPartitionable) {
            $partition = trim($job->queenPartition());
            if ($partition === '') {
                throw new RuntimeException('QueenPartitionable::queenPartition() must not return an empty string.');
            }
            $payload['_queen'] = ['partition' => $partition, 'attempts' => 0];
        }

        return $payload;
    }

    private function laterRaw(mixed $delay, string $payload, string $queue): string
    {
        $decoded = $this->decodePayload($payload);
        $partition = $this->partitionForPayload($decoded);
        $decoded['_queen'] = array_replace(
            ['attempts' => 0],
            is_array($decoded['_queen'] ?? null) ? $decoded['_queen'] : [],
            ['partition' => $partition],
        );
        $jobId = (string) ($decoded['uuid'] ?? Uuid::v7());

        $result = $this->queen->timers()->schedule(
            $queue,
            'laravel:delay:' . $jobId,
            max(0, $this->secondsUntil($delay)) * 1000,
            $decoded,
            ['txn' => $jobId, 'partition' => $partition],
        );

        if (!($result['ok'] ?? false)) {
            throw new RuntimeException('Unable to schedule Laravel job: ' . ($result['status'] ?? $result['error'] ?? 'unknown timer error'));
        }

        return $jobId;
    }

    private function getQueue(?string $queue): string
    {
        return $queue ?: $this->defaultQueue;
    }

    private function depth(?string $queue): array
    {
        try {
            $depth = $this->queen->admin()->getQueueDepth($this->getQueue($queue), $this->consumerGroup);
        } catch (HttpException $exception) {
            if ($exception->statusCode !== 404 || $exception->errorCode === 'no_such_route') {
                throw $exception;
            }

            // A timer can exist before its destination queue has received its
            // first durable push. In that state Laravel size() is the timer
            // count, not an exception from the absent depth resource.
            return [];
        }

        if (!is_array($depth)) {
            throw new UnexpectedValueException('Queen returned a malformed queue depth response.');
        }

        return $depth;
    }

    /**
     * @return array<string, true>|null Null means the response cannot safely
     *                                  identify its ready partitions.
     */
    private function readyPartitionNames(array $depth): ?array
    {
        $partitions = $depth['partitions'] ?? null;
        if (!is_array($partitions)) {
            return null;
        }

        $ready = [];
        foreach ($partitions as $partition) {
            if (!is_array($partition)) {
                return null;
            }

            $count = $this->nonNegativeCount($partition, 'ready');
            $name = $partition['partition'] ?? null;
            if ($count === null || !is_string($name) || $name === '') {
                return null;
            }
            if ($count > 0) {
                $ready[$name] = true;
            }
        }

        // A positive aggregate with no positive partition is malformed. Fall
        // back to the conservative queue-detail scan rather than report null.
        return $ready !== [] ? $ready : null;
    }

    private function nonNegativeCount(array $source, string $field): ?int
    {
        if (!array_key_exists($field, $source)) {
            return null;
        }

        $value = $source[$field];
        if (!is_int($value) || $value < 0) {
            throw new UnexpectedValueException("Queen returned a malformed [{$field}] counter.");
        }

        return $value;
    }

    private function partitionForPayload(array $payload): string
    {
        $declared = $payload['_queen']['partition'] ?? null;
        if (is_string($declared) && $declared !== '') {
            return $declared;
        }

        $key = (string) ($payload['uuid'] ?? json_encode($payload));
        $slot = hexdec(substr(hash('sha256', $key), 0, 8)) % $this->partitionCount;

        return sprintf('%s-%04d', $this->partitionPrefix, $slot);
    }

    private function decodePayload(string $payload): array
    {
        try {
            $decoded = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $exception) {
            throw new InvalidPayloadException('Unable to decode Laravel queue payload: ' . $exception->getMessage(), $payload);
        }

        if (!is_array($decoded)) {
            throw new InvalidPayloadException('Laravel queue payload must decode to an object.', $decoded);
        }

        return $decoded;
    }

    private function assertSuccessful(array $result, string $operation): void
    {
        if (($result['success'] ?? null) === false) {
            throw new RuntimeException("Unable to {$operation}: " . ($result['error'] ?? 'Queen rejected the operation'));
        }

        // The durable ACK routes return exactly one result for this one job.
        // Fail closed on an empty/malformed HTTP 200: treating it as success
        // could retire Laravel's failed_jobs record while the lease remains.
        $numeric = array_filter(array_keys($result), fn (mixed $key): bool => is_int($key));
        if ($numeric !== []) {
            if (count($numeric) !== 1 || !isset($result[0]) || !is_array($result[0])) {
                throw new RuntimeException("Unable to {$operation}: Queen returned a malformed acknowledgement");
            }
            $item = $result[0];
        } else {
            $hasAcknowledgementEvidence = array_key_exists('transactionId', $result)
                || array_key_exists('leaseReleased', $result)
                || array_key_exists('dlq', $result)
                || array_key_exists('noop', $result);
            if (!$hasAcknowledgementEvidence) {
                throw new RuntimeException("Unable to {$operation}: Queen returned a malformed acknowledgement");
            }
            $item = $result;
        }

        if (!($item['success'] ?? false)) {
            throw new RuntimeException("Unable to {$operation}: " . ($item['error'] ?? 'Queen rejected the operation'));
        }
    }

    private function assertPushAccepted(mixed $result): void
    {
        if (is_array($result)
            && ($result['buffered'] ?? false) === true
            && ($result['count'] ?? null) === 1) {
            return;
        }

        if (!is_array($result) || count($result) !== 1 || !isset($result[0]) || !is_array($result[0])) {
            throw new RuntimeException('Queen returned a malformed response for the Laravel job push.');
        }

        $status = $result[0]['status'] ?? null;
        if (!is_string($status) || !in_array($status, ['queued', 'duplicate', 'buffered'], true)) {
            throw new RuntimeException('Queen did not accept the Laravel job push: ' . (
                is_string($result[0]['error'] ?? null)
                    ? $result[0]['error']
                    : 'unexpected status ' . (is_scalar($status) ? (string) $status : get_debug_type($status))
            ));
        }
    }

    private function assertBulkPushAccepted(mixed $result, int $expected): void
    {
        if (!is_array($result) || count($result) !== $expected) {
            throw new RuntimeException('Queen returned a malformed response for the Laravel bulk push.');
        }

        foreach ($result as $item) {
            $status = is_array($item) ? ($item['status'] ?? null) : null;
            if (!is_string($status) || !in_array($status, ['queued', 'duplicate', 'buffered'], true)) {
                throw new RuntimeException('Queen did not accept a Laravel bulk job: ' . (
                    is_array($item) && is_string($item['error'] ?? null)
                        ? $item['error']
                        : 'unexpected status ' . (is_scalar($status) ? (string) $status : get_debug_type($status))
                ));
            }
        }
    }

    private function assertBatchAcknowledged(array $result, int $expected): void
    {
        if (($result['success'] ?? null) === false) {
            throw new RuntimeException('Unable to acknowledge jobs: ' . ($result['error'] ?? 'Queen rejected the operation'));
        }

        $items = [];
        foreach ($result as $key => $item) {
            if (is_int($key)) {
                $items[] = $item;
            }
        }
        if (count($items) !== $expected) {
            throw new RuntimeException('Unable to acknowledge jobs: Queen returned a malformed batch acknowledgement');
        }
        foreach ($items as $item) {
            if (!is_array($item) || !($item['success'] ?? false)) {
                throw new RuntimeException('Unable to acknowledge jobs: ' . (
                    is_array($item) ? ($item['error'] ?? 'Queen rejected the operation') : 'malformed acknowledgement'
                ));
            }
        }
    }

    /**
     * @return array<string, list<array{
     *     type: 'completed'|'retry',
     *     message: array,
     *     wire: array,
     *     group: string,
     *     affinity_key: ?string
     * }>>
     */
    private function shutdownAcknowledgementGroups(): array
    {
        $groups = [];

        foreach ($this->pendingAcknowledgements as $entry) {
            $wire = $entry['message'];
            $wire['_status'] = 'completed';
            $key = json_encode([$entry['group'], $entry['affinity_key']], JSON_THROW_ON_ERROR);
            $groups[$key][] = [
                'type' => 'completed',
                'message' => $entry['message'],
                'wire' => $wire,
                'group' => $entry['group'],
                'affinity_key' => $entry['affinity_key'],
            ];
        }

        foreach ($this->prefetched as $queue => $state) {
            $represented = [];
            $count = count($state['messages']);
            for ($index = $state['next']; $index < $count; ++$index) {
                $message = $state['messages'][$index];
                $leaseId = $this->leaseIdOrNull($message) ?? '';
                $partitionId = (string) ($message['partitionId'] ?? $message['partition_id'] ?? '');
                $partitionKey = $leaseId . "\0" . $partitionId;
                if (isset($represented[$partitionKey])) {
                    continue;
                }
                $represented[$partitionKey] = true;

                $affinityKey = "{$queue}:Default:{$this->consumerGroup}";
                $key = json_encode([$this->consumerGroup, $affinityKey], JSON_THROW_ON_ERROR);
                $wire = $message;
                $wire['_status'] = 'retry';
                $wire['_error'] = 'Laravel worker stopped before processing this prefetched delivery.';
                $groups[$key][] = [
                    'type' => 'retry',
                    'message' => $message,
                    'wire' => $wire,
                    'group' => $this->consumerGroup,
                    'affinity_key' => $affinityKey,
                ];
            }
        }

        return $groups;
    }

    /**
     * Forget every helper-side lease before closing it and erase local buffers.
     *
     * No network operation is attempted here. Anything not durably settled by
     * the one shutdown request becomes visible through ordinary lease expiry.
     */
    private function abandonUnsettledLocalState(): void
    {
        foreach (array_keys($this->leaseOutstanding) as $leaseId) {
            try {
                $this->leaseRenewer?->forget($leaseId);
            } catch (\Throwable) {
                // close() below tears down the helper's private pipe.
            }
        }

        $this->prefetched = [];
        $this->deliveryBatches = [];
        $this->activeDeliveries = [];
        $this->deliveryQueues = [];
        $this->batchOutstanding = [];
        $this->pendingAcknowledgements = [];
        $this->leaseOutstanding = [];
    }

    private function assertJobTimeoutIsSafe(QueenJob $job): void
    {
        $timeout = $job->timeout();
        if ($timeout === null) {
            // The worker CLI's --timeout is not part of Laravel's queue
            // connection contract. Supervisors validate it at startup.
            return;
        }

        if (!is_int($timeout) || $timeout < 0) {
            throw new RuntimeException(
                'Queen Laravel job timeout must be a non-negative integer or null.',
            );
        }

        if ($this->leaseRenewer === null && ($timeout === 0 || $timeout >= $this->retryAfter)) {
            throw new RuntimeException(
                "Queen Laravel job timeout [{$timeout}] must be positive and shorter than retry_after "
                . "[{$this->retryAfter}] when lease_renewal is disabled.",
            );
        }
    }

    private function takePrefetched(string $queue): ?array
    {
        $state = $this->prefetched[$queue] ?? null;
        if ($state === null) {
            return null;
        }

        $message = $state['messages'][$state['next']] ?? null;
        $state['next']++;
        if ($state['next'] >= count($state['messages'])) {
            unset($this->prefetched[$queue]);
        } else {
            $this->prefetched[$queue] = $state;
        }

        return is_array($message) ? $message : null;
    }

    /** @param list<array> $messages */
    private function registerPopBatch(array $messages): void
    {
        $batchId = 'batch-' . (++$this->nextBatchId);
        $this->batchOutstanding[$batchId] = count($messages);
        foreach ($messages as $message) {
            $this->deliveryBatches[$this->deliveryKey($message)] = $batchId;
        }
    }

    /** Return true when every delivery from the original pop has been handled. */
    private function markDeliveryHandled(array $message): bool
    {
        $key = $this->deliveryKey($message);
        $queue = $this->deliveryQueues[$key] ?? null;
        if ($queue !== null && ($this->activeDeliveries[$queue] ?? null) === $key) {
            unset($this->activeDeliveries[$queue]);
        }
        unset($this->deliveryQueues[$key]);

        $batchId = $this->deliveryBatches[$key] ?? null;
        if ($batchId === null) {
            return true;
        }

        unset($this->deliveryBatches[$key]);
        $remaining = max(0, ($this->batchOutstanding[$batchId] ?? 1) - 1);
        if ($remaining === 0) {
            unset($this->batchOutstanding[$batchId]);
            return true;
        }

        $this->batchOutstanding[$batchId] = $remaining;
        return false;
    }

    /**
     * Drop only unhandled local messages whose partition lease may have been
     * closed or made ambiguous by a nack, release or failed ACK. Other
     * partitions from the same multi-partition pop remain safe to process.
     */
    private function discardPrefetchedSiblings(array $message): void
    {
        $partitionId = (string) ($message['partitionId'] ?? $message['partition_id'] ?? '');
        $leaseId = (string) ($message['leaseId'] ?? $message['lease_id'] ?? '');
        if ($partitionId === '' || $leaseId === '') {
            return;
        }

        foreach ($this->prefetched as $queue => $state) {
            $kept = [];
            $count = count($state['messages']);
            for ($index = $state['next']; $index < $count; ++$index) {
                $candidate = $state['messages'][$index];
                $candidatePartitionId = (string) ($candidate['partitionId'] ?? $candidate['partition_id'] ?? '');
                $candidateLeaseId = (string) ($candidate['leaseId'] ?? $candidate['lease_id'] ?? '');
                if ($candidatePartitionId === $partitionId && $candidateLeaseId === $leaseId) {
                    $this->markDeliveryHandled($candidate);
                    $this->settleLeaseMessage($candidate);
                    continue;
                }
                $kept[] = $candidate;
            }

            if ($kept === []) {
                unset($this->prefetched[$queue]);
            } else {
                $this->prefetched[$queue] = ['messages' => $kept, 'next' => 0];
            }
        }
    }

    /** @param list<array> $messages */
    private function registerLeaseRenewal(array $messages, int $deadlineMonotonicMillis): void
    {
        $counts = [];
        foreach ($messages as $message) {
            $leaseId = $this->leaseId($message);
            $counts[$leaseId] = ($counts[$leaseId] ?? 0) + 1;
        }

        $started = [];
        $previous = [];
        try {
            foreach ($counts as $leaseId => $count) {
                $previous[$leaseId] = $this->leaseOutstanding[$leaseId] ?? null;
                if (!isset($this->leaseOutstanding[$leaseId])) {
                    $this->leaseRenewer?->track($leaseId, $deadlineMonotonicMillis);
                    $started[] = $leaseId;
                }
                $this->leaseOutstanding[$leaseId] = ($this->leaseOutstanding[$leaseId] ?? 0) + $count;
            }
        } catch (\Throwable $exception) {
            foreach ($started as $leaseId) {
                $this->leaseRenewer?->forget($leaseId);
            }
            // A failed track has an ambiguous child-side outcome. Close the
            // helper so no unconfirmed lease can be renewed as an orphan.
            $this->leaseRenewer?->close();
            foreach ($previous as $leaseId => $count) {
                if ($count === null) {
                    unset($this->leaseOutstanding[$leaseId]);
                } else {
                    $this->leaseOutstanding[$leaseId] = $count;
                }
            }
            throw $exception;
        }
    }

    private function settleLeaseMessage(array $message): void
    {
        if ($this->leaseRenewer === null) {
            return;
        }

        $leaseId = $this->leaseId($message);
        if (!isset($this->leaseOutstanding[$leaseId])) {
            return;
        }
        $remaining = $this->leaseOutstanding[$leaseId] - 1;
        if ($remaining > 0) {
            $this->leaseOutstanding[$leaseId] = $remaining;
            return;
        }

        unset($this->leaseOutstanding[$leaseId]);
        $this->leaseRenewer->forget($leaseId);
    }

    /**
     * Stop extending an ambiguous lease and discard every local tail sharing
     * it. The broker can then redeliver any unacknowledged position after the
     * original lease expires; already-observed job effects may therefore be
     * duplicated, which is the required at-least-once failure mode.
     */
    private function abandonLease(array $message): void
    {
        if ($this->leaseRenewer === null) {
            return;
        }

        $leaseId = $this->leaseId($message);
        unset($this->leaseOutstanding[$leaseId]);
        $this->leaseRenewer->forget($leaseId);

        foreach ($this->prefetched as $queue => $state) {
            $kept = [];
            $count = count($state['messages']);
            for ($index = $state['next']; $index < $count; ++$index) {
                $candidate = $state['messages'][$index];
                if ($this->leaseIdOrNull($candidate) === $leaseId) {
                    $this->markDeliveryHandled($candidate);
                    continue;
                }
                $kept[] = $candidate;
            }
            if ($kept === []) {
                unset($this->prefetched[$queue]);
            } else {
                $this->prefetched[$queue] = ['messages' => $kept, 'next' => 0];
            }
        }

        // Do not retry a deferred ACK for a lease whose outcome is ambiguous.
        // Any successful subset is idempotent; the unacknowledged subset must
        // become visible again after expiry.
        $this->pendingAcknowledgements = array_values(array_filter(
            $this->pendingAcknowledgements,
            fn (array $entry): bool => $this->leaseIdOrNull($entry['message']) !== $leaseId,
        ));
        $this->markDeliveryHandled($message);
    }

    private function leaseId(array $message): string
    {
        $leaseId = $this->leaseIdOrNull($message);
        if ($leaseId === null) {
            throw new RuntimeException('Queen returned a message without a lease ID while lease renewal is enabled.');
        }

        return $leaseId;
    }

    private function leaseIdOrNull(array $message): ?string
    {
        $leaseId = $message['leaseId'] ?? $message['lease_id'] ?? null;
        return is_string($leaseId) && $leaseId !== '' ? $leaseId : null;
    }

    private static function monotonicMillis(): int
    {
        if (PHP_INT_SIZE < 8) {
            throw new RuntimeException('Queen Laravel lease renewal requires 64-bit PHP monotonic timestamps.');
        }

        return intdiv(hrtime(true), 1_000_000);
    }

    private function deliveryKey(array $message): string
    {
        return implode("\0", [
            (string) ($message['partitionId'] ?? $message['partition_id'] ?? ''),
            (string) ($message['transactionId'] ?? $message['transaction_id'] ?? $message['id'] ?? ''),
            (string) ($message['leaseId'] ?? $message['lease_id'] ?? ''),
        ]);
    }

    private function deleteFailedSource(array $source): void
    {
        $partitionId = $source['partition_id'] ?? null;
        $transactionId = $source['transaction_id'] ?? null;
        if (!is_string($partitionId) || $partitionId === '' || !is_string($transactionId) || $transactionId === '') {
            return;
        }

        try {
            $this->queen->admin()->deleteMessage($partitionId, $transactionId);
        } catch (HttpException $exception) {
            // DELETE has one idempotent 404 shape: the broker-native
            // {error: "Message not found"} response for an absent DLQ row.
            // A proxy/broker route mismatch is also a 404, but means cleanup
            // never ran and must leave Laravel's failed row intact.
            $snapshotIsAbsent = $exception->statusCode === 404
                && $exception->errorCode === null
                && $exception->serverError === 'Message not found';
            if (!$snapshotIsAbsent) {
                throw $exception;
            }
        }
    }
}
