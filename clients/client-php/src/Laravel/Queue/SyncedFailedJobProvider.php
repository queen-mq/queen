<?php

namespace Queen\Laravel\Queue;

use DateTimeInterface;
use Illuminate\Queue\Failed\CountableFailedJobProvider;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Queue\Failed\FileFailedJobProvider;
use Illuminate\Queue\Failed\PrunableFailedJobProvider;
use RuntimeException;

/**
 * Keeps Laravel's failed-job repository and Queen's DLQ snapshots in step.
 *
 * Laravel remains the command/control index (`queue:failed`, retry, forget and
 * prune). Queen retains the broker-side snapshot for inspection. Destructive
 * Laravel operations remove that snapshot first; if Queen is unavailable the
 * Laravel row remains and the operation can be retried safely.
 */
final class SyncedFailedJobProvider implements
    FailedJobProviderInterface,
    CountableFailedJobProvider,
    PrunableFailedJobProvider
{
    private const MAX_RETRY_FENCE_ENTRIES = 1024;

    /** @var array<string, array{id: string|int, record: mixed}> */
    private array $pendingRetryFences = [];

    /** @var array<string, true> One-shot guards for Laravel's post-push forget(). */
    private array $completedRetryForgets = [];

    /**
     * @param \Closure(string): mixed $queueResolver
     * @param (\Closure(\Closure(\Closure(): void): mixed): mixed)|null $synchronize
     *        The outer closure acquires the failed-store lock and passes an
     *        ownership assertion into the operation.
     */
    public function __construct(
        private FailedJobProviderInterface $inner,
        private \Closure $queueResolver,
        private ?\Closure $synchronize = null,
    ) {
    }

    public function log($connection, $queue, $payload, $exception)
    {
        return $this->synchronized(
            function (\Closure $assertOwned) use ($connection, $queue, $payload, $exception): mixed {
                $this->makeRoomInBoundedFileProvider($assertOwned);
                $assertOwned();

                return $this->inner->log($connection, $queue, $payload, $exception);
            },
        );
    }

    public function ids($queue = null)
    {
        return $this->inner->ids($queue);
    }

    public function all()
    {
        return $this->inner->all();
    }

    public function find($id)
    {
        $record = $this->inner->find($id);

        return $record === null ? null : $this->withRetryFence($record);
    }

    public function forget($id)
    {
        $recordKey = $this->recordKey($id);
        if ($recordKey !== null && isset($this->completedRetryForgets[$recordKey])) {
            // RetryCommand always calls forget($id) after pushRaw(). A fenced
            // Queen retry already removed the exact old generation while it
            // held the failed-store lock. Consume this one-shot marker without
            // looking up the ID again: a very fast retried job may already
            // have logged a new failure with the same UUID.
            unset($this->completedRetryForgets[$recordKey]);

            return false;
        }

        return $this->synchronized(function (\Closure $assertOwned) use ($id): bool {
            $record = $this->inner->find($id);
            if ($record === null) {
                return false;
            }

            $this->cleanup($record);
            // The network request above can outlive a badly-sized cache-lock
            // TTL. Never delete the durable Laravel index after ownership was
            // lost; the now-missing DLQ snapshot is an idempotent retry.
            $assertOwned();

            return $this->inner->forget($id);
        });
    }

    /**
     * Atomically hand one exact failed-job generation back to Queen.
     *
     * Laravel's RetryCommand performs find -> pushRaw -> forget as three
     * independent calls. QueenQueue routes a retry carrying the unguessable
     * fence added by find() through this method, so log(), retry and every
     * destructive failed-store operation share one distributed lock.
     *
     * @param \Closure(): mixed $republish
     */
    public function retryWithFence(string $fence, \Closure $republish): mixed
    {
        $pending = $this->pendingRetryFences[$fence] ?? null;
        if ($pending === null) {
            throw new RuntimeException('The Queen failed-job retry fence is missing, expired, or already consumed.');
        }

        unset($this->pendingRetryFences[$fence]);
        $recordKey = $this->recordKey($pending['id']);

        return $this->synchronized(function (\Closure $assertOwned) use ($pending, $republish, $recordKey): mixed {
            $current = $this->inner->find($pending['id']);
            if ($current === null || !$this->sameRecordGeneration($pending['record'], $current)) {
                throw new RuntimeException(
                    'The Laravel failed job changed after queue:retry selected it; the stale generation was not published.',
                );
            }

            // Keep the lock across the broker hand-off. A retried job can be
            // consumed immediately, but its SyncedFailedJobProvider::log()
            // cannot insert (or collide with) the next generation until the
            // old durable row has been removed below.
            $result = $republish();
            $assertOwned();

            $current = $this->inner->find($pending['id']);
            if ($current === null || !$this->sameRecordGeneration($pending['record'], $current)) {
                throw new RuntimeException(
                    'The Laravel failed job changed during its Queen retry; no replacement generation was deleted.',
                );
            }
            $assertOwned();
            if (!$this->inner->forget($pending['id'])) {
                throw new RuntimeException(
                    'Queen accepted the failed-job retry, but Laravel could not remove the exact old generation.',
                );
            }

            $assertOwned();
            if ($recordKey !== null) {
                $this->markCompletedRetryForget($recordKey);
            }

            return $result;
        });
    }

    public function flush($hours = null)
    {
        // A one-second barrier makes an unqualified flush conservative when a
        // retried job fails again with the same Laravel UUID during the gap
        // between snapshot and per-ID cleanup. The next flush can remove that
        // boundary row; a post-snapshot failure must never be mistaken for the
        // snapshotted generation.
        $cutoff = $hours
            ? time() - max(0, (int) $hours) * 3600
            : time() - 1;
        $eligible = fn (mixed $record): bool => $this->failedAt($record) <= $cutoff;
        $records = $this->snapshot(function () use ($eligible): array {
            return array_values(array_filter(
                $this->inner->all(),
                $eligible,
            ));
        });

        // One lock acquisition per exact snapshotted ID bounds every critical
        // section to at most one Queen cleanup instead of holding a single TTL
        // across an arbitrarily large failed-job repository.
        $this->removeExactRecords($records, $eligible);
    }

    public function prune(DateTimeInterface $before)
    {
        $cutoff = $before->getTimestamp();
        $inclusive = $this->inner instanceof FileFailedJobProvider;
        $eligible = function (mixed $record) use ($cutoff, $inclusive): bool {
            $failedAt = $this->failedAt($record);
            return $failedAt < $cutoff || ($inclusive && $failedAt === $cutoff);
        };
        $records = $this->snapshot(function () use ($eligible): array {
            return array_values(array_filter(
                $this->inner->all(),
                $eligible,
            ));
        });

        return $this->removeExactRecords($records, $eligible);
    }

    public function count($connection = null, $queue = null)
    {
        if ($this->inner instanceof CountableFailedJobProvider) {
            return $this->inner->count($connection, $queue);
        }

        return count(array_filter($this->inner->all(), function (mixed $record) use ($connection, $queue): bool {
            return ($connection === null || $this->value($record, 'connection') === $connection)
                && ($queue === null || $this->value($record, 'queue') === $queue);
        }));
    }

    private function cleanup(mixed $record): void
    {
        $connection = $this->value($record, 'connection');
        $payload = $this->value($record, 'payload');
        if (!is_string($connection) || $connection === '' || !is_string($payload) || $payload === '') {
            return;
        }

        $queue = ($this->queueResolver)($connection);
        if ($queue instanceof QueenQueue) {
            $queue->deleteFailedPayloadSource($payload);
        }
    }

    /**
     * @param list<mixed> $records
     * @param (\Closure(mixed): bool)|null $eligible
     */
    private function removeExactRecords(array $records, ?\Closure $eligible = null): int
    {
        $deleted = 0;
        foreach ($records as $record) {
            if ($this->synchronized(
                fn (\Closure $assertOwned): bool => $this->removeRecordUnderLock(
                    $record,
                    $assertOwned,
                    $eligible,
                ),
            )) {
                ++$deleted;
            }
        }

        return $deleted;
    }

    /**
     * Laravel's file provider silently slices its oldest rows during log().
     * Remove those rows explicitly first so their Queen DLQ snapshots cannot
     * become unreachable orphans. The outer distributed lock serializes this
     * sequence across workers that resolve this synchronized provider.
     */
    private function makeRoomInBoundedFileProvider(\Closure $assertOwned): void
    {
        if (!$this->inner instanceof FileFailedJobProvider) {
            return;
        }

        try {
            $property = new \ReflectionProperty($this->inner, 'limit');
            $limit = $property->getValue($this->inner);
        } catch (\ReflectionException $exception) {
            throw new RuntimeException(
                'Unable to inspect Laravel file failed-job retention limit.',
                previous: $exception,
            );
        }
        if (is_string($limit) && preg_match('/^[1-9][0-9]*$/D', $limit) === 1) {
            $limit = filter_var($limit, FILTER_VALIDATE_INT);
        }
        if (!is_int($limit) || $limit < 1) {
            throw new RuntimeException('Laravel file failed-job retention limit must be a positive integer.');
        }

        $records = array_values($this->inner->all());
        $overflow = max(0, count($records) - $limit + 1);
        if ($overflow > 0) {
            foreach (array_slice($records, -$overflow) as $record) {
                $this->removeRecordUnderLock($record, $assertOwned);
            }
        }
    }

    /**
     * Remove one previously snapshotted record while the caller owns the
     * failed-store lock. Re-reading by ID makes concurrent forget/flush calls
     * idempotent and prevents stale record bodies from driving cleanup.
     */
    private function removeRecordUnderLock(
        mixed $record,
        \Closure $assertOwned,
        ?\Closure $eligible = null,
    ): bool
    {
        $id = $this->value($record, 'id');
        if (!is_string($id) && !is_int($id)) {
            throw new RuntimeException('Failed-job repository returned a record without a stable ID.');
        }

        $current = $this->inner->find($id);
        if ($current === null
            || ($eligible !== null && !$eligible($current))
            || !$this->sameRecordGeneration($record, $current)) {
            return false;
        }

        $this->cleanup($current);
        $assertOwned();

        return $this->inner->forget($id);
    }

    private function sameRecordGeneration(mixed $snapshot, mixed $current): bool
    {
        foreach (['id', 'connection', 'queue', 'payload', 'exception', 'failed_at'] as $field) {
            $before = $this->value($snapshot, $field);
            $after = $this->value($current, $field);
            if ($before instanceof DateTimeInterface) {
                $before = $before->format('U.u');
            }
            if ($after instanceof DateTimeInterface) {
                $after = $after->format('U.u');
            }
            if ($before !== $after) {
                return false;
            }
        }

        return true;
    }

    /**
     * Return a detached failed record whose payload carries an in-process,
     * one-use retry capability. The durable failed row is never modified.
     */
    private function withRetryFence(mixed $record): mixed
    {
        $id = $this->value($record, 'id');
        $recordKey = $this->recordKey($id);
        $payload = $this->value($record, 'payload');
        if ($recordKey === null || !is_string($payload) || $payload === '') {
            return $record;
        }

        try {
            $decoded = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            return $record;
        }
        if (!is_array($decoded) || !is_array($decoded['_queen'] ?? null)) {
            return $record;
        }

        $metadata = $decoded['_queen'];
        $manualRetry = $metadata['manual_retry'] ?? null;
        $source = $metadata['failed_source'] ?? null;
        $isManualRetry = (is_string($manualRetry) && $manualRetry !== '') || $manualRetry === true;
        $hasQueenSource = is_array($source)
            && is_string($source['partition_id'] ?? null)
            && $source['partition_id'] !== ''
            && is_string($source['transaction_id'] ?? null)
            && $source['transaction_id'] !== '';
        if (!$isManualRetry || !$hasQueenSource) {
            return $record;
        }

        $fence = bin2hex(random_bytes(32));
        $decoded['_queen']['retry_fence'] = $fence;
        $decoratedPayload = json_encode($decoded, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
        $decorated = $this->replacePayload($record, $decoratedPayload);
        if ($decorated === null) {
            return $record;
        }

        while (count($this->pendingRetryFences) >= self::MAX_RETRY_FENCE_ENTRIES) {
            $oldestFence = array_key_first($this->pendingRetryFences);
            if ($oldestFence === null) {
                break;
            }
            unset($this->pendingRetryFences[$oldestFence]);
        }

        $this->pendingRetryFences[$fence] = ['id' => $id, 'record' => $record];

        return $decorated;
    }

    private function replacePayload(mixed $record, string $payload): mixed
    {
        if (is_array($record)) {
            $record['payload'] = $payload;

            return $record;
        }
        if (!is_object($record)) {
            return null;
        }

        try {
            $copy = clone $record;
            $copy->payload = $payload;

            return $copy;
        } catch (\Throwable) {
            return null;
        }
    }

    private function recordKey(mixed $id): ?string
    {
        return is_string($id) || is_int($id) ? (string) $id : null;
    }

    private function markCompletedRetryForget(string $recordKey): void
    {
        unset($this->completedRetryForgets[$recordKey]);
        while (count($this->completedRetryForgets) >= self::MAX_RETRY_FENCE_ENTRIES) {
            $oldestKey = array_key_first($this->completedRetryForgets);
            if ($oldestKey === null) {
                break;
            }
            unset($this->completedRetryForgets[$oldestKey]);
        }
        $this->completedRetryForgets[$recordKey] = true;
    }

    /** @return list<mixed> */
    private function snapshot(\Closure $records): array
    {
        return $this->synchronized(
            fn (\Closure $assertOwned): array => $records(),
        );
    }

    private function synchronized(\Closure $operation): mixed
    {
        if ($this->synchronize === null) {
            return $operation(static function (): void {
            });
        }

        return ($this->synchronize)($operation);
    }

    private function failedAt(mixed $record): int
    {
        $value = $this->value($record, 'failed_at');
        if ($value instanceof DateTimeInterface) {
            return $value->getTimestamp();
        }

        $timestamp = is_string($value) ? strtotime($value) : false;

        // Unknown timestamps are treated as old. This makes cleanup a superset
        // of what the underlying provider may remove, never the reverse; rows
        // without a Queen marker remain a no-op.
        return $timestamp === false ? 0 : $timestamp;
    }

    private function value(mixed $record, string $key): mixed
    {
        if (is_array($record)) {
            return $record[$key] ?? null;
        }

        return is_object($record) ? ($record->{$key} ?? null) : null;
    }
}
