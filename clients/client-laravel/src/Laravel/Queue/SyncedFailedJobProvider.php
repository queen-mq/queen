<?php

namespace Queen\Laravel\Queue;

use DateTimeInterface;
use Illuminate\Queue\Failed\CountableFailedJobProvider;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Queue\Failed\PrunableFailedJobProvider;

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
    /** @param \Closure(string): mixed $queueResolver */
    public function __construct(
        private FailedJobProviderInterface $inner,
        private \Closure $queueResolver,
    ) {
    }

    public function log($connection, $queue, $payload, $exception)
    {
        return $this->inner->log($connection, $queue, $payload, $exception);
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
        return $this->inner->find($id);
    }

    public function forget($id)
    {
        $record = $this->inner->find($id);
        if ($record === null) {
            return false;
        }

        $this->cleanup($record);

        return $this->inner->forget($id);
    }

    public function flush($hours = null)
    {
        $cutoff = $hours ? time() - max(0, (int) $hours) * 3600 : null;
        foreach ($this->inner->all() as $record) {
            if ($cutoff === null || $this->failedAt($record) <= $cutoff) {
                $this->cleanup($record);
            }
        }

        $this->inner->flush($hours);
    }

    public function prune(DateTimeInterface $before)
    {
        $records = array_values(array_filter(
            $this->inner->all(),
            fn (mixed $record): bool => $this->failedAt($record) < $before->getTimestamp(),
        ));

        foreach ($records as $record) {
            $this->cleanup($record);
        }

        if ($this->inner instanceof PrunableFailedJobProvider) {
            return $this->inner->prune($before);
        }

        $deleted = 0;
        foreach ($records as $record) {
            $id = $this->value($record, 'id');
            if ($id !== null && $this->inner->forget($id)) {
                $deleted++;
            }
        }

        return $deleted;
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
