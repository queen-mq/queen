<?php

namespace Queen\Laravel\Monitoring;

use Queen\Queen;

/**
 * How long the oldest job still waiting on a queue has waited for one
 * consumer group, as the broker measures it. Horizon estimates a queue's wait
 * from its backlog and recent runtimes; this reads the age of the oldest
 * unconsumed message.
 *
 * Three broker reads, cheapest first: the group's pending count (nothing
 * waits when it is zero), the group's lag (present once the group has
 * consumed from the queue), and otherwise the oldest pending message of the
 * queue, which also covers a queue no worker has ever served.
 */
final class QueueWaits
{
    /** @var list<array<string, mixed>>|null */
    private ?array $lagging = null;

    public function __construct(private Queen $queen, private ?\Closure $clock = null)
    {
    }

    /** @return int seconds; 0 when no job of this group waits */
    public function seconds(string $queue, string $consumerGroup): int
    {
        $depth = $this->queen->admin()->getQueueDepth($queue, $consumerGroup);
        $pending = is_array($depth) ? ($depth['effectivePending'] ?? $depth['pending'] ?? null) : null;
        if (!is_int($pending)) {
            throw new \UnexpectedValueException("Queen returned a malformed depth for [{$queue}].");
        }
        if ($pending === 0) {
            return 0;
        }

        $lag = null;
        foreach ($this->lagging() as $row) {
            if (($row['queue_name'] ?? null) === $queue
                && ($row['consumer_group'] ?? null) === $consumerGroup
                && is_int($row['time_lag_seconds'] ?? null)) {
                $lag = max($lag ?? 0, $row['time_lag_seconds']);
            }
        }
        if ($lag !== null) {
            return $lag;
        }

        return $this->oldestPendingAge($queue);
    }

    /** @return list<array<string, mixed>> */
    private function lagging(): array
    {
        if ($this->lagging === null) {
            $rows = $this->queen->admin()->getLaggingConsumers(0);
            $this->lagging = is_array($rows) && array_is_list($rows)
                ? array_values(array_filter($rows, 'is_array'))
                : [];
        }

        return $this->lagging;
    }

    private function oldestPendingAge(string $queue): int
    {
        $detail = $this->queen->admin()->getQueue($queue);
        $oldest = null;
        foreach (is_array($detail) && is_array($detail['partitions'] ?? null) ? $detail['partitions'] : [] as $partition) {
            $pending = $partition['stats']['pending'] ?? 0;
            $at = is_string($partition['oldestMessage'] ?? null) ? strtotime($partition['oldestMessage']) : false;
            if (is_int($pending) && $pending > 0 && is_int($at)) {
                $oldest = min($oldest ?? $at, $at);
            }
        }

        return $oldest === null ? 0 : max(0, $this->now() - $oldest);
    }

    private function now(): int
    {
        return $this->clock !== null ? (int) ($this->clock)() : time();
    }
}
