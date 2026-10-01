<?php

namespace Queen\Laravel\Dashboard;

use GuzzleHttp\Promise\Utils;
use Illuminate\Contracts\Cache\Repository as CacheRepository;
use Queen\Queen;

/**
 * What is in each supervised queue now: jobs waiting, jobs running, and how
 * long the oldest unfinished job has been there. A summary, not a job list:
 * the Queen console browses the messages themselves.
 *
 * Waiting and running come from the consumer group's lease-aware depth (GET
 * /api/v1/resources/queues/{queue}/depth); the oldest unfinished job from the
 * broker's lagging partitions (GET /api/v1/consumer-groups/lagging), which
 * reports a partition once its oldest unacknowledged message is a minute old:
 * a job waiting, or a job running, that long. The answers are untrusted:
 * every number is validated, and a queue the broker does not answer for is
 * marked unavailable instead of shown as empty.
 */
final class QueueContentsReader
{
    /** The broker reports a lagging partition from this age. */
    public const LAG_FLOOR_SECONDS = 60;

    private const MAX_QUEUES = 32;

    private const CACHE_SECONDS = 5;

    /**
     * @param \Closure(string): Queen $queenFor a read client for a Laravel queue connection
     * @param (\Closure(): (CacheRepository|null))|null $cache
     */
    public function __construct(
        private \Closure $queenFor,
        private ?\Closure $cache = null,
        private int $timeoutMillis = 5000,
    ) {
    }

    /**
     * @param list<array<string, mixed>> $queues the dashboard's queue rows (connection, consumer_group, queue)
     * @return array{available: bool, queues: list<array{connection: string, consumer_group: string, queue: string, available: bool, waiting: ?int, running: ?int, oldest_seconds: ?int, oldest_partition: ?string}>}
     */
    public function read(array $queues): array
    {
        $targets = $this->targets($queues);
        $key = 'queen:dashboard:queue-contents:' . sha1(json_encode($targets, JSON_THROW_ON_ERROR));
        $cache = $this->cache !== null ? ($this->cache)() : null;
        if ($cache !== null) {
            try {
                $cached = $cache->get($key);
                if (is_array($cached) && isset($cached['queues'])) {
                    return $cached;
                }
            } catch (\Throwable) {
                // A cache outage costs one broker read per render.
            }
        }

        $result = $this->fetch($targets);
        if ($cache !== null) {
            try {
                $cache->put($key, $result, self::CACHE_SECONDS);
            } catch (\Throwable) {
                // The next render reads the broker again.
            }
        }

        return $result;
    }

    /**
     * @param list<array<string, mixed>> $queues
     * @return list<array{connection: string, consumer_group: string, queue: string}>
     */
    private function targets(array $queues): array
    {
        $targets = [];
        foreach ($queues as $queue) {
            $row = [
                'connection' => $queue['connection'] ?? null,
                'consumer_group' => $queue['consumer_group'] ?? null,
                'queue' => $queue['queue'] ?? null,
            ];
            if (in_array(null, $row, true) || in_array('', $row, true) || array_filter($row, 'is_string') !== $row) {
                continue;
            }
            $targets[implode("\0", $row)] = $row;
            if (count($targets) >= self::MAX_QUEUES) {
                break;
            }
        }

        return array_values($targets);
    }

    /**
     * @param list<array{connection: string, consumer_group: string, queue: string}> $targets
     * @return array<string, mixed>
     */
    private function fetch(array $targets): array
    {
        $clients = [];
        $promises = [];
        foreach ($targets as $index => $target) {
            try {
                $clients[$target['connection']] ??= ($this->queenFor)($target['connection']);
                $promises["depth:{$index}"] = $clients[$target['connection']]->admin()->getQueueDepthAsync(
                    $target['queue'],
                    $target['consumer_group'],
                    $this->timeoutMillis,
                );
            } catch (\Throwable) {
                // That queue shows as unavailable.
            }
        }
        foreach ($clients as $connection => $client) {
            try {
                $promises["lag:{$connection}"] = $client->admin()
                    ->getLaggingConsumersAsync(self::LAG_FLOOR_SECONDS, $this->timeoutMillis);
            } catch (\Throwable) {
                // The oldest age shows as unknown.
            }
        }
        $settled = Utils::settle($promises)->wait();

        $lag = [];
        foreach ($clients as $connection => $_) {
            $answer = $settled["lag:{$connection}"] ?? null;
            if (($answer['state'] ?? null) === 'fulfilled') {
                $lag[$connection] = $this->oldestPerQueue($answer['value']);
            }
        }

        $rows = [];
        $available = false;
        foreach ($targets as $index => $target) {
            $answer = $settled["depth:{$index}"] ?? null;
            $depth = ($answer['state'] ?? null) === 'fulfilled' && is_array($answer['value']) ? $answer['value'] : null;
            $oldest = $lag[$target['connection']][$target['consumer_group'] . "\0" . $target['queue']] ?? null;
            $running = $depth === null ? null : $this->count($depth['processing'] ?? null);
            $waiting = $depth === null ? null : ($this->count($depth['ready'] ?? null)
                ?? ($running === null ? $this->count($depth['pending'] ?? null) : null));
            $available = $available || $depth !== null;
            $rows[] = [
                ...$target,
                'available' => $depth !== null,
                'waiting' => $waiting,
                'running' => $running,
                'oldest_seconds' => $oldest['seconds'] ?? null,
                'oldest_partition' => $oldest['partition'] ?? null,
            ];
        }

        return ['available' => $available, 'queues' => $rows];
    }

    /** @return array<string, array{seconds: int, partition: ?string}> the oldest lag per group and queue */
    private function oldestPerQueue(mixed $answer): array
    {
        $rows = is_array($answer) && array_is_list($answer)
            ? $answer
            : (is_array($answer) ? ($answer['data'] ?? $answer['groups'] ?? $answer['consumers'] ?? []) : []);
        $oldest = [];
        foreach (is_array($rows) ? $rows : [] as $row) {
            if (!is_array($row)) {
                continue;
            }
            $group = $row['consumer_group'] ?? null;
            $queue = $row['queue_name'] ?? null;
            $seconds = $this->count($row['time_lag_seconds'] ?? null);
            if (!is_string($group) || !is_string($queue) || $seconds === null) {
                continue;
            }
            $key = $group . "\0" . $queue;
            if ($seconds > ($oldest[$key]['seconds'] ?? -1)) {
                $partition = $row['partition_name'] ?? null;
                $oldest[$key] = [
                    'seconds' => $seconds,
                    'partition' => is_string($partition) && strlen($partition) <= 128 ? $partition : null,
                ];
            }
        }

        return $oldest;
    }

    private function count(mixed $value): ?int
    {
        return is_int($value) && $value >= 0 ? $value : null;
    }
}
