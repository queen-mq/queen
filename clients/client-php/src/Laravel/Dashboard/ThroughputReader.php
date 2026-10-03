<?php

namespace Queen\Laravel\Dashboard;

use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Promise\Utils;
use Illuminate\Contracts\Cache\Repository as CacheRepository;
use Queen\Queen;

/**
 * Jobs completed and failed over time, from the broker's own per-queue
 * operation counters (GET /api/v1/analytics/queue-ops).
 *
 * The broker already counts every acknowledgement, so the dashboard adds no
 * write to the job path. A completed job is an ack with status `completed`; a
 * failed job is any other ack, which for Laravel jobs is the move to the DLQ.
 * A Laravel release is a transaction, not an ack, so a retried attempt is not
 * counted as completed. The counters are per queue: every consumer group that
 * acknowledges the queue adds to them.
 *
 * The response is untrusted input: every number is validated, rows for other
 * queues are ignored, and the bucket grid is rebuilt here so a missing bucket
 * reads as zero instead of shifting the chart.
 */
final class ThroughputReader
{
    /** Window lengths in seconds, in selector order. */
    public const RANGES = ['1h' => 3600, '6h' => 21600, '24h' => 86400, '7d' => 604800];

    public const DEFAULT_RANGE = '1h';

    private const MAX_QUEUES = 32;

    private const MAX_BUCKETS = 400;

    /** Buckets are at least a minute wide; a shorter cache still shows the current minute filling up. */
    private const CACHE_SECONDS = 15;

    /**
     * @param \Closure(string): Queen $queenFor a read client for a Laravel queue connection
     * @param (\Closure(): (CacheRepository|null))|null $cache
     * @param (\Closure(): int)|null $clock
     */
    public function __construct(
        private \Closure $queenFor,
        private ?\Closure $cache = null,
        private ?\Closure $clock = null,
        private int $timeoutMillis = 5000,
    ) {
    }

    public static function range(mixed $value): string
    {
        return is_string($value) && array_key_exists($value, self::RANGES) ? $value : self::DEFAULT_RANGE;
    }

    /** Bucket width in minutes for a window, as the broker derives it. */
    public static function bucketMinutes(int $seconds): int
    {
        return match (true) {
            $seconds <= 3600 => 1,
            $seconds <= 21600 => 5,
            $seconds <= 86400 => 15,
            default => 60,
        };
    }

    /**
     * @param list<array<string, mixed>> $queues the dashboard's queue rows (connection, queue)
     * @return array{available: bool, range: string, bucket_minutes: int, from: int, to: int, buckets: list<array{start: int, completed: int, failed: int, pushed: int}>, totals: array{completed: int, failed: int, pushed: int}, peak: int, queues: list<array{connection: string, queue: string, available: bool, completed: int, failed: int, pushed: int}>}
     */
    public function read(array $queues, string $range): array
    {
        $range = self::range($range);
        $targets = $this->targets($queues);
        $key = 'queen:dashboard:throughput:' . sha1(json_encode([$range, $targets], JSON_THROW_ON_ERROR));

        $cache = $this->cacheStore();
        if ($cache !== null) {
            try {
                $cached = $cache->get($key);
                if (is_array($cached) && ($cached['range'] ?? null) === $range) {
                    return $cached;
                }
            } catch (\Throwable) {
                // A cache outage costs one broker read per render, nothing more.
            }
        }

        $result = $this->fetch($targets, $range);

        if ($cache !== null) {
            try {
                $cache->put($key, $result, self::CACHE_SECONDS);
            } catch (\Throwable) {
                // Same as above: the next render reads the broker again.
            }
        }

        return $result;
    }

    /**
     * @param list<array<string, mixed>> $queues
     * @return list<array{connection: string, queue: string}>
     */
    private function targets(array $queues): array
    {
        $targets = [];
        foreach ($queues as $queue) {
            $connection = $queue['connection'] ?? null;
            $name = $queue['queue'] ?? null;
            if (!is_string($connection) || !is_string($name) || $connection === '' || $name === '') {
                continue;
            }
            // Counters are per queue, not per consumer group.
            $targets[$connection . "\0" . $name] = ['connection' => $connection, 'queue' => $name];
            if (count($targets) >= self::MAX_QUEUES) {
                break;
            }
        }

        return array_values($targets);
    }

    /**
     * @param list<array{connection: string, queue: string}> $targets
     * @return array<string, mixed>
     */
    private function fetch(array $targets, string $range): array
    {
        $to = $this->clock !== null ? ($this->clock)() : time();
        $from = $to - self::RANGES[$range];
        $width = self::bucketMinutes(self::RANGES[$range]) * 60;
        $params = ['from' => gmdate('Y-m-d\TH:i:s\Z', $from), 'to' => gmdate('Y-m-d\TH:i:s\Z', $to)];

        $promises = [];
        $clients = [];
        foreach ($targets as $index => $target) {
            try {
                $clients[$target['connection']] ??= ($this->queenFor)($target['connection']);
                $promises[$index] = $clients[$target['connection']]->admin()->getQueueOpsAsync(
                    $params + ['queue' => $target['queue']],
                    $this->timeoutMillis,
                );
            } catch (\Throwable) {
                // An unresolvable connection leaves only its queues unavailable.
            }
        }
        $settled = $promises === [] ? [] : Utils::settle($promises)->wait();

        $first = intdiv($from, $width) * $width;
        $last = intdiv($to, $width) * $width;
        if (($last - $first) / $width >= self::MAX_BUCKETS) {
            $first = $last - (self::MAX_BUCKETS - 1) * $width;
        }
        $grid = [];
        for ($start = $first; $start <= $last; $start += $width) {
            $grid[$start] = ['start' => $start, 'completed' => 0, 'failed' => 0, 'pushed' => 0];
        }

        $rows = [];
        $available = false;
        foreach ($targets as $index => $target) {
            $row = $target + ['available' => false, 'completed' => 0, 'failed' => 0, 'pushed' => 0];
            $outcome = $settled[$index] ?? null;
            $body = is_array($outcome) && ($outcome['state'] ?? null) === PromiseInterface::FULFILLED
                ? $outcome['value']
                : null;
            if (is_array($body) && is_array($body['series'] ?? null) && array_is_list($body['series'])) {
                $row['available'] = true;
                $available = true;
                foreach ($body['series'] as $point) {
                    $counts = $this->point($point, $target['queue']);
                    if ($counts === null) {
                        continue;
                    }
                    $start = intdiv($counts['time'], $width) * $width;
                    // Outside the window: neither drawn nor counted, so the
                    // per-queue totals always add up to the chart.
                    if (!isset($grid[$start])) {
                        continue;
                    }
                    foreach (['completed', 'failed', 'pushed'] as $field) {
                        $row[$field] += $counts[$field];
                        $grid[$start][$field] += $counts[$field];
                    }
                }
            }
            $rows[] = $row;
        }

        $totals = ['completed' => 0, 'failed' => 0, 'pushed' => 0];
        $peak = 0;
        foreach ($grid as $bucket) {
            foreach ($totals as $field => $sum) {
                $totals[$field] = $sum + $bucket[$field];
            }
            $peak = max($peak, $bucket['completed'] + $bucket['failed']);
        }

        return [
            'available' => $available,
            'range' => $range,
            'bucket_minutes' => intdiv($width, 60),
            'from' => $from,
            'to' => $to,
            'buckets' => array_values($grid),
            'totals' => $totals,
            'peak' => $peak,
            'queues' => $rows,
        ];
    }

    /** @return array{time: int, completed: int, failed: int, pushed: int}|null */
    private function point(mixed $point, string $queue): ?array
    {
        if (!is_array($point) || ($point['queueName'] ?? null) !== $queue || !is_string($point['bucket'] ?? null)) {
            return null;
        }
        $time = strtotime($point['bucket']);
        if ($time === false) {
            return null;
        }
        $counts = ['time' => $time];
        foreach (['completed' => 'ackSuccess', 'failed' => 'ackFailed', 'pushed' => 'pushMessages'] as $field => $source) {
            $value = $point[$source] ?? 0;
            if (!is_int($value) || $value < 0) {
                return null;
            }
            $counts[$field] = $value;
        }

        return $counts;
    }

    private function cacheStore(): ?CacheRepository
    {
        if ($this->cache === null) {
            return null;
        }
        try {
            return ($this->cache)();
        } catch (\Throwable) {
            return null;
        }
    }
}
