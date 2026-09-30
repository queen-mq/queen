<?php

namespace Queen\Laravel\Dashboard;

use Illuminate\Contracts\Cache\Repository as CacheRepository;
use Queen\Laravel\Monitoring\JobMetricsRecorder;
use Queen\Queen;

/**
 * Sums the per-job-class metrics every worker records (see
 * JobMetricsRecorder) over a window: one paged getPrefix from the window's
 * first bucket, since keys sort by time.
 *
 * The rows are untrusted input: every count is validated and a class list is
 * bounded, so a malformed or hostile row cannot distort the table.
 */
final class JobMetricsReader
{
    /** Window lengths in seconds; the recorder keeps a day. */
    public const RANGES = ['1h' => 3600, '6h' => 21600, '24h' => 86400];

    public const DEFAULT_RANGE = '1h';

    /** Under the broker's default QUEEN_KV_MAX_KEYS_PER_CALL (1024). */
    private const PAGE_LIMIT = 1000;

    /** 50,000 rows: a day of five-minute buckets for about 170 workers. */
    private const MAX_PAGES = 50;

    private const MAX_CLASSES = 500;

    private const CACHE_SECONDS = 15;

    /**
     * @param (\Closure(): (CacheRepository|null))|null $cache
     * @param (\Closure(): int)|null $clock
     */
    public function __construct(
        private Queen $queen,
        private string $namespace,
        private ?\Closure $cache = null,
        private ?\Closure $clock = null,
    ) {
    }

    public static function range(mixed $value): string
    {
        return is_string($value) && array_key_exists($value, self::RANGES) ? $value : self::DEFAULT_RANGE;
    }

    /**
     * `truncated` is true when the window held more rows than one read takes:
     * the table then covers only the window's oldest part.
     *
     * @return array{available: bool, truncated: bool, range: string, minutes: int, totals: array{processed: int, failed: int}, classes: list<array{class: string, processed: int, failed: int, runtime_ms: int, average_ms: ?int, per_minute: float}>}
     */
    public function read(string $range): array
    {
        $range = self::range($range);
        $cache = $this->cache !== null ? ($this->cache)() : null;
        $key = 'queen:dashboard:job-metrics:' . $range;
        if ($cache !== null) {
            try {
                $cached = $cache->get($key);
                if (is_array($cached)) {
                    return $cached;
                }
            } catch (\Throwable) {
                // Read the broker instead.
            }
        }

        $result = $this->fromBroker($range);
        if ($cache !== null && $result['available']) {
            try {
                $cache->put($key, $result, self::CACHE_SECONDS);
            } catch (\Throwable) {
                // Serving uncached is fine.
            }
        }

        return $result;
    }

    /** @return array<string, mixed> */
    private function fromBroker(string $range): array
    {
        $seconds = self::RANGES[$range];
        $now = $this->clock !== null ? (int) ($this->clock)() : time();
        $first = JobMetricsRecorder::bucket($now - $seconds + JobMetricsRecorder::BUCKET_SECONDS);
        $empty = [
            'available' => false,
            'truncated' => false,
            'range' => $range,
            'minutes' => intdiv($seconds, 60),
            'totals' => ['processed' => 0, 'failed' => 0],
            'classes' => [],
        ];

        $classes = [];
        $truncated = false;
        // The last key before the window's first bucket; `after` is exclusive.
        $after = JobMetricsRecorder::PREFIX . sprintf('%010d', $first - 1);
        try {
            for ($page = 0; $page < self::MAX_PAGES; ++$page) {
                $result = $this->queen->kv()->getPrefix(
                    $this->namespace,
                    JobMetricsRecorder::PREFIX,
                    ['after' => $after, 'limit' => self::PAGE_LIMIT],
                );
                $rows = $result['rows'] ?? null;
                if (!is_array($rows) || !array_is_list($rows)) {
                    return $empty;
                }
                foreach ($rows as $row) {
                    $this->add($classes, is_array($row) ? ($row['value']['classes'] ?? null) : null);
                    if (is_array($row) && is_string($row['key'] ?? null)) {
                        $after = $row['key'];
                    }
                }
                $truncated = ($result['truncated'] ?? false) === true && $rows !== [];
                if (!$truncated) {
                    break;
                }
            }
        } catch (\Throwable) {
            return $empty;
        }

        $minutes = max(1, intdiv($seconds, 60));
        $table = [];
        foreach ($classes as $class => $counts) {
            $runs = $counts['processed'] + $counts['failed'];
            $table[] = [
                'class' => (string) $class,
                ...$counts,
                'average_ms' => $runs > 0 ? intdiv($counts['runtime_ms'], $runs) : null,
                'per_minute' => round($runs / $minutes, 2),
            ];
        }
        usort($table, fn (array $a, array $b): int => [$b['processed'] + $b['failed'], $a['class']] <=> [$a['processed'] + $a['failed'], $b['class']]);

        return [
            ...$empty,
            'available' => true,
            'truncated' => $truncated,
            'totals' => [
                'processed' => array_sum(array_column($table, 'processed')),
                'failed' => array_sum(array_column($table, 'failed')),
            ],
            'classes' => $table,
        ];
    }

    /** @param array<string, array{processed: int, failed: int, runtime_ms: int}> $classes */
    private function add(array &$classes, mixed $recorded): void
    {
        if (!is_array($recorded) || array_is_list($recorded)) {
            return;
        }
        foreach ($recorded as $class => $counts) {
            if (!is_string($class) || $class === '' || strlen($class) > 255 || preg_match('/[\x00-\x1F\x7F]/', $class) === 1
                || !is_array($counts)) {
                continue;
            }
            $values = [];
            foreach (['processed', 'failed', 'runtime_ms'] as $field) {
                $value = $counts[$field] ?? 0;
                if (!is_int($value) || $value < 0 || $value > PHP_INT_MAX >> 8) {
                    continue 2;
                }
                $values[$field] = $value;
            }
            if (!isset($classes[$class]) && count($classes) >= self::MAX_CLASSES) {
                $class = JobMetricsRecorder::OTHER_CLASS;
            }
            $current = $classes[$class] ?? ['processed' => 0, 'failed' => 0, 'runtime_ms' => 0];
            foreach ($values as $field => $value) {
                $current[$field] += $value;
            }
            $classes[$class] = $current;
        }
    }
}
