<?php

namespace Queen\Laravel\Monitoring;

use Illuminate\Contracts\Queue\Job;
use Queen\Queen;

/**
 * Monitored tags, as in Horizon: the dashboard chooses tags to watch, and
 * workers record every job that carries one, in the broker's key/value
 * store, so the dashboard lists them for every worker of every host.
 *
 *   tags/v1/monitored                      ["App\\Models\\User:42", ...]   (forever)
 *   tags/v1/jobs/<tag>/<newest-first>-<n>  {tag, job_id, class, queue, status, attempts, runtime_ms, at}   (TTL)
 *
 * <tag> is FNV-1a 64 of the tag; <newest-first> is a millisecond timestamp
 * subtracted from a constant, so a prefix scan returns the newest job first.
 * A worker reads the monitored list at most every REFRESH_SECONDS. Recording
 * is best effort and never fails a job.
 */
final class TagMonitor
{
    public const PREFIX = 'tags/v1/';

    public const MONITORED_KEY = 'tags/v1/monitored';

    public const MAX_MONITORED = 50;

    private const REFRESH_SECONDS = 30;

    private const NEWEST_FIRST = 9_999_999_999_999;

    /** @var list<string>|null */
    private ?array $monitored = null;

    private float $refreshedAt = 0.0;

    /** @var array<int, int> job object id => start in nanoseconds */
    private array $started = [];

    /**
     * @param \Closure(): ?Queen $queen
     * @param (\Closure(): float)|null $clock
     */
    public function __construct(
        private \Closure $queen,
        private string $namespace,
        private int $retentionSeconds = 86400,
        private ?\Closure $clock = null,
    ) {
    }

    public function start(Job $job): void
    {
        $this->started[spl_object_id($job)] = hrtime(true);
    }

    public function record(Job $job, string $status): void
    {
        $started = $this->started[spl_object_id($job)] ?? null;
        unset($this->started[spl_object_id($job)]);
        try {
            $tags = JobTags::normalize($job->payload()['tags'] ?? []);
            if ($tags === []) {
                return;
            }
            $watched = array_values(array_intersect($tags, $this->monitoredForWorker()));
            if ($watched === []) {
                return;
            }
            $now = $this->now();
            $entry = [
                'job_id' => (string) $job->getJobId(),
                'class' => substr((string) $job->resolveName(), 0, 255),
                'queue' => (string) $job->getQueue(),
                'status' => $status,
                'attempts' => $job->attempts(),
                'runtime_ms' => $started === null ? null : intdiv(hrtime(true) - $started, 1_000_000),
                'at' => gmdate('Y-m-d\TH:i:s\Z', (int) $now),
            ];
            $operations = [];
            foreach ($watched as $tag) {
                $operations[] = [
                    'op' => 'put',
                    'ns' => $this->namespace,
                    'key' => self::jobKey($tag, $now),
                    'value' => ['tag' => $tag, ...$entry],
                    'ttlSeconds' => $this->retentionSeconds,
                ];
            }
            ($this->queen)()?->kv()->batch($operations);
        } catch (\Throwable) {
            // Monitoring never fails a job.
        }
    }

    /** @return list<string> */
    public function monitored(): array
    {
        $result = ($this->queen)()?->kv()->get($this->namespace, self::MONITORED_KEY);

        return JobTags::normalize(($result['found'] ?? false) === true ? ($result['value'] ?? []) : []);
    }

    /** Add or remove a monitored tag; compare-and-swap, retried on a concurrent change. */
    public function change(string $tag, bool $monitor): void
    {
        $tag = JobTags::normalize([$tag])[0] ?? throw new \InvalidArgumentException('The tag is empty or too long.');
        $queen = ($this->queen)() ?? throw new \RuntimeException('The tag connection is not a Queen connection.');
        for ($attempt = 0; $attempt < 5; ++$attempt) {
            $current = $queen->kv()->get($this->namespace, self::MONITORED_KEY);
            $found = ($current['found'] ?? false) === true;
            $tags = JobTags::normalize($found ? ($current['value'] ?? []) : []);
            $next = $monitor ? array_values(array_unique([...$tags, $tag])) : array_values(array_diff($tags, [$tag]));
            if ($monitor && count($next) > self::MAX_MONITORED) {
                throw new \InvalidArgumentException('At most ' . self::MAX_MONITORED . ' tags can be monitored.');
            }
            $written = $queen->kv()->put($this->namespace, self::MONITORED_KEY, $next, [
                'forever' => true,
                'expect' => $found && is_int($current['version'] ?? null) ? $current['version'] : 0,
            ]);
            if (($written['applied'] ?? false) === true) {
                return;
            }
        }

        throw new \RuntimeException('The monitored tags changed concurrently; try again.');
    }

    /** @return list<array<string, mixed>> the newest recorded jobs of a tag */
    public function recent(string $tag, int $limit = 25): array
    {
        $result = ($this->queen)()?->kv()->getPrefix($this->namespace, self::jobPrefix($tag), ['limit' => max(1, min(100, $limit))]);
        $jobs = [];
        foreach (is_array($result['rows'] ?? null) ? $result['rows'] : [] as $row) {
            $value = is_array($row) ? ($row['value'] ?? null) : null;
            if (is_array($value) && ($value['tag'] ?? null) === $tag) {
                $jobs[] = [
                    'job_id' => is_string($value['job_id'] ?? null) ? substr($value['job_id'], 0, 128) : null,
                    'class' => is_string($value['class'] ?? null) ? substr($value['class'], 0, 255) : 'Unknown',
                    'queue' => is_string($value['queue'] ?? null) ? substr($value['queue'], 0, 255) : null,
                    'status' => in_array($value['status'] ?? null, ['completed', 'failed'], true) ? $value['status'] : 'unknown',
                    'attempts' => is_int($value['attempts'] ?? null) ? $value['attempts'] : null,
                    'runtime_ms' => is_int($value['runtime_ms'] ?? null) ? $value['runtime_ms'] : null,
                    'at' => is_string($value['at'] ?? null) && strlen($value['at']) <= 32 ? $value['at'] : null,
                ];
            }
        }

        return $jobs;
    }

    public static function jobPrefix(string $tag): string
    {
        return self::PREFIX . 'jobs/' . hash('fnv1a64', $tag) . '/';
    }

    private static function jobKey(string $tag, float $now): string
    {
        return self::jobPrefix($tag)
            . sprintf('%013d', self::NEWEST_FIRST - (int) floor($now * 1000))
            . '-' . bin2hex(random_bytes(4));
    }

    /** @return list<string> */
    private function monitoredForWorker(): array
    {
        if ($this->monitored === null || $this->now() - $this->refreshedAt >= self::REFRESH_SECONDS) {
            $this->refreshedAt = $this->now();
            try {
                $this->monitored = $this->monitored();
            } catch (\Throwable) {
                $this->monitored ??= [];
            }
        }

        return $this->monitored;
    }

    private function now(): float
    {
        return $this->clock !== null ? (float) ($this->clock)() : microtime(true);
    }
}
