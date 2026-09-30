<?php

namespace Queen\Laravel\Monitoring;

use Illuminate\Contracts\Queue\Job;
use Queen\Queen;

/**
 * Per-job-class metrics, recorded by every worker into the broker's
 * key/value store, so the dashboard shows them for every worker of every
 * host or pod, whichever engine supervises it.
 *
 *   jobs/v1/<bucket>/<worker>   {"classes": {"App\\Jobs\\Send": {"processed": 4, "failed": 1, "runtime_ms": 820}}}
 *
 * A bucket is a five-minute window (its start, zero-padded epoch seconds, so
 * keys sort by time). A worker keeps its counts in memory and overwrites its
 * own key at most once per FLUSH_SECONDS, from its loop when it goes idle,
 * and when it stops: one write per worker every ten seconds, never one per
 * job, and no two workers ever write the same key. Keys expire after a day. Recording is best effort: a failed
 * write is dropped and never reaches the job.
 */
final class JobMetricsRecorder
{
    public const PREFIX = 'jobs/v1/';

    public const BUCKET_SECONDS = 300;

    public const TTL_SECONDS = 90000;

    public const MAX_CLASSES = 200;

    public const OTHER_CLASS = '(other)';

    private const FLUSH_SECONDS = 10;

    private ?int $pid = null;

    private string $worker = '';

    private ?int $bucket = null;

    /** @var array<string, array{processed: int, failed: int, runtime_ms: int}> */
    private array $classes = [];

    /** @var array<int, int> job object id => start in nanoseconds */
    private array $started = [];

    private float $lastFlush = 0.0;

    private bool $dirty = false;

    /**
     * @param \Closure(): ?Queen $queen the client of the metrics connection
     * @param (\Closure(): float)|null $clock
     */
    public function __construct(
        private \Closure $queen,
        private string $namespace,
        private ?\Closure $clock = null,
    ) {
    }

    public function start(Job $job): void
    {
        $this->forThisProcess();
        $this->started[spl_object_id($job)] = hrtime(true);
    }

    public function finish(Job $job, bool $failed): void
    {
        $this->forThisProcess();
        $started = $this->started[spl_object_id($job)] ?? null;
        unset($this->started[spl_object_id($job)]);
        $bucket = self::bucket((int) $this->now());
        if ($this->bucket !== null && $bucket !== $this->bucket) {
            // The previous window is complete: write it before it is reset.
            $this->flush();
            $this->classes = [];
        }
        $this->bucket = $bucket;

        $class = self::className($job);
        if (!isset($this->classes[$class]) && count($this->classes) >= self::MAX_CLASSES) {
            $class = self::OTHER_CLASS;
        }
        $counts = $this->classes[$class] ?? ['processed' => 0, 'failed' => 0, 'runtime_ms' => 0];
        $counts[$failed ? 'failed' : 'processed']++;
        if ($started !== null) {
            $counts['runtime_ms'] += intdiv(hrtime(true) - $started, 1_000_000);
        }
        $this->classes[$class] = $counts;
        $this->dirty = true;

        if ($this->now() - $this->lastFlush >= self::FLUSH_SECONDS) {
            $this->flush();
        }
    }

    /** Called on every worker loop: writes counts left by a burst that ended. */
    public function tick(): void
    {
        $this->forThisProcess();
        if ($this->dirty && $this->now() - $this->lastFlush >= self::FLUSH_SECONDS) {
            $this->flush();
        }
    }

    public function flush(): void
    {
        if (!$this->dirty || $this->bucket === null) {
            return;
        }
        $this->dirty = false;
        $this->lastFlush = $this->now();
        try {
            ($this->queen)()?->kv()->put(
                $this->namespace,
                self::PREFIX . sprintf('%010d', $this->bucket) . '/' . $this->worker,
                ['classes' => $this->classes],
                ['ttlSeconds' => self::TTL_SECONDS],
            );
        } catch (\Throwable) {
            // Metrics never fail a job; this window is written again later.
        }
    }

    public static function bucket(int $epoch): int
    {
        return intdiv($epoch, self::BUCKET_SECONDS) * self::BUCKET_SECONDS;
    }

    private static function className(Job $job): string
    {
        try {
            $name = $job->resolveName();
        } catch (\Throwable) {
            $name = null;
        }
        $name = is_string($name) ? preg_replace('/[\x00-\x1F\x7F]/', '', $name) : '';

        return $name === '' || $name === null ? 'Unknown' : substr($name, 0, 255);
    }

    /** A preforked worker inherits its fork server's recorder: start over. */
    private function forThisProcess(): void
    {
        $pid = getmypid();
        if ($this->pid === $pid) {
            return;
        }
        $this->pid = $pid;
        $this->worker = bin2hex(random_bytes(8));
        $this->bucket = null;
        $this->classes = [];
        $this->started = [];
        $this->dirty = false;
    }

    private function now(): float
    {
        return $this->clock !== null ? (float) ($this->clock)() : microtime(true);
    }
}
