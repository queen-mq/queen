<?php

namespace App\Jobs;

use App\Support\FailureMatrixLog;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use RuntimeException;
use Throwable;

/**
 * One job of the failure matrix. Every attempt is logged when it starts and
 * when it ends, so an attempt killed in the middle shows as started only.
 *
 * Modes:
 * - `ok`: succeeds after `sleepMs`;
 * - `throw`: always throws;
 * - `throw-once`: throws on its first attempt, succeeds after;
 * - `release-once`: releases itself on its first attempt, succeeds after;
 * - `fail`: marks itself failed without throwing;
 * - `memory`: grows the worker to `allocateMib` MiB in use and keeps it for the
 *   worker's life, then succeeds; above PHP's memory_limit the attempt dies.
 */
class FailureMatrixJob implements ShouldQueue
{
    use Queueable;

    public const MODES = ['ok', 'throw', 'throw-once', 'release-once', 'fail', 'memory'];

    /** Memory kept for the life of the worker, so the worker's --memory check trips. */
    private static array $ballast = [];

    public int $tries;

    public int $backoff;

    public int $timeout;

    public function __construct(
        public readonly string $runId,
        public readonly string $jobId,
        public readonly string $mode,
        public readonly int $sleepMs,
        int $tries,
        int $backoff,
        int $timeout,
        public readonly int $allocateMib = 0,
    ) {
        if (!in_array($mode, self::MODES, true)) {
            throw new RuntimeException("Unknown failure-matrix mode [{$mode}].");
        }
        $this->tries = $tries;
        $this->backoff = $backoff;
        $this->timeout = $timeout;
    }

    public function handle(FailureMatrixLog $log): void
    {
        $attempt = $this->attempts();
        $log->record($this->runId, $this->jobId, $attempt, 'started', $this->mode);

        if ($this->mode === 'throw' || ($this->mode === 'throw-once' && $attempt === 1)) {
            $log->record($this->runId, $this->jobId, $attempt, 'threw', $this->mode);
            throw new RuntimeException("failure matrix: {$this->mode} on attempt {$attempt}");
        }
        if ($this->mode === 'release-once' && $attempt === 1) {
            $log->record($this->runId, $this->jobId, $attempt, 'released', $this->mode);
            $this->release(2);

            return;
        }
        if ($this->mode === 'fail') {
            $log->record($this->runId, $this->jobId, $attempt, 'failed_by_job', $this->mode);
            $this->fail(new RuntimeException('failure matrix: failed by the job'));

            return;
        }
        if ($this->mode === 'memory') {
            $missing = $this->allocateMib * 1024 * 1024 - memory_get_usage(true);
            self::$ballast[] = str_repeat('x', max(1, $missing));
        }
        $this->sleepUntil(hrtime(true) + $this->sleepMs * 1_000_000);
        $log->record($this->runId, $this->jobId, $attempt, 'completed', $this->mode);
    }

    /** Laravel calls this once, when it gives up on the job. */
    public function failed(?Throwable $exception): void
    {
        app(FailureMatrixLog::class)->record(
            $this->runId,
            $this->jobId,
            null,
            'failed_hook',
            $this->mode,
            $exception === null ? null : $exception::class,
        );
    }

    /** A signal must not shorten the declared work. */
    private function sleepUntil(int $deadlineNs): void
    {
        while (($left = $deadlineNs - hrtime(true)) > 0) {
            usleep((int) min(intdiv($left, 1_000), 200_000));
        }
    }
}
