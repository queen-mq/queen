<?php

namespace App\Jobs\Compat;

use App\Support\FailureMatrixLog;
use DateTimeInterface;
use RuntimeException;

/**
 * retryUntil() as applications write it: `now()` plus a duration. Laravel
 * calls it once, when it builds the payload at dispatch, and the worker reads
 * the timestamp from the payload; were it called again at each attempt, the
 * deadline would move and the job would never fail. Each run logs the
 * deadline the worker sees. It throws on every run up to MAX_RUNS, then
 * completes: a moving deadline ends there instead of running forever.
 */
final class CompatDeadlineJob extends CompatJob
{
    public const MAX_RUNS = 15;

    public int $deadlineSeconds = 6;

    public int $backoff = 1;

    public function retryUntil(): DateTimeInterface
    {
        return now()->addSeconds($this->deadlineSeconds);
    }

    protected function work(FailureMatrixLog $log): void
    {
        $runs = $this->logged($log, 'started');
        $attempt = $this->attempts();
        $log->record($this->runId, $this->jobId, $attempt, 'started', $this->mode, null,
            ['retry_until' => $this->job?->retryUntil()]);
        if ($runs + 1 < self::MAX_RUNS) {
            $log->record($this->runId, $this->jobId, $attempt, 'threw', $this->mode);
            throw new RuntimeException("compat: deadline {$this->jobId} attempt {$attempt}");
        }
        $log->record($this->runId, $this->jobId, $attempt, 'completed', $this->mode);
    }
}
