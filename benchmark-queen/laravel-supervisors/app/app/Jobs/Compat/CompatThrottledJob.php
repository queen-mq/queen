<?php

namespace App\Jobs\Compat;

use App\Support\FailureMatrixLog;
use Illuminate\Queue\Middleware\ThrottlesExceptions;
use RuntimeException;

/**
 * Throws on its first three runs, then succeeds. ThrottlesExceptions(2, 4)
 * releases it after each exception; once two exceptions fall within four
 * seconds, it holds the job back until those four seconds have passed.
 */
final class CompatThrottledJob extends CompatJob
{
    public const THROWS = 3;

    public function middleware(): array
    {
        return [(new ThrottlesExceptions(2, 4))->by("compat:{$this->runId}:{$this->jobId}")];
    }

    protected function work(FailureMatrixLog $log): void
    {
        $runs = $this->logged($log, 'started');
        $attempt = $this->attempts();
        $log->record($this->runId, $this->jobId, $attempt, 'started', $this->mode);
        if ($runs < self::THROWS) {
            $log->record($this->runId, $this->jobId, $attempt, 'threw', $this->mode);
            throw new RuntimeException("compat: throttled {$this->jobId} run " . ($runs + 1));
        }
        $log->record($this->runId, $this->jobId, $attempt, 'completed', $this->mode);
    }
}
