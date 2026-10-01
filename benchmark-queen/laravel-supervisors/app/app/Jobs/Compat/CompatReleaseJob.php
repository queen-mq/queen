<?php

namespace App\Jobs\Compat;

use App\Support\FailureMatrixLog;

/** Releases itself for five seconds on its first run, then completes. */
final class CompatReleaseJob extends CompatJob
{
    public const DELAY = 5;

    protected function work(FailureMatrixLog $log): void
    {
        $attempt = $this->attempts();
        $log->record($this->runId, $this->jobId, $attempt, 'started', $this->mode);
        if ($this->logged($log, 'released') === 0) {
            $log->record($this->runId, $this->jobId, $attempt, 'released', $this->mode);
            $this->release(self::DELAY);

            return;
        }
        $log->record($this->runId, $this->jobId, $attempt, 'completed', $this->mode);
    }
}
