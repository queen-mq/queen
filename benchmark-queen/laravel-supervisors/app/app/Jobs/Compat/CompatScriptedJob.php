<?php

namespace App\Jobs\Compat;

use App\Support\FailureMatrixLog;
use RuntimeException;

/**
 * Follows a script, one letter per run: `R` releases the job for
 * `releaseDelay` seconds, `T` throws, `C` completes; past the end of the
 * script it completes. Runs are counted in the log, not by attempts(), so the
 * script does not depend on how a backend counts a release.
 */
final class CompatScriptedJob extends CompatJob
{
    public string $script = 'C';

    public int $releaseDelay = 1;

    public ?int $maxExceptions = null;

    public static function make(string $runId, string $jobId, string $script, int $tries, ?int $maxExceptions = null): self
    {
        $job = new self($runId, $jobId, 'scripted', 0, $tries);
        $job->script = $script;
        $job->maxExceptions = $maxExceptions;

        return $job;
    }

    protected function work(FailureMatrixLog $log): void
    {
        $step = $this->script[$this->logged($log, 'started')] ?? 'C';
        $attempt = $this->attempts();
        $log->record($this->runId, $this->jobId, $attempt, 'started', $this->mode);
        if ($step === 'R') {
            $log->record($this->runId, $this->jobId, $attempt, 'released', $this->mode);
            $this->release($this->releaseDelay);

            return;
        }
        if ($step === 'T') {
            $log->record($this->runId, $this->jobId, $attempt, 'threw', $this->mode);
            throw new RuntimeException("compat: scripted {$this->jobId} attempt {$attempt}");
        }
        $log->record($this->runId, $this->jobId, $attempt, 'completed', $this->mode);
    }
}
