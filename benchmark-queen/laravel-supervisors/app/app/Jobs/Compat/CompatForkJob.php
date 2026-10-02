<?php

namespace App\Jobs\Compat;

use App\Support\FailureMatrixLog;
use RuntimeException;

/**
 * Forks: the child logs its own pid and exits normally, so its destructors
 * and shutdown functions run; the parent waits for it, then completes. The
 * child's exit must leave the parent's worker, and its lease, alone.
 */
final class CompatForkJob extends CompatJob
{
    protected function work(FailureMatrixLog $log): void
    {
        $attempt = $this->attempts();
        $log->record($this->runId, $this->jobId, $attempt, 'started', $this->mode);
        $child = pcntl_fork();
        if ($child === -1) {
            throw new RuntimeException("compat: {$this->jobId} could not fork");
        }
        if ($child === 0) {
            $log->record($this->runId, $this->jobId, $attempt, 'child_ran', $this->mode);
            exit(0);
        }
        pcntl_waitpid($child, $status);
        $exited = pcntl_wifexited($status) && pcntl_wexitstatus($status) === 0;
        $log->record($this->runId, $this->jobId, $attempt, $exited ? 'child_exited' : 'child_failed', $this->mode);
        $log->record($this->runId, $this->jobId, $attempt, 'completed', $this->mode);
    }
}
