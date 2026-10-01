<?php

namespace App\Jobs\Compat;

use App\Support\FailureMatrixLog;

/** Cancels its own batch from inside the job, as Laravel's documentation does. */
final class CompatBatchCancellerJob extends CompatJob
{
    protected function work(FailureMatrixLog $log): void
    {
        $this->batch()?->cancel();
        $log->record($this->runId, $this->jobId, $this->attempts(), 'cancelled_batch', $this->mode);
    }
}
