<?php

namespace App\Jobs\Compat;

use App\Support\FailureMatrixLog;
use Illuminate\Bus\Batchable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Illuminate\Support\Facades\Cache;
use RuntimeException;
use Throwable;

/**
 * A job of the Laravel compatibility lanes. Modes: `ok` succeeds after
 * `sleepMs`; `throw` always throws; `fail-once` throws the first time its
 * key is seen in the cache and succeeds after, also after queue:retry, which
 * resets the attempt counter.
 */
class CompatJob implements ShouldQueue
{
    use Batchable;
    use Queueable;

    // Not readonly: Laravel restores a job's properties when it unserializes
    // it, and PHP refuses to set a parent's readonly property from a subclass.
    public function __construct(
        public string $runId,
        public string $jobId,
        public string $mode = 'ok',
        public int $sleepMs = 0,
        int $tries = 1,
    ) {
        $this->tries = $tries;
    }

    public int $tries = 1;

    public function handle(FailureMatrixLog $log): void
    {
        if ($this->batch()?->cancelled()) {
            $log->record($this->runId, $this->jobId, $this->attempts(), 'skipped_cancelled_batch', $this->mode);

            return;
        }
        $this->work($log);
    }

    public function failed(?Throwable $exception): void
    {
        app(FailureMatrixLog::class)->record($this->runId, $this->jobId, null, 'failed_hook', $this->mode,
            $exception === null ? null : $exception::class);
    }

    protected function work(FailureMatrixLog $log): void
    {
        $attempt = $this->attempts();
        $log->record($this->runId, $this->jobId, $attempt, 'started', $this->mode);
        $first = $this->mode === 'fail-once' && Cache::add("compat:{$this->runId}:{$this->jobId}", true, 3600);
        if ($this->mode === 'throw' || $first) {
            $log->record($this->runId, $this->jobId, $attempt, 'threw', $this->mode);
            throw new RuntimeException("compat: {$this->mode} {$this->jobId} attempt {$attempt}");
        }
        $deadline = hrtime(true) + $this->sleepMs * 1_000_000;
        while (($left = $deadline - hrtime(true)) > 0) {
            usleep((int) min(intdiv($left, 1_000), 200_000));
        }
        $log->record($this->runId, $this->jobId, $attempt, 'completed', $this->mode);
    }
}
