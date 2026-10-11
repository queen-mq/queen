<?php

namespace App\Jobs;

use App\Support\FailureMatrixLog;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Queen\Laravel\Contracts\QueenPartitionable;
use Throwable;

/**
 * A failure-matrix job whose $timeout is a numeric string, as
 * `$this->timeout = env('JOB_TIMEOUT')` leaves it. Laravel's worker accepts
 * it. The job succeeds at once, in a named Queen partition.
 */
final class FailureMatrixStringTimeoutJob implements ShouldQueue, QueenPartitionable
{
    use Queueable;

    /** @var string */
    public $timeout;

    public int $tries = 1;

    public function __construct(public string $runId, public string $jobId, public string $partition, string $timeout)
    {
        $this->timeout = $timeout;
    }

    public function handle(FailureMatrixLog $log): void
    {
        $attempt = $this->attempts();
        $log->record($this->runId, $this->jobId, $attempt, 'started', 'string-timeout', null,
            ['code' => app('bench.deployed_code')]);
        $log->record($this->runId, $this->jobId, $attempt, 'completed', 'string-timeout');
    }

    public function failed(?Throwable $exception): void
    {
        app(FailureMatrixLog::class)->record($this->runId, $this->jobId, null, 'failed_hook', 'string-timeout',
            $exception === null ? null : $exception::class);
    }

    public function queenPartition(): string
    {
        return $this->partition;
    }
}
