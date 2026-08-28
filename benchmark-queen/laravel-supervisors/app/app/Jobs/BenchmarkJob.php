<?php

namespace App\Jobs;

use App\Support\JsonlResultSink;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;

final class BenchmarkJob implements ShouldQueue
{
    use Queueable;

    public int $tries = 1;

    public function __construct(
        public readonly string $runId,
        public readonly string $jobId,
        public readonly int $enqueuedAtNs,
        public readonly int $sleepMs,
        public readonly int $cpuIterations,
    ) {
    }

    public function handle(JsonlResultSink $sink): void
    {
        $workStartedAt = hrtime(true);

        if ($this->sleepMs > 0) {
            usleep($this->sleepMs * 1000);
        }

        $digest = hash('sha256', $this->runId.':'.$this->jobId, true);
        for ($iteration = 0; $iteration < $this->cpuIterations; ++$iteration) {
            $digest = hash('sha256', $digest.pack('J', $iteration), true);
        }

        $completedAt = hrtime(true);
        $sink->append([
            'run_id' => $this->runId,
            'job_id' => $this->jobId,
            'connection' => $this->job?->getConnectionName() ?? $this->connection ?? 'unknown',
            'queue' => $this->job?->getQueue() ?? $this->queue ?? 'unknown',
            'enqueued_at_ns' => $this->enqueuedAtNs,
            'work_started_at_ns' => $workStartedAt,
            'completed_at_ns' => $completedAt,
            'queue_latency_ns' => max(0, $workStartedAt - $this->enqueuedAtNs),
            'end_to_end_ns' => max(0, $completedAt - $this->enqueuedAtNs),
            'work_duration_ns' => max(0, $completedAt - $workStartedAt),
            'attempt' => (int) $this->attempts(),
            'sleep_ms' => $this->sleepMs,
            'cpu_iterations' => $this->cpuIterations,
            'checksum' => bin2hex($digest),
            'worker_pid' => getmypid(),
            'worker_host' => gethostname() ?: 'unknown',
        ]);
    }
}
