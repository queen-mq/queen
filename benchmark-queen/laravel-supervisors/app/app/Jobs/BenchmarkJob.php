<?php

namespace App\Jobs;

use App\Support\BenchmarkEffectLedger;
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

    public function handle(JsonlResultSink $sink, BenchmarkEffectLedger $ledger): void
    {
        $workStartedAt = hrtime(true);
        $attempt = (int) $this->attempts();
        $ledgerAttemptId = $ledger->startAttempt(
            $this->runId,
            $this->jobId,
            $attempt,
            $workStartedAt,
        );

        try {
            if ($this->sleepMs > 0) {
                $this->sleepUntilMonotonicDeadline($this->sleepMs);
            }

            $digest = hash('sha256', $this->runId.':'.$this->jobId, true);
            for ($iteration = 0; $iteration < $this->cpuIterations; ++$iteration) {
                $digest = hash('sha256', $digest.pack('J', $iteration), true);
            }

            $completedAt = hrtime(true);
            $checksum = bin2hex($digest);
            $ledgerEffect = $ledgerAttemptId === null
                ? null
                : $ledger->commitEffect(
                    $this->runId,
                    $this->jobId,
                    $ledgerAttemptId,
                    $checksum,
                    $completedAt,
                );
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
                'attempt' => $attempt,
                'sleep_ms' => $this->sleepMs,
                'cpu_iterations' => $this->cpuIterations,
                'checksum' => $checksum,
                'worker_pid' => getmypid(),
                'worker_host' => gethostname() ?: 'unknown',
                'ledger_attempt_id' => $ledgerAttemptId,
                'ledger_effect_id' => $ledgerEffect['effect_id'] ?? null,
                'ledger_effect_outcome' => $ledgerEffect['outcome'] ?? null,
                'ledger_effect_created' => $ledgerEffect['created'] ?? null,
            ]);
            if ($ledgerAttemptId !== null) {
                $ledger->completeAttempt($this->runId, $ledgerAttemptId, hrtime(true));
            }
        } catch (\Throwable $exception) {
            if ($ledgerAttemptId !== null) {
                try {
                    $ledger->failAttempt($this->runId, $ledgerAttemptId, hrtime(true), $exception);
                } catch (\Throwable) {
                    // Preserve the original job failure. The verifier will
                    // expose the unfinalized attempt as crash/unknown evidence.
                }
            }
            throw $exception;
        }
    }

    /**
     * Preserve the declared workload duration when a supervisor signal
     * interrupts usleep(). A single interrupted sleep made fault campaigns
     * record an effect earlier than the manifest promised and could conceal
     * whether lease fencing really happened before job completion.
     */
    private function sleepUntilMonotonicDeadline(int $milliseconds): void
    {
        $deadline = hrtime(true) + $milliseconds * 1_000_000;
        while (($remainingNanoseconds = $deadline - hrtime(true)) > 0) {
            $remainingMicroseconds = (int) max(1, intdiv($remainingNanoseconds + 999, 1000));
            usleep(min(1_000_000, $remainingMicroseconds));
        }
    }
}
