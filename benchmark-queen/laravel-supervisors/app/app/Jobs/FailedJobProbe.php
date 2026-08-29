<?php

namespace App\Jobs;

use App\Support\JsonlResultSink;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use RuntimeException;

/**
 * Fails once behind a durable marker, then succeeds after `queue:retry`.
 *
 * The marker makes this a deterministic lifecycle probe, not an exactly-once
 * guarantee for arbitrary application effects.
 */
final class FailedJobProbe implements ShouldQueue
{
    use Queueable;

    public int $tries = 1;

    public function __construct(
        public readonly string $runId,
        public readonly string $probeId,
        public readonly int $enqueuedAtNs,
    ) {
    }

    public function handle(JsonlResultSink $sink): void
    {
        $this->assertIdentifier($this->runId, 'runId');
        $this->assertIdentifier($this->probeId, 'probeId');
        $resultsDirectory = (string) config('benchmark.results_directory');
        $markerDirectory = $resultsDirectory.'/'.$this->runId.'/failure-probes';
        if (!is_dir($markerDirectory)
            && !mkdir($markerDirectory, 0770, true)
            && !is_dir($markerDirectory)) {
            throw new RuntimeException("Unable to create failure-probe directory [{$markerDirectory}].");
        }
        $marker = $markerDirectory.'/'.$this->probeId.'.failed-once';
        $stream = @fopen($marker, 'x+b');
        if ($stream !== false) {
            try {
                $document = json_encode([
                    'run_id' => $this->runId,
                    'probe_id' => $this->probeId,
                    'failed_at_ns' => hrtime(true),
                    'worker_pid' => getmypid(),
                ], JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES)."\n";
                if (fwrite($stream, $document) !== strlen($document) || !fflush($stream)) {
                    throw new RuntimeException("Unable to persist failure-probe marker [{$marker}].");
                }
                if (function_exists('fsync') && !fsync($stream)) {
                    throw new RuntimeException("Unable to sync failure-probe marker [{$marker}].");
                }
            } finally {
                fclose($stream);
            }

            throw new RuntimeException("Intentional first failure for probe [{$this->probeId}].");
        }
        if (!is_file($marker)) {
            throw new RuntimeException("Unable to create or inspect failure-probe marker [{$marker}].");
        }

        $completedAt = hrtime(true);
        $sink->append([
            'run_id' => $this->runId,
            'job_id' => 'failure-probe:'.$this->probeId,
            'connection' => $this->job?->getConnectionName() ?? $this->connection ?? 'unknown',
            'queue' => $this->job?->getQueue() ?? $this->queue ?? 'unknown',
            'enqueued_at_ns' => $this->enqueuedAtNs,
            'work_started_at_ns' => $completedAt,
            'completed_at_ns' => $completedAt,
            'queue_latency_ns' => max(0, $completedAt - $this->enqueuedAtNs),
            'end_to_end_ns' => max(0, $completedAt - $this->enqueuedAtNs),
            'work_duration_ns' => 0,
            'attempt' => (int) $this->attempts(),
            'sleep_ms' => 0,
            'cpu_iterations' => 0,
            'checksum' => hash('sha256', $this->runId.':'.$this->probeId),
            'worker_pid' => getmypid(),
            'worker_host' => gethostname() ?: 'unknown',
        ]);
    }

    private function assertIdentifier(string $value, string $label): void
    {
        if (in_array($value, ['.', '..'], true)
            || preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $value) !== 1) {
            throw new RuntimeException("Failure-probe {$label} is invalid.");
        }
    }
}
