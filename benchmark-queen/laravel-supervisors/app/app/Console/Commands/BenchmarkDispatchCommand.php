<?php

namespace App\Console\Commands;

use App\Jobs\BenchmarkJob;
use App\Support\JsonlResultSink;
use Illuminate\Console\Command;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Support\Str;
use InvalidArgumentException;
use JsonException;

final class BenchmarkDispatchCommand extends Command
{
    protected $signature = 'bench:dispatch
        {--run-id= : Stable run identifier; a UUID is generated when omitted}
        {--jobs=1000 : Number of jobs to enqueue}
        {--sleep-ms=10 : Sleep performed by every job}
        {--cpu-iterations=0 : SHA-256 rounds performed by every job}
        {--connection= : Queue connection; defaults to BENCH_CONNECTION}
        {--queue= : Queue name; defaults to BENCH_QUEUE}
        {--dispatch-mode= : single or bulk; defaults to BENCH_DISPATCH_MODE}
        {--metadata= : Dispatch manifest path; defaults to <results>/<run-id>/dispatch.json}';

    protected $description = 'Dispatch one deterministic benchmark burst and print its JSON manifest';

    public function handle(JsonlResultSink $sink, QueueFactory $queues): int
    {
        $runId = $this->option('run-id');
        $runId = is_string($runId) && $runId !== '' ? $runId : (string) Str::uuid();
        $this->identifier($runId, 'run-id');

        $jobs = $this->integerOption('jobs', 1, 1_000_000);
        $sleepMs = $this->integerOption('sleep-ms', 0, 60_000);
        $cpuIterations = $this->integerOption('cpu-iterations', 0, 10_000_000);
        $connection = $this->stringOption('connection', (string) config('benchmark.connection'));
        $queue = $this->stringOption('queue', (string) config('benchmark.queue'));
        $dispatchMode = $this->stringOption('dispatch-mode', (string) config('benchmark.dispatch_mode'));
        $this->identifier($connection, 'connection');
        $this->identifier($queue, 'queue', forbidComma: true);
        if (!in_array($dispatchMode, ['single', 'bulk'], true)) {
            throw new InvalidArgumentException('--dispatch-mode must be single or bulk.');
        }

        $sink->reserveRun($runId);
        $startedAt = hrtime(true);
        if ($dispatchMode === 'single') {
            for ($index = 0; $index < $jobs; ++$index) {
                $jobId = sprintf('%09d', $index);
                BenchmarkJob::dispatch(
                    runId: $runId,
                    jobId: $jobId,
                    enqueuedAtNs: hrtime(true),
                    sleepMs: $sleepMs,
                    cpuIterations: $cpuIterations,
                )->onConnection($connection)->onQueue($queue);
            }
        } else {
            $batchSize = (int) config('benchmark.queen_bulk_batch');
            $connectionQueue = $queues->connection($connection);
            for ($offset = 0; $offset < $jobs; $offset += $batchSize) {
                $batch = [];
                $limit = min($jobs, $offset + $batchSize);
                for ($index = $offset; $index < $limit; ++$index) {
                    $job = new BenchmarkJob(
                        runId: $runId,
                        jobId: sprintf('%09d', $index),
                        enqueuedAtNs: hrtime(true),
                        sleepMs: $sleepMs,
                        cpuIterations: $cpuIterations,
                    );
                    $job->onConnection($connection)->onQueue($queue);
                    $batch[] = $job;
                }
                $connectionQueue->bulk($batch, '', $queue);
            }
        }
        $completedAt = hrtime(true);

        $manifest = [
            'run_id' => $runId,
            'jobs' => $jobs,
            'connection' => $connection,
            'queue' => $queue,
            'dispatch_mode' => $dispatchMode,
            'dispatch_batch_size' => $dispatchMode === 'bulk'
                ? (int) config('benchmark.queen_bulk_batch')
                : 1,
            'sleep_ms' => $sleepMs,
            'cpu_iterations' => $cpuIterations,
            'dispatch_started_ns' => $startedAt,
            'dispatch_finished_ns' => $completedAt,
            'dispatch_duration_ns' => $completedAt - $startedAt,
        ];
        $metadata = $this->option('metadata');
        $metadataPath = $sink->writeDispatchMetadata(
            $runId,
            $manifest,
            is_string($metadata) && $metadata !== '' ? $metadata : null,
        );
        $manifest['metadata_path'] = $metadataPath;
        $this->line($this->json($manifest));

        return self::SUCCESS;
    }

    private function integerOption(string $name, int $minimum, int $maximum): int
    {
        $raw = $this->option($name);
        if (!is_string($raw) || preg_match('/^(0|[1-9][0-9]*)$/D', $raw) !== 1) {
            throw new InvalidArgumentException("--{$name} must be an integer in {$minimum}..{$maximum}.");
        }

        $value = filter_var($raw, FILTER_VALIDATE_INT);
        if ($value === false || $value < $minimum || $value > $maximum) {
            throw new InvalidArgumentException("--{$name} must be an integer in {$minimum}..{$maximum}.");
        }

        return $value;
    }

    private function stringOption(string $name, string $default): string
    {
        $value = $this->option($name);

        return is_string($value) && $value !== '' ? $value : $default;
    }

    private function identifier(string $value, string $label, bool $forbidComma = false): void
    {
        if (strlen($value) > 128
            || preg_match('/^[A-Za-z0-9._:-]+$/D', $value) !== 1
            || ($forbidComma && str_contains($value, ','))) {
            throw new InvalidArgumentException(
                "--{$label} must be 1..128 ASCII letters, digits, dot, underscore, colon or dash.",
            );
        }
    }

    /** @param array<string, int|string> $value */
    private function json(array $value): string
    {
        try {
            return json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
        } catch (JsonException $exception) {
            throw new InvalidArgumentException('Unable to encode dispatch manifest.', previous: $exception);
        }
    }
}
