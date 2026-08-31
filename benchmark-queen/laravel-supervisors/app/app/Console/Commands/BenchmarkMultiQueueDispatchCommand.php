<?php

namespace App\Console\Commands;

use App\Jobs\BenchmarkJob;
use App\Support\BenchmarkEffectLedger;
use App\Support\JsonlResultSink;
use Illuminate\Console\Command;
use Illuminate\Support\Str;
use InvalidArgumentException;
use JsonException;

final class BenchmarkMultiQueueDispatchCommand extends Command
{
    protected $signature = 'bench:dispatch-multi
        {--run-id= : Stable run identifier; a UUID is generated when omitted}
        {--jobs-per-queue= : Equal number of jobs enqueued on every queue; defaults to 100}
        {--queue-counts= : Ordered per-queue job counts, mutually exclusive with --jobs-per-queue}
        {--queues= : Strict comma-separated queue list; defaults to BENCH_QUEUES}
        {--sleep-ms=10 : Sleep performed by every job}
        {--cpu-iterations=0 : SHA-256 rounds performed by every job}
        {--connection= : Queue connection; defaults to BENCH_CONNECTION}
        {--metadata= : Dispatch manifest path; defaults to <results>/<run-id>/dispatch.json}';

    protected $description = 'Dispatch a deterministic, optionally weighted round-robin burst across multiple queues';

    public function handle(JsonlResultSink $sink, BenchmarkEffectLedger $ledger): int
    {
        $runId = $this->option('run-id');
        $runId = is_string($runId) && $runId !== '' ? $runId : (string) Str::uuid();
        $this->identifier($runId, 'run-id');

        $sleepMs = $this->integerOption('sleep-ms', 0, 60_000);
        $cpuIterations = $this->integerOption('cpu-iterations', 0, 10_000_000);
        $connection = $this->stringOption('connection', (string) config('benchmark.connection'));
        $this->identifier($connection, 'connection');

        $configured = config('benchmark.queues', []);
        if (!is_array($configured)) {
            throw new InvalidArgumentException('benchmark.queues must be an array.');
        }
        $configuredQueues = $this->queueList(implode(',', $configured));
        $option = $this->option('queues');
        $queues = is_string($option) && $option !== ''
            ? $this->queueList($option)
            : $configuredQueues;
        if (count($queues) < 2) {
            throw new InvalidArgumentException('bench:dispatch-multi requires at least two queues.');
        }
        if ($queues !== $configuredQueues) {
            throw new InvalidArgumentException(
                '--queues must exactly match BENCH_QUEUES so the probe cannot publish unsupervised work.',
            );
        }

        $jobsOption = $this->option('jobs-per-queue');
        $countsOption = $this->option('queue-counts');
        if (is_string($jobsOption) && $jobsOption !== ''
            && is_string($countsOption) && $countsOption !== '') {
            throw new InvalidArgumentException(
                '--jobs-per-queue and --queue-counts are mutually exclusive.',
            );
        }
        if (is_string($countsOption) && $countsOption !== '') {
            $queueCounts = $this->queueCounts($countsOption, count($queues));
        } else {
            $jobsPerQueue = $this->integerOption('jobs-per-queue', 1, 1_000_000, 100);
            $queueCounts = array_fill(0, count($queues), $jobsPerQueue);
        }
        $totalJobs = array_sum($queueCounts);
        if ($totalJobs > 1_000_000) {
            throw new InvalidArgumentException('The multi-queue dispatch may not exceed 1,000,000 total jobs.');
        }
        $jobsByQueue = array_combine($queues, $queueCounts);
        if ($jobsByQueue === false) {
            throw new InvalidArgumentException('Unable to bind queue counts to queues.');
        }

        $sink->reserveRun($runId);
        $ledger->reserveRun($runId);
        $startedAt = hrtime(true);
        $maximumQueueCount = max($queueCounts);
        for ($index = 0; $index < $maximumQueueCount; ++$index) {
            foreach ($jobsByQueue as $queue => $queueCount) {
                if ($index >= $queueCount) {
                    continue;
                }
                BenchmarkJob::dispatch(
                    runId: $runId,
                    jobId: $queue.':'.sprintf('%09d', $index),
                    enqueuedAtNs: hrtime(true),
                    sleepMs: $sleepMs,
                    cpuIterations: $cpuIterations,
                )->onConnection($connection)->onQueue($queue);
            }
        }
        $completedAt = hrtime(true);

        $manifest = [
            'run_id' => $runId,
            'jobs' => $totalJobs,
            'jobs_per_queue' => count(array_unique($queueCounts)) === 1 ? $queueCounts[0] : null,
            'jobs_by_queue' => $jobsByQueue,
            'connection' => $connection,
            'queue' => $queues[0],
            'queues_csv' => implode(',', $queues),
            'dispatch_mode' => 'weighted-round-robin-single',
            'dispatch_batch_size' => 1,
            'sleep_ms' => $sleepMs,
            'cpu_iterations' => $cpuIterations,
            'ledger_mode' => $ledger->mode(),
            'ledger_semantics' => $ledger->enabled()
                ? 'fixture-local idempotent effect keyed by run_id+job_id; not queue-ACK atomic'
                : 'disabled',
            'dispatch_started_ns' => $startedAt,
            'dispatch_finished_ns' => $completedAt,
            'dispatch_duration_ns' => $completedAt - $startedAt,
        ];
        $metadata = $this->option('metadata');
        $manifest['metadata_path'] = $sink->writeDispatchMetadata(
            $runId,
            $manifest,
            is_string($metadata) && $metadata !== '' ? $metadata : null,
        );
        $this->line($this->json($manifest));

        return self::SUCCESS;
    }

    /** @return list<string> */
    private function queueList(string $value): array
    {
        if ($value === '') {
            throw new InvalidArgumentException('--queues must not be empty.');
        }
        $queues = explode(',', $value);
        if (count($queues) > 256) {
            throw new InvalidArgumentException('--queues may not contain more than 256 entries.');
        }

        $seen = [];
        foreach ($queues as $queue) {
            if ($queue === '' || $queue !== trim($queue)) {
                throw new InvalidArgumentException('--queues must not contain empty entries or surrounding whitespace.');
            }
            $this->identifier($queue, 'queues');
            // Multi-queue job IDs append ':' plus a zero-padded nine-digit
            // index. Keep the resulting ledger/result identifier within the
            // shared 128-byte contract before publishing any work.
            if (strlen($queue) > 118) {
                throw new InvalidArgumentException(
                    '--queues entries may not exceed 118 bytes for multi-queue job IDs.',
                );
            }
            if (isset($seen['queue:'.$queue])) {
                throw new InvalidArgumentException("--queues contains duplicate queue [{$queue}].");
            }
            $seen['queue:'.$queue] = true;
        }

        return array_values($queues);
    }

    /** @return list<int> */
    private function queueCounts(string $value, int $expectedCount): array
    {
        $parts = explode(',', $value);
        if (count($parts) !== $expectedCount) {
            throw new InvalidArgumentException(
                '--queue-counts must contain exactly one positive integer for every queue.',
            );
        }

        $counts = [];
        foreach ($parts as $part) {
            if (preg_match('/^[1-9][0-9]*$/D', $part) !== 1) {
                throw new InvalidArgumentException(
                    '--queue-counts must contain positive integers without whitespace.',
                );
            }
            $count = filter_var($part, FILTER_VALIDATE_INT);
            if ($count === false || $count > 1_000_000) {
                throw new InvalidArgumentException(
                    '--queue-counts entries must be in 1..1,000,000.',
                );
            }
            $counts[] = (int) $count;
        }

        return $counts;
    }

    private function integerOption(
        string $name,
        int $minimum,
        int $maximum,
        ?int $default = null,
    ): int
    {
        $raw = $this->option($name);
        if (($raw === null || $raw === '') && $default !== null) {
            $raw = (string) $default;
        }
        if (!is_string($raw) || preg_match('/^(0|[1-9][0-9]*)$/D', $raw) !== 1) {
            throw new InvalidArgumentException("--{$name} must be an integer in {$minimum}..{$maximum}.");
        }
        $value = filter_var($raw, FILTER_VALIDATE_INT);
        if ($value === false || $value < $minimum || $value > $maximum) {
            throw new InvalidArgumentException("--{$name} must be an integer in {$minimum}..{$maximum}.");
        }

        return (int) $value;
    }

    private function stringOption(string $name, string $default): string
    {
        $value = $this->option($name);

        return is_string($value) && $value !== '' ? $value : $default;
    }

    private function identifier(string $value, string $label): void
    {
        if (preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $value) !== 1) {
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
            throw new InvalidArgumentException('Unable to encode multi-queue manifest.', previous: $exception);
        }
    }
}
