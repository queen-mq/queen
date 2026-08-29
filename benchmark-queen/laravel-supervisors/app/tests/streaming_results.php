<?php

declare(strict_types=1);

use App\Support\BenchmarkResultAccumulator;
use App\Support\JsonlResultSink;

require dirname(__DIR__).'/vendor/autoload.php';

final class StreamingResultsTest
{
    private int $assertions = 0;

    public function run(): void
    {
        $baseDirectory = sys_get_temp_dir().DIRECTORY_SEPARATOR
            .'queen-streaming-results-'.getmypid().'-'.bin2hex(random_bytes(6));
        if (!mkdir($baseDirectory, 0770)) {
            throw new RuntimeException("Unable to create test directory [{$baseDirectory}].");
        }

        try {
            $this->testIncrementalSnapshots($baseDirectory);
            $this->testExactCompactSummary();
            $this->testMalformedJsonIsRejected($baseDirectory);
            $this->testFiftyThousandRecordCommandWithinMemoryLimit($baseDirectory);
        } finally {
            $this->removeTree($baseDirectory);
        }

        fwrite(STDOUT, "Streaming result tests passed ({$this->assertions} assertions).\n");
    }

    private function testIncrementalSnapshots(string $baseDirectory): void
    {
        $sink = new JsonlResultSink($baseDirectory);
        $sink->reserveRun('snapshot-run');
        $sink->append($this->record('snapshot-run', 'first', 10, 20));
        $firstSnapshot = $sink->snapshot('snapshot-run');

        $sink->append($this->record('snapshot-run', 'second', 30, 50));
        $secondSnapshot = $sink->snapshot('snapshot-run');

        $first = iterator_to_array($sink->stream($firstSnapshot), false);
        $delta = iterator_to_array($sink->stream($secondSnapshot, $firstSnapshot), false);
        $all = $sink->read('snapshot-run');

        $this->same(['first'], array_column($first, 'job_id'), 'first snapshot is stable');
        $this->same(['second'], array_column($delta, 'job_id'), 'snapshot delta is incremental');
        $this->same(['first', 'second'], array_column($all, 'job_id'), 'read compatibility is preserved');

        $sink->reserveRun('other-run');
        $this->throws(
            static fn (): array => iterator_to_array(
                $sink->stream($secondSnapshot, $sink->snapshot('other-run')),
                false,
            ),
            RuntimeException::class,
            'snapshots from different runs are rejected',
        );
    }

    private function testExactCompactSummary(): void
    {
        $accumulator = new BenchmarkResultAccumulator('summary-run');
        $records = [
            $this->record('summary-run', 'a', 10, 100, 40, 90, 4, 1),
            $this->record('summary-run', 'a', 5, 90, 10, 80, 1, 2),
            $this->record('summary-run', 'a', 1, 110, 1, 109, 0, 3),
            ['run_id' => 'summary-run', 'job_id' => 'b', 'attempt' => 1],
            $this->record('summary-run', 'b', 20, 200, 30, 180, 3, 4),
            ['run_id' => 'summary-run', 'job_id' => '', 'attempt' => 9],
            $this->record('different-run', 'ignored', 0, 1, 0, 1, 0, 99),
        ];
        foreach ($records as $record) {
            $accumulator->add($record);
        }

        $this->same(2, $accumulator->uniqueCompleted(), 'unique count');
        $this->same([
            'run_id' => 'summary-run',
            'expected' => 2,
            'complete' => true,
            'unique_completed' => 2,
            'records' => 6,
            'duplicates' => 4,
            'max_attempt' => 9,
            'elapsed_ns' => 195,
            'completion_span_ns' => 110,
            'queue_latency_ns' => ['min' => 10, 'p50' => 10, 'p95' => 30, 'p99' => 30, 'max' => 30],
            'end_to_end_ns' => ['min' => 80, 'p50' => 80, 'p95' => 180, 'p99' => 180, 'max' => 180],
            'sink_lock_wait_ns' => ['min' => 1, 'p50' => 1, 'p95' => 3, 'p99' => 3, 'max' => 3],
        ], $accumulator->summarize(2), 'summary semantics and earliest completion selection');
        $this->throws(
            function () use ($accumulator): void {
                $accumulator->add($this->record('summary-run', 'late', 0, 1));
            },
            RuntimeException::class,
            'summary seals accumulator state',
        );
    }

    private function testMalformedJsonIsRejected(string $baseDirectory): void
    {
        $sink = new JsonlResultSink($baseDirectory);
        $runDirectory = $sink->reserveRun('invalid-json');
        $eventsDirectory = $runDirectory.DIRECTORY_SEPARATOR.'events';
        if (!mkdir($eventsDirectory, 0770)) {
            throw new RuntimeException("Unable to create test event directory [{$eventsDirectory}].");
        }
        file_put_contents($eventsDirectory.DIRECTORY_SEPARATOR.'worker-123.jsonl', "{invalid}\n");

        $this->throws(
            static fn (): array => iterator_to_array($sink->stream($sink->snapshot('invalid-json')), false),
            RuntimeException::class,
            'malformed JSON remains a hard failure',
        );
    }

    private function testFiftyThousandRecordCommandWithinMemoryLimit(string $baseDirectory): void
    {
        $runId = 'memory-50k';
        $eventsDirectory = $baseDirectory.DIRECTORY_SEPARATOR.$runId.DIRECTORY_SEPARATOR.'events';
        if (!mkdir($eventsDirectory, 0770, true)) {
            throw new RuntimeException("Unable to create stress fixture [{$eventsDirectory}].");
        }
        $path = $eventsDirectory.DIRECTORY_SEPARATOR.'worker-999.jsonl';
        $stream = fopen($path, 'wb');
        if ($stream === false) {
            throw new RuntimeException("Unable to create stress fixture [{$path}].");
        }

        try {
            for ($index = 0; $index < 50_000; ++$index) {
                $enqueuedAt = 1_000 + ($index * 10);
                $record = [
                    'run_id' => $runId,
                    'job_id' => sprintf('%09d', $index),
                    'connection' => 'queen',
                    'queue' => 'benchmark',
                    'enqueued_at_ns' => $enqueuedAt,
                    'work_started_at_ns' => $enqueuedAt + 3,
                    'completed_at_ns' => $enqueuedAt + 7,
                    'queue_latency_ns' => 3,
                    'end_to_end_ns' => 7,
                    'work_duration_ns' => 4,
                    'attempt' => 1,
                    'sleep_ms' => 0,
                    'cpu_iterations' => 0,
                    'checksum' => str_repeat('ab', 32),
                    'worker_pid' => 999,
                    'worker_host' => 'fixture',
                    'sink_lock_wait_ns' => $index % 11,
                    'recorded_at_ns' => $enqueuedAt + 8,
                ];
                $line = json_encode($record, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES)."\n";
                if (fwrite($stream, $line) !== strlen($line)) {
                    throw new RuntimeException("Unable to populate stress fixture [{$path}].");
                }
            }
        } finally {
            fclose($stream);
        }

        [$exitCode, $stdout, $stderr] = $this->artisan(
            $baseDirectory,
            ['bench:results', $runId, '--expected=50000', '--wait=0'],
            '64M',
        );
        $this->same(0, $exitCode, "bench:results failed under 64M: {$stderr}");
        $summary = $this->lastJsonLine($stdout);
        $this->same(50_000, $summary['unique_completed'] ?? null, '50k result unique count');
        $this->same(50_000, $summary['records'] ?? null, '50k result record count');
        $this->same(0, $summary['duplicates'] ?? null, '50k result duplicate count');
        $this->same(499_990, $summary['completion_span_ns'] ?? null, '50k completion span');
        $this->same(3, $summary['queue_latency_ns']['p99'] ?? null, '50k exact percentile');

        [$exitCode, $stdout, $stderr] = $this->artisan(
            $baseDirectory,
            ['bench:count', $runId],
            '64M',
        );
        $this->same(0, $exitCode, "bench:count failed under 64M: {$stderr}");
        $count = $this->lastJsonLine($stdout);
        $this->same(50_000, $count['completed'] ?? null, '50k streaming count');
        $this->same(50_000, $count['records'] ?? null, '50k streaming record count');
    }

    /**
     * @return array<string, int|string>
     */
    private function record(
        string $runId,
        string $jobId,
        int $enqueuedAt,
        int $completedAt,
        int $queueLatency = 1,
        int $endToEnd = 2,
        int $sinkWait = 0,
        int $attempt = 1,
    ): array {
        return [
            'run_id' => $runId,
            'job_id' => $jobId,
            'enqueued_at_ns' => $enqueuedAt,
            'completed_at_ns' => $completedAt,
            'queue_latency_ns' => $queueLatency,
            'end_to_end_ns' => $endToEnd,
            'sink_lock_wait_ns' => $sinkWait,
            'attempt' => $attempt,
        ];
    }

    /** @param list<string> $arguments @return array{int, string, string} */
    private function artisan(
        string $resultsDirectory,
        array $arguments,
        string $memoryLimit,
    ): array {
        $command = [PHP_BINARY, '-d', "memory_limit={$memoryLimit}", 'artisan', ...$arguments];
        $environment = getenv();
        $environment = is_array($environment) ? $environment : [];
        $environment['APP_ENV'] = 'testing';
        $environment['BENCH_RESULTS_DIRECTORY'] = $resultsDirectory;
        $process = proc_open(
            $command,
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
            dirname(__DIR__),
            $environment,
        );
        if (!is_resource($process)) {
            throw new RuntimeException('Unable to launch fixture Artisan command.');
        }

        $stdout = stream_get_contents($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);

        return [proc_close($process), (string) $stdout, (string) $stderr];
    }

    /** @return array<string, mixed> */
    private function lastJsonLine(string $output): array
    {
        $lines = preg_split('/\R/', trim($output));
        $line = is_array($lines) ? end($lines) : false;
        $decoded = is_string($line) ? json_decode($line, true) : null;
        if (!is_array($decoded)) {
            throw new RuntimeException("Command did not emit a JSON object: {$output}");
        }

        return $decoded;
    }

    private function same(mixed $expected, mixed $actual, string $message): void
    {
        ++$this->assertions;
        if ($actual !== $expected) {
            throw new RuntimeException(
                $message.'; expected '.var_export($expected, true).', got '.var_export($actual, true),
            );
        }
    }

    /** @param callable(): mixed $callback */
    private function throws(callable $callback, string $class, string $message): void
    {
        ++$this->assertions;
        try {
            $callback();
        } catch (Throwable $exception) {
            if ($exception instanceof $class) {
                return;
            }
            throw new RuntimeException(
                "{$message}; expected {$class}, got ".get_debug_type($exception),
                previous: $exception,
            );
        }

        throw new RuntimeException("{$message}; expected {$class}, no exception was thrown");
    }

    private function removeTree(string $directory): void
    {
        if (!is_dir($directory)) {
            return;
        }
        $entries = scandir($directory);
        if ($entries === false) {
            throw new RuntimeException("Unable to inspect test directory [{$directory}].");
        }
        foreach ($entries as $entry) {
            if ($entry === '.' || $entry === '..') {
                continue;
            }
            $path = $directory.DIRECTORY_SEPARATOR.$entry;
            if (is_dir($path) && !is_link($path)) {
                $this->removeTree($path);
            } elseif (!unlink($path)) {
                throw new RuntimeException("Unable to remove test file [{$path}].");
            }
        }
        if (!rmdir($directory)) {
            throw new RuntimeException("Unable to remove test directory [{$directory}].");
        }
    }
}

(new StreamingResultsTest())->run();
