<?php

namespace App\Console\Commands;

use App\Support\JsonlResultSink;
use Illuminate\Console\Command;
use InvalidArgumentException;
use JsonException;

final class BenchmarkResultsCommand extends Command
{
    protected $signature = 'bench:results
        {run-id : Run identifier emitted by bench:dispatch}
        {--expected=0 : Unique completions required for success}
        {--wait=0 : Maximum seconds to wait}
        {--poll-ms=100 : Poll interval while waiting}';

    protected $description = 'Wait for and summarize JSONL benchmark completions';

    public function handle(JsonlResultSink $sink): int
    {
        $runId = (string) $this->argument('run-id');
        if (preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $runId) !== 1) {
            throw new InvalidArgumentException('run-id has an invalid format.');
        }

        $expected = $this->integerOption('expected', 0, 1_000_000);
        $waitSeconds = $this->integerOption('wait', 0, 86_400);
        $pollMs = $this->integerOption('poll-ms', 10, 60_000);
        $deadline = hrtime(true) + ($waitSeconds * 1_000_000_000);

        do {
            $records = array_values(array_filter(
                $sink->read($runId),
                static fn (array $record): bool => ($record['run_id'] ?? null) === $runId,
            ));
            $unique = [];
            foreach ($records as $record) {
                $jobId = $record['job_id'] ?? null;
                if (is_string($jobId) && $jobId !== '') {
                    $previous = $unique[$jobId] ?? null;
                    $completed = $record['completed_at_ns'] ?? null;
                    $previousCompleted = is_array($previous) ? ($previous['completed_at_ns'] ?? null) : null;
                    if (!is_array($previous)
                        || (is_int($completed) && (!is_int($previousCompleted) || $completed < $previousCompleted))) {
                        $unique[$jobId] = $record;
                    }
                }
            }

            if ($expected === 0 || count($unique) >= $expected || hrtime(true) >= $deadline) {
                break;
            }
            usleep($pollMs * 1000);
        } while (true);

        $summary = $this->summarize($runId, $records, array_values($unique), $expected);
        $this->line($this->json($summary));

        return $expected === 0 || count($unique) >= $expected ? self::SUCCESS : self::FAILURE;
    }

    /**
     * @param list<array<string, mixed>> $records
     * @param list<array<string, mixed>> $unique
     * @return array<string, mixed>
     */
    private function summarize(string $runId, array $records, array $unique, int $expected): array
    {
        $queueLatencies = [];
        $endToEndLatencies = [];
        $sinkWaits = [];
        $attempts = [];
        $enqueued = [];
        $completed = [];

        foreach ($unique as $record) {
            $this->collectInteger($record, 'queue_latency_ns', $queueLatencies);
            $this->collectInteger($record, 'end_to_end_ns', $endToEndLatencies);
            $this->collectInteger($record, 'sink_lock_wait_ns', $sinkWaits);
            $this->collectInteger($record, 'enqueued_at_ns', $enqueued);
            $this->collectInteger($record, 'completed_at_ns', $completed);
        }
        foreach ($records as $record) {
            $this->collectInteger($record, 'attempt', $attempts);
        }

        return [
            'run_id' => $runId,
            'expected' => $expected,
            'complete' => $expected === 0 || count($unique) >= $expected,
            'unique_completed' => count($unique),
            'records' => count($records),
            'duplicates' => max(0, count($records) - count($unique)),
            'max_attempt' => $attempts === [] ? null : max($attempts),
            'elapsed_ns' => $enqueued === [] || $completed === [] ? null : max($completed) - min($enqueued),
            'completion_span_ns' => $completed === [] ? null : max($completed) - min($completed),
            'queue_latency_ns' => $this->distribution($queueLatencies),
            'end_to_end_ns' => $this->distribution($endToEndLatencies),
            'sink_lock_wait_ns' => $this->distribution($sinkWaits),
        ];
    }

    /**
     * @param array<string, mixed> $record
     * @param list<int> $values
     */
    private function collectInteger(array $record, string $key, array &$values): void
    {
        $value = $record[$key] ?? null;
        if (is_int($value) && $value >= 0) {
            $values[] = $value;
        }
    }

    /** @param list<int> $values @return array<string, int|null> */
    private function distribution(array $values): array
    {
        if ($values === []) {
            return ['min' => null, 'p50' => null, 'p95' => null, 'p99' => null, 'max' => null];
        }

        sort($values, SORT_NUMERIC);

        return [
            'min' => $values[0],
            'p50' => $this->nearestRank($values, 0.50),
            'p95' => $this->nearestRank($values, 0.95),
            'p99' => $this->nearestRank($values, 0.99),
            'max' => $values[array_key_last($values)],
        ];
    }

    /** @param list<int> $values */
    private function nearestRank(array $values, float $percentile): int
    {
        $index = max(0, (int) ceil(count($values) * $percentile) - 1);

        return $values[$index];
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

    /** @param array<string, mixed> $value */
    private function json(array $value): string
    {
        try {
            return json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
        } catch (JsonException $exception) {
            throw new InvalidArgumentException('Unable to encode result summary.', previous: $exception);
        }
    }
}
