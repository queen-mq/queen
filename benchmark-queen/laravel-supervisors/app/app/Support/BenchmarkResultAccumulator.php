<?php

namespace App\Support;

use RuntimeException;

/**
 * Incremental, compact state for an exact benchmark result summary.
 *
 * Full decoded records are never retained. Exact duplicate detection and
 * percentiles still require O(unique jobs) scalar state, but the nested record
 * arrays that dominate PHP memory usage are discarded immediately.
 */
final class BenchmarkResultAccumulator
{
    private int $records = 0;

    private ?int $maxAttempt = null;

    /** @var array<string|int, int> */
    private array $jobIndexes = [];

    /** @var list<int|null> */
    private array $completedAt = [];

    /** @var array<int, int> */
    private array $enqueuedAt = [];

    /** @var array<int, int> */
    private array $queueLatencies = [];

    /** @var array<int, int> */
    private array $endToEndLatencies = [];

    /** @var array<int, int> */
    private array $sinkWaits = [];

    /** @var array<string, mixed>|null */
    private ?array $statistics = null;

    public function __construct(private readonly string $runId)
    {
        if (preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $runId) !== 1) {
            throw new RuntimeException('Benchmark accumulator run_id has an invalid format.');
        }
    }

    /** @param array<string, mixed> $record */
    public function add(array $record): void
    {
        if ($this->statistics !== null) {
            throw new RuntimeException('Cannot add benchmark results after producing a summary.');
        }
        if (($record['run_id'] ?? null) !== $this->runId) {
            return;
        }

        ++$this->records;
        $attempt = $this->nonNegativeInteger($record, 'attempt');
        if ($attempt !== null && ($this->maxAttempt === null || $attempt > $this->maxAttempt)) {
            $this->maxAttempt = $attempt;
        }

        $jobId = $record['job_id'] ?? null;
        if (!is_string($jobId) || $jobId === '') {
            return;
        }

        $candidateCompletedAt = $record['completed_at_ns'] ?? null;
        $candidateCompletedAt = is_int($candidateCompletedAt) ? $candidateCompletedAt : null;
        if (isset($this->jobIndexes[$jobId])) {
            $index = $this->jobIndexes[$jobId];
            $previousCompletedAt = $this->completedAt[$index];
            if ($candidateCompletedAt === null
                || ($previousCompletedAt !== null && $candidateCompletedAt >= $previousCompletedAt)) {
                return;
            }
        } else {
            $index = count($this->jobIndexes);
            $this->jobIndexes[$jobId] = $index;
        }

        $this->completedAt[$index] = $candidateCompletedAt;
        $this->replaceMetric($this->enqueuedAt, $index, $record, 'enqueued_at_ns');
        $this->replaceMetric($this->queueLatencies, $index, $record, 'queue_latency_ns');
        $this->replaceMetric($this->endToEndLatencies, $index, $record, 'end_to_end_ns');
        $this->replaceMetric($this->sinkWaits, $index, $record, 'sink_lock_wait_ns');
    }

    public function uniqueCompleted(): int
    {
        return count($this->jobIndexes);
    }

    /** @return array<string, mixed> */
    public function summarize(int $expected): array
    {
        $statistics = $this->statistics ??= $this->buildStatistics();
        $uniqueCompleted = $this->uniqueCompleted();

        return [
            'run_id' => $this->runId,
            'expected' => $expected,
            'complete' => $expected === 0 || $uniqueCompleted >= $expected,
            'unique_completed' => $uniqueCompleted,
            'records' => $this->records,
            'duplicates' => max(0, $this->records - $uniqueCompleted),
            'max_attempt' => $this->maxAttempt,
            ...$statistics,
        ];
    }

    /** @return array<string, mixed> */
    private function buildStatistics(): array
    {
        $enqueuedRange = $this->range($this->enqueuedAt);
        $completedRange = $this->range($this->completedAt);

        return [
            'elapsed_ns' => $enqueuedRange === null || $completedRange === null
                ? null
                : $completedRange['max'] - $enqueuedRange['min'],
            'completion_span_ns' => $completedRange === null
                ? null
                : $completedRange['max'] - $completedRange['min'],
            'queue_latency_ns' => $this->distribution($this->queueLatencies),
            'end_to_end_ns' => $this->distribution($this->endToEndLatencies),
            'sink_lock_wait_ns' => $this->distribution($this->sinkWaits),
        ];
    }

    /**
     * @param array<int, int> $values
     * @param array<string, mixed> $record
     */
    private function replaceMetric(array &$values, int $index, array $record, string $key): void
    {
        unset($values[$index]);
        $value = $this->nonNegativeInteger($record, $key);
        if ($value !== null) {
            $values[$index] = $value;
        }
    }

    /** @param array<string, mixed> $record */
    private function nonNegativeInteger(array $record, string $key): ?int
    {
        $value = $record[$key] ?? null;

        return is_int($value) && $value >= 0 ? $value : null;
    }

    /**
     * @param array<int, int|null> $values
     * @return array{min: int, max: int}|null
     */
    private function range(array $values): ?array
    {
        $minimum = null;
        $maximum = null;
        foreach ($values as $value) {
            if (!is_int($value) || $value < 0) {
                continue;
            }
            $minimum = $minimum === null ? $value : min($minimum, $value);
            $maximum = $maximum === null ? $value : max($maximum, $value);
        }

        return $minimum === null || $maximum === null
            ? null
            : ['min' => $minimum, 'max' => $maximum];
    }

    /**
     * Sorting is deliberately in-place: summarize() seals the accumulator, so
     * retaining a second copy of every latency solely for percentiles would be
     * wasted peak memory.
     *
     * @param array<int, int> $values
     * @return array<string, int|null>
     */
    private function distribution(array &$values): array
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
}
