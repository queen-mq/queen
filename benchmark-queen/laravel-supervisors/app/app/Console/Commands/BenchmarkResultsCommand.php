<?php

namespace App\Console\Commands;

use App\Support\BenchmarkResultAccumulator;
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
        $accumulator = new BenchmarkResultAccumulator($runId);
        $previousSnapshot = null;

        do {
            $snapshot = $sink->snapshot($runId);
            foreach ($sink->stream($snapshot, $previousSnapshot) as $record) {
                $accumulator->add($record);
            }
            $previousSnapshot = $snapshot;

            if ($expected === 0
                || $accumulator->uniqueCompleted() >= $expected
                || hrtime(true) >= $deadline) {
                break;
            }
            usleep($pollMs * 1000);
        } while (true);

        $summary = $accumulator->summarize($expected);
        $this->line($this->json($summary));

        return $summary['complete'] === true ? self::SUCCESS : self::FAILURE;
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
