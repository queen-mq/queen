<?php

namespace App\Console\Commands;

use App\Support\JsonlResultSink;
use Illuminate\Console\Command;
use InvalidArgumentException;
use JsonException;
use RuntimeException;

final class BenchmarkCountCommand extends Command
{
    protected $signature = 'bench:count {run-id : Run identifier emitted by bench:dispatch}';

    protected $description = 'Print completion, record and duplicate counts as one JSON object';

    public function handle(JsonlResultSink $sink): int
    {
        $runId = (string) $this->argument('run-id');
        if (preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $runId) !== 1) {
            throw new InvalidArgumentException('run-id has an invalid format.');
        }

        $records = array_values(array_filter(
            $sink->read($runId),
            static fn (array $record): bool => ($record['run_id'] ?? null) === $runId,
        ));
        $jobIds = [];
        foreach ($records as $record) {
            $jobId = $record['job_id'] ?? null;
            if (is_string($jobId) && $jobId !== '') {
                $jobIds[$jobId] = true;
            }
        }

        $recordsCount = count($records);
        $completed = count($jobIds);
        try {
            $this->line(json_encode([
                'run_id' => $runId,
                'completed' => $completed,
                'records' => $recordsCount,
                'duplicates' => max(0, $recordsCount - $completed),
            ], JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES));
        } catch (JsonException $exception) {
            throw new RuntimeException('Unable to encode benchmark count.', previous: $exception);
        }

        return self::SUCCESS;
    }
}
