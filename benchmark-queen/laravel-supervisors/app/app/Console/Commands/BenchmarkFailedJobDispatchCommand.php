<?php

namespace App\Console\Commands;

use App\Jobs\FailedJobProbe;
use App\Support\JsonlResultSink;
use Illuminate\Console\Command;
use Illuminate\Support\Str;
use InvalidArgumentException;
use JsonException;

final class BenchmarkFailedJobDispatchCommand extends Command
{
    protected $signature = 'bench:dispatch-failure
        {--run-id= : Stable run identifier; a UUID is generated when omitted}
        {--probe-id=probe-1 : Stable identifier for this fail-once job}
        {--connection= : Queue connection; defaults to BENCH_CONNECTION}
        {--queue= : Queue name; defaults to the first BENCH_QUEUES entry}';

    protected $description = 'Dispatch one fail-once job for failed-store and retry lifecycle tests';

    public function handle(JsonlResultSink $sink): int
    {
        if (config('benchmark.failed_driver') === 'null') {
            throw new InvalidArgumentException(
                'bench:dispatch-failure requires BENCH_FAILED_DRIVER=file.',
            );
        }

        $runId = $this->value('run-id', (string) Str::uuid());
        $probeId = $this->value('probe-id', 'probe-1');
        $connection = $this->value('connection', (string) config('benchmark.connection'));
        $queue = $this->value('queue', (string) config('benchmark.queue'));
        foreach ([
            'run-id' => $runId,
            'probe-id' => $probeId,
            'connection' => $connection,
            'queue' => $queue,
        ] as $label => $value) {
            $this->identifier($value, $label);
        }

        $sink->reserveRun($runId);
        $enqueuedAt = hrtime(true);
        FailedJobProbe::dispatch($runId, $probeId, $enqueuedAt)
            ->onConnection($connection)
            ->onQueue($queue);

        $manifest = [
            'run_id' => $runId,
            'probe_id' => $probeId,
            'jobs' => 1,
            'connection' => $connection,
            'queue' => $queue,
            'dispatch_mode' => 'failure-probe',
            'dispatch_started_ns' => $enqueuedAt,
            'dispatch_finished_ns' => hrtime(true),
        ];
        $sink->writeDispatchMetadata($runId, $manifest);
        try {
            $this->line(json_encode($manifest, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES));
        } catch (JsonException $exception) {
            throw new InvalidArgumentException('Unable to encode failure-probe manifest.', previous: $exception);
        }

        return self::SUCCESS;
    }

    private function value(string $name, string $default): string
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
}
