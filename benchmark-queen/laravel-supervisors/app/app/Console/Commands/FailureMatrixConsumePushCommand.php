<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use InvalidArgumentException;
use Queen\Queen;
use RuntimeException;

/**
 * Push the plain messages the pool of handlers reads (BENCH_CONSUMERS), as
 * another service would: no Laravel job, one partition per message. The job
 * IDs follow bench:matrix-dispatch's, so the matrix's checks read both alike.
 */
final class FailureMatrixConsumePushCommand extends Command
{
    protected $signature = 'bench:matrix-consume-push
        {--run-id= : Run identifier}
        {--jobs=1 : Messages to push}
        {--first=0 : The first job number}
        {--sleep-ms=0 : How long the handler works on each}';

    protected $description = 'Push plain messages for the pool of queen:consume handlers';

    public function handle(Queen $queen): int
    {
        $runId = (string) $this->option('run-id');
        if (preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $runId) !== 1) {
            throw new InvalidArgumentException('--run-id is required: 1..128 letters, digits, dot, underscore, colon or dash.');
        }
        $queue = (string) config('benchmark.consumers');
        if ($queue === '') {
            throw new InvalidArgumentException('BENCH_CONSUMERS names no queue: the lane has no pool of handlers.');
        }
        $first = max(0, (int) $this->option('first'));
        for ($job = $first; $job < $first + max(1, (int) $this->option('jobs')); $job++) {
            $jobId = sprintf('%06d', $job);
            $result = $queen->queue($queue)->partition("consume-{$jobId}")->push([['data' => [
                'run_id' => $runId,
                'job_id' => $jobId,
                'sleep_ms' => max(0, (int) $this->option('sleep-ms')),
            ]]])->execute();
            if (!is_array($result) || !in_array($result[0]['status'] ?? null, ['queued', 'buffered'], true)) {
                throw new RuntimeException('The broker refused the push: ' . json_encode($result));
            }
        }

        return self::SUCCESS;
    }
}
