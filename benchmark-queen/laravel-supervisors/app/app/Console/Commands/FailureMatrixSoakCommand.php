<?php

namespace App\Console\Commands;

use App\Jobs\FailureMatrixJob;
use Illuminate\Console\Command;
use InvalidArgumentException;

/**
 * A mixed workload for the soak lanes, dispatched at a steady rate for a
 * fixed time: mostly short jobs, some long ones, some that fail once and
 * succeed on retry, some that always fail, some delayed. The job id encodes
 * the kind (`ok-`, `long-`, `flaky-`, `bad-`, `late-`), so the verifier
 * knows what each job should end as.
 */
final class FailureMatrixSoakCommand extends Command
{
    protected $signature = 'bench:matrix-soak
        {--run-id= : Run identifier}
        {--seconds=600 : How long to dispatch}
        {--rate=10 : Jobs per second}
        {--seed=1 : Seed of the mix, so a lane can be repeated}';

    protected $description = 'Dispatch the soak mix at a steady rate';

    public function handle(): int
    {
        $runId = (string) $this->option('run-id');
        if (preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $runId) !== 1) {
            throw new InvalidArgumentException('--run-id is required.');
        }
        $seconds = max(1, (int) $this->option('seconds'));
        $rate = max(1, (int) $this->option('rate'));
        mt_srand((int) $this->option('seed'));
        $connection = (string) config('benchmark.connection');
        $queue = (string) config('benchmark.queue');

        $counts = ['ok' => 0, 'long' => 0, 'flaky' => 0, 'bad' => 0, 'late' => 0];
        $started = hrtime(true);
        $index = 0;
        while (($elapsed = (hrtime(true) - $started) / 1e9) < $seconds) {
            // Open loop: a late dispatch never shifts the ones after it.
            $due = $index / $rate;
            if ($due > $elapsed) {
                usleep((int) (($due - $elapsed) * 1e6));
            }
            $roll = mt_rand(1, 100);
            [$kind, $mode, $sleepMs, $tries, $delay] = match (true) {
                $roll <= 85 => ['ok', 'ok', mt_rand(10, 300), 3, 0],
                $roll <= 90 => ['long', 'ok', mt_rand(5_000, 20_000), 3, 0],
                $roll <= 95 => ['flaky', 'throw-once', mt_rand(10, 100), 3, 0],
                $roll <= 98 => ['bad', 'throw', 10, 2, 0],
                default => ['late', 'ok', mt_rand(10, 100), 3, mt_rand(10, 60)],
            };
            $job = FailureMatrixJob::dispatch(
                runId: $runId,
                jobId: sprintf('%s-%07d', $kind, $index),
                mode: $mode,
                sleepMs: $sleepMs,
                tries: $tries,
                backoff: 1,
                timeout: 60,
            )->onConnection($connection)->onQueue($queue);
            if ($delay > 0) {
                $job->delay($delay);
            }
            unset($job);
            ++$counts[$kind];
            ++$index;
        }
        $this->line(json_encode(['run_id' => $runId, 'dispatched' => $index, 'kinds' => $counts], JSON_THROW_ON_ERROR));

        return self::SUCCESS;
    }
}
