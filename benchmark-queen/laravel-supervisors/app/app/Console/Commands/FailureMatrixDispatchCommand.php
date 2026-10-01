<?php

namespace App\Console\Commands;

use App\Jobs\FailureMatrixJob;
use Illuminate\Console\Command;
use InvalidArgumentException;

final class FailureMatrixDispatchCommand extends Command
{
    protected $signature = 'bench:matrix-dispatch
        {--run-id= : Run identifier}
        {--mode=ok : ok, throw, throw-once, release-once, fail or memory}
        {--jobs=1 : Number of jobs}
        {--first=0 : Number of the first job, so several dispatches can share a run}
        {--sleep-ms=0 : Work of each successful attempt}
        {--tries=1 : The job\'s $tries}
        {--backoff=0 : The job\'s $backoff, seconds}
        {--timeout=60 : The job\'s $timeout, seconds}
        {--allocate-mib=0 : Memory the memory mode keeps}
        {--connection= : Queue connection; defaults to BENCH_CONNECTION}
        {--queue= : Queue name; defaults to BENCH_QUEUE}';

    protected $description = 'Dispatch failure-matrix jobs, one push each';

    public function handle(): int
    {
        $runId = (string) $this->option('run-id');
        if (preg_match('/^[A-Za-z0-9._:-]{1,128}$/D', $runId) !== 1) {
            throw new InvalidArgumentException('--run-id is required: 1..128 letters, digits, dot, underscore, colon or dash.');
        }
        $mode = (string) $this->option('mode');
        $connection = (string) ($this->option('connection') ?: config('benchmark.connection'));
        $queue = (string) ($this->option('queue') ?: config('benchmark.queue'));
        $first = $this->integer('first', 0, 1_000_000);
        $jobs = $this->integer('jobs', 1, 100_000);
        for ($index = $first; $index < $first + $jobs; ++$index) {
            FailureMatrixJob::dispatch(
                runId: $runId,
                jobId: sprintf('%06d', $index),
                mode: $mode,
                sleepMs: $this->integer('sleep-ms', 0, 3_600_000),
                tries: $this->integer('tries', 1, 100),
                backoff: $this->integer('backoff', 0, 3_600),
                timeout: $this->integer('timeout', 1, 86_400),
                allocateMib: $this->integer('allocate-mib', 0, 4_096),
            )->onConnection($connection)->onQueue($queue);
        }
        $this->line(json_encode([
            'run_id' => $runId,
            'mode' => $mode,
            'first' => $first,
            'jobs' => $jobs,
            'connection' => $connection,
            'queue' => $queue,
        ], JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES));

        return self::SUCCESS;
    }

    private function integer(string $name, int $minimum, int $maximum): int
    {
        $value = filter_var($this->option($name), FILTER_VALIDATE_INT);
        if ($value === false || $value < $minimum || $value > $maximum) {
            throw new InvalidArgumentException("--{$name} must be an integer in {$minimum}..{$maximum}.");
        }

        return $value;
    }
}
