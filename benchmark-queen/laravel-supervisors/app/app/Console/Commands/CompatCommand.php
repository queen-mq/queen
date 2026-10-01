<?php

namespace App\Console\Commands;

use App\Jobs\Compat\CompatEncryptedJob;
use App\Jobs\Compat\CompatJob;
use App\Jobs\Compat\CompatOverlapJob;
use App\Jobs\Compat\CompatPolicyJob;
use App\Jobs\Compat\CompatRateLimitedJob;
use App\Jobs\Compat\CompatUniqueJob;
use App\Support\FailureMatrixLog;
use Illuminate\Bus\Batch;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Bus;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Facades\Schema;
use InvalidArgumentException;
use RuntimeException;
use Throwable;

/**
 * One Laravel queue feature, end to end, against the running supervisor of
 * this container: dispatch, wait for the workers, check what Laravel
 * documents. Prints one JSON object: the scenario, its checks, and what was
 * observed. Run `bench:compat setup` once per lane first.
 */
final class CompatCommand extends Command
{
    public const SCENARIOS = [
        'delay', 'chain', 'chain-failure', 'batch', 'batch-failure', 'unique', 'without-overlapping',
        'rate-limited', 'backoff-array', 'retry-until', 'max-exceptions', 'fail-on-timeout', 'encrypted',
        'after-commit', 'events', 'failed-commands', 'queue-size',
    ];

    protected $signature = 'bench:compat {scenario : setup or one of the scenarios}
        {--run-id= : Run identifier}
        {--timeout=90 : Seconds to wait for the workers}';

    protected $description = 'Check one Laravel queue feature against the running workers';

    private FailureMatrixLog $log;

    private string $run;

    private string $connection;

    private string $queue;

    /** @var list<array{name: string, passed: bool, detail: string}> */
    private array $checks = [];

    /** @var array<string, mixed> */
    private array $observed = [];

    public function handle(FailureMatrixLog $log): int
    {
        $scenario = (string) $this->argument('scenario');
        if ($scenario === 'setup') {
            return $this->setup();
        }
        if (!in_array($scenario, self::SCENARIOS, true)) {
            throw new InvalidArgumentException("Unknown scenario [{$scenario}].");
        }
        $this->log = $log;
        $this->run = (string) ($this->option('run-id') ?: $scenario . '-' . bin2hex(random_bytes(4)));
        $this->connection = (string) config('benchmark.connection');
        $this->queue = (string) config('benchmark.queue');

        try {
            $this->{lcfirst(str_replace(' ', '', ucwords(str_replace('-', ' ', $scenario))))}();
        } catch (Throwable $error) {
            $this->check('ran without an exception', false, $error::class . ': ' . $error->getMessage());
        }
        if (in_array(false, array_column($this->checks, 'passed'), true)) {
            // The whole trail, to see why without reproducing it.
            $this->observed['events'] = array_map(static fn (array $e): array => [
                $e['job_id'], $e['event'], $e['attempt'], round((float) $e['at'], 2), $e['exception'], $e['pid'],
            ], $this->log->read($this->run));
        }
        $this->line(json_encode([
            'scenario' => $scenario,
            'run_id' => $this->run,
            'connection' => $this->connection,
            'passed' => $this->checks !== [] && !in_array(false, array_column($this->checks, 'passed'), true),
            'checks' => $this->checks,
            'observed' => $this->observed,
        ], JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_PARTIAL_OUTPUT_ON_ERROR));

        return self::SUCCESS;
    }

    private function setup(): int
    {
        // The database cache: its locks are atomic across processes, which
        // unique jobs, WithoutOverlapping and the rate limiter rely on.
        if (!Schema::hasTable('cache')) {
            Schema::create('cache', static function ($table): void {
                $table->string('key')->primary();
                $table->mediumText('value');
                $table->integer('expiration');
            });
            Schema::create('cache_locks', static function ($table): void {
                $table->string('key')->primary();
                $table->string('owner');
                $table->integer('expiration');
            });
        }
        if (!Schema::hasTable('job_batches')) {
            Schema::create('job_batches', static function ($table): void {
                $table->string('id')->primary();
                $table->string('name');
                $table->integer('total_jobs');
                $table->integer('pending_jobs');
                $table->integer('failed_jobs');
                $table->longText('failed_job_ids');
                $table->mediumText('options')->nullable();
                $table->integer('cancelled_at')->nullable();
                $table->integer('created_at');
                $table->integer('finished_at')->nullable();
            });
        }
        $this->line(json_encode(['setup' => 'ok', 'database' => config('database.connections.sqlite.database')]));

        return self::SUCCESS;
    }

    // ------------------------------------------------------------ scenarios

    private function delay(): void
    {
        $this->mark('dispatched');
        CompatJob::dispatch($this->run, 'd1')->onConnection($this->connection)->onQueue($this->queue)->delay(5);
        $this->waitFor(fn (): bool => $this->count('d1', 'completed') > 0);
        $waited = $this->first('d1', 'started') - $this->first('command', 'dispatched');
        $this->observed['waited_seconds'] = round($waited, 2);
        $this->check('a 5 s delay is respected', $waited >= 4.5 && $waited <= 30, round($waited, 2) . ' s');
    }

    private function chain(): void
    {
        Bus::chain([
            new CompatJob($this->run, 'c1', 'ok', 300),
            new CompatJob($this->run, 'c2', 'ok', 300),
            new CompatJob($this->run, 'c3', 'ok', 300),
        ])->onConnection($this->connection)->onQueue($this->queue)->dispatch();
        $this->waitFor(fn (): bool => $this->count('c3', 'completed') > 0);
        $this->check('the links ran in order, one after the other',
            $this->first('c2', 'started') >= $this->first('c1', 'completed')
            && $this->first('c3', 'started') >= $this->first('c2', 'completed'));
        $this->check('each link ran once', array_sum(array_map(fn (string $j): int => $this->count($j, 'completed'), ['c1', 'c2', 'c3'])) === 3);
    }

    private function chainFailure(): void
    {
        $run = $this->run;
        Bus::chain([
            new CompatJob($run, 'c1'),
            new CompatJob($run, 'c2', 'throw'),
            new CompatJob($run, 'c3'),
        ])->catch(static function (Throwable $error) use ($run): void {
            app(FailureMatrixLog::class)->record($run, 'chain', null, 'chain_catch', 'compat', $error::class);
        })->onConnection($this->connection)->onQueue($this->queue)->dispatch();
        $this->waitFor(fn (): bool => $this->count('chain', 'chain_catch') > 0);
        $this->settle(5);
        $this->check('the link before the failure ran', $this->count('c1', 'completed') === 1);
        $this->check('the link after the failure never ran', $this->count('c3', 'started') === 0);
        $this->check('catch() ran once', $this->count('chain', 'chain_catch') === 1);
    }

    private function batch(): void
    {
        $batch = $this->batchOf([0, 1, 2, 3, 4], []);
        $this->waitFor(fn (): bool => $this->count('batch', 'batch_finally') > 0);
        $this->settle(3);
        $this->check('every job of the batch ran once', $this->completedEach(['b0', 'b1', 'b2', 'b3', 'b4']));
        $this->check('then() ran once', $this->count('batch', 'batch_then') === 1);
        $this->check('catch() never ran', $this->count('batch', 'batch_catch') === 0);
        $this->check('finally() ran once', $this->count('batch', 'batch_finally') === 1);
        $found = Bus::findBatch($batch->id);
        $this->check('the batch is finished with no failure', $found !== null && $found->finished() && $found->failedJobs === 0,
            $found === null ? 'not found' : "finished {$found->finished()}, failed {$found->failedJobs}");
    }

    private function batchFailure(): void
    {
        $batch = $this->batchOf([0, 2], [1]);
        $this->waitFor(fn (): bool => $this->count('batch', 'batch_finally') > 0);
        $this->settle(3);
        $this->check('catch() ran once', $this->count('batch', 'batch_catch') === 1);
        $this->check('then() never ran', $this->count('batch', 'batch_then') === 0);
        $this->check('finally() ran once', $this->count('batch', 'batch_finally') === 1);
        $found = Bus::findBatch($batch->id);
        $this->check('the batch is cancelled with one failure', $found !== null && $found->cancelled() && $found->failedJobs === 1,
            $found === null ? 'not found' : "cancelled {$found->cancelled()}, failed {$found->failedJobs}");
    }

    private function unique(): void
    {
        foreach (range(1, 3) as $ignored) {
            CompatUniqueJob::dispatch($this->run, 'u1', 'ok', 2000)->onConnection($this->connection)->onQueue($this->queue);
        }
        $this->waitFor(fn (): bool => $this->count('u1', 'completed') > 0);
        $this->settle(3);
        $this->check('three dispatches while one is pending run once', $this->count('u1', 'started') === 1,
            $this->count('u1', 'started') . ' runs');
        CompatUniqueJob::dispatch($this->run, 'u1', 'ok', 0)->onConnection($this->connection)->onQueue($this->queue);
        $this->waitFor(fn (): bool => $this->count('u1', 'completed') > 1);
        $this->check('once it ran, the lock is released', $this->count('u1', 'completed') === 2);
    }

    private function withoutOverlapping(): void
    {
        foreach (['o1', 'o2'] as $job) {
            CompatOverlapJob::dispatch($this->run, $job, 'ok', 2000, 10)->onConnection($this->connection)->onQueue($this->queue);
        }
        $this->waitFor(fn (): bool => $this->completedEach(['o1', 'o2']));
        $first = min($this->first('o1', 'started'), $this->first('o2', 'started'));
        $firstJob = $this->first('o1', 'started') === $first ? 'o1' : 'o2';
        $secondJob = $firstJob === 'o1' ? 'o2' : 'o1';
        $this->check('both ran', $this->completedEach(['o1', 'o2']));
        $this->check('never at the same time', $this->first($secondJob, 'started') >= $this->first($firstJob, 'completed'));
    }

    private function rateLimited(): void
    {
        $jobs = ['r1', 'r2', 'r3', 'r4'];
        foreach ($jobs as $job) {
            CompatRateLimitedJob::dispatch($this->run, $job, 'ok', 0, 30)->onConnection($this->connection)->onQueue($this->queue);
        }
        $this->waitFor(fn (): bool => $this->completedEach($jobs));
        $starts = array_map(fn (string $job): float => $this->first($job, 'started'), $jobs);
        sort($starts);
        $released = count(array_filter($jobs, fn (string $job): bool => $this->attemptOf($job, 'completed') > 1));
        $this->observed['spread_seconds'] = round(end($starts) - $starts[0], 2);
        $this->observed['released_jobs'] = $released;
        $this->check('every job ran once', $this->completedEach($jobs));
        $this->check('the limiter spread them over time', end($starts) - $starts[0] >= 2.0, round(end($starts) - $starts[0], 2) . ' s');
        $this->check('jobs over the limit were released, not failed', $released >= 1, "{$released} released");
    }

    private function backoffArray(): void
    {
        $this->policy('p1', 'throw', 0, tries: 3, backoff: [1, 3]);
        $this->waitFor(fn (): bool => $this->count('p1', 'failed_hook') > 0);
        $starts = $this->times('p1', 'started');
        $throws = $this->times('p1', 'threw');
        $gaps = [($starts[1] ?? 0) - ($throws[0] ?? 0), ($starts[2] ?? 0) - ($throws[1] ?? 0)];
        $this->observed['gaps_seconds'] = array_map(static fn (float $g): float => round($g, 2), $gaps);
        $this->check('three attempts', count($starts) === 3);
        $this->check('the second waited 1 s, the third 3 s', $gaps[0] >= 0.9 && $gaps[1] >= 2.7, implode(', ', $this->observed['gaps_seconds']));
        $this->check('failed() ran once', $this->count('p1', 'failed_hook') === 1);
    }

    private function retryUntil(): void
    {
        $this->policy('t1', 'throw', 0, tries: 100, backoff: 1, retryForSeconds: 6);
        $this->waitFor(fn (): bool => $this->count('t1', 'failed_hook') > 0);
        $starts = $this->times('t1', 'started');
        $span = ($this->first('t1', 'failed_hook') ?: 0) - ($starts[0] ?? 0);
        $this->observed['attempts'] = count($starts);
        $this->observed['failed_after_seconds'] = round($span, 2);
        $this->check('retried until the deadline, then failed', count($starts) >= 3 && $span >= 4.5 && $span <= 20,
            count($starts) . " attempts, failed after {$this->observed['failed_after_seconds']} s");
    }

    private function maxExceptions(): void
    {
        $this->policy('m1', 'throw', 0, tries: 10, maxExceptions: 2);
        $this->waitFor(fn (): bool => $this->count('m1', 'failed_hook') > 0);
        $this->settle(3);
        $this->check('failed after two exceptions, not ten tries', $this->count('m1', 'started') === 2, $this->count('m1', 'started') . ' attempts');
    }

    private function failOnTimeout(): void
    {
        $this->policy('f1', 'ok', 6000, tries: 3, timeout: 2, failOnTimeout: true);
        $this->waitFor(fn (): bool => $this->count('f1', 'failed_hook') > 0);
        $this->settle(5);
        $this->check('failed on its first timeout, not retried', $this->count('f1', 'started') === 1 && $this->count('f1', 'completed') === 0,
            $this->count('f1', 'started') . ' attempts');
        $this->check('the failure is a timeout', str_contains((string) $this->exceptionOf('f1'), 'TimeoutExceeded'), (string) $this->exceptionOf('f1'));
    }

    private function encrypted(): void
    {
        CompatEncryptedJob::dispatch($this->run, 'e1')->onConnection($this->connection)->onQueue($this->queue);
        $this->waitFor(fn (): bool => $this->count('e1', 'completed') > 0);
        $this->check('an encrypted job runs', $this->count('e1', 'completed') === 1);
    }

    private function afterCommit(): void
    {
        try {
            DB::transaction(function (): void {
                CompatJob::dispatch($this->run, 'a1')->onConnection($this->connection)->onQueue($this->queue)->afterCommit();
                throw new RuntimeException('roll back');
            });
        } catch (RuntimeException) {
            // Rolled back on purpose.
        }
        DB::transaction(function (): void {
            CompatJob::dispatch($this->run, 'a2')->onConnection($this->connection)->onQueue($this->queue)->afterCommit();
        });
        $this->waitFor(fn (): bool => $this->count('a2', 'completed') > 0);
        $this->settle(3);
        $this->check('a job dispatched in a rolled-back transaction never runs', $this->count('a1', 'started') === 0);
        $this->check('a job dispatched in a committed transaction runs', $this->count('a2', 'completed') === 1);
    }

    private function events(): void
    {
        CompatJob::dispatch($this->run, 'v1')->onConnection($this->connection)->onQueue($this->queue);
        CompatJob::dispatch($this->run, 'v2', 'throw')->onConnection($this->connection)->onQueue($this->queue);
        $this->waitFor(fn (): bool => $this->count('v1', 'completed') > 0 && $this->count('v2', 'failed_hook') > 0);
        $this->settle(2);
        $this->check('JobProcessing fired for both', $this->count('v1', 'event_before') === 1 && $this->count('v2', 'event_before') === 1);
        $this->check('JobProcessed fired for the one that succeeded', $this->count('v1', 'event_after') === 1);
        $this->check('JobFailed fired once for the one that failed', $this->count('v2', 'event_failing') === 1);
    }

    private function failedCommands(): void
    {
        foreach (['f1', 'f2', 'f3'] as $job) {
            CompatJob::dispatch($this->run, $job, 'fail-once')->onConnection($this->connection)->onQueue($this->queue);
        }
        $this->waitFor(fn (): bool => count($this->failedIds()) === 3);
        $this->check('three failed-job rows', count($this->failedIds()) === 3);
        Artisan::call('queue:retry', ['id' => ['all']]);
        $this->waitFor(fn (): bool => $this->completedEach(['f1', 'f2', 'f3']));
        $this->settle(2);
        $this->check('queue:retry all ran them again, and they succeeded', $this->completedEach(['f1', 'f2', 'f3']));
        $this->check('and left the failed-job store', count($this->failedIds()) === 0, implode(',', array_keys($this->failedIds())));

        foreach (['f4', 'f5'] as $job) {
            CompatJob::dispatch($this->run, $job, 'fail-once')->onConnection($this->connection)->onQueue($this->queue);
        }
        $this->waitFor(fn (): bool => count($this->failedIds()) === 2);
        $ids = $this->failedIds();
        // Queen's failed-job store keeps the last second out of an unqualified
        // flush, so a retry failing again in that second is never lost.
        sleep(2);
        Artisan::call('queue:forget', ['id' => $ids['f4'] ?? 'missing']);
        $this->check('queue:forget removed one row', array_keys($this->failedIds()) === ['f5'], implode(',', array_keys($this->failedIds())));
        Artisan::call('queue:flush');
        $this->check('queue:flush removed the rest', $this->failedIds() === []);
        $this->settle(5);
        $this->check('none of the forgotten jobs ran', $this->count('f4', 'completed') === 0 && $this->count('f5', 'completed') === 0);
    }

    private function queueSize(): void
    {
        $this->pauseWorkers();
        foreach (['s1', 's2', 's3'] as $job) {
            CompatJob::dispatch($this->run, $job)->onConnection($this->connection)->onQueue($this->queue);
        }
        CompatJob::dispatch($this->run, 's4')->onConnection($this->connection)->onQueue($this->queue)->delay(120);
        sleep(1);
        $queue = Queue::connection($this->connection);
        $size = $queue->size($this->queue);
        $this->observed['size_with_three_waiting_and_one_delayed'] = $size;
        $this->observed['pending_size'] = method_exists($queue, 'pendingSize') ? $queue->pendingSize($this->queue) : null;
        $this->observed['delayed_size'] = method_exists($queue, 'delayedSize') ? $queue->delayedSize($this->queue) : null;
        $this->check('size() counts the waiting jobs', $size >= 3, "size {$size}");
        try {
            $cleared = Artisan::call('queue:clear', ['connection' => $this->connection, '--queue' => $this->queue, '--force' => true]);
            $this->observed['queue_clear'] = ['exit' => $cleared, 'output' => trim(Artisan::output())];
        } catch (Throwable $error) {
            $this->observed['queue_clear'] = ['error' => $error::class . ': ' . $error->getMessage()];
        }
        $this->observed['size_after_clear'] = $queue->size($this->queue);
        $this->resumeWorkers();
        $this->settle(10);
        $ran = array_sum(array_map(fn (string $job): int => $this->count($job, 'completed'), ['s1', 's2', 's3']));
        $this->observed['waiting_jobs_that_ran_after_clear'] = $ran;
    }

    // -------------------------------------------------------------- helpers

    /**
     * @param list<int> $ok
     * @param list<int> $throwing
     */
    private function batchOf(array $ok, array $throwing): Batch
    {
        $run = $this->run;
        $jobs = [];
        foreach ($ok as $index) {
            // Staggered, so the workers never update the batch row at the
            // same instant: SQLite would refuse the second writer.
            $jobs[] = new CompatJob($run, "b{$index}", 'ok', 200 + 400 * $index);
        }
        foreach ($throwing as $index) {
            $jobs[] = new CompatJob($run, "b{$index}", 'throw');
        }
        // Three plain closures: serializable-closure cannot serialize a
        // closure returned by another closure on the same line.
        return Bus::batch($jobs)
            ->then(static function (Batch $batch) use ($run): void {
                app(FailureMatrixLog::class)->record($run, 'batch', null, 'batch_then', 'compat');
            })
            ->catch(static function (Batch $batch, Throwable $error) use ($run): void {
                app(FailureMatrixLog::class)->record($run, 'batch', null, 'batch_catch', 'compat', $error::class);
            })
            ->finally(static function (Batch $batch) use ($run): void {
                app(FailureMatrixLog::class)->record($run, 'batch', null, 'batch_finally', 'compat');
            })
            ->onConnection($this->connection)
            ->onQueue($this->queue)
            ->dispatch();
    }

    /** @param int|list<int> $backoff */
    private function policy(string $job, string $mode, int $sleepMs, int $tries, int|array $backoff = 0, ?int $maxExceptions = null,
        int $timeout = 60, bool $failOnTimeout = false, ?int $retryForSeconds = null): void
    {
        dispatch(CompatPolicyJob::make($this->run, $job, $mode, $sleepMs, $tries, $backoff, $maxExceptions, $timeout, $failOnTimeout, $retryForSeconds)
            ->onConnection($this->connection)->onQueue($this->queue));
    }

    private function pauseWorkers(): void
    {
        if ($this->connection === 'redis') {
            Artisan::call('horizon:pause');
            sleep(4);

            return;
        }
        Artisan::call('queen:supervisor', ['action' => 'pause']);
        $this->waitFor(function (): bool {
            Artisan::call('queen:supervisor', ['action' => 'status', '--json' => true]);
            $status = json_decode(trim(Artisan::output()), true);

            return is_array($status) && ($status['paused'] ?? false) === true
                && ($status['process_budget']['active_worker_processes'] ?? 1) === 0;
        }, 60);
    }

    private function resumeWorkers(): void
    {
        $this->connection === 'redis'
            ? Artisan::call('horizon:continue')
            : Artisan::call('queen:supervisor', ['action' => 'continue']);
    }

    /** @return array<string, string> failed-job id per compat job id of this run */
    private function failedIds(): array
    {
        $ids = [];
        foreach (app('queue.failer')->all() as $record) {
            $record = (array) $record;
            $command = (string) (json_decode((string) ($record['payload'] ?? ''), true)['data']['command'] ?? '');
            if (str_contains($command, $this->run) && preg_match('/s:5:"jobId";s:\d+:"([^"]+)"/', $command, $match) === 1) {
                $ids[$match[1]] = (string) ($record['uuid'] ?? $record['id']);
            }
        }
        ksort($ids);

        return $ids;
    }

    private function mark(string $event): void
    {
        $this->log->record($this->run, 'command', null, $event, 'compat');
    }

    private function waitFor(callable $done, ?int $timeout = null): void
    {
        $deadline = microtime(true) + ($timeout ?? (int) $this->option('timeout'));
        while (!$done()) {
            if (microtime(true) > $deadline) {
                throw new RuntimeException('timed out waiting for the workers');
            }
            usleep(250_000);
        }
    }

    private function settle(int $seconds): void
    {
        sleep($seconds);
    }

    /** @return list<array<string, mixed>> */
    private function eventsOf(string $job): array
    {
        return array_values(array_filter($this->log->read($this->run), static fn (array $e): bool => $e['job_id'] === $job));
    }

    private function count(string $job, string $event): int
    {
        return count(array_filter($this->eventsOf($job), static fn (array $e): bool => $e['event'] === $event));
    }

    /** @return list<float> */
    private function times(string $job, string $event): array
    {
        return array_values(array_map(static fn (array $e): float => (float) $e['at'],
            array_filter($this->eventsOf($job), static fn (array $e): bool => $e['event'] === $event)));
    }

    private function first(string $job, string $event): float
    {
        return $this->times($job, $event)[0] ?? 0.0;
    }

    private function attemptOf(string $job, string $event): int
    {
        foreach ($this->eventsOf($job) as $e) {
            if ($e['event'] === $event) {
                return (int) $e['attempt'];
            }
        }

        return 0;
    }

    private function exceptionOf(string $job): ?string
    {
        foreach ($this->eventsOf($job) as $e) {
            if ($e['event'] === 'failed_hook') {
                return $e['exception'];
            }
        }

        return null;
    }

    /** @param list<string> $jobs */
    private function completedEach(array $jobs): bool
    {
        foreach ($jobs as $job) {
            if ($this->count($job, 'completed') !== 1) {
                return false;
            }
        }

        return true;
    }

    private function check(string $name, bool $passed, string $detail = ''): void
    {
        $this->checks[] = ['name' => $name, 'passed' => $passed, 'detail' => $detail];
    }
}
