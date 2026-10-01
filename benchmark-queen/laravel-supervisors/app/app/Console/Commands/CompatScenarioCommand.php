<?php

namespace App\Console\Commands;

use App\Support\FailureMatrixLog;
use Closure;
use Illuminate\Bus\Batch;
use Illuminate\Bus\PendingBatch;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use InvalidArgumentException;
use RuntimeException;
use Throwable;

/**
 * What the Laravel compatibility commands share: run one scenario against the
 * running supervisor of this container, read what the workers logged, and
 * print one JSON object with the scenario, its checks and what was observed.
 */
abstract class CompatScenarioCommand extends Command
{
    protected FailureMatrixLog $log;

    protected string $run;

    protected string $connection;

    protected string $queue;

    /** @var list<array{name: string, passed: bool, detail: string}> */
    protected array $checks = [];

    /** @var array<string, mixed> */
    protected array $observed = [];

    /**
     * @param list<string> $scenarios
     * @param Closure(string): void $play runs the scenario method of that name;
     *        a closure, so each command keeps its scenario methods private
     */
    protected function runScenario(FailureMatrixLog $log, array $scenarios, Closure $play): int
    {
        $scenario = (string) $this->argument('scenario');
        if (!in_array($scenario, $scenarios, true)) {
            throw new InvalidArgumentException("Unknown scenario [{$scenario}].");
        }
        $this->log = $log;
        $this->run = (string) ($this->option('run-id') ?: $scenario . '-' . bin2hex(random_bytes(4)));
        $this->connection = (string) config('benchmark.connection');
        $this->queue = (string) config('benchmark.queue');

        try {
            $play(lcfirst(str_replace(' ', '', ucwords(str_replace('-', ' ', $scenario)))));
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

    /** then(), catch() and finally() callbacks that log under the job id `batch`. */
    protected function withBatchCallbacks(PendingBatch $batch): PendingBatch
    {
        $run = $this->run;

        // Three plain closures: serializable-closure cannot serialize a
        // closure returned by another closure on the same line.
        return $batch
            ->then(static function (Batch $batch) use ($run): void {
                app(FailureMatrixLog::class)->record($run, 'batch', null, 'batch_then', 'compat');
            })
            ->catch(static function (Batch $batch, Throwable $error) use ($run): void {
                app(FailureMatrixLog::class)->record($run, 'batch', null, 'batch_catch', 'compat', $error::class);
            })
            ->finally(static function (Batch $batch) use ($run): void {
                app(FailureMatrixLog::class)->record($run, 'batch', null, 'batch_finally', 'compat');
            });
    }

    protected function pauseWorkers(): void
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

    protected function resumeWorkers(): void
    {
        $this->connection === 'redis'
            ? Artisan::call('horizon:continue')
            : Artisan::call('queen:supervisor', ['action' => 'continue']);
    }

    /** @return array<string, string> failed-job id per compat job id of this run */
    protected function failedIds(): array
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

    protected function mark(string $event): void
    {
        $this->log->record($this->run, 'command', null, $event, 'compat');
    }

    protected function waitFor(callable $done, ?int $timeout = null): void
    {
        $deadline = microtime(true) + ($timeout ?? (int) $this->option('timeout'));
        while (!$done()) {
            if (microtime(true) > $deadline) {
                throw new RuntimeException('timed out waiting for the workers');
            }
            usleep(250_000);
        }
    }

    protected function settle(int $seconds): void
    {
        sleep($seconds);
    }

    /** @return list<array<string, mixed>> */
    protected function eventsOf(string $job): array
    {
        return array_values(array_filter($this->log->read($this->run), static fn (array $e): bool => $e['job_id'] === $job));
    }

    protected function count(string $job, string $event): int
    {
        return count(array_filter($this->eventsOf($job), static fn (array $e): bool => $e['event'] === $event));
    }

    /** @return list<float> */
    protected function times(string $job, string $event): array
    {
        return array_values(array_map(static fn (array $e): float => (float) $e['at'],
            array_filter($this->eventsOf($job), static fn (array $e): bool => $e['event'] === $event)));
    }

    protected function first(string $job, string $event): float
    {
        return $this->times($job, $event)[0] ?? 0.0;
    }

    protected function attemptOf(string $job, string $event): int
    {
        foreach ($this->eventsOf($job) as $e) {
            if ($e['event'] === $event) {
                return (int) $e['attempt'];
            }
        }

        return 0;
    }

    protected function exceptionOf(string $job, string $event = 'failed_hook'): ?string
    {
        foreach ($this->eventsOf($job) as $e) {
            if ($e['event'] === $event) {
                return $e['exception'];
            }
        }

        return null;
    }

    /** Whether every `event` of the job was logged by a process other than this command: a worker. */
    protected function inWorker(string $job, string $event): bool
    {
        $pids = array_column(array_filter($this->eventsOf($job), static fn (array $e): bool => $e['event'] === $event), 'pid');

        return $pids !== [] && !in_array(getmypid(), $pids, true);
    }

    /** @param list<string> $jobs */
    protected function completedEach(array $jobs): bool
    {
        foreach ($jobs as $job) {
            if ($this->count($job, 'completed') !== 1) {
                return false;
            }
        }

        return true;
    }

    protected function check(string $name, bool $passed, string $detail = ''): void
    {
        $this->checks[] = ['name' => $name, 'passed' => $passed, 'detail' => $detail];
    }
}
