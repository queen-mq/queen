<?php

namespace App\Console\Commands;

use App\Jobs\Compat\CompatJob;
use App\Jobs\Compat\CompatRouteJob;
use App\Queue\QueueRoutes;
use App\Support\FailureMatrixLog;
use Illuminate\Bus\Batch;
use Illuminate\Queue\Events\JobQueued;
use Illuminate\Support\Facades\Bus;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Queue;
use LogicException;
use RuntimeException;
use Throwable;

/**
 * cb3's dispatch-only default connection, end to end, on a routed lane
 * (BENCH_ROUTED): every job is dispatched without naming a connection, so it
 * goes through `routed`, and must run on the connection of its queue's pool
 * with that connection's settings. Each job logs where it ran; JobQueued
 * shows which connection queued it, and when.
 */
final class CompatRoutedCommand extends CompatScenarioCommand
{
    public const SCENARIOS = [
        'route-push', 'route-later', 'route-bulk', 'route-batch', 'route-chain', 'route-after-commit', 'route-pop',
    ];

    protected $signature = 'bench:compat-routed {scenario : one of the scenarios}
        {--run-id= : Run identifier}
        {--timeout=90 : Seconds to wait for the workers}';

    protected $description = 'Check one path through the routed default connection against the running workers';

    /** @var array<string, array{connection: string, queue: string, delay: mixed, at: float}> JobQueued per compat job id */
    private array $queued = [];

    public function handle(FailureMatrixLog $log): int
    {
        Event::listen(JobQueued::class, function (JobQueued $event): void {
            if ($event->job instanceof CompatJob) {
                $this->queued[$event->job->jobId] = [
                    'connection' => (string) $event->connectionName,
                    'queue' => (string) $event->queue,
                    'delay' => $event->delay,
                    'at' => microtime(true),
                ];
            }
        });

        return $this->runScenario($log, self::SCENARIOS, function (string $scenario): void {
            if ($this->connection !== 'routed') {
                throw new RuntimeException('not a routed lane: set BENCH_ROUTED=true');
            }
            $this->{$scenario}();
        });
    }

    // ------------------------------------------------------------ scenarios

    private function routePush(): void
    {
        // Two jobs per queue, more on two queues: an ordered pool's single
        // partition takes every job; a 64-partition pool spreads six.
        $jobs = [];
        foreach ($this->queues() as $queue) {
            $count = match ($queue) {
                'default' => 6,
                'ordered-sync' => 4,
                default => 2,
            };
            foreach (range(1, $count) as $index) {
                $job = "{$queue}-{$index}";
                $jobs[$job] = $queue;
                dispatch(new CompatRouteJob($this->run, $job))->onQueue($queue);
            }
        }
        $this->waitFor(fn (): bool => $this->completedEach(array_keys($jobs)));
        $this->settle(2);
        $this->checkRan($jobs, 'dispatch()');
        foreach (['default', 'ordered-sync'] as $queue) {
            $partitions = array_values(array_unique(array_map(fn (string $job): mixed => $this->where($job)['partition'] ?? null,
                array_keys(array_filter($jobs, static fn (string $q): bool => $q === $queue)))));
            sort($partitions);
            $this->observed["partitions of {$queue}"] = $partitions;
            if ($this->backend === 'queen') {
                $expected = $queue === 'ordered-sync' ? count($partitions) === 1 : count($partitions) > 1;
                $this->check("queen: {$queue} jobs in " . ($queue === 'ordered-sync' ? 'the one partition' : 'several partitions'),
                    $expected, implode(',', $partitions));
            }
        }
    }

    private function routeLater(): void
    {
        $this->mark('dispatched');
        dispatch(new CompatRouteJob($this->run, 'l1'))->onQueue('ical')->delay(4);
        Queue::later(4, new CompatRouteJob($this->run, 'l2'), '', 'notifications');
        $this->waitFor(fn (): bool => $this->completedEach(['l1', 'l2']));
        $this->settle(2);
        $this->checkRan(['l1' => 'ical', 'l2' => 'notifications'], 'later()');
        foreach (['l1', 'l2'] as $job) {
            $waited = $this->first($job, 'started') - $this->first('command', 'dispatched');
            // Laravel's Redis queue keeps due times in whole seconds: allow 1 s early.
            $this->check("{$job} waited its 4 s delay", $waited >= 3.0 && $waited <= 30, round($waited, 2) . ' s');
            $this->near("{$job} delay", $waited, 1.5);
        }
    }

    private function routeBulk(): void
    {
        $jobs = ['k1' => 'background', 'k2' => 'background', 'k3' => 'background'];
        Queue::bulk(array_map(fn (string $job): CompatRouteJob => new CompatRouteJob($this->run, $job), array_keys($jobs)), '', 'background');
        // A bulk() in a transaction: the pool connections have after_commit on.
        DB::transaction(function (): void {
            Queue::bulk([new CompatRouteJob($this->run, 'k4'), new CompatRouteJob($this->run, 'k5')], '', 'compliance');
            sleep(2);
            $this->observed['in_transaction'] = ['queued' => array_keys(array_intersect_key($this->queued, ['k4' => 1, 'k5' => 1])),
                'started' => $this->count('k4', 'started') + $this->count('k5', 'started')];
        });
        $this->mark('committed');
        $jobs += ['k4' => 'compliance', 'k5' => 'compliance'];
        $this->waitFor(fn (): bool => $this->completedEach(array_keys($jobs)));
        $this->settle(2);
        $this->checkRan($jobs, 'bulk()');
        $this->check('a bulk() in a transaction was queued only at the commit',
            $this->observed['in_transaction'] === ['queued' => [], 'started' => 0]
            && min($this->queued['k4']['at'] ?? 0, $this->queued['k5']['at'] ?? 0) >= $this->first('command', 'committed') - 0.5,
            json_encode($this->observed['in_transaction']));
        $this->same('bulk in a transaction: queued or started before the commit', $this->observed['in_transaction']);
    }

    private function routeBatch(): void
    {
        $jobs = ['b1' => 'compliance', 'b2' => 'compliance', 'b3' => 'compliance'];
        $batch = $this->withBatchCallbacks(Bus::batch(array_map(fn (string $job): CompatRouteJob => new CompatRouteJob($this->run, $job),
            array_keys($jobs))))->onQueue('compliance')->dispatch();
        $this->waitFor(fn (): bool => $this->count('batch', 'batch_finally') > 0);
        $this->settle(3);
        $this->checkRan($jobs, 'Bus::batch()');
        $found = Bus::findBatch($batch->id);
        $callbacks = array_map(fn (string $event): int => $this->count('batch', $event), ['batch_then', 'batch_catch', 'batch_finally']);
        $this->check('then() and finally() ran once, catch() never', $callbacks === [1, 0, 1], implode(',', $callbacks));
        $this->check('the batch is finished', $found instanceof Batch && $found->finished() && $found->pendingJobs === 0);
        $this->same('batch then(), catch(), finally()', $callbacks);
    }

    private function routeChain(): void
    {
        // The next link is dispatched by the worker that ran the last one, on
        // another pool's connection: that pool's settings must apply to it.
        Bus::chain([
            new CompatRouteJob($this->run, 'c1'),
            (new CompatRouteJob($this->run, 'c2'))->onQueue('compliance'),
            (new CompatRouteJob($this->run, 'c3'))->onQueue('ordered-sync'),
        ])->dispatch();
        $this->waitFor(fn (): bool => $this->count('c3', 'completed') > 0);
        $this->settle(2);
        $this->checkRan(['c1' => 'default', 'c2' => 'compliance', 'c3' => 'ordered-sync'], 'a chain', withQueued: false);
        $this->check('the links ran in order',
            $this->first('c2', 'started') >= $this->first('c1', 'completed') && $this->first('c3', 'started') >= $this->first('c2', 'completed'));
    }

    private function routeAfterCommit(): void
    {
        // Laravel docs, Queues > Jobs and Database Transactions: with
        // after_commit, a job dispatched in a transaction is queued when it
        // commits, and never when it rolls back. The router leaves that to
        // the pool connection.
        DB::transaction(function (): void {
            dispatch(new CompatRouteJob($this->run, 'a1'))->onQueue('notifications');
            dispatch(new CompatRouteJob($this->run, 'a2'))->onQueue('compliance')->delay(1);
            sleep(3);
            $this->observed['in_transaction'] = [
                'queued' => array_keys(array_intersect_key($this->queued, ['a1' => 1, 'a2' => 1])),
                'started' => $this->count('a1', 'started') + $this->count('a2', 'started'),
                'size_notifications' => Queue::size('notifications'),
            ];
        });
        $this->mark('committed');
        try {
            DB::transaction(function (): void {
                dispatch(new CompatRouteJob($this->run, 'r1'))->onQueue('notifications');
                Queue::later(1, new CompatRouteJob($this->run, 'r2'), '', 'ical');
                Queue::bulk([new CompatRouteJob($this->run, 'r3')], '', 'background');
                throw new RuntimeException('roll back');
            });
        } catch (RuntimeException) {
            // Rolled back on purpose.
        }
        $this->waitFor(fn (): bool => $this->completedEach(['a1', 'a2']));
        $this->settle(5);
        $committed = $this->first('command', 'committed');
        $this->check('nothing was queued, run or visible before the commit',
            $this->observed['in_transaction'] === ['queued' => [], 'started' => 0, 'size_notifications' => 0],
            json_encode($this->observed['in_transaction']));
        $this->check('both were queued at the commit, and ran after it',
            min($this->queued['a1']['at'] ?? 0, $this->queued['a2']['at'] ?? 0) >= $committed - 0.5
            && min($this->first('a1', 'started'), $this->first('a2', 'started')) >= $committed - 0.5);
        $this->checkRan(['a1' => 'notifications', 'a2' => 'compliance'], 'a committed dispatch');
        $rolledBack = ['r1', 'r2', 'r3'];
        $this->check('dispatch(), later() and bulk() in a rolled-back transaction: never queued, never run',
            array_intersect_key($this->queued, array_flip($rolledBack)) === []
            && array_sum(array_map(fn (string $job): int => count($this->eventsOf($job)), $rolledBack)) === 0);
        $this->same('before the commit: queued, run, visible', $this->observed['in_transaction']);
        $this->same('rolled back: queued, events', [array_keys(array_intersect_key($this->queued, array_flip($rolledBack))),
            array_sum(array_map(fn (string $job): int => count($this->eventsOf($job)), $rolledBack))]);
    }

    private function routePop(): void
    {
        try {
            Queue::connection('routed')->pop('default');
            $refused = 'no exception';
        } catch (LogicException $error) {
            $refused = $error->getMessage();
        } catch (Throwable $error) {
            $refused = $error::class . ': ' . $error->getMessage();
        }
        $this->check('pop() on the routed connection is refused',
            $refused === 'The routed queue connection only dispatches; workers pop from the pool connections.', $refused);
        // Every worker of this container works a pool connection: one started
        // without a connection would work queue.default, `routed`, and fail
        // at its first pop.
        $workers = $this->workerConnections();
        $pools = array_map(fn (string $pool): string => "{$this->backend}-{$pool}", array_keys((array) config('benchmark.routed_pools')));
        $this->observed['worker_connections'] = $workers;
        $this->check('every worker works a pool connection', $workers !== [] && array_diff(array_values($workers), $pools) === [],
            json_encode($workers));
        $this->check('every pool has a worker', array_diff($pools, array_values($workers)) === [], json_encode($workers));
        $this->same('routed pop()', $refused);
        // Sorted: the order is the workers' pids, which each engine assigns its own way.
        $working = array_values(array_unique(array_map(static fn (string $c): string => explode('-', $c, 2)[1] ?? $c, array_values($workers))));
        sort($working);
        $this->same('pools with workers', $working);
    }

    // -------------------------------------------------------------- helpers

    /** @return list<string> */
    private function queues(): array
    {
        return array_merge(...array_values(array_column((array) config('benchmark.routed_pools'), 'queues')));
    }

    /**
     * Each job ran once, in a worker of its queue's pool, on that pool's
     * connection and settings, and (unless withQueued is false: a chain link
     * is queued by a worker, not by this command) was queued on it.
     *
     * @param array<string, string> $jobs queue per job id
     */
    private function checkRan(array $jobs, string $how, bool $withQueued = true): void
    {
        $routes = QueueRoutes::fromConfig();
        $pools = (array) config('benchmark.routed_pools');
        $wrong = [];
        $outcome = [];
        foreach ($jobs as $job => $queue) {
            $pool = (string) $routes->poolFor($queue);
            $connection = "{$this->backend}-{$pool}";
            $name = $this->backend === 'queen' ? QueueRoutes::queenName($queue) : $queue;
            $where = $this->where($job);
            $problems = [];
            if ($this->count($job, 'completed') !== 1) {
                $problems[] = 'ran ' . $this->count($job, 'completed') . ' times';
            }
            if (($where['connection'] ?? null) !== $connection || ($where['queue'] ?? null) !== $name) {
                $problems[] = "ran on {$where['connection']}:{$where['queue']}";
            }
            if ((int) ($where['retry_after'] ?? 0) !== $pools[$pool]['retry_after']) {
                $problems[] = "lease {$where['retry_after']} s, not {$pools[$pool]['retry_after']}";
            }
            if ($withQueued && (($this->queued[$job]['connection'] ?? null) !== $connection || ($this->queued[$job]['queue'] ?? null) !== $name)) {
                $problems[] = 'queued on ' . json_encode($this->queued[$job] ?? null);
            }
            if ($this->backend === 'queen') {
                $group = (string) config("queue.connections.{$connection}.consumer_group", config('queen.consumer_group'));
                $partition = (string) ($where['partition'] ?? '');
                $slot = (int) substr($partition, strrpos($partition, '-') + 1);
                if ($where['pool_connection'] !== $connection || (int) $where['pool_retry_after'] !== $pools[$pool]['retry_after']
                    || $where['pool_consumer_group'] !== $group) {
                    $problems[] = 'worker of pool ' . json_encode([$where['pool_connection'], $where['pool_retry_after'], $where['pool_consumer_group']]);
                }
                if ($where['consumer_group'] !== $group || $where['partitions'] !== $pools[$pool]['partitions']
                    || !str_starts_with($partition, $where['partition_prefix'] . '-') || $slot >= $pools[$pool]['partitions']) {
                    $problems[] = "group {$where['consumer_group']}, {$where['partitions']} partitions, partition {$partition}";
                }
            }
            if ($problems !== []) {
                $wrong[$job] = implode('; ', $problems);
            }
            // Comparable with Horizon: the pool and lease, not the backend's names.
            $outcome[$job] = ['pool' => $pool, 'runs' => $this->attemptsOf($job, 'started'),
                'lease' => (int) ($where['retry_after'] ?? 0), 'on_its_pool' => !isset($wrong[$job])];
        }
        $this->check("{$how}: every job ran once on its pool's connection, with the pool's lease"
            . ($this->backend === 'queen' ? ', consumer group and partitions' : ''), $wrong === [], json_encode($wrong));
        $this->same("{$how}: jobs", $outcome);
        $this->observed['where'] = array_map(fn (string $job): array => $this->where($job), array_combine(array_keys($jobs), array_keys($jobs)));
        $this->observed['queued'] = array_intersect_key($this->queued, $jobs);
    }

    /** @return array<string, mixed> where a worker ran the job: CompatRouteJob's log */
    private function where(string $job): array
    {
        foreach ($this->eventsOf($job) as $event) {
            if ($event['event'] === 'started') {
                return (array) ($event['detail'] ?? []);
            }
        }

        return [];
    }

    /** @return array<int, string> the connection each worker process of this container works, by pid */
    private function workerConnections(): array
    {
        $workers = [];
        foreach (glob('/proc/[0-9]*/cmdline') ?: [] as $path) {
            // A process that set its title has one string with spaces, not
            // one string per argument.
            $args = preg_split('/[\0 ]+/', trim((string) @file_get_contents($path), "\0 ")) ?: [];
            $at = array_search($this->backend === 'redis' ? 'horizon:work' : 'queue:work', $args, true);
            if ($at !== false) {
                $next = $args[$at + 1] ?? '';
                $workers[(int) basename(dirname($path))] = str_starts_with($next, '-') ? '(none)' : $next;
            }
        }
        ksort($workers);

        return $workers;
    }
}
