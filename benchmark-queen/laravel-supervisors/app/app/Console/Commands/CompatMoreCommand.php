<?php

namespace App\Console\Commands;

use App\Events\CompatEvent;
use App\Jobs\Compat\CompatBatchCancellerJob;
use App\Jobs\Compat\CompatForkJob;
use App\Jobs\Compat\CompatJob;
use App\Jobs\Compat\CompatMissingModelJob;
use App\Jobs\Compat\CompatModelJob;
use App\Jobs\Compat\CompatReleaseJob;
use App\Jobs\Compat\CompatSkipIfCancelledJob;
use App\Jobs\Compat\CompatSkippedJob;
use App\Jobs\Compat\CompatThrottledJob;
use App\Jobs\Compat\CompatUniqueUntilProcessingJob;
use App\Models\CompatUser;
use App\Notifications\CompatNotification;
use App\Support\FailureMatrixLog;
use Illuminate\Queue\Events\QueueBusy;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Bus;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Notification;
use RuntimeException;
use Throwable;

/**
 * More Laravel queue features, checked the way `bench:compat` checks its own:
 * queued listeners, notifications and closures, two more job middleware,
 * unique until processing, missing models, batch failures and cancellation,
 * the batch-retry and prune commands, and delays; and a job that forks.
 * `bench:compat setup` creates the tables these use too. prune-failed empties
 * the failed-job store: no scenario after it reads that store.
 */
final class CompatMoreCommand extends CompatScenarioCommand
{
    public const SCENARIOS = [
        'queued-listener', 'queued-notification', 'queued-closure', 'throttles-exceptions', 'skip-middleware',
        'unique-until-processing', 'missing-models', 'batch-allow-failures', 'batch-cancel', 'retry-batch',
        'delay-datetime', 'release-delay', 'queue-monitor', 'prune-failed', 'fork-in-job',
    ];

    protected $signature = 'bench:compat-more {scenario : one of the scenarios}
        {--run-id= : Run identifier}
        {--timeout=90 : Seconds to wait for the workers}';

    protected $description = 'Check one more Laravel queue feature against the running workers';

    public function handle(FailureMatrixLog $log): int
    {
        return $this->runScenario($log, self::SCENARIOS, fn (string $scenario) => $this->{$scenario}());
    }

    // ------------------------------------------------------------ scenarios

    private function queuedListener(): void
    {
        CompatEvent::dispatch($this->run, 'l1', 'ok');
        CompatEvent::dispatch($this->run, 'l2', 'throw');
        $this->waitFor(fn (): bool => $this->count('l1', 'completed') > 0 && $this->count('l2', 'failed_hook') > 0);
        $this->settle(3);
        // Laravel docs, Events > Queued Event Listeners.
        $this->check('the queued listener ran once, in a worker', $this->count('l1', 'completed') === 1 && $this->inWorker('l1', 'started'),
            $this->count('l1', 'started') . ' runs');
        // Laravel docs, Events > Queued Event Listeners > Handling Failed Jobs: $tries, then failed().
        $this->check('a throwing listener was tried twice ($tries = 2), then failed() ran once',
            $this->count('l2', 'started') === 2 && $this->count('l2', 'failed_hook') === 1,
            $this->count('l2', 'started') . ' runs, failed() ' . $this->count('l2', 'failed_hook') . ' times');
    }

    private function queuedNotification(): void
    {
        $users = array_map(fn (string $name): CompatUser => CompatUser::create(['name' => "{$this->run}-{$name}"]), ['a', 'b', 'c']);
        Notification::send($users, (new CompatNotification($this->run))->onConnection($this->connection)->onQueue($this->queue));
        $jobs = array_map(static fn (CompatUser $user): string => 'n' . $user->getKey(), $users);
        $this->waitFor(fn (): bool => array_filter($jobs, fn (string $job): bool => $this->count($job, 'notified') === 0) === []);
        $this->settle(3);
        $deliveries = array_map(fn (string $job): int => $this->count($job, 'notified'), $jobs);
        $this->observed['deliveries'] = array_combine($jobs, $deliveries);
        // Laravel docs, Notifications > Queueing Notifications: one queued job per notifiable and channel.
        $this->check('each of three notifiables was notified once', $deliveries === [1, 1, 1], implode(', ', $deliveries));
        $this->check('by a worker, not by the sender',
            array_filter($jobs, fn (string $job): bool => !$this->inWorker($job, 'notified')) === []);
    }

    private function queuedClosure(): void
    {
        $run = $this->run;
        dispatch(static function () use ($run): void {
            app(FailureMatrixLog::class)->record($run, 'k1', null, 'completed', 'closure');
        })->onConnection($this->connection)->onQueue($this->queue);
        // One closure per line: serializable-closure finds a closure by its line.
        dispatch(static function () use ($run): void {
            app(FailureMatrixLog::class)->record($run, 'k2', null, 'started', 'closure');
            throw new RuntimeException('compat: a queued closure that throws');
        })
            ->catch(static function (Throwable $error) use ($run): void {
                app(FailureMatrixLog::class)->record($run, 'k2', null, 'closure_catch', 'closure', $error::class);
            })
            ->onConnection($this->connection)
            ->onQueue($this->queue);
        $this->waitFor(fn (): bool => $this->count('k1', 'completed') > 0 && $this->count('k2', 'closure_catch') > 0);
        $this->settle(3);
        // Laravel docs, Queues > Queueing Closures.
        $this->check('a queued closure ran once, in a worker', $this->count('k1', 'completed') === 1 && $this->inWorker('k1', 'completed'));
        // Laravel docs, Queues > Queueing Closures: catch() runs once the closure has failed.
        // A closure has no $tries: the worker's --tries applies, 1 here, 2 in cb3's interactive pool.
        $tries = $this->workerTries();
        $this->check("a throwing closure ran {$tries} time(s), the worker's tries, then its catch() callback ran once",
            $this->count('k2', 'started') === $tries && $this->count('k2', 'closure_catch') === 1,
            $this->count('k2', 'started') . ' runs, catch() ' . $this->count('k2', 'closure_catch') . ' times');
        // Laravel docs, Queues > Dealing With Failed Jobs: a failed closure is
        // a failed job like any other, with its row in the failed-job store.
        $rows = $this->failedClosureRows();
        $this->check('the failed closure left one failed-job row', $rows === 1, "{$rows} rows");
        $this->same('k1 runs', $this->count('k1', 'completed'));
        $this->same('k2 runs, catch(), failed-job rows', [$this->count('k2', 'started'), $this->count('k2', 'closure_catch'), $rows]);
    }

    private function throttlesExceptions(): void
    {
        CompatThrottledJob::dispatch($this->run, 'h1', 'ok', 0, 10)->onConnection($this->connection)->onQueue($this->queue);
        $this->waitFor(fn (): bool => $this->count('h1', 'completed') > 0 || $this->count('h1', 'failed_hook') > 0);
        $this->settle(2);
        $starts = $this->times('h1', 'started');
        $throws = $this->times('h1', 'threw');
        $gaps = [($starts[1] ?? 0) - ($throws[0] ?? 0), ($starts[2] ?? 0) - ($throws[1] ?? 0)];
        $this->observed['gaps_seconds'] = array_map(static fn (float $g): float => round($g, 2), $gaps);
        $this->observed['attempt_that_completed'] = $this->attemptOf('h1', 'completed');
        // Laravel docs, Queues > Job Middleware > Throttling Exceptions.
        $this->check('every exception released the job: it never failed, and completed once',
            $this->count('h1', 'completed') === 1 && $this->count('h1', 'failed_hook') === 0 && !isset($this->failedIds()['h1']),
            count($starts) . ' runs, failed() ' . $this->count('h1', 'failed_hook') . ' times');
        $this->check('under the threshold, it ran again at once', $gaps[0] > 0.0 && $gaps[0] < 3.0, round($gaps[0], 2) . ' s');
        // Two exceptions reach ThrottlesExceptions(2, 4): no run for 4 s. Laravel's
        // Redis queue keeps due times in whole seconds, so allow 1 s early.
        $this->check('after two exceptions, it was held back for the 4 s decay', $gaps[1] >= 3.0, round($gaps[1], 2) . ' s');
    }

    private function skipMiddleware(): void
    {
        CompatSkippedJob::dispatch($this->run, 's1', 'skip')->onConnection($this->connection)->onQueue($this->queue);
        CompatSkippedJob::dispatch($this->run, 's2', 'ok')->onConnection($this->connection)->onQueue($this->queue);
        $this->waitFor(fn (): bool => $this->count('s1', 'event_after') > 0 && $this->count('s2', 'completed') > 0);
        $this->settle(3);
        // Laravel docs, Queues > Job Middleware > Skipping Jobs.
        $this->check('Skip::when(true): the job was taken once and deleted without running',
            $this->count('s1', 'event_before') === 1 && $this->count('s1', 'started') === 0,
            $this->count('s1', 'event_before') . ' takes, ' . $this->count('s1', 'started') . ' runs');
        $this->check('with no failed-job row and no failed()', !isset($this->failedIds()['s1']) && $this->count('s1', 'failed_hook') === 0);
        $this->check('Skip::when(false): the job ran', $this->count('s2', 'completed') === 1);
    }

    private function uniqueUntilProcessing(): void
    {
        CompatUniqueUntilProcessingJob::dispatch($this->run, 'u1', 'ok', 3000)
            ->onConnection($this->connection)->onQueue($this->queue)->delay(4);
        CompatUniqueUntilProcessingJob::dispatch($this->run, 'u2')->onConnection($this->connection)->onQueue($this->queue);
        $this->waitFor(fn (): bool => $this->count('u1', 'started') > 0);
        CompatUniqueUntilProcessingJob::dispatch($this->run, 'u3')->onConnection($this->connection)->onQueue($this->queue);
        $this->waitFor(fn (): bool => $this->count('u1', 'completed') > 0 && $this->count('u3', 'completed') > 0);
        $this->settle(3);
        $this->observed['third_started_before_the_first_completed'] = $this->first('u3', 'started') < $this->first('u1', 'completed');
        // Laravel docs, Queues > Unique Jobs > Keeping Jobs Unique Until Processing Begins.
        $this->check('a second dispatch while the first waited was refused', $this->count('u2', 'started') === 0);
        $this->check('a dispatch once the first was processing was accepted', $this->count('u3', 'completed') === 1);
        $this->check('the first ran once', $this->count('u1', 'completed') === 1);
        $this->sameJobs(['u1', 'u2', 'u3']);
    }

    private function missingModels(): void
    {
        $dropped = CompatUser::create(['name' => "{$this->run}-dropped"]);
        $failing = CompatUser::create(['name' => "{$this->run}-failing"]);
        dispatch(CompatMissingModelJob::carrying($dropped, $this->run, 'm1')->onConnection($this->connection)->onQueue($this->queue)->delay(3));
        dispatch(CompatModelJob::carrying($failing, $this->run, 'm2')->onConnection($this->connection)->onQueue($this->queue)->delay(3));
        $dropped->delete();
        $failing->delete();
        $this->waitFor(fn (): bool => $this->count('m1', 'event_after') > 0 && isset($this->failedIds()['m2']));
        $this->settle(3);
        $exception = (string) $this->exceptionOf('m2', 'event_failing');
        // Laravel docs, Queues > Class Structure > Handling Missing Models.
        $this->check('deleteWhenMissingModels: the job was deleted without running',
            $this->count('m1', 'event_before') === 1 && $this->count('m1', 'started') === 0);
        $this->check('with no failed-job row', !isset($this->failedIds()['m1']));
        $this->check('without it, the job failed with a ModelNotFoundException',
            $this->count('m2', 'started') === 0 && str_ends_with($exception, 'ModelNotFoundException'), $exception);
    }

    private function batchAllowFailures(): void
    {
        $run = $this->run;
        $batch = $this->withBatchCallbacks(Bus::batch([
            new CompatJob($run, 'a0', 'ok', 200),
            new CompatJob($run, 'a1', 'throw'),
            // Starts after the failure: on a batch that does not allow
            // failures it would find the batch cancelled.
            (new CompatJob($run, 'a2'))->delay(3),
        ])->allowFailures())->onConnection($this->connection)->onQueue($this->queue)->dispatch();
        $this->waitFor(fn (): bool => $this->count('batch', 'batch_finally') > 0);
        $this->settle(3);
        $found = Bus::findBatch($batch->id);
        // Laravel docs, Queues > Job Batching > Batch Failures > Allowing Failures.
        $this->check('a job that started after the failure still ran', $this->count('a2', 'completed') === 1
            && $this->count('a2', 'skipped_cancelled_batch') === 0, implode(',', array_column($this->eventsOf('a2'), 'event')));
        $this->check('the batch is not cancelled, and counts one failure', $found !== null && !$found->cancelled() && $found->failedJobs === 1,
            $found === null ? 'not found' : 'cancelled ' . json_encode($found->cancelled()) . ", failed {$found->failedJobs}");
        // Laravel docs, Queues > Job Batching > Dispatching Batches: then() needs every job to succeed.
        $this->check('catch() ran once', $this->count('batch', 'batch_catch') === 1);
        $this->check('then() never ran', $this->count('batch', 'batch_then') === 0);
        $this->check('finally() ran once', $this->count('batch', 'batch_finally') === 1);
        $this->sameJobs(['a0', 'a1', 'a2']);
        $this->same('batch then(), catch(), finally()', array_map(fn (string $event): int => $this->count('batch', $event),
            ['batch_then', 'batch_catch', 'batch_finally']));
        $this->same('batch state', $this->batchState($batch->id));
    }

    private function batchCancel(): void
    {
        $run = $this->run;
        $batch = Bus::batch([
            new CompatBatchCancellerJob($run, 'x0'),
            // Later, so that they start once the batch is cancelled.
            (new CompatJob($run, 'x1'))->delay(4),
            (new CompatSkipIfCancelledJob($run, 'x2'))->delay(4),
        ])->onConnection($this->connection)->onQueue($this->queue)->dispatch();
        $this->waitFor(fn (): bool => $this->count('x1', 'event_after') > 0 && $this->count('x2', 'event_after') > 0);
        $this->settle(2);
        $found = Bus::findBatch($batch->id);
        // Laravel docs, Queues > Job Batching > Cancelling Batches.
        $this->check('a job cancelled its own batch', $this->count('x0', 'cancelled_batch') === 1 && $found?->cancelled() === true,
            $found === null ? 'not found' : 'cancelled ' . json_encode($found->cancelled()));
        $this->check('a later job saw $this->batch()->cancelled() and skipped its work',
            $this->count('x1', 'skipped_cancelled_batch') === 1 && $this->count('x1', 'started') === 0,
            implode(',', array_column($this->eventsOf('x1'), 'event')));
        // Laravel docs, Queues > Job Batching > Cancelling Batches: the SkipIfBatchCancelled middleware.
        $this->check('SkipIfBatchCancelled kept a later job from running',
            $this->count('x2', 'event_before') === 1 && $this->count('x2', 'skipped_cancelled_batch') === 0 && $this->count('x2', 'started') === 0,
            implode(',', array_column($this->eventsOf('x2'), 'event')));
    }

    private function retryBatch(): void
    {
        $run = $this->run;
        $batch = $this->withBatchCallbacks(Bus::batch([
            new CompatJob($run, 'y0', 'ok', 200),
            new CompatJob($run, 'y1', 'fail-once'),
        ])->allowFailures())->onConnection($this->connection)->onQueue($this->queue)->dispatch();
        $this->waitFor(fn (): bool => isset($this->failedIds()['y1']) && $this->count('batch', 'batch_finally') > 0);
        $this->observed['before_retry'] = $this->batchState($batch->id);
        // Laravel docs, Queues > Job Batching > Retrying Failed Batch Jobs.
        $exit = Artisan::call('queue:retry-batch', ['id' => [$batch->id]]);
        $this->observed['retry_batch'] = ['exit' => $exit, 'output' => trim(Artisan::output())];
        $this->waitFor(fn (): bool => $this->count('y1', 'completed') > 0);
        $this->settle(3);
        $after = Bus::findBatch($batch->id);
        $this->observed['after_retry'] = $this->batchState($batch->id);
        $this->check('queue:retry-batch ran the failed job again, and it succeeded',
            $this->count('y1', 'started') === 2 && $this->count('y1', 'completed') === 1,
            $this->count('y1', 'started') . ' runs');
        $this->check('its failed-job row is gone', !isset($this->failedIds()['y1']));
        $this->check('the batch has no pending job left, and is finished', $after !== null && $after->pendingJobs === 0 && $after->finished(),
            json_encode($this->observed['after_retry']));
        $this->check('then() ran once, after the retry', $this->count('batch', 'batch_then') === 1);
    }

    private function delayDatetime(): void
    {
        $this->mark('dispatched');
        CompatJob::dispatch($this->run, 'd1')->onConnection($this->connection)->onQueue($this->queue)->delay(now()->addSeconds(5));
        $this->waitFor(fn (): bool => $this->count('d1', 'completed') > 0);
        $waited = $this->first('d1', 'started') - $this->first('command', 'dispatched');
        $this->observed['waited_seconds'] = round($waited, 2);
        // Laravel docs, Queues > Dispatching Jobs > Delayed Dispatching. Laravel's
        // Redis queue keeps due times in whole seconds, so allow 1 s early.
        $this->check('a delay given as a DateTimeInterface is respected', $waited >= 4.0 && $waited <= 30, round($waited, 2) . ' s');
    }

    private function releaseDelay(): void
    {
        CompatReleaseJob::dispatch($this->run, 'r1', 'ok', 0, 3)->onConnection($this->connection)->onQueue($this->queue);
        $this->waitFor(fn (): bool => $this->count('r1', 'completed') > 0 || $this->count('r1', 'failed_hook') > 0);
        $this->settle(2);
        $gap = ($this->times('r1', 'started')[1] ?? 0) - $this->first('r1', 'released');
        $this->observed['gap_seconds'] = round($gap, 2);
        $this->observed['attempts'] = array_column(array_filter($this->eventsOf('r1'), static fn (array $e): bool => $e['event'] === 'started'), 'attempt');
        // Laravel docs, Queues > Manually Releasing a Job: release(5) makes it available again after 5 s.
        // Laravel's Redis queue keeps due times in whole seconds, so allow 1 s early.
        $this->check('release(5) in handle() held the job back for 5 s', $gap >= 4.0 && $gap <= 20, round($gap, 2) . ' s');
        $this->check('then it ran again and completed, with no failure',
            $this->count('r1', 'started') === 2 && $this->count('r1', 'completed') === 1 && $this->count('r1', 'failed_hook') === 0
            && !isset($this->failedIds()['r1']), $this->count('r1', 'started') . ' runs');
        // The release counts as an attempt: the second run is attempt 2.
        $this->sameJobs(['r1']);
        $this->near('r1 release(5) delay', $gap, 1.5);
    }

    private function queueMonitor(): void
    {
        $busy = [];
        Event::listen(QueueBusy::class, static function (QueueBusy $event) use (&$busy): void {
            $busy[] = "{$event->connection}:{$event->queue} size {$event->size}";
        });
        $this->pauseWorkers();
        try {
            foreach (['w1', 'w2', 'w3'] as $job) {
                CompatJob::dispatch($this->run, $job)->onConnection($this->connection)->onQueue($this->queue);
            }
            sleep(1);
            // Laravel docs, Queues > Monitoring Your Queues.
            Artisan::call('queue:monitor', ['queues' => "{$this->connection}:{$this->queue}", '--max' => 3, '--json' => true]);
            $report = json_decode(trim(Artisan::output()), true);
        } finally {
            $this->resumeWorkers();
        }
        $this->observed['monitor'] = $report;
        $this->observed['queue_busy'] = $busy;
        $this->waitFor(fn (): bool => $this->completedEach(['w1', 'w2', 'w3']));
        $size = is_array($report) ? ($report[0]['size'] ?? null) : null;
        // At least three: a job another scenario left delayed counts too.
        $this->check('queue:monitor --json reported the three waiting jobs', is_int($size) && $size >= 3, 'size ' . json_encode($size));
        $this->check('and fired QueueBusy, the size being at --max', count($busy) === 1, implode('; ', $busy));
    }

    private function forkInJob(): void
    {
        $jobs = ['f1', 'f2', 'f3'];
        foreach ($jobs as $job) {
            CompatForkJob::dispatch($this->run, $job, 'ok', 0, 3)->onConnection($this->connection)->onQueue($this->queue);
        }
        try {
            $this->waitFor(fn (): bool => $this->completedEach($jobs), 30);
        } catch (RuntimeException) {
            // Judged on what happened in 30 s: a killed worker's lease must expire before a retry.
            $this->observed['completed_within_30_seconds'] = false;
        }
        $this->settle(3);
        $workers = array_map(fn (string $job): array => $this->pidsOf($job, ['started', 'child_exited', 'completed']), array_combine($jobs, $jobs));
        $children = array_map(fn (string $job): array => $this->pidsOf($job, ['child_ran']), array_combine($jobs, $jobs));
        $this->observed['worker_pids'] = $workers;
        $this->observed['child_pids'] = $children;
        // Not a Laravel feature: a guard for the supervisors. A job may fork, and the
        // child's normal exit must not stop the parent's worker or its lease renewal.
        $this->check('every job completed once', $this->completedEach($jobs),
            implode(', ', array_map(fn (string $job): string => "{$job}: " . $this->count($job, 'completed'), $jobs)));
        $this->check('with one attempt each: no second start',
            array_filter($jobs, fn (string $job): bool => $this->count($job, 'started') !== 1) === [],
            implode(', ', array_map(fn (string $job): string => "{$job}: " . $this->count($job, 'started'), $jobs)));
        $this->check('in one worker, still alive after the job: never killed',
            array_filter($workers, static fn (array $pids): bool => count($pids) !== 1 || !posix_kill($pids[0], 0)) === [],
            (string) json_encode($workers));
        $this->check('each child ran in a process of its own, and exited 0',
            array_filter($jobs, fn (string $job): bool => count($children[$job]) !== 1
                || array_intersect($children[$job], $workers[$job]) !== [] || $this->count($job, 'child_exited') !== 1) === [],
            (string) json_encode($children));
    }

        private function pruneFailed(): void
    {
        CompatJob::dispatch($this->run, 'z1', 'throw')->onConnection($this->connection)->onQueue($this->queue);
        $this->waitFor(fn (): bool => isset($this->failedIds()['z1']));
        // Failure times are whole seconds: make the row older than "now".
        sleep(2);
        // Laravel docs, Queues > Dealing With Failed Jobs > Pruning Failed Jobs.
        Artisan::call('queue:prune-failed', ['--hours' => 1]);
        $this->observed['hours_1'] = trim(Artisan::output());
        $kept = isset($this->failedIds()['z1']);
        Artisan::call('queue:prune-failed', ['--hours' => 0]);
        $this->observed['hours_0'] = trim(Artisan::output());
        $this->check('--hours=1 kept the row of a job that failed seconds ago', $kept, $this->observed['hours_1']);
        $this->check('--hours=0 pruned it', !isset($this->failedIds()['z1']), $this->observed['hours_0']);
    }

    // -------------------------------------------------------------- helpers

    /**
     * @param list<string> $events
     * @return list<int> the processes that logged these events of the job
     */
    private function pidsOf(string $job, array $events): array
    {
        return array_values(array_unique(array_map('intval', array_column(array_filter($this->eventsOf($job),
            static fn (array $e): bool => in_array($e['event'], $events, true)), 'pid'))));
    }

    /** @return array<string, mixed>|null */
    private function batchState(string $id): ?array
    {
        $batch = Bus::findBatch($id);

        return $batch === null ? null : [
            'pending' => $batch->pendingJobs,
            'failed' => $batch->failedJobs,
            'failed_job_ids' => count($batch->failedJobIds),
            'finished' => $batch->finished(),
            'cancelled' => $batch->cancelled(),
        ];
    }
}
