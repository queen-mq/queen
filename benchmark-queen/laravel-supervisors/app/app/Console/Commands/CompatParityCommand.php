<?php

namespace App\Console\Commands;

use App\Jobs\Compat\CompatDeadlineJob;
use App\Jobs\Compat\CompatMinuteLimitedJob;
use App\Jobs\Compat\CompatOverlapJob;
use App\Jobs\Compat\CompatScriptedJob;
use App\Support\FailureMatrixLog;

/**
 * How Laravel counts attempts, where the other commands only check that a
 * feature works: a release is an attempt, also a middleware's release; tries
 * and maxExceptions end a job; retryUntil() is fixed at dispatch and outranks
 * tries. Each scenario records the attempts of every run and pickup, so
 * failure_matrix.py can hold Queen to what Horizon does with the same job.
 */
final class CompatParityCommand extends CompatScenarioCommand
{
    public const SCENARIOS = [
        'release-attempts', 'retry-until-precedence', 'overlap-attempts', 'rate-limited-attempts',
    ];

    protected $signature = 'bench:compat-parity {scenario : one of the scenarios}
        {--run-id= : Run identifier}
        {--timeout=90 : Seconds to wait for the workers}';

    protected $description = 'Check how one Laravel retry rule counts attempts against the running workers';

    public function handle(FailureMatrixLog $log): int
    {
        return $this->runScenario($log, self::SCENARIOS, fn (string $scenario) => $this->{$scenario}());
    }

    // ------------------------------------------------------------ scenarios

    private function releaseAttempts(): void
    {
        // Laravel docs, Queues > Max Job Attempts: releasing a job counts as an
        // attempt. j1 is released on each run until tries runs out; j2
        // alternates release and exception, and maxExceptions (2) ends it, not
        // tries (10); j3 is released once and completes on its second attempt.
        $this->scripted('j1', 'RRRRR', tries: 3);
        $this->scripted('j2', 'RTRTRT', tries: 10, maxExceptions: 2);
        $this->scripted('j3', 'R', tries: 2);
        $this->waitFor(fn (): bool => $this->count('j1', 'failed_hook') > 0 && $this->count('j2', 'failed_hook') > 0
            && $this->count('j3', 'completed') > 0);
        $this->settle(3);
        $this->check('j1: three runs, each released, then failed on the fourth pickup',
            $this->attemptsOf('j1', 'started') === [1, 2, 3] && $this->attemptsOf('j1', 'event_before') === [1, 2, 3, 4]
            && str_ends_with((string) $this->exceptionOf('j1'), 'MaxAttemptsExceededException'),
            $this->trail('j1'));
        $this->check('j2: failed at its second exception, the fourth run',
            $this->attemptsOf('j2', 'started') === [1, 2, 3, 4] && $this->count('j2', 'failed_hook') === 1
            && $this->exceptionOf('j2') === 'RuntimeException', $this->trail('j2'));
        $this->check('j3: completed on its second attempt', $this->attemptsOf('j3', 'completed') === [2], $this->trail('j3'));
        $failed = $this->failedIds();
        $this->check('one failed-job row for j1 and j2, none for j3',
            isset($failed['j1'], $failed['j2']) && !isset($failed['j3']), implode(',', array_keys($failed)));
        $this->sameJobs(['j1', 'j2', 'j3']);
        foreach (['j1', 'j2', 'j3'] as $job) {
            $starts = $this->times($job, 'started');
            $released = $this->times($job, 'released');
            if (isset($starts[1], $released[0])) {
                $this->near("{$job} release(1) delay", $starts[1] - $released[0], self::DELAY_TOLERANCE);
            }
        }
    }

    private function retryUntilPrecedence(): void
    {
        // Laravel docs, Queues > Time Based Attempts: with retryUntil(), tries
        // is not used. The deadline is computed when the job is dispatched.
        $dispatched = time();
        CompatDeadlineJob::dispatch($this->run, 'd1', 'deadline', 0, 2)->onConnection($this->connection)->onQueue($this->queue);
        $this->waitFor(fn (): bool => $this->count('d1', 'failed_hook') > 0 || $this->count('d1', 'completed') > 0, 120);
        $this->settle(3);
        $runs = $this->attemptsOf('d1', 'started');
        $deadlines = array_values(array_unique(array_map(static fn (array $e): mixed => $e['detail']['retry_until'] ?? null,
            array_filter($this->eventsOf('d1'), static fn (array $e): bool => $e['event'] === 'started'))));
        $span = ($this->first('d1', 'failed_hook') ?: $this->first('d1', 'completed')) - $this->first('d1', 'started');
        $this->observed += ['runs' => $runs, 'deadlines_seen' => $deadlines, 'dispatched_at' => $dispatched, 'span_seconds' => round($span, 2)];
        $this->check('the deadline was fixed at dispatch: every run saw one timestamp, 6 s after the dispatch',
            count($deadlines) === 1 && is_int($deadlines[0]) && abs($deadlines[0] - $dispatched - 6) <= 1, json_encode($deadlines));
        $this->check('retried past tries (2) until the deadline', count($runs) > 2, count($runs) . ' runs');
        $this->check('then failed once, never completed, with a failed-job row',
            $this->count('d1', 'failed_hook') === 1 && $this->count('d1', 'completed') === 0 && isset($this->failedIds()['d1']));
        // How many runs fit before the deadline is the workers' pace.
        $this->same('d1 ran more than tries', count($runs) > 2);
        $this->same('d1 failed(), completed, failed-job row',
            [$this->count('d1', 'failed_hook'), $this->count('d1', 'completed'), isset($this->failedIds()['d1'])]);
        $this->same('d1 saw one deadline', count($deadlines));
        $this->near('d1 deadline after the dispatch', is_int($deadlines[0] ?? null) ? $deadlines[0] - $dispatched : -1, 1.0);
        $this->near('d1 failed after its first run', $span, 2.0);
    }

    private function overlapAttempts(): void
    {
        // Laravel docs, Queues > Job Middleware > Preventing Job Overlaps: each
        // release by WithoutOverlapping counts as an attempt. o1 holds the lock
        // for 15 s; o2 (tries 3) is released each second meanwhile, so its
        // fourth pickup exceeds tries and fails before it ever runs. o2 goes to
        // the pool's next queue: a pool balanced by backlog keeps a worker
        // there, while the first queue's may be o1's alone.
        CompatOverlapJob::dispatch($this->run, 'o1', 'ok', 15_000, 1)->onConnection($this->connection)->onQueue($this->queue);
        $this->waitFor(fn (): bool => $this->count('o1', 'started') > 0);
        CompatOverlapJob::dispatch($this->run, 'o2', 'ok', 0, 3)->onConnection($this->connection)->onQueue($this->nextQueue());
        $this->waitFor(fn (): bool => $this->count('o1', 'completed') > 0
            && ($this->count('o2', 'failed_hook') > 0 || $this->count('o2', 'completed') > 0));
        $this->settle(3);
        $this->check('o1 ran once', $this->attemptsOf('o1', 'completed') === [1], $this->trail('o1'));
        $this->check('o2: released at three pickups, failed at the fourth, never ran',
            $this->attemptsOf('o2', 'event_before') === [1, 2, 3, 4] && $this->count('o2', 'started') === 0
            && str_ends_with((string) $this->exceptionOf('o2'), 'MaxAttemptsExceededException'), $this->trail('o2'));
        $this->check('o2 has a failed-job row', isset($this->failedIds()['o2']));
        $this->sameJobs(['o1', 'o2']);
    }

    private function rateLimitedAttempts(): void
    {
        // Laravel docs, Queues > Job Middleware > Rate Limiting: each release by
        // RateLimited counts as an attempt. One job per minute passes; the
        // other two (tries 3) are released each second until they fail.
        // RateLimited checks the count and then adds to it: two workers that
        // pick two jobs at the same moment can both pass, on every engine. So
        // q1 has the minute to itself before q2 and q3 are dispatched.
        $jobs = ['q1', 'q2', 'q3'];
        $dispatch = fn (string $job) => CompatMinuteLimitedJob::dispatch($this->run, $job, 'ok', 0, 3)
            ->onConnection($this->connection)->onQueue($this->queue);
        $dispatch('q1');
        $this->waitFor(fn (): bool => $this->count('q1', 'completed') > 0 || $this->count('q1', 'failed_hook') > 0);
        $dispatch('q2');
        $dispatch('q3');
        $this->waitFor(fn (): bool => count(array_filter($jobs, fn (string $j): bool => $this->count($j, 'completed') > 0
            || $this->count($j, 'failed_hook') > 0)) === 3);
        $this->settle(3);
        $passed = array_values(array_filter($jobs, fn (string $j): bool => $this->count($j, 'completed') > 0));
        $limited = array_values(array_diff($jobs, $passed));
        $failed = $this->failedIds();
        $this->observed += ['passed' => $passed, 'limited' => $limited];
        $this->check('one job passed the limiter, on its first attempt',
            count($passed) === 1 && $this->attemptsOf($passed[0], 'completed') === [1], implode(',', $passed));
        $this->check('the other two were released at three pickups and failed at the fourth',
            count($limited) === 2 && array_filter($limited, fn (string $j): bool => $this->attemptsOf($j, 'event_before') !== [1, 2, 3, 4]
                || $this->count($j, 'started') > 0 || !str_ends_with((string) $this->exceptionOf($j), 'MaxAttemptsExceededException')) === [],
            implode('; ', array_map(fn (string $j): string => $this->trail($j), $limited)));
        $this->check('with one failed-job row each', count(array_intersect($limited, array_keys($failed))) === 2,
            implode(',', array_keys($failed)));
        // Compare the shapes, as the other items do.
        $this->same('the job that passed', $passed === [] ? null : $this->summaryOf($passed[0]));
        $this->same('the jobs that were limited', array_map(fn (string $j): array => $this->summaryOf($j), $limited));
    }

    // -------------------------------------------------------------- helpers

    /** The queue after this command's in its pool, or this one when the pool has no other. */
    private function nextQueue(): string
    {
        $queues = (array) config('benchmark.queues');
        foreach (config('benchmark.routed') ? (array) config('benchmark.routed_pools') : [] as $pool) {
            if (in_array($this->queue, $pool['queues'], true)) {
                $queues = $pool['queues'];
            }
        }
        $at = array_search($this->queue, $queues, true);

        return $at === false ? $this->queue : (string) ($queues[$at + 1] ?? $this->queue);
    }

    private function scripted(string $job, string $script, int $tries, ?int $maxExceptions = null): void
    {
        dispatch(CompatScriptedJob::make($this->run, $job, $script, $tries, $maxExceptions)
            ->onConnection($this->connection)->onQueue($this->queue));
    }

    /** @return array<string, mixed> a job's attempts, without its id */
    private function summaryOf(string $job): array
    {
        return [
            'runs' => $this->attemptsOf($job, 'started'),
            'pickups' => $this->attemptsOf($job, 'event_before'),
            'completed' => $this->attemptsOf($job, 'completed'),
            'failed_with' => $this->exceptionOf($job),
            'failed_row' => isset($this->failedIds()[$job]),
        ];
    }

    private function trail(string $job): string
    {
        return implode(',', array_map(static fn (array $e): string => $e['event'] . '@' . ($e['attempt'] ?? '-'), $this->eventsOf($job)));
    }
}
