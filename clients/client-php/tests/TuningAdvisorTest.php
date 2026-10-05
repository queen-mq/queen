<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Dashboard\TuningAdvisor;

final class TuningAdvisorTest extends TestCase
{
    public function testAWellTunedApplicationGetsNoAdvice(): void
    {
        $this->assertSame([], $this->advise());
    }

    public function testAJobLongerThanTheShutdownGraceIsInterruptedByDeploys(): void
    {
        // The running supervisor's grace wins over this host's configuration.
        $advice = $this->advise(
            config: $this->config(['supervisor' => ['shutdown_grace' => 600]]),
            metrics: $this->metrics([['class' => 'App\Jobs\Export', 'max_ms' => 130_000, 'average_ms' => 9_000]]),
        );

        $this->assertCount(1, $advice);
        $this->assertSame('warning', $advice[0]['severity']);
        $this->assertSame('Deploys interrupt App\Jobs\Export', $advice[0]['title']);
        $this->assertStringContainsString('2 min 10 s', $advice[0]['evidence']);
        $this->assertStringContainsString('75 s', $advice[0]['evidence']);
        $this->assertStringContainsString('Raise queen.supervisor.shutdown_grace above the longest run', $advice[0]['action']);
        $this->assertStringContainsString('terminationGracePeriodSeconds', $advice[0]['action']);
        $this->assertSame('https://queenmq.com/guides/laravel/supervisors', $advice[0]['doc']);

        $unpublished = $this->advise(
            snapshot: $this->snapshot(['configuration' => ['supervisors' => []]]),
            metrics: $this->metrics([['class' => 'App\Jobs\Export', 'max_ms' => 80_000]]),
        );
        $this->assertSame(['Deploys interrupt App\Jobs\Export'], array_column($unpublished, 'title'), "this host's 75 s without a running supervisor");
    }

    public function testJobsWithinTheShutdownGraceOrNeverMeasuredAreNotFlagged(): void
    {
        $this->assertSame([], $this->advise(metrics: $this->metrics([
            ['class' => 'App\Jobs\Export', 'max_ms' => 75_000, 'average_ms' => 9_000],
            // Workers of an earlier release record no longest run.
            ['class' => 'App\Jobs\Legacy', 'max_ms' => null, 'average_ms' => 200],
        ])));
        $this->assertSame([], $this->advise(metrics: ['available' => false, 'classes' => [['class' => 'App\Jobs\Export', 'max_ms' => 999_000]]]));
    }

    public function testOneWorkerForSeveralQueuesWithABacklogIsFlagged(): void
    {
        $advice = $this->advise(snapshot: $this->snapshot([
            'configuration' => ['shutdown_grace' => 75, 'poll_interval' => 3, 'supervisors' => [
                $this->publishedPool(['queues' => ['high', 'low'], 'balance' => 'off', 'processes' => 1, 'max_processes' => 1]),
            ]],
            'queues' => [$this->depth('high', 0), $this->depth('low', 42)],
        ]));

        $this->assertSame(['warning'], array_column($advice, 'severity'));
        $this->assertSame('Pool default has one worker for 2 queues', $advice[0]['title']);
        $this->assertStringContainsString('low has 42 jobs waiting', $advice[0]['evidence']);
        $this->assertStringContainsString('min_processes and max_processes in queen.supervisor.supervisors.default', $advice[0]['action']);
        $this->assertStringContainsString('its own pool', $advice[0]['action']);
    }

    public function testOneWorkerWithoutABacklogOrSeveralWorkersAreFine(): void
    {
        $pool = ['queues' => ['high', 'low'], 'balance' => 'off', 'processes' => 1, 'max_processes' => 1];
        $this->assertSame([], $this->advise(snapshot: $this->snapshot([
            'configuration' => ['shutdown_grace' => 75, 'poll_interval' => 3, 'supervisors' => [$this->publishedPool($pool)]],
            'queues' => [$this->depth('high', 0), $this->depth('low', 0)],
        ])));
        $this->assertSame([], $this->advise(snapshot: $this->snapshot([
            'configuration' => ['shutdown_grace' => 75, 'poll_interval' => 3, 'supervisors' => [$this->publishedPool([...$pool, 'processes' => 2, 'max_processes' => 2])]],
            'queues' => [$this->depth('high', 0), $this->depth('low', 42)],
        ])));
    }

    public function testShortJobsWithPrefetchOneAreToldAboutPrefetch(): void
    {
        $advice = $this->advise(metrics: $this->metrics([
            ['class' => 'App\Jobs\Ping', 'average_ms' => 12, 'max_ms' => 40, 'processed' => 900],
            ['class' => 'App\Jobs\Report', 'average_ms' => 4_000, 'max_ms' => 9_000],
        ]));

        $this->assertSame(['info'], array_column($advice, 'severity'));
        $this->assertStringContainsString('App\Jobs\Ping', $advice[0]['evidence']);
        $this->assertStringContainsString('12 ms', $advice[0]['evidence']);
        $this->assertStringNotContainsString('App\Jobs\Report', $advice[0]['evidence']);
        foreach (['prefetch 4 with ack_async and pop_ahead true', 'config/queen.php', 'queen connection in config/queue.php', 'need lease_renewal true'] as $setting) {
            $this->assertStringContainsString($setting, $advice[0]['action']);
        }
        $this->assertStringEndsWith(
            'Keep tries at 2 or more: after a crash that is not handed back, such as a lost node, each job a worker had prefetched comes back with one attempt more.',
            $advice[0]['action'],
        );
        $this->assertSame('https://queenmq.com/guides/laravel#a-faster-profile', $advice[0]['doc']);
    }

    public function testPrefetchAdviceIsAWarningForAPoolWithOneTry(): void
    {
        $advice = $this->advise(
            snapshot: $this->snapshot(['configuration' => ['shutdown_grace' => 75, 'poll_interval' => 3, 'supervisors' => [
                $this->publishedPool(['tries' => 1]),
            ]]]),
            metrics: $this->metrics([['class' => 'App\Jobs\Ping', 'average_ms' => 12, 'max_ms' => 40, 'processed' => 900]]),
        );

        $this->assertSame(['warning'], array_column($advice, 'severity'));
        $this->assertStringContainsString('Pool default has tries 1', $advice[0]['evidence']);
    }

    public function testLongerJobsOrAPrefetchAboveOneAreNotToldAboutPrefetch(): void
    {
        $short = $this->metrics([['class' => 'App\Jobs\Ping', 'average_ms' => 12, 'max_ms' => 40, 'processed' => 900]]);
        $this->assertSame([], $this->advise(config: $this->config([], ['prefetch' => 4, 'lease_renewal' => true]), metrics: $short));
        $this->assertSame([], $this->advise(metrics: $this->metrics([['class' => 'App\Jobs\Ping', 'average_ms' => 100, 'max_ms' => 300, 'processed' => 900]])));
    }

    public function testRareShortJobsOrAPopAlreadySentAheadAreNotToldAboutPrefetch(): void
    {
        // Fewer than one run a minute over the hour gains nothing worth a change.
        $this->assertSame([], $this->advise(metrics: $this->metrics([['class' => 'App\Jobs\Ping', 'average_ms' => 5, 'max_ms' => 9, 'processed' => 59]])));
        // pop_ahead already hides the pop behind the running job.
        $this->assertSame([], $this->advise(
            config: $this->config([], ['pop_ahead' => true, 'lease_renewal' => true]),
            metrics: $this->metrics([['class' => 'App\Jobs\Ping', 'average_ms' => 5, 'max_ms' => 9, 'processed' => 900]]),
        ));
    }

    public function testAPoolWithOneTryOnAPrefetchingConnectionIsWarned(): void
    {
        $advice = $this->advise(
            config: $this->config([], ['prefetch' => 4, 'pop_ahead' => true, 'lease_renewal' => true]),
            snapshot: $this->snapshot(['configuration' => ['shutdown_grace' => 75, 'poll_interval' => 3, 'supervisors' => [
                $this->publishedPool(['tries' => 1]),
                $this->publishedPool(['name' => 'reports', 'tries' => 2]),
            ]]]),
        );

        $this->assertSame(['warning'], array_column($advice, 'severity'));
        $this->assertSame('Pool default can fail a job that never ran', $advice[0]['title']);
        $this->assertStringContainsString(
            'its connection queen prefetches 4 jobs and pops the next batch ahead',
            $advice[0]['evidence'],
        );
        $this->assertStringContainsString('Set tries to 2 or more for pool default', $advice[0]['action']);
    }

    public function testOneTryWithoutPrefetchOrPopAheadIsFine(): void
    {
        $snapshot = $this->snapshot(['configuration' => ['shutdown_grace' => 75, 'poll_interval' => 3, 'supervisors' => [
            $this->publishedPool(['tries' => 1]),
        ]]]);

        $this->assertSame([], $this->advise(snapshot: $snapshot));
        $popAhead = $this->advise(config: $this->config([], ['pop_ahead' => true, 'lease_renewal' => true]), snapshot: $snapshot);
        $this->assertStringContainsString('its connection queen pops the next batch ahead.', $popAhead[0]['evidence']);
    }

    public function testASupervisorWarnsAtStartFromTheConfigurationAlone(): void
    {
        $config = $this->config(['supervisor' => ['supervisors' => ['default' => ['tries' => 1]]]], ['prefetch' => 4, 'lease_renewal' => true]);

        $warnings = TuningAdvisor::startupWarnings($config);

        $this->assertCount(1, $warnings);
        $this->assertStringStartsWith('Pool default can fail a job that never ran. It has tries 1', $warnings[0]);
        $this->assertStringContainsString('for job classes that set $tries = 1', $warnings[0]);
        $this->assertSame([], TuningAdvisor::startupWarnings($this->config()));
    }

    public function testWorkersThatBootLaravelOneByOneAreToldAboutPrefork(): void
    {
        $advice = $this->advise(config: $this->config(['supervisor' => ['prefork' => false]]));

        $this->assertSame(['info'], array_column($advice, 'severity'));
        $this->assertStringContainsString('2,062 MiB', $advice[0]['evidence']);
        $this->assertStringContainsString('225 MiB', $advice[0]['evidence']);
        $this->assertStringContainsString('QUEEN_SUPERVISOR_PREFORK=true', $advice[0]['action']);
        $this->assertSame('https://queenmq.com/guides/laravel/supervisors#prefork-workers', $advice[0]['doc']);
        $this->assertSame([], $this->advise(config: $this->config(['supervisor' => ['prefork' => true]])));
    }

    public function testALeaseHelperPerWorkerIsFlaggedOnlyWhenTheMastersServiceIsTurnedOff(): void
    {
        $renewing = $this->config(['supervisor' => ['lease_service' => false]], ['lease_renewal' => true]);
        $advice = $this->advise(config: $renewing);

        $this->assertSame(['info'], array_column($advice, 'severity'));
        $this->assertStringContainsString('queen.supervisor.lease_service turns', $advice[0]['evidence']);
        $this->assertStringContainsString('Set queen.supervisor.lease_service to true', $advice[0]['action']);
        $this->assertSame('https://queenmq.com/guides/laravel/supervisors#lease-renewal-in-the-master', $advice[0]['doc']);

        $this->assertSame([], $this->advise(config: $this->config([], ['lease_renewal' => true])), 'the service is on by default');
        $this->assertSame([], $this->advise(config: $this->config(['supervisor' => ['lease_service' => true]], ['lease_renewal' => true])));
        $this->assertSame([], $this->advise(config: $this->config(['supervisor' => ['lease_service' => 'false']], ['lease_renewal' => true])), 'the supervisor refuses a string');
        $this->assertSame([], $this->advise(config: $this->config(['supervisor' => ['lease_service' => false]])), 'no renewal, no helpers');
    }

    public function testAutoscalingThatWaitsForThePollIsToldAboutEventDrivenScaling(): void
    {
        $advice = $this->advise(config: $this->config(['supervisor' => ['event_driven' => false]]));

        $this->assertSame(['info'], array_column($advice, 'severity'));
        $this->assertSame('Bursts wait for the next poll', $advice[0]['title']);
        $this->assertStringContainsString('every 3 s', $advice[0]['evidence']);
        $this->assertStringContainsString('Set queen.supervisor.event_driven to true', $advice[0]['action']);
        $this->assertSame('https://queenmq.com/guides/laravel/supervisors#event-driven-scaling', $advice[0]['doc']);
    }

    public function testFixedPoolsDoNotWaitForThePoll(): void
    {
        // The running supervisor's pools win over this host's auto pool.
        $this->assertSame([], $this->advise(
            config: $this->config(['supervisor' => ['event_driven' => false]]),
            snapshot: $this->snapshot(['configuration' => ['shutdown_grace' => 75, 'poll_interval' => 3, 'supervisors' => [
                $this->publishedPool(['balance' => 'simple']),
            ]]]),
        ));
    }

    public function testAPoolWhoseLeaseEndsBeforeItsTimeoutCanRunAJobTwice(): void
    {
        $advice = $this->advise(config: $this->config(['supervisor' => ['supervisors' => ['default' => ['timeout' => 120]]]]));

        $this->assertSame(['critical'], array_column($advice, 'severity'));
        $this->assertSame('Pool default can run a job twice at once', $advice[0]['title']);
        $this->assertStringContainsString('120 s', $advice[0]['evidence']);
        $this->assertStringContainsString('90 s', $advice[0]['evidence']);
        $this->assertStringContainsString('Raise queen.supervisor.supervisors.default.retry_after above the timeout', $advice[0]['action']);

        $equal = $this->advise(config: $this->config(['supervisor' => ['supervisors' => ['default' => ['timeout' => 90]]]]));
        $this->assertSame(['critical'], array_column($equal, 'severity'), 'a lease as long as the timeout is too short');
    }

    public function testLeaseRenewalKeepsAJobFromRunningTwiceButTheSupervisorStillRefusesThePool(): void
    {
        $advice = $this->advise(config: $this->config(['supervisor' => ['supervisors' => ['default' => ['timeout' => 120]]]], ['lease_renewal' => true]));

        $this->assertSame(['critical'], array_column($advice, 'severity'));
        $this->assertSame('The supervisor does not start with pool default', $advice[0]['title']);
        $this->assertStringContainsString('even with lease renewal', $advice[0]['evidence']);
        $this->assertStringContainsString('Raise queen.supervisor.supervisors.default.retry_after above the timeout', $advice[0]['action']);
    }

    public function testALeaseLongerThanTheTimeoutIsFine(): void
    {
        $this->assertSame([], $this->advise(config: $this->config(['supervisor' => ['supervisors' => ['default' => ['timeout' => 120, 'retry_after' => 150]]]])));
    }

    public function testSupervisorsThatShareAQueueWithoutCoordinatingAreFlagged(): void
    {
        $advice = $this->advise(snapshot: $this->snapshot(['shared_queues' => [
            ['connection' => 'queen', 'consumer_group' => 'laravel', 'queue' => 'default', 'instances' => 3],
        ]]));

        $this->assertSame(['warning'], array_column($advice, 'severity'));
        $this->assertSame('3 supervisors autoscale queue default without coordinating', $advice[0]['title']);
        $this->assertStringContainsString('QUEEN_SUPERVISOR_COORDINATION=true', $advice[0]['action']);
        $this->assertSame('https://queenmq.com/guides/laravel/supervisors#several-replicas', $advice[0]['doc']);
    }

    public function testFailedJobsLinkToTheFailedJobsPage(): void
    {
        $advice = $this->advise(snapshot: $this->snapshot(['failed_jobs' => ['available' => true, 'total' => 51, 'total_exact' => false]]));

        $this->assertSame(['info'], array_column($advice, 'severity'));
        $this->assertSame('51+ failed jobs', $advice[0]['title']);
        $this->assertSame('failed-jobs', $advice[0]['page']);

        $this->assertSame([], $this->advise(snapshot: $this->snapshot(['failed_jobs' => ['available' => false, 'total' => null, 'total_exact' => false]])));
    }

    public function testAdviceIsSortedMostSevereFirst(): void
    {
        $advice = $this->advise(
            config: $this->config(['supervisor' => ['prefork' => false, 'supervisors' => ['default' => ['timeout' => 120]]]]),
            snapshot: $this->snapshot(['shared_queues' => [['connection' => 'queen', 'consumer_group' => 'laravel', 'queue' => 'default', 'instances' => 2]]]),
        );

        $this->assertSame(['critical', 'warning', 'info'], array_column($advice, 'severity'));
        foreach ($advice as $item) {
            $this->assertSame(['severity', 'title', 'evidence', 'action', 'doc', 'page'], array_keys($item));
            $this->assertStringStartsWith('https://queenmq.com/', $item['doc']);
        }
    }

    /**
     * @param array<string, mixed>|null $config
     * @param array<string, mixed>|null $snapshot
     * @param array<string, mixed>|null $metrics
     * @return list<array<string, mixed>>
     */
    private function advise(?array $config = null, ?array $snapshot = null, ?array $metrics = null): array
    {
        return (new TuningAdvisor())->advise($config ?? $this->config(), $snapshot ?? $this->snapshot(), $metrics ?? $this->metrics());
    }

    /**
     * A tuned application: prefork and event-driven scaling on, a lease longer
     * than the timeout.
     *
     * @param array<string, mixed> $queen merged into config('queen')
     * @param array<string, mixed> $connection the `queen` queue connection's own options
     * @return array<string, mixed>
     */
    private function config(array $queen = [], array $connection = []): array
    {
        return [
            'queen' => array_replace_recursive([
                'retry_after' => 90,
                'prefetch' => 1,
                'lease_renewal' => false,
                'supervisor' => [
                    'poll_interval' => 3,
                    'shutdown_grace' => 75,
                    'prefork' => true,
                    'event_driven' => true,
                    'supervisors' => ['default' => ['queues' => ['default'], 'balance' => 'auto', 'max_processes' => 4, 'timeout' => 60]],
                ],
            ], $queen),
            'queue' => ['connections' => ['queen' => ['driver' => 'queen', ...$connection]]],
        ];
    }

    /**
     * @param array<string, mixed> $overrides
     * @return array<string, mixed>
     */
    private function snapshot(array $overrides = []): array
    {
        return array_replace([
            'supervisor' => ['availability' => 'live', 'engine' => 'rust'],
            'instances' => [],
            'configuration' => ['poll_interval' => 3, 'shutdown_grace' => 75, 'supervisors' => [$this->publishedPool()]],
            'queues' => [$this->depth('default', 0)],
            'shared_queues' => [],
            'failed_jobs' => ['available' => true, 'total' => 0, 'total_exact' => true],
        ], $overrides);
    }

    /**
     * @param array<string, mixed> $overrides
     * @return array<string, mixed>
     */
    private function publishedPool(array $overrides = []): array
    {
        return array_replace([
            'name' => 'default', 'connection' => 'queen', 'consumer_group' => 'laravel', 'queues' => ['default'],
            'balance' => 'auto', 'strategy' => 'size', 'processes' => 4, 'min_processes' => 1, 'max_processes' => 4,
            'timeout' => 60, 'retry_after' => 90, 'tries' => 3, 'memory' => 128,
        ], $overrides);
    }

    /** @return array<string, mixed> */
    private function depth(string $queue, int $depth): array
    {
        return ['connection' => 'queen', 'consumer_group' => 'laravel', 'queue' => $queue, 'available' => true, 'depth' => $depth];
    }

    /**
     * @param list<array<string, mixed>> $classes
     * @return array<string, mixed>
     */
    private function metrics(array $classes = []): array
    {
        return [
            'available' => true,
            'truncated' => false,
            'range' => '1h',
            'minutes' => 60,
            'totals' => ['processed' => 0, 'failed' => 0],
            'classes' => array_map(static fn (array $class): array => $class + [
                'processed' => 10, 'failed' => 0, 'runtime_ms' => 0, 'max_ms' => null, 'average_ms' => null, 'per_minute' => 0.17,
            ], $classes),
        ];
    }
}
