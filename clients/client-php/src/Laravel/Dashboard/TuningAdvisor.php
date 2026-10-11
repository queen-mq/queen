<?php

namespace Queen\Laravel\Dashboard;

/**
 * Tuning advice for the Configuration page. Each rule reads this
 * application's settings, what the running supervisor published (its pools,
 * shutdown grace, sampled depth and shared queues), the failed-job count or
 * the per-class job metrics, and says what it found, what to change and where
 * the documentation explains it.
 *
 * Pure: no I/O, no clock, no container, so every rule is testable alone. The
 * inputs are read defensively, and a rule whose evidence is missing or invalid
 * gives no advice rather than a guess.
 */
final class TuningAdvisor
{
    public const DOCS = 'https://queenmq.com';

    private const SEVERITIES = ['critical' => 0, 'warning' => 1, 'info' => 2];

    /** Job classes named per rule, the most telling first. */
    private const MAX_CLASSES = 3;

    /** Below this, a job's pop is a large share of its time. */
    private const SHORT_JOB_MS = 100;

    /** One run a minute over the default hour; rarer jobs gain nothing worth a change. */
    private const SHORT_JOB_MIN_RUNS = 60;

    private const WINDOWS = ['1h' => 'hour', '6h' => '6 hours', '24h' => '24 hours'];

    /**
     * @param array<string, mixed> $config as ApplicationSettings takes it
     * @param array<string, mixed> $snapshot DashboardRepository::snapshot()
     * @param array<string, mixed> $jobMetrics JobMetricsReader::read()
     * @return list<array{severity: 'info'|'warning'|'critical', title: string, evidence: string, action: string, doc: string, page: ?string}>
     *   most severe first; `page` names a dashboard section to open, when one helps
     */
    public function advise(array $config, array $snapshot, array $jobMetrics): array
    {
        $settings = new ApplicationSettings($config);
        $published = $this->publishedPools($snapshot);
        $classes = $this->classes($jobMetrics);

        $advice = [
            ...$this->interruptedByDeploys($settings, $snapshot, $classes, $jobMetrics),
            ...$this->oneWorkerForSeveralQueues($published, $snapshot),
            ...$this->shortJobs($settings, $classes, $published),
            ...$this->prefork($settings),
            ...$this->leaseHelpers($settings),
            ...$this->pollingBursts($settings, $snapshot, $published),
            ...$this->runTwice($settings),
            ...$this->oneTryWithPrefetch($settings, $published),
            ...$this->sharedQueues($snapshot),
            ...$this->failedJobs($snapshot),
        ];
        // usort is stable: rules of one severity keep the order above.
        usort($advice, static fn (array $a, array $b): int => self::SEVERITIES[$a['severity']] <=> self::SEVERITIES[$b['severity']]);

        return $advice;
    }

    /**
     * The advice a supervisor prints when it starts, from this application's
     * configuration alone, for whoever does not open the dashboard.
     *
     * @param array<string, mixed> $config as ApplicationSettings takes it
     * @return list<string>
     */
    public static function startupWarnings(array $config): array
    {
        $settings = new ApplicationSettings($config);

        return array_map(
            static fn (array $item): string => "{$item['title']}. {$item['evidence']} {$item['action']}",
            (new self())->oneTryWithPrefetch($settings, []),
        );
    }

    /**
     * A job longer than shutdown_grace is killed by a deploy and runs again.
     *
     * @param list<array{class: string, runs: int, average_ms: ?int, max_ms: ?int}> $classes
     * @param array<string, mixed> $snapshot
     * @param array<string, mixed> $jobMetrics
     * @return list<array<string, mixed>>
     */
    private function interruptedByDeploys(ApplicationSettings $settings, array $snapshot, array $classes, array $jobMetrics): array
    {
        // What the running supervisor uses wins over this host's configuration.
        $grace = self::positive($snapshot['configuration']['shutdown_grace'] ?? null)
            ?? $settings->supervisorInteger('shutdown_grace', 75);
        if ($grace === null || $grace < 1) {
            return [];
        }
        $long = array_values(array_filter(
            $classes,
            static fn (array $class): bool => $class['max_ms'] !== null && $class['max_ms'] > $grace * 1000,
        ));
        usort($long, static fn (array $a, array $b): int => [$b['max_ms'], $a['class']] <=> [$a['max_ms'], $b['class']]);
        // A null array offset is deprecated since PHP 8.5.
        $range = $jobMetrics['range'] ?? null;
        $window = is_string($range) ? (self::WINDOWS[$range] ?? 'window') : 'window';

        $advice = [];
        foreach (array_slice($long, 0, self::MAX_CLASSES) as $class) {
            $advice[] = self::item(
                'warning',
                "Deploys interrupt {$class['class']}",
                "Its longest run in the last {$window} took " . self::duration($class['max_ms'])
                . ". A stopping worker gets {$grace} s (shutdown_grace) before the supervisor kills it, so a deploy can"
                . ' stop this job before it ends, and the job then runs again from the start.',
                "Raise queen.supervisor.shutdown_grace above the longest run, and the pod's terminationGracePeriodSeconds"
                . ' above that, or split the job into shorter jobs.',
                '/guides/laravel/supervisors',
            );
        }

        return $advice;
    }

    /**
     * One worker takes the queues of its pool in turns, so a backlog in one
     * holds up the others.
     *
     * @param list<array<string, mixed>> $published
     * @param array<string, mixed> $snapshot
     * @return list<array<string, mixed>>
     */
    private function oneWorkerForSeveralQueues(array $published, array $snapshot): array
    {
        $depths = $this->depths($snapshot);
        $advice = [];
        foreach ($published as $pool) {
            if ($pool['max_processes'] !== 1 || count($pool['queues']) < 2) {
                continue;
            }
            $waiting = [];
            foreach ($pool['queues'] as $queue) {
                $depth = $depths[$pool['connection'] . "\0" . $pool['consumer_group'] . "\0" . $queue] ?? 0;
                if ($depth > 0) {
                    $waiting[$queue] = $depth;
                }
            }
            if ($waiting === []) {
                continue;
            }
            arsort($waiting);
            $longest = (string) array_key_first($waiting);
            $advice[] = self::item(
                'warning',
                "Pool {$pool['name']} has one worker for " . count($pool['queues']) . ' queues',
                'Its max_processes is 1, so ' . self::list($pool['queues']) . " take turns on one worker, and {$longest} has "
                . self::jobs($waiting[$longest]) . ' waiting.',
                "Raise min_processes and max_processes in queen.supervisor.supervisors.{$pool['name']},"
                . ' or give the long queue its own pool.',
                '/guides/laravel/supervisors#how-a-pool-scales',
            );
        }

        return $advice;
    }

    /**
     * With prefetch 1 a worker pops before every job; for short jobs the pop
     * is a large share of the time. pop_ahead already hides it.
     *
     * @param list<array{class: string, runs: int, average_ms: ?int, max_ms: ?int}> $classes
     * @param list<array<string, mixed>> $published
     * @return list<array<string, mixed>>
     */
    private function shortJobs(ApplicationSettings $settings, array $classes, array $published): array
    {
        if ($settings->connectionPrefetch() !== 1 || $settings->connectionSwitch('pop_ahead', false) === true) {
            return [];
        }
        $short = array_values(array_filter(
            $classes,
            static fn (array $class): bool => $class['runs'] >= self::SHORT_JOB_MIN_RUNS
                && $class['average_ms'] !== null
                && $class['average_ms'] < self::SHORT_JOB_MS,
        ));
        if ($short === []) {
            return [];
        }
        usort($short, static fn (array $a, array $b): int => [$b['runs'], $a['class']] <=> [$a['runs'], $b['class']]);
        $named = array_map(
            static fn (array $class): string => "{$class['class']} ({$class['average_ms']} ms)",
            array_slice($short, 0, self::MAX_CLASSES),
        );
        $action = "Try prefetch 'auto', which sizes each pop from the jobs' runtime, with ack_async and pop_ahead true,"
            . ' in config/queen.php or on the queen connection in config/queue.php (whose values win), then compare the'
            . ' Jobs page.';
        if ($settings->connectionSwitch('lease_renewal', false) !== true) {
            $action .= ' Prefetch above 1 or auto, and pop_ahead, need lease_renewal true.';
        }
        // A crash nobody hands back fails each prefetched job that has no try left.
        $action .= ' Keep tries at 2 or more: after a crash that is not handed back, such as a lost node, each job'
            . ' a worker had prefetched comes back with one attempt more.';
        $pools = $published !== [] ? $published : $settings->pools();
        $oneTry = array_values(array_column(
            array_filter($pools, static fn (array $pool): bool => ($pool['tries'] ?? null) === 1),
            'name',
        ));

        return [self::item(
            $oneTry === [] ? 'info' : 'warning',
            'Short jobs wait for one pop each',
            'Prefetch is 1, so a worker asks the broker for each job before it runs it, and these jobs take under '
            . self::SHORT_JOB_MS . ' ms on average: ' . self::list(array_values($named)) . '.'
            . ($oneTry === [] ? '' : ' ' . (count($oneTry) === 1 ? "Pool {$oneTry[0]} has" : 'Pools ' . self::list($oneTry) . ' have') . ' tries 1.'),
            $action,
            '/guides/laravel#fast',
        )];
    }

    /**
     * With tries 1, an attempt charged to a held job that never ran fails it.
     *
     * @param list<array<string, mixed>> $published
     * @return list<array<string, mixed>>
     */
    private function oneTryWithPrefetch(ApplicationSettings $settings, array $published): array
    {
        $advice = [];
        $pools = $published !== [] ? $published : $settings->pools();
        foreach ($pools as $pool) {
            $connection = $pool['connection'] ?? null;
            if (($pool['tries'] ?? null) !== 1 || !is_string($connection)) {
                continue;
            }
            $prefetch = $settings->connectionPrefetch($connection);
            $popAhead = $settings->connectionSwitch('pop_ahead', false, $connection) === true;
            $held = array_filter([
                $prefetch !== null && $prefetch > 1 ? "prefetches up to {$prefetch} jobs" : null,
                $popAhead ? 'pops the next batch ahead' : null,
            ]);
            if ($held === []) {
                continue;
            }
            $advice[] = self::item(
                'warning',
                "Pool {$pool['name']} can fail a job that never ran",
                "It has tries 1, and its connection {$connection} " . implode(' and ', $held) . '. When a worker'
                . ' crashes, each job it held but had not started comes back with one attempt more, and with tries 1'
                . ' it fails without running. The worker\'s lease renewer hands those jobs back without the attempt,'
                . ' but not after a lost node, nor for a batch popped ahead whose answer the worker had not read.',
                "Set tries to 2 or more for pool {$pool['name']}, and for job classes that set \$tries = 1; or keep"
                . " prefetch 1 and pop_ahead off on connection {$connection}.",
                '/guides/laravel/concepts#prefetch-ack_async-and-pop_ahead',
            );
        }

        return $advice;
    }

    /** @return list<array<string, mixed>> */
    private function prefork(ApplicationSettings $settings): array
    {
        // On, or invalid (the supervisor would refuse it), or on for a pool
        // that sets its own: nothing to advise.
        if ($settings->supervisorSwitch('prefork', false) !== false
            || in_array(true, array_column($settings->pools(), 'prefork'), true)) {
            return [];
        }

        return [self::item(
            'info',
            'Every worker boots Laravel on its own',
            'Prefork is off. Measured on a Linux server with 64 workers after 10 ms jobs: 2,062 MiB when each worker'
            . ' booted Laravel, 225 MiB when they were forked from one booted copy with opcache on.',
            'Set QUEEN_SUPERVISOR_PREFORK=true, which needs ext-pcntl and ext-posix, and turn opcache.enable_cli on for'
            . ' the PHP that runs the workers.',
            '/guides/laravel/supervisors#prefork-workers',
        )];
    }

    /** @return list<array<string, mixed>> */
    private function leaseHelpers(ApplicationSettings $settings): array
    {
        $renewing = $settings->connectionSwitch('lease_renewal', false) === true
            || in_array(true, array_column($settings->pools(), 'lease_renewal'), true);
        if (!$renewing || !$settings->leaseServiceDisabled()) {
            return [];
        }

        return [self::item(
            'info',
            'Every worker starts its own lease helper',
            "lease_renewal is on and queen.supervisor.lease_service turns the master's lease service off, so each"
            . ' worker runs a PHP helper beside it. Measured on a Linux server with 32 prefetching workers and their'
            . ' master: 417 MiB with a helper per worker, 116 MiB without.',
            'Set queen.supervisor.lease_service to true, its default: the Rust supervisor on Linux then renews every'
            . ' lease itself.',
            '/guides/laravel/supervisors#lease-renewal-in-the-master',
        )];
    }

    /**
     * @param array<string, mixed> $snapshot
     * @param list<array<string, mixed>> $published
     * @return list<array<string, mixed>>
     */
    private function pollingBursts(ApplicationSettings $settings, array $snapshot, array $published): array
    {
        if ($settings->supervisorSwitch('event_driven', false) !== false) {
            return [];
        }
        // The pools that run, when a supervisor published them.
        $pools = $published !== [] ? $published : $settings->pools();
        $auto = array_values(array_column(
            array_filter($pools, static fn (array $pool): bool => $pool['balance'] === 'auto'),
            'name',
        ));
        if ($auto === []) {
            return [];
        }
        $poll = self::positive($snapshot['configuration']['poll_interval'] ?? null)
            ?? $settings->supervisorInteger('poll_interval', 3) ?? 3;

        return [self::item(
            'info',
            'Bursts wait for the next poll',
            (count($auto) === 1 ? "Pool {$auto[0]} follows" : 'Pools ' . self::list($auto) . ' follow')
            . " the backlog (balance auto) and event-driven scaling is off, so new jobs are seen at the next poll, every {$poll} s."
            . ' Measured on a Linux server with fast_scale_up: a pool of 1 to 64 workers had all 64 running 8.2 s after'
            . ' a burst of 30,000 jobs began.',
            'Set queen.supervisor.event_driven to true: the broker then wakes the supervisor when jobs arrive.',
            '/guides/laravel/supervisors#event-driven-scaling',
        )];
    }

    /**
     * Without renewal, a lease that ends before the timeout hands a running
     * job to a second worker. The running supervisor refuses such a pool, so
     * this reads this application's configuration.
     *
     * @return list<array<string, mixed>>
     */
    private function runTwice(ApplicationSettings $settings): array
    {
        $advice = [];
        foreach ($settings->pools() as $pool) {
            if ($pool['timeout'] === null || $pool['retry_after'] === null || $pool['timeout'] < $pool['retry_after']) {
                continue;
            }
            if ($pool['lease_renewal'] === false) {
                $advice[] = self::item(
                    'critical',
                    "Pool {$pool['name']} can run a job twice at once",
                    "Its timeout is {$pool['timeout']} s and its lease (retry_after) {$pool['retry_after']} s, with lease renewal"
                    . ' off: a job still running when its lease ends is handed to a second worker while the first one goes on.',
                    "Raise queen.supervisor.supervisors.{$pool['name']}.retry_after above the timeout, or lower the timeout."
                    . ' The Queen supervisor does not start with this pool until then.',
                    '/guides/laravel/supervisors#configure-a-pool',
                );
            } elseif ($pool['lease_renewal'] === true) {
                // Renewal keeps the lease alive, but SupervisorConfiguration
                // refuses the pair whatever the renewal.
                $advice[] = self::item(
                    'critical',
                    "The supervisor does not start with pool {$pool['name']}",
                    "Its timeout is {$pool['timeout']} s and its retry_after {$pool['retry_after']} s. The supervisor requires"
                    . ' retry_after to be longer than the timeout, even with lease renewal on.',
                    "Raise queen.supervisor.supervisors.{$pool['name']}.retry_after above the timeout, or lower the timeout.",
                    '/guides/laravel/supervisors#configure-a-pool',
                );
            }
        }

        return $advice;
    }

    /**
     * @param array<string, mixed> $snapshot
     * @return list<array<string, mixed>>
     */
    private function sharedQueues(array $snapshot): array
    {
        $shared = array_values(array_filter(
            is_array($snapshot['shared_queues'] ?? null) ? $snapshot['shared_queues'] : [],
            static fn (mixed $queue): bool => is_array($queue)
                && is_string($queue['queue'] ?? null)
                && is_string($queue['consumer_group'] ?? null)
                && is_int($queue['instances'] ?? null),
        ));
        if ($shared === []) {
            return [];
        }
        $first = $shared[0];
        $others = count($shared) - 1;

        return [self::item(
            'warning',
            "{$first['instances']} supervisors autoscale queue {$first['queue']} without coordinating",
            "Each master sizes its workers from the whole backlog of {$first['queue']} (consumer group"
            . " {$first['consumer_group']}), so together they can run more workers than max_processes."
            . ($others > 0 ? " {$others} other " . ($others === 1 ? 'queue is' : 'queues are') . ' shared too.' : ''),
            'Set QUEEN_SUPERVISOR_COORDINATION=true on every replica, so they share one worker target.',
            '/guides/laravel/supervisors#several-replicas',
        )];
    }

    /**
     * @param array<string, mixed> $snapshot
     * @return list<array<string, mixed>>
     */
    private function failedJobs(array $snapshot): array
    {
        $failed = $snapshot['failed_jobs'] ?? null;
        if (!is_array($failed)
            || ($failed['available'] ?? false) !== true
            || !is_int($failed['total'] ?? null)
            || $failed['total'] < 1) {
            return [];
        }
        $exact = ($failed['total_exact'] ?? false) === true;
        $count = number_format($failed['total']) . ($exact ? '' : '+');
        $jobs = $exact && $failed['total'] === 1 ? 'job' : 'jobs';

        return [self::item(
            'info',
            "{$count} failed {$jobs}",
            "Laravel's failed-job store holds {$count} {$jobs} that will not run again unless retried.",
            'Open each one to see why it failed, fix the cause, then retry it from its page or with php artisan queue:retry.',
            '/guides/laravel/dashboard#why-a-job-failed',
            'failed-jobs',
        )];
    }

    /**
     * The pools the running supervisor published, already checked by
     * DashboardRepository; checked again here, since this class takes any array.
     *
     * @param array<string, mixed> $snapshot
     * @return list<array{name: string, connection: string, consumer_group: string, queues: list<string>, balance: string, max_processes: int, tries: ?int}>
     */
    private function publishedPools(array $snapshot): array
    {
        $configured = $snapshot['configuration']['supervisors'] ?? null;
        $pools = [];
        foreach (is_array($configured) ? $configured : [] as $pool) {
            if (!is_array($pool)
                || !is_string($pool['name'] ?? null)
                || !is_string($pool['connection'] ?? null)
                || !is_string($pool['consumer_group'] ?? null)
                || !is_array($pool['queues'] ?? null)
                || !is_string($pool['balance'] ?? null)
                || !is_int($pool['max_processes'] ?? null)) {
                continue;
            }
            $pools[] = [
                'name' => $pool['name'],
                'connection' => $pool['connection'],
                'consumer_group' => $pool['consumer_group'],
                'queues' => array_values(array_filter($pool['queues'], 'is_string')),
                'balance' => $pool['balance'],
                'max_processes' => $pool['max_processes'],
                'tries' => is_int($pool['tries'] ?? null) ? $pool['tries'] : null,
            ];
        }

        return $pools;
    }

    /**
     * @param array<string, mixed> $jobMetrics
     * @return list<array{class: string, runs: int, average_ms: ?int, max_ms: ?int}>
     */
    private function classes(array $jobMetrics): array
    {
        if (($jobMetrics['available'] ?? false) !== true || !is_array($jobMetrics['classes'] ?? null)) {
            return [];
        }
        $classes = [];
        foreach ($jobMetrics['classes'] as $row) {
            if (!is_array($row) || !is_string($row['class'] ?? null) || $row['class'] === '') {
                continue;
            }
            $classes[] = [
                'class' => $row['class'],
                'runs' => (self::count($row['processed'] ?? null) ?? 0) + (self::count($row['failed'] ?? null) ?? 0),
                'average_ms' => self::count($row['average_ms'] ?? null),
                'max_ms' => self::count($row['max_ms'] ?? null),
            ];
        }

        return $classes;
    }

    /**
     * @param array<string, mixed> $snapshot
     * @return array<string, int> sampled depth by connection, consumer group and queue
     */
    private function depths(array $snapshot): array
    {
        $depths = [];
        foreach (is_array($snapshot['queues'] ?? null) ? $snapshot['queues'] : [] as $queue) {
            if (is_array($queue)
                && ($queue['available'] ?? false) === true
                && is_string($queue['connection'] ?? null)
                && is_string($queue['consumer_group'] ?? null)
                && is_string($queue['queue'] ?? null)
                && self::count($queue['depth'] ?? null) !== null) {
                $depths[$queue['connection'] . "\0" . $queue['consumer_group'] . "\0" . $queue['queue']] = $queue['depth'];
            }
        }

        return $depths;
    }

    /** @return array{severity: string, title: string, evidence: string, action: string, doc: string, page: ?string} */
    private static function item(string $severity, string $title, string $evidence, string $action, string $doc, ?string $page = null): array
    {
        return [
            'severity' => $severity,
            'title' => $title,
            'evidence' => $evidence,
            'action' => $action,
            'doc' => self::DOCS . $doc,
            'page' => $page,
        ];
    }

    /** Whole units: milliseconds, seconds, minutes and seconds, then hours and minutes. */
    private static function duration(int $milliseconds): string
    {
        $seconds = intdiv($milliseconds, 1000);

        return match (true) {
            $milliseconds < 1000 => $milliseconds . ' ms',
            $seconds < 60 => $seconds . ' s',
            $seconds < 3600 => intdiv($seconds, 60) . ' min ' . sprintf('%02d', $seconds % 60) . ' s',
            default => intdiv($seconds, 3600) . ' h ' . sprintf('%02d', intdiv($seconds % 3600, 60)) . ' min',
        };
    }

    private static function jobs(int $count): string
    {
        return number_format($count) . ($count === 1 ? ' job' : ' jobs');
    }

    /** @param list<string> $items "a", "a and b", "a, b and c"; five at most, then how many more */
    private static function list(array $items): string
    {
        $shown = array_slice($items, 0, 5);
        if (count($items) > count($shown)) {
            $shown[] = (count($items) - count($shown)) . ' more';
        }
        $last = array_pop($shown);

        return $shown === [] ? (string) $last : implode(', ', $shown) . ' and ' . $last;
    }

    private static function count(mixed $value): ?int
    {
        return is_int($value) && $value >= 0 ? $value : null;
    }

    private static function positive(mixed $value): ?int
    {
        return is_int($value) && $value >= 1 ? $value : null;
    }
}
