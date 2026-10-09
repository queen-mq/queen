<?php

namespace Queen\Laravel\Supervisor;

use InvalidArgumentException;
use Queen\Laravel\Queue\AdaptiveBatch;
use Queen\Laravel\Queue\QueenQueue;

final class SupervisorConfiguration
{
    public const VERSION = 2;

    public const MAX_CONFIG_BYTES = 1048576;

    private const MAX_QUEUES_PER_SUPERVISOR = 1024;

    /**
     * A status document contains both the normalized pool list and the legacy
     * pool map. Keeping this cap below the process limit leaves enough room
     * for both representations, worker PIDs and the public configuration
     * snapshot while retaining the 1 MiB status-file ceiling.
     */
    public const MAX_STATUS_POOLS = 256;

    private const MAX_IDENTIFIER_BYTES = 128;

    private const MAX_QUEUE_NAME_BYTES = 256;

    private const MAX_DURATION_SECONDS = 31536000;

    private const MIN_SCALING_SECONDS = 0.000001;

    private const DEPTH_POLL_CONCURRENCY = 16;

    private const PROCESS_START_BUDGET_SECONDS = 5;

    private const TELEMETRY_SCAN_BUDGET_SECONDS = 60;

    private const CONTROL_LOOP_MARGIN_SECONDS = 5;

    public static function resolve(
        array $queen,
        string $basePath,
        ?string $phpBinary = null,
        array $queueConnections = [],
    ): array {
        $raw = $queen['supervisor'] ?? [];
        if (!is_array($raw)) {
            throw new InvalidArgumentException('Queen supervisor configuration must be an array.');
        }

        $pollInterval = self::positiveDuration($raw['poll_interval'] ?? 3, 'poll_interval');
        $httpTimeout = self::positiveDuration($raw['http_timeout'] ?? 5, 'http_timeout');
        $controlTtl = self::positiveInteger($raw['control_ttl'] ?? 3600, 'control_ttl');
        if ($controlTtl < 30 || $controlTtl > 86400) {
            throw new InvalidArgumentException('Queen supervisor control_ttl must be between 30 and 86400 seconds.');
        }

        $processLimit = self::positiveInteger($raw['process_limit'] ?? 256, 'process_limit');
        if ($processLimit > 4096) {
            throw new InvalidArgumentException('Queen supervisor process_limit may not exceed 4096.');
        }
        $supervisors = $raw['supervisors'] ?? [];
        if (!is_array($supervisors) || $supervisors === []) {
            $supervisors = ['default' => []];
        }

        $resolved = [];
        $connections = [];
        $statusPools = 0;
        // A pool's own `prefork` wins; a pool without one follows the switch.
        $preforkDefault = self::boolean($raw['prefork'] ?? false, 'prefork');
        $preforkPools = [];
        $maximumControlLoopSeconds = $pollInterval;
        foreach ($supervisors as $name => $options) {
            $name = (string) $name;
            self::identifier($name, 'supervisor name');
            if (!is_array($options)) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] must be an array.");
            }

            $connection = (string) ($options['connection'] ?? 'queen');
            self::identifier($connection, "supervisor [{$name}] connection");
            // The settings the pool's workers run with: the connection's own
            // keys over config/queen.php. A pool that names no consumer group
            // or queues works the connection's, as `queue:work <connection>`
            // run by hand does.
            $connectionConfig = self::connectionConfig($connection, $queen, $queueConnections, $raw);
            $consumerGroup = (string) ($options['consumer_group'] ?? $connectionConfig['consumer_group'] ?? 'laravel');
            self::identifier($consumerGroup, "supervisor [{$name}] consumer_group");

            $queues = $options['queues'] ?? $options['queue'] ?? [$connectionConfig['queue'] ?? 'default'];
            $queues = is_string($queues) ? explode(',', $queues) : $queues;
            if (!is_array($queues)) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] queues must be an array or comma-separated string.");
            }
            $queues = array_map(static fn (mixed $queue): mixed => is_string($queue) ? trim($queue) : $queue, $queues);
            foreach ($queues as $queue) {
                if (!is_string($queue) || trim($queue) === '') {
                    throw new InvalidArgumentException("Queen supervisor [{$name}] queues must contain only non-empty strings.");
                }
                self::queueName($queue, $name);
            }
            $queues = array_values(array_unique($queues));
            if ($queues === []) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] must declare at least one queue.");
            }
            if (count($queues) > self::MAX_QUEUES_PER_SUPERVISOR) {
                throw new InvalidArgumentException(
                    "Queen supervisor [{$name}] may not declare more than " . self::MAX_QUEUES_PER_SUPERVISOR . ' queues.',
                );
            }
            if (count($queues) > self::MAX_STATUS_POOLS - $statusPools) {
                throw new InvalidArgumentException(
                    'Queen supervisors may not declare more than ' . self::MAX_STATUS_POOLS
                    . ' aggregate status pools.',
                );
            }
            $statusPools += count($queues);

            $balance = $options['balance'] ?? 'auto';
            if ($balance === false) {
                $balance = 'off';
            }
            if (!in_array($balance, ['auto', 'simple', 'off'], true)) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] has an invalid balance strategy.");
            }
            $strategy = (string) ($options['strategy'] ?? 'size');
            if (!in_array($strategy, ['size', 'time'], true)) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] has an invalid auto-scaling strategy.");
            }

            $min = self::nonNegativeInteger($options['min_processes'] ?? 1, "supervisor [{$name}] min_processes");
            $max = self::positiveInteger($options['max_processes'] ?? 10, "supervisor [{$name}] max_processes");
            if ($max < $min) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] max_processes must be >= min_processes.");
            }
            if ($max > $processLimit) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] exceeds process_limit [{$processLimit}].");
            }
            $processes = min($max, max($min, self::nonNegativeInteger($options['processes'] ?? $max, "supervisor [{$name}] processes")));
            if ($balance === 'auto' && $max < count($queues)) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] max_processes must cover every queue when balance is auto.");
            }
            if ($balance === 'simple' && $processes < count($queues)) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] processes must cover every queue when balance is simple.");
            }
            // Like Horizon's per-queue minProcesses: every queue of an auto
            // pool keeps this many workers, even without backlog.
            $minPerQueue = self::nonNegativeInteger(
                $options['min_processes_per_queue'] ?? 0,
                "supervisor [{$name}] min_processes_per_queue",
            );
            if ($minPerQueue > 0 && $balance !== 'auto') {
                throw new InvalidArgumentException(
                    "Queen supervisor [{$name}] min_processes_per_queue requires balance auto.",
                );
            }
            if ($minPerQueue > intdiv($max, count($queues))) {
                throw new InvalidArgumentException(
                    "Queen supervisor [{$name}] min_processes_per_queue for every queue must fit max_processes [{$max}].",
                );
            }
            $balanceMaxShift = self::positiveInteger(
                $options['balance_max_shift'] ?? 1,
                "supervisor [{$name}] balance_max_shift",
            );
            if ($balanceMaxShift > $max) {
                throw new InvalidArgumentException(
                    "Queen supervisor [{$name}] balance_max_shift must not exceed max_processes [{$max}].",
                );
            }

            $timeout = self::positiveDuration($options['timeout'] ?? 60, "supervisor [{$name}] timeout");
            $retryAfter = self::positiveDuration(
                $options['retry_after'] ?? $connectionConfig['retry_after'] ?? $queen['retry_after'] ?? 90,
                "supervisor [{$name}] retry_after",
            );
            if ($retryAfter <= $timeout) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] retry_after must be longer than timeout.");
            }
            // "auto" sizes each pop up to its ceiling, so it is a prefetch
            // above 1 for every rule below.
            $adaptivePrefetch = AdaptiveBatch::isAuto($connectionConfig['prefetch'] ?? 1);
            $prefetch = $adaptivePrefetch ? AdaptiveBatch::CEILING : self::positiveInteger(
                $connectionConfig['prefetch'] ?? 1,
                "supervisor [{$name}] connection prefetch",
            );
            if ($prefetch > 1000) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] connection prefetch may not exceed 1000.");
            }
            $leaseRenewal = self::boolean(
                $connectionConfig['lease_renewal'] ?? false,
                "supervisor [{$name}] connection lease_renewal",
            );
            // A prefetched tail remains leased in the Laravel connection while
            // the worker can pause indefinitely for maintenance mode,
            // queue:pause or a Looping listener. No timeout/rest arithmetic
            // can bound that pause, so multi-message leases require the
            // renewal helper that fences the worker before unsafe expiry.
            if ($prefetch > 1 && !$leaseRenewal) {
                throw new InvalidArgumentException(
                    "Queen supervisor [{$name}] connection prefetch [" . ($adaptivePrefetch ? 'auto' : $prefetch) . '] requires lease_renewal.',
                );
            }
            if ($leaseRenewal) {
                $intervalOption = $connectionConfig['lease_renewal_interval'] ?? null;
                $interval = self::positiveInteger(
                    $intervalOption === null || $intervalOption === ''
                        ? max(1, intdiv($retryAfter, 3))
                        : $intervalOption,
                    "supervisor [{$name}] connection lease_renewal_interval",
                );
                $requestTimeout = self::positiveInteger(
                    $connectionConfig['lease_renewal_timeout'] ?? 5,
                    "supervisor [{$name}] connection lease_renewal_timeout",
                );
                $killGrace = self::nonNegativeInteger(
                    $connectionConfig['lease_renewal_kill_grace'] ?? 2,
                    "supervisor [{$name}] connection lease_renewal_kill_grace",
                );
                $safetyMargin = self::positiveInteger(
                    $connectionConfig['lease_renewal_safety_margin'] ?? 1,
                    "supervisor [{$name}] connection lease_renewal_safety_margin",
                );
                $backendCount = self::connectionBackendCount($connectionConfig);
                if ($requestTimeout > intdiv(PHP_INT_MAX, $backendCount)) {
                    throw new InvalidArgumentException("Queen supervisor [{$name}] lease renewal request budget is too large.");
                }
                $requestBudget = $requestTimeout * $backendCount;
                if (!self::sumIsBelow(
                    [$interval, $requestBudget, $requestBudget, 1, $killGrace, $safetyMargin],
                    $retryAfter,
                )) {
                    throw new InvalidArgumentException(
                        "Queen supervisor [{$name}] lease renewal timing budget must be shorter than retry_after.",
                    );
                }
            }
            $processCostPerWorker = $leaseRenewal ? 2 : 1;
            if ($max > intdiv($processLimit, $processCostPerWorker)) {
                throw new InvalidArgumentException(
                    "Queen supervisor [{$name}] reserves {$processCostPerWorker} child processes per worker and exceeds "
                    . "process_limit [{$processLimit}].",
                );
            }
            $connections[$connection] = self::readConnection($connectionConfig);
            $depthWaves = intdiv(count($queues) + self::DEPTH_POLL_CONCURRENCY - 1, self::DEPTH_POLL_CONCURRENCY);
            $backendCount = self::connectionBackendCount($connectionConfig);
            if ($depthWaves > intdiv(PHP_INT_MAX, $backendCount)
                || $depthWaves * $backendCount > intdiv(PHP_INT_MAX, $httpTimeout)) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] depth polling budget is too large.");
            }
            $poolControlDelay = $depthWaves * $backendCount * $httpTimeout;
            if ($maximumControlLoopSeconds > PHP_INT_MAX - $poolControlDelay) {
                throw new InvalidArgumentException('Queen supervisor aggregate depth polling budget is too large.');
            }
            $maximumControlLoopSeconds += $poolControlDelay;

            $restartBackoff = self::nonNegativeDuration($options['restart_backoff'] ?? 1, "supervisor [{$name}] restart_backoff");
            $restartBackoffMax = self::nonNegativeDuration($options['restart_backoff_max'] ?? 30, "supervisor [{$name}] restart_backoff_max");
            if ($restartBackoffMax < $restartBackoff) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] restart_backoff_max must be >= restart_backoff.");
            }

            $resolved[$name] = [
                'connection' => $connection,
                'consumer_group' => $consumerGroup,
                'queues' => $queues,
                'balance' => $balance,
                'strategy' => $strategy,
                'processes' => $processes,
                'min_processes' => $min,
                'max_processes' => $max,
                'target_jobs_per_process' => self::positiveInteger($options['target_jobs_per_process'] ?? 10, "supervisor [{$name}] target_jobs_per_process"),
                'target_clear_seconds' => self::positiveFloat($options['target_clear_seconds'] ?? 60, "supervisor [{$name}] target_clear_seconds"),
                'default_runtime_seconds' => self::positiveFloat($options['default_runtime_seconds'] ?? 1, "supervisor [{$name}] default_runtime_seconds"),
                'balance_cooldown' => self::positiveDuration($options['balance_cooldown'] ?? 3, "supervisor [{$name}] balance_cooldown"),
                'balance_max_shift' => $balanceMaxShift,
                'scale_down_delay' => self::nonNegativeDuration($options['scale_down_delay'] ?? 10, "supervisor [{$name}] scale_down_delay"),
                'restart_backoff' => $restartBackoff,
                'restart_backoff_max' => $restartBackoffMax,
                'stable_after' => self::positiveDuration($options['stable_after'] ?? 60, "supervisor [{$name}] stable_after"),
                'sleep' => self::nonNegativeInteger($options['sleep'] ?? 1, "supervisor [{$name}] sleep"),
                'timeout' => $timeout,
                'retry_after' => $retryAfter,
                'lease_renewal' => $leaseRenewal,
                'tries' => self::nonNegativeInteger($options['tries'] ?? 3, "supervisor [{$name}] tries"),
                'memory' => self::positiveInteger($options['memory'] ?? 128, "supervisor [{$name}] memory"),
                'backoff' => self::nonNegativeInteger($options['backoff'] ?? 0, "supervisor [{$name}] backoff"),
                'max_jobs' => self::nonNegativeInteger($options['max_jobs'] ?? 0, "supervisor [{$name}] max_jobs"),
                'max_time' => self::nonNegativeInteger($options['max_time'] ?? 0, "supervisor [{$name}] max_time"),
                'rest' => self::nonNegativeInteger($options['rest'] ?? 0, "supervisor [{$name}] rest"),
                'force' => self::boolean($options['force'] ?? false, "supervisor [{$name}] force"),
                'quiet' => self::boolean($options['quiet'] ?? true, "supervisor [{$name}] quiet"),
            ];
            if ($minPerQueue > 0) {
                // Emitted only when set: engines reject unknown contract keys.
                $resolved[$name]['min_processes_per_queue'] = $minPerQueue;
            }
            if (self::boolean($options['fast_scale_up'] ?? false, "supervisor [{$name}] fast_scale_up")) {
                $resolved[$name]['fast_scale_up'] = true;
            }
            $preforkPools[$name] = ($options['prefork'] ?? null) === null
                ? $preforkDefault
                : self::boolean($options['prefork'], "supervisor [{$name}] prefork");
        }
        // One pool that forks is enough to start the fork server. The pools
        // that do not are the exception the engines need to be told about,
        // so the key is emitted only as false, and only beside a server.
        $prefork = in_array(true, $preforkPools, true);
        if ($prefork) {
            foreach (array_keys($preforkPools, false, true) as $name) {
                $resolved[$name]['prefork'] = false;
            }
        }

        $totalMaxProcesses = array_sum(array_column($resolved, 'max_processes'));
        $totalMaxProcessBudget = array_sum(array_map(
            static fn (array $options): int => $options['max_processes'] * ($options['lease_renewal'] ? 2 : 1),
            $resolved,
        ));
        if ($totalMaxProcessBudget > $processLimit) {
            throw new InvalidArgumentException(
                "Queen supervisors reserve [{$totalMaxProcessBudget}] child processes and exceed the aggregate "
                . "process_limit [{$processLimit}].",
            );
        }
        $timeSupervisors = count(array_filter(
            $resolved,
            static fn (array $options): bool => $options['strategy'] === 'time' && $options['balance'] !== 'simple',
        ));
        $remoteStatus = self::remoteStatusSettings($raw, $queen, $queueConnections);
        if ($remoteStatus !== null) {
            // One synchronous publish per endpoint may run inside a control
            // loop iteration, so it belongs to the heartbeat budget.
            $publishBudget = count($remoteStatus['connection']['urls']) * $httpTimeout;
            if ($maximumControlLoopSeconds > PHP_INT_MAX - $publishBudget) {
                throw new InvalidArgumentException('Queen supervisor remote status publish budget is too large.');
            }
            $maximumControlLoopSeconds += $publishBudget;
        }
        $coordination = self::coordinationSettings($raw, $queen, $queueConnections);
        if ($coordination !== null) {
            // Every control-loop iteration renews and lists the replicas of
            // each autoscaling pool: one call per POOLS_PER_CALL pools, one
            // attempt per endpoint.
            $scopes = count(array_unique(array_map(
                static fn (array $options): string => ReplicaCoordinator::scope(
                    $connections[$options['connection']]['urls'],
                    $options['consumer_group'],
                    $options['queues'],
                ),
                array_filter($resolved, static fn (array $options): bool => $options['balance'] !== 'simple'),
            )));
            $calls = intdiv($scopes + ReplicaCoordinator::POOLS_PER_CALL - 1, ReplicaCoordinator::POOLS_PER_CALL);
            $coordinationBudget = $calls * count($coordination['connection']['urls']) * $httpTimeout;
            if ($maximumControlLoopSeconds > PHP_INT_MAX - $coordinationBudget) {
                throw new InvalidArgumentException('Queen supervisor coordination budget is too large.');
            }
            $maximumControlLoopSeconds += $coordinationBudget;
        }
        $eventDriven = self::boolean($raw['event_driven'] ?? false, 'event_driven');
        if ($eventDriven) {
            // The PHP engine parks on one fetch per watched connection within
            // a loop iteration, trying each endpoint: the wait plus one
            // request per endpoint.
            $watchBudget = 1 + array_sum(array_map(
                static fn (array $connection): int => count($connection['urls']) * $httpTimeout,
                $connections,
            ));
            if ($maximumControlLoopSeconds > PHP_INT_MAX - $watchBudget) {
                throw new InvalidArgumentException('Queen supervisor event-driven budget is too large.');
            }
            $maximumControlLoopSeconds += $watchBudget;
        }
        $controlLoopRemainder = ($totalMaxProcesses * self::PROCESS_START_BUDGET_SECONDS)
            + ($timeSupervisors * self::TELEMETRY_SCAN_BUDGET_SECONDS)
            + self::CONTROL_LOOP_MARGIN_SECONDS;
        if ($maximumControlLoopSeconds > PHP_INT_MAX - $controlLoopRemainder) {
            throw new InvalidArgumentException('Queen supervisor aggregate control-loop budget is too large.');
        }
        $maximumControlLoopSeconds += $controlLoopRemainder;
        if ($controlTtl <= $maximumControlLoopSeconds) {
            throw new InvalidArgumentException(
                "Queen supervisor control_ttl [{$controlTtl}] must exceed the bounded depth/control loop budget "
                . "[{$maximumControlLoopSeconds}] seconds.",
            );
        }
        $heartbeatTimeout = self::positiveInteger(
            $raw['heartbeat_timeout'] ?? min(86400, max(15, $maximumControlLoopSeconds + 1)),
            'heartbeat_timeout',
        );
        if ($heartbeatTimeout > 86400 || $heartbeatTimeout <= $maximumControlLoopSeconds) {
            throw new InvalidArgumentException(
                "Queen supervisor heartbeat_timeout [{$heartbeatTimeout}] must be at most 86400 seconds and exceed "
                . "the bounded depth/control loop budget [{$maximumControlLoopSeconds}] seconds.",
            );
        }
        $maximumWorkerTimeout = max(array_column($resolved, 'timeout'));
        $shutdownGrace = self::positiveDuration($raw['shutdown_grace'] ?? $maximumWorkerTimeout + 15, 'shutdown_grace');
        if ($shutdownGrace <= $maximumWorkerTimeout) {
            throw new InvalidArgumentException('Queen supervisor shutdown_grace must be longer than every worker timeout.');
        }

        $defaultConnection = $connections['queen'] ?? reset($connections);
        $stateDirectory = self::stateDirectory($raw['state_directory'] ?? null, $basePath);

        $result = [
            'version' => self::VERSION,
            'cwd' => $basePath,
            'php_binary' => $phpBinary ?? PHP_BINARY,
            'artisan' => $basePath . DIRECTORY_SEPARATOR . 'artisan',
            'state_directory' => $stateDirectory,
            'poll_interval' => $pollInterval,
            'http_timeout' => $httpTimeout,
            'control_ttl' => $controlTtl,
            'heartbeat_timeout' => $heartbeatTimeout,
            'shutdown_grace' => $shutdownGrace,
            'process_limit' => $processLimit,
            'telemetry_ttl' => self::positiveDuration($raw['telemetry_ttl'] ?? 300, 'telemetry_ttl'),
            // `queen` remains the default/fallback for early v2 Rust binaries;
            // `connections` is authoritative for each supervisor pool.
            'queen' => $defaultConnection,
            'connections' => $connections,
            'supervisors' => $resolved,
        ];
        if ($remoteStatus !== null) {
            // Emitted only when enabled: engines reject unknown contract keys,
            // so a disabled feature keeps the document byte-identical.
            $result['remote_status'] = self::remoteStatusTiming($remoteStatus, $pollInterval, $heartbeatTimeout);
        }
        if ($prefork) {
            // Emitted only when enabled: engines reject unknown contract keys.
            $result['prefork'] = true;
        }
        if (!self::boolean($raw['lease_service'] ?? true, 'lease_service')) {
            // Emitted only when off: engines reject unknown contract keys, and
            // the Rust engine reads an absent key as on. The PHP engine has no
            // lease service: each lease_renewal worker starts its own helper.
            $result['lease_service'] = false;
        }
        if ($eventDriven) {
            // Emitted only when enabled: the partition stripes the Laravel
            // driver pushes to on each connection, which the engines watch.
            $stripes = [];
            foreach (array_keys($connections) as $connection) {
                $stripes[$connection] = self::stripes(
                    (string) $connection,
                    self::connectionConfig((string) $connection, $queen, $queueConnections, $raw),
                );
            }
            $result['event_driven'] = ['stripes' => $stripes];
        }
        if ($coordination !== null) {
            // A live replica renews its key within every control-loop
            // iteration; a crashed one stops counting when it expires, so the
            // TTL is the loop bound, not a longer explicit heartbeat_timeout.
            $result['coordination'] = [
                ...$coordination,
                'ttl' => min($heartbeatTimeout, max(15, $maximumControlLoopSeconds + 1)),
            ];
        }
        $encoded = json_encode($result, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
        if (strlen($encoded) + 1 > self::MAX_CONFIG_BYTES) {
            throw new InvalidArgumentException(
                'Resolved Queen supervisor engine configuration exceeds the 1 MiB transport limit.',
            );
        }

        return $result;
    }

    public static function stateDirectory(mixed $configured, string $basePath): string
    {
        if ($configured !== null && !is_string($configured)) {
            throw new InvalidArgumentException('Queen supervisor state_directory must be a string.');
        }
        $stateDirectory = $configured ?? rtrim($basePath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR . 'storage' . DIRECTORY_SEPARATOR . 'queen-supervisor';
        if ($stateDirectory === '' || preg_match('/[\x00-\x1F\x7F]/', $stateDirectory) === 1) {
            throw new InvalidArgumentException('Queen supervisor state_directory must not be empty.');
        }
        $windowsAbsolute = preg_match('/\A[A-Za-z]:[\\\\\/]/D', $stateDirectory) === 1
            || str_starts_with($stateDirectory, '\\\\');
        if (DIRECTORY_SEPARATOR === '/' && $windowsAbsolute) {
            throw new InvalidArgumentException(
                'Queen supervisor state_directory must use an absolute Unix path on this host.',
            );
        }
        $absolute = DIRECTORY_SEPARATOR === '/'
            ? str_starts_with($stateDirectory, '/')
            : (str_starts_with($stateDirectory, DIRECTORY_SEPARATOR) || $windowsAbsolute);
        if (!$absolute) {
            $stateDirectory = rtrim($basePath, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $stateDirectory;
        }
        $pathComponents = preg_split('/[\\\\\/]+/', $stateDirectory, -1, PREG_SPLIT_NO_EMPTY);
        $normalComponents = is_array($pathComponents)
            ? array_values(array_filter(
                $pathComponents,
                static fn (string $component): bool => $component !== '.',
            ))
            : [];
        if (preg_match('/\A[A-Za-z]:[\\\\\/]/D', $stateDirectory) === 1
            && isset($normalComponents[0])
            && preg_match('/\A[A-Za-z]:\z/D', $normalComponents[0]) === 1) {
            array_shift($normalComponents);
        } elseif (str_starts_with($stateDirectory, '\\\\')) {
            // A UNC server/share pair is the path root, not a state-directory
            // component. Require at least one real component below the share.
            $normalComponents = array_slice($normalComponents, 2);
        }
        if (!is_array($pathComponents)
            || in_array('..', $pathComponents, true)
            || $normalComponents === []) {
            throw new InvalidArgumentException(
                'Queen supervisor state_directory must be an absolute, non-root path without parent traversal.',
            );
        }

        return $stateDirectory;
    }

    /**
     * Where the supervisor publishes its status document, or null when remote
     * status is disabled. The dashboard reads the same key through this
     * method, so publisher and reader cannot drift apart.
     *
     * The connection is resolved without read_bearer_token: publishing is a
     * key/value write, which a read-only credential cannot perform.
     *
     * @param array<string, mixed> $raw the `queen.supervisor` configuration
     * @param array<string, mixed> $queen the `queen` configuration
     * @param array<string, mixed> $queueConnections the `queue.connections` configuration
     * @return array{connection: array{url: string, urls: list<string>, bearer_token: ?string, headers: array<string, string>}, namespace: string, key: string, interval: mixed, ttl: mixed}|null
     */
    public static function remoteStatusSettings(array $raw, array $queen, array $queueConnections): ?array
    {
        $settings = $raw['remote_status'] ?? null;
        if ($settings === null) {
            return null;
        }
        if (!is_array($settings)) {
            throw new InvalidArgumentException('Queen supervisor remote_status must be an array.');
        }
        if (!self::boolean($settings['enabled'] ?? false, 'remote_status.enabled')) {
            return null;
        }

        $connection = $settings['connection'] ?? 'queen';
        $namespace = $settings['namespace'] ?? 'queen-supervisor';
        $key = $settings['key'] ?? null;
        foreach (['connection' => $connection, 'namespace' => $namespace, 'key' => $key] as $label => $value) {
            if (!is_string($value)) {
                throw new InvalidArgumentException(
                    "Queen supervisor remote_status.{$label} must be a string when remote status is enabled.",
                );
            }
            self::identifier($value, "supervisor remote_status.{$label}");
        }

        return [
            'connection' => self::readConnection(self::connectionConfig($connection, $queen, $queueConnections, [])),
            'namespace' => $namespace,
            'key' => $key,
            'interval' => $settings['interval'] ?? null,
            'ttl' => $settings['ttl'] ?? null,
        ];
    }

    /**
     * Where replicas of the autoscaling pools register, or null when
     * coordination is disabled. Registering is a key/value write, so the
     * connection is resolved without read_bearer_token.
     *
     * @param array<string, mixed> $raw the `queen.supervisor` configuration
     * @param array<string, mixed> $queen the `queen` configuration
     * @param array<string, mixed> $queueConnections the `queue.connections` configuration
     * @return array{connection: array{url: string, urls: list<string>, bearer_token: ?string, headers: array<string, string>}, namespace: string}|null
     */
    public static function coordinationSettings(array $raw, array $queen, array $queueConnections): ?array
    {
        $settings = $raw['coordination'] ?? null;
        if ($settings === null) {
            return null;
        }
        if (!is_array($settings)) {
            throw new InvalidArgumentException('Queen supervisor coordination must be an array.');
        }
        if (!self::boolean($settings['enabled'] ?? false, 'coordination.enabled')) {
            return null;
        }

        $connection = $settings['connection'] ?? 'queen';
        $namespace = $settings['namespace'] ?? 'queen-supervisor';
        foreach (['connection' => $connection, 'namespace' => $namespace] as $label => $value) {
            if (!is_string($value)) {
                throw new InvalidArgumentException(
                    "Queen supervisor coordination.{$label} must be a string when coordination is enabled.",
                );
            }
            self::identifier($value, "supervisor coordination.{$label}");
        }

        return [
            'connection' => self::readConnection(self::connectionConfig($connection, $queen, $queueConnections, [])),
            'namespace' => $namespace,
        ];
    }

    /**
     * The endpoints and read credential the dashboard uses for one of the
     * supervisor's connections: read_bearer_token wins over the connection's
     * token, as it does for the supervisor's depth sampling.
     *
     * @param array<string, mixed> $raw the `queen.supervisor` configuration
     * @param array<string, mixed> $queen the `queen` configuration
     * @param array<string, mixed> $queueConnections the `queue.connections` configuration
     * @return array{url: string, urls: list<string>, bearer_token: ?string, headers: array<string, string>}
     */
    public static function readOnlyConnection(string $name, array $raw, array $queen, array $queueConnections): array
    {
        self::identifier($name, 'dashboard connection');

        return self::readConnection(self::connectionConfig($name, $queen, $queueConnections, $raw));
    }

    /**
     * @param array{connection: array<string, mixed>, namespace: string, key: string, interval: mixed, ttl: mixed} $settings
     * @return array{connection: array<string, mixed>, namespace: string, key: string, interval: int, ttl: int}
     */
    private static function remoteStatusTiming(array $settings, int $pollInterval, int $heartbeatTimeout): array
    {
        $interval = $settings['interval'] === null || $settings['interval'] === ''
            ? $pollInterval
            : self::positiveDuration($settings['interval'], 'remote_status.interval');
        if ($interval >= $heartbeatTimeout) {
            throw new InvalidArgumentException(
                "Queen supervisor remote_status.interval [{$interval}] must be shorter than heartbeat_timeout "
                . "[{$heartbeatTimeout}], or every published document would already look stale.",
            );
        }

        $ttl = $settings['ttl'] === null || $settings['ttl'] === ''
            ? min(86400, max(300, 2 * $heartbeatTimeout))
            : self::positiveInteger($settings['ttl'], 'remote_status.ttl');
        if ($ttl < $heartbeatTimeout || $ttl > 86400) {
            throw new InvalidArgumentException(
                "Queen supervisor remote_status.ttl [{$ttl}] must be at least heartbeat_timeout "
                . "[{$heartbeatTimeout}] and at most 86400 seconds, so a stopped supervisor shows as stale "
                . 'before its document expires.',
            );
        }

        return [
            'connection' => $settings['connection'],
            'namespace' => $settings['namespace'],
            'key' => $settings['key'],
            'interval' => $interval,
            'ttl' => $ttl,
        ];
    }

    private static function connectionConfig(string $name, array $queen, array $queueConnections, array $supervisor): array
    {
        if ($name !== 'queen' && !array_key_exists($name, $queueConnections)) {
            throw new InvalidArgumentException("Supervisor connection [{$name}] is not configured in Laravel.");
        }
        $connection = $queueConnections[$name] ?? [];
        if ($connection !== [] && !is_array($connection)) {
            throw new InvalidArgumentException("Laravel queue connection [{$name}] must be an array.");
        }
        if (($connection['driver'] ?? 'queen') !== 'queen') {
            throw new InvalidArgumentException("Supervisor connection [{$name}] must use the Queen queue driver.");
        }

        // As QueenConnector builds every queen connection: config/queen.php
        // first, the connection's own keys over it. The supervisor checks
        // the settings its workers will run with. A connection that names
        // its own url, and no urls, is not on the default cluster.
        if (array_key_exists('url', $connection) && !array_key_exists('urls', $connection)) {
            unset($queen['urls']);
        }
        $resolved = array_replace($queen, $connection);
        if (array_key_exists('read_bearer_token', $supervisor) && $supervisor['read_bearer_token'] !== null) {
            $resolved['bearer_token'] = $supervisor['read_bearer_token'];
            if (is_array($resolved['headers'] ?? null)) {
                foreach (array_keys($resolved['headers']) as $header) {
                    if (is_string($header) && strcasecmp($header, 'Authorization') === 0) {
                        unset($resolved['headers'][$header]);
                    }
                }
            }
        }
        return $resolved;
    }

    /**
     * The stripes QueenConnector pushes to: `<partition_prefix>-0000` to
     * `<partition_prefix>-<partitions - 1>`.
     *
     * @return array{prefix: string, count: int}
     */
    private static function stripes(string $name, array $connection): array
    {
        $count = $connection['partitions'] ?? 64;
        if (is_string($count) && ctype_digit($count)) {
            $count = (int) $count;
        }
        if (!is_int($count) || $count < 1 || $count > QueenQueue::MAX_PARTITIONS) {
            throw new InvalidArgumentException(
                "Queen connection [{$name}] partitions must be 1 to " . QueenQueue::MAX_PARTITIONS . ' for event_driven.',
            );
        }
        $prefix = $connection['partition_prefix'] ?? 'laravel';
        if (!is_string($prefix) || trim($prefix) === '' || preg_match('/[\x00-\x1F\x7F]/', $prefix) === 1) {
            throw new InvalidArgumentException("Queen connection [{$name}] partition_prefix must be a non-empty string without control characters.");
        }

        return ['prefix' => $prefix, 'count' => $count];
    }

    private static function readConnection(array $connection): array
    {
        $urls = $connection['urls'] ?? null;
        $urls = is_string($urls) ? array_map('trim', explode(',', $urls)) : $urls;
        $urls = is_array($urls) ? array_values(array_filter($urls, fn ($url) => is_string($url) && trim($url) !== '')) : [];
        if ($urls === []) {
            $urls = [(string) ($connection['url'] ?? 'http://localhost:6632')];
        }
        foreach ($urls as &$url) {
            $url = rtrim(trim($url), '/');
            $parts = parse_url($url);
            if (!filter_var($url, FILTER_VALIDATE_URL)
                || !is_array($parts)
                || !in_array(strtolower((string) ($parts['scheme'] ?? '')), ['http', 'https'], true)
                || !is_string($parts['host'] ?? null)
                || $parts['host'] === ''
                || isset($parts['user'])
                || isset($parts['pass'])
                || isset($parts['query'])
                || isset($parts['fragment'])) {
                throw new InvalidArgumentException(
                    'Invalid Queen supervisor URL [' . self::redactedUrl($url) . ']: use http or https with a host,'
                    . ' and no user info, query or fragment.',
                );
            }
        }
        unset($url);

        $headers = $connection['headers'] ?? [];
        if (!is_array($headers)) {
            throw new InvalidArgumentException('Queen supervisor connection headers must be an array.');
        }
        foreach ($headers as $header => $value) {
            if (!is_string($header)
                || preg_match('/^[!#$%&\'*+\-.^_`|~0-9A-Za-z]+$/D', $header) !== 1
                || !is_scalar($value)
                // Match the HTTP HeaderValue contract used by the Rust
                // engine: horizontal tab is permitted, other controls and
                // DEL are not.
                || preg_match('/[\x00-\x08\x0A-\x1F\x7F]/', (string) $value) === 1) {
                throw new InvalidArgumentException('Queen supervisor connection headers must contain valid scalar values.');
            }
            $headers[$header] = (string) $value;
        }

        $bearerToken = $connection['bearer_token'] ?? null;
        if ($bearerToken !== null && (
            !is_string($bearerToken)
            || $bearerToken === ''
            || preg_match('/[\x00-\x20\x7F]/', $bearerToken) === 1
        )) {
            throw new InvalidArgumentException('Queen supervisor bearer_token must be a non-empty header-safe string or null.');
        }

        return [
            'url' => $urls[0],
            'urls' => $urls,
            'bearer_token' => $bearerToken,
            'headers' => $headers,
        ];
    }

    /**
     * Scheme, host and port of a refused URL. The error reaches logs and error
     * pages, and the user info or query is often why the URL was refused.
     */
    private static function redactedUrl(string $url): string
    {
        $parts = parse_url($url);
        if (!is_array($parts) || !is_string($parts['host'] ?? null) || $parts['host'] === '') {
            return 'unreadable URL';
        }

        return ($parts['scheme'] ?? '') . '://' . $parts['host'] . (isset($parts['port']) ? ':' . $parts['port'] : '');
    }

    private static function connectionBackendCount(array $connection): int
    {
        $urls = $connection['urls'] ?? null;
        if (is_string($urls)) {
            $urls = array_filter(array_map('trim', explode(',', $urls)), fn (string $url): bool => $url !== '');
        }

        return is_array($urls) && $urls !== [] ? count($urls) : 1;
    }

    /** @param list<int> $values */
    private static function sumIsBelow(array $values, int $limit): bool
    {
        $sum = 0;
        foreach ($values as $value) {
            if ($value >= $limit - $sum) {
                return false;
            }
            $sum += $value;
        }

        return true;
    }

    private static function identifier(string $value, string $label): void
    {
        if (trim($value) === ''
            || strlen($value) > self::MAX_IDENTIFIER_BYTES
            || preg_match('/[\x00-\x1F\x7F]/', $value)
            || preg_match('//u', $value) !== 1) {
            throw new InvalidArgumentException(
                "Queen {$label} must be a non-empty UTF-8 value of at most "
                . self::MAX_IDENTIFIER_BYTES . ' bytes without control characters.',
            );
        }
    }

    private static function queueName(string $value, string $supervisor): void
    {
        if (trim($value) === ''
            || strlen($value) > self::MAX_QUEUE_NAME_BYTES
            || str_contains($value, ',')
            || preg_match('/[\x00-\x1F\x7F]/', $value)
            || preg_match('//u', $value) !== 1) {
            throw new InvalidArgumentException("Queen supervisor [{$supervisor}] has an invalid queue name [{$value}].");
        }
    }

    private static function boolean(mixed $value, string $label): bool
    {
        if (!is_bool($value)) {
            throw new InvalidArgumentException("Queen supervisor {$label} must be a boolean.");
        }

        return $value;
    }

    private static function positiveInteger(mixed $value, string $label): int
    {
        if (filter_var($value, FILTER_VALIDATE_INT) === false || (int) $value < 1) {
            throw new InvalidArgumentException("Queen supervisor {$label} must be a positive integer.");
        }
        return (int) $value;
    }

    private static function nonNegativeInteger(mixed $value, string $label): int
    {
        if (filter_var($value, FILTER_VALIDATE_INT) === false || (int) $value < 0) {
            throw new InvalidArgumentException("Queen supervisor {$label} must be a non-negative integer.");
        }
        return (int) $value;
    }

    private static function positiveDuration(mixed $value, string $label): int
    {
        $duration = self::positiveInteger($value, $label);
        if ($duration > self::MAX_DURATION_SECONDS) {
            throw new InvalidArgumentException(
                "Queen supervisor {$label} may not exceed " . self::MAX_DURATION_SECONDS . ' seconds.',
            );
        }

        return $duration;
    }

    private static function nonNegativeDuration(mixed $value, string $label): int
    {
        $duration = self::nonNegativeInteger($value, $label);
        if ($duration > self::MAX_DURATION_SECONDS) {
            throw new InvalidArgumentException(
                "Queen supervisor {$label} may not exceed " . self::MAX_DURATION_SECONDS . ' seconds.',
            );
        }

        return $duration;
    }

    private static function positiveFloat(mixed $value, string $label): float
    {
        $number = is_numeric($value) ? (float) $value : NAN;
        if (!is_finite($number)
            || $number < self::MIN_SCALING_SECONDS
            || $number > self::MAX_DURATION_SECONDS) {
            throw new InvalidArgumentException(
                "Queen supervisor {$label} must be between " . self::MIN_SCALING_SECONDS
                . ' and ' . self::MAX_DURATION_SECONDS . ' seconds.',
            );
        }
        return $number;
    }
}
