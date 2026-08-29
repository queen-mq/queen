<?php

namespace Queen\Laravel\Supervisor;

use InvalidArgumentException;

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
        $maximumControlLoopSeconds = $pollInterval;
        foreach ($supervisors as $name => $options) {
            $name = (string) $name;
            self::identifier($name, 'supervisor name');
            if (!is_array($options)) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] must be an array.");
            }

            $connection = (string) ($options['connection'] ?? 'queen');
            $consumerGroup = (string) ($options['consumer_group'] ?? $queen['consumer_group'] ?? 'laravel');
            self::identifier($connection, "supervisor [{$name}] connection");
            self::identifier($consumerGroup, "supervisor [{$name}] consumer_group");

            $queues = $options['queues'] ?? $options['queue'] ?? [$queen['queue'] ?? 'default'];
            $queues = is_string($queues) ? array_map('trim', explode(',', $queues)) : $queues;
            if (!is_array($queues)) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] queues must be an array or comma-separated string.");
            }
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
            $connectionConfig = self::connectionConfig($connection, $queen, $queueConnections, $raw);
            $retryAfter = self::positiveDuration(
                $options['retry_after'] ?? $connectionConfig['retry_after'] ?? $queen['retry_after'] ?? 90,
                "supervisor [{$name}] retry_after",
            );
            if ($retryAfter <= $timeout) {
                throw new InvalidArgumentException("Queen supervisor [{$name}] retry_after must be longer than timeout.");
            }
            $prefetch = self::positiveInteger(
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
                    "Queen supervisor [{$name}] connection prefetch [{$prefetch}] requires lease_renewal.",
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
        }

        $totalMaxProcesses = array_sum(array_column($resolved, 'max_processes'));
        if ($totalMaxProcesses > $processLimit) {
            throw new InvalidArgumentException("Queen supervisors exceed the aggregate process_limit [{$processLimit}].");
        }
        $timeSupervisors = count(array_filter(
            $resolved,
            static fn (array $options): bool => $options['strategy'] === 'time' && $options['balance'] !== 'simple',
        ));
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

        $resolved = array_replace($name === 'queen' ? $queen : [], $connection);
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
                throw new InvalidArgumentException("Invalid Queen supervisor URL [{$url}].");
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
