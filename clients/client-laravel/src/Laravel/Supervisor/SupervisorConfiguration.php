<?php

namespace Queen\Laravel\Supervisor;

use InvalidArgumentException;

final class SupervisorConfiguration
{
    public const VERSION = 2;

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

            $timeout = self::positiveInteger($options['timeout'] ?? 60, "supervisor [{$name}] timeout");
            $connectionConfig = self::connectionConfig($connection, $queen, $queueConnections, $raw);
            $retryAfter = self::positiveInteger(
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
            // Laravel handles a prefetched buffer serially. All jobs are
            // leased at pop time, so the final job must still be covered after
            // every earlier job has consumed its full timeout. A renewal helper
            // tracks the shared lease instead and fences the worker before an
            // unsafe expiry, so only non-renewing connections need this bound.
            if (!$leaseRenewal && $prefetch > intdiv($retryAfter - 1, $timeout)) {
                throw new InvalidArgumentException(
                    "Queen supervisor [{$name}] retry_after must be longer than timeout multiplied by connection prefetch [{$prefetch}].",
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

            $restartBackoff = self::nonNegativeInteger($options['restart_backoff'] ?? 1, "supervisor [{$name}] restart_backoff");
            $restartBackoffMax = self::nonNegativeInteger($options['restart_backoff_max'] ?? 30, "supervisor [{$name}] restart_backoff_max");
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
                'balance_cooldown' => self::positiveInteger($options['balance_cooldown'] ?? 3, "supervisor [{$name}] balance_cooldown"),
                'balance_max_shift' => self::positiveInteger($options['balance_max_shift'] ?? 1, "supervisor [{$name}] balance_max_shift"),
                'scale_down_delay' => self::nonNegativeInteger($options['scale_down_delay'] ?? 10, "supervisor [{$name}] scale_down_delay"),
                'restart_backoff' => $restartBackoff,
                'restart_backoff_max' => $restartBackoffMax,
                'stable_after' => self::positiveInteger($options['stable_after'] ?? 60, "supervisor [{$name}] stable_after"),
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

        if (array_sum(array_column($resolved, 'max_processes')) > $processLimit) {
            throw new InvalidArgumentException("Queen supervisors exceed the aggregate process_limit [{$processLimit}].");
        }
        $maximumWorkerTimeout = max(array_column($resolved, 'timeout'));
        $shutdownGrace = self::positiveInteger($raw['shutdown_grace'] ?? $maximumWorkerTimeout + 15, 'shutdown_grace');
        if ($shutdownGrace <= $maximumWorkerTimeout) {
            throw new InvalidArgumentException('Queen supervisor shutdown_grace must be longer than every worker timeout.');
        }

        $defaultConnection = $connections['queen'] ?? reset($connections);
        $stateDirectory = (string) ($raw['state_directory'] ?? $basePath . DIRECTORY_SEPARATOR . 'storage' . DIRECTORY_SEPARATOR . 'queen-supervisor');
        if ($stateDirectory === '') {
            throw new InvalidArgumentException('Queen supervisor state_directory must not be empty.');
        }
        if (!str_starts_with($stateDirectory, DIRECTORY_SEPARATOR)) {
            $stateDirectory = rtrim($basePath, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $stateDirectory;
        }

        return [
            'version' => self::VERSION,
            'cwd' => $basePath,
            'php_binary' => $phpBinary ?? PHP_BINARY,
            'artisan' => $basePath . DIRECTORY_SEPARATOR . 'artisan',
            'state_directory' => $stateDirectory,
            'poll_interval' => self::positiveInteger($raw['poll_interval'] ?? 3, 'poll_interval'),
            'http_timeout' => self::positiveInteger($raw['http_timeout'] ?? 5, 'http_timeout'),
            'shutdown_grace' => $shutdownGrace,
            'process_limit' => $processLimit,
            'telemetry_ttl' => self::positiveInteger($raw['telemetry_ttl'] ?? 300, 'telemetry_ttl'),
            // `queen` remains the default/fallback for early v2 Rust binaries;
            // `connections` is authoritative for each supervisor pool.
            'queen' => $defaultConnection,
            'connections' => $connections,
            'supervisors' => $resolved,
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
                || preg_match('/[\r\n]/', (string) $value) === 1) {
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
        if (trim($value) === '' || preg_match('/[\x00-\x1F\x7F]/', $value)) {
            throw new InvalidArgumentException("Queen {$label} must be a non-empty value without control characters.");
        }
    }

    private static function queueName(string $value, string $supervisor): void
    {
        if (trim($value) === '' || str_contains($value, ',') || preg_match('/[\x00-\x1F\x7F]/', $value)) {
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

    private static function positiveFloat(mixed $value, string $label): float
    {
        if (!is_numeric($value) || !is_finite((float) $value) || (float) $value <= 0) {
            throw new InvalidArgumentException("Queen supervisor {$label} must be a positive number.");
        }
        return (float) $value;
    }
}
