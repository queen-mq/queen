<?php

namespace Queen\Laravel\Dashboard;

use DateTimeInterface;
use Illuminate\Contracts\Config\Repository as ConfigRepository;
use Queen\Laravel\Supervisor\SupervisorState;
use RuntimeException;

/**
 * Builds the dashboard's deliberately narrow, non-sensitive read model.
 *
 * Raw supervisor documents, queue connection configuration, failed payloads,
 * exceptions and backend diagnostics must never cross this boundary.
 */
final class DashboardRepository
{
    private const MAX_POOLS = 256;

    private const MAX_PIDS_PER_POOL = 512;

    /**
     * @param \Closure(int): mixed $failedJobs
     */
    public function __construct(
        private SupervisorState $state,
        private ConfigRepository $config,
        private \Closure $failedJobs,
    ) {
    }

    /** @return array<string, mixed> */
    public function snapshot(): array
    {
        $document = $this->statusDocument();
        $configuration = $this->safeConfiguration($document['configuration'] ?? null);
        $supervisor = $this->supervisor($document, $configuration);
        $configuration ??= ['supervisors' => []];

        return [
            'generated_at' => gmdate(DATE_ATOM),
            'supervisor' => $supervisor,
            'configuration' => $configuration,
            'queues' => $this->queueDepths($configuration, $supervisor['pools']),
            'failed_jobs' => $this->failedJobs(),
        ];
    }

    public function request(string $command, string $expectedInstanceId): string
    {
        if (!in_array($command, ['pause', 'continue', 'terminate'], true)) {
            throw new DashboardConflictException('The requested supervisor action is not supported.');
        }
        if (!$this->validInstanceId($expectedInstanceId)) {
            throw new DashboardConflictException('The supervisor instance identifier is invalid.');
        }

        try {
            // SupervisorState checks heartbeat, owner lock and the selected
            // generation together while holding control.lock. A separate web
            // pre-check would only add a takeover race.
            return $this->state->request($command, $expectedInstanceId);
        } catch (\Throwable) {
            throw new DashboardConflictException(
                'The command was not accepted because the supervisor changed or another command is pending.',
            );
        }
    }

    /** @return array<string, mixed>|null */
    private function statusDocument(): ?array
    {
        try {
            $status = $this->state->status();
        } catch (\Throwable) {
            return null;
        }

        return is_array($status) ? $status : null;
    }

    /**
     * @param array<string, mixed>|null $raw
     * @param array<string, mixed>|null $configuration
     */
    private function supervisor(?array $raw, ?array $configuration): array
    {
        if ($raw === null || $configuration === null) {
            return $this->unavailableSupervisor();
        }
        if (($raw['schema'] ?? null) !== SupervisorState::STATUS_SCHEMA
            || !is_array($raw['pool_status'] ?? null)
            || !array_is_list($raw['pool_status'])) {
            return $this->unavailableSupervisor();
        }

        $instanceId = $raw['instance_id'] ?? null;
        $epoch = $this->heartbeatEpoch($raw);
        if (!$this->validInstanceId($instanceId) || $epoch === null) {
            return $this->unavailableSupervisor();
        }

        $now = time();
        $age = $now - $epoch;
        try {
            $live = $this->state->isLive($raw);
        } catch (\Throwable) {
            $live = false;
        }
        $pools = $this->normalizePools($raw, $configuration);

        return [
            'availability' => $live ? 'live' : 'stale',
            'engine' => in_array($raw['engine'] ?? null, ['php', 'rust'], true) ? $raw['engine'] : 'unknown',
            'state' => in_array($raw['state'] ?? null, ['starting', 'running', 'paused', 'terminating', 'stopped'], true)
                ? $raw['state']
                : 'unknown',
            'instance_id' => $instanceId,
            'pid' => $this->positiveInteger($raw['pid'] ?? null),
            'updated_at' => gmdate(DATE_ATOM, $epoch),
            'updated_at_epoch' => $epoch,
            'age_seconds' => max(0, $age),
            'workers' => array_sum(array_column($pools, 'processes')),
            'draining' => $this->nonNegativeInteger($raw['draining'] ?? null) ?? 0,
            'pools' => $pools,
        ];
    }

    /** @return array<string, mixed> */
    private function unavailableSupervisor(): array
    {
        return [
            'availability' => 'unavailable',
            'engine' => null,
            'state' => null,
            'instance_id' => null,
            'pid' => null,
            'updated_at' => null,
            'updated_at_epoch' => null,
            'age_seconds' => null,
            'workers' => 0,
            'draining' => 0,
            'pools' => [],
        ];
    }

    /** @param array<string, mixed> $raw */
    private function heartbeatEpoch(array $raw): ?int
    {
        $epoch = $this->positiveInteger($raw['updated_at_epoch'] ?? null);
        if ($epoch !== null) {
            return $epoch;
        }

        $timestamp = $raw['updated_at'] ?? null;
        if (!is_string($timestamp) || strlen($timestamp) > 64 || preg_match('/[\x00-\x1F\x7F]/', $timestamp) === 1) {
            return null;
        }
        $parsed = strtotime($timestamp);

        return is_int($parsed) && $parsed > 0 ? $parsed : null;
    }

    /**
     * @param array<string, mixed> $raw
     * @param array<string, mixed> $configuration
     * @return list<array<string, mixed>>
     */
    private function normalizePools(array $raw, array $configuration): array
    {
        $result = [];
        $seen = [];
        $poolStatus = $raw['pool_status'] ?? null;

        if (is_array($poolStatus) && array_is_list($poolStatus)) {
            foreach ($poolStatus as $entry) {
                if (!is_array($entry)) {
                    continue;
                }
                $this->appendPool($result, $seen, $entry['supervisor'] ?? $entry['name'] ?? null, $entry['queue'] ?? null, $entry);
            }
        } else {
            $legacy = $raw['pools'] ?? null;
            if (is_array($legacy)) {
                foreach ($legacy as $supervisor => $queues) {
                    if (!is_string($supervisor) || !is_array($queues)) {
                        continue;
                    }
                    if ($this->looksLikePool($queues)) {
                        [$name, $queue] = $this->splitLegacyPoolKey($supervisor, $configuration);
                        $this->appendPool($result, $seen, $name, $queue, $queues);
                        continue;
                    }
                    foreach ($queues as $queue => $entry) {
                        if (is_string($queue) && is_array($entry)) {
                            $this->appendPool($result, $seen, $supervisor, $queue, $entry);
                        }
                    }
                }
            }
        }

        foreach ($configuration['supervisors'] ?? [] as $supervisor) {
            if (!is_array($supervisor)) {
                continue;
            }
            foreach ($supervisor['queues'] ?? [] as $queue) {
                $this->appendPool($result, $seen, $supervisor['name'] ?? null, $queue, []);
            }
        }

        usort($result, static fn (array $left, array $right): int => [$left['supervisor'], $left['queue']]
            <=> [$right['supervisor'], $right['queue']]);

        return $result;
    }

    /**
     * @param list<array<string, mixed>> $result
     * @param array<string, true> $seen
     * @param array<string, mixed> $entry
     */
    private function appendPool(array &$result, array &$seen, mixed $supervisor, mixed $queue, array $entry): void
    {
        if (count($result) >= self::MAX_POOLS) {
            return;
        }
        $supervisor = $this->safeString($supervisor, 128);
        $queue = $this->safeString($queue, 256);
        if ($supervisor === null || $queue === null) {
            return;
        }
        $key = $supervisor . "\0" . $queue;
        if (isset($seen[$key])) {
            return;
        }
        $seen[$key] = true;

        $pids = [];
        if (is_array($entry['pids'] ?? null)) {
            foreach (array_slice($entry['pids'], 0, self::MAX_PIDS_PER_POOL) as $pid) {
                $pid = $this->positiveInteger($pid);
                if ($pid !== null) {
                    $pids[] = $pid;
                }
            }
        }
        $drainingPids = [];
        if (is_array($entry['draining_pids'] ?? null)) {
            foreach (array_slice($entry['draining_pids'], 0, self::MAX_PIDS_PER_POOL) as $pid) {
                $pid = $this->positiveInteger($pid);
                if ($pid !== null) {
                    $drainingPids[] = $pid;
                }
            }
        }
        $restartState = $entry['restart_state'] ?? null;
        $processes = $this->nonNegativeInteger($entry['running'] ?? $entry['processes'] ?? null) ?? count($pids);

        $result[] = [
            'supervisor' => $supervisor,
            'queue' => $queue,
            'processes' => $processes,
            'desired' => $this->nonNegativeInteger($entry['desired'] ?? null) ?? $processes,
            'pids' => array_values(array_unique($pids)),
            'draining_pids' => array_values(array_unique($drainingPids)),
            'draining' => $this->nonNegativeInteger($entry['draining'] ?? null) ?? 0,
            'restart_failures' => $this->nonNegativeInteger($entry['restart_failures'] ?? null) ?? 0,
            'restart_state' => in_array($restartState, ['closed', 'backoff', 'open', 'probe'], true)
                ? $restartState
                : 'closed',
            'restart_in_seconds' => $this->nonNegativeInteger($entry['restart_in_seconds'] ?? null),
            'depth_available' => ($entry['depth_available'] ?? array_key_exists('depth', $entry)) === true
                && $this->nonNegativeInteger($entry['depth'] ?? null) !== null,
            'depth' => $this->nonNegativeInteger($entry['depth'] ?? null),
        ];
    }

    /** @param array<string, mixed> $entry */
    private function looksLikePool(array $entry): bool
    {
        return array_key_exists('processes', $entry) || array_key_exists('pids', $entry);
    }

    /** @param array<string, mixed> $configuration */
    private function splitLegacyPoolKey(string $key, array $configuration): array
    {
        foreach ($configuration['supervisors'] ?? [] as $supervisor) {
            if (!is_array($supervisor) || !is_string($supervisor['name'] ?? null)) {
                continue;
            }
            foreach ($supervisor['queues'] ?? [] as $queue) {
                if (is_string($queue) && $key === $supervisor['name'] . ':' . $queue) {
                    return [$supervisor['name'], $queue];
                }
            }
        }
        $parts = explode(':', $key, 2);

        return count($parts) === 2 ? $parts : [$key, 'unknown'];
    }

    /** @return array<string, mixed>|null */
    private function safeConfiguration(mixed $configured): ?array
    {
        if (!is_array($configured)) {
            return null;
        }

        $pollInterval = $this->positiveInteger($configured['poll_interval'] ?? null);
        $httpTimeout = $this->positiveInteger($configured['http_timeout'] ?? null);
        $controlTtl = $this->positiveInteger($configured['control_ttl'] ?? null);
        $heartbeatTimeout = $this->positiveInteger($configured['heartbeat_timeout'] ?? null);
        $shutdownGrace = $this->positiveInteger($configured['shutdown_grace'] ?? null);
        $telemetryTtl = $this->positiveInteger($configured['telemetry_ttl'] ?? null);
        $processLimit = $this->positiveInteger($configured['process_limit'] ?? null);
        if ($pollInterval === null
            || $httpTimeout === null
            || $controlTtl === null
            || $controlTtl < 30
            || $controlTtl > 86400
            || $heartbeatTimeout === null
            || $heartbeatTimeout > 86400
            || $shutdownGrace === null
            || $telemetryTtl === null
            || $processLimit === null
            || $processLimit > 4096
            || !is_array($configured['supervisors'] ?? null)
            || !array_is_list($configured['supervisors'])) {
            return null;
        }

        $supervisors = [];
        $remainingPools = self::MAX_POOLS;
        $seenSupervisors = [];

        foreach ($configured['supervisors'] as $options) {
            if (!is_array($options)) {
                return null;
            }
            $name = $this->safeString($options['name'] ?? null, 128);
            $connection = $this->safeString($options['connection'] ?? null, 128);
            $group = $this->safeString($options['consumer_group'] ?? null, 128);
            if ($name === null
                || $connection === null
                || $group === null
                || isset($seenSupervisors[$name])) {
                return null;
            }
            $seenSupervisors[$name] = true;
            $queues = $options['queues'] ?? [];
            if (!is_array($queues) || !array_is_list($queues) || $queues === []) {
                return null;
            }
            $safeQueues = [];
            foreach ($queues as $queue) {
                $queue = $this->safeString($queue, 256);
                if ($queue === null || str_contains($queue, ',') || in_array($queue, $safeQueues, true)) {
                    return null;
                }
                $safeQueues[] = $queue;
            }
            if (count($safeQueues) > $remainingPools) {
                return null;
            }
            $remainingPools -= count($safeQueues);

            $balance = $this->enum($options['balance'] ?? null, ['auto', 'simple', 'off']);
            $strategy = $this->enum($options['strategy'] ?? null, ['size', 'time']);
            $processes = $this->nonNegativeInteger($options['processes'] ?? null);
            $minProcesses = $this->nonNegativeInteger($options['min_processes'] ?? null);
            $maxProcesses = $this->positiveInteger($options['max_processes'] ?? null);
            $timeout = $this->positiveInteger($options['timeout'] ?? null);
            $retryAfter = $this->positiveInteger($options['retry_after'] ?? null);
            $tries = $this->nonNegativeInteger($options['tries'] ?? null);
            $memory = $this->positiveInteger($options['memory'] ?? null);
            if ($balance === null
                || $strategy === null
                || $processes === null
                || $minProcesses === null
                || $maxProcesses === null
                || $minProcesses > $maxProcesses
                || $processes < $minProcesses
                || $processes > $maxProcesses
                || $maxProcesses > $processLimit
                || $timeout === null
                || $retryAfter === null
                || $retryAfter <= $timeout
                || $tries === null
                || $memory === null) {
                return null;
            }

            $supervisors[] = [
                'name' => $name,
                'connection' => $connection,
                'consumer_group' => $group,
                'queues' => $safeQueues,
                'balance' => $balance,
                'strategy' => $strategy,
                'min_processes' => $minProcesses,
                'max_processes' => $maxProcesses,
                'processes' => $processes,
                'timeout' => $timeout,
                'retry_after' => $retryAfter,
                'tries' => $tries,
                'memory' => $memory,
            ];
        }

        usort($supervisors, static fn (array $left, array $right): int => $left['name'] <=> $right['name']);

        return [
            'poll_interval' => $pollInterval,
            'http_timeout' => $httpTimeout,
            'control_ttl' => $controlTtl,
            'heartbeat_timeout' => $heartbeatTimeout,
            'shutdown_grace' => $shutdownGrace,
            'telemetry_ttl' => $telemetryTtl,
            'process_limit' => $processLimit,
            'supervisors' => $supervisors,
        ];
    }

    /**
     * @param array<string, mixed> $configuration
     * @param list<array<string, mixed>> $pools
     * @return list<array<string, mixed>>
     */
    private function queueDepths(array $configuration, array $pools): array
    {
        $result = [];
        $seen = [];
        $poolDepths = [];
        foreach ($pools as $pool) {
            if (!is_array($pool) || !is_string($pool['supervisor'] ?? null) || !is_string($pool['queue'] ?? null)) {
                continue;
            }
            $poolDepths[$pool['supervisor'] . "\0" . $pool['queue']] = $pool;
        }
        foreach ($configuration['supervisors'] ?? [] as $supervisor) {
            if (!is_array($supervisor)) {
                continue;
            }
            foreach ($supervisor['queues'] ?? [] as $queue) {
                $connection = $supervisor['connection'] ?? null;
                $consumerGroup = $supervisor['consumer_group'] ?? null;
                if (!is_string($connection) || !is_string($consumerGroup) || !is_string($queue)) {
                    continue;
                }
                // Queen depth is consumer-group scoped. The same physical
                // queue may legitimately appear more than once here.
                $key = $connection . "\0" . $consumerGroup . "\0" . $queue;
                if (isset($seen[$key]) || count($result) >= self::MAX_POOLS) {
                    continue;
                }
                $seen[$key] = true;
                $pool = $poolDepths[($supervisor['name'] ?? '') . "\0" . $queue] ?? [];
                $available = ($pool['depth_available'] ?? false) === true
                    && $this->nonNegativeInteger($pool['depth'] ?? null) !== null;
                $result[] = [
                    'connection' => $connection,
                    'consumer_group' => $consumerGroup,
                    'queue' => $queue,
                    'available' => $available,
                    'depth' => $available ? $pool['depth'] : null,
                ];
            }
        }

        return $result;
    }

    /** @return array<string, mixed> */
    private function failedJobs(): array
    {
        $limit = $this->boundedInteger(
            $this->config->get('queen.dashboard.failed_jobs_limit', 50),
            50,
            1,
            200,
        );

        try {
            $readModel = ($this->failedJobs)($limit);
            if (!is_array($readModel)
                || !is_int($readModel['total'] ?? null)
                || ($readModel['total'] ?? -1) < 0
                || !is_bool($readModel['total_exact'] ?? null)
                || !is_array($readModel['records'] ?? null)) {
                throw new RuntimeException('Malformed failed-job repository.');
            }
            $items = [];
            foreach (array_slice($readModel['records'], 0, $limit) as $record) {
                $record = is_array($record) ? $record : (is_object($record) ? (array) $record : []);
                $id = $this->safeIdentifier($record['id'] ?? null);
                if ($id === null) {
                    continue;
                }
                $connection = $this->safeString($record['connection'] ?? null, 128);
                $items[] = [
                    'id' => $id,
                    'connection' => $connection,
                    'queue' => $this->safeString($record['queue'] ?? null, 256),
                    'failed_at' => $this->safeTimestamp($record['failed_at'] ?? null),
                    // This is the expected index policy inferred from the
                    // configured connection, not proof that a corresponding
                    // broker DLQ record exists at refresh time.
                    'lifecycle_policy' => $this->failedLifecycle($connection),
                ];
            }

            return [
                'available' => true,
                'total' => $readModel['total'],
                'total_exact' => $readModel['total_exact'],
                'showing' => count($items),
                'limit' => $limit,
                'items' => $items,
            ];
        } catch (\Throwable) {
            return [
                'available' => false,
                'total' => null,
                'total_exact' => false,
                'showing' => 0,
                'limit' => $limit,
                'items' => [],
            ];
        }
    }

    private function safeIdentifier(mixed $value): string|int|null
    {
        if (is_int($value) && $value >= 0) {
            return $value;
        }

        return $this->safeString($value, 256);
    }

    private function failedLifecycle(?string $connection): string
    {
        $connections = $this->config->get('queue.connections', []);
        $driver = is_string($connection)
            && is_array($connections)
            && is_array($connections[$connection] ?? null)
            ? ($connections[$connection]['driver'] ?? null)
            : null;

        return $driver === 'queen' && $this->config->get('queen.sync_failed_jobs', true) === true
            ? 'laravel+queen-dlq'
            : 'laravel';
    }

    private function safeTimestamp(mixed $value): ?string
    {
        if ($value instanceof DateTimeInterface) {
            return $value->format(DATE_ATOM);
        }

        return $this->safeString($value, 64);
    }

    private function safeString(mixed $value, int $maximumBytes): ?string
    {
        if (!is_string($value) || $value === '' || strlen($value) > $maximumBytes) {
            return null;
        }
        if (preg_match('/[\x00-\x1F\x7F]/', $value) === 1 || preg_match('//u', $value) !== 1) {
            return null;
        }

        return $value;
    }

    private function validInstanceId(mixed $value): bool
    {
        return is_string($value)
            && $value !== ''
            && strlen($value) <= 128
            && preg_match('/^[A-Za-z0-9._:-]+$/D', $value) === 1;
    }

    /** @param list<string> $allowed */
    private function enum(mixed $value, array $allowed): ?string
    {
        return is_string($value) && in_array($value, $allowed, true) ? $value : null;
    }

    private function positiveInteger(mixed $value): ?int
    {
        $integer = $this->nonNegativeInteger($value);

        return $integer !== null && $integer > 0 ? $integer : null;
    }

    private function nonNegativeInteger(mixed $value): ?int
    {
        if (is_int($value)) {
            return $value >= 0 ? $value : null;
        }
        if (is_string($value) && preg_match('/^[0-9]+$/D', $value) === 1) {
            $value = filter_var($value, FILTER_VALIDATE_INT);

            return $value !== false ? $value : null;
        }

        return null;
    }

    private function boundedInteger(mixed $value, int $default, int $minimum, int $maximum): int
    {
        $integer = $this->nonNegativeInteger($value);

        return $integer !== null && $integer >= $minimum && $integer <= $maximum ? $integer : $default;
    }
}
