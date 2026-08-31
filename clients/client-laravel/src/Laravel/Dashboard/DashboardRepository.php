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

    private const MAX_PROCESS_LIMIT = 4096;

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
        $state = in_array($raw['state'] ?? null, ['starting', 'running', 'paused', 'terminating', 'stopped'], true)
            ? $raw['state']
            : 'unknown';
        $draining = $this->strictBoundedInteger(
            $raw['draining'] ?? null,
            0,
            $configuration['process_limit'],
        );
        $processBudget = $this->processBudget(
            $raw['process_budget'] ?? null,
            $configuration,
            $pools,
            $draining,
        );
        $allPoolsReady = $pools !== []
            && !in_array(false, array_column($pools, 'ready'), true);
        $allPoolsHealthy = $pools !== []
            && !in_array(false, array_column($pools, 'healthy'), true);
        $allCapacitySatisfied = $pools !== []
            && !in_array(false, array_column($pools, 'capacity_satisfied'), true);
        $ready = $live
            && $state === 'running'
            && ($raw['ready'] ?? null) === true
            && $allPoolsReady;
        $capacitySatisfied = ($raw['capacity_satisfied'] ?? null) === true
            && $allCapacitySatisfied;

        return [
            'availability' => $live ? 'live' : 'stale',
            'engine' => in_array($raw['engine'] ?? null, ['php', 'rust'], true) ? $raw['engine'] : 'unknown',
            'state' => $state,
            'instance_id' => $instanceId,
            'pid' => $this->positiveInteger($raw['pid'] ?? null),
            'updated_at' => gmdate(DATE_ATOM, $epoch),
            'updated_at_epoch' => $epoch,
            'age_seconds' => max(0, $age),
            'workers' => array_sum(array_column($pools, 'processes')),
            'draining' => $draining ?? 0,
            'ready' => $ready,
            'capacity_satisfied' => $capacitySatisfied,
            'processing_healthy' => $ready && $capacitySatisfied && $allPoolsHealthy,
            'process_budget' => $processBudget,
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
            'ready' => false,
            'capacity_satisfied' => false,
            'processing_healthy' => false,
            'process_budget' => $this->unavailableProcessBudget(),
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
        $processLimit = $configuration['process_limit'];

        if (is_array($poolStatus) && array_is_list($poolStatus)) {
            foreach ($poolStatus as $entry) {
                if (!is_array($entry)) {
                    continue;
                }
                $this->appendPool(
                    $result,
                    $seen,
                    $entry['supervisor'] ?? $entry['name'] ?? null,
                    $entry['queue'] ?? null,
                    $entry,
                    $processLimit,
                );
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
                        $this->appendPool($result, $seen, $name, $queue, $queues, $processLimit);
                        continue;
                    }
                    foreach ($queues as $queue => $entry) {
                        if (is_string($queue) && is_array($entry)) {
                            $this->appendPool($result, $seen, $supervisor, $queue, $entry, $processLimit);
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
                $this->appendPool($result, $seen, $supervisor['name'] ?? null, $queue, [], $processLimit);
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
    private function appendPool(
        array &$result,
        array &$seen,
        mixed $supervisor,
        mixed $queue,
        array $entry,
        int $processLimit,
    ): void
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
        $reportedProcesses = $this->strictBoundedInteger(
            $entry['running'] ?? $entry['processes'] ?? null,
            0,
            $processLimit,
        );
        $reportedDesired = $this->strictBoundedInteger($entry['desired'] ?? null, 0, $processLimit);
        $reportedDraining = $this->strictBoundedInteger($entry['draining'] ?? null, 0, $processLimit);
        $processes = $reportedProcesses ?? min(count($pids), $processLimit);
        $desired = $reportedDesired ?? $processes;
        $draining = $reportedDraining ?? 0;
        $restartFailures = $this->strictBoundedInteger(
            $entry['restart_failures'] ?? null,
            0,
            $processLimit,
        );
        $depth = $this->nonNegativeInteger($entry['depth'] ?? null);
        $depthAvailable = ($entry['depth_available'] ?? null) === true && $depth !== null;
        $healthy = ($entry['healthy'] ?? null) === true
            && $restartState === 'closed'
            && $restartFailures === 0;
        $ready = ($entry['ready'] ?? null) === true
            && $reportedProcesses !== null
            && $reportedDesired !== null
            && $reportedDraining !== null
            && $depthAvailable
            && ($reportedDesired === 0 || $reportedProcesses > 0);
        $capacitySatisfied = ($entry['capacity_satisfied'] ?? null) === true
            && $reportedProcesses !== null
            && $reportedDesired !== null
            && $reportedProcesses >= $reportedDesired;
        $processCost = $this->strictBoundedInteger(
            $entry['process_cost_per_worker'] ?? null,
            1,
            $processLimit,
        );
        $reservedProcesses = $this->strictBoundedInteger(
            $entry['reserved_processes'] ?? null,
            0,
            $processLimit,
        );
        $renewalHelpers = $this->strictBoundedInteger(
            $entry['renewal_helpers_reserved'] ?? null,
            0,
            $processLimit,
        );
        $workerSpan = $reportedProcesses !== null && $reportedDraining !== null
            ? $reportedProcesses + $reportedDraining
            : null;
        if ($workerSpan === null
            || $processCost === null
            || $reservedProcesses !== $workerSpan * $processCost
            || $renewalHelpers !== $workerSpan * ($processCost - 1)) {
            $processCost = null;
            $reservedProcesses = null;
            $renewalHelpers = null;
        }

        $result[] = [
            'supervisor' => $supervisor,
            'queue' => $queue,
            'processes' => $processes,
            'desired' => $desired,
            'pids' => array_values(array_unique($pids)),
            'draining_pids' => array_values(array_unique($drainingPids)),
            'draining' => $draining,
            'healthy' => $healthy,
            'ready' => $ready,
            'capacity_satisfied' => $capacitySatisfied,
            'restart_failures' => $restartFailures ?? 0,
            'restart_state' => in_array($restartState, ['closed', 'backoff', 'open', 'probe'], true)
                ? $restartState
                : 'closed',
            'restart_in_seconds' => $this->nonNegativeInteger($entry['restart_in_seconds'] ?? null),
            'depth_available' => $depthAvailable,
            'depth' => $depth,
            'process_cost_per_worker' => $processCost,
            'reserved_processes' => $reservedProcesses,
            'renewal_helpers_reserved' => $renewalHelpers,
        ];
    }

    /**
     * @param array<string, mixed> $configuration
     * @param list<array<string, mixed>> $pools
     * @return array<string, bool|int|null>
     */
    private function processBudget(
        mixed $raw,
        array $configuration,
        array $pools,
        ?int $reportedDraining,
    ): array
    {
        $unavailable = $this->unavailableProcessBudget($configuration['process_limit'] ?? null);
        if (!is_array($raw) || array_is_list($raw)) {
            return $unavailable;
        }

        $limit = $this->strictBoundedInteger($raw['limit'] ?? null, 1, self::MAX_PROCESS_LIMIT);
        $used = $this->strictBoundedInteger($raw['used'] ?? null, 0, self::MAX_PROCESS_LIMIT);
        $available = $this->strictBoundedInteger($raw['available'] ?? null, 0, self::MAX_PROCESS_LIMIT);
        $activeWorkers = $this->strictBoundedInteger(
            $raw['active_worker_processes'] ?? null,
            0,
            self::MAX_PROCESS_LIMIT,
        );
        $drainingWorkers = $this->strictBoundedInteger(
            $raw['draining_worker_processes'] ?? null,
            0,
            self::MAX_PROCESS_LIMIT,
        );
        $renewalHelpers = $this->strictBoundedInteger(
            $raw['renewal_helpers_reserved'] ?? null,
            0,
            self::MAX_PROCESS_LIMIT,
        );
        $configuredLimit = $configuration['process_limit'] ?? null;
        $poolHelpers = array_column($pools, 'renewal_helpers_reserved');
        $poolBudgetKnown = !in_array(null, $poolHelpers, true);
        $expectedActiveWorkers = array_sum(array_column($pools, 'processes'));
        $expectedDrainingWorkers = array_sum(array_column($pools, 'draining'));
        $expectedHelpers = $poolBudgetKnown ? array_sum($poolHelpers) : null;

        if ($limit === null
            || $used === null
            || $available === null
            || $activeWorkers === null
            || $drainingWorkers === null
            || $renewalHelpers === null
            || $limit !== $configuredLimit
            || $used > $limit
            || $available !== $limit - $used
            || $used !== $activeWorkers + $drainingWorkers + $renewalHelpers
            || $reportedDraining === null
            || $drainingWorkers !== $reportedDraining
            || $activeWorkers !== $expectedActiveWorkers
            || $drainingWorkers !== $expectedDrainingWorkers
            || !$poolBudgetKnown
            || $renewalHelpers !== $expectedHelpers) {
            return $unavailable;
        }

        return [
            'valid' => true,
            'limit' => $limit,
            'used' => $used,
            'available' => $available,
            'active_worker_processes' => $activeWorkers,
            'draining_worker_processes' => $drainingWorkers,
            'renewal_helpers_reserved' => $renewalHelpers,
        ];
    }

    /** @return array<string, bool|int|null> */
    private function unavailableProcessBudget(mixed $configuredLimit = null): array
    {
        return [
            'valid' => false,
            'limit' => $this->strictBoundedInteger($configuredLimit, 1, self::MAX_PROCESS_LIMIT),
            'used' => null,
            'available' => null,
            'active_worker_processes' => null,
            'draining_worker_processes' => null,
            'renewal_helpers_reserved' => null,
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

    private function strictBoundedInteger(mixed $value, int $minimum, int $maximum): ?int
    {
        return is_int($value) && $value >= $minimum && $value <= $maximum
            ? $value
            : null;
    }

    private function boundedInteger(mixed $value, int $default, int $minimum, int $maximum): int
    {
        $integer = $this->nonNegativeInteger($value);

        return $integer !== null && $integer >= $minimum && $integer <= $maximum ? $integer : $default;
    }
}
