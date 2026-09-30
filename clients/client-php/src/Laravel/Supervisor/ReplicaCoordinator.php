<?php

namespace Queen\Laravel\Supervisor;

use Queen\Queen;
use Queen\Support\KvOp;

/**
 * Lets replicas of one autoscaling pool, on several hosts or pods, find each
 * other through the broker's key/value store, so each one runs a share of the
 * fleet target instead of all of it (see AutoScaler::desired).
 *
 *   coordination/v1/<scope>/<instance_id>   {instance_id, hostname}, TTL
 *
 * The scope names the work, not the deployment: the broker endpoints, the
 * consumer group and the set of queues. Replicas coordinate only when all
 * three are identical; a pool on another broker or queue set, or a fixed
 * pool, keeps its own sizing.
 *
 * Every poll renews this instance's key and lists the scope in the same call.
 * The key expires after the TTL, the control-loop bound (at most the
 * heartbeat timeout): a live supervisor renews it on every iteration, while a
 * crashed one drops out within it.
 * A supervisor that pauses or stops deletes its keys at once, so the others
 * take over its share without waiting.
 *
 * Coordination is best effort. When the broker cannot be reached, the last
 * view is used until it is as old as the TTL, and then the supervisor sizes
 * its pools alone, as a single replica does: more workers than needed, never
 * fewer. A failure is reported once per failure streak.
 *
 * The Rust engine implements the same keys in supervisor/src/coordination.rs.
 */
final class ReplicaCoordinator
{
    public const PREFIX = 'coordination/v1/';

    /**
     * Replicas listed per pool. The broker counts a getPrefix limit against
     * QUEEN_KV_MAX_KEYS_PER_CALL (1024 by default), so POOLS_PER_CALL pools
     * fit in one call.
     */
    public const MEMBER_LIMIT = 100;

    public const POOLS_PER_CALL = 4;

    private const INSTANCE_PATTERN = '/\A[0-9a-f]{16,128}\z/D';

    /** @var array<string, array{members: list<string>, at: float}> */
    private array $views = [];

    private bool $failing = false;

    public function __construct(
        private Queen $queen,
        private string $namespace,
        private int $ttlSeconds,
        private string $instanceId,
        private ?string $hostname = null,
        private ?\Closure $output = null,
        private ?\Closure $clock = null,
    ) {
        if (preg_match(self::INSTANCE_PATTERN, $instanceId) !== 1) {
            throw new \InvalidArgumentException('Queen supervisor coordination needs a valid instance id.');
        }
        if ($ttlSeconds < 1) {
            throw new \InvalidArgumentException('Queen supervisor coordination TTL must be positive.');
        }
    }

    /**
     * The scope of a pool: FNV-1a 64 of the sorted endpoint URLs separated by
     * spaces, then the consumer group and the sorted queue names, one per
     * line. None of them may contain a space or a line break where it is used
     * as the separator.
     *
     * @param list<string> $endpoints the depth connection's broker URLs
     * @param list<string> $queues
     */
    public static function scope(array $endpoints, string $consumerGroup, array $queues): string
    {
        sort($endpoints, SORT_STRING);
        sort($queues, SORT_STRING);

        return hash('fnv1a64', implode(' ', $endpoints) . "\n" . $consumerGroup . "\n" . implode("\n", $queues));
    }

    /**
     * Renew this instance in every scope and read the replicas of each.
     *
     * @param list<string> $scopes
     */
    public function heartbeat(array $scopes): void
    {
        $scopes = array_values(array_unique($scopes));
        $now = $this->now();
        try {
            foreach (array_chunk($scopes, self::POOLS_PER_CALL) as $chunk) {
                $operations = [];
                foreach ($chunk as $scope) {
                    $operations[] = KvOp::put($this->namespace, $this->memberKey($scope), [
                        'instance_id' => $this->instanceId,
                        'hostname' => $this->hostname,
                    ], ['ttlSeconds' => $this->ttlSeconds]);
                    $operations[] = KvOp::getPrefix($this->namespace, $this->scopePrefix($scope), [
                        'keysOnly' => true,
                        'limit' => self::MEMBER_LIMIT,
                    ]);
                }
                $results = $this->queen->kv()->batch($operations)['results'] ?? null;
                if (!is_array($results) || count($results) !== count($operations)) {
                    throw new \RuntimeException('the broker did not answer every coordination operation');
                }
                foreach ($chunk as $index => $scope) {
                    if (($results[2 * $index]['applied'] ?? null) !== true) {
                        throw new \RuntimeException('the broker did not renew this replica');
                    }
                    $this->views[$scope] = ['members' => $this->members($scope, $results[2 * $index + 1]), 'at' => $now];
                }
            }
        } catch (\Throwable $exception) {
            if (!$this->failing) {
                $this->failing = true;
                $this->emit(
                    "Queen supervisor replica coordination failed: {$exception->getMessage()}. Pools keep the last "
                    . "known replicas for up to {$this->ttlSeconds} seconds, then size themselves alone.\n",
                    'err',
                );
            }

            return;
        }

        if ($this->failing) {
            $this->failing = false;
            $this->emit("Queen supervisor replica coordination recovered.\n", 'out');
        }
    }

    /**
     * Leave every scope at once, so the other replicas take over this share
     * without waiting for the TTL. Best effort: a key left behind expires.
     *
     * @param list<string> $scopes
     */
    public function leave(array $scopes): void
    {
        $scopes = array_values(array_unique($scopes));
        foreach ($scopes as $scope) {
            unset($this->views[$scope]);
        }
        if ($scopes === []) {
            return;
        }
        try {
            $this->queen->kv()->batch(array_map(
                fn (string $scope): array => KvOp::delete($this->namespace, $this->memberKey($scope)),
                $scopes,
            ));
        } catch (\Throwable) {
            // The keys expire with their TTL.
        }
    }

    /**
     * This instance's position among the live replicas of a scope, as
     * AutoScaler::desired takes it; alone when no current view exists.
     *
     * @return array{0: int, 1: int} [replica, replicas]
     */
    public function position(string $scope): array
    {
        $view = $this->views[$scope] ?? null;
        if ($view === null || $this->now() - $view['at'] > $this->ttlSeconds) {
            return [0, 1];
        }
        $replica = array_search($this->instanceId, $view['members'], true);

        return [is_int($replica) ? $replica : 0, count($view['members'])];
    }

    /**
     * Sorted instance ids of a getPrefix answer, this instance included even
     * when the listing ran before its own renewal.
     *
     * @return list<string>
     */
    private function members(string $scope, mixed $result): array
    {
        $rows = is_array($result) ? ($result['rows'] ?? null) : null;
        if (!is_array($rows) || !array_is_list($rows)) {
            throw new \RuntimeException('the broker returned a malformed replica listing');
        }
        $prefix = $this->scopePrefix($scope);
        $members = [$this->instanceId => true];
        foreach ($rows as $row) {
            $key = is_array($row) ? ($row['key'] ?? null) : null;
            if (is_string($key) && str_starts_with($key, $prefix)) {
                $instance = substr($key, strlen($prefix));
                if (preg_match(self::INSTANCE_PATTERN, $instance) === 1) {
                    $members[$instance] = true;
                }
            }
        }
        $members = array_map('strval', array_keys($members));
        sort($members, SORT_STRING);

        return $members;
    }

    private function scopePrefix(string $scope): string
    {
        return self::PREFIX . $scope . '/';
    }

    private function memberKey(string $scope): string
    {
        return $this->scopePrefix($scope) . $this->instanceId;
    }

    private function now(): float
    {
        return $this->clock !== null ? (float) ($this->clock)() : microtime(true);
    }

    private function emit(string $buffer, string $type): void
    {
        if ($this->output !== null) {
            ($this->output)($buffer, $type);
        }
    }
}
