<?php

namespace Queen\Laravel\Queue;

use Illuminate\Queue\Connectors\ConnectorInterface;
use InvalidArgumentException;
use Queen\Queen;

class QueenConnector implements ConnectorInterface
{
    /** PostgreSQL and the broker wire encode lease horizons as signed int32 seconds. */
    private const MAX_RETRY_AFTER_SECONDS = 2_147_483_647;

    /** A stopping worker must never spend its whole shutdown grace on a tail release. */
    private const SHUTDOWN_RELEASE_TIMEOUT_MILLIS = 2_000;

    /** @param (\Closure(string, \Closure(): mixed): mixed)|null $failedJobRetryHandler */
    public function __construct(
        private array $defaults = [],
        private ?\Closure $failedJobRetryHandler = null,
    ) {
    }

    public function connect(array $config): QueenQueue
    {
        $config = array_replace($this->defaults, $config);

        $workerConsumerGroup = getenv('QUEEN_LARAVEL_CONSUMER_GROUP');
        $workerRetryAfter = getenv('QUEEN_LARAVEL_RETRY_AFTER');
        $workerBlockFor = getenv('QUEEN_LARAVEL_BLOCK_FOR');

        $defaultQueue = self::name($config['queue'] ?? 'default', 'queue');
        $consumerGroup = self::name(
            is_string($workerConsumerGroup) && $workerConsumerGroup !== ''
                ? $workerConsumerGroup
                : ($config['consumer_group'] ?? 'laravel'),
            'consumer_group',
        );
        $partitionCount = self::boundedInteger(
            $config['partitions'] ?? 64,
            'partitions',
            1,
            64,
        );
        $partitionPrefix = self::name($config['partition_prefix'] ?? 'laravel', 'partition_prefix');
        $retryAfter = self::boundedInteger(
            is_string($workerRetryAfter) && $workerRetryAfter !== ''
                ? $workerRetryAfter
                : ($config['retry_after'] ?? 90),
            'retry_after',
            1,
            self::MAX_RETRY_AFTER_SECONDS,
        );
        $blockFor = self::boundedInteger(
            is_string($workerBlockFor) && $workerBlockFor !== ''
                ? $workerBlockFor
                : ($config['block_for'] ?? 0),
            'block_for',
            0,
            intdiv(PHP_INT_MAX - 5000, 1000),
        );
        $prefetch = self::boundedInteger($config['prefetch'] ?? 1, 'prefetch', 1, 1000);
        $ackBatch = self::boundedInteger($config['ack_batch'] ?? 1, 'ack_batch', 1, $prefetch);
        $bulkBatch = self::boundedInteger($config['bulk_batch'] ?? 100, 'bulk_batch', 1, 1000);
        $dispatchAfterCommit = self::boolean($config['after_commit'] ?? false, 'after_commit');
        $popAutopilot = self::boolean($config['autopilot'] ?? false, 'autopilot');
        $leaseRenewal = self::boolean($config['lease_renewal'] ?? false, 'lease_renewal');
        // The internal test handler override is exempt, and has to be: a
        // renewer builds its own Queen client, so lease_renewal refuses that
        // override outright below. Without this exemption the two rules would
        // compose into "prefetch above 1 is untestable", which is not what
        // either of them is for. Nothing reaches this branch in production,
        // where no handler is ever injected.
        if ($prefetch > 1 && !$leaseRenewal && !array_key_exists('handler', $config)) {
            throw new InvalidArgumentException(
                "Queen Laravel prefetch [{$prefetch}] requires lease_renewal so every prefetched lease remains fenced while Laravel executes synchronous job code.",
            );
        }
        $leaseRenewalIntervalOption = $config['lease_renewal_interval'] ?? null;
        $leaseRenewalInterval = self::boundedInteger(
            $leaseRenewalIntervalOption === null || $leaseRenewalIntervalOption === ''
                ? max(1, intdiv($retryAfter, 3))
                : $leaseRenewalIntervalOption,
            'lease_renewal_interval',
            1,
            PHP_INT_MAX,
        );
        $leaseRenewalTimeout = self::boundedInteger(
            $config['lease_renewal_timeout'] ?? 5,
            'lease_renewal_timeout',
            1,
            PHP_INT_MAX,
        );
        $leaseRenewalKillGrace = self::boundedInteger(
            $config['lease_renewal_kill_grace'] ?? 2,
            'lease_renewal_kill_grace',
            0,
            PHP_INT_MAX,
        );
        $leaseRenewalSafetyMargin = self::boundedInteger(
            $config['lease_renewal_safety_margin'] ?? 1,
            'lease_renewal_safety_margin',
            1,
            PHP_INT_MAX,
        );

        $urls = $config['urls'] ?? null;
        if (is_string($urls)) {
            $urls = array_values(array_filter(array_map('trim', explode(',', $urls))));
        }

        $retry429 = $config['retry_429'] ?? $config['retry429'] ?? [];
        if (is_array($retry429)) {
            $retry429 = array_filter($retry429, fn ($value) => $value !== null);
        }

        $clientConfig = [
            'bearerToken' => $config['bearer_token'] ?? $config['bearerToken'] ?? null,
            'timeoutMillis' => $config['timeout'] ?? $config['timeoutMillis'] ?? 30000,
            'retryAttempts' => $config['retry_attempts'] ?? $config['retryAttempts'] ?? 3,
            'retryDelayMillis' => $config['retry_delay'] ?? $config['retryDelayMillis'] ?? 1000,
            'loadBalancingStrategy' => $config['load_balancing_strategy'] ?? $config['loadBalancingStrategy'] ?? 'affinity',
            'enableFailover' => $config['enable_failover'] ?? $config['enableFailover'] ?? true,
            'affinityHashRing' => $config['affinity_hash_ring'] ?? $config['affinityHashRing'] ?? 150,
            'healthRetryAfterMillis' => $config['health_retry_after'] ?? $config['healthRetryAfterMillis'] ?? 30000,
            'headers' => $config['headers'] ?? [],
            'retry429' => $retry429,
        ];

        if (!empty($urls)) {
            $clientConfig['urls'] = $urls;
        } else {
            $clientConfig['url'] = $config['url'] ?? 'http://localhost:6632';
        }

        // Test-only Guzzle handler support already provided by the core client.
        if (array_key_exists('handler', $config)) {
            $clientConfig['handler'] = $config['handler'];
        }

        // Graceful shutdown is a best-effort optimization: correctness falls
        // back to lease expiry when it fails. Give that final retry ACK one
        // bounded attempt on the affinity-selected backend, independently of
        // the ordinary client's retry/failover policy, so WorkerStopping can
        // never consume the supervisor's entire shutdown grace.
        $shutdownClientConfig = $clientConfig;
        $shutdownClientConfig['timeoutMillis'] = self::SHUTDOWN_RELEASE_TIMEOUT_MILLIS;
        $shutdownClientConfig['retryAttempts'] = 1;
        $shutdownClientConfig['retryDelayMillis'] = 0;
        $shutdownClientConfig['enableFailover'] = false;
        $shutdownClientConfig['retry429'] = ['maxAttempts' => 1, 'baseMs' => 1, 'capMs' => 1];
        $shutdownQueen = null;
        $shutdownTailReleaser = static function (
            array $messages,
            string $group,
            ?string $affinityKey,
        ) use (&$shutdownQueen, $shutdownClientConfig): array {
            $shutdownQueen ??= new Queen($shutdownClientConfig);

            return $shutdownQueen->ack($messages, true, array_filter([
                'group' => $group,
                'affinityKey' => $affinityKey,
            ], static fn (mixed $value): bool => $value !== null));
        };

        $leaseRenewer = null;
        if ($leaseRenewal) {
            if (array_key_exists('handler', $clientConfig)) {
                throw new InvalidArgumentException(
                    'Queen Laravel lease_renewal cannot use the internal test HTTP handler override.',
                );
            }

            $backendCount = is_array($urls) && $urls !== [] ? count($urls) : 1;
            if ($leaseRenewalTimeout > intdiv(PHP_INT_MAX, $backendCount)) {
                throw new InvalidArgumentException('Queen Laravel lease renewal request budget is too large.');
            }
            $requestBudget = $leaseRenewalTimeout * $backendCount;
            // One scheduled attempt plus one bounded retry must fit before a
            // TERM/KILL fence and the previous lease's safety margin.
            if (!self::sumIsBelow(
                [
                    $leaseRenewalInterval,
                    $requestBudget,
                    $requestBudget,
                    1,
                    $leaseRenewalKillGrace,
                    $leaseRenewalSafetyMargin,
                ],
                $retryAfter,
            )) {
                throw new InvalidArgumentException(
                    'Queen Laravel lease_renewal timing is unsafe: interval + two request budgets + retry + kill grace + safety margin must be shorter than retry_after.',
                );
            }

            $leaseRenewer = new LazyLeaseRenewer(
                static fn (): LeaseRenewer => new ProcessLeaseRenewer(
                    $clientConfig,
                    $retryAfter,
                    $leaseRenewalInterval,
                    $leaseRenewalTimeout,
                    $requestBudget,
                    $leaseRenewalKillGrace,
                    $leaseRenewalSafetyMargin,
                ),
            );
        }

        return new QueenQueue(
            new Queen($clientConfig),
            defaultQueue: $defaultQueue,
            consumerGroup: $consumerGroup,
            // Queen currently checks out at most 64 partitions per pop.
            partitionCount: $partitionCount,
            partitionPrefix: $partitionPrefix,
            retryAfter: $retryAfter,
            blockFor: $blockFor,
            dispatchAfterCommit: $dispatchAfterCommit,
            prefetch: $prefetch,
            ackBatch: $ackBatch,
            bulkBatch: $bulkBatch,
            popAutopilot: $popAutopilot,
            leaseRenewer: $leaseRenewer,
            failedJobRetryHandler: $this->failedJobRetryHandler,
            shutdownTailReleaser: $shutdownTailReleaser,
        );
    }

    private static function name(mixed $value, string $label): string
    {
        if (!is_string($value) || trim($value) === '' || preg_match('/[\x00-\x1F\x7F]/', $value)) {
            throw new InvalidArgumentException(
                "Queen Laravel {$label} must be a non-empty string without control characters.",
            );
        }

        return $value;
    }

    private static function boundedInteger(mixed $value, string $label, int $minimum, int $maximum): int
    {
        if (is_bool($value)) {
            $integer = false;
        } elseif (is_string($value)) {
            $candidate = trim($value);
            if (!preg_match('/^[+-]?\d+$/D', $candidate)) {
                $integer = false;
            } else {
                $negative = str_starts_with($candidate, '-');
                $digits = ltrim($candidate, '+-0');
                $digits = $digits === '' ? '0' : $digits;
                $canonical = $negative && $digits !== '0' ? '-' . $digits : $digits;
                $integer = filter_var($canonical, FILTER_VALIDATE_INT);
            }
        } else {
            $integer = filter_var($value, FILTER_VALIDATE_INT);
        }

        if ($integer === false || $integer < $minimum || $integer > $maximum) {
            $range = $minimum === $maximum
                ? (string) $minimum
                : "{$minimum}..{$maximum}";
            throw new InvalidArgumentException(
                "Queen Laravel {$label} must be an integer in the range {$range}.",
            );
        }

        return $integer;
    }

    private static function boolean(mixed $value, string $label): bool
    {
        if (!is_bool($value)) {
            throw new InvalidArgumentException("Queen Laravel {$label} must be a boolean.");
        }

        return $value;
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
}
