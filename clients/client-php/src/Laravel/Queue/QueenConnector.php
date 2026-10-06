<?php

namespace Queen\Laravel\Queue;

use Illuminate\Queue\Connectors\ConnectorInterface;
use InvalidArgumentException;
use Queen\Queen;

class QueenConnector implements ConnectorInterface
{
    /** The broker wire encodes lease horizons as signed int32 seconds. */
    public const MAX_RETRY_AFTER_SECONDS = 2_147_483_647;

    /** A stopping worker must never spend its whole shutdown grace on a tail release. */
    private const SHUTDOWN_RELEASE_TIMEOUT_MILLIS = 2_000;

    /** @param (\Closure(string, \Closure(): mixed): mixed)|null $failedJobRetryHandler */
    public function __construct(
        private array $defaults = [],
        private ?\Closure $failedJobRetryHandler = null,
        private ?LeaseRenewerFactory $leaseRenewers = null,
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
            QueenQueue::MAX_PARTITIONS,
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
        // "auto" sizes each pop from the jobs' runtime (AdaptiveBatch), up to
        // its ceiling; every rule for a prefetch above 1 applies to it.
        $adaptivePrefetch = AdaptiveBatch::isAuto($config['prefetch'] ?? 1);
        $prefetch = $adaptivePrefetch
            ? AdaptiveBatch::CEILING
            : self::boundedInteger($config['prefetch'] ?? 1, 'prefetch', 1, 1000);
        $prefetchLabel = $adaptivePrefetch ? 'auto' : (string) $prefetch;
        $ackBatch = self::boundedInteger($config['ack_batch'] ?? 1, 'ack_batch', 1, $prefetch);
        $bulkBatch = self::boundedInteger($config['bulk_batch'] ?? 100, 'bulk_batch', 1, 1000);
        $ackAsync = self::boolean($config['ack_async'] ?? false, 'ack_async');
        $popAhead = self::boolean($config['pop_ahead'] ?? false, 'pop_ahead');
        if ($ackAsync && $ackBatch > 1) {
            throw new InvalidArgumentException('Queen Laravel ack_async requires ack_batch 1: a batch already defers its ACKs.');
        }
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
                "Queen Laravel prefetch [{$prefetchLabel}] requires lease_renewal so every prefetched lease remains fenced while Laravel executes synchronous job code.",
            );
        }
        // A batch popped ahead is a local tail too.
        if ($popAhead && !$leaseRenewal && !array_key_exists('handler', $config)) {
            throw new InvalidArgumentException(
                'Queen Laravel pop_ahead requires lease_renewal so the batch it pops ahead remains fenced.',
            );
        }
        $leaseRenewalTiming = LeaseRenewerFactory::timing($config, $retryAfter);

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
        // back to lease expiry when it fails. Give that final transaction one
        // bounded attempt on one backend, independently of the ordinary
        // client's retry/failover policy, so WorkerStopping can never consume
        // the supervisor's entire shutdown grace.
        $shutdownClientConfig = $clientConfig;
        $shutdownClientConfig['timeoutMillis'] = self::SHUTDOWN_RELEASE_TIMEOUT_MILLIS;
        $shutdownClientConfig['retryAttempts'] = 1;
        $shutdownClientConfig['retryDelayMillis'] = 0;
        $shutdownClientConfig['enableFailover'] = false;
        $shutdownClientConfig['retry429'] = ['maxAttempts' => 1, 'baseMs' => 1, 'capMs' => 1];
        $shutdownQueen = null;
        $shutdownClient = static function () use (&$shutdownQueen, $shutdownClientConfig): Queen {
            return $shutdownQueen ??= new Queen($shutdownClientConfig);
        };

        $leaseRenewer = $leaseRenewal
            ? ($this->leaseRenewers ?? new LeaseRenewerFactory())->make($clientConfig, $retryAfter, $leaseRenewalTiming)
            : null;

        return new QueenQueue(
            new Queen($clientConfig),
            defaultQueue: $defaultQueue,
            consumerGroup: $consumerGroup,
            partitionCount: $partitionCount,
            partitionPrefix: $partitionPrefix,
            retryAfter: $retryAfter,
            blockFor: $blockFor,
            dispatchAfterCommit: $dispatchAfterCommit,
            prefetch: $prefetch,
            ackBatch: $ackBatch,
            ackAsync: $ackAsync,
            popAhead: $popAhead,
            bulkBatch: $bulkBatch,
            popAutopilot: $popAutopilot,
            leaseRenewer: $leaseRenewer,
            failedJobRetryHandler: $this->failedJobRetryHandler,
            shutdownClient: $shutdownClient,
            adaptiveBatch: $adaptivePrefetch ? new AdaptiveBatch() : null,
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

    /**
     * An integer setting in [$minimum, $maximum], from an int or a decimal
     * string; anything else throws, naming the setting.
     *
     * @internal Shared with LeaseRenewerFactory and queen:consume.
     */
    public static function boundedInteger(mixed $value, string $label, int $minimum, int $maximum): int
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
}
