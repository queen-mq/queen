<?php

namespace Queen\Laravel\Queue;

use Illuminate\Queue\Connectors\ConnectorInterface;
use InvalidArgumentException;
use Queen\Queen;

class QueenConnector implements ConnectorInterface
{
    public function __construct(private array $defaults = [])
    {
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
            PHP_INT_MAX,
        );
        $blockFor = self::boundedInteger(
            is_string($workerBlockFor) && $workerBlockFor !== ''
                ? $workerBlockFor
                : ($config['block_for'] ?? 0),
            'block_for',
            0,
            intdiv(PHP_INT_MAX - 5000, 1000),
        );
        $dispatchAfterCommit = self::boolean($config['after_commit'] ?? false, 'after_commit');

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
}
