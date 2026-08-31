<?php

namespace Queen;

use Queen\Http\HttpClient;
use Queen\Http\LoadBalancer;
use Queen\Http\Retry429Policy;
use Queen\Buffer\BufferManager;
use Queen\Builders\QueueBuilder;
use Queen\Builders\TransactionBuilder;
use Queen\Support\Defaults;
use Queen\Support\PopAutopilot;

class Queen
{
    private HttpClient $httpClient;
    private BufferManager $bufferManager;
    private array $config;
    private ?Admin $admin = null;
    private ?Kv $kv = null;
    private ?Timers $timers = null;
    private ?Ephemeral $ephemeral = null;
    /** Process-wide kill switch for pop autopilot, settled in the constructor. */
    private bool $autopilotOff = false;

    /**
     * @param string|array $config Single URL string, array of URLs, or config array
     */
    public function __construct(string|array $config = [])
    {
        $this->config = $this->normalizeConfig($config);
        // Pop autopilot: on unless the environment rolls it back. Read ONCE here
        // rather than on every pop — it is a deployment-level rollback, and
        // re-reading it per request would let a running process change wire
        // shape halfway through. A per-builder ->autopilot(..) still outranks it.
        $this->autopilotOff = PopAutopilot::disabledByEnv();
        $this->httpClient = $this->createHttpClient();
        $this->bufferManager = new BufferManager($this->httpClient);
    }

    /**
     * Whether pop autopilot is off for this client because the environment
     * asked (QUEEN_SDK_POP_AUTOPILOT). Read by the builders; a per-call
     * ->autopilot(..) still outranks it.
     */
    public function autopilotOff(): bool
    {
        return $this->autopilotOff;
    }

    // ===========================
    // Queue Builder Entry Point
    // ===========================

    public function queue(?string $name = null): QueueBuilder
    {
        return new QueueBuilder($this, $this->httpClient, $this->bufferManager, $name);
    }

    // ===========================
    // Admin API
    // ===========================

    public function admin(): Admin
    {
        if ($this->admin === null) {
            $this->admin = new Admin($this->httpClient);
        }
        return $this->admin;
    }

    // ===========================
    // Key/Value API
    // ===========================

    /**
     * Transactional key/value state. Present on every broker, like push and
     * pop: there is nothing to switch on first and nothing to probe for. The
     * one thing that can take it away is an operator pulling the runtime kill
     * switch during an incident, which answers 503 `kv_disabled` with a
     * Retry-After and is a temporary condition, not a configuration.
     */
    public function kv(): Kv
    {
        if ($this->kv === null) {
            $this->kv = new Kv($this->httpClient);
        }
        return $this->kv;
    }

    // ===========================
    // Timers API
    // ===========================

    /**
     * Scheduled messages. Always present, like the KV surface. The operator's
     * runtime kill switch can pause the SCHEDULE half (503 `timers_disabled`);
     * cancel and the reads are never blocked by it, so a caller can always stop
     * a timer it has already promised.
     */
    public function timers(): Timers
    {
        if ($this->timers === null) {
            $this->timers = new Timers($this->httpClient);
        }
        return $this->timers;
    }

    // ===========================
    // Ephemeral API
    // ===========================

    /**
     * RAM-class queues (EPHEMERAL_QUEUES.md §1, §3.1). Read the note on the
     * Ephemeral class once: the contents survive NOTHING — treat a failover
     * like a Redis restart — while a declared configuration is durable and
     * comes back empty. Consumption semantics come from the pop's `group`,
     * exactly as on the durable engine; there is no queue-level mode.
     *
     * Unlike kv() and timers(), this one has a version floor: a broker or proxy
     * older than 1.1 has no such routes and answers 404 on all of them, which
     * this surface maps to EphemeralUnsupportedException.
     *
     * One 404 is NOT that: `depth` answers `ephemeral_queue_not_found` for a
     * queue that is not there, and that arrives as
     * EphemeralQueueNotFoundException. The two are told apart by the body's
     * code, never by the status they share.
     */
    public function ephemeral(): Ephemeral
    {
        if ($this->ephemeral === null) {
            $this->ephemeral = new Ephemeral($this->httpClient, $this->bufferManager);
        }
        return $this->ephemeral;
    }

    // ===========================
    // Transaction API
    // ===========================

    public function transaction(): TransactionBuilder
    {
        return new TransactionBuilder($this->httpClient);
    }

    // ===========================
    // Direct ACK API
    // ===========================

    /**
     * @param array|string $message Single message, array of messages, or transaction ID string
     * @param bool|string $status true/false or 'completed'/'failed'
     * @param array $context Optional context (group, error)
     */
    public function ack(array|string $message, bool|string $status = true, array $context = []): array
    {
        $affinityKey = is_string($context['affinityKey'] ?? null) && $context['affinityKey'] !== ''
            ? $context['affinityKey']
            : null;

        // Batch ack
        $isBatch = is_array($message) && (isset($message[0]) || empty($message));

        if ($isBatch) {
            if (empty($message)) {
                return ['processed' => 0, 'results' => []];
            }

            $hasIndividualStatus = false;
            foreach ($message as $msg) {
                if (is_array($msg) && (array_key_exists('_status', $msg) || array_key_exists('_error', $msg))) {
                    $hasIndividualStatus = true;
                    break;
                }
            }

            try {
                $acknowledgments = array_map(function ($msg) use ($status, $context, $hasIndividualStatus) {
                    return $this->buildAck($msg, $status, $context, $hasIndividualStatus);
                }, $message);

                $result = $this->httpClient->post('/api/v1/ack/batch', [
                    'acknowledgments' => $acknowledgments,
                    'consumerGroup' => $context['group'] ?? null,
                ], affinityKey: $affinityKey);

                if (is_array($result) && isset($result['error'])) {
                    return ['success' => false, 'error' => $result['error']];
                }

                return array_merge(['success' => true], $result ?? []);
            } catch (\Throwable $error) {
                return ['success' => false, 'error' => $error->getMessage()];
            }
        }

        // Single ack
        $msg = is_string($message) ? ['transactionId' => $message] : $message;
        $transactionId = $msg['transactionId'] ?? $msg['id'] ?? null;
        $partitionId = $msg['partitionId'] ?? null;
        $leaseId = $msg['leaseId'] ?? null;

        if ($transactionId === null) {
            return ['success' => false, 'error' => 'Message must have transactionId or id property'];
        }

        if ($partitionId === null) {
            return ['success' => false, 'error' => 'Message must have partitionId property to ensure message uniqueness'];
        }

        $statusStr = is_bool($status) ? ($status ? 'completed' : 'failed') : $status;

        $body = [
            'transactionId' => $transactionId,
            'partitionId' => $partitionId,
            'status' => $statusStr,
            'error' => $context['error'] ?? null,
            'consumerGroup' => $context['group'] ?? null,
        ];

        if ($leaseId !== null) {
            $body['leaseId'] = $leaseId;
        }

        try {
            $result = $this->httpClient->post('/api/v1/ack', $body, affinityKey: $affinityKey);

            if (is_array($result) && isset($result['error'])) {
                return ['success' => false, 'error' => $result['error']];
            }

            return array_merge(['success' => true], $result ?? []);
        } catch (\Throwable $error) {
            return ['success' => false, 'error' => $error->getMessage()];
        }
    }

    // ===========================
    // Lease Renewal API
    // ===========================

    /**
     * @param string|array $messageOrLeaseId Lease ID string, message array, or array of messages
     * @param int|null $seconds New lease horizon. Null retains the broker's
     *                          backwards-compatible 60 second default.
     */
    public function renew(string|array $messageOrLeaseId, ?int $seconds = null): array
    {
        if ($seconds !== null && ($seconds < 1 || $seconds > 2_147_483_647)) {
            throw new \InvalidArgumentException('Lease renewal seconds must be in the range 1..2147483647');
        }

        $leaseIds = [];

        if (is_string($messageOrLeaseId)) {
            $leaseIds = [$messageOrLeaseId];
        } elseif (isset($messageOrLeaseId[0])) {
            // Array of messages or lease IDs
            foreach ($messageOrLeaseId as $item) {
                if (is_string($item)) {
                    $leaseIds[] = $item;
                } elseif (is_array($item) && isset($item['leaseId'])) {
                    $leaseIds[] = $item['leaseId'];
                }
            }
        } elseif (isset($messageOrLeaseId['leaseId'])) {
            $leaseIds = [$messageOrLeaseId['leaseId']];
        }

        // Dedupe: with v4 multi-partition pop, all messages in one batch share
        // the same leaseId (one renew_lease_v2 call extends every claimed
        // partition_consumers row).
        $leaseIds = array_values(array_unique($leaseIds));

        if (empty($leaseIds)) {
            return ['success' => false, 'error' => 'No valid lease IDs found for renewal'];
        }

        $results = [];
        foreach ($leaseIds as $leaseId) {
            try {
                $result = $this->httpClient->post(
                    '/api/v1/lease/' . rawurlencode((string) $leaseId) . '/extend',
                    $seconds === null ? [] : ['seconds' => $seconds],
                );
                $renewed = is_array($result) ? ($result['renewed'] ?? null) : null;
                $expires = is_array($result)
                    ? ($result['newExpiresAt'] ?? $result['expiresAt'] ?? $result['lease_expires_at'] ?? null)
                    : null;
                // Only the affected-row count proves that the broker still
                // owned and extended this lease. An expiry string is useful
                // scheduling metadata, but must never turn renewed:0 (or a
                // legacy/ambiguous response without renewed) into success.
                $hasRenewalEvidence = is_int($renewed) && $renewed > 0;
                $hasValidExpiry = $expires === null
                    || is_string($expires) && self::isRfc3339Timestamp($expires);
                if (!is_array($result)
                    || ($result['success'] ?? null) !== true
                    || !$hasRenewalEvidence
                    || !$hasValidExpiry) {
                    $results[] = [
                        'leaseId' => $leaseId,
                        'success' => false,
                        'error' => is_array($result) && is_string($result['error'] ?? null)
                            ? $result['error']
                            : 'Queen rejected or could not verify the lease renewal',
                    ];
                    continue;
                }
                $results[] = [
                    'leaseId' => $leaseId,
                    'success' => true,
                    'newExpiresAt' => $expires,
                ];
            } catch (\Throwable $error) {
                $results[] = ['leaseId' => $leaseId, 'success' => false, 'error' => $error->getMessage()];
            }
        }

        // Return single result if single input
        return is_string($messageOrLeaseId) || !isset($messageOrLeaseId[0])
            ? $results[0]
            : $results;
    }

    private static function isRfc3339Timestamp(string $value): bool
    {
        if (preg_match(
            '/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/D',
            $value,
        ) !== 1) {
            return false;
        }

        try {
            new \DateTimeImmutable($value);
        } catch (\Exception) {
            return false;
        }

        $errors = \DateTimeImmutable::getLastErrors();
        return $errors === false
            || $errors['warning_count'] === 0 && $errors['error_count'] === 0;
    }

    // ===========================
    // Buffer Management
    // ===========================

    public function flushAllBuffers(): void
    {
        $this->bufferManager->flushAllBuffers();
    }

    public function getBufferStats(): array
    {
        return $this->bufferManager->getStats();
    }

    // ===========================
    // Consumer Group Management
    // ===========================

    public function deleteConsumerGroup(string $consumerGroup, bool $deleteMetadata = true): mixed
    {
        $dm = $deleteMetadata ? 'true' : 'false';
        return $this->httpClient->delete('/api/v1/consumer-groups/' . rawurlencode($consumerGroup) . "?deleteMetadata={$dm}");
    }

    public function updateConsumerGroupTimestamp(string $consumerGroup, string $timestamp): mixed
    {
        return $this->httpClient->post('/api/v1/consumer-groups/' . rawurlencode($consumerGroup) . '/subscription', [
            'subscriptionTimestamp' => $timestamp,
        ]);
    }

    // ===========================
    // Graceful Shutdown
    // ===========================

    public function close(): void
    {
        try {
            $this->bufferManager->flushAllBuffers();
        } catch (\Throwable $error) {
            // Best effort
        }
        $this->bufferManager->cleanup();
    }

    // ===========================
    // Internal
    // ===========================

    private function normalizeConfig(string|array $config): array
    {
        if (is_string($config)) {
            $normalized = array_merge(Defaults::CLIENT_DEFAULTS, ['urls' => [$config]]);
            return $this->validateConfig($normalized);
        }

        // Array of URLs (sequential numeric keys)
        if (isset($config[0])) {
            $normalized = array_merge(Defaults::CLIENT_DEFAULTS, ['urls' => $config]);
            return $this->validateConfig($normalized);
        }

        // Config array
        $normalized = array_merge(Defaults::CLIENT_DEFAULTS, $config);

        if (isset($normalized['urls'])) {
            // Already set
        } elseif (isset($normalized['url'])) {
            $normalized['urls'] = [$normalized['url']];
        } else {
            throw new \InvalidArgumentException('Must provide urls or url in configuration');
        }

        return $this->validateConfig($normalized);
    }

    private function validateConfig(array $config): array
    {
        if (!is_array($config['urls']) || $config['urls'] === []) {
            throw new \InvalidArgumentException('urls must be a non-empty array');
        }

        $config['urls'] = array_values(array_map(function (mixed $url): string {
            if (!is_string($url) || trim($url) === '') {
                throw new \InvalidArgumentException('Every Queen URL must be a non-empty string');
            }
            $url = rtrim(trim($url), '/');
            $parts = parse_url($url);
            if (!is_array($parts)
                || !in_array(strtolower((string) ($parts['scheme'] ?? '')), ['http', 'https'], true)
                || !is_string($parts['host'] ?? null)
                || $parts['host'] === ''
                || preg_match('/[\x00-\x20\x7F]/', $parts['host']) === 1
                || isset($parts['user'])
                || isset($parts['pass'])
                || isset($parts['query'])
                || isset($parts['fragment'])) {
                throw new \InvalidArgumentException("Invalid Queen URL [{$url}]; expected http:// or https://");
            }
            return $url;
        }, $config['urls']));

        foreach ([
            'timeoutMillis' => 1,
            'retryAttempts' => 1,
            'retryDelayMillis' => 0,
            'affinityHashRing' => 1,
            'healthRetryAfterMillis' => 0,
        ] as $name => $minimum) {
            $config[$name] = $this->normalizeInteger($config[$name] ?? null, $name, $minimum);
        }

        if (!in_array($config['loadBalancingStrategy'], ['affinity', 'round-robin', 'session'], true)) {
            throw new \InvalidArgumentException('loadBalancingStrategy must be affinity, round-robin, or session');
        }
        if (!is_bool($config['enableFailover'])) {
            throw new \InvalidArgumentException('enableFailover must be a boolean');
        }
        if ($config['bearerToken'] !== null && (
            !is_string($config['bearerToken'])
            || $config['bearerToken'] === ''
            || preg_match('/[\x00-\x20\x7F]/', $config['bearerToken']) === 1
        )) {
            throw new \InvalidArgumentException('bearerToken must be a non-empty header-safe string or null');
        }
        if (!is_array($config['headers'])) {
            throw new \InvalidArgumentException('headers must be an array');
        }
        foreach ($config['headers'] as $name => $value) {
            if (!is_string($name)
                || preg_match('/^[!#$%&\'*+\-.^_`|~0-9A-Za-z]+$/D', $name) !== 1) {
                throw new \InvalidArgumentException('Header names must be non-empty HTTP token strings');
            }

            $values = is_array($value) ? $value : [$value];
            if ($values === []) {
                throw new \InvalidArgumentException("Header [{$name}] must contain at least one value");
            }
            foreach ($values as $headerValue) {
                if (!is_scalar($headerValue) || preg_match('/[\r\n]/', (string) $headerValue) === 1) {
                    throw new \InvalidArgumentException("Header [{$name}] contains an invalid value");
                }
            }
            $config['headers'][$name] = is_array($value)
                ? array_map(static fn (mixed $item): string => (string) $item, $values)
                : (string) $value;
        }
        if (!is_array($config['retry429'])) {
            throw new \InvalidArgumentException('retry429 must be an array');
        }
        $unknownRetryKeys = array_diff(array_keys($config['retry429']), ['maxAttempts', 'baseMs', 'capMs']);
        if ($unknownRetryKeys !== []) {
            throw new \InvalidArgumentException('retry429 contains unknown option [' . reset($unknownRetryKeys) . ']');
        }
        foreach ($config['retry429'] as $name => $value) {
            $config['retry429'][$name] = $this->normalizeInteger($value, "retry429.{$name}", 0);
        }
        if (($config['retry429']['capMs'] ?? 1) > Retry429Policy::MAX_CAP_MILLIS) {
            throw new \InvalidArgumentException(
                'retry429.capMs must not exceed ' . Retry429Policy::MAX_CAP_MILLIS,
            );
        }

        return $config;
    }

    private function normalizeInteger(mixed $value, string $name, int $minimum): int
    {
        $integer = false;
        if (is_int($value)) {
            $integer = $value;
        } elseif (is_string($value) && preg_match('/^-?\d+$/D', $value) === 1) {
            $negative = str_starts_with($value, '-');
            $digits = ltrim($value, '-0');
            $digits = $digits === '' ? '0' : $digits;
            $canonical = $negative && $digits !== '0' ? '-' . $digits : $digits;
            $integer = filter_var($canonical, FILTER_VALIDATE_INT);
        }

        if ($integer === false) {
            throw new \InvalidArgumentException("{$name} must be an integer");
        }
        if ($integer < $minimum) {
            throw new \InvalidArgumentException("{$name} must be at least {$minimum}");
        }

        return $integer;
    }

    private function createHttpClient(): HttpClient
    {
        $urls = $this->config['urls'];
        $timeoutMillis = $this->config['timeoutMillis'];
        $retryAttempts = $this->config['retryAttempts'];
        $retryDelayMillis = $this->config['retryDelayMillis'];
        $bearerToken = $this->config['bearerToken'];
        $headers = $this->config['headers'];
        $retry429 = $this->config['retry429'];
        // Undocumented Guzzle handler override, threaded so tests can drive
        // the whole builder stack against a canned server.
        $handler = $this->config['handler'] ?? null;

        if (count($urls) === 1) {
            return new HttpClient([
                'baseUrl' => $urls[0],
                'timeoutMillis' => $timeoutMillis,
                'retryAttempts' => $retryAttempts,
                'retryDelayMillis' => $retryDelayMillis,
                'bearerToken' => $bearerToken,
                'headers' => $headers,
                'retry429' => $retry429,
                'handler' => $handler,
            ]);
        }

        $loadBalancer = new LoadBalancer($urls, $this->config['loadBalancingStrategy'], [
            'affinityHashRing' => $this->config['affinityHashRing'],
            'healthRetryAfterMillis' => $this->config['healthRetryAfterMillis'],
        ]);

        return new HttpClient([
            'loadBalancer' => $loadBalancer,
            'timeoutMillis' => $timeoutMillis,
            'retryAttempts' => $retryAttempts,
            'retryDelayMillis' => $retryDelayMillis,
            'enableFailover' => $this->config['enableFailover'],
            'bearerToken' => $bearerToken,
            'headers' => $headers,
            'retry429' => $retry429,
            'handler' => $handler,
        ]);
    }

    private function buildAck(array $msg, bool|string $status, array $context, bool $hasIndividualStatus): array
    {
        $transactionId = $msg['transactionId'] ?? $msg['id'] ?? null;
        $partitionId = $msg['partitionId'] ?? null;
        $leaseId = $msg['leaseId'] ?? null;

        if ($transactionId === null) {
            throw new \InvalidArgumentException('Message must have transactionId or id property');
        }

        if ($partitionId === null) {
            throw new \InvalidArgumentException('Message must have partitionId property to ensure message uniqueness');
        }

        $msgStatus = $hasIndividualStatus && array_key_exists('_status', $msg)
            ? $msg['_status']
            : $status;

        $statusStr = is_bool($msgStatus) ? ($msgStatus ? 'completed' : 'failed') : $msgStatus;

        $ack = [
            'transactionId' => $transactionId,
            'partitionId' => $partitionId,
            'status' => $statusStr,
            'error' => $msg['_error'] ?? $context['error'] ?? null,
        ];

        if ($leaseId !== null) {
            $ack['leaseId'] = $leaseId;
        }

        return $ack;
    }
}
