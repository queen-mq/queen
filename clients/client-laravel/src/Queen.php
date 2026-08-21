<?php

namespace Queen;

use Queen\Http\HttpClient;
use Queen\Http\LoadBalancer;
use Queen\Buffer\BufferManager;
use Queen\Builders\QueueBuilder;
use Queen\Builders\TransactionBuilder;
use Queen\Support\Defaults;

class Queen
{
    private HttpClient $httpClient;
    private BufferManager $bufferManager;
    private array $config;
    private ?Admin $admin = null;
    private ?Kv $kv = null;
    private ?Timers $timers = null;
    private ?Ephemeral $ephemeral = null;

    /**
     * @param string|array $config Single URL string, array of URLs, or config array
     */
    public function __construct(string|array $config = [])
    {
        $this->config = $this->normalizeConfig($config);
        $this->httpClient = $this->createHttpClient();
        $this->bufferManager = new BufferManager($this->httpClient);
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
                ]);

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
            $result = $this->httpClient->post('/api/v1/ack', $body);

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
     */
    public function renew(string|array $messageOrLeaseId): array
    {
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
                $result = $this->httpClient->post("/api/v1/lease/{$leaseId}/extend", []);
                $results[] = [
                    'leaseId' => $leaseId,
                    'success' => true,
                    'newExpiresAt' => $result['newExpiresAt'] ?? $result['lease_expires_at'] ?? null,
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
        return $this->httpClient->delete("/api/v1/consumer-groups/" . urlencode($consumerGroup) . "?deleteMetadata={$dm}");
    }

    public function updateConsumerGroupTimestamp(string $consumerGroup, string $timestamp): mixed
    {
        return $this->httpClient->post("/api/v1/consumer-groups/" . urlencode($consumerGroup) . "/subscription", [
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
            return array_merge(Defaults::CLIENT_DEFAULTS, ['urls' => [$config]]);
        }

        // Array of URLs (sequential numeric keys)
        if (isset($config[0])) {
            return array_merge(Defaults::CLIENT_DEFAULTS, ['urls' => $config]);
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

        return $normalized;
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
