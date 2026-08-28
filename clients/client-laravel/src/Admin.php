<?php

namespace Queen;

use GuzzleHttp\Promise\PromiseInterface;
use Queen\Http\HttpClient;

class Admin
{
    private HttpClient $httpClient;

    public function __construct(HttpClient $httpClient)
    {
        $this->httpClient = $httpClient;
    }

    // ===========================
    // Resources API
    // ===========================

    public function getOverview(): mixed
    {
        return $this->httpClient->get('/api/v1/resources/overview');
    }

    public function getNamespaces(): mixed
    {
        return $this->httpClient->get('/api/v1/resources/namespaces');
    }

    public function getTasks(): mixed
    {
        return $this->httpClient->get('/api/v1/resources/tasks');
    }

    // ===========================
    // Queues API
    // ===========================

    public function listQueues(array $params = []): mixed
    {
        return $this->httpClient->get('/api/v1/resources/queues' . $this->buildQueryString($params));
    }

    public function getQueue(string $name): mixed
    {
        return $this->httpClient->get('/api/v1/resources/queues/' . rawurlencode($name));
    }

    /**
     * Per-partition backlog for a queue — the cheap sibling of getQueue:
     * watermark arithmetic only, no segments, no timestamps. Shape:
     * {queue, group, pending, processing, ready, partitionsPending,
     *  partitionsReady, conflation, effectivePending, effectiveReady,
     *  partitions: [{partition, pending, processing, ready}]}.
     * A null group is queue-level pending under the same worst-cursor
     * precedence the dashboard publishes; a named group is that group's own
     * backlog per partition. Requires broker >= 1.0.4 — an older broker
     * answers 404 no_such_route, so fall back to getQueue there.
     *
     * `partitionsPending` (broker >= 1.1.0) is how many partitions have
     * anything pending at all. The lease-aware fields distinguish outstanding
     * positions from work that can be claimed now:
     *
     *   pending           Total outstanding positions, including live leases.
     *   processing        Positions covered by this group's live leases.
     *   ready             pending - processing; claimable positional work.
     *   partitionsReady   Partitions with ready > 0.
     *   effectivePending  Pending work adjusted for conflation.
     *   effectiveReady    Claimable work adjusted for conflation.
     *
     * For a CONFLATING group (`conflation: true`, see
     * QueueBuilder::conflation), effective depths are partition counts because
     * one partition yields one invocation however deep it is:
     * pending 4,000,000 with effectivePending 12 is a healthy conflating queue,
     * while the same two numbers on a non-conflating group are an incident.
     * Use effectiveReady for immediately schedulable work. Brokers predating
     * lease-aware depth omit processing/ready/partitionsReady/effectiveReady;
     * callers must retain a rolling-upgrade fallback.
     */
    public function getQueueDepth(string $name, ?string $group = null, ?int $timeoutMillis = null): mixed
    {
        return $this->httpClient->get($this->queueDepthPath($name, $group), $timeoutMillis);
    }

    public function getQueueDepthAsync(string $name, ?string $group = null, ?int $timeoutMillis = null): PromiseInterface
    {
        return $this->httpClient->getAsyncWithFailover(
            $this->queueDepthPath($name, $group),
            $timeoutMillis,
            $name,
        );
    }

    public function clearQueue(string $name, ?string $partition = null): mixed
    {
        throw new \BadMethodCallException(
            'Queen does not yet expose an atomic queue-clear operation; deleting a queue is not a safe substitute.'
        );
    }

    public function getPartitions(array $params = []): mixed
    {
        return $this->httpClient->get('/api/v1/resources/partitions' . $this->buildQueryString($params));
    }

    // ===========================
    // Messages API
    // ===========================

    public function listMessages(array $params = []): mixed
    {
        return $this->httpClient->get('/api/v1/messages' . $this->buildQueryString($params));
    }

    public function getMessage(string $partitionId, string $transactionId): mixed
    {
        return $this->httpClient->get('/api/v1/messages/' . rawurlencode($partitionId) . '/' . rawurlencode($transactionId));
    }

    public function deleteMessage(string $partitionId, string $transactionId): mixed
    {
        return $this->httpClient->delete('/api/v1/messages/' . rawurlencode($partitionId) . '/' . rawurlencode($transactionId));
    }

    public function retryMessage(string $partitionId, string $transactionId): mixed
    {
        return $this->httpClient->post('/api/v1/messages/' . rawurlencode($partitionId) . '/' . rawurlencode($transactionId) . '/retry', []);
    }

    public function moveMessageToDLQ(string $partitionId, string $transactionId): mixed
    {
        return $this->httpClient->post('/api/v1/messages/' . rawurlencode($partitionId) . '/' . rawurlencode($transactionId) . '/dlq', []);
    }

    // ===========================
    // Traces API
    // ===========================

    public function getTracesByName(string $traceName, array $params = []): mixed
    {
        return $this->httpClient->get('/api/v1/traces/by-name/' . rawurlencode($traceName) . $this->buildQueryString($params));
    }

    public function getTraceNames(array $params = []): mixed
    {
        return $this->httpClient->get('/api/v1/traces/names' . $this->buildQueryString($params));
    }

    public function getTracesForMessage(string $partitionId, string $transactionId): mixed
    {
        return $this->httpClient->get("/api/v1/traces/{$partitionId}/{$transactionId}");
    }

    // ===========================
    // Analytics/Status API
    // ===========================

    public function getStatus(array $params = []): mixed
    {
        return $this->httpClient->get('/api/v1/status' . $this->buildQueryString($params));
    }

    public function getQueueStats(array $params = []): mixed
    {
        return $this->httpClient->get('/api/v1/status/queues' . $this->buildQueryString($params));
    }

    public function getQueueDetail(string $name, array $params = []): mixed
    {
        return $this->httpClient->get('/api/v1/status/queues/' . rawurlencode($name) . $this->buildQueryString($params));
    }

    public function getAnalytics(array $params = []): mixed
    {
        return $this->httpClient->get('/api/v1/status/analytics' . $this->buildQueryString($params));
    }

    // ===========================
    // Consumer Groups API
    // ===========================

    public function listConsumerGroups(): mixed
    {
        return $this->httpClient->get('/api/v1/consumer-groups');
    }

    public function refreshConsumerStats(): mixed
    {
        return $this->httpClient->post('/api/v1/stats/refresh', []);
    }

    public function getConsumerGroup(string $name): mixed
    {
        return $this->httpClient->get('/api/v1/consumer-groups/' . rawurlencode($name));
    }

    public function getLaggingConsumers(int $minLagSeconds = 60): mixed
    {
        return $this->httpClient->get("/api/v1/consumer-groups/lagging?minLagSeconds={$minLagSeconds}");
    }

    public function deleteConsumerGroupForQueue(string $consumerGroup, string $queueName, bool $deleteMetadata = true): mixed
    {
        $dm = $deleteMetadata ? 'true' : 'false';
        return $this->httpClient->delete('/api/v1/consumer-groups/' . rawurlencode($consumerGroup) . '/queues/' . rawurlencode($queueName) . "?deleteMetadata={$dm}");
    }

    public function seekConsumerGroup(string $consumerGroup, string $queueName, array $options = []): mixed
    {
        return $this->httpClient->post('/api/v1/consumer-groups/' . rawurlencode($consumerGroup) . '/queues/' . rawurlencode($queueName) . '/seek', $options);
    }

    // ===========================
    // System API
    // ===========================

    public function health(): mixed
    {
        return $this->httpClient->get('/health');
    }

    public function metrics(): mixed
    {
        return $this->httpClient->get('/metrics');
    }

    public function getMaintenanceMode(): mixed
    {
        return $this->httpClient->get('/api/v1/system/maintenance');
    }

    public function setMaintenanceMode(bool $enabled): mixed
    {
        return $this->httpClient->post('/api/v1/system/maintenance', ['enabled' => $enabled]);
    }

    public function getPopMaintenanceMode(): mixed
    {
        return $this->httpClient->get('/api/v1/system/maintenance/pop');
    }

    public function setPopMaintenanceMode(bool $enabled): mixed
    {
        return $this->httpClient->post('/api/v1/system/maintenance/pop', ['enabled' => $enabled]);
    }

    public function getSystemMetrics(array $params = []): mixed
    {
        return $this->httpClient->get('/api/v1/analytics/system-metrics' . $this->buildQueryString($params));
    }

    public function getWorkerMetrics(array $params = []): mixed
    {
        return $this->httpClient->get('/api/v1/analytics/worker-metrics' . $this->buildQueryString($params));
    }

    public function getPostgresStats(): mixed
    {
        return $this->httpClient->get('/api/v1/analytics/postgres-stats');
    }

    // ===========================
    // Helpers
    // ===========================

    private function buildQueryString(array $params): string
    {
        $filtered = array_filter($params, fn($v) => $v !== null);
        if (empty($filtered)) {
            return '';
        }
        return '?' . http_build_query($filtered);
    }

    private function queueDepthPath(string $name, ?string $group): string
    {
        $query = $group !== null ? '?group=' . rawurlencode($group) : '';
        return '/api/v1/resources/queues/' . rawurlencode($name) . '/depth' . $query;
    }
}
