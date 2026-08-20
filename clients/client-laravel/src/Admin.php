<?php

namespace Queen;

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
        return $this->httpClient->get('/api/v1/resources/queues/' . urlencode($name));
    }

    /**
     * Per-partition backlog for a queue — the cheap sibling of getQueue:
     * watermark arithmetic only, no segments, no timestamps. Shape:
     * {queue, group, pending, partitions: [{partition, pending}]}.
     * A null group is queue-level pending under the same worst-cursor
     * precedence the dashboard publishes; a named group is that group's own
     * backlog per partition. Requires broker >= 1.0.4 — an older broker
     * answers 404 no_such_route, so fall back to getQueue there.
     */
    public function getQueueDepth(string $name, ?string $group = null): mixed
    {
        $query = $group !== null ? '?group=' . urlencode($group) : '';
        return $this->httpClient->get('/api/v1/resources/queues/' . urlencode($name) . '/depth' . $query);
    }

    public function clearQueue(string $name, ?string $partition = null): mixed
    {
        $query = $partition !== null ? '?partition=' . urlencode($partition) : '';
        return $this->httpClient->delete('/api/v1/queues/' . urlencode($name) . '/clear' . $query);
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
        return $this->httpClient->get("/api/v1/messages/{$partitionId}/{$transactionId}");
    }

    public function deleteMessage(string $partitionId, string $transactionId): mixed
    {
        return $this->httpClient->delete("/api/v1/messages/{$partitionId}/{$transactionId}");
    }

    public function retryMessage(string $partitionId, string $transactionId): mixed
    {
        return $this->httpClient->post("/api/v1/messages/{$partitionId}/{$transactionId}/retry", []);
    }

    public function moveMessageToDLQ(string $partitionId, string $transactionId): mixed
    {
        return $this->httpClient->post("/api/v1/messages/{$partitionId}/{$transactionId}/dlq", []);
    }

    // ===========================
    // Traces API
    // ===========================

    public function getTracesByName(string $traceName, array $params = []): mixed
    {
        return $this->httpClient->get('/api/v1/traces/by-name/' . urlencode($traceName) . $this->buildQueryString($params));
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
        return $this->httpClient->get('/api/v1/status/queues/' . urlencode($name) . $this->buildQueryString($params));
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
        return $this->httpClient->get('/api/v1/consumer-groups/' . urlencode($name));
    }

    public function getLaggingConsumers(int $minLagSeconds = 60): mixed
    {
        return $this->httpClient->get("/api/v1/consumer-groups/lagging?minLagSeconds={$minLagSeconds}");
    }

    public function deleteConsumerGroupForQueue(string $consumerGroup, string $queueName, bool $deleteMetadata = true): mixed
    {
        $dm = $deleteMetadata ? 'true' : 'false';
        return $this->httpClient->delete('/api/v1/consumer-groups/' . urlencode($consumerGroup) . '/queues/' . urlencode($queueName) . "?deleteMetadata={$dm}");
    }

    public function seekConsumerGroup(string $consumerGroup, string $queueName, array $options = []): mixed
    {
        return $this->httpClient->post('/api/v1/consumer-groups/' . urlencode($consumerGroup) . '/queues/' . urlencode($queueName) . '/seek', $options);
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
}
