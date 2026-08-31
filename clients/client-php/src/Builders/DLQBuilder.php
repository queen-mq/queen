<?php

namespace Queen\Builders;

use Queen\Http\HttpClient;

class DLQBuilder
{
    private HttpClient $httpClient;
    private string $queueName;
    private ?string $consumerGroup;
    private ?string $partition;
    private int $queryLimit = 100;
    private int $queryOffset = 0;
    private ?string $fromTimestamp = null;
    private ?string $toTimestamp = null;

    public function __construct(HttpClient $httpClient, string $queueName, ?string $consumerGroup, ?string $partition)
    {
        $this->httpClient = $httpClient;
        $this->queueName = $queueName;
        $this->consumerGroup = $consumerGroup;
        $this->partition = $partition !== 'Default' ? $partition : null;
    }

    public function limit(int $count): static
    {
        $this->queryLimit = max(1, $count);
        return $this;
    }

    public function offset(int $count): static
    {
        $this->queryOffset = max(0, $count);
        return $this;
    }

    public function from(string $timestamp): static
    {
        $this->fromTimestamp = $timestamp;
        return $this;
    }

    public function to(string $timestamp): static
    {
        $this->toTimestamp = $timestamp;
        return $this;
    }

    public function get(): array
    {
        $params = [
            'queue' => $this->queueName,
            'limit' => (string) $this->queryLimit,
            'offset' => (string) $this->queryOffset,
        ];

        if ($this->consumerGroup !== null) {
            $params['consumerGroup'] = $this->consumerGroup;
        }

        if ($this->partition !== null) {
            $params['partition'] = $this->partition;
        }

        if ($this->fromTimestamp !== null) {
            $params['from'] = $this->fromTimestamp;
        }

        if ($this->toTimestamp !== null) {
            $params['to'] = $this->toTimestamp;
        }

        $query = http_build_query($params);
        $result = $this->httpClient->get("/api/v1/dlq?{$query}");
        return $result ?? ['messages' => [], 'total' => 0];
    }
}
