<?php

namespace Queen\Laravel\Queue;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use Queen\Support\Uuid;

class QueenJob extends Job implements JobContract
{
    private ?\Throwable $failureException = null;
    private ?string $manualRetryId = null;

    public function __construct(
        Container $container,
        private QueenQueue $queen,
        private array $message,
        string $connectionName,
        string $queue,
        private string $consumerGroup,
    ) {
        $this->container = $container;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
    }

    public function release($delay = 0): void
    {
        parent::release($delay);

        $delaySeconds = max(0, $this->secondsUntil($delay));

        $this->queen->releaseReserved(
            $this->queue,
            $this->message,
            $this->getRawBody(),
            $this->consumerGroup,
            $delaySeconds,
            $this->attempts(),
        );
    }

    public function delete(): void
    {
        parent::delete();

        $this->queen->deleteReserved(
            $this->message,
            $this->consumerGroup,
            $this->hasFailed(),
            $this->failureException,
        );
    }

    public function fail($e = null): void
    {
        $this->failureException = $e;
        $this->manualRetryId ??= Uuid::v7();

        parent::fail($e);
    }

    public function attempts(): int
    {
        $payload = $this->payload();
        $completedAttempts = (int) ($payload['_queen']['attempts'] ?? 0);
        $deliveryAttempt = max(1, (int) ($this->message['deliveryAttempt']
            ?? $this->message['delivery_attempt']
            ?? 1));

        return $completedAttempts + $deliveryAttempt;
    }

    public function getJobId(): string
    {
        return (string) ($this->payload()['uuid']
            ?? $this->message['id']
            ?? $this->message['transactionId']);
    }

    public function getRawBody(): string
    {
        $data = $this->message['data'] ?? $this->message['payload'] ?? [];

        if ($this->manualRetryId !== null) {
            $data = is_string($data)
                ? json_decode($data, true, 512, JSON_THROW_ON_ERROR)
                : $data;
            if (is_array($data)) {
                $data['_queen'] = array_replace(
                    is_array($data['_queen'] ?? null) ? $data['_queen'] : [],
                    [
                        'manual_retry' => $this->manualRetryId,
                        'failed_source' => [
                            'partition_id' => $this->message['partitionId'] ?? $this->message['partition_id'] ?? null,
                            'transaction_id' => $this->message['transactionId'] ?? $this->message['transaction_id'] ?? null,
                        ],
                    ],
                );
            }
        }

        return is_string($data) ? $data : json_encode($data, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
    }

    public function getQueenMessage(): array
    {
        return $this->message;
    }
}
