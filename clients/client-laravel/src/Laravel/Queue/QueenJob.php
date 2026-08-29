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
    private ?string $rawBody = null;
    private ?array $decodedPayload = null;

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
            $this->queue,
        );
    }

    public function fail($e = null): void
    {
        $this->failureException = $e;
        $this->manualRetryId ??= Uuid::v7();
        // getRawBody() now decorates the payload with the stable manual-retry
        // hand-off. Do not let an earlier worker metadata lookup retain the
        // undecorated decoded value through Laravel's failure pipeline.
        $this->decodedPayload = null;

        parent::fail($e);
    }

    public function payload(): array
    {
        // Laravel asks for the same decoded payload repeatedly while resolving
        // timeout, tries, retry-until, handler metadata and the job id. Queen's
        // message is immutable for a delivery and Guzzle normally gave us an
        // array already, so avoid both a JSON encode and every repeated decode.
        if ($this->decodedPayload !== null) {
            return $this->decodedPayload;
        }
        $data = $this->message['data'] ?? $this->message['payload'] ?? [];
        if ($this->manualRetryId === null && is_array($data)) {
            return $this->decodedPayload = $data;
        }

        return $this->decodedPayload = json_decode(
            $this->getRawBody(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
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
        $rawBody = $this->baseRawBody();
        if ($this->manualRetryId === null) {
            return $rawBody;
        }

        $data = json_decode($rawBody, true, 512, JSON_THROW_ON_ERROR);
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

        return json_encode($data, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
    }

    public function getQueenMessage(): array
    {
        return $this->message;
    }

    private function baseRawBody(): string
    {
        if ($this->rawBody !== null) {
            return $this->rawBody;
        }

        $data = $this->message['data'] ?? $this->message['payload'] ?? [];
        return $this->rawBody = is_string($data)
            ? $data
            : json_encode($data, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
    }
}
