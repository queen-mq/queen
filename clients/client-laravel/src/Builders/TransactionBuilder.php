<?php

namespace Queen\Builders;

use Queen\Http\HttpClient;
use Queen\Support\Uuid;

class TransactionBuilder
{
    private HttpClient $httpClient;
    private array $operations = [];
    private array $requiredLeases = [];

    public function __construct(HttpClient $httpClient)
    {
        $this->httpClient = $httpClient;
    }

    public function ack(array|object $messages, string $status = 'completed', array $context = []): static
    {
        $msgs = is_array($messages) && (isset($messages[0]) || empty($messages)) ? $messages : [$messages];

        foreach ($msgs as $msg) {
            if (is_string($msg)) {
                $transactionId = $msg;
                $partitionId = null;
                $leaseId = null;
            } else {
                $msg = (array) $msg;
                $transactionId = $msg['transactionId'] ?? $msg['id'] ?? null;
                $partitionId = $msg['partitionId'] ?? null;
                $leaseId = $msg['leaseId'] ?? null;
            }

            if ($transactionId === null) {
                throw new \InvalidArgumentException('Message must have transactionId or id property');
            }

            if ($partitionId === null) {
                throw new \InvalidArgumentException('Message must have partitionId property to ensure message uniqueness');
            }

            $operation = [
                'type' => 'ack',
                'transactionId' => $transactionId,
                'partitionId' => $partitionId,
                'status' => $status,
            ];

            if (isset($context['consumerGroup'])) {
                $operation['consumerGroup'] = $context['consumerGroup'];
            }

            $this->operations[] = $operation;

            if ($leaseId !== null) {
                $this->requiredLeases[] = $leaseId;
            }
        }

        return $this;
    }

    /**
     * Returns a sub-builder for push operations on a queue
     */
    public function queue(string $queueName): TransactionQueueBuilder
    {
        return new TransactionQueueBuilder($this, $queueName);
    }

    /**
     * @internal Used by TransactionQueueBuilder
     */
    public function addPushOperation(string $queueName, ?string $partition, array $items): void
    {
        $this->operations[] = [
            'type' => 'push',
            'items' => array_map(function (array $item) use ($queueName, $partition) {
                $payloadValue = $item['data'] ?? $item['payload'] ?? $item;

                // Same contract as QueueBuilder::push: the caller's transactionId
                // is what makes a retried transaction idempotent inside the dedup
                // window, so it has to reach the wire. Absent, mint one here
                // rather than leaving the broker to do it.
                $result = [
                    'queue' => $queueName,
                    'payload' => $payloadValue,
                    'transactionId' => $item['transactionId'] ?? Uuid::v7(),
                ];

                if ($partition !== null) {
                    $result['partition'] = $partition;
                }

                if (isset($item['traceId'])) {
                    $result['traceId'] = $item['traceId'];
                }

                return $result;
            }, $items),
        ];
    }

    public function commit(): array
    {
        if (empty($this->operations)) {
            throw new \RuntimeException('Transaction has no operations to commit');
        }

        $result = $this->httpClient->post('/api/v1/transaction', [
            'operations' => $this->operations,
            'requiredLeases' => array_values(array_unique($this->requiredLeases)),
        ]);

        if (!($result['success'] ?? false)) {
            $error = $result['error'] ?? 'Transaction failed';
            $txId = $result['transactionId'] ?? null;
            $msg = $txId ? "Transaction {$txId} failed: {$error}" : "Transaction failed: {$error}";
            throw new \RuntimeException($msg);
        }

        return $result;
    }
}

class TransactionQueueBuilder
{
    private TransactionBuilder $parent;
    private string $queueName;
    private ?string $partition = null;

    public function __construct(TransactionBuilder $parent, string $queueName)
    {
        $this->parent = $parent;
        $this->queueName = $queueName;
    }

    public function partition(string $partition): static
    {
        $this->partition = $partition;
        return $this;
    }

    public function push(array $items): TransactionBuilder
    {
        $this->parent->addPushOperation($this->queueName, $this->partition, $items);
        return $this->parent;
    }
}
