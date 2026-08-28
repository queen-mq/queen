<?php

namespace Queen\Builders;

use Queen\Queen;
use Queen\Http\HttpClient;
use Queen\Http\Retry429Policy;
use Queen\Buffer\BufferManager;
use Queen\Consumer\HighLevelConsumer;
use Queen\Support\ConflationGuard;
use Queen\Support\Defaults;
use Queen\Support\Uuid;

class QueueBuilder
{
    private Queen $queen;
    private HttpClient $httpClient;
    private BufferManager $bufferManager;
    private ?string $queueName;
    private string $partition = 'Default';
    private ?string $namespace = null;
    private ?string $task = null;
    private ?string $group = null;
    private array $config = [];

    // Consume options
    private int $consumeConcurrency;
    private int $consumeBatch;
    private ?int $consumeLimit;
    private ?int $consumeIdleMillis;
    private bool $consumeAutoAck;
    private bool $consumeWait;
    private int $consumeTimeoutMillis;
    private ?int $consumeLeaseSeconds = null;
    private bool $consumeRenewLease;
    private ?int $consumeRenewLeaseIntervalMillis;
    private ?string $consumeSubscriptionMode;
    private ?string $consumeSubscriptionFrom;
    private bool $consumeEach = false;
    private int $consumeMaxPartitions = 1;
    private bool $consumeConflation;

    // Buffer options
    private ?array $bufferOptions = null;

    public function __construct(Queen $queen, HttpClient $httpClient, BufferManager $bufferManager, ?string $queueName = null)
    {
        $this->queen = $queen;
        $this->httpClient = $httpClient;
        $this->bufferManager = $bufferManager;
        $this->queueName = $queueName;

        // Initialize from consume defaults
        $d = Defaults::CONSUME_DEFAULTS;
        $this->consumeConcurrency = $d['concurrency'];
        $this->consumeBatch = $d['batch'];
        $this->consumeLimit = $d['limit'];
        $this->consumeIdleMillis = $d['idleMillis'];
        $this->consumeAutoAck = $d['autoAck'];
        $this->consumeWait = $d['wait'];
        $this->consumeTimeoutMillis = $d['timeoutMillis'];
        $this->consumeRenewLease = $d['renewLease'];
        $this->consumeRenewLeaseIntervalMillis = $d['renewLeaseIntervalMillis'];
        $this->consumeSubscriptionMode = $d['subscriptionMode'];
        $this->consumeSubscriptionFrom = $d['subscriptionFrom'];
        $this->consumeConflation = $d['conflation'];
    }

    // ===========================
    // Affinity Key Generation
    // ===========================

    private function getAffinityKey(): ?string
    {
        if ($this->queueName !== null) {
            $partition = $this->partition ?: '*';
            $group = $this->group ?: '__QUEUE_MODE__';
            return "{$this->queueName}:{$partition}:{$group}";
        }

        if ($this->namespace !== null || $this->task !== null) {
            $ns = $this->namespace ?: '*';
            $task = $this->task ?: '*';
            $group = $this->group ?: '__QUEUE_MODE__';
            return "{$ns}:{$task}:{$group}";
        }

        return null;
    }

    // ===========================
    // Queue Configuration
    // ===========================

    public function namespace(string $name): static
    {
        $this->namespace = $name;
        return $this;
    }

    public function task(string $name): static
    {
        $this->task = $name;
        return $this;
    }

    public function config(array $options): static
    {
        $this->config = array_merge(Defaults::QUEUE_DEFAULTS, $options);
        return $this;
    }

    public function create(): OperationBuilder
    {
        $fullConfig = !empty($this->config) ? $this->config : Defaults::QUEUE_DEFAULTS;

        return new OperationBuilder($this->httpClient, 'POST', '/api/v1/configure', [
            'queue' => $this->queueName,
            'namespace' => $this->namespace,
            'task' => $this->task,
            'options' => $fullConfig,
        ]);
    }

    public function delete(): OperationBuilder
    {
        if ($this->queueName === null) {
            throw new \RuntimeException('Queue name is required for delete operation');
        }

        return new OperationBuilder(
            $this->httpClient,
            'DELETE',
            '/api/v1/resources/queues/' . rawurlencode($this->queueName),
            null
        );
    }

    // ===========================
    // Push Methods
    // ===========================

    public function partition(string $name): static
    {
        $this->partition = $name;
        return $this;
    }

    public function buffer(array $options): static
    {
        $this->bufferOptions = $options;
        return $this;
    }

    public function push(array $payload): PushBuilder
    {
        if ($this->queueName === null) {
            throw new \RuntimeException('Queue name is required for push operation');
        }

        $items = isset($payload[0]) || empty($payload) ? $payload : [$payload];
        $formattedItems = array_map(function (array $item) {
            if (array_key_exists('data', $item)) {
                $payloadValue = $item['data'];
            } elseif (array_key_exists('payload', $item)) {
                $payloadValue = $item['payload'];
            } else {
                $payloadValue = $item;
            }

            $result = [
                'queue' => $this->queueName,
                'partition' => $item['partition'] ?? $this->partition,
                'payload' => $payloadValue,
                'transactionId' => $item['transactionId'] ?? Uuid::v7(),
            ];

            if (isset($item['traceId'])) {
                $result['traceId'] = $item['traceId'];
            }

            return $result;
        }, $items);

        return new PushBuilder(
            $this->httpClient,
            $this->bufferManager,
            $this->queueName,
            $this->partition,
            $formattedItems,
            $this->bufferOptions
        );
    }

    // ===========================
    // Consume Configuration
    // ===========================

    public function group(string $name): static
    {
        $this->group = $name;
        return $this;
    }

    public function concurrency(int $count): static
    {
        $this->consumeConcurrency = max(1, $count);
        return $this;
    }

    public function batch(int $size): static
    {
        $this->consumeBatch = max(1, $size);
        return $this;
    }

    /**
     * Pop messages from up to N partitions in a single call (v4 multi-partition pop).
     *
     * Use this to drain many sparsely-loaded partitions efficiently. With
     * partitions(N), the global batch(B) budget is shared across all
     * claimed partitions: at most B total messages, drawn from up to N
     * partitions, in a single network round-trip. All N share one leaseId
     * (renewing once extends them all).
     *
     * Default 1 = legacy single-partition behavior.
     */
    public function partitions(int $n): static
    {
        $this->consumeMaxPartitions = max(1, $n);
        return $this;
    }

    public function limit(int $count): static
    {
        $this->consumeLimit = $count;
        return $this;
    }

    public function idleMillis(int $millis): static
    {
        $this->consumeIdleMillis = $millis;
        return $this;
    }

    public function autoAck(bool $enabled): static
    {
        $this->consumeAutoAck = $enabled;
        return $this;
    }

    public function wait(bool $enabled): static
    {
        $this->consumeWait = $enabled;
        return $this;
    }

    public function timeoutMillis(int $millis): static
    {
        $this->consumeTimeoutMillis = $millis;
        return $this;
    }

    /**
     * Override the queue's visibility timeout for this consumer/pop.
     *
     * This is especially useful for framework queue workers: their process
     * timeout must remain shorter than the broker lease, otherwise a slow job
     * may be delivered to a second worker while the first one is still alive.
     */
    public function leaseSeconds(int $seconds): static
    {
        $this->consumeLeaseSeconds = max(1, $seconds);
        return $this;
    }

    public function renewLease(bool $enabled, ?int $intervalMillis = null): static
    {
        $this->consumeRenewLease = $enabled;
        if ($intervalMillis !== null) {
            $this->consumeRenewLeaseIntervalMillis = $intervalMillis;
        }
        return $this;
    }

    public function subscriptionMode(string $mode): static
    {
        $this->consumeSubscriptionMode = $mode;
        return $this;
    }

    public function subscriptionFrom(string $from): static
    {
        $this->consumeSubscriptionFrom = $from;
        return $this;
    }

    /**
     * Last-value delivery: under backlog, process only the NEWEST message per
     * partition and retire the rest.
     *
     * For command-style queues where one partition is one logical task key and
     * only the freshest pending message matters ("recompute entity X"). The
     * broker delivers exactly the newest visible message of a partition and the
     * ack commits the whole span behind it, so a 4,000,000-message backlog over
     * 12 dirty keys is 12 handler invocations, not 4,000,000.
     *
     * Three things to know before switching it on:
     *
     *  - It is a property of the consumer GROUP, not of this call. The first
     *    consumer to register the group fixes it; every later consumer of that
     *    group gets the stored setting whatever it asks for, and is warned once
     *    if it disagreed. That is what lets `workers` conflate while `audit`
     *    reads every message on the same queue.
     *  - It needs a group(). Queue mode is a shared cursor with no group
     *    identity to hang a policy on, and the broker answers 400.
     *  - It cannot be combined with autoAck(true), which the broker also
     *    refuses: auto-ack commits at delivery with no lease, so a crashed
     *    handler would lose the newest state — the one thing conflation exists
     *    to guarantee gets processed.
     *
     * Requires broker >= 1.1.0. Against an older one the consume loop raises
     * ConflationUnsupportedException on the first response rather than quietly
     * draining the backlog message by message.
     */
    public function conflation(bool $enabled = true): static
    {
        $this->consumeConflation = $enabled;
        return $this;
    }

    public function each(): static
    {
        $this->consumeEach = true;
        return $this;
    }

    // ===========================
    // Consume (callback-based)
    // ===========================

    public function consume(\Closure $handler): ConsumeBuilder
    {
        return new ConsumeBuilder($this->httpClient, $this->queen, $handler, $this->buildConsumeOptions());
    }

    // ===========================
    // High-Level Consumer (rdkafka-style)
    // ===========================

    public function getConsumer(): HighLevelConsumer
    {
        return new HighLevelConsumer($this->httpClient, $this->queen, $this->buildConsumeOptions());
    }

    // ===========================
    // Pop
    // ===========================

    public function pop(): array
    {
        $path = $this->buildPopPath();

        // Pop uses POP_DEFAULTS for autoAck unless explicitly changed
        $effectiveAutoAck = $this->consumeAutoAck !== Defaults::CONSUME_DEFAULTS['autoAck']
            ? $this->consumeAutoAck
            : Defaults::POP_DEFAULTS['autoAck'];

        $params = [
            'batch' => (string) $this->consumeBatch,
            'wait' => $this->consumeWait ? 'true' : 'false',
            'timeout' => (string) $this->consumeTimeoutMillis,
        ];

        if ($this->consumeLeaseSeconds !== null) {
            $params['leaseSeconds'] = (string) $this->consumeLeaseSeconds;
        }

        if ($this->group !== null) {
            $params['consumerGroup'] = $this->group;
        }
        if ($this->namespace !== null) {
            $params['namespace'] = $this->namespace;
        }
        if ($this->task !== null) {
            $params['task'] = $this->task;
        }
        if ($effectiveAutoAck) {
            $params['autoAck'] = 'true';
        }
        if ($this->consumeSubscriptionMode !== null) {
            $params['subscriptionMode'] = $this->consumeSubscriptionMode;
        }
        if ($this->consumeSubscriptionFrom !== null) {
            $params['subscriptionFrom'] = $this->consumeSubscriptionFrom;
        }
        if ($this->consumeMaxPartitions > 1) {
            $params['partitions'] = (string) $this->consumeMaxPartitions;
        }
        // Only ever sent when true, the rule autoAck follows above: the broker
        // treats presence as opt-in, and an explicit conflation=false would read
        // as a DISAGREEMENT with a group whose stored policy is true.
        if ($this->consumeConflation) {
            $params['conflation'] = 'true';
        }

        $query = http_build_query($params);
        $affinityKey = $this->getAffinityKey();

        // wait=true is a long-poll: on 429 it should back off and keep waiting
        // rather than give up after the bounded push-like budget.
        $retryKind = $this->consumeWait ? Retry429Policy::KIND_POP : null;
        $result = $this->httpClient->get("{$path}?{$query}", $this->consumeTimeoutMillis + 5000, $affinityKey, $retryKind);

        // Before the empty-response shortcut below, not after: an old broker's
        // empty pop is a bodiless 204 that arrives here as null, and that is
        // exactly the response an idle conflating consumer must not mistake for
        // "working" (PLAN_CONFLATION §4).
        ConflationGuard::check(
            $result,
            $this->consumeConflation,
            $this->queueName,
            $this->group,
            $this->namespace,
            $this->task
        );

        if (!$result || !isset($result['messages'])) {
            return [];
        }

        return array_filter($result['messages'], fn($msg) => $msg !== null);
    }

    // ===========================
    // Buffer
    // ===========================

    public function flushBuffer(): void
    {
        if ($this->queueName === null) {
            throw new \RuntimeException('Queue name is required for buffer flush');
        }
        $queueAddress = "{$this->queueName}/{$this->partition}";
        $this->bufferManager->flushBuffer($queueAddress);
    }

    // ===========================
    // DLQ
    // ===========================

    public function dlq(?string $consumerGroup = null): DLQBuilder
    {
        if ($this->queueName === null) {
            throw new \RuntimeException('Queue name is required for DLQ operations');
        }
        return new DLQBuilder($this->httpClient, $this->queueName, $consumerGroup, $this->partition);
    }

    // ===========================
    // Private helpers
    // ===========================

    private function buildConsumeOptions(): array
    {
        return [
            'queue' => $this->queueName,
            'partition' => $this->partition !== 'Default' ? $this->partition : null,
            'namespace' => $this->namespace,
            'task' => $this->task,
            'group' => $this->group,
            'concurrency' => $this->consumeConcurrency,
            'batch' => $this->consumeBatch,
            'limit' => $this->consumeLimit,
            'idleMillis' => $this->consumeIdleMillis,
            'autoAck' => $this->consumeAutoAck,
            'wait' => $this->consumeWait,
            'timeoutMillis' => $this->consumeTimeoutMillis,
            'leaseSeconds' => $this->consumeLeaseSeconds,
            'renewLease' => $this->consumeRenewLease,
            'renewLeaseIntervalMillis' => $this->consumeRenewLeaseIntervalMillis,
            'subscriptionMode' => $this->consumeSubscriptionMode,
            'subscriptionFrom' => $this->consumeSubscriptionFrom,
            'each' => $this->consumeEach,
            'maxPartitions' => $this->consumeMaxPartitions,
            'conflation' => $this->consumeConflation,
        ];
    }

    private function buildPopPath(): string
    {
        if ($this->queueName !== null) {
            if ($this->partition !== 'Default') {
                return '/api/v1/pop/queue/' . rawurlencode($this->queueName)
                    . '/partition/' . rawurlencode($this->partition);
            }
            return '/api/v1/pop/queue/' . rawurlencode($this->queueName);
        }

        if ($this->namespace !== null || $this->task !== null) {
            return '/api/v1/pop';
        }

        throw new \RuntimeException('Must specify queue, namespace, or task for pop operation');
    }
}
