<?php

namespace Queen\Builders;

use Queen\Queen;
use Queen\Http\HttpClient;
use Queen\Http\Retry429Policy;
use Queen\Buffer\BufferManager;
use Queen\Consumer\HighLevelConsumer;
use Queen\Support\ConflationGuard;
use Queen\Support\Defaults;
use Queen\Support\PopAutopilot;
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
    // consumeBatch / consumeMaxPartitions hold the USER's value, and null means
    // the setter was never called -- which is the dimension pop autopilot gets to
    // choose. The client-side defaults are applied at emission time
    // (Support\PopAutopilot), not here, because filling them in here would erase
    // the difference between "never called batch()" and "called batch(1)".
    private ?int $consumeBatch = null;
    private ?int $consumeLimit;
    private ?int $consumeIdleMillis;
    private bool $consumeAutoAck;
    private bool $consumeWait;
    private int $consumeTimeoutMillis;
    private bool $consumeRenewLease;
    private ?int $consumeRenewLeaseIntervalMillis;
    private ?string $consumeSubscriptionMode;
    private ?string $consumeSubscriptionFrom;
    private bool $consumeEach = false;
    private ?int $consumeMaxPartitions = null;
    private bool $consumeConflation;
    /** Per-builder override: null = the client default (on unless the env turned it off). */
    private ?bool $consumeAutopilot = null;

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
            '/api/v1/resources/queues/' . urlencode($this->queueName),
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
                'partition' => $this->partition,
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

    /**
     * Pin the message budget for one pop.
     *
     * Leave it unset and the broker sizes it (see autopilot()), where it used to
     * mean the client-side default of 1. batch(0) is not "a batch of zero" and
     * never was: it is the absence of an opinion, so it reads as unset.
     */
    public function batch(int $size): static
    {
        $this->consumeBatch = $size > 0 ? $size : null;
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
     * Leave it unset and the broker chooses the sweep width (see autopilot());
     * partitions(1) pins the legacy single-partition behaviour, which is a
     * decision the broker is told about and never overrides.
     */
    public function partitions(int $n): static
    {
        $this->consumeMaxPartitions = $n > 0 ? $n : null;
        return $this;
    }

    /**
     * Turn broker-side pop sizing on or off for this builder.
     *
     * On (the default) the broker chooses `batch` and `partitions` for the pops
     * of this builder. Even then, a batch or partitions set explicitly travels
     * on the wire as it always did and is never second-guessed: autopilot only
     * ever fills the knobs left unset.
     *
     * autopilot(false) restores this SDK's pre-1.2 behaviour byte for byte: the
     * client-side defaults come back (batch 1, partitions 1) and no autopilot
     * parameter is sent. QUEEN_SDK_POP_AUTOPILOT=off does the same for a whole
     * process; an explicit call here outranks the environment in both
     * directions.
     *
     * Setting BOTH batch and partitions leaves autopilot nothing to decide, so
     * no autopilot parameter is sent in that case either, whatever this flag
     * says.
     */
    public function autopilot(bool $enabled = true): static
    {
        $this->consumeAutopilot = $enabled;
        return $this;
    }

    /**
     * This builder's resolved autopilot decision: its own flag when set,
     * otherwise the client-wide default settled in the Queen constructor.
     */
    private function autopilotEnabled(): bool
    {
        return $this->consumeAutopilot ?? !$this->queen->autopilotOff();
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
        return $this->popWithDecision()['messages'];
    }

    /**
     * Claim messages and report what the broker chose for this pop.
     *
     * The same call as pop() — this is the shape that also carries the additive
     * `autopilot` echo, which is null when this pop did not engage autopilot or
     * the broker is older than 1.2.
     *
     *   ['messages' => array, 'autopilot' => ['partitions' => int, 'batch' => int, 'waitMillis' => int]|null]
     *
     * @return array{messages: array, autopilot: array{partitions: int, batch: int, waitMillis: int}|null}
     */
    public function popResult(): array
    {
        return $this->popWithDecision();
    }

    /** @return array{messages: array, autopilot: array{partitions: int, batch: int, waitMillis: int}|null} */
    private function popWithDecision(): array
    {
        $path = $this->buildPopPath();

        // Pop uses POP_DEFAULTS for autoAck unless explicitly changed
        $effectiveAutoAck = $this->consumeAutoAck !== Defaults::CONSUME_DEFAULTS['autoAck']
            ? $this->consumeAutoAck
            : Defaults::POP_DEFAULTS['autoAck'];

        // Batch, partitions and with them the autopilot flag. The RULE for which
        // of the three travel lives in one place (Support\PopAutopilot) because
        // this SDK has three pop param builders; only the PLACEMENT is here, and
        // it is the pre-autopilot placement so an autopilot-off request is
        // byte-identical to the one this SDK used to send.
        $sizing = PopAutopilot::sizing(
            $this->consumeBatch,
            $this->consumeMaxPartitions,
            Defaults::POP_DEFAULTS['batch'],
            $this->autopilotEnabled()
        );

        $params = [];
        if ($sizing['autopilot']) {
            $params['autopilot'] = 'true';
        }
        if ($sizing['batch'] !== null) {
            $params['batch'] = $sizing['batch'];
        }
        $params['wait'] = $this->consumeWait ? 'true' : 'false';
        $params['timeout'] = (string) $this->consumeTimeoutMillis;

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
        // Under autopilot a pinned width travels even when it is 1, because 1 is
        // then a decision and not the absence of one.
        if ($sizing['partitions'] !== null) {
            $params['partitions'] = $sizing['partitions'];
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

        // The broker's own account of how it sized this pop, when the request
        // engaged autopilot and the answer had a body to carry it (a bodiless
        // 204 cannot, so an empty short pop reports null).
        $autopilot = PopAutopilot::decision($result);

        if (!$result || !isset($result['messages'])) {
            return ['messages' => [], 'autopilot' => $autopilot];
        }

        return [
            'messages' => array_filter($result['messages'], fn($msg) => $msg !== null),
            'autopilot' => $autopilot,
        ];
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
            'renewLease' => $this->consumeRenewLease,
            'renewLeaseIntervalMillis' => $this->consumeRenewLeaseIntervalMillis,
            'subscriptionMode' => $this->consumeSubscriptionMode,
            'subscriptionFrom' => $this->consumeSubscriptionFrom,
            'each' => $this->consumeEach,
            'maxPartitions' => $this->consumeMaxPartitions,
            'conflation' => $this->consumeConflation,
            // Resolved here so the consumers see a decision and not a null.
            // batch and maxPartitions keep their null when autopilot is on, and
            // that null has to survive all the way to the param builders: it is
            // the ONLY record that the user said nothing about that dimension.
            'autopilot' => $this->autopilotEnabled(),
        ];
    }

    private function buildPopPath(): string
    {
        if ($this->queueName !== null) {
            if ($this->partition !== 'Default') {
                return "/api/v1/pop/queue/{$this->queueName}/partition/{$this->partition}";
            }
            return "/api/v1/pop/queue/{$this->queueName}";
        }

        if ($this->namespace !== null || $this->task !== null) {
            return '/api/v1/pop';
        }

        throw new \RuntimeException('Must specify queue, namespace, or task for pop operation');
    }
}
