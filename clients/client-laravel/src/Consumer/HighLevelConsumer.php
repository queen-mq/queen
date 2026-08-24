<?php

namespace Queen\Consumer;

use Queen\Exceptions\ConflationUnsupportedException;
use Queen\Http\HttpClient;
use Queen\Http\Retry429Policy;
use Queen\Queen;
use Queen\Support\ConflationGuard;
use Queen\Support\Defaults;
use Queen\Support\PopAutopilot;

/**
 * High-level consumer inspired by php-rdkafka's KafkaConsumer.
 *
 * Usage:
 *   $consumer = $queen->queue('orders')->group('processors')->batch(10)->getConsumer();
 *   $consumer->subscribe();
 *
 *   while (true) {
 *       $message = $consumer->consume(1000);
 *       if ($message === null) continue;
 *       processMessage($message);
 *       $consumer->ack($message);
 *   }
 *
 *   $consumer->close();
 */
class HighLevelConsumer
{
    private HttpClient $httpClient;
    private Queen $queen;
    private array $options;
    private bool $subscribed = false;
    private bool $closed = false;

    private string $popPath;
    /**
     * Everything subscribe() resolved that a per-call pop still needs. Held as
     * VALUES rather than as a rendered query string because both consume
     * surfaces override the sizing per call, and pop autopilot has to see the
     * final numbers: patching `batch` into an already-rendered string would send
     * `autopilot=true` next to two pinned knobs, which is a request that asks
     * the broker to decide nothing.
     */
    private array $popArgs = [];
    private ?string $affinityKey;
    /** [requested, queue, group, namespace, task] — see ConflationGuard. */
    private array $conflationScope = [false, null, null, null, null];

    public function __construct(HttpClient $httpClient, Queen $queen, array $options)
    {
        $this->httpClient = $httpClient;
        $this->queen = $queen;
        $this->options = $options;
    }

    /**
     * Subscribe to the queue (start consuming).
     * Must be called before consume().
     */
    public function subscribe(): void
    {
        $queue = $this->options['queue'] ?? null;
        $partition = $this->options['partition'] ?? null;
        $namespace = $this->options['namespace'] ?? null;
        $task = $this->options['task'] ?? null;
        $group = $this->options['group'] ?? null;
        // null means the user said nothing about this dimension, which is what
        // pop autopilot acts on (see ConsumerManager for the full note).
        $batch = $this->options['batch'] ?? null;
        $wait = $this->options['wait'] ?? true;
        $timeoutMillis = $this->options['timeoutMillis'] ?? 30000;
        $subscriptionMode = $this->options['subscriptionMode'] ?? null;
        $subscriptionFrom = $this->options['subscriptionFrom'] ?? null;
        $maxPartitions = $this->options['maxPartitions'] ?? null;
        $autopilot = $this->options['autopilot'] ?? !$this->queen->autopilotOff();
        $conflation = $this->options['conflation'] ?? false;

        $this->popPath = $this->buildPath($queue, $partition, $namespace, $task);
        $this->popArgs = [
            'group' => $group,
            'subscriptionMode' => $subscriptionMode,
            'subscriptionFrom' => $subscriptionFrom,
            'namespace' => $namespace,
            'task' => $task,
            'maxPartitions' => $maxPartitions,
            'conflation' => $conflation,
            'autopilot' => $autopilot,
        ];
        $this->affinityKey = $this->getAffinityKey($queue, $partition, $namespace, $task, $group);
        $this->conflationScope = [$conflation, $queue, $group, $namespace, $task];
        $this->subscribed = true;

        // Install signal handlers for graceful shutdown
        if (function_exists('pcntl_signal')) {
            pcntl_signal(SIGINT, function () {
                $this->closed = true;
            });
            pcntl_signal(SIGTERM, function () {
                $this->closed = true;
            });
        }
    }

    /**
     * Consume a single message from the queue.
     * Returns null if no message available within timeout.
     *
     * @param int $timeoutMs Timeout in milliseconds to wait for a message
     * @return array|null A single message array, or null if no message
     */
    public function consume(int $timeoutMs = 1000): ?array
    {
        $this->ensureSubscribed();

        if ($this->closed) {
            return null;
        }

        if (function_exists('pcntl_signal_dispatch')) {
            pcntl_signal_dispatch();
        }

        if ($this->closed) {
            return null;
        }

        // This surface hands back exactly one message, so batch=1 is a decision
        // of the SDK's and travels as one -- pop autopilot then has only the
        // sweep width left to choose, unless the caller pinned that too.
        $queryString = $this->popQuery(1, $timeoutMs);

        try {
            $clientTimeout = $timeoutMs + 5000;
            // Always a long-poll (wait=true above): a 429 backs off and keeps
            // waiting instead of exhausting the bounded push-like budget.
            $result = $this->httpClient->get("{$this->popPath}?{$queryString}", $clientTimeout, $this->affinityKey, Retry429Policy::KIND_POP);

            // Ahead of the empty-response shortcut: an old broker answers an
            // empty pop with a bodiless 204, which arrives here as null, and
            // that is the response the degrade-loudly check exists to catch
            // (PLAN_CONFLATION §4).
            ConflationGuard::check($result, ...$this->conflationScope);

            if (!$result || !isset($result['messages']) || empty($result['messages'])) {
                return null;
            }

            $messages = array_filter($result['messages'], fn($msg) => $msg !== null);
            $messages = array_values($messages);

            if (empty($messages)) {
                return null;
            }

            $message = $messages[0];
            $this->enhanceMessageWithTrace($message);

            return $message;
        } catch (\Throwable $error) {
            // Terminal by design (PLAN_CONFLATION §4), and re-thrown by TYPE
            // ahead of the message-matching branches: a consumer that asked for
            // conflation and is not getting it must stop, not poll on.
            if ($error instanceof ConflationUnsupportedException) {
                throw $error;
            }

            // Timeouts are normal for long polling
            if (str_contains($error->getMessage(), 'timeout') || str_contains($error->getMessage(), 'timed out')) {
                return null;
            }

            // Network errors — return null, caller can retry
            if (str_contains($error->getMessage(), 'Connection refused') || str_contains($error->getMessage(), 'cURL error')) {
                return null;
            }

            throw $error;
        }
    }

    /**
     * Consume a batch of messages from the queue.
     * Returns empty array if no messages available within timeout.
     *
     * @param int $timeoutMs Timeout in milliseconds
     * @param int $maxMessages Maximum number of messages to return
     * @return array Array of message arrays
     */
    public function consumeBatch(int $timeoutMs = 1000, int $maxMessages = 10): array
    {
        $this->ensureSubscribed();

        if ($this->closed) {
            return [];
        }

        if (function_exists('pcntl_signal_dispatch')) {
            pcntl_signal_dispatch();
        }

        if ($this->closed) {
            return [];
        }

        // $maxMessages IS the batch this call wants, so it travels as a pin.
        $queryString = $this->popQuery($maxMessages, $timeoutMs);

        try {
            $clientTimeout = $timeoutMs + 5000;
            $result = $this->httpClient->get("{$this->popPath}?{$queryString}", $clientTimeout, $this->affinityKey, Retry429Policy::KIND_POP);

            // See consume(): the check goes ahead of the empty shortcut so an
            // old broker's bodiless 204 cannot pass for a quiet queue.
            ConflationGuard::check($result, ...$this->conflationScope);

            if (!$result || !isset($result['messages']) || empty($result['messages'])) {
                return [];
            }

            $messages = array_values(array_filter($result['messages'], fn($msg) => $msg !== null));

            foreach ($messages as &$msg) {
                $this->enhanceMessageWithTrace($msg);
            }
            unset($msg);

            return $messages;
        } catch (\Throwable $error) {
            if ($error instanceof ConflationUnsupportedException) {
                throw $error;
            }
            if (str_contains($error->getMessage(), 'timeout') || str_contains($error->getMessage(), 'timed out')) {
                return [];
            }
            if (str_contains($error->getMessage(), 'Connection refused') || str_contains($error->getMessage(), 'cURL error')) {
                return [];
            }
            throw $error;
        }
    }

    /**
     * Acknowledge a message (or array of messages).
     *
     * @param array $message A single message or array of messages
     * @param bool $success Whether processing succeeded
     */
    public function ack(array $message, bool $success = true): array
    {
        $context = [];
        $group = $this->options['group'] ?? null;
        if ($group !== null) {
            $context['group'] = $group;
        }

        return $this->queen->ack($message, $success, $context);
    }

    /**
     * Negative-acknowledge a message (mark as failed).
     */
    public function nack(array $message): array
    {
        return $this->ack($message, false);
    }

    /**
     * Renew lease for a message (or array of messages).
     */
    public function renewLease(array|string $messageOrLeaseId): array
    {
        return $this->queen->renew($messageOrLeaseId);
    }

    /**
     * Check if the consumer has been closed (via signal or close()).
     */
    public function isClosed(): bool
    {
        if (function_exists('pcntl_signal_dispatch')) {
            pcntl_signal_dispatch();
        }
        return $this->closed;
    }

    /**
     * Close the consumer.
     */
    public function close(): void
    {
        $this->closed = true;
        $this->subscribed = false;
    }

    private function ensureSubscribed(): void
    {
        if ($this->closed) {
            return; // Allow graceful exit
        }
        if (!$this->subscribed) {
            throw new \RuntimeException('Consumer not subscribed. Call subscribe() before consume().');
        }
    }

    private function enhanceMessageWithTrace(array &$message): void
    {
        $httpClient = $this->httpClient;
        $consumerGroup = $this->options['group'] ?? '__QUEUE_MODE__';

        $message['trace'] = function (array $traceConfig) use ($httpClient, $consumerGroup, $message): array {
            try {
                if (!isset($traceConfig['data'])) {
                    return ['success' => false, 'error' => 'Invalid trace config: requires data key'];
                }

                $traceNames = null;
                if (isset($traceConfig['traceName'])) {
                    $traceNames = is_array($traceConfig['traceName'])
                        ? array_filter($traceConfig['traceName'], fn($n) => is_string($n) && strlen($n) > 0)
                        : [$traceConfig['traceName']];
                    if (empty($traceNames)) {
                        $traceNames = null;
                    }
                }

                $response = $httpClient->post('/api/v1/traces', [
                    'transactionId' => $message['transactionId'],
                    'partitionId' => $message['partitionId'],
                    'consumerGroup' => $consumerGroup,
                    'traceNames' => $traceNames,
                    'eventType' => $traceConfig['eventType'] ?? 'info',
                    'data' => $traceConfig['data'],
                ]);

                return array_merge(['success' => true], $response ?? []);
            } catch (\Throwable $error) {
                return ['success' => false, 'error' => $error->getMessage()];
            }
        };
    }

    private function getAffinityKey(?string $queue, ?string $partition, ?string $namespace, ?string $task, ?string $group): ?string
    {
        if ($queue !== null) {
            return "{$queue}:" . ($partition ?? '*') . ':' . ($group ?? '__QUEUE_MODE__');
        }
        if ($namespace !== null || $task !== null) {
            return ($namespace ?? '*') . ':' . ($task ?? '*') . ':' . ($group ?? '__QUEUE_MODE__');
        }
        return null;
    }

    private function buildPath(?string $queue, ?string $partition, ?string $namespace, ?string $task): string
    {
        if ($queue !== null) {
            if ($partition !== null) {
                return "/api/v1/pop/queue/{$queue}/partition/{$partition}";
            }
            return "/api/v1/pop/queue/{$queue}";
        }
        if ($namespace !== null || $task !== null) {
            return '/api/v1/pop';
        }
        throw new \RuntimeException('Must specify queue, namespace, or task');
    }

    /**
     * One pop's query string: what subscribe() resolved, with the batch and
     * timeout this call is asking for. Always a long poll -- both consume
     * surfaces block for the timeout they were given.
     */
    private function popQuery(int $batch, int $timeoutMs): string
    {
        $a = $this->popArgs;

        return $this->buildParams(
            $batch,
            true,
            $timeoutMs,
            $a['group'] ?? null,
            $a['subscriptionMode'] ?? null,
            $a['subscriptionFrom'] ?? null,
            $a['namespace'] ?? null,
            $a['task'] ?? null,
            $a['maxPartitions'] ?? null,
            $a['conflation'] ?? false,
            $a['autopilot'] ?? true
        );
    }

    private function buildParams(
        ?int $batch,
        bool $wait,
        int $timeoutMillis,
        ?string $group,
        ?string $subscriptionMode,
        ?string $subscriptionFrom,
        ?string $namespace,
        ?string $task,
        ?int $maxPartitions = null,
        bool $conflation = false,
        bool $autopilot = true,
    ): string {
        // Batch, partitions and with them the autopilot flag. null/0 means the
        // user set nothing (QueueBuilder leaves it that way on purpose), which is
        // the dimension the broker gets to choose. THE RULE lives in one place
        // (Support\PopAutopilot) precisely because this SDK has THREE pop param
        // builders -- PLAN_CONFLATION §4 opens on that hazard by name; only the
        // placement of the keys is here, and it is the pre-autopilot placement so
        // an autopilot-off request is byte-identical.
        $sizing = PopAutopilot::sizing($batch, $maxPartitions, Defaults::CONSUME_DEFAULTS['batch'], $autopilot);

        $params = [];
        if ($sizing['autopilot']) {
            $params['autopilot'] = 'true';
        }
        if ($sizing['batch'] !== null) {
            $params['batch'] = $sizing['batch'];
        }
        $params['wait'] = $wait ? 'true' : 'false';
        $params['timeout'] = (string) $timeoutMillis;

        if ($group !== null) {
            $params['consumerGroup'] = $group;
        }
        if ($subscriptionMode !== null) {
            $params['subscriptionMode'] = $subscriptionMode;
        }
        if ($subscriptionFrom !== null) {
            $params['subscriptionFrom'] = $subscriptionFrom;
        }
        if ($namespace !== null) {
            $params['namespace'] = $namespace;
        }
        if ($task !== null) {
            $params['task'] = $task;
        }
        // v4 multi-partition pop: drain up to N sparse partitions per call. Under
        // autopilot a pinned width travels even when it is 1, because 1 is then a
        // decision and not the absence of one.
        if ($sizing['partitions'] !== null) {
            $params['partitions'] = $sizing['partitions'];
        }
        // Last-value delivery, sent only when true: the broker treats presence
        // as opt-in, and conflation=false would read as a DISAGREEMENT with a
        // group whose stored policy is true.
        if ($conflation) {
            $params['conflation'] = 'true';
        }

        return http_build_query($params);
    }
}
