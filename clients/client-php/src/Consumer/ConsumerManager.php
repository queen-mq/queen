<?php

namespace Queen\Consumer;

use Queen\Exceptions\ConflationUnsupportedException;
use Queen\Exceptions\HttpException;
use Queen\Http\HttpClient;
use Queen\Http\Retry429Policy;
use Queen\Queen;
use Queen\Support\ConflationGuard;
use Queen\Support\Defaults;
use Queen\Support\PopAutopilot;
use GuzzleHttp\Promise\Utils as PromiseUtils;

class ConsumerManager
{
    private HttpClient $httpClient;
    private Queen $queen;
    private ?Supervision $supervision = null;
    /** Whether the handler takes a second argument, the lease renewal call. */
    private bool $handlerTakesRenew = false;

    public function __construct(HttpClient $httpClient, Queen $queen)
    {
        $this->httpClient = $httpClient;
        $this->queen = $queen;
    }

    public function start(\Closure $handler, array $options): void
    {
        $this->supervision = isset($options['supervision'])
            ? new Supervision($this->httpClient, $options['supervision'], $options) : null;
        $this->handlerTakesRenew = self::takesRenew($handler);
        $queue = $options['queue'] ?? null;
        $partition = $options['partition'] ?? null;
        $namespace = $options['namespace'] ?? null;
        $task = $options['task'] ?? null;
        $group = $options['group'] ?? null;
        $concurrency = $options['concurrency'] ?? 1;
        // null means the user said nothing about this dimension, which is what
        // pop autopilot acts on. The historical default of 1 is applied at
        // emission time instead (Support\PopAutopilot), and only when autopilot
        // is off, so that "never called batch()" and "called batch(1)" stay
        // distinguishable all the way to the wire.
        $batch = $options['batch'] ?? null;
        $limit = $options['limit'] ?? null;
        $idleMillis = $options['idleMillis'] ?? null;
        $autoAck = $options['autoAck'] ?? true;
        $wait = $options['wait'] ?? true;
        $timeoutMillis = $options['timeoutMillis'] ?? 30000;
        $leaseSeconds = $options['leaseSeconds'] ?? null;
        $renewLease = $options['renewLease'] ?? false;
        $renewLeaseIntervalMillis = $options['renewLeaseIntervalMillis'] ?? null;
        $each = $options['each'] ?? false;
        $subscriptionMode = $options['subscriptionMode'] ?? null;
        $subscriptionFrom = $options['subscriptionFrom'] ?? null;
        $maxPartitions = $options['maxPartitions'] ?? null;
        // The caller's explicit decision if there is one, otherwise the
        // client-wide default settled in the Queen constructor. The builder path
        // has already resolved it; the null case is for callers that drive this
        // manager with options of their own.
        $autopilot = $options['autopilot'] ?? !$this->queen->autopilotOff();
        $conflation = $options['conflation'] ?? false;

        $path = $this->buildPath($queue, $partition, $namespace, $task);
        $baseParams = $this->buildParams(
            $batch,
            $wait,
            $timeoutMillis,
            $group,
            $subscriptionMode,
            $subscriptionFrom,
            $namespace,
            $task,
            $maxPartitions,
            $conflation,
            $leaseSeconds,
            $autopilot,
        );
        $affinityKey = $this->getAffinityKey($queue, $partition, $namespace, $task, $group);
        // The identity the conflation checks report against: what was asked for,
        // and which (queue, group) pair a declaration conflict belongs to.
        $conflationScope = [$conflation, $queue, $group, $namespace, $task];

        // Install signal handlers, saving previous handlers for restoration
        $running = true;
        $prevSigint = null;
        $prevSigterm = null;
        if (function_exists('pcntl_signal')) {
            $prevSigint = pcntl_signal_get_handler(SIGINT);
            $prevSigterm = pcntl_signal_get_handler(SIGTERM);
            pcntl_signal(SIGINT, function () use (&$running) {
                $running = false;
            });
            pcntl_signal(SIGTERM, function () use (&$running) {
                $running = false;
            });
        }

        try {
            $this->supervision?->setRunning($concurrency);
            $this->supervision?->publish(force: true);
            if ($concurrency <= 1) {
                $this->worker(
                    $handler, $path, $baseParams, $batch, $limit, $idleMillis,
                    $autoAck, $wait, $timeoutMillis, $renewLease, $renewLeaseIntervalMillis,
                    $each, $group, $affinityKey, $running, $conflationScope
                );
            } else {
                $this->concurrentWorkers(
                    $concurrency, $handler, $path, $baseParams, $batch, $limit, $idleMillis,
                    $autoAck, $wait, $timeoutMillis, $renewLease, $renewLeaseIntervalMillis,
                    $each, $group, $affinityKey, $running, $conflationScope
                );
            }
        } finally {
            $this->supervision?->stop();
            // Restore previous signal handlers
            if (function_exists('pcntl_signal')) {
                if ($prevSigint !== null) {
                    pcntl_signal(SIGINT, $prevSigint);
                }
                if ($prevSigterm !== null) {
                    pcntl_signal(SIGTERM, $prevSigterm);
                }
            }
        }
    }

    /**
     * Run N concurrent workers using Guzzle async. All workers long-poll
     * concurrently via cURL multi-handle; processing remains sequential
     * per-worker but polls overlap.
     */
    private function concurrentWorkers(
        int $concurrency,
        \Closure $handler,
        string $path,
        string $baseParams,
        // Unused by the worker itself: it is the pop's message budget, and with pop
        // autopilot null means the broker sized it (Support\PopAutopilot).
        ?int $batch,
        ?int $limit,
        ?int $idleMillis,
        bool $autoAck,
        bool $wait,
        int $timeoutMillis,
        bool $renewLease,
        ?int $renewLeaseIntervalMillis,
        bool $each,
        ?string $group,
        ?string $affinityKey,
        bool &$running,
        array $conflationScope = [false, null, null, null, null],
    ): void {
        // Per-worker state
        $workerProcessed = array_fill(0, $concurrency, 0);
        $workerLastMsg = array_fill(0, $concurrency, $idleMillis !== null ? $this->nowMillis() : null);
        $perWorkerLimit = $limit !== null ? (int) ceil($limit / $concurrency) : null;
        $clientTimeout = $wait ? $timeoutMillis + 5000 : $timeoutMillis;
        $url = "{$path}?{$baseParams}";
        $pollPolicy = $this->httpClient->getRetry429Policy($wait ? Retry429Policy::KIND_POP : null);
        $consecutiveBackoffs = 0;

        while ($running) {
            if (function_exists('pcntl_signal_dispatch')) {
                pcntl_signal_dispatch();
            }
            if (!$running) {
                break;
            }

            // Determine which workers should poll
            $activeWorkers = [];
            for ($w = 0; $w < $concurrency; $w++) {
                if ($perWorkerLimit !== null && $workerProcessed[$w] >= $perWorkerLimit) {
                    continue;
                }
                if ($idleMillis !== null && $workerLastMsg[$w] !== null) {
                    if ($this->nowMillis() - $workerLastMsg[$w] >= $idleMillis) {
                        continue;
                    }
                }
                $activeWorkers[] = $w;
            }

            $this->supervision?->setRunning(count($activeWorkers));
            $this->supervision?->publish();

            if (empty($activeWorkers)) {
                break; // All workers done
            }

            // Fire N concurrent long-poll requests
            $promises = [];
            foreach ($activeWorkers as $w) {
                $promises[$w] = $this->httpClient->getAsync($url, $clientTimeout, $affinityKey);
            }

            // Settle all — don't throw on individual failures
            $results = HttpClient::settleAll($promises);
            // Every pop of this round has answered, so its lease runs from here
            // at the latest. The results are handled one after the other: a
            // batch that waits behind the handlers before it must count that
            // wait toward its renewal interval.
            $poppedAt = $this->nowMillis();

            if (function_exists('pcntl_signal_dispatch')) {
                pcntl_signal_dispatch();
            }
            if (!$running) {
                break;
            }

            // Process results per worker
            $backoffError = null;
            foreach ($results as $w => $outcome) {
                if (!$running) {
                    break;
                }

                if ($outcome['state'] === 'rejected') {
                    $error = $outcome['reason'];

                    // 429, or a transient 5xx such as the 503 of a leader
                    // election: the async path can't retry in flight without
                    // blocking every other worker on the shared multi-handle,
                    // so remember it and pace the next poll round once, below.
                    if ($error instanceof HttpException && ($error->statusCode === 429 || $error->isTransient())) {
                        $backoffError = $error;
                        continue;
                    }

                    $isTimeout = str_contains($error->getMessage(), 'timeout') || str_contains($error->getMessage(), 'timed out');
                    if ($isTimeout && $wait) {
                        continue; // Normal for long polling
                    }
                    // An answer that is not the broker's (empty or malformed, as a
                    // gateway's during a rollout) passes like a network error.
                    $isNetwork = str_contains($error->getMessage(), 'Connection refused') || str_contains($error->getMessage(), 'cURL error')
                        || $error instanceof \UnexpectedValueException;
                    if ($isNetwork) {
                        usleep(1_000_000);
                        continue;
                    }
                    // 403 (forbidden) included: cluster_suspended and the other
                    // terminal codes never resolve on their own, so surface
                    // them (with ->errorCode) instead of retrying.
                    throw $error;
                }

                $result = $outcome['value'];

                // Ahead of the empty-response shortcut: an old broker answers an
                // empty pop with a bodiless 204, which arrives here as null, and
                // that is the response the degrade-loudly check exists to catch
                // (PLAN_CONFLATION §4). Raising here leaves the poll round and
                // stops the consumer, which is the point — the alternative is
                // draining a backlog one message at a time in silence.
                ConflationGuard::check($result, ...$conflationScope);

                if (!$result || !isset($result['messages']) || empty($result['messages'])) {
                    if (!$wait) {
                        // The broker's advised pacing when this pop engaged
                        // autopilot and the broker had an opinion (it knows the
                        // arrival rate on this queue and this client does not),
                        // otherwise the historical 100ms.
                        usleep(PopAutopilot::emptyPollDelayMicros(PopAutopilot::decision($result)));
                    }
                    continue;
                }

                $messages = array_values(array_filter($result['messages'], fn($msg) => $msg !== null));
                if (empty($messages)) {
                    continue;
                }

                if ($idleMillis !== null) {
                    $workerLastMsg[$w] = $this->nowMillis();
                }

                $this->enhanceMessagesWithTrace($messages, $group);

                $leaseRenewalTime = null;
                if ($renewLease && $renewLeaseIntervalMillis !== null) {
                    $leaseRenewalTime = $poppedAt + $renewLeaseIntervalMillis;
                }
                $renew = $this->renewer($messages, $leaseRenewalTime, $renewLeaseIntervalMillis);

                if ($each) {
                    foreach ($messages as $message) {
                        if (!$running) {
                            break;
                        }
                        $this->renewLeaseIfNeeded($messages, $leaseRenewalTime, $renewLeaseIntervalMillis);
                        $this->processMessage($message, $handler, $autoAck, $group, $affinityKey, $renew);
                        $workerProcessed[$w]++;
                        if ($perWorkerLimit !== null && $workerProcessed[$w] >= $perWorkerLimit) {
                            break;
                        }
                    }
                } else {
                    $this->renewLeaseIfNeeded($messages, $leaseRenewalTime, $renewLeaseIntervalMillis);
                    $this->processBatch($messages, $handler, $autoAck, $group, $affinityKey, $renew);
                    $workerProcessed[$w] += count($messages);
                }
            }

            // One backoff for the whole round: every worker shares the tenant
            // bucket, so N sleeps would only stall the poll N times over.
            if ($backoffError !== null) {
                usleep($pollPolicy->delayMillis($consecutiveBackoffs, $backoffError->retryAfterSeconds) * 1000);
                $consecutiveBackoffs++;
            } else {
                $consecutiveBackoffs = 0;
            }

            // Check global limit
            if ($limit !== null && array_sum($workerProcessed) >= $limit) {
                break;
            }
        }
    }

    private function worker(
        \Closure $handler,
        string $path,
        string $baseParams,
        // Unused by the worker itself: it is the pop's message budget, and with pop
        // autopilot null means the broker sized it (Support\PopAutopilot).
        ?int $batch,
        ?int $limit,
        ?int $idleMillis,
        bool $autoAck,
        bool $wait,
        int $timeoutMillis,
        bool $renewLease,
        ?int $renewLeaseIntervalMillis,
        bool $each,
        ?string $group,
        ?string $affinityKey,
        bool &$running,
        array $conflationScope = [false, null, null, null, null],
    ): void {
        $processedCount = 0;
        $lastMessageTime = $idleMillis !== null ? $this->nowMillis() : null;
        $retryKind = $wait ? Retry429Policy::KIND_POP : null;
        $pollPolicy = $this->httpClient->getRetry429Policy($retryKind);
        $consecutiveBackoffs = 0;

        while ($running) {
            if (function_exists('pcntl_signal_dispatch')) {
                pcntl_signal_dispatch();
            }

            if (!$running) {
                break;
            }

            if ($limit !== null && $processedCount >= $limit) {
                break;
            }

            if ($idleMillis !== null && $lastMessageTime !== null) {
                $idleTime = $this->nowMillis() - $lastMessageTime;
                if ($idleTime >= $idleMillis) {
                    break;
                }
            }

            try {
                $this->supervision?->publish();
                $clientTimeout = $wait ? $timeoutMillis + 5000 : $timeoutMillis;
                // wait=true is a long-poll: mark it so a 429 backs off and keeps
                // waiting instead of giving up after the bounded push-like budget.
                $result = $this->httpClient->get("{$path}?{$baseParams}", $clientTimeout, $affinityKey, $retryKind);
                $consecutiveBackoffs = 0;

                // Ahead of the empty-response shortcut: an old broker answers an
                // empty pop with a bodiless 204, which arrives here as null, and
                // that is the response the degrade-loudly check exists to catch
                // (PLAN_CONFLATION §4).
                ConflationGuard::check($result, ...$conflationScope);

                if (!$result || !isset($result['messages']) || empty($result['messages'])) {
                    if (!$wait) {
                        // The broker's advised pacing when this pop engaged
                        // autopilot and the broker had an opinion (it knows the
                        // arrival rate on this queue and this client does not),
                        // otherwise the historical 100ms.
                        usleep(PopAutopilot::emptyPollDelayMicros(PopAutopilot::decision($result)));
                    }
                    continue;
                }

                $messages = array_values(array_filter($result['messages'], fn($msg) => $msg !== null));

                if (empty($messages)) {
                    continue;
                }

                if ($idleMillis !== null) {
                    $lastMessageTime = $this->nowMillis();
                }

                $this->enhanceMessagesWithTrace($messages, $group);

                $leaseRenewalTime = null;
                if ($renewLease && $renewLeaseIntervalMillis !== null) {
                    $leaseRenewalTime = $this->nowMillis() + $renewLeaseIntervalMillis;
                }
                $renew = $this->renewer($messages, $leaseRenewalTime, $renewLeaseIntervalMillis);

                if ($each) {
                    foreach ($messages as $message) {
                        if (!$running) {
                            break;
                        }

                        $this->renewLeaseIfNeeded($messages, $leaseRenewalTime, $renewLeaseIntervalMillis);
                        $this->processMessage($message, $handler, $autoAck, $group, $affinityKey, $renew);
                        $processedCount++;

                        if ($limit !== null && $processedCount >= $limit) {
                            break;
                        }
                    }
                } else {
                    $this->renewLeaseIfNeeded($messages, $leaseRenewalTime, $renewLeaseIntervalMillis);
                    $this->processBatch($messages, $handler, $autoAck, $group, $affinityKey, $renew);
                    $processedCount += count($messages);
                }
            } catch (\Throwable $error) {
                // Degrade-loudly (PLAN_CONFLATION §4) is terminal by design and
                // is re-thrown by TYPE, ahead of the message-matching branches
                // below: a consumer that asked for conflation and is not getting
                // it must stop, never back off and poll again.
                if ($error instanceof ConflationUnsupportedException) {
                    throw $error;
                }

                // 429: HttpClient already retried this with backoff (unbounded
                // for a wait=true poll), so getting here means an explicit
                // maxAttempts override ran out. Keep polling behind the same
                // backoff rather than hot-looping against the limiter.
                // A transient 5xx (the 503 of a leader election, which lasts
                // a few seconds, or a pop whose outcome the broker could not
                // tell) backs off the same way, at the pace of Retry-After,
                // instead of ending the consumer.
                if ($error instanceof HttpException && ($error->statusCode === 429 || $error->isTransient())) {
                    usleep($pollPolicy->delayMillis($consecutiveBackoffs, $error->retryAfterSeconds) * 1000);
                    $consecutiveBackoffs++;
                    continue;
                }

                $isTimeout = str_contains($error->getMessage(), 'timeout') || str_contains($error->getMessage(), 'timed out');
                if ($isTimeout && $wait) {
                    continue;
                }

                // An answer that is not the broker's (empty or malformed, as a
                // gateway's during a rollout) passes like a network error.
                $isNetwork = str_contains($error->getMessage(), 'Connection refused') || str_contains($error->getMessage(), 'cURL error')
                    || $error instanceof \UnexpectedValueException;
                if ($isNetwork) {
                    usleep(1_000_000);
                    continue;
                }

                // 403 (forbidden) falls through here: cluster_suspended and the
                // other terminal codes never resolve on their own, so surface
                // them (with ->errorCode) instead of retrying.
                throw $error;
            }
        }
    }

    private function processMessage(
        array $message,
        \Closure $handler,
        bool $autoAck,
        ?string $group,
        ?string $affinityKey,
        \Closure $renew,
    ): void
    {
        try {
            if ($this->supervision !== null) {
                $this->supervision->invoke(fn() => $this->handlerTakesRenew ? $handler($message, $renew) : $handler($message));
            } else {
                $this->handlerTakesRenew ? $handler($message, $renew) : $handler($message);
            }

            if ($autoAck) {
                $this->acknowledge($message, true, $group, $affinityKey);
            }
        } catch (\Throwable $error) {
            if ($autoAck) {
                $this->acknowledge($message, false, $group, $affinityKey);
                return;
            }
            throw $error;
        }
    }

    private function processBatch(
        array $messages,
        \Closure $handler,
        bool $autoAck,
        ?string $group,
        ?string $affinityKey,
        \Closure $renew,
    ): void
    {
        try {
            if ($this->supervision !== null) {
                $this->supervision->invoke(fn() => $this->handlerTakesRenew ? $handler($messages, $renew) : $handler($messages));
            } else {
                $this->handlerTakesRenew ? $handler($messages, $renew) : $handler($messages);
            }

            if ($autoAck) {
                $this->acknowledge($messages, true, $group, $affinityKey);
            }
        } catch (\Throwable $error) {
            if ($autoAck) {
                $this->acknowledge($messages, false, $group, $affinityKey);
                return;
            }
            throw $error;
        }
    }

    /**
     * Acknowledge what a handler took, after it returned or threw. Queen::ack()
     * returns a failure rather than throwing it, and the loop goes on either
     * way: the messages come back when their lease expires, and this log line
     * is the one place that says why.
     *
     * @param array $messages One message, or a list of them.
     */
    private function acknowledge(array $messages, bool $completed, ?string $group, ?string $affinityKey): void
    {
        $context = $group !== null ? ['group' => $group] : [];
        if ($affinityKey !== null) {
            $context['affinityKey'] = $affinityKey;
        }

        $result = $this->queen->ack($messages, $completed, $context);
        if (($result['success'] ?? false) === true) {
            return;
        }

        $error = $result['error'] ?? 'no reason given';
        error_log(sprintf(
            'Queen consume() could not acknowledge %d message(s) as %s: %s; the broker delivers them again when their lease expires',
            array_is_list($messages) ? count($messages) : 1,
            $completed ? 'completed' : 'failed',
            is_string($error) ? $error : (string) json_encode($error),
        ));
    }

    /**
     * Whether a handler declares a second parameter: only such a handler gets
     * the renewal call, so a one-parameter callable keeps its exact call.
     */
    public static function takesRenew(\Closure $handler): bool
    {
        $signature = new \ReflectionFunction($handler);

        return $signature->isVariadic() || $signature->getNumberOfParameters() >= 2;
    }

    /**
     * The handler's second argument, when it takes one: a call that renews the
     * lease of the messages in hand and says whether the broker extended it.
     * PHP runs the handler on the loop's only thread, so the loop cannot renew
     * while a handler runs; a long handler calls this between parts of its
     * work. With renewLease(true, $intervalMillis) it renews once the interval
     * has passed since the pop or the last renewal, and returns false when
     * nothing was due; without it every call renews.
     */
    private function renewer(array $messages, ?int &$leaseRenewalTime, ?int $intervalMillis): \Closure
    {
        return function () use ($messages, &$leaseRenewalTime, $intervalMillis): bool {
            $this->supervision?->publish();
            if ($leaseRenewalTime !== null && $intervalMillis !== null) {
                if ($this->nowMillis() < $leaseRenewalTime) {
                    return false;
                }
                $leaseRenewalTime = $this->nowMillis() + $intervalMillis;
            }
            $results = $this->queen->renew($messages);
            $results = isset($results[0]) ? $results : [$results];

            return $results !== [] && array_filter($results, fn(array $result): bool => ($result['success'] ?? false) !== true) === [];
        };
    }

    private function renewLeaseIfNeeded(array $messages, ?int &$leaseRenewalTime, ?int $intervalMillis): void
    {
        if ($leaseRenewalTime === null || $intervalMillis === null) {
            return;
        }

        if ($this->nowMillis() >= $leaseRenewalTime) {
            // The renewals of the pop's leases, sent together, each bounded by the
            // renewal interval: past it another is due anyway, and a broker that
            // does not answer must not hold the handler for the whole request
            // timeout, longer than the lease. A renewal that failed, or renewed
            // nothing, leaves the message to another consumer once its lease
            // expires while this handler still runs: the log says so.
            try {
                // One renewal per lease: the messages of a pop share it.
                $leaseIds = array_values(array_unique(array_filter(array_column($messages, 'leaseId'), fn($id) => $id !== null)));
                $promises = [];
                foreach ($leaseIds as $leaseId) {
                    $promises[(string) $leaseId] = $this->httpClient->postAsync(
                        '/api/v1/lease/' . rawurlencode((string) $leaseId) . '/extend',
                        [],
                        max(1_000, $intervalMillis),
                    );
                }
                foreach ($promises === [] ? [] : HttpClient::settleAll($promises) as $leaseId => $outcome) {
                    $renewed = ($outcome['state'] ?? null) === 'fulfilled' ? ($outcome['value']['renewed'] ?? null) : null;
                    if (is_int($renewed) && $renewed > 0) {
                        continue;
                    }
                    $reason = ($outcome['reason'] ?? null) instanceof \Throwable
                        ? $outcome['reason']->getMessage()
                        : 'the broker renewed no lease';
                    error_log(sprintf(
                        'Queen consume() could not renew lease %s: %s; another consumer may get its messages once it expires',
                        $leaseId,
                        $reason,
                    ));
                }
            } catch (\Throwable $e) {
                // Lease renewal failure is non-fatal
            }
            $leaseRenewalTime = $this->nowMillis() + $intervalMillis;
        }
    }

    private function enhanceMessagesWithTrace(array &$messages, ?string $group): void
    {
        $httpClient = $this->httpClient;
        $consumerGroup = $group ?? '__QUEUE_MODE__';

        foreach ($messages as &$message) {
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
        unset($message);
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
                return '/api/v1/pop/queue/' . rawurlencode($queue)
                    . '/partition/' . rawurlencode($partition);
            }
            return '/api/v1/pop/queue/' . rawurlencode($queue);
        }

        if ($namespace !== null || $task !== null) {
            return '/api/v1/pop';
        }

        throw new \RuntimeException('Must specify queue, namespace, or task');
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
        ?int $leaseSeconds = null,
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
        if ($leaseSeconds !== null) {
            $params['leaseSeconds'] = (string) $leaseSeconds;
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

    private function nowMillis(): int
    {
        return (int)(microtime(true) * 1000);
    }
}
