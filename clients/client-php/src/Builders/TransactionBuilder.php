<?php

namespace Queen\Builders;

use Queen\Http\HttpClient;
use Queen\Support\KvOp;
use Queen\Support\TimerOp;
use Queen\Support\Uuid;

class TransactionBuilder
{
    private HttpClient $httpClient;
    private array $operations = [];
    private array $requiredLeases = [];
    private array $kvOperations = [];
    private array $timerOperations = [];

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
     * Key/value writes and reads that commit WITH the acks and pushes of this
     * bundle, in one transaction.
     *
     * This — not `$queen->kv()` — is the primary fence. A marker written here
     * shares its fate with the ack: an expired lease fails the whole bundle and
     * the write never happened, which compare-and-swap cannot do, because an
     * `expect` on a still-matching version succeeds even from a zombie
     * consumer.
     *
     * `required: true` on an operation escalates a lost precondition into a
     * refusal of the WHOLE bundle. That is the idempotency idiom: put the
     * marker with required, and the redelivery that finds it already there does
     * not re-push and does not re-ack. commit() RETURNS that verdict rather
     * than throwing — see commit().
     *
     * getPrefix is deliberately absent: unbounded read work has no place inside
     * a transaction that holds the outermost lock space. get and getMany are
     * here because the caller fixes their cost.
     */
    public function kv(string $namespace): TransactionKvBuilder
    {
        return new TransactionKvBuilder($this, $namespace);
    }

    /**
     * Timers scheduled or cancelled WITH this bundle, in one transaction.
     *
     * The saga shape this exists for: ack the message, push the next step, and
     * schedule the compensation timer, atomically — so there is no window in
     * which the step happened and the timer did not.
     *
     * A cancel placed here inherits the transaction's class and can be refused
     * with the rest of the bundle. Timers::cancel() has a route that is never
     * blocked; use it when the cancel must land no matter what.
     */
    public function timers(string $queueName): TransactionTimerBuilder
    {
        return new TransactionTimerBuilder($this, $queueName);
    }

    /**
     * @internal Used by TransactionKvBuilder
     */
    public function addKvOperation(array $operation): void
    {
        $this->kvOperations[] = $operation;
    }

    /**
     * @internal Used by TransactionTimerBuilder
     */
    public function addTimerOperation(array $operation): void
    {
        $this->timerOperations[] = $operation;
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

    /**
     * @return array the broker's answer.
     *
     * THE ONE FAILURE THAT DOES NOT THROW. When an operation carried
     * `required: true` and lost its precondition, the broker answers HTTP 200
     * with success:false and reason:"kv_precondition", plus failedIndex,
     * kvReason, version and value. That is the EXPECTED outcome of every
     * legitimate redelivery — it is the idempotency marker doing its job — so
     * commit() RETURNS it. Throwing on the single most frequent outcome of the
     * product's number-one use case would put the happy path inside every
     * caller's catch block, and would put it in their error metrics and retry
     * policies too.
     *
     *     $result = $tx->ack($msg)
     *         ->kv('saga')->putIfAbsent($orderId, ['step' => 'reserved'],
     *                                   ['ttlSeconds' => 86400, 'required' => true])
     *         ->queue('payments')->push([['data' => $charge]])
     *         ->commit();
     *
     *     if (($result['reason'] ?? null) === 'kv_precondition') {
     *         // Somebody already did this. Nothing was pushed, nothing acked.
     *     }
     *
     * `failedIndex` is in the FLAT index space: `operations` first, then the kv
     * array, then timers.
     *
     * Every other failure still throws, exactly as before.
     */
    public function commit(): array
    {
        if (empty($this->operations) && empty($this->kvOperations) && empty($this->timerOperations)) {
            throw new \RuntimeException('Transaction has no operations to commit');
        }

        $body = [
            'operations' => $this->operations,
            'requiredLeases' => array_values(array_unique($this->requiredLeases)),
        ];

        // TOP-LEVEL arrays, never elements of `operations`, and OMITTED when
        // empty. Omitted and not null: `"kv": null` is what a naive optional
        // field produces, and the broker's jsonb_array_length would raise on a
        // transaction that works today. A bundle with no riders is byte-for-byte
        // the bundle this client has always sent.
        if ($this->kvOperations !== []) {
            $body['kv'] = array_values($this->kvOperations);
        }
        if ($this->timerOperations !== []) {
            $body['timers'] = array_values($this->timerOperations);
        }

        $result = $this->httpClient->post('/api/v1/transaction', $body);

        if (!($result['success'] ?? false)) {
            if (($result['reason'] ?? null) === 'kv_precondition') {
                return $result;
            }

            $error = $result['error'] ?? 'Transaction failed';
            $txId = $result['transactionId'] ?? null;
            $msg = $txId ? "Transaction {$txId} failed: {$error}" : "Transaction failed: {$error}";
            throw new \RuntimeException($msg);
        }

        return $result;
    }
}

/**
 * KV operations scoped to one namespace, inside a transaction.
 *
 * Every method returns the parent builder, so a bundle reads as one chain. The
 * operation shapes come from Queen\Support\KvOp, shared with the standalone
 * client, so the two surfaces cannot drift.
 */
class TransactionKvBuilder
{
    private TransactionBuilder $parent;
    private string $namespace;

    public function __construct(TransactionBuilder $parent, string $namespace)
    {
        $this->parent = $parent;
        $this->namespace = $namespace;
    }

    public function get(string $key): TransactionBuilder
    {
        $this->parent->addKvOperation(KvOp::get($this->namespace, $key));
        return $this->parent;
    }

    public function getMany(array $keys): TransactionBuilder
    {
        $this->parent->addKvOperation(KvOp::getMany($this->namespace, $keys));
        return $this->parent;
    }

    public function put(string $key, mixed $value, array $opts = []): TransactionBuilder
    {
        $this->parent->addKvOperation(KvOp::put($this->namespace, $key, $value, $opts));
        return $this->parent;
    }

    public function putIfAbsent(string $key, mixed $value, array $opts = []): TransactionBuilder
    {
        $this->parent->addKvOperation(KvOp::putIfAbsent($this->namespace, $key, $value, $opts));
        return $this->parent;
    }

    public function delete(string $key, array $opts = []): TransactionBuilder
    {
        $this->parent->addKvOperation(KvOp::delete($this->namespace, $key, $opts));
        return $this->parent;
    }

    public function incr(string $key, int|float $delta, array $opts = []): TransactionBuilder
    {
        $this->parent->addKvOperation(KvOp::incr($this->namespace, $key, $delta, $opts));
        return $this->parent;
    }
}

/**
 * Timer operations scoped to one destination queue, inside a transaction.
 */
class TransactionTimerBuilder
{
    private TransactionBuilder $parent;
    private string $queueName;

    public function __construct(TransactionBuilder $parent, string $queueName)
    {
        $this->parent = $parent;
        $this->queueName = $queueName;
    }

    public function schedule(string $timerKey, int $delayMs, mixed $payload, array $opts = []): TransactionBuilder
    {
        $this->parent->addTimerOperation(TimerOp::schedule($this->queueName, $timerKey, $delayMs, $payload, $opts));
        return $this->parent;
    }

    public function reschedule(string $timerKey, int $delayMs, mixed $payload, array $opts = []): TransactionBuilder
    {
        $this->parent->addTimerOperation(TimerOp::reschedule($this->queueName, $timerKey, $delayMs, $payload, $opts));
        return $this->parent;
    }

    /**
     * A cancel here shares the bundle's fate — and its class, so a blocked
     * cluster can refuse it. Timers::cancel() is the route that never is.
     */
    public function cancel(string $timerKey, ?string $txn = null): TransactionBuilder
    {
        $this->parent->addTimerOperation(TimerOp::cancel($this->queueName, $timerKey, $txn));
        return $this->parent;
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
