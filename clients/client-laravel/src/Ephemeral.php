<?php

namespace Queen;

use Queen\Buffer\BufferManager;
use Queen\Buffer\Destination;
use Queen\Buffer\Sink;
use Queen\Exceptions\EphemeralQueueNotFoundException;
use Queen\Exceptions\EphemeralUnsupportedException;
use Queen\Exceptions\ErrorCode;
use Queen\Exceptions\HttpException;
use Queen\Http\HttpClient;
use Queen\Http\Retry429Policy;

/**
 * RAM-class queues, reached through `$queen->ephemeral()`
 * (EPHEMERAL_QUEUES.md §1, §3.1, §4).
 *
 * Eight verbs over one route family, /api/v1/ephemeral/*: configure, reset,
 * delete, push, pop, ack, queues, depth. Flat methods, not a builder chain —
 * the durable queue('x')->partition('p')->push(...) fluency exists because a
 * durable queue has a dozen configured properties that read well as a sentence;
 * an ephemeral queue has a ring in a broker's RAM and a handful of bounds, and
 * a chain would only hide how few moving parts there are.
 *
 * WHAT THIS CLASS IS ABOUT, BEFORE ANY SIGNATURE: contents survive NOTHING
 * (§1.2). Not a restart, not a crash, not a deploy, not the ownership move a
 * membership change causes. Treat a failover like a Redis restart. Declared
 * CONFIGURATION is durable — it lives in PG and comes back after a restart, as
 * configured and EMPTY. There is no replay, no history, no subscriptionMode and
 * no DLQ, because none of those concepts has a referent when there is no
 * history to have.
 *
 * DELIVERY IS NOT "AT MOST ONCE" (§1.3), and the docs must not say it is. The
 * class picks what can be LOST; the ack mode picks the guarantee. `autoAck`
 * advances the cursor at delivery and is at-most-once. The default — explicit
 * ack — is at-least-once for as long as the owning broker incarnation lives: an
 * unacked message redelivers when its lease expires, with `attempts`
 * incremented, until `retryLimit`, after which it is DROPPED and counted.
 * Consumers still need idempotency, exactly as on durable queues.
 *
 * CONSUMPTION SEMANTICS COME FROM THE GROUP, EXACTLY AS ON THE DURABLE ENGINE
 * (§1.5). There is no queue-level mode to choose:
 *
 *     $eph->pop($q, ['group' => 'workers']);   // competing: one cursor
 *     $eph->pop($q, ['group' => 'tail-a']);    // fan-out: its own cursor
 *     $eph->pop($q);                           // groupless queue mode
 *
 * Every group has its own cursor over the ONE ring, so fan-out subscribers each
 * see everything and competing consumers of one group share the work.
 *
 * AND THE TWO KINDS OF 404, WHICH MUST NEVER BE CONFUSED FOR EACH OTHER. No SDK
 * negotiates a version, so against a broker or proxy older than 1.1 the whole
 * family answers 404 — the broker because the routes do not exist, the proxy
 * because an unknown API path is `route_blocked`. That is a DEPLOYMENT fact and
 * arrives as EphemeralUnsupportedException.
 *
 * But `depth` also answers a real 404, with `code: ephemeral_queue_not_found`,
 * when the queue simply is not there — and it is the only verb that can, since
 * push and pop create implicitly, `reset` answers dropped:0 and `delete` answers
 * deleted:false. That is a DATA fact and arrives as
 * EphemeralQueueNotFoundException. Collapsing it into the first would send
 * somebody chasing a broker version over a queue name typo.
 *
 * Both keep the broker's own refusal as the previous exception. Branch on the
 * type or on $errorCode, never on the prose.
 */
class Ephemeral
{
    /**
     * The seven knobs of configure (§3.1). A CLOSED list: an option this client
     * does not know is refused rather than dropped on the floor, because every
     * one of these bounds something (bytes, length, age, redelivery) and a
     * silently ignored `ttlSecond` is a ring that grows until a global budget
     * answers 503.
     */
    public const CONFIGURE_OPTIONS = [
        'maxBytes',
        'maxLength',
        'policy',
        'ttlSeconds',
        'leaseSeconds',
        'retryLimit',
        'windowBuffer',
    ];

    /** Long-poll default, matching the durable pop's, when wait is asked for without a timeout. */
    public const DEFAULT_WAIT_TIMEOUT_MILLIS = 30000;

    /**
     * The HTTP deadline must outlive the server's own long-poll timeout, or the
     * client aborts a request the broker was about to answer. Same 5s slack the
     * durable pop uses.
     */
    public const WAIT_TIMEOUT_SLACK_MILLIS = 5000;

    private HttpClient $httpClient;
    private ?BufferManager $bufferManager;

    public function __construct(HttpClient $httpClient, ?BufferManager $bufferManager = null)
    {
        $this->httpClient = $httpClient;
        $this->bufferManager = $bufferManager;
    }

    // ===========================
    // Declaration
    // ===========================

    /**
     * Declare a queue and its bounds. Persists the OPTIONS in PG (§1.1): the
     * configuration survives a restart, the contents never do, and the queue
     * comes back declared and empty.
     *
     * Optional in every sense — a push or a pop that names an unknown queue
     * creates it implicitly with the tenant defaults. Declare when you want
     * non-default bounds, or when you want the queue to exist in the dashboard
     * before its first message.
     *
     * @param array $options maxBytes / maxLength (the per-queue budget, with
     *   `policy` deciding whether breaching it rejects the push with 429 or
     *   drops the OLDEST message — feed semantics), ttlSeconds (drop messages
     *   older than this; it is NOT the durable `retention`, which cleans
     *   consumed history and never touches pending), leaseSeconds and
     *   retryLimit (redelivery), windowBuffer ['ms' => …, 'count' => …].
     *   An option this client does not know is REFUSED, never dropped.
     */
    public function configure(string $queue, array $options = []): mixed
    {
        $this->requireQueue($queue);

        return $this->call('POST', '/api/v1/ephemeral/configure', [
            'queue' => $queue,
            'options' => $this->buildConfigureOptions($options),
        ], queue: $queue);
    }

    /**
     * Drop every message, void every lease, rewind every group cursor. Answers
     * {dropped}.
     *
     * A verb that would be indefensible on a durable queue and is merely honest
     * here: it destroys nothing the class ever promised to keep (§1.2). The
     * declared configuration stays.
     */
    public function reset(string $queue): mixed
    {
        $this->requireQueue($queue);

        return $this->call('POST', '/api/v1/ephemeral/reset', ['queue' => $queue], queue: $queue);
    }

    /** Delete the queue: contents, cursors, and the declared configuration in PG. */
    public function delete(string $queue): mixed
    {
        $this->requireQueue($queue);

        return $this->call('DELETE', '/api/v1/ephemeral/queue/' . rawurlencode($queue), queue: $queue);
    }

    // ===========================
    // Push
    // ===========================

    /**
     * Push one message or many. All-or-nothing per request; answers {pushed}.
     *
     *     $queen->ephemeral()->push('presence', [['user' => 'a']]);
     *     $queen->ephemeral()->push('presence', $msgs, ['partition' => 'room-7']);
     *
     * @param mixed $messages One message or a list. Each may be a bare value,
     *   ['payload' => …] or ['data' => …] — the durable push's sugar,
     *   deliberately reproduced so one mental model covers both families,
     *   INCLUDING its trap: an array that happens to have a `data` key is read
     *   as the sugar and its other keys do not travel.
     * @param array $opts partition (picks the ring; omitted, the broker picks
     *   and this client does not invent a default), buffered (true or the
     *   buffer options — see §4.1 and the note on buffered pushes below).
     *
     * A buffered push answers ['buffered' => true, 'count' => n] once the
     * messages are IN the buffer, which in this SDK means they were accepted by
     * a bound that refuses to grow and were flushed inline if that bound or the
     * count threshold said so. A buffered message that has not flushed dies with
     * the process — already inside this class's contract, which is exactly why
     * buffering is a reasonable default here and a considered decision on a
     * durable queue.
     */
    public function push(string $queue, mixed $messages, array $opts = []): array
    {
        $this->requireQueue($queue);

        $items = $this->toMessages($messages);
        if (empty($items)) {
            return ['pushed' => 0];
        }

        $partition = $opts['partition'] ?? null;
        $buffered = $opts['buffered'] ?? null;

        if ($buffered !== null && $buffered !== false) {
            return $this->pushBuffered($queue, $partition, $items, $buffered);
        }

        $body = ['queue' => $queue];
        if ($partition !== null) {
            $body['partition'] = $partition;
        }
        $body['messages'] = $items;

        $result = $this->call('POST', Sink::EPHEMERAL_PATH, $body, queue: $queue);

        return is_array($result) ? $result : ['pushed' => count($items)];
    }

    /**
     * One buffer per "eph:<queue>/<partition>" address, so an ephemeral queue
     * and a durable queue of the same name never share a buffer or a drain
     * (§4.1).
     */
    private function pushBuffered(string $queue, ?string $partition, array $items, mixed $buffered): array
    {
        if ($this->bufferManager === null) {
            throw new \InvalidArgumentException(
                'ephemeral: buffered push needs the client\'s buffer manager — '
                . 'use $queen->ephemeral(), not a hand-built Ephemeral'
            );
        }

        $address = Destination::ephemeralAddress($queue, $partition);
        $destination = Destination::ephemeral($queue, $partition);
        $options = $this->bufferOptionsFrom($buffered);

        // Counted one at a time, exactly as the durable PushBuilder does it:
        // addMessage can raise because the bound refused the message, or
        // because the inline flush it triggered could not land inside
        // maxWaitMillis. Either way what follows is UNCONFIRMED, and reporting
        // the whole batch as buffered would be the false success the bounded
        // buffer exists to remove.
        $accepted = 0;
        foreach ($items as $item) {
            $this->bufferManager->addMessage($address, $item, $options, $destination);
            $accepted++;
        }

        return ['buffered' => true, 'count' => $accepted];
    }

    /** Send everything buffered for one ephemeral queue/partition, now. */
    public function flush(string $queue, ?string $partition = null): void
    {
        $this->requireQueue($queue);

        if ($this->bufferManager === null) {
            return;
        }

        $this->bufferManager->flushBuffer(Destination::ephemeralAddress($queue, $partition));
    }

    // ===========================
    // Pop
    // ===========================

    /**
     * Take up to `batch` messages. Answers ['queue' => …, 'messages' => [...]],
     * with `messages` an EMPTY ARRAY when there was nothing — never null, so the
     * foreach is always safe:
     *
     *     foreach ($queen->ephemeral()->pop('inbox', ['wait' => true])['messages'] as $m) { … }
     *
     * Each message is {id, partition, payload, attempts}. The `id` is opaque: it
     * encodes the owning broker incarnation, which is what lets an ack that
     * arrives after a restart or an ownership move answer `stale` instead of
     * acking somebody else's message.
     *
     * @param array $opts partition, batch, wait, timeout (alias timeoutMillis;
     *   milliseconds, default 30000 when waiting), group, autoAck.
     *
     * `wait => true` is a real long poll, parked on a RAM gate with no database
     * behind it and no polling interval anywhere (§3.4) — the structural reason
     * an ephemeral inbox answers in transport time. The HTTP deadline is set
     * past the broker's timeout so the broker's own timeout always fires first.
     *
     * `group` is the whole of the consumption semantics (§1.5): same group =
     * competing consumers, own group = fan-out, no group = queue mode.
     * `autoAck => true` commits at delivery and is at-most-once.
     */
    public function pop(string $queue, array $opts = []): array
    {
        $this->requireQueue($queue);

        $partition = $opts['partition'] ?? null;
        $group = $opts['group'] ?? null;
        $wait = ($opts['wait'] ?? false) === true;
        $timeoutMillis = $this->resolveTimeout($opts);

        $params = ['queue' => $queue];
        if ($partition !== null) {
            $params['partition'] = $partition;
        }
        if (isset($opts['batch'])) {
            $params['batch'] = (string) $opts['batch'];
        }
        // Sent only when true, so a plain pop is the shortest query this route
        // can receive and the broker's own defaults own everything else.
        if ($wait) {
            $params['wait'] = 'true';
            $params['timeout'] = (string) $timeoutMillis;
        }
        if ($group !== null) {
            $params['group'] = $group;
        }
        if (($opts['autoAck'] ?? false) === true) {
            $params['autoAck'] = 'true';
        }

        // Affinity so repeated pops of one queue land on one backend when the
        // client holds several URLs: the broker forwards to the rendezvous
        // owner either way, so this saves a hop, it does not create correctness.
        $affinityKey = sprintf('%s:%s:%s', $queue, $partition ?? '*', $group ?? '__QUEUE_MODE__');

        $result = $this->call(
            'GET',
            '/api/v1/ephemeral/pop?' . http_build_query($params),
            null,
            $wait ? $timeoutMillis + self::WAIT_TIMEOUT_SLACK_MILLIS : null,
            $affinityKey,
            // A long poll that meets a 429 should back off and keep waiting
            // rather than give up after a handful of tries.
            $wait ? Retry429Policy::KIND_POP : null,
            $queue
        );

        $body = is_array($result) ? $result : [];
        $messages = isset($body['messages']) && is_array($body['messages'])
            ? array_values(array_filter($body['messages'], fn($m) => $m !== null))
            : [];

        return ['queue' => $body['queue'] ?? $queue, 'messages' => $messages];
    }

    // ===========================
    // Ack
    // ===========================

    /**
     * Acknowledge popped messages. Answers {results:[{id, outcome}]} with
     * `outcome` in {acked, redelivered, stale, unknown}.
     *
     *     $eph->ack('inbox', $messages, ['group' => 'workers']);
     *     $eph->ack('inbox', [['id' => $id, 'status' => 'retry']]);
     *
     * `stale` is NOT an error and never arrives as one: it is the answer to an
     * ack whose message belonged to a previous incarnation of the ring, which is
     * how this class fences a restart or an ownership move without a lease
     * protocol. Pass the same `group` the pop used — cursors are per group.
     *
     * @param mixed $acks A popped message, a bare id string, the wire array
     *   itself, or a list of any of those.
     * @param array $opts group, status ('completed' | 'failed' | 'retry', or the
     *   boolean sugar), error. A per-entry status wins over the call-wide one,
     *   which is how a mixed batch travels in a single request.
     */
    public function ack(string $queue, mixed $acks, array $opts = []): mixed
    {
        $this->requireQueue($queue);

        $entries = $this->toAcks($acks, $opts);
        if (empty($entries)) {
            return ['results' => []];
        }

        $body = ['queue' => $queue];
        if (isset($opts['group']) && $opts['group'] !== null) {
            $body['group'] = $opts['group'];
        }
        $body['acks'] = $entries;

        return $this->call('POST', '/api/v1/ephemeral/ack', $body, queue: $queue);
    }

    // ===========================
    // Status
    // ===========================

    /**
     * Every ephemeral queue this tenant currently has, declared and implicit.
     *
     * Free to poll: the gauges are read out of the broker's own memory, with no
     * database behind them — unlike the durable meter, whose 1s poll is
     * load-bearing on PG.
     */
    public function queues(): mixed
    {
        return $this->call('GET', '/api/v1/ephemeral/queues');
    }

    /**
     * Depth gauges for one queue: ring length, bytes, and the per-group cursors.
     *
     * THE ONLY VERB THAT CAN TELL YOU A QUEUE IS MISSING. Everything else either
     * creates the queue (push, pop) or answers a normal body about having done
     * nothing (`reset` -> dropped:0, `delete` -> deleted:false). Here an unknown
     * queue raises EphemeralQueueNotFoundException — a different fact from
     * EphemeralUnsupportedException, which is about the broker's version, and
     * worth distinguishing precisely because both are 404s.
     */
    public function depth(string $queue): mixed
    {
        $this->requireQueue($queue);

        return $this->call(
            'GET',
            '/api/v1/ephemeral/queues/' . rawurlencode($queue) . '/depth',
            queue: $queue
        );
    }

    // ===========================
    // Internals
    // ===========================

    /**
     * Every request in this class goes through here, so the two 404 rules have
     * one home. `$queue` is passed only so a missing-queue error can name it.
     */
    private function call(
        string $method,
        string $path,
        ?array $body = null,
        ?int $timeoutMillis = null,
        ?string $affinityKey = null,
        ?string $retryKind = null,
        ?string $queue = null
    ): mixed {
        try {
            return match ($method) {
                'GET' => $this->httpClient->get($path, $timeoutMillis, $affinityKey, $retryKind),
                'DELETE' => $this->httpClient->delete($path, $timeoutMillis, $affinityKey, $retryKind),
                default => $this->httpClient->post($path, $body, $timeoutMillis, $affinityKey, $retryKind),
            };
        } catch (HttpException $error) {
            throw $this->map404($error, $queue);
        }
    }

    /**
     * Two facts arrive on this family as 404, and telling them apart is the
     * whole job of this method. THE BODY'S CODE decides, not the status:
     *
     *   - `ephemeral_queue_not_found` — the routes are there and answered; the
     *     QUEUE is not. Only `depth` can say this (§3.1): push and pop create
     *     implicitly, `reset` answers dropped:0, `delete` answers deleted:false.
     *     It is checked for on every verb anyway, because which verbs can say it
     *     is the broker's business and this client should not re-encode that
     *     list.
     *   - anything else — an old broker that never registered the routes, or an
     *     old proxy answering `route_blocked` because it fails closed on unknown
     *     API paths (§4, §8). Both mean "upgrade".
     *
     * The original refusal is kept as the previous exception either way, so
     * nothing the HTTP layer surfaced is lost by the mapping.
     */
    private function map404(HttpException $error, ?string $queue): HttpException
    {
        if ($error->statusCode !== 404
            || $error instanceof EphemeralUnsupportedException
            || $error instanceof EphemeralQueueNotFoundException) {
            return $error;
        }

        return $error->errorCode === ErrorCode::EPHEMERAL_QUEUE_NOT_FOUND
            ? EphemeralQueueNotFoundException::from($error, $queue)
            : EphemeralUnsupportedException::from($error);
    }

    private function requireQueue(string $queue): void
    {
        if ($queue === '') {
            throw new \InvalidArgumentException('ephemeral: queue must be a non-empty string');
        }
    }

    /** Options travel in a fixed order, and only when the caller gave them. */
    private function buildConfigureOptions(array $options): array
    {
        $unknown = array_diff(array_keys($options), self::CONFIGURE_OPTIONS);
        if (!empty($unknown)) {
            throw new \InvalidArgumentException(sprintf(
                'ephemeral: unknown configure option(s) %s — an option this client does not know '
                . 'would be silently dropped. Known options: %s',
                implode(', ', $unknown),
                implode(', ', self::CONFIGURE_OPTIONS)
            ));
        }

        $out = [];
        foreach (self::CONFIGURE_OPTIONS as $key) {
            if (array_key_exists($key, $options) && $options[$key] !== null) {
                $out[$key] = $options[$key];
            }
        }

        return $out;
    }

    /**
     * One message on the ephemeral wire is {payload} and nothing else — no
     * transactionId, because there is no dedup index to hold one, and no queue
     * or partition, because the envelope already carries them.
     */
    private function toMessages(mixed $messages): array
    {
        if ($messages === null) {
            throw new \InvalidArgumentException(
                'ephemeral: a message may not be null — write ["payload" => null] to push a null payload'
            );
        }

        // The durable push's own list test: a bare associative array is ONE
        // message, a list is many.
        $items = (is_array($messages) && (isset($messages[0]) || $messages === []))
            ? $messages
            : [$messages];

        return array_map(function ($item) {
            if ($item === null) {
                throw new \InvalidArgumentException(
                    'ephemeral: a message may not be null — write ["payload" => null] to push a null payload'
                );
            }
            if (is_array($item)) {
                if (array_key_exists('payload', $item)) {
                    return ['payload' => $item['payload']];
                }
                if (array_key_exists('data', $item)) {
                    return ['payload' => $item['data']];
                }
            }

            return ['payload' => $item];
        }, array_values($items));
    }

    /** true/false are sugar for the two statuses people actually mean. */
    private function normalizeStatus(mixed $status): mixed
    {
        if (is_bool($status)) {
            return $status ? 'completed' : 'failed';
        }

        return $status;
    }

    private function toAcks(mixed $acks, array $opts): array
    {
        if ($acks === null) {
            return [];
        }

        $items = (is_array($acks) && (isset($acks[0]) || $acks === [])) ? $acks : [$acks];
        $callWideStatus = $opts['status'] ?? null;
        $callWideError = $opts['error'] ?? null;

        $out = [];
        foreach (array_values($items) as $index => $entry) {
            $id = is_string($entry) ? $entry : (is_array($entry) ? ($entry['id'] ?? null) : null);
            if (!is_string($id) || $id === '') {
                throw new \InvalidArgumentException(sprintf(
                    'ephemeral: ack at index %d carries no message id — pass the popped message, or its `id`',
                    $index
                ));
            }

            $ack = ['id' => $id];

            $status = (is_array($entry) && isset($entry['status'])) ? $entry['status'] : $callWideStatus;
            if ($status !== null) {
                $ack['status'] = $this->normalizeStatus($status);
            }

            $error = (is_array($entry) && isset($entry['error'])) ? $entry['error'] : $callWideError;
            if ($error !== null) {
                $ack['error'] = $error;
            }

            $out[] = $ack;
        }

        return $out;
    }

    /**
     * `timeout` is the wire's name and milliseconds is the SDK's unit, so both
     * spellings are accepted — and BOTH AT ONCE is refused rather than silently
     * resolved, the same rule the KV expiry sugar follows.
     */
    private function resolveTimeout(array $opts): int
    {
        $hasTimeout = isset($opts['timeout']);
        $hasMillis = isset($opts['timeoutMillis']);

        if ($hasTimeout && $hasMillis) {
            throw new \InvalidArgumentException(
                'ephemeral: pass either `timeout` or `timeoutMillis`, not both — they are the same milliseconds'
            );
        }
        if ($hasTimeout) {
            return (int) $opts['timeout'];
        }
        if ($hasMillis) {
            return (int) $opts['timeoutMillis'];
        }

        return self::DEFAULT_WAIT_TIMEOUT_MILLIS;
    }

    /**
     * Buffer options are the durable buffer()'s, unchanged (§4.1): the two
     * families share the machinery, so they share its vocabulary —
     * messageCount / timeMillis / maxSize / retryDelayMillis / maxWaitMillis.
     * `intervalMillis` is accepted as a spelling of `timeMillis` because it is
     * the name the ephemeral plan's API sketch used; it is TRANSLATED rather
     * than passed through, since normalizeOptions carries unknown keys untouched
     * and would silently ignore it — a linger option that quietly does nothing
     * is a producer that batches on count alone and stalls below the threshold.
     * Both spellings at once is refused.
     */
    private function bufferOptionsFrom(mixed $buffered): array
    {
        if ($buffered === true) {
            return [];
        }
        if (!is_array($buffered)) {
            throw new \InvalidArgumentException(
                'ephemeral: `buffered` must be true or an options array'
            );
        }

        $options = $buffered;
        if (isset($options['intervalMillis'])) {
            if (isset($options['timeMillis'])) {
                throw new \InvalidArgumentException(
                    'ephemeral: pass either `timeMillis` or `intervalMillis`, not both — they are the same linger'
                );
            }
            $options['timeMillis'] = $options['intervalMillis'];
            unset($options['intervalMillis']);
        }

        return $options;
    }
}
