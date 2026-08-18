<?php

namespace Queen;

use Queen\Http\HttpClient;
use Queen\Support\TimerOp;

/**
 * Scheduled messages, reached through `$queen->timers()`.
 *
 * A timer is a message the broker holds and pushes into a real queue when its
 * delay elapses. It is delivered to whatever consumes that queue, with the
 * message id the schedule call promised, so nothing new has to be wired to
 * receive one.
 *
 * `deliverAt` is "NOT BEFORE", never "exactly at". A healthy timer lands within
 * a sweep cycle of its delay; a cluster with a broker down recovers within the
 * sweeper's maximum sleep. Anything that needs an exact instant needs a
 * different product.
 *
 * ORDER, which surprises people: two timers on the same (queue, partition) that
 * mature in the same batch enter the log in EXPIRY order, not in the order they
 * were scheduled.
 *
 * WHY cancel HAS ITS OWN METHOD AND ITS OWN ROUTE. DELETE
 * /api/v1/timers/:queue/*timerKey is the one call a full or throttled cluster
 * is required never to refuse — a tenant that cannot cancel keeps producing
 * messages it cannot stop, because the fire never switches itself off.
 * Cancelling through the batch route would inherit the schedule route's class
 * and lose that guarantee, so this client never does it.
 *
 * AND THE CONTRACT ON `absent`, which is where users get hurt. There is no
 * tombstone: once a timer is delivered its row is gone, so a later cancel
 * answers status `absent` with ok:false. **`absent` means "no longer pending"
 * and may mean ALREADY DELIVERED.** The authority is the log — look for the
 * `txn` this call hands back in the destination queue. A cancel on another
 * tenant's timer answers `absent` too, which is why it is ok:false and not a
 * cheerful success.
 *
 * The status taxonomy is closed, so a caller that has to distinguish writes a
 * match and not a string comparison against a sentence:
 * scheduled | rescheduled | cancelled | absent | too_late.
 */
class Timers
{
    private HttpClient $httpClient;

    public function __construct(HttpClient $httpClient)
    {
        $this->httpClient = $httpClient;
    }

    /**
     * @param int $delayMs milliseconds from now; negative is legal and fires on
     *   the first cycle.
     * @param mixed $payload JSON-serializable; delivered as the message body.
     * @param array $opts txn, partition.
     * @return array {ok, status, queue, timerKey, txn, messageId, deliverAt} —
     *   or {ok:false, status:'too_late'} when the timer is already claimed and
     *   about to be delivered.
     */
    public function schedule(string $queue, string $timerKey, int $delayMs, mixed $payload, array $opts = []): array
    {
        return $this->single(TimerOp::schedule($queue, $timerKey, $delayMs, $payload, $opts));
    }

    /**
     * The same upsert as schedule, so retrying after a crash is safe. The retry
     * budget and the last error are reset — a rescheduled timer is a new timer
     * under an old name.
     */
    public function reschedule(string $queue, string $timerKey, int $delayMs, mixed $payload, array $opts = []): array
    {
        return $this->single(TimerOp::reschedule($queue, $timerKey, $delayMs, $payload, $opts));
    }

    /**
     * Cancel through the dedicated route, which is never blocked by a quota, a
     * grant or an operator's pause.
     *
     * @param string|null $txn the txn you expect this timer to carry; echoed
     *   back on `absent` so the "was it already delivered?" check needs no
     *   second API.
     * @return array {ok, status, queue, timerKey, txn?}
     *
     * A timer in retry backoff IS cancellable — during backoff nothing holds a
     * claim on it, deliberately.
     */
    public function cancel(string $queue, string $timerKey, ?string $txn = null): array
    {
        $path = '/api/v1/timers/' . rawurlencode($queue) . '/' . rawurlencode($timerKey);

        if ($txn !== null) {
            $path .= '?' . http_build_query(['txn' => $txn]);
        }

        $result = $this->httpClient->delete($path);

        return is_array($result) ? $result : [];
    }

    /**
     * One pending timer, with its payload exactly as stored — `encrypted` tells
     * the truth about it, because an inspection surface must not quietly
     * decrypt what the delivery will hand over as an envelope.
     *
     * @return array {found, ...} — {found:false} on a miss, with HTTP 200.
     */
    public function peek(string $queue, string $timerKey): array
    {
        $path = '/api/v1/timers/' . rawurlencode($queue) . '/' . rawurlencode($timerKey);
        $result = $this->httpClient->get($path);

        return is_array($result) ? $result : [];
    }

    /**
     * Pending timers of ONE queue, keyset-paginated. There is no tenant-wide
     * list: it would be a scan that an end user of yours could trigger, on the
     * first endpoint of this product whose call rate is decided by somebody
     * else's web traffic.
     *
     * @param array $opts after (exclusive cursor), limit (clamped by the
     *   server, never rejected).
     * @return array {rows, truncated, nextAfter} — no payloads; use peek().
     */
    public function list(string $queue, array $opts = []): array
    {
        $query = array_filter([
            'after' => $opts['after'] ?? null,
            'limit' => $opts['limit'] ?? null,
        ], fn($v) => $v !== null);

        $path = '/api/v1/timers/' . rawurlencode($queue);
        if ($query !== []) {
            $path .= '?' . http_build_query($query);
        }

        $result = $this->httpClient->get($path);

        return is_array($result) ? $result : [];
    }

    /**
     * Schedule and reschedule many in one call and one transaction.
     *
     * A `cancel` built with TimerOp::cancel() is accepted here too — it is the
     * same stored procedure — but it then inherits this route's class and can
     * be refused with the rest of the batch. Use cancel() for cancels.
     *
     * @return array {results: [...]} index-aligned to the input.
     */
    public function batch(array $ops): array
    {
        $result = $this->httpClient->post('/api/v1/timers', ['operations' => array_values($ops)]);

        return is_array($result) ? $result : [];
    }

    private function single(array $op): array
    {
        $body = $this->batch([$op]);

        if (!isset($body['results']) || !is_array($body['results'])) {
            return $body;
        }

        return $body['results'][0] ?? $body;
    }
}
