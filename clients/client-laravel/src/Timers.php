<?php

namespace Queen;

use Queen\Exceptions\ErrorCode;
use Queen\Exceptions\HttpException;
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
     * Exact number of pending timers in one literal timer-key namespace.
     *
     * This is one index range count on `(tenant, queue, timerKey)`, not a walk
     * through list() pages. `%` and `_` are ordinary prefix bytes, never
     * wildcards. The broker intentionally refuses an empty prefix so an
     * application cannot turn this into an accidental whole-queue scan.
     *
     * @throws \InvalidArgumentException for an empty/oversized/NUL prefix.
     * @throws \UnexpectedValueException when a successful response is not the
     *   exact `{count: non-negative integer}` contract or the exact legacy
     *   list-page contract used during rolling broker upgrades.
     */
    public function count(string $queue, string $prefix): int
    {
        if ($queue === '') {
            throw new \InvalidArgumentException('Timer count requires a non-empty queue.');
        }
        if ($prefix === '') {
            throw new \InvalidArgumentException('Timer count requires a non-empty prefix.');
        }
        if (str_contains($prefix, "\0")) {
            throw new \InvalidArgumentException('Timer count prefix cannot contain NUL.');
        }
        if (preg_match('//u', $prefix) !== 1) {
            throw new \InvalidArgumentException('Timer count prefix must be valid UTF-8.');
        }
        if (strlen($prefix) > 128) {
            throw new \InvalidArgumentException('Timer count prefix cannot exceed 128 UTF-8 bytes.');
        }

        $path = '/api/v1/timers/' . rawurlencode($queue) . '?' . http_build_query([
            'mode' => 'count',
            'prefix' => $prefix,
        ]);
        try {
            $result = $this->httpClient->get($path);
        } catch (HttpException $exception) {
            if (!$this->isCountUnsupported($exception)) {
                throw $exception;
            }

            return $this->countLegacyPages(
                $queue,
                $prefix,
                $this->list($queue, ['limit' => 1000]),
            );
        }

        if (is_array($result)
            && array_keys($result) === ['count']
            && is_int($result['count'])
            && $result['count'] >= 0
        ) {
            return $result['count'];
        }

        // The immediately preceding broker knew this route only as list and
        // ignored unknown query parameters. During a rolling deployment it
        // therefore returns a real first page to `mode=count`. Recognise only
        // that closed structure, reuse it, and validate every cursor below.
        if ($this->isLegacyListStructure($result)) {
            return $this->countLegacyPages($queue, $prefix, $result);
        }

        throw new \UnexpectedValueException(
            'Queen returned a malformed timer count response; expected exactly {"count": <non-negative integer>}.'
        );
    }

    private function isCountUnsupported(HttpException $exception): bool
    {
        if (!in_array($exception->statusCode, [400, 404, 405, 501], true)) {
            return false;
        }

        $codes = [ErrorCode::NO_SUCH_ROUTE, ErrorCode::UNSUPPORTED];

        return in_array($exception->errorCode, $codes, true)
            || in_array($exception->reason, $codes, true)
            || in_array($exception->serverError, $codes, true);
    }

    private function isLegacyListStructure(mixed $page): bool
    {
        if (!is_array($page) || count($page) !== 3) {
            return false;
        }

        $keys = array_keys($page);
        sort($keys);

        return $keys === ['nextAfter', 'rows', 'truncated']
            && is_array($page['rows'])
            && array_is_list($page['rows'])
            && is_bool($page['truncated'])
            && ($page['nextAfter'] === null || is_string($page['nextAfter']));
    }

    private function countLegacyPages(string $queue, string $prefix, mixed $firstPage): int
    {
        $count = 0;
        $after = null;
        $seenCursors = [];
        $page = $firstPage;

        while (true) {
            if (!$this->isLegacyListStructure($page)) {
                throw new \UnexpectedValueException(
                    'Queen returned a malformed legacy timer list page while counting.'
                );
            }

            $previousKey = $after;
            $lastKey = null;
            foreach ($page['rows'] as $row) {
                if (!is_array($row)
                    || !array_key_exists('timerKey', $row)
                    || !is_string($row['timerKey'])
                    || $row['timerKey'] === ''
                ) {
                    throw new \UnexpectedValueException(
                        'Queen returned a malformed timer row while counting legacy pages.'
                    );
                }

                $timerKey = $row['timerKey'];
                if ($previousKey !== null && strcmp($timerKey, $previousKey) <= 0) {
                    throw new \UnexpectedValueException(
                        'Queen returned a non-monotonic or cyclic timer cursor while counting legacy pages.'
                    );
                }
                $previousKey = $timerKey;
                $lastKey = $timerKey;

                if (str_starts_with($timerKey, $prefix)) {
                    $count++;
                }
            }

            $next = $page['nextAfter'];
            if (!$page['truncated']) {
                if ($next !== null) {
                    throw new \UnexpectedValueException(
                        'Queen returned a nextAfter cursor on a non-truncated timer page.'
                    );
                }

                return $count;
            }

            if ($next === null || $next === '' || $lastKey === null || $next !== $lastKey) {
                throw new \UnexpectedValueException(
                    'Queen returned a truncated timer page without its last-row cursor.'
                );
            }
            if (isset($seenCursors[$next]) || ($after !== null && strcmp($next, $after) <= 0)) {
                throw new \UnexpectedValueException(
                    'Queen returned a non-monotonic or cyclic timer cursor while counting legacy pages.'
                );
            }

            $seenCursors[$next] = true;
            $after = $next;
            $page = $this->list($queue, ['after' => $after, 'limit' => 1000]);
        }
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
