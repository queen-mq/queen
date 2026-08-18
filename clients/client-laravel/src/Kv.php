<?php

namespace Queen;

use Queen\Http\HttpClient;
use Queen\Support\KvOp;

/**
 * Transactional key/value state, reached through `$queen->kv()`.
 *
 * Everything goes through POST /api/v1/kv, which is the complete surface and
 * the only route that accepts getPrefix and incr. The three path routes
 * (GET/PUT/DELETE /api/v1/kv/:ns/*key) exist as sugar for the cases people
 * write by hand with curl; an SDK built on them would need a second code path
 * for the two operations they cannot express, and would put a prefix in a URL
 * the first time somebody asked for it.
 *
 * THE ONE RULE ABOUT STATUS CODES, because it is why this class throws so
 * rarely: the HTTP status describes the outcome of the CALL, never the verdict
 * of the business predicate. A key that is not there, a putIfAbsent that lost
 * its race, a delete that hit nothing — all HTTP 200 with an explicit field in
 * the body. `applied:false` is the single most frequent outcome of this
 * product, and a 4xx would put it inside every retry policy and error
 * dashboard. So: check `applied` (writes) or `found` (reads). An HttpException
 * from this class means the CALL failed — bad shape, over a ceiling, rate
 * limited, the cell is unwell — never that your predicate lost.
 *
 * THE RULE THAT DECIDES EVERYTHING ELSE. A read-modify-write across two calls
 * is safe only when the KV key derives from the partition key: then the lanes
 * serialize and the key has no other writer inside that consumer group. When it
 * does not derive, use the atomics — `incr` for counters, `expect` for
 * compare-and-swap. And `expect` is worth passing even when you believe the
 * lane serializes you: if it never fails it cost nothing, and the day it fails
 * you have just discovered that two consumers are serving the same partition,
 * with a verdict instead of a wrong total.
 *
 * ORDER OF DEFENCES, in this order: the ack transaction is the primary fence,
 * `expect` is the secondary assertion. A write that shares its transaction with
 * the ack is undone when an expired lease makes the ack fail — which
 * compare-and-swap cannot do, because an `expect` on a still-matching version
 * succeeds even from a zombie. Use TransactionBuilder::kv() for that.
 *
 * ON NUMBERS. `version` is a 64-bit integer and survives PHP's json_decode
 * intact. `incr` runs on Postgres `numeric`, so a fractional delta comes back
 * as a PHP float; an integer counter comes back as an int. A counter driven
 * past PHP_INT_MAX would decode as a float and lose precision silently, which
 * is a reason to bound counters with `max` rather than a reason to distrust
 * them.
 */
class Kv
{
    private HttpClient $httpClient;

    public function __construct(HttpClient $httpClient)
    {
        $this->httpClient = $httpClient;
    }

    // ===========================
    // Reads
    // ===========================

    /**
     * @return array {found, key, value, version, expiresAt, updatedAt} — and
     *   only {found:false, key} on a miss. `found` is separate from the value
     *   because null is a legal value: {found:true, value:null} and
     *   {found:false} are different things and must not be collapsed.
     *
     * An expired key is NEVER returned and never counts as existing, even
     * before the sweeper has pruned its row.
     */
    public function get(string $ns, string $key): array
    {
        return $this->single(KvOp::get($ns, $key));
    }

    /**
     * @return array {rows, missing, truncated}. `missing` is explicit: absence
     *   is a datum, not a hole you compute by difference. Rows are rows, never
     *   a key/value map. Keys dropped by the server's byte budget are in
     *   neither list and `truncated` says so — calling them absent would be a
     *   lie.
     */
    public function getMany(string $ns, array $keys): array
    {
        return $this->single(KvOp::getMany($ns, $keys));
    }

    /**
     * @param array $opts after (exclusive keyset cursor), limit (clamped by the
     *   server, never rejected), keysOnly.
     * @return array {rows, truncated, nextAfter}
     *
     * Every page is its own snapshot: with `after` it may miss a key inserted
     * behind the cursor. Fine for compacting state, wrong for an exact count.
     * A namespace is not a table to enumerate, so the prefix is mandatory.
     */
    public function getPrefix(string $ns, string $prefix, array $opts = []): array
    {
        return $this->single(KvOp::getPrefix($ns, $prefix, $opts));
    }

    // ===========================
    // Writes
    // ===========================

    /**
     * @param array $opts exactly one of ttlSeconds (integer > 0) and
     *   forever:true — the broker enforces it, and this client never defaults
     *   one. Plus expect and required.
     * @return array {applied, key, value, version} — with the CURRENT value and
     *   version even when it did not apply, so the loser needs no second round
     *   trip. On `applied:false` there is also a `reason` from the closed set
     *   exists | absent | version | limit | type.
     */
    public function put(string $ns, string $key, mixed $value, array $opts = []): array
    {
        return $this->single(KvOp::put($ns, $key, $value, $opts));
    }

    public function putIfAbsent(string $ns, string $key, mixed $value, array $opts = []): array
    {
        return $this->single(KvOp::putIfAbsent($ns, $key, $value, $opts));
    }

    public function delete(string $ns, string $key, array $opts = []): array
    {
        return $this->single(KvOp::delete($ns, $key, $opts));
    }

    /**
     * With `max`, `applied` IS the admission decision — the ceiling does not
     * saturate, so a call that would overshoot does not apply at all and hands
     * back the current value.
     */
    public function incr(string $ns, string $key, int|float $delta, array $opts = []): array
    {
        return $this->single(KvOp::incr($ns, $key, $delta, $opts));
    }

    // ===========================
    // Batch
    // ===========================

    /**
     * Apply operations built with KvOp, in one call and one transaction.
     *
     * @return array the response envelope: {results: [...]} index-aligned to
     *   the input, or — when an operation carried required:true and lost its
     *   precondition — the verdict {ok:false, reason:'kv_precondition',
     *   failedIndex, kvReason, version, value}, which arrives as HTTP 200
     *   because a lost precondition is the expected outcome of a legitimate
     *   redelivery and must not enter a retry policy.
     */
    public function batch(array $ops): array
    {
        $result = $this->httpClient->post('/api/v1/kv', ['operations' => array_values($ops)]);

        return is_array($result) ? $result : [];
    }

    /**
     * A single-operation call answers with the ELEMENT, not a one-element
     * array. The precondition verdict has no `results`, so it is handed back
     * whole: a caller that passed required:true has to be able to tell the two
     * shapes apart, and inventing an element for it would hide the reason.
     */
    private function single(array $op): array
    {
        $body = $this->batch([$op]);

        if (!isset($body['results']) || !is_array($body['results'])) {
            return $body;
        }

        return $body['results'][0] ?? $body;
    }
}
