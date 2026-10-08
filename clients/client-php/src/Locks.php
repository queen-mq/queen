<?php

namespace Queen;

use Queen\Http\HttpClient;

/**
 * The four lock operations as the broker speaks them, reached through
 * `$queen->locks()`, with no state kept: the caller carries the token.
 * `$queen->lock()` is what most code wants; this is for `get` (who holds it?)
 * and for a caller with its own loop.
 *
 * WHAT A LOCK IS. A permit is one KV row in the namespace `queen-locks`,
 * written with a lifetime: acquire is a putIfAbsent, renew a put with expect,
 * release a delete with expect. The broker's POST /api/v1/locks does that
 * turning, so every client shares one implementation. A lock is the semaphore
 * of one permit.
 *
 * THE STATUS RULE IS KV'S: the HTTP status describes the outcome of the CALL,
 * never the verdict of an operation. A lock somebody else holds, a token that
 * is no longer the row's, a release of what was not held — all HTTP 200 with
 * an explicit field: `acquired`, `renewed`, `released`. An HttpException from
 * this class means the call did not happen.
 *
 * A RENEW ANSWERS A NEW TOKEN. It rewrites the row, so the token before it
 * stops working — for the guard, for the next renew, for the release. Use the
 * token of the last answer.
 */
class Locks
{
    public const NAMESPACE = 'queen-locks';

    private const NAME_MAX_BYTES = 256;
    private const OWNER_MAX_BYTES = 256;
    private const LIMIT_MAX = 1024;

    private HttpClient $httpClient;

    public function __construct(HttpClient $httpClient)
    {
        $this->httpClient = $httpClient;
    }

    /**
     * Several operations in one call, each on a different lock.
     *
     * @param array $ops operations built with the static factories below.
     * @return array the results, one per operation, in order. They are
     *   independent: nothing here is all-or-nothing.
     */
    public function batch(array $ops): array
    {
        $ops = array_values($ops);
        if ($ops === []) {
            throw new \InvalidArgumentException('a locks call needs at least one operation');
        }

        $body = $this->httpClient->post('/api/v1/locks', ['operations' => $ops]);
        $results = is_array($body) ? ($body['results'] ?? null) : null;

        if (!is_array($results) || count($results) !== count($ops)) {
            throw new \RuntimeException(sprintf(
                'locks: expected {"results":[...]} with %d element(s), got %s',
                count($ops),
                json_encode($body)
            ));
        }

        return $results;
    }

    /**
     * Take a permit.
     *
     * @param array $opts ttlSeconds (required, integer above zero), owner,
     *   limit (above 1 makes it a semaphore; every caller of one name passes
     *   the same limit, which is stored nowhere).
     * @return array {acquired, slot, token, owner, guard, already?}, or
     *   {acquired:false, reason:'held'|'contended', holders}.
     */
    public function acquire(string $name, array $opts = []): array
    {
        return $this->batch([self::acquireOp($name, $opts)])[0];
    }

    /**
     * Extend a permit.
     *
     * @param array $opts ttlSeconds (required), slot, owner.
     * @return array {renewed, slot, token, guard} with a NEW token, or
     *   {renewed:false, reason:'lost', holders}.
     */
    public function renew(string $name, int $token, array $opts = []): array
    {
        return $this->batch([self::renewOp($name, $token, $opts)])[0];
    }

    /**
     * Give a permit back.
     *
     * @return array {released}; false with reason:'lost' when the token is no
     *   longer the row's.
     */
    public function release(string $name, int $token, int $slot = 0): array
    {
        return $this->batch([self::releaseOp($name, $token, $slot)])[0];
    }

    /**
     * Who holds it.
     *
     * @return array {held, holders: [{slot, owner, token, since, expiresAt, renewedAt}]}
     */
    public function get(string $name): array
    {
        return $this->batch([self::getOp($name)])[0];
    }

    // ===========================
    // Operation shapes
    // ===========================

    public static function acquireOp(string $name, array $opts = []): array
    {
        self::onlyOptions($opts, ['ttlSeconds', 'owner', 'limit'], 'acquire');
        $op = ['op' => 'acquire', 'name' => self::name($name), 'ttlSeconds' => self::ttlSeconds($opts)];
        if (array_key_exists('owner', $opts)) {
            $op['owner'] = self::owner($opts['owner']);
        }
        if (array_key_exists('limit', $opts)) {
            $op['limit'] = self::limit($opts['limit']);
        }

        return $op;
    }

    public static function renewOp(string $name, int $token, array $opts = []): array
    {
        self::onlyOptions($opts, ['ttlSeconds', 'slot', 'owner'], 'renew');
        $op = [
            'op' => 'renew',
            'name' => self::name($name),
            'token' => self::token($token, 'renew'),
            'ttlSeconds' => self::ttlSeconds($opts),
        ];
        if (($opts['slot'] ?? 0) !== 0) {
            $op['slot'] = (int) $opts['slot'];
        }
        if (array_key_exists('owner', $opts)) {
            $op['owner'] = self::owner($opts['owner']);
        }

        return $op;
    }

    public static function releaseOp(string $name, int $token, int $slot = 0): array
    {
        $op = ['op' => 'release', 'name' => self::name($name), 'token' => self::token($token, 'release')];
        if ($slot !== 0) {
            $op['slot'] = $slot;
        }

        return $op;
    }

    public static function getOp(string $name): array
    {
        return ['op' => 'get', 'name' => self::name($name)];
    }

    /**
     * The lifetime, in whole seconds. Mandatory, and there is no `forever`: a
     * lock that never expires is one nobody can take back from a holder that
     * died.
     */
    public static function ttlSeconds(array $opts): int
    {
        $ttl = $opts['ttlSeconds'] ?? null;
        if (!is_int($ttl) || $ttl <= 0) {
            throw new \InvalidArgumentException(
                'a lock needs a lifetime: ttlSeconds, an integer above zero. '
                . 'There is no forever; a holder that needs longer renews'
            );
        }

        return $ttl;
    }

    public static function name(string $name): string
    {
        // The broker's rule, checked here so the mistake surfaces at the call:
        // no '#', which sits between a name and its slot in the row's key.
        if ($name === '' || strlen($name) > self::NAME_MAX_BYTES
            || preg_match('/[\x00-\x1f\x7f#]|\xc2[\x80-\x9f]/', $name) === 1) {
            throw new \InvalidArgumentException(sprintf(
                "a lock's name is a non-empty string of at most %d bytes, without control characters "
                . "and without '#' — got %s",
                self::NAME_MAX_BYTES,
                json_encode($name)
            ));
        }

        return $name;
    }

    public static function owner(mixed $owner): string
    {
        if (!is_string($owner) || $owner === '' || strlen($owner) > self::OWNER_MAX_BYTES
            || preg_match('/[\x00-\x1f\x7f]|\xc2[\x80-\x9f]/', $owner) === 1) {
            throw new \InvalidArgumentException(sprintf(
                'owner is a non-empty string of at most %d bytes, without control characters',
                self::OWNER_MAX_BYTES
            ));
        }

        return $owner;
    }

    public static function limit(mixed $limit): int
    {
        if (!is_int($limit) || $limit < 1 || $limit > self::LIMIT_MAX) {
            throw new \InvalidArgumentException(
                'limit is a whole number from 1 (a lock) to ' . self::LIMIT_MAX
            );
        }

        return $limit;
    }

    private static function token(int $token, string $what): int
    {
        if ($token <= 0) {
            throw new \InvalidArgumentException(
                "{$what} needs the token of the permit (the one the last acquire or renew answered)"
            );
        }

        return $token;
    }

    /**
     * A misspelled option is a loud failure, never a silent drop: `['ttl' =>
     * 30]` dropped would send an acquire with no lifetime.
     */
    private static function onlyOptions(array $opts, array $allowed, string $op): void
    {
        foreach (array_keys($opts) as $name) {
            if (!in_array($name, $allowed, true)) {
                throw new \InvalidArgumentException(sprintf(
                    'unknown lock option `%s` for %s; this operation accepts: %s',
                    $name,
                    $op,
                    implode(', ', $allowed)
                ));
            }
        }
    }
}
