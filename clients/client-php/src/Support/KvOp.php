<?php

namespace Queen\Support;

/**
 * Factories for the seven KV operations, and the ONE place their wire shape is
 * written down.
 *
 * Both surfaces share these: the standalone client (Queen\Kv, POST /api/v1/kv)
 * and the KV rider of a transaction (TransactionBuilder::kv). Two copies of the
 * shape would be two things that can disagree, and the one that disagrees is
 * always the one you are not testing.
 *
 * WHAT THIS CLASS DELIBERATELY DOES NOT VALIDATE. The expiry rule — exactly one
 * of `ttlSeconds` (an integer greater than zero) and `forever: true` on every
 * put, putIfAbsent and incr — lives in the broker's kv_apply_v1 and nowhere
 * else, so all seven clients and the embedded broker inherit it without a line
 * of their own. The same goes for the namespace charset and the key ceiling.
 * Re-implementing them here would add a second opinion that can drift, and the
 * drift would be discovered as "the PHP client rejects something the others
 * accept".
 *
 * What this class DOES enforce is the small set of rules the broker structurally
 * cannot: that a misspelled option is a loud failure rather than a silent drop,
 * and that no expiry is EVER defaulted. A put that silently inherited or
 * invented a TTL is the fastest way to make a marker immortal.
 */
final class KvOp
{
    /**
     * Fields that are arguments of the stored procedure, never fields of an
     * operation. The broker rejects all three; naming them here means a caller
     * finds out at their first unit test rather than in an audit.
     */
    private const NOT_AN_INPUT = [
        'tenant' => 'the tenant comes from the authenticated request, never from an operation',
        'tenantId' => 'the tenant comes from the authenticated request, never from an operation',
        '_tenant' => 'the tenant comes from the authenticated request, never from an operation',
        'ttl' => 'the field is `ttlSeconds` (an integer number of seconds)',
        'ttlMillis' => 'the field is `ttlSeconds`: durations that cannot be sub-second are in seconds',
        'expiresAt' => 'there is no absolute expiry on this wire; use ttlSeconds or forever:true',
    ];

    public static function get(string $ns, string $key): array
    {
        return ['op' => 'get', 'ns' => $ns, 'key' => $key];
    }

    /**
     * `missing` comes back as an explicit list: absence is a datum, not a hole
     * the caller computes by difference. The rows are rows, never a key/value
     * map, so the shape itself makes the confusion inexpressible.
     */
    public static function getMany(string $ns, array $keys): array
    {
        // array_values, so a filtered list still serializes as a JSON array
        // rather than an object keyed by whatever survived.
        return ['op' => 'getMany', 'ns' => $ns, 'keys' => array_values($keys)];
    }

    /**
     * Allowed ONLY in the body of POST /api/v1/kv — never in a query string and
     * never inside a transaction. A prefix in a URL is recorded by the broker's
     * access log, the proxy's, the meter sample, the tracing span and any
     * ingress in front; inside a transaction it is unbounded read work holding
     * the outermost lock space.
     *
     * `limit` is clamped by the broker and never rejected, and `truncated` in
     * the answer tells the truth. `after` is an exclusive keyset cursor, not an
     * offset: every page is its own snapshot, which is fine for compacting
     * state and wrong for an exact count.
     */
    public static function getPrefix(string $ns, string $prefix, array $opts = []): array
    {
        return self::withOpts(
            ['op' => 'getPrefix', 'ns' => $ns, 'prefix' => $prefix],
            $opts,
            ['after', 'limit', 'keysOnly']
        );
    }

    /**
     * @param mixed $value any JSON-serializable value. `null` is a legal value
     *   and is emitted as such: {found:true, value:null} and {found:false} are
     *   different things.
     * @param array $opts ttlSeconds|forever (exactly one, enforced by the
     *   broker), expect, required.
     *
     * `expect` is the optimistic lock. 0 means "must not exist" and wins even
     * against an expired row not yet pruned; N > 0 is a pure update that never
     * creates. The version handed back to a loser is ADVISORY — it is read on a
     * later snapshot than the write that beat you, so it is not a fencing token
     * to reuse blindly.
     */
    public static function put(string $ns, string $key, mixed $value, array $opts = []): array
    {
        return self::withOpts(
            ['op' => 'put', 'ns' => $ns, 'key' => $key, 'value' => $value],
            $opts,
            ['ttlSeconds', 'forever', 'expect', 'required']
        );
    }

    /**
     * An alias that desugars to put + expect:0 inside the stored procedure, so
     * it is one code path. It travels under its own name because that is the
     * name of the thing, and because `applied` — "did I win?" — is the question
     * most often asked of this API.
     *
     * Two concurrent putIfAbsent serialize: ON CONFLICT DO UPDATE takes the row
     * lock BEFORE evaluating its WHERE, so the second re-evaluates against the
     * new row and does not apply. Exactly one wins. The cost is that even a
     * FAILED conditional holds that row lock until commit.
     *
     * And the sentence that has to be said out loud: putIfAbsent plus a TTL is
     * NOT a distributed lock. A lock that expires is not revoked — the old
     * holder keeps working, it just no longer has the row. The defence is
     * fencing: carry your `version` as `expect` on every later write.
     */
    public static function putIfAbsent(string $ns, string $key, mixed $value, array $opts = []): array
    {
        return self::withOpts(
            ['op' => 'putIfAbsent', 'ns' => $ns, 'key' => $key, 'value' => $value],
            $opts,
            // No `expect`: putIfAbsent IS expect:0, and a different one is a
            // contradiction the broker refuses.
            ['ttlSeconds', 'forever', 'required']
        );
    }

    public static function delete(string $ns, string $key, array $opts = []): array
    {
        return self::withOpts(
            ['op' => 'delete', 'ns' => $ns, 'key' => $key],
            $opts,
            ['expect', 'required']
        );
    }

    /**
     * The way OUT of compare-and-swap, which is why it takes no `expect`.
     *
     * With `max`, `applied` IS the admission decision: the ceilings do not
     * saturate and do not truncate, so a call that would overshoot does not
     * apply and hands back the CURRENT value. Comparing client-side after
     * incrementing would mean the request that broke the ceiling had already
     * spent budget that cannot be given back.
     *
     * The TTL is CREATE-ONLY. A live row keeps its expiry: if incr extended it,
     * a fixed-window limiter on a permanently busy client would never close its
     * window, i.e. would stop limiting exactly under load. An expired row counts
     * as zero and starts a fresh window, which is what makes the limiter one
     * call instead of two.
     */
    public static function incr(string $ns, string $key, int|float $delta, array $opts = []): array
    {
        return self::withOpts(
            ['op' => 'incr', 'ns' => $ns, 'key' => $key, 'delta' => $delta],
            $opts,
            ['ttlSeconds', 'forever', 'min', 'max', 'required']
        );
    }

    /**
     * Append the options this operation accepts, in a declared order, and
     * refuse everything else.
     *
     * The refusal is the point. `['ttl' => 60]` silently dropped would send a
     * write with no expiry declaration, and the caller would read the broker's
     * `kv_expiry_not_specified` without ever suspecting their own spelling.
     *
     * A null option is refused for the same reason and one sharper one: an
     * explicitly null `expect` is a bug in the caller's code, never a silent
     * downgrade to an unconditional upsert. Writing the word `expect` declares
     * the intention to fence.
     */
    private static function withOpts(array $op, array $opts, array $allowed): array
    {
        foreach ($opts as $name => $value) {
            if (isset(self::NOT_AN_INPUT[$name])) {
                throw new \InvalidArgumentException(
                    "kv option `{$name}` is not part of the wire: " . self::NOT_AN_INPUT[$name]
                );
            }

            if (!in_array($name, $allowed, true)) {
                throw new \InvalidArgumentException(sprintf(
                    'unknown kv option `%s` for %s; this operation accepts: %s',
                    $name,
                    $op['op'],
                    $allowed === [] ? '(none)' : implode(', ', $allowed)
                ));
            }

            if ($value === null) {
                throw new \InvalidArgumentException(
                    "kv option `{$name}` is null; drop the option instead of passing null "
                    . '(an explicitly null `expect` is a bug in the caller, not an unconditional write)'
                );
            }
        }

        foreach ($allowed as $name) {
            if (array_key_exists($name, $opts)) {
                $op[$name] = $opts[$name];
            }
        }

        return $op;
    }
}
