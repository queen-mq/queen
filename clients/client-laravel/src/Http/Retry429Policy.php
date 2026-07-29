<?php

namespace Queen\Http;

/**
 * Backoff policy applied when a request is rate limited (HTTP 429), separate
 * from the 5xx/network retryAttempts of HttpClient. Pure value object: it
 * decides *whether* and *how long* to wait, never sleeps.
 *
 * Proxy error contract: a 429 carries {error, code: 'rate_limited' |
 * 'quota_exceeded'} plus an optional Retry-After header in seconds, which
 * wins over the computed exponential backoff. See PLAN_QUEEN_PROXY_CLOUD.md
 * §4/§9 (client 429 backoff, blocker B4).
 */
class Retry429Policy
{
    /**
     * Long-poll pop request kind. Its outer loop is already indefinite, so a
     * 429 there backs off and keeps waiting instead of giving up.
     */
    public const KIND_POP = 'pop';

    /** $maxAttempts sentinel: retry forever, paced by the backoff. */
    public const UNBOUNDED = 0;

    /** Attempt budget for ordinary requests (push, admin calls, non-waiting pop). */
    public const DEFAULT_MAX_ATTEMPTS = 10;

    public const DEFAULT_BASE_MILLIS = 500;
    public const DEFAULT_CAP_MILLIS = 30000;

    /** Fraction of the delay spread either way, to break up a synchronized herd. */
    private const JITTER = 0.2;

    /** Exponent ceiling; capMillis clamps the result long before this bites. */
    private const MAX_EXPONENT = 32;

    public function __construct(
        public readonly int $maxAttempts = self::DEFAULT_MAX_ATTEMPTS,
        public readonly int $baseMillis = self::DEFAULT_BASE_MILLIS,
        public readonly int $capMillis = self::DEFAULT_CAP_MILLIS,
    ) {
    }

    /**
     * Resolve the effective policy for a request kind out of the client's
     * ['maxAttempts' => int, 'baseMs' => int, 'capMs' => int] config, whose
     * keys are all optional. An explicit maxAttempts applies to both kinds;
     * absent one, pop is unbounded and everything else bounded at 10.
     */
    public static function forKind(array $config, ?string $retryKind = null): self
    {
        $maxAttempts = isset($config['maxAttempts']) && $config['maxAttempts'] > 0
            ? (int) $config['maxAttempts']
            : ($retryKind === self::KIND_POP ? self::UNBOUNDED : self::DEFAULT_MAX_ATTEMPTS);

        $baseMillis = isset($config['baseMs']) && $config['baseMs'] > 0
            ? (int) $config['baseMs']
            : self::DEFAULT_BASE_MILLIS;

        $capMillis = isset($config['capMs']) && $config['capMs'] > 0
            ? (int) $config['capMs']
            : self::DEFAULT_CAP_MILLIS;

        return new self($maxAttempts, $baseMillis, $capMillis);
    }

    /**
     * Whether $tries attempts (including the first) exhaust the budget.
     */
    public function isExhausted(int $tries): bool
    {
        return $this->maxAttempts !== self::UNBOUNDED && $tries >= $this->maxAttempts;
    }

    /**
     * Delay in milliseconds before retry number $attemptIndex (0-based).
     * A non-negative Retry-After (seconds) from the server wins over the
     * exponential backoff (baseMillis * 2^attemptIndex, capped at
     * capMillis); both are jittered by +-20%.
     */
    public function delayMillis(int $attemptIndex, ?float $retryAfterSeconds = null): int
    {
        if ($retryAfterSeconds !== null && $retryAfterSeconds >= 0) {
            $delay = $retryAfterSeconds * 1000;
        } else {
            $exponent = min(max($attemptIndex, 0), self::MAX_EXPONENT);
            $delay = min($this->capMillis, $this->baseMillis * (2 ** $exponent));
        }

        $jitter = 1 + (mt_rand() / mt_getrandmax()) * 2 * self::JITTER - self::JITTER;

        return max(0, (int) round($delay * $jitter));
    }
}
