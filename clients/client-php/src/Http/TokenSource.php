<?php

namespace Queen\Http;

use Queen\Exceptions\TokenSourceException;

/**
 * The credential a client presents (`bearerToken`): a fixed string, an API
 * key or a token it was handed, or a callable that fetches one, typically a
 * client token the application's own backend minted for this device or user
 * (`$queen->tokens()->mint()` there, POST /api/v1/tokens).
 *
 * With a callable, the client calls it before its first request and keeps the
 * token it returns. It calls it again:
 *
 * - shortly before a client token expires (its `exp`, read from the token
 *   itself, REFRESH_AHEAD_SECONDS ahead), so a request rarely meets an expired
 *   token at all;
 * - after an answer of 401, and then sends that request once more with the
 *   new token. A second 401 is the caller's.
 *
 * One fetch at a time needs no lock: a PHP process sends its requests one
 * after another, and every request of one Queen client (Guzzle, the cURL
 * transport, async and detached requests) asks this one object. A 401 met by
 * several async requests at once refreshes once: the first fetches, the
 * others find the token they were refused with already replaced.
 *
 * A forked worker (prefork, or a job that forks) carries a copy: it sends the
 * token the parent held, and its own copy of the callable fetches the next
 * one. Nothing is shared between processes.
 */
final class TokenSource
{
    /** How long before a client token's `exp` it is fetched again. */
    public const REFRESH_AHEAD_SECONDS = 30;

    /** What every client token starts with (the proxy's `clients::TOKEN_PREFIX`). */
    public const CLIENT_TOKEN_PREFIX = 'qt_';

    private ?string $fixed = null;

    private ?\Closure $fetch = null;

    /** The token the callable returned last. */
    private ?string $token = null;

    /** That token's `exp`, in unix seconds: null when it carries none. */
    private ?int $tokenExpiresAt = null;

    /**
     * @param string|(callable(): string)|null $bearerToken a string is always
     *        the token itself, even one that names a PHP function
     */
    public function __construct(string|callable|null $bearerToken = null)
    {
        if (is_string($bearerToken)) {
            $this->fixed = $bearerToken;
        } elseif ($bearerToken !== null) {
            $this->fetch = \Closure::fromCallable($bearerToken);
        }
    }

    /** Is there any credential at all? */
    public function configured(): bool
    {
        return $this->fixed !== null || $this->fetch !== null;
    }

    /** Can a new token be fetched (a callable was given)? */
    public function refreshable(): bool
    {
        return $this->fetch !== null;
    }

    /**
     * The token to send, null without a credential. $failed is the token a
     * request was just refused with (401): a new one is fetched, unless
     * another request already replaced it.
     *
     * @throws TokenSourceException when the callable throws or returns no token
     */
    public function get(?string $failed = null): ?string
    {
        if ($this->fetch === null) {
            return $this->fixed;
        }
        if ($this->token !== null) {
            $replaced = $failed !== null && $failed !== $this->token;
            if ($replaced || ($failed === null && !$this->expiring())) {
                return $this->token;
            }
        }

        try {
            $token = ($this->fetch)();
        } catch (\Throwable $error) {
            throw new TokenSourceException("token source: the token callable failed: {$error->getMessage()}", 0, $error);
        }
        $token = is_string($token) ? trim($token) : '';
        if ($token === '') {
            throw new TokenSourceException('token source: the token callable must return a non-empty string');
        }
        // What the client asks of a fixed bearerToken: it goes in a header.
        if (preg_match('/[\x00-\x20\x7F]/', $token) === 1) {
            throw new TokenSourceException('token source: the token callable returned a token with a space or a control character');
        }
        $this->token = $token;
        $this->tokenExpiresAt = self::expiresAt($token);

        return $token;
    }

    private function expiring(): bool
    {
        return $this->tokenExpiresAt !== null
            && $this->tokenExpiresAt - time() < self::REFRESH_AHEAD_SECONDS;
    }

    /**
     * A client token's expiry in unix seconds, read from its `exp` claim; null
     * for anything else (an API key, an opaque token). Read, never verified:
     * the broker is the one that verifies.
     */
    public static function expiresAt(string $token): ?int
    {
        if (!str_starts_with($token, self::CLIENT_TOKEN_PREFIX)) {
            return null;
        }
        $parts = explode('.', substr($token, strlen(self::CLIENT_TOKEN_PREFIX)));
        if (count($parts) !== 3) {
            return null;
        }
        // The JWT's middle segment: base64url, without padding.
        $json = base64_decode(strtr($parts[1], '-_', '+/'), true);
        $claims = is_string($json) ? json_decode($json, true) : null;
        $exp = is_array($claims) ? ($claims['exp'] ?? null) : null;

        return is_int($exp) ? $exp : null;
    }
}
