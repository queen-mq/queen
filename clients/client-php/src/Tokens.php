<?php

namespace Queen;

use Queen\Http\HttpClient;

/**
 * Client tokens (POST /api/v1/tokens), reached through `$queen->tokens()`: a
 * short-lived credential for ONE entity, a device or a browser user, minted
 * from a scoped key.
 *
 * The key is the role: its grants say which queues, partitions, KV keys and
 * timers it reaches, and may name the entity as `{sub}` (`telemetry` /
 * `{sub}`: each device its own partition). The token adds the entity, its
 * `sub`, and never reaches more than its key. It is minted on the
 * application's backend, which holds the key, and handed to the device:
 *
 *     // backend, with the scoped key `devices`
 *     $queen = new Queen(['url' => $url, 'bearerToken' => getenv('QUEEN_DEVICES_KEY')]);
 *     $minted = $queen->tokens()->mint($deviceId, ['ttlSeconds' => 3600]);
 *
 *     // device: a callable, so the client fetches a new token before this
 *     // one expires and after a 401
 *     $queen = new Queen(['url' => $url, 'bearerToken' => fn () => fetchTokenFromMyBackend()]);
 *
 * Revoking or narrowing the key reaches every token it minted within about a
 * second; blocking one `sub` refuses that entity's tokens on every node.
 */
class Tokens
{
    private HttpClient $httpClient;

    public function __construct(HttpClient $httpClient)
    {
        $this->httpClient = $httpClient;
    }

    /**
     * Mint a token for $sub, with this client's credential: a scoped key
     * mints for itself, an `admin` key for the scoped key `key` names.
     *
     * @param string $sub the entity: 1 to 128 letters, digits and `. _ - @ +`
     * @param array $opts ttlSeconds (default 3600, at most the key's ceiling);
     *   key (the scoped key to mint for, by id or name: only with an `admin`
     *   key); allow (grants that narrow the key's for this token, never widen
     *   them: each names exactly one of queue, kv and timers, a pattern, with
     *   optional partition, group and key patterns and its ops, produce and
     *   consume for a queue, read and write for kv and timers).
     * @throws \InvalidArgumentException before anything is sent, for an empty
     *   sub or an option this call does not take
     */
    public function mint(string $sub, array $opts = []): MintedToken
    {
        if ($sub === '') {
            throw new \InvalidArgumentException('tokens.mint: sub is required (the entity the token is for)');
        }
        // A misspelled option is a loud failure, never a silent drop: `ttl`
        // dropped would mint a token for the key's default lifetime.
        foreach (array_keys($opts) as $name) {
            if (!in_array($name, ['ttlSeconds', 'key', 'allow'], true)) {
                throw new \InvalidArgumentException(
                    "tokens.mint: unknown option `{$name}`; it accepts ttlSeconds, key and allow",
                );
            }
        }

        $body = ['sub' => $sub];
        if (isset($opts['ttlSeconds'])) {
            if (!is_int($opts['ttlSeconds']) || $opts['ttlSeconds'] < 1) {
                throw new \InvalidArgumentException('tokens.mint: ttlSeconds is a whole number of seconds above zero');
            }
            $body['ttlSeconds'] = $opts['ttlSeconds'];
        }
        if (isset($opts['key'])) {
            if (!is_string($opts['key']) || $opts['key'] === '') {
                throw new \InvalidArgumentException('tokens.mint: key names a scoped key, by its id or its name');
            }
            $body['key'] = $opts['key'];
        }
        if (isset($opts['allow'])) {
            $allow = $opts['allow'];
            if (!is_array($allow) || !array_is_list($allow)
                || array_filter($allow, static fn (mixed $grant): bool => !is_array($grant)) !== []) {
                throw new \InvalidArgumentException('tokens.mint: allow is a list of grants, each an array');
            }
            $body['allow'] = $allow;
        }

        return MintedToken::fromAnswer($this->httpClient->post('/api/v1/tokens', $body));
    }
}
