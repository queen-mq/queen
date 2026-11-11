<?php

namespace Queen;

/**
 * A client token as POST /api/v1/tokens answered it: what
 * `$queen->tokens()->mint()` returns. Hand `token` to the entity it was
 * minted for; `expiresAt` and `expiresIn` say when it stops working.
 */
final class MintedToken
{
    /**
     * @param string $token the bearer credential: `qt_` and a JWT
     * @param string $sub the entity the token is for
     * @param string $key the id of the scoped key the token belongs to
     * @param string $expiresAt when the token stops working, RFC 3339
     * @param int $expiresIn its lifetime in seconds, from when it was minted
     */
    public function __construct(
        public readonly string $token,
        public readonly string $sub,
        public readonly string $key,
        public readonly string $expiresAt,
        public readonly int $expiresIn,
    ) {
    }

    /**
     * @throws \UnexpectedValueException for an answer that is not a minted token
     */
    public static function fromAnswer(mixed $answer): self
    {
        if (!is_array($answer)
            || !is_string($answer['token'] ?? null) || $answer['token'] === ''
            || !is_string($answer['sub'] ?? null)
            || !is_string($answer['key'] ?? null)
            || !is_string($answer['expiresAt'] ?? null)
            || !is_int($answer['expiresIn'] ?? null)) {
            // The answer is not quoted: it may hold the token.
            throw new \UnexpectedValueException(
                'tokens.mint: POST /api/v1/tokens answered without the token, sub, key, expiresAt and expiresIn of a minted token',
            );
        }

        return new self($answer['token'], $answer['sub'], $answer['key'], $answer['expiresAt'], $answer['expiresIn']);
    }
}
