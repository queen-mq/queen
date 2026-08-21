<?php

namespace Queen\Exceptions;

/**
 * The broker or proxy in front of it is older than 1.1 and has no ephemeral
 * routes at all (EPHEMERAL_QUEUES.md §4, §8).
 *
 * No SDK in this product negotiates capabilities, so there is nothing to probe
 * and nothing to fall back to: the WHOLE family answers 404 — the broker
 * because the routes were never registered, the proxy because an unknown API
 * path is `route_blocked` and it fails closed. Both are one verdict, "upgrade",
 * and neither is "your queue is missing": the ephemeral verbs answer an absent
 * queue with an ordinary body, never a 404.
 *
 * It extends HttpException rather than RuntimeException on purpose. A 404 IS an
 * HTTP refusal, and every existing `catch (HttpException)` around a push or a
 * pop keeps catching this one; what the distinct type buys is the ability to
 * branch on the verdict without string-matching the message, which is forbidden
 * throughout this product. `$errorCode` is ErrorCode::EPHEMERAL_UNSUPPORTED and
 * `getPrevious()` is the original refusal, kept because "the proxy answered
 * route_blocked" is the evidence for the claim this exception makes.
 */
class EphemeralUnsupportedException extends HttpException
{
    /** Verbatim across every SDK: operators grep this string. */
    public const MESSAGE = 'broker/proxy does not support ephemeral queues (requires >= 1.1)';

    public static function from(HttpException $previous): self
    {
        return new self(
            self::MESSAGE,
            $previous->statusCode,
            0,
            $previous,
            ErrorCode::EPHEMERAL_UNSUPPORTED,
            null,
            $previous->reason,
            $previous->detail,
        );
    }
}
