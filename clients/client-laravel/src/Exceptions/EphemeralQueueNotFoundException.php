<?php

namespace Queen\Exceptions;

/**
 * `depth` named an ephemeral queue that is not there
 * (EPHEMERAL_QUEUES.md §3.1).
 *
 * The ONLY verb of the family that can say this, and that is worth knowing
 * rather than discovering: push and pop create a queue by naming it, `reset`
 * answers dropped:0 and `delete` answers deleted:false. So this is a real DATA
 * fact — a queue name typo, or a ring that was empty and idle long enough to be
 * collected — and not the DEPLOYMENT fact EphemeralUnsupportedException states.
 * Collapsing the two would send somebody chasing a broker version over a queue
 * name.
 *
 * The two share a status and nothing else, which is why the mapping in
 * Ephemeral::call() reads the body's `code`. This exception sits BESIDE
 * EphemeralUnsupportedException rather than under it, precisely so that
 * `catch (EphemeralUnsupportedException)` never catches a missing queue; both
 * extend HttpException, so a 404 stays an HTTP refusal and every existing
 * `catch (HttpException)` around a depth call keeps catching it.
 *
 * `$errorCode` is ErrorCode::EPHEMERAL_QUEUE_NOT_FOUND — the broker's own code
 * string, unchanged — `$queue` names the queue that was not found, and
 * `getPrevious()` is the original refusal, kept because that 404 is the evidence
 * for the claim this exception makes.
 */
class EphemeralQueueNotFoundException extends HttpException
{
    public function __construct(
        string $message,
        int $statusCode,
        int $code = 0,
        ?\Throwable $previous = null,
        ?string $errorCode = null,
        ?float $retryAfterSeconds = null,
        ?string $reason = null,
        ?string $detail = null,
        public readonly ?string $queue = null,
    ) {
        parent::__construct(
            $message,
            $statusCode,
            $code,
            $previous,
            $errorCode,
            $retryAfterSeconds,
            $reason,
            $detail,
        );
    }

    public static function from(HttpException $previous, ?string $queue = null): self
    {
        return new self(
            $queue !== null && $queue !== ''
                ? sprintf('ephemeral: queue "%s" does not exist', $queue)
                : 'ephemeral: that queue does not exist',
            $previous->statusCode,
            0,
            $previous,
            ErrorCode::EPHEMERAL_QUEUE_NOT_FOUND,
            null,
            $previous->reason,
            $previous->detail,
            $queue,
        );
    }
}
