<?php

namespace Queen\Exceptions;

/**
 * Machine-readable error codes from the proxy error contract, carried in the
 * JSON body's "code" field and surfaced as HttpException::$errorCode so
 * callers can branch without string-matching the message.
 *
 * 429 -> RATE_LIMITED | QUOTA_EXCEEDED (retryable, paced by Retry429Policy)
 * 403 -> CLUSTER_SUSPENDED | STORAGE_QUOTA_EXCEEDED | FEATURE_GATED |
 *        FORBIDDEN (terminal: nothing short of operator or plan action
 *        resolves them, so they must never be retried)
 *
 * $errorCode stays null when the body has no such field, e.g. errors coming
 * straight from a broker that predates the proxy contract.
 */
class ErrorCode
{
    public const RATE_LIMITED = 'rate_limited';
    public const QUOTA_EXCEEDED = 'quota_exceeded';
    public const CLUSTER_SUSPENDED = 'cluster_suspended';
    public const STORAGE_QUOTA_EXCEEDED = 'storage_quota_exceeded';
    public const FEATURE_GATED = 'feature_gated';
    public const FORBIDDEN = 'forbidden';

    /**
     * Not a server code: the SDK's own verdict for a 404 on the
     * /api/v1/ephemeral/* family, which means the routes are not there at all
     * (EPHEMERAL_QUEUES.md §4). Carried on EphemeralUnsupportedException so the
     * one condition no version negotiation can discover is still branchable.
     */
    public const EPHEMERAL_UNSUPPORTED = 'ephemeral_unsupported';

    /**
     * This one IS a server code, and it is the reason the 404 mapping above
     * reads the body rather than the status: a broker that fully supports the
     * family answers it when `depth` names a queue that is not there
     * (EPHEMERAL_QUEUES.md §3.1). Byte-identical across every SDK (Go
     * ErrEphemeralQueueNotFound, queen_protocol::EPHEMERAL_QUEUE_NOT_FOUND_CODE)
     * so a code seen in one language's logs means the same thing in the next.
     * Carried on EphemeralQueueNotFoundException.
     */
    public const EPHEMERAL_QUEUE_NOT_FOUND = 'ephemeral_queue_not_found';
}
