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
}
