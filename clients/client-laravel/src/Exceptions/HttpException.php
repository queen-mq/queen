<?php

namespace Queen\Exceptions;

class HttpException extends \RuntimeException
{
    /**
     * @param string|null $errorCode         Proxy error contract code (see ErrorCode), null when the
     *                                       response body carries none. Named apart from the inherited
     *                                       integer $code, which \Exception owns.
     * @param float|null  $retryAfterSeconds Parsed Retry-After response header, only ever set on a 429
     *                                       and null when the header is absent or non-numeric.
     */
    public function __construct(
        string $message,
        public readonly int $statusCode,
        int $code = 0,
        ?\Throwable $previous = null,
        public readonly ?string $errorCode = null,
        public readonly ?float $retryAfterSeconds = null,
    ) {
        parent::__construct($message, $code, $previous);
    }

    /**
     * Whether this is the terminal cluster_suspended 403: consumer loops must
     * stop entirely rather than back off and retry, since nothing short of
     * operator intervention resolves it. The other 403 codes are equally
     * non-retryable but are read off $errorCode directly.
     */
    public function isClusterSuspended(): bool
    {
        return $this->statusCode === 403 && $this->errorCode === ErrorCode::CLUSTER_SUSPENDED;
    }

    /**
     * Whether the request was rate limited (HTTP 429). HttpClient retries
     * these transparently, so one reaching a caller means the Retry429Policy
     * budget ran out.
     */
    public function isRateLimited(): bool
    {
        return $this->statusCode === 429;
    }
}
