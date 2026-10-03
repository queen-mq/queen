<?php

namespace App\Jobs\Compat;

use DateTimeInterface;

/**
 * Laravel's retry policy, set per job: an array backoff, maxExceptions,
 * retryUntil and failOnTimeout. Mode `throw` fails every attempt; mode `ok`
 * with a long `sleepMs` overruns `timeout`.
 */
final class CompatPolicyJob extends CompatJob
{
    /** @var int|list<int> */
    public int|array $backoff = 0;

    public ?int $maxExceptions = null;

    public int $timeout = 60;

    public bool $failOnTimeout = false;

    public ?int $retryForSeconds = null;

    public ?int $dispatchedAt = null;

    /** @param int|list<int> $backoff */
    public static function make(string $runId, string $jobId, string $mode, int $sleepMs, int $tries, int|array $backoff = 0,
        ?int $maxExceptions = null, int $timeout = 60, bool $failOnTimeout = false, ?int $retryForSeconds = null): self
    {
        $job = new self($runId, $jobId, $mode, $sleepMs, $tries);
        $job->backoff = $backoff;
        $job->maxExceptions = $maxExceptions;
        $job->timeout = $timeout;
        $job->failOnTimeout = $failOnTimeout;
        $job->retryForSeconds = $retryForSeconds;
        $job->dispatchedAt = time();

        return $job;
    }

    public function retryUntil(): ?DateTimeInterface
    {
        return $this->retryForSeconds === null
            ? null
            : now()->setTimestamp(($this->dispatchedAt ?? time()) + $this->retryForSeconds);
    }
}
