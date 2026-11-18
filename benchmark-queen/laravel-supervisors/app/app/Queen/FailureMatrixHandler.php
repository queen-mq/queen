<?php

namespace App\Queen;

use App\Support\FailureMatrixLog;
use InvalidArgumentException;

/**
 * The handler of the matrix's pool of handlers: a `queen:consume` worker
 * hands it the plain messages bench:matrix-consume-push sends. Each delivery
 * is logged when it starts and when it ends, as FailureMatrixJob logs an
 * attempt, so a delivery killed in the middle shows as started only.
 */
final class FailureMatrixHandler
{
    public function __construct(private FailureMatrixLog $log)
    {
    }

    public function handle(array $message): void
    {
        $data = $message['data'] ?? null;
        if (!is_array($data) || !is_string($data['run_id'] ?? null) || !is_string($data['job_id'] ?? null)) {
            throw new InvalidArgumentException('not a failure-matrix message');
        }
        $attempt = is_int($message['deliveryAttempt'] ?? null) ? $message['deliveryAttempt'] : null;
        $this->log->record($data['run_id'], $data['job_id'], $attempt, 'started', 'consume', null,
            ['code' => app('bench.deployed_code')]);
        // A signal must not shorten the declared work.
        $deadline = hrtime(true) + (int) ($data['sleep_ms'] ?? 0) * 1_000_000;
        while (($left = $deadline - hrtime(true)) > 0) {
            usleep(intdiv($left, 1_000) + 1);
        }
        $this->log->record($data['run_id'], $data['job_id'], $attempt, 'completed', 'consume');
    }
}
