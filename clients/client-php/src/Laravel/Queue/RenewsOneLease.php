<?php

namespace Queen\Laravel\Queue;

use RuntimeException;

/**
 * The worker-side rules both renewers share: one live lease per synchronous
 * worker, enough time left for two renewal attempts and a fence, and a lease
 * reported unsafe stays unsafe.
 *
 * The using class declares requestBudgetSeconds, killGraceSeconds and
 * safetyMarginSeconds.
 */
trait RenewsOneLease
{
    /** @var array<string, true> */
    private array $tracked = [];

    /** @var array<string, string> Lease ID to the renewer's reason. */
    private array $failures = [];

    private function validateLeaseId(string $leaseId): void
    {
        if ($leaseId === '' || strlen($leaseId) > 255 || preg_match('/[\x00-\x1F\x7F]/', $leaseId)) {
            throw new RuntimeException('Queen returned an invalid lease ID for renewal.');
        }
    }

    private function assertTrackable(string $leaseId, int $deadlineMonotonicMillis): void
    {
        if ($this->tracked !== []) {
            throw new RuntimeException(
                'Queen Laravel lease renewal supports exactly one live pop lease per synchronous worker.',
            );
        }
        $initialReserveSeconds = 2 * $this->requestBudgetSeconds
            + 1
            + $this->killGraceSeconds
            + $this->safetyMarginSeconds;
        if ($deadlineMonotonicMillis <= self::monotonicMillis() + $initialReserveSeconds * 1000) {
            throw new RuntimeException("Queen lease [{$leaseId}] reached its renewal deadline before tracking began.");
        }
    }

    private function recordFailure(?array $event): void
    {
        if (($event['event'] ?? null) === 'unsafe'
            && is_string($event['lease_id'] ?? null)
            && $event['lease_id'] !== '') {
            $this->failures[$event['lease_id']] = (string) ($event['error'] ?? 'renewal deadline exhausted');
        }
    }

    private function assertLeaseSafe(string $leaseId): void
    {
        if (isset($this->failures[$leaseId])) {
            throw new RuntimeException(
                "Queen lease renewal became unsafe for [{$leaseId}]: {$this->failures[$leaseId]}",
            );
        }
        if (!isset($this->tracked[$leaseId])) {
            throw new RuntimeException("Queen lease renewal is not tracking [{$leaseId}].");
        }
    }

    private static function monotonicMillis(): int
    {
        return intdiv(hrtime(true), 1_000_000);
    }
}
