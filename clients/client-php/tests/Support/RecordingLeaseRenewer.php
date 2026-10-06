<?php

namespace Queen\Tests\Support;

use Queen\Laravel\Queue\HandBackJournal;
use Queen\Laravel\Queue\LeaseRenewer;

/**
 * A lease renewer that records its calls instead of renewing: the real ones
 * refuse the test HTTP handler, since their helper builds its own client.
 */
final class RecordingLeaseRenewer implements LeaseRenewer
{
    /** @var list<array> [call, leaseId, extra], in order. */
    public array $events = [];

    /** Called at each forget(); what it returns is kept with the event. */
    public ?\Closure $onForget = null;

    /** Thrown by assertHealthy() when set. */
    public ?\Throwable $unhealthy = null;

    public function track(string $leaseId, int $deadlineMonotonicMillis): void
    {
        $this->events[] = ['track', $leaseId, $deadlineMonotonicMillis];
    }

    public function forget(string $leaseId): void
    {
        $this->events[] = ['forget', $leaseId, $this->onForget === null ? null : ($this->onForget)()];
    }

    public function assertHealthy(string $leaseId): void
    {
        $this->events[] = ['assertHealthy', $leaseId, null];
        if ($this->unhealthy !== null) {
            throw $this->unhealthy;
        }
    }

    public function close(): void
    {
        $this->events[] = ['close', null, null];
    }

    public function handBackJournal(): ?HandBackJournal
    {
        return null;
    }

    /** @return list<string> */
    public function calls(): array
    {
        return array_column($this->events, 0);
    }
}
