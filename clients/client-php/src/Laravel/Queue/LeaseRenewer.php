<?php

namespace Queen\Laravel\Queue;

/**
 * Keeps broker leases alive while Laravel is executing synchronous job code.
 *
 * A lease can cover several prefetched messages (and partitions), so callers
 * track the lease rather than an individual job. Implementations must fail
 * closed: losing the ability to renew may duplicate work, but must never turn
 * an expired lease into a successful acknowledgement.
 */
interface LeaseRenewer
{
    /**
     * @param int $deadlineMonotonicMillis Conservative expiry measured on the
     *        host monotonic clock before the pop request began.
     */
    public function track(string $leaseId, int $deadlineMonotonicMillis): void;

    public function forget(string $leaseId): void;

    public function assertHealthy(string $leaseId): void;

    public function close(): void;
}
