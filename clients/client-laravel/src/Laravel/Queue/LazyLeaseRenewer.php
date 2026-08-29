<?php

namespace Queen\Laravel\Queue;

use Closure;

/**
 * Defers the CLI-only renewal helper until a worker actually claims a lease.
 *
 * Laravel applications resolve the same queue connection from HTTP/FPM when
 * dispatching jobs. Constructing ProcessLeaseRenewer in the connector would
 * therefore make an opt-in worker safety feature break otherwise valid web
 * producers before they perform any consume operation.
 */
final class LazyLeaseRenewer implements LeaseRenewer
{
    private ?LeaseRenewer $delegate = null;

    /** @var Closure(): LeaseRenewer */
    private Closure $factory;

    /** @param callable(): LeaseRenewer $factory */
    public function __construct(callable $factory)
    {
        $this->factory = Closure::fromCallable($factory);
    }

    public function track(string $leaseId, int $deadlineMonotonicMillis): void
    {
        $this->instance()->track($leaseId, $deadlineMonotonicMillis);
    }

    public function forget(string $leaseId): void
    {
        $this->delegate?->forget($leaseId);
    }

    public function assertHealthy(string $leaseId): void
    {
        $this->instance()->assertHealthy($leaseId);
    }

    public function close(): void
    {
        $this->delegate?->close();
    }

    private function instance(): LeaseRenewer
    {
        if ($this->delegate === null) {
            $delegate = ($this->factory)();
            if (!$delegate instanceof LeaseRenewer) {
                throw new \LogicException('Queen lease renewer factory returned an invalid implementation.');
            }
            $this->delegate = $delegate;
        }

        return $this->delegate;
    }
}
