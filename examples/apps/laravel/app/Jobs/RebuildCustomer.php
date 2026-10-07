<?php

namespace App\Jobs;

use App\Examples\Journal;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Queen\Laravel\Contracts\QueenPartitionable;

// docs:start(app-laravel-ordering-job)
final class RebuildCustomer implements ShouldQueue, QueenPartitionable
{
    use Queueable;

    public function __construct(
        public string $customer,
        public int $seq,
    ) {}

    // The partition of this job: every job of one customer goes to
    // the same ordered lane, which the broker leases to one worker at
    // a time.
    public function queenPartition(): string
    {
        return 'customer:' . $this->customer;
    }

    public function handle(): void
    {
        $started = microtime(true);
        usleep(300_000); // the work: 300 ms
        Journal::record($this->job, [
            'event' => 'ran',
            'customer' => $this->customer,
            'seq' => $this->seq,
            'started' => $started,
            'ended' => microtime(true),
        ]);
    }
}
// docs:end
