<?php

namespace App\Jobs;

use App\Examples\Journal;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;

// docs:start(app-laravel-prefetch-job)
final class ProbeBatch implements ShouldQueue
{
    use Queueable;

    public function __construct(public int $seq, public int $millis) {}

    public function handle(): void
    {
        // The broker leases a pop's jobs together, under one lease:
        // jobs that share a lease id came in the same pop.
        Journal::record($this->job, [
            'event' => 'ran',
            'seq' => $this->seq,
            'lease' => $this->job->getQueenMessage()['leaseId'],
        ]);
        usleep($this->millis * 1000);
    }
}
// docs:end
