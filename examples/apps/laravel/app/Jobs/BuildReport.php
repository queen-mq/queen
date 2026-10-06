<?php

namespace App\Jobs;

use App\Examples\Journal;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;

// docs:start(app-laravel-lease-job)
final class BuildReport implements ShouldQueue
{
    use Queueable;

    // No $timeout here: the worker's --timeout applies. Without lease
    // renewal, the connection refuses to run a job whose own $timeout
    // is not shorter than retry_after.

    public function __construct(public int $seconds) {}

    public function handle(): void
    {
        Journal::record($this->job, [
            'event' => 'started',
            'attempt' => $this->attempts(),
        ]);
        sleep($this->seconds); // longer than the lease
    }
}
// docs:end
