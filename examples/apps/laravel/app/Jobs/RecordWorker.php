<?php

namespace App\Jobs;

use App\Examples\Journal;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;

// docs:start(app-laravel-prefork-job)
final class RecordWorker implements ShouldQueue
{
    use Queueable;

    public function handle(): void
    {
        // Journal::record() adds the worker's pid. The parent tells a
        // forked worker (a child of the fork server) from a spawned one
        // (a child of the master).
        Journal::record($this->job, [
            'event' => 'ran',
            'ppid' => posix_getppid(),
        ]);
    }
}
// docs:end
