<?php

namespace App\Events;

use Illuminate\Foundation\Events\Dispatchable;

/** The event of `bench:compat-more queued-listener`: `mode` tells its listener to succeed (`ok`) or throw (`throw`). */
final class CompatEvent
{
    use Dispatchable;

    public function __construct(
        public string $runId,
        public string $jobId,
        public string $mode = 'ok',
    ) {
    }
}
