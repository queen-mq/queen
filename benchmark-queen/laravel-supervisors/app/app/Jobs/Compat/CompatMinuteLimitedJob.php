<?php

namespace App\Jobs\Compat;

use Illuminate\Queue\Middleware\RateLimited;

/**
 * One per minute and run (the `compat-minute` limiter): the rest are released
 * for a second at each run, so within the minute only the attempts they use
 * up end them.
 */
final class CompatMinuteLimitedJob extends CompatJob
{
    public function middleware(): array
    {
        return [(new RateLimited('compat-minute'))->releaseAfter(1)];
    }
}
