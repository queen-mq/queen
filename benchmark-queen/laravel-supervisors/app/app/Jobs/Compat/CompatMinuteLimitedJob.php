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
        // RateLimited::releaseAfter() is Laravel 12's; both 11 and 12 take the
        // release delay from getTimeUntilNextRetry() when it is not set.
        return [new class('compat-minute') extends RateLimited {
            protected function getTimeUntilNextRetry($key)
            {
                return 1;
            }
        }];
    }
}
