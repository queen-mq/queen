<?php

namespace App\Jobs\Compat;

use Illuminate\Queue\Middleware\RateLimited;

/** One per second (the `compat` limiter): the rest are released and retried. */
final class CompatRateLimitedJob extends CompatJob
{
    public function middleware(): array
    {
        return [new RateLimited('compat')];
    }
}
