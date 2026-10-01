<?php

namespace App\Jobs\Compat;

use Illuminate\Queue\Middleware\WithoutOverlapping;

/** Never two at once for one key: an overlapping attempt is released. */
final class CompatOverlapJob extends CompatJob
{
    public function middleware(): array
    {
        return [(new WithoutOverlapping($this->runId))->releaseAfter(1)->expireAfter(60)];
    }
}
