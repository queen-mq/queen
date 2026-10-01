<?php

namespace App\Jobs\Compat;

use Illuminate\Contracts\Queue\ShouldBeUniqueUntilProcessing;

/** One per run while it waits: the cache lock goes as soon as one starts processing. */
final class CompatUniqueUntilProcessingJob extends CompatJob implements ShouldBeUniqueUntilProcessing
{
    public int $uniqueFor = 60;

    public function uniqueId(): string
    {
        return $this->runId;
    }
}
