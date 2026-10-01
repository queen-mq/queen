<?php

namespace App\Jobs\Compat;

use Illuminate\Contracts\Queue\ShouldBeUnique;

/** Unique by `jobId` while it waits or runs: the cache holds the lock. */
final class CompatUniqueJob extends CompatJob implements ShouldBeUnique
{
    public int $uniqueFor = 60;

    public function uniqueId(): string
    {
        return $this->runId . ':' . $this->jobId;
    }
}
