<?php

namespace App\Jobs\Compat;

use App\Models\CompatUser;

/**
 * Carries an Eloquent model: SerializesModels stores its key, the worker loads
 * the row again. When the row is gone, the job fails with a
 * ModelNotFoundException.
 */
class CompatModelJob extends CompatJob
{
    public ?CompatUser $user = null;

    public static function carrying(CompatUser $user, string $runId, string $jobId): static
    {
        $job = new static($runId, $jobId);
        $job->user = $user;

        return $job;
    }
}
