<?php

namespace App\Jobs\Compat;

use Illuminate\Queue\Middleware\Skip;

/** Mode `skip`: Skip::when() deletes the job before it runs. Other modes run as CompatJob. */
final class CompatSkippedJob extends CompatJob
{
    public function middleware(): array
    {
        return [Skip::when(fn (): bool => $this->mode === 'skip')];
    }
}
