<?php

namespace App\Jobs\Compat;

use Illuminate\Queue\Middleware\SkipIfBatchCancelled;

/** Never runs once its batch is cancelled: the middleware skips it before handle(). */
final class CompatSkipIfCancelledJob extends CompatJob
{
    public function middleware(): array
    {
        return [new SkipIfBatchCancelled()];
    }
}
