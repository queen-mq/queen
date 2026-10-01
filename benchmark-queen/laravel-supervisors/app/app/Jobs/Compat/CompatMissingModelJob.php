<?php

namespace App\Jobs\Compat;

/** Deleted, without a failure, when its model is gone. */
final class CompatMissingModelJob extends CompatModelJob
{
    public bool $deleteWhenMissingModels = true;
}
