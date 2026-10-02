<?php

namespace App\Jobs\Compat;

use Illuminate\Contracts\Queue\ShouldBeEncrypted;

/** Its payload travels encrypted with the application key. */
final class CompatEncryptedJob extends CompatJob implements ShouldBeEncrypted
{
}
