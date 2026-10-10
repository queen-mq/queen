<?php

namespace App\Support;

/**
 * The code a worker runs, as a deploy would change it. The failure matrix's
 * queue-restart scenario rewrites VERSION in the supervisor's container
 * before `queue:restart`; every matrix job logs the value its worker loaded.
 * The service provider loads this class at boot, so a worker forked from a
 * fork server booted before the change keeps the old value.
 */
final class DeployedCode
{
    public const VERSION = 'build';
}
