<?php

namespace Queen\Laravel\Supervisor;

use Composer\InstalledVersions;

final class SupervisorMetadata
{
    /** The installed package, never the separately pinned native binary. */
    public static function clientVersion(): ?string
    {
        try {
            $version = class_exists(InstalledVersions::class) && InstalledVersions::isInstalled('queen-mq/php-client')
                ? InstalledVersions::getPrettyVersion('queen-mq/php-client')
                : null;
            return is_string($version) && preg_match('/\A[^\x00-\x1f\x7f]{1,64}\z/D', $version) === 1
                ? $version : null;
        } catch (\Throwable) {
            return null;
        }
    }
}
