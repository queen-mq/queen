<?php

declare(strict_types=1);

use Queen\Laravel\Supervisor\Binary\SupervisorBinary;
use Queen\Laravel\Supervisor\Binary\SupervisorBinaryInstaller;

require __DIR__ . '/../../clients/client-php/vendor/autoload.php';

if ($argc !== 4) {
    fwrite(STDERR, "usage: php verify_client_archive.php MANIFEST ARCHIVE INSTALL_DIRECTORY\n");
    exit(2);
}

[$script, $manifest, $archive, $installDirectory] = $argv;
unset($script);

try {
    $result = (new SupervisorBinaryInstaller())->install(
        $installDirectory,
        $manifest,
        archiveSource: $archive,
        platform: SupervisorBinary::platform('Linux', 'amd64'),
    );
    if (($result['installed'] ?? false) !== true) {
        throw new RuntimeException('the release archive was not installed as a new generation');
    }
    SupervisorBinary::assertInstalled(
        $installDirectory,
        SupervisorBinary::platform('Linux', 'amd64'),
    );
} catch (Throwable $exception) {
    fwrite(STDERR, 'Laravel installer rejected release archive: ' . $exception->getMessage() . "\n");
    exit(1);
}

fwrite(STDOUT, "Laravel installer accepted and executed the Linux amd64 release archive.\n");
