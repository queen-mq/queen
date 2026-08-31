<?php

declare(strict_types=1);

use Queen\Laravel\Supervisor\Binary\SupervisorBinary;
use Queen\Laravel\Supervisor\Binary\SupervisorReleaseManifest;

require __DIR__ . '/../../clients/client-laravel/vendor/autoload.php';

if ($argc !== 2) {
    fwrite(STDERR, "usage: php verify_client_manifest.php MANIFEST\n");
    exit(2);
}

$path = $argv[1];
if (!is_file($path) || is_link($path)) {
    fwrite(STDERR, "manifest must be a regular, non-symlink file\n");
    exit(1);
}
$json = file_get_contents($path);
if (!is_string($json)) {
    fwrite(STDERR, "cannot read release manifest\n");
    exit(1);
}

try {
    $manifest = SupervisorReleaseManifest::fromJson($json);
    foreach ([
        ['Linux', 'x86_64'],
        ['Linux', 'aarch64'],
        ['Darwin', 'x86_64'],
        ['Darwin', 'arm64'],
    ] as [$os, $machine]) {
        $platform = SupervisorBinary::platform($os, $machine);
        $artifact = $manifest->artifactFor($platform);
        if (
            $artifact['os'] !== $platform['os']
            || $artifact['arch'] !== $platform['arch']
            || $artifact['target'] !== $platform['target']
        ) {
            throw new RuntimeException("manifest selected a mismatched {$os}/{$machine} artifact");
        }
    }
} catch (Throwable $exception) {
    fwrite(STDERR, 'Laravel manifest consumer rejected release: ' . $exception->getMessage() . "\n");
    exit(1);
}

fwrite(STDOUT, "Laravel manifest consumer accepted all release targets.\n");
