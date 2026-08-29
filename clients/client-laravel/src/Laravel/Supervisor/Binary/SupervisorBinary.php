<?php

namespace Queen\Laravel\Supervisor\Binary;

use RuntimeException;

final class SupervisorBinary
{
    /**
     * Kept in lockstep with supervisor/Cargo.toml by the release workflow.
     */
    public const VERSION = '0.1.0';

    public const MANIFEST_FILENAME = 'queen-supervisor-manifest.json';

    private const RELEASE_ROOT = 'https://github.com/queen-mq/queen/releases/download';

    public static function defaultReleaseBaseUrl(): string
    {
        return self::RELEASE_ROOT . '/supervisor%2Fv' . rawurlencode(self::VERSION);
    }

    public static function defaultManifestUrl(): string
    {
        return self::defaultReleaseBaseUrl() . '/' . self::MANIFEST_FILENAME;
    }

    public static function normalizeReleaseBaseUrl(string $url): string
    {
        self::assertHttpsUrl($url, 'release base URL');
        $parts = parse_url($url);
        if (isset($parts['query']) || isset($parts['fragment'])) {
            throw new RuntimeException('The release base URL must not contain a query string or fragment.');
        }

        return rtrim($url, '/');
    }

    public static function manifestUrlForBase(string $baseUrl): string
    {
        return self::normalizeReleaseBaseUrl($baseUrl) . '/' . self::MANIFEST_FILENAME;
    }

    public static function platform(?string $os = null, ?string $machine = null): array
    {
        $os ??= php_uname('s');
        $machine ??= php_uname('m');

        $normalizedOs = match (strtolower(trim($os))) {
            'linux' => 'linux',
            'darwin' => 'darwin',
            'windows', 'windows nt', 'winnt' => throw new RuntimeException(
                'The optimized Queen supervisor is not available on Windows: '
                . 'its worker fencing still requires Unix signals and process groups.',
            ),
            default => throw new RuntimeException(
                "The optimized Queen supervisor is not available for operating system {$os}.",
            ),
        };

        $normalizedMachine = match (strtolower(trim($machine))) {
            'x86_64', 'amd64' => 'amd64',
            'aarch64', 'arm64' => 'arm64',
            default => throw new RuntimeException(
                "No Queen supervisor release is available for {$normalizedOs} architecture {$machine}.",
            ),
        };

        $target = match ([$normalizedOs, $normalizedMachine]) {
            ['linux', 'amd64'] => 'x86_64-unknown-linux-musl',
            ['linux', 'arm64'] => 'aarch64-unknown-linux-musl',
            ['darwin', 'amd64'] => 'x86_64-apple-darwin',
            ['darwin', 'arm64'] => 'aarch64-apple-darwin',
        };

        return [
            'os' => $normalizedOs,
            'arch' => $normalizedMachine,
            'target' => $target,
        ];
    }

    public static function installationDirectory(string $basePath, array $platform): string
    {
        return rtrim($basePath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR . self::VERSION
            . DIRECTORY_SEPARATOR . $platform['os'] . '-' . $platform['arch'];
    }

    public static function binaryPath(string $basePath, array $platform): string
    {
        return self::installationDirectory($basePath, $platform)
            . DIRECTORY_SEPARATOR . 'queen-supervisor';
    }

    public static function receiptPath(string $basePath, array $platform): string
    {
        return self::installationDirectory($basePath, $platform)
            . DIRECTORY_SEPARATOR . 'receipt.json';
    }

    public static function assertHttpsUrl(string $url, string $description): void
    {
        $parts = parse_url($url);
        if (
            $parts === false
            || strtolower((string) ($parts['scheme'] ?? '')) !== 'https'
            || !isset($parts['host'])
            || trim((string) $parts['host']) === ''
            || isset($parts['user'])
            || isset($parts['pass'])
        ) {
            throw new RuntimeException("The {$description} must be an HTTPS URL without credentials.");
        }
    }

    public static function assertInstalled(string $basePath, array $platform): string
    {
        $binary = self::binaryPath($basePath, $platform);
        $receipt = self::receiptPath($basePath, $platform);
        if (!is_file($binary) || is_link($binary) || !is_executable($binary)) {
            throw new RuntimeException(
                "Queen supervisor " . self::VERSION
                . " is not installed for {$platform['os']}-{$platform['arch']}. "
                . 'Run php artisan queen:supervisor-install.',
            );
        }
        if (!is_file($receipt) || is_link($receipt)) {
            throw new RuntimeException('The Queen supervisor installation receipt is missing or unsafe. Reinstall it.');
        }

        $raw = file_get_contents($receipt);
        try {
            $data = is_string($raw) ? json_decode($raw, true, flags: JSON_THROW_ON_ERROR) : null;
        } catch (\JsonException $exception) {
            throw new RuntimeException('The Queen supervisor installation receipt is invalid. Reinstall it.', previous: $exception);
        }
        if (
            !is_array($data)
            || ($data['version'] ?? null) !== self::VERSION
            || ($data['target'] ?? null) !== $platform['target']
            || !is_string($data['binary_sha256'] ?? null)
            || preg_match('/^[a-f0-9]{64}$/D', $data['binary_sha256']) !== 1
        ) {
            throw new RuntimeException('The Queen supervisor installation receipt does not match this package. Reinstall it.');
        }

        $actual = hash_file('sha256', $binary);
        if (!is_string($actual) || !hash_equals($data['binary_sha256'], strtolower($actual))) {
            throw new RuntimeException('The installed Queen supervisor failed its SHA-256 integrity check. Reinstall it.');
        }

        return $binary;
    }
}
