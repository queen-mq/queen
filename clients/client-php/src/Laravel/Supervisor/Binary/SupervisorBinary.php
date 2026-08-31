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

    private const MAX_BINARY_BYTES = 67108864;

    private const MAX_RECEIPT_BYTES = 65536;

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
        return self::versionDirectory($basePath)
            . DIRECTORY_SEPARATOR . $platform['os'] . '-' . $platform['arch'];
    }

    public static function versionDirectory(string $basePath): string
    {
        return rtrim($basePath, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR . self::VERSION;
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
        $basePath = self::normalizeInstallBasePath($basePath);
        self::assertInstallBaseIsNotFilesystemRoot($basePath);
        $effectiveUserId = self::effectiveUserId();
        self::assertSafeDirectory($basePath, 'installation base', $effectiveUserId);
        $versionDirectory = self::versionDirectory($basePath);
        self::assertSafeDirectory($versionDirectory, 'version directory', $effectiveUserId);
        $installationDirectory = self::installationDirectory($basePath, $platform);
        self::assertSafeDirectory($installationDirectory, 'target directory', $effectiveUserId);

        $binary = self::assertInstalledInDirectory($installationDirectory, $platform, $effectiveUserId);

        self::assertSafeDirectory($basePath, 'installation base', $effectiveUserId);
        self::assertSafeDirectory($versionDirectory, 'version directory', $effectiveUserId);
        self::assertSafeDirectory($installationDirectory, 'target directory', $effectiveUserId);

        return $binary;
    }

    /**
     * Pin the process working directory to the verified target inode.
     *
     * Executing the returned relative path prevents a writable ancestor from
     * swapping the installation tree between the final check and pcntl_exec.
     */
    public static function pinInstalledForExecution(string $basePath, array $platform): string
    {
        $basePath = self::normalizeInstallBasePath($basePath);
        self::assertInstallBaseIsNotFilesystemRoot($basePath);
        $effectiveUserId = self::effectiveUserId();
        $expectedBase = self::assertSafeDirectory($basePath, 'installation base', $effectiveUserId);
        $previousDirectory = getcwd();
        if (!is_string($previousDirectory) || $previousDirectory === '') {
            throw new RuntimeException('Cannot determine the current directory before launching Queen supervisor.');
        }

        $versionDirectory = self::versionDirectory($basePath);
        $expectedVersion = self::assertSafeDirectory(
            $versionDirectory,
            'version directory',
            $effectiveUserId,
        );
        $installationDirectory = self::installationDirectory($basePath, $platform);
        $expectedDirectory = self::assertSafeDirectory(
            $installationDirectory,
            'target directory',
            $effectiveUserId,
        );
        if (!@chdir($installationDirectory)) {
            throw new RuntimeException(
                "Queen supervisor " . self::VERSION
                . " is not installed for {$platform['os']}-{$platform['arch']}. "
                . 'Run php artisan queen:supervisor-install.',
            );
        }

        try {
            self::assertDirectoryStillMatches(
                '..' . DIRECTORY_SEPARATOR . '..',
                $expectedBase,
                'pinned installation base',
                $effectiveUserId,
            );
            self::assertDirectoryStillMatches(
                '..',
                $expectedVersion,
                'pinned version directory',
                $effectiveUserId,
            );
            self::assertDirectoryStillMatches(
                '.',
                $expectedDirectory,
                'pinned target directory',
                $effectiveUserId,
            );
            $binary = self::assertInstalledInDirectory('.', $platform, $effectiveUserId);
            self::assertDirectoryStillMatches(
                '.',
                $expectedDirectory,
                'pinned target directory',
                $effectiveUserId,
            );
            self::assertDirectoryStillMatches(
                '..',
                $expectedVersion,
                'pinned version directory',
                $effectiveUserId,
            );
            self::assertDirectoryStillMatches(
                '..' . DIRECTORY_SEPARATOR . '..',
                $expectedBase,
                'pinned installation base',
                $effectiveUserId,
            );

            return $binary;
        } catch (\Throwable $exception) {
            @chdir($previousDirectory);
            throw $exception;
        }
    }

    private static function assertInstalledInDirectory(
        string $installationDirectory,
        array $platform,
        int $effectiveUserId,
    ): string {
        $binary = $installationDirectory . DIRECTORY_SEPARATOR . 'queen-supervisor';
        $receipt = $installationDirectory . DIRECTORY_SEPARATOR . 'receipt.json';

        if (@lstat($binary) === false) {
            throw new RuntimeException(
                "Queen supervisor " . self::VERSION
                . " is not installed for {$platform['os']}-{$platform['arch']}. "
                . 'Run php artisan queen:supervisor-install.',
            );
        }
        if (@lstat($receipt) === false) {
            throw new RuntimeException('The Queen supervisor installation receipt is missing or unsafe. Reinstall it.');
        }

        $binaryFile = self::openSafeFile(
            $binary,
            'binary',
            self::MAX_BINARY_BYTES,
            $effectiveUserId,
            executable: true,
        );
        $receiptFile = null;
        try {
            $receiptFile = self::openSafeFile(
                $receipt,
                'installation receipt',
                self::MAX_RECEIPT_BYTES,
                $effectiveUserId,
            );
            $raw = stream_get_contents($receiptFile['handle'], self::MAX_RECEIPT_BYTES + 1);
            if (!is_string($raw) || $raw === '' || strlen($raw) > self::MAX_RECEIPT_BYTES) {
                throw new RuntimeException(
                    'The Queen supervisor installation receipt is unreadable or oversized. Reinstall it.',
                );
            }
            try {
                $data = json_decode($raw, true, flags: JSON_THROW_ON_ERROR);
            } catch (\JsonException $exception) {
                throw new RuntimeException('The Queen supervisor installation receipt is invalid. Reinstall it.', previous: $exception);
            }
            if (
                !is_array($data)
                || ($data['version'] ?? null) !== self::VERSION
                || ($data['target'] ?? null) !== $platform['target']
                || !is_string($data['source_commit'] ?? null)
                || preg_match('/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/D', $data['source_commit']) !== 1
                || !is_string($data['manifest_sha256'] ?? null)
                || preg_match('/^[a-f0-9]{64}$/D', $data['manifest_sha256']) !== 1
                || !is_string($data['binary_sha256'] ?? null)
                || preg_match('/^[a-f0-9]{64}$/D', $data['binary_sha256']) !== 1
            ) {
                throw new RuntimeException('The Queen supervisor installation receipt does not match this package. Reinstall it.');
            }

            $hash = hash_init('sha256');
            $hashedBytes = hash_update_stream($hash, $binaryFile['handle'], self::MAX_BINARY_BYTES + 1);
            if (!is_int($hashedBytes)
                || $hashedBytes < 1
                || $hashedBytes > self::MAX_BINARY_BYTES
                || $hashedBytes !== $binaryFile['metadata']['size']) {
                throw new RuntimeException('The installed Queen supervisor changed while hashing it. Reinstall it.');
            }
            $actual = hash_final($hash);
            if (!hash_equals($data['binary_sha256'], strtolower($actual))) {
                throw new RuntimeException('The installed Queen supervisor failed its SHA-256 integrity check. Reinstall it.');
            }

            self::assertFileStillMatches(
                $binary,
                $binaryFile['metadata'],
                'binary',
                $effectiveUserId,
                executable: true,
            );
            self::assertFileStillMatches(
                $receipt,
                $receiptFile['metadata'],
                'installation receipt',
                $effectiveUserId,
            );
        } finally {
            if (is_array($receiptFile) && is_resource($receiptFile['handle'] ?? null)) {
                fclose($receiptFile['handle']);
            }
            fclose($binaryFile['handle']);
        }

        return $binary;
    }

    public static function effectiveUserId(): int
    {
        if (!function_exists('posix_geteuid')) {
            throw new RuntimeException(
                'ext-posix is required to verify ownership of the native Queen supervisor installation.',
            );
        }
        $effectiveUserId = posix_geteuid();
        if (!is_int($effectiveUserId) || $effectiveUserId < 0) {
            throw new RuntimeException('Cannot determine the native Queen supervisor effective user ID.');
        }

        return $effectiveUserId;
    }

    /**
     * This guard is intentionally read-only so callers can validate dangerous
     * aliases before creating a directory, lock or temporary file.
     */
    public static function assertInstallBaseIsNotFilesystemRoot(string $basePath): void
    {
        $basePath = self::normalizeInstallBasePath($basePath);
        $root = @realpath(DIRECTORY_SEPARATOR);
        $resolved = @realpath($basePath);
        if (is_string($root) && is_string($resolved) && $resolved === $root) {
            throw new RuntimeException('The Queen supervisor install path must not be a filesystem root.');
        }
    }

    /**
     * Return an absolute lexical spelling whose final component can be
     * inspected with lstat without a trailing-separator symlink bypass.
     */
    public static function normalizeInstallBasePath(string $basePath): string
    {
        if (trim($basePath) === ''
            || str_contains($basePath, "\0")
            || preg_match('/[\x00-\x1F\x7F]/', $basePath) === 1) {
            throw new RuntimeException('The Queen supervisor install path is invalid.');
        }

        $absolute = str_starts_with($basePath, DIRECTORY_SEPARATOR)
            ? $basePath
            : (($current = getcwd()) !== false
                ? $current . DIRECTORY_SEPARATOR . $basePath
                : throw new RuntimeException('Cannot resolve the Queen supervisor install path.'));
        $components = preg_split(
            '#' . preg_quote(DIRECTORY_SEPARATOR, '#') . '+#',
            $absolute,
            -1,
            PREG_SPLIT_NO_EMPTY,
        );
        if (!is_array($components)) {
            throw new RuntimeException('The Queen supervisor install path is invalid.');
        }
        $normalized = [];
        foreach ($components as $component) {
            if ($component === '.') {
                continue;
            }
            if ($component === '..') {
                throw new RuntimeException('The Queen supervisor install path must not contain parent traversal.');
            }
            $normalized[] = $component;
        }
        if ($normalized === []) {
            throw new RuntimeException('The Queen supervisor install path must not be a filesystem root.');
        }

        return DIRECTORY_SEPARATOR . implode(DIRECTORY_SEPARATOR, $normalized);
    }

    /** @return array<string, int> */
    private static function assertSafeDirectory(string $path, string $description, int $effectiveUserId): array
    {
        $metadata = @lstat($path);
        if (!is_array($metadata)
            || ($metadata['mode'] & 0170000) !== 0040000
            || ($metadata['mode'] & 0022) !== 0
            || ($metadata['uid'] ?? null) !== $effectiveUserId) {
            throw new RuntimeException(
                "The Queen supervisor {$description} must be a real, owned directory without group/world write access.",
            );
        }

        return $metadata;
    }

    /** @param array<string, int> $opened */
    private static function assertDirectoryStillMatches(
        string $path,
        array $opened,
        string $description,
        int $effectiveUserId,
    ): void {
        $current = @lstat($path);
        if (!is_array($current)
            || ($current['mode'] & 0170000) !== 0040000
            || $current['dev'] !== $opened['dev']
            || $current['ino'] !== $opened['ino']
            || ($current['uid'] ?? null) !== $effectiveUserId
            || ($current['mode'] & 0022) !== 0) {
            throw new RuntimeException(
                "The Queen supervisor {$description} changed during verification. Reinstall it.",
            );
        }
    }

    /** @return array{handle: resource, metadata: array<string, int>} */
    private static function openSafeFile(
        string $path,
        string $description,
        int $maximumBytes,
        int $effectiveUserId,
        bool $executable = false,
    ): array {
        $before = @lstat($path);
        $handle = @fopen($path, 'rb');
        $current = @lstat($path);
        $opened = is_resource($handle) ? fstat($handle) : false;
        if (!is_array($before)
            || !is_resource($handle)
            || !is_array($current)
            || !is_array($opened)
            || ($before['mode'] & 0170000) !== 0100000
            || ($current['mode'] & 0170000) !== 0100000
            || ($opened['mode'] & 0170000) !== 0100000
            || $before['dev'] !== $opened['dev']
            || $before['ino'] !== $opened['ino']
            || $current['dev'] !== $opened['dev']
            || $current['ino'] !== $opened['ino']
            || ($opened['uid'] ?? null) !== $effectiveUserId
            || ($opened['mode'] & 0022) !== 0
            || ($executable && ($opened['mode'] & 0100) === 0)
            || $opened['size'] < 1
            || $opened['size'] > $maximumBytes) {
            if (is_resource($handle)) {
                fclose($handle);
            }
            throw new RuntimeException(
                "The Queen supervisor {$description} is unsafe, non-executable or oversized. Reinstall it.",
            );
        }

        return ['handle' => $handle, 'metadata' => $opened];
    }

    /** @param array<string, int> $opened */
    private static function assertFileStillMatches(
        string $path,
        array $opened,
        string $description,
        int $effectiveUserId,
        bool $executable = false,
    ): void {
        $current = @lstat($path);
        if (!is_array($current)
            || ($current['mode'] & 0170000) !== 0100000
            || $current['dev'] !== $opened['dev']
            || $current['ino'] !== $opened['ino']
            || ($current['uid'] ?? null) !== $effectiveUserId
            || ($current['mode'] & 0022) !== 0
            || ($executable && ($current['mode'] & 0100) === 0)) {
            throw new RuntimeException(
                "The Queen supervisor {$description} changed during verification. Reinstall it.",
            );
        }
    }
}
