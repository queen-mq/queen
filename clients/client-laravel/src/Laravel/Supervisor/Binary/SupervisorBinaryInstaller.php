<?php

namespace Queen\Laravel\Supervisor\Binary;

use GuzzleHttp\Client;
use PharData;
use RuntimeException;

final class SupervisorBinaryInstaller
{
    public const MAX_ARCHIVE_BYTES = 67108864;
    public const MAX_BINARY_BYTES = 67108864;

    public const MAX_METADATA_BYTES = 1048576;

    private const ARCHIVE_ENTRIES = [
        'LICENSE.md' => self::MAX_METADATA_BYTES,
        'queen-supervisor' => self::MAX_BINARY_BYTES,
        'queen-supervisor.service.example' => self::MAX_METADATA_BYTES,
    ];

    /** @var \Closure(string, string, int): void */
    private readonly \Closure $downloader;

    public function __construct(?callable $downloader = null)
    {
        $this->downloader = $downloader === null
            ? self::defaultDownloader(...)
            : \Closure::fromCallable($downloader);
    }

    public function install(
        string $installBase,
        string $manifestSource,
        ?string $archiveSource = null,
        ?string $releaseBaseUrl = null,
        bool $force = false,
        ?array $platform = null,
        ?string $manifestSha256 = null,
    ): array {
        $platform ??= SupervisorBinary::platform();
        $previousDirectory = getcwd();
        if (!is_string($previousDirectory) || $previousDirectory === '') {
            throw new RuntimeException('Cannot determine the working directory before installing Queen supervisor.');
        }
        $manifestSource = $this->normalizeLocalSource($manifestSource, $previousDirectory);
        if ($archiveSource !== null) {
            $archiveSource = $this->normalizeLocalSource($archiveSource, $previousDirectory);
        }
        $preparedBase = $this->prepareInstallBase($installBase, $previousDirectory);
        $installBase = $preparedBase['path'];
        $expectedBase = $preparedBase['metadata'];

        $lock = null;
        $locked = false;
        try {
            $this->assertDirectoryStillMatches('.', $expectedBase, 'pinned installation base');
            $lock = $this->openInstallLock('.' . DIRECTORY_SEPARATOR . '.install.lock');
            if (!flock($lock, LOCK_EX)) {
                throw new RuntimeException("Cannot lock Queen supervisor installation directory {$installBase}.");
            }
            $locked = true;

            $result = $this->installLocked(
                '.',
                $manifestSource,
                $archiveSource,
                $releaseBaseUrl,
                $force,
                $platform,
                $manifestSha256,
            );
            $this->assertDirectoryStillMatches('.', $expectedBase, 'pinned installation base');
            $this->assertDirectoryStillMatches($installBase, $expectedBase, 'installation base path');

            $relativePrefix = '.' . DIRECTORY_SEPARATOR;
            if (!is_string($result['binary'] ?? null)
                || !str_starts_with($result['binary'], $relativePrefix)) {
                throw new RuntimeException('The Queen supervisor installer returned an invalid binary path.');
            }
            $result['binary'] = $installBase . DIRECTORY_SEPARATOR
                . substr($result['binary'], strlen($relativePrefix));

            return $result;
        } finally {
            if (is_resource($lock)) {
                if ($locked) {
                    flock($lock, LOCK_UN);
                }
                fclose($lock);
            }
            if (!@chdir($previousDirectory)) {
                throw new RuntimeException('Cannot restore the working directory after installing Queen supervisor.');
            }
        }
    }

    private function installLocked(
        string $installBase,
        string $manifestSource,
        ?string $archiveSource,
        ?string $releaseBaseUrl,
        bool $force,
        array $platform,
        ?string $expectedManifestHash,
    ): array {
        $manifestJson = $this->readSource($manifestSource, SupervisorReleaseManifest::MAX_BYTES, 'manifest');
        $manifestHash = hash('sha256', $manifestJson);
        if ($expectedManifestHash !== null) {
            $expectedManifestHash = strtolower(trim($expectedManifestHash));
            if (preg_match('/^[a-f0-9]{64}$/D', $expectedManifestHash) !== 1) {
                throw new RuntimeException('The pinned Queen supervisor manifest SHA-256 is invalid.');
            }
            if (!hash_equals($expectedManifestHash, $manifestHash)) {
                throw new RuntimeException('The Queen supervisor manifest failed its pinned SHA-256 check.');
            }
        }
        $manifest = SupervisorReleaseManifest::fromJson($manifestJson);
        $artifact = $manifest->artifactFor($platform);
        if ($releaseBaseUrl !== null) {
            $artifact['url'] = SupervisorBinary::normalizeReleaseBaseUrl($releaseBaseUrl)
                . '/' . rawurlencode($artifact['filename']);
        }

        $versionDirectory = SupervisorBinary::versionDirectory($installBase);
        $this->ensureDirectory($versionDirectory);
        $expectedVersionDirectory = $this->safeDirectoryMetadata(
            $versionDirectory,
            'version directory',
        );
        $targetDirectory = SupervisorBinary::installationDirectory($installBase, $platform);
        $this->ensureDirectory($targetDirectory);
        $expectedTargetDirectory = $this->safeDirectoryMetadata(
            $targetDirectory,
            'target directory',
        );
        $this->assertInstallDirectoriesStillMatch(
            $versionDirectory,
            $expectedVersionDirectory,
            $targetDirectory,
            $expectedTargetDirectory,
        );
        $binaryPath = SupervisorBinary::binaryPath($installBase, $platform);
        $receiptPath = SupervisorBinary::receiptPath($installBase, $platform);

        if (!$force && is_file($binaryPath) && is_file($receiptPath)) {
            try {
                SupervisorBinary::assertInstalled($installBase, $platform);
                $receipt = json_decode((string) file_get_contents($receiptPath), true, flags: JSON_THROW_ON_ERROR);
                if (($receipt['archive_sha256'] ?? null) === $artifact['sha256']) {
                    return $receipt + ['binary' => $binaryPath, 'installed' => false];
                }
            } catch (\Throwable) {
                // A partial, stale or tampered installation is replaced only
                // after the new archive passes every validation below.
            }
        }

        $archivePath = $this->temporaryPath($targetDirectory, '.archive-', '.tar.gz');
        $binaryTemporaryPath = null;
        $receiptTemporaryPath = null;
        try {
            if ($archiveSource !== null) {
                $this->copyLocalFile($archiveSource, $archivePath, self::MAX_ARCHIVE_BYTES, 'archive');
            } else {
                ($this->downloader)($artifact['url'], $archivePath, self::MAX_ARCHIVE_BYTES);
                $this->assertBoundedFile($archivePath, self::MAX_ARCHIVE_BYTES, 'downloaded archive');
            }

            $actualArchiveHash = hash_file('sha256', $archivePath);
            if (!is_string($actualArchiveHash) || !hash_equals($artifact['sha256'], strtolower($actualArchiveHash))) {
                throw new RuntimeException('The Queen supervisor archive failed its SHA-256 integrity check.');
            }

            $binaryTemporaryPath = $this->temporaryPath($targetDirectory, '.binary-');
            $this->extractBinary($archivePath, $binaryTemporaryPath);
            if (!chmod($binaryTemporaryPath, 0755)) {
                throw new RuntimeException('Cannot mark the Queen supervisor binary executable.');
            }
            $this->verifyExecutableVersion(
                $binaryTemporaryPath,
                '..' . DIRECTORY_SEPARATOR . '..',
            );
            $this->assertInstallDirectoriesStillMatch(
                $versionDirectory,
                $expectedVersionDirectory,
                $targetDirectory,
                $expectedTargetDirectory,
            );

            $binaryHash = hash_file('sha256', $binaryTemporaryPath);
            if (!is_string($binaryHash)) {
                throw new RuntimeException('Cannot hash the extracted Queen supervisor binary.');
            }
            $receipt = [
                'schema_version' => 1,
                'version' => SupervisorBinary::VERSION,
                'target' => $platform['target'],
                'source_commit' => $manifest->sourceCommit(),
                'manifest_sha256' => $manifestHash,
                'archive_filename' => $artifact['filename'],
                'archive_sha256' => $artifact['sha256'],
                'binary_sha256' => strtolower($binaryHash),
            ];
            $receiptTemporaryPath = $this->temporaryPath($targetDirectory, '.receipt-');
            $this->writeFile(
                $receiptTemporaryPath,
                json_encode($receipt, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR) . "\n",
                0644,
            );

            $this->assertInstallDirectoriesStillMatch(
                $versionDirectory,
                $expectedVersionDirectory,
                $targetDirectory,
                $expectedTargetDirectory,
            );

            if (!rename($binaryTemporaryPath, $binaryPath)) {
                throw new RuntimeException('Cannot atomically publish the Queen supervisor binary.');
            }
            $binaryTemporaryPath = null;
            $this->assertInstallDirectoriesStillMatch(
                $versionDirectory,
                $expectedVersionDirectory,
                $targetDirectory,
                $expectedTargetDirectory,
            );
            if (!rename($receiptTemporaryPath, $receiptPath)) {
                throw new RuntimeException('Cannot atomically publish the Queen supervisor installation receipt.');
            }
            $receiptTemporaryPath = null;
            $this->assertInstallDirectoriesStillMatch(
                $versionDirectory,
                $expectedVersionDirectory,
                $targetDirectory,
                $expectedTargetDirectory,
            );

            return $receipt + ['binary' => $binaryPath, 'installed' => true];
        } finally {
            @unlink($archivePath);
            if ($binaryTemporaryPath !== null) {
                @unlink($binaryTemporaryPath);
            }
            if ($receiptTemporaryPath !== null) {
                @unlink($receiptTemporaryPath);
            }
        }
    }

    private function readSource(string $source, int $maximumBytes, string $description): string
    {
        if ($this->isUrl($source)) {
            SupervisorBinary::assertHttpsUrl($source, $description . ' URL');
            $temporary = tempnam(sys_get_temp_dir(), 'queen-supervisor-manifest-');
            if ($temporary === false) {
                throw new RuntimeException('Cannot create a temporary manifest file.');
            }
            try {
                ($this->downloader)($source, $temporary, $maximumBytes);
                $this->assertBoundedFile($temporary, $maximumBytes, $description);
                $contents = file_get_contents($temporary);
            } finally {
                @unlink($temporary);
            }
        } else {
            $this->assertSafeLocalFile($source, $maximumBytes, $description);
            $contents = file_get_contents($source);
        }
        if (!is_string($contents)) {
            throw new RuntimeException("Cannot read Queen supervisor {$description}.");
        }

        return $contents;
    }

    private function copyLocalFile(string $source, string $destination, int $maximumBytes, string $description): void
    {
        $this->assertSafeLocalFile($source, $maximumBytes, $description);
        $input = @fopen($source, 'rb');
        $output = @fopen($destination, 'wb');
        if ($input === false || $output === false) {
            if (is_resource($input)) {
                fclose($input);
            }
            if (is_resource($output)) {
                fclose($output);
            }
            throw new RuntimeException("Cannot copy Queen supervisor {$description}.");
        }
        try {
            $copied = stream_copy_to_stream($input, $output, $maximumBytes + 1);
            if (!is_int($copied) || $copied > $maximumBytes) {
                throw new RuntimeException("The Queen supervisor {$description} exceeds the size limit.");
            }
            fflush($output);
            if (function_exists('fsync')) {
                fsync($output);
            }
        } finally {
            fclose($input);
            fclose($output);
        }
    }

    private function extractBinary(string $archivePath, string $destination): void
    {
        try {
            $archive = new PharData($archivePath);
        } catch (\Throwable $exception) {
            throw new RuntimeException('Cannot read the Queen supervisor release archive.', previous: $exception);
        }
        if (count($archive) !== count(self::ARCHIVE_ENTRIES)) {
            throw new RuntimeException('The release archive contains an unexpected entry set.');
        }
        foreach (self::ARCHIVE_ENTRIES as $name => $maximumBytes) {
            if (!isset($archive[$name])) {
                throw new RuntimeException("The release archive does not contain {$name}.");
            }
            $candidate = $archive[$name];
            if ($candidate->isDir()
                || $candidate->isLink()
                || $candidate->getSize() < 1
                || $candidate->getSize() > $maximumBytes) {
                throw new RuntimeException("The release archive entry {$name} is unsafe or exceeds the size limit.");
            }
        }
        $entry = $archive['queen-supervisor'];
        try {
            $contents = $entry->getContent();
        } catch (\Throwable $exception) {
            throw new RuntimeException('Cannot extract the Queen supervisor binary.', previous: $exception);
        }

        if (!is_string($contents) || $contents === '' || strlen($contents) > self::MAX_BINARY_BYTES) {
            throw new RuntimeException('The extracted Queen supervisor binary is empty or exceeds the size limit.');
        }
        $this->writeFile($destination, $contents, 0600);
    }

    private function verifyExecutableVersion(string $binary, string $restoreDirectory): void
    {
        if (!function_exists('proc_open')) {
            throw new RuntimeException('proc_open is required to verify the downloaded Queen supervisor.');
        }

        $effectiveUserId = SupervisorBinary::effectiveUserId();
        $directory = dirname($binary);
        $filename = basename($binary);
        $previousDirectory = getcwd();
        $expectedDirectory = @lstat($directory);
        $expectedBinary = @lstat($binary);
        if (!is_string($previousDirectory)
            || $previousDirectory === ''
            || $filename === ''
            || $filename === '.'
            || $filename === '..'
            || !is_array($expectedDirectory)
            || !is_array($expectedBinary)
            || ($expectedDirectory['mode'] & 0170000) !== 0040000
            || ($expectedDirectory['mode'] & 0022) !== 0
            || ($expectedDirectory['uid'] ?? null) !== $effectiveUserId
            || ($expectedBinary['mode'] & 0170000) !== 0100000
            || ($expectedBinary['mode'] & 0022) !== 0
            || ($expectedBinary['mode'] & 0100) === 0
            || ($expectedBinary['uid'] ?? null) !== $effectiveUserId) {
            throw new RuntimeException('The downloaded Queen supervisor executable is unsafe.');
        }
        if (!@chdir($directory)) {
            throw new RuntimeException('Cannot pin the downloaded Queen supervisor directory for verification.');
        }

        try {
            $pinnedDirectory = @lstat('.');
            $relativeBinary = '.' . DIRECTORY_SEPARATOR . $filename;
            $pinnedBinary = @lstat($relativeBinary);
            if (!is_array($pinnedDirectory)
                || !is_array($pinnedBinary)
                || ($pinnedDirectory['mode'] & 0170000) !== 0040000
                || $pinnedDirectory['dev'] !== $expectedDirectory['dev']
                || $pinnedDirectory['ino'] !== $expectedDirectory['ino']
                || ($pinnedDirectory['mode'] & 0022) !== 0
                || ($pinnedDirectory['uid'] ?? null) !== $effectiveUserId
                || ($pinnedBinary['mode'] & 0170000) !== 0100000
                || $pinnedBinary['dev'] !== $expectedBinary['dev']
                || $pinnedBinary['ino'] !== $expectedBinary['ino']
                || ($pinnedBinary['mode'] & 0022) !== 0
                || ($pinnedBinary['mode'] & 0100) === 0
                || ($pinnedBinary['uid'] ?? null) !== $effectiveUserId) {
                throw new RuntimeException(
                    'The downloaded Queen supervisor changed before its version smoke test.',
                );
            }

            $this->runExecutableVersionCheck($relativeBinary, $expectedBinary, $effectiveUserId);
        } finally {
            if (!@chdir($restoreDirectory)) {
                throw new RuntimeException('Cannot restore the working directory after supervisor verification.');
            }
        }
    }

    /** @param array<string, int> $expectedBinary */
    private function runExecutableVersionCheck(
        string $binary,
        array $expectedBinary,
        int $effectiveUserId,
    ): void {
        $pipes = [];
        $process = @proc_open(
            [$binary, '--version'],
            [0 => ['file', '/dev/null', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
            null,
            null,
            ['bypass_shell' => true],
        );
        if (!is_resource($process)) {
            throw new RuntimeException('Cannot execute the downloaded Queen supervisor for verification.');
        }
        stream_set_blocking($pipes[1], false);
        stream_set_blocking($pipes[2], false);
        $stdout = '';
        $stderr = '';
        $deadline = microtime(true) + 5.0;
        do {
            $stdout .= (string) stream_get_contents($pipes[1], 4096);
            $stderr .= (string) stream_get_contents($pipes[2], 4096);
            $status = proc_get_status($process);
            if (!$status['running']) {
                break;
            }
            usleep(10000);
        } while (microtime(true) < $deadline && strlen($stdout) + strlen($stderr) <= 8192);

        if ($status['running'] ?? false) {
            proc_terminate($process, 9);
        }
        $stdout .= (string) stream_get_contents($pipes[1], 4096);
        $stderr .= (string) stream_get_contents($pipes[2], 4096);
        fclose($pipes[1]);
        fclose($pipes[2]);
        $closedExit = proc_close($process);
        $exit = !($status['running'] ?? true) && is_int($status['exitcode'] ?? null) && $status['exitcode'] >= 0
            ? $status['exitcode']
            : $closedExit;
        if (
            ($status['running'] ?? false)
            || $exit !== 0
            || trim($stdout) !== 'queen-supervisor ' . SupervisorBinary::VERSION
            || trim($stderr) !== ''
        ) {
            throw new RuntimeException('The downloaded Queen supervisor failed its version smoke test.');
        }

        $current = @lstat($binary);
        if (!is_array($current)
            || ($current['mode'] & 0170000) !== 0100000
            || $current['dev'] !== $expectedBinary['dev']
            || $current['ino'] !== $expectedBinary['ino']
            || ($current['mode'] & 0022) !== 0
            || ($current['mode'] & 0100) === 0
            || ($current['uid'] ?? null) !== $effectiveUserId) {
            throw new RuntimeException('The downloaded Queen supervisor changed during its version smoke test.');
        }
    }

    /** @return array{path: string, metadata: array<string, int>} */
    private function prepareInstallBase(string $path, string $restoreDirectory): array
    {
        $path = SupervisorBinary::normalizeInstallBasePath($path);
        SupervisorBinary::assertInstallBaseIsNotFilesystemRoot($path);
        $parent = dirname($path);
        $leaf = basename($path);
        if ($leaf === '' || $leaf === '.' || $leaf === '..') {
            throw new RuntimeException('The Queen supervisor install path has no safe final component.');
        }

        $expectedParent = $this->realDirectoryMetadata($parent, 'installation parent');
        if (!@chdir($parent)) {
            throw new RuntimeException("Cannot pin Queen supervisor installation parent {$parent}.");
        }

        try {
            $this->assertRealDirectoryStillMatches('.', $expectedParent, 'pinned installation parent');
            if (@lstat($leaf) === false && !@mkdir($leaf, 0755) && @lstat($leaf) === false) {
                throw new RuntimeException("Cannot create Queen supervisor directory {$path}.");
            }
            $metadata = $this->safeDirectoryMetadata($leaf, 'installation base');
            if (!is_writable($leaf)) {
                throw new RuntimeException("Queen supervisor install path {$path} is not writable.");
            }
            SupervisorBinary::assertInstallBaseIsNotFilesystemRoot($path);
            if (!@chdir($leaf)) {
                throw new RuntimeException("Cannot pin Queen supervisor installation directory {$path}.");
            }
            $this->assertDirectoryStillMatches('.', $metadata, 'pinned installation base');

            return ['path' => $path, 'metadata' => $metadata];
        } catch (\Throwable $error) {
            if (!@chdir($restoreDirectory)) {
                throw new RuntimeException(
                    'Cannot restore the working directory after rejecting a Queen supervisor install path.',
                    previous: $error,
                );
            }

            throw $error;
        }
    }

    /** @return array<string, int> */
    private function realDirectoryMetadata(string $path, string $description): array
    {
        $metadata = @lstat($path);
        if (!is_array($metadata) || ($metadata['mode'] & 0170000) !== 0040000) {
            throw new RuntimeException("Queen supervisor {$description} must be an existing real directory.");
        }

        return $metadata;
    }

    /** @param array<string, int> $expected */
    private function assertRealDirectoryStillMatches(
        string $path,
        array $expected,
        string $description,
    ): void {
        $current = $this->realDirectoryMetadata($path, $description);
        if ($current['dev'] !== $expected['dev'] || $current['ino'] !== $expected['ino']) {
            throw new RuntimeException("Queen supervisor {$description} changed during installation.");
        }
    }

    private function normalizeLocalSource(string $source, string $baseDirectory): string
    {
        if ($this->isUrl($source) || str_starts_with($source, DIRECTORY_SEPARATOR)) {
            return $source;
        }

        return $baseDirectory . DIRECTORY_SEPARATOR . $source;
    }

    /** @return array<string, int> */
    private function safeDirectoryMetadata(string $path, string $description): array
    {
        $effectiveUserId = SupervisorBinary::effectiveUserId();
        $metadata = @lstat($path);
        if (!is_array($metadata)
            || ($metadata['mode'] & 0170000) !== 0040000
            || ($metadata['mode'] & 0022) !== 0
            || ($metadata['uid'] ?? null) !== $effectiveUserId) {
            throw new RuntimeException(
                "Queen supervisor {$description} must be a real, owned directory without group/world write access.",
            );
        }

        return $metadata;
    }

    /** @param array<string, int> $expected */
    private function assertDirectoryStillMatches(
        string $path,
        array $expected,
        string $description,
    ): void {
        $current = $this->safeDirectoryMetadata($path, $description);
        if ($current['dev'] !== $expected['dev'] || $current['ino'] !== $expected['ino']) {
            throw new RuntimeException("Queen supervisor {$description} changed during installation.");
        }
    }

    /**
     * @param array<string, int> $expectedVersionDirectory
     * @param array<string, int> $expectedTargetDirectory
     */
    private function assertInstallDirectoriesStillMatch(
        string $versionDirectory,
        array $expectedVersionDirectory,
        string $targetDirectory,
        array $expectedTargetDirectory,
    ): void {
        $this->assertDirectoryStillMatches(
            $versionDirectory,
            $expectedVersionDirectory,
            'version directory',
        );
        $this->assertDirectoryStillMatches(
            $targetDirectory,
            $expectedTargetDirectory,
            'target directory',
        );
    }

    private function ensureDirectory(string $path): void
    {
        $effectiveUserId = SupervisorBinary::effectiveUserId();
        if (!is_dir($path) && !@mkdir($path, 0755) && !is_dir($path)) {
            throw new RuntimeException("Cannot create Queen supervisor directory {$path}.");
        }
        $metadata = @lstat($path);
        if (!is_array($metadata)
            || ($metadata['mode'] & 0170000) !== 0040000
            || ($metadata['mode'] & 0022) !== 0
            || ($metadata['uid'] ?? null) !== $effectiveUserId) {
            throw new RuntimeException(
                "Queen supervisor directory {$path} must be a real, owned directory without group/world write access.",
            );
        }
    }

    /** @return resource */
    private function openInstallLock(string $path)
    {
        $effectiveUserId = SupervisorBinary::effectiveUserId();
        $metadata = @lstat($path);
        if (is_array($metadata) && (
            ($metadata['mode'] & 0170000) !== 0100000
            || ($metadata['mode'] & 0022) !== 0
            || ($metadata['uid'] ?? null) !== $effectiveUserId
        )) {
            throw new RuntimeException('The Queen supervisor installation lock is unsafe.');
        }
        $handle = @fopen($path, 'c+b');
        if (!is_resource($handle) || !@chmod($path, 0600)) {
            if (is_resource($handle)) {
                fclose($handle);
            }
            throw new RuntimeException('Cannot create the Queen supervisor installation lock.');
        }
        $current = @lstat($path);
        $opened = fstat($handle);
        if (!is_array($current)
            || !is_array($opened)
            || ($current['mode'] & 0170000) !== 0100000
            || ($opened['mode'] & 0170000) !== 0100000
            || ($opened['mode'] & 0777) !== 0600
            || $current['dev'] !== $opened['dev']
            || $current['ino'] !== $opened['ino']
            || ($opened['uid'] ?? null) !== $effectiveUserId) {
            fclose($handle);
            throw new RuntimeException('The Queen supervisor installation lock changed while opening it.');
        }

        return $handle;
    }

    private function assertSafeLocalFile(string $path, int $maximumBytes, string $description): void
    {
        if (!is_file($path) || is_link($path)) {
            throw new RuntimeException("The local Queen supervisor {$description} must be a regular non-symlink file.");
        }
        $this->assertBoundedFile($path, $maximumBytes, $description);
    }

    private function assertBoundedFile(string $path, int $maximumBytes, string $description): void
    {
        clearstatcache(true, $path);
        $size = filesize($path);
        if (!is_int($size) || $size < 1 || $size > $maximumBytes) {
            throw new RuntimeException("The Queen supervisor {$description} is empty or exceeds the size limit.");
        }
    }

    private function temporaryPath(string $directory, string $prefix, string $suffix = ''): string
    {
        $createdPath = tempnam($directory, $prefix);
        if ($createdPath === false) {
            throw new RuntimeException("Cannot create a temporary file in {$directory}.");
        }
        $path = rtrim($directory, DIRECTORY_SEPARATOR)
            . DIRECTORY_SEPARATOR . basename($createdPath);
        $relativeMetadata = @lstat($path);
        if (!is_array($relativeMetadata)
            || ($relativeMetadata['mode'] & 0170000) !== 0100000
            || ($relativeMetadata['mode'] & 0022) !== 0
            || ($relativeMetadata['uid'] ?? null) !== SupervisorBinary::effectiveUserId()) {
            @unlink($path);
            throw new RuntimeException("Cannot pin a temporary file in {$directory}.");
        }
        if ($suffix !== '') {
            $suffixed = $path . $suffix;
            if (!rename($path, $suffixed)) {
                @unlink($path);
                throw new RuntimeException("Cannot reserve a temporary file in {$directory}.");
            }
            $path = $suffixed;
        }

        return $path;
    }

    private function writeFile(string $path, string $contents, int $mode): void
    {
        $stream = @fopen($path, 'wb');
        if ($stream === false) {
            throw new RuntimeException("Cannot write Queen supervisor file {$path}.");
        }
        try {
            $offset = 0;
            $length = strlen($contents);
            while ($offset < $length) {
                $written = fwrite($stream, substr($contents, $offset));
                if (!is_int($written) || $written < 1) {
                    throw new RuntimeException("Cannot completely write Queen supervisor file {$path}.");
                }
                $offset += $written;
            }
            fflush($stream);
            if (function_exists('fsync')) {
                fsync($stream);
            }
        } finally {
            fclose($stream);
        }
        if (!chmod($path, $mode)) {
            throw new RuntimeException("Cannot set permissions on Queen supervisor file {$path}.");
        }
    }

    private function isUrl(string $source): bool
    {
        return preg_match('#^[A-Za-z][A-Za-z0-9+.-]*://#D', $source) === 1;
    }

    private static function defaultDownloader(string $url, string $destination, int $maximumBytes): void
    {
        SupervisorBinary::assertHttpsUrl($url, 'download URL');
        try {
            $response = (new Client())->request('GET', $url, [
                'allow_redirects' => [
                    'max' => 3,
                    'strict' => true,
                    'protocols' => ['https'],
                ],
                'connect_timeout' => 10,
                'timeout' => 120,
                'http_errors' => true,
                'sink' => $destination,
                'headers' => ['User-Agent' => 'queen-mq-php/' . SupervisorBinary::VERSION],
                'progress' => static function (
                    int $downloadTotal,
                    int $downloadedBytes,
                ) use ($maximumBytes): void {
                    if ($downloadTotal > $maximumBytes || $downloadedBytes > $maximumBytes) {
                        throw new RuntimeException('Queen supervisor download exceeds the size limit.');
                    }
                },
            ]);
        } catch (\Throwable $exception) {
            @unlink($destination);
            throw new RuntimeException("Cannot download Queen supervisor release from {$url}.", previous: $exception);
        }
        if ($response->getStatusCode() !== 200) {
            @unlink($destination);
            throw new RuntimeException("Queen supervisor download returned HTTP {$response->getStatusCode()}.");
        }
    }
}
