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
        ?string $owner = null,
    ): array {
        // Refuse an owner that cannot be honoured before anything is written.
        $owner = $owner === null ? null : $this->resolveOwner($owner);
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
                $expectedBase,
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

            if ($owner !== null) {
                $this->handOver($expectedBase, $platform, $owner);
                $result['owner_uid'] = $owner['uid'];
            }

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

    /**
     * Resolve --owner to a uid, and to the user's primary group when the user
     * database names one, so the result matches an install run by that user.
     *
     * @return array{uid: int, gid: ?int}
     */
    private function resolveOwner(string $owner): array
    {
        if (!function_exists('posix_getpwnam') || !function_exists('posix_getpwuid')
            || !function_exists('lchown') || !function_exists('lchgrp')) {
            throw new RuntimeException('The --owner option needs ext-posix, lchown() and lchgrp().');
        }
        if (preg_match('/^[0-9]{1,10}$/D', $owner) === 1) {
            $uid = (int) $owner;
            // (uid_t) -1 means "leave the owner unchanged" to chown(2).
            if ($uid > 4294967294) {
                throw new RuntimeException("The --owner uid {$owner} is out of range.");
            }
            $entry = posix_getpwuid($uid);
        } else {
            if ($owner === '' || preg_match('#[\x00-\x20\x7F:/]#', $owner) === 1) {
                throw new RuntimeException('The --owner value must be a user name or a numeric uid.');
            }
            $entry = posix_getpwnam($owner);
            if (!is_array($entry) || !is_int($entry['uid'] ?? null)) {
                throw new RuntimeException(
                    "The --owner user {$owner} does not exist on this system. Name a user that exists "
                    . 'where the installer runs, or give a numeric uid.',
                );
            }
            $uid = $entry['uid'];
        }

        $effectiveUserId = SupervisorBinary::effectiveUserId();
        if ($effectiveUserId !== 0) {
            throw new RuntimeException(
                'The --owner option of queen:supervisor-install requires root, but the effective user is '
                . SupervisorBinary::describeUser($effectiveUserId) . '. Without root, run the installer as '
                . 'the user that runs the supervisor, without --owner.',
            );
        }
        if (PHP_ZTS) {
            throw new RuntimeException(
                'The --owner option is not available on a thread-safe (ZTS) PHP, whose chdir() does not pin '
                . 'the installation directory while its owner changes. Run the installer with a '
                . 'non-thread-safe PHP CLI, or as the user that runs the supervisor.',
            );
        }

        return [
            'uid' => $uid,
            'gid' => is_array($entry) && is_int($entry['gid'] ?? null) ? $entry['gid'] : null,
        ];
    }

    /**
     * Give the verified installation to the user that will run the supervisor.
     *
     * Root installs and verifies everything as itself first, in directories
     * only root can write, so nothing can change under it until this runs. The
     * owner then changes from the leaves up: each entry changes while the
     * directory holding it still belongs to root, so the new owner cannot swap
     * it, or put a link in its path, before the change is made. lchown() never
     * follows a link, and the base changes through ".", the inode chdir()
     * pinned, so a parent its new owner can write to cannot redirect it. A
     * thread-safe PHP pins "." by name only, which is why resolveOwner()
     * refuses it.
     *
     * @param array<string, int> $expectedBase
     * @param array{uid: int, gid: ?int} $owner
     */
    private function handOver(array $expectedBase, array $platform, array $owner): void
    {
        foreach ([
            [SupervisorBinary::binaryPath('.', $platform), false],
            [SupervisorBinary::receiptPath('.', $platform), false],
            [SupervisorBinary::installationDirectory('.', $platform), true],
            [SupervisorBinary::versionDirectory('.'), true],
            ['.' . DIRECTORY_SEPARATOR . '.install.lock', false],
        ] as [$path, $directory]) {
            $this->handOverEntry($path, $directory, $owner);
        }
        $this->handOverEntry('.', true, $owner, $expectedBase);
    }

    /**
     * @param array{uid: int, gid: ?int} $owner
     * @param array<string, int>|null $expected the pinned identity the entry must still have
     */
    private function handOverEntry(string $path, bool $directory, array $owner, ?array $expected = null): void
    {
        $effectiveUserId = SupervisorBinary::effectiveUserId();
        $display = SupervisorBinary::displayPath($path);
        $recipient = SupervisorBinary::describeUser($owner['uid']);
        clearstatcache(true, $path);
        $before = @lstat($path);
        $detail = SupervisorBinary::explainUnsafePath($display, $before, $directory, $effectiveUserId);
        if ($detail === '' && $expected !== null
            && ($before['dev'] !== $expected['dev'] || $before['ino'] !== $expected['ino'])) {
            $detail = "{$display} changed during installation.";
        }
        if ($detail !== '') {
            throw new RuntimeException("Cannot hand the Queen supervisor installation to {$recipient}. {$detail}");
        }

        if (!@lchown($path, $owner['uid'])
            || ($owner['gid'] !== null && !@lchgrp($path, $owner['gid']))) {
            throw new RuntimeException("Cannot change the owner of {$display} to {$recipient}.");
        }

        clearstatcache(true, $path);
        $after = @lstat($path);
        if (!is_array($after)
            || $after['dev'] !== $before['dev']
            || $after['ino'] !== $before['ino']
            || ($after['uid'] ?? null) !== $owner['uid']
            || ($owner['gid'] !== null && ($after['gid'] ?? null) !== $owner['gid'])
            || ($after['mode'] & 07777) !== ($before['mode'] & 07777)) {
            throw new RuntimeException(
                "The owner of {$display} did not change to {$recipient}, or its mode changed with it; "
                . 'the file system may not support ownership changes.',
            );
        }
    }

    private function installLocked(
        string $installBase,
        array $expectedBase,
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
            $expectedBase,
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
                $expectedBase,
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
                $expectedBase,
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
                $expectedBase,
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
                $expectedBase,
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
            // Run the binary inside its pinned directory on every PHP build.
            SupervisorBinary::pinnedDirectoryForExec(),
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
                "Queen supervisor {$description} must be a real, owned directory without group/world write access. "
                . $this->explainUnsafePath($path, $metadata, true, $effectiveUserId),
            );
        }

        return $metadata;
    }

    /** @param array<string, int>|false $metadata */
    private function explainUnsafePath(
        string $path,
        array|false $metadata,
        bool $directory,
        int $effectiveUserId,
    ): string {
        $display = SupervisorBinary::displayPath($path);
        $user = SupervisorBinary::userArgument($effectiveUserId);

        // The installer never writes into a tree another user can change.
        return SupervisorBinary::explainUnsafePath(
            $display,
            $metadata,
            $directory,
            $effectiveUserId,
            "Run the installer as the user that owns it, or chown -R {$user} {$display} first; "
            . 'as root, --owner=<user> hands the installation to the user that runs the supervisor.',
        );
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
        array $expectedBase,
        string $versionDirectory,
        array $expectedVersionDirectory,
        string $targetDirectory,
        array $expectedTargetDirectory,
    ): void {
        // Always true where chdir() moved the process onto the pinned inode. A
        // ZTS chdir() pins by name only, so a swapped base must be caught here,
        // before anything is published into the replacement directory.
        $this->assertDirectoryStillMatches('.', $expectedBase, 'pinned installation base');
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
                "Queen supervisor directory {$path} must be a real, owned directory without group/world write access. "
                . $this->explainUnsafePath($path, $metadata, true, $effectiveUserId),
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
            throw new RuntimeException(
                'The Queen supervisor installation lock is unsafe. '
                . $this->explainUnsafePath($path, $metadata, false, $effectiveUserId),
            );
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
