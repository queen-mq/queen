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
    ): array {
        $platform ??= SupervisorBinary::platform();
        $installBase = $this->prepareInstallBase($installBase);
        $lockPath = $installBase . DIRECTORY_SEPARATOR . '.install.lock';
        $lock = @fopen($lockPath, 'c+b');
        if ($lock === false || !flock($lock, LOCK_EX)) {
            if (is_resource($lock)) {
                fclose($lock);
            }
            throw new RuntimeException("Cannot lock Queen supervisor installation directory {$installBase}.");
        }

        try {
            return $this->installLocked(
                $installBase,
                $manifestSource,
                $archiveSource,
                $releaseBaseUrl,
                $force,
                $platform,
            );
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }
    }

    private function installLocked(
        string $installBase,
        string $manifestSource,
        ?string $archiveSource,
        ?string $releaseBaseUrl,
        bool $force,
        array $platform,
    ): array {
        $manifestJson = $this->readSource($manifestSource, SupervisorReleaseManifest::MAX_BYTES, 'manifest');
        $manifest = SupervisorReleaseManifest::fromJson($manifestJson);
        $artifact = $manifest->artifactFor($platform);
        if ($releaseBaseUrl !== null) {
            $artifact['url'] = SupervisorBinary::normalizeReleaseBaseUrl($releaseBaseUrl)
                . '/' . rawurlencode($artifact['filename']);
        }

        $targetDirectory = SupervisorBinary::installationDirectory($installBase, $platform);
        $this->ensureDirectory($targetDirectory);
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
            $this->verifyExecutableVersion($binaryTemporaryPath);

            $binaryHash = hash_file('sha256', $binaryTemporaryPath);
            if (!is_string($binaryHash)) {
                throw new RuntimeException('Cannot hash the extracted Queen supervisor binary.');
            }
            $receipt = [
                'schema_version' => 1,
                'version' => SupervisorBinary::VERSION,
                'target' => $platform['target'],
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

            if (!rename($binaryTemporaryPath, $binaryPath)) {
                throw new RuntimeException('Cannot atomically publish the Queen supervisor binary.');
            }
            $binaryTemporaryPath = null;
            if (!rename($receiptTemporaryPath, $receiptPath)) {
                throw new RuntimeException('Cannot atomically publish the Queen supervisor installation receipt.');
            }
            $receiptTemporaryPath = null;

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

    private function verifyExecutableVersion(string $binary): void
    {
        if (!function_exists('proc_open')) {
            throw new RuntimeException('proc_open is required to verify the downloaded Queen supervisor.');
        }
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
    }

    private function prepareInstallBase(string $path): string
    {
        if (trim($path) === '' || str_contains($path, "\0")) {
            throw new RuntimeException('The Queen supervisor install path is invalid.');
        }
        $this->ensureDirectory($path);
        if (is_link($path)) {
            throw new RuntimeException('The Queen supervisor install path must not be a symbolic link.');
        }
        $real = realpath($path);
        if ($real === false || !is_dir($real) || !is_writable($real)) {
            throw new RuntimeException("Queen supervisor install path {$path} is not writable.");
        }

        return $real;
    }

    private function ensureDirectory(string $path): void
    {
        if (!is_dir($path) && !@mkdir($path, 0755, true) && !is_dir($path)) {
            throw new RuntimeException("Cannot create Queen supervisor directory {$path}.");
        }
        if (is_link($path)) {
            throw new RuntimeException("Queen supervisor directory {$path} must not be a symbolic link.");
        }
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
        $path = tempnam($directory, $prefix);
        if ($path === false) {
            throw new RuntimeException("Cannot create a temporary file in {$directory}.");
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
