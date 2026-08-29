<?php

namespace Queen\Laravel\Supervisor;

use RuntimeException;

final class SupervisorState
{
    public const STATUS_SCHEMA = 'queen.supervisor.status/v1';

    private const MAX_STATUS_BYTES = 1048576;

    private const MAX_CONTROL_BYTES = 16384;

    private const MAX_OWNER_BYTES = 65536;

    private const DEFAULT_STALE_AFTER_SECONDS = 60;

    private const DEFAULT_CONTROL_TTL_SECONDS = 3600;

    private const CLOCK_SKEW_SECONDS = 5;

    private ?string $instanceId = null;

    /** @var array{dev:int,ino:int,uid:int,mode:int}|null */
    private ?array $generationDirectory = null;

    public function __construct(private string $directory)
    {
        if (!extension_loaded('posix') || !function_exists('posix_geteuid')) {
            throw new RuntimeException('Queen supervisor state requires ext-posix for filesystem ownership checks.');
        }
        $this->assertSafeDirectoryPath();
        $requested = rtrim($this->directory, DIRECTORY_SEPARATOR);
        $visitedLinks = [];
        $this->assertRequestedAncestorChain($requested, $visitedLinks);
        $this->directory = $this->canonicalDirectoryPath($requested);
        $this->assertTrustedAncestorChain();
    }

    /** @return resource */
    public function acquireLock()
    {
        $this->ensureDirectory();
        $candidate = $this->existingDirectoryMetadata()
            ?? throw new RuntimeException("Queen supervisor state directory [{$this->directory}] is unavailable.");
        // Serialize generation takeover with web/CLI control publication. A
        // replacement must never hold supervisor.lock while its file still
        // describes the previous generation, otherwise a request could be
        // acknowledged for an instance that can no longer consume it.
        return $this->withControlLock(function () use ($candidate) {
            $this->assertDirectoryMatches($candidate);
            $handle = $this->openLockFile('supervisor.lock');
            if (!flock($handle, LOCK_EX | LOCK_NB)) {
                fclose($handle);
                throw new RuntimeException("Another Queen supervisor owns [{$this->directory}].");
            }
            @chmod($this->path('supervisor.lock'), 0600);
            $this->instanceId = bin2hex(random_bytes(16));
            $startedAtEpoch = time();
            $document = json_encode([
                'pid' => getmypid(),
                'instance_id' => $this->instanceId,
                'started_at' => gmdate('Y-m-d\TH:i:s\Z', $startedAtEpoch),
                'started_at_epoch' => $startedAtEpoch,
            ], JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
            if (!ftruncate($handle, 0)
                || fwrite($handle, $document) !== strlen($document)
                || !fflush($handle)) {
                flock($handle, LOCK_UN);
                fclose($handle);
                $this->instanceId = null;
                throw new RuntimeException('Unable to publish the Queen supervisor owner state.');
            }
            try {
                $this->assertDirectoryMatches($candidate);
            } catch (\Throwable $error) {
                flock($handle, LOCK_UN);
                fclose($handle);
                $this->instanceId = null;
                throw $error;
            }
            $this->generationDirectory = $candidate;

            return $handle;
        });
    }

    public function instanceId(): string
    {
        return $this->instanceId ?? throw new RuntimeException('Queen supervisor state lock has not been acquired.');
    }

    public function request(
        string $command,
        string $expectedInstanceId,
        ?int $staleAfterSeconds = null,
        ?int $controlTtlSeconds = null,
    ): string
    {
        if (!in_array($command, ['pause', 'continue', 'terminate'], true)) {
            throw new RuntimeException("Unknown Queen supervisor command [{$command}].");
        }
        $this->assertInstanceId($expectedInstanceId);
        if ($staleAfterSeconds !== null && $staleAfterSeconds < 1) {
            throw new RuntimeException('Queen supervisor stale-after must be a positive number of seconds.');
        }
        if ($controlTtlSeconds !== null && ($controlTtlSeconds < 30 || $controlTtlSeconds > 86400)) {
            throw new RuntimeException('Queen supervisor control TTL must be between 30 and 86400 seconds.');
        }

        $nonce = bin2hex(random_bytes(16));
        $this->withControlLock(function () use (
            $command,
            $nonce,
            $expectedInstanceId,
            $staleAfterSeconds,
            $controlTtlSeconds,
        ): void {
            $status = $this->assertCurrentInstance($expectedInstanceId, $staleAfterSeconds);
            $timing = $this->statusTiming($status);
            $effectiveControlTtlSeconds = $controlTtlSeconds ?? $timing['control_ttl'];
            if (@lstat($this->path('control.json')) !== false) {
                throw new RuntimeException(
                    'A Queen supervisor command is already pending; wait for it to be consumed.',
                );
            }
            $requestedAtEpoch = time();
            $this->writeJson('control.json', [
                'command' => $command,
                'nonce' => $nonce,
                'instance_id' => $expectedInstanceId,
                'requested_at' => gmdate('Y-m-d\TH:i:s\Z', $requestedAtEpoch),
                'requested_at_epoch' => $requestedAtEpoch,
                'expires_at_epoch' => $requestedAtEpoch + $effectiveControlTtlSeconds,
            ]);
        });
        return $nonce;
    }

    public function command(?string $lastNonce, string $instanceId): ?array
    {
        $this->assertInstanceId($instanceId);

        return $this->withControlLock(function () use ($lastNonce, $instanceId): ?array {
            try {
                $control = $this->readJson('control.json');
            } catch (\Throwable) {
                $this->discardInvalidControl();

                return null;
            }
            if ($control === null) {
                // readJson() returns null for malformed JSON as well as a
                // missing file. Under control.lock no conforming writer can
                // race this check, so remove poison documents fail-closed.
                $this->discardInvalidControl();

                return null;
            }
            if (!unlink($this->path('control.json'))) {
                throw new RuntimeException('Unable to consume the Queen supervisor command.');
            }
            if (
                !is_string($control['nonce'] ?? null)
                || !in_array($control['command'] ?? null, ['pause', 'continue', 'terminate'], true)
                || ($control['nonce'] ?? null) === $lastNonce
                || ($control['instance_id'] ?? null) !== $instanceId
                || !is_int($control['requested_at_epoch'] ?? null)
                || !is_int($control['expires_at_epoch'] ?? null)
                || $control['requested_at_epoch'] > time() + self::CLOCK_SKEW_SECONDS
                || $control['expires_at_epoch'] < $control['requested_at_epoch']
                || $control['expires_at_epoch'] < time()
            ) {
                return null;
            }
            return $control;
        });
    }

    private function discardInvalidControl(): void
    {
        $this->ensureDirectory();
        $path = $this->path('control.json');
        if (@lstat($path) !== false && !@unlink($path)) {
            throw new RuntimeException('Unable to discard an invalid Queen supervisor command.');
        }
    }

    public function isOwned(): bool
    {
        return $this->lockedOwner() !== null;
    }

    public function isOwnedBy(string $instanceId): bool
    {
        $this->assertInstanceId($instanceId);
        $owner = $this->lockedOwner();

        return ($owner['instance_id'] ?? null) === $instanceId;
    }

    public function isLive(array $status, ?int $staleAfterSeconds = null): bool
    {
        if ($staleAfterSeconds !== null && $staleAfterSeconds < 1) {
            throw new RuntimeException('Queen supervisor stale-after must be a positive number of seconds.');
        }
        try {
            // Timing belongs to the process generation that published this
            // status document. Laravel configuration may already describe a
            // later deployment and must not redefine liveness retroactively.
            $timing = $this->statusTiming($status);
        } catch (RuntimeException) {
            return false;
        }
        $effectiveStaleAfterSeconds = $staleAfterSeconds ?? $timing['heartbeat_timeout'];
        $instanceId = $status['instance_id'] ?? null;
        $pid = $status['pid'] ?? null;
        $updatedAtEpoch = $status['updated_at_epoch'] ?? null;
        $now = time();
        if (($status['schema'] ?? null) !== self::STATUS_SCHEMA
            || !is_string($instanceId)
            || $instanceId === ''
            || !is_int($pid)
            || $pid < 1
            || !is_int($updatedAtEpoch)
            || $updatedAtEpoch < 1
            || $updatedAtEpoch > $now + self::CLOCK_SKEW_SECONDS
            || $now - $updatedAtEpoch > $effectiveStaleAfterSeconds
            || !in_array($status['state'] ?? null, ['running', 'paused'], true)
            || !is_bool($status['paused'] ?? null)
            || $status['paused'] !== (($status['state'] ?? null) === 'paused')
            || ($status['stopping'] ?? null) !== false) {
            return false;
        }
        $owner = $this->lockedOwner();

        return ($owner['instance_id'] ?? null) === $instanceId
            && ($owner['pid'] ?? null) === $pid;
    }

    public function writeStatus(array $status): void
    {
        $updatedAtEpoch = time();
        $state = is_string($status['state'] ?? null) ? $status['state'] : 'unknown';
        $metadata = [
            'schema' => self::STATUS_SCHEMA,
            'updated_at' => gmdate('Y-m-d\TH:i:s\Z', $updatedAtEpoch),
            'updated_at_epoch' => $updatedAtEpoch,
            'pid' => getmypid(),
            'instance_id' => $this->instanceId,
            'paused' => $state === 'paused',
            'stopping' => $state === 'terminating',
        ];
        $this->writeJson('status.json', array_replace($status, $metadata));
    }

    public function status(): ?array
    {
        return $this->readJson('status.json');
    }

    public function telemetryDirectory(): string
    {
        $this->ensureDirectory();
        $path = $this->path('telemetry');
        if (!is_dir($path) && !@mkdir($path, 0700) && !is_dir($path)) {
            throw new RuntimeException("Unable to create Queen telemetry directory [{$path}].");
        }
        $metadata = @lstat($path);
        if ($metadata === false || ($metadata['mode'] & 0170000) !== 0040000) {
            throw new RuntimeException("Queen telemetry directory [{$path}] must not be a symbolic link.");
        }
        $this->makePrivate($path);
        return $path;
    }

    public function removeTelemetryForPid(int $pid): void
    {
        if ($pid < 1) {
            return;
        }

        $directory = $this->existingDirectoryMetadata();
        if ($directory === null) {
            return;
        }
        $telemetry = $this->path('telemetry');
        $telemetryMetadata = @lstat($telemetry);
        if (!is_array($telemetryMetadata)
            || ($telemetryMetadata['mode'] & 0170000) !== 0040000
            || ($telemetryMetadata['mode'] & 07777) !== 0700
            || ($telemetryMetadata['uid'] ?? null) !== ($directory['uid'] ?? null)) {
            return;
        }

        $path = $telemetry . DIRECTORY_SEPARATOR . $pid . '.json';
        $metadata = @lstat($path);
        if (!is_array($metadata)
            || ($metadata['mode'] & 0170000) !== 0100000
            || ($metadata['uid'] ?? null) !== ($telemetryMetadata['uid'] ?? null)) {
            return;
        }
        $current = @lstat($path);
        if (is_array($current)
            && ($current['mode'] & 0170000) === 0100000
            && $current['dev'] === $metadata['dev']
            && $current['ino'] === $metadata['ino']) {
            @unlink($path);
        }
    }

    private function readJson(string $file): ?array
    {
        $directory = $this->existingDirectoryMetadata();
        if ($directory === null) {
            return null;
        }
        $path = $this->path($file);
        if (@lstat($path) === false) {
            return null;
        }
        $maximumBytes = $this->maximumBytes($file);
        $handle = @fopen($path, 'rb');
        if ($handle === false) {
            throw new RuntimeException("Unable to open Queen supervisor state [{$path}].");
        }
        $metadata = @lstat($path);
        $opened = fstat($handle);
        if ($metadata === false
            || $opened === false
            || ($metadata['mode'] & 0170000) !== 0100000
            || ($opened['mode'] & 0170000) !== 0100000
            || $metadata['dev'] !== $opened['dev']
            || $metadata['ino'] !== $opened['ino']
            || ($opened['mode'] & 07777) !== 0600
            || ($directory['uid'] ?? null) !== ($opened['uid'] ?? null)
            || $opened['size'] < 1
            || $opened['size'] > $maximumBytes) {
            fclose($handle);
            throw new RuntimeException("Queen supervisor state [{$path}] must be a bounded regular file.");
        }
        $contents = stream_get_contents($handle, $maximumBytes + 1);
        fclose($handle);
        if (!is_string($contents) || $contents === '' || strlen($contents) > $maximumBytes) {
            throw new RuntimeException("Unable to read bounded Queen supervisor state [{$path}].");
        }
        $decoded = json_decode($contents, true, 32);
        return is_array($decoded) ? $decoded : null;
    }

    private function withControlLock(\Closure $operation): mixed
    {
        $this->ensureDirectory();
        $handle = $this->openLockFile('control.lock');
        if (!flock($handle, LOCK_EX)) {
            fclose($handle);
            throw new RuntimeException('Unable to acquire the Queen supervisor control lock.');
        }
        @chmod($this->path('control.lock'), 0600);
        try {
            // Pin the path again after acquiring the filesystem lock. This
            // prevents a path replacement from redirecting the operation to
            // a different generation while it waited for control.lock.
            $this->ensureDirectory();

            return $operation();
        } finally {
            flock($handle, LOCK_UN);
            fclose($handle);
        }
    }

    private function writeJson(string $file, array $value): void
    {
        $this->ensureDirectory();
        $path = $this->path($file);
        $temporary = tempnam($this->directory, '.queen-');
        if ($temporary === false) {
            throw new RuntimeException("Unable to create a temporary state file in [{$this->directory}].");
        }
        try {
            $json = json_encode($value, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
            if (strlen($json) > $this->maximumBytes($file)) {
                throw new RuntimeException("Queen supervisor state [{$path}] exceeds its size limit.");
            }
            if (file_put_contents($temporary, $json, LOCK_EX) !== strlen($json)
                || !chmod($temporary, 0600)
                || !rename($temporary, $path)) {
                throw new RuntimeException("Unable to publish Queen supervisor state [{$path}].");
            }
            $this->ensureDirectory();
        } catch (\Throwable $error) {
            @unlink($temporary);
            throw $error;
        }
    }

    private function ensureDirectory(): void
    {
        if ($this->generationDirectory !== null) {
            $this->assertDirectoryMatches($this->generationDirectory);

            return;
        }

        $this->assertTrustedAncestorChain();
        $created = false;
        if (!is_dir($this->directory)) {
            // Only the final component may be absent. Creating a hierarchy in
            // one recursive call could cross an untrusted writable ancestor.
            if (@mkdir($this->directory, 0700)) {
                $created = true;
            } elseif (!is_dir($this->directory)) {
                throw new RuntimeException("Unable to create Queen supervisor state directory [{$this->directory}].");
            }
        }
        $metadata = @lstat($this->directory);
        if ($metadata === false || ($metadata['mode'] & 0170000) !== 0040000) {
            throw new RuntimeException("Queen supervisor state directory [{$this->directory}] must not be a symbolic link.");
        }
        if (($metadata['uid'] ?? null) !== posix_geteuid()) {
            throw new RuntimeException(
                "Queen supervisor state directory [{$this->directory}] must be owned by the current user.",
            );
        }
        if ($created) {
            $this->makePrivate($this->directory);
            // Verify the exact postcondition through the same path used by
            // readers; chmod success alone is not a sufficient safety check.
            if ($this->existingDirectoryMetadata() === null) {
                throw new RuntimeException("Unable to verify Queen supervisor state directory [{$this->directory}].");
            }

            return;
        }
        if (($metadata['mode'] & 07777) !== 0700) {
            // Never repair an existing path: a typo such as /tmp or /etc must
            // fail without changing permissions on an unrelated directory.
            throw new RuntimeException(
                "Existing Queen supervisor state directory [{$this->directory}] must use mode 0700.",
            );
        }
        $this->assertTrustedAncestorChain();
    }

    private function assertSafeDirectoryPath(): void
    {
        if ($this->directory === ''
            || str_contains($this->directory, "\0")
            || preg_match('/[\x00-\x1F\x7F]/', $this->directory) === 1) {
            throw new RuntimeException('Queen supervisor state directory path is invalid.');
        }
        if (!str_starts_with($this->directory, DIRECTORY_SEPARATOR)) {
            throw new RuntimeException('Queen supervisor state directory must be an absolute path.');
        }
        $components = preg_split('/[\\\\\/]+/', $this->directory, -1, PREG_SPLIT_NO_EMPTY);
        if (!is_array($components) || in_array('..', $components, true)) {
            throw new RuntimeException('Queen supervisor state directory must not contain parent traversal.');
        }
        $meaningful = array_values(array_filter(
            $components,
            static fn (string $component): bool => $component !== '.',
        ));
        if ($meaningful === []) {
            throw new RuntimeException('Queen supervisor state directory must not be a filesystem root.');
        }
    }

    private function canonicalDirectoryPath(string $requested): string
    {
        if ($requested === '') {
            throw new RuntimeException('Queen supervisor state directory must not be a filesystem root.');
        }

        $leaf = @lstat($requested);
        if (is_array($leaf)) {
            if (($leaf['mode'] & 0170000) === 0120000) {
                throw new RuntimeException('Queen supervisor state directory must not be a symbolic link.');
            }
            $canonical = @realpath($requested);
        } else {
            $parent = dirname($requested);
            $canonicalParent = @realpath($parent);
            $canonical = is_string($canonicalParent)
                ? rtrim($canonicalParent, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . basename($requested)
                : false;
        }

        if (!is_string($canonical) || $canonical === '' || dirname($canonical) === $canonical) {
            throw new RuntimeException('Queen supervisor state directory must not be a filesystem root.');
        }

        return $canonical;
    }

    /**
     * Validate the spelling supplied by the operator before realpath() is
     * allowed to follow it. A symlink entry is safe to resolve only after its
     * containing directory is known not to be replaceable by another user.
     *
     * @param array<string, true> $visitedLinks
     */
    private function assertRequestedAncestorChain(
        string $requested,
        array &$visitedLinks,
        bool $stateLeaf = true,
    ): bool
    {
        $paths = $this->directoryPaths($requested);
        $last = array_key_last($paths);
        $parentWasSticky = false;

        foreach ($paths as $index => $path) {
            clearstatcache(true, $path);
            $metadata = @lstat($path);
            if (!is_array($metadata)) {
                if ($index !== $last || !$stateLeaf) {
                    throw new RuntimeException(
                        "Every parent of Queen supervisor state_directory must already exist [{$path}].",
                    );
                }

                return $parentWasSticky;
            }

            $fileType = $metadata['mode'] & 0170000;
            if ($fileType === 0120000) {
                if ($index === $last && $stateLeaf) {
                    throw new RuntimeException('Queen supervisor state directory must not be a symbolic link.');
                }
                if ($parentWasSticky
                    && ($metadata['uid'] ?? null) !== 0
                    && ($metadata['uid'] ?? null) !== posix_geteuid()) {
                    throw new RuntimeException(
                        "Queen supervisor state child [{$path}] below a sticky directory must be owned by the current user.",
                    );
                }
                $identity = ($metadata['dev'] ?? '?') . ':' . ($metadata['ino'] ?? '?');
                if (isset($visitedLinks[$identity])) {
                    throw new RuntimeException("Queen supervisor state ancestor [{$path}] contains a symbolic-link loop.");
                }
                $visitedLinks[$identity] = true;
                $link = @readlink($path);
                if (!is_string($link) || $link === '') {
                    throw new RuntimeException("Unable to resolve Queen supervisor state ancestor [{$path}].");
                }
                $target = str_starts_with($link, DIRECTORY_SEPARATOR)
                    ? $link
                    : dirname($path) . DIRECTORY_SEPARATOR . $link;
                $target = $this->normalizeAbsolutePath($target);
                $parentWasSticky = $this->assertRequestedAncestorChain(
                    $target,
                    $visitedLinks,
                    false,
                );
                unset($visitedLinks[$identity]);

                continue;
            }
            if ($fileType !== 0040000) {
                throw new RuntimeException("Queen supervisor state ancestor [{$path}] must be a directory.");
            }
            $parentWasSticky = $this->assertTrustedDirectory(
                $path,
                $metadata,
                $stateLeaf && $index === $last,
                $parentWasSticky,
            );
        }

        return $parentWasSticky;
    }

    private function assertTrustedAncestorChain(): void
    {
        $parentWasSticky = false;
        $paths = $this->directoryPaths($this->directory);
        $last = array_key_last($paths);

        foreach ($paths as $index => $path) {
            clearstatcache(true, $path);
            $metadata = @lstat($path);
            if (!is_array($metadata)) {
                if ($index !== $last) {
                    throw new RuntimeException(
                        "Every parent of Queen supervisor state_directory must already exist [{$path}].",
                    );
                }

                break;
            }
            if (($metadata['mode'] & 0170000) !== 0040000) {
                throw new RuntimeException("Queen supervisor state ancestor [{$path}] must be a real directory.");
            }
            $parentWasSticky = $this->assertTrustedDirectory(
                $path,
                $metadata,
                $index === $last,
                $parentWasSticky,
            );
        }
    }

    /** @param array<string, mixed> $metadata */
    private function assertTrustedDirectory(
        string $path,
        array $metadata,
        bool $stateLeaf,
        bool $parentWasSticky,
    ): bool {
        $effectiveUid = posix_geteuid();
        $owner = $metadata['uid'] ?? null;
        if ($parentWasSticky && $owner !== 0 && $owner !== $effectiveUid) {
            throw new RuntimeException(
                "Queen supervisor state child [{$path}] below a sticky directory must be owned by the current user.",
            );
        }
        // A foreign owner can chmod and then rename children even when the
        // current mode is 0555. Only root and the effective supervisor user
        // are trusted owners for path components.
        if ($owner !== 0 && $owner !== $effectiveUid) {
            throw new RuntimeException(
                "Queen supervisor state ancestor [{$path}] must be owned by root or the current user.",
            );
        }

        $mode = $metadata['mode'] ?? 0;
        $sticky = ($mode & 01000) !== 0;
        if (!$stateLeaf && ($mode & 0022) !== 0 && !$sticky) {
            throw new RuntimeException(
                "Queen supervisor state ancestor [{$path}] must not be group/world-writable unless it is a trusted sticky directory.",
            );
        }

        return $sticky;
    }

    /** @return list<string> */
    private function directoryPaths(string $directory): array
    {
        $components = preg_split('/\/+/', trim($directory, DIRECTORY_SEPARATOR), -1, PREG_SPLIT_NO_EMPTY);
        if (!is_array($components)) {
            throw new RuntimeException('Queen supervisor state directory path is invalid.');
        }

        $paths = [DIRECTORY_SEPARATOR];
        $current = DIRECTORY_SEPARATOR;
        foreach ($components as $component) {
            if ($component === '.') {
                continue;
            }
            $current = rtrim($current, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $component;
            $paths[] = $current;
        }

        return $paths;
    }

    private function normalizeAbsolutePath(string $path): string
    {
        if (!str_starts_with($path, DIRECTORY_SEPARATOR)) {
            throw new RuntimeException('Queen supervisor symbolic-link target must resolve to an absolute path.');
        }
        $components = preg_split('/\/+/', $path, -1, PREG_SPLIT_NO_EMPTY);
        if (!is_array($components)) {
            throw new RuntimeException('Queen supervisor state directory path is invalid.');
        }
        $normalized = [];
        foreach ($components as $component) {
            if ($component === '.') {
                continue;
            }
            if ($component === '..') {
                array_pop($normalized);

                continue;
            }
            $normalized[] = $component;
        }

        return DIRECTORY_SEPARATOR . implode(DIRECTORY_SEPARATOR, $normalized);
    }

    private function existingDirectoryMetadata(): ?array
    {
        $this->assertTrustedAncestorChain();
        $metadata = @lstat($this->directory);
        if ($metadata === false) {
            if ($this->generationDirectory !== null) {
                throw new RuntimeException('Queen supervisor state directory changed after generation acquisition.');
            }

            return null;
        }
        if (($metadata['mode'] & 0170000) !== 0040000 || ($metadata['mode'] & 07777) !== 0700) {
            throw new RuntimeException(
                "Queen supervisor state directory [{$this->directory}] must be a private real directory.",
            );
        }
        if (($metadata['uid'] ?? null) !== posix_geteuid()) {
            throw new RuntimeException(
                "Queen supervisor state directory [{$this->directory}] must be owned by the current user.",
            );
        }
        if ($this->generationDirectory !== null) {
            $this->assertDirectoryMetadataMatches($metadata, $this->generationDirectory);
        }

        return $metadata;
    }

    /** @param array<string, mixed> $expected */
    private function assertDirectoryMatches(array $expected): void
    {
        $this->assertTrustedAncestorChain();
        clearstatcache(true, $this->directory);
        $current = @lstat($this->directory);
        if (!is_array($current)) {
            throw new RuntimeException('Queen supervisor state directory changed after generation acquisition.');
        }
        $this->assertDirectoryMetadataMatches($current, $expected);
    }

    /**
     * @param array<string, mixed> $current
     * @param array<string, mixed> $expected
     */
    private function assertDirectoryMetadataMatches(array $current, array $expected): void
    {
        if (($current['mode'] & 0170000) !== 0040000
            || ($current['mode'] & 07777) !== 0700
            || ($current['uid'] ?? null) !== posix_geteuid()
            || ($current['dev'] ?? null) !== ($expected['dev'] ?? null)
            || ($current['ino'] ?? null) !== ($expected['ino'] ?? null)) {
            throw new RuntimeException('Queen supervisor state directory changed after generation acquisition.');
        }
    }

    /** @return resource */
    private function openLockFile(string $file)
    {
        $path = $this->path($file);
        $metadata = @lstat($path);
        if ($metadata !== false && ($metadata['mode'] & 0170000) !== 0100000) {
            throw new RuntimeException("Queen supervisor lock [{$path}] must not be a symbolic link.");
        }
        $handle = @fopen($path, 'c+b');
        $current = @lstat($path);
        $opened = is_resource($handle) ? fstat($handle) : false;
        if (!is_resource($handle)
            || $current === false
            || $opened === false
            || ($current['mode'] & 0170000) !== 0100000
            || ($opened['mode'] & 0170000) !== 0100000
            || $current['dev'] !== $opened['dev']
            || $current['ino'] !== $opened['ino']) {
            if (is_resource($handle)) {
                fclose($handle);
            }
            throw new RuntimeException("Unable to open a safe Queen supervisor lock [{$path}].");
        }

        return $handle;
    }

    /** @return resource|null */
    private function openExistingLockFile(string $file)
    {
        $directory = $this->existingDirectoryMetadata();
        if ($directory === null) {
            return null;
        }
        $path = $this->path($file);
        $metadata = @lstat($path);
        if ($metadata === false) {
            return null;
        }
        if (($metadata['mode'] & 0170000) !== 0100000
            || ($metadata['mode'] & 07777) !== 0600
            || ($directory['uid'] ?? null) !== ($metadata['uid'] ?? null)) {
            throw new RuntimeException("Queen supervisor lock [{$path}] must be a private owned regular file.");
        }
        $handle = @fopen($path, 'r+b');
        $current = @lstat($path);
        $opened = is_resource($handle) ? fstat($handle) : false;
        if (!is_resource($handle)
            || $current === false
            || $opened === false
            || ($current['mode'] & 0170000) !== 0100000
            || ($opened['mode'] & 0170000) !== 0100000
            || ($opened['mode'] & 07777) !== 0600
            || ($directory['uid'] ?? null) !== ($opened['uid'] ?? null)
            || $current['dev'] !== $opened['dev']
            || $current['ino'] !== $opened['ino']
            || $opened['size'] < 1
            || $opened['size'] > self::MAX_OWNER_BYTES) {
            if (is_resource($handle)) {
                fclose($handle);
            }
            throw new RuntimeException("Unable to open a safe existing Queen supervisor lock [{$path}].");
        }

        return $handle;
    }

    private function lockedOwner(): ?array
    {
        if ($this->existingDirectoryMetadata() === null
            || @lstat($this->path('supervisor.lock')) === false) {
            return null;
        }
        $handle = $this->openExistingLockFile('supervisor.lock');
        if ($handle === null) {
            return null;
        }
        $wouldBlock = 0;
        $available = flock($handle, LOCK_EX | LOCK_NB, $wouldBlock);
        if ($available) {
            flock($handle, LOCK_UN);
            fclose($handle);

            return null;
        }
        if ($wouldBlock !== 1) {
            fclose($handle);
            throw new RuntimeException('Unable to verify the Queen supervisor owner lock.');
        }
        if (!rewind($handle)) {
            fclose($handle);
            throw new RuntimeException('Unable to inspect the Queen supervisor owner lock.');
        }
        $contents = stream_get_contents($handle, self::MAX_OWNER_BYTES + 1);
        fclose($handle);
        if (!is_string($contents) || $contents === '' || strlen($contents) > self::MAX_OWNER_BYTES) {
            throw new RuntimeException('Unable to read the bounded Queen supervisor owner lock.');
        }
        $owner = json_decode($contents, true, 8);
        if (!is_array($owner)) {
            throw new RuntimeException('The Queen supervisor owner lock is malformed.');
        }

        return $owner;
    }

    /** @return array<string, mixed> */
    private function assertCurrentInstance(string $expectedInstanceId, ?int $staleAfterSeconds): array
    {
        $status = $this->readJson('status.json');
        if ($status === null
            || ($status['instance_id'] ?? null) !== $expectedInstanceId
            || !$this->isLive($status, $staleAfterSeconds)) {
            throw new RuntimeException(
                'Queen supervisor state is missing, stale, stopping, or belongs to another instance.',
            );
        }

        return $status;
    }

    /** @return array{heartbeat_timeout: int, control_ttl: int} */
    private function statusTiming(array $status): array
    {
        if (!array_key_exists('configuration', $status)) {
            // Compatibility with status/v1 documents emitted before the
            // generation snapshot was added. New documents must publish both
            // values and malformed snapshots fail closed.
            return [
                'heartbeat_timeout' => self::DEFAULT_STALE_AFTER_SECONDS,
                'control_ttl' => self::DEFAULT_CONTROL_TTL_SECONDS,
            ];
        }

        $configuration = $status['configuration'];
        $heartbeatTimeout = is_array($configuration)
            ? ($configuration['heartbeat_timeout'] ?? null)
            : null;
        $controlTtl = is_array($configuration)
            ? ($configuration['control_ttl'] ?? null)
            : null;
        if (!is_int($heartbeatTimeout)
            || $heartbeatTimeout < 1
            || $heartbeatTimeout > 86400
            || !is_int($controlTtl)
            || $controlTtl < 30
            || $controlTtl > 86400) {
            throw new RuntimeException('Queen supervisor generation timing is malformed.');
        }

        return [
            'heartbeat_timeout' => $heartbeatTimeout,
            'control_ttl' => $controlTtl,
        ];
    }

    private function assertInstanceId(string $instanceId): void
    {
        if ($instanceId === ''
            || strlen($instanceId) > 128
            || preg_match('/[\x00-\x1F\x7F]/', $instanceId) === 1) {
            throw new RuntimeException('Queen supervisor instance ID is invalid.');
        }
    }

    private function makePrivate(string $directory): void
    {
        @chmod($directory, 0700);
        clearstatcache(true, $directory);
        $permissions = fileperms($directory);
        if ($permissions === false || ($permissions & 0077) !== 0) {
            throw new RuntimeException("Queen supervisor directory [{$directory}] must have mode 0700.");
        }
    }

    private function path(string $file): string
    {
        return rtrim($this->directory, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $file;
    }

    private function maximumBytes(string $file): int
    {
        return match ($file) {
            'control.json' => self::MAX_CONTROL_BYTES,
            'supervisor.lock' => self::MAX_OWNER_BYTES,
            default => self::MAX_STATUS_BYTES,
        };
    }
}
