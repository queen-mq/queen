<?php

namespace Queen\Laravel\Supervisor;

use RuntimeException;

final class SupervisorState
{
    private const MAX_STATUS_BYTES = 1048576;

    private const MAX_CONTROL_BYTES = 65536;

    private ?string $instanceId = null;

    public function __construct(private string $directory)
    {
    }

    /** @return resource */
    public function acquireLock()
    {
        $this->ensureDirectory();
        $handle = fopen($this->path('supervisor.lock'), 'c+');
        if ($handle === false || !flock($handle, LOCK_EX | LOCK_NB)) {
            throw new RuntimeException("Another Queen supervisor owns [{$this->directory}].");
        }
        @chmod($this->path('supervisor.lock'), 0600);
        $this->instanceId = bin2hex(random_bytes(16));
        ftruncate($handle, 0);
        fwrite($handle, json_encode([
            'pid' => getmypid(),
            'instance_id' => $this->instanceId,
            'started_at' => gmdate(DATE_ATOM),
        ], JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR));
        fflush($handle);
        return $handle;
    }

    public function instanceId(): string
    {
        return $this->instanceId ?? throw new RuntimeException('Queen supervisor state lock has not been acquired.');
    }

    public function request(string $command, ?string $expectedInstanceId = null): string
    {
        if (!in_array($command, ['pause', 'continue', 'terminate'], true)) {
            throw new RuntimeException("Unknown Queen supervisor command [{$command}].");
        }
        if ($expectedInstanceId !== null && (
            $expectedInstanceId === ''
            || strlen($expectedInstanceId) > 128
            || preg_match('/[\x00-\x1F\x7F]/', $expectedInstanceId) === 1
        )) {
            throw new RuntimeException('Queen supervisor instance ID is invalid.');
        }

        if ($expectedInstanceId === null) {
            $current = $this->status();
            $candidate = $current['instance_id'] ?? null;
            $expectedInstanceId = is_string($candidate) && $candidate !== '' ? $candidate : null;
        }

        $nonce = bin2hex(random_bytes(16));
        $this->withControlLock(function () use ($command, $nonce, $expectedInstanceId): void {
            if (@lstat($this->path('control.json')) !== false) {
                throw new RuntimeException(
                    'A Queen supervisor command is already pending; wait for it to be consumed.',
                );
            }
            $this->writeJson('control.json', [
                'command' => $command,
                'nonce' => $nonce,
                'instance_id' => $expectedInstanceId,
                'requested_at' => gmdate(DATE_ATOM),
            ]);
        });
        return $nonce;
    }

    public function command(?string $lastNonce, ?string $instanceId = null): ?array
    {
        return $this->withControlLock(function () use ($lastNonce, $instanceId): ?array {
            $control = $this->readJson('control.json');
            if ($control === null) {
                return null;
            }
            if (!unlink($this->path('control.json'))) {
                throw new RuntimeException('Unable to consume the Queen supervisor command.');
            }
            if (
                !is_string($control['nonce'] ?? null)
                || !in_array($control['command'] ?? null, ['pause', 'continue', 'terminate'], true)
                || ($control['nonce'] ?? null) === $lastNonce
                || ($instanceId !== null && ($control['instance_id'] ?? null) !== $instanceId)
            ) {
                return null;
            }
            return $control;
        });
    }

    public function isOwned(): bool
    {
        $this->ensureDirectory();
        $handle = fopen($this->path('supervisor.lock'), 'c+');
        if ($handle === false) {
            return false;
        }
        $available = flock($handle, LOCK_EX | LOCK_NB);
        if ($available) {
            flock($handle, LOCK_UN);
        }
        fclose($handle);
        return !$available;
    }

    public function writeStatus(array $status): void
    {
        $metadata = [
            'updated_at' => gmdate(DATE_ATOM),
            'pid' => getmypid(),
        ];
        if ($this->instanceId !== null) {
            $metadata['instance_id'] = $this->instanceId;
        }
        $this->writeJson('status.json', array_replace($status, $metadata));
    }

    public function status(): ?array
    {
        return $this->readJson('status.json');
    }

    public function telemetryDirectory(): string
    {
        $path = $this->path('telemetry');
        if (!is_dir($path) && !mkdir($path, 0700, true) && !is_dir($path)) {
            throw new RuntimeException("Unable to create Queen telemetry directory [{$path}].");
        }
        $this->makePrivate($path);
        return $path;
    }

    private function readJson(string $file): ?array
    {
        $path = $this->path($file);
        $metadata = @lstat($path);
        if ($metadata === false) {
            return null;
        }
        $maximumBytes = $file === 'control.json' ? self::MAX_CONTROL_BYTES : self::MAX_STATUS_BYTES;
        if (($metadata['mode'] & 0170000) !== 0100000
            || $metadata['size'] < 1
            || $metadata['size'] > $maximumBytes) {
            throw new RuntimeException("Queen supervisor state [{$path}] must be a bounded regular file.");
        }
        $contents = @file_get_contents($path, false, null, 0, $maximumBytes + 1);
        if (!is_string($contents) || $contents === '' || strlen($contents) > $maximumBytes) {
            throw new RuntimeException("Unable to read bounded Queen supervisor state [{$path}].");
        }
        $decoded = json_decode($contents, true, 32);
        return is_array($decoded) ? $decoded : null;
    }

    private function withControlLock(\Closure $operation): mixed
    {
        $this->ensureDirectory();
        $path = $this->path('control.lock');
        if (is_link($path)) {
            throw new RuntimeException('Queen supervisor control lock must not be a symbolic link.');
        }
        $handle = @fopen($path, 'c+b');
        if ($handle === false || !flock($handle, LOCK_EX)) {
            if (is_resource($handle)) {
                fclose($handle);
            }
            throw new RuntimeException('Unable to acquire the Queen supervisor control lock.');
        }
        @chmod($path, 0600);
        try {
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
        file_put_contents($temporary, json_encode($value, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR), LOCK_EX);
        chmod($temporary, 0600);
        if (!rename($temporary, $path)) {
            @unlink($temporary);
            throw new RuntimeException("Unable to publish Queen supervisor state [{$path}].");
        }
    }

    private function ensureDirectory(): void
    {
        if (!is_dir($this->directory) && !mkdir($this->directory, 0700, true) && !is_dir($this->directory)) {
            throw new RuntimeException("Unable to create Queen supervisor state directory [{$this->directory}].");
        }
        $this->makePrivate($this->directory);
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
}
