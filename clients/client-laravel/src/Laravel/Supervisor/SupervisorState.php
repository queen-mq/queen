<?php

namespace Queen\Laravel\Supervisor;

use RuntimeException;

final class SupervisorState
{
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

    public function request(string $command): string
    {
        if (!in_array($command, ['pause', 'continue', 'terminate'], true)) {
            throw new RuntimeException("Unknown Queen supervisor command [{$command}].");
        }
        $nonce = bin2hex(random_bytes(16));
        $this->writeJson('control.json', [
            'command' => $command,
            'nonce' => $nonce,
            'instance_id' => $this->status()['instance_id'] ?? null,
            'requested_at' => gmdate(DATE_ATOM),
        ]);
        return $nonce;
    }

    public function command(?string $lastNonce, ?string $instanceId = null): ?array
    {
        $control = $this->readJson('control.json');
        if ($control === null) {
            return null;
        }
        @unlink($this->path('control.json'));
        if (
            !is_string($control['nonce'] ?? null)
            || !in_array($control['command'] ?? null, ['pause', 'continue', 'terminate'], true)
            || ($control['nonce'] ?? null) === $lastNonce
            || ($instanceId !== null && ($control['instance_id'] ?? null) !== $instanceId)
        ) {
            return null;
        }
        return $control;
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
        if (!is_file($path)) {
            return null;
        }
        $decoded = json_decode((string) file_get_contents($path), true);
        return is_array($decoded) ? $decoded : null;
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
