<?php

namespace Queen\Laravel\Supervisor\Prefork;

/**
 * The master's side of a fork server (see ForkServer): starts it, asks it for
 * workers and collects their exits.
 */
final class ForkServerClient
{
    private const MAX_EVENT_BYTES = 1048576;

    /** @var resource */
    private mixed $process;

    /** @var resource */
    private mixed $commands;

    /** @var resource */
    private mixed $events;

    private string $buffer = '';

    private int $nextId = 1;

    /** @var array<int, int> worker pid => raw wait status */
    private array $exits = [];

    /** @var list<array<string, mixed>> */
    private array $pending = [];

    /** @var array<int, true> fork requests the master stopped waiting for */
    private array $abandoned = [];

    /** @var array<int, true> workers forked for an abandoned request */
    private array $strays = [];

    private int $serverPid = 0;

    private function __construct()
    {
    }

    /**
     * @param list<string> $command how to run `queen:fork-server`
     * @param array<string, string> $environment the server's whole environment
     * @param (\Closure(): bool)|null $cancelled stops waiting for the boot, such as on SIGTERM
     */
    public static function start(array $command, string $cwd, array $environment, float $timeoutSeconds, ?\Closure $cancelled = null): self
    {
        $client = new self();
        $process = proc_open(
            $command,
            [0 => ['pipe', 'r'], 1 => STDOUT, 2 => STDERR, 3 => ['pipe', 'w']],
            $pipes,
            $cwd,
            $environment,
        );
        if (!is_resource($process)) {
            throw new \RuntimeException('the fork server could not be started');
        }
        $client->process = $process;
        $client->commands = $pipes[0];
        $client->events = $pipes[3];
        stream_set_blocking($client->events, false);

        try {
            $ready = $client->await(fn (array $event): bool => ($event['ready'] ?? null) === ForkServer::PROTOCOL, $timeoutSeconds, $cancelled);
            $client->serverPid = is_int($ready['pid'] ?? null) ? $ready['pid'] : 0;
        } catch (\Throwable $error) {
            $client->close(0);
            throw $error;
        }

        return $client;
    }

    /**
     * @param list<string> $argv queue:work arguments, the connection first
     * @param array<string, ?string> $environment set, or with null removed, in the worker
     * @return int the worker's pid
     */
    public function fork(array $argv, array $environment, float $timeoutSeconds): int
    {
        $id = $this->nextId++;
        $line = json_encode(['fork' => ['id' => $id, 'argv' => $argv, 'env' => (object) $environment]], JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
        if (@fwrite($this->commands, $line . "\n") !== strlen($line) + 1) {
            throw new \RuntimeException('the fork server is not accepting requests');
        }
        try {
            $reply = $this->await(
                fn (array $event): bool => ($event['forked'] ?? null) === $id || ($event['failed'] ?? null) === $id,
                $timeoutSeconds,
            );
        } catch (\Throwable $error) {
            // A late reply must not leave a worker nobody counts or drains.
            $this->abandoned[$id] = true;
            throw $error;
        }
        if (!is_int($reply['pid'] ?? null) || $reply['pid'] < 1) {
            throw new \RuntimeException('the fork server could not fork: ' . (is_string($reply['error'] ?? null) ? $reply['error'] : 'unknown error'));
        }

        return $reply['pid'];
    }

    /**
     * Read the events available now. A late reply to a request this master
     * gave up on stops its stray only when somebody reads it, and nothing
     * polls a server none of whose workers is tracked.
     */
    public function tick(): void
    {
        $this->poll();
    }

    /** The raw wait status of an exited worker, or null while it runs. */
    public function exitStatus(int $pid): ?int
    {
        $this->poll();

        return $this->exits[$pid] ?? null;
    }

    public function forget(int $pid): void
    {
        unset($this->exits[$pid]);
    }

    public function pid(): int
    {
        return $this->serverPid;
    }

    public function isAlive(): bool
    {
        // A closed server is gone: proc_close() freed its process handle.
        return is_resource($this->process) && proc_get_status($this->process)['running'];
    }

    /**
     * Closing stdin tells the server its master is done: it fences whatever
     * worker is left and exits.
     */
    public function close(float $timeoutSeconds): void
    {
        if (!is_resource($this->process)) {
            return;
        }
        if (is_resource($this->commands)) {
            fclose($this->commands);
        }
        $deadline = microtime(true) + $timeoutSeconds;
        while ($this->isAlive() && microtime(true) < $deadline) {
            usleep(20_000);
        }
        if ($this->isAlive()) {
            proc_terminate($this->process, SIGKILL);
        }
        if (is_resource($this->events)) {
            fclose($this->events);
        }
        proc_close($this->process);
    }

    /** Read every event available now, without blocking. */
    private function poll(): void
    {
        while (is_resource($this->events)) {
            $chunk = @fread($this->events, 65536);
            if ($chunk === false || $chunk === '') {
                break;
            }
            $this->buffer .= $chunk;
            if (strlen($this->buffer) > self::MAX_EVENT_BYTES) {
                throw new \RuntimeException('the fork server sent an oversized event');
            }
        }
        while (($newline = strpos($this->buffer, "\n")) !== false) {
            $event = json_decode(substr($this->buffer, 0, $newline), true);
            $this->buffer = substr($this->buffer, $newline + 1);
            if (!is_array($event)) {
                continue;
            }
            if (is_int($event['exited'] ?? null) && is_int($event['status'] ?? null)) {
                if (isset($this->strays[$event['exited']])) {
                    unset($this->strays[$event['exited']]);
                } else {
                    $this->exits[$event['exited']] = $event['status'];
                }
            } elseif (is_int($event['forked'] ?? null) && isset($this->abandoned[$event['forked']])) {
                unset($this->abandoned[$event['forked']]);
                if (is_int($event['pid'] ?? null) && $event['pid'] > 0) {
                    $this->strays[$event['pid']] = true;
                    @posix_kill(-$event['pid'], SIGTERM);
                    @posix_kill($event['pid'], SIGTERM);
                }
            } elseif (is_int($event['failed'] ?? null) && isset($this->abandoned[$event['failed']])) {
                unset($this->abandoned[$event['failed']]);
            } else {
                $this->pending[] = $event;
            }
        }
    }

    /**
     * @param \Closure(array<string, mixed>): bool $matches
     * @param (\Closure(): bool)|null $cancelled
     * @return array<string, mixed>
     */
    private function await(\Closure $matches, float $timeoutSeconds, ?\Closure $cancelled = null): array
    {
        $deadline = microtime(true) + $timeoutSeconds;
        do {
            $this->poll();
            foreach ($this->pending as $index => $event) {
                if ($matches($event)) {
                    unset($this->pending[$index]);
                    $this->pending = array_values($this->pending);

                    return $event;
                }
            }
            if (!$this->isAlive()) {
                throw new \RuntimeException('the fork server exited');
            }
            if ($cancelled !== null && $cancelled()) {
                throw new \RuntimeException('stopped while waiting for the fork server');
            }
            $read = [$this->events];
            $write = null;
            $except = null;
            @stream_select($read, $write, $except, 0, 50_000);
        } while (microtime(true) < $deadline);

        throw new \RuntimeException('the fork server did not answer in time');
    }
}
