<?php

namespace Queen\Laravel\Supervisor\Prefork;

use Symfony\Component\Process\Exception\LogicException;
use Symfony\Component\Process\Exception\RuntimeException;
use Symfony\Component\Process\Process;

/**
 * A worker a fork server forked, behind the part of the Symfony Process API
 * the PHP supervisor uses, so a preforked worker is drained, restarted and
 * fenced exactly like a spawned one.
 */
final class ForkedProcess extends Process
{
    private ?int $status = null;

    private bool $constructed = false;

    public function __construct(private ForkServerClient $server, private int $forkedPid)
    {
        parent::__construct(['queen-forked-worker']);
        $this->constructed = true;
    }

    public function isRunning(): bool
    {
        // The Symfony constructor asks before this worker is attached.
        if (!$this->constructed || $this->status !== null) {
            return false;
        }
        $status = $this->server->exitStatus($this->forkedPid);
        if ($status === null && !$this->server->isAlive() && !@posix_kill($this->forkedPid, 0)) {
            // The server is gone and so is the worker: its exit status went
            // with the server.
            $status = 0;
        }
        if ($status === null) {
            return true;
        }
        $this->status = $status;
        $this->server->forget($this->forkedPid);

        return false;
    }

    public function getPid(): ?int
    {
        return $this->isRunning() ? $this->forkedPid : null;
    }

    public function signal(int $signal): static
    {
        if (!$this->isRunning()) {
            throw new LogicException('Cannot send signal on a non running process.');
        }
        if (!posix_kill($this->forkedPid, $signal)) {
            throw new RuntimeException("Error while sending signal \"{$signal}\" to worker {$this->forkedPid}.");
        }

        return $this;
    }

    public function getExitCode(): ?int
    {
        if ($this->isRunning()) {
            return null;
        }
        if (pcntl_wifexited($this->status)) {
            return pcntl_wexitstatus($this->status);
        }

        return pcntl_wifsignaled($this->status) ? 128 + pcntl_wtermsig($this->status) : null;
    }

    public function clearOutput(): static
    {
        return $this;
    }

    public function clearErrorOutput(): static
    {
        return $this;
    }

    public function __destruct()
    {
        // Unlike a Symfony child, a forked worker is never stopped because
        // its handle is released: draining and fencing belong to the master.
    }
}
