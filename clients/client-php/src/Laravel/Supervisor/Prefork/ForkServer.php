<?php

namespace Queen\Laravel\Supervisor\Prefork;

/**
 * The forking half of prefork workers, run by `queen:fork-server`.
 *
 * A supervisor master starts one fork server. The server boots Laravel once,
 * opens no queue, database or cache connection, and then forks one child per
 * worker the master asks for. Every child shares the booted framework (and
 * the opcache) copy-on-write, sets up its own worker environment and becomes
 * `queue:work` with the same arguments a spawned worker gets.
 *
 * Protocol (JSON, one object per line):
 *
 *   master -> server (stdin)  {"fork": {"id": 7, "argv": ["queen", "--queue=high", ...], "env": {"NAME": "value" | null}}}
 *   server -> master (fd 3)   {"ready": "queen.fork-server/v1", "pid": 42}
 *                             {"forked": 7, "pid": 43} | {"failed": 7, "error": "..."}
 *                             {"exited": 43, "status": <raw waitpid status>}
 *
 * Every child leads its own session, so the master signals its process group
 * exactly as it signals a spawned worker. The server is a fence: when its
 * master is gone, it SIGKILLs every worker it forked before it exits, so a
 * crashed master never leaves workers behind for the next generation. It
 * learns that from stdin closing, or from a SIGTERM that arrives after it was
 * reparented (the Rust master's parent-death signal). A SIGTERM while the
 * master lives, such as systemd signalling the whole unit, and SIGINT from a
 * terminal are not a crash: the master drains the workers itself and then
 * closes stdin.
 *
 * The Rust engine speaks the same protocol (supervisor/src/prefork.rs).
 */
final class ForkServer
{
    public const PROTOCOL = 'queen.fork-server/v1';

    private const MAX_LINE_BYTES = 1048576;

    /** @var array<int, true> */
    private array $children = [];

    private bool $stopping = false;

    private int $master = 0;

    /**
     * @param resource $commands the master's requests (stdin)
     * @param resource $events the replies to the master (fd 3)
     * @param \Closure(list<string>): int $runWorker runs queue:work in a forked child
     * @param (\Closure(): void)|null $afterFork resets state a child must not share
     */
    public function __construct(
        private mixed $commands,
        private mixed $events,
        private \Closure $runWorker,
        private ?\Closure $afterFork = null,
    ) {
    }

    public function serve(): int
    {
        $this->master = posix_getppid();
        // Out of the master's process group: a terminal's Ctrl-C is for the
        // master, which drains the workers.
        @posix_setpgid(0, 0);
        pcntl_async_signals(true);
        pcntl_signal(SIGINT, SIG_IGN);
        pcntl_signal(SIGTERM, function (): void {
            if (posix_getppid() !== $this->master) {
                $this->stopping = true;
            }
        });
        stream_set_blocking($this->commands, false);
        $this->send(['ready' => self::PROTOCOL, 'pid' => getmypid()]);

        // The fence holds whatever ends the loop. A forked child never runs
        // this: it leaves through exit(), which runs no finally block.
        try {
            $this->loop();
        } finally {
            $this->killChildren();
        }

        return 0;
    }

    private function loop(): void
    {
        $buffer = '';
        while (!$this->stopping) {
            $this->reap();
            $read = [$this->commands];
            $write = null;
            $except = null;
            $ready = @stream_select($read, $write, $except, 0, 200_000);
            if ($ready === false || $ready === 0) {
                // Interrupted by a signal, or idle.
                continue;
            }
            $chunk = fread($this->commands, 65536);
            if ($chunk === false || $chunk === '') {
                if (feof($this->commands)) {
                    break;
                }
                continue;
            }
            $buffer .= $chunk;
            while (($newline = strpos($buffer, "\n")) !== false) {
                $line = substr($buffer, 0, $newline);
                $buffer = substr($buffer, $newline + 1);
                $this->handle($line);
            }
            if (strlen($buffer) > self::MAX_LINE_BYTES) {
                break;
            }
        }
    }

    private function handle(string $line): void
    {
        $request = json_decode($line, true);
        $fork = is_array($request) ? ($request['fork'] ?? null) : null;
        $id = is_array($fork) ? ($fork['id'] ?? null) : null;
        $argv = is_array($fork) ? ($fork['argv'] ?? null) : null;
        $environment = is_array($fork) ? ($fork['env'] ?? []) : null;
        if (!is_int($id)
            || !is_array($argv) || !array_is_list($argv)
            || array_filter($argv, fn ($argument) => !is_string($argument)) !== []
            || !is_array($environment)
            || array_filter($environment, fn ($value) => $value !== null && !is_string($value)) !== []) {
            $this->send(['failed' => is_int($id) ? $id : null, 'error' => 'malformed fork request']);

            return;
        }

        // Blocked across the fork: a SIGTERM for the new worker must not reach
        // the server's handler, which the child inherits until it resets it.
        pcntl_sigprocmask(SIG_BLOCK, [SIGTERM, SIGINT], $mask);
        // A failed fork (a pids or nproc limit, no memory) raises a warning,
        // which Laravel's error handler throws: it is a failed request, -1
        // below, never the end of the server.
        $pid = @pcntl_fork();
        if ($pid === 0) {
            $this->becomeWorker($argv, $environment, $mask);
        }
        pcntl_sigprocmask(SIG_SETMASK, $mask);
        if ($pid === -1) {
            $this->send(['failed' => $id, 'error' => 'fork failed: ' . pcntl_strerror(pcntl_get_last_error())]);

            return;
        }
        $this->children[$pid] = true;
        $this->send(['forked' => $id, 'pid' => $pid]);
    }

    /**
     * @param list<string> $argv
     * @param array<string, ?string> $environment
     * @param list<int> $mask the signal mask before the fork
     */
    private function becomeWorker(array $argv, array $environment, array $mask): never
    {
        $code = 1;
        try {
            foreach ([SIGTERM, SIGINT] as $signal) {
                pcntl_signal($signal, SIG_DFL);
            }
            pcntl_sigprocmask(SIG_SETMASK, $mask);
            if (posix_setsid() < 0) {
                throw new \RuntimeException('the worker could not lead its own session');
            }
            // Show the command line a spawned worker has, so ps, top and
            // process monitors see a queue:work worker, not the fork server.
            if (function_exists('cli_set_process_title')) {
                @cli_set_process_title(implode(' ', [PHP_BINARY, $_SERVER['argv'][0] ?? 'artisan', 'queue:work', ...$argv]));
            }
            // The events pipe does not belong to the worker. STDIN stays
            // open because console code probes it (stream_isatty); the
            // worker never reads it, and the server still sees EOF when the
            // master closes its end.
            fclose($this->events);
            foreach ($environment as $name => $value) {
                putenv($value === null ? (string) $name : "{$name}={$value}");
            }
            mt_srand();
            if ($this->afterFork !== null) {
                ($this->afterFork)();
            }
            $code = ($this->runWorker)($argv);
        } catch (\Throwable $error) {
            fwrite(STDERR, "Queen preforked worker failed: {$error->getMessage()}\n");
        }

        exit($code);
    }

    private function reap(): void
    {
        while (($pid = pcntl_waitpid(-1, $status, WNOHANG)) > 0) {
            unset($this->children[$pid]);
            $this->send(['exited' => $pid, 'status' => $status]);
        }
    }

    private function killChildren(): void
    {
        foreach (array_keys($this->children) as $pid) {
            @posix_kill(-$pid, SIGKILL);
            @posix_kill($pid, SIGKILL);
        }
        $deadline = microtime(true) + 5;
        while ($this->children !== [] && microtime(true) < $deadline) {
            $this->reap();
            usleep(20_000);
        }
    }

    /** @param array<string, mixed> $event */
    private function send(array $event): void
    {
        @fwrite($this->events, json_encode($event, JSON_UNESCAPED_SLASHES) . "\n");
    }
}
