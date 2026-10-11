<?php

namespace Queen\Laravel\Supervisor;

use Illuminate\Contracts\Container\Container;
use Illuminate\Queue\Events\Looping;
use Illuminate\Queue\Events\WorkerStopping;

/**
 * Makes a supervised worker that is alive but consumes nothing visible.
 *
 * Laravel's worker catches what pop() throws, reports it, sleeps a second and
 * pops again, for as long as it lives. A worker whose every pop fails stays
 * alive, and a master that watches its process counts it as capacity: with
 * php-client 2.3.1 forked workers popped from an application's default
 * connection that only dispatched, threw on every pop for as long as they
 * ran, and the supervisor said ready and at full capacity throughout.
 *
 * Two kinds of failure are told apart.
 *
 * One that no retry can fix: the worker loop works another connection than
 * its pool's (QUEEN_LARAVEL_CONNECTION), or a pop on the pool's connection
 * throws a LogicException (InvalidArgumentException included). The worker
 * throws WorkerCannotConsume from its next loop and exits 1, which both
 * engines count as a crash: the pool's restart_failures rise, its restarts
 * back off, and its circuit opens.
 *
 * Any other: the broker is down, slow or refusing. A restart cannot help, and
 * restarting every worker of every pool would turn a short outage into a
 * restart storm. The worker stays, and leaves `<pid>.pop-failures` in the
 * master's private exits directory (QUEEN_SUPERVISOR_EXITS_DIR) saying since
 * when its pops fail, from the start of the first that failed. Each queue of
 * the worker's list counts alone: a pop of a queue that succeeds, empty or
 * not, clears that queue's failure, and the file goes once no queue fails.
 * A healthy worker writes nothing.
 * SupervisorState reads the file for the pool's workers: see
 * SupervisorState::NOT_CONSUMING_AFTER_SECONDS.
 */
final class WorkerPopGuard
{
    /** The file of a worker whose pops fail, beside its exit marker. */
    public const FILE_SUFFIX = '.pop-failures';

    /** How often the file is brought up to date while pops keep failing. */
    private const REFRESH_SECONDS = 5;

    private const MAX_ERROR_BYTES = 512;

    private ?\Throwable $fatal = null;

    /**
     * Since when the pops of each failing queue fail, and how often. A
     * worker of --queue=high,low pops both each loop: a pop of high that
     * works says nothing of low.
     *
     * @var array<string, int>
     */
    private array $failingSince = [];

    /** @var array<string, int> */
    private array $failures = [];

    /** @var array<string, \Throwable> */
    private array $errors = [];

    private ?int $popStartedAt = null;

    private ?int $publishedAt = null;

    private \Closure $clock;

    /** @param (\Closure(): int)|null $clock epoch seconds */
    public function __construct(
        private string $supervisor,
        private string $connection,
        private ?WorkerExitMarker $exits = null,
        ?\Closure $clock = null,
    ) {
        $this->clock = $clock ?? static fn (): int => time();
        // A file an earlier process with this pid left says nothing of this one.
        $this->withdraw();
    }

    /** The guard of this worker, or null when no supervisor started it. */
    public static function fromEnvironment(): ?self
    {
        $connection = getenv('QUEEN_LARAVEL_CONNECTION');
        if (!is_string($connection) || $connection === '') {
            return null;
        }
        $supervisor = getenv('QUEEN_LARAVEL_SUPERVISOR');

        return new self(
            is_string($supervisor) && $supervisor !== '' ? $supervisor : 'default',
            $connection,
            WorkerExitMarker::fromEnvironment(),
        );
    }

    /**
     * Under a supervisor: bind the guard, which the pool's QueenQueue picks
     * up, and watch the worker loop. A spawned worker does this at boot, a
     * preforked one right after its fork.
     */
    public static function listenFromEnvironment(Container $app): void
    {
        $guard = self::fromEnvironment();
        if ($guard === null) {
            return;
        }
        $app->instance(self::class, $guard);
        $events = $app->make('events');
        $events->listen(Looping::class, static function (Looping $event) use ($guard): void {
            $guard->looping($event->connectionName);
        });
        $events->listen(WorkerStopping::class, static function () use ($guard): void {
            $guard->withdraw();
        });
    }

    /** Whether this guard watches the pops of $connection: the pool's. */
    public function watches(?string $connection): bool
    {
        return $connection === $this->connection;
    }

    /**
     * Laravel's Looping, before each pop. Throwing here leaves the worker
     * loop, which throwing from pop() never does.
     */
    public function looping(?string $connection): void
    {
        if ($connection !== $this->connection) {
            throw WorkerCannotConsume::wrongConnection($this->supervisor, (string) $connection, $this->connection);
        }
        if ($this->fatal !== null) {
            throw WorkerCannotConsume::popFailed($this->supervisor, $this->connection, $this->fatal);
        }
    }

    /** Before each pop of the pool's connection. */
    public function popping(): void
    {
        $this->popStartedAt = ($this->clock)();
    }

    /** A pop of $queue worked, empty or not. */
    public function popped(?string $queue = null): void
    {
        $this->popStartedAt = null;
        $key = $queue ?? '';
        if (!isset($this->failingSince[$key])) {
            return;
        }
        unset($this->failingSince[$key], $this->failures[$key], $this->errors[$key]);
        if ($this->failingSince === []) {
            $this->withdraw();

            return;
        }
        // Another queue still fails, maybe since later.
        $this->publish(($this->clock)(), end($this->errors));
    }

    public function popFailed(\Throwable $error, ?string $queue = null): void
    {
        $startedAt = $this->popStartedAt;
        $this->popStartedAt = null;
        if ($error instanceof \LogicException) {
            $this->fatal ??= $error;

            return;
        }
        $now = ($this->clock)();
        $key = $queue ?? '';
        // From the start of the pop: one that hung until its timeout, on a
        // broker that stopped answering, consumed nothing from then on.
        $this->failingSince[$key] ??= min($startedAt ?? $now, $now);
        $this->failures[$key] = ($this->failures[$key] ?? 0) + 1;
        // Last in the list: the error the file shows once another recovers.
        unset($this->errors[$key]);
        $this->errors[$key] = $error;
        if ($this->publishedAt === null || $now - $this->publishedAt >= self::REFRESH_SECONDS) {
            $this->publish($now, $error);
        }
    }

    /** Remove this worker's file: its pops work, or it stops. */
    public function withdraw(): void
    {
        $this->publishedAt = null;
        $pid = getmypid();
        if ($this->exits !== null && is_int($pid) && $pid > 0) {
            $this->exits->removeFile($pid . self::FILE_SUFFIX);
        }
    }

    private function publish(int $now, \Throwable $error): void
    {
        $pid = getmypid();
        if ($this->exits === null || !is_int($pid) || $pid < 1) {
            return;
        }
        $message = (string) preg_replace('/[\x00-\x1F\x7F]+/', ' ', get_class($error) . ': ' . $error->getMessage());
        $document = json_encode([
            'pid' => $pid,
            'supervisor' => $this->supervisor,
            'connection' => $this->connection,
            'failing_since' => min($this->failingSince),
            'failures' => array_sum($this->failures),
            'updated_at' => $now,
            'error' => strlen($message) > self::MAX_ERROR_BYTES
                ? substr($message, 0, self::MAX_ERROR_BYTES - 3) . '...'
                : $message,
        ], JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
        if (is_string($document) && $this->exits->writeFile($pid . self::FILE_SUFFIX, $document)) {
            $this->publishedAt = $now;
        }
    }
}
