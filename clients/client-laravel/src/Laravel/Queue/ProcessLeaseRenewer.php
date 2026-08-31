<?php

namespace Queen\Laravel\Queue;

use RuntimeException;

/**
 * One persistent, clean PHP helper per Laravel queue worker.
 *
 * This is a delivery-path reliability process, not a supervisor/control-plane
 * process. It tracks the single live pop lease owned by Laravel's synchronous
 * worker over a private line-oriented pipe and exits when that pipe reaches
 * EOF.
 */
final class ProcessLeaseRenewer implements LeaseRenewer
{
    /** @var array<int, \WeakReference<self>> Helper PID to owning worker-side renewer. */
    private static array $armedWatchdogs = [];

    private static mixed $previousSigchldHandler = null;

    private static bool $previousAsyncSignals = false;

    private static bool $sigchldHandlerInstalled = false;

    /** @var resource|null */
    private $process = null;

    /** @var array<int, resource> */
    private array $pipes = [];

    /** @var array<string, true> */
    private array $tracked = [];

    /** @var array<string, string> */
    private array $failures = [];

    private string $stderr = '';

    private string $stdoutBuffer = '';

    /** @var list<array> */
    private array $events = [];

    private ?int $watchdogPid = null;

    public function __construct(
        private array $clientConfig,
        private int $leaseSeconds,
        private int $intervalSeconds,
        private int $requestTimeoutSeconds,
        private int $requestBudgetSeconds,
        private int $killGraceSeconds = 2,
        private int $safetyMarginSeconds = 1,
        private string $phpBinary = PHP_BINARY,
        /** @var list<string>|null */
        private ?array $workerCommand = null,
    ) {
        if (!self::isSupported()) {
            throw new RuntimeException(
                'Queen Laravel lease renewal requires proc_open, proc_terminate, pcntl async signals, posix_getppid, posix_kill and posix_setpgid on a Unix CLI worker.',
            );
        }
        if (PHP_SAPI !== 'cli') {
            throw new RuntimeException('Queen Laravel lease renewal is supported only by CLI queue workers.');
        }
        $maximumSeconds = intdiv(PHP_INT_MAX, 1000);
        if ($leaseSeconds < 1 || $intervalSeconds < 1 || $requestTimeoutSeconds < 1
            || $requestBudgetSeconds < $requestTimeoutSeconds || $killGraceSeconds < 0
            || $safetyMarginSeconds < 1 || $leaseSeconds > $maximumSeconds
            || $intervalSeconds > $maximumSeconds || $requestBudgetSeconds > $maximumSeconds
            || $killGraceSeconds > $maximumSeconds || $safetyMarginSeconds > $maximumSeconds) {
            throw new RuntimeException('Queen Laravel lease renewal received invalid timing values.');
        }
        if (!self::sumIsBelow(
            [
                $intervalSeconds,
                $requestBudgetSeconds,
                $requestBudgetSeconds,
                1,
                $killGraceSeconds,
                $safetyMarginSeconds,
            ],
            $leaseSeconds,
        )) {
            throw new RuntimeException('Queen Laravel lease renewal timing budget must be shorter than the lease.');
        }
    }

    public static function isSupported(): bool
    {
        return DIRECTORY_SEPARATOR === '/'
            && PHP_INT_SIZE >= 8
            && function_exists('proc_open')
            && function_exists('proc_terminate')
            && function_exists('pcntl_async_signals')
            && function_exists('pcntl_signal')
            && function_exists('pcntl_signal_get_handler')
            && function_exists('posix_getppid')
            && function_exists('posix_kill')
            && function_exists('posix_setpgid')
            && defined('SIGTERM')
            && defined('SIGKILL')
            && defined('SIGCHLD');
    }

    public function __destruct()
    {
        $this->close();
    }

    public function track(string $leaseId, int $deadlineMonotonicMillis): void
    {
        $this->validateLeaseId($leaseId);
        if (isset($this->tracked[$leaseId])) {
            $this->assertHealthy($leaseId);
            return;
        }
        if ($this->tracked !== []) {
            throw new RuntimeException(
                'Queen Laravel lease renewal supports exactly one live pop lease per synchronous worker.',
            );
        }

        $now = self::monotonicMillis();
        $initialReserveSeconds = 2 * $this->requestBudgetSeconds
            + 1
            + $this->killGraceSeconds
            + $this->safetyMarginSeconds;
        if ($deadlineMonotonicMillis <= $now + $initialReserveSeconds * 1000) {
            throw new RuntimeException("Queen lease [{$leaseId}] reached its renewal deadline before tracking began.");
        }
        $this->start();
        $this->send([
            'command' => 'track',
            'lease_id' => $leaseId,
            'deadline_monotonic_millis' => $deadlineMonotonicMillis,
        ]);
        try {
            $this->waitUntilTracked($leaseId, $deadlineMonotonicMillis);
        } catch (\Throwable $exception) {
            // Registration outcome is ambiguous. Closing the private pipe is
            // the only reliable rollback: it stops this helper (and every
            // lease it knew) instead of risking an orphan renewal that keeps
            // an undelivered batch invisible forever.
            $this->close();
            throw $exception;
        }
        $this->tracked[$leaseId] = true;
        $this->armHelperDeathWatchdog();
        $this->assertHealthy($leaseId);
    }

    public function forget(string $leaseId): void
    {
        if (!isset($this->tracked[$leaseId])) {
            return;
        }

        unset($this->tracked[$leaseId], $this->failures[$leaseId]);
        if ($this->tracked === []) {
            $this->disarmHelperDeathWatchdog();
        }
        if (is_resource($this->process)) {
            try {
                $this->send(['command' => 'forget', 'lease_id' => $leaseId]);
            } catch (\Throwable) {
                // The lease has already been settled locally. A dead helper is
                // no longer a safety issue for this lease.
            }
        }
    }

    public function assertHealthy(string $leaseId): void
    {
        $this->drainOutput();
        if (isset($this->failures[$leaseId])) {
            throw new RuntimeException(
                "Queen lease renewal became unsafe for [{$leaseId}]: {$this->failures[$leaseId]}",
            );
        }
        if (!isset($this->tracked[$leaseId])) {
            throw new RuntimeException("Queen lease renewal is not tracking [{$leaseId}].");
        }
        if (!$this->running()) {
            $detail = $this->stderr !== '' ? ': ' . trim($this->stderr) : '';
            throw new RuntimeException("Queen lease renewal helper stopped unexpectedly{$detail}");
        }
    }

    public function close(): void
    {
        // Intentional shutdown must not be mistaken for an unsafe helper death.
        $this->disarmHelperDeathWatchdog();

        if (!is_resource($this->process)) {
            return;
        }

        try {
            $this->send(['command' => 'shutdown']);
        } catch (\Throwable) {
        }
        if (isset($this->pipes[0]) && is_resource($this->pipes[0])) {
            fclose($this->pipes[0]);
        }

        $deadline = microtime(true) + 0.5;
        while ($this->running() && microtime(true) < $deadline) {
            $this->drainOutput();
            usleep(10_000);
        }
        if ($this->running()) {
            @proc_terminate($this->process, SIGTERM);
        }
        foreach ([1, 2] as $index) {
            if (isset($this->pipes[$index]) && is_resource($this->pipes[$index])) {
                fclose($this->pipes[$index]);
            }
        }
        @proc_close($this->process);
        $this->process = null;
        $this->pipes = [];
        $this->tracked = [];
        $this->failures = [];
        $this->stdoutBuffer = '';
        $this->events = [];
    }

    /**
     * Fence this PHP worker if its exact renewal child disappears while a
     * lease is active. The process-wide handler chains any pre-existing
     * SIGCHLD callback and never reaps unrelated children.
     */
    private function armHelperDeathWatchdog(): void
    {
        if ($this->watchdogPid !== null || $this->tracked === []) {
            return;
        }
        if (!is_resource($this->process)) {
            self::fenceCurrentWorker('renewal helper process is unavailable');
        }

        $status = proc_get_status($this->process);
        $pid = (int) ($status['pid'] ?? 0);
        if ($pid < 1) {
            self::fenceCurrentWorker('renewal helper PID is unavailable');
        }

        self::installSigchldHandler();
        $this->watchdogPid = $pid;
        self::$armedWatchdogs[$pid] = \WeakReference::create($this);

        // Close the registration race: SIGCHLD may have arrived just before
        // the PID entered the registry.
        if (!$this->running()) {
            self::fenceCurrentWorker("renewal helper [{$pid}] already stopped");
        }
    }

    private function disarmHelperDeathWatchdog(): void
    {
        if ($this->watchdogPid === null) {
            return;
        }

        unset(self::$armedWatchdogs[$this->watchdogPid]);
        $this->watchdogPid = null;
        self::restoreSigchldHandlerWhenIdle();
    }

    private static function installSigchldHandler(): void
    {
        if (self::$sigchldHandlerInstalled) {
            return;
        }

        self::$previousSigchldHandler = pcntl_signal_get_handler(SIGCHLD);
        self::$previousAsyncSignals = pcntl_async_signals();
        if (!pcntl_signal(SIGCHLD, [self::class, 'handleSigchld'], true)) {
            throw new RuntimeException('Unable to install the Queen lease renewal SIGCHLD watchdog.');
        }
        pcntl_async_signals(true);
        self::$sigchldHandlerInstalled = true;
    }

    /** @internal Signal callback; public only because PCNTL invokes it out of scope. */
    public static function handleSigchld(int $signal, ?array $info = null): void
    {
        $reportedPid = (int) ($info['pid'] ?? 0);
        foreach (self::$armedWatchdogs as $pid => $reference) {
            $renewer = $reference->get();
            if (!$renewer instanceof self) {
                unset(self::$armedWatchdogs[$pid]);
                continue;
            }

            // siginfo identifies the child without touching any other process.
            // Scanning our own proc handles also covers platforms that omit PID
            // or coalesce several SIGCHLD deliveries.
            if ($reportedPid === $pid || !$renewer->running()) {
                self::fenceCurrentWorker("renewal helper [{$pid}] stopped unexpectedly");
            }
        }

        $previous = self::$previousSigchldHandler;
        if (is_callable($previous) && $previous !== [self::class, 'handleSigchld']) {
            $previous($signal, $info);
        }

        self::restoreSigchldHandlerWhenIdle();
    }

    private static function restoreSigchldHandlerWhenIdle(): void
    {
        if (!self::$sigchldHandlerInstalled || self::$armedWatchdogs !== []) {
            return;
        }

        $previous = self::$previousSigchldHandler;
        if (is_callable($previous) || is_int($previous)) {
            @pcntl_signal(SIGCHLD, $previous, true);
        } else {
            @pcntl_signal(SIGCHLD, SIG_DFL, true);
        }
        pcntl_async_signals(self::$previousAsyncSignals);
        self::$previousSigchldHandler = null;
        self::$sigchldHandlerInstalled = false;
    }

    private static function fenceCurrentWorker(string $reason): never
    {
        error_log("Queen Laravel lease renewal watchdog fenced this worker: {$reason}.");
        @posix_kill(getmypid(), SIGKILL);

        // SIGKILL should never return. Preserve fail-closed behavior on an
        // exotic runtime that refuses the signal.
        exit(1);
    }

    private function start(): void
    {
        if (is_resource($this->process)) {
            if ($this->running()) {
                return;
            }
            throw new RuntimeException('Queen lease renewal helper stopped before it could track a lease.');
        }

        $command = $this->workerCommand;
        if ($command === null) {
            $autoload = $this->findAutoload();
            $command = [
                $this->phpBinary,
                '-d',
                'display_errors=stderr',
                '-r',
                'require $argv[1]; \\Queen\\Laravel\\Queue\\LeaseRenewalWorker::main();',
                $autoload,
            ];
        }
        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];
        $pipes = [];
        $process = @proc_open($command, $descriptors, $pipes, null, null, ['bypass_shell' => true]);
        if (!is_resource($process)) {
            throw new RuntimeException('Unable to start the Queen lease renewal helper.');
        }
        $this->process = $process;
        $this->pipes = $pipes;
        stream_set_blocking($this->pipes[1], false);
        stream_set_blocking($this->pipes[2], false);

        $childConfig = $this->clientConfig;
        unset($childConfig['handler']);
        $childConfig['timeoutMillis'] = $this->requestTimeoutSeconds * 1000;
        $childConfig['retryAttempts'] = 1;
        $childConfig['retryDelayMillis'] = 0;
        $childConfig['retry429'] = ['maxAttempts' => 1, 'baseMs' => 1, 'capMs' => 1];
        $this->send([
            'command' => 'init',
            'client' => $childConfig,
            'parent_pid' => getmypid(),
            'lease_seconds' => $this->leaseSeconds,
            'interval_millis' => $this->intervalSeconds * 1000,
            'request_budget_millis' => $this->requestBudgetSeconds * 1000,
            'kill_grace_millis' => $this->killGraceSeconds * 1000,
            'safety_margin_millis' => $this->safetyMarginSeconds * 1000,
        ]);

        $deadline = microtime(true) + min(5, max(1, $this->requestTimeoutSeconds));
        while (microtime(true) < $deadline) {
            $event = $this->nextEvent();
            if (($event['event'] ?? null) === 'ready') {
                return;
            }
            if (($event['event'] ?? null) === 'startup_failed') {
                $error = (string) ($event['error'] ?? 'unknown child error');
                $this->close();
                throw new RuntimeException("Queen lease renewal helper failed to start: {$error}");
            }
            if (!$this->running()) {
                break;
            }
            $this->waitForOutput((int) max(1, min(100, ceil(($deadline - microtime(true)) * 1000))));
        }

        $detail = trim($this->readStderr());
        $this->close();
        throw new RuntimeException('Queen lease renewal helper did not become ready'
            . ($detail !== '' ? ": {$detail}" : '.'));
    }

    private function send(array $command): void
    {
        if (!isset($this->pipes[0]) || !is_resource($this->pipes[0])) {
            throw new RuntimeException('Queen lease renewal helper input is unavailable.');
        }
        $payload = json_encode($command, JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR) . "\n";
        $offset = 0;
        $length = strlen($payload);
        while ($offset < $length) {
            $written = @fwrite($this->pipes[0], substr($payload, $offset));
            if ($written === false || $written === 0) {
                throw new RuntimeException('Unable to communicate with the Queen lease renewal helper.');
            }
            $offset += $written;
        }
        fflush($this->pipes[0]);
    }

    private function drainOutput(): void
    {
        while (($event = $this->nextEvent()) !== null) {
            if (($event['event'] ?? null) === 'unsafe'
                && is_string($event['lease_id'] ?? null)
                && $event['lease_id'] !== '') {
                $this->failures[$event['lease_id']] = (string) ($event['error'] ?? 'renewal deadline exhausted');
            }
        }
        $this->stderr .= $this->readStderr();
        if (strlen($this->stderr) > 4096) {
            $this->stderr = substr($this->stderr, -4096);
        }
    }

    private function waitUntilTracked(string $leaseId, int $deadlineMonotonicMillis): void
    {
        $waitDeadline = min(
            self::monotonicMillis() + 1000,
            $deadlineMonotonicMillis - $this->safetyMarginSeconds * 1000,
        );
        while (self::monotonicMillis() < $waitDeadline) {
            $event = $this->nextEvent();
            if (($event['event'] ?? null) === 'tracked' && ($event['lease_id'] ?? null) === $leaseId) {
                return;
            }
            if (($event['event'] ?? null) === 'unsafe'
                && is_string($event['lease_id'] ?? null)
                && $event['lease_id'] !== '') {
                $this->failures[$event['lease_id']] = (string) ($event['error'] ?? 'renewal deadline exhausted');
            }
            if (!$this->running()) {
                break;
            }
            $this->waitForOutput(max(1, min(100, $waitDeadline - self::monotonicMillis())));
        }

        $detail = trim($this->readStderr());
        throw new RuntimeException("Queen lease renewal helper did not confirm tracking [{$leaseId}]"
            . ($detail !== '' ? ": {$detail}" : '.'));
    }

    private function nextEvent(): ?array
    {
        if ($this->events !== []) {
            return array_shift($this->events);
        }
        if (!isset($this->pipes[1]) || !is_resource($this->pipes[1])) {
            return null;
        }
        $chunk = stream_get_contents($this->pipes[1]);
        if (!is_string($chunk) || $chunk === '') {
            return null;
        }
        $this->stdoutBuffer .= $chunk;
        while (($newline = strpos($this->stdoutBuffer, "\n")) !== false) {
            $line = substr($this->stdoutBuffer, 0, $newline);
            $this->stdoutBuffer = substr($this->stdoutBuffer, $newline + 1);
            $event = json_decode($line, true);
            $this->events[] = is_array($event) ? $event : [];
        }

        return $this->events !== [] ? array_shift($this->events) : null;
    }

    private function readStderr(): string
    {
        if (!isset($this->pipes[2]) || !is_resource($this->pipes[2])) {
            return '';
        }
        $value = stream_get_contents($this->pipes[2]);
        return is_string($value) ? $value : '';
    }

    private function waitForOutput(int $timeoutMillis): bool
    {
        if (!isset($this->pipes[1]) || !is_resource($this->pipes[1])) {
            return false;
        }
        $read = [$this->pipes[1]];
        $write = null;
        $except = null;
        $seconds = intdiv($timeoutMillis, 1000);
        $micros = ($timeoutMillis % 1000) * 1000;

        return @stream_select($read, $write, $except, $seconds, $micros) > 0;
    }

    private function running(): bool
    {
        if (!is_resource($this->process)) {
            return false;
        }
        $status = proc_get_status($this->process);
        return ($status['running'] ?? false) === true;
    }

    private function findAutoload(): string
    {
        $reflection = new \ReflectionClass(\Queen\Queen::class);
        $directory = dirname((string) $reflection->getFileName());
        while (true) {
            foreach (["{$directory}/vendor/autoload.php", "{$directory}/autoload.php"] as $candidate) {
                if (is_file($candidate)) {
                    return $candidate;
                }
            }
            $parent = dirname($directory);
            if ($parent === $directory) {
                break;
            }
            $directory = $parent;
        }

        throw new RuntimeException('Unable to locate Composer autoload.php for the Queen lease renewal helper.');
    }

    private function validateLeaseId(string $leaseId): void
    {
        if ($leaseId === '' || strlen($leaseId) > 255 || preg_match('/[\x00-\x1F\x7F]/', $leaseId)) {
            throw new RuntimeException('Queen returned an invalid lease ID for renewal.');
        }
    }

    private static function monotonicMillis(): int
    {
        return intdiv(hrtime(true), 1_000_000);
    }

    /** @param list<int> $values */
    private static function sumIsBelow(array $values, int $limit): bool
    {
        $sum = 0;
        foreach ($values as $value) {
            if ($value >= $limit - $sum) {
                return false;
            }
            $sum += $value;
        }

        return true;
    }
}
