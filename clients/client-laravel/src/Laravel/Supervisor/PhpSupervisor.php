<?php

namespace Queen\Laravel\Supervisor;

use Illuminate\Queue\QueueManager;
use Queen\Exceptions\HttpException;
use Queen\Http\HttpClient;
use Queen\Laravel\Queue\QueenQueue;
use Queen\Queen;
use Symfony\Component\Process\Process;

final class PhpSupervisor
{
    private const DEPTH_POLL_CONCURRENCY = 16;

    /** @var array<string, array<string, list<Process>>> */
    private array $processes = [];
    /** @var list<array{process:Process,deadline:float,label:string,supervisor:string,queue:string}> */
    private array $draining = [];
    /** @var array<int, float> */
    private array $startedAt = [];
    /** @var array<int, int> */
    private array $workerPids = [];
    /** @var array<string, int> */
    private array $lastReconcile = [];
    /** @var array<string, array<string, int>> */
    private array $lastDesired = [];
    /** @var array<string, array<string, int>> */
    private array $lastDepths = [];
    /** @var array<string, bool> */
    private array $depthsAvailable = [];
    /** @var array<string, float> */
    private array $restartAfter = [];
    /** @var array<string, int> */
    private array $crashCount = [];
    /** @var array<string, float> */
    private array $lastCrashAt = [];
    /** @var array<string, array{target:int,since:float}> */
    private array $downscaleCandidates = [];
    /** @var array<string, array<int, true>> */
    private array $pendingTelemetryCleanup = [];
    private bool $running = true;
    private bool $paused = false;
    private ?string $lastCommandNonce = null;
    private SupervisorState $state;
    /** @var array<string, Queen> */
    private array $depthClients = [];

    public function __construct(
        private QueueManager $queues,
        private array $config,
        private AutoScaler $scaler = new AutoScaler(),
        private TelemetryReader $telemetry = new TelemetryReader(),
        private ?\Closure $output = null,
        private ?\Closure $queenFactory = null,
    ) {
        $this->state = new SupervisorState($config['state_directory']);
    }

    public function run(bool $once = false): void
    {
        $this->assertSupportedRuntime();
        $this->installSignalHandlers();
        $lock = $this->state->acquireLock();
        $failure = null;

        try {
            $this->writeStatus('running');
            $lastPoll = 0.0;
            do {
                $this->handleControlCommand();
                if (!$this->running) {
                    break;
                }

                foreach (array_keys($this->config['supervisors']) as $name) {
                    $this->reap($name, $this->config['supervisors'][$name]);
                }
                $this->reapDraining();

                if (microtime(true) - $lastPoll < $this->config['poll_interval']) {
                    usleep(200_000);
                    continue;
                }

                // One dead connection is sampled once per poll even when
                // several supervisor pools share it. Each affected pool still
                // performs its own fail-open reconcile from the last target.
                $unavailableConnections = [];
                foreach ($this->config['supervisors'] as $name => $options) {
                    if ($this->paused) {
                        $this->lastDepths[$name] = [];
                        $this->depthsAvailable[$name] = false;
                        continue;
                    }
                    if (time() - ($this->lastReconcile[$name] ?? 0) < $options['balance_cooldown']) {
                        continue;
                    }

                    $runtimes = [];
                    try {
                        // Scan independently from broker health. This both keeps
                        // time-based samples current and lets the bounded reader
                        // reclaim telemetry left by short-lived workers during a
                        // depth outage.
                        $runtimes = $this->observedRuntimes($name, $options);
                    } catch (\Throwable $error) {
                        $this->emit("[{$name}] telemetry failed: {$error->getMessage()}\n", 'err');
                    }

                    try {
                        if (isset($unavailableConnections[$options['connection']])) {
                            throw new \RuntimeException(
                                "Queen connection [{$options['connection']}] already failed this polling cycle."
                            );
                        }
                        $depths = $this->depths($options);
                        $this->lastDepths[$name] = $depths;
                        $this->depthsAvailable[$name] = true;
                        $desired = $this->scaler->desired($options, $depths, $runtimes);
                        $this->lastDesired[$name] = $desired;
                    } catch (\Throwable $error) {
                        $this->lastDepths[$name] = [];
                        $this->depthsAvailable[$name] = false;
                        $unavailableConnections[$options['connection']] = true;
                        $this->emit("[{$name}] depth failed: {$error->getMessage()}\n", 'err');
                        // An observability outage must not tear down healthy
                        // capacity or stop replacement of crashed workers. On
                        // the very first poll, treat every configured queue as
                        // active so a small min_processes cannot strand later
                        // queues until the broker recovers.
                        $desired = $this->lastDesired[$name]
                            ?? $this->scaler->desired($options, array_fill_keys($options['queues'], 1));
                    }

                    if (!$this->running) {
                        break;
                    }
                    if ($this->paused) {
                        continue;
                    }

                    $desired = $this->stabilizeDownscale($name, $options, $desired);
                    $this->lastDesired[$name] = $desired;
                    $this->reconcile($name, $options, $desired);
                    $this->lastReconcile[$name] = time();
                }

                if (!$this->running) {
                    break;
                }
                $this->writeStatus($this->paused ? 'paused' : 'running');
                $lastPoll = microtime(true);
                if ($once) {
                    break;
                }
            } while ($this->running);
        } catch (\Throwable $error) {
            // Finalization must not mask the original runtime failure. We
            // still fence dashboard controls and drain every owned worker
            // before returning the failure to the command.
            $failure = $error;
        }

        $shutdownComplete = false;
        try {
            try {
                // External signals and --once do not pass through the control
                // inbox, so publish the generation-specific drain fence here
                // before touching children in every termination path.
                $this->writeStatus('terminating');
            } catch (\Throwable $error) {
                $failure ??= $error;
            }
            try {
                $this->shutdown();
                $shutdownComplete = true;
            } catch (\Throwable $error) {
                $failure ??= $error;
            }
            if ($shutdownComplete) {
                try {
                    $this->writeStatus('stopped');
                } catch (\Throwable $error) {
                    $failure ??= $error;
                }
            }
        } finally {
            flock($lock, LOCK_UN);
            fclose($lock);
        }

        if ($failure !== null) {
            throw $failure;
        }
    }

    public function stop(): void
    {
        $this->running = false;
    }

    /** @return array<string, int> */
    private function depths(array $options): array
    {
        $admin = $this->depthClient($options['connection'])->admin();
        $depths = [];
        foreach (array_chunk($options['queues'], self::DEPTH_POLL_CONCURRENCY) as $queues) {
            $promises = [];
            foreach ($queues as $queue) {
                $promises[$queue] = $admin->getQueueDepthAsync(
                    $queue,
                    $options['consumer_group'],
                    $this->config['http_timeout'] * 1000,
                );
            }

            foreach (HttpClient::settleAll($promises) as $queue => $outcome) {
                if ($outcome['state'] === 'rejected') {
                    $error = $outcome['reason'] instanceof \Throwable
                        ? $outcome['reason']
                        : new \RuntimeException("Queen depth request for [{$queue}] failed with a non-exception rejection.");
                    if ($error instanceof HttpException && $error->statusCode === 404 && $error->errorCode !== 'no_such_route') {
                        // A consumer may start before the first push creates
                        // the durable queue registration.
                        $depths[$queue] = 0;
                        continue;
                    }
                    throw $error;
                }

                $depth = $outcome['value'];
                if (!is_array($depth)) {
                    throw new \UnexpectedValueException("Queen returned a malformed depth response for [{$queue}].");
                }
                $value = array_key_exists('effectivePending', $depth)
                    ? $depth['effectivePending']
                    : ($depth['pending'] ?? null);
                if (!is_int($value) || $value < 0) {
                    throw new \UnexpectedValueException("Queen returned a malformed depth response for [{$queue}].");
                }
                $depths[$queue] = $value;
            }
        }
        $ordered = [];
        foreach ($options['queues'] as $queue) {
            $ordered[$queue] = $depths[$queue];
        }
        return $ordered;
    }

    /** @return array<string, float> */
    private function observedRuntimes(string $name, array $options): array
    {
        if (($options['strategy'] ?? 'size') !== 'time' || ($options['balance'] ?? 'auto') === 'simple') {
            return [];
        }

        try {
            return $this->telemetry->runtimes(
                $this->state->telemetryDirectory(),
                $this->config['telemetry_ttl'],
                ['supervisor' => $name, 'connection' => $options['connection'], 'consumer_group' => $options['consumer_group']],
            );
        } finally {
            // A broken/replaced telemetry path must not retain an unbounded
            // in-memory PID set while max-jobs workers continue to churn.
            $this->flushTelemetryCleanup($name);
        }
    }

    private function depthClient(string $connectionName): Queen
    {
        if (isset($this->depthClients[$connectionName])) {
            return $this->depthClients[$connectionName];
        }

        $connections = $this->config['connections'] ?? [];
        $usesV2Connections = array_key_exists('connections', $this->config)
            || (int) ($this->config['version'] ?? 0) >= SupervisorConfiguration::VERSION;
        if ($usesV2Connections) {
            if (!array_key_exists($connectionName, $connections)) {
                throw new \RuntimeException("Queen supervisor v2 connection [{$connectionName}] is missing from the resolved contract.");
            }
            $connection = $connections[$connectionName];
            $options = [
                'urls' => $connection['urls'] ?? [$connection['url']],
                'bearerToken' => $connection['bearer_token'] ?? null,
                'headers' => $connection['headers'] ?? [],
                'timeoutMillis' => ($this->config['http_timeout'] ?? 5) * 1000,
                // Depth is advisory and polled again shortly. One attempt per
                // endpoint keeps the orchestrator responsive; multi-endpoint
                // failover is still handled by getQueueDepthAsync().
                'retryAttempts' => 1,
                'retryDelayMillis' => 0,
            ];
            $client = $this->queenFactory !== null
                ? ($this->queenFactory)($connectionName, $options)
                : new Queen($options);
            if (!$client instanceof Queen) {
                throw new \RuntimeException('Queen supervisor client factory must return a Queen client.');
            }

            return $this->depthClients[$connectionName] = $client;
        }

        // Constructor/API compatibility for callers still handing an old,
        // pre-v2 configuration directly to PhpSupervisor. Resolved v2
        // configurations always take the credential-isolated branch above.
        $connection = $this->queues->connection($connectionName);
        if (!$connection instanceof QueenQueue) {
            throw new \RuntimeException("Supervisor connection [{$connectionName}] is not a Queen queue connection.");
        }

        return $this->depthClients[$connectionName] = $connection->getQueen();
    }

    private function reconcile(string $name, array $options, array $desired, ?int $maximumShift = null): void
    {
        $active = array_sum(array_map('count', $this->processes[$name] ?? []));
        $budget = $maximumShift ?? $this->reconcileBudget($options, $active);

        // Free capacity first. Termination is asynchronous, so a single
        // reconcile never blocks for N * shutdown_grace.
        foreach ($desired as $queue => $target) {
            $pool =& $this->processes[$name][$queue];
            $pool ??= [];
            while ($budget > 0 && $target < count($pool)) {
                $this->beginTermination(array_pop($pool), $name, $queue);
                $budget--;
            }
        }

        foreach ($desired as $queue => $target) {
            $pool =& $this->processes[$name][$queue];
            while ($budget > 0 && $target > count($pool)) {
                // A process remains part of the global capacity budget until
                // it has actually exited. This prevents a slow graceful drain
                // from temporarily oversubscribing process_limit while a
                // replacement is started in another pool.
                if ($this->remainingProcessSlots() <= 0) {
                    break;
                }
                if (!$this->canRestart($name, $queue)) {
                    break;
                }
                try {
                    $pool[] = $this->startWorker($name, $queue, $options);
                    $budget--;
                } catch (\Throwable $error) {
                    $this->registerCrash($name, $queue, $options, $error->getMessage());
                    break;
                }
            }
        }
    }

    private function remainingProcessSlots(): int
    {
        $active = 0;
        foreach ($this->processes as $pools) {
            foreach ($pools as $pool) {
                $active += count($pool);
            }
        }

        return max(0, (int) ($this->config['process_limit'] ?? 256) - $active - count($this->draining));
    }

    private function reconcileBudget(array $options, int $active): int
    {
        // balance_max_shift bounds elastic changes, but it must never make a
        // supervisor take several cooldown windows to establish or restore
        // its configured baseline capacity.
        $baseline = $options['balance'] === 'simple'
            ? (int) $options['processes']
            : (int) $options['min_processes'];

        return max((int) $options['balance_max_shift'], $baseline - $active);
    }

    private function startWorker(string $name, string $queue, array $options): Process
    {
        $command = [
            $this->config['php_binary'], $this->config['artisan'], 'queue:work',
            $options['connection'],
            '--queue=' . ($options['balance'] === 'off' ? implode(',', $options['queues']) : $queue),
            '--sleep=' . $options['sleep'],
            '--timeout=' . $options['timeout'],
            '--tries=' . $options['tries'],
            '--memory=' . $options['memory'],
            '--backoff=' . $options['backoff'],
            '--max-jobs=' . $options['max_jobs'],
            '--max-time=' . $options['max_time'],
            '--rest=' . $options['rest'],
        ];
        if ($options['force']) {
            $command[] = '--force';
        }
        if ($options['quiet'] ?? false) {
            $command[] = '--quiet';
        }

        $environment = [
            // `false` also removes a value inherited by the supervisor.
            'QUEEN_SUPERVISOR_TELEMETRY_DIR' => ($options['strategy'] ?? 'size') === 'time'
                && ($options['balance'] ?? 'auto') !== 'simple'
                ? $this->state->telemetryDirectory()
                : false,
            'QUEEN_LARAVEL_CONSUMER_GROUP' => $options['consumer_group'],
            'QUEEN_LARAVEL_CONNECTION' => $options['connection'],
            'QUEEN_LARAVEL_SUPERVISOR' => $name,
            'QUEEN_LARAVEL_RETRY_AFTER' => (string) ($options['retry_after'] ?? ($options['timeout'] + 1)),
            'QUEEN_LARAVEL_BLOCK_FOR' => $options['balance'] === 'off' ? '0' : null,
        ];
        $process = new Process($command, $this->config['cwd'], $environment, timeout: null);
        // `--quiet` suppresses per-job chatter while retaining startup/fatal
        // diagnostics. The loop drains Symfony's small pipe buffers every
        // 200ms so an unusually noisy failure cannot backpressure a worker.
        $process->start(function (string $type, string $buffer) use ($name, $queue): void {
            $this->emit("[{$name}:{$queue}] {$buffer}", $type);
        });
        $objectId = spl_object_id($process);
        $this->startedAt[$objectId] = microtime(true);
        $pid = $process->getPid();
        if (is_int($pid)) {
            $this->workerPids[$objectId] = $pid;
        }
        $this->emit("started {$name}:{$queue} pid=" . ($pid ?? 'unknown') . "\n", 'out');
        return $process;
    }

    private function reap(string $name, array $options): void
    {
        foreach ($this->processes[$name] ?? [] as $queue => $pool) {
            $running = [];
            foreach ($pool as $process) {
                if ($process->isRunning()) {
                    $this->discardOutput($process);
                    $running[] = $process;
                    continue;
                }

                $objectId = spl_object_id($process);
                $runtime = microtime(true) - ($this->startedAt[$objectId] ?? microtime(true));
                unset($this->startedAt[$objectId]);
                $pid = $this->trackedPid($process);
                unset($this->workerPids[$objectId]);
                $this->scheduleTelemetryCleanup($name, $options, $pid);
                $exitCode = $process->getExitCode();
                $this->emit("exited {$name}:{$queue} pid=" . ($pid ?? 'unknown') . " code=" . ($exitCode ?? 'unknown') . "\n", $exitCode === 0 ? 'out' : 'err');
                if ($exitCode === 0) {
                    $this->resetCrashes($name, $queue);
                } else {
                    $this->registerCrash($name, $queue, $options, "exit code " . ($exitCode ?? 'unknown'), $runtime);
                }
            }
            $this->processes[$name][$queue] = $running;
        }
    }

    private function registerCrash(string $name, string $queue, array $options, string $reason, float $runtime = 0.0): void
    {
        $key = $this->poolKey($name, $queue);
        $now = microtime(true);
        $stableAfter = (float) $options['stable_after'];
        if ($runtime >= $stableAfter || $now - ($this->lastCrashAt[$key] ?? 0.0) >= $stableAfter) {
            $this->crashCount[$key] = 0;
        }
        $count = ($this->crashCount[$key] ?? 0) + 1;
        $this->crashCount[$key] = $count;
        $this->lastCrashAt[$key] = $now;
        $delay = $count >= 5
            ? (int) $options['restart_backoff_max']
            : min(
                (int) $options['restart_backoff_max'],
                (int) $options['restart_backoff'] * (2 ** min(20, $count - 1)),
            );
        $this->restartAfter[$key] = $now + $delay;
        $this->emit("[{$name}:{$queue}] worker crash ({$reason}); restart in {$delay}s (failure {$count})\n", 'err');
    }

    private function resetCrashes(string $name, string $queue): void
    {
        $key = $this->poolKey($name, $queue);
        unset($this->crashCount[$key], $this->lastCrashAt[$key], $this->restartAfter[$key]);
    }

    private function canRestart(string $name, string $queue): bool
    {
        return microtime(true) >= ($this->restartAfter[$this->poolKey($name, $queue)] ?? 0.0);
    }

    private function poolKey(string $name, string $queue): string
    {
        // Length-prefixing keeps every valid pair injective even when either
        // identifier contains the delimiter used in human-facing labels.
        return strlen($name) . ':' . $name . strlen($queue) . ':' . $queue;
    }

    /** @param array<string, int> $desired */
    private function stabilizeDownscale(string $name, array $options, array $desired): array
    {
        $current = array_sum(array_map('count', $this->processes[$name] ?? []));
        $target = array_sum($desired);
        if ($target >= $current || (int) $options['scale_down_delay'] === 0) {
            unset($this->downscaleCandidates[$name]);
            return $desired;
        }

        $candidate = $this->downscaleCandidates[$name] ?? null;
        if ($candidate === null || $candidate['target'] !== $target) {
            $this->downscaleCandidates[$name] = ['target' => $target, 'since' => microtime(true)];
            return $this->currentAllocation($name, $options['queues']);
        }
        if (microtime(true) - $candidate['since'] < $options['scale_down_delay']) {
            return $this->currentAllocation($name, $options['queues']);
        }

        return $desired;
    }

    /** @return array<string, int> */
    private function currentAllocation(string $name, array $queues): array
    {
        $allocation = [];
        foreach ($queues as $queue) {
            $allocation[$queue] = count($this->processes[$name][$queue] ?? []);
        }
        return $allocation;
    }

    private function beginTermination(Process $process, string $supervisor, string $queue): void
    {
        if (!$process->isRunning()) {
            $objectId = spl_object_id($process);
            unset($this->startedAt[$objectId]);
            $this->scheduleTelemetryCleanup(
                $supervisor,
                $this->config['supervisors'][$supervisor] ?? [],
                $this->trackedPid($process),
            );
            unset($this->workerPids[$objectId]);
            return;
        }
        try {
            $process->signal(SIGTERM);
        } catch (\Throwable $error) {
            $this->emit("[{$supervisor}:{$queue}] SIGTERM failed: {$error->getMessage()}\n", 'err');
        }
        $this->draining[] = [
            'process' => $process,
            'deadline' => microtime(true) + $this->config['shutdown_grace'],
            'label' => "{$supervisor}:{$queue}",
            'supervisor' => $supervisor,
            'queue' => $queue,
        ];
    }

    private function reapDraining(): void
    {
        $remaining = [];
        foreach ($this->draining as $entry) {
            $process = $entry['process'];
            if (!$process->isRunning()) {
                $objectId = spl_object_id($process);
                unset($this->startedAt[$objectId]);
                $this->scheduleTelemetryCleanup(
                    $entry['supervisor'],
                    $this->config['supervisors'][$entry['supervisor']] ?? [],
                    $this->trackedPid($process),
                );
                unset($this->workerPids[$objectId]);
                continue;
            }
            $this->discardOutput($process);
            if (microtime(true) >= $entry['deadline']) {
                try {
                    $process->signal(SIGKILL);
                } catch (\Throwable $error) {
                    $this->emit("[{$entry['label']}] SIGKILL failed: {$error->getMessage()}\n", 'err');
                }
                // A signal request is not proof of process death. Retain the
                // child in draining (and therefore in process_limit) until a
                // later poll observes the exit. Retry a failed/stuck kill at
                // a bounded cadence without spinning or flooding logs.
                $entry['deadline'] = microtime(true) + 1.0;
                $remaining[] = $entry;
            } else {
                $remaining[] = $entry;
            }
        }
        $this->draining = $remaining;
    }

    private function discardOutput(Process $process): void
    {
        if (!$process->isOutputDisabled()) {
            $process->clearOutput();
            $process->clearErrorOutput();
        }
    }

    private function shutdown(): void
    {
        /** @var array<int, Process> $running */
        $running = [];
        foreach ($this->processes as $pools) {
            foreach ($pools as $pool) {
                foreach ($pool as $process) {
                    if ($process->isRunning()) {
                        $running[spl_object_id($process)] = $process;
                    } else {
                        $this->removeTelemetry($this->trackedPid($process));
                    }
                }
            }
        }
        foreach ($this->draining as $entry) {
            if ($entry['process']->isRunning()) {
                $running[spl_object_id($entry['process'])] = $entry['process'];
            } else {
                $this->removeTelemetry($this->trackedPid($entry['process']));
            }
        }
        foreach ($running as $process) {
            try {
                $process->signal(SIGTERM);
            } catch (\Throwable) {
                // The worker may have exited between isRunning and signal.
            }
        }

        $deadline = microtime(true) + $this->config['shutdown_grace'];
        do {
            foreach ($running as $objectId => $process) {
                if (!$process->isRunning()) {
                    $this->removeTelemetry($this->trackedPid($process));
                    unset($this->workerPids[$objectId]);
                    unset($running[$objectId]);
                }
            }
            if ($running === [] || microtime(true) >= $deadline) {
                break;
            }
            usleep(50_000);
        } while (true);

        foreach ($running as $process) {
            try {
                $process->signal(SIGKILL);
            } catch (\Throwable) {
                // Retry below while retaining ownership of the generation.
            }
        }

        // Do not publish `stopped` or release supervisor.lock merely because
        // SIGKILL was requested. Until every child is observed dead, a new
        // generation could otherwise start replacement capacity alongside an
        // old unkillable worker. This intentionally fails stop: retry KILL and
        // keep the generation fence for as long as any child remains alive.
        $nextKillAttempt = microtime(true) + 1.0;
        while ($running !== []) {
            foreach ($running as $objectId => $process) {
                if (!$process->isRunning()) {
                    $this->removeTelemetry($this->trackedPid($process));
                    unset($this->workerPids[$objectId]);
                    unset($running[$objectId]);
                    continue;
                }
                $this->discardOutput($process);
            }
            if ($running === []) {
                break;
            }
            if (microtime(true) >= $nextKillAttempt) {
                foreach ($running as $process) {
                    try {
                        $process->signal(SIGKILL);
                    } catch (\Throwable) {
                        // The lock remains held; retry without allowing takeover.
                    }
                }
                $nextKillAttempt = microtime(true) + 1.0;
            }
            usleep(50_000);
        }
        $this->processes = [];
        $this->draining = [];
        $this->startedAt = [];
        $this->workerPids = [];
        foreach (array_keys($this->pendingTelemetryCleanup) as $supervisor) {
            $this->flushTelemetryCleanup($supervisor);
        }
    }

    private function scheduleTelemetryCleanup(string $supervisor, array $options, mixed $pid): void
    {
        if (!is_int($pid) || $pid < 1) {
            return;
        }
        if (($options['strategy'] ?? 'size') === 'time' && ($options['balance'] ?? 'auto') !== 'simple') {
            // The next scan must ingest this short-lived worker's final sample
            // before its PID file is removed (notably with --max-jobs=1).
            $this->pendingTelemetryCleanup[$supervisor][$pid] = true;

            return;
        }

        $this->removeTelemetry($pid);
    }

    private function flushTelemetryCleanup(string $supervisor): void
    {
        foreach (array_keys($this->pendingTelemetryCleanup[$supervisor] ?? []) as $pid) {
            $this->removeTelemetry($pid);
        }
        unset($this->pendingTelemetryCleanup[$supervisor]);
    }

    private function removeTelemetry(mixed $pid): void
    {
        if (!is_int($pid) || $pid < 1) {
            return;
        }
        try {
            $this->state->removeTelemetryForPid($pid);
        } catch (\Throwable $error) {
            // Telemetry is advisory. A malformed or externally changed state
            // directory must never abort worker reaping or release the
            // generation lock while an owned child may still be alive.
            $this->emit("telemetry cleanup failed for pid={$pid}: {$error->getMessage()}\n", 'err');
        }
    }

    private function trackedPid(Process $process): ?int
    {
        return $this->workerPids[spl_object_id($process)] ?? null;
    }

    private function assertSupportedRuntime(): void
    {
        if (
            PHP_OS_FAMILY === 'Windows'
            || !class_exists(Process::class)
            || !extension_loaded('pcntl')
            || !function_exists('pcntl_async_signals')
            || !defined('SIGTERM')
            || !defined('SIGKILL')
        ) {
            throw new \RuntimeException(
                'The Queen PHP supervisor requires a Unix-like OS, ext-pcntl and symfony/process. '
                . 'The Rust engine is the optimized alternative on supported Unix platforms.',
            );
        }
    }

    private function installSignalHandlers(): void
    {
        pcntl_async_signals(true);
        pcntl_signal(SIGINT, fn () => $this->stop());
        pcntl_signal(SIGTERM, fn () => $this->stop());
        if (defined('SIGQUIT')) {
            pcntl_signal(SIGQUIT, fn () => $this->stop());
        }
    }

    private function handleControlCommand(): void
    {
        $control = $this->state->command($this->lastCommandNonce, $this->state->instanceId());
        if ($control === null) {
            return;
        }
        $this->lastCommandNonce = $control['nonce'];
        match ($control['command']) {
            'pause' => $this->pause(),
            'continue' => $this->resume(),
            'terminate' => $this->stop(),
        };
        $this->writeStatus($this->paused ? 'paused' : ($this->running ? 'running' : 'terminating'));
    }

    private function pause(): void
    {
        if ($this->paused) {
            return;
        }
        $this->paused = true;
        $this->lastDepths = [];
        $this->depthsAvailable = array_fill_keys(array_keys($this->config['supervisors']), false);
        // A suspended queue:work process may still own a prefetched tail of
        // leased jobs without renewing it. Drain workers instead: Laravel can
        // finish the active job and exit, while no replacement is started for
        // as long as this supervisor remains paused.
        foreach ($this->processes as $supervisor => $pools) {
            foreach ($pools as $queue => $pool) {
                foreach ($pool as $process) {
                    $this->beginTermination($process, $supervisor, $queue);
                }
                $this->processes[$supervisor][$queue] = [];
            }
        }
    }

    private function resume(): void
    {
        if (!$this->paused) {
            return;
        }
        $this->paused = false;
    }

    private function writeStatus(string $status): void
    {
        $pools = [];
        $poolStatus = [];
        $drainingCounts = [];
        $drainingPids = [];
        foreach ($this->draining as $entry) {
            $key = $entry['supervisor'] . "\0" . $entry['queue'];
            $drainingCounts[$key] = ($drainingCounts[$key] ?? 0) + 1;
            $pid = $this->trackedPid($entry['process']);
            if (is_int($pid)) {
                $drainingPids[$key][] = $pid;
            }
        }

        $supervisors = $this->config['supervisors'];
        ksort($supervisors, SORT_STRING);
        foreach ($supervisors as $name => $options) {
            $name = (string) $name;
            foreach ($options['queues'] as $queue) {
                $processes = $this->processes[$name][$queue] ?? [];
                $pids = array_values(array_filter(
                    array_map(fn (Process $process) => $this->trackedPid($process), $processes),
                    'is_int',
                ));
                $key = $name . "\0" . $queue;
                $restartKey = $this->poolKey($name, $queue);
                $failures = $this->crashCount[$restartKey] ?? 0;
                $retryIn = isset($this->restartAfter[$restartKey])
                    ? max(0, (int) ceil($this->restartAfter[$restartKey] - microtime(true)))
                    : null;
                $restartState = $retryIn !== null && $retryIn > 0
                    ? 'backoff'
                    : ($failures > 0 ? 'probe' : 'closed');
                $desired = in_array($status, ['terminating', 'stopped'], true)
                    ? 0
                    : ($this->lastDesired[$name][$queue] ?? count($processes));
                $entry = [
                    'supervisor' => $name,
                    'queue' => $queue,
                    'desired' => $desired,
                    'running' => count($processes),
                    'draining' => $drainingCounts[$key] ?? 0,
                    'pids' => $pids,
                    'draining_pids' => $drainingPids[$key] ?? [],
                    'restart_state' => $restartState,
                    'restart_failures' => $failures,
                    'restart_in_seconds' => $retryIn !== null && $retryIn > 0 ? $retryIn : null,
                    'healthy' => $restartState === 'closed' && $failures === 0,
                    'depth' => ($this->depthsAvailable[$name] ?? false)
                        ? ($this->lastDepths[$name][$queue] ?? null)
                        : null,
                    'depth_available' => $this->depthsAvailable[$name] ?? false,
                ];
                $poolStatus[] = $entry;
                // Keep the original nested map and field names for consumers
                // predating the normalized v1 pool list.
                $pools[$name][$queue] = [
                    'processes' => $entry['running'],
                    'pids' => $entry['pids'],
                    'desired' => $entry['desired'],
                    'draining' => $entry['draining'],
                    'restart_state' => $entry['restart_state'],
                    'restart_failures' => $entry['restart_failures'],
                    'restart_in_seconds' => $entry['restart_in_seconds'],
                    'depth' => $entry['depth'],
                    'depth_available' => $entry['depth_available'],
                ];
            }
        }
        $this->state->writeStatus([
            'engine' => 'php',
            'state' => $status,
            'draining' => count($this->draining),
            'pools' => $pools,
            'pool_status' => $poolStatus,
            'configuration' => $this->statusConfiguration(),
        ]);
    }

    /** @return array<string, mixed> */
    private function statusConfiguration(): array
    {
        $configured = $this->config['supervisors'] ?? [];
        ksort($configured, SORT_STRING);
        $supervisors = [];
        foreach ($configured as $name => $options) {
            $supervisors[] = [
                'name' => (string) $name,
                'connection' => (string) ($options['connection'] ?? 'queen'),
                'consumer_group' => (string) ($options['consumer_group'] ?? 'laravel'),
                'queues' => array_values(array_map('strval', $options['queues'] ?? [])),
                'balance' => (string) ($options['balance'] ?? 'auto'),
                'strategy' => (string) ($options['strategy'] ?? 'size'),
                'processes' => (int) ($options['processes'] ?? $options['max_processes'] ?? 10),
                'min_processes' => (int) ($options['min_processes'] ?? 1),
                'max_processes' => (int) ($options['max_processes'] ?? 10),
                'timeout' => (int) ($options['timeout'] ?? 60),
                'retry_after' => (int) ($options['retry_after'] ?? 90),
                'tries' => (int) ($options['tries'] ?? 3),
                'memory' => (int) ($options['memory'] ?? 128),
            ];
        }

        return [
            'poll_interval' => (int) ($this->config['poll_interval'] ?? 3),
            'http_timeout' => (int) ($this->config['http_timeout'] ?? 5),
            'control_ttl' => (int) ($this->config['control_ttl'] ?? 3600),
            'heartbeat_timeout' => (int) ($this->config['heartbeat_timeout'] ?? 3600),
            'shutdown_grace' => (int) ($this->config['shutdown_grace'] ?? 75),
            'telemetry_ttl' => (int) ($this->config['telemetry_ttl'] ?? 300),
            'process_limit' => (int) ($this->config['process_limit'] ?? 256),
            'supervisors' => $supervisors,
        ];
    }

    private function emit(string $buffer, string $type): void
    {
        if ($this->output !== null) {
            ($this->output)($buffer, $type);
        }
    }
}
