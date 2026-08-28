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
    /** @var list<array{process:Process,deadline:float,label:string}> */
    private array $draining = [];
    /** @var array<int, float> */
    private array $startedAt = [];
    /** @var array<string, int> */
    private array $lastReconcile = [];
    /** @var array<string, array<string, int>> */
    private array $lastDesired = [];
    /** @var array<string, float> */
    private array $restartAfter = [];
    /** @var array<string, int> */
    private array $crashCount = [];
    /** @var array<string, float> */
    private array $lastCrashAt = [];
    /** @var array<string, array{target:int,since:float}> */
    private array $downscaleCandidates = [];
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
                        continue;
                    }
                    if (time() - ($this->lastReconcile[$name] ?? 0) < $options['balance_cooldown']) {
                        continue;
                    }

                    try {
                        if (isset($unavailableConnections[$options['connection']])) {
                            throw new \RuntimeException(
                                "Queen connection [{$options['connection']}] already failed this polling cycle."
                            );
                        }
                        $depths = $this->depths($options);
                        $runtimes = $this->telemetry->runtimes(
                            $this->state->telemetryDirectory(),
                            $this->config['telemetry_ttl'],
                            ['supervisor' => $name, 'connection' => $options['connection'], 'consumer_group' => $options['consumer_group']],
                        );
                        $desired = $this->scaler->desired($options, $depths, $runtimes);
                        $this->lastDesired[$name] = $desired;
                    } catch (\Throwable $error) {
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
        } finally {
            try {
                $this->shutdown();
                $this->writeStatus('stopped');
            } finally {
                flock($lock, LOCK_UN);
                fclose($lock);
            }
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
        $budget = $maximumShift ?? (int) $options['balance_max_shift'];

        // Free capacity first. Termination is asynchronous, so a single
        // reconcile never blocks for N * shutdown_grace.
        foreach ($desired as $queue => $target) {
            $pool =& $this->processes[$name][$queue];
            $pool ??= [];
            while ($budget > 0 && $target < count($pool)) {
                $this->beginTermination(array_pop($pool), "{$name}:{$queue}");
                $budget--;
            }
        }

        foreach ($desired as $queue => $target) {
            $pool =& $this->processes[$name][$queue];
            while ($budget > 0 && $target > count($pool)) {
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
            'QUEEN_SUPERVISOR_TELEMETRY_DIR' => $this->state->telemetryDirectory(),
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
        $this->startedAt[spl_object_id($process)] = microtime(true);
        $this->emit("started {$name}:{$queue} pid={$process->getPid()}\n", 'out');
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
                $exitCode = $process->getExitCode();
                $this->emit("exited {$name}:{$queue} pid={$process->getPid()} code=" . ($exitCode ?? 'unknown') . "\n", $exitCode === 0 ? 'out' : 'err');
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
        $key = "{$name}:{$queue}";
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
        $this->emit("[{$key}] worker crash ({$reason}); restart in {$delay}s (failure {$count})\n", 'err');
    }

    private function resetCrashes(string $name, string $queue): void
    {
        $key = "{$name}:{$queue}";
        unset($this->crashCount[$key], $this->lastCrashAt[$key], $this->restartAfter[$key]);
    }

    private function canRestart(string $name, string $queue): bool
    {
        return microtime(true) >= ($this->restartAfter["{$name}:{$queue}"] ?? 0.0);
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

    private function beginTermination(Process $process, string $label): void
    {
        if (!$process->isRunning()) {
            unset($this->startedAt[spl_object_id($process)]);
            return;
        }
        try {
            $process->signal(SIGTERM);
        } catch (\Throwable $error) {
            $this->emit("[{$label}] SIGTERM failed: {$error->getMessage()}\n", 'err');
        }
        $this->draining[] = [
            'process' => $process,
            'deadline' => microtime(true) + $this->config['shutdown_grace'],
            'label' => $label,
        ];
    }

    private function reapDraining(): void
    {
        $remaining = [];
        foreach ($this->draining as $entry) {
            $process = $entry['process'];
            if (!$process->isRunning()) {
                unset($this->startedAt[spl_object_id($process)]);
                continue;
            }
            $this->discardOutput($process);
            if (microtime(true) >= $entry['deadline']) {
                try {
                    $process->signal(SIGKILL);
                } catch (\Throwable $error) {
                    $this->emit("[{$entry['label']}] SIGKILL failed: {$error->getMessage()}\n", 'err');
                }
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
                    }
                }
            }
        }
        foreach ($this->draining as $entry) {
            if ($entry['process']->isRunning()) {
                $running[spl_object_id($entry['process'])] = $entry['process'];
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
            $running = array_filter($running, fn (Process $process) => $process->isRunning());
            if ($running === [] || microtime(true) >= $deadline) {
                break;
            }
            usleep(50_000);
        } while (true);

        foreach ($running as $process) {
            try {
                $process->signal(SIGKILL);
            } catch (\Throwable) {
                // Best effort after the common graceful deadline expired.
            }
        }
        $this->processes = [];
        $this->draining = [];
        $this->startedAt = [];
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
            || !defined('SIGUSR2')
            || !defined('SIGCONT')
        ) {
            throw new \RuntimeException('The Queen PHP supervisor requires a Unix-like OS, ext-pcntl and symfony/process. Use the Rust engine on unsupported hosts.');
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
        $this->signalAll(SIGUSR2);
    }

    private function resume(): void
    {
        if (!$this->paused) {
            return;
        }
        $this->paused = false;
        $this->signalAll(SIGCONT);
    }

    private function signalAll(int $signal): void
    {
        foreach ($this->processes as $pools) {
            foreach ($pools as $pool) {
                foreach ($pool as $process) {
                    if ($process->isRunning()) {
                        try {
                            $process->signal($signal);
                        } catch (\Throwable) {
                            // The worker may have exited between the check and signal.
                        }
                    }
                }
            }
        }
    }

    private function writeStatus(string $status): void
    {
        $pools = [];
        foreach ($this->processes as $name => $queues) {
            foreach ($queues as $queue => $processes) {
                $pools[$name][$queue] = [
                    'processes' => count($processes),
                    'pids' => array_values(array_filter(array_map(fn (Process $process) => $process->getPid(), $processes))),
                ];
            }
        }
        $this->state->writeStatus([
            'engine' => 'php',
            'state' => $status,
            'draining' => count($this->draining),
            'pools' => $pools,
        ]);
    }

    private function emit(string $buffer, string $type): void
    {
        if ($this->output !== null) {
            ($this->output)($buffer, $type);
        }
    }
}
