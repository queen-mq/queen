<?php

namespace Queen\Tests;

use Illuminate\Queue\QueueManager;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\PhpSupervisor;
use Queen\Laravel\Supervisor\SupervisorConfiguration;
use ReflectionMethod;
use ReflectionProperty;
use Symfony\Component\Process\Process;

/**
 * How the PHP engine replaces workers that exited, with real processes: a
 * fake `php` that runs until a signal, exiting 0 on SIGTERM (a clean exit, as
 * at queue:restart or --max-time) and 1 on SIGUSR1 (a crash). The Rust engine
 * asserts the same (`a_pool_that_lost_most_workers_at_once_is_refilled_in_one_reconcile`,
 * `a_clean_exit_is_replaced_while_a_restart_probe_runs`).
 */
final class PoolRefillTest extends TestCase
{
    private string $directory = '';

    protected function setUp(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('posix_kill')) {
            $this->markTestSkipped('ext-posix on a Unix host is required.');
        }
        $this->directory = sys_get_temp_dir() . '/queen-refill-' . bin2hex(random_bytes(6));
        mkdir($this->directory, 0700);
        file_put_contents(
            $this->directory . '/php',
            "#!/bin/sh\ntrap 'exit 0' TERM\ntrap 'exit 1' USR1\nwhile :; do sleep 0.05; done\n",
        );
        chmod($this->directory . '/php', 0700);
    }

    protected function tearDown(): void
    {
        foreach (glob($this->directory . '/*') ?: [] as $file) {
            @unlink($file);
        }
        @rmdir($this->directory);
    }

    public function testAPoolThatLostMostOfItsWorkersAtOnceIsRefilledInOneReconcile(): void
    {
        [$supervisor, $options] = $this->supervisor();
        try {
            $this->fill($supervisor, $options, 10);

            foreach (array_slice($this->pool($supervisor), 0, 9) as $worker) {
                posix_kill($worker->getPid(), SIGTERM);
            }
            $this->reapUntil($supervisor, $options, 1);
            $this->reconcile($supervisor, $options, 10);

            $this->assertCount(10, $this->pool($supervisor), 'one balance_max_shift per balance_cooldown would take nine');
        } finally {
            $this->stop($supervisor);
        }
    }

    public function testACleanExitIsReplacedWhileARestartProbeRuns(): void
    {
        [$supervisor, $options] = $this->supervisor(['restart_backoff' => 0]);
        try {
            $this->fill($supervisor, $options, 5);

            // A crash: the circuit backs off, and its replacement is the probe.
            posix_kill($this->pool($supervisor)[0]->getPid(), SIGUSR1);
            $this->reapUntil($supervisor, $options, 4);
            $this->reconcile($supervisor, $options, 5);
            $this->assertCount(5, $this->pool($supervisor));
            $this->assertSame(['probe'], array_values($this->property($supervisor, 'restartPhase')));

            // Two workers stop at --max-time while the probe runs.
            $probes = $this->property($supervisor, 'restartProbes');
            $siblings = array_values(array_filter(
                $this->pool($supervisor),
                static fn (Process $worker): bool => !isset($probes[spl_object_id($worker)]),
            ));
            posix_kill($siblings[0]->getPid(), SIGTERM);
            posix_kill($siblings[1]->getPid(), SIGTERM);
            $this->reapUntil($supervisor, $options, 3);
            $this->reconcile($supervisor, $options, 5);

            $this->assertCount(5, $this->pool($supervisor), 'clean exits wait for no circuit');
            $this->assertSame(['probe'], array_values($this->property($supervisor, 'restartPhase')));
        } finally {
            $this->stop($supervisor);
        }
    }

    public function testACrashStillWaitsForTheCircuit(): void
    {
        [$supervisor, $options] = $this->supervisor(['restart_backoff' => 30]);
        try {
            $this->fill($supervisor, $options, 3);

            posix_kill($this->pool($supervisor)[0]->getPid(), SIGUSR1);
            $this->reapUntil($supervisor, $options, 2);
            $this->reconcile($supervisor, $options, 3);

            $this->assertCount(2, $this->pool($supervisor), 'a crash is replaced after the backoff, by a probe');
        } finally {
            $this->stop($supervisor);
        }
    }

    /**
     * @param array<string, mixed> $overrides
     * @return array{PhpSupervisor, array<string, mixed>}
     */
    private function supervisor(array $overrides = []): array
    {
        $config = SupervisorConfiguration::resolve([
            'url' => 'http://queen.test:6632',
            'supervisor' => ['supervisors' => ['orders' => [
                'queues' => ['high'],
                'balance' => 'auto',
                'min_processes' => 1,
                'max_processes' => 10,
                ...$overrides,
            ]]],
        ], $this->directory);
        $config['php_binary'] = $this->directory . '/php';
        $config['state_directory'] = $this->directory . '/state';

        return [
            new PhpSupervisor($this->createStub(QueueManager::class), $config, output: static function (): void {
            }),
            $config['supervisors']['orders'],
        ];
    }

    /** @param array<string, mixed> $options */
    private function fill(PhpSupervisor $supervisor, array $options, int $workers): void
    {
        for ($i = 0; $i < $workers && count($this->pool($supervisor)) < $workers; $i++) {
            $this->reconcile($supervisor, $options, $workers);
        }
        $this->assertCount($workers, $this->pool($supervisor));
        // Every worker is running its loop, so it handles the signals.
        usleep(300_000);
    }

    /** @param array<string, mixed> $options */
    private function reconcile(PhpSupervisor $supervisor, array $options, int $target): void
    {
        (new ReflectionMethod(PhpSupervisor::class, 'reconcile'))->invoke($supervisor, 'orders', $options, ['high' => $target]);
    }

    /** @param array<string, mixed> $options */
    private function reapUntil(PhpSupervisor $supervisor, array $options, int $left): void
    {
        $reap = new ReflectionMethod(PhpSupervisor::class, 'reap');
        $deadline = microtime(true) + 10;
        do {
            $reap->invoke($supervisor, 'orders', $options);
            if (count($this->pool($supervisor)) <= $left) {
                return;
            }
            usleep(20_000);
        } while (microtime(true) < $deadline);
        $this->fail('the workers did not exit');
    }

    /** @return list<Process> */
    private function pool(PhpSupervisor $supervisor): array
    {
        return $this->property($supervisor, 'processes')['orders']['high'] ?? [];
    }

    private function stop(PhpSupervisor $supervisor): void
    {
        foreach ($this->pool($supervisor) as $worker) {
            $worker->stop(0, SIGKILL);
        }
    }

    private function property(object $object, string $name): mixed
    {
        return (new ReflectionProperty($object, $name))->getValue($object);
    }
}
