<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Queue\ProcessLeaseRenewer;
use Queen\Laravel\Queue\QueenConnector;
use Queen\Laravel\Queue\SupervisorLeaseRenewer;

class SupervisorLeaseRenewerTest extends TestCase
{
    /**
     * A stand-in for the supervisor master: it accepts one worker on the
     * socket, logs every command and answers by mode.
     */
    private const FAKE_MASTER = <<<'PHP'
[$script, $path, $log, $mode] = $argv;
$server = stream_socket_server('unix://' . $path, $code, $message);
if ($server === false) { fwrite(STDERR, $message); exit(1); }
$connection = stream_socket_accept($server, 10);
if ($connection === false) { exit(1); }
$send = static fn (array $event) => fwrite($connection, json_encode($event) . "\n");
while (($line = fgets($connection)) !== false) {
    file_put_contents($log, $line, FILE_APPEND);
    $command = json_decode($line, true);
    switch ($command['command'] ?? null) {
        case 'init':
            $send($mode === 'refuse'
                ? ['event' => 'startup_failed', 'error' => 'invalid renewal timing']
                : ['event' => 'ready']);
            break;
        case 'track':
            $send(['event' => 'tracked', 'lease_id' => $command['lease_id']]);
            if ($mode === 'unsafe') {
                $send(['event' => 'unsafe', 'lease_id' => $command['lease_id'], 'error' => 'lease not found']);
            }
            if ($mode === 'hangup') {
                fclose($connection);
                exit(0);
            }
            break;
        case 'shutdown':
            exit(0);
    }
}
PHP;

    private string $directory;

    /** @var resource|null */
    private $master = null;

    protected function setUp(): void
    {
        parent::setUp();
        if (!SupervisorLeaseRenewer::isSupported() || !function_exists('proc_open')) {
            $this->markTestSkipped('This platform cannot run the supervisor lease client.');
        }
        // Short: a Unix socket path is limited to about 100 bytes.
        $this->directory = sys_get_temp_dir() . '/qslr-' . bin2hex(random_bytes(4));
        mkdir($this->directory, 0700);
    }

    protected function tearDown(): void
    {
        if (is_resource($this->master)) {
            proc_terminate($this->master);
            proc_close($this->master);
        }
        foreach (glob($this->directory . '/*') ?: [] as $file) {
            @unlink($file);
        }
        @rmdir($this->directory);
        parent::tearDown();
    }

    public function testTheWorkerRegistersItsTimingAndClientThenTracksForgetsAndShutsDown(): void
    {
        $socket = $this->startMaster('ok');
        $renewer = new SupervisorLeaseRenewer(
            $socket,
            [
                'urls' => ['http://queen-a:6632', 'http://queen-b:6632'],
                'bearerToken' => 'secret',
                'headers' => ['X-Tenant' => 'a', 'X-Many' => ['1', '2']],
                'timeoutMillis' => 30_000,
                'retryAttempts' => 3,
            ],
            leaseSeconds: 120,
            intervalSeconds: 30,
            requestTimeoutSeconds: 2,
            requestBudgetSeconds: 4,
        );

        $renewer->track('lease-one', $this->monotonicMillis() + 120_000);
        $renewer->assertHealthy('lease-one');
        $renewer->forget('lease-one');
        $renewer->close();
        $this->waitForMasterExit();

        $commands = $this->commands();
        $this->assertSame(['init', 'track', 'forget', 'shutdown'], array_column($commands, 'command'));
        $init = $commands[0];
        $this->assertSame([
            'urls' => ['http://queen-a:6632', 'http://queen-b:6632'],
            'bearerToken' => 'secret',
            'headers' => ['X-Tenant' => 'a', 'X-Many' => '1, 2'],
            'timeoutMillis' => 2_000,
        ], $init['client']);
        $this->assertSame(120, $init['lease_seconds']);
        $this->assertSame(30_000, $init['interval_millis']);
        $this->assertSame(4_000, $init['request_budget_millis']);
        $this->assertSame(2_000, $init['kill_grace_millis']);
        $this->assertSame(1_000, $init['safety_margin_millis']);
        $this->assertEqualsWithDelta($this->monotonicMillis(), $init['monotonic_millis'], 10_000);
        $this->assertSame('lease-one', $commands[1]['lease_id']);
    }

    public function testEmptyHeadersAreSentAsAMap(): void
    {
        $socket = $this->startMaster('ok');
        $renewer = new SupervisorLeaseRenewer($socket, ['url' => 'http://queen:6632'], 120, 30, 1, 1);
        $renewer->close();
        $this->waitForMasterExit();

        $line = strtok((string) file_get_contents($this->directory . '/log'), "\n");
        $this->assertStringContainsString('"headers":{}', (string) $line);
        $this->assertSame(['http://queen:6632'], $this->commands()[0]['client']['urls']);
    }

    public function testARefusedWorkerFailsToConstruct(): void
    {
        $socket = $this->startMaster('refuse');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('refused this worker: invalid renewal timing');
        new SupervisorLeaseRenewer($socket, ['url' => 'http://queen:6632'], 120, 30, 1, 1);
    }

    public function testAnUnsafeLeaseFailsClosed(): void
    {
        $socket = $this->startMaster('unsafe');
        $renewer = new SupervisorLeaseRenewer($socket, ['url' => 'http://queen:6632'], 120, 30, 1, 1);
        try {
            $renewer->track('doomed', $this->monotonicMillis() + 120_000);
            $this->fail('An unsafe lease was reported healthy.');
        } catch (\RuntimeException $exception) {
            $this->assertStringContainsString('became unsafe for [doomed]: lease not found', $exception->getMessage());
        } finally {
            $renewer->close();
        }
    }

    public function testAConnectionClosedByTheMasterFailsClosed(): void
    {
        $socket = $this->startMaster('hangup');
        $renewer = new SupervisorLeaseRenewer($socket, ['url' => 'http://queen:6632'], 120, 30, 1, 1);

        // Whether track() or a later check sees the hang-up first is timing.
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('stopped unexpectedly');
        $renewer->track('held', $this->monotonicMillis() + 120_000);
        $this->waitForMasterExit();
        $renewer->assertHealthy('held');
    }

    public function testTheConnectorFallsBackToAHelperWhenTheServiceIsUnreachable(): void
    {
        $previous = getenv('QUEEN_SUPERVISOR_LEASE_SOCKET');
        $previousLog = ini_set('error_log', '/dev/null');
        putenv('QUEEN_SUPERVISOR_LEASE_SOCKET=' . $this->directory . '/missing.sock');
        try {
            $queue = (new QueenConnector())->connect([
                'url' => 'http://127.0.0.1:9',
                'queue' => 'default',
                'lease_renewal' => true,
                'retry_after' => 120,
                'lease_renewal_interval' => 30,
                'lease_renewal_timeout' => 5,
            ]);
            $lazy = (new \ReflectionProperty($queue, 'leaseRenewer'))->getValue($queue);
            $factory = (new \ReflectionProperty($lazy, 'factory'))->getValue($lazy);

            $this->assertInstanceOf(ProcessLeaseRenewer::class, $factory());
        } finally {
            putenv($previous === false ? 'QUEEN_SUPERVISOR_LEASE_SOCKET' : "QUEEN_SUPERVISOR_LEASE_SOCKET={$previous}");
            ini_set('error_log', $previousLog === false ? '' : $previousLog);
        }
    }

    private function startMaster(string $mode): string
    {
        $socket = $this->directory . '/lease.sock';
        $pipes = [];
        $this->master = proc_open(
            [PHP_BINARY, '-r', self::FAKE_MASTER, '--', $socket, $this->directory . '/log', $mode],
            [1 => ['file', '/dev/null', 'w'], 2 => ['file', $this->directory . '/stderr', 'w']],
            $pipes,
        );
        $deadline = microtime(true) + 5;
        while (!file_exists($socket)) {
            if (microtime(true) > $deadline) {
                $this->fail('The fake master did not listen: ' . @file_get_contents($this->directory . '/stderr'));
            }
            usleep(10_000);
        }

        return $socket;
    }

    private function waitForMasterExit(): void
    {
        $deadline = microtime(true) + 5;
        while ((proc_get_status($this->master)['running'] ?? false) === true) {
            if (microtime(true) > $deadline) {
                $this->fail('The fake master did not exit.');
            }
            usleep(10_000);
        }
    }

    /** @return list<array> */
    private function commands(): array
    {
        $lines = file($this->directory . '/log', FILE_IGNORE_NEW_LINES) ?: [];

        return array_map(static fn (string $line): array => json_decode($line, true), $lines);
    }

    private function monotonicMillis(): int
    {
        return intdiv(hrtime(true), 1_000_000);
    }
}
