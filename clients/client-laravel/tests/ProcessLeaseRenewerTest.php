<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Queue\ProcessLeaseRenewer;

class ProcessLeaseRenewerTest extends TestCase
{
    public function testHelperTracksOneLeaseWithoutNetworkTrafficAndUsesAnIsolatedProcessGroup(): void
    {
        if (!ProcessLeaseRenewer::isSupported()) {
            $this->markTestSkipped('This platform cannot run the lease renewal helper.');
        }

        $renewer = new ProcessLeaseRenewer(
            ['url' => 'http://127.0.0.1:9'],
            leaseSeconds: 120,
            intervalSeconds: 30,
            requestTimeoutSeconds: 1,
            requestBudgetSeconds: 1,
        );

        try {
            $deadline = intdiv(hrtime(true), 1_000_000) + 120_000;
            $renewer->track('lease-one', $deadline);
            $renewer->assertHealthy('lease-one');
            if (function_exists('posix_getpgid')) {
                $process = (new \ReflectionProperty($renewer, 'process'))->getValue($renewer);
                $status = is_resource($process) ? proc_get_status($process) : [];
                $childPid = (int) ($status['pid'] ?? 0);
                $this->assertGreaterThan(0, $childPid);
                $this->assertSame($childPid, posix_getpgid($childPid));
                $this->assertNotSame(posix_getpgid(getmypid()), posix_getpgid($childPid));
            }
            $renewer->forget('lease-one');
            $this->addToAssertionCount(1);
        } finally {
            $renewer->close();
        }
    }

    public function testConcurrentSecondLeaseFailsClosed(): void
    {
        if (!ProcessLeaseRenewer::isSupported()) {
            $this->markTestSkipped('This platform cannot run the lease renewal helper.');
        }

        $renewer = new ProcessLeaseRenewer(
            ['url' => 'http://127.0.0.1:9'],
            leaseSeconds: 120,
            intervalSeconds: 30,
            requestTimeoutSeconds: 1,
            requestBudgetSeconds: 1,
        );
        try {
            $deadline = intdiv(hrtime(true), 1_000_000) + 120_000;
            $renewer->track('lease-one', $deadline);

            $this->expectException(\RuntimeException::class);
            $this->expectExceptionMessage('exactly one live pop lease');
            $renewer->track('lease-two', $deadline);
        } finally {
            $renewer->close();
        }
    }

    public function testUnknownLeaseFailsClosed(): void
    {
        if (!ProcessLeaseRenewer::isSupported()) {
            $this->markTestSkipped('This platform cannot run the lease renewal helper.');
        }

        $renewer = new ProcessLeaseRenewer(
            ['url' => 'http://127.0.0.1:9'],
            leaseSeconds: 120,
            intervalSeconds: 30,
            requestTimeoutSeconds: 1,
            requestBudgetSeconds: 1,
        );

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('is not tracking');
        $renewer->assertHealthy('not-tracked');
    }

    public function testTrackFailsIfChildDiesBeforeConfirmingRegistration(): void
    {
        if (!ProcessLeaseRenewer::isSupported()) {
            $this->markTestSkipped('This platform cannot run the lease renewal helper.');
        }

        $renewer = $this->renewerWithWorkerCode(
            'fgets(STDIN); fwrite(STDOUT, "{\"event\":\"ready\"}\\n"); fflush(STDOUT); fgets(STDIN);',
        );
        try {
            $this->expectException(\RuntimeException::class);
            $this->expectExceptionMessage('did not confirm tracking');
            $renewer->track('lease-dies', intdiv(hrtime(true), 1_000_000) + 120_000);
        } finally {
            $renewer->close();
        }
    }

    public function testTrackFailsIfChildDelaysRegistrationAck(): void
    {
        if (!ProcessLeaseRenewer::isSupported()) {
            $this->markTestSkipped('This platform cannot run the lease renewal helper.');
        }

        $renewer = $this->renewerWithWorkerCode(
            'fgets(STDIN); fwrite(STDOUT, "{\"event\":\"ready\"}\\n"); fflush(STDOUT); fgets(STDIN); usleep(2000000);',
        );
        try {
            $this->expectException(\RuntimeException::class);
            $this->expectExceptionMessage('did not confirm tracking');
            $renewer->track('lease-delayed', intdiv(hrtime(true), 1_000_000) + 120_000);
        } finally {
            $renewer->close();
        }
    }

    public function testTrackRequiresEnoughResidualLeaseForTwoAttemptsAndFencing(): void
    {
        if (!ProcessLeaseRenewer::isSupported()) {
            $this->markTestSkipped('This platform cannot run the lease renewal helper.');
        }

        $renewer = new ProcessLeaseRenewer(
            ['url' => 'http://127.0.0.1:9'],
            leaseSeconds: 120,
            intervalSeconds: 30,
            requestTimeoutSeconds: 1,
            requestBudgetSeconds: 1,
        );
        try {
            // Two 1s attempts + 1s retry + 2s TERM grace + 1s safety = 6s.
            $this->expectException(\RuntimeException::class);
            $this->expectExceptionMessage('reached its renewal deadline');
            $renewer->track('lease-too-old', intdiv(hrtime(true), 1_000_000) + 6_000);
        } finally {
            $renewer->close();
        }
    }

    public function testSigkillAfterTrackFencesTheOwningWorkerWithoutPolling(): void
    {
        if (!ProcessLeaseRenewer::isSupported()) {
            $this->markTestSkipped('This platform cannot run the lease renewal helper.');
        }

        $pipes = [];
        $worker = proc_open(
            [PHP_BINARY, __DIR__ . '/Fixtures/LeaseRenewalWatchdogWorker.php'],
            [
                0 => ['pipe', 'r'],
                1 => ['pipe', 'w'],
                2 => ['pipe', 'w'],
            ],
            $pipes,
            null,
            null,
            ['bypass_shell' => true],
        );
        $this->assertIsResource($worker);
        stream_set_blocking($pipes[1], false);
        stream_set_blocking($pipes[2], false);

        $finalStatus = null;
        try {
            $line = '';
            $readyDeadline = microtime(true) + 5;
            while ($line === '' && microtime(true) < $readyDeadline) {
                $candidate = fgets($pipes[1]);
                if (is_string($candidate)) {
                    $line = $candidate;
                    break;
                }
                usleep(10_000);
            }

            $ready = $line !== '' ? json_decode($line, true) : null;
            $helperPid = is_array($ready) ? (int) ($ready['helper_pid'] ?? 0) : 0;
            $this->assertGreaterThan(0, $helperPid, 'The watchdog fixture did not publish its helper PID.');
            $this->assertTrue(posix_kill($helperPid, SIGKILL), 'Unable to SIGKILL the tracked renewal helper.');

            $deathDeadline = microtime(true) + 3;
            do {
                $finalStatus = proc_get_status($worker);
                if (($finalStatus['running'] ?? false) !== true) {
                    break;
                }
                usleep(10_000);
            } while (microtime(true) < $deathDeadline);

            $diagnostic = trim((string) stream_get_contents($pipes[2]));
            $this->assertFalse(
                $finalStatus['running'] ?? true,
                "The owning worker survived renewal-helper SIGKILL. {$diagnostic}",
            );
            $this->assertTrue($finalStatus['signaled'] ?? false, $diagnostic);
            $this->assertSame(SIGKILL, $finalStatus['termsig'] ?? null, $diagnostic);
        } finally {
            if (is_resource($worker)) {
                $status = proc_get_status($worker);
                if (($status['running'] ?? false) === true) {
                    @proc_terminate($worker, SIGKILL);
                }
            }
            foreach ($pipes as $pipe) {
                if (is_resource($pipe)) {
                    fclose($pipe);
                }
            }
            if (is_resource($worker)) {
                @proc_close($worker);
            }
        }
    }

    public function testWatchdogChainsSigchldForUnrelatedChildrenWithoutReapingThem(): void
    {
        if (!ProcessLeaseRenewer::isSupported()) {
            $this->markTestSkipped('This platform cannot run the lease renewal helper.');
        }

        $pipes = [];
        $worker = proc_open(
            [PHP_BINARY, __DIR__ . '/Fixtures/LeaseRenewalSigchldIsolationWorker.php'],
            [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
            null,
            null,
            ['bypass_shell' => true],
        );
        $this->assertIsResource($worker);
        fclose($pipes[0]);
        $stdout = stream_get_contents($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        $exitCode = proc_close($worker);

        $this->assertSame(0, $exitCode, trim((string) $stderr));
        $result = json_decode((string) $stdout, true, 512, JSON_THROW_ON_ERROR);
        $this->assertGreaterThan(0, $result['unrelated_pid']);
        $this->assertSame($result['unrelated_pid'], $result['observed_pid']);
        $this->assertTrue($result['worker_alive']);
    }

    public function testUnsafeDiagnosticErrorsAreBoundedAndPrintable(): void
    {
        $method = new \ReflectionMethod(\Queen\Laravel\Queue\LeaseRenewalWorker::class, 'boundedError');
        $error = $method->invoke(null, str_repeat("remote\nerror\\\xFF", 16_384));

        $this->assertIsString($error);
        $this->assertLessThanOrEqual(128, strlen($error));
        $this->assertSame(1, preg_match('/^[\x20-\x7E]*$/D', $error));
    }

    private function renewerWithWorkerCode(string $code): ProcessLeaseRenewer
    {
        return new ProcessLeaseRenewer(
            ['url' => 'http://127.0.0.1:9'],
            leaseSeconds: 120,
            intervalSeconds: 30,
            requestTimeoutSeconds: 1,
            requestBudgetSeconds: 1,
            workerCommand: [PHP_BINARY, '-r', $code],
        );
    }
}
