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

    /**
     * A job may fork, as Laravel's fork concurrency driver does. The child
     * inherits the renewer: neither its exit nor its own subprocesses may stop
     * the parent's helper, and the parent's watchdog must not fence the child.
     */
    #[\PHPUnit\Framework\Attributes\TestWith(['exit'])]
    #[\PHPUnit\Framework\Attributes\TestWith(['subprocess'])]
    public function testAForkedChildNeitherStopsTheHelperNorIsFencedByItsWatchdog(string $mode): void
    {
        if (!ProcessLeaseRenewer::isSupported() || !function_exists('pcntl_fork')) {
            $this->markTestSkipped('This platform cannot run the lease renewal helper.');
        }

        $pipes = [];
        $worker = proc_open(
            [PHP_BINARY, __DIR__ . '/Fixtures/LeaseRenewalForkedChildWorker.php', $mode],
            [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
            null,
            null,
            ['bypass_shell' => true],
        );
        $this->assertIsResource($worker);
        fclose($pipes[0]);
        $stdout = stream_get_contents($pipes[1]);
        $stderr = trim((string) stream_get_contents($pipes[2]));
        fclose($pipes[1]);
        fclose($pipes[2]);
        $status = proc_get_status($worker);
        $exitCode = proc_close($worker);

        $this->assertFalse($status['signaled'] ?? false, "The worker was killed by its own watchdog. {$stderr}");
        $this->assertSame(0, $exitCode, $stderr);
        $result = json_decode((string) $stdout, true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame(0, $result['child_exit'], "The forked child was fenced. {$stderr}");
        $this->assertNull($result['child_signal']);
    }

    public function testTheHelperHandsBackTheJournalOfAWorkerThatDiedHoldingItsLease(): void
    {
        if (!ProcessLeaseRenewer::isSupported()) {
            $this->markTestSkipped('This platform cannot run the lease renewal helper.');
        }
        [$url, $log, $broker] = $this->startRecordingBroker();
        try {
            [$status, $prefix] = $this->runHandBackWorker($url, 'crash');
            $this->assertSame(SIGKILL, $status['termsig'] ?? null);

            $transactions = $this->waitForTransactions($log, 1);
            $this->assertCount(1, $transactions);
            $this->assertSame('Bearer worker-secret', $transactions[0]['authorization']);
            $body = json_decode($transactions[0]['body'], true, 512, JSON_THROW_ON_ERROR);
            $this->assertSame(['lease-x'], $body['requiredLeases']);
            $this->assertSame(['ack', 'push', 'ack', 'push'], array_column($body['operations'], 'type'));
            $this->assertSame(['t1', 't2'], array_values(array_filter(array_column($body['operations'], 'transactionId'))));
            // The running job counts its run, the unstarted one does not.
            $this->assertSame(1, $body['operations'][1]['items'][0]['payload']['_queen']['attempts']);
            $this->assertSame(0, $body['operations'][3]['items'][0]['payload']['_queen']['attempts']);
            $this->waitUntil(static fn (): bool => !is_dir(dirname($prefix)), 'the journal removed');
        } finally {
            proc_terminate($broker);
            proc_close($broker);
            @unlink($log);
        }
    }

    public function testAWorkerThatClosesItsRenewerHandsBackNothing(): void
    {
        if (!ProcessLeaseRenewer::isSupported()) {
            $this->markTestSkipped('This platform cannot run the lease renewal helper.');
        }
        [$url, $log, $broker] = $this->startRecordingBroker();
        try {
            [$status, $prefix] = $this->runHandBackWorker($url, 'close');
            $this->assertSame(0, $status['exitcode'] ?? null);

            $this->assertSame([], $this->waitForTransactions($log, 1, 1.5));
            $this->assertDirectoryDoesNotExist(dirname($prefix));
        } finally {
            proc_terminate($broker);
            proc_close($broker);
            @unlink($log);
        }
    }

    public function testUnsafeDiagnosticErrorsAreBoundedAndPrintable(): void
    {
        $method = new \ReflectionMethod(\Queen\Laravel\Queue\LeaseRenewalWorker::class, 'boundedError');
        $error = $method->invoke(null, str_repeat("remote\nerror\\\xFF", 16_384));

        $this->assertIsString($error);
        $this->assertLessThanOrEqual(128, strlen($error));
        $this->assertSame(1, preg_match('/^[\x20-\x7E]*$/D', $error));
    }

    /**
     * Composer links a path repository's package into vendor/ with a symlink,
     * and PHP reports Queen.php by the link's target, outside the application:
     * the helper must load the application's autoloader, the one that loaded
     * this package, not one found by walking up from the package's own files.
     */
    public function testTheHelperLoadsTheAutoloaderThatLoadedThePackage(): void
    {
        $vendor = sys_get_temp_dir() . '/queen-app-' . bin2hex(random_bytes(6)) . '/vendor';
        mkdir($vendor, 0700, true);
        file_put_contents("{$vendor}/autoload.php", "<?php\n");
        $loader = new \Composer\Autoload\ClassLoader($vendor);
        $loader->addPsr4('Queen\\', dirname(__DIR__) . '/src');
        $loader->register(true);

        try {
            $renewer = (new \ReflectionClass(ProcessLeaseRenewer::class))->newInstanceWithoutConstructor();
            $autoload = (new \ReflectionMethod($renewer, 'findAutoload'))->invoke($renewer);

            $this->assertSame("{$vendor}/autoload.php", $autoload);
        } finally {
            $loader->unregister();
            unlink("{$vendor}/autoload.php");
            rmdir($vendor);
            rmdir(dirname($vendor));
        }
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

    /** @return array{0: string, 1: string, 2: resource} the URL, its request log and the server */
    private function startRecordingBroker(): array
    {
        $probe = stream_socket_server('tcp://127.0.0.1:0');
        $this->assertIsResource($probe);
        $address = (string) stream_socket_get_name($probe, false);
        fclose($probe);
        $log = tempnam(sys_get_temp_dir(), 'qrb');
        $server = proc_open(
            [PHP_BINARY, '-S', $address, __DIR__ . '/Fixtures/RecordingBroker.php'],
            [0 => ['file', '/dev/null', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']],
            $pipes,
            null,
            ['QUEEN_TEST_BROKER_LOG' => $log],
        );
        $this->assertIsResource($server);
        $this->waitUntil(static function () use ($address): bool {
            $connection = @stream_socket_client("tcp://{$address}", $code, $message, 0.1);
            if (!is_resource($connection)) {
                return false;
            }
            fclose($connection);
            return true;
        }, 'the recording broker');

        return ["http://{$address}", $log, $server];
    }

    /** @return array{0: array, 1: string} the worker's final status and its journal prefix */
    private function runHandBackWorker(string $url, string $mode): array
    {
        $worker = proc_open(
            [PHP_BINARY, __DIR__ . '/Fixtures/LeaseRenewalHandBackWorker.php', $url, $mode],
            [0 => ['file', '/dev/null', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
        );
        $this->assertIsResource($worker);
        $output = (string) stream_get_contents($pipes[1]);
        $errors = (string) stream_get_contents($pipes[2]);
        do {
            $status = proc_get_status($worker);
            usleep(10_000);
        } while ($status['running']);
        proc_close($worker);
        $prefix = json_decode(trim($output), true)['journal'] ?? null;
        $this->assertIsString($prefix, "The worker published no journal: {$errors}");

        return [$status, $prefix];
    }

    /** @return list<array<string, mixed>> the transactions the broker received */
    private function waitForTransactions(string $log, int $count, float $seconds = 5.0): array
    {
        $deadline = microtime(true) + $seconds;
        do {
            $requests = array_map(
                static fn (string $line): array => json_decode($line, true, 512, JSON_THROW_ON_ERROR),
                array_filter(explode("\n", (string) file_get_contents($log))),
            );
            $transactions = array_values(array_filter(
                $requests,
                static fn (array $request): bool => $request['method'] === 'POST' && $request['path'] === '/api/v1/transaction',
            ));
            if (count($transactions) >= $count) {
                return $transactions;
            }
            usleep(20_000);
        } while (microtime(true) < $deadline);

        return $transactions;
    }

    private function waitUntil(callable $done, string $what): void
    {
        $deadline = microtime(true) + 5;
        while (!$done()) {
            $this->assertLessThan($deadline, microtime(true), "Timed out waiting for {$what}.");
            usleep(20_000);
        }
    }
}
