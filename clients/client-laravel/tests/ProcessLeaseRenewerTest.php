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
