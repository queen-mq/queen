<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\OrphanReaper;

final class OrphanReaperTest extends TestCase
{
    protected function setUp(): void
    {
        if (!is_dir('/proc/self') || !function_exists('pcntl_fork') || !function_exists('pcntl_exec')) {
            $this->markTestSkipped('The orphan reaper reads Linux /proc and needs pcntl.');
        }
    }

    public function testAZombieChildTheMasterDoesNotTrackIsReaped(): void
    {
        $pid = $this->zombie(0);

        $this->assertSame([$pid], (new OrphanReaper(init: true))->reap([]));
        $this->assertDirectoryDoesNotExist("/proc/{$pid}");
    }

    public function testATrackedChildKeepsItsExitStatusForTheSupervisor(): void
    {
        $pid = $this->zombie(7);

        $this->assertSame([], (new OrphanReaper(init: true))->reap([$pid]));
        $this->assertSame($pid, pcntl_waitpid($pid, $status, WNOHANG));
        $this->assertSame(7, pcntl_wexitstatus($status));
    }

    public function testAMasterThatIsNotPidOneLeavesOrphansToInit(): void
    {
        $pid = $this->zombie(0);

        $this->assertSame([], (new OrphanReaper(init: false))->reap([]));
        // The test runner is not PID 1.
        $this->assertSame([], (new OrphanReaper())->reap([]));
        $this->assertSame($pid, pcntl_waitpid($pid, $status, WNOHANG));
    }

    /** A child that has exited with $code and that nothing has waited for. */
    private function zombie(int $code): int
    {
        $pid = pcntl_fork();
        if ($pid === 0) {
            // No PHP shutdown in the child: the runner's handlers stay the parent's.
            pcntl_exec('/bin/sh', ['-c', "exit {$code}"]);
            posix_kill(getmypid(), SIGKILL);
        }
        $this->assertGreaterThan(0, $pid);
        $deadline = microtime(true) + 5;
        do {
            $stat = (string) @file_get_contents("/proc/{$pid}/stat");
            if (str_contains(substr($stat, (int) strrpos($stat, ')')), ') Z ')) {
                return $pid;
            }
            usleep(10_000);
        } while (microtime(true) < $deadline);
        $this->fail("child {$pid} did not become a zombie");
    }
}
