<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\Prefork\ForkSafety;

/**
 * The fork server refuses to fork with a second thread and warns about
 * sockets its boot left open; both come from /proc, faked here.
 */
final class ForkSafetyTest extends TestCase
{
    private string $root;

    protected function setUp(): void
    {
        $this->root = sys_get_temp_dir() . '/queen-fork-safety-' . bin2hex(random_bytes(6));
        mkdir($this->root . '/4242/task/4242', 0700, true);
        mkdir($this->root . '/4242/fd', 0700, true);
        file_put_contents($this->root . '/4242/task/4242/comm', "php\n");
        symlink('4242', $this->root . '/self');
    }

    protected function tearDown(): void
    {
        exec('rm -rf ' . escapeshellarg($this->root));
    }

    public function testAServerWithOneThreadHasNoOtherThreads(): void
    {
        $this->assertSame([], $this->safety()->otherThreads());
    }

    public function testTheOtherThreadsAreNamed(): void
    {
        $this->thread(4243, 'grpc_global_tim');
        $this->thread(4244, '');

        $this->assertSame(['4244', 'grpc_global_tim'], $this->safety()->otherThreads());
    }

    public function testAThreadThatEndsWithinTheGraceIsNotReported(): void
    {
        $this->thread(4243, 'getaddrinfo');
        $task = $this->root . '/4242/task/4243';
        exec('(sleep 0.3; rm -rf ' . escapeshellarg($task) . ') >/dev/null 2>&1 &');

        $this->assertSame([], $this->safety()->threadsThatStay(5));
    }

    public function testAThreadThatStaysIsReportedAfterTheGrace(): void
    {
        $this->thread(4243, 'rdk:main');

        $started = microtime(true);
        $this->assertSame(['rdk:main'], $this->safety()->threadsThatStay(0.3));
        $this->assertGreaterThanOrEqual(0.3, microtime(true) - $started);
    }

    public function testWithoutProcNothingIsKnown(): void
    {
        $safety = new ForkSafety($this->root . '/missing');

        $this->assertNull($safety->otherThreads());
        $this->assertNull($safety->threadsThatStay(1));
        $this->assertSame([], $safety->sockets());
    }

    public function testOnlySocketsAboveTheProtocolDescriptorsAreReported(): void
    {
        foreach ([0 => 'pipe:[1]', 1 => 'socket:[2]', 3 => 'pipe:[3]', 4 => '/var/log/app.log', 7 => 'socket:[77]', 12 => 'socket:[78]'] as $fd => $target) {
            symlink($target, $this->root . "/4242/fd/{$fd}");
        }

        $this->assertSame([7, 12], $this->safety()->sockets());
    }

    private function safety(): ForkSafety
    {
        return new ForkSafety($this->root . '/self');
    }

    private function thread(int $tid, string $name): void
    {
        mkdir($this->root . "/4242/task/{$tid}", 0700);
        file_put_contents($this->root . "/4242/task/{$tid}/comm", $name === '' ? '' : "{$name}\n");
    }
}
