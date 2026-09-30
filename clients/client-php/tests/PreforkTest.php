<?php

namespace Queen\Tests;

use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\Prefork\ForkedProcess;
use Queen\Laravel\Supervisor\Prefork\ForkServerClient;

/**
 * Real forks through the fork-server protocol the Rust engine speaks too.
 */
final class PreforkTest extends TestCase
{
    private ?ForkServerClient $server = null;

    /** @var list<string> */
    private array $reports = [];

    protected function setUp(): void
    {
        if (!function_exists('pcntl_fork') || !function_exists('posix_setsid')) {
            $this->markTestSkipped('ext-pcntl and ext-posix are required.');
        }
        $environment = getenv();
        $environment['QUEEN_TEST_REMOVED'] = 'inherited';
        $this->server = ForkServerClient::start(
            [PHP_BINARY, __DIR__ . '/Fixtures/Prefork/fork_server.php'],
            __DIR__,
            $environment,
            10,
        );
    }

    protected function tearDown(): void
    {
        $this->server?->close(5);
        foreach ($this->reports as $report) {
            @unlink($report);
        }
    }

    public function testAWorkerGetsItsArgumentsAndEnvironmentAndLeadsItsOwnSession(): void
    {
        $report = $this->report();

        $worker = new ForkedProcess($this->server, $this->server->fork(
            ['exit', $report, '3'],
            ['QUEEN_TEST_VALUE' => 'per-worker', 'QUEEN_TEST_REMOVED' => null],
            5,
        ));

        $this->waitUntil(fn (): bool => !$worker->isRunning());
        $this->assertSame(3, $worker->getExitCode());
        $this->assertNull($worker->getPid());
        $seen = json_decode((string) file_get_contents($report), true);
        $this->assertSame(['exit', $report, '3'], $seen['argv']);
        $this->assertSame('per-worker', $seen['value']);
        $this->assertFalse($seen['removed']);
        $this->assertSame($seen['pid'], $seen['pgid']);
    }

    public function testASignalledWorkerReportsTheShellExitCode(): void
    {
        $report = $this->report();
        $worker = new ForkedProcess($this->server, $this->server->fork(['sleep', $report], [], 5));
        $this->waitUntil(fn (): bool => is_file($report) && filesize($report) > 0);

        $this->assertTrue($worker->isRunning());
        $this->assertIsInt($worker->getPid());
        $worker->signal(SIGTERM);

        $this->waitUntil(fn (): bool => !$worker->isRunning());
        $this->assertSame(128 + SIGTERM, $worker->getExitCode());
    }

    public function testClosingTheServerFencesTheWorkersLeft(): void
    {
        $report = $this->report();
        $pid = $this->server->fork(['sleep', $report], [], 5);
        $this->waitUntil(fn (): bool => is_file($report) && filesize($report) > 0);
        $this->assertTrue(posix_kill($pid, 0));

        $this->server->close(5);
        $this->server = null;

        $this->waitUntil(fn (): bool => !@posix_kill($pid, 0));
        $this->addToAssertionCount(1);
    }

    public function testASignalWhileTheMasterLivesLeavesTheWorkersToTheDrain(): void
    {
        $report = $this->report();
        $pid = $this->server->fork(['sleep', $report], [], 5);
        $this->waitUntil(fn (): bool => is_file($report) && filesize($report) > 0);

        // systemd's control-group kill, or a terminal's Ctrl-C.
        posix_kill($this->server->pid(), SIGTERM);
        posix_kill($this->server->pid(), SIGINT);
        usleep(300_000);

        $this->assertTrue($this->server->isAlive());
        $this->assertTrue(posix_kill($pid, 0));
        $this->assertNull($this->server->exitStatus($pid));
        $worker = new ForkedProcess($this->server, $this->server->fork(['exit', $this->report(), '0'], [], 5));
        $this->waitUntil(fn (): bool => !$worker->isRunning());
        $this->assertSame(0, $worker->getExitCode());
    }

    public function testAWorkerForkedAfterItsRequestTimedOutIsStopped(): void
    {
        // A loaded host can deliver the reply within the zero timeout: stop
        // that worker and ask again.
        for ($attempt = 1; ; ++$attempt) {
            $report = $this->report();
            try {
                $early = $this->server->fork(['sleep', $report], [], 0);
            } catch (\RuntimeException $error) {
                $this->assertStringContainsString('did not answer in time', $error->getMessage());
                break;
            }
            posix_kill($early, SIGKILL);
            $this->assertLessThan(5, $attempt, 'The fork never timed out.');
        }
        // Nothing reads the server meanwhile, so the late worker runs until
        // the client sees its reply.
        $this->waitUntil(fn (): bool => is_file($report) && filesize($report) > 0);
        $stray = json_decode((string) file_get_contents($report), true)['pid'];
        $this->assertTrue(posix_kill($stray, 0));

        $this->waitUntil(fn (): bool => $this->server->exitStatus($stray) === null && !@posix_kill($stray, 0));
        $strays = new \ReflectionProperty(ForkServerClient::class, 'strays');
        $this->waitUntil(fn (): bool => $this->server->exitStatus($stray) === null && $strays->getValue($this->server) === []);
        $worker = new ForkedProcess($this->server, $this->server->fork(['exit', $this->report(), '0'], [], 5));
        $this->waitUntil(fn (): bool => !$worker->isRunning());
        $this->assertSame(0, $worker->getExitCode());
    }

    public function testAMalformedRequestIsRefusedAndTheServerKeepsServing(): void
    {
        $reflection = new \ReflectionProperty(ForkServerClient::class, 'commands');
        fwrite($reflection->getValue($this->server), "{\"fork\": {\"id\": 99, \"argv\": [1]}}\n");

        $report = $this->report();
        $worker = new ForkedProcess($this->server, $this->server->fork(['exit', $report, '0'], [], 5));

        $this->waitUntil(fn (): bool => !$worker->isRunning());
        $this->assertSame(0, $worker->getExitCode());
    }

    public function testTheResolverExportsPreforkOnlyWhenEnabled(): void
    {
        $queen = fn (array $supervisor): array => ['url' => 'http://queen.test:6632', 'supervisor' => $supervisor];

        $this->assertArrayNotHasKey('prefork', \Queen\Laravel\Supervisor\SupervisorConfiguration::resolve($queen([]), '/app'));
        $this->assertTrue(\Queen\Laravel\Supervisor\SupervisorConfiguration::resolve($queen(['prefork' => true]), '/app')['prefork']);
    }

    private function report(): string
    {
        return $this->reports[] = sys_get_temp_dir() . '/queen-prefork-' . bin2hex(random_bytes(6)) . '.json';
    }

    private function waitUntil(\Closure $condition): void
    {
        $deadline = microtime(true) + 10;
        while (!$condition()) {
            if (microtime(true) > $deadline) {
                $this->fail('Timed out.');
            }
            usleep(20_000);
        }
    }
}
