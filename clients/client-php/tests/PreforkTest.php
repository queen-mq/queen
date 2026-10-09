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

    public function testAForkedWorkerShowsTheCommandLineOfASpawnedOne(): void
    {
        $report = $this->report();
        $pid = $this->server->fork(['sleep', $report], [], 5);
        $this->waitUntil(fn (): bool => is_file($report) && filesize($report) > 0);

        $command = is_readable("/proc/{$pid}/cmdline")
            ? str_replace("\0", ' ', (string) file_get_contents("/proc/{$pid}/cmdline"))
            : (string) shell_exec('ps -o command= -p ' . $pid);
        if (trim($command) === '') {
            $this->markTestSkipped('Neither /proc nor ps shows process command lines here.');
        }
        $this->assertStringContainsString('queue:work sleep ' . $report, $command);
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

    /**
     * queen:fork-server runs under Laravel's error handler, which throws the
     * warning pcntl_fork() raises when it fails (a pids or nproc limit, no
     * memory). That must be a failed fork, not the end of the server.
     */
    public function testAFailedForkIsReportedAndTheServerKeepsServing(): void
    {
        $server = ForkServerClient::start(
            [PHP_BINARY, __DIR__ . '/Fixtures/Prefork/fork_server_without_processes.php'],
            __DIR__,
            getenv(),
            10,
        );
        try {
            foreach ([1, 2] as $attempt) {
                try {
                    $server->fork(['exit'], [], 5);
                } catch (\RuntimeException $failure) {
                    $this->assertStringContainsString('could not fork', $failure->getMessage(), "attempt {$attempt}");
                    continue;
                }
                $this->markTestSkipped('This user can fork past RLIMIT_NPROC, as root can.');
            }
            $this->assertTrue($server->isAlive(), 'the server died with its failed fork');
        } finally {
            $server->close(5);
        }
    }

    /**
     * Pools run quiet by default: queue:work prints two lines per job
     * otherwise. Symfony applies --quiet and -v in Application::run(), which
     * a forked worker never goes through.
     */
    #[\PHPUnit\Framework\Attributes\TestWith([['--quiet'], true, false])]
    #[\PHPUnit\Framework\Attributes\TestWith([['-v'], false, true])]
    #[\PHPUnit\Framework\Attributes\TestWith([[], false, false])]
    public function testAForkedWorkerKeepsTheVerbosityItWasGiven(array $flags, bool $quiet, bool $verbose): void
    {
        $work = new class extends \Symfony\Component\Console\Command\Command {
            /** @var array{quiet: bool, verbose: bool, connection: ?string}|null */
            public ?array $seen = null;

            protected function configure(): void
            {
                $this->setName('queue:work')
                    ->addArgument('connection')
                    ->addOption('queue', null, \Symfony\Component\Console\Input\InputOption::VALUE_REQUIRED);
            }

            protected function execute(
                \Symfony\Component\Console\Input\InputInterface $input,
                \Symfony\Component\Console\Output\OutputInterface $output,
            ): int {
                $this->seen = [
                    'quiet' => $output->isQuiet(),
                    'verbose' => $output->isVerbose(),
                    'connection' => $input->getArgument('connection'),
                ];

                return 0;
            }
        };
        $work->setApplication(new \Symfony\Component\Console\Application());
        $server = new \Queen\Laravel\Commands\ForkServerCommand();
        $server->setOutput(new \Illuminate\Console\OutputStyle(
            new \Symfony\Component\Console\Input\ArrayInput([]),
            new \Symfony\Component\Console\Output\BufferedOutput(),
        ));

        $code = (new \ReflectionMethod($server, 'runWorker'))->invoke($server, $work, ['queen', '--queue=high', ...$flags]);

        $this->assertSame(0, $code);
        $this->assertSame(['quiet' => $quiet, 'verbose' => $verbose, 'connection' => 'queen'], $work->seen);
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

    public function testAPoolCanTurnPreforkOffWhileTheOthersStayForked(): void
    {
        $resolved = $this->resolve([
            'prefork' => true,
            'supervisors' => ['kafka' => ['queues' => ['kafka'], 'prefork' => false], 'default' => []],
        ]);

        $this->assertTrue($resolved['prefork']);
        $this->assertFalse($resolved['supervisors']['kafka']['prefork']);
        $this->assertArrayNotHasKey('prefork', $resolved['supervisors']['default']);
    }

    public function testAPoolCanTurnPreforkOnAlone(): void
    {
        $resolved = $this->resolve([
            'supervisors' => ['emails' => ['queues' => ['emails'], 'prefork' => true], 'default' => []],
        ]);

        $this->assertTrue($resolved['prefork']);
        $this->assertArrayNotHasKey('prefork', $resolved['supervisors']['emails']);
        $this->assertFalse($resolved['supervisors']['default']['prefork']);
    }

    public function testAPoolWithoutTheKeyFollowsTheSupervisorSwitch(): void
    {
        $resolved = $this->resolve(['prefork' => true, 'supervisors' => ['default' => ['prefork' => null]]]);

        $this->assertTrue($resolved['prefork']);
        $this->assertArrayNotHasKey('prefork', $resolved['supervisors']['default']);
    }

    public function testNoForkServerStartsWhenEveryPoolTurnsPreforkOff(): void
    {
        $resolved = $this->resolve(['prefork' => true, 'supervisors' => ['default' => ['prefork' => false]]]);

        $this->assertArrayNotHasKey('prefork', $resolved);
        $this->assertArrayNotHasKey('prefork', $resolved['supervisors']['default']);
    }

    public function testAPoolPreforkMustBeABoolean(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Queen supervisor supervisor [default] prefork must be a boolean.');

        $this->resolve(['supervisors' => ['default' => ['prefork' => 'yes']]]);
    }

    public function testThePhpEngineSpawnsTheWorkersOfAPoolWithPreforkOff(): void
    {
        $stateDirectory = sys_get_temp_dir() . '/queen-prefork-pools-' . bin2hex(random_bytes(6));
        mkdir($stateDirectory, 0700);
        $supervisor = new \Queen\Laravel\Supervisor\PhpSupervisor(
            $this->createStub(\Illuminate\Queue\QueueManager::class),
            [
                'state_directory' => $stateDirectory,
                'supervisors' => ['kafka' => ['prefork' => false], 'default' => []],
                'prefork' => true,
                'php_binary' => PHP_BINARY,
                'artisan' => __DIR__ . '/Fixtures/Prefork/fork_server.php',
                'cwd' => __DIR__,
            ],
            output: function (): void {
            },
        );
        (new \ReflectionMethod($supervisor, 'startForkServer'))->invoke($supervisor);
        $fork = new \ReflectionMethod($supervisor, 'forkWorker');
        $report = $this->report();

        try {
            $this->assertNull($fork->invoke($supervisor, 'kafka', 'kafka', ['exit', $report, '0'], []));
            $forked = $fork->invoke($supervisor, 'default', 'default', ['exit', $report, '0'], []);
            $this->assertInstanceOf(ForkedProcess::class, $forked);
            $this->waitUntil(fn (): bool => !$forked->isRunning());
        } finally {
            (new \ReflectionMethod($supervisor, 'closeForkServer'))->invoke($supervisor);
            @rmdir($stateDirectory);
        }
    }

    /** @param array<string, mixed> $supervisor */
    private function resolve(array $supervisor): array
    {
        return \Queen\Laravel\Supervisor\SupervisorConfiguration::resolve(
            ['url' => 'http://queen.test:6632', 'supervisor' => $supervisor],
            '/app',
        );
    }

    private function report(): string
    {
        return $this->reports[] = sys_get_temp_dir() . '/queen-prefork-' . bin2hex(random_bytes(6)) . '.json';
    }

    private function waitUntil(\Closure $condition): void
    {
        $deadline = microtime(true) + 10;
        while (true) {
            // is_file() and filesize() answer from PHP's stat cache: a second
            // look at a report first seen empty, as file_put_contents() leaves
            // it before it writes, would repeat the first until the deadline.
            clearstatcache();
            if ($condition()) {
                return;
            }
            if (microtime(true) > $deadline) {
                $this->fail('Timed out.');
            }
            usleep(20_000);
        }
    }
}
