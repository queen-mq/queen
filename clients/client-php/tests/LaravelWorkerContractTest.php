<?php

namespace Queen\Tests;

use Illuminate\Queue\QueueManager;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Queen\Laravel\Supervisor\PhpSupervisor;
use Queen\Laravel\Supervisor\SupervisorConfiguration;
use Queen\Tests\Support\WorkerInvocationFixture;

/**
 * The chain from config/queen.php to a worker's command line, pinned to
 * tests/Fixtures/Supervisor/worker-invocation.json: Laravel exports each pool
 * as the fixture says, and the PHP engine starts the pool's worker with the
 * arguments and environment the fixture says. The Rust master is pinned to
 * the same file in supervisor/src/main.rs, and the fork server in
 * LaravelWorkerInvocationTest.
 */
final class LaravelWorkerContractTest extends TestCase
{
    /** @var list<string> */
    private array $directories = [];

    protected function tearDown(): void
    {
        foreach ($this->directories as $directory) {
            foreach (glob($directory . '/{,.}*', GLOB_BRACE) ?: [] as $path) {
                if (is_file($path)) {
                    @unlink($path);
                }
            }
            @rmdir($directory);
        }
    }

    /** @return iterable<string, array{0: array<string, mixed>}> */
    public static function pools(): iterable
    {
        foreach (WorkerInvocationFixture::cases() as $case) {
            yield $case['name'] => [$case];
        }
    }

    /**
     * The document the master reads is what Laravel exports for this pool;
     * the Rust test refuses a field it does not know.
     */
    #[DataProvider('pools')]
    public function testLaravelExportsThePoolAsTheFixtureSays(array $case): void
    {
        $exported = SupervisorConfiguration::resolve(
            $case['config'],
            '/app',
            '/usr/bin/php',
            $case['queue_connections'],
        )['supervisors'][$case['supervisor']];

        $this->assertSame(self::sorted($case['pool']), self::sorted($exported));
    }

    /** The PHP engine runs the same command line as the Rust master. */
    #[DataProvider('pools')]
    public function testThePhpEngineStartsThePoolsWorkerAsTheFixtureSays(array $case): void
    {
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            [
                'state_directory' => $this->temporaryDirectory(),
                'cwd' => dirname(__DIR__),
                'php_binary' => PHP_BINARY,
                'artisan' => __DIR__ . '/Fixtures/FakeArtisan.php',
                'shutdown_grace' => 1,
            ],
        );

        $process = (new \ReflectionMethod(PhpSupervisor::class, 'startWorker'))
            ->invoke($supervisor, $case['supervisor'], $case['queue'], $case['pool']);
        $this->assertSame(0, $process->wait(), $process->getErrorOutput());
        $seen = json_decode(trim($process->getOutput()), true, 512, JSON_THROW_ON_ERROR);

        $this->assertSame(['queue:work', ...$case['arguments']], $seen['arguments']);
        $environment = $case['environment'];
        $this->assertSame($environment['QUEEN_LARAVEL_CONSUMER_GROUP'], $seen['consumer_group']);
        $this->assertSame($environment['QUEEN_LARAVEL_CONNECTION'], $seen['connection']);
        $this->assertSame($environment['QUEEN_LARAVEL_SUPERVISOR'], $seen['supervisor']);
        $this->assertSame($environment['QUEEN_LARAVEL_RETRY_AFTER'], $seen['retry_after']);
        // Absent from the fixture: the variable is not set for the worker.
        $this->assertSame($environment['QUEEN_LARAVEL_BLOCK_FOR'] ?? false, $seen['block_for']);
        if (($environment['QUEEN_SUPERVISOR_TELEMETRY_DIR'] ?? false) === true) {
            $this->assertStringEndsWith('/telemetry', (string) $seen['telemetry_directory']);
        } else {
            $this->assertFalse($seen['telemetry_directory']);
        }
    }

    /** A document without `quiet` ran the PHP engine's workers verbose; the Rust master reads it as quiet. */
    public function testThePhpEngineRunsQuietWhenTheDocumentDoesNotSay(): void
    {
        $case = WorkerInvocationFixture::cases()[0];
        $pool = $case['pool'];
        unset($pool['quiet']);
        $supervisor = new PhpSupervisor(
            $this->createStub(QueueManager::class),
            [
                'state_directory' => $this->temporaryDirectory(),
                'cwd' => dirname(__DIR__),
                'php_binary' => PHP_BINARY,
                'artisan' => __DIR__ . '/Fixtures/FakeArtisan.php',
                'shutdown_grace' => 1,
            ],
        );

        $process = (new \ReflectionMethod(PhpSupervisor::class, 'startWorker'))
            ->invoke($supervisor, $case['supervisor'], $case['queue'], $pool);
        $this->assertSame(0, $process->wait(), $process->getErrorOutput());
        $seen = json_decode(trim($process->getOutput()), true, 512, JSON_THROW_ON_ERROR);

        $this->assertContains('--quiet', $seen['arguments']);
    }

    /** @param array<string, mixed> $document */
    private static function sorted(array $document): array
    {
        ksort($document);

        return $document;
    }

    private function temporaryDirectory(): string
    {
        $directory = sys_get_temp_dir() . '/queen-worker-contract-' . bin2hex(random_bytes(6));
        mkdir($directory, 0700, true);
        $this->directories[] = $directory;

        return $directory;
    }
}
