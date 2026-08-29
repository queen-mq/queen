<?php

declare(strict_types=1);

use App\Jobs\FailedJobProbe;
use App\Support\JsonlResultSink;
use Illuminate\Contracts\Console\Kernel;
use Queen\Laravel\Queue\SyncedFailedJobProvider;

require dirname(__DIR__).'/vendor/autoload.php';

final class FeatureParityTest
{
    private int $assertions = 0;

    public function run(): void
    {
        $baseDirectory = sys_get_temp_dir().DIRECTORY_SEPARATOR
            .'queen-feature-parity-'.getmypid().'-'.bin2hex(random_bytes(6));
        if (!mkdir($baseDirectory, 0770)) {
            throw new RuntimeException("Unable to create test directory [{$baseDirectory}].");
        }

        try {
            $this->testMultiQueueConfiguration($baseDirectory);
            $this->testMultiQueueDispatch($baseDirectory);
            $this->testStrictQueueListValidation($baseDirectory);
            $this->testPersistentFailedStore($baseDirectory);
            $this->testFailureProbeFailsOnce($baseDirectory);
        } finally {
            $this->removeTree($baseDirectory);
        }

        fwrite(STDOUT, "Feature parity tests passed ({$this->assertions} assertions).\n");
    }

    private function testMultiQueueConfiguration(string $baseDirectory): void
    {
        [$exitCode, $stdout, $stderr] = $this->artisan(
            $baseDirectory,
            ['bench:config'],
            ['BENCH_QUEUES' => 'critical,default', 'BENCH_WORKERS' => '2'],
        );
        $this->same(0, $exitCode, "bench:config failed: {$stderr}");
        $config = $this->lastJsonDocument($stdout);
        $this->same(['critical', 'default'], $config['benchmark']['queues'] ?? null, 'benchmark queues');
        $this->same('critical', $config['benchmark']['queue'] ?? null, 'first queue is the default');
        $this->same(['critical', 'default'], $config['horizon_supervisor']['queue'] ?? null, 'Horizon queues');
        $this->same(['critical', 'default'], $config['queen_supervisor']['queues'] ?? null, 'Queen queues');

        [$exitCode, $stdout, $stderr] = $this->artisan(
            $baseDirectory,
            ['bench:config'],
            ['BENCH_QUEUES' => '123,default', 'BENCH_WORKERS' => '2'],
        );
        $this->same(0, $exitCode, "numeric queue config failed: {$stderr}");
        $numeric = $this->lastJsonDocument($stdout);
        $this->same(['123', 'default'], $numeric['benchmark']['queues'] ?? null, 'numeric queue names remain strings');
    }

    private function testMultiQueueDispatch(string $baseDirectory): void
    {
        [$exitCode, $stdout, $stderr] = $this->artisan(
            $baseDirectory,
            [
                'bench:dispatch-multi',
                '--run-id=multi-queue',
                '--jobs-per-queue=3',
                '--queues=critical,default',
                '--sleep-ms=0',
                '--connection=sync',
            ],
            ['BENCH_QUEUES' => 'critical,default', 'BENCH_WORKERS' => '2'],
        );
        $this->same(0, $exitCode, "bench:dispatch-multi failed: {$stderr}");
        $manifest = $this->lastJsonDocument($stdout);
        $this->same(6, $manifest['jobs'] ?? null, 'multi-queue total jobs');
        $this->same('critical,default', $manifest['queues_csv'] ?? null, 'multi-queue manifest order');

        $records = (new JsonlResultSink($baseDirectory))->read('multi-queue');
        $prefixes = ['critical' => 0, 'default' => 0];
        foreach ($records as $record) {
            $jobId = $record['job_id'] ?? null;
            if (is_string($jobId)) {
                $prefix = strstr($jobId, ':', true);
                if (is_string($prefix) && array_key_exists($prefix, $prefixes)) {
                    ++$prefixes[$prefix];
                }
            }
        }
        $this->same(['critical' => 3, 'default' => 3], $prefixes, 'round-robin job distribution');
    }

    private function testStrictQueueListValidation(string $baseDirectory): void
    {
        [$exitCode, $stdout, $stderr] = $this->artisan(
            $baseDirectory,
            ['bench:config'],
            [
                'BENCH_FAILED_DRIVER' => 'null',
                'BENCH_QUEUES' => 'critical,default',
                'BENCH_WORKERS' => '2',
            ],
        );
        $this->same(0, $exitCode, "the explicit null failed driver must remain a string: {$stderr}");
        $this->same(
            'null',
            $this->lastJsonDocument($stdout)['benchmark']['failed_driver'] ?? null,
            'explicit null failed driver',
        );

        foreach ([
            'critical, default' => 'surrounding whitespace',
            'critical,critical' => 'duplicates',
            'critical,' => 'empty entries',
        ] as $queues => $label) {
            [$exitCode] = $this->artisan(
                $baseDirectory,
                ['bench:config'],
                ['BENCH_QUEUES' => $queues, 'BENCH_WORKERS' => '2'],
            );
            $this->notSame(0, $exitCode, "BENCH_QUEUES rejects {$label}");
        }

        [$exitCode] = $this->artisan(
            $baseDirectory,
            ['bench:config'],
            ['BENCH_QUEUES' => 'critical,default', 'BENCH_WORKERS' => '1'],
        );
        $this->notSame(0, $exitCode, 'fixed capacity must cover every queue');

        [$exitCode] = $this->artisan(
            $baseDirectory,
            [
                'bench:dispatch-multi',
                '--run-id=mismatched-queues',
                '--jobs-per-queue=1',
                '--queues=critical,other',
                '--sleep-ms=0',
                '--connection=sync',
            ],
            ['BENCH_QUEUES' => 'critical,default', 'BENCH_WORKERS' => '2'],
        );
        $this->notSame(0, $exitCode, 'dispatcher rejects queues not supervised by BENCH_QUEUES');

        $maximumQueue = str_repeat('q', 118);
        [$exitCode, $stdout, $stderr] = $this->artisan(
            $baseDirectory,
            [
                'bench:dispatch-multi',
                '--run-id=maximum-queue-id',
                '--jobs-per-queue=1',
                "--queues={$maximumQueue},default",
                '--sleep-ms=0',
                '--connection=sync',
            ],
            ['BENCH_QUEUES' => "{$maximumQueue},default", 'BENCH_WORKERS' => '2'],
        );
        $this->same(0, $exitCode, "118-byte multi-queue name failed: {$stderr}");
        $this->same(2, $this->lastJsonDocument($stdout)['jobs'] ?? null, '118-byte queue boundary');

        $oversizedQueue = str_repeat('q', 119);
        [$exitCode] = $this->artisan(
            $baseDirectory,
            [
                'bench:dispatch-multi',
                '--run-id=oversized-queue-id',
                '--jobs-per-queue=1',
                "--queues={$oversizedQueue},default",
                '--sleep-ms=0',
                '--connection=sync',
            ],
            ['BENCH_QUEUES' => "{$oversizedQueue},default", 'BENCH_WORKERS' => '2'],
        );
        $this->notSame(0, $exitCode, '119-byte multi-queue name is rejected before dispatch');
    }

    private function testPersistentFailedStore(string $baseDirectory): void
    {
        $failedPath = $baseDirectory.'/failed-jobs.json';
        foreach ([
            'APP_ENV' => 'testing',
            'BENCH_CONNECTION' => 'queen',
            'QUEUE_CONNECTION' => 'queen',
            'BENCH_RESULTS_DIRECTORY' => $baseDirectory,
            'BENCH_FAILED_DRIVER' => 'file',
            'BENCH_FAILED_PATH' => $failedPath,
            'BENCH_FAILED_LIMIT' => '100',
            'BENCH_QUEUES' => 'critical,default',
            'BENCH_WORKERS' => '2',
        ] as $name => $value) {
            putenv("{$name}={$value}");
        }

        $app = require dirname(__DIR__).'/bootstrap/app.php';
        $app->make(Kernel::class)->bootstrap();
        $failer = $app['queue.failer'];
        $this->same(true, $failer instanceof SyncedFailedJobProvider, 'Queen decorates the failed store');

        $uuid = '018f1f72-9577-7ca7-a735-32c0c8987ad1';
        $payload = json_encode([
            'uuid' => $uuid,
            'displayName' => FailedJobProbe::class,
            '_queen' => [
                'manual_retry' => 'retry-probe-1',
                'failed_source' => [
                    'partition_id' => 'partition-probe-1',
                    'transaction_id' => 'transaction-probe-1',
                ],
            ],
        ], JSON_THROW_ON_ERROR);
        $this->same(
            $uuid,
            $failer->log('queen', 'critical', $payload, new RuntimeException('intentional probe')),
            'failed provider returns the durable UUID',
        );
        $this->same(1, $failer->count('queen', 'critical'), 'failed provider count');
        $this->same(true, is_file($failedPath), 'failed store is persisted below the shared results path');

        [$exitCode, $stdout, $stderr] = $this->artisan(
            $baseDirectory,
            ['queue:failed', '--no-ansi'],
            [
                'BENCH_CONNECTION' => 'queen',
                'QUEUE_CONNECTION' => 'queen',
                'BENCH_FAILED_DRIVER' => 'file',
                'BENCH_FAILED_PATH' => $failedPath,
                'BENCH_FAILED_LIMIT' => '100',
                'BENCH_QUEUES' => 'critical,default',
                'BENCH_WORKERS' => '2',
            ],
        );
        $this->same(0, $exitCode, "queue:failed could not reopen the store: {$stderr}");
        $this->same(true, str_contains($stdout, $uuid), 'failed row survives a fresh Artisan process');
    }

    private function testFailureProbeFailsOnce(string $baseDirectory): void
    {
        $sink = new JsonlResultSink($baseDirectory);
        $sink->reserveRun('failure-probe');
        $job = new FailedJobProbe('failure-probe', 'probe-1', hrtime(true));

        try {
            $job->handle($sink);
            throw new RuntimeException('Failure probe did not fail on its first execution.');
        } catch (RuntimeException $exception) {
            $this->same(
                true,
                str_contains($exception->getMessage(), 'Intentional first failure'),
                'failure probe first execution',
            );
        }

        $job->handle($sink);
        $records = $sink->read('failure-probe');
        $this->same(1, count($records), 'failure probe records one successful retry');
        $this->same('failure-probe:probe-1', $records[0]['job_id'] ?? null, 'failure probe retry identity');
    }

    /**
     * @param list<string> $arguments
     * @param array<string, string> $overrides
     * @return array{int, string, string}
     */
    private function artisan(string $resultsDirectory, array $arguments, array $overrides = []): array
    {
        $environment = getenv();
        $environment = is_array($environment) ? $environment : [];
        $environment = array_replace($environment, [
            'APP_ENV' => 'testing',
            'BENCH_RESULTS_DIRECTORY' => $resultsDirectory,
            'BENCH_FAILED_PATH' => $resultsDirectory.'/failed-jobs.json',
        ], $overrides);
        $process = proc_open(
            [PHP_BINARY, 'artisan', ...$arguments],
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
            dirname(__DIR__),
            $environment,
        );
        if (!is_resource($process)) {
            throw new RuntimeException('Unable to launch fixture Artisan command.');
        }
        $stdout = stream_get_contents($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);

        return [proc_close($process), (string) $stdout, (string) $stderr];
    }

    /** @return array<string, mixed> */
    private function lastJsonDocument(string $output): array
    {
        $decoded = json_decode(trim($output), true);
        if (!is_array($decoded)) {
            throw new RuntimeException("Command did not emit a JSON document: {$output}");
        }

        return $decoded;
    }

    private function same(mixed $expected, mixed $actual, string $message): void
    {
        ++$this->assertions;
        if ($actual !== $expected) {
            throw new RuntimeException(
                $message.'; expected '.var_export($expected, true).', got '.var_export($actual, true),
            );
        }
    }

    private function notSame(mixed $expected, mixed $actual, string $message): void
    {
        ++$this->assertions;
        if ($actual === $expected) {
            throw new RuntimeException($message.'; values unexpectedly match '.var_export($actual, true));
        }
    }

    private function removeTree(string $directory): void
    {
        if (!is_dir($directory)) {
            return;
        }
        $entries = scandir($directory);
        if ($entries === false) {
            throw new RuntimeException("Unable to inspect test directory [{$directory}].");
        }
        foreach ($entries as $entry) {
            if ($entry === '.' || $entry === '..') {
                continue;
            }
            $path = $directory.DIRECTORY_SEPARATOR.$entry;
            if (is_dir($path) && !is_link($path)) {
                $this->removeTree($path);
            } elseif (!unlink($path)) {
                throw new RuntimeException("Unable to remove test file [{$path}].");
            }
        }
        if (!rmdir($directory)) {
            throw new RuntimeException("Unable to remove test directory [{$directory}].");
        }
    }
}

(new FeatureParityTest())->run();
