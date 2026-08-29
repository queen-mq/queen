<?php

declare(strict_types=1);

use App\Support\BenchmarkEffectLedger;
use App\Support\JsonlResultSink;

require dirname(__DIR__).'/vendor/autoload.php';

final class EffectLedgerTest
{
    private int $assertions = 0;

    public function run(): void
    {
        $baseDirectory = sys_get_temp_dir().DIRECTORY_SEPARATOR
            .'queen-effect-ledger-'.getmypid().'-'.bin2hex(random_bytes(6));
        if (!mkdir($baseDirectory, 0770)) {
            throw new RuntimeException("Unable to create test directory [{$baseDirectory}].");
        }

        try {
            $this->testDurableAttemptAndEffects($baseDirectory);
            $this->testConcurrentIdempotencyKey($baseDirectory);
            $this->testOffModeIsNoOp($baseDirectory);
            $this->testInvalidIdentifiersFailClosed($baseDirectory);
        } finally {
            $this->removeTree($baseDirectory);
        }

        fwrite(STDOUT, "Effect ledger tests passed ({$this->assertions} assertions).\n");
    }

    private function testDurableAttemptAndEffects(string $baseDirectory): void
    {
        $sink = new JsonlResultSink($baseDirectory);
        $sink->reserveRun('durable-run');
        $ledger = new BenchmarkEffectLedger($baseDirectory, 'durable');
        $ledger->reserveRun('durable-run');

        $firstAttempt = $ledger->startAttempt('durable-run', '000000000', 1, 100);
        $this->true(is_string($firstAttempt), 'durable start returns attempt ID');
        $firstEffect = $ledger->commitEffect(
            'durable-run',
            '000000000',
            (string) $firstAttempt,
            str_repeat('ab', 32),
            200,
        );
        $ledger->completeAttempt('durable-run', (string) $firstAttempt, 300);

        $secondAttempt = $ledger->startAttempt('durable-run', '000000000', 2, 400);
        $secondEffect = $ledger->commitEffect(
            'durable-run',
            '000000000',
            (string) $secondAttempt,
            str_repeat('ab', 32),
            500,
        );
        $ledger->failAttempt(
            'durable-run',
            (string) $secondAttempt,
            600,
            new RuntimeException('fixture'),
        );

        $database = new PDO('sqlite:'.$baseDirectory.'/durable-run/ledger.sqlite3');
        $this->same(2, (int) $database->query('SELECT COUNT(*) FROM attempts')->fetchColumn(), 'attempt rows');
        $this->same(1, (int) $database->query('SELECT COUNT(*) FROM effects')->fetchColumn(), 'idempotent effect row');
        $this->same('completed', $database->query("SELECT outcome FROM attempts WHERE attempt_id = '{$firstAttempt}'")->fetchColumn(), 'completed outcome');
        $this->same('failed', $database->query("SELECT outcome FROM attempts WHERE attempt_id = '{$secondAttempt}'")->fetchColumn(), 'failed outcome');
        $this->same(true, $firstEffect['created'], 'first attempt creates effect');
        $this->same('created', $firstEffect['outcome'], 'first attempt outcome');
        $this->same(false, $secondEffect['created'], 'second attempt observes effect');
        $this->same('already_present', $secondEffect['outcome'], 'second attempt outcome');
        $this->same($firstEffect['effect_id'], $secondEffect['effect_id'], 'same idempotent effect ID');
        $checkpoint = $ledger->checkpointRun('durable-run');
        $this->same(0, $checkpoint['busy'], 'durable WAL checkpoint is not busy');
    }

    private function testConcurrentIdempotencyKey(string $baseDirectory): void
    {
        $runId = 'concurrent-run';
        $sink = new JsonlResultSink($baseDirectory);
        $sink->reserveRun($runId);
        $ledger = new BenchmarkEffectLedger($baseDirectory, 'durable');
        $ledger->reserveRun($runId);
        unset($ledger);

        $children = [];
        $workers = 4;
        for ($index = 0; $index < $workers; ++$index) {
            $pid = pcntl_fork();
            if ($pid === -1) {
                throw new RuntimeException('Unable to fork effect-ledger test worker.');
            }
            if ($pid === 0) {
                try {
                    $childLedger = new BenchmarkEffectLedger($baseDirectory, 'durable');
                    $attemptId = $childLedger->startAttempt(
                        $runId,
                        '000000000',
                        $index + 1,
                        hrtime(true),
                    );
                    file_put_contents("{$baseDirectory}/ready-{$index}", "ready\n", LOCK_EX);
                    $deadline = microtime(true) + 10;
                    while (!file_exists("{$baseDirectory}/go")) {
                        if (microtime(true) >= $deadline) {
                            throw new RuntimeException('Timed out waiting for concurrent commit barrier.');
                        }
                        usleep(1000);
                    }
                    $effect = $childLedger->commitEffect(
                        $runId,
                        '000000000',
                        (string) $attemptId,
                        str_repeat('cd', 32),
                        hrtime(true),
                    );
                    $childLedger->completeAttempt($runId, (string) $attemptId, hrtime(true));
                    file_put_contents(
                        "{$baseDirectory}/outcome-{$index}.json",
                        json_encode($effect, JSON_THROW_ON_ERROR)."\n",
                        LOCK_EX,
                    );
                    exit(0);
                } catch (Throwable $exception) {
                    file_put_contents("{$baseDirectory}/error-{$index}", (string) $exception);
                    exit(1);
                }
            }
            $children[] = $pid;
        }

        $deadline = microtime(true) + 15;
        while (count(glob($baseDirectory.'/ready-*') ?: []) < $workers) {
            if (microtime(true) >= $deadline) {
                throw new RuntimeException('Timed out waiting for forked ledger workers.');
            }
            usleep(1000);
        }
        file_put_contents($baseDirectory.'/go', "go\n", LOCK_EX);

        foreach ($children as $pid) {
            $waited = pcntl_waitpid($pid, $status);
            $this->same($pid, $waited, 'forked ledger worker reaped');
            $this->same(0, pcntl_wexitstatus($status), 'forked ledger worker succeeded');
        }

        $outcomes = [];
        for ($index = 0; $index < $workers; ++$index) {
            $outcome = json_decode(
                (string) file_get_contents("{$baseDirectory}/outcome-{$index}.json"),
                true,
                flags: JSON_THROW_ON_ERROR,
            );
            $outcomes[] = $outcome['outcome'] ?? null;
        }
        sort($outcomes);
        $this->same(
            ['already_present', 'already_present', 'already_present', 'created'],
            $outcomes,
            'concurrent create-or-observe outcomes',
        );
        $database = new PDO('sqlite:'.$baseDirectory.'/concurrent-run/ledger.sqlite3');
        $this->same(1, (int) $database->query('SELECT COUNT(*) FROM effects')->fetchColumn(), 'one concurrent effect');
        $this->same(4, (int) $database->query('SELECT COUNT(*) FROM attempts')->fetchColumn(), 'all concurrent attempts retained');
    }

    private function testOffModeIsNoOp(string $baseDirectory): void
    {
        $sink = new JsonlResultSink($baseDirectory);
        $sink->reserveRun('off-run');
        $ledger = new BenchmarkEffectLedger($baseDirectory, 'off');
        $ledger->reserveRun('off-run');
        $this->same(null, $ledger->startAttempt('off-run', '000000000', 1, 100), 'off start is null');
        $this->true(!file_exists($baseDirectory.'/off-run/ledger.sqlite3'), 'off mode creates no database');
    }

    private function testInvalidIdentifiersFailClosed(string $baseDirectory): void
    {
        $ledger = new BenchmarkEffectLedger($baseDirectory, 'durable');
        $this->throws(
            static fn () => $ledger->reserveRun('../escape'),
            RuntimeException::class,
            'unsafe run ID is rejected',
        );
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

    private function true(bool $condition, string $message): void
    {
        $this->same(true, $condition, $message);
    }

    /** @param callable(): mixed $callback */
    private function throws(callable $callback, string $class, string $message): void
    {
        ++$this->assertions;
        try {
            $callback();
        } catch (Throwable $exception) {
            if ($exception instanceof $class) {
                return;
            }
            throw new RuntimeException(
                "{$message}; expected {$class}, got ".get_debug_type($exception),
                previous: $exception,
            );
        }
        throw new RuntimeException("{$message}; expected {$class}, no exception was thrown");
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

(new EffectLedgerTest())->run();
