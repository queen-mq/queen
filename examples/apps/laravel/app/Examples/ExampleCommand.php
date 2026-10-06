<?php

namespace App\Examples;

use Illuminate\Console\Command;
use Queen\Queen;
use RuntimeException;
use Throwable;

/**
 * What every example shares, kept out of the programs the pages show: fresh
 * queue names, worker processes, waiting for a condition, the checks, the
 * PASS or FAIL line with its exit code, and the clean-up after a pass.
 */
abstract class ExampleCommand extends Command
{
    private string $run;

    private int $checks = 0;

    /** @var list<string> the queues this run created */
    private array $queues = [];

    /** @var array<int, array{process: resource, label: string}> by pid */
    private array $processes = [];

    /** The example itself: throws when a check fails. */
    abstract protected function example(): void;

    final public function handle(): int
    {
        $this->run = base_convert((string) (int) (microtime(true) * 1000), 10, 36);
        @mkdir(storage_path('examples'), 0755, true);
        $this->line('broker ' . config('queen.url'));

        try {
            $this->example();
            $this->stopAll();
            $this->cleanUp();
            $this->line("\nPASS: {$this->checks} checks");

            return self::SUCCESS;
        } catch (Throwable $error) {
            fwrite(STDERR, "\nFAIL: {$error->getMessage()}\n");
            foreach ($this->processes as $pid => $process) {
                fwrite(STDERR, "  output of {$process['label']} (pid {$pid}): {$this->logOf($process['label'])}\n");
            }
            fwrite(STDERR, '  the job records and worker logs of this run are in ' . storage_path('examples') . "/*{$this->run}*\n");

            return self::FAILURE;
        } finally {
            $this->stopAll();
        }
    }

    /** A queue name no earlier run used. */
    protected function freshQueue(string $name): string
    {
        return $this->queues[] = "laravel-{$name}-{$this->run}";
    }

    /** Delete this run's queues, job records and worker logs. */
    private function cleanUp(): void
    {
        foreach (array_unique($this->queues) as $queue) {
            app(Queen::class)->queue($queue)->delete()->execute();
        }
        foreach (glob(storage_path("examples/*{$this->run}*")) ?: [] as $file) {
            unlink($file);
        }
    }

    /**
     * Start `php artisan queue:work` processes on one queue. --sleep=0: every
     * connection here long-polls (block_for), so an idle worker waits on the
     * broker and takes a job the moment it is ready, instead of sleeping.
     *
     * @param array<string, int|string> $options queue:work options over the defaults, such as ['tries' => 2]
     * @return list<int> their pids
     */
    protected function startWorkers(int $count, string $connection, string $queue, array $options = []): array
    {
        $arguments = ['queue:work', $connection, "--queue={$queue}", '--quiet'];
        foreach ([...['sleep' => 0, 'tries' => 3], ...$options] as $option => $value) {
            $arguments[] = "--{$option}={$value}";
        }
        $pids = [];
        for ($n = 1; $n <= $count; $n++) {
            $pids[] = $this->startArtisan("{$queue}-worker-{$n}", $arguments);
        }

        return $pids;
    }

    /**
     * Start an Artisan command as a process of its own; its output goes to a
     * log file under storage/examples/.
     *
     * @param list<string> $arguments
     * @param array<string, string> $environment added to this process's own
     */
    protected function startArtisan(string $label, array $arguments, array $environment = []): int
    {
        $log = storage_path("examples/{$label}.log");
        $process = proc_open(
            [PHP_BINARY, base_path('artisan'), ...$arguments],
            [0 => ['file', '/dev/null', 'r'], 1 => ['file', $log, 'a'], 2 => ['file', $log, 'a']],
            $pipes,
            base_path(),
            $environment === [] ? null : [...getenv(), ...$environment],
        );
        if (!is_resource($process)) {
            throw new RuntimeException("could not start {$label}");
        }
        $pid = proc_get_status($process)['pid'];
        $this->processes[$pid] = ['process' => $process, 'label' => $label];

        return $pid;
    }

    /** The output a process started by startArtisan() wrote so far. */
    protected function logOf(string $label): string
    {
        $log = storage_path("examples/{$label}.log");

        return is_file($log) ? (string) file_get_contents($log) : '';
    }

    protected function isRunning(int $pid): bool
    {
        $process = $this->processes[$pid]['process'] ?? null;

        return is_resource($process) && proc_get_status($process)['running'];
    }

    /**
     * SIGTERM, which a Laravel worker takes as "finish the job and exit", then
     * SIGKILL for what is still there after the deadline.
     *
     * @param list<int>|null $pids null stops every process this example started
     */
    protected function stopProcesses(?array $pids = null, float $seconds = 15.0): void
    {
        $pids ??= array_keys($this->processes);
        foreach ($pids as $pid) {
            if ($this->isRunning($pid)) {
                proc_terminate($this->processes[$pid]['process'], SIGTERM);
            }
        }
        $deadline = microtime(true) + $seconds;
        foreach ($pids as $pid) {
            while ($this->isRunning($pid) && microtime(true) < $deadline) {
                usleep(50_000);
            }
            if (isset($this->processes[$pid])) {
                if ($this->isRunning($pid)) {
                    proc_terminate($this->processes[$pid]['process'], SIGKILL);
                }
                proc_close($this->processes[$pid]['process']);
                unset($this->processes[$pid]);
            }
        }
    }

    private function stopAll(): void
    {
        try {
            $this->stopProcesses();
        } catch (Throwable) {
            // Best effort: the run already has its verdict.
        }
    }

    /**
     * Poll until $condition returns something truthy, and return it; fail
     * after $seconds.
     */
    protected function waitFor(string $what, float $seconds, callable $condition): mixed
    {
        $deadline = microtime(true) + $seconds;
        while (true) {
            $result = $condition();
            if ($result) {
                return $result;
            }
            if (microtime(true) > $deadline) {
                throw new RuntimeException("gave up after {$seconds} s waiting for {$what}");
            }
            usleep(50_000);
        }
    }

    /**
     * A list of numbers with each run of three or more equal values written
     * once with its count: 1 1 2 2 12 ×4 9.
     *
     * @param list<int> $values
     */
    protected function runLengths(array $values): string
    {
        $out = [];
        for ($i = 0; $i < count($values); $i = $j) {
            for ($j = $i; $j < count($values) && $values[$j] === $values[$i]; $j++) {
            }
            $run = $j - $i;
            $out[] = $run >= 3 ? "{$values[$i]} ×{$run}" : implode(' ', array_slice($values, $i, $run));
        }

        return implode(' ', $out);
    }

    protected function check(bool $condition, string $description): void
    {
        if (!$condition) {
            throw new RuntimeException($description);
        }
        $this->checks++;
        $this->line("  ok: {$description}");
    }
}
